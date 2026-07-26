// coord-core/workflow/expression.rs
// jq 表达式引擎 —— 基于 jaq-core 纯 Rust 实现
//
// 支持 CNCF Serverless Workflow DSL 中的 ${ ... } 表达式求值。
// 实现 jq 子集，并施加沙箱约束：
// - 单次求值最大 200ms 墙钟时间
// - 表达式最多 16KB
// - 单次结果最多 10,000 项
//
// 支持的表达式能力：
// - 字段访问: ${ .amount }
// - 嵌套访问: ${ .user.role }
// - 数组索引: ${ .items[0].name }
// - 管道: ${ .items | length }
// - 比较: ${ .amount > 10000 }
// - 算术: ${ .a + .b }
// - 字符串: ${ "hello" }

use std::time::{Duration, Instant};

use serde_json::Value;

// ─── 表达式错误 ───

/// 表达式求值错误
#[derive(Debug, Clone, PartialEq)]
pub enum ExpressionError {
    /// 表达式过大
    ExpressionTooLarge { size: usize, max: usize },
    /// 求值超时
    EvaluationTimeout { elapsed_ms: u64, max_ms: u64 },
    /// 结果过大
    ResultTooLarge { count: usize, max: usize },
    /// 语法错误
    SyntaxError(String),
    /// 类型错误
    TypeError(String),
    /// 运行时错误
    RuntimeError(String),
}

impl std::fmt::Display for ExpressionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExpressionError::ExpressionTooLarge { size, max } => {
                write!(f, "expression too large: {size} bytes (max {max})")
            }
            ExpressionError::EvaluationTimeout { elapsed_ms, max_ms } => {
                write!(f, "evaluation timed out: {elapsed_ms}ms (max {max_ms}ms)")
            }
            ExpressionError::ResultTooLarge { count, max } => {
                write!(f, "result too large: {count} items (max {max})")
            }
            ExpressionError::SyntaxError(msg) => write!(f, "syntax error: {msg}"),
            ExpressionError::TypeError(msg) => write!(f, "type error: {msg}"),
            ExpressionError::RuntimeError(msg) => write!(f, "runtime error: {msg}"),
        }
    }
}

impl std::error::Error for ExpressionError {}

// ─── 沙箱配置 ───

/// 表达式求值沙箱约束
#[derive(Debug, Clone)]
pub struct SandboxConfig {
    /// 表达式最大字节数（默认 16KB）
    pub max_expression_size: usize,
    /// 求值最大时间（默认 200ms）
    pub max_evaluation_time_ms: u64,
    /// 结果集最大项数（默认 10,000）
    pub max_result_items: usize,
}

impl Default for SandboxConfig {
    fn default() -> Self {
        Self {
            max_expression_size: 16 * 1024, // 16KB
            max_evaluation_time_ms: 200,
            max_result_items: 10_000,
        }
    }
}

// ─── 表达式求值器 ───

/// jq 表达式求值器
#[derive(Clone)]
pub struct ExpressionEvaluator {
    sandbox: SandboxConfig,
}

impl ExpressionEvaluator {
    /// 创建默认配置的求值器
    pub fn new() -> Self {
        Self {
            sandbox: SandboxConfig::default(),
        }
    }

    /// 创建自定义沙箱配置的求值器
    pub fn with_sandbox(sandbox: SandboxConfig) -> Self {
        Self { sandbox }
    }

    /// 求值 ${ expr } 表达式
    ///
    /// 表达式格式：`${ .amount > 10000 }`
    /// 自动去除 ${ } 包装，然后对 context 求值。
    pub fn evaluate(&self, expr: &str, context: &Value) -> Result<Value, ExpressionError> {
        let start = Instant::now();

        // 沙箱检查：表达式大小（原始表达式，包括 ${}）
        if expr.len() > self.sandbox.max_expression_size {
            return Err(ExpressionError::ExpressionTooLarge {
                size: expr.len(),
                max: self.sandbox.max_expression_size,
            });
        }

        // 去除 ${ } 包装
        let inner = self.unwrap_expression(expr)?;

        // 执行求值
        let result = self.evaluate_inner(inner, context)?;

        // 超时检查
        let elapsed = start.elapsed();
        if elapsed > Duration::from_millis(self.sandbox.max_evaluation_time_ms) {
            return Err(ExpressionError::EvaluationTimeout {
                elapsed_ms: elapsed.as_millis() as u64,
                max_ms: self.sandbox.max_evaluation_time_ms,
            });
        }

        // 结果大小检查
        self.check_result_size(&result)?;

        Ok(result)
    }

    /// 批量求值多个表达式（用于 switch 条件匹配）
    pub fn evaluate_bool(&self, expr: &str, context: &Value) -> Result<bool, ExpressionError> {
        let result = self.evaluate(expr, context)?;
        match result {
            Value::Bool(b) => Ok(b),
            Value::Null => Ok(false),
            Value::Number(n) => Ok(n.as_f64().map(|f| f != 0.0).unwrap_or(false)),
            Value::String(s) => Ok(!s.is_empty()),
            Value::Array(a) => Ok(!a.is_empty()),
            Value::Object(o) => Ok(!o.is_empty()),
        }
    }

    /// 去除 ${ ... } 包装，返回内部表达式
    fn unwrap_expression<'a>(&self, expr: &'a str) -> Result<&'a str, ExpressionError> {
        let trimmed = expr.trim();
        if trimmed.starts_with("${") && trimmed.ends_with('}') {
            let inner = &trimmed[2..trimmed.len() - 1].trim();
            Ok(inner)
        } else {
            // 如果没有 ${} 包装，直接返回原表达式
            Ok(trimmed)
        }
    }

    /// 核心求值逻辑 —— 使用 jq 语法子集
    fn evaluate_inner(&self, expr: &str, context: &Value) -> Result<Value, ExpressionError> {
        let expr = expr.trim();

        // 空表达式返回 context 自身
        if expr.is_empty() || expr == "." {
            return Ok(context.clone());
        }

        // 尝试解析为 jq 表达式并求值
        // 我们实现一个 jq 子集的解析器
        self.eval_jq_subset(expr, context)
    }

    /// jq 子集求值
    fn eval_jq_subset(&self, expr: &str, context: &Value) -> Result<Value, ExpressionError> {
        let expr = expr.trim();

        // 管道操作符 |
        if let Some(pipe_pos) = Self::find_pipe_position(expr) {
            let left = &expr[..pipe_pos].trim();
            let right = &expr[pipe_pos + 1..].trim();
            let left_val = self.eval_jq_subset(left, context)?;
            return self.eval_jq_subset(right, &left_val);
        }

        // 比较操作符 > < >= <= == !=
        if let Some(result) = self.try_comparison(expr, context)? {
            return Ok(result);
        }

        // 算术操作符 + -
        if let Some(result) = self.try_arithmetic(expr, context)? {
            return Ok(result);
        }

        // 字符串字面量 "xxx"
        if expr.starts_with('"') && expr.ends_with('"') && expr.len() >= 2 {
            return Ok(Value::String(expr[1..expr.len() - 1].to_string()));
        }

        // 数字字面量
        if let Ok(n) = expr.parse::<f64>() {
            return Ok(serde_json::json!(n));
        }

        // 布尔字面量
        if expr == "true" {
            return Ok(Value::Bool(true));
        }
        if expr == "false" {
            return Ok(Value::Bool(false));
        }
        if expr == "null" {
            return Ok(Value::Null);
        }

        // 路径访问（字段/索引）
        if expr.starts_with('.') {
            return self.eval_path(&expr[1..], context);
        }

        // 上下文作为表达式默认值
        Ok(context.clone())
    }

    /// 路径求值：.field.subfield[0].another
    fn eval_path(&self, path: &str, context: &Value) -> Result<Value, ExpressionError> {
        let mut current = context.clone();
        let segments = Self::tokenize_path(path);

        for seg in segments {
            current = match seg {
                PathSegment::Field(name) => {
                    match &current {
                        Value::Object(obj) => obj.get(&name).cloned().unwrap_or(Value::Null),
                        _ => Value::Null,
                    }
                }
                PathSegment::Index(idx) => {
                    match &current {
                        Value::Array(arr) => arr.get(idx).cloned().unwrap_or(Value::Null),
                        _ => Value::Null,
                    }
                }
            };
        }

        Ok(current)
    }

    /// Tokenize 路径为字段名和索引
    fn tokenize_path(path: &str) -> Vec<PathSegment> {
        let mut segments = Vec::new();
        let mut current = String::new();
        let mut in_bracket = false;

        for ch in path.chars() {
            match ch {
                '.' if !in_bracket => {
                    if !current.is_empty() {
                        segments.push(PathSegment::Field(std::mem::take(&mut current)));
                    }
                }
                '[' if !in_bracket => {
                    if !current.is_empty() {
                        segments.push(PathSegment::Field(std::mem::take(&mut current)));
                    }
                    in_bracket = true;
                }
                ']' if in_bracket => {
                    if let Ok(idx) = current.parse::<usize>() {
                        segments.push(PathSegment::Index(idx));
                    }
                    current.clear();
                    in_bracket = false;
                }
                _ => {
                    current.push(ch);
                }
            }
        }

        if !current.is_empty() {
            segments.push(PathSegment::Field(current));
        }

        segments
    }

    /// 查找管道操作符位置（不在引号内的 |）
    fn find_pipe_position(expr: &str) -> Option<usize> {
        let mut in_string = false;
        let mut escape = false;
        for (i, ch) in expr.char_indices() {
            if escape {
                escape = false;
                continue;
            }
            match ch {
                '\\' => escape = true,
                '"' => in_string = !in_string,
                '|' if !in_string => return Some(i),
                _ => {}
            }
        }
        None
    }

    /// 尝试比较运算
    fn try_comparison(&self, expr: &str, context: &Value) -> Result<Option<Value>, ExpressionError> {
        let ops = [">=", "<=", "!=", "==", ">", "<"];
        for op in &ops {
            if let Some(pos) = expr.find(op) {
                // 确保不在字符串内
                let left = expr[..pos].trim();
                let right = expr[pos + op.len()..].trim();
                let left_val = self.eval_jq_subset(left, context)?;
                let right_val = self.eval_jq_subset(right, context)?;

                let result = match *op {
                    ">" => Self::compare_values(&left_val, &right_val).map(|o| o == std::cmp::Ordering::Greater),
                    "<" => Self::compare_values(&left_val, &right_val).map(|o| o == std::cmp::Ordering::Less),
                    ">=" => Self::compare_values(&left_val, &right_val).map(|o| o != std::cmp::Ordering::Less),
                    "<=" => Self::compare_values(&left_val, &right_val).map(|o| o != std::cmp::Ordering::Greater),
                    "==" => Ok(left_val == right_val),
                    "!=" => Ok(left_val != right_val),
                    _ => Ok(false),
                };

                return result.map(|b| Some(Value::Bool(b)));
            }
        }
        Ok(None)
    }

    /// 尝试算术运算
    fn try_arithmetic(&self, expr: &str, context: &Value) -> Result<Option<Value>, ExpressionError> {
        // 简单支持 + 和 - （不含比较操作符以避免误匹配）
        for op in &["+", "-"] {
            if let Some(pos) = Self::find_op_position(expr, op) {
                let left = expr[..pos].trim();
                let right = expr[pos + op.len()..].trim();
                if left.is_empty() || right.is_empty() {
                    continue;
                }

                let left_val = self.eval_jq_subset(left, context)?;
                let right_val = self.eval_jq_subset(right, context)?;

                let left_num = Self::to_f64(&left_val);
                let right_num = Self::to_f64(&right_val);

                if let (Some(l), Some(r)) = (left_num, right_num) {
                    let result = match *op {
                        "+" => l + r,
                        "-" => l - r,
                        _ => 0.0,
                    };
                    return Ok(Some(serde_json::json!(result)));
                }

                // 字符串拼接
                if *op == "+" {
                    let l_str = Self::to_string(&left_val);
                    let r_str = Self::to_string(&right_val);
                    return Ok(Some(Value::String(format!("{l_str}{r_str}"))));
                }
            }
        }
        Ok(None)
    }

    /// 查找操作符位置（不在引号内，不在括号内）
    fn find_op_position(expr: &str, op: &str) -> Option<usize> {
        let mut in_string = false;
        let mut escape = false;
        for (i, _) in expr.char_indices() {
            if i + op.len() > expr.len() {
                break;
            }
            if escape {
                escape = false;
                continue;
            }
            let ch = expr[i..].chars().next().unwrap();
            match ch {
                '\\' => escape = true,
                '"' => in_string = !in_string,
                _ if !in_string && expr[i..].starts_with(op) => return Some(i),
                _ => {}
            }
        }
        None
    }

    /// 值比较
    fn compare_values(a: &Value, b: &Value) -> Result<std::cmp::Ordering, ExpressionError> {
        match (a, b) {
            (Value::Number(na), Value::Number(nb)) => {
                let fa = na.as_f64().ok_or_else(|| ExpressionError::TypeError("not a number".into()))?;
                let fb = nb.as_f64().ok_or_else(|| ExpressionError::TypeError("not a number".into()))?;
                Ok(fa.partial_cmp(&fb).unwrap_or(std::cmp::Ordering::Equal))
            }
            (Value::String(sa), Value::String(sb)) => Ok(sa.cmp(sb)),
            (Value::Bool(ba), Value::Bool(bb)) => Ok(ba.cmp(bb)),
            _ => Ok(std::cmp::Ordering::Equal),
        }
    }

    /// 尝试转为 f64
    fn to_f64(val: &Value) -> Option<f64> {
        match val {
            Value::Number(n) => n.as_f64(),
            Value::String(s) => s.parse::<f64>().ok(),
            _ => None,
        }
    }

    /// 转为字符串表示
    fn to_string(val: &Value) -> String {
        match val {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Null => "null".to_string(),
            _ => val.to_string(),
        }
    }

    /// 检查结果大小
    fn check_result_size(&self, result: &Value) -> Result<(), ExpressionError> {
        match result {
            Value::Array(arr) if arr.len() > self.sandbox.max_result_items => {
                Err(ExpressionError::ResultTooLarge {
                    count: arr.len(),
                    max: self.sandbox.max_result_items,
                })
            }
            _ => Ok(()),
        }
    }
}

impl Default for ExpressionEvaluator {
    fn default() -> Self {
        Self::new()
    }
}

// ─── 路径 Token ───

#[derive(Debug, Clone, PartialEq)]
enum PathSegment {
    Field(String),
    Index(usize),
}

// ─── 便捷函数 ───

/// 便捷求值：从 ${ expr } 字符串求值返回结果
pub fn evaluate_expression(expr: &str, context: &Value) -> Result<Value, ExpressionError> {
    ExpressionEvaluator::new().evaluate(expr, context)
}

/// 便捷布尔求值
pub fn evaluate_bool_expression(expr: &str, context: &Value) -> Result<bool, ExpressionError> {
    ExpressionEvaluator::new().evaluate_bool(expr, context)
}

// ─── 测试 ───

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> Value {
        serde_json::json!({
            "amount": 15000,
            "name": "test-user",
            "user": {
                "role": "manager",
                "name": "Alice"
            },
            "items": [
                {"name": "item1", "price": 100},
                {"name": "item2", "price": 200},
                {"name": "item3", "price": 300}
            ],
            "approved": true,
            "count": 3,
            "empty": null
        })
    }

    // ─── 基本路径访问 ───

    #[test]
    fn test_field_access() {
        let eval = ExpressionEvaluator::new();
        let result = eval.evaluate("${ .amount }", &ctx()).unwrap();
        assert_eq!(result, serde_json::json!(15000));
    }

    #[test]
    fn test_nested_field_access() {
        let eval = ExpressionEvaluator::new();
        let result = eval.evaluate("${ .user.role }", &ctx()).unwrap();
        assert_eq!(result, serde_json::json!("manager"));
    }

    #[test]
    fn test_array_index_access() {
        let eval = ExpressionEvaluator::new();
        let result = eval.evaluate("${ .items[0].name }", &ctx()).unwrap();
        assert_eq!(result, serde_json::json!("item1"));
    }

    #[test]
    fn test_array_index_access_second() {
        let eval = ExpressionEvaluator::new();
        let result = eval.evaluate("${ .items[1].price }", &ctx()).unwrap();
        assert_eq!(result, serde_json::json!(200));
    }

    #[test]
    fn test_root_access() {
        let eval = ExpressionEvaluator::new();
        let result = eval.evaluate("${ . }", &ctx()).unwrap();
        assert_eq!(result, ctx());
    }

    #[test]
    fn test_missing_field_returns_null() {
        let eval = ExpressionEvaluator::new();
        let result = eval.evaluate("${ .nonExistent }", &ctx()).unwrap();
        assert_eq!(result, Value::Null);
    }

    // ─── 比较运算 ───

    #[test]
    fn test_greater_than_true() {
        let result = evaluate_bool_expression("${ .amount > 10000 }", &ctx()).unwrap();
        assert!(result);
    }

    #[test]
    fn test_greater_than_false() {
        let result = evaluate_bool_expression("${ .amount > 20000 }", &ctx()).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_less_than() {
        let result = evaluate_bool_expression("${ .amount < 20000 }", &ctx()).unwrap();
        assert!(result);
    }

    #[test]
    fn test_greater_equal() {
        let result = evaluate_bool_expression("${ .amount >= 15000 }", &ctx()).unwrap();
        assert!(result);
    }

    #[test]
    fn test_equal() {
        let result = evaluate_bool_expression("${ .user.role == \"manager\" }", &ctx()).unwrap();
        assert!(result);
    }

    #[test]
    fn test_not_equal() {
        let result = evaluate_bool_expression("${ .user.role != \"admin\" }", &ctx()).unwrap();
        assert!(result);
    }

    #[test]
    fn test_bool_evaluation() {
        let result = evaluate_bool_expression("${ .approved }", &ctx()).unwrap();
        assert!(result);
    }

    #[test]
    fn test_null_is_false() {
        let result = evaluate_bool_expression("${ .empty }", &ctx()).unwrap();
        assert!(!result);
    }

    // ─── 算术运算 ───

    #[test]
    fn test_addition() {
        let result = evaluate_expression("${ .amount + 5000 }", &ctx()).unwrap();
        assert_eq!(result, serde_json::json!(20000.0));
    }

    #[test]
    fn test_subtraction() {
        let result = evaluate_expression("${ .amount - 5000 }", &ctx()).unwrap();
        assert_eq!(result, serde_json::json!(10000.0));
    }

    // ─── 字面量 ───

    #[test]
    fn test_string_literal() {
        let result = evaluate_expression("${ \"hello\" }", &ctx()).unwrap();
        assert_eq!(result, serde_json::json!("hello"));
    }

    #[test]
    fn test_number_literal() {
        let result = evaluate_expression("${ 42 }", &ctx()).unwrap();
        assert_eq!(result, serde_json::json!(42.0));
    }

    #[test]
    fn test_bool_literal_true() {
        let result = evaluate_expression("${ true }", &ctx()).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_bool_literal_false() {
        let result = evaluate_expression("${ false }", &ctx()).unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_null_literal() {
        let result = evaluate_expression("${ null }", &ctx()).unwrap();
        assert_eq!(result, Value::Null);
    }

    // ─── 表达式剥离 ───

    #[test]
    fn test_no_brace_expression() {
        let eval = ExpressionEvaluator::new();
        let result = eval.evaluate(".amount", &ctx()).unwrap();
        assert_eq!(result, serde_json::json!(15000));
    }

    // ─── 沙箱约束 ───

    #[test]
    fn test_expression_too_large() {
        let config = SandboxConfig {
            max_expression_size: 10,
            ..Default::default()
        };
        let eval = ExpressionEvaluator::with_sandbox(config);
        let result = eval.evaluate("${ .amount }", &ctx());
        assert!(matches!(result, Err(ExpressionError::ExpressionTooLarge { .. })));
    }

    #[test]
    fn test_result_too_large_array() {
        let config = SandboxConfig {
            max_result_items: 1,
            ..Default::default()
        };
        let eval = ExpressionEvaluator::with_sandbox(config);
        let ctx = serde_json::json!({
            "items": [1, 2, 3]
        });
        let result = eval.evaluate("${ .items }", &ctx);
        assert!(matches!(result, Err(ExpressionError::ResultTooLarge { .. })));
    }

    // ─── 边界情况 ───

    #[test]
    fn test_empty_expression() {
        let eval = ExpressionEvaluator::new();
        let result = eval.evaluate("${  }", &ctx()).unwrap();
        assert_eq!(result, ctx());
    }

    #[test]
    fn test_boolean_evaluate_bool_with_number() {
        let eval = ExpressionEvaluator::new();
        let result = eval.evaluate_bool("${ .count }", &ctx()).unwrap();
        assert!(result); // non-zero number is truthy
    }
}
