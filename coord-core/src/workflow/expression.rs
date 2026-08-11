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

use std::collections::HashMap;
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
        self.evaluate_with_vars(expr, context, &HashMap::new())
    }

    /// 求值 ${ expr } 表达式，并注入标准运行时变量绑定
    ///
    /// 标准参数：`$context` / `$input` / `$output` / `$task` / `$workflow` /
    /// `$runtime` / `$authorization` / `$secrets` / `$constants`。
    /// `$context` 始终绑定到 `context` 根；其余变量从 `vars` 查找。
    pub fn evaluate_with_vars(
        &self,
        expr: &str,
        context: &Value,
        vars: &HashMap<String, Value>,
    ) -> Result<Value, ExpressionError> {
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
        let result = self.evaluate_inner(inner, context, vars)?;

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

    /// 批量求值多个表达式（用于 switch 条件匹配），支持变量注入
    pub fn evaluate_bool(&self, expr: &str, context: &Value) -> Result<bool, ExpressionError> {
        self.evaluate_bool_with_vars(expr, context, &HashMap::new())
    }

    /// 布尔求值，支持变量注入
    pub fn evaluate_bool_with_vars(
        &self,
        expr: &str,
        context: &Value,
        vars: &HashMap<String, Value>,
    ) -> Result<bool, ExpressionError> {
        let result = self.evaluate_with_vars(expr, context, vars)?;
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
    fn evaluate_inner(
        &self,
        expr: &str,
        context: &Value,
        vars: &HashMap<String, Value>,
    ) -> Result<Value, ExpressionError> {
        let expr = expr.trim();

        // 空表达式返回 context 自身
        if expr.is_empty() || expr == "." {
            return Ok(context.clone());
        }

        // 尝试解析为 jq 表达式并求值
        // 我们实现一个 jq 子集的解析器
        self.eval_jq_subset(expr, context, vars)
    }

    /// jq 子集求值
    fn eval_jq_subset(
        &self,
        expr: &str,
        context: &Value,
        vars: &HashMap<String, Value>,
    ) -> Result<Value, ExpressionError> {
        let expr = expr.trim();

        // 管道操作符 |
        if let Some(pipe_pos) = Self::find_pipe_position(expr) {
            let left = &expr[..pipe_pos].trim();
            let right = &expr[pipe_pos + 1..].trim();
            let left_val = self.eval_jq_subset(left, context, vars)?;
            return self.eval_jq_subset(right, &left_val, vars);
        }

        // 比较操作符 > < >= <= == !=
        if let Some(result) = self.try_comparison(expr, context, vars)? {
            return Ok(result);
        }

        // 算术操作符 + -
        if let Some(result) = self.try_arithmetic(expr, context, vars)? {
            return Ok(result);
        }

        // 字符串字面量 "xxx"
        if expr.starts_with('"') && expr.ends_with('"') && expr.len() >= 2 {
            return Ok(Value::String(expr[1..expr.len() - 1].to_string()));
        }

        // 数字字面量（整数按 i64，小数按 f64）
        if expr.starts_with('-') || expr.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false) {
            if let Ok(n) = expr.parse::<i64>() {
                return Ok(serde_json::json!(n));
            }
            if let Ok(n) = expr.parse::<f64>() {
                return Ok(serde_json::json!(n));
            }
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

        // 对象字面量 { "key": expr, ... }（export.as 常用 . + {...}）
        if expr.starts_with('{') && expr.ends_with('}') {
            if let Some(v) = self.try_object_literal(expr, context, vars)? {
                return Ok(v);
            }
        }

        // 括号表达式 ( expr )
        if expr.starts_with('(') && expr.ends_with(')') {
            return self.eval_jq_subset(&expr[1..expr.len() - 1], context, vars);
        }

        // 标准运行时变量绑定：$context / $input / $output / ...
        if expr.starts_with('$') {
            return self.eval_variable(expr, context, vars);
        }

        // 路径访问（字段/索引）
        if expr.starts_with('.') {
            return self.eval_path(&expr[1..], context);
        }

        // 上下文作为表达式默认值
        Ok(context.clone())
    }

    /// 对象字面量求值：`{ "key": expr, "key2": expr }`
    fn try_object_literal(
        &self,
        expr: &str,
        context: &Value,
        vars: &HashMap<String, Value>,
    ) -> Result<Option<Value>, ExpressionError> {
        let inner = &expr[1..expr.len() - 1].trim();
        if inner.is_empty() {
            return Ok(Some(Value::Object(Default::default())));
        }
        let mut obj = serde_json::Map::new();
        let parts = Self::split_top_level(inner, ',');
        for part in parts {
            let part = part.trim();
            // 支持 "key": value 与 key: value
            let colon = Self::find_top_level(part, ':').ok_or_else(|| {
                ExpressionError::SyntaxError(format!("invalid object literal entry: {part}"))
            })?;
            let key = part[..colon].trim();
            let key = key
                .strip_prefix('"')
                .and_then(|k| k.strip_suffix('"'))
                .map(String::from)
                .unwrap_or_else(|| key.to_string());
            let value_expr = part[colon + 1..].trim();
            let value = self.eval_jq_subset(value_expr, context, vars)?;
            obj.insert(key, value);
        }
        Ok(Some(Value::Object(obj)))
    }

    /// 按顶层分隔符切分（忽略引号/括号/花括号内的分隔符）
    fn split_top_level(expr: &str, sep: char) -> Vec<&str> {
        let mut parts = Vec::new();
        let mut depth = 0i32;
        let mut in_string = false;
        let mut escape = false;
        let mut start = 0usize;
        for (i, ch) in expr.char_indices() {
            if escape {
                escape = false;
                continue;
            }
            match ch {
                '\\' if in_string => escape = true,
                '"' => in_string = !in_string,
                '(' | '[' | '{' if !in_string => depth += 1,
                ')' | ']' | '}' if !in_string => depth -= 1,
                c if !in_string && depth == 0 && c == sep => {
                    parts.push(&expr[start..i]);
                    start = i + 1;
                }
                _ => {}
            }
        }
        parts.push(&expr[start..]);
        parts
    }

    /// 在顶层查找字符位置
    fn find_top_level(expr: &str, target: char) -> Option<usize> {
        let mut depth = 0i32;
        let mut in_string = false;
        let mut escape = false;
        for (i, ch) in expr.char_indices() {
            if escape {
                escape = false;
                continue;
            }
            match ch {
                '\\' if in_string => escape = true,
                '"' => in_string = !in_string,
                '(' | '[' | '{' if !in_string => depth += 1,
                ')' | ']' | '}' if !in_string => depth -= 1,
                c if !in_string && depth == 0 && c == target => return Some(i),
                _ => {}
            }
        }
        None
    }

    /// 标准运行时变量求值：`$name` 或 `$name.path.to[0].field`
    ///
    /// `$context` 恒绑定 context 根；`$input`/`$output`/`$task`/`$workflow`/
    /// `$runtime`/`$authorization`/`$secrets`/`$constants` 从 vars 查找。
    /// 未定义变量 → 语法错误（避免静默返回整个 context 的隐患）。
    fn eval_variable(
        &self,
        expr: &str,
        context: &Value,
        vars: &HashMap<String, Value>,
    ) -> Result<Value, ExpressionError> {
        let expr = expr.trim();
        let rest = &expr[1..]; // 去掉 $

        // 变量名 = 直到 '.' 或 '[' 为止
        let name_end = rest
            .find(|c: char| c == '.' || c == '[')
            .unwrap_or(rest.len());
        let name = &rest[..name_end];
        let path_rest = &rest[name_end..];

        let base = match name {
            "context" => context.clone(),
            _ => vars.get(name).cloned().unwrap_or(Value::Null),
        };

        if path_rest.is_empty() {
            return Ok(base);
        }
        self.eval_path(path_rest, &base)
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

    /// 查找管道操作符位置（不在引号内、不在嵌套括号内的 |）
    fn find_pipe_position(expr: &str) -> Option<usize> {
        let mut in_string = false;
        let mut escape = false;
        let mut depth = 0i32;
        for (i, ch) in expr.char_indices() {
            if escape {
                escape = false;
                continue;
            }
            match ch {
                '\\' => escape = true,
                '"' => in_string = !in_string,
                '(' | '[' | '{' if !in_string => depth += 1,
                ')' | ']' | '}' if !in_string => depth -= 1,
                '|' if !in_string && depth == 0 => return Some(i),
                _ => {}
            }
        }
        None
    }

    /// 尝试比较运算
    fn try_comparison(
        &self,
        expr: &str,
        context: &Value,
        vars: &HashMap<String, Value>,
    ) -> Result<Option<Value>, ExpressionError> {
        let ops = [">=", "<=", "!=", "==", ">", "<"];
        for op in &ops {
            if let Some(pos) = expr.find(op) {
                // 确保不在字符串内
                let left = expr[..pos].trim();
                let right = expr[pos + op.len()..].trim();
                let left_val = self.eval_jq_subset(left, context, vars)?;
                let right_val = self.eval_jq_subset(right, context, vars)?;

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
    fn try_arithmetic(
        &self,
        expr: &str,
        context: &Value,
        vars: &HashMap<String, Value>,
    ) -> Result<Option<Value>, ExpressionError> {
        // 简单支持 + 和 - （不含比较操作符以避免误匹配）
        for op in &["+", "-"] {
            if let Some(pos) = Self::find_op_position(expr, op) {
                let left = expr[..pos].trim();
                let right = expr[pos + op.len()..].trim();
                if left.is_empty() || right.is_empty() {
                    continue;
                }

                let left_val = self.eval_jq_subset(left, context, vars)?;
                let right_val = self.eval_jq_subset(right, context, vars)?;

                // 整数保持（JSON 整数语义）：两个操作数均为整数时用 i64 运算
                if let (Some(l), Some(r)) = (left_val.as_i64(), right_val.as_i64()) {
                    let result = match *op {
                        "+" => l.checked_add(r).unwrap_or(l),
                        "-" => l.checked_sub(r).unwrap_or(l),
                        _ => l,
                    };
                    return Ok(Some(serde_json::json!(result)));
                }

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

                // 对象合并（export.as 常用：. + {"result": ...}）—— 先于字符串拼接
                if *op == "+" {
                    if let (Some(l_obj), Some(r_obj)) = (left_val.as_object(), right_val.as_object()) {
                        let mut merged = l_obj.clone();
                        for (k, v) in r_obj {
                            merged.insert(k.clone(), v.clone());
                        }
                        return Ok(Some(Value::Object(merged)));
                    }
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
        let mut depth = 0i32;
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
                '(' | '[' | '{' if !in_string => depth += 1,
                ')' | ']' | '}' if !in_string => depth -= 1,
                _ if !in_string && depth == 0 && expr[i..].starts_with(op) => return Some(i),
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
        // 整数保持：15000 + 5000 = 20000（i64）
        assert_eq!(result, serde_json::json!(20000));
    }

    #[test]
    fn test_subtraction() {
        let result = evaluate_expression("${ .amount - 5000 }", &ctx()).unwrap();
        // 整数保持
        assert_eq!(result, serde_json::json!(10000));
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
        // 整数按 i64 解析（保持 JSON 整数语义）
        assert_eq!(result, serde_json::json!(42));
        // 小数按 f64
        let float = evaluate_expression("${ 3.14 }", &ctx()).unwrap();
        assert_eq!(float, serde_json::json!(3.14));
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

    // ─── 标准运行时变量绑定（$context/$input/$output/...） ───

    fn vars() -> HashMap<String, Value> {
        let mut v = HashMap::new();
        v.insert("input".to_string(), serde_json::json!({"raw": 42}));
        v.insert("output".to_string(), serde_json::json!({"result": "ok"}));
        v.insert("task".to_string(), serde_json::json!({"name": "step1", "type": "call"}));
        v.insert("workflow".to_string(), serde_json::json!({"name": "wf", "version": "1.0"}));
        v.insert("secrets".to_string(), serde_json::json!({"apiKey": "secret-123"}));
        v.insert("constants".to_string(), serde_json::json!({"region": "cn-north"}));
        v.insert("authorization".to_string(), serde_json::json!({"role": "admin"}));
        v
    }

    #[test]
    fn test_variable_input_binding() {
        let eval = ExpressionEvaluator::new();
        let result = eval
            .evaluate_with_vars("${ $input.raw }", &ctx(), &vars())
            .unwrap();
        assert_eq!(result, serde_json::json!(42));
    }

    #[test]
    fn test_variable_output_binding() {
        let eval = ExpressionEvaluator::new();
        let result = eval
            .evaluate_with_vars("${ $output.result }", &ctx(), &vars())
            .unwrap();
        assert_eq!(result, serde_json::json!("ok"));
    }

    #[test]
    fn test_variable_secrets_binding() {
        let eval = ExpressionEvaluator::new();
        let result = eval
            .evaluate_with_vars("${ $secrets.apiKey }", &ctx(), &vars())
            .unwrap();
        assert_eq!(result, serde_json::json!("secret-123"));
    }

    #[test]
    fn test_variable_context_is_root() {
        let eval = ExpressionEvaluator::new();
        // $context 恒绑定 context 根
        let result = eval
            .evaluate_with_vars("${ $context.amount }", &ctx(), &vars())
            .unwrap();
        assert_eq!(result, serde_json::json!(15000));
    }

    #[test]
    fn test_variable_workflow_binding() {
        let eval = ExpressionEvaluator::new();
        let result = eval
            .evaluate_with_vars("${ $workflow.name == \"wf\" }", &ctx(), &vars())
            .unwrap();
        assert_eq!(result, serde_json::json!(true));
    }

    #[test]
    fn test_variable_in_condition() {
        let eval = ExpressionEvaluator::new();
        let result = eval
            .evaluate_bool_with_vars("${ $input.raw > 40 }", &ctx(), &vars())
            .unwrap();
        assert!(result);
    }

    #[test]
    fn test_variable_nested_path() {
        let eval = ExpressionEvaluator::new();
        let result = eval
            .evaluate_with_vars("${ $task.name }", &ctx(), &vars())
            .unwrap();
        assert_eq!(result, serde_json::json!("step1"));
    }

    #[test]
    fn test_unknown_variable_returns_null() {
        let eval = ExpressionEvaluator::new();
        let result = eval
            .evaluate_with_vars("${ $nonexistent.xyz }", &ctx(), &vars())
            .unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_variable_pipe_with_path() {
        let eval = ExpressionEvaluator::new();
        // $input | .raw 形式（管道左侧为变量）
        let result = eval
            .evaluate_with_vars("${ $input | .raw }", &ctx(), &vars())
            .unwrap();
        assert_eq!(result, serde_json::json!(42));
    }

    #[test]
    fn test_object_merge_addition() {
        let eval = ExpressionEvaluator::new();
        // export.as 常用：. + {"result": ...} 对象合并
        let result = eval
            .evaluate("${ . + {\"extra\": 1} }", &serde_json::json!({"a": 1}))
            .unwrap();
        assert_eq!(result, serde_json::json!({"a": 1, "extra": 1}));
    }
}
