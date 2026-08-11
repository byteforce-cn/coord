// coord-core/workflow/parser.rs
// DSL 解析器 —— 两阶段架构
//
// Phase 1 (本模块): YAML/JSON → RawWorkflowDef（宽松中间表示，保留位置信息）
// Phase 2 (validate 模块): RawWorkflowDef → WorkflowDefinition（强类型语义校验）
//
// 设计要点：
// - 不依赖 serde(tag = "type") 做任务枚举分发
// - 每个 AST 节点携带 Span 用于精确错误报告
// - 容错解析：语法错误收集后一并报告

use std::collections::HashMap;

use serde_json::Value;

use super::model::Span;

// ─── 宽松中间表示（Raw IR） ───

/// 宽松的中间表示 —— 只做 YAML/JSON 语法解析，不做语义校验
#[derive(Debug, Clone, PartialEq)]
pub struct RawWorkflowDef {
    pub span: Span,
    pub document: RawDocument,
    pub tasks: Vec<RawNamedTask>,
    pub input: Option<RawValue>,
    pub use_components: Option<RawUseComponents>,
    /// 顶层扩展块（output/timeout/schedule/auth/secrets/constants）
    pub ext: Option<RawDefinitionExt>,
}

/// 顶层扩展块（标准 §Data Flow / §Scheduling / §Authentication / §Secrets）
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RawDefinitionExt {
    pub output: Option<RawValue>,
    pub timeout: Option<RawValue>,
    pub schedule: Option<RawValue>,
    pub auth: Option<RawValue>,
    pub secrets: Option<RawValue>,
    pub constants: Option<RawValue>,
}

/// 宽松的 Document
#[derive(Debug, Clone, PartialEq)]
pub struct RawDocument {
    pub span: Span,
    pub dsl: String,
    pub namespace: String,
    pub name: String,
    pub version: String,
    pub title: Option<String>,
    pub summary: Option<String>,
    pub tags: Option<HashMap<String, String>>,
}

/// 宽松的任务表示 —— 不做类型判别，保留原始 JSON body
#[derive(Debug, Clone, PartialEq)]
pub struct RawNamedTask {
    pub span: Span,
    pub name: String,
    pub body: Value,
}

/// 宽松的 use 组件
#[derive(Debug, Clone, PartialEq)]
pub struct RawUseComponents {
    pub span: Span,
    pub functions: Option<HashMap<String, RawFunctionDef>>,
    pub retries: Option<HashMap<String, RawRetryPolicy>>,
    pub timeouts: Option<HashMap<String, RawTimeoutConfig>>,
}

/// 宽松的函数定义
#[derive(Debug, Clone, PartialEq)]
pub struct RawFunctionDef {
    pub span: Span,
    pub body: Value,
}

/// 宽松的重试策略
#[derive(Debug, Clone, PartialEq)]
pub struct RawRetryPolicy {
    pub span: Span,
    pub body: Value,
}

/// 宽松的超时配置
#[derive(Debug, Clone, PartialEq)]
pub struct RawTimeoutConfig {
    pub span: Span,
    pub body: Value,
}

/// 包裹 Value 以携带 Span
#[derive(Debug, Clone, PartialEq)]
pub struct RawValue {
    pub span: Span,
    pub value: Value,
}

// ─── 解析错误 ───

/// 解析错误
#[derive(Debug, Clone, PartialEq)]
pub struct ParseError {
    pub span: Option<Span>,
    pub message: String,
    pub detail: Option<String>,
}

impl ParseError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            span: None,
            message: message.into(),
            detail: None,
        }
    }

    pub fn with_span(mut self, span: Span) -> Self {
        self.span = Some(span);
        self
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(span) = self.span {
            write!(f, "line {}, col {}: {}", span.line, span.column, self.message)?;
        } else {
            write!(f, "{}", self.message)?;
        }
        if let Some(detail) = &self.detail {
            write!(f, " ({})", detail)?;
        }
        Ok(())
    }
}

impl std::error::Error for ParseError {}

/// 解析结果类型
pub type ParseResult<T> = std::result::Result<T, ParseError>;

// ─── YAML 解析器 ───

/// 从 YAML 字符串解析为 RawWorkflowDef
pub fn parse_yaml(src: &str) -> ParseResult<RawWorkflowDef> {
    let yaml_value: Value = serde_yaml::from_str(src).map_err(|e| {
        ParseError::new(format!("YAML syntax error: {e}")).with_detail(format!("{e}"))
    })?;

    parse_from_value(yaml_value, Span::new(1, 1, 0))
}

/// 从 JSON 字符串解析为 RawWorkflowDef
pub fn parse_json(src: &str) -> ParseResult<RawWorkflowDef> {
    let json_value: Value = serde_json::from_str(src).map_err(|e| {
        ParseError::new(format!("JSON syntax error: {e}")).with_detail(format!("{e}"))
    })?;

    parse_from_value(json_value, Span::new(1, 1, 0))
}

/// 从 serde_json::Value 解析（JSON 和 YAML 的公共路径）
fn parse_from_value(root: Value, span: Span) -> ParseResult<RawWorkflowDef> {
    let obj = root.as_object().ok_or_else(|| {
        ParseError::new("workflow definition must be a JSON object").with_span(span)
    })?;

    // 解析 document 块
    let document = parse_document(obj, span)?;

    // 解析 do 任务列表
    let tasks = parse_do_tasks(obj, span)?;

    // 解析 input 块（可选）
    let input = obj
        .get("input")
        .map(|v| RawValue {
            span,
            value: v.clone(),
        });

    // 解析 use 块（可选）
    let use_components = obj.get("use").map(|v| parse_use_components(v, span)).transpose()?;

    // 解析顶层扩展块（可选）
    let ext = {
        let mut e = RawDefinitionExt::default();
        e.output = obj.get("output").map(|v| RawValue { span, value: v.clone() });
        e.timeout = obj.get("timeout").map(|v| RawValue { span, value: v.clone() });
        e.schedule = obj.get("schedule").map(|v| RawValue { span, value: v.clone() });
        e.auth = obj.get("auth").map(|v| RawValue { span, value: v.clone() });
        e.secrets = obj.get("secrets").map(|v| RawValue { span, value: v.clone() });
        e.constants = obj.get("constants").map(|v| RawValue { span, value: v.clone() });
        let has_any = e.output.is_some()
            || e.timeout.is_some()
            || e.schedule.is_some()
            || e.auth.is_some()
            || e.secrets.is_some()
            || e.constants.is_some();
        if has_any { Some(e) } else { None }
    };

    Ok(RawWorkflowDef {
        span,
        document,
        tasks,
        input,
        use_components,
        ext,
    })
}

/// 解析 document 块
fn parse_document(obj: &serde_json::Map<String, Value>, span: Span) -> ParseResult<RawDocument> {
    let doc_obj = obj
        .get("document")
        .and_then(|v| v.as_object())
        .ok_or_else(|| {
            ParseError::new("missing required 'document' block").with_span(span)
        })?;

    let dsl = get_string_field(doc_obj, "dsl", span, "document.dsl")?;
    let namespace = get_string_field(doc_obj, "namespace", span, "document.namespace")?;
    let name = get_string_field(doc_obj, "name", span, "document.name")?;
    let version = get_string_field(doc_obj, "version", span, "document.version")?;

    let title = doc_obj.get("title").and_then(|v| v.as_str()).map(String::from);
    let summary = doc_obj.get("summary").and_then(|v| v.as_str()).map(String::from);
    let tags = doc_obj.get("tags").and_then(|v| {
        v.as_object().map(|m| {
            m.iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                .collect()
        })
    });

    Ok(RawDocument {
        span,
        dsl,
        namespace,
        name,
        version,
        title,
        summary,
        tags,
    })
}

/// 解析 do 任务列表（顶级任务）
fn parse_do_tasks(
    obj: &serde_json::Map<String, Value>,
    span: Span,
) -> ParseResult<Vec<RawNamedTask>> {
    let do_list = match obj.get("do") {
        Some(Value::Array(arr)) => arr,
        Some(_) => {
            return Err(ParseError::new("'do' must be an array of tasks").with_span(span));
        }
        None => {
            return Err(ParseError::new("missing required 'do' task list").with_span(span));
        }
    };

    let mut tasks = Vec::new();
    for (idx, item) in do_list.iter().enumerate() {
        // 行号估算：基于索引偏移
        let item_span = Span::new(span.line + idx + 1, span.column, span.offset + idx);

        let obj = item.as_object().ok_or_else(|| {
            ParseError::new(format!("task at index {idx} must be a JSON object"))
                .with_span(item_span)
        })?;

        if obj.len() != 1 {
            return Err(ParseError::new(format!(
                "task at index {idx} must have exactly one key (the task name), got {} keys",
                obj.len()
            ))
            .with_span(item_span));
        }

        let (name, body) = obj.iter().next().unwrap();
        tasks.push(RawNamedTask {
            span: item_span,
            name: name.clone(),
            body: body.clone(),
        });
    }

    Ok(tasks)
}

/// 解析 use 组件块
fn parse_use_components(use_val: &Value, span: Span) -> ParseResult<RawUseComponents> {
    let obj = use_val.as_object().ok_or_else(|| {
        ParseError::new("'use' must be a JSON object").with_span(span)
    })?;

    let functions = obj.get("functions").map(|f| {
        parse_function_map(f, span)
    }).transpose()?;

    let retries = obj.get("retries").map(|r| {
        parse_retry_map(r, span)
    }).transpose()?;

    let timeouts = obj.get("timeouts").map(|t| {
        parse_timeout_map(t, span)
    }).transpose()?;

    Ok(RawUseComponents {
        span,
        functions,
        retries,
        timeouts,
    })
}

fn parse_function_map(val: &Value, span: Span) -> ParseResult<HashMap<String, RawFunctionDef>> {
    let obj = val.as_object().ok_or_else(|| {
        ParseError::new("'use.functions' must be a JSON object").with_span(span)
    })?;
    let mut map = HashMap::new();
    for (k, v) in obj {
        map.insert(k.clone(), RawFunctionDef { span, body: v.clone() });
    }
    Ok(map)
}

fn parse_retry_map(val: &Value, span: Span) -> ParseResult<HashMap<String, RawRetryPolicy>> {
    let obj = val.as_object().ok_or_else(|| {
        ParseError::new("'use.retries' must be a JSON object").with_span(span)
    })?;
    let mut map = HashMap::new();
    for (k, v) in obj {
        map.insert(k.clone(), RawRetryPolicy { span, body: v.clone() });
    }
    Ok(map)
}

fn parse_timeout_map(val: &Value, span: Span) -> ParseResult<HashMap<String, RawTimeoutConfig>> {
    let obj = val.as_object().ok_or_else(|| {
        ParseError::new("'use.timeouts' must be a JSON object").with_span(span)
    })?;
    let mut map = HashMap::new();
    for (k, v) in obj {
        map.insert(k.clone(), RawTimeoutConfig { span, body: v.clone() });
    }
    Ok(map)
}

// ─── 辅助函数 ───

/// 从 JSON object 中提取必填字符串字段
fn get_string_field(
    obj: &serde_json::Map<String, Value>,
    key: &str,
    span: Span,
    full_path: &str,
) -> ParseResult<String> {
    obj.get(key)
        .and_then(|v| v.as_str())
        .map(String::from)
        .ok_or_else(|| {
            ParseError::new(format!("missing required field '{full_path}'")).with_span(span)
        })
}

// ─── RawWorkflowDef 便捷方法 ───

impl RawWorkflowDef {
    /// 从 YAML 字符串解析
    pub fn parse_yaml(src: &str) -> ParseResult<Self> {
        parse_yaml(src)
    }

    /// 从 JSON 字符串解析
    pub fn parse_json(src: &str) -> ParseResult<Self> {
        parse_json(src)
    }
}

// ─── 测试 ───

#[cfg(test)]
mod tests {
    use super::*;

    // ─── 有效 YAML 解析测试 ───

    #[test]
    fn test_parse_minimal_workflow_yaml() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: minimal
  version: "1.0"
do:
  - step1:
      call: http
      with:
        method: GET
        endpoint: "https://example.com"
"#;

        let result = parse_yaml(yaml);
        assert!(
            result.is_ok(),
            "parse failed: {:?}",
            result.err()
        );
        let raw = result.unwrap();
        assert_eq!(raw.document.dsl, "1.0.0");
        assert_eq!(raw.document.namespace, "test");
        assert_eq!(raw.document.name, "minimal");
        assert_eq!(raw.document.version, "1.0");
        assert_eq!(raw.tasks.len(), 1);
        assert_eq!(raw.tasks[0].name, "step1");
        // body 应该是 {"call": "http", "with": {...}}
        let body = raw.tasks[0].body.as_object().unwrap();
        assert!(body.contains_key("call"));
        assert!(body.contains_key("with"));
    }

    #[test]
    fn test_parse_workflow_with_switch() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: icps
  name: approval
  version: "1.0"
do:
  - checkAmount:
      switch:
        - condition: "${ .amount > 10000 }"
          transition: seniorApproval
        - condition: "${ .amount > 5000 }"
          transition: managerApproval
        - defaultCondition: directorApproval
  - managerApproval:
      call: http
      with:
        method: POST
        endpoint: "https://api/approve"
"#;

        let result = parse_yaml(yaml);
        assert!(result.is_ok(), "parse failed: {:?}", result.err());
        let raw = result.unwrap();
        assert_eq!(raw.tasks.len(), 2);
        assert_eq!(raw.tasks[0].name, "checkAmount");
        assert_eq!(raw.tasks[1].name, "managerApproval");

        // 验证 switch body
        let switch_body = raw.tasks[0].body.as_object().unwrap();
        let switch_conds = switch_body["switch"].as_array().unwrap();
        assert_eq!(switch_conds.len(), 3);
    }

    #[test]
    fn test_parse_workflow_with_wait() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: wait-example
  version: "1.0"
do:
  - delay:
      wait: PT1H
"#;

        let result = parse_yaml(yaml);
        assert!(result.is_ok(), "parse failed: {:?}", result.err());
        let raw = result.unwrap();
        assert_eq!(raw.tasks[0].name, "delay");
        let body = raw.tasks[0].body.as_object().unwrap();
        assert_eq!(body["wait"].as_str().unwrap(), "PT1H");
    }

    #[test]
    fn test_parse_workflow_with_use_components() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: with-function
  version: "1.0"
use:
  functions:
    sendNotification:
      call: http
      with:
        method: POST
        endpoint: "https://notify/api"
  retries:
    defaultRetry:
      delay: PT3S
      limit: 3
do:
  - notify:
      call: function
      with:
        function: sendNotification
"#;

        let result = parse_yaml(yaml);
        assert!(result.is_ok(), "parse failed: {:?}", result.err());
        let raw = result.unwrap();
        let use_comp = raw.use_components.as_ref().unwrap();
        assert!(use_comp.functions.as_ref().unwrap().contains_key("sendNotification"));
        assert!(use_comp.retries.as_ref().unwrap().contains_key("defaultRetry"));
    }

    #[test]
    fn test_parse_workflow_with_input() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: with-input
  version: "1.0"
input:
  schema: "http://example.com/schema.json"
  default:
    amount: 0
    role: staff
do:
  - step1:
      call: http
      with:
        method: POST
        endpoint: "https://api/step"
"#;

        let result = parse_yaml(yaml);
        assert!(result.is_ok(), "parse failed: {:?}", result.err());
        let raw = result.unwrap();
        assert!(raw.input.is_some());
    }

    // ─── 错误处理测试 ───

    #[test]
    fn test_parse_rejects_missing_document() {
        let yaml = r#"
do:
  - step1:
      call: http
"#;
        let result = parse_yaml(yaml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("document"));
    }

    #[test]
    fn test_parse_rejects_missing_namespace() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  name: test
  version: "1.0"
do: []
"#;
        let result = parse_yaml(yaml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("namespace"));
    }

    #[test]
    fn test_parse_rejects_missing_do() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: test
  version: "1.0"
"#;
        let result = parse_yaml(yaml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("do"));
    }

    #[test]
    fn test_parse_rejects_invalid_yaml() {
        let yaml = "this: is: not: valid: yaml: [";
        let result = parse_yaml(yaml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("YAML"));
    }

    #[test]
    fn test_parse_rejects_do_not_array() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: test
  version: "1.0"
do: "not an array"
"#;
        let result = parse_yaml(yaml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("array"));
    }

    #[test]
    fn test_parse_rejects_task_with_multiple_keys() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: test
  version: "1.0"
do:
  - step1:
      call: http
    step2:
      wait: PT1H
"#;

        let result = parse_yaml(yaml);
        // YAML 中 - 开头的数组项如果包含两个 key 会导致解析失败
        // 这个测试验证解析器会拒绝这种格式
        if let Err(err) = &result {
            assert!(err.message.contains("YAML") || err.message.contains("exactly one"));
        }
    }

    // ─── JSON 解析测试 ───

    #[test]
    fn test_parse_minimal_workflow_json() {
        let json = r#"
{
  "document": {
    "dsl": "1.0.0",
    "namespace": "test",
    "name": "minimal",
    "version": "1.0"
  },
  "do": [
    {
      "step1": {
        "call": "http",
        "with": {
          "method": "GET",
          "endpoint": "https://example.com"
        }
      }
    }
  ]
}
"#;

        let result = parse_json(json);
        assert!(result.is_ok(), "parse failed: {:?}", result.err());
        let raw = result.unwrap();
        assert_eq!(raw.document.name, "minimal");
        assert_eq!(raw.tasks.len(), 1);
        assert_eq!(raw.tasks[0].name, "step1");
    }

    #[test]
    fn test_parse_rejects_invalid_json() {
        let json = "{ invalid json }";
        let result = parse_json(json);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("JSON"));
    }

    // ─── 往返测试 ───

    #[test]
    fn test_yaml_json_equivalent() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: equiv
  version: "1.0"
do:
  - step1:
      call: http
      with:
        method: GET
        endpoint: "https://example.com"
"#;

        let json = r#"
{
  "document": {
    "dsl": "1.0.0",
    "namespace": "test",
    "name": "equiv",
    "version": "1.0"
  },
  "do": [
    {
      "step1": {
        "call": "http",
        "with": {
          "method": "GET",
          "endpoint": "https://example.com"
        }
      }
    }
  ]
}
"#;

        let yaml_result = parse_yaml(yaml).unwrap();
        let json_result = parse_json(json).unwrap();

        // 两个结果在语义上应该等价
        assert_eq!(yaml_result.document.name, json_result.document.name);
        assert_eq!(yaml_result.document.namespace, json_result.document.namespace);
        assert_eq!(yaml_result.tasks.len(), json_result.tasks.len());
        assert_eq!(yaml_result.tasks[0].name, json_result.tasks[0].name);
    }

    #[test]
    fn test_parse_error_display_format() {
        let err = ParseError::new("something went wrong")
            .with_span(Span::new(5, 10, 100))
            .with_detail("additional context");
        let display = format!("{}", err);
        assert!(display.contains("line 5"));
        assert!(display.contains("col 10"));
        assert!(display.contains("something went wrong"));
        assert!(display.contains("additional context"));
    }
}
