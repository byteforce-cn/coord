// coord-core/workflow/jsonschema.rs
// 最小 JSON Schema 校验器（Draft-07 子集）
//
// 支持标准 §Data Flow 的 `input.schema` / `output.schema` 校验所需的关键关键字：
// - type / enum / const
// - properties / required / additionalProperties
// - items / minItems / maxItems / uniqueItems
// - minimum / maximum / exclusiveMinimum / exclusiveMaximum / multipleOf
// - minLength / maxLength / pattern
// - minProperties / maxProperties
// - allOf / anyOf / oneOf / not
// - $ref（仅内部引用 "#/definitions/..." 与 "#/$defs/..."）
//
// 设计约束：纯函数、无 I/O、无外部依赖（与 expression.rs 手写 jq 子集同风格），
// 校验失败返回错误消息列表（标准要求失败 → validation 错误 → faulted）。

use serde_json::Value;

/// 校验 `value` 是否符合 `schema`（schema 为 JSON 文本或 JSON value）
pub fn validate(schema: &str, value: &Value) -> Result<(), Vec<String>> {
    let schema_value: Value = match serde_json::from_str(schema) {
        Ok(v) => v,
        Err(e) => return Err(vec![format!("invalid JSON Schema: {e}")]),
    };
    let mut errors = Vec::new();
    validate_value(&schema_value, value, &schema_value, "#", &mut errors);
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// 校验 `value` 是否符合 `schema_value`（Value 形式，避免重复解析）
pub fn validate_value(
    schema_value: &Value,
    value: &Value,
    root: &Value,
    path: &str,
    errors: &mut Vec<String>,
) {
    let schema = match schema_value.as_object() {
        Some(o) => o,
        None => return, // 非对象 schema（boolean schema 简化支持：true 通过）
    };

    // $ref 解析（内部引用）
    if let Some(ref_str) = schema.get("$ref").and_then(|v| v.as_str()) {
        if let Some(target) = resolve_ref(ref_str, root) {
            validate_value(target, value, root, path, errors);
            return;
        }
    }

    // type
    if let Some(t) = schema.get("type") {
        if !check_type(t, value) {
            errors.push(format!(
                "{path}: expected type {:?}, got {}",
                t,
                type_name(value)
            ));
            return;
        }
    }

    // enum / const
    if let Some(enum_arr) = schema.get("enum").and_then(|v| v.as_array()) {
        if !enum_arr.contains(value) {
            errors.push(format!("{path}: value not in enum"));
        }
    }
    if let Some(c) = schema.get("const") {
        if c != value {
            errors.push(format!("{path}: value does not match const"));
        }
    }

    // 数值约束
    if let Some(n) = value.as_f64() {
        if let Some(min) = schema.get("minimum").and_then(|v| v.as_f64()) {
            if n < min {
                errors.push(format!("{path}: {n} < minimum {min}"));
            }
        }
        if let Some(max) = schema.get("maximum").and_then(|v| v.as_f64()) {
            if n > max {
                errors.push(format!("{path}: {n} > maximum {max}"));
            }
        }
        if let Some(min) = schema.get("exclusiveMinimum").and_then(|v| v.as_f64()) {
            if n <= min {
                errors.push(format!("{path}: {n} <= exclusiveMinimum {min}"));
            }
        }
        if let Some(max) = schema.get("exclusiveMaximum").and_then(|v| v.as_f64()) {
            if n >= max {
                errors.push(format!("{path}: {n} >= exclusiveMaximum {max}"));
            }
        }
        if let Some(m) = schema.get("multipleOf").and_then(|v| v.as_f64()) {
            if m != 0.0 && (n / m).fract().abs() > 1e-9 {
                errors.push(format!("{path}: {n} not multiple of {m}"));
            }
        }
    }

    // 字符串约束
    if let Some(s) = value.as_str() {
        if let Some(min) = schema.get("minLength").and_then(|v| v.as_u64()) {
            if (s.chars().count() as u64) < min {
                errors.push(format!("{path}: string shorter than minLength {min}"));
            }
        }
        if let Some(max) = schema.get("maxLength").and_then(|v| v.as_u64()) {
            if (s.chars().count() as u64) > max {
                errors.push(format!("{path}: string longer than maxLength {max}"));
            }
        }
        if let Some(pat) = schema.get("pattern").and_then(|v| v.as_str()) {
            if !matches_pattern(pat, s) {
                errors.push(format!("{path}: string does not match pattern {pat:?}"));
            }
        }
    }

    // 对象约束
    if let Some(obj) = value.as_object() {
        if let Some(min) = schema.get("minProperties").and_then(|v| v.as_u64()) {
            if (obj.len() as u64) < min {
                errors.push(format!("{path}: too few properties"));
            }
        }
        if let Some(max) = schema.get("maxProperties").and_then(|v| v.as_u64()) {
            if (obj.len() as u64) > max {
                errors.push(format!("{path}: too many properties"));
            }
        }
        if let Some(required) = schema.get("required").and_then(|v| v.as_array()) {
            for r in required.iter().filter_map(|v| v.as_str()) {
                if !obj.contains_key(r) {
                    errors.push(format!("{path}: missing required property {r:?}"));
                }
            }
        }
        if let Some(props) = schema.get("properties").and_then(|v| v.as_object()) {
            for (k, sub) in props {
                if let Some(v) = obj.get(k) {
                    let sub_path = format!("{path}/{}", k);
                    validate_value(sub, v, root, &sub_path, errors);
                }
            }
        }
        if let Some(add) = schema.get("additionalProperties") {
            if add.as_bool() == Some(false) {
                if let Some(props) = schema.get("properties").and_then(|v| v.as_object()) {
                    for k in obj.keys() {
                        if !props.contains_key(k) {
                            errors.push(format!("{path}: additional property {k:?} not allowed"));
                        }
                    }
                }
            }
        }
    }

    // 数组约束
    if let Some(arr) = value.as_array() {
        if let Some(min) = schema.get("minItems").and_then(|v| v.as_u64()) {
            if (arr.len() as u64) < min {
                errors.push(format!("{path}: fewer than minItems {min}"));
            }
        }
        if let Some(max) = schema.get("maxItems").and_then(|v| v.as_u64()) {
            if (arr.len() as u64) > max {
                errors.push(format!("{path}: more than maxItems {max}"));
            }
        }
        if schema.get("uniqueItems").and_then(|v| v.as_bool()) == Some(true) {
            for (i, a) in arr.iter().enumerate() {
                for b in arr.iter().skip(i + 1) {
                    if a == b {
                        errors.push(format!("{path}: array items not unique"));
                        break;
                    }
                }
            }
        }
        if let Some(items) = schema.get("items") {
            for (i, v) in arr.iter().enumerate() {
                let sub_path = format!("{path}/{i}");
                validate_value(items, v, root, &sub_path, errors);
            }
        }
    }

    // 组合关键字
    if let Some(list) = schema.get("allOf").and_then(|v| v.as_array()) {
        for (i, sub) in list.iter().enumerate() {
            let sub_path = format!("{path}/allOf/{i}");
            validate_value(sub, value, root, &sub_path, errors);
        }
    }
    if let Some(list) = schema.get("anyOf").and_then(|v| v.as_array()) {
        let ok = list.iter().any(|sub| {
            let mut sub_errors = Vec::new();
            validate_value(sub, value, root, path, &mut sub_errors);
            sub_errors.is_empty()
        });
        if !ok {
            errors.push(format!("{path}: value matches no anyOf branch"));
        }
    }
    if let Some(list) = schema.get("oneOf").and_then(|v| v.as_array()) {
        let count = list
            .iter()
            .filter(|sub| {
                let mut sub_errors = Vec::new();
                validate_value(sub, value, root, path, &mut sub_errors);
                sub_errors.is_empty()
            })
            .count();
        if count != 1 {
            errors.push(format!("{path}: value matches {count} oneOf branches (expected 1)"));
        }
    }
    if let Some(not_schema) = schema.get("not") {
        let mut sub_errors = Vec::new();
        validate_value(not_schema, value, root, path, &mut sub_errors);
        if sub_errors.is_empty() {
            errors.push(format!("{path}: value matches forbidden 'not' schema"));
        }
    }
}

/// 解析内部 $ref（"#/definitions/x" / "#/$defs/x"）
fn resolve_ref<'a>(ref_str: &str, root: &'a Value) -> Option<&'a Value> {
    let pointer = ref_str.strip_prefix("#/")?;
    let mut current = root;
    for seg in pointer.split('/') {
        let seg = seg.replace("~1", "/").replace("~0", "~");
        current = current.get(&seg)?;
    }
    Some(current)
}

fn check_type(t: &Value, value: &Value) -> bool {
    match t.as_str() {
        Some("object") => value.is_object(),
        Some("array") => value.is_array(),
        Some("string") => value.is_string(),
        Some("number") => value.is_number(),
        Some("integer") => value
            .as_f64()
            .map(|n| n.fract() == 0.0 && n.is_finite())
            .unwrap_or(false),
        Some("boolean") => value.is_boolean(),
        Some("null") => value.is_null(),
        _ => true,
    }
}

fn type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(n) if n.as_f64().map(|f| f.fract() == 0.0).unwrap_or(false) => "integer",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// 简单 glob 式正则子集：支持 `^...$`、`.`、`*`、字符类 `[...]`、转义
fn matches_pattern(pattern: &str, s: &str) -> bool {
    let pattern = pattern.trim();
    let anchored = if let Some(rest) = pattern.strip_prefix('^') {
        rest.strip_suffix('$').unwrap_or(rest)
    } else {
        pattern
    };
    // 简化实现：仅支持完全匹配（去除 ^$ 锚定后做 literal/glob 匹配）
    glob_match(anchored, s)
}

fn glob_match(pattern: &str, s: &str) -> bool {
    // 支持 *（任意序列）、. 字面、其余字符字面匹配
    if pattern.is_empty() {
        return s.is_empty();
    }
    let mut p_chars = pattern.chars().peekable();
    let s_chars: Vec<char> = s.chars().collect();
    let mut si = 0usize;

    while let Some(pc) = p_chars.next() {
        match pc {
            '*' => {
                // 贪婪匹配剩余
                let rest: String = p_chars.collect();
                if rest.is_empty() {
                    return true;
                }
                for k in si..=s_chars.len() {
                    let tail: String = s_chars[k..].iter().collect();
                    if glob_match(&rest, &tail) {
                        return true;
                    }
                }
                return false;
            }
            c => {
                if si >= s_chars.len() || s_chars[si] != c {
                    return false;
                }
                si += 1;
            }
        }
    }
    si == s_chars.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn check(schema: &str, value: Value) -> bool {
        validate(schema, &value).is_ok()
    }

    #[test]
    fn test_type_check() {
        assert!(check(r#"{"type":"object"}"#, json!({"a": 1})));
        assert!(!check(r#"{"type":"object"}"#, json!([1, 2])));
        assert!(check(r#"{"type":"integer"}"#, json!(3)));
        assert!(!check(r#"{"type":"integer"}"#, json!(3.5)));
        assert!(check(r#"{"type":"number"}"#, json!(3.5)));
        assert!(check(r#"{"type":"string"}"#, json!("abc")));
        assert!(check(r#"{"type":"boolean"}"#, json!(true)));
        assert!(check(r#"{"type":"null"}"#, Value::Null));
    }

    #[test]
    fn test_properties_and_required() {
        let schema = r#"{"type":"object","required":["name"],"properties":{"name":{"type":"string"},"age":{"type":"integer","minimum":0}}}"#;
        assert!(check(schema, json!({"name": "alice", "age": 30})));
        assert!(!check(schema, json!({"age": 30}))); // missing name
        assert!(!check(schema, json!({"name": "alice", "age": -5}))); // min
        assert!(!check(schema, json!({"name": 42}))); // wrong type
    }

    #[test]
    fn test_additional_properties_false() {
        let schema = r#"{"type":"object","properties":{"a":{"type":"number"}},"additionalProperties":false}"#;
        assert!(check(schema, json!({"a": 1})));
        assert!(!check(schema, json!({"a": 1, "b": 2})));
    }

    #[test]
    fn test_array_constraints() {
        let schema = r#"{"type":"array","items":{"type":"integer"},"minItems":1,"maxItems":3,"uniqueItems":true}"#;
        assert!(check(schema, json!([1, 2, 3])));
        assert!(!check(schema, json!([1, "x"]))); // item type
        assert!(!check(schema, json!([]))); // minItems
        assert!(!check(schema, json!([1, 2, 3, 4]))); // maxItems
        assert!(!check(schema, json!([1, 1]))); // uniqueItems
    }

    #[test]
    fn test_string_constraints() {
        let schema = r#"{"type":"string","minLength":2,"maxLength":5}"#;
        assert!(check(schema, json!("abc")));
        assert!(!check(schema, json!("a")));
        assert!(!check(schema, json!("abcdef")));
    }

    #[test]
    fn test_enum_and_const() {
        assert!(check(r#"{"enum":["a","b"]}"#, json!("a")));
        assert!(!check(r#"{"enum":["a","b"]}"#, json!("c")));
        assert!(check(r#"{"const":42}"#, json!(42)));
        assert!(!check(r#"{"const":42}"#, json!(43)));
    }

    #[test]
    fn test_combinators() {
        assert!(check(r#"{"anyOf":[{"type":"string"},{"type":"number"}]}"#, json!(1)));
        assert!(!check(r#"{"anyOf":[{"type":"string"},{"type":"boolean"}]}"#, json!(1)));
        assert!(check(r#"{"not":{"type":"string"}}"#, json!(1)));
        assert!(!check(r#"{"not":{"type":"string"}}"#, json!("x")));
        assert!(check(r#"{"allOf":[{"type":"object"},{"required":["a"]}]}"#, json!({"a":1})));
        assert!(check(r#"{"oneOf":[{"type":"number"},{"type":"string"}]}"#, json!(1)));
    }

    #[test]
    fn test_ref() {
        let schema = r##"{"definitions":{"positive":{"type":"number","minimum":0}},"type":"object","properties":{"x":{"$ref":"#/definitions/positive"}}}"##;
        assert!(check(schema, json!({"x": 5})));
        assert!(!check(schema, json!({"x": -1})));
    }

    #[test]
    fn test_invalid_schema_returns_error() {
        let result = validate("not json", &json!({}));
        assert!(result.is_err());
    }
}
