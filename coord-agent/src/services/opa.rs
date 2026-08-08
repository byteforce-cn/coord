// coord-agent: Regorus OPA 引擎集成 (OPA Engine)
//
// 基于 Regorus（Rust 原生 OPA 引擎）的策略评估引擎。
// 直接加载 Rego 策略进行本地评估，零 Wasm 依赖。
//
// 架构（v8.2 §4.11）:
// - Agent 内嵌 Regorus，直接加载 Rego 策略进行本地评估
// - 策略包由 Server 通过 KV/Watch 下发至 Agent
// - 评估结果缓存 30 秒，策略版本变更时立即失效
// - Bundle 存储由 PolicyService 管理（Server KV），OpaEngine 仅负责本地求值
//
// 参见 docs/client-agent-architecture.v8.2.md §4.11。

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::RwLock;
use regorus::{Engine, Value};

// ──── 公共类型 ────

/// OPA 引擎配置
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OpaConfig {
    /// 评估结果缓存 TTL（秒），默认 30
    pub cache_ttl_secs: u64,
    /// 最大策略文件数
    pub max_policies: usize,
}

impl Default for OpaConfig {
    fn default() -> Self {
        Self {
            cache_ttl_secs: 30,
            max_policies: 256,
        }
    }
}

/// OPA 评估输入
#[derive(Debug, Clone, Default)]
pub struct OpaInput {
    pub subject: String,
    pub action: String,
    pub resource: String,
    /// 上下文属性（如 IP、时间、租户 ID 等）
    pub context: HashMap<String, String>,
}

/// OPA 评估决策
#[derive(Debug, Clone)]
pub struct OpaDecision {
    /// 是否允许
    pub allowed: bool,
    /// 匹配的规则名列表
    pub matched_rules: Vec<String>,
    /// 决策原因
    pub reason: String,
}

// ──── 缓存条目 ────

#[derive(Clone)]
struct CacheEntry {
    decision: OpaDecision,
    cached_at: Instant,
}

// ──── OpaEngine ────

/// Regorus OPA 策略评估引擎
///
/// 线程安全，内建评估结果缓存。
/// 策略加载由外部（PolicyService）管理。
pub struct OpaEngine {
    /// Regorus 引擎实例（受 RwLock 保护）
    engine: RwLock<Engine>,
    /// 评估结果缓存
    cache: RwLock<HashMap<String, CacheEntry>>,
    /// 已加载的策略源码：policy_id → rego source
    policy_sources: RwLock<HashMap<String, String>>,
    /// 配置
    config: OpaConfig,
}

impl OpaEngine {
    pub fn new(config: OpaConfig) -> Result<Self, String> {
        let engine = Engine::new();
        Ok(Self {
            engine: RwLock::new(engine),
            cache: RwLock::new(HashMap::new()),
            policy_sources: RwLock::new(HashMap::new()),
            config,
        })
    }

    /// 加载/更新 Rego 策略
    pub fn add_policy(&self, policy_id: &str, rego: &str) -> Result<(), String> {
        // 先编译校验：解析失败的源码不得进入 policy_sources，
        // 避免残留坏源码毒化后续 rebuild_engine。
        let mut probe = Engine::new();
        probe
            .add_policy(policy_id.to_string(), rego.to_string())
            .map_err(|e| format!("OPA policy parse error in '{policy_id}': {e}"))?;

        self.policy_sources.write().insert(policy_id.to_string(), rego.to_string());
        self.rebuild_engine()?;
        self.cache.write().clear();
        let count = self.policy_sources.read().len();
        tracing::info!("OPA: loaded policy '{policy_id}' ({count} total)");
        Ok(())
    }

    /// 仅编译校验 Rego 源码（不加载到引擎）。
    /// 用于 bundle 写入前校验，失败时保证不落库、不改动引擎状态。
    pub fn validate_rego(&self, rego: &str) -> Result<(), String> {
        let mut probe = Engine::new();
        probe
            .add_policy("_validate".to_string(), rego.to_string())
            .map_err(|e| format!("OPA policy parse error: {e}"))?;
        Ok(())
    }

    /// 移除策略
    pub fn remove_policy(&self, policy_id: &str) {
        self.policy_sources.write().remove(policy_id);
        if let Err(e) = self.rebuild_engine() {
            tracing::warn!("OPA: rebuild after remove_policy failed: {e}");
        }
        self.cache.write().clear();
    }

    /// 清空所有策略
    pub fn clear_policies(&self) {
        self.policy_sources.write().clear();
        *self.engine.write() = Engine::new();
        self.cache.write().clear();
    }

    /// 批量加载策略
    pub fn load_policies(&self, policies: &[(String, String)]) -> Result<(), String> {
        {
            let mut sources = self.policy_sources.write();
            sources.clear();
            for (id, rego) in policies {
                sources.insert(id.clone(), rego.clone());
            }
        }
        self.rebuild_engine()?;
        self.cache.write().clear();
        tracing::info!("OPA: loaded {} policies", policies.len());
        Ok(())
    }

    /// 重建 Regorus 引擎
    pub fn rebuild_engine(&self) -> Result<(), String> {
        let mut engine = Engine::new();
        let sources = self.policy_sources.read();
        for (id, rego) in sources.iter() {
            engine
                .add_policy(id.clone(), rego.clone())
                .map_err(|e| format!("OPA policy parse error in '{id}': {e}"))?;
        }
        if sources.len() > self.config.max_policies {
            return Err(format!(
                "policy count {} exceeds max {}",
                sources.len(),
                self.config.max_policies
            ));
        }
        *self.engine.write() = engine;
        Ok(())
    }

    /// 评估访问请求
    pub fn evaluate(&self, package: &str, input: &OpaInput) -> Result<OpaDecision, String> {
        let cache_key = Self::cache_key(package, input);
        {
            let cache = self.cache.read();
            if let Some(entry) = cache.get(&cache_key) {
                if entry.cached_at.elapsed().as_secs() < self.config.cache_ttl_secs {
                    return Ok(entry.decision.clone());
                }
            }
        }

        let input_value = Self::build_input(input);
        let mut engine = self.engine.write();
        engine.set_input(input_value);

        let query = format!("data.{package}.allow");
        let allowed = engine
            .eval_bool_query(query, false)
            .map_err(|e| format!("OPA evaluation error: {e}"))?;

        let matched_rules = if allowed {
            vec!["allow".to_string()]
        } else {
            vec![]
        };

        let reason = if allowed {
            format!("allowed by rule 'allow' in package '{package}'")
        } else {
            "no matching rule (default deny)".to_string()
        };

        let decision = OpaDecision { allowed, matched_rules, reason };

        {
            let mut cache = self.cache.write();
            cache.insert(cache_key, CacheEntry {
                decision: decision.clone(),
                cached_at: Instant::now(),
            });
        }

        Ok(decision)
    }

    /// 清空评估缓存
    pub fn invalidate_cache(&self) {
        self.cache.write().clear();
    }

    /// 已加载策略数
    pub fn policy_count(&self) -> usize {
        self.policy_sources.read().len()
    }

    // ──── Explain ────

    /// 解释策略决策（trace），返回 JSON 格式
    pub fn explain(&self, query: &str, input_json: &str) -> Result<String, String> {
        let input_value: Value = serde_json::from_str(input_json)
            .map_err(|e| format!("invalid input JSON: {e}"))?;

        let mut engine = self.engine.write();
        engine.set_input(input_value);

        let allowed = engine
            .eval_bool_query(query.to_string(), false)
            .map_err(|e| format!("OPA evaluation error: {e}"))?;

        let matched: Vec<&str> = if allowed { vec!["allow"] } else { vec![] };
        let trace = serde_json::json!({
            "query": query,
            "input": serde_json::from_str::<serde_json::Value>(input_json).unwrap_or(serde_json::Value::Null),
            "result": allowed,
            "matched_rules": matched,
            "reason": if allowed {
                format!("query '{}' evaluated to true", query)
            } else {
                format!("query '{}' evaluated to false (default deny)", query)
            },
        });

        serde_json::to_string_pretty(&trace)
            .map_err(|e| format!("trace serialization error: {e}"))
    }

    /// 通用 Rego 求值：支持 `data.<package>.<rule>` 与纯表达式，返回 JSON 可序列化的结果值。
    ///
    /// 语义约定（与 CheckPermission 的 error/deny 区分）：
    /// - 求值错误（语法/输入/运行时）→ `Err`
    /// - 无匹配结果 → `Ok(Null)`（相当于 deny/未定义）
    /// - 单结果单表达式 → 返回该表达式值（如 `false` / 对象 `{field,operator,value}`）
    /// - 多结果或多表达式 → 返回数组
    ///
    /// 注意：原始求值不做结果缓存（query/input 任意，缓存键无法稳定界定）。
    pub fn eval_query(&self, query: &str, input_json: &str) -> Result<Value, String> {
        let input_value: Value = serde_json::from_str(input_json)
            .map_err(|e| format!("invalid input JSON: {e}"))?;

        let mut engine = self.engine.write();
        engine.set_input(input_value);

        let results = engine
            .eval_query(query.to_string(), false)
            .map_err(|e| format!("OPA evaluation error: {e}"))?;

        if results.result.is_empty() {
            return Ok(Value::Null);
        }

        let first = &results.result[0];
        if first.expressions.len() == 1 {
            return Ok(first.expressions[0].value.clone());
        }

        let vals: Vec<Value> = first.expressions.iter().map(|e| e.value.clone()).collect();
        Ok(Value::Array(vals.into()))
    }

    // ──── 内部方法 ────

    fn cache_key(package: &str, input: &OpaInput) -> String {
        // context 必须纳入缓存键：相同 subject/action/resource 但不同 context
        // （如 IP/租户/时间等 ABAC 属性）会得出不同决策，漏掉会导致错误命中。
        let mut ctx_pairs: Vec<(String, String)> = input
            .context
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        ctx_pairs.sort();
        let ctx = ctx_pairs
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        format!("{}:{}:{}:{}:{}", package, input.subject, input.action, input.resource, ctx)
    }

    fn build_input(input: &OpaInput) -> Value {
        let mut map: BTreeMap<Value, Value> = BTreeMap::new();
        map.insert(Value::String(Arc::<str>::from("subject")), Value::String(Arc::<str>::from(input.subject.as_str())));
        map.insert(Value::String(Arc::<str>::from("action")), Value::String(Arc::<str>::from(input.action.as_str())));
        map.insert(Value::String(Arc::<str>::from("resource")), Value::String(Arc::<str>::from(input.resource.as_str())));
        let mut ctx: BTreeMap<Value, Value> = BTreeMap::new();
        for (k, v) in &input.context {
            ctx.insert(Value::String(Arc::<str>::from(k.as_str())), Value::String(Arc::<str>::from(v.as_str())));
        }
        map.insert(Value::String(Arc::<str>::from("context")), Value::Object(Arc::new(ctx)));
        Value::Object(Arc::new(map))
    }
}

// ──── tests ────

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_REGO: &str = r#"
package test.rbac

default allow = false

allow if {
    input.subject == "alice"
    input.action == "read"
}
"#;

    #[test]
    fn test_opa_config_defaults() {
        let config = OpaConfig::default();
        assert_eq!(config.cache_ttl_secs, 30);
        assert_eq!(config.max_policies, 256);
    }

    #[test]
    fn test_opa_engine_creation() {
        let engine = OpaEngine::new(OpaConfig::default()).expect("create engine");
        assert_eq!(engine.policy_count(), 0);
    }

    #[test]
    fn test_build_input_basic() {
        let input = OpaInput {
            subject: "alice".into(),
            action: "read".into(),
            resource: "/data".into(),
            context: HashMap::new(),
        };
        let value = OpaEngine::build_input(&input);
        match &value {
            Value::Object(map) => {
                let subj_key = Value::String(Arc::<str>::from("subject"));
                let subj = map.get(&subj_key).unwrap();
                match subj {
                    Value::String(s) => assert_eq!(s.as_ref(), "alice"),
                    _ => panic!("expected string"),
                }
            }
            _ => panic!("expected object"),
        }
    }

    #[test]
    fn test_cache_key_deterministic() {
        let input = OpaInput { subject: "bob".into(), action: "write".into(), resource: "/admin".into(), context: Default::default() };
        let key1 = OpaEngine::cache_key("coord.auth", &input);
        let key2 = OpaEngine::cache_key("coord.auth", &input);
        assert_eq!(key1, key2);
    }

    #[test]
    fn test_cache_key_differs_by_package() {
        let input = OpaInput::default();
        let key1 = OpaEngine::cache_key("pkg.a", &input);
        let key2 = OpaEngine::cache_key("pkg.b", &input);
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_add_policy_and_evaluate() {
        let engine = OpaEngine::new(OpaConfig::default()).expect("create engine");
        engine.add_policy("test.rbac", TEST_REGO).expect("add policy");
        assert_eq!(engine.policy_count(), 1);
        let input = OpaInput { subject: "alice".into(), action: "read".into(), resource: "/data".into(), context: HashMap::new() };
        let decision = engine.evaluate("test.rbac", &input).expect("evaluate");
        assert!(decision.allowed);
    }

    #[test]
    fn test_evaluate_deny() {
        let engine = OpaEngine::new(OpaConfig::default()).expect("create engine");
        engine.add_policy("test.rbac", TEST_REGO).expect("add policy");
        let input = OpaInput { subject: "bob".into(), action: "write".into(), resource: "/data".into(), context: HashMap::new() };
        let decision = engine.evaluate("test.rbac", &input).expect("evaluate");
        assert!(!decision.allowed);
    }

    #[test]
    fn test_remove_policy() {
        let engine = OpaEngine::new(OpaConfig::default()).expect("create engine");
        engine.add_policy("test.rbac", TEST_REGO).expect("add policy");
        assert_eq!(engine.policy_count(), 1);
        engine.remove_policy("test.rbac");
        assert_eq!(engine.policy_count(), 0);
    }

    #[test]
    fn test_clear_policies() {
        let engine = OpaEngine::new(OpaConfig::default()).expect("create engine");
        engine.add_policy("p1", TEST_REGO).expect("add p1");
        engine.add_policy("p2", TEST_REGO).expect("add p2");
        assert_eq!(engine.policy_count(), 2);
        engine.clear_policies();
        assert_eq!(engine.policy_count(), 0);
    }

    #[test]
    fn test_load_policies_batch() {
        let engine = OpaEngine::new(OpaConfig::default()).expect("create engine");
        let policies = vec![
            ("p1".to_string(), TEST_REGO.to_string()),
            ("p2".to_string(), TEST_REGO.to_string()),
        ];
        engine.load_policies(&policies).expect("load policies");
        assert_eq!(engine.policy_count(), 2);
    }

    #[test]
    fn test_explain_returns_json_trace() {
        let engine = OpaEngine::new(OpaConfig::default()).expect("create engine");
        engine.add_policy("test.rbac", TEST_REGO).unwrap();
        let input = r#"{"subject": "alice", "action": "read"}"#;
        let trace = engine.explain("data.test.rbac.allow", input).expect("explain");
        let parsed: serde_json::Value = serde_json::from_str(&trace).expect("valid json");
        assert_eq!(parsed["query"], "data.test.rbac.allow");
        assert_eq!(parsed["result"], true);
        assert!(!parsed["matched_rules"].as_array().unwrap().is_empty());
    }

    #[test]
    fn test_explain_deny() {
        let engine = OpaEngine::new(OpaConfig::default()).expect("create engine");
        engine.add_policy("test.rbac", TEST_REGO).unwrap();
        let input = r#"{"subject": "bob", "action": "write"}"#;
        let trace = engine.explain("data.test.rbac.allow", input).expect("explain");
        let parsed: serde_json::Value = serde_json::from_str(&trace).expect("valid json");
        assert_eq!(parsed["result"], false);
        assert!(parsed["matched_rules"].as_array().unwrap().is_empty());
    }

    #[test]
    fn test_explain_invalid_input() {
        let engine = OpaEngine::new(OpaConfig::default()).expect("create engine");
        let result = engine.explain("data.test.rbac.allow", "not-json");
        assert!(result.is_err());
    }

    // ──── eval_query（raw rego 求值）────

    fn to_json(v: &Value) -> serde_json::Value {
        serde_json::to_value(v).expect("serialize value")
    }

    const FILTER_REGO: &str = r#"
package filter

default conditions := {}

conditions := {"field": "dept", "operator": "eq", "value": input.dept} if {
    input.enabled == true
}
"#;

    #[test]
    fn test_eval_query_data_rule_bool() {
        let engine = OpaEngine::new(OpaConfig::default()).expect("create engine");
        engine.add_policy("test.rbac", TEST_REGO).unwrap();
        let result = engine.eval_query("data.test.rbac.allow", r#"{"subject":"alice","action":"read"}"#).expect("eval");
        assert_eq!(to_json(&result), serde_json::json!(true));
    }

    #[test]
    fn test_eval_query_deny_is_false() {
        let engine = OpaEngine::new(OpaConfig::default()).expect("create engine");
        engine.add_policy("test.rbac", TEST_REGO).unwrap();
        let result = engine.eval_query("data.test.rbac.allow", r#"{"subject":"bob","action":"write"}"#).expect("eval");
        assert_eq!(to_json(&result), serde_json::json!(false));
    }

    #[test]
    fn test_eval_query_pure_expression() {
        let engine = OpaEngine::new(OpaConfig::default()).expect("create engine");
        // 纯表达式（不依赖策略包）
        let result = engine.eval_query("1 + 1 == 2", "{}").expect("eval");
        assert_eq!(to_json(&result), serde_json::json!(true));
    }

    #[test]
    fn test_eval_query_object_result() {
        // 结构化条件生成（{field, operator, value}）用例
        let engine = OpaEngine::new(OpaConfig::default()).expect("create engine");
        engine.add_policy("filter.rego", FILTER_REGO).unwrap();
        let result = engine.eval_query("data.filter.conditions", r#"{"enabled":true,"dept":"sales"}"#).expect("eval");
        assert_eq!(
            to_json(&result),
            serde_json::json!({"field": "dept", "operator": "eq", "value": "sales"})
        );
    }

    #[test]
    fn test_eval_query_no_match_returns_null() {
        // 无匹配结果 = deny/未定义 → null（与错误可区分）
        let engine = OpaEngine::new(OpaConfig::default()).expect("create engine");
        engine.add_policy("filter.rego", FILTER_REGO).unwrap();
        // 查询不存在的 rule（无 default）→ 空结果 → null
        let result = engine.eval_query("data.filter.undefined_rule", r#"{"enabled":true}"#).expect("eval");
        assert_eq!(to_json(&result), serde_json::Value::Null);
    }

    #[test]
    fn test_eval_query_invalid_input_errors() {
        let engine = OpaEngine::new(OpaConfig::default()).expect("create engine");
        let result = engine.eval_query("data.test.rbac.allow", "not-json");
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_query_bad_query_errors() {
        let engine = OpaEngine::new(OpaConfig::default()).expect("create engine");
        let result = engine.eval_query("this is not rego!!!", "{}");
        assert!(result.is_err());
    }

    // ──── validate_rego / 编译前置校验 ────

    #[test]
    fn test_validate_rego_ok() {
        let engine = OpaEngine::new(OpaConfig::default()).expect("create engine");
        assert!(engine.validate_rego(TEST_REGO).is_ok());
    }

    #[test]
    fn test_validate_rego_rejects_invalid() {
        let engine = OpaEngine::new(OpaConfig::default()).expect("create engine");
        assert!(engine.validate_rego("package broken\nallow if {").is_err());
    }

    #[test]
    fn test_add_policy_parse_error_does_not_poison() {
        // 回归：解析失败的源码不得残留 policy_sources，否则毒化后续 rebuild
        let engine = OpaEngine::new(OpaConfig::default()).expect("create engine");
        assert!(engine.add_policy("bad.rego", "package broken\nallow if {").is_err());
        assert_eq!(engine.policy_count(), 0);

        // 修复后仍可正常加载（evaluate 需用 Rego 声明的 package 名）
        engine.add_policy("good.rego", TEST_REGO).expect("add good");
        assert_eq!(engine.policy_count(), 1);
        let input = OpaInput { subject: "alice".into(), action: "read".into(), resource: "/data".into(), context: HashMap::new() };
        assert!(engine.evaluate("test.rbac", &input).unwrap().allowed);
    }

    // ──── 缓存键正确性（context 必须纳入）────

    #[test]
    fn test_cache_key_includes_context() {
        // 相同 subject/action/resource、不同 context → 必须不同缓存键（避免错误命中）
        let base = OpaInput { subject: "bob".into(), action: "read".into(), resource: "/data".into(), context: Default::default() };
        let mut with_ip = base.clone();
        with_ip.context.insert("ip".into(), "10.0.0.1".into());
        let mut other_ip = base.clone();
        other_ip.context.insert("ip".into(), "10.0.0.2".into());

        let k1 = OpaEngine::cache_key("pkg", &base);
        let k2 = OpaEngine::cache_key("pkg", &with_ip);
        let k3 = OpaEngine::cache_key("pkg", &other_ip);
        assert_ne!(k1, k2);
        assert_ne!(k2, k3);
    }
}
