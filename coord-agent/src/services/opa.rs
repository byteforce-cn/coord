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
        self.policy_sources.write().insert(policy_id.to_string(), rego.to_string());
        self.rebuild_engine()?;
        self.cache.write().clear();
        let count = self.policy_sources.read().len();
        tracing::info!("OPA: loaded policy '{policy_id}' ({count} total)");
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

    // ──── 内部方法 ────

    fn cache_key(package: &str, input: &OpaInput) -> String {
        format!("{}:{}:{}:{}", package, input.subject, input.action, input.resource)
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
}
