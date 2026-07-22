// coord-agent: 权限策略引擎 (Policy Service) — 安全层（Phase G）
//
// 实现 BaseService trait，提供基于规则的授权决策引擎（RBAC/ABAC）。
// 支持策略管理、条件匹配、优先级排序、通配符匹配。
// 设计为可扩展至 OPA Wasm 的策略决策点。
//
// Bundle 管理（Phase H）:
// - 策略包存储在 Server KV（`/_policy/bundles/` 前缀），多 Agent 共享
// - OpaEngine 负责本地 Rego 求值和 explain
// - PolicyService 负责 bundle CRUD（KV 读写）和 OpaEngine 策略同步
//
// 参见 docs/client-agent-architecture-v3.md §5.10。

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;

use crate::proxy::AgentInner;
use crate::service::{BaseService, ServiceResult};
use crate::services::opa::{OpaEngine, OpaConfig};

// ──── 公共类型 ────

/// 策略效果
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyEffect {
    Allow,
    Deny,
}

/// 策略条件
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PolicyCondition {
    pub attribute: String,
    pub operator: String,
    pub value: String,
}

/// 策略定义
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Policy {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub effect: PolicyEffect,
    pub subjects: Vec<String>,
    pub actions: Vec<String>,
    pub resources: Vec<String>,
    #[serde(default)]
    pub conditions: Vec<PolicyCondition>,
    pub priority: i32,
}

/// 访问请求
#[derive(Debug, Clone)]
pub struct AccessRequest {
    pub subject: String,
    pub action: String,
    pub resource: String,
    pub context: HashMap<String, String>,
}

/// 策略决策结果
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyDecision {
    pub effect: PolicyEffect,
    pub matched_policy_id: Option<String>,
    pub reason: String,
}

// ──── Bundle 类型 ────

/// 策略包信息（对外暴露，存储在 Server KV）
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BundleInfo {
    pub bundle_id: String,
    pub name: String,
    pub namespace: String,
    pub tenant_id: String,
    pub enabled: bool,
    pub created_at: i64,
    pub updated_at: i64,
}

/// 策略包完整内容（KV 存储格式）
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct BundleRecord {
    pub info: BundleInfo,
    pub rego_content: String,
}

impl BundleRecord {
    fn storage_key(bundle_id: &str) -> Vec<u8> {
        format!("/_policy/bundles/{bundle_id}").into_bytes()
    }

    fn prefix_key() -> Vec<u8> {
        b"/_policy/bundles/".to_vec()
    }

    fn make_bundle_id(tenant_id: &str, namespace: &str, name: &str) -> String {
        format!("{tenant_id}/{namespace}/{name}")
    }

    fn new(tenant_id: &str, namespace: &str, name: &str, rego: &str) -> Self {
        let bundle_id = Self::make_bundle_id(tenant_id, namespace, name);
        let now = unix_ts_i64();
        Self {
            info: BundleInfo {
                bundle_id,
                name: name.to_string(),
                namespace: namespace.to_string(),
                tenant_id: tenant_id.to_string(),
                enabled: true,
                created_at: now,
                updated_at: now,
            },
            rego_content: rego.to_string(),
        }
    }
}

fn unix_ts_i64() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

// ──── PolicyService ────

/// 权限策略引擎
///
/// 基于规则的授权决策点，支持 RBAC/ABAC 策略评估。
/// 集成 OpaEngine 以支持 Rego bundle 管理和 explain。
/// Bundle 存储在 Server KV，多 Agent 共享。
pub struct PolicyService {
    /// RBAC 策略（本地内存）
    policies: RwLock<BTreeMap<String, Policy>>,
    started: RwLock<bool>,
    max_policies: usize,
    /// OPA 引擎（Regorus），用于 Rego 求值和 explain
    opa_engine: Arc<OpaEngine>,
    /// Agent 内部句柄（访问 Server KV）
    inner: Option<Arc<AgentInner>>,
}

impl std::fmt::Debug for PolicyService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let p = self.policies.read();
        f.debug_struct("PolicyService")
            .field("policy_count", &p.len())
            .field("started", &self.started)
            .field("has_kv", &self.inner.is_some())
            .finish()
    }
}

impl PolicyService {
    /// 创建不带 KV 的 PolicyService（仅 RBAC 引擎）
    pub fn new(max_policies: usize) -> Self {
        let opa_engine = Arc::new(
            OpaEngine::new(OpaConfig::default())
                .expect("create OpaEngine")
        );
        Self {
            policies: RwLock::new(BTreeMap::new()),
            started: RwLock::new(false),
            max_policies,
            opa_engine,
            inner: None,
        }
    }

    /// 创建带 Server KV 接入的 PolicyService（支持 bundle CRUD）
    pub fn with_kv(max_policies: usize, inner: Arc<AgentInner>) -> Self {
        let opa_engine = Arc::new(
            OpaEngine::new(OpaConfig::default())
                .expect("create OpaEngine")
        );
        Self {
            policies: RwLock::new(BTreeMap::new()),
            started: RwLock::new(false),
            max_policies,
            opa_engine,
            inner: Some(inner),
        }
    }

    /// 获取 OPA 引擎引用
    pub fn opa_engine(&self) -> &Arc<OpaEngine> {
        &self.opa_engine
    }

    // ──── RBAC 策略管理 ────

    pub fn add_policy(&self, policy: Policy) -> ServiceResult<()> {
        let mut policies = self.policies.write();
        if policies.len() >= self.max_policies {
            return Err(format!("max policies ({}) reached", self.max_policies).into());
        }
        policies.insert(policy.id.clone(), policy);
        Ok(())
    }

    pub fn get_policy(&self, id: &str) -> ServiceResult<Option<Policy>> {
        let policies = self.policies.read();
        Ok(policies.get(id).cloned())
    }

    pub fn remove_policy(&self, id: &str) -> ServiceResult<bool> {
        let mut policies = self.policies.write();
        Ok(policies.remove(id).is_some())
    }

    pub fn list_policies(&self) -> ServiceResult<Vec<Policy>> {
        let policies = self.policies.read();
        Ok(policies.values().cloned().collect())
    }

    // ──── RBAC 策略评估 ────

    pub fn evaluate(&self, request: &AccessRequest) -> ServiceResult<PolicyDecision> {
        let policies = self.policies.read();
        let mut matches: Vec<&Policy> = policies
            .values()
            .filter(|p| self.policy_matches(p, request))
            .collect();
        matches.sort_by_key(|p| std::cmp::Reverse(p.priority));

        if let Some(deny) = matches.iter().find(|p| p.effect == PolicyEffect::Deny) {
            return Ok(PolicyDecision {
                effect: PolicyEffect::Deny,
                matched_policy_id: Some(deny.id.clone()),
                reason: format!("explicitly denied by policy '{}'", deny.name),
            });
        }

        if let Some(allow) = matches.iter().find(|p| p.effect == PolicyEffect::Allow) {
            return Ok(PolicyDecision {
                effect: PolicyEffect::Allow,
                matched_policy_id: Some(allow.id.clone()),
                reason: format!("allowed by policy '{}'", allow.name),
            });
        }

        Ok(PolicyDecision {
            effect: PolicyEffect::Deny,
            matched_policy_id: None,
            reason: "no matching policy (default deny)".into(),
        })
    }

    fn policy_matches(&self, policy: &Policy, request: &AccessRequest) -> bool {
        if !Self::match_any(&policy.subjects, &request.subject) { return false; }
        if !Self::match_any(&policy.actions, &request.action) { return false; }
        if !Self::match_any(&policy.resources, &request.resource) { return false; }
        for condition in &policy.conditions {
            if !Self::eval_condition(condition, &request.context) { return false; }
        }
        true
    }

    fn match_any(patterns: &[String], value: &str) -> bool {
        if patterns.is_empty() { return false; }
        patterns.iter().any(|p| Self::wildcard_match(p, value))
    }

    fn wildcard_match(pattern: &str, value: &str) -> bool {
        if pattern == "*" { return true; }
        if let Some(prefix) = pattern.strip_suffix('*') {
            return value.starts_with(prefix);
        }
        pattern == value
    }

    fn eval_condition(condition: &PolicyCondition, context: &HashMap<String, String>) -> bool {
        let attr_value = match context.get(&condition.attribute) {
            Some(v) => v,
            None => return false,
        };
        match condition.operator.as_str() {
            "eq" => attr_value == &condition.value,
            "neq" => attr_value != &condition.value,
            "contains" => attr_value.contains(&condition.value),
            "prefix" => attr_value.starts_with(&condition.value),
            "gte" | "lte" | "gt" | "lt" => {
                let a: f64 = match attr_value.parse() { Ok(v) => v, Err(_) => return false };
                let b: f64 = match condition.value.parse() { Ok(v) => v, Err(_) => return false };
                match condition.operator.as_str() {
                    "gte" => a >= b, "lte" => a <= b, "gt" => a > b, "lt" => a < b,
                    _ => false,
                }
            }
            _ => false,
        }
    }

    // ──── Bundle 管理（Server KV 后端）───

    fn require_kv(&self) -> ServiceResult<&Arc<AgentInner>> {
        self.inner.as_ref()
            .ok_or_else(|| "Policy bundle API requires AgentInner (server KV connection)".into())
    }

    /// 上传/更新 Rego 策略包到 Server KV
    pub async fn put_bundle(&self, tenant_id: &str, namespace: &str,
                            name: &str, rego: &str) -> ServiceResult<BundleInfo> {
        let inner = self.require_kv()?;
        let bundle_id = BundleRecord::make_bundle_id(tenant_id, namespace, name);
        let key = BundleRecord::storage_key(&bundle_id);

        // 尝试读取已有记录（upsert）
        let mut record = {
            let pairs = inner.client.kv()
                .range(&key, &key, 1, 0).await
                .map_err(|e| format!("kv range: {e}"))?;
            if let Some((_k, v)) = pairs.into_iter().next() {
                let mut rec: BundleRecord = serde_json::from_slice(&v)
                    .map_err(|e| format!("deserialize bundle: {e}"))?;
                rec.rego_content = rego.to_string();
                rec.info.updated_at = unix_ts_i64();
                rec
            } else {
                BundleRecord::new(tenant_id, namespace, name, rego)
            }
        };

        let value = serde_json::to_vec(&record)
            .map_err(|e| format!("serialize bundle: {e}"))?;
        inner.client.kv().put(&key, &value).await
            .map_err(|e| format!("kv put bundle: {e}"))?;

        // 同步到本地 OpaEngine（仅 enabled bundle）
        if record.info.enabled {
            let policy_id = format!("{}/{}", record.info.namespace, record.info.name);
            self.opa_engine.add_policy(&policy_id, &record.rego_content)
                .map_err(|e| format!("opa add_policy: {e}"))?;
        }

        tracing::info!("Policy: put bundle '{}' (tenant={}, ns={})", name, tenant_id, namespace);
        Ok(record.info)
    }

    /// 从 Server KV 删除策略包
    pub async fn delete_bundle(&self, bundle_id: &str) -> ServiceResult<bool> {
        let inner = self.require_kv()?;
        let key = BundleRecord::storage_key(bundle_id);

        // 先读取 bundle 信息用于清理 OpaEngine
        let namespace_and_name = {
            let pairs = inner.client.kv()
                .range(&key, &key, 1, 0).await
                .map_err(|e| format!("kv range: {e}"))?;
            if let Some((_k, v)) = pairs.into_iter().next() {
                let rec: BundleRecord = serde_json::from_slice(&v)
                    .map_err(|e| format!("deserialize bundle: {e}"))?;
                Some((rec.info.namespace, rec.info.name))
            } else {
                None
            }
        };

        inner.client.kv().delete(&key).await
            .map_err(|e| format!("kv delete bundle: {e}"))?;

        // 从本地 OpaEngine 移除
        if let Some((ns, name)) = namespace_and_name {
            let policy_id = format!("{}/{}", ns, name);
            self.opa_engine.remove_policy(&policy_id);
        }

        tracing::info!("Policy: deleted bundle '{}'", bundle_id);
        Ok(true)
    }

    /// 列出策略包（从 Server KV range scan）
    pub async fn list_bundles(&self, tenant_id: Option<&str>) -> ServiceResult<Vec<BundleInfo>> {
        let inner = self.require_kv()?;
        let prefix = BundleRecord::prefix_key();
        let range_end = prefix_end(&prefix);

        let pairs = inner.client.kv()
            .range(&prefix, &range_end, 0, 0).await
            .map_err(|e| format!("kv range: {e}"))?;

        let mut bundles: Vec<BundleInfo> = Vec::new();
        for (_k, v) in pairs {
            if let Ok(rec) = serde_json::from_slice::<BundleRecord>(&v) {
                if tenant_id.map_or(true, |tid| rec.info.tenant_id == tid) {
                    bundles.push(rec.info);
                }
            }
        }
        Ok(bundles)
    }

    /// 启用/禁用策略包（更新 Server KV）
    pub async fn set_bundle_enabled(&self, bundle_id: &str, enabled: bool) -> ServiceResult<bool> {
        let inner = self.require_kv()?;
        let key = BundleRecord::storage_key(bundle_id);

        let pairs = inner.client.kv()
            .range(&key, &key, 1, 0).await
            .map_err(|e| format!("kv range: {e}"))?;

        let (_k, v) = pairs.into_iter().next()
            .ok_or_else(|| format!("bundle '{bundle_id}' not found"))?;

        let mut rec: BundleRecord = serde_json::from_slice(&v)
            .map_err(|e| format!("deserialize bundle: {e}"))?;
        rec.info.enabled = enabled;
        rec.info.updated_at = unix_ts_i64();

        let new_val = serde_json::to_vec(&rec)
            .map_err(|e| format!("serialize bundle: {e}"))?;
        inner.client.kv().put(&key, &new_val).await
            .map_err(|e| format!("kv put bundle: {e}"))?;

        // 同步到本地 OpaEngine
        let policy_id = format!("{}/{}", rec.info.namespace, rec.info.name);
        if enabled {
            self.opa_engine.add_policy(&policy_id, &rec.rego_content)
                .map_err(|e| format!("opa add_policy: {e}"))?;
        } else {
            self.opa_engine.remove_policy(&policy_id);
        }

        tracing::info!("Policy: bundle '{}' enabled={}", bundle_id, enabled);
        Ok(true)
    }

    /// 解释策略决策（本地 OpaEngine）
    pub fn explain(&self, query: &str, input_json: &str) -> ServiceResult<String> {
        self.opa_engine.explain(query, input_json)
            .map_err(|e| e.into())
    }
}

// ──── BaseService trait ────

#[async_trait]
impl BaseService for PolicyService {
    fn name(&self) -> &'static str {
        "policy"
    }

    async fn start(&self) -> ServiceResult<()> {
        *self.started.write() = true;
        tracing::info!("PolicyService started");
        Ok(())
    }

    async fn stop(&self) -> ServiceResult<()> {
        *self.started.write() = false;
        self.policies.write().clear();
        tracing::info!("PolicyService stopped");
        Ok(())
    }

    fn health_check(&self) -> bool {
        *self.started.read()
    }
}

// ──── 工具函数 ────

/// 生成 range_end: 将 prefix 最后一个字节 +1
fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    for i in (0..end.len()).rev() {
        if end[i] < 0xff {
            end[i] += 1;
            end.truncate(i + 1);
            return end;
        }
    }
    // prefix 全为 0xff，返回空表示扫描到无穷
    vec![]
}

// ──── 单元测试 ────

#[cfg(test)]
mod tests {
    use super::*;

    fn new_svc() -> PolicyService {
        PolicyService::new(1024)
    }

    #[test]
    fn test_policy_add_and_evaluate() {
        let svc = new_svc();
        svc.add_policy(Policy {
            id: "p1".into(),
            name: "admin-access".into(),
            description: "".into(),
            effect: PolicyEffect::Allow,
            subjects: vec!["role:admin".into()],
            actions: vec!["*".into()],
            resources: vec!["*".into()],
            conditions: vec![],
            priority: 10,
        }).unwrap();

        let req = AccessRequest {
            subject: "role:admin".into(),
            action: "read".into(),
            resource: "/data".into(),
            context: HashMap::new(),
        };
        let decision = svc.evaluate(&req).unwrap();
        assert_eq!(decision.effect, PolicyEffect::Allow);
    }

    #[test]
    fn test_deny_overrides_allow() {
        let svc = new_svc();
        svc.add_policy(Policy {
            id: "allow-all".into(), name: "a".into(), description: "".into(),
            effect: PolicyEffect::Allow, subjects: vec!["*".into()],
            actions: vec!["*".into()], resources: vec!["*".into()],
            conditions: vec![], priority: 1,
        }).unwrap();
        svc.add_policy(Policy {
            id: "deny-bob".into(), name: "d".into(), description: "".into(),
            effect: PolicyEffect::Deny, subjects: vec!["user:bob".into()],
            actions: vec!["*".into()], resources: vec!["*".into()],
            conditions: vec![], priority: 100,
        }).unwrap();

        let req = AccessRequest {
            subject: "user:bob".into(), action: "read".into(),
            resource: "/data".into(), context: HashMap::new(),
        };
        let decision = svc.evaluate(&req).unwrap();
        assert_eq!(decision.effect, PolicyEffect::Deny);
    }

    #[test]
    fn test_default_deny() {
        let svc = new_svc();
        let req = AccessRequest {
            subject: "unknown".into(), action: "read".into(),
            resource: "/data".into(), context: HashMap::new(),
        };
        let decision = svc.evaluate(&req).unwrap();
        assert_eq!(decision.effect, PolicyEffect::Deny);
    }

    #[test]
    fn test_bundle_id_format() {
        let id = BundleRecord::make_bundle_id("tenant-1", "default", "my-policy");
        assert_eq!(id, "tenant-1/default/my-policy");
    }

    #[test]
    fn test_storage_key_format() {
        let key = BundleRecord::storage_key("tenant-1/default/my-policy");
        assert_eq!(String::from_utf8_lossy(&key), "/_policy/bundles/tenant-1/default/my-policy");
    }
}
