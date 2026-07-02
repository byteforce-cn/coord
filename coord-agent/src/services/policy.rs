// coord-agent: 权限策略引擎 (Policy Service) — 安全层（Phase G）
//
// 实现 BaseService trait，提供基于规则的授权决策引擎（RBAC/ABAC）。
// 支持策略管理、条件匹配、优先级排序、通配符匹配。
// 设计为可扩展至 OPA Wasm 的策略决策点。
//
// 架构（v3.0）:
// - 本地规则引擎（条件匹配 + 优先级排序）
// - Server 存储策略包，通过 Watch 同步
// - 支持透明拦截（自动检查 gRPC 请求）和显式 API
//
// 参见 docs/client-agent-architecture-v3.md §5.10。

use std::collections::{BTreeMap, HashMap};

use async_trait::async_trait;
use parking_lot::RwLock;

use crate::service::{BaseService, ServiceResult};

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
    /// 条件属性名（从 AccessRequest.context 取值）
    pub attribute: String,
    /// 操作符: eq, neq, gte, lte, gt, lt, contains, prefix
    pub operator: String,
    /// 比较值
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
    /// 主体列表（支持通配符 *）: "role:admin", "user:alice", "*"
    pub subjects: Vec<String>,
    /// 操作列表（支持通配符 *）: "read", "write", "*"
    pub actions: Vec<String>,
    /// 资源列表（支持通配符 *）: "/api/*", "/admin/users", "*"
    pub resources: Vec<String>,
    /// 条件列表（AND 关系）
    #[serde(default)]
    pub conditions: Vec<PolicyCondition>,
    /// 优先级（数值越大优先级越高）
    pub priority: i32,
}

/// 访问请求
#[derive(Debug, Clone)]
pub struct AccessRequest {
    pub subject: String,
    pub action: String,
    pub resource: String,
    /// 上下文属性（用于条件匹配）
    pub context: HashMap<String, String>,
}

/// 策略决策结果
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyDecision {
    pub effect: PolicyEffect,
    pub matched_policy_id: Option<String>,
    pub reason: String,
}

// ──── PolicyService ────

/// 权限策略引擎
///
/// 基于规则的授权决策点，支持 RBAC/ABAC 策略评估。
pub struct PolicyService {
    policies: RwLock<BTreeMap<String, Policy>>,
    started: RwLock<bool>,
    max_policies: usize,
}

impl std::fmt::Debug for PolicyService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let p = self.policies.read();
        f.debug_struct("PolicyService")
            .field("policy_count", &p.len())
            .field("started", &self.started)
            .finish()
    }
}

impl PolicyService {
    pub fn new(max_policies: usize) -> Self {
        Self {
            policies: RwLock::new(BTreeMap::new()),
            started: RwLock::new(false),
            max_policies,
        }
    }

    // ──── 策略管理 ────

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

    // ──── 策略评估 ────

    /// 评估访问请求，返回决策（Allow/Deny）
    ///
    /// 算法：
    /// 1. 收集所有匹配的策略
    /// 2. 按优先级降序排列
    /// 3. 若存在 Deny 匹配则返回 Deny（显式拒绝优先）
    /// 4. 若存在 Allow 匹配则返回 Allow
    /// 5. 默认 Deny
    pub fn evaluate(&self, request: &AccessRequest) -> ServiceResult<PolicyDecision> {
        let policies = self.policies.read();

        // 收集匹配的策略
        let mut matches: Vec<&Policy> = policies
            .values()
            .filter(|p| self.policy_matches(p, request))
            .collect();

        // 按优先级降序
        matches.sort_by_key(|p| std::cmp::Reverse(p.priority));

        // Deny 优先：只要有一个 Deny 匹配就拒绝
        if let Some(deny) = matches.iter().find(|p| p.effect == PolicyEffect::Deny) {
            return Ok(PolicyDecision {
                effect: PolicyEffect::Deny,
                matched_policy_id: Some(deny.id.clone()),
                reason: format!("explicitly denied by policy '{}'", deny.name),
            });
        }

        // 查找 Allow
        if let Some(allow) = matches.iter().find(|p| p.effect == PolicyEffect::Allow) {
            return Ok(PolicyDecision {
                effect: PolicyEffect::Allow,
                matched_policy_id: Some(allow.id.clone()),
                reason: format!("allowed by policy '{}'", allow.name),
            });
        }

        // 默认 Deny
        Ok(PolicyDecision {
            effect: PolicyEffect::Deny,
            matched_policy_id: None,
            reason: "no matching policy (default deny)".into(),
        })
    }

    /// 检查策略是否匹配请求
    fn policy_matches(&self, policy: &Policy, request: &AccessRequest) -> bool {
        // 主体匹配
        if !Self::match_any(&policy.subjects, &request.subject) {
            return false;
        }
        // 操作匹配
        if !Self::match_any(&policy.actions, &request.action) {
            return false;
        }
        // 资源匹配
        if !Self::match_any(&policy.resources, &request.resource) {
            return false;
        }
        // 条件匹配（AND）
        for condition in &policy.conditions {
            if !Self::eval_condition(condition, &request.context) {
                return false;
            }
        }
        true
    }

    /// 通配符匹配：检查 value 是否匹配 patterns 中的任意一项
    fn match_any(patterns: &[String], value: &str) -> bool {
        if patterns.is_empty() {
            return false;
        }
        patterns.iter().any(|p| Self::wildcard_match(p, value))
    }

    /// 通配符匹配: "*" 匹配一切, "prefix*" 前缀匹配, 否则精确匹配
    fn wildcard_match(pattern: &str, value: &str) -> bool {
        if pattern == "*" {
            return true;
        }
        if let Some(prefix) = pattern.strip_suffix('*') {
            return value.starts_with(prefix);
        }
        pattern == value
    }

    /// 条件求值
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
                // Numeric comparison
                let a: f64 = match attr_value.parse() {
                    Ok(v) => v,
                    Err(_) => return false,
                };
                let b: f64 = match condition.value.parse() {
                    Ok(v) => v,
                    Err(_) => return false,
                };
                match condition.operator.as_str() {
                    "gte" => a >= b,
                    "lte" => a <= b,
                    "gt" => a > b,
                    "lt" => a < b,
                    _ => false,
                }
            }
            _ => false,
        }
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

// ──── 单元测试 ────

#[cfg(test)]
mod tests {
    use super::*;

    fn new_svc() -> PolicyService {
        let svc = PolicyService::new(1024);
        // auto-start for tests
        svc
    }

    #[test]
    fn test_wildcard_match_exact() {
        assert!(PolicyService::wildcard_match("read", "read"));
        assert!(!PolicyService::wildcard_match("read", "write"));
    }

    #[test]
    fn test_wildcard_match_star() {
        assert!(PolicyService::wildcard_match("*", "anything"));
        assert!(PolicyService::wildcard_match("*", ""));
    }

    #[test]
    fn test_wildcard_match_prefix() {
        assert!(PolicyService::wildcard_match("/api/*", "/api/users"));
        assert!(PolicyService::wildcard_match("/api/*", "/api/"));
        assert!(!PolicyService::wildcard_match("/api/*", "/admin/users"));
    }

    #[test]
    fn test_default_deny() {
        let svc = new_svc();
        let req = AccessRequest {
            subject: "anyone".into(),
            action: "anything".into(),
            resource: "/any".into(),
            context: Default::default(),
        };
        let decision = svc.evaluate(&req).unwrap();
        assert_eq!(decision.effect, PolicyEffect::Deny);
    }

    #[test]
    fn test_priority_ordering() {
        let svc = new_svc();
        svc.add_policy(Policy {
            id: "low".into(), name: "low".into(), description: "".into(),
            effect: PolicyEffect::Allow, subjects: vec!["*".into()],
            actions: vec!["*".into()], resources: vec!["*".into()],
            conditions: vec![], priority: 10,
        }).unwrap();
        svc.add_policy(Policy {
            id: "high".into(), name: "high".into(), description: "".into(),
            effect: PolicyEffect::Deny, subjects: vec!["*".into()],
            actions: vec!["*".into()], resources: vec!["/secret/*".into()],
            conditions: vec![], priority: 100,
        }).unwrap();

        let req = AccessRequest {
            subject: "u".into(), action: "r".into(), resource: "/secret/key".into(),
            context: Default::default(),
        };
        let d = svc.evaluate(&req).unwrap();
        assert_eq!(d.effect, PolicyEffect::Deny);
        assert_eq!(d.matched_policy_id, Some("high".into()));
    }
}
