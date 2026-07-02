// coord-agent: 权限策略引擎测试（Phase G）
//
// TDD RED phase: 测试 PolicyService（基于规则的授权决策引擎）。
// 支持 RBAC/ABAC 策略评估、策略管理、条件匹配。
//
// 参见 docs/client-agent-architecture-v3.md §5.10。

use std::sync::Arc;

use coord_agent::services::policy::{
    PolicyService, Policy, PolicyDecision, PolicyEffect, AccessRequest,
    PolicyCondition,
};
use coord_agent::BaseService;

// ──── helpers ────

fn new_policy_service() -> PolicyService {
    let svc = PolicyService::new(1024);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async { svc.start().await.expect("start should succeed") });
    svc
}

// ──── G.9: Policy 服务创建 ────

#[test]
fn test_policy_service_creation() {
    let svc = PolicyService::new(1024);
    assert_eq!(svc.name(), "policy");
    assert!(!svc.health_check());
}

#[test]
fn test_policy_service_start_stop() {
    let svc = PolicyService::new(1024);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        svc.start().await.expect("start");
        assert!(svc.health_check());
        svc.stop().await.expect("stop");
        assert!(!svc.health_check());
    });
}

// ──── G.10: 策略 CRUD ────

#[test]
fn test_policy_add_and_get() {
    let svc = new_policy_service();
    let policy = Policy {
        id: "pol-001".into(),
        name: "allow-admin-read".into(),
        description: "Admins can read everything".into(),
        effect: PolicyEffect::Allow,
        subjects: vec!["role:admin".into()],
        actions: vec!["read".into()],
        resources: vec!["*".into()],
        conditions: vec![],
        priority: 100,
    };
    svc.add_policy(policy).expect("add policy");
    let got = svc.get_policy("pol-001").expect("get policy");
    assert!(got.is_some());
    assert_eq!(got.unwrap().name, "allow-admin-read");
}

#[test]
fn test_policy_remove() {
    let svc = new_policy_service();
    svc.add_policy(Policy {
        id: "pol-tmp".into(),
        name: "tmp".into(),
        description: "".into(),
        effect: PolicyEffect::Allow,
        subjects: vec!["user:test".into()],
        actions: vec!["*".into()],
        resources: vec!["*".into()],
        conditions: vec![],
        priority: 0,
    }).expect("add");
    assert!(svc.remove_policy("pol-tmp").expect("remove"));
    assert!(svc.get_policy("pol-tmp").expect("get").is_none());
}

#[test]
fn test_policy_list() {
    let svc = new_policy_service();
    for i in 0..3 {
        svc.add_policy(Policy {
            id: format!("pol-{}", i),
            name: format!("policy-{}", i),
            description: "".into(),
            effect: PolicyEffect::Allow,
            subjects: vec!["*".into()],
            actions: vec!["*".into()],
            resources: vec!["*".into()],
            conditions: vec![],
            priority: i * 10,
        }).expect("add");
    }
    let all = svc.list_policies().expect("list");
    assert_eq!(all.len(), 3);
}

// ──── G.11: 访问决策 — 简单 RBAC ────

#[test]
fn test_policy_eval_allow_role_match() {
    let svc = new_policy_service();
    svc.add_policy(Policy {
        id: "p1".into(),
        name: "admin-all".into(),
        description: "".into(),
        effect: PolicyEffect::Allow,
        subjects: vec!["role:admin".into()],
        actions: vec!["*".into()],
        resources: vec!["*".into()],
        conditions: vec![],
        priority: 100,
    }).expect("add");

    let req = AccessRequest {
        subject: "role:admin".into(),
        action: "write".into(),
        resource: "/api/users".into(),
        context: Default::default(),
    };

    let decision = svc.evaluate(&req).expect("eval");
    assert_eq!(decision.effect, PolicyEffect::Allow);
    assert_eq!(decision.matched_policy_id, Some("p1".into()));
}

#[test]
fn test_policy_eval_deny_no_match() {
    let svc = new_policy_service();
    svc.add_policy(Policy {
        id: "p1".into(),
        name: "user-read-only".into(),
        description: "".into(),
        effect: PolicyEffect::Allow,
        subjects: vec!["role:user".into()],
        actions: vec!["read".into()],
        resources: vec!["/api/*".into()],
        conditions: vec![],
        priority: 100,
    }).expect("add");

    let req = AccessRequest {
        subject: "role:user".into(),
        action: "write".into(), // not allowed
        resource: "/api/users".into(),
        context: Default::default(),
    };

    let decision = svc.evaluate(&req).expect("eval");
    assert_eq!(decision.effect, PolicyEffect::Deny);
    assert!(decision.matched_policy_id.is_none());
}

// ──── G.12: Deny 优先 ────

#[test]
fn test_policy_eval_deny_overrides_allow() {
    let svc = new_policy_service();
    // Allow all
    svc.add_policy(Policy {
        id: "allow-all".into(),
        name: "allow-all".into(),
        description: "".into(),
        effect: PolicyEffect::Allow,
        subjects: vec!["*".into()],
        actions: vec!["*".into()],
        resources: vec!["*".into()],
        conditions: vec![],
        priority: 0,
    }).expect("add");

    // Deny specific
    svc.add_policy(Policy {
        id: "deny-admin-api".into(),
        name: "deny-admin-api".into(),
        description: "".into(),
        effect: PolicyEffect::Deny,
        subjects: vec!["*".into()],
        actions: vec!["*".into()],
        resources: vec!["/admin/*".into()],
        conditions: vec![],
        priority: 100,
    }).expect("add");

    let req = AccessRequest {
        subject: "role:user".into(),
        action: "read".into(),
        resource: "/admin/config".into(),
        context: Default::default(),
    };

    let decision = svc.evaluate(&req).expect("eval");
    assert_eq!(decision.effect, PolicyEffect::Deny);
    assert_eq!(decision.matched_policy_id, Some("deny-admin-api".into()));
}

// ──── G.13: 条件匹配 ────

#[test]
fn test_policy_eval_with_conditions() {
    let svc = new_policy_service();
    svc.add_policy(Policy {
        id: "business-hours".into(),
        name: "business-hours-only".into(),
        description: "".into(),
        effect: PolicyEffect::Allow,
        subjects: vec!["*".into()],
        actions: vec!["write".into()],
        resources: vec!["*".into()],
        conditions: vec![
            PolicyCondition {
                attribute: "time_of_day".into(),
                operator: "gte".into(),
                value: "9".into(),
            },
            PolicyCondition {
                attribute: "time_of_day".into(),
                operator: "lte".into(),
                value: "17".into(),
            },
        ],
        priority: 100,
    }).expect("add");

    // During business hours
    let req = AccessRequest {
        subject: "role:user".into(),
        action: "write".into(),
        resource: "/api/data".into(),
        context: {
            let mut m = std::collections::HashMap::new();
            m.insert("time_of_day".into(), "14".into());
            m
        },
    };
    assert_eq!(svc.evaluate(&req).expect("eval").effect, PolicyEffect::Allow);

    // Outside business hours
    let req2 = AccessRequest {
        subject: "role:user".into(),
        action: "write".into(),
        resource: "/api/data".into(),
        context: {
            let mut m = std::collections::HashMap::new();
            m.insert("time_of_day".into(), "20".into());
            m
        },
    };
    assert_eq!(svc.evaluate(&req2).expect("eval").effect, PolicyEffect::Deny);
}

// ──── G.14: 优先级排序 ────

#[test]
fn test_policy_priority_ordering() {
    let svc = new_policy_service();
    // Low priority allow - specific resource
    svc.add_policy(Policy {
        id: "low-allow".into(),
        name: "low".into(),
        description: "".into(),
        effect: PolicyEffect::Allow,
        subjects: vec!["role:user".into()],
        actions: vec!["read".into()],
        resources: vec!["/public/*".into()],
        conditions: vec![],
        priority: 10,
    }).expect("add");

    // High priority deny - same scope
    svc.add_policy(Policy {
        id: "high-deny".into(),
        name: "high".into(),
        description: "".into(),
        effect: PolicyEffect::Deny,
        subjects: vec!["role:user".into()],
        actions: vec!["read".into()],
        resources: vec!["/public/*".into()],
        conditions: vec![],
        priority: 200,
    }).expect("add");

    let req = AccessRequest {
        subject: "role:user".into(),
        action: "read".into(),
        resource: "/public/index".into(),
        context: Default::default(),
    };
    // Higher priority deny should win
    assert_eq!(svc.evaluate(&req).expect("eval").effect, PolicyEffect::Deny);
}

// ──── G.15: 通配符匹配 ────

#[test]
fn test_policy_wildcard_matching() {
    let svc = new_policy_service();
    svc.add_policy(Policy {
        id: "wild".into(),
        name: "wildcard".into(),
        description: "".into(),
        effect: PolicyEffect::Allow,
        subjects: vec!["role:admin".into(), "service:auth".into()],
        actions: vec!["read".into(), "write".into()],
        resources: vec!["/api/v1/*".into(), "/api/v2/users".into()],
        conditions: vec![],
        priority: 100,
    }).expect("add");

    // Exact resource match
    let req = AccessRequest {
        subject: "role:admin".into(),
        action: "read".into(),
        resource: "/api/v2/users".into(),
        context: Default::default(),
    };
    assert_eq!(svc.evaluate(&req).expect("eval").effect, PolicyEffect::Allow);

    // Wildcard resource match
    let req2 = AccessRequest {
        subject: "role:admin".into(),
        action: "write".into(),
        resource: "/api/v1/orders".into(),
        context: Default::default(),
    };
    assert_eq!(svc.evaluate(&req2).expect("eval").effect, PolicyEffect::Allow);

    // No resource match
    let req3 = AccessRequest {
        subject: "role:admin".into(),
        action: "read".into(),
        resource: "/api/v3/data".into(),
        context: Default::default(),
    };
    assert_eq!(svc.evaluate(&req3).expect("eval").effect, PolicyEffect::Deny);
}

// ──── G.16: 并发安全 ────

#[test]
fn test_policy_concurrent_eval() {
    use std::thread;

    let svc = Arc::new(new_policy_service());
    svc.add_policy(Policy {
        id: "shared".into(),
        name: "shared".into(),
        description: "".into(),
        effect: PolicyEffect::Allow,
        subjects: vec!["*".into()],
        actions: vec!["read".into()],
        resources: vec!["*".into()],
        conditions: vec![],
        priority: 100,
    }).expect("add");

    let mut handles = vec![];
    for i in 0..20 {
        let svc = svc.clone();
        handles.push(thread::spawn(move || {
            let req = AccessRequest {
                subject: format!("user:{}", i),
                action: "read".into(),
                resource: format!("/api/resource/{}", i),
                context: Default::default(),
            };
            let decision = svc.evaluate(&req).expect("eval");
            assert_eq!(decision.effect, PolicyEffect::Allow);
        }));
    }
    for h in handles { h.join().unwrap(); }
}
