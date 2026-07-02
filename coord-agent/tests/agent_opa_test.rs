// TDD: Regorus OPA 引擎集成测试 (Phase E — 实施中)
//
// v8.2 §4.11: Agent 内嵌 Regorus（Rust 原生 OPA 引擎），直接加载 Rego 策略进行本地评估。
// - 无 Wasm 依赖
// - 策略包由 Server 通过 KV/Watch 下发至 Agent
// - 评估结果缓存 30 秒，策略版本变更时立即失效
//
// GREEN stage: OpaEngine 已实现，验证 Rego v1 语法策略评估。

use std::collections::HashMap;
use coord_agent::services::opa::{OpaEngine, OpaConfig, OpaInput};

/// 验证 OpaConfig 默认值
#[test]
fn test_opa_config_defaults() {
    let config = OpaConfig::default();
    assert_eq!(config.cache_ttl_secs, 30);
    assert_eq!(config.max_policies, 256);
}

/// 验证 OpaEngine 能加载并评估简单 Rego 策略（Allow）
#[test]
fn test_opa_eval_simple_allow() {
    let engine = OpaEngine::new(OpaConfig::default()).expect("create OpaEngine");

    // Rego v1 语法: 需要 `if` 关键字, `:=` 赋值
    let rego = r#"
package coord.auth

default allow := false

allow if {
    input.action == "read"
    input.resource == "public"
}
"#;
    engine.add_policy("test.rego", rego).expect("load policy");

    let input = OpaInput {
        subject: "alice".into(),
        action: "read".into(),
        resource: "public".into(),
        context: HashMap::new(),
    };
    let decision = engine.evaluate("coord.auth", &input).expect("evaluate");
    assert!(decision.allowed, "should allow read public");
    assert_eq!(decision.matched_rules, vec!["allow"]);
}

/// 验证 OpaEngine 正确拒绝（默认 Deny）
#[test]
fn test_opa_eval_default_deny() {
    let engine = OpaEngine::new(OpaConfig::default()).expect("create OpaEngine");

    let rego = r#"
package coord.auth

default allow := false

allow if {
    input.action == "write"
    input.resource == "admin"
}
"#;
    engine.add_policy("test.rego", rego).expect("load policy");

    let input = OpaInput {
        subject: "alice".into(),
        action: "read".into(),
        resource: "admin".into(),
        context: HashMap::new(),
    };
    let decision = engine.evaluate("coord.auth", &input).expect("evaluate");
    assert!(!decision.allowed, "should not allow read admin");
}

/// 验证基于 input 属性的条件匹配
#[test]
fn test_opa_conditional_access() {
    let engine = OpaEngine::new(OpaConfig::default()).expect("create OpaEngine");

    let rego = r#"
package coord.auth

default allow := false

allow if {
    input.subject == "admin-user"
}

allow if {
    input.action == "read"
    startswith(input.resource, "/public/")
}
"#;
    engine.add_policy("test.rego", rego).expect("load policy");

    // admin-user can access any resource
    let admin_input = OpaInput {
        subject: "admin-user".into(),
        action: "delete".into(),
        resource: "/secret/data".into(),
        context: HashMap::new(),
    };
    assert!(engine.evaluate("coord.auth", &admin_input).unwrap().allowed);

    // regular user can read public resources
    let user_input = OpaInput {
        subject: "alice".into(),
        action: "read".into(),
        resource: "/public/docs".into(),
        context: HashMap::new(),
    };
    assert!(engine.evaluate("coord.auth", &user_input).unwrap().allowed);

    // regular user cannot write public resources
    let deny_input = OpaInput {
        subject: "alice".into(),
        action: "write".into(),
        resource: "/public/docs".into(),
        context: HashMap::new(),
    };
    assert!(!engine.evaluate("coord.auth", &deny_input).unwrap().allowed);
}

/// 验证策略热加载（替换已有策略）
#[test]
fn test_opa_policy_reload() {
    let engine = OpaEngine::new(OpaConfig::default()).expect("create OpaEngine");

    // Initial policy: deny all
    let rego_v1 = r#"
package coord.auth

default allow := false
"#;
    engine.add_policy("test.rego", rego_v1).expect("load v1");

    let input = OpaInput::default();
    assert!(!engine.evaluate("coord.auth", &input).unwrap().allowed);

    // Hot reload: allow all
    let rego_v2 = r#"
package coord.auth

default allow := true
"#;
    engine.add_policy("test.rego", rego_v2).expect("load v2");

    assert!(engine.evaluate("coord.auth", &input).unwrap().allowed);
}

/// 验证多策略文件不同 package 隔离
#[test]
fn test_opa_multi_policy_isolation() {
    let engine = OpaEngine::new(OpaConfig::default()).expect("create OpaEngine");

    // Policy A: API access control
    engine.add_policy("api.rego", r#"
package coord.api

default allow := false
allow if { input.action == "read" }
"#).expect("load api policy");

    // Policy B: Admin access control
    engine.add_policy("admin.rego", r#"
package coord.admin

default allow := false
allow if { input.subject == "root" }
"#).expect("load admin policy");

    let read_input = OpaInput {
        action: "read".into(),
        ..Default::default()
    };
    assert!(engine.evaluate("coord.api", &read_input).unwrap().allowed);
    assert!(!engine.evaluate("coord.admin", &read_input).unwrap().allowed);

    let root_input = OpaInput {
        subject: "root".into(),
        ..Default::default()
    };
    assert!(!engine.evaluate("coord.api", &root_input).unwrap().allowed);
    assert!(engine.evaluate("coord.admin", &root_input).unwrap().allowed);
}
