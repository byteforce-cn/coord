// TDD: connect_via_agent / connect_direct 双模式构造器测试
//
// Phase A4 — 验证 Client 的双模式 API 签名和基本行为。

use coord_client::{Client, Config};

/// 验证 connect_via_agent 构造器签名存在且可编译
#[test]
fn test_connect_via_agent_signature_exists() {
    // 编译时检查：方法签名存在
    let _check: fn(&str) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Client, coord_core::error::Error>> + Send>,
    > = |addr: &str| {
        let addr = addr.to_string();
        Box::pin(Client::connect_via_agent(addr))
    };
}

/// 验证 connect_direct 构造器签名存在
#[test]
fn test_connect_direct_signature_exists() {
    let _check: fn(Config) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Client, coord_core::error::Error>> + Send>,
    > = |config: Config| Box::pin(Client::connect_direct(config));
}

/// 验证 Client::new 仍然可用（向后兼容，等同于 connect_direct）
#[test]
fn test_client_new_still_works() {
    // Client::new 接受 Config，签名不变
    let _check: fn(Config) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Client, coord_core::error::Error>> + Send>,
    > = |config: Config| Box::pin(Client::new(config));
}

/// 验证 Config 可以构造（确保现有 API 不受影响）
#[test]
fn test_config_construction() {
    let config = Config::new(vec!["127.0.0.1:50051".into()]);
    assert_eq!(config.endpoints.len(), 1);
    assert_eq!(config.max_retries, 5);
}
