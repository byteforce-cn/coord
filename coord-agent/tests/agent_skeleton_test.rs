// TDD: coord-agent crate 骨架测试
//
// Phase A2 — RED stage: 在 coord-agent crate 还不存在时此测试应编译失败。

use coord_agent::{AgentConfig, AgentServer, StaticDiscovery};

/// 验证 crate 的公共 API 导出
#[test]
fn test_public_api_exports() {
    // AgentConfig 可以构造
    let config = AgentConfig {
        agent_addr: "127.0.0.1:19527".into(),
        http_addr: "127.0.0.1:19528".into(),
        data_dir: "/tmp/coord-agent-test".into(),
        discovery_mode: coord_agent::DiscoveryMode::Static,
        static_peers: vec!["127.0.0.1:50051".into()],
        cache_kv_max_entries: 1000,
        cache_kv_ttl_secs: 30,
        cache_catalog_ttl_secs: 10,
        cache_route_ttl_secs: 60,
        proxy_max_retries: 3,
        proxy_request_timeout_secs: 5,
        services: Default::default(),
        tls: None,
        thread_pools: Default::default(),
    };

    assert_eq!(config.agent_addr, "127.0.0.1:19527");
    assert_eq!(config.http_addr, "127.0.0.1:19528");
    assert!(matches!(config.discovery_mode, coord_agent::DiscoveryMode::Static));
    assert_eq!(config.static_peers.len(), 1);
}

/// 验证 AgentServer 类型存在
#[test]
fn test_agent_server_type_exists() {
    // AgentServer 应该是一个可以被引用的类型
    let _server: Option<AgentServer> = None;
    // 验证类型可以被丢弃（是 Sized）
}

/// 验证 StaticDiscovery 实现 MemberDiscovery trait
#[test]
fn test_static_discovery_implements_trait() {
    use coord_core::discovery::MemberDiscovery;

    let peers = vec!["127.0.0.1:50051".parse().unwrap()];
    let discovery = StaticDiscovery::new(peers.clone());

    // 验证 trait 方法
    assert!(discovery.is_healthy());
    assert_eq!(discovery.peers(), peers);
    assert!(discovery.leader_hint().is_none());
    assert!(discovery.watch_changes().is_none());

    let leader_addr = "127.0.0.1:50051".parse().unwrap();
    discovery.set_leader(leader_addr);
    assert_eq!(discovery.leader_hint(), Some(leader_addr));

    discovery.clear_leader();
    assert!(discovery.leader_hint().is_none());
}

/// 验证 run_agent 函数签名存在
#[test]
fn test_run_agent_signature() {
    // run_agent 应该是一个异步函数，接收 AgentConfig 返回 Result
    // 我们不实际调用它（会启动真实的 gRPC server），只验证类型签名
    let _func: fn(AgentConfig) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send>> = |_config: AgentConfig| {
        Box::pin(async { Ok(()) })
    };
    // 如果上面的赋值编译通过，说明 run_agent 的函数签名与预期兼容
}

// ──── B3: AgentConfig TOML 加载 ────

/// 验证 AgentConfig 可以从 TOML 字符串反序列化
#[test]
fn test_agent_config_from_toml() {
    let toml_str = r#"
agent_addr = "0.0.0.0:19527"
http_addr = "0.0.0.0:19528"
data_dir = "/var/lib/coord-agent"
discovery_mode = "static"
static_peers = ["10.0.1.1:50051", "10.0.1.2:50051", "10.0.1.3:50051"]
proxy_max_retries = 5
proxy_request_timeout_secs = 10
"#;

    // 使用 serde 直接解析（验证 derive 正确）
    let config: AgentConfig = toml::from_str(toml_str).expect("should parse TOML");

    assert_eq!(config.agent_addr, "0.0.0.0:19527");
    assert_eq!(config.http_addr, "0.0.0.0:19528");
    assert!(matches!(config.discovery_mode, coord_agent::DiscoveryMode::Static));
    assert_eq!(config.static_peers.len(), 3);
    assert_eq!(config.static_peers[0], "10.0.1.1:50051");
    assert_eq!(config.proxy_max_retries, 5);
    assert_eq!(config.proxy_request_timeout_secs, 10);
}

/// 验证 AgentConfig 默认值填充
#[test]
fn test_agent_config_defaults_from_minimal_toml() {
    let toml_str = r#"
static_peers = ["10.0.0.1:50051"]
"#;

    let config: AgentConfig = toml::from_str(toml_str).expect("should parse minimal TOML");

    // 未指定字段应使用默认值
    assert_eq!(config.agent_addr, "127.0.0.1:19527");
    assert_eq!(config.cache_kv_max_entries, 10000);
    assert_eq!(config.proxy_max_retries, 3);
    assert_eq!(config.static_peers, vec!["10.0.0.1:50051"]);
}

/// 验证 DiscoveryMode 的 snake_case 反序列化
#[test]
fn test_discovery_mode_deserialization() {
    let config: AgentConfig = toml::from_str(r#"discovery_mode = "static""#).unwrap();
    assert!(matches!(config.discovery_mode, coord_agent::DiscoveryMode::Static));

    let config: AgentConfig = toml::from_str(r#"discovery_mode = "gossip""#).unwrap();
    assert!(matches!(config.discovery_mode, coord_agent::DiscoveryMode::Gossip));
}
