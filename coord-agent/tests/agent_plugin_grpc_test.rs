// 插件化服务宿主（端到端）：原生服务的 gRPC 面由**插件注册表**动态组装。
//
// 这组断言守护「原生服务 = 真插件」这一收敛，取代此前「ServiceManager 登记 +
// lib.rs 硬编码 add_optional_service 长链」的双清单结构：
//
// 1. 已启用的原生服务能被服务端**真实**响应（不是「登记进注册表就算数」）；
// 2. **未启用**的服务不会被挂载（fail-closed）——动态组装尊重注册表，
//    而不是像硬编码长链那样逐条写死；
// 3. `coord.plugin.Plugin/List` 把内建服务（原生）与脚本插件放进同一份清单，
//    并如实上报 `runtime=native` / `builtin=true` / `healthy`。
//
// 探针选用 `coord.agent.FeatureFlags/IsEnabled`（纯内存，无需 server 连接）。


use coord_agent::{AgentConfig, AgentServer, ServiceConfig};
use coord_proto::agent::feature_flags_client::FeatureFlagsClient;
use coord_proto::agent::FeatureFlagIsEnabledRequest;
use coord_proto::plugin::plugin_client::PluginClient;
use coord_proto::plugin::ListPluginsRequest;

/// 找一个可用端口。
fn find_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// 只启用指定的数据面服务（其余关闭）→ 可断言「未启用 = 不挂载」。
fn config_with_services(port: u16, tag: &str, flags: bool, cb: bool) -> AgentConfig {
    AgentConfig {
        agent_addr: format!("127.0.0.1:{port}"),
        http_addr: format!("127.0.0.1:{}", find_port()),
        data_dir: std::env::temp_dir()
            .join(format!("coord-agent-plugin-grpc-{tag}-{port}"))
            .to_string_lossy()
            .into_owned(),
        static_peers: vec![],
        services: ServiceConfig {
            registry: false,
            config_center: false,
            lock: false,
            idgen: false,
            leader_election: false,
            event_notification: false,
            cache: false,
            mq: false,
            workflow: false,
            policy: false,
            scheduler: false,
            circuit_breaker: cb,
            rate_limiter: false,
            feature_flags: flags,
            transit: false,
            pki: false,
            replication: false,
            ..Default::default()
        },
        ..Default::default()
    }
}

async fn spawn_agent(config: AgentConfig) -> String {
    let addr = config.agent_addr.clone();
    let server = AgentServer::new(config);
    tokio::spawn(async move {
        let _ = server.serve().await;
    });
    // 阻塞到端口真正可连接（固定 sleep 在并发跑全量套件时不够 → 假红）
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        match tokio::net::TcpStream::connect(&addr).await {
            Ok(stream) => {
                drop(stream);
                break;
            }
            Err(e) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "agent gRPC endpoint {addr} never became ready: {e}"
                );
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    }
    addr
}

/// 已启用的原生服务：gRPC 面由插件注册表挂载 → 真实可调用。
#[tokio::test]
async fn enabled_native_service_is_mounted_and_serving() {
    let port = find_port();
    let addr = spawn_agent(config_with_services(port, "enabled", true, false)).await;

    let mut client = FeatureFlagsClient::connect(format!("http://{addr}"))
        .await
        .expect("FeatureFlags client should connect (service mounted by plugin registry)");
    let resp = client
        .is_enabled(FeatureFlagIsEnabledRequest {
            flag_name: "definitely-not-defined".into(),
            context: vec![],
        })
        .await
        .expect("enabled service must answer, not UNIMPLEMENTED");
    // 未定义的开关 → false（而不是报错）
    assert!(!resp.into_inner().enabled);
}

/// 未启用的原生服务不会出现在插件注册表里 → 其 gRPC 面不挂载（fail-closed）。
#[tokio::test]
async fn disabled_native_service_is_not_mounted() {
    let port = find_port();
    let addr = spawn_agent(config_with_services(port, "disabled", false, true)).await;

    let mut flags = FeatureFlagsClient::connect(format!("http://{addr}"))
        .await
        .expect("channel connects regardless of service registration");
    let err = flags
        .is_enabled(FeatureFlagIsEnabledRequest {
            flag_name: "x".into(),
            context: vec![],
        })
        .await
        .expect_err("disabled service must not be mounted");
    assert_eq!(
        err.code(),
        tonic::Code::Unimplemented,
        "unregistered service must answer UNIMPLEMENTED, got {err:?}"
    );
}

/// 统一清单：内建服务（原生）与脚本插件同表，带 runtime/builtin/healthy。
#[tokio::test]
async fn plugin_list_reports_builtin_native_services() {
    let port = find_port();
    let addr = spawn_agent(config_with_services(port, "list", true, true)).await;

    let mut client = PluginClient::connect(format!("http://{addr}"))
        .await
        .expect("plugin API client should connect");
    let plugins = client
        .list(ListPluginsRequest {})
        .await
        .expect("plugin List must be available")
        .into_inner()
        .plugins;

    let names: Vec<&str> = plugins.iter().map(|p| p.name.as_str()).collect();
    assert!(
        names.contains(&"feature_flags") && names.contains(&"circuit_breaker"),
        "enabled native services must appear in the unified plugin inventory: {names:?}"
    );
    assert!(
        !names.contains(&"cache") && !names.contains(&"mq"),
        "disabled services must not appear in the inventory: {names:?}"
    );

    for p in plugins
        .iter()
        .filter(|p| p.name == "feature_flags" || p.name == "circuit_breaker")
    {
        assert_eq!(p.runtime, "native", "native service runtime tag");
        assert!(p.builtin, "native services are builtin plugins: {}", p.name);
        assert_eq!(p.status, "started", "{} must be started", p.name);
        assert!(p.healthy, "{} must report live health", p.name);
    }
}
