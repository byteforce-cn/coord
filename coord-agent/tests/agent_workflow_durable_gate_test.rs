// durable 工作流装配的 fail-closed 前置校验（默认关闭）
//
// `services.workflow_durable = true` 承诺的「事件不丢 / 一实例一驱动 / 副作用不重」
// 依赖 Server 连接（KV 持久化面）与 `services.mq`（投递面）。缺前置条件必须拒绝
// 启动，不得静默降级到非 durable 路径（静默降级正是该里程碑要消除的缺陷类）。
// 与 `agent_non_loopback_guard_test.rs` / `agent_dev_transit_kek_test.rs` 同一
// 模式：直接 `serve()` 取 `Err` 并断言消息点名缺失的前置。

use std::net::TcpListener;

use coord_agent::{AgentConfig, AgentServer, ServiceConfig};

fn find_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// loopback、无静态对端（skeleton）、数据目录指向临时目录；服务开关由调用方给出。
fn durable_config(
    port: u16,
    tag: &str,
    services: ServiceConfig,
) -> (AgentConfig, tempfile::TempDir) {
    let tmpdir = tempfile::tempdir().unwrap();
    let config = AgentConfig {
        agent_addr: format!("127.0.0.1:{port}"),
        http_addr: format!("127.0.0.1:{}", find_port()),
        data_dir: tmpdir
            .path()
            .join(format!("data-{tag}"))
            .to_string_lossy()
            .into_owned(),
        static_peers: vec![],
        services,
        ..Default::default()
    };
    (config, tmpdir)
}

/// mq=false ⇒ 拒绝启动，消息点名 `services.mq`。
#[tokio::test]
async fn test_durable_without_mq_is_fail_closed() {
    let services = ServiceConfig {
        workflow: true,
        workflow_durable: true,
        mq: false,
        ..ServiceConfig::default()
    };
    let (config, _tmp) = durable_config(find_port(), "no-mq", services);
    let err = AgentServer::new(config)
        .serve()
        .await
        .expect_err("durable without mq must refuse startup");
    let msg = err.to_string();
    assert!(
        msg.contains("services.mq"),
        "failure must name the mq prerequisite, got: {msg}"
    );
}

/// v2 数据面：MQ v1 被策略冻结（D13）⇒ 启动期拒绝，不能静默注册。
#[tokio::test]
async fn test_v2_data_plane_rejects_mq() {
    let services = ServiceConfig {
        mq: true,
        ..ServiceConfig::default()
    };
    let (mut config, _tmp) = durable_config(find_port(), "v2-mq", services);
    config.data_plane = coord_agent::AgentDataPlane::V2;
    let err = AgentServer::new(config)
        .serve()
        .await
        .expect_err("v2 data plane must reject services.mq");
    let msg = err.to_string();
    assert!(
        msg.contains("v2 data plane") && msg.contains("MQ"),
        "failure must name the v2/MQ boundary, got: {msg}"
    );
}

/// v2 数据面：即使 mq=true，workflow_durable 也不能装配（MQ v1 不服务）。
#[tokio::test]
async fn test_v2_data_plane_rejects_workflow_durable() {
    let services = ServiceConfig {
        workflow: true,
        workflow_durable: true,
        mq: true,
        ..ServiceConfig::default()
    };
    let (mut config, _tmp) = durable_config(find_port(), "v2-durable", services);
    config.data_plane = coord_agent::AgentDataPlane::V2;
    let err = AgentServer::new(config)
        .serve()
        .await
        .expect_err("v2 data plane must reject workflow_durable");
    let msg = err.to_string();
    assert!(
        msg.contains("v2 data plane"),
        "failure must name the v2 data-plane boundary, got: {msg}"
    );
}

/// 无 Server 连接（static_peers 空；mq=true）⇒ 拒绝启动，消息点名 server connection。
#[tokio::test]
async fn test_durable_without_server_is_fail_closed() {
    let services = ServiceConfig {
        workflow: true,
        workflow_durable: true,
        mq: true,
        ..ServiceConfig::default()
    };
    let (config, _tmp) = durable_config(find_port(), "no-server", services);
    let err = AgentServer::new(config)
        .serve()
        .await
        .expect_err("durable without a server connection must refuse startup");
    let msg = err.to_string();
    assert!(
        msg.contains("server connection"),
        "failure must name the server prerequisite, got: {msg}"
    );
}

/// workflow_durable=true 但 workflow=false ⇒ 拒绝启动（配置无效，不得静默失效）。
#[tokio::test]
async fn test_durable_without_workflow_is_fail_closed() {
    let services = ServiceConfig {
        workflow: false,
        workflow_durable: true,
        mq: true,
        ..ServiceConfig::default()
    };
    let (config, _tmp) = durable_config(find_port(), "no-workflow", services);
    let err = AgentServer::new(config)
        .serve()
        .await
        .expect_err("durable without workflow must refuse startup");
    let msg = err.to_string();
    assert!(
        msg.contains("services.workflow"),
        "failure must name the workflow prerequisite, got: {msg}"
    );
}

/// durable 关闭时不要求 MQ：workflow=true, durable=false, mq=false 可正常启动。
#[tokio::test]
async fn test_non_durable_workflow_does_not_require_mq() {
    let port = find_port();
    let services = ServiceConfig {
        workflow: true,
        workflow_durable: false,
        mq: false,
        ..ServiceConfig::default()
    };
    let (config, _tmp) = durable_config(port, "non-durable", services);
    let server = AgentServer::new(config);
    let shutdown = async move {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
        loop {
            if tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
                .await
                .is_ok()
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "non-durable workflow agent never became ready"
            );
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    };
    let serve = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        server.serve_with_shutdown(shutdown),
    )
    .await;
    match serve {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("non-durable workflow serve failed: {e}"),
        Err(_) => panic!("serve timed out (shutdown future should have fired)"),
    }
}
