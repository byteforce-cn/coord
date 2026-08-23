// P1-05 验收测试：agent 非 loopback 绑定强制 auth+TLS
//
// 决策文档 P1-05：agent 非 loopback 绑定强制 auth+TLS（与 server 侧
// P0-G.1 "非 loopback 无鉴权拒绝启动" 同口径）。
//
// 对应文档：`docs/production/15-milestone-task-breakdown.md` P1-05。

use std::net::TcpListener;

use coord_agent::{AgentConfig, AgentServer};

fn find_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// 非 loopback 绑定且 auth+TLS 均缺 → 拒绝启动。
#[tokio::test]
async fn test_non_loopback_without_auth_tls_rejected() {
    let mut config = AgentConfig::default();
    config.agent_addr = format!("0.0.0.0:{}", find_port());
    config.http_addr = format!("127.0.0.1:{}", find_port());
    config.auth.enabled = false;
    config.tls = None;

    let server = AgentServer::new(config);
    let err = server
        .serve()
        .await
        .expect_err("non-loopback without auth+TLS must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("non-loopback") && msg.contains("auth+TLS"),
        "guard message should explain the requirement, got: {msg}"
    );
}

/// loopback 绑定：无 auth/TLS 允许启动（开发模式）。
#[tokio::test]
async fn test_loopback_without_auth_allowed() {
    let port = find_port();
    let mut config = AgentConfig::default();
    config.agent_addr = format!("127.0.0.1:{port}");
    config.http_addr = format!("127.0.0.1:{}", find_port());
    config.auth.enabled = false;
    config.tls = None;

    let server = AgentServer::new(config);
    // 等待监听就绪（冷启动），随后优雅退出；此处仅验证不因 P1-05 闸被拒
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
                "loopback agent never became ready"
            );
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    };
    let serve = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        server.serve_with_shutdown(shutdown),
    )
    .await;
    // 就绪探测完成后 shutdown 触发，serve 正常返回
    match serve {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("loopback serve failed: {e}"),
        Err(_) => panic!("serve timed out (shutdown future should have fired)"),
    }
}
