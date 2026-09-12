// agent 非 loopback 绑定强制 auth+TLS
//
// agent 非 loopback 绑定强制 auth+TLS（与 server 侧
// "非 loopback 无鉴权拒绝启动" 同口径）。

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
    // 等待监听就绪（冷启动），随后优雅退出；此处仅验证不因守卫被拒
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

/// 非 loopback 绑定且 auth+TLS 均配置 → 允许启动。
///
/// 生产收口后，TLS 是**真实挂载**而非仅配置校验：本测试同时验证
/// 配置了 tls 的非 loopback agent 实际启动监听（由入站 TLS 集成测试
/// `agent_inbound_tls_test.rs` 验证握手细节）。
#[tokio::test]
async fn test_non_loopback_with_auth_and_tls_allowed() {
    let port = find_port();
    let tmpdir = tempfile::tempdir().unwrap();
    // 自签名证书（guard 仅要求文件存在；握手验证在入站 TLS 测试覆盖）
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_path = tmpdir.path().join("agent.crt");
    let key_path = tmpdir.path().join("agent.key");
    std::fs::write(&cert_path, cert.cert.pem().as_bytes()).unwrap();
    std::fs::write(&key_path, cert.signing_key.serialize_pem().as_bytes()).unwrap();

    let mut config = AgentConfig::default();
    config.agent_addr = format!("0.0.0.0:{port}");
    config.http_addr = format!("127.0.0.1:{}", find_port());
    config.auth.enabled = true;
    // A3：开启鉴权必须有密钥材料，否则拒绝启动（fail-closed）。这里提供一个
    // 32 字节 Ed25519 公钥（生产语义：agent 只持公钥验签）。
    config.auth.verifying_key_hex = "ab".repeat(32);
    // A3 连带：还要有出站凭据（bootstrap token），否则 RoleCache 永远同步不到，
    // 角色门控 RPC 会全量 403 —— 该配置错误现在在启动期就被拒绝。
    config.auth.bootstrap_token = "bootstrap-token-for-tests".to_string();
    config.tls = Some(coord_agent::AgentTlsConfig {
        cert_path,
        key_path,
        ca_path: None,
        server_name: None,
    });

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
                "non-loopback TLS agent never became ready"
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
        Ok(Err(e)) => panic!("non-loopback auth+TLS serve failed: {e}"),
        Err(_) => panic!("serve timed out (shutdown future should have fired)"),
    }
}
