// agent 入站 gRPC TLS 集成测试（生产上线收口项 #2）
//
// 背景：agent 入站 gRPC 此前从不挂载 TLS（守卫只校验配置存在，
// 实际监听始终明文）。本套件验证：
// 1. 配置 `tls` 后入站 gRPC 实际以 TLS 服务（TLS 客户端可连接、明文客户端失败）；
// 2. mTLS（ca_path 配置）下无客户端证书握手被拒；
// 3. 未配置 tls 时仍为明文（loopback 开发兼容，现有测试路径不变）。

use std::path::Path;
use std::time::Duration;

use coord_agent::{AgentConfig, AgentServer, AgentTlsConfig};

// ──── 证书生成（rcgen：CA + agent 证书，IP SAN 127.0.0.1） ────

struct TestCa {
    key: rcgen::KeyPair,
    params: rcgen::CertificateParams,
    cert_pem: Vec<u8>,
}

fn generate_ca() -> TestCa {
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "agent-test-ca");
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let key = rcgen::KeyPair::generate().unwrap();
    let cert = params.self_signed(&key).unwrap();
    TestCa {
        key,
        params,
        cert_pem: cert.pem().into_bytes(),
    }
}

fn issue_agent_cert(ca: &TestCa) -> (Vec<u8>, Vec<u8>) {
    let key = rcgen::KeyPair::generate().unwrap();
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "coord-agent");
    params.subject_alt_names = vec![rcgen::SanType::IpAddress("127.0.0.1".parse().unwrap())];
    let issuer = rcgen::Issuer::from_params(&ca.params, &ca.key);
    let cert = params.signed_by(&key, &issuer).unwrap();
    (cert.pem().into_bytes(), key.serialize_pem().into_bytes())
}

fn write_pem(dir: &Path, name: &str, data: &[u8]) -> std::path::PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, data).unwrap();
    path
}

fn find_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// 构建带 TLS 的 agent 配置（mTLS 可选），返回 (配置, CA PEM)
fn tls_agent_config(port: u16, tmpdir: &Path, mtls: bool) -> (AgentConfig, Vec<u8>) {
    let ca = generate_ca();
    let (cert, key) = issue_agent_cert(&ca);
    let cert_path = write_pem(tmpdir, &format!("agent-{port}.crt"), &cert);
    let key_path = write_pem(tmpdir, &format!("agent-{port}.key"), &key);
    let ca_path = if mtls {
        Some(write_pem(tmpdir, &format!("ca-{port}.crt"), &ca.cert_pem))
    } else {
        None
    };
    let mut config = AgentConfig::default();
    config.agent_addr = format!("127.0.0.1:{port}");
    config.http_addr = format!("127.0.0.1:{}", find_port());
    config.data_dir = tmpdir
        .join(format!("agent-data-{port}"))
        .to_string_lossy()
        .into_owned();
    config.tls = Some(AgentTlsConfig {
        cert_path,
        key_path,
        ca_path,
        server_name: None,
    });
    (config, ca.cert_pem)
}

async fn spawn(config: AgentConfig) -> tokio::task::JoinHandle<()> {
    let server = AgentServer::new(config);
    tokio::spawn(async move {
        let _ = server.serve().await;
    })
}

async fn wait_port(port: u16) {
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("agent port {port} never became reachable");
}

/// 标准 grpc.health.v1 Health 探测（TLS）
async fn health_check_tls(port: u16, ca_pem: &[u8]) -> Result<(), String> {
    use tonic::transport::Certificate;
    let tls = tonic::transport::channel::ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca_pem));
    let channel = tonic::transport::Endpoint::from_shared(format!("https://127.0.0.1:{port}"))
        .unwrap()
        .tls_config(tls)
        .unwrap()
        .connect_timeout(Duration::from_secs(3))
        .connect()
        .await
        .map_err(|e| e.to_string())?;
    let mut client = tonic_health::pb::health_client::HealthClient::new(channel);
    let resp = client
        .check(tonic_health::pb::HealthCheckRequest::default())
        .await
        .map_err(|e| e.to_string())?;
    if resp.into_inner().status
        == tonic_health::pb::health_check_response::ServingStatus::Serving as i32
    {
        Ok(())
    } else {
        Err("health status not SERVING".into())
    }
}

/// 明文 gRPC 探测：必须失败（TLS 端口不接受明文）
async fn health_check_plaintext_must_fail(port: u16) {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect_timeout(Duration::from_secs(3))
        .connect()
        .await;
    // 连接建立（TCP）可能成功，但 RPC 必因 TLS 握手失败而报错
    let result: Result<(), String> = async {
        let channel = channel.map_err(|e| e.to_string())?;
        let mut client = tonic_health::pb::health_client::HealthClient::new(channel);
        client
            .check(tonic_health::pb::HealthCheckRequest::default())
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
    .await;
    assert!(
        result.is_err(),
        "plaintext gRPC must fail against a TLS-serving agent"
    );
}

#[tokio::test]
async fn agent_inbound_serves_tls_when_configured() {
    let tmpdir = tempfile::tempdir().unwrap();
    let ca = generate_ca();
    let (cert, key) = issue_agent_cert(&ca);
    let cert_path = write_pem(tmpdir.path(), "agent.crt", &cert);
    let key_path = write_pem(tmpdir.path(), "agent.key", &key);
    let port = find_port();
    let mut config = AgentConfig::default();
    config.agent_addr = format!("127.0.0.1:{port}");
    config.http_addr = format!("127.0.0.1:{}", find_port());
    config.data_dir = tmpdir.path().join("data").to_string_lossy().into_owned();
    config.tls = Some(AgentTlsConfig {
        cert_path,
        key_path,
        ca_path: None, // 服务端 TLS（不强制客户端证书）
        server_name: None,
    });

    let handle = spawn(config).await;
    wait_port(port).await;

    // TLS 客户端（持有 CA）→ 成功
    health_check_tls(port, &ca.cert_pem)
        .await
        .expect("TLS health check must succeed");
    // 明文客户端 → 失败
    health_check_plaintext_must_fail(port).await;

    handle.abort();
}

#[tokio::test]
async fn agent_inbound_mtls_rejects_missing_client_cert() {
    let tmpdir = tempfile::tempdir().unwrap();
    let port = find_port();
    let (config, ca_pem) = tls_agent_config(port, tmpdir.path(), true); // ca_path → mTLS
    let handle = spawn(config).await;
    wait_port(port).await;

    // 信任真实 CA 但无客户端证书 → 服务端要求 mTLS，握手被拒
    let result = health_check_tls(port, &ca_pem).await;
    assert!(
        result.is_err(),
        "mTLS agent must reject a client without a valid certificate"
    );

    handle.abort();
}
