// TDD: Agent TLS/mTLS 测试 (Phase A-mTLS — RED→GREEN)
//
// 验证 Agent TLS 基础设施：
// 1. TLS 配置加载与序列化
// 2. TLS Channel 构建与连接
// 3. mTLS 双向证书验证
//
// 使用 rcgen 生成自签名证书进行测试。

use std::path::PathBuf;

use coord_agent::AgentConfig;
use coord_agent::AgentTlsConfig;

// ──── 证书生成 ────

/// 生成自签名证书（PEM 格式），返回 (cert_pem, key_pem)
fn generate_self_signed_cert(dns_name: &str) -> (Vec<u8>, Vec<u8>) {
    let cert = rcgen::generate_simple_self_signed(vec![dns_name.into()]).unwrap();
    (cert.cert.pem().into_bytes(), cert.signing_key.serialize_pem().into_bytes())
}

/// 写入临时 PEM 文件，返回路径
fn write_temp_pem(data: &[u8], prefix: &str) -> PathBuf {
    use std::io::Write;
    let dir = std::env::temp_dir();
    let path = dir.join(format!("{}_{}.pem", prefix, uuid::Uuid::new_v4()));
    let mut file = std::fs::File::create(&path).unwrap();
    file.write_all(data).unwrap();
    path
}

// ──── T1: TLS 配置加载 ────

/// A-mTLS.1: AgentTlsConfig 从 PEM 文件路径加载
#[test]
fn test_tls_config_from_paths() {
    let (cert, key) = generate_self_signed_cert("agent.local");

    let cert_path = write_temp_pem(&cert, "cert");
    let key_path = write_temp_pem(&key, "key");

    let tls_config = AgentTlsConfig {
        cert_path: cert_path.clone(),
        key_path: key_path.clone(),
        ca_path: Some(cert_path.clone()),
    };

    assert!(tls_config.is_configured());
    let loaded_cert = tls_config.load_cert().unwrap();
    let loaded_key = tls_config.load_key().unwrap();
    let loaded_ca = tls_config.load_ca().unwrap().unwrap();

    assert!(!loaded_cert.is_empty());
    assert!(!loaded_key.is_empty());
    assert!(!loaded_ca.is_empty());

    let _ = std::fs::remove_file(&cert_path);
    let _ = std::fs::remove_file(&key_path);
}

/// A-mTLS.2: AgentTlsConfig 缺失 CA 时 is_configured() 仍为 true
#[test]
fn test_tls_config_without_ca() {
    let (cert, key) = generate_self_signed_cert("agent.local");

    let cert_path = write_temp_pem(&cert, "cert");
    let key_path = write_temp_pem(&key, "key");

    let tls_config = AgentTlsConfig {
        cert_path: cert_path.clone(),
        key_path: key_path.clone(),
        ca_path: None,
    };

    assert!(tls_config.is_configured());
    assert!(tls_config.load_ca().unwrap().is_none());

    let _ = std::fs::remove_file(&cert_path);
    let _ = std::fs::remove_file(&key_path);
}

/// A-mTLS.3: 缺失证书时 is_configured() 返回 false
#[test]
fn test_tls_config_missing_cert_returns_false() {
    let tls_config = AgentTlsConfig {
        cert_path: PathBuf::from("/nonexistent/cert.pem"),
        key_path: PathBuf::from("/nonexistent/key.pem"),
        ca_path: None,
    };
    assert!(!tls_config.is_configured());
}

// ──── T2: TLS 配置序列化 ────

/// A-mTLS.4: AgentConfig 包含可选 TLS 字段，支持 TOML 反序列化
#[test]
fn test_agent_config_tls_toml_deserialization() {
    let toml_str = r#"
agent_addr = "127.0.0.1:19527"
http_addr = "127.0.0.1:19528"
data_dir = "/var/lib/coord-agent"

[tls]
cert_path = "/etc/coord-agent/agent.crt"
key_path = "/etc/coord-agent/agent.key"
ca_path = "/etc/coord-agent/ca.crt"
"#;

    let config: AgentConfig = toml::from_str(toml_str).unwrap();
    let tls = config.tls.unwrap();
    assert_eq!(tls.cert_path, PathBuf::from("/etc/coord-agent/agent.crt"));
    assert_eq!(tls.key_path, PathBuf::from("/etc/coord-agent/agent.key"));
    assert_eq!(tls.ca_path, Some(PathBuf::from("/etc/coord-agent/ca.crt")));
}

/// A-mTLS.5: 不包含 TLS 字段时 tls 为 None
#[test]
fn test_agent_config_without_tls_is_none() {
    let toml_str = r#"
agent_addr = "127.0.0.1:19527"
http_addr = "127.0.0.1:19528"
"#;
    let config: AgentConfig = toml::from_str(toml_str).unwrap();
    assert!(config.tls.is_none());
}

// ──── T3: TLS Channel 集成测试 ────

/// A-mTLS.6: 使用自签名证书的 TLS Channel 连接成功
#[tokio::test]
async fn test_tls_channel_with_self_signed_cert() {
    let (cert, key) = generate_self_signed_cert("localhost");

    let cert_path = write_temp_pem(&cert, "srv-cert");
    let key_path = write_temp_pem(&key, "srv-key");

    let port = find_port();

    let server_tls = coord_agent::build_agent_tls_server_config(
        &cert_path, &key_path, None,
    ).unwrap();

    let srv_addr = format!("127.0.0.1:{}", port);
    let _server = tokio::spawn(async move {
        let addr = srv_addr.parse().unwrap();
        tonic::transport::Server::builder()
            .tls_config(server_tls)
            .unwrap()
            .add_service(
                coord_proto::maintenance::maintenance_server::MaintenanceServer::new(
                    MockMaintenance::default(),
                ),
            )
            .serve(addr)
            .await
            .unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let tls_config = AgentTlsConfig {
        cert_path: cert_path.clone(),
        key_path: key_path.clone(),
        ca_path: Some(cert_path.clone()),
    };

    let result = coord_agent::build_agent_tls_channel(
        &format!("https://127.0.0.1:{}", port),
        &tls_config,
    ).await;

    assert!(result.is_ok(), "TLS connection failed: {:?}", result.err());

    if let Ok(channel) = result {
        let mut client = coord_proto::maintenance::maintenance_client::MaintenanceClient::new(channel);
        let resp = client.status(coord_proto::maintenance::StatusRequest::default()).await;
        assert!(resp.is_ok(), "RPC over TLS failed: {:?}", resp.err());
    }

    let _ = std::fs::remove_file(&cert_path);
    let _ = std::fs::remove_file(&key_path);
}

/// A-mTLS.7: mTLS 模式下无有效客户端证书应被拒绝
#[tokio::test]
async fn test_mtls_rejects_without_client_cert() {
    let (server_cert, server_key) = generate_self_signed_cert("localhost");
    let (ca_cert, _ca_key) = generate_self_signed_cert("test-ca");

    let server_cert_path = write_temp_pem(&server_cert, "srv-cert");
    let server_key_path = write_temp_pem(&server_key, "srv-key");
    let ca_path = write_temp_pem(&ca_cert, "ca");

    let port = find_port();

    let server_tls = coord_agent::build_agent_tls_server_config(
        &server_cert_path, &server_key_path, Some(&ca_path),
    ).unwrap();

    let srv_addr = format!("127.0.0.1:{}", port);
    let _server = tokio::spawn(async move {
        let addr = srv_addr.parse().unwrap();
        tonic::transport::Server::builder()
            .tls_config(server_tls)
            .unwrap()
            .add_service(
                coord_proto::maintenance::maintenance_server::MaintenanceServer::new(
                    MockMaintenance::default(),
                ),
            )
            .serve(addr)
            .await
            .unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // 无客户端证书：证书路径指向不存在文件
    let tls_config = AgentTlsConfig {
        cert_path: PathBuf::from("/nonexistent/cli-cert.pem"),
        key_path: PathBuf::from("/nonexistent/cli-key.pem"),
        ca_path: Some(ca_path.clone()),
    };

    let result = coord_agent::build_agent_tls_channel(
        &format!("https://127.0.0.1:{}", port),
        &tls_config,
    ).await;

    assert!(result.is_err(), "mTLS should reject connections without valid client cert");

    let _ = std::fs::remove_file(&server_cert_path);
    let _ = std::fs::remove_file(&server_key_path);
    let _ = std::fs::remove_file(&ca_path);
}

// ──── Helpers ────

fn find_port() -> u16 {
    use std::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

#[derive(Default)]
struct MockMaintenance;

#[tonic::async_trait]
impl coord_proto::maintenance::maintenance_server::Maintenance for MockMaintenance {
    async fn status(
        &self,
        _request: tonic::Request<coord_proto::maintenance::StatusRequest>,
    ) -> Result<tonic::Response<coord_proto::maintenance::StatusResponse>, tonic::Status> {
        Ok(tonic::Response::new(coord_proto::maintenance::StatusResponse {
            revision: 42,
            raft_index: 1,
            raft_term: 1,
            raft_leader: "node-1".into(),
            seal_status: "unsealed".into(),
        }))
    }

    async fn seal(&self, _: tonic::Request<coord_proto::maintenance::SealRequest>)
        -> Result<tonic::Response<coord_proto::maintenance::SealResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    async fn unseal(&self, _: tonic::Request<coord_proto::maintenance::UnsealRequest>)
        -> Result<tonic::Response<coord_proto::maintenance::UnsealResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }

    type SnapshotStream = std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<coord_proto::maintenance::SnapshotResponse, tonic::Status>> + Send>>;
    async fn snapshot(&self, _: tonic::Request<coord_proto::maintenance::SnapshotRequest>)
        -> Result<tonic::Response<Self::SnapshotStream>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }

    async fn member_add(&self, _: tonic::Request<coord_proto::maintenance::MemberAddRequest>)
        -> Result<tonic::Response<coord_proto::maintenance::MemberAddResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    async fn member_remove(&self, _: tonic::Request<coord_proto::maintenance::MemberRemoveRequest>)
        -> Result<tonic::Response<coord_proto::maintenance::MemberRemoveResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    async fn member_promote(&self, _: tonic::Request<coord_proto::maintenance::MemberPromoteRequest>)
        -> Result<tonic::Response<coord_proto::maintenance::MemberPromoteResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
    async fn member_list(&self, _: tonic::Request<coord_proto::maintenance::MemberListRequest>)
        -> Result<tonic::Response<coord_proto::maintenance::MemberListResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(""))
    }
}
