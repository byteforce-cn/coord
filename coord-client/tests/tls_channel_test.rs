// TDD: coord-client TLS 通道测试（RED→GREEN）
//
// 验证 Config.tls 在实际连接路径生效（Leader 发现 + 连接池走 TLS）：
// 1. 服务端 TLS：CA 校验 + server name 从 endpoint host 派生
// 2. mTLS：未配置客户端身份被服务端拒绝
// 3. server_name 覆盖：经 IP 连接 DNS SAN 证书

use coord_client::config::{Config, TlsConfig};

// ──── 证书生成 ────

fn generate_ip_san_cert(ip: &str) -> (Vec<u8>, Vec<u8>) {
    use rcgen::{CertificateParams, KeyPair, SanType};
    let ip = ip.parse::<std::net::IpAddr>().unwrap();
    let key_pair = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.subject_alt_names = vec![SanType::IpAddress(ip)];
    let cert = params.self_signed(&key_pair).unwrap();
    (
        cert.pem().into_bytes(),
        key_pair.serialize_pem().into_bytes(),
    )
}

fn generate_dns_cert(dns_name: &str) -> (Vec<u8>, Vec<u8>) {
    let cert = rcgen::generate_simple_self_signed(vec![dns_name.into()]).unwrap();
    (
        cert.cert.pem().into_bytes(),
        cert.signing_key.serialize_pem().into_bytes(),
    )
}

fn find_port() -> u16 {
    use std::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

// ──── Mock Maintenance（Leader 发现依赖 Status RPC）───

#[derive(Default)]
struct MockMaintenance;

#[tonic::async_trait]
impl coord_proto::maintenance::maintenance_server::Maintenance for MockMaintenance {
    type SnapshotStream = std::pin::Pin<
        Box<
            dyn tokio_stream::Stream<
                    Item = Result<coord_proto::maintenance::SnapshotResponse, tonic::Status>,
                > + Send,
        >,
    >;

    async fn status(
        &self,
        _request: tonic::Request<coord_proto::maintenance::StatusRequest>,
    ) -> Result<tonic::Response<coord_proto::maintenance::StatusResponse>, tonic::Status> {
        Ok(tonic::Response::new(
            coord_proto::maintenance::StatusResponse {
                raft_leader: "1".into(),
                ..Default::default()
            },
        ))
    }

    async fn seal(
        &self,
        _: tonic::Request<coord_proto::maintenance::SealRequest>,
    ) -> Result<tonic::Response<coord_proto::maintenance::SealResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("test"))
    }

    async fn unseal(
        &self,
        _: tonic::Request<coord_proto::maintenance::UnsealRequest>,
    ) -> Result<tonic::Response<coord_proto::maintenance::UnsealResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("test"))
    }

    async fn snapshot(
        &self,
        _: tonic::Request<coord_proto::maintenance::SnapshotRequest>,
    ) -> Result<tonic::Response<Self::SnapshotStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("test"))
    }

    async fn compact(
        &self,
        _: tonic::Request<coord_proto::maintenance::CompactRequest>,
    ) -> Result<tonic::Response<coord_proto::maintenance::CompactResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("test"))
    }

    async fn member_add(
        &self,
        _: tonic::Request<coord_proto::maintenance::MemberAddRequest>,
    ) -> Result<tonic::Response<coord_proto::maintenance::MemberAddResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("test"))
    }

    async fn member_remove(
        &self,
        _: tonic::Request<coord_proto::maintenance::MemberRemoveRequest>,
    ) -> Result<tonic::Response<coord_proto::maintenance::MemberRemoveResponse>, tonic::Status>
    {
        Err(tonic::Status::unimplemented("test"))
    }

    async fn member_promote(
        &self,
        _: tonic::Request<coord_proto::maintenance::MemberPromoteRequest>,
    ) -> Result<tonic::Response<coord_proto::maintenance::MemberPromoteResponse>, tonic::Status>
    {
        Err(tonic::Status::unimplemented("test"))
    }

    async fn member_list(
        &self,
        _: tonic::Request<coord_proto::maintenance::MemberListRequest>,
    ) -> Result<tonic::Response<coord_proto::maintenance::MemberListResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("test"))
    }

    async fn join(
        &self,
        _: tonic::Request<coord_proto::maintenance::JoinRequest>,
    ) -> Result<tonic::Response<coord_proto::maintenance::JoinResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("test"))
    }
}

/// 启动 TLS gRPC 服务（可选用 client_ca 开启 mTLS）
async fn spawn_tls_server(cert: Vec<u8>, key: Vec<u8>, client_ca: Option<Vec<u8>>) -> String {
    let mut server_tls = tonic::transport::server::ServerTlsConfig::new()
        .identity(tonic::transport::Identity::from_pem(&cert, &key));
    if let Some(ca) = client_ca {
        server_tls = server_tls.client_ca_root(tonic::transport::Certificate::from_pem(&ca));
    }
    let port = find_port();
    let addr = format!("127.0.0.1:{port}");
    let bind_addr = addr.clone();
    let _server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .tls_config(server_tls)
            .unwrap()
            .add_service(
                coord_proto::maintenance::maintenance_server::MaintenanceServer::new(
                    MockMaintenance::default(),
                ),
            )
            .serve(bind_addr.parse().unwrap())
            .await
            .unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    addr
}

// ──── T1: 服务端 TLS 直连 ────

#[tokio::test]
async fn test_client_connects_to_tls_server() {
    let (cert, key) = generate_ip_san_cert("127.0.0.1");
    let addr = spawn_tls_server(cert.clone(), key, None).await;

    let config = Config::new(vec![addr]).with_tls(TlsConfig {
        ca_pem: cert,
        client_cert_pem: None,
        client_key_pem: None,
        server_name: None,
    });
    let client = coord_client::Client::connect_direct(config).await.unwrap();
    let status = client.maintenance().status().await;
    assert!(status.is_ok(), "status over TLS failed: {:?}", status.err());
}

// ──── T2: mTLS 缺客户端身份被拒 ────

#[tokio::test]
async fn test_client_without_identity_rejected_by_mtls_server() {
    let (cert, key) = generate_ip_san_cert("127.0.0.1");
    // 服务端以同一自签名证书为客户端 CA：无身份的客户端握手被拒
    let addr = spawn_tls_server(cert.clone(), key, Some(cert.clone())).await;

    let config = Config::new(vec![addr]).with_tls(TlsConfig {
        ca_pem: cert,
        client_cert_pem: None,
        client_key_pem: None,
        server_name: None,
    });
    let client = coord_client::Client::connect_direct(config).await.unwrap();
    let status = client.maintenance().status().await;
    assert!(
        status.is_err(),
        "mTLS server must reject clients without identity"
    );
}

// ──── T3: server_name 覆盖 ────

#[tokio::test]
async fn test_client_server_name_override_via_ip() {
    let (cert, key) = generate_dns_cert("coord.internal");
    let addr = spawn_tls_server(cert.clone(), key, None).await;

    let config = Config::new(vec![addr]).with_tls(TlsConfig {
        ca_pem: cert,
        client_cert_pem: None,
        client_key_pem: None,
        server_name: Some("coord.internal".to_string()),
    });
    let client = coord_client::Client::connect_direct(config).await.unwrap();
    let status = client.maintenance().status().await;
    assert!(
        status.is_ok(),
        "server_name override failed: {:?}",
        status.err()
    );
}
