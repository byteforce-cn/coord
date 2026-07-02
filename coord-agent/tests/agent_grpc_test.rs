// TDD: Agent gRPC Server 启动测试 (Phase B1 — RED)
//
// 验证 Agent 可以：
// 1. 在指定端口启动 gRPC server
// 2. 注册全部 5 个 gRPC 服务（KV/Txn/Lease/Watch/Maintenance）
// 3. 接受客户端连接并响应请求
//
// RED 阶段：run_agent 尚未实现 gRPC server 启动，此测试预期失败。

use std::time::Duration;

use coord_agent::{AgentConfig, AgentServer};
use coord_proto::kv::kv_client::KvClient;
use coord_proto::kv::PutRequest;
use coord_proto::txn::txn_client::TxnClient;
use coord_proto::txn::TxnRequest;
use coord_proto::lease::lease_client::LeaseClient;
use coord_proto::lease::LeaseGrantRequest;
use coord_proto::watch::watch_client::WatchClient;
use coord_proto::maintenance::maintenance_client::MaintenanceClient;
use coord_proto::maintenance::StatusRequest;

/// Find an available TCP port on localhost
fn find_port() -> u16 {
    use std::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn test_config(port: u16) -> AgentConfig {
    AgentConfig {
        agent_addr: format!("127.0.0.1:{}", port),
        http_addr: format!("127.0.0.1:{}", find_port()),
        data_dir: "/tmp/coord-agent-test".into(),
        static_peers: vec![],  // B1 骨架模式：不连接真实 Server
        ..Default::default()
    }
}

/// B1.1: Agent gRPC server 能启动并监听指定端口
#[tokio::test]
async fn test_agent_grpc_server_starts_and_listens() {
    let port = find_port();
    let config = test_config(port);
    let addr = config.agent_addr.clone();

    // 启动 Agent gRPC server（后台任务）
    let server = AgentServer::new(config.clone());
    let handle = tokio::spawn(async move {
        server.serve().await.unwrap();
    });

    // 等待 server 启动
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 验证端口已监听
    let stream = tokio::net::TcpStream::connect(&addr).await;
    assert!(stream.is_ok(), "Agent gRPC server should listen on {addr}");
    drop(stream);

    handle.abort();
}

/// B1.2: KV 服务已注册，可接受 gRPC 调用
#[tokio::test]
async fn test_agent_kv_service_registered() {
    let port = find_port();
    let config = test_config(port);
    let addr = config.agent_addr.clone();

    let server = AgentServer::new(config);
    let handle = tokio::spawn(async move {
        server.serve().await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 建立 gRPC 连接并调用 KV::Put
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .expect("should connect to agent gRPC");

    let mut kv_client = KvClient::new(channel);
    let resp = kv_client
        .put(PutRequest {
            key: b"test_key".to_vec(),
            value: b"test_value".to_vec(),
            ..Default::default()
        })
        .await;

    // RED 阶段：当前 Agent 未实现 KV 代理，预期失败
    // GREEN 阶段：应返回 Ok 响应
    assert!(resp.is_ok(), "KV Put should succeed: {resp:?}");

    handle.abort();
}

/// B1.3: Txn/Lease/Watch/Maintenance 服务全部注册
#[tokio::test]
async fn test_agent_all_services_registered() {
    let port = find_port();
    let config = test_config(port);
    let addr = config.agent_addr.clone();

    let server = AgentServer::new(config);
    let handle = tokio::spawn(async move {
        server.serve().await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .expect("should connect");

    // Txn service
    let mut txn_client = TxnClient::new(channel.clone());
    let txn_resp = txn_client
        .txn(TxnRequest::default())
        .await;
    assert!(txn_resp.is_ok(), "Txn should be registered: {txn_resp:?}");

    // Lease service
    let mut lease_client = LeaseClient::new(channel.clone());
    let lease_resp = lease_client
        .lease_grant(LeaseGrantRequest { ttl: 30, id: 0 })
        .await;
    assert!(lease_resp.is_ok(), "Lease should be registered: {lease_resp:?}");

    // Watch service (bidirectional streaming — 验证服务已注册)
    let watch_client = WatchClient::new(channel.clone());
    // 仅验证 stub 可以构造并连接（stream 调用在 Phase B4 详细测试）
    let _ = watch_client; // 服务注册验证：若服务未注册，构造不会失败但首帧会报错

    // Maintenance service
    let mut maint_client = MaintenanceClient::new(channel.clone());
    let status_resp = maint_client
        .status(StatusRequest {})
        .await;
    assert!(status_resp.is_ok(), "Maintenance should be registered: {status_resp:?}");

    handle.abort();
}
