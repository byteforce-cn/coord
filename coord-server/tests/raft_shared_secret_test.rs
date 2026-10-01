// 验收测试（L2 进程内）：raft 端口共享密钥 HMAC 认证
//
// - 未携带有效 auth_tag 的节点无法注入 Vote/AppendEntries/Snapshot（UNAUTHENTICATED）；
// - 携带正确 tag 的消息通过认证层（后续由 raft 实例处理）；
// - 篡改 payload 后 tag 失效。

use std::net::TcpListener;
use std::time::Duration;

use coord_proto::raft::raft_client::RaftClient;
use coord_proto::raft::RaftMessage;
use coord_server::raft::network::{RaftRpcServer, RaftRpcService};

fn find_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn hmac_sha256(payload: &[u8], secret: &[u8]) -> Vec<u8> {
    use hmac::{Hmac, Mac};
    type HmacSha256 = Hmac<sha2::Sha256>;
    let mut mac = HmacSha256::new_from_slice(secret).unwrap();
    mac.update(payload);
    mac.finalize().into_bytes().to_vec()
}

fn raft_msg(payload: Vec<u8>, tag: Vec<u8>) -> RaftMessage {
    RaftMessage {
        payload,
        region_id: 0,
        trace_context: Vec::new(),
        auth_tag: tag,
    }
}

/// 启动共享密钥 raft RPC 服务（不设置 raft 实例：认证在 raft 访问之前）。
async fn start_raft_server(secret: &str) -> (String, tokio::task::JoinHandle<()>) {
    let addr = format!("127.0.0.1:{}", find_port());
    let sock_addr: std::net::SocketAddr = addr.parse().unwrap();
    let svc = RaftRpcServer::new(RaftRpcService::new().with_shared_secret(Some(secret)));
    let handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(svc)
            .serve(sock_addr)
            .await;
    });
    // 等待就绪
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "raft server never ready"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    (addr, handle)
}

#[tokio::test]
async fn test_raft_shared_secret_rejects_untagged_messages() {
    let secret = "integration-secret-16+chars";
    let (addr, handle) = start_raft_server(secret).await;

    let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = RaftClient::new(channel);

    // 1. 无 tag 的 AppendEntries → UNAUTHENTICATED
    let resp = client
        .append_entries(tonic::Request::new(raft_msg(
            b"payload".to_vec(),
            Vec::new(),
        )))
        .await;
    assert!(
        matches!(resp, Err(ref s) if s.code() == tonic::Code::Unauthenticated),
        "untagged raft message must be rejected: {resp:?}"
    );

    // 2. 错误 tag → UNAUTHENTICATED
    let resp = client
        .vote(tonic::Request::new(raft_msg(
            b"payload".to_vec(),
            vec![0u8; 32],
        )))
        .await;
    assert!(
        matches!(resp, Err(ref s) if s.code() == tonic::Code::Unauthenticated),
        "bad-tagged raft message must be rejected: {resp:?}"
    );

    // 3. 篡改 payload 后正确 tag 失效 → UNAUTHENTICATED
    let tag = hmac_sha256(b"original-payload", secret.as_bytes());
    let resp = client
        .append_entries(tonic::Request::new(raft_msg(
            b"tampered-payload".to_vec(),
            tag,
        )))
        .await;
    assert!(
        matches!(resp, Err(ref s) if s.code() == tonic::Code::Unauthenticated),
        "tampered payload must be rejected: {resp:?}"
    );

    handle.abort();
}

#[tokio::test]
async fn test_raft_shared_secret_accepts_valid_tag() {
    let secret = "integration-secret-16+chars";
    let (addr, handle) = start_raft_server(secret).await;

    let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = RaftClient::new(channel);

    // 正确 tag → 通过认证层（raft 未初始化 → INTERNAL，非 UNAUTHENTICATED）
    let payload = b"valid-payload".to_vec();
    let tag = hmac_sha256(&payload, secret.as_bytes());
    let resp = client
        .append_entries(tonic::Request::new(raft_msg(payload, tag)))
        .await;
    match resp {
        Err(s) => {
            assert_ne!(
                s.code(),
                tonic::Code::Unauthenticated,
                "valid tag must pass auth layer: {s:?}"
            );
        }
        Ok(_) => {}
    }

    handle.abort();
}
