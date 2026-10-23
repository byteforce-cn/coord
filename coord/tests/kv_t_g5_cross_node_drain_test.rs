//! KV-T G5（#167）跨协调节点 drain 的节点间 RPC 验收：
//!
//! - `DrainRange` 经 raft 端口把区间照原样送到接收方（空 `end` = +∞）；
//! - 接收方可达且排空 → `Drained`；仍有会话 → `Blocked`（迁移失败重试）；
//! - 未装配（`[kv_t]` 关闭的节点）→ RPC 错误；
//! - 连不上 → 传输层错误（调用方按节点失联跳过）。
//!
//! 断言使用真实 tonic server/客户端与 postcard/HMAC 载荷路径；会话表语义
//! 由 `kvt/txn.rs` 的本地 drain 单测覆盖，这里只验证扇出协议。

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use coord_server::raft::network::{
    DrainRpcOutcome, RaftNetworkFactoryImpl, RaftRpcServer, RaftRpcService,
};
use coord_server::split_merge::SplitDrain;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

#[derive(Default)]
struct Observed {
    calls: parking_lot::Mutex<Vec<(Vec<u8>, Vec<u8>, Duration)>>,
}

struct FakeDrain {
    outcome: Result<(), String>,
    observed: Arc<Observed>,
}

impl FakeDrain {
    fn new(outcome: Result<(), String>) -> (Arc<Self>, Arc<Observed>) {
        let observed = Arc::new(Observed::default());
        (
            Arc::new(Self {
                outcome,
                observed: Arc::clone(&observed),
            }),
            observed,
        )
    }
}

#[async_trait]
impl SplitDrain for FakeDrain {
    async fn drain(&self, start: &[u8], end: &[u8], timeout: Duration) -> Result<(), String> {
        self.observed
            .calls
            .lock()
            .push((start.to_vec(), end.to_vec(), timeout));
        self.outcome.clone()
    }
}

async fn start_rpc_server(drain: Option<Arc<dyn SplitDrain>>) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind raft rpc");
    let addr = listener.local_addr().expect("raft rpc addr");
    let service = RaftRpcService::new();
    service.set_split_drain(drain);
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(RaftRpcServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    addr
}

fn factory_to(node_id: u64, addr: std::net::SocketAddr) -> RaftNetworkFactoryImpl {
    let factory = RaftNetworkFactoryImpl::new(1);
    factory.register_node(node_id, addr.to_string());
    factory
}

#[tokio::test]
async fn test_drain_range_rpc_roundtrip() {
    let (drain, observed) = FakeDrain::new(Ok(()));
    let addr = start_rpc_server(Some(drain)).await;
    let factory = factory_to(2, addr);

    let outcome = factory
        .drain_range(2, b"m", b"z", Duration::from_millis(250))
        .await
        .expect("reachable node");
    assert!(matches!(outcome, DrainRpcOutcome::Drained));

    let calls = observed.calls.lock().clone();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, b"m");
    assert_eq!(calls[0].1, b"z");
    assert_eq!(calls[0].2, Duration::from_millis(250));
}

#[tokio::test]
async fn test_drain_range_rpc_forwards_an_unbounded_end() {
    let (drain, observed) = FakeDrain::new(Ok(()));
    let addr = start_rpc_server(Some(drain)).await;
    let factory = factory_to(2, addr);

    factory
        .drain_range(2, b"m", b"", Duration::from_millis(50))
        .await
        .expect("reachable node");
    let calls = observed.calls.lock().clone();
    assert_eq!(
        calls[0].1, b"",
        "empty end means +infinity, not an empty range"
    );
}

#[tokio::test]
async fn test_drain_range_rpc_reports_blocked() {
    let (drain, _observed) = FakeDrain::new(Err("1 active transaction(s)".to_string()));
    let addr = start_rpc_server(Some(drain)).await;
    let factory = factory_to(2, addr);

    let outcome = factory
        .drain_range(2, b"m", b"z", Duration::from_millis(50))
        .await
        .expect("a reachable blocked peer is not a transport error");
    match outcome {
        DrainRpcOutcome::Blocked(reason) => {
            assert!(reason.contains("1 active transaction"), "reason: {reason}");
        }
        DrainRpcOutcome::Drained => panic!("an active session must not report drained"),
    }
}

#[tokio::test]
async fn test_drain_range_rpc_requires_handler() {
    let addr = start_rpc_server(None).await;
    let factory = factory_to(2, addr);

    let err = factory
        .drain_range(2, b"m", b"z", Duration::from_millis(50))
        .await
        .expect_err("a node without the kv_t assembly cannot drain");
    assert!(err.contains("split drain is not assembled"), "err: {err}");
}

#[tokio::test]
async fn test_drain_range_rpc_unreachable() {
    // 绑定后立即释放：地址存在但无人监听（连接失败，不是 drain 超时）。
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    drop(listener);

    let factory = factory_to(2, addr);
    let err = factory
        .drain_range(2, b"m", b"z", Duration::from_millis(50))
        .await
        .expect_err("an unreachable node must return a transport error");
    assert!(err.contains("connect to node 2"), "err: {err}");
}
