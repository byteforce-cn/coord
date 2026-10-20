//! KV-T G4（#158）`coord.watch.v2` 端到端验收：
//!
//! - Create 建立订阅、服务端先回空批 ack；Cancel 后不再产出事件；
//! - 事件按 `commit_ts` 全局有序，同一事务原子批；
//! - 断线用响应里的 `WatchCursor` 续订，不重放上一事件；
//! - 历史窗口不足返回 `OUT_OF_RANGE` + `HISTORY_UNAVAILABLE`；
//! - `[kv_t]` 关闭/未装配 shard、保留命名空间、`/obj/` 的拒绝路径。
//!
//! 客户端包装的续订/重连见 `watch_client_*` 用例。

#[path = "support/kv_t_watch_node.rs"]
mod watch_node;

use std::sync::Arc;
use std::time::Duration;

use coord_core::error_code::error_code_of;
use coord_core::storage::StorageBackend;
use coord_core::types::StorageConfig;
use coord_proto::watch::v2 as watch_v2;
use coord_proto::watch::v2::watch_client::WatchClient;
use coord_server::kvt::{KvTCapability, KvTRuntimeConfig, KvTState};
use coord_server::server::CoordNode;
use coord_server::storage::mvcc::{MvccStorage, RevisionMode};
use coord_server::storage::redb_backend::RedbBackend;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Server};
use tonic::{Code, Request, Streaming};
use watch_node::WatchNode;
use watch_v2::watch_server::WatchServer;

use watch_v2::watch_request::Request as Frame;
use watch_v2::WatchCreateRequest;

fn create(key: &[u8], range_end: &[u8], start_revision: i64) -> WatchCreateRequest {
    WatchCreateRequest {
        key: key.to_vec(),
        range_end: range_end.to_vec(),
        start_revision,
        start_cursor: None,
        prev_kv: false,
        filter: 1,
    }
}

#[derive(Debug)]
struct OpenWatch {
    tx: mpsc::Sender<watch_v2::WatchRequest>,
    stream: Streaming<watch_v2::WatchResponse>,
}

async fn connect(addr: &str) -> WatchClient<Channel> {
    let channel = Channel::from_shared(format!("http://{addr}"))
        .expect("endpoint")
        .connect()
        .await
        .expect("connect");
    WatchClient::new(channel)
}

async fn open_watch(addr: &str, create: WatchCreateRequest) -> Result<OpenWatch, tonic::Status> {
    let mut stub = connect(addr).await;
    let (tx, rx) = mpsc::channel(4);
    tx.send(watch_v2::WatchRequest {
        request: Some(Frame::Create(create)),
    })
    .await
    .expect("send create");
    let response = stub
        .watch(Request::new(ReceiverStream::new(rx)))
        .await?
        .into_inner();
    Ok(OpenWatch {
        tx,
        stream: response,
    })
}

async fn next_response(open: &mut OpenWatch) -> watch_v2::WatchResponse {
    tokio::time::timeout(Duration::from_secs(10), open.stream.message())
        .await
        .expect("watch response timed out")
        .expect("watch stream error")
        .expect("watch stream closed")
}

/// 收集事件直到至少 `min_events` 条；返回（全部事件，最后一批响应）。
async fn collect_events(
    open: &mut OpenWatch,
    min_events: usize,
) -> (Vec<watch_v2::WatchEvent>, watch_v2::WatchResponse) {
    let mut events = Vec::new();
    loop {
        let response = next_response(open).await;
        events.extend(response.events.iter().cloned());
        if events.len() >= min_events {
            return (events, response);
        }
    }
}

async fn serve_watch_only(
    node: Arc<CoordNode>,
) -> (String, oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let (tx, rx) = oneshot::channel();
    let handle = tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(WatchServer::from_arc(node))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = rx.await;
            })
            .await;
    });
    (addr, tx, handle)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_watch_rpc_streams_global_order() {
    let fx = WatchNode::start().await;
    fx.open_first_shard().await;
    let ts1 = fx.commit_pairs(&[(b"aa", b"v1")]).await;
    let ts2 = fx.commit_pairs(&[(b"bb", b"v2")]).await;
    assert!(ts2 > ts1);

    let server = fx.serve().await;
    let mut open = open_watch(&server.addr, create(b"", b"", ts1 as i64))
        .await
        .expect("open watch");
    let (events, last) = collect_events(&mut open, 2).await;
    let timestamps: Vec<i64> = events.iter().map(|event| event.commit_ts).collect();
    assert_eq!(
        timestamps,
        vec![ts1 as i64, ts2 as i64],
        "strict global order"
    );

    let cursor = last.cursor.expect("cursor must be echoed");
    assert_eq!(cursor.commit_ts as u64, ts2);
    assert!(
        (last.watermark as u64) >= ts2,
        "watermark must cover the delivered events: {}",
        last.watermark
    );
    drop(open);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_watch_rpc_ack_and_cancel() {
    let fx = WatchNode::start().await;
    fx.open_first_shard().await;
    let server = fx.serve().await;

    // 还没有任何 v2 提交：首条响应必须是空批 ack，并回显解析后的 cursor。
    let mut open = open_watch(&server.addr, create(b"", b"", 0))
        .await
        .expect("open watch");
    let ack = next_response(&mut open).await;
    assert!(ack.events.is_empty(), "first response is the ack");
    assert_eq!(ack.watch_id, 1);
    let cursor = ack.cursor.expect("ack carries the resolved cursor");
    assert_eq!(cursor.commit_ts, 0, "ack resolves to the shard start");

    let ts1 = fx.commit_pairs(&[(b"aa", b"v1")]).await;
    let (events, _) = collect_events(&mut open, 1).await;
    assert_eq!(events[0].commit_ts as u64, ts1);

    // cancel 后即使有新提交也不再产出事件。
    open.tx
        .send(watch_v2::WatchRequest {
            request: Some(Frame::Cancel(watch_v2::WatchCancelRequest {
                watch_id: ack.watch_id,
            })),
        })
        .await
        .expect("send cancel");
    tokio::time::sleep(Duration::from_millis(200)).await;
    fx.commit_pairs(&[(b"bb", b"v2")]).await;
    let quiet = tokio::time::timeout(Duration::from_millis(700), open.stream.message()).await;
    assert!(
        quiet.is_err(),
        "cancelled watch must not deliver more events: {quiet:?}"
    );
    drop(open);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_watch_rpc_resumes_from_cursor() {
    let fx = WatchNode::start().await;
    fx.open_first_shard().await;
    let ts1 = fx.commit_pairs(&[(b"aa", b"v1")]).await;
    let server = fx.serve().await;

    let mut open = open_watch(&server.addr, create(b"", b"", ts1 as i64))
        .await
        .expect("open watch");
    let (events, response) = collect_events(&mut open, 1).await;
    assert_eq!(events[0].commit_ts as u64, ts1);
    let cursor = response.cursor.expect("cursor");
    drop(open);

    let ts2 = fx.commit_pairs(&[(b"bb", b"v2")]).await;
    let mut resumed_create = create(b"", b"", 0);
    resumed_create.start_cursor = Some(watch_v2::WatchCursor {
        shard_id: cursor.shard_id,
        commit_ts: cursor.commit_ts,
    });
    let mut resumed = open_watch(&server.addr, resumed_create)
        .await
        .expect("resume watch");
    let (events, _) = collect_events(&mut resumed, 1).await;
    assert_eq!(
        events.iter().map(|e| e.commit_ts).collect::<Vec<_>>(),
        vec![ts2 as i64],
        "resuming from the cursor must not replay the consumed event"
    );
    drop(resumed);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_watch_rpc_history_unavailable() {
    let fx = WatchNode::start().await;
    fx.open_first_shard().await;
    let _first_ts = fx.commit_pairs(&[(b"aa", b"v1")]).await;
    let second_ts = fx.commit_pairs(&[(b"bb", b"v2")]).await;
    fx.wait_until_sealed(1).await;

    let gc_before = fx.shards.table().unwrap().shards[0].end_ts;
    assert_eq!(gc_before, second_ts);
    fx.advance_clock_ms(60_000);
    fx.wait_until_gc_before(gc_before).await;

    let server = fx.serve().await;
    let err = open_watch(&server.addr, create(b"", b"", 1))
        .await
        .expect_err("reclaimed history must be rejected");
    assert_eq!(err.code(), Code::OutOfRange);
    assert_eq!(error_code_of(&err).as_deref(), Some("HISTORY_UNAVAILABLE"));
    assert!(
        err.message().contains("oldest retained position"),
        "oldest available position must be visible: {}",
        err.message()
    );
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_watch_rpc_filter_and_prev_kv() {
    let fx = WatchNode::start().await;
    fx.open_first_shard().await;
    let ts = fx.commit_pairs(&[(b"aa", b"v1"), (b"zz", b"v2")]).await;
    let server = fx.serve().await;

    let mut create = create(b"aa", b"", ts as i64);
    create.prev_kv = true;
    let mut open = open_watch(&server.addr, create).await.expect("open watch");
    let (events, _) = collect_events(&mut open, 1).await;
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].changes.len(),
        1,
        "filter keeps only matching keys"
    );
    let change = &events[0].changes[0];
    assert_eq!(change.key, b"aa");
    assert_eq!(change.value, b"v1");
    assert_eq!(change.r#type, watch_v2::change::Type::Put as i32);
    let prev = change.prev_kv.as_ref().expect("prev_kv requested");
    assert_eq!(prev.key, b"aa");
    assert_eq!(prev.value, b"seed");
    drop(open);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_watch_rpc_gate_guards_and_reserved_namespaces() {
    let dir = tempfile::tempdir().unwrap();
    let backend = RedbBackend::open(dir.path(), &StorageConfig::default()).unwrap();
    let mvcc =
        Arc::new(MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs).unwrap());

    let disabled = Arc::new(CoordNode::new(Arc::clone(&mvcc)));
    let (addr, shutdown, handle) = serve_watch_only(Arc::clone(&disabled)).await;
    let err = open_watch(&addr, create(b"/app/a", b"", 1))
        .await
        .expect_err("v2 disabled");
    assert_eq!(err.code(), Code::FailedPrecondition);
    let _ = shutdown.send(());
    let _ = handle.await;

    let mut enabled = CoordNode::new(Arc::clone(&mvcc));
    enabled.set_kvt_state(
        KvTState::new(true, KvTRuntimeConfig::default()).with_implemented([KvTCapability::Watch]),
    );
    let enabled = Arc::new(enabled);
    let (addr, shutdown, handle) = serve_watch_only(Arc::clone(&enabled)).await;
    let err = open_watch(&addr, create(b"/app/a", b"", 1))
        .await
        .expect_err("shard assembly missing");
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(err.message().contains("Commit Log shard"));
    let _ = shutdown.send(());
    let _ = handle.await;

    let fx = WatchNode::start().await;
    fx.open_first_shard().await;
    let server = fx.serve().await;
    let err = open_watch(&server.addr, create(b"/_pki/x", b"", 1))
        .await
        .expect_err("reserved namespace");
    assert_eq!(err.code(), Code::PermissionDenied);
    let err = open_watch(&server.addr, create(b"/obj/a", b"", 1))
        .await
        .expect_err("object namespace");
    assert_eq!(err.code(), Code::InvalidArgument);
    server.shutdown().await;
}

// ──── coord-client 包装：cursor 续订、断线重连、历史不足终止 ────

async fn client_for(addr: &str) -> coord_client::Client {
    coord_client::Client::new(
        coord_client::Config::new(vec![addr.to_string()]).with_max_retries(10),
    )
    .await
    .expect("client")
}

async fn client_batch(sub: &mut coord_client::WatchV2Subscription) -> coord_client::WatchV2Batch {
    tokio::time::timeout(Duration::from_secs(10), sub.recv())
        .await
        .expect("client batch timed out")
        .expect("subscription closed")
        .expect("subscription error")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_watch_client_resumes_from_cursor() {
    let fx = WatchNode::start().await;
    fx.open_first_shard().await;
    let ts1 = fx.commit_pairs(&[(b"aa", b"v1")]).await;
    let server = fx.serve().await;
    let client = client_for(&server.addr).await;

    let mut sub = client
        .watch_v2()
        .watch(coord_client::WatchV2Options::new(b"").from_revision(ts1))
        .await
        .expect("subscribe");
    let first = client_batch(&mut sub).await;
    assert_eq!(
        first.events.iter().map(|e| e.commit_ts).collect::<Vec<_>>(),
        vec![ts1]
    );
    let cursor = sub.cursor().expect("cursor after the first batch");
    sub.cancel().await;

    let ts2 = fx.commit_pairs(&[(b"bb", b"v2")]).await;
    let mut resumed = client
        .watch_v2()
        .watch(coord_client::WatchV2Options::new(b"").from_cursor(cursor))
        .await
        .expect("resume");
    let next = client_batch(&mut resumed).await;
    assert_eq!(
        next.events.iter().map(|e| e.commit_ts).collect::<Vec<_>>(),
        vec![ts2],
        "cursor resume must not replay the consumed batch"
    );
    resumed.cancel().await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_watch_client_reconnects_after_server_restart() {
    let fx = WatchNode::start().await;
    fx.open_first_shard().await;
    let ts1 = fx.commit_pairs(&[(b"aa", b"v1")]).await;
    let server = fx.serve().await;
    let port: u16 = server.addr.rsplit(':').next().unwrap().parse().unwrap();
    let client = client_for(&server.addr).await;

    let mut sub = client
        .watch_v2()
        .watch(coord_client::WatchV2Options::new(b"").from_revision(ts1))
        .await
        .expect("subscribe");
    let first = client_batch(&mut sub).await;
    assert_eq!(
        first.events.iter().map(|e| e.commit_ts).collect::<Vec<_>>(),
        vec![ts1]
    );

    // 模拟进程/网络故障：立即断开全部流；数据面（raft/shard）仍在。
    server.abort();
    let ts2 = fx.commit_pairs(&[(b"bb", b"v2")]).await;
    let server = fx.serve_at(port).await;

    let next = client_batch(&mut sub).await;
    assert_eq!(
        next.events.iter().map(|e| e.commit_ts).collect::<Vec<_>>(),
        vec![ts2],
        "the wrapper must resume from the last cursor without replaying ts1"
    );
    sub.cancel().await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_watch_client_history_unavailable_is_terminal() {
    let fx = WatchNode::start().await;
    fx.open_first_shard().await;
    let _first_ts = fx.commit_pairs(&[(b"aa", b"v1")]).await;
    let second_ts = fx.commit_pairs(&[(b"bb", b"v2")]).await;
    fx.wait_until_sealed(1).await;
    let gc_before = fx.shards.table().unwrap().shards[0].end_ts;
    assert_eq!(gc_before, second_ts);
    fx.advance_clock_ms(60_000);
    fx.wait_until_gc_before(gc_before).await;

    let server = fx.serve().await;
    let client = client_for(&server.addr).await;
    let mut sub = client
        .watch_v2()
        .watch(coord_client::WatchV2Options::new(b"").from_revision(1))
        .await
        .expect("subscribe");

    let err = tokio::time::timeout(Duration::from_secs(5), sub.recv())
        .await
        .expect("history error timed out")
        .expect("subscription closed without an error")
        .expect_err("reclaimed history must fail the subscription");
    assert!(
        matches!(err, coord_core::error::Error::HistoryUnavailable { .. }),
        "unexpected error: {err}"
    );
    // 终止而不是重连：通道随即关闭。
    assert!(tokio::time::timeout(Duration::from_secs(1), sub.recv())
        .await
        .expect("subscription must end after a terminal error")
        .is_none());
    server.shutdown().await;
}
