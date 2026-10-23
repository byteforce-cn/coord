//! Agent 的 `coord.txn.v2` 代理端到端测试。
//!
//! 假 `coord.txn.v2` 服务记录调用，验证代理**真的转发**（同一 `txn_id` 的
//! Read/Commit 落在 Begin 选中的协调节点上），并保留 wire 码（`TXN_CONFLICT` /
//! `TXN_NOT_FOUND`，而不是被 gRPC 状态码兜底压成 `INTERNAL`）。
//!
//! 会话亲和的判据不是「leader 缓存没变」——那对无状态转发也成立。这里先把
//! leader 缓存真的切到另一个节点（一次 `UNAVAILABLE` + `coord-leader-hint` 的
//! v2 KV Put），再断言 Txn Read 仍回到 Begin 的节点：无状态实现会读到 B 的值
//! 或直接失败。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use coord_agent::cache::AgentCache;
use coord_agent::{AgentInner, TxnV2Proxy};
use coord_client::{Client, Config};
use coord_core::error_code::{attach, error_code_of, CoordErrorCode};
use coord_proto::kv::v2 as kv_v2;
use coord_proto::kv::v2::kv_server::{Kv as KvV2, KvServer as KvV2Server};
use coord_proto::txn::v2 as txn_v2;
use coord_proto::txn::v2::txn_server::{Txn as TxnV2, TxnServer as TxnV2Server};
use parking_lot::Mutex;
use tokio::net::TcpListener;

// ──── 假 coord.txn.v2 ────

#[derive(Default)]
struct TxnCalls {
    reads: Mutex<Vec<(Vec<u8>, Vec<u8>)>>,
    commits: Mutex<Vec<Vec<u8>>>,
    aborts: Mutex<Vec<Vec<u8>>>,
}

struct FakeTxn {
    label: &'static str,
    next: AtomicU64,
    calls: Arc<TxnCalls>,
    /// 第一次 Commit 返回 `UNAVAILABLE`，用于验证代理在传输失败后保留会话
    /// （同一 txn_id + request_id 的幂等重试）。
    fail_first_commit: AtomicU64,
}

impl FakeTxn {
    fn new(label: &'static str) -> (Self, Arc<TxnCalls>) {
        let calls = Arc::new(TxnCalls::default());
        (
            Self {
                label,
                next: AtomicU64::new(1),
                calls: Arc::clone(&calls),
                fail_first_commit: AtomicU64::new(0),
            },
            calls,
        )
    }
}

#[tonic::async_trait]
impl TxnV2 for FakeTxn {
    async fn begin(
        &self,
        request: tonic::Request<txn_v2::BeginRequest>,
    ) -> Result<tonic::Response<txn_v2::BeginResponse>, tonic::Status> {
        let req = request.into_inner();
        let seq = self.next.fetch_add(1, Ordering::SeqCst);
        let start_ts = if req.start_ts == 0 { 42 } else { req.start_ts };
        Ok(tonic::Response::new(txn_v2::BeginResponse {
            txn_id: format!("{}-{seq}", self.label).into_bytes(),
            start_ts,
        }))
    }

    async fn read(
        &self,
        request: tonic::Request<txn_v2::ReadRequest>,
    ) -> Result<tonic::Response<txn_v2::ReadResponse>, tonic::Status> {
        let req = request.into_inner();
        self.calls
            .reads
            .lock()
            .push((req.txn_id.clone(), req.key.clone()));
        Ok(tonic::Response::new(txn_v2::ReadResponse {
            kvs: vec![kv_v2::KeyValue {
                key: req.key,
                value: format!("from-{}", self.label).into_bytes(),
                create_revision: 42,
                mod_revision: 42,
                version: 1,
                lease_id: 0,
            }],
            read_ts: 42,
        }))
    }

    async fn commit(
        &self,
        request: tonic::Request<txn_v2::CommitRequest>,
    ) -> Result<tonic::Response<txn_v2::CommitResponse>, tonic::Status> {
        let req = request.into_inner();
        self.calls.commits.lock().push(req.txn_id.clone());
        // N>0 时让前 N 次 Commit 失败；用 fetch_sub 保证并发下也只消耗一次。
        if self.fail_first_commit.load(Ordering::SeqCst) > 0
            && self.fail_first_commit.fetch_sub(1, Ordering::SeqCst) > 0
        {
            return Err(tonic::Status::unavailable("commit outcome unknown"));
        }
        let conflict = req
            .mutations
            .iter()
            .any(|m| m.key == b"/conflict" || m.key == b"conflict");
        if conflict {
            return Err(attach(
                tonic::Status::aborted("transaction conflict at commit_ts 7"),
                CoordErrorCode::TxnConflict,
            ));
        }
        Ok(tonic::Response::new(txn_v2::CommitResponse {
            commit_ts: 100,
        }))
    }

    async fn abort(
        &self,
        request: tonic::Request<txn_v2::AbortRequest>,
    ) -> Result<tonic::Response<txn_v2::AbortResponse>, tonic::Status> {
        self.calls.aborts.lock().push(request.into_inner().txn_id);
        Ok(tonic::Response::new(txn_v2::AbortResponse {}))
    }
}

// ──── 假 coord.kv.v2（只为切换 leader 缓存）────

struct FakeKv {
    puts: AtomicU64,
    /// 非空时 Put 返回 `UNAVAILABLE` + 该地址的 `coord-leader-hint`。
    hint: Option<String>,
}

impl FakeKv {
    fn new(hint: Option<String>) -> Self {
        Self {
            puts: AtomicU64::new(0),
            hint,
        }
    }
}

#[tonic::async_trait]
impl KvV2 for FakeKv {
    async fn put(
        &self,
        request: tonic::Request<kv_v2::PutRequest>,
    ) -> Result<tonic::Response<kv_v2::PutResponse>, tonic::Status> {
        self.puts.fetch_add(1, Ordering::SeqCst);
        if let Some(hint) = &self.hint {
            let mut status = tonic::Status::unavailable("not the leader");
            status.metadata_mut().insert(
                "coord-leader-hint",
                hint.parse().expect("leader hint metadata"),
            );
            return Err(status);
        }
        let req = request.into_inner();
        Ok(tonic::Response::new(kv_v2::PutResponse {
            prev_kv: None,
            commit_ts: req.commit_ts,
        }))
    }

    async fn range(
        &self,
        _request: tonic::Request<kv_v2::RangeRequest>,
    ) -> Result<tonic::Response<kv_v2::RangeResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "range not needed in this test",
        ))
    }

    async fn delete(
        &self,
        _request: tonic::Request<kv_v2::DeleteRequest>,
    ) -> Result<tonic::Response<kv_v2::DeleteResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "delete not needed in this test",
        ))
    }
}

// ──── 装配 ────

struct FakeServer {
    addr: String,
    handle: tokio::task::JoinHandle<()>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

impl FakeServer {
    async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        let _ = self.handle.await;
    }
}

async fn start_txn_server(label: &'static str) -> (FakeServer, Arc<TxnCalls>) {
    start_txn_server_with(label, 0).await
}

async fn start_txn_server_with(
    label: &'static str,
    fail_first_commit: u64,
) -> (FakeServer, Arc<TxnCalls>) {
    let (txn, calls) = FakeTxn::new(label);
    txn.fail_first_commit
        .store(fail_first_commit, Ordering::SeqCst);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(TxnV2Server::new(txn))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    let _ = shutdown_rx.await;
                },
            )
            .await
            .unwrap();
    });
    (
        FakeServer {
            addr,
            handle,
            shutdown: Some(shutdown_tx),
        },
        calls,
    )
}

/// 同时提供 Txn 与 KV（A 用 KV Put 发 leader hint，B 用 KV Put 成功）。
async fn start_txn_kv_server(
    label: &'static str,
    hint: Option<String>,
) -> (FakeServer, Arc<TxnCalls>) {
    let (txn, calls) = FakeTxn::new(label);
    let kv = Arc::new(FakeKv::new(hint));
    let kv_service = Arc::clone(&kv);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(TxnV2Server::new(txn))
            .add_service(KvV2Server::from_arc(kv_service))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    let _ = shutdown_rx.await;
                },
            )
            .await
            .unwrap();
    });
    (
        FakeServer {
            addr,
            handle,
            shutdown: Some(shutdown_tx),
        },
        calls,
    )
}

async fn agent_inner_via(addr: &str) -> Arc<AgentInner> {
    let client = Client::connect_via_agent(addr)
        .await
        .expect("connect agent-style client");
    Arc::new(AgentInner {
        client,
        cache: AgentCache::new(0, 0, 0, 0),
    })
}

fn txn_not_found(status: &tonic::Status) {
    assert_eq!(status.code(), tonic::Code::Aborted, "{status:?}");
    assert_eq!(
        error_code_of(status).as_deref(),
        Some(CoordErrorCode::TxnNotFound.as_str()),
        "{status:?}"
    );
}

// ──── 用例 ────

#[tokio::test(flavor = "multi_thread")]
async fn proxy_forwards_txn_session_and_keeps_wire_codes() {
    let (server, calls) = start_txn_server("T").await;
    let inner = agent_inner_via(&server.addr).await;
    let proxy = TxnV2Proxy::new(Some(Arc::clone(&inner)));

    let begin = proxy
        .begin(tonic::Request::new(txn_v2::BeginRequest {
            start_ts: 0,
            ttl_ms: 0,
        }))
        .await
        .expect("Begin through proxy")
        .into_inner();
    assert_eq!(begin.start_ts, 42);
    assert_eq!(begin.txn_id.as_slice(), b"T-1");

    let read = proxy
        .read(tonic::Request::new(txn_v2::ReadRequest {
            txn_id: begin.txn_id.clone(),
            key: b"/k".to_vec(),
            ..Default::default()
        }))
        .await
        .expect("Read through proxy")
        .into_inner();
    assert_eq!(read.read_ts, 42);
    assert_eq!(read.kvs[0].value, b"from-T");
    assert_eq!(
        calls.reads.lock().as_slice(),
        &[(b"T-1".to_vec(), b"/k".to_vec())]
    );

    let commit = proxy
        .commit(tonic::Request::new(txn_v2::CommitRequest {
            txn_id: begin.txn_id.clone(),
            mutations: vec![txn_v2::Mutation {
                op: txn_v2::mutation::Op::Put as i32,
                key: b"/k".to_vec(),
                value: b"v".to_vec(),
                ..Default::default()
            }],
            request_id: b"req-1".to_vec(),
        }))
        .await
        .expect("Commit through proxy")
        .into_inner();
    assert_eq!(commit.commit_ts, 100);
    assert_eq!(calls.commits.lock().as_slice(), &[b"T-1".to_vec()]);

    // 会话已结束：代理本地就拒绝，不能再落到服务端。
    let err = proxy
        .read(tonic::Request::new(txn_v2::ReadRequest {
            txn_id: b"T-1".to_vec(),
            key: b"/k".to_vec(),
            ..Default::default()
        }))
        .await
        .expect_err("committed session must be gone");
    txn_not_found(&err);
    assert_eq!(
        calls.reads.lock().len(),
        1,
        "no second read may reach the server"
    );

    // 冲突是业务结果，wire 码必须原样到调用方。
    let begin = proxy
        .begin(tonic::Request::new(txn_v2::BeginRequest {
            start_ts: 0,
            ttl_ms: 0,
        }))
        .await
        .expect("second Begin")
        .into_inner();
    let err = proxy
        .commit(tonic::Request::new(txn_v2::CommitRequest {
            txn_id: begin.txn_id,
            mutations: vec![txn_v2::Mutation {
                op: txn_v2::mutation::Op::Put as i32,
                key: b"/conflict".to_vec(),
                value: b"v".to_vec(),
                ..Default::default()
            }],
            request_id: Vec::new(),
        }))
        .await
        .expect_err("conflict must surface as an error");
    assert_eq!(err.code(), tonic::Code::Aborted, "{err:?}");
    assert_eq!(
        error_code_of(&err).as_deref(),
        Some(CoordErrorCode::TxnConflict.as_str()),
        "{err:?}"
    );

    // Abort 对未知/已结束会话幂等成功（与服务端同一口径）。
    proxy
        .abort(tonic::Request::new(txn_v2::AbortRequest {
            txn_id: b"never-existed".to_vec(),
        }))
        .await
        .expect("unknown Abort must be idempotent");
    assert!(
        calls.aborts.lock().is_empty(),
        "unknown Abort must not reach the server"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn proxy_restores_the_session_when_commit_transport_fails() {
    // 先占掉 fail_first_commit 的“第一次”；本 server 的第一次 Commit 必定 UNAVAILABLE。
    let (server, calls) = start_txn_server_with("U", 1).await;
    let inner = agent_inner_via(&server.addr).await;
    let proxy = TxnV2Proxy::new(Some(Arc::clone(&inner)));

    let begin = proxy
        .begin(tonic::Request::new(txn_v2::BeginRequest {
            start_ts: 0,
            ttl_ms: 0,
        }))
        .await
        .expect("Begin through proxy")
        .into_inner();

    let commit = |txn_id: Vec<u8>| {
        let proxy = proxy.clone();
        async move {
            proxy
                .commit(tonic::Request::new(txn_v2::CommitRequest {
                    txn_id,
                    mutations: vec![txn_v2::Mutation {
                        op: txn_v2::mutation::Op::Put as i32,
                        key: b"/k".to_vec(),
                        value: b"v".to_vec(),
                        ..Default::default()
                    }],
                    request_id: b"retry-1".to_vec(),
                }))
                .await
        }
    };

    let err = commit(begin.txn_id.clone())
        .await
        .expect_err("first commit transport failure must surface");
    assert_eq!(err.code(), tonic::Code::Unavailable, "{err:?}");

    // 会话必须还在：同一 txn_id + request_id 的重试由服务端幂等回放。
    let retried = commit(begin.txn_id)
        .await
        .expect("the proxy must keep the session for an idempotent retry")
        .into_inner();
    assert_eq!(retried.commit_ts, 100);
    assert_eq!(calls.commits.lock().len(), 2);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn proxy_pins_the_txn_session_to_the_begin_coordinator() {
    // B 先监听，确保它的地址可写进 A 的 leader hint。
    let (server_b, calls_b) = start_txn_kv_server("B", None).await;
    let (server_a, calls_a) = start_txn_kv_server("A", Some(server_b.addr.clone())).await;

    // endpoints 顺序决定初始 leader=A（Status 不可用被当作候选）。
    let client = Client::connect_direct(Config::new(vec![
        server_a.addr.clone(),
        server_b.addr.clone(),
    ]))
    .await
    .expect("direct client");
    let inner = Arc::new(AgentInner {
        client,
        cache: AgentCache::new(0, 0, 0, 0),
    });

    let proxy = TxnV2Proxy::new(Some(Arc::clone(&inner)));
    let begin = proxy
        .begin(tonic::Request::new(txn_v2::BeginRequest {
            start_ts: 0,
            ttl_ms: 0,
        }))
        .await
        .expect("Begin through proxy")
        .into_inner();
    assert!(
        begin.txn_id.starts_with(b"A-"),
        "leader must be A: {begin:?}"
    );

    // 把客户端 leader 缓存真的切到 B：A 的 v2 Put 返回 UNAVAILABLE + hint。
    inner
        .client
        .kv_v2()
        .put(b"/switch", b"v", 1, false, b"")
        .await
        .expect("put retries onto the hinted leader");

    // 证明 leader 缓存真的切到了 B：新的 Begin 应落在 B。
    let on_b = proxy
        .begin(tonic::Request::new(txn_v2::BeginRequest {
            start_ts: 0,
            ttl_ms: 0,
        }))
        .await
        .expect("Begin after the leader switch")
        .into_inner();
    assert!(
        on_b.txn_id.starts_with(b"B-"),
        "leader cache must follow the hint: {on_b:?}"
    );

    // 之后 Txn Read 必须仍走 Begin 的 A（session 钉住 channel），而不是新 leader B。
    let read = proxy
        .read(tonic::Request::new(txn_v2::ReadRequest {
            txn_id: begin.txn_id,
            key: b"/k".to_vec(),
            ..Default::default()
        }))
        .await
        .expect("Read through proxy after a leader switch")
        .into_inner();
    assert_eq!(read.kvs[0].value, b"from-A");
    assert_eq!(calls_a.reads.lock().len(), 1);
    assert!(
        calls_b.reads.lock().is_empty(),
        "pinned session must not follow the new leader"
    );

    server_a.shutdown().await;
    server_b.shutdown().await;
}
