//! KV-T G6（#172）Rust client 学习 `coord-routing-epoch`：
//!
//! - 首次请求不带 metadata（0 = 未知，服务端不校验）；
//! - 收到 `EPOCH_STALE` 的 trailing metadata 后记住版本，重试带新版本；
//! - 学到后后续请求直接携带，不需要再被拒一次；
//! - Txn 会话 RPC 只在 `EPOCH_STALE` 上重试，`TXN_CONFLICT` 等业务结果不受影响。

use std::str::FromStr;
use std::sync::{Arc, Mutex};

use coord_client::{Client, TxnCommit, WatchV2Cursor, WatchV2Options};
use coord_core::error_code::{attach, CoordErrorCode};
use coord_proto::kv::v2 as kv;
use coord_proto::txn::v2 as txn;
use coord_proto::watch::v2 as watch;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::metadata::MetadataValue;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

const EPOCH: &str = "coord-routing-epoch";

#[derive(Default)]
struct Calls {
    kv_put: Vec<Option<u64>>,
    kv_range: Vec<Option<u64>>,
    txn_begin: Vec<Option<u64>>,
    txn_read: Vec<Option<u64>>,
    txn_commit: Vec<Option<u64>>,
    txn_abort: Vec<Option<u64>>,
    kv_put_stale: usize,
    kv_range_stale: usize,
    txn_begin_stale: usize,
    txn_read_stale: usize,
    txn_commit_stale: usize,
    txn_commit_conflict: bool,
    watch_open: Vec<Option<u64>>,
    watch_stale: usize,
}

fn epoch_of<T>(request: &Request<T>) -> Option<u64> {
    request
        .metadata()
        .get(EPOCH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
}

fn stale_epoch(server: &str) -> Status {
    let mut status = attach(
        Status::aborted("stale routing epoch"),
        CoordErrorCode::EpochStale,
    );
    status.metadata_mut().insert(
        EPOCH,
        MetadataValue::from_str(server).expect("ascii metadata"),
    );
    status
}

struct Mock {
    calls: Arc<Mutex<Calls>>,
}

#[tonic::async_trait]
impl kv::kv_server::Kv for Mock {
    async fn put(
        &self,
        request: Request<kv::PutRequest>,
    ) -> Result<Response<kv::PutResponse>, Status> {
        let epoch = epoch_of(&request);
        let mut calls = self.calls.lock().expect("calls");
        calls.kv_put.push(epoch);
        if calls.kv_put_stale > 0 {
            calls.kv_put_stale -= 1;
            drop(calls);
            return Err(stale_epoch("2"));
        }
        drop(calls);
        let request = request.into_inner();
        Ok(Response::new(kv::PutResponse {
            prev_kv: None,
            commit_ts: request.commit_ts,
        }))
    }

    async fn range(
        &self,
        request: Request<kv::RangeRequest>,
    ) -> Result<Response<kv::RangeResponse>, Status> {
        let epoch = epoch_of(&request);
        let mut calls = self.calls.lock().expect("calls");
        calls.kv_range.push(epoch);
        if calls.kv_range_stale > 0 {
            calls.kv_range_stale -= 1;
            drop(calls);
            return Err(stale_epoch("2"));
        }
        drop(calls);
        let request = request.into_inner();
        Ok(Response::new(kv::RangeResponse {
            kvs: Vec::new(),
            count: 0,
            read_ts: request.read_ts,
        }))
    }

    async fn delete(
        &self,
        request: Request<kv::DeleteRequest>,
    ) -> Result<Response<kv::DeleteResponse>, Status> {
        let request = request.into_inner();
        Ok(Response::new(kv::DeleteResponse {
            deleted: 1,
            prev_kvs: Vec::new(),
            commit_ts: request.commit_ts,
        }))
    }
}

#[tonic::async_trait]
impl txn::txn_server::Txn for Mock {
    async fn begin(
        &self,
        request: Request<txn::BeginRequest>,
    ) -> Result<Response<txn::BeginResponse>, Status> {
        let epoch = epoch_of(&request);
        let mut calls = self.calls.lock().expect("calls");
        calls.txn_begin.push(epoch);
        if calls.txn_begin_stale > 0 {
            calls.txn_begin_stale -= 1;
            drop(calls);
            return Err(stale_epoch("2"));
        }
        drop(calls);
        Ok(Response::new(txn::BeginResponse {
            txn_id: b"txn".to_vec(),
            start_ts: 1,
        }))
    }

    async fn read(
        &self,
        request: Request<txn::ReadRequest>,
    ) -> Result<Response<txn::ReadResponse>, Status> {
        let epoch = epoch_of(&request);
        let mut calls = self.calls.lock().expect("calls");
        calls.txn_read.push(epoch);
        if calls.txn_read_stale > 0 {
            calls.txn_read_stale -= 1;
            drop(calls);
            // 比 Begin 学到的版本更新：客户端取较大值，不被服务端降级。
            return Err(stale_epoch("3"));
        }
        drop(calls);
        Ok(Response::new(txn::ReadResponse {
            kvs: Vec::new(),
            read_ts: 1,
        }))
    }

    async fn commit(
        &self,
        request: Request<txn::CommitRequest>,
    ) -> Result<Response<txn::CommitResponse>, Status> {
        let epoch = epoch_of(&request);
        let mut calls = self.calls.lock().expect("calls");
        calls.txn_commit.push(epoch);
        if calls.txn_commit_stale > 0 {
            calls.txn_commit_stale -= 1;
            drop(calls);
            // 比客户端已学到的版本更旧：落后节点不能把它降级。
            return Err(stale_epoch("2"));
        }
        if calls.txn_commit_conflict {
            drop(calls);
            return Err(attach(
                Status::aborted("write conflict"),
                CoordErrorCode::TxnConflict,
            ));
        }
        drop(calls);
        Ok(Response::new(txn::CommitResponse { commit_ts: 9 }))
    }

    async fn abort(
        &self,
        request: Request<txn::AbortRequest>,
    ) -> Result<Response<txn::AbortResponse>, Status> {
        self.calls
            .lock()
            .expect("calls")
            .txn_abort
            .push(epoch_of(&request));
        Ok(Response::new(txn::AbortResponse {}))
    }
}

async fn start_server(calls: Arc<Mutex<Calls>>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let kv = kv::kv_server::KvServer::new(Mock {
        calls: Arc::clone(&calls),
    });
    let txn = txn::txn_server::TxnServer::new(Mock {
        calls: Arc::clone(&calls),
    });
    let watch = watch::watch_server::WatchServer::new(Mock { calls });
    tokio::spawn(async move {
        Server::builder()
            .add_service(kv)
            .add_service(txn)
            .add_service(watch)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("serve");
    });
    addr.to_string()
}

#[tokio::test]
async fn kv_v2_learns_the_epoch_and_retries_a_stale_write() {
    let calls = Arc::new(Mutex::new(Calls {
        kv_put_stale: 1,
        ..Default::default()
    }));
    let addr = start_server(Arc::clone(&calls)).await;
    let client = Client::connect_via_agent(addr).await.expect("client");
    let kv = client.kv_v2();

    let (_, commit_ts) = kv.put(b"k", b"v", 7, false, b"req-1").await.expect("put");
    assert_eq!(commit_ts, 7);
    assert_eq!(
        calls.lock().expect("calls").kv_put,
        vec![None, Some(2)],
        "first attempt carries no epoch; the retry carries the learned one"
    );

    let (_, commit_ts) = kv.put(b"k", b"v2", 8, false, b"req-2").await.expect("put");
    assert_eq!(commit_ts, 8);
    assert_eq!(
        calls.lock().expect("calls").kv_put,
        vec![None, Some(2), Some(2)],
        "a learned epoch is attached without waiting for another rejection"
    );
}

#[tokio::test]
async fn kv_v2_range_retries_a_stale_epoch_read() {
    let calls = Arc::new(Mutex::new(Calls {
        kv_range_stale: 1,
        ..Default::default()
    }));
    let addr = start_server(Arc::clone(&calls)).await;
    let client = Client::connect_via_agent(addr).await.expect("client");

    let (kvs, count, read_ts) = client
        .kv_v2()
        .range(b"k", b"", 0, 0, false, false)
        .await
        .expect("range");
    assert!(kvs.is_empty());
    assert_eq!(count, 0);
    assert_eq!(read_ts, 0);
    assert_eq!(
        calls.lock().expect("calls").kv_range,
        vec![None, Some(2)],
        "range refreshes and retries instead of surfacing EPOCH_STALE"
    );
}

#[tokio::test]
async fn txn_v2_retries_stale_session_rpcs_and_keeps_the_newer_epoch() {
    let calls = Arc::new(Mutex::new(Calls {
        txn_begin_stale: 1,
        txn_read_stale: 1,
        txn_commit_stale: 1,
        ..Default::default()
    }));
    let addr = start_server(Arc::clone(&calls)).await;
    let client = Client::connect_via_agent(addr).await.expect("client");

    let session = client.txn_v2().begin(0, 1_000).await.expect("begin");
    assert_eq!(session.start_ts(), 1);
    assert_eq!(
        calls.lock().expect("calls").txn_begin,
        vec![None, Some(2)],
        "begin retries with the version returned by the guard"
    );

    let (kvs, read_ts) = session.read(b"k", b"", 0, false).await.expect("read");
    assert!(kvs.is_empty());
    assert_eq!(read_ts, 1);
    assert_eq!(
        calls.lock().expect("calls").txn_read,
        vec![Some(2), Some(3)],
        "a larger server version wins; the client never downgrades"
    );

    let commit = session.commit(Vec::new(), b"req").await.expect("commit");
    assert!(matches!(commit, TxnCommit::Committed { commit_ts: 9 }));

    let session = client.txn_v2().begin(0, 1_000).await.expect("begin");
    session.abort().await.expect("abort");
    let calls = calls.lock().expect("calls");
    assert_eq!(
        calls.txn_commit,
        vec![Some(3), Some(3)],
        "a lagging node's smaller version must not downgrade the client"
    );
    assert_eq!(calls.txn_begin.last(), Some(&Some(3)));
    assert_eq!(calls.txn_abort, vec![Some(3)]);
}

#[tokio::test]
async fn txn_commit_conflict_is_not_retried_as_epoch_stale() {
    let calls = Arc::new(Mutex::new(Calls {
        txn_commit_conflict: true,
        ..Default::default()
    }));
    let addr = start_server(Arc::clone(&calls)).await;
    let client = Client::connect_via_agent(addr).await.expect("client");
    let session = client.txn_v2().begin(0, 1_000).await.expect("begin");

    let commit = session.commit(Vec::new(), b"req").await.expect("commit");
    assert_eq!(commit, TxnCommit::Conflict);
    assert_eq!(
        calls.lock().expect("calls").txn_commit.len(),
        1,
        "TXN_CONFLICT is a business result, not an epoch retry"
    );
}

#[tonic::async_trait]
impl watch::watch_server::Watch for Mock {
    type WatchStream = ReceiverStream<Result<watch::WatchResponse, Status>>;

    async fn watch(
        &self,
        request: Request<Streaming<watch::WatchRequest>>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let epoch = epoch_of(&request);
        {
            let mut calls = self.calls.lock().expect("calls");
            calls.watch_open.push(epoch);
            if calls.watch_stale > 0 {
                calls.watch_stale -= 1;
                return Err(stale_epoch("3"));
            }
        }
        let mut stream = request.into_inner();
        let create = match stream.message().await? {
            Some(watch::WatchRequest {
                request: Some(watch::watch_request::Request::Create(create)),
            }) => create,
            _ => return Err(Status::invalid_argument("missing create frame")),
        };
        if create.key != b"k" {
            return Err(Status::invalid_argument("unexpected watch key"));
        }
        let (tx, rx) = mpsc::channel(2);
        let _ = tx.try_send(Ok(watch::WatchResponse {
            watch_id: 1,
            events: vec![watch::WatchEvent {
                commit_ts: 5,
                changes: vec![watch::Change {
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                    prev_kv: None,
                    r#type: watch::change::Type::Put as i32,
                }],
            }],
            cursor: Some(watch::WatchCursor {
                shard_id: 1,
                commit_ts: 5,
            }),
            watermark: 5,
            sealed: false,
        }));
        // 保持流开启：订阅由 cancel() 结束，不靠 EOF（否则客户端会立刻重连）。
        tokio::spawn(async move {
            let _held = tx;
            std::future::pending::<()>().await;
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[tokio::test]
async fn watch_v2_attaches_the_learned_epoch_and_retries_a_stale_open() {
    let calls = Arc::new(Mutex::new(Calls {
        watch_stale: 1,
        ..Default::default()
    }));
    let addr = start_server(Arc::clone(&calls)).await;
    let client = Client::connect_via_agent(addr).await.expect("client");

    let mut subscription = client
        .watch_v2()
        .watch(WatchV2Options::new(b"k"))
        .await
        .expect("watch");
    let batch = subscription
        .recv()
        .await
        .expect("a batch reaches the subscriber")
        .expect("the retried open succeeds");
    assert_eq!(batch.events.len(), 1);
    assert_eq!(batch.events[0].commit_ts, 5);
    assert_eq!(
        subscription.cursor(),
        Some(WatchV2Cursor {
            shard_id: 1,
            commit_ts: 5
        })
    );
    assert_eq!(
        calls.lock().expect("calls").watch_open,
        vec![None, Some(3)],
        "first open has no epoch; the reopen after EPOCH_STALE carries the learned one"
    );
    subscription.cancel().await;
}
