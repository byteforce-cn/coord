//! `DataPlane::V2` 的 v1 形状兼容层（#170）。
//!
//! 用进程内假 `coord.*.v2` 服务验证 wire 与语义：写走 `coord.txn.v2`（不直连
//! `coord.kv.v2.Put`）、CAS 冲突重跑、前缀 Watch 转半开区间、Lease 映射。
//! `DataPlane::V1` 的既有路径由 `integration_test` 覆盖，这里只钉 V2 分支。

use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use coord_client::{Client, Config, DataPlane};
use coord_core::error_code::{attach, CoordErrorCode};
use coord_proto::kv::v2 as kv_v2;
use coord_proto::kv::v2::kv_server::{Kv as KvV2, KvServer as KvV2Server};
use coord_proto::lease::v2 as lease_v2;
use coord_proto::lease::v2::lease_server::{Lease as LeaseV2, LeaseServer as LeaseV2Server};
use coord_proto::txn::v2 as txn_v2;
use coord_proto::txn::v2::mutation::Op as MutationOp;
use coord_proto::txn::v2::txn_server::{Txn as TxnV2, TxnServer as TxnV2Server};
use coord_proto::watch::v2 as watch_v2;
use coord_proto::watch::v2::watch_request;
use coord_proto::watch::v2::watch_server::{Watch as WatchV2, WatchServer as WatchV2Server};
use parking_lot::Mutex;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

// ──── 假 v2 集群 ────

#[derive(Default)]
struct FakeState {
    data: Mutex<BTreeMap<Vec<u8>, kv_v2::KeyValue>>,
    sessions: Mutex<HashMap<Vec<u8>, ()>>,
    next_ts: AtomicU64,
    commits: Mutex<Vec<Vec<txn_v2::Mutation>>>,
    begins: AtomicU64,
    /// 前 N 次 Commit 返回 `TXN_CONFLICT`（验证 CAS 重跑）。
    conflicts_left: AtomicU64,
    kv_puts: AtomicU64,
    kv_deletes: AtomicU64,
    range_read_ts: AtomicU64,
    watch_creates: Mutex<Vec<watch_v2::WatchCreateRequest>>,
    watch_history_unavailable: AtomicBool,
    lease_grants: AtomicU64,
    lease_keep_alives: AtomicU64,
    lease_revokes: AtomicU64,
}

impl FakeState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            next_ts: AtomicU64::new(100),
            ..Default::default()
        })
    }

    fn seed(&self, key: &[u8], value: &[u8], ts: u64) {
        self.data.lock().insert(
            key.to_vec(),
            kv_v2::KeyValue {
                key: key.to_vec(),
                value: value.to_vec(),
                create_revision: ts as i64,
                mod_revision: ts as i64,
                version: 1,
                lease_id: 0,
            },
        );
    }
}

struct FakeService {
    st: Arc<FakeState>,
}

#[tonic::async_trait]
impl KvV2 for FakeService {
    async fn put(
        &self,
        _request: tonic::Request<kv_v2::PutRequest>,
    ) -> Result<tonic::Response<kv_v2::PutResponse>, tonic::Status> {
        self.st.kv_puts.fetch_add(1, Ordering::SeqCst);
        Err(tonic::Status::internal(
            "direct kv.v2 Put must not be used by the v1-shape adapter",
        ))
    }

    async fn range(
        &self,
        request: tonic::Request<kv_v2::RangeRequest>,
    ) -> Result<tonic::Response<kv_v2::RangeResponse>, tonic::Status> {
        let req = request.into_inner();
        self.st
            .range_read_ts
            .fetch_max(req.read_ts.max(0) as u64, Ordering::SeqCst);
        let data = self.st.data.lock();
        let point = req.range_end.is_empty() || req.range_end == req.key;
        let mut kvs: Vec<kv_v2::KeyValue> = data
            .values()
            .filter(|kv| {
                if point {
                    kv.key == req.key
                } else {
                    kv.key.as_slice() >= req.key.as_slice()
                        && kv.key.as_slice() < req.range_end.as_slice()
                }
            })
            .cloned()
            .collect();
        kvs.sort_by(|a, b| a.key.cmp(&b.key));
        let count = kvs.len() as i64;
        if req.count_only {
            kvs.clear();
        } else if req.limit > 0 {
            kvs.truncate(req.limit as usize);
        }
        if req.keys_only {
            for kv in &mut kvs {
                kv.value.clear();
            }
        }
        let read_ts = req
            .read_ts
            .max(data.values().map(|kv| kv.mod_revision).max().unwrap_or(0));
        Ok(tonic::Response::new(kv_v2::RangeResponse {
            kvs,
            count,
            read_ts,
        }))
    }

    async fn delete(
        &self,
        _request: tonic::Request<kv_v2::DeleteRequest>,
    ) -> Result<tonic::Response<kv_v2::DeleteResponse>, tonic::Status> {
        self.st.kv_deletes.fetch_add(1, Ordering::SeqCst);
        Err(tonic::Status::internal(
            "direct kv.v2 Delete must not be used by the v1-shape adapter",
        ))
    }
}

#[tonic::async_trait]
impl TxnV2 for FakeService {
    async fn begin(
        &self,
        request: tonic::Request<txn_v2::BeginRequest>,
    ) -> Result<tonic::Response<txn_v2::BeginResponse>, tonic::Status> {
        let req = request.into_inner();
        let seq = self.st.begins.fetch_add(1, Ordering::SeqCst) + 1;
        let id = format!("txn-{seq}").into_bytes();
        self.st.sessions.lock().insert(id.clone(), ());
        Ok(tonic::Response::new(txn_v2::BeginResponse {
            txn_id: id,
            start_ts: if req.start_ts == 0 { 100 } else { req.start_ts },
        }))
    }

    async fn read(
        &self,
        request: tonic::Request<txn_v2::ReadRequest>,
    ) -> Result<tonic::Response<txn_v2::ReadResponse>, tonic::Status> {
        let req = request.into_inner();
        if !self.st.sessions.lock().contains_key(&req.txn_id) {
            return Err(attach(
                tonic::Status::aborted("unknown txn"),
                CoordErrorCode::TxnNotFound,
            ));
        }
        let data = self.st.data.lock();
        let point = req.range_end.is_empty() || req.range_end == req.key;
        let mut kvs: Vec<kv_v2::KeyValue> = data
            .values()
            .filter(|kv| {
                if point {
                    kv.key == req.key
                } else {
                    kv.key.as_slice() >= req.key.as_slice()
                        && kv.key.as_slice() < req.range_end.as_slice()
                }
            })
            .cloned()
            .collect();
        kvs.sort_by(|a, b| a.key.cmp(&b.key));
        if req.limit > 0 {
            kvs.truncate(req.limit as usize);
        }
        if req.keys_only {
            for kv in &mut kvs {
                kv.value.clear();
            }
        }
        Ok(tonic::Response::new(txn_v2::ReadResponse {
            kvs,
            read_ts: 100,
        }))
    }

    async fn commit(
        &self,
        request: tonic::Request<txn_v2::CommitRequest>,
    ) -> Result<tonic::Response<txn_v2::CommitResponse>, tonic::Status> {
        let req = request.into_inner();
        if self.st.sessions.lock().remove(&req.txn_id).is_none() {
            return Err(attach(
                tonic::Status::aborted("unknown txn"),
                CoordErrorCode::TxnNotFound,
            ));
        }
        if self.st.conflicts_left.load(Ordering::SeqCst) > 0
            && self.st.conflicts_left.fetch_sub(1, Ordering::SeqCst) > 0
        {
            return Err(attach(
                tonic::Status::aborted("conflict"),
                CoordErrorCode::TxnConflict,
            ));
        }
        let commit_ts = self.st.next_ts.fetch_add(1, Ordering::SeqCst);
        self.st.commits.lock().push(req.mutations.clone());
        let mut data = self.st.data.lock();
        for m in req.mutations {
            match MutationOp::try_from(m.op).ok() {
                Some(MutationOp::Put) => {
                    let version = data.get(&m.key).map(|kv| kv.version + 1).unwrap_or(1);
                    let create = data
                        .get(&m.key)
                        .map(|kv| kv.create_revision)
                        .unwrap_or(commit_ts as i64);
                    data.insert(
                        m.key.clone(),
                        kv_v2::KeyValue {
                            key: m.key,
                            value: m.value,
                            create_revision: create,
                            mod_revision: commit_ts as i64,
                            version,
                            lease_id: m.lease_id,
                        },
                    );
                }
                Some(MutationOp::Delete) => {
                    data.remove(&m.key);
                }
                Some(MutationOp::DeleteRange) => {
                    data.retain(|k, _| {
                        !(k.as_slice() >= m.key.as_slice() && k.as_slice() < m.range_end.as_slice())
                    });
                }
                None | Some(MutationOp::Unspecified) => {}
            }
        }
        Ok(tonic::Response::new(txn_v2::CommitResponse {
            commit_ts: commit_ts as i64,
        }))
    }

    async fn abort(
        &self,
        request: tonic::Request<txn_v2::AbortRequest>,
    ) -> Result<tonic::Response<txn_v2::AbortResponse>, tonic::Status> {
        self.st.sessions.lock().remove(&request.into_inner().txn_id);
        Ok(tonic::Response::new(txn_v2::AbortResponse {}))
    }
}

#[tonic::async_trait]
impl LeaseV2 for FakeService {
    async fn lease_grant(
        &self,
        request: tonic::Request<lease_v2::LeaseGrantRequest>,
    ) -> Result<tonic::Response<lease_v2::LeaseGrantResponse>, tonic::Status> {
        let req = request.into_inner();
        self.st.lease_grants.fetch_add(1, Ordering::SeqCst);
        let id = if req.lease_id == 0 { 42 } else { req.lease_id };
        Ok(tonic::Response::new(lease_v2::LeaseGrantResponse {
            lease_id: id,
            ttl_seconds: req.ttl_seconds,
            granted_ts: 100,
        }))
    }

    async fn lease_revoke(
        &self,
        _request: tonic::Request<lease_v2::LeaseRevokeRequest>,
    ) -> Result<tonic::Response<lease_v2::LeaseRevokeResponse>, tonic::Status> {
        self.st.lease_revokes.fetch_add(1, Ordering::SeqCst);
        Ok(tonic::Response::new(lease_v2::LeaseRevokeResponse {
            commit_ts: 0,
        }))
    }

    type LeaseKeepAliveStream = Pin<
        Box<
            dyn tokio_stream::Stream<Item = Result<lease_v2::LeaseKeepAliveResponse, tonic::Status>>
                + Send,
        >,
    >;

    async fn lease_keep_alive(
        &self,
        request: tonic::Request<tonic::Streaming<lease_v2::LeaseKeepAliveRequest>>,
    ) -> Result<tonic::Response<Self::LeaseKeepAliveStream>, tonic::Status> {
        let mut inbound = request.into_inner();
        let st = Arc::clone(&self.st);
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            while let Ok(Some(req)) = inbound.message().await {
                st.lease_keep_alives.fetch_add(1, Ordering::SeqCst);
                if tx
                    .send(Ok(lease_v2::LeaseKeepAliveResponse {
                        lease_id: req.lease_id,
                        ttl_seconds: 10,
                    }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        Ok(tonic::Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn lease_time_to_live(
        &self,
        request: tonic::Request<lease_v2::LeaseTimeToLiveRequest>,
    ) -> Result<tonic::Response<lease_v2::LeaseTimeToLiveResponse>, tonic::Status> {
        Ok(tonic::Response::new(lease_v2::LeaseTimeToLiveResponse {
            lease_id: request.into_inner().lease_id,
            ttl_seconds: 10,
            granted_ttl_seconds: 10,
            keys: vec![b"/app/leased".to_vec()],
        }))
    }
}

#[tonic::async_trait]
impl WatchV2 for FakeService {
    type WatchStream = Pin<
        Box<dyn tokio_stream::Stream<Item = Result<watch_v2::WatchResponse, tonic::Status>> + Send>,
    >;

    async fn watch(
        &self,
        request: tonic::Request<tonic::Streaming<watch_v2::WatchRequest>>,
    ) -> Result<tonic::Response<Self::WatchStream>, tonic::Status> {
        let mut inbound = request.into_inner();
        let first = inbound
            .message()
            .await?
            .ok_or_else(|| tonic::Status::invalid_argument("missing watch create"))?;
        let Some(watch_request::Request::Create(create)) = first.request else {
            return Err(tonic::Status::invalid_argument("expected watch create"));
        };
        self.st.watch_creates.lock().push(create);

        let (tx, rx) = mpsc::channel(4);
        if self.st.watch_history_unavailable.load(Ordering::SeqCst) {
            let _ = tx
                .send(Err(attach(
                    tonic::Status::out_of_range("history reclaimed"),
                    CoordErrorCode::HistoryUnavailable,
                )))
                .await;
            return Ok(tonic::Response::new(Box::pin(ReceiverStream::new(rx))));
        }

        let _ = tx
            .send(Ok(watch_v2::WatchResponse {
                watch_id: 1,
                events: vec![watch_v2::WatchEvent {
                    commit_ts: 7,
                    changes: vec![
                        watch_v2::Change {
                            r#type: watch_v2::change::Type::Put as i32,
                            key: b"/_registry/services/a".to_vec(),
                            value: b"one".to_vec(),
                            prev_kv: None,
                        },
                        watch_v2::Change {
                            r#type: watch_v2::change::Type::Delete as i32,
                            key: b"/_registry/services/b".to_vec(),
                            value: Vec::new(),
                            prev_kv: None,
                        },
                    ],
                }],
                cursor: Some(watch_v2::WatchCursor {
                    shard_id: 1,
                    commit_ts: 7,
                }),
                watermark: 7,
                sealed: false,
            }))
            .await;
        tokio::spawn(async move {
            // 保持流存活，直到客户端取消/丢弃订阅。
            tokio::time::sleep(Duration::from_millis(500)).await;
            drop(tx);
        });
        Ok(tonic::Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

struct TestCluster {
    addr: String,
    st: Arc<FakeState>,
    shutdown: tokio::sync::oneshot::Sender<()>,
    handle: tokio::task::JoinHandle<()>,
}

async fn start_cluster(history_unavailable: bool) -> TestCluster {
    let st = FakeState::new();
    st.watch_history_unavailable
        .store(history_unavailable, Ordering::SeqCst);
    let svc = Arc::new(FakeService {
        st: Arc::clone(&st),
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(KvV2Server::from_arc(Arc::clone(&svc)))
            .add_service(TxnV2Server::from_arc(Arc::clone(&svc)))
            .add_service(LeaseV2Server::from_arc(Arc::clone(&svc)))
            .add_service(WatchV2Server::from_arc(svc))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    let _ = shutdown_rx.await;
                },
            )
            .await
            .unwrap();
    });
    TestCluster {
        addr,
        st,
        shutdown,
        handle,
    }
}

impl TestCluster {
    async fn stop(self) {
        let _ = self.shutdown.send(());
        let _ = self.handle.await;
    }
}

async fn v2_client(cluster: &TestCluster) -> Client {
    let config = Config::new(vec![cluster.addr.clone()]).with_data_plane(DataPlane::V2);
    Client::connect_via_agent_with(config, cluster.addr.clone())
        .await
        .expect("connect fake v2 cluster")
}

// ──── 用例 ────

#[tokio::test(flavor = "multi_thread")]
async fn v1_put_uses_v2_txn_not_direct_kv() {
    let cluster = start_cluster(false).await;
    let client = v2_client(&cluster).await;

    let rev = client.kv().put(b"/_lock/a", b"one").await.unwrap();
    assert_eq!(rev, 100, "v1 revision = v2 commit_ts");
    assert_eq!(
        cluster.st.kv_puts.load(Ordering::SeqCst),
        0,
        "adapter must not use coord.kv.v2.Put"
    );
    assert_eq!(cluster.st.commits.lock().len(), 1);
    assert_eq!(
        cluster
            .st
            .data
            .lock()
            .get(b"/_lock/a".as_slice())
            .unwrap()
            .value,
        b"one"
    );

    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn v1_range_maps_v2_metadata_and_delete_counts_from_session_read() {
    let cluster = start_cluster(false).await;
    cluster.st.seed(b"/app/a", b"1", 90);
    cluster.st.seed(b"/app/b", b"2", 91);
    let client = v2_client(&cluster).await;

    let (rows, count, _rev) = client
        .kv()
        .range_with_lease_full(b"/app/a", b"/app/c", 0, 0, false, false)
        .await
        .unwrap();
    assert_eq!(count, 2);
    assert_eq!(rows[0], (b"/app/a".to_vec(), b"1".to_vec(), 0, 1));

    let (deleted, commit_ts) = client
        .kv()
        .delete_full(b"/app/a", b"/app/c", false, b"")
        .await
        .unwrap();
    assert_eq!(deleted, 2);
    assert!(commit_ts > 0);
    assert!(cluster.st.data.lock().is_empty());

    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn v1_txn_compare_then_put_uses_v2_session() {
    let cluster = start_cluster(false).await;
    let client = v2_client(&cluster).await;

    let compare = version_equals(0);
    let success = put_op(b"/app/a", b"one", 0, false);
    let resp = client
        .txn()
        .txn(vec![compare], vec![success], vec![])
        .await
        .unwrap();
    assert!(resp.succeeded);
    assert_eq!(resp.revision, 100);
    assert_eq!(
        cluster
            .st
            .data
            .lock()
            .get(b"/app/a".as_slice())
            .unwrap()
            .value,
        b"one"
    );

    // 第二次 version==0 不成立 → failure 写集生效（v1 语义）。
    let failure = put_op(b"/app/b", b"two", 0, false);
    let resp = client
        .txn()
        .txn(vec![version_equals(0)], vec![], vec![failure])
        .await
        .unwrap();
    assert!(!resp.succeeded);
    assert_eq!(
        cluster
            .st
            .data
            .lock()
            .get(b"/app/b".as_slice())
            .unwrap()
            .value,
        b"two"
    );

    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn txn_retries_on_conflict_with_a_fresh_session() {
    let cluster = start_cluster(false).await;
    cluster.st.conflicts_left.store(1, Ordering::SeqCst);
    let client = v2_client(&cluster).await;

    let resp = client
        .txn()
        .txn(
            vec![version_equals(0)],
            vec![put_op(b"/app/a", b"one", 0, false)],
            vec![],
        )
        .await
        .unwrap();
    assert!(resp.succeeded);
    assert_eq!(cluster.st.begins.load(Ordering::SeqCst), 2);
    assert_eq!(
        cluster
            .st
            .data
            .lock()
            .get(b"/app/a".as_slice())
            .unwrap()
            .value,
        b"one"
    );
    assert!(
        cluster.st.sessions.lock().is_empty(),
        "conflicted session must be aborted, not left for TTL"
    );

    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn txn_range_op_sees_prior_put_in_the_same_txn() {
    let cluster = start_cluster(false).await;
    let client = v2_client(&cluster).await;

    let range_op = coord_proto::txn::RequestOp {
        op: Some(coord_proto::txn::request_op::Op::RequestRange(
            coord_proto::kv::RangeRequest {
                key: b"/app/a".to_vec(),
                range_end: Vec::new(),
                limit: 0,
                revision: 0,
                keys_only: false,
                count_only: false,
            },
        )),
    };
    let resp = client
        .txn()
        .txn(
            vec![],
            vec![put_op(b"/app/a", b"one", 0, false), range_op],
            vec![],
        )
        .await
        .unwrap();
    let Some(coord_proto::txn::response_op::Op::ResponseRange(range)) =
        resp.responses.get(1).and_then(|op| op.op.clone())
    else {
        panic!("expected range response");
    };
    assert_eq!(range.kvs[0].value, b"one");

    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn txn_range_after_delete_range_hides_snapshot_rows() {
    let cluster = start_cluster(false).await;
    cluster.st.seed(b"/app/a", b"1", 90);
    cluster.st.seed(b"/app/b", b"2", 91);
    cluster.st.seed(b"/app/c", b"3", 92);
    let client = v2_client(&cluster).await;

    let delete_range = coord_proto::txn::RequestOp {
        op: Some(coord_proto::txn::request_op::Op::RequestDelete(
            coord_proto::kv::DeleteRequest {
                key: b"/app/a".to_vec(),
                range_end: b"/app/c".to_vec(),
                prev_kv: false,
                request_id: Vec::new(),
            },
        )),
    };
    let range = coord_proto::txn::RequestOp {
        op: Some(coord_proto::txn::request_op::Op::RequestRange(
            coord_proto::kv::RangeRequest {
                key: b"/app/a".to_vec(),
                range_end: b"/app/d".to_vec(),
                limit: 0,
                revision: 0,
                keys_only: false,
                count_only: false,
            },
        )),
    };
    let resp = client
        .txn()
        .txn(vec![], vec![delete_range, range], vec![])
        .await
        .unwrap();
    let Some(coord_proto::txn::response_op::Op::ResponseRange(rows)) =
        resp.responses.get(1).and_then(|op| op.op.clone())
    else {
        panic!("expected range response");
    };
    assert_eq!(rows.kvs.len(), 1);
    assert_eq!(rows.kvs[0].key, b"/app/c");

    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn watch_empty_range_end_uses_prefix_successor_and_batches_events() {
    let cluster = start_cluster(false).await;
    let client = v2_client(&cluster).await;

    let mut rx = client
        .watch()
        .watch(b"/_registry/services/", 0)
        .await
        .unwrap();
    let event = rx.recv().await.unwrap().unwrap();
    assert_eq!(event.revision, 7);
    assert_eq!(
        event.r#type,
        coord_proto::watch::watch_event::EventType::Put as i32
    );
    assert_eq!(event.kvs.len(), 2);
    assert_eq!(event.kvs[0].key, b"/_registry/services/a");
    assert_eq!(event.kvs[0].mod_revision, 7);
    drop(rx);

    {
        let creates = cluster.st.watch_creates.lock();
        assert_eq!(creates[0].range_end, b"/_registry/services0");
        assert_eq!(creates[0].start_revision, 0);
    }

    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn watch_history_unavailable_is_a_terminal_v1_event() {
    let cluster = start_cluster(true).await;
    let client = v2_client(&cluster).await;

    let mut rx = client
        .watch()
        .watch(b"/_registry/services/", 0)
        .await
        .unwrap();
    let event = rx.recv().await.unwrap().unwrap();
    assert_eq!(
        event.r#type,
        coord_proto::watch::watch_event::EventType::HistoryUnavailable as i32
    );

    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn lease_calls_route_to_v2_and_background_keep_alive_renews() {
    let cluster = start_cluster(false).await;
    let client = v2_client(&cluster).await;

    let id = client.lease().grant(10).await.unwrap();
    assert_eq!(id, 42);
    assert_eq!(client.lease().keep_alive(id).await.unwrap(), 10);
    client.lease().revoke(id).await.unwrap();

    let keeper = client.lease().keep_alive_background(id).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    keeper.release().await.unwrap();
    assert!(
        cluster.st.lease_keep_alives.load(Ordering::SeqCst) >= 2,
        "background keeper must renew at least once"
    );

    cluster.stop().await;
}

fn version_equals(version: i64) -> coord_proto::txn::Compare {
    coord_proto::txn::Compare {
        result: coord_proto::txn::compare::CompareResult::Equal as i32,
        target: coord_proto::txn::compare::Target::Version as i32,
        key: b"/app/a".to_vec(),
        target_value: Some(coord_proto::txn::compare::TargetValue::Version(version)),
    }
}

fn put_op(key: &[u8], value: &[u8], lease_id: i64, prev_kv: bool) -> coord_proto::txn::RequestOp {
    coord_proto::txn::RequestOp {
        op: Some(coord_proto::txn::request_op::Op::RequestPut(
            coord_proto::kv::PutRequest {
                key: key.to_vec(),
                value: value.to_vec(),
                lease_id,
                prev_kv,
                request_id: Vec::new(),
            },
        )),
    }
}
