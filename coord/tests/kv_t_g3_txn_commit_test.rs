//! KV-T G3（#151）`coord.txn.v2.Txn.Commit` 验收：
//!
//! - 提交的写集在决策落盘后返回 `commit_ts`，apply 后按同一 `read_ts` 可见；
//! - 同一 `txn_id` + `request_id` 重放返回首次 `commit_ts`，不重复决策；
//! - 冲突返回 `ABORTED` + `TXN_CONFLICT` 且不写决策；换新 `start_ts` 重跑成功；
//! - 写集超限 `TXN_TOO_LARGE`、非法 mutation `INVALID_ARGUMENT`，失败不卡住会话。

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use coord_core::error::Result as CoordResult;
use coord_core::error_code::error_code_of;
use coord_core::storage::StorageBackend;
use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionMeta, StorageConfig};
use coord_proto::kv::v2::kv_server::Kv as KvV2;
use coord_proto::kv::v2::RangeRequest as RangeRequestV2;
use coord_proto::txn::v2::mutation::Op;
use coord_proto::txn::v2::txn_server::Txn as TxnV2;
use coord_proto::txn::v2::{AbortRequest, BeginRequest, CommitRequest, Mutation, ReadRequest};
use coord_server::kvt::decision_apply::apply_pending_decisions;
use coord_server::kvt::sequencer::CommitSequencer;
use coord_server::kvt::{KvTCapability, KvTRuntimeConfig, KvTState};
use coord_server::metrics::Metrics;
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcService};
use coord_server::raft::region::RegionManager;
use coord_server::raft::region_runtime::{region_data_dir, RegionRuntimeSpec};
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::tso::{TsoBatch, TsoClient, TsoSource};
use coord_server::raft::type_config::{Command, Response, SequencerValidationLimits};
use coord_server::raft::{new_basic_node, new_raft, RaftConfig, WatchReceiver};
use coord_server::server::CoordNode;
use coord_server::storage::mvcc::{MvccStorage, RevisionMode};
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::snapshot::SnapshotTracker;
use tempfile::TempDir;
use tonic::Code;

/// seed 写入的 `commit_ts`；TSO 高水位紧随其后，保证首个提交时间戳更大。
const SEED_TS: u64 = 1_000;

fn find_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn raft_config() -> RaftConfig {
    RaftConfig {
        heartbeat_interval: 200,
        election_timeout_min: 800,
        election_timeout_max: 1_500,
        ..Default::default()
    }
}

struct FakeTso {
    high_water: AtomicU64,
}

impl FakeTso {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            high_water: AtomicU64::new(SEED_TS + 1),
        })
    }
}

#[async_trait]
impl TsoSource for FakeTso {
    async fn is_leader(&self) -> bool {
        true
    }

    async fn allocate(&self, batch_size: u32) -> CoordResult<TsoBatch> {
        let start = self
            .high_water
            .fetch_add(u64::from(batch_size), Ordering::SeqCst);
        Ok(TsoBatch {
            start_ts: start,
            end_ts: start + u64::from(batch_size),
        })
    }
}

/// 单节点、region 0 sequencer + 2 个数据 Region，两个 Region 都已 apply 到
/// [`SEED_TS`]（`safe_ts == SEED_TS`），因此显式 `start_ts = SEED_TS` 的
/// `Begin` 不会进入 safe_ts 等待。
struct TxnCommitNode {
    node: Arc<CoordNode>,
    manager: Arc<RegionManager>,
    _metrics: Arc<Metrics>,
    _dir: TempDir,
    _factory: RaftNetworkFactoryImpl,
    _rpc: RaftRpcService,
}

impl TxnCommitNode {
    async fn start() -> Self {
        Self::start_with(4 * 1024 * 1024, true).await
    }

    async fn start_with_max_txn_bytes(max_txn_bytes: u64) -> Self {
        Self::start_with(max_txn_bytes, true).await
    }

    async fn start_without_sequencer() -> Self {
        Self::start_with(4 * 1024 * 1024, false).await
    }

    async fn start_with(max_txn_bytes: u64, with_sequencer: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().to_path_buf();
        let addr = format!("127.0.0.1:{}", find_port());

        // region 0 system raft（决策日志的权威日志）
        let root_dir = base.join("region-0");
        std::fs::create_dir_all(&root_dir).unwrap();
        let backend = RedbBackend::open(&root_dir, &StorageConfig::default()).unwrap();
        let root_mvcc =
            Arc::new(MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs).unwrap());
        let root_factory = RaftNetworkFactoryImpl::new(1);
        root_factory.register_node(1, addr.clone());
        let root_tracker = Arc::new(SnapshotTracker::default());
        let root_log = LogStore::new(&root_dir).await.unwrap();
        let root_sm = StateMachineStore::new(
            Arc::clone(&root_mvcc),
            root_dir.join("snapshots"),
            root_tracker,
        );
        let root_raft = Arc::new(
            new_raft(1, Arc::new(raft_config()), root_factory, root_log, root_sm)
                .await
                .expect("create region 0 raft"),
        );
        root_raft
            .initialize(BTreeMap::from([(1u64, new_basic_node(&addr))]))
            .await
            .expect("initialize region 0 raft");

        // 数据 Region 1/2（单 voter）
        let factory = RaftNetworkFactoryImpl::new(1);
        factory.register_node(1, addr.clone());
        let rpc = RaftRpcService::new();
        let manager = Arc::new(RegionManager::new(1));
        for (region_id, start, end) in [
            (1u64, b"".to_vec(), b"m".to_vec()),
            (2u64, b"m".to_vec(), b"".to_vec()),
        ] {
            let meta = RegionMeta {
                region_id,
                start_key: start,
                end_key: end,
                epoch: RegionEpoch::initial(),
                peers: vec![Peer {
                    node_id: 1,
                    raft_addr: addr.clone(),
                    role: PeerRole::Voter,
                }],
                approximate_size: 0,
                approximate_keys: 0,
            };
            manager
                .spawn_region(
                    &factory,
                    &rpc,
                    RegionRuntimeSpec {
                        meta,
                        data_dir: region_data_dir(&base, region_id),
                        raft_config: Arc::new(raft_config()),
                        object_store: None,
                        revision_mode: RevisionMode::CommitTs,
                    },
                    true,
                )
                .await
                .expect("spawn data region");
        }

        let metrics = Arc::new(Metrics::new());
        let mut node = CoordNode::new(Arc::clone(&root_mvcc));
        node.node_id = 1;
        node.raft = Some(Arc::clone(&root_raft));
        node.region_manager = Some(Arc::clone(&manager));
        node.set_kvt_state(
            KvTState::new(
                true,
                KvTRuntimeConfig {
                    read_ts_wait_ms: 50,
                    max_txn_bytes,
                    ..KvTRuntimeConfig::default()
                },
            )
            .with_implemented([
                KvTCapability::Kv,
                KvTCapability::Tso,
                KvTCapability::Txn,
            ]),
        );
        node.set_metrics(Arc::clone(&metrics));
        let tso = TsoClient::new(FakeTso::new(), 1024, None);
        node.set_tso_client(Arc::clone(&tso));
        if with_sequencer {
            node.set_commit_sequencer(Arc::new(CommitSequencer::new(
                root_raft.as_ref().clone(),
                1,
                tso,
                Arc::clone(&root_mvcc),
                256,
                4 * 1024 * 1024,
                SequencerValidationLimits {
                    max_read_set: 4_096,
                    max_range: 10_000,
                },
                Duration::from_secs(5),
            )));
        }
        let node = Arc::new(node);

        wait_for_leaders(&node, &manager).await;

        let fixture = Self {
            node,
            manager,
            _metrics: metrics,
            _dir: dir,
            _factory: factory,
            _rpc: rpc,
        };
        // 两个 Region 都推进到 SEED_TS，避免 Begin 因 safe_ts 落后而等待。
        fixture.put(b"apple", b"seed", SEED_TS).await;
        fixture.put(b"melon", b"seed", SEED_TS).await;
        assert_eq!(fixture.node.safe_ts(), SEED_TS);
        fixture
    }

    /// 直接向目标 Region 的 raft 提交 `PutV2`（模拟已存在的快照数据）。
    async fn put(&self, key: &[u8], value: &[u8], commit_ts: u64) {
        let rt = self.manager.route_runtime(key).expect("route key");
        let resp = rt
            .raft
            .client_write(Command::PutV2 {
                key: key.to_vec(),
                value: value.to_vec(),
                lease_id: None,
                commit_ts,
            })
            .await
            .expect("put v2");
        assert!(
            matches!(resp.response(), Response::Put { revision } if *revision == commit_ts),
            "put must apply at its commit_ts"
        );
    }

    async fn begin(&self, start_ts: i64) -> coord_proto::txn::v2::BeginResponse {
        TxnV2::begin(
            &*self.node,
            tonic::Request::new(BeginRequest {
                start_ts,
                ttl_ms: 0,
            }),
        )
        .await
        .expect("begin txn")
        .into_inner()
    }

    async fn read(
        &self,
        txn_id: &[u8],
        key: &[u8],
        end: &[u8],
    ) -> Result<coord_proto::txn::v2::ReadResponse, tonic::Status> {
        TxnV2::read(
            &*self.node,
            tonic::Request::new(ReadRequest {
                txn_id: txn_id.to_vec(),
                key: key.to_vec(),
                range_end: end.to_vec(),
                limit: 0,
                keys_only: false,
            }),
        )
        .await
        .map(|resp| resp.into_inner())
    }

    async fn commit(
        &self,
        txn_id: &[u8],
        writes: &[(&[u8], &[u8])],
        request_id: &[u8],
    ) -> Result<i64, tonic::Status> {
        let mutations = writes
            .iter()
            .map(|(key, value)| Mutation {
                op: Op::Put as i32,
                key: key.to_vec(),
                range_end: Vec::new(),
                value: value.to_vec(),
                lease_id: 0,
            })
            .collect();
        self.commit_raw(txn_id, mutations, request_id).await
    }

    async fn commit_raw(
        &self,
        txn_id: &[u8],
        mutations: Vec<Mutation>,
        request_id: &[u8],
    ) -> Result<i64, tonic::Status> {
        TxnV2::commit(
            &*self.node,
            tonic::Request::new(CommitRequest {
                txn_id: txn_id.to_vec(),
                mutations,
                request_id: request_id.to_vec(),
            }),
        )
        .await
        .map(|resp| resp.into_inner().commit_ts)
    }

    async fn abort(&self, txn_id: &[u8]) {
        TxnV2::abort(
            &*self.node,
            tonic::Request::new(AbortRequest {
                txn_id: txn_id.to_vec(),
            }),
        )
        .await
        .expect("abort txn");
    }

    async fn range(
        &self,
        key: &[u8],
        end: &[u8],
        read_ts: u64,
    ) -> Vec<coord_proto::kv::v2::KeyValue> {
        KvV2::range(
            &*self.node,
            tonic::Request::new(RangeRequestV2 {
                key: key.to_vec(),
                range_end: end.to_vec(),
                limit: 0,
                read_ts: read_ts as i64,
                keys_only: false,
                count_only: false,
            }),
        )
        .await
        .expect("range")
        .into_inner()
        .kvs
    }
}

async fn wait_for_leaders(node: &CoordNode, manager: &RegionManager) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let root_leader = node
            .raft
            .as_ref()
            .expect("region 0 raft")
            .current_leader()
            .await
            == Some(1);
        let data_ready = (1..=2).all(|id| {
            manager.runtime(id).is_some_and(|rt| {
                let m = rt.raft.metrics();
                let m = m.borrow_watched();
                m.current_leader == Some(1) && m.last_quorum_acked.is_some()
            })
        });
        if root_leader && data_ready {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "raft leaders not elected in time"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_txn_commit_is_visible_and_replays_same_request_id() {
    let fx = TxnCommitNode::start().await;
    let txn = fx.begin(SEED_TS as i64).await;
    assert_eq!(txn.start_ts, SEED_TS as i64);

    let read = fx.read(&txn.txn_id, b"apple", b"").await.unwrap();
    assert_eq!(read.kvs[0].value, b"seed");

    let commit_ts = fx
        .commit(&txn.txn_id, &[(b"apple", b"v1")], b"req-1")
        .await
        .unwrap();
    assert_eq!(commit_ts, (SEED_TS + 1) as i64);

    // 同一 request_id 重放：同一 commit_ts，决策只有一条。
    assert_eq!(
        fx.commit(&txn.txn_id, &[(b"apple", b"v1")], b"req-1")
            .await
            .unwrap(),
        commit_ts
    );
    assert_eq!(
        fx.node
            .storage
            .sequencer_decisions_after(SEED_TS, 16)
            .unwrap()
            .len(),
        1
    );

    // 不同 request_id 不能静默接受另一份写集。
    let err = fx
        .commit(&txn.txn_id, &[(b"apple", b"v2")], b"req-2")
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");
    assert_eq!(
        error_code_of(&err).as_deref(),
        Some("FAILED_PRECONDITION"),
        "{err:?}"
    );

    // 提交后不再接受读。
    let err = fx.read(&txn.txn_id, b"apple", b"").await.unwrap_err();
    assert_eq!(err.code(), Code::Aborted, "{err:?}");
    assert_eq!(error_code_of(&err).as_deref(), Some("TXN_NOT_FOUND"));

    // 决策在 apply 前不可见；apply 后同一 read_ts 可见。
    apply_pending_decisions(&fx.node, 16)
        .await
        .expect("apply decisions");
    assert_eq!(fx.node.safe_ts(), commit_ts as u64);
    let kvs = fx.range(b"apple", b"", commit_ts as u64).await;
    assert_eq!(kvs[0].value, b"v1");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_txn_conflict_then_retry_with_new_start_ts() {
    let fx = TxnCommitNode::start().await;
    let a = fx.begin(SEED_TS as i64).await;
    fx.read(&a.txn_id, b"apple", b"").await.unwrap();

    let b = fx.begin(SEED_TS as i64).await;
    let b_ts = fx
        .commit(&b.txn_id, &[(b"apple", b"from-b")], b"req-b")
        .await
        .unwrap();
    assert!(b_ts > SEED_TS as i64);

    let err = fx
        .commit(&a.txn_id, &[(b"apple", b"from-a")], b"req-a")
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Aborted, "{err:?}");
    assert_eq!(error_code_of(&err).as_deref(), Some("TXN_CONFLICT"));

    // 冲突不写决策：A 的提交时间戳（B 之后的下一个）在日志里不存在。
    let a_ts = b_ts + 1;
    assert!(fx
        .node
        .storage
        .get_sequencer_decision(a_ts as u64)
        .unwrap()
        .is_none());
    // 会话仍可 Abort；Abort 后按不存在处理。
    fx.abort(&a.txn_id).await;
    let err = fx
        .commit(&a.txn_id, &[(b"apple", b"from-a")], b"req-a")
        .await
        .unwrap_err();
    assert_eq!(error_code_of(&err).as_deref(), Some("TXN_NOT_FOUND"));

    // 应用 B 后，用 B 的 commit_ts 作为新 start_ts 重跑成功。
    apply_pending_decisions(&fx.node, 16)
        .await
        .expect("apply decisions");
    let retry = fx.begin(b_ts).await;
    let read = fx.read(&retry.txn_id, b"apple", b"").await.unwrap();
    assert_eq!(read.kvs[0].value, b"from-b");
    let retry_ts = fx
        .commit(&retry.txn_id, &[(b"apple", b"from-a")], b"req-a2")
        .await
        .unwrap();
    assert!(retry_ts > b_ts);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_txn_commit_limits_and_invalid_mutations_keep_session_usable() {
    let fx = TxnCommitNode::start_with_max_txn_bytes(256).await;
    let txn = fx.begin(SEED_TS as i64).await;

    let err = fx
        .commit(&txn.txn_id, &[(b"big", &[0u8; 512])], b"req-big")
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err:?}");
    assert_eq!(error_code_of(&err).as_deref(), Some("TXN_TOO_LARGE"));

    let err = fx
        .commit_raw(
            &txn.txn_id,
            vec![Mutation {
                op: 0,
                ..Default::default()
            }],
            b"req-bad",
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err:?}");

    // 失败都释放了提交占用：同一会话仍可读、可成功提交。
    fx.read(&txn.txn_id, b"apple", b"").await.unwrap();
    let ts = fx
        .commit(&txn.txn_id, &[(b"apple", b"ok")], b"req-ok")
        .await
        .unwrap();
    assert!(ts > SEED_TS as i64);

    let err = fx.commit(b"missing", &[], b"req-x").await.unwrap_err();
    assert_eq!(err.code(), Code::Aborted, "{err:?}");
    assert_eq!(error_code_of(&err).as_deref(), Some("TXN_NOT_FOUND"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_txn_commit_requires_sequencer_when_absent() {
    let fx = TxnCommitNode::start_without_sequencer().await;
    let txn = fx.begin(SEED_TS as i64).await;
    let err = fx
        .commit(&txn.txn_id, &[(b"apple", b"v")], b"req")
        .await
        .expect_err("commit without sequencer");
    assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");
    // 失败释放了提交占用：会话仍可 Abort。
    fx.abort(&txn.txn_id).await;
}
