//! KV-T G3（#198）`coord.txn.v2` 区间写验收：
//!
//! - `DeleteRange [key, range_end)` 跨 Region 原子可见（safe_ts 收敛）；
//! - 同一 txn/request 重放与重复 apply 幂等，不产生第二个决策；
//! - point/range 读集与区间写相交冲突、不相交/相邻不冲突；
//! - 非法形状 `INVALID_ARGUMENT`，失败不卡住会话。

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

fn delete_range_mutation(start: &[u8], end: &[u8]) -> Mutation {
    Mutation {
        op: Op::DeleteRange as i32,
        key: start.to_vec(),
        range_end: end.to_vec(),
        value: Vec::new(),
        lease_id: 0,
    }
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
struct DeleteRangeNode {
    node: Arc<CoordNode>,
    manager: Arc<RegionManager>,
    _metrics: Arc<Metrics>,
    _dir: TempDir,
    _factory: RaftNetworkFactoryImpl,
    _rpc: RaftRpcService,
}

impl DeleteRangeNode {
    async fn start() -> Self {
        Self::start_with(4 * 1024 * 1024, true).await
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
        // 两个 Region 都推进到 SEED_TS（zebra 更早，仍低于 safe_ts），
        // 避免 Begin 因 safe_ts 落后而等待。
        fixture.put(b"zebra", b"seed", SEED_TS - 1).await;
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

    async fn commit_delete_range(
        &self,
        txn_id: &[u8],
        start: &[u8],
        end: &[u8],
        request_id: &[u8],
    ) -> Result<i64, tonic::Status> {
        self.commit_raw(txn_id, vec![delete_range_mutation(start, end)], request_id)
            .await
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
async fn test_txn_delete_range_is_atomic_across_regions() {
    let fx = DeleteRangeNode::start().await;
    let txn = fx.begin(SEED_TS as i64).await;
    let read = fx.read(&txn.txn_id, b"", b"z").await.unwrap();
    assert_eq!(read.kvs.len(), 2, "apple and melon are inside [.., z)");
    assert!(read.kvs.iter().all(|kv| kv.key.as_slice() != b"zebra"));

    let commit_ts = fx
        .commit_delete_range(&txn.txn_id, b"", b"z", b"req-dr")
        .await
        .unwrap();
    assert_eq!(commit_ts, (SEED_TS + 1) as i64);

    // 决策落盘但尚未 apply：safe_ts 还停在 seed 水位。
    assert_eq!(fx.node.safe_ts(), SEED_TS);

    apply_pending_decisions(&fx.node, 16)
        .await
        .expect("apply decisions");
    assert_eq!(fx.node.safe_ts(), commit_ts as u64);
    for region_id in [1u64, 2] {
        assert_eq!(
            fx.manager
                .runtime(region_id)
                .unwrap()
                .mvcc
                .last_applied_decision_ts()
                .unwrap(),
            commit_ts as u64,
            "region {region_id} must advance its decision cursor"
        );
    }

    assert!(fx.range(b"", b"z", commit_ts as u64).await.is_empty());
    let outside = fx.range(b"zebra", b"", commit_ts as u64).await;
    assert_eq!(outside.len(), 1, "zebra is outside the deleted range");
    assert_eq!(outside[0].value, b"seed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_txn_delete_range_replays_same_request_id_and_apply_is_idempotent() {
    let fx = DeleteRangeNode::start().await;
    let txn = fx.begin(SEED_TS as i64).await;

    let commit_ts = fx
        .commit_delete_range(&txn.txn_id, b"a", b"m", b"req-dr")
        .await
        .unwrap();
    assert_eq!(commit_ts, (SEED_TS + 1) as i64);
    assert_eq!(
        fx.commit_delete_range(&txn.txn_id, b"a", b"m", b"req-dr")
            .await
            .unwrap(),
        commit_ts,
        "the same request id must replay the first decision"
    );
    assert_eq!(
        fx.node
            .storage
            .sequencer_decisions_after(SEED_TS, 16)
            .unwrap()
            .len(),
        1
    );

    apply_pending_decisions(&fx.node, 16)
        .await
        .expect("apply decisions");
    assert_eq!(fx.node.safe_ts(), commit_ts as u64);
    apply_pending_decisions(&fx.node, 16)
        .await
        .expect("replay apply");
    assert_eq!(fx.node.safe_ts(), commit_ts as u64);
    assert!(fx.range(b"apple", b"", commit_ts as u64).await.is_empty());
    assert_eq!(
        fx.range(b"melon", b"", commit_ts as u64).await.len(),
        1,
        "melon is outside [a, m)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_txn_delete_range_conflicts_with_point_and_range_read_sets() {
    // point 读落在区间写内：提交必须冲突，且不写决策。
    let fx = DeleteRangeNode::start().await;
    let a = fx.begin(SEED_TS as i64).await;
    fx.read(&a.txn_id, b"apple", b"").await.unwrap();
    let b = fx.begin(SEED_TS as i64).await;
    let b_ts = fx
        .commit_delete_range(&b.txn_id, b"a", b"m", b"req-b")
        .await
        .unwrap();
    let err = fx
        .commit(&a.txn_id, &[(b"apple", b"from-a")], b"req-a")
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Aborted, "{err:?}");
    assert_eq!(error_code_of(&err).as_deref(), Some("TXN_CONFLICT"));
    assert!(
        fx.node
            .storage
            .get_sequencer_decision(b_ts as u64 + 1)
            .unwrap()
            .is_none(),
        "a conflict must not leave a decision"
    );
    fx.abort(&a.txn_id).await;

    // range 读与区间写相交：同样是冲突。
    let fx = DeleteRangeNode::start().await;
    let c = fx.begin(SEED_TS as i64).await;
    fx.read(&c.txn_id, b"a", b"f").await.unwrap();
    let d = fx.begin(SEED_TS as i64).await;
    fx.commit_delete_range(&d.txn_id, b"d", b"k", b"req-d")
        .await
        .unwrap();
    let err = fx.commit(&c.txn_id, &[], b"req-c").await.unwrap_err();
    assert_eq!(err.code(), Code::Aborted, "{err:?}");
    assert_eq!(error_code_of(&err).as_deref(), Some("TXN_CONFLICT"));

    // 不相交：读 [a,c) 与删除 [m,z) 无共同 key，提交成功。
    let fx = DeleteRangeNode::start().await;
    let e = fx.begin(SEED_TS as i64).await;
    fx.read(&e.txn_id, b"a", b"c").await.unwrap();
    let f = fx.begin(SEED_TS as i64).await;
    fx.commit_delete_range(&f.txn_id, b"m", b"z", b"req-f")
        .await
        .unwrap();
    let ts = fx.commit(&e.txn_id, &[], b"req-e").await.unwrap();
    assert!(ts > SEED_TS as i64);

    // 相邻（[a,m) 与 [m,z)）不算相交：读 [a,m) 的会话可提交。
    let fx = DeleteRangeNode::start().await;
    let g = fx.begin(SEED_TS as i64).await;
    fx.read(&g.txn_id, b"a", b"m").await.unwrap();
    let h = fx.begin(SEED_TS as i64).await;
    fx.commit_delete_range(&h.txn_id, b"m", b"z", b"req-h")
        .await
        .unwrap();
    let ts = fx.commit(&g.txn_id, &[], b"req-g").await.unwrap();
    assert!(ts > SEED_TS as i64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_txn_delete_range_rejects_invalid_shapes() {
    let fx = DeleteRangeNode::start().await;
    let txn = fx.begin(SEED_TS as i64).await;

    let bad = vec![
        (b"a".to_vec(), Vec::new()),
        (b"b".to_vec(), b"a".to_vec()),
        (b"a".to_vec(), b"a".to_vec()),
    ];
    for (index, (start, end)) in bad.into_iter().enumerate() {
        let err = fx
            .commit_raw(
                &txn.txn_id,
                vec![delete_range_mutation(&start, &end)],
                format!("req-bad-{index}").as_bytes(),
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument, "{err:?}");
        assert_eq!(error_code_of(&err).as_deref(), Some("INVALID_ARGUMENT"));
    }

    let err = fx
        .commit_raw(
            &txn.txn_id,
            vec![Mutation {
                op: 0,
                ..Default::default()
            }],
            b"req-unspec",
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err:?}");

    // 失败都释放了提交占用：会话仍可读、可成功提交。
    fx.read(&txn.txn_id, b"apple", b"").await.unwrap();
    let ts = fx
        .commit(&txn.txn_id, &[(b"apple", b"ok")], b"req-ok")
        .await
        .unwrap();
    assert!(ts > SEED_TS as i64);
}
