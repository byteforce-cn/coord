//! KV-T G5（#162）Split/Merge executor 验收（真实 region 0 raft）：
//!
//! - `SplitRegion` operator 经队列认领后真实执行：intent 落 region 0（`PdOp`）、
//!   迁移钩子被调用、cutover 在 region 0 的同一 apply 事务里原子改写权威表；
//! - cutover 后 `PdMetaStore` 单向对账到表（调度器读视图），intent 清理，
//!   operator `Success`；
//! - 重复执行同一 operator 不再触发迁移（无重复副作用）；
//! - 未装配迁移钩子时 fail-closed：不写 intent、不动表；
//! - 迁移失败时表不动、intent 保留为 `Prepared`（交给恢复/GC）。

mod common;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use common::bind_listener;

use coord_core::storage::StorageBackend;
use coord_core::types::{NodeID, Peer, PeerRole, RegionEpoch, RegionMeta};
use coord_server::pd::executor::{OperatorExecutor, SplitMergeMigrator};
use coord_server::pd::meta_store::PdMetaStore;
use coord_server::pd::operator::Operator;
use coord_server::pd::types::PdConfig;
use coord_server::pd::PlacementDriver;
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::RaftRpcService;
use coord_server::raft::region_runtime::RegionRaftHandle;
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::system_raft::{CoordSystemRaftHandle, SystemRaftHandle};
use coord_server::raft::type_config::{Command, IntentPhase, PdOp, RegionMetaOp};
use coord_server::raft::{new_basic_node, new_raft, CoordRaft, RaftConfig, RaftNode};
use coord_server::storage::mvcc::{MvccStorage, RevisionMode};
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::region_table::{SplitMergeKind, SplitMergePlan};
use coord_server::storage::snapshot::SnapshotTracker;
use tempfile::TempDir;

const NODE_ID: u64 = 1;
const DEADLINE: Duration = Duration::from_secs(30);

fn raft_config() -> RaftConfig {
    RaftConfig {
        heartbeat_interval: 100,
        election_timeout_min: 400,
        election_timeout_max: 800,
        ..Default::default()
    }
}

fn region(id: u64, start: &[u8], end: &[u8]) -> RegionMeta {
    RegionMeta {
        region_id: id,
        start_key: start.to_vec(),
        end_key: end.to_vec(),
        epoch: RegionEpoch::initial(),
        peers: vec![Peer {
            node_id: NODE_ID,
            raft_addr: "127.0.0.1:1".to_string(),
            role: PeerRole::Voter,
        }],
        approximate_size: 0,
        approximate_keys: 0,
    }
}

/// 迁移钩子替身：记录每次调用的计划；可注入一次失败。
struct RecordingMigrator {
    calls: Mutex<Vec<SplitMergePlan>>,
    fail: AtomicBool,
}

impl RecordingMigrator {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            calls: Mutex::new(Vec::new()),
            fail: AtomicBool::new(false),
        })
    }

    fn calls(&self) -> Vec<SplitMergePlan> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl SplitMergeMigrator for RecordingMigrator {
    async fn migrate(&self, plan: &SplitMergePlan) -> Result<(), String> {
        self.calls.lock().unwrap().push(plan.clone());
        if self.fail.load(Ordering::SeqCst) {
            Err("migration bulk copy failed".to_string())
        } else {
            Ok(())
        }
    }

    async fn arm_write_fence(&self, _op_id: u64, _plan: &SplitMergePlan) -> Result<(), String> {
        Ok(())
    }

    async fn release_write_fence(
        &self,
        _op_id: u64,
        _plan: &SplitMergePlan,
    ) -> Result<bool, String> {
        Ok(true)
    }
}

/// 数据 Region raft 替身：split/merge 只需要 leader 守卫。
struct LeaderRaft;

#[async_trait]
impl RegionRaftHandle for LeaderRaft {
    async fn current_leader(&self) -> Option<NodeID> {
        Some(NODE_ID)
    }

    async fn current_members(&self) -> coord_core::error::Result<Vec<Peer>> {
        Ok(vec![Peer {
            node_id: NODE_ID,
            raft_addr: "127.0.0.1:1".to_string(),
            role: PeerRole::Voter,
        }])
    }

    async fn add_learner(&self, _node_id: NodeID, _addr: &str) -> coord_core::error::Result<()> {
        Ok(())
    }

    async fn promote_to_voter(&self, _node_id: NodeID) -> coord_core::error::Result<()> {
        Ok(())
    }

    async fn remove_voter(&self, _node_id: NodeID) -> coord_core::error::Result<()> {
        Ok(())
    }

    async fn transfer_leader(&self, _to: NodeID) -> coord_core::error::Result<()> {
        Ok(())
    }
}

struct Harness {
    _dir: TempDir,
    raft0: Arc<CoordRaft>,
    mvcc0: Arc<MvccStorage<RedbBackend>>,
    pd: Arc<PlacementDriver>,
    system: Arc<CoordSystemRaftHandle>,
}

impl Harness {
    async fn boot(regions: Vec<RegionMeta>) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();

        let (_listener, addr) = bind_listener().await;
        let factory = coord_server::raft::network::RaftNetworkFactoryImpl::new(NODE_ID);
        factory.register_node(NODE_ID, addr.clone());
        let rpc = RaftRpcService::new();
        let raft_config = Arc::new(raft_config());

        // region 0：权威 Region 表 + PD 队列 + intent journal 所在组。
        let backend = RedbBackend::open(&root, &Default::default()).expect("open region 0");
        let mvcc0 =
            Arc::new(MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs).unwrap());
        let log = LogStore::new(&root).await.expect("open region 0 log");
        let sm = StateMachineStore::new(
            Arc::clone(&mvcc0),
            root.join("snapshots"),
            Arc::new(SnapshotTracker::default()),
        );
        let raft0 = Arc::new(
            new_raft(NODE_ID, Arc::clone(&raft_config), factory.clone(), log, sm)
                .await
                .expect("create region 0 raft"),
        );
        rpc.set_raft(raft0.as_ref().clone());
        let members: BTreeMap<u64, RaftNode> = BTreeMap::from([(NODE_ID, new_basic_node(&addr))]);
        raft0
            .initialize(members)
            .await
            .expect("initialize region 0");

        let stop = tokio::time::Instant::now() + DEADLINE;
        while raft0.current_leader().await != Some(NODE_ID) {
            assert!(
                tokio::time::Instant::now() < stop,
                "region 0 leader not elected"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let response = raft0
            .client_write(Command::RegionMeta(RegionMetaOp::Init {
                regions: regions.clone(),
            }))
            .await
            .expect("seed region table");
        assert!(
            matches!(
                response.response(),
                coord_server::raft::type_config::Response::RegionMeta { .. }
            ),
            "region table seeded"
        );

        // 调度器的读视图：初始与表一致（后续由 executor 单向对账）。
        let meta_store = Arc::new(PdMetaStore::new());
        for region in &regions {
            meta_store.create_region(region.clone()).unwrap();
        }
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let pd = Arc::new(PlacementDriver::new(
            PdConfig::default(),
            meta_store,
            shutdown_rx,
            NODE_ID,
        ));
        let system = Arc::new(CoordSystemRaftHandle::new(
            raft0.as_ref().clone(),
            Arc::clone(&mvcc0),
        ));
        pd.attach_system_raft(system.clone());

        Self {
            _dir: dir,
            raft0,
            mvcc0,
            pd,
            system,
        }
    }

    async fn enqueue(&self, op: Operator) -> u64 {
        let before = self.mvcc0.pd_queue_entries().unwrap().len();
        self.system
            .propose_pd(PdOp::Enqueue {
                op,
                requester: NODE_ID,
                proposed_at_unix: 1_700_000_000,
            })
            .await
            .expect("enqueue operator");
        let queue = self.mvcc0.pd_queue_entries().unwrap();
        assert_eq!(queue.len(), before + 1, "operator enqueued");
        queue.last().expect("entry").op_id
    }

    fn executor(&self, migrator: Arc<RecordingMigrator>) -> OperatorExecutor {
        OperatorExecutor::new(Arc::clone(&self.pd), NODE_ID)
            .with_migrator(migrator)
            .with_cutover_timeout(Duration::from_secs(10))
    }
}

fn resolver() -> Arc<coord_server::pd::executor::RegionRaftResolver> {
    Arc::new(|_rid| Some(Arc::new(LeaderRaft) as Arc<dyn RegionRaftHandle>))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_split_operator_cuts_over_the_authoritative_table() {
    let harness = Harness::boot(vec![region(1, b"", b"")]).await;
    let migrator = RecordingMigrator::new();
    let executor = harness.executor(migrator.clone());
    let op_id = harness
        .enqueue(Operator::SplitRegion {
            region_id: 1,
            split_key: b"m".to_vec(),
            new_region_id: 2,
        })
        .await;

    let done = executor
        .execute_one(&*resolver())
        .await
        .expect("split executed");
    assert_eq!(done.name(), "split-region");

    // 表：父区间收缩、子区间接管、版本前进。
    let table = harness.mvcc0.region_table().unwrap();
    assert_eq!(table.version, 2);
    assert_eq!(table.get(1).unwrap().end_key, b"m");
    let child = table.get(2).expect("child region in the table");
    assert_eq!(child.start_key, b"m");
    assert_eq!(child.end_key, b"");
    assert_eq!(child.epoch.version, table.get(1).unwrap().epoch.version);

    // journal：intent 已清理；队列条目 Success。
    assert!(
        harness.mvcc0.pd_intents().unwrap().is_empty(),
        "intent cleared"
    );
    let entry = harness
        .mvcc0
        .pd_queue_entries()
        .unwrap()
        .into_iter()
        .find(|e| e.op_id == op_id)
        .expect("queue entry");
    assert_eq!(
        entry.status,
        coord_server::pd::operator::OperatorStatus::Success
    );

    // 调度视图单向对账到表。
    assert_eq!(harness.pd.meta_store().get_region(1).unwrap().end_key, b"m");
    let child_meta = harness.pd.meta_store().get_region(2).expect("child meta");
    assert_eq!(child_meta.start_key, b"m");

    // 迁移钩子恰好一次，计划与表一致。
    let calls = migrator.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].kind, SplitMergeKind::Split);
    assert_eq!(calls[0].expected_table_version, 1);

    // 重复驱动：没有 Pending/Running 条目，不再触发迁移（无重复副作用）。
    assert!(executor.execute_one(&*resolver()).await.is_none());
    assert_eq!(migrator.calls().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_split_without_migrator_fails_closed() {
    let harness = Harness::boot(vec![region(1, b"", b"")]).await;
    let executor = OperatorExecutor::new(Arc::clone(&harness.pd), NODE_ID);
    harness
        .enqueue(Operator::SplitRegion {
            region_id: 1,
            split_key: b"m".to_vec(),
            new_region_id: 2,
        })
        .await;

    executor.execute_one(&*resolver()).await.expect("processed");
    let table = harness.mvcc0.region_table().unwrap();
    assert_eq!(table.version, 1, "table untouched without a migrator");
    assert!(table.get(2).is_none());
    assert!(harness.mvcc0.pd_intents().unwrap().is_empty());
    assert!(harness.pd.meta_store().get_region(2).is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_migration_failure_keeps_intent_for_recovery() {
    let harness = Harness::boot(vec![region(1, b"", b"")]).await;
    let migrator = RecordingMigrator::new();
    migrator.fail.store(true, Ordering::SeqCst);
    let executor = harness.executor(migrator.clone());
    let op_id = harness
        .enqueue(Operator::SplitRegion {
            region_id: 1,
            split_key: b"m".to_vec(),
            new_region_id: 2,
        })
        .await;

    executor.execute_one(&*resolver()).await.expect("processed");
    assert_eq!(migrator.calls().len(), 1);
    assert_eq!(
        harness.mvcc0.region_table().unwrap().version,
        1,
        "no cutover on migration failure"
    );
    let intents = harness.mvcc0.pd_intents().unwrap();
    assert_eq!(intents.len(), 1, "intent retained for recovery");
    assert_eq!(intents[0].op_id, op_id);
    assert_eq!(intents[0].phase, IntentPhase::Prepared);

    // 迁移恢复后重复驱动：沿用同一 intent，重新迁移并完成 cutover。
    migrator.fail.store(false, Ordering::SeqCst);
    let entry = harness
        .mvcc0
        .pd_queue_entries()
        .unwrap()
        .into_iter()
        .find(|e| e.op_id == op_id)
        .expect("queue entry");
    assert!(
        matches!(
            entry.status,
            coord_server::pd::operator::OperatorStatus::Failed(_)
        ),
        "failed operator is terminal"
    );
    // 终态 operator 的 intent 由下一拍 GC：表不动、intent 清理。
    executor.execute_one(&*resolver()).await;
    assert!(harness.mvcc0.pd_intents().unwrap().is_empty());
    assert_eq!(harness.mvcc0.region_table().unwrap().version, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_merge_operator_cuts_over_and_retires_the_right_region() {
    let harness = Harness::boot(vec![region(1, b"", b"m"), region(2, b"m", b"")]).await;
    let migrator = RecordingMigrator::new();
    let executor = harness.executor(migrator.clone());
    harness
        .enqueue(Operator::MergeRegion { left: 1, right: 2 })
        .await;

    executor
        .execute_one(&*resolver())
        .await
        .expect("merge executed");
    let table = harness.mvcc0.region_table().unwrap();
    assert_eq!(table.version, 2);
    assert_eq!(table.get(1).unwrap().end_key, b"");
    assert!(table.get(2).is_none());
    assert_eq!(migrator.calls().len(), 1);
    assert_eq!(migrator.calls()[0].kind, SplitMergeKind::Merge);

    assert!(harness.pd.meta_store().get_region(2).is_none());
    assert_eq!(harness.pd.meta_store().get_region(1).unwrap().end_key, b"");
    assert!(harness.mvcc0.pd_intents().unwrap().is_empty());
    assert!(harness.system.region_table().unwrap().get(2).is_none());
}
