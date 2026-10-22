//! KV-T G5（#163）split 搬迁器验收：真实目标 Region raft + 真实源存储。
//!
//! - 分页导入必须经目标 Region 的 raft 日志（`Command::ImportRange`），
//!   apply 后目标存储看到行与元数据、水位钉在源快照 revision；
//! - 搬迁器先 drain 子区间，再按固定 revision 分页导出/导入；
//! - 源 revision 在导出期间前进 → 整次迁移失败，不拼接两代快照；
//! - 重复执行幂等（同值重放不写、revision 不倒退）；
//! - merge 在 #164 前 fail-closed。

mod common;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use common::bind_listener;

use coord_core::storage::StorageBackend;
use coord_core::types::{NodeID, Peer, PeerRole, RegionEpoch, RegionMeta};
use coord_server::pd::executor::SplitMergeMigrator;
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcService};
use coord_server::raft::region_runtime::CoordRangeImportTarget;
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::{new_basic_node, new_raft, RaftConfig, RaftNode};
use coord_server::split_merge::{
    RangeImportMigrator, RangeImportTarget, RegionImportTargetResolver, RegionMvccResolver,
    SplitDrain,
};
use coord_server::storage::mvcc::{AppliedLogId, KvMetadata, MvccStorage, RevisionMode};
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::region_table::{plan_split, RegionTable, SplitMergePlan};
use coord_server::storage::snapshot::SnapshotTracker;
use tempfile::TempDir;

const NODE_ID: NodeID = 1;
const DEADLINE: Duration = Duration::from_secs(30);

fn raft_config() -> RaftConfig {
    RaftConfig {
        heartbeat_interval: 100,
        election_timeout_min: 400,
        election_timeout_max: 800,
        ..Default::default()
    }
}

/// 未初始化的单节点目标 Region raft（新 split 目标预创建后的真实状态：
/// raft 组已存在，leader 尚未选出）。
struct TargetRaft {
    dir: TempDir,
    mvcc: Arc<MvccStorage<RedbBackend>>,
    raft: coord_server::raft::CoordRaft,
    members: BTreeMap<u64, RaftNode>,
}

async fn boot_target_raft() -> TargetRaft {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();

    let backend = RedbBackend::open(&root, &Default::default()).expect("open target region store");
    let mvcc = Arc::new(
        MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs).expect("target mvcc"),
    );
    let log = LogStore::new(&root).await.expect("open target region log");
    let sm = StateMachineStore::new(
        Arc::clone(&mvcc),
        root.join("snapshots"),
        Arc::new(SnapshotTracker::default()),
    );
    let (_listener, addr) = bind_listener().await;
    let factory = RaftNetworkFactoryImpl::new(NODE_ID);
    factory.register_node(NODE_ID, addr.clone());
    let rpc = RaftRpcService::new();
    let raft = new_raft(NODE_ID, Arc::new(raft_config()), factory, log, sm)
        .await
        .expect("create target region raft");
    rpc.set_raft(raft.clone());

    let members: BTreeMap<u64, RaftNode> = BTreeMap::from([(NODE_ID, new_basic_node(&addr))]);
    TargetRaft {
        dir,
        mvcc,
        raft,
        members,
    }
}

/// 起一个单节点目标 Region raft（revision = commit_ts），返回其存储与导入端口。
async fn boot_target_region() -> (
    TempDir,
    Arc<MvccStorage<RedbBackend>>,
    Arc<CoordRangeImportTarget>,
) {
    let target = boot_target_raft().await;
    target
        .raft
        .initialize(target.members.clone())
        .await
        .expect("initialize target region raft");

    let stop = tokio::time::Instant::now() + DEADLINE;
    while target.raft.current_leader().await != Some(NODE_ID) {
        assert!(
            tokio::time::Instant::now() < stop,
            "target region leader not elected"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let port = Arc::new(CoordRangeImportTarget::from_raft(target.raft));
    (target.dir, target.mvcc, port)
}

fn live(key: &[u8], value: &[u8], revision: i64, lease_id: i64) -> (Vec<u8>, Vec<u8>, KvMetadata) {
    (
        key.to_vec(),
        value.to_vec(),
        KvMetadata {
            version: 1,
            create_revision: revision,
            mod_revision: revision,
            lease_id,
            deleted: false,
        },
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_import_range_crosses_the_target_region_raft() {
    let (_dir, mvcc, target) = boot_target_region().await;

    let imported = target
        .import_range(b"m", b"", vec![live(b"m", b"1", 10, 7)], 10, true, true)
        .await
        .expect("import through the target raft");

    assert_eq!(imported, 1);
    assert_eq!(mvcc.get(b"m").unwrap().as_deref(), Some(&b"1"[..]));
    let meta = mvcc.get_kv_metadata(b"m").unwrap().expect("metadata");
    assert_eq!((meta.mod_revision, meta.lease_id), (10, 7));
    assert_eq!(mvcc.current_revision(), 10);
    assert_eq!(mvcc.compacted_revision().unwrap(), 10);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_target_import_waits_for_leader_election() {
    let target = boot_target_raft().await;
    let raft = target.raft.clone();
    let members = target.members.clone();
    let election = tokio::spawn(async move {
        // pending 目标刚建组，leader 选举在导入开始之后才完成。
        tokio::time::sleep(Duration::from_millis(300)).await;
        raft.initialize(members)
            .await
            .expect("initialize target region raft");
    });

    let port = CoordRangeImportTarget::from_raft(target.raft.clone())
        .with_leader_wait(Duration::from_secs(5));
    let imported = port
        .import_range(b"m", b"", vec![live(b"m", b"1", 10, 7)], 10, true, true)
        .await
        .expect("import waits through leader election");
    election.await.expect("election task");

    assert_eq!(imported, 1);
    assert_eq!(target.mvcc.get(b"m").unwrap().as_deref(), Some(&b"1"[..]));
}

// ───────────────────────── Task 2: 真实搬迁器 ─────────────────────────

fn parent_region() -> RegionMeta {
    RegionMeta {
        region_id: 1,
        start_key: Vec::new(),
        end_key: Vec::new(),
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

fn split_plan() -> SplitMergePlan {
    let table = RegionTable {
        version: 1,
        regions: vec![parent_region()],
    };
    plan_split(&table, 1, b"m", 2).expect("split plan")
}

fn applied(index: u64) -> AppliedLogId {
    AppliedLogId {
        term: 1,
        node_id: 1,
        index,
    }
}

/// 父 Region：a=10、m=11（lease 7）、z=12，n 在 14 被删除（子区间 tombstone）。
async fn source_region_with_rows() -> (TempDir, Arc<MvccStorage<RedbBackend>>) {
    let dir = tempfile::tempdir().unwrap();
    let backend = RedbBackend::open(dir.path(), &Default::default()).expect("open source store");
    let storage = Arc::new(
        MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs).expect("source mvcc"),
    );
    storage
        .put_at_commit_ts(b"a", b"1", None, 10, applied(1))
        .unwrap();
    storage
        .put_at_commit_ts(b"m", b"1", Some(7), 11, applied(2))
        .unwrap();
    storage
        .put_at_commit_ts(b"z", b"3", None, 12, applied(3))
        .unwrap();
    storage
        .put_at_commit_ts(b"n", b"9", None, 13, applied(4))
        .unwrap();
    storage.delete_at_commit_ts(b"n", 14, applied(5)).unwrap();
    (dir, storage)
}

/// 记录被 drain 的区间；不注入延迟。
#[derive(Default)]
struct RecordingDrain {
    calls: Mutex<Vec<(Vec<u8>, Vec<u8>)>>,
}

impl RecordingDrain {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn calls(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl SplitDrain for RecordingDrain {
    async fn drain(&self, start: &[u8], end: &[u8], _timeout: Duration) -> Result<(), String> {
        self.calls
            .lock()
            .unwrap()
            .push((start.to_vec(), end.to_vec()));
        Ok(())
    }
}

fn migrator(
    source: &Arc<MvccStorage<RedbBackend>>,
    target: Arc<dyn RangeImportTarget>,
    drain: Arc<RecordingDrain>,
) -> RangeImportMigrator {
    let src = Arc::clone(source);
    let source_resolver: Arc<RegionMvccResolver> =
        Arc::new(move |region_id| (region_id == 1).then(|| Arc::clone(&src)));
    let target_resolver: Arc<RegionImportTargetResolver> =
        Arc::new(move |region_id| (region_id == 2).then(|| Arc::clone(&target)));
    RangeImportMigrator::new(source_resolver, target_resolver, Arc::new(|_| None), drain)
}

/// 第一次导入前先推进源 revision，模拟「导出期间源没冻结」的漂移。
struct DriftTarget {
    inner: Arc<CoordRangeImportTarget>,
    source: Arc<MvccStorage<RedbBackend>>,
    fired: AtomicBool,
}

#[async_trait]
impl RangeImportTarget for DriftTarget {
    async fn import_range(
        &self,
        start: &[u8],
        end: &[u8],
        rows: Vec<(Vec<u8>, Vec<u8>, KvMetadata)>,
        watermark: u64,
        seal_prior_history: bool,
        done: bool,
    ) -> Result<u64, String> {
        if !self.fired.swap(true, Ordering::SeqCst) {
            self.source
                .put_at_commit_ts(b"zz", b"late", None, 99, applied(9))
                .expect("inject source drift");
        }
        self.inner
            .import_range(start, end, rows, watermark, seal_prior_history, done)
            .await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_split_migrator_moves_live_rows_and_pins_revision() {
    let (_src_dir, source) = source_region_with_rows().await;
    let (_dst_dir, target_mvcc, target) = boot_target_region().await;
    let drain = RecordingDrain::new();

    migrator(&source, target.clone(), Arc::clone(&drain))
        .with_page_size(1)
        .migrate(&split_plan())
        .await
        .expect("split migration");

    assert_eq!(
        drain.calls(),
        vec![(b"m".to_vec(), b"".to_vec())],
        "drain 只覆盖即将易主的子区间"
    );
    assert_eq!(target_mvcc.get(b"m").unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(target_mvcc.get(b"z").unwrap().as_deref(), Some(&b"3"[..]));
    assert!(target_mvcc.get(b"a").unwrap().is_none(), "父区间左半不搬迁");
    assert!(
        target_mvcc.get(b"n").unwrap().is_none(),
        "tombstone 不得复活"
    );
    let meta = target_mvcc
        .get_kv_metadata(b"m")
        .unwrap()
        .expect("imported metadata");
    assert_eq!((meta.create_revision, meta.mod_revision), (11, 11));
    assert_eq!(meta.lease_id, 7, "lease 绑定随行搬迁");
    assert_eq!(target_mvcc.current_revision(), 14);
    assert_eq!(target_mvcc.compacted_revision().unwrap(), 14);
    assert_eq!(source.current_revision(), 14, "源只读、revision 不变");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_split_migrator_is_idempotent_across_restarts() {
    let (_src_dir, source) = source_region_with_rows().await;
    let (_dst_dir, target_mvcc, target) = boot_target_region().await;
    let m = migrator(&source, target, RecordingDrain::new()).with_page_size(1);

    m.migrate(&split_plan()).await.expect("first migration");
    let before = target_mvcc.get_kv_metadata(b"m").unwrap().unwrap();
    let value_before = target_mvcc.get(b"m").unwrap();

    m.migrate(&split_plan()).await.expect("replayed migration");

    assert_eq!(target_mvcc.get(b"m").unwrap(), value_before);
    let after = target_mvcc.get_kv_metadata(b"m").unwrap().unwrap();
    assert_eq!(
        (after.mod_revision, after.version),
        (before.mod_revision, before.version),
        "重放不得改写版本"
    );
    assert_eq!(target_mvcc.current_revision(), 14);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_split_migrator_fails_on_source_drift() {
    let (_src_dir, source) = source_region_with_rows().await;
    let (_dst_dir, target_mvcc, target) = boot_target_region().await;
    let drifting: Arc<dyn RangeImportTarget> = Arc::new(DriftTarget {
        inner: target,
        source: Arc::clone(&source),
        fired: AtomicBool::new(false),
    });

    let err = migrator(&source, drifting, RecordingDrain::new())
        .with_page_size(1)
        .migrate(&split_plan())
        .await
        .expect_err("drift must fail the migration");

    assert!(err.contains("source revision advanced"), "{err}");
    assert!(
        target_mvcc.get(b"m").unwrap().is_some(),
        "已导入的页保留（重试幂等）"
    );
    assert!(
        target_mvcc.get(b"z").unwrap().is_none(),
        "漂移之后不得继续导页"
    );
}

/// 目标 Region 的 raft 组由 lifecycle 异步预创建：解析失败不是终局错误。
/// 搬迁器在 `target_wait` 内重试解析，不能把「pending 尚未装配」直接判失败
/// （executor 会把该 operator 标成 Failed）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_split_migrator_waits_for_the_target_region_to_be_assembled() {
    let (_src_dir, source) = source_region_with_rows().await;
    let (_dst_dir, target_mvcc, target) = boot_target_region().await;

    let src = Arc::clone(&source);
    let source_resolver: Arc<RegionMvccResolver> =
        Arc::new(move |region_id| (region_id == 1).then(|| Arc::clone(&src)));
    let attempts = Arc::new(AtomicUsize::new(0));
    let tries = Arc::clone(&attempts);
    let target: Arc<dyn RangeImportTarget> = target;
    let target_resolver: Arc<RegionImportTargetResolver> = Arc::new(move |region_id| {
        if region_id != 2 {
            return None;
        }
        // 前 3 次解析模拟 lifecycle 还没预创建出 pending 目标。
        if tries.fetch_add(1, Ordering::SeqCst) < 3 {
            None
        } else {
            Some(Arc::clone(&target))
        }
    });

    RangeImportMigrator::new(
        source_resolver,
        target_resolver,
        Arc::new(|_| None),
        RecordingDrain::new(),
    )
    .with_page_size(1)
    .with_target_wait(Duration::from_secs(2))
    .migrate(&split_plan())
    .await
    .expect("migration waits for the pending target");

    assert!(
        attempts.load(Ordering::SeqCst) >= 4,
        "resolver must be retried"
    );
    assert_eq!(target_mvcc.get(b"m").unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(target_mvcc.get(b"z").unwrap().as_deref(), Some(&b"3"[..]));
}

/// 目标始终不出现 → 有界失败（intent 保留、可重试），错误信息指向装配缺口。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_split_migrator_gives_up_when_the_target_never_appears() {
    let (_src_dir, source) = source_region_with_rows().await;
    let src = Arc::clone(&source);
    let source_resolver: Arc<RegionMvccResolver> =
        Arc::new(move |region_id| (region_id == 1).then(|| Arc::clone(&src)));
    let target_resolver: Arc<RegionImportTargetResolver> = Arc::new(|_| None);

    let started = std::time::Instant::now();
    let err = RangeImportMigrator::new(
        source_resolver,
        target_resolver,
        Arc::new(|_| None),
        RecordingDrain::new(),
    )
    .with_target_wait(Duration::from_millis(150))
    .migrate(&split_plan())
    .await
    .expect_err("missing target must fail the migration");

    assert!(err.contains("not assembled"), "{err}");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "target wait must stay bounded"
    );
}
