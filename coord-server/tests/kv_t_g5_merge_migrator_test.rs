//! KV-T G5（#164）merge 搬迁器验收：真实左（接收方）Region raft + 真实右 Region 存储。
//!
//! - drain 覆盖合并后的整段区间（左右两侧的读集都冻结）后才导出；
//! - 右 Region 的 live 行按固定 revision 分页导出，经左 Region raft 导入；
//! - 左 Region 的 `current_revision` 取两侧最大，`compacted_revision` 不被抬高
//!   （merge 不压缩接收方自己的历史）；
//! - 重放幂等；drain 失败/右区间不一致时 fail-closed、不导入任何页。

mod common;

use std::collections::BTreeMap;
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
    RegionRangeResolver, SplitDrain,
};
use coord_server::storage::mvcc::{AppliedLogId, MvccStorage, RevisionMode};
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::region_table::{
    plan_merge, RegionTable, SplitMergeKind, SplitMergePlan,
};
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

fn applied(index: u64) -> AppliedLogId {
    AppliedLogId {
        term: 1,
        node_id: NODE_ID,
        index,
    }
}

fn region(region_id: u64, start: &[u8], end: &[u8]) -> RegionMeta {
    RegionMeta {
        region_id,
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

/// 合并计划：region 1 = `["", "m")` 吸收 region 2 = `["m", "")`。
fn merge_plan() -> SplitMergePlan {
    let table = RegionTable {
        version: 1,
        regions: vec![region(1, b"", b"m"), region(2, b"m", b"")],
    };
    plan_merge(&table, 1, 2).expect("merge plan")
}

/// 单节点左 Region raft（真实导入路径：`Command::ImportRange` 经 raft apply）。
async fn boot_left_region() -> (
    TempDir,
    Arc<MvccStorage<RedbBackend>>,
    Arc<dyn RangeImportTarget>,
) {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().to_path_buf();

    let backend = RedbBackend::open(&root, &Default::default()).expect("open left region store");
    let mvcc = Arc::new(
        MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs).expect("left mvcc"),
    );
    let log = LogStore::new(&root).await.expect("open left region log");
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
        .expect("create left region raft");
    rpc.set_raft(raft.clone());

    let members: BTreeMap<u64, RaftNode> = BTreeMap::from([(NODE_ID, new_basic_node(&addr))]);
    raft.initialize(members)
        .await
        .expect("initialize left region raft");

    let stop = tokio::time::Instant::now() + DEADLINE;
    while raft.current_leader().await != Some(NODE_ID) {
        assert!(
            tokio::time::Instant::now() < stop,
            "left region leader not elected"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let port: Arc<dyn RangeImportTarget> =
        Arc::new(CoordRangeImportTarget::from_raft(raft.clone()));
    (dir, mvcc, port)
}

/// 右 Region（被吸收方）：m=11（lease 7）、z=12，n 在 14 被删除。
async fn donor_region() -> (TempDir, Arc<MvccStorage<RedbBackend>>) {
    let dir = tempfile::tempdir().expect("temp dir");
    let backend = RedbBackend::open(dir.path(), &Default::default()).expect("open donor store");
    let storage = Arc::new(
        MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs).expect("donor mvcc"),
    );
    storage
        .put_at_commit_ts(b"m", b"1", Some(7), 11, applied(1))
        .unwrap();
    storage
        .put_at_commit_ts(b"z", b"3", None, 12, applied(2))
        .unwrap();
    storage
        .put_at_commit_ts(b"n", b"9", None, 13, applied(3))
        .unwrap();
    storage.delete_at_commit_ts(b"n", 14, applied(4)).unwrap();
    (dir, storage)
}

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

struct FailingDrain;

#[async_trait]
impl SplitDrain for FailingDrain {
    async fn drain(&self, _start: &[u8], _end: &[u8], _timeout: Duration) -> Result<(), String> {
        Err("drain timed out".to_string())
    }
}

fn migrator(
    left: &Arc<MvccStorage<RedbBackend>>,
    target: Arc<dyn RangeImportTarget>,
    donor: &Arc<MvccStorage<RedbBackend>>,
    drain: Arc<dyn SplitDrain>,
) -> RangeImportMigrator {
    let donor = Arc::clone(donor);
    let left_source = Arc::clone(left);
    let source: Arc<RegionMvccResolver> = Arc::new(move |region_id| match region_id {
        1 => Some(Arc::clone(&left_source)),
        2 => Some(Arc::clone(&donor)),
        _ => None,
    });
    let targets: Arc<RegionImportTargetResolver> =
        Arc::new(move |region_id| (region_id == 1).then(|| Arc::clone(&target)));
    let ranges: Arc<RegionRangeResolver> =
        Arc::new(|region_id| (region_id == 2).then(|| (b"m".to_vec(), Vec::new())));
    RangeImportMigrator::new(source, targets, ranges, drain)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_merge_migrator_moves_right_rows_into_left_and_keeps_history() {
    let (_left_dir, left, target) = boot_left_region().await;
    left.put_at_commit_ts(b"a", b"left", None, 20, applied(10))
        .expect("seed left row");
    let (_donor_dir, donor) = donor_region().await;
    let drain = RecordingDrain::new();

    migrator(
        &left,
        target,
        &donor,
        Arc::clone(&drain) as Arc<dyn SplitDrain>,
    )
    .with_page_size(1)
    .migrate(&merge_plan())
    .await
    .expect("merge migration");

    assert_eq!(
        drain.calls(),
        vec![(b"".to_vec(), b"".to_vec())],
        "drain 覆盖合并后的整段区间"
    );
    assert_eq!(left.get(b"a").unwrap().as_deref(), Some(&b"left"[..]));
    assert_eq!(left.get(b"m").unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(left.get(b"z").unwrap().as_deref(), Some(&b"3"[..]));
    assert!(left.get(b"n").unwrap().is_none(), "tombstone 不得复活");
    let meta = left.get_kv_metadata(b"m").unwrap().expect("metadata");
    assert_eq!(
        (meta.create_revision, meta.mod_revision, meta.lease_id),
        (11, 11, 7)
    );
    assert_eq!(left.current_revision(), 20, "较新的左 revision 不回退");
    assert_eq!(
        left.compacted_revision().unwrap(),
        0,
        "merge 不压缩接收方历史"
    );
    assert_eq!(donor.current_revision(), 14, "源只读、revision 不变");
    assert_eq!(
        left.range(b"", 10).unwrap().len(),
        3,
        "合并后左 Region 持有两侧数据"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_merge_migrator_is_idempotent_across_restarts() {
    let (_left_dir, left, target) = boot_left_region().await;
    // 接收方已 apply 到源水位（合并前它一直在推进自己的 revision）。
    left.put_at_commit_ts(b"a", b"left", None, 14, applied(10))
        .expect("seed left revision");
    let (_donor_dir, donor) = donor_region().await;
    let m = migrator(
        &left,
        target,
        &donor,
        RecordingDrain::new() as Arc<dyn SplitDrain>,
    )
    .with_page_size(1);

    m.migrate(&merge_plan()).await.expect("first migration");
    let before = left.get_kv_metadata(b"m").unwrap().unwrap();
    let value_before = left.get(b"m").unwrap();

    m.migrate(&merge_plan()).await.expect("replayed migration");

    assert_eq!(left.get(b"m").unwrap(), value_before);
    let after = left.get_kv_metadata(b"m").unwrap().unwrap();
    assert_eq!(
        (after.mod_revision, after.version),
        (before.mod_revision, before.version),
        "重放不得改写版本"
    );
    assert_eq!(left.current_revision(), 14);
    assert_eq!(left.compacted_revision().unwrap(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_merge_fails_closed_when_receiver_lags_donor() {
    let (_left_dir, left, target) = boot_left_region().await;
    assert_eq!(
        left.current_revision(),
        0,
        "左 Region 还没有 apply 任何决策"
    );
    let (_donor_dir, donor) = donor_region().await;

    let err = migrator(
        &left,
        target,
        &donor,
        RecordingDrain::new() as Arc<dyn SplitDrain>,
    )
    .migrate(&merge_plan())
    .await
    .expect_err("接收方落后于源水位时必须失败");

    assert!(err.contains("behind"), "{err}");
    assert!(left.get(b"m").unwrap().is_none(), "不得导入任何行");
    assert_eq!(left.current_revision(), 0, "失败不得抬水位");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_merge_keeps_left_history_and_seals_absorbed_range() {
    let (_left_dir, left, target) = boot_left_region().await;
    left.put_at_commit_ts(b"a", b"left", None, 5, applied(9))
        .expect("seed early left row");
    left.put_at_commit_ts(b"b", b"left", None, 20, applied(10))
        .expect("push left revision past the donor");
    let (_donor_dir, donor) = donor_region().await;

    migrator(
        &left,
        target,
        &donor,
        RecordingDrain::new() as Arc<dyn SplitDrain>,
    )
    .migrate(&merge_plan())
    .await
    .expect("merge migration");

    assert_eq!(left.current_revision(), 20, "水位不回退");
    assert_eq!(
        left.compacted_revision().unwrap(),
        0,
        "merge 不压缩接收方历史"
    );

    // 左半历史仍然可读。
    let left_rows = left
        .range_at_revision(b"", b"m", 10, 10)
        .expect("left history stays readable");
    assert_eq!(left_rows, vec![(b"a".to_vec(), b"left".to_vec())]);

    // 吸收区间的历史读在导入水位之前必须显式失败（导入行没有 changelog）。
    assert!(
        left.range_at_revision(b"m", b"", 10, 10).is_err(),
        "低于导入水位的区间历史必须 HISTORY_UNAVAILABLE"
    );
    assert!(
        left.get_at_revision(b"z", 10).is_err(),
        "点读同样不得回填导入值"
    );
    assert!(
        left.get_at_revision(b"z", 20).ok().flatten().is_some(),
        "导入水位之后的历史读可回填导入值"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_merge_drain_failure_imports_nothing() {
    let (_left_dir, left, target) = boot_left_region().await;
    let (_donor_dir, donor) = donor_region().await;

    let err = migrator(&left, target, &donor, Arc::new(FailingDrain))
        .migrate(&merge_plan())
        .await
        .expect_err("drain 失败必须让整次搬迁失败");

    assert!(err.contains("drain"), "{err}");
    assert!(left.get(b"m").unwrap().is_none(), "未 drain 不得导入");
    assert!(left.get(b"z").unwrap().is_none());
    assert_eq!(left.current_revision(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_merge_clears_stale_rows_from_a_prior_partial_run() {
    let (_left_dir, left, target) = boot_left_region().await;
    // 上一次部分导入留下、但源侧后来已删除的 Key：不先清空就会在重试后复活。
    left.put_at_commit_ts(b"n", b"stale", None, 9, applied(9))
        .expect("seed stale imported row");
    left.put_at_commit_ts(b"a", b"left", None, 20, applied(10))
        .expect("seed left row");
    let (_donor_dir, donor) = donor_region().await;

    migrator(
        &left,
        target,
        &donor,
        RecordingDrain::new() as Arc<dyn SplitDrain>,
    )
    .migrate(&merge_plan())
    .await
    .expect("merge migration");

    assert!(
        left.get(b"n").unwrap().is_none(),
        "重试必须清掉上次部分导入、而源侧已删除的行"
    );
    assert_eq!(left.get(b"m").unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(left.get(b"z").unwrap().as_deref(), Some(&b"3"[..]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_merge_fails_closed_when_donor_range_is_missing_or_mismatched() {
    let (_left_dir, left, target) = boot_left_region().await;
    let (_donor_dir, donor) = donor_region().await;

    let donor_source = Arc::clone(&donor);
    let source: Arc<RegionMvccResolver> =
        Arc::new(move |region_id| (region_id == 2).then(|| Arc::clone(&donor_source)));
    let target_port = Arc::clone(&target);
    let targets: Arc<RegionImportTargetResolver> =
        Arc::new(move |region_id| (region_id == 1).then(|| Arc::clone(&target_port)));

    // 表里查不到被吸收 Region → 不猜区间、不导入。
    let missing: Arc<RegionRangeResolver> = Arc::new(|_| None);
    let err = RangeImportMigrator::new(
        Arc::clone(&source),
        Arc::clone(&targets),
        missing,
        RecordingDrain::new(),
    )
    .migrate(&merge_plan())
    .await
    .expect_err("missing donor range must fail");
    assert!(err.contains("donor"), "{err}");

    // 区间端点与计划不一致（例如表已变化）→ 同样 fail-closed。
    let mismatched: Arc<RegionRangeResolver> =
        Arc::new(|region_id| (region_id == 2).then(|| (b"m".to_vec(), b"zz".to_vec())));
    let err = RangeImportMigrator::new(source, targets, mismatched, RecordingDrain::new())
        .migrate(&merge_plan())
        .await
        .expect_err("mismatched donor range must fail");
    assert!(err.contains("does not match"), "{err}");

    assert!(left.get(b"m").unwrap().is_none());
    assert_eq!(left.current_revision(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_merge_plan_without_donor_region_is_rejected() {
    let (_left_dir, left, target) = boot_left_region().await;
    let (_donor_dir, donor) = donor_region().await;
    let mut plan = merge_plan();
    assert_eq!(plan.kind, SplitMergeKind::Merge);
    plan.changes.retain(|change| {
        !matches!(
            change,
            coord_server::storage::region_table::RegionChange::Remove { .. }
        )
    });

    let err = migrator(
        &left,
        target,
        &donor,
        RecordingDrain::new() as Arc<dyn SplitDrain>,
    )
    .migrate(&plan)
    .await
    .expect_err("merge without a donor must fail");
    assert!(err.contains("donor"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_merge_plan_with_two_donors_is_rejected() {
    let (_left_dir, left, target) = boot_left_region().await;
    let (_donor_dir, donor) = donor_region().await;
    let mut plan = merge_plan();
    let second = plan
        .changes
        .iter()
        .find(|change| {
            matches!(
                change,
                coord_server::storage::region_table::RegionChange::Remove { .. }
            )
        })
        .cloned()
        .expect("merge plan removes its donor");
    plan.changes.push(second);

    let err = migrator(
        &left,
        target,
        &donor,
        RecordingDrain::new() as Arc<dyn SplitDrain>,
    )
    .migrate(&plan)
    .await
    .expect_err("merge with two donors must fail");
    assert!(err.contains("exactly one donor"), "{err}");
}
