// M0 验收测试（L2 进程内，规格 A.4/A.5/A.6）：
// - applied 状态同事务持久化，重启从 `META_LAST_APPLIED` 恢复（D-A4）
// - revision ≡ log index（D-A2），apply 幂等守卫（D-A3）
// - 快照落盘（tmp → fsync → rename → 校验和）与启动加载（A.6.1–A.6.3）
// - LogStore purge 前置守卫（M0-5）
//
// 对应文档：`docs/production/11-architecture-redesign.md` 规格 A；
// `docs/production/15-milestone-task-breakdown.md` M0-3/M0-4/M0-5。

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::sync::Arc;

use coord_core::storage::StorageBackend;
use coord_core::types::StorageConfig;
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::RaftNetworkFactoryImpl;
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::type_config::{Command, Response, TypeConfig};
use coord_server::raft::{
    new_basic_node, new_raft, LeaderId, LogIdOf, Membership, RaftConfig, RaftLogStorage,
    RaftSnapshotBuilder, RaftStateMachine, StoredMembershipOf,
};
use coord_server::storage::mvcc::{AppliedLogId, MvccStorage};
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::snapshot::SnapshotTracker;

fn find_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn log_id(term: u64, node_id: u64, index: u64) -> LogIdOf<TypeConfig> {
    LogIdOf::<TypeConfig>::new(LeaderId { term, node_id }, index)
}

/// 进程内启动单节点 raft + state machine，返回（raft, mvcc）。
/// 数据目录由调用方持有（TempDir），全句柄 drop 即等价进程退出。
async fn start_node(
    data_dir: &std::path::Path,
    tracker: Arc<SnapshotTracker>,
) -> (
    Arc<coord_server::raft::CoordRaft>,
    Arc<MvccStorage<RedbBackend>>,
) {
    let storage_config = StorageConfig::default();
    let backend = RedbBackend::open(data_dir, &storage_config).expect("open redb backend");
    let mvcc = Arc::new(MvccStorage::new(backend).expect("create mvcc"));

    let log_store = LogStore::new(data_dir)
        .await
        .expect("create raft log store")
        .with_snapshot_tracker(Arc::clone(&tracker));
    let sm_store = StateMachineStore::new(
        Arc::clone(&mvcc),
        data_dir.join("snapshots"),
        Arc::clone(&tracker),
    );

    let raft_addr = format!("127.0.0.1:{}", find_port());
    let network_factory = RaftNetworkFactoryImpl::new(1);
    network_factory.register_node(1, raft_addr.clone());

    let raft = new_raft(
        1,
        Arc::new(RaftConfig::default()),
        network_factory,
        log_store,
        sm_store,
    )
    .await
    .expect("create raft instance");

    let mut members = BTreeMap::new();
    members.insert(1, new_basic_node(&raft_addr));
    raft.initialize(members).await.expect("raft initialize");

    (Arc::new(raft), mvcc)
}

/// 写入 N 条 KV，返回最后一条的 revision
async fn put_keys(raft: &coord_server::raft::CoordRaft, prefix: &str, n: u64) -> u64 {
    let mut last_rev = 0;
    for i in 0..n {
        let key = format!("{prefix}/{i:05}");
        let cmd = Command::Put {
            key: key.into_bytes(),
            value: format!("v{i}").into_bytes(),
            lease_id: None,
        };
        let resp = raft.client_write(cmd).await.expect("client_write");
        match resp.response() {
            Response::Put { revision } => last_rev = *revision,
            _ => panic!("unexpected response"),
        }
    }
    last_rev
}

/// M0-3：applied 持久化 —— 写入后"重启"（全句柄 drop + 重新打开），
/// `applied_state` 从盘恢复、revision 不重复、changelog 无重复条目。
#[tokio::test]
async fn test_applied_persisted_across_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();
    let tracker = Arc::new(SnapshotTracker::default());

    let (raft, mvcc) = start_node(&data_dir, Arc::clone(&tracker)).await;

    // 写入 3 条
    let last_rev = put_keys(&raft, "/m0/a", 3).await;
    assert!(last_rev >= 3);

    // 磁盘 META_LAST_APPLIED 与 revision 一致
    let applied = mvcc.get_applied_log_id().unwrap().unwrap();
    assert_eq!(applied.index, last_rev, "applied index must equal revision");

    // 全量停转（等价于进程退出；redb 文件锁要求后台任务完全释放句柄）
    let _ = raft.shutdown().await;
    drop(raft);
    drop(mvcc);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // "重启"
    let storage_config = StorageConfig::default();
    let backend = RedbBackend::open(&data_dir, &storage_config).expect("reopen redb backend");
    let mvcc2 = Arc::new(MvccStorage::new(backend).expect("recreate mvcc"));
    let (applied, tail) = mvcc2.verify_consistency().unwrap();
    assert_eq!(
        applied, last_rev,
        "applied must be restored from META_LAST_APPLIED"
    );
    assert_eq!(tail, Some(last_rev), "changelog tail must match applied");

    // changelog 无重复条目
    let events = mvcc2.read_changelog_entries(1).unwrap();
    let mut revs: Vec<u64> = events.iter().map(|e| e.revision).collect();
    revs.sort_unstable();
    let orig_len = revs.len();
    revs.dedup();
    assert_eq!(
        revs.len(),
        orig_len,
        "changelog must contain no duplicate revisions"
    );

    // 数据完整
    assert_eq!(mvcc2.get(b"/m0/a/00000").unwrap(), Some(b"v0".to_vec()));
    assert_eq!(mvcc2.current_revision(), last_rev);

    // 重新拉起 raft：新写入的 revision 从 last_rev+1 继续（不再从内存计数器重来）
    let tracker2 = Arc::new(SnapshotTracker::default());
    let log_store2 = LogStore::new(&data_dir)
        .await
        .expect("recreate log store")
        .with_snapshot_tracker(Arc::clone(&tracker2));
    let sm_store2 = StateMachineStore::new(
        Arc::clone(&mvcc2),
        data_dir.join("snapshots"),
        Arc::clone(&tracker2),
    );

    let network_factory = RaftNetworkFactoryImpl::new(1);
    network_factory.register_node(1, format!("127.0.0.1:{}", find_port()));
    let raft2 = new_raft(
        1,
        Arc::new(RaftConfig::default()),
        network_factory,
        log_store2,
        sm_store2,
    )
    .await
    .expect("recreate raft");

    // 等待重启后完成选举（openraft 单节点需重新当选 leader）
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        if raft2.current_leader().await == Some(1) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "leader not elected after restart"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    // 选举完成的内部状态就绪存在竞态：对 ForwardToLeader 做重试直至成功
    let cmd = Command::Put {
        key: b"/m0/after-restart".to_vec(),
        value: b"x".to_vec(),
        lease_id: None,
    };
    let write_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
    let revision_after_restart = loop {
        match raft2.client_write(cmd.clone()).await {
            Ok(resp) => match resp.response() {
                Response::Put { revision } => break *revision,
                _ => panic!("unexpected response"),
            },
            Err(e) => {
                assert!(
                    tokio::time::Instant::now() < write_deadline,
                    "post-restart write kept failing: {e}"
                );
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        }
    };
    assert_eq!(
        revision_after_restart,
        last_rev + 1,
        "revision must continue from applied+1"
    );
    assert_eq!(mvcc2.current_revision(), last_rev + 1);
}

/// M0-2：apply 幂等守卫 —— 同 revision 重复 apply 无副作用
#[tokio::test]
async fn test_replay_is_idempotent() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();
    let tracker = Arc::new(SnapshotTracker::default());

    let (raft, mvcc) = start_node(&data_dir, Arc::clone(&tracker)).await;
    let last_rev = put_keys(&raft, "/m0/b", 2).await;

    // 模拟重启重放：对已 applied 的 revision 再次 apply
    let applied = AppliedLogId {
        term: 1,
        node_id: 1,
        index: last_rev,
    };
    let outcome = mvcc
        .put_at_revision(b"/m0/b/00000", b"OVERWRITE", None, last_rev, applied)
        .unwrap();
    assert!(
        outcome.replayed,
        "re-applying an applied revision must be replayed"
    );
    assert_eq!(
        mvcc.get(b"/m0/b/00000").unwrap(),
        Some(b"v0".to_vec()),
        "replay must not overwrite data"
    );

    drop(raft);
    drop(mvcc);
}

/// M0-4：快照落盘 + 启动加载（tmp → fsync → rename → SHA256 → META_SNAPSHOT）
#[tokio::test]
async fn test_snapshot_persisted_and_loaded() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();
    let tracker = Arc::new(SnapshotTracker::default());

    // 直接驱动状态机（不经 raft）：apply 5 条 + 设置 applied/membership
    {
        let storage_config = StorageConfig::default();
        let backend = RedbBackend::open(&data_dir, &storage_config).expect("open redb backend");
        let mvcc = Arc::new(MvccStorage::new(backend).expect("create mvcc"));
        let mut sm_store = StateMachineStore::new(
            Arc::clone(&mvcc),
            data_dir.join("snapshots"),
            Arc::clone(&tracker),
        );

        for i in 1..=5u64 {
            let key = format!("/m0/c/{i:05}");
            let outcome = mvcc
                .put_at_revision(
                    key.as_bytes(),
                    format!("v{i}").as_bytes(),
                    None,
                    i,
                    AppliedLogId {
                        term: 1,
                        node_id: 1,
                        index: i,
                    },
                )
                .unwrap();
            assert!(!outcome.replayed);
        }
        *sm_store.last_applied.lock() = Some(log_id(1, 1, 5));
        let voters: std::collections::BTreeSet<u64> = [1].into_iter().collect();
        let membership: Membership<u64, coord_server::raft::RaftNode> =
            Membership::new_with_defaults(vec![voters], vec![]);
        *sm_store.last_membership.lock() =
            StoredMembershipOf::<TypeConfig>::new(Some(log_id(1, 1, 0)), membership);

        let snapshot = sm_store.build_snapshot().await.expect("build snapshot");
        assert_eq!(snapshot.meta.last_log_id, Some(log_id(1, 1, 5)));

        // 快照文件已落盘且 tracker 登记
        let snap_dir = data_dir.join("snapshots");
        let files: Vec<_> = std::fs::read_dir(&snap_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|e| e == "snap").unwrap_or(false))
            .collect();
        assert_eq!(files.len(), 1, "one snapshot file must exist");
        assert!(
            tracker.durable_covers(5),
            "tracker must record durable snapshot"
        );
    }

    // "重启"：从 META_SNAPSHOT 加载（含 SHA256 校验）
    let storage_config = StorageConfig::default();
    let backend = RedbBackend::open(&data_dir, &storage_config).expect("reopen redb backend");
    let mvcc2 = Arc::new(MvccStorage::new(backend).expect("recreate mvcc"));
    let tracker2 = Arc::new(SnapshotTracker::default());
    let mut sm_store2 = StateMachineStore::new(
        Arc::clone(&mvcc2),
        data_dir.join("snapshots"),
        Arc::clone(&tracker2),
    );

    let cur = sm_store2.get_current_snapshot().await.unwrap();
    assert!(cur.is_some(), "current_snapshot must be loaded from disk");
    assert_eq!(cur.unwrap().meta.last_log_id, Some(log_id(1, 1, 5)));
    assert!(
        tracker2.durable_covers(5),
        "restart must register durable snapshot"
    );
    assert_eq!(mvcc2.current_revision(), 5, "applied must be restored");
}

/// M0-5：purge 前置守卫 —— 无覆盖快照时拒绝删除日志
#[tokio::test]
async fn test_purge_guard_refuses_without_durable_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();

    let tracker = Arc::new(SnapshotTracker::default());
    let mut log_store = LogStore::new(&data_dir)
        .await
        .expect("create log store")
        .with_snapshot_tracker(Arc::clone(&tracker));

    let err = log_store.purge(log_id(1, 1, 10)).await;
    assert!(err.is_err(), "purge without durable snapshot must fail");
    assert!(
        format!("{err:?}").contains("no durable snapshot"),
        "error must explain the missing snapshot: {err:?}"
    );

    // 登记一份覆盖快照后 purge 放行（S-RCV-01：durable_covers 校验文件真实存在）
    let snap_dir = data_dir.join("snapshots");
    std::fs::create_dir_all(&snap_dir).unwrap();
    let snap_path = snap_dir.join("s-10-1.snap");
    std::fs::write(&snap_path, b"dummy snapshot bytes").unwrap();
    tracker.record_durable(10, 1, snap_path);
    log_store
        .purge(log_id(1, 1, 10))
        .await
        .expect("purge with durable snapshot must succeed");
}
