// P0-A 验收测试（L3 进程内）：快照导出 → 删光数据 → 导入恢复
//
// 验收标准（规格 A.8 第 4 条）：`version/create_revision/mod_revision` 完整。
// 对应 `coord snapshot save/restore` 工具的数据路径（CLI 仅做文件读写，
// 核心逻辑为 export_snapshot_data / import_snapshot_data）。
//
// 对应文档：`docs/production/11-architecture-redesign.md` 规格 A.8；
// `docs/production/15-milestone-task-breakdown.md` P0-A.3。

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::sync::Arc;

use coord_core::storage::StorageBackend;
use coord_core::types::StorageConfig;
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::RaftNetworkFactoryImpl;
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::type_config::{Command, Response};
use coord_server::storage::mvcc::MvccStorage;
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::snapshot::{
    export_snapshot_data, import_snapshot_data, SnapshotTracker,
};

fn find_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

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

    let raft = coord_server::raft::new_raft(
        1,
        Arc::new(coord_server::raft::RaftConfig::default()),
        network_factory,
        log_store,
        sm_store,
    )
    .await
    .expect("create raft instance");

    let mut members = BTreeMap::new();
    members.insert(1, coord_server::raft::new_basic_node(&raft_addr));
    raft.initialize(members).await.expect("raft initialize");

    (Arc::new(raft), mvcc)
}

async fn put(
    raft: &Arc<coord_server::raft::CoordRaft>,
    key: &str,
    value: &str,
    lease: Option<i64>,
) {
    let cmd = Command::Put {
        key: key.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
        lease_id: lease,
    };
    let _ = raft.client_write(cmd).await.expect("client_write");
}

/// P0-A.3：导出 → 删光数据目录 → 导入恢复，元数据（version/create_revision/
/// mod_revision/lease_id/deleted）完整，且恢复后 revision 从 applied+1 续写。
#[tokio::test]
async fn test_snapshot_export_wipe_import_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();
    let tracker = Arc::new(SnapshotTracker::default());

    let (raft, mvcc) = start_node(&data_dir, Arc::clone(&tracker)).await;

    // 写入：k1 两次（version=2）、k2 带 lease、k3 写入后删除（tombstone）
    let _ = put(&raft, "/snap/k1", "v1", None).await;
    let _ = put(&raft, "/snap/k1", "v2", None).await;
    let _ = put(&raft, "/snap/k2", "v2", Some(7)).await;
    let _ = put(&raft, "/snap/k3", "v3", None).await;
    let del_resp = raft
        .client_write(Command::Delete {
            key: b"/snap/k3".to_vec(),
        })
        .await
        .expect("client_write delete");
    let last_revision = match del_resp.response() {
        Response::Delete { revision } => *revision,
        other => panic!("unexpected response: {other:?}"),
    };

    // 源端元数据基线
    let source = export_snapshot_data(&mvcc, last_revision, 1).expect("export source");
    assert_eq!(source.kv_pairs.len(), 3, "k1/k2/k3 均有数据");
    assert_eq!(source.kv_metadata.len(), 3);
    let mut source_meta = source.kv_metadata.clone();
    source_meta.sort_by(|a, b| a.key.cmp(&b.key));
    let k1_meta = &source_meta[0];
    assert_eq!((k1_meta.version, k1_meta.deleted), (2, false));
    let k3_meta = &source_meta[2];
    assert!(k3_meta.deleted, "删除必须保留 tombstone 元数据");

    // 序列化往返（CLI 文件载体）
    let bytes = source.to_bytes().expect("to_bytes");
    let source =
        coord_server::storage::snapshot::SnapshotData::from_bytes(&bytes).expect("from_bytes");

    // 停节点并删光数据（模拟灾难恢复）
    let _ = raft.shutdown().await;
    drop(raft);
    drop(mvcc);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    std::fs::remove_dir_all(&data_dir).expect("wipe data dir");
    std::fs::create_dir_all(&data_dir).expect("recreate data dir");

    // 恢复
    let storage_config = StorageConfig::default();
    let backend = RedbBackend::open(&data_dir, &storage_config).expect("open fresh backend");
    let mvcc2 = MvccStorage::new(backend).expect("create mvcc");
    import_snapshot_data(&mvcc2, &source).expect("import");

    // 恢复后导出一致：数据 + 元数据逐字段完整
    let restored = export_snapshot_data(&mvcc2, 0, 0).expect("export restored");
    let mut r_pairs = restored.kv_pairs.clone();
    let mut s_pairs = source.kv_pairs.clone();
    r_pairs.sort_by(|a, b| a.key.cmp(&b.key));
    s_pairs.sort_by(|a, b| a.key.cmp(&b.key));
    assert_eq!(s_pairs.len(), r_pairs.len());
    for (s, r) in s_pairs.iter().zip(r_pairs.iter()) {
        assert_eq!(s.key, r.key);
        assert_eq!(s.value, r.value);
    }

    let mut r_meta = restored.kv_metadata.clone();
    let mut s_meta = source.kv_metadata.clone();
    r_meta.sort_by(|a, b| a.key.cmp(&b.key));
    s_meta.sort_by(|a, b| a.key.cmp(&b.key));
    assert_eq!(s_meta.len(), r_meta.len());
    for (s, r) in s_meta.iter().zip(r_meta.iter()) {
        assert_eq!(s.key, r.key, "key set must match");
        assert_eq!(s.version, r.version, "version 必须完整");
        assert_eq!(
            s.create_revision, r.create_revision,
            "create_revision 必须完整"
        );
        assert_eq!(s.mod_revision, r.mod_revision, "mod_revision 必须完整");
        assert_eq!(s.lease_id, r.lease_id, "lease_id 必须完整");
        assert_eq!(s.deleted, r.deleted, "tombstone 标记必须完整");
    }

    // 数据可读：k1 现值、k3 已删
    assert_eq!(mvcc2.get(b"/snap/k1").unwrap(), Some(b"v2".to_vec()));
    assert_eq!(mvcc2.get(b"/snap/k3").unwrap(), None);

    // revision 从 applied+1 续写（D-A2/D-A4）
    assert_eq!(mvcc2.current_revision(), last_revision);
    let next_rev = mvcc2
        .put(b"/snap/after", b"x", None)
        .expect("put after import");
    assert_eq!(next_rev, last_revision + 1);
}
