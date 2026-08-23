// P1-08 验收测试（L2 进程内）：Maintenance/Snapshot 流式导出 + 备份恢复
//
// 覆盖决策文档 P1-08：
// - `Maintenance/Snapshot` 流式实现（分块 1MiB，首块携带 last_included_index/term）
// - 拉取后校验（版本 + bincode）→ 导入全新存储 → KV 完整恢复
//
// 对应文档：`docs/production/15-milestone-task-breakdown.md` P1-08。

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::sync::Arc;

use coord_core::storage::StorageBackend;
use coord_core::types::StorageConfig;
use coord_proto::maintenance::maintenance_client::MaintenanceClient;
use coord_proto::maintenance::maintenance_server::MaintenanceServer;
use coord_proto::maintenance::SnapshotRequest;
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::RaftNetworkFactoryImpl;
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::type_config::{Command, Response};
use coord_server::raft::{new_basic_node, new_raft, RaftConfig};
use coord_server::server::CoordNode;
use coord_server::storage::mvcc::MvccStorage;
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::snapshot::{
    export_snapshot_data, import_snapshot_data, SnapshotData, SnapshotTracker,
};

fn find_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// 启动单节点 raft + 客户端 gRPC（Maintenance 服务）。
async fn start_node() -> (
    String,
    Arc<coord_server::raft::CoordRaft>,
    Arc<MvccStorage<RedbBackend>>,
    tempfile::TempDir,
    tokio::task::JoinHandle<()>,
) {
    let tmpdir = tempfile::tempdir().unwrap();
    let data_dir = tmpdir.path().to_path_buf();
    let grpc_addr = format!("127.0.0.1:{}", find_port());
    let raft_addr = format!("127.0.0.1:{}", find_port());

    let storage_config = StorageConfig::default();
    let backend = RedbBackend::open(&data_dir, &storage_config).expect("open backend");
    let mvcc = Arc::new(MvccStorage::new(backend).expect("create mvcc"));
    let tracker = Arc::new(SnapshotTracker::default());

    let log_store = LogStore::new(&data_dir)
        .await
        .expect("create log store")
        .with_snapshot_tracker(Arc::clone(&tracker));
    let sm_store = StateMachineStore::new(
        Arc::clone(&mvcc),
        data_dir.join("snapshots"),
        Arc::clone(&tracker),
    );

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
    let raft = Arc::new(raft);

    // 等待领导权
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline && raft.current_leader().await != Some(1) {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let mut node = CoordNode::new(Arc::clone(&mvcc));
    node.node_id = 1;
    node.raft = Some(Arc::clone(&raft));
    let node = Arc::new(node);

    let maint_svc = MaintenanceServer::from_arc(Arc::clone(&node));
    let grpc_addr_parse: std::net::SocketAddr = grpc_addr.parse().unwrap();
    let handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(maint_svc)
            .serve(grpc_addr_parse)
            .await;
    });

    // 等待 gRPC 就绪
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if tokio::net::TcpStream::connect(&grpc_addr).await.is_ok() {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "grpc never ready");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    (grpc_addr, raft, mvcc, tmpdir, handle)
}

/// P1-08-1：流式拉取 → 解析校验 → 导入全新存储 → KV 完整恢复。
#[tokio::test]
async fn test_snapshot_rpc_stream_and_restore() {
    use tokio_stream::StreamExt;

    let (grpc_addr, raft, _mvcc, _tmpdir, _handle) = start_node().await;

    // 写入 30 条（含带 lease 与删除 tombstone）
    for i in 0..30u32 {
        let key = format!("/snap/k{i:02}");
        let resp = raft
            .client_write(Command::Put {
                key: key.into_bytes(),
                value: format!("v{i}").into_bytes(),
                lease_id: if i % 5 == 0 { Some(100) } else { None },
            })
            .await
            .expect("put");
        assert!(matches!(resp.response(), Response::Put { .. }));
    }
    let del_resp = raft
        .client_write(Command::Delete {
            key: b"/snap/k00".to_vec(),
        })
        .await
        .expect("delete");
    assert!(matches!(del_resp.response(), Response::Delete { .. }));

    // 拉取流式快照
    let mut client = MaintenanceClient::new(
        tonic::transport::Endpoint::from_shared(format!("http://{grpc_addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap(),
    );
    let mut stream = client
        .snapshot(tonic::Request::new(SnapshotRequest {}))
        .await
        .expect("snapshot rpc")
        .into_inner();

    let mut bytes: Vec<u8> = Vec::new();
    let mut first_index: Option<i64> = None;
    let mut first_term: Option<u64> = None;
    let mut chunks = 0u64;
    while let Some(chunk) = stream.message().await.expect("stream message") {
        if chunks == 0 {
            first_index = Some(chunk.last_included_index);
            first_term = Some(chunk.last_included_term);
        }
        bytes.extend_from_slice(&chunk.data);
        chunks += 1;
    }
    assert!(chunks >= 1, "at least one chunk");
    assert!(
        first_index.unwrap_or(0) > 0,
        "first chunk carries last_included_index"
    );
    let _ = first_term;

    // 解析校验
    let snapshot_data = SnapshotData::from_bytes(&bytes).expect("valid snapshot bytes");

    // 导入全新存储并验证
    let restore_dir = tempfile::tempdir().unwrap();
    let backend2 = RedbBackend::open(restore_dir.path(), &StorageConfig::default()).unwrap();
    let mvcc2 = MvccStorage::new(backend2).unwrap();
    import_snapshot_data(&mvcc2, &snapshot_data).expect("import");

    for i in 1..30u32 {
        let key = format!("/snap/k{i:02}");
        assert_eq!(
            mvcc2.get(key.as_bytes()).unwrap(),
            Some(format!("v{i}").into_bytes()),
            "restored key {key}"
        );
    }
    // k00 已删除：tombstone 保留（get None 且元数据 deleted）
    assert_eq!(mvcc2.get(b"/snap/k00").unwrap(), None);
    let meta = mvcc2
        .get_kv_metadata(b"/snap/k00")
        .unwrap()
        .expect("tombstone meta");
    assert!(meta.deleted);
    // lease 绑定保留
    let meta5 = mvcc2.get_kv_metadata(b"/snap/k05").unwrap().unwrap();
    assert_eq!(meta5.lease_id, 100);
}

/// P1-08-2：导出与在线拉取产物一致性（同源 export_snapshot_data 对照）。
#[tokio::test]
async fn test_snapshot_rpc_matches_local_export() {
    use tokio_stream::StreamExt;

    let (grpc_addr, raft, mvcc, _tmpdir, _handle) = start_node().await;

    for i in 0..10u32 {
        let key = format!("/match/k{i}");
        let _ = raft
            .client_write(Command::Put {
                key: key.into_bytes(),
                value: format!("v{i}").into_bytes(),
                lease_id: None,
            })
            .await
            .expect("put");
    }

    // 本地导出对照
    let applied = mvcc.get_applied_log_id().unwrap().unwrap();
    let local = export_snapshot_data(&mvcc, applied.index, applied.term).unwrap();

    // 在线拉取
    let mut client = MaintenanceClient::new(
        tonic::transport::Endpoint::from_shared(format!("http://{grpc_addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap(),
    );
    let mut stream = client
        .snapshot(tonic::Request::new(SnapshotRequest {}))
        .await
        .unwrap()
        .into_inner();
    let mut bytes: Vec<u8> = Vec::new();
    while let Some(chunk) = stream.message().await.unwrap() {
        bytes.extend_from_slice(&chunk.data);
    }
    let remote = SnapshotData::from_bytes(&bytes).unwrap();

    // 同源一致：KV 对集合一致
    assert_eq!(local.kv_pairs.len(), remote.kv_pairs.len());
    let mut local_keys: Vec<Vec<u8>> = local.kv_pairs.iter().map(|p| p.key.clone()).collect();
    let mut remote_keys: Vec<Vec<u8>> = remote.kv_pairs.iter().map(|p| p.key.clone()).collect();
    local_keys.sort();
    remote_keys.sort();
    assert_eq!(local_keys, remote_keys);
}
