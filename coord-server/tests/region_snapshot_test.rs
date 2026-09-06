// per-Region 快照导出/恢复验收测试
//
// 单节点装配 region 1 raft（目录隔离 MVCC）+ CoordNode（region_manager 装配）。
// 验证：
//   - Maintenance/snapshot RPC 带 region_id=1 → 导出 Region 1 MVCC 的 SnapshotData
//     （首块带该 Region 自己的 applied index/term）；region_id=0 → 导出节点级
//     storage（legacy 语义不变）；
//   - region_id 指向未装配 Region → NOT_FOUND；
//   - 导出 → import 到新目录级数据目录 → 数据一致（离线 per-Region 恢复路径）。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use coord_core::storage::StorageBackend;
use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionId, RegionMeta, StorageConfig};
use coord_proto::maintenance::maintenance_server::Maintenance;
use coord_proto::maintenance::SnapshotRequest;
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcService};
use coord_server::raft::region::RegionManager;
use coord_server::raft::region_runtime::{region_data_dir, RegionRuntimeSpec};
use coord_server::raft::type_config::Command;
use coord_server::raft::{new_raft, RaftConfig, WatchReceiver};
use coord_server::server::CoordNode;
use coord_server::storage::mvcc::MvccStorage;
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::snapshot::SnapshotData;

fn find_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct Host {
    node: Arc<CoordNode>,
    manager: Arc<RegionManager>,
    base: std::path::PathBuf,
    _dir: tempfile::TempDir,
}

async fn start_single_node_single_region() -> Host {
    let tmpdir = tempfile::tempdir().unwrap();
    let base = tmpdir.path().to_path_buf();

    let factory = RaftNetworkFactoryImpl::new(1);
    let raft_addr = format!("127.0.0.1:{}", find_port());
    factory.register_node(1, raft_addr.clone());

    let manager = Arc::new(RegionManager::new(1));
    let rpc = RaftRpcService::new();
    let spec = RegionRuntimeSpec {
        meta: RegionMeta {
            region_id: 1,
            start_key: vec![],
            end_key: vec![],
            epoch: RegionEpoch::initial(),
            peers: vec![Peer {
                node_id: 1,
                raft_addr: raft_addr.clone(),
                role: PeerRole::Voter,
            }],
            approximate_size: 0,
            approximate_keys: 0,
        },
        data_dir: region_data_dir(&base, 1),
        raft_config: Arc::new(RaftConfig {
            heartbeat_interval: 200,
            election_timeout_min: 800,
            election_timeout_max: 1500,
            ..Default::default()
        }),
    };
    manager
        .spawn_region(&factory, &rpc, spec, true)
        .await
        .expect("spawn region");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(rt) = manager.runtime(1) {
            let m = rt.raft.metrics();
            let m = m.borrow_watched();
            if m.last_quorum_acked.is_some() && m.current_leader == Some(1) {
                break;
            }
        }
        assert!(tokio::time::Instant::now() < deadline, "region leader");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // CoordNode：scratch 节点级 storage（region 模式快照走 manager 路由）
    let scratch = base.join("scratch-node");
    std::fs::create_dir_all(&scratch).unwrap();
    let backend = RedbBackend::open(&scratch, &StorageConfig::default()).unwrap();
    let scratch_mvcc = Arc::new(MvccStorage::new(backend).unwrap());
    let mut node = CoordNode::new(scratch_mvcc);
    node.node_id = 1;
    node.region_manager = Some(Arc::clone(&manager));

    Host {
        node: Arc::new(node),
        manager,
        base,
        _dir: tmpdir,
    }
}

/// 驱动 Maintenance snapshot handler，收集完整字节。
async fn collect_snapshot(
    node: &CoordNode,
    region_id: u64,
) -> Result<Vec<u8>, tonic::Status> {
    use tokio_stream::StreamExt as _;
    let resp = node
        .snapshot(tonic::Request::new(SnapshotRequest { region_id }))
        .await?;
    let mut stream = resp.into_inner();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| {
            tonic::Status::internal(format!("snapshot stream: {e}"))
        })?;
        bytes.extend_from_slice(&chunk.data);
    }
    Ok(bytes)
}

#[tokio::test]
async fn test_region_snapshot_export_roundtrip() {
    let host = start_single_node_single_region().await;
    let rt = host.manager.runtime(1).expect("runtime");

    // 写入 3 个 key + 1 个删除（tombstone 也在快照内）
    for (i, k) in [b"r1/a", b"r1/b", b"r1/c"].iter().enumerate() {
        rt.raft
            .client_write(Command::Put {
                key: k.to_vec(),
                value: format!("v{i}").into_bytes(),
                lease_id: None,
            })
            .await
            .expect("put");
    }
    rt.raft
        .client_write(Command::Delete {
            key: b"r1/a".to_vec(),
        })
        .await
        .expect("delete");

    // region_id=1：导出 Region MVCC 快照
    let bytes = collect_snapshot(&host.node, 1).await.expect("region snapshot");
    let snap = SnapshotData::from_bytes(&bytes).expect("parse snapshot");
    assert!(!snap.kv_pairs.is_empty(), "region snapshot must contain keys");
    // 首块 applied index 由 handler 内从 Region MVCC 读取（export_snapshot_data 传参）
    assert!(snap.last_included_index > 0, "region snapshot applied index");

    // region_id=0：legacy 节点级 storage（scratch，无 key）
    let bytes0 = collect_snapshot(&host.node, 0).await.expect("region0 snapshot");
    let snap0 = SnapshotData::from_bytes(&bytes0).expect("parse region0 snapshot");
    assert!(snap0.kv_pairs.is_empty(), "scratch node storage has no user kv");

    // 未装配 Region → NOT_FOUND
    let err = collect_snapshot(&host.node, 99).await.expect_err("region 99 absent");
    assert_eq!(err.code(), tonic::Code::NotFound, "absent region: {err}");

    // 恢复路径：import 到新的 Region 数据目录 → 逐 key 校验一致
    let restore_dir = host.base.join("regions").join("restore-test");
    std::fs::create_dir_all(&restore_dir).unwrap();
    let backend = RedbBackend::open(&restore_dir, &StorageConfig::default()).unwrap();
    let mvcc2 = Arc::new(MvccStorage::new(backend).unwrap());
    coord_server::storage::snapshot::import_snapshot_data(&mvcc2, &snap).expect("import");

    // r1/a 已删除、r1/b、r1/c 完好
    assert_eq!(mvcc2.get(b"r1/a").unwrap(), None, "tombstone in snapshot");
    assert_eq!(mvcc2.get(b"r1/b").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(mvcc2.get(b"r1/c").unwrap(), Some(b"v2".to_vec()));
    assert_eq!(
        mvcc2.current_revision(),
        snap.last_included_index,
        "restored revision equals snapshot applied index"
    );
}
