//! KV-T G1（#141）单 Region v2 KV 生产路径：
//! handler → per-Region raft（`PutV2`/`DeleteV2`/`DeleteRangeV2`）→ CommitTs MVCC。
//!
//! 直接调用 `CoordNode` 的 v2 trait 方法（不经过网络），验证 Region 路由、
//! leader 写路径、`CommitRejected` 与快照读在真实 raft apply 后成立。

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use coord_core::error_code::error_code_of;
use coord_core::storage::StorageBackend;
use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionMeta, StorageConfig};
use coord_proto::kv::v2::kv_server::Kv as KvV2;
use coord_proto::kv::v2::{DeleteRequest, PutRequest, RangeRequest};
use coord_server::kvt::{KvTCapability, KvTRuntimeConfig, KvTState};
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcServer, RaftRpcService};
use coord_server::raft::region::RegionManager;
use coord_server::raft::region_runtime::{region_data_dir, RegionRuntimeSpec};
use coord_server::raft::{RaftConfig, WatchReceiver};
use coord_server::server::CoordNode;
use coord_server::storage::mvcc::{MvccStorage, RevisionMode};
use coord_server::storage::redb_backend::RedbBackend;

fn find_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn start_single_region_node() -> (tempfile::TempDir, Arc<CoordNode>, Arc<RegionManager>) {
    let tmpdir = tempfile::tempdir().unwrap();
    let base = tmpdir.path().to_path_buf();
    let raft_addr = format!("127.0.0.1:{}", find_port());

    let factory = RaftNetworkFactoryImpl::new(1);
    factory.register_node(1, raft_addr.clone());
    let rpc = RaftRpcService::new();
    let manager = Arc::new(RegionManager::new(1));
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
            heartbeat_interval: 50,
            election_timeout_min: 200,
            election_timeout_max: 400,
            ..Default::default()
        }),
        object_store: None,
        revision_mode: RevisionMode::CommitTs,
    };
    manager
        .spawn_region(&factory, &rpc, spec, true)
        .await
        .expect("spawn region 1");

    // 每节点一个 raft gRPC server（真实网络用于单节点选举/提交）。
    let raft_addr: SocketAddr = raft_addr.parse().unwrap();
    let raft_svc = RaftRpcServer::new(rpc);
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(raft_svc)
            .serve(raft_addr)
            .await;
    });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(rt) = manager.runtime(1) {
            let metrics = rt.raft.metrics();
            let metrics = metrics.borrow_watched();
            if metrics.last_quorum_acked.is_some() && metrics.current_leader == Some(1) {
                break;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "region 1 must elect a leader"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // CoordNode 的 scratch storage：region 模式下 KV 全量走 manager 路由。
    let backend = RedbBackend::open(&base, &StorageConfig::default()).unwrap();
    let scratch = Arc::new(MvccStorage::new(backend).unwrap());
    let mut node = CoordNode::new(scratch);
    node.node_id = 1;
    node.region_manager = Some(Arc::clone(&manager));
    node.set_kvt_state(
        KvTState::new(true, KvTRuntimeConfig::default()).with_implemented([KvTCapability::Kv]),
    );
    let node = Arc::new(node);
    (tmpdir, node, manager)
}

fn put(key: &[u8], value: &[u8], commit_ts: i64) -> PutRequest {
    PutRequest {
        key: key.to_vec(),
        value: value.to_vec(),
        commit_ts,
        ..Default::default()
    }
}

fn range(key: &[u8], read_ts: i64) -> RangeRequest {
    RangeRequest {
        key: key.to_vec(),
        read_ts,
        ..Default::default()
    }
}

#[tokio::test]
async fn test_single_region_v2_kv_over_raft() {
    let (_tmpdir, node, manager) = start_single_region_node().await;

    let resp = node
        .put(tonic::Request::new(put(b"k", b"v1", 1_000)))
        .await
        .expect("put 1000 over raft")
        .into_inner();
    assert_eq!(resp.commit_ts, 1_000);
    assert_eq!(manager.runtime(1).unwrap().mvcc.current_revision(), 1_000);

    let resp = node
        .range(tonic::Request::new(range(b"k", 0)))
        .await
        .expect("range read-your-writes")
        .into_inner();
    assert_eq!(resp.read_ts, 1_000);
    assert_eq!(resp.kvs.len(), 1);
    assert_eq!(resp.kvs[0].value, b"v1");
    assert_eq!(resp.kvs[0].mod_revision, 1_000);

    // 回退 commit_ts：用户级拒绝，raft apply 不失败，数据不落盘。
    let err = node
        .put(tonic::Request::new(put(b"k2", b"v", 999)))
        .await
        .expect_err("stale commit_ts must be rejected");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition, "{err:?}");
    assert_eq!(
        error_code_of(&err).as_deref(),
        Some("COMMIT_REJECTED"),
        "{err:?}"
    );
    assert_eq!(manager.runtime(1).unwrap().mvcc.get(b"k2").unwrap(), None);

    // 删除后：最新读不可见，快照读仍看到删除前的值。
    let resp = node
        .delete(tonic::Request::new(DeleteRequest {
            key: b"k".to_vec(),
            commit_ts: 1_001,
            prev_kv: true,
            ..Default::default()
        }))
        .await
        .expect("delete over raft")
        .into_inner();
    assert_eq!(resp.deleted, 1);
    assert_eq!(resp.prev_kvs[0].value, b"v1");

    let latest = node
        .range(tonic::Request::new(range(b"k", 0)))
        .await
        .expect("latest range after delete")
        .into_inner();
    assert!(latest.kvs.is_empty());

    let snapshot = node
        .range(tonic::Request::new(range(b"k", 1_000)))
        .await
        .expect("snapshot read before delete")
        .into_inner();
    assert_eq!(snapshot.kvs.len(), 1);
    assert_eq!(snapshot.kvs[0].value, b"v1");

    // 拒绝之后仍可继续写（applied 已推进，客户端没有被卡住）。
    let resp = node
        .put(tonic::Request::new(put(b"k3", b"v3", 1_002)))
        .await
        .expect("post-reject write")
        .into_inner();
    assert_eq!(resp.commit_ts, 1_002);
}
