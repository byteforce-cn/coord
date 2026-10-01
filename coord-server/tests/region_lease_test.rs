// Lease per-Region 验收测试
//
// 单节点装配：region 0（CoordNode.node.raft，全局租约表）+ region 1/2（独立 raft
// 与 MVCC，目录隔离），region 0 状态机挂 lease_revoke_tx 广播，CoordNode 启动
// expiry/reconciler/region-revoker 三个 worker。验证：
//   - region 模式 Put/Txn 允许带 lease_id（guard 已移除）；
//   - 全局租约记录（`/_lease/{id}`）落 region 0 MVCC，绑定 Key 落所属 Region MVCC
//     （KvMetadata.lease_id）；
//   - 显式 revoke：region 0 LeaseOp::Revoke apply → 广播 → 各 Region leader 经
//     Command::DeleteKeysByLease 删除绑定 Key（含 Watch 事件路径）；
//   - 过期清理：TTL 到期后同样经广播 → per-Region 删除，Key 从 Region MVCC 消失，
//     租约记录消失。
//
// 单节点 raft 语义：self-only 成员（quorum=1），无 raft RPC server 亦可 client_write
// （与 restart_recovery_test 同模式）。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use coord_core::storage::StorageBackend;
use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionId, RegionMeta, StorageConfig};
use coord_proto::kv::kv_server::Kv;
use coord_proto::kv::PutRequest;
use coord_server::lease::wall_clock_now_ms;
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcService};
use coord_server::raft::region::RegionManager;
use coord_server::raft::region_runtime::region_data_dir;
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::type_config::{Command, LeaseOp, Response};
use coord_server::raft::{new_basic_node, new_raft, RaftConfig, RegionRuntimeSpec, WatchReceiver};
use coord_server::server::CoordNode;
use coord_server::storage::mvcc::MvccStorage;
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::snapshot::SnapshotTracker;
use coord_server::timer::TimerWheel;

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
    lease_manager: Arc<coord_server::lease::LeaseManager>,
    mvcc0: Arc<MvccStorage<RedbBackend>>,
    _dir: tempfile::TempDir,
}

async fn wait_region_leader(host: &Host, region_id: RegionId, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if let Some(rt) = host.manager.runtime(region_id) {
            let m = rt.raft.metrics();
            let m = m.borrow_watched();
            if m.last_quorum_acked.is_some() && m.current_leader == Some(1) {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("region {region_id} leader not elected");
}

/// 装配单节点：region 0 raft（CoordNode）+ region 1/2 raft，全部 worker 启动。
async fn start_single_node_with_regions() -> Host {
    let tmpdir = tempfile::tempdir().unwrap();
    let base = tmpdir.path().to_path_buf();

    // ── region 0：CoordNode 的 raft（全局租约表 `/_lease/`）──
    let storage_config = StorageConfig::default();
    let backend = RedbBackend::open(&base, &storage_config).expect("open region0 backend");
    let mvcc0 = Arc::new(MvccStorage::new(backend).expect("create region0 mvcc"));
    let tracker0 = Arc::new(SnapshotTracker::default());
    let log_store0 = LogStore::new(&base)
        .await
        .expect("region0 log store")
        .with_snapshot_tracker(Arc::clone(&tracker0));
    let mut sm_store0 = StateMachineStore::new(
        Arc::clone(&mvcc0),
        base.join("snapshots"),
        Arc::clone(&tracker0),
    );
    // region 0 状态机挂 Lease Revoke 广播
    let (lease_revoke_tx, lease_revoke_rx) = tokio::sync::mpsc::unbounded_channel::<i64>();
    sm_store0.set_lease_revoke_tx(lease_revoke_tx);

    let factory0 = RaftNetworkFactoryImpl::new(1);
    let raft0_addr = format!("127.0.0.1:{}", find_port());
    factory0.register_node(1, raft0_addr.clone());
    let raft0 = new_raft(
        1,
        Arc::new(RaftConfig::default()),
        factory0,
        log_store0,
        sm_store0,
    )
    .await
    .expect("create region0 raft");
    let mut members0 = BTreeMap::new();
    members0.insert(1, new_basic_node(&raft0_addr));
    raft0
        .initialize(members0)
        .await
        .expect("initialize region0 raft");
    let raft0 = Arc::new(raft0);

    // ── region 1/2：RegionManager 装配（共享网络 factory，与 region0 raft 独立）──
    let shared_factory = RaftNetworkFactoryImpl::new(1);
    shared_factory.register_node(1, raft0_addr.clone());
    let raft_addrs: BTreeMap<u64, String> = BTreeMap::from([(1, raft0_addr)]);
    let manager = Arc::new(RegionManager::new(1));
    let rpc = RaftRpcService::new();
    let region_table: [(u64, &[u8], &[u8]); 2] = [(1, b"", b"m"), (2, b"m", b"")];
    for (region_id, start, end) in region_table {
        let spec = RegionRuntimeSpec {
            meta: RegionMeta {
                region_id,
                start_key: start.to_vec(),
                end_key: end.to_vec(),
                epoch: RegionEpoch::initial(),
                peers: vec![Peer {
                    node_id: 1,
                    raft_addr: raft_addrs[&1].clone(),
                    role: PeerRole::Voter,
                }],
                approximate_size: 0,
                approximate_keys: 0,
            },
            data_dir: region_data_dir(&base, region_id),
            raft_config: Arc::new(RaftConfig {
                heartbeat_interval: 200,
                election_timeout_min: 800,
                election_timeout_max: 1500,
                ..Default::default()
            }),
            object_store: None,
        };
        manager
            .spawn_region(&shared_factory, &rpc, spec, true)
            .await
            .expect("spawn region");
    }

    // ── CoordNode ──
    let lease_manager = Arc::new(coord_server::lease::LeaseManager::new(TimerWheel::start()));
    let mut node = CoordNode::new(Arc::clone(&mvcc0));
    node.node_id = 1;
    node.raft = Some(Arc::clone(&raft0));
    node.region_manager = Some(Arc::clone(&manager));
    node.lease_manager = Some(Arc::clone(&lease_manager));
    let node = Arc::new(node);

    node.start_lease_expiry_worker();
    node.start_lease_leader_reconciler();
    node.start_region_lease_revoker(lease_revoke_rx);

    let host = Host {
        node,
        manager,
        lease_manager,
        mvcc0,
        _dir: tmpdir,
    };
    wait_region_leader(&host, 1, Duration::from_secs(15)).await;
    wait_region_leader(&host, 2, Duration::from_secs(15)).await;
    // region 0 leader 就绪
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let m = raft0.metrics();
        let m = m.borrow_watched();
        if m.current_leader == Some(1) && m.last_quorum_acked.is_some() {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "region0 leader");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    host
}

/// 经 LeaseManager + region0 raft 授出租约（镜像 handler 流程），返回 lease id。
async fn grant_lease(host: &Host, ttl: i64) -> i64 {
    let id = host
        .lease_manager
        .grant_with_id(ttl, 0)
        .await
        .expect("grant_with_id");
    let op = LeaseOp::Grant {
        id,
        ttl,
        deadline_wall_ms: wall_clock_now_ms() + ttl * 1000,
    };
    let resp = host
        .node
        .raft
        .as_ref()
        .unwrap()
        .client_write(Command::Lease(op))
        .await
        .expect("grant via raft");
    assert!(matches!(resp.response(), Response::Lease { .. }));
    id
}

/// 验收 1：region 模式 Put 带 lease 绑定 + 显式 revoke 全链路清理。
#[tokio::test]
async fn test_region_mode_put_with_lease_and_revoke_cleans_keys() {
    let host = start_single_node_with_regions().await;

    // 授出租约（TTL 足够长，本用例走显式 revoke）
    let lease_id = grant_lease(&host, 300).await;

    // region 1（apple）与 region 2（peach）各绑一个 key（经 CoordNode put handler
    // —— 路由 + guard 移除后的 region 模式 lease 绑定路径）
    host.node
        .put(tonic::Request::new(PutRequest {
            key: b"apple".to_vec(),
            value: b"v1".to_vec(),
            lease_id,
            prev_kv: false,
            request_id: vec![],
        }))
        .await
        .expect("region-mode put with lease (region1)");
    host.node
        .put(tonic::Request::new(PutRequest {
            key: b"peach".to_vec(),
            value: b"v2".to_vec(),
            lease_id,
            prev_kv: false,
            request_id: vec![],
        }))
        .await
        .expect("region-mode put with lease (region2)");

    // 租约记录在 region 0 MVCC；绑定 Key 落各自 Region MVCC
    assert!(
        host.mvcc0.get_lease_record(lease_id).unwrap().is_some(),
        "lease record must live in region0 storage"
    );
    assert_eq!(
        host.manager.runtime(1).unwrap().mvcc.get(b"apple").unwrap(),
        Some(b"v1".to_vec())
    );
    assert_eq!(
        host.manager.runtime(2).unwrap().mvcc.get(b"peach").unwrap(),
        Some(b"v2".to_vec())
    );
    // 绑定元数据携带 lease_id（权威绑定）
    assert_eq!(
        host.manager
            .runtime(1)
            .unwrap()
            .mvcc
            .get_kv_metadata(b"apple")
            .unwrap()
            .unwrap()
            .lease_id,
        lease_id
    );

    // 显式 revoke（region 0 raft 语义同 handler）
    let op = LeaseOp::Revoke {
        id: lease_id,
        delete_keys: true,
    };
    host.node
        .raft
        .as_ref()
        .unwrap()
        .client_write(Command::Lease(op))
        .await
        .expect("revoke via raft");
    let _ = host.lease_manager.revoke(lease_id).await;

    // 广播 → region revoker → 两个 Region 的绑定 Key 都被删除（轮询等待）
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let r1_gone = host
            .manager
            .runtime(1)
            .unwrap()
            .mvcc
            .get(b"apple")
            .unwrap()
            .is_none();
        let r2_gone = host
            .manager
            .runtime(2)
            .unwrap()
            .mvcc
            .get(b"peach")
            .unwrap()
            .is_none();
        let rec_gone = host.mvcc0.get_lease_record(lease_id).unwrap().is_none();
        if r1_gone && r2_gone && rec_gone {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "region lease revoke cleanup did not converge (r1_gone={r1_gone} r2_gone={r2_gone} rec_gone={rec_gone})"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// 验收 2：租约过期（TTL 到期）→ 广播 → per-Region 删除绑定 Key。
#[tokio::test]
async fn test_region_mode_lease_expiry_cleans_keys() {
    let host = start_single_node_with_regions().await;

    // 1s TTL：到期由 expiry worker（leader）propose Revoke → 广播 → per-Region 清理
    let lease_id = grant_lease(&host, 1).await;
    host.node
        .put(tonic::Request::new(PutRequest {
            key: b"apple".to_vec(),
            value: b"v1".to_vec(),
            lease_id,
            prev_kv: false,
            request_id: vec![],
        }))
        .await
        .expect("region-mode put with lease (region1)");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let key_gone = host
            .manager
            .runtime(1)
            .unwrap()
            .mvcc
            .get(b"apple")
            .unwrap()
            .is_none();
        let rec_gone = host.mvcc0.get_lease_record(lease_id).unwrap().is_none();
        if key_gone && rec_gone {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "lease expiry cleanup did not converge (key_gone={key_gone} rec_gone={rec_gone})"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// 验收 3：Watch 可见 per-Region Lease 删除事件（DeleteKeysByLease 走状态机
/// apply → per-Region dispatcher 分发）。
#[tokio::test]
async fn test_region_lease_cleanup_emits_watch_events() {
    let host = start_single_node_with_regions().await;

    // 直接订阅 region1 dispatcher（与 gRPC watch 路由同源）
    let rt1 = host.manager.runtime(1).expect("region1 runtime");
    let dispatcher = Arc::clone(&rt1.watch_dispatcher);
    let (watch_id, mut event_rx) = dispatcher
        .subscribe(
            coord_server::watch::WatchRequest {
                key: b"apple".to_vec(),
                range_end: vec![],
                start_revision: 0,
            },
            256,
            rt1.mvcc.current_revision(),
        )
        .expect("subscribe region1 watch");

    let lease_id = grant_lease(&host, 300).await;
    host.node
        .put(tonic::Request::new(PutRequest {
            key: b"apple".to_vec(),
            value: b"v1".to_vec(),
            lease_id,
            prev_kv: false,
            request_id: vec![],
        }))
        .await
        .expect("put apple with lease (region1)");

    // revoke → region1 DeleteKeysByLease → apple 删除事件
    let op = LeaseOp::Revoke {
        id: lease_id,
        delete_keys: true,
    };
    host.node
        .raft
        .as_ref()
        .unwrap()
        .client_write(Command::Lease(op))
        .await
        .expect("revoke via raft");
    let _ = host.lease_manager.revoke(lease_id).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        match tokio::time::timeout(Duration::from_secs(15), event_rx.recv()).await {
            Ok(Some(ev)) => {
                let deleted_apple = ev.events.iter().any(|item| {
                    item.kvs
                        .iter()
                        .any(|kv| kv.key == b"apple" && kv.value.is_none())
                });
                if deleted_apple {
                    break;
                }
            }
            Ok(None) => panic!("watch channel closed"),
            Err(_) => panic!("watch event timeout"),
        }
        assert!(tokio::time::Instant::now() < deadline, "watch delete event");
    }
    dispatcher.unsubscribe(watch_id);
}
