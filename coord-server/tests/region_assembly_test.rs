// Region 运行时装配验收测试（Phase 2 T2.3：RegionManager 生产接线）
//
// 验证：
// - `RegionManager::spawn_region` 一步装配 Region 的存储（MvccStorage/LogStore/
//   SnapshotTracker）+ per-region Raft + 网络注册（RaftRpcService.set_region_raft）
//   + 路由（regions/key_index）与运行时注册。
// - 存储按目录隔离（`region_data_dir`：region≥1 →
//   `<data_dir>/regions/region-{region_id:016x}/`）；仅装配 region 1/2 时节点根
//   目录不产生 store.db / raft-log（region 0 legacy 布局不受影响）。
// - 2 节点 × 2 Region 共享单一连接池与单一 raft gRPC server：各自独立选举、
//   独立写入、互不串扰；`RegionManager.route`/`route_runtime` 按 key range
//   路由到正确 Region 运行时。

use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Duration;

use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionId, RegionMeta};
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcServer, RaftRpcService};
use coord_server::raft::region::RegionManager;
use coord_server::raft::region_runtime::region_data_dir;
use coord_server::raft::type_config::{Command, Response};
use coord_server::raft::{RegionRuntime, RegionRuntimeSpec, RaftConfig, WatchReceiver};

fn find_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// 构造 Region 元数据（voter peers = 全部节点，raft_addr 取自共享地址表）
fn region_meta(
    region_id: RegionId,
    start: &[u8],
    end: &[u8],
    raft_addrs: &BTreeMap<u64, String>,
) -> RegionMeta {
    RegionMeta {
        region_id,
        start_key: start.to_vec(),
        end_key: end.to_vec(),
        epoch: RegionEpoch::initial(),
        peers: raft_addrs
            .iter()
            .map(|(node_id, raft_addr)| Peer {
                node_id: *node_id,
                raft_addr: raft_addr.clone(),
                role: PeerRole::Voter,
            })
            .collect(),
        approximate_size: 0,
        approximate_keys: 0,
    }
}

struct NodeHost {
    node_id: u64,
    manager: Arc<RegionManager>,
    // 保持临时数据目录存活（Region 数据在其下）；仅生命周期用途
    _dir: tempfile::TempDir,
    _factory: RaftNetworkFactoryImpl,
    _raft_handle: tokio::task::JoinHandle<()>,
}

/// 启动 2 节点 × 2 Region 集群。
///
/// region 1 覆盖 ["", "m")，region 2 覆盖 ["m", ∞)。每节点经 RegionManager
/// 装配两个 Region；node 1 是 bootstrap（initialize=true），node 2 靠复制追赶。
async fn start_two_node_two_region_cluster() -> Vec<NodeHost> {
    let raft_addrs: BTreeMap<u64, String> = (1..=2)
        .map(|id| (id, format!("127.0.0.1:{}", find_port())))
        .collect();
    let regions: [(RegionId, &[u8], &[u8]); 2] = [(1, b"", b"m"), (2, b"m", b"")];

    let mut hosts = Vec::new();
    for node_id in 1..=2u64 {
        let tmpdir = tempfile::tempdir().unwrap();
        let base = tmpdir.path().to_path_buf();

        let factory = RaftNetworkFactoryImpl::new(node_id);
        for (id, addr) in &raft_addrs {
            factory.register_node(*id, addr.clone());
        }
        let rpc = RaftRpcService::new();
        let manager = Arc::new(RegionManager::new(node_id));

        for (region_id, start, end) in regions {
            let spec = RegionRuntimeSpec {
                meta: region_meta(region_id, start, end, &raft_addrs),
                data_dir: region_data_dir(&base, region_id),
                raft_config: Arc::new(RaftConfig {
                    heartbeat_interval: 200,
                    election_timeout_min: 800,
                    election_timeout_max: 1500,
                    ..Default::default()
                }),
            };
            manager
                .spawn_region(&factory, &rpc, spec, node_id == 1)
                .await
                .expect("spawn region");
        }

        // 存储隔离断言：region≥1 数据落在 regions/region-{id} 子目录；
        // 未装配 region 0 → 节点根目录无 legacy store.db / raft-log。
        assert!(
            !base.join("store.db").exists(),
            "region 0 legacy store must not be created when only regions >= 1 spawn"
        );
        assert!(
            !base.join("raft-log").exists(),
            "region 0 legacy raft-log must not be created when only regions >= 1 spawn"
        );
        for region_id in [1u64, 2] {
            let rdir = region_data_dir(&base, region_id);
            assert!(
                rdir.join("store.db").exists(),
                "region {region_id} store.db under region dir"
            );
            assert!(
                rdir.join("raft-log/log.db").exists(),
                "region {region_id} raft log under region dir"
            );
        }

        // 每节点一个 raft gRPC server，承载全部 Region（按 region_id 解复用）
        let raft_addr: SocketAddr = raft_addrs[&node_id].parse().expect("parse raft addr");
        let raft_svc = RaftRpcServer::new(rpc);
        let raft_handle = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(raft_svc)
                .serve(raft_addr)
                .await;
        });

        hosts.push(NodeHost {
            node_id,
            manager,
            _dir: tmpdir,
            _factory: factory,
            _raft_handle: raft_handle,
        });
    }
    hosts
}

async fn wait_leader_for_region(
    hosts: &[NodeHost],
    region_id: RegionId,
    timeout: Duration,
) -> Option<(usize, u64)> {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        for (host_idx, host) in hosts.iter().enumerate() {
            if let Some(rt) = host.manager.runtime(region_id) {
                let m = rt.raft.metrics();
                let m = m.borrow_watched();
                if m.last_quorum_acked.is_some() && m.current_leader == Some(host.node_id) {
                    return Some((host_idx, host.node_id));
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}

async fn propose_put(rt: &RegionRuntime, key: &[u8], value: &[u8]) -> u64 {
    let resp = rt
        .raft
        .client_write(Command::Put {
            key: key.to_vec(),
            value: value.to_vec(),
            lease_id: None,
        })
        .await
        .expect("client_write put");
    match resp.response() {
        Response::Put { revision } => *revision,
        other => panic!("unexpected response: {other:?}"),
    }
}

/// T2.3 核心验收：RegionManager 装配的两个 Region 独立选举 + 独立写入，
/// key range 路由（route / route_runtime）落到正确 Region。
#[tokio::test]
async fn test_spawn_regions_independent_election_and_routing() {
    let hosts = start_two_node_two_region_cluster().await;

    for region_id in [1u64, 2] {
        let (host_idx, node_id) =
            wait_leader_for_region(&hosts, region_id, Duration::from_secs(25))
                .await
                .expect("leader");
        let host = &hosts[host_idx];

        // key range 路由：region1=["","m")、region2=["m",∞)
        let key: &[u8] = if region_id == 1 { b"apple" } else { b"peach" };
        let handle = host.manager.route(key).expect("route key");
        assert_eq!(
            handle.region_id(),
            region_id,
            "route(key) should hit region {region_id}"
        );
        let rt = host
            .manager
            .route_runtime(key)
            .expect("route_runtime key");
        assert_eq!(rt.region_id(), region_id, "route_runtime region");

        // 经该 Region 的 Raft 独立写入
        let rev = propose_put(&rt, key, b"v").await;
        assert!(rev >= 1, "region {region_id} write revision");
        eprintln!("region {region_id}: leader=node{node_id}, rev={rev}");
    }

    // 边界 key 路由
    let host = &hosts[0];
    assert_eq!(host.manager.route(b"").unwrap().region_id(), 1);
    assert_eq!(host.manager.route(b"\xff").unwrap().region_id(), 2);
    assert_eq!(host.manager.region_count(), 2);
}

/// 独立性 + 收敛：写入经复制扩散到两节点的对应 Region 存储；
/// 各 Region MVCC 只见自己的 key（目录隔离 + raft 不串扰）。
#[tokio::test]
async fn test_spawn_regions_isolated_and_converged() {
    let hosts = start_two_node_two_region_cluster().await;

    // 在各自 leader 上写入
    for region_id in [1u64, 2] {
        let (host_idx, _) =
            wait_leader_for_region(&hosts, region_id, Duration::from_secs(25))
                .await
                .expect("leader");
        let rt = hosts[host_idx]
            .manager
            .runtime(region_id)
            .expect("runtime");
        let key: &[u8] = if region_id == 1 { b"apple" } else { b"peach" };
        propose_put(&rt, key, b"v").await;
    }

    // 收敛 + 隔离：每节点每 Region 都能读到自己的 key，且读不到对方 region 的 key
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let mut all_converged = true;
        for host in &hosts {
            for region_id in [1u64, 2] {
                let rt = host.manager.runtime(region_id).expect("runtime");
                let own_key: &[u8] = if region_id == 1 { b"apple" } else { b"peach" };
                let other_key: &[u8] = if region_id == 1 { b"peach" } else { b"apple" };
                if rt.mvcc.get(own_key).unwrap() != Some(b"v".to_vec()) {
                    all_converged = false;
                    continue;
                }
                assert_eq!(
                    rt.mvcc.get(other_key).unwrap(),
                    None,
                    "node {} region {region_id} must not see other region's key",
                    host.node_id
                );
            }
        }
        if all_converged {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "regions did not converge within timeout"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
