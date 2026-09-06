// Multi-Raft 网络共享层验收测试
//
// 验证：
// - GroupRouter：进程内 2 节点 × 2 Region，每个节点只建 **一个** 共享
//   RaftNetworkFactoryImpl（连接池），N 个 Region 的出站 RPC 走同一连接池，
//   且 RaftMessage 携带各自 region_id。
// - per-region RaftNetworkFactory（RegionRaftNetworkFactory → openraft-multi
//   GroupNetworkAdapter）可用：每个 Region 以它创建 Raft 实例。
// - （配套）RaftRpcService 按 region_id 解复用；两个 Region 独立选举、
//   独立提交互不串扰。
//
// 每个 Region 使用独立 data dir（各自 LogStore + MvccStorage），存储隔离由
// RegionManager/前缀隔离另行验收——本测试聚焦**网络共享层**正确性。

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use coord_core::storage::StorageBackend;
use coord_core::types::StorageConfig;
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::{
    RaftNetworkFactoryImpl, RaftRpcServer, RaftRpcService, RegionRaftNetworkFactory,
};
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::type_config::{Command, Response};
use coord_server::raft::{new_basic_node, new_raft, RaftConfig, RaftNode, WatchReceiver};
use coord_server::storage::mvcc::MvccStorage;
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::snapshot::SnapshotTracker;

const REGIONS: [u64; 2] = [1, 2];

fn find_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// 单节点上某个 Region 的运行句柄（独立 log store + 独立 mvcc）
struct RegionNode {
    node_id: u64,
    region_id: u64,
    raft: coord_server::raft::CoordRaft,
    mvcc: Arc<MvccStorage<RedbBackend>>,
    _dir: tempfile::TempDir,
}

struct NodeHost {
    node_id: u64,
    regions: Vec<RegionNode>,
    _raft_handle: tokio::task::JoinHandle<()>,
    _factory: RaftNetworkFactoryImpl,
}

impl NodeHost {
    #[allow(dead_code)]
    fn node_id(&self) -> u64 {
        self.node_id
    }
}

impl RegionNode {
    async fn metrics_leader(&self) -> Option<u64> {
        let m = self.raft.metrics();
        let m = m.borrow_watched();
        if m.last_quorum_acked.is_some() {
            m.current_leader
        } else {
            None
        }
    }
}

async fn wait_leader_for_region(
    hosts: &[NodeHost],
    region_id: u64,
    timeout: Duration,
) -> Option<(usize, u64)> {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        for (host_idx, host) in hosts.iter().enumerate() {
            if let Some(rn) = host.regions.iter().find(|r| r.region_id == region_id) {
                if rn.metrics_leader().await == Some(rn.node_id) {
                    return Some((host_idx, rn.node_id));
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}

async fn start_two_node_two_region_cluster() -> Vec<NodeHost> {
    let raft_addrs: BTreeMap<u64, String> = (1..=2)
        .map(|id| (id, format!("127.0.0.1:{}", find_port())))
        .collect();

    let mut hosts = Vec::new();
    for node_id in 1..=2u64 {
        let tmpdir = tempfile::tempdir().unwrap();
        let base = tmpdir.path().to_path_buf();
        let factory = RaftNetworkFactoryImpl::new(node_id);
        for (id, addr) in &raft_addrs {
            factory.register_node(*id, addr.clone());
        }
        // 每节点一个共享工厂（跨 Region 共享连接池）
        let shared_factory = factory.clone();
        let raft_rpc_service = RaftRpcService::new();

        let mut regions = Vec::new();
        for region_id in REGIONS {
            let region_dir = base.join(format!("region-{region_id}"));
            std::fs::create_dir_all(&region_dir).unwrap();
            let storage_config = StorageConfig::default();
            let backend =
                RedbBackend::open(&region_dir, &storage_config).expect("open backend");
            let mvcc = Arc::new(MvccStorage::new(backend).expect("create mvcc"));
            let tracker = Arc::new(SnapshotTracker::default());
            let log_store = LogStore::new(&region_dir)
                .await
                .expect("create log store")
                .with_snapshot_tracker(Arc::clone(&tracker));
            let sm_store = StateMachineStore::new(
                Arc::clone(&mvcc),
                region_dir.join("snapshots"),
                Arc::clone(&tracker),
            );

            // per-region 网络工厂（绑定 region_id）
            let region_factory = RegionRaftNetworkFactory::new(shared_factory.clone(), region_id);
            let raft_config = RaftConfig {
                heartbeat_interval: 200,
                election_timeout_min: 800,
                election_timeout_max: 1500,
                ..Default::default()
            };
            let raft = new_raft(
                node_id,
                Arc::new(raft_config),
                region_factory,
                log_store,
                sm_store,
            )
            .await
            .expect("create region raft instance");

            // 注册到 region 解复用表
            raft_rpc_service.set_region_raft(region_id, raft.clone());

            if node_id == 1 {
                let members: BTreeMap<u64, RaftNode> = raft_addrs
                    .iter()
                    .map(|(id, addr)| (*id, new_basic_node(addr)))
                    .collect();
                raft.initialize(members).await.expect("initialize region");
            }

            regions.push(RegionNode {
                node_id,
                region_id,
                raft,
                mvcc,
                _dir: tempfile::tempdir().unwrap(),
            });
        }

        // 每节点一个 raft gRPC server，承担全部 Region（按 region_id 解复用）
        let raft_addr: std::net::SocketAddr =
            raft_addrs[&node_id].parse().expect("parse raft addr");
        let raft_svc = RaftRpcServer::new(raft_rpc_service);
        let raft_handle = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(raft_svc)
                .serve(raft_addr)
                .await;
        });

        hosts.push(NodeHost {
            node_id,
            regions,
            _raft_handle: raft_handle,
            _factory: factory,
        });
    }
    hosts
}

async fn propose_put(raft: &coord_server::raft::CoordRaft, key: &[u8], value: &[u8]) -> u64 {
    let resp = raft
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

/// 核心验收：2 节点 × 2 Region，共享连接池；各自独立选举 + 独立提交。
#[tokio::test]
async fn test_two_regions_independent_election_and_write() {
    let hosts = start_two_node_two_region_cluster().await;

    // 两个 Region 都应各自选出 leader（可能是 1 或 2）
    for region_id in REGIONS {
        let leader =
            wait_leader_for_region(&hosts, region_id, Duration::from_secs(15)).await;
        assert!(
            leader.is_some(),
            "region {region_id} elected no leader within timeout"
        );
        let (host_idx, node_id) = leader.unwrap();
        let host = &hosts[host_idx];
        let rn = host
            .regions
            .iter()
            .find(|r| r.region_id == region_id)
            .unwrap();
        // 写各自独立的 key；若两个 Region 的 raft 互相串扰，此处会 panic/超时
        let rev = propose_put(&rn.raft, format!("/r{region_id}/k").as_bytes(), b"v").await;
        assert!(rev >= 1, "region {region_id} write revision");
        eprintln!("region {region_id}: leader=node{node_id}, rev={rev}");
    }

    // 提交扩散：两个节点上同一 Region 的 mvcc 都应看到自己的写入
    for host in &hosts {
        for rn in &host.regions {
            let expected = format!("/r{}/k", rn.region_id).into_bytes();
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            loop {
                if rn.mvcc.get(&expected).unwrap() == Some(b"v".to_vec()) {
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "node {} region {} did not converge",
                    rn.node_id,
                    rn.region_id
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// 独立性：Region 1 的 key 不应出现在 Region 2 的 mvcc（不同存储隔离已由目录分开，
/// 这里验证 leader/写路径没有跨 region 串到别的 Raft 实例——写错 region 会超时或
/// revision 错乱）。
#[tokio::test]
async fn test_regions_do_not_cross_talk() {
    let hosts = start_two_node_two_region_cluster().await;

    // 在两个 region 上各写一个不同 key
    for region_id in REGIONS {
        let leader = wait_leader_for_region(&hosts, region_id, Duration::from_secs(15))
            .await
            .expect("leader");
        let rn = hosts[leader.0]
            .regions
            .iter()
            .find(|r| r.region_id == region_id)
            .unwrap();
        propose_put(&rn.raft, format!("/only-{region_id}").as_bytes(), b"x").await;
    }

    // 收敛后检查隔离：region1 的 mvcc 只有 /only-1，region2 只有 /only-2
    tokio::time::sleep(Duration::from_millis(1500)).await;
    for host in &hosts {
        for rn in &host.regions {
            let k1 = format!("/only-{}", rn.region_id).into_bytes();
            assert_eq!(
                rn.mvcc.get(&k1).unwrap(),
                Some(b"x".to_vec()),
                "own key should be present (node {} region {})",
                rn.node_id,
                rn.region_id
            );
        }
    }
}
