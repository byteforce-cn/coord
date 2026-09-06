// 配置驱动装配验收测试（TDD 迭代 #8）
//
// 与 region_assembly_test / region_kv_routing_test（手工逐 Region `spawn_region`）
// 不同，本测试驱动生产装配路径：`RegionSeed` + `spawn_configured_regions`——
// 即 `coord server` 在 `[multi_raft].enabled=true` 时（main.rs）调用的同一函数，
// 以 2 节点 × 2 Region 验证：
//   - 配置驱动的批量装配（每 Region peers = 全部集群成员、仅 bootstrap 节点
//     initialize、region ≥1 目录级存储隔离于 `<base>/regions/region-{id:016x}/`）；
//   - 装配后各 Region 独立选举，CoordNode 挂 region_manager 后 KV 按 key 路由、
//     收敛、互不串扰；
//   - 写非 leader 节点 → RegionNotLeader（UNAVAILABLE）+ `coord-leader-hint`；
//   - 非法 region 表（不平铺 / 节点非成员）被防御性拒绝。
//
// 说明：raft gRPC 为真实网络；KV handler 直接调用（与 region_kv_routing_test 相同）。

use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Duration;

use coord_core::storage::StorageBackend;
use coord_core::types::{Peer, PeerRole, RegionId, StorageConfig};
use coord_proto::kv::kv_server::Kv;
use coord_proto::kv::{PutRequest, RangeRequest};
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcServer, RaftRpcService};
use coord_server::raft::region::{spawn_configured_regions, RegionManager, RegionSeed};
use coord_server::raft::{RaftConfig, WatchReceiver};
use coord_server::server::CoordNode;
use coord_server::storage::mvcc::MvccStorage;
use coord_server::storage::redb_backend::RedbBackend;

const LEADER_HINT_KEY: &str = "coord-leader-hint";

fn find_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// 测试用 Raft 运行参数（宽松选举窗口，保证 2 节点测试稳定）。
fn raft_test_config() -> Arc<RaftConfig> {
    Arc::new(RaftConfig {
        heartbeat_interval: 200,
        election_timeout_min: 800,
        election_timeout_max: 1500,
        ..Default::default()
    })
}

struct NodeHost {
    node_id: u64,
    /// CoordNode（region_manager 已装配；KV handler 直接调用）
    node: Arc<CoordNode>,
    manager: Arc<RegionManager>,
    /// KV gRPC 地址（注册进各 node 的 node_grpc_addrs，供 leader hint）
    kv_grpc_addr: String,
    // 保持临时数据目录/连接池/raft gRPC server 存活
    _dir: tempfile::TempDir,
    _factory: RaftNetworkFactoryImpl,
    _raft_handle: tokio::task::JoinHandle<()>,
}

/// 经 `spawn_configured_regions`（生产装配路径）启动 2 节点 × 2 Region 集群。
///
/// region 1 覆盖 ["", "m")、region 2 覆盖 ["m", ∞)；node 1 是 bootstrap
/// （initialize=true），node 2 靠 leader 复制追赶。每个 Region 复制到全部节点。
async fn start_two_node_two_region_config_cluster() -> Vec<NodeHost> {
    let raft_addrs: BTreeMap<u64, String> = (1..=2)
        .map(|id| (id, format!("127.0.0.1:{}", find_port())))
        .collect();
    let kv_addrs: BTreeMap<u64, String> = (1..=2)
        .map(|id| (id, format!("127.0.0.1:{}", find_port())))
        .collect();
    let seeds = vec![
        RegionSeed {
            region_id: 1,
            start_key: b"".to_vec(),
            end_key: b"m".to_vec(),
        },
        RegionSeed {
            region_id: 2,
            start_key: b"m".to_vec(),
            end_key: vec![],
        },
    ];
    // peers = 全部集群成员（v1 静态复制：每个 Region 复制到同一批节点）
    let peers: Vec<Peer> = (1..=2)
        .map(|id| Peer {
            node_id: id,
            raft_addr: raft_addrs[&id].clone(),
            role: PeerRole::Voter,
        })
        .collect();

    let mut hosts = Vec::new();
    for node_id in 1..=2u64 {
        let tmpdir = tempfile::tempdir().unwrap();
        let base = tmpdir.path().to_path_buf();

        let factory = RaftNetworkFactoryImpl::new(node_id);
        for (id, addr) in &raft_addrs {
            factory.register_node(*id, addr.clone());
        }
        let rpc = RaftRpcService::new();

        let manager = spawn_configured_regions(
            node_id,
            &base,
            &factory,
            &rpc,
            raft_test_config(),
            None, // object storage disabled
            &seeds,
            &peers,
            node_id == 1, // 仅 bootstrap 节点 initialize 各 Region 成员
        )
        .await
        .expect("spawn configured regions");

        // CoordNode 的 node 级 legacy storage（region 模式下 KV 走 manager 路由，
        // 该 scratch 仅满足构造签名；region≥1 数据在 base/regions/ 下互不干扰）
        let scratch_dir = base.join("scratch");
        std::fs::create_dir_all(&scratch_dir).expect("create scratch dir");
        let storage_config = StorageConfig::default();
        let backend =
            RedbBackend::open(&scratch_dir, &storage_config).expect("open scratch backend");
        let scratch_mvcc = Arc::new(MvccStorage::new(backend).expect("create scratch mvcc"));
        let mut node = CoordNode::new(scratch_mvcc);
        node.node_id = node_id;
        node.region_manager = Some(Arc::clone(&manager));
        for (id, addr) in &kv_addrs {
            node.register_grpc_addr(*id, addr);
        }
        let node = Arc::new(node);

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
            node,
            manager,
            kv_grpc_addr: kv_addrs[&node_id].clone(),
            _dir: tmpdir,
            _factory: factory,
            _raft_handle: raft_handle,
        });
    }
    hosts
}

/// 等待某 Region 选出 leader（leader lease 就绪，可接受 client_write）。
async fn wait_leader_for_region(
    hosts: &[NodeHost],
    region_id: RegionId,
    timeout: Duration,
) -> Option<usize> {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        for (host_idx, host) in hosts.iter().enumerate() {
            if let Some(rt) = host.manager.runtime(region_id) {
                let m = rt.raft.metrics();
                let m = m.borrow_watched();
                if m.last_quorum_acked.is_some() && m.current_leader == Some(host.node_id) {
                    return Some(host_idx);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}

fn put_req(key: &[u8], value: &[u8]) -> PutRequest {
    PutRequest {
        key: key.to_vec(),
        value: value.to_vec(),
        lease_id: 0,
        prev_kv: false,
        request_id: vec![],
    }
}

fn range_req(key: &[u8], range_end: &[u8]) -> RangeRequest {
    RangeRequest {
        key: key.to_vec(),
        range_end: range_end.to_vec(),
        limit: 0,
        revision: 0,
        keys_only: false,
        count_only: false,
    }
}

/// 验收 1：配置驱动装配 → 各 Region 独立选举 → KV 路由收敛/隔离。
#[tokio::test]
async fn test_config_assembly_routes_and_converges_across_regions() {
    let hosts = start_two_node_two_region_config_cluster().await;

    let l1 = wait_leader_for_region(&hosts, 1, Duration::from_secs(25))
        .await
        .expect("region 1 leader");
    let l2 = wait_leader_for_region(&hosts, 2, Duration::from_secs(25))
        .await
        .expect("region 2 leader");
    eprintln!(
        "region1 leader=node{}, region2 leader=node{}",
        hosts[l1].node_id, hosts[l2].node_id
    );

    // ── 目录级存储隔离：region ≥1 数据目录 = <base>/regions/region-{id:016x} ──
    for host in &hosts {
        for rid in [1u64, 2] {
            let rt = host.manager.runtime(rid).expect("runtime");
            let name = format!("region-{rid:016x}");
            assert!(
                rt.data_dir.ends_with(&name),
                "region {rid} data dir must be isolated: {}",
                rt.data_dir.display()
            );
            assert!(rt.data_dir.is_dir(), "region {rid} data dir exists");
        }
    }

    // ── Put 路由：apple(region1) 走 l1、peach(region2) 走 l2 ──
    hosts[l1]
        .node
        .put(tonic::Request::new(put_req(b"apple", b"v1")))
        .await
        .expect("put apple via region1 leader");
    hosts[l2]
        .node
        .put(tonic::Request::new(put_req(b"peach", b"v2")))
        .await
        .expect("put peach via region2 leader");

    // ── 收敛 + 隔离：每节点每 Region 的 MVCC 只见本 Region 的 key ──
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let mut converged = true;
        for host in &hosts {
            for (rid, own, other) in [(1u64, b"apple", b"peach"), (2u64, b"peach", b"apple")] {
                let rt = host.manager.runtime(rid).expect("runtime");
                let expect = if rid == 1 {
                    b"v1".to_vec()
                } else {
                    b"v2".to_vec()
                };
                if rt.mvcc.get(own).unwrap() != Some(expect) {
                    converged = false;
                    continue;
                }
                assert_eq!(
                    rt.mvcc.get(other).unwrap(),
                    None,
                    "node {} region {rid} must not see other region's key",
                    host.node_id
                );
            }
        }
        if converged {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "regions did not converge"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // ── Range 单点读走各自 Region ──
    let r = hosts[l1]
        .node
        .range(tonic::Request::new(range_req(b"apple", b"")))
        .await
        .expect("range apple via region1 leader");
    assert_eq!(
        r.get_ref().kvs.iter().find(|kv| kv.key == b"apple").map(|kv| kv.value.as_slice()),
        Some(b"v1".as_slice())
    );
    let r = hosts[l2]
        .node
        .range(tonic::Request::new(range_req(b"peach", b"")))
        .await
        .expect("range peach via region2 leader");
    assert_eq!(
        r.get_ref().kvs.iter().find(|kv| kv.key == b"peach").map(|kv| kv.value.as_slice()),
        Some(b"v2".as_slice())
    );
}

/// 验收 2（经配置驱动装配路径）：写非 leader 节点 → RegionNotLeader
/// （UNAVAILABLE）+ leader hint 指向该 Region 真实 leader 的 KV 地址。
#[tokio::test]
async fn test_config_assembly_write_via_follower_returns_region_not_leader_hint() {
    let hosts = start_two_node_two_region_config_cluster().await;

    let l1 = wait_leader_for_region(&hosts, 1, Duration::from_secs(25))
        .await
        .expect("region 1 leader");
    let follower_idx = hosts
        .iter()
        .position(|h| h.node_id != hosts[l1].node_id)
        .expect("follower node");

    // follower 上写 region1 的 key：必须 UNAVAILABLE + hint = region1 leader 的 KV 地址。
    // （选举刚结束的窄窗口内 leader_id 可能尚为 None → 重试几次）
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let res = hosts[follower_idx]
            .node
            .put(tonic::Request::new(put_req(b"apple", b"x")))
            .await;
        match res {
            Ok(_) => panic!("write on non-leader must not succeed"),
            Err(status) => {
                assert_eq!(
                    status.code(),
                    tonic::Code::Unavailable,
                    "region not-leader write must be UNAVAILABLE: {status}"
                );
                let hint = status
                    .metadata()
                    .get(LEADER_HINT_KEY)
                    .and_then(|v| v.to_str().ok())
                    .expect("coord-leader-hint metadata present");
                if hint == hosts[l1].kv_grpc_addr {
                    return;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "leader hint never matches"
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

/// 防御性校验：不平铺 / 首 region 非空起点 / 节点非成员 → 装配前拒绝。
#[tokio::test]
async fn test_config_assembly_rejects_invalid_region_tables() {
    let tmp = tempfile::tempdir().unwrap();
    let factory = RaftNetworkFactoryImpl::new(1);
    let rpc = RaftRpcService::new();
    let peers = vec![Peer {
        node_id: 1,
        raft_addr: "127.0.0.1:1".to_string(),
        role: PeerRole::Voter,
    }];

    // ① 空洞：region1 结束于 "m"，region2 从 "n" 开始
    let gap = vec![
        RegionSeed {
            region_id: 1,
            start_key: b"".to_vec(),
            end_key: b"m".to_vec(),
        },
        RegionSeed {
            region_id: 2,
            start_key: b"n".to_vec(),
            end_key: vec![],
        },
    ];
    let err = match spawn_configured_regions(
        1,
        tmp.path(),
        &factory,
        &rpc,
        raft_test_config(),
        None,
        &gap,
        &peers,
        true,
    )
    .await
    {
        Ok(_) => panic!("gap between region ranges must be rejected"),
        Err(e) => e,
    };

    // ② 首 region 起点非空（keyspace 前缀无归属）
    let non_tiled = vec![RegionSeed {
        region_id: 1,
        start_key: b"a".to_vec(),
        end_key: vec![],
    }];
    match spawn_configured_regions(
        1,
        tmp.path(),
        &factory,
        &rpc,
        raft_test_config(),
        None,
        &non_tiled,
        &peers,
        true,
    )
    .await
    {
        Ok(_) => panic!("region table must start at empty start_key"),
        Err(_) => {}
    }

    // ③ 节点非 Region 成员（peers 不含 node_id）
    let tiled = vec![RegionSeed {
        region_id: 1,
        start_key: b"".to_vec(),
        end_key: vec![],
    }];
    let other_peers = vec![Peer {
        node_id: 2,
        raft_addr: "127.0.0.1:2".to_string(),
        role: PeerRole::Voter,
    }];
    match spawn_configured_regions(
        1,
        tmp.path(),
        &factory,
        &rpc,
        raft_test_config(),
        None,
        &tiled,
        &other_peers,
        true,
    )
    .await
    {
        Ok(_) => panic!("node not a region member must be rejected"),
        Err(_) => {}
    }

    // 校验失败发生在任何存储创建之前（数据目录未产生）
    assert!(
        !tmp.path().join("regions").exists(),
        "invalid region table must not create storage"
    );
    // gap 的错误信息包含 contiguity 提示
    assert!(err.to_string().contains("contiguous") || err.to_string().contains("tile"));
}
