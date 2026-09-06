// Operator 执行器验收测试（TDD 迭代 #11）
//
// M3 验收「PD 能完成一次 add-peer 与 transfer-leader」的组件级到真实 raft 的
// 证明：3 节点 × 1 Region 真实 raft（gRPC 网络），Region 成员从 {n1,n2} 起步、
// n3 以空节点（非成员）就绪，经 `OperatorExecutor`（PD 组件）驱动真实
// `CoordRegionRaftHandle`：
//   - AddPeer{region, n3}：add_learner(blocking 追平) → 晋升 Voter → PD
//     meta_store peers 同步（conf_ver 递增、写穿落盘）；
//   - TransferLeader → n3：等待真实 leader 切换，n3 成为 leader 后继续提交；
//   - RemovePeer{n2}：成员收缩 {n1,n3}，meta 同步，后续对 n2 的转移被拒。
//
// 一致性边界（接线层）：执行器运行在 Region leader 所在节点（真实部署
// 中每节点内嵌 PD；operator 跨节点去重/转移依赖 region 0 system raft 复制 PD
// 命令——本迭代不覆盖）。本测试把 PD 挂在 leader 节点上驱动。

use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Duration;

use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionMeta};
use coord_server::pd::meta_store::PdMetaStore;
use coord_server::pd::{Operator, OperatorExecutor, PdConfig, PlacementDriver};
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcServer, RaftRpcService};
use coord_server::raft::region::RegionManager;
use coord_server::raft::region_runtime::region_data_dir;
use coord_server::raft::type_config::{Command, Response};
use coord_server::raft::{
    CoordRegionRaftHandle, RaftConfig, RegionRaftHandle, RegionRuntimeSpec, WatchReceiver,
};
use tokio::sync::watch;

fn find_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

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
    /// 本节点 raft gRPC 监听地址（真实地址，供 AddPeer 目标等使用）
    raft_addr: String,
    manager: Arc<RegionManager>,
    // 保持临时数据目录/连接池/raft gRPC server 存活
    _dir: tempfile::TempDir,
    _factory: RaftNetworkFactoryImpl,
    _raft_handle: tokio::task::JoinHandle<()>,
}

/// 启动 3 节点 × 1 Region 集群（region 覆盖整个 keyspace）。
///
/// 成员关系：n1 bootstrap（initialize=true，成员 {n1,n2}）；n2 为成员但靠
/// 复制追赶（initialize=false）；**n3 空节点**（非成员、initialize=false）——
/// 模拟加入路径（与 coord 单 Raft join 中"新节点未初始化"状态一致），供
/// AddPeer 把其拉入成员。
async fn start_three_node_one_region_cluster() -> Vec<NodeHost> {
    let raft_addrs: BTreeMap<u64, String> = (1..=3)
        .map(|id| (id, format!("127.0.0.1:{}", find_port())))
        .collect();
    let members: Vec<Peer> = [1u64, 2]
        .iter()
        .map(|id| Peer {
            node_id: *id,
            raft_addr: raft_addrs[id].clone(),
            role: PeerRole::Voter,
        })
        .collect();

    let mut hosts = Vec::new();
    for node_id in 1..=3u64 {
        let tmpdir = tempfile::tempdir().unwrap();
        let base = tmpdir.path().to_path_buf();

        let factory = RaftNetworkFactoryImpl::new(node_id);
        for (id, addr) in &raft_addrs {
            factory.register_node(*id, addr.clone());
        }
        let rpc = RaftRpcService::new();
        let manager = Arc::new(RegionManager::new(node_id));

        let meta = RegionMeta {
            region_id: 1,
            start_key: vec![],
            end_key: vec![],
            epoch: RegionEpoch::initial(),
            peers: members.clone(),
            approximate_size: 0,
            approximate_keys: 0,
        };
        let spec = RegionRuntimeSpec {
            meta,
            data_dir: region_data_dir(&base, 1),
            raft_config: raft_test_config(),
        };
        // 仅 n1 bootstrap initialize（成员 {n1,n2}）；n2/n3 靠复制/加入
        manager
            .spawn_region(&factory, &rpc, spec, node_id == 1)
            .await
            .expect("spawn region");

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
            raft_addr: raft_addrs[&node_id].clone(),
            manager,
            _dir: tmpdir,
            _factory: factory,
            _raft_handle: raft_handle,
        });
    }
    hosts
}

/// 等待某节点成为 Region 1 leader（含 last_quorum_acked，可接受 client_write）
async fn wait_leader(hosts: &[NodeHost], node_id: u64, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        let rt = hosts
            .iter()
            .find(|h| h.node_id == node_id)
            .unwrap()
            .manager
            .runtime(1)
            .unwrap();
        let m = rt.raft.metrics();
        let m = m.borrow_watched();
        if m.current_leader == Some(node_id) && m.last_quorum_acked.is_some() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

/// 找出 Region 1 当前 leader 的 host 下标（在成员节点中轮询）
async fn find_leader_idx(hosts: &[NodeHost], timeout: Duration) -> usize {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        for (idx, host) in hosts.iter().enumerate() {
            if let Some(rt) = host.manager.runtime(1) {
                let m = rt.raft.metrics();
                let m = m.borrow_watched();
                if m.last_quorum_acked.is_some() && m.current_leader == Some(host.node_id) {
                    return idx;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("no region 1 leader after {timeout:?}");
}

/// 等待某节点的 mvcc 中 key == expect（None = 不存在）
async fn wait_mvcc(
    hosts: &[NodeHost],
    node_id: u64,
    key: &[u8],
    expect: Option<Vec<u8>>,
    timeout: Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let rt = hosts
            .iter()
            .find(|h| h.node_id == node_id)
            .unwrap()
            .manager
            .runtime(1)
            .unwrap();
        if rt.mvcc.get(key).unwrap() == expect {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "node {node_id} mvcc {key:?} did not converge to {expect:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// 经 Region raft client_write 写入（leader 就绪 + acked 后）
async fn propose_put(hosts: &[NodeHost], node_id: u64, key: &[u8], value: &[u8]) -> u64 {
    assert!(
        wait_leader(hosts, node_id, Duration::from_secs(20)).await,
        "node {node_id} never became leader with acked lease"
    );
    let rt = hosts
        .iter()
        .find(|h| h.node_id == node_id)
        .unwrap()
        .manager
        .runtime(1)
        .unwrap();
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

/// 在 leader 节点上构造 PD + 执行器（meta_store 含 region 1 成员 {n1,n2}）
fn make_pd_on(hosts: &[NodeHost], leader_idx: usize) -> (Arc<PlacementDriver>, OperatorExecutor) {
    let dir = hosts[leader_idx]._dir.path().join("pd-test");
    let meta_store = Arc::new(PdMetaStore::open(&dir).expect("open pd meta store"));

    // 成员 {n1,n2}：raft_addr 字段仅作元数据记录（执行器对 raft 的交互经真实
    // RegionRaftHandle；地址一致性是 接线层把 handle/addr 对齐的职责）
    let meta = RegionMeta {
        region_id: 1,
        start_key: vec![],
        end_key: vec![],
        epoch: RegionEpoch::initial(),
        peers: vec![
            Peer {
                node_id: 1,
                raft_addr: "node1:raft".into(),
                role: PeerRole::Voter,
            },
            Peer {
                node_id: 2,
                raft_addr: "node2:raft".into(),
                role: PeerRole::Voter,
            },
        ],
        approximate_size: 0,
        approximate_keys: 0,
    };
    meta_store.create_region(meta).unwrap();

    let config = PdConfig::default();
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let pd = Arc::new(PlacementDriver::new(config, meta_store, shutdown_rx, hosts[leader_idx].node_id));
    let ex = OperatorExecutor::new(Arc::clone(&pd), hosts[leader_idx].node_id);
    (pd, ex)
}

fn region_raft_handle(hosts: &[NodeHost], node_id: u64) -> Arc<dyn RegionRaftHandle> {
    let rt = hosts
        .iter()
        .find(|h| h.node_id == node_id)
        .unwrap()
        .manager
        .runtime(1)
        .expect("region 1 runtime");
    Arc::new(CoordRegionRaftHandle::new(rt.raft.clone()))
}

fn peers_of_region1(pd: &PlacementDriver) -> Vec<Peer> {
    pd.meta_store().get_region(1).expect("region 1").peers
}

fn conf_ver(pd: &PlacementDriver) -> u64 {
    pd.meta_store().get_region(1).unwrap().epoch.conf_ver
}

fn has_voter(peers: &[Peer], node_id: u64) -> bool {
    peers
        .iter()
        .any(|p| p.node_id == node_id && p.role == PeerRole::Voter)
}

/// M3 验收：PD 完成一次真实 add-peer、transfer-leader 与 remove-peer。
#[tokio::test]
async fn test_pd_executor_real_raft_add_transfer_remove_peer() {
    let hosts = start_three_node_one_region_cluster().await;

    // ── 初始：成员 {n1,n2} 选出 leader，n3 非成员 ──
    let leader_idx = find_leader_idx(&hosts, Duration::from_secs(25)).await;
    let leader_id = hosts[leader_idx].node_id;
    let non_leader_member_id = if leader_id == 1 { 2 } else { 1 };
    eprintln!("initial leader = node{leader_id} (members {{1,2}}; node3 empty)");

    // 写 v1 并经成员复制收敛（n3 尚非成员，不应有数据）
    propose_put(&hosts, leader_id, b"apple", b"v1").await;
    wait_mvcc(
        &hosts,
        1,
        b"apple",
        Some(b"v1".to_vec()),
        Duration::from_secs(20),
    )
    .await;
    wait_mvcc(
        &hosts,
        2,
        b"apple",
        Some(b"v1".to_vec()),
        Duration::from_secs(20),
    )
    .await;
    wait_mvcc(&hosts, 3, b"apple", None, Duration::from_secs(10)).await;

    // ── PD + 执行器挂在 leader 节点 ──
    let (pd, ex) = make_pd_on(&hosts, leader_idx);
    let handle = region_raft_handle(&hosts, leader_id);

    // ── AddPeer node3 → 晋升 Voter + meta 同步 ──
    // node3 是空节点（非成员）；raft_addr 用其真实 raft gRPC 地址
    let raft_addr_3 = hosts[2].raft_addr.clone();
    ex.execute(
        &Operator::AddPeer {
            region_id: 1,
            node_id: 3,
            raft_addr: raft_addr_3,
        },
        Arc::clone(&handle),
    )
    .await
    .expect("add-peer node3 should succeed on region leader");

    let peers = peers_of_region1(&pd);
    assert!(has_voter(&peers, 3), "node3 must become voter: {peers:?}");
    assert!(has_voter(&peers, 1) && has_voter(&peers, 2));
    assert_eq!(conf_ver(&pd), 2);
    eprintln!("add-peer: meta peers={peers:?}");

    // node3 复制追平（add_learner blocking 已等；此处兜底断言 apply）
    wait_mvcc(
        &hosts,
        3,
        b"apple",
        Some(b"v1".to_vec()),
        Duration::from_secs(20),
    )
    .await;

    // ── TransferLeader → node3（node3 是 voter）────
    ex.execute(
        &Operator::TransferLeader {
            region_id: 1,
            to_node: 3,
        },
        Arc::clone(&handle),
    )
    .await
    .expect("transfer-leader to node3 should succeed");
    assert!(
        wait_leader(&hosts, 3, Duration::from_secs(20)).await,
        "node3 should become leader after transfer"
    );
    eprintln!("transfer-leader: node3 is now leader");

    // node3 leader 后继续提交 → 三成员收敛
    propose_put(&hosts, 3, b"peach", b"v2").await;
    wait_mvcc(
        &hosts,
        1,
        b"peach",
        Some(b"v2".to_vec()),
        Duration::from_secs(20),
    )
    .await;
    wait_mvcc(
        &hosts,
        2,
        b"peach",
        Some(b"v2".to_vec()),
        Duration::from_secs(20),
    )
    .await;
    wait_mvcc(
        &hosts,
        3,
        b"peach",
        Some(b"v2".to_vec()),
        Duration::from_secs(20),
    )
    .await;

    // ── RemovePeer node2（执行器 node_id=3=当前 leader）──
    let ex3 = OperatorExecutor::new(Arc::clone(&pd), 3);
    let handle3 = region_raft_handle(&hosts, 3);
    ex3.execute(
        &Operator::RemovePeer {
            region_id: 1,
            node_id: non_leader_member_id,
        },
        handle3,
    )
    .await
    .expect("remove-peer should succeed on region leader");

    let peers = peers_of_region1(&pd);
    assert!(!has_voter(&peers, non_leader_member_id), "peers: {peers:?}");
    assert_eq!(peers.len(), 2);
    assert_eq!(conf_ver(&pd), 3);
    eprintln!("remove-peer node{non_leader_member_id}: meta peers={peers:?}");

    // 收缩后 {1,3} quorum 继续写：node3 提交 cherry 收敛到 node1+node3。
    // （被移除 voter 在 openraft 中降级为 Learner——仍接收复制但不参与投票，
    // 因此不能断言"其 mvcc 无新数据"；移除的证明 = 成员变更 + 后续 quorum 写
    // 不再需要它，见下）
    propose_put(&hosts, 3, b"cherry", b"v3").await;
    wait_mvcc(
        &hosts,
        3,
        b"cherry",
        Some(b"v3".to_vec()),
        Duration::from_secs(20),
    )
    .await;
    wait_mvcc(
        &hosts,
        1,
        b"cherry",
        Some(b"v3".to_vec()),
        Duration::from_secs(20),
    )
    .await;

    // raft 成员收敛：node3 视角的 voter 集合 == {1,3}（node2 不再是 voter）
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let rt3 = hosts
            .iter()
            .find(|h| h.node_id == 3)
            .unwrap()
            .manager
            .runtime(1)
            .unwrap();
        let m = rt3.raft.metrics();
        let m = m.borrow_watched();
        let voters: Vec<u64> = m.membership_config.voter_ids().collect();
        if voters.len() == 2 && voters.contains(&1) && voters.contains(&3) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "region voter membership did not converge to {{1,3}}: {voters:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // ── 向已移除成员转移 leader → 拒绝（meta 守卫）──
    let err = ex3
        .execute(
            &Operator::TransferLeader {
                region_id: 1,
                to_node: non_leader_member_id,
            },
            region_raft_handle(&hosts, 3),
        )
        .await
        .expect_err("transfer to removed node must be rejected");
    assert!(err.contains("not a voter"), "err: {err}");
}

/// 执行器在非 leader 节点不执行（Leader 守卫在真实 raft 上成立）。
#[tokio::test]
async fn test_pd_executor_skips_when_not_region_leader() {
    let hosts = start_three_node_one_region_cluster().await;
    let leader_idx = find_leader_idx(&hosts, Duration::from_secs(25)).await;
    let leader_id = hosts[leader_idx].node_id;
    let follower_id = if leader_id == 1 { 2 } else { 1 };

    // 把 PD 挂在 follower 节点（executor.node_id = follower），但 handle 指向
    // 该 follower 自己的 region raft（它不是 leader）
    let (pd, _ex) = make_pd_on(&hosts, leader_idx);
    // 构造 follower 上的 executor
    let ex_f = OperatorExecutor::new(Arc::clone(&pd), follower_id);
    let handle_f = region_raft_handle(&hosts, follower_id);

    let err = ex_f
        .execute(
            &Operator::AddPeer {
                region_id: 1,
                node_id: 3,
                raft_addr: "node3:raft".into(),
            },
            handle_f,
        )
        .await
        .expect_err("follower must not execute membership change");
    assert!(err.contains("not leader"), "err: {err}");
}
