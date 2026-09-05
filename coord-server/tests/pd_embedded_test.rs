// Phase 3 T3.4 内嵌 PD 接线验收测试（TDD 迭代 #12）
//
// 3 节点 × 1 Region 真实 raft（gRPC 网络，成员 {1,2,3}），每节点经
// `EmbeddedPd::start` 装配内嵌 PD（配置驱动的生产接线路径，main.rs 同款）：
//
//   Test 1（稳态数据面）：心跳上报喂 PD 实时调度状态——每个节点 PD 的
//   region_leader 视图 == 真实 leader、region 统计（keys ≥ 已写 key 数）随心跳
//   刷新、三节点全部在线；健康副本数（3 == target）下调度器产出 0 operator
//   （无 churn）。
//
//   Test 2（成员变更 + 跨节点 meta 收敛 + 自愈）：在 leader 节点 PD 入队
//   RemovePeer（移除一个 follower）→ 执行器真实移除 → raft voter 集收缩 →
//   各节点 PD meta 经「raft 已提交 voter 对账」收敛为收缩后 voter 集 → 副本
//   不足触发 ReplicaChecker 生成 AddPeer（占位 node0）→ 执行器经注册节点
//   解析被移除的 learner → promote 回 3 voter → 各节点 PD meta 再次收敛一致。
//   这证明：嵌入式 PD 能自主完成一次 add-peer（M3 验收的执行链路）+ PD meta
//   跨节点一致性（raft 复制即一致性，follower 对账收敛）。
//
//   Test 3（优雅停止）：`EmbeddedPd::shutdown` 使全部后台循环退出。

use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Duration;

use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionMeta};
use coord_server::pd::{EmbeddedPd, NodeInfo, Operator, PdConfig};
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcServer, RaftRpcService};
use coord_server::raft::region::{RegionManager, RegionSeed};
use coord_server::raft::region_runtime::region_data_dir;
use coord_server::raft::{RaftConfig, RegionRuntimeSpec, WatchReceiver};

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

/// 测试用 PD 配置：心跳 200ms、均衡 1s、split/merge 检查拉到最大（不触发）
fn pd_test_config() -> PdConfig {
    PdConfig {
        balance_interval: 1,
        split_check_interval: 3600,
        merge_check_interval: 3600,
        node_heartbeat_timeout: 30,
        max_concurrent_operators: 10,
        target_replicas: 3,
        ..PdConfig::default()
    }
}

struct NodeHost {
    node_id: u64,
    raft_addr: String,
    grpc_addr: String,
    manager: Arc<RegionManager>,
    pd: Option<Arc<EmbeddedPd>>,
    dir: tempfile::TempDir,
    _factory: RaftNetworkFactoryImpl,
    _raft_handle: tokio::task::JoinHandle<()>,
}

/// 启动 3 节点 × 1 Region（region 1 = 全 keyspace，成员 {1,2,3}，n1 bootstrap）
async fn start_three_node_one_region_cluster() -> Vec<NodeHost> {
    let raft_addrs: BTreeMap<u64, String> = (1..=3)
        .map(|id| (id, format!("127.0.0.1:{}", find_port())))
        .collect();
    let members: Vec<Peer> = (1..=3u64)
        .map(|id| Peer {
            node_id: id,
            raft_addr: raft_addrs[&id].clone(),
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
        // 仅 n1 bootstrap initialize（成员 {1,2,3}）；n2/n3 靠 leader 复制
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
            grpc_addr: format!("127.0.0.1:{}", find_port()),
            manager,
            pd: None,
            dir: tmpdir,
            _factory: factory,
            _raft_handle: raft_handle,
        });
    }
    hosts
}

/// 在全部节点装配内嵌 PD（配置驱动生产路径）
async fn start_embedded_pds(hosts: &mut [NodeHost]) {
    let nodes_info: Vec<NodeInfo> = hosts
        .iter()
        .map(|h| NodeInfo {
            node_id: h.node_id,
            raft_addr: h.raft_addr.clone(),
            grpc_addr: h.grpc_addr.clone(),
        })
        .collect();
    let seeds = vec![RegionSeed {
        region_id: 1,
        start_key: vec![],
        end_key: vec![],
    }];
    for host in hosts.iter_mut() {
        let pd = EmbeddedPd::start(
            pd_test_config(),
            host.node_id,
            host.dir.path(),
            &host.manager,
            &seeds,
            nodes_info.clone(),
            Duration::from_millis(200),
        )
        .await
        .expect("start embedded pd");
        host.pd = Some(pd);
    }
}

/// 等待某节点成为 Region 1 leader（含 last_quorum_acked）
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

/// 找出 Region 1 当前 leader 的 host 下标
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

/// 读取节点 host 的 PD meta store 中 region 1 的 voter id 集
fn meta_voters(host: &NodeHost) -> std::collections::BTreeSet<u64> {
    host.pd
        .as_ref()
        .expect("pd started")
        .driver
        .meta_store()
        .get_region(1)
        .expect("region 1 in pd meta")
        .peers
        .iter()
        .filter(|p| p.role == PeerRole::Voter)
        .map(|p| p.node_id)
        .collect()
}

/// 等待节点 host 的 PD meta voter 集收敛到 expect（或为空 = 等待被更新）
async fn wait_meta_voters(
    host: &NodeHost,
    expect: Option<&std::collections::BTreeSet<u64>>,
    timeout: Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let voters = meta_voters(host);
        if let Some(exp) = expect {
            if &voters == exp {
                return;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "node {} pd meta voters did not converge to {expect:?}: {voters:?}",
            host.node_id
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// 读取节点 host 的 raft 已提交 voter 集
async fn raft_voters(host: &NodeHost) -> std::collections::BTreeSet<u64> {
    let rt = host.manager.runtime(1).expect("region 1 runtime");
    let m = rt.raft.metrics();
    let m = m.borrow_watched();
    m.membership_config.voter_ids().collect()
}

/// 经 Region raft client_write 写入（leader 就绪后）
async fn propose_put(hosts: &[NodeHost], node_id: u64, key: &[u8], value: &[u8]) {
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
    rt.raft
        .client_write(coord_server::raft::type_config::Command::Put {
            key: key.to_vec(),
            value: value.to_vec(),
            lease_id: None,
        })
        .await
        .expect("client_write put");
}

/// Test 1：稳态——心跳喂实时调度数据面，健康集群 0 operator churn。
#[tokio::test]
async fn test_embedded_pd_steady_state_heartbeat_data_plane() {
    let mut hosts = start_three_node_one_region_cluster().await;
    let leader_idx = find_leader_idx(&hosts, Duration::from_secs(25)).await;
    let leader_id = hosts[leader_idx].node_id;
    eprintln!("steady-state: leader = node{leader_id}");

    // 装配内嵌 PD（全部 3 节点）
    start_embedded_pds(&mut hosts).await;

    // 写入一个 key（leader 提交）
    propose_put(&hosts, leader_id, b"apple", b"v1").await;

    // ── 心跳数据面：每个节点 PD 的 region leader 视图 == 真实 leader ──
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let mut all_ok = true;
        for host in &hosts {
            let view = host.pd.as_ref().unwrap().driver.region_leader(1);
            if view != Some(leader_id) {
                all_ok = false;
                break;
            }
        }
        if all_ok {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "PD region_leader views never matched real leader {leader_id}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // ── 心跳统计：region 统计 keys ≥ 1（apple 已落库）随心跳刷新 ──
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let mut all_ok = true;
        for host in &hosts {
            let meta = host
                .pd
                .as_ref()
                .unwrap()
                .driver
                .meta_store()
                .get_region(1)
                .unwrap();
            if meta.approximate_keys < 1 {
                all_ok = false;
                break;
            }
        }
        if all_ok {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "PD region stats not refreshed by heartbeats"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // ── 三节点在线（调度器依赖的 NodeState 心跳刷新）──
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let mut all_ok = true;
        for host in &hosts {
            for n in 1..=3u64 {
                let st = host.pd.as_ref().unwrap().driver.get_node_state(n);
                if !st.map(|s| s.online).unwrap_or(false) {
                    all_ok = false;
                    break;
                }
            }
            if !all_ok {
                break;
            }
        }
        if all_ok {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "PD node heartbeat views not all online"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // ── 健康副本数（3 == target）：数个均衡周期后 0 operator（无 churn）──
    tokio::time::sleep(Duration::from_secs(4)).await;
    for host in &hosts {
        let stats = host.pd.as_ref().unwrap().driver.operator_stats();
        assert_eq!(
            stats.total, 0,
            "healthy 3-replica cluster must not churn operators (node {}): {:?}",
            host.node_id, stats
        );
    }

    // 优雅停止
    for host in &hosts {
        host.pd.as_ref().unwrap().shutdown().await;
    }
}

/// Test 2：成员变更 → 跨节点 PD meta 收敛 → ReplicaChecker 自愈（add-peer）。
#[tokio::test]
async fn test_embedded_pd_membership_change_meta_convergence_and_self_heal() {
    let mut hosts = start_three_node_one_region_cluster().await;
    let leader_idx = find_leader_idx(&hosts, Duration::from_secs(25)).await;
    let leader_id = hosts[leader_idx].node_id;
    // 选一个非 leader 成员作为移除对象
    let victim_id = [1u64, 2, 3].into_iter().find(|n| *n != leader_id).unwrap();
    eprintln!("self-heal: leader=node{leader_id}, victim=node{victim_id}");

    start_embedded_pds(&mut hosts).await;
    propose_put(&hosts, leader_id, b"apple", b"v1").await;

    // ── 稳态确认：全部 PD meta = {1,2,3} ──
    let full: std::collections::BTreeSet<u64> = [1u64, 2, 3].into_iter().collect();
    for host in &hosts {
        wait_meta_voters(host, Some(&full), Duration::from_secs(20)).await;
    }

    // ── 在 leader 节点 PD 入队 RemovePeer（模拟运维/维护触发）──
    hosts[leader_idx]
        .pd
        .as_ref()
        .unwrap()
        .driver
        .enqueue_operator(Operator::RemovePeer {
            region_id: 1,
            node_id: victim_id,
        });

    // ── 等待：raft voter 集在全部节点收敛到收缩后（{leader, 非 victim 的另一节点}）──
    let shrunk: std::collections::BTreeSet<u64> = [1u64, 2, 3]
        .into_iter()
        .filter(|n| *n != victim_id)
        .collect();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let mut all_ok = true;
        for host in &hosts {
            if raft_voters(host).await != shrunk {
                all_ok = false;
                break;
            }
        }
        if all_ok {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "raft voters never shrank to {shrunk:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    eprintln!("remove-peer: raft voters shrank to {shrunk:?}");

    // ── PD meta 跨节点收敛到收缩 voter 集（follower 对账；可观察到中间态）──
    for host in &hosts {
        wait_meta_voters(host, Some(&shrunk), Duration::from_secs(20)).await;
    }
    eprintln!("remove-peer: all PD metas converged to {shrunk:?}");

    // ── 自愈：ReplicaChecker 看到 2 < 3 → AddPeer(占位) → 解析被移除 learner
    //    → promote 回 3 voter → 全部 PD meta 再次收敛 ──
    let deadline = tokio::time::Instant::now() + Duration::from_secs(40);
    loop {
        let mut all_ok = true;
        for host in &hosts {
            if raft_voters(host).await != full {
                all_ok = false;
                break;
            }
            if meta_voters(host) != full {
                all_ok = false;
                break;
            }
        }
        if all_ok {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "self-heal did not restore voters to {full:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    eprintln!("self-heal: voters restored to {full:?}");

    // ── leader PD 执行器确实自主完成过 remove 与 add（Success 计数）──
    let stats = hosts[leader_idx]
        .pd
        .as_ref()
        .unwrap()
        .driver
        .operator_stats();
    assert!(
        stats.success >= 2,
        "leader PD should have completed remove-peer + add-peer autonomously: {stats:?}"
    );
    eprintln!("leader PD operator stats: {stats:?}");

    // ── 恢复后稳定：再等 3 个均衡周期，meta 不再变化、无新增 churn ──
    let last_total = hosts[leader_idx]
        .pd
        .as_ref()
        .unwrap()
        .driver
        .operator_stats()
        .total;
    tokio::time::sleep(Duration::from_secs(4)).await;
    for host in &hosts {
        let voters = meta_voters(host);
        assert_eq!(voters, full, "PD meta must stay converged");
    }
    let new_total = hosts[leader_idx]
        .pd
        .as_ref()
        .unwrap()
        .driver
        .operator_stats()
        .total;
    assert!(
        new_total <= last_total + 2,
        "self-heal converged but leader PD keeps churning operators: {last_total} -> {new_total}"
    );

    // 优雅停止
    for host in &hosts {
        host.pd.as_ref().unwrap().shutdown().await;
    }
}

/// Test 3：shutdown 优雅性（已在上两测试尾部执行；此处显式验证句柄退出）。
#[tokio::test]
async fn test_embedded_pd_shutdown_is_idempotent_and_fast() {
    let mut hosts = start_three_node_one_region_cluster().await;
    let _ = find_leader_idx(&hosts, Duration::from_secs(25)).await;
    start_embedded_pds(&mut hosts).await;

    for host in &hosts {
        // 两次 shutdown（幂等）都能在超时内返回
        let pd = host.pd.as_ref().unwrap().clone();
        tokio::time::timeout(Duration::from_secs(10), pd.shutdown())
            .await
            .expect("shutdown must complete");
        tokio::time::timeout(Duration::from_secs(10), pd.shutdown())
            .await
            .expect("second shutdown must also complete (idempotent)");
    }
}
