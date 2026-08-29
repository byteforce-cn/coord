// P0-B 验收测试（L2 进程内多节点 raft）：Lease 全链路 raft 化
//
// 覆盖规格 B.7：
// - lease_revoke 走 raft，三节点 `/_lease/` 与 KV 状态一致
// - failover：新 leader 从状态机重建 Lease 表，到期 Lease 的绑定 key 被清理
// - follower 上 grant/keepalive/revoke 被拒绝并返回 leader 提示
//
// 对应文档：`docs/production/11-architecture-redesign.md` 规格 B；
// `docs/production/15-milestone-task-breakdown.md` P0-B.5。

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use coord_core::storage::StorageBackend;
use coord_core::types::StorageConfig;
use coord_proto::lease::lease_server::Lease as LeaseSvc;
use coord_proto::lease::{LeaseGrantRequest, LeaseRevokeRequest};
use coord_server::lease::wall_clock_now_ms;
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcServer, RaftRpcService};
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::type_config::{Command, LeaseOp, Response};
use coord_server::raft::{new_basic_node, new_raft, RaftConfig, RaftNode, WatchReceiver};
use coord_server::server::CoordNode;
use coord_server::storage::mvcc::MvccStorage;
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::snapshot::SnapshotTracker;
use coord_server::timer::TimerWheel;

fn find_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// 一个运行中的进程内 raft 节点（真实 openraft + 真实 gRPC raft RPC）
struct TestNode {
    node_id: u64,
    raft: Arc<coord_server::raft::CoordRaft>,
    node: Arc<CoordNode>,
    mvcc: Arc<MvccStorage<RedbBackend>>,
    lease_manager: Arc<coord_server::lease::LeaseManager>,
    _raft_handle: tokio::task::JoinHandle<()>,
    _data_dir: tempfile::TempDir,
}

impl TestNode {
    /// 启动 n 节点集群：node 1 初始化全部成员，其余节点等待复制。
    async fn start_cluster(n: u64) -> Vec<TestNode> {
        let raft_addrs: BTreeMap<u64, String> = (1..=n)
            .map(|id| (id, format!("127.0.0.1:{}", find_port())))
            .collect();

        let mut nodes = Vec::new();
        for node_id in 1..=n {
            let tmpdir = tempfile::tempdir().unwrap();
            let data_dir = tmpdir.path().to_path_buf();

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

            let network_factory = RaftNetworkFactoryImpl::new(node_id);
            for (id, addr) in &raft_addrs {
                network_factory.register_node(*id, addr.clone());
            }

            let raft_config = RaftConfig {
                heartbeat_interval: 200,
                election_timeout_min: 800,
                election_timeout_max: 1500,
                ..Default::default()
            };

            let raft_rpc_service = RaftRpcService::new();
            let raft = new_raft(
                node_id,
                Arc::new(raft_config),
                network_factory,
                log_store,
                sm_store,
            )
            .await
            .expect("create raft instance");
            raft_rpc_service.set_raft(raft.clone());

            if node_id == 1 {
                let members: BTreeMap<u64, RaftNode> = raft_addrs
                    .iter()
                    .map(|(id, addr)| (*id, new_basic_node(addr)))
                    .collect();
                raft.initialize(members).await.expect("initialize");
            }

            let raft = Arc::new(raft);

            let lease_manager =
                Arc::new(coord_server::lease::LeaseManager::new(TimerWheel::start()));
            let mut node = CoordNode::new(Arc::clone(&mvcc));
            node.node_id = node_id;
            node.raft = Some(Arc::clone(&raft));
            node.lease_manager = Some(Arc::clone(&lease_manager));
            let node = Arc::new(node);

            // 真实 raft RPC gRPC server（节点间通信）
            let raft_addr: std::net::SocketAddr =
                raft_addrs[&node_id].parse().expect("parse raft addr");
            let raft_svc = RaftRpcServer::new(raft_rpc_service);
            let raft_handle = tokio::spawn(async move {
                let _ = tonic::transport::Server::builder()
                    .add_service(raft_svc)
                    .serve(raft_addr)
                    .await;
            });

            nodes.push(TestNode {
                node_id,
                raft,
                node,
                mvcc,
                lease_manager,
                _raft_handle: raft_handle,
                _data_dir: tmpdir,
            });
        }

        // 等待 leader 选出
        let leader = wait_for_leader(&nodes, Duration::from_secs(15)).await;
        assert!(
            leader.is_some(),
            "no leader elected within timeout among {} nodes",
            nodes.len()
        );
        nodes
    }
}

/// 轮询等待集群选出 leader，返回 leader 节点下标。
///
/// openraft 0.10.0-alpha.34 起 leader 需先获得 quorum 确认（leader lease）才能
/// 接受写入；`current_leader` 就绪不代表 lease 已建立，因此同时要求
/// `last_quorum_acked` 为 Some，避免选举后立即 client_write 收到
/// `ForwardToLeader(leader_id: None)`。
async fn wait_for_leader(nodes: &[TestNode], timeout: Duration) -> Option<usize> {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        for (i, n) in nodes.iter().enumerate() {
            let metrics = n.raft.metrics();
            let m = metrics.borrow_watched();
            if m.current_leader == Some(n.node_id) && m.last_quorum_acked.is_some() {
                return Some(i);
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}

/// 通过 raft 提交 LeaseOp，返回 revision。
async fn propose_lease(leader: &TestNode, op: LeaseOp) -> u64 {
    let resp = leader
        .raft
        .client_write(Command::Lease(op))
        .await
        .expect("client_write lease op");
    match resp.response() {
        Response::Lease { revision } => *revision,
        other => panic!("unexpected response: {other:?}"),
    }
}

/// 通过 raft 写入带 lease 绑定的 KV。
async fn put_with_lease(leader: &TestNode, key: &[u8], value: &[u8], lease_id: i64) {
    let resp = leader
        .raft
        .client_write(Command::Put {
            key: key.to_vec(),
            value: value.to_vec(),
            lease_id: Some(lease_id),
        })
        .await
        .expect("client_write put");
    assert!(matches!(resp.response(), Response::Put { .. }));
}

/// 轮询直到断言成立（跨节点复制有延迟）。
async fn wait_until<F: Fn() -> bool>(what: &str, f: F, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("condition not met within timeout: {what}");
}

/// B.7-1：revoke 走 raft，三节点 `/_lease/` 与 KV 状态一致。
#[tokio::test]
async fn test_lease_revoke_via_raft_three_nodes_consistent() {
    let nodes = TestNode::start_cluster(3).await;
    let leader_idx = wait_for_leader(&nodes, Duration::from_secs(5))
        .await
        .unwrap();
    let leader = &nodes[leader_idx];

    // Grant（入 raft 日志）
    let deadline = wall_clock_now_ms() + 60_000;
    propose_lease(
        leader,
        LeaseOp::Grant {
            id: 1,
            ttl: 60,
            deadline_wall_ms: deadline,
        },
    )
    .await;

    // 绑定 Key（KV 写入与 lease_id 同事务，apply 内派生绑定关系）
    put_with_lease(leader, b"/lease/k1", b"v1", 1).await;

    // 全节点确认 lease 记录与 KV 已复制
    for n in &nodes {
        wait_until(
            &format!("node {} sees lease record", n.node_id),
            || {
                n.mvcc
                    .get_lease_record(1)
                    .map(|r| r.is_some())
                    .unwrap_or(false)
            },
            Duration::from_secs(10),
        )
        .await;
        wait_until(
            &format!("node {} sees key", n.node_id),
            || {
                n.mvcc
                    .get(b"/lease/k1")
                    .map(|v| v.is_some())
                    .unwrap_or(false)
            },
            Duration::from_secs(10),
        )
        .await;
    }

    // Revoke（走 raft，禁止直写）
    propose_lease(
        leader,
        LeaseOp::Revoke {
            id: 1,
            delete_keys: true,
        },
    )
    .await;

    // 三节点一致：lease 记录删除、绑定 key 删除
    for n in &nodes {
        wait_until(
            &format!("node {} lease removed", n.node_id),
            || {
                n.mvcc
                    .get_lease_record(1)
                    .map(|r| r.is_none())
                    .unwrap_or(false)
            },
            Duration::from_secs(10),
        )
        .await;
        wait_until(
            &format!("node {} key removed", n.node_id),
            || {
                n.mvcc
                    .get(b"/lease/k1")
                    .map(|v| v.is_none())
                    .unwrap_or(false)
            },
            Duration::from_secs(10),
        )
        .await;
    }

    // revision 一致（revision ≡ log index）
    let revs: Vec<u64> = nodes.iter().map(|n| n.mvcc.current_revision()).collect();
    assert!(
        revs.windows(2).all(|w| w[0] == w[1]),
        "revisions diverge: {revs:?}"
    );
}

/// B.7-2：leader 挂掉后新 leader 从状态机重建 Lease 表，到期 lease 的绑定 key 被清理。
#[tokio::test]
async fn test_lease_failover_rebuild_and_expired_cleanup() {
    let nodes = TestNode::start_cluster(3).await;
    let leader_idx = wait_for_leader(&nodes, Duration::from_secs(5))
        .await
        .unwrap();

    {
        let leader = &nodes[leader_idx];
        // 短 TTL lease + 绑定 key
        let deadline = wall_clock_now_ms() + 1_000;
        propose_lease(
            leader,
            LeaseOp::Grant {
                id: 2,
                ttl: 1,
                deadline_wall_ms: deadline,
            },
        )
        .await;
        put_with_lease(leader, b"/lease/ephemeral", b"v2", 2).await;
        for n in &nodes {
            wait_until(
                &format!("node {} sees key", n.node_id),
                || {
                    n.mvcc
                        .get(b"/lease/ephemeral")
                        .map(|v| v.is_some())
                        .unwrap_or(false)
                },
                Duration::from_secs(10),
            )
            .await;
        }
    }

    // 等待 lease 过期（> TTL），随后 leader 挂掉触发 failover
    tokio::time::sleep(Duration::from_millis(1300)).await;
    let old_leader = &nodes[leader_idx];
    let _ = old_leader.raft.shutdown().await;

    // 新 leader 选出（在存活节点中轮询；旧节点 raft 已关停，不再自认 leader，
    // 且其 current_leader 状态可能残留旧值，故不作为判定依据）。
    // 同时要求 `last_quorum_acked`（alpha.34 leader lease 已建立），否则新 leader
    // 选举后立即 propose 会收到 `ForwardToLeader(leader_id: None)`。
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut new_leader_idx = None;
    while tokio::time::Instant::now() < deadline {
        for (i, n) in nodes.iter().enumerate() {
            if i == leader_idx {
                continue;
            }
            let metrics = n.raft.metrics();
            let m = metrics.borrow_watched();
            if m.current_leader == Some(n.node_id) && m.last_quorum_acked.is_some() {
                new_leader_idx = Some(i);
                break;
            }
        }
        if new_leader_idx.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let new_leader_idx = new_leader_idx.expect("no new leader elected after old leader shutdown");
    let new_leader = &nodes[new_leader_idx];

    // 新 leader 从状态机重建（reconciler 的行为：list_lease_records + rebuild）
    let records = new_leader.mvcc.list_lease_records().unwrap();
    assert_eq!(records.len(), 1, "persisted lease should survive failover");
    let rebuilt = new_leader.lease_manager.rebuild(records).await;
    assert_eq!(rebuilt, 1);

    // 重建后 lease 已过期 → check_expired 检出 → 经 raft propose Revoke 清理
    let actions = new_leader.lease_manager.check_expired();
    assert_eq!(
        actions.len(),
        1,
        "expired lease must be detected after rebuild"
    );

    propose_lease(
        new_leader,
        LeaseOp::Revoke {
            id: 2,
            delete_keys: true,
        },
    )
    .await;

    // 存活节点均清理（旧 leader 已关停，重启后由日志追平，不在本次断言范围）
    for (i, n) in nodes.iter().enumerate() {
        if i == leader_idx {
            continue;
        }
        wait_until(
            &format!("node {} key cleaned after failover", n.node_id),
            || {
                n.mvcc
                    .get(b"/lease/ephemeral")
                    .map(|v| v.is_none())
                    .unwrap_or(false)
            },
            Duration::from_secs(10),
        )
        .await;
    }
}

/// B.7-3：follower 上 grant/revoke 被拒绝并返回 leader 提示。
#[tokio::test]
async fn test_follower_rejects_lease_operations() {
    let nodes = TestNode::start_cluster(3).await;
    let leader_idx = wait_for_leader(&nodes, Duration::from_secs(5))
        .await
        .unwrap();
    let follower_idx = (0..nodes.len()).find(|&i| i != leader_idx).unwrap();
    let follower = &nodes[follower_idx];
    let leader = &nodes[leader_idx];

    // Follower 上 grant → UNAVAILABLE + leader 提示
    let err = follower
        .node
        .lease_grant(tonic::Request::new(LeaseGrantRequest { ttl: 30, id: 0 }))
        .await
        .expect_err("follower must reject grant");
    assert_eq!(err.code(), tonic::Code::Unavailable);
    assert!(
        err.message().contains("leader"),
        "error must carry leader hint: {err}"
    );

    // Follower 上 revoke → UNAVAILABLE
    let err = follower
        .node
        .lease_revoke(tonic::Request::new(LeaseRevokeRequest { id: 1 }))
        .await
        .expect_err("follower must reject revoke");
    assert_eq!(err.code(), tonic::Code::Unavailable);

    // Leader 上 grant 正常
    let resp = leader
        .node
        .lease_grant(tonic::Request::new(LeaseGrantRequest { ttl: 30, id: 0 }))
        .await
        .expect("leader must accept grant");
    assert!(resp.into_inner().id > 0);
}
