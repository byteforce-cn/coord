// P1-01 验收测试（L2 进程内多节点 raft）：Compaction 真实现
//
// 覆盖决策文档 P1-01：
// - raft 下发 compact revision（节点一致）：leader 提案后三节点 `META_COMPACT_REVISION`
//   一致、changelog < revision 的条目被删除、KV 数据完好
// - 非法 revision（> 当前 applied）在提案层被拒绝（RPC 层校验，规格 13 §三）
// - 幂等：重复 compact 同 revision 无副作用
//
// 对应文档：`docs/production/15-milestone-task-breakdown.md` P1-01；
// `docs/production/11-architecture-redesign.md`（P0-A 快照生命周期延伸）。

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use coord_core::storage::StorageBackend;
use coord_core::types::StorageConfig;
use coord_proto::maintenance::maintenance_server::Maintenance;
use coord_proto::maintenance::CompactRequest;
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcServer, RaftRpcService};
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::type_config::{Command, Response};
use coord_server::raft::{new_basic_node, new_raft, RaftConfig, RaftNode, WatchReceiver};
use coord_server::server::CoordNode;
use coord_server::storage::mvcc::MvccStorage;
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::snapshot::SnapshotTracker;

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
    _raft_handle: tokio::task::JoinHandle<()>,
    _data_dir: tempfile::TempDir,
}

impl TestNode {
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

            let mut node = CoordNode::new(Arc::clone(&mvcc));
            node.node_id = node_id;
            node.raft = Some(Arc::clone(&raft));
            let node = Arc::new(node);

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
                _raft_handle: raft_handle,
                _data_dir: tmpdir,
            });
        }

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

async fn propose_put(leader: &TestNode, key: &[u8], value: &[u8]) -> u64 {
    let resp = leader
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

/// P1-01-1：raft 下发 compact revision —— 三节点一致删除、KV 完好。
#[tokio::test]
async fn test_compact_via_raft_three_nodes_consistent() {
    let nodes = TestNode::start_cluster(3).await;
    let leader_idx = wait_for_leader(&nodes, Duration::from_secs(5))
        .await
        .unwrap();
    let leader = &nodes[leader_idx];

    // 写入 20 条
    for i in 0..20u32 {
        propose_put(leader, format!("/k{i:02}").as_bytes(), b"v").await;
    }

    // 等待复制到全部节点
    for n in &nodes {
        wait_until(
            &format!("node {} catches up", n.node_id),
            || n.mvcc.current_revision() >= 20,
            Duration::from_secs(10),
        )
        .await;
    }

    // leader 提案 Compact{revision: 11}
    let resp = leader
        .raft
        .client_write(Command::Compact { revision: 11 })
        .await
        .expect("client_write compact");
    match resp.response() {
        Response::Compact { compacted_revision } => assert_eq!(*compacted_revision, 11),
        other => panic!("unexpected response: {other:?}"),
    }

    // 三节点一致：compacted_revision=11；[1,11) changelog 已删；>=11 保留；KV 完好
    for n in &nodes {
        wait_until(
            &format!("node {} applied compact", n.node_id),
            || n.mvcc.compacted_revision().unwrap() == 11,
            Duration::from_secs(10),
        )
        .await;

        for rev in 1..11u64 {
            assert!(
                !n.mvcc.changelog_contains_revision(rev).unwrap(),
                "node {} should have compacted rev {rev}",
                n.node_id
            );
        }
        for rev in 11..=20u64 {
            assert!(
                n.mvcc.changelog_contains_revision(rev).unwrap(),
                "node {} should retain rev {rev}",
                n.node_id
            );
        }
        assert_eq!(n.mvcc.get(b"/k00").unwrap(), Some(b"v".to_vec()));
        assert_eq!(n.mvcc.get(b"/k19").unwrap(), Some(b"v".to_vec()));
    }
}

/// P1-01-2：幂等 —— 重复/更低 revision 的 compact 不产生副作用。
#[tokio::test]
async fn test_compact_repeated_is_idempotent_across_nodes() {
    let nodes = TestNode::start_cluster(3).await;
    let leader_idx = wait_for_leader(&nodes, Duration::from_secs(5))
        .await
        .unwrap();
    let leader = &nodes[leader_idx];

    for i in 0..10u32 {
        propose_put(leader, format!("/m{i}").as_bytes(), b"v").await;
    }
    for n in &nodes {
        wait_until(
            &format!("node {} catches up", n.node_id),
            || n.mvcc.current_revision() >= 10,
            Duration::from_secs(10),
        )
        .await;
    }

    // 连续两次同 revision compact（第二次 apply 为幂等 no-op）
    for _ in 0..2 {
        let resp = leader
            .raft
            .client_write(Command::Compact { revision: 6 })
            .await
            .expect("client_write compact");
        assert!(matches!(
            resp.response(),
            Response::Compact {
                compacted_revision: 6
            }
        ));
    }

    for n in &nodes {
        wait_until(
            &format!("node {} applied compact", n.node_id),
            || n.mvcc.compacted_revision().unwrap() == 6,
            Duration::from_secs(10),
        )
        .await;
        // rev 6..=10 保留（第二次 compact 未误删）
        for rev in 6..=10u64 {
            assert!(
                n.mvcc.changelog_contains_revision(rev).unwrap(),
                "node {} must retain rev {rev} after idempotent compact",
                n.node_id
            );
        }
    }
}

/// P1-01-3：单节点模式（无 raft）直接 apply（提案层前置校验由 RPC 承担）。
#[test]
fn test_compact_standalone_apply() {
    let tmpdir = tempfile::tempdir().unwrap();
    let storage_config = StorageConfig::default();
    let backend = RedbBackend::open(tmpdir.path(), &storage_config).unwrap();
    let mvcc = Arc::new(MvccStorage::new(backend).unwrap());

    let mut node = CoordNode::new(Arc::clone(&mvcc));
    node.node_id = 1; // 无 raft：单节点模式
    let node = Arc::new(node);

    for i in 0..5u32 {
        node.storage
            .put(format!("/s{i}").as_bytes(), b"v", None)
            .unwrap();
    }

    // 直接调用 Maintenance::compact 的同源方法（无 raft → 本地 apply）
    let rev = coord_server::server::apply_compact_local(&node, 3).expect("standalone compact");
    assert_eq!(rev, 3);
    assert_eq!(mvcc.compacted_revision().unwrap(), 3);
    assert!(!mvcc.changelog_contains_revision(1).unwrap());
    assert!(mvcc.changelog_contains_revision(3).unwrap());
}

/// P1-01-4：RPC 层门控与校验 —— revision 0=压缩到当前；非 leader UNAVAILABLE；
/// 未来 revision INVALID_ARGUMENT。
#[tokio::test]
async fn test_compact_rpc_leader_gate_and_validation() {
    let nodes = TestNode::start_cluster(3).await;
    let leader_idx = wait_for_leader(&nodes, Duration::from_secs(5))
        .await
        .unwrap();
    let leader = &nodes[leader_idx];

    for i in 0..5u32 {
        propose_put(leader, format!("/r{i}").as_bytes(), b"v").await;
    }
    for n in &nodes {
        wait_until(
            &format!("node {} catches up", n.node_id),
            || n.mvcc.current_revision() >= 5,
            Duration::from_secs(10),
        )
        .await;
    }

    // revision 0 → 压缩到当前 revision（注：membership 入口占用一个 log index，
    // current_revision = 1(membership) + 5(puts) = 6）
    let current_before = leader.mvcc.current_revision();
    let resp = leader
        .node
        .compact(tonic::Request::new(CompactRequest { revision: 0 }))
        .await
        .expect("leader compact rpc")
        .into_inner();
    assert_eq!(resp.compacted_revision, current_before as i64);

    // 未来 revision → INVALID_ARGUMENT
    let err = leader
        .node
        .compact(tonic::Request::new(CompactRequest { revision: 999 }))
        .await
        .err()
        .expect("future revision must be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    // follower → UNAVAILABLE（附 leader 提示）
    let follower = nodes
        .iter()
        .find(|n| n.node_id != leader.node_id)
        .expect("a follower exists");
    let err = follower
        .node
        .compact(tonic::Request::new(CompactRequest { revision: 1 }))
        .await
        .err()
        .expect("follower compact must be rejected");
    assert_eq!(err.code(), tonic::Code::Unavailable);
    assert!(err.message().contains("not leader"));
}
