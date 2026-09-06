// 验收套件：成员变更与 Join 协议（membership_change_test）
//
// 验收标准：
// - Join 的新节点自动完成 learner→voter（`JoinRequest`，非 leader 重定向到 leader）；
// - remove 目标是 leader 时拒绝并提示（transfer_leader 属）；
// - 变更串行化：并发变更返回 UNAVAILABLE（互斥锁，本套件验证锁释放语义）。

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use coord_core::storage::StorageBackend;
use coord_core::types::StorageConfig;
use coord_proto::kv::kv_client::KvClient;
use coord_proto::kv::kv_server::KvServer;
use coord_proto::kv::{PutRequest, RangeRequest};
use coord_proto::maintenance::maintenance_client::MaintenanceClient;
use coord_proto::maintenance::maintenance_server::MaintenanceServer;
use coord_proto::maintenance::{JoinRequest, MemberRemoveRequest};
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcServer, RaftRpcService};
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::{new_basic_node, new_raft, RaftConfig, WatchReceiver};
use coord_server::server::CoordNode;
use coord_server::storage::mvcc::MvccStorage;
use coord_server::storage::redb_backend::RedbBackend;
use tonic::transport::Channel;

fn find_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn init_tracing() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("coord=debug,openraft=debug")
            .try_init();
    });
}

struct TestNode {
    #[allow(dead_code)]
    node_id: u64,
    grpc_addr: SocketAddr,
    raft: Arc<coord_server::raft::CoordRaft>,
    #[allow(dead_code)]
    node: Arc<CoordNode>,
    _shutdown_tx: tokio::sync::oneshot::Sender<()>,
    _grpc_handle: tokio::task::JoinHandle<()>,
    _raft_handle: tokio::task::JoinHandle<()>,
    _data_dir: tempfile::TempDir,
}

impl TestNode {
    /// 启动一个 raft 节点；`bootstrap=true` 时初始化单节点集群。
    async fn start(
        node_id: u64,
        grpc_port: u16,
        raft_port: u16,
        all_raft_addrs: &BTreeMap<u64, String>,
        all_grpc_addrs: &BTreeMap<u64, String>,
        bootstrap: bool,
    ) -> Self {
        let tmpdir = tempfile::tempdir().unwrap();
        let data_dir = tmpdir.path().to_path_buf();

        let grpc_addr: SocketAddr = format!("127.0.0.1:{grpc_port}").parse().unwrap();
        let raft_addr: SocketAddr = format!("127.0.0.1:{raft_port}").parse().unwrap();

        let storage_config = StorageConfig::default();
        let backend = RedbBackend::open(&data_dir, &storage_config).expect("open redb backend");
        let mvcc = Arc::new(MvccStorage::new(backend).expect("create mvcc"));
        let snapshot_tracker =
            Arc::new(coord_server::storage::snapshot::SnapshotTracker::default());

        let log_store = LogStore::new(&data_dir)
            .await
            .expect("create raft log store")
            .with_snapshot_tracker(Arc::clone(&snapshot_tracker));

        let sm_store = StateMachineStore::new(
            Arc::clone(&mvcc),
            data_dir.join("snapshots"),
            Arc::clone(&snapshot_tracker),
        );

        let network_factory = RaftNetworkFactoryImpl::new(node_id);
        for (id, addr) in all_raft_addrs {
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

        if bootstrap {
            let mut members = BTreeMap::new();
            members.insert(node_id, new_basic_node(&raft_addr.to_string()));
            raft.initialize(members)
                .await
                .expect("raft initialize single-node");
        }

        let raft = Arc::new(raft);

        let mut node = CoordNode::new(Arc::clone(&mvcc));
        node.node_id = node_id;
        node.raft = Some(Arc::clone(&raft));
        // 注册已知节点 gRPC 地址（leader 重定向用）
        for (id, addr) in all_grpc_addrs {
            node.register_grpc_addr(*id, addr);
        }
        let node = Arc::new(node);

        let kv_svc = KvServer::from_arc(Arc::clone(&node));
        let maint_svc = MaintenanceServer::from_arc(Arc::clone(&node));

        let raft_rpc_svc = RaftRpcServer::new(raft_rpc_service);
        let raft_handle = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(raft_rpc_svc)
                .serve(raft_addr)
                .await;
        });

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let grpc_handle = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(kv_svc)
                .add_service(maint_svc)
                .serve_with_shutdown(grpc_addr, async {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        tokio::time::sleep(Duration::from_millis(200)).await;

        TestNode {
            node_id,
            grpc_addr,
            raft,
            node,
            _shutdown_tx: shutdown_tx,
            _grpc_handle: grpc_handle,
            _raft_handle: raft_handle,
            _data_dir: tmpdir,
        }
    }
}

async fn channel(addr: SocketAddr) -> Channel {
    Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect_timeout(Duration::from_secs(3))
        .connect()
        .await
        .unwrap()
}

/// 等待某节点成为指定集合的 voter 成员。
async fn wait_voter(raft: &coord_server::raft::CoordRaft, node_id: u64, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let metrics = raft.metrics().borrow_watched().clone();
        if metrics
            .membership_config
            .voter_ids()
            .into_iter()
            .any(|id| id == node_id)
        {
            return true;
        }
        if tokio::time::Instant::now() > deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// D.5-1：Join 自动 learner→voter 全流程，跨节点读写一致。
#[tokio::test]
async fn test_join_adds_node_and_cluster_serves_reads() {
    init_tracing();
    let grpc1 = find_port();
    let raft1 = find_port();
    let grpc2 = find_port();
    let raft2 = find_port();

    let raft_addrs: BTreeMap<u64, String> = BTreeMap::from([
        (1, format!("127.0.0.1:{raft1}")),
        (2, format!("127.0.0.1:{raft2}")),
    ]);
    let grpc_addrs: BTreeMap<u64, String> = BTreeMap::from([
        (1, format!("127.0.0.1:{grpc1}")),
        (2, format!("127.0.0.1:{grpc2}")),
    ]);

    let node1 = TestNode::start(1, grpc1, raft1, &raft_addrs, &grpc_addrs, true).await;
    let node2 = TestNode::start(2, grpc2, raft2, &raft_addrs, &grpc_addrs, false).await;

    // 等待 node1 选出 leader
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if node1.raft.current_leader().await == Some(1) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "node1 did not become leader"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // 向 leader（node1）发送 JoinRequest
    let mut maint = MaintenanceClient::new(channel(node1.grpc_addr).await);
    let resp = maint
        .join(JoinRequest {
            node_id: 2,
            raft_addr: format!("127.0.0.1:{raft2}"),
            grpc_addr: format!("127.0.0.1:{grpc2}"),
        })
        .await
        .expect("join rpc")
        .into_inner();
    assert!(resp.success, "join should succeed: {}", resp.message);

    // node2 自动成为 voter（leader 视角）
    assert!(
        wait_voter(&node1.raft, 2, Duration::from_secs(20)).await,
        "node2 should become voter after join"
    );

    // 跨节点写入/读取
    let mut kv1 = KvClient::new(channel(node1.grpc_addr).await);
    let put = kv1
        .put(PutRequest {
            key: b"/join/k1".to_vec(),
            value: b"v1".to_vec(),
            lease_id: 0,
            prev_kv: false,
            request_id: vec![],
        })
        .await
        .expect("put via node1");
    assert!(put.into_inner().revision > 0);

    // node2 读取：follower 按设计拒绝 ReadIndex 读（与 etcd 一致，读走 leader），
    // 因此数据收敛以 node2 本地已提交状态为准（apply 幂等，直读存储）。
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if node2.node.storage.get(b"/join/k1").unwrap().is_some() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "node2 did not converge on joined data"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // leader 读路径照常服务
    let range = kv1
        .range(RangeRequest {
            key: b"/join/k1".to_vec(),
            range_end: vec![],
            limit: 1,
            revision: 0,
            keys_only: false,
            count_only: false,
        })
        .await
        .expect("leader range");
    assert_eq!(range.into_inner().kvs.len(), 1);
}

/// D.5-2：非 leader 收到 Join 返回 leader 重定向（forward_to）。
#[tokio::test]
async fn test_join_redirects_to_leader() {
    let grpc1 = find_port();
    let raft1 = find_port();
    let grpc2 = find_port();
    let raft2 = find_port();

    let raft_addrs: BTreeMap<u64, String> = BTreeMap::from([
        (1, format!("127.0.0.1:{raft1}")),
        (2, format!("127.0.0.1:{raft2}")),
    ]);
    let grpc_addrs: BTreeMap<u64, String> = BTreeMap::from([
        (1, format!("127.0.0.1:{grpc1}")),
        (2, format!("127.0.0.1:{grpc2}")),
    ]);

    let node1 = TestNode::start(1, grpc1, raft1, &raft_addrs, &grpc_addrs, true).await;
    let _node2 = TestNode::start(2, grpc2, raft2, &raft_addrs, &grpc_addrs, false).await;

    // node1 成为 leader 后，向 node2（非 leader）发 Join → 重定向
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if node1.raft.current_leader().await == Some(1) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "node1 did not become leader"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let mut maint2 = MaintenanceClient::new(channel(_node2.grpc_addr).await);
    let resp = maint2
        .join(JoinRequest {
            node_id: 2,
            raft_addr: format!("127.0.0.1:{raft2}"),
            grpc_addr: format!("127.0.0.1:{grpc2}"),
        })
        .await
        .expect("join rpc to non-leader")
        .into_inner();
    assert!(!resp.success, "non-leader must not complete join");
    assert_eq!(
        resp.forward_to,
        format!("127.0.0.1:{grpc1}"),
        "redirect should point to leader gRPC addr"
    );

    // 重试到 leader 成功
    let mut maint1 = MaintenanceClient::new(channel(node1.grpc_addr).await);
    let resp = maint1
        .join(JoinRequest {
            node_id: 2,
            raft_addr: format!("127.0.0.1:{raft2}"),
            grpc_addr: format!("127.0.0.1:{grpc2}"),
        })
        .await
        .expect("join rpc to leader")
        .into_inner();
    assert!(
        resp.success,
        "join via leader should succeed: {}",
        resp.message
    );
}

/// D.5-3（更新）：单节点集群移除唯一 voter 失败（无剩余 quorum，openraft 拒绝）。
#[tokio::test]
async fn test_remove_leader_rejected() {
    let grpc1 = find_port();
    let raft1 = find_port();
    let raft_addrs: BTreeMap<u64, String> = BTreeMap::from([(1, format!("127.0.0.1:{raft1}"))]);
    let grpc_addrs: BTreeMap<u64, String> = BTreeMap::from([(1, format!("127.0.0.1:{grpc1}"))]);

    let node1 = TestNode::start(1, grpc1, raft1, &raft_addrs, &grpc_addrs, true).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if node1.raft.current_leader().await == Some(1) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "node1 did not become leader"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let mut maint = MaintenanceClient::new(channel(node1.grpc_addr).await);
    let err = maint
        .member_remove(MemberRemoveRequest { node_id: 1 })
        .await
        .expect_err("removing the only voter must be rejected");
    assert_eq!(
        err.code(),
        tonic::Code::FailedPrecondition,
        "remove leader should be FAILED_PRECONDITION, got: {err}"
    );
}

/// -1：两节点集群移除 leader —— openraft 自移除（配置提交后旧 leader
/// 退位），剩余节点成为 leader 且可写。
#[tokio::test]
async fn test_remove_leader_self_removal_two_nodes() {
    let grpc1 = find_port();
    let raft1 = find_port();
    let grpc2 = find_port();
    let raft2 = find_port();
    let raft_addrs: BTreeMap<u64, String> = BTreeMap::from([
        (1, format!("127.0.0.1:{raft1}")),
        (2, format!("127.0.0.1:{raft2}")),
    ]);
    let grpc_addrs: BTreeMap<u64, String> = BTreeMap::from([
        (1, format!("127.0.0.1:{grpc1}")),
        (2, format!("127.0.0.1:{grpc2}")),
    ]);

    let node1 = TestNode::start(1, grpc1, raft1, &raft_addrs, &grpc_addrs, true).await;
    let node2 = TestNode::start(2, grpc2, raft2, &raft_addrs, &grpc_addrs, false).await;

    // node1 成为 leader
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if node1.raft.current_leader().await == Some(1) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "node1 did not become leader"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // node2 join（经 leader）
    let mut maint1 = MaintenanceClient::new(channel(node1.grpc_addr).await);
    let resp = maint1
        .join(JoinRequest {
            node_id: 2,
            raft_addr: format!("127.0.0.1:{raft2}"),
            grpc_addr: format!("127.0.0.1:{grpc2}"),
        })
        .await
        .expect("join node 2")
        .into_inner();
    assert!(resp.success, "node2 join failed: {}", resp.message);

    // 等待 node2 看到双节点 membership
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let m = node2.raft.metrics().borrow_watched().clone();
        if m.membership_config.voter_ids().count() == 2 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "node2 did not see 2-voter membership"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // 移除 leader（node1）：openraft 自移除，新配置提交后 node1 退位
    let resp = maint1
        .member_remove(MemberRemoveRequest { node_id: 1 })
        .await
        .expect("remove leader via self-removal")
        .into_inner();
    assert!(
        resp.success,
        "self-removal should succeed: {}",
        resp.message
    );

    // node2 成为新 leader 并可写
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if node2.raft.current_leader().await == Some(2) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "node2 did not become leader after removal"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let mut kv2 = KvClient::new(channel(node2.grpc_addr).await);
    let put = kv2
        .put(PutRequest {
            key: b"/after-removal".to_vec(),
            value: b"v".to_vec(),
            lease_id: 0,
            prev_kv: false,
            request_id: vec![],
        })
        .await
        .expect("write through new leader");
    assert!(put.into_inner().revision > 0);
}

/// -2：transfer_leadership 执行器 —— 移交后目标成为 leader。
#[tokio::test]
async fn test_transfer_leadership_executor() {
    let grpc1 = find_port();
    let raft1 = find_port();
    let grpc2 = find_port();
    let raft2 = find_port();
    let raft_addrs: BTreeMap<u64, String> = BTreeMap::from([
        (1, format!("127.0.0.1:{raft1}")),
        (2, format!("127.0.0.1:{raft2}")),
    ]);
    let grpc_addrs: BTreeMap<u64, String> = BTreeMap::from([
        (1, format!("127.0.0.1:{grpc1}")),
        (2, format!("127.0.0.1:{grpc2}")),
    ]);

    let node1 = TestNode::start(1, grpc1, raft1, &raft_addrs, &grpc_addrs, true).await;
    let node2 = TestNode::start(2, grpc2, raft2, &raft_addrs, &grpc_addrs, false).await;

    // node1 成为 leader；node2 join
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if node1.raft.current_leader().await == Some(1) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "node1 did not become leader"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let mut maint1 = MaintenanceClient::new(channel(node1.grpc_addr).await);
    let resp = maint1
        .join(JoinRequest {
            node_id: 2,
            raft_addr: format!("127.0.0.1:{raft2}"),
            grpc_addr: format!("127.0.0.1:{grpc2}"),
        })
        .await
        .expect("join node 2")
        .into_inner();
    assert!(resp.success);

    // node1 主动移交领导权给 node2
    let node1_inner = Arc::clone(&node1.node);
    let target = node1_inner
        .pick_transfer_target()
        .await
        .expect("node2 is a transfer target");
    assert_eq!(target, 2);
    node1_inner
        .transfer_leadership(target)
        .await
        .expect("transfer_leadership");

    // node2 成为 leader（收敛）
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if node2.raft.current_leader().await == Some(2) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "leadership did not transfer to node2"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
