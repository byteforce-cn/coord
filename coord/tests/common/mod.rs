//! coord 集成测试共享夹具（single source of truth）。
//!
//! 仓库存在"复制粘贴族"：`find_port()` 在 42 个测试文件里
//! 各写一份，进程内 `start_test_server()` 也在 `agent_proxy_test.rs`、
//! `agent_watch_test.rs` 等处逐字重复。改一处要记得改 N 处，漏掉一处就是
//! 行为分叉。本模块把这些夹具收敛为**一份**定义，各集成测试以
//! `mod common;` 引入（Cargo 不会把 `tests/common/` 当作独立测试目标）。
//!
//! 只放"跨测试文件确实共享"的东西；单文件专用夹具仍留在各自文件里。

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use coord_core::storage::StorageBackend;
use coord_core::types::StorageConfig;
use coord_proto::kv::kv_server::KvServer;
use coord_proto::lease::lease_server::LeaseServer;
use coord_proto::maintenance::maintenance_server::MaintenanceServer;
use coord_proto::txn::txn_server::TxnServer;
use coord_proto::watch::watch_server::WatchServer;
use coord_server::lease::LeaseManager;
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcServer, RaftRpcService};
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::{new_basic_node, new_raft, RaftConfig};
use coord_server::server::CoordNode;
use coord_server::storage::compaction::CompactionManager;
use coord_server::storage::mvcc::MvccStorage;
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::timer::TimerWheel;
use coord_server::watch::WatchDispatcher;

/// 申请一个空闲的本地端口。
///
/// ⚠️ 本函数 bind(:0) → 取端口 → **立即释放**，服务器随后才重绑 —— 存在
/// TOCTOU 竞态：并行的测试（同进程内的其它测试线程）可能拿到**同一个**端口，
/// 后绑定的 server 失败被 `let _ = serve(..)` 吞掉后，客户端会连到**别的**
/// 测试的监听器上（典型症状：租约/读写 RPC 偶发 `Unimplemented`、`transport
/// error`、`no leader found`，即「假故障」）。
///
/// 因此：
/// - 进程内起 server 一律用 [`bind_listener`]（监听器保持到 `serve_with_incoming`
///   接管），不得用本函数返回值再 `serve(addr)`；
/// - 保留本函数仅供「端口号要交给**子进程**（无法传递监听器）」的调用方，
///   并经进程级去重避免同一进程内两次调用返回同一端口。
pub fn find_port() -> u16 {
    use std::collections::HashSet;
    use std::sync::{Mutex, OnceLock};

    // 进程级已发放端口集合：并发调用时若内核把刚释放的端口再次分配出来，
    // 重试换一个。只增不删——端口池 6w+，测试量级下不会耗尽。
    static ISSUED: OnceLock<Mutex<HashSet<u16>>> = OnceLock::new();
    let issued = ISSUED.get_or_init(|| Mutex::new(HashSet::new()));

    for _ in 0..128 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        // 先登记再返回：登记失败（并发调用已抢先登记同一端口）则重试。
        if issued.lock().unwrap().insert(port) {
            return port;
        }
    }
    panic!("find_port: 128 次尝试仍拿到重复端口（端口池异常）");
}

/// 绑定一个本地监听器并返回 `(listener, "127.0.0.1:port")`。
///
/// 与 [`find_port`] 不同：监听器**保持绑定**，直到交给
/// `tonic::transport::Server::serve_with_incoming[_shutdown]` 接管。
/// 这消除了「取端口→释放→重绑」窗口 —— 并行测试不可能再撞到同一端口。
pub async fn bind_listener() -> (tokio::net::TcpListener, String) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback listener");
    let addr = listener
        .local_addr()
        .expect("listener local_addr")
        .to_string();
    (listener, addr)
}

/// 进程内启动单节点 coord-server（真实 raft + 真实 gRPC），返回：
/// `(grpc_addr, shutdown_tx, grpc_handle, raft_handle, tmpdir)`。
///
/// 数据目录由返回的 `TempDir` 持有；drop 即清理。返回时已经等到 raft 选出 Leader
/// （或 3 秒超时），调用方可直接发起请求。
///
/// 端口用 [`bind_listener`] 预绑定（而非 `find_port` 后重绑）：见 `find_port`
/// 的竞态说明。
pub async fn start_test_server() -> (
    String,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
    tempfile::TempDir,
) {
    let tmpdir = tempfile::tempdir().unwrap();
    let data_dir = tmpdir.path().to_path_buf();

    // 预绑定（保持到 serve_with_incoming 接管）：不得用 find_port() + serve(addr)，
    // 那存在「取端口→释放→重绑」竞态（见 find_port 文档）。
    let (grpc_listener, grpc_addr) = bind_listener().await;
    let (raft_listener, raft_addr) = bind_listener().await;

    let storage_config = StorageConfig::default();
    let backend = RedbBackend::open(&data_dir, &storage_config).expect("open redb backend");
    let mvcc = Arc::new(MvccStorage::new(backend).expect("create mvcc"));
    let snapshot_tracker = Arc::new(coord_server::storage::snapshot::SnapshotTracker::default());

    let watch_dispatcher = Arc::new(WatchDispatcher::start());
    let log_store = LogStore::new(&data_dir)
        .await
        .expect("create raft log store")
        .with_snapshot_tracker(Arc::clone(&snapshot_tracker));
    let sm_store = StateMachineStore::new(
        Arc::clone(&mvcc),
        data_dir.join("snapshots"),
        Arc::clone(&snapshot_tracker),
    );

    let network_factory = RaftNetworkFactoryImpl::new(1);
    network_factory.register_node(1, raft_addr.clone());

    let raft_config = RaftConfig {
        heartbeat_interval: 200,
        election_timeout_min: 800,
        election_timeout_max: 1500,
        ..Default::default()
    };

    let raft_rpc_service = RaftRpcService::new();
    let raft = new_raft(
        1,
        Arc::new(raft_config),
        network_factory,
        log_store,
        sm_store,
    )
    .await
    .expect("create raft instance");

    raft_rpc_service.set_raft(raft.clone());

    let mut members = BTreeMap::new();
    members.insert(1, new_basic_node(&raft_addr));
    raft.initialize(members).await.expect("raft initialize");
    let raft = Arc::new(raft);

    let mut node = CoordNode::new(Arc::clone(&mvcc));
    node.node_id = 1;
    node.watch_dispatcher = Some(Arc::clone(&watch_dispatcher));
    node.raft = Some(Arc::clone(&raft));

    let timer_handle = TimerWheel::start();
    let lease_manager = Arc::new(LeaseManager::new(timer_handle));
    node.lease_manager = Some(Arc::clone(&lease_manager));
    let node = Arc::new(node);

    let compaction_config = coord_server::storage::compaction::CompactionConfig::default();
    let compaction_proposer: Arc<dyn coord_server::storage::compaction::CompactProposer> =
        node.clone();
    let _compaction_mgr = CompactionManager::start(
        Arc::clone(&mvcc),
        compaction_config,
        Some(compaction_proposer),
        None,
    );

    let kv_svc = KvServer::from_arc(Arc::clone(&node));
    let txn_svc = TxnServer::from_arc(Arc::clone(&node));
    let lease_svc = LeaseServer::from_arc(Arc::clone(&node));
    let watch_svc = WatchServer::from_arc(Arc::clone(&node));
    let maint_svc = MaintenanceServer::from_arc(Arc::clone(&node));

    let raft_rpc_svc = RaftRpcServer::new(raft_rpc_service);
    let raft_handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(raft_rpc_svc)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                raft_listener,
            ))
            .await;
    });

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let grpc_handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(kv_svc)
            .add_service(txn_svc)
            .add_service(lease_svc)
            .add_service(watch_svc)
            .add_service(maint_svc)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(grpc_listener),
                async {
                    let _ = shutdown_rx.await;
                },
            )
            .await;
    });

    // 等待 raft 选出 Leader（有界等待，不引入"必然成立"的固定 sleep）
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if raft.current_leader().await.is_some() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "raft did not elect a leader within 10s"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    (grpc_addr, shutdown_tx, grpc_handle, raft_handle, tmpdir)
}

/// 有界等待 `addr` 可建立 TCP 连接（避免固定 sleep 造成的并行竞态）。
pub async fn wait_tcp_ready(addr: &str, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "endpoint {addr} not reachable within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// 有界轮询等待条件成立（替代固定 `sleep`：既快又不脆）。
///
/// 超时即断言失败——**不得**静默通过（那会把"没等到"伪装成"通过"）。
pub async fn wait_until<F, Fut>(mut cond: F, timeout: Duration, what: &str)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if cond().await {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "condition not met within {timeout:?}: {what}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
