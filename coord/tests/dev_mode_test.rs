// TDD: coord dev 模式集成测试
//
// 验证单命令同时启动 Server + Agent 的开发模式行为。
// 参见 docs/dev-mode-plan.md、agent_proxy_test.rs
//
// 测试列表:
//   1. test_dev_mode_starts_server_and_agent — Server + Agent 并发启动，Agent 代理 KV 读写
//   2. test_dev_mode_graceful_shutdown — 关闭信号后端口释放
//   3. test_dev_mode_agent_waits_for_server — Agent 在 Server 就绪后自动连接

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use coord_core::storage::StorageBackend;
    use coord_core::types::StorageConfig;
    use coord_server::raft::log_store::LogStore;
    use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcServer, RaftRpcService};
    use coord_server::raft::state_machine::StateMachineStore;
    use coord_server::server::CoordNode;
    use coord_server::storage::compaction::{CompactionConfig, CompactionManager};
    use coord_server::storage::mvcc::MvccStorage;
    use coord_server::storage::redb_backend::RedbBackend;
    use coord_server::timer::TimerWheel;
    use coord_server::lease::LeaseManager;
    use coord_server::watch::WatchDispatcher;
    use coord_proto::kv::kv_server::KvServer;
    use coord_proto::txn::txn_server::TxnServer;
    use coord_proto::lease::lease_server::LeaseServer;
    use coord_proto::watch::watch_server::WatchServer;
    use coord_proto::maintenance::maintenance_server::MaintenanceServer;
    use coord_proto::kv::kv_client::KvClient;
    use coord_proto::kv::{PutRequest, RangeRequest};
    use coord_proto::agent::id_gen_client::IdGenClient;
    use coord_proto::agent::IdGenNextIdRequest;
    use coord_agent::{AgentConfig, AgentServer};
    use std::collections::BTreeMap;

    // ──── 工具函数 ────

    /// 查找可用端口
    fn find_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    /// 等待端口可连接
    async fn wait_for_port(addr: &str, timeout: Duration) -> Result<(), String> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if tokio::time::Instant::now() > deadline {
                return Err(format!("port {} not ready within {:?}", addr, timeout));
            }
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
    }

    /// 启动单节点 Raft Server（dev 模式用）
    ///
    /// 返回 (grpc_addr, shutdown_tx, grpc_handle, raft_handle, _tempdir)。
    /// 通过 `shutdown_tx` 可触发优雅关闭。
    async fn start_raft_server(
        grpc_port: u16,
        raft_port: u16,
    ) -> (
        String,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
        tokio::task::JoinHandle<()>,
        tempfile::TempDir,
    ) {
        let tmpdir = tempfile::tempdir().unwrap();
        let data_dir = tmpdir.path().to_path_buf();
        let grpc_addr = format!("127.0.0.1:{}", grpc_port);
        let raft_addr = format!("127.0.0.1:{}", raft_port);

        let storage_config = StorageConfig::default();
        let backend = RedbBackend::open(&data_dir, &storage_config).expect("open redb backend");
        let mvcc_read = Arc::new(MvccStorage::new(backend.clone()).expect("create mvcc read"));
        let mvcc_raft = MvccStorage::new(backend).expect("create mvcc raft");

        let watch_dispatcher = Arc::new(WatchDispatcher::start());
        let log_store = LogStore::new(&data_dir).await.expect("create raft log store");
        let sm_store = StateMachineStore::new(mvcc_raft);

        let network_factory = RaftNetworkFactoryImpl::new(1);
        network_factory.register_node(1, raft_addr.clone());

        let raft_config = openraft::Config {
            heartbeat_interval: 200,
            election_timeout_min: 800,
            election_timeout_max: 1500,
            ..Default::default()
        };

        let raft_rpc_service = RaftRpcService::new();
        let raft = openraft::Raft::new(
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
        members.insert(1, openraft::impls::BasicNode::new(&raft_addr));
        raft.initialize(members).await.expect("raft initialize");
        let raft = Arc::new(raft);

        let mut node = CoordNode::new(Arc::clone(&mvcc_read));
        node.watch_dispatcher = Some(Arc::clone(&watch_dispatcher));
        node.raft = Some(Arc::clone(&raft));

        let timer_handle = TimerWheel::start();
        let lease_manager = Arc::new(LeaseManager::new(timer_handle));
        node.lease_manager = Some(Arc::clone(&lease_manager));
        let node = Arc::new(node);

        let compaction_config = CompactionConfig::default();
        let _compaction_mgr = CompactionManager::start(Arc::clone(&mvcc_read), compaction_config);

        let kv_svc = KvServer::from_arc(Arc::clone(&node));
        let txn_svc = TxnServer::from_arc(Arc::clone(&node));
        let lease_svc = LeaseServer::from_arc(Arc::clone(&node));
        let watch_svc = WatchServer::from_arc(Arc::clone(&node));
        let maint_svc = MaintenanceServer::from_arc(Arc::clone(&node));

        let raft_rpc_svc = RaftRpcServer::new(raft_rpc_service);
        let raft_addr_parse: std::net::SocketAddr = raft_addr.parse().unwrap();
        let raft_handle = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(raft_rpc_svc)
                .serve(raft_addr_parse)
                .await;
        });

        let grpc_addr_parse: std::net::SocketAddr = grpc_addr.parse().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let grpc_handle = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(kv_svc)
                .add_service(txn_svc)
                .add_service(lease_svc)
                .add_service(watch_svc)
                .add_service(maint_svc)
                .serve_with_shutdown(grpc_addr_parse, async {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        // 等待 Raft Leader 选举完成
        for _ in 0..30 {
            if raft.current_leader().await.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        (grpc_addr, shutdown_tx, grpc_handle, raft_handle, tmpdir)
    }

    /// 启动 Agent 守护进程
    ///
    /// 返回 (agent_addr, agent_handle)。
    async fn start_agent(
        agent_port: u16,
        http_port: u16,
        server_addr: &str,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let agent_addr = format!("127.0.0.1:{}", agent_port);
        let agent_config = AgentConfig {
            agent_addr: agent_addr.clone(),
            http_addr: format!("127.0.0.1:{}", http_port),
            data_dir: "/tmp/coord-dev-agent-test".into(),
            static_peers: vec![server_addr.to_string()],
            ..Default::default()
        };

        let server = AgentServer::new(agent_config);
        let agent_handle = tokio::spawn(async move {
            if let Err(e) = server.serve().await {
                tracing::warn!("Agent server exited: {e}");
            }
        });

        (agent_addr, agent_handle)
    }

    // ──── 测试 1: Dev 模式 Server + Agent 并发启动 ────

    /// 验证 dev 模式核心行为：
    /// 1. Server 率先启动并就绪
    /// 2. Agent 随后启动并连接 Server
    /// 3. 通过 Agent 端口执行 KV Put/Get 成功
    #[tokio::test(flavor = "multi_thread")]
    async fn test_dev_mode_starts_server_and_agent() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("coord=info,coord_agent=info")
            .try_init();

        let grpc_port = find_port();
        let raft_port = find_port();
        let agent_port = find_port();
        let http_port = find_port();

        // 1. 启动 Server
        let (server_addr, server_shutdown_tx, _grpc_handle, _raft_handle, _tmpdir) =
            start_raft_server(grpc_port, raft_port).await;

        let server_ready_addr = format!("127.0.0.1:{}", grpc_port);
        wait_for_port(&server_ready_addr, Duration::from_secs(10))
            .await
            .expect("server gRPC should be ready");

        tracing::info!("[dev test] Server ready on {}", server_addr);

        // 2. 启动 Agent（连接 Server）
        let (agent_addr, agent_handle) =
            start_agent(agent_port, http_port, &server_addr).await;

        wait_for_port(&agent_addr, Duration::from_secs(10))
            .await
            .expect("agent gRPC should be ready");

        tracing::info!("[dev test] Agent ready on {}", agent_addr);

        // 3. 通过 Agent 端口执行 KV 操作
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{agent_addr}"))
            .unwrap()
            .connect_timeout(Duration::from_secs(3))
            .connect()
            .await
            .expect("connect to agent");

        let mut kv_client = KvClient::new(channel);

        // Put
        let put_resp = kv_client
            .put(PutRequest {
                key: b"dev-test-key".to_vec(),
                value: b"dev-test-value".to_vec(),
                lease_id: 0,
                prev_kv: false,
                request_id: vec![],
            })
            .await
            .expect("KV Put via agent should succeed");
        let put_inner = put_resp.into_inner();
        assert!(put_inner.revision > 0, "Put should return positive revision, got {}", put_inner.revision);
        tracing::info!("[dev test] KV Put OK");

        // Range
        let range_resp = kv_client
            .range(RangeRequest {
                key: b"dev-test-key".to_vec(),
                range_end: vec![],
                limit: 1,
                keys_only: false,
                count_only: false,
                ..Default::default()
            })
            .await
            .expect("KV Range via agent should succeed");

        let inner = range_resp.into_inner();
        assert_eq!(inner.kvs.len(), 1, "should return 1 KV pair");
        assert_eq!(inner.kvs[0].value, b"dev-test-value", "value should match");
        assert!(inner.count > 0, "count should be positive");
        tracing::info!("[dev test] KV Range OK: count={}", inner.count);

        // 4. 发送关闭信号
        drop(server_shutdown_tx);

        // Agent 会在 Server 断开后自然退出（gRPC 连接断开）
        let _ = tokio::time::timeout(Duration::from_secs(5), agent_handle).await;

        tracing::info!("[dev test] Graceful shutdown complete");
    }

    // ──── 测试 2: Dev 模式优雅关闭 ────

    /// 验证 Server 关闭后 Agent 端口释放，可立即重新绑定。
    #[tokio::test(flavor = "multi_thread")]
    async fn test_dev_mode_graceful_shutdown() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("coord=info")
            .try_init();

        let grpc_port = find_port();
        let raft_port = find_port();
        let agent_port = find_port();
        let http_port = find_port();

        let (server_addr, server_shutdown_tx, grpc_handle, raft_handle, _tmpdir) =
            start_raft_server(grpc_port, raft_port).await;

        wait_for_port(&format!("127.0.0.1:{}", grpc_port), Duration::from_secs(10))
            .await
            .expect("server should be ready");

        let (_agent_addr, agent_handle) =
            start_agent(agent_port, http_port, &server_addr).await;

        wait_for_port(&format!("127.0.0.1:{}", agent_port), Duration::from_secs(10))
            .await
            .expect("agent should be ready");

        // 触发关闭
        drop(server_shutdown_tx);

        // 等待 gRPC server 关闭
        let _ = tokio::time::timeout(Duration::from_secs(5), grpc_handle).await;
        raft_handle.abort();

        // 等待端口释放（Agent 的 serve() 无 shutdown signal，abort 后需短暂等待 OS 回收）
        agent_handle.abort();
        let _ = tokio::time::timeout(Duration::from_secs(3), agent_handle).await;
        tokio::time::sleep(Duration::from_millis(300)).await;

        // 验证 Server 端口已释放（可重新绑定）
        let rebind = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", grpc_port)).await;
        assert!(rebind.is_ok(), "server gRPC port {} should be released after shutdown: {:?}", grpc_port, rebind.err());
        drop(rebind);

        // 验证 Agent 端口已释放
        let rebind_agent = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", agent_port)).await;
        assert!(rebind_agent.is_ok(), "agent port {} should be released after abort: {:?}", agent_port, rebind_agent.err());

        tracing::info!("[dev test] Ports released after shutdown");
    }

    // ──── 测试 3: Agent 在 Server 未就绪时不崩溃 ────

    /// 验证 Agent 在 Server 未就绪时不会崩溃，
    /// 能以降级模式（skeleton）启动并监听端口。
    #[tokio::test(flavor = "multi_thread")]
    async fn test_dev_mode_agent_does_not_crash_without_server() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("coord=info")
            .try_init();

        let grpc_port = find_port(); // Server 端口，但不立即启动
        let agent_port = find_port();
        let http_port = find_port();
        let server_addr = format!("127.0.0.1:{}", grpc_port);

        // 启动 Agent（此时 Server 尚未就绪）
        let agent_config = AgentConfig {
            agent_addr: format!("127.0.0.1:{}", agent_port),
            http_addr: format!("127.0.0.1:{}", http_port),
            data_dir: "/tmp/coord-dev-agent-test".into(),
            static_peers: vec![server_addr.clone()],
            ..Default::default()
        };

        let server = AgentServer::new(agent_config);
        let agent_handle = tokio::spawn(async move {
            if let Err(e) = server.serve().await {
                tracing::warn!("Agent server exited: {e}");
            }
        });

        // Agent 应能启动（skeleton 模式降级）
        wait_for_port(&format!("127.0.0.1:{}", agent_port), Duration::from_secs(10))
            .await
            .expect("agent should start even without server");

        tracing::info!("[dev test] Agent started in skeleton mode without server");

        // 清理
        agent_handle.abort();
        let _ = tokio::time::timeout(Duration::from_secs(3), agent_handle).await;
    }

    // ──── 测试 4: 无 Server 时 IdGen 本地模式可用 ────

    /// 验证无 Server 连接（skeleton 模式）时 IdGen gRPC 服务仍注册，
    /// `nextId()` 返回本地雪花 ID（而非 UNIMPLEMENTED）。
    #[tokio::test(flavor = "multi_thread")]
    async fn test_dev_mode_agent_idgen_works_without_server() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("coord=info")
            .try_init();

        let agent_port = find_port();
        let http_port = find_port();

        // 启动 Agent（无 static_peers → skeleton 模式）
        let agent_config = AgentConfig {
            agent_addr: format!("127.0.0.1:{}", agent_port),
            http_addr: format!("127.0.0.1:{}", http_port),
            data_dir: "/tmp/coord-dev-agent-test".into(),
            static_peers: vec![],
            ..Default::default()
        };

        let server = AgentServer::new(agent_config);
        let agent_handle = tokio::spawn(async move {
            if let Err(e) = server.serve().await {
                tracing::warn!("Agent server exited: {e}");
            }
        });

        wait_for_port(&format!("127.0.0.1:{}", agent_port), Duration::from_secs(10))
            .await
            .expect("agent should start in skeleton mode");

        // 通过 Agent gRPC 调用 IdGen.NextId（无 Server，应走本地雪花模式）
        let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{}", agent_port))
            .unwrap()
            .connect_timeout(Duration::from_secs(3))
            .connect()
            .await
            .expect("connect to agent");

        let mut idgen_client = IdGenClient::new(channel);
        let resp = idgen_client
            .next_id(IdGenNextIdRequest {
                name: "test-order".to_string(),
                step: 0,
            })
            .await
            .expect("IdGen NextId should succeed without server (not UNIMPLEMENTED)");

        let id = resp.into_inner().id;
        assert!(id > 0, "local snowflake id should be positive, got {id}");

        // 清理
        agent_handle.abort();
        let _ = tokio::time::timeout(Duration::from_secs(3), agent_handle).await;
    }

    // ──── 测试 5: ISSUE-011 回归 —— 默认雪花模式，fresh Server 连续调用唯一 ────

    /// 验证默认实现（雪花 nodeid）：fresh Server 上同一 name 连续调用唯一且单调。
    #[tokio::test(flavor = "multi_thread")]
    async fn test_idgen_snowflake_fresh_sequential_unique() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("coord=info")
            .try_init();

        let grpc_port = find_port();
        let raft_port = find_port();
        let agent_port = find_port();
        let http_port = find_port();

        let (server_addr, server_shutdown_tx, _grpc_handle, _raft_handle, _tmpdir) =
            start_raft_server(grpc_port, raft_port).await;
        wait_for_port(&format!("127.0.0.1:{}", grpc_port), Duration::from_secs(10))
            .await
            .expect("server should be ready");

        let (agent_addr, agent_handle) = start_agent(agent_port, http_port, &server_addr).await;
        wait_for_port(&format!("127.0.0.1:{}", agent_port), Duration::from_secs(10))
            .await
            .expect("agent should be ready");

        let channel = tonic::transport::Endpoint::from_shared(format!("http://{agent_addr}"))
            .unwrap()
            .connect_timeout(Duration::from_secs(3))
            .connect()
            .await
            .expect("connect to agent");
        let mut idgen_client = IdGenClient::new(channel);

        let mut ids: Vec<i64> = Vec::new();
        for _ in 0..10 {
            let resp = idgen_client
                .next_id(IdGenNextIdRequest {
                    name: "permission".to_string(),
                    step: 0,
                })
                .await
                .expect("NextId should succeed");
            let id = resp.into_inner().id;
            assert!(id > 0, "snowflake id should be positive, got {id}");
            assert!(!ids.contains(&id), "duplicate id {id} in {ids:?}");
            if let Some(last) = ids.last() {
                assert!(id > *last, "snowflake id must be monotonic: {id} <= {last}");
            }
            ids.push(id);
        }

        drop(server_shutdown_tx);
        let _ = tokio::time::timeout(Duration::from_secs(5), agent_handle).await;
    }

    // ──── 测试 6: ISSUE-011 回归 —— 默认雪花模式，fresh Server 并发调用唯一 ────

    /// 验证默认实现（雪花 nodeid）：同一 agent 上并发 nextId 全部唯一。
    #[tokio::test(flavor = "multi_thread")]
    async fn test_idgen_snowflake_fresh_concurrent_unique() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("coord=info")
            .try_init();

        let grpc_port = find_port();
        let raft_port = find_port();
        let agent_port = find_port();
        let http_port = find_port();

        let (server_addr, server_shutdown_tx, _grpc_handle, _raft_handle, _tmpdir) =
            start_raft_server(grpc_port, raft_port).await;
        wait_for_port(&format!("127.0.0.1:{}", grpc_port), Duration::from_secs(10))
            .await
            .expect("server should be ready");

        let (agent_addr, agent_handle) = start_agent(agent_port, http_port, &server_addr).await;
        wait_for_port(&format!("127.0.0.1:{}", agent_port), Duration::from_secs(10))
            .await
            .expect("agent should be ready");

        let mut handles = Vec::new();
        for _ in 0..20 {
            let a = agent_addr.clone();
            handles.push(tokio::spawn(async move {
                let channel = tonic::transport::Endpoint::from_shared(format!("http://{a}"))
                    .unwrap()
                    .connect_timeout(Duration::from_secs(3))
                    .connect()
                    .await
                    .expect("connect to agent");
                let mut idgen_client = IdGenClient::new(channel);
                let resp = idgen_client
                    .next_id(IdGenNextIdRequest {
                        name: "obs-change".to_string(),
                        step: 0,
                    })
                    .await
                    .expect("NextId should succeed");
                resp.into_inner().id
            }));
        }

        let mut ids: Vec<i64> = Vec::new();
        for h in handles {
            let id = h.await.expect("concurrent task should succeed");
            assert!(id > 0, "snowflake id should be positive, got {id}");
            assert!(!ids.contains(&id), "duplicate id {id} in {ids:?}");
            ids.push(id);
        }

        drop(server_shutdown_tx);
        let _ = tokio::time::timeout(Duration::from_secs(5), agent_handle).await;
    }

    // ──── 测试 7: ISSUE-011 核心回归 —— 号段模式（opt-in），fresh Server 并发唯一 ────

    /// ISSUE-011 复现场景转绿：fresh Server + 号段模式（segment）并发 nextId 同一 name → 全部唯一。
    /// 旧实现（range + 普通 put，无 CAS）并发双方都读到 current_max=0 → 都返回 1 → duplicate key。
    #[tokio::test(flavor = "multi_thread")]
    async fn test_idgen_segment_fresh_concurrent_unique() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("coord=info")
            .try_init();

        let grpc_port = find_port();
        let raft_port = find_port();
        let agent_port = find_port();
        let http_port = find_port();

        let (server_addr, server_shutdown_tx, _grpc_handle, _raft_handle, _tmpdir) =
            start_raft_server(grpc_port, raft_port).await;
        wait_for_port(&format!("127.0.0.1:{}", grpc_port), Duration::from_secs(10))
            .await
            .expect("server should be ready");

        // Agent 配置为号段模式（ISSUE-011 复现场景）
        let agent_addr = format!("127.0.0.1:{}", agent_port);
        let agent_config = AgentConfig {
            agent_addr: agent_addr.clone(),
            http_addr: format!("127.0.0.1:{}", http_port),
            data_dir: tempfile::tempdir()
                .unwrap()
                .path()
                .to_string_lossy()
                .to_string(),
            static_peers: vec![server_addr.clone()],
            services: coord_agent::ServiceConfig {
                idgen_mode: "segment".into(),
                ..Default::default()
            },
            ..Default::default()
        };
        let server = AgentServer::new(agent_config);
        let agent_handle = tokio::spawn(async move {
            if let Err(e) = server.serve().await {
                tracing::warn!("Agent server exited: {e}");
            }
        });
        wait_for_port(&format!("127.0.0.1:{}", agent_port), Duration::from_secs(10))
            .await
            .expect("agent should be ready");

        // 并发 20 个 nextId（同一 name，fresh 状态）
        let mut handles = Vec::new();
        for _ in 0..20 {
            let a = agent_addr.clone();
            handles.push(tokio::spawn(async move {
                let channel = tonic::transport::Endpoint::from_shared(format!("http://{a}"))
                    .unwrap()
                    .connect_timeout(Duration::from_secs(3))
                    .connect()
                    .await
                    .expect("connect to agent");
                let mut idgen_client = IdGenClient::new(channel);
                let resp = idgen_client
                    .next_id(IdGenNextIdRequest {
                        name: "permission".to_string(),
                        step: 0,
                    })
                    .await
                    .expect("NextId should succeed");
                resp.into_inner().id
            }));
        }

        let mut ids: Vec<i64> = Vec::new();
        for h in handles {
            let id = h.await.expect("concurrent task should succeed");
            assert!(id > 0, "segment id should be positive, got {id}");
            assert!(
                !ids.contains(&id),
                "duplicate segment id {id} in {ids:?} (ISSUE-011 regression)"
            );
            ids.push(id);
        }

        drop(server_shutdown_tx);
        let _ = tokio::time::timeout(Duration::from_secs(5), agent_handle).await;
    }
}
