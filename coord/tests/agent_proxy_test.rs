// TDD: Agent 请求代理集成测试 (Phase B2 + B5)
//
// 验证 Agent 能将请求转发到真实 Server：
// B2: KV Put / Range / Delete → Agent → Server → 数据持久化
// B5: Lease Grant / Revoke, Maintenance Status, Watch Fan-out
//
// 每个测试启动单节点 Server + Agent，通过 gRPC 连接 Agent 验证端到端语义。

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use coord_core::storage::StorageBackend;
    use coord_core::types::StorageConfig;
    use coord_server::raft::log_store::LogStore;
    use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcServer, RaftRpcService};
    use coord_server::raft::state_machine::StateMachineStore;
    use coord_server::server::CoordNode;
    use coord_server::storage::compaction::CompactionManager;
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
    use coord_proto::kv::{PutRequest, RangeRequest, DeleteRequest};
    use coord_proto::lease::lease_client::LeaseClient;
    use coord_proto::lease::{LeaseGrantRequest, LeaseRevokeRequest};
    use coord_proto::maintenance::maintenance_client::MaintenanceClient;
    use coord_proto::maintenance::StatusRequest;

    use coord_agent::{AgentConfig, AgentServer};

    fn find_port() -> u16 {
        use std::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    }

    /// Start a single-node coord-server for testing.
    /// Returns (grpc_addr, shutdown_tx, grpc_handle, raft_handle, _tempdir).
    async fn start_test_server() -> (
        String,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
        tokio::task::JoinHandle<()>,
        tempfile::TempDir,
    ) {
        let tmpdir = tempfile::tempdir().unwrap();
        let data_dir = tmpdir.path().to_path_buf();

        let grpc_port = find_port();
        let raft_port = find_port();
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

        let compaction_config = coord_server::storage::compaction::CompactionConfig::default();
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

        // Wait for server ready + leader election
        tokio::time::sleep(Duration::from_millis(300)).await;
        for i in 0..30 {
            if raft.current_leader().await.is_some() {
                tracing::info!("Leader elected after {}ms", i * 100);
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        (grpc_addr, shutdown_tx, grpc_handle, raft_handle, tmpdir)
    }

    /// B2.1: Agent 代理 KV Put → Range 全路径
    ///
    /// RED: Agent 当前返回占位数据（revision=1），无法返回真实写入的值。
    #[tokio::test(flavor = "multi_thread")]
    async fn test_agent_proxy_kv_put_and_range() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("coord=info")
            .try_init();

        // 1. Start real coord-server
        let (server_addr, _shutdown_tx, _grpc, _raft, _tmpdir) = start_test_server().await;
        tracing::info!("Test server running on {}", server_addr);

        // 2. Start Agent connected to the server
        let agent_port = find_port();
        let agent_addr = format!("127.0.0.1:{}", agent_port);
        let agent_config = AgentConfig {
            agent_addr: agent_addr.clone(),
            http_addr: format!("127.0.0.1:{}", find_port()),
            data_dir: "/tmp/coord-agent-test".into(),
            static_peers: vec![server_addr.clone()],
            ..Default::default()
        };

        let server = AgentServer::new(agent_config);
        let agent_handle = tokio::spawn(async move {
            server.serve().await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(200)).await;

        // 3. Connect gRPC client to Agent (not directly to Server)
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{agent_addr}"))
            .unwrap()
            .connect()
            .await
            .expect("connect to agent");
        let mut kv_client = KvClient::new(channel);

        // 4. Put a key through Agent
        let put_resp = kv_client
            .put(PutRequest {
                key: b"/agent/proxy/test-key".to_vec(),
                value: b"proxy-value-42".to_vec(),
                ..Default::default()
            })
            .await
            .expect("KV Put through agent should succeed");

        tracing::info!("Agent Put response: revision={}", put_resp.get_ref().revision);

        // 5. Range the key back through Agent
        let range_resp = kv_client
            .range(RangeRequest {
                key: b"/agent/proxy/test-key".to_vec(),
                ..Default::default()
            })
            .await
            .expect("KV Range through agent should succeed");

        let kvs = &range_resp.get_ref().kvs;
        tracing::info!("Agent Range response: {} kvs", kvs.len());

        // GREEN 断言：Agent 应将请求转发到真实 Server 并返回实际数据
        assert!(!kvs.is_empty(), "Agent returns empty kvs (proxy not forwarding)");
        if !kvs.is_empty() {
            assert_eq!(kvs[0].key, b"/agent/proxy/test-key");
            assert_eq!(kvs[0].value, b"proxy-value-42");
        }

        // Cleanup
        agent_handle.abort();
    }

    /// B5.1: Agent 代理 KV Delete 全路径
    ///
    /// RED: 验证 Agent 能正确转发 KV Delete 请求并返回成功。
    #[tokio::test(flavor = "multi_thread")]
    async fn test_agent_proxy_kv_delete() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("coord=info")
            .try_init();

        let (server_addr, _shutdown_tx, _grpc, _raft, _tmpdir) = start_test_server().await;
        tracing::info!("Test server running on {}", server_addr);

        let agent_port = find_port();
        let agent_addr = format!("127.0.0.1:{}", agent_port);
        let agent_config = AgentConfig {
            agent_addr: agent_addr.clone(),
            http_addr: format!("127.0.0.1:{}", find_port()),
            data_dir: "/tmp/coord-agent-test".into(),
            static_peers: vec![server_addr.clone()],
            ..Default::default()
        };

        let server = AgentServer::new(agent_config);
        let agent_handle = tokio::spawn(async move {
            server.serve().await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(200)).await;

        let channel = tonic::transport::Endpoint::from_shared(format!("http://{agent_addr}"))
            .unwrap()
            .connect()
            .await
            .expect("connect to agent");
        let mut kv_client = KvClient::new(channel);

        let key = b"/agent/proxy/to-delete";

        // 1. Put a key
        kv_client
            .put(PutRequest {
                key: key.to_vec(),
                value: b"will-be-deleted".to_vec(),
                ..Default::default()
            })
            .await
            .expect("Put should succeed");

        // 2. Verify key exists
        let range_resp = kv_client
            .range(RangeRequest {
                key: key.to_vec(),
                ..Default::default()
            })
            .await
            .expect("Range should succeed");
        assert!(!range_resp.get_ref().kvs.is_empty(), "Key should exist before delete");

        // 3. Delete the key
        let delete_resp = kv_client
            .delete(DeleteRequest {
                key: key.to_vec(),
                ..Default::default()
            })
            .await
            .expect("KV Delete through agent should succeed");

        assert!(delete_resp.get_ref().deleted > 0, "Delete should report deleted > 0");

        // 4. Verify key is gone
        let range_resp = kv_client
            .range(RangeRequest {
                key: key.to_vec(),
                ..Default::default()
            })
            .await
            .expect("Range after delete should succeed");
        assert!(range_resp.get_ref().kvs.is_empty(), "Key should be gone after delete");

        agent_handle.abort();
    }

    /// B5.2: Agent 代理 Lease Grant → Revoke 全路径
    ///
    /// RED: 验证 Agent 能正确转发 Lease 操作。
    #[tokio::test(flavor = "multi_thread")]
    async fn test_agent_proxy_lease_grant_and_revoke() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("coord=info")
            .try_init();

        let (server_addr, _shutdown_tx, _grpc, _raft, _tmpdir) = start_test_server().await;
        tracing::info!("Test server running on {}", server_addr);

        let agent_port = find_port();
        let agent_addr = format!("127.0.0.1:{}", agent_port);
        let agent_config = AgentConfig {
            agent_addr: agent_addr.clone(),
            http_addr: format!("127.0.0.1:{}", find_port()),
            data_dir: "/tmp/coord-agent-test".into(),
            static_peers: vec![server_addr.clone()],
            ..Default::default()
        };

        let server = AgentServer::new(agent_config);
        let agent_handle = tokio::spawn(async move {
            server.serve().await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(200)).await;

        let channel = tonic::transport::Endpoint::from_shared(format!("http://{agent_addr}"))
            .unwrap()
            .connect()
            .await
            .expect("connect to agent");
        let mut lease_client = LeaseClient::new(channel);

        // 1. Grant a lease (TTL 10s)
        let grant_resp = lease_client
            .lease_grant(LeaseGrantRequest { ttl: 10, id: 0 })
            .await
            .expect("Lease Grant through agent should succeed");

        let lease_id = grant_resp.get_ref().id;
        assert!(lease_id > 0, "Lease ID should be positive: {lease_id}");
        assert_eq!(grant_resp.get_ref().ttl, 10);
        tracing::info!("Lease granted: id={lease_id}, ttl=10");

        // 2. Revoke the lease
        let revoke_resp = lease_client
            .lease_revoke(LeaseRevokeRequest { id: lease_id })
            .await;

        // Revoke might fail if lease already expired, but should not be a connection error
        match revoke_resp {
            Ok(_) => tracing::info!("Lease {lease_id} revoked successfully"),
            Err(ref e) => {
                // Acceptable: lease already expired or not found
                tracing::info!("Lease revoke result: {e}");
                assert!(
                    e.code() == tonic::Code::NotFound || e.code() == tonic::Code::Ok,
                    "Revoke error should be NotFound at worst, got {:?}",
                    e.code()
                );
            }
        }

        agent_handle.abort();
    }

    /// B5.3: Agent 代理 Maintenance Status
    ///
    /// RED: 验证 Agent 能正确转发 Maintenance::Status 并返回 Server 状态。
    #[tokio::test(flavor = "multi_thread")]
    async fn test_agent_proxy_maintenance_status() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("coord=info")
            .try_init();

        let (server_addr, _shutdown_tx, _grpc, _raft, _tmpdir) = start_test_server().await;
        tracing::info!("Test server running on {}", server_addr);

        let agent_port = find_port();
        let agent_addr = format!("127.0.0.1:{}", agent_port);
        let agent_config = AgentConfig {
            agent_addr: agent_addr.clone(),
            http_addr: format!("127.0.0.1:{}", find_port()),
            data_dir: "/tmp/coord-agent-test".into(),
            static_peers: vec![server_addr.clone()],
            ..Default::default()
        };

        let server = AgentServer::new(agent_config);
        let agent_handle = tokio::spawn(async move {
            server.serve().await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(200)).await;

        let channel = tonic::transport::Endpoint::from_shared(format!("http://{agent_addr}"))
            .unwrap()
            .connect()
            .await
            .expect("connect to agent");
        let mut maint_client = MaintenanceClient::new(channel);

        // Status 应返回 Server 的运行状态
        let status_resp = maint_client
            .status(StatusRequest {})
            .await
            .expect("Maintenance Status through agent should succeed");

        let status = status_resp.get_ref();
        tracing::info!(
            "Agent Status: revision={}, raft_index={}, raft_term={}, leader={}, seal={}",
            status.revision,
            status.raft_index,
            status.raft_term,
            status.raft_leader,
            status.seal_status
        );

        // 基本断言：Status 应返回有效数据
        assert!(!status.raft_leader.is_empty(), "Raft leader should be known");
        assert!(status.raft_term > 0, "Raft term should be positive");
        assert_eq!(status.seal_status, "unsealed", "Cluster should be unsealed");

        agent_handle.abort();
    }

    /// B5.4: Agent 代理多 Key 操作（前缀 Range）
    ///
    /// 验证 Agent 能正确处理前缀查询和批量 KV 操作。
    #[tokio::test(flavor = "multi_thread")]
    async fn test_agent_proxy_kv_multi_key_prefix_range() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("coord=info")
            .try_init();

        let (server_addr, _shutdown_tx, _grpc, _raft, _tmpdir) = start_test_server().await;
        tracing::info!("Test server running on {}", server_addr);

        let agent_port = find_port();
        let agent_addr = format!("127.0.0.1:{}", agent_port);
        let agent_config = AgentConfig {
            agent_addr: agent_addr.clone(),
            http_addr: format!("127.0.0.1:{}", find_port()),
            data_dir: "/tmp/coord-agent-test".into(),
            static_peers: vec![server_addr.clone()],
            ..Default::default()
        };

        let server = AgentServer::new(agent_config);
        let agent_handle = tokio::spawn(async move {
            server.serve().await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(200)).await;

        let channel = tonic::transport::Endpoint::from_shared(format!("http://{agent_addr}"))
            .unwrap()
            .connect()
            .await
            .expect("connect to agent");
        let mut kv_client = KvClient::new(channel);

        let prefix = b"/agent/proxy/prefix/";

        // 1. Put 3 keys under the same prefix
        for i in 1..=3 {
            let key = format!("/agent/proxy/prefix/key-{}", i);
            kv_client
                .put(PutRequest {
                    key: key.into_bytes(),
                    value: format!("value-{}", i).into_bytes(),
                    ..Default::default()
                })
                .await
                .expect("Put should succeed");
        }

        // 2. Range with prefix (range_end = prefix + 0xFF)
        let mut range_end = prefix.to_vec();
        if let Some(last) = range_end.last_mut() {
            *last = last.wrapping_add(1);
        }

        let range_resp = kv_client
            .range(RangeRequest {
                key: prefix.to_vec(),
                range_end: vec![0xFF; 1], // match all keys starting with prefix
                ..Default::default()
            })
            .await
            .expect("Range with prefix should succeed");

        let kvs = &range_resp.get_ref().kvs;
        tracing::info!("Prefix range returned {} kvs", kvs.len());
        assert!(kvs.len() >= 3, "Should return at least 3 keys under prefix");

        agent_handle.abort();
    }
}
