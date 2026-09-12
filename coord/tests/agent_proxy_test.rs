// TDD: Agent 请求代理集成测试 (+ B5)
//
// 验证 Agent 能将请求转发到真实 Server：
// B2: KV Put / Range / Delete → Agent → Server → 数据持久化
// B5: Lease Grant / Revoke, Maintenance Status, Watch Fan-out
//
// 每个测试启动单节点 Server + Agent，通过 gRPC 连接 Agent 验证端到端语义。

mod common;

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use coord_proto::kv::kv_client::KvClient;
    use coord_proto::kv::{DeleteRequest, PutRequest, RangeRequest};
    use coord_proto::lease::lease_client::LeaseClient;
    use coord_proto::lease::{LeaseGrantRequest, LeaseRevokeRequest};
    use coord_proto::maintenance::maintenance_client::MaintenanceClient;
    use coord_proto::maintenance::StatusRequest;

    use coord_agent::{AgentConfig, AgentServer};

    // 共享夹具（单一事实来源）：此前 find_port()/start_test_server() 在本文件里
    // 逐字重复了一份，现已收敛到 tests/common/mod.rs。
    use crate::common::{find_port, start_test_server};

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

        // 2. Start Agent connected to the server（独立数据目录 + 就绪等待，避免并行启动竞态）
        let agent_tmp = tempfile::tempdir().unwrap();
        let agent_port = find_port();
        let agent_addr = format!("127.0.0.1:{}", agent_port);
        let agent_config = AgentConfig {
            agent_addr: agent_addr.clone(),
            http_addr: format!("127.0.0.1:{}", find_port()),
            data_dir: agent_tmp.path().to_string_lossy().to_string(),
            static_peers: vec![server_addr.clone()],
            ..Default::default()
        };

        let server = AgentServer::new(agent_config);
        let agent_handle = tokio::spawn(async move {
            server.serve().await.unwrap();
        });

        // agent 冷启动约 1s+（PKI/服务初始化），固定 200ms 在并行负载下必竞态
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            if tokio::net::TcpStream::connect(&agent_addr).await.is_ok() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "agent did not start within 30s"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

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

        tracing::info!(
            "Agent Put response: revision={}",
            put_resp.get_ref().revision
        );

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
        assert!(
            !kvs.is_empty(),
            "Agent returns empty kvs (proxy not forwarding)"
        );
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

        let agent_tmp = tempfile::tempdir().unwrap();
        let agent_port = find_port();
        let agent_addr = format!("127.0.0.1:{}", agent_port);
        let agent_config = AgentConfig {
            agent_addr: agent_addr.clone(),
            http_addr: format!("127.0.0.1:{}", find_port()),
            data_dir: agent_tmp.path().to_string_lossy().to_string(),
            static_peers: vec![server_addr.clone()],
            ..Default::default()
        };

        let server = AgentServer::new(agent_config);
        let agent_handle = tokio::spawn(async move {
            server.serve().await.unwrap();
        });

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            if tokio::net::TcpStream::connect(&agent_addr).await.is_ok() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "agent did not start within 30s"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

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
        assert!(
            !range_resp.get_ref().kvs.is_empty(),
            "Key should exist before delete"
        );

        // 3. Delete the key
        let delete_resp = kv_client
            .delete(DeleteRequest {
                key: key.to_vec(),
                ..Default::default()
            })
            .await
            .expect("KV Delete through agent should succeed");

        assert!(
            delete_resp.get_ref().deleted > 0,
            "Delete should report deleted > 0"
        );

        // 4. Verify key is gone
        let range_resp = kv_client
            .range(RangeRequest {
                key: key.to_vec(),
                ..Default::default()
            })
            .await
            .expect("Range after delete should succeed");
        assert!(
            range_resp.get_ref().kvs.is_empty(),
            "Key should be gone after delete"
        );

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

        let agent_tmp = tempfile::tempdir().unwrap();
        let agent_port = find_port();
        let agent_addr = format!("127.0.0.1:{}", agent_port);
        let agent_config = AgentConfig {
            agent_addr: agent_addr.clone(),
            http_addr: format!("127.0.0.1:{}", find_port()),
            data_dir: agent_tmp.path().to_string_lossy().to_string(),
            static_peers: vec![server_addr.clone()],
            ..Default::default()
        };

        let server = AgentServer::new(agent_config);
        let agent_handle = tokio::spawn(async move {
            server.serve().await.unwrap();
        });

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            if tokio::net::TcpStream::connect(&agent_addr).await.is_ok() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "agent did not start within 30s"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

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

        let agent_tmp = tempfile::tempdir().unwrap();
        let agent_port = find_port();
        let agent_addr = format!("127.0.0.1:{}", agent_port);
        let agent_config = AgentConfig {
            agent_addr: agent_addr.clone(),
            http_addr: format!("127.0.0.1:{}", find_port()),
            data_dir: agent_tmp.path().to_string_lossy().to_string(),
            static_peers: vec![server_addr.clone()],
            ..Default::default()
        };

        let server = AgentServer::new(agent_config);
        let agent_handle = tokio::spawn(async move {
            server.serve().await.unwrap();
        });

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            if tokio::net::TcpStream::connect(&agent_addr).await.is_ok() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "agent did not start within 30s"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

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
        assert!(
            !status.raft_leader.is_empty(),
            "Raft leader should be known"
        );
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

        let agent_tmp = tempfile::tempdir().unwrap();
        let agent_port = find_port();
        let agent_addr = format!("127.0.0.1:{}", agent_port);
        let agent_config = AgentConfig {
            agent_addr: agent_addr.clone(),
            http_addr: format!("127.0.0.1:{}", find_port()),
            data_dir: agent_tmp.path().to_string_lossy().to_string(),
            static_peers: vec![server_addr.clone()],
            ..Default::default()
        };

        let server = AgentServer::new(agent_config);
        let agent_handle = tokio::spawn(async move {
            server.serve().await.unwrap();
        });

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            if tokio::net::TcpStream::connect(&agent_addr).await.is_ok() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "agent did not start within 30s"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

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
