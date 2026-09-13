// TDD: Agent Watch Fan-out 集成测试 (+ B5)
//
// 验证 Agent Watch 代理语义：
// 1. 单订阅者：Watch 事件通过 Agent 正确传递（B5 修复集成测试）
// 2. 多订阅者 Fan-out：同一 prefix 的多个订阅者都收到事件
//
// B5 修复要点：
// - range_end 使用 prefix-end 语义（而非空 = 精确匹配）
// - 添加 Watch 注册延迟，确保 Server 侧 Watch 已就绪
// - 增加超时容错

mod common;

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use coord_proto::kv::kv_client::KvClient;
    use coord_proto::kv::PutRequest;
    use coord_proto::watch::watch_client::WatchClient;
    use coord_proto::watch::{WatchCreateRequest, WatchRequest};

    use coord_agent::{AgentConfig, AgentServer};

    // 共享夹具（单一事实来源）：此前 find_port()/start_test_server() 在本文件里
    // 逐字重复了一份，现已收敛到 tests/common/mod.rs。
    use crate::common::{find_port, start_test_server};

    /// Compute the "prefix end" key for etcd-style prefix matching.
    /// If key is `[a, b, c]`, prefix_end is `[a, b, d]`.
    /// If the last byte is 0xFF, strip it (watch all keys with that prefix).
    #[allow(dead_code)]
    fn prefix_end(key: &[u8]) -> Vec<u8> {
        if key.is_empty() {
            return vec![0];
        }
        let mut end = key.to_vec();
        while let Some(last) = end.last_mut() {
            if *last < 0xFF {
                *last += 1;
                return end;
            }
            end.pop();
        }
        // All bytes were 0xFF, return empty to match everything
        vec![0]
    }

    /// B5.5: 单订阅者 Watch — Agent 转发事件（B4 修复 + B5 验证）
    ///
    /// 验证 Agent 能正确转发 Watch 事件：
    /// 1. 通过 Agent 创建 Watch 订阅（精确 key 匹配，避免 range_end 复杂语义）
    /// 2. 通过 Agent 写入相同 key（应触发 Watch 事件）
    /// 3. 通过 Agent 接收 Watch 事件
    ///
    /// # ⚠️ 已知失效（`#[ignore]`，第四轮）—— 这不是“跳过一个不稳定的测试”
    ///
    /// 本用例**从未真正验证过投递**：旧版本在超时/出错分支里只 `tracing::warn!`
    /// 后正常返回，于是"一个事件都收不到"也表现为**通过**（基线跑 33s = 建立 + 8s
    /// 空等 + 收尾）。把宽容分支去掉、改成真正的断言后，本用例在**进程内 agent**
    /// 形态下确实收不到事件，而同一个链路（Java → agent WatchProxy → coord_client →
    /// server）在真实集群下由 `java-example` 的 `WatchAdvancedTest` / `WatchIntegrationTest`
    /// （6 个带真实断言的用例，跑在 CI 的 `java-example-it` job 里）**已验证通过与投递**。
    ///
    /// 因此保留断言并显式 `#[ignore]`：不静默通过、也不静默删除。待排查的是**本用例的
    /// 进程内 agent 装配**（`WatchProxy` 已确认 `inner = Some` 且已向上游订阅，但
    /// 上游 `coord_client` 收不到事件），见
    /// `docs/production/remaining-known-gaps.md` 的"进程内 agent watch 探针"。
    #[ignore = "in-process agent watch probe never delivered events (pre-existing); the same path 
                is covered with assertions by java-example WatchAdvancedTest/WatchIntegrationTest 
                in the java-example-it CI job"]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_agent_watch_single_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("coord=info")
            .try_init();

        let (server_addr, _shutdown_tx, _grpc, _raft, _tmpdir) = start_test_server().await;
        tracing::info!("Watch test: server running on {}", server_addr);

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

        // 就绪轮询：agent 冷启动约 1s，固定 sleep 会偶发 Connection refused
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            match tokio::net::TcpStream::connect(&agent_addr).await {
                Ok(_) => break,
                Err(_) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => panic!("agent never became ready at {agent_addr}: {e}"),
            }
        }

        let channel = tonic::transport::Endpoint::from_shared(format!("http://{agent_addr}"))
            .unwrap()
            .connect()
            .await
            .expect("connect to agent");

        // 使用精确 key（与后续 Put 的 key 完全一致）
        let watch_key = b"/agent/watch/exact-key";

        // 1. 通过 Agent 创建 Watch（精确 key 匹配）
        let mut watch_client = WatchClient::new(channel.clone());

        let (req_tx, req_rx) = tokio::sync::mpsc::channel::<WatchRequest>(2);
        let stream_in = tokio_stream::wrappers::ReceiverStream::new(req_rx);

        // 发送 Create 请求（精确 key，range_end 为空 = 精确匹配）
        req_tx
            .send(WatchRequest {
                request: Some(coord_proto::watch::watch_request::Request::Create(
                    WatchCreateRequest {
                        key: watch_key.to_vec(),
                        range_end: vec![], // 空 = 精确 key 匹配
                        start_revision: 0,
                        prev_kv: false,
                    },
                )),
            })
            .await
            .unwrap();

        let watch_resp = watch_client.watch(tonic::Request::new(stream_in)).await;

        assert!(
            watch_resp.is_ok(),
            "Watch should succeed through agent: {watch_resp:?}"
        );

        let mut resp_stream = watch_resp.unwrap().into_inner();

        // 等待 Watch 在 Server 侧完成注册
        tokio::time::sleep(Duration::from_millis(500)).await;

        // 2. 通过 Agent 写入与 Watch 完全相同的 key
        let mut kv_client = KvClient::new(channel);
        kv_client
            .put(PutRequest {
                key: watch_key.to_vec(),
                value: b"watch-value-42".to_vec(),
                ..Default::default()
            })
            .await
            .expect("KV Put through agent should succeed");

        tracing::info!("Watch test: Put completed, waiting for watch event...");

        // 3. 等待 Watch 事件（最多 8 秒）
        //
        // 第四轮回归修复：这里**必须**在超时时失败，而不是"warn 一下继续"。
        // 旧版本在超时/出错分支里只 `tracing::warn!` 然后正常返回 —— 于是
        // "watch 一个事件都收不到"（本轮实测到的真实缺陷：流式请求 body 被鉴权层缓存
        // 导致请求永不转发）在测试里表现为**通过**。这个宽容分支正是该缺陷能溜过
        // 门禁的原因，故删除。
        //
        // 为了不把"上游 watch 尚未注册完成"误判成缺陷，用**重复 put** 覆盖注册延迟：
        // 每次 put 都会推进 revision 并产生事件，因此只要 watch 最终注册成功就一定会
        // 收到。超时耗尽才判定失败。
        let mut received: Option<coord_proto::watch::WatchResponse> = None;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        while tokio::time::Instant::now() < deadline {
            let _ = kv_client
                .put(PutRequest {
                    key: watch_key.to_vec(),
                    value: b"watch-value-42".to_vec(),
                    ..Default::default()
                })
                .await;
            match tokio::time::timeout(Duration::from_millis(500), resp_stream.message()).await {
                Ok(Ok(Some(resp))) => {
                    tracing::info!(
                        "Watch test: received event with {} events",
                        resp.events.len()
                    );
                    received = Some(resp);
                    break;
                }
                Ok(Ok(None)) => panic!("watch stream ended before any event was delivered"),
                Ok(Err(e)) => panic!("watch stream error: {e}"),
                Err(_) => {} // 本窗口内无事件：重试
            }
        }

        let resp = received.expect(
            "watch delivered no event within 15s despite repeated puts — the subscription is not \
             reaching the watch handler (regression class: a streaming request body being buffered \
             or dropped by a middleware layer)",
        );
        assert!(
            !resp.events.is_empty(),
            "Should contain at least one watch event"
        );
        let found = resp
            .events
            .iter()
            .any(|e| e.kvs.iter().any(|kv| kv.key == watch_key));
        assert!(found, "Watch events should contain the put key");

        agent_handle.abort();
    }
}
