// G-EV-1: event 持久化游标与补投端到端测试（ADR-0010）
//
// 契约口径（event.proto 头注）：
// - 默认 `Subscribe`（cursor 空）= 实时推送，断线不补投（既有语义不变）；
// - 显式 `cursor`（上次收到的 seq）= 保留窗口内按位点补投，再转实时；
// - `seq` 全局单调（跨重启不重置）——消费方持久化最大 seq 作为游标。
//
// 验收锚点：断线重连后可按位点补投；投递窗口与语义在契约中明示。

mod common;

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use coord_agent::{AgentConfig, AgentServer};
    use coord_proto::event::v1::event_client::EventClient;
    use coord_proto::event::v1::{EventPublishRequest, EventSubscribeRequest};

    // 共享夹具（单一事实来源）：find_port()/start_test_server() 收敛在
    // tests/common/mod.rs，不得在本文件里再复制一份。
    use crate::common::{find_port, start_test_server, wait_tcp_ready};

    type EventGrpc = EventClient<tonic::transport::Channel>;

    async fn start_agent(server_addr: &str, data_dir: &std::path::Path) -> (String, tokio::task::JoinHandle<()>) {
        let agent_port = find_port();
        let agent_addr = format!("127.0.0.1:{agent_port}");
        let mut config = AgentConfig {
            agent_addr: agent_addr.clone(),
            http_addr: format!("127.0.0.1:{}", find_port()),
            data_dir: data_dir.to_string_lossy().to_string(),
            static_peers: vec![server_addr.to_string()],
            ..Default::default()
        };
        config.services.event_notification = true;

        let server = AgentServer::new(config);
        let handle = tokio::spawn(async move {
            server.serve().await.unwrap();
        });
        wait_tcp_ready(&agent_addr, Duration::from_secs(30)).await;
        (agent_addr, handle)
    }

    async fn connect(addr: &str) -> EventGrpc {
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .expect("connect to agent");
        EventClient::new(channel)
    }

    async fn publish(client: &mut EventGrpc, event_type: &str, payload: &[u8]) -> String {
        client
            .publish(EventPublishRequest {
                event_type: event_type.to_string(),
                source: "it".to_string(),
                data: payload.to_vec(),
                data_content_type: "application/json".to_string(),
                subject: String::new(),
            })
            .await
            .expect("publish")
            .into_inner()
            .event_id
    }

    /// 从流里收下一条（带超时，失败即断言 —— 不静默通过）
    async fn next_event(
        stream: &mut tonic::Streaming<coord_proto::event::v1::CloudEventMessage>,
    ) -> coord_proto::event::v1::CloudEventMessage {
        tokio::time::timeout(Duration::from_secs(5), stream.message())
            .await
            .expect("event within 5s")
            .expect("stream ok")
            .expect("stream not ended")
    }

    /// G-EV-1 验收：默认不补投；显式 cursor 补投且重连后不丢事件
    #[tokio::test(flavor = "multi_thread")]
    async fn test_event_cursor_replay_across_reconnect() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("coord=info")
            .try_init();

        let (server_addr, _sd, _g, _r, _tmp) = start_test_server().await;
        let dir = tempfile::tempdir().unwrap();
        let (addr, handle) = start_agent(&server_addr, dir.path()).await;
        let mut client = connect(&addr).await;

        // 预置 2 条事件（seq 1、2）
        publish(&mut client, "t.a", b"e1").await;
        publish(&mut client, "t.a", b"e2").await;

        // 1) 默认订阅（cursor 空）⇒ 不补投既有事件；新事件实时到达 seq=3
        let mut live = client
            .subscribe(EventSubscribeRequest {
                event_type: "t.a".to_string(),
                cursor: String::new(),
            })
            .await
            .expect("subscribe live")
            .into_inner();
        publish(&mut client, "t.a", b"e3").await;
        let e3 = next_event(&mut live).await;
        assert_eq!(e3.data, b"e3".to_vec());
        assert_eq!(e3.seq, 3, "seq 全局单调且对消费方可见");

        // 2) 「断线」：丢弃流；期间发布 e4、e5
        drop(live);
        publish(&mut client, "t.a", b"e4").await;
        publish(&mut client, "t.a", b"e5").await;

        // 3) 重连并携带位点 cursor=3 ⇒ 补投 e4、e5（seq 4、5），随后实时 e6
        let mut resumed = client
            .subscribe(EventSubscribeRequest {
                event_type: "t.a".to_string(),
                cursor: "3".to_string(),
            })
            .await
            .expect("subscribe with cursor")
            .into_inner();
        let e4 = next_event(&mut resumed).await;
        let e5 = next_event(&mut resumed).await;
        assert_eq!(
            (e4.data.as_slice(), e4.seq, e5.data.as_slice(), e5.seq),
            (&b"e4"[..], 4, &b"e5"[..], 5),
            "断线窗口内的事件必须按位点补投且保序"
        );
        publish(&mut client, "t.a", b"e6").await;
        let e6 = next_event(&mut resumed).await;
        assert_eq!(
            (e6.data.as_slice(), e6.seq),
            (&b"e6"[..], 6),
            "补投后必须无缝转入实时投递（不丢不重）"
        );

        // 4) 从 0 全量补投（保留窗口内）：1..=6 按序
        let mut from_zero = client
            .subscribe(EventSubscribeRequest {
                event_type: String::new(),
                cursor: "0".to_string(),
            })
            .await
            .expect("subscribe from zero")
            .into_inner();
        let mut seqs = Vec::new();
        for _ in 0..6 {
            seqs.push(next_event(&mut from_zero).await.seq);
        }
        assert_eq!(seqs, vec![1, 2, 3, 4, 5, 6], "补投按 seq 升序且覆盖窗口");

        // 5) 类型过滤在补投路径同样生效：仅收 "t.b"
        publish(&mut client, "t.b", b"b1").await;
        let mut filtered = client
            .subscribe(EventSubscribeRequest {
                event_type: "t.b".to_string(),
                cursor: "0".to_string(),
            })
            .await
            .expect("subscribe filtered")
            .into_inner();
        let b1 = next_event(&mut filtered).await;
        assert_eq!(b1.r#type, "t.b");
        assert_eq!(b1.data, b"b1".to_vec());
        assert_eq!(b1.seq, 7);

        handle.abort();
    }
}
