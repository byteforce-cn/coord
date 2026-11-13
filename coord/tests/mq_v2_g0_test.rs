//! MQ v2 M0-4 验收（issue #332）：
//! - `coord.mq.v2` 服务**始终注册**、reflection 可见；
//! - `[mq_v2].enabled=false`（默认）时每个 RPC 返回 `FAILED_PRECONDITION`
//!   （不是 `UNIMPLEMENTED`/`UNAVAILABLE`），trailer 带结构化码；
//! - 开启但能力未交付（M0）同样 `FAILED_PRECONDITION`，消息点名契约包；
//! - 门禁放行的能力落到 `UNIMPLEMENTED`（实现进度问题，与配置问题可区分）；
//! - v1 数据面在两种状态下都不受影响。

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use coord_core::error_code::error_code_of;
    use coord_core::storage::StorageBackend;
    use coord_core::types::StorageConfig;
    use coord_proto::kv::kv_client::KvClient;
    use coord_proto::kv::kv_server::KvServer;
    use coord_proto::kv::{PutRequest, RangeRequest};
    use coord_proto::mq::v2 as mq_v2;
    use coord_proto::mq::v2::mq_client::MqClient;
    use coord_proto::mq::v2::mq_server::MqServer;
    use coord_server::mq_v2::{MqV2Capability, MqV2RuntimeConfig, MqV2State};
    use coord_server::server::CoordNode;
    use coord_server::storage::mvcc::MvccStorage;
    use coord_server::storage::redb_backend::RedbBackend;
    use coord_server::watch::WatchDispatcher;
    use tokio::net::TcpListener;
    use tonic::transport::{Channel, Server};
    use tonic::Code;

    async fn start_server(state: MqV2State) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let tmpdir = tempfile::tempdir().unwrap();
        let data_dir = tmpdir.path().to_path_buf();
        let backend = RedbBackend::open(&data_dir, &StorageConfig::default()).unwrap();
        let mvcc = Arc::new(MvccStorage::new(backend).unwrap());

        let mut node = CoordNode::new(Arc::clone(&mvcc));
        node.watch_dispatcher = Some(Arc::new(WatchDispatcher::start()));
        node.set_mq_v2_state(state);
        let node = Arc::new(node);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let reflection = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(coord_proto::FILE_DESCRIPTOR_SET)
            .build_v1()
            .unwrap();

        let handle = tokio::spawn(async move {
            Server::builder()
                .add_service(KvServer::from_arc(Arc::clone(&node)))
                .add_service(MqServer::from_arc(Arc::clone(&node)))
                .add_service(reflection)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        tokio::time::sleep(Duration::from_millis(150)).await;
        (addr, handle)
    }

    /// 每个 handler 都必须先过同一道门禁；这里覆盖五个能力面各一个 RPC。
    async fn assert_mq_v2_gated(addr: SocketAddr, expect_message: &str) {
        let mut mq = MqClient::connect(format!("http://{addr}")).await.unwrap();

        let err = mq
            .create_topic(mq_v2::CreateTopicRequest {
                topic: "orders".into(),
                partitions: 4,
                ..Default::default()
            })
            .await
            .expect_err("CreateTopic must be gated");
        assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");
        assert!(
            err.message().contains(expect_message),
            "message should contain {expect_message:?}: {:?}",
            err.message()
        );
        assert_eq!(
            error_code_of(&err).as_deref(),
            Some("FAILED_PRECONDITION"),
            "structured trailer code must survive the wire"
        );

        for err in [
            mq.publish(mq_v2::PublishRequest {
                topic: "orders".into(),
                partition: 0,
                payload: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .expect_err("Publish must be gated"),
            mq.poll(mq_v2::PollRequest {
                topic: "orders".into(),
                partition: 0,
                ..Default::default()
            })
            .await
            .expect_err("Poll must be gated"),
            mq.join_group(mq_v2::JoinGroupRequest {
                group: "g".into(),
                topics: vec!["orders".into()],
                ..Default::default()
            })
            .await
            .expect_err("JoinGroup must be gated"),
            mq.move_to_dlq(mq_v2::MoveToDlqRequest {
                topic: "orders".into(),
                partition: 0,
                offset: 0,
                reason: "poison".into(),
                ..Default::default()
            })
            .await
            .expect_err("MoveToDlq must be gated"),
            mq.subscribe(mq_v2_subscribe_request())
                .await
                .expect_err("Subscribe must be gated"),
        ] {
            assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");
            assert_eq!(
                error_code_of(&err).as_deref(),
                Some("FAILED_PRECONDITION"),
                "every gated RPC carries the structured code: {err:?}"
            );
        }
    }

    fn mq_v2_subscribe_request() -> mq_v2::SubscribeRequest {
        mq_v2::SubscribeRequest {
            topic: "orders".into(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn disabled_service_is_gated_and_v1_unchanged() {
        let (addr, handle) = start_server(MqV2State::disabled()).await;

        // v1 路径不受影响（MQ v1 在 agent 侧；这里验证服务端的 v1 数据面）。
        let mut kv = KvClient::connect(format!("http://{addr}")).await.unwrap();
        kv.put(PutRequest {
            key: b"m0/key".to_vec(),
            value: b"v".to_vec(),
            ..Default::default()
        })
        .await
        .expect("v1 Put must keep working");
        let resp = kv
            .range(RangeRequest {
                key: b"m0/key".to_vec(),
                range_end: b"m0/key\x00".to_vec(),
                ..Default::default()
            })
            .await
            .expect("v1 Range must keep working");
        assert_eq!(resp.into_inner().kvs.len(), 1);

        assert_mq_v2_gated(addr, "mq_v2 disabled").await;
        handle.abort();
    }

    #[tokio::test]
    async fn enabled_without_delivered_capability_is_still_gated() {
        let (addr, handle) = start_server(MqV2State::new(true, MqV2RuntimeConfig::default())).await;
        assert_mq_v2_gated(addr, "not implemented").await;
        handle.abort();
    }

    /// M0–M4 已把 21 个 RPC 全部接线：门禁放行后 handler 真的被执行（不再是
    /// `UNIMPLEMENTED`）；未交付的能力仍然按 gate 拒绝。
    #[tokio::test]
    async fn delivered_capability_reaches_the_wired_handler() {
        let (addr, handle) = start_server(
            MqV2State::new(true, MqV2RuntimeConfig::default())
                .with_implemented([MqV2Capability::Topic]),
        )
        .await;
        let mut mq = MqClient::connect(format!("http://{addr}")).await.unwrap();

        // `CreateTopic` 已接线：没有 region 0 raft 装配 ⇒ FAILED_PRECONDITION
        // （而不是"handler 未实现"）。
        let err = mq
            .create_topic(mq_v2::CreateTopicRequest {
                topic: "orders".into(),
                partitions: 1,
                ..Default::default()
            })
            .await
            .expect_err("no raft assembly");
        assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");
        assert!(
            !err.message().contains("not implemented"),
            "a wired handler must not report UNIMPLEMENTED: {err:?}"
        );

        // 未交付的能力仍然按配置/gate 拒绝。
        let err = mq
            .publish(mq_v2::PublishRequest {
                topic: "orders".into(),
                payload: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .expect_err("Publish capability is not delivered in this state");
        assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");
        handle.abort();
    }

    /// 关闭态也必须 reflection 可见：客户端在连接阶段就能发现"集群没有 MQ v2"。
    #[tokio::test]
    async fn reflection_lists_mq_v2_even_when_disabled() {
        let (addr, handle) = start_server(MqV2State::disabled()).await;

        let channel = Channel::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut reflection =
            tonic_reflection::pb::v1::server_reflection_client::ServerReflectionClient::new(
                channel,
            );
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tx.send(tonic_reflection::pb::v1::ServerReflectionRequest {
            host: String::new(),
            message_request: Some(
                tonic_reflection::pb::v1::server_reflection_request::MessageRequest::ListServices(
                    String::new(),
                ),
            ),
        })
        .await
        .unwrap();
        drop(tx);
        let mut stream = reflection
            .server_reflection_info(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await
            .unwrap()
            .into_inner();
        let msg = stream
            .message()
            .await
            .unwrap()
            .expect("reflection must answer ListServices");
        let names: Vec<String> = match msg.message_response {
            Some(
                tonic_reflection::pb::v1::server_reflection_response::MessageResponse::ListServicesResponse(
                    resp,
                ),
            ) => resp.service.into_iter().map(|s| s.name).collect(),
            other => panic!("unexpected reflection response: {other:?}"),
        };
        assert!(
            names.iter().any(|n| n == "coord.mq.v2.MQ"),
            "reflection missing coord.mq.v2.MQ: {names:?}"
        );
        handle.abort();
    }
}
