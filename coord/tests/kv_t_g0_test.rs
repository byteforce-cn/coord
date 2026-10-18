//! KV-T G0 验收（issue #133/#134）：
//! - v2 服务始终注册、reflection 可见；
//! - `[kv_t].enabled=false` 时每个 v2 RPC 返回 `FAILED_PRECONDITION`（不是
//!   `UNIMPLEMENTED`/`UNAVAILABLE`）；
//! - 开启但能力未交付（G0）同样 `FAILED_PRECONDITION`，消息点名契约包；
//! - v1 服务在两种状态下都不受影响。

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
    use coord_proto::kv::v2::kv_client::KvClient as KvV2Client;
    use coord_proto::kv::v2::kv_server::KvServer as KvV2Server;
    use coord_proto::kv::v2::RangeRequest as V2RangeRequest;
    use coord_proto::kv::{PutRequest, RangeRequest};
    use coord_proto::lease::v2::lease_server::LeaseServer as LeaseV2Server;
    use coord_proto::storage::v2::storage_server::StorageServer as StorageV2Server;
    use coord_proto::tso::v2::tso_client::TsoClient;
    use coord_proto::tso::v2::tso_server::TsoServer;
    use coord_proto::tso::v2::GetTsRequest;
    use coord_proto::txn::v2::txn_server::TxnServer as TxnV2Server;
    use coord_proto::watch::v2::watch_server::WatchServer as WatchV2Server;
    use coord_server::kvt::{KvTRuntimeConfig, KvTState};
    use coord_server::server::CoordNode;
    use coord_server::storage::mvcc::MvccStorage;
    use coord_server::storage::redb_backend::RedbBackend;
    use coord_server::watch::WatchDispatcher;
    use tokio::net::TcpListener;
    use tonic::transport::{Channel, Server};
    use tonic::Code;

    async fn start_server(kv_t_enabled: bool) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let tmpdir = tempfile::tempdir().unwrap();
        let data_dir = tmpdir.path().to_path_buf();
        let backend = RedbBackend::open(&data_dir, &StorageConfig::default()).unwrap();
        let mvcc = Arc::new(MvccStorage::new(backend).unwrap());

        let mut node = CoordNode::new(Arc::clone(&mvcc));
        node.watch_dispatcher = Some(Arc::new(WatchDispatcher::start()));
        node.set_kvt_state(KvTState::new(kv_t_enabled, KvTRuntimeConfig::default()));
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
                .add_service(KvV2Server::from_arc(Arc::clone(&node)))
                .add_service(TsoServer::from_arc(Arc::clone(&node)))
                .add_service(TxnV2Server::from_arc(Arc::clone(&node)))
                .add_service(WatchV2Server::from_arc(Arc::clone(&node)))
                .add_service(LeaseV2Server::from_arc(Arc::clone(&node)))
                .add_service(StorageV2Server::from_arc(Arc::clone(&node)))
                .add_service(reflection)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        tokio::time::sleep(Duration::from_millis(150)).await;
        (addr, handle)
    }

    async fn assert_v2_gated(addr: SocketAddr, expect_message: &str) {
        let mut kv = KvV2Client::connect(format!("http://{addr}")).await.unwrap();
        let err = kv
            .range(V2RangeRequest {
                key: b"k".to_vec(),
                ..Default::default()
            })
            .await
            .expect_err("v2 KV must be gated");
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

        let mut tso = TsoClient::connect(format!("http://{addr}")).await.unwrap();
        let err = tso
            .get_ts(GetTsRequest {})
            .await
            .expect_err("v2 TSO must be gated");
        assert_eq!(err.code(), Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn v1_unchanged_and_v2_rejected_when_disabled() {
        let (addr, handle) = start_server(false).await;

        // v1 路径不受影响。
        let mut kv = KvClient::connect(format!("http://{addr}")).await.unwrap();
        kv.put(PutRequest {
            key: b"g0/key".to_vec(),
            value: b"v".to_vec(),
            ..Default::default()
        })
        .await
        .expect("v1 Put must keep working");
        let resp = kv
            .range(RangeRequest {
                key: b"g0/key".to_vec(),
                range_end: b"g0/key\x00".to_vec(),
                ..Default::default()
            })
            .await
            .expect("v1 Range must keep working");
        assert_eq!(resp.into_inner().kvs.len(), 1);

        assert_v2_gated(addr, "[kv_t].enabled=false").await;
        handle.abort();
    }

    #[tokio::test]
    async fn v2_enabled_but_g0_capabilities_still_gated() {
        let (addr, handle) = start_server(true).await;
        assert_v2_gated(addr, "not implemented").await;
        handle.abort();
    }

    #[tokio::test]
    async fn reflection_lists_v2_services() {
        let (addr, handle) = start_server(false).await;

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
        for expected in [
            "coord.kv.v2.KV",
            "coord.tso.v2.TSO",
            "coord.txn.v2.Txn",
            "coord.watch.v2.Watch",
            "coord.lease.v2.Lease",
            "coord.storage.v2.Storage",
        ] {
            assert!(
                names.iter().any(|n| n == expected),
                "reflection missing {expected}: {names:?}"
            );
        }
        handle.abort();
    }
}
