//! KV-T G1（#136）TSO RPC 边界：
//! - 关闭/能力未交付 → `FAILED_PRECONDITION`（不是 UNIMPLEMENTED）；
//! - 开启且 TSO 已交付但没有 region 0 raft 装配 → `FAILED_PRECONDITION` 且点名 raft；
//! - 超上限批大小 → `INVALID_ARGUMENT`（不静默截断）。

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;

    use coord_core::storage::StorageBackend;
    use coord_core::types::StorageConfig;
    use coord_proto::tso::v2::tso_client::TsoClient;
    use coord_proto::tso::v2::tso_server::TsoServer;
    use coord_proto::tso::v2::{AllocateRequest, GetTsRequest};
    use coord_server::kvt::{KvTCapability, KvTRuntimeConfig, KvTState};
    use coord_server::server::CoordNode;
    use coord_server::storage::mvcc::MvccStorage;
    use coord_server::storage::redb_backend::RedbBackend;
    use tonic::transport::{Channel, Server};
    use tonic::Code;

    struct TestServer {
        addr: SocketAddr,
        _handle: tokio::task::JoinHandle<()>,
        _tmpdir: tempfile::TempDir,
    }

    async fn start_server(enabled: bool, with_tso: bool) -> TestServer {
        let tmpdir = tempfile::tempdir().unwrap();
        let backend = RedbBackend::open(tmpdir.path(), &StorageConfig::default()).unwrap();
        let mvcc = Arc::new(MvccStorage::new(backend).unwrap());

        let mut node = CoordNode::new(Arc::clone(&mvcc));
        let state = if with_tso {
            KvTState::new(enabled, KvTRuntimeConfig::default())
                .with_implemented([KvTCapability::Tso])
        } else {
            KvTState::new(enabled, KvTRuntimeConfig::default())
        };
        node.set_kvt_state(state);
        let node = Arc::new(node);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            Server::builder()
                .add_service(TsoServer::from_arc(node))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        TestServer {
            addr,
            _handle: handle,
            _tmpdir: tmpdir,
        }
    }

    async fn client(addr: SocketAddr) -> TsoClient<Channel> {
        TsoClient::connect(format!("http://{addr}")).await.unwrap()
    }

    #[tokio::test]
    async fn test_disabled_and_unimplemented_are_failed_precondition() {
        let disabled = start_server(false, true).await;
        let err = client(disabled.addr)
            .await
            .allocate(AllocateRequest { batch_size: 8 })
            .await
            .expect_err("disabled must reject");
        assert_eq!(err.code(), Code::FailedPrecondition);
        assert!(err.message().contains("[kv_t].enabled=false"));

        let unimplemented = start_server(true, false).await;
        let err = client(unimplemented.addr)
            .await
            .get_ts(GetTsRequest {})
            .await
            .expect_err("capability not delivered must reject");
        assert_eq!(err.code(), Code::FailedPrecondition);
        assert!(err.message().contains("coord.tso.v2"));
    }

    #[tokio::test]
    async fn test_batch_upper_bound_is_rejected_not_truncated() {
        let server = start_server(true, true).await;
        let err = client(server.addr)
            .await
            .allocate(AllocateRequest { batch_size: 65_537 })
            .await
            .expect_err("oversized batch must be rejected");
        assert_eq!(err.code(), Code::InvalidArgument);
        assert!(err.message().contains("batch_size"));
    }

    #[tokio::test]
    async fn test_enabled_without_region0_raft_points_at_assembly() {
        let server = start_server(true, true).await;
        let mut client = client(server.addr).await;
        for err in [
            client
                .allocate(AllocateRequest { batch_size: 8 })
                .await
                .expect_err("no raft assembly"),
            client
                .get_ts(GetTsRequest {})
                .await
                .expect_err("no raft assembly"),
        ] {
            assert_eq!(err.code(), Code::FailedPrecondition);
            assert!(
                err.message().contains("region 0 raft"),
                "must name the missing assembly: {}",
                err.message()
            );
        }
    }
}
