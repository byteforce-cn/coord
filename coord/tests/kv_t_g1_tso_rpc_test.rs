//! KV-T G1（#136/#137）TSO RPC 边界：
//! - 关闭/能力未交付 → `FAILED_PRECONDITION`（不是 UNIMPLEMENTED）；
//! - 开启且 TSO 已交付但没有 region 0 raft 装配 → `FAILED_PRECONDITION` 且点名 raft；
//! - 超上限批大小 → `INVALID_ARGUMENT`（不静默截断）；
//! - `GetTs` 走本地批次缓存（一批 8 个只申请一次）；follower → `NOT_LEADER`。

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Arc;

    use async_trait::async_trait;
    use coord_core::error::Result;
    use coord_core::error_code::error_code_of;
    use coord_core::storage::StorageBackend;
    use coord_core::types::StorageConfig;
    use coord_proto::tso::v2::tso_client::TsoClient as TsoRpcClient;
    use coord_proto::tso::v2::tso_server::TsoServer;
    use coord_proto::tso::v2::{AllocateRequest, GetTsRequest};
    use coord_server::kvt::{KvTCapability, KvTRuntimeConfig, KvTState};
    use coord_server::raft::tso::{TsoBatch, TsoClient, TsoSource};
    use coord_server::server::CoordNode;
    use coord_server::storage::mvcc::MvccStorage;
    use coord_server::storage::redb_backend::RedbBackend;
    use tonic::transport::{Channel, Server};
    use tonic::Code;

    struct TestServer {
        addr: SocketAddr,
        _handle: tokio::task::JoinHandle<()>,
        _tmpdir: tempfile::TempDir,
        source: Arc<FakeTso>,
    }

    /// 可控 TSO 源：真实实现要 region 0 raft；这里只验证 RPC 是否走本地缓存。
    struct FakeTso {
        leader: AtomicBool,
        high_water: AtomicU64,
        allocations: AtomicU64,
    }

    impl FakeTso {
        fn new(leader: bool) -> Arc<Self> {
            Arc::new(Self {
                leader: AtomicBool::new(leader),
                high_water: AtomicU64::new(1),
                allocations: AtomicU64::new(0),
            })
        }
    }

    #[async_trait]
    impl TsoSource for FakeTso {
        async fn is_leader(&self) -> bool {
            self.leader.load(Ordering::SeqCst)
        }

        async fn allocate(&self, batch_size: u32) -> Result<TsoBatch> {
            self.allocations.fetch_add(1, Ordering::SeqCst);
            let start = self.high_water.load(Ordering::SeqCst);
            let end = start + u64::from(batch_size);
            self.high_water.store(end, Ordering::SeqCst);
            Ok(TsoBatch {
                start_ts: start,
                end_ts: end,
            })
        }
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

        let (addr, handle) = serve(node).await;
        TestServer {
            addr,
            _handle: handle,
            _tmpdir: tmpdir,
            source: FakeTso::new(false),
        }
    }

    /// 开启 TSO 且注入本地批次缓存；`leader` 控制 fake 源的 leadership。
    async fn start_server_with_tso_client(leader: bool) -> TestServer {
        let tmpdir = tempfile::tempdir().unwrap();
        let backend = RedbBackend::open(tmpdir.path(), &StorageConfig::default()).unwrap();
        let mvcc = Arc::new(MvccStorage::new(backend).unwrap());

        let mut node = CoordNode::new(Arc::clone(&mvcc));
        node.set_kvt_state(
            KvTState::new(true, KvTRuntimeConfig::default()).with_implemented([KvTCapability::Tso]),
        );
        let source = FakeTso::new(leader);
        node.set_tso_client(TsoClient::new(
            Arc::clone(&source) as Arc<dyn TsoSource>,
            8,
            None,
        ));
        let node = Arc::new(node);

        let (addr, handle) = serve(node).await;
        TestServer {
            addr,
            _handle: handle,
            _tmpdir: tmpdir,
            source,
        }
    }

    async fn serve(node: Arc<CoordNode>) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            Server::builder()
                .add_service(TsoServer::from_arc(node))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        (addr, handle)
    }

    async fn client(addr: SocketAddr) -> TsoRpcClient<Channel> {
        TsoRpcClient::connect(format!("http://{addr}"))
            .await
            .unwrap()
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

    #[tokio::test]
    async fn test_get_ts_served_from_local_batch() {
        let server = start_server_with_tso_client(true).await;
        let mut client = client(server.addr).await;

        let mut issued = Vec::new();
        for _ in 0..5 {
            issued.push(
                client
                    .get_ts(GetTsRequest {})
                    .await
                    .unwrap()
                    .into_inner()
                    .ts,
            );
        }
        assert_eq!(issued, (1..=5).collect::<Vec<_>>());
        assert_eq!(
            server.source.allocations.load(Ordering::SeqCst),
            1,
            "GetTs 不应每次过 raft（batch=8）"
        );
    }

    #[tokio::test]
    async fn test_get_ts_on_follower_is_not_leader() {
        let server = start_server_with_tso_client(false).await;
        let err = client(server.addr)
            .await
            .get_ts(GetTsRequest {})
            .await
            .expect_err("follower must reject GetTs");
        assert_eq!(err.code(), Code::Unavailable);
        assert_eq!(error_code_of(&err).as_deref(), Some("NOT_LEADER"));
        assert_eq!(
            server.source.allocations.load(Ordering::SeqCst),
            0,
            "follower 不得申请批次"
        );
    }
}
