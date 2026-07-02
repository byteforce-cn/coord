// End-to-end integration test: start server, connect client, run CRUD operations
//
// Tests the full gRPC stack: KV (Put/Get/Delete/Range), Txn (CAS), and Status.

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use coord_core::storage::StorageBackend;
    use coord_core::types::StorageConfig;
    use coord_server::server::CoordNode;
    use coord_server::storage::mvcc::MvccStorage;
    use coord_server::storage::redb_backend::RedbBackend;
    use coord_server::watch::WatchDispatcher;
    use coord_proto::kv::kv_client::KvClient;
    use coord_proto::kv::{DeleteRequest, PutRequest, RangeRequest};
    use coord_proto::maintenance::maintenance_client::MaintenanceClient;
    use coord_proto::maintenance::StatusRequest;
    use coord_proto::txn::txn_client::TxnClient;
    use coord_proto::txn::{
        compare::Target, Compare, RequestOp, TxnRequest,
    };
    use coord_proto::kv::kv_server::KvServer;
    use coord_proto::txn::txn_server::TxnServer;
    use coord_proto::maintenance::maintenance_server::MaintenanceServer;
    use tonic::transport::{Channel, Server};
    use tokio::net::TcpListener;

    /// Start a test server on a random port, return (addr, join_handle)
    async fn start_test_server() -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let tmpdir = tempfile::tempdir().unwrap();
        let data_dir = tmpdir.path().to_path_buf();

        // Initialize storage
        let config = StorageConfig::default();
        let backend = RedbBackend::open(&data_dir, &config).unwrap();
        let mvcc = Arc::new(MvccStorage::new(backend).unwrap());

        // Build CoordNode
        let mut node = CoordNode::new(Arc::clone(&mvcc));
        let watch = Arc::new(WatchDispatcher::start());
        node.watch_dispatcher = Some(watch);
        let node = Arc::new(node);

        // Bind to random port
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let kv_svc = KvServer::from_arc(Arc::clone(&node));
        let txn_svc = TxnServer::from_arc(Arc::clone(&node));
        let maint_svc = MaintenanceServer::from_arc(Arc::clone(&node));

        let handle = tokio::spawn(async move {
            Server::builder()
                .add_service(kv_svc)
                .add_service(txn_svc)
                .add_service(maint_svc)
                .serve_with_incoming(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                )
                .await
                .unwrap();
        });

        // Wait for server to be ready
        tokio::time::sleep(Duration::from_millis(100)).await;

        (addr, handle)
    }

    async fn connect(addr: SocketAddr) -> (KvClient<Channel>, TxnClient<Channel>, MaintenanceClient<Channel>) {
        let endpoint = format!("http://{}", addr);
        let channel = Channel::from_shared(endpoint)
            .unwrap()
            .connect()
            .await
            .unwrap();
        (
            KvClient::new(channel.clone()),
            TxnClient::new(channel.clone()),
            MaintenanceClient::new(channel),
        )
    }

    #[tokio::test]
    async fn test_e2e_put_get() {
        let (addr, _handle) = start_test_server().await;
        let (mut kv, _, _) = connect(addr).await;

        // Put
        let put_resp = kv
            .put(PutRequest {
                key: b"hello".to_vec(),
                value: b"world".to_vec(),
                lease_id: 0,
                prev_kv: false,
                request_id: vec![],
            })
            .await
            .unwrap()
            .into_inner();

        assert!(put_resp.revision > 0, "revision should be > 0");

        // Get
        let range_resp = kv
            .range(RangeRequest {
                key: b"hello".to_vec(),
                range_end: vec![],
                limit: 0,
                revision: 0,
                keys_only: false,
                count_only: false,
            })
            .await
            .unwrap()
            .into_inner();

        assert_eq!(range_resp.kvs.len(), 1);
        assert_eq!(range_resp.kvs[0].key, b"hello");
        assert_eq!(range_resp.kvs[0].value, b"world");
    }

    #[tokio::test]
    async fn test_e2e_delete() {
        let (addr, _handle) = start_test_server().await;
        let (mut kv, _, _) = connect(addr).await;

        // Put then Delete
        kv.put(PutRequest {
            key: b"temp".to_vec(),
            value: b"data".to_vec(),
            lease_id: 0,
            prev_kv: false,
            request_id: vec![],
        })
        .await
        .unwrap();

        let del_resp = kv
            .delete(DeleteRequest {
                key: b"temp".to_vec(),
                range_end: vec![],
                prev_kv: false,
                request_id: vec![],
            })
            .await
            .unwrap()
            .into_inner();

        assert_eq!(del_resp.deleted, 1);

        // Get should return empty
        let range_resp = kv
            .range(RangeRequest {
                key: b"temp".to_vec(),
                range_end: vec![],
                limit: 0,
                revision: 0,
                keys_only: false,
                count_only: false,
            })
            .await
            .unwrap()
            .into_inner();

        assert!(range_resp.kvs.is_empty());
    }

    #[tokio::test]
    async fn test_e2e_range_prefix() {
        let (addr, _handle) = start_test_server().await;
        let (mut kv, _, _) = connect(addr).await;

        // Put keys with shared prefix
        for i in 0..5u8 {
            kv.put(PutRequest {
                key: format!("/app/config/{}", i).into_bytes(),
                value: format!("val{}", i).into_bytes(),
                lease_id: 0,
                prev_kv: false,
                request_id: vec![],
            })
            .await
            .unwrap();
        }

        // Range scan with prefix
        let range_resp = kv
            .range(RangeRequest {
                key: b"/app/config/".to_vec(),
                range_end: b"/app/config0".to_vec(), // range_end > prefix for prefix scan
                limit: 0,
                revision: 0,
                keys_only: false,
                count_only: false,
            })
            .await
            .unwrap()
            .into_inner();

        assert!(
            range_resp.kvs.len() >= 1,
            "expected at least 1 key, got {}",
            range_resp.kvs.len()
        );
    }

    #[tokio::test]
    async fn test_e2e_txn_cas() {
        let (addr, _handle) = start_test_server().await;
        let (mut kv, mut txn, _) = connect(addr).await;

        // Put initial value
        kv.put(PutRequest {
            key: b"counter".to_vec(),
            value: 1u64.to_be_bytes().to_vec(),
            lease_id: 0,
            prev_kv: false,
            request_id: vec![],
        })
        .await
        .unwrap();

        // CAS: compare version=1 (first write), then update
        let txn_resp = txn
            .txn(TxnRequest {
                compare: vec![Compare {
                    result: coord_proto::txn::compare::CompareResult::Equal as i32,
                    target: Target::Version as i32,
                    key: b"counter".to_vec(),
                    target_value: Some(
                        coord_proto::txn::compare::TargetValue::Version(1),
                    ),
                }],
                success: vec![RequestOp {
                    op: Some(coord_proto::txn::request_op::Op::RequestPut(
                        PutRequest {
                            key: b"counter".to_vec(),
                            value: 2u64.to_be_bytes().to_vec(),
                            lease_id: 0,
                            prev_kv: false,
                            request_id: vec![],
                        },
                    )),
                }],
                failure: vec![],
                request_id: vec![],
            })
            .await
            .unwrap()
            .into_inner();

        assert!(txn_resp.succeeded, "CAS should succeed on version=1");

        // Verify value updated
        let range_resp = kv
            .range(RangeRequest {
                key: b"counter".to_vec(),
                range_end: vec![],
                limit: 0,
                revision: 0,
                keys_only: false,
                count_only: false,
            })
            .await
            .unwrap()
            .into_inner();

        let val = u64::from_be_bytes(
            range_resp.kvs[0].value[..8].try_into().unwrap(),
        );
        assert_eq!(val, 2, "value should be updated to 2");

        // CAS: compare version=1 again (should fail since version is now 2)
        let txn_resp2 = txn
            .txn(TxnRequest {
                compare: vec![Compare {
                    result: coord_proto::txn::compare::CompareResult::Equal as i32,
                    target: Target::Version as i32,
                    key: b"counter".to_vec(),
                    target_value: Some(
                        coord_proto::txn::compare::TargetValue::Version(1),
                    ),
                }],
                success: vec![RequestOp {
                    op: Some(coord_proto::txn::request_op::Op::RequestPut(
                        PutRequest {
                            key: b"counter".to_vec(),
                            value: 99u64.to_be_bytes().to_vec(),
                            lease_id: 0,
                            prev_kv: false,
                            request_id: vec![],
                        },
                    )),
                }],
                failure: vec![],
                request_id: vec![],
            })
            .await
            .unwrap()
            .into_inner();

        assert!(!txn_resp2.succeeded, "CAS should fail on version=1 when version is 2");
    }

    #[tokio::test]
    async fn test_e2e_status() {
        let (addr, _handle) = start_test_server().await;
        let (_, _, mut maint) = connect(addr).await;

        let status = maint
            .status(StatusRequest {})
            .await
            .unwrap()
            .into_inner();

        assert!(status.revision >= 0);
        assert_eq!(status.seal_status, "unsealed");
    }
}
