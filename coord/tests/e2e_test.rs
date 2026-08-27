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
    use coord_proto::kv::kv_client::KvClient;
    use coord_proto::kv::kv_server::KvServer;
    use coord_proto::kv::{DeleteRequest, PutRequest, RangeRequest};
    use coord_proto::maintenance::maintenance_client::MaintenanceClient;
    use coord_proto::maintenance::maintenance_server::MaintenanceServer;
    use coord_proto::maintenance::{SealRequest, StatusRequest, UnsealRequest};
    use coord_proto::txn::txn_client::TxnClient;
    use coord_proto::txn::txn_server::TxnServer;
    use coord_proto::txn::{compare::Target, Compare, RequestOp, TxnRequest};
    use coord_server::server::CoordNode;
    use coord_server::storage::mvcc::MvccStorage;
    use coord_server::storage::redb_backend::RedbBackend;
    use coord_server::watch::WatchDispatcher;
    use tokio::net::TcpListener;
    use tonic::transport::{Channel, Server};

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
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        // Wait for server to be ready
        tokio::time::sleep(Duration::from_millis(100)).await;

        (addr, handle)
    }

    /// R-SEC-01：启用静态加密的测试服务器（Barrier + Keyring + root 密钥提供者）
    async fn start_encrypted_test_server(
        root_key: [u8; 32],
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        use coord_server::security::barrier::Barrier;
        use coord_server::security::key_management::Keyring;
        use std::sync::Arc as StdArc;

        let tmpdir = tempfile::tempdir().unwrap();
        let data_dir = tmpdir.path().to_path_buf();

        let config = StorageConfig::default();
        let backend = RedbBackend::open(&data_dir, &config).unwrap();
        let mvcc = StdArc::new(MvccStorage::new(backend).unwrap());

        let (keyring, dek) = Keyring::bootstrap_from_root_key(&root_key).unwrap();
        let keyring = StdArc::new(keyring);
        mvcc.set_barrier(Barrier::new(StdArc::clone(&keyring)));

        let mut node = CoordNode::new(StdArc::clone(&mvcc));
        node.install_keyring(StdArc::clone(&keyring), vec![dek]);
        // unseal 用 root 密钥提供者
        node.root_key_provider = Some(StdArc::new(move || Some(root_key.to_vec())));
        let watch = StdArc::new(WatchDispatcher::start());
        node.watch_dispatcher = Some(watch);
        let node = StdArc::new(node);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let kv_svc = KvServer::from_arc(StdArc::clone(&node));
        let txn_svc = TxnServer::from_arc(StdArc::clone(&node));
        let maint_svc = MaintenanceServer::from_arc(StdArc::clone(&node));

        let handle = tokio::spawn(async move {
            Server::builder()
                .add_service(kv_svc)
                .add_service(txn_svc)
                .add_service(maint_svc)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        tokio::time::sleep(Duration::from_millis(100)).await;
        (addr, handle)
    }

    async fn connect(
        addr: SocketAddr,
    ) -> (
        KvClient<Channel>,
        TxnClient<Channel>,
        MaintenanceClient<Channel>,
    ) {
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

    /// R-SVC-07-1：范围读实现 [key, range_end) 半开区间语义，上界生效
    #[tokio::test]
    async fn test_e2e_range_half_open() {
        let (addr, _handle) = start_test_server().await;
        let (mut kv, _, _) = connect(addr).await;

        for (k, v) in [
            (b"a".to_vec(), b"1".to_vec()),
            (b"ab".to_vec(), b"2".to_vec()),
            (b"abc".to_vec(), b"3".to_vec()),
            (b"b".to_vec(), b"4".to_vec()),
            (b"c".to_vec(), b"5".to_vec()),
        ] {
            kv.put(PutRequest {
                key: k,
                value: v,
                lease_id: 0,
                prev_kv: false,
                request_id: vec![],
            })
            .await
            .unwrap();
        }

        // [a, b)：a/ab/abc 可见，b/c 不可见
        let resp = kv
            .range(RangeRequest {
                key: b"a".to_vec(),
                range_end: b"b".to_vec(),
                limit: 0,
                revision: 0,
                keys_only: false,
                count_only: false,
            })
            .await
            .unwrap()
            .into_inner();
        let mut keys: Vec<&[u8]> = resp.kvs.iter().map(|kv| kv.key.as_slice()).collect();
        keys.sort();
        assert_eq!(
            keys,
            vec![b"a".as_slice(), b"ab".as_slice(), b"abc".as_slice()],
            "半开区间 [a, b) 上界必须排除 b"
        );

        // 区间内 key 不共享 start 前缀：[ab, c) 应含 abc 与 b
        let resp = kv
            .range(RangeRequest {
                key: b"ab".to_vec(),
                range_end: b"c".to_vec(),
                limit: 0,
                revision: 0,
                keys_only: false,
                count_only: false,
            })
            .await
            .unwrap()
            .into_inner();
        let mut keys: Vec<&[u8]> = resp.kvs.iter().map(|kv| kv.key.as_slice()).collect();
        keys.sort();
        assert_eq!(
            keys,
            vec![b"ab".as_slice(), b"abc".as_slice(), b"b".as_slice()],
            "区间扫描不能仅依赖前缀匹配"
        );
    }

    /// R-SVC-07-2：带 revision 的范围读返回目标 revision 的历史视图（非实时数据错标）
    #[tokio::test]
    async fn test_e2e_range_at_revision() {
        let (addr, _handle) = start_test_server().await;
        let (mut kv, _, _) = connect(addr).await;

        async fn put_key(kv: &mut KvClient<Channel>, k: &[u8], v: &[u8]) -> i64 {
            kv.put(PutRequest {
                key: k.to_vec(),
                value: v.to_vec(),
                lease_id: 0,
                prev_kv: false,
                request_id: vec![],
            })
            .await
            .unwrap()
            .into_inner()
            .revision
        }

        let r1 = put_key(&mut kv, b"a", b"v1").await;
        let _r2 = put_key(&mut kv, b"b", b"v1").await;
        let r3 = put_key(&mut kv, b"a", b"v2").await;

        // 读取 rev=r1 的历史视图：a=v1（此时 b 尚未写入）
        let resp = kv
            .range(RangeRequest {
                key: b"a".to_vec(),
                range_end: b"c".to_vec(),
                limit: 0,
                revision: r1,
                keys_only: false,
                count_only: false,
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.revision, r1, "响应 revision 必须等于查询 revision");
        assert_eq!(resp.kvs.len(), 1);
        assert_eq!(resp.kvs[0].key, b"a".to_vec());
        assert_eq!(resp.kvs[0].value, b"v1".to_vec());

        // 读取 rev=r3 的历史视图：a=v2, b=v1
        let resp = kv
            .range(RangeRequest {
                key: b"a".to_vec(),
                range_end: b"c".to_vec(),
                limit: 0,
                revision: r3,
                keys_only: false,
                count_only: false,
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.kvs.len(), 2);
        assert_eq!(resp.revision, r3);
    }

    /// R-SVC-07-3：范围删除为单个原子命令，[key, range_end) 边界正确
    #[tokio::test]
    async fn test_e2e_delete_range() {
        let (addr, _handle) = start_test_server().await;
        let (mut kv, _, _) = connect(addr).await;

        for (k, v) in [
            (b"a".to_vec(), b"1".to_vec()),
            (b"ab".to_vec(), b"2".to_vec()),
            (b"b".to_vec(), b"3".to_vec()),
        ] {
            kv.put(PutRequest {
                key: k,
                value: v,
                lease_id: 0,
                prev_kv: false,
                request_id: vec![],
            })
            .await
            .unwrap();
        }

        // 删除 [a, b)
        let del = kv
            .delete(DeleteRequest {
                key: b"a".to_vec(),
                range_end: b"b".to_vec(),
                prev_kv: false,
                request_id: vec![],
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(del.deleted, 2, "a 与 ab 应被删除");

        // a/ab 不可见，b 保留
        let resp = kv
            .range(RangeRequest {
                key: b"a".to_vec(),
                range_end: b"c".to_vec(),
                limit: 0,
                revision: 0,
                keys_only: false,
                count_only: false,
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.kvs.len(), 1, "仅剩 b");
        assert_eq!(resp.kvs[0].key, b"b".to_vec());
    }

    /// R-SEC-01：静态加密端到端——写入加密、Seal 拒绝写、Unseal 恢复读
    #[tokio::test]
    async fn test_e2e_encryption_seal_unseal() {
        let root_key = [0x99u8; 32];
        let (addr, _handle) = start_encrypted_test_server(root_key).await;
        let (mut kv, _, mut maint) = connect(addr).await;

        // 写入正常（Barrier 加密）
        kv.put(PutRequest {
            key: b"secret".to_vec(),
            value: b"super-secret-value".to_vec(),
            lease_id: 0,
            prev_kv: false,
            request_id: vec![],
        })
        .await
        .unwrap();

        // status 反映 unsealed
        let st = maint.status(StatusRequest {}).await.unwrap().into_inner();
        assert_eq!(st.seal_status, "unsealed");

        // Seal → 写请求被拒
        maint.seal(SealRequest {}).await.unwrap();
        let st = maint.status(StatusRequest {}).await.unwrap().into_inner();
        assert_eq!(st.seal_status, "sealed");
        let put_err = kv
            .put(PutRequest {
                key: b"blocked".to_vec(),
                value: b"x".to_vec(),
                lease_id: 0,
                prev_kv: false,
                request_id: vec![],
            })
            .await;
        assert!(put_err.is_err(), "sealed: writes must be refused");

        // Unseal（root 密钥提供者）→ 读取与写入恢复
        let unseal = maint.unseal(UnsealRequest { shares: vec![] }).await;
        assert!(unseal.is_ok(), "unseal via root key provider: {unseal:?}");
        let st = maint.status(StatusRequest {}).await.unwrap().into_inner();
        assert_eq!(st.seal_status, "unsealed");

        let resp = kv
            .range(RangeRequest {
                key: b"secret".to_vec(),
                range_end: vec![],
                limit: 0,
                revision: 0,
                keys_only: false,
                count_only: false,
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.kvs.len(), 1);
        assert_eq!(resp.kvs[0].value, b"super-secret-value".to_vec());

        kv.put(PutRequest {
            key: b"after-unseal".to_vec(),
            value: b"ok".to_vec(),
            lease_id: 0,
            prev_kv: false,
            request_id: vec![],
        })
        .await
        .unwrap();
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
                    target_value: Some(coord_proto::txn::compare::TargetValue::Version(1)),
                }],
                success: vec![RequestOp {
                    op: Some(coord_proto::txn::request_op::Op::RequestPut(PutRequest {
                        key: b"counter".to_vec(),
                        value: 2u64.to_be_bytes().to_vec(),
                        lease_id: 0,
                        prev_kv: false,
                        request_id: vec![],
                    })),
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

        let val = u64::from_be_bytes(range_resp.kvs[0].value[..8].try_into().unwrap());
        assert_eq!(val, 2, "value should be updated to 2");

        // CAS: compare version=1 again (should fail since version is now 2)
        let txn_resp2 = txn
            .txn(TxnRequest {
                compare: vec![Compare {
                    result: coord_proto::txn::compare::CompareResult::Equal as i32,
                    target: Target::Version as i32,
                    key: b"counter".to_vec(),
                    target_value: Some(coord_proto::txn::compare::TargetValue::Version(1)),
                }],
                success: vec![RequestOp {
                    op: Some(coord_proto::txn::request_op::Op::RequestPut(PutRequest {
                        key: b"counter".to_vec(),
                        value: 99u64.to_be_bytes().to_vec(),
                        lease_id: 0,
                        prev_kv: false,
                        request_id: vec![],
                    })),
                }],
                failure: vec![],
                request_id: vec![],
            })
            .await
            .unwrap()
            .into_inner();

        assert!(
            !txn_resp2.succeeded,
            "CAS should fail on version=1 when version is 2"
        );
    }

    #[tokio::test]
    async fn test_e2e_status() {
        let (addr, _handle) = start_test_server().await;
        let (_, _, mut maint) = connect(addr).await;

        let status = maint.status(StatusRequest {}).await.unwrap().into_inner();

        assert!(status.revision >= 0);
        assert_eq!(status.seal_status, "unsealed");
    }
}
