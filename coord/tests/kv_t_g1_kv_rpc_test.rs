//! KV-T G1（#141）`coord.kv.v2` 单 Region 读写：
//! - 显式 `commit_ts` 写、`read_ts` 快照读与 header revision；
//! - `COMMIT_REJECTED` / `SAFE_TS_LAG` / `OUT_OF_RANGE` 结构化错误；
//! - `request_id` 幂等回放与 v2 关闭时 v1 不变；
//! - 重启后 revision/数据不倒退。

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;

    use coord_core::error::Result as CoordResult;
    use coord_core::error_code::error_code_of;
    use coord_core::storage::StorageBackend;
    use coord_core::types::StorageConfig;
    use coord_proto::kv::kv_client::KvClient as KvV1Client;
    use coord_proto::kv::kv_server::KvServer as KvV1Server;
    use coord_proto::kv::v2::kv_client::KvClient as KvV2Client;
    use coord_proto::kv::v2::kv_server::KvServer as KvV2Server;
    use coord_proto::kv::v2::{
        DeleteRequest as DeleteRequestV2, PutRequest as PutRequestV2,
        RangeRequest as RangeRequestV2,
    };
    use coord_proto::kv::{PutRequest as PutRequestV1, RangeRequest as RangeRequestV1};
    use coord_proto::tso::v2::tso_server::TsoServer;
    use coord_server::kvt::{KvTCapability, KvTRuntimeConfig, KvTState};
    use coord_server::raft::tso::{TsoBatch, TsoClient, TsoSource};
    use coord_server::server::CoordNode;
    use coord_server::storage::mvcc::{AppliedLogId, MvccStorage, RevisionMode};
    use coord_server::storage::redb_backend::RedbBackend;
    use tokio::net::TcpListener;
    use tonic::transport::{Channel, Server};
    use tonic::Code;

    struct TestServer {
        addr: SocketAddr,
        handle: tokio::task::JoinHandle<()>,
        shutdown_tx: tokio::sync::oneshot::Sender<()>,
        node: Arc<CoordNode>,
    }

    impl TestServer {
        async fn shutdown(self) {
            // 优雅停 accept loop：连接任务持有 node（= Redb 文件锁），直接 abort
            // 会留下未释放的 Arc，同一目录无法立即重开。
            let _ = self.shutdown_tx.send(());
            let _ = self.handle.await;
            drop(self.node);
        }
    }

    /// 进程内 TSO 源：测试只验证 client 走 GetTs 链路，不启动 region 0 raft。
    struct FakeTso {
        high_water: AtomicU64,
    }

    impl FakeTso {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                high_water: AtomicU64::new(1),
            })
        }
    }

    #[async_trait]
    impl TsoSource for FakeTso {
        async fn is_leader(&self) -> bool {
            true
        }

        async fn allocate(&self, batch_size: u32) -> CoordResult<TsoBatch> {
            let start = self
                .high_water
                .fetch_add(u64::from(batch_size), Ordering::SeqCst);
            Ok(TsoBatch {
                start_ts: start,
                end_ts: start + u64::from(batch_size),
            })
        }
    }

    fn config(wait_ms: u64) -> KvTRuntimeConfig {
        KvTRuntimeConfig {
            read_ts_wait_ms: wait_ms,
            ..KvTRuntimeConfig::default()
        }
    }

    async fn start_server(
        dir: &Path,
        mode: RevisionMode,
        enabled: bool,
        capabilities: &[KvTCapability],
        config: KvTRuntimeConfig,
    ) -> TestServer {
        let backend = RedbBackend::open(dir, &StorageConfig::default()).unwrap();
        let mvcc = Arc::new(MvccStorage::new_with_revision_mode(backend, mode).unwrap());

        let mut node = CoordNode::new(Arc::clone(&mvcc));
        node.set_kvt_state(
            KvTState::new(enabled, config).with_implemented(capabilities.iter().copied()),
        );
        if enabled {
            node.set_tso_client(TsoClient::new(FakeTso::new(), 8, None));
        }
        let node = Arc::new(node);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_node = Arc::clone(&node);
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            Server::builder()
                .add_service(KvV1Server::from_arc(Arc::clone(&server_node)))
                .add_service(KvV2Server::from_arc(Arc::clone(&server_node)))
                .add_service(TsoServer::from_arc(server_node))
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async {
                        let _ = shutdown_rx.await;
                    },
                )
                .await
                .unwrap();
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        TestServer {
            addr,
            handle,
            shutdown_tx,
            node,
        }
    }

    async fn kv_v2(addr: SocketAddr) -> KvV2Client<Channel> {
        KvV2Client::connect(format!("http://{addr}")).await.unwrap()
    }

    async fn kv_v1(addr: SocketAddr) -> KvV1Client<Channel> {
        KvV1Client::connect(format!("http://{addr}")).await.unwrap()
    }

    fn put(key: &[u8], value: &[u8], commit_ts: i64) -> PutRequestV2 {
        PutRequestV2 {
            key: key.to_vec(),
            value: value.to_vec(),
            commit_ts,
            ..Default::default()
        }
    }

    fn delete(key: &[u8], range_end: &[u8], commit_ts: i64, prev_kv: bool) -> DeleteRequestV2 {
        DeleteRequestV2 {
            key: key.to_vec(),
            range_end: range_end.to_vec(),
            commit_ts,
            prev_kv,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_put_delete_and_commit_rejected() {
        let tmpdir = tempfile::tempdir().unwrap();
        let server = start_server(
            tmpdir.path(),
            RevisionMode::CommitTs,
            true,
            &[KvTCapability::Kv],
            config(50),
        )
        .await;
        let mut kv = kv_v2(server.addr).await;

        let resp = kv
            .put(put(b"a", b"v1", 100))
            .await
            .expect("put at 100")
            .into_inner();
        assert_eq!(resp.commit_ts, 100);
        assert!(resp.prev_kv.is_none());

        let resp = kv
            .put(PutRequestV2 {
                prev_kv: true,
                ..put(b"a", b"v2", 101)
            })
            .await
            .expect("put at 101")
            .into_inner();
        let prev = resp.prev_kv.expect("prev_kv requested");
        assert_eq!(prev.value, b"v1");
        assert_eq!(prev.mod_revision, 100);
        assert_eq!(resp.commit_ts, 101);

        // commit_ts 回退且未命中已有 revision → 显式拒绝，不得静默写。
        let err = kv
            .put(put(b"new", b"v", 99))
            .await
            .expect_err("stale commit_ts must be rejected");
        assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");
        assert_eq!(
            error_code_of(&err).as_deref(),
            Some("COMMIT_REJECTED"),
            "{err:?}"
        );

        // v2 Lease 未交付前非零 lease 必须显式拒绝（不能让 v1 清理路径
        // 在 CommitTs 模式下按 log index 删除数据）。
        let err = kv
            .put(PutRequestV2 {
                lease_id: 7,
                ..put(b"leased", b"v", 102)
            })
            .await
            .expect_err("lease binding must be gated");
        assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");
        assert!(
            err.message().contains("lease"),
            "message should name lease: {err:?}"
        );

        let resp = kv
            .delete(delete(b"a", b"", 102, true))
            .await
            .expect("delete a")
            .into_inner();
        assert_eq!(resp.deleted, 1);
        assert_eq!(resp.commit_ts, 102);
        let prev = resp.prev_kvs.first().expect("prev_kvs requested");
        assert_eq!(prev.value, b"v2");

        // 已删除的 key 再删：成功但 deleted=0（不制造幽灵删除）。
        let resp = kv
            .delete(delete(b"a", b"", 103, false))
            .await
            .expect("delete a again")
            .into_inner();
        assert_eq!(resp.deleted, 0);

        // 区间删除：同一 commit_ts 原子删除多个 key。
        kv.put(put(b"b", b"1", 104)).await.unwrap();
        kv.put(put(b"c", b"2", 105)).await.unwrap();
        let resp = kv
            .delete(delete(b"b", b"d", 106, true))
            .await
            .expect("delete range [b,d)")
            .into_inner();
        assert_eq!(resp.deleted, 2);
        assert_eq!(resp.prev_kvs.len(), 2);
        assert_eq!(resp.commit_ts, 106);

        drop(kv);
        server.shutdown().await;
    }

    #[tokio::test]
    async fn test_request_id_replay_returns_first_response() {
        let tmpdir = tempfile::tempdir().unwrap();
        let server = start_server(
            tmpdir.path(),
            RevisionMode::CommitTs,
            true,
            &[KvTCapability::Kv],
            config(50),
        )
        .await;
        let mut kv = kv_v2(server.addr).await;

        let first = kv
            .put(PutRequestV2 {
                prev_kv: true,
                request_id: b"put-r1".to_vec(),
                ..put(b"k", b"v1", 100)
            })
            .await
            .expect("first put")
            .into_inner();
        assert!(first.prev_kv.is_none());
        assert_eq!(first.commit_ts, 100);

        // 同一 request_id 重试（哪怕调用方换了 commit_ts）必须回放首次响应，
        // 不能把“首次 prev_kv=None”变成“当前值”。
        let replay = kv
            .put(PutRequestV2 {
                prev_kv: true,
                request_id: b"put-r1".to_vec(),
                ..put(b"k", b"v2", 200)
            })
            .await
            .expect("replay put")
            .into_inner();
        assert!(replay.prev_kv.is_none());
        assert_eq!(replay.commit_ts, 100);

        let resp = kv
            .delete(DeleteRequestV2 {
                request_id: b"del-r1".to_vec(),
                prev_kv: true,
                ..delete(b"missing", b"", 201, true)
            })
            .await
            .expect("delete missing")
            .into_inner();
        assert_eq!(resp.deleted, 0);

        let replay = kv
            .delete(DeleteRequestV2 {
                request_id: b"del-r1".to_vec(),
                prev_kv: true,
                ..delete(b"missing", b"", 202, true)
            })
            .await
            .expect("replay delete")
            .into_inner();
        assert_eq!(replay.deleted, 0);
        assert_eq!(replay.commit_ts, 201);

        drop(kv);
        server.shutdown().await;
    }

    #[tokio::test]
    async fn test_disabled_v2_rejects_and_v1_unchanged() {
        let tmpdir = tempfile::tempdir().unwrap();
        let server = start_server(
            tmpdir.path(),
            RevisionMode::LogIndex,
            false,
            &[],
            KvTRuntimeConfig::default(),
        )
        .await;

        let err = kv_v2(server.addr)
            .await
            .put(put(b"k", b"v", 100))
            .await
            .expect_err("v2 must be gated");
        assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");
        assert!(err.message().contains("[kv_t].enabled=false"));

        let mut v1 = kv_v1(server.addr).await;
        let put_resp = v1
            .put(PutRequestV1 {
                key: b"legacy".to_vec(),
                value: b"ok".to_vec(),
                ..Default::default()
            })
            .await
            .expect("v1 put must keep working")
            .into_inner();
        assert!(put_resp.revision > 0);
        let range_resp = v1
            .range(RangeRequestV1 {
                key: b"legacy".to_vec(),
                ..Default::default()
            })
            .await
            .expect("v1 range must keep working")
            .into_inner();
        assert_eq!(range_resp.kvs.len(), 1);
        assert_eq!(range_resp.kvs[0].value, b"ok");

        drop(v1);
        server.shutdown().await;
    }

    fn range(key: &[u8], range_end: &[u8], read_ts: i64) -> RangeRequestV2 {
        RangeRequestV2 {
            key: key.to_vec(),
            range_end: range_end.to_vec(),
            read_ts,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_range_read_your_writes_and_snapshot() {
        let tmpdir = tempfile::tempdir().unwrap();
        let server = start_server(
            tmpdir.path(),
            RevisionMode::CommitTs,
            true,
            &[KvTCapability::Kv],
            config(50),
        )
        .await;
        let mut kv = kv_v2(server.addr).await;

        kv.put(put(b"a", b"v1", 100)).await.unwrap();
        kv.put(put(b"b", b"v1", 101)).await.unwrap();
        kv.put(put(b"a", b"v2", 102)).await.unwrap();

        // read_ts=0 → safe_ts=current_revision；读己之写。
        let latest = kv
            .range(range(b"a", b"z", 0))
            .await
            .expect("latest range")
            .into_inner();
        assert_eq!(latest.read_ts, 102);
        assert_eq!(latest.count, 2);
        let a = latest.kvs.iter().find(|kv| kv.key == b"a").unwrap();
        assert_eq!(a.value, b"v2");
        assert_eq!(a.mod_revision, 102);
        assert_eq!(a.create_revision, 100);
        assert_eq!(a.version, 2);

        // read_ts=100：a 当时还不存在（create=100 恰好可见），b 尚未创建。
        let at_100 = kv
            .range(range(b"a", b"z", 100))
            .await
            .expect("snapshot at 100")
            .into_inner();
        assert_eq!(at_100.read_ts, 100);
        assert_eq!(at_100.kvs.len(), 1);
        assert_eq!(at_100.kvs[0].key, b"a");
        assert_eq!(at_100.kvs[0].value, b"v1");
        assert_eq!(at_100.kvs[0].mod_revision, 100);
        assert_eq!(at_100.kvs[0].version, 1);

        // 单键历史读与区间读同口径。
        let single = kv
            .range(range(b"a", b"", 101))
            .await
            .expect("single key snapshot")
            .into_inner();
        assert_eq!(single.kvs.len(), 1);
        assert_eq!(single.kvs[0].value, b"v1");
        assert_eq!(single.kvs[0].mod_revision, 100);

        // count_only / keys_only。
        let counted = kv
            .range(RangeRequestV2 {
                count_only: true,
                ..range(b"a", b"z", 101)
            })
            .await
            .expect("count only")
            .into_inner();
        assert_eq!(counted.count, 2);
        assert!(counted.kvs.is_empty());

        let keys_only = kv
            .range(RangeRequestV2 {
                keys_only: true,
                ..range(b"a", b"z", 0)
            })
            .await
            .expect("keys only")
            .into_inner();
        assert_eq!(keys_only.kvs.len(), 2);
        assert!(keys_only.kvs.iter().all(|kv| kv.value.is_empty()));

        drop(kv);
        server.shutdown().await;
    }

    #[tokio::test]
    async fn test_range_waits_for_read_ts_then_safe_ts_lag() {
        let tmpdir = tempfile::tempdir().unwrap();
        let server = start_server(
            tmpdir.path(),
            RevisionMode::CommitTs,
            true,
            &[KvTCapability::Kv],
            config(50),
        )
        .await;
        let mut kv = kv_v2(server.addr).await;
        kv.put(put(b"k", b"v", 100)).await.unwrap();

        let started = tokio::time::Instant::now();
        let err = kv
            .range(range(b"k", b"", 200))
            .await
            .expect_err("read_ts beyond safe_ts must time out");
        assert_eq!(err.code(), Code::Unavailable, "{err:?}");
        assert_eq!(
            error_code_of(&err).as_deref(),
            Some("SAFE_TS_LAG"),
            "{err:?}"
        );
        assert!(
            started.elapsed() >= Duration::from_millis(40),
            "must wait for the configured window"
        );

        drop(kv);
        server.shutdown().await;
    }

    #[tokio::test]
    async fn test_range_compacted_read_ts_is_out_of_range_and_limit_is_capped() {
        let tmpdir = tempfile::tempdir().unwrap();
        let server = start_server(
            tmpdir.path(),
            RevisionMode::CommitTs,
            true,
            &[KvTCapability::Kv],
            KvTRuntimeConfig {
                max_range: 5,
                read_ts_wait_ms: 50,
                ..KvTRuntimeConfig::default()
            },
        )
        .await;
        let mut kv = kv_v2(server.addr).await;
        for (i, key) in [b"a", b"b", b"c", b"d", b"e", b"f"].iter().enumerate() {
            kv.put(put(key.as_slice(), b"v", 100 + i as i64))
                .await
                .unwrap();
        }
        server
            .node
            .storage
            .apply_compact(100, AppliedLogId::standalone(107))
            .unwrap();

        let err = kv
            .range(range(b"a", b"z", 100))
            .await
            .expect_err("compacted read_ts must be rejected");
        assert_eq!(err.code(), Code::OutOfRange, "{err:?}");
        assert_eq!(
            error_code_of(&err).as_deref(),
            Some("OUT_OF_RANGE"),
            "{err:?}"
        );

        let err = kv
            .range(RangeRequestV2 {
                limit: 6,
                ..range(b"a", b"z", 0)
            })
            .await
            .expect_err("over-limit read must be rejected");
        assert_eq!(err.code(), Code::InvalidArgument, "{err:?}");
        assert_eq!(
            error_code_of(&err).as_deref(),
            Some("READ_SET_TOO_LARGE"),
            "{err:?}"
        );

        let err = kv
            .range(RangeRequestV2 {
                count_only: true,
                ..range(b"a", b"z", 0)
            })
            .await
            .expect_err("count-only scan over max_range must be rejected");
        assert_eq!(err.code(), Code::InvalidArgument, "{err:?}");
        assert_eq!(
            error_code_of(&err).as_deref(),
            Some("READ_SET_TOO_LARGE"),
            "{err:?}"
        );

        drop(kv);
        server.shutdown().await;
    }

    #[tokio::test]
    async fn test_restart_preserves_revision_and_data() {
        let tmpdir = tempfile::tempdir().unwrap();
        let server = start_server(
            tmpdir.path(),
            RevisionMode::CommitTs,
            true,
            &[KvTCapability::Kv],
            config(50),
        )
        .await;
        let mut kv = kv_v2(server.addr).await;
        kv.put(put(b"k", b"v1", 100)).await.unwrap();
        drop(kv);
        server.shutdown().await;

        let server = start_server(
            tmpdir.path(),
            RevisionMode::CommitTs,
            true,
            &[KvTCapability::Kv],
            config(50),
        )
        .await;
        let mut kv = kv_v2(server.addr).await;
        let err = kv
            .put(put(b"k2", b"v", 99))
            .await
            .expect_err("revision must survive restart");
        assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");
        assert_eq!(
            error_code_of(&err).as_deref(),
            Some("COMMIT_REJECTED"),
            "{err:?}"
        );

        kv.put(put(b"k", b"v2", 101)).await.unwrap();
        let snapshot = kv
            .range(range(b"k", b"", 100))
            .await
            .expect("historical range after restart")
            .into_inner();
        assert_eq!(snapshot.kvs.len(), 1);
        assert_eq!(snapshot.kvs[0].value, b"v1");
        assert_eq!(snapshot.kvs[0].mod_revision, 100);

        drop(kv);
        server.shutdown().await;
    }

    #[tokio::test]
    async fn test_client_v2_gets_ts_then_reads_own_write() {
        let tmpdir = tempfile::tempdir().unwrap();
        let server = start_server(
            tmpdir.path(),
            RevisionMode::CommitTs,
            true,
            &[KvTCapability::Kv, KvTCapability::Tso],
            config(50),
        )
        .await;

        let client = coord_client::Client::connect_direct(coord_client::Config::new(vec![server
            .addr
            .to_string()]))
        .await
        .expect("connect client");
        let kv = client.kv_v2();

        let commit_ts = kv.get_commit_ts().await.expect("get ts");
        assert!(commit_ts >= 1);
        let (prev, written_ts) = kv
            .put(b"client-key", b"client-value", commit_ts, false, b"")
            .await
            .expect("client v2 put");
        assert!(prev.is_none());
        assert_eq!(written_ts, commit_ts);

        let (kvs, count, read_ts) = kv
            .range(b"client-key", b"", 0, commit_ts, false, false)
            .await
            .expect("client v2 range");
        assert_eq!(count, 1);
        assert_eq!(read_ts, commit_ts);
        assert_eq!(kvs[0].value, b"client-value");
        assert_eq!(kvs[0].mod_revision, commit_ts as i64);

        drop(client);
        server.shutdown().await;
    }
}
