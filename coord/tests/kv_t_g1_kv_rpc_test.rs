//! KV-T G1（#141）`coord.kv.v2` 单 Region 读写：
//! - 显式 `commit_ts` 写、`read_ts` 快照读与 header revision；
//! - `COMMIT_REJECTED` / `SAFE_TS_LAG` / `OUT_OF_RANGE` 结构化错误；
//! - `request_id` 幂等回放与 v2 关闭时 v1 不变；
//! - 重启后 revision/数据不倒退。

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

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
    use coord_server::kvt::{KvTCapability, KvTRuntimeConfig, KvTState};
    use coord_server::server::CoordNode;
    use coord_server::storage::mvcc::{MvccStorage, RevisionMode};
    use coord_server::storage::redb_backend::RedbBackend;
    use tokio::net::TcpListener;
    use tonic::transport::{Channel, Server};
    use tonic::Code;

    struct TestServer {
        addr: SocketAddr,
        handle: tokio::task::JoinHandle<()>,
        node: Arc<CoordNode>,
    }

    impl TestServer {
        async fn shutdown(self) {
            self.handle.abort();
            let _ = self.handle.await;
            drop(self.node);
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
        let node = Arc::new(node);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_node = Arc::clone(&node);
        let handle = tokio::spawn(async move {
            Server::builder()
                .add_service(KvV1Server::from_arc(Arc::clone(&server_node)))
                .add_service(KvV2Server::from_arc(server_node))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        TestServer { addr, handle, node }
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

        server.shutdown().await;
    }

    // Task 3 会在这里补充 Range 的 safe_ts / 快照 / 上限用例。
}
