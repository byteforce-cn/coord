//! KV-T G5（#166）`coord.storage.v2` 端到端验收：
//!
//! - 流式 Put/Get/Stat/Delete 闭环，manifest 行 revision = 全局 `commit_ts`；
//! - `read_ts` 快照读：历史 manifest 可见，chunk 字节不保留时显式
//!   `OUT_OF_RANGE` + `HISTORY_UNAVAILABLE`（不回退成当前值）；
//! - manifest 只落固定 `/obj/` Region，普通 Region 不出现；
//! - 配额/上限/能力门禁与 v1 同口径；
//! - GC 在 v2 下经 `commit_ts` 回收 stale Creating（v1 命令在 v2 Region 是 no-op）。

#[path = "support/kv_t_storage_node.rs"]
mod storage_node;

use std::sync::Arc;
use std::time::Duration;

use coord_core::error_code::error_code_of;
use coord_proto::storage::v2 as storage_v2;
use coord_proto::storage::v2::storage_client::StorageClient;
use coord_server::server::object_storage::{object_gc_once, ObjectGcTarget};
use storage_node::{StorageNode, OBJECT};
use storage_v2::put_request::Part as PutPart;
use storage_v2::{GetRequest, PutMeta, PutRequest, StatRequest};
use tonic::transport::Channel;
use tonic::Code;

const BUCKET: &str = "bucket";

async fn connect(addr: &str) -> StorageClient<Channel> {
    let channel = Channel::from_shared(format!("http://{addr}"))
        .expect("endpoint")
        .connect()
        .await
        .expect("connect");
    StorageClient::new(channel)
}

fn put_meta(total_size: i64) -> PutRequest {
    PutRequest {
        part: Some(PutPart::Meta(PutMeta {
            bucket: BUCKET.to_string(),
            object_id: b"obj-1".to_vec(),
            total_size,
        })),
    }
}

fn put_chunk(data: &[u8]) -> PutRequest {
    PutRequest {
        part: Some(PutPart::Chunk(data.to_vec())),
    }
}

async fn put(
    client: &mut StorageClient<Channel>,
    total_size: i64,
    chunks: &[&[u8]],
) -> Result<storage_v2::PutResponse, tonic::Status> {
    let mut frames = vec![put_meta(total_size)];
    frames.extend(chunks.iter().map(|c| put_chunk(c)));
    client
        .put(tokio_stream::iter(frames))
        .await
        .map(|r| r.into_inner())
}

async fn get_all(
    client: &mut StorageClient<Channel>,
    read_ts: i64,
) -> Result<(storage_v2::ObjectStat, Vec<u8>), tonic::Status> {
    let mut stream = client
        .get(GetRequest {
            bucket: BUCKET.to_string(),
            object_id: b"obj-1".to_vec(),
            read_ts,
        })
        .await?
        .into_inner();
    let mut stat = None;
    let mut body = Vec::new();
    while let Some(msg) = stream.message().await? {
        match msg.part {
            Some(storage_v2::get_response::Part::Stat(s)) => stat = Some(s),
            Some(storage_v2::get_response::Part::Chunk(c)) => body.extend_from_slice(&c),
            None => panic!("empty GetResponse"),
        }
    }
    Ok((stat.expect("first GetResponse carries stat"), body))
}

async fn stat_at(client: &mut StorageClient<Channel>, read_ts: i64) -> storage_v2::ObjectStat {
    client
        .stat(StatRequest {
            bucket: BUCKET.to_string(),
            object_id: b"obj-1".to_vec(),
            read_ts,
        })
        .await
        .expect("stat")
        .into_inner()
        .stat
        .expect("stat field")
}

async fn delete(
    client: &mut StorageClient<Channel>,
) -> Result<storage_v2::DeleteResponse, tonic::Status> {
    client
        .delete(storage_v2::DeleteRequest {
            bucket: BUCKET.to_string(),
            object_id: b"obj-1".to_vec(),
            request_id: b"del-1".to_vec(),
        })
        .await
        .map(|r| r.into_inner())
}

/// 手动推进各 Region 的 safe-ts 水位（生产由 `kvt::safe_ts::advance_once` 驱动；
/// 夹具的 FakeTso 不接 region 0 的 TSO 状态机，所以直接提案）。
async fn advance_safe_ts(node: &StorageNode, ts: u64) {
    for region_id in [storage_node::LEFT, OBJECT, storage_node::RIGHT] {
        let runtime = node.manager.runtime(region_id).expect("runtime");
        let resp = runtime
            .raft
            .client_write(coord_server::raft::type_config::Command::AdvanceSafeTs { commit_ts: ts })
            .await
            .expect("advance safe ts");
        match resp.response() {
            coord_server::raft::type_config::Response::SafeTsAdvanced { revision } => {
                assert!(*revision >= ts)
            }
            other => panic!("unexpected AdvanceSafeTs response: {other:?}"),
        }
    }
    assert!(node.node.safe_ts() >= ts, "safe_ts must cover the target");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_storage_v2_put_get_stat_delete_roundtrip() {
    let node = StorageNode::start().await;
    let server = node.serve().await;
    let mut client = connect(&server.addr).await;

    let chunks: [&[u8]; 3] = [b"hello ", b"world", &[7u8; 300]];
    let expected: Vec<u8> = chunks.iter().flat_map(|c| c.to_vec()).collect();
    let put = put(&mut client, -1, &chunks).await.expect("put");
    assert_eq!(put.size, expected.len() as i64);
    assert_eq!(put.chunks, 3);
    assert!(put.commit_ts > 0);

    let (got, body) = get_all(&mut client, 0).await.expect("get");
    assert!(got.exists && got.committed);
    assert_eq!(got.size, expected.len() as i64);
    assert_eq!(got.chunks, 3);
    assert_eq!(got.commit_ts, put.commit_ts);
    assert_eq!(body, expected, "chunk 字节必须与写入一致");

    let s = stat_at(&mut client, 0).await;
    assert!(s.exists && s.committed);
    assert_eq!(s.commit_ts, put.commit_ts);
    assert!(
        s.create_ts <= s.commit_ts,
        "create_ts 是 Begin 的 commit_ts，不得晚于最后一次 manifest 写入"
    );

    let del = delete(&mut client).await.expect("delete");
    assert!(del.deleted);
    assert!(
        del.commit_ts > put.commit_ts,
        "tombstone 必须是更大的 commit_ts"
    );

    let s = stat_at(&mut client, 0).await;
    assert!(!s.exists, "删除后 Stat 返回 exists=false 而不是 NOT_FOUND");
    assert_eq!(s.commit_ts, 0);

    let err = get_all(&mut client, 0).await.expect_err("get after delete");
    assert_eq!(err.code(), Code::NotFound);
    let err = delete(&mut client).await.expect_err("second delete");
    assert_eq!(err.code(), Code::NotFound);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_storage_v2_put_is_idempotent_until_delete() {
    let node = StorageNode::start().await;
    let server = node.serve().await;
    let mut client = connect(&server.addr).await;

    let first = put(&mut client, 5, &[b"first"]).await.expect("first put");
    let err = put(&mut client, 5, &[b"other"])
        .await
        .expect_err("duplicate");
    assert_eq!(err.code(), Code::AlreadyExists);

    // 声明长度不一致 → Commit 拒绝，manifest 停在 Creating（由 GC 回收）
    assert_eq!(delete(&mut client).await.expect("delete").deleted, true);
    let err = put(&mut client, 9, &[b"short"]).await.expect_err("short");
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(
        node.manifest_row(BUCKET.as_bytes(), b"obj-1").is_some(),
        "失败的 Commit 不能删掉 manifest（v1 同语义，留给 GC）"
    );
    // 上传中（Creating）也拒绝重复 Begin
    let err = put(&mut client, 5, &[b"again"])
        .await
        .expect_err("creating");
    assert_eq!(err.code(), Code::AlreadyExists);

    // 删除 Creating 后可重建；commit_ts 必须前进
    assert_eq!(delete(&mut client).await.expect("delete").deleted, true);
    let second = put(&mut client, 6, &[b"second"]).await.expect("rebuild");
    assert!(second.commit_ts > first.commit_ts);
    let (s, body) = get_all(&mut client, 0).await.expect("get");
    assert_eq!(body, b"second");
    assert_eq!(s.commit_ts, second.commit_ts);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_storage_v2_read_ts_snapshot_and_history_unavailable() {
    let node = StorageNode::start().await;
    let server = node.serve().await;
    let mut client = connect(&server.addr).await;

    let old = put(&mut client, 5, &[b"aaaaa"]).await.expect("old put");
    advance_safe_ts(&node, old.commit_ts as u64).await;
    let deleted = delete(&mut client).await.expect("delete");
    advance_safe_ts(&node, deleted.commit_ts as u64).await;
    let new = put(&mut client, 4, &[b"bbbb"]).await.expect("new put");
    assert!(new.commit_ts > deleted.commit_ts);

    // 历史 manifest 可见：read_ts = 删除前 → 旧对象仍然存在
    let at_old = stat_at(&mut client, old.commit_ts).await;
    assert!(at_old.exists, "read_ts 早于 Delete 时必须看到旧对象");
    assert_eq!(at_old.size, 5);
    assert_eq!(at_old.commit_ts, old.commit_ts);

    // 当前版本是新对象
    let current = stat_at(&mut client, 0).await;
    assert_eq!(current.size, 4);
    assert_eq!(current.commit_ts, new.commit_ts);

    // 历史 chunk 已被 Delete 删掉：显式 OUT_OF_RANGE，不回退成当前值
    let err = get_all(&mut client, old.commit_ts)
        .await
        .expect_err("historical chunk bytes are not retained");

    assert_eq!(err.code(), Code::OutOfRange);
    assert_eq!(
        error_code_of(&err).as_deref(),
        Some("OUT_OF_RANGE"),
        "历史上不可用必须带结构化错误码"
    );
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_storage_v2_manifest_stays_in_object_region() {
    let node = StorageNode::start().await;
    let server = node.serve().await;
    let mut client = connect(&server.addr).await;

    put(&mut client, 3, &[b"abc"]).await.expect("put");
    assert!(
        node.manifest_row(BUCKET.as_bytes(), b"obj-1").is_some(),
        "manifest 行必须落在固定 /obj/ Region"
    );
    assert!(
        !node.non_object_regions_have(BUCKET.as_bytes(), b"obj-1"),
        "普通 Region 不得出现对象 manifest"
    );
    let key = coord_server::storage::object_store::manifest_key(BUCKET.as_bytes(), b"obj-1");
    let runtime = node
        .manager
        .route_runtime(&key)
        .expect("route manifest key");
    assert_eq!(runtime.region_id(), OBJECT);

    // 删除后行消失（tombstone）
    delete(&mut client).await.expect("delete");
    assert!(node.manifest_row(BUCKET.as_bytes(), b"obj-1").is_none());
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_storage_v2_quota_and_limits() {
    let node = StorageNode::start_with(coord_server::storage::object_store::ObjectLimits {
        chunk_size: 1024,
        max_object_size: 2048,
        quota_bytes: 2500,
        upload_timeout_secs: 300,
        dek_rotation_secs: 0,
    })
    .await;
    let server = node.serve().await;
    let mut client = connect(&server.addr).await;

    // 声明超过 max_object_size
    let err = put(&mut client, 4096, &[b"x"]).await.expect_err("too big");
    assert_eq!(err.code(), Code::InvalidArgument);

    // 单 chunk 超过 chunk_size（Begin 已提交，残留 Creating 必须先删掉）
    let big = vec![9u8; 2048];
    let err = put(&mut client, 2048, &[&big])
        .await
        .expect_err("chunk too big");
    assert_eq!(err.code(), Code::InvalidArgument);
    delete(&mut client).await.expect("cleanup creating");

    // 第一次两个 1000 字节 chunk（总 2000）落在配额内；第二次同尺寸触发 quota 拒绝
    let chunk = vec![1u8; 1000];
    put(&mut client, 2000, &[&chunk, &chunk])
        .await
        .expect("first object");
    let err = put(&mut client, 2000, &[&chunk, &chunk])
        .await
        .expect_err("quota exceeded");
    assert_eq!(err.code(), Code::ResourceExhausted);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_storage_v2_gc_reclaims_stale_creating() {
    let node = StorageNode::start_with(coord_server::storage::object_store::ObjectLimits {
        chunk_size: 1024,
        max_object_size: 4096,
        // 0 = 立即视为 stale（夹具只验证回收路径，不验证墙钟窗口）
        upload_timeout_secs: 0,
        ..Default::default()
    })
    .await;
    let server = node.serve().await;
    let mut client = connect(&server.addr).await;

    // 声明长度不符 → Commit 被拒，manifest 停在 Creating
    let err = put(&mut client, 10, &[b"abc"]).await.expect_err("commit");
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(node.manifest_row(BUCKET.as_bytes(), b"obj-1").is_some());

    // stale 判定是秒级墙钟（`now - last_write > upload_timeout`），等过一个整秒
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let runtime = node.object_runtime();
    let target = ObjectGcTarget {
        node_id: storage_node::NODE_ID,
        region_id: OBJECT,
        raft: Arc::new(runtime.raft.clone()),
        mvcc: Arc::clone(&runtime.mvcc),
        chunk_store: Arc::clone(runtime.chunk_store.as_ref().expect("chunk store")),
        limits: Arc::clone(&node.limits),
        interval_secs: 1,
        v2: Some(Arc::clone(&node.node)),
    };
    let outcome = object_gc_once(&target).await;
    assert_eq!(outcome.deleted_stale, 1, "GC 必须回收 stale Creating");
    assert!(
        node.manifest_row(BUCKET.as_bytes(), b"obj-1").is_none(),
        "stale Creating 必须经 v2 Delete（commit_ts）回收"
    );
    // 回收后同一对象可重新上传
    put(&mut client, 3, &[b"abc"])
        .await
        .expect("re-put after gc");
    server.shutdown().await;
}

/// 能力门禁：未启用 / 未登记 Storage / 未启用对象存储全部 FAILED_PRECONDITION。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_storage_v2_capability_gate() {
    use coord_core::storage::StorageBackend;
    use coord_core::types::StorageConfig;
    use coord_proto::storage::v2::storage_server::StorageServer as StorageV2Server;
    use coord_server::kvt::{KvTCapability, KvTRuntimeConfig, KvTState};
    use coord_server::server::CoordNode;
    use coord_server::storage::mvcc::{MvccStorage, RevisionMode};
    use coord_server::storage::redb_backend::RedbBackend;

    async fn serve_node(node: Arc<CoordNode>) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let handle = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(StorageV2Server::from_arc(node))
                .serve_with_incoming(incoming)
                .await;
        });
        (addr, handle)
    }

    fn node(dir: &tempfile::TempDir, state: KvTState, object_limits: bool) -> Arc<CoordNode> {
        let backend = RedbBackend::open(dir.path(), &StorageConfig::default()).unwrap();
        let mvcc =
            Arc::new(MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs).unwrap());
        let mut node = CoordNode::new(mvcc);
        if object_limits {
            node.object_limits = Some(Arc::new(
                coord_server::storage::object_store::ObjectLimits::default(),
            ));
        }
        node.set_kvt_state(state);
        Arc::new(node)
    }

    let dir = tempfile::tempdir().unwrap();
    let (addr, handle) = serve_node(node(&dir, KvTState::disabled(), true)).await;
    let mut client = connect(&addr).await;
    let err = client.stat(StatRequest {
        bucket: BUCKET.to_string(),
        object_id: b"x".to_vec(),
        read_ts: 0,
    });
    let err = err.await.expect_err("disabled");
    assert_eq!(err.code(), Code::FailedPrecondition);

    // 开启但未登记 Storage 能力
    let dir2 = tempfile::tempdir().unwrap();
    let enabled_without = KvTState::new(true, KvTRuntimeConfig::default());
    let (addr, handle2) = serve_node(node(&dir2, enabled_without, true)).await;
    let mut client2 = connect(&addr).await;
    let err = client2
        .stat(StatRequest {
            bucket: BUCKET.to_string(),
            object_id: b"x".to_vec(),
            read_ts: 0,
        })
        .await
        .expect_err("capability not implemented");
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(
        err.message().contains("coord.storage.v2"),
        "错误信息必须点名能力：{err}"
    );

    // 能力已登记但 [object_storage] 未启用
    let dir3 = tempfile::tempdir().unwrap();
    let enabled =
        KvTState::new(true, KvTRuntimeConfig::default()).with_implemented([KvTCapability::Storage]);
    let (addr, handle3) = serve_node(node(&dir3, enabled, false)).await;
    let mut client3 = connect(&addr).await;
    let err = client3
        .stat(StatRequest {
            bucket: BUCKET.to_string(),
            object_id: b"x".to_vec(),
            read_ts: 0,
        })
        .await
        .expect_err("object storage disabled");
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(err.message().contains("[object_storage]"), "{err}");

    handle.abort();
    handle2.abort();
    handle3.abort();
    // 保持 Duration 引用（夹具共用常量）
    let _ = Duration::from_secs(1);
}
