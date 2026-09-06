// 对象存储 raft 集成测试（数据面闭环核心验收）
//
// 两种场景（都是真实单节点 raft）：
//   1) legacy/root 路径（无 region_manager）：对象经 root raft（region 0）apply，
//      manifest 落 root MVCC `/kv//obj/m/...`，chunk 文件落 `<data_dir>/objects/`。
//   2) 多 Region 路径 + chunk 加密：对象 manifest key 路由进 Region（覆盖
//      `/obj/` 前缀），Region 自己的 raft apply，chunk 加密落 Region 数据目录。
//
// 覆盖：Begin 冲突 / Chunk 顺序 / Commit 字节校验 / tombstone+文件删除 /
// 重建（delete→re-begin）/ 加密往返 / manifest 读取与读取工具 / GC 孤儿回收语义。
//
// 说明：对象 gRPC 层（流式 Put/Get 分帧与 4MiB 边界）由 Phase A 验收（Jepsen
// blob / 进程级 e2e）覆盖，此处验证 raft apply + 文件数据面闭环。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use coord_core::storage::StorageBackend;
use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionMeta, StorageConfig};
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::RaftNetworkFactoryImpl;
use coord_server::raft::region::RegionManager;
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::type_config::{Command, ObjectStoreOp, Response};
use coord_server::raft::{new_basic_node, new_raft, RaftConfig, RegionRuntimeSpec};
use coord_server::server::CoordNode;
use coord_server::storage::mvcc::MvccStorage;
use coord_server::storage::object_store::{
    live_object_hashes, manifest_key, read_manifest, validate_ref, ChunkStore, ObjectLimits,
    ObjectStoreCtx,
};
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::snapshot::SnapshotTracker;
use coord_server::raft::region_runtime::region_data_dir;

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// 经 raft 提交一个 ObjectStore 命令，返回 (revision, ok)
async fn propose(
    raft: &Arc<coord_server::raft::CoordRaft>,
    op: ObjectStoreOp,
) -> (u64, bool) {
    let resp = raft
        .client_write(Command::ObjectStore(op))
        .await
        .expect("object op client_write");
    match resp.response() {
        Response::ObjectStore { revision, ok } => (*revision, *ok),
        other => panic!("unexpected response: {other:?}"),
    }
}

/// 完整上传一个对象（Begin + N chunk + Commit）走 raft
async fn upload_object(
    raft: &Arc<coord_server::raft::CoordRaft>,
    bucket: &str,
    object_id: &[u8],
    data: &[u8],
    chunk_size: usize,
    total_size: Option<u64>,
) -> (u64, bool) {
    let total = total_size.unwrap_or(data.len() as u64);
    let (_, ok) = propose(
        raft,
        ObjectStoreOp::Begin {
            bucket: bucket.as_bytes().to_vec(),
            object_id: object_id.to_vec(),
            total_size: total,
            started_at_unix: now_unix(),
        },
    )
    .await;
    assert!(ok, "begin should succeed");
    let mut seq = 0u32;
    for chunk in data.chunks(chunk_size) {
        let (_, ok) = propose(
            raft,
            ObjectStoreOp::Chunk {
                bucket: bucket.as_bytes().to_vec(),
                object_id: object_id.to_vec(),
                seq,
                data: chunk.to_vec(),
                now_unix: now_unix(),
            },
        )
        .await;
        assert!(ok, "chunk {seq} should append");
        seq += 1;
    }
    propose(
        raft,
        ObjectStoreOp::Commit {
            bucket: bucket.as_bytes().to_vec(),
            object_id: object_id.to_vec(),
        },
    )
    .await
}

fn build_limits(chunk_size: usize) -> Arc<ObjectLimits> {
    Arc::new(ObjectLimits {
        chunk_size,
        max_object_size: 64 * 1024 * 1024,
        quota_bytes: 0,
        upload_timeout_secs: 300,
        dek_rotation_secs: 0,
    })
}

// ──── 场景 1：legacy/root 单 raft（无 region_manager） ────

#[tokio::test]
async fn object_store_legacy_root_apply_roundtrip() {
    let tmpdir = tempfile::tempdir().unwrap();
    let base = tmpdir.path().to_path_buf();
    let chunk_size = 4 * 1024 * 1024;

    // root raft（region 0）
    let storage_config = StorageConfig::default();
    let backend = RedbBackend::open(&base, &storage_config).unwrap();
    let mvcc = Arc::new(MvccStorage::new(backend).unwrap());
    let tracker = Arc::new(SnapshotTracker::default());
    let log_store = LogStore::new(&base)
        .await
        .unwrap()
        .with_snapshot_tracker(Arc::clone(&tracker));
    let mut sm_store = StateMachineStore::new(Arc::clone(&mvcc), base.join("snapshots"), tracker);
    // 对象存储启用：root chunk store（<data_dir>/objects/，明文）
    let store = ChunkStore::new(&base, build_limits(chunk_size), None).unwrap();
    sm_store.set_object_chunk_store(Some(Arc::clone(&store)));
    let factory = RaftNetworkFactoryImpl::new(1);
    let raft_addr = format!("127.0.0.1:{}", {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    });
    factory.register_node(1, raft_addr.clone());
    let raft = new_raft(1, Arc::new(RaftConfig::default()), factory, log_store, sm_store)
        .await
        .unwrap();
    let mut members = BTreeMap::new();
    members.insert(1, new_basic_node(&raft_addr));
    raft.initialize(members).await.unwrap();
    let raft = Arc::new(raft);

    // CoordNode（legacy：raft Some + region_manager None + chunk_store Some）
    let mut node = CoordNode::new(Arc::clone(&mvcc));
    node.node_id = 1;
    node.raft = Some(Arc::clone(&raft));
    node.object_limits = Some(build_limits(chunk_size));
    node.chunk_store = Some(Arc::clone(&store));
    let node = Arc::new(node);

    // 等 leader
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while raft.current_leader().await != Some(1) {
        assert!(tokio::time::Instant::now() < deadline, "no leader");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    validate_ref(b"bucket-1", b"obj-1").unwrap();
    let data: Vec<u8> = (0..=255u8).cycle().take(9 * 1024 * 1024 + 7).collect(); // ~9MiB, 跨 chunk
    let chunks = data.len().div_ceil(chunk_size);

    // 上传
    let (rev, ok) = upload_object(&raft, "bucket-1", b"obj-1", &data, chunk_size, None).await;
    assert!(ok, "commit ok");
    assert!(rev > 0);

    // manifest 落 root MVCC（/kv//obj/m/bucket-1/obj-1），可经对象读取工具读
    let m = read_manifest(&mvcc, b"bucket-1", b"obj-1")
        .unwrap()
        .expect("manifest present");
    assert!(m.committed);
    assert_eq!(m.size, data.len() as u64);
    assert_eq!(m.chunks.len(), chunks);

    // 用户 KV 读路径看不到 /obj/ 内部行（mvcc.get 直读会看到——那是内部行；
    // 用户层过滤在 gRPC Range 层，此处验证 manifest key 形态正确）
    assert!(mvcc.get(&manifest_key(b"bucket-1", b"obj-1")).unwrap().is_some());

    // chunk 文件落 <base>/objects/ 且可读
    assert!(store.chunk_file_exists(b"bucket-1", b"obj-1", 0));
    for (i, chunk) in data.chunks(chunk_size).enumerate() {
        let got = store.read_chunk(b"bucket-1", b"obj-1", i as u32).unwrap();
        assert_eq!(got, chunk, "chunk {i} roundtrip");
    }

    // Begin 冲突（Committed 已存在）→ ok=false
    let (_, ok) = propose(
        &raft,
        ObjectStoreOp::Begin {
            bucket: b"bucket-1".to_vec(),
            object_id: b"obj-1".to_vec(),
            total_size: 1,
            started_at_unix: now_unix(),
        },
    )
    .await;
    assert!(!ok, "re-begin on committed object must conflict");

    // Delete → tombstone：manifest 消失 + chunk 文件删除
    let (_, ok) = propose(
        &raft,
        ObjectStoreOp::Delete {
            bucket: b"bucket-1".to_vec(),
            object_id: b"obj-1".to_vec(),
        },
    )
    .await;
    assert!(ok, "delete ok");
    assert!(read_manifest(&mvcc, b"bucket-1", b"obj-1").unwrap().is_none());
    assert!(!store.chunk_file_exists(b"bucket-1", b"obj-1", 0));
    // 目录已删（delete_object_files 删除对象目录）
    assert!(!store.chunk_path(b"bucket-1", b"obj-1", 0).exists());

    // 删除后重建（tombstone 允许 re-begin）
    let data2 = b"second-life-object".repeat(1000);
    let (_, ok) = upload_object(&raft, "bucket-1", b"obj-1", &data2, chunk_size, None).await;
    assert!(ok);
    let m2 = read_manifest(&mvcc, b"bucket-1", b"obj-1").unwrap().unwrap();
    assert!(m2.committed);
    assert_eq!(m2.size, data2.len() as u64);
    assert_eq!(store.read_chunk(b"bucket-1", b"obj-1", 0).unwrap(), &data2[..chunk_size.min(data2.len())]);

    // GC 孤儿回收：无 manifest 的残留文件被清扫（制造孤儿 = 直接写文件再清扫）
    let orphans = {
        let store2 = ChunkStore::new(&base, build_limits(chunk_size), None).unwrap();
        store2.write_chunk(b"orphan-bucket", b"dead", 0, b"junk").unwrap();
        let mlist = coord_server::storage::object_store::list_manifests(&mvcc).unwrap();
        let live = live_object_hashes(&mlist);
        store2.sweep_orphans(&live).unwrap()
    };
    assert_eq!(orphans, 1, "one orphan dir swept");

    // 读取工具与节点句柄一致性（Stat 同源）
    assert!(node.object_enabled());
}

// ──── 场景 2：多 Region 路径 + chunk 加密 ────

#[tokio::test]
async fn object_store_region_apply_encrypted() {
    let tmpdir = tempfile::tempdir().unwrap();
    let base = tmpdir.path().to_path_buf();
    let chunk_size = 1024 * 1024;
    let enc_key = "cd".repeat(32); // hex64
    let ctx = Arc::new(ObjectStoreCtx {
        limits: build_limits(chunk_size),
        encryption_root_key_hex: Some(enc_key),
    });

    // region 1（覆盖全 keyspace；/obj/ 前缀对象路由进这里）
    let handle = RegionMeta {
        region_id: 1,
        start_key: vec![],
        end_key: vec![],
        epoch: RegionEpoch::initial(),
        peers: vec![Peer {
            node_id: 1,
            raft_addr: "127.0.0.1:1".to_string(),
            role: PeerRole::Voter,
        }],
        approximate_size: 0,
        approximate_keys: 0,
    };
    let manager = Arc::new(RegionManager::new(1));
    let factory = RaftNetworkFactoryImpl::new(1);
    let raft_addr = format!("127.0.0.1:{}", {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    });
    factory.register_node(1, raft_addr.clone());
    let spec = RegionRuntimeSpec {
        meta: handle,
        data_dir: region_data_dir(&base, 1),
        raft_config: Arc::new(RaftConfig {
            heartbeat_interval: 200,
            election_timeout_min: 800,
            election_timeout_max: 1500,
            ..Default::default()
        }),
        object_store: Some(ctx),
    };
    manager
        .spawn_region(
            &factory,
            &coord_server::raft::network::RaftRpcService::new(),
            spec,
            true,
        )
        .await
        .expect("spawn region");
    let rt = manager.runtime(1).expect("region 1 runtime");
    let rt_raft = Arc::new(rt.raft.clone());
    let store = rt.chunk_store.clone().expect("region chunk store present");
    let mvcc = Arc::clone(&rt.mvcc);

    // 对象 manifest key 路由进 region 1（覆盖全 keyspace）
    let routed = manager
        .route_runtime(&manifest_key(b"region-bucket", b"obj-enc"))
        .expect("route");
    assert_eq!(routed.region_id(), 1);

    // 等 leader
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while rt_raft.current_leader().await != Some(1) {
        assert!(tokio::time::Instant::now() < deadline, "no region leader");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let data: Vec<u8> = (0..100u8).cycle().take(2 * chunk_size + 123).collect();
    let (_, ok) = upload_object(&rt_raft, "region-bucket", b"obj-enc", &data, chunk_size, None).await;
    assert!(ok);

    // manifest 在 Region 1 MVCC；chunk 文件加密落盘（读回明文一致，raw 非明文）
    let m = read_manifest(&mvcc, b"region-bucket", b"obj-enc")
        .unwrap()
        .expect("manifest in region mvcc");
    assert!(m.committed);
    assert_eq!(m.size, data.len() as u64);
    for (i, chunk) in data.chunks(chunk_size).enumerate() {
        assert_eq!(
            store.read_chunk(b"region-bucket", b"obj-enc", i as u32).unwrap(),
            chunk,
            "encrypted chunk {i} roundtrip"
        );
    }
    let raw0 = std::fs::read(store.chunk_path(b"region-bucket", b"obj-enc", 0)).unwrap();
    assert!(!raw0.windows(data.len().min(11)).any(|w| w == &data[..w.len()]));

    // 删除 → 文件清空
    let (_, ok) = propose(
        &rt_raft,
        ObjectStoreOp::Delete {
            bucket: b"region-bucket".to_vec(),
            object_id: b"obj-enc".to_vec(),
        },
    )
    .await;
    assert!(ok);
    assert!(read_manifest(&mvcc, b"region-bucket", b"obj-enc").unwrap().is_none());
    assert!(!store.chunk_file_exists(b"region-bucket", b"obj-enc", 0));
}
