//! KV-T G2（#143）`coord.kv.v2.Range` 跨 Region 扇出：
//!
//! - 一个请求内所有分段共享同一个 `read_ts`；领先 Region 走精确历史视图；
//! - 半开边界（Region 起点包含、终点排除）、单键、空区间、零快照；
//! - 全局 `limit` / `count_only` / `keys_only` 与结果集上限；
//! - 任一 Region 缺失 → 整请求失败；v1 跨 Region Range 仍显式拒绝。
//!
//! 夹具为单节点 × 3 个数据 Region（`["", "d")` / `["d", "n")` / `["n", "")`），
//! `RevisionMode::CommitTs`，每个 Region 单 voter（进程内选主即可，无需 raft RPC
//! server）。与 `coord/tests/kv_t_g2_safe_ts_test.rs` 的 `G2Node` 同路径。

use std::sync::Arc;
use std::time::Duration;

use coord_core::error_code::error_code_of;
use coord_core::storage::StorageBackend;
use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionId, RegionMeta, StorageConfig};
use coord_proto::kv::v2::kv_server::Kv as KvV2;
use coord_proto::kv::v2::{RangeRequest as RangeRequestV2, RangeResponse as RangeResponseV2};
use coord_server::kvt::{KvTCapability, KvTRuntimeConfig, KvTState};
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcService};
use coord_server::raft::region::RegionManager;
use coord_server::raft::region_runtime::{region_data_dir, RegionRuntimeSpec};
use coord_server::raft::type_config::{Command, Response};
use coord_server::raft::{RaftConfig, WatchReceiver};
use coord_server::server::CoordNode;
use coord_server::storage::mvcc::{AppliedLogId, MvccStorage, RevisionMode};
use coord_server::storage::redb_backend::RedbBackend;
use tempfile::TempDir;
use tonic::Code;

const REGIONS: [(RegionId, &[u8], &[u8]); 3] = [(1, b"", b"d"), (2, b"d", b"n"), (3, b"n", b"")];

fn find_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct FanoutNode {
    node: Arc<CoordNode>,
    manager: Arc<RegionManager>,
    _dir: TempDir,
}

impl FanoutNode {
    async fn start(max_range: u32) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().to_path_buf();
        let addr = format!("127.0.0.1:{}", find_port());

        let factory = RaftNetworkFactoryImpl::new(1);
        factory.register_node(1, addr.clone());
        let rpc = RaftRpcService::new();
        let manager = Arc::new(RegionManager::new(1));
        for (region_id, start, end) in REGIONS {
            let meta = RegionMeta {
                region_id,
                start_key: start.to_vec(),
                end_key: end.to_vec(),
                epoch: RegionEpoch::initial(),
                peers: vec![Peer {
                    node_id: 1,
                    raft_addr: addr.clone(),
                    role: PeerRole::Voter,
                }],
                approximate_size: 0,
                approximate_keys: 0,
            };
            manager
                .spawn_region(
                    &factory,
                    &rpc,
                    RegionRuntimeSpec {
                        meta,
                        data_dir: region_data_dir(&base, region_id),
                        raft_config: Arc::new(RaftConfig {
                            heartbeat_interval: 50,
                            election_timeout_min: 200,
                            election_timeout_max: 400,
                            ..Default::default()
                        }),
                        object_store: None,
                        revision_mode: RevisionMode::CommitTs,
                    },
                    true,
                )
                .await
                .expect("spawn data region");
        }

        // region 模式下 KV 全量走 manager 路由；scratch 只满足构造签名。
        let backend = RedbBackend::open(&base, &StorageConfig::default()).unwrap();
        let scratch = Arc::new(MvccStorage::new(backend).unwrap());
        let mut node = CoordNode::new(scratch);
        node.node_id = 1;
        node.region_manager = Some(Arc::clone(&manager));
        node.set_kvt_state(
            KvTState::new(
                true,
                KvTRuntimeConfig {
                    max_range,
                    read_ts_wait_ms: 50,
                    ..KvTRuntimeConfig::default()
                },
            )
            .with_implemented([KvTCapability::Kv]),
        );
        let node = Arc::new(node);

        wait_for_leaders(&manager).await;
        Self {
            node,
            manager,
            _dir: dir,
        }
    }

    /// 直接向目标 Region 的 raft 提交 `PutV2`（与 G2 夹具同路径）。
    async fn put(&self, key: &[u8], value: &[u8], commit_ts: u64) {
        let rt = self.manager.route_runtime(key).expect("route key");
        let resp = rt
            .raft
            .client_write(Command::PutV2 {
                key: key.to_vec(),
                value: value.to_vec(),
                lease_id: None,
                commit_ts,
            })
            .await
            .expect("put v2");
        assert!(
            matches!(resp.response(), Response::Put { revision } if *revision == commit_ts),
            "region {commit_ts} put must apply at its commit_ts"
        );
    }

    async fn range(
        &self,
        key: &[u8],
        end: &[u8],
        read_ts: i64,
        limit: i64,
        keys_only: bool,
        count_only: bool,
    ) -> Result<RangeResponseV2, tonic::Status> {
        KvV2::range(
            &*self.node,
            tonic::Request::new(RangeRequestV2 {
                key: key.to_vec(),
                range_end: end.to_vec(),
                limit,
                read_ts,
                keys_only,
                count_only,
            }),
        )
        .await
        .map(|resp| resp.into_inner())
    }
}

async fn wait_for_leaders(manager: &RegionManager) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let ready = REGIONS.iter().all(|(id, _, _)| {
            manager.runtime(*id).is_some_and(|rt| {
                let m = rt.raft.metrics();
                let m = m.borrow_watched();
                m.current_leader == Some(1) && m.last_quorum_acked.is_some()
            })
        });
        if ready {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "region leaders not elected in time"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn keys(resp: &RangeResponseV2) -> Vec<Vec<u8>> {
    resp.kvs.iter().map(|kv| kv.key.clone()).collect()
}

/// 8 个 key 跨 3 个 Region（r1: a,a2,a3,b；r2: d,e,e2；r3: n,z），
/// 并把三区 current 抬到 >= 400（safe_ts = 400）。
async fn standard_data() -> FanoutNode {
    let fx = FanoutNode::start(100).await;
    fx.put(b"a", b"a100", 100).await;
    fx.put(b"b", b"b200", 200).await;
    fx.put(b"a2", b"a260", 260).await;
    fx.put(b"a3", b"a400", 400).await;
    fx.put(b"d", b"d100", 100).await;
    fx.put(b"e", b"e250", 250).await;
    fx.put(b"e2", b"e420", 420).await;
    fx.put(b"n", b"n100", 100).await;
    fx.put(b"z", b"z400", 400).await;
    assert_eq!(fx.node.safe_ts(), 400);
    fx
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_range_reads_all_segments_at_one_read_ts() {
    let fx = FanoutNode::start(100).await;
    fx.put(b"a", b"a100", 100).await;
    fx.put(b"b", b"b200", 200).await;
    fx.put(b"d", b"d100", 100).await;
    fx.put(b"e", b"e250", 250).await;
    fx.put(b"n", b"n100", 100).await;
    fx.put(b"z", b"z400", 400).await;

    // safe_ts = min(200, 250, 400) = 200：领先 Region 只暴露 <= 200 的历史。
    let r = fx.range(b"a", b"zzz", 0, 0, false, false).await.unwrap();
    assert_eq!(r.read_ts, 200);
    assert_eq!(
        keys(&r),
        vec![b"a".to_vec(), b"b".to_vec(), b"d".to_vec(), b"n".to_vec()]
    );
    assert!(r.kvs.iter().all(|kv| kv.mod_revision <= 200));

    // 显式 read_ts <= safe_ts：三区同一快照，不等待。
    let r = fx.range(b"a", b"zzz", 100, 0, false, false).await.unwrap();
    assert_eq!(r.read_ts, 100);
    assert_eq!(keys(&r), vec![b"a".to_vec(), b"d".to_vec(), b"n".to_vec()]);

    // 抬 r1 到 260 → safe_ts = 250，e@250 进入快照。
    fx.put(b"a2", b"a260", 260).await;
    let r = fx.range(b"a", b"zzz", 0, 0, false, false).await.unwrap();
    assert_eq!(r.read_ts, 250);
    assert_eq!(
        keys(&r),
        vec![
            b"a".to_vec(),
            b"b".to_vec(),
            b"d".to_vec(),
            b"e".to_vec(),
            b"n".to_vec()
        ]
    );

    // 抬三区到 400+ → 全量快照（e2@420 仍排除）。
    fx.put(b"a3", b"a400", 400).await;
    fx.put(b"e2", b"e420", 420).await;
    let r = fx.range(b"a", b"zzz", 0, 0, false, false).await.unwrap();
    assert_eq!(r.read_ts, 400);
    assert_eq!(
        keys(&r),
        vec![
            b"a".to_vec(),
            b"a2".to_vec(),
            b"a3".to_vec(),
            b"b".to_vec(),
            b"d".to_vec(),
            b"e".to_vec(),
            b"n".to_vec(),
            b"z".to_vec(),
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_range_is_half_open_at_region_boundaries_and_single_key() {
    let fx = standard_data().await;
    let r = fx.range(b"a", b"n", 400, 0, false, false).await.unwrap();
    assert_eq!(
        keys(&r),
        vec![
            b"a".to_vec(),
            b"a2".to_vec(),
            b"a3".to_vec(),
            b"b".to_vec(),
            b"d".to_vec(),
            b"e".to_vec(),
        ],
        "跨 Region 的 end 排除（n 在末段起点）"
    );

    let r = fx.range(b"d", b"n", 400, 0, false, false).await.unwrap();
    assert_eq!(
        keys(&r),
        vec![b"d".to_vec(), b"e".to_vec()],
        "start 包含、end 排除"
    );

    let r = fx.range(b"d", b"", 400, 0, false, false).await.unwrap();
    assert_eq!(keys(&r), vec![b"d".to_vec()], "空 range_end = 单键");

    let r = fx.range(b"n", b"d", 400, 0, false, false).await.unwrap();
    assert!(r.kvs.is_empty());
    assert_eq!(r.read_ts, 400);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_range_zero_snapshot_is_empty_across_regions() {
    let fx = FanoutNode::start(100).await;
    let r = fx.range(b"a", b"zzz", 0, 0, false, false).await.unwrap();
    assert_eq!(r.read_ts, 0, "safe_ts=0 时快照 revision 为 0");
    assert!(r.kvs.is_empty());
    assert_eq!(r.count, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_range_limit_count_and_keys_only_are_global() {
    let fx = standard_data().await;
    let r = fx.range(b"a", b"zzz", 400, 3, false, false).await.unwrap();
    assert_eq!(
        keys(&r),
        vec![b"a".to_vec(), b"a2".to_vec(), b"a3".to_vec()]
    );
    assert_eq!(r.count, 3, "limit 是整请求上限，不是每段上限");

    // limit 跨越 Region 1 → 2 的边界：仍按 key 序取全局前 N。
    let r = fx.range(b"a", b"zzz", 400, 5, false, false).await.unwrap();
    assert_eq!(
        keys(&r),
        vec![
            b"a".to_vec(),
            b"a2".to_vec(),
            b"a3".to_vec(),
            b"b".to_vec(),
            b"d".to_vec()
        ]
    );
    assert_eq!(r.count, 5);

    let r = fx.range(b"a", b"zzz", 400, 0, true, false).await.unwrap();
    assert_eq!(r.kvs.len(), 8);
    assert!(r.kvs.iter().all(|kv| kv.value.is_empty()));

    let r = fx.range(b"a", b"zzz", 400, 0, false, true).await.unwrap();
    assert_eq!(r.count, 8);
    assert!(r.kvs.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_range_count_only_caps_across_segments() {
    let fx = FanoutNode::start(4).await;
    // 每个 Region 内 commit_ts 严格递增；三区 min = 101，6 个 key 都可见。
    fx.put(b"a", b"a", 100).await;
    fx.put(b"b", b"b", 101).await;
    fx.put(b"d", b"d", 100).await;
    fx.put(b"e", b"e", 101).await;
    fx.put(b"n", b"n", 100).await;
    fx.put(b"o", b"o", 101).await;

    let err = fx
        .range(b"a", b"zzz", 0, 0, false, true)
        .await
        .expect_err("count-only over max_range must fail closed");
    assert_eq!(err.code(), Code::InvalidArgument);
    assert_eq!(error_code_of(&err).as_deref(), Some("READ_SET_TOO_LARGE"));

    let err = fx
        .range(b"a", b"zzz", 0, 5, false, false)
        .await
        .expect_err("explicit limit over max_range must be rejected");
    assert_eq!(error_code_of(&err).as_deref(), Some("READ_SET_TOO_LARGE"));

    let r = fx.range(b"a", b"zzz", 0, 4, false, false).await.unwrap();
    assert_eq!(r.kvs.len(), 4);
    assert_eq!(r.count, 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_range_fails_closed_when_a_region_is_missing() {
    let fx = standard_data().await;
    fx.manager.remove_region_runtime(2).unwrap();
    let err = fx
        .range(b"a", b"zzz", 400, 0, false, false)
        .await
        .expect_err("gap must fail the whole request, not return a partial prefix");
    assert_eq!(err.code(), Code::Unavailable, "{err:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_v1_cross_region_range_is_still_rejected() {
    let fx = standard_data().await;
    let err = coord_proto::kv::kv_server::Kv::range(
        &*fx.node,
        tonic::Request::new(coord_proto::kv::RangeRequest {
            key: b"a".to_vec(),
            range_end: b"zzz".to_vec(),
            ..Default::default()
        }),
    )
    .await
    .expect_err("the v1 data plane is frozen on a v2 cluster");
    assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");
    assert!(
        err.message()
            .contains("v1 data plane is frozen on a v2 cluster"),
        "freeze message must stay diagnosable: {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_range_fails_closed_when_one_segment_read_is_compacted() {
    let fx = standard_data().await;
    // 只压缩 Region 2；读快照落在压缩水位之前 → 该分段 OUT_OF_RANGE。
    fx.manager
        .runtime(2)
        .unwrap()
        .mvcc
        .apply_compact(300, AppliedLogId::standalone(999))
        .unwrap();

    let err = fx
        .range(b"a", b"zzz", 200, 0, false, false)
        .await
        .expect_err("a failing segment must fail the whole request");
    assert_eq!(err.code(), Code::OutOfRange, "{err:?}");
    assert_eq!(error_code_of(&err).as_deref(), Some("OUT_OF_RANGE"));
}
