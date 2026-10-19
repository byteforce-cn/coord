//! KV-T G2（#142）全局 SafeTs 验收：
//!
//! - `safe_ts = min(各数据 Region 已 apply 的 commit_ts 水位)`（legacy = region 0）；
//! - region 0 发布 TSO 高水位 fence，各 Region leader 以 raft 命令抬升水位；
//! - v2 Range 的 `read_ts=0`/显式 `read_ts` 以全局 safe_ts 为准；
//! - `GetTs` 不再发放 `<= published fence` 的值；
//! - 指标暴露 safe_ts 与每 Region apply lag。

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use coord_core::error::Result as CoordResult;
use coord_core::error_code::error_code_of;
use coord_proto::tso::v2::tso_server::Tso as TsoV2;
use coord_core::storage::StorageBackend;
use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionMeta, StorageConfig};
use coord_proto::kv::v2::kv_server::Kv as KvV2;
use coord_proto::kv::v2::RangeRequest as RangeRequestV2;
use coord_server::kvt::{KvTCapability, KvTRuntimeConfig, KvTState};
use tonic::Code;
use coord_server::metrics::Metrics;
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcService};
use coord_server::raft::region::RegionManager;
use coord_server::raft::region_runtime::{region_data_dir, RegionRuntimeSpec};
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::type_config::{Command, Response};
use coord_server::raft::{new_basic_node, new_raft, RaftConfig, WatchReceiver};
use coord_server::server::CoordNode;
use coord_server::storage::mvcc::{AppliedLogId, MvccStorage, RevisionMode};
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::raft::tso::{TsoBatch, TsoClient, TsoSource};
use coord_server::storage::snapshot::SnapshotTracker;
use tempfile::TempDir;

fn find_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn raft_config() -> RaftConfig {
    RaftConfig {
        heartbeat_interval: 200,
        election_timeout_min: 800,
        election_timeout_max: 1_500,
        ..Default::default()
    }
}

#[allow(dead_code)]
fn applied(index: u64) -> AppliedLogId {
    AppliedLogId {
        term: 1,
        node_id: 1,
        index,
    }
}

/// 进程内 TSO 源：只验证 GetTs 的 fence 跳值，不启动真实 region 0 allocator。
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

/// 单节点、region 0 system raft + 2 个数据 Region 的 G2 夹具。
///
/// 数据 Region 平铺 `["", "m")` / `["m", "")`；每个 Region 单 voter，因此
/// 进程内即可选主/提交，不需要 gRPC raft RPC server。
struct G2Node {
    node: Arc<CoordNode>,
    manager: Arc<RegionManager>,
    metrics: Arc<Metrics>,
    _dir: TempDir,
    _factory: RaftNetworkFactoryImpl,
    _rpc: RaftRpcService,
}

impl G2Node {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().to_path_buf();
        let addr = format!("127.0.0.1:{}", find_port());

        // region 0 system raft（TSO/fence 的权威日志）
        let root_dir = base.join("region-0");
        std::fs::create_dir_all(&root_dir).unwrap();
        let backend = RedbBackend::open(&root_dir, &StorageConfig::default()).unwrap();
        let mvcc = Arc::new(
            MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs).unwrap(),
        );
        let root_factory = RaftNetworkFactoryImpl::new(1);
        root_factory.register_node(1, addr.clone());
        let root_tracker = Arc::new(SnapshotTracker::default());
        let root_log = LogStore::new(&root_dir).await.unwrap();
        let root_sm = StateMachineStore::new(
            Arc::clone(&mvcc),
            root_dir.join("snapshots"),
            root_tracker,
        );
        let root_raft = Arc::new(
            new_raft(
                1,
                Arc::new(raft_config()),
                root_factory,
                root_log,
                root_sm,
            )
            .await
            .expect("create region 0 raft"),
        );
        root_raft
            .initialize(BTreeMap::from([(1u64, new_basic_node(&addr))]))
            .await
            .expect("initialize region 0 raft");

        // 数据 Region 1/2（共享节点网络工厂；单 voter 不需要真实 RPC server）
        let factory = RaftNetworkFactoryImpl::new(1);
        factory.register_node(1, addr.clone());
        let rpc = RaftRpcService::new();
        let manager = Arc::new(RegionManager::new(1));
        for (region_id, start, end) in [
            (1u64, b"".to_vec(), b"m".to_vec()),
            (2u64, b"m".to_vec(), b"".to_vec()),
        ] {
            let meta = RegionMeta {
                region_id,
                start_key: start,
                end_key: end,
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
                        raft_config: Arc::new(raft_config()),
                        object_store: None,
                        revision_mode: RevisionMode::CommitTs,
                    },
                    true,
                )
                .await
                .expect("spawn data region");
        }

        let mut node = CoordNode::new(Arc::clone(&mvcc));
        node.node_id = 1;
        node.raft = Some(root_raft);
        node.region_manager = Some(Arc::clone(&manager));
        node.set_kvt_state(
            KvTState::new(
                true,
                KvTRuntimeConfig {
                    read_ts_wait_ms: 50,
                    ..KvTRuntimeConfig::default()
                },
            )
                .with_implemented([KvTCapability::Kv, KvTCapability::Tso]),
        );
        node.set_tso_client(TsoClient::new(FakeTso::new(), 8, None));
        let metrics = Arc::new(Metrics::new());
        node.set_metrics(Arc::clone(&metrics));
        let node = Arc::new(node);

        wait_for_leaders(&node, &manager).await;

        Self {
            node,
            manager,
            metrics,
            _dir: dir,
            _factory: factory,
            _rpc: rpc,
        }
    }

    #[allow(dead_code)]
    fn runtime(&self, region_id: u64) -> Arc<coord_server::raft::region_runtime::RegionRuntime> {
        self.manager.runtime(region_id).expect("region runtime")
    }

    async fn put_v2(&self, key: &[u8], value: &[u8], commit_ts: u64) -> Response {
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
        resp.response().clone()
    }

    fn metrics_text(&self) -> String {
        self.metrics.render_prometheus_text()
    }
}

async fn wait_for_leaders(node: &CoordNode, manager: &RegionManager) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let root_leader = node
            .raft
            .as_ref()
            .expect("region 0 raft")
            .current_leader()
            .await
            == Some(1);
        let data_ready = (1..=2).all(|id| {
            manager.runtime(id).is_some_and(|rt| {
                let m = rt.raft.metrics();
                let m = m.borrow_watched();
                m.current_leader == Some(1) && m.last_quorum_acked.is_some()
            })
        });
        if root_leader && data_ready {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "raft leaders not elected in time"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn range_req(key: &[u8], range_end: &[u8], read_ts: i64) -> RangeRequestV2 {
    RangeRequestV2 {
        key: key.to_vec(),
        range_end: range_end.to_vec(),
        limit: 0,
        read_ts,
        keys_only: false,
        count_only: false,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_safe_ts_is_min_region_revision_and_metrics() {
    let g2 = G2Node::start().await;

    assert!(matches!(
        g2.put_v2(b"apple", b"v", 100).await,
        Response::Put { revision: 100 }
    ));
    assert!(matches!(
        g2.put_v2(b"melon", b"v", 60).await,
        Response::Put { revision: 60 }
    ));

    assert_eq!(
        g2.node.safe_ts(),
        60,
        "safe_ts must be the minimum applied commit_ts watermark across regions"
    );
    assert_eq!(g2.node.safe_ts_fence(), 0, "no fence published yet");

    let text = g2.metrics_text();
    assert!(text.contains("coord_kv_t_safe_ts 60"), "{text}");
    assert!(
        text.contains("coord_region_applied_revision{region_id=\"1\"} 100"),
        "{text}"
    );
    assert!(
        text.contains("coord_region_applied_revision{region_id=\"2\"} 60"),
        "{text}"
    );
    assert!(
        text.contains("coord_region_apply_lag{region_id=\"1\"} 0"),
        "{text}"
    );
    assert!(
        text.contains("coord_region_apply_lag{region_id=\"2\"} 40"),
        "{text}"
    );

    // 直接通过 trait 读 Range 的测试在 Task 5 加入；这里只验证观测面。
    let _ = Arc::clone(&g2.manager);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_get_ts_stays_above_published_fence() {
    let g2 = G2Node::start().await;
    assert_eq!(
        g2.node
            .storage
            .set_safe_ts_fence(1025, applied(1))
            .unwrap(),
        1025
    );

    let ts = g2
        .node
        .get_ts(tonic::Request::new(coord_proto::tso::v2::GetTsRequest {}))
        .await
        .expect("GetTs")
        .into_inner()
        .ts;
    assert!(
        ts > 1025,
        "GetTs must not issue a value the published fence already rejects (got {ts})"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_advance_once_publishes_fence_and_fences_all_regions() {
    let g2 = G2Node::start().await;
    g2.node
        .storage
        .apply_tso_allocate(1024, 1, AppliedLogId::standalone(0))
        .expect("raise TSO high water");
    assert_eq!(g2.node.storage.tso_high_water().unwrap(), 1025);

    let advanced = coord_server::kvt::safe_ts::advance_once(&g2.node)
        .await
        .expect("advance safe ts");
    assert_eq!(advanced, 2, "both local region leaders must be fenced");
    assert_eq!(g2.node.safe_ts_fence(), 1025);
    for region_id in [1u64, 2] {
        assert_eq!(
            g2.runtime(region_id).mvcc.current_revision(),
            1025,
            "region {region_id} applied watermark"
        );
        assert!(
            !g2.runtime(region_id)
                .mvcc
                .changelog_contains_revision(1025)
                .unwrap(),
            "fence must not write a changelog entry"
        );
    }
    assert_eq!(g2.node.safe_ts(), 1025);

    // 高水位未变：第二轮不发布/不重复 fence。
    let advanced = coord_server::kvt::safe_ts::advance_once(&g2.node)
        .await
        .expect("second advance");
    assert_eq!(advanced, 0);
    assert_eq!(g2.node.safe_ts(), 1025);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_idle_regions_do_not_pin_safe_ts() {
    let g2 = G2Node::start().await;
    g2.node
        .storage
        .apply_tso_allocate(512, 1, AppliedLogId::standalone(0))
        .unwrap();

    assert_eq!(
        g2.node.safe_ts(),
        0,
        "before the first fence both idle regions are at revision 0"
    );
    coord_server::kvt::safe_ts::advance_once(&g2.node)
        .await
        .expect("advance safe ts");
    assert_eq!(
        g2.node.safe_ts(),
        513,
        "an idle region must not pin safe_ts forever"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_fence_rejects_stale_commit_ts_without_raft_error() {
    let g2 = G2Node::start().await;
    g2.node
        .storage
        .apply_tso_allocate(256, 1, AppliedLogId::standalone(0))
        .unwrap();
    coord_server::kvt::safe_ts::advance_once(&g2.node)
        .await
        .expect("advance safe ts");
    let fence = g2.node.safe_ts_fence();
    assert!(fence > 0);

    let rt = g2.manager.route_runtime(b"apple").unwrap();
    let stale = rt
        .raft
        .client_write(Command::PutV2 {
            key: b"apple".to_vec(),
            value: b"v".to_vec(),
            lease_id: None,
            commit_ts: fence,
        })
        .await
        .expect("user-level rejection must not fail raft");
    assert!(
        matches!(
            stale.response(),
            Response::CommitRejected {
                commit_ts,
                current_revision
            } if *commit_ts == fence && *current_revision == fence
        ),
        "unexpected response: {:?}",
        stale.response()
    );

    assert!(matches!(
        g2.put_v2(b"apple", b"v", fence + 1).await,
        Response::Put { revision } if revision == fence + 1
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_range_read_ts_zero_uses_global_safe_ts() {
    let g2 = G2Node::start().await;
    assert!(matches!(
        g2.put_v2(b"apple", b"a", 2_000).await,
        Response::Put { revision: 2_000 }
    ));
    assert!(matches!(
        g2.put_v2(b"melon", b"b", 1_500).await,
        Response::Put { revision: 1_500 }
    ));

    // fence 到 1800：region1 仍领先（2000），region2 被抬到 1800；
    // safe_ts=1800，因此 read_ts=0 不得看到 2000 的写入。
    g2.node
        .storage
        .apply_tso_allocate(1_799, 1, AppliedLogId::standalone(0))
        .unwrap();
    coord_server::kvt::safe_ts::advance_once(&g2.node)
        .await
        .expect("first fence");
    assert_eq!(g2.node.safe_ts(), 1_800);

    let early = g2
        .node
        .range(tonic::Request::new(range_req(b"apple", b"", 0)))
        .await
        .expect("range at safe_ts")
        .into_inner();
    assert_eq!(early.read_ts, 1_800, "header must echo the global safe_ts");
    assert!(
        early.kvs.is_empty(),
        "commit_ts 2000 is beyond the global safe_ts: {:?}",
        early.kvs
    );

    // 再 fence 到 2200：所有 Region 都越过 2000，read_ts=0 才可见。
    g2.node
        .storage
        .apply_tso_allocate(400, 2, AppliedLogId::standalone(1))
        .unwrap();
    coord_server::kvt::safe_ts::advance_once(&g2.node)
        .await
        .expect("second fence");
    assert_eq!(g2.node.safe_ts(), 2_200);

    let late = g2
        .node
        .range(tonic::Request::new(range_req(b"apple", b"", 0)))
        .await
        .expect("range after catch-up")
        .into_inner();
    assert_eq!(late.read_ts, 2_200);
    assert_eq!(late.kvs.len(), 1);
    assert_eq!(late.kvs[0].value, b"a");
    assert_eq!(late.kvs[0].mod_revision, 2_000);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_explicit_read_ts_waits_for_lagging_region_then_safe_ts_lag() {
    let g2 = G2Node::start().await;
    assert!(matches!(
        g2.put_v2(b"apple", b"a", 2_000).await,
        Response::Put { revision: 2_000 }
    ));
    assert!(matches!(
        g2.put_v2(b"melon", b"b", 1_500).await,
        Response::Put { revision: 1_500 }
    ));
    g2.node
        .storage
        .apply_tso_allocate(1_799, 1, AppliedLogId::standalone(0))
        .unwrap();
    coord_server::kvt::safe_ts::advance_once(&g2.node)
        .await
        .expect("fence");
    assert_eq!(g2.node.safe_ts(), 1_800);

    // region1 已 apply 2000，但 region2 只到 1800：read_ts=2000 必须等待
    // 全局水位，而不是只看目标 Region。
    let started = tokio::time::Instant::now();
    let err = g2
        .node
        .range(tonic::Request::new(range_req(b"apple", b"", 2_000)))
        .await
        .expect_err("read_ts above the lagging region's watermark must time out");
    assert_eq!(err.code(), Code::Unavailable, "{err:?}");
    assert_eq!(
        error_code_of(&err).as_deref(),
        Some("SAFE_TS_LAG"),
        "{err:?}"
    );
    assert!(
        started.elapsed() >= Duration::from_millis(40),
        "must respect read_ts_wait_ms"
    );

    let text = g2.metrics_text();
    assert!(
        text.contains("coord_kv_v2_safe_ts_wait_total 1"),
        "{text}"
    );
    assert!(
        text.contains("coord_kv_v2_safe_ts_lag_total 1"),
        "{text}"
    );
}
