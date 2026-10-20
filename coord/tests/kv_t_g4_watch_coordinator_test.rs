//! KV-T G4 Watch Coordinator 端到端验收：
//!
//! - 写入 → 决策 apply → outbox 搬入 shard 组 → Watch 按 `commit_ts` 全局有序读出；
//! - 同一事务跨两个数据 Region 的分片合并为一个原子事件；
//! - cursor 在读取端重建后无丢失、无重复；
//! - key/range 过滤不改变 cursor 推进；
//! - 保留窗口到点后整 shard 回收，旧起点返回 `HISTORY_UNAVAILABLE`。
//!
//! v2 Watch RPC 由后续 issue 覆盖。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use coord_core::error::Result as CoordResult;
use coord_core::storage::StorageBackend;
use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionMeta, StorageConfig};
use coord_proto::txn::v2::mutation::Op;
use coord_proto::txn::v2::txn_server::Txn as TxnV2;
use coord_proto::txn::v2::{BeginRequest, CommitRequest, Mutation};
use coord_server::kvt::commit_log_shards::CommitLogShardManager;
use coord_server::kvt::decision_apply::apply_pending_decisions;
use coord_server::kvt::sequencer::CommitSequencer;
use coord_server::kvt::watch::{
    WatchCoordinator, WatchCursor, WatchError, WatchEvent, WatchFilter, WatchStart,
};
use coord_server::kvt::{KvTCapability, KvTRuntimeConfig, KvTState};
use coord_server::metrics::Metrics;
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcService};
use coord_server::raft::region::RegionManager;
use coord_server::raft::region_runtime::{region_data_dir, RegionRuntimeSpec};
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::tso::{TsoBatch, TsoClient, TsoSource};
use coord_server::raft::type_config::SequencerValidationLimits;
use coord_server::raft::{new_basic_node, new_raft, RaftConfig, WatchReceiver};
use coord_server::server::CoordNode;
use coord_server::storage::mvcc::{MvccStorage, RevisionMode};
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::snapshot::SnapshotTracker;
use tempfile::TempDir;

/// 种子数据用的 commit_ts（低于后续事务的分配值）。
const SEED_TS: u64 = 100;

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

/// 进程内 TSO 源：只验证决策与记录落盘，不启动真实 allocator。
struct FakeTso {
    high_water: std::sync::atomic::AtomicU64,
}

impl FakeTso {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            high_water: std::sync::atomic::AtomicU64::new(SEED_TS + 10),
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
            .fetch_add(u64::from(batch_size), std::sync::atomic::Ordering::SeqCst);
        Ok(TsoBatch {
            start_ts: start,
            end_ts: start + u64::from(batch_size),
        })
    }
}

struct WatchNode {
    node: Arc<CoordNode>,
    manager: Arc<RegionManager>,
    shards: Arc<CommitLogShardManager>,
    now: Arc<AtomicU64>,
    _metrics: Arc<Metrics>,
    _dir: TempDir,
    _factory: RaftNetworkFactoryImpl,
    _rpc: RaftRpcService,
}

impl WatchNode {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().to_path_buf();
        let addr = format!("127.0.0.1:{}", find_port());

        // region 0 system raft（shard 元数据权威）
        let root_dir = base.join("region-0");
        std::fs::create_dir_all(&root_dir).unwrap();
        let backend = RedbBackend::open(&root_dir, &StorageConfig::default()).unwrap();
        let root_mvcc =
            Arc::new(MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs).unwrap());
        let root_factory = RaftNetworkFactoryImpl::new(1);
        root_factory.register_node(1, addr.clone());
        let root_log = LogStore::new(&root_dir).await.unwrap();
        let root_sm = StateMachineStore::new(
            Arc::clone(&root_mvcc),
            root_dir.join("snapshots"),
            Arc::new(SnapshotTracker::default()),
        );
        let root_raft = Arc::new(
            new_raft(1, Arc::new(raft_config()), root_factory, root_log, root_sm)
                .await
                .expect("create region 0 raft"),
        );
        root_raft
            .initialize(std::collections::BTreeMap::from([(
                1u64,
                new_basic_node(&addr),
            )]))
            .await
            .expect("initialize region 0 raft");

        // 两个数据 Region（[.., "m") / ["m", ..)）
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

        let metrics = Arc::new(Metrics::new());
        let mut node = CoordNode::new(Arc::clone(&root_mvcc));
        node.node_id = 1;
        node.raft = Some(Arc::clone(&root_raft));
        node.region_manager = Some(Arc::clone(&manager));
        node.set_kvt_state(
            KvTState::new(
                true,
                KvTRuntimeConfig {
                    read_ts_wait_ms: 50,
                    // 一条记录即触发定界/预创建，让测试真正跨 shard 读取。
                    commit_log_shard_records: 1,
                    // 回收窗口用测试时钟推进，不需要真的等 24h。
                    watch_retention: 60,
                    ..KvTRuntimeConfig::default()
                },
            )
            .with_implemented([
                KvTCapability::Kv,
                KvTCapability::Tso,
                KvTCapability::Txn,
            ]),
        );
        node.set_metrics(Arc::clone(&metrics));
        let tso = TsoClient::new(FakeTso::new(), 1024, None);
        node.set_tso_client(Arc::clone(&tso));
        node.set_commit_sequencer(Arc::new(CommitSequencer::new(
            root_raft.as_ref().clone(),
            1,
            tso,
            Arc::clone(&root_mvcc),
            256,
            4 * 1024 * 1024,
            SequencerValidationLimits {
                max_read_set: 4_096,
                max_range: 10_000,
            },
            Duration::from_secs(5),
        )));

        let now = Arc::new(AtomicU64::new(1_000_000));
        let clock_now = Arc::clone(&now);
        let clock: Arc<dyn Fn() -> u64 + Send + Sync> =
            Arc::new(move || clock_now.load(Ordering::SeqCst));
        let shards = Arc::new(
            CommitLogShardManager::new(
                1,
                root_raft.as_ref().clone(),
                Arc::clone(&root_mvcc),
                Arc::clone(&manager),
                factory.clone(),
                rpc.clone(),
                base,
                Arc::new(raft_config()),
                node.kvt().config().clone(),
            )
            .with_clock(clock),
        );
        node.set_commit_log_shards(Arc::clone(&shards));

        let node = Arc::new(node);
        wait_for_leaders(&node, &manager).await;
        // 种子数据：两个 Region 各有已提交值，prev 值可用于校验完整记录。
        let fixture = Self {
            node,
            manager,
            shards,
            now,
            _metrics: metrics,
            _dir: dir,
            _factory: factory,
            _rpc: rpc,
        };
        fixture.put_v2(b"aa", b"seed", SEED_TS).await;
        fixture.put_v2(b"zz", b"seed", SEED_TS).await;
        fixture
    }

    async fn put_v2(&self, key: &[u8], value: &[u8], commit_ts: u64) {
        let rt = self.manager.route_runtime(key).expect("route key");
        rt.raft
            .client_write(coord_server::raft::type_config::Command::PutV2 {
                key: key.to_vec(),
                value: value.to_vec(),
                lease_id: None,
                commit_ts,
            })
            .await
            .expect("put v2");
    }

    async fn begin(&self, start_ts: i64) -> coord_proto::txn::v2::BeginResponse {
        TxnV2::begin(
            &*self.node,
            tonic::Request::new(BeginRequest {
                start_ts,
                ttl_ms: 0,
            }),
        )
        .await
        .expect("begin txn")
        .into_inner()
    }

    async fn commit_puts(&self, txn_id: &[u8], pairs: &[(&[u8], &[u8])], request_id: &[u8]) -> u64 {
        let mutations = pairs
            .iter()
            .map(|(key, value)| Mutation {
                op: Op::Put as i32,
                key: key.to_vec(),
                range_end: Vec::new(),
                value: value.to_vec(),
                lease_id: 0,
            })
            .collect();
        TxnV2::commit(
            &*self.node,
            tonic::Request::new(CommitRequest {
                txn_id: txn_id.to_vec(),
                mutations,
                request_id: request_id.to_vec(),
            }),
        )
        .await
        .expect("commit txn")
        .into_inner()
        .commit_ts as u64
    }

    /// 提交一个事务、应用到数据 Region、推进一轮 shard 协调。
    ///
    /// 返回 `commit_ts`；调用方据此定位 Watch 起点。
    async fn commit_pairs(&self, pairs: &[(&[u8], &[u8])]) -> u64 {
        let txn = self.begin(SEED_TS as i64).await;
        let commit_ts = self.commit_puts(&txn.txn_id, pairs, &txn.txn_id).await;
        apply_pending_decisions(&self.node, 16)
            .await
            .expect("apply decisions to data regions");
        self.settle(commit_ts).await;
        commit_ts
    }

    /// 反复 tick 直到该 `commit_ts` 的 outbox 记录已搬入 shard、watermark 越过它。
    ///
    /// shard raft 组在首轮 tick 里才装配，leader 选举完成前 `ship_shard` 只跳过；
    /// 测试必须等到搬运真正完成，否则会在「open shard + in-flight」状态上断言。
    async fn settle(&self, commit_ts: u64) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            self.shards.tick().await.expect("shard tick");
            let watermark = self
                .shards
                .local_watermark()
                .await
                .expect("local watermark")
                .watermark;
            if watermark >= commit_ts {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "commit_ts {commit_ts} not shipped before the deadline (watermark {watermark})"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// 打开第一个 shard（region 0 leader 的首轮 tick）。
    async fn open_first_shard(&self) {
        self.shards.tick().await.expect("first shard tick");
    }

    fn advance_clock_ms(&self, delta: u64) {
        self.now.fetch_add(delta, Ordering::SeqCst);
    }

    /// 反复 tick 直到指定 shard 已 seal（seal 依赖 watermark 越过 end_ts）。
    async fn wait_until_sealed(&self, shard_id: u64) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            self.shards.tick().await.expect("shard tick");
            let table = self.shards.table().expect("shard table");
            if table
                .shards
                .iter()
                .any(|meta| meta.shard_id == shard_id && meta.sealed_at_ms != 0)
            {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "commit log shard {shard_id} was not sealed in time"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn poll_until(
        &self,
        coordinator: &WatchCoordinator,
        mut cursor: WatchCursor,
        filter: WatchFilter,
        expected: usize,
    ) -> (Vec<WatchEvent>, WatchCursor) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let mut events = Vec::new();
        loop {
            let batch = coordinator.poll(cursor, &filter).await.expect("watch poll");
            cursor = batch.cursor;
            events.extend(batch.events);
            if events.len() >= expected {
                return (events, cursor);
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "watch poll timed out with {} of {expected} events",
                events.len()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_watch_global_order_by_commit_ts() {
    let fx = WatchNode::start().await;
    fx.open_first_shard().await;
    let ts1 = fx.commit_pairs(&[(b"aa", b"v1")]).await;
    let ts2 = fx.commit_pairs(&[(b"bb", b"v2")]).await;
    assert!(ts2 > ts1);

    let coordinator = WatchCoordinator::new(Arc::clone(&fx.shards));
    let cursor = coordinator
        .start(WatchStart {
            start_revision: ts1,
            start_cursor: None,
        })
        .await
        .unwrap();
    let (events, cursor) = fx
        .poll_until(&coordinator, cursor, WatchFilter::all(), 2)
        .await;
    let timestamps: Vec<u64> = events.iter().map(|event| event.commit_ts).collect();
    assert_eq!(timestamps, vec![ts1, ts2], "strict global order");
    assert_eq!(cursor.commit_ts, ts2);
    let table = fx.shards.table().unwrap();
    assert!(
        table.shards.len() >= 2 && table.shards[0].sealed_at_ms > 0,
        "the events must span a sealed shard boundary: {table:?}"
    );

    let (again, _) = fx
        .poll_until(&coordinator, cursor, WatchFilter::all(), 0)
        .await;
    assert!(
        again.is_empty(),
        "cursor at the watermark must not repeat events"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_watch_atomic_multi_key_txn() {
    let fx = WatchNode::start().await;
    fx.open_first_shard().await;
    let ts = fx.commit_pairs(&[(b"aa", b"v1"), (b"zz", b"v2")]).await;

    let coordinator = WatchCoordinator::new(Arc::clone(&fx.shards));
    let start = WatchStart {
        start_revision: ts,
        start_cursor: None,
    };
    let cursor = coordinator.start(start.clone()).await.unwrap();
    let (events, _) = fx
        .poll_until(&coordinator, cursor, WatchFilter::all(), 1)
        .await;
    assert_eq!(events.len(), 1, "one txn = one event");
    assert_eq!(events[0].commit_ts, ts);
    let mut keys: Vec<Vec<u8>> = events[0]
        .changes
        .iter()
        .map(|change| change.key.clone())
        .collect();
    keys.sort();
    assert_eq!(keys, vec![b"aa".to_vec(), b"zz".to_vec()]);

    let only_aa = WatchFilter {
        key: b"aa".to_vec(),
        ..WatchFilter::all()
    };
    let cursor = coordinator.start(start).await.unwrap();
    let (filtered, _) = fx.poll_until(&coordinator, cursor, only_aa, 1).await;
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].changes.len(), 1);
    assert_eq!(filtered[0].changes[0].key, b"aa");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_watch_cursor_resumes_without_gap_or_duplicate() {
    let fx = WatchNode::start().await;
    fx.open_first_shard().await;
    let ts1 = fx.commit_pairs(&[(b"aa", b"v1")]).await;
    let ts2 = fx.commit_pairs(&[(b"bb", b"v2")]).await;

    let reader = WatchCoordinator::new(Arc::clone(&fx.shards)).with_limits(1, 1024 * 1024);
    let cursor = reader
        .start(WatchStart {
            start_revision: ts1,
            start_cursor: None,
        })
        .await
        .unwrap();
    let first = reader.poll(cursor, &WatchFilter::all()).await.unwrap();
    assert_eq!(
        first
            .events
            .iter()
            .map(|event| event.commit_ts)
            .collect::<Vec<_>>(),
        vec![ts1]
    );
    assert!(first.has_more, "the batch cap stops before the next event");

    // 新的协调器 = 读取端重建；cursor 只依赖持久化的 shard 日志。
    let resumed = WatchCoordinator::new(Arc::clone(&fx.shards)).with_limits(1, 1024 * 1024);
    let (rest, _) = fx
        .poll_until(&resumed, first.cursor, WatchFilter::all(), 1)
        .await;
    assert_eq!(
        rest.iter().map(|event| event.commit_ts).collect::<Vec<_>>(),
        vec![ts2],
        "no duplicate of the first event and no skipped commit_ts"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_watch_history_unavailable() {
    let fx = WatchNode::start().await;
    fx.open_first_shard().await;
    let first_ts = fx.commit_pairs(&[(b"aa", b"v1")]).await;
    // 第二个事务把 watermark 推过 shard 1 的 end_ts，使其 seal；它自己落进 shard 2。
    let second_ts = fx.commit_pairs(&[(b"bb", b"v2")]).await;
    assert!(second_ts > first_ts);
    fx.wait_until_sealed(1).await;

    let table = fx.shards.table().unwrap();
    let gc_before = table.shards[0].end_ts;
    assert_eq!(gc_before, second_ts, "shard 2 starts at the first retained ts");
    let sealed_at = table.shards[0].sealed_at_ms;
    assert!(sealed_at != 0);

    // 窗口最后 1ms：不回收，历史仍可读。
    fx.advance_clock_ms(59_999);
    fx.shards.tick().await.unwrap();
    assert_eq!(fx.shards.table().unwrap().gc_before_ts, 0);
    let reader = WatchCoordinator::new(Arc::clone(&fx.shards));
    let cursor = reader
        .start(WatchStart {
            start_revision: 1,
            start_cursor: None,
        })
        .await
        .expect("history is still inside the window");
    let (events, cursor) = fx
        .poll_until(&reader, cursor, WatchFilter::all(), 1)
        .await;
    assert_eq!(events[0].commit_ts, first_ts);
    let batch = reader.poll(cursor, &WatchFilter::all()).await.unwrap();
    assert_eq!(
        batch.oldest_available,
        WatchCursor::beginning(),
        "nothing has been reclaimed yet"
    );

    // 窗口到点：回收 shard 1；seal 状态保留，本地组与数据目录删除。
    fx.advance_clock_ms(1);
    fx.shards.tick().await.unwrap();
    let table = fx.shards.table().unwrap();
    assert_eq!(table.gc_before_ts, gc_before);
    assert_ne!(
        table.shards[0].sealed_at_ms, 0,
        "gc must not disturb the seal state"
    );
    assert!(fx.shards.runtime(1).is_none(), "the shard group is gone");

    // 旧起点 → HISTORY_UNAVAILABLE，且带上最老可用位点。
    let err = reader
        .start(WatchStart {
            start_revision: 1,
            start_cursor: None,
        })
        .await
        .unwrap_err();
    let oldest = match err {
        WatchError::HistoryUnavailable {
            requested,
            oldest_available,
        } => {
            assert_eq!(requested, 1);
            oldest_available
        }
        other => panic!("unexpected error: {other}"),
    };
    assert!(oldest.commit_ts + 1 >= gc_before);

    // 最老可用 cursor 仍可续订到第一个保留事件。
    let resumed = reader
        .start(WatchStart {
            start_revision: 0,
            start_cursor: Some(oldest),
        })
        .await
        .expect("the oldest available cursor must still resume");
    let (events, _) = fx
        .poll_until(&reader, resumed, WatchFilter::all(), 1)
        .await;
    assert_eq!(
        events[0].commit_ts, second_ts,
        "the first retained event is exactly the shard boundary"
    );

    // 重启读取端：状态来自 region 0 表，不会复活已回收 shard。
    let restarted = WatchCoordinator::new(Arc::clone(&fx.shards));
    assert!(matches!(
        restarted
            .start(WatchStart {
                start_revision: 1,
                start_cursor: None,
            })
            .await,
        Err(WatchError::HistoryUnavailable { .. })
    ));
    fx.shards.tick().await.unwrap();
    assert!(
        fx.shards.runtime(1).is_none(),
        "tick must not respawn a reclaimed shard"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_gc_keeps_unsealed_shard_and_its_history() {
    let fx = WatchNode::start().await;
    fx.open_first_shard().await;
    let first_ts = fx.commit_pairs(&[(b"aa", b"v1")]).await;

    // shard 1 已定界但 watermark 还没越过 end_ts（没有后续事务）；窗口再久也不回收。
    fx.advance_clock_ms(120_000);
    fx.shards.tick().await.unwrap();
    let table = fx.shards.table().unwrap();
    assert_eq!(table.gc_before_ts, 0);
    assert!(fx.shards.runtime(1).is_some());
    let reader = WatchCoordinator::new(Arc::clone(&fx.shards));
    let cursor = reader
        .start(WatchStart {
            start_revision: 1,
            start_cursor: None,
        })
        .await
        .expect("an unsealed shard keeps its history");
    let (events, cursor) = fx
        .poll_until(&reader, cursor, WatchFilter::all(), 1)
        .await;
    assert_eq!(events[0].commit_ts, first_ts);
    let batch = reader.poll(cursor, &WatchFilter::all()).await.unwrap();
    assert_eq!(
        batch.oldest_available,
        WatchCursor::beginning(),
        "nothing has been reclaimed yet"
    );

    // 后续事务让 watermark 越过 end_ts：seal 后窗口到点才回收。
    let second_ts = fx.commit_pairs(&[(b"bb", b"v2")]).await;
    fx.wait_until_sealed(1).await;
    fx.advance_clock_ms(60_000);
    fx.shards.tick().await.unwrap();
    let table = fx.shards.table().unwrap();
    assert_eq!(table.gc_before_ts, second_ts);
    assert!(fx.shards.runtime(1).is_none());

    let err = reader
        .start(WatchStart {
            start_revision: 1,
            start_cursor: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(err, WatchError::HistoryUnavailable { .. }));
    // 最老保留事件仍在 shard 2：从边界 revision 起读能看到它。
    let oldest = WatchCoordinator::new(Arc::clone(&fx.shards))
        .start(WatchStart {
            start_revision: second_ts,
            start_cursor: None,
        })
        .await
        .unwrap();
    let batch = reader.poll(oldest, &WatchFilter::all()).await.unwrap();
    assert_eq!(
        batch
            .events
            .iter()
            .map(|event| event.commit_ts)
            .collect::<Vec<_>>(),
        vec![second_ts],
        "the boundary event itself is retained"
    );
}
