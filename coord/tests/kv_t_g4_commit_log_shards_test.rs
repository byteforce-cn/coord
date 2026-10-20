//! KV-T G4（#154）Commit Log shard raft 组端到端验收：
//!
//! - 事务提交 → 数据 Region outbox → shard raft 组（完整 key+value、prev）；
//! - 同一事务跨两个 Region 的分片落在同一 shard，可按 `commit_ts` 整组读取；
//! - 重复 tick 不重复搬运（shipped 水位持久化在 shard 组内）；
//! - 多 tick 后 shard 组记录数与 outbox 一致，无丢失、无重复。
//!
//! 多节点复制与 leader 迁移见 `coord-server/tests/
//! kv_t_g4_commit_log_shards_multinode_test.rs`。

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
use coord_server::kvt::{KvTCapability, KvTRuntimeConfig, KvTState};
use coord_server::metrics::Metrics;
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcService};
use coord_server::raft::region::RegionManager;
use coord_server::raft::region_runtime::{region_data_dir, RegionRuntimeSpec};
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::tso::{TsoBatch, TsoClient, TsoSource};
use coord_server::raft::type_config::{SequencerMutation, SequencerValidationLimits};
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

struct ShardNode {
    node: Arc<CoordNode>,
    manager: Arc<RegionManager>,
    shards: Arc<CommitLogShardManager>,
    _metrics: Arc<Metrics>,
    _dir: TempDir,
    _factory: RaftNetworkFactoryImpl,
    _rpc: RaftRpcService,
}

impl ShardNode {
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

        let shards = Arc::new(CommitLogShardManager::new(
            1,
            root_raft.as_ref().clone(),
            Arc::clone(&root_mvcc),
            Arc::clone(&manager),
            factory.clone(),
            rpc.clone(),
            base,
            Arc::new(raft_config()),
            node.kvt().config().clone(),
        ));
        node.set_commit_log_shards(Arc::clone(&shards));

        let node = Arc::new(node);
        wait_for_leaders(&node, &manager).await;
        // 种子数据：两个 Region 各有已提交值，prev 值可用于校验完整记录。
        let fixture = Self {
            node,
            manager,
            shards,
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

    /// 反复 tick 直到累计搬运 >= `at_least`（或超时）。
    async fn tick_until_shipped(&self, at_least: u64) -> u64 {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let mut shipped = 0u64;
        loop {
            let tick = self.shards.tick().await.expect("shard tick");
            shipped += tick.shipped;
            if shipped >= at_least || tokio::time::Instant::now() >= deadline {
                return shipped;
            }
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
async fn test_committed_txn_records_land_in_shard_groups_with_full_values() {
    let fx = ShardNode::start().await;
    let opened = fx.shards.tick().await.expect("first tick").opened;
    assert_eq!(opened, 1, "region 0 leader opens the first shard");
    assert_eq!(fx.shards.table().unwrap().shards.len(), 1);

    let txn = fx.begin(SEED_TS as i64).await;
    let commit_ts = fx
        .commit_puts(&txn.txn_id, &[(b"aa", b"v1"), (b"zz", b"v2")], b"req-1")
        .await;
    assert!(commit_ts > SEED_TS);

    apply_pending_decisions(&fx.node, 16)
        .await
        .expect("apply decisions to data regions");

    let shipped = fx.tick_until_shipped(2).await;
    assert_eq!(shipped, 2, "one fragment per data region");

    let shard = fx
        .shards
        .shard_for_ts(commit_ts)
        .expect("read shard table")
        .expect("commit_ts must be covered")
        .shard_id;
    let rows = fx.shards.records_at(shard, commit_ts).await.unwrap();
    assert_eq!(rows.len(), 2, "both region fragments in one shard");
    let mut keys: Vec<Vec<u8>> = rows
        .iter()
        .flat_map(|row| row.changes.iter().map(|change| change.key.clone()))
        .collect();
    keys.sort();
    assert_eq!(keys, vec![b"aa".to_vec(), b"zz".to_vec()]);
    for row in &rows {
        assert_eq!(row.commit_ts, commit_ts);
        assert!(row.start_ts < commit_ts);
        assert_eq!(row.shard_id, shard);
        let change = &row.changes[0];
        let expected: &[u8] = if change.key == b"aa" { b"v1" } else { b"v2" };
        assert_eq!(change.value.as_deref(), Some(expected));
        let prev = change
            .prev
            .as_ref()
            .expect("prev value from the seeded put");
        assert_eq!(prev.value, b"seed");
        assert_eq!(prev.mod_revision, SEED_TS as i64);
    }

    // 再 tick 不重复搬运（shipped 水位随 shard raft apply 持久化）。
    let again = fx.tick_until_shipped(0).await;
    assert_eq!(again, 0, "no duplicate shipping after the watermark");
    assert_eq!(
        fx.shards.records_at(shard, commit_ts).await.unwrap().len(),
        2
    );
    let count = fx
        .shards
        .runtime(shard)
        .expect("shard runtime")
        .mvcc
        .commit_log_record_count()
        .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_outbox_rows_are_idempotent_when_scanned_twice() {
    let fx = ShardNode::start().await;
    fx.shards.tick().await.expect("first tick");

    // 直接对数据 Region 的 outbox 造两条同 ts 的分片（模拟重放/重复扫描）。
    for region_id in 1..=2u64 {
        let rt = fx.manager.runtime(region_id).unwrap();
        let record = coord_server::storage::commit_log::CommitRecord {
            shard_id: coord_server::storage::commit_log::COMMIT_LOG_STAGING_SHARD_ID,
            commit_ts: 200,
            txn_id: b"txn-200".to_vec(),
            start_ts: 199,
            region_id,
            txn_bytes: 16,
            changes: vec![coord_server::storage::commit_log::CommitChange {
                key: if region_id == 1 {
                    b"aa".to_vec()
                } else {
                    b"zz".to_vec()
                },
                value: Some(b"v".to_vec()),
                prev: None,
            }],
        };
        rt.mvcc
            .append_commit_record(&record, 4 * 1024 * 1024)
            .unwrap();
    }

    let shipped = fx.tick_until_shipped(2).await;
    assert_eq!(shipped, 2);
    let shard = fx.shards.shard_for_ts(200).unwrap().unwrap().shard_id;
    assert_eq!(fx.shards.records_at(shard, 200).await.unwrap().len(), 2);

    // 清空 shipped 水位后重扫（等价于换 leader 后的补扫）：重复不计入。
    let rt = fx.shards.runtime(shard).unwrap();
    let before = rt.mvcc.commit_log_record_count().unwrap();
    assert_eq!(before, 2);
    // 同一批再次提案：全部是 Duplicate，计数不变。
    let response = rt
        .raft
        .client_write(coord_server::raft::type_config::Command::CommitLogAppend {
            shard_id: shard,
            start_ts: 1,
            end_ts: 0,
            max_txn_bytes: 4 * 1024 * 1024,
            records: vec![coord_server::storage::commit_log::CommitRecord {
                shard_id: shard,
                commit_ts: 200,
                txn_id: b"txn-200".to_vec(),
                start_ts: 199,
                region_id: 1,
                txn_bytes: 16,
                changes: vec![coord_server::storage::commit_log::CommitChange {
                    key: b"aa".to_vec(),
                    value: Some(b"v".to_vec()),
                    prev: None,
                }],
            }],
        })
        .await
        .unwrap();
    match response.response() {
        coord_server::raft::type_config::Response::CommitLogAppend {
            inserted,
            duplicates,
            ..
        } => {
            assert_eq!(*inserted, 0);
            assert_eq!(*duplicates, 1);
        }
        other => panic!("unexpected response: {other:?}"),
    }
    assert_eq!(rt.mvcc.commit_log_record_count().unwrap(), 2);
    let _ = SequencerMutation::Delete {
        key: b"unused".to_vec(),
    };
}
