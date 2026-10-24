//! KV-T G4 e2e 共享夹具：两数据 Region + region 0 shard 元数据 + Commit Log shard 协调器。
//!
//! 由 `kv_t_g4_watch_coordinator_test`（直接调 coordinator）与
//! `kv_t_g4_watch_rpc_test`（真实 gRPC 流）共用。夹具不启动 gRPC 服务；
//! RPC 测试用 [`WatchNode::serve`] 按需挂载。
#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;
use tonic::transport::Server;

use coord_core::storage::StorageBackend;
use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionMeta, StorageConfig};
use coord_proto::maintenance::maintenance_server::MaintenanceServer;
use coord_proto::txn::v2::mutation::Op;
use coord_proto::txn::v2::txn_server::Txn as TxnV2;
use coord_proto::txn::v2::{BeginRequest, CommitRequest, Mutation};
use coord_proto::watch::v2::watch_server::WatchServer as WatchV2Server;
use coord_server::kvt::commit_log_shards::CommitLogShardManager;
use coord_server::kvt::decision_apply::apply_pending_decisions;
use coord_server::kvt::sequencer::CommitSequencer;
use coord_server::kvt::watch::{WatchCoordinator, WatchCursor, WatchEvent, WatchFilter};
use coord_server::kvt::{KvTCapability, KvTRuntimeConfig, KvTState};
use coord_server::metrics::Metrics;
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcService};
use coord_server::raft::region::RegionManager;
use coord_server::raft::region_runtime::{region_data_dir, RegionRuntimeSpec};
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::tso::{TsoAllocator, TsoClient};
use coord_server::raft::type_config::{SequencerMutation, SequencerValidationLimits};
use coord_server::raft::{new_basic_node, new_raft, RaftConfig, WatchReceiver};
use coord_server::server::CoordNode;
use coord_server::storage::mvcc::{AppliedLogId, MvccStorage, RevisionMode};
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::region_table::RegionTable;
use coord_server::storage::snapshot::SnapshotTracker;
use tempfile::TempDir;

/// 种子数据用的 commit_ts（低于后续事务的分配值）。
pub const SEED_TS: u64 = 100;

/// 直写用例在 `SEED_TS` 之上手工取 `commit_ts`（最大 offset 22）；TSO 高水位
/// 必须覆盖这些值，否则直写次序守卫会把它们当作「未由 TSO 发放」拒绝。
const SEED_HEADROOM: u64 = 64;

pub fn find_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

pub fn raft_config() -> RaftConfig {
    RaftConfig {
        heartbeat_interval: 200,
        election_timeout_min: 800,
        election_timeout_max: 1_500,
        ..Default::default()
    }
}

/// 一条可重启的 gRPC 服务句柄（Maintenance 供客户端 Leader 发现，Watch v2 是被测面）。
pub struct WatchServerHandle {
    pub addr: String,
    shutdown: Option<oneshot::Sender<()>>,
    handle: tokio::task::JoinHandle<()>,
}

impl WatchServerHandle {
    /// 优雅停机：先停 accept，再等 in-flight 请求排空。
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        let _ = self.handle.await;
    }

    /// 立即断开：模拟网络/进程故障（in-flight 流全部收到传输错误）。
    pub fn abort(self) {
        self.handle.abort();
    }
}

pub struct WatchNode {
    pub node: Arc<CoordNode>,
    pub manager: Arc<RegionManager>,
    pub shards: Arc<CommitLogShardManager>,
    pub now: Arc<AtomicU64>,
    pub metrics: Arc<Metrics>,
    _dir: TempDir,
    _factory: RaftNetworkFactoryImpl,
    _rpc: RaftRpcService,
}

impl WatchNode {
    pub async fn start() -> Self {
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
                KvTCapability::Watch,
            ]),
        );
        node.set_metrics(Arc::clone(&metrics));
        let tso = TsoClient::new(
            Arc::new(TsoAllocator::new(root_raft.as_ref().clone(), 1)),
            1024,
            None,
        );
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
        // 种子数据占 `SEED_TS`，TSO 必须从它之上起发：把高水位推到 `SEED_TS+10`
        // （经 region 0 raft apply，与生产 allocator 一致；直写次序守卫用高水位
        // 给 safe-ts fence 封顶）。
        let allocator = TsoAllocator::new(root_raft.as_ref().clone(), 1);
        let primed = allocator
            .allocate((SEED_TS + SEED_HEADROOM) as u32)
            .await
            .expect("prime TSO high-water");
        assert!(
            primed.start_ts <= SEED_TS && primed.end_ts > SEED_TS,
            "primed batch must cover the seed revision: {primed:?}"
        );
        // 种子数据：两个 Region 各有已提交值，prev 值可用于校验完整记录。
        let fixture = Self {
            node,
            manager,
            shards,
            now,
            metrics,
            _dir: dir,
            _factory: factory,
            _rpc: rpc,
        };
        fixture.put_v2(b"aa", b"seed", SEED_TS).await;
        fixture.put_v2(b"zz", b"seed", SEED_TS).await;
        fixture
    }

    /// 在随机端口挂载 Maintenance（Leader 发现）+ `coord.watch.v2`。
    ///
    /// Maintenance 是 `coord_client::Client` 找 leader 的入口；v2 watch 本身
    /// 任何持有 Commit Log shard 的节点都能服务，仍复用同一连接。
    pub async fn serve(&self) -> WatchServerHandle {
        self.serve_at(0).await
    }

    /// 在指定端口挂载（`0` = 随机端口）。重启重连测试需要在同一端口重新监听。
    pub async fn serve_at(&self, port: u16) -> WatchServerHandle {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let (tx, rx) = oneshot::channel();
        let node = Arc::clone(&self.node);
        let handle = tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(MaintenanceServer::from_arc(Arc::clone(&node)))
                .add_service(WatchV2Server::from_arc(node))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = rx.await;
                })
                .await;
        });
        WatchServerHandle {
            addr,
            shutdown: Some(tx),
            handle,
        }
    }

    pub async fn put_v2(&self, key: &[u8], value: &[u8], commit_ts: u64) {
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

    /// 应用权威 Region 表，并为表中尚未装配的 Region 建运行时（单节点 initialize）。
    ///
    /// 生产由 `RegionLifecycle::reconcile` 做这件事；测试要在 split/merge 拓扑
    /// 变化后写数据，因此在这里同步等 leader。
    pub async fn apply_region_table_and_spawn(&self, table: RegionTable) {
        self.manager
            .apply_region_table(&table)
            .expect("apply region table");
        let base = self._dir.path().to_path_buf();
        for meta in &table.regions {
            if self.manager.runtime(meta.region_id).is_some() {
                continue;
            }
            self.manager
                .spawn_registered_region(
                    &self._factory,
                    &self._rpc,
                    RegionRuntimeSpec {
                        meta: meta.clone(),
                        data_dir: region_data_dir(&base, meta.region_id),
                        raft_config: Arc::new(raft_config()),
                        object_store: None,
                        revision_mode: RevisionMode::CommitTs,
                    },
                    true,
                )
                .await
                .expect("spawn table region");
        }
        wait_for_leaders(&self.node, &self.manager).await;
    }

    /// 表已移除的 Region：停 raft 组并摘除运行时（模拟 RegionLifecycle retire）。
    pub async fn retire_region(&self, region_id: u64) {
        let Some(rt) = self.manager.runtime(region_id) else {
            return;
        };
        rt.raft.shutdown().await.expect("shutdown region raft");
        self.manager.detach_runtime(region_id);
    }

    /// 在指定 Region 上直接 apply 一条带 Commit Log 的决策，模拟 cutover 窗口
    /// 内父 Region 先 apply、子 Region 稍后从决策日志重放同一 `commit_ts`。
    pub async fn replay_decision_on_region(
        &self,
        region_id: u64,
        key: &[u8],
        value: &[u8],
        commit_ts: u64,
    ) {
        let rt = self.manager.runtime(region_id).expect("region runtime");
        rt.mvcc
            .apply_commit_decisions_with_log(
                &[coord_server::raft::type_config::CommitDecisionApplyLog {
                    shard_id: coord_server::storage::commit_log::COMMIT_LOG_STAGING_SHARD_ID,
                    region_id,
                    commit_ts,
                    txn_id: format!("replay-{commit_ts}").into_bytes(),
                    start_ts: commit_ts.saturating_sub(1),
                    mutations: vec![SequencerMutation::Put {
                        key: key.to_vec(),
                        value: value.to_vec(),
                        lease_id: None,
                    }],
                }],
                1,
                AppliedLogId {
                    term: 1,
                    node_id: 1,
                    index: 1,
                },
                4 * 1024 * 1024,
            )
            .expect("replay decision");
    }

    pub async fn begin(&self, start_ts: i64) -> coord_proto::txn::v2::BeginResponse {
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

    pub async fn commit_puts(
        &self,
        txn_id: &[u8],
        pairs: &[(&[u8], &[u8])],
        request_id: &[u8],
    ) -> u64 {
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
    pub async fn commit_pairs(&self, pairs: &[(&[u8], &[u8])]) -> u64 {
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
    pub async fn settle(&self, commit_ts: u64) {
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
    pub async fn open_first_shard(&self) {
        self.shards.tick().await.expect("first shard tick");
    }

    pub fn advance_clock_ms(&self, delta: u64) {
        self.now.fetch_add(delta, Ordering::SeqCst);
    }

    /// 反复 tick 直到 region 0 表推进到期望的回收水位。
    pub async fn wait_until_gc_before(&self, expected: u64) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            self.shards.tick().await.expect("shard tick");
            if self.shards.table().expect("shard table").gc_before_ts == expected {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "commit log gc did not reach {expected}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// 反复 tick 直到指定 shard 已 seal（seal 依赖 watermark 越过 end_ts）。
    pub async fn wait_until_sealed(&self, shard_id: u64) {
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

    pub async fn poll_until(
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

pub async fn wait_for_leaders(node: &CoordNode, manager: &RegionManager) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let root_leader = node
            .raft
            .as_ref()
            .expect("region 0 raft")
            .current_leader()
            .await
            == Some(1);
        let runtimes = manager.list_runtimes();
        let data_ready = !runtimes.is_empty()
            && runtimes.iter().all(|rt| {
                let m = rt.raft.metrics();
                let m = m.borrow_watched();
                m.current_leader == Some(1) && m.last_quorum_acked.is_some()
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
