// Commit Log shard 协调器（ADR-0015 D5/D9，#154）。
//
// 职责边界：
// - **region 0 system raft** 是 shard 元数据（边界/状态/时间戳）的唯一权威；
//   只有 region 0 leader 提出元数据变更。本协调器不发明第二份元数据。
// - **每个 shard 一个独立 Raft 组**：复用 `spawn_region_runtime` 的目录隔离
//   （`regions/region-{id}`）、日志与网络装配；region id 走
//   `commit_log_shard::shard_region_id` 的保留命名空间，和数据 Region 共用
//   `RaftRpcService` 的解复用表但永不撞号。
// - **记录先写数据 Region 的 outbox（#153），再由 shard leader 搬进 shard 组**：
//   apply 必须保持确定性（不能在数据 apply 里直接写另一个 raft 组），而 outbox
//   与数据同事务落盘，保证「数据可见 ⇒ 记录已落盘」。搬运是幂等的（同键同内容
//   = Duplicate），中断/换 leader/重启后从 per-Region shipped 水位续传。
//
// 读取（#156）以 shard 组为准；outbox 的回收与保留窗口归 #157。

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use coord_core::error::{Error, Result};
use coord_core::types::{NodeID, RegionEpoch, RegionMeta};

use crate::kvt::KvTRuntimeConfig;
use crate::raft::network::RaftNetworkFactoryImpl;
use crate::raft::region::RegionHandle;
use crate::raft::region_runtime::{
    region_data_dir, spawn_region_runtime, RegionRuntime, RegionRuntimeSpec,
};
use crate::raft::type_config::CommitLogShardOp;
use crate::raft::{CoordRaft, RaftConfig};
use crate::storage::commit_log::CommitRecord;
use crate::storage::commit_log_shard::{shard_region_id, CommitLogShardMeta, CommitLogShardTable};
use crate::storage::mvcc::{MvccStorage, RevisionMode};
use crate::storage::redb_backend::RedbBackend;

/// 默认 wall clock（Unix 毫秒）；测试用 [`CommitLogShardManager::with_clock`] 注入。
fn wall_clock_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// 元数据提案的结果：region 0 apply 后的整表视图。
pub type ShardTable = Vec<CommitLogShardMeta>;

/// Commit Log shard 协调器。
///
/// 由 `main.rs` 在 `[kv_t].enabled=true` 且 Multi-Raft 装配完成后构造一次；
/// `tick()` 由后台任务周期调用（也供测试直接驱动）。
pub struct CommitLogShardManager {
    node_id: NodeID,
    /// region 0 system raft（元数据提案入口）
    raft: CoordRaft,
    /// region 0 MVCC（shard 表读取）
    mvcc: Arc<MvccStorage<RedbBackend>>,
    /// 数据 Region 管理器（peers 来源；shard 组不注册进路由表）
    manager: Arc<crate::raft::region::RegionManager>,
    factory: RaftNetworkFactoryImpl,
    rpc: crate::raft::network::RaftRpcService,
    base_dir: PathBuf,
    raft_config: Arc<RaftConfig>,
    config: KvTRuntimeConfig,
    runtimes: RwLock<BTreeMap<u64, Arc<RegionRuntime>>>,
    /// 串行化 shard 运行时装配（避免并发 tick/读取重复 spawn 同一组）
    spawn_lock: tokio::sync::Mutex<()>,
    clock: Arc<dyn Fn() -> u64 + Send + Sync>,
    /// 已 ship 记录数（进程内计数，指标用；不参与恢复）
    shipped_total: AtomicU64,
    rejected_total: AtomicU64,
}

impl CommitLogShardManager {
    /// 构造协调器（不触碰磁盘；runtime 在 `tick`/读取时按表懒装配）。
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        raft: CoordRaft,
        mvcc: Arc<MvccStorage<RedbBackend>>,
        manager: Arc<crate::raft::region::RegionManager>,
        factory: RaftNetworkFactoryImpl,
        rpc: crate::raft::network::RaftRpcService,
        base_dir: PathBuf,
        raft_config: Arc<RaftConfig>,
        config: KvTRuntimeConfig,
    ) -> Self {
        Self {
            node_id,
            raft,
            mvcc,
            manager,
            factory,
            rpc,
            base_dir,
            raft_config,
            config,
            runtimes: RwLock::new(BTreeMap::new()),
            spawn_lock: tokio::sync::Mutex::new(()),
            clock: Arc::new(wall_clock_ms),
            shipped_total: AtomicU64::new(0),
            rejected_total: AtomicU64::new(0),
        }
    }

    /// 替换 wall clock（测试注入；时间只用于 seal/预创建判定，不参与路由）。
    pub fn with_clock(mut self, clock: Arc<dyn Fn() -> u64 + Send + Sync>) -> Self {
        self.clock = clock;
        self
    }

    pub fn node_id(&self) -> NodeID {
        self.node_id
    }

    pub fn config(&self) -> &KvTRuntimeConfig {
        &self.config
    }

    pub(crate) fn now_ms(&self) -> u64 {
        (self.clock)()
    }

    pub(crate) fn shipped_total(&self) -> u64 {
        self.shipped_total.load(Ordering::Relaxed)
    }

    pub(crate) fn rejected_total(&self) -> u64 {
        self.rejected_total.load(Ordering::Relaxed)
    }

    /// 本地已装配的 shard runtime 快照（按 shard_id 升序）。
    pub fn runtimes(&self) -> Vec<(u64, Arc<RegionRuntime>)> {
        self.runtimes
            .read()
            .map(|map| map.iter().map(|(id, rt)| (*id, Arc::clone(rt))).collect())
            .unwrap_or_default()
    }

    /// region 0 的 shard 表快照（本节点 apply 视图）。
    pub fn table(&self) -> Result<CommitLogShardTable> {
        self.mvcc.commit_log_shard_table()
    }

    /// `commit_ts` 所属 shard 元数据（本地表视图；路由竞态由 shard 组拒收兜底）。
    pub fn shard_for_ts(&self, ts: u64) -> Result<Option<CommitLogShardMeta>> {
        Ok(self.table()?.covering(ts).cloned())
    }

    /// 是否是 region 0 leader（元数据提案/首建 shard 的唯一合法节点）。
    pub async fn is_region0_leader(&self) -> bool {
        self.raft.current_leader().await == Some(self.node_id)
    }

    /// 提案一条 shard 元数据操作（仅 region 0 leader；follower 返回 NotLeader）。
    pub async fn propose(&self, op: CommitLogShardOp) -> Result<ShardTable> {
        let response = self
            .raft
            .client_write(crate::raft::type_config::Command::CommitLogShard(op))
            .await
            .map_err(|err| {
                if matches!(
                    &err,
                    openraft::error::RaftError::APIError(
                        openraft::error::ClientWriteError::ForwardToLeader(_)
                    )
                ) {
                    Error::NotLeaderNoHint
                } else {
                    Error::Internal(format!("commit log shard propose failed: {err}"))
                }
            })?;
        match response.response() {
            crate::raft::type_config::Response::CommitLogShard { shards } => Ok(shards.clone()),
            other => Err(Error::Internal(format!(
                "unexpected commit log shard propose response: {other:?}"
            ))),
        }
    }

    /// 装配（或取回）某个 shard 的 Raft 组。
    ///
    /// 成员与数据 Region 相同（v1 静态复制）；只有 region 0 leader 用
    /// `initialize=true` 建成员，其余节点等 leader 复制。重复初始化是良性
    /// 竞争（另一节点已建组），降级为一次不带 initialize 的重试。
    pub async fn ensure_runtime(
        &self,
        shard_id: u64,
    ) -> std::result::Result<Arc<RegionRuntime>, String> {
        if let Some(rt) = self.runtime(shard_id) {
            return Ok(rt);
        }
        let _guard = self.spawn_lock.lock().await;
        if let Some(rt) = self.runtime(shard_id) {
            return Ok(rt);
        }
        let exists = self
            .table()
            .map_err(|err| format!("read commit log shard table: {err}"))?
            .shards
            .into_iter()
            .any(|meta| meta.shard_id == shard_id);
        if !exists {
            return Err(format!(
                "commit log shard {shard_id} is not in the region 0 table"
            ));
        }
        let peers = self
            .manager
            .list_regions()
            .first()
            .map(|handle| handle.meta.read().peers.clone())
            .filter(|peers| !peers.is_empty())
            .ok_or_else(|| {
                "commit log shard requires at least one assembled data region".to_string()
            })?;
        let region_id = shard_region_id(shard_id);
        let region_meta = RegionMeta {
            region_id,
            start_key: format!("/_sys/commit_log/{shard_id:016x}/").into_bytes(),
            end_key: format!("/_sys/commit_log/{:016x}/", shard_id + 1).into_bytes(),
            epoch: RegionEpoch::initial(),
            peers,
            approximate_size: 0,
            approximate_keys: 0,
        };
        let handle = Arc::new(RegionHandle::new(region_meta.clone()));
        let spec = RegionRuntimeSpec {
            meta: region_meta,
            data_dir: region_data_dir(&self.base_dir, region_id),
            raft_config: Arc::clone(&self.raft_config),
            object_store: None,
            revision_mode: RevisionMode::CommitTs,
        };
        let initialize = self.is_region0_leader().await;
        let spawned = match spawn_region_runtime(
            self.node_id,
            &self.factory,
            &self.rpc,
            spec.clone(),
            Arc::clone(&handle),
            initialize,
        )
        .await
        {
            Ok(rt) => rt,
            Err(err) if initialize => {
                tracing::info!(
                    shard_id,
                    error = %err,
                    "commit log shard initialize failed (likely a concurrent initializer); \
                     retrying as follower"
                );
                spawn_region_runtime(self.node_id, &self.factory, &self.rpc, spec, handle, false)
                    .await
                    .map_err(|err| format!("spawn commit log shard {shard_id}: {err}"))?
            }
            Err(err) => {
                return Err(format!("spawn commit log shard {shard_id}: {err}"));
            }
        };
        self.runtimes
            .write()
            .map_err(|_| "commit log shard runtime registry poisoned".to_string())?
            .insert(shard_id, Arc::clone(&spawned));
        Ok(spawned)
    }

    pub fn runtime(&self, shard_id: u64) -> Option<Arc<RegionRuntime>> {
        self.runtimes
            .read()
            .ok()
            .and_then(|map| map.get(&shard_id).cloned())
    }

    /// 同一 `commit_ts` 的全部分片（读取口径：shard 组，不是 outbox）。
    pub async fn records_at(&self, shard_id: u64, commit_ts: u64) -> Result<Vec<CommitRecord>> {
        let rt = self
            .ensure_runtime(shard_id)
            .await
            .map_err(Error::Internal)?;
        rt.mvcc.commit_records_at(shard_id, commit_ts)
    }

    /// `commit_ts > after_ts` 的分片（严格下界；`limit=0` 不限）。
    pub async fn records_after(
        &self,
        shard_id: u64,
        after_ts: u64,
        limit: usize,
    ) -> Result<Vec<CommitRecord>> {
        let rt = self
            .ensure_runtime(shard_id)
            .await
            .map_err(Error::Internal)?;
        rt.mvcc.commit_records_after(shard_id, after_ts, limit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::TcpListener;

    use coord_core::storage::StorageBackend;
    use coord_core::types::{Peer, PeerRole, StorageConfig};

    use crate::raft::log_store::LogStore;
    use crate::raft::network::RaftRpcService;
    use crate::raft::region::RegionManager;
    use crate::raft::state_machine::StateMachineStore;
    use crate::raft::{new_basic_node, new_raft};
    use crate::storage::commit_log_shard::shard_region_id;
    use crate::storage::snapshot::SnapshotTracker;
    use tempfile::TempDir;
    use tokio::time::{sleep, Instant};

    fn find_port() -> u16 {
        TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    fn raft_config() -> RaftConfig {
        RaftConfig {
            heartbeat_interval: 100,
            election_timeout_min: 300,
            election_timeout_max: 600,
            ..Default::default()
        }
    }

    struct ShardFixture {
        dir: TempDir,
        manager: Arc<CommitLogShardManager>,
        rpc: RaftRpcService,
    }

    impl ShardFixture {
        async fn start() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let base = dir.path().to_path_buf();
            let addr = format!("127.0.0.1:{}", find_port());

            // region 0 system raft
            let root_dir = base.join("region-0");
            std::fs::create_dir_all(&root_dir).unwrap();
            let backend = RedbBackend::open(&root_dir, &StorageConfig::default()).unwrap();
            let root_mvcc = Arc::new(
                MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs).unwrap(),
            );
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
                    .unwrap(),
            );
            root_raft
                .initialize(std::collections::BTreeMap::from([(
                    1u64,
                    new_basic_node(&addr),
                )]))
                .await
                .unwrap();

            // 一个数据 Region（只提供 peers / 目录布局，不写数据）
            let factory = RaftNetworkFactoryImpl::new(1);
            factory.register_node(1, addr.clone());
            let rpc = RaftRpcService::new();
            let manager = Arc::new(RegionManager::new(1));
            let data_meta = RegionMeta {
                region_id: 1,
                start_key: b"".to_vec(),
                end_key: b"".to_vec(),
                epoch: RegionEpoch::initial(),
                peers: vec![Peer {
                    node_id: 1,
                    raft_addr: addr,
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
                        meta: data_meta,
                        data_dir: region_data_dir(&base, 1),
                        raft_config: Arc::new(raft_config()),
                        object_store: None,
                        revision_mode: RevisionMode::CommitTs,
                    },
                    true,
                )
                .await
                .unwrap();

            let manager = Arc::new(CommitLogShardManager::new(
                1,
                root_raft.as_ref().clone(),
                Arc::clone(&root_mvcc),
                manager,
                factory,
                rpc.clone(),
                base,
                Arc::new(raft_config()),
                KvTRuntimeConfig::default(),
            ));
            wait_for_region0_leader(&manager).await;
            Self { dir, manager, rpc }
        }

        async fn open_shard(&self, start_ts: u64, opened_at_ms: u64) {
            self.manager
                .propose(CommitLogShardOp::Open {
                    start_ts,
                    opened_at_ms,
                })
                .await
                .unwrap();
        }
    }

    async fn wait_for_region0_leader(manager: &CommitLogShardManager) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if manager.is_region0_leader().await {
                return;
            }
            sleep(Duration::from_millis(20)).await;
        }
        panic!("region 0 did not elect a leader in time");
    }

    #[tokio::test]
    async fn shard_runtime_spawns_from_table_and_serves_reads() {
        let fx = ShardFixture::start().await;
        fx.open_shard(1, 1_000).await;
        assert!(fx.manager.shard_for_ts(1).unwrap().is_some());
        assert!(fx.manager.shard_for_ts(u64::MAX / 2).unwrap().is_some());

        let rt = fx.manager.ensure_runtime(1).await.unwrap();
        assert_eq!(rt.region_id(), shard_region_id(1));
        let again = fx.manager.ensure_runtime(1).await.unwrap();
        assert_eq!(again.region_id(), rt.region_id(), "idempotent spawn");
        assert_eq!(fx.manager.runtimes().len(), 1);

        // 未知 shard 显式失败，不静默建组。
        assert!(fx.manager.ensure_runtime(9).await.is_err());
        let _ = &fx.dir;
        let _ = &fx.rpc;
    }

    #[tokio::test]
    async fn shard_group_accepts_appends_and_serves_reads() {
        let fx = ShardFixture::start().await;
        fx.open_shard(1, 1_000).await;
        let rt = fx.manager.ensure_runtime(1).await.unwrap();
        let response = rt
            .raft
            .client_write(crate::raft::type_config::Command::CommitLogAppend {
                shard_id: 1,
                start_ts: 1,
                end_ts: 0,
                max_txn_bytes: 4 * 1024 * 1024,
                records: vec![CommitRecord {
                    shard_id: 1,
                    commit_ts: 10,
                    txn_id: b"txn-10".to_vec(),
                    start_ts: 9,
                    region_id: 1,
                    txn_bytes: 16,
                    changes: vec![crate::storage::commit_log::CommitChange {
                        key: b"k".to_vec(),
                        value: Some(b"v".to_vec()),
                        prev: None,
                    }],
                }],
            })
            .await
            .unwrap();
        match response.response() {
            crate::raft::type_config::Response::CommitLogAppend { inserted, .. } => {
                assert_eq!(*inserted, 1)
            }
            other => panic!("unexpected response: {other:?}"),
        }
        let rows = fx.manager.records_at(1, 10).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].changes[0].key, b"k");
        assert_eq!(rt.mvcc.commit_log_cursor().unwrap(), 10);
        assert_eq!(rt.mvcc.commit_log_shipped_through(1).unwrap(), 10);
        assert_eq!(rt.mvcc.commit_log_record_count().unwrap(), 1);
    }
}
