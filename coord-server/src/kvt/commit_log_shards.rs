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

use std::sync::{Arc, RwLock};
use std::time::Duration;

use coord_core::error::{Error, Result};
use coord_core::types::{NodeID, RegionEpoch, RegionMeta};

use crate::kvt::commit_watermark::{
    commit_watermark, pending_decision, unshipped_record, CommitWatermarkState,
};
use crate::kvt::KvTRuntimeConfig;
use crate::raft::network::RaftNetworkFactoryImpl;
use crate::raft::region::RegionHandle;
use crate::raft::region_runtime::{
    region_data_dir, spawn_region_runtime, RegionRuntime, RegionRuntimeSpec,
};
use crate::raft::type_config::{Command, CommitLogShardOp, Response};
use crate::raft::{CoordRaft, RaftConfig};
use crate::storage::commit_log::CommitRecord;
use crate::storage::commit_log_shard::{
    precreate_due, seal_due, seal_timeout_due, shard_region_id, split_end_ts, CommitLogShardMeta,
    CommitLogShardTable,
};
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

    /// 本节点视图的 watermark（不发布）：决策日志最后 ts + 两类 in-flight 候选。
    ///
    /// 只读本地副本：v1 静态复制下 region 0 leader 持有每个数据 Region 与每个
    /// shard 的副本；本地 apply 落后只会让 watermark 更保守，不会越过未完成事务。
    pub async fn local_watermark(&self) -> std::result::Result<CommitWatermarkState, String> {
        let decision_last = self
            .mvcc
            .sequencer_last_commit_ts()
            .map_err(|err| format!("read sequencer last commit_ts: {err}"))?;
        let regions = self.manager.list_runtimes();
        let table = self
            .table()
            .map_err(|err| format!("read commit log shard table: {err}"))?;
        let mut in_flight = Vec::new();

        for rt in &regions {
            let applied = rt
                .mvcc
                .last_applied_decision_ts()
                .map_err(|err| format!("region {} read decision cursor: {err}", rt.region_id()))?;
            let next = self
                .mvcc
                .next_sequencer_decision_ts(applied)
                .map_err(|err| format!("region 0 scan pending decisions: {err}"))?;
            in_flight.extend(pending_decision(applied, next));
        }

        for meta in &table.shards {
            let shard_rt = self.ensure_runtime(meta.shard_id).await?;
            for rt in &regions {
                let region_id = rt.region_id();
                let shipped = shard_rt
                    .mvcc
                    .commit_log_shipped_through(region_id)
                    .map_err(|err| {
                        format!("read shipped watermark for region {region_id}: {err}")
                    })?;
                let after = shipped.max(meta.start_ts.saturating_sub(1));
                let next = rt
                    .mvcc
                    .commit_records_after(
                        crate::storage::commit_log::COMMIT_LOG_STAGING_SHARD_ID,
                        after,
                        1,
                    )
                    .map_err(|err| format!("read outbox of region {region_id}: {err}"))?
                    .first()
                    .map(|record| record.commit_ts);
                in_flight.extend(unshipped_record(meta.start_ts, meta.end_ts, next));
            }
        }

        Ok(commit_watermark(decision_last, &in_flight))
    }

    /// 已发布的 watermark（region 0 视图；Watch 读取口径，#156）。
    pub fn published_watermark(&self) -> std::result::Result<u64, String> {
        self.mvcc
            .commit_watermark()
            .map_err(|err| format!("read published commit watermark: {err}"))
    }

    /// 经 region 0 raft 发布 watermark（实际生效值；仅 region 0 leader 调用）。
    pub async fn publish_watermark(&self, watermark: u64) -> std::result::Result<u64, String> {
        let response = self
            .raft
            .client_write(Command::PublishCommitWatermark { watermark })
            .await
            .map_err(|err| format!("publish commit watermark {watermark}: {err}"))?;
        match response.response() {
            Response::CommitWatermarkPublished { watermark } => Ok(*watermark),
            other => Err(format!(
                "unexpected PublishCommitWatermark response: {other:?}"
            )),
        }
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

    /// 一次协调周期：装配缺失的 shard 组 → （region 0 leader）推进元数据 →
    /// （本地 shard leader）搬运 outbox 记录。
    ///
    /// 单 shard/单数据 Region 的失败只告警并跳过，不阻塞其它 shard；返回值只用于
    /// 指标与测试。
    pub async fn tick(&self) -> std::result::Result<CommitLogShardTick, String> {
        let mut tick = CommitLogShardTick::default();
        let table = self
            .table()
            .map_err(|err| format!("read commit log shard table: {err}"))?;
        for meta in &table.shards {
            self.ensure_runtime(meta.shard_id).await?;
        }

        // 先搬运再计算：本轮 watermark 应包含刚刚追加的记录。
        let table = self
            .table()
            .map_err(|err| format!("read commit log shard table: {err}"))?;
        for meta in &table.shards {
            let Some(rt) = self.runtime(meta.shard_id) else {
                continue;
            };
            if rt.raft.current_leader().await != Some(self.node_id) {
                continue;
            }
            let (shipped, rejected) = self.ship_shard(&rt, meta).await?;
            tick.shipped += shipped;
            tick.rejected += rejected;
        }

        let state = self.local_watermark().await?;
        tick.watermark = state.watermark;
        tick.in_flight = state.in_flight;

        if self.is_region0_leader().await {
            if state.watermark > self.published_watermark()? {
                self.publish_watermark(state.watermark).await?;
            }
            let (opened, sealed, stalls) = self.advance_metadata(state.watermark).await?;
            tick.opened = opened;
            tick.sealed = sealed;
            tick.stalls = stalls;
        }

        let now = self.now_ms();
        tick.seal_lag_secs = self
            .table()
            .map_err(|err| format!("read commit log shard table: {err}"))?
            .shards
            .iter()
            .filter(|meta| meta.end_ts != 0 && meta.sealed_at_ms == 0)
            .map(|meta| now.saturating_sub(meta.ended_at_ms) / 1_000)
            .max()
            .unwrap_or(0);

        Ok(tick)
    }

    /// 元数据推进（只有 region 0 leader 调用）。
    ///
    /// `end_ts` 取 `max(本 shard 已 ship 的 ts, sequencer 最后决策 ts) + 1`：
    /// 之后的新决策一定落进新 shard，之前的落进旧 shard。sealed shard 仍接受
    /// 区间内的迟到记录（outbox 晚到），因此不会丢事件；`watermark >= end_ts`
    /// 才说明「不再有迟到记录」。
    async fn advance_metadata(
        &self,
        watermark: u64,
    ) -> std::result::Result<(u64, u64, u64), String> {
        let now = self.now_ms();
        let cfg = &self.config;
        let table = self
            .table()
            .map_err(|err| format!("read commit log shard table: {err}"))?;
        let mut opened = 0;
        let mut sealed = 0;
        let mut stalls = 0;

        let Some(latest) = table.shards.last().cloned() else {
            self.propose(CommitLogShardOp::Open {
                start_ts: 1,
                opened_at_ms: now,
            })
            .await
            .map_err(|err| format!("open the first commit log shard: {err}"))?;
            return Ok((1, 0, 0));
        };

        let rt = self.ensure_runtime(latest.shard_id).await?;
        let records = rt
            .mvcc
            .commit_log_record_count()
            .map_err(|err| format!("count shard {} records: {err}", latest.shard_id))?;
        let age = now.saturating_sub(latest.opened_at_ms);

        if latest.end_ts == 0 {
            let full = seal_due(
                records,
                age,
                cfg.commit_log_shard_records,
                cfg.commit_log_shard_secs,
            );
            if full
                || precreate_due(
                    records,
                    age,
                    cfg.commit_log_shard_records,
                    cfg.commit_log_shard_secs,
                )
            {
                let cursor = rt
                    .mvcc
                    .commit_log_cursor()
                    .map_err(|err| format!("read shard {} cursor: {err}", latest.shard_id))?;
                let last_decision = self
                    .mvcc
                    .sequencer_last_commit_ts()
                    .map_err(|err| format!("read sequencer last commit_ts: {err}"))?;
                let end = split_end_ts(cursor, last_decision, latest.start_ts);
                self.propose(CommitLogShardOp::SetEnd {
                    shard_id: latest.shard_id,
                    end_ts: end,
                    now_ms: now,
                })
                .await
                .map_err(|err| format!("set shard {} end: {err}", latest.shard_id))?;
                self.propose(CommitLogShardOp::Open {
                    start_ts: end,
                    opened_at_ms: now,
                })
                .await
                .map_err(|err| format!("pre-create the next commit log shard: {err}"))?;
                opened += 1;
            }
        }

        let table = self
            .table()
            .map_err(|err| format!("read commit log shard table: {err}"))?;
        // seal 主条件是 D9 的 `watermark >= end_ts`：它证明区间内的记录已全部
        // 落进 shard，且此后不会再有更小的 commit_ts。超时只是停滞兜底——钉住
        // watermark 的 Region/shipper 可能长时间不可用，不能让 shard 永久 open。
        for meta in table
            .shards
            .iter()
            .filter(|meta| meta.end_ts != 0 && meta.sealed_at_ms == 0)
        {
            if watermark >= meta.end_ts {
                self.propose(CommitLogShardOp::Seal {
                    shard_id: meta.shard_id,
                    now_ms: now,
                })
                .await
                .map_err(|err| format!("seal shard {}: {err}", meta.shard_id))?;
                sealed += 1;
            } else if seal_timeout_due(meta.ended_at_ms, now, cfg.shard_seal_timeout_secs) {
                tracing::warn!(
                    shard_id = meta.shard_id,
                    end_ts = meta.end_ts,
                    watermark,
                    "commit log shard seal stalled; sealing on timeout"
                );
                self.propose(CommitLogShardOp::Seal {
                    shard_id: meta.shard_id,
                    now_ms: now,
                })
                .await
                .map_err(|err| format!("seal shard {}: {err}", meta.shard_id))?;
                sealed += 1;
                stalls += 1;
            }
        }
        Ok((opened, sealed, stalls))
    }

    /// 把一个 shard 的 outbox 记录搬进 shard raft 组（仅本地 shard leader）。
    ///
    /// 每个数据 Region 的扫描下界取本 shard 对该 Region 的 shipped 水位
    /// （`max(shipped, start_ts - 1)`）：数据 Region 按 `commit_ts` 递增 apply，
    /// 水位只会越过「整批已追加」的记录，因此不会跳过区间内的记录；中断后
    /// 重扫的重复由 shard apply 的幂等追加吸收。
    async fn ship_shard(
        &self,
        rt: &Arc<RegionRuntime>,
        meta: &CommitLogShardMeta,
    ) -> std::result::Result<(u64, u64), String> {
        let mut shipped = 0u64;
        let mut rejected = 0u64;
        for data_rt in self.manager.list_runtimes() {
            let region_id = data_rt.region_id();
            let floor = rt
                .mvcc
                .commit_log_shipped_through(region_id)
                .map_err(|err| format!("read shipped watermark for region {region_id}: {err}"))?;
            let after = floor.max(meta.start_ts.saturating_sub(1));
            let rows = data_rt
                .mvcc
                .commit_records_after(
                    crate::storage::commit_log::COMMIT_LOG_STAGING_SHARD_ID,
                    after,
                    SHIP_BATCH_RECORDS,
                )
                .map_err(|err| format!("read outbox of region {region_id}: {err}"))?;
            let batch: Vec<CommitRecord> = rows
                .into_iter()
                .filter(|record| meta.covers(record.commit_ts))
                .map(|mut record| {
                    record.shard_id = meta.shard_id;
                    record
                })
                .collect();
            for chunk in split_ship_batches(batch) {
                let command = crate::raft::type_config::Command::CommitLogAppend {
                    shard_id: meta.shard_id,
                    start_ts: meta.start_ts,
                    end_ts: meta.end_ts,
                    max_txn_bytes: self.config.max_txn_bytes,
                    records: chunk,
                };
                let response = rt.raft.client_write(command).await.map_err(|err| {
                    format!(
                        "append records to commit log shard {} region {region_id}: {err}",
                        meta.shard_id
                    )
                })?;
                match response.response() {
                    crate::raft::type_config::Response::CommitLogAppend {
                        inserted,
                        duplicates,
                        rejected: refused,
                        ..
                    } => {
                        shipped += inserted + duplicates;
                        rejected += refused;
                    }
                    other => {
                        return Err(format!(
                            "unexpected commit log shard append response: {other:?}"
                        ))
                    }
                }
            }
        }
        Ok((shipped, rejected))
    }
}

/// 一次协调周期的可观测结果。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CommitLogShardTick {
    pub opened: u64,
    pub sealed: u64,
    pub shipped: u64,
    pub rejected: u64,
    /// 本轮计算的 watermark（本节点视图；leader 已尝试发布）。
    pub watermark: u64,
    /// 钉住 watermark 的未完成项数量（未 apply 决策 + 未搬运记录）。
    pub in_flight: u64,
    /// 因 watermark 停滞超时而 seal 的 shard 数。
    pub stalls: u64,
    /// 有界未 seal shard 的最大等待秒数（无 = 0）。
    pub seal_lag_secs: u64,
}

/// 单批记录条数上限（与 sequencer 批同量级；raft 负载由字节预算再切）。
const SHIP_BATCH_RECORDS: usize = 256;

/// 协调周期（seal/预创建的判定粒度；搬运也按此节奏，窗口足够小）。
pub const SHARD_TICK_INTERVAL: Duration = Duration::from_millis(500);

/// 单批写集字节预算（记录已压缩，实际帧更小；上限防止 256×4MiB 的 raft 负载）。
const SHIP_BATCH_TXN_BYTES: u64 = 8 * 1024 * 1024;

/// 按写集字节预算切片；首条无论多大都保留（绝不丢记录）。
fn split_ship_batches(records: Vec<CommitRecord>) -> Vec<Vec<CommitRecord>> {
    let mut batches: Vec<Vec<CommitRecord>> = Vec::new();
    let mut current: Vec<CommitRecord> = Vec::new();
    let mut bytes = 0u64;
    for record in records {
        let size = u64::from(record.txn_bytes);
        if !current.is_empty() && bytes.saturating_add(size) > SHIP_BATCH_TXN_BYTES {
            batches.push(std::mem::take(&mut current));
            bytes = 0;
        }
        bytes = bytes.saturating_add(size);
        current.push(record);
    }
    if !current.is_empty() {
        batches.push(current);
    }
    batches
}

/// 启动 shard 协调后台任务（进程生命周期；关闭 kv_t 时不构造）。
pub fn start_shard_manager(
    node: Arc<crate::server::CoordNode>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let Some(manager) = node.commit_log_shards() else {
                continue;
            };
            match manager.tick().await {
                Ok(tick) => {
                    if let Some(metrics) = node.metrics() {
                        let (total, sealed) = manager
                            .table()
                            .map(|table| {
                                (
                                    table.shards.len() as u64,
                                    table
                                        .shards
                                        .iter()
                                        .filter(|meta| meta.sealed_at_ms != 0)
                                        .count() as u64,
                                )
                            })
                            .unwrap_or((0, 0));
                        metrics.set_kv_t_commit_log_shards(total, sealed);
                        metrics.add_kv_t_commit_log_records(tick.shipped, tick.rejected);
                        metrics.set_kv_t_commit_watermark(tick.watermark, tick.in_flight);
                        metrics.set_kv_t_commit_log_seal_lag(tick.seal_lag_secs);
                        for _ in 0..tick.stalls {
                            metrics.inc_kv_t_commit_watermark_stall();
                        }
                    }
                    if tick.opened > 0 || tick.sealed > 0 || tick.shipped > 0 || tick.rejected > 0 {
                        tracing::debug!(
                            opened = tick.opened,
                            sealed = tick.sealed,
                            shipped = tick.shipped,
                            rejected = tick.rejected,
                            "commit log shard tick"
                        );
                    }
                }
                Err(err) => {
                    tracing::warn!(error = %err, "commit log shard tick failed; retrying next tick");
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::TcpListener;
    use std::sync::atomic::AtomicU64;

    use coord_core::storage::StorageBackend;
    use coord_core::types::{Peer, PeerRole, StorageConfig};

    use crate::raft::log_store::LogStore;
    use crate::raft::network::RaftRpcService;
    use crate::raft::region::RegionManager;
    use crate::raft::state_machine::StateMachineStore;
    use crate::raft::{new_basic_node, new_raft};
    use crate::storage::commit_log::{CommitChange, CommitRecord};
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
        root_mvcc: Arc<MvccStorage<RedbBackend>>,
        data: Arc<RegionRuntime>,
        rpc: RaftRpcService,
        now: Arc<AtomicU64>,
    }

    impl ShardFixture {
        async fn start() -> Self {
            Self::start_with(KvTRuntimeConfig::default(), 1_000).await
        }

        async fn start_with(config: KvTRuntimeConfig, now_ms: u64) -> Self {
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

            // 一个数据 Region（提供 peers / 目录布局 / outbox）
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
            let data = manager
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

            let now = Arc::new(AtomicU64::new(now_ms));
            let clock_now = Arc::clone(&now);
            let clock: Arc<dyn Fn() -> u64 + Send + Sync> =
                Arc::new(move || clock_now.load(std::sync::atomic::Ordering::SeqCst));
            let manager = Arc::new(
                CommitLogShardManager::new(
                    1,
                    root_raft.as_ref().clone(),
                    Arc::clone(&root_mvcc),
                    manager,
                    factory,
                    rpc.clone(),
                    base,
                    Arc::new(raft_config()),
                    config,
                )
                .with_clock(clock),
            );
            wait_for_region0_leader(&manager).await;
            Self {
                dir,
                manager,
                root_mvcc,
                data,
                rpc,
                now,
            }
        }

        fn set_now(&self, ms: u64) {
            self.now.store(ms, std::sync::atomic::Ordering::SeqCst);
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

        /// 写一条 outbox 记录（数据 Region apply 的正常路径产物，shard 仍是 staging）。
        fn append_outbox(&self, commit_ts: u64, key: &[u8], value: &[u8]) {
            let record = CommitRecord {
                shard_id: crate::storage::commit_log::COMMIT_LOG_STAGING_SHARD_ID,
                commit_ts,
                txn_id: format!("txn-{commit_ts}").into_bytes(),
                start_ts: commit_ts.saturating_sub(1),
                region_id: self.data.region_id(),
                txn_bytes: (key.len() + value.len() + 16) as u32,
                changes: vec![CommitChange {
                    key: key.to_vec(),
                    value: Some(value.to_vec()),
                    prev: None,
                }],
            };
            self.data
                .mvcc
                .append_commit_record(&record, self.manager.config().max_txn_bytes)
                .unwrap();
        }

        async fn ship_one(&self, commit_ts: u64, key: &[u8], value: &[u8]) {
            self.append_outbox(commit_ts, key, value);
            self.manager.tick().await.unwrap();
        }

        /// 写 region 0 决策日志（真实 sequencer apply 路径，不做 outbox 伪造）。
        fn write_decisions(&self, timestamps: &[u64]) {
            use crate::raft::type_config::{
                SequencerMutation, SequencerOp, SequencerTxn, SequencerTxnWithRanges,
                SequencerValidationLimits,
            };
            let txns = timestamps
                .iter()
                .map(|ts| SequencerTxnWithRanges {
                    txn: SequencerTxn {
                        txn_id: format!("txn-{ts}").into_bytes(),
                        start_ts: ts.saturating_sub(1),
                        commit_ts: *ts,
                        request_id: format!("req-{ts}").into_bytes(),
                        read_set: Vec::new(),
                        mutations: vec![SequencerMutation::Put {
                            key: format!("k{ts}").into_bytes(),
                            value: b"v".to_vec(),
                            lease_id: None,
                        }],
                    },
                    read_ranges: Vec::new(),
                })
                .collect();
            self.root_mvcc
                .apply_sequencer_op(
                    &SequencerOp::ValidateAndCommitWithRanges {
                        txns,
                        limits: SequencerValidationLimits {
                            max_read_set: 4_096,
                            max_range: 10_000,
                        },
                    },
                    1,
                    crate::storage::mvcc::AppliedLogId::standalone(1),
                )
                .unwrap();
        }

        /// 把决策 apply 进数据 Region：outbox 记录与 apply 游标在同一事务里前进。
        fn apply_decisions(&self, timestamps: &[u64]) {
            use crate::raft::type_config::{CommitDecisionApplyLog, SequencerMutation};
            let decisions: Vec<CommitDecisionApplyLog> = timestamps
                .iter()
                .map(|ts| CommitDecisionApplyLog {
                    shard_id: crate::storage::commit_log::COMMIT_LOG_STAGING_SHARD_ID,
                    region_id: self.data.region_id(),
                    commit_ts: *ts,
                    txn_id: format!("txn-{ts}").into_bytes(),
                    start_ts: ts.saturating_sub(1),
                    mutations: vec![SequencerMutation::Put {
                        key: format!("k{ts}").into_bytes(),
                        value: b"v".to_vec(),
                        lease_id: None,
                    }],
                })
                .collect();
            self.data
                .mvcc
                .apply_commit_decisions_with_log(
                    &decisions,
                    1,
                    crate::storage::mvcc::AppliedLogId::standalone(1),
                    self.manager.config().max_txn_bytes,
                )
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
                    changes: vec![CommitChange {
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

    #[tokio::test]
    async fn tick_opens_shards_precreates_at_80_percent_and_seals_by_timeout() {
        let fx = ShardFixture::start_with(
            KvTRuntimeConfig {
                commit_log_shard_records: 10,
                commit_log_shard_secs: 3_600,
                shard_seal_timeout_secs: 300,
                ..KvTRuntimeConfig::default()
            },
            1_000,
        )
        .await;

        let first = fx.manager.tick().await.unwrap();
        assert_eq!(first.opened, 1, "region 0 leader opens the first shard");
        assert_eq!(fx.manager.table().unwrap().shards.len(), 1);

        for ts in 1..=8u64 {
            fx.ship_one(ts, b"k", b"v").await;
        }
        let count = fx
            .manager
            .runtime(1)
            .unwrap()
            .mvcc
            .commit_log_record_count()
            .unwrap();
        assert_eq!(count, 8);

        // ship 先于元数据：第 8 条（80%）的 tick 已经预创建 shard 2。
        let table = fx.manager.table().unwrap();
        assert_eq!(table.shards.len(), 2, "80% pre-creates the next shard");
        assert!(table.shards[0].end_ts > 0);
        assert_eq!(table.shards[1].start_ts, table.shards[0].end_ts);
        assert_eq!(table.shards[1].end_ts, 0);

        fx.set_now(2_000 + 300_000);
        let tick = fx.manager.tick().await.unwrap();
        assert_eq!(tick.sealed, 1, "seal timeout closes the bounded shard");
        assert_eq!(tick.stalls, 1, "stalled watermark is observable");
        let table = fx.manager.table().unwrap();
        assert_eq!(table.shards[0].sealed_at_ms, 2_000 + 300_000);
    }

    #[tokio::test]
    async fn tick_bounds_a_full_shard_and_the_timeout_seals_it() {
        let fx = ShardFixture::start_with(
            KvTRuntimeConfig {
                commit_log_shard_records: 2,
                ..KvTRuntimeConfig::default()
            },
            1_000,
        )
        .await;
        fx.manager.tick().await.unwrap();
        fx.ship_one(1, b"a", b"1").await;
        fx.ship_one(2, b"b", b"2").await;
        // 达阈值只定界 + 预创建；没有新决策时 watermark 不大于 end_ts。
        let table = fx.manager.table().unwrap();
        assert_eq!(table.shards.len(), 2);
        assert_eq!(table.shards[0].sealed_at_ms, 0);
        assert!(table.shards[0].end_ts > table.shards[0].start_ts);

        fx.set_now(1_000 + 300_000);
        let tick = fx.manager.tick().await.unwrap();
        assert_eq!(tick.stalls, 1);
        assert_eq!(tick.sealed, 1);
        assert!(fx.manager.table().unwrap().shards[0].sealed_at_ms > 0);
    }

    #[tokio::test]
    async fn ship_moves_outbox_rows_once_and_keeps_full_values() {
        let fx = ShardFixture::start().await;
        fx.manager.tick().await.unwrap();
        fx.append_outbox(1, b"a", b"1");
        fx.append_outbox(2, b"b", b"2");
        let tick = fx.manager.tick().await.unwrap();
        assert_eq!(tick.shipped, 2);
        assert_eq!(tick.rejected, 0);

        let shard = fx.manager.shard_for_ts(2).unwrap().unwrap().shard_id;
        let rows = fx.manager.records_after(shard, 0, 0).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].changes[0].key, b"a");
        assert_eq!(rows[0].changes[0].value.as_deref(), Some(b"1".as_slice()));
        assert_eq!(rows[0].shard_id, shard);

        let again = fx.manager.tick().await.unwrap();
        assert_eq!(again.shipped, 0, "shipped watermark prevents re-shipping");
        assert_eq!(again.rejected, 0);
        assert_eq!(
            fx.manager.records_after(shard, 0, 0).await.unwrap().len(),
            2
        );
    }

    #[tokio::test]
    async fn watermark_never_passes_a_decision_the_data_region_has_not_applied() {
        let fx = ShardFixture::start().await;
        fx.manager.tick().await.unwrap(); // open shard 1

        fx.write_decisions(&[2, 3, 4]);
        let tick = fx.manager.tick().await.unwrap();
        assert_eq!(tick.watermark, 1, "决定 2 未 apply，watermark 停在 1");
        assert_eq!(tick.in_flight, 1);
        assert_eq!(fx.manager.published_watermark().unwrap(), 1);

        fx.apply_decisions(&[2]);
        let tick = fx.manager.tick().await.unwrap();
        assert_eq!(tick.watermark, 2, "apply 2 之后仍等 3");

        fx.apply_decisions(&[3, 4]);
        let tick = fx.manager.tick().await.unwrap();
        assert_eq!(tick.in_flight, 0);
        assert_eq!(tick.watermark, 4);
        assert_eq!(fx.manager.published_watermark().unwrap(), 4);
    }

    #[tokio::test]
    async fn watermark_never_passes_an_applied_record_that_is_not_in_the_shard() {
        let fx = ShardFixture::start().await;
        fx.manager.tick().await.unwrap(); // open shard 1

        fx.write_decisions(&[2, 3]);
        fx.apply_decisions(&[2, 3]);
        // 不跑 tick：outbox 已有 2/3，但 shard 还没有记录。
        let state = fx.manager.local_watermark().await.unwrap();
        assert_eq!(state.min_in_flight_ts, Some(2));
        assert_eq!(state.watermark, 1);

        let tick = fx.manager.tick().await.unwrap();
        assert_eq!(tick.shipped, 2);
        assert_eq!(tick.watermark, 3);
    }

    #[tokio::test]
    async fn watermark_seals_a_completed_shard_without_waiting_for_the_timeout() {
        let fx = ShardFixture::start().await;
        fx.manager.tick().await.unwrap(); // open shard 1
        fx.write_decisions(&[2, 3, 4]);
        fx.apply_decisions(&[2, 3, 4]);
        fx.manager.tick().await.unwrap(); // ship 2/3/4, watermark = 4
        fx.manager
            .propose(CommitLogShardOp::SetEnd {
                shard_id: 1,
                end_ts: 4,
                now_ms: 1_000,
            })
            .await
            .unwrap();

        // 时钟不动：seal 只能来自 watermark，不是超时。
        let tick = fx.manager.tick().await.unwrap();
        assert_eq!(tick.stalls, 0);
        assert_eq!(tick.sealed, 1);
        let table = fx.manager.table().unwrap();
        assert_eq!(table.shards[0].sealed_at_ms, 1_000, "tick 使用注入时钟");
        assert_eq!(table.shards[0].end_ts, 4, "seal 不改边界");
    }

    #[tokio::test]
    async fn stalled_watermark_is_observable_and_still_lands_late_records() {
        let fx = ShardFixture::start().await;
        fx.manager.tick().await.unwrap(); // open shard 1
        fx.write_decisions(&[2, 3]);
        fx.manager
            .propose(CommitLogShardOp::SetEnd {
                shard_id: 1,
                end_ts: 3,
                now_ms: 2_000,
            })
            .await
            .unwrap();

        // 决策 2 未 apply：watermark 停在 1，< end_ts。
        let tick = fx.manager.tick().await.unwrap();
        assert_eq!(tick.sealed, 0);
        assert_eq!(tick.stalls, 0);

        fx.set_now(2_000 + 150_000);
        let tick = fx.manager.tick().await.unwrap();
        assert_eq!(tick.seal_lag_secs, 150, "seal 延迟按秒上报");
        assert_eq!(tick.stalls, 0);

        fx.set_now(2_000 + 300_000);
        let tick = fx.manager.tick().await.unwrap();
        assert_eq!(tick.stalls, 1, "超时后走兜底并计入停滞");
        assert_eq!(tick.sealed, 1);

        // seal 之后迟到的区间内记录仍必须落进 shard（不丢事件）。
        fx.apply_decisions(&[2]);
        let tick = fx.manager.tick().await.unwrap();
        assert_eq!(tick.shipped, 1);
        assert_eq!(fx.manager.records_at(1, 2).await.unwrap().len(), 1);

        // seal 后不再接受越界记录：边界 [1,3) 之外的 ts=3 被 shard 拒收。
        let response = fx
            .manager
            .runtime(1)
            .unwrap()
            .raft
            .client_write(Command::CommitLogAppend {
                shard_id: 1,
                start_ts: 1,
                end_ts: 3,
                max_txn_bytes: fx.manager.config().max_txn_bytes,
                records: vec![crate::storage::commit_log::CommitRecord {
                    shard_id: 1,
                    commit_ts: 3,
                    txn_id: b"txn-3".to_vec(),
                    start_ts: 2,
                    region_id: fx.data.region_id(),
                    txn_bytes: 16,
                    changes: vec![crate::storage::commit_log::CommitChange {
                        key: b"k3".to_vec(),
                        value: Some(b"v".to_vec()),
                        prev: None,
                    }],
                }],
            })
            .await
            .unwrap();
        match response.response() {
            Response::CommitLogAppend {
                inserted, rejected, ..
            } => {
                assert_eq!(*inserted, 0, "sealed shard 不写越界记录");
                assert_eq!(*rejected, 1);
            }
            other => panic!("unexpected response: {other:?}"),
        }
        assert!(fx.manager.records_at(1, 3).await.unwrap().is_empty());
    }
}
