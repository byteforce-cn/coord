// 数据 Region 的运行时生命周期（ADR-0015 D6，#160）
//
// 权威来源是 region 0 的 Region 表（#159）；本模块只做一件事：让「表里的区间」
// 与「本节点上的 raft 组 / 存储 / CompactionManager」保持一致。
//
// - **新增**：表里有、本节点是副本、但还没有运行时 → 按目录隔离装配
//   （`regions/region-{id:016x}`，同 `spawn_configured_regions` 的布局），并同时
//   启动该 Region 自己的 `CompactionManager`（B-RG-1：新建 Region 永不压缩）。
// - **移除**：表里已没有 → `raft.shutdown()` → 摘运行时与 CompactionManager。
//   数据目录**保留**：split/merge 的数据搬迁与回收归 #163/#164，停机路径不做
//   删除（删了就没法救援）。
// - **顺序契约**：先让路由表收敛（`apply_region_table`），再装配运行时。收敛
//   窗口内新 Region 的请求得到可重试的 `RouteNotReady`（`route_runtime`），不会
//   读到错误的 Region。装配失败不阻断同一个 tick 的其它 Region，下一次 tick 重试。
//
// 成员变更（`meta.peers` 与 raft 成员集不一致）与 leader 转移不在本模块：v1 静态
// 复制下每个 Region 的 peers 相同，按 Region 差异化分布归 #161/#162。

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use coord_core::types::{NodeID, RegionId, RegionMeta};
use parking_lot::RwLock;

use crate::metrics::Metrics;
use crate::raft::network::{RaftNetworkFactoryImpl, RaftRpcService};
use crate::raft::region::RegionManager;
use crate::raft::region_runtime::{
    region_data_dir, RegionCompactProposer, RegionRuntime, RegionRuntimeSpec,
};
use crate::raft::{CoordRaft, RaftConfig};
use crate::storage::compaction::{CompactProposer, CompactionConfig, CompactionManager};
use crate::storage::mvcc::{MvccStorage, RevisionMode};
use crate::storage::object_store::ObjectStoreCtx;
use crate::storage::redb_backend::RedbBackend;

/// 后台 reconcile 的默认间隔。
///
/// 一次 tick 是一次单键读（表版本没前进就 no-op），间隔只决定「表变化 → 运行时
/// 就绪」的延迟上界；期间数据面拿到可重试的 `RouteNotReady`。
pub const REGION_RECONCILE_INTERVAL: Duration = Duration::from_secs(1);

/// 构造 [`RegionLifecycle`] 所需的节点级依赖（全部是启动期装配一次的句柄）。
pub struct RegionLifecycleSpec {
    /// 本节点 ID
    pub node_id: NodeID,
    /// 路由表（与数据面共享同一实例）
    pub manager: Arc<RegionManager>,
    /// Region 间 raft 网络工厂（连接池共享）
    pub factory: RaftNetworkFactoryImpl,
    /// Region raft RPC 注册表（动态装配要注册解复用键）
    pub rpc: RaftRpcService,
    /// 节点数据根目录（`region_data_dir` 的基准）
    pub base_dir: PathBuf,
    /// Region 组的 raft 调优
    pub raft_config: Arc<RaftConfig>,
    /// 对象存储启用配置（跟随启动装配；动态 Region 与静态 Region 同布局）
    pub object_store: Option<Arc<ObjectStoreCtx>>,
    /// revision 语义（`[kv_t].enabled` = `CommitTs`）
    pub revision_mode: RevisionMode,
    /// region 0 的 MVCC（权威 Region 表的本地副本）
    pub table: Arc<MvccStorage<RedbBackend>>,
    /// region 0 的 raft（决定谁执行首次成员初始化）
    pub region0_raft: CoordRaft,
    /// 指标（compaction 回收量等）
    pub metrics: Option<Arc<Metrics>>,
    /// compaction 水位上界来源（`[kv_t]` = `CoordNode::safe_ts`）
    pub safe_ts: Option<Arc<dyn crate::storage::compaction::SafeTsSource>>,
    /// per-Region compaction 配置
    pub compaction: CompactionConfig,
}

/// 一次 reconcile 的结果（用于日志/测试断言）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReconcileOutcome {
    /// 路由表版本是否前进（`apply_region_table` 的返回值）
    pub applied: bool,
    /// 新装配的运行时数量
    pub spawned: u64,
    /// 停机并摘除的运行时数量
    pub retired: u64,
}

struct RegionMember {
    runtime: Arc<RegionRuntime>,
    compaction: CompactionManager<RedbBackend>,
}

/// 数据 Region 运行时的所有者（每节点一个）。
pub struct RegionLifecycle {
    node_id: NodeID,
    manager: Arc<RegionManager>,
    factory: RaftNetworkFactoryImpl,
    rpc: RaftRpcService,
    base_dir: PathBuf,
    raft_config: Arc<RaftConfig>,
    object_store: Option<Arc<ObjectStoreCtx>>,
    revision_mode: RevisionMode,
    table: Arc<MvccStorage<RedbBackend>>,
    region0_raft: CoordRaft,
    metrics: Option<Arc<Metrics>>,
    safe_ts: Option<Arc<dyn crate::storage::compaction::SafeTsSource>>,
    compaction: CompactionConfig,
    members: RwLock<std::collections::BTreeMap<RegionId, RegionMember>>,
    /// 串行化装配/停机：同一时刻只允许一个 Region 进出，避免「刚 spawn 就被
    /// 另一个 tick retire」的交错。
    transition: tokio::sync::Mutex<()>,
}

impl RegionLifecycle {
    pub fn new(spec: RegionLifecycleSpec) -> Self {
        Self {
            node_id: spec.node_id,
            manager: spec.manager,
            factory: spec.factory,
            rpc: spec.rpc,
            base_dir: spec.base_dir,
            raft_config: spec.raft_config,
            object_store: spec.object_store,
            revision_mode: spec.revision_mode,
            table: spec.table,
            region0_raft: spec.region0_raft,
            metrics: spec.metrics,
            safe_ts: spec.safe_ts,
            compaction: spec.compaction,
            members: RwLock::new(std::collections::BTreeMap::new()),
            transition: tokio::sync::Mutex::new(()),
        }
    }

    pub fn node_id(&self) -> NodeID {
        self.node_id
    }

    /// 本节点已接管的 Region 数（= 有 CompactionManager 的 Region 数）。
    pub fn member_count(&self) -> usize {
        self.members.read().len()
    }

    /// 该 Region 是否已有 CompactionManager（B-RG-1 的验收点）。
    pub fn has_compaction(&self, region_id: RegionId) -> bool {
        self.members.read().contains_key(&region_id)
    }

    /// 已接管的运行时（未接管 = `None`）。
    pub fn runtime(&self, region_id: RegionId) -> Option<Arc<RegionRuntime>> {
        self.members
            .read()
            .get(&region_id)
            .map(|member| Arc::clone(&member.runtime))
    }

    /// 手动触发某个 Region 的完整 compaction（运维/测试入口）。
    pub fn trigger_compaction(&self, region_id: RegionId) {
        if let Some(member) = self.members.read().get(&region_id) {
            member.compaction.trigger_compact();
        }
    }

    /// 接管（或补装配）一个 Region：运行时 + CompactionManager。
    ///
    /// - 本节点不是该 Region 的副本 → `Ok(None)`（不装配、不接管）；
    /// - 已接管 → 幂等返回；
    /// - 运行时就绪但 compaction 未接管（启动期静态装配的 Region）→ 只补
    ///   CompactionManager。
    pub async fn ensure_region(
        &self,
        meta: &RegionMeta,
    ) -> std::result::Result<Option<Arc<RegionRuntime>>, String> {
        if !self.is_local_replica(meta) {
            return Ok(None);
        }
        if let Some(runtime) = self.runtime(meta.region_id) {
            return Ok(Some(runtime));
        }

        let _guard = self.transition.lock().await;
        // 双检：上一个持有者可能刚装配完同一 Region。
        if let Some(runtime) = self.runtime(meta.region_id) {
            return Ok(Some(runtime));
        }

        let runtime = match self.manager.runtime(meta.region_id) {
            Some(runtime) => runtime,
            None => self.spawn_runtime(meta).await?,
        };
        let compaction = self.start_compaction(&runtime);
        self.members.write().insert(
            meta.region_id,
            RegionMember {
                runtime: Arc::clone(&runtime),
                compaction,
            },
        );
        tracing::info!(
            region_id = meta.region_id,
            node = self.node_id,
            "region runtime attached (raft group + compaction manager)"
        );
        Ok(Some(runtime))
    }

    /// 停机并摘除一个 Region 的运行时与 CompactionManager。
    ///
    /// 返回 `Ok(false)` = 本来就没接管。shutdown 失败时保留运行时并返回 `Err`
    /// （下一次 reconcile 重试）：宁可晚停，也不让一个仍在跑的 raft 组丢句柄。
    /// 数据目录保留（搬迁/回收归 #163/#164）。
    pub async fn retire_region(&self, region_id: RegionId) -> std::result::Result<bool, String> {
        let _guard = self.transition.lock().await;
        let member = self.members.write().remove(&region_id);
        match member {
            Some(member) => {
                if let Err(err) = member.runtime.raft.shutdown().await {
                    self.members.write().insert(region_id, member);
                    return Err(format!("region {region_id} raft shutdown failed: {err}"));
                }
                member.compaction.shutdown();
            }
            None => {
                // 只在 manager 里存在运行时（尚未被本模块接管）：同样要先停机。
                let Some(runtime) = self.manager.runtime(region_id) else {
                    return Ok(false);
                };
                runtime
                    .raft
                    .shutdown()
                    .await
                    .map_err(|err| format!("region {region_id} raft shutdown failed: {err}"))?;
            }
        }
        // 路由条目已随表移除（apply_region_table）；这里兜底，幂等。
        let _ = self.manager.unregister_region(region_id);
        self.manager.detach_runtime(region_id);
        tracing::info!(
            region_id,
            node = self.node_id,
            "region runtime retired (data dir kept for split/merge recovery)"
        );
        Ok(true)
    }

    /// 让运行时跟随本节点已 apply 的权威 Region 表。
    ///
    /// 表未初始化（`version == 0`）→ no-op：`[kv_t]` 关闭的 v1 静态装配不受影响。
    /// 单个 Region 装配/停机失败只记日志（下一次 tick 重试），不阻断其它 Region，
    /// 也不回滚路由表——表是权威，运行时是它的派生状态。
    pub async fn reconcile(&self) -> std::result::Result<ReconcileOutcome, String> {
        let table = self
            .table
            .region_table()
            .map_err(|err| format!("read authoritative region table: {err}"))?;
        if table.version == 0 {
            return Ok(ReconcileOutcome::default());
        }
        let applied = self
            .manager
            .apply_region_table(&table)
            .map_err(|err| format!("apply authoritative region table v{}: {err}", table.version))?;

        let desired: BTreeSet<RegionId> = table
            .regions
            .iter()
            .filter(|meta| self.is_local_replica(meta))
            .map(|meta| meta.region_id)
            .collect();
        let mut outcome = ReconcileOutcome {
            applied,
            ..ReconcileOutcome::default()
        };

        for meta in table
            .regions
            .iter()
            .filter(|meta| desired.contains(&meta.region_id))
        {
            if self.manager.runtime(meta.region_id).is_some() && self.has_compaction(meta.region_id)
            {
                continue;
            }
            let existed = self.manager.runtime(meta.region_id).is_some();
            match self.ensure_region(meta).await {
                Ok(Some(_)) => {
                    if !existed {
                        outcome.spawned += 1;
                    }
                }
                Ok(None) => {}
                Err(err) => tracing::warn!(
                    region_id = meta.region_id,
                    error = %err,
                    "assemble region runtime failed; retrying on the next reconcile tick"
                ),
            }
        }

        // 先取快照再停机：边迭代边改运行时会看到自己刚摘除的条目。
        let extra: Vec<RegionId> = self
            .manager
            .list_runtimes()
            .iter()
            .map(|runtime| runtime.region_id())
            .filter(|region_id| !desired.contains(region_id))
            .collect();
        for region_id in extra {
            match self.retire_region(region_id).await {
                Ok(true) => outcome.retired += 1,
                Ok(false) => {}
                Err(err) => tracing::warn!(
                    region_id,
                    error = %err,
                    "retire region runtime failed; retrying on the next reconcile tick"
                ),
            }
        }
        Ok(outcome)
    }

    /// 启动后台 reconcile（受监督：任务意外退出会被记为能力失效，而不是静默消失）。
    pub fn start_reconcile_loop(
        self: &Arc<Self>,
        interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        let this = Arc::clone(self);
        crate::supervisor::spawn_supervised("region_lifecycle_reconcile", async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                match this.reconcile().await {
                    Ok(outcome)
                        if outcome.applied || outcome.spawned > 0 || outcome.retired > 0 =>
                    {
                        tracing::info!(
                            table_version = this.manager.table_version(),
                            spawned = outcome.spawned,
                            retired = outcome.retired,
                            "region lifecycle reconciled"
                        );
                    }
                    Ok(_) => {}
                    Err(err) => {
                        tracing::warn!(error = %err, "region lifecycle reconcile failed; retrying on the next tick");
                    }
                }
            }
        })
    }

    fn is_local_replica(&self, meta: &RegionMeta) -> bool {
        meta.peers.iter().any(|peer| peer.node_id == self.node_id)
    }

    async fn spawn_runtime(
        &self,
        meta: &RegionMeta,
    ) -> std::result::Result<Arc<RegionRuntime>, String> {
        let spec = RegionRuntimeSpec {
            meta: meta.clone(),
            data_dir: region_data_dir(&self.base_dir, meta.region_id),
            raft_config: Arc::clone(&self.raft_config),
            object_store: self.object_store.clone(),
            revision_mode: self.revision_mode,
        };
        // 首次成员初始化只由 region 0 leader 提交（同 Commit Log shard 组）：
        // 其余节点用空成员集启动，从 leader 复制的日志里学会配置。
        let initialize = self.region0_raft.current_leader().await == Some(self.node_id);
        match self
            .manager
            .spawn_registered_region(&self.factory, &self.rpc, spec.clone(), initialize)
            .await
        {
            Ok(runtime) => Ok(runtime),
            Err(err) if initialize => {
                tracing::info!(
                    region_id = meta.region_id,
                    error = %err,
                    "region initialize failed (likely a concurrent initializer); retrying as follower"
                );
                self.manager
                    .spawn_registered_region(&self.factory, &self.rpc, spec, false)
                    .await
                    .map_err(|err| format!("assemble region {} runtime: {err}", meta.region_id))
            }
            Err(err) => Err(format!("assemble region {} runtime: {err}", meta.region_id)),
        }
    }

    fn start_compaction(&self, runtime: &RegionRuntime) -> CompactionManager<RedbBackend> {
        let region_id = runtime.region_id();
        let proposer: Arc<dyn CompactProposer> =
            Arc::new(RegionCompactProposer::from_runtime(self.node_id, runtime));
        let manager = CompactionManager::start_with_safe_ts(
            Arc::clone(&runtime.mvcc),
            self.compaction.clone(),
            Some(proposer),
            self.metrics.clone(),
            self.safe_ts.clone(),
        );
        tracing::info!(
            region_id,
            node = self.node_id,
            "region compaction manager started"
        );
        manager
    }
}
