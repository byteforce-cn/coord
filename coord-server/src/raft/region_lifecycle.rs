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

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use coord_core::types::{NodeID, RegionId, RegionMeta};
use parking_lot::RwLock;

use crate::metrics::Metrics;
use crate::raft::network::{RaftNetworkFactoryImpl, RaftRpcService};
use crate::raft::region::{RegionHandle, RegionManager};
use crate::raft::region_runtime::{
    region_data_dir, spawn_region_runtime, RegionCompactProposer, RegionRuntime, RegionRuntimeSpec,
};
use crate::raft::type_config::IntentPhase;
use crate::raft::{CoordRaft, RaftConfig};
use crate::storage::compaction::{CompactProposer, CompactionConfig, CompactionManager};
use crate::storage::mvcc::{MvccStorage, RevisionMode};
use crate::storage::object_store::ObjectStoreCtx;
use crate::storage::redb_backend::RedbBackend;
use crate::storage::region_table::{RegionChange, RegionTable};

/// 后台 reconcile 的默认间隔。
///
/// 一次 tick 是一次单键读（表版本没前进就 no-op），间隔只决定「表变化 → 运行时
/// 就绪」的延迟上界；期间数据面拿到可重试的 `RouteNotReady`。
pub const REGION_RECONCILE_INTERVAL: Duration = Duration::from_secs(1);

/// 预创建目标目录里的持久化标记：存在 = 这个目录是「为 split 预创建、从未
/// cutover」的目标。
///
/// 不能只凭「表里没有、intent 里也没有」判定目录可回收——正式 Region 被 merge
/// 从表里移除后目录要保留（救援路径）。标记在预创建时写入、提升时删除（及接管
/// 时兜底清理），因此跨进程重启仍能区分「孤儿目标」与「已提升过的 Region」。
pub const PENDING_TARGET_MARKER: &str = "pending-split-target";

/// 写入预创建标记（先落标记再建 raft 组：崩溃留下的目录必须能被重启后的清扫
/// 认出来）。`create_dir_all` 允许目录已存在（重试/重启路径）。
fn mark_pending_target(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    std::fs::write(
        dir.join(PENDING_TARGET_MARKER),
        b"pending split target; removed when the region is promoted\n",
    )
}

/// `regions/region-{id:016x}` → id（非该形态的目录一律忽略）。
fn parse_region_dir_name(name: &std::ffi::OsStr) -> Option<RegionId> {
    let hex = name.to_str()?.strip_prefix("region-")?;
    let region_id = RegionId::from_str_radix(hex, 16).ok()?;
    (region_id != 0).then_some(region_id)
}

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
    /// 新预创建的 split 目标数量（cutover 前，未路由）
    pub pending_spawned: u64,
    /// cutover 后提升为正式 Region 的 split 目标数量
    pub pending_promoted: u64,
    /// 未 cutover 就被回收（停机）的 split 目标数量
    pub pending_retired: u64,
    /// 重启后按持久化标记清扫的孤儿 split 目标目录数量
    pub pending_dirs_swept: u64,
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
    members: RwLock<BTreeMap<RegionId, RegionMember>>,
    /// cutover 前的 split 目标：raft 组已装配但**未注册路由**，也不参与
    /// `RegionManager::list_runtimes`（safe_ts / watermark / decision apply 的
    /// 聚合只看已 cutover 的 Region）。
    pending: RwLock<BTreeMap<RegionId, Arc<RegionRuntime>>>,
    /// 已停机、待删除数据目录的 split 目标（删除失败下轮重试）。
    reclaim: RwLock<BTreeSet<RegionId>>,
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
            members: RwLock::new(BTreeMap::new()),
            pending: RwLock::new(BTreeMap::new()),
            reclaim: RwLock::new(BTreeSet::new()),
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

    /// cutover 前预创建的目标 Region 数（未路由）。
    pub fn pending_count(&self) -> usize {
        self.pending.read().len()
    }

    /// 预创建中的目标 Region 运行时（缺失 = None）。
    pub fn pending_runtime(&self, region_id: RegionId) -> Option<Arc<RegionRuntime>> {
        self.pending.read().get(&region_id).cloned()
    }

    /// 搬迁目标运行时：已接管的 Region 或 cutover 前的 pending 目标。
    ///
    /// 数据搬迁只经目标 Region 自己的 raft 写入；resolver 用它取目标，pending
    /// 阶段不对外路由，因此不会提前服务读写。
    pub fn migration_target(&self, region_id: RegionId) -> Option<Arc<RegionRuntime>> {
        self.runtime(region_id)
            .or_else(|| self.pending_runtime(region_id))
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
        // 提升窗口里崩溃/重启后目标已进表：落过 pending 标记的目录这里是正式
        // Region，清掉标记（否则将来 merge 把 Region 移出表后会被当成孤儿回收）。
        self.clear_pending_marker(meta.region_id);
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

    /// intent journal 里尚未提升的目标 Region（不含表里已有的）；不做本节点
    /// 副本过滤——孤儿目录清扫用它判断「是否仍被计划引用」。
    ///
    /// 只认 `Prepared`/`Applied`：`Rejected` 计划不改表，目标不该继续占用 raft
    /// 组或目录。`Applied` 但表中尚无目标只出现在本节点视图滞后的窗口，按 pending
    /// 处理下一 tick 就会提升。
    fn planned_targets(&self, table: &RegionTable) -> std::result::Result<Vec<RegionMeta>, String> {
        let intents = self
            .table
            .pd_intents()
            .map_err(|err| format!("read split/merge intents: {err}"))?;
        let mut targets: BTreeMap<RegionId, RegionMeta> = BTreeMap::new();
        for intent in intents {
            if matches!(intent.phase, IntentPhase::Rejected { .. }) {
                continue;
            }
            for change in &intent.plan.changes {
                let RegionChange::Add { meta } = change else {
                    continue;
                };
                if table.get(meta.region_id).is_none() {
                    targets.insert(meta.region_id, meta.clone());
                }
            }
        }
        Ok(targets.into_values().collect())
    }

    /// 本节点需要预创建的 split 目标（intent journal 里尚未 cutover 的 Add）。
    fn desired_pending_targets(
        &self,
        table: &RegionTable,
    ) -> std::result::Result<Vec<RegionMeta>, String> {
        Ok(self
            .planned_targets(table)?
            .into_iter()
            .filter(|meta| self.is_local_replica(meta))
            .collect())
    }

    /// 删除目录上的 pending 标记（提升/接管后目录已是正式 Region）。
    ///
    /// 删除失败只告警：表里的 Region 清扫不碰，且同一 tick 的接管路径会再试
    /// 一次；真正的风险只在「清理一直失败 + 该 Region 后来被 merge 移出表」。
    fn clear_pending_marker(&self, region_id: RegionId) {
        let path = region_data_dir(&self.base_dir, region_id).join(PENDING_TARGET_MARKER);
        match std::fs::remove_file(&path) {
            Ok(()) => tracing::info!(
                region_id,
                node = self.node_id,
                "pending split target marker cleared"
            ),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => tracing::warn!(
                region_id,
                path = %path.display(),
                error = %err,
                "clear pending split target marker failed"
            ),
        }
    }

    /// 装配一个未注册路由的目标 Region 运行时（cutover 前）。
    ///
    /// 数据搬迁必须经目标 Region 的 raft 日志，而 append 需要多数派副本都有该
    /// 组；这里按 intent 的计划元数据先建组。路由仍由权威表驱动，因此 pending
    /// 阶段不会提前服务读写。
    async fn ensure_pending(&self, meta: &RegionMeta) -> std::result::Result<bool, String> {
        let region_id = meta.region_id;
        if !self.is_local_replica(meta) {
            return Ok(false);
        }
        if self.pending.read().contains_key(&region_id) || self.manager.runtime(region_id).is_some()
        {
            return Ok(false);
        }
        let _guard = self.transition.lock().await;
        if self.pending.read().contains_key(&region_id) || self.manager.runtime(region_id).is_some()
        {
            return Ok(false);
        }
        let spec = RegionRuntimeSpec {
            meta: meta.clone(),
            data_dir: region_data_dir(&self.base_dir, region_id),
            raft_config: Arc::clone(&self.raft_config),
            object_store: self.object_store.clone(),
            revision_mode: self.revision_mode,
        };
        // 先落持久化标记再建组：进程在「预创建 → 提升」之间崩溃时，重启后的
        // reconcile 只能靠它认出「从未提升过」的目标目录。
        mark_pending_target(&spec.data_dir)
            .map_err(|err| format!("mark split target dir {}: {err}", spec.data_dir.display()))?;
        // 成员初始化只由 region 0 leader 提交（与数据 Region / Commit Log shard
        // 组同约定）：其余节点用空成员集启动，从 leader 复制的日志里学会配置。
        let initialize = self.region0_raft.current_leader().await == Some(self.node_id);
        let handle = Arc::new(RegionHandle::new(meta.clone()));
        let runtime = match spawn_region_runtime(
            self.node_id,
            &self.factory,
            &self.rpc,
            spec.clone(),
            Arc::clone(&handle),
            initialize,
        )
        .await
        {
            Ok(runtime) => runtime,
            Err(err) if initialize => {
                tracing::info!(
                    region_id,
                    error = %err,
                    "pending region initialize failed (likely a concurrent initializer); retrying as follower"
                );
                spawn_region_runtime(self.node_id, &self.factory, &self.rpc, spec, handle, false)
                    .await
                    .map_err(|err| format!("pre-create split target region {region_id}: {err}"))?
            }
            Err(err) => return Err(format!("pre-create split target region {region_id}: {err}")),
        };
        self.pending.write().insert(region_id, runtime);
        tracing::info!(
            region_id,
            node = self.node_id,
            "split target runtime pre-created (unrouted)"
        );
        Ok(true)
    }

    /// 停机一个未 cutover 的目标 Region，并登记其数据目录待回收。
    ///
    /// shutdown 失败时保留 pending 待下一 tick 重试。`Ok(false)` = 本来就不在
    /// pending。目录删除由 [`Self::reclaim_aborted_dirs`] 按最新视图复核后执行。
    async fn retire_pending(&self, region_id: RegionId) -> std::result::Result<bool, String> {
        let _guard = self.transition.lock().await;
        let Some(runtime) = self.pending.read().get(&region_id).cloned() else {
            return Ok(false);
        };
        runtime
            .raft
            .shutdown()
            .await
            .map_err(|err| format!("pending region {region_id} raft shutdown failed: {err}"))?;
        self.pending.write().remove(&region_id);
        self.reclaim.write().insert(region_id);
        tracing::info!(
            region_id,
            node = self.node_id,
            "split target runtime retired without cutover (data dir queued for reclaim)"
        );
        Ok(true)
    }

    /// 清扫崩溃遗留的 split 目标目录（跨进程重启的回收路径）。
    ///
    /// 重启后内存里的 [`Self::reclaim`] 是空的：预创建完、还没 cutover 的目标若在
    /// 停机期间丢掉 intent（执行失败/被裁掉），目录就只剩持久化标记。判据（全部
    /// 满足才删）：
    ///
    /// 1. 目录带 [`PENDING_TARGET_MARKER`]——已提升过的 Region 即使之后被 merge
    ///    移出表也保留目录（救援路径不变）；
    /// 2. 权威表里没有这个 id（在表里 = 已 cutover 或 id 已被重新使用）；
    /// 3. intent journal 里没有未终止计划引用它（重启后仍被计划 = 搬迁还要用它）。
    ///
    /// 由 [`Self::reconcile`] 每拍在拿到非空权威表后调用：目录数很少，删除失败下拍
    /// 重试，重复执行幂等；持 `transition` 与预创建/停机互斥。表未初始化
    /// （`version == 0`，v1 静态装配）没有权威判据 → 不碰任何目录。
    pub async fn sweep_orphan_split_dirs(&self) -> std::result::Result<u64, String> {
        let table = self
            .table
            .region_table()
            .map_err(|err| format!("read authoritative region table: {err}"))?;
        self.sweep_orphan_split_dirs_with(&table).await
    }

    async fn sweep_orphan_split_dirs_with(
        &self,
        table: &RegionTable,
    ) -> std::result::Result<u64, String> {
        if table.version == 0 {
            return Ok(0);
        }
        let _guard = self.transition.lock().await;
        let planned: BTreeSet<RegionId> = self
            .planned_targets(table)?
            .into_iter()
            .map(|meta| meta.region_id)
            .collect();
        let regions_dir = self.base_dir.join("regions");
        let entries = match std::fs::read_dir(&regions_dir) {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(err) => {
                return Err(format!("scan {}: {err}", regions_dir.display()));
            }
        };
        let mut swept = 0;
        for entry in entries {
            let entry = entry.map_err(|err| format!("scan {}: {err}", regions_dir.display()))?;
            let Some(region_id) = parse_region_dir_name(&entry.file_name()) else {
                continue;
            };
            if table.get(region_id).is_some()
                || planned.contains(&region_id)
                || self.pending.read().contains_key(&region_id)
            {
                continue;
            }
            let dir = entry.path();
            if !dir.join(PENDING_TARGET_MARKER).is_file() {
                continue;
            }
            match std::fs::remove_dir_all(&dir) {
                Ok(()) => {
                    swept += 1;
                    tracing::info!(
                        region_id,
                        node = self.node_id,
                        path = %dir.display(),
                        "orphan split target dir reclaimed after restart"
                    );
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => swept += 1,
                Err(err) => tracing::warn!(
                    region_id,
                    path = %dir.display(),
                    error = %err,
                    "reclaim orphan split target dir failed; retrying on the next reconcile tick"
                ),
            }
        }
        Ok(swept)
    }

    /// 删除未 cutover 目标的数据目录（已导入的页与 raft 日志）。
    ///
    /// 删除前用最新视图复核：表里有、pending 已重新装配、或 intent 再次引用
    /// 同一 id（失败后 id 可能被新的 split 复用）——任一命中都不删。持
    /// `transition` 让新的 pending 装配不能与删除交错；剩余窗口只有「删除系统
    /// 调用期间表被并发 cutover」，而新 cutover 需要已装配的 pending 与完成搬迁。
    async fn reclaim_aborted_dirs(&self) {
        if self.reclaim.read().is_empty() {
            return;
        }
        let _guard = self.transition.lock().await;
        let table = match self.table.region_table() {
            Ok(table) => table,
            Err(err) => {
                tracing::warn!("reclaim split target dirs: read region table failed: {err}");
                return;
            }
        };
        let desired: BTreeSet<RegionId> = match self.desired_pending_targets(&table) {
            Ok(metas) => metas.into_iter().map(|meta| meta.region_id).collect(),
            Err(err) => {
                tracing::warn!("reclaim split target dirs: read intents failed: {err}");
                return;
            }
        };
        let candidates: Vec<RegionId> = self.reclaim.read().iter().copied().collect();
        for region_id in candidates {
            if table.get(region_id).is_some()
                || self.pending.read().contains_key(&region_id)
                || desired.contains(&region_id)
            {
                // 同一 id 已被新计划/表重新使用：目录现在是活的。
                self.reclaim.write().remove(&region_id);
                continue;
            }
            let dir = region_data_dir(&self.base_dir, region_id);
            match std::fs::remove_dir_all(&dir) {
                Ok(()) => {
                    self.reclaim.write().remove(&region_id);
                    tracing::info!(
                        region_id,
                        path = %dir.display(),
                        "aborted split target dir reclaimed"
                    );
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    self.reclaim.write().remove(&region_id);
                }
                Err(err) => tracing::warn!(
                    region_id,
                    path = %dir.display(),
                    error = %err,
                    "reclaim aborted split target dir failed; retrying on the next reconcile tick"
                ),
            }
        }
    }

    /// 让运行时跟随本节点已 apply 的权威 Region 表与 split/merge intent。
    ///
    /// 表未初始化（`version == 0`）→ no-op：`[kv_t]` 关闭的 v1 静态装配不受影响。
    /// 单个 Region 装配/停机失败只记日志（下一次 tick 重试），不阻断其它 Region，
    /// 也不回滚路由表——表是权威，运行时是它的派生状态。
    ///
    /// 顺序契约：先提升已 cutover 的 pending 目标（装入 runtimes），再让路由表
    /// 收敛（复用同一 handle），然后装配/接管表内 Region，最后预创建尚未 cutover
    /// 的 split 目标并回收表外运行时。
    pub async fn reconcile(&self) -> std::result::Result<ReconcileOutcome, String> {
        let table = self
            .table
            .region_table()
            .map_err(|err| format!("read authoritative region table: {err}"))?;
        if table.version == 0 {
            return Ok(ReconcileOutcome::default());
        }

        let desired_pending = self.desired_pending_targets(&table)?;
        let desired_pending_ids: BTreeSet<RegionId> =
            desired_pending.iter().map(|meta| meta.region_id).collect();
        let mut outcome = ReconcileOutcome::default();

        // cutover 生效：把预创建的运行时装入 runtimes（尚未注册路由），让
        // `apply_region_table` 复用同一个 RegionHandle；intent 已消失且表里没有
        // 的目标停机回收（目录保留）。
        let pending_snapshot: Vec<(RegionId, Arc<RegionRuntime>)> = self
            .pending
            .read()
            .iter()
            .map(|(region_id, runtime)| (*region_id, Arc::clone(runtime)))
            .collect();
        for (region_id, runtime) in pending_snapshot {
            match table.get(region_id) {
                Some(meta) if self.is_local_replica(meta) => {
                    if self.manager.attach_runtime(Arc::clone(&runtime)) {
                        outcome.pending_promoted += 1;
                    }
                    self.clear_pending_marker(region_id);
                    self.pending.write().remove(&region_id);
                }
                Some(_) => {
                    if let Err(err) = self.retire_pending(region_id).await {
                        tracing::warn!(region_id, error = %err, "retire pending region failed; retrying on the next reconcile tick");
                        continue;
                    }
                    outcome.pending_retired += 1;
                }
                None if !desired_pending_ids.contains(&region_id) => {
                    match self.retire_pending(region_id).await {
                        Ok(true) => outcome.pending_retired += 1,
                        Ok(false) => {}
                        Err(err) => {
                            tracing::warn!(region_id, error = %err, "retire pending region failed; retrying on the next reconcile tick")
                        }
                    }
                }
                None => {}
            }
        }

        let applied = self
            .manager
            .apply_region_table(&table)
            .map_err(|err| format!("apply authoritative region table v{}: {err}", table.version))?;
        outcome.applied = applied;

        let desired: BTreeSet<RegionId> = table
            .regions
            .iter()
            .filter(|meta| self.is_local_replica(meta))
            .map(|meta| meta.region_id)
            .collect();

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

        // 尚未 cutover 的 split 目标：导入前每个副本都必须已有目标 raft 组，
        // 否则目标 leader 的 append 打不到多数派。未路由、不参与数据面聚合。
        for meta in &desired_pending {
            match self.ensure_pending(meta).await {
                Ok(true) => outcome.pending_spawned += 1,
                Ok(false) => {}
                Err(err) => tracing::warn!(
                    region_id = meta.region_id,
                    error = %err,
                    "pre-create split target failed; retrying on the next reconcile tick"
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

        self.reclaim_aborted_dirs().await;
        match self.sweep_orphan_split_dirs_with(&table).await {
            Ok(swept) => outcome.pending_dirs_swept = swept,
            Err(err) => tracing::warn!(
                error = %err,
                "sweep orphan split target dirs failed; retrying on the next reconcile tick"
            ),
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
                        if outcome.applied
                            || outcome.spawned > 0
                            || outcome.retired > 0
                            || outcome.pending_spawned > 0
                            || outcome.pending_promoted > 0
                            || outcome.pending_retired > 0
                            || outcome.pending_dirs_swept > 0 =>
                    {
                        tracing::info!(
                            table_version = this.manager.table_version(),
                            spawned = outcome.spawned,
                            retired = outcome.retired,
                            pending_spawned = outcome.pending_spawned,
                            pending_promoted = outcome.pending_promoted,
                            pending_retired = outcome.pending_retired,
                            pending_dirs_swept = outcome.pending_dirs_swept,
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
