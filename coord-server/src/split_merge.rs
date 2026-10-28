// Split/Merge 数据搬迁（#163 / #164）
//
// 数据搬过的每一行都必须经**目标 Region 的 raft 日志**写入：直接改目标
// `store.db` 会破坏「状态机 = 日志的确定性 apply」这一约束，新 follower、
// 快照与压缩都会与日志分叉。本模块只描述搬迁器需要的两个窄端口，真实 raft
// 与真实会话表的实现分别位于 `raft/region_runtime.rs`（`CoordRangeImportTarget`）
// 与 `kvt/txn.rs`（`impl SplitDrain for TxnSessions`）。
//
// 端口刻意不暴露 openraft/Command：搬迁器只表达「导入一页」与「等区间
// drain」，让数据搬迁可以脱离 raft 类型单测；是否经 `Command::ImportRange`、
// 如何映射错误留给端口实现。

use std::sync::Arc;
use std::time::Duration;

use coord_core::types::{NodeID, RegionId, Revision};

use crate::pd::executor::SplitMergeMigrator;
use crate::raft::network::{DrainRpcOutcome, RaftNetworkFactoryImpl};
use crate::raft::region::RegionManager;
use crate::raft::region_lifecycle::RegionLifecycle;
use crate::raft::region_runtime::CoordRangeImportTarget;
use crate::raft::type_config::{Command, Response};
use crate::raft::CoordRaft;
use crate::storage::mvcc::{MvccStorage, VersionedKv};
use crate::storage::redb_backend::RedbBackend;
use crate::storage::region_table::{RegionChange, SplitMergeKind, SplitMergePlan};

/// Region → 本节点该 Region 的业务存储（源侧导出用）。
pub type RegionMvccResolver =
    dyn Fn(RegionId) -> Option<Arc<MvccStorage<RedbBackend>>> + Send + Sync;

/// Region → 本节点已应用权威表里的 Key Range（merge 定位被吸收区间）。
///
/// 不把区间写进 `SplitMergePlan`：计划经 bincode 存进 region 0 intent journal，
/// 追加字段会让既有 intent 无法解码。
pub type RegionRangeResolver = dyn Fn(RegionId) -> Option<(Vec<u8>, Vec<u8>)> + Send + Sync;

/// Region → 经该 Region raft 导入一页的目标端口。
pub type RegionImportTargetResolver =
    dyn Fn(RegionId) -> Option<Arc<dyn RangeImportTarget>> + Send + Sync;

/// Region → 本节点上该 Region 的 raft 句柄（直写 fence 的提案入口）。
///
/// 只在**本节点是该 Region leader** 时提案才有意义（openraft 把 follower 的
/// `client_write` 拒为 `ForwardToLeader`）；split 的认领条件就是源 Region 的
/// leader，所以源侧 fence 总是可提案的。
pub type RegionRaftResolver = dyn Fn(RegionId) -> Option<CoordRaft> + Send + Sync;

/// 源 Region 决策游标之后的下一条 sequencer 决策（无 = None）。
///
/// 决策日志在 region 0；迁移器需要它来判定「导出水位以下是否还有未 apply 的
/// 决策」。直写只推进 revision、不产生决策，所以不能用
/// `last_applied_decision_ts == current_revision` 当导出判据（有直写的 Region
/// 永远不相等）。
pub type PendingDecisionResolver =
    dyn Fn(Revision) -> std::result::Result<Option<Revision>, String> + Send + Sync;

/// 目标 Region 的分页导入端口。
///
/// - `watermark` = 源快照 revision，必须逐页恒等（导入行的 `mod_revision`
///   不得越过它）；
/// - `seal_prior_history` 只在最后一页有意义：split 的全新目标据此把
///   「split 前历史不可用」的压缩水位钉到 `watermark`；merge 的接收方传
///   `false`，保留自己的历史（区间历史下限由存储层记录）；
/// - `done` 只在最后一页为真；
/// - 返回实际写入行数：重放已导入的页返回 0 而不是错误（幂等契约）。
#[async_trait::async_trait]
pub trait RangeImportTarget: Send + Sync {
    /// 清空 `[start, end)`：每次搬迁重试的“本轮权威”前置步骤。
    ///
    /// 源侧已删除的 Key 不会被重新导出（tombstone 不搬），不清空就会在
    /// 重试后复活上一次部分导入的残留。只在 cutover 前调用（区间尚不属于
    /// 目标 Region）。
    async fn reset_range(&self, start: &[u8], end: &[u8]) -> std::result::Result<(), String>;

    async fn import_range(
        &self,
        start: &[u8],
        end: &[u8],
        rows: Vec<VersionedKv>,
        watermark: u64,
        seal_prior_history: bool,
        done: bool,
    ) -> std::result::Result<u64, String>;
}

/// Split/Merge 的区间 drain 端口。
///
/// 契约：`Ok(())` 表示此刻没有活跃事务的读集命中 `[start, end)`；超时或
/// 失败必须返回 `Err`——调用方不得在未 drain 的区间上导出快照。
#[async_trait::async_trait]
pub trait SplitDrain: Send + Sync {
    async fn drain(
        &self,
        start: &[u8],
        end: &[u8],
        timeout: Duration,
    ) -> std::result::Result<(), String>;
}

/// 把 drain 扇出到其他节点的传输端口。
///
/// `Err` 表示节点不可达（连接/协议失败）；`Ok(Blocked)` 表示节点可达但
/// 区间内仍有活跃会话——两者对迁移的含义不同（D12.2：会话不复制，节点
/// 失联即其会话失效；可达节点必须真正排空）。
#[async_trait::async_trait]
pub trait DrainTransport: Send + Sync {
    async fn drain_peer(
        &self,
        node_id: NodeID,
        start: &[u8],
        end: &[u8],
        timeout: Duration,
    ) -> std::result::Result<DrainRpcOutcome, String>;
}

#[async_trait::async_trait]
impl DrainTransport for RaftNetworkFactoryImpl {
    async fn drain_peer(
        &self,
        node_id: NodeID,
        start: &[u8],
        end: &[u8],
        timeout: Duration,
    ) -> std::result::Result<DrainRpcOutcome, String> {
        self.drain_range(node_id, start, end, timeout).await
    }
}

/// split/merge 执行器所在节点的 drain 端口：本地会话表 + region 0 成员扇出。
///
/// 每个 peer 的 drain 与本地 drain 并发执行，上界都是同一个 `timeout`；不
/// 可达节点按会话失效跳过（记 warning），可达节点的 `Blocked` 直接让整次
/// 迁移失败重试。
pub struct ClusterSplitDrain {
    local: Arc<dyn SplitDrain>,
    node_id: NodeID,
    peers: Arc<dyn Fn() -> Vec<NodeID> + Send + Sync>,
    transport: Arc<dyn DrainTransport>,
}

impl ClusterSplitDrain {
    pub fn new(
        local: Arc<dyn SplitDrain>,
        node_id: NodeID,
        peers: Arc<dyn Fn() -> Vec<NodeID> + Send + Sync>,
        transport: Arc<dyn DrainTransport>,
    ) -> Self {
        Self {
            local,
            node_id,
            peers,
            transport,
        }
    }
}

#[async_trait::async_trait]
impl SplitDrain for ClusterSplitDrain {
    async fn drain(
        &self,
        start: &[u8],
        end: &[u8],
        timeout: Duration,
    ) -> std::result::Result<(), String> {
        let peers: Vec<NodeID> = (self.peers)()
            .into_iter()
            .filter(|node_id| *node_id != self.node_id)
            .collect();
        let remote = peers.iter().map(|node_id| {
            let transport = Arc::clone(&self.transport);
            async move {
                (
                    *node_id,
                    transport.drain_peer(*node_id, start, end, timeout).await,
                )
            }
        });
        let (local, remote) = futures::future::join(
            self.local.drain(start, end, timeout),
            futures::future::join_all(remote),
        )
        .await;
        local.map_err(|reason| format!("local drain failed: {reason}"))?;
        for (node_id, outcome) in remote {
            match outcome {
                Ok(DrainRpcOutcome::Drained) => {}
                Ok(DrainRpcOutcome::Blocked(reason)) => {
                    return Err(format!("node {node_id} drain failed: {reason}"));
                }
                Err(reason) => tracing::warn!(
                    node_id,
                    reason,
                    "split drain: node unreachable; treating its sessions as lost"
                ),
            }
        }
        Ok(())
    }
}

/// 每页导出的行数上界（0 = 一页取尽）。
const DEFAULT_PAGE_SIZE: usize = 1024;

/// 等区间内活跃事务收尾的默认上界；超时整次迁移失败（intent 保留重试）。
const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// 等决策屏障（导出水位以下的决策 apply 完）的默认上界。
///
/// 决策 apply 循环每拍从 region 0 日志按批推进；30s 覆盖一次批积压/换届，
/// 超时即整次迁移失败、intent 保留重试。
const DEFAULT_DECISION_WAIT: Duration = Duration::from_secs(30);

/// 决策屏障轮询间隔。
const DECISION_BARRIER_POLL: Duration = Duration::from_millis(50);

/// 等待 split 目标 Region 预创建装配上界的默认值。
///
/// executor 与 `RegionLifecycle` 各自跑循环：operator 可能先于 pending 目标
/// 装配被认领。解析失败不能立即把 operator 判 `Failed`，而要等 lifecycle 的
/// 下一拍；等待仍有界（装配始终不成功 → 失败，intent 保留重试）。
const DEFAULT_TARGET_WAIT: Duration = Duration::from_secs(10);

/// 目标解析重试间隔（`RegionLifecycle` 的 reconcile 周期为 1s）。
const TARGET_RESOLVE_POLL: Duration = Duration::from_millis(50);

/// 目标组刚预创建时等待 leader 选举的上界（与目标解析等待同一量级）。
const TARGET_LEADER_WAIT: Duration = Duration::from_secs(10);

/// fence 提案的等待上界：认领后可能刚发生换届，`client_write` 会返回
/// `ForwardToLeader`；有界重试后仍失败则整次失败（intent 保留重试）。
const FENCE_PROPOSE_WAIT: Duration = Duration::from_secs(10);

/// fence 提案重试间隔。
const FENCE_PROPOSE_POLL: Duration = Duration::from_millis(50);

/// 等本地 donor 副本 apply fence 的上界。
///
/// donor leader 的 executor 默认每 5s 扫描一次 intent；30s 给换届/一次长迁移
/// 留出余量，又不会把真正卡住的 merge 无限挂住。
const DEFAULT_DONOR_FENCE_WAIT: Duration = Duration::from_secs(30);

/// Split 的真实数据搬迁器：drain → 固定 revision 分页导出 → 经目标 raft 导入。
///
/// 幂等契约：每页导入按目标行的 `mod_revision` 去重，重放同一页返回 0；因此
/// executor 重试同一 `Prepared` intent、或进程在任意页后重启，都不会重复写
/// 或让目标 revision 倒退。源侧只读：cutover 后落在父 Region 的旧行不再被
/// 路由到，物理回收不在这里做。
pub struct RangeImportMigrator {
    source: Arc<RegionMvccResolver>,
    targets: Arc<RegionImportTargetResolver>,
    ranges: Arc<RegionRangeResolver>,
    source_raft: Arc<RegionRaftResolver>,
    drain: Arc<dyn SplitDrain>,
    /// 决策日志入口（region 0）：迁移器用它判定导出水位以下是否还有未 apply
    /// 的决策。
    pending: Arc<PendingDecisionResolver>,
    /// 本节点 ID：判断 donor Region 的 leader 是否在本节点（fence 只能本地提案）。
    node_id: NodeID,
    page_size: usize,
    drain_timeout: Duration,
    target_wait: Duration,
    donor_fence_wait: Duration,
    decision_wait: Duration,
}

impl RangeImportMigrator {
    pub fn new(
        source: Arc<RegionMvccResolver>,
        targets: Arc<RegionImportTargetResolver>,
        ranges: Arc<RegionRangeResolver>,
        source_raft: Arc<RegionRaftResolver>,
        drain: Arc<dyn SplitDrain>,
        pending: Arc<PendingDecisionResolver>,
    ) -> Self {
        Self {
            source,
            targets,
            ranges,
            source_raft,
            drain,
            pending,
            node_id: 0,
            page_size: DEFAULT_PAGE_SIZE,
            drain_timeout: DEFAULT_DRAIN_TIMEOUT,
            target_wait: DEFAULT_TARGET_WAIT,
            donor_fence_wait: DEFAULT_DONOR_FENCE_WAIT,
            decision_wait: DEFAULT_DECISION_WAIT,
        }
    }

    /// 本节点 ID（生产接线传 `RegionManager::node_id`）。
    pub fn with_node_id(mut self, node_id: NodeID) -> Self {
        self.node_id = node_id;
        self
    }

    /// 等本地 donor 副本 apply fence 的上界（0 = 只查一次）。
    pub fn with_donor_fence_wait(mut self, wait: Duration) -> Self {
        self.donor_fence_wait = wait;
        self
    }

    /// 分页大小（0 = 不分页）。小页让故障恢复的重放代价更小。
    pub fn with_page_size(mut self, page_size: usize) -> Self {
        self.page_size = page_size;
        self
    }

    /// drain 等待上界（与 #150 的 drain 契约同一个语义：超时即失败）。
    pub fn with_drain_timeout(mut self, timeout: Duration) -> Self {
        self.drain_timeout = timeout;
        self
    }

    /// 等待目标 Region 装配的上界（0 = 只解析一次）。
    pub fn with_target_wait(mut self, timeout: Duration) -> Self {
        self.target_wait = timeout;
        self
    }

    /// 等决策屏障（导出水位以下没有未 apply 的决策）的上界。
    pub fn with_decision_wait(mut self, timeout: Duration) -> Self {
        self.decision_wait = timeout;
        self
    }

    /// 非阻塞复核：`pinned` 以下没有未 apply 的决策。
    ///
    /// 导出水位钉住目标的 `DECISION_APPLIED_TS`；水位以下未 apply 的决策既不在
    /// 导出快照里、又不会被目标重放（游标已越过），只能 fail-closed。
    fn require_no_pending_decision(
        &self,
        cursor: Revision,
        pinned: Revision,
        what: &str,
    ) -> std::result::Result<(), String> {
        match (self.pending)(cursor)
            .map_err(|e| format!("{what}: scan sequencer decisions after {cursor}: {e}"))?
        {
            Some(next) if next <= pinned => Err(format!(
                "{what}: sequencer decision {next} is decided but not applied at export \
                 revision {pinned}; refusing to seed the target from an unapplied watermark"
            )),
            _ => Ok(()),
        }
    }

    /// 有界等待决策屏障：`pinned` 以下不再有未 apply 的决策。
    ///
    /// 直写被 fence 冻结后 revision 只会因决策 apply 前进，而决策 apply 会同时
    /// 推进游标，所以 barrier 之后重新取 `current_revision()` 得到的是目标游标
    /// 可以续跑的水位。等待有界：超时整次迁移失败、intent 保留重试。
    async fn wait_for_decision_barrier(
        &self,
        source: &MvccStorage<RedbBackend>,
        pinned: Revision,
        what: &str,
    ) -> std::result::Result<(), String> {
        let started = tokio::time::Instant::now();
        loop {
            let cursor = source
                .last_applied_decision_ts()
                .map_err(|e| format!("{what}: read decision cursor: {e}"))?;
            match (self.pending)(cursor)
                .map_err(|e| format!("{what}: scan sequencer decisions after {cursor}: {e}"))?
            {
                Some(next) if next <= pinned => {
                    if started.elapsed() >= self.decision_wait {
                        return Err(format!(
                            "{what}: sequencer decision {next} is still unapplied at revision \
                             {pinned} after {:?}; refusing to seed the target from an unapplied \
                             watermark",
                            self.decision_wait
                        ));
                    }
                    tokio::time::sleep(DECISION_BARRIER_POLL).await;
                }
                _ => return Ok(()),
            }
        }
    }

    /// 解析目标 Region，并在 `target_wait` 内重试（pending 预创建是异步的）。
    async fn resolve_target(
        &self,
        target_id: RegionId,
    ) -> std::result::Result<Arc<dyn RangeImportTarget>, String> {
        let started = tokio::time::Instant::now();
        loop {
            if let Some(target) = (self.targets)(target_id) {
                return Ok(target);
            }
            if started.elapsed() >= self.target_wait {
                return Err(format!(
                    "region {target_id} raft is not assembled on this node \
                     (target region pre-creation/attachment has not caught up)"
                ));
            }
            tokio::time::sleep(TARGET_RESOLVE_POLL).await;
        }
    }

    /// fence 目标：split 的源 Region + 子 Region 接管区间 `[start, end)`。
    ///
    /// merge 返回 `None`：执行器的认领条件只看吸收方（左）的 leader，donor 的
    /// leader 可能在别的节点。donor 的 fence 由 donor leader 自己的 executor 提案
    /// （见 [`Self::arm_donor_write_fence`]），吸收方只等在本地副本里看到它。
    fn split_fence(plan: &SplitMergePlan) -> Option<(RegionId, Vec<u8>, Vec<u8>)> {
        if plan.kind != SplitMergeKind::Split {
            return None;
        }
        let target_id = plan.target_region?;
        let child = plan.changes.iter().find_map(|change| match change {
            RegionChange::Add { meta } if meta.region_id == target_id => Some(meta),
            _ => None,
        })?;
        Some((
            plan.source_region,
            child.start_key.clone(),
            child.end_key.clone(),
        ))
    }

    /// 向源 Region raft 提案一条 fence 命令，有界重试换届窗口。
    async fn propose_source_fence(
        &self,
        region_id: RegionId,
        cmd: Command,
    ) -> std::result::Result<Response, String> {
        let raft = (self.source_raft)(region_id).ok_or_else(|| {
            format!("source region {region_id} raft is not assembled on this node")
        })?;
        let deadline = tokio::time::Instant::now() + FENCE_PROPOSE_WAIT;
        loop {
            match raft.client_write(cmd.clone()).await {
                Ok(resp) => return Ok(resp.response().clone()),
                Err(err) => {
                    if tokio::time::Instant::now() >= deadline {
                        return Err(format!(
                            "source region {region_id} fence proposal failed: {err}"
                        ));
                    }
                    tokio::time::sleep(FENCE_PROPOSE_POLL).await;
                }
            }
        }
    }

    /// merge 的 donor Region 与它在本地权威表里的区间。
    fn merge_donor_range(
        &self,
        plan: &SplitMergePlan,
    ) -> std::result::Result<(RegionId, Vec<u8>, Vec<u8>), String> {
        if plan.kind != SplitMergeKind::Merge {
            return Err("plan is not a merge".to_string());
        }
        let donor = plan
            .changes
            .iter()
            .find_map(|change| match change {
                RegionChange::Remove { region_id, .. } => Some(*region_id),
                _ => None,
            })
            .ok_or_else(|| "merge plan has no donor region to fence".to_string())?;
        let (start, end) = (self.ranges)(donor)
            .ok_or_else(|| format!("merge donor region {donor} is not in the local table"))?;
        Ok((donor, start, end))
    }

    /// donor leader 自己把 fence 提进 donor Region 的 raft。
    ///
    /// 返回 `Ok(true)` = 本节点是 donor leader 且 fence 已 apply；`Ok(false)` =
    /// donor 不在本节点或本节点不是它的 leader（吸收方会等别处的 donor leader
    /// 完成同一提案）。fence 只进 donor 自己的日志，才与被它挡的直写共享 apply 序。
    pub async fn arm_donor_write_fence(
        &self,
        op_id: u64,
        plan: &SplitMergePlan,
    ) -> std::result::Result<bool, String> {
        if plan.kind != SplitMergeKind::Merge {
            return Ok(false);
        }
        let (donor, start, end) = self.merge_donor_range(plan)?;
        let Some(storage) = (self.source)(donor) else {
            return Ok(false);
        };
        if storage
            .direct_write_fence()
            .map_err(|e| format!("read donor region {donor} fence: {e}"))?
            .is_some_and(|fence| fence.op_id == op_id)
        {
            return Ok(true);
        }
        let Some(raft) = (self.source_raft)(donor) else {
            return Ok(false);
        };
        if raft.current_leader().await != Some(self.node_id) {
            return Ok(false);
        }
        let cmd = Command::FenceDirectWrites {
            start_key: start,
            end_key: end,
            op_id,
        };
        match raft.client_write(cmd).await {
            Ok(resp) => match resp.response() {
                Response::DirectWritesFenced { op_id: armed } if *armed == op_id => Ok(true),
                other => Err(format!(
                    "donor region {donor} returned unexpected fence response: {other:?}"
                )),
            },
            Err(e) => Err(format!("donor region {donor} fence proposal failed: {e}")),
        }
    }

    /// 清除 donor fence。
    ///
    /// `Ok(true)` = 本地是 donor leader 且已清（或 donor 已不在本地表）；`Ok(false)`
    /// = 本节点不负责（不是 donor leader / 没有本地副本），留给真正的 leader；
    /// `Err` = 提案失败，下一拍重试。
    pub async fn release_donor_write_fence(
        &self,
        op_id: u64,
        plan: &SplitMergePlan,
    ) -> std::result::Result<bool, String> {
        if plan.kind != SplitMergeKind::Merge {
            return Ok(true);
        }
        let Ok((donor, _, _)) = self.merge_donor_range(plan) else {
            // cutover 已把 donor 从表里移除：fence 随该 Region 一起消失。
            return Ok(true);
        };
        let Some(raft) = (self.source_raft)(donor) else {
            return Ok(false);
        };
        if raft.current_leader().await != Some(self.node_id) {
            return Ok(false);
        }
        match raft
            .client_write(Command::ClearDirectWriteFence { op_id })
            .await
        {
            Ok(resp) => match resp.response() {
                Response::DirectWriteFenceCleared { .. } => Ok(true),
                other => Err(format!(
                    "donor region {donor} returned unexpected fence clear response: {other:?}"
                )),
            },
            Err(e) => Err(format!("donor region {donor} fence clear failed: {e}")),
        }
    }

    /// 等本地 donor 副本 apply 同一 `op_id` 的 fence。
    ///
    /// 这一步是 merge 的导出屏障：本地副本 apply 了 fence，就说明它已包含 fence
    /// 之前的所有直写；之后的直写会被 donor leader 拒绝。不看到 fence 就导出会
    /// 在 cutover 时丢掉窗口内的写。
    async fn wait_for_donor_fence(
        &self,
        op_id: u64,
        donor: RegionId,
        storage: &MvccStorage<RedbBackend>,
    ) -> std::result::Result<(), String> {
        let started = tokio::time::Instant::now();
        loop {
            if storage
                .direct_write_fence()
                .map_err(|e| format!("read merge donor region {donor} fence: {e}"))?
                .is_some_and(|fence| fence.op_id == op_id)
            {
                return Ok(());
            }
            if started.elapsed() >= self.donor_fence_wait {
                return Err(format!(
                    "merge donor region {donor} did not apply fence op {op_id} within {:?}; \
                     refusing to export without the write barrier",
                    self.donor_fence_wait
                ));
            }
            tokio::time::sleep(FENCE_PROPOSE_POLL).await;
        }
    }

    /// 分页导入：首页取到的 revision 钉死整次搬迁，后续页用
    /// `expected_revision` 校验源没有前进，避免把两代快照拼进同一个 Region。
    ///
    /// `seal_prior_history` 只影响最后一页。调用前先清空目标区间：本轮导出
    /// 是唯一权威，上一次部分导入的残留（尤其源侧已删除的 Key）必须消失。
    async fn import_pages(
        &self,
        source: &MvccStorage<RedbBackend>,
        target: &dyn RangeImportTarget,
        start: &[u8],
        end: &[u8],
        seal_prior_history: bool,
        expected_revision: Option<u64>,
    ) -> std::result::Result<u64, String> {
        let mut after: Option<Vec<u8>> = None;
        let mut watermark: Option<u64> = expected_revision;
        target.reset_range(start, end).await?;
        if expected_revision.is_none() {
            // 首屏导出前先等决策屏障：fence 已冻结直写，剩下的推进只来自
            // 「已决定、未 apply」的 sequencer 决策；等它们 apply 完再取水位，
            // 导出快照才是目标游标可以续跑的 cut。
            let revision = source.current_revision();
            self.wait_for_decision_barrier(source, revision, "split source export")
                .await?;
        }
        loop {
            let page = source
                .export_range_page(start, end, after.as_deref(), self.page_size, watermark)
                .map_err(|e| format!("export range page: {e}"))?;
            let pinned = *watermark.get_or_insert(page.revision);
            if seal_prior_history && after.is_none() {
                // 水位以下不得有未 apply 的决策：目标从 watermark 续跑会永久
                // 跳过它们。导出期间源继续前进由下一页的 `expected_revision`
                // 拒掉（重试时重新取水位）。
                let cursor = source
                    .last_applied_decision_ts()
                    .map_err(|e| format!("read split source decision cursor: {e}"))?;
                self.require_no_pending_decision(cursor, pinned, "split source export")?;
            }
            let done = page.next_after.is_none();
            target
                .import_range(start, end, page.rows, pinned, seal_prior_history, done)
                .await?;
            if done {
                break;
            }
            after = page.next_after;
        }
        Ok(watermark.unwrap_or(0))
    }

    async fn migrate_split(&self, plan: &SplitMergePlan) -> std::result::Result<(), String> {
        let target_id = plan
            .target_region
            .ok_or_else(|| "split plan has no target region".to_string())?;
        let child = plan
            .changes
            .iter()
            .find_map(|change| match change {
                RegionChange::Add { meta } if meta.region_id == target_id => Some(meta),
                _ => None,
            })
            .ok_or_else(|| format!("split plan does not add target region {target_id}"))?;

        let source = (self.source)(plan.source_region).ok_or_else(|| {
            format!(
                "split source region {} storage is not assembled on this node",
                plan.source_region
            )
        })?;
        let target = self.resolve_target(target_id).await?;

        let start = child.start_key.as_slice();
        let end = child.end_key.as_slice();
        self.drain.drain(start, end, self.drain_timeout).await?;
        let watermark = self
            .import_pages(source.as_ref(), target.as_ref(), start, end, true, None)
            .await?;

        tracing::info!(
            region_id = target_id,
            watermark,
            "split range import complete"
        );
        Ok(())
    }

    /// merge：左（吸收方）保留自己的历史，只把右（被吸收方）的 live 行搬进来。
    async fn migrate_merge(&self, plan: &SplitMergePlan) -> std::result::Result<(), String> {
        let absorbing = plan.source_region;
        let merged = plan
            .changes
            .iter()
            .find_map(|change| match change {
                RegionChange::Update { meta } if meta.region_id == absorbing => Some(meta),
                _ => None,
            })
            .ok_or_else(|| {
                format!("merge plan does not update the absorbing region {absorbing}")
            })?;
        let mut donors = plan.changes.iter().filter_map(|change| match change {
            RegionChange::Remove { region_id, .. } => Some(*region_id),
            _ => None,
        });
        let donor = donors
            .next()
            .ok_or_else(|| "merge plan has no donor region to absorb".to_string())?;
        if donors.next().is_some() {
            return Err("merge plan must remove exactly one donor region".to_string());
        }

        let (donor_start, donor_end) = (self.ranges)(donor).ok_or_else(|| {
            format!("merge donor region {donor} is not in the local region table")
        })?;
        if donor_end != merged.end_key
            || donor_start < merged.start_key
            || (!donor_end.is_empty() && donor_start > donor_end)
        {
            return Err(format!(
                "merge donor region {donor} range [{donor_start:?}, {donor_end:?}) does not \
                 match the merged region [{:?}, {:?})",
                merged.start_key, merged.end_key
            ));
        }

        let source = (self.source)(donor).ok_or_else(|| {
            format!("merge donor region {donor} storage is not assembled on this node")
        })?;
        let target = self.resolve_target(absorbing).await?;

        // 左右两侧的读集都要冻结：合并后它们属于同一个 Region。
        self.drain
            .drain(&merged.start_key, &merged.end_key, self.drain_timeout)
            .await?;

        // 接收方尚未 apply 到源水位时不能导入：导入行会不可见，而把
        // `current_revision` 抬过未应用决策会让该 Region 的决策 apply 永久
        // 卡住（safe_ts 用同一约束）。直接失败：cutover 不推进，调度器会在
        // apply 追上后重新下发 merge。
        let pin = source.current_revision();
        let receiver = (self.source)(absorbing).ok_or_else(|| {
            format!("absorbing region {absorbing} storage is not assembled on this node")
        })?;
        let receiver_revision = receiver.current_revision();
        if receiver_revision < pin {
            return Err(format!(
                "absorbing region {absorbing} has applied revision {receiver_revision}, behind \
                 the merge donor's {pin}; refusing to import until decision apply catches up"
            ));
        }
        if receiver_revision > pin {
            return Err(format!(
                "merge donor region {donor} has applied revision {pin}, behind the absorbing \
                 region {absorbing}'s {receiver_revision}; refusing to skip the donor's pending \
                 decisions"
            ));
        }
        // 接收方游标若落后自身水位，`(cursor, pin]` 的决策还没 apply；导入快照
        // 只覆盖 donor 区间，直接合并会让这些接收方自己的决策永远被游标跳过。
        let donor_cursor = source
            .last_applied_decision_ts()
            .map_err(|e| format!("read merge donor decision cursor: {e}"))?;
        if donor_cursor != pin {
            return Err(format!(
                "merge donor region {donor} decision cursor {donor_cursor} is behind its \
                 applied revision {pin}; refusing to import until the donor catches up"
            ));
        }
        let receiver_cursor = receiver
            .last_applied_decision_ts()
            .map_err(|e| format!("read absorbing region decision cursor: {e}"))?;
        if receiver_cursor != receiver_revision {
            return Err(format!(
                "absorbing region {absorbing} decision cursor {receiver_cursor} is behind its \
                 applied revision {receiver_revision}; refusing to skip its pending decisions"
            ));
        }

        let watermark = self
            .import_pages(
                source.as_ref(),
                target.as_ref(),
                &donor_start,
                &donor_end,
                false,
                (pin > 0).then_some(pin),
            )
            .await?;

        tracing::info!(
            region_id = absorbing,
            donor_region = donor,
            watermark,
            "merge range import complete"
        );
        Ok(())
    }
}

#[async_trait::async_trait]
impl SplitMergeMigrator for RangeImportMigrator {
    async fn migrate(&self, plan: &SplitMergePlan) -> std::result::Result<(), String> {
        match plan.kind {
            SplitMergeKind::Split => self.migrate_split(plan).await,
            SplitMergeKind::Merge => self.migrate_merge(plan).await,
        }
    }

    async fn arm_write_fence(
        &self,
        op_id: u64,
        plan: &SplitMergePlan,
    ) -> std::result::Result<(), String> {
        let Some((region_id, start, end)) = Self::split_fence(plan) else {
            // merge：吸收方扩回被吸收区间前先**无条件**摘掉上一次 split 可能留下
            // 的 fence（clear 是 best-effort，换届时提不出去；否则扩回后的区间
            // 会被一条陈旧的 fence 永久拒写）。
            let absorbing = plan.source_region;
            let cmd = Command::ClearDirectWriteFence { op_id: 0 };
            match self.propose_source_fence(absorbing, cmd).await? {
                Response::DirectWriteFenceCleared { .. } => {}
                other => {
                    return Err(format!(
                        "absorbing region {absorbing} returned unexpected fence response: {other}"
                    ))
                }
            }
            // donor 的 fence 由 donor leader 自己提案（本节点是 leader 时直接提，
            // 否则等它的 executor 扫描到 Running intent）。导出前必须看到本地副本
            // apply 同一 op_id：这是 merge 的写屏障。
            let (donor, _, _) = self.merge_donor_range(plan)?;
            let storage = (self.source)(donor).ok_or_else(|| {
                format!("merge donor region {donor} storage is not assembled on this node")
            })?;
            self.arm_donor_write_fence(op_id, plan).await?;
            self.wait_for_donor_fence(op_id, donor, storage.as_ref())
                .await?;
            return Ok(());
        };
        let cmd = Command::FenceDirectWrites {
            start_key: start,
            end_key: end,
            op_id,
        };
        match self.propose_source_fence(region_id, cmd).await? {
            Response::DirectWritesFenced { op_id: armed } if armed == op_id => Ok(()),
            other => Err(format!(
                "source region {region_id} returned unexpected fence response: {other}"
            )),
        }
    }

    async fn release_write_fence(
        &self,
        op_id: u64,
        plan: &SplitMergePlan,
    ) -> std::result::Result<bool, String> {
        let Some((region_id, ..)) = Self::split_fence(plan) else {
            // merge 不在吸收方上 arm fence（只在 arm 时清一次陈旧记录）。
            return self.release_donor_write_fence(op_id, plan).await;
        };
        let Some(raft) = (self.source_raft)(region_id) else {
            return Ok(false);
        };
        // 不是源 leader 就不能（也不该）从本节点清：把 intent 留给真正的源
        // leader，由它的下一拍重试。quick check 避免在每个 follower 上白等。
        if raft.current_leader().await != Some(self.node_id) {
            return Ok(false);
        }
        let cmd = Command::ClearDirectWriteFence { op_id };
        match raft.client_write(cmd).await {
            Ok(resp) => match resp.response() {
                Response::DirectWriteFenceCleared { op_id: cleared } if *cleared == op_id => {
                    Ok(true)
                }
                other => Err(format!(
                    "source region {region_id} returned unexpected fence response: {other:?}"
                )),
            },
            Err(e) => Err(format!("source region {region_id} fence clear failed: {e}")),
        }
    }

    async fn arm_donor_write_fence(
        &self,
        op_id: u64,
        plan: &SplitMergePlan,
    ) -> std::result::Result<bool, String> {
        RangeImportMigrator::arm_donor_write_fence(self, op_id, plan).await
    }

    async fn release_donor_write_fence(
        &self,
        op_id: u64,
        plan: &SplitMergePlan,
    ) -> std::result::Result<bool, String> {
        RangeImportMigrator::release_donor_write_fence(self, op_id, plan).await
    }
}

/// 生产接线：源 = `RegionManager` 本节点运行时 MVCC；目标 = `RegionLifecycle`
/// 的 pending/已接管运行时（搬迁经目标 Region 自己的 raft）。
///
/// `drain` 由接线层传入（`main.rs` = `ClusterSplitDrain`：本节点 `TxnSessions`
/// + region 0 成员的远程 drain 扇出）；单节点/单测可直接传本地会话表端口。
///
/// `pending` 读 region 0 决策日志（导出水位的决策屏障）；未装配时每次迁移
/// 都会 fail-closed。
pub fn range_import_migrator(
    manager: &Arc<RegionManager>,
    lifecycle: &Arc<RegionLifecycle>,
    drain: Arc<dyn SplitDrain>,
    pending: Arc<PendingDecisionResolver>,
) -> Arc<dyn SplitMergeMigrator> {
    let node_id = manager.node_id();
    let manager = Arc::clone(manager);
    let ranges_source = Arc::clone(&manager);
    let raft_source = Arc::clone(&manager);
    let source: Arc<RegionMvccResolver> = Arc::new(move |region_id| {
        manager
            .runtime(region_id)
            .map(|runtime| Arc::clone(&runtime.mvcc))
    });
    let ranges: Arc<RegionRangeResolver> = Arc::new(move |region_id| {
        ranges_source.get_region(region_id).map(|handle| {
            let meta = handle.meta.read();
            (meta.start_key.clone(), meta.end_key.clone())
        })
    });

    let lifecycle = Arc::clone(lifecycle);
    let targets: Arc<RegionImportTargetResolver> = Arc::new(move |region_id| {
        lifecycle.migration_target(region_id).map(|runtime| {
            let target: Arc<dyn RangeImportTarget> = Arc::new(
                CoordRangeImportTarget::from_runtime(&runtime).with_leader_wait(TARGET_LEADER_WAIT),
            );
            target
        })
    });

    let source_raft: Arc<RegionRaftResolver> =
        Arc::new(move |region_id| raft_source.runtime(region_id).map(|rt| rt.raft.clone()));

    Arc::new(
        RangeImportMigrator::new(source, targets, ranges, source_raft, drain, pending)
            .with_node_id(node_id),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::mvcc::{AppliedLogId, RevisionMode};
    use crate::storage::region_table::{plan_merge, plan_split, RegionTable};
    use coord_core::storage::StorageBackend;
    use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionMeta};

    fn region(id: RegionId, start: &[u8], end: &[u8]) -> RegionMeta {
        RegionMeta {
            region_id: id,
            start_key: start.to_vec(),
            end_key: end.to_vec(),
            epoch: RegionEpoch::initial(),
            peers: vec![Peer {
                node_id: 1,
                raft_addr: "127.0.0.1:1".to_string(),
                role: PeerRole::Voter,
            }],
            approximate_size: 0,
            approximate_keys: 0,
        }
    }

    fn table(regions: Vec<RegionMeta>) -> RegionTable {
        let mut table = RegionTable::default();
        table.init(regions).unwrap();
        table
    }

    /// fence 区间 = 子 Region 接管的右半段；父 Region 左半段保持可写。
    #[test]
    fn test_split_fence_covers_only_the_child_range() {
        let plan = plan_split(&table(vec![region(1, b"", b"")]), 1, b"m", 2).unwrap();
        let (region_id, start, end) = RangeImportMigrator::split_fence(&plan).expect("split fence");
        assert_eq!(region_id, 1);
        assert_eq!(start, b"m");
        assert!(end.is_empty(), "父 Region 无上界 → fence 到 +∞");
    }

    /// 父 Region 有上界时 fence 到同一上界（兄弟 Region 的区间不受影响）。
    #[test]
    fn test_split_fence_respects_the_parent_upper_bound() {
        let plan = plan_split(
            &table(vec![region(1, b"", b"z"), region(3, b"z", b"")]),
            1,
            b"m",
            2,
        )
        .unwrap();
        let (_, start, end) = RangeImportMigrator::split_fence(&plan).expect("split fence");
        assert_eq!(start, b"m");
        assert_eq!(end, b"z");
    }

    /// merge 没有 split 的源区间 fence（donor fence 由 donor leader 单独提案）。
    #[test]
    fn test_merge_has_no_split_fence_range() {
        let plan = plan_merge(
            &table(vec![region(1, b"", b"m"), region(2, b"m", b"")]),
            1,
            2,
        )
        .unwrap();
        assert!(RangeImportMigrator::split_fence(&plan).is_none());
    }

    struct NoopDrain;

    #[async_trait::async_trait]
    impl SplitDrain for NoopDrain {
        async fn drain(
            &self,
            _start: &[u8],
            _end: &[u8],
            _timeout: Duration,
        ) -> std::result::Result<(), String> {
            Ok(())
        }
    }

    fn donor_range_migrator(node_id: NodeID) -> RangeImportMigrator {
        let source: Arc<RegionMvccResolver> = Arc::new(|_| None);
        let targets: Arc<RegionImportTargetResolver> = Arc::new(|_| None);
        let ranges: Arc<RegionRangeResolver> = Arc::new(|region_id| match region_id {
            1 => Some((b"".to_vec(), b"m".to_vec())),
            2 => Some((b"m".to_vec(), Vec::new())),
            _ => None,
        });
        let source_raft: Arc<RegionRaftResolver> = Arc::new(|_| None);
        RangeImportMigrator::new(
            source,
            targets,
            ranges,
            source_raft,
            Arc::new(NoopDrain),
            Arc::new(|_| Ok(None)),
        )
        .with_node_id(node_id)
        .with_donor_fence_wait(Duration::from_millis(200))
    }

    #[tokio::test]
    async fn test_merge_donor_range_matches_the_removed_region() {
        let plan = plan_merge(
            &table(vec![region(1, b"", b"m"), region(2, b"m", b"")]),
            1,
            2,
        )
        .unwrap();
        let migrator = donor_range_migrator(1);
        let (donor, start, end) = migrator.merge_donor_range(&plan).expect("donor range");
        assert_eq!(donor, 2);
        assert_eq!(start, b"m");
        assert!(end.is_empty());

        let split = plan_split(&table(vec![region(1, b"", b"")]), 1, b"m", 2).unwrap();
        assert!(
            migrator.merge_donor_range(&split).is_err(),
            "split plans have no donor"
        );
        assert!(
            !migrator
                .arm_donor_write_fence(7, &split)
                .await
                .expect("non-merge is a no-op"),
            "only merge plans have a donor fence"
        );
    }

    /// donor 不在本节点（或本节点不是它的 leader）时不提案，由吸收方等别处。
    #[tokio::test]
    async fn test_merge_donor_fence_is_not_armed_without_the_local_runtime() {
        let plan = plan_merge(
            &table(vec![region(1, b"", b"m"), region(2, b"m", b"")]),
            1,
            2,
        )
        .unwrap();
        let migrator = donor_range_migrator(1);
        let armed = migrator
            .arm_donor_write_fence(9, &plan)
            .await
            .expect("no local donor is not an error");
        assert!(!armed);
        // 释放同理：本节点不拥有该 fence，intent 必须留给 donor leader 收口。
        assert!(
            !migrator
                .release_write_fence(9, &plan)
                .await
                .expect("not owning the fence is not an error"),
            "only the donor leader owns the release"
        );

        let split = plan_split(&table(vec![region(1, b"", b"")]), 1, b"m", 2).unwrap();
        assert!(
            !migrator
                .release_write_fence(9, &split)
                .await
                .expect("not owning the source fence is not an error"),
            "only the source leader owns the split release"
        );
    }

    /// 吸收方的导出屏障：本地副本必须 apply 同一 `op_id` 的 fence。
    #[tokio::test]
    async fn test_merge_waits_for_the_local_donor_fence() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = RedbBackend::open(dir.path(), &coord_core::types::StorageConfig::default())
            .expect("open storage");
        let storage = MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs)
            .expect("mvcc storage");
        let migrator = donor_range_migrator(1);

        let err = migrator
            .wait_for_donor_fence(9, 2, &storage)
            .await
            .expect_err("a missing fence is not a barrier");
        assert!(err.contains("did not apply fence op 9"), "err: {err}");

        storage
            .fence_direct_writes(b"m", b"", 9, AppliedLogId::standalone(1))
            .expect("arm local fence");
        migrator
            .wait_for_donor_fence(9, 2, &storage)
            .await
            .expect("the same op id is the barrier");

        let err = migrator
            .wait_for_donor_fence(10, 2, &storage)
            .await
            .expect_err("another attempt's fence is not this barrier");
        assert!(err.contains("did not apply fence op 10"), "err: {err}");
    }

    struct FixedDrain(std::result::Result<(), String>);

    #[async_trait::async_trait]
    impl SplitDrain for FixedDrain {
        async fn drain(
            &self,
            _start: &[u8],
            _end: &[u8],
            _timeout: Duration,
        ) -> std::result::Result<(), String> {
            self.0.clone()
        }
    }

    /// 按节点返回 drain 结果；不在表里的节点模拟连接失败（不可达）。
    #[derive(Default)]
    struct FakeTransport {
        calls: std::sync::Mutex<Vec<(NodeID, Vec<u8>, Vec<u8>, Duration)>>,
        outcomes: std::collections::BTreeMap<NodeID, std::result::Result<(), String>>,
    }

    impl FakeTransport {
        fn calls(&self) -> Vec<(NodeID, Vec<u8>, Vec<u8>)> {
            self.calls
                .lock()
                .expect("transport calls lock")
                .iter()
                .map(|(node_id, start, end, _)| (*node_id, start.clone(), end.clone()))
                .collect()
        }
    }

    #[async_trait::async_trait]
    impl DrainTransport for FakeTransport {
        async fn drain_peer(
            &self,
            node_id: NodeID,
            start: &[u8],
            end: &[u8],
            timeout: Duration,
        ) -> std::result::Result<DrainRpcOutcome, String> {
            self.calls.lock().expect("transport calls lock").push((
                node_id,
                start.to_vec(),
                end.to_vec(),
                timeout,
            ));
            match self.outcomes.get(&node_id) {
                Some(Ok(())) => Ok(DrainRpcOutcome::Drained),
                Some(Err(reason)) => Ok(DrainRpcOutcome::Blocked(reason.clone())),
                None => Err(format!("unreachable node {node_id}")),
            }
        }
    }

    fn cluster_drain(
        local: std::result::Result<(), String>,
        peers: Vec<NodeID>,
        outcomes: &[(NodeID, std::result::Result<(), String>)],
    ) -> (ClusterSplitDrain, Arc<FakeTransport>) {
        let transport = Arc::new(FakeTransport {
            calls: std::sync::Mutex::new(Vec::new()),
            outcomes: outcomes.iter().cloned().collect(),
        });
        let drain = ClusterSplitDrain::new(
            Arc::new(FixedDrain(local)),
            1,
            Arc::new(move || peers.clone()),
            Arc::clone(&transport) as Arc<dyn DrainTransport>,
        );
        (drain, transport)
    }

    #[tokio::test]
    async fn test_cluster_drain_waits_on_every_peer_including_learners() {
        let (drain, transport) = cluster_drain(Ok(()), vec![1, 2, 3], &[(2, Ok(())), (3, Ok(()))]);
        drain
            .drain(b"m", b"z", Duration::from_millis(250))
            .await
            .expect("all peers drained");
        assert_eq!(
            transport.calls(),
            vec![
                (2, b"m".to_vec(), b"z".to_vec()),
                (3, b"m".to_vec(), b"z".to_vec()),
            ],
            "self is drained locally; every other member is contacted"
        );
    }

    #[tokio::test]
    async fn test_cluster_drain_fails_when_a_reachable_peer_times_out() {
        let (drain, _transport) = cluster_drain(
            Ok(()),
            vec![1, 2, 3],
            &[(2, Ok(())), (3, Err("1 active transaction(s)".to_string()))],
        );
        let err = drain
            .drain(b"m", b"z", Duration::from_millis(50))
            .await
            .expect_err("a reachable blocked peer must fail the drain");
        assert!(err.contains("node 3 drain failed"), "err: {err}");
        assert!(err.contains("1 active transaction(s)"), "err: {err}");
    }

    #[tokio::test]
    async fn test_cluster_drain_skips_unreachable_peers() {
        // 节点 3 不在 outcomes 表 → 传输层不可达；它的会话按失效处理。
        let (drain, transport) = cluster_drain(Ok(()), vec![1, 2, 3], &[(2, Ok(()))]);
        drain
            .drain(b"m", b"z", Duration::from_millis(50))
            .await
            .expect("an unreachable node must not block migration");
        assert_eq!(transport.calls().len(), 2);
    }

    #[tokio::test]
    async fn test_cluster_drain_fails_when_local_sessions_remain() {
        let (drain, _transport) = cluster_drain(
            Err("2 active transaction(s)".to_string()),
            vec![1, 2],
            &[(2, Ok(()))],
        );
        let err = drain
            .drain(b"m", b"z", Duration::from_millis(50))
            .await
            .expect_err("the local session table must gate the drain");
        assert!(err.contains("local drain failed"), "err: {err}");
        assert!(err.contains("2 active transaction(s)"), "err: {err}");
    }

    #[tokio::test]
    async fn test_cluster_drain_single_node_uses_local_only() {
        let (drain, transport) = cluster_drain(Ok(()), vec![1], &[]);
        drain
            .drain(b"", b"", Duration::from_millis(50))
            .await
            .expect("single-node drain is local only");
        assert!(transport.calls().is_empty());
    }

    /// 本地侧接真实会话表：相交会话未收尾时 cluster drain 必须超时失败。
    #[tokio::test]
    async fn test_cluster_drain_gates_on_the_real_local_session_table() {
        let sessions = Arc::new(crate::kvt::txn::TxnSessions::new());
        let txn = sessions.begin(10, Duration::from_secs(5));
        sessions
            .record_read(&txn, crate::kvt::txn::ReadIntent::Point(b"m"), 4096)
            .expect("record an overlapping read");
        let transport = Arc::new(FakeTransport::default());
        let drain = ClusterSplitDrain::new(
            Arc::clone(&sessions) as Arc<dyn SplitDrain>,
            1,
            Arc::new(|| vec![1]),
            Arc::clone(&transport) as Arc<dyn DrainTransport>,
        );

        let err = drain
            .drain(b"m", b"z", Duration::from_millis(20))
            .await
            .expect_err("an active transaction must block the drain");
        assert!(err.contains("local drain failed"), "err: {err}");

        sessions.abort(&txn);
        drain
            .drain(b"m", b"z", Duration::from_millis(50))
            .await
            .expect("aborting the transaction releases the drain");
    }
}
