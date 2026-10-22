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

use coord_core::types::RegionId;

use crate::pd::executor::SplitMergeMigrator;
use crate::raft::region::RegionManager;
use crate::raft::region_lifecycle::RegionLifecycle;
use crate::raft::region_runtime::CoordRangeImportTarget;
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

/// 每页导出的行数上界（0 = 一页取尽）。
const DEFAULT_PAGE_SIZE: usize = 1024;

/// 等区间内活跃事务收尾的默认上界；超时整次迁移失败（intent 保留重试）。
const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

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
    drain: Arc<dyn SplitDrain>,
    page_size: usize,
    drain_timeout: Duration,
    target_wait: Duration,
}

impl RangeImportMigrator {
    pub fn new(
        source: Arc<RegionMvccResolver>,
        targets: Arc<RegionImportTargetResolver>,
        ranges: Arc<RegionRangeResolver>,
        drain: Arc<dyn SplitDrain>,
    ) -> Self {
        Self {
            source,
            targets,
            ranges,
            drain,
            page_size: DEFAULT_PAGE_SIZE,
            drain_timeout: DEFAULT_DRAIN_TIMEOUT,
            target_wait: DEFAULT_TARGET_WAIT,
        }
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
        loop {
            let page = source
                .export_range_page(start, end, after.as_deref(), self.page_size, watermark)
                .map_err(|e| format!("export range page: {e}"))?;
            let pinned = *watermark.get_or_insert(page.revision);
            if seal_prior_history && after.is_none() {
                // 快照 revision 已包含 == cursor 的决策。源游标落后说明有决策
                // 尚未 apply 却已被水位越过；目标从 watermark 续跑就会永久跳过
                // 它们（apply 在 fence 前停住）。
                let cursor = source
                    .last_applied_decision_ts()
                    .map_err(|e| format!("read split source decision cursor: {e}"))?;
                if cursor != pinned {
                    return Err(format!(
                        "split source decision cursor {cursor} is behind the exported revision \
                         {pinned}; refusing to seed the target from an unapplied watermark"
                    ));
                }
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
}

/// 生产接线：源 = `RegionManager` 本节点运行时 MVCC；目标 = `RegionLifecycle`
/// 的 pending/已接管运行时（搬迁经目标 Region 自己的 raft）。
///
/// `drain` 由接线层传入（`main.rs` = 本节点 `TxnSessions` 句柄）。只覆盖本节点
/// 协调的会话；跨协调节点会话的 drain 属 #167 连续性验收。
pub fn range_import_migrator(
    manager: &Arc<RegionManager>,
    lifecycle: &Arc<RegionLifecycle>,
    drain: Arc<dyn SplitDrain>,
) -> Arc<dyn SplitMergeMigrator> {
    let manager = Arc::clone(manager);
    let ranges_source = Arc::clone(&manager);
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

    Arc::new(RangeImportMigrator::new(source, targets, ranges, drain))
}
