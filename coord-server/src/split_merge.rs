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

/// Region → 经该 Region raft 导入一页的目标端口。
pub type RegionImportTargetResolver =
    dyn Fn(RegionId) -> Option<Arc<dyn RangeImportTarget>> + Send + Sync;

/// 目标 Region 的分页导入端口。
///
/// - `watermark` = 源快照 revision，必须逐页恒等（导入行的 `mod_revision`
///   不得越过它）；
/// - `seal_prior_history` 只在最后一页有意义：split 的全新目标据此把
///   「split 前历史不可用」的压缩水位钉到 `watermark`；merge 的接收方传
///   `false`，保留自己的历史；
/// - `done` 只在最后一页为真：实现方据此把目标 revision 抬到 `watermark`；
/// - 返回实际写入行数：重放已导入的页返回 0 而不是错误（幂等契约）。
#[async_trait::async_trait]
pub trait RangeImportTarget: Send + Sync {
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
    drain: Arc<dyn SplitDrain>,
    page_size: usize,
    drain_timeout: Duration,
    target_wait: Duration,
}

impl RangeImportMigrator {
    pub fn new(
        source: Arc<RegionMvccResolver>,
        targets: Arc<RegionImportTargetResolver>,
        drain: Arc<dyn SplitDrain>,
    ) -> Self {
        Self {
            source,
            targets,
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
                    "split target region {target_id} raft is not assembled on this node \
                     (pending region pre-creation has not caught up)"
                ));
            }
            tokio::time::sleep(TARGET_RESOLVE_POLL).await;
        }
    }
}

#[async_trait::async_trait]
impl SplitMergeMigrator for RangeImportMigrator {
    async fn migrate(&self, plan: &SplitMergePlan) -> std::result::Result<(), String> {
        if plan.kind != SplitMergeKind::Split {
            // merge 接收方已有自己的历史与水位，不能复用「新 Region 抬压缩
            // 水位」的 split 语义；在其专属实现交付前拒绝切表（#164）。
            return Err(
                "merge data migration is not implemented (#164); refusing to cut over".to_string(),
            );
        }
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

        let mut after: Option<Vec<u8>> = None;
        // 首页取到的 revision 钉死整次搬迁：后续页用 expected_revision 校验源
        // 没有前进，避免把两代快照拼进同一个新 Region。
        let mut watermark: Option<u64> = None;
        loop {
            let page = source
                .export_range_page(start, end, after.as_deref(), self.page_size, watermark)
                .map_err(|e| format!("export split range page: {e}"))?;
            let pinned = *watermark.get_or_insert(page.revision);
            let done = page.next_after.is_none();
            // 空白源 Region 的 revision 可以是 0，但导入水位必须 > 0：
            // 用 1 让目标水位仍可被未来 commit_ts 推开。
            target
                .import_range(start, end, page.rows, pinned.max(1), true, done)
                .await?;
            if done {
                break;
            }
            after = page.next_after;
        }

        tracing::info!(
            region_id = target_id,
            watermark = watermark.unwrap_or(0),
            "split range import complete"
        );
        Ok(())
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
    let source: Arc<RegionMvccResolver> = Arc::new(move |region_id| {
        manager
            .runtime(region_id)
            .map(|runtime| Arc::clone(&runtime.mvcc))
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

    Arc::new(RangeImportMigrator::new(source, targets, drain))
}
