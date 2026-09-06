// Compaction 调度与管理
//
// 设计决策：
// - **节点一致的压缩修订号**：changelog/tombstone 删除不再由各节点本地自决，
//   而是经 raft 下发 `Command::Compact{revision}`（apply 内分片删除、确定性）；
// - **文件级空间回收**：redb `compact()` 需要独占引用，由本管理器定时在维护
//   窗口内执行，属节点本地优化，不影响状态机一致性；
// - 定时任务仅在 leader（或单节点）上计算保留窗口并提案；follower 经 apply 收敛。

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;

use coord_core::storage::StorageBackend;

use super::mvcc::MvccStorage;
use crate::metrics::Metrics;

// ──── CompactionConfig ────

/// Compaction 配置
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Changelog 保留的 Revision 数量（默认 100,000）
    pub changelog_retention_revisions: u64,
    /// Raft Log 保留的 Entry 数量（默认 1,000）
    pub raft_log_retention_entries: u64,
    /// KV Tombstone 保留的 Revision 数量（默认 100,000）
    pub tombstone_retention_revisions: u64,
    /// 定时 Compaction 间隔（默认 1 小时）
    pub interval: Duration,
    /// 是否启用自动 Compaction
    pub auto_compact: bool,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            changelog_retention_revisions: 100_000,
            raft_log_retention_entries: 1_000,
            tombstone_retention_revisions: 100_000,
            interval: Duration::from_secs(3600),
            auto_compact: true,
        }
    }
}

/// R-RFT-19：文件级 compact 前的空闲窗口（无写入静默期）。
/// 等待该时长内无写入再拿独占写锁，规避「compact 期间新读写全部阻塞」。
const COMPACT_IDLE_WINDOW: Duration = Duration::from_millis(500);

/// R-RFT-19：空闲窗口最长等待时间；超时仍执行 compact（空间回收优先）。
const COMPACT_IDLE_MAX_WAIT: Duration = Duration::from_secs(30);

// ──── CompactProposer ────

/// Compact 提案器（压缩修订号经 raft 下发，节点一致）
///
/// 由持有 Raft 句柄的层实现（`coord-server/src/server/mod.rs` 对 `CoordNode` 实现）。
#[async_trait::async_trait]
pub trait CompactProposer: Send + Sync {
    /// 本节点当前是否可提案（raft 模式仅 leader；单节点模式恒 true）。
    async fn can_propose(&self) -> bool;

    /// 提案压缩到 `revision`，返回实际生效的 compacted revision。
    async fn propose(&self, revision: u64) -> std::result::Result<u64, String>;
}

// ──── CompactionManager ────

/// Compaction 管理器
///
/// 后台任务职责：
/// 1. 自动压缩（`auto_compact=true`）：仅 leader 提案 `revision = current - retention`
///    （经 raft，三节点一致应用；单节点直接 apply）；
/// 2. 文件级 compact：定时调用 redb `compact()` 回收磁盘空间（维护窗口，节点本地）。
pub struct CompactionManager<B: StorageBackend> {
    #[allow(dead_code)]
    storage: Arc<MvccStorage<B>>,
    config: CompactionConfig,
    /// 发送触发指令的通道（内部持有 sender，外部通过 handle 发送）
    trigger_tx: mpsc::UnboundedSender<CompactionTrigger>,
}

/// Compaction 触发指令
enum CompactionTrigger {
    /// 执行完整 Compaction（提案压缩 + 文件级 compact）
    Full,
    /// 停止后台任务
    Shutdown,
}

impl<B: StorageBackend + Clone + 'static> CompactionManager<B> {
    /// 创建并启动 Compaction 后台任务
    ///
    /// `metrics`：可选指标注册表（R-OBS-10：文件级 compact 回收字节计数）。
    pub fn start(
        storage: Arc<MvccStorage<B>>,
        config: CompactionConfig,
        proposer: Option<Arc<dyn CompactProposer>>,
        metrics: Option<Arc<Metrics>>,
    ) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<CompactionTrigger>();

        let storage_clone = Arc::clone(&storage);
        let config_clone = config.clone();
        let interval = config.interval;
        let metrics_clone = metrics.clone();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        Self::run_cycle(
                            &storage_clone,
                            &config_clone,
                            proposer.as_deref(),
                            metrics_clone.as_ref(),
                        )
                        .await;
                    }
                    msg = rx.recv() => {
                        match msg {
                            Some(CompactionTrigger::Full) => {
                                tracing::info!("Manual compaction triggered");
                                Self::run_cycle(
                                    &storage_clone,
                                    &config_clone,
                                    proposer.as_deref(),
                                    metrics_clone.as_ref(),
                                )
                                .await;
                            }
                            Some(CompactionTrigger::Shutdown) => {
                                tracing::info!("Compaction manager shutting down");
                                break;
                            }
                            None => break,
                        }
                    }
                }
            }
        });

        Self {
            storage,
            config,
            trigger_tx: tx,
        }
    }

    /// 单轮 Compaction：自动压缩（leader 提案）+ 文件级 compact
    async fn run_cycle(
        storage: &MvccStorage<B>,
        config: &CompactionConfig,
        proposer: Option<&dyn CompactProposer>,
        metrics: Option<&Arc<Metrics>>,
    ) {
        // 1. 自动压缩：仅 leader 提案（节点一致），单节点模式直接 apply
        if config.auto_compact {
            if let Some(p) = proposer {
                if p.can_propose().await {
                    let current = storage.current_revision();
                    let cutoff = current.saturating_sub(config.changelog_retention_revisions);
                    if cutoff > 0 {
                        match storage.compacted_revision() {
                            Ok(prev) if cutoff > prev => {
                                if let Err(e) = p.propose(cutoff).await {
                                    tracing::warn!(
                                        "Auto compaction proposal failed (revision={cutoff}): {e}"
                                    );
                                }
                            }
                            Ok(_) => {}
                            Err(e) => {
                                tracing::warn!("Read compacted revision failed: {e}");
                            }
                        }
                    }
                }
            }
        }

        // 2. 文件级 compact（空间回收）：redb 需要独占引用，放 spawn_blocking
        //    执行，避免阻塞异步运行时。R-RFT-19：等待空闲窗口（无写入静默期）
        //    再执行，将维护压缩与在线读写错开；超时仍执行（空间回收优先）。
        // R-OBS-10：记录回收字节（compact 前后磁盘大小差）
        let size_before = storage.backend().disk_size_bytes().unwrap_or(0);
        let backend = storage.backend().clone();
        let _ = tokio::task::spawn_blocking(move || {
            if let Err(e) =
                backend.compact_with_idle_window(COMPACT_IDLE_WINDOW, COMPACT_IDLE_MAX_WAIT)
            {
                tracing::warn!("File-level compact failed: {e}");
            }
        })
        .await;
        let size_after = storage.backend().disk_size_bytes().unwrap_or(size_before);
        if let Some(metrics) = metrics {
            if size_before > size_after {
                metrics.add_compact_reclaimed_bytes(size_before - size_after);
            }
        }
    }

    /// 手动触发完整 Compaction
    pub fn trigger_compact(&self) {
        let _ = self.trigger_tx.send(CompactionTrigger::Full);
    }

    /// 停止 Compaction 后台任务
    pub fn shutdown(&self) {
        let _ = self.trigger_tx.send(CompactionTrigger::Shutdown);
    }

    /// 获取配置的引用
    pub fn config(&self) -> &CompactionConfig {
        &self.config
    }
}

impl<B: StorageBackend> Drop for CompactionManager<B> {
    fn drop(&mut self) {
        let _ = self.trigger_tx.send(CompactionTrigger::Shutdown);
    }
}

// ──── 测试 ────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::mvcc::AppliedLogId;
    use crate::storage::redb_backend::RedbBackend;
    use coord_core::types::StorageConfig;
    use std::time::Duration;
    use tempfile::TempDir;

    fn setup_storage() -> (TempDir, Arc<MvccStorage<RedbBackend>>) {
        let tmp = TempDir::new().unwrap();
        let config = StorageConfig::default();
        let backend = RedbBackend::open(tmp.path(), &config).unwrap();
        let storage = Arc::new(MvccStorage::new(backend).unwrap());
        (tmp, storage)
    }

    /// 单节点提案器（无 raft）：propose 直接本地 apply（与 CoordNode 的
    /// standalone 分支同源，`compact_impl` 的 else 路径）。
    struct StandaloneProposer {
        storage: Arc<MvccStorage<RedbBackend>>,
    }

    #[async_trait::async_trait]
    impl CompactProposer for StandaloneProposer {
        async fn can_propose(&self) -> bool {
            true
        }

        async fn propose(&self, revision: u64) -> std::result::Result<u64, String> {
            let new_rev = self.storage.current_revision().saturating_add(1);
            self.storage
                .apply_compact(revision, AppliedLogId::standalone(new_rev))
                .map_err(|e| e.to_string())?;
            Ok(revision.min(new_rev))
        }
    }

    /// 单节点路径的完整压缩（propose → apply），返回 compacted revision。
    fn apply_compact_standalone(storage: &MvccStorage<RedbBackend>, revision: u64) -> u64 {
        let new_rev = storage.current_revision().saturating_add(1);
        storage
            .apply_compact(revision, AppliedLogId::standalone(new_rev))
            .unwrap();
        revision.min(new_rev)
    }

    #[test]
    fn test_config_defaults() {
        let config = CompactionConfig::default();
        assert_eq!(config.changelog_retention_revisions, 100_000);
        assert_eq!(config.raft_log_retention_entries, 1_000);
        assert_eq!(config.tombstone_retention_revisions, 100_000);
        assert!(config.auto_compact);
    }

    #[test]
    fn test_compact_below_retention_is_noop() {
        let (_tmp, storage) = setup_storage();
        for i in 0..10u32 {
            storage
                .put(format!("/key{}", i).as_bytes(), b"val", None)
                .unwrap();
        }
        // 保留窗口 100_000 > current(10)：cutoff=0，不提案 → 无副作用
        let outcome = storage
            .apply_compact(0, AppliedLogId::standalone(10))
            .unwrap();
        assert_eq!(outcome.deleted_changelog, 0);
        assert_eq!(storage.compacted_revision().unwrap(), 0);
    }

    #[test]
    fn test_compact_applies_retention_cutoff() {
        let (_tmp, storage) = setup_storage();
        for i in 0..20u32 {
            storage
                .put(format!("/key{}", i).as_bytes(), b"val", None)
                .unwrap();
        }
        let compacted = apply_compact_standalone(&storage, 10);
        assert_eq!(compacted, 10);
        assert_eq!(storage.compacted_revision().unwrap(), 10);
        assert!(!storage.changelog_contains_revision(9).unwrap());
        assert!(storage.changelog_contains_revision(10).unwrap());
    }

    #[tokio::test]
    async fn test_compaction_manager_start_and_shutdown() {
        let (_tmp, storage) = setup_storage();

        let config = CompactionConfig {
            interval: Duration::from_secs(3600),
            ..Default::default()
        };

        let mgr = CompactionManager::start(
            Arc::clone(&storage),
            config,
            Some(Arc::new(StandaloneProposer {
                storage: Arc::clone(&storage),
            })),
            None,
        );
        mgr.shutdown();
    }

    #[tokio::test]
    async fn test_auto_cycle_proposes_and_compacts() {
        let (_tmp, storage) = setup_storage();
        for i in 0..60u32 {
            storage
                .put(format!("/key{:03}", i).as_bytes(), b"val", None)
                .unwrap();
        }
        let current = storage.current_revision();
        assert!(current >= 60);

        let config = CompactionConfig {
            changelog_retention_revisions: 50,
            interval: Duration::from_secs(3600),
            ..Default::default()
        };
        let proposer = Arc::new(StandaloneProposer {
            storage: Arc::clone(&storage),
        });
        CompactionManager::<RedbBackend>::run_cycle(
            &storage,
            &config,
            Some(proposer.as_ref()),
            None,
        )
        .await;

        // cutoff = current - 50 > 0 → 提案后 compacted_revision 推进
        let compacted = storage.compacted_revision().unwrap();
        assert!(
            compacted > 0,
            "auto cycle should have proposed compaction, compacted={compacted}"
        );
        // 保留窗口内的条目仍在
        assert!(storage.changelog_contains_revision(current).unwrap());
    }

    #[tokio::test]
    async fn test_auto_cycle_skips_when_behind_retention() {
        let (_tmp, storage) = setup_storage();
        for i in 0..10u32 {
            storage
                .put(format!("/few{}", i).as_bytes(), b"val", None)
                .unwrap();
        }
        // retention 100_000 > current → cutoff=0 → 不提案
        let config = CompactionConfig {
            changelog_retention_revisions: 100_000,
            interval: Duration::from_secs(3600),
            ..Default::default()
        };
        let proposer = Arc::new(StandaloneProposer {
            storage: Arc::clone(&storage),
        });
        CompactionManager::<RedbBackend>::run_cycle(
            &storage,
            &config,
            Some(proposer.as_ref()),
            None,
        )
        .await;
        assert_eq!(storage.compacted_revision().unwrap(), 0);
    }

    #[tokio::test]
    async fn test_manual_trigger_via_handle() {
        let (_tmp, storage) = setup_storage();
        for i in 0..80u32 {
            storage
                .put(format!("/manual{}", i).as_bytes(), b"v", None)
                .unwrap();
        }

        let config = CompactionConfig {
            changelog_retention_revisions: 50,
            interval: Duration::from_secs(3600),
            ..Default::default()
        };
        let mgr = CompactionManager::start(
            Arc::clone(&storage),
            config,
            Some(Arc::new(StandaloneProposer {
                storage: Arc::clone(&storage),
            })),
            None,
        );
        mgr.trigger_compact();
        // 等待后台任务消费触发指令
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while std::time::Instant::now() < deadline && storage.compacted_revision().unwrap() == 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        mgr.shutdown();
        assert!(
            storage.compacted_revision().unwrap() > 0,
            "manual trigger should run a full cycle"
        );
    }
}
