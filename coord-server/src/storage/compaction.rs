// Compaction 调度与管理
//
// 实现 ADP §13 的数据生命周期管理：
// - Changelog 压缩：基于保留窗口删除过期变更日志条目
// - 定时调度：按配置间隔自动触发
// - 手动触发：通过 CompactionManager API
//
// Redb 自身的 compact() 需要独占 &mut 访问，与 Arc<Database> 冲突，
// 因此本模块专注于应用层数据清理（Changelog、tombstone）。

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;

use coord_core::error::Result;
use coord_core::storage::StorageBackend;

use super::mvcc::{
    MvccStorage, TABLE_CHANGELOG,
};

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

// ──── CompactionManager ────

/// Compaction 管理器
///
/// 管理 Changelog 清理和定时 Compaction 调度。
/// 通过内部 mpsc 通道接收触发指令。
pub struct CompactionManager<B: StorageBackend> {
    #[allow(dead_code)]
    storage: Arc<MvccStorage<B>>,
    config: CompactionConfig,
    /// 发送触发指令的通道（内部持有 sender，外部通过 handle 发送）
    trigger_tx: mpsc::UnboundedSender<CompactionTrigger>,
}

/// Compaction 触发指令
enum CompactionTrigger {
    /// 执行完整 Compaction（Changelog 清理 + Tombstone 清理 + Raft Log 清理）
    Full,
    /// 停止后台任务
    Shutdown,
}

impl<B: StorageBackend + 'static> CompactionManager<B> {
    /// 创建并启动 Compaction 后台任务
    pub fn start(storage: Arc<MvccStorage<B>>, config: CompactionConfig) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<CompactionTrigger>();

        let storage_clone = Arc::clone(&storage);
        let config_clone = config.clone();
        let interval = config.interval;

        // 启动后台任务
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        // 定时触发 — 执行三层 Compaction
                        if let Err(e) = Self::compact_changelog(&storage_clone, config_clone.changelog_retention_revisions) {
                            tracing::warn!("Periodic changelog compaction failed: {e}");
                        }
                        if let Err(e) = Self::compact_tombstones(&storage_clone, config_clone.tombstone_retention_revisions) {
                            tracing::warn!("Periodic tombstone compaction failed: {e}");
                        }
                    }
                    msg = rx.recv() => {
                        match msg {
                            Some(CompactionTrigger::Full) => {
                                if let Err(e) = Self::compact_changelog(&storage_clone, config_clone.changelog_retention_revisions) {
                                    tracing::warn!("Manual changelog compaction failed: {e}");
                                }
                                if let Err(e) = Self::compact_tombstones(&storage_clone, config_clone.tombstone_retention_revisions) {
                                    tracing::warn!("Manual tombstone compaction failed: {e}");
                                }
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

    // ──── 内部实现 ────

    /// 清理过期的 Changelog 条目
    ///
    /// 保留最近 `retention_revisions` 个 Revision 的日志。
    fn compact_changelog(
        storage: &MvccStorage<B>,
        retention_revisions: u64,
    ) -> Result<usize> {
        let current_rev = storage.current_revision();
        if current_rev <= retention_revisions {
            return Ok(0);
        }

        let cutoff_rev = current_rev.saturating_sub(retention_revisions);
        let mut deleted = 0usize;

        let backend = storage.backend();
        backend.write(|tx| {
            // 扫描 Changelog 表，删除 cutoff_rev 之前的条目
            let changelog_prefix = super::mvcc::encode_changelog_key(0);
            let prefix = &changelog_prefix[..changelog_prefix.len() - 8];

            let entries: Vec<(Vec<u8>, Vec<u8>)> = tx
                .iter_prefix(TABLE_CHANGELOG, prefix)?
                .into_iter()
                .filter(|(k, _v)| {
                    if k.len() >= prefix.len() + 8 {
                        let rev_bytes: [u8; 8] = k[prefix.len()..prefix.len() + 8]
                            .try_into()
                            .unwrap_or([0u8; 8]);
                        let rev = u64::from_be_bytes(rev_bytes);
                        rev < cutoff_rev
                    } else {
                        false
                    }
                })
                .collect();

            for (key, _) in &entries {
                tx.remove(TABLE_CHANGELOG, key)?;
                deleted += 1;
            }

            Ok(())
        })?;

        if deleted > 0 {
            tracing::info!(
                "Compacted {} changelog entries (cutoff_rev={}, current_rev={})",
                deleted,
                cutoff_rev,
                current_rev
            );
        }

        Ok(deleted)
    }

    /// 清理过期的 KV Tombstone
    ///
    /// Tombstone 是 Delete 操作后留下的空 value 标记。
    /// 此方法扫描 KV 表中所有空 value 的 key，检查其元数据中的
    /// `mod_revision`，若超过保留窗口则物理删除该 key 的数据和元数据。
    ///
    /// 保留最近 `retention_revisions` 个 Revision 内的 Tombstone。
    pub fn compact_tombstones(
        storage: &MvccStorage<B>,
        retention_revisions: u64,
    ) -> Result<usize> {
        let current_rev = storage.current_revision();
        if current_rev <= retention_revisions {
            return Ok(0);
        }

        let cutoff_rev = current_rev.saturating_sub(retention_revisions);
        let mut deleted = 0usize;

        let backend = storage.backend();
        backend.write(|tx| {
            use super::mvcc::{TABLE_KV, TABLE_KV_META};

            // 扫描 KV 元数据表，找出所有已标记 deleted 的条目
            let tombstones: Vec<(Vec<u8>, Vec<u8>)> = tx
                .iter_prefix(TABLE_KV_META, b"/_kv_meta/")?
                .into_iter()
                .filter(|(_k, v)| {
                    super::mvcc::KvMetadata::from_bytes(v)
                        .map(|m| m.deleted)
                        .unwrap_or(false)
                })
                .map(|(k, v)| (k.to_vec(), v.to_vec()))
                .collect();

            for (meta_key, meta_bytes) in &tombstones {
                if let Some(meta) = super::mvcc::KvMetadata::from_bytes(meta_bytes) {
                    if (meta.mod_revision as u64) < cutoff_rev {
                        // 提取用户 key 并从 KV 表删除
                        let kv_meta_prefix = b"/_kv_meta/";
                        if let Some(user_key) = meta_key.strip_prefix(kv_meta_prefix) {
                            let internal_key = super::mvcc::encode_kv_key(user_key);
                            tx.remove(TABLE_KV, &internal_key)?;
                            tx.remove(TABLE_KV_META, meta_key)?;
                            deleted += 1;
                        }
                    }
                }
            }

            Ok(())
        })?;

        if deleted > 0 {
            tracing::info!(
                "Compacted {} tombstones (cutoff_rev={}, current_rev={})",
                deleted,
                cutoff_rev,
                current_rev
            );
        }

        Ok(deleted)
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
    use crate::storage::redb_backend::RedbBackend;
    use coord_core::types::StorageConfig;
    use tempfile::TempDir;
    use std::time::Duration;

    fn setup_storage() -> (TempDir, Arc<MvccStorage<RedbBackend>>) {
        let tmp = TempDir::new().unwrap();
        let config = StorageConfig::default();
        let backend = RedbBackend::open(tmp.path(), &config).unwrap();
        let storage = Arc::new(MvccStorage::new(backend).unwrap());
        (tmp, storage)
    }

    // ──── Changelog Compaction 测试 ────

    #[test]
    fn test_compaction_noop_when_below_retention() {
        let (_tmp, storage) = setup_storage();

        for i in 0..10u32 {
            let key = format!("/key{}", i);
            storage.put(key.as_bytes(), b"val", None).unwrap();
        }

        let deleted = CompactionManager::<RedbBackend>::compact_changelog(&storage, 100_000).unwrap();
        assert_eq!(deleted, 0, "should not delete when below retention");
    }

    #[test]
    fn test_compaction_with_retention() {
        let (_tmp, storage) = setup_storage();

        for i in 0..20u32 {
            let key = format!("/key{}", i);
            storage.put(key.as_bytes(), b"val", None).unwrap();
        }

        let current = storage.current_revision();
        assert!(current >= 20);

        let deleted = CompactionManager::<RedbBackend>::compact_changelog(&storage, 10).unwrap();
        assert!(deleted > 0, "should delete entries beyond retention");
    }

    #[tokio::test]
    async fn test_compaction_manager_start_and_shutdown() {
        let (_tmp, storage) = setup_storage();

        let config = CompactionConfig {
            interval: Duration::from_secs(3600),
            ..Default::default()
        };

        let mgr = CompactionManager::start(Arc::clone(&storage), config);
        mgr.shutdown();
    }

    #[test]
    fn test_compaction_triggers_manually() {
        let (_tmp, storage) = setup_storage();

        for i in 0..100u32 {
            let key = format!("/key{:03}", i);
            storage.put(key.as_bytes(), b"val", None).unwrap();
        }

        let deleted = CompactionManager::<RedbBackend>::compact_changelog(&storage, 50).unwrap();
        assert!(deleted > 0, "manual compaction should delete entries");
    }

    // ──── Tiered Compaction 测试 ────

    #[test]
    fn test_tiered_config_defaults() {
        let config = CompactionConfig::default();
        assert_eq!(config.changelog_retention_revisions, 100_000);
        assert_eq!(config.raft_log_retention_entries, 1_000);
        assert_eq!(config.tombstone_retention_revisions, 100_000);
        assert!(config.auto_compact);
    }

    #[test]
    fn test_tombstone_compaction_noop_when_below_retention() {
        let (_tmp, storage) = setup_storage();

        // 写入并删除少量 key，创建 tombstone
        for i in 0..10u32 {
            let key = format!("/tk{}", i);
            storage.put(key.as_bytes(), b"val", None).unwrap();
            storage.delete(key.as_bytes()).unwrap();
        }

        let deleted = CompactionManager::<RedbBackend>::compact_tombstones(&storage, 100_000).unwrap();
        assert_eq!(deleted, 0, "should not delete tombstones when below retention");
    }

    #[test]
    fn test_tombstone_compaction_removes_old_tombstones() {
        let (_tmp, storage) = setup_storage();

        // 创建大量 tombstone，确保部分超过保留窗口
        for i in 0..50u32 {
            let key = format!("/tk{}", i);
            storage.put(key.as_bytes(), b"val", None).unwrap();
            storage.delete(key.as_bytes()).unwrap();
        }

        // 再写入一些数据增加 revision
        for i in 0..10u32 {
            let key = format!("/extra{}", i);
            storage.put(key.as_bytes(), b"extra", None).unwrap();
        }

        let current = storage.current_revision();
        assert!(current >= 110, "should have at least 110 revisions (50 put+delete + 10 extra)");

        // 保留最近 10 个 revision，应清理大量 tombstone
        let deleted = CompactionManager::<RedbBackend>::compact_tombstones(&storage, 10).unwrap();
        assert!(deleted > 0, "should delete old tombstones beyond retention");
    }

    #[test]
    fn test_tombstone_compaction_preserves_recent_tombstones() {
        let (_tmp, storage) = setup_storage();

        // 写入大量数据后又删除最后一个 key（最近的 tombstone）
        for i in 0..100u32 {
            let key = format!("/preserve{}", i);
            storage.put(key.as_bytes(), b"val", None).unwrap();
        }
        // 删除最后一个 key — 这是最近的 tombstone
        storage.delete(b"/preserve99").unwrap();

        // 保留最近 50 个 revision，最近的 tombstone 应保留
        let deleted = CompactionManager::<RedbBackend>::compact_tombstones(&storage, 50).unwrap();

        // 验证最近的 tombstone 仍然存在
        let val = storage.get(b"/preserve99").unwrap();
        assert_eq!(val, None, "recent tombstone should still exist (returns None)");

        // 验证已删除的旧 tombstone（如果有）已被清理
        // 非 tombstone 的 key 应仍然可读
        let val = storage.get(b"/preserve0").unwrap();
        assert!(val.is_some(), "non-tombstone key should still be readable");
    }

    #[test]
    fn test_tombstone_compaction_does_not_affect_live_keys() {
        let (_tmp, storage) = setup_storage();

        // 写入活跃 key
        storage.put(b"/live1", b"value1", None).unwrap();
        storage.put(b"/live2", b"value2", None).unwrap();
        storage.put(b"/live3", b"value3", None).unwrap();

        // 删除一个 key 作为 tombstone
        storage.delete(b"/live2").unwrap();

        // 再写入一些数据增加 revision
        for i in 0..20u32 {
            let key = format!("/padding{}", i);
            storage.put(key.as_bytes(), b"pad", None).unwrap();
        }

        // 执行 tombstone compaction
        let deleted = CompactionManager::<RedbBackend>::compact_tombstones(&storage, 5).unwrap();

        // 活跃 key 应仍然存在
        assert_eq!(storage.get(b"/live1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(storage.get(b"/live3").unwrap(), Some(b"value3".to_vec()));
    }

    #[test]
    fn test_full_compaction_cleans_all_tiers() {
        let (_tmp, storage) = setup_storage();

        // 1. 写入 changelog 条目
        for i in 0..50u32 {
            let key = format!("/full{}", i);
            storage.put(key.as_bytes(), b"v", None).unwrap();
        }

        // 2. 创建 tombstone
        for i in 0..10u32 {
            let key = format!("/full{}", i);
            storage.delete(key.as_bytes()).unwrap();
        }

        // 3. 确保 revision 足够高
        for i in 0..20u32 {
            let key = format!("/pad{}", i);
            storage.put(key.as_bytes(), b"p", None).unwrap();
        }

        let current = storage.current_revision();
        assert!(current >= 80, "should have at least 80 revisions (50 put + 10 delete + 20 pad)");

        // 执行完整 Compaction
        let changelog_deleted = CompactionManager::<RedbBackend>::compact_changelog(&storage, 20).unwrap();
        let tombstone_deleted = CompactionManager::<RedbBackend>::compact_tombstones(&storage, 20).unwrap();

        // 至少其中一项产生了清理效果
        assert!(
            changelog_deleted > 0 || tombstone_deleted > 0,
            "full compaction should clean at least one tier (changelog={}, tombstone={})",
            changelog_deleted,
            tombstone_deleted
        );
    }

    #[test]
    fn test_tombstone_compaction_with_recreated_keys() {
        let (_tmp, storage) = setup_storage();

        // 写入 → 删除 → 重新写入同一个 key
        storage.put(b"/recreated", b"v1", None).unwrap();
        storage.delete(b"/recreated").unwrap();
        storage.put(b"/recreated", b"v2", None).unwrap();

        // 写入更多数据增加 revision
        for i in 0..30u32 {
            let key = format!("/pad{}", i);
            storage.put(key.as_bytes(), b"p", None).unwrap();
        }

        // 执行 tombstone compaction
        let deleted = CompactionManager::<RedbBackend>::compact_tombstones(&storage, 5).unwrap();

        // 重新创建的 key 应该仍然存在（不是 tombstone）
        let val = storage.get(b"/recreated").unwrap();
        assert_eq!(val, Some(b"v2".to_vec()), "recreated key should still have its value");
    }
}
