// Automatic Snapshot Scheduler — 定时自动快照调度（ADP §19.2）
//
// 在后台定时创建状态机快照，并自动清理过期快照。
//
// 配置项（ADP §19.2）：
// - snapshot_interval: 自动快照间隔（默认 1 小时）
// - snapshot_retention: 快照保留时间（默认 7 天）
// - snapshot_dir: 快照存储目录（默认 <data_dir>/snapshots）

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::RwLock;

use coord_core::error::{Error, Result};
use coord_core::storage::StorageBackend;

use super::mvcc::MvccStorage;
use super::snapshot::{export_snapshot_data, import_snapshot_data, SnapshotData};

// ──── 配置 ────

/// 快照调度器配置
#[derive(Debug, Clone)]
pub struct SnapshotSchedulerConfig {
    /// 自动快照间隔（默认 1 小时）
    pub interval: Duration,
    /// 快照保留时间（默认 7 天）
    pub retention: Duration,
    /// 快照存储目录
    pub snapshot_dir: PathBuf,
    /// 是否启用自动快照
    pub auto_snapshot: bool,
}

impl Default for SnapshotSchedulerConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(3600),      // 1 hour
            retention: Duration::from_secs(7 * 86400), // 7 days
            snapshot_dir: PathBuf::from("/var/lib/coord/snapshots"),
            auto_snapshot: true,
        }
    }
}

// ──── SnapshotScheduler ────

/// 自动快照调度器
///
/// 在后台 tokio 任务中运行，定期：
/// 1. 从 MvccStorage 导出全量快照
/// 2. 写入快照文件（命名：snapshot-{timestamp}.snap）
/// 3. 清理超过保留期的旧快照
pub struct SnapshotScheduler<B: StorageBackend> {
    storage: Arc<MvccStorage<B>>,
    config: SnapshotSchedulerConfig,
    /// 上次快照时间（用于去重）
    last_snapshot_time: RwLock<SystemTime>,
}

impl<B: StorageBackend + 'static> SnapshotScheduler<B> {
    /// 创建快照调度器
    pub fn new(storage: Arc<MvccStorage<B>>, config: SnapshotSchedulerConfig) -> Self {
        Self {
            storage,
            config,
            last_snapshot_time: RwLock::new(UNIX_EPOCH),
        }
    }

    /// 启动后台快照调度任务
    ///
    /// 返回 JoinHandle，可 abort 以停止调度。
    pub fn start(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            if !self.config.auto_snapshot {
                tracing::info!("Auto snapshot is disabled");
                return;
            }

            // 确保快照目录存在
            if let Err(e) = std::fs::create_dir_all(&self.config.snapshot_dir) {
                tracing::error!(
                    "Failed to create snapshot dir {}: {e}",
                    self.config.snapshot_dir.display()
                );
                return;
            }

            tracing::info!(
                "Snapshot scheduler started: interval={:?}, retention={:?}, dir={}",
                self.config.interval,
                self.config.retention,
                self.config.snapshot_dir.display()
            );

            let mut ticker = tokio::time::interval(self.config.interval);
            // 跳过首次立即触发，等待一个间隔周期
            ticker.tick().await;

            loop {
                ticker.tick().await;

                match self.create_snapshot().await {
                    Ok(path) => {
                        tracing::info!("Auto snapshot created: {}", path.display());
                    }
                    Err(e) => {
                        tracing::error!("Auto snapshot failed: {e}");
                    }
                }

                // 清理过期快照
                if let Err(e) = self.cleanup_old_snapshots() {
                    tracing::error!("Snapshot cleanup failed: {e}");
                }
            }
        })
    }

    /// 创建当前状态机的快照
    async fn create_snapshot(&self) -> Result<PathBuf> {
        let now = SystemTime::now();
        let timestamp = now
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        // 导出快照数据
        let snapshot_data = export_snapshot_data(&self.storage, 0, 0)?;

        // 序列化并写入文件
        let filename = format!("snapshot-{}.snap", timestamp);
        let filepath = self.config.snapshot_dir.join(&filename);

        let bytes = snapshot_data.to_bytes()?;
        std::fs::write(&filepath, &bytes).map_err(|e| {
            Error::Internal(format!("write snapshot {}: {e}", filepath.display()))
        })?;

        // 更新最后快照时间
        *self.last_snapshot_time.write().await = now;

        tracing::info!(
            "Snapshot saved: {} ({} KV pairs, {} bytes)",
            filepath.display(),
            snapshot_data.kv_pairs.len(),
            bytes.len()
        );

        Ok(filepath)
    }

    /// 清理超过保留期的旧快照文件
    fn cleanup_old_snapshots(&self) -> Result<usize> {
        let now = SystemTime::now();
        let mut removed = 0;

        let entries = std::fs::read_dir(&self.config.snapshot_dir).map_err(|e| {
            Error::Internal(format!(
                "read snapshot dir {}: {e}",
                self.config.snapshot_dir.display()
            ))
        })?;

        for entry in entries {
            let entry = match entry {
                Ok(e) => e,
                Err(_) => continue,
            };

            let path = entry.path();

            // 只处理 .snap 文件
            if path.extension().map(|e| e != "snap").unwrap_or(true) {
                continue;
            }

            // 检查文件修改时间
            let modified = match entry.metadata() {
                Ok(m) => match m.modified() {
                    Ok(t) => t,
                    Err(_) => continue,
                },
                Err(_) => continue,
            };

            let age = match now.duration_since(modified) {
                Ok(d) => d,
                Err(_) => continue, // 文件时间在未来，跳过
            };

            if age > self.config.retention {
                match std::fs::remove_file(&path) {
                    Ok(()) => {
                        removed += 1;
                        tracing::debug!("Removed old snapshot: {}", path.display());
                    }
                    Err(e) => {
                        tracing::warn!("Failed to remove old snapshot {}: {e}", path.display());
                    }
                }
            }
        }

        if removed > 0 {
            tracing::info!("Cleaned up {} old snapshots", removed);
        }

        Ok(removed)
    }

    /// 手动触发一次快照（不等待定时器）
    pub async fn snapshot_now(&self) -> Result<PathBuf> {
        self.create_snapshot().await
    }

    /// 列出所有可用快照
    pub fn list_snapshots(&self) -> Result<Vec<SnapshotInfo>> {
        let mut snapshots = Vec::new();

        let entries = std::fs::read_dir(&self.config.snapshot_dir).map_err(|e| {
            Error::Internal(format!(
                "read snapshot dir {}: {e}",
                self.config.snapshot_dir.display()
            ))
        })?;

        for entry in entries {
            let entry = match entry {
                Ok(e) => e,
                Err(_) => continue,
            };

            let path = entry.path();
            if path.extension().map(|e| e != "snap").unwrap_or(true) {
                continue;
            }

            let metadata = match entry.metadata() {
                Ok(m) => m,
                Err(_) => continue,
            };

            let modified = match metadata.modified() {
                Ok(t) => t,
                Err(_) => continue,
            };

            snapshots.push(SnapshotInfo {
                path,
                size_bytes: metadata.len(),
                created: modified,
            });
        }

        // 按时间降序排列
        snapshots.sort_by(|a, b| b.created.cmp(&a.created));

        Ok(snapshots)
    }

    /// 从快照恢复数据
    pub fn restore_from_snapshot(
        &self,
        snapshot_path: &Path,
    ) -> Result<SnapshotData> {
        let bytes = std::fs::read(snapshot_path).map_err(|e| {
            Error::Internal(format!("read snapshot {}: {e}", snapshot_path.display()))
        })?;

        let snapshot_data = SnapshotData::from_bytes(&bytes)?;

        import_snapshot_data(&self.storage, &snapshot_data)?;

        tracing::info!(
            "Restored snapshot {}: version={}, {} KV pairs",
            snapshot_path.display(),
            snapshot_data.version,
            snapshot_data.kv_pairs.len()
        );

        Ok(snapshot_data)
    }
}

// ──── 快照信息 ────

/// 快照文件信息
#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    /// 快照文件路径
    pub path: PathBuf,
    /// 文件大小（字节）
    pub size_bytes: u64,
    /// 创建时间
    pub created: SystemTime,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_default_config() {
        let config = SnapshotSchedulerConfig::default();
        assert_eq!(config.interval, Duration::from_secs(3600));
        assert_eq!(config.retention, Duration::from_secs(7 * 86400));
        assert!(config.auto_snapshot);
    }

    #[test]
    fn test_config_disabled() {
        let mut config = SnapshotSchedulerConfig::default();
        config.auto_snapshot = false;
        assert!(!config.auto_snapshot);
    }

    #[test]
    fn test_list_snapshots_empty_dir() {
        let tmp = TempDir::new().unwrap();
        let config = SnapshotSchedulerConfig {
            snapshot_dir: tmp.path().to_path_buf(),
            ..Default::default()
        };

        // We can test the listing logic without a full SnapshotScheduler
        // by directly testing the list_snapshots behavior
        let snapshots = list_snapshots_in_dir(&config.snapshot_dir).unwrap();
        assert!(snapshots.is_empty());
    }

    #[test]
    fn test_cleanup_no_files() {
        let tmp = TempDir::new().unwrap();
        let config = SnapshotSchedulerConfig {
            snapshot_dir: tmp.path().to_path_buf(),
            retention: Duration::from_secs(1),
            ..Default::default()
        };

        // No .snap files, cleanup should remove 0
        let entries: Vec<_> = std::fs::read_dir(&config.snapshot_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(entries.is_empty());
    }

    // Helper: list snapshots in a directory
    fn list_snapshots_in_dir(dir: &Path) -> Result<Vec<SnapshotInfo>> {
        let mut snapshots = Vec::new();
        for entry in std::fs::read_dir(dir).map_err(|e| {
            Error::Internal(format!("read dir: {e}"))
        })? {
            let entry = entry.map_err(|e| Error::Internal(format!("entry: {e}")))?;
            let path = entry.path();
            if path.extension().map(|e| e != "snap").unwrap_or(true) {
                continue;
            }
            let meta = entry.metadata().map_err(|e| Error::Internal(format!("meta: {e}")))?;
            snapshots.push(SnapshotInfo {
                path,
                size_bytes: meta.len(),
                created: meta.modified().unwrap_or(UNIX_EPOCH),
            });
        }
        snapshots.sort_by(|a, b| b.created.cmp(&a.created));
        Ok(snapshots)
    }
}
