// Redb 存储后端实现
//
// 将 coord-core::storage::StorageBackend trait 适配到 Redb 4.1.0。
// 直接使用 Redb 内置 MVCC，不额外建立应用层版本管理。
//
// 并发模型（P1-01 改造）：内部以 `parking_lot::RwLock<Database>` 持有。
// - 读事务持读锁并行；写事务持写锁（与 redb 单写者语义一致，仅提前阻塞）；
// - `compact()` 需要独占 `&mut Database`（redb 4.1 API），持写锁的维护窗口内执行，
//   期间阻塞读写 —— 这是 redb 4.1 的固有限制（决策文档 §八风险表），
//   调度由 `CompactionManager` 以小时级间隔执行。
// - R-RFT-19：`compact_with_idle_window` 等待一段无写入静默期再拿独占写锁，
//   将在线读写与压缩窗口错开，规避「长事务窗口期间新读写全部等待」。

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use coord_core::error::Result;
use coord_core::storage::{ReadTx, StorageBackend, WriteTx};
use coord_core::types::StorageConfig;
use parking_lot::RwLock;
use redb::{
    Database, ReadTransaction, ReadableDatabase, ReadableTable, ReadableTableMetadata,
    TableDefinition, WriteTransaction,
};

// ──── 表定义 ────
//
// Redb 使用静态 TableDefinition 定义表结构。每个 Key 空间前缀映射到一个表。

/// 用户 KV 数据表：Key=bytes, Value=bytes
const TABLE_KV: TableDefinition<&[u8], &[u8]> = TableDefinition::new("kv");

/// 内部元数据表：Key=bytes, Value=bytes
const TABLE_META: TableDefinition<&[u8], &[u8]> = TableDefinition::new("meta");

/// Lease 绑定表：Key=bytes, Value=bytes
const TABLE_LEASE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("lease");

/// 认证数据表：Key=bytes, Value=bytes
const TABLE_AUTH: TableDefinition<&[u8], &[u8]> = TableDefinition::new("auth");

/// 变更日志表（Changelog）：Key=bytes, Value=bytes
const TABLE_CHANGELOG: TableDefinition<&[u8], &[u8]> = TableDefinition::new("changelog");

/// KV 元数据表：Key=bytes, Value=bytes
/// 存储每个用户 Key 的版本号、创建/修改 Revision、关联 Lease
const TABLE_KV_META: TableDefinition<&[u8], &[u8]> = TableDefinition::new("kv_meta");

/// 根据表名返回对应的 TableDefinition。
/// 显式标注 'static 生命周期以防止 Rust 的 lifetime elision 将返回类型生命周期
/// 绑定到输入参数（name: &str）上。
#[allow(mismatched_lifetime_syntaxes)]
fn resolve_table(name: &str) -> Result<TableDefinition<&'static [u8], &'static [u8]>> {
    match name {
        "kv" => Ok(TABLE_KV),
        "meta" => Ok(TABLE_META),
        "lease" => Ok(TABLE_LEASE),
        "auth" => Ok(TABLE_AUTH),
        "changelog" => Ok(TABLE_CHANGELOG),
        "kv_meta" => Ok(TABLE_KV_META),
        unknown => Err(coord_core::error::Error::InvalidArgument(format!(
            "unknown table: {}",
            unknown
        ))),
    }
}

// ──── RedbBackend ────

/// Redb 存储后端
///
/// 封装 redb::Database，实现 coord_core::storage::StorageBackend trait。
/// 内部线程安全，支持并发读写。Clone 共享底层 `Arc<RwLock<Database>>`。
#[derive(Clone)]
pub struct RedbBackend {
    db: Arc<RwLock<Database>>,
    /// store.db 路径（`disk_size_bytes` 用文件系统元数据计算）
    db_path: PathBuf,
    #[allow(dead_code)]
    config: StorageConfig,
    /// R-RFT-19：最近一次写入时间（unix 毫秒）——compact 空闲窗口判定依据
    last_write_ms: Arc<AtomicU64>,
}

impl StorageBackend for RedbBackend {
    fn open(path: &Path, config: &StorageConfig) -> Result<Self>
    where
        Self: Sized,
    {
        let db_path = path.join("store.db");
        let db = if db_path.exists() {
            Database::open(&db_path).map_err(|e| {
                coord_core::error::Error::Storage(format!("failed to open database: {}", e))
            })?
        } else {
            Database::create(&db_path).map_err(|e| {
                coord_core::error::Error::Storage(format!("failed to create database: {}", e))
            })?
        };

        // 确保所有表已创建（Redb 需要在首次使用时创建表）
        {
            let write_tx = db.begin_write().map_err(|e| {
                coord_core::error::Error::Storage(format!("failed to begin write tx: {}", e))
            })?;
            {
                let _ = write_tx.open_table(TABLE_KV);
                let _ = write_tx.open_table(TABLE_META);
                let _ = write_tx.open_table(TABLE_LEASE);
                let _ = write_tx.open_table(TABLE_AUTH);
                let _ = write_tx.open_table(TABLE_CHANGELOG);
                let _ = write_tx.open_table(TABLE_KV_META);
            }
            write_tx.commit().map_err(|e| {
                coord_core::error::Error::Storage(format!("failed to commit init tx: {}", e))
            })?;
        }

        Ok(Self {
            db: Arc::new(RwLock::new(db)),
            db_path,
            config: config.clone(),
            last_write_ms: Arc::new(AtomicU64::new(now_ms())),
        })
    }

    fn read<T>(&self, f: impl FnOnce(&dyn ReadTx) -> Result<T>) -> Result<T> {
        let db = self.db.read();
        let read_tx = db
            .begin_read()
            .map_err(|e| coord_core::error::Error::Storage(format!("begin read tx: {}", e)))?;

        let adapter = RedbReadTx { tx: read_tx };
        f(&adapter)
    }

    fn write<T>(&self, f: impl FnOnce(&mut dyn WriteTx) -> Result<T>) -> Result<T> {
        let db = self.db.write();
        let write_tx = db
            .begin_write()
            .map_err(|e| coord_core::error::Error::Storage(format!("begin write tx: {}", e)))?;

        let mut adapter = RedbWriteTx { tx: write_tx };
        let result = f(&mut adapter)?;

        adapter
            .tx
            .commit()
            .map_err(|e| coord_core::error::Error::Storage(format!("commit tx: {}", e)))?;

        // R-RFT-19：写入活动时间戳（compact 空闲窗口判定依据）
        self.last_write_ms.store(now_ms(), Ordering::Relaxed);

        Ok(result)
    }

    fn compact(&self) -> Result<()> {
        // P1-01：维护窗口内真实执行 redb 文件压缩（空间回收）。
        // redb 4.1 `Database::compact(&mut self)` 需要独占引用；写锁提供互斥，
        // 期间新读写阻塞（调度由 CompactionManager 控制，小时级间隔）。
        let mut db = self.db.write();
        let reclaimed = db.compact().map_err(|e| {
            coord_core::error::Error::Storage(format!("redb compact failed: {}", e))
        })?;
        if reclaimed {
            tracing::info!("redb compact: file shrunk (space reclaimed)");
        }
        Ok(())
    }

    fn disk_size_bytes(&self) -> Result<u64> {
        // P1-01：真实文件大小（此前恒 0，磁盘水位告警/只读依赖它，见 P1-02）
        std::fs::metadata(&self.db_path)
            .map(|m| m.len())
            .map_err(|e| {
                coord_core::error::Error::Storage(format!("stat {}: {}", self.db_path.display(), e))
            })
    }

    fn key_count(&self) -> Result<u64> {
        let db = self.db.read();
        let count = db
            .begin_read()
            .map_err(|e| coord_core::error::Error::Storage(format!("begin read tx: {}", e)))?
            .open_table(TABLE_KV)
            .map_err(|e| coord_core::error::Error::Storage(format!("open kv table: {}", e)))?
            .len()
            .map_err(|e| coord_core::error::Error::Storage(format!("count keys: {}", e)))?;
        Ok(count)
    }
}

// ──── RedbReadTx 适配器 ────

/// Redb 只读事务适配器
struct RedbReadTx {
    tx: ReadTransaction,
}

impl ReadTx for RedbReadTx {
    fn get(&self, table: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let table_def = resolve_table(table)?;
        let table = self
            .tx
            .open_table(table_def)
            .map_err(|e| coord_core::error::Error::Storage(format!("open table: {}", e)))?;

        let result = match table.get(key) {
            Ok(Some(guard)) => {
                let v: &[u8] = guard.value();
                Ok(Some(v.to_vec()))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(coord_core::error::Error::Storage(format!("get key: {}", e))),
        };
        result
    }

    fn iter_prefix(&self, table_name: &str, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let table_def = resolve_table(table_name)?;
        let table = self
            .tx
            .open_table(table_def)
            .map_err(|e| coord_core::error::Error::Storage(format!("open table: {}", e)))?;

        let mut results = Vec::new();
        let prefix_vec = prefix.to_vec();

        let iter = table
            .range(prefix..)
            .map_err(|e| coord_core::error::Error::Storage(format!("range scan: {}", e)))?;

        for item in iter {
            let (k, v) =
                item.map_err(|e| coord_core::error::Error::Storage(format!("iter item: {}", e)))?;
            let key_bytes: &[u8] = k.value();
            let val_bytes: &[u8] = v.value();

            if !key_bytes.starts_with(&prefix_vec) {
                break;
            }

            results.push((key_bytes.to_vec(), val_bytes.to_vec()));
        }

        Ok(results)
    }

    fn iter_range(
        &self,
        table_name: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let table_def = resolve_table(table_name)?;
        let table = self
            .tx
            .open_table(table_def)
            .map_err(|e| coord_core::error::Error::Storage(format!("open table: {}", e)))?;

        let mut results = Vec::new();

        let iter = table
            .range(start..)
            .map_err(|e| coord_core::error::Error::Storage(format!("range scan: {}", e)))?;

        for item in iter {
            let (k, v) =
                item.map_err(|e| coord_core::error::Error::Storage(format!("iter item: {}", e)))?;
            let key_bytes: &[u8] = k.value();
            let val_bytes: &[u8] = v.value();

            // 半开区间 [start, end)：end 非空且 key >= end 时终止
            if !end.is_empty() && key_bytes >= end {
                break;
            }
            results.push((key_bytes.to_vec(), val_bytes.to_vec()));
        }

        Ok(results)
    }
}

// ──── RedbWriteTx 适配器 ────

/// Redb 读写事务适配器
struct RedbWriteTx {
    tx: WriteTransaction,
}

impl ReadTx for RedbWriteTx {
    fn get(&self, table: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let table_def = resolve_table(table)?;
        let table = self
            .tx
            .open_table(table_def)
            .map_err(|e| coord_core::error::Error::Storage(format!("open table: {}", e)))?;

        let result = match table.get(key) {
            Ok(Some(guard)) => {
                let v: &[u8] = guard.value();
                Ok(Some(v.to_vec()))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(coord_core::error::Error::Storage(format!("get key: {}", e))),
        };
        result
    }

    fn iter_prefix(&self, table_name: &str, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let table_def = resolve_table(table_name)?;
        let table = self
            .tx
            .open_table(table_def)
            .map_err(|e| coord_core::error::Error::Storage(format!("open table: {}", e)))?;

        let mut results = Vec::new();
        let prefix_vec = prefix.to_vec();

        let iter = table
            .range(prefix..)
            .map_err(|e| coord_core::error::Error::Storage(format!("range scan: {}", e)))?;

        for item in iter {
            let (k, v) =
                item.map_err(|e| coord_core::error::Error::Storage(format!("iter item: {}", e)))?;
            let key_bytes: &[u8] = k.value();
            let val_bytes: &[u8] = v.value();

            if !key_bytes.starts_with(&prefix_vec) {
                break;
            }
            results.push((key_bytes.to_vec(), val_bytes.to_vec()));
        }

        Ok(results)
    }

    fn iter_range(
        &self,
        table_name: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let table_def = resolve_table(table_name)?;
        let table = self
            .tx
            .open_table(table_def)
            .map_err(|e| coord_core::error::Error::Storage(format!("open table: {}", e)))?;

        let mut results = Vec::new();

        let iter = table
            .range(start..)
            .map_err(|e| coord_core::error::Error::Storage(format!("range scan: {}", e)))?;

        for item in iter {
            let (k, v) =
                item.map_err(|e| coord_core::error::Error::Storage(format!("iter item: {}", e)))?;
            let key_bytes: &[u8] = k.value();
            let val_bytes: &[u8] = v.value();

            // 半开区间 [start, end)：end 非空且 key >= end 时终止
            if !end.is_empty() && key_bytes >= end {
                break;
            }
            results.push((key_bytes.to_vec(), val_bytes.to_vec()));
        }

        Ok(results)
    }
}

impl WriteTx for RedbWriteTx {
    fn insert(&mut self, table: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let table_def = resolve_table(table)?;
        let mut table = self
            .tx
            .open_table(table_def)
            .map_err(|e| coord_core::error::Error::Storage(format!("open table: {}", e)))?;

        table
            .insert(key, value)
            .map_err(|e| coord_core::error::Error::Storage(format!("insert: {}", e)))?;
        Ok(())
    }

    fn remove(&mut self, table: &str, key: &[u8]) -> Result<()> {
        let table_def = resolve_table(table)?;
        let mut table = self
            .tx
            .open_table(table_def)
            .map_err(|e| coord_core::error::Error::Storage(format!("open table: {}", e)))?;

        table
            .remove(key)
            .map_err(|e| coord_core::error::Error::Storage(format!("remove: {}", e)))?;
        Ok(())
    }
}

// ──── 测试 ────

// ──── R-RFT-19：compact 空闲窗口（独立 impl，供 CompactionManager 调用）───

impl RedbBackend {
    /// 等待 `idle` 时长的无写入静默期再执行 compact，将维护压缩与在线读写
    /// 窗口错开，规避「compact 长事务窗口期间新读写全部阻塞」。
    ///
    /// - 最多等待 `max_wait`；超时仍执行（空间回收优先，不长期积压）；
    /// - 同步实现（配合 `spawn_blocking` 调用，内部 `std::thread::sleep`）。
    pub fn compact_with_idle_window(
        &self,
        idle: std::time::Duration,
        max_wait: std::time::Duration,
    ) -> Result<()> {
        let deadline = std::time::Instant::now() + max_wait;
        loop {
            let last = self.last_write_ms.load(Ordering::Relaxed);
            let elapsed_ms = now_ms().saturating_sub(last);
            if elapsed_ms >= idle.as_millis() as u64 {
                break;
            }
            if std::time::Instant::now() >= deadline {
                tracing::warn!(
                    "redb compact: idle window not reached within {max_wait:?} (last write {elapsed_ms}ms ago); compacting anyway"
                );
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        self.compact()
    }
}

// ──── 工具 ────

/// 当前 unix 毫秒（写入活动时间戳；墙钟精度足够用于空闲窗口判定）
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compact_with_idle_window_waits_for_quiet() {
        let tmpdir = tempfile::tempdir().unwrap();
        let backend = RedbBackend::open(tmpdir.path(), &StorageConfig::default()).unwrap();
        // 刚写入后立即请求 compact：必须等待至少 idle 窗口才执行
        backend.write(|tx| tx.insert("kv", b"k", b"v")).unwrap();
        let start = std::time::Instant::now();
        let idle = std::time::Duration::from_millis(120);
        backend
            .compact_with_idle_window(idle, std::time::Duration::from_secs(5))
            .unwrap();
        assert!(
            start.elapsed() >= idle,
            "compact 应等待空闲窗口而非立即执行（实际 {}ms）",
            start.elapsed().as_millis()
        );
    }

    #[test]
    fn test_compact_with_idle_window_immediate_when_quiet() {
        let tmpdir = tempfile::tempdir().unwrap();
        let backend = RedbBackend::open(tmpdir.path(), &StorageConfig::default()).unwrap();
        backend.write(|tx| tx.insert("kv", b"k", b"v")).unwrap();
        // 距上次写入已超 idle 窗口 → 直接执行。不断言耗时（CI 负载不可控），
        // 以 compact 成功 + 数据完整为验证口径（等待路径见 wait 测试）。
        std::thread::sleep(std::time::Duration::from_millis(50));
        backend
            .compact_with_idle_window(
                std::time::Duration::from_millis(10),
                std::time::Duration::from_secs(5),
            )
            .unwrap();
        assert_eq!(
            backend.read(|tx| tx.get("kv", b"k")).unwrap(),
            Some(b"v".to_vec()),
            "compact 后数据保持完整"
        );
    }
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_open_and_read_write() {
        let dir = TempDir::new().unwrap();
        let config = StorageConfig {
            data_dir: dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };

        let backend = RedbBackend::open(dir.path(), &config).unwrap();

        // 写入
        backend
            .write(|tx| {
                tx.insert("kv", b"hello", b"world")?;
                Ok(())
            })
            .unwrap();

        // 读取
        let value = backend.read(|tx| tx.get("kv", b"hello")).unwrap();

        assert_eq!(value, Some(b"world".to_vec()));
    }

    #[test]
    fn test_prefix_scan() {
        let dir = TempDir::new().unwrap();
        let config = StorageConfig::default();
        let backend = RedbBackend::open(dir.path(), &config).unwrap();

        backend
            .write(|tx| {
                tx.insert("kv", b"/app/config/a", b"1")?;
                tx.insert("kv", b"/app/config/b", b"2")?;
                tx.insert("kv", b"/app/data/x", b"3")?;
                Ok(())
            })
            .unwrap();

        let results = backend
            .read(|tx| tx.iter_prefix("kv", b"/app/config/"))
            .unwrap();

        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_delete() {
        let dir = TempDir::new().unwrap();
        let config = StorageConfig::default();
        let backend = RedbBackend::open(dir.path(), &config).unwrap();

        backend
            .write(|tx| {
                tx.insert("kv", b"key1", b"val1")?;
                Ok(())
            })
            .unwrap();

        backend
            .write(|tx| {
                tx.remove("kv", b"key1")?;
                Ok(())
            })
            .unwrap();

        let value = backend.read(|tx| tx.get("kv", b"key1")).unwrap();

        assert_eq!(value, None);
    }

    #[test]
    fn test_unknown_table() {
        let dir = TempDir::new().unwrap();
        let config = StorageConfig::default();
        let backend = RedbBackend::open(dir.path(), &config).unwrap();

        let result = backend.read(|tx| tx.get("unknown_table", b"key"));
        assert!(result.is_err());
    }

    // ──── P1-01 Compaction ────

    #[test]
    fn test_disk_size_bytes_reports_file_size() {
        let dir = TempDir::new().unwrap();
        let config = StorageConfig::default();
        let backend = RedbBackend::open(dir.path(), &config).unwrap();

        backend
            .write(|tx| {
                for i in 0..100u32 {
                    let key = format!("/k{i}");
                    tx.insert("kv", key.as_bytes(), &[0xABu8; 512])?;
                }
                Ok(())
            })
            .unwrap();

        let size = backend.disk_size_bytes().unwrap();
        assert!(size > 0, "disk size should reflect store.db file size");
    }

    #[test]
    fn test_compact_preserves_data() {
        let dir = TempDir::new().unwrap();
        let config = StorageConfig::default();
        let backend = RedbBackend::open(dir.path(), &config).unwrap();

        backend
            .write(|tx| {
                tx.insert("kv", b"k1", b"v1")?;
                tx.insert("kv", b"k2", b"v2")?;
                Ok(())
            })
            .unwrap();

        // 删除一个 key 制造空闲页，再 compact（维护窗口：独占引用）
        backend
            .write(|tx| {
                tx.remove("kv", b"k1")?;
                Ok(())
            })
            .unwrap();
        backend.compact().unwrap();

        let v = backend.read(|tx| tx.get("kv", b"k2")).unwrap();
        assert_eq!(v, Some(b"v2".to_vec()));
        let gone = backend.read(|tx| tx.get("kv", b"k1")).unwrap();
        assert_eq!(gone, None);
    }
}
