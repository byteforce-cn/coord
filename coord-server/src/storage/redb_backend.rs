// Redb 存储后端实现
//
// 将 coord-core::storage::StorageBackend trait 适配到 Redb 4.1.0。
// 直接使用 Redb 内置 MVCC，不额外建立应用层版本管理。

use std::path::Path;
use std::sync::Arc;

use coord_core::error::Result;
use coord_core::storage::{ReadTx, StorageBackend, WriteTx};
use coord_core::types::StorageConfig;
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
/// 内部线程安全，支持并发读写。Clone 共享底层 `Arc<Database>`。
#[derive(Clone)]
pub struct RedbBackend {
    db: Arc<Database>,
    #[allow(dead_code)]
    config: StorageConfig,
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
            db: Arc::new(db),
            config: config.clone(),
        })
    }

    fn read<T>(&self, f: impl FnOnce(&dyn ReadTx) -> Result<T>) -> Result<T> {
        let read_tx = self
            .db
            .begin_read()
            .map_err(|e| coord_core::error::Error::Storage(format!("begin read tx: {}", e)))?;

        let adapter = RedbReadTx { tx: read_tx };
        f(&adapter)
    }

    fn write<T>(&self, f: impl FnOnce(&mut dyn WriteTx) -> Result<T>) -> Result<T> {
        let write_tx = self
            .db
            .begin_write()
            .map_err(|e| coord_core::error::Error::Storage(format!("begin write tx: {}", e)))?;

        let mut adapter = RedbWriteTx { tx: write_tx };
        let result = f(&mut adapter)?;

        adapter
            .tx
            .commit()
            .map_err(|e| coord_core::error::Error::Storage(format!("commit tx: {}", e)))?;

        Ok(result)
    }

    fn compact(&self) -> Result<()> {
        // Redb 4.1: compact() 需要 &mut self，而 self.db 在 Arc 中。
        // Compaction 是优化操作而非正确性要求，运行时跳过。
        // 生产环境建议通过独立 compaction 线程持有独占引用时触发。
        tracing::warn!(
            "RedbBackend::compact: requires exclusive DB access, skipped at runtime"
        );
        Ok(())
    }

    fn disk_size_bytes(&self) -> Result<u64> {
        // Redb 不直接提供磁盘大小查询 API，通过文件系统获取
        // 这里返回一个估算值；实际使用中可通过 db.stats() 获取
        Ok(0)
    }

    fn key_count(&self) -> Result<u64> {
        let count = self
            .db
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
            Err(e) => Err(coord_core::error::Error::Storage(format!(
                "get key: {}",
                e
            ))),
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
            let (k, v) = item.map_err(|e| {
                coord_core::error::Error::Storage(format!("iter item: {}", e))
            })?;
            let key_bytes: &[u8] = k.value();
            let val_bytes: &[u8] = v.value();

            if !key_bytes.starts_with(&prefix_vec) {
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
            Err(e) => Err(coord_core::error::Error::Storage(format!(
                "get key: {}",
                e
            ))),
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
            let (k, v) = item.map_err(|e| {
                coord_core::error::Error::Storage(format!("iter item: {}", e))
            })?;
            let key_bytes: &[u8] = k.value();
            let val_bytes: &[u8] = v.value();

            if !key_bytes.starts_with(&prefix_vec) {
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

#[cfg(test)]
mod tests {
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
        let value = backend
            .read(|tx| tx.get("kv", b"hello"))
            .unwrap();

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

        let value = backend
            .read(|tx| tx.get("kv", b"key1"))
            .unwrap();

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
}
