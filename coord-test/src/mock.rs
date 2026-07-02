// coord-test: Mock 存储后端
//
// 提供基于 HashMap 的内存存储后端，实现 StorageBackend trait。
// 用于单元测试中替代 Redb，避免磁盘 I/O。

use std::collections::BTreeMap;
use std::sync::RwLock;

/// 内存中的 Mock 存储后端。
///
/// 使用 `BTreeMap` 模拟表结构，支持前缀扫描。
/// 线程安全（内部 `RwLock`）。
///
/// # 示例
/// ```ignore
/// use coord_test::mock::MockStorage;
///
/// let storage = MockStorage::new();
/// storage.write(|tx| {
///     tx.insert("kv", b"key1", b"value1")?;
///     Ok(())
/// }).unwrap();
/// ```
#[derive(Debug, Default)]
pub struct MockStorage {
    /// 表名 → (Key → Value)，使用 BTreeMap 支持有序遍历
    tables: RwLock<BTreeMap<String, BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl MockStorage {
    /// 创建空的 Mock 存储。
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(BTreeMap::new()),
        }
    }

    /// 在只读事务中执行操作。
    pub fn read<T>(&self, f: impl FnOnce(&MockReadTx) -> Result<T, String>) -> Result<T, String> {
        let tables = self.tables.read().map_err(|e| format!("lock error: {e}"))?;
        let tx = MockReadTx { tables: &tables };
        f(&tx)
    }

    /// 在读写事务中执行操作。
    pub fn write<T>(&self, f: impl FnOnce(&mut MockWriteTx) -> Result<T, String>) -> Result<T, String> {
        let mut tables = self.tables.write().map_err(|e| format!("lock error: {e}"))?;
        let mut tx = MockWriteTx { tables: &mut tables };
        f(&mut tx)
    }

    /// 获取所有表名。
    pub fn table_names(&self) -> Vec<String> {
        self.tables.read().unwrap().keys().cloned().collect()
    }

    /// 获取指定表的所有键值对。
    pub fn dump_table(&self, table: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.tables
            .read()
            .unwrap()
            .get(table)
            .map(|t| t.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default()
    }

    /// 获取指定表的键数量。
    pub fn key_count(&self, table: &str) -> usize {
        self.tables
            .read()
            .unwrap()
            .get(table)
            .map(|t| t.len())
            .unwrap_or(0)
    }
}

/// Mock 只读事务句柄。
pub struct MockReadTx<'a> {
    tables: &'a BTreeMap<String, BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl<'a> MockReadTx<'a> {
    /// 读取单个键的值。
    pub fn get(&self, table: &str, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        Ok(self
            .tables
            .get(table)
            .and_then(|t| t.get(key))
            .cloned())
    }

    /// 前缀扫描：返回所有匹配前缀的键值对。
    pub fn iter_prefix(
        &self,
        table: &str,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        let table_data = match self.tables.get(table) {
            Some(t) => t,
            None => return Ok(Vec::new()),
        };

        // BTreeMap range scan: find first key >= prefix, collect while prefix matches
        let results: Vec<_> = table_data
            .range(prefix.to_vec()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        Ok(results)
    }

    /// 检查键是否存在。
    pub fn exists(&self, table: &str, key: &[u8]) -> Result<bool, String> {
        Ok(self
            .tables
            .get(table)
            .map(|t| t.contains_key(key))
            .unwrap_or(false))
    }
}

/// Mock 读写事务句柄。
pub struct MockWriteTx<'a> {
    tables: &'a mut BTreeMap<String, BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl<'a> MockWriteTx<'a> {
    /// 插入键值对。
    pub fn insert(&mut self, table: &str, key: &[u8], value: &[u8]) -> Result<(), String> {
        let table_data = self.tables.entry(table.to_string()).or_default();
        table_data.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    /// 删除键。
    pub fn remove(&mut self, table: &str, key: &[u8]) -> Result<(), String> {
        if let Some(table_data) = self.tables.get_mut(table) {
            table_data.remove(key);
        }
        Ok(())
    }

    /// 获取只读视图（在同一事务内读取）。
    pub fn as_read(&self) -> MockReadTx<'_> {
        MockReadTx {
            tables: self.tables,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_storage_insert_and_get() {
        let storage = MockStorage::new();

        storage
            .write(|tx| {
                tx.insert("kv", b"hello", b"world")?;
                Ok(())
            })
            .unwrap();

        let value = storage
            .read(|tx| tx.get("kv", b"hello"))
            .unwrap();

        assert_eq!(value, Some(b"world".to_vec()));
    }

    #[test]
    fn test_mock_storage_delete() {
        let storage = MockStorage::new();

        storage
            .write(|tx| {
                tx.insert("kv", b"key1", b"val1")?;
                tx.insert("kv", b"key2", b"val2")?;
                tx.remove("kv", b"key1")?;
                Ok(())
            })
            .unwrap();

        assert_eq!(storage.key_count("kv"), 1);
    }

    #[test]
    fn test_mock_storage_prefix_scan() {
        let storage = MockStorage::new();

        storage
            .write(|tx| {
                tx.insert("kv", b"/app/config", b"v1")?;
                tx.insert("kv", b"/app/timeout", b"v2")?;
                tx.insert("kv", b"/other/data", b"v3")?;
                Ok(())
            })
            .unwrap();

        let results = storage
            .read(|tx| tx.iter_prefix("kv", b"/app/"))
            .unwrap();

        assert_eq!(results.len(), 2);
        let keys: Vec<&[u8]> = results.iter().map(|(k, _)| k.as_slice()).collect();
        assert!(keys.contains(&b"/app/config".as_slice()));
        assert!(keys.contains(&b"/app/timeout".as_slice()));
    }

    #[test]
    fn test_mock_storage_multiple_tables() {
        let storage = MockStorage::new();

        storage
            .write(|tx| {
                tx.insert("kv", b"k1", b"v1")?;
                tx.insert("meta", b"revision", &1u64.to_be_bytes())?;
                Ok(())
            })
            .unwrap();

        assert_eq!(storage.key_count("kv"), 1);
        assert_eq!(storage.key_count("meta"), 1);
        assert_eq!(storage.key_count("nonexistent"), 0);
    }
}
