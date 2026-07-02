// Snapshot 导出/导入
//
// 实现 ADP §19 的状态机快照能力：
// - export_snapshot_data: 从 MvccStorage 导出全量数据
// - import_snapshot_data: 将快照数据恢复到 MvccStorage
//
// 快照格式使用 bincode 序列化，包含所有 KV 数据、元数据和 Raft 检查点。

use serde::{Deserialize, Serialize};

use coord_core::error::{Error, Result};
use coord_core::storage::StorageBackend;

use super::mvcc::{
    encode_kv_key, encode_kv_meta_key, KvMetadata, MvccStorage,
    TABLE_KV, TABLE_KV_META, TABLE_META, META_NEXT_REVISION, META_APPLIED_INDEX,
};

// ──── Snapshot 数据结构 ────

/// 全量快照数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotData {
    /// 快照版本（用于向前兼容）
    pub version: u32,
    /// Raft 最后包含的 Log Index
    pub last_included_index: u64,
    /// Raft 最后包含的 Term
    pub last_included_term: u64,
    /// 全局 Revision 计数器（下一个可用 Revision）
    pub next_revision: u64,
    /// 已 Apply 的最大 Raft Index
    pub applied_index: u64,
    /// 所有 KV 数据对（加密后的密文）
    pub kv_pairs: Vec<SnapshotKvPair>,
    /// 所有 KV 元数据
    pub kv_metadata: Vec<SnapshotKvMeta>,
}

/// 快照中的单条 KV 记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotKvPair {
    /// 用户 Key（不含 /kv/ 前缀）
    pub key: Vec<u8>,
    /// Value（加密后的密文，空表示 tombstone）
    pub value: Vec<u8>,
}

/// 快照中的单条 KV 元数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotKvMeta {
    /// 用户 Key（不含前缀）
    pub key: Vec<u8>,
    pub version: i64,
    pub create_revision: i64,
    pub mod_revision: i64,
    pub lease_id: i64,
    pub deleted: bool,
}

impl SnapshotData {
    /// 当前快照格式版本
    const CURRENT_VERSION: u32 = 1;

    /// 创建空快照
    pub fn new(
        last_included_index: u64,
        last_included_term: u64,
    ) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            last_included_index,
            last_included_term,
            next_revision: 1,
            applied_index: 0,
            kv_pairs: Vec::new(),
            kv_metadata: Vec::new(),
        }
    }

    /// 序列化为字节（用于网络传输和磁盘存储）
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::serialize(self)
            .map_err(|e| Error::Internal(format!("snapshot serialize: {e}")))
    }

    /// 从字节反序列化
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        bincode::deserialize(data)
            .map_err(|e| Error::Internal(format!("snapshot deserialize: {e}")))
    }
}

// ──── 导出/导入函数 ────

/// 从 MvccStorage 导出快照数据
///
/// 遍历所有 KV 数据和元数据，包含 Raft 检查点。
/// 导出的 Value 是加密后的密文（不经过 Barrier 解密），保证 Snapshot 不接触明文。
pub fn export_snapshot_data<B: StorageBackend>(
    storage: &MvccStorage<B>,
    last_included_index: u64,
    last_included_term: u64,
) -> Result<SnapshotData> {
    let mut data = SnapshotData::new(last_included_index, last_included_term);

    // 读取 Revision 计数器
    data.next_revision = storage.current_revision().saturating_add(1);
    if data.next_revision == 0 {
        data.next_revision = 1;
    }

    // 读取 Applied Index
    let backend = storage.backend();
    data.applied_index = backend
        .read(|tx| {
            tx.get(TABLE_META, META_APPLIED_INDEX)
                .map(|opt| {
                    opt.and_then(|bytes| {
                        if bytes.len() == 8 {
                            Some(u64::from_be_bytes(bytes.as_slice().try_into().unwrap()))
                        } else {
                            None
                        }
                    })
                    .unwrap_or(0)
                })
        })?;

    // 导出 KV 数据（密文，直接读取不经过 Barrier）
    let kv_prefix = encode_kv_key(b"");
    let kv_rows = backend.read(|tx| tx.iter_prefix(TABLE_KV, &kv_prefix))?;
    for (internal_key, value) in kv_rows.into_iter() {
        if let Some(user_key) = super::mvcc::decode_kv_key(&internal_key) {
            data.kv_pairs.push(SnapshotKvPair {
                key: user_key.to_vec(),
                value,
            });
        }
    }

    // 导出 KV 元数据
    let meta_prefix = encode_kv_meta_key(b"");
    let meta_rows = backend.read(|tx| tx.iter_prefix(TABLE_KV_META, &meta_prefix))?;
    for (internal_key, meta_bytes) in meta_rows.into_iter() {
        // 提取用户 Key：去掉 /_kv_meta/ 前缀
        let kv_meta_prefix = b"/_kv_meta/";
        if let Some(user_key) = internal_key.strip_prefix(kv_meta_prefix) {
            if let Some(meta) = KvMetadata::from_bytes(&meta_bytes) {
                data.kv_metadata.push(SnapshotKvMeta {
                    key: user_key.to_vec(),
                    version: meta.version,
                    create_revision: meta.create_revision,
                    mod_revision: meta.mod_revision,
                    lease_id: meta.lease_id,
                    deleted: meta.deleted,
                });
            }
        }
    }

    Ok(data)
}

/// 将快照数据导入到 MvccStorage
///
/// 清空现有数据后写入快照中的全部 KV 数据和元数据。
/// Barrier 加密/解密不介入——快照导入的是原始密文。
pub fn import_snapshot_data<B: StorageBackend>(
    storage: &MvccStorage<B>,
    data: &SnapshotData,
) -> Result<()> {
    if data.version != SnapshotData::CURRENT_VERSION {
        return Err(Error::Internal(format!(
            "unsupported snapshot version: {} (expected {})",
            data.version,
            SnapshotData::CURRENT_VERSION
        )));
    }

    let backend = storage.backend();

    backend.write(|tx| {
        // 清空 KV 表（删除所有以 /kv/ 开头的 Key）
        let kv_prefix = encode_kv_key(b"");
        let existing_kv: Vec<Vec<u8>> = tx
            .iter_prefix(TABLE_KV, &kv_prefix)?
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        for key in &existing_kv {
            tx.remove(TABLE_KV, key)?;
        }

        // 清空 KV 元数据表
        let meta_prefix = encode_kv_meta_key(b"");
        let existing_meta: Vec<Vec<u8>> = tx
            .iter_prefix(TABLE_KV_META, &meta_prefix)?
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        for key in &existing_meta {
            tx.remove(TABLE_KV_META, key)?;
        }

        // 写入 KV 数据
        for pair in &data.kv_pairs {
            let internal_key = encode_kv_key(&pair.key);
            tx.insert(TABLE_KV, &internal_key, &pair.value)?;
        }

        // 写入 KV 元数据
        for meta in &data.kv_metadata {
            let meta_key = encode_kv_meta_key(&meta.key);
            let meta_bytes = KvMetadata {
                version: meta.version,
                create_revision: meta.create_revision,
                mod_revision: meta.mod_revision,
                lease_id: meta.lease_id,
                deleted: meta.deleted,
            }
            .to_bytes();
            tx.insert(TABLE_KV_META, &meta_key, &meta_bytes)?;
        }

        // 更新 Revision 计数器
        tx.insert(
            TABLE_META,
            META_NEXT_REVISION,
            &data.next_revision.to_be_bytes(),
        )?;

        // 更新 Applied Index
        tx.insert(
            TABLE_META,
            META_APPLIED_INDEX,
            &data.applied_index.to_be_bytes(),
        )?;

        Ok(())
    })?;

    // 更新内存中的 Revision 计数器
    storage.set_next_revision(data.next_revision);

    Ok(())
}

// ──── 测试 ────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::redb_backend::RedbBackend;
    use coord_core::types::StorageConfig;
    use tempfile::TempDir;

    fn setup_storage() -> (TempDir, MvccStorage<RedbBackend>) {
        let tmp = TempDir::new().unwrap();
        let config = StorageConfig::default();
        let backend = RedbBackend::open(tmp.path(), &config).unwrap();
        let storage = MvccStorage::new(backend).unwrap();
        (tmp, storage)
    }

    #[test]
    fn test_empty_snapshot_roundtrip() {
        let (_tmp, storage) = setup_storage();

        // 导出空快照
        let data = export_snapshot_data(&storage, 5, 3).unwrap();
        assert_eq!(data.last_included_index, 5);
        assert_eq!(data.last_included_term, 3);
        assert_eq!(data.kv_pairs.len(), 0);
        assert_eq!(data.kv_metadata.len(), 0);

        // 序列化/反序列化
        let bytes = data.to_bytes().unwrap();
        let restored = SnapshotData::from_bytes(&bytes).unwrap();
        assert_eq!(restored.last_included_index, 5);
        assert_eq!(restored.last_included_term, 3);

        // 导入空快照
        import_snapshot_data(&storage, &restored).unwrap();
    }

    #[test]
    fn test_snapshot_with_kv_data() {
        let (_tmp, storage) = setup_storage();

        // 写入一些数据
        storage.put(b"/app/config", b"value1", None).unwrap();
        storage.put(b"/app/secret", b"value2", None).unwrap();
        storage.put(b"/service/addr", b"127.0.0.1:8080", None).unwrap();

        // 导出版本（不含 Barrier，直接读密文）
        let data = export_snapshot_data(&storage, 10, 2).unwrap();
        assert_eq!(data.kv_pairs.len(), 3);
        assert_eq!(data.kv_metadata.len(), 3);

        // 序列化往返
        let bytes = data.to_bytes().unwrap();
        let restored = SnapshotData::from_bytes(&bytes).unwrap();
        assert_eq!(restored.kv_pairs.len(), 3);
        assert_eq!(restored.kv_metadata.len(), 3);

        // 导入到新 storage
        let tmp2 = TempDir::new().unwrap();
        let config2 = StorageConfig::default();
        let backend2 = RedbBackend::open(tmp2.path(), &config2).unwrap();
        let storage2 = MvccStorage::new(backend2).unwrap();
        import_snapshot_data(&storage2, &restored).unwrap();

        // 验证数据可读
        let v1 = storage2.get(b"/app/config").unwrap();
        assert_eq!(v1, Some(b"value1".to_vec()));
        let v2 = storage2.get(b"/app/secret").unwrap();
        assert_eq!(v2, Some(b"value2".to_vec()));
        let v3 = storage2.get(b"/service/addr").unwrap();
        assert_eq!(v3, Some(b"127.0.0.1:8080".to_vec()));
    }

    #[test]
    fn test_snapshot_with_delete_tombstone() {
        let (_tmp, storage) = setup_storage();

        storage.put(b"/key1", b"val1", None).unwrap();
        storage.put(b"/key2", b"val2", None).unwrap();
        storage.delete(b"/key1").unwrap();

        let data = export_snapshot_data(&storage, 1, 1).unwrap();
        // key1 存在但 value 为空 (tombstone), key2 有值
        assert_eq!(data.kv_pairs.len(), 2);

        let bytes = data.to_bytes().unwrap();
        let restored = SnapshotData::from_bytes(&bytes).unwrap();

        let tmp2 = TempDir::new().unwrap();
        let config2 = StorageConfig::default();
        let backend2 = RedbBackend::open(tmp2.path(), &config2).unwrap();
        let storage2 = MvccStorage::new(backend2).unwrap();
        import_snapshot_data(&storage2, &restored).unwrap();

        // key1 应为 tombstone（None）
        assert!(storage2.get(b"/key1").unwrap().is_none());
        // key2 应有值
        assert_eq!(storage2.get(b"/key2").unwrap(), Some(b"val2".to_vec()));
    }

    #[test]
    fn test_snapshot_many_keys() {
        let (_tmp, storage) = setup_storage();

        // 写入 100 个 key
        for i in 0..100u32 {
            let key = format!("/test/key{:04}", i);
            let val = format!("value{}", i);
            storage.put(key.as_bytes(), val.as_bytes(), None).unwrap();
        }

        let data = export_snapshot_data(&storage, 100, 5).unwrap();
        assert_eq!(data.kv_pairs.len(), 100);
        assert_eq!(data.kv_metadata.len(), 100);

        // 序列化大小合理
        let bytes = data.to_bytes().unwrap();
        assert!(bytes.len() < 100_000, "snapshot should be compact");

        let restored = SnapshotData::from_bytes(&bytes).unwrap();

        let tmp2 = TempDir::new().unwrap();
        let config2 = StorageConfig::default();
        let backend2 = RedbBackend::open(tmp2.path(), &config2).unwrap();
        let storage2 = MvccStorage::new(backend2).unwrap();
        import_snapshot_data(&storage2, &restored).unwrap();

        for i in 0..100u32 {
            let key = format!("/test/key{:04}", i);
            let val = format!("value{}", i);
            assert_eq!(
                storage2.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes())
            );
        }
    }
}
