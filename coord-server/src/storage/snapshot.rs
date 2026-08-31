// Snapshot 导出/导入
//
// 实现 ADP §19 的状态机快照能力：
// - export_snapshot_data: 从 MvccStorage 导出全量数据
// - import_snapshot_data: 将快照数据恢复到 MvccStorage
//
// 快照格式使用 bincode 序列化，包含所有 KV 数据、元数据和 Raft 检查点。

use std::path::PathBuf;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use coord_core::error::{Error, Result};
use coord_core::storage::StorageBackend;

use super::mvcc::{
    encode_kv_key, encode_kv_meta_key, AppliedLogId, KvMetadata, MvccStorage,
    META_COMPACT_REVISION, META_LAST_APPLIED, TABLE_KV, TABLE_KV_META, TABLE_META,
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
    /// R-RFT-06：已 Apply LogId 的 term（与 index 同事务持久化，导入后完整恢复）
    pub applied_term: u64,
    /// R-RFT-06：已 Apply LogId 的 node_id
    pub applied_node_id: u64,
    /// 所有 KV 数据对（加密后的密文）
    pub kv_pairs: Vec<SnapshotKvPair>,
    /// 所有 KV 元数据
    pub kv_metadata: Vec<SnapshotKvMeta>,
    /// R-RFT-06：auth 域原始条目（`/_sys/auth/*` → bytes：用户/角色/会话/吊销登记）
    pub auth_entries: Vec<SnapshotRawEntry>,
    /// R-RFT-06：lease 域原始条目（`/_lease/*` → bytes）
    pub lease_entries: Vec<SnapshotRawEntry>,
    /// R-RFT-06：changelog 压缩水位（`META_COMPACT_REVISION`，0 = 未压缩）
    pub compacted_revision: u64,
}

/// R-RFT-06：快照中的原始内部条目（非用户 KV 域）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotRawEntry {
    /// 内部存储 key（含 `/_sys/auth/` 或 `/_lease/` 前缀）
    pub internal_key: Vec<u8>,
    /// 原始 value 字节（密文/序列化字节，快照不接触明文）
    pub value: Vec<u8>,
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
    /// 当前快照格式版本（P0-A：版本号 +1；0.1.x 数据不承诺兼容）
    /// v3（R-RFT-06）：新增 auth/lease 域 + compacted 水位，导入写完整 LogId。
    const CURRENT_VERSION: u32 = 3;

    /// 创建空快照
    pub fn new(last_included_index: u64, last_included_term: u64) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            last_included_index,
            last_included_term,
            next_revision: 1,
            applied_index: 0,
            applied_term: 0,
            applied_node_id: 0,
            kv_pairs: Vec::new(),
            kv_metadata: Vec::new(),
            auth_entries: Vec::new(),
            lease_entries: Vec::new(),
            compacted_revision: 0,
        }
    }

    /// 序列化为字节（用于网络传输和磁盘存储）
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| Error::Internal(format!("snapshot serialize: {e}")))
    }

    /// 从字节反序列化
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        bincode::deserialize(data)
            .map_err(|e| Error::Internal(format!("snapshot deserialize: {e}")))
    }

    /// R-TST-21：反序列化 + 旧格式迁移（数据格式升级兼容）。
    ///
    /// 直接解析成功且版本匹配 → 原样返回；否则尝试 v2 格式迁移
    /// （v2 = R-RFT-06 之前：无 auth/lease 域、无 compacted 水位、
    /// applied term/node_id 不持久化）。迁移结果：域置空、水位 0、
    /// applied term/node_id 回退 0（与 v2 运行时语义一致）。
    pub fn from_bytes_migrating(data: &[u8]) -> Result<Self> {
        match bincode::deserialize::<Self>(data) {
            Ok(snapshot) if snapshot.version == Self::CURRENT_VERSION => Ok(snapshot),
            Ok(snapshot) => Err(Error::Internal(format!(
                "unsupported snapshot version: {} (expected {})",
                snapshot.version,
                Self::CURRENT_VERSION
            ))),
            Err(_) => {
                // 尝试 v2 迁移
                let v2: SnapshotDataV2 = bincode::deserialize(data)
                    .map_err(|e| Error::Internal(format!("snapshot deserialize (v3+v2): {e}")))?;
                if v2.version != 2 {
                    return Err(Error::Internal(format!(
                        "unsupported snapshot version: {} (expected 2 or {})",
                        v2.version,
                        Self::CURRENT_VERSION
                    )));
                }
                tracing::warn!(
                    "snapshot v2 detected; migrating to v{} (auth/lease empty, compacted=0, applied standalone)",
                    Self::CURRENT_VERSION
                );
                Ok(Self::migrate_v2_to_v3(v2))
            }
        }
    }

    /// v2 → v3 迁移：补空 auth/lease 域、水位 0、applied term/node_id 回退 0。
    fn migrate_v2_to_v3(v2: SnapshotDataV2) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            last_included_index: v2.last_included_index,
            last_included_term: v2.last_included_term,
            next_revision: v2.next_revision,
            applied_index: v2.applied_index,
            applied_term: 0,
            applied_node_id: 0,
            kv_pairs: v2.kv_pairs,
            kv_metadata: v2.kv_metadata,
            auth_entries: Vec::new(),
            lease_entries: Vec::new(),
            compacted_revision: 0,
        }
    }
}

/// R-TST-21：v2 快照格式（R-RFT-06 之前）。字段顺序与 v2 时点一致，
/// 仅用于旧数据升级迁移，不参与导出。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotDataV2 {
    version: u32,
    last_included_index: u64,
    last_included_term: u64,
    next_revision: u64,
    applied_index: u64,
    kv_pairs: Vec<SnapshotKvPair>,
    kv_metadata: Vec<SnapshotKvMeta>,
}

// ──── 导出/导入函数 ────

/// 从 MvccStorage 导出快照数据
///
/// 遍历所有 KV 数据和元数据，包含 Raft 检查点。
/// 导出的 Value 是加密后的密文（不经过 Barrier 解密），保证 Snapshot 不接触明文。
///
/// R-RFT-06：
/// - **单读事务**导出全部表（redb 读事务提供一致性视图），消除此前
///   applied / kv / kv_meta 三次独立读事务的撕裂快照；
/// - 补充 auth 域（`/_sys/auth/*`）、lease 域（`/_lease/*`）与 compacted 水位。
pub fn export_snapshot_data<B: StorageBackend>(
    storage: &MvccStorage<B>,
    last_included_index: u64,
    last_included_term: u64,
) -> Result<SnapshotData> {
    let mut data = SnapshotData::new(last_included_index, last_included_term);

    let backend = storage.backend();
    let (applied_bytes, compacted_bytes, kv_rows, meta_rows, auth_rows, lease_rows) = backend
        .read(|tx| {
            let applied = tx.get(TABLE_META, META_LAST_APPLIED)?;
            let compacted = tx.get(TABLE_META, META_COMPACT_REVISION)?;
            let kv_prefix = encode_kv_key(b"");
            let kv_rows = tx.iter_prefix(TABLE_KV, &kv_prefix)?;
            let meta_prefix = encode_kv_meta_key(b"");
            let meta_rows = tx.iter_prefix(TABLE_KV_META, &meta_prefix)?;
            // R-RFT-06：auth / lease 域随快照导出（恢复后用户/角色/会话/租约不丢）
            let auth_rows = tx.iter_prefix(TABLE_KV, b"/_sys/auth/")?;
            let lease_rows = tx.iter_prefix(TABLE_KV, b"/_lease/")?;
            Ok((
                applied, compacted, kv_rows, meta_rows, auth_rows, lease_rows,
            ))
        })?;

    let applied = applied_bytes.as_deref().and_then(AppliedLogId::from_bytes);
    data.applied_index = applied.map(|a| a.index).unwrap_or(last_included_index);
    data.applied_term = applied.map(|a| a.term).unwrap_or(0);
    data.applied_node_id = applied.map(|a| a.node_id).unwrap_or(0);
    data.next_revision = data.applied_index.saturating_add(1);
    data.compacted_revision = compacted_bytes
        .as_deref()
        .and_then(|b| {
            let arr: [u8; 8] = b.try_into().ok()?;
            Some(u64::from_be_bytes(arr))
        })
        .unwrap_or(0);

    // 导出 KV 数据（密文，直接读取不经过 Barrier）
    for (internal_key, value) in kv_rows.into_iter() {
        if let Some(user_key) = super::mvcc::decode_kv_key(&internal_key) {
            data.kv_pairs.push(SnapshotKvPair {
                key: user_key.to_vec(),
                value,
            });
        }
    }

    // 导出 KV 元数据
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

    // R-RFT-06：auth / lease 原始条目
    data.auth_entries = auth_rows
        .into_iter()
        .map(|(internal_key, value)| SnapshotRawEntry {
            internal_key,
            value,
        })
        .collect();
    data.lease_entries = lease_rows
        .into_iter()
        .map(|(internal_key, value)| SnapshotRawEntry {
            internal_key,
            value,
        })
        .collect();

    Ok(data)
}

/// 将快照数据导入到 MvccStorage
///
/// 清空现有数据后写入快照中的全部 KV 数据、元数据、auth/lease 域。
/// Barrier 加密/解密不介入——快照导入的是原始密文。
///
/// R-RFT-06：
/// - `META_LAST_APPLIED` 写入快照携带的完整 LogId（term/node_id/index），
///   此前 `AppliedLogId::standalone` 将 term/node_id 置零；
/// - 恢复 compacted 水位，保证压缩语义跨快照一致。
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
        // 清空 KV 表全表（含 /_kv/、/_sys/auth/、/_lease/ 所有域）
        let existing_kv: Vec<Vec<u8>> = tx
            .iter_prefix(TABLE_KV, b"")?
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        for key in &existing_kv {
            tx.remove(TABLE_KV, key)?;
        }

        // 清空 KV 元数据表
        let existing_meta: Vec<Vec<u8>> = tx
            .iter_prefix(TABLE_KV_META, b"")?
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

        // R-RFT-06：恢复 auth / lease 域原始条目
        for entry in &data.auth_entries {
            tx.insert(TABLE_KV, &entry.internal_key, &entry.value)?;
        }
        for entry in &data.lease_entries {
            tx.insert(TABLE_KV, &entry.internal_key, &entry.value)?;
        }

        // R-RFT-06：持久化 applied 状态（完整 term/node_id，来自快照导出时的真实 LogId）
        tx.insert(
            TABLE_META,
            META_LAST_APPLIED,
            &AppliedLogId {
                term: data.applied_term,
                node_id: data.applied_node_id,
                index: data.applied_index,
            }
            .to_bytes(),
        )?;

        // R-RFT-06：恢复 compacted 水位
        if data.compacted_revision > 0 {
            tx.insert(
                TABLE_META,
                META_COMPACT_REVISION,
                &data.compacted_revision.to_be_bytes(),
            )?;
        } else {
            tx.remove(TABLE_META, META_COMPACT_REVISION)?;
        }

        Ok(())
    })?;

    Ok(())
}

// ──── SnapshotTracker：purge 前置条件守卫（M0-5） ────

/// 已持久化到磁盘的快照元数据（供 LogStore::purge 前置校验与启动检查）
#[derive(Debug, Clone)]
pub struct DurableSnapshot {
    pub index: u64,
    pub term: u64,
    pub path: PathBuf,
}

/// 记录"最新一份已落盘（fsync + 原子 rename）快照"的共享状态
///
/// StateMachineStore 在快照文件持久化成功后调用 `record_durable`；
/// LogStore::purge 在删除日志前调用 `durable_covers` 校验（openraft 仅在
/// 快照构建成功后触发 purge，守卫保证"无快照 + 日志已删"的不可恢复状态不出现）。
#[derive(Debug, Default)]
pub struct SnapshotTracker {
    durable: Mutex<Option<DurableSnapshot>>,
}

impl SnapshotTracker {
    /// 记录一份已持久化快照（仅当 index 不小于当前记录时覆盖）
    pub fn record_durable(&self, index: u64, term: u64, path: PathBuf) {
        let mut durable = self.durable.lock();
        let should_replace = durable.as_ref().map(|d| index >= d.index).unwrap_or(true);
        if should_replace {
            *durable = Some(DurableSnapshot { index, term, path });
        }
    }

    /// 是否存在覆盖指定 index 的持久化快照（S-RCV-01：同时校验文件仍在磁盘上）
    pub fn durable_covers(&self, index: u64) -> bool {
        self.durable
            .lock()
            .as_ref()
            .map(|d| d.index >= index && d.path.is_file())
            .unwrap_or(false)
    }

    /// 读取当前记录的持久化快照
    pub fn latest(&self) -> Option<DurableSnapshot> {
        self.durable.lock().clone()
    }
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
        storage
            .put(b"/service/addr", b"127.0.0.1:8080", None)
            .unwrap();

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

    // ──── R-RFT-06：auth/lease 域与完整 LogId、compacted 水位 ────

    #[test]
    fn test_snapshot_preserves_auth_lease_and_full_logid() {
        let (_tmp, storage) = setup_storage();
        storage.put(b"/user/key", b"v", None).unwrap();

        // 直接写 auth / lease 域原始条目（模拟 raft apply 后的持久化状态）
        let backend = storage.backend();
        backend
            .write(|tx| {
                tx.insert(TABLE_KV, b"/_sys/auth/user/alice", b"hash-bytes")?;
                tx.insert(TABLE_KV, b"/_lease/42", &[1u8, 2, 3])?;
                Ok(())
            })
            .unwrap();

        // 模拟完整 applied LogId（term/node_id 非零）
        backend
            .write(|tx| {
                tx.insert(
                    TABLE_META,
                    META_LAST_APPLIED,
                    &AppliedLogId {
                        term: 7,
                        node_id: 3,
                        index: 9,
                    }
                    .to_bytes(),
                )?;
                Ok(())
            })
            .unwrap();

        let data = export_snapshot_data(&storage, 9, 7).unwrap();
        assert_eq!(data.applied_term, 7);
        assert_eq!(data.applied_node_id, 3);
        assert_eq!(data.applied_index, 9);
        assert_eq!(data.auth_entries.len(), 1);
        assert_eq!(data.lease_entries.len(), 1);

        // 导入新 storage
        let tmp2 = TempDir::new().unwrap();
        let backend2 = RedbBackend::open(tmp2.path(), &StorageConfig::default()).unwrap();
        let storage2 = MvccStorage::new(backend2).unwrap();
        import_snapshot_data(&storage2, &data).unwrap();

        // auth / lease 域逐字段一致
        let backend2 = storage2.backend();
        let auth_val = backend2
            .read(|tx| tx.get(TABLE_KV, b"/_sys/auth/user/alice"))
            .unwrap()
            .unwrap();
        assert_eq!(auth_val, b"hash-bytes".to_vec());
        let lease_val = backend2
            .read(|tx| tx.get(TABLE_KV, b"/_lease/42"))
            .unwrap()
            .unwrap();
        assert_eq!(lease_val, vec![1u8, 2, 3]);

        // applied 恢复完整 LogId（此前 standalone 将 term/node_id 置零）
        let applied = storage2.get_applied_log_id().unwrap().unwrap();
        assert_eq!(applied.term, 7);
        assert_eq!(applied.node_id, 3);
        assert_eq!(applied.index, 9);
    }

    #[test]
    fn test_snapshot_compacted_revision_roundtrip() {
        let (_tmp, storage) = setup_storage();
        storage.put(b"/a", b"1", None).unwrap();

        // 写入 compacted 水位
        storage
            .backend()
            .write(|tx| {
                tx.insert(TABLE_META, META_COMPACT_REVISION, &42u64.to_be_bytes())?;
                Ok(())
            })
            .unwrap();

        let data = export_snapshot_data(&storage, 1, 1).unwrap();
        assert_eq!(data.compacted_revision, 42);

        let tmp2 = TempDir::new().unwrap();
        let backend2 = RedbBackend::open(tmp2.path(), &StorageConfig::default()).unwrap();
        let storage2 = MvccStorage::new(backend2).unwrap();
        import_snapshot_data(&storage2, &data).unwrap();
        assert_eq!(storage2.compacted_revision().unwrap(), 42);

        // 未压缩的快照导入后 compacted 保持 0
        let data0 = export_snapshot_data(&storage2, 1, 1).unwrap();
        let tmp3 = TempDir::new().unwrap();
        let backend3 = RedbBackend::open(tmp3.path(), &StorageConfig::default()).unwrap();
        let storage3 = MvccStorage::new(backend3).unwrap();
        // 构造无压缩水位的数据
        let mut data_empty = data0.clone();
        data_empty.compacted_revision = 0;
        import_snapshot_data(&storage3, &data_empty).unwrap();
        assert_eq!(storage3.compacted_revision().unwrap(), 0);
    }

    // ──── R-TST-21：数据格式升级兼容（v2 → v3 迁移）────

    #[test]
    fn test_snapshot_v2_upgrade_migration() {
        // 构造一条 v2 格式快照（R-RFT-06 之前的字段序，无 auth/lease/水位）
        let v2 = SnapshotDataV2 {
            version: 2,
            last_included_index: 11,
            last_included_term: 6,
            next_revision: 12,
            applied_index: 11,
            kv_pairs: vec![SnapshotKvPair {
                key: b"/legacy/key".to_vec(),
                value: b"legacy-value".to_vec(),
            }],
            kv_metadata: vec![SnapshotKvMeta {
                key: b"/legacy/key".to_vec(),
                version: 1,
                create_revision: 3,
                mod_revision: 3,
                lease_id: 0,
                deleted: false,
            }],
        };
        let v2_bytes = bincode::serialize(&v2).unwrap();

        // 直接解析（v3 结构）失败 → 迁移路径成功
        assert!(SnapshotData::from_bytes(&v2_bytes).is_err());
        let migrated = SnapshotData::from_bytes_migrating(&v2_bytes).expect("v2 快照应可迁移为 v3");
        assert_eq!(migrated.version, 3);
        assert_eq!(migrated.last_included_index, 11);
        assert_eq!(migrated.last_included_term, 6);
        assert_eq!(migrated.applied_index, 11);
        assert_eq!(migrated.applied_term, 0, "v2 无 applied term → 回退 0");
        assert_eq!(migrated.applied_node_id, 0);
        assert!(migrated.auth_entries.is_empty());
        assert!(migrated.lease_entries.is_empty());
        assert_eq!(migrated.compacted_revision, 0);
        assert_eq!(migrated.kv_pairs.len(), 1);
        assert_eq!(migrated.kv_pairs[0].value, b"legacy-value");

        // 迁移后的 v3 快照可正常导入恢复
        let tmp = TempDir::new().unwrap();
        let backend = RedbBackend::open(tmp.path(), &StorageConfig::default()).unwrap();
        let storage = MvccStorage::new(backend).unwrap();
        import_snapshot_data(&storage, &migrated).unwrap();
        assert_eq!(
            storage.get(b"/legacy/key").unwrap(),
            Some(b"legacy-value".to_vec())
        );
        let applied = storage.get_applied_log_id().unwrap().unwrap();
        assert_eq!(applied.index, 11);

        // 未知版本拒绝（v1 等）
        let mut v1 = v2.clone();
        v1.version = 1;
        let v1_bytes = bincode::serialize(&v1).unwrap();
        assert!(SnapshotData::from_bytes_migrating(&v1_bytes).is_err());
    }
}
