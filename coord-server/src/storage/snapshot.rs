// Snapshot 导出/导入
//
// 实现 的状态机快照能力：
// - export_snapshot_data: 从 MvccStorage 导出全量数据
// - import_snapshot_data: 将快照数据恢复到 MvccStorage
//
// 快照格式：统一信封 V2（`MAGIC | VERSION_V2 | postcard`），读写一致；
// V1 / 无前缀历史行已退役（显式拒绝，无迁移阶梯）。包含所有 KV 数据、
// 元数据和 Raft 检查点。

use std::path::PathBuf;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use coord_core::error::{Error, Result};
use coord_core::storage::StorageBackend;

use super::envelope;
use super::mvcc::{
    encode_kv_key, encode_kv_meta_key, AppliedLogId, KvMetadata, MvccStorage, CHANGELOG_PREFIX,
    META_COMPACT_REVISION, META_LAST_APPLIED, TABLE_CHANGELOG, TABLE_KV, TABLE_KV_META, TABLE_META,
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
    /// region 0 PD 全局队列域原始条目（`/_pd/*` → bytes）。
    ///
    /// region 0 状态机含 `/_pd/ops/{op_id}` operator 治理记录（全局去重
    /// 队列）。快照必须携带该域——导入时 `TABLE_KV` 全表清空重灌，缺此域会让
    /// 快照追平/恢复把队列清空（op 丢失、调度停滞）。
    pub pd_entries: Vec<SnapshotRawEntry>,
    /// region 0 其余 system 域原始条目（`/_sys/*` 中不属于 `/_sys/auth/*`
    /// 的行——迁移标记 `/_sys/migration/legacy-v1` 等）。恢复后启动闸/迁移
    /// 状态跨快照不丢。
    pub sys_entries: Vec<SnapshotRawEntry>,
    /// changelog 窗口原始条目（key = `/_changelog/{rev_be}`，value = `ChangeEvent`）。
    ///
    /// 快照必须携带**源节点当前持有的全部 changelog 行**（`apply_compact` 的
    /// 保留窗口 `>= compacted_revision`）：装快照会把 KV 状态整体替换到 S，而
    /// 历史读（`get_at_revision` / `range_at_revision`）与 watch 重放
    /// （`read_changelog_from`）都从 `TABLE_CHANGELOG` 重建。不携带（且导入不
    /// 清空）时，本地旧条目与新状态之间会形成「洞」(旧水位, S]，洞内 target 的
    /// 历史读会**静默返回前值**（与实时读不一致）。导入时**整体替换**（先清空再
    /// 灌入）。
    pub changelog_entries: Vec<SnapshotRawEntry>,
}

/// R-RFT-06：快照中的原始内部条目（非用户 KV 域）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotRawEntry {
    /// 内部存储 key（`/_sys/auth/`、`/_lease/`、`/_pd/`、`/_sys/` 等内部前缀）
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
    /// 当前快照格式版本（版本号 +1；0.1.x 数据不承诺兼容）。
    /// v5：携带 changelog 窗口（`changelog_entries`）——装快照后历史读 /
    ///   watch 重放不再出现「洞」（静默返回前值）。v2–v4 格式已随 bincode
    ///   退场退役：`from_bytes` 仅接受 v5，旧字节显式拒绝（无迁移阶梯）。
    const CURRENT_VERSION: u32 = 5;

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
            pd_entries: Vec::new(),
            sys_entries: Vec::new(),
            changelog_entries: Vec::new(),
        }
    }

    /// 序列化为字节（用于网络传输和磁盘存储；统一信封 V2-postcard，内部
    /// `version` 字段仍为 [`Self::CURRENT_VERSION`]。
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        envelope::encode(self).map_err(|e| Error::Internal(format!("snapshot serialize: {e}")))
    }

    /// 从字节反序列化（唯一格式：信封 V2 / postcard；内部 `version` 必须为
    /// [`Self::CURRENT_VERSION`]）。
    ///
    /// P3 后快照无迁移阶梯：V1 / 无前缀历史行显式拒绝（不得按旧结构试解）。
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let snapshot: Self = envelope::decode(data)
            .map_err(|e| Error::Internal(format!("snapshot deserialize: {e}")))?;
        if snapshot.version != Self::CURRENT_VERSION {
            return Err(Error::Internal(format!(
                "unsupported snapshot version: {} (expected {})",
                snapshot.version,
                Self::CURRENT_VERSION
            )));
        }
        Ok(snapshot)
    }
}

// ──── 导出/导入函数 ────

/// 从 MvccStorage 导出快照数据
///
/// 遍历所有 KV 数据和元数据，包含 Raft 检查点。
/// 导出的 Value 是加密后的密文（不经过 Barrier 解密），保证 Snapshot 不接触明文。
///
/// R-RFT-06：
/// - **单读事务**导出全部表（redb 读事务提供一致性视图），消除
///   applied / kv / kv_meta 三次独立读事务的撕裂快照；
/// - 补充 auth 域（`/_sys/auth/*`）、lease 域（`/_lease/*`）与 compacted 水位。
///
///
/// - 补充 region 0 `/_pd/*`（PD 全局队列）与 `/_sys/*` 非 auth 域（迁移标记
///   等）——region 0 状态机内部记录随快照导出，导入/追平不丢。
pub fn export_snapshot_data<B: StorageBackend>(
    storage: &MvccStorage<B>,
    last_included_index: u64,
    last_included_term: u64,
) -> Result<SnapshotData> {
    let mut data = SnapshotData::new(last_included_index, last_included_term);

    let backend = storage.backend();
    let (
        applied_bytes,
        compacted_bytes,
        kv_rows,
        meta_rows,
        auth_rows,
        lease_rows,
        pd_rows,
        sys_rows,
        changelog_rows,
    ) = backend.read(|tx| {
        let applied = tx.get(TABLE_META, META_LAST_APPLIED)?;
        let compacted = tx.get(TABLE_META, META_COMPACT_REVISION)?;
        let kv_prefix = encode_kv_key(b"");
        let kv_rows = tx.iter_prefix(TABLE_KV, &kv_prefix)?;
        let meta_prefix = encode_kv_meta_key(b"");
        let meta_rows = tx.iter_prefix(TABLE_KV_META, &meta_prefix)?;
        // R-RFT-06：auth / lease 域随快照导出（恢复后用户/角色/会话/租约不丢）
        let auth_rows = tx.iter_prefix(TABLE_KV, b"/_sys/auth/")?;
        let lease_rows = tx.iter_prefix(TABLE_KV, b"/_lease/")?;
        // region 0 PD 队列 / 其余 system 域随快照导出
        let pd_rows = tx.iter_prefix(TABLE_KV, b"/_pd/")?;
        let sys_rows = tx.iter_prefix(TABLE_KV, b"/_sys/")?;
        // changelog 窗口（`apply_compact` 保留的 `>= compacted` 行）——
        // 装快照后的历史读/watch 重放依赖它，不能缺席（见 SnapshotData 字段注释）。
        let changelog_rows = tx.iter_prefix(TABLE_CHANGELOG, CHANGELOG_PREFIX)?;
        Ok((
            applied,
            compacted,
            kv_rows,
            meta_rows,
            auth_rows,
            lease_rows,
            pd_rows,
            sys_rows,
            changelog_rows,
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

    // `/_pd/*` 全量；`/_sys/*` 中去掉已入 auth 域的行
    // （`/_sys/auth/*`），其余（迁移标记等）进 sys_entries。
    data.pd_entries = pd_rows
        .into_iter()
        .map(|(internal_key, value)| SnapshotRawEntry {
            internal_key,
            value,
        })
        .collect();
    data.sys_entries = sys_rows
        .into_iter()
        .filter(|(internal_key, _)| !internal_key.starts_with(b"/_sys/auth/"))
        .map(|(internal_key, value)| SnapshotRawEntry {
            internal_key,
            value,
        })
        .collect();

    // changelog 窗口（原样导出/灌入；导入侧先清空再回填）
    data.changelog_entries = changelog_rows
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
/// - `META_LAST_APPLIED` 写入快照携带的完整 LogId（term/node_id/index）；
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

        // 清空本地 changelog —— 快照携带的是**源节点**的窗口；不清空会让
        // 本地旧条目（旧窗口）与新状态混在一起，在 (旧水位, S] 区间形成「洞」，
        // 洞内 target 的历史读会静默返回前值。
        let existing_changelog: Vec<Vec<u8>> = tx
            .iter_prefix(TABLE_CHANGELOG, CHANGELOG_PREFIX)?
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        for key in &existing_changelog {
            tx.remove(TABLE_CHANGELOG, key)?;
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

        // 恢复 region 0 `/_pd/*`（PD 队列）与 `/_sys/*`
        // 非 auth 域（迁移标记等）原始条目——TABLE_KV 全表清空后一并回填。
        for entry in &data.pd_entries {
            tx.insert(TABLE_KV, &entry.internal_key, &entry.value)?;
        }
        for entry in &data.sys_entries {
            tx.insert(TABLE_KV, &entry.internal_key, &entry.value)?;
        }

        // 灌入快照携带的 changelog 窗口（“整体替换”的第二步；对 v4 迁移
        // 快照为空窗口，此时旧条目已在上面清空、compacted 水位已抬到 applied）。
        for entry in &data.changelog_entries {
            tx.insert(TABLE_CHANGELOG, &entry.internal_key, &entry.value)?;
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

// ──── SnapshotTracker：purge 前置条件守卫 ────

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

        // applied 恢复完整 LogId（不得用 standalone 的置零 term/node_id）
        let applied = storage2.get_applied_log_id().unwrap().unwrap();
        assert_eq!(applied.term, 7);
        assert_eq!(applied.node_id, 3);
        assert_eq!(applied.index, 9);
    }

    // ──── region 0 `/_pd/` 与 `/_sys/`（非 auth）域 ────

    #[test]
    fn test_snapshot_preserves_pd_queue_and_sys_domains() {
        let (_tmp, storage) = setup_storage();

        // 直接写 region 0 内部域原始条目：`/_pd/` 队列 + `/_sys/auth/`（已有
        // 域，确认不重复进 sys）+ `/_sys/migration/` 标记
        let backend = storage.backend();
        backend
            .write(|tx| {
                tx.insert(TABLE_KV, b"/_pd/ops/0000000000000001", b"pd-entry-1")?;
                tx.insert(TABLE_KV, b"/_pd/ops/0000000000000002", b"pd-entry-2")?;
                tx.insert(TABLE_KV, b"/_sys/auth/user/alice", b"hash-bytes")?;
                tx.insert(TABLE_KV, b"/_sys/migration/legacy-v1", b"done")?;
                Ok(())
            })
            .unwrap();

        let data = export_snapshot_data(&storage, 9, 7).unwrap();
        assert_eq!(data.auth_entries.len(), 1, "auth domain unchanged");
        assert!(data.lease_entries.is_empty());
        assert_eq!(data.pd_entries.len(), 2, "pd queue domain exported");
        assert_eq!(
            data.sys_entries.len(),
            1,
            "non-auth /_sys/ rows (migration marker) exported"
        );
        assert_eq!(
            data.sys_entries[0].internal_key, b"/_sys/migration/legacy-v1",
            "auth rows must NOT leak into sys_entries"
        );

        // 导入新 storage → 内部域逐字段一致
        let tmp2 = TempDir::new().unwrap();
        let backend2 = RedbBackend::open(tmp2.path(), &StorageConfig::default()).unwrap();
        let storage2 = MvccStorage::new(backend2).unwrap();
        import_snapshot_data(&storage2, &data).unwrap();

        let backend2 = storage2.backend();
        let pd1 = backend2
            .read(|tx| tx.get(TABLE_KV, b"/_pd/ops/0000000000000001"))
            .unwrap()
            .unwrap();
        assert_eq!(pd1, b"pd-entry-1".to_vec());
        let pd2 = backend2
            .read(|tx| tx.get(TABLE_KV, b"/_pd/ops/0000000000000002"))
            .unwrap()
            .unwrap();
        assert_eq!(pd2, b"pd-entry-2".to_vec());
        let marker = backend2
            .read(|tx| tx.get(TABLE_KV, b"/_sys/migration/legacy-v1"))
            .unwrap()
            .unwrap();
        assert_eq!(marker, b"done".to_vec());
        let auth = backend2
            .read(|tx| tx.get(TABLE_KV, b"/_sys/auth/user/alice"))
            .unwrap()
            .unwrap();
        assert_eq!(auth, b"hash-bytes".to_vec());

        // 序列化往返（当前版本直接解析）
        let bytes = data.to_bytes().unwrap();
        let restored = SnapshotData::from_bytes(&bytes).unwrap();
        assert_eq!(restored.version, SnapshotData::CURRENT_VERSION);
        assert_eq!(restored.pd_entries.len(), 2);
        assert_eq!(restored.sys_entries.len(), 1);
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

    // ──── 退役格式（P3）：旧字节显式拒绝 ────

    /// 退役锚点：V1 信封行 / 无前缀历史行 / 未知版本 ⇒ 显式拒绝
    /// （不得按旧结构试解，无迁移阶梯）。
    /// 负控制：恢复任一历史读腿 ⇒ 本用例必红。
    #[test]
    fn test_snapshot_retired_formats_rejected() {
        // V1 信封行（历史写路径产物）
        let mut v1 = Vec::new();
        v1.extend_from_slice(&envelope::MAGIC);
        v1.push(1);
        v1.extend_from_slice(&[0xAA, 0xBB, 0xCC]);
        assert!(
            SnapshotData::from_bytes(&v1).is_err(),
            "V1 快照行必须显式拒绝"
        );

        // 无前缀历史行
        let legacy = vec![0x05u8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        assert!(
            SnapshotData::from_bytes(&legacy).is_err(),
            "无前缀快照行必须显式拒绝"
        );

        // 未知版本字节
        let mut unknown = Vec::new();
        unknown.extend_from_slice(&envelope::MAGIC);
        unknown.push(9);
        unknown.extend_from_slice(&[0xAA]);
        assert!(
            SnapshotData::from_bytes(&unknown).is_err(),
            "未知信封版本必须显式拒绝"
        );
    }

    // ──── 装快照后洞内历史读（判据）────

    /// 判据（正）：快照携带 changelog 窗口 ⇒ 装快照后洞内 target 的历史读
    /// 返回**真值**。
    ///
    /// 现场形态：A 写 K=v_old(r1) / K=v_new(r3)；B 只 apply 到 r1 后导入 A 的
    /// 快照（S≥r3）。洞内 target 若不携带 changelog，`B.get_at_revision(K, r3)`
    /// 会返回 v_old（静默错答）；必须返回 v_new。
    #[test]
    fn test_snapshot_carries_changelog_gap_reads_return_correct_value() {
        let (_tmp_a, storage_a) = setup_storage();
        // r1..r4（含其他 key，模拟真实日志密度）
        storage_a
            .put_at_revision(b"/k", b"v_old", None, 1, AppliedLogId::standalone(1))
            .unwrap();
        storage_a
            .put_at_revision(b"/other", b"x", None, 2, AppliedLogId::standalone(2))
            .unwrap();
        storage_a
            .put_at_revision(b"/k", b"v_new", None, 3, AppliedLogId::standalone(3))
            .unwrap();
        storage_a
            .put_at_revision(b"/other", b"y", None, 4, AppliedLogId::standalone(4))
            .unwrap();

        let data = export_snapshot_data(&storage_a, 5, 1).unwrap();
        assert!(
            !data.changelog_entries.is_empty(),
            "快照必须携带 changelog 窗口（不得为空）"
        );

        // B：只 apply 到 r1（K=v_old）；(1, S] 对 B 原本是「洞」
        let tmp_b = TempDir::new().unwrap();
        let backend_b = RedbBackend::open(tmp_b.path(), &StorageConfig::default()).unwrap();
        let storage_b = MvccStorage::new(backend_b).unwrap();
        storage_b
            .put_at_revision(b"/k", b"v_old", None, 1, AppliedLogId::standalone(1))
            .unwrap();

        import_snapshot_data(&storage_b, &data).unwrap();

        // 洞内 target：必须返回 v_new（不得静默返回 v_old）
        assert_eq!(
            storage_b.get_at_revision(b"/k", 3).unwrap(),
            Some(b"v_new".to_vec()),
            "装快照后洞内 target 的历史读必须返回真值"
        );
        // 边界：r2 之前仍是 v_old；r1 亦然
        assert_eq!(
            storage_b.get_at_revision(b"/k", 2).unwrap(),
            Some(b"v_old".to_vec())
        );
        assert_eq!(
            storage_b.get_at_revision(b"/k", 1).unwrap(),
            Some(b"v_old".to_vec())
        );
        // watch 重放：从 r2 起可读到 r3 的事件（该区间不得缺失）
        let events = storage_b.read_changelog_entries_strict(2).unwrap();
        assert!(
            events.iter().any(|e| e.revision == 3),
            "快照窗口内的事件必须可回放（watch resume）"
        );
        // 启动一致性校验：applied 与 changelog 尾部一致（不再断链）
        let (applied, tail) = storage_b.verify_consistency().unwrap();
        assert_eq!(applied, 4);
        assert_eq!(tail, Some(4), "changelog 尾部应覆盖到最新 apply");
    }

    // ──── 格式信封（P3：唯一 V2 + 精确消费）────

    fn envelope_sample_snapshot() -> SnapshotData {
        let mut data = SnapshotData::new(7, 3);
        data.next_revision = 8;
        data.applied_index = 7;
        data.applied_term = 3;
        data.applied_node_id = 1;
        data.kv_pairs = vec![SnapshotKvPair {
            key: b"/k".to_vec(),
            value: b"v".to_vec(),
        }];
        data.changelog_entries = vec![SnapshotRawEntry {
            internal_key: b"/_changelog/0000000000000007".to_vec(),
            value: b"ev".to_vec(),
        }];
        data
    }

    /// V2 行（postcard，唯一格式，仅承载 v5）⇒ 解码成功。
    /// 负控制：删除 V2 读腿 ⇒ 本用例必红。
    #[test]
    fn test_snapshot_v2_row_decodes() {
        let data = envelope_sample_snapshot();
        let bytes = envelope::encode(&data).unwrap();

        let restored = SnapshotData::from_bytes(&bytes).expect("V2 快照必须可解码");
        assert_eq!(restored.version, SnapshotData::CURRENT_VERSION);
        assert_eq!(restored.last_included_index, 7);
        assert_eq!(restored.kv_pairs.len(), 1);
        assert_eq!(restored.changelog_entries.len(), 1);
        assert_eq!(restored.applied_term, 3);
    }

    /// V2 行版本不是 v5 ⇒ 显式报错（无迁移阶梯，不得按旧结构试解）。
    /// 负控制：去掉 `version == CURRENT_VERSION` 检查 ⇒ 本用例必红。
    #[test]
    fn test_snapshot_v2_wrong_version_rejected() {
        let mut data = envelope_sample_snapshot();
        data.version = 4;
        let bytes = envelope::encode(&data).unwrap();

        let err = SnapshotData::from_bytes(&bytes).expect_err("version != 5 必须显式报错");
        assert!(
            format!("{err:?}").contains("unsupported snapshot version"),
            "错误必须是版本不受支持，实际：{err:?}"
        );
    }

    /// V2 行尾随字节 ⇒ 显式失败（精确消费）。
    /// 负控制：去掉 postcard remainder 空断言 ⇒ 本用例必红。
    #[test]
    fn test_snapshot_v2_trailing_bytes_rejected() {
        let data = envelope_sample_snapshot();
        let mut bytes = envelope::encode(&data).unwrap();
        bytes.extend_from_slice(&[0xDE, 0xAD]);
        assert!(SnapshotData::from_bytes(&bytes).is_err());
    }

    /// V2 行魔数 / 版本字节篡改 ⇒ 显式失败（不得按旧格式静默解出偏差值）。
    /// 负控制：去掉魔数比较 / 版本检查放宽 ⇒ 本用例必红。
    #[test]
    fn test_snapshot_v2_tampered_prefix_rejected() {
        let data = envelope_sample_snapshot();

        let mut magic = envelope::encode(&data).unwrap();
        magic[0] = 0x03;
        assert!(
            SnapshotData::from_bytes(&magic).is_err(),
            "魔数破坏后不得静默解出快照"
        );

        let mut version = envelope::encode(&data).unwrap();
        version[envelope::MAGIC.len()] = 9;
        let err = SnapshotData::from_bytes(&version).expect_err("未知版本必须显式报错");
        assert!(
            format!("{err:?}").contains("unsupported envelope version"),
            "错误必须是未知信封版本，实际：{err:?}"
        );
    }

    /// V2 行截断 ⇒ 显式失败。
    #[test]
    fn test_snapshot_v2_truncated_rejected() {
        let data = envelope_sample_snapshot();
        let bytes = envelope::encode(&data).unwrap();
        assert!(SnapshotData::from_bytes(&bytes[..bytes.len() - 1]).is_err());
    }

    /// 写路径断言：`to_bytes` 产物必须带 `MAGIC + VERSION_V2` 前缀（唯一格式）。
    /// 负控制：写路径回退 V1 版本字节 ⇒ 本用例必红。
    #[test]
    fn test_snapshot_to_bytes_writes_v2_envelope() {
        let data = envelope_sample_snapshot();
        let bytes = data.to_bytes().unwrap();
        assert!(
            bytes.starts_with(&envelope::MAGIC),
            "快照写产物必须带信封魔数"
        );
        assert_eq!(
            bytes[envelope::MAGIC.len()],
            envelope::VERSION_V2,
            "快照写产物必须为 V2 信封"
        );
        let restored = SnapshotData::from_bytes(&bytes).unwrap();
        assert_eq!(restored.last_included_index, 7);
    }
}
