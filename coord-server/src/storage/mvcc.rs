// MVCC 版本化存储层
//
// 在 StorageBackend 之上提供版本化 KV 存储能力：
// - 全局单调递增 Revision
// - Key 空间编码（/kv/ 用户数据、/_meta/ 元数据、/_changelog/ 变更日志）
// - 基于 Revision 的快照读
// - 写入时自动生成 Changelog 条目
//
// 直接复用 Redb 内置 MVCC，本层只负责应用层语义（Revision 分配、Key 编码、
// Changelog 写入），不额外建立版本管理。

use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

use coord_core::error::{Error, Result};
use coord_core::storage::{StorageBackend, WriteTx};
use coord_core::types::{LeaseID, Revision};

use crate::security::barrier::Barrier;

// ──── Key 空间常量 ────

/// 用户 KV 数据的 Key 前缀
const KV_PREFIX: &[u8] = b"/kv/";

/// 内部元数据的 Key 前缀（P1-P3 阶段使用）
#[allow(dead_code)]
const META_PREFIX: &[u8] = b"/_meta/";

/// Lease 绑定的 Key 前缀（P2 阶段使用）
#[allow(dead_code)]
const LEASE_PREFIX: &[u8] = b"/_lease/";

/// 变更日志的 Key 前缀
const CHANGELOG_PREFIX: &[u8] = b"/_changelog/";

/// 认证数据的 Key 前缀（P2 阶段使用）
#[allow(dead_code)]
const AUTH_PREFIX: &[u8] = b"/_auth/";

// ──── Meta 子键 ────

/// 全局 Revision 计数器
pub(crate) const META_NEXT_REVISION: &[u8] = b"/_meta/next_revision";

/// 已 Apply 的最大 Raft Index（崩溃恢复检查点）
pub(crate) const META_APPLIED_INDEX: &[u8] = b"/_meta/applied_index";

/// Seal 状态：0=Unsealed, 1=Sealed, 2=Unsealing（P3 阶段使用）
#[allow(dead_code)]
const META_SEAL_STATUS: &[u8] = b"/_meta/seal_status";

/// Auth 是否启用（P2 阶段使用）
#[allow(dead_code)]
const META_AUTH_ENABLED: &[u8] = b"/_meta/auth_enabled";

// ──── 表名常量 ────

pub(crate) const TABLE_KV: &str = "kv";
pub(crate) const TABLE_META: &str = "meta";
pub(crate) const TABLE_CHANGELOG: &str = "changelog";
pub(crate) const TABLE_KV_META: &str = "kv_meta";

// ──── KV 元数据 Key 前缀 ────

/// KV 元数据的 Key 前缀：/_kv_meta/{user_key}
const KV_META_PREFIX: &[u8] = b"/_kv_meta/";

/// 将用户 Key 编码为元数据存储 Key
pub(crate) fn encode_kv_meta_key(user_key: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(KV_META_PREFIX.len() + user_key.len());
    encoded.extend_from_slice(KV_META_PREFIX);
    encoded.extend_from_slice(user_key);
    encoded
}

// ──── Key 编码工具 ────

/// 将用户 Key 编码为内部存储格式：/kv/{user_key}
pub fn encode_kv_key(user_key: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(KV_PREFIX.len() + user_key.len());
    encoded.extend_from_slice(KV_PREFIX);
    encoded.extend_from_slice(user_key);
    encoded
}

/// 将内部存储格式解码为用户 Key。若非 /kv/ 前缀则返回 None。
pub fn decode_kv_key(internal_key: &[u8]) -> Option<&[u8]> {
    internal_key.strip_prefix(KV_PREFIX)
}

/// 将 Revision 编码为 Changelog Key：/_changelog/{revision_be}
pub(crate) fn encode_changelog_key(revision: Revision) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(CHANGELOG_PREFIX.len() + 8);
    encoded.extend_from_slice(CHANGELOG_PREFIX);
    encoded.extend_from_slice(&revision.to_be_bytes());
    encoded
}

/// 将 Revision 编码为大端字节
pub fn revision_to_bytes(revision: Revision) -> [u8; 8] {
    revision.to_be_bytes()
}

// ──── ChangeEvent ────

/// 变更事件类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    Put = 0,
    Delete = 1,
    Txn = 2,
}

/// 单条 Key-Value 变更记录
#[derive(Debug, Clone)]
pub struct KeyValueChange {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub prev_value: Option<Vec<u8>>,
}

/// Changelog 条目：一条 Apply 操作产生的所有变更
#[derive(Debug, Clone)]
pub struct ChangeEvent {
    pub revision: Revision,
    pub changes: Vec<KeyValueChange>,
    pub event_type: EventType,
}

impl ChangeEvent {
    /// 序列化为字节（简化版，生产环境应使用 Protobuf）
    pub fn to_bytes(&self) -> Vec<u8> {
        // 使用简单的二进制格式：
        // revision(8BE) | event_type(1) | num_changes(4BE) | [key_len(4BE)|key|has_value(1)|value...]
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.revision.to_be_bytes());
        buf.push(self.event_type as u8);
        buf.extend_from_slice(&(self.changes.len() as u32).to_be_bytes());
        for change in &self.changes {
            buf.extend_from_slice(&(change.key.len() as u32).to_be_bytes());
            buf.extend_from_slice(&change.key);
            match &change.value {
                Some(v) => {
                    buf.push(1);
                    buf.extend_from_slice(&(v.len() as u32).to_be_bytes());
                    buf.extend_from_slice(v);
                }
                None => {
                    buf.push(0);
                }
            }
        }
        buf
    }

    /// 从字节反序列化（简化版）
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < 13 {
            return Err(Error::DataCorruption("change event too short".into()));
        }
        let revision = Revision::from_be_bytes(data[0..8].try_into().unwrap());
        let event_type = match data[8] {
            0 => EventType::Put,
            1 => EventType::Delete,
            2 => EventType::Txn,
            t => return Err(Error::DataCorruption(format!("unknown event type: {}", t))),
        };
        let num_changes = u32::from_be_bytes(data[9..13].try_into().unwrap()) as usize;

        let mut changes = Vec::with_capacity(num_changes);
        let mut offset = 13;
        for _ in 0..num_changes {
            if offset + 4 > data.len() {
                return Err(Error::DataCorruption("truncated change".into()));
            }
            let key_len = u32::from_be_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            if offset + key_len > data.len() {
                return Err(Error::DataCorruption("truncated key".into()));
            }
            let key = data[offset..offset + key_len].to_vec();
            offset += key_len;

            if offset >= data.len() {
                return Err(Error::DataCorruption("missing value flag".into()));
            }
            let has_value = data[offset] == 1;
            offset += 1;

            let value = if has_value {
                if offset + 4 > data.len() {
                    return Err(Error::DataCorruption("truncated value len".into()));
                }
                let val_len =
                    u32::from_be_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;
                if offset + val_len > data.len() {
                    return Err(Error::DataCorruption("truncated value".into()));
                }
                let v = data[offset..offset + val_len].to_vec();
                offset += val_len;
                Some(v)
            } else {
                None
            };

            changes.push(KeyValueChange {
                key,
                value,
                prev_value: None,
            });
        }

        Ok(Self {
            revision,
            changes,
            event_type,
        })
    }
}

// ──── KvMetadata ────

/// 单个 Key 的元数据，用于 Version / Revision 追踪和 Lease 绑定
#[derive(Debug, Clone, Copy)]
pub struct KvMetadata {
    /// Key 被修改次数（从 1 开始）
    pub version: i64,
    /// Key 创建时的 Revision
    pub create_revision: i64,
    /// Key 最后修改的 Revision
    pub mod_revision: i64,
    /// 关联的 Lease ID（0 表示无 Lease）
    pub lease_id: i64,
    /// 是否已删除（true 表示该 Key 已被逻辑删除）
    pub deleted: bool,
}

impl KvMetadata {
    /// 序列化为 33 字节固定格式：
    /// version(8BE) | create_revision(8BE) | mod_revision(8BE) | lease_id(8BE) | deleted(1)
    pub fn to_bytes(&self) -> [u8; 33] {
        let mut buf = [0u8; 33];
        buf[0..8].copy_from_slice(&self.version.to_be_bytes());
        buf[8..16].copy_from_slice(&self.create_revision.to_be_bytes());
        buf[16..24].copy_from_slice(&self.mod_revision.to_be_bytes());
        buf[24..32].copy_from_slice(&self.lease_id.to_be_bytes());
        buf[32] = if self.deleted { 1 } else { 0 };
        buf
    }

    /// 从字节反序列化（兼容 32 字节旧格式：默认 deleted=false）
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 32 {
            return None;
        }
        let deleted = if bytes.len() >= 33 { bytes[32] == 1 } else { false };
        Some(Self {
            version: i64::from_be_bytes(bytes[0..8].try_into().unwrap()),
            create_revision: i64::from_be_bytes(bytes[8..16].try_into().unwrap()),
            mod_revision: i64::from_be_bytes(bytes[16..24].try_into().unwrap()),
            lease_id: i64::from_be_bytes(bytes[24..32].try_into().unwrap()),
            deleted,
        })
    }

    /// 创建新 Key 的初始元数据
    pub fn new_key(revision: Revision, lease_id: i64) -> Self {
        Self {
            version: 1,
            create_revision: revision as i64,
            mod_revision: revision as i64,
            lease_id,
            deleted: false,
        }
    }

    /// 更新已有 Key 的元数据（递增 version，更新 mod_revision）
    pub fn update(&self, revision: Revision, lease_id: i64) -> Self {
        Self {
            version: self.version + 1,
            create_revision: self.create_revision,
            mod_revision: revision as i64,
            lease_id,
            deleted: false,
        }
    }

    /// 标记 Key 为已删除
    pub fn mark_deleted(&self, revision: Revision) -> Self {
        Self {
            version: self.version + 1,
            create_revision: self.create_revision,
            mod_revision: revision as i64,
            lease_id: self.lease_id,
            deleted: true,
        }
    }
}

// ──── MvccStorage ────

/// MVCC 版本化存储
///
/// 在 StorageBackend 之上提供应用层 MVCC 语义：
/// - Revision 分配与管理
/// - Key 空间编码
/// - Changelog 自动写入
/// - 范围查询与历史快照读
pub struct MvccStorage<B: StorageBackend> {
    backend: B,
    /// 内存中的下一个 Revision 缓存（启动时从 _meta/next_revision 恢复）
    next_revision: AtomicU64,
    /// 可选的存储屏障（用于 Value 加密/解密，ADP §21）
    barrier: RwLock<Option<Barrier>>,
}

impl<B: StorageBackend> MvccStorage<B> {
    /// 创建 MvccStorage 实例，从存储后端恢复 Revision 状态
    pub fn new(backend: B) -> Result<Self> {
        let next_rev = backend
            .read(|tx| {
                tx.get(TABLE_META, META_NEXT_REVISION)
                    .map(|opt| {
                        opt.and_then(|bytes| {
                            if bytes.len() == 8 {
                                Some(Revision::from_be_bytes(
                                    bytes.as_slice().try_into().unwrap(),
                                ))
                            } else {
                                None
                            }
                        })
                        .unwrap_or(1) // 首次启动从 1 开始
                    })
            })?;

        Ok(Self {
            backend,
            next_revision: AtomicU64::new(next_rev),
            barrier: RwLock::new(None),
        })
    }

    /// 设置存储屏障（在 Keyring 初始化后调用）
    ///
    /// 设置后，所有 `/kv/` 下的 Value 写入前加密、读取后解密。
    /// 元数据（/_meta/）不受屏障影响。
    pub fn set_barrier(&self, barrier: Barrier) {
        *self.barrier.write() = Some(barrier);
    }

    /// 加密 Value（如果 Barrier 已设置）
    fn encrypt_value(&self, value: &[u8]) -> Result<Vec<u8>> {
        match self.barrier.read().as_ref() {
            Some(barrier) => barrier.encrypt(value),
            None => Ok(value.to_vec()),
        }
    }

    /// 解密 Value（如果 Barrier 已设置）
    fn decrypt_value(&self, encrypted: &[u8]) -> Result<Vec<u8>> {
        match self.barrier.read().as_ref() {
            Some(barrier) => {
                // Check if this looks like encrypted data (has key_id prefix)
                if encrypted.len() >= 32 {
                    barrier.decrypt(encrypted)
                } else {
                    // Plaintext (legacy data or meta), return as-is
                    Ok(encrypted.to_vec())
                }
            }
            None => Ok(encrypted.to_vec()),
        }
    }

    /// 获取底层 StorageBackend 的引用（用于只读操作）
    pub fn backend(&self) -> &B {
        &self.backend
    }

    /// 获取当前 Revision（已提交的最大 Revision）
    pub fn current_revision(&self) -> Revision {
        self.next_revision.load(Ordering::SeqCst).saturating_sub(1)
    }

    /// 设置内存中的 Revision 计数器（快照导入后使用）
    pub fn set_next_revision(&self, revision: Revision) {
        self.next_revision.store(revision, Ordering::SeqCst);
    }

    /// Put 操作：写入或更新一个 Key
    ///
    /// 返回写入分配的 Revision。该操作在一个写事务中完成：
    /// 1. 分配 Revision
    /// 2. 读取已有元数据（用于版本追踪）
    /// 3. 写入用户数据
    /// 4. 更新/创建 KV 元数据
    /// 5. 写入 Changelog 条目
    /// 6. 更新 Revision 计数器
    pub fn put(
        &self,
        key: &[u8],
        value: &[u8],
        lease_id: Option<LeaseID>,
    ) -> Result<Revision> {
        let lid = lease_id.unwrap_or(0);
        self.backend.write(|tx| {
            let revision = self.allocate_revision(tx)?;
            let internal_key = encode_kv_key(key);
            let meta_key = encode_kv_meta_key(key);

            // 读取已有元数据
            let existing_meta = tx
                .get(TABLE_KV_META, &meta_key)?
                .and_then(|bytes| KvMetadata::from_bytes(&bytes));

            // 更新元数据
            let meta = match existing_meta {
                Some(m) => m.update(revision, lid),
                None => KvMetadata::new_key(revision, lid),
            };

            // 写入用户数据（经过 Barrier 加密）
            let encrypted = self.encrypt_value(value)?;
            tx.insert(TABLE_KV, &internal_key, &encrypted)?;

            // 写入 KV 元数据
            tx.insert(TABLE_KV_META, &meta_key, &meta.to_bytes())?;

            // 写入 Changelog
            let event = ChangeEvent {
                revision,
                changes: vec![KeyValueChange {
                    key: key.to_vec(),
                    value: Some(value.to_vec()),
                    prev_value: None,
                }],
                event_type: EventType::Put,
            };
            tx.insert(
                TABLE_CHANGELOG,
                &encode_changelog_key(revision),
                &event.to_bytes(),
            )?;

            // 更新 Revision 计数器
            tx.insert(
                TABLE_META,
                META_NEXT_REVISION,
                &(revision + 1).to_be_bytes(),
            )?;

            Ok(revision)
        })
    }

    /// Delete 操作：删除一个 Key
    ///
    /// 返回操作分配的 Revision 和是否实际删除了一个存在的 Key。
    pub fn delete(&self, key: &[u8]) -> Result<Revision> {
        self.backend.write(|tx| {
            let meta_key = encode_kv_meta_key(key);

            // 读取已有元数据（检查 Key 是否存在且未被删除）
            let existing_meta = tx
                .get(TABLE_KV_META, &meta_key)?
                .and_then(|bytes| KvMetadata::from_bytes(&bytes));

            let revision = self.allocate_revision(tx)?;

            match existing_meta {
                Some(m) if !m.deleted => {
                    // Key 存在且未被删除：标记为已删除
                    let meta = m.mark_deleted(revision);
                    tx.insert(TABLE_KV_META, &meta_key, &meta.to_bytes())?;

                    // 写入 Changelog
                    let event = ChangeEvent {
                        revision,
                        changes: vec![KeyValueChange {
                            key: key.to_vec(),
                            value: None,
                            prev_value: None,
                        }],
                        event_type: EventType::Delete,
                    };
                    tx.insert(
                        TABLE_CHANGELOG,
                        &encode_changelog_key(revision),
                        &event.to_bytes(),
                    )?;

                    tx.insert(
                        TABLE_META,
                        META_NEXT_REVISION,
                        &(revision + 1).to_be_bytes(),
                    )?;
                }
                _ => {
                    // Key 不存在或已删除：不分配新 revision，回滚
                    // 但不回滚 revision（简单实现：仍消耗一个 revision）
                    // 返回 revision-1 表示没有实际删除
                }
            }

            Ok(revision)
        })
    }

    /// Get 操作：读取单个 Key 的最新值（经过 Barrier 解密）
    ///
    /// 通过元数据的 deleted 标志区分空 value put 和删除 tombstone。
    /// 所有读取在单个读事务中完成。
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let internal_key = encode_kv_key(key);
        let meta_key = encode_kv_meta_key(key);
        self.backend.read(|tx| {
            let raw = tx.get(TABLE_KV, &internal_key)?;
            match raw {
                Some(data) => {
                    // 在同一事务内检查元数据的 deleted 标志
                    let meta = tx.get(TABLE_KV_META, &meta_key)?;
                    let is_deleted = meta
                        .and_then(|bytes| KvMetadata::from_bytes(&bytes))
                        .map(|m| m.deleted)
                        .unwrap_or(false);
                    if is_deleted {
                        Ok(None)
                    } else if data.is_empty() {
                        Ok(Some(Vec::new()))
                    } else {
                        Ok(Some(self.decrypt_value(&data)?))
                    }
                }
                None => Ok(None),
            }
        })
    }

    /// Range 操作：前缀扫描（返回解密后的 Value）
    ///
    /// 扫描以 prefix 为前缀的所有 Key，按 Key 字典序返回。
    /// 通过元数据的 deleted 标志过滤已删除的 Key。
    /// 所有读取在单个读事务中完成，避免嵌套事务问题。
    pub fn range(&self, prefix: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let internal_prefix = encode_kv_key(prefix);
        self.backend.read(|tx| {
            let all = tx.iter_prefix(TABLE_KV, &internal_prefix)?;
            let mut results = Vec::new();
            for (ik, v) in all {
                if results.len() >= limit && limit > 0 {
                    break;
                }
                if let Some(user_key) = decode_kv_key(&ik) {
                    // 在同一事务内检查元数据的 deleted 标志
                    let meta_key = encode_kv_meta_key(user_key);
                    let is_deleted = tx
                        .get(TABLE_KV_META, &meta_key)?
                        .and_then(|bytes| KvMetadata::from_bytes(&bytes))
                        .map(|m| m.deleted)
                        .unwrap_or(false);
                    if is_deleted {
                        continue;
                    }
                    let plaintext = if v.is_empty() {
                        Vec::new()
                    } else {
                        self.decrypt_value(&v)?
                    };
                    results.push((user_key.to_vec(), plaintext));
                }
            }
            Ok(results)
        })
    }

    /// 读取 Key 的元数据（version, create_revision, mod_revision, lease_id）
    pub fn get_kv_metadata(&self, key: &[u8]) -> Result<Option<KvMetadata>> {
        let meta_key = encode_kv_meta_key(key);
        self.backend
            .read(|tx| tx.get(TABLE_KV_META, &meta_key))
            .map(|opt| opt.and_then(|bytes| KvMetadata::from_bytes(&bytes)))
    }

    /// 查找并删除所有绑定到指定 Lease 的 Key
    ///
    /// 扫描 KV_META 表，删除所有 `lease_id` 匹配的 Key。
    /// 返回被删除的 Key 数量。
    pub fn delete_keys_by_lease(&self, target_lease_id: i64) -> Result<usize> {
        // 收集匹配的 user_key（在单个读事务中完成）
        let keys_to_delete: Vec<Vec<u8>> = self.backend.read(|tx| {
            let all_meta = tx.iter_prefix(TABLE_KV_META, KV_META_PREFIX)?;
            let mut keys = Vec::new();
            for (meta_key_bytes, meta_value) in &all_meta {
                if let Some(meta) = KvMetadata::from_bytes(&meta_value) {
                    if meta.lease_id == target_lease_id && !meta.deleted {
                        if meta_key_bytes.starts_with(KV_META_PREFIX) {
                            let user_key = &meta_key_bytes[KV_META_PREFIX.len()..];
                            keys.push(user_key.to_vec());
                        }
                    }
                }
            }
            Ok(keys)
        })?;

        let count = keys_to_delete.len();
        for key in &keys_to_delete {
            let _ = self.delete(key);
        }
        Ok(count)
    }

    /// Txn 原子事务执行
    ///
    /// 在单个写事务中完成：
    /// 1. 分配 Revision
    /// 2. 评估所有比较条件
    /// 3. 执行 success 或 failure 分支操作
    /// 4. 写入 Changelog
    /// 5. 更新 Revision 计数器
    pub fn execute_txn(
        &self,
        compares: &[crate::txn::TxnCompare],
        success_ops: &[crate::txn::TxnOp],
        failure_ops: &[crate::txn::TxnOp],
    ) -> Result<crate::txn::TxnResult> {
        use crate::txn::TxnResult;

        self.backend.write(|tx| {
            let revision = self.allocate_revision(tx)?;

            // 1. 评估所有比较条件
            let succeeded = self.evaluate_compares_in_tx(tx, compares)?;

            // 2. 选择执行分支
            let ops = if succeeded { success_ops } else { failure_ops };

            // 3. 执行操作并收集变更
            let mut responses = Vec::with_capacity(ops.len());
            let mut changes = Vec::new();

            for op in ops {
                let (resp, change) = self.execute_op_in_tx(tx, op, revision)?;
                responses.push(resp);
                if let Some(c) = change {
                    changes.push(c);
                }
            }

            // 4. 写入 Changelog
            let event = ChangeEvent {
                revision,
                changes,
                event_type: EventType::Txn,
            };
            tx.insert(
                TABLE_CHANGELOG,
                &encode_changelog_key(revision),
                &event.to_bytes(),
            )?;

            // 5. 更新 Revision 计数器
            tx.insert(
                TABLE_META,
                META_NEXT_REVISION,
                &(revision + 1).to_be_bytes(),
            )?;

            Ok(TxnResult {
                succeeded,
                revision,
                responses,
            })
        })
    }

    // ──── 内部辅助方法 ────

    /// 在写事务内评估所有比较条件
    fn evaluate_compares_in_tx(
        &self,
        tx: &mut dyn WriteTx,
        compares: &[crate::txn::TxnCompare],
    ) -> Result<bool> {
        use crate::txn::CompareTarget;

        for cmp in compares {
            let meta_key = encode_kv_meta_key(&cmp.key);
            let kv_key = encode_kv_key(&cmp.key);

            let meta = tx
                .get(TABLE_KV_META, &meta_key)?
                .and_then(|bytes| KvMetadata::from_bytes(&bytes));

            let value = tx.get(TABLE_KV, &kv_key)?;

            let matched = match &cmp.target {
                CompareTarget::Version => {
                    let actual_version = meta.map(|m| m.version).unwrap_or(0);
                    if let crate::txn::CompareValue::Version(target_v) = &cmp.target_value {
                        let actual = crate::txn::CompareValue::Version(actual_version);
                        let target = crate::txn::CompareValue::Version(*target_v);
                        actual.compare(&target, &cmp.op)
                    } else {
                        false
                    }
                }
                CompareTarget::Value => {
                    let actual_value = value.unwrap_or_default();
                    if let crate::txn::CompareValue::Value(target_v) = &cmp.target_value {
                        let actual = crate::txn::CompareValue::Value(actual_value);
                        let target = crate::txn::CompareValue::Value(target_v.clone());
                        actual.compare(&target, &cmp.op)
                    } else {
                        false
                    }
                }
                CompareTarget::ModRevision => {
                    let actual_mod_rev = meta.map(|m| m.mod_revision).unwrap_or(0);
                    if let crate::txn::CompareValue::ModRevision(target_v) = &cmp.target_value {
                        let actual = crate::txn::CompareValue::ModRevision(actual_mod_rev);
                        let target = crate::txn::CompareValue::ModRevision(*target_v);
                        actual.compare(&target, &cmp.op)
                    } else {
                        false
                    }
                }
                CompareTarget::CreateRevision => {
                    let actual_create_rev = meta.map(|m| m.create_revision).unwrap_or(0);
                    if let crate::txn::CompareValue::CreateRevision(target_v) =
                        &cmp.target_value
                    {
                        let actual =
                            crate::txn::CompareValue::CreateRevision(actual_create_rev);
                        let target = crate::txn::CompareValue::CreateRevision(*target_v);
                        actual.compare(&target, &cmp.op)
                    } else {
                        false
                    }
                }
            };

            if !matched {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// 在写事务内执行单个 Txn 操作
    fn execute_op_in_tx(
        &self,
        tx: &mut dyn WriteTx,
        op: &crate::txn::TxnOp,
        revision: Revision,
    ) -> Result<(crate::txn::TxnOpResponse, Option<KeyValueChange>)> {
        use crate::txn::{TxnOp, TxnOpResponse};

        match op {
            TxnOp::Put {
                key,
                value,
                lease_id,
            } => {
                let lid = lease_id.unwrap_or(0);
                let internal_key = encode_kv_key(key);
                let meta_key = encode_kv_meta_key(key);

                // 读取已有元数据
                let existing_meta = tx
                    .get(TABLE_KV_META, &meta_key)?
                    .and_then(|bytes| KvMetadata::from_bytes(&bytes));

                // 更新元数据
                let meta = match existing_meta {
                    Some(m) => m.update(revision, lid),
                    None => KvMetadata::new_key(revision, lid),
                };

                // 写入用户数据（经过 Barrier 加密）
                let encrypted = self.encrypt_value(value)?;
                tx.insert(TABLE_KV, &internal_key, &encrypted)?;
                tx.insert(TABLE_KV_META, &meta_key, &meta.to_bytes())?;

                let change = KeyValueChange {
                    key: key.to_vec(),
                    value: Some(value.to_vec()),
                    prev_value: None,
                };

                Ok((
                    TxnOpResponse::Put {
                        revision,
                    },
                    Some(change),
                ))
            }
            TxnOp::Delete { key } => {
                let meta_key = encode_kv_meta_key(key);

                // 读取已有元数据并标记删除
                let existing_meta = tx
                    .get(TABLE_KV_META, &meta_key)?
                    .and_then(|bytes| KvMetadata::from_bytes(&bytes));

                if let Some(m) = existing_meta {
                    let meta = m.mark_deleted(revision);
                    tx.insert(TABLE_KV_META, &meta_key, &meta.to_bytes())?;
                }

                let change = KeyValueChange {
                    key: key.to_vec(),
                    value: None,
                    prev_value: None,
                };

                Ok((
                    TxnOpResponse::Delete {
                        revision,
                    },
                    Some(change),
                ))
            }
            TxnOp::Range {
                key,
                range_end,
                limit,
            } => {
                // Range 在 Txn 内部执行：扫描 KV 表
                let internal_prefix = encode_kv_key(key);
                let all = tx.iter_prefix(TABLE_KV, &internal_prefix)?;

                let mut kvs = Vec::new();
                let max = if *limit > 0 {
                    *limit as usize
                } else {
                    usize::MAX
                };

                for (ik, v) in all {
                    if kvs.len() >= max {
                        break;
                    }
                    if let Some(user_key) = decode_kv_key(&ik) {
                        // 检查 range_end
                        if !range_end.is_empty() && user_key.as_ref() >= range_end.as_slice() {
                            break;
                        }
                        // 跳过 tombstone
                        if v.is_empty() {
                            continue;
                        }
                        let plaintext = self.decrypt_value(&v)?;
                        kvs.push((user_key.to_vec(), plaintext));
                    }
                }

                let count = kvs.len() as i64;
                Ok((
                    TxnOpResponse::Range {
                        kvs,
                        count,
                        revision,
                    },
                    None, // Range 不产生变更
                ))
            }
        }
    }

    /// 获取 Applied Index（崩溃恢复检查点）
    pub fn get_applied_index(&self) -> Result<Option<u64>> {
        self.backend.read(|tx| {
            tx.get(TABLE_META, META_APPLIED_INDEX).map(|opt| {
                opt.and_then(|bytes| {
                    if bytes.len() == 8 {
                        Some(u64::from_be_bytes(bytes.as_slice().try_into().unwrap()))
                    } else {
                        None
                    }
                })
            })
        })
    }

    /// 设置 Applied Index（崩溃恢复检查点）
    pub fn set_applied_index(&self, index: u64) -> Result<()> {
        self.backend.write(|tx| {
            tx.insert(TABLE_META, META_APPLIED_INDEX, &index.to_be_bytes())
        })
    }

    /// 分配一个新的 Revision（在写事务内调用）
    fn allocate_revision(&self, _tx: &mut dyn WriteTx) -> Result<Revision> {
        // 从内存原子计数器获取下一个 Revision
        // 注意：此处的原子递增仅在 Leader 节点执行，
        // Follower 节点通过 Raft Log Apply 获得相同的 Revision
        let revision = self.next_revision.fetch_add(1, Ordering::SeqCst);

        // 确保 Revision > 0（首次启动时 next_revision=1，fetch_add 返回 1）
        if revision == 0 {
            return Err(Error::Internal("revision overflow".into()));
        }

        Ok(revision)
    }

    /// 从指定 Revision 开始读取 Changelog 条目（含 start_revision）
    ///
    /// 用于 Watch 历史回放：新订阅者通过此方法获取 [start_revision, ∞) 的变更事件。
    pub fn read_changelog_entries(&self, start_revision: Revision) -> Result<Vec<ChangeEvent>> {
        let start_key = encode_changelog_key(start_revision);
        self.backend.read(|tx| {
            let entries = tx.iter_prefix(TABLE_CHANGELOG, CHANGELOG_PREFIX)?;
            let mut events = Vec::new();
            for (key, value) in entries {
                // 只读取 >= start_revision 的条目
                if key.as_slice() < start_key.as_slice() {
                    continue;
                }
                if let Ok(event) = ChangeEvent::from_bytes(&value) {
                    events.push(event);
                }
            }
            Ok(events)
        })
    }

    /// 读取 Key 在指定历史 Revision 时的值
    ///
    /// 通过扫描 Changelog 找到该 Key 在 <= target_revision 时的最后一次写入值。
    /// 如果 Key 在 target_revision 时不存在或已被删除，返回 None。
    pub fn get_at_revision(&self, key: &[u8], target_revision: Revision) -> Result<Option<Vec<u8>>> {
        let start_key = encode_changelog_key(1); // 从 rev 1 开始扫描
        let end_key = encode_changelog_key(target_revision.saturating_add(1));
        self.backend.read(|tx| {
            let entries = tx.iter_prefix(TABLE_CHANGELOG, CHANGELOG_PREFIX)?;
            let mut last_value: Option<Vec<u8>> = None;
            for (ch_key, ch_value) in entries {
                // 只读取 [1, target_revision] 范围内的条目
                if ch_key.as_slice() < start_key.as_slice() {
                    continue;
                }
                if ch_key.as_slice() >= end_key.as_slice() {
                    break;
                }
                if let Ok(event) = ChangeEvent::from_bytes(&ch_value) {
                    for change in &event.changes {
                        if change.key == key {
                            // 该 Revision 修改了此 Key，更新 value
                            last_value = change.value.clone();
                        }
                    }
                }
            }
            Ok(last_value)
        })
    }
}

// ──── ChangelogReader impl ────

use crate::watch::ChangelogReader;

impl<B: StorageBackend> ChangelogReader for MvccStorage<B> {
    fn read_changelog_from(
        &self,
        start_revision: Revision,
    ) -> std::result::Result<Vec<ChangeEvent>, String> {
        MvccStorage::read_changelog_entries(self, start_revision)
            .map_err(|e| format!("changelog read error: {e}"))
    }
}

// ──── 测试 ────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::redb_backend::RedbBackend;
    use coord_core::types::StorageConfig;
    use tempfile::TempDir;

    fn create_storage() -> (TempDir, MvccStorage<RedbBackend>) {
        let dir = TempDir::new().unwrap();
        let config = StorageConfig {
            data_dir: dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let backend = RedbBackend::open(dir.path(), &config).unwrap();
        let storage = MvccStorage::new(backend).unwrap();
        (dir, storage)
    }

    #[test]
    fn test_initial_revision_is_zero() {
        let (_dir, storage) = create_storage();
        assert_eq!(storage.current_revision(), 0);
    }

    #[test]
    fn test_put_and_get() {
        let (_dir, storage) = create_storage();
        let rev = storage.put(b"hello", b"world", None).unwrap();
        assert_eq!(rev, 1);
        assert_eq!(storage.current_revision(), 1);

        let value = storage.get(b"hello").unwrap();
        assert_eq!(value, Some(b"world".to_vec()));
    }

    #[test]
    fn test_revision_monotonic() {
        let (_dir, storage) = create_storage();
        let rev1 = storage.put(b"key1", b"val1", None).unwrap();
        let rev2 = storage.put(b"key2", b"val2", None).unwrap();
        let rev3 = storage.put(b"key3", b"val3", None).unwrap();
        assert!(rev1 < rev2);
        assert!(rev2 < rev3);
        assert_eq!(storage.current_revision(), 3);
    }

    #[test]
    fn test_delete() {
        let (_dir, storage) = create_storage();
        storage.put(b"key1", b"val1", None).unwrap();
        assert!(storage.get(b"key1").unwrap().is_some());

        storage.delete(b"key1").unwrap();
        // Delete 后 get 返回 None（tombstone 被过滤）
        assert_eq!(storage.get(b"key1").unwrap(), None);
    }

    #[test]
    fn test_range_prefix() {
        let (_dir, storage) = create_storage();
        storage.put(b"/app/config/a", b"1", None).unwrap();
        storage.put(b"/app/config/b", b"2", None).unwrap();
        storage.put(b"/app/data/x", b"3", None).unwrap();

        let results = storage.range(b"/app/config/", 0).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_range_limit() {
        let (_dir, storage) = create_storage();
        storage.put(b"/app/a", b"1", None).unwrap();
        storage.put(b"/app/b", b"2", None).unwrap();
        storage.put(b"/app/c", b"3", None).unwrap();

        let results = storage.range(b"/app/", 2).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_applied_index() {
        let (_dir, storage) = create_storage();
        assert_eq!(storage.get_applied_index().unwrap(), None);

        storage.set_applied_index(42).unwrap();
        assert_eq!(storage.get_applied_index().unwrap(), Some(42));
    }

    #[test]
    fn test_changelog_roundtrip() {
        let event = ChangeEvent {
            revision: 5,
            changes: vec![
                KeyValueChange {
                    key: b"key1".to_vec(),
                    value: Some(b"val1".to_vec()),
                    prev_value: None,
                },
                KeyValueChange {
                    key: b"key2".to_vec(),
                    value: None,
                    prev_value: Some(b"old".to_vec()),
                },
            ],
            event_type: EventType::Txn,
        };

        let bytes = event.to_bytes();
        let decoded = ChangeEvent::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.revision, 5);
        assert_eq!(decoded.event_type, EventType::Txn);
        assert_eq!(decoded.changes.len(), 2);
        assert_eq!(decoded.changes[0].key, b"key1");
        assert_eq!(decoded.changes[0].value, Some(b"val1".to_vec()));
        assert_eq!(decoded.changes[1].key, b"key2");
        assert_eq!(decoded.changes[1].value, None);
    }

    #[test]
    fn test_persistence_across_restart() {
        let dir = TempDir::new().unwrap();
        let config = StorageConfig {
            data_dir: dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };

        // 第一次启动：写入数据
        {
            let backend = RedbBackend::open(dir.path(), &config).unwrap();
            let storage = MvccStorage::new(backend).unwrap();
            storage.put(b"persist", b"data", None).unwrap();
            assert_eq!(storage.current_revision(), 1);
        }

        // 第二次启动：读取数据
        {
            let backend = RedbBackend::open(dir.path(), &config).unwrap();
            let storage = MvccStorage::new(backend).unwrap();
            assert_eq!(storage.current_revision(), 1);
            assert_eq!(storage.get(b"persist").unwrap(), Some(b"data".to_vec()));
        }
    }

    // ──── Txn 测试 ────

    use crate::txn::{
        CompareOp, CompareTarget, CompareValue, TxnCompare, TxnOp, TxnOpResponse,
    };

    /// 辅助：创建 Value 相等比较
    fn cmp_value_eq(key: &[u8], value: &[u8]) -> TxnCompare {
        TxnCompare {
            key: key.to_vec(),
            target: CompareTarget::Value,
            op: CompareOp::Equal,
            target_value: CompareValue::Value(value.to_vec()),
        }
    }

    /// 辅助：创建 Put 操作
    fn txn_put(key: &[u8], value: &[u8]) -> TxnOp {
        TxnOp::Put {
            key: key.to_vec(),
            value: value.to_vec(),
            lease_id: None,
        }
    }

    /// 辅助：创建 Delete 操作
    fn txn_delete(key: &[u8]) -> TxnOp {
        TxnOp::Delete {
            key: key.to_vec(),
        }
    }

    #[test]
    fn test_txn_cas_success() {
        let (_dir, storage) = create_storage();
        storage.put(b"lock", b"unlocked", None).unwrap();

        // CAS: 如果 lock == "unlocked"，则改为 "locked"
        let compares = vec![cmp_value_eq(b"lock", b"unlocked")];
        let success_ops = vec![txn_put(b"lock", b"locked")];
        let failure_ops = vec![];

        let result = storage
            .execute_txn(&compares, &success_ops, &failure_ops)
            .unwrap();

        assert!(result.succeeded);
        assert_eq!(result.revision, 2);
        assert_eq!(result.responses.len(), 1);

        // 验证实际值已变更
        assert_eq!(
            storage.get(b"lock").unwrap(),
            Some(b"locked".to_vec())
        );
    }

    #[test]
    fn test_txn_cas_failure() {
        let (_dir, storage) = create_storage();
        storage.put(b"lock", b"locked", None).unwrap();

        // CAS: 如果 lock == "unlocked"，则改为 "locked"（会失败）
        let compares = vec![cmp_value_eq(b"lock", b"unlocked")];
        let success_ops = vec![txn_put(b"lock", b"acquired")];
        let failure_ops = vec![txn_put(b"lock", b"still_locked")];

        let result = storage
            .execute_txn(&compares, &success_ops, &failure_ops)
            .unwrap();

        assert!(!result.succeeded);
        assert_eq!(result.revision, 2);
        assert_eq!(result.responses.len(), 1);

        // 验证执行了 failure 分支
        assert_eq!(
            storage.get(b"lock").unwrap(),
            Some(b"still_locked".to_vec())
        );
    }

    #[test]
    fn test_txn_version_compare() {
        let (_dir, storage) = create_storage();
        storage.put(b"key", b"v1", None).unwrap(); // version=1
        storage.put(b"key", b"v2", None).unwrap(); // version=2

        // 比较 version == 2
        let compares = vec![TxnCompare {
            key: b"key".to_vec(),
            target: CompareTarget::Version,
            op: CompareOp::Equal,
            target_value: CompareValue::Version(2),
        }];
        let success_ops = vec![txn_put(b"key", b"v3")];
        let failure_ops = vec![];

        let result = storage
            .execute_txn(&compares, &success_ops, &failure_ops)
            .unwrap();

        assert!(result.succeeded);
        assert_eq!(storage.get(b"key").unwrap(), Some(b"v3".to_vec()));
    }

    #[test]
    fn test_txn_version_compare_failure() {
        let (_dir, storage) = create_storage();
        storage.put(b"key", b"v1", None).unwrap(); // version=1

        // 比较 version == 99（不存在的版本）
        let compares = vec![TxnCompare {
            key: b"key".to_vec(),
            target: CompareTarget::Version,
            op: CompareOp::Equal,
            target_value: CompareValue::Version(99),
        }];
        let success_ops = vec![txn_put(b"key", b"should_not_write")];
        let failure_ops = vec![txn_put(b"key", b"version_mismatch")];

        let result = storage
            .execute_txn(&compares, &success_ops, &failure_ops)
            .unwrap();

        assert!(!result.succeeded);
        assert_eq!(
            storage.get(b"key").unwrap(),
            Some(b"version_mismatch".to_vec())
        );
    }

    #[test]
    fn test_txn_mod_revision_compare() {
        let (_dir, storage) = create_storage();
        storage.put(b"key", b"v1", None).unwrap(); // mod_revision=1
        storage.put(b"key", b"v2", None).unwrap(); // mod_revision=2

        // 比较 mod_revision > 1
        let compares = vec![TxnCompare {
            key: b"key".to_vec(),
            target: CompareTarget::ModRevision,
            op: CompareOp::Greater,
            target_value: CompareValue::ModRevision(1),
        }];
        let success_ops = vec![txn_put(b"key", b"v3")];
        let failure_ops = vec![];

        let result = storage
            .execute_txn(&compares, &success_ops, &failure_ops)
            .unwrap();

        assert!(result.succeeded);
        assert_eq!(storage.get(b"key").unwrap(), Some(b"v3".to_vec()));
    }

    #[test]
    fn test_txn_create_revision_compare() {
        let (_dir, storage) = create_storage();
        let rev1 = storage.put(b"key", b"v1", None).unwrap(); // create_revision=1
        storage.put(b"key", b"v2", None).unwrap(); // create_revision stays 1

        // 比较 create_revision == 1
        let compares = vec![TxnCompare {
            key: b"key".to_vec(),
            target: CompareTarget::CreateRevision,
            op: CompareOp::Equal,
            target_value: CompareValue::CreateRevision(rev1 as i64),
        }];
        let success_ops = vec![txn_put(b"key", b"v3")];
        let failure_ops = vec![];

        let result = storage
            .execute_txn(&compares, &success_ops, &failure_ops)
            .unwrap();

        assert!(result.succeeded);
    }

    #[test]
    fn test_txn_multiple_compares_and() {
        let (_dir, storage) = create_storage();
        storage.put(b"a", b"1", None).unwrap();
        storage.put(b"b", b"2", None).unwrap();

        // AND 条件：a=="1" AND b=="2" → 全部满足
        let compares = vec![
            cmp_value_eq(b"a", b"1"),
            cmp_value_eq(b"b", b"2"),
        ];
        let success_ops = vec![txn_put(b"a", b"ok")];
        let failure_ops = vec![];

        let result = storage
            .execute_txn(&compares, &success_ops, &failure_ops)
            .unwrap();

        assert!(result.succeeded);
        assert_eq!(storage.get(b"a").unwrap(), Some(b"ok".to_vec()));
    }

    #[test]
    fn test_txn_multiple_ops_in_branch() {
        let (_dir, storage) = create_storage();
        storage.put(b"key1", b"v1", None).unwrap();
        storage.put(b"key2", b"v2", None).unwrap();

        // 在 success 分支执行多个操作
        let compares = vec![cmp_value_eq(b"key1", b"v1")];
        let success_ops = vec![
            txn_put(b"key1", b"updated"),
            txn_put(b"key2", b"also_updated"),
            txn_delete(b"key3"), // key3 不存在，删除也是合法的
        ];
        let failure_ops = vec![];

        let result = storage
            .execute_txn(&compares, &success_ops, &failure_ops)
            .unwrap();

        assert!(result.succeeded);
        assert_eq!(result.responses.len(), 3);
        assert_eq!(
            storage.get(b"key1").unwrap(),
            Some(b"updated".to_vec())
        );
        assert_eq!(
            storage.get(b"key2").unwrap(),
            Some(b"also_updated".to_vec())
        );
    }

    #[test]
    fn test_txn_range_inside() {
        let (_dir, storage) = create_storage();
        storage.put(b"/svc/a", b"addr1", None).unwrap();
        storage.put(b"/svc/b", b"addr2", None).unwrap();
        storage.put(b"/svc/c", b"addr3", None).unwrap();

        // 在 Txn 内执行 Range 读取
        let compares = vec![cmp_value_eq(b"/svc/a", b"addr1")];
        let success_ops = vec![TxnOp::Range {
            key: b"/svc/".to_vec(),
            range_end: vec![],
            limit: 10,
        }];
        let failure_ops = vec![];

        let result = storage
            .execute_txn(&compares, &success_ops, &failure_ops)
            .unwrap();

        assert!(result.succeeded);
        assert_eq!(result.responses.len(), 1);
        if let TxnOpResponse::Range { kvs, count, .. } = &result.responses[0] {
            assert_eq!(*count, 3);
            assert_eq!(kvs.len(), 3);
        } else {
            panic!("expected Range response");
        }
    }

    #[test]
    fn test_txn_on_nonexistent_key() {
        let (_dir, storage) = create_storage();

        // 比较不存在的 key：value 为默认空
        let compares = vec![TxnCompare {
            key: b"nonexistent".to_vec(),
            target: CompareTarget::Value,
            op: CompareOp::Equal,
            target_value: CompareValue::Value(vec![]),
        }];
        let success_ops = vec![txn_put(b"nonexistent", b"created")];
        let failure_ops = vec![];

        let result = storage
            .execute_txn(&compares, &success_ops, &failure_ops)
            .unwrap();

        assert!(result.succeeded);
        assert_eq!(
            storage.get(b"nonexistent").unwrap(),
            Some(b"created".to_vec())
        );
    }

    #[test]
    fn test_txn_revision_monotonic() {
        let (_dir, storage) = create_storage();
        storage.put(b"k", b"v", None).unwrap(); // rev=1

        // 执行 Txn
        let compares = vec![cmp_value_eq(b"k", b"v")];
        let result = storage
            .execute_txn(&compares, &[txn_put(b"k", b"v2")], &[])
            .unwrap();
        assert_eq!(result.revision, 2);

        // 再次 Put
        let rev3 = storage.put(b"k", b"v3", None).unwrap();
        assert_eq!(rev3, 3);
    }

    #[test]
    fn test_kv_metadata_tracking() {
        let (_dir, storage) = create_storage();

        // 首次 Put：version=1, create_revision=mod_revision=rev1
        let rev1 = storage.put(b"key", b"v1", None).unwrap();
        let meta = storage.get_kv_metadata(b"key").unwrap().unwrap();
        assert_eq!(meta.version, 1);
        assert_eq!(meta.create_revision, rev1 as i64);
        assert_eq!(meta.mod_revision, rev1 as i64);

        // 第二次 Put：version=2, create_revision 不变, mod_revision=rev2
        let rev2 = storage.put(b"key", b"v2", None).unwrap();
        let meta = storage.get_kv_metadata(b"key").unwrap().unwrap();
        assert_eq!(meta.version, 2);
        assert_eq!(meta.create_revision, rev1 as i64);
        assert_eq!(meta.mod_revision, rev2 as i64);

        // Delete：version=3
        let _rev3 = storage.delete(b"key").unwrap();
        let meta = storage.get_kv_metadata(b"key").unwrap().unwrap();
        assert_eq!(meta.version, 3);
        assert_eq!(meta.create_revision, rev1 as i64);
    }
}
