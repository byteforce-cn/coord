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

use parking_lot::RwLock;

use serde::{Deserialize, Serialize};

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

/// 已 Apply 的最大 Raft LogId（崩溃恢复检查点；与命令写入同一事务，D-A4）
pub(crate) const META_LAST_APPLIED: &[u8] = b"/_meta/last_applied";

/// 已持久化快照元数据（last_log_id/checksum/path，D-A4/A.6）
pub(crate) const META_SNAPSHOT: &[u8] = b"/_meta/snapshot";

/// 已持久化的 Raft membership（与 applied 持久化配套：重启后 leader 选举依赖它）
pub(crate) const META_MEMBERSHIP: &[u8] = b"/_meta/membership";

/// 已持久化的 compacted revision（P1-01：raft 下发，节点一致；
/// 小于等于它的 changelog/tombstone 已被物理删除）
pub(crate) const META_COMPACT_REVISION: &[u8] = b"/_meta/compacted_revision";

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
    /// Lease 生命周期事件（Grant/KeepAlive/Revoke，P0-B）
    Lease = 3,
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

/// Changelog 格式版本（P0-A：版本号 +1，0.1.x 数据不承诺兼容）
const CHANGELOG_FORMAT_VERSION: u8 = 2;

impl ChangeEvent {
    /// 序列化为字节
    ///
    /// v2 格式：version(1) | revision(8BE) | event_type(1) | num_changes(4BE) | [key_len(4BE)|key|has_value(1)|value...]
    /// v1（旧）格式：revision(8BE) | event_type(1) | ...，首个字节 0/1/2 可判别。
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(CHANGELOG_FORMAT_VERSION);
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

    /// 从字节反序列化（兼容 v1 旧格式）
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < 13 {
            return Err(Error::DataCorruption("change event too short".into()));
        }
        // 判别格式：v2 首个字节为版本号 2；v1 旧格式首个字节为 event_type（0/1/2）
        let (revision_start, event_type_pos) = if data[0] == CHANGELOG_FORMAT_VERSION {
            (1usize, 9usize)
        } else {
            (0usize, 8usize)
        };
        let revision = match data[revision_start..revision_start + 8].try_into() {
            Ok(bytes) => Revision::from_be_bytes(bytes),
            Err(_) => return Err(Error::DataCorruption("truncated revision".into())),
        };
        let event_type = match data[event_type_pos] {
            0 => EventType::Put,
            1 => EventType::Delete,
            2 => EventType::Txn,
            3 => EventType::Lease,
            t => return Err(Error::DataCorruption(format!("unknown event type: {}", t))),
        };
        let num_changes = match data[event_type_pos + 1..event_type_pos + 5].try_into() {
            Ok(bytes) => u32::from_be_bytes(bytes) as usize,
            Err(_) => return Err(Error::DataCorruption("truncated change count".into())),
        };

        let mut changes = Vec::with_capacity(num_changes);
        let mut offset = event_type_pos + 5;
        for _ in 0..num_changes {
            if offset + 4 > data.len() {
                return Err(Error::DataCorruption("truncated change".into()));
            }
            let key_len = match data[offset..offset + 4].try_into() {
                Ok(bytes) => u32::from_be_bytes(bytes) as usize,
                Err(_) => return Err(Error::DataCorruption("truncated key length".into())),
            };
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
                let val_len = match data[offset..offset + 4].try_into() {
                    Ok(bytes) => u32::from_be_bytes(bytes) as usize,
                    Err(_) => return Err(Error::DataCorruption("truncated value length".into())),
                };
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
        let deleted = if bytes.len() >= 33 {
            bytes[32] == 1
        } else {
            false
        };
        Some(Self {
            version: i64::from_be_bytes(bytes[0..8].try_into().ok()?),
            create_revision: i64::from_be_bytes(bytes[8..16].try_into().ok()?),
            mod_revision: i64::from_be_bytes(bytes[16..24].try_into().ok()?),
            lease_id: i64::from_be_bytes(bytes[24..32].try_into().ok()?),
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

// ──── AppliedLogId ────

/// 持久化的已 Apply LogId（D-A4：与命令写入同一事务）
///
/// raft apply 路径写入 `{term, node_id}` 来自 `entry.log_id`；
/// 单节点模式（无 raft）写入 `{0, 0, revision}`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedLogId {
    pub term: u64,
    pub node_id: u64,
    pub index: u64,
}

impl AppliedLogId {
    /// 单节点模式（无 raft）的合成 LogId
    pub fn standalone(index: u64) -> Self {
        Self {
            term: 0,
            node_id: 0,
            index,
        }
    }

    pub(crate) fn to_bytes(self) -> Vec<u8> {
        bincode::serialize(&self).unwrap_or_else(|_| Vec::new())
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Option<Self> {
        bincode::deserialize(bytes).ok()
    }
}

// ──── LeaseRecord（P0-B：raft 状态机内持久化 lease 表） ────

/// `/_lease/{id}` 的持久化记录（规格 B.3）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseRecord {
    /// 租约 TTL（秒）
    pub ttl: i64,
    /// 租约过期墙钟时刻（epoch 毫秒，grant/keepalive 时由 leader 计算后入日志）
    pub deadline_wall_ms: i64,
    /// 最后一次续约的 revision（log index）
    pub keepalive_revision: i64,
}

impl LeaseRecord {
    fn to_bytes(self) -> [u8; 24] {
        let mut buf = [0u8; 24];
        buf[0..8].copy_from_slice(&self.ttl.to_be_bytes());
        buf[8..16].copy_from_slice(&self.deadline_wall_ms.to_be_bytes());
        buf[16..24].copy_from_slice(&self.keepalive_revision.to_be_bytes());
        buf
    }

    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 24 {
            return None;
        }
        Some(Self {
            ttl: i64::from_be_bytes(bytes[0..8].try_into().ok()?),
            deadline_wall_ms: i64::from_be_bytes(bytes[8..16].try_into().ok()?),
            keepalive_revision: i64::from_be_bytes(bytes[16..24].try_into().ok()?),
        })
    }
}

/// 将 Lease ID 编码为内部存储 Key：/_lease/{id_be}
pub(crate) fn encode_lease_key(lease_id: i64) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(LEASE_PREFIX.len() + 8);
    encoded.extend_from_slice(LEASE_PREFIX);
    encoded.extend_from_slice(&lease_id.to_be_bytes());
    encoded
}

// ──── ApplyOutcome ────

/// apply 结果：是否因幂等守卫（D-A3）跳过了实际写入
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApplyOutcome {
    /// true = 该 revision 的 changelog 已存在，本次为重放，未产生副作用
    pub replayed: bool,
}

impl ApplyOutcome {
    pub fn applied() -> Self {
        Self { replayed: false }
    }

    pub fn replayed() -> Self {
        Self { replayed: true }
    }
}

/// 范围删除（DeleteRange，R-SVC-07）apply 结果
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeleteRangeOutcome {
    /// 实际被标记删除的 Key 列表（不含已删除/不存在的 Key）
    pub deleted_keys: Vec<Vec<u8>>,
    /// true = 该 revision 的 changelog 已存在，本次为重放，未产生副作用
    pub replayed: bool,
}

// ──── Compact（P1-01） ────

/// 单次 compaction 批删除上限（P1-01：单写事务分片删除，避免巨型事务）
pub(crate) const COMPACT_BATCH_SIZE: usize = 500;

/// Compact apply 结果统计（P1-01）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactOutcome {
    /// 被删除的 changelog 条目数
    pub deleted_changelog: usize,
    /// 被物理删除的 tombstone 数（KV 行 + 元数据行）
    pub deleted_tombstones: usize,
}

// ──── MvccStorage ────

/// MVCC 版本化存储
///
/// 在 StorageBackend 之上提供应用层 MVCC 语义：
/// - revision ≡ raft log index（D-A2：由 apply 传入，不再由本层分配）
/// - Changelog 自动写入
/// - applied 状态同事务持久化（D-A4）
/// - Lease 状态表（P0-B）
///
/// 单实例语义（D-A1）：全链路共享一个实例；读走 redb 读事务（天然读已提交）。
pub struct MvccStorage<B: StorageBackend> {
    backend: B,
    /// 可选的存储屏障（用于 Value 加密/解密，ADP §21）
    barrier: RwLock<Option<Barrier>>,
    /// 单节点模式（无 raft）的 revision 分配锁：保证并发写入分配不同 revision
    /// （raft 模式下 apply 以 log index 为 revision，不需要此锁）
    standalone_lock: parking_lot::Mutex<()>,
}

impl<B: StorageBackend> MvccStorage<B> {
    /// 创建 MvccStorage 实例
    ///
    /// revision 不再从元数据恢复（D-A2：revision 由 raft apply 传入）。
    /// 启动一致性校验（M0-3）由 `verify_consistency` 显式执行。
    pub fn new(backend: B) -> Result<Self> {
        Ok(Self {
            backend,
            barrier: RwLock::new(None),
            standalone_lock: parking_lot::Mutex::new(()),
        })
    }

    /// 设置存储屏障（在 Keyring 初始化后调用）
    ///
    /// 设置后，所有 `/kv/` 下的 Value 写入前加密、读取后解密。
    /// 元数据（/_meta/）不受屏障影响。
    pub fn set_barrier(&self, barrier: Barrier) {
        *self.barrier.write() = Some(barrier);
    }

    /// 加密 Value（如果 Barrier 已设置）。R-SEC-01：仅加密 `/kv/` 用户数据——
    /// `/_lease/`、`/_sys/` 等内部结构化记录（TABLE_KV 内）不加密，否则
    /// `LeaseRecord::from_bytes` 等解析会因密文长度/内容不符而失败。
    fn encrypt_value(&self, internal_key: &[u8], value: &[u8]) -> Result<Vec<u8>> {
        if !internal_key.starts_with(KV_PREFIX) {
            return Ok(value.to_vec());
        }
        match self.barrier.read().as_ref() {
            Some(barrier) => barrier.encrypt(value),
            None => Ok(value.to_vec()),
        }
    }

    /// 解密 Value（如果 Barrier 已设置）。仅解密 `/kv/` 用户数据；
    /// 短密文（<32B，legacy 明文或内部记录）透传。
    fn decrypt_value(&self, internal_key: &[u8], encrypted: &[u8]) -> Result<Vec<u8>> {
        if !internal_key.starts_with(KV_PREFIX) {
            return Ok(encrypted.to_vec());
        }
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
    ///
    /// 从盘上 `META_LAST_APPLIED` 读取（D-A4），无持久化条目时为 0。
    pub fn current_revision(&self) -> Revision {
        self.get_applied_log_id()
            .ok()
            .flatten()
            .map(|a| a.index)
            .unwrap_or(0)
    }

    /// 读取持久化的已 Apply LogId（`META_LAST_APPLIED`）
    pub fn get_applied_log_id(&self) -> Result<Option<AppliedLogId>> {
        self.backend
            .read(|tx| tx.get(TABLE_META, META_LAST_APPLIED))
            .map(|opt| opt.and_then(|bytes| AppliedLogId::from_bytes(&bytes)))
    }

    /// 单独持久化 applied LogId（仅用于 Membership/Blank 等不写 KV 事务的条目）
    pub fn set_last_applied(&self, applied: AppliedLogId) -> Result<()> {
        self.backend
            .write(|tx| tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes()))
    }

    /// 检查某 revision 的 changelog 条目是否已存在（D-A3 幂等守卫）
    pub fn changelog_contains_revision(&self, revision: Revision) -> Result<bool> {
        self.backend.read(|tx| {
            tx.get(TABLE_CHANGELOG, &encode_changelog_key(revision))
                .map(|opt| opt.is_some())
        })
    }

    /// M0-3 启动一致性校验：`META_LAST_APPLIED` 与 changelog 尾部一致
    ///
    /// 返回（持久化 applied 索引、changelog 最大 revision）。不一致时调用方显式告警
    /// 并按"快照 → 日志"顺序恢复（重放由幂等守卫兜底）。
    pub fn verify_consistency(&self) -> Result<(u64, Option<u64>)> {
        let applied = self.get_applied_log_id()?.map(|a| a.index).unwrap_or(0);
        let changelog_tail = self.backend.read(|tx| {
            let entries = tx.iter_prefix(TABLE_CHANGELOG, CHANGELOG_PREFIX)?;
            let mut tail: Option<u64> = None;
            for (key, _) in entries {
                if key.len() >= CHANGELOG_PREFIX.len() + 8 {
                    if let Ok(rev_bytes) = key[key.len() - 8..].try_into() {
                        tail = Some(Revision::from_be_bytes(rev_bytes));
                    }
                }
            }
            Ok(tail)
        })?;
        Ok((applied, changelog_tail))
    }

    /// Put 操作（单节点模式入口）
    ///
    /// revision 取 `current_revision + 1`（在独立锁内分配，防止并发同 revision 冲突）；
    /// 集群模式应走 `put_at_revision`（raft apply 传入）。
    pub fn put(&self, key: &[u8], value: &[u8], lease_id: Option<LeaseID>) -> Result<Revision> {
        let _guard = self.standalone_lock.lock();
        let revision = self.current_revision().saturating_add(1);
        let applied = AppliedLogId::standalone(revision);
        self.put_at_revision(key, value, lease_id, revision, applied)?;
        Ok(revision)
    }

    /// Put 操作（raft apply 路径）：revision ≡ log index（D-A2）
    ///
    /// 幂等守卫（D-A3）：该 revision 的 changelog 已存在则跳过写入。
    /// 业务写入 + changelog + META_LAST_APPLIED 在同一写事务内原子完成（D-A4）。
    pub fn put_at_revision(
        &self,
        key: &[u8],
        value: &[u8],
        lease_id: Option<LeaseID>,
        revision: Revision,
        applied: AppliedLogId,
    ) -> Result<ApplyOutcome> {
        if self.changelog_contains_revision(revision)? {
            return Ok(ApplyOutcome::replayed());
        }
        let lid = lease_id.unwrap_or(0);
        self.backend.write(|tx| {
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

            // 写入用户数据（经过 Barrier 加密，仅 /kv/ 前缀）
            let encrypted = self.encrypt_value(&internal_key, value)?;
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

            // 持久化 applied 状态（D-A4：与命令写入同一事务）
            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;

            Ok(ApplyOutcome::applied())
        })
    }

    /// Delete 操作（单节点模式入口）
    pub fn delete(&self, key: &[u8]) -> Result<Revision> {
        let _guard = self.standalone_lock.lock();
        let revision = self.current_revision().saturating_add(1);
        let applied = AppliedLogId::standalone(revision);
        self.delete_at_revision(key, revision, applied)?;
        Ok(revision)
    }

    /// 范围删除（单节点模式入口，R-SVC-07-3）：原子删除 `[start, range_end)` 内所有 Key
    ///
    /// 返回（revision, 实际删除的 Key 数）。
    pub fn delete_range(&self, start: &[u8], range_end: &[u8]) -> Result<(Revision, usize)> {
        let _guard = self.standalone_lock.lock();
        let revision = self.current_revision().saturating_add(1);
        let applied = AppliedLogId::standalone(revision);
        let outcome = self.delete_range_at_revision(start, range_end, revision, applied)?;
        Ok((revision, outcome.deleted_keys.len()))
    }

    /// Delete 操作（raft apply 路径）：revision ≡ log index（D-A2）
    ///
    /// no-op delete 统一语义（D-A5）：无论 Key 是否存在，始终消耗一个 revision
    /// 并写 changelog（与 etcd 一致），消除"回滚计数器"分支。
    pub fn delete_at_revision(
        &self,
        key: &[u8],
        revision: Revision,
        applied: AppliedLogId,
    ) -> Result<ApplyOutcome> {
        if self.changelog_contains_revision(revision)? {
            return Ok(ApplyOutcome::replayed());
        }
        self.backend.write(|tx| {
            let meta_key = encode_kv_meta_key(key);

            // 读取已有元数据（检查 Key 是否存在且未被删除）
            let existing_meta = tx
                .get(TABLE_KV_META, &meta_key)?
                .and_then(|bytes| KvMetadata::from_bytes(&bytes));

            if let Some(m) = existing_meta.filter(|m| !m.deleted) {
                // Key 存在且未被删除：标记为已删除
                let meta = m.mark_deleted(revision);
                tx.insert(TABLE_KV_META, &meta_key, &meta.to_bytes())?;
            }
            // 无论是否存在，均写 changelog（D-A5：始终消耗 revision）

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

            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;

            Ok(ApplyOutcome::applied())
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
                        Ok(Some(self.decrypt_value(&internal_key, &data)?))
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
                        self.decrypt_value(&ik, &v)?
                    };
                    results.push((user_key.to_vec(), plaintext));
                }
            }
            Ok(results)
        })
    }

    /// Range 操作：半开区间 `[start, range_end)` 扫描（R-SVC-07，etcd 语义）
    ///
    /// 返回满足 `start <= user_key < range_end` 的所有 Key（按字典序）。
    /// 与 `range()`（前缀扫描）不同，本方法使用底层 `iter_range` 从 `start`
    /// 扫描到 `range_end`，区间内不共享 `start` 前缀的 Key 也能命中。
    /// 通过元数据的 deleted 标志过滤已删除的 Key。
    pub fn range_in(
        &self,
        start: &[u8],
        range_end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let internal_start = encode_kv_key(start);
        let internal_end = encode_kv_key(range_end);
        self.backend.read(|tx| {
            let all = tx.iter_range(TABLE_KV, &internal_start, &internal_end)?;
            let mut results = Vec::new();
            for (ik, v) in all {
                if results.len() >= limit && limit > 0 {
                    break;
                }
                if let Some(user_key) = decode_kv_key(&ik) {
                    // iter_range 已保证 [start, range_end)，此处防御性复核
                    if !range_end.is_empty() && user_key >= range_end {
                        break;
                    }
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
                        self.decrypt_value(&ik, &v)?
                    };
                    results.push((user_key.to_vec(), plaintext));
                }
            }
            Ok(results)
        })
    }

    /// Range 历史读：返回 `[start, range_end)` 在目标 revision 时刻的视图（R-SVC-07-2）
    ///
    /// 通过回放 `[1, target_revision]` 的 changelog 重建历史视图（与 `get_at_revision`
    /// 同一模式）。changelog 中 value 为明文（put 时直接写入），故无需解密。
    /// 注意：早于 `compacted_revision` 的历史不可达（与 `get_at_revision` 一致）。
    pub fn range_at_revision(
        &self,
        start: &[u8],
        range_end: &[u8],
        limit: usize,
        target_revision: Revision,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let end_key = encode_changelog_key(target_revision.saturating_add(1));
        self.backend.read(|tx| {
            let entries = tx.iter_prefix(TABLE_CHANGELOG, CHANGELOG_PREFIX)?;
            // BTreeMap 保持 Key 字典序
            let mut view: std::collections::BTreeMap<Vec<u8>, Vec<u8>> =
                std::collections::BTreeMap::new();
            for (ch_key, ch_value) in entries {
                if ch_key.as_slice() >= end_key.as_slice() {
                    break;
                }
                if let Ok(event) = ChangeEvent::from_bytes(&ch_value) {
                    for change in &event.changes {
                        if change.key.as_slice() < start {
                            continue;
                        }
                        if !range_end.is_empty() && change.key.as_slice() >= range_end {
                            continue;
                        }
                        match &change.value {
                            Some(v) => {
                                view.insert(change.key.clone(), v.clone());
                            }
                            None => {
                                view.remove(&change.key);
                            }
                        }
                    }
                }
            }
            let mut results = Vec::new();
            for (k, v) in view {
                if results.len() >= limit && limit > 0 {
                    break;
                }
                results.push((k, v));
            }
            Ok(results)
        })
    }

    /// 范围删除（raft apply 路径，R-SVC-07-3）：单个写事务内原子标记
    /// `[start, range_end)` 内所有未删除 Key 为 tombstone。
    ///
    /// 与 `delete_at_revision` 相同的幂等守卫（D-A3）与 applied 持久化（D-A4）。
    pub fn delete_range_at_revision(
        &self,
        start: &[u8],
        range_end: &[u8],
        revision: Revision,
        applied: AppliedLogId,
    ) -> Result<DeleteRangeOutcome> {
        if self.changelog_contains_revision(revision)? {
            return Ok(DeleteRangeOutcome {
                deleted_keys: Vec::new(),
                replayed: true,
            });
        }
        let internal_start = encode_kv_key(start);
        let internal_end = encode_kv_key(range_end);
        self.backend.write(|tx| {
            let all = tx.iter_range(TABLE_KV, &internal_start, &internal_end)?;
            let mut deleted_keys = Vec::new();
            for (ik, _v) in all {
                if let Some(user_key) = decode_kv_key(&ik) {
                    if !range_end.is_empty() && user_key >= range_end {
                        break;
                    }
                    let meta_key = encode_kv_meta_key(user_key);
                    let existing_meta = tx
                        .get(TABLE_KV_META, &meta_key)?
                        .and_then(|bytes| KvMetadata::from_bytes(&bytes));
                    if let Some(m) = existing_meta.filter(|m| !m.deleted) {
                        let meta = m.mark_deleted(revision);
                        tx.insert(TABLE_KV_META, &meta_key, &meta.to_bytes())?;
                        deleted_keys.push(user_key.to_vec());
                    }
                }
            }

            // 始终写 changelog（D-A5：始终消耗 revision）
            let event = ChangeEvent {
                revision,
                changes: deleted_keys
                    .iter()
                    .map(|k| KeyValueChange {
                        key: k.clone(),
                        value: None,
                        prev_value: None,
                    })
                    .collect(),
                event_type: EventType::Delete,
            };
            tx.insert(
                TABLE_CHANGELOG,
                &encode_changelog_key(revision),
                &event.to_bytes(),
            )?;

            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;

            Ok(DeleteRangeOutcome {
                deleted_keys,
                replayed: false,
            })
        })
    }

    /// 读取 Key 的元数据（version, create_revision, mod_revision, lease_id）
    pub fn get_kv_metadata(&self, key: &[u8]) -> Result<Option<KvMetadata>> {
        let meta_key = encode_kv_meta_key(key);
        self.backend
            .read(|tx| tx.get(TABLE_KV_META, &meta_key))
            .map(|opt| opt.and_then(|bytes| KvMetadata::from_bytes(&bytes)))
    }

    /// 在写事务内标记删除所有绑定到指定 Lease 的 Key（不写 changelog，由调用方统一写入）
    ///
    /// 返回被标记删除的 Key 列表（用于构造 Lease Revoke 的 changelog 事件）。
    /// 仅用于 raft apply 路径（P0-B：任何路径不得直写本地存储）。
    fn delete_keys_by_lease_in_tx(
        tx: &mut dyn WriteTx,
        target_lease_id: i64,
        revision: Revision,
    ) -> Result<Vec<Vec<u8>>> {
        let all_meta = tx.iter_prefix(TABLE_KV_META, KV_META_PREFIX)?;
        let mut deleted_keys = Vec::new();
        for (meta_key_bytes, meta_value) in &all_meta {
            if let Some(meta) = KvMetadata::from_bytes(meta_value) {
                if meta.lease_id == target_lease_id
                    && !meta.deleted
                    && meta_key_bytes.starts_with(KV_META_PREFIX)
                {
                    let user_key = &meta_key_bytes[KV_META_PREFIX.len()..];
                    let meta_key = encode_kv_meta_key(user_key);
                    let tombstone = meta.mark_deleted(revision);
                    tx.insert(TABLE_KV_META, &meta_key, &tombstone.to_bytes())?;
                    deleted_keys.push(user_key.to_vec());
                }
            }
        }
        Ok(deleted_keys)
    }

    /// Txn 原子事务执行（单节点模式入口）
    pub fn execute_txn(
        &self,
        compares: &[crate::txn::TxnCompare],
        success_ops: &[crate::txn::TxnOp],
        failure_ops: &[crate::txn::TxnOp],
    ) -> Result<crate::txn::TxnResult> {
        let _guard = self.standalone_lock.lock();
        let revision = self.current_revision().saturating_add(1);
        let applied = AppliedLogId::standalone(revision);
        self.execute_txn_at_revision(compares, success_ops, failure_ops, revision, applied)
    }

    /// Txn 原子事务执行（raft apply 路径）：revision ≡ log index（D-A2）
    ///
    /// 幂等守卫（D-A3）：该 revision 的 changelog 已存在则跳过并返回重放标记。
    pub fn execute_txn_at_revision(
        &self,
        compares: &[crate::txn::TxnCompare],
        success_ops: &[crate::txn::TxnOp],
        failure_ops: &[crate::txn::TxnOp],
        revision: Revision,
        applied: AppliedLogId,
    ) -> Result<crate::txn::TxnResult> {
        use crate::txn::TxnResult;

        if self.changelog_contains_revision(revision)? {
            return Ok(TxnResult {
                succeeded: false,
                revision,
                responses: Vec::new(),
            });
        }

        self.backend.write(|tx| {
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

            // 5. 持久化 applied 状态（D-A4）
            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;

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

            let raw_meta = tx
                .get(TABLE_KV_META, &meta_key)?
                .and_then(|bytes| KvMetadata::from_bytes(&bytes));

            // 软删除的 Key 应视为不存在：过滤掉 deleted=true 的元数据
            let meta = raw_meta.filter(|m| !m.deleted);

            // Value 读取同样受软删除影响：deleted=true 时视为无值
            let is_deleted = raw_meta.map(|m| m.deleted).unwrap_or(false);
            let value = if is_deleted {
                None
            } else {
                tx.get(TABLE_KV, &kv_key)?
            };

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
                    if let crate::txn::CompareValue::CreateRevision(target_v) = &cmp.target_value {
                        let actual = crate::txn::CompareValue::CreateRevision(actual_create_rev);
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

                // 写入用户数据（经过 Barrier 加密，仅 /kv/ 前缀）
                let encrypted = self.encrypt_value(&internal_key, value)?;
                tx.insert(TABLE_KV, &internal_key, &encrypted)?;
                tx.insert(TABLE_KV_META, &meta_key, &meta.to_bytes())?;

                let change = KeyValueChange {
                    key: key.to_vec(),
                    value: Some(value.to_vec()),
                    prev_value: None,
                };

                Ok((TxnOpResponse::Put { revision }, Some(change)))
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

                Ok((TxnOpResponse::Delete { revision }, Some(change)))
            }
            TxnOp::Range {
                key,
                range_end,
                limit,
            } => {
                // R-SVC-07-4：与顶层 range()/range_in() 对齐——
                // ① 区间扫描（range_end 为空时回退前缀扫描语义）；
                // ② 必须检查 KV_META 的 deleted 标志（值非空的软删 key 不得复活）。
                let internal_start = encode_kv_key(key);
                let all = if range_end.is_empty() {
                    tx.iter_prefix(TABLE_KV, &internal_start)?
                } else {
                    let internal_end = encode_kv_key(range_end);
                    tx.iter_range(TABLE_KV, &internal_start, &internal_end)?
                };

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
                        // 半开区间上界（iter_range 已保证，防御性复核）
                        if !range_end.is_empty() && user_key >= range_end.as_slice() {
                            break;
                        }
                        // 检查元数据 deleted 标志（R-SVC-07-4）
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
                            self.decrypt_value(&ik, &v)?
                        };
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

    // ──── Lease 状态表（P0-B：raft 状态机内持久化） ────

    /// 读取 `/_lease/{id}` 记录
    pub fn get_lease_record(&self, lease_id: i64) -> Result<Option<LeaseRecord>> {
        let lease_key = encode_lease_key(lease_id);
        self.backend
            .read(|tx| tx.get(TABLE_KV, &lease_key))
            .map(|opt| opt.and_then(|bytes| LeaseRecord::from_bytes(&bytes)))
    }

    /// 列出全部持久化 Lease 记录（`/_lease/` 前缀），返回 `(lease_id, record)`。
    ///
    /// 供 P0-B failover 重建使用：新 leader 从状态机读出全部 Lease
    /// 重建内存 TTL 视图；损坏条目跳过并计数（由调用方决定日志级别）。
    pub fn list_lease_records(&self) -> Result<Vec<(i64, LeaseRecord)>> {
        let rows = self
            .backend
            .read(|tx| tx.iter_prefix(TABLE_KV, LEASE_PREFIX))?;
        let mut records = Vec::with_capacity(rows.len());
        for (key, value) in rows {
            let Some(id_bytes) = key.strip_prefix(LEASE_PREFIX) else {
                continue;
            };
            let Some(id_bytes): Option<[u8; 8]> = id_bytes.try_into().ok() else {
                tracing::warn!("corrupt lease key in storage: {} bytes", key.len());
                continue;
            };
            let id = i64::from_be_bytes(id_bytes);
            if let Some(record) = LeaseRecord::from_bytes(&value) {
                records.push((id, record));
            } else {
                tracing::warn!("corrupt lease record for lease id {id}, skipped");
            }
        }
        Ok(records)
    }

    /// 应用 LeaseOp（单节点模式入口）：锁内分配 revision，防止并发同 revision 冲突
    pub fn apply_lease_op_standalone(
        &self,
        op: &crate::raft::type_config::LeaseOp,
    ) -> Result<Revision> {
        let _guard = self.standalone_lock.lock();
        let revision = self.current_revision().saturating_add(1);
        self.apply_lease_op(op, revision, AppliedLogId::standalone(revision))?;
        Ok(revision)
    }

    /// 应用 LeaseOp（raft apply 路径）：revision ≡ log index（D-A2）
    ///
    /// Grant/KeepAlive 写 `/_lease/{id}`；Revoke 删除记录并按 `KvMetadata.lease_id`
    /// 扫描删除绑定 Key。全部与 changelog + META_LAST_APPLIED 同事务原子完成。
    /// 返回 apply 结果与被删除的绑定 Key（供 Watch 事件构造）。
    pub fn apply_lease_op(
        &self,
        op: &crate::raft::type_config::LeaseOp,
        revision: Revision,
        applied: AppliedLogId,
    ) -> Result<(ApplyOutcome, Vec<KeyValueChange>)> {
        use crate::raft::type_config::LeaseOp;

        if self.changelog_contains_revision(revision)? {
            return Ok((ApplyOutcome::replayed(), Vec::new()));
        }

        self.backend.write(|tx| {
            let mut changes: Vec<KeyValueChange> = Vec::new();

            match op {
                LeaseOp::Grant {
                    id,
                    ttl,
                    deadline_wall_ms,
                } => {
                    let record = LeaseRecord {
                        ttl: *ttl,
                        deadline_wall_ms: *deadline_wall_ms,
                        keepalive_revision: revision as i64,
                    };
                    tx.insert(TABLE_KV, &encode_lease_key(*id), &record.to_bytes())?;
                }
                LeaseOp::KeepAlive {
                    id,
                    deadline_wall_ms,
                } => {
                    let lease_key = encode_lease_key(*id);
                    if let Some(record) = tx
                        .get(TABLE_KV, &lease_key)?
                        .and_then(|bytes| LeaseRecord::from_bytes(&bytes))
                    {
                        let updated = LeaseRecord {
                            deadline_wall_ms: *deadline_wall_ms,
                            keepalive_revision: revision as i64,
                            ..record
                        };
                        tx.insert(TABLE_KV, &lease_key, &updated.to_bytes())?;
                    }
                }
                LeaseOp::Revoke { id, delete_keys } => {
                    tx.remove(TABLE_KV, &encode_lease_key(*id))?;
                    if *delete_keys {
                        let deleted = Self::delete_keys_by_lease_in_tx(tx, *id, revision)?;
                        for key in deleted {
                            changes.push(KeyValueChange {
                                key,
                                value: None,
                                prev_value: None,
                            });
                        }
                    }
                }
            }

            // Changelog（Lease 事件；Revoke 时携带被删 Key 供 Watch 分发）
            let event = ChangeEvent {
                revision,
                changes: changes.clone(),
                event_type: EventType::Lease,
            };
            tx.insert(
                TABLE_CHANGELOG,
                &encode_changelog_key(revision),
                &event.to_bytes(),
            )?;

            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;

            Ok((ApplyOutcome::applied(), changes))
        })
    }

    /// 应用 AuthOp（raft apply 路径，P0-C.2）：revision ≡ log index（D-A2）
    ///
    /// 用户/角色/吊销登记写入 `/_sys/auth/` 前缀（原始 bincode，不经 Barrier
    /// 加密——auth 元数据非密文，与 Lease 记录同口径），与 changelog +
    /// META_LAST_APPLIED 同事务原子完成；幂等守卫同 lease 路径。
    pub fn apply_auth_op(
        &self,
        op: &crate::raft::type_config::AuthOp,
        revision: Revision,
        applied: AppliedLogId,
    ) -> Result<ApplyOutcome> {
        use crate::auth::manager::{
            AuthRevocationRecord, AuthRoleRecord, AuthSessionRecord, AuthUserRecord,
            AUTH_REVOKED_PREFIX, AUTH_ROLE_PREFIX, AUTH_SESSION_PREFIX, AUTH_USER_PREFIX,
        };
        use crate::raft::type_config::AuthOp;

        if self.changelog_contains_revision(revision)? {
            return Ok(ApplyOutcome::replayed());
        }

        self.backend.write(|tx| {
            match op {
                AuthOp::UserAdd { name, hash, roles } => {
                    let key = [AUTH_USER_PREFIX, name.as_bytes()].concat();
                    let rec = AuthUserRecord {
                        name: name.clone(),
                        password_hash: hash.as_bytes().to_vec(),
                        roles: roles.clone(),
                    };
                    tx.insert(TABLE_KV, &key, &rec.to_bytes()?)?;
                }
                AuthOp::UserDelete { name } => {
                    let key = [AUTH_USER_PREFIX, name.as_bytes()].concat();
                    tx.remove(TABLE_KV, &key)?;
                }
                AuthOp::UserSetPassword { name, hash } => {
                    let key = [AUTH_USER_PREFIX, name.as_bytes()].concat();
                    if let Some(rec) = tx
                        .get(TABLE_KV, &key)?
                        .and_then(|bytes| AuthUserRecord::from_bytes(&bytes))
                    {
                        let updated = AuthUserRecord {
                            password_hash: hash.as_bytes().to_vec(),
                            ..rec
                        };
                        tx.insert(TABLE_KV, &key, &updated.to_bytes()?)?;
                    }
                }
                AuthOp::UserGrantRole { name, role } => {
                    let key = [AUTH_USER_PREFIX, name.as_bytes()].concat();
                    if let Some(rec) = tx
                        .get(TABLE_KV, &key)?
                        .and_then(|bytes| AuthUserRecord::from_bytes(&bytes))
                    {
                        let mut roles = rec.roles;
                        if !roles.iter().any(|r| r == role) {
                            roles.push(role.clone());
                        }
                        let updated = AuthUserRecord { roles, ..rec };
                        tx.insert(TABLE_KV, &key, &updated.to_bytes()?)?;
                    }
                }
                AuthOp::UserRevokeRole { name, role } => {
                    let key = [AUTH_USER_PREFIX, name.as_bytes()].concat();
                    if let Some(rec) = tx
                        .get(TABLE_KV, &key)?
                        .and_then(|bytes| AuthUserRecord::from_bytes(&bytes))
                    {
                        let roles: Vec<String> =
                            rec.roles.into_iter().filter(|r| r != role).collect();
                        let updated = AuthUserRecord { roles, ..rec };
                        tx.insert(TABLE_KV, &key, &updated.to_bytes()?)?;
                    }
                }
                AuthOp::RoleAdd { role } => {
                    let key = [AUTH_ROLE_PREFIX, role.as_bytes()].concat();
                    if tx.get(TABLE_KV, &key)?.is_none() {
                        let rec = AuthRoleRecord {
                            name: role.clone(),
                            permissions: Vec::new(),
                            capability_grants: Vec::new(),
                            high_sensitive: false,
                        };
                        tx.insert(TABLE_KV, &key, &rec.to_bytes()?)?;
                    }
                }
                AuthOp::RoleDelete { role } => {
                    let key = [AUTH_ROLE_PREFIX, role.as_bytes()].concat();
                    tx.remove(TABLE_KV, &key)?;
                    // 同时从所有用户移除该角色
                    let users = tx.iter_prefix(TABLE_KV, AUTH_USER_PREFIX)?;
                    let mut updates: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
                    for (k, v) in users {
                        if let Some(rec) = AuthUserRecord::from_bytes(&v) {
                            let before = rec.roles.len();
                            let roles: Vec<String> =
                                rec.roles.into_iter().filter(|r| r != role).collect();
                            if roles.len() != before {
                                let updated = AuthUserRecord { roles, ..rec };
                                updates.push((k, updated.to_bytes()?));
                            }
                        }
                    }
                    for (k, v) in updates {
                        tx.insert(TABLE_KV, &k, &v)?;
                    }
                }
                AuthOp::RoleGrantPermission {
                    role,
                    perm_type,
                    key,
                    range_end,
                } => {
                    let role_key = [AUTH_ROLE_PREFIX, role.as_bytes()].concat();
                    if let Some(rec) = tx
                        .get(TABLE_KV, &role_key)?
                        .and_then(|bytes| AuthRoleRecord::from_bytes(&bytes))
                    {
                        use crate::auth::manager::AuthPermissionRecord;
                        let is_dup = rec.permissions.iter().any(|p| {
                            p.perm_type == *perm_type
                                && p.key_prefix == *key
                                && p.range_end == *range_end
                        });
                        if !is_dup {
                            let mut updated = rec;
                            updated.permissions.push(AuthPermissionRecord {
                                perm_type: *perm_type,
                                key_prefix: key.clone(),
                                range_end: range_end.clone(),
                            });
                            tx.insert(TABLE_KV, &role_key, &updated.to_bytes()?)?;
                        }
                    }
                }
                AuthOp::RoleRevokePermission {
                    role,
                    key,
                    range_end,
                } => {
                    let role_key = [AUTH_ROLE_PREFIX, role.as_bytes()].concat();
                    if let Some(rec) = tx
                        .get(TABLE_KV, &role_key)?
                        .and_then(|bytes| AuthRoleRecord::from_bytes(&bytes))
                    {
                        let mut updated = rec;
                        updated
                            .permissions
                            .retain(|p| !(p.key_prefix == *key && p.range_end == *range_end));
                        tx.insert(TABLE_KV, &role_key, &updated.to_bytes()?)?;
                    }
                }
                AuthOp::RevokeJti { jti } => {
                    let key = [AUTH_REVOKED_PREFIX, jti.as_bytes()].concat();
                    if tx.get(TABLE_KV, &key)?.is_none() {
                        let rec = AuthRevocationRecord {
                            jti: jti.clone(),
                            revoked_at: crate::lease::wall_clock_now_ms() / 1000,
                        };
                        tx.insert(TABLE_KV, &key, &rec.to_bytes()?)?;
                    }
                }
                AuthOp::IssueSession {
                    hash_hex,
                    username,
                    expires_at_unix,
                    is_refresh,
                } => {
                    // P2-07：会话落盘（键为 token 哈希，值不含明文 token）
                    let key = [AUTH_SESSION_PREFIX, hash_hex.as_bytes()].concat();
                    let rec = AuthSessionRecord {
                        username: username.clone(),
                        expires_at_unix: *expires_at_unix,
                        is_refresh: *is_refresh,
                    };
                    tx.insert(TABLE_KV, &key, &rec.to_bytes()?)?;
                }
                AuthOp::ConsumeSession { hash_hex } => {
                    // P2-07：会话消费（refresh 单次使用 / 登出 / 吊销）
                    let key = [AUTH_SESSION_PREFIX, hash_hex.as_bytes()].concat();
                    tx.remove(TABLE_KV, &key)?;
                }
            }

            // Changelog（Auth 事件：无 Key 变更，仅记录 revision 供 Watch 水位推进）
            let event = ChangeEvent {
                revision,
                changes: Vec::new(),
                event_type: EventType::Put,
            };
            tx.insert(
                TABLE_CHANGELOG,
                &encode_changelog_key(revision),
                &event.to_bytes(),
            )?;

            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;

            Ok(ApplyOutcome::applied())
        })
    }

    // ──── Compact（P1-01：raft 下发 compact revision，节点一致）────

    /// 读取已持久化的 compacted revision（`META_COMPACT_REVISION`）。
    ///
    /// 无持久化条目时返回 0（从未压缩）。
    pub fn compacted_revision(&self) -> Result<Revision> {
        self.backend
            .read(|tx| tx.get(TABLE_META, META_COMPACT_REVISION))
            .map(|opt| {
                opt.and_then(|bytes| {
                    let arr: [u8; 8] = bytes.as_slice().try_into().ok()?;
                    Some(u64::from_be_bytes(arr))
                })
                .unwrap_or(0)
            })
    }

    /// 应用 Compact：物理删除 `< revision` 的 changelog 条目与过期 tombstone，
    /// 并持久化 `META_COMPACT_REVISION`（与 `META_LAST_APPLIED` 同事务）。
    ///
    /// 语义（P1-01 设计决策）：
    /// - **确定性**：所有节点 apply 同一命令得到相同删除集合（删除条件只依赖
    ///   revision 与持久化状态，不读墙钟/随机数，规格 A.4 约束 1）；
    /// - **分片删除**：每批 `COMPACT_BATCH_SIZE` 条一个写事务，避免巨型事务；
    /// - **幂等**：`revision <= 已持久化 compacted_revision` 为 no-op（重启重放安全）；
    /// - **不得失败**：openraft 将 apply 错误视为致命，故 `revision > applied.index`
    ///   时钳制为 applied.index（未来 revision 的拒绝由 RPC/提案层负责，错误码
    ///   `INVALID_ARGUMENT`）。
    pub fn apply_compact(
        &self,
        revision: Revision,
        applied: AppliedLogId,
    ) -> Result<CompactOutcome> {
        let effective = revision.min(applied.index);
        if effective == 0 {
            return Ok(CompactOutcome {
                deleted_changelog: 0,
                deleted_tombstones: 0,
            });
        }
        let prev = self.compacted_revision()?;
        if effective <= prev {
            return Ok(CompactOutcome {
                deleted_changelog: 0,
                deleted_tombstones: 0,
            });
        }

        let mut deleted_changelog = 0usize;
        let mut deleted_tombstones = 0usize;

        // 分片删除：每批一个写事务（P1-01：单写事务分片删除）
        loop {
            let mut batch_changelog = 0usize;
            let mut batch_tombstones = 0usize;
            self.backend.write(|tx| {
                // 1. changelog 条目（rev < effective；key 编码 /_changelog/{rev_be}）
                let mut stale: Vec<Vec<u8>> = Vec::new();
                for (key, _) in tx.iter_prefix(TABLE_CHANGELOG, CHANGELOG_PREFIX)? {
                    if stale.len() >= COMPACT_BATCH_SIZE {
                        break;
                    }
                    let rev = key
                        .get(CHANGELOG_PREFIX.len()..)
                        .and_then(|tail| <[u8; 8]>::try_from(tail).ok())
                        .map(u64::from_be_bytes);
                    if rev.is_some_and(|r| r < effective) {
                        stale.push(key.to_vec());
                    }
                }
                for key in &stale {
                    tx.remove(TABLE_CHANGELOG, key)?;
                }
                batch_changelog = stale.len();

                // 2. 过期 tombstone（deleted && mod_revision < effective）：
                //    物理删除 KV 行 + 元数据行
                let mut stale_tomb: Vec<Vec<u8>> = Vec::new();
                for (meta_key, meta_value) in tx.iter_prefix(TABLE_KV_META, KV_META_PREFIX)? {
                    if stale_tomb.len() >= COMPACT_BATCH_SIZE {
                        break;
                    }
                    if let Some(meta) = KvMetadata::from_bytes(&meta_value) {
                        if meta.deleted && (meta.mod_revision as u64) < effective {
                            stale_tomb.push(meta_key.to_vec());
                        }
                    }
                }
                for meta_key in &stale_tomb {
                    if let Some(user_key) = meta_key.strip_prefix(KV_META_PREFIX) {
                        let internal_key = encode_kv_key(user_key);
                        tx.remove(TABLE_KV, &internal_key)?;
                    }
                    tx.remove(TABLE_KV_META, meta_key)?;
                }
                batch_tombstones = stale_tomb.len();
                Ok(())
            })?;

            deleted_changelog += batch_changelog;
            deleted_tombstones += batch_tombstones;
            if batch_changelog == 0 && batch_tombstones == 0 {
                break;
            }
        }

        // 3. 持久化 compacted revision + applied（同一事务）
        self.backend.write(|tx| {
            tx.insert(TABLE_META, META_COMPACT_REVISION, &effective.to_be_bytes())?;
            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;
            Ok(())
        })?;

        tracing::info!(
            "Compaction applied: revision={effective}, deleted_changelog={deleted_changelog}, \
             deleted_tombstones={deleted_tombstones}"
        );

        Ok(CompactOutcome {
            deleted_changelog,
            deleted_tombstones,
        })
    }

    /// 原始前缀扫描（不经 Barrier 解密）：供 `/_sys/auth/`、`/_lease/` 等
    /// 内部元数据前缀的启动装载使用（P0-C.2）。
    pub fn list_raw_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.backend.read(|tx| {
            let rows = tx.iter_prefix(TABLE_KV, prefix)?;
            let mut out = Vec::new();
            for (k, v) in rows {
                out.push((k.to_vec(), v.to_vec()));
            }
            Ok(out)
        })
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

    /// 严格读取 Changelog（P0-E.2）：损坏条目返回 Err（不再静默跳过）。
    ///
    /// Watch 历史回放使用此方法：损坏即中止回放并下发 `HistoryUnavailable`，
    /// 客户端不再收到静默缺洞。
    pub fn read_changelog_entries_strict(
        &self,
        start_revision: Revision,
    ) -> Result<Vec<ChangeEvent>> {
        let start_key = encode_changelog_key(start_revision);
        self.backend.read(|tx| {
            let entries = tx.iter_prefix(TABLE_CHANGELOG, CHANGELOG_PREFIX)?;
            let mut events = Vec::new();
            for (key, value) in entries {
                if key.as_slice() < start_key.as_slice() {
                    continue;
                }
                match ChangeEvent::from_bytes(&value) {
                    Ok(event) => events.push(event),
                    Err(_) => {
                        return Err(coord_core::error::Error::DataCorruption(format!(
                            "changelog entry at key {} is corrupt",
                            String::from_utf8_lossy(&key)
                        )))
                    }
                }
            }
            Ok(events)
        })
    }

    /// 读取 Key 在指定历史 Revision 时的值
    ///
    /// 通过扫描 Changelog 找到该 Key 在 <= target_revision 时的最后一次写入值。
    /// 如果 Key 在 target_revision 时不存在或已被删除，返回 None。
    pub fn get_at_revision(
        &self,
        key: &[u8],
        target_revision: Revision,
    ) -> Result<Option<Vec<u8>>> {
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
        // P0-E.2：严格读取，损坏条目报错（不静默跳过）
        MvccStorage::read_changelog_entries_strict(self, start_revision)
            .map_err(|e| format!("changelog read error: {e}"))
    }

    fn compacted_revision(&self) -> std::result::Result<Revision, String> {
        // P1-01：压缩水位（回放起点低于它时历史不可达）
        MvccStorage::compacted_revision(self).map_err(|e| format!("compacted revision: {e}"))
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

    // ──── R-SVC-07：半开区间 [key, range_end) 语义 ────

    #[test]
    fn test_range_in_half_open_interval() {
        let (_dir, storage) = create_storage();
        storage.put(b"a", b"1", None).unwrap();
        storage.put(b"ab", b"2", None).unwrap();
        storage.put(b"abc", b"3", None).unwrap();
        storage.put(b"b", b"4", None).unwrap();
        storage.put(b"c", b"5", None).unwrap();

        // [a, b) = a, ab, abc（不含 b）
        let results = storage.range_in(b"a", b"b", 0).unwrap();
        let keys: Vec<&[u8]> = results.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(
            keys,
            vec![b"a".as_slice(), b"ab".as_slice(), b"abc".as_slice()]
        );

        // [b, c) = b（不含 c）
        let results = storage.range_in(b"b", b"c", 0).unwrap();
        let keys: Vec<&[u8]> = results.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(keys, vec![b"b".as_slice()]);

        // 区间内 key 不共享 start 前缀（start=ab, end=c 应含 abc、b 不含 ab）
        let results = storage.range_in(b"ab", b"c", 0).unwrap();
        let keys: Vec<&[u8]> = results.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(
            keys,
            vec![b"ab".as_slice(), b"abc".as_slice(), b"b".as_slice()]
        );

        // limit 生效
        let results = storage.range_in(b"a", b"b", 2).unwrap();
        assert_eq!(results.len(), 2);

        // 软删除的 key 不可见
        storage.delete(b"ab").unwrap();
        let results = storage.range_in(b"a", b"b", 0).unwrap();
        let keys: Vec<&[u8]> = results.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(keys, vec![b"a".as_slice(), b"abc".as_slice()]);
    }

    #[test]
    fn test_delete_range_at_revision_atomic() {
        let (_dir, storage) = create_storage();
        storage.put(b"a", b"1", None).unwrap();
        storage.put(b"ab", b"2", None).unwrap();
        storage.put(b"b", b"3", None).unwrap();

        // 原子删除 [a, b)
        let outcome = storage
            .delete_range_at_revision(
                b"a",
                b"b",
                4,
                AppliedLogId {
                    term: 1,
                    node_id: 1,
                    index: 4,
                },
            )
            .unwrap();
        assert!(!outcome.replayed);
        assert_eq!(outcome.deleted_keys, vec![b"a".to_vec(), b"ab".to_vec()]);

        // 范围内已删除、范围外保留
        assert_eq!(storage.get(b"a").unwrap(), None);
        assert_eq!(storage.get(b"ab").unwrap(), None);
        assert_eq!(storage.get(b"b").unwrap(), Some(b"3".to_vec()));

        // 幂等守卫：同 revision 重放不产生副作用
        let replay = storage
            .delete_range_at_revision(
                b"a",
                b"b",
                4,
                AppliedLogId {
                    term: 1,
                    node_id: 1,
                    index: 4,
                },
            )
            .unwrap();
        assert!(replay.replayed);

        // 范围读确认无残留
        assert!(storage.range_in(b"a", b"b", 0).unwrap().is_empty());
    }

    #[test]
    fn test_range_at_revision_historical_view() {
        let (_dir, storage) = create_storage();
        storage.put(b"a", b"v1", None).unwrap(); // rev 1
        storage.put(b"b", b"v1", None).unwrap(); // rev 2
        storage.put(b"a", b"v2", None).unwrap(); // rev 3

        // rev 2 历史视图：a=v1, b=v1
        let results = storage.range_at_revision(b"a", b"c", 0, 2).unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.contains(&(b"a".to_vec(), b"v1".to_vec())));
        assert!(results.contains(&(b"b".to_vec(), b"v1".to_vec())));

        // rev 3 历史视图：a=v2, b=v1
        let results = storage.range_at_revision(b"a", b"c", 0, 3).unwrap();
        assert!(results.contains(&(b"a".to_vec(), b"v2".to_vec())));

        // 删除后的历史视图：rev 3 前 b 存在，rev 4（删除 b）后不可见
        storage.delete(b"b").unwrap(); // rev 4
        let results = storage.range_at_revision(b"a", b"c", 0, 3).unwrap();
        assert!(results.contains(&(b"b".to_vec(), b"v1".to_vec())));
        let results = storage.range_at_revision(b"a", b"c", 0, 4).unwrap();
        assert!(!results.iter().any(|(k, _)| k == b"b"));
    }

    /// R-SVC-07-4：Txn 内 Range 必须过滤已软删除的 Key（与顶层 range() 对齐）
    #[test]
    fn test_txn_range_filters_deleted_keys() {
        use crate::txn::{TxnOp, TxnOpResponse};

        let (_dir, storage) = create_storage();
        storage.put(b"a", b"1", None).unwrap();
        storage.put(b"b", b"2", None).unwrap();
        // 软删除 a（值仍在 /kv/ 中，仅 KV_META 标记 deleted）
        storage.delete(b"a").unwrap();

        let result = storage
            .execute_txn(
                &[],
                &[TxnOp::Range {
                    key: b"a".to_vec(),
                    range_end: b"c".to_vec(),
                    limit: 0,
                }],
                &[],
            )
            .unwrap();
        assert_eq!(result.responses.len(), 1);
        match &result.responses[0] {
            TxnOpResponse::Range { kvs, count, .. } => {
                assert_eq!(*count, 1, "已删除 key a 不得在 Txn Range 中复活");
                assert_eq!(kvs.len(), 1);
                assert_eq!(kvs[0].0, b"b".to_vec());
            }
            other => panic!("expected Range response, got {:?}", other),
        }
    }

    #[test]
    fn test_applied_log_id() {
        let (_dir, storage) = create_storage();
        assert_eq!(storage.get_applied_log_id().unwrap(), None);

        storage
            .set_last_applied(AppliedLogId::standalone(42))
            .unwrap();
        assert_eq!(
            storage.get_applied_log_id().unwrap(),
            Some(AppliedLogId::standalone(42))
        );
        assert_eq!(storage.current_revision(), 42);
    }

    // ──── R-SEC-01：静态加密接线（Barrier/Seal/Unseal） ────

    #[test]
    fn test_barrier_encrypts_user_values_on_disk() {
        use crate::security::key_management::Keyring;
        use std::sync::Arc;

        let (_dir, storage) = create_storage();
        let (keyring, _dek) = Keyring::bootstrap_from_root_key(&[0xABu8; 32]).unwrap();
        storage.set_barrier(crate::security::barrier::Barrier::new(Arc::new(keyring)));

        storage.put(b"secret", b"super-secret-value", None).unwrap();

        // 落盘原始字节不得含明文用户值
        let raw = storage
            .backend()
            .read(|tx| tx.get(TABLE_KV, &encode_kv_key(b"secret")))
            .unwrap()
            .unwrap();
        assert!(
            !raw.windows(b"super-secret-value".len())
                .any(|w| w == b"super-secret-value"),
            "on-disk value must not contain plaintext"
        );
        assert!(raw.len() >= 32, "ciphertext must carry barrier header");

        // 读取仍返回明文（Barrier 透明解密）
        assert_eq!(
            storage.get(b"secret").unwrap(),
            Some(b"super-secret-value".to_vec())
        );
        // 范围读同样解密
        let results = storage.range_in(b"sec", b"secx", 0).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, b"super-secret-value".to_vec());
    }

    #[test]
    fn test_barrier_seal_blocks_writes_after_unseal_restores() {
        use crate::security::key_management::Keyring;
        use std::sync::Arc;

        let (_dir, storage) = create_storage();
        let root_key = [0xCDu8; 32];
        let (keyring, dek) = Keyring::bootstrap_from_root_key(&root_key).unwrap();
        let keyring = Arc::new(keyring);
        storage.set_barrier(crate::security::barrier::Barrier::new(Arc::clone(&keyring)));
        storage.put(b"a", b"v1", None).unwrap();

        // Seal → 写入被拒（active_dek 返回错误，非全零密钥）
        keyring.seal();
        assert!(keyring.is_sealed());
        assert!(
            storage.put(b"b", b"v2", None).is_err(),
            "sealed: writes refused"
        );

        // Unseal（root 密钥重建）→ 读取正常
        let recovered = Keyring::from_root_key(&root_key, &[dek]).unwrap();
        assert!(!recovered.is_sealed());
        storage.set_barrier(crate::security::barrier::Barrier::new(Arc::new(recovered)));
        assert_eq!(storage.get(b"a").unwrap(), Some(b"v1".to_vec()));
        let rev = storage.put(b"c", b"v3", None).unwrap();
        assert!(rev > 0);
        assert_eq!(storage.get(b"c").unwrap(), Some(b"v3".to_vec()));
    }

    /// 内部记录（lease/auth 等非 /kv/ 前缀）不得被 Barrier 加密，
    /// 否则结构化记录（如 LeaseRecord 24B）解析会失败。
    #[test]
    fn test_barrier_does_not_encrypt_internal_records() {
        use crate::security::key_management::Keyring;
        use std::sync::Arc;

        let (_dir, storage) = create_storage();
        let (keyring, _dek) = Keyring::bootstrap_from_root_key(&[0x11u8; 32]).unwrap();
        storage.set_barrier(crate::security::barrier::Barrier::new(Arc::new(keyring)));

        // 直接写一条 /_lease/ 内部记录（绕过 raft，模拟内部记录落盘）
        let lease_key = crate::storage::mvcc::encode_lease_key(42);
        let record = [0u8; 24];
        storage
            .backend()
            .write(|tx| tx.insert(TABLE_KV, &lease_key, &record))
            .unwrap();

        // 内部记录以明文读取（长度与内容不变，无需解密）
        let raw = storage
            .backend()
            .read(|tx| tx.get(TABLE_KV, &lease_key))
            .unwrap()
            .unwrap();
        assert_eq!(raw.len(), 24, "internal records must stay plaintext");
    }

    // ──── P0-A 新语义：revision ≡ log index + 幂等守卫 + applied 持久化 ────

    #[test]
    fn test_put_at_revision_uses_log_index_as_revision() {
        let (_dir, storage) = create_storage();
        // raft log index 7 → revision 7（不再从内存计数器分配）
        let outcome = storage
            .put_at_revision(
                b"k",
                b"v",
                None,
                7,
                AppliedLogId {
                    term: 1,
                    node_id: 1,
                    index: 7,
                },
            )
            .unwrap();
        assert!(!outcome.replayed);
        assert_eq!(storage.current_revision(), 7);
        assert_eq!(
            storage.get_applied_log_id().unwrap(),
            Some(AppliedLogId {
                term: 1,
                node_id: 1,
                index: 7
            })
        );
        assert_eq!(storage.get(b"k").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn test_apply_idempotence_guard() {
        let (_dir, storage) = create_storage();
        let applied = AppliedLogId {
            term: 1,
            node_id: 1,
            index: 7,
        };
        let outcome1 = storage
            .put_at_revision(b"k", b"v1", None, 7, applied)
            .unwrap();
        assert!(!outcome1.replayed);

        // 同 revision 重复 apply（重启重放/重复 apply）→ 跳过，无副作用
        let outcome2 = storage
            .put_at_revision(b"k", b"v2", None, 7, applied)
            .unwrap();
        assert!(outcome2.replayed);
        assert_eq!(storage.get(b"k").unwrap(), Some(b"v1".to_vec()));

        // changelog 只有一条记录
        let entries = storage.read_changelog_entries(1).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].revision, 7);
    }

    #[test]
    fn test_delete_always_consumes_revision() {
        let (_dir, storage) = create_storage();
        // D-A5：不存在的 Key 也消耗 revision 并写 changelog
        let outcome = storage
            .delete_at_revision(b"missing", 5, AppliedLogId::standalone(5))
            .unwrap();
        assert!(!outcome.replayed);
        assert_eq!(storage.current_revision(), 5);
        let entries = storage.read_changelog_entries(5).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].event_type, EventType::Delete);
    }

    #[test]
    fn test_verify_consistency() {
        let (_dir, storage) = create_storage();
        // 空库
        let (applied, tail) = storage.verify_consistency().unwrap();
        assert_eq!(applied, 0);
        assert_eq!(tail, None);

        storage
            .put_at_revision(b"a", b"1", None, 1, AppliedLogId::standalone(1))
            .unwrap();
        storage
            .put_at_revision(b"b", b"2", None, 3, AppliedLogId::standalone(3))
            .unwrap();
        let (applied, tail) = storage.verify_consistency().unwrap();
        assert_eq!(applied, 3);
        assert_eq!(tail, Some(3));
    }

    #[test]
    fn test_standalone_concurrent_puts_allocate_distinct_revisions() {
        let (_dir, storage) = create_storage();
        let storage = std::sync::Arc::new(storage);
        let mut handles = Vec::new();
        for i in 0..8usize {
            let storage = std::sync::Arc::clone(&storage);
            handles.push(std::thread::spawn(move || {
                let key = format!("/conc/{i}");
                storage.put(key.as_bytes(), b"v", None).unwrap()
            }));
        }
        let mut revs: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        revs.sort_unstable();
        revs.dedup();
        assert_eq!(
            revs.len(),
            8,
            "concurrent standalone puts must get distinct revisions"
        );
        assert_eq!(storage.current_revision(), 8);
        for i in 0..8usize {
            let key = format!("/conc/{i}");
            assert_eq!(storage.get(key.as_bytes()).unwrap(), Some(b"v".to_vec()));
        }
    }

    #[test]
    fn test_changelog_v2_roundtrip_and_legacy_compat() {
        let event = ChangeEvent {
            revision: 5,
            changes: vec![KeyValueChange {
                key: b"k".to_vec(),
                value: Some(b"v".to_vec()),
                prev_value: None,
            }],
            event_type: EventType::Put,
        };
        let bytes = event.to_bytes();
        assert_eq!(bytes[0], 2); // v2 版本号
        let decoded = ChangeEvent::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.revision, 5);
        assert_eq!(decoded.event_type, EventType::Put);

        // v1 旧格式（无版本号字节）仍可读
        let mut legacy = Vec::new();
        legacy.extend_from_slice(&5u64.to_be_bytes());
        legacy.push(0u8); // Put
        legacy.extend_from_slice(&1u32.to_be_bytes());
        legacy.extend_from_slice(&1u32.to_be_bytes());
        legacy.extend_from_slice(b"k");
        legacy.push(1u8);
        legacy.extend_from_slice(&1u32.to_be_bytes());
        legacy.push(b'v');
        let decoded = ChangeEvent::from_bytes(&legacy).unwrap();
        assert_eq!(decoded.revision, 5);
        assert_eq!(decoded.event_type, EventType::Put);
        assert_eq!(decoded.changes.len(), 1);
    }

    // ──── P0-B Lease 状态表 ────

    #[test]
    fn test_lease_op_grant_keepalive_revoke() {
        let (_dir, storage) = create_storage();
        use crate::raft::type_config::LeaseOp;

        // Grant @rev 1
        let (outcome, changes) = storage
            .apply_lease_op(
                &LeaseOp::Grant {
                    id: 10,
                    ttl: 60,
                    deadline_wall_ms: 1000,
                },
                1,
                AppliedLogId::standalone(1),
            )
            .unwrap();
        assert!(!outcome.replayed);
        assert!(changes.is_empty());
        let record = storage.get_lease_record(10).unwrap().unwrap();
        assert_eq!(record.ttl, 60);
        assert_eq!(record.deadline_wall_ms, 1000);
        assert_eq!(record.keepalive_revision, 1);

        // KeepAlive @rev 2
        let (outcome, _) = storage
            .apply_lease_op(
                &LeaseOp::KeepAlive {
                    id: 10,
                    deadline_wall_ms: 2000,
                },
                2,
                AppliedLogId::standalone(2),
            )
            .unwrap();
        assert!(!outcome.replayed);
        let record = storage.get_lease_record(10).unwrap().unwrap();
        assert_eq!(record.deadline_wall_ms, 2000);
        assert_eq!(record.keepalive_revision, 2);

        // 绑定 Key 后 Revoke @rev 3 → 记录删除 + 绑定 Key 标记删除
        storage
            .put_at_revision(b"k", b"v", Some(10), 3, AppliedLogId::standalone(3))
            .unwrap();
        let (outcome, changes) = storage
            .apply_lease_op(
                &LeaseOp::Revoke {
                    id: 10,
                    delete_keys: true,
                },
                4,
                AppliedLogId::standalone(4),
            )
            .unwrap();
        assert!(!outcome.replayed);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].key, b"k");
        assert!(storage.get_lease_record(10).unwrap().is_none());
        assert_eq!(storage.get(b"k").unwrap(), None);
    }

    #[test]
    fn test_list_lease_records() {
        let (_dir, storage) = create_storage();
        use crate::raft::type_config::LeaseOp;

        for (i, id) in [7i64, 8, 9].into_iter().enumerate() {
            storage
                .apply_lease_op(
                    &LeaseOp::Grant {
                        id,
                        ttl: 60,
                        deadline_wall_ms: 1000 + i as i64,
                    },
                    (i + 1) as u64,
                    AppliedLogId::standalone((i + 1) as u64),
                )
                .unwrap();
        }
        let mut records = storage.list_lease_records().unwrap();
        records.sort_by_key(|(id, _)| *id);
        assert_eq!(
            records.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![7, 8, 9]
        );
        assert_eq!(records[0].1.deadline_wall_ms, 1000);
        assert_eq!(records[2].1.deadline_wall_ms, 1002);
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

    use crate::txn::{CompareOp, CompareTarget, CompareValue, TxnCompare, TxnOp, TxnOpResponse};

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
        TxnOp::Delete { key: key.to_vec() }
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
        assert_eq!(storage.get(b"lock").unwrap(), Some(b"locked".to_vec()));
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
        let compares = vec![cmp_value_eq(b"a", b"1"), cmp_value_eq(b"b", b"2")];
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
        assert_eq!(storage.get(b"key1").unwrap(), Some(b"updated".to_vec()));
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

    /// Txn CAS Version==0 对软删除的 Key 应返回成功（视为不存在）
    /// 这是分布式锁 release/expire 后重新 acquire 的核心语义。
    #[test]
    fn test_txn_version_zero_on_deleted_key_succeeds() {
        let (_dir, storage) = create_storage();

        // 1. 创建 Key：version=1
        storage.put(b"lock", b"holder-a", None).unwrap();
        assert_eq!(storage.get(b"lock").unwrap(), Some(b"holder-a".to_vec()));

        // 2. 删除 Key（软删除：version=2, deleted=true）
        storage.delete(b"lock").unwrap();
        // get 返回 None（因为 deleted=true 过滤）
        assert_eq!(storage.get(b"lock").unwrap(), None);
        // 但元数据仍然存在且 version>0
        let meta = storage.get_kv_metadata(b"lock").unwrap().unwrap();
        assert!(meta.deleted);
        assert_eq!(meta.version, 2);

        // 3. Txn CAS Version==0 → 应成功（软删除视为不存在）
        let compares = vec![TxnCompare {
            key: b"lock".to_vec(),
            target: CompareTarget::Version,
            op: CompareOp::Equal,
            target_value: CompareValue::Version(0),
        }];
        let success_ops = vec![txn_put(b"lock", b"holder-b")];
        let failure_ops = vec![txn_put(b"lock", b"conflict")];

        let result = storage
            .execute_txn(&compares, &success_ops, &failure_ops)
            .unwrap();

        // 应成功执行 success 分支
        assert!(
            result.succeeded,
            "Txn CAS Version==0 should succeed on soft-deleted key"
        );
        assert_eq!(
            storage.get(b"lock").unwrap(),
            Some(b"holder-b".to_vec()),
            "holder-b should have re-acquired the lock"
        );
    }

    /// 软删除 Key 的 ModRevision/CreateRevision 比较也应视为 0
    #[test]
    fn test_txn_revision_on_deleted_key_is_zero() {
        let (_dir, storage) = create_storage();

        storage.put(b"key", b"val", None).unwrap();
        let rev1 = storage.delete(b"key").unwrap();

        // 软删除后 ModRevision==0（视为不存在）
        let compares = vec![TxnCompare {
            key: b"key".to_vec(),
            target: CompareTarget::ModRevision,
            op: CompareOp::Equal,
            target_value: CompareValue::ModRevision(0),
        }];
        let success_ops = vec![txn_put(b"key", b"recreated")];
        let failure_ops = vec![];

        let result = storage
            .execute_txn(&compares, &success_ops, &failure_ops)
            .unwrap();

        assert!(
            result.succeeded,
            "ModRevision==0 should match soft-deleted key"
        );
        assert_eq!(storage.get(b"key").unwrap(), Some(b"recreated".to_vec()));
    }

    /// 软删除 Key 的 Value 比较应视为空
    #[test]
    fn test_txn_value_on_deleted_key_is_empty() {
        let (_dir, storage) = create_storage();

        storage.put(b"key", b"original", None).unwrap();
        storage.delete(b"key").unwrap();

        // 软删除后 Value==空
        let compares = vec![TxnCompare {
            key: b"key".to_vec(),
            target: CompareTarget::Value,
            op: CompareOp::Equal,
            target_value: CompareValue::Value(vec![]),
        }];
        let success_ops = vec![txn_put(b"key", b"new-val")];
        let failure_ops = vec![];

        let result = storage
            .execute_txn(&compares, &success_ops, &failure_ops)
            .unwrap();

        assert!(
            result.succeeded,
            "Value==empty should match soft-deleted key"
        );
        assert_eq!(storage.get(b"key").unwrap(), Some(b"new-val".to_vec()));
    }

    // ──── P1-01 Compaction ────

    #[test]
    fn test_apply_compact_deletes_changelog_below_revision() {
        let (_dir, storage) = create_storage();
        for i in 0..10u32 {
            storage
                .put(format!("/ck{}", i).as_bytes(), b"v", None)
                .unwrap();
        }
        assert_eq!(storage.current_revision(), 10);

        let outcome = storage
            .apply_compact(5, AppliedLogId::standalone(5))
            .unwrap();
        assert!(
            outcome.deleted_changelog > 0,
            "entries below cutoff deleted"
        );
        assert_eq!(storage.compacted_revision().unwrap(), 5);

        for rev in 1..5u64 {
            assert!(
                !storage.changelog_contains_revision(rev).unwrap(),
                "changelog entry at rev {rev} should be compacted away"
            );
        }
        for rev in 5..=10u64 {
            assert!(
                storage.changelog_contains_revision(rev).unwrap(),
                "changelog entry at rev {rev} must survive compaction"
            );
        }
        // KV 数据不受影响
        assert_eq!(storage.get(b"/ck0").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn test_apply_compact_removes_old_tombstones() {
        let (_dir, storage) = create_storage();
        for i in 0..20u32 {
            storage
                .put(format!("/tk{}", i).as_bytes(), b"v", None)
                .unwrap();
        }
        for i in 0..10u32 {
            storage.delete(format!("/tk{}", i).as_bytes()).unwrap();
        }
        let current = storage.current_revision();
        assert!(current >= 30);

        let outcome = storage
            .apply_compact(current, AppliedLogId::standalone(current))
            .unwrap();
        // 注意：mod_revision == current 的 tombstone 恰好在截止线上，必须保留
        // （删除条件为 mod_revision < revision），其余 9 个被物理删除。
        assert!(
            outcome.deleted_tombstones >= 9,
            "old tombstones should be physically removed, got {}",
            outcome.deleted_tombstones
        );
        // 物理删除后元数据不存在（不再有 deleted 标记）
        assert!(storage.get_kv_metadata(b"/tk0").unwrap().is_none());
        // 截止线上的 tombstone 保留（deleted 标记仍在）
        let meta = storage.get_kv_metadata(b"/tk9").unwrap();
        assert!(meta.is_some_and(|m| m.deleted), "cutoff tombstone retained");
        // 未删除的 key 保留
        assert_eq!(storage.get(b"/tk15").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn test_apply_compact_idempotent_and_monotonic() {
        let (_dir, storage) = create_storage();
        for i in 0..5u32 {
            storage
                .put(format!("/id{}", i).as_bytes(), b"v", None)
                .unwrap();
        }
        storage
            .apply_compact(3, AppliedLogId::standalone(3))
            .unwrap();

        let again = storage
            .apply_compact(3, AppliedLogId::standalone(3))
            .unwrap();
        assert_eq!(again.deleted_changelog, 0);
        assert_eq!(again.deleted_tombstones, 0);

        let lower = storage
            .apply_compact(2, AppliedLogId::standalone(2))
            .unwrap();
        assert_eq!(lower.deleted_changelog, 0);

        // 未来 revision（> 命令自身 entry index）在 apply 内钳制为该 index
        // （apply 不得失败——openraft 视为致命；真正的"未来 revision"拒绝由
        // RPC/提案层返回 INVALID_ARGUMENT）：钳制后 compacted_revision 推进
        let future = storage
            .apply_compact(999, AppliedLogId::standalone(10))
            .unwrap();
        assert_eq!(future.deleted_changelog, 3); // rev 3,4,5（< 钳制后的 10）
        assert_eq!(storage.compacted_revision().unwrap(), 10);
    }

    #[test]
    fn test_apply_compact_sharded_batches() {
        let (_dir, storage) = create_storage();
        // 超过单批上限（COMPACT_BATCH_SIZE）的条目数，验证分片删除
        let total = super::COMPACT_BATCH_SIZE as u32 + 500;
        for i in 0..total {
            storage
                .put(format!("/shard{}", i).as_bytes(), b"v", None)
                .unwrap();
        }
        let current = storage.current_revision();
        assert_eq!(current, total as u64);

        let outcome = storage
            .apply_compact(current, AppliedLogId::standalone(current))
            .unwrap();
        assert_eq!(
            outcome.deleted_changelog as u64,
            current - 1,
            "all entries below cutoff must be deleted across batches"
        );
        assert!(storage.changelog_contains_revision(current).unwrap());
        assert_eq!(storage.compacted_revision().unwrap(), current);
    }

    #[test]
    fn test_compacted_revision_persists_across_reopen() {
        let (dir, storage) = create_storage();
        for i in 0..5u32 {
            storage
                .put(format!("/p{}", i).as_bytes(), b"v", None)
                .unwrap();
        }
        storage
            .apply_compact(3, AppliedLogId::standalone(3))
            .unwrap();
        drop(storage);

        let config = StorageConfig::default();
        let backend = RedbBackend::open(dir.path(), &config).unwrap();
        let reopened = MvccStorage::new(backend).unwrap();
        assert_eq!(reopened.compacted_revision().unwrap(), 3);
    }
}
