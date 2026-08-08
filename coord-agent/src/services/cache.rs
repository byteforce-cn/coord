// coord-agent: 缓存 (Cache Service) — 数据面（Phase F）
//
// 实现 BaseService trait，基于 redb 提供本地持久化缓存引擎。
// 支持 String/Hash/List/Set 四种数据类型、TTL 过期。
//
// 架构（v3.0，源码核验版 + v2.1 ISR 落地）:
// - 本地 redb 存储引擎；启用 ISR 复制后，写路径经复制管理器同步到 ISR Followers
// - List pop 为单写事务原子出队（G2 已修复）；复制后 pop 仅 Leader 执行（C2）
// - TTL 复制 Leader 计算的绝对到期时间戳（C5，encode_value 内嵌 expires_at）
//
// ✅ 状态声明（v2.1，2026-08-08）：ISR 复制**已实现并落地**（docs/cache-mq-isr-evaluation.md）。
// 复制日志 / 持久化幂等键 / 本地序列号在 cache.redb 内与数据写同事务提交。
//
// 参见 docs/client-agent-architecture-v3.md §5.5。

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use parking_lot::RwLock;
use redb::{ReadableDatabase, ReadableTable};

use crate::service::{BaseService, ServiceResult};

// ──── 可重导出类型 ────

/// 缓存数据类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CacheDataType {
    String,
    Hash,
    List,
    Set,
}

/// 缓存条目（用于迭代/导出）
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub data_type: CacheDataType,
    pub expires_at: Option<u64>,
}

/// 分片元数据
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CacheShardMeta {
    pub shard_id: String,
    pub leader_agent: String,
    pub replicas: Vec<String>,
    pub key_range_start: Vec<u8>,
    pub key_range_end: Vec<u8>,
}

/// 缓存统计信息
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct CacheStats {
    pub string_count: u64,
    pub hash_count: u64,
    pub list_count: u64,
    pub set_count: u64,
    pub shard_count: u64,
    pub total_size_bytes: u64,
}

// ──── redb 表定义 ────

const STRING_TABLE: redb::TableDefinition<&[u8], &[u8]> =
    redb::TableDefinition::new("cache:string");
const HASH_TABLE: redb::TableDefinition<&[u8], &[u8]> =
    redb::TableDefinition::new("cache:hash");
const LIST_TABLE: redb::TableDefinition<&[u8], &[u8]> =
    redb::TableDefinition::new("cache:list");
const SET_TABLE: redb::TableDefinition<&[u8], u64> =
    redb::TableDefinition::new("cache:set");
const SHARD_TABLE: redb::TableDefinition<&str, &[u8]> =
    redb::TableDefinition::new("cache:shards");

// ──── 复制日志表（ISR，v2.1）────
// 复制条目日志: key = [shard_len:u32][shard_bytes][seq:u64 BE]
const CACHE_REPL_ENTRY_TABLE: redb::TableDefinition<&[u8], &[u8]> =
    redb::TableDefinition::new("cache:repl_entries");
// 持久化幂等键（Q2）: key = idempotency_key bytes
const CACHE_REPL_APPLIED_KEYS: redb::TableDefinition<&[u8], ()> =
    redb::TableDefinition::new("cache:repl_applied");
// 各 shard 最后已应用序列号: key = shard bytes
const CACHE_REPL_LOCAL_SEQ: redb::TableDefinition<&[u8], u64> =
    redb::TableDefinition::new("cache:repl_local_seq");

// ──── Key 编码辅助 ────

/// Hash key 编码: key_bytes + b'\x00' + field_bytes
fn encode_hash_key(key: &str, field: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(key.len() + 1 + field.len());
    v.extend_from_slice(key.as_bytes());
    v.push(0);
    v.extend_from_slice(field.as_bytes());
    v
}

/// Hash key 前缀
fn hash_key_prefix(key: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(key.len() + 1);
    v.extend_from_slice(key.as_bytes());
    v.push(0);
    v
}

/// List key 编码: key_bytes + b'\x00' + index (8 bytes BE, offset by i64::MAX)
fn encode_list_key(key: &str, index: i64) -> Vec<u8> {
    let mut v = Vec::with_capacity(key.len() + 9);
    v.extend_from_slice(key.as_bytes());
    v.push(0);
    let adjusted = (index as i128).wrapping_add(i64::MAX as i128) as u64;
    v.extend_from_slice(&adjusted.to_be_bytes());
    v
}

/// List key 前缀
fn list_key_prefix(key: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(key.len() + 1);
    v.extend_from_slice(key.as_bytes());
    v.push(0);
    v
}

/// 解析 List key 中的 index
fn decode_list_index(encoded: &[u8], prefix_len: usize) -> Option<i64> {
    if encoded.len() < prefix_len + 8 {
        return None;
    }
    let adjusted = u64::from_be_bytes(encoded[prefix_len..prefix_len + 8].try_into().ok()?);
    Some((adjusted as i128).wrapping_sub(i64::MAX as i128) as i64)
}

/// Set key 编码: key_bytes + b'\x00' + member_bytes
fn encode_set_key(key: &str, member: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(key.len() + 1 + member.len());
    v.extend_from_slice(key.as_bytes());
    v.push(0);
    v.extend_from_slice(member);
    v
}

/// Set key 前缀
fn set_key_prefix(key: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(key.len() + 1);
    v.extend_from_slice(key.as_bytes());
    v.push(0);
    v
}

/// 从 Set 编码 key 中提取 member
fn decode_set_member(encoded: &[u8], prefix_len: usize) -> Vec<u8> {
    encoded[prefix_len..].to_vec()
}

// ──── TTL 编解码 ────

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn encode_ttl(ttl_secs: u64) -> u64 {
    if ttl_secs == 0 {
        0
    } else {
        now_secs().saturating_add(ttl_secs)
    }
}

fn is_expired(expires_at: u64) -> bool {
    expires_at > 0 && now_secs() >= expires_at
}

/// 将 value + TTL 编码: [8 bytes expires_at BE][value]
fn encode_value(value: &[u8], ttl_secs: u64) -> Vec<u8> {
    let expires_at = encode_ttl(ttl_secs);
    let mut v = Vec::with_capacity(8 + value.len());
    v.extend_from_slice(&expires_at.to_be_bytes());
    v.extend_from_slice(value);
    v
}

/// 解码存储格式，检查 TTL；返回 None 若已过期
fn decode_value(raw: &[u8]) -> Option<Vec<u8>> {
    if raw.len() < 8 {
        return None;
    }
    let expires_at = u64::from_be_bytes(raw[..8].try_into().ok()?);
    if is_expired(expires_at) {
        return None;
    }
    Some(raw[8..].to_vec())
}

// ──── CacheService ────

/// 分布式缓存服务（数据面）
///
/// 基于 redb 的本地持久化缓存引擎。
/// 线程安全：使用 parking_lot::RwLock 保护 redb Database。
pub struct CacheService {
    db_path: PathBuf,
    db: RwLock<Option<redb::Database>>,
    started: RwLock<bool>,
    default_ttl_secs: u64,
    /// ISR 复制管理器（None = 单 agent 本地语义，零复制路径保留，C6）
    replication: RwLock<Option<Arc<crate::services::replication::ReplicationManager>>>,
}

impl std::fmt::Debug for CacheService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheService")
            .field("db_path", &self.db_path)
            .field("started", &self.started)
            .field("default_ttl_secs", &self.default_ttl_secs)
            .finish()
    }
}

impl CacheService {
    pub fn new(db_path: PathBuf, _max_size_bytes: u64, default_ttl_secs: u64) -> Self {
        Self {
            db_path,
            db: RwLock::new(None),
            started: RwLock::new(false),
            default_ttl_secs,
            replication: RwLock::new(None),
        }
    }

    /// 挂载 ISR 复制管理器（None = 关闭复制，单 agent 零破坏）
    pub fn set_replication(
        &self,
        manager: Option<Arc<crate::services::replication::ReplicationManager>>,
    ) {
        *self.replication.write() = manager;
    }

    /// 复制是否启用
    pub fn replication_enabled(&self) -> bool {
        self.replication.read().is_some()
    }

    /// 复制管理器引用（None = 复制关闭）
    pub fn replication_manager(&self) -> Option<Arc<crate::services::replication::ReplicationManager>> {
        self.replication.read().clone()
    }

    fn read_tx(&self) -> ServiceResult<redb::ReadTransaction> {
        let guard = self.db.read();
        let db = guard.as_ref().ok_or("CacheService not started")?;
        Ok(db.begin_read()?)
    }

    fn write_tx(&self) -> ServiceResult<redb::WriteTransaction> {
        let guard = self.db.read();
        let db = guard.as_ref().ok_or("CacheService not started")?;
        Ok(db.begin_write()?)
    }

    // ──── String 操作 ────

    pub fn string_put(&self, key: &str, value: Vec<u8>, ttl_secs: Option<u64>) -> ServiceResult<()> {
        let ttl = ttl_secs.unwrap_or(self.default_ttl_secs);
        let encoded = encode_value(&value, ttl);
        let wtx = self.write_tx()?;
        {
            let mut table = wtx.open_table(STRING_TABLE)?;
            table.insert(key.as_bytes(), encoded.as_slice())?;
        }
        wtx.commit()?;
        Ok(())
    }

    pub fn string_get(&self, key: &str) -> ServiceResult<Option<Vec<u8>>> {
        let rtx = self.read_tx()?;
        let raw: Option<Vec<u8>> = {
            let table = rtx.open_table(STRING_TABLE)?;
            match table.get(key.as_bytes())? {
                Some(v) => Some(v.value().to_vec()),
                None => None,
            }
        };
        drop(rtx);

        match raw {
            Some(raw) => match decode_value(&raw) {
                Some(val) => Ok(Some(val)),
                None => {
                    let _ = self.string_delete(key);
                    Ok(None)
                }
            },
            None => Ok(None),
        }
    }

    pub fn string_delete(&self, key: &str) -> ServiceResult<bool> {
        let wtx = self.write_tx()?;
        let existed = {
            let mut table = wtx.open_table(STRING_TABLE)?;
            let x = table.remove(key.as_bytes())?.is_some();
            x
        };
        wtx.commit()?;
        Ok(existed)
    }

    pub fn string_exists(&self, key: &str) -> ServiceResult<bool> {
        self.string_get(key).map(|v| v.is_some())
    }

    // ──── Hash 操作 ────

    pub fn hash_field_put(&self, key: &str, field: &str, value: Vec<u8>, ttl_secs: Option<u64>) -> ServiceResult<()> {
        let ttl = ttl_secs.unwrap_or(self.default_ttl_secs);
        let encoded = encode_value(&value, ttl);
        let hk = encode_hash_key(key, field);
        let wtx = self.write_tx()?;
        {
            let mut table = wtx.open_table(HASH_TABLE)?;
            table.insert(hk.as_slice(), encoded.as_slice())?;
        }
        wtx.commit()?;
        Ok(())
    }

    pub fn hash_field_get(&self, key: &str, field: &str) -> ServiceResult<Option<Vec<u8>>> {
        let hk = encode_hash_key(key, field);
        let rtx = self.read_tx()?;
        let raw: Option<Vec<u8>> = {
            let table = rtx.open_table(HASH_TABLE)?;
            match table.get(hk.as_slice())? {
                Some(v) => Some(v.value().to_vec()),
                None => None,
            }
        };
        drop(rtx);

        match raw {
            Some(raw) => match decode_value(&raw) {
                Some(val) => Ok(Some(val)),
                None => {
                    let _ = self.hash_field_delete(key, field);
                    Ok(None)
                }
            },
            None => Ok(None),
        }
    }

    pub fn hash_get_all(&self, key: &str) -> ServiceResult<BTreeMap<String, Vec<u8>>> {
        let prefix = hash_key_prefix(key);
        let plen = prefix.len();
        let rtx = self.read_tx()?;
        let (result, expired): (BTreeMap<String, Vec<u8>>, Vec<String>) = {
            let table = rtx.open_table(HASH_TABLE)?;
            let mut m = BTreeMap::new();
            let mut ex = Vec::new();
            let range: std::ops::RangeFrom<&[u8]> = prefix.as_slice()..;
            for item in table.range(range)? {
                let (k, raw) = item?;
                let k = k.value();
                if !k.starts_with(&prefix) || k.len() <= plen {
                    break;
                }
                let field = String::from_utf8_lossy(&k[plen..]).to_string();
                match decode_value(raw.value()) {
                    Some(val) => { m.insert(field, val); }
                    None => { ex.push(field); }
                }
            }
            (m, ex)
        };
        drop(rtx);

        for f in &expired {
            let _ = self.hash_field_delete(key, f);
        }

        Ok(result)
    }

    pub fn hash_field_delete(&self, key: &str, field: &str) -> ServiceResult<bool> {
        let hk = encode_hash_key(key, field);
        let wtx = self.write_tx()?;
        let existed = {
            let mut table = wtx.open_table(HASH_TABLE)?;
            let x = table.remove(hk.as_slice())?.is_some();
            x
        };
        wtx.commit()?;
        Ok(existed)
    }

    pub fn hash_field_count(&self, key: &str) -> ServiceResult<u64> {
        let prefix = hash_key_prefix(key);
        let rtx = self.read_tx()?;
        let table = rtx.open_table(HASH_TABLE)?;
        let mut count = 0u64;
        let range: std::ops::RangeFrom<&[u8]> = prefix.as_slice()..;
        for item in table.range(range)? {
            let (k, raw) = item?;
            let k = k.value();
            if !k.starts_with(&prefix) {
                break;
            }
            if decode_value(raw.value()).is_some() {
                count += 1;
            }
        }
        Ok(count)
    }

    // ──── List 操作 ────

    pub fn list_push_right(&self, key: &str, value: Vec<u8>, ttl_secs: Option<u64>) -> ServiceResult<()> {
        let ttl = ttl_secs.unwrap_or(self.default_ttl_secs);
        let encoded = encode_value(&value, ttl);
        let prefix = list_key_prefix(key);
        let plen = prefix.len();

        let rtx = self.read_tx()?;
        let max_idx = {
            let table = rtx.open_table(LIST_TABLE)?;
            let mut last = -1i64;
            let range: std::ops::RangeFrom<&[u8]> = prefix.as_slice()..;
            for item in table.range(range)? {
                let (k, _) = item?;
                let k = k.value();
                if !k.starts_with(&prefix) { break; }
                if let Some(idx) = decode_list_index(k, plen) {
                    last = last.max(idx);
                }
            }
            last
        };
        drop(rtx);

        let lk = encode_list_key(key, max_idx + 1);
        let wtx = self.write_tx()?;
        {
            let mut table = wtx.open_table(LIST_TABLE)?;
            table.insert(lk.as_slice(), encoded.as_slice())?;
        }
        wtx.commit()?;
        Ok(())
    }

    pub fn list_push_left(&self, key: &str, value: Vec<u8>, ttl_secs: Option<u64>) -> ServiceResult<()> {
        let ttl = ttl_secs.unwrap_or(self.default_ttl_secs);
        let encoded = encode_value(&value, ttl);
        let prefix = list_key_prefix(key);
        let plen = prefix.len();

        let rtx = self.read_tx()?;
        let min_idx = {
            let table = rtx.open_table(LIST_TABLE)?;
            let mut first: Option<i64> = None;
            let range: std::ops::RangeFrom<&[u8]> = prefix.as_slice()..;
            for item in table.range(range)? {
                let (k, _) = item?;
                let k = k.value();
                if !k.starts_with(&prefix) { break; }
                if let Some(idx) = decode_list_index(k, plen) {
                    if first.map_or(true, |f| idx < f) {
                        first = Some(idx);
                    }
                }
            }
            first.unwrap_or(0)
        };
        drop(rtx);

        let lk = encode_list_key(key, min_idx - 1);
        let wtx = self.write_tx()?;
        {
            let mut table = wtx.open_table(LIST_TABLE)?;
            table.insert(lk.as_slice(), encoded.as_slice())?;
        }
        wtx.commit()?;
        Ok(())
    }

    /// 原子出队（G2，单写事务）：
    /// 同一写事务内迭代前缀 range 找极值（跳过已过期）→ 事务内删除 → commit。
    /// redb 单写者模型保证写事务串行化，消除并发重复出队 / 丢元素。
    /// 已知代价（R3）：大 list 下 O(n) 定位极值，首版接受。
    fn list_pop_atomic(&self, key: &str, find_max: bool) -> ServiceResult<Option<Vec<u8>>> {
        let prefix = list_key_prefix(key);
        let plen = prefix.len();

        let wtx = self.write_tx()?;
        let popped: Option<Vec<u8>> = {
            let mut table = wtx.open_table(LIST_TABLE)?;
            // 写事务内找极值（跳过已过期）
            let mut best: Option<(i64, Vec<u8>)> = None;
            let range: std::ops::RangeFrom<&[u8]> = prefix.as_slice()..;
            for item in table.range(range)? {
                let (k, raw) = item?;
                let k = k.value();
                if !k.starts_with(&prefix) { break; }
                if let Some(idx) = decode_list_index(k, plen) {
                    let is_better = match &best {
                        Some((b, _)) => if find_max { idx > *b } else { idx < *b },
                        None => true,
                    };
                    if is_better {
                        if let Some(val) = decode_value(raw.value()) {
                            best = Some((idx, val));
                        }
                    }
                }
            }
            // 事务内删除
            match best {
                Some((idx, val)) => {
                    let lk = encode_list_key(key, idx);
                    table.remove(lk.as_slice())?;
                    Some(val)
                }
                None => None,
            }
        };
        wtx.commit()?;
        Ok(popped)
    }

    pub fn list_pop_right(&self, key: &str) -> ServiceResult<Option<Vec<u8>>> {
        self.list_pop_atomic(key, true)
    }

    pub fn list_pop_left(&self, key: &str) -> ServiceResult<Option<Vec<u8>>> {
        self.list_pop_atomic(key, false)
    }

    pub fn list_range(&self, key: &str, start: i64, end: i64) -> ServiceResult<Vec<Vec<u8>>> {
        let prefix = list_key_prefix(key);
        let plen = prefix.len();
        let rtx = self.read_tx()?;
        let mut items: Vec<(i64, Vec<u8>)> = {
            let table = rtx.open_table(LIST_TABLE)?;
            let mut v = Vec::new();
            let range: std::ops::RangeFrom<&[u8]> = prefix.as_slice()..;
            for item in table.range(range)? {
                let (k, raw) = item?;
                let k = k.value();
                if !k.starts_with(&prefix) { break; }
                if let Some(idx) = decode_list_index(k, plen) {
                    if let Some(val) = decode_value(raw.value()) {
                        v.push((idx, val));
                    }
                }
            }
            v
        };
        drop(rtx);

        items.sort_by_key(|(idx, _)| *idx);
        let len = items.len() as i64;
        let end = if end < 0 { len + end + 1 } else { end.min(len) };
        let start = start.max(0);
        Ok(items.into_iter().skip(start as usize).take((end - start).max(0) as usize).map(|(_, v)| v).collect())
    }

    pub fn list_length(&self, key: &str) -> ServiceResult<u64> {
        let prefix = list_key_prefix(key);
        let rtx = self.read_tx()?;
        let table = rtx.open_table(LIST_TABLE)?;
        let mut count = 0u64;
        let range: std::ops::RangeFrom<&[u8]> = prefix.as_slice()..;
        for item in table.range(range)? {
            let (k, raw) = item?;
            let k = k.value();
            if !k.starts_with(&prefix) { break; }
            if decode_value(raw.value()).is_some() { count += 1; }
        }
        Ok(count)
    }

    // ──── Set 操作 ────

    pub fn set_add(&self, key: &str, member: Vec<u8>, ttl_secs: Option<u64>) -> ServiceResult<bool> {
        let ttl = ttl_secs.unwrap_or(self.default_ttl_secs);
        let expires_at = encode_ttl(ttl);
        let sk = encode_set_key(key, &member);
        let wtx = self.write_tx()?;
        let existed = {
            let table = wtx.open_table(SET_TABLE)?;
            let x = table.get(sk.as_slice())?.is_some();
            x
        };
        if !existed {
            let mut table = wtx.open_table(SET_TABLE)?;
            table.insert(sk.as_slice(), expires_at)?;
        }
        wtx.commit()?;
        Ok(!existed)
    }

    pub fn set_remove(&self, key: &str, member: &[u8]) -> ServiceResult<bool> {
        let sk = encode_set_key(key, member);
        let wtx = self.write_tx()?;
        let existed = {
            let mut table = wtx.open_table(SET_TABLE)?;
            let x = table.remove(sk.as_slice())?.is_some();
            x
        };
        wtx.commit()?;
        Ok(existed)
    }

    pub fn set_contains(&self, key: &str, member: &[u8]) -> ServiceResult<bool> {
        let sk = encode_set_key(key, member);
        let rtx = self.read_tx()?;
        let expires_at: Option<u64> = {
            let table = rtx.open_table(SET_TABLE)?;
            match table.get(sk.as_slice())? {
                Some(v) => Some(v.value()),
                None => None,
            }
        };
        drop(rtx);
        match expires_at {
            Some(exp) if !is_expired(exp) => Ok(true),
            Some(_) => { let _ = self.set_remove(key, member); Ok(false) }
            None => Ok(false),
        }
    }

    pub fn set_members(&self, key: &str) -> ServiceResult<Vec<Vec<u8>>> {
        let prefix = set_key_prefix(key);
        let plen = prefix.len();
        let rtx = self.read_tx()?;
        let (members, expired): (Vec<Vec<u8>>, Vec<Vec<u8>>) = {
            let table = rtx.open_table(SET_TABLE)?;
            let mut m = Vec::new();
            let mut ex = Vec::new();
            let range: std::ops::RangeFrom<&[u8]> = prefix.as_slice()..;
            for item in table.range(range)? {
                let (k, exp) = item?;
                let k = k.value();
                if !k.starts_with(&prefix) { break; }
                let member = decode_set_member(k, plen);
                if is_expired(exp.value()) { ex.push(member); } else { m.push(member); }
            }
            (m, ex)
        };
        drop(rtx);
        for m in &expired { let _ = self.set_remove(key, m); }
        Ok(members)
    }

    pub fn set_cardinality(&self, key: &str) -> ServiceResult<u64> {
        let prefix = set_key_prefix(key);
        let rtx = self.read_tx()?;
        let table = rtx.open_table(SET_TABLE)?;
        let mut count = 0u64;
        let range: std::ops::RangeFrom<&[u8]> = prefix.as_slice()..;
        for item in table.range(range)? {
            let (k, exp) = item?;
            let k = k.value();
            if !k.starts_with(&prefix) { break; }
            if !is_expired(exp.value()) { count += 1; }
        }
        Ok(count)
    }

    // ──── 分片元数据 ────

    pub fn set_shard_meta(&self, shard_id: &str, meta: CacheShardMeta) -> ServiceResult<()> {
        let json = serde_json::to_vec(&meta)?;
        let wtx = self.write_tx()?;
        {
            let mut table = wtx.open_table(SHARD_TABLE)?;
            table.insert(shard_id, json.as_slice())?;
        }
        wtx.commit()?;
        Ok(())
    }

    pub fn get_shard_meta(&self, shard_id: &str) -> ServiceResult<Option<CacheShardMeta>> {
        let rtx = self.read_tx()?;
        let raw: Option<Vec<u8>> = {
            let table = rtx.open_table(SHARD_TABLE)?;
            match table.get(shard_id)? {
                Some(v) => Some(v.value().to_vec()),
                None => None,
            }
        };
        drop(rtx);
        match raw {
            Some(raw) => Ok(Some(serde_json::from_slice(&raw)?)),
            None => Ok(None),
        }
    }

    pub fn list_shards(&self) -> ServiceResult<Vec<CacheShardMeta>> {
        let rtx = self.read_tx()?;
        let table = rtx.open_table(SHARD_TABLE)?;
        let mut shards = Vec::new();
        for item in table.iter()? {
            let (_, raw) = item?;
            shards.push(serde_json::from_slice(raw.value())?);
        }
        Ok(shards)
    }

    // ──── 运维操作 ────

    pub fn stats(&self) -> ServiceResult<CacheStats> {
        let rtx = self.read_tx()?;

        let string_count = {
            let table = rtx.open_table(STRING_TABLE)?;
            let mut count = 0u64;
            for item in table.iter()? {
                let (_, raw) = item?;
                if decode_value(raw.value()).is_some() { count += 1; }
            }
            count
        };

        let (hash_count, list_count, set_count, shard_count) = {
            let mut seen_hash = std::collections::HashSet::new();
            let mut seen_list = std::collections::HashSet::new();
            let mut seen_set = std::collections::HashSet::new();

            {
                let table = rtx.open_table(HASH_TABLE)?;
                for item in table.iter()? {
                    let (k, raw) = item?;
                    let k = k.value();
                    if decode_value(raw.value()).is_some() {
                        if let Some(pos) = k.iter().position(|&b| b == 0) {
                            seen_hash.insert(k[..pos].to_vec());
                        }
                    }
                }
            }
            {
                let table = rtx.open_table(LIST_TABLE)?;
                for item in table.iter()? {
                    let (k, _) = item?;
                    let k = k.value();
                    if let Some(pos) = k.iter().position(|&b| b == 0) {
                        seen_list.insert(k[..pos].to_vec());
                    }
                }
            }
            {
                let table = rtx.open_table(SET_TABLE)?;
                for item in table.iter()? {
                    let (k, exp) = item?;
                    let k = k.value();
                    if !is_expired(exp.value()) {
                        if let Some(pos) = k.iter().position(|&b| b == 0) {
                            seen_set.insert(k[..pos].to_vec());
                        }
                    }
                }
            }
            let sc = {
                let table = rtx.open_table(SHARD_TABLE)?;
                table.iter()?.count() as u64
            };
            (seen_hash.len() as u64, seen_list.len() as u64, seen_set.len() as u64, sc)
        };

        Ok(CacheStats {
            string_count,
            hash_count,
            list_count,
            set_count,
            shard_count,
            total_size_bytes: 0,
        })
    }

    pub fn flush_all(&self) -> ServiceResult<()> {
        // Use separate transactions to avoid borrow conflicts
        {
            let wtx = self.write_tx()?;
            let keys: Vec<Vec<u8>> = {
                let table = wtx.open_table(STRING_TABLE)?;
                table.iter()?.filter_map(|r| r.ok().map(|(k, _)| k.value().to_vec())).collect()
            };
            let mut table = wtx.open_table(STRING_TABLE)?;
            for k in &keys { let _ = table.remove(k.as_slice()); }
            drop(table);
            wtx.commit()?;
        }
        {
            let wtx = self.write_tx()?;
            let keys: Vec<Vec<u8>> = {
                let table = wtx.open_table(HASH_TABLE)?;
                table.iter()?.filter_map(|r| r.ok().map(|(k, _)| k.value().to_vec())).collect()
            };
            let mut table = wtx.open_table(HASH_TABLE)?;
            for k in &keys { let _ = table.remove(k.as_slice()); }
            drop(table);
            wtx.commit()?;
        }
        {
            let wtx = self.write_tx()?;
            let keys: Vec<Vec<u8>> = {
                let table = wtx.open_table(LIST_TABLE)?;
                table.iter()?.filter_map(|r| r.ok().map(|(k, _)| k.value().to_vec())).collect()
            };
            let mut table = wtx.open_table(LIST_TABLE)?;
            for k in &keys { let _ = table.remove(k.as_slice()); }
            drop(table);
            wtx.commit()?;
        }
        {
            let wtx = self.write_tx()?;
            let keys: Vec<Vec<u8>> = {
                let table = wtx.open_table(SET_TABLE)?;
                table.iter()?.filter_map(|r| r.ok().map(|(k, _)| k.value().to_vec())).collect()
            };
            let mut table = wtx.open_table(SET_TABLE)?;
            for k in &keys { let _ = table.remove(k.as_slice()); }
            drop(table);
            wtx.commit()?;
        }
        {
            let wtx = self.write_tx()?;
            let keys: Vec<String> = {
                let table = wtx.open_table(SHARD_TABLE)?;
                table.iter()?.filter_map(|r| r.ok().map(|(k, _)| k.value().to_string())).collect()
            };
            let mut table = wtx.open_table(SHARD_TABLE)?;
            for k in &keys { let _ = table.remove(k.as_str()); }
            drop(table);
            wtx.commit()?;
        }
        Ok(())
    }
}

// ──── ISR 复制（v2.1 已落地）────
//
// 复制日志 / 持久化幂等键 / 本地序列号在本服务 redb 内与数据写同事务提交。
// 复制条目携带 Leader 计算的**物理 key**（含 list index / hash field / set member）
// 与 **绝对到期时间戳**（encode_value 内嵌 expires_at，C5），Follower 原样应用。
// pop 仅 Leader 执行（C2）：Leader 单事务内原子出队 + 记录 CacheDelete 复制条目。
//
// 参见 docs/cache-mq-isr-evaluation.md §4。

use crate::services::replication::{
    IdempotencyKey, ReplicationEntry, ReplicationError, ReplicationOp, ReplicatedStore,
};

/// Cache 单 shard
const CACHE_SHARD: &str = "cache";

/// 复制日志 key: [shard_len:u32][shard][seq:u64 BE]
fn encode_repl_key(shard: &str, seq: u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(4 + shard.len() + 8);
    v.extend_from_slice(&(shard.len() as u32).to_be_bytes());
    v.extend_from_slice(shard.as_bytes());
    v.extend_from_slice(&seq.to_be_bytes());
    v
}

/// 复制日志前缀: [shard_len:u32][shard]
fn encode_repl_prefix(shard: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(4 + shard.len());
    v.extend_from_slice(&(shard.len() as u32).to_be_bytes());
    v.extend_from_slice(shard.as_bytes());
    v
}

/// 从复制日志 key 解码序列号
fn decode_repl_seq(encoded: &[u8], prefix_len: usize) -> Option<u64> {
    if encoded.len() < prefix_len + 8 {
        return None;
    }
    Some(u64::from_be_bytes(encoded[prefix_len..prefix_len + 8].try_into().ok()?))
}

impl CacheService {
    /// 生成缓存写幂等键（基于物理 key，全局唯一）
    fn cache_idem_key(op: &str, key: &[u8]) -> IdempotencyKey {
        IdempotencyKey::new(
            format!("cache:{op}:{}", String::from_utf8_lossy(key)),
            now_secs(),
        )
    }

    /// 下一序列号（= 本地最后序列号 + 1）
    fn next_sequence(&self) -> ServiceResult<u64> {
        let rtx = self.read_tx()?;
        let table = rtx.open_table(CACHE_REPL_LOCAL_SEQ)?;
        let seq = match table.get(CACHE_SHARD.as_bytes())? {
            Some(v) => v.value(),
            None => 0,
        };
        Ok(seq + 1)
    }

    /// 在指定写事务内写入复制簿记（日志条目 + 持久化幂等键 + 本地序列号）
    fn write_repl_bookkeeping_tx(
        wtx: &redb::WriteTransaction,
        entry: &ReplicationEntry,
    ) -> ServiceResult<()> {
        let rk = encode_repl_key(&entry.shard_id, entry.sequence_num);
        let encoded = serde_json::to_vec(entry).map_err(|e| e.to_string())?;
        let mut t = wtx.open_table(CACHE_REPL_ENTRY_TABLE)?;
        t.insert(rk.as_slice(), encoded.as_slice())?;

        let ik = entry.idempotency_key.to_string();
        let mut at = wtx.open_table(CACHE_REPL_APPLIED_KEYS)?;
        at.insert(ik.as_bytes(), ())?;

        let sk = entry.shard_id.as_bytes();
        let mut lt = wtx.open_table(CACHE_REPL_LOCAL_SEQ)?;
        let cur = match lt.get(sk)? {
            Some(v) => v.value(),
            None => 0,
        };
        lt.insert(sk, cur.max(entry.sequence_num))?;
        Ok(())
    }

    /// 单事务应用复制条目：幂等检查 + 数据 op（CachePut / CacheDelete）+ 簿记。
    /// Leader 本地提交与 Follower 应用共用此路径（数据一致）。
    fn apply_op_tx(&self, wtx: &redb::WriteTransaction, entry: &ReplicationEntry) -> ServiceResult<()> {
        // 幂等检查（持久化键）
        let ik = entry.idempotency_key.to_string();
        let applied = {
            let t = wtx.open_table(CACHE_REPL_APPLIED_KEYS)?;
            let x = t.get(ik.as_bytes())?.is_some();
            x
        };
        if applied {
            return Ok(());
        }
        match &entry.operation {
            ReplicationOp::CachePut { key, value, data_type } => match data_type.as_str() {
                "string" => {
                    let mut t = wtx.open_table(STRING_TABLE)?;
                    t.insert(key.as_slice(), value.as_slice())?;
                }
                "hash" => {
                    let mut t = wtx.open_table(HASH_TABLE)?;
                    t.insert(key.as_slice(), value.as_slice())?;
                }
                "list" => {
                    let mut t = wtx.open_table(LIST_TABLE)?;
                    t.insert(key.as_slice(), value.as_slice())?;
                }
                "set" => {
                    if value.len() != 8 {
                        return Err("invalid set expires_at encoding (need 8 bytes)".into());
                    }
                    let exp = u64::from_be_bytes(value[..8].try_into().unwrap());
                    let mut t = wtx.open_table(SET_TABLE)?;
                    t.insert(key.as_slice(), exp)?;
                }
                other => return Err(format!("unknown cache data_type '{other}'").into()),
            },
            ReplicationOp::CacheDelete { key, data_type } => match data_type.as_str() {
                "string" => {
                    let mut t = wtx.open_table(STRING_TABLE)?;
                    t.remove(key.as_slice())?;
                }
                "hash" => {
                    let mut t = wtx.open_table(HASH_TABLE)?;
                    t.remove(key.as_slice())?;
                }
                "list" => {
                    let mut t = wtx.open_table(LIST_TABLE)?;
                    t.remove(key.as_slice())?;
                }
                "set" => {
                    let mut t = wtx.open_table(SET_TABLE)?;
                    t.remove(key.as_slice())?;
                }
                other => return Err(format!("unknown cache data_type '{other}'").into()),
            },
            _ => return Err("cache cannot apply MqPublish op".into()),
        }
        Self::write_repl_bookkeeping_tx(wtx, entry)?;
        Ok(())
    }

    /// Leader 本地单事务应用（幂等 + 数据 + 簿记）
    fn replicated_apply_local(&self, entry: &ReplicationEntry) -> ServiceResult<()> {
        let wtx = self.write_tx()?;
        self.apply_op_tx(&wtx, entry)?;
        wtx.commit()?;
        Ok(())
    }

    /// 通用 Leader 复制写：构建 entry → 单事务本地应用 → 推送 ISR Followers →
    /// min_isr 校验（同步复制）。返回 None 表示成功。
    async fn replicated_write(&self, op: ReplicationOp) -> ServiceResult<()> {
        let rm = self
            .replication
            .read()
            .clone()
            .ok_or_else(|| "replication not enabled".to_string())?;
        if !rm.is_leader(CACHE_SHARD) {
            return Err(format!(
                "not leader for shard '{CACHE_SHARD}' (leader is {})",
                rm.shard_leader(CACHE_SHARD)
            )
            .into());
        }
        let seq = self.next_sequence()?;
        let ik = match &op {
            ReplicationOp::CachePut { key, .. } => Self::cache_idem_key("put", key),
            ReplicationOp::CacheDelete { key, .. } => Self::cache_idem_key("del", key),
            _ => return Err("cache replicated_write: unexpected op".into()),
        };
        let entry = ReplicationEntry {
            idempotency_key: ik,
            shard_id: CACHE_SHARD.to_string(),
            sequence_num: seq,
            operation: op,
        };
        self.replicated_apply_local(&entry)?;
        let acked = rm.push_to_followers(&entry).await.map_err(|e| e.to_string())?;
        rm.ensure_isr(acked + 1).map_err(|e| e.to_string())?;
        Ok(())
    }

    /// List 最大 index（读路径，Leader 用于分配新元素物理 key）
    fn list_max_index(&self, key: &str) -> ServiceResult<i64> {
        let prefix = list_key_prefix(key);
        let plen = prefix.len();
        let rtx = self.read_tx()?;
        let table = rtx.open_table(LIST_TABLE)?;
        let mut last = -1i64;
        let range: std::ops::RangeFrom<&[u8]> = prefix.as_slice()..;
        for item in table.range(range)? {
            let (k, _) = item?;
            let k = k.value();
            if !k.starts_with(&prefix) {
                break;
            }
            if let Some(idx) = decode_list_index(k, plen) {
                last = last.max(idx);
            }
        }
        Ok(last)
    }

    /// List 最小 index（读路径，Leader 用于分配左推物理 key）
    fn list_min_index(&self, key: &str) -> ServiceResult<i64> {
        let prefix = list_key_prefix(key);
        let plen = prefix.len();
        let rtx = self.read_tx()?;
        let table = rtx.open_table(LIST_TABLE)?;
        let mut first: Option<i64> = None;
        let range: std::ops::RangeFrom<&[u8]> = prefix.as_slice()..;
        for item in table.range(range)? {
            let (k, _) = item?;
            let k = k.value();
            if !k.starts_with(&prefix) {
                break;
            }
            if let Some(idx) = decode_list_index(k, plen) {
                if first.map_or(true, |f| idx < f) {
                    first = Some(idx);
                }
            }
        }
        Ok(first.unwrap_or(0))
    }

    // ──── 复制写路径（gRPC handler 在复制启用时调用）────

    pub async fn string_put_replicated(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl_secs: Option<u64>,
    ) -> ServiceResult<()> {
        let ttl = ttl_secs.unwrap_or(self.default_ttl_secs);
        let encoded = encode_value(&value, ttl); // 含绝对到期时间戳（C5）
        self.replicated_write(ReplicationOp::CachePut {
            key: key.as_bytes().to_vec(),
            value: encoded,
            data_type: "string".to_string(),
        })
        .await
    }

    pub async fn string_delete_replicated(&self, key: &str) -> ServiceResult<bool> {
        let existed = self.string_exists(key)?;
        self.replicated_write(ReplicationOp::CacheDelete {
            key: key.as_bytes().to_vec(),
            data_type: "string".to_string(),
        })
        .await?;
        Ok(existed)
    }

    pub async fn hash_field_put_replicated(
        &self,
        key: &str,
        field: &str,
        value: Vec<u8>,
        ttl_secs: Option<u64>,
    ) -> ServiceResult<()> {
        let ttl = ttl_secs.unwrap_or(self.default_ttl_secs);
        let encoded = encode_value(&value, ttl);
        let hk = encode_hash_key(key, field);
        self.replicated_write(ReplicationOp::CachePut {
            key: hk,
            value: encoded,
            data_type: "hash".to_string(),
        })
        .await
    }

    pub async fn list_push_right_replicated(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl_secs: Option<u64>,
    ) -> ServiceResult<()> {
        let ttl = ttl_secs.unwrap_or(self.default_ttl_secs);
        let encoded = encode_value(&value, ttl);
        let idx = self.list_max_index(key)? + 1;
        let lk = encode_list_key(key, idx);
        self.replicated_write(ReplicationOp::CachePut {
            key: lk,
            value: encoded,
            data_type: "list".to_string(),
        })
        .await
    }

    pub async fn list_push_left_replicated(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl_secs: Option<u64>,
    ) -> ServiceResult<()> {
        let ttl = ttl_secs.unwrap_or(self.default_ttl_secs);
        let encoded = encode_value(&value, ttl);
        let idx = self.list_min_index(key)? - 1;
        let lk = encode_list_key(key, idx);
        self.replicated_write(ReplicationOp::CachePut {
            key: lk,
            value: encoded,
            data_type: "list".to_string(),
        })
        .await
    }

    /// 复制原子出队（C2：pop 仅 Leader 执行）。Leader 在单事务内
    /// 原子出队并记录 CacheDelete 复制条目；Follower 收到删除后本地移除。
    pub async fn list_pop_replicated(
        &self,
        key: &str,
        find_max: bool,
    ) -> ServiceResult<Option<Vec<u8>>> {
        let rm = self
            .replication
            .read()
            .clone()
            .ok_or_else(|| "replication not enabled".to_string())?;
        if !rm.is_leader(CACHE_SHARD) {
            return Err(format!(
                "not leader for shard '{CACHE_SHARD}' (leader is {})",
                rm.shard_leader(CACHE_SHARD)
            )
            .into());
        }
        let seq = self.next_sequence()?;
        let prefix = list_key_prefix(key);
        let plen = prefix.len();
        let wtx = self.write_tx()?;
        let result: ServiceResult<Option<(Vec<u8>, ReplicationEntry)>> = (|| {
            let mut table = wtx.open_table(LIST_TABLE)?;
            let mut best: Option<(i64, Vec<u8>)> = None;
            let range: std::ops::RangeFrom<&[u8]> = prefix.as_slice()..;
            for item in table.range(range)? {
                let (k, raw) = item?;
                let k = k.value();
                if !k.starts_with(&prefix) {
                    break;
                }
                if let Some(idx) = decode_list_index(k, plen) {
                    let is_better = match &best {
                        Some((b, _)) => if find_max { idx > *b } else { idx < *b },
                        None => true,
                    };
                    if is_better {
                        if let Some(val) = decode_value(raw.value()) {
                            best = Some((idx, val));
                        }
                    }
                }
            }
            match best {
                Some((idx, val)) => {
                    let pkey = encode_list_key(key, idx);
                    table.remove(pkey.as_slice())?;
                    let entry = ReplicationEntry {
                        idempotency_key: Self::cache_idem_key("pop", &pkey),
                        shard_id: CACHE_SHARD.to_string(),
                        sequence_num: seq,
                        operation: ReplicationOp::CacheDelete {
                            key: pkey,
                            data_type: "list".to_string(),
                        },
                    };
                    drop(table);
                    Self::write_repl_bookkeeping_tx(&wtx, &entry)?;
                    Ok(Some((val, entry)))
                }
                None => Ok(None),
            }
        })();
        let popped = match result {
            Ok(v) => v,
            Err(e) => return Err(e),
        };
        wtx.commit()?;
        match popped {
            Some((val, entry)) => {
                let acked = rm.push_to_followers(&entry).await.map_err(|e| e.to_string())?;
                rm.ensure_isr(acked + 1).map_err(|e| e.to_string())?;
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }

    pub async fn set_add_replicated(
        &self,
        key: &str,
        member: Vec<u8>,
        ttl_secs: Option<u64>,
    ) -> ServiceResult<bool> {
        let ttl = ttl_secs.unwrap_or(self.default_ttl_secs);
        let expires_at = encode_ttl(ttl);
        let sk = encode_set_key(key, &member);
        let existed = self.set_contains(key, &member)?;
        self.replicated_write(ReplicationOp::CachePut {
            key: sk,
            value: expires_at.to_be_bytes().to_vec(),
            data_type: "set".to_string(),
        })
        .await?;
        Ok(!existed)
    }
}

// ──── ReplicatedStore（复制存储接口实现）────

impl ReplicatedStore for CacheService {
    fn shards(&self) -> Vec<String> {
        vec![CACHE_SHARD.to_string()]
    }

    fn last_local_sequence(&self, shard: &str) -> u64 {
        let rtx = match self.read_tx() {
            Ok(t) => t,
            Err(_) => return 0,
        };
        let table = match rtx.open_table(CACHE_REPL_LOCAL_SEQ) {
            Ok(t) => t,
            Err(_) => return 0,
        };
        match table.get(shard.as_bytes()) {
            Ok(Some(v)) => v.value(),
            _ => 0,
        }
    }

    fn apply_entry(&self, entry: &ReplicationEntry) -> Result<(), ReplicationError> {
        let wtx = match self.write_tx() {
            Ok(t) => t,
            Err(e) => return Err(ReplicationError::Store(e.to_string())),
        };
        let result = self.apply_op_tx(&wtx, entry);
        match result {
            Ok(()) => {
                wtx.commit().map_err(|e| ReplicationError::Store(e.to_string()))?;
                Ok(())
            }
            Err(e) => Err(ReplicationError::Store(e.to_string())),
        }
    }

    fn read_entries(&self, shard: &str, from_seq: u64, limit: u64) -> Vec<ReplicationEntry> {
        let prefix = encode_repl_prefix(shard);
        let plen = prefix.len();
        let rtx = match self.read_tx() {
            Ok(t) => t,
            Err(_) => return Vec::new(),
        };
        let table = match rtx.open_table(CACHE_REPL_ENTRY_TABLE) {
            Ok(t) => t,
            Err(_) => return Vec::new(),
        };
        let mut out = Vec::new();
        let range: std::ops::RangeFrom<&[u8]> = prefix.as_slice()..;
        if let Ok(iter) = table.range(range) {
            for item in iter {
                let (k, raw) = match item {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let k = k.value();
                if !k.starts_with(&prefix) {
                    break;
                }
                let seq = match decode_repl_seq(k, plen) {
                    Some(s) => s,
                    None => continue,
                };
                if seq < from_seq {
                    continue;
                }
                if limit > 0 && out.len() as u64 >= limit {
                    break;
                }
                if let Ok(entry) = serde_json::from_slice::<ReplicationEntry>(raw.value()) {
                    out.push(entry);
                }
            }
        }
        out
    }
}

// ──── BaseService trait 实现 ────

#[async_trait]
impl BaseService for CacheService {
    fn name(&self) -> &'static str {
        "cache"
    }

    async fn start(&self) -> ServiceResult<()> {
        if *self.started.read() {
            return Ok(());
        }

        let db_path = self.db_path.join("cache.redb");
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let db = if db_path.exists() {
            redb::Database::open(&db_path)?
        } else {
            redb::Database::create(&db_path)?
        };
        let wtx = db.begin_write()?;
        {
            wtx.open_table(STRING_TABLE)?;
            wtx.open_table(HASH_TABLE)?;
            wtx.open_table(LIST_TABLE)?;
            wtx.open_table(SET_TABLE)?;
            wtx.open_table(SHARD_TABLE)?;
            wtx.open_table(CACHE_REPL_ENTRY_TABLE)?;
            wtx.open_table(CACHE_REPL_APPLIED_KEYS)?;
            wtx.open_table(CACHE_REPL_LOCAL_SEQ)?;
        }
        wtx.commit()?;

        *self.db.write() = Some(db);
        *self.started.write() = true;
        tracing::info!("CacheService started: db_path={}", db_path.display());
        Ok(())
    }

    async fn stop(&self) -> ServiceResult<()> {
        if !*self.started.read() {
            return Ok(());
        }
        *self.db.write() = None;
        *self.started.write() = false;
        tracing::info!("CacheService stopped");
        Ok(())
    }

    fn health_check(&self) -> bool {
        if !*self.started.read() {
            return false;
        }
        self.read_tx().is_ok()
    }
}

// ──── 单元测试 ────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("failed to create temp dir")
    }

    fn new_svc(dir: &TempDir, ttl: u64) -> CacheService {
        let svc = CacheService::new(dir.path().to_path_buf(), 1024 * 1024, ttl);
        // Auto-start for unit tests
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        rt.block_on(async { svc.start().await.expect("start") });
        svc
    }

    #[test]
    fn test_name_and_default_state() {
        let dir = temp_dir();
        let svc = CacheService::new(dir.path().to_path_buf(), 1024 * 1024, 3600);
        assert_eq!(svc.name(), "cache");
        // Not started yet
        assert!(!svc.health_check());
    }

    #[test]
    fn test_string_put_get() {
        let dir = temp_dir();
        let svc = new_svc(&dir, 3600);
        svc.string_put("hello", b"world".to_vec(), None).unwrap();
        assert_eq!(svc.string_get("hello").unwrap(), Some(b"world".to_vec()));
    }

    #[test]
    fn test_string_get_missing() {
        let dir = temp_dir();
        let svc = new_svc(&dir, 3600);
        assert_eq!(svc.string_get("nope").unwrap(), None);
    }

    #[test]
    fn test_string_delete() {
        let dir = temp_dir();
        let svc = new_svc(&dir, 3600);
        svc.string_put("k", b"v".to_vec(), None).unwrap();
        assert!(svc.string_delete("k").unwrap());
        assert!(!svc.string_delete("k").unwrap());
        assert_eq!(svc.string_get("k").unwrap(), None);
    }

    #[test]
    fn test_hash_operations() {
        let dir = temp_dir();
        let svc = new_svc(&dir, 3600);
        svc.hash_field_put("user:1", "name", b"Alice".to_vec(), None).unwrap();
        svc.hash_field_put("user:1", "age", b"30".to_vec(), None).unwrap();
        assert_eq!(svc.hash_field_get("user:1", "name").unwrap(), Some(b"Alice".to_vec()));
        let all = svc.hash_get_all("user:1").unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(svc.hash_field_count("user:1").unwrap(), 2);
        assert!(svc.hash_field_delete("user:1", "name").unwrap());
        assert_eq!(svc.hash_field_count("user:1").unwrap(), 1);
    }

    #[test]
    fn test_list_operations() {
        let dir = temp_dir();
        let svc = new_svc(&dir, 3600);
        svc.list_push_right("l", b"a".to_vec(), None).unwrap();
        svc.list_push_right("l", b"b".to_vec(), None).unwrap();
        svc.list_push_right("l", b"c".to_vec(), None).unwrap();
        assert_eq!(svc.list_length("l").unwrap(), 3);
        assert_eq!(svc.list_range("l", 0, -1).unwrap(), vec![b"a", b"b", b"c"]);
        assert_eq!(svc.list_pop_left("l").unwrap(), Some(b"a".to_vec()));
        assert_eq!(svc.list_pop_right("l").unwrap(), Some(b"c".to_vec()));
        assert_eq!(svc.list_length("l").unwrap(), 1);
    }

    /// 并发 pop 原子性（G2，RED→GREEN）
    ///
    /// 不变量：无重复、无丢失、恰好 total 个、队列最终清空。
    /// 旧实现（读事务找极值 + 写事务删除，两次提交）在高竞争下会
    /// 重复出队（两个线程读到同一极值）→ unique < total。
    /// 单写事务实现由 redb 单写者模型保证串行化 → 确定性通过。
    #[test]
    fn test_list_pop_concurrent_atomic() {
        use std::collections::HashSet;
        use std::sync::{Arc, Barrier};
        use std::thread;

        let dir = temp_dir();
        let svc = Arc::new(new_svc(&dir, 3600));

        let total: usize = 200;
        for i in 0..total {
            svc.list_push_right("q", i.to_le_bytes().to_vec(), None).unwrap();
        }

        let threads = 8;
        let barrier = Arc::new(Barrier::new(threads));
        let mut handles = vec![];
        for _ in 0..threads {
            let svc = svc.clone();
            let barrier = barrier.clone();
            handles.push(thread::spawn(move || {
                barrier.wait(); // 同步起跑，放大竞争窗口
                let mut got = Vec::new();
                while let Some(v) = svc.list_pop_right("q").unwrap() {
                    got.push(v);
                }
                got
            }));
        }
        let mut results: Vec<Vec<u8>> = Vec::new();
        for h in handles {
            results.extend(h.join().unwrap());
        }

        let unique: HashSet<Vec<u8>> = results.iter().cloned().collect();
        assert_eq!(
            results.len(),
            total,
            "无丢失：应恰好 pop {} 个，实际 {}（重复出队会使总数膨胀）",
            total,
            results.len()
        );
        assert_eq!(
            unique.len(),
            total,
            "无重复：应恰好 {} 个唯一值，实际 {}",
            total,
            unique.len()
        );
        assert_eq!(svc.list_length("q").unwrap(), 0, "队列应被完全清空");
    }

    #[test]
    fn test_set_operations() {
        let dir = temp_dir();
        let svc = new_svc(&dir, 3600);
        assert!(svc.set_add("s", b"m1".to_vec(), None).unwrap());
        assert!(!svc.set_add("s", b"m1".to_vec(), None).unwrap());
        assert!(svc.set_add("s", b"m2".to_vec(), None).unwrap());
        assert_eq!(svc.set_cardinality("s").unwrap(), 2);
        assert!(svc.set_contains("s", b"m1").unwrap());
        assert!(!svc.set_contains("s", b"m3").unwrap());
        let mut members = svc.set_members("s").unwrap();
        members.sort();
        assert_eq!(members, vec![b"m1".to_vec(), b"m2".to_vec()]);
        assert!(svc.set_remove("s", b"m1").unwrap());
        assert_eq!(svc.set_cardinality("s").unwrap(), 1);
    }

    #[test]
    fn test_persistence() {
        let dir = temp_dir();
        let db_path = dir.path().to_path_buf();
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        {
            let svc = CacheService::new(db_path.clone(), 1024 * 1024, 3600);
            rt.block_on(async { svc.start().await.expect("start") });
            svc.string_put("pk", b"pv".to_vec(), None).unwrap();
        }
        {
            let svc = CacheService::new(db_path.clone(), 1024 * 1024, 3600);
            rt.block_on(async { svc.start().await.expect("start") });
            assert_eq!(svc.string_get("pk").unwrap(), Some(b"pv".to_vec()));
        }
    }

    #[test]
    fn test_flush_all() {
        let dir = temp_dir();
        let svc = new_svc(&dir, 3600);
        svc.string_put("k", b"v".to_vec(), None).unwrap();
        svc.set_add("s", b"m".to_vec(), None).unwrap();
        svc.flush_all().unwrap();
        assert_eq!(svc.string_get("k").unwrap(), None);
        assert_eq!(svc.set_cardinality("s").unwrap(), 0);
    }

    #[test]
    fn test_shard_metadata() {
        let dir = temp_dir();
        let svc = new_svc(&dir, 3600);
        svc.set_shard_meta("shard-1", CacheShardMeta {
            shard_id: "shard-1".into(),
            leader_agent: "a:9500".into(),
            replicas: vec!["b:9500".into()],
            key_range_start: vec![0],
            key_range_end: vec![127],
        }).unwrap();
        let meta = svc.get_shard_meta("shard-1").unwrap().unwrap();
        assert_eq!(meta.leader_agent, "a:9500");
        assert_eq!(svc.list_shards().unwrap().len(), 1);
    }
}

// ──── CacheBackend / CacheConfig / MokaCacheService (Phase D-moka) ────

use std::collections::HashSet;
use std::sync::Arc;

/// 缓存后端选择
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheBackend {
    /// redb 持久化嵌入式数据库（默认）
    Redb {
        data_dir: String,
    },
    /// moka 纯内存缓存（高性能，可容忍丢失）
    Moka {
        max_capacity: u64,
        time_to_live: Option<std::time::Duration>,
    },
}

impl Default for CacheBackend {
    fn default() -> Self {
        CacheBackend::Redb {
            data_dir: "/var/lib/coord-agent/cache".into(),
        }
    }
}

/// 缓存配置
#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub backend: CacheBackend,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            backend: CacheBackend::default(),
        }
    }
}

/// Moka 缓存服务 — 纯内存缓存后端
///
/// 支持 String/Hash/List/Set 四种数据类型，可选 TTL。
/// 数据不持久化，适合极高读写性能、可容忍丢失的场景。
///
/// 线程安全：所有数据结构被 Arc + parking_lot::Mutex 保护。
pub struct MokaCacheService {
    string_cache: moka::sync::Cache<String, Vec<u8>>,
    hash_cache: moka::sync::Cache<String, Vec<u8>>,
    list_cache: Arc<parking_lot::Mutex<BTreeMap<String, Vec<Vec<u8>>>>>,
    set_cache: Arc<parking_lot::Mutex<BTreeMap<String, HashSet<Vec<u8>>>>>,
    string_count: Arc<std::sync::atomic::AtomicU64>,
}

impl MokaCacheService {
    /// 使用 CacheConfig 创建 MokaCacheService
    pub fn new(config: CacheConfig) -> Self {
        let (max_capacity, ttl) = match config.backend {
            CacheBackend::Moka { max_capacity, time_to_live } => (max_capacity, time_to_live),
            _ => (1000, None), // fallback
        };

        let mut string_builder = moka::sync::Cache::builder()
            .max_capacity(max_capacity);
        let mut hash_builder = moka::sync::Cache::builder()
            .max_capacity(max_capacity * 4);

        if let Some(ttl) = ttl {
            string_builder = string_builder.time_to_live(ttl);
            hash_builder = hash_builder.time_to_live(ttl);
        }

        Self {
            string_cache: string_builder.build(),
            hash_cache: hash_builder.build(),
            list_cache: Arc::new(parking_lot::Mutex::new(BTreeMap::new())),
            set_cache: Arc::new(parking_lot::Mutex::new(BTreeMap::new())),
            string_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    // ──── String 操作 ────

    pub fn string_set(&self, key: &str, value: &[u8]) -> crate::service::ServiceResult<()> {
        self.string_cache.insert(key.to_string(), value.to_vec());
        self.string_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    pub fn string_get(&self, key: &str) -> crate::service::ServiceResult<Option<Vec<u8>>> {
        Ok(self.string_cache.get(&key.to_string()))
    }

    pub fn string_delete(&self, key: &str) -> crate::service::ServiceResult<bool> {
        let existed = self.string_cache.remove(&key.to_string()).is_some();
        if existed {
            self.string_count.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(existed)
    }

    pub fn string_exists(&self, key: &str) -> crate::service::ServiceResult<bool> {
        Ok(self.string_cache.contains_key(&key.to_string()))
    }

    // ──── Hash 操作 ────

    fn hash_compound_key(key: &str, field: &str) -> String {
        format!("{key}\x00{field}")
    }

    pub fn hash_field_set(&self, key: &str, field: &str, value: &[u8]) -> crate::service::ServiceResult<()> {
        let ck = Self::hash_compound_key(key, field);
        self.hash_cache.insert(ck, value.to_vec());
        // Track field in auxiliary index
        let index_key = format!("_hash_idx:{key}");
        let mut sets = self.set_cache.lock();
        sets.entry(index_key).or_default().insert(field.as_bytes().to_vec());
        Ok(())
    }

    pub fn hash_field_get(&self, key: &str, field: &str) -> crate::service::ServiceResult<Option<Vec<u8>>> {
        let ck = Self::hash_compound_key(key, field);
        Ok(self.hash_cache.get(&ck))
    }

    pub fn hash_get_all(&self, key: &str) -> crate::service::ServiceResult<BTreeMap<String, Vec<u8>>> {
        let index_key = format!("_hash_idx:{key}");
        let fields: Vec<String> = {
            let sets = self.set_cache.lock();
            sets.get(&index_key)
                .map(|s| s.iter().filter_map(|b| String::from_utf8(b.clone()).ok()).collect())
                .unwrap_or_default()
        };

        let mut result = BTreeMap::new();
        for field in &fields {
            if let Some(val) = self.hash_field_get(key, field)? {
                result.insert(field.clone(), val);
            }
        }
        Ok(result)
    }

    pub fn hash_field_delete(&self, key: &str, field: &str) -> crate::service::ServiceResult<bool> {
        let ck = Self::hash_compound_key(key, field);
        let existed = self.hash_cache.remove(&ck).is_some();
        if existed {
            // Remove from auxiliary index
            let index_key = format!("_hash_idx:{key}");
            let mut sets = self.set_cache.lock();
            if let Some(s) = sets.get_mut(&index_key) {
                s.remove(field.as_bytes());
            }
        }
        Ok(existed)
    }

    // ──── List 操作 ────

    pub fn list_push_left(&self, key: &str, value: Vec<u8>) -> crate::service::ServiceResult<()> {
        let mut lists = self.list_cache.lock();
        lists.entry(key.to_string()).or_default().insert(0, value);
        Ok(())
    }

    pub fn list_push_right(&self, key: &str, value: Vec<u8>) -> crate::service::ServiceResult<()> {
        let mut lists = self.list_cache.lock();
        lists.entry(key.to_string()).or_default().push(value);
        Ok(())
    }

    pub fn list_pop_left(&self, key: &str) -> crate::service::ServiceResult<Option<Vec<u8>>> {
        let mut lists = self.list_cache.lock();
        if let Some(list) = lists.get_mut(key) {
            if list.is_empty() {
                Ok(None)
            } else {
                Ok(Some(list.remove(0)))
            }
        } else {
            Ok(None)
        }
    }

    pub fn list_pop_right(&self, key: &str) -> crate::service::ServiceResult<Option<Vec<u8>>> {
        let mut lists = self.list_cache.lock();
        if let Some(list) = lists.get_mut(key) {
            Ok(list.pop())
        } else {
            Ok(None)
        }
    }

    pub fn list_len(&self, key: &str) -> crate::service::ServiceResult<usize> {
        let lists = self.list_cache.lock();
        Ok(lists.get(key).map(|l| l.len()).unwrap_or(0))
    }

    pub fn list_range(&self, key: &str, start: usize, end: usize) -> crate::service::ServiceResult<Vec<Vec<u8>>> {
        let lists = self.list_cache.lock();
        if let Some(list) = lists.get(key) {
            let end = end.min(list.len());
            if start >= end {
                return Ok(vec![]);
            }
            Ok(list[start..end].to_vec())
        } else {
            Ok(vec![])
        }
    }

    // ──── Set 操作 ────

    pub fn set_add(&self, key: &str, member: &[u8]) -> crate::service::ServiceResult<bool> {
        let mut sets = self.set_cache.lock();
        Ok(sets.entry(key.to_string()).or_default().insert(member.to_vec()))
    }

    pub fn set_remove(&self, key: &str, member: &[u8]) -> crate::service::ServiceResult<bool> {
        let mut sets = self.set_cache.lock();
        Ok(sets.get_mut(key).map(|s| s.remove(member)).unwrap_or(false))
    }

    pub fn set_contains(&self, key: &str, member: &[u8]) -> crate::service::ServiceResult<bool> {
        let sets = self.set_cache.lock();
        Ok(sets.get(key).map(|s| s.contains(member)).unwrap_or(false))
    }

    pub fn set_members(&self, key: &str) -> crate::service::ServiceResult<Vec<Vec<u8>>> {
        let sets = self.set_cache.lock();
        Ok(sets.get(key).map(|s| s.iter().cloned().collect()).unwrap_or_default())
    }

    // ──── Stats ────

    pub fn stats(&self) -> CacheStats {
        CacheStats {
            string_count: self.string_count.load(std::sync::atomic::Ordering::Relaxed),
            hash_count: 0,
            list_count: self.list_cache.lock().len() as u64,
            set_count: self.set_cache.lock().len() as u64,
            shard_count: 0,
            total_size_bytes: 0,
        }
    }
}
