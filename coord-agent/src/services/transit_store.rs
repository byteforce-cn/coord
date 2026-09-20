// coord-agent: TransitDekStore —— Transit DEK 状态共享存储抽象（Memory / Kv）
//
// 目标：信封加密的数据密钥（DEK）**重启不丢、跨 Agent 可见**（B-06 / 计划书 E1）。
// - 落盘内容**只有经 KEK 包裹的 DEK packet**（`nonce || AES-256-GCM(DEK, KEK)`）；
//   明文 DEK 永不落盘、永不离开内存（`TransitService` 侧用后 `zeroize`）。
// - 值经 coord-server redb 持久化 + Barrier 加密落库（agent 侧零加密代码），
//   与 `pki_store.rs`（`KvPkiStore`）同一条路径、同一信任模型。
// - **单次使用（用后即焚）** 语义不在本层，由 `TransitService` 在解密成功后
//   调 `delete_dek` 强制；本层只保证"读写同一份状态"。
// - 记录带 `created_at` / `expires_at`（TTL 取 `TransitConfig::dek_ttl_secs`），
//   `get_dek` 对过期条目返回 `None` 并顺手删除；`sweep_expired` 做全量清扫
//   （由 `TransitService::start` 与加密路径低频触发），避免 KV 无界增长。
//
// Key 空间：
//   /_transit/v1/dek/{dek_id}  → DekRecord（JSON）

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::proxy::AgentInner;
use crate::services::workflow_store::prefix_end;

/// DEK 键前缀（KV 空间）
pub const DEK_PREFIX: &[u8] = b"/_transit/v1/dek/";

/// 当前 UNIX 秒
pub fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// 落盘的 DEK 记录
///
/// 只含**被 KEK 包裹后**的 DEK packet；明文 DEK 从不进入本结构。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DekRecord {
    /// `nonce(12) || AES-256-GCM(DEK, KEK)(48)` —— 与密文包头内的同构
    pub dek_packet: Vec<u8>,
    /// 创建时间（UNIX 秒）
    pub created_at: u64,
    /// 过期时间（UNIX 秒）；0 = 永不过期（`dek_ttl_secs = 0`）
    pub expires_at: u64,
}

impl DekRecord {
    /// 构造记录；`ttl_secs = 0` 表示不过期（沿用本仓「0 = 关闭」的口径）
    pub fn new(dek_packet: Vec<u8>, created_at: u64, ttl_secs: u64) -> Self {
        let expires_at = if ttl_secs == 0 {
            0
        } else {
            created_at.saturating_add(ttl_secs)
        };
        Self {
            dek_packet,
            created_at,
            expires_at,
        }
    }

    /// 是否已过期（`expires_at = 0` 视为永不过期）
    pub fn is_expired(&self, now: u64) -> bool {
        self.expires_at != 0 && now >= self.expires_at
    }
}

// ──── DekStoreError ────

/// 存储层错误
#[derive(Debug)]
pub enum DekStoreError {
    /// 底层 KV 错误（连接/重定向/超时）
    Kv(String),
    /// 序列化/反序列化错误
    Serialization(String),
}

impl std::fmt::Display for DekStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Kv(msg) => write!(f, "kv error: {msg}"),
            Self::Serialization(msg) => write!(f, "serialization error: {msg}"),
        }
    }
}

impl std::error::Error for DekStoreError {}

// ──── TransitDekStore trait ────

/// Transit DEK 状态存储抽象
///
/// 生产实现 [`KvTransitDekStore`]（coord-server 共享 KV；重启不丢 + 多 Agent 共享），
/// 开发/单测实现 [`MemoryTransitDekStore`]（替代历史纯内存行为，具备同样的 TTL 语义）。
#[async_trait]
pub trait TransitDekStore: Send + Sync {
    /// 写入 DEK 记录（同 id 覆盖：`rewrap` 后 id 变化，不存在同 id 二次写入）
    async fn put_dek(&self, dek_id: &str, record: &DekRecord) -> Result<(), DekStoreError>;

    /// 读取 DEK 记录；已过期返回 `Ok(None)` 并顺手删除
    async fn get_dek(&self, dek_id: &str) -> Result<Option<DekRecord>, DekStoreError>;

    /// 删除 DEK 记录（用后即焚 / 轮换回收）；不存在不算错误
    async fn delete_dek(&self, dek_id: &str) -> Result<(), DekStoreError>;

    /// 清扫全部已过期记录，返回删除条数
    async fn sweep_expired(&self, now: u64) -> Result<usize, DekStoreError>;
}

// ──── 序列化助手 ────

pub fn serialize_dek(record: &DekRecord) -> Result<Vec<u8>, DekStoreError> {
    serde_json::to_vec(record).map_err(|e| DekStoreError::Serialization(e.to_string()))
}

pub fn deserialize_dek(bytes: &[u8]) -> Result<DekRecord, DekStoreError> {
    serde_json::from_slice(bytes).map_err(|e| DekStoreError::Serialization(e.to_string()))
}

/// `/_transit/v1/dek/{dek_id}`
pub fn dek_key(dek_id: &str) -> Vec<u8> {
    let mut k = DEK_PREFIX.to_vec();
    k.extend_from_slice(dek_id.as_bytes());
    k
}

// ──── MemoryTransitDekStore（开发 / 单测）────

/// 内存实现：与 `KvTransitDekStore` 同语义（含 TTL），用于无 server 的骨架模式与单测。
#[derive(Default)]
pub struct MemoryTransitDekStore {
    entries: RwLock<HashMap<String, DekRecord>>,
}

impl MemoryTransitDekStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// 当前条目数（测试/可观测性用）
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl TransitDekStore for MemoryTransitDekStore {
    async fn put_dek(&self, dek_id: &str, record: &DekRecord) -> Result<(), DekStoreError> {
        self.entries.write().insert(dek_id.to_string(), record.clone());
        Ok(())
    }

    async fn get_dek(&self, dek_id: &str) -> Result<Option<DekRecord>, DekStoreError> {
        let found = self.entries.read().get(dek_id).cloned();
        match found {
            Some(rec) if rec.is_expired(now_unix()) => {
                self.entries.write().remove(dek_id);
                Ok(None)
            }
            other => Ok(other),
        }
    }

    async fn delete_dek(&self, dek_id: &str) -> Result<(), DekStoreError> {
        self.entries.write().remove(dek_id);
        Ok(())
    }

    async fn sweep_expired(&self, now: u64) -> Result<usize, DekStoreError> {
        let mut map = self.entries.write();
        let before = map.len();
        map.retain(|_k, v| !v.is_expired(now));
        Ok(before - map.len())
    }
}

// ──── KvTransitDekStore（生产：coord-server 共享 KV）────

/// 生产实现：通过 `AgentInner.client` 的 KV 能力访问 coord-server 共享存储。
///
/// - 值经 coord-server redb 持久化 + Barrier 加密落库；
/// - agent 重启后仍能取回**加密态** DEK，从而解密重启前的密文（B-06 的整改目标）；
/// - 多 agent 共享同一 key 空间（同一 DEK 只被使用一次 —— 由解密方删除）。
pub struct KvTransitDekStore {
    inner: Arc<AgentInner>,
}

impl KvTransitDekStore {
    pub fn new(inner: Arc<AgentInner>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl TransitDekStore for KvTransitDekStore {
    async fn put_dek(&self, dek_id: &str, record: &DekRecord) -> Result<(), DekStoreError> {
        let value = serialize_dek(record)?;
        self.inner
            .client
            .kv()
            .put(&dek_key(dek_id), &value)
            .await
            .map_err(|e| DekStoreError::Kv(e.to_string()))?;
        Ok(())
    }

    async fn get_dek(&self, dek_id: &str) -> Result<Option<DekRecord>, DekStoreError> {
        let pairs = self
            .inner
            .client
            .kv()
            .range(&dek_key(dek_id), &[], 1, 0)
            .await
            .map_err(|e| DekStoreError::Kv(e.to_string()))?;
        let Some((_k, v)) = pairs.first() else {
            return Ok(None);
        };
        let rec = deserialize_dek(v)?;
        if rec.is_expired(now_unix()) {
            // 过期即视为不存在，并顺手回收（best-effort：失败不覆盖 None 结论）
            if let Err(e) = self.delete_dek(dek_id).await {
                tracing::warn!("transit: purge expired DEK '{dek_id}' failed: {e}");
            }
            return Ok(None);
        }
        Ok(Some(rec))
    }

    async fn delete_dek(&self, dek_id: &str) -> Result<(), DekStoreError> {
        self.inner
            .client
            .kv()
            .delete(&dek_key(dek_id))
            .await
            .map_err(|e| DekStoreError::Kv(e.to_string()))?;
        Ok(())
    }

    async fn sweep_expired(&self, now: u64) -> Result<usize, DekStoreError> {
        let end = prefix_end(DEK_PREFIX);
        // limit = 0：全量返回。DEK 条目生命周期短（默认 TTL 1h）且随用随删，
        // 量级可控；清扫本身低频（见 TransitService::maybe_sweep）。
        let pairs = self
            .inner
            .client
            .kv()
            .range(DEK_PREFIX, &end, 0, 0)
            .await
            .map_err(|e| DekStoreError::Kv(e.to_string()))?;

        let mut removed = 0usize;
        for (k, v) in pairs {
            let expired = match deserialize_dek(&v) {
                Ok(rec) => rec.is_expired(now),
                // 解析失败的条目：不动（可能是更新版本的记录，删除会丢数据）
                Err(_) => false,
            };
            if expired {
                self.inner
                    .client
                    .kv()
                    .delete(&k)
                    .await
                    .map_err(|e| DekStoreError::Kv(e.to_string()))?;
                removed += 1;
            }
        }
        Ok(removed)
    }
}

// ═══════════════════════════════════════════════════════════════════
// 测试
// ═══════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(bytes: &[u8], created_at: u64, ttl: u64) -> DekRecord {
        DekRecord::new(bytes.to_vec(), created_at, ttl)
    }

    #[test]
    fn test_record_expiry_semantics() {
        // ttl = 0 → 永不过期（本仓「0 = 关闭」口径）
        let never = rec(b"x", 100, 0);
        assert_eq!(never.expires_at, 0);
        assert!(!never.is_expired(u64::MAX));

        let ttl = rec(b"x", 100, 60);
        assert_eq!(ttl.expires_at, 160);
        assert!(!ttl.is_expired(159));
        assert!(ttl.is_expired(160));
    }

    #[tokio::test]
    async fn test_memory_store_roundtrip_and_delete() {
        let store = MemoryTransitDekStore::new();
        assert!(store.get_dek("missing").await.unwrap().is_none());

        store.put_dek("a", &rec(b"packet", now_unix(), 3600)).await.unwrap();
        assert_eq!(store.len(), 1);
        let got = store.get_dek("a").await.unwrap().expect("record");
        assert_eq!(got.dek_packet, b"packet");

        store.delete_dek("a").await.unwrap();
        assert!(store.get_dek("a").await.unwrap().is_none());
        // 幂等：重复删除不报错
        store.delete_dek("a").await.unwrap();
    }

    #[tokio::test]
    async fn test_memory_store_get_hides_and_purges_expired() {
        let store = MemoryTransitDekStore::new();
        store.put_dek("stale", &rec(b"p", 1, 1)).await.unwrap(); // expires_at = 2
        assert!(store.get_dek("stale").await.unwrap().is_none());
        assert_eq!(store.len(), 0, "过期条目应在读路径被回收");
    }

    #[tokio::test]
    async fn test_memory_sweep_expired_counts_only_expired() {
        let store = MemoryTransitDekStore::new();
        let now = now_unix();
        store.put_dek("live", &rec(b"p", now, 3600)).await.unwrap();
        store.put_dek("dead1", &rec(b"p", 1, 1)).await.unwrap();
        store.put_dek("dead2", &rec(b"p", 1, 5)).await.unwrap();
        let removed = store.sweep_expired(now).await.unwrap();
        assert_eq!(removed, 2);
        assert!(store.get_dek("live").await.unwrap().is_some());
    }

    #[test]
    fn test_dek_key_layout() {
        assert_eq!(dek_key("abcd"), b"/_transit/v1/dek/abcd".to_vec());
        assert!(dek_key("abcd").starts_with(DEK_PREFIX));
    }

    #[test]
    fn test_serialize_roundtrip() {
        let original = rec(&[1u8, 2, 3], 42, 7);
        let bytes = serialize_dek(&original).unwrap();
        assert_eq!(deserialize_dek(&bytes).unwrap(), original);
    }
}
