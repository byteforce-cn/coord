// coord-agent: FeatureFlagStore —— 特性开关状态共享存储抽象（Memory / Kv）
//
// 目标：开关状态**重启不丢、跨 Agent 可见**（计划书 P0-5 / E2）。
//
// 背景（计划书 §2.3）：`FeatureFlagService` 此前把开关放在
// `Arc<RwLock<HashMap<String, FlagState>>>`（`feature_flags.rs:71`）——
// 重启即丢；多 Agent 各持一份 ⇒ 同一开关在不同 agent 上求值结果可能不同。
// 这与 `WHITEPAPER.md` §9.1 对 Scheduler 的判定同族（"声明基于 KV，实为内存"）。
//
// ⚠️ 读路径**不做本地缓存**（有意为之）：wire 面（`coord.featureflags.v1`）
// 只有 `IsEnabled` / `Evaluate` 两个**只读** RPC，没有 Set/Delete 写面 ——
// 因此开关更新是**带外**发生的（另一个工具 / 直连 KV 写入）。没有 Watch 订阅的
// 本地缓存会静默陈旧，而"陈旧但看起来正常"正是本计划要清除的那类缺陷
// （计划书 §1 体例约束）。**正确性优先于延迟**：每次求值直读存储。
// 若后续引入写面或 Watch 失效，再按 `KvWorkflowStore` 的模式加缓存。
//
// Key 空间：
//   /_featureflags/v1/flag/{flag_key}  → FlagState（JSON）

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;

use crate::feature_flags::FlagState;
use crate::proxy::AgentInner;
use crate::services::workflow_store::prefix_end;

/// 开关键前缀（KV 空间）
pub const FEATURE_FLAG_PREFIX: &[u8] = b"/_featureflags/v1/flag/";

// ──── FeatureFlagStoreError ────

/// 存储层错误
#[derive(Debug)]
pub enum FeatureFlagStoreError {
    /// 底层 KV 错误（连接 / 重定向 / 超时）
    Kv(String),
    /// 序列化 / 反序列化错误
    Serialization(String),
}

impl std::fmt::Display for FeatureFlagStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Kv(msg) => write!(f, "kv error: {msg}"),
            Self::Serialization(msg) => write!(f, "serialization error: {msg}"),
        }
    }
}

impl std::error::Error for FeatureFlagStoreError {}

// ──── FeatureFlagStore trait ────

/// 特性开关状态存储抽象
///
/// 生产实现 [`KvFeatureFlagStore`]（coord-server 共享 KV：重启不丢 + 跨 Agent 一致）；
/// 开发 / 单测实现 [`MemoryFeatureFlagStore`]。
#[async_trait]
pub trait FeatureFlagStore: Send + Sync {
    /// 读取单个开关；不存在返回 `Ok(None)`
    async fn get(&self, key: &str) -> Result<Option<FlagState>, FeatureFlagStoreError>;

    /// 列出全部开关（按 key 升序，保证结果稳定可比）
    async fn list(&self) -> Result<Vec<(String, FlagState)>, FeatureFlagStoreError>;

    /// 写入（同 key 覆盖）
    async fn put(&self, key: &str, state: &FlagState) -> Result<(), FeatureFlagStoreError>;

    /// 删除（不存在不算错误）
    async fn delete(&self, key: &str) -> Result<(), FeatureFlagStoreError>;
}

// ──── 序列化助手 ────

pub fn serialize_flag(state: &FlagState) -> Result<Vec<u8>, FeatureFlagStoreError> {
    serde_json::to_vec(state).map_err(|e| FeatureFlagStoreError::Serialization(e.to_string()))
}

pub fn deserialize_flag(bytes: &[u8]) -> Result<FlagState, FeatureFlagStoreError> {
    serde_json::from_slice(bytes).map_err(|e| FeatureFlagStoreError::Serialization(e.to_string()))
}

/// `/_featureflags/v1/flag/{flag_key}`
pub fn flag_key(key: &str) -> Vec<u8> {
    let mut k = FEATURE_FLAG_PREFIX.to_vec();
    k.extend_from_slice(key.as_bytes());
    k
}

/// 从前缀键剥离出 flag key（与 [`flag_key`] 互逆）
fn strip_prefix(k: &[u8]) -> String {
    let raw = k.strip_prefix(FEATURE_FLAG_PREFIX).unwrap_or(k);
    String::from_utf8_lossy(raw).into_owned()
}

// ──── MemoryFeatureFlagStore（开发 / 单测）────

/// 内存实现：与 [`KvFeatureFlagStore`] 同语义，用于无 server 的骨架模式与单测。
#[derive(Default)]
pub struct MemoryFeatureFlagStore {
    flags: RwLock<HashMap<String, FlagState>>,
}

impl MemoryFeatureFlagStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// 当前开关数（测试 / 可观测性用）
    pub fn len(&self) -> usize {
        self.flags.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl FeatureFlagStore for MemoryFeatureFlagStore {
    async fn get(&self, key: &str) -> Result<Option<FlagState>, FeatureFlagStoreError> {
        Ok(self.flags.read().get(key).cloned())
    }

    async fn list(&self) -> Result<Vec<(String, FlagState)>, FeatureFlagStoreError> {
        let flags = self.flags.read();
        let mut out: Vec<(String, FlagState)> =
            flags.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    async fn put(&self, key: &str, state: &FlagState) -> Result<(), FeatureFlagStoreError> {
        self.flags.write().insert(key.to_string(), state.clone());
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), FeatureFlagStoreError> {
        self.flags.write().remove(key);
        Ok(())
    }
}

// ──── KvFeatureFlagStore（生产：coord-server 共享 KV）────

/// 生产实现：经 `AgentInner.client` 的 KV 能力访问 coord-server 共享存储。
///
/// - 值经 coord-server redb 持久化 + Raft 共识落库 ⇒ agent 重启后开关仍在；
/// - 多 Agent 共享同一键空间 ⇒ 同一开关在各 agent 上求值一致。
pub struct KvFeatureFlagStore {
    inner: Arc<AgentInner>,
}

impl KvFeatureFlagStore {
    pub fn new(inner: Arc<AgentInner>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl FeatureFlagStore for KvFeatureFlagStore {
    async fn get(&self, key: &str) -> Result<Option<FlagState>, FeatureFlagStoreError> {
        let k = flag_key(key);
        let pairs = self
            .inner
            .client
            .kv()
            .range(&k, &[], 1, 0)
            .await
            .map_err(|e| FeatureFlagStoreError::Kv(e.to_string()))?;
        match pairs.first() {
            Some((_k, v)) => Ok(Some(deserialize_flag(v)?)),
            None => Ok(None),
        }
    }

    async fn list(&self) -> Result<Vec<(String, FlagState)>, FeatureFlagStoreError> {
        let end = prefix_end(FEATURE_FLAG_PREFIX);
        let pairs = self
            .inner
            .client
            .kv()
            .range(FEATURE_FLAG_PREFIX, &end, 0, 0)
            .await
            .map_err(|e| FeatureFlagStoreError::Kv(e.to_string()))?;

        let mut out = Vec::with_capacity(pairs.len());
        for (k, v) in pairs {
            out.push((strip_prefix(&k), deserialize_flag(&v)?));
        }
        // KV range 本已按键序返回；显式排序使契约不依赖后端行为。
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    async fn put(&self, key: &str, state: &FlagState) -> Result<(), FeatureFlagStoreError> {
        let value = serialize_flag(state)?;
        self.inner
            .client
            .kv()
            .put(&flag_key(key), &value)
            .await
            .map_err(|e| FeatureFlagStoreError::Kv(e.to_string()))?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), FeatureFlagStoreError> {
        self.inner
            .client
            .kv()
            .delete(&flag_key(key))
            .await
            .map_err(|e| FeatureFlagStoreError::Kv(e.to_string()))?;
        Ok(())
    }
}

// ═══════════════════════════════════════════════════════════════════
// 测试
// ═══════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    fn state(enabled: bool, percentage: Option<u8>) -> FlagState {
        FlagState {
            enabled,
            percentage,
        }
    }

    #[test]
    fn test_flag_key_layout_and_roundtrip() {
        assert_eq!(flag_key("beta"), b"/_featureflags/v1/flag/beta".to_vec());
        assert!(flag_key("beta").starts_with(FEATURE_FLAG_PREFIX));
        assert_eq!(strip_prefix(&flag_key("beta")), "beta");
    }

    #[test]
    fn test_serialize_roundtrip() {
        let s = state(true, Some(25));
        let bytes = serialize_flag(&s).unwrap();
        assert_eq!(deserialize_flag(&bytes).unwrap(), s);
    }

    #[tokio::test]
    async fn test_memory_put_get_delete() {
        let store = MemoryFeatureFlagStore::new();
        assert!(store.get("missing").await.unwrap().is_none());

        store.put("a", &state(true, None)).await.unwrap();
        assert_eq!(store.get("a").await.unwrap(), Some(state(true, None)));
        assert_eq!(store.len(), 1);

        // 覆盖写
        store.put("a", &state(false, Some(10))).await.unwrap();
        assert_eq!(store.get("a").await.unwrap(), Some(state(false, Some(10))));
        assert_eq!(store.len(), 1);

        store.delete("a").await.unwrap();
        assert!(store.get("a").await.unwrap().is_none());
        // 幂等：重复删除不报错
        store.delete("a").await.unwrap();
    }

    #[tokio::test]
    async fn test_memory_list_is_sorted_and_complete() {
        let store = MemoryFeatureFlagStore::new();
        store.put("zeta", &state(true, None)).await.unwrap();
        store.put("alpha", &state(false, None)).await.unwrap();
        store.put("mid", &state(true, Some(50))).await.unwrap();

        let list = store.list().await.unwrap();
        assert_eq!(
            list.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
            vec!["alpha", "mid", "zeta"],
            "list 必须按键升序，结果稳定可比"
        );
    }

    #[tokio::test]
    async fn test_memory_store_is_shared_between_service_instances() {
        // 「重启存续」的进程内等价物：同一 store、不同 service 实例
        let store = Arc::new(MemoryFeatureFlagStore::new());
        store.put("persisted", &state(true, None)).await.unwrap();

        let s1: Arc<dyn FeatureFlagStore> = store.clone();
        let s2: Arc<dyn FeatureFlagStore> = store;
        assert_eq!(
            s1.get("persisted").await.unwrap(),
            s2.get("persisted").await.unwrap()
        );
        assert!(s2.get("persisted").await.unwrap().is_some());
    }
}
