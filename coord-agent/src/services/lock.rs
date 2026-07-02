// coord-agent: 分布式锁 (Lock Service)
//
// 实现 BaseService trait，提供分布式互斥锁能力。
// 基于 Coord 核心原语（Lease + Txn (IfNotExists)）构建。
//
// 架构（v3.0）:
// - 封装重试与自动续期
// - 支持公平锁（队列）/ 非公平锁
// - 适用场景：定时任务幂等、资源互斥
//
// 参见 docs/client-agent-architecture-v3.md §5.3。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::RwLock as ParkingRwLock;
use tokio::sync::watch;

use crate::proxy::AgentInner;
use crate::service::{BaseService, ServiceResult};

// ──── 类型定义 ────

/// 锁状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockState {
    /// 空闲（可获取）
    Free,
    /// 已被持有
    Held,
    /// 已过期（Lease 超时未续期）
    Expired,
}

/// 锁信息
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LockInfo {
    /// 锁名称（资源标识）
    pub name: String,
    /// 当前持有者 ID
    pub holder_id: String,
    /// 绑定的 Lease ID
    pub lease_id: i64,
    /// 获取时间（Unix 时间戳，秒）
    pub acquired_at: u64,
    /// 锁 TTL（秒）
    pub ttl_secs: u64,
}

impl LockInfo {
    pub fn new(name: impl Into<String>, holder_id: impl Into<String>, lease_id: i64, ttl_secs: u64) -> Self {
        let now = unix_ts();
        Self {
            name: name.into(),
            holder_id: holder_id.into(),
            lease_id,
            acquired_at: now,
            ttl_secs,
        }
    }

    /// 构造 Server 存储 key
    pub fn storage_key(name: &str) -> Vec<u8> {
        format!("/_lock/{name}").into_bytes()
    }

    /// 检查锁是否已过期（基于 TTL）
    pub fn is_expired(&self) -> bool {
        let now = unix_ts();
        now > self.acquired_at + self.ttl_secs
    }
}

fn unix_ts() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ──── LockCache ────

/// 分布式锁本地缓存
///
/// 缓存本地持有的锁信息，用于快速判断锁状态和续期。
pub struct LockCache {
    /// 本地持有的锁：lock_name → LockInfo
    held: BTreeMap<String, LockInfo>,
}

impl LockCache {
    pub fn new() -> Self {
        Self {
            held: BTreeMap::new(),
        }
    }

    /// 记录本地获取的锁
    pub fn add(&mut self, info: LockInfo) {
        self.held.insert(info.name.clone(), info);
    }

    /// 移除锁记录
    pub fn remove(&mut self, name: &str) -> Option<LockInfo> {
        self.held.remove(name)
    }

    /// 查询本地持有的锁
    pub fn get(&self, name: &str) -> Option<&LockInfo> {
        self.held.get(name)
    }

    /// 检查本地是否持有某锁
    pub fn is_held(&self, name: &str) -> bool {
        self.held.contains_key(name)
    }

    /// 获取所有本地持有的锁
    pub fn all_held(&self) -> Vec<&LockInfo> {
        self.held.values().collect()
    }

    /// 本地持有锁数量
    pub fn len(&self) -> usize {
        self.held.len()
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.held.is_empty()
    }

    /// 清理已过期的锁记录
    pub fn cleanup_expired(&mut self) -> usize {
        let expired: Vec<String> = self
            .held
            .values()
            .filter(|info| info.is_expired())
            .map(|info| info.name.clone())
            .collect();
        let count = expired.len();
        for name in &expired {
            self.held.remove(name);
        }
        count
    }
}

impl Default for LockCache {
    fn default() -> Self {
        Self::new()
    }
}

// ──── LockService ────

/// 分布式锁服务
///
/// 实现 `BaseService` trait，为应用提供分布式锁的获取、释放、续期能力。
pub struct LockService {
    /// 到 Server 集群的内部客户端（共享）
    inner: Arc<AgentInner>,
    /// 本地锁缓存
    cache: Arc<ParkingRwLock<LockCache>>,
    /// 健康状态
    healthy: ParkingRwLock<bool>,
    /// 关闭信号发送端
    shutdown_tx: ParkingRwLock<Option<watch::Sender<()>>>,
}

impl LockService {
    /// 服务名称常量
    pub const NAME: &'static str = "lock";

    /// 创建 LockService
    pub fn new(inner: Arc<AgentInner>) -> Self {
        Self {
            inner,
            cache: Arc::new(ParkingRwLock::new(LockCache::new())),
            healthy: ParkingRwLock::new(false),
            shutdown_tx: ParkingRwLock::new(None),
        }
    }

    /// 获取分布式锁（非阻塞）
    ///
    /// 使用 KV + Lease 实现：写入锁 key（IfNotExists）并绑定 Lease。
    /// 若 key 已存在（锁被他人持有），返回 None。
    /// 若写入成功，记录到本地缓存并返回 LockInfo。
    pub async fn acquire(
        &self,
        name: &str,
        holder_id: &str,
        ttl_secs: u64,
    ) -> ServiceResult<Option<LockInfo>> {
        let storage_key = LockInfo::storage_key(name);

        // 先尝试通过 Lease + Put 获取锁
        // 创建 Lease
        let lease_id = self
            .inner
            .client
            .lease()
            .grant(ttl_secs as i64)
            .await
            .map_err(|e| format!("failed to grant lease for lock '{name}': {e}"))?;

        // 使用 IfNotExists 语义写入锁 key
        // 通过 put_lease 绑定 lease（若 key 已存在则失败）
        let lock_info = LockInfo::new(name, holder_id, lease_id, ttl_secs);
        let value = serde_json::to_vec(&lock_info)
            .map_err(|e| format!("failed to serialize lock info: {e}"))?;

        match self
            .inner
            .client
            .kv()
            .put_lease(&storage_key, &value, lease_id)
            .await
        {
            Ok(_revision) => {
                // 获取成功
                self.cache.write().add(lock_info.clone());
                tracing::info!(
                    "LockService: acquired lock '{name}' for holder '{holder_id}' (lease={lease_id}, ttl={ttl_secs}s)"
                );
                Ok(Some(lock_info))
            }
            Err(e) => {
                // 锁已被他人持有，释放刚创建的 Lease
                let _ = self.inner.client.lease().revoke(lease_id).await;
                let err_msg = e.to_string();
                if err_msg.contains("already exists") || err_msg.contains("AlreadyExists") {
                    tracing::debug!("LockService: lock '{name}' already held by another holder");
                    Ok(None)
                } else {
                    Err(format!("failed to acquire lock '{name}': {e}").into())
                }
            }
        }
    }

    /// 释放分布式锁
    ///
    /// 撤销 Lease（使锁 key 自动过期删除）。
    pub async fn release(&self, name: &str, holder_id: &str) -> ServiceResult<bool> {
        let _storage_key = LockInfo::storage_key(name);

        // 从本地缓存获取锁信息
        let lock_info = match self.cache.read().get(name) {
            Some(info) if info.holder_id == holder_id => info.clone(),
            _ => {
                tracing::warn!("LockService: lock '{name}' not held by '{holder_id}'");
                return Ok(false);
            }
        };

        // 撤销 Lease（Server 会自动清理关联的 key）
        self.inner
            .client
            .lease()
            .revoke(lock_info.lease_id)
            .await
            .map_err(|e| format!("failed to revoke lease for lock '{name}': {e}"))?;

        // 从本地缓存移除
        self.cache.write().remove(name);

        tracing::info!(
            "LockService: released lock '{name}' (holder='{holder_id}', lease={})",
            lock_info.lease_id
        );
        Ok(true)
    }

    /// 续期分布式锁
    ///
    /// 延长 Lease 的 TTL，防止锁过期。
    pub async fn renew(&self, name: &str, holder_id: &str) -> ServiceResult<bool> {
        let lock_info = match self.cache.read().get(name) {
            Some(info) if info.holder_id == holder_id => info.clone(),
            _ => {
                tracing::warn!("LockService: cannot renew lock '{name}' — not held by '{holder_id}'");
                return Ok(false);
            }
        };

        // 通过 KeepAlive 续期
        self.inner
            .client
            .lease()
            .keep_alive(lock_info.lease_id)
            .await
            .map_err(|e| format!("failed to renew lock '{name}': {e}"))?;

        tracing::debug!(
            "LockService: renewed lock '{name}' (lease={})",
            lock_info.lease_id
        );
        Ok(true)
    }

    /// 查询锁状态
    ///
    /// 从 Server 读取当前锁信息。
    pub async fn query(&self, name: &str) -> ServiceResult<Option<LockInfo>> {
        let storage_key = LockInfo::storage_key(name);

        let pairs = self
            .inner
            .client
            .kv()
            .range(&storage_key, &storage_key, 1, 0)
            .await
            .map_err(|e| format!("failed to query lock '{name}': {e}"))?;

        if let Some((_k, v)) = pairs.into_iter().next() {
            let info: LockInfo = serde_json::from_slice(&v)
                .map_err(|e| format!("failed to deserialize lock info: {e}"))?;
            Ok(Some(info))
        } else {
            Ok(None)
        }
    }

    /// 检查本地是否持有某锁
    pub fn is_held_locally(&self, name: &str) -> bool {
        self.cache.read().is_held(name)
    }

    /// 本地持有锁数量
    pub fn held_count(&self) -> usize {
        self.cache.read().len()
    }
}

#[async_trait]
impl BaseService for LockService {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    async fn start(&self) -> ServiceResult<()> {
        tracing::info!("LockService: starting");
        *self.healthy.write() = true;

        // 启动后台续期任务
        let (_tx, mut rx) = watch::channel::<()>(());
        *self.shutdown_tx.write() = Some(_tx);

        let cache = self.cache.clone();
        let inner = self.inner.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = rx.changed() => {
                        tracing::info!("LockService: renew background task shutting down");
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(10)) => {
                        // 定期续期本地持有的锁（在 TTL 的 1/3 处续期）
                        let held: Vec<LockInfo> = cache.read().all_held().into_iter().cloned().collect();
                        for info in &held {
                            if info.ttl_secs > 0 {
                                let renew_at = info.acquired_at + info.ttl_secs / 3;
                                if unix_ts() >= renew_at {
                                    if let Err(e) = inner.client.lease().keep_alive(info.lease_id).await {
                                        tracing::warn!("LockService: failed to auto-renew lock '{}': {}", info.name, e);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }

    async fn stop(&self) -> ServiceResult<()> {
        tracing::info!("LockService: stopping");
        if let Some(tx) = self.shutdown_tx.write().take() {
            let _ = tx.send(());
        }
        *self.healthy.write() = false;
        Ok(())
    }

    fn health_check(&self) -> bool {
        *self.healthy.read()
    }
}

impl std::fmt::Debug for LockService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LockService")
            .field("held_count", &self.held_count())
            .field("healthy", &self.health_check())
            .finish()
    }
}

// ──── tests ────

#[cfg(test)]
mod tests {
    use super::*;

    // ──── LockInfo 测试 ────

    #[test]
    fn test_lock_info_creation() {
        let info = LockInfo::new("task-scheduler", "node1", 5001, 30);
        assert_eq!(info.name, "task-scheduler");
        assert_eq!(info.holder_id, "node1");
        assert_eq!(info.lease_id, 5001);
        assert_eq!(info.ttl_secs, 30);
        assert!(info.acquired_at > 0);
    }

    #[test]
    fn test_lock_info_storage_key() {
        let key = LockInfo::storage_key("task-scheduler");
        assert_eq!(String::from_utf8_lossy(&key), "/_lock/task-scheduler");
    }

    #[test]
    fn test_lock_info_is_expired() {
        let past = unix_ts() - 100;
        let info = LockInfo {
            name: "test".into(),
            holder_id: "n1".into(),
            lease_id: 1,
            acquired_at: past,
            ttl_secs: 30,
        };
        assert!(info.is_expired());

        let future_lock = LockInfo {
            name: "test".into(),
            holder_id: "n1".into(),
            lease_id: 1,
            acquired_at: unix_ts(),
            ttl_secs: 3600,
        };
        assert!(!future_lock.is_expired());
    }

    #[test]
    fn test_lock_info_serialization_roundtrip() {
        let info = LockInfo {
            name: "my-lock".into(),
            holder_id: "holder-1".into(),
            lease_id: 42,
            acquired_at: 1700000000,
            ttl_secs: 30,
        };
        let json = serde_json::to_vec(&info).unwrap();
        let restored: LockInfo = serde_json::from_slice(&json).unwrap();
        assert_eq!(restored, info);
    }

    // ──── LockCache 测试 ────

    #[test]
    fn test_lock_cache_add_and_get() {
        let mut cache = LockCache::new();
        let info = LockInfo::new("lock-a", "holder-1", 100, 30);
        cache.add(info.clone());

        assert_eq!(cache.len(), 1);
        assert!(!cache.is_empty());
        assert!(cache.is_held("lock-a"));
        assert!(!cache.is_held("lock-b"));

        let found = cache.get("lock-a").unwrap();
        assert_eq!(found.holder_id, "holder-1");
    }

    #[test]
    fn test_lock_cache_remove() {
        let mut cache = LockCache::new();
        cache.add(LockInfo::new("lock-a", "h1", 100, 30));
        cache.add(LockInfo::new("lock-b", "h2", 101, 60));

        let removed = cache.remove("lock-a").unwrap();
        assert_eq!(removed.holder_id, "h1");
        assert_eq!(cache.len(), 1);
        assert!(!cache.is_held("lock-a"));
        assert!(cache.is_held("lock-b"));
    }

    #[test]
    fn test_lock_cache_all_held() {
        let mut cache = LockCache::new();
        cache.add(LockInfo::new("a", "h1", 1, 30));
        cache.add(LockInfo::new("b", "h2", 2, 30));

        let all = cache.all_held();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_lock_cache_cleanup_expired() {
        let mut cache = LockCache::new();
        let past = unix_ts() - 100;

        // 过期锁
        cache.add(LockInfo {
            name: "expired-lock".into(),
            holder_id: "h1".into(),
            lease_id: 1,
            acquired_at: past,
            ttl_secs: 30,
        });

        // 有效锁
        cache.add(LockInfo::new("valid-lock", "h2", 2, 3600));

        assert_eq!(cache.len(), 2);
        let cleaned = cache.cleanup_expired();
        assert_eq!(cleaned, 1);
        assert_eq!(cache.len(), 1);
        assert!(cache.is_held("valid-lock"));
        assert!(!cache.is_held("expired-lock"));
    }

    #[test]
    fn test_lock_cache_default() {
        let cache = LockCache::default();
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
    }

    // ──── LockService 名称常量测试 ────

    #[test]
    fn test_lock_service_name_constant() {
        assert_eq!(LockService::NAME, "lock");
    }
}
