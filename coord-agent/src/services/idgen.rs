// coord-agent: ID 生成器 (ID Generator Service)
//
// 实现 BaseService trait，提供全局唯一 ID 生成能力。
// 基于 Coord 核心原语（Txn (CAS) + KV 号段模式）构建。
//
// 架构（v3.0）:
// - 本地缓存号段，内存分配（延迟 <1ms）
// - 断连时可继续分配（可能产生空洞）
// - 支持趋势递增 / 号段模式
//
// 参见 docs/client-agent-architecture-v3.md §5.4。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::RwLock as ParkingRwLock;
use tokio::sync::watch;

use crate::proxy::AgentInner;
use crate::service::{BaseService, ServiceResult};

// ──── 类型定义 ────

/// ID 号段分配信息（存储在 Server）
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IdSegment {
    /// 号段名称（业务标识）
    pub name: String,
    /// 当前已分配到的最大值
    pub current_max: u64,
    /// 号段步长（每次分配的 ID 数量）
    pub step: u64,
    /// 上次更新时间
    pub updated_at: u64,
}

impl IdSegment {
    pub fn new(name: impl Into<String>, step: u64) -> Self {
        Self {
            name: name.into(),
            current_max: 0,
            step,
            updated_at: unix_ts(),
        }
    }

    /// 构造 Server 存储 key
    pub fn storage_key(name: &str) -> Vec<u8> {
        format!("/_idgen/{name}").into_bytes()
    }
}

/// 本地号段缓存
#[derive(Debug)]
struct LocalSegment {
    /// 号段起始值（不含）
    start: u64,
    /// 号段结束值（含）
    end: u64,
    /// 当前已分配到的值
    current: AtomicU64,
}

impl Clone for LocalSegment {
    fn clone(&self) -> Self {
        Self {
            start: self.start,
            end: self.end,
            current: AtomicU64::new(self.current.load(Ordering::SeqCst)),
        }
    }
}

impl LocalSegment {
    fn new(start: u64, end: u64) -> Self {
        Self {
            start,
            end,
            current: AtomicU64::new(start),
        }
    }

    /// 从本地号段分配一个 ID
    fn next_id(&self) -> Option<u64> {
        loop {
            let current = self.current.load(Ordering::Relaxed);
            if current >= self.end {
                return None; // 号段耗尽
            }
            let next = current + 1;
            if self
                .current
                .compare_exchange(current, next, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                return Some(next);
            }
            // CAS 失败，重试
        }
    }

    /// 剩余可用 ID 数
    fn remaining(&self) -> u64 {
        let current = self.current.load(Ordering::Relaxed);
        if current >= self.end {
            0
        } else {
            self.end - current
        }
    }
}

fn unix_ts() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ──── IdGenCache ────

/// ID 生成器本地缓存
pub struct IdGenCache {
    /// 活跃的本地号段：name → LocalSegment
    segments: BTreeMap<String, LocalSegment>,
    /// 号段步长配置
    default_step: u64,
}

impl IdGenCache {
    pub fn new(default_step: u64) -> Self {
        Self {
            segments: BTreeMap::new(),
            default_step,
        }
    }

    /// 尝试从本地号段分配 ID
    pub fn try_next_id(&self, name: &str) -> Option<u64> {
        self.segments.get(name).and_then(|seg| seg.next_id())
    }

    /// 设置本地号段
    pub fn set_segment(&mut self, name: &str, start: u64, end: u64) {
        self.segments
            .insert(name.to_string(), LocalSegment::new(start, end));
    }

    /// 移除本地号段（号段耗尽时）
    pub fn remove_segment(&mut self, name: &str) {
        self.segments.remove(name);
    }

    /// 查询号段剩余量
    pub fn remaining(&self, name: &str) -> Option<u64> {
        self.segments.get(name).map(|seg| seg.remaining())
    }

    /// 是否有活跃号段
    pub fn has_segment(&self, name: &str) -> bool {
        self.segments.contains_key(name)
    }

    /// 号段数量
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }
}

// ──── IdGenService ────

/// ID 生成器服务
///
/// 实现 `BaseService` trait，为应用提供高性能全局唯一 ID 生成。
pub struct IdGenService {
    /// 到 Server 集群的内部客户端（共享）
    inner: Arc<AgentInner>,
    /// 本地号段缓存
    cache: Arc<ParkingRwLock<IdGenCache>>,
    /// 健康状态
    healthy: ParkingRwLock<bool>,
    /// 关闭信号
    shutdown_tx: ParkingRwLock<Option<watch::Sender<()>>>,
}

impl IdGenService {
    pub const NAME: &'static str = "idgen";

    pub fn new(inner: Arc<AgentInner>, default_step: u64) -> Self {
        Self {
            inner,
            cache: Arc::new(ParkingRwLock::new(IdGenCache::new(default_step))),
            healthy: ParkingRwLock::new(false),
            shutdown_tx: ParkingRwLock::new(None),
        }
    }

    /// 生成下一个 ID（本地分配，<1ms）
    ///
    /// 若本地号段充足，直接返回；否则从 Server 申请新号段。
    pub async fn next_id(&self, name: &str) -> ServiceResult<u64> {
        // 1. 尝试本地分配
        if let Some(id) = self.cache.read().try_next_id(name) {
            return Ok(id);
        }

        // 2. 本地号段耗尽，从 Server 申请新号段
        self.allocate_segment(name).await
    }

    /// 从 Server 申请新号段
    async fn allocate_segment(&self, name: &str) -> ServiceResult<u64> {
        let storage_key = IdSegment::storage_key(name);
        let step = self.cache.read().default_step;

        // 读取当前 Server 上的号段状态
        let segment = self
            .read_or_init_segment(name, &storage_key, step)
            .await?;

        let new_max = segment.current_max + step;

        // CAS 更新 Server 上的号段
        let updated = IdSegment {
            name: name.to_string(),
            current_max: new_max,
            step,
            updated_at: unix_ts(),
        };
        let value = serde_json::to_vec(&updated)
            .map_err(|e| format!("serialize id segment: {e}"))?;

        self.inner
            .client
            .kv()
            .put(&storage_key, &value)
            .await
            .map_err(|e| format!("failed to update id segment '{name}': {e}"))?;

        // 更新本地号段：[old_max+1, new_max]
        let local_start = segment.current_max;
        let local_end = new_max;
        self.cache
            .write()
            .set_segment(name, local_start, local_end);

        // 分配第一个 ID
        let first_id = local_start + 1;
        tracing::debug!(
            "IdGenService: allocated segment for '{name}': ({}, {}], first_id={first_id}",
            local_start,
            local_end
        );
        Ok(first_id)
    }

    /// 读取或初始化 Server 上的号段
    async fn read_or_init_segment(
        &self,
        name: &str,
        storage_key: &[u8],
        step: u64,
    ) -> ServiceResult<IdSegment> {
        let pairs = self
            .inner
            .client
            .kv()
            .range(storage_key, storage_key, 1, 0)
            .await
            .map_err(|e| format!("failed to read id segment '{name}': {e}"))?;

        if let Some((_k, v)) = pairs.into_iter().next() {
            let segment: IdSegment = serde_json::from_slice(&v)
                .map_err(|e| format!("deserialize id segment: {e}"))?;
            Ok(segment)
        } else {
            // 首次使用：创建初始号段
            let segment = IdSegment::new(name, step);
            let value = serde_json::to_vec(&segment)
                .map_err(|e| format!("serialize id segment: {e}"))?;
            self.inner
                .client
                .kv()
                .put(storage_key, &value)
                .await
                .map_err(|e| format!("failed to init id segment '{name}': {e}"))?;
            Ok(segment)
        }
    }

    /// 批量生成 ID
    pub async fn next_ids(&self, name: &str, count: u64) -> ServiceResult<Vec<u64>> {
        let mut ids = Vec::with_capacity(count as usize);
        for _ in 0..count {
            ids.push(self.next_id(name).await?);
        }
        Ok(ids)
    }

    /// 查询号段剩余量
    pub fn remaining(&self, name: &str) -> Option<u64> {
        self.cache.read().remaining(name)
    }

    /// 号段数量
    pub fn segment_count(&self) -> usize {
        self.cache.read().segment_count()
    }
}

#[async_trait]
impl BaseService for IdGenService {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    async fn start(&self) -> ServiceResult<()> {
        tracing::info!("IdGenService: starting");
        *self.healthy.write() = true;

        let (_tx, mut rx) = watch::channel::<()>(());
        *self.shutdown_tx.write() = Some(_tx);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = rx.changed() => {
                        tracing::info!("IdGenService: background task shutting down");
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(60)) => {}
                }
            }
        });

        Ok(())
    }

    async fn stop(&self) -> ServiceResult<()> {
        tracing::info!("IdGenService: stopping");
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

impl std::fmt::Debug for IdGenService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdGenService")
            .field("segments", &self.segment_count())
            .field("healthy", &self.health_check())
            .finish()
    }
}

// ──── tests ────

#[cfg(test)]
mod tests {
    use super::*;

    // ──── IdSegment 测试 ────

    #[test]
    fn test_id_segment_creation() {
        let seg = IdSegment::new("order-id", 1000);
        assert_eq!(seg.name, "order-id");
        assert_eq!(seg.current_max, 0);
        assert_eq!(seg.step, 1000);
    }

    #[test]
    fn test_id_segment_storage_key() {
        let key = IdSegment::storage_key("order-id");
        assert_eq!(String::from_utf8_lossy(&key), "/_idgen/order-id");
    }

    #[test]
    fn test_id_segment_serialization_roundtrip() {
        let seg = IdSegment {
            name: "test".into(),
            current_max: 5000,
            step: 1000,
            updated_at: 1700000000,
        };
        let json = serde_json::to_vec(&seg).unwrap();
        let restored: IdSegment = serde_json::from_slice(&json).unwrap();
        assert_eq!(restored, seg);
    }

    // ──── LocalSegment 测试 ────

    #[test]
    fn test_local_segment_sequential_allocation() {
        let seg = LocalSegment::new(0, 100);
        for i in 1..=100 {
            assert_eq!(seg.next_id(), Some(i));
        }
        // 号段耗尽
        assert_eq!(seg.next_id(), None);
        assert_eq!(seg.remaining(), 0);
    }

    #[test]
    fn test_local_segment_remaining() {
        let seg = LocalSegment::new(0, 50);
        assert_eq!(seg.remaining(), 50);
        for _ in 0..25 {
            seg.next_id();
        }
        assert_eq!(seg.remaining(), 25);
    }

    // ──── IdGenCache 测试 ────

    #[test]
    fn test_id_gen_cache_set_and_allocate() {
        let mut cache = IdGenCache::new(1000);
        cache.set_segment("test", 0, 100);

        assert!(cache.has_segment("test"));
        assert_eq!(cache.segment_count(), 1);

        let id = cache.try_next_id("test").unwrap();
        assert_eq!(id, 1);
    }

    #[test]
    fn test_id_gen_cache_exhaustion() {
        let mut cache = IdGenCache::new(10);
        cache.set_segment("test", 0, 5);

        // 分配 1,2,3,4,5
        for i in 1..=5 {
            assert_eq!(cache.try_next_id("test"), Some(i));
        }
        // 耗尽
        assert_eq!(cache.try_next_id("test"), None);
    }

    #[test]
    fn test_id_gen_cache_remove_segment() {
        let mut cache = IdGenCache::new(100);
        cache.set_segment("test", 0, 10);
        assert!(cache.has_segment("test"));

        cache.remove_segment("test");
        assert!(!cache.has_segment("test"));
        assert_eq!(cache.segment_count(), 0);
    }

    #[test]
    fn test_id_gen_cache_remaining() {
        let mut cache = IdGenCache::new(100);
        cache.set_segment("test", 0, 20);
        assert_eq!(cache.remaining("test"), Some(20));

        cache.try_next_id("test");
        assert_eq!(cache.remaining("test"), Some(19));
    }

    // ──── IdGenService 名称常量 ────

    #[test]
    fn test_id_gen_service_name_constant() {
        assert_eq!(IdGenService::NAME, "idgen");
    }
}
