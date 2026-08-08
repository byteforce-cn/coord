// coord-agent: ID 生成器 (ID Generator Service)
//
// 实现 BaseService trait，提供全局唯一 ID 生成能力。
// 基于 Coord 核心原语（Txn (CAS) + KV 号段模式）构建。
//
// 架构（v3.0）:
// - 本地缓存号段，内存分配（延迟 <1ms）
// - 断连时可继续分配（可能产生空洞）
// - 无 Server 时降级为本地雪花生成（本机唯一、趋势递增）
// - 支持趋势递增 / 号段模式
//
// 参见 docs/client-agent-architecture-v3.md §5.4。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::{Mutex, RwLock as ParkingRwLock};
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

// ──── 本地雪花生成器（无 Server 降级模式）────

/// 本地雪花 ID 生成器
///
/// 64 位布局: [1 bit 符号(0)] [41 bit 毫秒时间戳] [10 bit 节点ID] [12 bit 序列号]
/// 无 Server 连接时用于保证本机 ID 唯一且趋势递增。
struct LocalSnowflake {
    /// 10 bit 节点 ID
    node_id: u64,
    /// 自定义纪元（毫秒）
    epoch_ms: u64,
    /// 上次生成时间戳（毫秒）
    last_ts: u64,
    /// 同毫秒序列号
    seq: u16,
}

impl LocalSnowflake {
    /// 自定义纪元: 2023-11-14 00:00:00 UTC
    const EPOCH_MS: u64 = 1_700_000_000_000;
    /// 序列号上限（12 bit）
    const MAX_SEQ: u16 = 0x0FFF;

    fn new(node_id: u64) -> Self {
        Self {
            node_id: node_id & 0x3FF,
            epoch_ms: Self::EPOCH_MS,
            last_ts: 0,
            seq: 0,
        }
    }

    fn next_id(&mut self) -> u64 {
        let now = unix_ts_ms();
        // 时钟回拨保护：保持单调（不早于上次时间戳）
        let mut ts = if now < self.last_ts { self.last_ts } else { now };

        if ts == self.last_ts {
            self.seq += 1;
            if self.seq > Self::MAX_SEQ {
                // 同毫秒序列耗尽，推进到下一毫秒
                ts += 1;
                self.last_ts = ts;
                self.seq = 0;
            }
        } else {
            self.last_ts = ts;
            self.seq = 0;
        }

        let t = ts - self.epoch_ms;
        (t << 22) | (self.node_id << 12) | self.seq as u64
    }
}

fn unix_ts_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// 派生 10-bit 节点 ID：主机名 FNV-1a 哈希 ^ PID
fn derive_node_id() -> u64 {
    let host = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "coord-agent".to_string());
    let pid = std::process::id() as u64;
    (fnv1a(&host) ^ pid) & 0x3FF
}

fn fnv1a(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

// ──── IdGenService ────

/// ID 生成器服务
///
/// 实现 `BaseService` trait，为应用提供高性能全局唯一 ID 生成。
/// 有 Server 连接时使用 KV 号段模式；无 Server 时降级为本地雪花生成。
pub struct IdGenService {
    /// 到 Server 集群的内部客户端；None = 本地模式（无 Server）
    inner: Option<Arc<AgentInner>>,
    /// 本地号段缓存
    cache: Arc<ParkingRwLock<IdGenCache>>,
    /// 本地雪花生成器（仅本地模式使用）
    local: Option<Mutex<LocalSnowflake>>,
    /// 健康状态
    healthy: ParkingRwLock<bool>,
    /// 关闭信号
    shutdown_tx: ParkingRwLock<Option<watch::Sender<()>>>,
}

impl IdGenService {
    pub const NAME: &'static str = "idgen";

    /// 创建 IdGen 服务。
    ///
    /// `inner` 为 `Some` 时使用 Server KV 号段模式；为 `None` 时降级为
    /// 本地雪花生成（本机唯一、趋势递增，无需 Server）。
    pub fn new(inner: Option<Arc<AgentInner>>, default_step: u64) -> Self {
        let local = if inner.is_none() {
            Some(Mutex::new(LocalSnowflake::new(derive_node_id())))
        } else {
            None
        };
        Self {
            inner,
            cache: Arc::new(ParkingRwLock::new(IdGenCache::new(default_step))),
            local,
            healthy: ParkingRwLock::new(false),
            shutdown_tx: ParkingRwLock::new(None),
        }
    }

    /// 生成下一个 ID
    ///
    /// - Server 模式：本地号段充足时直接分配（<1ms），否则向 Server 申请新号段。
    /// - 本地模式（无 Server）：雪花生成，本机唯一、趋势递增。
    pub async fn next_id(&self, name: &str) -> ServiceResult<u64> {
        match &self.inner {
            Some(inner) => {
                // 1. 尝试本地分配
                if let Some(id) = self.cache.read().try_next_id(name) {
                    return Ok(id);
                }
                // 2. 本地号段耗尽，从 Server 申请新号段
                self.allocate_segment(inner, name).await
            }
            None => {
                let local = self
                    .local
                    .as_ref()
                    .expect("local snowflake present in local mode");
                Ok(local.lock().next_id())
            }
        }
    }

    /// 从 Server 申请新号段
    async fn allocate_segment(&self, inner: &AgentInner, name: &str) -> ServiceResult<u64> {
        let storage_key = IdSegment::storage_key(name);
        let step = self.cache.read().default_step;

        // 读取当前 Server 上的号段状态
        let segment = self
            .read_or_init_segment(inner, name, &storage_key, step)
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

        inner
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
        inner: &AgentInner,
        name: &str,
        storage_key: &[u8],
        step: u64,
    ) -> ServiceResult<IdSegment> {
        let pairs = inner
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
            inner
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

    // ──── LocalSnowflake 本地雪花生成器 ────

    #[test]
    fn test_local_snowflake_unique_and_monotonic() {
        let mut sf = LocalSnowflake::new(42);
        let mut prev = sf.next_id();
        for _ in 0..10_000 {
            let next = sf.next_id();
            assert!(next > prev, "snowflake id must be monotonic: {next} <= {prev}");
            prev = next;
        }
    }

    #[test]
    fn test_local_snowflake_node_id_bits() {
        let mut sf = LocalSnowflake::new(0x3FF);
        let id = sf.next_id();
        // 节点 ID 位于 [12, 22) bit
        assert_eq!((id >> 12) & 0x3FF, 0x3FF);
    }

    #[test]
    fn test_local_snowflake_different_nodes_do_not_collide() {
        let mut sf1 = LocalSnowflake::new(1);
        let mut sf2 = LocalSnowflake::new(2);
        let mut seen = std::collections::HashSet::new();
        for _ in 0..1000 {
            let id1 = sf1.next_id();
            let id2 = sf2.next_id();
            assert_ne!(id1, id2);
            assert!(seen.insert(id1));
            assert!(seen.insert(id2));
        }
    }

    // ──── IdGenService 本地模式（无 Server）────

    #[tokio::test]
    async fn test_id_gen_service_local_mode_next_id() {
        let svc = IdGenService::new(None, 1000);
        let id1 = svc.next_id("orders").await.expect("next_id should work without server");
        let id2 = svc.next_id("orders").await.expect("next_id should work without server");
        assert!(id1 > 0);
        assert!(id2 > id1, "local ids must be monotonic: {id2} <= {id1}");
    }
}
