// coord-agent: 本地缓存层 (Cache Layer)
//
// 实现本地缓存：
// - KvCache: KV 读缓存（LruCache + TTL），用于串行化 Range 读
//
// 已废弃：
// - RegistryCache: 已迁移到 services::registry::RegistryCache（v3.0 架构）
//   旧类型保留为向后兼容别名，新代码请使用 services::registry::RegistryCache
//
// Watch 驱动的缓存失效通过 invalidate()/invalidate_prefix() 触发。
//
// 参见 docs/client-agent-architecture-v3.md §5.1。

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use lru::LruCache;
use parking_lot::Mutex;

// ──── CacheStats ────

/// 缓存统计信息
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    /// 缓存命中次数
    pub hits: u64,
    /// 缓存未命中次数
    pub misses: u64,
    /// 当前缓存条目数
    pub len: usize,
    /// 最大容量
    pub cap: usize,
}

// ──── AgentCache ────

/// Agent 本地缓存集合
///
/// 所有代理服务共享同一个 AgentCache 实例。
/// 使用 parking_lot::Mutex 包装以获得低竞争开销。
///
/// # 废弃通知
///
/// `registry` 字段已废弃（v3.0）。服务注册发现缓存已迁移到
/// `services::registry::RegistryCache`，由 `RegistryService` 管理。
/// 此字段保留仅为向后兼容，新代码请使用 RegistryService。
pub struct AgentCache {
    /// KV 读缓存（串行化 Range 读）
    pub kv: Mutex<KvCache>,
    /// [已废弃] Service Catalog 缓存 — 请使用 `services::registry::RegistryService` 替代
    #[allow(deprecated)]
    pub registry: Mutex<RegistryCache>,
}

impl AgentCache {
    /// 创建 AgentCache
    #[allow(deprecated)]
    pub fn new(kv_max_entries: usize, kv_ttl_secs: u64, registry_max_entries: usize, registry_ttl_secs: u64) -> Self {
        Self {
            kv: Mutex::new(KvCache::new(kv_max_entries, kv_ttl_secs)),
            registry: Mutex::new(RegistryCache::new(registry_max_entries, registry_ttl_secs)),
        }
    }
}

// ──── KvCache ────

/// KV 读缓存条目
struct KvEntry {
    value: Vec<u8>,
    expires_at: Instant,
}

/// KV 读缓存
///
/// 基于 LruCache，带 TTL 过期和原子统计。
/// 仅缓存串行化读结果；线性一致性读绕过缓存直连 Leader。
pub struct KvCache {
    inner: LruCache<Vec<u8>, KvEntry>,
    ttl: Duration,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl KvCache {
    /// 创建 KV 读缓存
    ///
    /// - `max_entries`: 最大缓存条目数
    /// - `ttl_secs`: 缓存 TTL（秒）
    pub fn new(max_entries: usize, ttl_secs: u64) -> Self {
        let cap = NonZeroUsize::new(max_entries.max(1)).unwrap();
        Self {
            inner: LruCache::new(cap),
            ttl: Duration::from_secs(ttl_secs),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// 读取缓存
    ///
    /// 返回 Some(value) 若命中且未过期，否则返回 None。
    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        match self.inner.get(key) {
            Some(entry) if entry.expires_at > Instant::now() => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(entry.value.clone())
            }
            Some(_) => {
                // TTL 过期，移除
                self.inner.pop(key);
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// 写入缓存
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let entry = KvEntry {
            value,
            expires_at: Instant::now() + self.ttl,
        };
        self.inner.put(key, entry);
    }

    /// 主动失效单个 key（Watch 驱动）
    pub fn invalidate(&mut self, key: &[u8]) {
        self.inner.pop(key);
    }

    /// 前缀失效：移除所有以 prefix 开头的 key（Watch 驱动）
    pub fn invalidate_prefix(&mut self, prefix: &[u8]) {
        // 收集需要失效的 key
        let keys_to_remove: Vec<Vec<u8>> = self
            .inner
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, _)| k.clone())
            .collect();
        for key in keys_to_remove {
            self.inner.pop(&key);
        }
    }

    /// 清空全部缓存（用于 Lease Revoke 等批量失效场景）
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    /// 当前缓存条目数
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// 缓存是否为空
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// 获取统计信息
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            len: self.inner.len(),
            cap: self.inner.cap().get(),
        }
    }
}

// ──── [已废弃] RegistryCache ────
//
// 此类型已迁移到 `services::registry::RegistryCache`（v3.0 可插拔服务架构）。
// 保留此定义仅为向后兼容，新代码请使用 `coord_agent::services::registry::RegistryCache`。

/// [已废弃] Service Catalog 缓存条目
struct RegistryEntry {
    value: Vec<u8>,
    _loaded_at: Instant,
}

/// [已废弃] Service Catalog 缓存
///
/// 缓存 `/_registry/` 前缀下的服务注册信息。
/// 已迁移到 `services::registry::RegistryCache`（v3.0），该版本支持：
/// - 类型化 `ServiceInstance` 存储
/// - 自我保护模式
/// - 按服务名发现
#[deprecated(since = "0.2.0", note = "请使用 services::registry::RegistryCache 替代")]
pub struct RegistryCache {
    inner: LruCache<Vec<u8>, RegistryEntry>,
}

#[allow(deprecated)]
impl RegistryCache {
    /// 创建 Registry 缓存
    ///
    /// - `max_entries`: 最大缓存条目数（默认 500）
    /// - `_ttl_secs`: 缓存 TTL（秒，目前为预留，Watch 驱动更新为主要失效方式）
    pub fn new(max_entries: usize, _ttl_secs: u64) -> Self {
        let cap = NonZeroUsize::new(max_entries.max(1)).unwrap();
        Self {
            inner: LruCache::new(cap),
        }
    }

    /// 全量加载（如首次连接时从 Server 拉取 /_registry/ 前缀全量数据）
    pub fn load_full(&mut self, entries: Vec<(Vec<u8>, Vec<u8>)>) {
        for (key, value) in entries {
            self.inner.put(
                key,
                RegistryEntry {
                    value,
                    _loaded_at: Instant::now(),
                },
            );
        }
    }

    /// 应用单个 Watch 事件（增量更新）
    ///
    /// - `value = Some(data)`: Put 事件，新增或更新
    /// - `value = None`: Delete 事件，移除
    pub fn apply_event(&mut self, key: &[u8], value: Option<Vec<u8>>) {
        match value {
            Some(data) => {
                self.inner.put(
                    key.to_vec(),
                    RegistryEntry {
                        value: data,
                        _loaded_at: Instant::now(),
                    },
                );
            }
            None => {
                self.inner.pop(key);
            }
        }
    }

    /// 查询缓存
    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.inner.get(key).map(|e| e.value.clone())
    }

    /// 当前缓存条目数
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// 缓存是否为空
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kv_cache_lru_eviction_order() {
        let mut cache = KvCache::new(3, 60);
        cache.put(b"a".to_vec(), b"1".to_vec());
        cache.put(b"b".to_vec(), b"2".to_vec());
        cache.put(b"c".to_vec(), b"3".to_vec());

        // 访问 a 使其成为最近使用
        let _ = cache.get(b"a");

        // 插入 d，应淘汰 b（最久未使用）
        cache.put(b"d".to_vec(), b"4".to_vec());
        assert_eq!(cache.len(), 3);
        assert!(cache.get(b"a").is_some());
        assert!(cache.get(b"b").is_none()); // 被淘汰
        assert!(cache.get(b"c").is_some());
    }

    #[test]
    fn test_registry_cache_empty_operations() {
        #[allow(deprecated)]
        let mut cache = RegistryCache::new(10, 10);
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.get(b"/_registry/any"), None);

        // apply_event on empty: Put
        cache.apply_event(b"/_registry/svc/x", Some(b"data".to_vec()));
        assert!(!cache.is_empty());
        assert_eq!(cache.len(), 1);

        // Delete
        cache.apply_event(b"/_registry/svc/x", None);
        assert!(cache.is_empty());
    }
}
