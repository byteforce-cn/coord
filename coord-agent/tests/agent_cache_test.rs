// TDD: Agent 本地缓存测试 (Phase C1/C2)
//
// C1: KV 读缓存验证 — LruCache + TTL + Watch 失效
// C2: Service Catalog 缓存验证 — 旧 RegistryCache（已废弃） + 新 services::registry::RegistryCache
//
// v3.0 迁移说明：
// - 旧 `cache::RegistryCache` 已废弃，测试保留向后兼容
// - 新 `services::registry::RegistryCache` 为 v3.0 可插拔服务架构的实现

#![allow(deprecated)]

use std::time::Duration;

use coord_agent::cache::KvCache;
use coord_agent::cache::RegistryCache;
// v3.0 新架构
use coord_agent::services::registry::{RegistryCache as RegistryCacheV3, ServiceInstance};

// ════════════════════════════════════════════════════════════════
// C1: KV 读缓存
// ════════════════════════════════════════════════════════════════

/// C1.1: 缓存基本 put/get 操作
#[test]
fn test_kv_cache_put_and_get() {
    let mut cache = KvCache::new(100, 30);
    assert_eq!(cache.len(), 0);

    cache.put(b"key1".to_vec(), b"value1".to_vec());
    assert_eq!(cache.len(), 1);

    let val = cache.get(b"key1");
    assert_eq!(val, Some(b"value1".to_vec()));

    // 不存在的 key 返回 None
    let val = cache.get(b"nonexistent");
    assert_eq!(val, None);
}

/// C1.2: 缓存 hit/miss 统计
#[test]
fn test_kv_cache_stats() {
    let mut cache = KvCache::new(100, 30);

    // 初始状态
    let stats = cache.stats();
    assert_eq!(stats.hits, 0);
    assert_eq!(stats.misses, 0);

    // Miss
    let _ = cache.get(b"key1");
    let stats = cache.stats();
    assert_eq!(stats.hits, 0);
    assert_eq!(stats.misses, 1);

    // Hit
    cache.put(b"key1".to_vec(), b"val".to_vec());
    let _ = cache.get(b"key1");
    let stats = cache.stats();
    assert_eq!(stats.hits, 1);
    assert_eq!(stats.misses, 1);
}

/// C1.3: TTL 过期
#[test]
fn test_kv_cache_ttl_expiry() {
    // 使用 1 秒 TTL
    let mut cache = KvCache::new(100, 1);
    cache.put(b"key1".to_vec(), b"value1".to_vec());

    // 立即读取应命中
    assert_eq!(cache.get(b"key1"), Some(b"value1".to_vec()));

    // 等待 TTL 过期
    std::thread::sleep(Duration::from_secs(2));

    // 过期后应 miss
    assert_eq!(cache.get(b"key1"), None);
}

/// C1.4: LRU 淘汰
#[test]
fn test_kv_cache_lru_eviction() {
    // 最大 2 条
    let mut cache = KvCache::new(2, 60);

    cache.put(b"k1".to_vec(), b"v1".to_vec());
    cache.put(b"k2".to_vec(), b"v2".to_vec());
    assert_eq!(cache.len(), 2);

    // 插入第 3 条，应淘汰最久未使用的 k1
    cache.put(b"k3".to_vec(), b"v3".to_vec());
    assert_eq!(cache.len(), 2);
    assert_eq!(cache.get(b"k1"), None);
    assert_eq!(cache.get(b"k2"), Some(b"v2".to_vec()));
    assert_eq!(cache.get(b"k3"), Some(b"v3".to_vec()));
}

/// C1.5: 缓存主动失效（Watch 驱动）
#[test]
fn test_kv_cache_invalidation() {
    let mut cache = KvCache::new(100, 60);

    cache.put(b"app/config".to_vec(), b"v1".to_vec());
    cache.put(b"app/timeout".to_vec(), b"v2".to_vec());
    assert_eq!(cache.len(), 2);

    // Watch 通知 key 变更，主动失效
    cache.invalidate(b"app/config");
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.get(b"app/config"), None);
    assert_eq!(cache.get(b"app/timeout"), Some(b"v2".to_vec()));

    // 失效不存在的 key 不 panic
    cache.invalidate(b"nonexistent");
    assert_eq!(cache.len(), 1);
}

/// C1.6: 前缀失效（Watch prefix 事件）
#[test]
fn test_kv_cache_prefix_invalidation() {
    let mut cache = KvCache::new(100, 60);

    cache.put(b"/_registry/services/a".to_vec(), b"va".to_vec());
    cache.put(b"/_registry/services/b".to_vec(), b"vb".to_vec());
    cache.put(b"/app/config".to_vec(), b"vc".to_vec());
    assert_eq!(cache.len(), 3);

    // 前缀失效：失效所有 /_registry/ 开头的 key
    cache.invalidate_prefix(b"/_registry/");
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.get(b"/app/config"), Some(b"vc".to_vec()));
}

// ════════════════════════════════════════════════════════════════
// C2: Service Catalog 缓存（旧 RegistryCache，向后兼容）
// ════════════════════════════════════════════════════════════════

/// C2.1: [已废弃] RegistryCache 基本操作
#[test]
#[allow(deprecated)]
fn test_registry_cache_basic() {
    let mut cache = RegistryCache::new(500, 10);

    // 初始为空
    assert!(cache.is_empty());

    // 全量加载
    let entries = vec![
        (b"/_registry/services/a".to_vec(), b"{}".to_vec()),
        (b"/_registry/services/b".to_vec(), b"{}".to_vec()),
    ];
    cache.load_full(entries);
    assert!(!cache.is_empty());
    assert_eq!(cache.len(), 2);

    // 查询
    let result = cache.get(b"/_registry/services/a");
    assert_eq!(result, Some(b"{}".to_vec()));
}

/// C2.2: [已废弃] RegistryCache 增量更新
#[test]
#[allow(deprecated)]
fn test_registry_cache_incremental() {
    let mut cache = RegistryCache::new(500, 10);

    cache.load_full(vec![
        (b"/_registry/svc/a".to_vec(), b"v1".to_vec()),
    ]);
    assert_eq!(cache.len(), 1);

    // Put 事件：新增
    cache.apply_event(b"/_registry/svc/b", Some(b"v2".to_vec()));
    assert_eq!(cache.len(), 2);

    // Delete 事件：移除
    cache.apply_event(b"/_registry/svc/a", None);
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.get(b"/_registry/svc/a"), None);
}

// ════════════════════════════════════════════════════════════════
// C2v3: Service Catalog 缓存（新 services::registry::RegistryCache，v3.0）
// ════════════════════════════════════════════════════════════════

/// C2v3.1: 新 RegistryCache 类型化实例存储
#[test]
fn test_registry_cache_v3_typed_instances() {
    let mut cache = RegistryCacheV3::new(500);

    let inst = ServiceInstance::new(
        "order-service",
        "node1:8080",
        "10.0.0.1:8080",
        br#"{"zone":"us-east-1"}"#.to_vec(),
        1001,
    );
    let key = RegistryCacheV3::storage_key("order-service", "node1:8080");
    let value = serde_json::to_vec(&inst).unwrap();
    cache.apply_event(&key, Some(&value));

    assert_eq!(cache.len(), 1);
    let discovered = cache.discover("order-service");
    assert_eq!(discovered.len(), 1);
    assert_eq!(discovered[0].address, "10.0.0.1:8080");
}

/// C2v3.2: 新 RegistryCache 按服务名发现
#[test]
fn test_registry_cache_v3_discover_by_service() {
    let mut cache = RegistryCacheV3::new(500);

    let inst_a1 = ServiceInstance::new("svc-a", "i1", "addr1", vec![], 0);
    let inst_a2 = ServiceInstance::new("svc-a", "i2", "addr2", vec![], 0);
    let inst_b1 = ServiceInstance::new("svc-b", "i1", "addr3", vec![], 0);

    for inst in [&inst_a1, &inst_a2, &inst_b1] {
        let key = RegistryCacheV3::storage_key(&inst.service_name, &inst.instance_id);
        let value = serde_json::to_vec(inst).unwrap();
        cache.apply_event(&key, Some(&value));
    }

    assert_eq!(cache.discover("svc-a").len(), 2);
    assert_eq!(cache.discover("svc-b").len(), 1);
    assert_eq!(cache.discover("nonexistent").len(), 0);
}

/// C2v3.3: 新 RegistryCache 自我保护模式
#[test]
fn test_registry_cache_v3_self_protection() {
    let mut cache = RegistryCacheV3::new(10);
    assert!(!cache.is_self_protection());

    cache.enter_self_protection();
    assert!(cache.is_self_protection());

    // load_full 应退出自我保护
    let inst = ServiceInstance::new("svc", "i1", "addr", vec![], 0);
    cache.load_full(vec![inst]);
    assert!(!cache.is_self_protection());
}

/// C2v3.4: 新 RegistryCache discover_all
#[test]
fn test_registry_cache_v3_discover_all() {
    let mut cache = RegistryCacheV3::new(500);

    let inst1 = ServiceInstance::new("svc-a", "i1", "addr1", vec![], 0);
    let inst2 = ServiceInstance::new("svc-b", "i1", "addr2", vec![], 0);

    for inst in [&inst1, &inst2] {
        let key = RegistryCacheV3::storage_key(&inst.service_name, &inst.instance_id);
        let value = serde_json::to_vec(inst).unwrap();
        cache.apply_event(&key, Some(&value));
    }

    let all = cache.discover_all();
    assert_eq!(all.len(), 2);
    assert_eq!(all.get("svc-a").unwrap().len(), 1);
}
