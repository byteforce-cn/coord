// coord-agent: Cache 数据面服务测试（Phase F）
//
// TDD RED phase: 测试 CacheService（基于 redb 的持久化缓存引擎）。
// 支持 String/Hash/List/Set 数据类型、TTL、分片元数据。
//
// 参见 docs/client-agent-architecture-v3.md §5.5。

use std::sync::Arc;
use tempfile::TempDir;

use coord_agent::services::cache::{
    CacheDataType, CacheEntry, CacheService, CacheShardMeta, CacheStats,
};
use coord_agent::BaseService;

// ──── helpers ────

fn temp_data_dir() -> TempDir {
    tempfile::tempdir().expect("failed to create temp dir")
}

fn new_cache_service(dir: &TempDir) -> CacheService {
    let svc = CacheService::new(dir.path().to_path_buf(), 1024 * 1024, 3600);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async { svc.start().await.expect("start should succeed") });
    svc
}

// ──── F.1: CacheService 创建与 BaseService trait ────

#[test]
fn test_cache_service_creation() {
    let dir = temp_data_dir();
    let svc = new_cache_service(&dir);
    assert_eq!(svc.name(), "cache");
    assert!(svc.health_check());
}

#[test]
fn test_cache_service_start_stop() {
    let dir = temp_data_dir();
    let svc = new_cache_service(&dir);
    // start
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        svc.start().await.expect("start should succeed");
        assert!(svc.health_check());
        svc.stop().await.expect("stop should succeed");
        // After stop, DB is closed; health check returns false
        assert!(!svc.health_check());
    });
}

// ──── F.2: String 类型操作 ────

#[test]
fn test_cache_string_put_get() {
    let dir = temp_data_dir();
    let svc = new_cache_service(&dir);
    svc.string_put("key1", b"value1".to_vec(), None).expect("put should succeed");
    let val = svc.string_get("key1").expect("get should succeed");
    assert_eq!(val, Some(b"value1".to_vec()));
}

#[test]
fn test_cache_string_get_missing() {
    let dir = temp_data_dir();
    let svc = new_cache_service(&dir);
    let val = svc.string_get("nonexistent").expect("get should succeed");
    assert_eq!(val, None);
}

#[test]
fn test_cache_string_delete() {
    let dir = temp_data_dir();
    let svc = new_cache_service(&dir);
    svc.string_put("key1", b"value1".to_vec(), None).expect("put should succeed");
    let deleted = svc.string_delete("key1").expect("delete should succeed");
    assert!(deleted);
    let val = svc.string_get("key1").expect("get should succeed");
    assert_eq!(val, None);
}

#[test]
fn test_cache_string_delete_missing() {
    let dir = temp_data_dir();
    let svc = new_cache_service(&dir);
    let deleted = svc.string_delete("nonexistent").expect("delete should succeed");
    assert!(!deleted);
}

#[test]
fn test_cache_string_overwrite() {
    let dir = temp_data_dir();
    let svc = new_cache_service(&dir);
    svc.string_put("key1", b"v1".to_vec(), None).expect("put should succeed");
    svc.string_put("key1", b"v2".to_vec(), None).expect("put should succeed");
    let val = svc.string_get("key1").expect("get should succeed");
    assert_eq!(val, Some(b"v2".to_vec()));
}

#[test]
fn test_cache_string_ttl_expiry() {
    let dir = temp_data_dir();
    let svc = CacheService::new(dir.path().to_path_buf(), 1024 * 1024, 3600);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async { svc.start().await.expect("start") });
    // TTL=1 second, then wait 2 seconds
    svc.string_put("key1", b"value1".to_vec(), Some(1)).expect("put should succeed");
    std::thread::sleep(std::time::Duration::from_secs(2));
    let val = svc.string_get("key1").expect("get should succeed");
    assert_eq!(val, None, "expired entry should return None");
}

// ──── F.3: Hash 类型操作 ────

#[test]
fn test_cache_hash_field_put_get() {
    let dir = temp_data_dir();
    let svc = new_cache_service(&dir);
    svc.hash_field_put("hash1", "field1", b"val1".to_vec(), None).expect("put should succeed");
    let val = svc.hash_field_get("hash1", "field1").expect("get should succeed");
    assert_eq!(val, Some(b"val1".to_vec()));
}

#[test]
fn test_cache_hash_get_all() {
    let dir = temp_data_dir();
    let svc = new_cache_service(&dir);
    svc.hash_field_put("hash1", "f1", b"a".to_vec(), None).expect("put should succeed");
    svc.hash_field_put("hash1", "f2", b"b".to_vec(), None).expect("put should succeed");
    let all = svc.hash_get_all("hash1").expect("get_all should succeed");
    assert_eq!(all.len(), 2);
    assert_eq!(all.get("f1").map(|v| v.as_slice()), Some(b"a".as_slice()));
    assert_eq!(all.get("f2").map(|v| v.as_slice()), Some(b"b".as_slice()));
}

#[test]
fn test_cache_hash_field_delete() {
    let dir = temp_data_dir();
    let svc = new_cache_service(&dir);
    svc.hash_field_put("hash1", "f1", b"a".to_vec(), None).expect("put should succeed");
    svc.hash_field_put("hash1", "f2", b"b".to_vec(), None).expect("put should succeed");
    let deleted = svc.hash_field_delete("hash1", "f1").expect("delete should succeed");
    assert!(deleted);
    let all = svc.hash_get_all("hash1").expect("get_all should succeed");
    assert_eq!(all.len(), 1);
    assert!(all.contains_key("f2"));
}

#[test]
fn test_cache_hash_field_count() {
    let dir = temp_data_dir();
    let svc = new_cache_service(&dir);
    assert_eq!(svc.hash_field_count("hash1").expect("count should succeed"), 0);
    svc.hash_field_put("hash1", "f1", b"a".to_vec(), None).expect("put should succeed");
    assert_eq!(svc.hash_field_count("hash1").expect("count should succeed"), 1);
}

// ──── F.4: List 类型操作 ────

#[test]
fn test_cache_list_push_pop() {
    let dir = temp_data_dir();
    let svc = new_cache_service(&dir);
    svc.list_push_left("list1", b"a".to_vec(), None).expect("push should succeed");
    svc.list_push_left("list1", b"b".to_vec(), None).expect("push should succeed");
    // LIFO: b was pushed last to left, so left pop gives b
    let val = svc.list_pop_left("list1").expect("pop should succeed");
    assert_eq!(val, Some(b"b".to_vec()));
}

#[test]
fn test_cache_list_push_right_pop_right() {
    let dir = temp_data_dir();
    let svc = new_cache_service(&dir);
    svc.list_push_right("list1", b"a".to_vec(), None).expect("push should succeed");
    svc.list_push_right("list1", b"b".to_vec(), None).expect("push should succeed");
    let val = svc.list_pop_right("list1").expect("pop should succeed");
    assert_eq!(val, Some(b"b".to_vec()));
}

#[test]
fn test_cache_list_range() {
    let dir = temp_data_dir();
    let svc = new_cache_service(&dir);
    for i in 0..5u8 {
        svc.list_push_right("list1", vec![i], None).expect("push should succeed");
    }
    let range = svc.list_range("list1", 1, 4).expect("range should succeed");
    assert_eq!(range, vec![vec![1u8], vec![2], vec![3]]);
}

#[test]
fn test_cache_list_length() {
    let dir = temp_data_dir();
    let svc = new_cache_service(&dir);
    assert_eq!(svc.list_length("list1").expect("len should succeed"), 0);
    svc.list_push_right("list1", b"a".to_vec(), None).expect("push should succeed");
    assert_eq!(svc.list_length("list1").expect("len should succeed"), 1);
}

// ──── F.5: Set 类型操作 ────

#[test]
fn test_cache_set_add_contains() {
    let dir = temp_data_dir();
    let svc = new_cache_service(&dir);
    let added = svc.set_add("set1", b"member1".to_vec(), None).expect("add should succeed");
    assert!(added);
    let added2 = svc.set_add("set1", b"member1".to_vec(), None).expect("add should succeed");
    assert!(!added2, "duplicate should return false");
    assert!(svc.set_contains("set1", b"member1").expect("contains should succeed"));
    assert!(!svc.set_contains("set1", b"member2").expect("contains should succeed"));
}

#[test]
fn test_cache_set_remove() {
    let dir = temp_data_dir();
    let svc = new_cache_service(&dir);
    svc.set_add("set1", b"m1".to_vec(), None).expect("add should succeed");
    let removed = svc.set_remove("set1", b"m1").expect("remove should succeed");
    assert!(removed);
    let removed2 = svc.set_remove("set1", b"m1").expect("remove should succeed");
    assert!(!removed2);
}

#[test]
fn test_cache_set_members() {
    let dir = temp_data_dir();
    let svc = new_cache_service(&dir);
    svc.set_add("set1", b"a".to_vec(), None).expect("add should succeed");
    svc.set_add("set1", b"b".to_vec(), None).expect("add should succeed");
    let mut members = svc.set_members("set1").expect("members should succeed");
    members.sort();
    assert_eq!(members, vec![b"a".to_vec(), b"b".to_vec()]);
}

#[test]
fn test_cache_set_cardinality() {
    let dir = temp_data_dir();
    let svc = new_cache_service(&dir);
    assert_eq!(svc.set_cardinality("set1").expect("card should succeed"), 0);
    svc.set_add("set1", b"a".to_vec(), None).expect("add should succeed");
    svc.set_add("set1", b"b".to_vec(), None).expect("add should succeed");
    assert_eq!(svc.set_cardinality("set1").expect("card should succeed"), 2);
}

// ──── F.6: 持久化恢复 ────

#[test]
fn test_cache_persistence_across_restart() {
    let dir = temp_data_dir();
    let db_path = dir.path().to_path_buf();
    let rt = tokio::runtime::Runtime::new().unwrap();

    // 写入
    {
        let svc = CacheService::new(db_path.clone(), 1024 * 1024, 3600);
        rt.block_on(async { svc.start().await.expect("start") });
        svc.string_put("persist_key", b"persist_val".to_vec(), None).expect("put should succeed");
        svc.hash_field_put("persist_hash", "f1", b"hv".to_vec(), None).expect("hput should succeed");
        svc.list_push_right("persist_list", b"lv".to_vec(), None).expect("lpush should succeed");
        svc.set_add("persist_set", b"sv".to_vec(), None).expect("sadd should succeed");
        // drop svc (closes DB)
    }

    // 重新打开
    {
        let svc = CacheService::new(db_path.clone(), 1024 * 1024, 3600);
        rt.block_on(async { svc.start().await.expect("start") });
        assert_eq!(svc.string_get("persist_key").expect("get"), Some(b"persist_val".to_vec()));
        assert_eq!(svc.hash_field_get("persist_hash", "f1").expect("hget"), Some(b"hv".to_vec()));
        assert_eq!(svc.list_range("persist_list", 0, -1).expect("lrange"), vec![b"lv".to_vec()]);
        assert!(svc.set_contains("persist_set", b"sv").expect("scontains"));
    }
}

// ──── F.7: 统计信息 ────

#[test]
fn test_cache_stats() {
    let dir = temp_data_dir();
    let svc = new_cache_service(&dir);
    let stats = svc.stats().expect("stats should succeed");
    assert_eq!(stats.string_count, 0);
    assert_eq!(stats.hash_count, 0);
    assert_eq!(stats.list_count, 0);
    assert_eq!(stats.set_count, 0);

    svc.string_put("k1", b"v1".to_vec(), None).expect("put");
    svc.hash_field_put("h1", "f1", b"v1".to_vec(), None).expect("hput");
    svc.list_push_right("l1", b"v1".to_vec(), None).expect("lpush");
    svc.set_add("s1", b"v1".to_vec(), None).expect("sadd");

    let stats = svc.stats().expect("stats should succeed");
    assert_eq!(stats.string_count, 1);
    assert_eq!(stats.hash_count, 1);
    assert_eq!(stats.list_count, 1);
    assert_eq!(stats.set_count, 1);
}

// ──── F.8: 分片元数据 ────

#[test]
fn test_cache_shard_metadata() {
    let dir = temp_data_dir();
    let svc = new_cache_service(&dir);
    svc.set_shard_meta("shard-001", CacheShardMeta {
        shard_id: "shard-001".into(),
        leader_agent: "agent-a:9500".into(),
        replicas: vec!["agent-b:9500".into(), "agent-c:9500".into()],
        key_range_start: vec![0],
        key_range_end: vec![127],
    }).expect("set shard meta should succeed");

    let meta = svc.get_shard_meta("shard-001").expect("get shard meta should succeed");
    assert!(meta.is_some());
    let meta = meta.unwrap();
    assert_eq!(meta.shard_id, "shard-001");
    assert_eq!(meta.leader_agent, "agent-a:9500");
    assert_eq!(meta.replicas.len(), 2);
}

#[test]
fn test_cache_list_shards() {
    let dir = temp_data_dir();
    let svc = new_cache_service(&dir);
    for i in 0..3 {
        svc.set_shard_meta(&format!("shard-{i:03}"), CacheShardMeta {
            shard_id: format!("shard-{i:03}"),
            leader_agent: format!("agent-{}:9500", i),
            replicas: vec![],
            key_range_start: vec![i as u8],
            key_range_end: vec![i as u8 + 1],
        }).expect("set shard meta");
    }
    let shards = svc.list_shards().expect("list shards should succeed");
    assert_eq!(shards.len(), 3);
}

// ──── F.9: 全量清空 ────

#[test]
fn test_cache_flush_all() {
    let dir = temp_data_dir();
    let svc = new_cache_service(&dir);
    svc.string_put("k1", b"v1".to_vec(), None).expect("put");
    svc.hash_field_put("h1", "f1", b"v1".to_vec(), None).expect("hput");
    svc.list_push_right("l1", b"v1".to_vec(), None).expect("lpush");
    svc.set_add("s1", b"v1".to_vec(), None).expect("sadd");

    svc.flush_all().expect("flush should succeed");

    assert_eq!(svc.string_get("k1").expect("get"), None);
    assert_eq!(svc.hash_field_get("h1", "f1").expect("hget"), None);
    assert_eq!(svc.list_length("l1").expect("llen"), 0);
    assert_eq!(svc.set_cardinality("s1").expect("scard"), 0);
}

// ──── F.10: 并发读写安全 ────

#[test]
fn test_cache_concurrent_access() {
    use std::thread;

    let dir = temp_data_dir();
    let svc = Arc::new(new_cache_service(&dir));

    let mut handles = vec![];
    for i in 0..10 {
        let svc = svc.clone();
        handles.push(thread::spawn(move || {
            let key = format!("concurrent_key_{}", i);
            svc.string_put(&key, format!("val_{}", i).into_bytes(), None)
                .expect("put should succeed");
            let val = svc.string_get(&key).expect("get should succeed");
            assert_eq!(val, Some(format!("val_{}", i).into_bytes()));
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    let stats = svc.stats().expect("stats");
    assert_eq!(stats.string_count, 10);
}
