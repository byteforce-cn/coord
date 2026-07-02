// TDD: Moka 缓存后端测试 (Phase D-moka — RED→GREEN)
//
// v8.2 §4.7: "moka（可选）纯内存缓存"

use std::time::Duration;

use coord_agent::services::cache::{CacheBackend, CacheConfig, MokaCacheService};

#[test]
fn test_moka_backend_creation() {
    let backend = CacheBackend::Moka {
        max_capacity: 1000,
        time_to_live: Some(Duration::from_secs(60)),
    };
    assert!(matches!(backend, CacheBackend::Moka { .. }));
}

#[test]
fn test_moka_string_set_get() {
    let config = CacheConfig {
        backend: CacheBackend::Moka { max_capacity: 100, time_to_live: None },
    };
    let svc = MokaCacheService::new(config);
    svc.string_set("greeting", b"hello").unwrap();
    let val = svc.string_get("greeting").unwrap();
    assert_eq!(val, Some(b"hello".to_vec()));
    assert_eq!(svc.string_get("missing").unwrap(), None);
}

#[test]
fn test_moka_string_delete() {
    let config = CacheConfig {
        backend: CacheBackend::Moka { max_capacity: 100, time_to_live: None },
    };
    let svc = MokaCacheService::new(config);
    svc.string_set("temp", b"val").unwrap();
    assert!(svc.string_exists("temp").unwrap());
    svc.string_delete("temp").unwrap();
    assert!(!svc.string_exists("temp").unwrap());
}

#[test]
fn test_moka_hash_operations() {
    let config = CacheConfig {
        backend: CacheBackend::Moka { max_capacity: 1000, time_to_live: None },
    };
    let svc = MokaCacheService::new(config);
    svc.hash_field_set("user:1", "name", b"Alice").unwrap();
    svc.hash_field_set("user:1", "age", b"30").unwrap();
    assert_eq!(svc.hash_field_get("user:1", "name").unwrap(), Some(b"Alice".to_vec()));
    assert_eq!(svc.hash_get_all("user:1").unwrap().len(), 2);
    svc.hash_field_delete("user:1", "age").unwrap();
    assert_eq!(svc.hash_get_all("user:1").unwrap().len(), 1);
}

#[test]
fn test_moka_list_push_pop() {
    let config = CacheConfig {
        backend: CacheBackend::Moka { max_capacity: 100, time_to_live: None },
    };
    let svc = MokaCacheService::new(config);
    svc.list_push_left("q", b"a".to_vec()).unwrap();
    svc.list_push_right("q", b"b".to_vec()).unwrap();
    assert_eq!(svc.list_len("q").unwrap(), 2);
    assert_eq!(svc.list_pop_left("q").unwrap(), Some(b"a".to_vec()));
    assert_eq!(svc.list_pop_right("q").unwrap(), Some(b"b".to_vec()));
}

#[test]
fn test_moka_set_operations() {
    let config = CacheConfig {
        backend: CacheBackend::Moka { max_capacity: 1000, time_to_live: None },
    };
    let svc = MokaCacheService::new(config);
    svc.set_add("tags", b"rust").unwrap();
    svc.set_add("tags", b"go").unwrap();
    assert!(svc.set_contains("tags", b"rust").unwrap());
    assert!(!svc.set_contains("tags", b"java").unwrap());
    assert_eq!(svc.set_members("tags").unwrap().len(), 2);
    svc.set_remove("tags", b"go").unwrap();
    assert_eq!(svc.set_members("tags").unwrap().len(), 1);
}

#[test]
fn test_moka_cache_stats() {
    let config = CacheConfig {
        backend: CacheBackend::Moka { max_capacity: 100, time_to_live: None },
    };
    let svc = MokaCacheService::new(config);
    svc.string_set("k1", b"v1").unwrap();
    let stats = svc.stats();
    assert!(stats.string_count >= 1);
}
