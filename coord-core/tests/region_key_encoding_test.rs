// region_key_encoding_test.rs — Phase 0: Region Key 编码测试
//
// TDD: 测试共享存储的 Key 前缀编码方案
// 编码规范: /r/{region_id_hex}/kv/{user_key}
//          /r/{region_id_hex}/raft_log/{index_be}
//          /r/{region_id_hex}/raft_meta/{meta_key}
//          /r/{region_id_hex}/changelog/{rev_be}
//          /pd/region/{region_id_hex}

use coord_core::region::{
    encode_pd_region_key, encode_raft_log_key, encode_raft_meta_key, encode_region_changelog_key,
    encode_region_kv_key, encode_region_kv_meta_key, KEY_PREFIX_LEN, RAFT_LOG_PREFIX_LEN,
};
use coord_core::types::RegionId;

// ============================================================================
// Key 前缀格式测试
// ============================================================================

#[test]
fn test_kv_key_prefix_format() {
    // /r/{region_id:016x}/kv/{user_key}
    let key = encode_region_kv_key(1, b"hello");
    let prefix = format!("/r/{:016x}/kv/", 1u64);
    assert!(key.starts_with(prefix.as_bytes()), "key: {:?}, prefix: {:?}", key, prefix);
}

#[test]
fn test_kv_key_different_regions_different_prefixes() {
    let key1 = encode_region_kv_key(1, b"same_key");
    let key2 = encode_region_kv_key(2, b"same_key");
    // 不同 Region 的相同 user_key 应有不同的编码
    assert_ne!(key1, key2);
}

#[test]
fn test_kv_key_same_region_different_user_keys() {
    let key1 = encode_region_kv_key(1, b"key_a");
    let key2 = encode_region_kv_key(1, b"key_b");
    assert_ne!(key1, key2);
}

#[test]
fn test_kv_key_region_id_zero() {
    let key = encode_region_kv_key(0, b"data");
    let prefix = format!("/r/{:016x}/kv/", 0u64);
    assert!(key.starts_with(prefix.as_bytes()));
}

#[test]
fn test_kv_key_region_id_max() {
    let key = encode_region_kv_key(u64::MAX, b"data");
    let prefix = format!("/r/{:016x}/kv/", u64::MAX);
    assert!(key.starts_with(prefix.as_bytes()));
}

#[test]
fn test_kv_key_empty_user_key() {
    let key = encode_region_kv_key(1, b"");
    let prefix = format!("/r/{:016x}/kv/", 1u64);
    assert_eq!(key, prefix.into_bytes());
}

#[test]
fn test_kv_key_binary_user_key() {
    // user_key 可以是任意字节，包括 0x00
    let user_key = vec![0x00, 0xFF, 0x12, 0xAB];
    let key = encode_region_kv_key(1, &user_key);
    let prefix = format!("/r/{:016x}/kv/", 1u64);
    assert!(key.starts_with(prefix.as_bytes()));
    // 验证 user_key 部分完整保留
    let suffix = &key[prefix.len()..];
    assert_eq!(suffix, user_key.as_slice());
}

// ============================================================================
// KV Meta Key 编码测试
// ============================================================================

#[test]
fn test_kv_meta_key_format() {
    let key = encode_region_kv_meta_key(1, b"mykey");
    assert!(key.starts_with(format!("/r/{:016x}/kv_meta/", 1u64).as_bytes()));
}

#[test]
fn test_kv_meta_key_different_from_kv_key() {
    let kv_key = encode_region_kv_key(1, b"x");
    let meta_key = encode_region_kv_meta_key(1, b"x");
    assert_ne!(kv_key, meta_key);
}

// ============================================================================
// Raft Log Key 编码测试
// ============================================================================

#[test]
fn test_raft_log_key_format() {
    // /r/{region_id:016x}/raft_log/{index:016x}
    let key = encode_raft_log_key(1, 42);
    let expected_prefix = format!("/r/{:016x}/raft_log/", 1u64);
    assert!(key.starts_with(expected_prefix.as_bytes()));
    // 后 16 字节是 index 的大端编码
    assert_eq!(key.len(), RAFT_LOG_PREFIX_LEN + 16);
}

#[test]
fn test_raft_log_key_ordering() {
    // Raft Log Key 必须按 index 有序，以便范围扫描
    let key1 = encode_raft_log_key(1, 1);
    let key2 = encode_raft_log_key(1, 2);
    let key100 = encode_raft_log_key(1, 100);
    assert!(key1 < key2);
    assert!(key2 < key100);
}

#[test]
fn test_raft_log_key_different_regions() {
    let key_r1 = encode_raft_log_key(1, 42);
    let key_r2 = encode_raft_log_key(2, 42);
    assert_ne!(key_r1, key_r2);
}

#[test]
fn test_raft_log_key_index_zero() {
    let key = encode_raft_log_key(1, 0);
    assert!(!key.is_empty());
}

#[test]
fn test_raft_log_key_index_max() {
    let key = encode_raft_log_key(1, u64::MAX);
    assert_eq!(key.len(), RAFT_LOG_PREFIX_LEN + 16);
}

// ============================================================================
// Raft Meta Key 编码测试
// ============================================================================

#[test]
fn test_raft_meta_key_vote() {
    let key = encode_raft_meta_key(1, b"vote");
    assert!(key.starts_with(format!("/r/{:016x}/raft_meta/", 1u64).as_bytes()));
}

#[test]
fn test_raft_meta_key_committed() {
    let key = encode_raft_meta_key(1, b"committed");
    assert!(key.starts_with(format!("/r/{:016x}/raft_meta/", 1u64).as_bytes()));
}

#[test]
fn test_raft_meta_key_last_purged() {
    let key = encode_raft_meta_key(1, b"last_purged");
    assert!(key.starts_with(format!("/r/{:016x}/raft_meta/", 1u64).as_bytes()));
}

// ============================================================================
// Changelog Key 编码测试
// ============================================================================

#[test]
fn test_changelog_key_format() {
    // /r/{region_id:016x}/changelog/{revision:016x}
    let key = encode_region_changelog_key(1, 1);
    let expected_prefix = format!("/r/{:016x}/changelog/", 1u64);
    assert!(key.starts_with(expected_prefix.as_bytes()));
}

#[test]
fn test_changelog_key_ordering() {
    let k1 = encode_region_changelog_key(1, 1);
    let k2 = encode_region_changelog_key(1, 10);
    let k100 = encode_region_changelog_key(1, 100);
    assert!(k1 < k2);
    assert!(k2 < k100);
}

// ============================================================================
// PD Meta Key 编码测试
// ============================================================================

#[test]
fn test_pd_region_key_format() {
    let key = encode_pd_region_key(1);
    let expected = format!("/pd/region/{:016x}", 1u64);
    assert_eq!(key, expected.as_bytes());
}

#[test]
fn test_pd_region_key_unique_per_region() {
    let k1 = encode_pd_region_key(1);
    let k2 = encode_pd_region_key(2);
    assert_ne!(k1, k2);
}

// ============================================================================
// Key 前缀扫描测试（验证前缀可正确前缀扫描）
// ============================================================================

#[test]
fn test_kv_key_prefix_scannable() {
    // 同一 Region 的所有 KV key 共享相同前缀，可进行前缀扫描
    let r = 1u64;
    let keys: Vec<_> = (0..10)
        .map(|i| encode_region_kv_key(r, format!("key_{}", i).as_bytes()))
        .collect();

    let prefix = format!("/r/{:016x}/kv/", r);
    for key in &keys {
        assert!(
            key.starts_with(prefix.as_bytes()),
            "key {:?} should start with {}",
            key,
            prefix
        );
    }
}

#[test]
fn test_raft_log_key_prefix_scannable() {
    let r = 1u64;
    let keys: Vec<_> = (0..10).map(|i| encode_raft_log_key(r, i)).collect();

    let prefix = format!("/r/{:016x}/raft_log/", r);
    for key in &keys {
        assert!(key.starts_with(prefix.as_bytes()));
    }
}

// ============================================================================
// 多 Region 隔离测试
// ============================================================================

#[test]
fn test_multi_region_key_isolation() {
    // 不同 Region 的 key 不应重叠
    let r1_keys: Vec<_> = (0..5)
        .map(|i| encode_region_kv_key(1, &[i as u8]))
        .collect();
    let r2_keys: Vec<_> = (0..5)
        .map(|i| encode_region_kv_key(2, &[i as u8]))
        .collect();

    // 确保 Region 1 和 Region 2 的 key 不重叠
    for k1 in &r1_keys {
        for k2 in &r2_keys {
            assert_ne!(k1, k2);
            // 前缀不同：/r/0000000000000001/ vs /r/0000000000000002/
        }
    }
}

#[test]
fn test_region_prefix_extraction() {
    // 从编码后的 key 中应能提取出 region_id
    // 这个功能在路由/拆分时有用
    let key = encode_region_kv_key(42, b"test");
    let prefix = "/r/000000000000002a/kv/";
    assert!(key.starts_with(prefix.as_bytes()));
}

// ============================================================================
// 边界条件测试
// ============================================================================

#[test]
fn test_very_long_user_key() {
    let long_key = vec![b'x'; 10_000];
    let encoded = encode_region_kv_key(1, &long_key);
    assert!(encoded.len() > long_key.len());
}

#[test]
fn test_single_byte_user_key() {
    let encoded = encode_region_kv_key(1, b"a");
    assert!(!encoded.is_empty());
}

#[test]
fn test_region_id_hex_consistency() {
    // 确保 region_id 在所有编码函数中使用相同的 16 位十六进制格式
    let region_id: RegionId = 255;
    let hex_str = format!("{:016x}", region_id);
    assert_eq!(hex_str, "00000000000000ff");

    let kv_key = encode_region_kv_key(region_id, b"x");
    let log_key = encode_raft_log_key(region_id, 1);

    // 所有 key 的前缀部分应包含相同的 region_id 十六进制表示
    let kv_prefix = std::str::from_utf8(&kv_key[..KEY_PREFIX_LEN]).unwrap();
    let log_prefix = std::str::from_utf8(&log_key[..KEY_PREFIX_LEN]).unwrap();
    assert!(kv_prefix.contains("00000000000000ff"));
    assert!(log_prefix.contains("00000000000000ff"));
}
