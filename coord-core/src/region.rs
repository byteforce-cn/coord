// Region Key 编码 — 共享存储的多 Region 前缀隔离
//
// 编码规范（§5.1）：
//   /r/{region_id:016x}/kv/{user_key}          — KV 数据
//   /r/{region_id:016x}/kv_meta/{user_key}     — KV 元数据
//   /r/{region_id:016x}/raft_log/{index:016x}  — Raft Log Entry
//   /r/{region_id:016x}/raft_meta/{meta_key}   — Raft 元数据 (vote, committed, last_purged)
//   /r/{region_id:016x}/changelog/{rev:016x}   — 变更日志
//   /pd/region/{region_id:016x}                — PD Region 元数据
//
// 所有 Region 共享同一物理存储（redb Database），通过 Key 前缀实现逻辑隔离。
// 同一 Region 内的 Key 按类型 + 后缀有序排列，支持高效前缀扫描。

use crate::types::RegionId;

/// Key 前缀固定长度：`/r/{region_id:016x}/` = 20 字节
pub const KEY_PREFIX_LEN: usize = 20;

/// KV Key 前缀长度：`/r/{region_id:016x}/kv/` = 23 字节
pub const KV_PREFIX_LEN: usize = 23;

/// Raft Log Key 前缀长度：`/r/{region_id:016x}/raft_log/` = 29 字节
pub const RAFT_LOG_PREFIX_LEN: usize = 29;

// ============================================================================
// 内部辅助函数
// ============================================================================

/// 格式化 region_id 为 16 位十六进制字符串（小写，零填充）
#[inline]
fn fmt_region_hex(region_id: RegionId) -> String {
    format!("{:016x}", region_id)
}

// ============================================================================
// 公开 Key 编码函数
// ============================================================================

/// 编码 Region KV 数据 Key
///
/// 格式: `/r/{region_id:016x}/kv/{user_key}`
///
/// # Examples
/// ```
/// use coord_core::region::encode_region_kv_key;
/// let key = encode_region_kv_key(1, b"hello");
/// assert!(key.starts_with(b"/r/0000000000000001/kv/"));
/// ```
pub fn encode_region_kv_key(region_id: RegionId, user_key: &[u8]) -> Vec<u8> {
    let prefix = format!("/r/{}/kv/", fmt_region_hex(region_id));
    let mut key = Vec::with_capacity(prefix.len() + user_key.len());
    key.extend_from_slice(prefix.as_bytes());
    key.extend_from_slice(user_key);
    key
}

/// 编码 Region KV 元数据 Key
///
/// 格式: `/r/{region_id:016x}/kv_meta/{user_key}`
pub fn encode_region_kv_meta_key(region_id: RegionId, user_key: &[u8]) -> Vec<u8> {
    let prefix = format!("/r/{}/kv_meta/", fmt_region_hex(region_id));
    let mut key = Vec::with_capacity(prefix.len() + user_key.len());
    key.extend_from_slice(prefix.as_bytes());
    key.extend_from_slice(user_key);
    key
}

/// 编码 Region Raft Log Key
///
/// 格式: `/r/{region_id:016x}/raft_log/{index:016x}` — 固定 48 字节
///
/// 使用 16 字符十六进制 index 而非大端字节，保证字典序与数值序一致。
pub fn encode_raft_log_key(region_id: RegionId, index: u64) -> Vec<u8> {
    let prefix = format!("/r/{}/raft_log/", fmt_region_hex(region_id));
    let formatted = format!("{}{:016x}", prefix, index);
    formatted.into_bytes()
}

/// 编码 Region Raft 元数据 Key
///
/// 格式: `/r/{region_id:016x}/raft_meta/{meta_key}`
pub fn encode_raft_meta_key(region_id: RegionId, meta_key: &[u8]) -> Vec<u8> {
    let prefix = format!("/r/{}/raft_meta/", fmt_region_hex(region_id));
    let mut key = Vec::with_capacity(prefix.len() + meta_key.len());
    key.extend_from_slice(prefix.as_bytes());
    key.extend_from_slice(meta_key);
    key
}

/// 编码 Region Changelog Key
///
/// 格式: `/r/{region_id:016x}/changelog/{revision:016x}`
pub fn encode_region_changelog_key(region_id: RegionId, revision: u64) -> Vec<u8> {
    let prefix = format!("/r/{}/changelog/", fmt_region_hex(region_id));
    let formatted = format!("{}{:016x}", prefix, revision);
    formatted.into_bytes()
}

/// 编码 PD Region 元数据 Key
///
/// 格式: `/pd/region/{region_id:016x}`
pub fn encode_pd_region_key(region_id: RegionId) -> Vec<u8> {
    format!("/pd/region/{}", fmt_region_hex(region_id)).into_bytes()
}

/// 编码 PD Node 元数据 Key
///
/// 格式: `/pd/node/{node_id:016x}`
pub fn encode_pd_node_key(node_id: u64) -> Vec<u8> {
    format!("/pd/node/{:016x}", node_id).into_bytes()
}

// ============================================================================
// Key 解码函数
// ============================================================================

/// 从 KV Key 中提取 user_key（去掉 Region 前缀）
///
/// 返回 `Some(user_key)` 如果 key 格式正确，否则 `None`。
pub fn decode_user_key_from_kv_key(key: &[u8]) -> Option<&[u8]> {
    // key 必须至少包含 "/r/{16x}/kv/" 前缀
    if key.len() <= KV_PREFIX_LEN {
        return None;
    }
    // 格式: /r/region_hex/kv/user_key
    // 找到第 4 个 '/' 之后的内容（即 user_key）
    let mut slash_count = 0u8;
    let mut kv_start = 0usize;
    for (i, &b) in key.iter().enumerate() {
        if b == b'/' {
            slash_count += 1;
            if slash_count == 4 {
                kv_start = i + 1;
                break;
            }
        }
    }
    if kv_start > 0 && kv_start < key.len() {
        Some(&key[kv_start..])
    } else {
        None
    }
}

/// 从编码后的 key 前缀中提取 region_id
///
/// 格式要求: `/r/{16 位 hex}`
pub fn decode_region_id_from_key(key: &[u8]) -> Option<RegionId> {
    if key.len() < 20 || &key[..3] != b"/r/" {
        return None;
    }
    let hex_str = std::str::from_utf8(&key[3..19]).ok()?;
    u64::from_str_radix(hex_str, 16).ok()
}

// ============================================================================
// 测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_roundtrip() {
        let region_id = 42;
        let user_key = b"my_test_key";
        let encoded = encode_region_kv_key(region_id, user_key);
        let decoded = decode_user_key_from_kv_key(&encoded);
        assert_eq!(decoded, Some(user_key.as_slice()));
    }

    #[test]
    fn test_decode_region_id() {
        let key = encode_region_kv_key(0xABCD, b"x");
        let rid = decode_region_id_from_key(&key);
        assert_eq!(rid, Some(0xABCD));
    }

    #[test]
    fn test_decode_invalid_key() {
        assert_eq!(decode_user_key_from_kv_key(b"not_a_valid_key"), None);
        assert_eq!(decode_region_id_from_key(b"invalid"), None);
    }

    #[test]
    fn test_raft_log_key_lexicographic_order() {
        // 十六进制编码保证字典序与数值序一致
        let k1 = encode_raft_log_key(1, 1);
        let k2 = encode_raft_log_key(1, 2);
        let k10 = encode_raft_log_key(1, 10);
        let k100 = encode_raft_log_key(1, 100);
        assert!(k1 < k2);
        assert!(k2 < k10);
        assert!(k10 < k100);
    }

    #[test]
    fn test_pd_node_key_format() {
        let key = encode_pd_node_key(1);
        assert_eq!(key, b"/pd/node/0000000000000001");
    }

    #[test]
    fn test_region_prefix_lengths() {
        // 确保常量与实际编码一致
        let region_id = 1u64;
        let kv_key = encode_region_kv_key(region_id, b"x");
        let kv_prefix = format!("/r/{:016x}/kv/", region_id);
        assert_eq!(kv_prefix.len(), KV_PREFIX_LEN);
        assert!(kv_key.starts_with(kv_prefix.as_bytes()));

        let raft_log_key = encode_raft_log_key(region_id, 0);
        let raft_log_prefix = format!("/r/{:016x}/raft_log/", region_id);
        assert_eq!(raft_log_prefix.len(), RAFT_LOG_PREFIX_LEN);
        assert!(raft_log_key.starts_with(raft_log_prefix.as_bytes()));
    }
}
