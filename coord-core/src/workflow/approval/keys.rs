//! v2 审批实例的 KV 键布局（ADR-0013 D8）。
//!
//! 状态是单文档 `/_workflow/v4/instances/{instance_id}`；v4 前缀与 v1 的实例扫描面
//! （`/_workflow/v3/instances/`）互不可见。事件仍写 v3 outbox 传输设施
//! （`/_workflow/v3/outbox/**`，relay 已认该前缀），只有实例状态换到独立键空间。

/// v2 审批实例的状态前缀。
pub const APPROVAL_INSTANCE_PREFIX: &[u8] = b"/_workflow/v4/instances/";

/// 实例状态文档键：`APPROVAL_INSTANCE_PREFIX + instance_id`。
pub fn instance_key(instance_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(APPROVAL_INSTANCE_PREFIX.len() + instance_id.len());
    key.extend_from_slice(APPROVAL_INSTANCE_PREFIX);
    key.extend_from_slice(instance_id.as_bytes());
    key
}

/// 实例扫描前缀（[`instance_key`] 的前缀部分）。
pub fn instance_prefix() -> &'static [u8] {
    APPROVAL_INSTANCE_PREFIX
}

/// 前缀扫描的字典序上界：最后一个字节 +1（逐字节进位），全 0xFF 前缀返回空 Vec。
///
/// 空 Vec 在 KV range 里表示「扫到无穷」——这正是全 0xFF 前缀没有有限上界的情形。
pub fn prefix_range_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    for i in (0..end.len()).rev() {
        if end[i] < 0xFF {
            end[i] += 1;
            end.truncate(i + 1);
            return end;
        }
    }
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn approval_instance_key_layout() {
        assert_eq!(
            String::from_utf8_lossy(&instance_key("inst-1")),
            "/_workflow/v4/instances/inst-1"
        );
        assert_eq!(
            String::from_utf8_lossy(instance_prefix()),
            "/_workflow/v4/instances/"
        );
    }

    #[test]
    fn approval_keys_are_outside_v1_instance_prefix() {
        let key = instance_key("inst-1");
        assert!(!key.starts_with(b"/_workflow/v3/instances/"));

        let start = instance_prefix();
        let end = prefix_range_end(start);
        let v1_key: &[u8] = b"/_workflow/v3/instances/x";
        assert!(v1_key < start);
        assert!(!(v1_key >= start && v1_key < end.as_slice()));
    }

    #[test]
    fn prefix_range_end_semantics() {
        assert_eq!(
            prefix_range_end(b"/_workflow/v4/instances/"),
            b"/_workflow/v4/instances0"
        );
        assert_eq!(prefix_range_end(b"/a\xff"), b"/b");
        assert_eq!(prefix_range_end(&[0xFF, 0xFF, 0xFF]), Vec::<u8>::new());
        assert_eq!(prefix_range_end(b""), Vec::<u8>::new());
    }
}
