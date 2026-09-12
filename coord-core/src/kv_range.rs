//! KV `Range` 请求的**有效语义**——协议层唯一定义。
//!
//! 背景（第三轮复核 P0）：同一份代码里 `range_end` 为空曾有两种解读——
//! 鉴权层按「点查」建模、而 Txn 内层 `TxnOp::Range` 按「字节前缀扫描」执行
//! （顶层 `KV/Range` 则是点查）。两者不一致会产生**可越权的语义裂缝**：
//! 持 `scope="/app/a/"` 的凭据发 `Txn{Range(key="/app/a", range_end="")}`
//! 会被鉴权层判为「对 `/app/a` 的单键访问」而放行，服务端却做前缀扫描，
//! 返回 `/app/abc/config`、`/app/admin/root-token` 等 scope 外的 Key。
//!
//! 结构性修复：把「`(key, range_end)` 到底是什么意思」收敛为**这一个函数**，
//! 由服务端读取路径（顶层 `Range`/`Delete`、Txn 内层 op）与鉴权层
//! （`extract_scope_access`）**共同**调用。只要两边都走 [`RangeSemantics::of`]，
//! 建模与执行就不可能再分叉。
//!
//! 语义取 etcd 口径，也是仓库对其他两处的既有约定：
//! - 顶层 `KV/Range`（`coord-server/src/server/mod.rs`）：
//!   `range_end` 为空 或 `range_end == key` → 单键精确查询；
//! - 插件 ABI（`coord-agent/wit/coord-plugin.wit`）：
//!   「`kv.range`（`range-end` 空 = 单键精确查询）」。

/// `Range` 请求（顶层 `KV/Range`、`KV/Delete` 与 Txn 内层
/// `TxnOp::Range` / `TxnOp::Delete` 共用）的有效语义。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeSemantics {
    /// 单键精确查询：`range_end` 为空，或 `range_end == key`。
    SingleKey,
    /// 半开区间 `[key, range_end)`。
    Interval,
}

impl RangeSemantics {
    /// 判定 `(key, range_end)` 的有效语义。
    ///
    /// **不得**在别处复制这段判断——复制出来的第二份就是下一个语义裂缝。
    pub fn of(key: &[u8], range_end: &[u8]) -> Self {
        if range_end.is_empty() || range_end == key {
            Self::SingleKey
        } else {
            Self::Interval
        }
    }

    /// 是否为单键精确查询。
    pub fn is_single_key(self) -> bool {
        matches!(self, Self::SingleKey)
    }

    /// 是否为半开区间扫描。
    pub fn is_interval(self) -> bool {
        matches!(self, Self::Interval)
    }
}

/// etcd 语义下的「无界上界」：`range_end == "\0"` 表示从 `key` 扫描到 keyspace 末尾。
///
/// 鉴权层对**有界** scope 一律拒绝该区间（除非 scope 是 match-all）。
pub const UNBOUNDED_RANGE_END: &[u8] = b"\0";

/// 前缀扫描惯用法的区间上界：`prefix` 的字典序后继（末字节 +1，进位丢弃）。
///
/// - `prefix = ""` → `None`（空前缀的上界不存在，语义为「全 keyspace」）；
/// - `prefix` 全为 `0xFF` → `None`（不存在这样的字节串）；
/// - 其余 → `Some(succ)`，且 `[prefix, succ)` 恰好等于「以 `prefix` 为字节前缀的串集合」。
///
/// 调用方在 `None` 时**必须 fail-closed**（不得退化为无界扫描）。
pub fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut out = prefix.to_vec();
    while let Some(last) = out.pop() {
        if last != 0xFF {
            out.push(last.wrapping_add(1));
            return Some(out);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_key_when_range_end_empty_or_equal() {
        assert_eq!(RangeSemantics::of(b"/k", b""), RangeSemantics::SingleKey);
        assert_eq!(RangeSemantics::of(b"/k", b"/k"), RangeSemantics::SingleKey);
        assert_eq!(RangeSemantics::of(b"", b""), RangeSemantics::SingleKey);
        assert!(RangeSemantics::of(b"/k", b"").is_single_key());
    }

    #[test]
    fn interval_when_range_end_differs() {
        assert_eq!(RangeSemantics::of(b"/k", b"/k0"), RangeSemantics::Interval);
        assert_eq!(
            RangeSemantics::of(b"/k", UNBOUNDED_RANGE_END),
            RangeSemantics::Interval
        );
        assert!(RangeSemantics::of(b"/k", b"/k0").is_interval());
    }

    #[test]
    fn prefix_successor_is_exact_upper_bound() {
        assert_eq!(prefix_successor(b"/app/"), Some(b"/app0".to_vec()));
        assert_eq!(prefix_successor(b"abc"), Some(b"abd".to_vec()));
        // 进位：末字节 0xFF 丢弃后前一位 +1
        assert_eq!(prefix_successor(b"ab\xff"), Some(b"ac".to_vec()));
        assert_eq!(prefix_successor(b"\xff\xff"), None);
        assert_eq!(prefix_successor(b""), None);
    }
}
