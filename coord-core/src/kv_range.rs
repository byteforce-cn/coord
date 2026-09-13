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

/// 半开区间 `[lo, hi)` 的包含判定；`hi = None` 表示无上界。
///
/// 与 [`watch_match_interval`] 配套：区间一旦由**同一处**算出，判定也只有一份。
pub fn interval_contains(lo: &[u8], hi: Option<&[u8]>, key: &[u8]) -> bool {
    if key < lo {
        return false;
    }
    match hi {
        None => true,
        Some(hi) => key < hi,
    }
}

/// Watch 订阅的**有效匹配区间**（半开 `[lo, hi)`，`hi = None` = 无上界）。
///
/// 第四轮 §3.8：Watch 与 KV/Txn 的语义差异**只有一处**，且是刻意的——`range_end`
/// 为空时，KV/Txn 是 `RangeSemantics::SingleKey`（点查），Watch 是**字节前缀订阅**
/// （`PrefixScan` 与配置中心订阅依赖它）。把 Wire 语义改成 KV 口径是一次破坏性
/// 协议变更（需同步改 Rust/Java 客户端与所有订阅方），因此本函数**不改变行为**，
/// 只把"Watch 实际会投递哪些 key"收敛成**一处定义**，供两处共用：
///
/// 1. `coord-server/src/watch/mod.rs::key_matches`（投递判定）；
/// 2. `coord-core/src/grpc_auth.rs::extract_scope_access`（鉴权区间）。
///
/// 此前两处各自实现（"第三套语义"），本函数使之不再可能漂移。
///
/// - `range_end` 为空 → 前缀订阅：`[key, prefix_successor(key))`；
///   `prefix_successor` 为 `None`（空前缀 / 全 `0xFF`）时上界为 `None`（全 keyspace）；
/// - `range_end` 非空 → 实际集合是 `starts_with(key) ∩ (-∞, range_end)`，
///   即 `[key, min(range_end, prefix_successor(key)))`——**不是**裸的
///   `[key, range_end)`：后者会包含不以 `key` 开头的 key（如 `key="/app"`、
///   `range_end="/apz"` 时的 `"/apq"`），是鉴权区间大于实际投递集合的
///   "理想化"写法。
pub fn watch_match_interval(key: &[u8], range_end: &[u8]) -> (Vec<u8>, Option<Vec<u8>>) {
    let prefix_upper = prefix_successor(key);
    let upper = if range_end.is_empty() {
        prefix_upper
    } else {
        match prefix_upper {
            Some(pu) => Some(pu.min(range_end.to_vec())),
            None => Some(range_end.to_vec()),
        }
    };
    (key.to_vec(), upper)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 第四轮 §3.8 回归：`watch_match_interval` 必须与 Watch 原有的
    /// `key_matches` 实现**逐 key 等价**（否则"统一到单点定义"就是悄悄改语义）。
    ///
    /// 参照实现 = 修复前 `coord-server/src/watch/mod.rs::key_matches` 的字面语义：
    /// `starts_with(prefix) && (range_end.is_empty() || key < range_end)`。
    fn legacy_key_matches(key: &[u8], prefix: &[u8], range_end: &[u8]) -> bool {
        if !key.starts_with(prefix) {
            return false;
        }
        if range_end.is_empty() {
            true
        } else {
            key < range_end
        }
    }

    #[test]
    fn watch_interval_agrees_with_watch_delivery_predicate() {
        let prefixes: Vec<Vec<u8>> = vec![
            b"".to_vec(),
            b"/".to_vec(),
            b"/app".to_vec(),
            b"/app/".to_vec(),
            b"/app/cfg".to_vec(),
            vec![0xFF],
            vec![0x61, 0xFF],
        ];
        let range_ends: Vec<Vec<u8>> = vec![
            Vec::new(),        // 空前缀订阅
            b"/app0".to_vec(), // == prefix_successor("/app/")
            b"/apq".to_vec(),  // == prefix_successor("/app")
            b"/zzz".to_vec(),  // 远大于前缀上界
            b"/a".to_vec(),    // 小于 key（空区间）
            b"\0".to_vec(),    // UNBOUNDED_RANGE_END
            vec![0xFF, 0xFF],
        ];
        let keys: Vec<Vec<u8>> = vec![
            b"".to_vec(),
            b"/".to_vec(),
            b"/a".to_vec(),
            b"/ap".to_vec(),
            b"/app".to_vec(),
            b"/app/".to_vec(),
            b"/app/a".to_vec(),
            b"/app/cfg/x".to_vec(),
            b"/app0".to_vec(),
            b"/application".to_vec(),
            b"/appq".to_vec(),
            b"/apq".to_vec(),
            b"/zzz".to_vec(),
            b"/zzz0".to_vec(),
            vec![0xFF],
            vec![0xFF, 0x01],
            vec![0x61, 0xFF, 0x00],
        ];

        for prefix in &prefixes {
            for range_end in &range_ends {
                let (lo, hi) = watch_match_interval(prefix, range_end);
                assert_eq!(&lo, prefix, "区间下界必须就是 key");
                for key in &keys {
                    let expected = legacy_key_matches(key, prefix, range_end);
                    let got = interval_contains(&lo, hi.as_deref(), key);
                    assert_eq!(
                        got, expected,
                        "prefix={prefix:?} range_end={range_end:?} key={key:?}: \
                         interval [{lo:?}, {hi:?}) 与实际投递集合不一致"
                    );
                }
            }
        }
    }

    /// 显式 `range_end` 也必须被前缀上界**钳制**：`[key, range_end)` 大于实际集合
    /// （会包含不以 key 开头的 key），鉴权层若直接采信就是区间大于实际访问。
    #[test]
    fn watch_interval_clamps_explicit_range_end_to_prefix() {
        // key="/app", range_end="/zzz"：实际集合只到 succ("/app") = "/apq"，
        // 不应把 "/apq".."/zzz" 之间的 key 也算进访问区间。
        let (lo, hi) = watch_match_interval(b"/app", b"/zzz");
        assert_eq!(lo, b"/app");
        assert_eq!(hi.as_deref(), Some(&b"/apq"[..]));
        assert!(!interval_contains(&lo, hi.as_deref(), b"/apq"));
    }

    /// 无界上界 = 全 keyspace（空前缀 / 全 0xFF 前缀）。
    #[test]
    fn watch_interval_without_upper_bound() {
        assert_eq!(watch_match_interval(b"", b""), (Vec::new(), None));
        assert_eq!(watch_match_interval(&[0xFF], b""), (vec![0xFF], None));
        assert!(interval_contains(b"", None, b"/anything"));
    }

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
