//! CommitWatermark 计算（ADR-0015 D5/D9，#155）。
//!
//! D9 口径：`watermark = min(in-flight commit_ts) - 1`；无 in-flight 时等于
//! `max_committed_ts`。in-flight 有两类来源，均以「最早未完成 ts」表示：
//! 1. 已决策但尚未被某个数据 Region apply 的决策（apply 游标之后的下一条）；
//! 2. 已 apply（outbox 有记录）但尚未追加进 shard 组的记录。
//!
//! 这里只做纯计算；候选的收集（扫描游标/outbox）在协调器里完成。

/// 一次 watermark 计算的结果（供 tick/指标/测试读取）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitWatermarkState {
    pub watermark: u64,
    pub in_flight: u64,
    pub min_in_flight_ts: Option<u64>,
}

/// D9：`min(in-flight) - 1`；无 in-flight = `decision_last_ts`。
///
/// 候选高于已决策水位时按已决策水位封顶（防御性：调用方的扫描游标不会产生
/// 这种输入，但封顶保证 watermark 不越过尚未存在的决策）。
pub fn commit_watermark(decision_last_ts: u64, in_flight: &[u64]) -> CommitWatermarkState {
    let min_in_flight = in_flight.iter().copied().min();
    let watermark = match min_in_flight {
        Some(ts) => ts.saturating_sub(1).min(decision_last_ts),
        None => decision_last_ts,
    };
    CommitWatermarkState {
        watermark,
        in_flight: in_flight.len() as u64,
        min_in_flight_ts: min_in_flight,
    }
}

/// 某数据 Region 已 apply 游标之后的下一条决策（无 = 该 Region 不 in-flight）。
pub fn pending_decision(last_applied_ts: u64, next_decision_ts: Option<u64>) -> Option<u64> {
    next_decision_ts.filter(|ts| *ts > last_applied_ts)
}

/// 某 shard 对某数据 Region 的下一条未 ship outbox 记录（区间外 = 该 Region
/// 在本 shard 已搬完）。
///
/// 扫描本身从 `max(shipped, start - 1)` 之后开始，因此返回的 `ts` 不会低于
/// `start_ts`；这里仍做半开区间校验，避免调用方误传全量扫描结果。
pub fn unshipped_record(start_ts: u64, end_ts: u64, next_outbox_ts: Option<u64>) -> Option<u64> {
    next_outbox_ts.filter(|ts| *ts >= start_ts && (end_ts == 0 || *ts < end_ts))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watermark_is_the_last_committed_ts_without_in_flight() {
        assert_eq!(commit_watermark(0, &[]).watermark, 0);
        let state = commit_watermark(10, &[]);
        assert_eq!(state.watermark, 10);
        assert_eq!(state.in_flight, 0);
        assert_eq!(state.min_in_flight_ts, None);
    }

    #[test]
    fn watermark_stops_below_the_earliest_in_flight_ts() {
        let state = commit_watermark(10, &[9, 7]);
        assert_eq!(state.watermark, 6);
        assert_eq!(state.in_flight, 2);
        assert_eq!(state.min_in_flight_ts, Some(7));
        assert_eq!(commit_watermark(10, &[1]).watermark, 0);
        assert_eq!(
            commit_watermark(10, &[11]).watermark,
            10,
            "候选不得超过已决策水位"
        );
    }

    #[test]
    fn pending_decision_starts_after_the_applied_cursor() {
        assert_eq!(pending_decision(5, Some(7)), Some(7));
        assert_eq!(
            pending_decision(7, Some(7)),
            None,
            "等于游标的决策已 apply"
        );
        assert_eq!(pending_decision(7, None), None);
        assert_eq!(pending_decision(0, Some(2)), Some(2));
    }

    #[test]
    fn unshipped_record_only_counts_inside_the_shard_range() {
        assert_eq!(unshipped_record(1, 10, Some(7)), Some(7));
        assert_eq!(unshipped_record(1, 10, Some(10)), None, "end 是半开区间");
        assert_eq!(
            unshipped_record(5, 10, Some(4)),
            None,
            "扫描下界之下不再有记录"
        );
        assert_eq!(
            unshipped_record(1, 0, Some(7)),
            Some(7),
            "未定界 shard 接受任意更大 ts"
        );
        assert_eq!(unshipped_record(1, 10, None), None);
    }
}
