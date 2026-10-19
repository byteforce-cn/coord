//! `coord.txn.v2` 读阶段会话与 OCC 读集（ADR-0015 D4/D12.2，#144）。
//!
//! 读集由服务端持有：`Read` 在把 point key / range 谓词记入会话后，才在
//! `start_ts` 快照上取数；`Commit` 交由 #151 消费这份读集。会话只活在
//! 协调节点内存里（不复制），丢失/过期只能让客户端重跑整个事务。

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

/// 读集上限的分类；线缆上统一映射为 `INVALID_ARGUMENT` + `READ_SET_TOO_LARGE`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadSetLimit {
    /// point + range 条目数超过 `max_read_set`。
    Entries,
    /// 单次 range 扫描超过 `max_range` 个 key。
    RangeKeys,
}

/// 半开读谓词 `[start, end)`；`end <= start` 是**空谓词**（读不到任何 key）。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ReadRange {
    pub(crate) start: Vec<u8>,
    pub(crate) end: Vec<u8>,
}

impl ReadRange {
    pub(crate) fn new(start: &[u8], end: &[u8]) -> Self {
        Self {
            start: start.to_vec(),
            end: end.to_vec(),
        }
    }

    fn is_empty(&self) -> bool {
        self.end <= self.start
    }

    fn contains(&self, key: &[u8]) -> bool {
        !self.is_empty() && self.start.as_slice() <= key && key < self.end.as_slice()
    }

    fn covers(&self, other: &Self) -> bool {
        !self.is_empty() && self.start <= other.start && other.end <= self.end
    }

    /// 重叠或相邻（`[a,c)` 与 `[c,d)` 可合并为一个谓词）。
    fn overlaps_or_adjacent(&self, other: &Self) -> bool {
        !self.is_empty() && !other.is_empty() && self.start <= other.end && other.start <= self.end
    }
}

/// 一个事务会话的 OCC 读集：point key / 非空 range / 空谓词。
///
/// 不变量：`ranges` 已排序且互不重叠/不相邻；`points` 不被任何 range 覆盖。
/// 因此同一组读无论顺序如何，`len()` 与内容都相同（读集只增不减）。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ReadSet {
    points: BTreeSet<Vec<u8>>,
    ranges: Vec<ReadRange>,
    empty_ranges: BTreeSet<ReadRange>,
}

impl ReadSet {
    /// 计入上限的条目数：point + 非空 range + 空谓词各算一条。
    pub(crate) fn len(&self) -> usize {
        self.points.len() + self.ranges.len() + self.empty_ranges.len()
    }

    pub(crate) fn try_add_point(&mut self, key: &[u8], max: usize) -> Result<(), ReadSetLimit> {
        if self.points.contains(key) || self.ranges.iter().any(|r| r.contains(key)) {
            return Ok(());
        }
        if self.len() >= max {
            return Err(ReadSetLimit::Entries);
        }
        self.points.insert(key.to_vec());
        Ok(())
    }

    pub(crate) fn try_add_range(
        &mut self,
        start: &[u8],
        end: &[u8],
        max: usize,
    ) -> Result<(), ReadSetLimit> {
        let range = ReadRange::new(start, end);
        if range.is_empty() {
            // 空谓词读不到任何 key，但按 #129 保留在读集里（去重、计入上限）。
            if self.empty_ranges.contains(&range) {
                return Ok(());
            }
            if self.len() >= max {
                return Err(ReadSetLimit::Entries);
            }
            self.empty_ranges.insert(range);
            return Ok(());
        }
        if self.ranges.iter().any(|r| r.covers(&range)) {
            return Ok(());
        }
        // 与已有 range 合并时条目数不增（还会吸收被覆盖的 point），满额也允许；
        // 独立谓词才 +1，因此上限判断必须在插入前完成。
        let merges = self.ranges.iter().any(|r| r.overlaps_or_adjacent(&range));
        if !merges && self.len() >= max {
            return Err(ReadSetLimit::Entries);
        }
        self.ranges.push(range);
        self.merge_ranges();
        Ok(())
    }

    fn merge_ranges(&mut self) {
        let mut sorted = std::mem::take(&mut self.ranges);
        sorted.sort();
        let mut merged: Vec<ReadRange> = Vec::with_capacity(sorted.len());
        for range in sorted {
            match merged.last_mut() {
                Some(last) if last.end >= range.start => {
                    if range.end > last.end {
                        last.end = range.end;
                    }
                }
                _ => merged.push(range),
            }
        }
        self.ranges = merged;
        let ranges = &self.ranges;
        self.points
            .retain(|point| !ranges.iter().any(|r| r.contains(point)));
    }
}

/// 一次 `Read` 对读集的增量。
#[derive(Debug, Clone, Copy)]
pub(crate) enum ReadIntent<'a> {
    Point(&'a [u8]),
    Range { start: &'a [u8], end: &'a [u8] },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionError {
    /// 会话不存在、已过期或已 abort。
    NotFound,
    ReadSetLimit(ReadSetLimit),
}

struct TxnSession {
    start_ts: u64,
    expires_at: Instant,
    read_set: ReadSet,
}

/// 协调节点的会话表（ADR-0015 D12.2：不复制）。
pub(crate) struct TxnSessions {
    sessions: Mutex<BTreeMap<Vec<u8>, TxnSession>>,
}

impl TxnSessions {
    pub(crate) fn new() -> Self {
        Self {
            sessions: Mutex::new(BTreeMap::new()),
        }
    }

    pub(crate) fn begin(&self, start_ts: u64, ttl: Duration) -> Vec<u8> {
        self.purge_expired();
        let id = uuid::Uuid::new_v4().as_bytes().to_vec();
        let now = Instant::now();
        self.sessions.lock().insert(
            id.clone(),
            TxnSession {
                start_ts,
                expires_at: now + ttl,
                read_set: ReadSet::default(),
            },
        );
        id
    }

    /// 记录一次读并返回会话的 `start_ts`（读阶段所有读共用该快照）。
    ///
    /// 记录发生在数据读之前：并发读无法在“先读后判”的窗口里穿过上限；
    /// 数据读失败时谓词保留，读集是超集只会造成保守冲突。
    pub(crate) fn record_read(
        &self,
        id: &[u8],
        intent: ReadIntent<'_>,
        max_read_set: usize,
    ) -> Result<u64, SessionError> {
        let now = Instant::now();
        let mut sessions = self.sessions.lock();
        if sessions
            .get(id)
            .is_some_and(|session| session.expires_at <= now)
        {
            sessions.remove(id);
            return Err(SessionError::NotFound);
        }
        let session = sessions.get_mut(id).ok_or(SessionError::NotFound)?;
        match intent {
            ReadIntent::Point(key) => session.read_set.try_add_point(key, max_read_set),
            ReadIntent::Range { start, end } => {
                session.read_set.try_add_range(start, end, max_read_set)
            }
        }
        .map_err(SessionError::ReadSetLimit)?;
        Ok(session.start_ts)
    }

    /// 幂等删除：未知/过期/已 abort 的会话不报错。
    pub(crate) fn abort(&self, id: &[u8]) {
        self.sessions.lock().remove(id);
    }

    pub(crate) fn purge_expired(&self) {
        let now = Instant::now();
        self.sessions
            .lock()
            .retain(|_, session| session.expires_at > now);
    }

    #[cfg(test)]
    fn read_set(&self, id: &[u8]) -> Option<ReadSet> {
        self.sessions
            .lock()
            .get(id)
            .map(|session| session.read_set.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn read_set_merges_overlapping_and_adjacent_ranges() {
        let mut set = ReadSet::default();
        set.try_add_range(b"a", b"c", 10).unwrap();
        set.try_add_range(b"b", b"d", 10).unwrap();
        set.try_add_range(b"d", b"f", 10).unwrap(); // 相邻也合并
        assert_eq!(set.len(), 1);
        assert_eq!(set.ranges, [ReadRange::new(b"a", b"f")]);
    }

    #[test]
    fn read_set_absorbs_points_covered_by_range() {
        let mut set = ReadSet::default();
        set.try_add_point(b"b", 10).unwrap();
        set.try_add_range(b"a", b"c", 10).unwrap();
        assert_eq!(set.len(), 1);
        assert!(set.points.is_empty());
        // 先 range 后 point：已覆盖，不增长。
        set.try_add_point(b"b", 10).unwrap();
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn read_set_dedupes_and_rejects_over_limit() {
        let mut set = ReadSet::default();
        set.try_add_point(b"a", 2).unwrap();
        set.try_add_point(b"a", 2).unwrap();
        set.try_add_point(b"b", 2).unwrap();
        assert_eq!(set.try_add_point(b"c", 2), Err(ReadSetLimit::Entries));
        assert_eq!(set.len(), 2, "被拒绝的谓词不得改动读集");
    }

    #[test]
    fn read_set_at_limit_can_still_merge() {
        let mut set = ReadSet::default();
        set.try_add_range(b"a", b"c", 2).unwrap();
        set.try_add_point(b"z", 2).unwrap();
        // 与已有 range 重叠 → 条目不增，满额也允许。
        set.try_add_range(b"b", b"d", 2).unwrap();
        assert_eq!(set.len(), 2);
        // 完全独立的谓词 → 拒绝。
        assert_eq!(set.try_add_range(b"x", b"y", 2), Err(ReadSetLimit::Entries));
        assert_eq!(set.try_add_range(b"q", b"q", 2), Err(ReadSetLimit::Entries));
    }

    #[test]
    fn empty_range_is_recorded_and_deduped() {
        let mut set = ReadSet::default();
        set.try_add_range(b"z", b"a", 2).unwrap();
        set.try_add_range(b"z", b"a", 2).unwrap();
        assert_eq!(set.len(), 1);
        assert_eq!(set.empty_ranges.len(), 1);
    }

    #[test]
    fn sessions_expire_and_abort_is_idempotent() {
        let sessions = TxnSessions::new();
        let id = sessions.begin(7, Duration::from_millis(1));
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(
            sessions.record_read(&id, ReadIntent::Point(b"a"), 10),
            Err(SessionError::NotFound)
        );

        let id = sessions.begin(7, Duration::from_secs(30));
        assert_eq!(
            sessions.record_read(&id, ReadIntent::Point(b"a"), 10),
            Ok(7)
        );
        sessions.abort(&id);
        sessions.abort(&id);
        assert_eq!(sessions.read_set(&id), None);
    }

    #[test]
    fn session_records_range_and_point_in_one_read_set() {
        let sessions = TxnSessions::new();
        let id = sessions.begin(9, Duration::from_secs(30));
        sessions
            .record_read(
                &id,
                ReadIntent::Range {
                    start: b"a",
                    end: b"c",
                },
                10,
            )
            .unwrap();
        sessions
            .record_read(&id, ReadIntent::Point(b"b"), 10)
            .unwrap();
        let set = sessions.read_set(&id).unwrap();
        assert_eq!(set.len(), 1);
        assert_eq!(set.ranges, [ReadRange::new(b"a", b"c")]);
    }
}
