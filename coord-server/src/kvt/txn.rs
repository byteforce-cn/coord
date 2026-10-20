//! `coord.txn.v2` 读阶段会话与 OCC 读集（ADR-0015 D4/D12.2，#144）。
//!
//! 读集由服务端持有：`Read` 在把 point key / range 谓词记入会话后，才在
//! `start_ts` 快照上取数；`Commit` 交由 #151 消费这份读集。会话只活在
//! 协调节点内存里（不复制），丢失/过期只能让客户端重跑整个事务。
//!
//! TTL 从 `Begin` 起算且**不因读续期**；过期是惰性判定（读/写先看
//! `expires_at`），后台 reaper 只负责释放不再被访问的会话内存。
//!
//! OCC 读阶段没有 intent/lock：`abort` 只丢会话（读集随之释放），不写
//! MVCC/决策；已决策事务的 apply 不受 abort 影响（#148）。
//!
//! Split/merge 的 drain 是节点本地原语：标记区间后拒绝命中它的新读
//! （`UNAVAILABLE`，可重试），`DrainGuard::wait` 等待相交会话
//! commit/abort/TTL 到期并有超时上限。读集之外的盲写由 #151 提交路径与
//! #163 的 cutover/epoch 拒绝。

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
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

    /// 有共同 key（相邻区间 `[a,c)`/`[c,e)` 不重叠）。
    fn overlaps(&self, other: &Self) -> bool {
        !self.is_empty() && !other.is_empty() && self.start < other.end && other.start < self.end
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

    /// 读集是否与 `range` 有共同 key（drain 等待/准入共用）。
    /// 空谓词读不到任何 key，不计入。
    // G5（#163）在 split/merge 中消费 drain 契约；本 gate 先交付测试过的原语，
    // 生产调用点尚未接入。
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn intersects(&self, range: &ReadRange) -> bool {
        if range.is_empty() {
            return false;
        }
        self.points.iter().any(|point| range.contains(point))
            || self.ranges.iter().any(|r| r.overlaps(range))
    }
}

/// 一次 `Read` 对读集的增量。
#[derive(Debug, Clone, Copy)]
pub(crate) enum ReadIntent<'a> {
    Point(&'a [u8]),
    Range { start: &'a [u8], end: &'a [u8] },
}

impl ReadIntent<'_> {
    /// 该读是否会碰到 `range`（空谓词不碰任何 key）。
    fn intersects(&self, range: &ReadRange) -> bool {
        match self {
            Self::Point(key) => range.contains(key),
            Self::Range { start, end } => ReadRange::new(start, end).overlaps(range),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionError {
    /// 会话不存在、已过期或已 abort。
    NotFound,
    ReadSetLimit(ReadSetLimit),
    /// 读区间正在为 split/merge drain；会话仍有效，退避后重试。
    Draining,
}

struct TxnSession {
    start_ts: u64,
    expires_at: Instant,
    read_set: ReadSet,
}

/// 会话表的可变部分：后台 reaper 与 drain guard 共享同一把锁，
/// 保证「清理过期会话」与「登记 drain 区间」看到的是一致快照。
#[derive(Default)]
struct TxnTable {
    sessions: BTreeMap<Vec<u8>, TxnSession>,
    /// 正在 drain 的区间；同区间可重复登记（每个 guard 一条）。
    drains: Vec<ReadRange>,
}

/// drain 的结果：`TimedOut` 带上截止时仍相交的会话数（调用方据此决定回滚/重试）。
// G5（#163）消费；见 `ReadSet::intersects` 的说明。
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DrainOutcome {
    Drained,
    TimedOut { active: usize },
}

/// split/merge 的 drain 标记；drop 即解除（成功/失败/回滚都要释放）。
// G5（#163）消费；见 `ReadSet::intersects` 的说明。
#[cfg_attr(not(test), allow(dead_code))]
#[must_use = "drop 会立即解除 drain 标记"]
pub(crate) struct DrainGuard {
    table: Arc<Mutex<TxnTable>>,
    range: ReadRange,
}

impl DrainGuard {
    /// 等到没有会话的读集与 `range` 相交，或 `timeout` 到期。
    ///
    /// 轮询而非事件通知：每次轮询前清理过期会话，因此 TTL 到期必然放行；
    /// split 是低频运维动作，10ms 的延迟不值得给每次 abort/commit 加通知开销。
    // G5（#163）消费；见 `ReadSet::intersects` 的说明。
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn wait(&self, timeout: Duration) -> DrainOutcome {
        let deadline = Instant::now() + timeout;
        loop {
            let active = {
                let mut table = self.table.lock();
                sweep(&mut table, Instant::now());
                table
                    .sessions
                    .values()
                    .filter(|session| session.read_set.intersects(&self.range))
                    .count()
            };
            if active == 0 {
                return DrainOutcome::Drained;
            }
            let now = Instant::now();
            if now >= deadline {
                return DrainOutcome::TimedOut { active };
            }
            tokio::time::sleep((deadline - now).min(Duration::from_millis(10))).await;
        }
    }
}

impl Drop for DrainGuard {
    fn drop(&mut self) {
        let mut table = self.table.lock();
        if let Some(pos) = table.drains.iter().position(|range| range == &self.range) {
            table.drains.remove(pos);
        }
    }
}

fn sweep(table: &mut TxnTable, now: Instant) -> usize {
    let before = table.sessions.len();
    table.sessions.retain(|_, session| session.expires_at > now);
    before - table.sessions.len()
}

/// 协调节点的会话表（ADR-0015 D12.2：不复制）。
pub(crate) struct TxnSessions {
    table: Arc<Mutex<TxnTable>>,
}

impl TxnSessions {
    pub(crate) fn new() -> Self {
        Self {
            table: Arc::new(Mutex::new(TxnTable::default())),
        }
    }

    pub(crate) fn begin(&self, start_ts: u64, ttl: Duration) -> Vec<u8> {
        let id = uuid::Uuid::new_v4().as_bytes().to_vec();
        let now = Instant::now();
        let mut table = self.table.lock();
        sweep(&mut table, now);
        table.sessions.insert(
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
        let mut table = self.table.lock();
        if table
            .sessions
            .get(id)
            .is_some_and(|session| session.expires_at <= now)
        {
            table.sessions.remove(id);
            return Err(SessionError::NotFound);
        }
        if !table.sessions.contains_key(id) {
            return Err(SessionError::NotFound);
        }
        if table.drains.iter().any(|range| intent.intersects(range)) {
            return Err(SessionError::Draining);
        }
        let session = table.sessions.get_mut(id).ok_or(SessionError::NotFound)?;
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
        self.table.lock().sessions.remove(id);
    }

    /// 标记 `range` 进入 drain：新读命中该区间返回 `Draining`，
    /// 已存在且读集相交的会话通过 [`DrainGuard::wait`] 等待其结束/过期。
    // G5（#163）消费；见 `ReadSet::intersects` 的说明。
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn drain(&self, range: ReadRange) -> DrainGuard {
        self.table.lock().drains.push(range.clone());
        DrainGuard {
            table: Arc::clone(&self.table),
            range,
        }
    }

    /// 清掉过期会话并返回删除数量（测试直接驱动；生产路径由 reaper 调 `sweep`）。
    #[cfg(test)]
    pub(crate) fn purge_expired(&self) -> usize {
        sweep(&mut self.table.lock(), Instant::now())
    }

    /// 后台 TTL 回收：每 `interval` 清一次过期会话。
    ///
    /// 过期本身是惰性语义（读/写会检查 `expires_at`），reaper 只负责释放
    /// 不再被访问的会话内存；只在 `[kv_t].enabled=true` 时由 main 启动。
    pub(crate) fn spawn_reaper(&self, interval: Duration) -> tokio::task::JoinHandle<()> {
        let table = Arc::clone(&self.table);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                sweep(&mut table.lock(), Instant::now());
            }
        })
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.table.lock().sessions.len()
    }

    #[cfg(test)]
    fn read_set(&self, id: &[u8]) -> Option<ReadSet> {
        self.table
            .lock()
            .sessions
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
    fn purge_expired_removes_only_expired_sessions_and_reports_count() {
        let sessions = TxnSessions::new();
        let long = sessions.begin(7, Duration::from_secs(30));
        let short = sessions.begin(8, Duration::from_millis(1));
        std::thread::sleep(Duration::from_millis(5));

        assert_eq!(sessions.purge_expired(), 1);
        assert_eq!(sessions.len(), 1);
        assert!(sessions.read_set(&long).is_some());
        assert!(sessions.read_set(&short).is_none());
        // 幂等：没有新过期会话时返回 0。
        assert_eq!(sessions.purge_expired(), 0);
    }

    #[tokio::test]
    async fn reaper_reclaims_expired_sessions() {
        let sessions = TxnSessions::new();
        let _reaper = sessions.spawn_reaper(Duration::from_millis(5));
        let _id = sessions.begin(9, Duration::from_millis(1));
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(sessions.len(), 0);
    }

    #[test]
    fn read_set_intersects_only_real_key_overlap() {
        let mut set = ReadSet::default();
        set.try_add_point(b"d", 10).unwrap();
        set.try_add_range(b"a", b"c", 10).unwrap();
        set.try_add_range(b"z", b"a", 10).unwrap(); // 空谓词

        assert!(set.intersects(&ReadRange::new(b"c", b"e"))); // d 命中
        assert!(set.intersects(&ReadRange::new(b"b", b"b1"))); // range 重叠
        assert!(!set.intersects(&ReadRange::new(b"c", b"d"))); // 相邻 [a,c) 无共同 key
        assert!(!set.intersects(&ReadRange::new(b"e", b"f")));
        assert!(!set.intersects(&ReadRange::new(b"z", b"a"))); // 空 drain 区间
    }

    #[tokio::test]
    async fn drain_waits_for_intersecting_session_and_times_out() {
        let sessions = TxnSessions::new();
        let id = sessions.begin(1, Duration::from_secs(30));
        sessions
            .record_read(&id, ReadIntent::Point(b"d"), 10)
            .unwrap();
        let guard = sessions.drain(ReadRange::new(b"c", b"e"));

        let outcome = guard.wait(Duration::from_millis(30)).await;
        assert_eq!(outcome, DrainOutcome::TimedOut { active: 1 });

        sessions.abort(&id);
        assert_eq!(
            guard.wait(Duration::from_secs(1)).await,
            DrainOutcome::Drained
        );
    }

    #[tokio::test]
    async fn drain_ignores_unrelated_and_expired_sessions() {
        let sessions = TxnSessions::new();
        let unrelated = sessions.begin(1, Duration::from_secs(30));
        sessions
            .record_read(
                &unrelated,
                ReadIntent::Range {
                    start: b"a",
                    end: b"c",
                },
                10,
            )
            .unwrap();
        let expired = sessions.begin(1, Duration::from_millis(1));
        sessions
            .record_read(&expired, ReadIntent::Point(b"d"), 10)
            .unwrap();
        std::thread::sleep(Duration::from_millis(5));

        let guard = sessions.drain(ReadRange::new(b"c", b"e"));
        assert_eq!(
            guard.wait(Duration::from_millis(50)).await,
            DrainOutcome::Drained
        );
    }

    #[tokio::test]
    async fn drain_rejects_new_reads_only_inside_range() {
        let sessions = TxnSessions::new();
        let id = sessions.begin(1, Duration::from_secs(30));
        let guard = sessions.drain(ReadRange::new(b"c", b"e"));

        assert_eq!(
            sessions.record_read(&id, ReadIntent::Point(b"d"), 10),
            Err(SessionError::Draining)
        );
        assert_eq!(
            sessions.record_read(
                &id,
                ReadIntent::Range {
                    start: b"b",
                    end: b"d",
                },
                10,
            ),
            Err(SessionError::Draining)
        );
        assert_eq!(
            sessions.record_read(&id, ReadIntent::Point(b"a"), 10),
            Ok(1)
        );
        // 被拒绝的读不进读集：读集仍只有 a。
        assert_eq!(sessions.read_set(&id).unwrap().len(), 1);

        drop(guard);
        assert_eq!(
            sessions.record_read(&id, ReadIntent::Point(b"d"), 10),
            Ok(1)
        );
    }

    #[tokio::test]
    async fn nested_drains_release_independently() {
        let sessions = TxnSessions::new();
        let id = sessions.begin(1, Duration::from_secs(30));
        let first = sessions.drain(ReadRange::new(b"c", b"e"));
        let second = sessions.drain(ReadRange::new(b"c", b"e"));

        drop(first);
        assert_eq!(
            sessions.record_read(&id, ReadIntent::Point(b"d"), 10),
            Err(SessionError::Draining)
        );
        drop(second);
        assert_eq!(
            sessions.record_read(&id, ReadIntent::Point(b"d"), 10),
            Ok(1)
        );
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
