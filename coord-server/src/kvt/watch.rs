//! KV-T v2 Watch Coordinator（ADR-0015 D5/D9）。
//!
//! 本模块只做「按 commit_ts 全局有序地读 Commit Log shard」：
//! - 读取上界取本节点的 `local_watermark`（D9 口径，且只反映本地已 apply 的事实），
//!   因此副本滞后时不会跳过记录；
//! - 同一 `commit_ts` 的全部 Region 分片合并为一个原子事件，绝不拆批；
//! - cursor 为 `(shard_id, commit_ts)`，`shard_id = 0` 表示首个 shard 之前。
//!
//! RPC/授权/指标在 `coord.watch.v2` 的 RPC 层（#158）；保留窗口与 GC 归 #157。

use coord_core::error::Error;

use crate::storage::commit_log::{CommitChange, CommitPrevValue, CommitRecord};

/// 单批 change 条数上限（D9；协议同口径）。
pub const WATCH_MAX_CHANGES_PER_BATCH: usize = 1_000;

/// 单批载荷上限（D9；协议同口径）。单个事务不受此上限拆分。
pub const WATCH_MAX_BYTES_PER_BATCH: usize = 1024 * 1024;

/// Watch 读取位点：`commit_ts` 之前的记录已消费；`shard_id = 0` 表示首个 shard 之前。
///
/// `commit_ts = 0` 且 `shard_id != 0` 表示「该 shard 的起点之前」，用于
/// `start_revision` 恰好落在 shard 边界时精确续订。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WatchCursor {
    pub shard_id: u64,
    pub commit_ts: u64,
}

impl WatchCursor {
    /// 首个 shard 之前（最小位点）。
    pub fn beginning() -> Self {
        Self {
            shard_id: 0,
            commit_ts: 0,
        }
    }

    /// 是否尚未进入任何 shard。
    pub fn is_before_first_shard(&self) -> bool {
        self.shard_id == 0
    }
}

/// 事件类型过滤（与 `coord.watch.v2.EventFilter` 对应）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EventKindFilter {
    #[default]
    All,
    PutsOnly,
    DeletesOnly,
}

/// key/range + 事件类型 + 是否携带旧值的过滤条件。
///
/// `key` 与 `range_end` 都空 = 全 keyspace；`range_end` 空 = 单 key；否则
/// `[key, range_end)`。过滤只影响返回的 changes，不影响 cursor 推进——否则
/// 被过滤的事务会被反复扫描。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WatchFilter {
    pub key: Vec<u8>,
    pub range_end: Vec<u8>,
    pub kind: EventKindFilter,
    pub prev_kv: bool,
}

impl WatchFilter {
    /// 全 keyspace、全部事件类型、不带旧值。
    pub fn all() -> Self {
        Self::default()
    }

    /// 反向区间是请求错误；空 `range_end` 是单 key，不是区间。
    pub fn validate(&self) -> Result<(), WatchError> {
        if !self.range_end.is_empty() && self.range_end <= self.key {
            return Err(WatchError::InvalidRequest(format!(
                "watch range_end {:?} must be greater than key {:?}",
                self.range_end, self.key
            )));
        }
        Ok(())
    }

    /// change 是否匹配 key/range 与事件类型。
    pub fn matches(&self, change: &CommitChange) -> bool {
        let kind_matches = match self.kind {
            EventKindFilter::All => true,
            EventKindFilter::PutsOnly => change.value.is_some(),
            EventKindFilter::DeletesOnly => change.value.is_none(),
        };
        if !kind_matches {
            return false;
        }
        self.key_matches(&change.key)
    }

    fn key_matches(&self, key: &[u8]) -> bool {
        if self.key.is_empty() && self.range_end.is_empty() {
            return true;
        }
        if self.range_end.is_empty() {
            return key == self.key;
        }
        key >= self.key.as_slice() && key < self.range_end.as_slice()
    }
}

/// 一个事务的原子事件（同一 `commit_ts` 的全部 Region 分片合并）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchEvent {
    pub commit_ts: u64,
    pub changes: Vec<CommitChange>,
}

impl WatchEvent {
    /// 合并同 ts 的 Region 分片，按过滤条件裁剪。
    ///
    /// 返回 `None` 表示该事务没有匹配的 change；调用方仍必须推进 cursor。
    pub fn from_records(
        commit_ts: u64,
        records: Vec<CommitRecord>,
        filter: &WatchFilter,
    ) -> Option<Self> {
        let mut changes = Vec::new();
        for record in records {
            for change in record.changes {
                if !filter.matches(&change) {
                    continue;
                }
                let mut change = change;
                if !filter.prev_kv {
                    change.prev = None;
                }
                changes.push(change);
            }
        }
        if changes.is_empty() {
            return None;
        }
        Some(Self { commit_ts, changes })
    }

    /// 批大小估算：commit_ts + 每条 change 的 key/value/旧值（含固定开销）。
    fn wire_bytes(&self) -> usize {
        8 + self
            .changes
            .iter()
            .map(|change| {
                change.key.len()
                    + change.value.as_ref().map_or(0, Vec::len)
                    + change.prev.as_ref().map_or(0, |prev| prev.value.len() + 8)
                    + 8
            })
            .sum::<usize>()
    }
}

/// 拉取式批组装：调用方控制节奏，服务端不维护无界订阅缓冲。
///
/// 空批的第一条事件无论多大都接受——原子事务不能因为批上限被拆开。
pub(crate) struct BatchBuilder {
    events: Vec<WatchEvent>,
    changes: usize,
    bytes: usize,
    max_changes: usize,
    max_bytes: usize,
}

impl BatchBuilder {
    pub(crate) fn new(max_changes: usize, max_bytes: usize) -> Self {
        Self {
            events: Vec::new(),
            changes: 0,
            bytes: 0,
            max_changes,
            max_bytes,
        }
    }

    pub(crate) fn try_push(&mut self, event: WatchEvent) -> bool {
        let changes = event.changes.len();
        let bytes = event.wire_bytes();
        if !self.events.is_empty()
            && (self.changes.saturating_add(changes) > self.max_changes
                || self.bytes.saturating_add(bytes) > self.max_bytes)
        {
            return false;
        }
        self.changes = self.changes.saturating_add(changes);
        self.bytes = self.bytes.saturating_add(bytes);
        self.events.push(event);
        true
    }

    pub(crate) fn events(&self) -> &[WatchEvent] {
        &self.events
    }

    pub(crate) fn into_events(self) -> Vec<WatchEvent> {
        self.events
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

/// 一次轮询的结果；`cursor` 是「本批之后」的位置，可直接持久化后重连。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchBatch {
    pub events: Vec<WatchEvent>,
    pub cursor: WatchCursor,
    pub watermark: u64,
    /// cursor 所在 shard 是否已 seal（对调用方是提示，不改变事件语义）。
    pub sealed: bool,
    /// 是否因批上限提前停止；调用方可以立即继续轮询。
    pub has_more: bool,
}

/// 订阅起点：`start_revision` 与 `start_cursor` 互斥。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WatchStart {
    /// 首个期望的 `commit_ts`（含）；`0` = 当前 watermark + 1。
    pub start_revision: u64,
    /// 断点续订的精确位置；`Some` 时忽略 `start_revision`。
    pub start_cursor: Option<WatchCursor>,
}

/// Watch 读取错误。历史窗口不足的语义由保留窗口实现补齐；当前只区分参数、
/// 失效 cursor、暂时不可用与存储错误。
#[derive(Debug, thiserror::Error)]
pub enum WatchError {
    #[error("invalid watch request: {0}")]
    InvalidRequest(String),
    #[error("watch cursor at shard {shard_id} is no longer available")]
    CursorUnavailable { shard_id: u64 },
    #[error("watch is temporarily unavailable: {0}")]
    Unavailable(String),
    #[error(transparent)]
    Storage(#[from] Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn change(key: &[u8], value: Option<&[u8]>) -> CommitChange {
        CommitChange {
            key: key.to_vec(),
            value: value.map(<[u8]>::to_vec),
            prev: Some(CommitPrevValue {
                value: b"old".to_vec(),
                create_revision: 1,
                mod_revision: 2,
                version: 1,
                lease_id: 0,
            }),
        }
    }

    fn record(commit_ts: u64, region_id: u64, key: &[u8]) -> CommitRecord {
        CommitRecord {
            shard_id: 1,
            commit_ts,
            txn_id: format!("txn-{commit_ts}").into_bytes(),
            start_ts: commit_ts.saturating_sub(1),
            region_id,
            txn_bytes: 16,
            changes: vec![change(key, Some(b"v"))],
        }
    }

    fn event_with_changes(count: usize) -> WatchEvent {
        WatchEvent {
            commit_ts: 1,
            changes: (0..count)
                .map(|index| change(format!("k{index}").as_bytes(), Some(b"v")))
                .collect(),
        }
    }

    fn event_with_bytes(value_bytes: usize) -> WatchEvent {
        WatchEvent {
            commit_ts: 2,
            changes: vec![change(b"k", Some(&vec![0u8; value_bytes]))],
        }
    }

    #[test]
    fn filter_matches_exact_key_range_and_event_kinds() {
        let put = change(b"a", Some(b"v"));
        let del = change(b"b", None);

        assert!(WatchFilter::all().matches(&put));
        assert!(WatchFilter {
            key: b"a".to_vec(),
            ..WatchFilter::all()
        }
        .matches(&put));
        assert!(!WatchFilter {
            key: b"a".to_vec(),
            ..WatchFilter::all()
        }
        .matches(&del));

        let range = WatchFilter {
            key: b"a".to_vec(),
            range_end: b"c".to_vec(),
            ..WatchFilter::all()
        };
        assert!(range.matches(&put));
        assert!(range.matches(&del));
        assert!(
            !range.matches(&change(b"c", Some(b"v"))),
            "end is exclusive"
        );

        let puts = WatchFilter {
            kind: EventKindFilter::PutsOnly,
            ..WatchFilter::all()
        };
        assert!(puts.matches(&put));
        assert!(!puts.matches(&del));
        let deletes = WatchFilter {
            kind: EventKindFilter::DeletesOnly,
            ..WatchFilter::all()
        };
        assert!(!deletes.matches(&put));
        assert!(deletes.matches(&del));
    }

    #[test]
    fn filter_rejects_reversed_range() {
        let filter = WatchFilter {
            key: b"z".to_vec(),
            range_end: b"a".to_vec(),
            ..WatchFilter::all()
        };
        assert!(filter.validate().is_err());
    }

    #[test]
    fn event_merges_region_fragments_and_strips_prev_when_disabled() {
        let filter = WatchFilter::all();
        let event =
            WatchEvent::from_records(7, vec![record(7, 1, b"a"), record(7, 2, b"b")], &filter)
                .expect("matching changes");
        assert_eq!(event.commit_ts, 7);
        let keys: Vec<_> = event.changes.iter().map(|c| c.key.clone()).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);
        assert!(event.changes.iter().all(|c| c.prev.is_none()));

        let mut with_prev_filter = filter.clone();
        with_prev_filter.prev_kv = true;
        let with_prev =
            WatchEvent::from_records(7, vec![record(7, 1, b"a")], &with_prev_filter).unwrap();
        assert!(with_prev.changes[0].prev.is_some());
    }

    #[test]
    fn event_with_only_filtered_out_changes_is_none() {
        let filter = WatchFilter {
            key: b"nope".to_vec(),
            ..WatchFilter::all()
        };
        assert!(WatchEvent::from_records(7, vec![record(7, 1, b"a")], &filter).is_none());
    }

    #[test]
    fn batch_keeps_an_oversized_atomic_event_and_stops_before_the_next() {
        let mut builder = BatchBuilder::new(2, 1024);
        let big = event_with_changes(3);
        assert!(
            builder.try_push(big),
            "an oversized single txn is never split"
        );
        assert!(
            !builder.try_push(event_with_changes(1)),
            "next event exceeds the cap"
        );
        assert_eq!(builder.events().len(), 1);
        assert_eq!(builder.events()[0].changes.len(), 3);
    }

    #[test]
    fn batch_respects_the_byte_budget() {
        let mut builder = BatchBuilder::new(1000, 64);
        assert!(builder.try_push(event_with_bytes(32)));
        assert!(!builder.try_push(event_with_bytes(64)));
    }
}
