//! KV-T v2 Watch Coordinator（ADR-0015 D5/D9）。
//!
//! 本模块只做「按 commit_ts 全局有序地读 Commit Log shard」：
//! - 读取上界取本节点的 `local_watermark`（D9 口径，且只反映本地已 apply 的事实），
//!   因此副本滞后时不会跳过记录；
//! - 同一 `commit_ts` 的全部 Region 分片合并为一个原子事件，绝不拆批；
//! - cursor 为 `(shard_id, commit_ts)`，`shard_id = 0` 表示首个 shard 之前。
//!
//! 读取端必须在持有数据面 outbox 的节点上运行（本地是数据 Region leader /
//! 已 apply 决策）；否则 `local_watermark` 会保守地停在最早未完成的决策，
//! 只影响时延、不会跳事件。跨节点服务方式由 RPC 层决定。
//!
//! RPC/授权/指标在 `coord.watch.v2` 的 RPC 层。保留窗口由 region 0 表的
//! `gc_before_ts` 表达：起点/续订落在回收区间时返回 [`WatchError::HistoryUnavailable`]，
//! 不静默跳过被回收的事件。

use std::sync::Arc;

use coord_core::error::Error;

use crate::kvt::commit_log_shards::CommitLogShardManager;
use crate::storage::commit_log::{CommitChange, CommitRecord};
use crate::storage::commit_log_shard::{CommitLogShardMeta, CommitLogShardTable};

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

impl std::fmt::Display for WatchCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "shard {}, commit_ts {}", self.shard_id, self.commit_ts)
    }
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
        // cutover 窗口内同一决策可能先在父 Region apply、再在子 Region 重放，
        // 两条 CommitRecord 的 region_id 不同但 key 相同。按 record 顺序去重：
        // 同一 record 内的多次变更（同事务 put+delete 同 key）不是重放，保留。
        let mut seen_keys = std::collections::BTreeSet::new();
        for record in records {
            let mut record_keys = Vec::new();
            for change in record.changes {
                if !filter.matches(&change) {
                    continue;
                }
                if seen_keys.contains(&change.key) {
                    continue;
                }
                record_keys.push(change.key.clone());
                let mut change = change;
                if !filter.prev_kv {
                    change.prev = None;
                }
                changes.push(change);
            }
            seen_keys.extend(record_keys);
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

    #[cfg(test)]
    pub(crate) fn events(&self) -> &[WatchEvent] {
        &self.events
    }

    pub(crate) fn into_events(self) -> Vec<WatchEvent> {
        self.events
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
    /// 当前最老可读位点（`gc_before_ts == 0` 时为 [`WatchCursor::beginning`]）。
    ///
    /// 供 RPC 层在 `HISTORY_UNAVAILABLE` 时回显/诊断；客户端只需全量重同步。
    pub oldest_available: WatchCursor,
}

/// 订阅起点：`start_revision` 与 `start_cursor` 互斥。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WatchStart {
    /// 首个期望的 `commit_ts`（含）；`0` = 当前 watermark + 1。
    pub start_revision: u64,
    /// 断点续订的精确位置；`Some` 时不得再设置 `start_revision`。
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
    /// 起点/续订位置已被保留窗口回收；`requested` 是第一个不可读的 `commit_ts`。
    ///
    /// 不可重试：调用方必须全量重同步（`coord.watch.v2` 映射为 `OUT_OF_RANGE` +
    /// `HISTORY_UNAVAILABLE`）。
    #[error(
        "watch history unavailable: commit_ts {requested} is before the oldest retained position ({oldest_available})"
    )]
    HistoryUnavailable {
        requested: u64,
        oldest_available: WatchCursor,
    },
    #[error("watch is temporarily unavailable: {0}")]
    Unavailable(String),
    #[error(transparent)]
    Storage(#[from] Error),
}

/// Watch 读取端：只依赖 Commit Log shard 组，不持有订阅状态。
///
/// 读取是拉模型，每次 `poll` 最多组装一批；调用方（RPC 流）负责节奏与
/// 有界通道，服务端不维护无界缓冲。
pub struct WatchCoordinator {
    shards: Arc<CommitLogShardManager>,
    max_changes: usize,
    max_bytes: usize,
}

impl WatchCoordinator {
    pub fn new(shards: Arc<CommitLogShardManager>) -> Self {
        Self {
            shards,
            max_changes: WATCH_MAX_CHANGES_PER_BATCH,
            max_bytes: WATCH_MAX_BYTES_PER_BATCH,
        }
    }

    /// 覆盖批上限（测试/运维调参；`0` 视为 1，避免永不返回事件）。
    pub fn with_limits(mut self, max_changes: usize, max_bytes: usize) -> Self {
        self.max_changes = max_changes.max(1);
        self.max_bytes = max_bytes.max(1);
        self
    }

    /// 解析订阅起点。
    ///
    /// `start_revision = 0` 表示「当前 watermark + 1」；`> 0` 含该 revision。
    /// 显式 cursor 与 start_revision 互斥，避免两个起点产生歧义。
    pub async fn start(&self, start: WatchStart) -> Result<WatchCursor, WatchError> {
        validate_start(&start)?;
        let table = self.shards.table()?;
        let cursor = if let Some(cursor) = start.start_cursor {
            resolve_position(&table, cursor)?;
            cursor
        } else if start.start_revision == 0 {
            let watermark = self
                .shards
                .local_watermark()
                .await
                .map_err(WatchError::Unavailable)?
                .watermark;
            position_before(&table, watermark)
        } else {
            position_before(&table, start.start_revision - 1)
        };
        ensure_available(&table, cursor)?;
        Ok(cursor)
    }

    /// 读取一批事件；`cursor` 是上一批返回的位置。
    ///
    /// 上界取本地 watermark：它只由本地已 apply 的决策/已 ship 的记录计算，
    /// 副本再滞后也不会把尚未到达本地的记录当成「不存在」而跳过。
    pub async fn poll(
        &self,
        cursor: WatchCursor,
        filter: &WatchFilter,
    ) -> Result<WatchBatch, WatchError> {
        let table = self.shards.table()?;
        match self.poll_with_table(&table, cursor, filter).await {
            Ok(batch) => Ok(batch),
            Err(err) => {
                // GC 可能在可用性检查与 shard 读取之间推进：此时正确的语义是
                // HISTORY_UNAVAILABLE，而不是把内部错误泄露给调用方。
                if let Ok(fresh) = self.shards.table() {
                    ensure_available(&fresh, cursor)?;
                }
                Err(err)
            }
        }
    }

    async fn poll_with_table(
        &self,
        table: &CommitLogShardTable,
        cursor: WatchCursor,
        filter: &WatchFilter,
    ) -> Result<WatchBatch, WatchError> {
        filter.validate()?;
        ensure_available(table, cursor)?;
        let watermark = self
            .shards
            .local_watermark()
            .await
            .map_err(WatchError::Unavailable)?
            .watermark;
        if table.shards.is_empty() {
            return Ok(WatchBatch {
                events: Vec::new(),
                cursor,
                watermark,
                sealed: false,
                has_more: false,
                oldest_available: oldest_available(table),
            });
        }

        let (mut index, mut after) = resolve_position(table, cursor)?;
        let mut builder = BatchBuilder::new(self.max_changes, self.max_bytes);
        let mut stopped_for_limit = false;

        loop {
            let meta = &table.shards[index];
            if after < meta.start_ts.saturating_sub(1) {
                after = meta.start_ts.saturating_sub(1);
            }
            let bound = read_bound(meta, watermark);
            if after < bound {
                match self
                    .scan_shard(meta, after, bound, filter, &mut builder)
                    .await?
                {
                    ScanOutcome::Done(next_after) => after = next_after,
                    ScanOutcome::Full(next_after) => {
                        after = next_after;
                        stopped_for_limit = true;
                        break;
                    }
                }
            }

            // 跨 shard 只在 sealed 且 watermark 越过 end_ts 后发生：此时区间内
            // 不会再有迟到记录，下一 shard 的最小 ts 也不可能更小。
            if !stopped_for_limit && index + 1 < table.shards.len() && may_cross(meta, watermark) {
                index += 1;
                after = table.shards[index].start_ts.saturating_sub(1);
                continue;
            }
            break;
        }

        let meta = &table.shards[index];
        let cursor = if after < meta.start_ts {
            WatchCursor {
                shard_id: meta.shard_id,
                commit_ts: 0,
            }
        } else {
            WatchCursor {
                shard_id: meta.shard_id,
                commit_ts: after,
            }
        };
        Ok(WatchBatch {
            events: builder.into_events(),
            cursor,
            watermark,
            sealed: meta.sealed_at_ms != 0,
            has_more: stopped_for_limit,
            oldest_available: oldest_available(table),
        })
    }

    /// 读取单个 shard 内 `(after, bound]` 的事件。
    ///
    /// 先取「下一个 ts」再按 ts 取整组：`records_after` 的 limit 可能切在同一
    /// 事务的 Region 分片中间，只有 `records_at` 能保证原子事件完整。
    async fn scan_shard(
        &self,
        meta: &CommitLogShardMeta,
        mut after: u64,
        bound: u64,
        filter: &WatchFilter,
        builder: &mut BatchBuilder,
    ) -> Result<ScanOutcome, WatchError> {
        loop {
            let rows = self.shards.records_after(meta.shard_id, after, 1).await?;
            let Some(first) = rows.first() else {
                return Ok(ScanOutcome::Done(after));
            };
            let commit_ts = first.commit_ts;
            if commit_ts > bound {
                return Ok(ScanOutcome::Done(after));
            }
            let records = self.shards.records_at(meta.shard_id, commit_ts).await?;
            if records.is_empty() {
                return Err(WatchError::Unavailable(format!(
                    "commit log shard {} has no records for commit_ts {commit_ts}",
                    meta.shard_id
                )));
            }
            if let Some(event) = WatchEvent::from_records(commit_ts, records, filter) {
                if !builder.try_push(event) {
                    // 批满：cursor 停在这条事件之前，下轮重新读取它。
                    return Ok(ScanOutcome::Full(after));
                }
            }
            after = commit_ts;
        }
    }
}

/// 单 shard 读取的停止原因：正常读到上界，或因批上限停在某事件之前。
enum ScanOutcome {
    Done(u64),
    Full(u64),
}

/// shard 内可安全读取的最大 `commit_ts`（未定界 shard 只受 watermark 限制）。
fn read_bound(meta: &CommitLogShardMeta, watermark: u64) -> u64 {
    if meta.end_ts == 0 {
        watermark
    } else {
        watermark.min(meta.end_ts - 1)
    }
}

/// 离开当前 shard 的条件：已定界、已 seal，且 watermark 越过 `end_ts`。
fn may_cross(meta: &CommitLogShardMeta, watermark: u64) -> bool {
    meta.end_ts != 0 && meta.sealed_at_ms != 0 && watermark >= meta.end_ts
}

/// 历史可用性：第一个未读事件必须仍在保留窗口内。
///
/// `commit_ts = 0`（含跨 shard 后的 `{shard_id, 0}`）表示「在该 shard 起点之前」，
/// 第一个未读事件是 `start_ts` 而不是 `1`；直接按 `commit_ts + 1` 判定会把一个
/// 已追平、停在 shard 起点的订阅误判成超出窗口。过滤条件不参与判定——被过滤掉
/// 的事件同样必须先可读，否则 cursor 无法证明「消费过该段历史」。
fn ensure_available(table: &CommitLogShardTable, cursor: WatchCursor) -> Result<(), WatchError> {
    let first_unread = match table
        .shards
        .iter()
        .find(|meta| meta.shard_id == cursor.shard_id)
    {
        Some(meta) if cursor.commit_ts < meta.start_ts => meta.start_ts,
        _ => cursor.commit_ts.saturating_add(1),
    };
    if table.gc_before_ts == 0 || first_unread >= table.gc_before_ts {
        return Ok(());
    }
    Err(WatchError::HistoryUnavailable {
        requested: first_unread,
        oldest_available: oldest_available(table),
    })
}

/// 最老可续订位点：回收水位前一个 `commit_ts`（`0` = 尚未回收）。
///
/// 返回的 cursor 满足 `commit_ts + 1 == gc_before_ts`，因此可以直接交给
/// [`WatchCoordinator::poll`] 从最老保留事件继续读取。
pub fn oldest_available(table: &CommitLogShardTable) -> WatchCursor {
    if table.gc_before_ts == 0 {
        return WatchCursor::beginning();
    }
    position_before(table, table.gc_before_ts.saturating_sub(1))
}

/// `start_revision` 与 `start_cursor` 同时设置是请求歧义，不是「后者优先」。
fn validate_start(start: &WatchStart) -> Result<(), WatchError> {
    if start.start_cursor.is_some() && start.start_revision != 0 {
        return Err(WatchError::InvalidRequest(
            "start_revision and start_cursor are mutually exclusive".to_string(),
        ));
    }
    Ok(())
}

/// 把「已消费到 `ts`」换算成 `(shard, ts)`；表空时返回最小位点。
fn position_before(table: &CommitLogShardTable, ts: u64) -> WatchCursor {
    if ts == 0 || table.shards.is_empty() {
        return WatchCursor::beginning();
    }
    let Some(meta) = table.covering(ts) else {
        return WatchCursor::beginning();
    };
    if ts < meta.start_ts {
        WatchCursor {
            shard_id: meta.shard_id,
            commit_ts: 0,
        }
    } else {
        WatchCursor {
            shard_id: meta.shard_id,
            commit_ts: ts,
        }
    }
}

/// 校验 cursor 并把 shard_id 解析成表下标；返回 `(index, after_ts)`。
///
/// `after_ts` 是「已消费到的 commit_ts」，读取时严格取更大值；shard 起点之前
/// 的 cursor 规整为 `start_ts - 1`，避免把 `0` 误当成真实时间戳。
fn resolve_position(
    table: &CommitLogShardTable,
    cursor: WatchCursor,
) -> Result<(usize, u64), WatchError> {
    if table.shards.is_empty() {
        if cursor.shard_id == 0 {
            return Ok((0, cursor.commit_ts));
        }
        return Err(WatchError::CursorUnavailable {
            shard_id: cursor.shard_id,
        });
    }
    if cursor.shard_id == 0 {
        return Ok((0, cursor.commit_ts));
    }
    let Some(index) = table
        .shards
        .iter()
        .position(|meta| meta.shard_id == cursor.shard_id)
    else {
        return Err(WatchError::CursorUnavailable {
            shard_id: cursor.shard_id,
        });
    };
    let meta = &table.shards[index];
    if cursor.commit_ts == 0 || cursor.commit_ts < meta.start_ts {
        return Ok((index, meta.start_ts.saturating_sub(1)));
    }
    if meta.end_ts != 0 && cursor.commit_ts >= meta.end_ts {
        // 续订 cursor 已越过该 shard 的末尾：定位到覆盖该 ts 的 shard。
        if let Some((cover_index, _)) = table
            .shards
            .iter()
            .enumerate()
            .find(|(_, candidate)| candidate.covers(cursor.commit_ts))
        {
            return Ok((cover_index, cursor.commit_ts));
        }
    }
    Ok((index, cursor.commit_ts))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::commit_log::CommitPrevValue;

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
    fn event_dedups_cross_region_replays() {
        let filter = WatchFilter::all();
        // cutover 窗口内同一决策先在父 Region apply、再在子 Region 重放：
        // 两条记录携带同一个 key，只能产出一个 change。
        let event = WatchEvent::from_records(
            7,
            vec![
                record(7, 1, b"replayed"),
                record(7, 3, b"replayed"),
                record(7, 3, b"other"),
            ],
            &filter,
        )
        .expect("matching changes");
        let keys: Vec<_> = event.changes.iter().map(|c| c.key.clone()).collect();
        assert_eq!(
            keys,
            vec![b"replayed".to_vec(), b"other".to_vec()],
            "same key from a replayed region fragment must collapse to one change"
        );

        // 同一 record 内的多次变更（如 put+delete 同 key）不是重放，必须保留。
        let mut same_record = record(7, 1, b"twice");
        same_record.changes.push(change(b"twice", None));
        let event =
            WatchEvent::from_records(7, vec![same_record], &filter).expect("matching changes");
        assert_eq!(event.changes.len(), 2);
        assert_eq!(event.changes[0].key, b"twice");
        assert_eq!(event.changes[1].key, b"twice");
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

    fn shard(id: u64, start: u64, end: u64) -> CommitLogShardMeta {
        CommitLogShardMeta {
            shard_id: id,
            start_ts: start,
            end_ts: end,
            opened_at_ms: 1,
            ended_at_ms: if end == 0 { 0 } else { 2 },
            sealed_at_ms: 0,
        }
    }

    fn two_shards() -> CommitLogShardTable {
        CommitLogShardTable {
            shards: vec![shard(1, 1, 101), shard(2, 101, 0)],
            gc_before_ts: 0,
        }
    }

    #[test]
    fn start_revision_resolves_to_the_previous_position() {
        let table = two_shards();
        assert_eq!(position_before(&table, 0), WatchCursor::beginning());
        assert_eq!(
            position_before(&table, 1),
            WatchCursor {
                shard_id: 1,
                commit_ts: 1
            }
        );
        assert_eq!(
            position_before(&table, 50),
            WatchCursor {
                shard_id: 1,
                commit_ts: 50
            }
        );
        assert_eq!(
            position_before(&table, 100),
            WatchCursor {
                shard_id: 1,
                commit_ts: 100
            }
        );
        assert_eq!(
            position_before(&table, 101),
            WatchCursor {
                shard_id: 2,
                commit_ts: 101
            }
        );
        assert_eq!(
            position_before(&CommitLogShardTable::default(), 42),
            WatchCursor::beginning(),
            "an empty table has no position to point at yet"
        );
    }

    #[test]
    fn resolve_position_crosses_a_consumed_shard_and_rejects_unknown_shards() {
        let table = two_shards();
        assert_eq!(
            resolve_position(
                &table,
                WatchCursor {
                    shard_id: 1,
                    commit_ts: 100
                }
            )
            .unwrap(),
            (0, 100)
        );
        assert_eq!(
            resolve_position(
                &table,
                WatchCursor {
                    shard_id: 1,
                    commit_ts: 101
                }
            )
            .unwrap(),
            (1, 101),
            "a cursor past the shard end resumes in the covering shard"
        );
        assert_eq!(
            resolve_position(
                &table,
                WatchCursor {
                    shard_id: 2,
                    commit_ts: 0
                }
            )
            .unwrap(),
            (1, 100),
            "zero means the start of that shard"
        );
        assert!(resolve_position(
            &table,
            WatchCursor {
                shard_id: 9,
                commit_ts: 0
            }
        )
        .is_err());
        assert_eq!(
            resolve_position(&CommitLogShardTable::default(), WatchCursor::beginning()).unwrap(),
            (0, 0)
        );
    }

    #[test]
    fn shard_read_bound_is_the_watermark_or_the_shard_end() {
        let open = shard(1, 1, 0);
        assert_eq!(read_bound(&open, 50), 50);
        let bounded = shard(2, 101, 201);
        assert_eq!(read_bound(&bounded, 500), 200);
        assert_eq!(read_bound(&bounded, 150), 150);
    }

    #[test]
    fn crossing_requires_a_sealed_shard_below_the_watermark() {
        let bounded = shard(1, 1, 101);
        assert!(!may_cross(&bounded, 100), "not sealed yet");
        let sealed = CommitLogShardMeta {
            sealed_at_ms: 1,
            ..bounded
        };
        assert!(!may_cross(&sealed, 100), "watermark must reach end_ts");
        assert!(may_cross(&sealed, 101));
        assert!(
            !may_cross(&shard(2, 101, 0), 10_000),
            "the latest shard is unbounded"
        );
    }

    #[test]
    fn history_before_the_gc_boundary_is_unavailable() {
        let table = CommitLogShardTable {
            shards: vec![shard(1, 1, 101), shard(2, 101, 0)],
            gc_before_ts: 101,
        };
        assert!(matches!(
            ensure_available(&table, WatchCursor::beginning()),
            Err(WatchError::HistoryUnavailable { requested: 1, .. })
        ));
        let err = ensure_available(
            &table,
            WatchCursor {
                shard_id: 1,
                commit_ts: 99,
            },
        )
        .unwrap_err();
        match err {
            WatchError::HistoryUnavailable {
                requested,
                oldest_available,
            } => {
                assert_eq!(requested, 100);
                assert_eq!(
                    oldest_available,
                    WatchCursor {
                        shard_id: 1,
                        commit_ts: 100
                    }
                );
            }
            other => panic!("unexpected error: {other}"),
        }
        assert!(ensure_available(
            &table,
            WatchCursor {
                shard_id: 1,
                commit_ts: 100
            }
        )
        .is_ok());
        assert!(ensure_available(
            &table,
            WatchCursor {
                shard_id: 2,
                commit_ts: 100
            }
        )
        .is_ok());
    }

    #[test]
    fn a_cursor_parked_at_a_shard_start_is_not_before_the_window() {
        let table = CommitLogShardTable {
            shards: vec![shard(1, 1, 101), shard(2, 101, 0)],
            gc_before_ts: 101,
        };
        assert!(
            ensure_available(
                &table,
                WatchCursor {
                    shard_id: 2,
                    commit_ts: 0
                }
            )
            .is_ok(),
            "crossing into shard 2 yields a start cursor, not history from ts 1"
        );
        assert!(ensure_available(
            &table,
            WatchCursor {
                shard_id: 1,
                commit_ts: 0
            }
        )
        .is_err());
    }

    #[test]
    fn oldest_available_points_at_the_last_reclaimed_ts() {
        assert_eq!(
            oldest_available(&CommitLogShardTable::default()),
            WatchCursor::beginning()
        );
        let table = CommitLogShardTable {
            shards: vec![shard(1, 1, 101), shard(2, 101, 0)],
            gc_before_ts: 101,
        };
        assert_eq!(
            oldest_available(&table),
            WatchCursor {
                shard_id: 1,
                commit_ts: 100
            }
        );
    }

    #[test]
    fn start_rejects_dual_positioning() {
        let dual = WatchStart {
            start_revision: 5,
            start_cursor: Some(WatchCursor::beginning()),
        };
        assert!(matches!(
            validate_start(&dual),
            Err(WatchError::InvalidRequest(_))
        ));
        assert!(validate_start(&WatchStart {
            start_revision: 0,
            start_cursor: Some(WatchCursor::beginning()),
        })
        .is_ok());
    }
}
