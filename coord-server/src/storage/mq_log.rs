//! MQ v2 段日志状态机（ADR-0016 D4/D7/D8/D9，M1-2）。
//!
//! 语义单一归属：`docs/adr/0016-mq-v2.md`；工作项与验收见 MQ v2 设计文档
//! 的 §6/M1（`docs/superpowers/specs/` 下的 mq-v2-design）。
//!
//! 本模块只做**确定性 apply**：offset 由 apply 分配，时间由命令携带的 `now_ms`
//! 决定，上限/配额由命令携带（重放节点不读本地配置）。段数据、offset、幂等索引、
//! DLQ 与 shard 的 applied 水位写在同一 redb 事务里——崩溃后要么全部可见，要么
//! 全部不可见。
//!
//! 存储布局（每个日志分片是独立 Region 的 `MvccStorage`，表之间不共享 key 空间）：
//!
//! | 表 | key | value |
//! |:--|:--|:--|
//! | `mq_log` | `topic_id(8) ++ partition(4) ++ offset(8)` | 信封(postcard(`MqStoredRecord`)) |
//! | `mq_idem` | `topic_id(8) ++ partition(4) ++ idempotency_key` | 信封(postcard(`MqIdemEntry`)) |
//! | `mq_dlq` | `topic_id(8) ++ partition(4) ++ dlq_offset(8)` | 信封(postcard(`MqDlqRecord`)) |
//! | `mq_meta` | `'S' ++ shard_id(8)` / `'P' ++ topic(8) ++ partition(4)` / `'T' ++ topic(8)` | 信封(postcard(meta)) |
//!
//! 记账口径（ADR-0016 D7）：消息按**实际写入字节**计费；幂等索引按条目字节计费；
//! DLQ 独立计费且在主动保留期内不因 tombstone 缩短原段。配额按
//! `[mq_v2]`/topic 配置的 `max_bytes` 在本分片内按 topic 汇总后严格检查；跨分片的
//! topic 总量需要额外协调（M1-4 的分区放置与 M6 容量报告，见设计 §8.4）。

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use coord_core::error::{Error, Result};
use coord_core::storage::{ReadTx, StorageBackend, WriteTx};

use crate::storage::envelope;
use crate::storage::mvcc::{AppliedLogId, MvccStorage, META_LAST_APPLIED, TABLE_META};

/// 主日志段表。
pub const TABLE_MQ_LOG: &str = "mq_log";
/// 幂等索引表。
pub const TABLE_MQ_IDEM: &str = "mq_idem";
/// 死信表。
pub const TABLE_MQ_DLQ: &str = "mq_dlq";
/// 分片/分区/topic 元数据表。
pub const TABLE_MQ_META: &str = "mq_meta";

/// 元数据 key 的命名空间标签。
const META_SHARD: u8 = b'S';
const META_PARTITION: u8 = b'P';
const META_TOPIC: u8 = b'T';

/// 已提交消息（主日志与 DLQ 复用的记录形态）。
///
/// `moved_to_dlq = Some(dlq_offset)` 时本行是 tombstone：`Poll` 跳过；原消息在
/// DLQ 表里完整保留（含 reason/detail）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqStoredRecord {
    pub offset: u64,
    pub timestamp_ms: i64,
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
    pub headers: BTreeMap<String, String>,
    pub producer: String,
    pub moved_to_dlq: Option<u64>,
}

impl MqStoredRecord {
    /// 主日志可见消息（未被移入 DLQ）。
    pub fn is_visible(&self) -> bool {
        self.moved_to_dlq.is_none()
    }
}

/// 幂等索引条目：首次分配的 offset + 该消息的记账字节（回收时扣减）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqIdemEntry {
    pub offset: u64,
    pub bytes: u64,
}

/// DLQ 记录：保留原 topic/partition/offset 与移入原因。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqDlqRecord {
    pub dlq_offset: u64,
    pub topic_id: u64,
    pub partition: u32,
    /// 原主日志 offset（`PollDlq` 响应里的 `Message.offset`）。
    pub offset: u64,
    pub timestamp_ms: i64,
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
    pub headers: BTreeMap<String, String>,
    pub producer: String,
    pub reason: String,
    pub detail: String,
    pub moved_at_ms: i64,
}

/// 一个分片的状态机元数据（apply 水位 + 最近一次 append 的响应）。
///
/// 保存最近响应是为了让**同一条日志的重复 apply**返回相同的 offset（崩溃恢复/
/// 快照重放不会把同一条命令追加两次）。更早的重复 apply 只可能是快照之后不会
/// 发生的历史条目，返回最近一次响应（不写任何状态）。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqShardMeta {
    pub applied_index: u64,
    pub last_append: Option<MqAppendBatchResponse>,
}

/// 单个分区的段/offset/记账状态。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqPartitionMeta {
    pub next_offset: u64,
    pub earliest_offset: u64,
    pub segments: Vec<MqSegmentMeta>,
    pub next_segment_id: u64,
    pub next_dlq_offset: u64,
    pub log_bytes: u64,
    pub dlq_bytes: u64,
    pub idem_bytes: u64,
}

/// 分区的可读边界：`earliest` = 最早仍可读的 offset，`next` = 下一条将被分配的 offset。
///
/// `Poll` 用 `next` 判空/长轮询，用 `earliest` 把低于窗口的请求映射成
/// `OFFSET_OUT_OF_RANGE`（不静默 clamp，ADR-0016 D5）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MqPartitionBounds {
    pub earliest_offset: u64,
    pub next_offset: u64,
}

/// 一个段：`[start_offset, end_offset)`，按 append 顺序封口。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqSegmentMeta {
    pub id: u64,
    pub start_offset: u64,
    pub end_offset: u64,
    pub bytes: u64,
    pub opened_at_ms: i64,
}

/// 本分片内按 topic 汇总的记账（配额检查用）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqTopicUsage {
    pub log_bytes: u64,
    pub dlq_bytes: u64,
    pub idem_bytes: u64,
}

impl MqTopicUsage {
    pub fn total(&self) -> u64 {
        self.log_bytes
            .saturating_add(self.dlq_bytes)
            .saturating_add(self.idem_bytes)
    }
}

// ──── 命令载荷（写进 raft 日志；重放时不得依赖本地配置/时钟）────

/// 一条 `Command::MqAppendBatch`：同一分片内多个请求各自独立成败。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqAppendBatch {
    pub shard_id: u32,
    /// 由提议方取一次墙钟并写进日志；apply 不读时钟。
    pub now_ms: i64,
    pub requests: Vec<MqAppendRequest>,
}

/// 单个分区的原子写请求（`PublishBatch` 的原子单位）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqAppendRequest {
    pub topic_id: u64,
    pub partition: u32,
    /// topic 级覆盖（0 → 使用命令里携带的默认值，由 M2 handler 展开）。
    pub max_message_bytes: u32,
    /// 0 = 不限。
    pub max_bytes: u64,
    pub segment_bytes: u64,
    pub entries: Vec<MqAppendEntry>,
}

/// 一条待追加消息。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqAppendEntry {
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
    pub idempotency_key: String,
    pub headers: BTreeMap<String, String>,
    pub producer: String,
}

/// `Command::MqTruncate`：按整段回收（至少保留最新段）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqTruncate {
    pub shard_id: u32,
    pub topic_id: u64,
    pub partition: u32,
    /// 回收所有 `end_offset <= up_to_offset` 的段（但不动最后一个段）。
    pub up_to_offset: u64,
}

/// `Command::MqDropPartition`：物理回收一个分区的全部数据（DeleteTopic 的回收路径）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqDropPartition {
    pub shard_id: u32,
    pub topic_id: u64,
    pub partition: u32,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqDropPartitionResult {
    pub records_removed: u64,
    pub dlq_records_removed: u64,
    pub idem_entries_removed: u64,
    pub bytes_freed: u64,
}

/// `Command::MqMoveToDlq`：复制到 DLQ 并在主日志写 tombstone。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqMoveToDlq {
    pub shard_id: u32,
    pub topic_id: u64,
    pub partition: u32,
    pub offset: u64,
    pub reason: String,
    pub detail: String,
    pub now_ms: i64,
}

// ──── apply 结果（与请求一一对应，顺序一致）────

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqAppendBatchResponse {
    pub requests: Vec<MqAppendOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MqAppendOutcome {
    Applied(MqAppended),
    Rejected(MqAppendRejection),
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqAppended {
    pub entries: Vec<MqAppendEntryResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqAppendEntryResult {
    pub offset: u64,
    pub timestamp_ms: i64,
    pub deduplicated: bool,
}

/// 整批拒绝原因（用户级拒绝不是 apply 错误，D11.5 同口径）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MqAppendRejection {
    /// 第 `entry` 条编码后超过 `limit` 字节。
    MessageTooLarge { entry: usize, size: u64, limit: u64 },
    /// 段 + DLQ + 幂等索引超过 topic 配额。
    QuotaExceeded {
        used: u64,
        requested: u64,
        limit: u64,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqTruncateResult {
    pub dropped_segments: u64,
    pub dropped_records: u64,
    pub dropped_bytes: u64,
    pub earliest_offset: u64,
    pub next_offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MqMoveToDlqOutcome {
    Moved {
        dlq_offset: u64,
    },
    /// 重复调用幂等：返回首次写入的 DLQ 位点。
    AlreadyMoved {
        dlq_offset: u64,
    },
    /// 主日志没有这个 offset（调用方映射 `OFFSET_OUT_OF_RANGE`）。
    Missing,
}

// ──── key 编码 ────

fn partition_prefix(topic_id: u64, partition: u32) -> [u8; 12] {
    let mut key = [0u8; 12];
    key[..8].copy_from_slice(&topic_id.to_be_bytes());
    key[8..].copy_from_slice(&partition.to_be_bytes());
    key
}

fn record_key(topic_id: u64, partition: u32, offset: u64) -> [u8; 20] {
    let mut key = [0u8; 20];
    key[..12].copy_from_slice(&partition_prefix(topic_id, partition));
    key[12..].copy_from_slice(&offset.to_be_bytes());
    key
}

fn idem_key(topic_id: u64, partition: u32, idempotency_key: &str) -> Vec<u8> {
    let mut key = partition_prefix(topic_id, partition).to_vec();
    key.extend_from_slice(idempotency_key.as_bytes());
    key
}

fn shard_meta_key(shard_id: u32) -> [u8; 9] {
    let mut key = [0u8; 9];
    key[0] = META_SHARD;
    key[1..].copy_from_slice(&(u64::from(shard_id)).to_be_bytes());
    key
}

fn partition_meta_key(topic_id: u64, partition: u32) -> [u8; 13] {
    let mut key = [0u8; 13];
    key[0] = META_PARTITION;
    key[1..].copy_from_slice(&partition_prefix(topic_id, partition));
    key
}

fn topic_usage_key(topic_id: u64) -> [u8; 9] {
    let mut key = [0u8; 9];
    key[0] = META_TOPIC;
    key[1..].copy_from_slice(&topic_id.to_be_bytes());
    key
}

// ──── 编解码 ────

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    envelope::encode(value).map_err(|err| Error::Storage(format!("encode mq log row: {err}")))
}

fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T> {
    envelope::decode(bytes).map_err(|err| Error::Storage(format!("decode mq log row: {err}")))
}

fn read_meta<T: for<'de> Deserialize<'de>>(
    tx: &dyn ReadTx,
    table: &str,
    key: &[u8],
) -> Result<Option<T>> {
    tx.get(table, key)?.map(|bytes| decode(&bytes)).transpose()
}

fn write_meta<T: Serialize>(tx: &mut dyn WriteTx, key: &[u8], value: &T) -> Result<()> {
    let bytes = encode(value)?;
    tx.insert(TABLE_MQ_META, key, &bytes)
}

fn read_record(
    tx: &dyn ReadTx,
    topic_id: u64,
    partition: u32,
    offset: u64,
) -> Result<Option<MqStoredRecord>> {
    tx.get(TABLE_MQ_LOG, &record_key(topic_id, partition, offset))?
        .map(|bytes| decode(&bytes))
        .transpose()
}

// ──── MvccStorage 上的 apply 入口 ────

impl<B: StorageBackend> MvccStorage<B> {
    /// 追加一批请求（`Command::MqAppendBatch` 的 apply）。
    ///
    /// 每个请求独立成败：超限/超配额只拒绝该请求，不影响同一 Raft 条目里的其他
    /// 请求（ADR-0016 D4）。重复 apply 同一条日志返回首次响应，不重复追加。
    pub fn apply_mq_append_batch(
        &self,
        op: &MqAppendBatch,
        applied: AppliedLogId,
    ) -> Result<MqAppendBatchResponse> {
        self.backend().write(|tx| {
            let shard_key = shard_meta_key(op.shard_id);
            let mut shard =
                read_meta::<MqShardMeta>(tx, TABLE_MQ_META, &shard_key)?.unwrap_or_default();
            if applied.index <= shard.applied_index {
                return Ok(shard.last_append.unwrap_or_default());
            }

            let mut response = MqAppendBatchResponse {
                requests: Vec::with_capacity(op.requests.len()),
            };
            for request in &op.requests {
                response
                    .requests
                    .push(apply_append_request(tx, op.now_ms, request)?);
            }

            shard.applied_index = applied.index;
            shard.last_append = Some(response.clone());
            write_meta(tx, &shard_key, &shard)?;
            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;
            Ok(response)
        })
    }

    /// 按整段回收主日志（`Command::MqTruncate` 的 apply）。至少保留最新段。
    pub fn apply_mq_truncate(
        &self,
        op: &MqTruncate,
        applied: AppliedLogId,
    ) -> Result<MqTruncateResult> {
        self.backend().write(|tx| {
            let mut shard = read_shard_meta(tx, op.shard_id)?;
            if applied.index <= shard.applied_index {
                let meta = read_partition_meta(tx, op.topic_id, op.partition)?;
                return Ok(MqTruncateResult {
                    earliest_offset: meta.earliest_offset,
                    next_offset: meta.next_offset,
                    ..MqTruncateResult::default()
                });
            }

            let mut meta = read_partition_meta(tx, op.topic_id, op.partition)?;
            let mut result = MqTruncateResult {
                earliest_offset: meta.earliest_offset,
                next_offset: meta.next_offset,
                ..MqTruncateResult::default()
            };

            // 至少保留最新段：只在还有后继段时允许删除。
            let mut drop_count = 0usize;
            while drop_count + 1 < meta.segments.len()
                && meta.segments[drop_count].end_offset <= op.up_to_offset
            {
                drop_count += 1;
            }

            if drop_count > 0 {
                let mut pruned_idem = std::collections::BTreeMap::<Vec<u8>, u64>::new();
                for segment in meta.segments.drain(..drop_count) {
                    result.dropped_segments += 1;
                    result.dropped_bytes = result.dropped_bytes.saturating_add(segment.bytes);
                    let rows = tx.iter_range(
                        TABLE_MQ_LOG,
                        &record_key(op.topic_id, op.partition, segment.start_offset),
                        &record_key(op.topic_id, op.partition, segment.end_offset),
                    )?;
                    for (key, _) in rows {
                        tx.remove(TABLE_MQ_LOG, &key)?;
                        result.dropped_records += 1;
                    }
                }
                // 幂等窗口 = 保留期：被回收 offset 的索引条目一并回收。
                let earliest = meta
                    .segments
                    .first()
                    .map(|segment| segment.start_offset)
                    .unwrap_or(meta.next_offset);
                for (key, value) in
                    tx.iter_prefix(TABLE_MQ_IDEM, &partition_prefix(op.topic_id, op.partition))?
                {
                    let entry: MqIdemEntry = decode(&value)?;
                    if entry.offset < earliest {
                        pruned_idem.insert(key, entry.bytes);
                    }
                }
                let mut pruned_bytes = 0u64;
                for (key, bytes) in &pruned_idem {
                    tx.remove(TABLE_MQ_IDEM, key)?;
                    pruned_bytes = pruned_bytes.saturating_add(*bytes);
                }
                meta.idem_bytes = meta.idem_bytes.saturating_sub(pruned_bytes);
                meta.earliest_offset = earliest;
                meta.log_bytes = meta.log_bytes.saturating_sub(result.dropped_bytes);

                let usage_key = topic_usage_key(op.topic_id);
                let mut usage =
                    read_meta::<MqTopicUsage>(tx, TABLE_MQ_META, &usage_key)?.unwrap_or_default();
                usage.log_bytes = usage.log_bytes.saturating_sub(result.dropped_bytes);
                usage.idem_bytes = usage.idem_bytes.saturating_sub(pruned_bytes);
                write_meta(tx, &usage_key, &usage)?;
            }

            result.earliest_offset = meta.earliest_offset;
            result.next_offset = meta.next_offset;
            write_partition_meta(tx, op.topic_id, op.partition, &meta)?;

            shard.applied_index = applied.index;
            write_meta(tx, &shard_meta_key(op.shard_id), &shard)?;
            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;
            Ok(result)
        })
    }

    /// 物理回收一个分区（`Command::MqDropPartition`）：删除段记录、DLQ、幂等索引与
    /// 分区元数据，并从本分片的 topic 记账里扣除。
    ///
    /// 重放幂等：行已删光时各项计数为 0，仍推进 apply 水位（不产生副作用）。
    pub fn apply_mq_drop_partition(
        &self,
        op: &MqDropPartition,
        applied: AppliedLogId,
    ) -> Result<MqDropPartitionResult> {
        self.backend().write(|tx| {
            let mut result = MqDropPartitionResult::default();
            let meta = read_partition_meta(tx, op.topic_id, op.partition)?;
            let prefix = partition_prefix(op.topic_id, op.partition);
            let end = partition_successor(op.topic_id, op.partition);

            for (key, value) in tx.iter_range(TABLE_MQ_LOG, &prefix, &end)? {
                result.bytes_freed = result.bytes_freed.saturating_add(value.len() as u64);
                result.records_removed = result.records_removed.saturating_add(1);
                tx.remove(TABLE_MQ_LOG, &key)?;
            }
            for (key, value) in tx.iter_prefix(TABLE_MQ_IDEM, &prefix)? {
                result.bytes_freed = result.bytes_freed.saturating_add(value.len() as u64);
                result.idem_entries_removed = result.idem_entries_removed.saturating_add(1);
                tx.remove(TABLE_MQ_IDEM, &key)?;
            }
            for (key, value) in tx.iter_range(TABLE_MQ_DLQ, &prefix, &end)? {
                result.bytes_freed = result.bytes_freed.saturating_add(value.len() as u64);
                result.dlq_records_removed = result.dlq_records_removed.saturating_add(1);
                tx.remove(TABLE_MQ_DLQ, &key)?;
            }

            let usage_key = topic_usage_key(op.topic_id);
            let mut usage =
                read_meta::<MqTopicUsage>(tx, TABLE_MQ_META, &usage_key)?.unwrap_or_default();
            usage.log_bytes = usage.log_bytes.saturating_sub(meta.log_bytes);
            usage.dlq_bytes = usage.dlq_bytes.saturating_sub(meta.dlq_bytes);
            usage.idem_bytes = usage.idem_bytes.saturating_sub(meta.idem_bytes);
            write_meta(tx, &usage_key, &usage)?;

            tx.remove(
                TABLE_MQ_META,
                &partition_meta_key(op.topic_id, op.partition),
            )?;
            let mut shard = read_shard_meta(tx, op.shard_id)?;
            shard.applied_index = applied.index;
            write_meta(tx, &shard_meta_key(op.shard_id), &shard)?;
            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;
            Ok(result)
        })
    }

    /// 把一条消息移入 DLQ 并在主日志写 tombstone（`Command::MqMoveToDlq`）。
    pub fn apply_mq_move_to_dlq(
        &self,
        op: &MqMoveToDlq,
        applied: AppliedLogId,
    ) -> Result<MqMoveToDlqOutcome> {
        self.backend().write(|tx| {
            let mut shard = read_shard_meta(tx, op.shard_id)?;
            if applied.index <= shard.applied_index {
                // 重复 apply：状态里已有 tombstone 时必须返回首次 DLQ 位点，
                // 不能报 Missing（调用方会把 `Missing` 映射成 OFFSET_OUT_OF_RANGE）。
                return Ok(
                    match read_record(tx, op.topic_id, op.partition, op.offset)? {
                        Some(record) => match record.moved_to_dlq {
                            Some(dlq_offset) => MqMoveToDlqOutcome::AlreadyMoved { dlq_offset },
                            None => MqMoveToDlqOutcome::Missing,
                        },
                        None => MqMoveToDlqOutcome::Missing,
                    },
                );
            }

            let mut meta = read_partition_meta(tx, op.topic_id, op.partition)?;
            let Some(mut record) = read_record(tx, op.topic_id, op.partition, op.offset)? else {
                return Ok(MqMoveToDlqOutcome::Missing);
            };
            if let Some(dlq_offset) = record.moved_to_dlq {
                shard.applied_index = applied.index;
                write_meta(tx, &shard_meta_key(op.shard_id), &shard)?;
                tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;
                return Ok(MqMoveToDlqOutcome::AlreadyMoved { dlq_offset });
            }

            let dlq_offset = meta.next_dlq_offset;
            let dlq_record = MqDlqRecord {
                dlq_offset,
                topic_id: op.topic_id,
                partition: op.partition,
                offset: record.offset,
                timestamp_ms: record.timestamp_ms,
                key: std::mem::take(&mut record.key),
                payload: std::mem::take(&mut record.payload),
                headers: std::mem::take(&mut record.headers),
                producer: std::mem::take(&mut record.producer),
                reason: op.reason.clone(),
                detail: op.detail.clone(),
                moved_at_ms: op.now_ms,
            };
            let dlq_value = encode(&dlq_record)?;
            let dlq_bytes = dlq_value.len() as u64;
            tx.insert(
                TABLE_MQ_DLQ,
                &record_key(op.topic_id, op.partition, dlq_offset),
                &dlq_value,
            )?;

            record.moved_to_dlq = Some(dlq_offset);
            let tombstone = encode(&record)?;
            tx.insert(
                TABLE_MQ_LOG,
                &record_key(op.topic_id, op.partition, record.offset),
                &tombstone,
            )?;

            // 主段保留期内按两份计费（ADR-0016 D7）：tombstone 不缩短原段保留，
            // 段字节不因移入 DLQ 而减少；DLQ 副本另行计费。
            meta.next_dlq_offset = dlq_offset.saturating_add(1);
            meta.dlq_bytes = meta.dlq_bytes.saturating_add(dlq_bytes);

            let usage_key = topic_usage_key(op.topic_id);
            let mut usage =
                read_meta::<MqTopicUsage>(tx, TABLE_MQ_META, &usage_key)?.unwrap_or_default();
            usage.dlq_bytes = usage.dlq_bytes.saturating_add(dlq_bytes);
            write_meta(tx, &usage_key, &usage)?;

            write_partition_meta(tx, op.topic_id, op.partition, &meta)?;
            shard.applied_index = applied.index;
            write_meta(tx, &shard_meta_key(op.shard_id), &shard)?;
            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;
            Ok(MqMoveToDlqOutcome::Moved { dlq_offset })
        })
    }

    /// 读分区元数据（段/offset/记账）；不存在 = `None`。
    pub fn mq_partition_meta(
        &self,
        topic_id: u64,
        partition: u32,
    ) -> Result<Option<MqPartitionMeta>> {
        self.backend().read(|tx| {
            read_meta::<MqPartitionMeta>(
                tx,
                TABLE_MQ_META,
                &partition_meta_key(topic_id, partition),
            )
        })
    }

    /// 读分区可读边界；分区不存在（从未 append）= `None`。
    pub fn mq_partition_bounds(
        &self,
        topic_id: u64,
        partition: u32,
    ) -> Result<Option<MqPartitionBounds>> {
        self.backend().read(|tx| {
            Ok(read_meta::<MqPartitionMeta>(
                tx,
                TABLE_MQ_META,
                &partition_meta_key(topic_id, partition),
            )?
            .map(|meta| MqPartitionBounds {
                earliest_offset: meta.earliest_offset,
                next_offset: meta.next_offset,
            }))
        })
    }

    /// 枚举本分片已有元数据的分区（按 key 升序 = topic_id/partition 升序）。
    ///
    /// 保留 reaper 用它逐分区算 `up_to_offset`（分片内分区数受
    /// `max_partitions_per_shard` 约束）。
    pub fn mq_partitions(&self) -> Result<Vec<(u64, u32)>> {
        self.backend().read(|tx| {
            let rows = tx.iter_prefix(TABLE_MQ_META, &[META_PARTITION])?;
            let mut partitions = Vec::with_capacity(rows.len());
            for (key, _) in rows {
                let Some(rest) = key.strip_prefix(&[META_PARTITION]) else {
                    continue;
                };
                let (Some(topic_id), Some(partition)) = (
                    rest.get(..8).and_then(|bytes| bytes.try_into().ok()),
                    rest.get(8..).and_then(|bytes| bytes.try_into().ok()),
                ) else {
                    continue;
                };
                partitions.push((u64::from_be_bytes(topic_id), u32::from_be_bytes(partition)));
            }
            Ok(partitions)
        })
    }

    /// 本分片内某 topic 的记账汇总。
    pub fn mq_topic_usage(&self, topic_id: u64) -> Result<MqTopicUsage> {
        self.backend().read(|tx| {
            Ok(
                read_meta::<MqTopicUsage>(tx, TABLE_MQ_META, &topic_usage_key(topic_id))?
                    .unwrap_or_default(),
            )
        })
    }

    /// 读主日志消息 `[start_offset, start_offset + limit)`（含 tombstone；调用方过滤）。
    pub fn mq_records(
        &self,
        topic_id: u64,
        partition: u32,
        start_offset: u64,
        limit: usize,
    ) -> Result<Vec<MqStoredRecord>> {
        let rows = self.backend().read(|tx| {
            let start = record_key(topic_id, partition, start_offset);
            let end = partition_successor(topic_id, partition);
            tx.iter_range(TABLE_MQ_LOG, &start, &end)
        })?;
        let mut records = Vec::with_capacity(limit.min(rows.len()));
        for (_, value) in rows {
            if records.len() >= limit {
                break;
            }
            records.push(decode(&value)?);
        }
        Ok(records)
    }

    /// 读 DLQ 消息 `[start_dlq_offset, ...)`（最多 `limit` 条）。
    pub fn mq_dlq_records(
        &self,
        topic_id: u64,
        partition: u32,
        start_dlq_offset: u64,
        limit: usize,
    ) -> Result<Vec<MqDlqRecord>> {
        let rows = self.backend().read(|tx| {
            let start = record_key(topic_id, partition, start_dlq_offset);
            let end = partition_successor(topic_id, partition);
            tx.iter_range(TABLE_MQ_DLQ, &start, &end)
        })?;
        let mut records = Vec::with_capacity(limit.min(rows.len()));
        for (_, value) in rows {
            if records.len() >= limit {
                break;
            }
            records.push(decode(&value)?);
        }
        Ok(records)
    }
}

/// 分区前缀的字典序后继（用于区间扫描到下一个分区）。
///
/// 12 字节定宽前缀整体 +1；前缀全为 `0xFF`（`topic_id = u64::MAX` 且
/// `partition = u32::MAX`）时没有后继，返回空 end = 扫描到表尾。
fn partition_successor(topic_id: u64, partition: u32) -> Vec<u8> {
    let mut prefix = partition_prefix(topic_id, partition).to_vec();
    for byte in prefix.iter_mut().rev() {
        if *byte == u8::MAX {
            *byte = 0;
        } else {
            *byte += 1;
            return prefix;
        }
    }
    Vec::new()
}

fn read_shard_meta(tx: &dyn ReadTx, shard_id: u32) -> Result<MqShardMeta> {
    Ok(read_meta::<MqShardMeta>(tx, TABLE_MQ_META, &shard_meta_key(shard_id))?.unwrap_or_default())
}

fn read_partition_meta(tx: &dyn ReadTx, topic_id: u64, partition: u32) -> Result<MqPartitionMeta> {
    Ok(
        read_meta::<MqPartitionMeta>(tx, TABLE_MQ_META, &partition_meta_key(topic_id, partition))?
            .unwrap_or_default(),
    )
}

fn write_partition_meta(
    tx: &mut dyn WriteTx,
    topic_id: u64,
    partition: u32,
    meta: &MqPartitionMeta,
) -> Result<()> {
    write_meta(tx, &partition_meta_key(topic_id, partition), meta)
}

/// 单个请求的原子 apply：任一校验失败整批不写。
fn apply_append_request(
    tx: &mut dyn WriteTx,
    now_ms: i64,
    request: &MqAppendRequest,
) -> Result<MqAppendOutcome> {
    let mut meta = read_partition_meta(tx, request.topic_id, request.partition)?;
    let usage_key = topic_usage_key(request.topic_id);
    let usage = read_meta::<MqTopicUsage>(tx, TABLE_MQ_META, &usage_key)?.unwrap_or_default();

    // 第一遍只做校验与尺寸计算：分配候选 offset，但不写任何行。
    let mut next_offset = meta.next_offset;
    let mut planned: Vec<(usize, u64, u64)> = Vec::new(); // (entry index, offset, encoded size)
    let mut results: Vec<MqAppendEntryResult> = Vec::with_capacity(request.entries.len());
    let mut batch_idem: BTreeMap<&str, u64> = BTreeMap::new();
    let mut added_log_bytes = 0u64;
    let mut added_idem_bytes = 0u64;
    let mut idem_writes: Vec<(Vec<u8>, MqIdemEntry)> = Vec::new();

    for (index, entry) in request.entries.iter().enumerate() {
        if !entry.idempotency_key.is_empty() {
            if let Some(offset) = batch_idem.get(entry.idempotency_key.as_str()) {
                results.push(MqAppendEntryResult {
                    offset: *offset,
                    timestamp_ms: now_ms,
                    deduplicated: true,
                });
                continue;
            }
            let key = idem_key(request.topic_id, request.partition, &entry.idempotency_key);
            if let Some(existing) = read_meta::<MqIdemEntry>(tx, TABLE_MQ_IDEM, &key)? {
                batch_idem.insert(entry.idempotency_key.as_str(), existing.offset);
                results.push(MqAppendEntryResult {
                    offset: existing.offset,
                    timestamp_ms: now_ms,
                    deduplicated: true,
                });
                continue;
            }
        }

        let record = MqStoredRecord {
            offset: next_offset,
            timestamp_ms: now_ms,
            key: entry.key.clone(),
            payload: entry.payload.clone(),
            headers: entry.headers.clone(),
            producer: entry.producer.clone(),
            moved_to_dlq: None,
        };
        let size = encode(&record)?.len() as u64;
        if request.max_message_bytes > 0 && size > u64::from(request.max_message_bytes) {
            return Ok(MqAppendOutcome::Rejected(
                MqAppendRejection::MessageTooLarge {
                    entry: index,
                    size,
                    limit: u64::from(request.max_message_bytes),
                },
            ));
        }

        if !entry.idempotency_key.is_empty() {
            let key = idem_key(request.topic_id, request.partition, &entry.idempotency_key);
            let idem = MqIdemEntry {
                offset: next_offset,
                bytes: key.len() as u64 + 8,
            };
            added_idem_bytes = added_idem_bytes.saturating_add(idem.bytes);
            idem_writes.push((key, idem));
        }

        batch_idem.insert(entry.idempotency_key.as_str(), next_offset);
        results.push(MqAppendEntryResult {
            offset: next_offset,
            timestamp_ms: now_ms,
            deduplicated: false,
        });
        added_log_bytes = added_log_bytes.saturating_add(size);
        planned.push((index, next_offset, size));
        next_offset = next_offset.saturating_add(1);
    }

    if request.max_bytes > 0 {
        let requested = added_log_bytes.saturating_add(added_idem_bytes);
        if usage.total().saturating_add(requested) > request.max_bytes {
            return Ok(MqAppendOutcome::Rejected(
                MqAppendRejection::QuotaExceeded {
                    used: usage.total(),
                    requested,
                    limit: request.max_bytes,
                },
            ));
        }
    }

    // 第二遍写行：校验已全部通过，不会有部分提交。
    for (index, offset, size) in &planned {
        let entry = &request.entries[*index];
        let record = MqStoredRecord {
            offset: *offset,
            timestamp_ms: now_ms,
            key: entry.key.clone(),
            payload: entry.payload.clone(),
            headers: entry.headers.clone(),
            producer: entry.producer.clone(),
            moved_to_dlq: None,
        };
        let value = encode(&record)?;
        tx.insert(
            TABLE_MQ_LOG,
            &record_key(request.topic_id, request.partition, *offset),
            &value,
        )?;

        open_segment_for(&mut meta, *offset, *size, request.segment_bytes, now_ms);
    }
    for (key, idem) in &idem_writes {
        let value = encode(idem)?;
        tx.insert(TABLE_MQ_IDEM, key, &value)?;
    }
    meta.next_offset = next_offset;
    // 分区级幂等索引字节必须与 topic 记账同步：`MqTruncate`/`MqDropPartition`
    // 以 `meta.idem_bytes` 为扣减基数，漏记会让回收后仍占用配额。
    meta.idem_bytes = meta.idem_bytes.saturating_add(added_idem_bytes);

    if added_log_bytes > 0 || added_idem_bytes > 0 {
        let mut updated = usage;
        updated.log_bytes = updated.log_bytes.saturating_add(added_log_bytes);
        updated.idem_bytes = updated.idem_bytes.saturating_add(added_idem_bytes);
        write_meta(tx, &usage_key, &updated)?;
        write_partition_meta(tx, request.topic_id, request.partition, &meta)?;
    }

    Ok(MqAppendOutcome::Applied(MqAppended { entries: results }))
}

/// 把消息计入当前段；超过 `segment_bytes` 时先封口再开新段。
fn open_segment_for(
    meta: &mut MqPartitionMeta,
    offset: u64,
    size: u64,
    segment_bytes: u64,
    now_ms: i64,
) {
    let needs_new = match meta.segments.last() {
        None => true,
        Some(segment) => {
            segment.bytes > 0
                && segment_bytes > 0
                && segment.bytes.saturating_add(size) > segment_bytes
        }
    };
    if needs_new {
        let segment = MqSegmentMeta {
            id: meta.next_segment_id,
            start_offset: offset,
            end_offset: offset,
            bytes: 0,
            opened_at_ms: now_ms,
        };
        meta.next_segment_id = meta.next_segment_id.saturating_add(1);
        meta.segments.push(segment);
    }
    if let Some(segment) = meta.segments.last_mut() {
        segment.end_offset = offset.saturating_add(1);
        segment.bytes = segment.bytes.saturating_add(size);
    }
    meta.log_bytes = meta.log_bytes.saturating_add(size);
}

/// 保留策略：算出可回收的整段前缀边界（`up_to_offset`）。
///
/// 只考虑已封口段（`segments[..len-1]`）：至少保留最新段（与
/// `apply_mq_truncate` 同口径）。`retention_secs == 0` = 不限保留；段按
/// `opened_at_ms` 升序，遇到第一个未过期段即停。时间由调用方传入（Leader 的
/// `now_ms`），apply 不读时钟。
pub fn mq_retention_cutoff(
    segments: &[MqSegmentMeta],
    now_ms: i64,
    retention_secs: u64,
) -> Option<u64> {
    if retention_secs == 0 || segments.len() < 2 {
        return None;
    }
    let retention_ms = i64::try_from(retention_secs).ok()?.saturating_mul(1_000);
    let mut cutoff = None;
    for segment in &segments[..segments.len() - 1] {
        if now_ms.saturating_sub(segment.opened_at_ms) < retention_ms {
            break;
        }
        cutoff = Some(segment.end_offset);
    }
    cutoff
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::redb_backend::RedbBackend;
    use coord_core::types::StorageConfig;
    use tempfile::TempDir;

    fn setup() -> (TempDir, MvccStorage<RedbBackend>) {
        let dir = TempDir::new().unwrap();
        let storage = open(&dir);
        (dir, storage)
    }

    /// 打开（或重开）同一目录上的存储；重启测试复用。
    fn open(dir: &TempDir) -> MvccStorage<RedbBackend> {
        let backend = RedbBackend::open(dir.path(), &StorageConfig::default()).unwrap();
        MvccStorage::new(backend).unwrap()
    }

    fn applied(index: u64) -> AppliedLogId {
        AppliedLogId {
            term: 1,
            node_id: 1,
            index,
        }
    }

    fn entry(key: &[u8], payload: &[u8]) -> MqAppendEntry {
        MqAppendEntry {
            key: key.to_vec(),
            payload: payload.to_vec(),
            idempotency_key: String::new(),
            headers: BTreeMap::new(),
            producer: "test".to_string(),
        }
    }

    fn request(entries: Vec<MqAppendEntry>) -> MqAppendRequest {
        MqAppendRequest {
            topic_id: 7,
            partition: 0,
            max_message_bytes: 1024,
            max_bytes: 1 << 20,
            segment_bytes: 1 << 20,
            entries,
        }
    }

    fn append(
        storage: &MvccStorage<RedbBackend>,
        index: u64,
        entries: Vec<MqAppendEntry>,
    ) -> MqAppendBatchResponse {
        append_at(storage, index, 1_700_000_000_000, 1 << 20, entries)
    }

    fn append_at(
        storage: &MvccStorage<RedbBackend>,
        index: u64,
        now_ms: i64,
        segment_bytes: u64,
        entries: Vec<MqAppendEntry>,
    ) -> MqAppendBatchResponse {
        let mut request = request(entries);
        request.segment_bytes = segment_bytes;
        storage
            .apply_mq_append_batch(
                &MqAppendBatch {
                    shard_id: 0,
                    now_ms,
                    requests: vec![request],
                },
                applied(index),
            )
            .unwrap()
    }

    fn offsets(response: &MqAppendBatchResponse) -> Vec<u64> {
        match &response.requests[0] {
            MqAppendOutcome::Applied(appended) => {
                appended.entries.iter().map(|entry| entry.offset).collect()
            }
            other => panic!("expected applied, got {other:?}"),
        }
    }

    #[test]
    fn drop_partition_removes_records_dlq_and_idempotency_and_replays_idempotently() {
        let (_dir, storage) = setup();
        let keyed = |id: &str, payload: &str| MqAppendEntry {
            key: Vec::new(),
            payload: payload.as_bytes().to_vec(),
            idempotency_key: id.to_string(),
            headers: BTreeMap::new(),
            producer: "test".to_string(),
        };
        append(
            &storage,
            1,
            vec![keyed("a", "one"), keyed("b", "two"), keyed("c", "three")],
        );
        // 一条移入 DLQ（tombstone + DLQ 行）。
        let moved = storage
            .apply_mq_move_to_dlq(
                &MqMoveToDlq {
                    shard_id: 0,
                    topic_id: 7,
                    partition: 0,
                    offset: 1,
                    reason: "poison".to_string(),
                    detail: String::new(),
                    now_ms: 1_700_000_000_100,
                },
                applied(2),
            )
            .unwrap();
        assert!(matches!(moved, MqMoveToDlqOutcome::Moved { .. }));
        assert_eq!(
            storage
                .mq_partition_meta(7, 0)
                .unwrap()
                .unwrap()
                .next_offset,
            3
        );

        let dropped = storage
            .apply_mq_drop_partition(
                &MqDropPartition {
                    shard_id: 0,
                    topic_id: 7,
                    partition: 0,
                },
                applied(3),
            )
            .unwrap();
        assert_eq!(dropped.records_removed, 3);
        assert_eq!(dropped.dlq_records_removed, 1);
        assert_eq!(dropped.idem_entries_removed, 3);
        assert!(dropped.bytes_freed > 0);
        assert!(storage.mq_partition_meta(7, 0).unwrap().is_none());
        assert!(storage.mq_records(7, 0, 0, 10).unwrap().is_empty());
        assert!(storage.mq_dlq_records(7, 0, 0, 10).unwrap().is_empty());
        let usage = storage.mq_topic_usage(7).unwrap();
        assert_eq!(usage.total(), 0, "drop must release the topic accounting");

        // 重放同一命令：不重复扣账（计数为 0）。
        let replay = storage
            .apply_mq_drop_partition(
                &MqDropPartition {
                    shard_id: 0,
                    topic_id: 7,
                    partition: 0,
                },
                applied(3),
            )
            .unwrap();
        assert_eq!(replay, MqDropPartitionResult::default());
        assert_eq!(storage.mq_topic_usage(7).unwrap().total(), 0);
    }

    #[test]
    fn append_assigns_monotonic_offsets_and_replays_without_duplicates() {
        let (_dir, storage) = setup();
        let first = append(&storage, 1, vec![entry(b"k1", b"a"), entry(b"k2", b"b")]);
        assert_eq!(offsets(&first), vec![0, 1]);

        let second = append(&storage, 2, vec![entry(b"k3", b"c")]);
        assert_eq!(offsets(&second), vec![2]);

        let meta = storage.mq_partition_meta(7, 0).unwrap().unwrap();
        assert_eq!(meta.next_offset, 3);
        assert_eq!(meta.earliest_offset, 0);

        // 同一条日志的重复 apply：返回首次响应，offset 不前进、不重复写。
        let replay = append(&storage, 2, vec![entry(b"k3", b"c")]);
        assert_eq!(offsets(&replay), vec![2]);
        let meta = storage.mq_partition_meta(7, 0).unwrap().unwrap();
        assert_eq!(meta.next_offset, 3);

        let records = storage.mq_records(7, 0, 0, 10).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[2].payload, b"c");
    }

    #[test]
    fn idempotency_key_returns_the_first_offset() {
        let (_dir, storage) = setup();
        let mut first = entry(b"k", b"v");
        first.idempotency_key = "idem-1".to_string();
        let response = append(&storage, 1, vec![first.clone()]);
        assert_eq!(offsets(&response), vec![0]);

        let response = append(&storage, 2, vec![first]);
        match &response.requests[0] {
            MqAppendOutcome::Applied(appended) => {
                assert_eq!(appended.entries[0].offset, 0);
                assert!(appended.entries[0].deduplicated);
            }
            other => panic!("expected applied, got {other:?}"),
        }
        assert_eq!(
            storage
                .mq_partition_meta(7, 0)
                .unwrap()
                .unwrap()
                .next_offset,
            1
        );

        // 同批内重复键按首次处理。
        let mut a = entry(b"a", b"1");
        a.idempotency_key = "idem-2".to_string();
        let mut b = entry(b"b", b"2");
        b.idempotency_key = "idem-2".to_string();
        let response = append(&storage, 3, vec![a, b]);
        match &response.requests[0] {
            MqAppendOutcome::Applied(appended) => {
                assert_eq!(appended.entries[0].offset, 1);
                assert!(!appended.entries[0].deduplicated);
                assert_eq!(appended.entries[1].offset, 1);
                assert!(appended.entries[1].deduplicated);
            }
            other => panic!("expected applied, got {other:?}"),
        }
        assert_eq!(
            storage
                .mq_partition_meta(7, 0)
                .unwrap()
                .unwrap()
                .next_offset,
            2
        );
    }

    #[test]
    fn oversized_entry_rejects_the_whole_batch() {
        let (_dir, storage) = setup();
        let mut request = request(vec![entry(b"ok", b"fine"), entry(b"big", &[0u8; 512])]);
        request.max_message_bytes = 64;
        let response = storage
            .apply_mq_append_batch(
                &MqAppendBatch {
                    shard_id: 0,
                    now_ms: 1,
                    requests: vec![request],
                },
                applied(1),
            )
            .unwrap();
        match &response.requests[0] {
            MqAppendOutcome::Rejected(MqAppendRejection::MessageTooLarge { entry, .. }) => {
                assert_eq!(*entry, 1);
            }
            other => panic!("expected MessageTooLarge, got {other:?}"),
        }
        assert!(storage.mq_records(7, 0, 0, 10).unwrap().is_empty());
        assert!(storage.mq_partition_meta(7, 0).unwrap().is_none());
    }

    #[test]
    fn quota_rejects_the_whole_batch_without_partial_writes() {
        let (_dir, storage) = setup();
        let mut request = request(vec![entry(b"a", &[0u8; 100]), entry(b"b", &[0u8; 100])]);
        request.max_bytes = 150;
        let response = storage
            .apply_mq_append_batch(
                &MqAppendBatch {
                    shard_id: 0,
                    now_ms: 1,
                    requests: vec![request],
                },
                applied(1),
            )
            .unwrap();
        assert!(matches!(
            &response.requests[0],
            MqAppendOutcome::Rejected(MqAppendRejection::QuotaExceeded { .. })
        ));
        assert!(storage.mq_records(7, 0, 0, 10).unwrap().is_empty());
        assert!(storage.mq_partition_meta(7, 0).unwrap().is_none());
    }

    #[test]
    fn segments_seal_on_size_and_truncate_keeps_the_latest() {
        let (_dir, storage) = setup();
        let mut request = request(vec![entry(b"a", &[0u8; 40]), entry(b"b", &[0u8; 40])]);
        request.segment_bytes = 60;
        storage
            .apply_mq_append_batch(
                &MqAppendBatch {
                    shard_id: 0,
                    now_ms: 1,
                    requests: vec![request],
                },
                applied(1),
            )
            .unwrap();
        let meta = storage.mq_partition_meta(7, 0).unwrap().unwrap();
        assert_eq!(meta.segments.len(), 2);
        assert_eq!(meta.segments[0].start_offset, 0);
        assert_eq!(meta.segments[0].end_offset, 1);
        assert_eq!(meta.segments[1].start_offset, 1);

        let result = storage
            .apply_mq_truncate(
                &MqTruncate {
                    shard_id: 0,
                    topic_id: 7,
                    partition: 0,
                    up_to_offset: 1,
                },
                applied(2),
            )
            .unwrap();
        assert_eq!(result.dropped_segments, 1);
        assert_eq!(result.dropped_records, 1);
        assert_eq!(result.earliest_offset, 1);
        let meta = storage.mq_partition_meta(7, 0).unwrap().unwrap();
        assert_eq!(meta.segments.len(), 1);
        // 至少保留最新段：即使 up_to 覆盖它也不删。
        let result = storage
            .apply_mq_truncate(
                &MqTruncate {
                    shard_id: 0,
                    topic_id: 7,
                    partition: 0,
                    up_to_offset: 100,
                },
                applied(3),
            )
            .unwrap();
        assert_eq!(result.dropped_segments, 0);
        assert_eq!(
            storage
                .mq_partition_meta(7, 0)
                .unwrap()
                .unwrap()
                .segments
                .len(),
            1
        );
    }

    #[test]
    fn move_to_dlq_is_idempotent_and_marks_the_primary_tombstone() {
        let (_dir, storage) = setup();
        append(&storage, 1, vec![entry(b"k", b"payload")]);
        let moved = storage
            .apply_mq_move_to_dlq(
                &MqMoveToDlq {
                    shard_id: 0,
                    topic_id: 7,
                    partition: 0,
                    offset: 0,
                    reason: "poison".to_string(),
                    detail: "bad".to_string(),
                    now_ms: 2,
                },
                applied(2),
            )
            .unwrap();
        assert_eq!(moved, MqMoveToDlqOutcome::Moved { dlq_offset: 0 });
        let again = storage
            .apply_mq_move_to_dlq(
                &MqMoveToDlq {
                    shard_id: 0,
                    topic_id: 7,
                    partition: 0,
                    offset: 0,
                    reason: "poison".to_string(),
                    detail: "bad".to_string(),
                    now_ms: 3,
                },
                applied(3),
            )
            .unwrap();
        assert_eq!(again, MqMoveToDlqOutcome::AlreadyMoved { dlq_offset: 0 });

        let records = storage.mq_records(7, 0, 0, 10).unwrap();
        assert_eq!(records.len(), 1);
        assert!(!records[0].is_visible());
        let dlq = storage.mq_dlq_records(7, 0, 0, 10).unwrap();
        assert_eq!(dlq.len(), 1);
        assert_eq!(dlq[0].reason, "poison");
        assert_eq!(dlq[0].payload, b"payload");
        assert!(storage.mq_partition_meta(7, 0).unwrap().unwrap().dlq_bytes > 0);
    }

    #[test]
    fn snapshot_roundtrip_preserves_offsets_segments_and_dlq() {
        use crate::storage::snapshot::{export_snapshot_data, import_snapshot_data, SnapshotData};

        let (_dir, storage) = setup();
        append(&storage, 1, vec![entry(b"a", b"one"), entry(b"b", b"two")]);
        storage
            .apply_mq_move_to_dlq(
                &MqMoveToDlq {
                    shard_id: 0,
                    topic_id: 7,
                    partition: 0,
                    offset: 1,
                    reason: "poison".to_string(),
                    detail: "bad".to_string(),
                    now_ms: 2,
                },
                applied(2),
            )
            .unwrap();
        let meta_before = storage.mq_partition_meta(7, 0).unwrap().unwrap();

        let data = export_snapshot_data(&storage, 2, 1).unwrap();
        let restored = SnapshotData::from_bytes(&data.to_bytes().unwrap()).unwrap();
        assert_eq!(restored.mq_log_entries.len(), 2);
        assert_eq!(restored.mq_dlq_entries.len(), 1);
        assert_eq!(restored.mq_idem_entries.len(), 0);
        assert!(!restored.mq_meta_entries.is_empty());

        let (_dir2, restored_storage) = setup();
        import_snapshot_data(&restored_storage, &restored).unwrap();
        assert_eq!(
            restored_storage.mq_partition_meta(7, 0).unwrap().unwrap(),
            meta_before
        );
        let dlq = restored_storage.mq_dlq_records(7, 0, 0, 10).unwrap();
        assert_eq!(dlq.len(), 1);
        assert_eq!(dlq[0].offset, 1);

        // 装快照后的下一条消息必须继续 next_offset，不倒退/不复用。
        let response = append(&restored_storage, 3, vec![entry(b"c", b"three")]);
        assert_eq!(offsets(&response), vec![2]);
    }

    #[test]
    fn topic_usage_accounts_log_dlq_and_idempotency() {
        let (_dir, storage) = setup();
        let mut entry = entry(b"k", b"v");
        entry.idempotency_key = "idem".to_string();
        append(&storage, 1, vec![entry]);
        let usage = storage.mq_topic_usage(7).unwrap();
        assert!(usage.log_bytes > 0);
        assert!(usage.idem_bytes > 0);
        assert_eq!(usage.dlq_bytes, 0);
    }

    /// 保留策略只回收已封口且过期的整段前缀，最新段永远保留。
    #[test]
    fn retention_cutoff_keeps_the_latest_segment() {
        let (_dir, storage) = setup();
        let day = 86_400_000i64;
        let t0 = 1_700_000_000_000i64;
        // segment_bytes = 1 ⇒ 每条消息单独封口，三段分别开在三天。
        append_at(&storage, 1, t0, 1, vec![entry(b"a", b"1")]);
        append_at(&storage, 2, t0 + 10 * day, 1, vec![entry(b"b", b"2")]);
        append_at(&storage, 3, t0 + 20 * day, 1, vec![entry(b"c", b"3")]);
        let meta = storage.mq_partition_meta(7, 0).unwrap().unwrap();
        assert_eq!(meta.segments.len(), 3);

        assert_eq!(
            mq_retention_cutoff(&meta.segments, t0 + 20 * day, 604_800),
            Some(2),
            "前两段超过 7d，回收边界 = 第二段 end_offset"
        );
        assert_eq!(
            mq_retention_cutoff(&meta.segments, t0 + 10 * day, 604_800),
            Some(1)
        );
        assert_eq!(
            mq_retention_cutoff(&meta.segments, t0 + day, 604_800),
            None,
            "没有过期段 ⇒ 不提案"
        );
        assert_eq!(
            mq_retention_cutoff(&meta.segments, t0 + 100 * day, 0),
            None,
            "0 = 不限保留"
        );

        let result = storage
            .apply_mq_truncate(
                &MqTruncate {
                    shard_id: 0,
                    topic_id: 7,
                    partition: 0,
                    up_to_offset: 2,
                },
                applied(4),
            )
            .unwrap();
        assert_eq!(result.dropped_segments, 2);
        assert_eq!(result.dropped_records, 2);
        assert_eq!(result.earliest_offset, 2);
        assert_eq!(result.next_offset, 3);
        let meta = storage.mq_partition_meta(7, 0).unwrap().unwrap();
        assert_eq!(meta.segments.len(), 1);
        assert_eq!(meta.earliest_offset, 2);
        assert_eq!(
            mq_retention_cutoff(&meta.segments, t0 + 30 * day, 604_800),
            None,
            "至少保留最新段"
        );
    }

    /// 回收释放配额、幂等索引随段回收：被回收 offset 的键不再命中去重。
    #[test]
    fn truncate_frees_quota_and_prunes_idempotency_entries() {
        let (_dir, storage) = setup();
        let t0 = 1_700_000_000_000i64;
        for index in 0..3u64 {
            let mut entry = entry(b"k", &[0u8; 512]);
            entry.idempotency_key = format!("idem-{index}");
            append_at(&storage, index + 1, t0 + index as i64, 1, vec![entry]);
        }
        let usage_before = storage.mq_topic_usage(7).unwrap();
        assert!(usage_before.log_bytes > 0);
        assert!(usage_before.idem_bytes > 0);

        // 同一配额上限：回收前整批拒绝。
        let mut pending = request(vec![entry(b"new", &[0u8; 512])]);
        pending.max_message_bytes = 4096;
        pending.max_bytes = usage_before.total() + 1;
        let response = storage
            .apply_mq_append_batch(
                &MqAppendBatch {
                    shard_id: 0,
                    now_ms: t0 + 10,
                    requests: vec![pending.clone()],
                },
                applied(10),
            )
            .unwrap();
        assert!(matches!(
            &response.requests[0],
            MqAppendOutcome::Rejected(MqAppendRejection::QuotaExceeded { .. })
        ));

        let result = storage
            .apply_mq_truncate(
                &MqTruncate {
                    shard_id: 0,
                    topic_id: 7,
                    partition: 0,
                    up_to_offset: 2,
                },
                applied(11),
            )
            .unwrap();
        assert_eq!(result.dropped_segments, 2);
        let meta = storage.mq_partition_meta(7, 0).unwrap().unwrap();
        assert_eq!(meta.earliest_offset, 2);
        let usage_after = storage.mq_topic_usage(7).unwrap();
        assert!(usage_after.total() < usage_before.total());
        assert_eq!(usage_after.log_bytes, meta.log_bytes);
        assert!(
            usage_after.idem_bytes < usage_before.idem_bytes,
            "低于 earliest 的幂等索引必须随段回收"
        );

        // 同一上限现在通过：回收的字节真的释放了配额。
        let response = storage
            .apply_mq_append_batch(
                &MqAppendBatch {
                    shard_id: 0,
                    now_ms: t0 + 20,
                    requests: vec![pending],
                },
                applied(12),
            )
            .unwrap();
        assert!(matches!(&response.requests[0], MqAppendOutcome::Applied(_)));

        // 被回收 offset 的幂等键不再命中：重新使用得到新 offset。
        let mut reuse = entry(b"k", &[0u8; 512]);
        reuse.idempotency_key = "idem-0".to_string();
        let response = append_at(&storage, 13, t0 + 30, 1 << 20, vec![reuse]);
        match &response.requests[0] {
            MqAppendOutcome::Applied(appended) => {
                assert_eq!(appended.entries[0].offset, 4);
                assert!(!appended.entries[0].deduplicated);
            }
            other => panic!("expected Applied, got {other:?}"),
        }
    }

    /// 同一条日志的 truncate 重放：不重复删除、earliest 不回退。
    #[test]
    fn truncate_replay_is_idempotent() {
        let (_dir, storage) = setup();
        let t0 = 1_700_000_000_000i64;
        for index in 0..3u64 {
            append_at(
                &storage,
                index + 1,
                t0 + index as i64,
                1,
                vec![entry(b"k", b"v")],
            );
        }
        let command = MqTruncate {
            shard_id: 0,
            topic_id: 7,
            partition: 0,
            up_to_offset: 2,
        };
        let first = storage.apply_mq_truncate(&command, applied(10)).unwrap();
        assert_eq!(first.dropped_segments, 2);
        assert_eq!(first.earliest_offset, 2);

        let replay = storage.apply_mq_truncate(&command, applied(10)).unwrap();
        assert_eq!(replay.dropped_segments, 0);
        assert_eq!(replay.earliest_offset, 2);
        assert_eq!(replay.next_offset, 3);

        let again = storage.apply_mq_truncate(&command, applied(11)).unwrap();
        assert_eq!(again.dropped_segments, 0);
        assert_eq!(again.earliest_offset, 2);
        assert_eq!(
            storage
                .mq_partition_meta(7, 0)
                .unwrap()
                .unwrap()
                .earliest_offset,
            2
        );
    }

    /// earliest/latest 随 append 与 truncate 前进；分区枚举供 reaper 使用。
    #[test]
    fn partition_bounds_track_earliest_and_latest() {
        let (_dir, storage) = setup();
        let t0 = 1_700_000_000_000i64;
        assert!(storage.mq_partition_bounds(7, 0).unwrap().is_none());
        assert!(storage.mq_partitions().unwrap().is_empty());

        append(&storage, 1, vec![entry(b"a", b"1"), entry(b"b", b"2")]);
        let bounds = storage.mq_partition_bounds(7, 0).unwrap().unwrap();
        assert_eq!(bounds.earliest_offset, 0);
        assert_eq!(bounds.next_offset, 2);

        let mut other = request(vec![entry(b"a", b"1")]);
        other.partition = 1;
        storage
            .apply_mq_append_batch(
                &MqAppendBatch {
                    shard_id: 0,
                    now_ms: t0,
                    requests: vec![other],
                },
                applied(2),
            )
            .unwrap();
        assert_eq!(storage.mq_partitions().unwrap(), vec![(7, 0), (7, 1)]);

        // 先把分区 0 切成两段，再回收第一段。
        append_at(&storage, 3, t0 + 1, 1, vec![entry(b"c", b"3")]);
        storage
            .apply_mq_truncate(
                &MqTruncate {
                    shard_id: 0,
                    topic_id: 7,
                    partition: 0,
                    up_to_offset: 2,
                },
                applied(4),
            )
            .unwrap();
        let bounds = storage.mq_partition_bounds(7, 0).unwrap().unwrap();
        assert_eq!(bounds.earliest_offset, 2);
        assert_eq!(bounds.next_offset, 3);
    }

    /// 进程重启（同目录重开）后 offset/段/DLQ 与关闭前一致，继续追加不倒退。
    #[test]
    fn reopening_the_storage_preserves_offsets_segments_and_dlq() {
        let dir = TempDir::new().unwrap();
        let state_before = {
            let storage = open(&dir);
            append(&storage, 1, vec![entry(b"a", b"one"), entry(b"b", b"two")]);
            storage
                .apply_mq_move_to_dlq(
                    &MqMoveToDlq {
                        shard_id: 0,
                        topic_id: 7,
                        partition: 0,
                        offset: 1,
                        reason: "poison".to_string(),
                        detail: "bad".to_string(),
                        now_ms: 2,
                    },
                    applied(2),
                )
                .unwrap();
            (
                storage.mq_partition_meta(7, 0).unwrap().unwrap(),
                storage.mq_records(7, 0, 0, 10).unwrap(),
                storage.mq_dlq_records(7, 0, 0, 10).unwrap(),
            )
        };

        let storage = open(&dir);
        assert_eq!(
            storage.mq_partition_meta(7, 0).unwrap().unwrap(),
            state_before.0
        );
        assert_eq!(storage.mq_records(7, 0, 0, 10).unwrap(), state_before.1);
        assert_eq!(storage.mq_dlq_records(7, 0, 0, 10).unwrap(), state_before.2);
        let response = append_at(
            &storage,
            3,
            1_700_000_000_500,
            1 << 20,
            vec![entry(b"c", b"three")],
        );
        assert_eq!(offsets(&response), vec![2]);
    }

    /// 副本对同一命令序列 apply 得到相同状态；换届后在副本上继续追加 offset 接着走。
    #[test]
    fn replica_applies_the_same_log_to_the_same_state() {
        let (_leader_dir, leader) = setup();
        let (_follower_dir, follower) = setup();
        for index in 1..=2u64 {
            let command = MqAppendBatch {
                shard_id: 0,
                now_ms: 1_700_000_000_000 + index as i64,
                requests: vec![request(vec![entry(b"k", b"payload")])],
            };
            leader
                .apply_mq_append_batch(&command, applied(index))
                .unwrap();
            follower
                .apply_mq_append_batch(&command, applied(index))
                .unwrap();
        }
        assert_eq!(
            leader.mq_partition_meta(7, 0).unwrap(),
            follower.mq_partition_meta(7, 0).unwrap()
        );
        assert_eq!(
            leader.mq_records(7, 0, 0, 10).unwrap(),
            follower.mq_records(7, 0, 0, 10).unwrap()
        );

        let response = append_at(
            &follower,
            3,
            1_700_000_000_100,
            1 << 20,
            vec![entry(b"k", b"after-failover")],
        );
        assert_eq!(offsets(&response), vec![2]);
    }
}
