//! MQ v2 读路径：`Poll` / 长轮询 / offset 边界（ADR-0016 D5；M3-1）。
//!
//! 语义单一归属：`docs/adr/0016-mq-v2.md`；offset 分配与段存储的唯一实现在
//! `storage::mq_log`（M1-2），组位点在 `storage::mq_group`（M1-3）。本模块只做：
//!
//! 1. 只读**分片 Leader** 的已 apply 状态（不读未提交缓冲，Poll 不提案、不推进位点）；
//! 2. 跳过 DLQ tombstone（`PollDlq` 才是 DLQ 的读面，M4-1），`next_offset` 仍跨过它们；
//! 3. 显式 `start_offset` 低于最早可用 ⇒ `OFFSET_OUT_OF_RANGE` + earliest/latest
//!    trailer（**不静默 clamp**）；未设置 ⇒ 从最早可用开始（调用方没指定任何值）；
//! 4. 长轮询：`wait_ms`（上限 `[mq_v2].long_poll_max_ms`）等本分片的下一次提交；
//!    `Notify` 不存 permit，所以带 200ms 兜底重查（丢一次唤醒只多等一个兜底周期）。

use std::time::{Duration, Instant};

use coord_core::error_code::{attach, CoordErrorCode};
use coord_proto::mq::v2 as mq_v2;
use tonic::Status;

use crate::raft::region_runtime::RegionRuntime;
use crate::server::{map_err, CoordNode};
use crate::storage::mq_log::MqStoredRecord;

use super::publish::MqPartitionTarget;

/// `OFFSET_OUT_OF_RANGE` 的 earliest trailer（十进制 offset 字符串）。
pub const EARLIEST_OFFSET_METADATA_KEY: &str = "coord-earliest-offset";
/// `OFFSET_OUT_OF_RANGE` 的 latest trailer（= 下一条将被分配的 offset）。
pub const LATEST_OFFSET_METADATA_KEY: &str = "coord-latest-offset";

/// 长轮询的兜底重查周期（`Notify::notify_waiters` 不排队 permit）。
const POLL_FALLBACK: Duration = Duration::from_millis(200);

impl CoordNode {
    /// `Poll`：读已提交消息（含长轮询）。
    pub(crate) async fn mq_poll(
        &self,
        request: mq_v2::PollRequest,
    ) -> Result<mq_v2::PollResponse, Status> {
        let target =
            self.mq_partition_target(&request.topic, request.partition, request.topic_epoch)?;
        self.mq_ensure_shard_leader(&target).await?;

        let config = self.mq_v2().config();
        let max_messages = effective_messages(request.max_messages, config.poll_max_messages);
        let max_bytes = effective_bytes(request.max_bytes, config.poll_max_bytes);
        let wait =
            Duration::from_millis(effective_wait_ms(request.wait_ms, config.long_poll_max_ms));
        let explicit_start = match request.start_offset {
            None => None,
            Some(raw) => Some(u64::try_from(raw).map_err(|_| {
                attach(
                    Status::invalid_argument(format!(
                        "start_offset must not be negative (got {raw})"
                    )),
                    CoordErrorCode::InvalidArgument,
                )
            })?),
        };
        let started = Instant::now();
        loop {
            let (earliest, latest) = self.mq_bounds(&target)?;
            let start = match explicit_start {
                None => earliest,
                Some(offset) if offset < earliest => {
                    if let Some(metrics) = self.metrics() {
                        metrics.record_mq_v2_poll_out_of_range();
                    }
                    return Err(offset_out_of_range(earliest, latest));
                }
                Some(offset) => offset,
            };
            let (messages, next_offset) =
                read_visible(&target.runtime, &target, start, max_messages, max_bytes)?;
            let remaining = wait.saturating_sub(started.elapsed());
            if !messages.is_empty() || remaining.is_zero() {
                let bytes = messages
                    .iter()
                    .map(|message| (message.key.len() + message.payload.len()) as u64)
                    .sum();
                if let Some(metrics) = self.metrics() {
                    metrics.record_mq_v2_poll(messages.len() as u64, bytes, messages.is_empty());
                }
                return Ok(mq_v2::PollResponse {
                    messages,
                    next_offset: i64::try_from(next_offset).unwrap_or(i64::MAX),
                    earliest_offset: i64::try_from(earliest).unwrap_or(i64::MAX),
                    latest_offset: i64::try_from(latest).unwrap_or(i64::MAX),
                });
            }
            // 空批：等本分片的下一次提交（兜底 200ms 重查，最多等到 wait 用尽）。
            let batcher = self.mq_v2_batchers().shard(target.shard_id);
            tokio::select! {
                _ = batcher.appended() => {}
                _ = tokio::time::sleep(remaining.min(POLL_FALLBACK)) => {}
            }
        }
    }

    fn mq_bounds(&self, target: &MqPartitionTarget) -> Result<(u64, u64), Status> {
        let bounds = target
            .runtime
            .mvcc
            .mq_partition_bounds(target.topic_id, target.partition)
            .map_err(map_err)?;
        Ok(bounds
            .map(|bounds| (bounds.earliest_offset, bounds.next_offset))
            .unwrap_or((0, 0)))
    }
}

/// 读 `[start, latest)` 里至多 `max_messages` 条可见消息（跳过 tombstone）。
///
/// 返回 `(messages, next_offset)`：`next_offset` = 最后一条已读记录 + 1（空批 =
/// `start`）；因字节上限提前停止时，`next_offset` 指向**未返回的那条**，下次 Poll
/// 从它继续，不会跳过消息。
fn read_visible(
    runtime: &RegionRuntime,
    target: &MqPartitionTarget,
    start: u64,
    max_messages: usize,
    max_bytes: u64,
) -> Result<(Vec<mq_v2::Message>, u64), Status> {
    let mut cursor = start;
    let mut next_offset = start;
    let mut messages = Vec::new();
    let mut bytes = 0u64;
    while messages.len() < max_messages {
        let limit = max_messages - messages.len();
        let chunk = runtime
            .mvcc
            .mq_records(target.topic_id, target.partition, cursor, limit)
            .map_err(map_err)?;
        if chunk.is_empty() {
            break;
        }
        for record in chunk {
            cursor = record.offset.saturating_add(1);
            if !record.is_visible() {
                // DLQ tombstone：不返回，但位点必须跨过它。
                next_offset = record.offset.saturating_add(1);
                continue;
            }
            let size = (record.key.len() + record.payload.len()) as u64;
            // 第一条可见消息总是返回（否则超大的单条消息永远无法被消费）。
            if !messages.is_empty() && bytes.saturating_add(size) > max_bytes {
                return Ok((messages, next_offset));
            }
            bytes = bytes.saturating_add(size);
            next_offset = record.offset.saturating_add(1);
            messages.push(message_from_record(
                &target.topic_name,
                target.partition,
                record,
            ));
            if messages.len() >= max_messages {
                break;
            }
        }
    }
    Ok((messages, next_offset))
}

fn message_from_record(topic: &str, partition: u32, record: MqStoredRecord) -> mq_v2::Message {
    mq_v2::Message {
        topic: topic.to_string(),
        partition: partition as i32,
        offset: i64::try_from(record.offset).unwrap_or(i64::MAX),
        timestamp_ms: record.timestamp_ms,
        key: record.key,
        payload: record.payload,
        headers: record.headers.into_iter().collect(),
        producer: record.producer,
        dlq_reason: String::new(),
        dlq_detail: String::new(),
    }
}

/// `OFFSET_OUT_OF_RANGE`：带 earliest/latest trailer，绝不 clamp。
fn offset_out_of_range(earliest: u64, latest: u64) -> Status {
    let mut status = attach(
        Status::out_of_range(format!(
            "start_offset is below the earliest available offset {earliest} \
             (latest committed offset {latest}); reset the consumer offset explicitly \
             (no silent clamp)"
        )),
        CoordErrorCode::OffsetOutOfRange,
    );
    if let Ok(value) = tonic::metadata::MetadataValue::try_from(earliest.to_string()) {
        status
            .metadata_mut()
            .insert(EARLIEST_OFFSET_METADATA_KEY, value);
    }
    if let Ok(value) = tonic::metadata::MetadataValue::try_from(latest.to_string()) {
        status
            .metadata_mut()
            .insert(LATEST_OFFSET_METADATA_KEY, value);
    }
    status
}

/// 请求条数：`<= 0` ⇒ 配置默认；否则与配置上限取小。
fn effective_messages(requested: i32, cap: u32) -> usize {
    let cap = cap as usize;
    if requested <= 0 {
        return cap;
    }
    (requested as usize).min(cap)
}

/// 请求字节上限：`<= 0` ⇒ 配置默认；否则与配置上限取小。
fn effective_bytes(requested: i32, cap: u32) -> u64 {
    if requested <= 0 {
        return u64::from(cap);
    }
    (requested as u64).min(u64::from(cap))
}

/// 等待窗口：`<= 0` ⇒ 立即返回；否则截断到 `[mq_v2].long_poll_max_ms`。
fn effective_wait_ms(requested: i32, cap: u32) -> u64 {
    if requested <= 0 {
        return 0;
    }
    (requested as u64).min(u64::from(cap))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_default_and_clamp_to_the_config_caps() {
        assert_eq!(effective_messages(0, 1000), 1000);
        assert_eq!(effective_messages(-5, 1000), 1000);
        assert_eq!(effective_messages(10, 1000), 10);
        assert_eq!(effective_messages(5000, 1000), 1000);

        assert_eq!(effective_bytes(0, 4096), 4096);
        assert_eq!(effective_bytes(-1, 4096), 4096);
        assert_eq!(effective_bytes(1024, 4096), 1024);
        assert_eq!(effective_bytes(1 << 20, 4096), 4096);

        assert_eq!(effective_wait_ms(0, 30_000), 0);
        assert_eq!(effective_wait_ms(-1, 30_000), 0);
        assert_eq!(effective_wait_ms(500, 30_000), 500);
        assert_eq!(effective_wait_ms(60_000, 30_000), 30_000);
    }

    #[test]
    fn offset_out_of_range_carries_both_boundaries() {
        let status = offset_out_of_range(35, 40);
        assert_eq!(status.code(), tonic::Code::OutOfRange);
        assert_eq!(
            coord_core::error_code::error_code_of(&status).as_deref(),
            Some("OFFSET_OUT_OF_RANGE")
        );
        let earliest = status
            .metadata()
            .get(EARLIEST_OFFSET_METADATA_KEY)
            .and_then(|value| value.to_str().ok());
        let latest = status
            .metadata()
            .get(LATEST_OFFSET_METADATA_KEY)
            .and_then(|value| value.to_str().ok());
        assert_eq!(earliest, Some("35"));
        assert_eq!(latest, Some("40"));
    }
}
