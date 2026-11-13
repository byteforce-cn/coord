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

use std::sync::Arc;
use std::time::{Duration, Instant};

use coord_core::error_code::{attach, CoordErrorCode};
use coord_proto::mq::v2 as mq_v2;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
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

/// `Subscribe` 的推送队列长度（满即丢弃并计数：best-effort，不重投）。
const SUBSCRIBE_BUFFER: usize = 64;

/// `Subscribe` 空转时的轮询周期（best-effort 推送，不承诺低延迟）。
const SUBSCRIBE_IDLE: Duration = Duration::from_millis(50);

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

/// `Subscribe` 的一个分区：所有者分片运行时 + 固定起点。
struct SubscribePartition {
    topic: String,
    topic_id: u64,
    partition: u32,
    runtime: Arc<RegionRuntime>,
    start_offset: u64,
}

impl CoordNode {
    /// `Subscribe`：best-effort 推送（背压丢弃、不重投、不推进已提交位点）。
    ///
    /// 起点：`start_offset = 0`（未设置）⇒ 各分区最新；显式低于最早可用 ⇒ 打开时
    /// 直接 `OFFSET_OUT_OF_RANGE`；流运行期间保留回收越过 cursor 时，流以
    /// `OFFSET_OUT_OF_RANGE` 结束（客户端重新订阅）。
    pub(crate) async fn mq_subscribe(
        &self,
        request: mq_v2::SubscribeRequest,
    ) -> Result<ReceiverStream<Result<mq_v2::Message, Status>>, Status> {
        let table = self.mq_topic_table()?;
        let Some(meta) = table.topic(&request.topic) else {
            return Err(attach(
                Status::not_found(format!("topic not found: {}", request.topic)),
                CoordErrorCode::TopicNotFound,
            ));
        };
        if request.topic_epoch != 0 && request.topic_epoch != meta.epoch {
            return Err(attach(
                Status::aborted(format!(
                    "topic {} epoch {} is stale (current epoch {})",
                    request.topic, request.topic_epoch, meta.epoch
                )),
                CoordErrorCode::TopicEpochStale,
            ));
        }
        let partitions: Vec<u32> = if request.partitions.is_empty() {
            (0..meta.partitions).collect()
        } else {
            let mut requested = Vec::with_capacity(request.partitions.len());
            for partition in &request.partitions {
                let value = u32::try_from(*partition)
                    .ok()
                    .filter(|value| *value < meta.partitions)
                    .ok_or_else(|| {
                        attach(
                            Status::not_found(format!(
                                "partition {partition} does not exist on topic {} \
                                 ({} partitions)",
                                request.topic, meta.partitions
                            )),
                            CoordErrorCode::PartitionNotFound,
                        )
                    })?;
                requested.push(value);
            }
            requested
        };
        let explicit_start = u64::try_from(request.start_offset).map_err(|_| {
            attach(
                Status::invalid_argument("start_offset must not be negative"),
                CoordErrorCode::InvalidArgument,
            )
        })?;
        let mut subscribe_partitions = Vec::with_capacity(partitions.len());
        for partition in partitions {
            let target =
                self.mq_partition_target(&request.topic, partition as i32, request.topic_epoch)?;
            let bounds = target
                .runtime
                .mvcc
                .mq_partition_bounds(target.topic_id, target.partition)
                .map_err(map_err)?
                .unwrap_or(crate::storage::mq_log::MqPartitionBounds {
                    earliest_offset: 0,
                    next_offset: 0,
                });
            let start_offset = if explicit_start == 0 {
                bounds.next_offset
            } else {
                if explicit_start < bounds.earliest_offset {
                    return Err(offset_out_of_range(
                        bounds.earliest_offset,
                        bounds.next_offset,
                    ));
                }
                explicit_start
            };
            subscribe_partitions.push(SubscribePartition {
                topic: meta.name.clone(),
                topic_id: meta.topic_id,
                partition: target.partition,
                runtime: Arc::clone(&target.runtime),
                start_offset,
            });
        }
        // 数据面只服务分片 Leader（与 Poll 同口径）。
        for partition in &subscribe_partitions {
            let target = self.mq_partition_target(
                &partition.topic,
                partition.partition as i32,
                request.topic_epoch,
            )?;
            self.mq_ensure_shard_leader(&target).await?;
        }

        let (tx, rx) = mpsc::channel(SUBSCRIBE_BUFFER);
        let metrics = self.metrics().cloned();
        tokio::spawn(async move {
            if let Some(metrics) = &metrics {
                metrics.inc_mq_v2_subscribe_stream();
            }
            push_subscribed(&tx, &metrics, subscribe_partitions).await;
            if let Some(metrics) = &metrics {
                metrics.dec_mq_v2_subscribe_stream();
            }
        });
        Ok(ReceiverStream::new(rx))
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

/// 推送循环：逐分区读已提交记录，队列满即丢弃（best-effort），直到客户端断开或
/// 保留回收越过 cursor（`OFFSET_OUT_OF_RANGE` 结束流）。
async fn push_subscribed(
    tx: &mpsc::Sender<Result<mq_v2::Message, Status>>,
    metrics: &Option<Arc<crate::metrics::Metrics>>,
    partitions: Vec<SubscribePartition>,
) {
    let mut cursors: Vec<u64> = partitions
        .iter()
        .map(|partition| partition.start_offset)
        .collect();
    let mut dropped = 0u64;
    let mut delivered = 0u64;
    let mut recorded_messages = 0u64;
    let mut recorded_dropped = 0u64;
    loop {
        if tx.is_closed() {
            break;
        }
        let mut delivered_any = false;
        let mut terminal = None;
        for (index, partition) in partitions.iter().enumerate() {
            let cursor = cursors[index];
            let bounds = match partition
                .runtime
                .mvcc
                .mq_partition_bounds(partition.topic_id, partition.partition)
            {
                Ok(bounds) => bounds.unwrap_or(crate::storage::mq_log::MqPartitionBounds {
                    earliest_offset: 0,
                    next_offset: 0,
                }),
                Err(err) => {
                    terminal = Some(Status::internal(format!("read MQ partition bounds: {err}")));
                    break;
                }
            };
            if cursor < bounds.earliest_offset {
                terminal = Some(offset_out_of_range(
                    bounds.earliest_offset,
                    bounds.next_offset,
                ));
                break;
            }
            if cursor >= bounds.next_offset {
                continue;
            }
            let chunk = match partition.runtime.mvcc.mq_records(
                partition.topic_id,
                partition.partition,
                cursor,
                MAX_SUBSCRIBE_CHUNK,
            ) {
                Ok(chunk) => chunk,
                Err(err) => {
                    terminal = Some(Status::internal(format!("read MQ records: {err}")));
                    break;
                }
            };
            for record in chunk {
                cursors[index] = record.offset.saturating_add(1);
                if !record.is_visible() {
                    continue;
                }
                let message = message_from_record(&partition.topic, partition.partition, record);
                match try_push(tx, message) {
                    PushOutcome::Delivered => {
                        delivered_any = true;
                        delivered = delivered.saturating_add(1);
                    }
                    PushOutcome::Dropped => dropped = dropped.saturating_add(1),
                    PushOutcome::Closed => return,
                }
            }
        }
        if let Some(status) = terminal {
            // 结束流：先交付错误，再让接收端看到流关闭。
            let _ = tx.send(Err(status)).await;
            break;
        }
        // 增量写指标（流还开着时也能观测背压丢弃）。
        if let Some(metrics) = metrics {
            let messages_delta = delivered.saturating_sub(recorded_messages);
            let dropped_delta = dropped.saturating_sub(recorded_dropped);
            if messages_delta > 0 || dropped_delta > 0 {
                metrics.record_mq_v2_subscribe(messages_delta, dropped_delta);
                recorded_messages = delivered;
                recorded_dropped = dropped;
            }
        }
        if !delivered_any {
            tokio::time::sleep(SUBSCRIBE_IDLE).await;
        }
    }
}

/// 单次推送最多读多少条（背压丢弃的粒度）。
const MAX_SUBSCRIBE_CHUNK: usize = 64;

/// 一次推送的结果（`Subscribe` 是 best-effort：满即丢弃）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PushOutcome {
    Delivered,
    Dropped,
    Closed,
}

fn try_push(
    tx: &mpsc::Sender<Result<mq_v2::Message, Status>>,
    message: mq_v2::Message,
) -> PushOutcome {
    match tx.try_send(Ok(message)) {
        Ok(()) => PushOutcome::Delivered,
        Err(mpsc::error::TrySendError::Full(_)) => PushOutcome::Dropped,
        Err(mpsc::error::TrySendError::Closed(_)) => PushOutcome::Closed,
    }
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

/// `OFFSET_OUT_OF_RANGE`：带 earliest/latest trailer，绝不 clamp（DLQ/M4 复用）。
pub(crate) fn offset_out_of_range(earliest: u64, latest: u64) -> Status {
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
    fn subscribe_drops_when_the_queue_is_full_and_stops_when_closed() {
        let (tx, rx) = mpsc::channel::<Result<mq_v2::Message, Status>>(1);
        let message = mq_v2::Message {
            topic: "t".to_string(),
            partition: 0,
            offset: 0,
            ..Default::default()
        };
        assert_eq!(try_push(&tx, message.clone()), PushOutcome::Delivered);
        assert_eq!(
            try_push(&tx, message.clone()),
            PushOutcome::Dropped,
            "best-effort: a full queue drops instead of blocking the write path"
        );
        drop(rx);
        assert_eq!(try_push(&tx, message), PushOutcome::Closed);
    }

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
