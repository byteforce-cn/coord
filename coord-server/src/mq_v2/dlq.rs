//! MQ v2 死信（ADR-0016 D9；M4-1）。
//!
//! 语义：`MoveToDlq` 是**显式管理动作**（能力 `coord:mq:dlq`）：把一条主日志消息复制到
//! 该分区的 DLQ 日志，并在主日志写 tombstone；`Poll` 跳过 tombstone（M3-1），
//! `PollDlq` 读 DLQ（原 topic/partition/offset + reason/detail）。
//!
//! 幂等：同 `(topic_id, partition, offset)` 重复调用返回**首次** `dlq_offset`
//! （`already_moved = true`）。不做通用消费者的自动投毒判定（只有工作流触发器在
//! `max_attempts` 后自动移入，M5-3）。

use coord_core::error_code::{attach, CoordErrorCode};
use coord_proto::mq::v2 as mq_v2;
use tonic::Status;

use crate::raft::type_config::{Command, Response as RaftResponse};
use crate::server::{map_err, CoordNode};
use crate::storage::mq_log::{MqDlqRecord, MqMoveToDlq, MqMoveToDlqOutcome};

use super::consume::offset_out_of_range;
use super::now_ms;

impl CoordNode {
    /// `MoveToDlq`：复制到 DLQ + 主日志 tombstone（DLQ 独立 offset）。
    pub(crate) async fn mq_move_to_dlq(
        &self,
        request: mq_v2::MoveToDlqRequest,
    ) -> Result<mq_v2::MoveToDlqResponse, Status> {
        let target =
            self.mq_partition_target(&request.topic, request.partition, request.topic_epoch)?;
        self.mq_ensure_shard_leader(&target).await?;
        let offset = u64::try_from(request.offset).map_err(|_| {
            attach(
                Status::invalid_argument(format!(
                    "offset must not be negative (got {})",
                    request.offset
                )),
                CoordErrorCode::InvalidArgument,
            )
        })?;
        let response = self
            .client_write_with_timeout(
                &target.runtime.raft,
                Command::MqMoveToDlq(MqMoveToDlq {
                    shard_id: target.shard_id,
                    topic_id: target.topic_id,
                    partition: target.partition,
                    offset,
                    reason: request.reason,
                    detail: request.detail,
                    now_ms: now_ms(),
                }),
                target.region_id,
            )
            .await?;
        let outcome = match response.response() {
            RaftResponse::MqMoveToDlq { outcome } => *outcome,
            other => {
                return Err(Status::internal(format!(
                    "unexpected raft response for MqMoveToDlq: {other:?}"
                )))
            }
        };
        match outcome {
            MqMoveToDlqOutcome::Moved { dlq_offset } => {
                if let Some(metrics) = self.metrics() {
                    metrics.record_mq_v2_dlq_move(1);
                }
                Ok(mq_v2::MoveToDlqResponse {
                    dlq_offset: i64::try_from(dlq_offset).unwrap_or(i64::MAX),
                    already_moved: false,
                })
            }
            MqMoveToDlqOutcome::AlreadyMoved { dlq_offset } => Ok(mq_v2::MoveToDlqResponse {
                dlq_offset: i64::try_from(dlq_offset).unwrap_or(i64::MAX),
                already_moved: true,
            }),
            MqMoveToDlqOutcome::Missing => {
                let bounds = target
                    .runtime
                    .mvcc
                    .mq_partition_bounds(target.topic_id, target.partition)
                    .map_err(map_err)?
                    .unwrap_or(crate::storage::mq_log::MqPartitionBounds {
                        earliest_offset: 0,
                        next_offset: 0,
                    });
                Err(offset_out_of_range(
                    bounds.earliest_offset,
                    bounds.next_offset,
                ))
            }
        }
    }

    /// `PollDlq`：读 DLQ（已提交副本；不推进任何位点）。
    pub(crate) async fn mq_poll_dlq(
        &self,
        request: mq_v2::PollDlqRequest,
    ) -> Result<mq_v2::PollDlqResponse, Status> {
        let target =
            self.mq_partition_target(&request.topic, request.partition, request.topic_epoch)?;
        self.mq_ensure_shard_leader(&target).await?;
        let config = self.mq_v2().config();
        let max_messages = if request.max_messages <= 0 {
            config.poll_max_messages as usize
        } else {
            (request.max_messages as usize).min(config.poll_max_messages as usize)
        };
        let start = match request.start_offset {
            None => 0,
            Some(raw) => u64::try_from(raw).map_err(|_| {
                attach(
                    Status::invalid_argument(format!(
                        "start_offset must not be negative (got {raw})"
                    )),
                    CoordErrorCode::InvalidArgument,
                )
            })?,
        };
        let records = target
            .runtime
            .mvcc
            .mq_dlq_records(target.topic_id, target.partition, start, max_messages)
            .map_err(map_err)?;
        let next_dlq_offset = records
            .last()
            .map(|record| record.dlq_offset.saturating_add(1))
            .unwrap_or(start);
        let messages: Vec<mq_v2::Message> = records
            .into_iter()
            .map(|record| dlq_message(&target.topic_name, target.partition, record))
            .collect();
        if let Some(metrics) = self.metrics() {
            metrics.record_mq_v2_poll_dlq(messages.len() as u64);
        }
        Ok(mq_v2::PollDlqResponse {
            messages,
            next_dlq_offset: i64::try_from(next_dlq_offset).unwrap_or(i64::MAX),
        })
    }
}

fn dlq_message(topic: &str, partition: u32, record: MqDlqRecord) -> mq_v2::Message {
    mq_v2::Message {
        topic: topic.to_string(),
        partition: partition as i32,
        // 契约：DLQ 消息的 `offset` 是**原**主日志 offset（DLQ 自身位点见
        // `next_dlq_offset`）。
        offset: i64::try_from(record.offset).unwrap_or(i64::MAX),
        timestamp_ms: record.timestamp_ms,
        key: record.key,
        payload: record.payload,
        headers: record.headers.into_iter().collect(),
        producer: record.producer,
        dlq_reason: record.reason,
        dlq_detail: record.detail,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dlq_message_keeps_the_original_offset_and_reason() {
        let message = dlq_message(
            "orders",
            1,
            MqDlqRecord {
                dlq_offset: 7,
                topic_id: 3,
                partition: 1,
                offset: 42,
                timestamp_ms: 9,
                key: b"k".to_vec(),
                payload: b"p".to_vec(),
                headers: Default::default(),
                producer: "test".to_string(),
                reason: "poison".to_string(),
                detail: "bad payload".to_string(),
                moved_at_ms: 10,
            },
        );
        assert_eq!(message.topic, "orders");
        assert_eq!(message.partition, 1);
        assert_eq!(message.offset, 42, "DLQ messages carry the original offset");
        assert_eq!(message.dlq_reason, "poison");
        assert_eq!(message.dlq_detail, "bad payload");
    }
}
