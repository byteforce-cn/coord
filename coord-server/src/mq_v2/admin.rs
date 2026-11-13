//! MQ v2 topic 管理读面与更新（ADR-0016 D10；M4-3）。
//!
//! `DescribeTopic`/`ListTopics` 是只读面（任意节点可答）；`UpdateTopic` 走 region 0
//! 的 Raft 提案（单条命令）。删除/回收（`DeleteTopic`/`GetTopicDeletion`）在 M4-2。
//!
//! 一致性口径：分区的 `earliest/latest/log_bytes` 读本节点分片副本的已 apply 状态，
//! 可滞后（观测用途）；`leader_addr` 来自 raft metrics + `grpc_addr_of`。

use coord_core::error_code::{attach, CoordErrorCode};
use coord_proto::mq::v2 as mq_v2;
use tonic::Status;

use crate::raft::type_config::Command;
use crate::server::CoordNode;
use crate::storage::mq_topic::{
    MqTopicConfig, MqTopicDelete, MqTopicDeletionState, MqTopicOp, MqTopicUpdate,
};

use super::now_ms;
use super::topics::{topic_config_to_proto, topic_not_found, validate_topic_config};

impl CoordNode {
    /// `DescribeTopic`：配置 + 分区（分片/Leader/边界/记账字节）。
    pub(crate) async fn mq_describe_topic(
        &self,
        topic: &str,
    ) -> Result<mq_v2::DescribeTopicResponse, Status> {
        let table = self.mq_topic_table()?;
        let Some(meta) = table.topic(topic) else {
            return Err(topic_not_found(topic));
        };
        let mut partitions = Vec::with_capacity(meta.partitions as usize);
        for partition in 0..meta.partitions {
            let shard_id = meta
                .shard_of_partition
                .get(partition as usize)
                .copied()
                .unwrap_or(0);
            let mut leader_addr = String::new();
            let mut earliest_offset = 0u64;
            let mut next_offset = 0u64;
            let mut log_bytes = 0u64;
            if let Ok(runtime) = self.mq_shard_runtime(shard_id) {
                let leader = runtime.raft.current_leader().await;
                leader_addr = leader
                    .and_then(|id| self.grpc_addr_of(id))
                    .unwrap_or_default();
                if let Ok(Some(partition_meta)) =
                    runtime.mvcc.mq_partition_meta(meta.topic_id, partition)
                {
                    earliest_offset = partition_meta.earliest_offset;
                    next_offset = partition_meta.next_offset;
                    log_bytes = partition_meta
                        .log_bytes
                        .saturating_add(partition_meta.dlq_bytes)
                        .saturating_add(partition_meta.idem_bytes);
                }
            }
            partitions.push(mq_v2::PartitionInfo {
                partition: partition as i32,
                shard_id,
                leader_addr,
                earliest_offset: i64::try_from(earliest_offset).unwrap_or(i64::MAX),
                next_offset: i64::try_from(next_offset).unwrap_or(i64::MAX),
                log_bytes,
            });
        }
        Ok(mq_v2::DescribeTopicResponse {
            topic: meta.name.clone(),
            topic_id: meta.topic_id,
            topic_epoch: meta.epoch,
            partition_count: i32::try_from(meta.partitions).unwrap_or(i32::MAX),
            config: Some(topic_config_to_proto(meta.config)),
            created_at_ms: meta.created_at_ms,
            partitions,
        })
    }

    /// `ListTopics`：按名字升序分页；`page_token` = 上一页最后一个 topic 名。
    pub(crate) fn mq_list_topics(
        &self,
        prefix: &str,
        page_size: i32,
        page_token: &str,
    ) -> Result<mq_v2::ListTopicsResponse, Status> {
        let table = self.mq_topic_table()?;
        let page_size = if page_size <= 0 {
            100usize
        } else {
            (page_size as usize).min(1_000)
        };
        let mut topics = Vec::new();
        let mut last_name = None;
        let mut has_more = false;
        for meta in table
            .topics
            .iter()
            .filter(|topic| topic.name.starts_with(prefix))
            .filter(|topic| page_token.is_empty() || topic.name.as_str() > page_token)
        {
            if topics.len() >= page_size {
                has_more = true;
                break;
            }
            topics.push(mq_v2::TopicSummary {
                topic: meta.name.clone(),
                topic_id: meta.topic_id,
                topic_epoch: meta.epoch,
                partition_count: i32::try_from(meta.partitions).unwrap_or(i32::MAX),
                config: Some(topic_config_to_proto(meta.config)),
            });
            last_name = Some(meta.name.clone());
        }
        Ok(mq_v2::ListTopicsResponse {
            next_page_token: if has_more {
                last_name.unwrap_or_default()
            } else {
                String::new()
            },
            topics,
        })
    }

    /// `UpdateTopic`：合并配置（0 = 保持当前值）后走 region 0 Raft。
    pub(crate) async fn mq_update_topic(
        &self,
        request: mq_v2::UpdateTopicRequest,
    ) -> Result<mq_v2::UpdateTopicResponse, Status> {
        let table = self.mq_topic_table()?;
        let Some(meta) = table.topic(&request.topic) else {
            return Err(topic_not_found(&request.topic));
        };
        let config = merge_topic_config(meta.config, request.config.unwrap_or_default())?;
        let raft = self.raft.as_ref().ok_or_else(|| {
            attach(
                Status::failed_precondition(
                    "mq_v2 UpdateTopic requires a region 0 raft (multi_raft assembly missing)",
                ),
                CoordErrorCode::FailedPrecondition,
            )
        })?;
        let command = Command::MqTopic(MqTopicOp::Update(MqTopicUpdate {
            name: request.topic.clone(),
            expected_epoch: request.expected_topic_epoch,
            config,
        }));
        let response = self.client_write_with_timeout(raft, command, 0).await?;
        let outcome = match response.response() {
            crate::raft::type_config::Response::MqTopic { outcome } => outcome.clone(),
            other => {
                return Err(Status::internal(format!(
                    "unexpected raft response for UpdateTopic: {other:?}"
                )))
            }
        };
        use crate::storage::mq_topic::MqTopicOutcome;
        match outcome {
            MqTopicOutcome::Updated { epoch, config, .. } => Ok(mq_v2::UpdateTopicResponse {
                topic: request.topic,
                topic_epoch: epoch,
                config: Some(topic_config_to_proto(config)),
            }),
            MqTopicOutcome::NotFound => Err(topic_not_found(&request.topic)),
            MqTopicOutcome::EpochStale { current_epoch } => Err(attach(
                Status::aborted(format!(
                    "topic {} epoch is stale (current epoch {current_epoch})",
                    request.topic
                )),
                CoordErrorCode::TopicEpochStale,
            )),
            MqTopicOutcome::Invalid { reason } => Err(attach(
                Status::invalid_argument(reason),
                CoordErrorCode::InvalidArgument,
            )),
            other => Err(Status::internal(format!(
                "unexpected UpdateTopic outcome: {other:?}"
            ))),
        }
    }
}

impl CoordNode {
    /// `DeleteTopic`：标记删除（立即 `TOPIC_NOT_FOUND`），数据由 reaper 异步回收。
    pub(crate) async fn mq_delete_topic(
        &self,
        request: mq_v2::DeleteTopicRequest,
    ) -> Result<mq_v2::DeleteTopicResponse, Status> {
        let table = self.mq_topic_table()?;
        let Some(meta) = table.topic(&request.topic) else {
            return Err(topic_not_found(&request.topic));
        };
        if request.expected_topic_epoch != 0 && request.expected_topic_epoch != meta.epoch {
            return Err(attach(
                Status::aborted(format!(
                    "topic {} epoch {} is stale (current epoch {})",
                    request.topic, request.expected_topic_epoch, meta.epoch
                )),
                CoordErrorCode::TopicEpochStale,
            ));
        }
        let raft = self.raft.as_ref().ok_or_else(|| {
            attach(
                Status::failed_precondition(
                    "mq_v2 DeleteTopic requires a region 0 raft (multi_raft assembly missing)",
                ),
                CoordErrorCode::FailedPrecondition,
            )
        })?;
        let now = now_ms();
        let now_u64 = u64::try_from(now).map_err(|_| {
            Status::internal("system clock is before the unix epoch; cannot build a delete id")
        })?;
        // 回收任务 id 必须**按 topic 唯一**（`GetTopicDeletion` 只按它查）：
        // 同毫秒删除不同 topic 时，纯时间戳会撞车；用 topic 名的 FNV-1a 异或时间。
        let delete_id = (fnv1a64(request.topic.as_bytes()) ^ now_u64).max(1);
        let command = Command::MqTopic(MqTopicOp::Delete(MqTopicDelete {
            name: request.topic.clone(),
            delete_id,
            now_ms: now,
        }));
        let response = self.client_write_with_timeout(raft, command, 0).await?;
        use crate::storage::mq_topic::MqTopicOutcome;
        match response.response() {
            crate::raft::type_config::Response::MqTopic { outcome } => match outcome {
                MqTopicOutcome::Deleted {
                    delete_id,
                    deleted_epoch,
                    ..
                } => Ok(mq_v2::DeleteTopicResponse {
                    delete_id: delete_id.to_string(),
                    deleted_topic_epoch: *deleted_epoch,
                }),
                MqTopicOutcome::NotFound => Err(topic_not_found(&request.topic)),
                MqTopicOutcome::EpochStale { current_epoch } => Err(attach(
                    Status::aborted(format!(
                        "topic {} epoch is stale (current epoch {current_epoch})",
                        request.topic
                    )),
                    CoordErrorCode::TopicEpochStale,
                )),
                other => Err(Status::internal(format!(
                    "unexpected DeleteTopic outcome: {other:?}"
                ))),
            },
            other => Err(Status::internal(format!(
                "unexpected raft response for DeleteTopic: {other:?}"
            ))),
        }
    }

    /// `GetTopicDeletion`：查异步回收进度（`delete_id` 是 `DeleteTopic` 返回的字符串）。
    pub(crate) fn mq_get_topic_deletion(
        &self,
        delete_id: &str,
    ) -> Result<mq_v2::GetTopicDeletionResponse, Status> {
        let id = delete_id.parse::<u64>().map_err(|_| {
            attach(
                Status::invalid_argument(format!("invalid delete_id: {delete_id:?}")),
                CoordErrorCode::InvalidArgument,
            )
        })?;
        let table = self.mq_topic_table()?;
        let Some(deletion) = table.deletion_by_id(id) else {
            return Err(attach(
                Status::not_found(format!("topic deletion not found: {delete_id}")),
                CoordErrorCode::TopicNotFound,
            ));
        };
        let state = match deletion.state {
            MqTopicDeletionState::Pending => mq_v2::TopicDeletionState::Pending,
            MqTopicDeletionState::Done => mq_v2::TopicDeletionState::Done,
            MqTopicDeletionState::Failed => mq_v2::TopicDeletionState::Failed,
        };
        Ok(mq_v2::GetTopicDeletionResponse {
            delete_id: deletion.delete_id.to_string(),
            topic: deletion.name.clone(),
            topic_id: deletion.topic_id,
            state: state as i32,
            partitions_total: deletion.partitions_total,
            partitions_reclaimed: deletion
                .partitions_total
                .saturating_sub(deletion.pending_partitions.len() as u32),
            messages_removed: deletion.messages_removed,
            bytes_reclaimed: deletion.bytes_reclaimed,
        })
    }
}

/// FNV-1a 64（回收任务 id 的 topic 标识；与组 shard 哈希同一实现口径）。
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// 合并 `TopicConfig`：非 0 字段覆盖当前值（契约：0/空 = 保持当前值）。
///
/// 注意 `max_bytes`：0 在 `CreateTopic` 里表示「不限」，但在 `UpdateTopic` 里按契约
/// 表示「保持当前值」——想清空配额需删除重建（本版不做原位清空）。
pub(crate) fn merge_topic_config(
    current: MqTopicConfig,
    requested: mq_v2::TopicConfig,
) -> Result<MqTopicConfig, Status> {
    let invalid = |reason: String| {
        attach(
            Status::invalid_argument(reason),
            CoordErrorCode::InvalidArgument,
        )
    };
    let retention_secs = if requested.retention_secs == 0 {
        current.retention_secs
    } else {
        u64::try_from(requested.retention_secs)
            .map_err(|_| invalid("retention_secs must not be negative".to_string()))?
    };
    let dlq_retention_secs = if requested.dlq_retention_secs == 0 {
        current.dlq_retention_secs
    } else {
        u64::try_from(requested.dlq_retention_secs)
            .map_err(|_| invalid("dlq_retention_secs must not be negative".to_string()))?
    };
    let max_message_bytes = if requested.max_message_bytes == 0 {
        current.max_message_bytes
    } else {
        requested.max_message_bytes
    };
    let segment_bytes = if requested.segment_bytes == 0 {
        current.segment_bytes
    } else {
        requested.segment_bytes
    };
    let max_bytes = if requested.max_bytes == 0 {
        current.max_bytes
    } else {
        requested.max_bytes
    };
    let config = MqTopicConfig {
        retention_secs,
        max_bytes,
        max_message_bytes,
        segment_bytes,
        dlq_retention_secs,
    };
    validate_topic_config(&config)?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use coord_core::error_code::error_code_of;
    use tonic::Code;

    fn current() -> MqTopicConfig {
        MqTopicConfig {
            retention_secs: 604_800,
            max_bytes: 1024 * 1024,
            max_message_bytes: 4096,
            segment_bytes: 1024 * 1024,
            dlq_retention_secs: 604_800,
        }
    }

    #[test]
    fn update_merges_only_non_zero_fields() {
        let merged = merge_topic_config(
            current(),
            mq_v2::TopicConfig {
                retention_secs: 86_400,
                ..Default::default()
            },
        )
        .expect("merge");
        assert_eq!(merged.retention_secs, 86_400);
        assert_eq!(merged.max_bytes, current().max_bytes);
        assert_eq!(merged.max_message_bytes, current().max_message_bytes);
        assert_eq!(merged.segment_bytes, current().segment_bytes);
        assert_eq!(merged.dlq_retention_secs, current().dlq_retention_secs);
    }

    #[test]
    fn update_validates_the_merged_configuration() {
        let cases = [
            mq_v2::TopicConfig {
                retention_secs: 1,
                ..Default::default()
            },
            mq_v2::TopicConfig {
                max_message_bytes: 9 * 1024 * 1024,
                ..Default::default()
            },
            mq_v2::TopicConfig {
                segment_bytes: 512,
                ..Default::default()
            },
            mq_v2::TopicConfig {
                dlq_retention_secs: 2_592_001,
                ..Default::default()
            },
        ];
        for case in cases {
            let status = merge_topic_config(current(), case).expect_err("must reject");
            assert_eq!(status.code(), Code::InvalidArgument, "{case:?}");
            assert_eq!(
                error_code_of(&status).as_deref(),
                Some("INVALID_ARGUMENT"),
                "{case:?}"
            );
        }
    }
}
