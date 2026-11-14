//! `coord.mq.v2` 客户端写路径（M2-4）：CreateTopic / GetTopicLeader / Publish /
//! PublishBatch。
//!
//! 与服务端对齐的语义（`docs/adr/0016-mq-v2.md` D4/D5/D8/D10）：
//! - `Publish` 返回即已提交（Raft 多数派 + apply）；offset 每分区严格递增，客户端
//!   **不**自己算 offset；
//! - 分区归属某个日志分片，分片 Leader 是**按分区**不同的节点：客户端缓存
//!   `(topic, partition) → Leader 地址`，收到 `NOT_LEADER` 时用
//!   `coord-leader-hint` 更新该分区并立即重试（**不**污染全局 leader 缓存——那会
//!   把 region 0 的 leader 与某个分片的 leader 混成一个值）；
//! - `idempotency_key` 非空时同键重放返回首次 offset（`deduplicated=true`）；空键
//!   不去重，重复发布可能产生重复消息（at-least-once 生产）。
//!
//! 本模块只覆盖 M2 的写面；Poll/Ack/消费组随 M3，管理面其余 RPC 随 M4。

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use coord_core::error::{Error, Result};
use coord_core::error_code::{error_code_of, CoordErrorCode};
use coord_proto::mq::v2 as mq_v2;
use parking_lot::RwLock;

use crate::client::{classify_tonic, from_status_v2, Client};
use crate::credential::AuthedChannel;
use crate::retry::{RetryDecision, RetryState};

/// 分片 Leader hint 的 trailer key（与服务端 `LEADER_HINT_METADATA_KEY` 一致）。
pub const LEADER_HINT_METADATA_KEY: &str = "coord-leader-hint";

/// 单次 `PublishBatch` 的条数上限（契约 1–64 条）。
pub const MAX_BATCH_ENTRIES: usize = 64;

/// 单次 `AckBatch` 的条数上限（契约 1–256 条）。
pub const MAX_ACK_BATCH: usize = 256;

/// `CreateTopic` 请求（0 值的展开口径由服务端决定，见 `mq_v2` proto 头注）。
#[derive(Debug, Clone, Default)]
pub struct MqCreateTopic {
    pub topic: String,
    /// 分区数（建后不可变）。
    pub partitions: i32,
    /// 幂等键：非空时同请求重放返回首次的 topic_id/topic_epoch。
    pub request_id: String,
    /// `None` = 全用服务端默认值（含 topic 默认容量配额）。
    pub config: Option<MqTopicConfig>,
}

/// topic 配置的可选覆盖值（`0` 的口径逐字段见 proto；`None` 走服务端默认）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MqTopicConfig {
    pub retention_secs: u64,
    pub max_bytes: u64,
    pub max_message_bytes: u32,
    pub segment_bytes: u64,
    pub dlq_retention_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqTopicCreated {
    pub topic: String,
    pub topic_id: u64,
    pub topic_epoch: u64,
    pub partition_count: i32,
    pub config: MqTopicConfig,
}

/// 一个分区的路由信息（`GetTopicLeader`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqPartitionLeader {
    pub partition: i32,
    pub shard_id: u32,
    /// 分片 Leader 地址（`host:port`）；空 = 选举中。
    pub leader_addr: String,
    /// 本地副本观测到的最早可用 offset（可滞后）。
    pub earliest_offset: u64,
    /// 本地副本观测到的下一条 offset（可滞后）。
    pub next_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqTopicLeaders {
    pub topic: String,
    pub topic_id: u64,
    pub topic_epoch: u64,
    pub partitions: Vec<MqPartitionLeader>,
}

/// 一个分区的描述（`DescribeTopic` 的 `partitions[]`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqPartitionInfo {
    pub partition: i32,
    pub shard_id: u32,
    pub leader_addr: String,
    pub earliest_offset: u64,
    pub next_offset: u64,
    pub log_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqTopicDescription {
    pub topic: String,
    pub topic_id: u64,
    pub topic_epoch: u64,
    pub partition_count: i32,
    pub config: MqTopicConfig,
    pub created_at_ms: i64,
    pub partitions: Vec<MqPartitionInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqTopicSummary {
    pub topic: String,
    pub topic_id: u64,
    pub topic_epoch: u64,
    pub partition_count: i32,
    pub config: MqTopicConfig,
}

/// `UpdateTopic`：`config` 里的 0/空字段 = 保持当前值。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MqUpdateTopic {
    pub topic: String,
    /// 非 0 时做 CAS：不一致 ⇒ `TOPIC_EPOCH_STALE`。
    pub expected_topic_epoch: u64,
    pub config: Option<MqTopicConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqTopicUpdated {
    pub topic: String,
    pub topic_epoch: u64,
    pub config: MqTopicConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MqDeleteTopic {
    pub topic: String,
    pub expected_topic_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqTopicDeleted {
    pub delete_id: String,
    pub deleted_topic_epoch: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MqTopicDeletionState {
    Unspecified,
    Pending,
    Done,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqTopicDeletion {
    pub delete_id: String,
    pub topic: String,
    pub topic_id: u64,
    pub state: MqTopicDeletionState,
    pub partitions_total: u32,
    pub partitions_reclaimed: u32,
    pub messages_removed: u64,
    pub bytes_reclaimed: u64,
}

#[derive(Debug, Clone, Default)]
pub struct MqPollDlq {
    pub topic: String,
    pub partition: i32,
    pub topic_epoch: u64,
    /// `None` = 从 DLQ 起点开始。
    pub start_offset: Option<u64>,
    pub max_messages: i32,
}

/// 一条死信消息：原消息 + 移入原因/细节（`PollDlq` 独有）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqDlqMessage {
    pub message: MqMessage,
    pub reason: String,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqPollDlqResult {
    pub messages: Vec<MqDlqMessage>,
    pub next_dlq_offset: u64,
}

/// 一条待发布消息。
#[derive(Debug, Clone, Default)]
pub struct MqPublish {
    pub topic: String,
    pub partition: i32,
    /// 0 = 不校验代际（SDK 使用时建议始终带上 `GetTopicLeader`/`CreateTopic` 的返回值）。
    pub topic_epoch: u64,
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
    /// 非空时按 `(topic_id, partition, key)` 去重（窗口 = topic 保留期）。
    pub idempotency_key: String,
    pub headers: BTreeMap<String, String>,
    pub producer: String,
}

/// `PublishBatch` 的单条消息（同一次调用必须是同一 topic/partition）。
#[derive(Debug, Clone, Default)]
pub struct MqBatchEntry {
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
    pub idempotency_key: String,
    pub headers: BTreeMap<String, String>,
    pub producer: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MqPublishResult {
    pub offset: u64,
    pub timestamp_ms: i64,
    pub deduplicated: bool,
}

/// `coord.mq.v2` 客户端（`Clone` 共享同一份分片 Leader 缓存）。
#[derive(Clone)]
pub struct MqV2Client {
    client: Client,
    /// `(topic, partition) → 分片 Leader 地址`。分片 Leader 与 region 0 Leader 是
    /// 两套事实，不能复用 `Client` 的全局 leader 缓存。
    leaders: Arc<RwLock<HashMap<String, String>>>,
    /// `group → group shard Leader 地址`（组操作按组名路由，与分区无关）。
    group_leaders: Arc<RwLock<HashMap<String, String>>>,
}

impl MqV2Client {
    pub(crate) fn new(client: Client) -> Self {
        Self {
            client,
            leaders: Arc::new(RwLock::new(HashMap::new())),
            group_leaders: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 建 topic（region 0 的 leader 重定向由通用重试承担）。
    pub async fn create_topic(&self, request: &MqCreateTopic) -> Result<MqTopicCreated> {
        let payload = mq_v2::CreateTopicRequest {
            topic: request.topic.clone(),
            partitions: request.partitions,
            request_id: request.request_id.clone(),
            config: request.config.map(config_to_proto),
        };
        let created = self
            .client
            .execute_write_with_retry(move |channel| {
                let mut stub = mq_v2::mq_client::MqClient::new(channel);
                let request = tonic::Request::new(payload.clone());
                async move { stub.create_topic(request).await.map(|r| r.into_inner()) }
            })
            .await?;
        Ok(MqTopicCreated {
            topic: created.topic,
            topic_id: created.topic_id,
            topic_epoch: created.topic_epoch,
            partition_count: created.partition_count,
            config: created.config.map(config_from_proto).unwrap_or_default(),
        })
    }

    /// 读 topic 的分区 → 分片/Leader 视图（任意节点可答）。
    pub async fn get_topic_leader(&self, topic: &str) -> Result<MqTopicLeaders> {
        let payload = mq_v2::GetTopicLeaderRequest {
            topic: topic.to_string(),
        };
        let leaders = self
            .with_any_endpoint(move |mut stub| {
                let payload = payload.clone();
                async move { stub.get_topic_leader(payload).await.map(|r| r.into_inner()) }
            })
            .await?;
        let response = MqTopicLeaders {
            topic: leaders.topic,
            topic_id: leaders.topic_id,
            topic_epoch: leaders.topic_epoch,
            partitions: leaders
                .partitions
                .into_iter()
                .map(|entry| MqPartitionLeader {
                    partition: entry.partition,
                    shard_id: entry.shard_id,
                    leader_addr: entry.leader_addr,
                    earliest_offset: u64::try_from(entry.earliest_offset).unwrap_or(0),
                    next_offset: u64::try_from(entry.next_offset).unwrap_or(0),
                })
                .collect(),
        };
        for entry in &response.partitions {
            if !entry.leader_addr.is_empty() {
                self.remember_leader(&response.topic, entry.partition, &entry.leader_addr);
            }
        }
        Ok(response)
    }

    /// `DescribeTopic`：分区/配置/边界（任意节点可答）。
    pub async fn describe_topic(&self, topic: &str) -> Result<MqTopicDescription> {
        let payload = mq_v2::DescribeTopicRequest {
            topic: topic.to_string(),
        };
        let response = self
            .with_any_endpoint(move |mut stub| {
                let payload = payload.clone();
                async move {
                    stub.describe_topic(tonic::Request::new(payload))
                        .await
                        .map(|response| response.into_inner())
                }
            })
            .await?;
        let mut partitions = Vec::with_capacity(response.partitions.len());
        for partition in response.partitions {
            partitions.push(MqPartitionInfo {
                partition: partition.partition,
                shard_id: partition.shard_id,
                leader_addr: partition.leader_addr,
                earliest_offset: offset_from_i64(partition.earliest_offset)?,
                next_offset: offset_from_i64(partition.next_offset)?,
                log_bytes: partition.log_bytes,
            });
        }
        Ok(MqTopicDescription {
            topic: response.topic,
            topic_id: response.topic_id,
            topic_epoch: response.topic_epoch,
            partition_count: response.partition_count,
            config: response.config.map(config_from_proto).unwrap_or_default(),
            created_at_ms: response.created_at_ms,
            partitions,
        })
    }

    /// `ListTopics`：按前缀分页（任意节点可答）。
    pub async fn list_topics(
        &self,
        prefix: &str,
        page_size: i32,
        page_token: &str,
    ) -> Result<(Vec<MqTopicSummary>, String)> {
        let payload = mq_v2::ListTopicsRequest {
            prefix: prefix.to_string(),
            page_size,
            page_token: page_token.to_string(),
        };
        let response = self
            .with_any_endpoint(move |mut stub| {
                let payload = payload.clone();
                async move {
                    stub.list_topics(tonic::Request::new(payload))
                        .await
                        .map(|response| response.into_inner())
                }
            })
            .await?;
        let topics = response
            .topics
            .into_iter()
            .map(|topic| MqTopicSummary {
                topic: topic.topic,
                topic_id: topic.topic_id,
                topic_epoch: topic.topic_epoch,
                partition_count: topic.partition_count,
                config: topic.config.map(config_from_proto).unwrap_or_default(),
            })
            .collect();
        Ok((topics, response.next_page_token))
    }

    /// `UpdateTopic`：只改设置了的字段（0/空 = 保持当前值）。
    pub async fn update_topic(&self, request: &MqUpdateTopic) -> Result<MqTopicUpdated> {
        let payload = mq_v2::UpdateTopicRequest {
            topic: request.topic.clone(),
            expected_topic_epoch: request.expected_topic_epoch,
            config: request.config.map(config_to_proto),
        };
        let response = self
            .client
            .execute_write_with_retry(move |channel| {
                let mut stub = mq_v2::mq_client::MqClient::new(channel);
                let request = tonic::Request::new(payload.clone());
                async move {
                    stub.update_topic(request)
                        .await
                        .map(|response| response.into_inner())
                }
            })
            .await?;
        Ok(MqTopicUpdated {
            topic: response.topic,
            topic_epoch: response.topic_epoch,
            config: response.config.map(config_from_proto).unwrap_or_default(),
        })
    }

    /// `DeleteTopic`：标记删除并返回回收任务 id（回收异步）。
    pub async fn delete_topic(&self, request: &MqDeleteTopic) -> Result<MqTopicDeleted> {
        let payload = mq_v2::DeleteTopicRequest {
            topic: request.topic.clone(),
            expected_topic_epoch: request.expected_topic_epoch,
        };
        let response = self
            .client
            .execute_write_with_retry(move |channel| {
                let mut stub = mq_v2::mq_client::MqClient::new(channel);
                let request = tonic::Request::new(payload.clone());
                async move {
                    stub.delete_topic(request)
                        .await
                        .map(|response| response.into_inner())
                }
            })
            .await?;
        Ok(MqTopicDeleted {
            delete_id: response.delete_id,
            deleted_topic_epoch: response.deleted_topic_epoch,
        })
    }

    /// `GetTopicDeletion`：查回收进度（任意节点可答）。
    pub async fn get_topic_deletion(&self, delete_id: &str) -> Result<MqTopicDeletion> {
        let payload = mq_v2::GetTopicDeletionRequest {
            delete_id: delete_id.to_string(),
        };
        let response = self
            .with_any_endpoint(move |mut stub| {
                let payload = payload.clone();
                async move {
                    stub.get_topic_deletion(tonic::Request::new(payload))
                        .await
                        .map(|response| response.into_inner())
                }
            })
            .await?;
        let state = match response.state {
            1 => MqTopicDeletionState::Pending,
            2 => MqTopicDeletionState::Done,
            3 => MqTopicDeletionState::Failed,
            _ => MqTopicDeletionState::Unspecified,
        };
        Ok(MqTopicDeletion {
            delete_id: response.delete_id,
            topic: response.topic,
            topic_id: response.topic_id,
            state,
            partitions_total: response.partitions_total,
            partitions_reclaimed: response.partitions_reclaimed,
            messages_removed: response.messages_removed,
            bytes_reclaimed: response.bytes_reclaimed,
        })
    }

    /// 发布一条消息；返回即已提交。
    pub async fn publish(&self, request: &MqPublish) -> Result<MqPublishResult> {
        let batch = MqPublishBatch {
            topic: request.topic.clone(),
            partition: request.partition,
            topic_epoch: request.topic_epoch,
            entries: vec![MqBatchEntry {
                key: request.key.clone(),
                payload: request.payload.clone(),
                idempotency_key: request.idempotency_key.clone(),
                headers: request.headers.clone(),
                producer: request.producer.clone(),
            }],
        };
        let mut results = self.publish_batch(&batch).await?;
        results
            .pop()
            .ok_or_else(|| Error::Internal("publish returned no result".to_string()))
    }

    /// 同一 `(topic, partition)` 批量发布；服务端按单个分区请求原子提交。
    pub async fn publish_batch(&self, request: &MqPublishBatch) -> Result<Vec<MqPublishResult>> {
        if request.entries.is_empty() || request.entries.len() > MAX_BATCH_ENTRIES {
            return Err(Error::InvalidArgument(format!(
                "PublishBatch accepts 1..={MAX_BATCH_ENTRIES} entries (got {})",
                request.entries.len()
            )));
        }
        let payload = mq_v2::PublishBatchRequest {
            topic: request.topic.clone(),
            partition: request.partition,
            topic_epoch: request.topic_epoch,
            entries: request
                .entries
                .iter()
                .map(|entry| mq_v2::BatchEntry {
                    key: entry.key.clone(),
                    payload: entry.payload.clone(),
                    idempotency_key: entry.idempotency_key.clone(),
                    headers: entry.headers.clone().into_iter().collect(),
                    producer: entry.producer.clone(),
                })
                .collect(),
        };
        let response = self
            .partition_call(
                &request.topic,
                request.partition,
                payload,
                |mut stub, payload| async move {
                    stub.publish_batch(tonic::Request::new(payload))
                        .await
                        .map(|response| response.into_inner())
                },
            )
            .await?;
        response
            .results
            .into_iter()
            .map(publish_result_from_proto)
            .collect()
    }

    /// `Poll`：读已提交消息（`start_offset = None` ⇒ 从最早可用开始）。
    pub async fn poll(&self, request: &MqPoll) -> Result<MqPollResult> {
        let payload = mq_v2::PollRequest {
            topic: request.topic.clone(),
            partition: request.partition,
            topic_epoch: request.topic_epoch,
            start_offset: match request.start_offset {
                Some(offset) => Some(crate::client::ts_to_i64("start_offset", offset)?),
                None => None,
            },
            max_messages: request.max_messages,
            max_bytes: request.max_bytes,
            wait_ms: request.wait_ms,
        };
        let response = self
            .partition_call(
                &request.topic,
                request.partition,
                payload,
                |mut stub, payload| async move {
                    stub.poll(tonic::Request::new(payload))
                        .await
                        .map(|response| response.into_inner())
                },
            )
            .await?;
        let mut messages = Vec::with_capacity(response.messages.len());
        for message in response.messages {
            messages.push(MqMessage {
                topic: message.topic,
                partition: message.partition,
                offset: offset_from_i64(message.offset)?,
                timestamp_ms: message.timestamp_ms,
                key: message.key,
                payload: message.payload,
                headers: message.headers.into_iter().collect(),
                producer: message.producer,
            });
        }
        Ok(MqPollResult {
            messages,
            next_offset: offset_from_i64(response.next_offset)?,
            earliest_offset: offset_from_i64(response.earliest_offset)?,
            latest_offset: offset_from_i64(response.latest_offset)?,
        })
    }

    /// `PollDlq`：读死信队列（独立 offset 与保留期；`start_offset = None` = 起点）。
    pub async fn poll_dlq(&self, request: &MqPollDlq) -> Result<MqPollDlqResult> {
        let payload = mq_v2::PollDlqRequest {
            topic: request.topic.clone(),
            partition: request.partition,
            topic_epoch: request.topic_epoch,
            start_offset: match request.start_offset {
                Some(offset) => Some(crate::client::ts_to_i64("start_offset", offset)?),
                None => None,
            },
            max_messages: request.max_messages,
        };
        let response = self
            .partition_call(
                &request.topic,
                request.partition,
                payload,
                |mut stub, payload| async move {
                    stub.poll_dlq(tonic::Request::new(payload))
                        .await
                        .map(|response| response.into_inner())
                },
            )
            .await?;
        let mut messages = Vec::with_capacity(response.messages.len());
        for message in response.messages {
            messages.push(MqDlqMessage {
                message: MqMessage {
                    topic: message.topic,
                    partition: message.partition,
                    offset: offset_from_i64(message.offset)?,
                    timestamp_ms: message.timestamp_ms,
                    key: message.key,
                    payload: message.payload,
                    headers: message.headers.into_iter().collect(),
                    producer: message.producer,
                },
                reason: message.dlq_reason,
                detail: message.dlq_detail,
            });
        }
        Ok(MqPollDlqResult {
            messages,
            next_dlq_offset: offset_from_i64(response.next_dlq_offset)?,
        })
    }

    /// `JoinGroup`：订阅的 topic 必须已存在；返回分配与已提交位点。
    pub async fn join_group(&self, request: &MqJoinGroup) -> Result<MqJoined> {
        let payload = mq_v2::JoinGroupRequest {
            group: request.group.clone(),
            member_id: request.member_id.clone(),
            topics: request.topics.clone(),
            session_timeout_ms: i32::try_from(request.session_timeout_ms).unwrap_or(i32::MAX),
        };
        let group = request.group.clone();
        let response = self
            .group_call(&group, payload, |mut stub, payload| async move {
                stub.join_group(tonic::Request::new(payload))
                    .await
                    .map(|response| response.into_inner())
            })
            .await?;
        let mut assignments = Vec::with_capacity(response.assignments.len());
        for assignment in response.assignments {
            assignments.push(MqAssignment {
                topic: assignment.topic,
                partition: assignment.partition,
                committed_offset: offset_from_i64(assignment.committed_offset)?,
            });
        }
        Ok(MqJoined {
            member_id: response.member_id,
            generation: response.generation,
            assignments,
            heartbeat_interval_ms: u32::try_from(response.heartbeat_interval_ms).unwrap_or(0),
            session_timeout_ms: u32::try_from(response.session_timeout_ms).unwrap_or(0),
        })
    }

    /// `Heartbeat`：续期；过期代际返回 `GROUP_EPOCH_STALE`（调用方重新 Join）。
    pub async fn heartbeat(
        &self,
        group: &str,
        member_id: &str,
        generation: u64,
    ) -> Result<MqHeartbeat> {
        let payload = mq_v2::HeartbeatRequest {
            group: group.to_string(),
            member_id: member_id.to_string(),
            generation,
        };
        let response = self
            .group_call(group, payload, |mut stub, payload| async move {
                stub.heartbeat(tonic::Request::new(payload))
                    .await
                    .map(|response| response.into_inner())
            })
            .await?;
        Ok(MqHeartbeat {
            generation: response.generation,
            rebalance_required: response.rebalance_required,
        })
    }

    /// `LeaveGroup`：主动离组（重复调用幂等）。
    pub async fn leave_group(&self, group: &str, member_id: &str, generation: u64) -> Result<()> {
        let payload = mq_v2::LeaveGroupRequest {
            group: group.to_string(),
            member_id: member_id.to_string(),
            generation,
        };
        let group_name = group.to_string();
        self.group_call(&group_name, payload, |mut stub, payload| async move {
            stub.leave_group(tonic::Request::new(payload))
                .await
                .map(|_| ())
        })
        .await
    }

    /// `Ack`：提交一个分区位点，返回推进后的 committed（下一条待消费）。
    pub async fn ack(
        &self,
        group: &str,
        member_id: &str,
        generation: u64,
        ack: &MqAck,
    ) -> Result<u64> {
        let payload = mq_v2::AckRequest {
            topic: ack.topic.clone(),
            partition: ack.partition,
            topic_epoch: ack.topic_epoch,
            group: group.to_string(),
            member_id: member_id.to_string(),
            generation,
            offset: crate::client::ts_to_i64("offset", ack.offset)?,
        };
        let response = self
            .group_call(group, payload, |mut stub, payload| async move {
                stub.ack(tonic::Request::new(payload))
                    .await
                    .map(|response| response.into_inner())
            })
            .await?;
        offset_from_i64(response.committed_offset)
    }

    /// `MoveToDlq`：把一条消息移入该分区 DLQ（复制 + 主日志 tombstone）。
    ///
    /// 管理/维护路径，不要求 `coord:mq:manage`（ADR-0016 D9）；重复调用幂等，
    /// 返回首次的 `dlq_offset` 且 `already_moved = true`。
    pub async fn move_to_dlq(&self, request: &MqMoveToDlq) -> Result<MqMoveToDlqResult> {
        let payload = move_to_dlq_request(request)?;
        let response = self
            .partition_call(
                &request.topic,
                request.partition,
                payload,
                |mut stub, payload| async move {
                    stub.move_to_dlq(tonic::Request::new(payload))
                        .await
                        .map(|response| response.into_inner())
                },
            )
            .await?;
        Ok(MqMoveToDlqResult {
            dlq_offset: offset_from_i64(response.dlq_offset)?,
            already_moved: response.already_moved,
        })
    }

    /// `AckBatch`：1–256 条、同一组；任一条未分配 ⇒ 整批不写。
    pub async fn ack_batch(
        &self,
        group: &str,
        member_id: &str,
        generation: u64,
        acks: &[MqAck],
    ) -> Result<Vec<MqAckResult>> {
        if acks.is_empty() || acks.len() > MAX_ACK_BATCH {
            return Err(Error::InvalidArgument(format!(
                "AckBatch accepts 1..={MAX_ACK_BATCH} entries (got {})",
                acks.len()
            )));
        }
        let mut entries = Vec::with_capacity(acks.len());
        for ack in acks {
            entries.push(mq_v2::AckEntry {
                topic: ack.topic.clone(),
                partition: ack.partition,
                topic_epoch: ack.topic_epoch,
                offset: crate::client::ts_to_i64("offset", ack.offset)?,
            });
        }
        let payload = mq_v2::AckBatchRequest {
            group: group.to_string(),
            member_id: member_id.to_string(),
            generation,
            acks: entries,
        };
        let response = self
            .group_call(group, payload, |mut stub, payload| async move {
                stub.ack_batch(tonic::Request::new(payload))
                    .await
                    .map(|response| response.into_inner())
            })
            .await?;
        let mut results = Vec::with_capacity(response.results.len());
        for result in response.results {
            results.push(MqAckResult {
                topic: result.topic,
                partition: result.partition,
                committed_offset: offset_from_i64(result.committed_offset)?,
            });
        }
        Ok(results)
    }

    /// `GetCommittedOffsets`：`partitions` 为空 = 该组的全部已提交位点。
    pub async fn get_committed_offsets(
        &self,
        group: &str,
        partitions: &[(String, i32)],
    ) -> Result<Vec<MqCommitted>> {
        let payload = mq_v2::GetCommittedOffsetsRequest {
            group: group.to_string(),
            partitions: partitions
                .iter()
                .map(|(topic, partition)| mq_v2::TopicPartition {
                    topic: topic.clone(),
                    partition: *partition,
                })
                .collect(),
        };
        let response = self
            .group_call(group, payload, |mut stub, payload| async move {
                stub.get_committed_offsets(tonic::Request::new(payload))
                    .await
                    .map(|response| response.into_inner())
            })
            .await?;
        let mut offsets = Vec::with_capacity(response.offsets.len());
        for offset in response.offsets {
            offsets.push(MqCommitted {
                topic: offset.topic,
                partition: offset.partition,
                offset: offset_from_i64(offset.offset)?,
                committed: offset.committed,
            });
        }
        Ok(offsets)
    }

    /// `DescribeGroup`：成员/分配/lag（lag 读日志分片本地副本，可滞后）。
    pub async fn describe_group(&self, group: &str) -> Result<MqGroupDescription> {
        let payload = mq_v2::DescribeGroupRequest {
            group: group.to_string(),
        };
        let response = self
            .group_call(group, payload, |mut stub, payload| async move {
                stub.describe_group(tonic::Request::new(payload))
                    .await
                    .map(|response| response.into_inner())
            })
            .await?;
        let mut members = Vec::with_capacity(response.members.len());
        for member in response.members {
            let mut assignments = Vec::with_capacity(member.assignments.len());
            for assignment in member.assignments {
                assignments.push(MqAssignment {
                    topic: assignment.topic,
                    partition: assignment.partition,
                    committed_offset: offset_from_i64(assignment.committed_offset)?,
                });
            }
            members.push(MqGroupMemberInfo {
                member_id: member.member_id,
                topics: member.topics,
                assignments,
                last_heartbeat_ms: member.last_heartbeat_ms,
            });
        }
        let mut partitions = Vec::with_capacity(response.partitions.len());
        for lag in response.partitions {
            partitions.push(MqPartitionLag {
                topic: lag.topic,
                partition: lag.partition,
                committed_offset: offset_from_i64(lag.committed_offset)?,
                next_offset: offset_from_i64(lag.next_offset)?,
                lag: offset_from_i64(lag.lag)?,
            });
        }
        Ok(MqGroupDescription {
            group: response.group,
            generation: response.generation,
            state: MqGroupState::from_wire(response.state),
            members,
            partitions,
        })
    }

    /// `ListGroups`：`prefix` 过滤、`page_token` 续页（空 = 没有下一页）。
    pub async fn list_groups(
        &self,
        prefix: &str,
        page_size: i32,
        page_token: &str,
    ) -> Result<(Vec<String>, String)> {
        let payload = mq_v2::ListGroupsRequest {
            prefix: prefix.to_string(),
            page_size,
            page_token: page_token.to_string(),
        };
        let response = self
            .with_any_endpoint(move |mut stub| {
                let payload = payload.clone();
                async move {
                    stub.list_groups(tonic::Request::new(payload))
                        .await
                        .map(|response| response.into_inner())
                }
            })
            .await?;
        Ok((response.groups, response.next_page_token))
    }

    fn remember_leader(&self, topic: &str, partition: i32, endpoint: &str) {
        self.leaders
            .write()
            .insert(partition_key(topic, partition), endpoint.to_string());
    }

    /// 解析写入端点：缓存 → `GetTopicLeader` → 报错（不猜端点）。
    async fn resolve_leader(&self, key: &str, topic: &str, partition: i32) -> Result<String> {
        if let Some(endpoint) = self.leaders.read().get(key) {
            return Ok(endpoint.clone());
        }
        let leaders = self.get_topic_leader(topic).await?;
        if let Some(endpoint) = self.leaders.read().get(key) {
            return Ok(endpoint.clone());
        }
        let entry = leaders
            .partitions
            .iter()
            .find(|entry| entry.partition == partition)
            .ok_or_else(|| Error::NotFound {
                resource: "partition",
                key: format!("{topic}/{partition}"),
            })?;
        if entry.leader_addr.is_empty() {
            return Err(Error::ClusterUnavailable(format!(
                "no leader for {topic}/{partition} yet (election in progress)"
            )));
        }
        Ok(entry.leader_addr.clone())
    }

    /// 分片请求的通用重试循环（`PublishBatch`/`Poll`）：按分区缓存 Leader，
    /// `NOT_LEADER` hint 只更新该分区，选举窗口与普通 `UNAVAILABLE` 都消耗同一份
    /// 重试预算（有界）。
    async fn partition_call<T, Req, F, Fut>(
        &self,
        topic: &str,
        partition: i32,
        request: Req,
        attempt: F,
    ) -> Result<T>
    where
        Req: Clone,
        F: Fn(mq_v2::mq_client::MqClient<AuthedChannel>, Req) -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, tonic::Status>>,
    {
        let key = partition_key(topic, partition);
        let mut retry = RetryState::new(self.client.retry_config());
        let mut hint_retries = 0u32;
        loop {
            let endpoint = match self.resolve_leader(&key, topic, partition).await {
                Ok(endpoint) => endpoint,
                // 分片 Leader 尚未选出（选举窗口）：可重试；未知 topic/分区不可重试。
                Err(err @ Error::ClusterUnavailable(_)) => match retry.next_attempt() {
                    Some(backoff) => {
                        tokio::time::sleep(backoff).await;
                        continue;
                    }
                    None => return Err(err),
                },
                Err(err) => return Err(err),
            };
            let channel = match self.client.channel_for(&endpoint).await {
                Ok(channel) => channel,
                Err(err) => {
                    // 缓存里的 Leader 连不上（进程 kill -9 / 端口变化）：先驱逐再
                    // 重新发现。否则一次 leader 故障会把这个分区永久钉在死地址上。
                    self.leaders.write().remove(&key);
                    match retry.next_attempt() {
                        Some(backoff) => {
                            tokio::time::sleep(backoff).await;
                            continue;
                        }
                        None => return Err(err),
                    }
                }
            };
            let result = attempt(
                mq_v2::mq_client::MqClient::new(channel.clone()),
                request.clone(),
            )
            .await;
            self.client.return_channel(&endpoint, channel);
            match result {
                Ok(value) => return Ok(value),
                Err(status) => {
                    if let Some(hint) = leader_hint(&status) {
                        self.remember_leader(topic, partition, &hint);
                        if hint_retries < MAX_HINT_RETRIES {
                            hint_retries += 1;
                            continue;
                        }
                    } else if error_code_of(&status).as_deref()
                        == Some(CoordErrorCode::NotLeader.as_str())
                        || status.code() == tonic::Code::Unavailable
                    {
                        // 连接级 UNAVAILABLE（leader 进程被杀、端口失效）没有 hint 可
                        // 跟随；缓存必须驱逐，否则这个分区会一直钉在死地址上。
                        self.leaders.write().remove(&key);
                    }
                    match classify_tonic(&status) {
                        RetryDecision::Abort => return Err(from_status_v2(status)),
                        RetryDecision::RetryImmediately => match retry.next_attempt() {
                            Some(_) => continue,
                            None => return Err(from_status_v2(status)),
                        },
                        RetryDecision::RetryAfter(wait) => match retry.next_attempt() {
                            Some(backoff) => {
                                tokio::time::sleep(backoff.max(wait)).await;
                                continue;
                            }
                            None => return Err(from_status_v2(status)),
                        },
                    }
                }
            }
        }
    }

    /// 组命令的通用重试循环：按组缓存 group shard Leader（首次从任意端点开始，
    /// 由 `NOT_LEADER` hint 纠正）。
    async fn group_call<T, Req, F, Fut>(&self, group: &str, request: Req, attempt: F) -> Result<T>
    where
        Req: Clone,
        F: Fn(mq_v2::mq_client::MqClient<AuthedChannel>, Req) -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, tonic::Status>>,
    {
        let key = group_key(group);
        let endpoints = self.client.endpoints();
        if endpoints.is_empty() {
            return Err(Error::ClusterUnavailable(
                "no endpoints configured".to_string(),
            ));
        }
        let mut retry = RetryState::new(self.client.retry_config());
        let mut hint_retries = 0u32;
        let mut fallback = 0usize;
        loop {
            let endpoint = match self.group_leaders.read().get(&key).cloned() {
                Some(endpoint) => endpoint,
                None => {
                    let endpoint = endpoints[fallback % endpoints.len()].clone();
                    fallback = fallback.wrapping_add(1);
                    endpoint
                }
            };
            let channel = match self.client.channel_for(&endpoint).await {
                Ok(channel) => channel,
                Err(err) => {
                    // 组 shard Leader 换了地址：驱逐缓存，下一轮从任意端点重新探测。
                    self.group_leaders.write().remove(&key);
                    match retry.next_attempt() {
                        Some(backoff) => {
                            tokio::time::sleep(backoff).await;
                            continue;
                        }
                        None => return Err(err),
                    }
                }
            };
            let result = attempt(
                mq_v2::mq_client::MqClient::new(channel.clone()),
                request.clone(),
            )
            .await;
            self.client.return_channel(&endpoint, channel);
            match result {
                Ok(value) => return Ok(value),
                Err(status) => {
                    if let Some(hint) = leader_hint(&status) {
                        self.group_leaders.write().insert(key.clone(), hint);
                        if hint_retries < MAX_HINT_RETRIES {
                            hint_retries += 1;
                            continue;
                        }
                    } else if error_code_of(&status).as_deref()
                        == Some(CoordErrorCode::NotLeader.as_str())
                        || status.code() == tonic::Code::Unavailable
                    {
                        self.group_leaders.write().remove(&key);
                    }
                    match classify_tonic(&status) {
                        RetryDecision::Abort => return Err(from_status_v2(status)),
                        RetryDecision::RetryImmediately => match retry.next_attempt() {
                            Some(_) => continue,
                            None => return Err(from_status_v2(status)),
                        },
                        RetryDecision::RetryAfter(wait) => match retry.next_attempt() {
                            Some(backoff) => {
                                tokio::time::sleep(backoff.max(wait)).await;
                                continue;
                            }
                            None => return Err(from_status_v2(status)),
                        },
                    }
                }
            }
        }
    }

    /// 逐端点尝试一次（`GetTopicLeader` 这类任意节点可答的读面）。
    async fn with_any_endpoint<T, Fut, F>(&self, mut attempt: F) -> Result<T>
    where
        F: FnMut(mq_v2::mq_client::MqClient<AuthedChannel>) -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, tonic::Status>>,
    {
        let endpoints = self.client.endpoints();
        if endpoints.is_empty() {
            return Err(Error::ClusterUnavailable(
                "no endpoints configured".to_string(),
            ));
        }
        let mut last = None;
        for endpoint in endpoints {
            let channel = self.client.channel_for(&endpoint).await?;
            let stub = mq_v2::mq_client::MqClient::new(channel.clone());
            let result = attempt(stub).await;
            self.client.return_channel(&endpoint, channel);
            match result {
                Ok(value) => return Ok(value),
                Err(status) => last = Some(status),
            }
        }
        Err(from_status_v2(last.unwrap_or_else(|| {
            tonic::Status::unavailable("no endpoint answered")
        })))
    }
}

/// 一次请求最多跟随几次 `NOT_LEADER` hint。
const MAX_HINT_RETRIES: u32 = 3;

/// 缓存键：topic 名与分区号（topic 名不含 NUL，分隔符安全）。
fn partition_key(topic: &str, partition: i32) -> String {
    format!("{topic}\u{0}{partition}")
}

/// 组级 Leader 缓存键（与分区键分开：同一个 `group` 只有一份组 Leader）。
fn group_key(group: &str) -> String {
    format!("group\u{0}{group}")
}

/// 从 `NOT_LEADER` 的 trailer 取分片 Leader 地址。
fn leader_hint(status: &tonic::Status) -> Option<String> {
    status
        .metadata()
        .get(LEADER_HINT_METADATA_KEY)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
        .filter(|hint| !hint.is_empty())
}

fn config_to_proto(config: MqTopicConfig) -> mq_v2::TopicConfig {
    mq_v2::TopicConfig {
        retention_secs: i64::try_from(config.retention_secs).unwrap_or(i64::MAX),
        max_bytes: config.max_bytes,
        max_message_bytes: config.max_message_bytes,
        segment_bytes: config.segment_bytes,
        dlq_retention_secs: i64::try_from(config.dlq_retention_secs).unwrap_or(i64::MAX),
    }
}

fn config_from_proto(config: mq_v2::TopicConfig) -> MqTopicConfig {
    MqTopicConfig {
        retention_secs: u64::try_from(config.retention_secs).unwrap_or(0),
        max_bytes: config.max_bytes,
        max_message_bytes: config.max_message_bytes,
        segment_bytes: config.segment_bytes,
        dlq_retention_secs: u64::try_from(config.dlq_retention_secs).unwrap_or(0),
    }
}

fn offset_from_i64(offset: i64) -> Result<u64> {
    u64::try_from(offset)
        .map_err(|_| Error::Internal(format!("negative offset in response: {offset}")))
}

fn publish_result_from_proto(response: mq_v2::PublishResponse) -> Result<MqPublishResult> {
    Ok(MqPublishResult {
        offset: u64::try_from(response.offset).map_err(|_| {
            Error::Internal(format!("negative offset in response: {}", response.offset))
        })?,
        timestamp_ms: response.timestamp_ms,
        deduplicated: response.deduplicated,
    })
}

/// `Poll` 请求。
#[derive(Debug, Clone, Default)]
pub struct MqPoll {
    pub topic: String,
    pub partition: i32,
    pub topic_epoch: u64,
    /// `None` = 从当前最早可用开始（服务端不 clamp 显式越界值）。
    pub start_offset: Option<u64>,
    /// 0 = 服务端 `[mq_v2].poll_max_messages`。
    pub max_messages: i32,
    /// 0 = 服务端 `[mq_v2].poll_max_bytes`。
    pub max_bytes: i32,
    /// 0 = 立即返回；上限 `[mq_v2].long_poll_max_ms`。
    pub wait_ms: i32,
}

/// 一条已提交消息（`Poll` 的返回项）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: u64,
    pub timestamp_ms: i64,
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
    pub headers: BTreeMap<String, String>,
    pub producer: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqPollResult {
    pub messages: Vec<MqMessage>,
    /// 下次拉取建议的 offset（空批 = 请求起点）。
    pub next_offset: u64,
    pub earliest_offset: u64,
    pub latest_offset: u64,
}

/// `JoinGroup` 请求。
#[derive(Debug, Clone, Default)]
pub struct MqJoinGroup {
    pub group: String,
    /// 空 = 新成员（由服务端分配 `member_id`）。
    pub member_id: String,
    pub topics: Vec<String>,
    /// 0 = 服务端 `[mq_v2].group_session_timeout_ms`。
    pub session_timeout_ms: u32,
}

/// 一个分区分配（含该分区的已提交位点，0 = 尚未提交）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqAssignment {
    pub topic: String,
    pub partition: i32,
    pub committed_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqJoined {
    pub member_id: String,
    pub generation: u64,
    pub assignments: Vec<MqAssignment>,
    pub heartbeat_interval_ms: u32,
    pub session_timeout_ms: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MqHeartbeat {
    pub generation: u64,
    pub rebalance_required: bool,
}

/// 一条待提交位点（`Ack`/`AckBatch`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqAck {
    pub topic: String,
    pub partition: i32,
    pub topic_epoch: u64,
    /// 已处理完的最后一条 offset（含）；服务端把 committed 推进到 `offset + 1`。
    pub offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqAckResult {
    pub topic: String,
    pub partition: i32,
    pub committed_offset: u64,
}

/// `MoveToDlq` 请求：把 `(topic, partition, offset)` 复制进 DLQ 并在主日志写 tombstone。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqMoveToDlq {
    pub topic: String,
    pub partition: i32,
    pub topic_epoch: u64,
    pub offset: u64,
    /// 稳定原因锚点（如 `workflow_trigger_max_attempts`）。
    pub reason: String,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqMoveToDlqResult {
    /// DLQ 内首次写入的位置。
    pub dlq_offset: u64,
    /// true = 该 offset 已在 DLQ（重复调用幂等）。
    pub already_moved: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqCommitted {
    pub topic: String,
    pub partition: i32,
    pub offset: u64,
    pub committed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqGroupMemberInfo {
    pub member_id: String,
    pub topics: Vec<String>,
    pub assignments: Vec<MqAssignment>,
    pub last_heartbeat_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqPartitionLag {
    pub topic: String,
    pub partition: i32,
    pub committed_offset: u64,
    pub next_offset: u64,
    pub lag: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MqGroupState {
    Unspecified,
    Empty,
    Stable,
    PreparingRebalance,
    Dead,
}

impl MqGroupState {
    fn from_wire(value: i32) -> Self {
        match value {
            1 => Self::Empty,
            2 => Self::Stable,
            3 => Self::PreparingRebalance,
            4 => Self::Dead,
            _ => Self::Unspecified,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqGroupDescription {
    pub group: String,
    pub generation: u64,
    pub state: MqGroupState,
    pub members: Vec<MqGroupMemberInfo>,
    pub partitions: Vec<MqPartitionLag>,
}

/// `PublishBatch` 的目标（同一次调用必须是同一 topic/partition）。
#[derive(Debug, Clone, Default)]
pub struct MqPublishBatch {
    pub topic: String,
    pub partition: i32,
    pub topic_epoch: u64,
    pub entries: Vec<MqBatchEntry>,
}

/// `MoveToDlq` 请求映射（纯函数：offset 上界在这里就拒绝，不发 RPC）。
fn move_to_dlq_request(request: &MqMoveToDlq) -> Result<mq_v2::MoveToDlqRequest> {
    Ok(mq_v2::MoveToDlqRequest {
        topic: request.topic.clone(),
        partition: request.partition,
        topic_epoch: request.topic_epoch,
        offset: crate::client::ts_to_i64("offset", request.offset)?,
        reason: request.reason.clone(),
        detail: request.detail.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn move_to_dlq_maps_reason_and_offset_into_the_wire_request() {
        let wire = move_to_dlq_request(&MqMoveToDlq {
            topic: "orders".to_string(),
            partition: 1,
            topic_epoch: 9,
            offset: 42,
            reason: "workflow_trigger_max_attempts".to_string(),
            detail: "3 consecutive start failures".to_string(),
        })
        .expect("map");
        assert_eq!(wire.topic, "orders");
        assert_eq!(wire.partition, 1);
        assert_eq!(wire.topic_epoch, 9);
        assert_eq!(wire.offset, 42);
        assert_eq!(wire.reason, "workflow_trigger_max_attempts");
        assert_eq!(wire.detail, "3 consecutive start failures");
    }

    #[test]
    fn leader_hint_is_read_from_the_trailer() {
        let mut status = tonic::Status::unavailable("not leader");
        status.metadata_mut().insert(
            LEADER_HINT_METADATA_KEY,
            "127.0.0.1:50051".parse().expect("metadata value"),
        );
        assert_eq!(leader_hint(&status).as_deref(), Some("127.0.0.1:50051"));
    }

    #[test]
    fn leader_hint_ignores_missing_and_empty_values() {
        assert_eq!(leader_hint(&tonic::Status::unavailable("x")), None);
        let mut status = tonic::Status::unavailable("x");
        status
            .metadata_mut()
            .insert(LEADER_HINT_METADATA_KEY, "".parse().expect("metadata"));
        assert_eq!(leader_hint(&status), None);
    }

    #[test]
    fn partition_cache_is_scoped_to_topic_and_partition() {
        assert_ne!(partition_key("orders", 0), partition_key("orders", 1));
        assert_ne!(partition_key("orders", 0), partition_key("events", 0));
    }

    #[test]
    fn batch_limits_are_validated_before_any_rpc() {
        // 不建客户端：`publish_batch` 的条数校验在第一次 await 之前返回。
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let client = runtime.block_on(async {
            Client::new(crate::config::Config::new(vec!["127.0.0.1:1".to_string()]))
                .await
                .expect("build client")
        });
        let mq = client.mq_v2();
        let empty = MqPublishBatch {
            topic: "orders".to_string(),
            partition: 0,
            topic_epoch: 0,
            entries: Vec::new(),
        };
        assert!(matches!(
            runtime.block_on(mq.publish_batch(&empty)),
            Err(Error::InvalidArgument(_))
        ));
        let oversized = MqPublishBatch {
            entries: vec![MqBatchEntry::default(); MAX_BATCH_ENTRIES + 1],
            ..empty
        };
        assert!(matches!(
            runtime.block_on(mq.publish_batch(&oversized)),
            Err(Error::InvalidArgument(_))
        ));
    }

    #[test]
    fn config_roundtrips_through_the_wire_types() {
        let config = MqTopicConfig {
            retention_secs: 86_400,
            max_bytes: 4096,
            max_message_bytes: 1024,
            segment_bytes: 1024 * 1024,
            dlq_retention_secs: 3_600,
        };
        assert_eq!(config_from_proto(config_to_proto(config)), config);
    }
}
