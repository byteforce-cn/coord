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
}

impl MqV2Client {
    pub(crate) fn new(client: Client) -> Self {
        Self {
            client,
            leaders: Arc::new(RwLock::new(HashMap::new())),
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
        let key = partition_key(&request.topic, request.partition);
        let mut retry = RetryState::new(self.client.retry_config());
        let mut hint_retries = 0u32;
        loop {
            let endpoint = self
                .resolve_leader(&key, &request.topic, request.partition)
                .await?;
            let channel = self.client.channel_for(&endpoint).await?;
            let mut stub = mq_v2::mq_client::MqClient::new(channel.clone());
            let result = stub
                .publish_batch(tonic::Request::new(payload.clone()))
                .await;
            self.client.return_channel(&endpoint, channel);
            match result {
                Ok(response) => {
                    return response
                        .into_inner()
                        .results
                        .into_iter()
                        .map(publish_result_from_proto)
                        .collect()
                }
                Err(status) => {
                    // `NOT_LEADER` + hint：只更新**这个分区**的目标并立即重试。
                    if let Some(hint) = leader_hint(&status) {
                        self.remember_leader(&request.topic, request.partition, &hint);
                        if hint_retries < MAX_HINT_RETRIES {
                            hint_retries += 1;
                            continue;
                        }
                    } else if error_code_of(&status).as_deref()
                        == Some(CoordErrorCode::NotLeader.as_str())
                    {
                        // 没有可重定向目标（选举中）：丢掉缓存，退避后重新发现。
                        self.leaders.write().remove(&key);
                    }
                    match classify_tonic(&status) {
                        RetryDecision::Abort => return Err(from_status_v2(status)),
                        RetryDecision::RetryImmediately => continue,
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

fn publish_result_from_proto(response: mq_v2::PublishResponse) -> Result<MqPublishResult> {
    Ok(MqPublishResult {
        offset: u64::try_from(response.offset).map_err(|_| {
            Error::Internal(format!("negative offset in response: {}", response.offset))
        })?,
        timestamp_ms: response.timestamp_ms,
        deduplicated: response.deduplicated,
    })
}

/// `PublishBatch` 的目标（同一次调用必须是同一 topic/partition）。
#[derive(Debug, Clone, Default)]
pub struct MqPublishBatch {
    pub topic: String,
    pub partition: i32,
    pub topic_epoch: u64,
    pub entries: Vec<MqBatchEntry>,
}

#[cfg(test)]
mod tests {
    use super::*;

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
