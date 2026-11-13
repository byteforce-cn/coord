// coord-agent: MQ v2 投递面（durable 工作流的 outbox relay / listen）
//
// v2 集群只服务 `coord.mq.v2`（ADR-0015 D13 / ADR-0016 D11）：durable 引擎不得再依赖
// Agent 本地 v1 MQ。本模块提供与 v1 对等的两个投递面实现：
//
// - [`MqV2OutboxSink`]：outbox → MQ v2。`idempotency_key = OutboxRecord::id`
//   （event_id），relay 崩溃重投由 broker 幂等索引去重（ADR-0016 D8/D12）；
// - [`MqV2EventProvider`]：`listen` 从事件 topic 的未消费位点续读（游标复用
//   `CursorStore`，键空间 `/_workflow/v3/mq-offsets/`）。
//
// 事件形状与 v1 逐字一致：payload = `OutboxRecord` JSON，headers 携带 CloudEvents
// 属性（见 `crate::services::outbox_relay::MqOutboxSink`）。v1 路径不经过本模块，
// 行为不变（ADR-0001）。
//
// topic 形状固定为 1 分区 / 7 天保留 / 1 MiB 单条上限——与 v1 durable topic 的参数
// 一致。已存在的 topic 不改配置（不自动 `UpdateTopic`）：所有事件投 partition 0，
// per-instance 有序仍然成立。

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::Value;

use coord_client::{MqCreateTopic, MqPoll, MqPublish, MqTopicConfig, MqV2Client};
use coord_core::error::Error;
use coord_core::error_code::CoordErrorCode;
use coord_core::workflow::durable::{OutboxRecord, OutboxSink};
use coord_core::workflow::ports::{EventDelivery, EventProvider};

use super::mq_event_provider::CursorStore;
use super::mq_event_provider::{decode_event, encode_event, event_matches, subscription_key};
use crate::proxy::AgentInner;

/// durable 事件 topic 的固定形状（与 v1 `MqOutboxSink` 建 topic 的参数一致）。
pub const DURABLE_TOPIC_PARTITIONS: i32 = 1;
pub const DURABLE_TOPIC_RETENTION_SECS: u64 = 7 * 24 * 3600;
pub const DURABLE_TOPIC_MAX_MESSAGE_BYTES: u32 = 1024 * 1024;
/// 启动期等 MQ v2 topic 就绪的上限。
pub const TOPIC_READY_TIMEOUT: Duration = Duration::from_secs(30);
const TOPIC_READY_POLL: Duration = Duration::from_millis(100);
/// `CreateTopic` 的幂等键前缀：同 topic 名的重放返回首次的 topic_id/epoch。
const TOPIC_REQUEST_ID_PREFIX: &str = "workflow-durable:";
const OUTBOX_PRODUCER: &str = "coord-agent/workflow-durable";
const EVENT_PRODUCER: &str = "coord-agent/workflow-events";

// ─── MqV2OutboxSink ───

/// outbox → `coord.mq.v2` 投递面。
pub struct MqV2OutboxSink {
    mq: MqV2Client,
    topic: String,
    /// 最近一次 `CreateTopic`/`GetTopicLeader` 得到的代际（0 = 尚未引导）。
    epoch: AtomicU64,
}

impl MqV2OutboxSink {
    pub fn new(inner: Arc<AgentInner>, topic: impl Into<String>) -> Self {
        Self {
            mq: inner.client.mq_v2(),
            topic: topic.into(),
            epoch: AtomicU64::new(0),
        }
    }

    /// 引导事件 topic（幂等），返回 `topic_epoch`。单次尝试：失败由调用方决定是否重试。
    ///
    /// 同名 topic 已由别处创建（`TOPIC_EXISTS`）时接受既有配置并取当前代际；不自动
    /// `UpdateTopic`——静默改别人的保留期/配额比报错更坏。
    pub async fn ensure_topic(&self) -> Result<u64, String> {
        self.ensure_topic_inner()
            .await
            .map_err(|err| self.context(err))
    }

    /// 启动期引导：瞬时错误（选举/超时）重试到 [`TOPIC_READY_TIMEOUT`]，其余立即失败。
    ///
    /// MQ v2 未启用（`[mq_v2].enabled=false`）时服务端返回 `FAILED_PRECONDITION`：
    /// 这里立即失败并点名 MQ v2——durable 承诺的投递面不存在时不得静默启动。
    pub async fn wait_until_ready(&self) -> Result<u64, String> {
        let deadline = Instant::now() + TOPIC_READY_TIMEOUT;
        loop {
            match self.ensure_topic_inner().await {
                Ok(epoch) => return Ok(epoch),
                Err(err) if is_transient(&err) && Instant::now() < deadline => {
                    tokio::time::sleep(TOPIC_READY_POLL).await;
                }
                Err(err) => return Err(self.context(err)),
            }
        }
    }

    fn context(&self, err: Error) -> String {
        format!(
            "workflow durable: ensure events topic '{}' via coord.mq.v2 failed: {err} \
             (check [mq_v2].enabled on the servers)",
            self.topic
        )
    }

    async fn ensure_topic_inner(&self) -> Result<u64, Error> {
        let request = MqCreateTopic {
            topic: self.topic.clone(),
            partitions: DURABLE_TOPIC_PARTITIONS,
            request_id: format!("{TOPIC_REQUEST_ID_PREFIX}{}", self.topic),
            config: Some(MqTopicConfig {
                retention_secs: DURABLE_TOPIC_RETENTION_SECS,
                max_message_bytes: DURABLE_TOPIC_MAX_MESSAGE_BYTES,
                ..Default::default()
            }),
        };
        let epoch = match self.mq.create_topic(&request).await {
            Ok(created) => created.topic_epoch,
            Err(Error::V2 {
                code: CoordErrorCode::TopicExists,
                ..
            }) => self.mq.get_topic_leader(&self.topic).await?.topic_epoch,
            Err(err) => return Err(err),
        };
        self.epoch.store(epoch, Ordering::SeqCst);
        Ok(epoch)
    }
}

#[async_trait]
impl OutboxSink for MqV2OutboxSink {
    async fn deliver(&self, rec: &OutboxRecord) -> Result<(), String> {
        let mut epoch = self.epoch.load(Ordering::SeqCst);
        if epoch == 0 {
            epoch = self.ensure_topic().await?;
        }
        let request = outbox_publish_request(&self.topic, epoch, rec);
        match self.mq.publish(&request).await {
            Ok(_) => Ok(()),
            // 代际过期/topic 重建：刷新一次再投。投递本身幂等（同 event_id），
            // 刷新后重试不会产生第二条消息。
            Err(Error::V2 {
                code: CoordErrorCode::TopicEpochStale | CoordErrorCode::TopicNotFound,
                ..
            }) => {
                let refreshed = self.ensure_topic().await?;
                let retry = outbox_publish_request(&self.topic, refreshed, rec);
                self.mq
                    .publish(&retry)
                    .await
                    .map(|_| ())
                    .map_err(|err| err.to_string())
            }
            Err(err) => Err(err.to_string()),
        }
    }
}

/// `OutboxRecord` → `Publish` 请求（纯映射，可单测）。
///
/// `idempotency_key` 必须逐字是 `rec.id`：relay 重投/崩溃恢复靠 broker 侧去重，
/// 换成别的键（例如 instance_id）会让重复事件真的重复。
fn outbox_publish_request(topic: &str, topic_epoch: u64, rec: &OutboxRecord) -> MqPublish {
    let payload = serde_json::to_vec(rec).unwrap_or_default();
    let mut headers: BTreeMap<String, String> = BTreeMap::new();
    headers.insert("ce-id".to_string(), rec.id.clone());
    headers.insert("ce-type".to_string(), rec.event_type.clone());
    headers.insert("ce-source".to_string(), rec.source.clone());
    headers.insert("ce-specversion".to_string(), rec.specversion.clone());
    headers.insert("ce-dataschema".to_string(), rec.dataschema.clone());
    headers.insert("partitionkey".to_string(), rec.aggregate_id.clone());
    headers.insert("sequence".to_string(), rec.sequence.to_string());
    MqPublish {
        topic: topic.to_string(),
        partition: 0,
        topic_epoch,
        key: Vec::new(),
        payload,
        idempotency_key: rec.id.clone(),
        headers,
        producer: OUTBOX_PRODUCER.to_string(),
    }
}

// ─── MqV2EventProvider ───

/// `listen` 的 v2 投递面：从未消费位点轮询事件 topic。
pub struct MqV2EventProvider {
    mq: MqV2Client,
    topic: String,
    cursors: Arc<dyn CursorStore>,
}

impl MqV2EventProvider {
    pub fn new(inner: Arc<AgentInner>, topic: impl Into<String>) -> Self {
        Self::new_with_cursor_store(
            inner,
            topic,
            Arc::new(super::mq_event_provider::MemoryCursorStore::new()),
        )
    }

    /// 注入持久化游标存储（重启续读）。订阅键与 v1 实现逐字一致。
    pub fn new_with_cursor_store(
        inner: Arc<AgentInner>,
        topic: impl Into<String>,
        cursors: Arc<dyn CursorStore>,
    ) -> Self {
        Self {
            mq: inner.client.mq_v2(),
            topic: topic.into(),
            cursors,
        }
    }
}

#[async_trait]
impl EventProvider for MqV2EventProvider {
    async fn emit(&self, event_type: &str, source: Option<&str>, data: &Value) {
        let payload = encode_event(event_type, source, data);
        let mut headers = BTreeMap::new();
        headers.insert("ce-type".to_string(), event_type.to_string());
        headers.insert(
            "ce-source".to_string(),
            source.unwrap_or("coord/workflow").to_string(),
        );
        let request = MqPublish {
            topic: self.topic.clone(),
            partition: 0,
            topic_epoch: 0,
            key: Vec::new(),
            payload,
            idempotency_key: String::new(),
            headers,
            producer: EVENT_PRODUCER.to_string(),
        };
        // best-effort（与 v1 的 `produce` 一致）：事件总线不是 durable 的 at-least-once 路径。
        let _ = self.mq.publish(&request).await;
    }

    async fn wait_for_event(
        &self,
        event_types: &[&str],
        source: Option<&str>,
        subject: Option<&str>,
        timeout_ms: u64,
    ) -> Option<EventDelivery> {
        let subscription = subscription_key(&self.topic, event_types, source, subject);
        let mut start = Some(self.cursors.load(&subscription).await.unwrap_or(0));
        let deadline = Instant::now() + Duration::from_millis(timeout_ms);
        loop {
            let request = MqPoll {
                topic: self.topic.clone(),
                partition: 0,
                topic_epoch: 0,
                start_offset: start,
                max_messages: 64,
                max_bytes: 0,
                wait_ms: 0,
            };
            match self.mq.poll(&request).await {
                Ok(batch) => {
                    for rec in &batch.messages {
                        if let Some((et, src, sub, _data)) = decode_event(&rec.payload) {
                            if event_matches(
                                event_types,
                                source,
                                subject,
                                (&et, src.as_deref(), sub.as_deref()),
                            ) {
                                // 命中即返回，**不**提交游标：位点由调用方在状态写成功后
                                // 经 `ack_event` 提交（先提交会把崩溃窗口变成丢投递）。
                                return Some(EventDelivery {
                                    event_type: et,
                                    subscription,
                                    offset: rec.offset,
                                });
                            }
                        }
                    }
                    // 本轮无命中：只推进本地扫描，不持久化。
                    if !batch.messages.is_empty() {
                        start = Some(batch.next_offset);
                    }
                }
                // 位点早于最早可用 offset（保留期删了段）：从最早可用重新开始。
                // 这是可观测的降级（WARN），不是静默跳过——后续事件仍会被投递。
                Err(Error::V2 {
                    code: CoordErrorCode::OffsetOutOfRange,
                    ..
                }) => {
                    tracing::warn!(
                        topic = %self.topic,
                        subscription = %subscription,
                        "workflow listen cursor is below the earliest available offset; \
                         resetting to the earliest offset"
                    );
                    start = None;
                }
                // 与 v1 一致：poll 失败按超时/重试处理，不返回错误（事件总线是 best-effort）。
                Err(_) => {}
            }
            if Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn ack_event(&self, delivery: &EventDelivery) {
        let _ = self
            .cursors
            .store(&delivery.subscription, delivery.offset + 1)
            .await;
    }
}

/// 启动期可重试的错误：选举未完成、raft 超时、region 0 事务冲突/安全 ts 追赶。
fn is_transient(err: &Error) -> bool {
    matches!(
        err,
        Error::ClusterUnavailable(_)
            | Error::RequestTimeout
            | Error::V2 {
                code: CoordErrorCode::TxnConflict | CoordErrorCode::SafeTsLag,
                ..
            }
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use coord_core::workflow::durable::OutboxStatus;
    use serde_json::json;

    fn outbox_record(id: &str) -> OutboxRecord {
        OutboxRecord {
            id: id.to_string(),
            aggregate_id: "inst-1".to_string(),
            sequence: 3,
            event_type: "com.china-workflow.instance.completed.v1".to_string(),
            specversion: "1.0".to_string(),
            source: "coord/workflow".to_string(),
            subject: None,
            dataschema: "https://coord.dev/schemas/instance-completed.json".to_string(),
            time: "2026-11-13T00:00:00Z".to_string(),
            data: json!({"workflow_id": "inst-1"}),
            created_at_ms: 1_762_000_000_000,
            status: OutboxStatus::Pending,
            attempts: 0,
            next_attempt_at_ms: 1_762_000_000_000,
            delivered_at_ms: None,
        }
    }

    #[test]
    fn publish_request_carries_the_event_id_as_the_idempotency_key() {
        let rec = outbox_record("evt-1");
        let request = outbox_publish_request("com.china-workflow.events", 7, &rec);
        assert_eq!(request.topic, "com.china-workflow.events");
        assert_eq!(request.partition, 0);
        assert_eq!(request.topic_epoch, 7);
        assert_eq!(request.idempotency_key, "evt-1");
        assert_eq!(request.producer, OUTBOX_PRODUCER);

        let decoded: OutboxRecord =
            serde_json::from_slice(&request.payload).expect("payload is the record");
        assert_eq!(decoded.id, "evt-1");
        assert_eq!(decoded.aggregate_id, "inst-1");
    }

    #[test]
    fn publish_request_headers_carry_the_cloudevent_attributes() {
        let rec = outbox_record("evt-9");
        let request = outbox_publish_request("t", 1, &rec);
        assert_eq!(
            request.headers.get("ce-id").map(String::as_str),
            Some("evt-9")
        );
        assert_eq!(
            request.headers.get("ce-type").map(String::as_str),
            Some("com.china-workflow.instance.completed.v1")
        );
        assert_eq!(
            request.headers.get("ce-specversion").map(String::as_str),
            Some("1.0")
        );
        assert_eq!(
            request.headers.get("partitionkey").map(String::as_str),
            Some("inst-1")
        );
        assert_eq!(
            request.headers.get("sequence").map(String::as_str),
            Some("3")
        );
    }

    #[test]
    fn event_provider_decodes_a_cloud_event_and_matches_the_filter() {
        let payload = encode_event(
            "com.example.created",
            Some("coord/workflow"),
            &json!({"n": 1}),
        );
        let (event_type, source, subject, data) =
            decode_event(&payload).expect("valid cloud event");
        assert_eq!(event_type, "com.example.created");
        assert_eq!(source.as_deref(), Some("coord/workflow"));
        assert!(subject.is_none());
        assert_eq!(data, json!({"n": 1}));

        let got = (event_type.as_str(), source.as_deref(), subject.as_deref());
        assert!(event_matches(&["com.example.created"], None, None, got));
        assert!(event_matches(&[], Some("coord/workflow"), None, got));
        assert!(!event_matches(&["com.example.other"], None, None, got));
    }

    #[test]
    fn event_provider_reuses_the_subscription_key_of_the_v1_provider() {
        assert_eq!(
            subscription_key("t", &["b", "a"], None, None),
            subscription_key("t", &["a", "b"], None, None)
        );
        assert_ne!(
            subscription_key("t", &["a"], None, None),
            subscription_key("t", &["a"], Some("s"), None)
        );
    }
}
