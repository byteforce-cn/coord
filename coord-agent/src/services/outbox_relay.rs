// coord-agent: Outbox 投递面（MQ Poll+Ack）
//
// 投递面选择：MQ（`MessageQueueService`，redb 持久化 + topic/partition/offset）
// 而非 `coord.event.v1`，理由与边界见
// `spike/2026-10/spike-3-recovery/OUTBOX.md`「投递面选择」：
//
// - 引擎所在进程（coord-agent）**本进程内**就能 `produce`，无需再起 agent gRPC
//   面；`coord.event.v1` 的 `EventPublish` 只在 agent 面暴露，引擎侧没有直接
//   客户端入口；
// - MQ 的 `Poll + Ack` 是 consumer-contract §5 声明的**唯一正确性路径**
//   （Subscribe 推送在背压时丢弃，不做不丢承诺）；
// - `produce_idempotent(idempotency_key = event_id)` 让 broker 也按事件 ID 去重，
//   是「重复投递」的第二道闸。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use tokio::task::JoinHandle;

use coord_core::workflow::durable::{OutboxRecord, OutboxRelay, OutboxSink};

use super::mq::{MessageQueueService, TopicConfig};

/// Outbox → MQ 投递面。
pub struct MqOutboxSink {
    mq: Arc<MessageQueueService>,
    topic: String,
    /// partitionkey（= instance_id）→ 分区；单分区 topic 恒 0。
    partitions: u32,
}

impl MqOutboxSink {
    pub fn new(mq: Arc<MessageQueueService>, topic: impl Into<String>) -> Self {
        let topic = topic.into();
        let partitions = 1u32;
        let _ = mq.create_topic(
            &topic,
            TopicConfig {
                partitions,
                retention_secs: 7 * 24 * 3600,
                max_message_size: 1024 * 1024,
            },
        );
        Self {
            mq,
            topic,
            partitions,
        }
    }

    /// 确定性分区选择：partition = FNV(instance_id) % partitions。
    ///
    /// 同一实例的事件落在同一分区 ⇒ 分区内有序 = per-instance 有序。
    fn partition_for(&self, record: &OutboxRecord) -> u32 {
        if self.partitions <= 1 {
            return 0;
        }
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for b in record.aggregate_id.as_bytes() {
            hash ^= *b as u64;
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        (hash % self.partitions as u64) as u32
    }
}

#[async_trait]
impl OutboxSink for MqOutboxSink {
    async fn deliver(&self, rec: &OutboxRecord) -> Result<(), String> {
        let payload = serde_json::to_vec(rec).map_err(|e| e.to_string())?;
        let mut headers = BTreeMap::new();
        headers.insert("ce-id".to_string(), rec.id.clone());
        headers.insert("ce-type".to_string(), rec.event_type.clone());
        headers.insert("ce-source".to_string(), rec.source.clone());
        headers.insert("ce-specversion".to_string(), rec.specversion.clone());
        headers.insert("ce-dataschema".to_string(), rec.dataschema.clone());
        headers.insert("partitionkey".to_string(), rec.aggregate_id.clone());
        headers.insert("sequence".to_string(), rec.sequence.to_string());
        // broker 侧按 event_id 去重：relay 重投不产生第二个 offset。
        self.mq
            .produce_idempotent(
                &self.topic,
                self.partition_for(rec),
                payload,
                Some(headers),
                Some(&rec.id),
            )
            .map(|_offset| ())
            .map_err(|e| e.to_string())
    }
}

/// 运行 relay 循环直到进程结束。
///
/// `tick()` 的返回**不得静默丢弃**：它同时承载投递结果与保留清理计数，
/// 一次失败的 tick 意味着本轮到期的投递与回收都停摆——留一条 WARN 让运维
/// 可见；`dead > 0`（逐条投递耗尽尝试上限）同样是 WARN，否则单条投递失败
/// 要等到 24h 保留窗口清理时才可见；`pruned > 0` 是真实的回收事件，用 INFO
/// 留证（`RelayReport` 无指标出口，日志是当前唯一的可读路径）。
pub async fn run_outbox_relay_loop(relay: Arc<OutboxRelay>, interval: Duration) {
    loop {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        match relay.tick(now_ms).await {
            Ok(report) => {
                if report.dead > 0 {
                    tracing::warn!(
                        dead = report.dead,
                        "outbox relay moved records to dead-letter after exhausting delivery \
                         attempts; they remain until the retention prune"
                    );
                }
                if report.pruned > 0 {
                    tracing::info!(
                        pruned = report.pruned,
                        delivered = report.delivered,
                        retried = report.retried,
                        dead = report.dead,
                        "outbox relay reclaimed records past retention"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "outbox relay tick failed: due deliveries and retention reclamation \
                     are stalled until a tick succeeds"
                );
            }
        }
        tokio::time::sleep(interval).await;
    }
}

/// 启动 relay 后台任务。
pub fn spawn_outbox_relay(relay: Arc<OutboxRelay>, interval: Duration) -> JoinHandle<()> {
    tokio::spawn(run_outbox_relay_loop(relay, interval))
}

/// 证据/对账用：从头（offset 0）拉取整个 topic 的消息。
pub fn drain_topic(
    mq: &MessageQueueService,
    topic: &str,
    partition: u32,
) -> Result<Vec<super::mq::MessageRecord>, String> {
    mq.consume(topic, partition, 0, 100_000)
        .map_err(|e| e.to_string())
}
