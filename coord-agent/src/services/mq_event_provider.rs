// coord-agent/services/mq_event_provider.rs
// MQ 事件总线（标准 §Events：`emit`/`listen` 经 coord MQ 发布/订阅）
//
// 实现 coord-core `EventProvider` trait：
// - `emit`: 将 CloudEvent 发布到 MQ topic（跨 agent 可消费）
// - `wait_for_event`: 轮询消费并匹配事件过滤器（event_type/source/subject）
//
// 单 agent 部署使用 `MemoryEventProvider`（coord-core 内置）；本实现提供
// 持久化/跨 agent 的事件总线路径。

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::Value;

use coord_core::workflow::ports::EventProvider;

use super::mq::MessageQueueService;

/// 工作流事件总线 Topic
pub const WORKFLOW_EVENTS_TOPIC: &str = "coord.workflow.events";

/// MQ 事件提供者
pub struct MqEventProvider {
    mq: Arc<MessageQueueService>,
    topic: String,
    partition: u32,
    /// 轮询起始 offset（wait_for_event 游标）
    next_offset: Mutex<u64>,
}

impl MqEventProvider {
    /// 创建提供者（自动创建事件 topic）
    pub fn new(mq: Arc<MessageQueueService>, topic: impl Into<String>) -> Self {
        let provider = Self {
            mq,
            topic: topic.into(),
            partition: 0,
            next_offset: Mutex::new(0),
        };
        let _ = provider.mq.create_topic(
            &provider.topic,
            super::mq::TopicConfig {
                partitions: 1,
                retention_secs: 7 * 24 * 3600,
                max_message_size: 1024 * 1024,
            },
        );
        provider
    }

    /// 编码 CloudEvent 为 MQ 消息（headers 携带 event_type/source）
    fn encode_event(event_type: &str, source: Option<&str>, data: &Value) -> Vec<u8> {
        let cloud_event = serde_json::json!({
            "specversion": "1.0",
            "type": event_type,
            "source": source.unwrap_or("coord/workflow"),
            "id": uuid_v4_like(),
            "time": now_ms(),
            "data": data,
        });
        serde_json::to_vec(&cloud_event).unwrap_or_default()
    }

    /// 从 MQ 消息解码 CloudEvent 字段
    fn decode_event(payload: &[u8]) -> Option<(String, Option<String>, Option<String>, Value)> {
        let v: Value = serde_json::from_slice(payload).ok()?;
        let event_type = v.get("type")?.as_str()?.to_string();
        let source = v.get("source").and_then(|s| s.as_str()).map(String::from);
        let subject = v.get("subject").and_then(|s| s.as_str()).map(String::from);
        let data = v.get("data").cloned().unwrap_or(Value::Null);
        Some((event_type, source, subject, data))
    }
}

#[async_trait]
impl EventProvider for MqEventProvider {
    async fn emit(&self, event_type: &str, source: Option<&str>, data: &Value) {
        let payload = Self::encode_event(event_type, source, data);
        let mut headers = BTreeMap::new();
        headers.insert("ce-type".to_string(), event_type.to_string());
        headers.insert(
            "ce-source".to_string(),
            source.unwrap_or("coord/workflow").to_string(),
        );
        let _ = self
            .mq
            .produce(&self.topic, self.partition, payload, Some(headers));
    }

    async fn wait_for_event(
        &self,
        event_type: Option<&str>,
        source: Option<&str>,
        subject: Option<&str>,
        timeout_ms: u64,
    ) -> bool {
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
        loop {
            let offset = *self.next_offset.lock().unwrap();
            match self
                .mq
                .consume(&self.topic, self.partition, offset, 16)
            {
                Ok(records) => {
                    if !records.is_empty() {
                        let mut next = offset;
                        for rec in &records {
                            next = rec.offset + 1;
                            if let Some((et, src, sub, _data)) = Self::decode_event(&rec.payload) {
                                let type_match = event_type.map(|t| t == et).unwrap_or(true);
                                let source_match = source.map(|s| src.as_deref() == Some(s)).unwrap_or(true);
                                let subject_match = subject.map(|s| sub.as_deref() == Some(s)).unwrap_or(true);
                                if type_match && source_match && subject_match {
                                    *self.next_offset.lock().unwrap() = next;
                                    return true;
                                }
                            }
                        }
                        *self.next_offset.lock().unwrap() = next;
                    }
                }
                Err(_) => {}
            }
            if std::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }
}

/// UUID v4 风格 ID（与 mq.rs 一致，避免额外依赖）
fn uuid_v4_like() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("wf-ev-{}-{}", now_ms(), n)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn mq_provider(dir: &TempDir) -> (Arc<MessageQueueService>, Arc<MqEventProvider>) {
        let mq = Arc::new(MessageQueueService::new(dir.path().to_path_buf(), 1024 * 1024 * 1024));
        use crate::service::BaseService;
        mq.start().await.expect("start mq service");
        let provider = Arc::new(MqEventProvider::new(Arc::clone(&mq), "test.events"));
        (mq, provider)
    }

    #[tokio::test]
    async fn test_mq_event_emit_and_wait() {
        let dir = tempfile::tempdir().unwrap();
        let (_mq, provider) = mq_provider(&dir).await;

        // 发布事件
        provider
            .emit("order.created", Some("coord/orders"), &serde_json::json!({"orderId": "1"}))
            .await;

        // 消费并匹配
        let found = provider
            .wait_for_event(Some("order.created"), Some("coord/orders"), None, 2000)
            .await;
        assert!(found, "should find the emitted event");
    }

    #[tokio::test]
    async fn test_mq_event_filter_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let (_mq, provider) = mq_provider(&dir).await;

        provider
            .emit("order.created", Some("coord/orders"), &serde_json::json!({}))
            .await;

        // event_type 不匹配 → 超时返回 false
        let found = provider
            .wait_for_event(Some("other.type"), None, None, 300)
            .await;
        assert!(!found);
    }

    #[tokio::test]
    async fn test_mq_event_no_event_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let (_mq, provider) = mq_provider(&dir).await;

        let found = provider
            .wait_for_event(Some("never.comes"), None, None, 200)
            .await;
        assert!(!found);
    }
}
