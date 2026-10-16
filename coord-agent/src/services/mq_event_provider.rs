// coord-agent/services/mq_event_provider.rs
// MQ 事件总线（CloudEvents：`emit`/`listen` 经 coord MQ 发布/订阅）
//
// 实现 coord-core `EventProvider` trait：
// - `emit`: 将 CloudEvent 发布到 MQ topic（跨 agent 可消费）
// - `wait_for_event`: 轮询消费并匹配事件过滤器（event_type/source/subject）
//
// 单 agent 部署使用 `MemoryEventProvider`（coord-core 内置）；本实现提供
// 持久化/跨 agent 的事件总线路径。
//
// # 游标语义（修复两处旧缺陷）
//
// 旧实现用**一个** `Mutex<u64>` 当全 provider 的消费游标，有两个硬伤：
//
// 1. **共享单游标偷事件**：并发的两个 `wait_for_event`（不同过滤器 = 两个独立订阅）
//    共享同一个游标，A 消费后 B 从 A 的游标继续 —— B 自己的事件被 A「吃掉」。
// 2. **重启归零重放**：游标只在内存，进程重启即回 0，已消费事件被重放。
//
// 现在游标按**订阅**（topic + 过滤器规范化后的哈希）寻址，且可注入持久化
// [`CursorStore`]：每个订阅各走各的游标，重启后从上次位点续读。
// 匹配到事件时**只返回投递信封、不提交游标**：消费位点在调用方把实例状态写成功
// 之后经 `ack_event` 提交（`offset + 1`）⇒ 崩溃窗口从「丢一次投递」变为「重复
// 投递」，由消费方的状态守卫吸收（`spawn_event_wait` 的接线见 `coord-core` 运行时）。
// 扫描中不匹配的记录只推进本地扫描、不提交位点（下次重扫同一段无害）。

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::Value;

use coord_core::workflow::ports::{EventDelivery, EventProvider};

use super::mq::MessageQueueService;

/// 工作流事件总线 Topic
pub const WORKFLOW_EVENTS_TOPIC: &str = "coord.workflow.events";

/// 订阅游标存储 —— 每个订阅的消费位点。
#[async_trait]
pub trait CursorStore: Send + Sync {
    /// 读取订阅的下一个待消费 offset（缺省 0）。
    async fn load(&self, subscription: &str) -> Result<u64, String>;
    /// 提交订阅的下一个待消费 offset。
    async fn store(&self, subscription: &str, next_offset: u64) -> Result<(), String>;
}

/// 进程内游标（重启即失；单进程测试/无 Server 场景用）。
#[derive(Debug, Default)]
pub struct MemoryCursorStore {
    inner: Mutex<std::collections::HashMap<String, u64>>,
}

impl MemoryCursorStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl CursorStore for MemoryCursorStore {
    async fn load(&self, subscription: &str) -> Result<u64, String> {
        Ok(self
            .inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(subscription)
            .copied()
            .unwrap_or(0))
    }

    async fn store(&self, subscription: &str, next_offset: u64) -> Result<(), String> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(subscription.to_string(), next_offset);
        Ok(())
    }
}

/// 订阅键：topic + 过滤器规范化后的哈希。
///
/// 用哈希而不是把过滤器原文拼进键：事件类型里可能含 `:`、`/` 等分隔字符，拼键会
/// 产生歧义；哈希保证「同过滤器 ⇒ 同订阅、不同过滤器 ⇒ 不同订阅」。
pub fn subscription_key(
    topic: &str,
    event_types: &[&str],
    source: Option<&str>,
    subject: Option<&str>,
) -> String {
    let mut canonical = String::new();
    canonical.push_str(topic);
    canonical.push('|');
    let mut types: Vec<&str> = event_types.to_vec();
    types.sort_unstable();
    canonical.push_str(&types.join(","));
    canonical.push('|');
    canonical.push_str(source.unwrap_or("*"));
    canonical.push('|');
    canonical.push_str(subject.unwrap_or("*"));
    // FNV-1a 64：足够区分订阅，且无需引入哈希依赖
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in canonical.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{topic}:{hash:016x}")
}

/// MQ 事件提供者
pub struct MqEventProvider {
    mq: Arc<MessageQueueService>,
    topic: String,
    partition: u32,
    /// 每个订阅独立的消费游标（见模块头注的两处缺陷说明）
    cursors: Arc<dyn CursorStore>,
}

impl MqEventProvider {
    /// 创建提供者（自动创建事件 topic；游标仅进程内）。
    pub fn new(mq: Arc<MessageQueueService>, topic: impl Into<String>) -> Self {
        Self::new_with_cursor_store(mq, topic, Arc::new(MemoryCursorStore::new()))
    }

    /// 创建提供者并注入持久化游标存储（重启续读）。
    pub fn new_with_cursor_store(
        mq: Arc<MessageQueueService>,
        topic: impl Into<String>,
        cursors: Arc<dyn CursorStore>,
    ) -> Self {
        let provider = Self {
            mq,
            topic: topic.into(),
            partition: 0,
            cursors,
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
        event_types: &[&str],
        source: Option<&str>,
        subject: Option<&str>,
        timeout_ms: u64,
    ) -> Option<EventDelivery> {
        let subscription = subscription_key(&self.topic, event_types, source, subject);
        // 该订阅自己的游标：并发订阅之间不共享、不互相推进。
        let mut cursor = self.cursors.load(&subscription).await.unwrap_or(0);
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
        loop {
            if let Ok(records) = self.mq.consume(&self.topic, self.partition, cursor, 64) {
                if !records.is_empty() {
                    for rec in &records {
                        cursor = rec.offset + 1;
                        if let Some((et, src, sub, _data)) = Self::decode_event(&rec.payload) {
                            let type_match =
                                event_types.is_empty() || event_types.contains(&et.as_str());
                            let source_match =
                                source.map(|s| src.as_deref() == Some(s)).unwrap_or(true);
                            let subject_match =
                                subject.map(|s| sub.as_deref() == Some(s)).unwrap_or(true);
                            if type_match && source_match && subject_match {
                                // 命中即返回，**不**提交游标：位点由调用方在状态写成功后
                                // 经 `ack_event` 提交（先提交会让崩溃窗口丢投递）。
                                return Some(EventDelivery {
                                    event_type: et,
                                    subscription,
                                    offset: rec.offset,
                                });
                            }
                        }
                    }
                    // 本轮全是无关事件：只推进本地扫描（不持久化）；命中那条才由
                    // `ack_event` 真正推进位点。
                }
            }
            if std::time::Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    /// 提交消费位点：命中记录的下一个 offset（`offset + 1`）。
    ///
    /// 由调用方在实例状态写成功后调用；此前的崩溃只会重投同一事件。
    async fn ack_event(&self, delivery: &EventDelivery) {
        let _ = self
            .cursors
            .store(&delivery.subscription, delivery.offset + 1)
            .await;
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
        let mq = Arc::new(MessageQueueService::new(
            dir.path().to_path_buf(),
            1024 * 1024 * 1024,
        ));
        use crate::service::BaseService;
        mq.start().await.expect("start mq service");
        let provider = Arc::new(MqEventProvider::new(Arc::clone(&mq), "test.events"));
        (mq, provider)
    }

    async fn mq_started(dir: &TempDir) -> Arc<MessageQueueService> {
        let mq = Arc::new(MessageQueueService::new(
            dir.path().to_path_buf(),
            1024 * 1024 * 1024,
        ));
        use crate::service::BaseService;
        mq.start().await.expect("start mq service");
        let _ = mq.create_topic(
            "test.events",
            super::super::mq::TopicConfig {
                partitions: 1,
                retention_secs: 3600,
                max_message_size: 1024 * 1024,
            },
        );
        mq
    }

    #[tokio::test]
    async fn test_mq_event_emit_and_wait() {
        let dir = tempfile::tempdir().unwrap();
        let (_mq, provider) = mq_provider(&dir).await;

        // 发布事件
        provider
            .emit(
                "order.created",
                Some("coord/orders"),
                &serde_json::json!({"orderId": "1"}),
            )
            .await;

        // 消费并匹配
        let found = provider
            .wait_for_event(&["order.created"], Some("coord/orders"), None, 2000)
            .await;
        assert_eq!(
            found.map(|d| d.event_type).as_deref(),
            Some("order.created"),
            "should find the emitted event"
        );
    }

    #[tokio::test]
    async fn test_mq_event_filter_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let (_mq, provider) = mq_provider(&dir).await;

        provider
            .emit(
                "order.created",
                Some("coord/orders"),
                &serde_json::json!({}),
            )
            .await;

        // event_type 不匹配 → 超时返回 None
        let found = provider
            .wait_for_event(&["other.type"], None, None, 300)
            .await;
        assert!(found.is_none());
    }

    #[tokio::test]
    async fn test_mq_event_no_event_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let (_mq, provider) = mq_provider(&dir).await;

        let found = provider
            .wait_for_event(&["never.comes"], None, None, 200)
            .await;
        assert!(found.is_none());
    }

    // ─── 缺陷复现（旧实现的共享单游标 / 重启归零） ───

    /// 旧实现逐字复刻：**一个** `Mutex<u64>` 当全 provider 的消费游标。
    struct LegacySharedCursorProvider {
        mq: Arc<MessageQueueService>,
        topic: String,
        partition: u32,
        next_offset: Mutex<u64>,
    }

    impl LegacySharedCursorProvider {
        fn new(mq: Arc<MessageQueueService>, topic: &str) -> Self {
            let _ = mq.create_topic(
                topic,
                super::super::mq::TopicConfig {
                    partitions: 1,
                    retention_secs: 3600,
                    max_message_size: 1024 * 1024,
                },
            );
            Self {
                mq,
                topic: topic.to_string(),
                partition: 0,
                next_offset: Mutex::new(0),
            }
        }

        async fn emit(&self, event_type: &str) {
            let payload = MqEventProvider::encode_event(event_type, Some("test"), &Value::Null);
            let _ = self.mq.produce(&self.topic, self.partition, payload, None);
        }

        /// 旧 `wait_for_event` 的核心逻辑（共享游标版）。
        async fn wait_for_event(&self, event_types: &[&str], timeout_ms: u64) -> Option<String> {
            let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
            loop {
                let offset = *self.next_offset.lock().unwrap_or_else(|e| e.into_inner());
                if let Ok(records) = self.mq.consume(&self.topic, self.partition, offset, 16) {
                    if !records.is_empty() {
                        let mut next = offset;
                        for rec in &records {
                            next = rec.offset + 1;
                            if let Some((et, _, _, _)) = MqEventProvider::decode_event(&rec.payload)
                            {
                                if event_types.is_empty() || event_types.contains(&et.as_str()) {
                                    *self.next_offset.lock().unwrap_or_else(|e| e.into_inner()) =
                                        next;
                                    return Some(et);
                                }
                            }
                        }
                        *self.next_offset.lock().unwrap_or_else(|e| e.into_inner()) = next;
                    }
                }
                if std::time::Instant::now() >= deadline {
                    return None;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }
    }

    /// 缺陷 1 复现：共享单游标 ⇒ 订阅 B 的事件被订阅 A 的消费推进「偷走」。
    ///
    /// 断言的是**缺陷发生**（B 超时拿不到自己的事件），不是正确行为；这条测试是
    /// 「旧实现确实会丢事件」的机器可验证证据。
    #[tokio::test]
    async fn defect_repro_shared_cursor_steals_events() {
        let dir = tempfile::tempdir().unwrap();
        let mq = mq_started(&dir).await;
        let legacy = Arc::new(LegacySharedCursorProvider::new(
            Arc::clone(&mq),
            "defect.events",
        ));

        // A 的订阅先到并消费掉 B 的事件，共享游标随之推进
        legacy.emit("b.only.event").await;
        let stolen = legacy.wait_for_event(&["a.wanted.event"], 100).await;
        assert!(stolen.is_none(), "A 自己没匹配到，但游标已被推进");

        // B 的订阅随后启动：游标已经越过 B 的事件 ⇒ B 永远拿不到
        let b = legacy.wait_for_event(&["b.only.event"], 300).await;
        assert!(
            b.is_none(),
            "缺陷复现：共享游标导致 B 的事件在 A 消费时被偷（B 超时）"
        );
    }

    /// 缺陷 1 的反证（修复后）：两个不同过滤器的并发订阅互不影响。
    #[tokio::test]
    async fn fixed_provider_subscriptions_do_not_steal_events() {
        let dir = tempfile::tempdir().unwrap();
        let mq = mq_started(&dir).await;
        let provider = Arc::new(MqEventProvider::new(Arc::clone(&mq), "fixed.events"));

        provider
            .emit("b.only.event", Some("test"), &Value::Null)
            .await;

        // A 用不同过滤器轮询（跳过 B 的事件）——不得推进 B 的订阅游标
        let a = provider
            .wait_for_event(&["a.wanted.event"], None, None, 100)
            .await;
        assert!(a.is_none());

        let b = provider
            .wait_for_event(&["b.only.event"], None, None, 1000)
            .await;
        assert_eq!(
            b.map(|d| d.event_type).as_deref(),
            Some("b.only.event"),
            "修复后：B 的订阅游标独立，事件不被偷"
        );
    }

    /// 缺陷 2 复现：游标只在内存 ⇒ 重建 provider（= 进程重启）后归零重放。
    #[tokio::test]
    async fn defect_repro_offset_resets_on_restart() {
        let dir = tempfile::tempdir().unwrap();
        let mq = mq_started(&dir).await;

        let first = LegacySharedCursorProvider::new(Arc::clone(&mq), "restart.events");
        first.emit("once.event").await;
        assert_eq!(
            first.wait_for_event(&["once.event"], 1000).await.as_deref(),
            Some("once.event")
        );

        // 「重启」：同一 topic，全新 provider（内存游标 = 0）
        let restarted = LegacySharedCursorProvider::new(Arc::clone(&mq), "restart.events");
        let replayed = restarted.wait_for_event(&["once.event"], 300).await;
        assert_eq!(
            replayed.as_deref(),
            Some("once.event"),
            "缺陷复现：重启后游标归零，已消费事件被重放"
        );
    }

    /// 缺陷 2 的反证（修复后）：持久化游标存储 ⇒ 重启后从上次位点续读，不重放。
    #[tokio::test]
    async fn fixed_provider_cursor_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let mq = mq_started(&dir).await;
        let cursors: Arc<dyn CursorStore> = Arc::new(MemoryCursorStore::new());

        let first = MqEventProvider::new_with_cursor_store(
            Arc::clone(&mq),
            "durable.events",
            Arc::clone(&cursors),
        );
        first.emit("once.event", Some("test"), &Value::Null).await;
        let delivery = first
            .wait_for_event(&["once.event"], None, None, 1000)
            .await
            .expect("first delivery");
        assert_eq!(delivery.event_type, "once.event");
        first.ack_event(&delivery).await;

        // 「重启」：新 provider，同一持久化游标存储
        let restarted =
            MqEventProvider::new_with_cursor_store(Arc::clone(&mq), "durable.events", cursors);
        let replayed = restarted
            .wait_for_event(&["once.event"], None, None, 300)
            .await;
        assert!(
            replayed.is_none(),
            "修复后：游标持久化，ack 后重启不重放已消费事件"
        );
    }

    /// 位点只在**显式** `ack_event` 后推进：未 ack ⇒ 「重启」后同一事件重投；
    /// ack 后 ⇒ 不再投递。前者是崩溃窗口从「丢投递」变为「重复投递」的根据。
    #[tokio::test]
    async fn delivery_is_acked_only_after_explicit_ack() {
        let dir = tempfile::tempdir().unwrap();
        let mq = mq_started(&dir).await;
        let cursors: Arc<dyn CursorStore> = Arc::new(MemoryCursorStore::new());

        let first = MqEventProvider::new_with_cursor_store(
            Arc::clone(&mq),
            "ack.events",
            Arc::clone(&cursors),
        );
        first.emit("ack.me", Some("test"), &Value::Null).await;

        let delivery = first
            .wait_for_event(&["ack.me"], None, None, 1000)
            .await
            .expect("event delivered");
        assert_eq!(delivery.event_type, "ack.me");

        // 未 ack：位点未提交 ⇒ 新 provider（= 重启）仍能再次取到
        let restarted = MqEventProvider::new_with_cursor_store(
            Arc::clone(&mq),
            "ack.events",
            Arc::clone(&cursors),
        );
        let redelivered = restarted
            .wait_for_event(&["ack.me"], None, None, 1000)
            .await
            .expect("未 ack 的投递在「重启」后必须重投");

        // ack 后：位点提交到 offset+1 ⇒ 不再取到
        restarted.ack_event(&redelivered).await;
        let after_ack =
            MqEventProvider::new_with_cursor_store(Arc::clone(&mq), "ack.events", cursors);
        assert!(
            after_ack
                .wait_for_event(&["ack.me"], None, None, 300)
                .await
                .is_none(),
            "ack 之后位点已提交，不得再投递"
        );
    }

    #[test]
    fn subscription_key_is_stable_and_filter_sensitive() {
        let a = subscription_key("t", &["x", "y"], None, None);
        let b = subscription_key("t", &["y", "x"], None, None);
        assert_eq!(a, b, "过滤器顺序不影响订阅身份");
        let c = subscription_key("t", &["x"], None, None);
        assert_ne!(a, c, "不同过滤器 = 不同订阅");
        let d = subscription_key("t2", &["x", "y"], None, None);
        assert_ne!(a, d, "不同 topic = 不同订阅");
    }
}
