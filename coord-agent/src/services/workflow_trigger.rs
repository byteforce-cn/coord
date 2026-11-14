// coord-agent/src/services/workflow_trigger.rs
//
// MQ 触发器分发器（M5-3；ADR-0016 D12、M5-1 的投递面）。
//
// 责任链：对账已部署定义 → 每个声明了 `trigger` 的定义一个 `coord.mq.v2` 消费组
// （组名 `wf-trigger:<definition_id>`）→ `Poll` → 过滤命中 → 以
// `mq:<topic_id>:<partition>:<offset>` 为 Start 幂等键启动实例 → **Start 持久化后**
// 才 `Ack`；连续失败到 `max_attempts` → `MoveToDlq` + `Ack`（毒消息可审计，分区不卡死）。
//
// 边界（逐条都是契约，改动需同步 ADR/文档）：
// - topic 必须已存在：这里**不** `CreateTopic`；不存在就退避重试并 WARN。
// - 至少一次：重复投递是正常形态，由 Start 幂等键吸收（同键 = 同一实例）。
// - 过滤不命中的消息照常 `Ack`（本组已消费，不能卡住分区后续消息）。
// - `Ack` / `MoveToDlq` 失败 ⇒ 交给对账循环重建消费者（位点不推进 = 重投，不丢）。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::{watch, Mutex};

use coord_client::mq_consumer::{MqConsumer, MqConsumerOptions, ResetPolicy};
use coord_client::mq_v2::{MqMoveToDlq, MqV2Client};
use coord_client::MqMessage;
use coord_core::workflow::model::WorkflowDefinition;
use coord_core::workflow::trigger::{TriggerConfig, DLQ_REASON_MAX_ATTEMPTS};

use super::workflow::phase4::WorkflowEngineService;
use crate::proxy::AgentInner;

/// 定义对账周期：新部署/删除的定义最多一个周期后被接管/停用。
pub const RECONCILE_INTERVAL: Duration = Duration::from_secs(5);

/// 单条消息重试的退避基数（第 n 次失败睡 `base * n`）。
pub const RETRY_BACKOFF: Duration = Duration::from_millis(200);

/// 消费失败后重建消费者的退避上限（选举/再平衡窗口）。
const REJOIN_BACKOFF_MAX: Duration = Duration::from_secs(5);

/// 空批长轮询上限（毫秒）：决定停用触发器的响应延迟。
const POLL_WAIT_MS: i32 = 500;

/// 启动实例（可注入：单测用桩替代真实引擎）。
#[async_trait]
pub(crate) trait TriggerStarter: Send + Sync {
    async fn start(
        &self,
        definition_id: &str,
        input: Value,
        idempotency_key: &str,
    ) -> Result<(), String>;
}

/// 消费通道（可注入：单测用桩替代真实 `MqConsumer`）。
#[async_trait]
pub(crate) trait TriggerChannel: Send + Sync {
    async fn ack(&self, message: &MqMessage) -> Result<(), String>;
    async fn move_to_dlq(
        &self,
        message: &MqMessage,
        reason: &str,
        detail: &str,
    ) -> Result<(), String>;
}

/// MQ 触发器分发器：对账循环 + 每定义一个消费任务。
pub struct TriggerDispatcher {
    inner: Arc<AgentInner>,
    engine: Arc<WorkflowEngineService>,
    /// `definition_id → 停用信号`。
    tasks: parking_lot::Mutex<HashMap<String, watch::Sender<bool>>>,
}

impl TriggerDispatcher {
    pub fn new(inner: Arc<AgentInner>, engine: Arc<WorkflowEngineService>) -> Self {
        Self {
            inner,
            engine,
            tasks: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    /// 启动对账循环（只在 v2 + durable + mq 装配时调用）。
    pub fn spawn(self: &Arc<Self>) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(RECONCILE_INTERVAL);
            loop {
                ticker.tick().await;
                if let Err(err) = this.reconcile_once().await {
                    tracing::warn!(error = %err, "workflow trigger reconcile failed; retrying");
                }
            }
        });
    }

    /// 单次对账：停掉不再声明的定义，给新出现的定义起消费任务。
    pub(crate) async fn reconcile_once(&self) -> Result<(), String> {
        let defs = self
            .engine
            .list_definitions("", usize::MAX, None)
            .await
            .map_err(|err| format!("list definitions: {err}"))?;
        let desired: Vec<(String, TriggerConfig)> = defs
            .iter()
            .filter_map(|def| def.id.clone().zip(desired_trigger(def)))
            .collect();
        let desired_ids: HashSet<&str> = desired.iter().map(|(id, _)| id.as_str()).collect();

        {
            let mut tasks = self.tasks.lock();
            tasks.retain(|id, stop| {
                if desired_ids.contains(id.as_str()) {
                    return true;
                }
                // 停用：任务在下一个 poll 周期看到信号后 `close()`（主动离组）。
                let _ = stop.send(true);
                false
            });
        }

        for (definition_id, trigger) in desired {
            let mut tasks = self.tasks.lock();
            if tasks.contains_key(&definition_id) {
                continue;
            }
            let (stop_tx, stop_rx) = watch::channel(false);
            tasks.insert(definition_id.clone(), stop_tx);
            drop(tasks);
            tokio::spawn(run_trigger(
                Arc::clone(&self.inner),
                Arc::clone(&self.engine),
                definition_id,
                trigger,
                stop_rx,
            ));
        }
        Ok(())
    }
}

/// 定义的触发器（空配置 / 无 id ⇒ 不装配）。
pub(crate) fn desired_trigger(def: &WorkflowDefinition) -> Option<TriggerConfig> {
    if def.id.is_none() || def.trigger.is_empty() || def.trigger.topic.is_none() {
        return None;
    }
    Some(def.trigger.clone())
}

/// Start 幂等键（ADR-0016 D12 逐字）：`mq:<topic_id>:<partition>:<offset>`。
pub(crate) fn idempotency_key(topic_id: u64, partition: i32, offset: u64) -> String {
    format!("mq:{topic_id}:{partition}:{offset}")
}

/// 消息信封（触发器输入，不做模板语言）。
///
/// `payload` 按编码可逆地呈现：合法 JSON ⇒ 原值 + `payload_encoding="json"`；
/// 合法 UTF-8 ⇒ 字符串 + `"utf8"`；否则 base64 + `"base64"`（不静默丢二进制）。
pub(crate) fn envelope(message: &MqMessage) -> Value {
    let (payload, encoding) = match serde_json::from_slice::<Value>(&message.payload) {
        Ok(value) => (value, "json"),
        Err(_) => match std::str::from_utf8(&message.payload) {
            Ok(text) => (Value::String(text.to_string()), "utf8"),
            Err(_) => (Value::String(base64_encode(&message.payload)), "base64"),
        },
    };
    json!({
        "topic": message.topic,
        "partition": message.partition,
        "offset": message.offset,
        "timestamp_ms": message.timestamp_ms,
        "key": hex_encode(&message.key),
        "headers": message.headers,
        "producer": message.producer,
        "payload": payload,
        "payload_encoding": encoding,
    })
}

/// 处理一条消息：过滤 → Start（重试）→ Ack；毒消息 → DLQ + Ack。
///
/// 返回 `Err` 只表示**通道**故障（ack / DLQ 失败）：此时位点不推进，调用方应丢弃
/// 消费者并让对账循环重建（重投由 Start 幂等吸收）。
pub(crate) async fn handle_message(
    starter: &dyn TriggerStarter,
    channel: &dyn TriggerChannel,
    definition_id: &str,
    trigger: &TriggerConfig,
    message: &MqMessage,
    topic_id: u64,
    backoff_base: Duration,
) -> Result<(), String> {
    if !trigger.matches(&message.headers) {
        return channel.ack(message).await;
    }

    let key = idempotency_key(topic_id, message.partition, message.offset);
    let max_attempts = trigger.effective_max_attempts();
    let mut attempts: u32 = 0;
    loop {
        match starter.start(definition_id, envelope(message), &key).await {
            Ok(()) => return channel.ack(message).await,
            Err(err) => {
                attempts = attempts.saturating_add(1);
                if attempts >= max_attempts {
                    channel
                        .move_to_dlq(
                            message,
                            DLQ_REASON_MAX_ATTEMPTS,
                            &format!("{attempts} consecutive Start failures: {err}"),
                        )
                        .await?;
                    tracing::error!(
                        topic = %message.topic,
                        partition = message.partition,
                        offset = message.offset,
                        attempts,
                        error = %err,
                        reason = DLQ_REASON_MAX_ATTEMPTS,
                        "workflow trigger: message moved to DLQ after max Start attempts"
                    );
                    return channel.ack(message).await;
                }
                tracing::warn!(
                    topic = %message.topic,
                    partition = message.partition,
                    offset = message.offset,
                    attempts,
                    error = %err,
                    "workflow trigger: Start failed; retrying the same message"
                );
                tokio::time::sleep(backoff_base * attempts).await;
            }
        }
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn base64_encode(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

// ─── 生产实现：真实引擎 + 真实消费通道 ───

struct EngineStarter {
    engine: Arc<WorkflowEngineService>,
}

#[async_trait]
impl TriggerStarter for EngineStarter {
    async fn start(
        &self,
        definition_id: &str,
        input: Value,
        idempotency_key: &str,
    ) -> Result<(), String> {
        self.engine
            .start_instance_as_with_key(definition_id, input, None, Some(idempotency_key))
            .await
            .map(|_| ())
            .map_err(|err| err.to_string())
    }
}

struct LiveChannel {
    consumer: Mutex<MqConsumer>,
    mq: MqV2Client,
    topic_epoch: u64,
}

impl LiveChannel {
    async fn close(self) {
        let consumer = self.consumer.into_inner();
        if let Err(err) = consumer.close().await {
            tracing::warn!(error = %err, "workflow trigger: leave group failed");
        }
    }
}

#[async_trait]
impl TriggerChannel for LiveChannel {
    async fn ack(&self, message: &MqMessage) -> Result<(), String> {
        self.consumer
            .lock()
            .await
            .ack(message)
            .await
            .map(|_| ())
            .map_err(|err| {
                format!(
                    "ack {}:{}:{}: {err}",
                    message.topic, message.partition, message.offset
                )
            })
    }

    async fn move_to_dlq(
        &self,
        message: &MqMessage,
        reason: &str,
        detail: &str,
    ) -> Result<(), String> {
        self.mq
            .move_to_dlq(&MqMoveToDlq {
                topic: message.topic.clone(),
                partition: message.partition,
                topic_epoch: self.topic_epoch,
                offset: message.offset,
                reason: reason.to_string(),
                detail: detail.to_string(),
            })
            .await
            .map(|_| ())
            .map_err(|err| {
                format!(
                    "move_to_dlq {}:{}:{}: {err}",
                    message.topic, message.partition, message.offset
                )
            })
    }
}

/// 单个定义的消费任务：解析 topic → 加入组 → Poll → 处理 → 离组（失败退避后重建）。
async fn run_trigger(
    inner: Arc<AgentInner>,
    engine: Arc<WorkflowEngineService>,
    definition_id: String,
    trigger: TriggerConfig,
    mut stop: watch::Receiver<bool>,
) {
    let Some(topic) = trigger.topic.clone() else {
        return;
    };
    let group = trigger.group_name(&definition_id);
    let mq = inner.client.mq_v2();
    let starter = EngineStarter {
        engine: Arc::clone(&engine),
    };
    let mut backoff = Duration::from_millis(500);
    tracing::info!(
        definition = %definition_id,
        topic = %topic,
        group = %group,
        "workflow trigger started"
    );

    'rejoin: loop {
        if *stop.borrow() {
            tracing::info!(definition = %definition_id, "workflow trigger stopped");
            return;
        }
        // topic 必须已存在：GetTopicLeader 是唯一的前置探测（不自动建 topic）。
        let leaders = match mq.get_topic_leader(&topic).await {
            Ok(leaders) if !leaders.partitions.is_empty() => leaders,
            Ok(_) => {
                tracing::warn!(topic = %topic, "workflow trigger: topic has no partitions yet; retrying");
                if wait_or_stop(&mut stop, backoff).await {
                    return;
                }
                backoff = (backoff * 2).min(REJOIN_BACKOFF_MAX);
                continue 'rejoin;
            }
            Err(err) => {
                tracing::warn!(
                    topic = %topic,
                    error = %err,
                    "workflow trigger: topic is not available (triggers never create topics); retrying"
                );
                if wait_or_stop(&mut stop, backoff).await {
                    return;
                }
                backoff = (backoff * 2).min(REJOIN_BACKOFF_MAX);
                continue 'rejoin;
            }
        };
        let topic_id = leaders.topic_id;
        let topic_epoch = leaders.topic_epoch;
        let consumer = match MqConsumer::join(
            mq.clone(),
            MqConsumerOptions {
                group: group.clone(),
                member_id: String::new(),
                topics: vec![topic.clone()],
                session_timeout_ms: 0,
                // 保留期删段：从最早可用继续并 WARN（不静默停摆；与事件 provider 一致）。
                reset_policy: ResetPolicy::Earliest,
                max_messages: 0,
                max_bytes: 0,
                wait_ms: POLL_WAIT_MS,
            },
        )
        .await
        {
            Ok(consumer) => consumer,
            Err(err) => {
                tracing::warn!(
                    group = %group,
                    error = %err,
                    "workflow trigger: join group failed; retrying"
                );
                if wait_or_stop(&mut stop, backoff).await {
                    return;
                }
                backoff = (backoff * 2).min(REJOIN_BACKOFF_MAX);
                continue 'rejoin;
            }
        };
        tracing::info!(
            group = %group,
            member = %consumer.member_id(),
            generation = consumer.generation(),
            assignments = ?consumer.assignments(),
            "workflow trigger joined group"
        );
        backoff = Duration::from_millis(500);
        let channel = LiveChannel {
            consumer: Mutex::new(consumer),
            mq: mq.clone(),
            topic_epoch,
        };

        loop {
            if *stop.borrow() {
                channel.close().await;
                tracing::info!(definition = %definition_id, "workflow trigger stopped");
                return;
            }
            let batch = { channel.consumer.lock().await.poll().await };
            match batch {
                Ok(messages) => {
                    for message in &messages {
                        if *stop.borrow() {
                            channel.close().await;
                            tracing::info!(definition = %definition_id, "workflow trigger stopped");
                            return;
                        }
                        if let Err(err) = handle_message(
                            &starter,
                            &channel,
                            &definition_id,
                            &trigger,
                            message,
                            topic_id,
                            RETRY_BACKOFF,
                        )
                        .await
                        {
                            // 通道故障：位点未推进 ⇒ 重建消费者重投（Start 幂等吸收）。
                            tracing::warn!(
                                group = %group,
                                error = %err,
                                "workflow trigger: channel failure; rebuilding the consumer"
                            );
                            channel.close().await;
                            continue 'rejoin;
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        group = %group,
                        error = %err,
                        "workflow trigger: poll failed; rebuilding the consumer"
                    );
                    channel.close().await;
                    continue 'rejoin;
                }
            }
        }
    }
}

/// 等退避；`true` = 收到停用信号（调用方应退出）。
async fn wait_or_stop(stop: &mut watch::Receiver<bool>, delay: Duration) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(delay) => *stop.borrow(),
        changed = stop.changed() => changed.is_err() || *stop.borrow(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn message(offset: u64, headers: &[(&str, &str)], payload: &[u8]) -> MqMessage {
        MqMessage {
            topic: "orders".to_string(),
            partition: 0,
            offset,
            timestamp_ms: 1_700_000_000_000,
            key: b"k-1".to_vec(),
            payload: payload.to_vec(),
            headers: headers
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect::<BTreeMap<_, _>>(),
            producer: "tests".to_string(),
        }
    }

    #[derive(Default)]
    struct StubStarter {
        attempts: AtomicUsize,
        keys: parking_lot::Mutex<Vec<String>>,
        fail_until: usize,
    }

    #[async_trait]
    impl TriggerStarter for StubStarter {
        async fn start(&self, _def: &str, _input: Value, key: &str) -> Result<(), String> {
            self.keys.lock().push(key.to_string());
            let n = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
            if n <= self.fail_until {
                return Err("start unavailable".to_string());
            }
            Ok(())
        }
    }

    #[derive(Default)]
    struct StubChannel {
        acks: AtomicUsize,
        dlq: parking_lot::Mutex<Vec<(String, String)>>,
        dlq_fails: bool,
    }

    #[async_trait]
    impl TriggerChannel for StubChannel {
        async fn ack(&self, _message: &MqMessage) -> Result<(), String> {
            self.acks.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn move_to_dlq(
            &self,
            _message: &MqMessage,
            reason: &str,
            detail: &str,
        ) -> Result<(), String> {
            if self.dlq_fails {
                return Err("dlq unavailable".to_string());
            }
            self.dlq
                .lock()
                .push((reason.to_string(), detail.to_string()));
            Ok(())
        }
    }

    fn trigger(max_attempts: Option<u32>) -> TriggerConfig {
        TriggerConfig {
            topic: Some("orders".to_string()),
            filter: None,
            max_attempts,
        }
    }

    #[test]
    fn envelope_carries_the_message_identity_and_headers() {
        let json = envelope(&message(42, &[("ce-type", "order.created")], b"{\"id\":1}"));
        assert_eq!(json["topic"], "orders");
        assert_eq!(json["partition"], 0);
        assert_eq!(json["offset"], 42);
        assert_eq!(json["key"], "6b2d31");
        assert_eq!(json["headers"]["ce-type"], "order.created");
        assert_eq!(json["payload"]["id"], 1);
        assert_eq!(json["payload_encoding"], "json");

        let text = envelope(&message(43, &[], b"plain"));
        assert_eq!(text["payload"], "plain");
        assert_eq!(text["payload_encoding"], "utf8");

        let binary = envelope(&message(44, &[], &[0xff, 0x00]));
        assert_eq!(binary["payload_encoding"], "base64");
        assert_eq!(binary["payload"], base64_encode(&[0xff, 0x00]));
    }

    #[test]
    fn replayed_offset_maps_to_the_same_idempotency_key() {
        assert_eq!(idempotency_key(7, 0, 42), "mq:7:0:42");
        assert_eq!(idempotency_key(7, 0, 42), idempotency_key(7, 0, 42));
        assert_ne!(idempotency_key(7, 0, 42), idempotency_key(7, 0, 43));
        assert_ne!(idempotency_key(7, 0, 42), idempotency_key(8, 0, 42));
    }

    #[tokio::test]
    async fn non_matching_messages_are_acked_without_starting() {
        let starter = StubStarter::default();
        let channel = StubChannel::default();
        let filtered = TriggerConfig {
            topic: Some("orders".to_string()),
            filter: Some(coord_core::workflow::model::EventFilter {
                event_type: Some("order.created".to_string()),
                event_types: Vec::new(),
                source: None,
                subject: None,
            }),
            max_attempts: None,
        };
        handle_message(
            &starter,
            &channel,
            "def-1",
            &filtered,
            &message(1, &[("ce-type", "order.cancelled")], b"{}"),
            7,
            Duration::from_millis(1),
        )
        .await
        .expect("ack non-matching");
        assert_eq!(channel.acks.load(Ordering::SeqCst), 1);
        assert_eq!(starter.attempts.load(Ordering::SeqCst), 0);
        assert!(channel.dlq.lock().is_empty());
    }

    #[tokio::test]
    async fn a_transient_start_failure_is_retried_and_acked() {
        let starter = StubStarter {
            fail_until: 2,
            ..Default::default()
        };
        let channel = StubChannel::default();
        handle_message(
            &starter,
            &channel,
            "def-1",
            &trigger(None),
            &message(5, &[], b"{}"),
            7,
            Duration::from_millis(1),
        )
        .await
        .expect("retry then ack");
        assert_eq!(starter.attempts.load(Ordering::SeqCst), 3);
        assert_eq!(channel.acks.load(Ordering::SeqCst), 1);
        assert!(channel.dlq.lock().is_empty(), "3 attempts < default max 3");
        // 每次重试都用同一个幂等键（崩溃重投不重复建实例）。
        let keys = starter.keys.lock().clone();
        assert_eq!(keys.len(), 3);
        assert!(keys.iter().all(|key| key == "mq:7:0:5"));
    }

    #[tokio::test]
    async fn a_poison_message_moves_to_dlq_with_the_stable_reason_and_is_acked() {
        let starter = StubStarter {
            fail_until: usize::MAX,
            ..Default::default()
        };
        let channel = StubChannel::default();
        handle_message(
            &starter,
            &channel,
            "def-1",
            &trigger(Some(2)),
            &message(9, &[], b"{}"),
            7,
            Duration::from_millis(1),
        )
        .await
        .expect("dlq then ack");
        assert_eq!(starter.attempts.load(Ordering::SeqCst), 2);
        let dlq = channel.dlq.lock().clone();
        assert_eq!(dlq.len(), 1);
        assert_eq!(dlq[0].0, DLQ_REASON_MAX_ATTEMPTS);
        assert!(
            dlq[0].1.contains("2 consecutive Start failures"),
            "{:?}",
            dlq[0]
        );
        assert_eq!(channel.acks.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_dlq_failure_leaves_the_message_unacked_for_redelivery() {
        let starter = StubStarter {
            fail_until: usize::MAX,
            ..Default::default()
        };
        let channel = StubChannel {
            dlq_fails: true,
            ..Default::default()
        };
        let err = handle_message(
            &starter,
            &channel,
            "def-1",
            &trigger(Some(1)),
            &message(11, &[], b"{}"),
            7,
            Duration::from_millis(1),
        )
        .await
        .expect_err("dlq failure must surface");
        assert!(err.contains("dlq unavailable"), "{err}");
        assert_eq!(
            channel.acks.load(Ordering::SeqCst),
            0,
            "must not ack without DLQ"
        );
    }

    #[test]
    fn only_definitions_that_declare_a_topic_get_a_consumer() {
        let mut def = coord_core::workflow::model::WorkflowDefinition {
            id: Some("def-1".to_string()),
            document: coord_core::workflow::model::Document {
                dsl: "1.0.0".to_string(),
                namespace: "test".to_string(),
                name: "wf".to_string(),
                version: "1.0".to_string(),
                title: None,
                summary: None,
                tags: None,
            },
            do_tasks: Vec::new(),
            input: None,
            output: None,
            timeout: None,
            use_components: None,
            schedule: Default::default(),
            trigger: Default::default(),
            auth: Default::default(),
            secrets: Default::default(),
            constants: Default::default(),
            task_meta: Default::default(),
            raw_yaml: None,
        };
        assert!(desired_trigger(&def).is_none());
        def.trigger = trigger(None);
        assert_eq!(
            desired_trigger(&def).and_then(|t| t.topic),
            Some("orders".to_string())
        );
        def.id = None;
        assert!(
            desired_trigger(&def).is_none(),
            "unpersisted definitions have no id"
        );
    }
}
