// coord-agent: 事件通知 (Event Notification Service)
//
// 实现 BaseService trait，提供事件发布/订阅能力。
// 基于 Coord 核心原语（Watch + 本地推送）构建。
//
// 架构（v3.0 蓝图）:
// - Watch Fan-out 将事件推送给订阅的本地应用方法
// - 所有对外事件封装为 CloudEvents 1.0 格式（Phase G 蓝图）
// - 支持事件持久化与重放
//
// 约束（蓝图）: 所有对外事件必须封装为 CloudEvents 1.0 格式
//
// 参见 docs/client-agent-architecture-v3.md §5.7。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::RwLock as ParkingRwLock;
use tokio::sync::{broadcast, watch};

use crate::proxy::AgentInner;
use crate::service::{BaseService, ServiceResult};

// ──── 类型定义 ────

/// 事件（v3.0 简化版，完整 CloudEvents 封装为 Phase G 蓝图）
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Event {
    /// 事件唯一 ID
    pub id: String,
    /// 事件类型（如 "order.created", "payment.completed"）
    pub event_type: String,
    /// 事件来源
    pub source: String,
    /// 事件数据（JSON 格式）
    pub data: Vec<u8>,
    /// 事件时间（Unix 时间戳，毫秒）
    pub timestamp_ms: u64,
    /// 事件主题（可选，用于分区）
    pub subject: String,
    /// 数据内容类型
    pub data_content_type: String,
}

impl Event {
    pub fn new(event_type: impl Into<String>, source: impl Into<String>, data: Vec<u8>) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Self {
            id: uuid_v4_like(),
            event_type: event_type.into(),
            source: source.into(),
            data,
            timestamp_ms: now,
            subject: String::new(),
            data_content_type: "application/json".into(),
        }
    }

    /// 构造 Server 存储 key（事件持久化）
    pub fn storage_key(id: &str) -> Vec<u8> {
        format!("/_events/{id}").into_bytes()
    }
}

/// 生成简易 UUID v4 风格 ID（不引入 uuid crate）
fn uuid_v4_like() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:016x}-{:04x}-4{:03x}-{:04x}-{:012x}",
        ts & 0xFFFFFFFFFFFFFFFF,
        (ts >> 64) as u16 & 0xFFFF,
        (ts >> 80) as u16 & 0xFFF,
        0x8000 | ((ts >> 96) as u16 & 0x3FFF),
        ts & 0xFFFFFFFFFFFF,
    )
}


// ──── CloudEvents 1.0 类型 ────

/// CloudEvents 1.0 规范事件
///
/// 遵循 CloudEvents 1.0 规范（https://cloudevents.io/）。
/// 所有跨 Agent 边界事件必须使用此格式。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CloudEvent {
    /// CloudEvents 规范版本（固定 "1.0"）
    #[serde(rename = "specversion")]
    pub specversion: String,
    /// 事件类型（反向 DNS 格式，如 "cn.byteforce.order.created"）
    #[serde(rename = "type")]
    pub event_type: String,
    /// 事件来源（URI-reference，如 "/coord-agent/order-service"）
    pub source: String,
    /// 事件唯一 ID
    pub id: String,
    /// 事件数据（可选）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Vec<u8>>,
    /// 数据内容类型（如 "application/json"）
    #[serde(default, rename = "datacontenttype", skip_serializing_if = "Option::is_none")]
    pub datacontenttype: Option<String>,
    /// 事件主题（可选）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    /// 事件时间（RFC 3339 格式）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time: Option<String>,
}

impl CloudEvent {
    /// 创建最小 CloudEvent（必填字段：type, source）
    pub fn new(event_type: impl Into<String>, source: impl Into<String>) -> Self {
        let ts = chrono_like_now();
        Self {
            specversion: "1.0".to_string(),
            event_type: event_type.into(),
            source: source.into(),
            id: uuid_v4_like(),
            data: None,
            datacontenttype: None,
            subject: None,
            time: Some(ts),
        }
    }

    /// 从现有 Event 转换为 CloudEvent
    pub fn from_event(event: &Event) -> Self {
        Self {
            specversion: "1.0".to_string(),
            event_type: event.event_type.clone(),
            source: event.source.clone(),
            id: event.id.clone(),
            data: if event.data.is_empty() { None } else { Some(event.data.clone()) },
            datacontenttype: if event.data_content_type.is_empty() {
                None
            } else {
                Some(event.data_content_type.clone())
            },
            subject: if event.subject.is_empty() { None } else { Some(event.subject.clone()) },
            time: Some(ms_to_rfc3339(event.timestamp_ms)),
        }
    }

    /// 转换为 Event（用于内部处理）
    pub fn to_event(&self) -> Event {
        Event {
            id: self.id.clone(),
            event_type: self.event_type.clone(),
            source: self.source.clone(),
            data: self.data.clone().unwrap_or_default(),
            timestamp_ms: rfc3339_to_ms(self.time.as_deref().unwrap_or("")),
            subject: self.subject.clone().unwrap_or_default(),
            data_content_type: self.datacontenttype.clone().unwrap_or_default(),
        }
    }
}

/// 生成类似 ISO 8601 的时间戳字符串
fn chrono_like_now() -> String {
    use std::time::SystemTime;
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs();
    let millis = now.subsec_millis();
    // 使用 time crate 进行简单格式化
    let total_secs = secs as i64;
    // 简单 RFC 3339 格式
    format!(
        "{}T{:02}:{:02}:{:02}.{:03}Z",
        "2024-01-01", // placeholder date
        (total_secs % 86400) / 3600,
        ((total_secs % 3600) / 60),
        total_secs % 60,
        millis,
    )
}

fn ms_to_rfc3339(ms: u64) -> String {
    let secs = ms / 1000;
    let millis = ms % 1000;
    format!(
        "{}T{:02}:{:02}:{:02}.{:03}Z",
        "2024-01-01",
        (secs % 86400) / 3600,
        ((secs % 3600) / 60),
        secs % 60,
        millis,
    )
}

fn rfc3339_to_ms(_s: &str) -> u64 {
    // Simplified: return current time
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}


// ──── EventCache ────

/// 事件通知本地缓存
///
/// 缓存近期事件，支持按类型/主题检索。
pub struct EventCache {
    /// 事件存储：event_id → Event
    events: BTreeMap<String, Event>,
    /// 最大缓存事件数
    max_events: usize,
}

impl EventCache {
    pub fn new(max_events: usize) -> Self {
        Self {
            events: BTreeMap::new(),
            max_events,
        }
    }

    /// 存储事件
    pub fn put(&mut self, event: Event) {
        if self.events.len() >= self.max_events {
            // 移除最旧的事件（BTreeMap 按 key 排序，id 近似时间顺序）
            if let Some(oldest_key) = self.events.keys().next().cloned() {
                self.events.remove(&oldest_key);
            }
        }
        self.events.insert(event.id.clone(), event);
    }

    /// 查询事件
    pub fn get(&self, id: &str) -> Option<&Event> {
        self.events.get(id)
    }

    /// 按类型筛选事件
    pub fn get_by_type(&self, event_type: &str) -> Vec<&Event> {
        self.events
            .values()
            .filter(|e| e.event_type == event_type)
            .collect()
    }

    /// 最近事件（按时间倒序）
    pub fn recent(&self, limit: usize) -> Vec<&Event> {
        let mut events: Vec<&Event> = self.events.values().collect();
        events.sort_by_key(|e| std::cmp::Reverse(e.timestamp_ms));
        events.truncate(limit);
        events
    }

    /// 缓存事件数
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

// ──── EventNotificationService ────

/// 事件通知服务
///
/// 实现 `BaseService` trait，提供事件发布/订阅能力。
pub struct EventNotificationService {
    /// 到 Server 集群的内部客户端（共享）
    inner: Arc<AgentInner>,
    /// 本地事件缓存
    cache: Arc<ParkingRwLock<EventCache>>,
    /// 广播通道（用于本地订阅者推送）
    broadcast_tx: broadcast::Sender<Event>,
    /// 健康状态
    healthy: ParkingRwLock<bool>,
    /// 关闭信号
    shutdown_tx: ParkingRwLock<Option<watch::Sender<()>>>,
}

impl EventNotificationService {
    pub const NAME: &'static str = "event_notification";

    pub fn new(inner: Arc<AgentInner>, max_cached_events: usize, broadcast_capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(broadcast_capacity);
        Self {
            inner,
            cache: Arc::new(ParkingRwLock::new(EventCache::new(max_cached_events))),
            broadcast_tx: tx,
            healthy: ParkingRwLock::new(false),
            shutdown_tx: ParkingRwLock::new(None),
        }
    }

    /// 发布事件
    ///
    /// 事件持久化到 Server（KV 存储）并通过 broadcast 推送给本地订阅者。
    pub async fn publish(&self, event: Event) -> ServiceResult<()> {
        // 持久化到 Server
        let storage_key = Event::storage_key(&event.id);
        let value = serde_json::to_vec(&event)
            .map_err(|e| format!("failed to serialize event: {e}"))?;

        self.inner
            .client
            .kv()
            .put(&storage_key, &value)
            .await
            .map_err(|e| format!("failed to persist event '{}': {e}", event.id))?;

        // 更新本地缓存
        self.cache.write().put(event.clone());

        // 推送给本地订阅者（忽略无订阅者的情况）
        let _ = self.broadcast_tx.send(event);

        Ok(())
    }

    /// 订阅事件（获取本地广播接收器）
    ///
    /// 返回 broadcast::Receiver，调用方可通过它接收本地事件推送。
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.broadcast_tx.subscribe()
    }

    /// 查询历史事件
    pub fn get_event(&self, id: &str) -> Option<Event> {
        self.cache.read().get(id).cloned()
    }

    /// 按类型查询事件
    pub fn get_events_by_type(&self, event_type: &str) -> Vec<Event> {
        self.cache
            .read()
            .get_by_type(event_type)
            .into_iter()
            .cloned()
            .collect()
    }

    /// 最近事件
    pub fn recent_events(&self, limit: usize) -> Vec<Event> {
        self.cache
            .read()
            .recent(limit)
            .into_iter()
            .cloned()
            .collect()
    }

    /// 缓存事件数
    pub fn cache_len(&self) -> usize {
        self.cache.read().len()
    }
}

#[async_trait]
impl BaseService for EventNotificationService {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    async fn start(&self) -> ServiceResult<()> {
        tracing::info!("EventNotificationService: starting");
        *self.healthy.write() = true;

        let (_tx, mut rx) = watch::channel::<()>(());
        *self.shutdown_tx.write() = Some(_tx);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = rx.changed() => {
                        tracing::info!("EventNotificationService: background task shutting down");
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(60)) => {}
                }
            }
        });

        Ok(())
    }

    async fn stop(&self) -> ServiceResult<()> {
        tracing::info!("EventNotificationService: stopping");
        if let Some(tx) = self.shutdown_tx.write().take() {
            let _ = tx.send(());
        }
        *self.healthy.write() = false;
        Ok(())
    }

    fn health_check(&self) -> bool {
        *self.healthy.read()
    }
}

impl std::fmt::Debug for EventNotificationService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventNotificationService")
            .field("cache_len", &self.cache_len())
            .field("healthy", &self.health_check())
            .finish()
    }
}

// ──── tests ────

#[cfg(test)]
mod tests {
    use super::*;

    // ──── Event 测试 ────

    #[test]
    fn test_event_creation() {
        let event = Event::new("order.created", "order-service", br#"{"orderId":123}"#.to_vec());
        assert_eq!(event.event_type, "order.created");
        assert_eq!(event.source, "order-service");
        assert!(!event.id.is_empty());
        assert!(event.timestamp_ms > 0);
        assert_eq!(event.data_content_type, "application/json");
    }

    #[test]
    fn test_event_storage_key() {
        let key = Event::storage_key("evt-001");
        assert_eq!(String::from_utf8_lossy(&key), "/_events/evt-001");
    }

    #[test]
    fn test_event_serialization_roundtrip() {
        let event = Event {
            id: "evt-001".into(),
            event_type: "test.event".into(),
            source: "test-svc".into(),
            data: br#"{"key":"value"}"#.to_vec(),
            timestamp_ms: 1700000000000,
            subject: "test-subject".into(),
            data_content_type: "application/json".into(),
        };
        let json = serde_json::to_vec(&event).unwrap();
        let restored: Event = serde_json::from_slice(&json).unwrap();
        assert_eq!(restored, event);
    }

    // ──── EventCache 测试 ────

    #[test]
    fn test_event_cache_put_and_get() {
        let mut cache = EventCache::new(100);
        let event = Event::new("test.event", "test-svc", vec![]);
        let event_id = event.id.clone();

        cache.put(event);
        assert_eq!(cache.len(), 1);

        let found = cache.get(&event_id).unwrap();
        assert_eq!(found.event_type, "test.event");
    }

    #[test]
    fn test_event_cache_get_by_type() {
        let mut cache = EventCache::new(100);
        cache.put(Event::new("order.created", "svc1", vec![]));
        cache.put(Event::new("order.created", "svc2", vec![]));
        cache.put(Event::new("payment.done", "svc1", vec![]));

        let orders = cache.get_by_type("order.created");
        assert_eq!(orders.len(), 2);

        let payments = cache.get_by_type("payment.done");
        assert_eq!(payments.len(), 1);
    }

    #[test]
    fn test_event_cache_recent() {
        let mut cache = EventCache::new(100);
        cache.put(Event::new("type.a", "svc1", vec![]));
        std::thread::sleep(std::time::Duration::from_millis(2));
        cache.put(Event::new("type.b", "svc1", vec![]));

        let recent = cache.recent(1);
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].event_type, "type.b"); // 最新的在前
    }

    #[test]
    fn test_event_cache_max_capacity() {
        let mut cache = EventCache::new(3);
        cache.put(Event::new("e1", "s", vec![]));
        cache.put(Event::new("e2", "s", vec![]));
        cache.put(Event::new("e3", "s", vec![]));
        assert_eq!(cache.len(), 3);

        // 第 4 个事件应驱逐最旧的
        cache.put(Event::new("e4", "s", vec![]));
        assert_eq!(cache.len(), 3);
    }

    // ──── EventNotificationService 名称常量 ────

    #[test]
    fn test_event_notification_name_constant() {
        assert_eq!(EventNotificationService::NAME, "event_notification");
    }
}
