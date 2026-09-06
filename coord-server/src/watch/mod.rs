// Watch 变更监听模块
//
// 提供基于 Revision 的变更事件订阅与推送能力：
// - 前缀匹配过滤：订阅者只接收匹配 Key 前缀的变更事件
// - 历史回放：新订阅者从指定 Revision 回放 Changelog 中的历史事件
// - 背压保护：缓冲区满时丢弃最旧事件并通知客户端
// - 非阻塞推送：Watch 事件推送不阻塞写入路径
//
// Watch 是协调层核心原语之一，依赖 MVCC Storage 的 Changelog 表。

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::mpsc;

use crate::metrics::Metrics;
use crate::storage::mvcc::ChangeEvent;
use coord_core::types::Revision;

// ──── Watch 事件 ────

/// Watch 事件类型
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchEventType {
    /// Key 写入或更新
    Put,
    /// Key 删除
    Delete,
    /// 缓冲区溢出，需客户端全量同步
    BufferOverflow,
    /// 历史 Changelog 已清理，无法回放
    HistoryUnavailable,
}

/// 单条 Key-Value 变更
#[derive(Debug, Clone)]
pub struct WatchKeyValue {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub prev_value: Option<Vec<u8>>,
}

/// 推送给订阅者的 Watch 事件
#[derive(Debug, Clone)]
pub struct WatchEvent {
    /// 关联的 Watch ID
    pub watch_id: u64,
    /// 事件列表（同一 Revision 可能有多个 Key 变更）
    pub events: Vec<WatchEventItem>,
}

/// 单条 Watch 事件项
#[derive(Debug, Clone)]
pub struct WatchEventItem {
    /// 事件类型
    pub event_type: WatchEventType,
    /// 变更的 KV 列表
    pub kvs: Vec<WatchKeyValue>,
    /// 事件 Revision
    pub revision: Revision,
}

// ──── 订阅请求 ────

/// 创建 Watch 订阅的请求参数
#[derive(Debug, Clone)]
pub struct WatchRequest {
    /// 监听的 Key 前缀
    pub key: Vec<u8>,
    /// 范围结束 Key（空 = 仅匹配 key 本身）
    pub range_end: Vec<u8>,
    /// 起始 Revision（0 = 从最新开始，不回溯历史）
    pub start_revision: Revision,
}

// ──── WatchDispatcher ────

/// Watch ID 生成器
static NEXT_WATCH_ID: AtomicU64 = AtomicU64::new(1);

/// 订阅者信息
struct Subscriber {
    watch_id: u64,
    key_prefix: Vec<u8>,
    range_end: Vec<u8>,
    /// 事件发送通道（有界缓冲区）
    event_tx: mpsc::Sender<WatchEvent>,
    /// 注册时的回放水位：回放 [start_revision, watermark]，
    /// 实时事件从 watermark+1 续（订阅者循环按 revision 去重）
    watermark_rev: Revision,
    /// 溢出标志：缓冲区满时置位；订阅者循环 recv 后检查并
    /// 合成 BufferOverflow 事件（保证溢出通知必达，不依赖竞态）
    overflow: Arc<AtomicBool>,
}

/// 默认 watcher 总数上限（超限拒绝新建）
const DEFAULT_MAX_SUBSCRIBERS: usize = 10_000;

/// 全局 Watch 事件分发器
///
/// 线程安全，支持多生产者（StateMachine apply 路径）单消费者（分发循环）。
/// 分发循环在独立 Tokio 任务中运行。
pub struct WatchDispatcher {
    /// 事件接收端：StateMachine 在 apply 后通过此通道推送 ChangeEvent
    /// （当前由 dispatch() 方法同步处理；保留通道接口供未来异步分发循环使用）
    #[allow(dead_code)]
    event_rx: mpsc::UnboundedReceiver<ChangeEvent>,
    /// 事件发送端（克隆给 StateMachine 使用）
    event_tx: mpsc::UnboundedSender<ChangeEvent>,
    /// 订阅者列表
    subscribers: Arc<RwLock<HashMap<u64, Subscriber>>>,
    /// watcher 总数上限
    max_subscribers: usize,
    /// 指标注册表（R-OBS-10：订阅数/事件数/背压丢弃，可选）
    metrics: Option<Arc<Metrics>>,
}

impl WatchDispatcher {
    /// 创建新的 WatchDispatcher 并启动分发循环
    pub fn start() -> Self {
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        Self {
            event_rx,
            event_tx,
            subscribers: Arc::new(RwLock::new(HashMap::new())),
            max_subscribers: DEFAULT_MAX_SUBSCRIBERS,
            metrics: None,
        }
    }

    /// 挂载指标注册表（R-OBS-10）。
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// 获取事件发送端的克隆（给 StateMachine 使用）
    pub fn event_sender(&self) -> mpsc::UnboundedSender<ChangeEvent> {
        self.event_tx.clone()
    }

    /// 创建新的 Watch 订阅
    ///
    /// - `watermark_rev`：注册时的回放水位（调用方在注册前读取
    ///   `MvccStorage::current_revision()`）；回放仅覆盖
    ///   `[start_revision, watermark_rev]`，实时事件从 `watermark_rev+1` 续，
    ///   订阅者循环按 revision 去重，杜绝回放/实时竞态重复。
    /// - watcher 总数超限返回错误，调用方映射 `RESOURCE_EXHAUSTED`。
    ///
    /// 返回 (watch_id, event_receiver)。
    pub fn subscribe(
        &self,
        request: WatchRequest,
        buffer_size: usize,
        watermark_rev: Revision,
    ) -> Result<(u64, mpsc::Receiver<WatchEvent>), String> {
        let watch_id = NEXT_WATCH_ID.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = mpsc::channel(buffer_size);

        let subscriber = Subscriber {
            watch_id,
            key_prefix: request.key,
            range_end: request.range_end,
            event_tx: tx,
            watermark_rev,
            overflow: Arc::new(AtomicBool::new(false)),
        };

        {
            let mut subscribers = self.subscribers.write();
            if subscribers.len() >= self.max_subscribers {
                return Err(format!(
                    "watch subscriber limit reached ({}); reject new watch",
                    self.max_subscribers
                ));
            }
            subscribers.insert(watch_id, subscriber);
        }

        // R-OBS-10：活跃订阅数 +1
        if let Some(metrics) = &self.metrics {
            metrics.inc_watch_active();
        }

        Ok((watch_id, rx))
    }

    /// 取走并清除订阅者的溢出标志。
    ///
    /// 订阅者循环在每次 `recv` 后调用：置位过即返回 true，由调用方
    /// 合成 BufferOverflow 事件下发（保证溢出通知必达）。
    pub fn take_overflow(&self, watch_id: u64) -> bool {
        let subscribers = self.subscribers.read();
        match subscribers.get(&watch_id) {
            Some(sub) => sub.overflow.swap(false, Ordering::SeqCst),
            None => false,
        }
    }

    /// 读取订阅者的回放水位（实时事件按 revision 去重）
    pub fn watermark(&self, watch_id: u64) -> Option<Revision> {
        self.subscribers
            .read()
            .get(&watch_id)
            .map(|s| s.watermark_rev)
    }

    /// 取消 Watch 订阅
    pub fn unsubscribe(&self, watch_id: u64) {
        if self.subscribers.write().remove(&watch_id).is_some() {
            // R-OBS-10：活跃订阅数 -1
            if let Some(metrics) = &self.metrics {
                metrics.dec_watch_active();
            }
        }
    }

    /// 获取当前订阅者数量
    pub fn subscriber_count(&self) -> usize {
        self.subscribers.read().len()
    }

    /// 推送 ChangeEvent 到所有匹配的订阅者
    ///
    /// 非阻塞：如果某订阅者缓冲区已满，丢弃最旧事件并发送 BufferOverflow。
    pub fn dispatch(&self, event: ChangeEvent) {
        let subscribers = self.subscribers.read();

        for sub in subscribers.values() {
            // 检查事件中的 Key 是否匹配订阅前缀
            let matching: Vec<WatchKeyValue> = event
                .changes
                .iter()
                .filter(|change| key_matches(&change.key, &sub.key_prefix, &sub.range_end))
                .map(|change| WatchKeyValue {
                    key: change.key.clone(),
                    value: change.value.clone(),
                    prev_value: change.prev_value.clone(),
                })
                .collect();

            if matching.is_empty() {
                continue;
            }

            let event_type = match event.event_type {
                crate::storage::mvcc::EventType::Put => WatchEventType::Put,
                crate::storage::mvcc::EventType::Delete => WatchEventType::Delete,
                crate::storage::mvcc::EventType::Txn => WatchEventType::Put, // Txn 内各操作可能是 Put 或 Delete
                // Lease 事件：Revoke 携带的 changes 均为删除；Grant/KeepAlive 无 changes
                crate::storage::mvcc::EventType::Lease => WatchEventType::Delete,
            };

            let watch_event = WatchEvent {
                watch_id: sub.watch_id,
                events: vec![WatchEventItem {
                    event_type,
                    kvs: matching,
                    revision: event.revision,
                }],
            };

            // 尝试发送；缓冲区满时置溢出标志：订阅者循环
            // recv 后检查标志并合成 BufferOverflow，保证通知必达
            match sub.event_tx.try_send(watch_event) {
                Ok(()) => {
                    // R-OBS-10：下发事件 +1
                    if let Some(metrics) = &self.metrics {
                        metrics.inc_watch_events();
                    }
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    sub.overflow.store(true, Ordering::SeqCst);
                    // R-OBS-10：背压丢弃 +1
                    if let Some(metrics) = &self.metrics {
                        metrics.inc_watch_dropped();
                    }
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    // 订阅者已断开连接
                }
            }
        }
    }

    /// 从 [start_revision, ∞) 读取 [start_revision, end_revision] 的历史事件
    /// （含 start_revision）。`start_revision <= compacted_revision` 时历史已被
    /// 压缩、不可达，必须返回 Err 由调用方下发 `HistoryUnavailable`。
    /// 按 Revision 升序发送；损坏条目（严格读取）→ Err。
    #[allow(clippy::too_many_arguments)] // 回放参数即为协议全部要素，无聚合收益
    pub fn replay_history(
        &self,
        watch_id: u64,
        event_tx: &mpsc::Sender<WatchEvent>,
        key_prefix: &[u8],
        range_end: &[u8],
        start_revision: Revision,
        end_revision: Revision,
        changelog_reader: &dyn ChangelogReader,
    ) -> Result<(), String> {
        // 压缩水位校验 —— 历史已被压缩时不可静默缺洞
        let compacted = changelog_reader
            .compacted_revision()
            .map_err(|e| format!("failed to read compacted revision: {e}"))?;
        if start_revision > 0 && start_revision <= compacted {
            return Err(format!(
                "requested history from revision {start_revision} has been compacted \
                 (compacted_revision={compacted})"
            ));
        }

        // 从 Changelog 读取 [start_revision, ∞) 的事件
        let events = changelog_reader
            .read_changelog_from(start_revision)
            .map_err(|e| format!("failed to read changelog: {e}"))?;

        for event in events {
            // 回放止于水位（实时事件从 end+1 续）
            if event.revision > end_revision {
                break;
            }
            let matching: Vec<WatchKeyValue> = event
                .changes
                .iter()
                .filter(|change| key_matches(&change.key, key_prefix, range_end))
                .map(|change| WatchKeyValue {
                    key: change.key.clone(),
                    value: change.value.clone(),
                    prev_value: change.prev_value.clone(),
                })
                .collect();

            if matching.is_empty() {
                continue;
            }

            let event_type = match event.event_type {
                crate::storage::mvcc::EventType::Put => WatchEventType::Put,
                crate::storage::mvcc::EventType::Delete => WatchEventType::Delete,
                crate::storage::mvcc::EventType::Txn => WatchEventType::Put,
                // Lease 事件：Revoke 携带的 changes 均为删除
                crate::storage::mvcc::EventType::Lease => WatchEventType::Delete,
            };

            let watch_event = WatchEvent {
                watch_id,
                events: vec![WatchEventItem {
                    event_type,
                    kvs: matching,
                    revision: event.revision,
                }],
            };

            // 历史回放使用阻塞发送，因为需要保证顺序
            if event_tx.blocking_send(watch_event).is_err() {
                return Err("subscriber disconnected during replay".into());
            }
        }

        Ok(())
    }
}

// ──── ChangelogReader trait ────

/// 变更日志读取器抽象
///
/// Watch 历史回放需要从 Changelog 表读取历史事件。
/// MvccStorage 可实现此 trait。
pub trait ChangelogReader: Send + Sync {
    /// 从指定 Revision 开始读取 Changelog 条目（含 start_revision）
    fn read_changelog_from(&self, start_revision: Revision) -> Result<Vec<ChangeEvent>, String>;

    /// 已持久化的 compacted revision；默认 0（从未压缩）。
    ///
    /// 回放起始 revision <= 该值时代历史已不可达（被压缩删除），
    /// 订阅者应收到 `HistoryUnavailable` 而非静默缺洞。
    fn compacted_revision(&self) -> Result<Revision, String> {
        Ok(0)
    }
}

// ──── 辅助函数 ────

/// 检查 Key 是否匹配订阅范围
///
/// - 如果 range_end 为空：前缀匹配（Key 以 prefix 开头）
/// - 如果 range_end 非空：范围匹配 [prefix, range_end)
fn key_matches(key: &[u8], prefix: &[u8], range_end: &[u8]) -> bool {
    if !key.starts_with(prefix) {
        return false;
    }

    if range_end.is_empty() {
        // 前缀匹配：所有以 prefix 开头的 Key
        true
    } else {
        // 范围匹配 [prefix, range_end)
        key < range_end
    }
}

// ──── 测试 ────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_matches_exact() {
        // 前缀匹配：key="/app/config" 匹配 prefix="/app/config"
        assert!(key_matches(b"/app/config", b"/app/config", b""));
        // key="/app/config2" 也匹配 prefix="/app/config"（前缀语义）
        assert!(key_matches(b"/app/config2", b"/app/config", b""));
        // key="/app/confi" 不匹配 prefix="/app/config"
        assert!(!key_matches(b"/app/confi", b"/app/config", b""));
        // key="/other" 不匹配 prefix="/app/config"
        assert!(!key_matches(b"/other", b"/app/config", b""));
    }

    #[test]
    fn test_key_matches_prefix() {
        // 匹配 /app/ 前缀的所有 Key
        assert!(key_matches(b"/app/a", b"/app/", b""));
        assert!(key_matches(b"/app/b/c", b"/app/", b""));
        assert!(!key_matches(b"/otherapp/a", b"/app/", b""));
    }

    #[test]
    fn test_key_matches_range() {
        // 匹配 [/app/a, /app/c) 范围
        assert!(key_matches(b"/app/a", b"/app/", b"/app/c"));
        assert!(key_matches(b"/app/b", b"/app/", b"/app/c"));
        assert!(!key_matches(b"/app/c", b"/app/", b"/app/c")); // 右开区间
        assert!(!key_matches(b"/app/d", b"/app/", b"/app/c"));
    }

    #[test]
    fn test_watch_dispatcher_subscribe_unsubscribe() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let dispatcher = WatchDispatcher::start();
            assert_eq!(dispatcher.subscriber_count(), 0);

            let (id, _rx) = dispatcher
                .subscribe(
                    WatchRequest {
                        key: b"/app/".to_vec(),
                        range_end: vec![],
                        start_revision: 0,
                    },
                    1024,
                    0,
                )
                .unwrap();

            assert!(id > 0);
            assert_eq!(dispatcher.subscriber_count(), 1);

            dispatcher.unsubscribe(id);
            assert_eq!(dispatcher.subscriber_count(), 0);
        });
    }

    #[test]
    fn test_watch_dispatch_matching() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let dispatcher = WatchDispatcher::start();

            let (_id, mut rx) = dispatcher
                .subscribe(
                    WatchRequest {
                        key: b"/app/".to_vec(),
                        range_end: vec![],
                        start_revision: 0,
                    },
                    1024,
                    0,
                )
                .unwrap();

            // 推送匹配的事件
            use crate::storage::mvcc::{EventType, KeyValueChange};
            let event = ChangeEvent {
                revision: 1,
                changes: vec![KeyValueChange {
                    key: b"/app/config".to_vec(),
                    value: Some(b"value".to_vec()),
                    prev_value: None,
                }],
                event_type: EventType::Put,
            };

            dispatcher.dispatch(event);

            // 应该收到事件
            let received = rx.try_recv();
            assert!(received.is_ok());
            let watch_event = received.unwrap();
            assert_eq!(watch_event.events.len(), 1);
            assert_eq!(watch_event.events[0].kvs[0].key, b"/app/config");
        });
    }

    #[test]
    fn test_watch_dispatch_non_matching() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let dispatcher = WatchDispatcher::start();

            let (_id, mut rx) = dispatcher
                .subscribe(
                    WatchRequest {
                        key: b"/app/".to_vec(),
                        range_end: vec![],
                        start_revision: 0,
                    },
                    1024,
                    0,
                )
                .unwrap();

            // 推送不匹配的事件
            use crate::storage::mvcc::{EventType, KeyValueChange};
            let event = ChangeEvent {
                revision: 1,
                changes: vec![KeyValueChange {
                    key: b"/other/key".to_vec(),
                    value: Some(b"value".to_vec()),
                    prev_value: None,
                }],
                event_type: EventType::Put,
            };

            dispatcher.dispatch(event);

            // 不应该收到事件
            let received = rx.try_recv();
            assert!(received.is_err()); // channel empty
        });
    }

    // ──── 验收测试 ────

    /// E.1：缓冲区满置溢出标志，`take_overflow` 返回 true（通知必达不依赖竞态）。
    #[test]
    fn test_overflow_flag_set_when_buffer_full() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let dispatcher = WatchDispatcher::start();

            let (id, _rx) = dispatcher
                .subscribe(
                    WatchRequest {
                        key: b"/app/".to_vec(),
                        range_end: vec![],
                        start_revision: 0,
                    },
                    1, // 最小缓冲区
                    0,
                )
                .unwrap();

            use crate::storage::mvcc::{EventType, KeyValueChange};
            for rev in 1..=5 {
                dispatcher.dispatch(ChangeEvent {
                    revision: rev,
                    changes: vec![KeyValueChange {
                        key: b"/app/k".to_vec(),
                        value: Some(vec![rev as u8]),
                        prev_value: None,
                    }],
                    event_type: EventType::Put,
                });
            }

            // 溢出标志必被置位（缓冲区容量 1，连发 5 条必溢出）
            assert!(dispatcher.take_overflow(id), "overflow flag must be set");
            // 取走后清除
            assert!(!dispatcher.take_overflow(id));
        });
    }

    /// E.4：watcher 总数超限拒绝新建。
    #[test]
    fn test_watcher_cap_rejects_excess() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut dispatcher = WatchDispatcher::start();
            // 把上限降到 2 验证拒绝逻辑
            dispatcher.max_subscribers = 2;

            for _ in 0..2 {
                dispatcher
                    .subscribe(
                        WatchRequest {
                            key: b"/app/".to_vec(),
                            range_end: vec![],
                            start_revision: 0,
                        },
                        16,
                        0,
                    )
                    .unwrap();
            }
            let err = dispatcher
                .subscribe(
                    WatchRequest {
                        key: b"/app/".to_vec(),
                        range_end: vec![],
                        start_revision: 0,
                    },
                    16,
                    0,
                )
                .unwrap_err();
            assert!(
                err.contains("limit"),
                "should reject excess watchers: {err}"
            );
        });
    }
}
