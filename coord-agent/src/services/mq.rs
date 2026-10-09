// coord-agent: 消息队列 (MQ Service) — 数据面（支持 ISR 复制）
//
// 实现 BaseService trait，基于 redb 提供本地分段日志消息队列。
// 支持 Topic/Partition/ConsumerGroup/DeadLetterQueue。
//
// 架构（v3.0 + ISR）:
// - Agent 本地持久化日志（redb 分段日志）；**默认单 agent 语义**
// - Topic 配置 / 分区 / 消费组偏移 / DLQ 均为 per-agent 本地存储
// - 消费模型：poll（按 offset 增量拉取）+ ack（提交消费组偏移）→ at-least-once
// - subscribe 为基于消费组 offset 的长轮询推送
//
// ISR 复制（可选，默认关闭）：
// - `produce_replicated`：分区 Leader 独占分配 offset，单事务（NEXT_OFFSET_TABLE
//   + 消息 + 复制日志 + 幂等键 + 本地序列号）→ 同步推送到 ISR Followers → min_isr 校验
// - Follower 幂等应用 + 自动建 topic；subscribe / ack 仅 Leader
// - `services.replication=false` = 纯单 agent 本地语义；启用后为分布式 / 高可用形态
//
// ── 容量上界（B-PL-4）──
// `max_size_bytes`（0 = 不限）在 publish 入口**强制**：消息与 DLQ 的物理字节计入
// `mq:meta`（与数据同一次写事务 ⇒ 逐写严格上界；超界与单条超限均拒绝
// `RESOURCE_EXHAUSTED`）。服务内 reaper（默认 10s）按 topic 的 `retention_secs`
// （0 = 不按时间回收）清扫过期消息与 DLQ 条目。周期回收语义与残余边界（记账范围 /
// ISR 本地行为 / delete_topic 存量行）见 `docs/production/ops/boundaries.md`
// B-PL-4（单一归属）。

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use parking_lot::RwLock;
use redb::{ReadableDatabase, ReadableTable};
use tokio::sync::mpsc;

use crate::service::{BaseService, ServiceResult};

// ──── 公共类型 ────

/// Topic 配置
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TopicConfig {
    pub partitions: u32,
    pub retention_secs: u64,
    pub max_message_size: u64,
}

/// Topic 信息（含运行时统计）
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TopicInfo {
    pub name: String,
    pub config: TopicConfig,
    pub created_at: u64,
}

/// 消息记录
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageRecord {
    pub offset: u64,
    pub payload: Vec<u8>,
    pub timestamp: u64,
    pub headers: BTreeMap<String, String>,
}

/// DLQ 消息记录
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DlqRecord {
    pub offset: u64,
    pub payload: Vec<u8>,
    pub timestamp: u64,
    pub error_reason: Option<String>,
    pub error_detail: Option<String>,
}

/// MQ 统计信息
#[derive(Debug, Clone, Default)]
pub struct MqStats {
    pub topic_count: u64,
    pub total_messages: u64,
    pub dlq_messages: u64,
    /// 记账口径的活跃字节（消息 + DLQ 物理字节；见模块头「容量上界」，
    /// `MessageQueueService::accounted_bytes` 为同一真值）
    pub total_bytes: u64,
}

/// 单轮 reaper 统计（`MessageQueueService::reap_once` 返回）
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MqReapStats {
    /// 本轮回收到期的消息数
    pub expired_messages: u64,
    /// 本轮回收到期的 DLQ 条目数
    pub expired_dlq: u64,
    /// 本轮回收的字节数（记账口径）
    pub purged_bytes: u64,
    /// 本轮结束时的活跃字节
    pub active_bytes: u64,
    /// 当前上界（0 = 不限）
    pub limit_bytes: u64,
}

/// reaper 累计统计（单调；由 reaper 后台任务/显式调用更新）
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MqReapCounters {
    /// 累计执行轮数
    pub passes: u64,
    /// 累计回收到期的消息数
    pub expired_messages: u64,
    /// 累计回收到期的 DLQ 条目数
    pub expired_dlq: u64,
    /// 累计回收字节数（记账口径）
    pub purged_bytes: u64,
    /// 累计失败轮数（单调；>0 表示至少有一轮 reap 失败）
    pub faults: u64,
}

/// topic 删除的逐项回收计数（`delete_topic_full` 返回；G-MQ-4）
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DeleteTopicStats {
    /// 删除的消息条目数
    pub messages_removed: u64,
    /// 删除的 DLQ 条目数
    pub dlq_removed: u64,
    /// 删除的消费位点条目数
    pub offsets_removed: u64,
    /// 删除的幂等索引条目数
    pub idempotency_removed: u64,
    /// 回收的记账字节（消息 + DLQ 物理字节；B-PL-4 同口径）
    pub bytes_reclaimed: u64,
}

// ──── redb 表定义 ────

const TOPIC_TABLE: redb::TableDefinition<&str, &[u8]> = redb::TableDefinition::new("mq:topics");
// Messages: key = [topic_len:u32][topic_bytes][partition:u32][offset:u64 BE]
const MESSAGE_TABLE: redb::TableDefinition<&[u8], &[u8]> =
    redb::TableDefinition::new("mq:messages");
// Consumer offsets: key = [group_len:u32][group_bytes][topic_len:u32][topic_bytes][partition:u32]
const OFFSET_TABLE: redb::TableDefinition<&[u8], u64> = redb::TableDefinition::new("mq:offsets");
// DLQ: key = [topic_len:u32][topic_bytes][partition:u32][offset:u64 BE]
const DLQ_TABLE: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("mq:dlq");
// Next offset counter: key = [topic_len:u32][topic_bytes][partition:u32]
const NEXT_OFFSET_TABLE: redb::TableDefinition<&[u8], u64> =
    redb::TableDefinition::new("mq:next_offset");

// ──── 生产幂等表 ────
// 幂等键 → 已分配 offset：key = [topic_len:u32][topic_bytes][partition:u32][ikey_len:u32][ikey_bytes]
// value = JSON `{"offset":u64,"ts_ms":u64}`。
// 存在意义：调用方在"响应丢失后重试"时不得为下游多出一条消息。
// 条目按 topic 的 `retention_secs` 窗口保留，由 `produce_idempotent` 低频机会式清扫。
const IDEMPOTENCY_TABLE: redb::TableDefinition<&[u8], &[u8]> =
    redb::TableDefinition::new("mq:idempotency");

/// 幂等条目清扫频率（每 N 次带幂等键的生产触发一次机会式清扫）
const IDEM_PRUNE_EVERY: u64 = 256;

// ──── 复制日志表（ISR，v2.1）────
// 复制条目日志: key = [shard_len:u32][shard_bytes][seq:u64 BE]
const REPL_ENTRY_TABLE: redb::TableDefinition<&[u8], &[u8]> =
    redb::TableDefinition::new("mq:repl_entries");
// 持久化幂等键: key = idempotency_key bytes
const REPL_APPLIED_KEYS: redb::TableDefinition<&[u8], ()> =
    redb::TableDefinition::new("mq:repl_applied");
// 各 shard 最后已应用序列号: key = shard bytes
const REPL_LOCAL_SEQ: redb::TableDefinition<&[u8], u64> =
    redb::TableDefinition::new("mq:repl_local_seq");

// ──── 容量记账表（B-PL-4）────
//
// 记账口径：`active_bytes` = `mq:messages` + `mq:dlq` 内**物理存储行**大小之和，
// 单条大小 = 物理 key 长度 + 存储值长度。不含消费位点（每 group×topic×partition
// 一行，非消息级增长）、幂等索引（已有窗口清扫）、ISR 复制日志（复制设计保留）
// 与 redb 页面开销 —— 因此 redb 文件体积**大于**记账值。
const MQ_META_TABLE: redb::TableDefinition<&str, u64> = redb::TableDefinition::new("mq:meta");

/// meta 表键：已记账活跃字节
const META_ACTIVE_BYTES: &str = "active_bytes";

/// reaper 过期清扫每批最多删除的条目数（控制单事务规模）
const REAP_PURGE_CHUNK: usize = 512;
/// 后台 reaper 默认周期（毫秒）
const DEFAULT_REAPER_INTERVAL_MS: u64 = 10_000;

// ──── Key 编码辅助 ────

fn encode_msg_key(topic: &str, partition: u32, offset: u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(4 + topic.len() + 4 + 8);
    v.extend_from_slice(&(topic.len() as u32).to_be_bytes());
    v.extend_from_slice(topic.as_bytes());
    v.extend_from_slice(&partition.to_be_bytes());
    v.extend_from_slice(&offset.to_be_bytes());
    v
}

fn msg_key_prefix(topic: &str, partition: u32) -> Vec<u8> {
    let mut v = Vec::with_capacity(4 + topic.len() + 4);
    v.extend_from_slice(&(topic.len() as u32).to_be_bytes());
    v.extend_from_slice(topic.as_bytes());
    v.extend_from_slice(&partition.to_be_bytes());
    v
}

fn encode_offset_key(group: &str, topic: &str, partition: u32) -> Vec<u8> {
    let mut v = Vec::with_capacity(4 + group.len() + 4 + topic.len() + 4);
    v.extend_from_slice(&(group.len() as u32).to_be_bytes());
    v.extend_from_slice(group.as_bytes());
    v.extend_from_slice(&(topic.len() as u32).to_be_bytes());
    v.extend_from_slice(topic.as_bytes());
    v.extend_from_slice(&partition.to_be_bytes());
    v
}

fn encode_dlq_key(topic: &str, partition: u32, offset: u64) -> Vec<u8> {
    // Same as msg_key but in DLQ table
    encode_msg_key(topic, partition, offset)
}

fn encode_next_offset_key(topic: &str, partition: u32) -> Vec<u8> {
    let mut v = Vec::with_capacity(4 + topic.len() + 4);
    v.extend_from_slice(&(topic.len() as u32).to_be_bytes());
    v.extend_from_slice(topic.as_bytes());
    v.extend_from_slice(&partition.to_be_bytes());
    v
}

/// 幂等索引条目
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct IdempotencyEntry {
    /// 首次为该幂等键分配的 offset
    offset: u64,
    /// 写入墙钟（毫秒，清扫用）
    ts_ms: u64,
}

/// topic 下全部幂等键的前缀（用于清扫时按 topic 过滤）
fn idempotency_prefix(topic: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(4 + topic.len() + 4);
    v.extend_from_slice(&(topic.len() as u32).to_be_bytes());
    v.extend_from_slice(topic.as_bytes());
    v.extend_from_slice(&u32::MAX.to_be_bytes()); // partition 占位：前缀只到 topic
    v
}

/// 幂等索引键：[topic_len:u32][topic][partition:u32][ikey_len:u32][ikey]
fn encode_idempotency_key(topic: &str, partition: u32, ikey: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(4 + topic.len() + 4 + 4 + ikey.len());
    v.extend_from_slice(&(topic.len() as u32).to_be_bytes());
    v.extend_from_slice(topic.as_bytes());
    v.extend_from_slice(&partition.to_be_bytes());
    v.extend_from_slice(&(ikey.len() as u32).to_be_bytes());
    v.extend_from_slice(ikey.as_bytes());
    v
}

/// 复制日志 key: [shard_len:u32][shard][seq:u64 BE]
fn encode_repl_key(shard: &str, seq: u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(4 + shard.len() + 8);
    v.extend_from_slice(&(shard.len() as u32).to_be_bytes());
    v.extend_from_slice(shard.as_bytes());
    v.extend_from_slice(&seq.to_be_bytes());
    v
}

/// 复制日志前缀: [shard_len:u32][shard]
fn encode_repl_prefix(shard: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(4 + shard.len());
    v.extend_from_slice(&(shard.len() as u32).to_be_bytes());
    v.extend_from_slice(shard.as_bytes());
    v
}

/// 从复制日志 key 解码序列号
fn decode_repl_seq(encoded: &[u8], prefix_len: usize) -> Option<u64> {
    if encoded.len() < prefix_len + 8 {
        return None;
    }
    Some(u64::from_be_bytes(
        encoded[prefix_len..prefix_len + 8].try_into().ok()?,
    ))
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Encode message payload with metadata: [timestamp:u64 BE][headers_len:u32][headers_json][payload]
fn encode_message(payload: &[u8], headers: &BTreeMap<String, String>) -> Vec<u8> {
    let headers_json = serde_json::to_vec(headers).unwrap_or_default();
    let mut v = Vec::with_capacity(8 + 4 + headers_json.len() + payload.len());
    v.extend_from_slice(&now_millis().to_be_bytes());
    v.extend_from_slice(&(headers_json.len() as u32).to_be_bytes());
    v.extend_from_slice(&headers_json);
    v.extend_from_slice(payload);
    v
}

/// Decode message: returns (payload, timestamp, headers)
fn decode_message(raw: &[u8]) -> Option<(Vec<u8>, u64, BTreeMap<String, String>)> {
    if raw.len() < 12 {
        return None;
    }
    let timestamp = u64::from_be_bytes(raw[..8].try_into().ok()?);
    let headers_len = u32::from_be_bytes(raw[8..12].try_into().ok()?) as usize;
    if raw.len() < 12 + headers_len {
        return None;
    }
    let headers: BTreeMap<String, String> =
        serde_json::from_slice(&raw[12..12 + headers_len]).unwrap_or_default();
    let payload = raw[12 + headers_len..].to_vec();
    Some((payload, timestamp, headers))
}

/// Encode DLQ message: [timestamp:u64][reason_len:u32][reason][detail_len:u32][detail][payload]
fn encode_dlq_message(payload: &[u8], reason: &str, detail: &str) -> Vec<u8> {
    let reason_bytes = reason.as_bytes();
    let detail_bytes = detail.as_bytes();
    let mut v =
        Vec::with_capacity(8 + 4 + reason_bytes.len() + 4 + detail_bytes.len() + payload.len());
    v.extend_from_slice(&now_millis().to_be_bytes());
    v.extend_from_slice(&(reason_bytes.len() as u32).to_be_bytes());
    v.extend_from_slice(reason_bytes);
    v.extend_from_slice(&(detail_bytes.len() as u32).to_be_bytes());
    v.extend_from_slice(detail_bytes);
    v.extend_from_slice(payload);
    v
}

/// Decode DLQ message
fn decode_dlq_message(raw: &[u8]) -> Option<(Vec<u8>, u64, String, String)> {
    if raw.len() < 16 {
        return None;
    }
    let timestamp = u64::from_be_bytes(raw[..8].try_into().ok()?);
    let reason_len = u32::from_be_bytes(raw[8..12].try_into().ok()?) as usize;
    if raw.len() < 12 + reason_len + 4 {
        return None;
    }
    let reason = String::from_utf8_lossy(&raw[12..12 + reason_len]).to_string();
    let detail_len_start = 12 + reason_len;
    let detail_len = u32::from_be_bytes(
        raw[detail_len_start..detail_len_start + 4]
            .try_into()
            .ok()?,
    ) as usize;
    if raw.len() < detail_len_start + 4 + detail_len {
        return None;
    }
    let detail =
        String::from_utf8_lossy(&raw[detail_len_start + 4..detail_len_start + 4 + detail_len])
            .to_string();
    let payload = raw[detail_len_start + 4 + detail_len..].to_vec();
    Some((payload, timestamp, reason, detail))
}

// ──── MessageQueueService ────

/// 消息队列服务（数据面）
///
/// 基于 redb 的本地持久化分段日志 MQ。
///
/// 订阅（subscribe）实现：produce 提交后向订阅者 channel 直接推送
/// （按消费组偏移过滤），subscribe 时回放已提交偏移之后的消息。
/// 订阅者条目：（consumer_group, 消息 channel）
type SubscriberEntry = (String, mpsc::Sender<(u32, MessageRecord)>);

pub struct MessageQueueService {
    db_path: PathBuf,
    db: RwLock<Option<redb::Database>>,
    started: RwLock<bool>,
    /// 容量上界（字节；0 = 不限）。**已在 publish 入口强制**（消息 + DLQ 与数据
    /// 同事务记账 ⇒ 逐写严格上界；超界与单条超限均拒绝），并按 topic 的
    /// `retention_secs` 由 reaper 周期回收 —— 契约与残余边界见
    /// `docs/production/ops/boundaries.md` B-PL-4。
    max_size_bytes: u64,
    /// 后台 reaper 周期（毫秒；装配/测试旋钮）
    reaper_interval_ms: AtomicU64,
    /// 后台 reaper 是否已挂载（幂等；与 cache 同口径）
    reaper_spawned: AtomicBool,
    /// reaper 累计执行轮数
    reap_passes: AtomicU64,
    /// reaper 累计回收消息数
    reap_expired_messages: AtomicU64,
    /// reaper 累计回收 DLQ 条目数
    reap_expired_dlq: AtomicU64,
    /// reaper 累计回收字节数（记账口径）
    reap_purged_bytes: AtomicU64,
    /// reaper 累计失败轮数
    reap_faults: AtomicU64,
    /// publish 因配额被拒绝的累计次数（单调；背压可观测）
    publish_rejections: AtomicU64,
    /// 订阅者注册表：topic → (consumer_group, 消息 channel [(partition, record)])
    subscriptions: RwLock<HashMap<String, Vec<SubscriberEntry>>>,
    /// ISR 复制管理器（None = 单 agent 本地语义，零复制路径保留）
    replication: RwLock<Option<Arc<crate::services::replication::ReplicationManager>>>,
    /// 自身 Arc 弱引用（spawn_blocking 升级用，见 bind_self_weak）
    self_arc: RwLock<Option<std::sync::Weak<MessageQueueService>>>,
    /// 幂等条目机会式清扫计数器（每 `IDEM_PRUNE_EVERY` 次触发一次，
    /// 避免每次生产都 O(n) 扫全表）
    idem_prune_tick: AtomicU64,
}

impl std::fmt::Debug for MessageQueueService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageQueueService")
            .field("db_path", &self.db_path)
            .field("started", &self.started)
            // 如实报出"配了多少、有没有生效"（0 = 不限 ⇒ 未设置强制上界）
            .field("max_size_bytes", &self.max_size_bytes)
            .field("max_size_enforced", &(self.max_size_bytes > 0))
            .finish()
    }
}

impl MessageQueueService {
    /// 创建 MQ 服务。
    ///
    /// # 容量上界（B-PL-4）
    ///
    /// `max_size_bytes`（0 = 不限）**在 publish 入口强制**：消息与 DLQ 物理字节
    /// 与数据在**同一次写事务**记账（redb 单写者串行化 ⇒ 逐写严格上界，超界即
    /// 拒绝 `RESOURCE_EXHAUSTED`，不是周期收敛）。后台 reaper（默认 10s）按
    /// topic 的 `retention_secs`（0 = 不按时间回收）清扫过期消息与 DLQ 条目、
    /// 释放配额。语义边界（记账范围 / ISR 本地行为 / delete_topic 存量行）与
    /// `docs/production/ops/boundaries.md` B-PL-4 单一归属。
    pub fn new(db_path: PathBuf, max_size_bytes: u64) -> Self {
        if max_size_bytes == 0 {
            tracing::warn!(
                "MessageQueueService: no size limit configured (max_size_bytes=0); growth \
                 is bounded only by consumption and retention reaping"
            );
        }
        Self {
            db_path,
            db: RwLock::new(None),
            started: RwLock::new(false),
            max_size_bytes,
            reaper_interval_ms: AtomicU64::new(DEFAULT_REAPER_INTERVAL_MS),
            reaper_spawned: AtomicBool::new(false),
            reap_passes: AtomicU64::new(0),
            reap_expired_messages: AtomicU64::new(0),
            reap_expired_dlq: AtomicU64::new(0),
            reap_purged_bytes: AtomicU64::new(0),
            reap_faults: AtomicU64::new(0),
            publish_rejections: AtomicU64::new(0),
            subscriptions: RwLock::new(HashMap::new()),
            replication: RwLock::new(None),
            self_arc: RwLock::new(None),
            idem_prune_tick: AtomicU64::new(0),
        }
    }

    /// 绑定自身 Arc 弱引用（gRPC handler 升级为强引用后，
    /// 把同步 redb 事务放到 `spawn_blocking`，避免阻塞 agent 异步执行器）。
    ///
    /// 由服务装配方在 `Arc::new` 后调用一次（lib.rs run_agent 数据面初始化）。
    pub fn bind_self_weak(&self, me: &Arc<Self>) {
        *self.self_arc.write() = Some(Arc::downgrade(me));
    }

    /// 升级自身强引用（未绑定或已释放返回 None）
    pub fn self_arc(&self) -> Option<Arc<Self>> {
        self.self_arc.read().as_ref().and_then(|w| w.upgrade())
    }

    /// 在阻塞线程池上执行同步 redb 操作。
    /// 服务装配时必须先 `bind_self_weak`。
    pub async fn run_blocking<F, R>(&self, f: F) -> ServiceResult<R>
    where
        F: FnOnce(Arc<Self>) -> ServiceResult<R> + Send + 'static,
        R: Send + 'static,
    {
        let me = self
            .self_arc()
            .ok_or_else(|| "MQService self_arc not bound".to_string())?;
        tokio::task::spawn_blocking(move || f(me))
            .await
            .map_err(|e| format!("mq blocking task join: {e}"))?
    }

    /// 挂载 ISR 复制管理器（None = 关闭复制，单 agent 零破坏）
    pub fn set_replication(
        &self,
        manager: Option<Arc<crate::services::replication::ReplicationManager>>,
    ) {
        *self.replication.write() = manager;
    }

    /// 复制是否启用
    pub fn replication_enabled(&self) -> bool {
        self.replication.read().is_some()
    }

    /// 复制管理器引用（None = 复制关闭）
    pub fn replication_manager(
        &self,
    ) -> Option<Arc<crate::services::replication::ReplicationManager>> {
        self.replication.read().clone()
    }

    fn read_tx(&self) -> ServiceResult<redb::ReadTransaction> {
        let guard = self.db.read();
        let db = guard.as_ref().ok_or("MQ Service not started")?;
        Ok(db.begin_read()?)
    }

    fn write_tx(&self) -> ServiceResult<redb::WriteTransaction> {
        let guard = self.db.read();
        let db = guard.as_ref().ok_or("MQ Service not started")?;
        Ok(db.begin_write()?)
    }

    // ──── 容量上界：记账 / 配额 / reaper（B-PL-4）────
    //
    // 契约与残余边界与 `docs/production/ops/boundaries.md` B-PL-4 单一归属：
    // - 记账 = `mq:messages` + `mq:dlq` 物理行大小之和（key 长度 + 存储值长度），
    //   与数据在**同一次写事务**提交（redb 单写者串行化 ⇒ 无读-改-写竞态）；
    // - 强制点只有 publish 入口（单 agent 与 ISR Leader 本地提交前）：`active +
    //   entry > max` 即拒绝；Follower apply 刻意不检查 —— 必须镜像 Leader 已提交
    //   的决定，否则副本分叉（配额为节点本地 ingress 行为）；
    // - 回收 = 按 topic `retention_secs` 周期清扫过期行（消息 + DLQ）；
    // - `move_to_dlq` 为维护路径，不受配额拒绝（净增仅 reason/detail 开销）。

    /// 服务是否已启动（后台 reaper 据此决定是否执行本轮）
    pub fn is_started(&self) -> bool {
        *self.started.read()
    }

    /// 配置的容量上界（字节；0 = 不限）
    pub fn max_size_bytes(&self) -> u64 {
        self.max_size_bytes
    }

    /// 已记账的活跃字节（直接读 redb meta；未启动返回 Err）
    pub fn accounted_bytes(&self) -> ServiceResult<u64> {
        let rtx = self.read_tx()?;
        let meta = rtx.open_table(MQ_META_TABLE)?;
        let x = meta.get(META_ACTIVE_BYTES)?.map(|v| v.value()).unwrap_or(0);
        Ok(x)
    }

    /// publish 因配额被拒绝的累计次数（单调；背压可观测）
    pub fn publish_rejections(&self) -> u64 {
        self.publish_rejections.load(Ordering::Relaxed)
    }

    /// reaper 累计统计快照（原子读；单调计数器）
    pub fn reap_counters(&self) -> MqReapCounters {
        MqReapCounters {
            passes: self.reap_passes.load(Ordering::Relaxed),
            expired_messages: self.reap_expired_messages.load(Ordering::Relaxed),
            expired_dlq: self.reap_expired_dlq.load(Ordering::Relaxed),
            purged_bytes: self.reap_purged_bytes.load(Ordering::Relaxed),
            faults: self.reap_faults.load(Ordering::Relaxed),
        }
    }

    /// 调整后台 reaper 周期（装配/测试旋钮；默认 10s）
    pub fn set_reaper_interval(&self, interval: Duration) {
        self.reaper_interval_ms
            .store(interval.as_millis().max(1) as u64, Ordering::Relaxed);
    }

    /// 当前后台 reaper 周期
    pub fn reaper_interval(&self) -> Duration {
        Duration::from_millis(self.reaper_interval_ms.load(Ordering::Relaxed))
    }

    fn record_reap_fault(&self) {
        self.reap_faults.fetch_add(1, Ordering::Relaxed);
    }

    /// 单条记账大小：物理 key 长度 + 存储值长度
    fn entry_size(physical_key_len: usize, stored_value_len: usize) -> u64 {
        (physical_key_len + stored_value_len) as u64
    }

    /// 配额检查（在写事务内）：`active + entry ≤ max` 才放行。
    /// 单条自身超界与总量超界走同一拒绝路径（这种条目永远装不下，写成功再
    /// 回收等于假成功）；拒绝计入 `publish_rejections`。
    ///
    /// **负控制**：移除 `produce_idempotent` 中的本调用 ⇒
    /// `test_produce_rejected_over_limit` 必红。
    fn ensure_quota_tx(&self, wtx: &redb::WriteTransaction, entry_size: u64) -> ServiceResult<()> {
        if self.max_size_bytes == 0 {
            return Ok(());
        }
        let cur = {
            let meta = wtx.open_table(MQ_META_TABLE)?;
            let x = meta.get(META_ACTIVE_BYTES)?.map(|v| v.value()).unwrap_or(0);
            x
        };
        let total = cur
            .checked_add(entry_size)
            .ok_or_else(|| format!("mq accounting overflow (cur={cur} entry={entry_size})"))?;
        if total > self.max_size_bytes {
            self.publish_rejections.fetch_add(1, Ordering::Relaxed);
            return Err(format!(
                "mq publish rejected: message needs {entry_size} bytes, active {cur} (sum \
                 {total}) exceeds max_size_bytes={} — quota is enforced at publish \
                 (no silent drop); consume/ack or the retention reaper frees space \
                 (see boundaries.md B-PL-4)",
                self.max_size_bytes
            )
            .into());
        }
        Ok(())
    }

    /// 同事务增加记账，返回更新后的 active_bytes。
    fn account_add_tx(wtx: &redb::WriteTransaction, added: u64) -> ServiceResult<u64> {
        let mut meta = wtx.open_table(MQ_META_TABLE)?;
        let cur = meta.get(META_ACTIVE_BYTES)?.map(|v| v.value()).unwrap_or(0);
        let new_cur = cur
            .checked_add(added)
            .ok_or_else(|| format!("mq accounting overflow (cur={cur} added={added})"))?;
        meta.insert(META_ACTIVE_BYTES, new_cur)?;
        Ok(new_cur)
    }

    /// 同事务扣减记账，返回更新后的 active_bytes。
    fn account_sub_tx(wtx: &redb::WriteTransaction, removed: u64) -> ServiceResult<u64> {
        let mut meta = wtx.open_table(MQ_META_TABLE)?;
        let cur = meta.get(META_ACTIVE_BYTES)?.map(|v| v.value()).unwrap_or(0);
        let new_cur = cur.checked_sub(removed).ok_or_else(|| {
            format!("mq accounting underflow on remove (cur={cur} removed={removed})")
        })?;
        meta.insert(META_ACTIVE_BYTES, new_cur)?;
        Ok(new_cur)
    }

    /// 启动时初始化记账：已存在 ⇒ 直接采用；缺失（旧库升级）⇒ 一次性全量重建。
    fn ensure_accounting_initialized(&self) -> ServiceResult<u64> {
        let existing = {
            let rtx = self.read_tx()?;
            match rtx.open_table(MQ_META_TABLE) {
                Ok(meta) => {
                    let x = meta.get(META_ACTIVE_BYTES)?.map(|v| v.value());
                    x
                }
                Err(redb::TableError::TableDoesNotExist(_)) => None,
                Err(e) => return Err(e.into()),
            }
        };
        if let Some(v) = existing {
            return Ok(v);
        }
        // 重建：扫描消息 + DLQ 全表（一次性升级成本，单事务）。
        let wtx = self.write_tx()?;
        let mut accounted = 0u64;
        {
            macro_rules! sum_table {
                ($def:expr) => {{
                    let table = wtx.open_table($def)?;
                    for item in table.iter()? {
                        let (k, v) = item?;
                        accounted += Self::entry_size(k.value().len(), v.value().len());
                    }
                }};
            }
            sum_table!(MESSAGE_TABLE);
            sum_table!(DLQ_TABLE);
        }
        {
            let mut meta = wtx.open_table(MQ_META_TABLE)?;
            meta.insert(META_ACTIVE_BYTES, accounted)?;
        }
        wtx.commit()?;
        tracing::info!(
            accounted_bytes = accounted,
            "MessageQueueService: rebuilt capacity accounting for existing database \
             (one-time upgrade; see boundaries.md B-PL-4)"
        );
        Ok(accounted)
    }

    /// 执行一轮回收：按 topic 的 `retention_secs` 清扫过期消息与 DLQ 条目。
    ///
    /// 由后台任务按周期调用；测试可直接调用以获得确定性（无 sleep）。
    /// `retention_secs = 0` 的 topic 不做时间回收（沿用 cache 的「0 = 不限」口径）。
    ///
    /// **负控制**：移除对 `MESSAGE_TABLE` 的 `purge_expired_in_table` 调用 ⇒
    /// `test_reaper_purges_expired_messages` 必红。
    pub fn reap_once(&self) -> ServiceResult<MqReapStats> {
        if !self.is_started() {
            return Err("MessageQueueService not started".into());
        }
        let now = now_millis();
        let mut stats = MqReapStats {
            limit_bytes: self.max_size_bytes,
            ..MqReapStats::default()
        };
        for t in self.list_topics()? {
            let retention = t.config.retention_secs;
            if retention == 0 {
                continue;
            }
            let cutoff = now.saturating_sub(retention.saturating_mul(1000));
            let (n, bytes) =
                self.purge_expired_in_table(MESSAGE_TABLE, &t.name, t.config.partitions, cutoff)?;
            stats.expired_messages += n;
            stats.purged_bytes += bytes;
            let (n, bytes) =
                self.purge_expired_in_table(DLQ_TABLE, &t.name, t.config.partitions, cutoff)?;
            stats.expired_dlq += n;
            stats.purged_bytes += bytes;
        }
        stats.active_bytes = self.accounted_bytes()?;

        self.reap_passes.fetch_add(1, Ordering::Relaxed);
        self.reap_expired_messages
            .fetch_add(stats.expired_messages, Ordering::Relaxed);
        self.reap_expired_dlq
            .fetch_add(stats.expired_dlq, Ordering::Relaxed);
        self.reap_purged_bytes
            .fetch_add(stats.purged_bytes, Ordering::Relaxed);
        Ok(stats)
    }

    /// 清扫单表内某 topic 的过期行（消息表 / DLQ 表共用：key 布局相同，
    /// 时间戳都在存储值前 8 字节）。按分区从最旧 offset 开始，遇到未过期行即停
    /// （时间戳由写入/入队时刻决定，分区内单写者 ⇒ 正常单调；时钟回拨的极端
    /// 情形下，被非过期行挡住的过期行会等该行过期后的后续轮次再回收），
    /// 每批至多 `REAP_PURGE_CHUNK` 条、单事务删除并扣账。返回 (条数, 字节)。
    fn purge_expired_in_table(
        &self,
        table_def: redb::TableDefinition<&[u8], &[u8]>,
        topic: &str,
        partitions: u32,
        cutoff_ms: u64,
    ) -> ServiceResult<(u64, u64)> {
        let mut total_entries = 0u64;
        let mut total_bytes = 0u64;
        for partition in 0..partitions {
            let prefix = msg_key_prefix(topic, partition);
            loop {
                let wtx = self.write_tx()?;
                let mut batch: Vec<(Vec<u8>, u64)> = Vec::new();
                {
                    let table = wtx.open_table(table_def)?;
                    for item in table.range(prefix.as_slice()..)? {
                        let (k, v) = item?;
                        let k = k.value();
                        if !k.starts_with(&prefix) {
                            break;
                        }
                        let raw = v.value();
                        if raw.len() < 8 {
                            continue; // 损坏行：跳过（不猜测，不删除）
                        }
                        let ts = u64::from_be_bytes(raw[..8].try_into().unwrap_or_default());
                        if ts >= cutoff_ms {
                            break; // 未过期：其后的行更新（正常单调）
                        }
                        batch.push((k.to_vec(), Self::entry_size(k.len(), raw.len())));
                        if batch.len() >= REAP_PURGE_CHUNK {
                            break;
                        }
                    }
                }
                if batch.is_empty() {
                    // 无过期行：释放写事务（redb 只允许一个写事务，不 commit 会一直占着）
                    drop(wtx);
                    break;
                }
                let n = batch.len();
                let mut bytes = 0u64;
                {
                    let mut table = wtx.open_table(table_def)?;
                    for (k, size) in &batch {
                        table.remove(k.as_slice())?;
                        bytes += size;
                    }
                }
                Self::account_sub_tx(&wtx, bytes)?;
                wtx.commit()?;
                total_entries += n as u64;
                total_bytes += bytes;
                if n < REAP_PURGE_CHUNK {
                    break;
                }
                // 满批：继续本分区（已删除的行不会被再扫到，严格前进）
            }
        }
        Ok((total_entries, total_bytes))
    }

    /// 挂载后台 reaper（每周期一轮 `reap_once`；幂等 —— 仅在首次 start 时 spawn）。
    ///
    /// 依赖装配时 `bind_self_weak`（与 `run_blocking` 同一前置条件）；未绑定或
    /// 无 tokio runtime 时不挂载（单测直接调用 `reap_once` 获得确定性）。
    fn spawn_reaper(&self) {
        if self.reaper_spawned.swap(true, Ordering::SeqCst) {
            return;
        }
        let Some(me) = self.self_arc() else {
            tracing::debug!(
                "MessageQueueService: self_arc not bound; background reaper not spawned"
            );
            return;
        };
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::warn!("MessageQueueService: no tokio runtime; background reaper not spawned");
            return;
        };
        handle.spawn(async move {
            loop {
                tokio::time::sleep(me.reaper_interval()).await;
                if !me.is_started() {
                    continue;
                }
                let worker = me.clone();
                match tokio::task::spawn_blocking(move || worker.reap_once()).await {
                    Ok(Ok(_stats)) => {}
                    Ok(Err(e)) => {
                        me.record_reap_fault();
                        tracing::warn!(
                            error = %e,
                            "mq reaper pass failed (metric: coord_agent_mq_reaper_faults_total)"
                        );
                    }
                    Err(join) => {
                        me.record_reap_fault();
                        tracing::error!(error = %join, "mq reaper blocking task failed");
                    }
                }
            }
        });
    }

    // ──── Topic 管理 ────

    pub fn create_topic(&self, name: &str, config: TopicConfig) -> ServiceResult<()> {
        let wtx = self.write_tx()?;
        {
            let table = wtx.open_table(TOPIC_TABLE)?;
            if table.get(name)?.is_some() {
                return Err(format!("topic '{name}' already exists").into());
            }
        }
        {
            let json = serde_json::to_vec(&config)?;
            let mut table = wtx.open_table(TOPIC_TABLE)?;
            table.insert(name, json.as_slice())?;
        }
        wtx.commit()?;
        Ok(())
    }

    pub fn delete_topic(&self, name: &str) -> ServiceResult<()> {
        let wtx = self.write_tx()?;
        let existed = {
            let mut table = wtx.open_table(TOPIC_TABLE)?;
            let x = table.remove(name)?.is_some();
            x
        };
        wtx.commit()?;
        if !existed {
            return Err(format!("topic '{name}' not found").into());
        }
        Ok(())
    }

    /// 删除 topic 并**回收全部存量**（G-MQ-4）：消息 / DLQ / 消费位点 /
    /// 幂等索引 / next-offset 计数 / 配置行，配额随之归还。
    ///
    /// 单事务执行（调用方应先停该 topic 的读写——前置条件与并发语义见
    /// mq.proto 头注「删除语义」）；ISR 启用时由 Leader 调用并把删除决定经
    /// 复制通道下发（`delete_topic_replicated`）。
    ///
    /// topic 不存在 → 错误（`NOT_FOUND` 语义）。同名重建 = 空 topic。
    pub fn delete_topic_full(&self, name: &str) -> ServiceResult<DeleteTopicStats> {
        let wtx = self.write_tx()?;
        let existed = {
            let table = wtx.open_table(TOPIC_TABLE)?;
            let x = table.get(name)?.is_some();
            x
        };
        if !existed {
            return Err(format!("topic '{name}' not found").into());
        }
        let stats = Self::purge_topic_tx(&wtx, name)?;
        {
            let mut table = wtx.open_table(TOPIC_TABLE)?;
            table.remove(name)?;
        }
        wtx.commit()?;
        Ok(stats)
    }

    /// 在给定写事务内清扫 topic 的全部存量（幂等：无存量时各计数为 0）。
    ///
    /// 范围：消息 / DLQ / next-offset（topic 前缀）+ 消费位点（内嵌 topic 段，
    /// 全表扫描匹配）+ 幂等索引（topic 前缀）；同步做记账净额调整。
    /// 复制日志/序列号**不**在此清扫：落后的 Follower 依赖复制日志重放删除决定。
    fn purge_topic_tx(wtx: &redb::WriteTransaction, topic: &str) -> ServiceResult<DeleteTopicStats> {
        let mut stats = DeleteTopicStats::default();
        let mut account_removed: u64 = 0;

        // [len:u32][topic] —— 消息 / DLQ / next-offset / 幂等索引四表的公共前缀
        let mut topic_prefix = Vec::with_capacity(4 + topic.len());
        topic_prefix.extend_from_slice(&(topic.len() as u32).to_be_bytes());
        topic_prefix.extend_from_slice(topic.as_bytes());

        // 按前缀删三个同构表（消息 / DLQ / next-offset）
        macro_rules! purge_prefixed {
            ($table:expr, $counter:ident, $count_bytes:expr) => {{
                let mut victims: Vec<(Vec<u8>, u64)> = Vec::new();
                {
                    let table = wtx.open_table($table)?;
                    let range: std::ops::RangeFrom<&[u8]> = topic_prefix.as_slice()..;
                    for item in table.range(range)? {
                        let (k, v) = item?;
                        let kb = k.value();
                        if !kb.starts_with(&topic_prefix) {
                            break;
                        }
                        let bytes = Self::entry_size(kb.len(), v.value().len());
                        victims.push((kb.to_vec(), bytes));
                    }
                }
                let mut table = wtx.open_table($table)?;
                for (k, bytes) in &victims {
                    table.remove(k.as_slice())?;
                    if $count_bytes {
                        account_removed += bytes;
                    }
                }
                stats.$counter = victims.len() as u64;
            }};
        }

        purge_prefixed!(MESSAGE_TABLE, messages_removed, true);
        purge_prefixed!(DLQ_TABLE, dlq_removed, true);
        // next-offset 计数行（不单列计数；清除即可 —— 同名重建从 0 起）
        {
            let mut victims: Vec<Vec<u8>> = Vec::new();
            {
                let table = wtx.open_table(NEXT_OFFSET_TABLE)?;
                let range: std::ops::RangeFrom<&[u8]> = topic_prefix.as_slice()..;
                for item in table.range(range)? {
                    let (k, _v) = item?;
                    if !k.value().starts_with(&topic_prefix) {
                        break;
                    }
                    victims.push(k.value().to_vec());
                }
            }
            let mut table = wtx.open_table(NEXT_OFFSET_TABLE)?;
            for k in &victims {
                table.remove(k.as_slice())?;
            }
        }

        // 幂等索引（[len][topic][partition][ikey]），不计入记账（模块头口径）
        {
            let mut victims: Vec<Vec<u8>> = Vec::new();
            {
                let table = wtx.open_table(IDEMPOTENCY_TABLE)?;
                let range: std::ops::RangeFrom<&[u8]> = topic_prefix.as_slice()..;
                for item in table.range(range)? {
                    let (k, _v) = item?;
                    if !k.value().starts_with(&topic_prefix) {
                        break;
                    }
                    victims.push(k.value().to_vec());
                }
            }
            let mut table = wtx.open_table(IDEMPOTENCY_TABLE)?;
            for k in &victims {
                table.remove(k.as_slice())?;
            }
            stats.idempotency_removed = victims.len() as u64;
        }

        // 消费位点：[group_len][group][topic_len][topic][partition] —— topic 段在
        // 键中间，按前缀无法命中，全表扫描匹配（位点条目数 = 组×topic×分区，量级小）
        {
            let mut victims: Vec<Vec<u8>> = Vec::new();
            {
                let table = wtx.open_table(OFFSET_TABLE)?;
                for item in table.iter()? {
                    let (k, _v) = item?;
                    let kb = k.value();
                    if Self::offset_key_matches_topic(kb, topic) {
                        victims.push(kb.to_vec());
                    }
                }
            }
            let mut table = wtx.open_table(OFFSET_TABLE)?;
            for k in &victims {
                table.remove(k.as_slice())?;
            }
            stats.offsets_removed = victims.len() as u64;
        }

        // 记账净额（消息 + DLQ 物理字节）
        if account_removed > 0 {
            Self::account_sub_tx(wtx, account_removed)?;
            stats.bytes_reclaimed = account_removed;
        }
        Ok(stats)
    }

    /// 消费位点键（[group_len][group][topic_len][topic][partition]）的 topic 段匹配
    fn offset_key_matches_topic(key: &[u8], topic: &str) -> bool {
        if key.len() < 8 {
            return false;
        }
        let glen = u32::from_be_bytes(key[..4].try_into().unwrap_or([0; 4])) as usize;
        let topic_len_pos = 4 + glen;
        if key.len() < topic_len_pos + 4 {
            return false;
        }
        let tlen = u32::from_be_bytes(
            key[topic_len_pos..topic_len_pos + 4]
                .try_into()
                .unwrap_or([0; 4]),
        ) as usize;
        if key.len() < topic_len_pos + 4 + tlen {
            return false;
        }
        &key[topic_len_pos + 4..topic_len_pos + 4 + tlen] == topic.as_bytes()
    }

    pub fn topic_exists(&self, name: &str) -> ServiceResult<bool> {
        let rtx = self.read_tx()?;
        let table = rtx.open_table(TOPIC_TABLE)?;
        Ok(table.get(name)?.is_some())
    }

    pub fn get_topic_config(&self, name: &str) -> ServiceResult<Option<TopicConfig>> {
        let rtx = self.read_tx()?;
        let table = rtx.open_table(TOPIC_TABLE)?;
        match table.get(name)? {
            Some(v) => Ok(Some(serde_json::from_slice(v.value())?)),
            None => Ok(None),
        }
    }

    pub fn list_topics(&self) -> ServiceResult<Vec<TopicInfo>> {
        let rtx = self.read_tx()?;
        let table = rtx.open_table(TOPIC_TABLE)?;
        let mut topics = Vec::new();
        for item in table.iter()? {
            let (name, raw) = item?;
            let config: TopicConfig = serde_json::from_slice(raw.value())?;
            topics.push(TopicInfo {
                name: name.value().to_string(),
                config,
                created_at: 0, // not tracked yet
            });
        }
        Ok(topics)
    }

    // ──── 消息生产 ────

    /// 幂等索引查询（在给定写事务内；命中返回首次分配的 offset）
    ///
    /// 抽成独立函数而非内联：redb 的 `AccessGuard` 借用表、表借用事务，
    /// 内联时 `?` 的解糖临时量会活到语句末，触发 ``table` does not live long enough``。
    /// 幂等索引查询：命中返回首次分配的 offset
    ///
    /// 形态刻意对齐已知可编译的 `get_consumer_offset`：`table` 与 `match`
    /// 同处**函数体**（无内层块），scrutinee 临时量在语句末即析构。
    /// 注意：调用方须**先持有写事务**再查（redb 同一时刻只允许一个写事务
    /// ⇒ 查与写在写锁内构成原子的 read-modify-write）。
    fn lookup_idempotency(&self, ik: &[u8]) -> ServiceResult<Option<u64>> {
        let rtx = self.read_tx()?;
        // 表尚未创建（该库上从未带幂等键生产过）⇒ 等价于"无条目"。
        // redb 的读事务不能像写事务那样隐式建表，必须显式处理。
        let table = match rtx.open_table(IDEMPOTENCY_TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let bytes: Option<Vec<u8>> = table.get(ik)?.map(|v| v.value().to_vec());
        match bytes {
            Some(b) => Ok(Some(serde_json::from_slice::<IdempotencyEntry>(&b)?.offset)),
            None => Ok(None),
        }
    }

    /// 生产一条消息（无幂等键）。
    ///
    /// ⚠️ 若调用方可能重试，请用 [`Self::produce_idempotent`] —— 否则"响应丢失后
    /// 重试"会给下游多一条消息。
    pub fn produce(
        &self,
        topic: &str,
        partition: u32,
        payload: Vec<u8>,
        headers: Option<BTreeMap<String, String>>,
    ) -> ServiceResult<u64> {
        self.produce_idempotent(topic, partition, payload, headers, None)
    }

    /// 生产一条消息，可按 `idempotency_key` 去重。
    ///
    /// 语义：同一 `(topic, partition, idempotency_key)` 重复生产**不产生第二个
    /// offset**，而是返回首次分配的 offset。去重索引与消息写入在**同一 redb
    /// 写事务**中提交，因此不存在"消息已写但索引未写"的窗口。
    ///
    /// `idempotency_key = None`（或空串）⇒ 无去重（等价于旧 [`Self::produce`]）。
    pub fn produce_idempotent(
        &self,
        topic: &str,
        partition: u32,
        payload: Vec<u8>,
        headers: Option<BTreeMap<String, String>>,
        idempotency_key: Option<&str>,
    ) -> ServiceResult<u64> {
        let config = self
            .get_topic_config(topic)?
            .ok_or_else(|| format!("topic '{topic}' not found"))?;

        if partition >= config.partitions {
            return Err(format!(
                "partition {partition} out of range for topic '{topic}' (max {})",
                config.partitions
            )
            .into());
        }

        if payload.len() as u64 > config.max_message_size {
            return Err(format!(
                "message size {} exceeds max {}",
                payload.len(),
                config.max_message_size
            )
            .into());
        }

        let headers = headers.unwrap_or_default();
        let encoded = encode_message(&payload, &headers);

        let next_key = encode_next_offset_key(topic, partition);
        let msg_key_prefix = msg_key_prefix(topic, partition);
        let idem_key = idempotency_key
            .filter(|k| !k.is_empty())
            .map(|k| encode_idempotency_key(topic, partition, k));

        let now = now_millis();
        let wtx = self.write_tx()?;

        // 去重命中：直接返回首次分配的 offset，不追加消息、不推进计数器
        if let Some(ik) = idem_key.as_ref() {
            if let Some(offset) = self.lookup_idempotency(ik.as_slice())? {
                return Ok(offset);
            }
        }

        // 容量配额（B-PL-4，同事务 ⇒ 逐写严格上界）；拒绝不推进 offset
        let entry_size = Self::entry_size(msg_key_prefix.len() + 8, encoded.len());
        self.ensure_quota_tx(&wtx, entry_size)?;

        // Get and increment next offset
        let offset = {
            let current = {
                let table = wtx.open_table(NEXT_OFFSET_TABLE)?;
                let x = match table.get(next_key.as_slice())? {
                    Some(v) => v.value(),
                    None => 0u64,
                };
                x
            };
            // drop previous table reference before opening again
            let mut table = wtx.open_table(NEXT_OFFSET_TABLE)?;
            table.insert(next_key.as_slice(), current + 1)?;
            current
        };

        // Write message
        let mk = encode_msg_key(topic, partition, offset);
        {
            let mut table = wtx.open_table(MESSAGE_TABLE)?;
            table.insert(mk.as_slice(), encoded.as_slice())?;
        }

        // 幂等索引（与消息同事务 ⇒ 无"消息已写索引未写"窗口）
        if let Some(ik) = idem_key.as_ref() {
            let entry = IdempotencyEntry { offset, ts_ms: now };
            let bytes = serde_json::to_vec(&entry)?;
            let mut table = wtx.open_table(IDEMPOTENCY_TABLE)?;
            table.insert(ik.as_slice(), bytes.as_slice())?;
        }

        // 记账（与消息同事务提交；幂等索引不计入 —— 见模块头「容量上界」）
        Self::account_add_tx(&wtx, entry_size)?;

        wtx.commit()?;

        let _ = (msg_key_prefix, next_key); // silence unused warnings

        // 机会式清扫（低频）：防幂等索引无界增长
        if idem_key.is_some() {
            self.maybe_prune_idempotency(topic, config.retention_secs, now);
        }

        // 推送通知订阅者（流式 subscribe：基于消费组偏移过滤）
        self.notify_subscribers(topic, partition, offset, payload.clone(), now);

        Ok(offset)
    }

    /// 机会式清扫：每 `IDEM_PRUNE_EVERY` 次触发一次，删除超过保留窗口的幂等条目。
    ///
    /// 保留窗口取 topic 的 `retention_secs`（与消息保留一致 —— 消息已过期后，
    /// 对它的去重已无意义）。清扫失败**不阻断生产**（best-effort），仅记日志。
    fn maybe_prune_idempotency(&self, topic: &str, retention_secs: u64, now_ms: u64) {
        let tick = self.idem_prune_tick.fetch_add(1, Ordering::Relaxed);
        if !tick.is_multiple_of(IDEM_PRUNE_EVERY) {
            return;
        }
        let Ok(wtx) = self.write_tx() else {
            return;
        };
        let cutoff = now_ms.saturating_sub(retention_secs.saturating_mul(1000));
        let result = (|| -> ServiceResult<usize> {
            let prefix = idempotency_prefix(topic);
            let mut stale: Vec<Vec<u8>> = Vec::new();
            {
                let table = wtx.open_table(IDEMPOTENCY_TABLE)?;
                for item in table.iter()? {
                    let (k, v) = item?;
                    let kb = k.value();
                    if !kb.starts_with(&prefix) {
                        continue;
                    }
                    let Ok(entry) = serde_json::from_slice::<IdempotencyEntry>(v.value()) else {
                        continue; // 解析失败不动（可能是更新版本的记录）
                    };
                    if entry.ts_ms < cutoff {
                        stale.push(kb.to_vec());
                    }
                }
            }
            if stale.is_empty() {
                return Ok(0);
            }
            let mut table = wtx.open_table(IDEMPOTENCY_TABLE)?;
            for k in &stale {
                table.remove(k.as_slice())?;
            }
            Ok(stale.len())
        })();

        match result {
            Ok(n) if n > 0 => {
                if let Err(e) = wtx.commit() {
                    tracing::warn!("mq: idempotency prune commit failed: {e}");
                    return;
                }
                tracing::debug!(topic, removed = n, "mq: pruned stale idempotency entries");
            }
            Ok(_) => {
                // 无过期待删：释放写事务（redb 只允许一个写事务，不 commit 会一直占着）
                drop(wtx);
            }
            Err(e) => {
                drop(wtx);
                tracing::warn!("mq: idempotency prune failed: {e}");
            }
        }
    }

    /// 消费消息：从指定 offset 开始读取最多 max_count 条
    pub fn consume(
        &self,
        topic: &str,
        partition: u32,
        start_offset: u64,
        max_count: u64,
    ) -> ServiceResult<Vec<MessageRecord>> {
        let prefix = msg_key_prefix(topic, partition);
        let prefix_len = prefix.len();

        let rtx = self.read_tx()?;
        let mut records = Vec::new();
        {
            let table = rtx.open_table(MESSAGE_TABLE)?;
            let range: std::ops::RangeFrom<&[u8]> = prefix.as_slice()..;
            for item in table.range(range)? {
                let (k, raw) = item?;
                let k = k.value();
                if !k.starts_with(&prefix) {
                    break;
                }
                // Extract offset from key (last 8 bytes)
                if k.len() < prefix_len + 8 {
                    continue;
                }
                let Ok(off_bytes) = k[prefix_len..prefix_len + 8].try_into() else {
                    continue;
                };
                let offset = u64::from_be_bytes(off_bytes);
                if offset < start_offset {
                    continue;
                }
                if records.len() as u64 >= max_count {
                    break;
                }

                if let Some((payload, timestamp, headers)) = decode_message(raw.value()) {
                    records.push(MessageRecord {
                        offset,
                        payload,
                        timestamp,
                        headers,
                    });
                }
            }
        }
        Ok(records)
    }

    // ──── 流式订阅 ────

    /// 注册订阅者并回放已提交偏移之后的消息。
    ///
    /// 语义：基于消费组 offset 的推送。订阅时以 (group, topic, partition)
    /// 当前提交偏移为起点，回放其后全部消息并提交偏移；此后 produce 推送
    /// 新消息（按消费组偏移过滤）并自动提交偏移。
    ///
    /// 说明：subscribe 与 poll+ack 共享同一消费组偏移；推送路径在 channel
    /// 打满（背压）时丢弃消息（try_send），**可靠消费请使用 poll + ack**。
    pub async fn subscribe(
        &self,
        topic: &str,
        group: &str,
        tx: mpsc::Sender<(u32, MessageRecord)>,
    ) -> ServiceResult<()> {
        let partitions = match self.get_topic_config(topic)? {
            Some(c) => c.partitions,
            None => return Err(format!("topic '{topic}' not found").into()),
        };

        // 回放：每个分区从提交偏移起，推送全部现存消息并提交偏移
        for p in 0..partitions {
            let committed = self.get_consumer_offset(group, topic, p)?;
            let msgs = self.consume(topic, p, committed, u64::MAX)?;
            let mut last = committed;
            for m in msgs {
                let offset = m.offset;
                if tx.send((p, m)).await.is_err() {
                    return Err("subscriber channel closed".into());
                }
                last = offset + 1;
            }
            if last != committed {
                self.commit_offset(group, topic, p, last)?;
            }
        }

        // 注册订阅者
        self.subscriptions
            .write()
            .entry(topic.to_string())
            .or_default()
            .push((group.to_string(), tx));
        Ok(())
    }

    /// produce 提交后向该 topic 的订阅者推送（按消费组偏移过滤 + 自动提交）。
    fn notify_subscribers(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        payload: Vec<u8>,
        timestamp: u64,
    ) {
        let subs: Vec<(String, mpsc::Sender<(u32, MessageRecord)>)> = {
            let guard = self.subscriptions.read();
            match guard.get(topic) {
                Some(v) => v.iter().map(|(g, tx)| (g.clone(), tx.clone())).collect(),
                None => return,
            }
        };
        if subs.is_empty() {
            return;
        }

        for (group, tx) in subs {
            let committed = match self.get_consumer_offset(&group, topic, partition) {
                Ok(c) => c,
                Err(_) => continue,
            };
            if offset < committed {
                continue; // 已被消费/回放过
            }
            let record = MessageRecord {
                offset,
                payload: payload.clone(),
                timestamp,
                headers: BTreeMap::new(),
            };
            if tx.try_send((partition, record)).is_ok() {
                // 自动提交偏移（推送即确认；客户端断连前已推送的消息可能重复）
                let _ = self.commit_offset(&group, topic, partition, offset + 1);
            } else {
                // 订阅者 channel 打满或已关闭 → 丢弃并清理
                self.subscriptions
                    .write()
                    .entry(topic.to_string())
                    .or_default()
                    .retain(|(g, _)| g != &group);
            }
        }
    }

    // ──── Consumer Group 偏移管理 ────

    pub fn commit_offset(
        &self,
        group: &str,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> ServiceResult<()> {
        let key = encode_offset_key(group, topic, partition);
        let wtx = self.write_tx()?;
        {
            let mut table = wtx.open_table(OFFSET_TABLE)?;
            table.insert(key.as_slice(), offset)?;
        }
        wtx.commit()?;
        Ok(())
    }

    pub fn get_consumer_offset(
        &self,
        group: &str,
        topic: &str,
        partition: u32,
    ) -> ServiceResult<u64> {
        let key = encode_offset_key(group, topic, partition);
        let rtx = self.read_tx()?;
        let table = rtx.open_table(OFFSET_TABLE)?;
        match table.get(key.as_slice())? {
            Some(v) => Ok(v.value()),
            None => Ok(0),
        }
    }

    // ──── 死信队列 (DLQ) ────

    pub fn move_to_dlq(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        reason: &str,
        detail: &str,
    ) -> ServiceResult<()> {
        let wtx = self.write_tx()?;
        let moved = Self::move_to_dlq_tx(&wtx, topic, partition, offset, reason, detail)?;
        if !moved {
            return Err(format!("message {topic}/{partition}/{offset} not found").into());
        }
        wtx.commit()?;
        Ok(())
    }

    /// 在给定写事务内把一条消息从主日志移入 DLQ（净额记账：消息行 → DLQ 行）。
    ///
    /// 返回 `false` = 主日志无该消息（调用方决定错误语义；ISR Follower 应用时为
    /// 幂等 no-op）。维护路径不受配额拒绝（B-PL-4）。
    fn move_to_dlq_tx(
        wtx: &redb::WriteTransaction,
        topic: &str,
        partition: u32,
        offset: u64,
        reason: &str,
        detail: &str,
    ) -> ServiceResult<bool> {
        let mk = encode_msg_key(topic, partition, offset);
        let raw = {
            let table = wtx.open_table(MESSAGE_TABLE)?;
            let x = table.get(mk.as_slice())?.map(|v| v.value().to_vec());
            x
        };
        let Some(raw) = raw else {
            return Ok(false);
        };

        let payload = match decode_message(&raw) {
            Some((p, _, _)) => p,
            None => return Err("failed to decode message".into()),
        };

        let dlq_encoded = encode_dlq_message(&payload, reason, detail);
        let dk = encode_dlq_key(topic, partition, offset);

        let removed_size = Self::entry_size(mk.len(), raw.len());
        let added_size = Self::entry_size(dk.len(), dlq_encoded.len());

        {
            let mut table = wtx.open_table(MESSAGE_TABLE)?;
            table.remove(mk.as_slice())?;
        }
        {
            let mut table = wtx.open_table(DLQ_TABLE)?;
            table.insert(dk.as_slice(), dlq_encoded.as_slice())?;
        }
        Self::account_sub_tx(wtx, removed_size)?;
        Self::account_add_tx(wtx, added_size)?;
        Ok(true)
    }

    pub fn consume_dlq(
        &self,
        topic: &str,
        partition: u32,
        max_count: u64,
    ) -> ServiceResult<Vec<DlqRecord>> {
        let prefix = msg_key_prefix(topic, partition);
        let prefix_len = prefix.len();

        let rtx = self.read_tx()?;
        let mut records = Vec::new();
        {
            let table = rtx.open_table(DLQ_TABLE)?;
            let range: std::ops::RangeFrom<&[u8]> = prefix.as_slice()..;
            for item in table.range(range)? {
                let (k, raw) = item?;
                let k = k.value();
                if !k.starts_with(&prefix) {
                    break;
                }
                if k.len() < prefix_len + 8 {
                    continue;
                }
                let Ok(off_bytes) = k[prefix_len..prefix_len + 8].try_into() else {
                    continue;
                };
                let offset = u64::from_be_bytes(off_bytes);
                if records.len() as u64 >= max_count {
                    break;
                }

                if let Some((payload, timestamp, reason, detail)) = decode_dlq_message(raw.value())
                {
                    records.push(DlqRecord {
                        offset,
                        payload,
                        timestamp,
                        error_reason: if reason.is_empty() {
                            None
                        } else {
                            Some(reason)
                        },
                        error_detail: if detail.is_empty() {
                            None
                        } else {
                            Some(detail)
                        },
                    });
                }
            }
        }
        Ok(records)
    }

    // ──── 统计信息 ────

    pub fn stats(&self) -> ServiceResult<MqStats> {
        let rtx = self.read_tx()?;

        let topic_count = {
            let table = rtx.open_table(TOPIC_TABLE)?;
            table.iter()?.count() as u64
        };

        let total_messages = {
            let table = rtx.open_table(MESSAGE_TABLE)?;
            table.iter()?.count() as u64
        };
        // 记账口径（消息 + DLQ 物理字节；见模块头「容量上界」）
        let total_bytes = {
            let meta = rtx.open_table(MQ_META_TABLE)?;
            let x = meta.get(META_ACTIVE_BYTES)?.map(|v| v.value()).unwrap_or(0);
            x
        };

        let dlq_messages = {
            let table = rtx.open_table(DLQ_TABLE)?;
            table.iter()?.count() as u64
        };

        Ok(MqStats {
            topic_count,
            total_messages,
            dlq_messages,
            total_bytes,
        })
    }

    /// 消费位点快照：(group, topic, partition, committed_offset)（G-MQ-3 指标采样用）
    pub fn consumer_offsets(&self) -> ServiceResult<Vec<(String, String, u32, u64)>> {
        let rtx = self.read_tx()?;
        let table = match rtx.open_table(OFFSET_TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        let mut out = Vec::new();
        for item in table.iter()? {
            let (k, v) = item?;
            let kb = k.value();
            if kb.len() < 12 {
                continue;
            }
            let Ok(glen_bytes) = kb[..4].try_into() else {
                continue;
            };
            let glen = u32::from_be_bytes(glen_bytes) as usize;
            let tlpos = 4 + glen;
            if kb.len() < tlpos + 4 {
                continue;
            }
            let Ok(tlen_bytes) = kb[tlpos..tlpos + 4].try_into() else {
                continue;
            };
            let tlen = u32::from_be_bytes(tlen_bytes) as usize;
            if kb.len() < tlpos + 4 + tlen + 4 {
                continue;
            }
            let group = String::from_utf8_lossy(&kb[4..4 + glen]).to_string();
            let topic = String::from_utf8_lossy(&kb[tlpos + 4..tlpos + 4 + tlen]).to_string();
            let plen = tlpos + 4 + tlen;
            let Ok(p_bytes) = kb[plen..plen + 4].try_into() else {
                continue;
            };
            let partition = u32::from_be_bytes(p_bytes);
            out.push((group, topic, partition, v.value()));
        }
        Ok(out)
    }

    /// 全部 topic 的 next-offset 快照：(topic, partition, next_offset)（G-MQ-3）
    pub fn next_offsets_all(&self) -> ServiceResult<Vec<(String, u32, u64)>> {
        let rtx = self.read_tx()?;
        let table = match rtx.open_table(NEXT_OFFSET_TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        let mut out = Vec::new();
        for item in table.iter()? {
            let (k, v) = item?;
            let kb = k.value();
            if kb.len() < 8 {
                continue;
            }
            let Ok(tlen_bytes) = kb[..4].try_into() else {
                continue;
            };
            let tlen = u32::from_be_bytes(tlen_bytes) as usize;
            if kb.len() < 4 + tlen + 4 {
                continue;
            }
            let topic = String::from_utf8_lossy(&kb[4..4 + tlen]).to_string();
            let Ok(p_bytes) = kb[4 + tlen..4 + tlen + 4].try_into() else {
                continue;
            };
            let partition = u32::from_be_bytes(p_bytes);
            out.push((topic, partition, v.value()));
        }
        Ok(out)
    }
}

// ──── ISR 复制（已落地）────
//
// 复制日志 / 持久化幂等键 / 本地序列号与本服务数据同 redb（mq.redb），
// 与数据写同事务提交（收敛决策：NEXT_OFFSET_TABLE 与复制条目同事务）。
// 分区 Leader 独占分配 offset 并广播；Follower 只应用不分配。

use crate::services::replication::{
    IdempotencyKey, ReplicatedStore, ReplicationEntry, ReplicationError, ReplicationOp,
};

impl MessageQueueService {
    /// 分区 shard 标识（topic 维度，单 shard 内序列号全局单调）
    fn topic_shard(topic: &str) -> String {
        format!("mq:{topic}")
    }

    /// 生成发布幂等键（key 携带 topic/partition/offset，全局唯一）
    fn publish_idem_key(topic: &str, partition: u32, offset: u64) -> IdempotencyKey {
        IdempotencyKey::new(
            format!("mq:publish:{topic}:{partition}:{offset}"),
            now_millis(),
        )
    }

    /// 某 shard 的下一个序列号（= 本地最后序列号 + 1）
    fn next_sequence(&self, shard: &str) -> ServiceResult<u64> {
        let rtx = self.read_tx()?;
        let table = rtx.open_table(REPL_LOCAL_SEQ)?;
        let seq = match table.get(shard.as_bytes())? {
            Some(v) => v.value(),
            None => 0,
        };
        Ok(seq + 1)
    }

    /// 在指定写事务内写入复制簿记（日志条目 + 持久化幂等键 + 本地序列号）
    fn write_repl_bookkeeping_tx(
        wtx: &redb::WriteTransaction,
        entry: &ReplicationEntry,
    ) -> ServiceResult<()> {
        let rk = encode_repl_key(&entry.shard_id, entry.sequence_num);
        let encoded = serde_json::to_vec(entry).map_err(|e| e.to_string())?;
        let mut t = wtx.open_table(REPL_ENTRY_TABLE)?;
        t.insert(rk.as_slice(), encoded.as_slice())?;

        let ik = entry.idempotency_key.to_string();
        let mut at = wtx.open_table(REPL_APPLIED_KEYS)?;
        at.insert(ik.as_bytes(), ())?;

        let sk = entry.shard_id.as_bytes();
        let mut lt = wtx.open_table(REPL_LOCAL_SEQ)?;
        let cur = match lt.get(sk)? {
            Some(v) => v.value(),
            None => 0,
        };
        lt.insert(sk, cur.max(entry.sequence_num))?;
        Ok(())
    }

    /// Leader 侧单事务提交：NEXT_OFFSET_TABLE + 消息 + 复制日志 + 幂等键 + 本地序列号。
    fn replicated_publish_local(
        &self,
        entry: &ReplicationEntry,
        idem_key: Option<&[u8]>,
        now_ms: u64,
    ) -> ServiceResult<()> {
        let (topic, partition, offset, payload) = match &entry.operation {
            ReplicationOp::MqPublish {
                topic,
                partition,
                offset,
                payload,
            } => (topic.as_str(), *partition, *offset, payload),
            _ => return Err("replicated_publish_local: not an MqPublish op".into()),
        };
        // ⚠️ headers 不进复制条目：`ReplicationEntry` 未建模 headers，
        // Leader 写进去而 Follower 写不进去会造成副本分叉 ⇒ 调用方若给了
        // headers（如消息 key），`produce_replicated` 已经 fail-loud 拒绝。
        let encoded = encode_message(payload, &BTreeMap::new());
        let nk = encode_next_offset_key(topic, partition);
        let mk = encode_msg_key(topic, partition, offset);
        let entry_size = Self::entry_size(mk.len(), encoded.len());

        let wtx = self.write_tx()?;
        // 配额在 Leader 本地提交前强制（Follower apply 刻意不检查——镜像已提交
        // 的决定，避免复制分叉；B-PL-4）
        self.ensure_quota_tx(&wtx, entry_size)?;
        {
            let cur = {
                let t = wtx.open_table(NEXT_OFFSET_TABLE)?;
                let x = match t.get(nk.as_slice())? {
                    Some(v) => v.value(),
                    None => 0,
                };
                x
            };
            let mut t = wtx.open_table(NEXT_OFFSET_TABLE)?;
            t.insert(nk.as_slice(), cur.max(offset + 1))?;

            let mut t = wtx.open_table(MESSAGE_TABLE)?;
            t.insert(mk.as_slice(), encoded.as_slice())?;

            // 生产者级幂等索引：与 offset 分配 / 消息写入**同事务**
            if let Some(ik) = idem_key {
                let bytes = serde_json::to_vec(&IdempotencyEntry {
                    offset,
                    ts_ms: now_ms,
                })?;
                let mut t = wtx.open_table(IDEMPOTENCY_TABLE)?;
                t.insert(ik, bytes.as_slice())?;
            }

            // 记账（与消息同事务；B-PL-4）
            Self::account_add_tx(&wtx, entry_size)?;

            Self::write_repl_bookkeeping_tx(&wtx, entry)?;
        }
        wtx.commit()?;
        Ok(())
    }

    /// Follower 侧应用 MqPublish（幂等；单事务：幂等检查 + 自动建 topic + 消息 +
    /// NEXT_OFFSET_TABLE + 复制日志 + 幂等键 + 本地序列号）。
    fn apply_mq_publish(&self, entry: &ReplicationEntry) -> Result<(), ReplicationError> {
        let (topic, partition, offset, payload) = match &entry.operation {
            ReplicationOp::MqPublish {
                topic,
                partition,
                offset,
                payload,
            } => (topic.as_str(), *partition, *offset, payload),
            _ => {
                return Err(ReplicationError::Store(
                    "apply_mq_publish: not an MqPublish op".to_string(),
                ))
            }
        };
        let wtx = match self.write_tx() {
            Ok(t) => t,
            Err(e) => return Err(ReplicationError::Store(e.to_string())),
        };
        let result: ServiceResult<()> = (|| {
            // 幂等检查（持久化键）
            let ik = entry.idempotency_key.to_string();
            let applied = {
                let t = wtx.open_table(REPL_APPLIED_KEYS)?;
                let x = t.get(ik.as_bytes())?.is_some();
                x
            };
            if applied {
                return Ok(());
            }
            // 自动创建 topic（若本 agent 尚未创建；复制路径不复制 topic 元数据）
            let exists = {
                let t = wtx.open_table(TOPIC_TABLE)?;
                let x = t.get(topic)?.is_some();
                x
            };
            if !exists {
                let mut t = wtx.open_table(TOPIC_TABLE)?;
                let cfg = TopicConfig {
                    partitions: (partition + 1).max(1),
                    retention_secs: 86400,
                    max_message_size: 1024 * 1024,
                };
                let raw = serde_json::to_vec(&cfg).map_err(|e| e.to_string())?;
                t.insert(topic, raw.as_slice())?;
            }
            let encoded = encode_message(payload, &BTreeMap::new());
            let nk = encode_next_offset_key(topic, partition);
            let mk = encode_msg_key(topic, partition, offset);
            let cur = {
                let t = wtx.open_table(NEXT_OFFSET_TABLE)?;
                let x = match t.get(nk.as_slice())? {
                    Some(v) => v.value(),
                    None => 0,
                };
                x
            };
            let mut t = wtx.open_table(NEXT_OFFSET_TABLE)?;
            t.insert(nk.as_slice(), cur.max(offset + 1))?;
            let mut t = wtx.open_table(MESSAGE_TABLE)?;
            t.insert(mk.as_slice(), encoded.as_slice())?;

            // 记账必须包含镜像写入（否则本节点配额记账失真）；配额检查刻意
            // 缺席 —— Follower 镜像 Leader 已提交的决定（B-PL-4）
            Self::account_add_tx(&wtx, Self::entry_size(mk.len(), encoded.len()))?;

            Self::write_repl_bookkeeping_tx(&wtx, entry)?;
            Ok(())
        })();
        match result {
            Ok(()) => {
                wtx.commit()
                    .map_err(|e| ReplicationError::Store(e.to_string()))?;
                Ok(())
            }
            Err(e) => Err(ReplicationError::Store(e.to_string())),
        }
    }

    /// Leader 侧复制生产：单事务本地提交（含 offset 分配）→
    /// 推送 ISR Followers（同步复制）→ min_isr 校验 → Leader 推送订阅者。
    pub async fn produce_replicated(
        &self,
        topic: &str,
        partition: u32,
        payload: Vec<u8>,
        headers: Option<BTreeMap<String, String>>,
        idempotency_key: Option<&str>,
    ) -> ServiceResult<u64> {
        let rm = self
            .replication
            .read()
            .clone()
            .ok_or_else(|| "replication not enabled".to_string())?;
        let shard = Self::topic_shard(topic);
        if !rm.is_leader(&shard) {
            return Err(format!(
                "not leader for shard '{shard}' (leader is {})",
                rm.shard_leader(&shard)
            )
            .into());
        }

        let config = self
            .get_topic_config(topic)?
            .ok_or_else(|| format!("topic '{topic}' not found"))?;
        if partition >= config.partitions {
            return Err(format!(
                "partition {partition} out of range for topic '{topic}' (max {})",
                config.partitions
            )
            .into());
        }
        if payload.len() as u64 > config.max_message_size {
            return Err(format!(
                "message size {} exceeds max {}",
                payload.len(),
                config.max_message_size
            )
            .into());
        }

        // ⚠️ 诚实边界（fail-loud，不静默丢）：`ReplicationEntry` 未建模 headers，
        // 因此消息 key / headers 在复制路径上**无法传播**。不得直接丢弃
        // `_headers`（调用方会以为写入成功）——这里必须明确报错。
        if headers.as_ref().is_some_and(|h| !h.is_empty()) {
            return Err("mq: message headers (e.g. key) are not carried by the ISR \
                        replication entry; refusing to drop them silently — use \
                        services.replication=false, or omit the key"
                .into());
        }

        // 生产者级幂等：与本地路径共用同一张表、同一语义。
        // 命中则直接返回首次分配的 offset，不追加消息、不推进计数器。
        let idem_key = idempotency_key
            .filter(|k| !k.is_empty())
            .map(|k| encode_idempotency_key(topic, partition, k));
        if let Some(ik) = idem_key.as_ref() {
            let rtx = self.read_tx()?;
            let t = rtx.open_table(IDEMPOTENCY_TABLE)?;
            if let Some(v) = t.get(ik.as_slice())? {
                let e: IdempotencyEntry = serde_json::from_slice(v.value())?;
                return Ok(e.offset);
            }
        }

        let seq = self.next_sequence(&shard)?;
        let offset = {
            let rtx = self.read_tx()?;
            let nk = encode_next_offset_key(topic, partition);
            let t = rtx.open_table(NEXT_OFFSET_TABLE)?;
            match t.get(nk.as_slice())? {
                Some(v) => v.value(),
                None => 0,
            }
        };
        let notify_payload = payload.clone();
        let entry = ReplicationEntry::new_mq_publish(
            Self::publish_idem_key(topic, partition, offset),
            shard.clone(),
            topic.to_string(),
            partition,
            offset,
            payload,
            seq,
        );

        // 单事务本地提交（NEXT_OFFSET_TABLE + 消息 + 复制日志 + 幂等键 + 本地序列号）
        self.replicated_publish_local(&entry, idem_key.as_deref(), now_millis())?;

        // 同步复制：推送到 ISR Followers，min_isr 校验（自身 + 确认 follower 数）
        let acked = rm
            .push_to_followers(&entry)
            .await
            .map_err(|e| e.to_string())?;
        rm.ensure_isr(acked + 1).map_err(|e| e.to_string())?;

        // Leader 推送订阅者（仅 Leader 推送）
        self.notify_subscribers(topic, partition, offset, notify_payload, now_millis());

        Ok(offset)
    }

    /// topic 删除幂等键（携带 shard 序列号 ⇒ 全局唯一）
    fn delete_idem_key(topic: &str, seq: u64) -> IdempotencyKey {
        IdempotencyKey::new(format!("mq:delete-topic:{topic}:{seq}"), now_millis())
    }

    /// Leader 侧删除（ISR 启用）：本地单事务全量回收 + 复制日志记录删除决定 →
    /// 推送到 ISR Followers → min_isr 校验。删除决定留存于复制日志 ⇒ 落后的
    /// Follower 重连后经 Reconcile 重放（G-MQ-4 全域一致）。
    pub async fn delete_topic_replicated(&self, topic: &str) -> ServiceResult<DeleteTopicStats> {
        let rm = self
            .replication
            .read()
            .clone()
            .ok_or_else(|| "replication not enabled".to_string())?;
        let shard = Self::topic_shard(topic);
        if !rm.is_leader(&shard) {
            return Err(format!(
                "not leader for shard '{shard}' (leader is {})",
                rm.shard_leader(&shard)
            )
            .into());
        }
        if self.get_topic_config(topic)?.is_none() {
            return Err(format!("topic '{topic}' not found").into());
        }

        let seq = self.next_sequence(&shard)?;
        let entry = ReplicationEntry::new_mq_delete_topic(
            Self::delete_idem_key(topic, seq),
            shard.clone(),
            topic.to_string(),
            seq,
        );

        // 本地：全量回收 + 配置行删除 + 复制簿记（同事务）
        let stats = {
            let wtx = self.write_tx()?;
            let stats = Self::purge_topic_tx(&wtx, topic)?;
            {
                let mut table = wtx.open_table(TOPIC_TABLE)?;
                table.remove(topic)?;
            }
            Self::write_repl_bookkeeping_tx(&wtx, &entry)?;
            wtx.commit()?;
            stats
        };

        // 全域下发（Follower 幂等应用）；min_isr 不足时返回错误，
        // 删除决定仍在复制日志中，Follower 重连后补课
        let acked = rm
            .push_to_followers(&entry)
            .await
            .map_err(|e| e.to_string())?;
        rm.ensure_isr(acked + 1).map_err(|e| e.to_string())?;
        Ok(stats)
    }

    /// Leader 侧移入 DLQ（ISR 启用）：本地净额迁移 + 复制簿记同事务 →
    /// 推送到 ISR Followers（副本不分叉）→ min_isr 校验。
    pub async fn move_to_dlq_replicated(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        reason: &str,
        detail: &str,
    ) -> ServiceResult<()> {
        let rm = self
            .replication
            .read()
            .clone()
            .ok_or_else(|| "replication not enabled".to_string())?;
        let shard = Self::topic_shard(topic);
        if !rm.is_leader(&shard) {
            return Err(format!(
                "not leader for shard '{shard}' (leader is {})",
                rm.shard_leader(&shard)
            )
            .into());
        }

        let seq = self.next_sequence(&shard)?;
        let entry = ReplicationEntry::new_mq_move_to_dlq(
            IdempotencyKey::new(
                format!("mq:dlq:{topic}:{partition}:{offset}:{seq}"),
                now_millis(),
            ),
            shard.clone(),
            topic.to_string(),
            partition,
            offset,
            reason.to_string(),
            detail.to_string(),
            seq,
        );

        let moved = {
            let wtx = self.write_tx()?;
            let moved = Self::move_to_dlq_tx(&wtx, topic, partition, offset, reason, detail)?;
            if moved {
                Self::write_repl_bookkeeping_tx(&wtx, &entry)?;
            }
            wtx.commit()?;
            moved
        };
        if !moved {
            return Err(format!("message {topic}/{partition}/{offset} not found").into());
        }

        let acked = rm
            .push_to_followers(&entry)
            .await
            .map_err(|e| e.to_string())?;
        rm.ensure_isr(acked + 1).map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Follower 侧应用 MqMoveToDlq（幂等；主日志无该消息时为 no-op 但仍记幂等键
    /// —— 重放/追平不重复入 DLQ，也不阻断序列推进）
    fn apply_mq_move_to_dlq(&self, entry: &ReplicationEntry) -> Result<(), ReplicationError> {
        let (topic, partition, offset, reason, detail) = match &entry.operation {
            ReplicationOp::MqMoveToDlq {
                topic,
                partition,
                offset,
                reason,
                detail,
            } => (
                topic.as_str(),
                *partition,
                *offset,
                reason.as_str(),
                detail.as_str(),
            ),
            _ => {
                return Err(ReplicationError::Store(
                    "apply_mq_move_to_dlq: not an MqMoveToDlq op".to_string(),
                ))
            }
        };
        let wtx = match self.write_tx() {
            Ok(t) => t,
            Err(e) => return Err(ReplicationError::Store(e.to_string())),
        };
        let result: ServiceResult<()> = (|| {
            let ik = entry.idempotency_key.to_string();
            let applied = {
                let t = wtx.open_table(REPL_APPLIED_KEYS)?;
                let x = t.get(ik.as_bytes())?.is_some();
                x
            };
            if applied {
                return Ok(());
            }
            let _ = Self::move_to_dlq_tx(&wtx, topic, partition, offset, reason, detail)?;
            Self::write_repl_bookkeeping_tx(&wtx, entry)?;
            Ok(())
        })();
        match result {
            Ok(()) => {
                wtx.commit()
                    .map_err(|e| ReplicationError::Store(e.to_string()))?;
                Ok(())
            }
            Err(e) => Err(ReplicationError::Store(e.to_string())),
        }
    }

    /// Follower 侧应用 MqDeleteTopic（幂等；单事务：幂等检查 + 全量回收 + 簿记）
    fn apply_mq_delete_topic(&self, entry: &ReplicationEntry) -> Result<(), ReplicationError> {
        let topic = match &entry.operation {
            ReplicationOp::MqDeleteTopic { topic } => topic.as_str(),
            _ => {
                return Err(ReplicationError::Store(
                    "apply_mq_delete_topic: not an MqDeleteTopic op".to_string(),
                ))
            }
        };
        let wtx = match self.write_tx() {
            Ok(t) => t,
            Err(e) => return Err(ReplicationError::Store(e.to_string())),
        };
        let result: ServiceResult<()> = (|| {
            let ik = entry.idempotency_key.to_string();
            let applied = {
                let t = wtx.open_table(REPL_APPLIED_KEYS)?;
                let x = t.get(ik.as_bytes())?.is_some();
                x
            };
            if applied {
                return Ok(());
            }
            Self::purge_topic_tx(&wtx, topic)?;
            {
                let mut t = wtx.open_table(TOPIC_TABLE)?;
                t.remove(topic)?;
            }
            Self::write_repl_bookkeeping_tx(&wtx, entry)?;
            Ok(())
        })();
        match result {
            Ok(()) => {
                wtx.commit()
                    .map_err(|e| ReplicationError::Store(e.to_string()))?;
                Ok(())
            }
            Err(e) => Err(ReplicationError::Store(e.to_string())),
        }
    }
}

// ──── ReplicatedStore（复制存储接口实现）────

impl ReplicatedStore for MessageQueueService {
    fn shards(&self) -> Vec<String> {
        match self.list_topics() {
            Ok(topics) => topics
                .into_iter()
                .map(|t| Self::topic_shard(&t.name))
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    fn last_local_sequence(&self, shard: &str) -> u64 {
        let rtx = match self.read_tx() {
            Ok(t) => t,
            Err(_) => return 0,
        };
        let table = match rtx.open_table(REPL_LOCAL_SEQ) {
            Ok(t) => t,
            Err(_) => return 0,
        };
        match table.get(shard.as_bytes()) {
            Ok(Some(v)) => v.value(),
            _ => 0,
        }
    }

    fn apply_entry(&self, entry: &ReplicationEntry) -> Result<(), ReplicationError> {
        match &entry.operation {
            ReplicationOp::MqPublish { .. } => self.apply_mq_publish(entry),
            ReplicationOp::MqDeleteTopic { .. } => self.apply_mq_delete_topic(entry),
            ReplicationOp::MqMoveToDlq { .. } => self.apply_mq_move_to_dlq(entry),
            other => Err(ReplicationError::Store(format!(
                "mq cannot apply op {other:?}"
            ))),
        }
    }

    fn read_entries(&self, shard: &str, from_seq: u64, limit: u64) -> Vec<ReplicationEntry> {
        let prefix = encode_repl_prefix(shard);
        let plen = prefix.len();
        let rtx = match self.read_tx() {
            Ok(t) => t,
            Err(_) => return Vec::new(),
        };
        let table = match rtx.open_table(REPL_ENTRY_TABLE) {
            Ok(t) => t,
            Err(_) => return Vec::new(),
        };
        let mut out = Vec::new();
        let range: std::ops::RangeFrom<&[u8]> = prefix.as_slice()..;
        if let Ok(iter) = table.range(range) {
            for item in iter {
                let (k, raw) = match item {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let k = k.value();
                if !k.starts_with(&prefix) {
                    break;
                }
                let seq = match decode_repl_seq(k, plen) {
                    Some(s) => s,
                    None => continue,
                };
                if seq < from_seq {
                    continue;
                }
                if limit > 0 && out.len() as u64 >= limit {
                    break;
                }
                if let Ok(entry) = serde_json::from_slice::<ReplicationEntry>(raw.value()) {
                    out.push(entry);
                }
            }
        }
        out
    }
}

// ──── BaseService trait ────

#[async_trait]
impl BaseService for MessageQueueService {
    fn name(&self) -> &'static str {
        "mq"
    }

    async fn start(&self) -> ServiceResult<()> {
        if *self.started.read() {
            return Ok(());
        }

        let db_path = self.db_path.join("mq.redb");
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let db = if db_path.exists() {
            redb::Database::open(&db_path)?
        } else {
            redb::Database::create(&db_path)?
        };
        let wtx = db.begin_write()?;
        {
            wtx.open_table(TOPIC_TABLE)?;
            wtx.open_table(MESSAGE_TABLE)?;
            wtx.open_table(OFFSET_TABLE)?;
            wtx.open_table(DLQ_TABLE)?;
            wtx.open_table(NEXT_OFFSET_TABLE)?;
            wtx.open_table(REPL_ENTRY_TABLE)?;
            wtx.open_table(REPL_APPLIED_KEYS)?;
            wtx.open_table(REPL_LOCAL_SEQ)?;
            wtx.open_table(MQ_META_TABLE)?;
        }
        wtx.commit()?;

        *self.db.write() = Some(db);
        // 记账初始化（旧库无 mq:meta ⇒ 启动时一次性重建）—— 必须在
        // started=true / reaper 挂载之前完成；失败时回退 db 句柄，避免重试
        // start 时对仍打开的文件重开（redb 会拒绝）。
        let accounted = match self.ensure_accounting_initialized() {
            Ok(a) => a,
            Err(e) => {
                *self.db.write() = None;
                return Err(e);
            }
        };
        *self.started.write() = true;
        self.spawn_reaper();
        tracing::info!(
            db_path = %db_path.display(),
            max_size_bytes = self.max_size_bytes,
            accounted_bytes = accounted,
            reaper_interval_ms = self.reaper_interval().as_millis() as u64,
            "MessageQueueService started (capacity quota + retention reaper active; \
             see boundaries.md B-PL-4)"
        );
        Ok(())
    }

    async fn stop(&self) -> ServiceResult<()> {
        if !*self.started.read() {
            return Ok(());
        }
        *self.db.write() = None;
        *self.started.write() = false;
        tracing::info!("MessageQueueService stopped");
        Ok(())
    }

    fn health_check(&self) -> bool {
        if !*self.started.read() {
            return false;
        }
        self.read_tx().is_ok()
    }
}

// ──── 单元测试 ────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("failed to create temp dir")
    }

    fn new_svc(dir: &TempDir) -> MessageQueueService {
        let svc = MessageQueueService::new(dir.path().to_path_buf(), 1024 * 1024 * 1024);
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        rt.block_on(async { svc.start().await.expect("start") });
        svc
    }

    #[test]
    fn test_name_and_state() {
        let dir = temp_dir();
        let svc = MessageQueueService::new(dir.path().to_path_buf(), 1024 * 1024);
        assert_eq!(svc.name(), "mq");
        assert!(!svc.health_check());
    }

    #[test]
    fn test_create_and_list_topics() {
        let dir = temp_dir();
        let svc = new_svc(&dir);
        svc.create_topic(
            "t1",
            TopicConfig {
                partitions: 1,
                retention_secs: 60,
                max_message_size: 1024,
            },
        )
        .unwrap();
        assert_eq!(svc.list_topics().unwrap().len(), 1);
    }

    #[test]
    fn test_produce_consume_basic() {
        let dir = temp_dir();
        let svc = new_svc(&dir);
        svc.create_topic(
            "test",
            TopicConfig {
                partitions: 1,
                retention_secs: 3600,
                max_message_size: 1024,
            },
        )
        .unwrap();
        svc.produce("test", 0, b"hello".to_vec(), None).unwrap();
        let msgs = svc.consume("test", 0, 0, 10).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].payload, b"hello");
        assert_eq!(msgs[0].offset, 0);
    }

    #[test]
    fn test_consumer_offset() {
        let dir = temp_dir();
        let svc = new_svc(&dir);
        svc.create_topic(
            "test",
            TopicConfig {
                partitions: 1,
                retention_secs: 3600,
                max_message_size: 1024,
            },
        )
        .unwrap();
        svc.produce("test", 0, b"m1".to_vec(), None).unwrap();
        svc.produce("test", 0, b"m2".to_vec(), None).unwrap();
        svc.commit_offset("g1", "test", 0, 1).unwrap();
        assert_eq!(svc.get_consumer_offset("g1", "test", 0).unwrap(), 1);
    }

    #[test]
    fn test_dlq() {
        let dir = temp_dir();
        let svc = new_svc(&dir);
        svc.create_topic(
            "test",
            TopicConfig {
                partitions: 1,
                retention_secs: 3600,
                max_message_size: 1024,
            },
        )
        .unwrap();
        svc.produce("test", 0, b"bad".to_vec(), None).unwrap();
        svc.move_to_dlq("test", 0, 0, "err", "details").unwrap();
        let dlq = svc.consume_dlq("test", 0, 10).unwrap();
        assert_eq!(dlq.len(), 1);
        assert_eq!(dlq[0].payload, b"bad");
    }

    #[test]
    fn test_persistence() {
        let dir = temp_dir();
        let db_path = dir.path().to_path_buf();
        let rt = tokio::runtime::Runtime::new().unwrap();
        {
            let svc = MessageQueueService::new(db_path.clone(), 1024 * 1024);
            rt.block_on(async { svc.start().await.unwrap() });
            svc.create_topic(
                "p",
                TopicConfig {
                    partitions: 1,
                    retention_secs: 60,
                    max_message_size: 1024,
                },
            )
            .unwrap();
            svc.produce("p", 0, b"data".to_vec(), None).unwrap();
        }
        {
            let svc = MessageQueueService::new(db_path.clone(), 1024 * 1024);
            rt.block_on(async { svc.start().await.unwrap() });
            assert!(svc.topic_exists("p").unwrap());
            let msgs = svc.consume("p", 0, 0, 10).unwrap();
            assert_eq!(msgs.len(), 1);
        }
    }

    /// 流式订阅 —— 回放已提交偏移之后的消息 + produce 实时推送
    #[test]
    fn test_subscribe_replay_and_push() {
        use tokio::sync::mpsc;

        let dir = temp_dir();
        let svc = new_svc(&dir);
        svc.create_topic(
            "sub-topic",
            TopicConfig {
                partitions: 1,
                retention_secs: 3600,
                max_message_size: 1024,
            },
        )
        .unwrap();
        svc.produce("sub-topic", 0, b"pre-1".to_vec(), None)
            .unwrap();
        svc.produce("sub-topic", 0, b"pre-2".to_vec(), None)
            .unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        let (tx, mut rx) = mpsc::channel::<(u32, MessageRecord)>(16);

        // 订阅：回放已有消息
        rt.block_on(async {
            svc.subscribe("sub-topic", "cg-sub", tx).await.unwrap();
        });

        let mut received: Vec<(u32, Vec<u8>)> = Vec::new();
        while let Ok((p, m)) = rx.try_recv() {
            received.push((p, m.payload));
        }
        assert_eq!(received.len(), 2, "订阅时应回放 2 条已有消息");
        assert_eq!(received[0].0, 0);
        assert_eq!(received[0].1, b"pre-1".to_vec());
        assert_eq!(received[1].0, 0);
        assert_eq!(received[1].1, b"pre-2".to_vec());

        // 新 produce → 实时推送
        svc.produce("sub-topic", 0, b"live-3".to_vec(), None)
            .unwrap();
        rt.block_on(async {
            let (p, m) = rx.recv().await.expect("should be pushed");
            assert_eq!(p, 0);
            assert_eq!(m.payload, b"live-3");
        });

        // 偏移已自动提交 → 新订阅不重复回放
        assert_eq!(
            svc.get_consumer_offset("cg-sub", "sub-topic", 0).unwrap(),
            3
        );
        let (tx2, mut rx2) = mpsc::channel::<(u32, MessageRecord)>(16);
        rt.block_on(async {
            svc.subscribe("sub-topic", "cg-sub", tx2).await.unwrap();
        });
        assert!(rx2.try_recv().is_err(), "已提交偏移后新订阅不应重放旧消息");
    }

    // ── 容量上界（B-PL-4）：记账 / 配额 / retention reaper ──
    //
    // 负控制（提交前已实跑，破坏后还原）：
    // - 移除 `reap_once` 中对 `MESSAGE_TABLE` 的 `purge_expired_in_table` 调用 ⇒
    //   `test_reaper_purges_expired_messages` 必红；
    // - 移除 `produce_idempotent` 的 `ensure_quota_tx` 调用 ⇒
    //   `test_produce_rejected_over_limit` 必红；
    // - 移除 `produce_idempotent` 的 `account_add_tx` 调用 ⇒
    //   `test_accounting_bytes_tracked` 必红。

    fn new_svc_with_max(dir: &TempDir, max_size_bytes: u64) -> MessageQueueService {
        let svc = MessageQueueService::new(dir.path().to_path_buf(), max_size_bytes);
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        rt.block_on(async { svc.start().await.expect("start") });
        svc
    }

    fn topic_cfg(partitions: u32, retention_secs: u64) -> TopicConfig {
        TopicConfig {
            partitions,
            retention_secs,
            max_message_size: 1024,
        }
    }

    /// 消息行的记账大小（口径 = 物理 key + 存储值）：
    /// key = [topic_len:4][topic][partition:4][offset:8]；
    /// value = [ts:8][headers_len:4][headers_json("{}")=2][payload]。
    fn message_entry_size(topic: &str, payload_len: usize) -> u64 {
        (4 + topic.len() + 4 + 8 + 8 + 4 + 2 + payload_len) as u64
    }

    /// 记账口径：单条 = 物理 key 长度 + 存储值长度；consume 不回收；
    /// move_to_dlq 为净额调整（消息行 → DLQ 行）。
    #[test]
    fn test_accounting_bytes_tracked() {
        let dir = temp_dir();
        let svc = new_svc_with_max(&dir, 1024 * 1024);
        svc.create_topic("t", topic_cfg(1, 3600)).unwrap();
        assert_eq!(svc.accounted_bytes().unwrap(), 0);

        let one = message_entry_size("t", 5); // "hello"
        svc.produce("t", 0, b"hello".to_vec(), None).unwrap();
        assert_eq!(svc.accounted_bytes().unwrap(), one);
        assert_eq!(svc.stats().unwrap().total_bytes, one);

        svc.produce("t", 0, b"hello".to_vec(), None).unwrap();
        assert_eq!(svc.accounted_bytes().unwrap(), 2 * one);

        // consume 不回收配额（回收只由 retention reaper / move_to_dlq 产生）
        assert_eq!(svc.consume("t", 0, 0, 10).unwrap().len(), 2);
        assert_eq!(svc.accounted_bytes().unwrap(), 2 * one);

        // move_to_dlq：扣消息行，加 DLQ 行（dk 同长 17；
        // dlq value = 8 + (4+3) + (4+7) + 5 = 31）
        svc.move_to_dlq("t", 0, 0, "err", "details").unwrap();
        let dlq_one = 17 + 8 + (4 + 3) + (4 + 7) + 5;
        assert_eq!(svc.accounted_bytes().unwrap(), one + dlq_one);
        assert_eq!(svc.stats().unwrap().total_bytes, one + dlq_one);
    }

    /// 配额：publish 入口**严格**拒绝（超界不写入、不推进 offset、计数），
    /// 错误含 `max_size_bytes` 锚点（handler 映射 RESOURCE_EXHAUSTED）。
    #[test]
    fn test_produce_rejected_over_limit() {
        let dir = temp_dir();
        // 两条 36B 消息已 72B；仅剩 8B —— 第三条必须被拒
        let svc = new_svc_with_max(&dir, 80);
        svc.create_topic("t", topic_cfg(1, 3600)).unwrap();
        svc.produce("t", 0, b"hello".to_vec(), None).unwrap();
        svc.produce("t", 0, b"hello".to_vec(), None).unwrap();
        assert_eq!(svc.accounted_bytes().unwrap(), 72);

        let err = svc
            .produce("t", 0, b"hello".to_vec(), None)
            .expect_err("over quota must be rejected");
        assert!(
            err.to_string().contains("max_size_bytes"),
            "错误信息需含 max_size_bytes 锚点（handler 映射 RESOURCE_EXHAUSTED）: {err}"
        );
        assert_eq!(svc.accounted_bytes().unwrap(), 72, "拒绝不得产生任何写入");
        assert_eq!(svc.publish_rejections(), 1);
        assert_eq!(svc.consume("t", 0, 0, 10).unwrap().len(), 2);

        // 单条自身超上界：同路径拒绝（永远装不下；写成功再回收等于假成功）
        let err = svc
            .produce("t", 0, vec![0u8; 300], None)
            .expect_err("single oversize entry must be rejected");
        assert!(err.to_string().contains("max_size_bytes"));
        assert_eq!(svc.accounted_bytes().unwrap(), 72);
        assert_eq!(svc.publish_rejections(), 2);
    }

    /// retention 回收（负控制目标）：过期消息由 reaper 物理删除并扣账；
    /// 回收释放的配额可再次写入，被拒的 publish 未推进 offset ⇒ 新消息 offset=2。
    #[test]
    fn test_reaper_purges_expired_messages() {
        let dir = temp_dir();
        let svc = new_svc_with_max(&dir, 80);
        svc.create_topic("t", topic_cfg(1, 1)).unwrap(); // retention = 1s
        svc.produce("t", 0, b"hello".to_vec(), None).unwrap();
        svc.produce("t", 0, b"hello".to_vec(), None).unwrap();
        let err = svc
            .produce("t", 0, b"hello".to_vec(), None)
            .expect_err("full");
        assert!(err.to_string().contains("max_size_bytes"));

        std::thread::sleep(std::time::Duration::from_millis(1_200));
        let stats = svc.reap_once().unwrap();
        assert_eq!(stats.expired_messages, 2);
        assert_eq!(stats.expired_dlq, 0);
        assert_eq!(stats.purged_bytes, 72);
        assert_eq!(stats.active_bytes, 0);
        assert_eq!(svc.accounted_bytes().unwrap(), 0);

        // 幂等：已在界内 ⇒ 再跑一轮不再回收
        let again = svc.reap_once().unwrap();
        assert_eq!(again.expired_messages, 0);
        assert_eq!(again.purged_bytes, 0);

        // 回收释放配额；被拒的 publish 未推进 offset ⇒ 新消息 offset = 2
        let off = svc.produce("t", 0, b"hello".to_vec(), None).unwrap();
        assert_eq!(off, 2);
        let counters = svc.reap_counters();
        assert_eq!(counters.passes, 2);
        assert_eq!(counters.expired_messages, 2);
        assert_eq!(counters.expired_dlq, 0);
        assert_eq!(counters.purged_bytes, 72);
        assert_eq!(counters.faults, 0);
    }

    /// DLQ 同窗口回收：move_to_dlq 的条目也按 topic retention 清扫、释放配额。
    #[test]
    fn test_reaper_purges_expired_dlq() {
        let dir = temp_dir();
        let svc = new_svc_with_max(&dir, 1024 * 1024);
        svc.create_topic("t", topic_cfg(1, 1)).unwrap();
        svc.produce("t", 0, b"bad".to_vec(), None).unwrap();
        svc.move_to_dlq("t", 0, 0, "err", "details").unwrap();
        assert_eq!(svc.consume_dlq("t", 0, 10).unwrap().len(), 1);
        assert!(svc.accounted_bytes().unwrap() > 0);

        std::thread::sleep(std::time::Duration::from_millis(1_200));
        let stats = svc.reap_once().unwrap();
        assert_eq!(stats.expired_messages, 0);
        assert_eq!(stats.expired_dlq, 1);
        assert_eq!(stats.active_bytes, 0);
        assert!(svc.consume_dlq("t", 0, 10).unwrap().is_empty());
        assert_eq!(svc.accounted_bytes().unwrap(), 0);
    }

    /// `retention_secs = 0` ⇒ 不做时间回收（不误删）。
    #[test]
    fn test_retention_zero_disables_time_purge() {
        let dir = temp_dir();
        let svc = new_svc_with_max(&dir, 1024 * 1024);
        svc.create_topic("t", topic_cfg(1, 0)).unwrap();
        svc.produce("t", 0, b"keep".to_vec(), None).unwrap();
        let stats = svc.reap_once().unwrap();
        assert_eq!(stats.purged_bytes, 0);
        assert_eq!(svc.consume("t", 0, 0, 10).unwrap().len(), 1);
    }

    /// 记账跨重启持久化（redb 同事务真值）：重启后继续使用不漂移。
    #[test]
    fn test_accounting_persists_across_restart() {
        let dir = temp_dir();
        let db_path = dir.path().to_path_buf();
        let expected;
        {
            let svc = new_svc_with_max(&dir, 4096);
            svc.create_topic("p", topic_cfg(1, 3600)).unwrap();
            svc.produce("p", 0, b"persist".to_vec(), None).unwrap();
            expected = svc.accounted_bytes().unwrap();
            assert!(expected > 0);
            // drop（未 stop）——模拟重启前崩溃窗口
        }
        let svc = MessageQueueService::new(db_path, 4096);
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async { svc.start().await.expect("restart") });
        assert_eq!(svc.accounted_bytes().unwrap(), expected);
        assert_eq!(svc.reap_once().unwrap().purged_bytes, 0);
    }

    /// 旧库迁移：无 `mq:meta` 的存量库启动时一次性重建记账。
    #[test]
    fn test_migration_rebuilds_accounting_for_legacy_db() {
        let dir = temp_dir();
        let db_path = dir.path().join("mq.redb");

        // 手工构造「旧版本」库：只有消息表，无 mq:meta
        {
            let db = redb::Database::create(&db_path).unwrap();
            let wtx = db.begin_write().unwrap();
            {
                let mut t = wtx.open_table(MESSAGE_TABLE).unwrap();
                let mk = encode_msg_key("legacy", 0, 0);
                let encoded = encode_message(b"old", &BTreeMap::new());
                t.insert(mk.as_slice(), encoded.as_slice()).unwrap();
            }
            wtx.commit().unwrap();
        }

        let svc = MessageQueueService::new(dir.path().to_path_buf(), 4096);
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async { svc.start().await.expect("start") });
        assert_eq!(
            svc.accounted_bytes().unwrap(),
            message_entry_size("legacy", 3)
        );
    }

    /// 后台 reaper 闭环：绑定 self_arc + 短周期后，过期消息**无需手动调用**
    /// reap 即被回收。负控制：移除 `start` 中的 `spawn_reaper` ⇒ 本测试必红（超时）。
    #[test]
    fn test_background_reaper_converges() {
        let dir = temp_dir();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let svc = Arc::new(MessageQueueService::new(
            dir.path().to_path_buf(),
            1024 * 1024,
        ));
        svc.bind_self_weak(&svc);
        svc.set_reaper_interval(std::time::Duration::from_millis(50));
        rt.block_on(async { svc.start().await.expect("start") });
        svc.create_topic("t", topic_cfg(1, 1)).unwrap();
        svc.produce("t", 0, b"x".to_vec(), None).unwrap();
        assert!(svc.accounted_bytes().unwrap() > 0);

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let accounted = svc.accounted_bytes().unwrap();
            if accounted == 0 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "后台 reaper 未在期限内回收: accounted={accounted}"
            );
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert!(svc.reap_counters().passes > 0);
    }
}
