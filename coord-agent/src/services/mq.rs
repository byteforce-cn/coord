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
// ✅ 状态声明（2026-08-08）：ISR 复制**已实现并落地**——
// - `produce_replicated`：分区 Leader 独占分配 offset，单事务（NEXT_OFFSET_TABLE
//   + 消息 + 复制日志 + 幂等键 + 本地序列号）→ 同步推送到 ISR Followers → min_isr 校验
// - Follower 幂等应用 + 自动建 topic；subscribe / ack 仅 Leader
// - 默认关闭（services.replication=false）= 纯单 agent，零破坏；启用后可宣称分布式 / 高可用

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

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
    pub total_bytes: u64,
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

// ──── 生产幂等表（F-57）────
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
    /// 声明的容量上限（字节）。**当前未被强制执行**，与
    /// [`crate::services::cache::CacheService`] 同一取舍（第四轮 §3.10 h/i）：
    /// 保留值以便如实报出，而不是用一个 `#[allow(dead_code)]` 字段假装遵守。
    max_size_bytes: u64,
    /// 订阅者注册表：topic → (consumer_group, 消息 channel [(partition, record)])
    subscriptions: RwLock<HashMap<String, Vec<SubscriberEntry>>>,
    /// ISR 复制管理器（None = 单 agent 本地语义，零复制路径保留）
    replication: RwLock<Option<Arc<crate::services::replication::ReplicationManager>>>,
    /// 自身 Arc 弱引用（spawn_blocking 升级用，见 bind_self_weak）
    self_arc: RwLock<Option<std::sync::Weak<MessageQueueService>>>,
    /// 幂等条目机会式清扫计数器（每 `IDEM_PRUNE_EVERY` 次触发一次，
    /// 避免每次生产都 O(n) 扫全表）
    idem_prune_tick: std::sync::atomic::AtomicU64,
}

impl std::fmt::Debug for MessageQueueService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageQueueService")
            .field("db_path", &self.db_path)
            .field("started", &self.started)
            // 如实报出"配了多少、有没有生效"：该上限**未被执行**（与
            // `CacheService::max_size_bytes` 同一取舍，第四轮 §3.10 h/i）。
            .field("max_size_bytes", &self.max_size_bytes)
            .field("max_size_enforced", &false)
            .finish()
    }
}

impl MessageQueueService {
    pub fn new(db_path: PathBuf, max_size_bytes: u64) -> Self {
        if max_size_bytes > 0 {
            tracing::warn!(
                max_size_bytes,
                "MessageQueueService: configured size limit is NOT enforced (no eviction \
                 implemented); growth is bounded only by message consumption and disk space"
            );
        }
        Self {
            db_path,
            db: RwLock::new(None),
            started: RwLock::new(false),
            max_size_bytes,
            subscriptions: RwLock::new(HashMap::new()),
            replication: RwLock::new(None),
            self_arc: RwLock::new(None),
            idem_prune_tick: std::sync::atomic::AtomicU64::new(0),
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
    /// 重试"会给下游多一条消息（F-57）。
    pub fn produce(
        &self,
        topic: &str,
        partition: u32,
        payload: Vec<u8>,
        headers: Option<BTreeMap<String, String>>,
    ) -> ServiceResult<u64> {
        self.produce_idempotent(topic, partition, payload, headers, None)
    }

    /// 生产一条消息，可按 `idempotency_key` 去重（F-57 / V9）。
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
        use std::sync::atomic::Ordering;
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
        // Read the message first
        let mk = encode_msg_key(topic, partition, offset);
        let rtx = self.read_tx()?;
        let raw = {
            let table = rtx.open_table(MESSAGE_TABLE)?;
            match table.get(mk.as_slice())? {
                Some(v) => v.value().to_vec(),
                None => {
                    return Err(format!("message {topic}/{partition}/{offset} not found").into())
                }
            }
        };
        drop(rtx);

        // Decode payload
        let payload = match decode_message(&raw) {
            Some((p, _, _)) => p,
            None => return Err("failed to decode message".into()),
        };

        let dlq_encoded = encode_dlq_message(&payload, reason, detail);
        let dk = encode_dlq_key(topic, partition, offset);

        let wtx = self.write_tx()?;
        // Delete from main message table
        {
            let mut table = wtx.open_table(MESSAGE_TABLE)?;
            table.remove(mk.as_slice())?;
        }
        // Insert into DLQ
        {
            let mut table = wtx.open_table(DLQ_TABLE)?;
            table.insert(dk.as_slice(), dlq_encoded.as_slice())?;
        }
        wtx.commit()?;
        Ok(())
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

        let (total_messages, total_bytes) = {
            let table = rtx.open_table(MESSAGE_TABLE)?;
            let mut count = 0u64;
            let mut bytes = 0u64;
            for item in table.iter()? {
                let (_, raw) = item?;
                count += 1;
                bytes += raw.value().len() as u64;
            }
            (count, bytes)
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

        let wtx = self.write_tx()?;
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

            // 生产者级幂等索引：与 offset 分配 / 消息写入**同事务**（F-57）
            if let Some(ik) = idem_key {
                let bytes = serde_json::to_vec(&IdempotencyEntry {
                    offset,
                    ts_ms: now_ms,
                })?;
                let mut t = wtx.open_table(IDEMPOTENCY_TABLE)?;
                t.insert(ik, bytes.as_slice())?;
            }

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
        // 因此消息 key / headers 在复制路径上**无法传播**。此前 `_headers`
        // 被直接丢弃（调用方以为写入成功）—— 现改为明确报错。
        if headers.as_ref().is_some_and(|h| !h.is_empty()) {
            return Err("mq: message headers (e.g. key) are not carried by the ISR \
                        replication entry; refusing to drop them silently — use \
                        services.replication=false, or omit the key"
                .into());
        }

        // 生产者级幂等（F-57 / V9）：与本地路径共用同一张表、同一语义。
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
        }
        wtx.commit()?;

        *self.db.write() = Some(db);
        *self.started.write() = true;
        tracing::info!("MessageQueueService started: db_path={}", db_path.display());
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
}
