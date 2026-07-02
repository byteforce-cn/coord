// coord-agent: 消息队列 (MQ Service) — 数据面（Phase F）
//
// 实现 BaseService trait，基于 redb 提供本地分段日志消息队列。
// 支持 Topic/Partition/ConsumerGroup/DeadLetterQueue。
//
// 架构（v3.0）:
// - Agent 本地持久化日志（redb 分段日志）
// - Server 管理 Topic 配置、分区分配表、消费组偏移快照
// - Agent 作为分区 Leader 负责写入和 ISR 复制
// - 消费者长轮询拉取
//
// 参见 docs/client-agent-architecture-v3.md §5.6。

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use parking_lot::RwLock;
use redb::{ReadableDatabase, ReadableTable};

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

const TOPIC_TABLE: redb::TableDefinition<&str, &[u8]> =
    redb::TableDefinition::new("mq:topics");
// Messages: key = [topic_len:u32][topic_bytes][partition:u32][offset:u64 BE]
const MESSAGE_TABLE: redb::TableDefinition<&[u8], &[u8]> =
    redb::TableDefinition::new("mq:messages");
// Consumer offsets: key = [group_len:u32][group_bytes][topic_len:u32][topic_bytes][partition:u32]
const OFFSET_TABLE: redb::TableDefinition<&[u8], u64> =
    redb::TableDefinition::new("mq:offsets");
// DLQ: key = [topic_len:u32][topic_bytes][partition:u32][offset:u64 BE]
const DLQ_TABLE: redb::TableDefinition<&[u8], &[u8]> =
    redb::TableDefinition::new("mq:dlq");
// Next offset counter: key = [topic_len:u32][topic_bytes][partition:u32]
const NEXT_OFFSET_TABLE: redb::TableDefinition<&[u8], u64> =
    redb::TableDefinition::new("mq:next_offset");

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
    if raw.len() < 12 { return None; }
    let timestamp = u64::from_be_bytes(raw[..8].try_into().ok()?);
    let headers_len = u32::from_be_bytes(raw[8..12].try_into().ok()?) as usize;
    if raw.len() < 12 + headers_len { return None; }
    let headers: BTreeMap<String, String> = serde_json::from_slice(&raw[12..12 + headers_len]).unwrap_or_default();
    let payload = raw[12 + headers_len..].to_vec();
    Some((payload, timestamp, headers))
}

/// Encode DLQ message: [timestamp:u64][reason_len:u32][reason][detail_len:u32][detail][payload]
fn encode_dlq_message(payload: &[u8], reason: &str, detail: &str) -> Vec<u8> {
    let reason_bytes = reason.as_bytes();
    let detail_bytes = detail.as_bytes();
    let mut v = Vec::with_capacity(8 + 4 + reason_bytes.len() + 4 + detail_bytes.len() + payload.len());
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
    if raw.len() < 16 { return None; }
    let timestamp = u64::from_be_bytes(raw[..8].try_into().ok()?);
    let reason_len = u32::from_be_bytes(raw[8..12].try_into().ok()?) as usize;
    if raw.len() < 12 + reason_len + 4 { return None; }
    let reason = String::from_utf8_lossy(&raw[12..12 + reason_len]).to_string();
    let detail_len_start = 12 + reason_len;
    let detail_len = u32::from_be_bytes(raw[detail_len_start..detail_len_start + 4].try_into().ok()?) as usize;
    if raw.len() < detail_len_start + 4 + detail_len { return None; }
    let detail = String::from_utf8_lossy(&raw[detail_len_start + 4..detail_len_start + 4 + detail_len]).to_string();
    let payload = raw[detail_len_start + 4 + detail_len..].to_vec();
    Some((payload, timestamp, reason, detail))
}

// ──── MessageQueueService ────

/// 消息队列服务（数据面）
///
/// 基于 redb 的本地持久化分段日志 MQ。
pub struct MessageQueueService {
    db_path: PathBuf,
    db: RwLock<Option<redb::Database>>,
    started: RwLock<bool>,
    #[allow(dead_code)]
    max_size_bytes: u64,
}

impl std::fmt::Debug for MessageQueueService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageQueueService")
            .field("db_path", &self.db_path)
            .field("started", &self.started)
            .finish()
    }
}

impl MessageQueueService {
    pub fn new(db_path: PathBuf, max_size_bytes: u64) -> Self {
        Self {
            db_path,
            db: RwLock::new(None),
            started: RwLock::new(false),
            max_size_bytes,
        }
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

    pub fn produce(&self, topic: &str, partition: u32, payload: Vec<u8>, headers: Option<BTreeMap<String, String>>) -> ServiceResult<u64> {
        let config = self.get_topic_config(topic)?
            .ok_or_else(|| format!("topic '{topic}' not found"))?;

        if partition >= config.partitions {
            return Err(format!("partition {partition} out of range for topic '{topic}' (max {})", config.partitions).into());
        }

        if payload.len() as u64 > config.max_message_size {
            return Err(format!("message size {} exceeds max {}", payload.len(), config.max_message_size).into());
        }

        let headers = headers.unwrap_or_default();
        let encoded = encode_message(&payload, &headers);

        let next_key = encode_next_offset_key(topic, partition);
        let msg_key_prefix = msg_key_prefix(topic, partition);

        let wtx = self.write_tx()?;
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
        wtx.commit()?;

        let _ = (msg_key_prefix, next_key); // silence unused warnings
        Ok(offset)
    }

    /// 消费消息：从指定 offset 开始读取最多 max_count 条
    pub fn consume(&self, topic: &str, partition: u32, start_offset: u64, max_count: u64) -> ServiceResult<Vec<MessageRecord>> {
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
                if !k.starts_with(&prefix) { break; }
                // Extract offset from key (last 8 bytes)
                if k.len() < prefix_len + 8 { continue; }
                let offset = u64::from_be_bytes(k[prefix_len..prefix_len + 8].try_into().unwrap());
                if offset < start_offset { continue; }
                if records.len() as u64 >= max_count { break; }

                if let Some((payload, timestamp, headers)) = decode_message(raw.value()) {
                    records.push(MessageRecord { offset, payload, timestamp, headers });
                }
            }
        }
        Ok(records)
    }

    // ──── Consumer Group 偏移管理 ────

    pub fn commit_offset(&self, group: &str, topic: &str, partition: u32, offset: u64) -> ServiceResult<()> {
        let key = encode_offset_key(group, topic, partition);
        let wtx = self.write_tx()?;
        {
            let mut table = wtx.open_table(OFFSET_TABLE)?;
            table.insert(key.as_slice(), offset)?;
        }
        wtx.commit()?;
        Ok(())
    }

    pub fn get_consumer_offset(&self, group: &str, topic: &str, partition: u32) -> ServiceResult<u64> {
        let key = encode_offset_key(group, topic, partition);
        let rtx = self.read_tx()?;
        let table = rtx.open_table(OFFSET_TABLE)?;
        match table.get(key.as_slice())? {
            Some(v) => Ok(v.value()),
            None => Ok(0),
        }
    }

    // ──── 死信队列 (DLQ) ────

    pub fn move_to_dlq(&self, topic: &str, partition: u32, offset: u64, reason: &str, detail: &str) -> ServiceResult<()> {
        // Read the message first
        let mk = encode_msg_key(topic, partition, offset);
        let rtx = self.read_tx()?;
        let raw = {
            let table = rtx.open_table(MESSAGE_TABLE)?;
            match table.get(mk.as_slice())? {
                Some(v) => v.value().to_vec(),
                None => return Err(format!("message {topic}/{partition}/{offset} not found").into()),
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

    pub fn consume_dlq(&self, topic: &str, partition: u32, max_count: u64) -> ServiceResult<Vec<DlqRecord>> {
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
                if !k.starts_with(&prefix) { break; }
                if k.len() < prefix_len + 8 { continue; }
                let offset = u64::from_be_bytes(k[prefix_len..prefix_len + 8].try_into().unwrap());
                if records.len() as u64 >= max_count { break; }

                if let Some((payload, timestamp, reason, detail)) = decode_dlq_message(raw.value()) {
                    records.push(DlqRecord {
                        offset,
                        payload,
                        timestamp,
                        error_reason: if reason.is_empty() { None } else { Some(reason) },
                        error_detail: if detail.is_empty() { None } else { Some(detail) },
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
        svc.create_topic("t1", TopicConfig { partitions: 1, retention_secs: 60, max_message_size: 1024 }).unwrap();
        assert_eq!(svc.list_topics().unwrap().len(), 1);
    }

    #[test]
    fn test_produce_consume_basic() {
        let dir = temp_dir();
        let svc = new_svc(&dir);
        svc.create_topic("test", TopicConfig { partitions: 1, retention_secs: 3600, max_message_size: 1024 }).unwrap();
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
        svc.create_topic("test", TopicConfig { partitions: 1, retention_secs: 3600, max_message_size: 1024 }).unwrap();
        svc.produce("test", 0, b"m1".to_vec(), None).unwrap();
        svc.produce("test", 0, b"m2".to_vec(), None).unwrap();
        svc.commit_offset("g1", "test", 0, 1).unwrap();
        assert_eq!(svc.get_consumer_offset("g1", "test", 0).unwrap(), 1);
    }

    #[test]
    fn test_dlq() {
        let dir = temp_dir();
        let svc = new_svc(&dir);
        svc.create_topic("test", TopicConfig { partitions: 1, retention_secs: 3600, max_message_size: 1024 }).unwrap();
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
            svc.create_topic("p", TopicConfig { partitions: 1, retention_secs: 60, max_message_size: 1024 }).unwrap();
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
}
