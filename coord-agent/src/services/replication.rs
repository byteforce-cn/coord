// coord-agent: 跨 Agent 数据复制 (Push/Reconcile/ISR)
//
// 实现 v8.2 §4.7-4.8 定义的同步复制协议:
// - Push 复制: Leader 写入后推送到 ISR Followers
// - Reconcile 恢复: Follower 重启后从 Leader 拉取缺失数据
// - ISR 管理: 跟踪同步副本集，检测降级
// - 幂等键: 防止重复写入
//
// 参见 docs/client-agent-architecture.v8.2.md §4.7-4.8。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::RwLock;

// ──── ReplicationConfig ────

/// 复制配置
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReplicationConfig {
    /// 最小同步副本数 (默认 2)
    #[serde(default = "default_min_isr")]
    pub min_isr: usize,
    /// 同步确认超时 (毫秒, 默认 2000)
    #[serde(default = "default_sync_timeout_ms")]
    pub sync_timeout_ms: u64,
}

fn default_min_isr() -> usize { 2 }
fn default_sync_timeout_ms() -> u64 { 2000 }

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            min_isr: default_min_isr(),
            sync_timeout_ms: default_sync_timeout_ms(),
        }
    }
}

impl ReplicationConfig {
    /// 验证配置合法性
    pub fn validate(&self) -> Result<(), String> {
        if self.min_isr == 0 {
            return Err("min_isr must be at least 1".to_string());
        }
        Ok(())
    }
}

// ──── IdempotencyKey ────

/// 幂等键: 唯一标识一次写操作，用于防止重复应用
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct IdempotencyKey {
    /// 操作标识符 (如 "cache:put:user:123" 或 "mq:publish:orders:0:42")
    pub key: String,
    /// 创建时间戳 (毫秒)
    pub timestamp_ms: u64,
}

impl IdempotencyKey {
    /// 创建新的幂等键
    pub fn new(key: impl Into<String>, timestamp_ms: u64) -> Self {
        Self { key: key.into(), timestamp_ms }
    }

    /// 序列化为字符串
    pub fn to_string(&self) -> String {
        format!("{}:{}", self.key, self.timestamp_ms)
    }
}

impl std::fmt::Display for IdempotencyKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.key, self.timestamp_ms)
    }
}

// ──── ReplicationOp ────

/// 复制操作类型
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ReplicationOp {
    /// 缓存写入
    CachePut {
        key: Vec<u8>,
        value: Vec<u8>,
        data_type: String,
    },
    /// 缓存删除
    CacheDelete {
        key: Vec<u8>,
        data_type: String,
    },
    /// 消息发布
    MqPublish {
        topic: String,
        partition: u32,
        payload: Vec<u8>,
    },
}

// ──── ReplicationEntry ────

/// 复制条目: 单次写操作的完整记录
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReplicationEntry {
    /// 幂等键
    pub idempotency_key: IdempotencyKey,
    /// 分片标识
    pub shard_id: String,
    /// 单调递增序列号
    pub sequence_num: u64,
    /// 操作内容
    pub operation: ReplicationOp,
}

impl ReplicationEntry {
    /// 创建缓存写入复制条目
    pub fn new_cache_put(
        idempotency_key: IdempotencyKey,
        shard_id: String,
        key: Vec<u8>,
        value: Vec<u8>,
        data_type: String,
        sequence_num: u64,
    ) -> Self {
        Self {
            idempotency_key,
            shard_id,
            sequence_num,
            operation: ReplicationOp::CachePut { key, value, data_type },
        }
    }

    /// 创建缓存删除复制条目
    pub fn new_cache_delete(
        idempotency_key: IdempotencyKey,
        shard_id: String,
        key: Vec<u8>,
        data_type: String,
        sequence_num: u64,
    ) -> Self {
        Self {
            idempotency_key,
            shard_id,
            sequence_num,
            operation: ReplicationOp::CacheDelete { key, data_type },
        }
    }

    /// 创建消息发布复制条目
    pub fn new_mq_publish(
        idempotency_key: IdempotencyKey,
        shard_id: String,
        topic: String,
        partition: u32,
        payload: Vec<u8>,
        sequence_num: u64,
    ) -> Self {
        Self {
            idempotency_key,
            shard_id,
            sequence_num,
            operation: ReplicationOp::MqPublish { topic, partition, payload },
        }
    }
}

// ──── ReplicationState ────

/// 复制状态: 管理 ISR 和序列号
#[derive(Debug)]
pub struct ReplicationState {
    /// 最小同步副本数
    min_isr: usize,
    /// 当前 ISR 集合 (agent 地址)
    isr: HashSet<String>,
    /// 最后应用的序列号
    last_sequence: u64,
    /// 每个 Follower 的 ack 位置
    follower_positions: HashMap<String, u64>,
}

impl ReplicationState {
    /// 创建新的复制状态
    pub fn new(min_isr: usize) -> Self {
        Self {
            min_isr,
            isr: HashSet::new(),
            last_sequence: 0,
            follower_positions: HashMap::new(),
        }
    }

    /// 添加 Agent 到 ISR
    pub fn add_to_isr(&mut self, agent_addr: String) {
        self.isr.insert(agent_addr);
    }

    /// 从 ISR 移除 Agent
    pub fn remove_from_isr(&mut self, agent_addr: &str) {
        self.isr.remove(agent_addr);
        self.follower_positions.remove(agent_addr);
    }

    /// 检查 Agent 是否在 ISR 中
    pub fn is_in_sync(&self, agent_addr: &str) -> bool {
        self.isr.contains(agent_addr)
    }

    /// ISR 大小
    pub fn isr_size(&self) -> usize {
        self.isr.len()
    }

    /// 是否处于降级模式 (ISR < min_isr)
    pub fn is_degraded(&self) -> bool {
        self.isr.len() < self.min_isr
    }

    /// 是否健康 (ISR >= min_isr)
    pub fn is_healthy(&self) -> bool {
        !self.is_degraded()
    }

    /// 是否为单副本模式
    pub fn is_single_replica(&self) -> bool {
        self.min_isr == 1 && self.isr.len() == 1
    }

    /// 最后应用的序列号
    pub fn last_sequence(&self) -> u64 {
        self.last_sequence
    }

    /// 更新最后序列号
    fn advance_sequence(&mut self, seq: u64) {
        if seq > self.last_sequence {
            self.last_sequence = seq;
        }
    }

    /// 获取 ISR 迭代器
    pub fn isr_iter(&self) -> impl Iterator<Item = &String> {
        self.isr.iter()
    }
}

// ──── IdempotencyGuard ────

/// 幂等保护: 基于 LRU 的已应用幂等键追踪
#[derive(Debug)]
pub struct IdempotencyGuard {
    /// 已应用的幂等键 (key → ())
    keys: lru::LruCache<String, ()>,
}

impl IdempotencyGuard {
    /// 创建幂等保护，指定容量
    pub fn new(capacity: usize) -> Self {
        Self {
            keys: lru::LruCache::new(std::num::NonZeroUsize::new(capacity.max(1)).unwrap()),
        }
    }

    /// 检查并记录幂等键
    /// 返回 true 表示新键（可安全应用），false 表示重复（应跳过）
    pub fn check_and_record(&mut self, key: &IdempotencyKey) -> bool {
        let key_str = key.to_string();
        if self.keys.contains(&key_str) {
            false
        } else {
            self.keys.put(key_str, ());
            true
        }
    }

    /// 当前记录数
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

// ──── ReplicationManager ────

/// 复制管理器: 协调整体复制流程
#[derive(Debug)]
pub struct ReplicationManager {
    /// 当前 Agent 地址
    agent_addr: String,
    /// 复制配置
    config: ReplicationConfig,
    /// 复制状态 (ISR + 序列号)
    state: Arc<RwLock<ReplicationState>>,
    /// 幂等保护
    idempotency_guard: Arc<RwLock<IdempotencyGuard>>,
}

impl ReplicationManager {
    /// 创建复制管理器
    pub fn new(config: ReplicationConfig, agent_addr: String) -> Self {
        let guard_capacity = 10000; // 默认缓存 10000 个最近幂等键
        let min_isr = config.min_isr;
        Self {
            agent_addr,
            config,
            state: Arc::new(RwLock::new(ReplicationState::new(min_isr))),
            idempotency_guard: Arc::new(RwLock::new(IdempotencyGuard::new(guard_capacity))),
        }
    }

    /// 获取当前 Agent 地址
    pub fn agent_addr(&self) -> &str {
        &self.agent_addr
    }

    /// 获取复制状态 (只读)
    pub fn state(&self) -> parking_lot::RwLockReadGuard<'_, ReplicationState> {
        self.state.read()
    }

    /// 获取复制状态 (可写)
    pub fn state_mut(&self) -> parking_lot::RwLockWriteGuard<'_, ReplicationState> {
        self.state.write()
    }

    /// 添加副本到 ISR
    pub fn add_replica(&self, agent_addr: String) {
        self.state.write().add_to_isr(agent_addr);
    }

    /// 本地提交 (单副本或 Leader 本地写入)
    /// 仅在 ISR 满足 min_isr 或单副本模式下成功
    pub fn try_commit_local(&self, entry: ReplicationEntry) -> Result<(), ReplicationError> {
        let mut guard = self.idempotency_guard.write();

        // 幂等检查
        if !guard.check_and_record(&entry.idempotency_key) {
            return Err(ReplicationError::DuplicateIdempotencyKey {
                key: entry.idempotency_key.to_string(),
            });
        }

        // 更新序列号
        let mut state = self.state.write();
        state.advance_sequence(entry.sequence_num);

        Ok(())
    }

    /// 接收来自 Leader 的 Push 条目 (Follower 端)
    pub fn receive_push(&self, entry: ReplicationEntry) -> Result<(), ReplicationError> {
        let mut guard = self.idempotency_guard.write();

        // 幂等检查
        if !guard.check_and_record(&entry.idempotency_key) {
            return Err(ReplicationError::DuplicateIdempotencyKey {
                key: entry.idempotency_key.to_string(),
            });
        }

        // 更新序列号
        let mut state = self.state.write();
        state.advance_sequence(entry.sequence_num);

        Ok(())
    }

    /// 获取配置
    pub fn config(&self) -> &ReplicationConfig {
        &self.config
    }
}

// ──── ReconcileState ────

/// Reconcile 状态: 跟踪 Follower 追赶 Leader 的进度
#[derive(Debug, Clone)]
pub struct ReconcileState {
    /// Follower Agent 地址
    agent_addr: String,
    /// 分片标识
    shard_id: String,
    /// Follower 本地序列号
    local_sequence: u64,
    /// Leader 当前序列号
    leader_sequence: u64,
}

impl ReconcileState {
    /// 创建 Reconcile 状态
    pub fn new(agent_addr: String, shard_id: String) -> Self {
        Self {
            agent_addr,
            shard_id,
            local_sequence: 0,
            leader_sequence: 0,
        }
    }

    /// Agent 地址
    pub fn agent_addr(&self) -> &str {
        &self.agent_addr
    }

    /// 分片标识
    pub fn shard_id(&self) -> &str {
        &self.shard_id
    }

    /// 本地序列号
    pub fn local_sequence(&self) -> u64 {
        self.local_sequence
    }

    /// 设置本地序列号
    pub fn set_local_sequence(&mut self, seq: u64) {
        self.local_sequence = seq;
    }

    /// 设置 Leader 序列号
    pub fn set_leader_sequence(&mut self, seq: u64) {
        self.leader_sequence = seq;
    }

    /// 缺失起始序列号 (如果已追上返回 0)
    pub fn missing_since_seq(&self) -> u64 {
        if self.local_sequence >= self.leader_sequence {
            0
        } else {
            self.local_sequence + 1
        }
    }

    /// 计算缺失的序列号范围
    /// 返回 Some((start, end)) 如果有缺失，None 表示已追上
    pub fn compute_missing_range(&self) -> Option<(u64, u64)> {
        if self.local_sequence >= self.leader_sequence {
            None
        } else {
            Some((self.local_sequence + 1, self.leader_sequence))
        }
    }

    /// 缺失条目数
    pub fn missing_count(&self) -> u64 {
        if self.local_sequence >= self.leader_sequence {
            0
        } else {
            self.leader_sequence - self.local_sequence
        }
    }

    /// 标记已应用指定序列号
    pub fn mark_applied(&mut self, seq: u64) {
        if seq > self.local_sequence {
            self.local_sequence = seq;
        }
    }
}

// ──── ReplicationError ────

/// 复制错误
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReplicationError {
    #[error("duplicate idempotency key: {key}")]
    DuplicateIdempotencyKey { key: String },

    #[error("ISR degraded: need {required} replicas, have {actual}")]
    IsrDegraded { required: usize, actual: usize },

    #[error("sync timeout after {timeout_ms}ms")]
    SyncTimeout { timeout_ms: u64 },

    #[error("replication not configured for this operation")]
    NotConfigured,
}
