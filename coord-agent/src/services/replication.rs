// coord-agent: 跨 Agent 数据复制 (Push/Reconcile/ISR) — v2.1 已落地
//
// ✅ 状态声明（v2.1，2026-08-08）：ISR 复制**已实现并落地**。
// - 复制日志与幂等键持久化位于各数据面服务（MQ/Cache）的 redb 内，与数据写同事务；
// - 分区 Leader 静态分配（min-addr，Q1），Leader 独占分配 MQ offset 并广播（C1）；
// - 同步复制：写操作在 min_isr 副本 ack 后才对客户端成功；
// - 幂等：重复复制条目不重复应用（持久化幂等键 + 内存 LRU，Q2）；
// - 顺序：按序列号单调应用；Reconcile 恢复落后 Follower；ISR 降级拒绝写（进入降级）。
// - 单 agent 部署 min_isr 自动降级为 1（C6），现有部署零破坏。
//
// 落地记录与决策见 docs/cache-mq-isr-evaluation.md（v2.1）。
// 协议（v8.2 §4.7-4.8）:
// - Push 复制: Leader 写入后推送到 ISR Followers，等待确认
// - Reconcile 恢复: Follower 重启/落后后从 Leader 拉取缺失数据
// - ISR 管理: 跟踪同步副本集，检测降级（IsrHeartbeat 双向心跳）
// - 幂等键: 防止重复写入（持久化 + LRU）
//
// 参见 docs/client-agent-architecture-v3.md §4.7-4.8。

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
    /// 消息发布（offset 由分区 Leader 独占分配并广播，决策 C1）
    MqPublish {
        topic: String,
        partition: u32,
        /// Leader 分配的全局一致 offset
        offset: u64,
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

    /// 创建消息发布复制条目（offset 由 Leader 分配，决策 C1）
    pub fn new_mq_publish(
        idempotency_key: IdempotencyKey,
        shard_id: String,
        topic: String,
        partition: u32,
        offset: u64,
        payload: Vec<u8>,
        sequence_num: u64,
    ) -> Self {
        Self {
            idempotency_key,
            shard_id,
            sequence_num,
            operation: ReplicationOp::MqPublish { topic, partition, offset, payload },
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

/// 复制管理器: 协调跨 Agent 复制流程（ISR 成员 / 分区 Leader / 推送 / 心跳 / Reconcile）
///
/// 职责边界（v2.1 已落地）：
/// - 复制日志与幂等键的持久化位于各数据面服务（MQ/Cache）的 redb 内，
///   与数据写同事务提交（Phase A 收敛决策：NEXT_OFFSET_TABLE 与复制条目同事务持久化）；
/// - 本管理器负责 ISR 成员发现、静态分区 Leader 分配（min-addr）、
///   向 Followers 推送复制条目并等待确认（同步复制）、心跳维护与 Reconcile 追赶。
#[derive(Debug)]
pub struct ReplicationManager {
    /// 当前 Agent 地址
    agent_addr: String,
    /// 复制配置
    config: ReplicationConfig,
    /// 复制状态 (ISR 成员 + 全局序列号记账)
    state: Arc<RwLock<ReplicationState>>,
    /// 幂等保护（内存 LRU，与数据面持久化幂等键互补）
    idempotency_guard: Arc<RwLock<IdempotencyGuard>>,
    /// 已知对端 agent（不含自身）：ISR 成员发现（Registry / 静态配置）
    peers: RwLock<HashSet<String>>,
    /// 分区 Leader 显式分配覆盖（空 = 自动 min-addr 静态分配，Q1）
    shard_leaders: RwLock<HashMap<String, String>>,
    /// 对端复制客户端缓存
    clients: RwLock<HashMap<String, ReplicaClient>>,
    /// 心跳间隔（毫秒）
    heartbeat_interval_ms: u64,
    /// 心跳后台任务句柄
    heartbeat_task: RwLock<Option<tokio::task::JoinHandle<()>>>,
    /// 心跳停止标志（start_heartbeat 写入）
    heartbeat_stop: RwLock<Option<Arc<AtomicBool>>>,
}

impl ReplicationManager {
    /// 创建复制管理器（自身自动加入 ISR）
    pub fn new(config: ReplicationConfig, agent_addr: String) -> Self {
        let guard_capacity = 10000; // 默认缓存 10000 个最近幂等键
        let min_isr = config.min_isr;
        let mut state = ReplicationState::new(min_isr);
        state.add_to_isr(agent_addr.clone()); // 自身始终在 ISR
        Self {
            agent_addr,
            config,
            state: Arc::new(RwLock::new(state)),
            idempotency_guard: Arc::new(RwLock::new(IdempotencyGuard::new(guard_capacity))),
            peers: RwLock::new(HashSet::new()),
            shard_leaders: RwLock::new(HashMap::new()),
            clients: RwLock::new(HashMap::new()),
            heartbeat_interval_ms: 2000,
            heartbeat_task: RwLock::new(None),
            heartbeat_stop: RwLock::new(None),
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

    #[error("not leader for shard {shard} (leader is {leader})")]
    NotLeader { shard: String, leader: String },

    #[error("store error: {0}")]
    Store(String),

    #[error("network error: {0}")]
    Network(String),

    #[error("follower apply error: {0}")]
    FollowerApply(String),
}

// ──── ReplicatedStore（数据面复制存储接口）────

/// 数据面服务（MQ / Cache）实现的复制存储接口。
///
/// 复制日志、持久化幂等键与本地序列号由数据面服务在自己的 redb 内维护，
/// 与数据写同事务提交；本接口供 Replica gRPC 服务与 ReplicationManager
/// （心跳落后检测 / Reconcile 追赶）调用。
pub trait ReplicatedStore: Send + Sync {
    /// 本 agent 负责的 shard 列表（MQ: "mq:{topic}"；Cache: "cache"）
    fn shards(&self) -> Vec<String>;
    /// 某 shard 最后已应用序列号
    fn last_local_sequence(&self, shard: &str) -> u64;
    /// 应用一条来自 Leader 的复制条目（幂等：重复条目返回 Ok）
    fn apply_entry(&self, entry: &ReplicationEntry) -> Result<(), ReplicationError>;
    /// 读取复制日志 [from_seq, ...) 的条目（Reconcile 服务端）
    fn read_entries(&self, shard: &str, from_seq: u64, limit: u64) -> Vec<ReplicationEntry>;
}

// ──── Proto 转换 ────

use coord_proto::agent::{
    replica_op, ReplicaCacheDelete, ReplicaCachePut, ReplicaEntry as ReplicaEntryProto,
    ReplicaMqPublish, ReplicaShardProgress,
};

impl ReplicationEntry {
    /// 序列化为 proto ReplicaEntry
    pub fn to_proto(&self) -> ReplicaEntryProto {
        let operation = Some(coord_proto::agent::ReplicaOp {
            op: Some(match &self.operation {
                ReplicationOp::CachePut { key, value, data_type } => {
                    replica_op::Op::CachePut(ReplicaCachePut {
                        key: key.clone(),
                        value: value.clone(),
                        data_type: data_type.clone(),
                    })
                }
                ReplicationOp::CacheDelete { key, data_type } => {
                    replica_op::Op::CacheDelete(ReplicaCacheDelete {
                        key: key.clone(),
                        data_type: data_type.clone(),
                    })
                }
                ReplicationOp::MqPublish { topic, partition, offset, payload } => {
                    replica_op::Op::MqPublish(ReplicaMqPublish {
                        topic: topic.clone(),
                        partition: *partition,
                        offset: *offset,
                        payload: payload.clone(),
                    })
                }
            }),
        });
        ReplicaEntryProto {
            idempotency_key: self.idempotency_key.key.clone(),
            idempotency_timestamp_ms: self.idempotency_key.timestamp_ms,
            shard_id: self.shard_id.clone(),
            sequence_num: self.sequence_num,
            operation,
        }
    }

    /// 从 proto ReplicaEntry 反序列化
    pub fn from_proto(p: &ReplicaEntryProto) -> Result<Self, ReplicationError> {
        let op = p.operation.as_ref().ok_or_else(|| {
            ReplicationError::Network("replica entry missing operation".to_string())
        })?;
        let operation = match op.op.as_ref().ok_or_else(|| {
            ReplicationError::Network("replica operation missing variant".to_string())
        })? {
            replica_op::Op::CachePut(x) => ReplicationOp::CachePut {
                key: x.key.clone(),
                value: x.value.clone(),
                data_type: x.data_type.clone(),
            },
            replica_op::Op::CacheDelete(x) => ReplicationOp::CacheDelete {
                key: x.key.clone(),
                data_type: x.data_type.clone(),
            },
            replica_op::Op::MqPublish(x) => ReplicationOp::MqPublish {
                topic: x.topic.clone(),
                partition: x.partition,
                offset: x.offset,
                payload: x.payload.clone(),
            },
        };
        Ok(Self {
            idempotency_key: IdempotencyKey::new(p.idempotency_key.clone(), p.idempotency_timestamp_ms),
            shard_id: p.shard_id.clone(),
            sequence_num: p.sequence_num,
            operation,
        })
    }
}

// ──── ReplicaClient（Agent↔Agent 复制客户端）────

/// Agent↔Agent 复制客户端（tonic）
#[derive(Clone)]
pub struct ReplicaClient {
    inner: coord_proto::agent::replica_client::ReplicaClient<tonic::transport::Channel>,
}

impl std::fmt::Debug for ReplicaClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicaClient").finish_non_exhaustive()
    }
}

impl ReplicaClient {
    /// 连接对端 agent 的 Replica 服务
    pub async fn connect(addr: &str) -> Result<Self, ReplicationError> {
        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .map_err(|e| ReplicationError::Network(format!("invalid endpoint {addr}: {e}")))?
            .connect_timeout(Duration::from_secs(2));
        let channel = endpoint.connect().await.map_err(|e| {
            ReplicationError::Network(format!("connect {addr}: {e}"))
        })?;
        Ok(Self {
            inner: coord_proto::agent::replica_client::ReplicaClient::new(channel),
        })
    }

    /// 推送一条复制条目（幂等；重复条目视为成功）
    pub async fn apply(&self, entry: &ReplicationEntry) -> Result<(), ReplicationError> {
        let req = coord_proto::agent::ReplicaApplyRequest {
            entry: Some(entry.to_proto()),
        };
        let resp = self
            .inner
            .clone()
            .apply(req)
            .await
            .map_err(|e| ReplicationError::Network(format!("apply rpc: {e}")))?;
        let resp = resp.into_inner();
        if resp.applied {
            Ok(())
        } else {
            Err(ReplicationError::FollowerApply(
                format!("follower rejected entry seq={}", entry.sequence_num),
            ))
        }
    }

    /// 从 Leader 拉取缺失序列号区间的复制条目（Reconcile 客户端）
    pub async fn reconcile(
        &self,
        shard: &str,
        from_seq: u64,
        limit: u64,
    ) -> Result<Vec<ReplicationEntry>, ReplicationError> {
        let req = coord_proto::agent::ReplicaReconcileRequest {
            shard_id: shard.to_string(),
            start_sequence: from_seq,
            limit,
        };
        let mut stream = self
            .inner
            .clone()
            .reconcile(req)
            .await
            .map_err(|e| ReplicationError::Network(format!("reconcile rpc: {e}")))?
            .into_inner();
        let mut out = Vec::new();
        while let Some(item) = stream
            .message()
            .await
            .map_err(|e| ReplicationError::Network(format!("reconcile stream: {e}")))?
        {
            if let Ok(entry) = ReplicationEntry::from_proto(&item) {
                out.push(entry);
            }
        }
        Ok(out)
    }

    /// 发送 IsrHeartbeat（维护 ISR 成员 + 交换各 shard 最后序列号）
    pub async fn heartbeat(
        &self,
        agent_addr: &str,
        progress: &[ReplicaShardProgress],
    ) -> Result<coord_proto::agent::ReplicaHeartbeatResponse, ReplicationError> {
        let req = coord_proto::agent::ReplicaHeartbeatRequest {
            agent_addr: agent_addr.to_string(),
            progress: progress.to_vec(),
        };
        self.inner
            .clone()
            .isr_heartbeat(req)
            .await
            .map(|r| r.into_inner())
            .map_err(|e| ReplicationError::Network(format!("heartbeat rpc: {e}")))
    }
}

// ──── ReplicationManager: ISR 成员发现 / Leader 分配 / 推送 / 心跳 / Reconcile ────

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

impl ReplicationManager {
    // ──── ISR 成员发现 ────

    /// 添加对端 agent（ISR 候选；忽略自身）
    pub fn add_peer(&self, addr: String) {
        if addr == self.agent_addr {
            return;
        }
        self.peers.write().insert(addr.clone());
        self.state.write().add_to_isr(addr.clone());
        tracing::info!("replication peer added: {addr}");
    }

    /// 批量设置对端（幂等；移除已不存在的对端）
    pub fn set_peers(&self, addrs: Vec<String>) {
        let current: HashSet<String> = addrs
            .into_iter()
            .filter(|a| a != &self.agent_addr)
            .collect();
        let mut peers = self.peers.write();
        let mut state = self.state.write();
        for addr in peers.difference(&current) {
            state.remove_from_isr(addr);
            tracing::info!("replication peer removed: {addr}");
        }
        for addr in current.difference(&peers) {
            state.add_to_isr(addr.clone());
            tracing::info!("replication peer added: {addr}");
        }
        *peers = current;
    }

    /// 移除对端
    pub fn remove_peer(&self, addr: &str) {
        self.peers.write().remove(addr);
        self.state.write().remove_from_isr(addr);
        self.clients.write().remove(addr);
    }

    /// 对端列表（不含自身）
    pub fn peers(&self) -> Vec<String> {
        self.peers.read().iter().cloned().collect()
    }

    /// 心跳间隔（毫秒）
    pub fn set_heartbeat_interval_ms(&mut self, ms: u64) {
        self.heartbeat_interval_ms = ms;
    }

    // ──── 分区 Leader 静态分配（Q1 / C1）────

    /// 显式分配分区 Leader（空 = 自动 min-addr）
    pub fn set_shard_leader(&self, shard: &str, leader_addr: String) {
        self.shard_leaders.write().insert(shard.to_string(), leader_addr);
    }

    /// 分区 Leader：显式覆盖优先；否则 ISR 成员（含自身）中地址最小者。
    /// 所有 agent 基于同一成员集合计算，结果一致（静态分配）。
    pub fn shard_leader(&self, shard: &str) -> String {
        if let Some(l) = self.shard_leaders.read().get(shard) {
            return l.clone();
        }
        let mut addrs: Vec<String> = self.peers.read().iter().cloned().collect();
        addrs.push(self.agent_addr.clone());
        addrs.into_iter().min().unwrap_or_else(|| self.agent_addr.clone())
    }

    /// 本 agent 是否为指定 shard 的 Leader
    pub fn is_leader(&self, shard: &str) -> bool {
        self.shard_leader(shard) == self.agent_addr
    }

    /// 生效的 min_isr：单 agent（无对端）自动降级为 1，零破坏兼容（C6）。
    /// 即 min(config.min_isr, 1 + 对端数)。
    pub fn effective_min_isr(&self) -> usize {
        let peer_count = self.peers.read().len();
        self.config.min_isr.min(1 + peer_count).max(1)
    }

    /// 当前 ISR 是否满足写入条件（acked_including_self = 自身 + 已确认 follower 数）
    pub fn isr_satisfied(&self, acked_including_self: usize) -> bool {
        acked_including_self >= self.effective_min_isr()
    }

    /// ISR 成员快照（含自身）
    pub fn isr_members(&self) -> Vec<String> {
        self.state.read().isr_iter().cloned().collect()
    }

    /// 是否降级（ISR < 生效 min_isr）
    pub fn is_degraded(&self) -> bool {
        self.state.read().isr_size() < self.effective_min_isr()
    }

    // ──── 同步复制推送（Leader 侧）────

    /// 获取（或建立）对端复制客户端
    async fn client_for(&self, addr: &str) -> Result<ReplicaClient, ReplicationError> {
        if let Some(c) = self.clients.read().get(addr) {
            return Ok(c.clone());
        }
        let client = ReplicaClient::connect(addr).await?;
        self.clients.write().insert(addr.to_string(), client.clone());
        Ok(client)
    }

    /// 推送到 ISR Followers（排除自身），返回成功确认数。
    /// 单 follower 失败不影响其余；确认数由调用方结合自身做 min_isr 判定。
    pub async fn push_to_followers(
        &self,
        entry: &ReplicationEntry,
    ) -> Result<usize, ReplicationError> {
        let followers: Vec<String> = self
            .state
            .read()
            .isr_iter()
            .cloned()
            .filter(|a| a != &self.agent_addr)
            .collect();
        if followers.is_empty() {
            return Ok(0);
        }
        let timeout = Duration::from_millis(self.config.sync_timeout_ms);
        let mut acked = 0usize;
        for addr in followers {
            let client = match self.client_for(&addr).await {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!("replication: no client for {addr}: {e}");
                    continue;
                }
            };
            match tokio::time::timeout(timeout, client.apply(entry)).await {
                Ok(Ok(())) => acked += 1,
                Ok(Err(e)) => {
                    tracing::warn!("replication: follower {addr} apply failed: {e}");
                }
                Err(_) => {
                    tracing::warn!(
                        "replication: follower {addr} apply timeout ({}ms)",
                        self.config.sync_timeout_ms
                    );
                }
            }
        }
        Ok(acked)
    }

    /// 校验同步复制是否满足 min_isr（acked_including_self = 自身 + 确认 follower 数）
    pub fn ensure_isr(&self, acked_including_self: usize) -> Result<(), ReplicationError> {
        let required = self.effective_min_isr();
        if acked_including_self >= required {
            Ok(())
        } else {
            Err(ReplicationError::IsrDegraded {
                required,
                actual: acked_including_self,
            })
        }
    }

    // ──── 心跳与 Reconcile 追赶 ────

    /// 启动后台心跳任务：周期性向对端发送 IsrHeartbeat，交换各 shard
    /// 最后序列号，维护 ISR 成员并触发落后 Follower 的 Reconcile 追赶。
    pub fn start_heartbeat(self: &Arc<Self>, store: Arc<dyn ReplicatedStore + Send + Sync>) {
        if self.heartbeat_task.read().is_some() {
            return;
        }
        let manager = self.clone();
        let interval = self.heartbeat_interval_ms;
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = stop.clone();
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(interval.max(100)));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                if stop_flag.load(Ordering::SeqCst) {
                    break;
                }
                manager.heartbeat_once(&*store).await;
            }
        });
        self.heartbeat_stop.write().replace(stop);
        *self.heartbeat_task.write() = Some(handle);
    }

    /// 停止心跳任务（用于测试清理）
    pub fn stop_heartbeat(&self) {
        if let Some(flag) = self.heartbeat_stop.write().take() {
            flag.store(true, Ordering::SeqCst);
        }
        if let Some(handle) = self.heartbeat_task.write().take() {
            handle.abort();
        }
    }

    /// 单轮心跳：向全部对端发送 IsrHeartbeat，维护 ISR 成员 + 落后检测
    pub async fn heartbeat_once(&self, store: &dyn ReplicatedStore) {
        let peers = self.peers();
        let progress: Vec<ReplicaShardProgress> = store
            .shards()
            .into_iter()
            .map(|s| ReplicaShardProgress {
                shard_id: s.clone(),
                last_sequence: store.last_local_sequence(&s),
            })
            .collect();
        for addr in peers {
            let client = match self.client_for(&addr).await {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!("replication: heartbeat to {addr} failed (no client): {e}");
                    self.state.write().remove_from_isr(&addr);
                    continue;
                }
            };
            match client.heartbeat(&self.agent_addr, &progress).await {
                Ok(resp) => {
                    self.state.write().add_to_isr(addr.clone());
                    // 落后检测：对端声明的各 shard 序列号高于本地 → 触发 Reconcile 追赶
                    if !resp.leader_progress.is_empty() {
                        for p in resp.leader_progress {
                            let local = store.last_local_sequence(&p.shard_id);
                            if p.last_sequence > local {
                                tracing::info!(
                                    "replication: follower {addr} behind on shard {} (local={} leader={}), reconciling",
                                    p.shard_id, local, p.last_sequence
                                );
                                let _ = self.pull_and_catch_up(store, &p.shard_id, local + 1).await;
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("replication: heartbeat to {addr} failed: {e}");
                    self.state.write().remove_from_isr(&addr);
                    self.clients.write().remove(&addr);
                }
            }
        }
    }

    /// Reconcile 追赶（Follower 侧）：从 Leader 拉取缺失序列号区间并应用。
    /// 返回本次应用的条目数。
    pub async fn pull_and_catch_up(
        &self,
        store: &dyn ReplicatedStore,
        shard: &str,
        from_seq: u64,
    ) -> Result<u64, ReplicationError> {
        let leader = self.shard_leader(shard);
        if leader == self.agent_addr {
            return Ok(0);
        }
        let client = self.client_for(&leader).await?;
        let entries = client.reconcile(shard, from_seq, 0).await?;
        let mut applied = 0u64;
        for entry in entries {
            store.apply_entry(&entry)?;
            applied += 1;
        }
        if applied > 0 {
            tracing::info!("replication: reconciled {applied} entries for shard {shard} from {leader}");
        }
        Ok(applied)
    }
}
