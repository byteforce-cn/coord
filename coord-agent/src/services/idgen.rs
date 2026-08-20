// coord-agent: ID 生成器 (ID Generator Service)
//
// 实现 BaseService trait，提供全局唯一 ID 生成能力。
//
// 架构（v4.0，ISSUE-011 决策）:
// - 默认实现：雪花（nodeid）——agent 作为 node 上 daemonset，按 10bit nodeid 生成
//   64-bit long 雪花 ID（无状态、不依赖 KV、清库不重置、离线可用）
// - 可选实现：KV 号段模式（opt-in，idgen.mode = "segment"）——本地缓存号段，
//   Server 侧 Txn CAS 原子递增分配（修复 fresh 重复根因，见 ISSUE-011）
// - 节点 ID 稳定唯一：显式 COORD_NODE_ID / idgen_node_id > 主机名稳定哈希；
//   有 Server 时启动期在 /_idgen/nodes/{nodeid} CAS 注册（冲突顺延、重启保持）
//
// 参见 docs/client-agent-architecture-v3.md §5.4、docs/issue/ISSUE-011。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use coord_proto::kv::PutRequest;
use coord_proto::txn::compare::{CompareResult, Target, TargetValue};
use coord_proto::txn::request_op::Op;
use coord_proto::txn::{Compare, RequestOp};
use parking_lot::{Mutex, RwLock as ParkingRwLock};
use tokio::sync::watch;

use crate::proxy::AgentInner;
use crate::service::{BaseService, ServiceResult};

// ──── 类型定义 ────

/// ID 生成器实现模式
///
/// - [`Snowflake`](IdGenMode::Snowflake)（默认）：按 nodeid 生成 64-bit 雪花 ID，
///   无状态、全局唯一、不依赖 KV（agent 作为 node 上 daemonset 的理想默认实现）；
/// - [`Segment`](IdGenMode::Segment)（opt-in）：KV 号段模式，本地缓存号段 + Server 侧
///   Txn CAS 原子递增（适合仍需小整数连续 ID 的场景）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdGenMode {
    Snowflake,
    Segment,
}

impl IdGenMode {
    /// 解析配置字符串（默认 snowflake）
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "segment" | "idgen-segment" => IdGenMode::Segment,
            _ => IdGenMode::Snowflake,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            IdGenMode::Snowflake => "snowflake",
            IdGenMode::Segment => "segment",
        }
    }
}

/// ID 号段分配信息（存储在 Server）
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IdSegment {
    /// 号段名称（业务标识）
    pub name: String,
    /// 当前已分配到的最大值
    pub current_max: u64,
    /// 号段步长（每次分配的 ID 数量）
    pub step: u64,
    /// 上次更新时间
    pub updated_at: u64,
}

impl IdSegment {
    pub fn new(name: impl Into<String>, step: u64) -> Self {
        Self {
            name: name.into(),
            current_max: 0,
            step,
            updated_at: unix_ts(),
        }
    }

    /// 构造 Server 存储 key
    pub fn storage_key(name: &str) -> Vec<u8> {
        format!("/_idgen/{name}").into_bytes()
    }
}

/// 本地号段缓存
#[derive(Debug)]
struct LocalSegment {
    /// 号段起始值（不含）
    start: u64,
    /// 号段结束值（含）
    end: u64,
    /// 当前已分配到的值
    current: AtomicU64,
}

impl Clone for LocalSegment {
    fn clone(&self) -> Self {
        Self {
            start: self.start,
            end: self.end,
            current: AtomicU64::new(self.current.load(Ordering::SeqCst)),
        }
    }
}

impl LocalSegment {
    fn new(start: u64, end: u64) -> Self {
        Self {
            start,
            end,
            current: AtomicU64::new(start),
        }
    }

    /// 从本地号段分配一个 ID
    fn next_id(&self) -> Option<u64> {
        loop {
            let current = self.current.load(Ordering::Relaxed);
            if current >= self.end {
                return None; // 号段耗尽
            }
            let next = current + 1;
            if self
                .current
                .compare_exchange(current, next, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                return Some(next);
            }
            // CAS 失败，重试
        }
    }

    /// 剩余可用 ID 数
    fn remaining(&self) -> u64 {
        let current = self.current.load(Ordering::Relaxed);
        if current >= self.end {
            0
        } else {
            self.end - current
        }
    }
}

fn unix_ts() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// 构造号段 CAS 请求（纯函数，可单测）
///
/// - `current == None`（key 不存在）：Txn `Version == 0` 才写入 `current_max = step`，
///   分配区间 `(0, step]`，first_id = 1；
/// - `current == Some(v)`：Txn `Value == v`（读到后未被他人改动）才写入
///   `current_max = old_max + step`，分配区间 `(old_max, old_max + step]`，
///   first_id = old_max + 1。
///
/// 返回 `(local_start, local_end, compare, 新序列化值)`。
fn build_segment_cas(
    key: &[u8],
    step: u64,
    current: Option<&[u8]>,
    name: &str,
) -> ServiceResult<(u64, u64, Compare, Vec<u8>)> {
    match current {
        None => {
            let seg = IdSegment {
                current_max: step,
                updated_at: unix_ts(),
                ..IdSegment::new(name, step)
            };
            let compare = Compare {
                result: CompareResult::Equal as i32,
                target: Target::Version as i32,
                key: key.to_vec(),
                target_value: Some(TargetValue::Version(0)),
            };
            let value = serde_json::to_vec(&seg)
                .map_err(|e| format!("serialize id segment: {e}"))?;
            Ok((0, step, compare, value))
        }
        Some(v) => {
            let old: IdSegment = serde_json::from_slice(v)
                .map_err(|e| format!("deserialize id segment: {e}"))?;
            let new_max = old.current_max + step;
            let seg = IdSegment {
                current_max: new_max,
                updated_at: unix_ts(),
                ..old
            };
            let compare = Compare {
                result: CompareResult::Equal as i32,
                target: Target::Value as i32,
                key: key.to_vec(),
                target_value: Some(TargetValue::Value(v.to_vec())),
            };
            let value = serde_json::to_vec(&seg)
                .map_err(|e| format!("serialize id segment: {e}"))?;
            Ok((old.current_max, new_max, compare, value))
        }
    }
}

// ──── IdGenCache ────

/// ID 生成器本地缓存
pub struct IdGenCache {
    /// 活跃的本地号段：name → LocalSegment
    segments: BTreeMap<String, LocalSegment>,
    /// 号段步长配置
    default_step: u64,
}

impl IdGenCache {
    pub fn new(default_step: u64) -> Self {
        Self {
            segments: BTreeMap::new(),
            default_step,
        }
    }

    /// 尝试从本地号段分配 ID
    pub fn try_next_id(&self, name: &str) -> Option<u64> {
        self.segments.get(name).and_then(|seg| seg.next_id())
    }

    /// 设置本地号段
    pub fn set_segment(&mut self, name: &str, start: u64, end: u64) {
        self.segments
            .insert(name.to_string(), LocalSegment::new(start, end));
    }

    /// 移除本地号段（号段耗尽时）
    pub fn remove_segment(&mut self, name: &str) {
        self.segments.remove(name);
    }

    /// 查询号段剩余量
    pub fn remaining(&self, name: &str) -> Option<u64> {
        self.segments.get(name).map(|seg| seg.remaining())
    }

    /// 是否有活跃号段
    pub fn has_segment(&self, name: &str) -> bool {
        self.segments.contains_key(name)
    }

    /// 号段数量
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }
}

// ──── 本地雪花生成器（无 Server 降级模式）────

/// 本地雪花 ID 生成器
///
/// 64 位布局: [1 bit 符号(0)] [41 bit 毫秒时间戳] [10 bit 节点ID] [12 bit 序列号]
/// 无 Server 连接时用于保证本机 ID 唯一且趋势递增。
struct LocalSnowflake {
    /// 10 bit 节点 ID
    node_id: u64,
    /// 自定义纪元（毫秒）
    epoch_ms: u64,
    /// 上次生成时间戳（毫秒）
    last_ts: u64,
    /// 同毫秒序列号
    seq: u16,
}

impl LocalSnowflake {
    /// 自定义纪元: 2023-11-14 00:00:00 UTC
    const EPOCH_MS: u64 = 1_700_000_000_000;
    /// 序列号上限（12 bit）
    const MAX_SEQ: u16 = 0x0FFF;

    fn new(node_id: u64) -> Self {
        Self {
            node_id: node_id & 0x3FF,
            epoch_ms: Self::EPOCH_MS,
            last_ts: 0,
            seq: 0,
        }
    }

    fn node_id(&self) -> u64 {
        self.node_id
    }

    /// 更新节点 ID（启动期注册/冲突顺延后使用）
    fn set_node_id(&mut self, node_id: u64) {
        self.node_id = node_id & 0x3FF;
    }

    fn next_id(&mut self) -> u64 {
        let now = unix_ts_ms();
        // 时钟回拨保护：保持单调（不早于上次时间戳）
        let mut ts = if now < self.last_ts { self.last_ts } else { now };

        if ts == self.last_ts {
            self.seq += 1;
            if self.seq > Self::MAX_SEQ {
                // 同毫秒序列耗尽，推进到下一毫秒
                ts += 1;
                self.last_ts = ts;
                self.seq = 0;
            }
        } else {
            self.last_ts = ts;
            self.seq = 0;
        }

        let t = ts - self.epoch_ms;
        (t << 22) | (self.node_id << 12) | self.seq as u64
    }
}

fn unix_ts_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// 解析雪花节点 ID（10bit，0-1023）
///
/// 优先级：显式配置 `idgen_node_id` > 环境变量 `COORD_NODE_ID` > 主机名稳定哈希。
fn resolve_node_id(override_id: Option<u64>) -> u64 {
    if let Some(id) = override_id {
        return id & 0x3FF;
    }
    if let Ok(v) = std::env::var("COORD_NODE_ID") {
        if let Ok(id) = v.trim().parse::<u64>() {
            return id & 0x3FF;
        }
    }
    node_id_from_host()
}

/// 主机名稳定哈希派生（不掺 PID，重启稳定；一节点一 agent 时天然唯一）
fn node_id_from_host() -> u64 {
    fnv1a(&hostname()) & 0x3FF
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "coord-agent".to_string())
}

/// 节点注册 owner：`hostname:agent_addr`（区分同主机多 agent）
fn node_owner(agent_addr: &str) -> String {
    format!("{}:{}", hostname(), agent_addr)
}

fn fnv1a(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

// ──── IdGenService ────

/// ID 生成器服务
///
/// 实现 `BaseService` trait，为应用提供高性能全局唯一 ID 生成。
/// 有 Server 连接时使用 KV 号段模式；无 Server 时降级为本地雪花生成。
pub struct IdGenService {
    /// 到 Server 集群的内部客户端；None = 本地模式（无 Server）
    inner: Option<Arc<AgentInner>>,
    /// 本地号段缓存（segment 模式使用）
    cache: Arc<ParkingRwLock<IdGenCache>>,
    /// 本地雪花生成器（snowflake 默认模式恒有；segment 模式仅在无 Server 时降级使用）
    local: Option<Mutex<LocalSnowflake>>,
    /// 实现模式：snowflake（默认）| segment（opt-in）
    mode: IdGenMode,
    /// 节点注册 owner（hostname:agent_addr，用于 /_idgen/nodes 注册归属）
    node_owner: String,
    /// 健康状态
    healthy: ParkingRwLock<bool>,
    /// 关闭信号
    shutdown_tx: ParkingRwLock<Option<watch::Sender<()>>>,
}

impl IdGenService {
    pub const NAME: &'static str = "idgen";

    /// 创建 IdGen 服务（默认实现：雪花，nodeid 派生）。
    pub fn new(inner: Option<Arc<AgentInner>>, default_step: u64) -> Self {
        Self::new_with_options(inner, default_step, IdGenMode::Snowflake, None, "agent")
    }

    /// 完整构造（配置驱动）。
    ///
    /// - `mode`: 默认实现 [`IdGenMode::Snowflake`] 或号段 [`IdGenMode::Segment`]（opt-in）
    /// - `node_id_override`: 显式雪花节点 ID（0-1023），为空时按主机名派生 + 注册
    /// - `agent_addr`: 节点注册 owner 的地址部分（hostname:agent_addr）
    pub fn new_with_options(
        inner: Option<Arc<AgentInner>>,
        default_step: u64,
        mode: IdGenMode,
        node_id_override: Option<u64>,
        agent_addr: &str,
    ) -> Self {
        let local = match mode {
            // 雪花为默认：无论是否有 Server 都用本地雪花（无状态、不依赖 KV）
            IdGenMode::Snowflake => Some(Mutex::new(LocalSnowflake::new(resolve_node_id(
                node_id_override,
            )))),
            // 号段为 opt-in：有 Server 用 KV 号段；无 Server 降级雪花
            IdGenMode::Segment => {
                if inner.is_none() {
                    Some(Mutex::new(LocalSnowflake::new(resolve_node_id(
                        node_id_override,
                    ))))
                } else {
                    None
                }
            }
        };
        Self {
            inner,
            cache: Arc::new(ParkingRwLock::new(IdGenCache::new(default_step))),
            local,
            mode,
            node_owner: node_owner(agent_addr),
            healthy: ParkingRwLock::new(false),
            shutdown_tx: ParkingRwLock::new(None),
        }
    }

    /// 生成下一个 ID
    ///
    /// - 默认（snowflake）：本地雪花生成，按 nodeid 全局唯一、趋势递增，无网络 I/O。
    /// - opt-in（segment）+ Server：本地号段充足时直接分配（<1ms），否则向 Server CAS 申请新号段。
    /// - opt-in（segment）+ 无 Server：降级雪花生成。
    pub async fn next_id(&self, name: &str) -> ServiceResult<u64> {
        match self.mode {
            IdGenMode::Snowflake => {
                let local = self
                    .local
                    .as_ref()
                    .expect("snowflake present in snowflake mode");
                Ok(local.lock().next_id())
            }
            IdGenMode::Segment => match &self.inner {
                Some(inner) => {
                    // 1. 尝试本地分配
                    if let Some(id) = self.cache.read().try_next_id(name) {
                        return Ok(id);
                    }
                    // 2. 本地号段耗尽，从 Server 申请新号段（Txn CAS 原子）
                    self.allocate_segment(inner, name).await
                }
                None => {
                    let local = self
                        .local
                        .as_ref()
                        .expect("local snowflake present in local mode");
                    Ok(local.lock().next_id())
                }
            },
        }
    }

    /// 从 Server 申请新号段（Txn CAS 原子读写改写，失败重试）
    ///
    /// ISSUE-011 根因修复：旧实现为「range 读 + 普通 put 写」的非原子 RMW，且初始化
    /// 无 CAS——fresh 状态（key 不存在）下并发调用方（单 agent 并发 / 多 agent 独立缓存）
    /// 会读到同一基线 0 并都返回 first_id=1 → 重复。改为 CAS 后，同一号段键在同一时刻
    /// 只有一个调用方能推进基线，其余重试。
    async fn allocate_segment(&self, inner: &AgentInner, name: &str) -> ServiceResult<u64> {
        let storage_key = IdSegment::storage_key(name);
        let step = self.cache.read().default_step;

        loop {
            // 读取当前 Server 上的号段状态
            let pairs = inner
                .client
                .kv()
                .range(&storage_key, &storage_key, 1, 0)
                .await
                .map_err(|e| format!("failed to read id segment '{name}': {e}"))?;
            let current = pairs.into_iter().next().map(|(_k, v)| v);

            // 构造 CAS：key 不存在（Version==0 初始化）或值未变（Value==期望）才写入
            let (local_start, local_end, compare, value) =
                build_segment_cas(&storage_key, step, current.as_deref(), name)?;

            let put_op = RequestOp {
                op: Some(Op::RequestPut(PutRequest {
                    key: storage_key.clone(),
                    value,
                    lease_id: 0,
                    prev_kv: false,
                    request_id: Vec::new(),
                })),
            };

            let resp = inner
                .client
                .txn()
                .txn(vec![compare], vec![put_op], vec![])
                .await
                .map_err(|e| format!("failed to CAS id segment '{name}': {e}"))?;

            if resp.succeeded {
                // 更新本地号段：(local_start, local_end]
                self.cache.write().set_segment(name, local_start, local_end);
                let first_id = local_start + 1;
                tracing::debug!(
                    "IdGenService: allocated segment for '{name}': ({}, {}], first_id={first_id}",
                    local_start,
                    local_end
                );
                return Ok(first_id);
            }
            // CAS 失败：他方已推进号段 → 重读重试
            tokio::task::yield_now().await;
        }
    }

    /// 批量生成 ID
    pub async fn next_ids(&self, name: &str, count: u64) -> ServiceResult<Vec<u64>> {
        let mut ids = Vec::with_capacity(count as usize);
        for _ in 0..count {
            ids.push(self.next_id(name).await?);
        }
        Ok(ids)
    }

    /// 查询号段剩余量
    pub fn remaining(&self, name: &str) -> Option<u64> {
        self.cache.read().remaining(name)
    }

    /// 号段数量
    pub fn segment_count(&self) -> usize {
        self.cache.read().segment_count()
    }

    /// 当前雪花节点 ID
    fn current_node_id(&self) -> u64 {
        self.local.as_ref().map(|l| l.lock().node_id()).unwrap_or(0)
    }

    /// 更新雪花节点 ID（注册/冲突顺延后）
    fn update_node_id(&self, node_id: u64) {
        if let Some(l) = &self.local {
            l.lock().set_node_id(node_id);
        }
    }

    /// 注册雪花节点 ID（best-effort，不阻塞启动）
    ///
    /// 在 `/_idgen/nodes/{nodeid}` 用 Txn CAS（`Version == 0`）登记 owner；
    /// - 已存在且 owner 相同（本节点重启）→ 保持当前 nodeid；
    /// - 已存在且 owner 不同（冲突）→ 顺延下一候选，最多尝试 1024 个；
    /// - 通信失败 → 告警并沿用派生 nodeid（与无 Server 场景一致的 best-effort 唯一性）。
    async fn register_node_id(&self, inner: &AgentInner) {
        let mut node_id = self.current_node_id();
        for _ in 0..1024 {
            let key = format!("/_idgen/nodes/{node_id}").into_bytes();
            let compare = Compare {
                result: CompareResult::Equal as i32,
                target: Target::Version as i32,
                key: key.clone(),
                target_value: Some(TargetValue::Version(0)),
            };
            let put = RequestOp {
                op: Some(Op::RequestPut(PutRequest {
                    key: key.clone(),
                    value: self.node_owner.as_bytes().to_vec(),
                    lease_id: 0,
                    prev_kv: false,
                    request_id: Vec::new(),
                })),
            };

            match inner.client.txn().txn(vec![compare], vec![put], vec![]).await {
                Ok(resp) if resp.succeeded => {
                    tracing::info!(
                        "IdGenService: registered snowflake node_id={node_id} (owner={})",
                        self.node_owner
                    );
                    self.update_node_id(node_id);
                    return;
                }
                Ok(_resp) => {
                    // 已存在：检查 owner 是否为本节点（重启保持）
                    match inner
                        .client
                        .kv()
                        .range(&key, &key, 1, 0)
                        .await
                        .ok()
                        .and_then(|pairs| pairs.into_iter().next().map(|(_k, v)| v))
                    {
                        Some(v) if v == self.node_owner.as_bytes() => {
                            tracing::info!(
                                "IdGenService: node_id={node_id} already owned by this agent (restart), keeping"
                            );
                            self.update_node_id(node_id);
                            return;
                        }
                        _ => {
                            // 冲突 → 顺延下一候选
                            node_id = (node_id + 1) & 0x3FF;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "IdGenService: failed to register node_id (best-effort, using derived): {e}"
                    );
                    return;
                }
            }
        }
        tracing::warn!(
            "IdGenService: all 1024 node ids occupied; falling back to derived node_id={}",
            self.current_node_id()
        );
    }
}

#[async_trait]
impl BaseService for IdGenService {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    async fn start(&self) -> ServiceResult<()> {
        tracing::info!("IdGenService: starting (mode={})", self.mode.as_str());

        // 雪花默认模式：有 Server 时注册 nodeid（CAS 冲突顺延、重启保持）
        if self.mode == IdGenMode::Snowflake {
            if let Some(inner) = &self.inner {
                self.register_node_id(inner).await;
            }
        }

        *self.healthy.write() = true;

        let (_tx, mut rx) = watch::channel::<()>(());
        *self.shutdown_tx.write() = Some(_tx);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = rx.changed() => {
                        tracing::info!("IdGenService: background task shutting down");
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(60)) => {}
                }
            }
        });

        Ok(())
    }

    async fn stop(&self) -> ServiceResult<()> {
        tracing::info!("IdGenService: stopping");
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

impl std::fmt::Debug for IdGenService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdGenService")
            .field("segments", &self.segment_count())
            .field("healthy", &self.health_check())
            .finish()
    }
}

// ──── tests ────

#[cfg(test)]
mod tests {
    use super::*;

    // ──── IdSegment 测试 ────

    #[test]
    fn test_id_segment_creation() {
        let seg = IdSegment::new("order-id", 1000);
        assert_eq!(seg.name, "order-id");
        assert_eq!(seg.current_max, 0);
        assert_eq!(seg.step, 1000);
    }

    #[test]
    fn test_id_segment_storage_key() {
        let key = IdSegment::storage_key("order-id");
        assert_eq!(String::from_utf8_lossy(&key), "/_idgen/order-id");
    }

    #[test]
    fn test_id_segment_serialization_roundtrip() {
        let seg = IdSegment {
            name: "test".into(),
            current_max: 5000,
            step: 1000,
            updated_at: 1700000000,
        };
        let json = serde_json::to_vec(&seg).unwrap();
        let restored: IdSegment = serde_json::from_slice(&json).unwrap();
        assert_eq!(restored, seg);
    }

    // ──── LocalSegment 测试 ────

    #[test]
    fn test_local_segment_sequential_allocation() {
        let seg = LocalSegment::new(0, 100);
        for i in 1..=100 {
            assert_eq!(seg.next_id(), Some(i));
        }
        // 号段耗尽
        assert_eq!(seg.next_id(), None);
        assert_eq!(seg.remaining(), 0);
    }

    #[test]
    fn test_local_segment_remaining() {
        let seg = LocalSegment::new(0, 50);
        assert_eq!(seg.remaining(), 50);
        for _ in 0..25 {
            seg.next_id();
        }
        assert_eq!(seg.remaining(), 25);
    }

    // ──── IdGenCache 测试 ────

    #[test]
    fn test_id_gen_cache_set_and_allocate() {
        let mut cache = IdGenCache::new(1000);
        cache.set_segment("test", 0, 100);

        assert!(cache.has_segment("test"));
        assert_eq!(cache.segment_count(), 1);

        let id = cache.try_next_id("test").unwrap();
        assert_eq!(id, 1);
    }

    #[test]
    fn test_id_gen_cache_exhaustion() {
        let mut cache = IdGenCache::new(10);
        cache.set_segment("test", 0, 5);

        // 分配 1,2,3,4,5
        for i in 1..=5 {
            assert_eq!(cache.try_next_id("test"), Some(i));
        }
        // 耗尽
        assert_eq!(cache.try_next_id("test"), None);
    }

    #[test]
    fn test_id_gen_cache_remove_segment() {
        let mut cache = IdGenCache::new(100);
        cache.set_segment("test", 0, 10);
        assert!(cache.has_segment("test"));

        cache.remove_segment("test");
        assert!(!cache.has_segment("test"));
        assert_eq!(cache.segment_count(), 0);
    }

    #[test]
    fn test_id_gen_cache_remaining() {
        let mut cache = IdGenCache::new(100);
        cache.set_segment("test", 0, 20);
        assert_eq!(cache.remaining("test"), Some(20));

        cache.try_next_id("test");
        assert_eq!(cache.remaining("test"), Some(19));
    }

    // ──── IdGenService 名称常量 ────

    #[test]
    fn test_id_gen_service_name_constant() {
        assert_eq!(IdGenService::NAME, "idgen");
    }

    // ──── LocalSnowflake 本地雪花生成器 ────

    #[test]
    fn test_local_snowflake_unique_and_monotonic() {
        let mut sf = LocalSnowflake::new(42);
        let mut prev = sf.next_id();
        for _ in 0..10_000 {
            let next = sf.next_id();
            assert!(next > prev, "snowflake id must be monotonic: {next} <= {prev}");
            prev = next;
        }
    }

    #[test]
    fn test_local_snowflake_node_id_bits() {
        let mut sf = LocalSnowflake::new(0x3FF);
        let id = sf.next_id();
        // 节点 ID 位于 [12, 22) bit
        assert_eq!((id >> 12) & 0x3FF, 0x3FF);
    }

    #[test]
    fn test_local_snowflake_different_nodes_do_not_collide() {
        let mut sf1 = LocalSnowflake::new(1);
        let mut sf2 = LocalSnowflake::new(2);
        let mut seen = std::collections::HashSet::new();
        for _ in 0..1000 {
            let id1 = sf1.next_id();
            let id2 = sf2.next_id();
            assert_ne!(id1, id2);
            assert!(seen.insert(id1));
            assert!(seen.insert(id2));
        }
    }

    // ──── IdGenService 本地模式（无 Server）────

    #[tokio::test]
    async fn test_id_gen_service_local_mode_next_id() {
        let svc = IdGenService::new(None, 1000);
        let id1 = svc.next_id("orders").await.expect("next_id should work without server");
        let id2 = svc.next_id("orders").await.expect("next_id should work without server");
        assert!(id1 > 0);
        assert!(id2 > id1, "local ids must be monotonic: {id2} <= {id1}");
    }

    // ──── IdGenMode 解析 ────

    #[test]
    fn test_id_gen_mode_parse_defaults_to_snowflake() {
        assert_eq!(IdGenMode::parse("snowflake"), IdGenMode::Snowflake);
        assert_eq!(IdGenMode::parse("Snowflake"), IdGenMode::Snowflake);
        assert_eq!(IdGenMode::parse(""), IdGenMode::Snowflake);
        assert_eq!(IdGenMode::parse("garbage"), IdGenMode::Snowflake);
        assert_eq!(IdGenMode::parse("segment"), IdGenMode::Segment);
        assert_eq!(IdGenMode::parse("  segment "), IdGenMode::Segment);
        assert_eq!(IdGenMode::Snowflake.as_str(), "snowflake");
        assert_eq!(IdGenMode::Segment.as_str(), "segment");
    }

    // ──── 雪花 nodeid 解析 ────

    #[test]
    fn test_resolve_node_id_override() {
        assert_eq!(resolve_node_id(Some(0)), 0);
        assert_eq!(resolve_node_id(Some(42)), 42);
        // 超范围掩码到 10bit
        assert_eq!(resolve_node_id(Some(0x3FF)), 0x3FF);
        assert_eq!(resolve_node_id(Some(0x400)), 0);
        assert_eq!(resolve_node_id(Some(0x7FF)), 0x3FF);
    }

    #[test]
    fn test_node_id_from_host_is_stable() {
        // 主机名稳定哈希（不掺 PID）：多次调用结果一致
        assert_eq!(node_id_from_host(), node_id_from_host());
        assert!(node_id_from_host() <= 0x3FF);
    }

    #[test]
    fn test_node_owner_format() {
        let owner = node_owner("127.0.0.1:19527");
        assert!(
            owner.ends_with(":127.0.0.1:19527"),
            "owner should embed agent addr, got {owner}"
        );
    }

    // ──── 号段 CAS 请求构造（ISSUE-011 修复）────

    #[test]
    fn test_build_segment_cas_init() {
        let key = IdSegment::storage_key("permission");
        let (start, end, compare, value) =
            build_segment_cas(&key, 1000, None, "permission").unwrap();
        assert_eq!((start, end), (0, 1000));
        // 初始化：Version == 0 才写入
        assert_eq!(compare.result, CompareResult::Equal as i32);
        assert_eq!(compare.target, Target::Version as i32);
        assert_eq!(compare.target_value, Some(TargetValue::Version(0)));
        let seg: IdSegment = serde_json::from_slice(&value).unwrap();
        assert_eq!(seg.current_max, 1000);
        assert_eq!(seg.name, "permission");
        assert_eq!(seg.step, 1000);
    }

    #[test]
    fn test_build_segment_cas_advance() {
        let key = IdSegment::storage_key("permission");
        let old = IdSegment {
            name: "permission".into(),
            current_max: 1000,
            step: 1000,
            updated_at: 1,
        };
        let old_bytes = serde_json::to_vec(&old).unwrap();
        let (start, end, compare, value) =
            build_segment_cas(&key, 1000, Some(&old_bytes), "permission").unwrap();
        assert_eq!((start, end), (1000, 2000));
        // 递增：Value == 期望（读到后未被他人改动）才写入
        assert_eq!(compare.result, CompareResult::Equal as i32);
        assert_eq!(compare.target, Target::Value as i32);
        assert_eq!(compare.target_value, Some(TargetValue::Value(old_bytes)));
        let seg: IdSegment = serde_json::from_slice(&value).unwrap();
        assert_eq!(seg.current_max, 2000);
    }

    #[test]
    fn test_build_segment_cas_invalid_value_errors() {
        let key = IdSegment::storage_key("permission");
        assert!(build_segment_cas(&key, 1000, Some(b"not-json".as_slice()), "permission").is_err());
    }

    // ──── 默认实现 = 雪花（ISSUE-011 决策）────

    #[test]
    fn test_snowflake_mode_is_default_with_server() {
        // 无法在单测内构造真实 AgentInner，这里验证：snowflake 模式始终持有本地生成器
        // （new 默认即 snowflake；有 Server 时也走雪花，不依赖 KV 号段）
        let svc = IdGenService::new_with_options(None, 1000, IdGenMode::Snowflake, Some(7), "t");
        assert!(svc.local.is_some(), "snowflake mode must hold a local generator");
        assert_eq!(svc.current_node_id(), 7);
    }
}
