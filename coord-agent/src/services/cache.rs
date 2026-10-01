// coord-agent: 缓存 (Cache Service) — 数据面
//
// 实现 BaseService trait，基于 redb 提供本地持久化缓存引擎。
// 支持 String/Hash/List/Set 四种数据类型、TTL 过期。
//
// 架构（v3.0，源码核验版 + v2.1 ISR 落地）:
// - 本地 redb 存储引擎；启用 ISR 复制后，写路径经复制管理器同步到 ISR Followers
// - List pop 为单写事务原子出队；复制后 pop 仅 Leader 执行
// - TTL 复制 Leader 计算的绝对到期时间戳（encode_value 内嵌 expires_at）
//
// ISR 复制：复制日志 / 持久化幂等键 / 本地序列号在 cache.redb 内与数据写同事务提交。
//
// ⚠️ 复制语义的**准确边界**。
//    准确边界（与 `WHITEPAPER.md` 对齐）：
//
//    1. **本地**提交是原子的（幂等键 + 数据 + 序列号同事务，见
//       `replicated_apply_local`）；**跨节点**提交不是原子的（见 ADR-0002）。`replicated_write`
//       的顺序固定为「本地提交 → 推送 ISR → `ensure_isr` 校验」。
//    2. 由此产生一个**对调用方可见**的后果：若推送或 `min_isr` 校验失败，本函数
//       返回错误，但**本地写入已经生效**。调用方无法从返回值区分「没写进去」与
//       「写进去了但副本不足」—— 幂等键使重试安全，但**不能**据此宣称"失败即未写入"。
//    3. 落后副本**不是永久**的：心跳（`start_heartbeat` → `heartbeat_once`）检测到
//       对端序列号高于本地即触发 `pull_and_catch_up` → Reconcile 拉取缺失区间；
//       复制日志在 cache.redb 内**无上限保留**，故只要 Leader 可达就能补齐。
//       真正的窗口是"Leader 在本地提交后、Follower 补齐前崩溃"这段**暂时**不一致，
//       而不是"Follower 永久落后"。
//    4. **分区 Leader 是静态分配的**（`replication.rs` 的 `shard_leader`：显式覆盖
//       优先，否则取 ISR 成员中地址最小者）—— 没有自动故障转移。这是**设计边界**，
//       不是缺陷：所有 agent 基于同一成员集合算出同一结果，代价是 Leader 失联时
//       该分区**不可写**（不产生脑裂），恢复靠运维介入而非选举。
//
//    以上 1/2/4 已同步写入对外契约 `cache.proto` 的边界声明。
//
// ── 容量上界（B-PL-3）──
// 数据面 4 表活跃字节记账（`cache:meta`）+ 淘汰索引（`cache:evict*`）+ 服务内
// reaper（默认 10s：TTL 过期清扫 + 超界按「最后写入序」淘汰；单条超限写在写入
// 路径直接拒绝）。周期收敛语义与残余边界见 `CacheService::new` 文档与
// `docs/production/ops/boundaries.md` B-PL-3（单一归属）。

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use parking_lot::RwLock;
use redb::{ReadableDatabase, ReadableTable};

use crate::service::{BaseService, ServiceResult};

// ──── 可重导出类型 ────

/// 缓存数据类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CacheDataType {
    String,
    Hash,
    List,
    Set,
}

/// 缓存条目（用于迭代/导出）
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub data_type: CacheDataType,
    pub expires_at: Option<u64>,
}

/// 分片元数据
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CacheShardMeta {
    pub shard_id: String,
    pub leader_agent: String,
    pub replicas: Vec<String>,
    pub key_range_start: Vec<u8>,
    pub key_range_end: Vec<u8>,
}

/// 缓存统计信息
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct CacheStats {
    pub string_count: u64,
    pub hash_count: u64,
    pub list_count: u64,
    pub set_count: u64,
    pub shard_count: u64,
    /// 活跃字节（记账口径见「容量上界」小节；`CacheService` 为 redb 表记账值，
    /// `MokaCacheService` 暂未实现记账，恒为 0）
    pub total_size_bytes: u64,
}

/// 单轮 reaper 统计（`CacheService::reap_once` 返回）
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReapStats {
    /// 本轮回收到期的条目数
    pub expired_entries: u64,
    /// 本轮超界淘汰的条目数
    pub evicted_entries: u64,
    /// 本轮超界淘汰的字节数（记账口径）
    pub evicted_bytes: u64,
    /// 本轮结束时的活跃字节
    pub active_bytes: u64,
    /// 当前上界（0 = 不限）
    pub limit_bytes: u64,
}

/// reaper 累计统计（单调；由 reaper 后台任务/显式调用更新）
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReapCounters {
    /// 累计执行轮数
    pub passes: u64,
    /// 累计 TTL 过期回收条目数
    pub expired_entries: u64,
    /// 累计超界淘汰条目数
    pub evicted_entries: u64,
    /// 累计超界淘汰字节数（记账口径）
    pub evicted_bytes: u64,
    /// 累计失败轮数（单调；>0 表示至少有一轮 reap 失败）
    pub faults: u64,
}

// ──── redb 表定义 ────

const STRING_TABLE: redb::TableDefinition<&[u8], &[u8]> =
    redb::TableDefinition::new("cache:string");
const HASH_TABLE: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("cache:hash");
const LIST_TABLE: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("cache:list");
const SET_TABLE: redb::TableDefinition<&[u8], u64> = redb::TableDefinition::new("cache:set");
const SHARD_TABLE: redb::TableDefinition<&str, &[u8]> = redb::TableDefinition::new("cache:shards");

// ──── 复制日志表（ISR，v2.1）────
// 复制条目日志: key = [shard_len:u32][shard_bytes][seq:u64 BE]
const CACHE_REPL_ENTRY_TABLE: redb::TableDefinition<&[u8], &[u8]> =
    redb::TableDefinition::new("cache:repl_entries");
// 持久化幂等键: key = idempotency_key bytes
const CACHE_REPL_APPLIED_KEYS: redb::TableDefinition<&[u8], ()> =
    redb::TableDefinition::new("cache:repl_applied");
// 各 shard 最后已应用序列号: key = shard bytes
const CACHE_REPL_LOCAL_SEQ: redb::TableDefinition<&[u8], u64> =
    redb::TableDefinition::new("cache:repl_local_seq");

// ──── 容量记账 / 淘汰索引表（B-PL-3）────
//
// 记账口径：`active_bytes` = 4 张数据类型表内**物理存储行**大小之和，单条大小 =
// 物理 key 长度 + 存储值长度（含 TTL 前缀 / 到期时间戳）。不含 ISR 复制日志
// （其无上限保留是复制设计的一部分，见模块头）、分片元数据、索引表与 redb 页面
// 开销 —— 因此 redb 文件体积**大于**记账值。
//
// 淘汰索引按「最后写入序」排序（近似 LRU：写刷新、读不刷新 —— get 热路径零写放大）：
// - cache:evict:     seq(u64 BE) → [table_id:u8][key_len:u32 BE][key][size:u64 BE]
// - cache:evict_rev: [table_id:u8][key] → seq（替换/删除时定位旧序号）
// - cache:meta:      "active_bytes" → u64、"next_evict_seq" → u64
const CACHE_META_TABLE: redb::TableDefinition<&str, u64> = redb::TableDefinition::new("cache:meta");
const CACHE_EVICT_TABLE: redb::TableDefinition<&[u8], &[u8]> =
    redb::TableDefinition::new("cache:evict");
const CACHE_EVICT_REV_TABLE: redb::TableDefinition<&[u8], u64> =
    redb::TableDefinition::new("cache:evict_rev");

/// meta 表键：已记账活跃字节
const META_ACTIVE_BYTES: &str = "active_bytes";
/// meta 表键：下一个淘汰序号
const META_NEXT_EVICT_SEQ: &str = "next_evict_seq";

/// 数据表 id（索引 value 内）
const TID_STRING: u8 = 0;
const TID_HASH: u8 = 1;
const TID_LIST: u8 = 2;
const TID_SET: u8 = 3;

/// reaper 过期清扫每批（每表）最多删除的条目数（控制单事务规模）
const REAP_SWEEP_CHUNK: usize = 1024;
/// reaper 超界淘汰每批最多删除的条目数（控制单事务规模）
const REAP_EVICT_CHUNK: usize = 256;
/// 后台 reaper 默认周期（毫秒）
const DEFAULT_REAPER_INTERVAL_MS: u64 = 10_000;

// ──── Key 编码辅助 ────

/// Hash key 编码: key_bytes + b'\x00' + field_bytes
fn encode_hash_key(key: &str, field: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(key.len() + 1 + field.len());
    v.extend_from_slice(key.as_bytes());
    v.push(0);
    v.extend_from_slice(field.as_bytes());
    v
}

/// Hash key 前缀
fn hash_key_prefix(key: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(key.len() + 1);
    v.extend_from_slice(key.as_bytes());
    v.push(0);
    v
}

/// List key 编码: key_bytes + b'\x00' + index (8 bytes BE, offset by i64::MAX)
fn encode_list_key(key: &str, index: i64) -> Vec<u8> {
    let mut v = Vec::with_capacity(key.len() + 9);
    v.extend_from_slice(key.as_bytes());
    v.push(0);
    let adjusted = (index as i128).wrapping_add(i64::MAX as i128) as u64;
    v.extend_from_slice(&adjusted.to_be_bytes());
    v
}

/// List key 前缀
fn list_key_prefix(key: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(key.len() + 1);
    v.extend_from_slice(key.as_bytes());
    v.push(0);
    v
}

/// 解析 List key 中的 index
fn decode_list_index(encoded: &[u8], prefix_len: usize) -> Option<i64> {
    if encoded.len() < prefix_len + 8 {
        return None;
    }
    let adjusted = u64::from_be_bytes(encoded[prefix_len..prefix_len + 8].try_into().ok()?);
    Some((adjusted as i128).wrapping_sub(i64::MAX as i128) as i64)
}

/// Set key 编码: key_bytes + b'\x00' + member_bytes
fn encode_set_key(key: &str, member: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(key.len() + 1 + member.len());
    v.extend_from_slice(key.as_bytes());
    v.push(0);
    v.extend_from_slice(member);
    v
}

/// Set key 前缀
fn set_key_prefix(key: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(key.len() + 1);
    v.extend_from_slice(key.as_bytes());
    v.push(0);
    v
}

/// 从 Set 编码 key 中提取 member
fn decode_set_member(encoded: &[u8], prefix_len: usize) -> Vec<u8> {
    encoded[prefix_len..].to_vec()
}

// ──── 淘汰索引编码辅助（B-PL-3）────

/// 淘汰序号 key（u64 BE —— 字节序即时间序）
fn evict_seq_key(seq: u64) -> [u8; 8] {
    seq.to_be_bytes()
}

/// 反向索引 key: [table_id:u8][physical_key]
fn evict_rev_key(table_id: u8, physical_key: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(1 + physical_key.len());
    v.push(table_id);
    v.extend_from_slice(physical_key);
    v
}

/// 淘汰索引 value: [table_id:u8][key_len:u32 BE][key][size:u64 BE]
fn encode_evict_value(table_id: u8, physical_key: &[u8], size: u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(1 + 4 + physical_key.len() + 8);
    v.push(table_id);
    v.extend_from_slice(&(physical_key.len() as u32).to_be_bytes());
    v.extend_from_slice(physical_key);
    v.extend_from_slice(&size.to_be_bytes());
    v
}

/// 解析淘汰索引 value；格式非法返回 None（调用方 fail-closed 处理）
fn decode_evict_value(raw: &[u8]) -> Option<(u8, &[u8], u64)> {
    if raw.len() < 1 + 4 + 8 {
        return None;
    }
    let table_id = raw[0];
    let klen = u32::from_be_bytes(raw[1..5].try_into().ok()?) as usize;
    if raw.len() != 1 + 4 + klen + 8 {
        return None;
    }
    let key = &raw[5..5 + klen];
    let size = u64::from_be_bytes(raw[5 + klen..].try_into().ok()?);
    Some((table_id, key, size))
}

// ──── TTL 编解码 ────

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn encode_ttl(ttl_secs: u64) -> u64 {
    if ttl_secs == 0 {
        0
    } else {
        now_secs().saturating_add(ttl_secs)
    }
}

fn is_expired(expires_at: u64) -> bool {
    expires_at > 0 && now_secs() >= expires_at
}

/// 将 value + TTL 编码: [8 bytes expires_at BE][value]
fn encode_value(value: &[u8], ttl_secs: u64) -> Vec<u8> {
    let expires_at = encode_ttl(ttl_secs);
    let mut v = Vec::with_capacity(8 + value.len());
    v.extend_from_slice(&expires_at.to_be_bytes());
    v.extend_from_slice(value);
    v
}

/// 解码存储格式，检查 TTL；返回 None 若已过期
fn decode_value(raw: &[u8]) -> Option<Vec<u8>> {
    if raw.len() < 8 {
        return None;
    }
    let expires_at = u64::from_be_bytes(raw[..8].try_into().ok()?);
    if is_expired(expires_at) {
        return None;
    }
    Some(raw[8..].to_vec())
}

// ──── CacheService ────

/// 分布式缓存服务（数据面）
///
/// 基于 redb 的本地持久化缓存引擎。
/// 线程安全：使用 parking_lot::RwLock 保护 redb Database。
pub struct CacheService {
    db_path: PathBuf,
    db: RwLock<Option<redb::Database>>,
    started: RwLock<bool>,
    default_ttl_secs: u64,
    /// 容量上界（字节；0 = 不限）。记账 / 淘汰语义见 [`CacheService::new`] 与
    /// `docs/production/ops/boundaries.md` B-PL-3。
    max_size_bytes: u64,
    /// 后台 reaper 周期（毫秒；`set_reaper_interval` 可调）
    reaper_interval_ms: AtomicU64,
    /// reaper 是否已挂载（防止 start 多次 spawn）
    reaper_spawned: AtomicBool,
    /// reaper 累计轮数
    reap_passes: AtomicU64,
    /// reaper 累计 TTL 过期回收条目数
    reap_expired_entries: AtomicU64,
    /// reaper 累计超界淘汰条目数
    reap_evicted_entries: AtomicU64,
    /// reaper 累计超界淘汰字节数
    reap_evicted_bytes: AtomicU64,
    /// reaper 累计失败轮数（单调）
    reap_faults: AtomicU64,
    /// ISR 复制管理器（None = 单 agent 本地语义，零复制路径保留）
    replication: RwLock<Option<Arc<crate::services::replication::ReplicationManager>>>,
    /// 自身 Arc 弱引用（spawn_blocking 升级用，见 bind_self_weak）
    self_arc: RwLock<Option<std::sync::Weak<CacheService>>>,
}

impl std::fmt::Debug for CacheService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheService")
            .field("db_path", &self.db_path)
            .field("started", &self.started)
            .field("default_ttl_secs", &self.default_ttl_secs)
            .field("max_size_bytes", &self.max_size_bytes)
            .field("max_size_enforced", &(self.max_size_bytes > 0))
            .field(
                "reaper_interval_ms",
                &self.reaper_interval_ms.load(Ordering::Relaxed),
            )
            .field("reap_counters", &self.reap_counters())
            .finish()
    }
}

impl CacheService {
    /// 创建缓存服务。
    ///
    /// # 容量上界（B-PL-3）
    ///
    /// `max_size_bytes`（0 = 不限）**被强制执行**：写入路径在事务内对 4 张数据
    /// 类型表做字节记账；后台 reaper（默认周期 10s）清扫 TTL 过期，并在超界时按
    /// 「最后写入序」（近似 LRU —— 写刷新、读不刷新，get 零写放大）淘汰最旧条目
    /// 直到回到界内；单条写自身超过上界时**直接拒绝**（这种条目永远无法满足上界，
    /// 写成功再驱逐等于假成功）。
    ///
    /// # 语义边界（与 `docs/production/ops/boundaries.md` B-PL-3 单一归属）
    ///
    /// - 上界是**周期收敛**：reaper 周期内允许短暂超界（指标
    ///   `coord_agent_cache_active_bytes` / `coord_agent_cache_limit_bytes` 可观测）；
    /// - 淘汰为「最后写入」新近度的近似 LRU，不是严格 LRU；
    /// - 记账只覆盖数据表活跃字节：不含 ISR 复制日志（其无上限保留是复制设计的
    ///   一部分）、索引表与 redb 页面开销 —— redb 文件体积大于记账值；
    /// - ISR 复制启用时，淘汰/过期回收是**各节点本地行为**（不跨节点复制）。
    pub fn new(db_path: PathBuf, max_size_bytes: u64, default_ttl_secs: u64) -> Self {
        Self {
            db_path,
            db: RwLock::new(None),
            started: RwLock::new(false),
            max_size_bytes,
            reaper_interval_ms: AtomicU64::new(DEFAULT_REAPER_INTERVAL_MS),
            reaper_spawned: AtomicBool::new(false),
            reap_passes: AtomicU64::new(0),
            reap_expired_entries: AtomicU64::new(0),
            reap_evicted_entries: AtomicU64::new(0),
            reap_evicted_bytes: AtomicU64::new(0),
            reap_faults: AtomicU64::new(0),
            default_ttl_secs,
            replication: RwLock::new(None),
            self_arc: RwLock::new(None),
        }
    }

    /// 绑定自身 Arc 弱引用（gRPC handler 升级为强引用后，
    /// 把同步 redb 事务放到 `spawn_blocking`，避免阻塞 agent 异步执行器）。
    ///
    /// 由服务装配方在 `Arc::new` 后调用一次（lib.rs run_agent 数据面初始化）。
    /// 用 Weak 避免 Arc 自引用环导致无法释放。
    pub fn bind_self_weak(&self, me: &Arc<Self>) {
        *self.self_arc.write() = Some(Arc::downgrade(me));
    }

    /// 升级自身强引用（未绑定或已释放返回 None）
    pub fn self_arc(&self) -> Option<Arc<Self>> {
        self.self_arc.read().as_ref().and_then(|w| w.upgrade())
    }

    /// 在阻塞线程池上执行同步 redb 操作。
    ///
    /// gRPC handler 持有 `&CacheService`，但 `spawn_blocking` 需要 `'static` 自有
    /// 数据——通过 `self_arc` 弱引用升级为 `Arc<Self>` 后移入闭包，再调用同步方法，
    /// 保持现有完成通知语义不变。服务装配时必须先 `bind_self_weak`，否则返回
    /// "self_arc not bound"（装配缺陷，fail-fast 暴露）。
    pub async fn run_blocking<F, R>(&self, f: F) -> ServiceResult<R>
    where
        F: FnOnce(Arc<Self>) -> ServiceResult<R> + Send + 'static,
        R: Send + 'static,
    {
        let me = self
            .self_arc()
            .ok_or_else(|| "CacheService self_arc not bound".to_string())?;
        tokio::task::spawn_blocking(move || f(me))
            .await
            .map_err(|e| format!("cache blocking task join: {e}"))?
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

    // ──── 容量上界：记账 / 淘汰 / reaper（B-PL-3）────
    //
    // 契约与残余边界与 `docs/production/ops/boundaries.md` B-PL-3 单一归属：
    // - 记账 = 4 张数据类型表内**物理存储行**大小之和（物理 key 长度 + 存储值
    //   长度）。不含 ISR 复制日志 / 分片元数据 / 索引表 / redb 页面开销。
    // - 记账与数据在**同一次写事务**提交（redb 单写者串行化 ⇒ 无读-改-写竞态）。
    // - 强制点只有 reaper：周期清扫 TTL 过期 + 超界时按「最后写入序」淘汰最旧，
    //   到界内为止。**不是**逐写严格上界。
    // - 单条写 > max 时直接拒绝（永不驻留，避免「写成功但立即被淘汰」的假成功）。
    // - 读路径零写放大：get 不刷新淘汰序（淘汰序 = 最后写入序的近似 LRU）。

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
        let meta = rtx.open_table(CACHE_META_TABLE)?;
        Ok(meta.get(META_ACTIVE_BYTES)?.map(|v| v.value()).unwrap_or(0))
    }

    /// reaper 累计统计快照（原子读；单调计数器）
    pub fn reap_counters(&self) -> ReapCounters {
        ReapCounters {
            passes: self.reap_passes.load(Ordering::Relaxed),
            expired_entries: self.reap_expired_entries.load(Ordering::Relaxed),
            evicted_entries: self.reap_evicted_entries.load(Ordering::Relaxed),
            evicted_bytes: self.reap_evicted_bytes.load(Ordering::Relaxed),
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

    /// 单条写是否永远无法满足上界（超限 ⇒ 直接拒绝，不写不删不驱逐）
    fn ensure_entry_fits(&self, physical_key: &[u8], stored_value: &[u8]) -> ServiceResult<()> {
        if self.max_size_bytes == 0 {
            return Ok(());
        }
        let size = Self::entry_size(physical_key.len(), stored_value.len());
        if size > self.max_size_bytes {
            return Err(format!(
                "cache entry needs {size} bytes (key {} + value {}) but max_size_bytes={}; \
                 write rejected — this entry can never fit under the configured limit \
                 (see boundaries.md B-PL-3)",
                physical_key.len(),
                stored_value.len(),
                self.max_size_bytes
            )
            .into());
        }
        Ok(())
    }

    /// 同事务 upsert 记账：旧索引行移除 + 新索引行写入 + active_bytes 更新。
    /// `old_value_len` = 被替换的存储值长度（None = 此前无该物理 key）。
    /// 返回更新后的 active_bytes。
    fn account_upsert_tx(
        wtx: &redb::WriteTransaction,
        table_id: u8,
        physical_key: &[u8],
        old_value_len: Option<usize>,
        new_value_len: usize,
    ) -> ServiceResult<u64> {
        let rev_key = evict_rev_key(table_id, physical_key);
        let old_size = old_value_len
            .map(|l| Self::entry_size(physical_key.len(), l))
            .unwrap_or(0);
        let new_size = Self::entry_size(physical_key.len(), new_value_len);

        let mut meta = wtx.open_table(CACHE_META_TABLE)?;
        let cur = meta.get(META_ACTIVE_BYTES)?.map(|v| v.value()).unwrap_or(0);
        let seq = meta
            .get(META_NEXT_EVICT_SEQ)?
            .map(|v| v.value())
            .unwrap_or(0)
            + 1;
        let new_cur = cur
            .checked_sub(old_size)
            .and_then(|v| v.checked_add(new_size))
            .ok_or_else(|| {
                format!(
                    "cache accounting overflow/underflow (cur={cur} old={old_size} new={new_size})"
                )
            })?;

        // 替换写：先摘除旧索引行（rev → 旧 seq → evict），避免索引泄漏
        let old_seq = {
            let mut rev = wtx.open_table(CACHE_EVICT_REV_TABLE)?;
            let x = rev.remove(rev_key.as_slice())?.map(|v| v.value());
            x
        };
        if let Some(old_seq) = old_seq {
            let sk = evict_seq_key(old_seq);
            let mut ev = wtx.open_table(CACHE_EVICT_TABLE)?;
            ev.remove(sk.as_slice())?;
        }
        // 写入新索引行（新 seq = 最新写入序）
        let sk = evict_seq_key(seq);
        let encoded = encode_evict_value(table_id, physical_key, new_size);
        {
            let mut ev = wtx.open_table(CACHE_EVICT_TABLE)?;
            ev.insert(sk.as_slice(), encoded.as_slice())?;
        }
        {
            let mut rev = wtx.open_table(CACHE_EVICT_REV_TABLE)?;
            rev.insert(rev_key.as_slice(), seq)?;
        }
        meta.insert(META_NEXT_EVICT_SEQ, seq)?;
        meta.insert(META_ACTIVE_BYTES, new_cur)?;
        Ok(new_cur)
    }

    /// 同事务删除记账：旧索引行移除 + active_bytes 扣减。
    /// 仅在数据行**确实存在**时调用（调用方先 remove 检查）。
    fn account_remove_tx(
        wtx: &redb::WriteTransaction,
        table_id: u8,
        physical_key: &[u8],
        removed_value_len: usize,
    ) -> ServiceResult<u64> {
        let removed_size = Self::entry_size(physical_key.len(), removed_value_len);
        let rev_key = evict_rev_key(table_id, physical_key);
        let old_seq = {
            let mut rev = wtx.open_table(CACHE_EVICT_REV_TABLE)?;
            let x = rev.remove(rev_key.as_slice())?.map(|v| v.value());
            x
        };
        if let Some(old_seq) = old_seq {
            let sk = evict_seq_key(old_seq);
            let mut ev = wtx.open_table(CACHE_EVICT_TABLE)?;
            ev.remove(sk.as_slice())?;
        }
        let mut meta = wtx.open_table(CACHE_META_TABLE)?;
        let cur = meta.get(META_ACTIVE_BYTES)?.map(|v| v.value()).unwrap_or(0);
        let new_cur = cur.checked_sub(removed_size).ok_or_else(|| {
            format!("cache accounting underflow on remove (cur={cur} removed={removed_size})")
        })?;
        meta.insert(META_ACTIVE_BYTES, new_cur)?;
        Ok(new_cur)
    }

    /// 执行一轮回收：TTL 过期清扫 + 超界淘汰（收敛到 `max_size_bytes` 内）。
    ///
    /// 由后台任务按周期调用；测试可直接调用以获得确定性（无 sleep）。
    /// **负控制**：移除本函数中的淘汰循环 ⇒ `test_reaper_enforces_max_size` 必红；
    /// 移除 `sweep_expired` 调用 ⇒ `test_reaper_reclaims_expired_bytes` 必红。
    pub fn reap_once(&self) -> ServiceResult<ReapStats> {
        if !self.is_started() {
            return Err("CacheService not started".into());
        }
        let mut stats = ReapStats {
            expired_entries: self.sweep_expired()?,
            ..ReapStats::default()
        };
        if self.max_size_bytes > 0 {
            loop {
                let accounted = self.accounted_bytes()?;
                if accounted <= self.max_size_bytes {
                    break;
                }
                let (n, bytes) = self.evict_oldest_batch(REAP_EVICT_CHUNK, self.max_size_bytes)?;
                if n == 0 {
                    // 记账超界但索引为空：记账与索引同事务维护，正常不可能出现；
                    // fail-closed 记录并停止本轮（不得死循环）。
                    tracing::error!(
                        accounted,
                        max = self.max_size_bytes,
                        "cache reaper: over limit but eviction index is empty; stopping pass"
                    );
                    break;
                }
                stats.evicted_entries += n;
                stats.evicted_bytes += bytes;
            }
        }
        stats.active_bytes = self.accounted_bytes()?;
        stats.limit_bytes = self.max_size_bytes;

        self.reap_passes.fetch_add(1, Ordering::Relaxed);
        self.reap_expired_entries
            .fetch_add(stats.expired_entries, Ordering::Relaxed);
        self.reap_evicted_entries
            .fetch_add(stats.evicted_entries, Ordering::Relaxed);
        self.reap_evicted_bytes
            .fetch_add(stats.evicted_bytes, Ordering::Relaxed);
        Ok(stats)
    }

    /// TTL 过期清扫（分块，每块一个写事务）：把 4 张表内的过期行物理删除并扣账。
    /// 读路径的惰性删除只覆盖被访问的 key；这里是无人访问的过期数据的兜底回收。
    fn sweep_expired(&self) -> ServiceResult<u64> {
        macro_rules! sweep_one {
            ($def:expr, $tid:expr, $is_dead:expr, $stored_len:expr) => {{
                let mut cursor: Option<Vec<u8>> = None;
                let mut removed_total = 0u64;
                loop {
                    let wtx = self.write_tx()?;
                    let mut processed = 0usize;
                    let mut last_key: Option<Vec<u8>> = None;
                    {
                        let mut table = wtx.open_table($def)?;
                        // 续扫用包含式 RangeFrom：游标 = 本块最后一条已被消费的过期行
                        // （已在同事务内删除）。每次块推进严格增大游标 ⇒ 无重复
                        // 消费、无死循环。
                        let start: &[u8] = cursor.as_deref().unwrap_or(&[]);
                        let range: std::ops::RangeFrom<&[u8]> = start..;
                        let mut it = table.extract_from_if(range, $is_dead)?;
                        // 只消费到块上限：未读到的条目不会被删除，下块从
                        // last_key 之后继续（严格前进，无重复消费）。
                        for item in it.by_ref() {
                            let (k, v) = item?;
                            let key = k.value().to_vec();
                            Self::account_remove_tx(&wtx, $tid, &key, ($stored_len)(v.value()))?;
                            last_key = Some(key);
                            processed += 1;
                            if processed >= REAP_SWEEP_CHUNK {
                                break;
                            }
                        }
                    }
                    wtx.commit()?;
                    removed_total += processed as u64;
                    if processed < REAP_SWEEP_CHUNK {
                        // 区间已耗尽
                        break;
                    }
                    cursor = last_key;
                }
                removed_total
            }};
        }

        let mut removed = 0u64;
        removed += sweep_one!(
            STRING_TABLE,
            TID_STRING,
            |_k: &[u8], v: &[u8]| decode_value(v).is_none(),
            |v: &[u8]| v.len()
        );
        removed += sweep_one!(
            HASH_TABLE,
            TID_HASH,
            |_k: &[u8], v: &[u8]| decode_value(v).is_none(),
            |v: &[u8]| v.len()
        );
        removed += sweep_one!(
            LIST_TABLE,
            TID_LIST,
            |_k: &[u8], v: &[u8]| decode_value(v).is_none(),
            |v: &[u8]| v.len()
        );
        removed += sweep_one!(
            SET_TABLE,
            TID_SET,
            |_k: &[u8], exp: u64| is_expired(exp),
            |_v: u64| 8usize
        );
        Ok(removed)
    }

    /// 淘汰一批「最旧写入」条目（最多 `limit` 条，单事务），**恰好收敛到
    /// `target_max` 内即停**（不多淘汰一条）。返回 (条数, 字节)。
    ///
    /// 淘汰序 = 最后写入序（近似 LRU：写刷新、读不刷新）。索引悬挂（数据行缺失）
    /// 按记账偏差兜底处理：清索引行并照常扣账（饱和到 0 并告警）。
    fn evict_oldest_batch(&self, limit: usize, target_max: u64) -> ServiceResult<(u64, u64)> {
        let wtx = self.write_tx()?;
        let cur = {
            let meta = wtx.open_table(CACHE_META_TABLE)?;
            let x = meta.get(META_ACTIVE_BYTES)?.map(|v| v.value()).unwrap_or(0);
            x
        };
        let needed = cur.saturating_sub(target_max);
        if needed == 0 {
            return Ok((0, 0));
        }
        // 先读后删：evict 表在迭代期间不能同时被修改
        let mut oldest: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        {
            let ev = wtx.open_table(CACHE_EVICT_TABLE)?;
            for item in ev.iter()?.take(limit) {
                let (k, v) = item?;
                oldest.push((k.value().to_vec(), v.value().to_vec()));
            }
        }
        if oldest.is_empty() {
            return Ok((0, 0));
        }
        let mut count = 0u64;
        let mut bytes = 0u64;
        for (seq_raw, encoded) in &oldest {
            let Some((tid, key, size)) = decode_evict_value(encoded) else {
                return Err("cache evict index row is malformed; refusing to continue".into());
            };
            match tid {
                TID_STRING => {
                    let mut t = wtx.open_table(STRING_TABLE)?;
                    t.remove(key)?;
                }
                TID_HASH => {
                    let mut t = wtx.open_table(HASH_TABLE)?;
                    t.remove(key)?;
                }
                TID_LIST => {
                    let mut t = wtx.open_table(LIST_TABLE)?;
                    t.remove(key)?;
                }
                TID_SET => {
                    let mut t = wtx.open_table(SET_TABLE)?;
                    t.remove(key)?;
                }
                other => {
                    return Err(format!("cache evict index has unknown table id {other}").into());
                }
            }
            {
                let mut ev = wtx.open_table(CACHE_EVICT_TABLE)?;
                ev.remove(seq_raw.as_slice())?;
            }
            {
                let mut rev = wtx.open_table(CACHE_EVICT_REV_TABLE)?;
                rev.remove(evict_rev_key(tid, key).as_slice())?;
            }
            count += 1;
            bytes += size;
            if bytes >= needed {
                break;
            }
        }
        // 扣账（饱和：悬挂索引的兜底策略 —— 落到 0 并告警，不得死锁回收路径）
        let mut meta = wtx.open_table(CACHE_META_TABLE)?;
        if bytes > cur {
            tracing::warn!(
                bytes,
                cur,
                "cache evict: accounting underflow repaired by saturating to 0"
            );
            meta.insert(META_ACTIVE_BYTES, 0u64)?;
        } else {
            meta.insert(META_ACTIVE_BYTES, cur - bytes)?;
        }
        drop(meta);
        wtx.commit()?;
        Ok((count, bytes))
    }

    /// 为存量库（此前版本无记账表）一次性重建记账与淘汰索引。
    ///
    /// 先清空索引（幂等：迁移中途崩溃后重启可安全重做）；最后写入 meta 才算
    /// 完成 —— 在此之前每次启动都会重新重建。单事务完成（一次性升级成本）。
    fn rebuild_accounting(&self) -> ServiceResult<u64> {
        let wtx = self.write_tx()?;
        {
            let mut ev = wtx.open_table(CACHE_EVICT_TABLE)?;
            ev.retain(|_, _| false)?;
        }
        {
            let mut rev = wtx.open_table(CACHE_EVICT_REV_TABLE)?;
            rev.retain(|_, _| false)?;
        }
        let mut accounted = 0u64;
        let mut seq = 0u64;
        {
            let mut ev = wtx.open_table(CACHE_EVICT_TABLE)?;
            let mut rev = wtx.open_table(CACHE_EVICT_REV_TABLE)?;
            macro_rules! index_table {
                ($def:expr, $tid:expr, $stored_len:expr) => {{
                    let table = wtx.open_table($def)?;
                    for item in table.iter()? {
                        let (k, v) = item?;
                        let key = k.value();
                        let size = Self::entry_size(key.len(), ($stored_len)(v.value()));
                        accounted += size;
                        seq += 1;
                        let sk = evict_seq_key(seq);
                        let encoded = encode_evict_value($tid, key, size);
                        ev.insert(sk.as_slice(), encoded.as_slice())?;
                        rev.insert(evict_rev_key($tid, key).as_slice(), seq)?;
                    }
                }};
            }
            index_table!(STRING_TABLE, TID_STRING, |v: &[u8]| v.len());
            index_table!(HASH_TABLE, TID_HASH, |v: &[u8]| v.len());
            index_table!(LIST_TABLE, TID_LIST, |v: &[u8]| v.len());
            index_table!(SET_TABLE, TID_SET, |_v: u64| 8usize);
        }
        {
            let mut meta = wtx.open_table(CACHE_META_TABLE)?;
            meta.insert(META_ACTIVE_BYTES, accounted)?;
            meta.insert(META_NEXT_EVICT_SEQ, seq)?;
        }
        wtx.commit()?;
        Ok(accounted)
    }

    /// 启动时初始化记账：已存在 ⇒ 直接采用；缺失（旧库升级）⇒ 全量重建。
    fn ensure_accounting_initialized(&self) -> ServiceResult<u64> {
        let existing = {
            let rtx = self.read_tx()?;
            let meta = rtx.open_table(CACHE_META_TABLE)?;
            meta.get(META_ACTIVE_BYTES)?.map(|v| v.value())
        };
        if let Some(v) = existing {
            return Ok(v);
        }
        let rebuilt = self.rebuild_accounting()?;
        tracing::info!(
            accounted_bytes = rebuilt,
            "CacheService: rebuilt capacity accounting + eviction index for existing database \
             (one-time upgrade; see boundaries.md B-PL-3)"
        );
        Ok(rebuilt)
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
            tracing::debug!("CacheService: self_arc not bound; background reaper not spawned");
            return;
        };
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::warn!("CacheService: no tokio runtime; background reaper not spawned");
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
                            "cache reaper pass failed \
                             (metric: coord_agent_cache_reaper_faults_total)"
                        );
                    }
                    Err(join) => {
                        me.record_reap_fault();
                        tracing::error!(error = %join, "cache reaper blocking task failed");
                    }
                }
            }
        });
    }

    fn read_tx(&self) -> ServiceResult<redb::ReadTransaction> {
        let guard = self.db.read();
        let db = guard.as_ref().ok_or("CacheService not started")?;
        Ok(db.begin_read()?)
    }

    fn write_tx(&self) -> ServiceResult<redb::WriteTransaction> {
        let guard = self.db.read();
        let db = guard.as_ref().ok_or("CacheService not started")?;
        Ok(db.begin_write()?)
    }

    // ──── String 操作 ────

    pub fn string_put(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl_secs: Option<u64>,
    ) -> ServiceResult<()> {
        let ttl = ttl_secs.unwrap_or(self.default_ttl_secs);
        let encoded = encode_value(&value, ttl);
        self.ensure_entry_fits(key.as_bytes(), &encoded)?;
        let wtx = self.write_tx()?;
        let old_len = {
            let mut table = wtx.open_table(STRING_TABLE)?;
            let x = table
                .insert(key.as_bytes(), encoded.as_slice())?
                .map(|v| v.value().len());
            x
        };
        Self::account_upsert_tx(&wtx, TID_STRING, key.as_bytes(), old_len, encoded.len())?;
        wtx.commit()?;
        Ok(())
    }

    pub fn string_get(&self, key: &str) -> ServiceResult<Option<Vec<u8>>> {
        let rtx = self.read_tx()?;
        let raw: Option<Vec<u8>> = {
            let table = rtx.open_table(STRING_TABLE)?;
            table.get(key.as_bytes())?.map(|v| v.value().to_vec())
        };
        drop(rtx);

        match raw {
            Some(raw) => match decode_value(&raw) {
                Some(val) => Ok(Some(val)),
                None => {
                    let _ = self.string_delete(key);
                    Ok(None)
                }
            },
            None => Ok(None),
        }
    }

    pub fn string_delete(&self, key: &str) -> ServiceResult<bool> {
        let wtx = self.write_tx()?;
        let removed_len = {
            let mut table = wtx.open_table(STRING_TABLE)?;
            let x = table.remove(key.as_bytes())?.map(|v| v.value().len());
            x
        };
        if let Some(len) = removed_len {
            Self::account_remove_tx(&wtx, TID_STRING, key.as_bytes(), len)?;
        }
        wtx.commit()?;
        Ok(removed_len.is_some())
    }

    pub fn string_exists(&self, key: &str) -> ServiceResult<bool> {
        self.string_get(key).map(|v| v.is_some())
    }

    // ──── Hash 操作 ────

    pub fn hash_field_put(
        &self,
        key: &str,
        field: &str,
        value: Vec<u8>,
        ttl_secs: Option<u64>,
    ) -> ServiceResult<()> {
        let ttl = ttl_secs.unwrap_or(self.default_ttl_secs);
        let encoded = encode_value(&value, ttl);
        let hk = encode_hash_key(key, field);
        self.ensure_entry_fits(&hk, &encoded)?;
        let wtx = self.write_tx()?;
        let old_len = {
            let mut table = wtx.open_table(HASH_TABLE)?;
            let x = table
                .insert(hk.as_slice(), encoded.as_slice())?
                .map(|v| v.value().len());
            x
        };
        Self::account_upsert_tx(&wtx, TID_HASH, &hk, old_len, encoded.len())?;
        wtx.commit()?;
        Ok(())
    }

    pub fn hash_field_get(&self, key: &str, field: &str) -> ServiceResult<Option<Vec<u8>>> {
        let hk = encode_hash_key(key, field);
        let rtx = self.read_tx()?;
        let raw: Option<Vec<u8>> = {
            let table = rtx.open_table(HASH_TABLE)?;
            table.get(hk.as_slice())?.map(|v| v.value().to_vec())
        };
        drop(rtx);

        match raw {
            Some(raw) => match decode_value(&raw) {
                Some(val) => Ok(Some(val)),
                None => {
                    let _ = self.hash_field_delete(key, field);
                    Ok(None)
                }
            },
            None => Ok(None),
        }
    }

    pub fn hash_get_all(&self, key: &str) -> ServiceResult<BTreeMap<String, Vec<u8>>> {
        let prefix = hash_key_prefix(key);
        let plen = prefix.len();
        let rtx = self.read_tx()?;
        let (result, expired): (BTreeMap<String, Vec<u8>>, Vec<String>) = {
            let table = rtx.open_table(HASH_TABLE)?;
            let mut m = BTreeMap::new();
            let mut ex = Vec::new();
            let range: std::ops::RangeFrom<&[u8]> = prefix.as_slice()..;
            for item in table.range(range)? {
                let (k, raw) = item?;
                let k = k.value();
                if !k.starts_with(&prefix) || k.len() <= plen {
                    break;
                }
                let field = String::from_utf8_lossy(&k[plen..]).to_string();
                match decode_value(raw.value()) {
                    Some(val) => {
                        m.insert(field, val);
                    }
                    None => {
                        ex.push(field);
                    }
                }
            }
            (m, ex)
        };
        drop(rtx);

        for f in &expired {
            let _ = self.hash_field_delete(key, f);
        }

        Ok(result)
    }

    pub fn hash_field_delete(&self, key: &str, field: &str) -> ServiceResult<bool> {
        let hk = encode_hash_key(key, field);
        let wtx = self.write_tx()?;
        let removed_len = {
            let mut table = wtx.open_table(HASH_TABLE)?;
            let x = table.remove(hk.as_slice())?.map(|v| v.value().len());
            x
        };
        if let Some(len) = removed_len {
            Self::account_remove_tx(&wtx, TID_HASH, &hk, len)?;
        }
        wtx.commit()?;
        Ok(removed_len.is_some())
    }

    pub fn hash_field_count(&self, key: &str) -> ServiceResult<u64> {
        let prefix = hash_key_prefix(key);
        let rtx = self.read_tx()?;
        let table = rtx.open_table(HASH_TABLE)?;
        let mut count = 0u64;
        let range: std::ops::RangeFrom<&[u8]> = prefix.as_slice()..;
        for item in table.range(range)? {
            let (k, raw) = item?;
            let k = k.value();
            if !k.starts_with(&prefix) {
                break;
            }
            if decode_value(raw.value()).is_some() {
                count += 1;
            }
        }
        Ok(count)
    }

    // ──── List 操作 ────

    pub fn list_push_right(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl_secs: Option<u64>,
    ) -> ServiceResult<()> {
        let ttl = ttl_secs.unwrap_or(self.default_ttl_secs);
        let encoded = encode_value(&value, ttl);
        let prefix = list_key_prefix(key);
        let plen = prefix.len();

        let rtx = self.read_tx()?;
        let max_idx = {
            let table = rtx.open_table(LIST_TABLE)?;
            let mut last = -1i64;
            let range: std::ops::RangeFrom<&[u8]> = prefix.as_slice()..;
            for item in table.range(range)? {
                let (k, _) = item?;
                let k = k.value();
                if !k.starts_with(&prefix) {
                    break;
                }
                if let Some(idx) = decode_list_index(k, plen) {
                    last = last.max(idx);
                }
            }
            last
        };
        drop(rtx);

        let lk = encode_list_key(key, max_idx + 1);
        self.ensure_entry_fits(&lk, &encoded)?;
        let wtx = self.write_tx()?;
        let old_len = {
            let mut table = wtx.open_table(LIST_TABLE)?;
            let x = table
                .insert(lk.as_slice(), encoded.as_slice())?
                .map(|v| v.value().len());
            x
        };
        Self::account_upsert_tx(&wtx, TID_LIST, &lk, old_len, encoded.len())?;
        wtx.commit()?;
        Ok(())
    }

    pub fn list_push_left(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl_secs: Option<u64>,
    ) -> ServiceResult<()> {
        let ttl = ttl_secs.unwrap_or(self.default_ttl_secs);
        let encoded = encode_value(&value, ttl);
        let prefix = list_key_prefix(key);
        let plen = prefix.len();

        let rtx = self.read_tx()?;
        let min_idx = {
            let table = rtx.open_table(LIST_TABLE)?;
            let mut first: Option<i64> = None;
            let range: std::ops::RangeFrom<&[u8]> = prefix.as_slice()..;
            for item in table.range(range)? {
                let (k, _) = item?;
                let k = k.value();
                if !k.starts_with(&prefix) {
                    break;
                }
                if let Some(idx) = decode_list_index(k, plen) {
                    if first.is_none_or(|f| idx < f) {
                        first = Some(idx);
                    }
                }
            }
            first.unwrap_or(0)
        };
        drop(rtx);

        let lk = encode_list_key(key, min_idx - 1);
        self.ensure_entry_fits(&lk, &encoded)?;
        let wtx = self.write_tx()?;
        let old_len = {
            let mut table = wtx.open_table(LIST_TABLE)?;
            let x = table
                .insert(lk.as_slice(), encoded.as_slice())?
                .map(|v| v.value().len());
            x
        };
        Self::account_upsert_tx(&wtx, TID_LIST, &lk, old_len, encoded.len())?;
        wtx.commit()?;
        Ok(())
    }

    /// 原子出队（单写事务）：
    /// 同一写事务内迭代前缀 range 找极值（跳过已过期）→ 事务内删除 → commit。
    /// redb 单写者模型保证写事务串行化，消除并发重复出队 / 丢元素。
    /// 已知代价（R3）：大 list 下 O(n) 定位极值，首版接受。
    fn list_pop_atomic(&self, key: &str, find_max: bool) -> ServiceResult<Option<Vec<u8>>> {
        let prefix = list_key_prefix(key);
        let plen = prefix.len();

        let wtx = self.write_tx()?;
        let mut removed: Option<(Vec<u8>, usize)> = None;
        let popped: Option<Vec<u8>> = {
            let mut table = wtx.open_table(LIST_TABLE)?;
            // 写事务内找极值（跳过已过期）
            let mut best: Option<(i64, Vec<u8>, usize)> = None;
            let range: std::ops::RangeFrom<&[u8]> = prefix.as_slice()..;
            for item in table.range(range)? {
                let (k, raw) = item?;
                let k = k.value();
                if !k.starts_with(&prefix) {
                    break;
                }
                if let Some(idx) = decode_list_index(k, plen) {
                    let is_better = match &best {
                        Some((b, _, _)) => {
                            if find_max {
                                idx > *b
                            } else {
                                idx < *b
                            }
                        }
                        None => true,
                    };
                    if is_better {
                        let stored_len = raw.value().len();
                        if let Some(val) = decode_value(raw.value()) {
                            best = Some((idx, val, stored_len));
                        }
                    }
                }
            }
            // 事务内删除（记账随后、同事务提交）
            match best {
                Some((idx, val, stored_len)) => {
                    let lk = encode_list_key(key, idx);
                    table.remove(lk.as_slice())?;
                    removed = Some((lk, stored_len));
                    Some(val)
                }
                None => None,
            }
        };
        if let Some((lk, stored_len)) = &removed {
            Self::account_remove_tx(&wtx, TID_LIST, lk, *stored_len)?;
        }
        wtx.commit()?;
        Ok(popped)
    }

    pub fn list_pop_right(&self, key: &str) -> ServiceResult<Option<Vec<u8>>> {
        self.list_pop_atomic(key, true)
    }

    pub fn list_pop_left(&self, key: &str) -> ServiceResult<Option<Vec<u8>>> {
        self.list_pop_atomic(key, false)
    }

    pub fn list_range(&self, key: &str, start: i64, end: i64) -> ServiceResult<Vec<Vec<u8>>> {
        let prefix = list_key_prefix(key);
        let plen = prefix.len();
        let rtx = self.read_tx()?;
        let mut items: Vec<(i64, Vec<u8>)> = {
            let table = rtx.open_table(LIST_TABLE)?;
            let mut v = Vec::new();
            let range: std::ops::RangeFrom<&[u8]> = prefix.as_slice()..;
            for item in table.range(range)? {
                let (k, raw) = item?;
                let k = k.value();
                if !k.starts_with(&prefix) {
                    break;
                }
                if let Some(idx) = decode_list_index(k, plen) {
                    if let Some(val) = decode_value(raw.value()) {
                        v.push((idx, val));
                    }
                }
            }
            v
        };
        drop(rtx);

        items.sort_by_key(|(idx, _)| *idx);
        let len = items.len() as i64;
        let end = if end < 0 { len + end + 1 } else { end.min(len) };
        let start = start.max(0);
        Ok(items
            .into_iter()
            .skip(start as usize)
            .take((end - start).max(0) as usize)
            .map(|(_, v)| v)
            .collect())
    }

    pub fn list_length(&self, key: &str) -> ServiceResult<u64> {
        let prefix = list_key_prefix(key);
        let rtx = self.read_tx()?;
        let table = rtx.open_table(LIST_TABLE)?;
        let mut count = 0u64;
        let range: std::ops::RangeFrom<&[u8]> = prefix.as_slice()..;
        for item in table.range(range)? {
            let (k, raw) = item?;
            let k = k.value();
            if !k.starts_with(&prefix) {
                break;
            }
            if decode_value(raw.value()).is_some() {
                count += 1;
            }
        }
        Ok(count)
    }

    // ──── Set 操作 ────

    pub fn set_add(
        &self,
        key: &str,
        member: Vec<u8>,
        ttl_secs: Option<u64>,
    ) -> ServiceResult<bool> {
        let ttl = ttl_secs.unwrap_or(self.default_ttl_secs);
        let expires_at = encode_ttl(ttl);
        let sk = encode_set_key(key, &member);
        let exp_bytes = expires_at.to_be_bytes();
        self.ensure_entry_fits(&sk, &exp_bytes)?;
        let wtx = self.write_tx()?;
        let existed = {
            let table = wtx.open_table(SET_TABLE)?;
            let x = table.get(sk.as_slice())?.is_some();
            x
        };
        if !existed {
            let old_len = {
                let mut table = wtx.open_table(SET_TABLE)?;
                let x = table.insert(sk.as_slice(), expires_at)?.map(|_| 8usize);
                x
            };
            Self::account_upsert_tx(&wtx, TID_SET, &sk, old_len, 8)?;
        }
        wtx.commit()?;
        Ok(!existed)
    }

    pub fn set_remove(&self, key: &str, member: &[u8]) -> ServiceResult<bool> {
        let sk = encode_set_key(key, member);
        let wtx = self.write_tx()?;
        let removed = {
            let mut table = wtx.open_table(SET_TABLE)?;
            let x = table.remove(sk.as_slice())?.is_some();
            x
        };
        if removed {
            Self::account_remove_tx(&wtx, TID_SET, &sk, 8)?;
        }
        wtx.commit()?;
        Ok(removed)
    }

    pub fn set_contains(&self, key: &str, member: &[u8]) -> ServiceResult<bool> {
        let sk = encode_set_key(key, member);
        let rtx = self.read_tx()?;
        let expires_at: Option<u64> = {
            let table = rtx.open_table(SET_TABLE)?;
            table.get(sk.as_slice())?.map(|v| v.value())
        };
        drop(rtx);
        match expires_at {
            Some(exp) if !is_expired(exp) => Ok(true),
            Some(_) => {
                let _ = self.set_remove(key, member);
                Ok(false)
            }
            None => Ok(false),
        }
    }

    pub fn set_members(&self, key: &str) -> ServiceResult<Vec<Vec<u8>>> {
        let prefix = set_key_prefix(key);
        let plen = prefix.len();
        let rtx = self.read_tx()?;
        let (members, expired): (Vec<Vec<u8>>, Vec<Vec<u8>>) = {
            let table = rtx.open_table(SET_TABLE)?;
            let mut m = Vec::new();
            let mut ex = Vec::new();
            let range: std::ops::RangeFrom<&[u8]> = prefix.as_slice()..;
            for item in table.range(range)? {
                let (k, exp) = item?;
                let k = k.value();
                if !k.starts_with(&prefix) {
                    break;
                }
                let member = decode_set_member(k, plen);
                if is_expired(exp.value()) {
                    ex.push(member);
                } else {
                    m.push(member);
                }
            }
            (m, ex)
        };
        drop(rtx);
        for m in &expired {
            let _ = self.set_remove(key, m);
        }
        Ok(members)
    }

    pub fn set_cardinality(&self, key: &str) -> ServiceResult<u64> {
        let prefix = set_key_prefix(key);
        let rtx = self.read_tx()?;
        let table = rtx.open_table(SET_TABLE)?;
        let mut count = 0u64;
        let range: std::ops::RangeFrom<&[u8]> = prefix.as_slice()..;
        for item in table.range(range)? {
            let (k, exp) = item?;
            let k = k.value();
            if !k.starts_with(&prefix) {
                break;
            }
            if !is_expired(exp.value()) {
                count += 1;
            }
        }
        Ok(count)
    }

    // ──── 分片元数据 ────

    pub fn set_shard_meta(&self, shard_id: &str, meta: CacheShardMeta) -> ServiceResult<()> {
        let json = serde_json::to_vec(&meta)?;
        let wtx = self.write_tx()?;
        {
            let mut table = wtx.open_table(SHARD_TABLE)?;
            table.insert(shard_id, json.as_slice())?;
        }
        wtx.commit()?;
        Ok(())
    }

    pub fn get_shard_meta(&self, shard_id: &str) -> ServiceResult<Option<CacheShardMeta>> {
        let rtx = self.read_tx()?;
        let raw: Option<Vec<u8>> = {
            let table = rtx.open_table(SHARD_TABLE)?;
            table.get(shard_id)?.map(|v| v.value().to_vec())
        };
        drop(rtx);
        match raw {
            Some(raw) => Ok(Some(serde_json::from_slice(&raw)?)),
            None => Ok(None),
        }
    }

    pub fn list_shards(&self) -> ServiceResult<Vec<CacheShardMeta>> {
        let rtx = self.read_tx()?;
        let table = rtx.open_table(SHARD_TABLE)?;
        let mut shards = Vec::new();
        for item in table.iter()? {
            let (_, raw) = item?;
            shards.push(serde_json::from_slice(raw.value())?);
        }
        Ok(shards)
    }

    // ──── 运维操作 ────

    pub fn stats(&self) -> ServiceResult<CacheStats> {
        let rtx = self.read_tx()?;

        let string_count = {
            let table = rtx.open_table(STRING_TABLE)?;
            let mut count = 0u64;
            for item in table.iter()? {
                let (_, raw) = item?;
                if decode_value(raw.value()).is_some() {
                    count += 1;
                }
            }
            count
        };

        let (hash_count, list_count, set_count, shard_count) = {
            let mut seen_hash = std::collections::HashSet::new();
            let mut seen_list = std::collections::HashSet::new();
            let mut seen_set = std::collections::HashSet::new();

            {
                let table = rtx.open_table(HASH_TABLE)?;
                for item in table.iter()? {
                    let (k, raw) = item?;
                    let k = k.value();
                    if decode_value(raw.value()).is_some() {
                        if let Some(pos) = k.iter().position(|&b| b == 0) {
                            seen_hash.insert(k[..pos].to_vec());
                        }
                    }
                }
            }
            {
                let table = rtx.open_table(LIST_TABLE)?;
                for item in table.iter()? {
                    let (k, _) = item?;
                    let k = k.value();
                    if let Some(pos) = k.iter().position(|&b| b == 0) {
                        seen_list.insert(k[..pos].to_vec());
                    }
                }
            }
            {
                let table = rtx.open_table(SET_TABLE)?;
                for item in table.iter()? {
                    let (k, exp) = item?;
                    let k = k.value();
                    if !is_expired(exp.value()) {
                        if let Some(pos) = k.iter().position(|&b| b == 0) {
                            seen_set.insert(k[..pos].to_vec());
                        }
                    }
                }
            }
            let sc = {
                let table = rtx.open_table(SHARD_TABLE)?;
                table.iter()?.count() as u64
            };
            (
                seen_hash.len() as u64,
                seen_list.len() as u64,
                seen_set.len() as u64,
                sc,
            )
        };

        let total_size_bytes = {
            let meta = rtx.open_table(CACHE_META_TABLE)?;
            meta.get(META_ACTIVE_BYTES)?.map(|v| v.value()).unwrap_or(0)
        };

        Ok(CacheStats {
            string_count,
            hash_count,
            list_count,
            set_count,
            shard_count,
            total_size_bytes,
        })
    }

    pub fn flush_all(&self) -> ServiceResult<()> {
        // 单写事务：数据表 + 索引表清空、记账归零，原子生效。
        // （此前分表多事务仅为规避借用冲突；记账要求与数据同事务，故合并。）
        let wtx = self.write_tx()?;
        {
            let mut t = wtx.open_table(STRING_TABLE)?;
            t.retain(|_, _| false)?;
        }
        {
            let mut t = wtx.open_table(HASH_TABLE)?;
            t.retain(|_, _| false)?;
        }
        {
            let mut t = wtx.open_table(LIST_TABLE)?;
            t.retain(|_, _| false)?;
        }
        {
            let mut t = wtx.open_table(SET_TABLE)?;
            t.retain(|_, _| false)?;
        }
        {
            let mut t = wtx.open_table(SHARD_TABLE)?;
            t.retain(|_, _| false)?;
        }
        {
            let mut t = wtx.open_table(CACHE_EVICT_TABLE)?;
            t.retain(|_, _| false)?;
        }
        {
            let mut t = wtx.open_table(CACHE_EVICT_REV_TABLE)?;
            t.retain(|_, _| false)?;
        }
        {
            let mut meta = wtx.open_table(CACHE_META_TABLE)?;
            meta.insert(META_ACTIVE_BYTES, 0u64)?;
        }
        wtx.commit()?;
        Ok(())
    }
}

// ──── ISR 复制（已落地）────
//
// 复制日志 / 持久化幂等键 / 本地序列号在本服务 redb 内与数据写同事务提交。
// 复制条目携带 Leader 计算的**物理 key**（含 list index / hash field / set member）
// 与 **绝对到期时间戳**（encode_value 内嵌 expires_at），Follower 原样应用。
// pop 仅 Leader 执行：Leader 单事务内原子出队 + 记录 CacheDelete 复制条目。

use crate::services::replication::{
    IdempotencyKey, ReplicatedStore, ReplicationEntry, ReplicationError, ReplicationOp,
};

/// Cache 单 shard
const CACHE_SHARD: &str = "cache";

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

impl CacheService {
    /// 生成缓存写幂等键（基于物理 key，全局唯一）
    fn cache_idem_key(op: &str, key: &[u8]) -> IdempotencyKey {
        IdempotencyKey::new(
            format!("cache:{op}:{}", String::from_utf8_lossy(key)),
            now_secs(),
        )
    }

    /// 下一序列号（= 本地最后序列号 + 1）
    fn next_sequence(&self) -> ServiceResult<u64> {
        let rtx = self.read_tx()?;
        let table = rtx.open_table(CACHE_REPL_LOCAL_SEQ)?;
        let seq = match table.get(CACHE_SHARD.as_bytes())? {
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
        let mut t = wtx.open_table(CACHE_REPL_ENTRY_TABLE)?;
        t.insert(rk.as_slice(), encoded.as_slice())?;

        let ik = entry.idempotency_key.to_string();
        let mut at = wtx.open_table(CACHE_REPL_APPLIED_KEYS)?;
        at.insert(ik.as_bytes(), ())?;

        let sk = entry.shard_id.as_bytes();
        let mut lt = wtx.open_table(CACHE_REPL_LOCAL_SEQ)?;
        let cur = match lt.get(sk)? {
            Some(v) => v.value(),
            None => 0,
        };
        lt.insert(sk, cur.max(entry.sequence_num))?;
        Ok(())
    }

    /// 单事务应用复制条目：幂等检查 + 数据 op（CachePut / CacheDelete）+ 簿记。
    /// Leader 本地提交与 Follower 应用共用此路径（数据一致）。
    fn apply_op_tx(
        &self,
        wtx: &redb::WriteTransaction,
        entry: &ReplicationEntry,
    ) -> ServiceResult<()> {
        // 幂等检查（持久化键）
        let ik = entry.idempotency_key.to_string();
        let applied = {
            let t = wtx.open_table(CACHE_REPL_APPLIED_KEYS)?;
            let x = t.get(ik.as_bytes())?.is_some();
            x
        };
        if applied {
            return Ok(());
        }
        match &entry.operation {
            ReplicationOp::CachePut {
                key,
                value,
                data_type,
            } => match data_type.as_str() {
                "string" => {
                    let old_len = {
                        let mut t = wtx.open_table(STRING_TABLE)?;
                        let x = t
                            .insert(key.as_slice(), value.as_slice())?
                            .map(|v| v.value().len());
                        x
                    };
                    Self::account_upsert_tx(wtx, TID_STRING, key, old_len, value.len())?;
                }
                "hash" => {
                    let old_len = {
                        let mut t = wtx.open_table(HASH_TABLE)?;
                        let x = t
                            .insert(key.as_slice(), value.as_slice())?
                            .map(|v| v.value().len());
                        x
                    };
                    Self::account_upsert_tx(wtx, TID_HASH, key, old_len, value.len())?;
                }
                "list" => {
                    let old_len = {
                        let mut t = wtx.open_table(LIST_TABLE)?;
                        let x = t
                            .insert(key.as_slice(), value.as_slice())?
                            .map(|v| v.value().len());
                        x
                    };
                    Self::account_upsert_tx(wtx, TID_LIST, key, old_len, value.len())?;
                }
                "set" => {
                    if value.len() != 8 {
                        return Err("invalid set expires_at encoding (need 8 bytes)".into());
                    }
                    let Ok(exp_bytes) = value[..8].try_into() else {
                        return Err("invalid set expires_at encoding (need 8 bytes)".into());
                    };
                    let exp = u64::from_be_bytes(exp_bytes);
                    let old_len = {
                        let mut t = wtx.open_table(SET_TABLE)?;
                        let x = t.insert(key.as_slice(), exp)?.map(|_| 8usize);
                        x
                    };
                    Self::account_upsert_tx(wtx, TID_SET, key, old_len, 8)?;
                }
                other => return Err(format!("unknown cache data_type '{other}'").into()),
            },
            ReplicationOp::CacheDelete { key, data_type } => match data_type.as_str() {
                "string" => {
                    let removed_len = {
                        let mut t = wtx.open_table(STRING_TABLE)?;
                        let x = t.remove(key.as_slice())?.map(|v| v.value().len());
                        x
                    };
                    if let Some(len) = removed_len {
                        Self::account_remove_tx(wtx, TID_STRING, key, len)?;
                    }
                }
                "hash" => {
                    let removed_len = {
                        let mut t = wtx.open_table(HASH_TABLE)?;
                        let x = t.remove(key.as_slice())?.map(|v| v.value().len());
                        x
                    };
                    if let Some(len) = removed_len {
                        Self::account_remove_tx(wtx, TID_HASH, key, len)?;
                    }
                }
                "list" => {
                    let removed_len = {
                        let mut t = wtx.open_table(LIST_TABLE)?;
                        let x = t.remove(key.as_slice())?.map(|v| v.value().len());
                        x
                    };
                    if let Some(len) = removed_len {
                        Self::account_remove_tx(wtx, TID_LIST, key, len)?;
                    }
                }
                "set" => {
                    let removed = {
                        let mut t = wtx.open_table(SET_TABLE)?;
                        let x = t.remove(key.as_slice())?.is_some();
                        x
                    };
                    if removed {
                        Self::account_remove_tx(wtx, TID_SET, key, 8)?;
                    }
                }
                other => return Err(format!("unknown cache data_type '{other}'").into()),
            },
            _ => return Err("cache cannot apply MqPublish op".into()),
        }
        Self::write_repl_bookkeeping_tx(wtx, entry)?;
        Ok(())
    }

    /// Leader 本地单事务应用（幂等 + 数据 + 簿记）
    fn replicated_apply_local(&self, entry: &ReplicationEntry) -> ServiceResult<()> {
        let wtx = self.write_tx()?;
        self.apply_op_tx(&wtx, entry)?;
        wtx.commit()?;
        Ok(())
    }

    /// 通用 Leader 复制写：构建 entry → 单事务本地应用 → 推送 ISR Followers →
    /// min_isr 校验（同步复制）。返回 None 表示成功。
    ///
    /// **顺序是契约的一部分**：本地提交 → 推送 ISR → `ensure_isr` 校验。
    /// 因此本函数返回 `Err` 时，**本地写入可能已经生效** —— 调用方不得把错误读作
    /// "未写入"。改成"先复制再本地提交"会把读己之写（read-your-write）与幂等回放
    /// 一起打破，且跨节点原子提交本身不在 v0.2.0 的承诺面内（见模块头与
    /// `cache.proto` 的边界声明）。
    async fn replicated_write(&self, op: ReplicationOp) -> ServiceResult<()> {
        let rm = self
            .replication
            .read()
            .clone()
            .ok_or_else(|| "replication not enabled".to_string())?;
        if !rm.is_leader(CACHE_SHARD) {
            return Err(format!(
                "not leader for shard '{CACHE_SHARD}' (leader is {})",
                rm.shard_leader(CACHE_SHARD)
            )
            .into());
        }
        let seq = self.next_sequence()?;
        let ik = match &op {
            ReplicationOp::CachePut { key, .. } => Self::cache_idem_key("put", key),
            ReplicationOp::CacheDelete { key, .. } => Self::cache_idem_key("del", key),
            _ => return Err("cache replicated_write: unexpected op".into()),
        };
        let entry = ReplicationEntry {
            idempotency_key: ik,
            shard_id: CACHE_SHARD.to_string(),
            sequence_num: seq,
            operation: op,
        };
        self.replicated_apply_local(&entry)?;
        let acked = rm
            .push_to_followers(&entry)
            .await
            .map_err(|e| e.to_string())?;
        rm.ensure_isr(acked + 1).map_err(|e| e.to_string())?;
        Ok(())
    }

    /// List 最大 index（读路径，Leader 用于分配新元素物理 key）
    fn list_max_index(&self, key: &str) -> ServiceResult<i64> {
        let prefix = list_key_prefix(key);
        let plen = prefix.len();
        let rtx = self.read_tx()?;
        let table = rtx.open_table(LIST_TABLE)?;
        let mut last = -1i64;
        let range: std::ops::RangeFrom<&[u8]> = prefix.as_slice()..;
        for item in table.range(range)? {
            let (k, _) = item?;
            let k = k.value();
            if !k.starts_with(&prefix) {
                break;
            }
            if let Some(idx) = decode_list_index(k, plen) {
                last = last.max(idx);
            }
        }
        Ok(last)
    }

    /// List 最小 index（读路径，Leader 用于分配左推物理 key）
    fn list_min_index(&self, key: &str) -> ServiceResult<i64> {
        let prefix = list_key_prefix(key);
        let plen = prefix.len();
        let rtx = self.read_tx()?;
        let table = rtx.open_table(LIST_TABLE)?;
        let mut first: Option<i64> = None;
        let range: std::ops::RangeFrom<&[u8]> = prefix.as_slice()..;
        for item in table.range(range)? {
            let (k, _) = item?;
            let k = k.value();
            if !k.starts_with(&prefix) {
                break;
            }
            if let Some(idx) = decode_list_index(k, plen) {
                if first.is_none_or(|f| idx < f) {
                    first = Some(idx);
                }
            }
        }
        Ok(first.unwrap_or(0))
    }

    // ──── 复制写路径（gRPC handler 在复制启用时调用）────

    pub async fn string_put_replicated(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl_secs: Option<u64>,
    ) -> ServiceResult<()> {
        let ttl = ttl_secs.unwrap_or(self.default_ttl_secs);
        let encoded = encode_value(&value, ttl); // 含绝对到期时间戳
        self.ensure_entry_fits(key.as_bytes(), &encoded)?;
        self.replicated_write(ReplicationOp::CachePut {
            key: key.as_bytes().to_vec(),
            value: encoded,
            data_type: "string".to_string(),
        })
        .await
    }

    pub async fn string_delete_replicated(&self, key: &str) -> ServiceResult<bool> {
        let existed = self.string_exists(key)?;
        self.replicated_write(ReplicationOp::CacheDelete {
            key: key.as_bytes().to_vec(),
            data_type: "string".to_string(),
        })
        .await?;
        Ok(existed)
    }

    pub async fn hash_field_put_replicated(
        &self,
        key: &str,
        field: &str,
        value: Vec<u8>,
        ttl_secs: Option<u64>,
    ) -> ServiceResult<()> {
        let ttl = ttl_secs.unwrap_or(self.default_ttl_secs);
        let encoded = encode_value(&value, ttl);
        let hk = encode_hash_key(key, field);
        self.ensure_entry_fits(&hk, &encoded)?;
        self.replicated_write(ReplicationOp::CachePut {
            key: hk,
            value: encoded,
            data_type: "hash".to_string(),
        })
        .await
    }

    pub async fn list_push_right_replicated(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl_secs: Option<u64>,
    ) -> ServiceResult<()> {
        let ttl = ttl_secs.unwrap_or(self.default_ttl_secs);
        let encoded = encode_value(&value, ttl);
        let idx = self.list_max_index(key)? + 1;
        let lk = encode_list_key(key, idx);
        self.ensure_entry_fits(&lk, &encoded)?;
        self.replicated_write(ReplicationOp::CachePut {
            key: lk,
            value: encoded,
            data_type: "list".to_string(),
        })
        .await
    }

    pub async fn list_push_left_replicated(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl_secs: Option<u64>,
    ) -> ServiceResult<()> {
        let ttl = ttl_secs.unwrap_or(self.default_ttl_secs);
        let encoded = encode_value(&value, ttl);
        let idx = self.list_min_index(key)? - 1;
        let lk = encode_list_key(key, idx);
        self.ensure_entry_fits(&lk, &encoded)?;
        self.replicated_write(ReplicationOp::CachePut {
            key: lk,
            value: encoded,
            data_type: "list".to_string(),
        })
        .await
    }

    /// 复制原子出队（pop 仅 Leader 执行）。Leader 在单事务内
    /// 原子出队并记录 CacheDelete 复制条目；Follower 收到删除后本地移除。
    pub async fn list_pop_replicated(
        &self,
        key: &str,
        find_max: bool,
    ) -> ServiceResult<Option<Vec<u8>>> {
        let rm = self
            .replication
            .read()
            .clone()
            .ok_or_else(|| "replication not enabled".to_string())?;
        if !rm.is_leader(CACHE_SHARD) {
            return Err(format!(
                "not leader for shard '{CACHE_SHARD}' (leader is {})",
                rm.shard_leader(CACHE_SHARD)
            )
            .into());
        }
        let seq = self.next_sequence()?;
        let prefix = list_key_prefix(key);
        let plen = prefix.len();
        let wtx = self.write_tx()?;
        let result: ServiceResult<Option<(Vec<u8>, ReplicationEntry)>> = (|| {
            let mut table = wtx.open_table(LIST_TABLE)?;
            let mut best: Option<(i64, Vec<u8>, usize)> = None;
            let range: std::ops::RangeFrom<&[u8]> = prefix.as_slice()..;
            for item in table.range(range)? {
                let (k, raw) = item?;
                let k = k.value();
                if !k.starts_with(&prefix) {
                    break;
                }
                if let Some(idx) = decode_list_index(k, plen) {
                    let is_better = match &best {
                        Some((b, _, _)) => {
                            if find_max {
                                idx > *b
                            } else {
                                idx < *b
                            }
                        }
                        None => true,
                    };
                    if is_better {
                        let stored_len = raw.value().len();
                        if let Some(val) = decode_value(raw.value()) {
                            best = Some((idx, val, stored_len));
                        }
                    }
                }
            }
            match best {
                Some((idx, val, stored_len)) => {
                    let pkey = encode_list_key(key, idx);
                    table.remove(pkey.as_slice())?;
                    let entry = ReplicationEntry {
                        idempotency_key: Self::cache_idem_key("pop", &pkey),
                        shard_id: CACHE_SHARD.to_string(),
                        sequence_num: seq,
                        operation: ReplicationOp::CacheDelete {
                            key: pkey.clone(),
                            data_type: "list".to_string(),
                        },
                    };
                    drop(table);
                    Self::write_repl_bookkeeping_tx(&wtx, &entry)?;
                    Self::account_remove_tx(&wtx, TID_LIST, &pkey, stored_len)?;
                    Ok(Some((val, entry)))
                }
                None => Ok(None),
            }
        })();
        let popped = match result {
            Ok(v) => v,
            Err(e) => return Err(e),
        };
        wtx.commit()?;
        match popped {
            Some((val, entry)) => {
                let acked = rm
                    .push_to_followers(&entry)
                    .await
                    .map_err(|e| e.to_string())?;
                rm.ensure_isr(acked + 1).map_err(|e| e.to_string())?;
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }

    pub async fn set_add_replicated(
        &self,
        key: &str,
        member: Vec<u8>,
        ttl_secs: Option<u64>,
    ) -> ServiceResult<bool> {
        let ttl = ttl_secs.unwrap_or(self.default_ttl_secs);
        let expires_at = encode_ttl(ttl);
        let sk = encode_set_key(key, &member);
        let exp_bytes = expires_at.to_be_bytes();
        self.ensure_entry_fits(&sk, &exp_bytes)?;
        let existed = self.set_contains(key, &member)?;
        self.replicated_write(ReplicationOp::CachePut {
            key: sk,
            value: expires_at.to_be_bytes().to_vec(),
            data_type: "set".to_string(),
        })
        .await?;
        Ok(!existed)
    }
}

// ──── ReplicatedStore（复制存储接口实现）────

impl ReplicatedStore for CacheService {
    fn shards(&self) -> Vec<String> {
        vec![CACHE_SHARD.to_string()]
    }

    fn last_local_sequence(&self, shard: &str) -> u64 {
        let rtx = match self.read_tx() {
            Ok(t) => t,
            Err(_) => return 0,
        };
        let table = match rtx.open_table(CACHE_REPL_LOCAL_SEQ) {
            Ok(t) => t,
            Err(_) => return 0,
        };
        match table.get(shard.as_bytes()) {
            Ok(Some(v)) => v.value(),
            _ => 0,
        }
    }

    fn apply_entry(&self, entry: &ReplicationEntry) -> Result<(), ReplicationError> {
        let wtx = match self.write_tx() {
            Ok(t) => t,
            Err(e) => return Err(ReplicationError::Store(e.to_string())),
        };
        let result = self.apply_op_tx(&wtx, entry);
        match result {
            Ok(()) => {
                wtx.commit()
                    .map_err(|e| ReplicationError::Store(e.to_string()))?;
                Ok(())
            }
            Err(e) => Err(ReplicationError::Store(e.to_string())),
        }
    }

    fn read_entries(&self, shard: &str, from_seq: u64, limit: u64) -> Vec<ReplicationEntry> {
        let prefix = encode_repl_prefix(shard);
        let plen = prefix.len();
        let rtx = match self.read_tx() {
            Ok(t) => t,
            Err(_) => return Vec::new(),
        };
        let table = match rtx.open_table(CACHE_REPL_ENTRY_TABLE) {
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

// ──── BaseService trait 实现 ────

#[async_trait]
impl BaseService for CacheService {
    fn name(&self) -> &'static str {
        "cache"
    }

    async fn start(&self) -> ServiceResult<()> {
        if *self.started.read() {
            return Ok(());
        }

        let db_path = self.db_path.join("cache.redb");
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
            wtx.open_table(STRING_TABLE)?;
            wtx.open_table(HASH_TABLE)?;
            wtx.open_table(LIST_TABLE)?;
            wtx.open_table(SET_TABLE)?;
            wtx.open_table(SHARD_TABLE)?;
            wtx.open_table(CACHE_REPL_ENTRY_TABLE)?;
            wtx.open_table(CACHE_REPL_APPLIED_KEYS)?;
            wtx.open_table(CACHE_REPL_LOCAL_SEQ)?;
            wtx.open_table(CACHE_META_TABLE)?;
            wtx.open_table(CACHE_EVICT_TABLE)?;
            wtx.open_table(CACHE_EVICT_REV_TABLE)?;
        }
        wtx.commit()?;

        *self.db.write() = Some(db);
        // 记账初始化（旧库无记账 ⇒ 启动时一次性重建，含淘汰索引）——
        // 必须在 started=true / reaper 挂载之前完成；失败时回退 db 句柄，
        // 避免重试 start 时对仍打开的文件重开（redb 会拒绝）。
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
            "CacheService started (capacity accounting active; see boundaries.md B-PL-3)"
        );
        Ok(())
    }

    async fn stop(&self) -> ServiceResult<()> {
        if !*self.started.read() {
            return Ok(());
        }
        *self.db.write() = None;
        *self.started.write() = false;
        tracing::info!("CacheService stopped");
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

    fn new_svc(dir: &TempDir, ttl: u64) -> CacheService {
        let svc = CacheService::new(dir.path().to_path_buf(), 1024 * 1024, ttl);
        // Auto-start for unit tests
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        rt.block_on(async { svc.start().await.expect("start") });
        svc
    }

    fn new_svc_with_max(dir: &TempDir, max_size_bytes: u64, ttl: u64) -> CacheService {
        let svc = CacheService::new(dir.path().to_path_buf(), max_size_bytes, ttl);
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        rt.block_on(async { svc.start().await.expect("start") });
        svc
    }

    #[test]
    fn test_name_and_default_state() {
        let dir = temp_dir();
        let svc = CacheService::new(dir.path().to_path_buf(), 1024 * 1024, 3600);
        assert_eq!(svc.name(), "cache");
        // Not started yet
        assert!(!svc.health_check());
    }

    #[test]
    fn test_string_put_get() {
        let dir = temp_dir();
        let svc = new_svc(&dir, 3600);
        svc.string_put("hello", b"world".to_vec(), None).unwrap();
        assert_eq!(svc.string_get("hello").unwrap(), Some(b"world".to_vec()));
    }

    #[test]
    fn test_string_get_missing() {
        let dir = temp_dir();
        let svc = new_svc(&dir, 3600);
        assert_eq!(svc.string_get("nope").unwrap(), None);
    }

    #[test]
    fn test_string_delete() {
        let dir = temp_dir();
        let svc = new_svc(&dir, 3600);
        svc.string_put("k", b"v".to_vec(), None).unwrap();
        assert!(svc.string_delete("k").unwrap());
        assert!(!svc.string_delete("k").unwrap());
        assert_eq!(svc.string_get("k").unwrap(), None);
    }

    #[test]
    fn test_hash_operations() {
        let dir = temp_dir();
        let svc = new_svc(&dir, 3600);
        svc.hash_field_put("user:1", "name", b"Alice".to_vec(), None)
            .unwrap();
        svc.hash_field_put("user:1", "age", b"30".to_vec(), None)
            .unwrap();
        assert_eq!(
            svc.hash_field_get("user:1", "name").unwrap(),
            Some(b"Alice".to_vec())
        );
        let all = svc.hash_get_all("user:1").unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(svc.hash_field_count("user:1").unwrap(), 2);
        assert!(svc.hash_field_delete("user:1", "name").unwrap());
        assert_eq!(svc.hash_field_count("user:1").unwrap(), 1);
    }

    #[test]
    fn test_list_operations() {
        let dir = temp_dir();
        let svc = new_svc(&dir, 3600);
        svc.list_push_right("l", b"a".to_vec(), None).unwrap();
        svc.list_push_right("l", b"b".to_vec(), None).unwrap();
        svc.list_push_right("l", b"c".to_vec(), None).unwrap();
        assert_eq!(svc.list_length("l").unwrap(), 3);
        assert_eq!(svc.list_range("l", 0, -1).unwrap(), vec![b"a", b"b", b"c"]);
        assert_eq!(svc.list_pop_left("l").unwrap(), Some(b"a".to_vec()));
        assert_eq!(svc.list_pop_right("l").unwrap(), Some(b"c".to_vec()));
        assert_eq!(svc.list_length("l").unwrap(), 1);
    }

    /// 并发 pop 原子性（RED→GREEN）
    ///
    /// 不变量：无重复、无丢失、恰好 total 个、队列最终清空。
    /// **不得**用读事务找极值 + 写事务删除（两次提交）：高竞争下会
    /// 重复出队（两个线程读到同一极值）→ unique < total。
    /// 单写事务实现由 redb 单写者模型保证串行化 → 确定性通过。
    #[test]
    fn test_list_pop_concurrent_atomic() {
        use std::collections::HashSet;
        use std::sync::{Arc, Barrier};
        use std::thread;

        let dir = temp_dir();
        let svc = Arc::new(new_svc(&dir, 3600));

        let total: usize = 200;
        for i in 0..total {
            svc.list_push_right("q", i.to_le_bytes().to_vec(), None)
                .unwrap();
        }

        let threads = 8;
        let barrier = Arc::new(Barrier::new(threads));
        let mut handles = vec![];
        for _ in 0..threads {
            let svc = svc.clone();
            let barrier = barrier.clone();
            handles.push(thread::spawn(move || {
                barrier.wait(); // 同步起跑，放大竞争窗口
                let mut got = Vec::new();
                while let Some(v) = svc.list_pop_right("q").unwrap() {
                    got.push(v);
                }
                got
            }));
        }
        let mut results: Vec<Vec<u8>> = Vec::new();
        for h in handles {
            results.extend(h.join().unwrap());
        }

        let unique: HashSet<Vec<u8>> = results.iter().cloned().collect();
        assert_eq!(
            results.len(),
            total,
            "无丢失：应恰好 pop {} 个，实际 {}（重复出队会使总数膨胀）",
            total,
            results.len()
        );
        assert_eq!(
            unique.len(),
            total,
            "无重复：应恰好 {} 个唯一值，实际 {}",
            total,
            unique.len()
        );
        assert_eq!(svc.list_length("q").unwrap(), 0, "队列应被完全清空");
    }

    #[test]
    fn test_set_operations() {
        let dir = temp_dir();
        let svc = new_svc(&dir, 3600);
        assert!(svc.set_add("s", b"m1".to_vec(), None).unwrap());
        assert!(!svc.set_add("s", b"m1".to_vec(), None).unwrap());
        assert!(svc.set_add("s", b"m2".to_vec(), None).unwrap());
        assert_eq!(svc.set_cardinality("s").unwrap(), 2);
        assert!(svc.set_contains("s", b"m1").unwrap());
        assert!(!svc.set_contains("s", b"m3").unwrap());
        let mut members = svc.set_members("s").unwrap();
        members.sort();
        assert_eq!(members, vec![b"m1".to_vec(), b"m2".to_vec()]);
        assert!(svc.set_remove("s", b"m1").unwrap());
        assert_eq!(svc.set_cardinality("s").unwrap(), 1);
    }

    #[test]
    fn test_persistence() {
        let dir = temp_dir();
        let db_path = dir.path().to_path_buf();
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        {
            let svc = CacheService::new(db_path.clone(), 1024 * 1024, 3600);
            rt.block_on(async { svc.start().await.expect("start") });
            svc.string_put("pk", b"pv".to_vec(), None).unwrap();
        }
        {
            let svc = CacheService::new(db_path.clone(), 1024 * 1024, 3600);
            rt.block_on(async { svc.start().await.expect("start") });
            assert_eq!(svc.string_get("pk").unwrap(), Some(b"pv".to_vec()));
        }
    }

    #[test]
    fn test_flush_all() {
        let dir = temp_dir();
        let svc = new_svc(&dir, 3600);
        svc.string_put("k", b"v".to_vec(), None).unwrap();
        svc.set_add("s", b"m".to_vec(), None).unwrap();
        assert!(svc.accounted_bytes().unwrap() > 0);
        svc.flush_all().unwrap();
        assert_eq!(svc.string_get("k").unwrap(), None);
        assert_eq!(svc.set_cardinality("s").unwrap(), 0);
        // flush 后记账归零，且后续写/删与索引仍然一致（不会残留悬挂索引）
        assert_eq!(svc.accounted_bytes().unwrap(), 0);
        svc.string_put("k2", b"v2".to_vec(), None).unwrap();
        assert_eq!(
            svc.accounted_bytes().unwrap(),
            2 + (8 + 2) /* key + stored value */
        );
        assert!(svc.string_delete("k2").unwrap());
        assert_eq!(svc.accounted_bytes().unwrap(), 0);
    }

    #[test]
    fn test_shard_metadata() {
        let dir = temp_dir();
        let svc = new_svc(&dir, 3600);
        svc.set_shard_meta(
            "shard-1",
            CacheShardMeta {
                shard_id: "shard-1".into(),
                leader_agent: "a:9500".into(),
                replicas: vec!["b:9500".into()],
                key_range_start: vec![0],
                key_range_end: vec![127],
            },
        )
        .unwrap();
        let meta = svc.get_shard_meta("shard-1").unwrap().unwrap();
        assert_eq!(meta.leader_agent, "a:9500");
        assert_eq!(svc.list_shards().unwrap().len(), 1);
    }

    // ── 容量上界（B-PL-3）：记账 / 淘汰 / reaper ──
    //
    // 负控制（提交前已实跑，破坏后还原）：
    // - 移除 `reap_once` 中的淘汰循环 ⇒ `test_reaper_enforces_max_size` 必红；
    // - 移除 `reap_once` 中的 `sweep_expired` 调用 ⇒
    //   `test_reaper_reclaims_expired_bytes` 必红；
    // - 移除 `string_put` 的 `account_upsert_tx` 调用 ⇒
    //   `test_accounting_bytes_tracked` 必红；
    // - 移除 `start` 中的 `spawn_reaper` ⇒ `test_background_reaper_converges` 必红。

    /// 记账口径：单条 = 物理 key 长度 + 存储值长度（含 8 字节 TTL 前缀）。
    /// 负控制见本段头注释；不变量：增删改跨 4 表全部精确入账。
    #[test]
    fn test_accounting_bytes_tracked() {
        let dir = temp_dir();
        let svc = new_svc(&dir, 3600);
        assert_eq!(svc.accounted_bytes().unwrap(), 0);

        svc.string_put("k", b"v".to_vec(), None).unwrap(); // 1 + (8+1) = 10
        svc.hash_field_put("h", "f", b"abc".to_vec(), None).unwrap(); // hk=3 + (8+3)=11 → 14
        svc.list_push_right("l", b"xy".to_vec(), None).unwrap(); // lk=10 + (8+2)=10 → 20
        svc.set_add("s", b"m".to_vec(), None).unwrap(); // sk=3 + 8 → 11
        assert_eq!(svc.accounted_bytes().unwrap(), 55);
        assert_eq!(svc.stats().unwrap().total_size_bytes, 55);

        // 替换写：旧字节必须被扣减，不得重复累计
        svc.string_put("k", b"12345".to_vec(), None).unwrap(); // 1 + 13 = 14（旧10）
        assert_eq!(svc.accounted_bytes().unwrap(), 59);

        // 逐类型删除扣减
        assert!(svc.hash_field_delete("h", "f").unwrap());
        assert_eq!(svc.accounted_bytes().unwrap(), 45);
        assert_eq!(svc.list_pop_left("l").unwrap(), Some(b"xy".to_vec()));
        assert_eq!(svc.accounted_bytes().unwrap(), 25);
        assert!(svc.set_remove("s", b"m").unwrap());
        assert_eq!(svc.accounted_bytes().unwrap(), 14);
        assert!(svc.string_delete("k").unwrap());
        assert_eq!(svc.accounted_bytes().unwrap(), 0);

        // 全部清空后 reaper 无旧可淘、无过期可扫
        let stats = svc.reap_once().unwrap();
        assert_eq!(stats.expired_entries, 0);
        assert_eq!(stats.evicted_entries, 0);
    }

    /// 单条写超过上界必须**直接拒绝**（永不驻留）：写成功再驱逐等于假成功。
    #[test]
    fn test_entry_over_limit_rejected() {
        let dir = temp_dir();
        let svc = new_svc_with_max(&dir, 256, 3600);
        let err = svc
            .string_put("big", vec![0u8; 300], None)
            .expect_err("oversized entry must be rejected");
        assert!(
            err.to_string().contains("max_size_bytes"),
            "错误信息需含 max_size_bytes 锚点（handler 映射 RESOURCE_EXHAUSTED）: {err}"
        );
        assert_eq!(svc.accounted_bytes().unwrap(), 0, "拒绝不得产生任何写入");
        // 能装下的正常写入不受影响（2 + (8+10) = 20 ≤ 256）
        svc.string_put("ok", b"0123456789".to_vec(), None).unwrap();
        assert_eq!(svc.accounted_bytes().unwrap(), 20);
        svc.string_delete("ok").unwrap();
        assert_eq!(svc.accounted_bytes().unwrap(), 0);
    }

    /// 上界断言（负控制目标）：持续写入超界负载后，reaper 一轮后
    /// 记账值必须收敛到 ≤ max；淘汰序 = 最后写入序（最旧的先走）。
    #[test]
    fn test_reaper_enforces_max_size() {
        let dir = temp_dir();
        let max = 4096u64;
        let svc = new_svc_with_max(&dir, max, 3600);
        // 每条：3B key + (8B TTL 前缀 + 512B value) = 523B
        for i in 0..20 {
            svc.string_put(&format!("k{i:02}"), vec![7u8; 512], None)
                .unwrap();
        }
        assert_eq!(svc.accounted_bytes().unwrap(), 20 * 523);
        assert!(svc.accounted_bytes().unwrap() > max);

        let stats = svc.reap_once().unwrap();
        // 20*523 = 10460；淘汰 13 条后 3661 ≤ 4096（第 14 条会降到 3138 也 ≤，
        // 但循环在首次满足上界即停）
        assert_eq!(stats.evicted_entries, 13);
        assert!(
            stats.active_bytes <= max,
            "上界断言: {} > {max}",
            stats.active_bytes
        );
        assert_eq!(svc.accounted_bytes().unwrap(), 7 * 523);
        // 最旧先走、最新保留
        assert_eq!(svc.string_get("k00").unwrap(), None);
        assert_eq!(svc.string_get("k12").unwrap(), None);
        assert_eq!(svc.string_get("k13").unwrap(), Some(vec![7u8; 512]));
        assert_eq!(svc.string_get("k19").unwrap(), Some(vec![7u8; 512]));
        // 幂等：已在界内 ⇒ 再跑一轮不再淘汰
        let again = svc.reap_once().unwrap();
        assert_eq!(again.evicted_entries, 0);
        assert_eq!(svc.accounted_bytes().unwrap(), 7 * 523);
    }

    /// TTL 过期必须计入字节回收：① 惰性路径（读命中过期）立即扣减；
    /// ② reaper 清扫无人访问的过期行（含 set 表）。
    #[test]
    fn test_reaper_reclaims_expired_bytes() {
        let dir = temp_dir();
        let svc = new_svc(&dir, 3600);
        svc.string_put("lazy", b"v".to_vec(), Some(1)).unwrap();
        svc.string_put("sweep-str", b"v".to_vec(), Some(1)).unwrap();
        svc.hash_field_put("sweep-h", "f", b"v".to_vec(), Some(1))
            .unwrap();
        svc.list_push_right("sweep-l", b"v".to_vec(), Some(1))
            .unwrap();
        svc.set_add("sweep-s", b"m".to_vec(), Some(1)).unwrap();
        let before = svc.accounted_bytes().unwrap();
        assert!(before > 0);

        std::thread::sleep(std::time::Duration::from_millis(1_200));

        // ① 惰性：读命中过期 ⇒ 删除并扣账（"lazy"：4 + (8+1) = 13）
        assert_eq!(svc.string_get("lazy").unwrap(), None);
        assert_eq!(svc.accounted_bytes().unwrap(), before - 13);

        // ② reaper 清扫其余 4 条（含 hash/list/set）
        let stats = svc.reap_once().unwrap();
        assert_eq!(stats.expired_entries, 4);
        assert_eq!(stats.active_bytes, 0);
        assert_eq!(svc.accounted_bytes().unwrap(), 0);
        let counters = svc.reap_counters();
        assert_eq!(counters.passes, 1);
        assert_eq!(counters.expired_entries, 4);
        assert_eq!(counters.faults, 0);
    }

    /// 记账跨重启持久化（redb 同事务真值）：重启后继续增删不漂移。
    #[test]
    fn test_accounting_persists_across_restart() {
        let dir = temp_dir();
        let db_path = dir.path().to_path_buf();
        let expected;
        {
            let svc = new_svc_with_max(&dir, 4096, 3600);
            svc.string_put("pk", b"persist".to_vec(), None).unwrap();
            svc.set_add("ps", b"m".to_vec(), None).unwrap();
            expected = svc.accounted_bytes().unwrap();
            assert!(expected > 0);
            // drop（未 stop）——模拟重启前崩溃窗口
        }
        let svc = CacheService::new(db_path, 4096, 3600);
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async { svc.start().await.expect("restart") });
        assert_eq!(svc.accounted_bytes().unwrap(), expected);
        assert!(svc.string_delete("pk").unwrap());
        assert_eq!(svc.accounted_bytes().unwrap(), expected - (2 + 15));
        // reaper 在重启后的库上仍可工作（索引未丢失）
        assert_eq!(svc.reap_once().unwrap().evicted_entries, 0);
    }

    /// 旧库迁移：无记账/索引的存量库启动时重建（含淘汰索引），
    /// 之后删除/淘汰与记账保持一致（不欠账、不悬挂）。
    #[test]
    fn test_migration_rebuilds_accounting_for_legacy_db() {
        let dir = temp_dir();
        let db_path = dir.path().join("cache.redb");

        // 手工构造「旧版本」库：只有数据表，无 cache:meta / cache:evict*
        {
            let db = redb::Database::create(&db_path).unwrap();
            let wtx = db.begin_write().unwrap();
            {
                let mut t = wtx.open_table(STRING_TABLE).unwrap();
                t.insert(b"old".as_slice(), encode_value(b"v", 0).as_slice())
                    .unwrap(); // 3 + 9 = 12
            }
            {
                let mut t = wtx.open_table(HASH_TABLE).unwrap();
                let hk = encode_hash_key("h", "f");
                t.insert(hk.as_slice(), encode_value(b"vv", 0).as_slice())
                    .unwrap(); // 3 + 10 = 13
            }
            {
                let mut t = wtx.open_table(LIST_TABLE).unwrap();
                let lk = encode_list_key("l", 0);
                t.insert(lk.as_slice(), encode_value(b"vvv", 0).as_slice())
                    .unwrap(); // 10 + 11 = 21
            }
            {
                let mut t = wtx.open_table(SET_TABLE).unwrap();
                let sk = encode_set_key("s", b"m");
                t.insert(sk.as_slice(), 0u64).unwrap(); // 3 + 8 = 11
            }
            wtx.commit().unwrap();
        }

        let svc = new_svc_with_max(&dir, 40, 3600);
        assert_eq!(svc.accounted_bytes().unwrap(), 57, "启动时应重建记账");
        // 重建的索引可被淘汰使用：按重建序（string → hash → list → set）
        // 淘汰最旧两条 12+13 后 32 ≤ 40
        let stats = svc.reap_once().unwrap();
        assert_eq!(stats.evicted_entries, 2);
        assert_eq!(svc.accounted_bytes().unwrap(), 32);
        assert_eq!(svc.string_get("old").unwrap(), None);
        assert_eq!(
            svc.hash_field_get("h", "f").unwrap(),
            None,
            "hash 条目应被淘汰"
        );
        assert_eq!(svc.list_range("l", 0, -1).unwrap(), vec![b"vvv".to_vec()]);
        assert!(svc.set_contains("s", b"m").unwrap());
    }

    /// ISR 复制应用路径同样记账（apply_op_tx 是 Leader 本地提交与 Follower
    /// 应用的共用路径）：put 入账、delete 扣账。
    #[test]
    fn test_replicated_apply_updates_accounting() {
        use crate::services::replication::{
            IdempotencyKey, ReplicatedStore, ReplicationEntry, ReplicationOp,
        };
        let dir = temp_dir();
        let svc = new_svc(&dir, 3600);

        let put = ReplicationEntry {
            idempotency_key: IdempotencyKey::new("t:put".to_string(), 1),
            shard_id: "cache".to_string(),
            sequence_num: 1,
            operation: ReplicationOp::CachePut {
                key: b"rk".to_vec(),
                value: encode_value(b"rv", 0),
                data_type: "string".to_string(),
            },
        };
        svc.apply_entry(&put).unwrap();
        assert_eq!(
            svc.accounted_bytes().unwrap(),
            2 + 10,
            "2B key + 8+2B value"
        );
        // 幂等重放不重复记账
        svc.apply_entry(&put).unwrap();
        assert_eq!(svc.accounted_bytes().unwrap(), 12);

        let del = ReplicationEntry {
            idempotency_key: IdempotencyKey::new("t:del".to_string(), 1),
            shard_id: "cache".to_string(),
            sequence_num: 2,
            operation: ReplicationOp::CacheDelete {
                key: b"rk".to_vec(),
                data_type: "string".to_string(),
            },
        };
        svc.apply_entry(&del).unwrap();
        assert_eq!(svc.accounted_bytes().unwrap(), 0);
    }

    /// 后台 reaper 闭环：绑定 self_arc + 短周期后，持续写入超界数据
    /// **无需手动调用** reap，记账值应在一个周期量级内收敛到上界内。
    /// 负控制：移除 `start` 中的 `spawn_reaper` ⇒ 本测试必红（超时）。
    #[test]
    fn test_background_reaper_converges() {
        use std::sync::Arc;
        let dir = temp_dir();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let svc = Arc::new(CacheService::new(dir.path().to_path_buf(), 2048, 3600));
        svc.bind_self_weak(&svc);
        svc.set_reaper_interval(std::time::Duration::from_millis(50));
        rt.block_on(async { svc.start().await.expect("start") });

        for i in 0..10 {
            svc.string_put(&format!("bg{i}"), vec![9u8; 512], None)
                .unwrap(); // 每条约 523B ⇒ 总量 5230 > 2048
        }
        assert!(svc.accounted_bytes().unwrap() > 2048);

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let accounted = svc.accounted_bytes().unwrap();
            if accounted <= 2048 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "后台 reaper 未在期限内收敛: accounted={accounted}"
            );
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert!(svc.reap_counters().passes > 0);
    }

    // ── 属性测试：存储值编解码（非可信字节解析路径） ──
    //
    // 负控制：移除 `decode_value` 的 `raw.len() < 8` 检查 ⇒
    // `prop_decode_value_truncated_prefix_is_none` 必红（切片 panic）。
    use proptest::prelude::*;

    proptest! {
        /// 往返：任意 value + 未来 TTL ⇒ 原样返回（8 字节前缀不吞字节）。
        #[test]
        fn prop_decode_value_roundtrip(
            value in proptest::collection::vec(any::<u8>(), 0..1024),
            ttl in 60u64..86_400,
        ) {
            let encoded = encode_value(&value, ttl);
            prop_assert!(encoded.len() >= 8);
            let decoded = decode_value(&encoded);
            prop_assert_eq!(decoded.as_deref(), Some(value.as_slice()));
        }

        /// ttl=0（永不过期）与任意 value 的往返。
        #[test]
        fn prop_decode_value_zero_ttl_roundtrip(
            value in proptest::collection::vec(any::<u8>(), 0..1024),
        ) {
            let encoded = encode_value(&value, 0);
            let decoded = decode_value(&encoded);
            prop_assert_eq!(decoded.as_deref(), Some(value.as_slice()));
        }

        /// 截断（<8 字节前缀）必须是 None，且不得 panic。
        #[test]
        fn prop_decode_value_truncated_prefix_is_none(
            raw in proptest::collection::vec(any::<u8>(), 0..8),
        ) {
            prop_assert!(decode_value(&raw).is_none());
        }

        /// 任意字节串不得 panic；一旦 Some，返回的必须是前缀之后的原样字节。
        #[test]
        fn prop_decode_value_arbitrary_bytes_never_panics(
            raw in proptest::collection::vec(any::<u8>(), 0..2048),
        ) {
            if let Some(v) = decode_value(&raw) {
                prop_assert!(raw.len() >= 8);
                prop_assert_eq!(v, raw[8..].to_vec());
            }
        }

        /// 过期前缀（expires_at=1，已是过去）必须判过期：不得漏出 value。
        #[test]
        fn prop_decode_value_expired_prefix_is_none(
            value in proptest::collection::vec(any::<u8>(), 0..1024),
        ) {
            let mut raw = 1u64.to_be_bytes().to_vec();
            raw.extend_from_slice(&value);
            prop_assert!(decode_value(&raw).is_none());
        }
    }
}

// ──── CacheBackend / CacheConfig / MokaCacheService ────

use std::collections::HashSet;
use std::sync::Arc;

/// 缓存后端选择
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheBackend {
    /// redb 持久化嵌入式数据库（默认）
    Redb { data_dir: String },
    /// moka 纯内存缓存（高性能，可容忍丢失）
    Moka {
        max_capacity: u64,
        time_to_live: Option<std::time::Duration>,
    },
}

impl Default for CacheBackend {
    fn default() -> Self {
        CacheBackend::Redb {
            data_dir: "/var/lib/coord-agent/cache".into(),
        }
    }
}

/// 缓存配置
#[derive(Debug, Clone, Default)]
pub struct CacheConfig {
    pub backend: CacheBackend,
}

/// Moka 缓存服务 — 纯内存缓存后端
///
/// 支持 String/Hash/List/Set 四种数据类型，可选 TTL。
/// 数据不持久化，适合极高读写性能、可容忍丢失的场景。
///
/// 线程安全：所有数据结构被 Arc + parking_lot::Mutex 保护。
pub struct MokaCacheService {
    string_cache: moka::sync::Cache<String, Vec<u8>>,
    hash_cache: moka::sync::Cache<String, Vec<u8>>,
    list_cache: Arc<parking_lot::Mutex<BTreeMap<String, Vec<Vec<u8>>>>>,
    set_cache: Arc<parking_lot::Mutex<BTreeMap<String, HashSet<Vec<u8>>>>>,
    string_count: Arc<std::sync::atomic::AtomicU64>,
}

impl MokaCacheService {
    /// 使用 CacheConfig 创建 MokaCacheService
    pub fn new(config: CacheConfig) -> Self {
        let (max_capacity, ttl) = match config.backend {
            CacheBackend::Moka {
                max_capacity,
                time_to_live,
            } => (max_capacity, time_to_live),
            _ => (1000, None), // fallback
        };

        let mut string_builder = moka::sync::Cache::builder().max_capacity(max_capacity);
        let mut hash_builder = moka::sync::Cache::builder().max_capacity(max_capacity * 4);

        if let Some(ttl) = ttl {
            string_builder = string_builder.time_to_live(ttl);
            hash_builder = hash_builder.time_to_live(ttl);
        }

        Self {
            string_cache: string_builder.build(),
            hash_cache: hash_builder.build(),
            list_cache: Arc::new(parking_lot::Mutex::new(BTreeMap::new())),
            set_cache: Arc::new(parking_lot::Mutex::new(BTreeMap::new())),
            string_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    // ──── String 操作 ────

    pub fn string_set(&self, key: &str, value: &[u8]) -> crate::service::ServiceResult<()> {
        self.string_cache.insert(key.to_string(), value.to_vec());
        self.string_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    pub fn string_get(&self, key: &str) -> crate::service::ServiceResult<Option<Vec<u8>>> {
        Ok(self.string_cache.get(&key.to_string()))
    }

    pub fn string_delete(&self, key: &str) -> crate::service::ServiceResult<bool> {
        let existed = self.string_cache.remove(&key.to_string()).is_some();
        if existed {
            self.string_count
                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(existed)
    }

    pub fn string_exists(&self, key: &str) -> crate::service::ServiceResult<bool> {
        Ok(self.string_cache.contains_key(&key.to_string()))
    }

    // ──── Hash 操作 ────

    fn hash_compound_key(key: &str, field: &str) -> String {
        format!("{key}\x00{field}")
    }

    pub fn hash_field_set(
        &self,
        key: &str,
        field: &str,
        value: &[u8],
    ) -> crate::service::ServiceResult<()> {
        let ck = Self::hash_compound_key(key, field);
        self.hash_cache.insert(ck, value.to_vec());
        // Track field in auxiliary index
        let index_key = format!("_hash_idx:{key}");
        let mut sets = self.set_cache.lock();
        sets.entry(index_key)
            .or_default()
            .insert(field.as_bytes().to_vec());
        Ok(())
    }

    pub fn hash_field_get(
        &self,
        key: &str,
        field: &str,
    ) -> crate::service::ServiceResult<Option<Vec<u8>>> {
        let ck = Self::hash_compound_key(key, field);
        Ok(self.hash_cache.get(&ck))
    }

    pub fn hash_get_all(
        &self,
        key: &str,
    ) -> crate::service::ServiceResult<BTreeMap<String, Vec<u8>>> {
        let index_key = format!("_hash_idx:{key}");
        let fields: Vec<String> = {
            let sets = self.set_cache.lock();
            sets.get(&index_key)
                .map(|s| {
                    s.iter()
                        .filter_map(|b| String::from_utf8(b.clone()).ok())
                        .collect()
                })
                .unwrap_or_default()
        };

        let mut result = BTreeMap::new();
        for field in &fields {
            if let Some(val) = self.hash_field_get(key, field)? {
                result.insert(field.clone(), val);
            }
        }
        Ok(result)
    }

    pub fn hash_field_delete(&self, key: &str, field: &str) -> crate::service::ServiceResult<bool> {
        let ck = Self::hash_compound_key(key, field);
        let existed = self.hash_cache.remove(&ck).is_some();
        if existed {
            // Remove from auxiliary index
            let index_key = format!("_hash_idx:{key}");
            let mut sets = self.set_cache.lock();
            if let Some(s) = sets.get_mut(&index_key) {
                s.remove(field.as_bytes());
            }
        }
        Ok(existed)
    }

    // ──── List 操作 ────

    pub fn list_push_left(&self, key: &str, value: Vec<u8>) -> crate::service::ServiceResult<()> {
        let mut lists = self.list_cache.lock();
        lists.entry(key.to_string()).or_default().insert(0, value);
        Ok(())
    }

    pub fn list_push_right(&self, key: &str, value: Vec<u8>) -> crate::service::ServiceResult<()> {
        let mut lists = self.list_cache.lock();
        lists.entry(key.to_string()).or_default().push(value);
        Ok(())
    }

    pub fn list_pop_left(&self, key: &str) -> crate::service::ServiceResult<Option<Vec<u8>>> {
        let mut lists = self.list_cache.lock();
        if let Some(list) = lists.get_mut(key) {
            if list.is_empty() {
                Ok(None)
            } else {
                Ok(Some(list.remove(0)))
            }
        } else {
            Ok(None)
        }
    }

    pub fn list_pop_right(&self, key: &str) -> crate::service::ServiceResult<Option<Vec<u8>>> {
        let mut lists = self.list_cache.lock();
        if let Some(list) = lists.get_mut(key) {
            Ok(list.pop())
        } else {
            Ok(None)
        }
    }

    pub fn list_len(&self, key: &str) -> crate::service::ServiceResult<usize> {
        let lists = self.list_cache.lock();
        Ok(lists.get(key).map(|l| l.len()).unwrap_or(0))
    }

    pub fn list_range(
        &self,
        key: &str,
        start: usize,
        end: usize,
    ) -> crate::service::ServiceResult<Vec<Vec<u8>>> {
        let lists = self.list_cache.lock();
        if let Some(list) = lists.get(key) {
            let end = end.min(list.len());
            if start >= end {
                return Ok(vec![]);
            }
            Ok(list[start..end].to_vec())
        } else {
            Ok(vec![])
        }
    }

    // ──── Set 操作 ────

    pub fn set_add(&self, key: &str, member: &[u8]) -> crate::service::ServiceResult<bool> {
        let mut sets = self.set_cache.lock();
        Ok(sets
            .entry(key.to_string())
            .or_default()
            .insert(member.to_vec()))
    }

    pub fn set_remove(&self, key: &str, member: &[u8]) -> crate::service::ServiceResult<bool> {
        let mut sets = self.set_cache.lock();
        Ok(sets.get_mut(key).map(|s| s.remove(member)).unwrap_or(false))
    }

    pub fn set_contains(&self, key: &str, member: &[u8]) -> crate::service::ServiceResult<bool> {
        let sets = self.set_cache.lock();
        Ok(sets.get(key).map(|s| s.contains(member)).unwrap_or(false))
    }

    pub fn set_members(&self, key: &str) -> crate::service::ServiceResult<Vec<Vec<u8>>> {
        let sets = self.set_cache.lock();
        Ok(sets
            .get(key)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default())
    }

    // ──── Stats ────

    pub fn stats(&self) -> CacheStats {
        CacheStats {
            string_count: self.string_count.load(std::sync::atomic::Ordering::Relaxed),
            hash_count: 0,
            list_count: self.list_cache.lock().len() as u64,
            set_count: self.set_cache.lock().len() as u64,
            shard_count: 0,
            total_size_bytes: 0,
        }
    }
}
