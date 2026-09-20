// Coord Server — gRPC 服务实现
//
// 实现 5 个 gRPC 服务（KV/Lease/Watch/Txn/Maintenance），
// 对接底层 Raft 共识 + StateMachine + LeaseManager + WatchDispatcher + Barrier。
//
// CoordNode 是服务端核心结构体，持有所有组件的引用。
// 写请求（Put/Delete/Txn）通过 Raft 共识提交，读请求直接访问本地状态机。

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use coord_proto::kv::{
    kv_server::Kv, DeleteRequest, DeleteResponse, KeyValue, PutRequest, PutResponse, RangeRequest,
    RangeResponse,
};
use coord_proto::lease::{
    lease_server::Lease, LeaseGrantRequest, LeaseGrantResponse, LeaseKeepAliveRequest,
    LeaseKeepAliveResponse, LeaseRevokeRequest, LeaseRevokeResponse,
};
use coord_proto::maintenance::{
    maintenance_server::Maintenance, CompactRequest, CompactResponse, JoinRequest, JoinResponse,
    MemberAddRequest, MemberAddResponse, MemberListRequest, MemberListResponse, MemberNode,
    MemberPromoteRequest, MemberPromoteResponse, MemberRemoveRequest, MemberRemoveResponse,
    SealRequest, SealResponse, SnapshotRequest, SnapshotResponse, StatusRequest, StatusResponse,
    UnsealRequest, UnsealResponse,
};
use coord_proto::txn::{txn_server::Txn, Compare, RequestOp, ResponseOp, TxnRequest, TxnResponse};
use coord_proto::watch::{watch_server::Watch, WatchEvent, WatchRequest, WatchResponse};

use coord_core::kv_range::RangeSemantics;
use coord_core::types::RegionId;

use crate::auth::service::AuthOpProposer;
use crate::lease::LeaseManager;
use crate::raft::log_store::LogStore;
use crate::raft::region::{RegionHandle, RegionManager};
use crate::raft::type_config::{AuthOp, Command, LeaseOp, Response};
use crate::raft::{CoordRaft, ReadPolicy, WatchReceiver};
use crate::security::barrier::Barrier;
use crate::security::key_management::{EncryptedDek, Keyring};
use crate::storage::mvcc::{AppliedLogId, MvccStorage};
use crate::storage::redb_backend::RedbBackend;
use crate::txn::{TxnCompare, TxnOp, TxnOpResponse};
use crate::watch::WatchDispatcher;

/// 对象存储 gRPC 服务实现（coord.storage，EXPERIMENTAL 数据面）。
/// 见 `storage/object_store.rs` 与 docs/volume-object-storage.md 决策记录。
pub mod object_storage;

// ──── CoordNode ────

/// R-SVC-18：运行时资源限制（per-RPC 超时、规模上限、幂等缓存参数）。
///
/// 由配置层（`coord` CLI 的 `[limits]` 段）构造后经 `CoordNode::set_limits`
/// 注入；默认值对齐生产保守口径（读/写 5s、Range 1 万、Txn 128 op）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeLimits {
    /// 读路径超时（线性一致性读 + 本地扫描）
    pub read_timeout: std::time::Duration,
    /// 写路径 raft 提交超时（Put/Delete/Txn/Auth 管理操作）
    pub write_timeout: std::time::Duration,
    /// Lease 写路径超时（Grant/Revoke/KeepAlive）
    pub lease_timeout: std::time::Duration,
    /// Compact 提案超时
    pub compact_timeout: std::time::Duration,
    /// Range 单次扫描上限（0 = 不限制；客户端显式 limit 超过该值 → INVALID_ARGUMENT）
    pub max_range_limit: usize,
    /// Txn compare + success + failure 操作数上限（0 = 不限制）
    pub max_txn_ops: usize,
    /// 幂等缓存条目 TTL
    pub idempotency_ttl: std::time::Duration,
    /// 幂等缓存容量上限（FIFO 淘汰最旧条目）
    pub idempotency_max_entries: usize,
}

impl Default for RuntimeLimits {
    fn default() -> Self {
        Self {
            read_timeout: std::time::Duration::from_secs(5),
            write_timeout: std::time::Duration::from_secs(5),
            lease_timeout: std::time::Duration::from_secs(5),
            compact_timeout: std::time::Duration::from_secs(10),
            max_range_limit: 10_000,
            max_txn_ops: 128,
            idempotency_ttl: std::time::Duration::from_secs(60),
            idempotency_max_entries: 4096,
        }
    }
}

/// 幂等请求去重缓存条目
#[derive(Debug, Clone)]
struct IdempotentEntry {
    /// 写入缓存的时间（TTL 依据）
    inserted_at: std::time::Instant,
    /// 上次响应返回的 revision
    revision: i64,
    /// 上次响应是否 succeeded（仅 Txn 使用）
    succeeded: bool,
    /// R-SVC-18：Txn 缓存的完整响应（命中时回放，此前返回空 responses）
    responses: Vec<ResponseOp>,
    /// F-02（jepsen T1.4）：Put 缓存的完整响应 —— `prev_kv` 必须与首次执行
    /// 一致。此前命中路径硬编码 `prev_kv: None`，依赖 prev_kv 做 CAS 后置校验
    /// / 审计的上层会把 None 读成「该 key 此前不存在」（静默的错误结论）。
    prev_kv: Option<KeyValue>,
    /// F-01（jepsen T1.4）：Delete 缓存的完整响应。
    /// `deleted` 为首次执行实际删除的 Key 数；`prev_kvs` 为首次执行返回的被删
    /// 旧值。范围删重放时必须回放这两项，否则「返回首次执行的结果」不成立，
    /// 且重放会删掉首次执行之后新写入的数据（重试放大破坏面）。
    deleted: i64,
    prev_kvs: Vec<KeyValue>,
}

impl IdempotentEntry {
    /// 只带 revision 的基础条目（其余载荷取默认值）。Put / Delete / Txn 各自
    /// 用 `..IdempotentEntry::with_revision(rev)` 补上自己的载荷字段。
    fn with_revision(revision: i64) -> Self {
        Self {
            inserted_at: std::time::Instant::now(),
            revision,
            succeeded: true,
            responses: Vec::new(),
            prev_kv: None,
            deleted: 0,
            prev_kvs: Vec::new(),
        }
    }
}

/// R-SVC-18：幂等去重缓存（request 维度 + 客户端身份 + TTL + 容量上限）。
///
/// 此前为无界 `HashMap<request_id, …>`：不同客户端复用同一 request_id 会互相
/// 命中、条目永不过期、内存无限增长。现改为：
/// - 键 = 客户端身份哈希（8B，取自 authorization metadata）+ request_id；
/// - 条目带 TTL，读取时惰性过期；
/// - FIFO 淘汰，容量上限 `idempotency_max_entries`。
struct IdempotencyCache {
    entries: HashMap<Vec<u8>, IdempotentEntry>,
    /// FIFO 插入序（队头最旧，淘汰用）
    order: std::collections::VecDeque<Vec<u8>>,
}

impl IdempotencyCache {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            order: std::collections::VecDeque::new(),
        }
    }

    /// 查询并惰性过期：命中且未超 TTL 返回条目，否则移除并返回 None。
    fn check(&mut self, key: &[u8], ttl: std::time::Duration) -> Option<IdempotentEntry> {
        let expired = self
            .entries
            .get(key)
            .is_some_and(|e| e.inserted_at.elapsed() > ttl);
        if expired {
            self.remove(key);
            return None;
        }
        self.entries.get(key).cloned()
    }

    /// 插入条目；容量满时 FIFO 淘汰最旧。重复 key 不覆盖（首次响应为准）。
    fn insert(&mut self, key: Vec<u8>, entry: IdempotentEntry, max_entries: usize) {
        if max_entries == 0 || self.entries.contains_key(&key) {
            return;
        }
        while self.entries.len() >= max_entries {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            self.entries.remove(&oldest);
        }
        self.order.push_back(key.clone());
        self.entries.insert(key, entry);
    }

    fn remove(&mut self, key: &[u8]) {
        self.entries.remove(key);
        self.order.retain(|k| k.as_slice() != key);
    }
}

/// R-SVC-18：幂等缓存键 = 客户端身份哈希（8B）+ request_id。
///
/// 身份取自 authorization metadata 的哈希——不同凭据的客户端即使使用相同
/// request_id 也不会互相命中。无凭据的调用（如未启用 auth 的 dev 路径）
/// 退化为仅 request_id 维度。
fn idempotency_key(metadata: &tonic::metadata::MetadataMap, request_id: &[u8]) -> Vec<u8> {
    use std::hash::{Hash, Hasher};
    let identity = metadata
        .get("authorization")
        .and_then(|v| v.to_bytes().ok())
        .map(|bytes| {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            bytes.hash(&mut hasher);
            hasher.finish()
        })
        .unwrap_or(0);
    let mut key = Vec::with_capacity(8 + request_id.len());
    key.extend_from_slice(&identity.to_be_bytes());
    key.extend_from_slice(request_id);
    key
}

/// R-SVC-08：follower 重定向 hint 的 gRPC metadata key
pub const LEADER_HINT_METADATA_KEY: &str = "coord-leader-hint";

/// 服务端核心节点，持有所有组件并实现 gRPC 服务 trait
pub struct CoordNode {
    /// 本节点 ID（集群模式下由 main.rs 设置；单节点模式为 0）
    pub node_id: u64,
    /// MVCC 存储层（共享引用，读写均通过此实例）
    pub storage: Arc<MvccStorage<RedbBackend>>,
    /// Raft 共识实例（可选，集群模式下设置；单节点模式为 None）
    pub raft: Option<Arc<CoordRaft>>,
    /// 本地 Raft Log 存储句柄（读路径一致性校验用，防陈旧读；集群模式下设置）
    pub raft_log_store: Option<LogStore>,
    /// Multi-Raft Region 路由/运行时管理。
    ///
    /// `Some` = 多 Region 模式：KV Put/Range/Delete/Txn 按 key 经
    /// `RegionManager::route_runtime` 路由到 per-region raft/mvcc（非 leader 返回
    /// RegionNotLeader + leader hint）。`None` = 单 Raft 模式（region 0 legacy 路径，
    /// 使用本节点 `storage`/`raft`/`raft_log_store`，字节级退化）。
    pub region_manager: Option<Arc<RegionManager>>,
    /// Lease 管理器（可选，Leader 节点持有）
    pub lease_manager: Option<Arc<LeaseManager>>,
    /// Watch 分发器
    pub watch_dispatcher: Option<Arc<WatchDispatcher>>,
    /// 幂等请求去重缓存（身份哈希 + request_id → 上次响应；TTL + FIFO 上限）
    idempotent_cache: RwLock<IdempotencyCache>,
    /// R-SVC-18：运行时资源限制（per-RPC 超时/规模上限/幂等参数）
    limits: RwLock<RuntimeLimits>,
    /// 集群已知节点的 node_id → gRPC 地址（Join 重定向用；best-effort）
    node_grpc_addrs: RwLock<HashMap<u64, String>>,
    /// 成员变更互斥（单 pending change，并发变更返回 UNAVAILABLE）
    member_change_lock: tokio::sync::Mutex<()>,
    /// 磁盘水位只读闸：磁盘可用 < 5% 时写请求 RESOURCE_EXHAUSTED
    disk_read_only: std::sync::atomic::AtomicBool,
    /// 每 watcher 事件队列长度（可配，默认 1024）
    watch_buffer: std::sync::atomic::AtomicUsize,
    /// 静态加密 Keyring（None = 未启用静态加密）
    keyring: parking_lot::RwLock<Option<Arc<Keyring>>>,
    /// 持久化的密文 DEK（unseal 时重建 Keyring 用）
    encrypted_deks: parking_lot::RwLock<Vec<EncryptedDek>>,
    /// root 密钥提供者（配置/环境变量/密钥文件；unseal 用）。
    /// root 密钥提供者（配置/环境变量/密钥文件；unseal 用）。
    /// 由 `run_server` 在构造后（Arc 包装前）设置。
    pub root_key_provider: Option<Arc<dyn Fn() -> Option<Vec<u8>> + Send + Sync>>,
    /// 对象存储启用（Some = 限额；None = 关闭）。
    /// 由 `run_server` 按 `[object_storage]` 段设置；保留前缀守卫据此生效。
    pub object_limits: Option<Arc<crate::storage::object_store::ObjectLimits>>,
    /// 对象存储 root/legacy（region 0）chunk 文件存储（`<data_dir>/objects/`）。
    /// Multi-Raft 模式下对象按 key 路由进数据 Region，各自 RegionRuntime 持
    /// 有自己的 ChunkStore；本字段仅 legacy/region 0 单 Raft 路径使用。
    pub chunk_store: Option<Arc<crate::storage::object_store::ChunkStore>>,
}

/// KV 请求解析出的执行目标（一个 Region，或 legacy 单 Raft）。
///
/// - region 模式（`region_manager` 为 Some）：按 key 路由到 RegionManager 装配的
///   RegionRuntime——per-region raft + 目录隔离 MVCC + 该 Region 的 LogStore
///   （读屏障幻影态终检需要，`RegionRuntime::raft_log_store` 与 `main.rs` 单 Raft
///   的 `node_raft_log` 同理）。
/// - legacy 模式（manager 为 None）：region 0 目标，使用 CoordNode 自有
///   storage/raft/raft_log_store（单 Raft / 兼容路径，行为字节级不变）。
struct KvTarget {
    /// 目标 Region（legacy 模式为 0）
    region_id: RegionId,
    /// 该 Region 的 MVCC（读写目标）
    mvcc: Arc<MvccStorage<RedbBackend>>,
    /// 该 Region 的 Raft 实例（None = 单节点直写模式，仅 legacy 出现）
    raft: Option<Arc<CoordRaft>>,
    /// 该 Region 的 LogStore（读屏障幻影态终检用）
    raft_log_store: Option<LogStore>,
    /// region 模式下的 Region 句柄（key range containment 校验）；legacy 为 None
    handle: Option<Arc<RegionHandle>>,
}

/// 对象存储执行目标（一个 Region，或 legacy 单 Raft）——对象 manifest 读写的
/// raft/mvcc + 该目标数据目录下的 chunk 文件存储。
struct ObjectTarget {
    /// 目标 Region（legacy 模式为 0）
    region_id: RegionId,
    /// 该 Region 的 MVCC（manifest 读写目标）
    mvcc: Arc<MvccStorage<RedbBackend>>,
    /// 该 Region 的 Raft 实例（None = 单节点直写模式，仅测试出现）
    raft: Option<Arc<CoordRaft>>,
    /// 该 Region 的 LogStore（读屏障幻影态终检用）
    raft_log_store: Option<LogStore>,
    /// 该 Region 的 chunk 文件存储（对象存储启用时为 Some）
    chunk_store: Option<Arc<crate::storage::object_store::ChunkStore>>,
}

impl CoordNode {
    pub fn new(storage: Arc<MvccStorage<RedbBackend>>) -> Self {
        Self {
            node_id: 0,
            storage,
            raft: None,
            raft_log_store: None,
            region_manager: None,
            lease_manager: None,
            watch_dispatcher: None,
            idempotent_cache: RwLock::new(IdempotencyCache::new()),
            limits: RwLock::new(RuntimeLimits::default()),
            node_grpc_addrs: RwLock::new(HashMap::new()),
            member_change_lock: tokio::sync::Mutex::new(()),
            disk_read_only: std::sync::atomic::AtomicBool::new(false),
            watch_buffer: std::sync::atomic::AtomicUsize::new(1024),
            keyring: parking_lot::RwLock::new(None),
            encrypted_deks: parking_lot::RwLock::new(Vec::new()),
            root_key_provider: None,
            object_limits: None,
            chunk_store: None,
        }
    }

    /// R-SVC-18：注入运行时资源限制（由配置层在启动时调用；默认值可直接使用）。
    pub fn set_limits(&self, limits: RuntimeLimits) {
        *self.limits.write() = limits;
    }

    /// 注入静态加密 Keyring 与持久化密文 DEK。
    /// 由 `run_server` 在启动时调用（bootstrap/恢复/解封后）。
    pub fn install_keyring(&self, keyring: Arc<Keyring>, encrypted_deks: Vec<EncryptedDek>) {
        *self.keyring.write() = Some(keyring);
        *self.encrypted_deks.write() = encrypted_deks;
    }

    /// 当前 Keyring（静态加密启用时返回 Some）
    pub fn keyring(&self) -> Option<Arc<Keyring>> {
        self.keyring.read().clone()
    }

    /// 设置每 watcher 事件队列长度（由配置层调用；支持 SIGHUP 热更新，新订阅生效）
    pub fn set_watch_buffer(&self, buffer: usize) {
        self.watch_buffer
            .store(buffer.max(16), std::sync::atomic::Ordering::Relaxed);
    }

    /// 设置磁盘只读闸（磁盘水位监控任务调用）
    pub fn set_disk_read_only(&self, read_only: bool) {
        self.disk_read_only
            .store(read_only, std::sync::atomic::Ordering::Relaxed);
    }

    /// 磁盘只读闸校验：写请求入口调用，可用 < 5% 时拒绝
    pub fn ensure_writable(&self) -> Result<(), tonic::Status> {
        if self
            .disk_read_only
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Err(tonic::Status::resource_exhausted(
                "disk space below 5%: cluster is read-only",
            ));
        }
        Ok(())
    }

    /// 注册/更新某节点的 gRPC 地址。
    pub fn register_grpc_addr(&self, node_id: u64, addr: &str) {
        self.node_grpc_addrs
            .write()
            .insert(node_id, addr.to_string());
    }

    /// 查询已知的某节点 gRPC 地址（leader 重定向）。
    pub fn grpc_addr_of(&self, node_id: u64) -> Option<String> {
        self.node_grpc_addrs.read().get(&node_id).cloned()
    }

    // ──── 对象存储（coord.storage）路由 / 保留前缀守卫 ────

    /// 对象存储是否启用
    pub fn object_enabled(&self) -> bool {
        self.object_limits.is_some()
    }

    /// 校验写 key 不侵入保留对象空间（/obj/ 前缀）
    pub fn guard_object_write_key(&self, key: &[u8]) -> Result<(), tonic::Status> {
        if self.object_enabled() && crate::storage::object_store::key_in_object_space(key) {
            return Err(tonic::Status::invalid_argument(
                "key in reserved object-storage namespace /obj/ (use the coord.storage \
                 service; object manifests are internal)",
            ));
        }
        Ok(())
    }

    /// 校验写区间 [start, end) 不侵入保留对象空间（防止经 KV 删 manifest）
    pub fn guard_object_write_range(&self, start: &[u8], end: &[u8]) -> Result<(), tonic::Status> {
        if self.object_enabled()
            && crate::storage::object_store::range_touches_object_space(start, end)
        {
            return Err(tonic::Status::invalid_argument(
                "range touches reserved object-storage namespace /obj/ (use the \
                 coord.storage service)",
            ));
        }
        Ok(())
    }

    /// 将对象 (bucket, object_id) 解析为执行目标（Region 或 legacy 单 Raft）。
    ///
    /// 路由键 = manifest key（`/obj/m/{bucket}/{object_id}`），与 KV 共享 keyspace
    /// 平铺——Multi-Raft 开启时经 RegionManager 自动路由进所属 Region（chunk 文件
    /// 存该 Region 数据目录）；关闭时 = region 0 根（字节级退化）。
    fn object_target_for(
        &self,
        bucket: &[u8],
        object_id: &[u8],
    ) -> Result<ObjectTarget, tonic::Status> {
        use crate::storage::object_store::manifest_key;
        let key = manifest_key(bucket, object_id);
        if let Some(manager) = &self.region_manager {
            let rt = manager.route_runtime(&key).map_err(map_err)?;
            Ok(ObjectTarget {
                region_id: rt.region_id(),
                mvcc: Arc::clone(&rt.mvcc),
                raft: Some(Arc::new(rt.raft.clone())),
                raft_log_store: Some(rt.raft_log_store.clone()),
                chunk_store: rt.chunk_store.clone(),
            })
        } else {
            Ok(ObjectTarget {
                region_id: 0,
                mvcc: Arc::clone(&self.storage),
                raft: self.raft.clone(),
                raft_log_store: self.raft_log_store.clone(),
                chunk_store: self.chunk_store.clone(),
            })
        }
    }

    /// 对象写路径：raft 提交 ObjectStore 命令（须先过水位/配额闸）。
    async fn propose_object_op(
        &self,
        target: &ObjectTarget,
        op: crate::raft::type_config::ObjectStoreOp,
    ) -> Result<(u64, bool), tonic::Status> {
        let raft = target.raft.as_ref().ok_or_else(|| {
            tonic::Status::unavailable("object storage requires raft mode (single-node dev off)")
        })?;
        let cmd = crate::raft::type_config::Command::ObjectStore(op);
        let resp = self
            .client_write_with_timeout(raft, cmd, target.region_id)
            .await?;
        match resp.response() {
            Response::ObjectStore { revision, ok } => Ok((*revision, *ok)),
            _ => Err(tonic::Status::internal(
                "unexpected raft response for object op",
            )),
        }
    }

    /// 对象读路径：ReadIndex 线性一致屏障后返回（与 KV 读同口径）
    async fn object_linearizable(&self, target: &ObjectTarget) -> Result<(), tonic::Status> {
        self.ensure_linearizable_on(
            target.raft.as_deref(),
            target.raft_log_store.as_ref(),
            target.region_id,
        )
        .await
    }

    // ──── Multi-Raft KV 路由 ────

    /// 将单个 key 解析为 KV 执行目标（Region 或 legacy 单 Raft）。
    ///
    /// region 模式经 `RegionManager::route_runtime` 路由，错误映射为既有 gRPC
    /// Status（RouteNotReady→unavailable / RegionNotFound→not_found /
    /// KeyNotInRegion→invalid_argument）；legacy 模式返回 region 0 目标
    /// （CoordNode 自有 storage/raft/raft_log_store，行为与现状一致）。
    fn kv_target_for_key(&self, key: &[u8]) -> Result<KvTarget, tonic::Status> {
        if let Some(manager) = &self.region_manager {
            let rt = manager.route_runtime(key).map_err(map_err)?;
            Ok(KvTarget {
                region_id: rt.region_id(),
                mvcc: Arc::clone(&rt.mvcc),
                raft: Some(Arc::new(rt.raft.clone())),
                raft_log_store: Some(rt.raft_log_store.clone()),
                handle: Some(rt.handle()),
            })
        } else {
            Ok(KvTarget {
                region_id: 0,
                mvcc: Arc::clone(&self.storage),
                raft: self.raft.clone(),
                raft_log_store: self.raft_log_store.clone(),
                handle: None,
            })
        }
    }

    /// 校验 key 属于 target Region（Txn 的多 key 同区约束）。
    /// legacy 模式（handle=None）直接放行——与现状一致。
    fn guard_key_in_target(&self, target: &KvTarget, key: &[u8]) -> Result<(), tonic::Status> {
        if let Some(handle) = &target.handle {
            if !handle.contains_key(key) {
                return Err(tonic::Status::invalid_argument(format!(
                    "key {} outside region {} (range {:?}..{:?}); cross-region txn is \
                     not supported",
                    String::from_utf8_lossy(key),
                    target.region_id,
                    String::from_utf8_lossy(&handle.meta.read().start_key),
                    String::from_utf8_lossy(&handle.meta.read().end_key),
                )));
            }
        }
        Ok(())
    }

    /// 校验半开区间 [start, end) 不越过 target Region 边界。
    ///
    /// 单点（end 空或 end==start）无需校验——路由已保证 start 属于本 Region。
    /// region 模式非末段（end_key 非空）且 end 越界 → INVALID_ARGUMENT
    /// （v1 明确不支持跨 Region 范围，宁可拒绝不可静默错答）；legacy 放行。
    fn guard_range_in_target(
        &self,
        target: &KvTarget,
        start: &[u8],
        end: &[u8],
    ) -> Result<(), tonic::Status> {
        if end.is_empty() || end == start {
            return Ok(());
        }
        let Some(handle) = &target.handle else {
            return Ok(());
        };
        let meta = handle.meta.read();
        if !meta.end_key.is_empty() && end > meta.end_key.as_slice() {
            return Err(tonic::Status::invalid_argument(format!(
                "range [.., {}) crosses region {} boundary (end_key {:?}); \
                 cross-region range is not supported",
                String::from_utf8_lossy(end),
                target.region_id,
                String::from_utf8_lossy(&meta.end_key),
            )));
        }
        Ok(())
    }

    /// 解析 Watch 目标的 dispatcher 与历史 reader（MVCC）。
    ///
    /// - legacy（无 region_manager）：节点级 dispatcher + 节点级 storage（行为不变）；
    /// - region 模式：按 watch key/前缀路由到所属 Region 的 per-Region dispatcher +
    ///   该 Region 的 MVCC（per-Region revision 语义，见 `` L13）。
    ///
    /// 跨 Region 显式拒绝（对齐 G1/G3/L11，宁可拒绝不可静默丢事件）：
    /// - 区间 `[key, range_end)`：`range_end` 越过所属 Region 边界 → 拒绝；
    /// - 前缀（`range_end` 为空）：前缀匹配集 `[key, successor(key))` 越过边界 → 拒绝；
    ///   空 key 前缀（匹配全 keyspace）在多 Region 下必然跨区 → 拒绝。
    fn resolve_watch_target(
        &self,
        key: &[u8],
        range_end: &[u8],
    ) -> Result<(Arc<WatchDispatcher>, Arc<MvccStorage<RedbBackend>>), tonic::Status> {
        let Some(manager) = &self.region_manager else {
            let dispatcher = self
                .watch_dispatcher
                .as_ref()
                .ok_or_else(|| tonic::Status::unavailable("watch not available"))?;
            return Ok((Arc::clone(dispatcher), Arc::clone(&self.storage)));
        };

        // region 模式：路由 + 区间/前缀跨区显式拒绝
        let rt = manager.route_runtime(key).map_err(map_err)?;
        let meta = rt.handle.meta.read();
        let region_end = meta.end_key.as_slice();
        let unbounded = region_end.is_empty();

        if range_end.is_empty() {
            // 前缀语义（WatchDispatcher：key.starts_with(prefix)，匹配集 [key, 上界)）
            if !unbounded {
                match prefix_successor(key) {
                    // 匹配集 [key, upper) 完全落在本 Region [start, end) 内 → 放行
                    Some(upper) if upper.as_slice() <= region_end => {}
                    _ => {
                        return Err(tonic::Status::invalid_argument(format!(
                            "watch prefix {:?} crosses region {} boundary (end {:?}); \
                             cross-region watch is not supported",
                            String::from_utf8_lossy(key),
                            rt.region_id(),
                            String::from_utf8_lossy(region_end),
                        )));
                    }
                }
            }
        } else if range_end != key {
            // 区间 [key, range_end)：与 guard_range_in_target 同口径
            if !unbounded && range_end > region_end {
                return Err(tonic::Status::invalid_argument(format!(
                    "watch range [.., {:?}) crosses region {} boundary (end {:?}); \
                     cross-region watch is not supported",
                    String::from_utf8_lossy(range_end),
                    rt.region_id(),
                    String::from_utf8_lossy(region_end),
                )));
            }
        }
        drop(meta);

        Ok((Arc::clone(&rt.watch_dispatcher), Arc::clone(&rt.mvcc)))
    }

    /// R-SVC-08：将 raft `client_write` 错误映射为 gRPC Status（legacy 单 Raft）。
    ///
    /// follower 上的写请求会返回 `ForwardToLeader`——映射为 `UNAVAILABLE` 并在
    /// gRPC metadata 中携带 leader 地址 hint（`coord-leader-hint`），客户端据此
    /// 重定向到当前 leader。其余错误映射为 `INTERNAL`。
    fn map_client_write_error(
        &self,
        e: openraft::error::ClientWriteError<crate::raft::type_config::TypeConfig>,
    ) -> tonic::Status {
        match e {
            openraft::error::ClientWriteError::ForwardToLeader(ftl) => {
                let hint = ftl.leader_id.and_then(|id| self.grpc_addr_of(id));
                // 第四轮 §3.14.2：not-leader 必须与"集群不可用"区分开。
                // 修复前两者都是裸 `UNAVAILABLE`，Java SDK 因此把 not-leader 报成
                // `AGENT_UNAVAILABLE`——而 SDK 的重试矩阵正是按这个码决策的：
                // not-leader 应当**重定向到 leader 后立即重试**，
                // 不是"等待 agent 恢复"。
                let mut status = coord_core::error_code::attach(
                    tonic::Status::unavailable("not leader: forward to current leader"),
                    coord_core::error_code::CoordErrorCode::NotLeader,
                );
                if let Some(addr) = hint {
                    if let Ok(v) = tonic::metadata::MetadataValue::from_str(&addr) {
                        status.metadata_mut().insert(LEADER_HINT_METADATA_KEY, v);
                    }
                }
                status
            }
            other => coord_core::error_code::attach(
                tonic::Status::internal(format!("raft write failed: {other}")),
                coord_core::error_code::CoordErrorCode::Internal,
            ),
        }
    }

    /// 将某 Region 的 raft `client_write` 错误映射为 gRPC Status。
    ///
    /// 与 legacy 版本相同的前向语义，但错误携带 Region 标识：`ForwardToLeader` →
    /// `Error::RegionNotLeader { region_id, leader_addr }`（经 [`map_core_error`]
    /// 统一映射为 UNAVAILABLE + `coord-leader-hint`），客户端据此按 Region 重定向。
    fn map_region_client_write_error(
        &self,
        region_id: RegionId,
        e: openraft::error::ClientWriteError<crate::raft::type_config::TypeConfig>,
    ) -> tonic::Status {
        match e {
            openraft::error::ClientWriteError::ForwardToLeader(ftl) => {
                let leader_addr = ftl.leader_id.and_then(|id| self.grpc_addr_of(id));
                map_core_error(&coord_core::error::Error::RegionNotLeader {
                    region_id,
                    leader_addr,
                })
            }
            other => tonic::Status::internal(format!("raft write failed: {other}")),
        }
    }

    /// R-SVC-08：写路径超时保护——失去 quorum 时快速失败而非无限挂起。
    /// R-SVC-18：超时从 `RuntimeLimits.write_timeout` 读取（配置可调，默认 5s）。
    /// `region_id` 用于非 0 Region 的错误映射（RegionNotLeader + hint）；
    /// region 0 / legacy 走 [`Self::map_client_write_error`]，行为与现状一致。
    async fn client_write_with_timeout(
        &self,
        raft: &CoordRaft,
        cmd: Command,
        region_id: RegionId,
    ) -> Result<
        openraft::raft::ClientWriteResponse<crate::raft::type_config::TypeConfig>,
        tonic::Status,
    > {
        let timeout = self.limits.read().write_timeout;
        let fut = raft.client_write(cmd);
        match tokio::time::timeout(timeout, fut).await {
            Ok(res) => res.map_err(|e| match e {
                openraft::error::RaftError::APIError(cwe) if region_id != 0 => {
                    self.map_region_client_write_error(region_id, cwe)
                }
                openraft::error::RaftError::APIError(cwe) => self.map_client_write_error(cwe),
                other => tonic::Status::internal(format!("raft write failed: {other}")),
            }),
            Err(_) => Err(tonic::Status::deadline_exceeded(
                "raft write timed out (no quorum?)",
            )),
        }
    }

    /// 领导权移交（非阻塞触发；收敛由调用方轮询 `current_leader`）。
    pub async fn transfer_leadership(&self, target: u64) -> Result<(), String> {
        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| "not a raft node".to_string())?;
        raft.trigger()
            .transfer_leader(target)
            .await
            .map_err(|e| format!("transfer_leader failed: {e}"))
    }

    /// 挑选一个非自身的 voter 作为领导权移交目标（无可用目标返回 None）。
    pub async fn pick_transfer_target(&self) -> Option<u64> {
        let raft = self.raft.as_ref()?;
        let m = raft.metrics().borrow_watched().clone();
        let voters: Vec<u64> = m.membership_config.voter_ids().collect();
        voters.into_iter().find(|id| *id != self.node_id)
    }

    /// 尝试获取成员变更互斥锁（非阻塞，占用中返回 None）。
    fn try_lock_member_change(&self) -> Option<tokio::sync::MutexGuard<'_, ()>> {
        self.member_change_lock.try_lock().ok()
    }
    /// 当前是否为 raft leader（单节点模式恒为 true）
    pub async fn is_raft_leader(&self) -> bool {
        match self.raft {
            Some(ref raft) => raft.current_leader().await == Some(self.node_id),
            None => true,
        }
    }

    /// Lease 准入检查（B.4.1）：仅 leader 接受 grant/keepalive/revoke；
    /// 非 leader 返回 `UNAVAILABLE` 并携带 leader 提示。
    pub async fn ensure_lease_leader(&self) -> Result<(), tonic::Status> {
        if let Some(ref raft) = self.raft {
            let leader = raft.current_leader().await;
            if leader != Some(self.node_id) {
                return Err(tonic::Status::unavailable(format!(
                    "lease operations require leader: current leader is {:?}, this node is {}",
                    leader, self.node_id
                )));
            }
        }
        Ok(())
    }

    /// 启动 Lease 过期轮询循环（后台任务）。
    ///
    /// 每 200ms 调用 `LeaseManager::check_expired()`，对已过期的 Lease
    /// 经 raft 下发 `LeaseOp::Revoke{delete_keys:true}`（任何路径不得直写本地存储）。
    ///
    /// 应在 server 启动后调用（Leader 独占；Follower 无 LeaseManager 则跳过）。
    pub fn start_lease_expiry_worker(self: &Arc<Self>) {
        let node = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(200));
            loop {
                interval.tick().await;
                // B.4.2：过期检测仅 leader 执行（follower 上 LeaseManager 空转无意义，
                // 且 follower 经 raft propose 会被 openraft 拒绝）
                if !node.is_raft_leader().await {
                    continue;
                }
                let Some(ref lm) = node.lease_manager else {
                    continue;
                };
                let actions = lm.check_expired();
                for action in actions {
                    match action {
                        crate::lease::LeaseAction::Expired { lease_id, .. } => {
                            let op = LeaseOp::Revoke {
                                id: lease_id,
                                delete_keys: true,
                            };
                            // 通过 Raft（集群模式）或直接 apply（单节点模式）
                            if let Some(ref raft) = node.raft {
                                let cmd = Command::Lease(op);
                                if let Err(e) = raft.client_write(cmd).await {
                                    tracing::warn!(
                                        "Lease {} expiry: failed to revoke via raft: {}",
                                        lease_id,
                                        e
                                    );
                                }
                            } else if let Err(e) = node.storage.apply_lease_op_standalone(&op) {
                                tracing::warn!(
                                    "Lease {} expiry: failed to apply revoke: {}",
                                    lease_id,
                                    e
                                );
                            }
                        }
                    }
                }
            }
        });
    }

    /// per-Region Lease 清理 worker（region 模式）。
    ///
    /// region 0 状态机在 `LeaseOp::Revoke`（显式吊销或过期清理，apply 在**全部**
    /// 节点发生）后经 `rx` 广播 lease_id。本 worker 维护待清理集合
    /// {(region_id, lease_id)}，每 200ms 对**本节点是 leader** 的 Region 发起
    /// `Command::DeleteKeysByLease`（region raft 内原子删除绑定 Key + Watch 事件，
    /// 幂等）；若 Region leader 是其他节点，则该条目由对端处理（对端同样收到
    /// region 0 广播），本端丢弃以限制内存——不会无限重试堆积。
    ///
    /// 语义保证：只要某 Region 有 leader、且其 region 0 raft apply 在推进，
    /// 该 Region 中绑定被吊销 Lease 的 Key 终会被删除。
    ///
    /// legacy（region_manager=None）：无需启动；通道关闭后 worker 自行退出。
    pub fn start_region_lease_revoker(
        self: &Arc<Self>,
        mut rx: tokio::sync::mpsc::UnboundedReceiver<i64>,
    ) {
        let node = Arc::clone(self);
        tokio::spawn(async move {
            let mut pending: std::collections::BTreeSet<(RegionId, i64)> = Default::default();
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(200));
            loop {
                tokio::select! {
                    maybe = rx.recv() => {
                        match maybe {
                            Some(lease_id) => {
                                if let Some(manager) = &node.region_manager {
                                    for handle in manager.list_regions() {
                                        pending.insert((handle.region_id(), lease_id));
                                    }
                                }
                            }
                            None => break, // region 0 状态机通道关闭
                        }
                    }
                    _ = interval.tick() => {}
                }

                let Some(manager) = &node.region_manager else {
                    continue;
                };
                let timeout = node.limits.read().write_timeout;
                let mut done: Vec<(RegionId, i64)> = Vec::new();
                for (region_id, lease_id) in &pending {
                    let Some(rt) = manager.runtime(*region_id) else {
                        done.push((*region_id, *lease_id));
                        continue;
                    };
                    match rt.raft.current_leader().await {
                        Some(leader) if leader == node.node_id => {
                            let cmd = Command::DeleteKeysByLease {
                                lease_id: *lease_id,
                            };
                            let ok = match tokio::time::timeout(timeout, rt.raft.client_write(cmd))
                                .await
                            {
                                Ok(Ok(_)) => true,
                                Ok(Err(e)) => {
                                    tracing::warn!(
                                        region_id,
                                        lease_id,
                                        "region lease cleanup propose failed: {e}"
                                    );
                                    false
                                }
                                Err(_) => false,
                            };
                            if ok {
                                done.push((*region_id, *lease_id));
                            }
                        }
                        Some(_) => {
                            // 其他节点是该 Region leader：对端同样收到广播并清理
                            done.push((*region_id, *lease_id));
                        }
                        None => {
                            // 选举中/leader 未知：保留待下轮重试
                        }
                    }
                }
                for key in done {
                    pending.remove(&key);
                }
            }
        });
    }

    /// 启动 Lease failover reconciler（B.4.4）
    ///
    /// 每 500ms 检测 leader 身份；检测到本节点成为 leader（含启动即 leader 与
    /// 单节点模式）时，从状态机 `/_lease/` 记录重建 LeaseManager：
    /// - 新 leader 接管：未过期 Lease 以剩余 TTL 继续（at-least TTL），
    /// - 已过期 Lease：立即到期，由过期 worker 经 raft propose Revoke 清理。
    pub fn start_lease_leader_reconciler(self: &Arc<Self>) {
        let node = Arc::clone(self);
        tokio::spawn(async move {
            let mut was_leader = false;
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
            loop {
                interval.tick().await;
                let is_leader = node.is_raft_leader().await;
                if is_leader && !was_leader {
                    let Some(ref lm) = node.lease_manager else {
                        was_leader = is_leader;
                        continue;
                    };
                    match node.storage.list_lease_records() {
                        Ok(records) => {
                            let n = lm.rebuild(records).await;
                            tracing::info!("LeaseManager rebuilt from state machine: {n} leases");
                        }
                        Err(e) => {
                            tracing::warn!("failed to read lease records for rebuild: {e}")
                        }
                    }
                }
                was_leader = is_leader;
            }
        });
    }

    /// 提交 Lease 命令：集群模式走 raft，单节点模式直接 apply
    /// R-SVC-18：raft 提交带 `lease_timeout` 超时（此前无超时，quorum 丢失时无限挂起）
    async fn submit_lease_op(&self, op: LeaseOp) -> Result<u64, tonic::Status> {
        if let Some(ref raft) = self.raft {
            let cmd = Command::Lease(op);
            let timeout = self.limits.read().lease_timeout;
            let resp = tokio::time::timeout(timeout, raft.client_write(cmd))
                .await
                .map_err(|_| {
                    tonic::Status::deadline_exceeded("lease write timed out (no quorum?)")
                })?
                .map_err(|e| tonic::Status::internal(format!("raft lease write failed: {e}")))?;
            match resp.response() {
                Response::Lease { revision } => Ok(*revision),
                _ => Err(tonic::Status::internal("unexpected raft response")),
            }
        } else {
            let revision = self
                .storage
                .apply_lease_op_standalone(&op)
                .map_err(map_err)?;
            Ok(revision)
        }
    }

    /// F-01/F-02：命中时取出**完整条目**（而不只是 revision），让 Put 能回放
    /// `prev_kv`、Delete 能回放 `deleted`/`prev_kvs`。
    fn check_idempotent_entry(&self, key: &[u8]) -> Option<IdempotentEntry> {
        let ttl = self.limits.read().idempotency_ttl;
        self.idempotent_cache.write().check(key, ttl)
    }

    /// F-02：缓存 Put 的完整响应（含 `prev_kv`），命中时整体回放。
    fn cache_idempotent_put(&self, key: Vec<u8>, revision: i64, prev_kv: Option<KeyValue>) {
        self.idempotent_cache.write().insert(
            key,
            IdempotentEntry {
                prev_kv,
                ..IdempotentEntry::with_revision(revision)
            },
            self.idempotency_max_entries(),
        );
    }

    /// F-01：缓存 Delete 的完整响应（含 `deleted` / `prev_kvs`），命中时整体回放。
    ///
    /// 范围删必须缓存 `prev_kvs` 与 `deleted`：否则「返回首次执行的结果」无法
    /// 满足，而且重放会在区间内新写入补齐之后再次删一遍（重试放大破坏面）。
    fn cache_idempotent_delete(
        &self,
        key: Vec<u8>,
        deleted: i64,
        prev_kvs: Vec<KeyValue>,
        revision: i64,
    ) {
        self.idempotent_cache.write().insert(
            key,
            IdempotentEntry {
                deleted,
                prev_kvs,
                ..IdempotentEntry::with_revision(revision)
            },
            self.idempotency_max_entries(),
        );
    }

    fn idempotency_max_entries(&self) -> usize {
        self.limits.read().idempotency_max_entries
    }

    /// 检查 Txn 幂等请求：命中且未过期返回 (succeeded, revision, responses)
    fn check_idempotent_txn(&self, key: &[u8]) -> Option<(bool, i64, Vec<ResponseOp>)> {
        let ttl = self.limits.read().idempotency_ttl;
        self.idempotent_cache
            .write()
            .check(key, ttl)
            .map(|e| (e.succeeded, e.revision, e.responses))
    }

    /// 缓存 Txn 幂等请求结果（R-SVC-18：连同完整 responses 一并缓存）
    fn cache_idempotent_txn(
        &self,
        key: Vec<u8>,
        succeeded: bool,
        revision: i64,
        responses: Vec<ResponseOp>,
    ) {
        let max_entries = self.limits.read().idempotency_max_entries;
        self.idempotent_cache.write().insert(
            key,
            IdempotentEntry {
                succeeded,
                responses,
                ..IdempotentEntry::with_revision(revision)
            },
            max_entries,
        );
    }

    /// 对指定 raft / log_store 执行线性一致读屏障（region 0 legacy 与
    /// per-region 共用；range/delete 等读路径统一经此方法）。
    ///
    /// 仅在 Raft 模式下生效（raft=None——单节点直写模式直接返回）。
    /// R-SVC-18：带 `read_timeout` 超时（此前无超时，leader 失联时读无限挂起）。
    ///
    /// 陈旧读防御（「宁可失败也不返回过期值」，对应 Jepsen partition-halves /
    /// partition-ring 复现的陈旧读异常）：
    /// ReadIndex 确认领导权后，再对本地状态机与提交前沿做一致性复核：
    ///   - 节点必须处于 `Leader` 状态（双保险：防止领导权切换窗口内以非 leader 身份
    ///     应答读请求）；
    ///   - `last_applied.index` 不得超过 `local_committed.index`。正常 Raft 中 applied
    ///     永远 <= committed；若 applied 的 index 超过 committed，说明本地状态机应用过
    ///     后来被新 leader 截断的条目（"幻影态"，旧 leader 曾以失效的复制进度把未真正
    ///     落盘的条目提交并 apply）；
    ///   - 最关键的一层：校验本地日志在 `last_applied.index` 处的实际条目与
    ///     `last_applied` 一致。即便 committed 已追上 applied（applied <= committed），
    ///     若日志在该 index 的条目是新的（被新 leader 截断后重写的），而状态机 apply 的
    ///     是旧的被截断条目，状态机仍持有过期数据——openraft 的 ReadIndex
    ///     `applied_index_at_least` 只按 index 比较，会误判为"已追上"从而返回陈旧值。
    ///     此处任一异常均直接以 UNAVAILABLE 拒绝读。
    ///
    /// `region_id` 仅用于日志标注（非 0 时 raft_wm 日志带 ` region=`）；region 0
    /// 的日志格式与历史完全一致（soak 取证日志不变量）。
    async fn ensure_linearizable_on(
        &self,
        raft: Option<&CoordRaft>,
        raft_log_store: Option<&LogStore>,
        region_id: RegionId,
    ) -> Result<(), tonic::Status> {
        if let Some(raft) = raft {
            let timeout = self.limits.read().read_timeout;
            let read_log_id =
                tokio::time::timeout(timeout, raft.ensure_linearizable(ReadPolicy::ReadIndex))
                    .await
                    .map_err(|_| tonic::Status::deadline_exceeded("linearizable read timed out"))?
                    .map_err(|e| {
                        tonic::Status::internal(format!("linearizable read failed: {e}"))
                    })?;

            // R-SVC-18 补充：ReadIndex 之后的一致性 / 身份复核（防陈旧读）
            let m = raft.metrics().borrow_watched().clone();

            // 水位观测（迭代 #2 陈旧读定位）：成功放行与每个拒绝分支都输出 raft 水位，
            // 用于对照返回给客户端的实际值，判定 openraft 屏障声称的 applied/committed
            // 是否与状态机真实数据一致（区分「屏障/水位超前」与「读值滞后」两类缺陷）。
            let raft_wm = || {
                format!(
                    "raft_wm{} node={} state={:?} leader={:?} term={} last_log_index={:?} \
                     local_committed={:?} cluster_committed={:?} last_applied={:?} read_log_id={}",
                    if region_id != 0 {
                        format!(" region={region_id}")
                    } else {
                        String::new()
                    },
                    self.node_id,
                    m.state,
                    m.current_leader,
                    m.current_term,
                    m.last_log_index,
                    m.local_committed,
                    m.cluster_committed,
                    m.last_applied,
                    read_log_id,
                )
            };

            if !matches!(m.state, openraft::ServerState::Leader) {
                tracing::info!("{} read_refused=not_leader", raft_wm());
                return Err(tonic::Status::unavailable(
                    "not leader: refusing linearizable read (leadership lost during ReadIndex)",
                ));
            }
            if let (Some(applied), Some(committed)) =
                (m.last_applied.as_ref(), m.local_committed.as_ref())
            {
                if applied.index > committed.index {
                    tracing::info!(
                        "{} read_refused=applied_ahead_of_committed applied_index={} \
                         committed_index={}",
                        raft_wm(),
                        applied.index,
                        committed.index
                    );
                    return Err(tonic::Status::unavailable(format!(
                        "read consistency check failed: last_applied index {} exceeds \
                         local_committed index {} (state machine ahead of commit frontier); \
                         refusing to serve stale data",
                        applied.index, committed.index
                    )));
                }
            }

            // 幻影态终检：last_applied 必须与本地日志同 index 的实际条目一致。
            // 若该 index 已被 purge（快照覆盖），则视为合法（状态机来自快照）。
            if let (Some(applied), Some(log_store)) = (m.last_applied.as_ref(), raft_log_store) {
                let covered_by_snapshot = log_store
                    .last_purged()
                    .ok()
                    .flatten()
                    .is_some_and(|purged| applied.index <= purged.index);
                if !covered_by_snapshot {
                    match log_store.get_entry_at(applied.index) {
                        Ok(Some(entry)) => {
                            if entry.log_id != *applied {
                                tracing::info!(
                                    "{} read_refused=phantom_state applied={:?} log_entry={:?}",
                                    raft_wm(),
                                    applied,
                                    entry.log_id
                                );
                                return Err(tonic::Status::unavailable(format!(
                                    "read consistency check failed: state machine applied {:?} \
                                     but local log at index {} is {:?} (stale/phantom state); \
                                     refusing to serve stale data",
                                    applied, applied.index, entry.log_id
                                )));
                            }
                        }
                        Ok(None) => {
                            tracing::info!(
                                "{} read_refused=no_local_log applied_index={}",
                                raft_wm(),
                                applied.index
                            );
                            return Err(tonic::Status::unavailable(format!(
                                "read consistency check failed: no local log entry at applied \
                                 index {} (stale/phantom state); refusing to serve stale data",
                                applied.index
                            )));
                        }
                        Err(e) => {
                            tracing::info!(
                                "{} read_refused=log_read_error applied_index={}",
                                raft_wm(),
                                applied.index
                            );
                            return Err(tonic::Status::internal(format!(
                                "read consistency check failed: log read error at index {}: {e}",
                                applied.index
                            )));
                        }
                    }
                }
            }
            tracing::info!("{} read_served", raft_wm());
        }
        Ok(())
    }
}

// ──── 工具函数 ────

fn to_kv_proto(
    key: &[u8],
    value: &[u8],
    meta: Option<&crate::storage::mvcc::KvMetadata>,
) -> KeyValue {
    match meta {
        Some(m) => KeyValue {
            key: key.to_vec(),
            value: value.to_vec(),
            create_revision: m.create_revision,
            mod_revision: m.mod_revision,
            version: m.version,
            lease_id: m.lease_id,
        },
        None => KeyValue {
            key: key.to_vec(),
            value: value.to_vec(),
            create_revision: 0,
            mod_revision: 0,
            version: 1,
            lease_id: 0,
        },
    }
}

/// coord-core Error → tonic::Status 结构化映射。
///
/// 只回传安全的业务信息（key、lease id、revision 等）；
/// 内部细节（storage/raft/crypto）只进服务端日志，不回传客户端（脱敏）。
fn map_core_error(e: &coord_core::error::Error) -> tonic::Status {
    use coord_core::error::Error;
    match e {
        Error::InvalidArgument(m) => tonic::Status::invalid_argument(m.clone()),
        Error::NotFound { resource, key } => {
            tonic::Status::not_found(format!("{resource} not found: {key}"))
        }
        Error::AlreadyExists { resource, key } => {
            tonic::Status::already_exists(format!("{resource} already exists: {key}"))
        }
        Error::PermissionDenied(m) => tonic::Status::permission_denied(m.clone()),
        Error::Unauthenticated(m) => tonic::Status::unauthenticated(m.clone()),
        Error::NotLeader { leader_addr } => {
            let mut status = tonic::Status::unavailable("not leader");
            if let Some(addr) = leader_addr {
                if let Ok(v) = tonic::metadata::MetadataValue::from_str(addr) {
                    status.metadata_mut().insert(LEADER_HINT_METADATA_KEY, v);
                }
            }
            status
        }
        Error::NotLeaderNoHint => tonic::Status::unavailable("not leader, leader hint unavailable"),
        Error::ClusterUnavailable(m) => tonic::Status::unavailable(m.clone()),
        Error::RequestTimeout => tonic::Status::deadline_exceeded("request timeout"),
        Error::RevisionCompacted { revision, oldest } => tonic::Status::out_of_range(format!(
            "revision {revision} compacted; oldest available: {oldest}"
        )),
        Error::LeaseNotFound { lease_id } => {
            tonic::Status::not_found(format!("lease {lease_id} not found or expired"))
        }
        Error::LeaseTTLOutOfRange { ttl, min, max } => {
            tonic::Status::invalid_argument(format!("lease TTL {ttl}s out of range [{min}, {max}]"))
        }
        Error::TxnTooLarge { ops, max } => {
            tonic::Status::invalid_argument(format!("txn too large: {ops} operations, max {max}"))
        }
        Error::TxnCompareFailed => tonic::Status::failed_precondition("txn compare failed"),
        Error::WatchTooManyConnections { current, max } => tonic::Status::resource_exhausted(
            format!("too many watch connections: {current}/{max}"),
        ),
        Error::Backpressure(m) => tonic::Status::resource_exhausted(m.clone()),
        Error::ClusterSealed => tonic::Status::unavailable("cluster is sealed"),
        Error::ClusterUnsealing => tonic::Status::unavailable("cluster is unsealing"),
        Error::InsufficientShares { have, need } => tonic::Status::invalid_argument(format!(
            "insufficient shares: have {have}, need {need}"
        )),
        Error::AuthNotEnabled => tonic::Status::unauthenticated("auth not enabled"),
        Error::TokenExpired => tonic::Status::unauthenticated("token expired"),
        Error::InvalidToken(_) => tonic::Status::unauthenticated("invalid token"),
        Error::UserAlreadyExists { name } => {
            tonic::Status::already_exists(format!("user {name} already exists"))
        }
        Error::RoleAlreadyExists { name } => {
            tonic::Status::already_exists(format!("role {name} already exists"))
        }
        Error::RegionNotFound { region_id } => {
            tonic::Status::not_found(format!("region {region_id} not found"))
        }
        Error::RegionNotLeader {
            region_id,
            leader_addr,
        } => {
            let mut status =
                tonic::Status::unavailable(format!("not leader for region {region_id}"));
            if let Some(addr) = leader_addr {
                if let Ok(v) = tonic::metadata::MetadataValue::from_str(addr) {
                    status.metadata_mut().insert(LEADER_HINT_METADATA_KEY, v);
                }
            }
            status
        }
        Error::EpochStale { .. } => tonic::Status::unavailable("stale epoch; refresh route table"),
        Error::KeyNotInRegion { region_id } => {
            tonic::Status::invalid_argument(format!("key not in region {region_id} range"))
        }
        Error::RegionSplitInProgress { region_id } => {
            tonic::Status::unavailable(format!("region {region_id} split in progress"))
        }
        Error::PdUnavailable(m) => tonic::Status::unavailable(m.clone()),
        Error::RouteNotReady => tonic::Status::unavailable("route table not ready"),
        // 内部错误脱敏：详情只进服务端日志，不回传客户端
        Error::Internal(m) => {
            tracing::error!(error = %m, "internal error returned to client (sanitized)");
            tonic::Status::internal("internal error")
        }
        Error::Storage(m) => {
            tracing::error!(error = %m, "storage error returned to client (sanitized)");
            tonic::Status::internal("storage error")
        }
        Error::DataCorruption(m) => {
            tracing::error!(error = %m, "data corruption returned to client (sanitized)");
            tonic::Status::internal("data corruption")
        }
        Error::Crypto(_) => tonic::Status::internal("crypto error"),
    }
}

/// 将存储/raft 等内部错误映射为 gRPC Status。
///
/// - `coord_core::error::Error`：结构化映射（见 [`map_core_error`]）；
/// - `std::io::Error`：按 ErrorKind 映射；
/// - 其余类型：仅识别安全且明确的字符串模式，兜底脱敏为 `INTERNAL`
///   （原始信息只进服务端日志，不再原样回传客户端）。
fn map_err<E: std::fmt::Display + 'static>(e: E) -> tonic::Status {
    use std::any::Any;
    let any = &e as &dyn Any;
    if let Some(core) = any.downcast_ref::<coord_core::error::Error>() {
        return map_core_error(core);
    }
    if let Some(io) = any.downcast_ref::<std::io::Error>() {
        let code = match io.kind() {
            std::io::ErrorKind::NotFound => tonic::Code::NotFound,
            std::io::ErrorKind::PermissionDenied => tonic::Code::PermissionDenied,
            std::io::ErrorKind::InvalidInput | std::io::ErrorKind::InvalidData => {
                tonic::Code::InvalidArgument
            }
            _ => tonic::Code::Internal,
        };
        return if code == tonic::Code::Internal {
            tracing::error!(error = %io, "io error returned to client (sanitized)");
            tonic::Status::internal("i/o error")
        } else {
            tonic::Status::new(code, io.to_string())
        };
    }
    let msg = e.to_string();
    let lowered = msg.to_ascii_lowercase();
    if lowered.contains("not leader") {
        return tonic::Status::unavailable("not leader");
    }
    if lowered.contains("compacted") {
        return tonic::Status::out_of_range(msg);
    }
    if lowered.contains("timed out") || lowered.contains("timeout") {
        return tonic::Status::deadline_exceeded(msg);
    }
    tracing::error!(error = %msg, "unclassified error returned to client (sanitized)");
    tonic::Status::internal("internal error")
}

/// 计算「以 `prefix` 开头的全部 key」集合的最小上界字符串（字节字典序）。
///
/// 第四轮 §3.8：本函数此前是 `coord_core::kv_range::prefix_successor` 的**第二份
/// 实现**（同样的算法、同样的边界，各自维护）。同一算法存在两份实现本身就是下一个
/// 语义裂缝（参见 `RangeSemantics` 的注释），故改为转调 core 的单点定义。
pub fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    coord_core::kv_range::prefix_successor(prefix)
}

// ──── AuthOp 提案器（管理操作入 raft 日志）────

#[async_trait::async_trait]
impl AuthOpProposer for CoordNode {
    async fn propose_auth_op(&self, op: AuthOp) -> Result<u64, tonic::Status> {
        if let Some(ref raft) = self.raft {
            // 与 KV 写路径（R-SVC-08 / client_write_with_timeout）同口径：
            // follower 上的 ForwardToLeader 映射为 UNAVAILABLE（附 leader hint），
            // 客户端据此重定向到当前 leader；超时映射为 DEADLINE_EXCEEDED，
            // 其余映射为 INTERNAL。修复前此处直接返回 INTERNAL，导致客户端
            // 在任意非 leader 节点上打开会话即失败（:no-client 风暴）。
            let timeout = self.limits.read().write_timeout;
            let resp = tokio::time::timeout(timeout, raft.client_write(Command::Auth(op)))
                .await
                .map_err(|_| {
                    tonic::Status::deadline_exceeded("raft auth write timed out (no quorum?)")
                })?
                .map_err(|e| match e {
                    openraft::error::RaftError::APIError(cwe) => self.map_client_write_error(cwe),
                    other => tonic::Status::internal(format!("raft auth write failed: {other}")),
                })?;
            match resp.response() {
                Response::Auth { revision } => Ok(*revision),
                _ => Err(tonic::Status::internal(
                    "unexpected raft response for AuthOp",
                )),
            }
        } else {
            // 无 raft：直接本地 apply（与 Lease standalone 同口径，锁内分配 revision）
            let revision = self.storage.current_revision().saturating_add(1);
            self.storage
                .apply_auth_op(&op, revision, AppliedLogId::standalone(revision))
                .map_err(|e| tonic::Status::internal(format!("auth apply failed: {e}")))?;
            Ok(revision)
        }
    }
}

// ──── Compact 执行（raft 下发 compact revision，节点一致）────

impl CoordNode {
    /// 执行压缩：raft 模式经 `client_write(Command::Compact)` 提案，
    /// 单节点模式直接本地 apply。
    ///
    /// 前置校验（由 RPC/调用层保证）：`revision <= current_revision`；
    /// 未来 revision 由 RPC 层返回 `INVALID_ARGUMENT`。
    pub async fn compact_impl(&self, revision: u64) -> Result<u64, String> {
        if let Some(ref raft) = self.raft {
            let timeout = self.limits.read().compact_timeout;
            let resp =
                tokio::time::timeout(timeout, raft.client_write(Command::Compact { revision }))
                    .await
                    .map_err(|_| "raft compact write timed out (no quorum?)".to_string())?
                    .map_err(|e| format!("raft compact write failed: {e}"))?;
            match resp.response() {
                Response::Compact { compacted_revision } => Ok(*compacted_revision),
                _ => Err("unexpected raft response for Compact".into()),
            }
        } else {
            // 单节点：compact 消耗一个 revision（无 changelog 条目，与 membership 同口径）
            let new_rev = self.storage.current_revision().saturating_add(1);
            let applied = AppliedLogId::standalone(new_rev);
            self.storage
                .apply_compact(revision, applied)
                .map_err(|e| e.to_string())?;
            Ok(revision.min(new_rev))
        }
    }
}

/// 单节点本地 apply（无 raft，测试与 dev 路径复用 `compact_impl` 的 else 分支）。
pub fn apply_compact_local(node: &Arc<CoordNode>, revision: u64) -> Result<u64, String> {
    if node.raft.is_some() {
        return Err("apply_compact_local requires a raft-less node".into());
    }
    let new_rev = node.storage.current_revision().saturating_add(1);
    let applied = AppliedLogId::standalone(new_rev);
    node.storage
        .apply_compact(revision, applied)
        .map_err(|e| e.to_string())?;
    Ok(revision.min(new_rev))
}

// ──── Compact 提案器（定时压缩经 raft 下发，节点一致）────

#[async_trait::async_trait]
impl crate::storage::compaction::CompactProposer for CoordNode {
    async fn can_propose(&self) -> bool {
        match &self.raft {
            None => true,
            Some(raft) => raft.current_leader().await == Some(self.node_id),
        }
    }

    async fn propose(&self, revision: u64) -> Result<u64, String> {
        self.compact_impl(revision).await
    }
}

// ──── KV Service ────

#[tonic::async_trait]
impl Kv for CoordNode {
    async fn put(
        &self,
        request: tonic::Request<PutRequest>,
    ) -> Result<tonic::Response<PutResponse>, tonic::Status> {
        // 磁盘水位只读闸
        self.ensure_writable()?;

        let request_metadata = request.metadata().clone();
        let req = request.into_inner();
        let request_id = req.request_id.clone();

        // 保留对象空间守卫（/obj/ 为对象存储内部 manifest，禁止经 KV 写入）
        self.guard_object_write_key(&req.key)?;

        // 幂等检查：相同（客户端身份 + request_id）返回缓存的**完整响应**
        // （F-02：此前这里硬编码 `prev_kv: None`，与「返回首次执行的结果」矛盾）
        if !request_id.is_empty() {
            if let Some(entry) =
                self.check_idempotent_entry(&idempotency_key(&request_metadata, &request_id))
            {
                return Ok(tonic::Response::new(PutResponse {
                    prev_kv: entry.prev_kv,
                    revision: entry.revision,
                }));
            }
        }

        let lease_id = if req.lease_id != 0 {
            Some(req.lease_id)
        } else {
            None
        };

        // 按 key 解析执行目标 Region（legacy 单 Raft = region 0）。
        // region 模式允许 Lease 绑定——Lease 记录存 region 0 全局
        // 租约表，绑定 Key 落在所属 Region MVCC（KvMetadata.lease_id）；到期/吊销
        // 经 region 0 Revoke 广播 + 各 Region leader 的 DeleteKeysByLease 清理。
        let target = self.kv_target_for_key(&req.key)?;

        // 若请求 prev_kv，在写入前读取当前值
        let prev_kv = if req.prev_kv {
            target
                .mvcc
                .get(&req.key)
                .map_err(map_err)?
                .map(|prev_value| {
                    let meta = target.mvcc.get_kv_metadata(&req.key).map_err(map_err)?;
                    Ok::<_, tonic::Status>(to_kv_proto(&req.key, &prev_value, meta.as_ref()))
                })
                .transpose()?
        } else {
            None
        };

        // 通过 Raft 共识提交（集群模式），或直接写入存储（单节点模式）。
        // raft/storage 均取自目标 Region（per-region raft + 目录隔离 MVCC）。
        let revision: u64 = if let Some(raft) = &target.raft {
            let cmd = Command::Put {
                key: req.key.clone(),
                value: req.value.clone(),
                lease_id,
            };
            let resp = self
                .client_write_with_timeout(raft, cmd, target.region_id)
                .await?;
            match resp.response() {
                Response::Put { revision } => *revision,
                _ => return Err(tonic::Status::internal("unexpected raft response")),
            }
        } else {
            target
                .mvcc
                .put(&req.key, &req.value, lease_id)
                .map_err(map_err)?
        };

        // 若关联了 Lease，将 Key 绑定到 Lease（本地 leader 内存镜像，供即时本地
        // 吊销；权威绑定为各 Region KV 的 KvMetadata.lease_id，到期清理按 Region
        // raft 的 DeleteKeysByLease 走。legacy 与 region 模式均可达）。
        if let Some(lid) = lease_id {
            if let Some(ref lm) = self.lease_manager {
                let _ = lm.attach_key(lid, &req.key);
            }
        }

        // 缓存幂等结果（键含客户端身份，防止不同客户端同 request_id 互相命中）。
        // F-02：连同 `prev_kv` 一并缓存 —— 命中时必须返回**首次执行的完整响应**。
        if !request_id.is_empty() {
            self.cache_idempotent_put(
                idempotency_key(&request_metadata, &request_id),
                revision as i64,
                prev_kv.clone(),
            );
        }

        tracing::debug!(revision, "KV put applied");
        Ok(tonic::Response::new(PutResponse {
            prev_kv,
            revision: revision as i64,
        }))
    }

    async fn range(
        &self,
        request: tonic::Request<RangeRequest>,
    ) -> Result<tonic::Response<RangeResponse>, tonic::Status> {
        let req = request.into_inner();
        // R-SVC-18：规模上限——客户端显式 limit 超过 max_range_limit 直接拒绝；
        // 未指定 limit（0）时以 max_range_limit 兜底（此前 usize::MAX 无上限）。
        let max_range_limit = self.limits.read().max_range_limit;
        let limit = if req.limit > 0 {
            let requested = req.limit as usize;
            if max_range_limit > 0 && requested > max_range_limit {
                return Err(tonic::Status::invalid_argument(format!(
                    "range limit {requested} exceeds max {max_range_limit}"
                )));
            }
            requested
        } else {
            max_range_limit
        };
        let keys_only = req.keys_only;
        let count_only = req.count_only;
        let target_revision = if req.revision > 0 {
            req.revision as u64
        } else {
            0
        };

        // 按起始 key 解析目标 Region；多键范围 [key, range_end) 不得越过
        // Region 边界（跨 Region 范围 v1 不支持，显式拒绝）。
        let target = self.kv_target_for_key(&req.key)?;
        self.guard_range_in_target(&target, &req.key, &req.range_end)?;

        // 线性一致性读：确认目标 Region Leader 身份后再读取。
        // 屏障按 Region 走各自 raft（含该 Region 的 LogStore 幻影态终检）。
        self.ensure_linearizable_on(
            target.raft.as_deref(),
            target.raft_log_store.as_ref(),
            target.region_id,
        )
        .await?;

        // R-SVC-07-1 / 第三轮 P0-1：`range_end` 为空 或 == key → 单键精确查询。
        // 判定与 Txn 内层 `TxnOp::Range` 及鉴权层**共用同一份定义**
        // （`coord_core::kv_range::RangeSemantics`）——此前两边各写一份，
        // 产生了跨 scope 越权读的语义裂缝。
        let single_key = RangeSemantics::of(&req.key, &req.range_end).is_single_key();

        let mut kvs = Vec::new();

        if target_revision > 0 && single_key {
            // 历史快照读：单键查询指定 Revision 时的值
            if let Some(value) = target
                .mvcc
                .get_at_revision(&req.key, target_revision)
                .map_err(map_err)?
            {
                let meta = target.mvcc.get_kv_metadata(&req.key).map_err(map_err)?;
                // 历史读取：使用查询 revision 作为 mod_revision
                let kv = if let Some(m) = meta {
                    KeyValue {
                        key: req.key.clone(),
                        value,
                        create_revision: m.create_revision,
                        mod_revision: target_revision as i64,
                        version: m.version,
                        lease_id: m.lease_id,
                    }
                } else {
                    KeyValue {
                        key: req.key.clone(),
                        value,
                        create_revision: target_revision as i64,
                        mod_revision: target_revision as i64,
                        version: 1,
                        lease_id: 0,
                    }
                };
                kvs.push(kv);
            }
        } else if single_key {
            // 单键精确查询（最新值）
            if let Some(value) = target.mvcc.get(&req.key).map_err(map_err)? {
                let meta = target.mvcc.get_kv_metadata(&req.key).map_err(map_err)?;
                let kv = to_kv_proto(&req.key, &value, meta.as_ref());
                kvs.push(kv);
            }
        } else if target_revision > 0 {
            // R-SVC-07-2：带 revision 的范围读走历史扫描（changelog 重建），
            // 返回目标 revision 的历史视图而非实时数据
            let results = target
                .mvcc
                .range_at_revision(&req.key, &req.range_end, limit, target_revision)
                .map_err(map_err)?;
            for (k, v) in results {
                kvs.push(to_kv_proto(&k, &v, None));
            }
        } else {
            // R-SVC-07-1：最新范围读，半开区间 [key, range_end)
            let results = target
                .mvcc
                .range_in(&req.key, &req.range_end, limit)
                .map_err(map_err)?;
            for (k, v) in results {
                let meta = target.mvcc.get_kv_metadata(&k).map_err(map_err)?;
                let kv = to_kv_proto(&k, &v, meta.as_ref());
                kvs.push(kv);
            }
        }

        // 对象存储启用时：保留对象空间 /obj/ 的 manifest 内部行对用户 KV 不可见
        //（Range 整体不可见——写侧已拒绝，读侧这里过滤，避免全 keyspace 扫描被破坏）
        if self.object_enabled() {
            kvs.retain(|kv| !crate::storage::object_store::key_in_object_space(&kv.key));
        }

        let count = kvs.len() as i64;
        let revision = if target_revision > 0 {
            target_revision as i64
        } else {
            target.mvcc.current_revision() as i64
        };

        if count_only {
            // 仅返回计数，不返回 kvs
            Ok(tonic::Response::new(RangeResponse {
                kvs: vec![],
                count,
                revision,
            }))
        } else {
            // 若 keys_only 为 true，清除 value 字段
            if keys_only {
                for kv in &mut kvs {
                    kv.value = Vec::new();
                }
            }

            Ok(tonic::Response::new(RangeResponse {
                kvs,
                count,
                revision,
            }))
        }
    }

    async fn delete(
        &self,
        request: tonic::Request<DeleteRequest>,
    ) -> Result<tonic::Response<DeleteResponse>, tonic::Status> {
        // 磁盘水位只读闸
        self.ensure_writable()?;

        let request_metadata = request.metadata().clone();
        let req = request.into_inner();
        let prev_kv_requested = req.prev_kv;
        let request_id = req.request_id.clone();

        // F-01（jepsen T1.4）：Delete 必须参与 `request_id` 去重。
        //
        // 契约（kv.proto:81）明确写了「幂等去重键（语义同 PutRequest.request_id）」
        // ——「重复提交相同 request_id 的请求不会重复生效，返回首次执行的结果」。
        // 修复前 delete 处理器里没有任何 `*_idempotent*` 调用，后果分三档：
        //   1. 计数/审计语义重复（删一次记两次）；
        //   2. `prev_kv=true` 的响应不可幂等：首次返回旧值、重试返回空
        //      `prev_kvs` —— 同一个 request_id 两次得到不同结果；
        //   3. **范围删重放会删掉首次执行之后新写入的数据**（重试放大破坏面，
        //      实测 39/39 复现）—— 这是数据破坏，不是计数问题。
        if !request_id.is_empty() {
            if let Some(entry) =
                self.check_idempotent_entry(&idempotency_key(&request_metadata, &request_id))
            {
                return Ok(tonic::Response::new(DeleteResponse {
                    deleted: entry.deleted,
                    prev_kvs: entry.prev_kvs,
                    revision: entry.revision,
                }));
            }
        }

        // R-SVC-07-3 / 第三轮 P0-1：range_end 非空且 != key → 原子范围删除 [key, range_end)；
        // 否则为单键删除。与顶层 Range / Txn 内层 op / 鉴权层共用同一份语义定义
        // （`coord_core::kv_range::RangeSemantics`）。
        let is_range = RangeSemantics::of(&req.key, &req.range_end).is_interval();

        // 保留对象空间守卫（/obj/ 内部 manifest：禁止经 KV 范围删除）
        if is_range {
            self.guard_object_write_range(&req.key, &req.range_end)?;
        } else {
            self.guard_object_write_key(&req.key)?;
        }

        // 按 key 解析目标 Region；范围删除不得越过 Region 边界。
        let target = self.kv_target_for_key(&req.key)?;
        if is_range {
            self.guard_range_in_target(&target, &req.key, &req.range_end)?;
        }

        // 线性一致性读：确保能看到最新数据后再扫描要删除的 Key。
        // 屏障按目标 Region 的 raft/log_store 执行。
        self.ensure_linearizable_on(
            target.raft.as_deref(),
            target.raft_log_store.as_ref(),
            target.region_id,
        )
        .await?;

        // 获取 prev_kv（如果需要）；R-SVC-18：范围扫描以 max_range_limit 封顶
        let max_range_limit = self.limits.read().max_range_limit;
        let prev_kvs: Vec<KeyValue> = if prev_kv_requested {
            if is_range {
                target
                    .mvcc
                    .range_in(&req.key, &req.range_end, max_range_limit)
                    .map_err(map_err)?
                    .into_iter()
                    .map(|(k, v)| {
                        let meta = target.mvcc.get_kv_metadata(&k).ok().flatten();
                        to_kv_proto(&k, &v, meta.as_ref())
                    })
                    .collect()
            } else {
                target
                    .mvcc
                    .get(&req.key)
                    .map_err(map_err)?
                    .map(|value| {
                        let meta = target.mvcc.get_kv_metadata(&req.key).ok().flatten();
                        to_kv_proto(&req.key, &value, meta.as_ref())
                    })
                    .into_iter()
                    .collect()
            }
        } else {
            vec![]
        };

        let (deleted, revision): (i64, i64) = if is_range {
            // 范围删除：单个 raft Command::DeleteRange 原子执行（R-SVC-07-3）
            if let Some(raft) = &target.raft {
                let cmd = Command::DeleteRange {
                    key: req.key.clone(),
                    range_end: req.range_end.clone(),
                };
                let resp = self
                    .client_write_with_timeout(raft, cmd, target.region_id)
                    .await?;
                match resp.response() {
                    Response::DeleteRange { revision, deleted } => {
                        (*deleted as i64, *revision as i64)
                    }
                    _ => return Err(tonic::Status::internal("unexpected raft response")),
                }
            } else {
                let (rev, deleted) = target
                    .mvcc
                    .delete_range(&req.key, &req.range_end)
                    .map_err(map_err)?;
                (deleted as i64, rev as i64)
            }
        } else {
            // 单键删除（原有逻辑）
            let exists = target.mvcc.get(&req.key).map_err(map_err)?.is_some();
            if let Some(raft) = &target.raft {
                let cmd = Command::Delete {
                    key: req.key.clone(),
                };
                let resp = self
                    .client_write_with_timeout(raft, cmd, target.region_id)
                    .await?;
                if let Response::Delete { revision: rev } = resp.response() {
                    if exists {
                        (1, *rev as i64)
                    } else {
                        (0, *rev as i64)
                    }
                } else {
                    return Err(tonic::Status::internal("unexpected raft response"));
                }
            } else if exists {
                let rev = target.mvcc.delete(&req.key).map_err(map_err)?;
                (1, rev as i64)
            } else {
                (0, 0)
            }
        };

        tracing::debug!(deleted, revision, "KV delete applied");
        // F-01：缓存完整 DeleteResponse，命中时整体回放（见上面的入口检查）
        if !request_id.is_empty() {
            self.cache_idempotent_delete(
                idempotency_key(&request_metadata, &request_id),
                deleted,
                prev_kvs.clone(),
                revision,
            );
        }
        Ok(tonic::Response::new(DeleteResponse {
            deleted,
            prev_kvs,
            revision,
        }))
    }
}

// ──── Txn Service ────

fn convert_compare(c: &Compare) -> Result<TxnCompare, tonic::Status> {
    use crate::txn::{CompareOp, CompareTarget, CompareValue};
    use coord_proto::txn::compare::{CompareResult, Target};

    let op = match CompareResult::try_from(c.result) {
        Ok(CompareResult::Equal) => CompareOp::Equal,
        Ok(CompareResult::Greater) => CompareOp::Greater,
        Ok(CompareResult::Less) => CompareOp::Less,
        Ok(CompareResult::NotEqual) => CompareOp::NotEqual,
        Err(_) => return Err(tonic::Status::invalid_argument("unknown compare result")),
    };

    let target = match Target::try_from(c.target) {
        Ok(Target::Version) => CompareTarget::Version,
        Ok(Target::Value) => CompareTarget::Value,
        Ok(Target::ModRev) => CompareTarget::ModRevision,
        Err(_) => return Err(tonic::Status::invalid_argument("unknown compare target")),
    };

    let target_value = match c.target_value.as_ref() {
        Some(coord_proto::txn::compare::TargetValue::Version(v)) => CompareValue::Version(*v),
        Some(coord_proto::txn::compare::TargetValue::Value(v)) => CompareValue::Value(v.clone()),
        Some(coord_proto::txn::compare::TargetValue::ModRevision(v)) => {
            CompareValue::ModRevision(*v)
        }
        None => return Err(tonic::Status::invalid_argument("missing target value")),
    };

    Ok(TxnCompare {
        key: c.key.clone(),
        op,
        target,
        target_value,
    })
}

fn convert_request_op(op: &RequestOp) -> Result<TxnOp, tonic::Status> {
    match op.op.as_ref() {
        Some(coord_proto::txn::request_op::Op::RequestPut(p)) => Ok(TxnOp::Put {
            key: p.key.clone(),
            value: p.value.clone(),
            lease_id: if p.lease_id != 0 {
                Some(p.lease_id)
            } else {
                None
            },
        }),
        Some(coord_proto::txn::request_op::Op::RequestDelete(d)) => {
            Ok(TxnOp::Delete { key: d.key.clone() })
        }
        Some(coord_proto::txn::request_op::Op::RequestRange(r)) => Ok(TxnOp::Range {
            key: r.key.clone(),
            range_end: r.range_end.clone(),
            limit: r.limit,
        }),
        None => Err(tonic::Status::invalid_argument("empty request op")),
    }
}

fn convert_response_op(resp: &TxnOpResponse) -> ResponseOp {
    match resp {
        TxnOpResponse::Put { revision } => ResponseOp {
            op: Some(coord_proto::txn::response_op::Op::ResponsePut(
                PutResponse {
                    prev_kv: None,
                    revision: *revision as i64,
                },
            )),
        },
        TxnOpResponse::Delete { revision } => ResponseOp {
            op: Some(coord_proto::txn::response_op::Op::ResponseDelete(
                DeleteResponse {
                    deleted: 1,
                    prev_kvs: vec![],
                    revision: *revision as i64,
                },
            )),
        },
        TxnOpResponse::Range {
            kvs,
            count,
            revision,
        } => {
            let proto_kvs: Vec<KeyValue> =
                kvs.iter().map(|(k, v)| to_kv_proto(k, v, None)).collect();
            ResponseOp {
                op: Some(coord_proto::txn::response_op::Op::ResponseRange(
                    RangeResponse {
                        kvs: proto_kvs,
                        count: *count,
                        revision: *revision as i64,
                    },
                )),
            }
        }
    }
}

#[tonic::async_trait]
impl Txn for CoordNode {
    async fn txn(
        &self,
        request: tonic::Request<TxnRequest>,
    ) -> Result<tonic::Response<TxnResponse>, tonic::Status> {
        // 磁盘水位只读闸
        self.ensure_writable()?;

        let request_metadata = request.metadata().clone();
        let req = request.into_inner();
        let request_id = req.request_id.clone();

        // 幂等检查：相同（客户端身份 + request_id）返回缓存结果（含完整 responses）
        if !request_id.is_empty() {
            if let Some((cached_succeeded, cached_rev, cached_responses)) =
                self.check_idempotent_txn(&idempotency_key(&request_metadata, &request_id))
            {
                return Ok(tonic::Response::new(TxnResponse {
                    succeeded: cached_succeeded,
                    responses: cached_responses,
                    revision: cached_rev,
                }));
            }
        }

        // R-SVC-18：Txn 规模上限——compare + success + failure 总操作数超限直接拒绝
        let max_txn_ops = self.limits.read().max_txn_ops;
        let total_ops = req.compare.len() + req.success.len() + req.failure.len();
        if max_txn_ops > 0 && total_ops > max_txn_ops {
            return Err(tonic::Status::invalid_argument(format!(
                "txn ops {total_ops} exceeds max {max_txn_ops}"
            )));
        }

        let compares: Vec<TxnCompare> = req
            .compare
            .iter()
            .map(convert_compare)
            .collect::<Result<Vec<_>, _>>()?;

        let success_ops: Vec<TxnOp> = req
            .success
            .iter()
            .map(convert_request_op)
            .collect::<Result<Vec<_>, _>>()?;

        let failure_ops: Vec<TxnOp> = req
            .failure
            .iter()
            .map(convert_request_op)
            .collect::<Result<Vec<_>, _>>()?;

        // 保留对象空间守卫：Txn 不得读写 /obj/（manifest 内部；Txn 内 Range 无
        // 过滤能力 → 整体拒绝）
        if self.object_enabled() {
            for c in &compares {
                self.guard_object_write_key(&c.key)?;
            }
            for op in success_ops.iter().chain(failure_ops.iter()) {
                match op {
                    TxnOp::Put { key, .. } | TxnOp::Delete { key } => {
                        self.guard_object_write_key(key)?
                    }
                    TxnOp::Range { key, range_end, .. } => {
                        self.guard_object_write_range(key, range_end)?
                    }
                }
            }
        }

        // R-SVC-18：Txn 内 Range op 的 limit 同样受 max_range_limit 约束
        {
            let max_range_limit = self.limits.read().max_range_limit;
            for op in success_ops.iter().chain(failure_ops.iter()) {
                if let TxnOp::Range { limit, .. } = op {
                    if max_range_limit > 0 && *limit > max_range_limit as i64 {
                        return Err(tonic::Status::invalid_argument(format!(
                            "txn range limit {limit} exceeds max {max_range_limit}"
                        )));
                    }
                }
            }
        }

        // 整个 Txn 是单 Region 原子单元——所有 compare/op 引用的 key 必须落在
        // 同一 Region。先取第一个引用的 key 路由出目标，再逐一校验同区（key 越界 /
        // 范围越界 → INVALID_ARGUMENT）。
        let first_key: Option<&[u8]> = compares.first().map(|c| c.key.as_slice()).or_else(|| {
            success_ops
                .iter()
                .chain(failure_ops.iter())
                .map(|op| match op {
                    TxnOp::Put { key, .. } | TxnOp::Delete { key } => key.as_slice(),
                    TxnOp::Range { key, .. } => key.as_slice(),
                })
                .next()
        });
        let target = match first_key {
            Some(k) => self.kv_target_for_key(k)?,
            // 无任何 key 引用的空 Txn：region 模式无法路由（每 Region 独立
            // revision/MVCC），显式拒绝；legacy 模式返回 region 0 目标（保持现状）。
            None => {
                if self.region_manager.is_some() {
                    return Err(tonic::Status::invalid_argument(
                        "txn references no key; cannot route to a region",
                    ));
                }
                self.kv_target_for_key(b"")?
            }
        };
        for c in &compares {
            self.guard_key_in_target(&target, &c.key)?;
        }
        for op in success_ops.iter().chain(failure_ops.iter()) {
            match op {
                TxnOp::Put { key, .. } | TxnOp::Delete { key } => {
                    self.guard_key_in_target(&target, key)?;
                }
                TxnOp::Range { key, range_end, .. } => {
                    self.guard_range_in_target(&target, key, range_end)?;
                }
            }
        }
        // region 模式：Txn 内 Put（无论 success 还是 failure 分支）允许绑定 Lease
        // （Lease 记录存 region 0，绑定 Key 落所属 Region，清理见
        // region raft DeleteKeysByLease）。

        // 通过 Raft 共识提交（集群模式），或直接执行（单节点模式）。
        // raft/storage 均取自目标 Region（compare+execute 在单 Region 状态机原子执行）。
        let result = if let Some(raft) = &target.raft {
            let cmd = Command::Txn {
                compares: compares.clone(),
                success_ops: success_ops.clone(),
                failure_ops: failure_ops.clone(),
            };
            let resp = self
                .client_write_with_timeout(raft, cmd, target.region_id)
                .await?;
            match resp.response() {
                Response::Txn {
                    succeeded,
                    revision,
                    responses,
                } => crate::txn::TxnResult {
                    succeeded: *succeeded,
                    revision: *revision,
                    responses: responses.to_vec(),
                },
                _ => return Err(tonic::Status::internal("unexpected raft response")),
            }
        } else {
            target
                .mvcc
                .execute_txn(&compares, &success_ops, &failure_ops)
                .map_err(map_err)?
        };

        let responses: Vec<ResponseOp> = result.responses.iter().map(convert_response_op).collect();

        // 若 Txn 成功执行，将 success_ops 中所有带 lease_id 的 Put 操作的 key 绑定到对应 Lease
        // （与 Put handler 保持一致的 Lease-Key 绑定语义，确保 Revoke/Expiry 时能正确清理）
        if result.succeeded {
            if let Some(ref lm) = self.lease_manager {
                for op in &success_ops {
                    if let TxnOp::Put {
                        key,
                        lease_id: Some(lid),
                        ..
                    } = op
                    {
                        let _ = lm.attach_key(*lid, key);
                    }
                }
            }
        }

        // 缓存幂等结果（R-SVC-18：连同完整 responses 缓存，命中时回放）
        if !request_id.is_empty() {
            self.cache_idempotent_txn(
                idempotency_key(&request_metadata, &request_id),
                result.succeeded,
                result.revision as i64,
                responses.clone(),
            );
        }

        tracing::debug!(
            succeeded = result.succeeded,
            revision = result.revision,
            "KV txn applied"
        );
        Ok(tonic::Response::new(TxnResponse {
            succeeded: result.succeeded,
            responses,
            revision: result.revision as i64,
        }))
    }
}

// ──── Lease Service ────

#[tonic::async_trait]
impl Lease for CoordNode {
    type LeaseKeepAliveStream =
        tokio_stream::wrappers::ReceiverStream<Result<LeaseKeepAliveResponse, tonic::Status>>;

    async fn lease_grant(
        &self,
        request: tonic::Request<LeaseGrantRequest>,
    ) -> Result<tonic::Response<LeaseGrantResponse>, tonic::Status> {
        // 磁盘水位只读闸
        self.ensure_writable()?;

        let req = request.into_inner();
        // B.4.1：仅 leader 接受（含 leader 提示）
        self.ensure_lease_leader().await?;
        let lease_mgr = self
            .lease_manager
            .as_ref()
            .ok_or_else(|| tonic::Status::unavailable("lease manager not available"))?;

        // C1（跨节点收口，第三轮）：分配 ID **之前**做一次线性一致读屏障。
        //
        // 上一轮只把「检查 + 插入」收进同一写锁临界区（消除**进程内** TOCTOU），
        // 但 `id_taken` 读的是**本地已 apply 视图**（`get_lease_record`）而没有任何
        // 屏障：leader 切换后，新主若尚未 apply 到前任已提交的那条 Grant，
        // 两个客户端就能用同一显式 ID 通过检查 → 状态机层被静默覆盖。
        //
        // ReadIndex 返回时本地状态机已追上集群提交位，因此下面的存在性检查看到的是
        // **完整的已提交历史**。这是本路径唯一需要的线性化点。
        self.ensure_linearizable_on(self.raft.as_deref(), self.raft_log_store.as_ref(), 0)
            .await?;

        // LeaseManager 负责 TTL 校验与 ID 分配（内存 TTL 缓存）
        // C1：分配时**同时核对状态机**（`/_lease/{id}`）——新 leader 上任到
        // `rebuild()` 完成之间进程内计数器可能落后，仅查内存 map 会分配到与
        // 状态机既有租约相同的 ID。
        let storage = Arc::clone(&self.storage);
        let id = lease_mgr
            .grant_with_id_checked(req.ttl, req.id, |candidate| {
                matches!(storage.get_lease_record(candidate), Ok(Some(_)))
            })
            .await
            .map_err(map_err)?;

        // Grant 入 raft 日志（状态机持久化 `/_lease/{id}`）
        let deadline_wall_ms = crate::lease::wall_clock_now_ms() + req.ttl * 1000;
        let op = LeaseOp::Grant {
            id,
            ttl: req.ttl,
            deadline_wall_ms,
        };
        let applied_revision = match self.submit_lease_op(op).await {
            Ok(rev) => rev,
            Err(e) => {
                // 失败则回滚本地缓存，避免幽灵 Lease
                let _ = lease_mgr.revoke(id).await;
                return Err(e);
            }
        };

        // C1 终检（纵深防御）：状态机对已存在的 `/_lease/{id}` **拒绝覆盖**。
        // 因此若本 ID 其实已被别人占用，本次 Grant 不会生效——读回的记录不是
        // 本次写入的那一条。此时必须回滚本地缓存并**明确报错**，绝不假装成功：
        // 共享 lease 会在到期时互相删除对方绑定的 Key。
        match self.storage.get_lease_record(id) {
            Ok(Some(record)) if record.keepalive_revision == applied_revision as i64 => {}
            Ok(_) => {
                let _ = lease_mgr.revoke(id).await;
                return Err(tonic::Status::already_exists(format!(
                    "lease id {id} is already in use; request a different id \
                     (or omit id to let the server allocate one)"
                )));
            }
            Err(e) => {
                // 读失败不阻断成功路径，但必须留下可诊断的痕迹
                tracing::warn!(
                    lease_id = id,
                    "lease grant post-apply verification read failed: {e}"
                );
            }
        }

        Ok(tonic::Response::new(LeaseGrantResponse {
            id,
            ttl: req.ttl,
            error: String::new(),
        }))
    }

    async fn lease_revoke(
        &self,
        request: tonic::Request<LeaseRevokeRequest>,
    ) -> Result<tonic::Response<LeaseRevokeResponse>, tonic::Status> {
        // 磁盘水位只读闸
        self.ensure_writable()?;

        let req = request.into_inner();
        // B.4.1：仅 leader 接受（含 leader 提示）
        self.ensure_lease_leader().await?;
        let lease_mgr = self
            .lease_manager
            .as_ref()
            .ok_or_else(|| tonic::Status::unavailable("lease manager not available"))?;

        // Revoke 走 raft（apply 内按 KvMetadata.lease_id 索引删除绑定 Key），
        // 禁止任何直写本地存储路径。
        let op = LeaseOp::Revoke {
            id: req.id,
            delete_keys: true,
        };
        self.submit_lease_op(op).await?;

        // 清理本地 TTL 缓存
        let _ = lease_mgr.revoke(req.id).await;

        Ok(tonic::Response::new(LeaseRevokeResponse {}))
    }

    async fn lease_keep_alive(
        &self,
        request: tonic::Request<tonic::Streaming<LeaseKeepAliveRequest>>,
    ) -> Result<tonic::Response<Self::LeaseKeepAliveStream>, tonic::Status> {
        let lease_mgr = self
            .lease_manager
            .as_ref()
            .ok_or_else(|| tonic::Status::unavailable("lease manager not available"))?;
        let lease_mgr = Arc::clone(lease_mgr);
        let raft = self.raft.clone();
        let storage = Arc::clone(&self.storage);
        let node_id = self.node_id;
        // C2：keep_alive 的 raft 提交必须带上超时（与 submit_lease_op 同口径）。
        let lease_timeout = self.limits.read().lease_timeout;

        let mut stream = request.into_inner();
        let (tx, rx) = mpsc::channel::<Result<LeaseKeepAliveResponse, tonic::Status>>(16);

        // 后台任务：持续接收客户端的 KeepAlive 请求并续约
        tokio::spawn(async move {
            while let Ok(Some(req)) = stream.message().await {
                // B.4.1：leader 转移后停止服务（客户端重连新 leader）
                let is_leader = match raft {
                    Some(ref raft) => raft.current_leader().await == Some(node_id),
                    None => true,
                };
                if !is_leader {
                    let _ = tx
                        .send(Err(tonic::Status::unavailable(format!(
                            "lease keep-alive rejected: node {node_id} is not the leader"
                        ))))
                        .await;
                    break;
                }

                // TTL 以本地缓存为准；deadline 由 leader 计算后随命令入日志（确定性）
                let ttl = match lease_mgr.get_lease(req.id) {
                    Some(lease) => lease.ttl_seconds,
                    None => {
                        let status = tonic::Status::not_found(format!(
                            "keep-alive failed: lease {} not found",
                            req.id
                        ));
                        let _ = tx.send(Err(status)).await;
                        break;
                    }
                };

                let deadline_wall_ms = crate::lease::wall_clock_now_ms() + ttl * 1000;
                let op = LeaseOp::KeepAlive {
                    id: req.id,
                    deadline_wall_ms,
                };

                // KeepAlive 入 raft 日志（推进 keepalive_revision）
                let raft_result = if let Some(ref raft) = raft {
                    let cmd = Command::Lease(op);
                    // C2：与 submit_lease_op 一致 —— quorum 丢失时必须在
                    // lease_timeout 内返回错误，不得无限挂起（R-SVC-18 同口径）。
                    match tokio::time::timeout(lease_timeout, raft.client_write(cmd)).await {
                        Ok(Ok(_)) => Ok(()),
                        Ok(Err(e)) => Err(tonic::Status::internal(format!(
                            "raft lease write failed: {e}"
                        ))),
                        Err(_) => Err(tonic::Status::deadline_exceeded(
                            "lease keep-alive timed out (no quorum?)",
                        )),
                    }
                } else {
                    storage
                        .apply_lease_op_standalone(&op)
                        .map(|_| ())
                        .map_err(map_err)
                };

                match raft_result {
                    Ok(_) => {
                        // B.4.3：响应携带服务端计算的剩余 TTL（非配置 TTL）
                        let remaining = match storage.get_lease_record(req.id) {
                            Ok(Some(record)) => crate::lease::remaining_ttl_from_deadline(
                                record.deadline_wall_ms,
                                crate::lease::wall_clock_now_ms(),
                            ),
                            _ => 0,
                        };
                        if remaining <= 0 {
                            let _ = tx
                                .send(Err(tonic::Status::not_found(format!(
                                    "keep-alive failed: lease {} expired",
                                    req.id
                                ))))
                                .await;
                            break;
                        }

                        // 同步本地 TTL 缓存
                        if let Err(e) = lease_mgr.keep_alive(req.id).await {
                            let _ = tx
                                .send(Err(tonic::Status::not_found(format!(
                                    "keep-alive failed: {e}"
                                ))))
                                .await;
                            break;
                        }
                        let resp = LeaseKeepAliveResponse {
                            id: req.id,
                            ttl: remaining,
                        };
                        if tx.send(Ok(resp)).await.is_err() {
                            // 客户端已断开连接，停止处理
                            break;
                        }
                    }
                    Err(status) => {
                        let _ = tx.send(Err(status)).await;
                        break;
                    }
                }
            }
        });

        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }
}

// ──── Watch Service ────

#[tonic::async_trait]
impl Watch for CoordNode {
    type WatchStream = tokio_stream::wrappers::ReceiverStream<Result<WatchResponse, tonic::Status>>;

    async fn watch(
        &self,
        request: tonic::Request<tonic::Streaming<WatchRequest>>,
    ) -> Result<tonic::Response<Self::WatchStream>, tonic::Status> {
        let mut stream = request.into_inner();
        let first_req = stream
            .message()
            .await
            .map_err(|e| tonic::Status::internal(format!("watch stream error: {e}")))?
            .ok_or_else(|| tonic::Status::invalid_argument("empty watch request"))?;

        let create_req = match first_req.request {
            Some(coord_proto::watch::watch_request::Request::Create(c)) => c,
            None => {
                return Err(tonic::Status::invalid_argument(
                    "first watch request must be create",
                ))
            }
        };

        // 保留对象空间守卫：Watch 不得订阅 /obj/（内部 manifest，事件不隔离）
        if self.object_enabled()
            && crate::storage::object_store::range_touches_object_space(
                &create_req.key,
                &create_req.range_end,
            )
        {
            return Err(tonic::Status::invalid_argument(
                "watch on reserved object-storage namespace /obj/ is not supported",
            ));
        }

        // 解析 Watch 目标——legacy 用节点级 dispatcher + storage；
        // region 模式路由到所属 Region 的 per-Region dispatcher + MVCC（per-Region
        // revision 语义）；跨 Region 区间/前缀在此显式拒绝（resolve_watch_target）。
        let (dispatcher, target_mvcc) =
            self.resolve_watch_target(&create_req.key, &create_req.range_end)?;

        let watch_req = crate::watch::WatchRequest {
            key: create_req.key.clone(),
            range_end: create_req.range_end.clone(),
            start_revision: create_req.start_revision as u64,
        };

        // 先注册取水位 R0（current_revision；region 模式 = 该 Region 的
        //        revision），回放 [start, R0]，实时从 R0+1 续并按 revision 去重。
        let watermark_rev = target_mvcc.current_revision();

        let (watch_id, mut event_rx) = dispatcher
            .subscribe(
                watch_req,
                self.watch_buffer.load(std::sync::atomic::Ordering::Relaxed),
                watermark_rev,
            )
            .map_err(tonic::Status::resource_exhausted)?;

        let (tx, rx) = mpsc::channel::<Result<WatchResponse, tonic::Status>>(16);

        let dispatcher_ref = Arc::clone(&dispatcher);
        let storage_ref = Arc::clone(&target_mvcc);
        let start_rev = create_req.start_revision as u64;
        let key_prefix = create_req.key;
        let range_end = create_req.range_end;

        tokio::spawn(async move {
            // 如果指定了 start_revision > 0，先回放历史事件（止于水位）
            if start_rev > 0 {
                let dispatcher_for_replay = Arc::clone(&dispatcher_ref);
                let (history_tx, mut history_rx) = mpsc::channel::<crate::watch::WatchEvent>(256);

                let key_p = key_prefix.clone();
                let range_e = range_end.clone();
                let reader: Arc<dyn crate::watch::ChangelogReader> = storage_ref;
                let history_tx_for_closure = history_tx.clone();

                let replay_result = tokio::task::spawn_blocking(move || {
                    dispatcher_for_replay.replay_history(
                        watch_id,
                        &history_tx_for_closure,
                        &key_p,
                        &range_e,
                        start_rev,
                        watermark_rev,
                        reader.as_ref(),
                    )
                })
                .await;

                match replay_result {
                    Ok(Ok(())) => {
                        // 回放成功：drain 历史事件并发送
                        drop(history_tx); // 关闭 sender 使 receiver 可以终止
                        while let Some(event) = history_rx.recv().await {
                            if let Some(resp) = convert_watch_event_to_response(watch_id, &event) {
                                if tx.send(Ok(resp)).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        tracing::warn!(watch_id, "watch history replay failed: {e}");
                        // 损坏/不可用 → HistoryUnavailable 事件通知客户端
                        let resp = WatchResponse {
                            watch_id: watch_id as i64,
                            events: vec![WatchEvent {
                                r#type:
                                    coord_proto::watch::watch_event::EventType::HistoryUnavailable
                                        as i32,
                                kvs: vec![],
                                prev_kv: None,
                                revision: 0,
                            }],
                        };
                        let _ = tx.send(Ok(resp)).await;
                    }
                    Err(e) => {
                        tracing::warn!(watch_id, "spawn_blocking for watch replay panicked: {e}");
                    }
                }
            }

            // 实时事件循环（溢出合成 + revision 去重）
            loop {
                // 溢出标志：满时置位 → 合成 BufferOverflow（必达）
                if dispatcher_ref.take_overflow(watch_id) {
                    let resp = WatchResponse {
                        watch_id: watch_id as i64,
                        events: vec![WatchEvent {
                            r#type: coord_proto::watch::watch_event::EventType::BufferOverflow
                                as i32,
                            kvs: vec![],
                            prev_kv: None,
                            revision: 0,
                        }],
                    };
                    if tx.send(Ok(resp)).await.is_err() {
                        break;
                    }
                }

                // 第四轮 §3.6 c：**客户端断开即回收**。
                //
                // 旧实现的 per-stream 循环只能通过 `tx.send(..).is_err()` 或
                // `event_rx.recv()` 返回 None 退出。而对一个**空闲**订阅，
                // `event_rx.recv()` 在 `Subscriber`（持有 `event_tx`）仍在 map 中时
                // **永不返回 None**，`tx.send` 也**永不被调用**——于是客户端断开后
                // 该任务永久 parked，`unsubscribe(watch_id)` 永不可达：
                // 每轮泄漏 1 个任务 + 1 个 Subscriber + 1 个 mpsc block；
                // 达 10000 上限后 `subscribe` 返回 Err → **watch 对所有客户端永久
                // 不可用，且不自愈**。`tx.closed()` 在接收端 drop 后立即就绪。
                let event = tokio::select! {
                    _ = tx.closed() => break,
                    event = event_rx.recv() => event,
                };
                match event {
                    Some(event) => {
                        // 去重——仅投递水位之后的实时事件（回放已覆盖 ≤ 水位）
                        if event
                            .events
                            .iter()
                            .all(|item| item.revision <= watermark_rev)
                        {
                            continue;
                        }
                        if let Some(resp) = convert_watch_event_to_response(watch_id, &event) {
                            if tx.send(Ok(resp)).await.is_err() {
                                break;
                            }
                        }
                    }
                    None => break,
                }
            }
            dispatcher_ref.unsubscribe(watch_id);
        });

        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }
}

/// 将内部 WatchEvent 转换为 protobuf WatchResponse
fn convert_watch_event_to_response(
    watch_id: u64,
    event: &crate::watch::WatchEvent,
) -> Option<WatchResponse> {
    let proto_events: Vec<WatchEvent> = event
        .events
        .iter()
        .map(|item| {
            let kvs: Vec<KeyValue> = item
                .kvs
                .iter()
                .map(|kv| KeyValue {
                    key: kv.key.clone(),
                    value: kv.value.clone().unwrap_or_default(),
                    create_revision: 0,
                    mod_revision: item.revision as i64,
                    version: 1,
                    lease_id: 0,
                })
                .collect();

            let event_type = match item.event_type {
                crate::watch::WatchEventType::Put => {
                    coord_proto::watch::watch_event::EventType::Put
                }
                crate::watch::WatchEventType::Delete => {
                    coord_proto::watch::watch_event::EventType::Delete
                }
                crate::watch::WatchEventType::BufferOverflow => {
                    coord_proto::watch::watch_event::EventType::BufferOverflow
                }
                crate::watch::WatchEventType::HistoryUnavailable => {
                    coord_proto::watch::watch_event::EventType::HistoryUnavailable
                }
            };

            WatchEvent {
                r#type: event_type as i32,
                kvs,
                prev_kv: None,
                revision: item.revision as i64,
            }
        })
        .collect();

    Some(WatchResponse {
        watch_id: watch_id as i64,
        events: proto_events,
    })
}

// ──── Maintenance Service ────

#[tonic::async_trait]
impl Maintenance for CoordNode {
    type SnapshotStream =
        tokio_stream::wrappers::ReceiverStream<Result<SnapshotResponse, tonic::Status>>;

    async fn seal(
        &self,
        _request: tonic::Request<SealRequest>,
    ) -> Result<tonic::Response<SealResponse>, tonic::Status> {
        // 接线真实 Seal（此前为 unimplemented stub）
        let keyring = self.keyring.read().clone().ok_or_else(|| {
            tonic::Status::failed_precondition("static encryption is not enabled on this node")
        })?;
        keyring.seal();
        tracing::info!("Cluster sealed: key material zeroized; writes/reads refused");
        Ok(tonic::Response::new(SealResponse {}))
    }

    async fn unseal(
        &self,
        request: tonic::Request<UnsealRequest>,
    ) -> Result<tonic::Response<UnsealResponse>, tonic::Status> {
        // 接线真实 Unseal（此前为 unimplemented stub）。
        // 优先使用 Shamir 分片；其次使用 root 密钥提供者（配置/环境变量/密钥文件）。
        if !self.keyring.read().as_ref().is_some_and(|k| k.is_sealed()) {
            return Err(tonic::Status::failed_precondition(
                "keyring is not sealed; nothing to unseal",
            ));
        }
        let req = request.into_inner();
        let encrypted_deks = self.encrypted_deks.read().clone();
        if encrypted_deks.is_empty() {
            return Err(tonic::Status::failed_precondition(
                "no persisted encrypted DEKs found; cannot unseal",
            ));
        }

        // 路径 1：Shamir 分片解封
        let mut shares = Vec::new();
        if !req.shares.is_empty() {
            for raw in &req.shares {
                match crate::security::seal::Share::from_bytes(raw) {
                    Ok(s) => shares.push(s),
                    Err(e) => {
                        return Err(tonic::Status::invalid_argument(format!(
                            "invalid share: {e}"
                        )))
                    }
                }
            }
        }

        let recovered = if !shares.is_empty() {
            Keyring::unseal(&shares, &encrypted_deks)
                .map_err(|e| tonic::Status::unauthenticated(format!("unseal failed: {e}")))?
        } else if let Some(provider) = &self.root_key_provider {
            let root_key = provider().ok_or_else(|| {
                tonic::Status::failed_precondition(
                    "no root key available for unseal (set security.encryption_root_key)",
                )
            })?;
            Keyring::from_root_key(&root_key, &encrypted_deks)
                .map_err(|e| tonic::Status::unauthenticated(format!("unseal failed: {e}")))?
        } else {
            return Err(tonic::Status::failed_precondition(
                "unseal requires Shamir shares or a root key provider",
            ));
        };

        // 重新接线 Barrier 与 Keyring
        let recovered = Arc::new(recovered);
        let barrier = Barrier::new(Arc::clone(&recovered));
        self.storage.set_barrier(barrier);
        *self.keyring.write() = Some(Arc::clone(&recovered));
        tracing::info!("Cluster unsealed: keyring restored, encryption active");
        Ok(tonic::Response::new(UnsealResponse {
            nodes_unsealed: 1,
            total_nodes: 1,
        }))
    }

    async fn status(
        &self,
        _request: tonic::Request<StatusRequest>,
    ) -> Result<tonic::Response<StatusResponse>, tonic::Status> {
        let revision = self.storage.current_revision();

        let (raft_index, raft_term, raft_leader) = if let Some(ref raft) = self.raft {
            let m = raft.metrics().borrow_watched().clone();
            let leader = raft.current_leader().await;
            (
                m.last_applied.as_ref().map(|id| id.index).unwrap_or(0) as i64,
                m.current_term,
                leader.map(|id| id.to_string()).unwrap_or_default(),
            )
        } else {
            (0i64, 0u64, String::new())
        };

        // seal_status 返回真实状态（此前硬编码 "unsealed"）
        let seal_status = match self.keyring.read().as_ref() {
            Some(k) if k.is_sealed() => "sealed".to_string(),
            Some(_) => "unsealed".to_string(),
            None => "unsealed".to_string(), // 未启用静态加密
        };

        Ok(tonic::Response::new(StatusResponse {
            revision: revision as i64,
            raft_index,
            raft_term,
            raft_leader,
            seal_status,
        }))
    }

    async fn snapshot(
        &self,
        request: tonic::Request<SnapshotRequest>,
    ) -> Result<tonic::Response<Self::SnapshotStream>, tonic::Status> {
        // 流式快照导出（在线备份）。从本地状态机导出（任何节点可服务，
        // 运维建议从 leader 或已追平 follower 拉取；数据为 v2 格式密文直传，
        // 不经过 Barrier）。首块携带 last_included_index/term，客户端按块拼接。
        // region_id > 0 时导出对应 Region（region ≥1 的目录级
        // 隔离 MVCC）——region 0 / 缺省（0）= legacy 单 Raft 或 system raft。
        let region_id = request.into_inner().region_id;
        let target_mvcc: Arc<MvccStorage<RedbBackend>> = if region_id > 0 {
            let Some(manager) = &self.region_manager else {
                return Err(tonic::Status::failed_precondition(
                    "region_id > 0 requires multi_raft mode",
                ));
            };
            let Some(rt) = manager.runtime(region_id) else {
                return Err(tonic::Status::not_found(format!(
                    "region {region_id} not found on this node"
                )));
            };
            Arc::clone(&rt.mvcc)
        } else {
            Arc::clone(&self.storage)
        };

        let applied = target_mvcc.get_applied_log_id().map_err(map_err)?;
        let last_included_index = applied.map(|a| a.index).unwrap_or(0);
        let last_included_term = applied.map(|a| a.term).unwrap_or(0);

        let snapshot_data = crate::storage::snapshot::export_snapshot_data(
            &target_mvcc,
            last_included_index,
            last_included_term,
        )
        .map_err(|e| tonic::Status::internal(format!("export snapshot: {e}")))?;
        let bytes = snapshot_data
            .to_bytes()
            .map_err(|e| tonic::Status::internal(format!("serialize snapshot: {e}")))?;

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<SnapshotResponse, tonic::Status>>(4);
        tokio::spawn(async move {
            const CHUNK: usize = 1024 * 1024; // 1MiB/块（流式传输内存上限）
            for (i, chunk) in bytes.chunks(CHUNK).enumerate() {
                let resp = SnapshotResponse {
                    data: chunk.to_vec(),
                    last_included_index: if i == 0 {
                        last_included_index as i64
                    } else {
                        0
                    },
                    last_included_term: if i == 0 { last_included_term } else { 0 },
                };
                if tx.send(Ok(resp)).await.is_err() {
                    break; // 客户端断开
                }
            }
        });

        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }

    // ──── Compaction────

    async fn compact(
        &self,
        request: tonic::Request<CompactRequest>,
    ) -> Result<tonic::Response<CompactResponse>, tonic::Status> {
        // 磁盘水位只读闸（compact 虽删除数据但需写入 raft 日志）
        self.ensure_writable()?;

        let req = request.into_inner();

        // 仅 leader 接受（非 leader 返回 UNAVAILABLE + leader 提示）
        if let Some(ref raft) = self.raft {
            let leader = raft.current_leader().await;
            if leader != Some(self.node_id) {
                let forward = leader
                    .and_then(|l| self.grpc_addr_of(l))
                    .unwrap_or_default();
                return Err(tonic::Status::unavailable(format!(
                    "not leader (leader={leader:?}); retry against the leader at {forward}"
                )));
            }
        }

        // 前置校验：revision 0 = 压缩到当前；未来 revision → INVALID_ARGUMENT
        let current = self.storage.current_revision();
        let revision = if req.revision <= 0 {
            current
        } else {
            req.revision as u64
        };
        if revision > current {
            return Err(tonic::Status::invalid_argument(format!(
                "compact revision {revision} is in the future (current={current})"
            )));
        }

        let compacted = self.compact_impl(revision).await.map_err(|e| {
            tracing::error!("compact failed: {e}");
            tonic::Status::internal(e)
        })?;

        tracing::debug!(compacted, "compact applied");
        Ok(tonic::Response::new(CompactResponse {
            compacted_revision: compacted as i64,
            revision: self.storage.current_revision() as i64,
        }))
    }

    // ──── Member Management ────

    async fn member_add(
        &self,
        request: tonic::Request<MemberAddRequest>,
    ) -> Result<tonic::Response<MemberAddResponse>, tonic::Status> {
        let req = request.into_inner();
        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| tonic::Status::failed_precondition("not a raft node"))?;

        // 变更串行化 —— 集群级互斥，并发变更返回 UNAVAILABLE
        let _guard = self.try_lock_member_change().ok_or_else(|| {
            tonic::Status::unavailable("another membership change is in progress")
        })?;

        // Step 1: Add as learner（blocking=true 等待复制追平）
        let node = crate::raft::new_basic_node(&req.raft_addr);
        raft.add_learner(req.node_id, node, true)
            .await
            .map_err(|e| tonic::Status::internal(format!("add_learner failed: {e}")))?;

        // 注册 gRPC 地址（leader 重定向需要）
        self.register_grpc_addr(req.node_id, &req.grpc_addr);

        // Step 2: Promote to voter
        let mut voter_ids = std::collections::BTreeSet::new();
        voter_ids.insert(req.node_id);
        raft.change_membership(crate::raft::add_voter_ids(voter_ids), true)
            .await
            .map_err(|e| tonic::Status::internal(format!("change_membership failed: {e}")))?;

        Ok(tonic::Response::new(MemberAddResponse {
            success: true,
            message: format!(
                "node {} added as voter (grpc={}, raft={})",
                req.node_id, req.grpc_addr, req.raft_addr
            ),
        }))
    }

    async fn member_remove(
        &self,
        request: tonic::Request<MemberRemoveRequest>,
    ) -> Result<tonic::Response<MemberRemoveResponse>, tonic::Status> {
        let req = request.into_inner();
        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| tonic::Status::failed_precondition("not a raft node"))?;

        let _guard = self.try_lock_member_change().ok_or_else(|| {
            tonic::Status::unavailable("another membership change is in progress")
        })?;

        // / D.2.4：remove 目标是 leader。
        // 设计决策（见 evidence/m2.md 偏差记录）：openraft 0.10 支持 leader
        // 自移除 —— change_membership(RemoveVoters) 经 joint→uniform 配置提交，
        // 新配置提交后旧 leader 自动退位、剩余 quorum 继续服务。若先显式
        // transfer_leader，本节点即成为 follower，反而无法再提交该变更
        // （openraft 拒绝非 leader 的 change_membership），而内部 gRPC 转发会
        // 破坏鉴权 fail-closed 模型。显式移交执行器（transfer_leadership）
        // 供维护/优雅停机路径使用。
        if raft.current_leader().await == Some(req.node_id) {
            tracing::info!(
                "Removing leader node {} via openraft self-removal \
                 (auto step-down after config commit)",
                req.node_id
            );
        }

        let mut remove_ids = std::collections::BTreeSet::new();
        remove_ids.insert(req.node_id);
        raft.change_membership(crate::raft::remove_voter_ids(remove_ids), true)
            .await
            .map_err(|e| {
                tonic::Status::failed_precondition(format!("change_membership failed: {e}"))
            })?;

        Ok(tonic::Response::new(MemberRemoveResponse {
            success: true,
            message: format!("node {} removed from cluster", req.node_id),
        }))
    }

    async fn member_promote(
        &self,
        request: tonic::Request<MemberPromoteRequest>,
    ) -> Result<tonic::Response<MemberPromoteResponse>, tonic::Status> {
        let req = request.into_inner();
        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| tonic::Status::failed_precondition("not a raft node"))?;

        let _guard = self.try_lock_member_change().ok_or_else(|| {
            tonic::Status::unavailable("another membership change is in progress")
        })?;

        let mut voter_ids = std::collections::BTreeSet::new();
        voter_ids.insert(req.node_id);
        raft.change_membership(crate::raft::add_voter_ids(voter_ids), true)
            .await
            .map_err(|e| tonic::Status::internal(format!("change_membership failed: {e}")))?;

        Ok(tonic::Response::new(MemberPromoteResponse {
            success: true,
            message: format!("node {} promoted to voter", req.node_id),
        }))
    }

    async fn join(
        &self,
        request: tonic::Request<JoinRequest>,
    ) -> Result<tonic::Response<JoinResponse>, tonic::Status> {
        let req = request.into_inner();
        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| tonic::Status::failed_precondition("not a raft node"))?;

        // 非 leader 返回 leader 重定向（客户端重试到 leader）。
        // 注：未初始化/未加入的节点 current_leader 为 None，同样重定向
        // （重定向到已知的其它节点；绝不能在本节点自调 add_learner）。
        let leader = raft.current_leader().await;
        if leader != Some(self.node_id) {
            let forward_to = match leader {
                Some(l) => self.grpc_addr_of(l),
                None => self
                    .node_grpc_addrs
                    .read()
                    .iter()
                    .find(|(id, _)| **id != self.node_id)
                    .map(|(_, addr)| addr.clone()),
            }
            .unwrap_or_default();
            return Ok(tonic::Response::new(JoinResponse {
                success: false,
                message: format!("not leader (leader={leader:?}); retry against the leader"),
                forward_to,
            }));
        }

        let _guard = self.try_lock_member_change().ok_or_else(|| {
            tonic::Status::unavailable("another membership change is in progress")
        })?;

        // add_learner(blocking=true)：等待复制追平，再晋升 voter
        let node = crate::raft::new_basic_node(&req.raft_addr);
        raft.add_learner(req.node_id, node, true)
            .await
            .map_err(|e| tonic::Status::internal(format!("add_learner failed: {e}")))?;
        self.register_grpc_addr(req.node_id, &req.grpc_addr);

        let mut voter_ids = std::collections::BTreeSet::new();
        voter_ids.insert(req.node_id);
        raft.change_membership(crate::raft::add_voter_ids(voter_ids), true)
            .await
            .map_err(|e| tonic::Status::internal(format!("change_membership failed: {e}")))?;

        tracing::info!(
            "Join complete: node {} (raft={}) promoted to voter",
            req.node_id,
            req.raft_addr
        );
        Ok(tonic::Response::new(JoinResponse {
            success: true,
            message: format!("node {} joined as voter", req.node_id),
            forward_to: String::new(),
        }))
    }

    async fn member_list(
        &self,
        _request: tonic::Request<MemberListRequest>,
    ) -> Result<tonic::Response<MemberListResponse>, tonic::Status> {
        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| tonic::Status::failed_precondition("not a raft node"))?;

        let m = raft.metrics().borrow_watched().clone();
        let leader_id = raft.current_leader().await;

        // Build member list from membership config
        let mut nodes = Vec::new();
        let membership = &m.membership_config;

        // Collect voter IDs for role classification
        let voter_ids: std::collections::BTreeSet<u64> = membership.voter_ids().collect();

        // Iterate over all nodes (voters + learners)
        for (id, _node) in membership.nodes() {
            let role = if voter_ids.contains(id) {
                if leader_id == Some(*id) {
                    "Leader"
                } else {
                    "Voter"
                }
            } else {
                "Learner"
            };
            nodes.push(MemberNode {
                id: *id,
                role: role.to_string(),
            });
        }

        Ok(tonic::Response::new(MemberListResponse {
            nodes,
            leader_id: leader_id.unwrap_or(0),
        }))
    }
}

// ──── 测试 ────

#[cfg(test)]
mod tests {
    use super::*;

    // ──── to_kv_proto ────

    #[test]
    fn test_to_kv_proto_basic() {
        use crate::storage::mvcc::KvMetadata;
        let meta = KvMetadata {
            version: 3,
            create_revision: 10,
            mod_revision: 42,
            lease_id: 0,
            deleted: false,
        };
        let kv = to_kv_proto(b"hello", b"world", Some(&meta));
        assert_eq!(kv.key, b"hello");
        assert_eq!(kv.value, b"world");
        assert_eq!(kv.create_revision, 10);
        assert_eq!(kv.mod_revision, 42);
        assert_eq!(kv.version, 3);
        assert_eq!(kv.lease_id, 0);
    }

    #[test]
    fn test_to_kv_proto_empty_value() {
        let kv = to_kv_proto(b"empty", b"", None);
        assert_eq!(kv.key, b"empty");
        assert!(kv.value.is_empty());
        assert_eq!(kv.create_revision, 0);
    }

    #[test]
    fn test_to_kv_proto_no_metadata() {
        let kv = to_kv_proto(b"k", b"v", None);
        assert_eq!(kv.version, 1);
        assert_eq!(kv.create_revision, 0);
        assert_eq!(kv.mod_revision, 0);
    }

    // ──── map_err ────

    #[test]
    fn test_map_err_returns_internal_status() {
        let status = map_err("test error message");
        assert_eq!(status.code(), tonic::Code::Internal);
        // 脱敏：原始信息不回传客户端
        assert!(!status.message().contains("test error message"));
        assert_eq!(status.message(), "internal error");
    }

    #[test]
    fn test_map_err_with_display_type() {
        let err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let status = map_err(err);
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[test]
    fn test_map_err_classifies_core_errors() {
        use coord_core::error::Error;
        assert_eq!(
            map_err(Error::NotLeader { leader_addr: None }).code(),
            tonic::Code::Unavailable
        );
        assert_eq!(
            map_err(Error::NotFound {
                resource: "key",
                key: "k".into()
            })
            .code(),
            tonic::Code::NotFound
        );
        assert_eq!(
            map_err(Error::RevisionCompacted {
                revision: 5,
                oldest: 3
            })
            .code(),
            tonic::Code::OutOfRange
        );
        assert_eq!(
            map_err(Error::PermissionDenied("no".into())).code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(
            map_err(Error::TokenExpired).code(),
            tonic::Code::Unauthenticated
        );
    }

    #[test]
    fn test_map_err_sanitizes_internal_core_errors() {
        use coord_core::error::Error;
        let status = map_err(Error::Storage(
            "redb: table corrupted at offset 12345".into(),
        ));
        assert_eq!(status.code(), tonic::Code::Internal);
        assert!(
            !status.message().contains("redb"),
            "{} != sanitized",
            status.message()
        );
        assert_eq!(status.message(), "storage error");
    }

    // ──── EpochStale 映射（此前因 check_epoch 无生产调用方而不可达）────

    #[test]
    fn test_map_err_epoch_stale_unavailable() {
        use coord_core::error::Error;
        // v1 无 client-epoch 协议，但 RegionHandle::check_epoch 防御性返回的
        // EpochStale 必须映射为可重试的 UNAVAILABLE（客户端刷新路由表后重试），
        // 而非 5xx/INTERNAL。
        let status = map_err(Error::EpochStale {
            region_id: 3,
            client_conf_ver: 1,
            client_version: 1,
            server_conf_ver: 2,
            server_version: 1,
        });
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert!(
            status.message().contains("stale epoch"),
            "{} != stale-epoch message",
            status.message()
        );
    }

    #[test]
    fn test_map_region_errors_route_table_semantics() {
        use coord_core::error::Error;
        // 与 v1「服务端按 key 权威路由」配套的错误面：
        //  - RegionNotLeader → UNAVAILABLE + leader hint（客户端按 Region 重定向）
        //  - KeyNotInRegion → INVALID_ARGUMENT（跨 Region 写被显式拒绝，不静默错答）
        //  - RegionNotFound / RouteNotReady → 对应可重试状态
        let not_leader = map_err(Error::RegionNotLeader {
            region_id: 2,
            leader_addr: Some("10.0.0.2:50051".into()),
        });
        assert_eq!(not_leader.code(), tonic::Code::Unavailable);
        assert!(not_leader.message().contains("region 2"));

        let key_out = map_err(Error::KeyNotInRegion { region_id: 2 });
        assert_eq!(key_out.code(), tonic::Code::InvalidArgument);

        assert_eq!(
            map_err(Error::RegionNotFound { region_id: 9 }).code(),
            tonic::Code::NotFound
        );
        assert_eq!(
            map_err(Error::RouteNotReady).code(),
            tonic::Code::Unavailable
        );
    }

    // ──── convert_compare ────

    #[test]
    fn test_convert_compare_equal_version() {
        let c = coord_proto::txn::Compare {
            key: b"mykey".to_vec(),
            result: coord_proto::txn::compare::CompareResult::Equal as i32,
            target: coord_proto::txn::compare::Target::Version as i32,
            target_value: Some(coord_proto::txn::compare::TargetValue::Version(5)),
        };
        let result = convert_compare(&c).unwrap();
        assert_eq!(result.key, b"mykey");
        assert!(matches!(result.op, crate::txn::CompareOp::Equal));
        assert!(matches!(result.target, crate::txn::CompareTarget::Version));
    }

    #[test]
    fn test_convert_compare_greater_value() {
        let c = coord_proto::txn::Compare {
            key: b"k".to_vec(),
            result: coord_proto::txn::compare::CompareResult::Greater as i32,
            target: coord_proto::txn::compare::Target::Value as i32,
            target_value: Some(coord_proto::txn::compare::TargetValue::Value(
                b"val".to_vec(),
            )),
        };
        let result = convert_compare(&c).unwrap();
        assert!(matches!(result.op, crate::txn::CompareOp::Greater));
        assert!(matches!(result.target, crate::txn::CompareTarget::Value));
    }

    #[test]
    fn test_convert_compare_less_mod_revision() {
        let c = coord_proto::txn::Compare {
            key: b"k".to_vec(),
            result: coord_proto::txn::compare::CompareResult::Less as i32,
            target: coord_proto::txn::compare::Target::ModRev as i32,
            target_value: Some(coord_proto::txn::compare::TargetValue::ModRevision(10)),
        };
        let result = convert_compare(&c).unwrap();
        assert!(matches!(result.op, crate::txn::CompareOp::Less));
        assert!(matches!(
            result.target,
            crate::txn::CompareTarget::ModRevision
        ));
    }

    #[test]
    fn test_convert_compare_not_equal() {
        let c = coord_proto::txn::Compare {
            key: b"k".to_vec(),
            result: coord_proto::txn::compare::CompareResult::NotEqual as i32,
            target: coord_proto::txn::compare::Target::Version as i32,
            target_value: Some(coord_proto::txn::compare::TargetValue::Version(3)),
        };
        let result = convert_compare(&c).unwrap();
        assert!(matches!(result.op, crate::txn::CompareOp::NotEqual));
    }

    #[test]
    fn test_convert_compare_missing_target_value() {
        let c = coord_proto::txn::Compare {
            key: b"k".to_vec(),
            result: coord_proto::txn::compare::CompareResult::Equal as i32,
            target: coord_proto::txn::compare::Target::Version as i32,
            target_value: None,
        };
        let result = convert_compare(&c);
        assert!(result.is_err());
    }

    // ──── convert_request_op ────

    #[test]
    fn test_convert_request_op_put() {
        let op = coord_proto::txn::RequestOp {
            op: Some(coord_proto::txn::request_op::Op::RequestPut(
                coord_proto::kv::PutRequest {
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                    lease_id: 0,
                    prev_kv: false,
                    request_id: vec![],
                },
            )),
        };
        let result = convert_request_op(&op).unwrap();
        match result {
            crate::txn::TxnOp::Put {
                key,
                value,
                lease_id,
            } => {
                assert_eq!(key, b"k");
                assert_eq!(value, b"v");
                assert_eq!(lease_id, None);
            }
            _ => panic!("expected Put"),
        }
    }

    #[test]
    fn test_convert_request_op_delete() {
        let op = coord_proto::txn::RequestOp {
            op: Some(coord_proto::txn::request_op::Op::RequestDelete(
                coord_proto::kv::DeleteRequest {
                    key: b"del".to_vec(),
                    range_end: vec![],
                    prev_kv: false,
                    request_id: vec![],
                },
            )),
        };
        let result = convert_request_op(&op).unwrap();
        match result {
            crate::txn::TxnOp::Delete { key } => assert_eq!(key, b"del"),
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn test_convert_request_op_range() {
        let op = coord_proto::txn::RequestOp {
            op: Some(coord_proto::txn::request_op::Op::RequestRange(
                coord_proto::kv::RangeRequest {
                    key: b"prefix".to_vec(),
                    range_end: b"prefixz".to_vec(),
                    limit: 100,
                    revision: 0,
                    keys_only: false,
                    count_only: false,
                },
            )),
        };
        let result = convert_request_op(&op).unwrap();
        match result {
            crate::txn::TxnOp::Range {
                key,
                range_end,
                limit,
            } => {
                assert_eq!(key, b"prefix");
                assert_eq!(range_end, b"prefixz");
                assert_eq!(limit, 100);
            }
            _ => panic!("expected Range"),
        }
    }

    #[test]
    fn test_convert_request_op_empty() {
        let op = coord_proto::txn::RequestOp { op: None };
        let result = convert_request_op(&op);
        assert!(result.is_err());
    }

    // ──── convert_response_op ────

    #[test]
    fn test_convert_response_op_put() {
        let resp = crate::txn::TxnOpResponse::Put { revision: 42 };
        let proto = convert_response_op(&resp);
        match proto.op {
            Some(coord_proto::txn::response_op::Op::ResponsePut(p)) => {
                assert_eq!(p.revision, 42);
            }
            _ => panic!("expected ResponsePut"),
        }
    }

    #[test]
    fn test_convert_response_op_delete() {
        let resp = crate::txn::TxnOpResponse::Delete { revision: 7 };
        let proto = convert_response_op(&resp);
        match proto.op {
            Some(coord_proto::txn::response_op::Op::ResponseDelete(d)) => {
                assert_eq!(d.revision, 7);
                assert_eq!(d.deleted, 1);
            }
            _ => panic!("expected ResponseDelete"),
        }
    }

    #[test]
    fn test_convert_response_op_range() {
        let resp = crate::txn::TxnOpResponse::Range {
            kvs: vec![(b"k".to_vec(), b"v".to_vec())],
            count: 1,
            revision: 10,
        };
        let proto = convert_response_op(&resp);
        match proto.op {
            Some(coord_proto::txn::response_op::Op::ResponseRange(r)) => {
                assert_eq!(r.kvs.len(), 1);
                assert_eq!(r.count, 1);
                assert_eq!(r.revision, 10);
            }
            _ => panic!("expected ResponseRange"),
        }
    }

    // ──── CoordNode type verification ────

    /// CoordNode fields are pub for external access. This test verifies
    /// the struct definition compiles (compile-time assertion).
    #[test]
    fn test_coord_node_type_accessible() {
        // Verify helper functions are callable (compile-time check)
        let kv = to_kv_proto(b"k", b"v", None);
        assert_eq!(kv.key, b"k");

        let err = map_err("ok");
        assert_eq!(err.code(), tonic::Code::Internal);
    }

    // ──── 磁盘只读闸 ────

    #[test]
    fn test_disk_read_only_gate_rejects_writes() {
        use coord_core::storage::StorageBackend;
        let tmpdir = tempfile::TempDir::new().unwrap();
        let config = coord_core::types::StorageConfig::default();
        let backend = RedbBackend::open(tmpdir.path(), &config).unwrap();
        let storage = Arc::new(MvccStorage::new(backend).unwrap());
        let node = CoordNode::new(Arc::clone(&storage));

        // 默认可写
        assert!(node.ensure_writable().is_ok());

        // 磁盘水位 < 5%：置只读闸 → RESOURCE_EXHAUSTED
        node.set_disk_read_only(true);
        let err = node.ensure_writable().expect_err("must be read-only");
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);

        // 恢复后放行
        node.set_disk_read_only(false);
        assert!(node.ensure_writable().is_ok());
    }

    #[test]
    fn test_watch_buffer_minimum_clamp() {
        use coord_core::storage::StorageBackend;
        let tmpdir = tempfile::TempDir::new().unwrap();
        let config = coord_core::types::StorageConfig::default();
        let backend = RedbBackend::open(tmpdir.path(), &config).unwrap();
        let storage = Arc::new(MvccStorage::new(backend).unwrap());
        let mut node = CoordNode::new(Arc::clone(&storage));

        assert_eq!(
            node.watch_buffer.load(std::sync::atomic::Ordering::Relaxed),
            1024
        );
        node.set_watch_buffer(8);
        assert_eq!(
            node.watch_buffer.load(std::sync::atomic::Ordering::Relaxed),
            16,
            "clamped to minimum 16"
        );
        node.set_watch_buffer(4096);
        assert_eq!(
            node.watch_buffer.load(std::sync::atomic::Ordering::Relaxed),
            4096
        );
    }

    // ──── R-SVC-18：幂等缓存（TTL / 容量 / 身份维度）───

    fn cache_key(identity: u64, request_id: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(8 + request_id.len());
        key.extend_from_slice(&identity.to_be_bytes());
        key.extend_from_slice(request_id);
        key
    }

    #[test]
    fn test_runtime_limits_defaults() {
        let limits = RuntimeLimits::default();
        assert_eq!(limits.read_timeout, std::time::Duration::from_secs(5));
        assert_eq!(limits.write_timeout, std::time::Duration::from_secs(5));
        assert_eq!(limits.max_range_limit, 10_000);
        assert_eq!(limits.max_txn_ops, 128);
        assert_eq!(limits.idempotency_ttl, std::time::Duration::from_secs(60));
        assert_eq!(limits.idempotency_max_entries, 4096);
    }

    #[test]
    fn test_idempotency_cache_hit_and_ttl_expiry() {
        let mut cache = IdempotencyCache::new();
        let key = cache_key(7, b"req-1");
        cache.insert(
            key.clone(),
            IdempotentEntry {
                revision: 42,
                ..IdempotentEntry::with_revision(0)
            },
            16,
        );
        // 命中
        assert_eq!(
            cache
                .check(&key, std::time::Duration::from_secs(60))
                .unwrap()
                .revision,
            42
        );
        // 回填插入时间（10s 前）→ 1s TTL 判定过期 → None 且条目被清除
        let backdated = cache.entries.get_mut(&key).unwrap();
        backdated.inserted_at = std::time::Instant::now() - std::time::Duration::from_secs(10);
        assert!(cache
            .check(&key, std::time::Duration::from_secs(1))
            .is_none());
        assert_eq!(cache.entries.len(), 0);
    }

    #[test]
    fn test_idempotency_cache_fifo_eviction() {
        let mut cache = IdempotencyCache::new();
        for i in 0..8 {
            let key = cache_key(1, format!("req-{i}").as_bytes());
            cache.insert(
                key,
                IdempotentEntry {
                    revision: i as i64,
                    ..IdempotentEntry::with_revision(0)
                },
                4,
            );
        }
        assert_eq!(
            cache.entries.len(),
            4,
            "capacity enforced via FIFO eviction"
        );
        // 最早插入的 req-0..3 被淘汰
        for i in 0..4 {
            assert!(cache
                .check(
                    &cache_key(1, format!("req-{i}").as_bytes()),
                    std::time::Duration::from_secs(60)
                )
                .is_none());
        }
        // 最近插入的 req-4..7 仍命中
        for i in 4..8 {
            assert_eq!(
                cache
                    .check(
                        &cache_key(1, format!("req-{i}").as_bytes()),
                        std::time::Duration::from_secs(60)
                    )
                    .unwrap()
                    .revision,
                i as i64
            );
        }
    }

    #[test]
    fn test_idempotency_key_distinguishes_clients() {
        // 相同 request_id、不同 authorization → 不同缓存键
        let mut md1 = tonic::metadata::MetadataMap::new();
        md1.insert(
            "authorization",
            tonic::metadata::MetadataValue::from_static("Bearer token-a"),
        );
        let mut md2 = tonic::metadata::MetadataMap::new();
        md2.insert(
            "authorization",
            tonic::metadata::MetadataValue::from_static("Bearer token-b"),
        );
        let k1 = idempotency_key(&md1, b"same-request");
        let k2 = idempotency_key(&md2, b"same-request");
        assert_ne!(k1, k2);
        // 无凭据 → 退化为仅 request_id 维度（确定性）
        let md_empty = tonic::metadata::MetadataMap::new();
        let k3 = idempotency_key(&md_empty, b"same-request");
        assert!(k3.ends_with(b"same-request"));
    }

    #[test]
    fn test_idempotency_txn_cache_replays_responses() {
        let mut cache = IdempotencyCache::new();
        let key = cache_key(9, b"txn-1");
        let responses = vec![ResponseOp {
            op: Some(coord_proto::txn::response_op::Op::ResponsePut(
                PutResponse {
                    prev_kv: None,
                    revision: 5,
                },
            )),
        }];
        cache.insert(
            key.clone(),
            IdempotentEntry {
                revision: 5,
                responses: responses.clone(),
                ..IdempotentEntry::with_revision(0)
            },
            16,
        );
        let hit = cache
            .check(&key, std::time::Duration::from_secs(60))
            .unwrap();
        assert!(hit.succeeded);
        assert_eq!(
            hit.responses.len(),
            1,
            "Txn 命中回放完整 responses（R-SVC-18）"
        );
    }

    /// F-02（jepsen T1.4）：Put 命中必须回放**首次执行**的 `prev_kv`。
    /// 修复前命中路径硬编码 `prev_kv: None`，依赖 prev_kv 做 CAS 后置校验 /
    /// 审计的上层会把 `None` 读成「该 key 此前不存在」——静默的错误结论。
    #[test]
    fn test_idempotency_put_replays_prev_kv() {
        let mut cache = IdempotencyCache::new();
        let key = cache_key(3, b"put-1");
        let old = KeyValue {
            key: b"/k".to_vec(),
            value: b"OLD".to_vec(),
            create_revision: 7,
            mod_revision: 7,
            version: 1,
            lease_id: 0,
        };
        cache.insert(
            key.clone(),
            IdempotentEntry {
                prev_kv: Some(old.clone()),
                ..IdempotentEntry::with_revision(9)
            },
            16,
        );
        let hit = cache
            .check(&key, std::time::Duration::from_secs(60))
            .unwrap();
        assert_eq!(hit.revision, 9);
        assert_eq!(
            hit.prev_kv.as_ref().map(|kv| kv.value.clone()),
            Some(b"OLD".to_vec()),
            "F-02：命中必须回放首次响应的 prev_kv（而不是 None）"
        );
    }

    /// F-01（jepsen T1.4）：Delete 命中必须回放**首次执行**的 `deleted` 与
    /// `prev_kvs`。范围删尤其重要：重放若重新执行，会把首次执行之后新写入的
    /// Key 再删一遍（重试放大破坏面）。
    #[test]
    fn test_idempotency_delete_replays_deleted_and_prev_kvs() {
        let mut cache = IdempotencyCache::new();
        let key = cache_key(4, b"del-1");
        let kv = KeyValue {
            key: b"/k".to_vec(),
            value: b"V".to_vec(),
            create_revision: 1,
            mod_revision: 2,
            version: 1,
            lease_id: 0,
        };
        cache.insert(
            key.clone(),
            IdempotentEntry {
                deleted: 1,
                prev_kvs: vec![kv.clone()],
                ..IdempotentEntry::with_revision(11)
            },
            16,
        );
        let hit = cache
            .check(&key, std::time::Duration::from_secs(60))
            .unwrap();
        assert_eq!(hit.deleted, 1, "F-01：命中回放首次执行的 deleted");
        assert_eq!(hit.prev_kvs.len(), 1, "F-01：命中回放首次执行的 prev_kvs");
        assert_eq!(hit.prev_kvs[0].value, b"V".to_vec());
    }
}
