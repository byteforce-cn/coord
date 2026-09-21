//! gRPC 方法 ↔ 能力 ID 的**单一事实来源**，以及鉴权层的 scope 访问提取。
//!
//! 背景（第四轮复核 §3.4）：`coord-server` 与 `coord-agent` **各自维护一份**
//! RPC → capability 映射表，两份表已经漂移——
//!
//! - **拼写错误**：agent 表写 `/coord.kv.Kv/*`，而 proto 的真实路径是
//!   `/coord.kv.KV/*`（`package coord.kv;` 与 `service KV`，服务名全大写）。
//!   agent 用真实 URI path 查表 → 查不到 → 落到 `_ => None`（未知 RPC，
//!   fail-closed 拒绝）→ **开启 agent 鉴权时所有 KV 调用被拒**，且拒绝原因是
//!   会误导人的 "unknown RPC method"。
//! - **覆盖缺失**：agent 表只登记了 9 个服务，`Registry` / `Config` / `Lock` /
//!   `LeaderElection` / `IdGen` / `Event` / `Cache` / `MQ` / `Workflow` /
//!   `Scheduler` / `Policy` 等**全部缺失** → 目标场景 ①② 的服务调用全部被拒。
//!
//! 修法与第三轮 P0-1（[`crate::kv_range::RangeSemantics`]）同构：把「这个方法需要
//! 什么能力」收敛为**一个纯函数**，两侧共同调用。只要两边都走 [`rpc_capability`]，
//! 表内容就不可能再分叉。
//!
//! 本模块同时承载 scope 提取（[`extract_scope_access`]），因为它是鉴权层与
//! 能力表**成对使用**的：`needs_scope_extraction` 决定哪些方法需要读 body。
//! 语义判定（`(key, range_end)` 到底是点查还是区间）不在本模块定义，而由
//! [`crate::kv_range::RangeSemantics::of`] 单点给出。

use crate::auth::trie::ScopeTrie;
use crate::kv_range::RangeSemantics;
use prost::Message;

/// gRPC 方法 → 所需 capability ID 的**唯一**映射表。
///
/// 返回值语义：
/// - `Some(cap)` —— 该 RPC 需要 `cap`（再由角色授权决定是否放行）；
/// - `None` —— 该 RPC **自身无能力要求**（登录/健康/凭据自证等白名单端点），
///   **或**是未知方法。调用方必须显式区分这两种情况（服务端查 `is_whitelisted`，
///   agent 查 `CapabilityTable` 的 allowlist），不得把 `None` 直接当作放行。
///
/// # 维护约定
///
/// 新增 RPC 时必须在此登记。两侧共用此表，**不得**在 crate 内再复制一份
/// （复制出来的第二份就是下一个语义裂缝）。
pub fn rpc_capability(rpc_method: &str) -> Option<&'static str> {
    match rpc_method {
        // ── KV（`package coord.kv; service KV` → 路径段是 **KV**，不是 Kv）──
        "/coord.kv.KV/Range" => Some("data:kv:read"),
        "/coord.kv.KV/Put" => Some("data:kv:write"),
        "/coord.kv.KV/Delete" => Some("data:kv:delete"),

        // ── Txn ──
        "/coord.txn.Txn/Txn" => Some("data:txn:execute"),

        // ── Lease ──
        "/coord.lease.Lease/LeaseGrant" => Some("data:lease:grant"),
        "/coord.lease.Lease/LeaseRevoke" => Some("data:lease:revoke"),
        "/coord.lease.Lease/LeaseKeepAlive" => Some("data:lease:keepalive"),

        // ── Watch ──
        "/coord.watch.Watch/Watch" => Some("data:watch:subscribe"),

        // ── 对象存储（coord.storage，EXPERIMENTAL 数据面）──
        "/coord.storage.Storage/Get" => Some("data:storage:read"),
        "/coord.storage.Storage/Stat" => Some("data:storage:read"),
        "/coord.storage.Storage/Put" => Some("data:storage:write"),
        "/coord.storage.Storage/Delete" => Some("data:storage:write"),

        // ── Maintenance（集群管理）──
        "/coord.maintenance.Maintenance/Status" => Some("admin:maintenance:status"),
        "/coord.maintenance.Maintenance/Seal" => Some("admin:maintenance:seal"),
        "/coord.maintenance.Maintenance/Unseal" => Some("admin:maintenance:unseal"),
        "/coord.maintenance.Maintenance/Snapshot" => Some("admin:maintenance:snapshot"),
        "/coord.maintenance.Maintenance/Compact" => Some("admin:maintenance:compact"),
        "/coord.maintenance.Maintenance/MemberAdd" => Some("admin:maintenance:member_add"),
        "/coord.maintenance.Maintenance/MemberRemove" => Some("admin:maintenance:member_remove"),
        "/coord.maintenance.Maintenance/MemberPromote" => Some("admin:maintenance:member_promote"),
        "/coord.maintenance.Maintenance/MemberList" => Some("admin:maintenance:member_list"),
        // 第四轮 §3.1 附带发现：`Join` 此前不在映射表内 → **加集群恒 403**
        // （与 auth 开关无关）。这里补上，与 MemberAdd 同权限点。
        "/coord.maintenance.Maintenance/Join" => Some("admin:maintenance:member_add"),

        // ── Auth 管理 ──
        "/coord.auth.Auth/AuthEnable" => Some("admin:auth:enable"),
        "/coord.auth.Auth/AuthDisable" => Some("admin:auth:disable"),
        "/coord.auth.Auth/AuthStatus" => Some("admin:auth:status"),
        "/coord.auth.Auth/UserAdd" => Some("admin:auth:user_add"),
        "/coord.auth.Auth/UserDelete" => Some("admin:auth:user_delete"),
        "/coord.auth.Auth/UserChangePassword" => Some("admin:auth:user_add"),
        "/coord.auth.Auth/UserList" => Some("admin:auth:user_list"),
        "/coord.auth.Auth/UserGet" => Some("admin:auth:user_list"),
        "/coord.auth.Auth/RoleAdd" => Some("admin:auth:role_add"),
        "/coord.auth.Auth/RoleDelete" => Some("admin:auth:role_delete"),
        "/coord.auth.Auth/RoleGrantPermission" => Some("admin:auth:role_grant"),
        "/coord.auth.Auth/RoleRevokePermission" => Some("admin:auth:role_revoke"),
        "/coord.auth.Auth/RoleGrantCapability" => Some("admin:auth:role_grant"),
        "/coord.auth.Auth/RoleRevokeCapability" => Some("admin:auth:role_revoke"),
        "/coord.auth.Auth/RoleList" => Some("admin:auth:role_list"),
        "/coord.auth.Auth/ListRoles" => Some("admin:auth:role_list"),
        "/coord.auth.Auth/UserGrantRole" => Some("admin:auth:user_grant_role"),
        "/coord.auth.Auth/UserRevokeRole" => Some("admin:auth:user_revoke_role"),
        "/coord.auth.Auth/BootstrapTokenIssue" => Some("admin:auth:bootstrap_token"),
        "/coord.auth.Auth/BootstrapTokenList" => Some("admin:auth:bootstrap_token"),
        "/coord.auth.Auth/BootstrapTokenRevoke" => Some("admin:auth:bootstrap_token"),
        // 认证前置端点：自身无能力要求（服务内自证/一次性令牌消费）。
        "/coord.auth.Auth/Authenticate" => None,
        "/coord.auth.Auth/RefreshToken" => None,
        "/coord.auth.Auth/Bootstrap" => None,
        // agent 角色同步端点：仅需合法 CCT，由服务端特判（见 ServerAuthService）。
        "/coord.auth.Auth/GetRevocationDelta" => None,

        // ── Capability 注册表 ──
        "/coord.capability.CapabilityRegistry/List" => Some("admin:capability:list"),
        "/coord.capability.CapabilityRegistry/Get" => Some("admin:capability:list"),
        "/coord.capability.CapabilityRegistry/Register" => Some("admin:capability:register"),
        "/coord.capability.CapabilityRegistry/Deprecate" => Some("admin:capability:deprecate"),

        // ── 插件调用面（coord.plugin.Plugin；agent 本地服务）──
        "/coord.plugin.Plugin/Invoke" => Some("coord:plugin:invoke"),
        "/coord.plugin.Plugin/List" => Some("coord:plugin:list"),

        // ══════════════════════════════════════════════════════════════════
        // coord.agent.* —— agent 侧服务面（核心代理 + 原生「真插件」服务）。
        // 第四轮 §3.4：这些此前**全部未登记** → 开启 agent 鉴权时被 `_ => None`
        // 拒绝，即目标场景 ①②③ 的服务调用全部不可用。
        // ══════════════════════════════════════════════════════════════════
        // Handshake（声明未实现，仍登记以免落到"未知 RPC"）
        "/coord.agent.Handshake/Negotiate" => Some("coord:handshake:negotiate"),
        // Health（Java SDK healthCheck 调用此自定义服务）
        "/coord.agent.Health/Check" => Some("coord:health:check"),

        // Registry —— 服务注册发现（场景 ①）
        "/coord.registry.v1.Registry/Register" => Some("coord:registry:register"),
        "/coord.registry.v1.Registry/Deregister" => Some("coord:registry:deregister"),
        "/coord.registry.v1.Registry/Heartbeat" => Some("coord:registry:heartbeat"),
        "/coord.registry.v1.Registry/Discover" => Some("coord:registry:discover"),
        "/coord.registry.v1.Registry/Watch" => Some("coord:registry:watch"),

        // Config —— 配置中心（场景 ①）
        "/coord.config.v1.Config/Get" => Some("coord:config:read"),
        "/coord.config.v1.Config/Put" => Some("coord:config:write"),
        "/coord.config.v1.Config/List" => Some("coord:config:list"),
        "/coord.config.v1.Config/Watch" => Some("coord:config:watch"),

        // Lock —— 分布式锁（场景 ②）
        "/coord.lock.v1.Lock/Acquire" => Some("coord:lock:acquire"),
        "/coord.lock.v1.Lock/Release" => Some("coord:lock:release"),
        "/coord.lock.v1.Lock/Renew" => Some("coord:lock:renew"),
        "/coord.lock.v1.Lock/GetLockInfo" => Some("coord:lock:info"),

        // IdGen —— 分布式 ID
        "/coord.idgen.v1.IdGen/NextId" => Some("coord:idgen:next"),
        "/coord.idgen.v1.IdGen/NextBatch" => Some("coord:idgen:next"),

        // LeaderElection —— 选举（场景 ②）
        "/coord.election.v1.LeaderElection/Campaign" => Some("coord:election:campaign"),
        "/coord.election.v1.LeaderElection/Resign" => Some("coord:election:resign"),
        "/coord.election.v1.LeaderElection/GetLeader" => Some("coord:election:read"),
        "/coord.election.v1.LeaderElection/Watch" => Some("coord:election:watch"),

        // Event —— 事件通知
        "/coord.event.v1.Event/Publish" => Some("coord:event:publish"),
        "/coord.event.v1.Event/Subscribe" => Some("coord:event:subscribe"),
        "/coord.event.v1.Event/Unsubscribe" => Some("coord:event:unsubscribe"),

        // Cache —— 缓存（EXPERIMENTAL）
        "/coord.cache.v1.Cache/Get" => Some("coord:cache:read"),
        "/coord.cache.v1.Cache/HGet" => Some("coord:cache:read"),
        "/coord.cache.v1.Cache/HGetAll" => Some("coord:cache:read"),
        "/coord.cache.v1.Cache/LRange" => Some("coord:cache:read"),
        "/coord.cache.v1.Cache/LLen" => Some("coord:cache:read"),
        "/coord.cache.v1.Cache/SMembers" => Some("coord:cache:read"),
        "/coord.cache.v1.Cache/Set" => Some("coord:cache:write"),
        "/coord.cache.v1.Cache/HSet" => Some("coord:cache:write"),
        "/coord.cache.v1.Cache/LPush" => Some("coord:cache:write"),
        "/coord.cache.v1.Cache/RPop" => Some("coord:cache:write"),
        "/coord.cache.v1.Cache/SAdd" => Some("coord:cache:write"),
        "/coord.cache.v1.Cache/Delete" => Some("coord:cache:write"),

        // MQ —— 消息队列（EXPERIMENTAL）
        "/coord.mq.v1.MQ/CreateTopic" => Some("coord:mq:manage"),
        "/coord.mq.v1.MQ/PollDlq" => Some("coord:mq:manage"),
        "/coord.mq.v1.MQ/Publish" => Some("coord:mq:publish"),
        "/coord.mq.v1.MQ/Subscribe" => Some("coord:mq:subscribe"),
        "/coord.mq.v1.MQ/Ack" => Some("coord:mq:consume"),
        "/coord.mq.v1.MQ/Poll" => Some("coord:mq:consume"),

        // Replica —— 副本（内部面，但经 agent 暴露）
        "/coord.agent.Replica/Apply" => Some("coord:replica:write"),
        "/coord.agent.Replica/IsrHeartbeat" => Some("coord:replica:write"),
        "/coord.agent.Replica/Reconcile" => Some("coord:replica:read"),

        // Scheduler —— 调度（EXPERIMENTAL）
        "/coord.scheduler.v1.Scheduler/RegisterJob" => Some("coord:scheduler:manage"),
        "/coord.scheduler.v1.Scheduler/ClaimJob" => Some("coord:scheduler:execute"),
        "/coord.scheduler.v1.Scheduler/Heartbeat" => Some("coord:scheduler:execute"),
        "/coord.scheduler.v1.Scheduler/CompleteJob" => Some("coord:scheduler:execute"),

        // Workflow —— 工作流（EXPERIMENTAL）
        "/coord.workflow.v1.Workflow/Start" => Some("coord:workflow:execute"),
        "/coord.workflow.v1.Workflow/Signal" => Some("coord:workflow:execute"),
        "/coord.workflow.v1.Workflow/Cancel" => Some("coord:workflow:execute"),
        "/coord.workflow.v1.Workflow/GetStatus" => Some("coord:workflow:read"),
        "/coord.workflow.v1.Workflow/ListDefinitions" => Some("coord:workflow:read"),
        "/coord.workflow.v1.Workflow/GetDefinition" => Some("coord:workflow:read"),
        "/coord.workflow.v1.Workflow/ListDefinitionVersions" => Some("coord:workflow:read"),
        "/coord.workflow.v1.Workflow/ListInstances" => Some("coord:workflow:read"),
        "/coord.workflow.v1.Workflow/Deploy" => Some("coord:workflow:define"),
        "/coord.workflow.v1.Workflow/RollbackDefinition" => Some("coord:workflow:define"),

        // Policy —— 策略/OPA
        "/coord.policy.v1.Policy/CheckPermission" => Some("coord:policy:evaluate"),
        "/coord.policy.v1.Policy/Evaluate" => Some("coord:policy:evaluate"),
        "/coord.policy.v1.Policy/Explain" => Some("coord:policy:evaluate"),
        "/coord.policy.v1.Policy/PutBundle" => Some("coord:policy:manage"),
        "/coord.policy.v1.Policy/DeleteBundle" => Some("coord:policy:manage"),
        "/coord.policy.v1.Policy/SetBundleEnabled" => Some("coord:policy:manage"),
        "/coord.policy.v1.Policy/RollbackBundle" => Some("coord:policy:manage"),
        "/coord.policy.v1.Policy/ListBundles" => Some("coord:policy:manage"),
        "/coord.policy.v1.Policy/ListBundleVersions" => Some("coord:policy:manage"),

        // Transit —— 加密/签名
        "/coord.transit.v1.Transit/Encrypt" => Some("coord:transit:crypto"),
        "/coord.transit.v1.Transit/Decrypt" => Some("coord:transit:crypto"),
        "/coord.transit.v1.Transit/HmacSign" => Some("coord:transit:crypto"),
        "/coord.transit.v1.Transit/HmacVerify" => Some("coord:transit:crypto"),

        // CircuitBreaker —— 熔断
        "/coord.circuitbreaker.v1.CircuitBreaker/GetState" => Some("coord:breaker:read"),
        "/coord.circuitbreaker.v1.CircuitBreaker/ReportSuccess" => Some("coord:breaker:report"),
        "/coord.circuitbreaker.v1.CircuitBreaker/ReportFailure" => Some("coord:breaker:report"),
        "/coord.circuitbreaker.v1.CircuitBreaker/Reset" => Some("coord:breaker:manage"),

        // RateLimiter / FeatureFlags
        "/coord.ratelimiter.v1.RateLimiter/Allow" => Some("coord:ratelimit:check"),
        "/coord.featureflags.v1.FeatureFlags/IsEnabled" => Some("coord:flags:read"),
        "/coord.featureflags.v1.FeatureFlags/Evaluate" => Some("coord:flags:read"),

        // PKI：私钥集中存储前必须上鉴权
        "/coord.pki.v1.Pki/InitCa" => Some("pki:ca:init"),
        "/coord.pki.v1.Pki/IssueCert" => Some("pki:cert:issue"),
        "/coord.pki.v1.Pki/RenewCert" => Some("pki:cert:issue"),
        "/coord.pki.v1.Pki/RotateCert" => Some("pki:cert:rotate"),
        "/coord.pki.v1.Pki/ListCerts" => Some("pki:cert:read"),
        "/coord.pki.v1.Pki/GetCertByCN" => Some("pki:cert:read"),
        "/coord.pki.v1.Pki/GetCaCert" => Some("pki:cert:read"),
        "/coord.pki.v1.Pki/VerifyCert" => Some("pki:cert:read"),

        // 未知 RPC —— 调用方必须按 fail-closed 处理（拒绝）。
        _ => None,
    }
}

/// 需要从**请求 body** 提取 scope key 的 RPC 方法集合。
///
/// 这些方法的资源键在 protobuf 消息里而非 header 中，因此鉴权层必须先缓存
/// body（受 [`MAX_SCOPE_BODY_BYTES`] 硬上限保护）再解析。
///
/// # ⚠️ 只允许**一元（unary）** RPC
///
/// 本集合的语义是"可以安全地把整个 body 先读完再转发"。对**流式** RPC 这个前提
/// 不成立：流的 body 在客户端 half-close 之前永远不会结束，缓存整个 body 等于把
/// 请求永久挡在 handler 之外。
///
/// 第四轮曾把 `/coord.watch.Watch/Watch` 加进来（§3.8：让 Watch 同时"可用"且
/// "受 scope 约束"），结果：**watch 彻底不通** ——
/// `WatchServer::watch` 是 `stream WatchRequest -> stream WatchResponse`，
/// 鉴权层 `buffer_request_body` 等不到流结束，请求永不转发；客户端既不收到事件也
/// 不会报错（Rust 侧 `message().await` 永久挂起，Java 侧 5s 超时失败）。
/// 而且 agent 的鉴权层**无论 auth 开不开都挂载**，所以这个缺陷与鉴权开关无关。
/// 已有回归卡口：`streaming_rpcs_must_not_be_body_buffered`（本文件）与
/// `coord/tests/agent_watch_test.rs`。
///
/// **Watch 的 scope 约束不在这里做**：Watch 的资源键在**首条** `WatchCreateRequest`
/// 里，只能在 handler 拿到已解码的首帧后判定（见
/// [`extract_scope_access`] 对 Watch 分支的处理，供 handler 侧复用）。
pub fn needs_scope_extraction(rpc_method: &str) -> bool {
    matches!(
        rpc_method,
        "/coord.kv.KV/Put" | "/coord.kv.KV/Range" | "/coord.kv.KV/Delete" | "/coord.txn.Txn/Txn"
    )
}

/// 该 RPC 是否为**流式**（其 body 不可整体缓存，因而不得进入
/// [`needs_scope_extraction`]）。
///
/// 存在的意义是把"一元/流式"这一隐含前提变成**可断言**的事实：如果哪天有人把
/// 一个流式方法加回缓存集合，测试 `streaming_rpcs_must_not_be_body_buffered`
/// 会直接红。
///
/// **⚠️ 本清单曾整体失效过（2026-09-19 修）**：名单里写的是
/// `/coord.registry.Registry/Watch`、`/coord.mq.MQ/Subscribe`、
/// `/coord.object_storage.ObjectStorage/*` 这类**迁移前/从未存在过**的路径，
/// 而真实 RPC 全名早已是 `coord.<domain>.v1.*`（迁移前则是 `coord.agent.*`）。
/// 于是这份"机器守卫"实际上**从不匹配任何真实请求**，却一直显示为绿。
///
/// 现在它由 `streaming_set_matches_the_proto_descriptors` 测试**与 descriptor
/// 逐条对齐**：该测试从 `coord_proto::FILE_DESCRIPTOR_SET` 反解出所有
/// `stream` 方法，断言本函数与那份集合**完全相等**（双向）。也就是说，
/// 手写清单仍然保留（鉴权热路径要的是廉价纯函数，不要反射），
/// 但它的**正确性不再依赖人记得改**。
pub fn is_streaming_rpc(rpc_method: &str) -> bool {
    matches!(
        rpc_method,
        // 服务端流
        "/coord.watch.Watch/Watch"
            | "/coord.registry.v1.Registry/Watch"
            | "/coord.config.v1.Config/Watch"
            | "/coord.election.v1.LeaderElection/Watch"
            | "/coord.event.v1.Event/Subscribe"
            | "/coord.mq.v1.MQ/Subscribe"
            | "/coord.storage.Storage/Put"
            | "/coord.storage.Storage/Get"
            | "/coord.maintenance.Maintenance/Snapshot"
            | "/coord.agent.Replica/Reconcile"
            // 双向流
            | "/coord.lease.Lease/LeaseKeepAlive"
            | "/coord.raft.Raft/InstallSnapshotStreaming"
    )
}

/// gRPC 消息解码上限（对齐 `tonic` 的 `max_decoding_message_size` 默认口径）。
///
/// RPC 服务的解码上限由服务端显式设置为该值（见 `coord/src/main.rs`），鉴权层的
/// body 上限**必须与之对齐**，否则会出现"合法请求在鉴权层被拒、而它本可以通过
/// 解码"的回归。
pub const MAX_GRPC_DECODING_BYTES: usize = 4 * 1024 * 1024; // 4 MiB

/// scope 提取前请求体上限：解码上限 + 64 KiB 余量（gRPC 帧头 + protobuf 字段头）。
///
/// 该路径在**鉴权前**缓存 body（auth 关闭时同样执行），若不加限则无凭据请求即可
/// 触发无界 `collect()`。
pub const MAX_SCOPE_BODY_BYTES: usize = MAX_GRPC_DECODING_BYTES + 64 * 1024;

/// 一次请求触碰的 key 区间。
///
/// 语义判定**不在此处定义**，而由 [`RangeSemantics::of`] 单点给出（与服务端实际
/// 执行路径共用），详见 [`ScopeAccess::from_range`]：
///
/// - 单键（`range_end` 为空 或 == `key`）→ 点访问（`range_end` 留空）；
/// - `range_end == "\0"` → 从 `key` 到无穷（etcd 语义）；
/// - 否则 → 区间 `[key, range_end)`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeAccess {
    /// 起始 key（含）。
    pub key: Vec<u8>,
    /// 区间上界（不含）。空 = 单键访问。
    pub range_end: Vec<u8>,
}

impl ScopeAccess {
    /// 单 key 访问。
    pub fn point(key: Vec<u8>) -> Self {
        Self {
            key,
            range_end: Vec::new(),
        }
    }

    /// 区间访问。
    pub fn range(key: Vec<u8>, range_end: Vec<u8>) -> Self {
        Self { key, range_end }
    }

    /// 由请求的 `(key, range_end)` 构造 —— 语义判定必须与服务端**实际执行**
    /// 共用 [`RangeSemantics::of`]（第三轮 P0-1）。
    pub fn from_range(key: Vec<u8>, range_end: Vec<u8>) -> Self {
        match RangeSemantics::of(&key, &range_end) {
            RangeSemantics::SingleKey => Self::point(key),
            RangeSemantics::Interval => Self { key, range_end },
        }
    }
}

/// Watch 首帧（**已解码**的 `WatchCreateRequest`）→ 该订阅触碰的 scope 区间。
///
/// # 为什么这个函数必须存在
///
/// Watch 是**客户端流式** RPC：订阅的前缀/区间在**首帧** `WatchCreateRequest` 里，
/// 而鉴权层拿不到它 —— 要拿到就得把整个流 body 缓存下来，而流的 body 在客户端
/// half-close 前永不结束，缓存 = 请求永久挂起（第四轮的 P0 事故，见
/// [`is_streaming_rpc`]）。所以 Watch 的 scope 判定只能**在 handler 里、拿到首帧之后**做。
///
/// 本函数是该判定的输入侧，与 [`extract_scope_access`] 的 Watch 分支**同一实现**
/// （不是拷贝）：Watch 的 `range_end` 为空 = **字节前缀订阅**（与 KV 的"单键"语义
/// 不同），区间由 [`crate::kv_range::watch_match_interval`] 给出 —— 与投递侧
/// `coord-server/src/watch/mod.rs::key_matches` 是同一个函数，故不可能漂移。
///
/// `hi = None`（空前缀 / 全 0xFF）表示无有限上界：用 etcd 的无界上界符号表示，
/// 有界 scope 一律拒绝（fail-closed）。
pub fn watch_create_access(key: &[u8], range_end: &[u8]) -> ScopeAccess {
    let (lo, hi) = crate::kv_range::watch_match_interval(key, range_end);
    let upper = hi.unwrap_or_else(|| crate::kv_range::UNBOUNDED_RANGE_END.to_vec());
    ScopeAccess::range(lo, upper)
}

/// scope 判定（fail-closed）：本次访问的**全部**区间都必须被授权覆盖。
///
/// - `accesses` 为空（未能提取资源键）→ 只有**无约束**授权（存在空 scope）放行，
///   存在非空 scope 限制时拒绝；
/// - 否则逐条判定：单键走 [`ScopeTrie::matches`]；区间走
///   [`crate::auth::trie::scope_covers_interval`]（要求**整体包含**，与服务端 A1
///   的区间语义一致）。
///
/// # 单一实现
///
/// 鉴权层（一元 RPC：先缓存 body、再提取）与流式 handler（Watch：解码首帧后提取）
/// **必须**用同一个判定；两份实现就等于"同一条 scope 在两条路径上语义不同"。
/// 因此本函数是本仓库**唯一**的 scope 覆盖判定实现，`coord_agent::auth::interceptor`
/// 只做委托，不得再写一份。
pub fn scope_allows(grant_scopes: &[String], accesses: &[ScopeAccess]) -> bool {
    if accesses.is_empty() {
        return grant_scopes.iter().any(|s| s.is_empty());
    }
    accesses.iter().all(|access| {
        grant_scopes.iter().any(|scope| {
            if scope.is_empty() {
                return true; // 无约束授权覆盖一切
            }
            if access.range_end.is_empty() {
                match std::str::from_utf8(&access.key) {
                    Ok(key) => {
                        let mut trie = ScopeTrie::new();
                        trie.insert(scope).is_ok() && trie.matches(key)
                    }
                    // 非 UTF-8 key 无法与字符串 scope 比对 → 拒绝（fail-closed）
                    Err(_) => false,
                }
            } else {
                crate::auth::trie::scope_covers_interval(scope, &access.key, &access.range_end)
            }
        })
    })
}

/// 该 RPC 的 scope 判定是否**被延后到 handler**（客户端流式 RPC）。
///
/// 判据是结构性的，不是偏好：这类 RPC 的资源键在**流 body** 里，鉴权层看不到、
/// 又不能缓存（缓存 ⇒ 永久挂起）。所以判定必须由 handler 解码首帧后执行，
/// 鉴权层则把授权快照（[`DeferredScopeGrants`]）放进请求扩展交给它。
///
/// # 与 [`needs_scope_extraction`] 的关系
///
/// 两者**互斥且必须覆盖每一个 scope 承载的 RPC**：
/// - `needs_scope_extraction(x)` ⇒ 鉴权层缓存 body 后判定（`body_scope_extractor`）；
/// - `is_deferred_scope_rpc(x)` ⇒ handler 判定（本仓库目前只有 Watch）；
/// - 两者皆 false 而该 RPC 在 [`rpc_capability`] 里**有**能力 ⇒ **无人判定 scope**
///   （fail-open 的形态之一）。
///
/// 卡口：`coord-agent` 侧 `deferred_scope_rpcs_are_client_streaming_and_scope_bearing`
/// 钉住「延后集合 ⊆ 流式集合 ∩ 有能力的集合」「延后集合 ∩ 缓存集合 = ∅」；
/// `coord-core` 侧 `deferred_and_buffered_scope_sets_are_disjoint` 钉住互斥。
pub fn is_deferred_scope_rpc(rpc_method: &str) -> bool {
    matches!(rpc_method, "/coord.watch.Watch/Watch")
}

/// 鉴权层交给**流式 handler** 的授权快照（见 [`is_deferred_scope_rpc`]）。
///
/// 语义：`grant_scopes` 是调用方在该能力上的授权 scope 列表（`""` = 无约束）。
/// 鉴权层已完成认证与能力判定，只剩 scope 判定；它把授权列表交给 handler，
/// 由 handler 用首帧解码出的访问区间调 [`scope_allows`]。
///
/// # 缺省语义（fail-closed 的落点写在注释里，不写在文档里是不够的）
///
/// 请求扩展里**没有**本类型 ⇒ handler 不做 scope 判定。该缺省只对应三种"本层不施加
/// scope 约束"的既有语义：① 鉴权关闭（`AuthInterceptor::enabled == false`，与
/// `validate_request_accesses` 的早退同口径）；② root（全能力旁路，与第 5b 步同口径）；
/// ③ 请求根本没经过鉴权层（单元测试直调 handler / 未挂载该 layer）。
///
/// 反过来：**有约束**的授权一定伴随本类型 —— 鉴权层在放行时插入它，且
/// `grant_scopes` 必非空（空授权在第 6 步已被拒）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeferredScopeGrants {
    /// 被判定为需要 scope 的能力 ID（[`rpc_capability`] 的输出）。
    pub capability_id: String,
    /// 授权 scope 列表；含空串 = 存在无约束授权。
    pub grant_scopes: Vec<String>,
}

/// 流式 handler 的 scope 判定入口（唯一实现，见 [`scope_allows`]）。
///
/// `grants = None` ⇒ 放行（无约束，语义见 [`DeferredScopeGrants`] 的缺省段落）。
/// 返回 `Err` 的字符串是拒绝原因，handler 必须把它变成
/// `PERMISSION_DENIED`（而不是 `UNAUTHENTICATED`：身份是有效的，缺的是权限）。
pub fn check_deferred_scope(
    grants: Option<&DeferredScopeGrants>,
    accesses: &[ScopeAccess],
) -> Result<(), String> {
    let Some(grants) = grants else {
        return Ok(());
    };
    if scope_allows(&grants.grant_scopes, accesses) {
        return Ok(());
    }
    Err(format!(
        "scope restriction: capability '{}' not granted for the requested resource(s); \
         grant scopes {:?}, accesses {:?} (deferred/fail-closed)",
        grants.capability_id, grants.grant_scopes, accesses
    ))
}

/// 从请求 body 提取 scope **访问区间**列表。
///
/// - Put：单 key；
/// - Range/Delete：`[key, range_end)`（`range_end` 为空 = 单 key）；
/// - Txn：全部 compare key 与 success/failure 操作触碰的 key/区间；
/// - Watch：全部 `create_request`（订阅的 key/区间）；
/// - 解析失败返回 `Err`（请求本身畸形，调用方按失败关闭拒绝）。
///
/// `body` 为 gRPC 帧流（5 字节前缀：1 字节压缩标志 + 4 字节大端长度），
/// 解析前先剥离帧头；无帧头的裸 protobuf（测试路径）直接按消息解析。
pub fn extract_scope_access(rpc_method: &str, body: &[u8]) -> Result<Vec<ScopeAccess>, String> {
    // 剥离 gRPC 帧头（压缩标志非 0 → 无法解析，按畸形请求拒绝）
    let payload = if body.len() >= 5 && body[0] == 0 {
        let msg_len = u32::from_be_bytes([body[1], body[2], body[3], body[4]]) as usize;
        if body.len() >= 5 + msg_len {
            &body[5..5 + msg_len]
        } else {
            return Err("truncated gRPC frame".to_string());
        }
    } else if body.len() >= 5 && body[0] == 1 {
        return Err("compressed request body is not supported for scope extraction".to_string());
    } else {
        body
    };

    match rpc_method {
        "/coord.kv.KV/Put" => {
            let req = coord_proto::kv::PutRequest::decode(payload)
                .map_err(|e| format!("failed to parse PutRequest body: {e}"))?;
            Ok(vec![ScopeAccess::point(req.key)])
        }
        "/coord.kv.KV/Range" => {
            let req = coord_proto::kv::RangeRequest::decode(payload)
                .map_err(|e| format!("failed to parse RangeRequest body: {e}"))?;
            Ok(vec![ScopeAccess::from_range(req.key, req.range_end)])
        }
        "/coord.kv.KV/Delete" => {
            let req = coord_proto::kv::DeleteRequest::decode(payload)
                .map_err(|e| format!("failed to parse DeleteRequest body: {e}"))?;
            Ok(vec![ScopeAccess::from_range(req.key, req.range_end)])
        }
        "/coord.txn.Txn/Txn" => {
            let txn = coord_proto::txn::TxnRequest::decode(payload)
                .map_err(|e| format!("failed to parse TxnRequest body: {e}"))?;
            let mut accesses: Vec<ScopeAccess> = txn
                .compare
                .iter()
                .map(|c| ScopeAccess::point(c.key.clone()))
                .collect();
            for op in txn.success.iter().chain(txn.failure.iter()) {
                use coord_proto::txn::request_op::Op;
                match &op.op {
                    Some(Op::RequestPut(p)) => accesses.push(ScopeAccess::point(p.key.clone())),
                    Some(Op::RequestDelete(d)) => {
                        accesses.push(ScopeAccess::from_range(d.key.clone(), d.range_end.clone()))
                    }
                    Some(Op::RequestRange(r)) => {
                        accesses.push(ScopeAccess::from_range(r.key.clone(), r.range_end.clone()))
                    }
                    None => {}
                }
            }
            Ok(accesses)
        }
        // 第四轮 §3.8：Watch 纳入 scope 提取。`Watch` 是**双向流**，body 可能是
        // 一个或多个 `WatchRequest` 帧；逐帧解析，取每个 `create_request` 的
        // `(key, range_end)`。`cancel_request` / `progress_request` 不触碰新 key。
        "/coord.watch.Watch/Watch" => {
            let mut accesses = Vec::new();
            // `body` 有两种形态：① 无帧头的裸 protobuf（测试路径 / 单消息）；
            // ② 一个或多个 gRPC 帧（双向流）。用"首字节 = 未压缩标志 + 长度前缀自洽"
            // 来区分——裸 `WatchRequest` 的首字节是字段 tag（0x0A），不会误判。
            let framed = body.len() >= 5
                && body[0] == 0
                && (u32::from_be_bytes([body[1], body[2], body[3], body[4]]) as usize)
                    <= body.len() - 5;
            let frames: Vec<&[u8]> = if framed {
                let mut out = Vec::new();
                let mut rest = body;
                while rest.len() >= 5 {
                    if rest[0] != 0 {
                        return Err(
                            "compressed request body is not supported for scope extraction"
                                .to_string(),
                        );
                    }
                    let msg_len = u32::from_be_bytes([rest[1], rest[2], rest[3], rest[4]]) as usize;
                    if rest.len() < 5 + msg_len {
                        return Err("truncated gRPC frame".to_string());
                    }
                    out.push(&rest[5..5 + msg_len]);
                    rest = &rest[5 + msg_len..];
                }
                out
            } else {
                vec![payload]
            };
            for frame in frames {
                let req = coord_proto::watch::WatchRequest::decode(frame)
                    .map_err(|e| format!("failed to parse WatchRequest body: {e}"))?;
                if let Some(coord_proto::watch::watch_request::Request::Create(create)) =
                    req.request
                {
                    // 第四轮 §3.8：Watch 的 `range_end` 为空 = **字节前缀订阅**
                    // （见 `coord-server/src/watch/mod.rs` 的 `key_matches`），
                    // 与 KV/Txn 的"单键"语义**不同**。鉴权层必须按它**实际执行的**
                    // 语义建模，否则 `key="/app"`（无尾斜杠）+ scope `/app/`
                    // 会放行 `/application/...` 的订阅。
                    //
                    // 区间由 [`watch_create_access`] 计算 —— 与投递侧 `key_matches`
                    // 是**同一个**函数，且与 handler 侧延迟判定**同一实现**，
                    // 故两条路径不可能漂移。
                    accesses.push(watch_create_access(&create.key, &create.range_end));
                }
            }
            Ok(accesses)
        }
        _ => Ok(Vec::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kv_service_path_uses_uppercase_kv() {
        // 真实路径是 `service KV`（proto：`package coord.kv; service KV`）。
        assert_eq!(rpc_capability("/coord.kv.KV/Range"), Some("data:kv:read"));
        assert_eq!(rpc_capability("/coord.kv.KV/Put"), Some("data:kv:write"));
        assert_eq!(
            rpc_capability("/coord.kv.KV/Delete"),
            Some("data:kv:delete")
        );
        // 旧拼写必须**不再**被接受（它正是"开启 agent 鉴权即拒绝全部 KV"的根因）。
        assert_eq!(rpc_capability("/coord.kv.Kv/Range"), None);
        assert_eq!(rpc_capability("/coord.kv.Kv/Put"), None);
    }

    #[test]
    fn agent_services_are_all_registered() {
        // 目标场景 ①② 的服务面（第四轮 §3.4：此前全部缺失 → 开启鉴权即被拒）。
        for (method, cap) in [
            (
                "/coord.registry.v1.Registry/Register",
                "coord:registry:register",
            ),
            (
                "/coord.registry.v1.Registry/Discover",
                "coord:registry:discover",
            ),
            ("/coord.config.v1.Config/Get", "coord:config:read"),
            ("/coord.config.v1.Config/Put", "coord:config:write"),
            ("/coord.lock.v1.Lock/Acquire", "coord:lock:acquire"),
            ("/coord.lock.v1.Lock/Release", "coord:lock:release"),
            (
                "/coord.election.v1.LeaderElection/Campaign",
                "coord:election:campaign",
            ),
            ("/coord.idgen.v1.IdGen/NextId", "coord:idgen:next"),
            ("/coord.event.v1.Event/Publish", "coord:event:publish"),
        ] {
            assert_eq!(rpc_capability(method), Some(cap), "missing entry: {method}");
        }
    }

    #[test]
    fn join_is_mapped() {
        assert_eq!(
            rpc_capability("/coord.maintenance.Maintenance/Join"),
            Some("admin:maintenance:member_add")
        );
    }

    #[test]
    fn whitelisted_endpoints_have_no_capability() {
        assert_eq!(rpc_capability("/coord.auth.Auth/Authenticate"), None);
        assert_eq!(rpc_capability("/coord.auth.Auth/RefreshToken"), None);
        assert_eq!(rpc_capability("/coord.auth.Auth/Bootstrap"), None);
    }

    #[test]
    fn scope_extraction_detects_watch_requests() {
        // ⚠️ 第四轮回归修复：`/coord.watch.Watch/Watch` **不在** body 缓存集合里。
        //
        // 它是服务端流式 RPC，body 在客户端 half-close 之前不会结束；把它纳入
        // `needs_scope_extraction` 会让鉴权层永久缓存 body，请求永不转发 —— watch
        // 彻底不通（这正是本轮实测到的可用性回归）。Watch 的 scope 只能在 handler
        // 拿到**首帧**后判定，因此 `extract_scope_access` 仍然认识 Watch 请求体
        // （供 handler 侧复用），但"要不要先读完 body"的答案必须是 false。
        assert!(!needs_scope_extraction("/coord.watch.Watch/Watch"));
        assert!(is_streaming_rpc("/coord.watch.Watch/Watch"));

        let create = coord_proto::watch::WatchCreateRequest {
            key: b"/app/cfg".to_vec(),
            range_end: Vec::new(),
            ..Default::default()
        };
        let req = coord_proto::watch::WatchRequest {
            request: Some(coord_proto::watch::watch_request::Request::Create(create)),
        };
        let accesses = extract_scope_access("/coord.watch.Watch/Watch", &req.encode_to_vec())
            .expect("watch body should parse");
        // 空前缀 = 字节前缀订阅 → 建模为区间 `[key, prefix_successor(key))`
        // （与 Watch 实际执行的前缀匹配一致，而不是 KV 的“单键”语义）。
        assert_eq!(
            accesses,
            vec![ScopeAccess::range(
                b"/app/cfg".to_vec(),
                b"/app/cfh".to_vec()
            )]
        );
    }

    /// 第四轮回归卡口（core 侧）：**流式 RPC 不得要求缓存 body**。
    ///
    /// 这是本轮实测到的可用性事故的机器化守卫：把流式方法放进
    /// `needs_scope_extraction` 会让鉴权层去 `buffer_request_body`，而流式 body
    /// 永不结束 → 请求永久挡在 handler 之外。
    #[test]
    fn streaming_rpcs_must_not_be_body_buffered() {
        for rpc in [
            "/coord.watch.Watch/Watch",
            "/coord.lease.Lease/LeaseKeepAlive",
            "/coord.mq.v1.MQ/Subscribe",
            "/coord.event.v1.Event/Subscribe",
        ] {
            assert!(is_streaming_rpc(rpc), "{rpc} 应被识别为流式");
            assert!(
                !needs_scope_extraction(rpc),
                "{rpc} 是流式 RPC，不得要求缓存 body（会导致该 RPC 永久挂起）"
            );
        }
        // 一元 scope 承载 RPC 保持不变
        for rpc in [
            "/coord.kv.KV/Put",
            "/coord.kv.KV/Range",
            "/coord.kv.KV/Delete",
            "/coord.txn.Txn/Txn",
        ] {
            assert!(needs_scope_extraction(rpc), "{rpc} 应要求 scope 提取");
            assert!(!is_streaming_rpc(rpc), "{rpc} 是一元 RPC");
        }
    }

    /// [`is_streaming_rpc`] 的**全量**判据：与 proto descriptor 逐条相等（双向）。
    ///
    /// 为什么必须有这条：该函数原先的名单里写着
    /// `/coord.registry.Registry/Watch`、`/coord.mq.MQ/Subscribe`、
    /// `/coord.object_storage.ObjectStorage/Put` 这类**从未存在过/已迁移掉**的路径，
    /// 于是它**从不匹配任何真实请求**，却一直显示为绿 —— 一份"机器守卫"名不副实。
    /// 只有从 descriptor 反解出真实流式集合并**双向**比对，才能让这份清单
    /// 不再依赖"人记得同步改"。
    ///
    /// 判据：
    ///   - 每个 `stream` 方法的 `/pkg.Service/Method` 都必须为 true（不缺）；
    ///   - 每个一元方法的路径都必须为 false（不误报）。
    #[test]
    fn streaming_set_matches_the_proto_descriptors() {
        use prost::Message;

        let fds =
            <prost_types::FileDescriptorSet as Message>::decode(coord_proto::FILE_DESCRIPTOR_SET)
                .expect("coord_descriptor.bin 应能反解为 FileDescriptorSet");

        let mut streaming = std::collections::BTreeSet::new();
        let mut unary = std::collections::BTreeSet::new();
        for file in &fds.file {
            let Some(pkg) = file.package.as_deref() else {
                continue;
            };
            for svc in &file.service {
                let Some(svc_name) = svc.name.as_deref() else {
                    continue;
                };
                for m in &svc.method {
                    let Some(m_name) = m.name.as_deref() else {
                        continue;
                    };
                    let path = format!("/{pkg}.{svc_name}/{m_name}");
                    // prost-types 里这两个是 `Option<bool>`（proto3 省略即未设置）
                    if m.client_streaming.unwrap_or(false) || m.server_streaming.unwrap_or(false) {
                        streaming.insert(path);
                    } else {
                        unary.insert(path);
                    }
                }
            }
        }

        assert!(
            streaming.len() > 5,
            "descriptor 里流式方法太少（{}），判据本身可能失效",
            streaming.len()
        );

        let missing: Vec<&String> = streaming
            .iter()
            .filter(|p| !is_streaming_rpc(p))
            .collect::<Vec<_>>()
            .into_iter()
            .collect();
        assert!(
            missing.is_empty(),
            "以下流式 RPC 未被 is_streaming_rpc 识别（鉴权层会去缓存其 body ⇒ 请求永久挂起）：{missing:?}"
        );

        let overclaimed: Vec<&String> = unary
            .iter()
            .filter(|p| is_streaming_rpc(p))
            .collect::<Vec<_>>()
            .into_iter()
            .collect();
        assert!(
            overclaimed.is_empty(),
            "以下一元 RPC 被误判为流式（会跳过 body 缓存与 scope 判定）：{overclaimed:?}"
        );
    }

    #[test]
    fn watch_explicit_range_end_keeps_interval_semantics() {
        let create = coord_proto::watch::WatchCreateRequest {
            key: b"/app/".to_vec(),
            range_end: b"/app0".to_vec(),
            ..Default::default()
        };
        let req = coord_proto::watch::WatchRequest {
            request: Some(coord_proto::watch::watch_request::Request::Create(create)),
        };
        let accesses = extract_scope_access("/coord.watch.Watch/Watch", &req.encode_to_vec())
            .expect("watch body should parse");
        assert_eq!(
            accesses,
            vec![ScopeAccess::range(b"/app/".to_vec(), b"/app0".to_vec())]
        );
    }

    #[test]
    fn txn_empty_range_end_is_single_key_access() {
        let txn = coord_proto::txn::TxnRequest {
            compare: vec![],
            success: vec![coord_proto::txn::RequestOp {
                op: Some(coord_proto::txn::request_op::Op::RequestRange(
                    coord_proto::kv::RangeRequest {
                        key: b"/app/a".to_vec(),
                        range_end: Vec::new(),
                        ..Default::default()
                    },
                )),
            }],
            failure: vec![],
            request_id: Vec::new(),
        };
        let accesses =
            extract_scope_access("/coord.txn.Txn/Txn", &txn.encode_to_vec()).expect("parse");
        assert_eq!(accesses, vec![ScopeAccess::point(b"/app/a".to_vec())]);
    }

    // ══════════════════════════════════════════════════════════════════════
    // W1-6：流式 RPC（Watch）的 scope 判定改在 handler 侧执行
    //
    // 「延后判定」的前提是**恰好有一个人判定**：鉴权层（缓存 body 提取）或
    // handler（解码首帧）。两份名单若相交，watch 会因为 body 缓存而挂起
    // （第四轮 P0）；若都不覆盖一个 scope 承载的 RPC，则该 RPC 的 scope
    // 判定**不存在**（fail-open）。下面两条把这两个方向都钉住。
    // ══════════════════════════════════════════════════════════════════════

    /// 延后集合的结构性判据：**客户端流式** ∩ **有能力的 RPC**，且与 body 缓存集合互斥。
    #[test]
    fn deferred_scope_rpcs_are_streaming_scope_bearing_and_disjoint() {
        let watch = "/coord.watch.Watch/Watch";
        assert!(
            is_deferred_scope_rpc(watch),
            "Watch 是客户端流式 RPC，其 scope 判定必须延后到 handler"
        );
        assert!(
            is_streaming_rpc(watch),
            "延后的前提是它确实是流式 RPC（否则应走鉴权层 body 提取）"
        );
        assert!(
            !needs_scope_extraction(watch),
            "延后集合与 body 缓存集合必须互斥：同时为真 ⇒ 鉴权层会缓存流 body ⇒ \
             请求永久挂起（第四轮 P0 事故形态）"
        );
        assert_eq!(
            rpc_capability(watch),
            Some("data:watch:subscribe"),
            "延后判定只对**有能力的** RPC 有意义：没有能力的 RPC 无需判定 scope"
        );

        // 反向：body 缓存集合里的每一个都不得同时是流式/延后
        for rpc in [
            "/coord.kv.KV/Put",
            "/coord.kv.KV/Range",
            "/coord.kv.KV/Delete",
            "/coord.txn.Txn/Txn",
        ] {
            assert!(needs_scope_extraction(rpc));
            assert!(!is_streaming_rpc(rpc), "{rpc} 不得被判为流式");
            assert!(!is_deferred_scope_rpc(rpc), "{rpc} 不得被判为延后");
        }
    }

    /// handler 侧的 `watch_create_access` 与鉴权层 body 提取**必须同结果**。
    ///
    /// 这不是"两个实现碰巧一致"的抽查，而是"两条路径共用同一语义"的判据：
    /// 若有人给其中一条换了区间算法（例如把前缀订阅当成单键），本测试立刻红。
    #[test]
    fn watch_create_access_is_identical_to_body_extraction() {
        let cases: Vec<(&[u8], &[u8])> = vec![
            (b"/app/", b""),      // 前缀订阅
            (b"/app", b""),       // 无尾斜杠前缀：不得被当成单键
            (b"", b""),           // 空前缀 = 全 keyspace（上界无界）
            (b"/app/", b"/app0"), // 显式区间
            (b"/app", b"/apz"),   // 区间被前缀收窄（不是裸 [key, range_end)）
            (b"/app/\xff", b""),  // 全 0xFF 尾 → 无有限上界
        ];
        for (key, range_end) in cases {
            let from_handler = watch_create_access(key, range_end);
            let create = coord_proto::watch::WatchCreateRequest {
                key: key.to_vec(),
                range_end: range_end.to_vec(),
                ..Default::default()
            };
            let req = coord_proto::watch::WatchRequest {
                request: Some(coord_proto::watch::watch_request::Request::Create(create)),
            };
            let from_body = extract_scope_access("/coord.watch.Watch/Watch", &req.encode_to_vec())
                .expect("watch body 应可解析");
            assert_eq!(
                from_body,
                vec![from_handler.clone()],
                "key={:?} range_end={:?}：handler 侧区间与鉴权层 body 提取不一致",
                String::from_utf8_lossy(key),
                String::from_utf8_lossy(range_end)
            );
        }
    }

    /// `scope_allows` 的四个方向：无约束授权 / 越界 / 区间半覆盖 / 空 accesses。
    #[test]
    fn scope_allows_is_fail_closed_and_interval_aware() {
        // ① 空 accesses（未能提取资源键）+ 有约束授权 ⇒ 拒绝（fail-closed）
        assert!(!scope_allows(&["/app/".to_string()], &[]));
        // ② 空 accesses + 存在无约束授权 ⇒ 放行
        assert!(scope_allows(&[String::new()], &[]));
        assert!(scope_allows(&["/app/".to_string(), String::new()], &[]));
        // ③ 点访问：命中 / 越界
        assert!(scope_allows(
            &["/app/".to_string()],
            &[ScopeAccess::point(b"/app/a".to_vec())]
        ));
        assert!(!scope_allows(
            &["/app/".to_string()],
            &[ScopeAccess::point(b"/other/a".to_vec())]
        ));
        // ④ 区间：必须**整体包含**（半覆盖 = 拒绝）
        //
        // scope "/app/" 的安全字节前缀是 ["/app/", "/app0")：上界 "/app1" 越出 ⇒ 拒绝。
        let half = ScopeAccess::range(b"/app/".to_vec(), b"/app1".to_vec());
        assert!(
            !scope_allows(&["/app/".to_string()], &[half.clone()]),
            "区间上界越出 scope 覆盖面 ⇒ 必须拒绝（否则可读到未授权 key）"
        );
        // 区间整体落在 scope 内 ⇒ 放行（证明上一条不是"区间一律拒绝"）
        let contained = ScopeAccess::range(b"/app/a".to_vec(), b"/app/b".to_vec());
        assert!(scope_allows(&["/app/".to_string()], &[contained]));
        // 无界上界（etcd 的 "\0" = 到 keyspace 末尾）：有界 scope 不可能覆盖
        let unbounded = ScopeAccess::range(b"/app/".to_vec(), b"\0".to_vec());
        assert!(!scope_allows(&["/app/".to_string()], &[unbounded.clone()]));
        assert!(scope_allows(&["/".to_string()], &[unbounded]));
        // ⑤ 多个访问：任一越界即拒绝
        assert!(!scope_allows(
            &["/app/".to_string()],
            &[
                ScopeAccess::point(b"/app/a".to_vec()),
                ScopeAccess::point(b"/other/a".to_vec()),
            ]
        ));
    }

    /// `check_deferred_scope`：快照缺失 = 无约束（三种既有语义）；快照存在 = 强制判定。
    #[test]
    fn check_deferred_scope_enforces_only_when_snapshot_present() {
        let out_of_scope = vec![ScopeAccess::point(b"/other/a".to_vec())];
        let in_scope = vec![ScopeAccess::point(b"/app/a".to_vec())];

        // 无快照 ⇒ 放行（鉴权关闭 / root / 未经鉴权层）
        assert!(check_deferred_scope(None, &out_of_scope).is_ok());

        // 有约束快照 ⇒ 越界拒绝、范围内放行
        let scoped = DeferredScopeGrants {
            capability_id: "data:watch:subscribe".to_string(),
            grant_scopes: vec!["/app/".to_string()],
        };
        let err =
            check_deferred_scope(Some(&scoped), &out_of_scope).expect_err("越界访问必须被拒绝");
        assert!(
            err.contains("data:watch:subscribe") && err.contains("deferred/fail-closed"),
            "拒绝原因必须可归因（能力 + fail-closed 标注），实际：{err}"
        );
        assert!(check_deferred_scope(Some(&scoped), &in_scope).is_ok());

        // 无约束快照（`""`）⇒ 放行一切（评审：这是"有授权但无 scope 限制"，不是漏洞）
        let unrestricted = DeferredScopeGrants {
            capability_id: "data:watch:subscribe".to_string(),
            grant_scopes: vec![String::new()],
        };
        assert!(check_deferred_scope(Some(&unrestricted), &out_of_scope).is_ok());
    }
}
