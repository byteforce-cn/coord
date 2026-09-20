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
pub fn is_streaming_rpc(rpc_method: &str) -> bool {
    matches!(
        rpc_method,
        // 服务端流
        "/coord.watch.Watch/Watch"
            | "/coord.registry.Registry/Watch"
            | "/coord.config.ConfigCenter/Watch"
            | "/coord.leader_election.LeaderElection/Watch"
            | "/coord.event.Event/Subscribe"
            | "/coord.mq.MQ/Subscribe"
            | "/coord.object_storage.ObjectStorage/Put"
            | "/coord.object_storage.ObjectStorage/Get"
            // 客户端流
            | "/coord.lease.Lease/LeaseKeepAlive"
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
                    // 区间由 `watch_match_interval` 计算——与投递侧 `key_matches`
                    // 是**同一个**函数，故两者不可能漂移。
                    let (lo, hi) =
                        crate::kv_range::watch_match_interval(&create.key, &create.range_end);
                    // `hi = None`（空前缀 / 全 0xFF）⇒ 无有限上界：用 etcd 的无界上界
                    // 符号表示，有界 scope 一律拒绝（fail-closed）。
                    let upper = hi.unwrap_or_else(|| crate::kv_range::UNBOUNDED_RANGE_END.to_vec());
                    accesses.push(ScopeAccess::range(lo, upper));
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
            "/coord.mq.MQ/Subscribe",
            "/coord.event.Event/Subscribe",
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
}
