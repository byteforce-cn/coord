// Capability Registry — stores, manages, and bootstraps capability definitions
//
// Capabilities are stored in Server KV under /_system/capabilities/{id},
// encrypted with Barrier (DEK).
//
// This module provides:
// - In-memory capability store (backed by KV)
// - Bootstrap of all built-in capabilities (89 live + 31 deprecated)
// - Register/Deprecate/List/Get operations
// - Barrier-encrypted persistence via CapabilityStore

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use prost::Message;

use crate::security::barrier::Barrier;

// ──── Capability Registry gRPC Service ────

use coord_proto::capability::capability_registry_server::CapabilityRegistry as CapabilityRegistryTrait;
use coord_proto::capability::*;

/// gRPC service implementation for CapabilityRegistry.
///
/// Wraps the in-memory CapabilityRegistry and exposes Register/List/Get/Deprecate RPCs.
pub struct CapabilityRegistryService {
    pub registry: Arc<CapabilityRegistry>,
}

#[tonic::async_trait]
impl CapabilityRegistryTrait for CapabilityRegistryService {
    async fn register(
        &self,
        request: tonic::Request<CapabilityRegisterRequest>,
    ) -> Result<tonic::Response<CapabilityRegisterResponse>, tonic::Status> {
        let req = request.into_inner();
        let proto_cap = req
            .capability
            .ok_or_else(|| tonic::Status::invalid_argument("missing capability definition"))?;

        let cap = CapabilityDef::from_proto(&proto_cap);
        self.registry
            .register(cap)
            .map_err(tonic::Status::already_exists)?;

        tracing::info!("Capability registered: {}", proto_cap.capability_id);
        Ok(tonic::Response::new(CapabilityRegisterResponse {}))
    }

    async fn list(
        &self,
        _request: tonic::Request<CapabilityListRequest>,
    ) -> Result<tonic::Response<CapabilityListResponse>, tonic::Status> {
        let capabilities: Vec<CapabilityDefinition> = self
            .registry
            .list()
            .into_iter()
            .map(|cap| cap.to_proto())
            .collect();

        Ok(tonic::Response::new(CapabilityListResponse {
            capabilities,
        }))
    }

    async fn get(
        &self,
        request: tonic::Request<CapabilityGetRequest>,
    ) -> Result<tonic::Response<CapabilityGetResponse>, tonic::Status> {
        let req = request.into_inner();
        let capability = self
            .registry
            .get(&req.capability_id)
            .map(|cap| cap.to_proto());

        Ok(tonic::Response::new(CapabilityGetResponse { capability }))
    }

    async fn deprecate(
        &self,
        request: tonic::Request<CapabilityDeprecateRequest>,
    ) -> Result<tonic::Response<CapabilityDeprecateResponse>, tonic::Status> {
        let req = request.into_inner();
        self.registry
            .deprecate(&req.capability_id)
            .map_err(tonic::Status::not_found)?;

        tracing::info!("Capability deprecated: {}", req.capability_id);
        Ok(tonic::Response::new(CapabilityDeprecateResponse {}))
    }
}

// ──── Capability Definition ────

/// Represents a single capability in the unified model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilityDef {
    /// Full capability ID: "data:kv:read"
    pub id: String,
    /// Top-level domain: data | coord | admin
    pub domain: String,
    /// Service name: kv | lease | registry | workflow ...
    pub service: String,
    /// Action: read | write | acquire | define ...
    pub action: String,
    /// Human-readable description
    pub description: String,
    /// Capability type
    pub cap_type: CapabilityType,
    /// Whether this capability is deprecated
    pub deprecated: bool,
    /// Monotonic version number
    pub version: i64,
    /// Documentation for the scope field
    pub scope_description: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilityType {
    Read,
    Write,
    Admin,
}

// ──── Capability Registry ────

/// In-memory capability registry.
///
/// In production, capabilities are persisted to KV via Barrier encryption.
/// This in-memory store serves as a fast cache and is the source of truth
/// during server startup (bootstrapped from built-in definitions).
pub struct CapabilityRegistry {
    /// capability_id → CapabilityDef
    capabilities: Arc<RwLock<HashMap<String, CapabilityDef>>>,
}

impl Default for CapabilityRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl CapabilityRegistry {
    /// Create an empty capability registry.
    pub fn new() -> Self {
        Self {
            capabilities: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a new capability. Returns error if already exists.
    pub fn register(&self, cap: CapabilityDef) -> Result<(), String> {
        let mut caps = self.capabilities.write();
        if caps.contains_key(&cap.id) {
            return Err(format!("capability '{}' already exists", cap.id));
        }
        caps.insert(cap.id.clone(), cap);
        Ok(())
    }

    /// Get a capability by ID.
    pub fn get(&self, id: &str) -> Option<CapabilityDef> {
        self.capabilities.read().get(id).cloned()
    }

    /// List all registered capabilities.
    pub fn list(&self) -> Vec<CapabilityDef> {
        self.capabilities.read().values().cloned().collect()
    }

    /// Deprecate a capability (marks it deprecated, does not remove).
    pub fn deprecate(&self, id: &str) -> Result<(), String> {
        let mut caps = self.capabilities.write();
        match caps.get_mut(id) {
            Some(cap) => {
                cap.deprecated = true;
                Ok(())
            }
            None => Err(format!("capability '{}' not found", id)),
        }
    }

    /// Bootstrap all built-in capabilities (live catalog + deprecated names).
    ///
    /// live 目录是执行面的镜像（`coord_core::grpc_auth::rpc_capability`）；
    /// 已废弃的名字只为存量授权与拼写错误可发现而保留，永不被执行。
    pub fn bootstrap_builtin(&self) {
        let builtins = builtin_capabilities();
        let mut caps = self.capabilities.write();
        for cap in builtins {
            caps.entry(cap.id.clone()).or_insert(cap);
        }
    }

    /// Returns the total count of registered capabilities.
    pub fn count(&self) -> usize {
        self.capabilities.read().len()
    }
}

// ──── Conversion: CapabilityDef ↔ Proto ────

impl CapabilityDef {
    /// Convert to protobuf CapabilityDefinition.
    pub fn to_proto(&self) -> coord_proto::capability::CapabilityDefinition {
        let cap_type = match self.cap_type {
            CapabilityType::Read => coord_proto::capability::CapabilityType::Read,
            CapabilityType::Write => coord_proto::capability::CapabilityType::Write,
            CapabilityType::Admin => coord_proto::capability::CapabilityType::Admin,
        };
        coord_proto::capability::CapabilityDefinition {
            capability_id: self.id.clone(),
            domain: self.domain.clone(),
            service: self.service.clone(),
            action: self.action.clone(),
            description: self.description.clone(),
            r#type: cap_type.into(),
            deprecated: self.deprecated,
            version: self.version,
            scope_description: self.scope_description.clone(),
        }
    }

    /// Convert from protobuf CapabilityDefinition.
    pub fn from_proto(proto: &coord_proto::capability::CapabilityDefinition) -> Self {
        let cap_type = match coord_proto::capability::CapabilityType::try_from(proto.r#type) {
            Ok(coord_proto::capability::CapabilityType::Read) => CapabilityType::Read,
            Ok(coord_proto::capability::CapabilityType::Write) => CapabilityType::Write,
            Ok(coord_proto::capability::CapabilityType::Admin) => CapabilityType::Admin,
            Err(_) => CapabilityType::Read, // Default fallback
        };
        Self {
            id: proto.capability_id.clone(),
            domain: proto.domain.clone(),
            service: proto.service.clone(),
            action: proto.action.clone(),
            description: proto.description.clone(),
            cap_type,
            deprecated: proto.deprecated,
            version: proto.version,
            scope_description: proto.scope_description.clone(),
        }
    }
}

// ──── Capability Store (Barrier-encrypted persistence) ────

/// Encrypted capability persistence layer.
///
/// Capabilities are serialized to protobuf, encrypted with Barrier (AES-256-GCM),
/// and stored under `/_system/capabilities/{id}` in the Server KV.
///
/// For testing, the store can operate without a real KV backend by using
/// an in-memory buffer.
pub struct CapabilityStore {
    barrier: Barrier,
}

impl CapabilityStore {
    /// Create a new capability store backed by the given Barrier.
    pub fn new(barrier: Barrier) -> Self {
        Self { barrier }
    }

    /// Serialize and encrypt a capability definition.
    ///
    /// Returns the encrypted bytes suitable for KV storage.
    pub fn encrypt_capability(&self, cap: &CapabilityDef) -> Result<Vec<u8>, String> {
        let proto = cap.to_proto();
        let serialized = proto.encode_to_vec();
        self.barrier
            .encrypt(&serialized)
            .map_err(|e| format!("Barrier encrypt failed: {e}"))
    }

    /// Decrypt and deserialize a capability definition.
    pub fn decrypt_capability(&self, encrypted: &[u8]) -> Result<CapabilityDef, String> {
        let plaintext = self
            .barrier
            .decrypt(encrypted)
            .map_err(|e| format!("Barrier decrypt failed: {e}"))?;
        let proto = coord_proto::capability::CapabilityDefinition::decode(plaintext.as_slice())
            .map_err(|e| format!("Protobuf decode failed: {e}"))?;
        Ok(CapabilityDef::from_proto(&proto))
    }

    /// Encrypt and serialize multiple capabilities.
    pub fn encrypt_all(&self, caps: &[CapabilityDef]) -> Result<Vec<Vec<u8>>, String> {
        caps.iter()
            .map(|cap| self.encrypt_capability(cap))
            .collect()
    }

    /// Decrypt and deserialize multiple capabilities.
    pub fn decrypt_all(&self, encrypted_list: &[Vec<u8>]) -> Result<Vec<CapabilityDef>, String> {
        encrypted_list
            .iter()
            .map(|enc| self.decrypt_capability(enc))
            .collect()
    }

    /// Get a reference to the underlying Barrier.
    pub fn barrier(&self) -> &Barrier {
        &self.barrier
    }
}

// ──── Built-in Capabilities ────
//
// 两张表就是发现面的全部事实。`BUILTIN_CAPABILITY_META` 的 ID 集合必须与执行面
// （`coord_core::grpc_auth::rpc_capability` 的返回值集合）**双向恒等**；
// `DEPRECATED_CAPABILITIES` 里的名字永远不会被执行，只用于让存量授权与拼写错误可发现。
// 双向恒等与废弃纪律由本文件 tests 从 proto 描述符集枚举方法后机检，
// 并由 `scripts/check-capability-registry.sh` 作为 CI 门禁（含负控制）。

/// live 能力目录：`(id, 类型, 描述)`。
///
/// `domain:service:action` 三段从 ID 解析（[`capability_def`]），不在表里重复；
/// 三段式由 `builtin_ids_are_domain_service_action` 钉住。
const BUILTIN_CAPABILITY_META: &[(&str, CapabilityType, &str)] = &[
    // ──── data ────
    (
        "data:kv:delete",
        CapabilityType::Write,
        "按前缀/范围删除键值对",
    ),
    (
        "data:kv:read",
        CapabilityType::Read,
        "按前缀/范围读取键值对",
    ),
    ("data:kv:write", CapabilityType::Write, "写入/更新键值对"),
    ("data:lease:grant", CapabilityType::Write, "创建租约"),
    (
        "data:lease:keepalive",
        CapabilityType::Write,
        "租约心跳续期",
    ),
    ("data:lease:revoke", CapabilityType::Write, "撤销租约"),
    (
        "data:lease:read",
        CapabilityType::Read,
        "查询租约剩余 TTL 与绑定的 key（coord.lease.v2 TimeToLive）",
    ),
    (
        "data:tso:allocate",
        CapabilityType::Write,
        "分配全局 TSO 批次/单值（coord.tso.v2；推进持久化水位）",
    ),
    (
        "data:storage:read",
        CapabilityType::Read,
        "Get/Stat 对象（coord.storage）",
    ),
    (
        "data:storage:write",
        CapabilityType::Write,
        "Put/Delete 对象（coord.storage）",
    ),
    ("data:txn:execute", CapabilityType::Write, "执行原子事务"),
    (
        "data:watch:subscribe",
        CapabilityType::Read,
        "订阅 Key 变更事件",
    ),
    // ──── coord ────
    (
        "coord:breaker:manage",
        CapabilityType::Write,
        "重置熔断器（CircuitBreaker/Reset）",
    ),
    (
        "coord:breaker:read",
        CapabilityType::Read,
        "读取熔断器状态（CircuitBreaker/GetState）",
    ),
    (
        "coord:breaker:report",
        CapabilityType::Write,
        "上报熔断器成功/失败（ReportSuccess/ReportFailure）",
    ),
    (
        "coord:cache:read",
        CapabilityType::Read,
        "读取缓存（Get/HGet/HGetAll/Keys/LLen/LRange/SMembers）",
    ),
    (
        "coord:cache:write",
        CapabilityType::Write,
        "写入/删除缓存（Set/HSet/LPush/RPop/SAdd/Delete/Clear）",
    ),
    (
        "coord:config:list",
        CapabilityType::Read,
        "列出配置（Config/List）",
    ),
    ("coord:config:read", CapabilityType::Read, "读取配置"),
    (
        "coord:config:watch",
        CapabilityType::Read,
        "订阅配置变更（Config/Watch）",
    ),
    ("coord:config:write", CapabilityType::Write, "变更配置"),
    (
        "coord:election:campaign",
        CapabilityType::Write,
        "参与 Leader 选举（LeaderElection/Campaign）",
    ),
    (
        "coord:election:read",
        CapabilityType::Read,
        "查询当前 Leader（LeaderElection/GetLeader）",
    ),
    (
        "coord:election:resign",
        CapabilityType::Write,
        "放弃 Leader（LeaderElection/Resign）",
    ),
    (
        "coord:election:watch",
        CapabilityType::Read,
        "订阅 Leader 变更（LeaderElection/Watch）",
    ),
    ("coord:event:publish", CapabilityType::Write, "发布事件"),
    ("coord:event:subscribe", CapabilityType::Read, "订阅事件"),
    (
        "coord:event:unsubscribe",
        CapabilityType::Write,
        "取消事件订阅（Event/Unsubscribe）",
    ),
    (
        "coord:flags:manage",
        CapabilityType::Write,
        "管理 Feature Flag（PutFlag/DeleteFlag）",
    ),
    (
        "coord:flags:read",
        CapabilityType::Read,
        "读取/评估 Feature Flag（Evaluate/IsEnabled/ListFlags）",
    ),
    (
        "coord:handshake:negotiate",
        CapabilityType::Read,
        "agent 握手协商（Handshake/Negotiate）",
    ),
    (
        "coord:health:check",
        CapabilityType::Read,
        "agent 健康检查（Health/Check）",
    ),
    (
        "coord:idgen:next",
        CapabilityType::Write,
        "生成全局唯一 ID（NextId/NextBatch）",
    ),
    ("coord:lock:acquire", CapabilityType::Write, "获取分布式锁"),
    (
        "coord:lock:info",
        CapabilityType::Read,
        "查询锁状态（Lock/GetLockInfo）",
    ),
    ("coord:lock:release", CapabilityType::Write, "释放分布式锁"),
    ("coord:lock:renew", CapabilityType::Write, "续期分布式锁"),
    (
        "coord:mq:consume",
        CapabilityType::Write,
        "消费消息（Poll/Ack/GetTopicLeader）",
    ),
    (
        "coord:mq:dlq",
        CapabilityType::Write,
        "死信队列处置（MoveToDlq/PollDlq）",
    ),
    (
        "coord:mq:manage",
        CapabilityType::Write,
        "管理 Topic（CreateTopic/DeleteTopic）",
    ),
    (
        "coord:mq:publish",
        CapabilityType::Write,
        "发布消息（MQ/Publish）",
    ),
    (
        "coord:mq:subscribe",
        CapabilityType::Read,
        "订阅 Topic（MQ/Subscribe）",
    ),
    (
        "coord:plugin:invoke",
        CapabilityType::Write,
        "调用插件（Plugin/Invoke）",
    ),
    (
        "coord:plugin:list",
        CapabilityType::Read,
        "列出插件（Plugin/List）",
    ),
    (
        "coord:policy:evaluate",
        CapabilityType::Read,
        "执行策略评估",
    ),
    ("coord:policy:manage", CapabilityType::Write, "管理策略规则"),
    (
        "coord:ratelimit:check",
        CapabilityType::Read,
        "限流检查（RateLimiter/Allow）",
    ),
    (
        "coord:registry:deregister",
        CapabilityType::Write,
        "注销服务实例",
    ),
    ("coord:registry:discover", CapabilityType::Read, "服务发现"),
    (
        "coord:registry:heartbeat",
        CapabilityType::Write,
        "实例心跳上报",
    ),
    (
        "coord:registry:register",
        CapabilityType::Write,
        "注册服务实例",
    ),
    (
        "coord:registry:watch",
        CapabilityType::Read,
        "订阅服务目录变更（Registry/Watch）",
    ),
    (
        "coord:replica:read",
        CapabilityType::Read,
        "副本对账（Replica/Reconcile）",
    ),
    (
        "coord:replica:write",
        CapabilityType::Write,
        "副本写入与 ISR 心跳（Replica/Apply、Replica/IsrHeartbeat）",
    ),
    (
        "coord:scheduler:execute",
        CapabilityType::Write,
        "领取/完成调度任务（ClaimJob/CompleteJob/Heartbeat）",
    ),
    (
        "coord:scheduler:manage",
        CapabilityType::Write,
        "注册调度任务（Scheduler/RegisterJob）",
    ),
    (
        "coord:scheduler:read",
        CapabilityType::Read,
        "查询调度任务（Scheduler/GetJob）",
    ),
    (
        "coord:transit:crypto",
        CapabilityType::Write,
        "加解密与 HMAC（Encrypt/Decrypt/HmacSign/HmacVerify/Rewrap）",
    ),
    (
        "coord:workflow:define",
        CapabilityType::Write,
        "定义工作流模板",
    ),
    (
        "coord:workflow:execute",
        CapabilityType::Write,
        "驱动工作流实例（Start/Signal/Approve/Reject/…）",
    ),
    (
        "coord:workflow:read",
        CapabilityType::Read,
        "查询工作流定义与实例（Get/List 系列）",
    ),
    // ──── pki ────
    (
        "pki:ca:init",
        CapabilityType::Write,
        "初始化 PKI CA（Pki/InitCa）",
    ),
    (
        "pki:cert:issue",
        CapabilityType::Write,
        "签发/续期证书（IssueCert/RenewCert）",
    ),
    (
        "pki:cert:read",
        CapabilityType::Read,
        "读取/校验证书（GetCaCert/GetCertByCN/ListCerts/VerifyCert）",
    ),
    (
        "pki:cert:rotate",
        CapabilityType::Write,
        "轮换证书（Pki/RotateCert）",
    ),
    // ──── admin ────
    (
        "admin:auth:bootstrap_token",
        CapabilityType::Admin,
        "签发/列出/撤销 agent bootstrap 令牌（TTL + 一次性）",
    ),
    ("admin:auth:disable", CapabilityType::Admin, "禁用认证"),
    ("admin:auth:enable", CapabilityType::Admin, "启用认证"),
    ("admin:auth:role_add", CapabilityType::Admin, "创建角色"),
    ("admin:auth:role_delete", CapabilityType::Admin, "删除角色"),
    (
        "admin:auth:role_grant",
        CapabilityType::Admin,
        "为角色授权能力",
    ),
    ("admin:auth:role_list", CapabilityType::Read, "列出角色"),
    (
        "admin:auth:role_revoke",
        CapabilityType::Admin,
        "撤销角色能力",
    ),
    ("admin:auth:status", CapabilityType::Read, "查询认证状态"),
    (
        "admin:auth:user_add",
        CapabilityType::Admin,
        "创建用户/AppRole",
    ),
    (
        "admin:auth:user_delete",
        CapabilityType::Admin,
        "删除用户/AppRole",
    ),
    (
        "admin:auth:user_grant_role",
        CapabilityType::Admin,
        "为用户分配角色",
    ),
    ("admin:auth:user_list", CapabilityType::Read, "列出用户"),
    (
        "admin:auth:user_revoke_role",
        CapabilityType::Admin,
        "撤销用户角色",
    ),
    (
        "admin:capability:deprecate",
        CapabilityType::Admin,
        "废弃能力",
    ),
    (
        "admin:capability:list",
        CapabilityType::Read,
        "列出所有能力",
    ),
    (
        "admin:capability:register",
        CapabilityType::Admin,
        "注册新能力定义",
    ),
    (
        "admin:maintenance:compact",
        CapabilityType::Admin,
        "压缩历史（raft 下发）",
    ),
    (
        "admin:maintenance:member_add",
        CapabilityType::Admin,
        "添加节点",
    ),
    (
        "admin:maintenance:member_list",
        CapabilityType::Read,
        "列出节点",
    ),
    (
        "admin:maintenance:member_promote",
        CapabilityType::Admin,
        "晋升 Learner",
    ),
    (
        "admin:maintenance:member_remove",
        CapabilityType::Admin,
        "移除节点",
    ),
    ("admin:maintenance:seal", CapabilityType::Admin, "封存集群"),
    (
        "admin:maintenance:snapshot",
        CapabilityType::Read,
        "导出快照",
    ),
    (
        "admin:maintenance:status",
        CapabilityType::Read,
        "查询集群状态",
    ),
    (
        "admin:maintenance:unseal",
        CapabilityType::Admin,
        "解封集群",
    ),
];

/// 已废弃能力：`(id, 类型, 原描述, 替代项)`。
///
/// 替代项为空串 = 无替代（该功能未实现）。这些 ID 不在执行面，
/// `RoleGrantCapability` 会拒绝它们并点名替代项。
const DEPRECATED_CAPABILITIES: &[(&str, CapabilityType, &str, &str)] = &[
    // ──── data ────
    (
        "data:cache:delete",
        CapabilityType::Write,
        "删除/清空本地缓存",
        "coord:cache:write",
    ),
    (
        "data:cache:read",
        CapabilityType::Read,
        "读取本地缓存",
        "coord:cache:read",
    ),
    (
        "data:cache:write",
        CapabilityType::Write,
        "写入本地缓存",
        "coord:cache:write",
    ),
    (
        "data:idgen:generate",
        CapabilityType::Write,
        "生成全局唯一 ID",
        "coord:idgen:next",
    ),
    (
        "data:mq:manage",
        CapabilityType::Write,
        "管理 Topic",
        "coord:mq:manage",
    ),
    (
        "data:mq:receive",
        CapabilityType::Read,
        "消费消息",
        "coord:mq:consume",
    ),
    (
        "data:mq:send",
        CapabilityType::Write,
        "发送消息",
        "coord:mq:publish",
    ),
    // ──── coord ────
    (
        "coord:config:rollback",
        CapabilityType::Write,
        "回滚配置",
        "",
    ),
    (
        "coord:leader:campaign",
        CapabilityType::Write,
        "参与 Leader 选举",
        "coord:election:campaign",
    ),
    (
        "coord:leader:observe",
        CapabilityType::Read,
        "观察选举结果",
        "coord:election:read",
    ),
    (
        "coord:leader:resign",
        CapabilityType::Write,
        "放弃 Leader",
        "coord:election:resign",
    ),
    (
        "coord:lock:query",
        CapabilityType::Read,
        "查询锁状态",
        "coord:lock:info",
    ),
    (
        "coord:pki:issue",
        CapabilityType::Write,
        "签发短期证书",
        "pki:cert:issue",
    ),
    (
        "coord:pki:read",
        CapabilityType::Read,
        "查询证书",
        "pki:cert:read",
    ),
    ("coord:pki:revoke", CapabilityType::Write, "吊销证书", ""),
    (
        "coord:resilience:circuit_breaker",
        CapabilityType::Write,
        "熔断开关",
        "coord:breaker:manage",
    ),
    (
        "coord:resilience:rate_limit",
        CapabilityType::Write,
        "限流规则管理",
        "coord:ratelimit:check",
    ),
    (
        "coord:resilience:replication",
        CapabilityType::Write,
        "副本同步控制",
        "coord:replica:read",
    ),
    (
        "coord:saga:compensate",
        CapabilityType::Write,
        "补偿/回滚",
        "",
    ),
    ("coord:saga:execute", CapabilityType::Write, "启动 Saga", ""),
    (
        "coord:saga:query",
        CapabilityType::Read,
        "查询 Saga 状态",
        "",
    ),
    (
        "coord:scheduler:cancel",
        CapabilityType::Write,
        "取消定时任务",
        "",
    ),
    (
        "coord:scheduler:query",
        CapabilityType::Read,
        "查询任务状态",
        "coord:scheduler:read",
    ),
    (
        "coord:scheduler:schedule",
        CapabilityType::Write,
        "创建定时任务",
        "coord:scheduler:manage",
    ),
    (
        "coord:transit:decrypt",
        CapabilityType::Read,
        "解密数据",
        "coord:transit:crypto",
    ),
    (
        "coord:transit:encrypt",
        CapabilityType::Write,
        "加密数据",
        "coord:transit:crypto",
    ),
    (
        "coord:workflow:cancel",
        CapabilityType::Write,
        "取消工作流",
        "coord:workflow:execute",
    ),
    (
        "coord:workflow:query",
        CapabilityType::Read,
        "查询工作流状态",
        "coord:workflow:read",
    ),
    (
        "coord:workflow:signal",
        CapabilityType::Write,
        "发送信号/推进状态",
        "coord:workflow:execute",
    ),
    (
        "coord:workflow:start",
        CapabilityType::Write,
        "启动工作流实例",
        "coord:workflow:execute",
    ),
    // ──── admin ────
    (
        "admin:capability:get",
        CapabilityType::Read,
        "查询能力详情",
        "admin:capability:list",
    ),
];

/// 把 `domain:service:action` 解析为三段；段数或空段即返回 `None`。
///
/// 返回 `None` 而不 panic：内建目录是编译期常量，坏行由 `builtin_ids_are_domain_service_action` 置红，
/// 运行时（bootstrap）不得因一条坏数据让进程崩溃。
fn capability_def(
    id: &str,
    cap_type: CapabilityType,
    description: String,
    deprecated: bool,
) -> Option<CapabilityDef> {
    let mut parts = id.split(':');
    let (Some(domain), Some(service), Some(action), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return None;
    };
    if domain.is_empty() || service.is_empty() || action.is_empty() {
        return None;
    }
    Some(CapabilityDef {
        id: id.to_string(),
        domain: domain.to_string(),
        service: service.to_string(),
        action: action.to_string(),
        description,
        cap_type,
        deprecated,
        version: 1,
        scope_description: String::new(),
    })
}

/// `id` 是否属于 live 内建能力（执行面覆盖的能力）。
pub fn is_live_builtin(capability_id: &str) -> bool {
    BUILTIN_CAPABILITY_META
        .iter()
        .any(|(id, _, _)| *id == capability_id)
}

/// `id` 是否属于已废弃的内建能力。
pub fn is_deprecated_builtin(capability_id: &str) -> bool {
    DEPRECATED_CAPABILITIES
        .iter()
        .any(|(id, _, _, _)| *id == capability_id)
}

/// 已废弃能力的替代项：`None` = 不是已废弃能力；`Some("")` = 无替代。
pub fn deprecated_replacement(capability_id: &str) -> Option<&'static str> {
    DEPRECATED_CAPABILITIES
        .iter()
        .find(|(id, _, _, _)| *id == capability_id)
        .map(|(_, _, _, repl)| *repl)
}

/// Bootstrap 用的内建能力清单：live 在前，deprecated 在后。
fn builtin_capabilities() -> Vec<CapabilityDef> {
    let live = BUILTIN_CAPABILITY_META
        .iter()
        .filter_map(|(id, ty, desc)| capability_def(id, *ty, (*desc).to_string(), false));
    let deprecated = DEPRECATED_CAPABILITIES
        .iter()
        .filter_map(|(id, ty, desc, repl)| {
            let note = if repl.is_empty() {
                "（已废弃：无替代，功能未实现）".to_string()
            } else {
                format!("（已废弃：替代能力 {repl}）")
            };
            capability_def(id, *ty, format!("{desc}{note}"), true)
        });
    live.chain(deprecated).collect()
}

// ──── Tests ────

#[cfg(test)]
mod tests {
    use super::*;

    // ──── 执行面 ↔ 发现面（机检的 Rust 侧）────

    /// proto 事实源里的全部方法（`/package.Service/Method`）。
    ///
    /// 输入是编译期 FileDescriptorSet，不是另一份手写清单：新增 RPC 会自动出现在这里。
    fn proto_methods() -> Vec<String> {
        let set = prost_types::FileDescriptorSet::decode(coord_proto::FILE_DESCRIPTOR_SET)
            .expect("compiled FileDescriptorSet must decode");
        let mut out = Vec::new();
        for file in &set.file {
            let package = file.package.clone().unwrap_or_default();
            for service in &file.service {
                for method in &service.method {
                    out.push(format!(
                        "/{package}.{}/{}",
                        service.name.clone().unwrap_or_default(),
                        method.name.clone().unwrap_or_default()
                    ));
                }
            }
        }
        out.sort();
        out
    }

    /// 执行面：`rpc_capability` 对 proto 全部方法返回的能力 ID 集合。
    fn execution_face() -> std::collections::BTreeSet<String> {
        proto_methods()
            .into_iter()
            .filter_map(|m| coord_core::grpc_auth::rpc_capability(&m).map(str::to_string))
            .collect()
    }

    /// 不需要能力的方法：认证前置端点（interceptor 白名单 + 角色同步端点）与 raft 内部 RPC。
    ///
    /// raft RPC 走 raft 端口，不经过 agent 面鉴权层；其余端点由服务内自证。
    /// 本清单是**收口常量**：豁免范围的变化必须显式改这里。
    const CAPABILITY_FREE_METHODS: &[&str] = &[
        "/coord.auth.Auth/Authenticate",
        "/coord.auth.Auth/RefreshToken",
        "/coord.auth.Auth/Bootstrap",
        "/coord.auth.Auth/GetRevocationDelta",
        "/coord.raft.Raft/AppendEntries",
        "/coord.raft.Raft/DrainRange",
        "/coord.raft.Raft/InstallSnapshot",
        "/coord.raft.Raft/InstallSnapshotStreaming",
        "/coord.raft.Raft/ReportRegionStats",
        "/coord.raft.Raft/SubmitPdOp",
        "/coord.raft.Raft/SubmitRegionCommand",
        "/coord.raft.Raft/TransferLeader",
        "/coord.raft.Raft/Vote",
    ];

    fn is_capability_free(method: &str) -> bool {
        crate::auth::interceptor::is_whitelisted(method)
            || method == "/coord.auth.Auth/GetRevocationDelta"
            || method.starts_with("/coord.raft.Raft/")
    }

    /// 每个 proto 方法都必须要么有执行面映射，要么在豁免清单里。
    #[test]
    fn every_proto_method_is_mapped_or_capability_free() {
        let mut unmapped = Vec::new();
        let mut exempt = std::collections::BTreeSet::new();
        for method in proto_methods() {
            if coord_core::grpc_auth::rpc_capability(&method).is_some() {
                continue;
            }
            if is_capability_free(&method) {
                exempt.insert(method);
            } else {
                unmapped.push(method);
            }
        }
        assert!(
            unmapped.is_empty(),
            "RPC 未登记能力映射（新增 RPC 必须先在 rpc_capability 登记，或显式加入豁免清单）: {unmapped:?}"
        );
        let expected: std::collections::BTreeSet<String> = CAPABILITY_FREE_METHODS
            .iter()
            .map(|m| (*m).to_string())
            .collect();
        assert_eq!(
            exempt, expected,
            "能力豁免集合漂移：白名单/raft 内部 RPC 的增删必须同步 CAPABILITY_FREE_METHODS"
        );
    }

    /// registry 的 live 集合与执行面**双向恒等**。
    #[test]
    fn registry_live_set_equals_execution_face() {
        let executed = execution_face();
        let live: std::collections::BTreeSet<String> = builtin_capabilities()
            .into_iter()
            .filter(|cap| !cap.deprecated)
            .map(|cap| cap.id)
            .collect();
        let missing: Vec<&String> = executed.difference(&live).collect();
        let extra: Vec<&String> = live.difference(&executed).collect();
        assert!(
            missing.is_empty(),
            "执行面有、registry 未登记（live）: {missing:?}"
        );
        assert!(
            extra.is_empty(),
            "registry 有、执行面不执行且未标 deprecated: {extra:?}"
        );
    }

    /// 废弃条目必须写明替代项（或明确「无替代」），替代项本身必须是 live 能力；
    /// 标了废弃却仍在执行面 = 语义自相矛盾，必须红。
    #[test]
    fn deprecated_entries_name_a_live_replacement_or_reason() {
        let executed = execution_face();
        let deprecated: Vec<CapabilityDef> = builtin_capabilities()
            .into_iter()
            .filter(|cap| cap.deprecated)
            .collect();
        assert!(
            !deprecated.is_empty(),
            "废弃目录不得为空（空集合会让本测试假绿）"
        );
        for cap in deprecated {
            let replacement = deprecated_replacement(&cap.id)
                .expect("deprecated capability must come from DEPRECATED_CAPABILITIES");
            assert!(
                !is_live_builtin(&cap.id),
                "{} 同时出现在 live 与 deprecated 目录",
                cap.id
            );
            assert!(
                !executed.contains(&cap.id),
                "{} 已标废弃但仍在执行面（rpc_capability 仍返回它）",
                cap.id
            );
            if replacement.is_empty() {
                assert!(
                    cap.description.contains("无替代"),
                    "{} 没有替代项，描述必须写明「无替代」及原因",
                    cap.id
                );
            } else {
                assert!(
                    cap.description.contains(replacement),
                    "{} 的描述必须点名替代能力 {replacement}",
                    cap.id
                );
                assert!(
                    is_live_builtin(replacement),
                    "{} 的替代项 {replacement} 不是 live 能力",
                    cap.id
                );
            }
        }
    }

    /// 内建目录的 ID 必须是 `domain:service:action` 三段非空，且与拆解字段一致。
    #[test]
    fn builtin_ids_are_domain_service_action() {
        for cap in builtin_capabilities() {
            let parts: Vec<&str> = cap.id.split(':').collect();
            assert_eq!(
                parts.len(),
                3,
                "{} 不是 domain:service:action 三段式",
                cap.id
            );
            assert_eq!(parts[0], cap.domain, "{} 的 domain 与 ID 不一致", cap.id);
            assert_eq!(parts[1], cap.service, "{} 的 service 与 ID 不一致", cap.id);
            assert_eq!(parts[2], cap.action, "{} 的 action 与 ID 不一致", cap.id);
            assert!(parts.iter().all(|p| !p.is_empty()), "{} 含空段", cap.id);
        }
    }

    // ──── Capability Registry CRUD ────

    #[test]
    fn test_register_and_get_capability() {
        let registry = CapabilityRegistry::new();
        let cap = CapabilityDef {
            id: "test:svc:action".to_string(),
            domain: "test".to_string(),
            service: "svc".to_string(),
            action: "action".to_string(),
            description: "Test capability".to_string(),
            cap_type: CapabilityType::Read,
            deprecated: false,
            version: 1,
            scope_description: "".to_string(),
        };

        registry
            .register(cap.clone())
            .expect("register should succeed");
        let retrieved = registry
            .get("test:svc:action")
            .expect("should find capability");
        assert_eq!(retrieved, cap);
    }

    #[test]
    fn test_register_duplicate_fails() {
        let registry = CapabilityRegistry::new();
        let cap = CapabilityDef {
            id: "dup:svc:action".to_string(),
            domain: "dup".to_string(),
            service: "svc".to_string(),
            action: "action".to_string(),
            description: "Dup".to_string(),
            cap_type: CapabilityType::Read,
            deprecated: false,
            version: 1,
            scope_description: "".to_string(),
        };

        registry
            .register(cap.clone())
            .expect("first register should succeed");
        let result = registry.register(cap);
        assert!(result.is_err(), "duplicate register should fail");
    }

    #[test]
    fn test_deprecate_capability() {
        let registry = CapabilityRegistry::new();
        let cap = CapabilityDef {
            id: "dep:svc:action".to_string(),
            domain: "dep".to_string(),
            service: "svc".to_string(),
            action: "action".to_string(),
            description: "To deprecate".to_string(),
            cap_type: CapabilityType::Read,
            deprecated: false,
            version: 1,
            scope_description: "".to_string(),
        };

        registry.register(cap).unwrap();
        registry
            .deprecate("dep:svc:action")
            .expect("deprecate should succeed");
        let deprecated = registry.get("dep:svc:action").unwrap();
        assert!(deprecated.deprecated, "capability should be deprecated");
    }

    #[test]
    fn test_deprecate_nonexistent_fails() {
        let registry = CapabilityRegistry::new();
        let result = registry.deprecate("nonexistent:svc:action");
        assert!(result.is_err());
    }

    #[test]
    fn test_list_capabilities() {
        let registry = CapabilityRegistry::new();
        let cap1 = CapabilityDef {
            id: "list:svc:read".to_string(),
            domain: "list".to_string(),
            service: "svc".to_string(),
            action: "read".to_string(),
            description: "Read".to_string(),
            cap_type: CapabilityType::Read,
            deprecated: false,
            version: 1,
            scope_description: "".to_string(),
        };
        let cap2 = CapabilityDef {
            id: "list:svc:write".to_string(),
            domain: "list".to_string(),
            service: "svc".to_string(),
            action: "write".to_string(),
            description: "Write".to_string(),
            cap_type: CapabilityType::Write,
            deprecated: false,
            version: 1,
            scope_description: "".to_string(),
        };

        registry.register(cap1).unwrap();
        registry.register(cap2).unwrap();

        let all = registry.list();
        assert_eq!(all.len(), 2);
    }

    // ──── Bootstrap built-in capabilities ────

    #[test]
    fn test_bootstrap_all_capabilities() {
        let registry = CapabilityRegistry::new();
        registry.bootstrap_builtin();
        assert_eq!(
            registry.count(),
            122,
            "should have all 122 built-in capabilities (91 live: data 12 / coord 49 / pki 4 / admin 26; \
             31 deprecated: data 7 / coord 23 / admin 1)"
        );
    }

    #[test]
    fn test_bootstrap_is_idempotent() {
        let registry = CapabilityRegistry::new();
        registry.bootstrap_builtin();
        let count1 = registry.count();
        registry.bootstrap_builtin();
        assert_eq!(registry.count(), count1, "bootstrap should be idempotent");
    }

    #[test]
    fn test_bootstrap_has_expected_domains() {
        let registry = CapabilityRegistry::new();
        registry.bootstrap_builtin();

        // Verify key capabilities exist across domains
        assert!(registry.get("data:kv:read").is_some());
        assert!(registry.get("data:kv:write").is_some());
        assert!(registry.get("coord:lock:acquire").is_some());
        assert!(registry.get("coord:workflow:define").is_some());
        assert!(registry.get("admin:auth:enable").is_some());
        assert!(registry.get("admin:capability:register").is_some());
    }

    #[test]
    fn test_capability_type_enum() {
        let registry = CapabilityRegistry::new();
        registry.bootstrap_builtin();

        let kv_read = registry.get("data:kv:read").unwrap();
        assert_eq!(kv_read.cap_type, CapabilityType::Read);

        let kv_write = registry.get("data:kv:write").unwrap();
        assert_eq!(kv_write.cap_type, CapabilityType::Write);

        let admin_op = registry.get("admin:auth:enable").unwrap();
        assert_eq!(admin_op.cap_type, CapabilityType::Admin);
    }

    // ──── Barrier-encrypted capability storage ────

    /// Helper: create a Barrier + CapabilityStore for testing.
    fn make_store() -> CapabilityStore {
        let (keyring, _encrypted_dek) =
            crate::security::key_management::Keyring::bootstrap().unwrap();
        let barrier = Barrier::new(Arc::new(keyring));
        CapabilityStore::new(barrier)
    }

    #[test]
    fn test_encrypt_decrypt_capability_roundtrip() {
        let store = make_store();
        let cap = CapabilityDef {
            id: "data:kv:read".to_string(),
            domain: "data".to_string(),
            service: "kv".to_string(),
            action: "read".to_string(),
            description: "按前缀/范围读取键值对".to_string(),
            cap_type: CapabilityType::Read,
            deprecated: false,
            version: 1,
            scope_description: "key prefix or range".to_string(),
        };

        let encrypted = store
            .encrypt_capability(&cap)
            .expect("encrypt should succeed");
        // Encrypted data should be different from plaintext
        let proto = cap.to_proto();
        let plain_bytes = proto.encode_to_vec();
        assert_ne!(
            encrypted, plain_bytes,
            "encrypted data must differ from plaintext"
        );

        let decrypted = store
            .decrypt_capability(&encrypted)
            .expect("decrypt should succeed");
        assert_eq!(decrypted.id, cap.id);
        assert_eq!(decrypted.domain, cap.domain);
        assert_eq!(decrypted.service, cap.service);
        assert_eq!(decrypted.action, cap.action);
        assert_eq!(decrypted.description, cap.description);
        assert_eq!(decrypted.cap_type, cap.cap_type);
        assert_eq!(decrypted.deprecated, cap.deprecated);
        assert_eq!(decrypted.version, cap.version);
    }

    #[test]
    fn test_encrypt_decrypt_all_capabilities() {
        let store = make_store();
        let registry = CapabilityRegistry::new();
        registry.bootstrap_builtin();
        let all_caps = registry.list();
        assert_eq!(all_caps.len(), 122);

        // Encrypt all 120 capabilities
        let encrypted_all = store
            .encrypt_all(&all_caps)
            .expect("encrypt all should succeed");
        assert_eq!(encrypted_all.len(), 122);

        // Decrypt all
        let decrypted_all = store
            .decrypt_all(&encrypted_all)
            .expect("decrypt all should succeed");
        assert_eq!(decrypted_all.len(), 122);

        // Verify round-trip for each
        for (orig, dec) in all_caps.iter().zip(decrypted_all.iter()) {
            assert_eq!(orig.id, dec.id);
            assert_eq!(orig.cap_type, dec.cap_type);
            assert_eq!(orig.deprecated, dec.deprecated);
        }
    }

    #[test]
    fn test_decrypt_tampered_data_fails() {
        let store = make_store();
        let cap = CapabilityDef {
            id: "test:svc:action".to_string(),
            domain: "test".to_string(),
            service: "svc".to_string(),
            action: "action".to_string(),
            description: "Test".to_string(),
            cap_type: CapabilityType::Read,
            deprecated: false,
            version: 1,
            scope_description: "".to_string(),
        };

        let mut encrypted = store
            .encrypt_capability(&cap)
            .expect("encrypt should succeed");

        // Tamper with the ciphertext
        if encrypted.len() > 20 {
            encrypted[20] ^= 0xFF;
        }

        let result = store.decrypt_capability(&encrypted);
        assert!(
            result.is_err(),
            "tampered data should fail decryption (GCM auth tag)"
        );
    }

    #[test]
    fn test_decrypt_empty_data_fails() {
        let store = make_store();
        let result = store.decrypt_capability(&[]);
        assert!(result.is_err(), "empty data should fail");
    }

    #[test]
    fn test_proto_roundtrip_all_domains() {
        let registry = CapabilityRegistry::new();
        registry.bootstrap_builtin();

        // Test proto conversion for one capability from each domain
        for cap_id in &["data:kv:read", "coord:lock:acquire", "admin:auth:enable"] {
            let cap = registry.get(cap_id).expect("capability should exist");
            let proto = cap.to_proto();
            let restored = CapabilityDef::from_proto(&proto);
            assert_eq!(restored.id, cap.id);
            assert_eq!(restored.cap_type, cap.cap_type);
            assert_eq!(restored.domain, cap.domain);
        }
    }
}
