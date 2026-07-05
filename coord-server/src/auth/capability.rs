// Capability Registry — stores, manages, and bootstraps capability definitions
//
// Capabilities are stored in Server KV under /_system/capabilities/{id},
// encrypted with Barrier (DEK). See docs/capability-auth-implementation.md §5.
//
// This module provides:
// - In-memory capability store (backed by KV)
// - Bootstrap of all 77 built-in capabilities (Appendix A)
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
            .map_err(|e| tonic::Status::already_exists(e))?;

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

        Ok(tonic::Response::new(CapabilityListResponse { capabilities }))
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
            .map_err(|e| tonic::Status::not_found(e))?;

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

    /// Bootstrap all 77 built-in capabilities from Appendix A.
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

// ──── Capability Store (Phase 1.4: Barrier-encrypted persistence) ────

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
        let proto = coord_proto::capability::CapabilityDefinition::decode(
            plaintext.as_slice(),
        )
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

// ──── Built-in Capabilities (Appendix A — 77 capabilities) ────

fn builtin_capabilities() -> Vec<CapabilityDef> {
    let def = |id: &str, domain: &str, service: &str, action: &str, cap_type: CapabilityType, desc: &str| {
        CapabilityDef {
            id: id.to_string(),
            domain: domain.to_string(),
            service: service.to_string(),
            action: action.to_string(),
            description: desc.to_string(),
            cap_type,
            deprecated: false,
            version: 1,
            scope_description: String::new(),
        }
    };

    vec![
        // ──── 数据面 (15) ────
        def("data:kv:read",             "data", "kv",      "read",      CapabilityType::Read,  "按前缀/范围读取键值对"),
        def("data:kv:write",            "data", "kv",      "write",     CapabilityType::Write, "写入/更新键值对"),
        def("data:kv:delete",           "data", "kv",      "delete",    CapabilityType::Write, "按前缀/范围删除键值对"),
        def("data:txn:execute",         "data", "txn",     "execute",   CapabilityType::Write, "执行原子事务"),
        def("data:lease:grant",         "data", "lease",   "grant",     CapabilityType::Write, "创建租约"),
        def("data:lease:revoke",        "data", "lease",   "revoke",    CapabilityType::Write, "撤销租约"),
        def("data:lease:keepalive",     "data", "lease",   "keepalive", CapabilityType::Write, "租约心跳续期"),
        def("data:watch:subscribe",     "data", "watch",   "subscribe", CapabilityType::Read,  "订阅 Key 变更事件"),
        def("data:cache:read",          "data", "cache",   "read",      CapabilityType::Read,  "读取本地缓存"),
        def("data:cache:write",         "data", "cache",   "write",     CapabilityType::Write, "写入本地缓存"),
        def("data:cache:delete",        "data", "cache",   "delete",    CapabilityType::Write, "删除/清空本地缓存"),
        def("data:mq:send",             "data", "mq",      "send",      CapabilityType::Write, "发送消息"),
        def("data:mq:receive",          "data", "mq",      "receive",   CapabilityType::Read,  "消费消息"),
        def("data:mq:manage",           "data", "mq",      "manage",    CapabilityType::Write, "管理 Topic"),
        def("data:idgen:generate",      "data", "idgen",   "generate",  CapabilityType::Write, "生成全局唯一 ID"),

        // ──── 协调面 (37) ────
        def("coord:registry:register",  "coord", "registry", "register",  CapabilityType::Write, "注册服务实例"),
        def("coord:registry:deregister","coord", "registry", "deregister",CapabilityType::Write, "注销服务实例"),
        def("coord:registry:heartbeat", "coord", "registry", "heartbeat", CapabilityType::Write, "实例心跳上报"),
        def("coord:registry:discover",  "coord", "registry", "discover",  CapabilityType::Read,  "服务发现"),
        def("coord:lock:acquire",       "coord", "lock",    "acquire",    CapabilityType::Write, "获取分布式锁"),
        def("coord:lock:release",       "coord", "lock",    "release",    CapabilityType::Write, "释放分布式锁"),
        def("coord:lock:renew",         "coord", "lock",    "renew",      CapabilityType::Write, "续期分布式锁"),
        def("coord:lock:query",         "coord", "lock",    "query",      CapabilityType::Read,  "查询锁状态"),
        def("coord:workflow:define",    "coord", "workflow","define",     CapabilityType::Write, "定义工作流模板"),
        def("coord:workflow:start",     "coord", "workflow","start",      CapabilityType::Write, "启动工作流实例"),
        def("coord:workflow:signal",    "coord", "workflow","signal",     CapabilityType::Write, "发送信号/推进状态"),
        def("coord:workflow:cancel",    "coord", "workflow","cancel",     CapabilityType::Write, "取消工作流"),
        def("coord:workflow:query",     "coord", "workflow","query",      CapabilityType::Read,  "查询工作流状态"),
        def("coord:config:read",        "coord", "config",  "read",       CapabilityType::Read,  "读取配置"),
        def("coord:config:write",       "coord", "config",  "write",      CapabilityType::Write, "变更配置"),
        def("coord:config:rollback",    "coord", "config",  "rollback",   CapabilityType::Write, "回滚配置"),
        def("coord:leader:campaign",    "coord", "leader",  "campaign",   CapabilityType::Write, "参与 Leader 选举"),
        def("coord:leader:resign",      "coord", "leader",  "resign",     CapabilityType::Write, "放弃 Leader"),
        def("coord:leader:observe",     "coord", "leader",  "observe",    CapabilityType::Read,  "观察选举结果"),
        def("coord:event:publish",      "coord", "event",   "publish",    CapabilityType::Write, "发布事件"),
        def("coord:event:subscribe",    "coord", "event",   "subscribe",  CapabilityType::Read,  "订阅事件"),
        def("coord:policy:evaluate",    "coord", "policy",  "evaluate",   CapabilityType::Read,  "执行策略评估"),
        def("coord:policy:manage",      "coord", "policy",  "manage",     CapabilityType::Write, "管理策略规则"),
        def("coord:pki:issue",          "coord", "pki",     "issue",      CapabilityType::Write, "签发短期证书"),
        def("coord:pki:revoke",         "coord", "pki",     "revoke",     CapabilityType::Write, "吊销证书"),
        def("coord:pki:read",           "coord", "pki",     "read",       CapabilityType::Read,  "查询证书"),
        def("coord:transit:encrypt",    "coord", "transit", "encrypt",    CapabilityType::Write, "加密数据"),
        def("coord:transit:decrypt",    "coord", "transit", "decrypt",    CapabilityType::Read,  "解密数据"),
        def("coord:saga:execute",       "coord", "saga",    "execute",    CapabilityType::Write, "启动 Saga"),
        def("coord:saga:compensate",    "coord", "saga",    "compensate", CapabilityType::Write, "补偿/回滚"),
        def("coord:saga:query",         "coord", "saga",    "query",      CapabilityType::Read,  "查询 Saga 状态"),
        def("coord:resilience:circuit_breaker", "coord", "resilience", "circuit_breaker", CapabilityType::Write, "熔断开关"),
        def("coord:resilience:rate_limit",      "coord", "resilience", "rate_limit",      CapabilityType::Write, "限流规则管理"),
        def("coord:resilience:replication",     "coord", "resilience", "replication",     CapabilityType::Write, "副本同步控制"),
        def("coord:scheduler:schedule", "coord", "scheduler", "schedule",   CapabilityType::Write, "创建定时任务"),
        def("coord:scheduler:cancel",   "coord", "scheduler", "cancel",     CapabilityType::Write, "取消定时任务"),
        def("coord:scheduler:query",    "coord", "scheduler", "query",      CapabilityType::Read,  "查询任务状态"),

        // ──── 管控面 (25) ────
        def("admin:maintenance:status",         "admin", "maintenance", "status",          CapabilityType::Read,  "查询集群状态"),
        def("admin:maintenance:seal",           "admin", "maintenance", "seal",            CapabilityType::Admin, "封存集群"),
        def("admin:maintenance:unseal",         "admin", "maintenance", "unseal",          CapabilityType::Admin, "解封集群"),
        def("admin:maintenance:snapshot",       "admin", "maintenance", "snapshot",        CapabilityType::Read,  "导出快照"),
        def("admin:maintenance:member_add",     "admin", "maintenance", "member_add",      CapabilityType::Admin, "添加节点"),
        def("admin:maintenance:member_remove",  "admin", "maintenance", "member_remove",   CapabilityType::Admin, "移除节点"),
        def("admin:maintenance:member_promote", "admin", "maintenance", "member_promote",  CapabilityType::Admin, "晋升 Learner"),
        def("admin:maintenance:member_list",    "admin", "maintenance", "member_list",     CapabilityType::Read,  "列出节点"),
        def("admin:auth:enable",        "admin", "auth", "enable",         CapabilityType::Admin, "启用认证"),
        def("admin:auth:disable",       "admin", "auth", "disable",        CapabilityType::Admin, "禁用认证"),
        def("admin:auth:status",        "admin", "auth", "status",         CapabilityType::Read,  "查询认证状态"),
        def("admin:auth:user_add",      "admin", "auth", "user_add",       CapabilityType::Admin, "创建用户/AppRole"),
        def("admin:auth:user_delete",   "admin", "auth", "user_delete",    CapabilityType::Admin, "删除用户/AppRole"),
        def("admin:auth:user_list",     "admin", "auth", "user_list",      CapabilityType::Read,  "列出用户"),
        def("admin:auth:role_add",      "admin", "auth", "role_add",       CapabilityType::Admin, "创建角色"),
        def("admin:auth:role_delete",   "admin", "auth", "role_delete",    CapabilityType::Admin, "删除角色"),
        def("admin:auth:role_grant",    "admin", "auth", "role_grant",     CapabilityType::Admin, "为角色授权能力"),
        def("admin:auth:role_revoke",   "admin", "auth", "role_revoke",    CapabilityType::Admin, "撤销角色能力"),
        def("admin:auth:role_list",     "admin", "auth", "role_list",      CapabilityType::Read,  "列出角色"),
        def("admin:auth:user_grant_role",    "admin", "auth", "user_grant_role",   CapabilityType::Admin, "为用户分配角色"),
        def("admin:auth:user_revoke_role",   "admin", "auth", "user_revoke_role",  CapabilityType::Admin, "撤销用户角色"),
        def("admin:capability:register",     "admin", "capability", "register",     CapabilityType::Admin, "注册新能力定义"),
        def("admin:capability:deprecate",    "admin", "capability", "deprecate",    CapabilityType::Admin, "废弃能力"),
        def("admin:capability:list",         "admin", "capability", "list",         CapabilityType::Read,  "列出所有能力"),
        def("admin:capability:get",          "admin", "capability", "get",          CapabilityType::Read,  "查询能力详情"),
    ]
}

// ──── Tests ────

#[cfg(test)]
mod tests {
    use super::*;

    // ──── Phase 1.2: Capability Registry CRUD ────

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

        registry.register(cap.clone()).expect("register should succeed");
        let retrieved = registry.get("test:svc:action").expect("should find capability");
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

        registry.register(cap.clone()).expect("first register should succeed");
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
        registry.deprecate("dep:svc:action").expect("deprecate should succeed");
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

    // ──── Phase 1.3: Bootstrap built-in capabilities ────

    #[test]
    fn test_bootstrap_all_capabilities() {
        let registry = CapabilityRegistry::new();
        registry.bootstrap_builtin();
        assert_eq!(registry.count(), 77, "should have all 77 built-in capabilities (15 data + 37 coord + 25 admin)");
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

    // ──── Phase 1.4: Barrier-encrypted capability storage ────

    /// Helper: create a Barrier + CapabilityStore for testing.
    fn make_store() -> CapabilityStore {
        let (keyring, _encrypted_dek) = crate::security::key_management::Keyring::bootstrap();
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

        let encrypted = store.encrypt_capability(&cap).expect("encrypt should succeed");
        // Encrypted data should be different from plaintext
        let proto = cap.to_proto();
        let plain_bytes = proto.encode_to_vec();
        assert_ne!(encrypted, plain_bytes, "encrypted data must differ from plaintext");

        let decrypted = store.decrypt_capability(&encrypted).expect("decrypt should succeed");
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
        assert_eq!(all_caps.len(), 77);

        // Encrypt all 77 capabilities
        let encrypted_all = store.encrypt_all(&all_caps).expect("encrypt all should succeed");
        assert_eq!(encrypted_all.len(), 77);

        // Decrypt all
        let decrypted_all = store.decrypt_all(&encrypted_all).expect("decrypt all should succeed");
        assert_eq!(decrypted_all.len(), 77);

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

        let mut encrypted = store.encrypt_capability(&cap).expect("encrypt should succeed");

        // Tamper with the ciphertext
        if encrypted.len() > 20 {
            encrypted[20] ^= 0xFF;
        }

        let result = store.decrypt_capability(&encrypted);
        assert!(result.is_err(), "tampered data should fail decryption (GCM auth tag)");
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
