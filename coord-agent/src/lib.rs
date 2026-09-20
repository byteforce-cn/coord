// coord-agent: Agent 守护进程实现
//
// 部署在每台机器上，对本地应用暴露与 Server 完全相同的 gRPC 接口。
// Java 应用连接 localhost:19527 即可使用 Coord 全部能力。
//
// 公共导出：
// - AgentServer: Agent gRPC 服务端（含可插拔服务框架）
// - AgentConfig: Agent 配置结构体（含 ServiceConfig）
// - StaticDiscovery: 静态配置成员发现实现
// - run_agent(): 启动入口函数
// - service: 可插拔服务框架（BaseService trait + ServiceManager）
// - services: 高级基础服务（Registry、Workflow 等）
//
// 参见。

pub mod auth;
pub mod cache;
pub mod config_watcher;
mod discovery;
pub mod feature_flags;
pub mod feature_flags_store;
pub mod health;
pub mod key_util;
pub mod metrics;
pub mod pki;
pub mod pki_store;
pub mod plugin;
mod proxy;
pub mod service;
pub mod services;
pub mod threadpool;
pub mod tls;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use coord_proto::kv::kv_server::KvServer;
use coord_proto::lease::lease_server::LeaseServer;
use coord_proto::maintenance::maintenance_server::MaintenanceServer;
use coord_proto::storage::storage_server::StorageServer;
use coord_proto::txn::txn_server::TxnServer;
use coord_proto::watch::watch_server::WatchServer;

// 重新导出公共类型
pub use discovery::StaticDiscovery;
// `AgentInner`（到 Server 集群的 Direct 模式客户端句柄）是各代理服务共享的构造入口，
// 之前被 `mod proxy;`（私有模块）挡住，导致**任何集成测试都无法构造一个
// LeaderElectionService / LockService 去验证真实语义**（第三轮复核：这两个服务
// 分别"零功能测试"与"零故障注入测试"）。按其既有 `pub struct` + `pub fn new`
// 的意图对外导出。
pub use key_util::{
    FileKeyStore, KeyStore, KeyStoreBackend, KeyStoreError, KeyUtil, KeyUtilConfig,
};
pub use pki::{CertInfo, PkiConfig, PkiError, PkiService};
pub use pki_store::{
    CaRecord, CertRecord, CertStatus, KvPkiStore, MemoryPkiStore, PkiStore, PkiStoreError,
};
pub use proxy::AgentInner;
pub use service::{BaseService, ServiceConfig, ServiceResult};
pub use services::transit_store::{
    DekRecord, DekStoreError, KvTransitDekStore, MemoryTransitDekStore, TransitDekStore,
};
pub use threadpool::{AgentThreadPools, ThreadPoolConfig};
pub use tls::{build_agent_tls_channel, build_agent_tls_server_config, AgentTlsConfig};

// ──── DiscoveryMode ────

/// 成员发现模式
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryMode {
    /// 静态配置（从配置文件/命令行读取 Server 列表）
    #[default]
    Static,
    /// [未来] SWIM Gossip 协议
    #[allow(dead_code)]
    Gossip,
}

// ──── AgentConfig ────

/// Agent 配置结构体
///
/// 可通过命令行参数或 TOML 配置文件加载。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct AgentConfig {
    /// Agent 本地 gRPC 监听地址（默认 127.0.0.1:19527）
    #[serde(default = "default_agent_addr")]
    pub agent_addr: String,
    /// HTTP 可观测性监听地址（默认 127.0.0.1:19528）
    #[serde(default = "default_http_addr")]
    pub http_addr: String,
    /// 数据目录路径
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
    /// 成员发现模式
    #[serde(default)]
    pub discovery_mode: DiscoveryMode,
    /// 静态配置的 Server 节点列表（discovery_mode = "static" 时使用）
    #[serde(default)]
    pub static_peers: Vec<String>,

    // 缓存参数
    /// KV 读缓存最大条目数（默认 10000）
    #[serde(default = "default_cache_kv_max_entries")]
    pub cache_kv_max_entries: usize,
    /// KV 读缓存 TTL（秒，默认 0 = **关闭**本地 KV 读缓存）
    ///
    /// 第四轮 §3.15(7)：此处此前写"默认 30"，而 `default_cache_kv_ttl_secs()`
    /// 返回 0（其自身注释也写明"0 = 关闭"）。doc 与实现不一致会让读者以为缓存默认开着。
    #[serde(default = "default_cache_kv_ttl_secs")]
    pub cache_kv_ttl_secs: u64,
    /// [已废弃] Service Catalog 缓存 TTL（秒，默认 10）
    /// 请使用 services.registry 配置替代
    #[serde(default = "default_cache_catalog_ttl_secs")]
    pub cache_catalog_ttl_secs: u64,
    /// Route Table 缓存 TTL（秒，默认 60）
    #[serde(default = "default_cache_route_ttl_secs")]
    pub cache_route_ttl_secs: u64,

    // 代理参数
    /// 最大重试次数（默认 3）
    #[serde(default = "default_proxy_max_retries")]
    pub proxy_max_retries: u32,
    /// 请求超时（秒，默认 5）
    #[serde(default = "default_proxy_request_timeout_secs")]
    pub proxy_request_timeout_secs: u64,

    // 可插拔服务配置
    /// 高级基础服务启用配置（v3.0 可插拔服务框架）
    #[serde(default)]
    pub services: ServiceConfig,

    // ISR 复制配置（services.replication = true 时生效）
    /// 跨 Agent 数据复制配置（min_isr / sync_timeout_ms）
    #[serde(default)]
    pub replication: crate::services::replication::ReplicationConfig,
    /// ISR 复制对端 agent 地址列表（services.replication = true 时使用；
    /// 首版静态成员，Registry 发现为演进路径，Q1）
    #[serde(default)]
    pub replication_peers: Vec<String>,

    // TLS/mTLS 传输安全
    /// TLS 证书配置（None = 禁用 TLS，仅用于开发环境）
    #[serde(default)]
    pub tls: Option<AgentTlsConfig>,

    // 线程池资源隔离
    /// 线程池配置
    #[serde(default)]
    pub thread_pools: ThreadPoolConfig,

    // Agent 侧 CCT 鉴权
    /// 鉴权配置（默认关闭；开启后所有 gRPC RPC 校验 CCT + capability）
    #[serde(default)]
    pub auth: AgentAuthConfig,

    // 插件引擎（混合引擎：rquickjs + wasm；原生服务经适配器统一 SPI）
    /// 插件引擎配置（默认关闭 = 零开销）
    #[serde(default)]
    pub plugins: crate::plugin::PluginEngineConfig,
}

/// Agent 侧 CCT 鉴权配置（私钥集中存储前必须上鉴权）
#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct AgentAuthConfig {
    /// 是否启用鉴权（默认 false；启用后所有 gRPC RPC 均校验 CCT）
    #[serde(default)]
    pub enabled: bool,
    /// CCT HMAC 签名密钥（hex 编码；历史对称方案，宽限期兼容验证用，
    /// 之后新签发全部为 Ed25519，此字段仅用于存量 token 验证）
    #[serde(default)]
    pub signing_key_hex: String,
    /// CCT Ed25519 验证公钥（hex 编码 64 字符 = 32 字节）。
    /// server 持私钥签发，agent 仅存公钥验证，任一 agent 被控无法伪造 token。
    #[serde(default)]
    pub verifying_key_hex: String,
    /// 时钟漂移容忍（秒，默认 300）
    #[serde(default = "default_auth_clock_drift_secs")]
    pub clock_drift_secs: i64,
    /// 一次性 bootstrap token（首启开通 agent 自身与**插件服务账户**；空 = 跳过）。
    /// 仅在 `enabled = true` 时生效；插件账户开通失败会降级为共享未鉴权客户端。
    #[serde(default)]
    pub bootstrap_token: String,
    /// provisioner 服务账户用户名（批次 12）。
    ///
    /// 首启以一次性引导 CCT 自举该**持久**账户（密码落盘在 `data_dir`），
    /// 之后 agent 用它（refresh token / 密码重认证**自动续期**）开通插件账户，
    /// 因此运行中（SIGHUP）新增插件不再受「引导 CCT 10 分钟 + 令牌一次性」限制。
    ///
    /// 多个 agent 共用一个集群时必须各自取**唯一**名字：同名的第二个 agent
    /// 无法用自己随机生成的密码通过认证，会退化为旧行为（仅窗口内可开通）。
    #[serde(default = "default_provisioner_user")]
    pub provisioner_user: String,
}

fn default_provisioner_user() -> String {
    crate::plugin::identity::DEFAULT_PROVISIONER_USER.to_string()
}

fn default_auth_clock_drift_secs() -> i64 {
    300
}

// Serde default functions
fn default_agent_addr() -> String {
    "127.0.0.1:19527".into()
}
fn default_http_addr() -> String {
    "127.0.0.1:19528".into()
}
fn default_data_dir() -> String {
    "/var/lib/coord-agent".into()
}
fn default_cache_kv_max_entries() -> usize {
    10000
}
fn default_cache_kv_ttl_secs() -> u64 {
    // B1：默认 **0 = 关闭本地 KV 读缓存**。
    //
    // 开启后本地缓存只能返回 value，无法提供真实的 MVCC 元数据，且跨 agent 写
    // 之后、本地 TTL 到期之前会读到旧值（并伪造 revision）。默认关闭即默认正确，
    // 需要串行化读性能的场景可由运维显式开启并承担该窗口。
    0
}

/// A3：鉴权密钥材料校验（启动期 fail-closed）。
///
/// `auth.enabled = true` 时要求：
/// 1. 至少一种**真实**密钥材料：`verifying_key_hex`（Ed25519 公钥，推荐；服务端持
///    私钥签发，agent 只验签）或 `signing_key_hex`（历史 HMAC 对称密钥）；
/// 2. `signing_key_hex` 非空时必须是合法 hex 且 **>= 32 字节**
///    （[`MIN_HMAC_KEY_LEN`]）；
/// 3. `verifying_key_hex` 非空时必须是 32 字节 Ed25519 公钥；
/// 4. `bootstrap_token` 非空——否则本机 RoleCache **永远无法同步**，所有
///    角色门控 RPC 都会被拒（"启动了但全 403" 是比拒绝启动更糟的故障形态）。
///
/// 为什么第 2 条是 P0：`hex::decode("")` 得到 `Ok(vec![])`，而 `hmac` 对空 key 返回
/// `Ok` —— 即**空 HMAC 密钥**；任何知道"密钥为空"的调用方都能自签
/// `roles:["root"]` 的 CCT。旧实现只在"两者都为空"时拒绝，而推荐配置（只设公钥）
/// 恰好留下 `signing_key_hex = ""`，漏洞仍然成立。
fn validate_auth_key_material(auth: &AgentAuthConfig) -> Result<(), String> {
    if !auth.enabled {
        return Ok(());
    }
    let signing = auth.signing_key_hex.trim();
    let verifying = auth.verifying_key_hex.trim();

    if signing.is_empty() && verifying.is_empty() {
        return Err(
            "auth.enabled=true but neither auth.signing_key_hex nor auth.verifying_key_hex \
             is configured: refusing to start with empty key material (A3 fail-closed)"
                .to_string(),
        );
    }

    if !signing.is_empty() {
        let raw = hex::decode(signing)
            .map_err(|e| format!("auth.signing_key_hex is not valid hex: {e}"))?;
        if raw.len() < coord_core::auth::cct::MIN_HMAC_KEY_LEN {
            return Err(format!(
                "auth.signing_key_hex decodes to {} bytes but needs >= {} bytes \
                 ({} hex chars): an empty/short HMAC key lets anyone forge a \
                 roles:[\"root\"] CCT (A3 fail-closed)",
                raw.len(),
                coord_core::auth::cct::MIN_HMAC_KEY_LEN,
                coord_core::auth::cct::MIN_HMAC_KEY_LEN * 2
            ));
        }
    }

    if !verifying.is_empty() {
        let raw = hex::decode(verifying)
            .map_err(|e| format!("auth.verifying_key_hex is not valid hex: {e}"))?;
        if raw.len() != 32 {
            return Err(format!(
                "auth.verifying_key_hex must be 64 hex chars (32-byte Ed25519 public key); \
                 got {} bytes (A3 fail-closed)",
                raw.len()
            ));
        }
    }

    // A3（连带项）：没有出站凭据 → RoleCache 同步永不成功 → 全部 RPC 被拒。
    if auth.bootstrap_token.trim().is_empty() {
        return Err(
            "auth.enabled=true but auth.bootstrap_token is empty: the agent has no outbound \
             credential to sync its RoleCache from the server, so every role-gated RPC would \
             be denied. Configure [auth].bootstrap_token (server side: \
             [security].agent_bootstrap_tokens) (A3 fail-closed)"
                .to_string(),
        );
    }

    Ok(())
}

#[cfg(test)]
mod auth_key_material_tests {
    use super::*;

    #[test]
    fn disabled_auth_needs_no_key_material() {
        let auth = AgentAuthConfig::default();
        assert!(!auth.enabled);
        assert!(validate_auth_key_material(&auth).is_ok());
    }

    #[test]
    fn enabled_auth_without_keys_is_rejected() {
        let auth = AgentAuthConfig {
            enabled: true,
            ..AgentAuthConfig::default()
        };
        let err = validate_auth_key_material(&auth).expect_err("must fail-closed");
        assert!(err.contains("A3 fail-closed"), "err: {err}");
    }

    #[test]
    fn enabled_auth_with_verifying_key_is_ok() {
        let auth = AgentAuthConfig {
            enabled: true,
            verifying_key_hex: "ab".repeat(32),
            bootstrap_token: "bootstrap-token-for-tests".to_string(),
            ..AgentAuthConfig::default()
        };
        assert!(validate_auth_key_material(&auth).is_ok());
    }

    #[test]
    fn enabled_auth_with_legacy_signing_key_is_ok() {
        let auth = AgentAuthConfig {
            enabled: true,
            signing_key_hex: "cd".repeat(32),
            bootstrap_token: "bootstrap-token-for-tests".to_string(),
            ..AgentAuthConfig::default()
        };
        assert!(validate_auth_key_material(&auth).is_ok());
    }

    #[test]
    fn whitespace_only_key_material_is_rejected() {
        let auth = AgentAuthConfig {
            enabled: true,
            signing_key_hex: "   ".to_string(),
            verifying_key_hex: "\t".to_string(),
            bootstrap_token: "bootstrap-token-for-tests".to_string(),
            ..AgentAuthConfig::default()
        };
        assert!(validate_auth_key_material(&auth).is_err());
    }

    /// A3 回归固化：只配公钥（推荐配置）+ `signing_key_hex = ""` 曾使空 HMAC
    /// 密钥生效——现在必须被拒绝，且不得因为"公钥已配置"而放行。
    #[test]
    fn empty_signing_key_alongside_verifying_key_is_rejected() {
        let auth = AgentAuthConfig {
            enabled: true,
            signing_key_hex: String::new(),
            verifying_key_hex: "ab".repeat(32),
            bootstrap_token: "bootstrap-token-for-tests".to_string(),
            ..AgentAuthConfig::default()
        };
        // 密钥材料这一层允许（只配公钥是推荐配置）……
        assert!(validate_auth_key_material(&auth).is_ok());
        // ……但空 HMAC 密钥绝不能被当作可用密钥：由 cct 层拒绝
        // （端到端行为见 coord-core cct 的单测与 agent 鉴权集成测试）。
        assert!(!coord_core::auth::cct::is_usable_hmac_key(&[]));
    }

    #[test]
    fn short_signing_key_is_rejected() {
        let auth = AgentAuthConfig {
            enabled: true,
            signing_key_hex: "cd".repeat(8), // 8 字节
            bootstrap_token: "bootstrap-token-for-tests".to_string(),
            ..AgentAuthConfig::default()
        };
        let err = validate_auth_key_material(&auth).expect_err("short HMAC key must fail closed");
        assert!(err.contains("A3 fail-closed"), "err: {err}");
    }

    #[test]
    fn non_hex_signing_key_is_rejected() {
        let auth = AgentAuthConfig {
            enabled: true,
            signing_key_hex: "zz".repeat(32),
            bootstrap_token: "bootstrap-token-for-tests".to_string(),
            ..AgentAuthConfig::default()
        };
        assert!(validate_auth_key_material(&auth).is_err());
    }

    #[test]
    fn enabled_auth_without_bootstrap_token_is_rejected() {
        // 无法同步 RoleCache 的"开鉴权"配置只会全量 403，属配置错误 → 启动即失败。
        let auth = AgentAuthConfig {
            enabled: true,
            verifying_key_hex: "ab".repeat(32),
            bootstrap_token: "   ".to_string(),
            ..AgentAuthConfig::default()
        };
        let err = validate_auth_key_material(&auth).expect_err("missing bootstrap token must fail");
        assert!(err.contains("bootstrap_token"), "err: {err}");
    }
}
fn default_cache_catalog_ttl_secs() -> u64 {
    10
}
fn default_cache_route_ttl_secs() -> u64 {
    60
}
fn default_proxy_max_retries() -> u32 {
    3
}
fn default_proxy_request_timeout_secs() -> u64 {
    5
}

impl AgentConfig {
    /// 从 TOML 文件加载配置
    pub fn from_file(
        path: &std::path::Path,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&content)?;
        Ok(config)
    }
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            agent_addr: "127.0.0.1:19527".into(),
            http_addr: "127.0.0.1:19528".into(),
            data_dir: "/var/lib/coord-agent".into(),
            discovery_mode: DiscoveryMode::Static,
            static_peers: Vec::new(),
            cache_kv_max_entries: 10000,
            // B1：默认 **0 = 关闭本地 KV 读缓存**（与 `default_cache_kv_ttl_secs()`
            // 保持一致）。此前这里写死 30，导致"不带 --agent-config 启动"时缓存
            // 仍然是开的 —— 与同一次整改里把默认值改成 0 的口径自相矛盾，等于
            // 「默认关闭」只在配置文件路径上成立。
            cache_kv_ttl_secs: default_cache_kv_ttl_secs(),
            cache_catalog_ttl_secs: 10,
            cache_route_ttl_secs: 60,
            proxy_max_retries: 3,
            proxy_request_timeout_secs: 5,
            services: ServiceConfig::default(),
            replication: crate::services::replication::ReplicationConfig::default(),
            replication_peers: Vec::new(),
            tls: None,
            thread_pools: ThreadPoolConfig::default(),
            auth: AgentAuthConfig::default(),
            plugins: crate::plugin::PluginEngineConfig::default(),
        }
    }
}

// ──── gRPC 服务链组装（泛型于 Server<L>：明文 Identity / TLS TlsAcceptor 共用）────

/// agent gRPC 服务句柄集合（核心代理面 + 插件引擎自身的调用面）。
///
/// 原生服务（Registry / Config / Lock / IdGen / Workflow ...）**不在**这里：
/// 它们已作为内建插件注册进 `PluginManager`，服务链由
/// [`crate::plugin::PluginManager::build_grpc_router`] 依 `Plugin::grpc_service()`
/// 动态组装——服务「存在与否」由插件注册表给出，「是否对外」由插件状态给出，
/// 不再有第二份硬编码清单。
struct AgentGrpcSvcs {
    inner: Option<Arc<crate::proxy::AgentInner>>,
    /// 通用插件调用面（coord.plugin.Plugin；Invoke/List）
    ///
    /// 刻意**不**做成插件：它是插件引擎自身的管理面，做成插件会与
    /// `PluginManager` 构成 Arc 环（manager → plugin → manager）。
    plugin: Option<Arc<crate::plugin::PluginService>>,
}

/// 把一个原生服务登记为**内建插件**（插件拥有其生命周期与 gRPC 服务面）。
///
/// 返回服务句柄（注册失败 → `None`，与旧 `ServiceManager::register` 的
/// 错误语义一致：装配期其他逻辑不应继续引用未注册的服务）。
///
/// 这是原生服务进入插件 SPI 的**唯一**入口：不再有「ServiceManager 一份 +
/// 硬编码路由链一份」的双清单。
async fn register_native_service<T>(
    manager: &Arc<crate::plugin::PluginManager>,
    service: Arc<T>,
    grpc: crate::plugin::AgentGrpcService,
) -> Option<Arc<T>>
where
    T: crate::service::BaseService + 'static,
{
    let name = service.name().to_string();
    let handle = Arc::clone(&service);
    let adapter = Arc::new(
        crate::plugin::NativePluginAdapter::new(service as Arc<dyn crate::service::BaseService>)
            .with_grpc(grpc),
    );
    match manager.register_builtin(adapter).await {
        Ok(()) => {
            tracing::info!("service '{name}' registered as builtin plugin");
            Some(handle)
        }
        Err(e) => {
            tracing::error!("failed to register service '{name}' as plugin: {e}");
            None
        }
    }
}

/// 组装 agent gRPC 服务链（核心 6 代理服务 + 插件动态服务面 + 自定义 Health）。
///
/// 泛型于 `Server<L>`（`L` = 明文 `Identity` 或 TLS `TlsAcceptor`）：调用方分别在
/// `.tls_config()` 前后构造 builder，服务链单份组装，避免两条 serve 路径漂移。
fn build_agent_grpc_router<L>(
    mut server: tonic::transport::server::Server<L>,
    svcs: AgentGrpcSvcs,
    metrics: Option<crate::metrics::AgentMetrics>,
    plugin_manager: &Arc<crate::plugin::PluginManager>,
) -> Result<tonic::transport::server::Router<L>, Box<dyn std::error::Error + Send + Sync>>
where
    L: Clone,
{
    let router = server
        .add_service(KvServer::new(crate::proxy::KvProxy::new(
            svcs.inner.clone(),
        )))
        .add_service(TxnServer::new(crate::proxy::TxnProxy::new(
            svcs.inner.clone(),
        )))
        .add_service(LeaseServer::new(crate::proxy::LeaseProxy::new(
            svcs.inner.clone(),
        )))
        .add_service(WatchServer::new(
            crate::proxy::WatchProxy::new(svcs.inner.clone()).with_metrics(metrics),
        ))
        .add_service(MaintenanceServer::new(crate::proxy::MaintenanceProxy::new(
            svcs.inner.clone(),
        )))
        .add_service(StorageServer::new(crate::proxy::StorageProxy::new(
            svcs.inner,
        )))
        .add_optional_service(
            svcs.plugin
                .map(coord_proto::plugin::plugin_server::PluginServer::from_arc),
        );

    // 原生服务 + 脚本插件：服务面由插件注册表动态给出（仅**已启动**的插件会被挂载，
    // 启动失败的插件不会暴露半死的 gRPC 接口）。
    let router = plugin_manager.build_grpc_router(router);

    // 注册自定义 Health gRPC 服务（coord.agent.Health）
    // Java SDK healthCheck() 调用的是此自定义服务（而非标准 grpc.health.v1.Health）。
    // 此前未注册 → UNIMPLEMENTED → NOT_SERVING 误报（注册/ID 生成等服务实际可用）。
    // 修复：注册并返回 SERVING（存活语义，与 HTTP /health 一致）；健康状态以 /api/v1/health 为准。
    // 已反馈 jinhe-starter/coord 团队（见 .github/pr/）。
    let router = router.add_service(coord_proto::agent::health_server::HealthServer::new(
        crate::health::GrpcHealthService,
    ));

    Ok(router)
}

/// `build_plugin_loader` 的入参集合（避免参数过多；均为借用/轻量克隆）。
///
/// 结构体本身不按 feature 门控（无插件 feature 时由调用方构造、函数返回 `None`），
/// 因此字段在无 feature 构建下也需被读取以避免 dead_code。
struct PluginLoaderArgs<'a> {
    cfg: &'a crate::plugin::PluginEngineConfig,
    auth: &'a AgentAuthConfig,
    data_dir: &'a str,
    tls: Option<coord_client::config::TlsConfig>,
    static_peers: &'a [String],
    /// 共享（未鉴权）回退客户端；`None` = skeleton 模式
    fallback: Option<&'a coord_client::Client>,
    /// 插件身份开通用的凭据句柄（进程级：一次性 bootstrap token 只兑换一次，
    /// 由 `serve` 持有并在 SIGHUP 重建加载器时复用同一引导 CCT）
    identity_provider: &'a Arc<coord_client::credential::CachedTokenProvider>,
    metrics: Option<crate::metrics::AgentMetrics>,
}

/// 构建插件加载器（Phase 1：rquickjs JS 宿主；Phase 4：wasmtime wasm 宿主）。
///
/// - skeleton 模式（无 coord-client 后端）→ `None`（配置条目在 `reload` 中记入
///   `deferred`，插件不会带缺失的 SDK 启动）；
/// - 命中 `plugin-js` / `plugin-wasm` feature → `EnginePluginLoader` 按
///   `manifest.runtime` 派发到对应引擎（后端为 `CoordSdkBackend`）；
/// - 配置了 `auth.bootstrap_token` → 先用一次性令牌换短期引导 CCT，
///   再挂载 `PluginIdentityManager`（每插件独立服务账户 + 受限 CCT 自动续期，
///   D5/§10.1；JS / wasm 两个引擎一致）。
/// - 传入 `metrics` → 插件调用结果与沙箱 trap（fuel/epoch）计数进入 `/metrics`。
async fn build_plugin_loader(
    args: PluginLoaderArgs<'_>,
) -> Option<Arc<dyn crate::plugin::PluginLoader>> {
    #[cfg(not(any(feature = "plugin-js", feature = "plugin-wasm")))]
    {
        // 无插件引擎 feature：读一遍字段（避免 dead_code）后返回 None。
        let _ = (
            args.cfg,
            args.auth,
            args.data_dir,
            args.tls,
            args.static_peers,
            args.fallback,
            args.identity_provider,
            args.metrics,
        );
        None
    }

    #[cfg(any(feature = "plugin-js", feature = "plugin-wasm"))]
    {
        let PluginLoaderArgs {
            cfg,
            auth,
            data_dir,
            tls,
            static_peers,
            fallback,
            identity_provider,
            metrics,
        } = args;
        use crate::plugin::sdk::{CoordSdkBackend, PluginSdkBackend};
        let fallback = fallback?;
        let rc = tokio::runtime::Handle::current();

        // 插件身份（可选）：配置了 `auth.bootstrap_token` 即视为运维显式开启
        //（令牌来自 server 的 `[security].agent_bootstrap_tokens` 或动态签发）。
        // **不**依赖 agent 自身的入站鉴权开关（`auth.enabled`）：两者正交——
        // agent 是否校验入站 CCT，与插件是否用独立服务账户访问 server 无关。
        //
        // 引导 CCT 只注入**独立**的插件身份客户端，不污染共享 `inner.client`
        //（代理数据面流量不应携带引导凭据）。
        //
        // 引导是 **best-effort**：一次性令牌可能已被上一次启动消费，此时无法再建新
        // 账户，但插件账户密码已持久化在 agent data_dir，`PluginIdentityManager`
        // 仍能直接 `Authenticate` 既有账户并续期 —— 因此这里**始终**挂载身份管理器
        // （退化情形下用共享客户端作 gateway）。
        if !auth.bootstrap_token.trim().is_empty() {
            use crate::plugin::identity::{CoordAuthGateway, PluginClients, PluginIdentityManager};
            let (identity_client, bootstrap_ready) = match ensure_plugin_identity_token(
                auth,
                static_peers,
                tls.clone(),
                identity_provider,
            )
            .await
            {
                Ok(Some(c)) => (c, true),
                Ok(None) => (fallback.clone(), false),
                Err(e) => {
                    tracing::warn!(
                        "plugin identity: bootstrap CCT unavailable ({e}); existing plugin \
                             accounts will be authenticated with persisted passwords, but no new \
                             account can be provisioned until a fresh bootstrap token is provided"
                    );
                    (fallback.clone(), false)
                }
            };
            let clients = Arc::new(PluginClients::new(fallback.clone()));
            let gateway: Arc<dyn crate::plugin::identity::PluginAuthGateway> =
                Arc::new(CoordAuthGateway::new(identity_client.clone()));
            match PluginIdentityManager::new(
                gateway,
                static_peers.to_vec(),
                tls,
                data_dir,
                Arc::clone(&clients),
            ) {
                Ok(mgr) => {
                    let mgr = Arc::new(mgr);
                    // 批次 12：自举**持久** provisioner 服务账户 —— 开通能力不再受
                    // 「引导 CCT 10 分钟 + 令牌一次性」限制（运行中新增插件可用）。
                    // 引导 CCT 仅在**本次**成功兑换时可用于自举；令牌已消费 / 已过期时
                    // 靠已落盘密码匿名认证 provisioner 账户（重启后仍成立）。
                    let provisioner_user = {
                        let configured = auth.provisioner_user.trim();
                        if configured.is_empty() {
                            crate::plugin::identity::DEFAULT_PROVISIONER_USER.to_string()
                        } else {
                            configured.to_string()
                        }
                    };
                    if let Err(e) = mgr
                        .bootstrap_provisioner(
                            bootstrap_ready.then_some(&identity_client),
                            &provisioner_user,
                        )
                        .await
                    {
                        tracing::warn!(
                            "plugin identity: provisioner session unavailable ({e}); account \
                             provisioning stays limited to the bootstrap CCT window"
                        );
                    }
                    let backend: Arc<dyn PluginSdkBackend> =
                        Arc::new(CoordSdkBackend::with_source(mgr.client_source()));
                    return crate::plugin::EnginePluginLoader::new(
                        std::path::PathBuf::from(&cfg.dir),
                        backend,
                        cfg.env.clone(),
                        rc,
                    )
                    .map(|l| l.with_identity(mgr))
                    .map(|l| match metrics.clone() {
                        Some(m) => l.with_metrics(m),
                        None => l,
                    })
                    .map(|l| Arc::new(l) as Arc<dyn crate::plugin::PluginLoader>)
                    .map_err(|e| tracing::error!("plugin engine init failed: {e}"))
                    .ok();
                }
                Err(e) => tracing::warn!(
                    "plugin identity disabled (manager init failed: {e}); plugins use \
                     the shared agent client"
                ),
            }
        }

        let backend: Arc<dyn PluginSdkBackend> = Arc::new(CoordSdkBackend::new(fallback.clone()));
        match crate::plugin::EnginePluginLoader::new(
            std::path::PathBuf::from(&cfg.dir),
            backend,
            cfg.env.clone(),
            rc,
        ) {
            Ok(l) => {
                let l = match metrics {
                    Some(m) => l.with_metrics(m),
                    None => l,
                };
                Some(Arc::new(l))
            }
            Err(e) => {
                tracing::error!("plugin engine init failed: {e}; plugin entries deferred");
                None
            }
        }
    }
}

/// 换取（或复用）agent 引导 CCT，返回带凭据的**出站客户端**。
///
/// - 已有缓存 CCT（`identity_provider` 已 set：重启 / SIGHUP 重建加载器 / 角色同步
///   先于插件加载器兑换）→ 直接复用，**不**再次兑换（一次性令牌只能消费一次）；
/// - 首次 → `Auth.Bootstrap(token)` 换取短期 `agent-bootstrap` CCT 写入句柄。
///
/// 失败原因（无静态 peers / 令牌被拒）以 `Err` 返回，由调用方降级。
///
/// 该函数**不再**受 `plugin-js/wasm` feature 门控：角色同步同样需要出站凭据，
/// 否则没有插件 feature 的构建在 `auth.enabled=true` 时永远无法同步角色。
async fn ensure_plugin_identity_token(
    auth: &AgentAuthConfig,
    static_peers: &[String],
    tls: Option<coord_client::config::TlsConfig>,
    identity_provider: &Arc<coord_client::credential::CachedTokenProvider>,
) -> Result<Option<coord_client::Client>, String> {
    // 已缓存（前一次启动/SIGHUP 已兑换）→ 复用
    if identity_provider.is_set() {
        return build_identity_client(static_peers, tls, identity_provider)
            .await
            .map(Some);
    }
    if static_peers.is_empty() {
        return Err("no static server endpoints configured".into());
    }

    let client = build_identity_client(static_peers, tls, identity_provider).await?;
    let resp = client
        .auth()
        .bootstrap(&auth.bootstrap_token)
        .await
        .map_err(|e| format!("Auth.Bootstrap rejected the token: {e}"))?;
    if resp.cct.is_empty() {
        return Err("Auth.Bootstrap returned an empty CCT".into());
    }
    tracing::info!(
        "agent bootstrap succeeded (plugin identity provisioning enabled; CCT expires at {})",
        resp.expires_at
    );
    identity_provider.set(resp.cct);
    Ok(Some(client))
}

/// 构造使用给定凭据句柄的客户端（与共享客户端隔离）。
async fn build_identity_client(
    static_peers: &[String],
    tls: Option<coord_client::config::TlsConfig>,
    identity_provider: &Arc<coord_client::credential::CachedTokenProvider>,
) -> Result<coord_client::Client, String> {
    let mut config = coord_client::Config::new(static_peers.to_vec())
        .with_token_provider(Arc::clone(identity_provider) as Arc<dyn coord_client::TokenProvider>);
    if let Some(t) = tls {
        config = config.with_tls(t);
    }
    coord_client::Client::connect_direct(config)
        .await
        .map_err(|e| format!("failed to build plugin identity client: {e}"))
}

/// 判断地址主机段是否 loopback（支持 `127.0.0.1:port` / `localhost:port` /
/// `[::1]:port` / 裸地址；未知主机名按非 loopback 处理，fail-closed）。
fn is_loopback_host(addr: &str) -> bool {
    let host = match addr.rsplit_once(':') {
        Some((host, _)) if host.contains(':') => host.trim_start_matches('[').trim_end_matches(']'),
        Some((host, _)) => host,
        None => addr,
    };
    host == "localhost"
        || host
            .parse::<std::net::IpAddr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or(false)
}

// ──── AgentServer ────
/// Agent gRPC 服务端
///
/// 注册核心代理服务（KV/Txn/Lease/Watch/Maintenance）。
/// 根据 `ServiceConfig` 按需加载可插拔高级服务（Registry、Workflow 等）。
/// 内部包含请求代理层、本地缓存层、Server 连接管理。
#[derive(Debug)]
pub struct AgentServer {
    config: AgentConfig,
    /// R-AGT-20：资源隔离线程池（可选；背景任务经 background 池 spawn）
    thread_pools: Option<Arc<crate::threadpool::AgentThreadPools>>,
    /// R-AGT-20：指标注册表（gRPC 计数中间件 + 连接状态）
    metrics: Option<crate::metrics::AgentMetrics>,
    /// R-AGT-20：就绪标志（连接探针实时回写，供 /health?ready=true）
    ready_flag: Option<Arc<std::sync::atomic::AtomicBool>>,
    /// 配置文件监视器（Some = 支持 SIGHUP 触发的插件集热重载）
    config_watcher: Option<crate::config_watcher::ConfigWatcher>,
}

impl AgentServer {
    /// 创建新的 AgentServer 实例
    pub fn new(config: AgentConfig) -> Self {
        Self {
            config,
            thread_pools: None,
            metrics: None,
            ready_flag: None,
            config_watcher: None,
        }
    }

    /// R-AGT-20：挂载资源隔离线程池。
    pub fn with_thread_pools(mut self, pools: Arc<crate::threadpool::AgentThreadPools>) -> Self {
        self.thread_pools = Some(pools);
        self
    }

    /// R-AGT-20：挂载指标注册表。
    pub fn with_metrics(mut self, metrics: crate::metrics::AgentMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// R-AGT-20：挂载共享就绪标志（连接探针实时回写）。
    pub fn with_ready_flag(mut self, flag: Arc<std::sync::atomic::AtomicBool>) -> Self {
        self.ready_flag = Some(flag);
        self
    }

    /// 挂载配置文件监视器：SIGHUP 时重载配置并应用**插件集 diff**
    /// （新增/移除/版本替换；引擎参数与默认限制等结构性变更需重启）。
    pub fn with_config_watcher(mut self, watcher: crate::config_watcher::ConfigWatcher) -> Self {
        self.config_watcher = Some(watcher);
        self
    }

    /// 获取配置引用
    pub fn config(&self) -> &AgentConfig {
        &self.config
    }

    /// 启动 Agent gRPC server，阻塞直到 shutdown 或错误。
    ///
    /// 注册全部核心 gRPC 服务（KV/Txn/Lease/Watch/Maintenance），
    /// 以及根据 `config.services` 启用的可插拔服务。
    /// 监听 `config.agent_addr`。
    ///
    /// 若配置了 `static_peers`，自动创建到 Server 集群的 Direct 模式连接，
    /// 并将所有请求代理转发到真实 Server。
    ///
    /// # Errors
    /// 返回绑定失败或 server 运行时错误。
    pub async fn serve(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.serve_with_shutdown(std::future::pending::<()>()).await
    }

    /// 启动 Agent gRPC server，支持外部关闭信号。
    ///
    /// 与 `serve()` 相同，但当 `shutdown` future 就绪时触发 tonic graceful shutdown。
    /// 用于 `coord dev` 模式等需要外部控制关闭的场景。
    pub async fn serve_with_shutdown(
        &self,
        shutdown: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        #[allow(deprecated)]
        use crate::cache::AgentCache;
        use proxy::{AgentInner, KvProxy, LeaseProxy, MaintenanceProxy, TxnProxy, WatchProxy};
        use std::sync::Arc;

        let addr: std::net::SocketAddr = self
            .config
            .agent_addr
            .parse()
            .map_err(|e| format!("invalid agent_addr {}: {e}", self.config.agent_addr))?;

        // 非 loopback 绑定强制 auth + TLS（与 server 侧 同口径）。
        // 防止生产网络裸奔（默认 auth 关闭、TLS None，仅限本机开发）。
        // 生产收口：TLS 不再是“仅校验配置”——下方 serve 路径真实挂载 `.tls_config()`。
        {
            let is_loopback = addr.ip().is_loopback();
            let auth_ok = self.config.auth.enabled;
            let tls_ok = self.config.tls.is_some();
            if !(is_loopback || auth_ok && tls_ok) {
                return Err(format!(
                    "refusing to bind non-loopback address {addr} without auth+TLS \
                     (auth.enabled={auth_ok}, tls={tls_ok}); set both or bind a loopback address"
                )
                .into());
            }
        }

        // 生产收口（#2）：入站 gRPC TLS 真实挂载——配置 tls 时构建 ServerTlsConfig，
        // 证书/私钥加载失败即拒绝启动（fail-closed）；ca_path 存在时强制 mTLS。
        let inbound_tls = match self.config.tls.as_ref() {
            Some(t) => Some(
                build_agent_tls_server_config(&t.cert_path, &t.key_path, t.ca_path.as_deref())
                    .map_err(|e| format!("agent inbound TLS: {e}"))?,
            ),
            None => None,
        };
        if inbound_tls.is_some() {
            tracing::info!(
                "coord-agent inbound gRPC TLS enabled (mTLS={})",
                self.config
                    .tls
                    .as_ref()
                    .and_then(|t| t.ca_path.as_ref())
                    .is_some()
            );
        }

        // 若配置了 Server 端点，创建内部 Client 用于请求转发
        // 带指数退避重试（最多 30 秒），避免 Server 尚未就绪时立即降级
        //
        // F-50（2026-09-19）：**agent 自身身份**的凭据句柄在这里就建好 —— `AgentInner`
        // 的共享出站客户端要装它作回退凭据，而开通（`Auth.Authenticate`）要等
        // 出站通道与插件身份管理器就绪后才做（见下方 `bootstrap_self_identity`）。
        // 句柄是 `Arc<CachedTokenProvider>`（内部可变），因此"先装句柄、后填凭据"
        // 的顺序是安全的：填充前读取得到 `None`（等价旧行为），填充后立即生效。
        let agent_self_identity =
            Arc::new(coord_client::credential::CachedTokenProvider::new(None));
        let inner = if !self.config.static_peers.is_empty() {
            tracing::info!(
                "coord-agent connecting to server cluster: {:?}",
                self.config.static_peers
            );
            // TLS/mTLS：AgentConfig.tls → coord-client TlsConfig（PEM 加载，fail-closed）
            let client_tls = match self.config.tls.as_ref() {
                Some(t) => Some(
                    t.to_coord_client_tls()
                        .map_err(|e| format!("agent TLS config: {e}"))?,
                ),
                None => None,
            };
            let retry_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
            let mut attempt = 0u32;
            loop {
                attempt += 1;
                // 每次尝试创建新的 AgentCache（失败时丢弃，成功时由 AgentInner 持有）
                let retry_cache = AgentCache::new(
                    self.config.cache_kv_max_entries,
                    self.config.cache_kv_ttl_secs,
                    500,
                    self.config.cache_catalog_ttl_secs,
                );
                match AgentInner::new(
                    self.config.static_peers.clone(),
                    retry_cache,
                    client_tls.clone(),
                    Arc::clone(&agent_self_identity),
                )
                .await
                {
                    Ok(inner) => {
                        tracing::info!(
                            "coord-agent connected to server cluster (attempt {})",
                            attempt
                        );
                        // R-AGT-20：连接成功 → 就绪位 + 连接指标实时回写
                        if let Some(ref flag) = self.ready_flag {
                            flag.store(true, std::sync::atomic::Ordering::Relaxed);
                        }
                        if let Some(ref metrics) = self.metrics {
                            metrics.set_connected(true);
                        }
                        break Some(Arc::new(inner));
                    }
                    Err(e) => {
                        if tokio::time::Instant::now() > retry_deadline {
                            tracing::warn!(
                                "coord-agent failed to connect to server cluster after {} attempts in 30s: {:?}; running in skeleton mode",
                                attempt, e
                            );
                            // R-AGT-20：skeleton 模式 → 未就绪
                            if let Some(ref flag) = self.ready_flag {
                                flag.store(false, std::sync::atomic::Ordering::Relaxed);
                            }
                            if let Some(ref metrics) = self.metrics {
                                metrics.set_connected(false);
                            }
                            break None;
                        }
                        let backoff_ms =
                            std::cmp::min(100u64 * 2u64.saturating_pow(attempt - 1), 5000);
                        tracing::warn!(
                            "coord-agent connection attempt {} failed: {:?}; retrying in {}ms",
                            attempt,
                            e,
                            backoff_ms
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                    }
                }
            }
        } else {
            tracing::warn!("coord-agent: no static_peers configured, running in skeleton mode");
            None
        };

        // 插件身份开通用的出站凭据句柄（进程级）：一次性 bootstrap token 只能
        // 兑换一次，SIGHUP 重建插件加载器时复用同一引导 CCT，避免二次兑换失败。
        //
        // 位置说明（F-50，2026-09-19）：句柄与下面的 **agent 自身身份**自举都放在
        // 「启动原生服务」**之前** —— 服务的后台任务（registry 订阅 / idgen nodeid
        // 注册 / 锁保活）一启动就会发请求，凭据必须先就位，否则启动窗口内必然
        // 重现 F-50 的 `missing CCT token`（即使随后自愈，日志与首轮请求也已失败）。
        let plugin_identity_provider =
            Arc::new(coord_client::credential::CachedTokenProvider::new(None));

        // ──── agent 自身身份（F-50，2026-09-19）────
        //
        // 代理路径解决的是"**替调用方**发请求时的凭据"（按请求转发，见
        // `auth/interceptor.rs`）。这里解决的是"**agent 为自己**发请求时的凭据"
        // —— 后者此前在结构上没有任何通道，于是服务端开启鉴权时，锁自动续期 /
        // registry 目录加载与订阅 / idgen nodeid 注册**全线** `missing CCT token`
        // （F-50：锁会"静默丢失"，调用方以为仍持有）。
        //
        // 触发条件（任一满足即尝试）：
        // - `auth.enabled`：agent 校验入站 CCT ⇒ 服务端几乎必然也开着鉴权；
        // - 配了 `auth.bootstrap_token`：运维显式给了开通凭据。
        // 两者都不满足（本地 dev 明文模式）时**不**尝试 —— 明文模式下凭据本就多余，
        // 尝试只会制造无意义噪声。
        // 取 agent 自身身份要用的出站客户端（与 `inner` 同一个连接）。
        //
        // 这里返回 `Option` 而**不是** `expect("inner.is_some() checked above")`：
        // 生产代码零 panic 路径是本仓硬卡口（`scripts/check-panics.sh`，P0-F.4）。
        // 更进一步：把“取值”与“条件”做成同一个绑定的两半，就不存在
        // “一处判定 Some、另一处再 assume Some”这种可能漂移的写法。
        let self_identity_client = inner.as_ref().map(|i| i.client.clone());
        let self_identity_wanted = !self.config.static_peers.is_empty()
            && (self.config.auth.enabled || !self.config.auth.bootstrap_token.trim().is_empty());

        if let Some(self_client) = self_identity_client.filter(|_| self_identity_wanted) {
            use crate::plugin::identity::{
                CoordAuthGateway, PluginAuthGateway, PluginClients, PluginIdentityManager,
            };
            let self_tls = self
                .config
                .tls
                .as_ref()
                .and_then(|t| t.to_coord_client_tls().ok());
            // ① 开通账户需要"能调 admin:auth:* 的凭据" —— 复用插件身份的引导 CCT。
            //    一次性令牌在这里**首次**兑换并缓存到 `plugin_identity_provider`，
            //    后续（插件加载器 / 角色同步）复用缓存，不会二次消费令牌。
            let bootstrap_client = match ensure_plugin_identity_token(
                &self.config.auth,
                &self.config.static_peers,
                self_tls.clone(),
                &plugin_identity_provider,
            )
            .await
            {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(
                        "agent self identity: bootstrap credential unavailable ({e}); \
                         retrying with the persisted account only"
                    );
                    None
                }
            };
            // ② 复用插件身份管理器：**同一套**开通序列（user_add → role_add →
            //    逐能力授权 → 角色绑定）与**同一套**续期循环，避免两套实现漂移。
            //    `gateway` 只用于续期失败后的密码重认证 / refresh，二者都是匿名白名单
            //    端点，故用一个无凭据客户端即可。
            let anonymous = Arc::new(coord_client::credential::CachedTokenProvider::new(None));
            match build_identity_client(&self.config.static_peers, self_tls, &anonymous).await {
                Ok(gw_client) => {
                    let gateway: Arc<dyn PluginAuthGateway> =
                        Arc::new(CoordAuthGateway::new(gw_client));
                    let clients = Arc::new(PluginClients::new(self_client));
                    match PluginIdentityManager::new(
                        gateway,
                        self.config.static_peers.clone(),
                        self.config
                            .tls
                            .as_ref()
                            .and_then(|t| t.to_coord_client_tls().ok()),
                        &self.config.data_dir,
                        clients,
                    ) {
                        Ok(mgr) => {
                            let user = crate::plugin::identity::DEFAULT_SELF_USER;
                            match mgr
                                .bootstrap_self_identity(
                                    bootstrap_client.as_ref(),
                                    user,
                                    &agent_self_identity,
                                )
                                .await
                            {
                                Ok(()) => {}
                                Err(e) => tracing::error!(
                                    "agent self identity UNAVAILABLE ({e}); agent-initiated \
                                     traffic (lock renew / registry catalog+watch / idgen \
                                     nodeid) will be rejected by the server with \
                                     `missing CCT token` (F-50) — provide a bootstrap token or \
                                     pre-create the '{user}' account"
                                ),
                            }
                        }
                        Err(e) => tracing::error!(
                            "agent self identity: identity manager unavailable ({e}); \
                             agent-initiated traffic will be rejected by the server (F-50)"
                        ),
                    }
                }
                Err(e) => tracing::error!(
                    "agent self identity: gateway client build failed ({e}); agent-initiated \
                     traffic will be rejected by the server (F-50)"
                ),
            }
        }

        // ──── 服务宿主：`PluginManager`（唯一注册表）────
        //
        // 原生服务与脚本插件共用同一宿主：生命周期（init/start/stop）、gRPC 服务面
        // （`Plugin::grpc_service`）、健康（`Plugin::health_check`）三者都从这
        // 一份注册表出去——不再有 ServiceManager + 硬编码路由链的双清单。
        let plugin_metrics = self.metrics.clone().unwrap_or_default();
        let plugin_manager = Arc::new(
            crate::plugin::PluginManager::new(self.config.plugins.clone())
                .with_metrics(plugin_metrics.clone()),
        );

        // 按配置启用高级服务
        // 保存各服务的 Arc 句柄，用于后续注册 gRPC Server
        // 装配期需要跨块复用的服务句柄；其余内建插件注册后句柄即不再保留
        //（插件注册表持有它们，装配期无需多留一份）。
        let mut event_grpc_svc: Option<
            Arc<crate::services::event_notification::EventNotificationService>,
        > = None;
        let mut cache_grpc_svc: Option<Arc<crate::services::cache::CacheService>> = None;
        let mut mq_grpc_svc: Option<Arc<crate::services::mq::MessageQueueService>> = None;

        if self.config.services.registry {
            if let Some(ref inner) = inner {
                let registry_svc = Arc::new(
                    crate::services::registry::RegistryService::new(inner.clone(), 500)
                        // R-AGT-20：watch/探测后台任务经 background 池
                        .with_thread_pools(self.thread_pools.clone()),
                );
                let _ = register_native_service(
                    &plugin_manager,
                    registry_svc.clone(),
                    crate::plugin::AgentGrpcService::Registry(registry_svc),
                )
                .await;
            } else {
                tracing::warn!("Registry service enabled but no server connection; skipping");
            }
        }

        // 保存 ConfigCenterService 的 Arc 句柄，用于后续注册 gRPC ConfigServer

        if self.config.services.config_center {
            if let Some(ref inner) = inner {
                let config_svc = Arc::new(
                    crate::services::config_center::ConfigCenterService::new(inner.clone())
                        // R-AGT-20：watch 后台任务经 background 池
                        .with_thread_pools(self.thread_pools.clone()),
                );
                let _ = register_native_service(
                    &plugin_manager,
                    config_svc.clone(),
                    crate::plugin::AgentGrpcService::ConfigCenter(config_svc),
                )
                .await;
            } else {
                tracing::warn!("ConfigCenter service enabled but no server connection; skipping");
            }
        }

        if self.config.services.lock {
            if let Some(ref inner) = inner {
                let lock_svc = Arc::new(crate::services::lock::LockService::new(inner.clone()));
                let _ = register_native_service(
                    &plugin_manager,
                    lock_svc.clone(),
                    crate::plugin::AgentGrpcService::Lock(lock_svc),
                )
                .await;
            } else {
                tracing::warn!("Lock service enabled but no server connection; skipping");
            }
        }

        if self.config.services.idgen {
            // IdGen 数据面服务：默认雪花（nodeid），可选号段（segment，opt-in）
            let idgen_mode =
                crate::services::idgen::IdGenMode::parse(&self.config.services.idgen_mode);
            let idgen_svc = Arc::new(crate::services::idgen::IdGenService::new_with_options(
                inner.clone(),
                1000,
                idgen_mode,
                self.config.services.idgen_node_id,
                &self.config.agent_addr,
            ));
            let registered = register_native_service(
                &plugin_manager,
                idgen_svc.clone(),
                crate::plugin::AgentGrpcService::IdGen(idgen_svc),
            )
            .await
            .is_some();
            if registered {
                if idgen_mode == crate::services::idgen::IdGenMode::Snowflake {
                    tracing::info!("ID Generator service registered (snowflake nodeid mode)");
                } else if inner.is_some() {
                    tracing::info!("ID Generator service registered (segment mode via server)");
                } else {
                    tracing::info!(
                        "ID Generator service registered (segment mode, local snowflake fallback)"
                    );
                }
            }
        }

        if self.config.services.event_notification {
            if let Some(ref inner) = inner {
                let event_svc = Arc::new(
                    crate::services::event_notification::EventNotificationService::new(
                        inner.clone(),
                        1000,
                        256,
                    ),
                );
                event_grpc_svc = register_native_service(
                    &plugin_manager,
                    event_svc.clone(),
                    crate::plugin::AgentGrpcService::EventNotification(event_svc),
                )
                .await;
            } else {
                tracing::warn!(
                    "EventNotification service enabled but no server connection; skipping"
                );
            }
        }

        if self.config.services.leader_election {
            if let Some(ref inner) = inner {
                let election_svc = Arc::new(
                    crate::services::leader_election::LeaderElectionService::new(
                        inner.clone(),
                        256,
                    ),
                );
                let _ = register_native_service(
                    &plugin_manager,
                    election_svc.clone(),
                    crate::plugin::AgentGrpcService::LeaderElection(election_svc),
                )
                .await;
            } else {
                tracing::warn!("LeaderElection service enabled but no server connection; skipping");
            }
        }

        if self.config.services.workflow {
            // R-AGT-09：有 server 连接时走 KvWorkflowStore（raft 持久化），
            // 启动时 init() 全量重建本地缓存 + watch 断连重连对账；
            // 无连接时退回内存态（单 agent 测试/离线场景）。
            let workflow_svc = match &inner {
                Some(inner_ref) => Arc::new(
                    crate::services::workflow::phase4::WorkflowEngineService::new_with_kv_store(
                        inner_ref.clone(),
                    ),
                ),
                None => {
                    tracing::warn!(
                        "Workflow engine enabled but no server connection; using memory-only store"
                    );
                    Arc::new(crate::services::workflow::phase4::WorkflowEngineService::new())
                }
            };
            let workflow_grpc_svc = register_native_service(
                &plugin_manager,
                workflow_svc.clone(),
                crate::plugin::AgentGrpcService::Workflow(workflow_svc),
            )
            .await;
            if workflow_grpc_svc.is_some() {
                tracing::info!("Workflow engine service registered (v4.0 coord-core engine)");
            }

            // 启动工作流调度器（标准 §Scheduling：schedule.every/cron/after/on）
            if let Some(engine) = workflow_grpc_svc.as_ref() {
                let scheduler = Arc::new(
                    crate::services::workflow_scheduler::WorkflowScheduler::new(Arc::clone(engine)),
                );
                scheduler.spawn();
                tracing::info!("Workflow scheduler started (every/cron/after/on)");

                // on 模式：订阅 EventNotification 事件 → 触发新实例
                if let Some(event_svc) = event_grpc_svc.as_ref() {
                    let sched = Arc::clone(&scheduler);
                    let mut rx = event_svc.subscribe();
                    tokio::spawn(async move {
                        loop {
                            match rx.recv().await {
                                Ok(ev) => {
                                    let data = serde_json::from_slice(&ev.data)
                                        .unwrap_or(serde_json::Value::Null);
                                    let we = crate::services::workflow_scheduler::WorkflowEvent {
                                        event_type: ev.event_type.clone(),
                                        source: Some(ev.source.clone()),
                                        data,
                                    };
                                    sched.handle_event(&we).await;
                                }
                                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                    continue
                                }
                                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                            }
                        }
                    });
                }
            }
        }

        if self.config.services.scheduler {
            // 调度状态持久化（P0-10 / E9）：有 server 连接 ⇒ 共享 KV（重启不丢、
            // 跨 Agent 唯一认领）；无 ⇒ 内存降级 **+ 明确告警**（不静默）。
            use crate::services::scheduler::{DefaultConfig, SchedulerService};
            use crate::services::scheduler_store::{KvSchedulerStore, SchedulerStore};

            let claim_ttl = std::time::Duration::from_secs(60);
            let scheduler_svc = match inner.as_ref() {
                Some(inner) => {
                    let store: Arc<dyn SchedulerStore> =
                        Arc::new(KvSchedulerStore::new(Arc::clone(inner)));
                    SchedulerService::with_store(store, claim_ttl)
                }
                None => {
                    tracing::warn!(
                        "Scheduler service running without Server KV: task/claim state is \
                         process-local and will be lost on restart, and multi-agent \
                         unique claiming is NOT guaranteed"
                    );
                    SchedulerService::new(DefaultConfig)
                }
            };
            let scheduler_svc = Arc::new(scheduler_svc);
            let _ = register_native_service(
                &plugin_manager,
                scheduler_svc.clone(),
                crate::plugin::AgentGrpcService::Scheduler(scheduler_svc),
            )
            .await;
        }

        // 数据面服务（Cache + MQ，基于 redb 本地引擎，无需 Server 连接）
        if self.config.services.cache {
            let data_dir = std::path::PathBuf::from(&self.config.data_dir);
            let cache_svc = Arc::new(crate::services::cache::CacheService::new(
                data_dir.clone(),
                // ⚠️ 第四轮 §3.10 h：这个值**只是声明**，CacheService 尚未实现淘汰
                // （见 `CacheService::new` 的注释与启动告警）。构造参数保留是为了把"配了
                // 多少"如实报出来，而不是继续用一个下划线参数假装遵守了它。
                1024 * 1024 * 1024, // 声明的上限：1GB（未强制执行）
                3600,               // default TTL 1 hour
            ));
            // 绑定自身弱引用，gRPC handler 才能升级 Arc 走 spawn_blocking
            cache_svc.bind_self_weak(&cache_svc);
            cache_grpc_svc = register_native_service(
                &plugin_manager,
                cache_svc.clone(),
                crate::plugin::AgentGrpcService::Cache(cache_svc),
            )
            .await;
        }

        if self.config.services.mq {
            let data_dir = std::path::PathBuf::from(&self.config.data_dir);
            let mq_svc = Arc::new(crate::services::mq::MessageQueueService::new(
                data_dir.clone(),
                1024 * 1024 * 1024, // 1GB max
            ));
            // 绑定自身弱引用，gRPC handler 才能升级 Arc 走 spawn_blocking
            mq_svc.bind_self_weak(&mq_svc);
            mq_grpc_svc = register_native_service(
                &plugin_manager,
                mq_svc.clone(),
                crate::plugin::AgentGrpcService::Mq(mq_svc),
            )
            .await;
        }

        // ISR 跨 Agent 数据复制（v2.1 已落地）：Cache/MQ 写路径经复制管理器
        // 同步到 ISR Followers（min_isr 可配置；单 agent 部署自动降级为 1，C6）。
        if self.config.services.replication {
            use crate::services::grpc_handlers::ReplicaRouter;
            use crate::services::replication::ReplicationManager;

            // 生产收口（#3）：复制通道 TLS——复用 AgentConfig.tls（同一 PKI）注入
            // ReplicaClient；PEM 加载失败即拒绝启动（fail-closed）。
            let repl_tls = match self.config.tls.as_ref() {
                Some(t) => Some(
                    t.to_coord_client_tls()
                        .map_err(|e| format!("replication TLS config: {e}"))?,
                ),
                None => None,
            };
            // fail-closed：复制启用且存在非 loopback 对端时，无 TLS 拒绝启动
            // （明文复制仅限 loopback 开发/单机）。
            if repl_tls.is_none() {
                for peer in &self.config.replication_peers {
                    if !is_loopback_host(peer) {
                        return Err(format!(
                            "refusing to start ISR replication with non-loopback peer {peer} \
                             without TLS (services.replication requires tls for non-loopback peers)"
                        )
                        .into());
                    }
                }
            }

            let manager = Arc::new(ReplicationManager::new(
                self.config.replication.clone(),
                self.config.agent_addr.clone(),
            ));
            manager.set_tls(repl_tls);
            // 首版静态成员（Q1）；Registry 发现为演进路径
            manager.set_peers(self.config.replication_peers.clone());
            tracing::info!(
                "ISR replication enabled: agent={} min_isr={} peers={:?}",
                self.config.agent_addr,
                self.config.replication.min_isr,
                self.config.replication_peers
            );

            // 挂载到数据面服务（MQ / Cache 写路径接入复制）
            if let Some(mq) = &mq_grpc_svc {
                mq.set_replication(Some(manager.clone()));
            }
            if let Some(cache) = &cache_grpc_svc {
                cache.set_replication(Some(manager.clone()));
            }

            // Replica gRPC 服务路由（对端 Apply / Reconcile / IsrHeartbeat）
            let router = Arc::new(ReplicaRouter::new(
                manager.clone(),
                mq_grpc_svc.clone(),
                cache_grpc_svc.clone(),
            ));
            // 心跳不再在此处启动：它是复制服务插件 `start()` 的动作
            //（`ReplicaRouter` 的 `BaseService` 实现），由 PluginManager 统一驱动。
            router.bind_self_weak(&router);

            let _ = register_native_service(
                &plugin_manager,
                router.clone(),
                crate::plugin::AgentGrpcService::Replication(router),
            )
            .await;
        }

        // 安全策略引擎（本地 RBAC/ABAC，可扩展至 OPA）
        if self.config.services.policy {
            // 有 Server KV 连接时启用 bundle 通道（with_kv），否则仅 RBAC 引擎
            let policy_svc = Arc::new(match &inner {
                Some(inner) => {
                    tracing::info!("Policy service bundle channel: Server KV connected");
                    crate::services::policy::PolicyService::with_kv(1024, inner.clone())
                }
                None => {
                    tracing::warn!(
                        "Policy service running without Server KV (bundle API disabled)"
                    );
                    crate::services::policy::PolicyService::new(1024)
                }
            });
            let registered = register_native_service(
                &plugin_manager,
                policy_svc.clone(),
                crate::plugin::AgentGrpcService::Policy(policy_svc),
            )
            .await
            .is_some();
            if registered {
                tracing::info!("Policy service registered (v3.0, RBAC/ABAC engine)");
            }
        }

        // Transit 信封加密（/ DEK 持久化：生产走 coord-server 共享 KV，B-06 / E1）
        if self.config.services.transit {
            use crate::services::transit::{TransitConfig, TransitService};
            use crate::services::transit_store::{KvTransitDekStore, TransitDekStore};

            let transit_config = TransitConfig::default();
            // 生产（已连接 server 集群）：加密态 DEK 落共享 KV —— 重启不丢密钥、
            // 单次使用跨 Agent 成立；骨架模式（无 server）：降级内存 store（dev/单测）。
            let transit_svc = match &inner {
                Some(inner) => {
                    let store: Arc<dyn TransitDekStore> =
                        Arc::new(KvTransitDekStore::new(inner.clone()));
                    TransitService::with_store(transit_config, store)
                }
                None => {
                    tracing::warn!(
                        "Transit service running without Server KV: DEK persistence disabled \
                         (keys are lost on restart)"
                    );
                    TransitService::new(transit_config)
                }
            };

            match transit_svc {
                Ok(transit_svc) => {
                    let transit_svc = Arc::new(transit_svc);
                    let _ = register_native_service(
                        &plugin_manager,
                        transit_svc.clone(),
                        crate::plugin::AgentGrpcService::Transit(transit_svc),
                    )
                    .await;
                }
                Err(e) => {
                    tracing::error!("failed to create transit service: {e}");
                }
            }
        }

        if self.config.services.circuit_breaker {
            use std::time::Duration;
            let cb_svc = Arc::new(
                crate::services::circuit_breaker::CircuitBreakerService::new(
                    5,
                    Duration::from_secs(30),
                ),
            );
            let _ = register_native_service(
                &plugin_manager,
                cb_svc.clone(),
                crate::plugin::AgentGrpcService::CircuitBreaker(cb_svc),
            )
            .await;
        }

        if self.config.services.rate_limiter {
            use crate::services::rate_limiter::RateLimiterConfig;
            let rl_config = RateLimiterConfig {
                max_tokens: 100,
                refill_rate: 10.0,
            };
            let rl_svc = Arc::new(crate::services::rate_limiter::RateLimiterService::new(
                rl_config,
            ));
            let _ = register_native_service(
                &plugin_manager,
                rl_svc.clone(),
                crate::plugin::AgentGrpcService::RateLimiter(rl_svc),
            )
            .await;
        }

        if self.config.services.feature_flags {
            // 开关持久化（P0-5 / E2）：有 server 连接 ⇒ 共享 KV（重启不丢、
            // 跨 Agent 一致）；无 ⇒ 内存降级 **+ 明确告警**（不静默）。
            use crate::feature_flags::{FeatureFlagService, FlagConfig};
            use crate::feature_flags_store::{FeatureFlagStore, KvFeatureFlagStore};

            let ff_svc = match inner.as_ref() {
                Some(inner) => {
                    let store: Arc<dyn FeatureFlagStore> =
                        Arc::new(KvFeatureFlagStore::new(Arc::clone(inner)));
                    FeatureFlagService::with_store(FlagConfig::default(), store)
                }
                None => {
                    tracing::warn!(
                        "Feature flags service running without Server KV: flag state is \
                         process-local, will be lost on restart, and is NOT shared across agents"
                    );
                    FeatureFlagService::new(FlagConfig::default())
                }
            };
            let ff_svc = Arc::new(ff_svc);
            let _ = register_native_service(
                &plugin_manager,
                ff_svc.clone(),
                crate::plugin::AgentGrpcService::FeatureFlags(ff_svc),
            )
            .await;
        }

        // PKI CA 证书签发服务（/ get-or-create + 共享 KV 持久化）
        if self.config.services.pki {
            use crate::pki::PkiConfig;
            use crate::pki_store::{KvPkiStore, PkiStore};

            let pki_config = PkiConfig::default();
            // 生产（已连接 server 集群）：共享 KV store —— 多 agent 共享同一 CA 根、重启不丢；
            // 骨架模式（无 server）：降级内存 store（dev/单测）。
            let pki_svc: Option<crate::pki::PkiService> = match inner {
                Some(ref inner) => {
                    let store: Arc<dyn PkiStore> = Arc::new(KvPkiStore::new(inner.clone()));
                    Some(crate::pki::PkiService::with_store(pki_config, store))
                }
                None => crate::pki::PkiService::new(pki_config).ok(),
            };

            if let Some(pki_svc) = pki_svc {
                // CA 的 get-or-create 是 `PkiService::start()` 的动作（插件生命周期），
                // 不再在此处内联 await。
                let pki_svc = Arc::new(pki_svc);
                let _ = register_native_service(
                    &plugin_manager,
                    pki_svc.clone(),
                    crate::plugin::AgentGrpcService::Pki(pki_svc),
                )
                .await;
            } else {
                tracing::error!("failed to create PKI service");
            }
        }

        // 协议版本协商（P0-4 / D6）：**无条件**注册，且必须先于其余服务就绪 ——
        // 它的存在意义就是让"协议不匹配"变得**可诊断**（而不是裸的 `UNIMPLEMENTED`），
        // 因此在任何改名切换之前就必须可达。
        {
            let handshake = Arc::new(crate::services::handshake::HandshakeService::new());
            let _ = register_native_service(
                &plugin_manager,
                handshake.clone(),
                crate::plugin::AgentGrpcService::Handshake(handshake),
            )
            .await;
        }

        // 启动全部已注册的原生服务（生命周期由插件管理器统一驱动；
        // 单个服务启动失败只隔离该服务，不阻塞其余）。
        let native_start_failures = plugin_manager.start_all().await;
        if !native_start_failures.is_empty() {
            tracing::error!(
                "{} builtin service(s) failed to start: {:?}",
                native_start_failures.len(),
                native_start_failures
            );
        }

        // 插件身份开通用的出站凭据句柄已在**服务启动之前**创建（见上方 F-50 段），
        // 此处不再重复声明。

        // ──── 插件引擎：加载配置声明的脚本插件（js/wasm）────
        //
        // 原生服务已在上方作为**内建插件**注册并启动，不走这条路径（它们不参与
        // 配置 diff）。此处只处理 `[plugins].entries` 声明的外部插件。
        if plugin_manager.is_enabled() {
            // JS 加载器：需要真实 coord-client（skeleton 模式下无后端 → deferred）
            let loader = build_plugin_loader(PluginLoaderArgs {
                cfg: &self.config.plugins,
                auth: &self.config.auth,
                data_dir: &self.config.data_dir,
                tls: self
                    .config
                    .tls
                    .as_ref()
                    .and_then(|t| t.to_coord_client_tls().ok()),
                static_peers: &self.config.static_peers,
                fallback: inner.as_ref().map(|i| &i.client),
                identity_provider: &plugin_identity_provider,
                metrics: Some(plugin_metrics.clone()),
            })
            .await;
            if loader.is_none() && !self.config.plugins.entries.is_empty() {
                tracing::warn!(
                    "plugin engine: {} configured entry(ies) deferred (no coord-client backend \
                     available in skeleton mode)",
                    self.config.plugins.entries.len()
                );
            }
            let report = plugin_manager
                .reload(&self.config.plugins.entries, loader.as_deref())
                .await;
            let (healthy, total, unhealthy) = plugin_manager.health_check_all();
            tracing::info!(
                "plugin engine ENABLED: {}/{} plugin(s) healthy (unhealthy: {:?}); configured \
                 entries: added={:?} replaced={:?} deferred={:?} failed={:?}; builtin start \
                 failures: {}",
                healthy,
                total,
                unhealthy,
                report.added,
                report.replaced,
                report.deferred,
                report.failed,
                native_start_failures.len()
            );
        } else {
            let (healthy, total, unhealthy) = plugin_manager.health_check_all();
            tracing::debug!(
                "external plugin engine disabled (plugins.enabled=false); {}/{} builtin \
                 service plugin(s) healthy (unhealthy: {:?})",
                healthy,
                total,
                unhealthy
            );
        }

        // SIGHUP：重载配置并应用插件集 diff（新增/移除/版本替换）。
        // 引擎参数 / 默认限制 / 监听地址等结构性变更需重启。
        if let Some(watcher) = self.config_watcher.clone() {
            let pm = Arc::clone(&plugin_manager);
            let sighup_loader = build_plugin_loader(PluginLoaderArgs {
                cfg: &self.config.plugins,
                auth: &self.config.auth,
                data_dir: &self.config.data_dir,
                tls: self
                    .config
                    .tls
                    .as_ref()
                    .and_then(|t| t.to_coord_client_tls().ok()),
                static_peers: &self.config.static_peers,
                fallback: inner.as_ref().map(|i| &i.client),
                identity_provider: &plugin_identity_provider,
                metrics: Some(plugin_metrics.clone()),
            })
            .await;
            #[cfg(unix)]
            tokio::spawn(async move {
                let mut sig =
                    match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()) {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::warn!("SIGHUP handler unavailable: {e}");
                            return;
                        }
                    };
                loop {
                    if sig.recv().await.is_none() {
                        break;
                    }
                    match watcher.reload() {
                        Ok(()) => {
                            let cfg = watcher.current_config();
                            let report = pm
                                .reload(&cfg.plugins.entries, sighup_loader.as_deref())
                                .await;
                            tracing::info!(
                                "SIGHUP: plugin set reloaded (added={:?} removed={:?} \
                                 replaced={:?} deferred={:?} failed={:?})",
                                report.added,
                                report.removed,
                                report.replaced,
                                report.deferred,
                                report.failed
                            );
                        }
                        Err(e) => tracing::warn!(
                            "SIGHUP: failed to reload config from {}: {e}; keeping current",
                            watcher.path().display()
                        ),
                    }
                }
            });
            #[cfg(not(unix))]
            {
                let _ = (watcher, pm, sighup_loader);
                tracing::debug!("SIGHUP plugin reload not supported on this platform");
            }
        }

        tracing::info!(
            "coord-agent gRPC server listening on {}",
            self.config.agent_addr
        );

        // A3：鉴权开启时**必须有**可用密钥材料，否则拒绝启动。
        // 此前 `signing_key_hex = ""` 会被 `hex::decode("")` 解成 `Ok(vec![])`
        // —— 即**空 HMAC 密钥**；一旦角色同步补齐，可被自签 `roles:["root"]` 利用。
        validate_auth_key_material(&self.config.auth)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?;

        // A3：RoleCache 只建一次，由鉴权中间件与后台角色同步**共享**同一实例。
        let role_cache = Arc::new(crate::auth::role_cache::RoleCache::new());

        // agent gRPC 挂 AuthInterceptor（默认关闭，开启后全量 RPC 校验 CCT）
        let mut auth_interceptor = crate::auth::interceptor::AuthInterceptor::new(
            if self.config.auth.enabled {
                hex::decode(&self.config.auth.signing_key_hex)
                    .map_err(|e| format!("invalid auth.signing_key_hex: {e}"))?
            } else {
                Vec::new()
            },
            Arc::clone(&role_cache),
            self.config.auth.clock_drift_secs,
        );
        // 配置 Ed25519 公钥 → 验证 server 非对称签发的 CCT（仅存公钥，不可伪造）
        if self.config.auth.enabled && !self.config.auth.verifying_key_hex.is_empty() {
            let vk = hex::decode(&self.config.auth.verifying_key_hex)
                .map_err(|e| format!("invalid auth.verifying_key_hex: {e}"))?;
            if vk.len() != 32 {
                return Err(
                    "auth.verifying_key_hex must be 64 hex chars (32-byte Ed25519 public key)"
                        .into(),
                );
            }
            auth_interceptor = auth_interceptor.with_verifying_key(vk);
        }
        auth_interceptor.set_enabled(self.config.auth.enabled);
        if self.config.auth.enabled {
            // A3：接线 SyncScheduler → RoleCache（此前 `sync_full` 零生产调用方，
            // 开启鉴权后缓存恒空 → 全量 RPC 被拒且原因不可见）。
            //
            // A3 连带项修复：同步客户端复用**同一**出站凭据句柄（引导 CCT /
            // 持久化会话）。此前该句柄只在「plugin-js/wasm feature **且**
            // bootstrap_token 非空」时被填充——没有插件 feature 时角色同步永远
            // 拿不到凭据 → 开鉴权后依然全量 403。这里显式做一次引导兑换
            // （已兑换则复用缓存，不重复消费一次性令牌）。
            let outbound_tls = self
                .config
                .tls
                .as_ref()
                .and_then(|t| t.to_coord_client_tls().ok());
            if self.config.auth.bootstrap_token.trim().is_empty() {
                tracing::error!(
                    "auth.enabled=true but auth.bootstrap_token is empty: role sync has no \
                     outbound credential (startup validation should have refused this)"
                );
            } else if !plugin_identity_provider.is_set() {
                match ensure_plugin_identity_token(
                    &self.config.auth,
                    &self.config.static_peers,
                    outbound_tls.clone(),
                    &plugin_identity_provider,
                )
                .await
                {
                    Ok(Some(_)) => tracing::info!(
                        "agent role sync credential acquired via Auth.Bootstrap \
                         (shared with plugin identity)"
                    ),
                    Ok(None) => {}
                    Err(e) => tracing::error!(
                        "agent role sync could not bootstrap an outbound credential: {e}; \
                         role sync will be denied by the server (fail-closed)"
                    ),
                }
            }
            match crate::auth::sync::spawn_role_sync(
                Arc::clone(&role_cache),
                self.config.static_peers.clone(),
                outbound_tls,
                Arc::clone(&plugin_identity_provider),
            )
            .await
            {
                Ok(()) => tracing::info!(
                    "agent role sync scheduled (RoleCache shared with auth interceptor)"
                ),
                Err(e) => tracing::error!(
                    "agent role sync could not start: {e}; local authorization will deny \
                     every role-gated RPC (fail-closed)"
                ),
            }
            tracing::info!(
                "coord-agent auth interceptor ENABLED (CCT + capability, unknown RPC deny)"
            );
        } else {
            tracing::warn!(
                "coord-agent auth interceptor DISABLED (set agent.auth.enabled=true to enable)"
            );
        }

        // 构建 gRPC router：核心服务 + Registry + Config + 可插拔服务。
        // 生产收口（#2）：服务链泛型于 Server<L>（明文 Identity / TLS TlsAcceptor），
        // 由 build_agent_grpc_router 单份组装；inbound_tls Some 时真实挂载 TLS。
        // R-AGT-20：最外层挂 gRPC 请求计数中间件（record_grpc_request 接线）
        let auth_interceptor = Arc::new(auth_interceptor);
        let metrics_layer_value = self.metrics.clone().unwrap_or_default();
        let svcs = AgentGrpcSvcs {
            inner,
            plugin: Some(Arc::new(crate::plugin::PluginService::new(Arc::clone(
                &plugin_manager,
            )))),
        };

        // Phase 2.1：插件网关层（观察默认开启；拒绝钩子由宿主 / 插件注册）。
        // 网关位于 auth 之后、router 之前，故代理透传流量也经过它（仅观察）。
        let plugin_gateway = Arc::new(crate::plugin::PluginGateway::new());
        if self.config.plugins.hooks_enabled {
            for path in [
                "/coord.kv.KV/Put",
                "/coord.kv.KV/Range",
                "/coord.kv.KV/Delete",
                "/coord.txn.Txn/Txn",
                "/coord.lease.Lease/LeaseGrant",
                "/coord.lease.Lease/LeaseRevoke",
                "/coord.lease.Lease/LeaseKeepAlive",
                "/coord.watch.Watch/Watch",
                "/coord.storage.Storage/Put",
                "/coord.storage.Storage/Get",
                "/coord.storage.Storage/Stat",
                "/coord.storage.Storage/Delete",
                "/coord.plugin.Plugin/Invoke",
            ] {
                plugin_gateway.watch_path(path);
            }
            tracing::debug!("plugin gateway observe paths registered (hooks_enabled=true)");
        }

        // 注册 gRPC Health Check 服务（标准 grpc.health.v1.Health）
        let (health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter.set_serving::<KvServer<KvProxy>>().await;
        health_reporter.set_serving::<TxnServer<TxnProxy>>().await;
        health_reporter
            .set_serving::<LeaseServer<LeaseProxy>>()
            .await;
        health_reporter
            .set_serving::<WatchServer<WatchProxy>>()
            .await;
        health_reporter
            .set_serving::<MaintenanceServer<MaintenanceProxy>>()
            .await;

        // 注册 gRPC Server Reflection 服务
        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(coord_proto::FILE_DESCRIPTOR_SET)
            .build_v1()
            .map_err(|e| format!("failed to build reflection service: {e}"))?;

        // Phase 2.1：插件网关层（auth 之后、router 之前）。
        // 无钩子且无观察路径时 `is_passthrough()` → 中间件完全直通（零开销）。
        tracing::debug!(
            "plugin gateway layer mounted (hooks={}, passthrough={})",
            plugin_gateway.hook_count(),
            plugin_gateway.is_passthrough()
        );

        match inbound_tls {
            Some(server_tls) => {
                let server = tonic::transport::Server::builder()
                    .layer(crate::plugin::PluginGatewayLayer::new(Arc::clone(
                        &plugin_gateway,
                    )))
                    .layer(crate::auth::interceptor::AuthLayer::new(Arc::clone(
                        &auth_interceptor,
                    )))
                    .layer(crate::metrics::AgentGrpcMetricsLayer::new(
                        metrics_layer_value.clone(),
                    ))
                    .tls_config(server_tls)
                    .map_err(|e| format!("agent inbound TLS server: {e}"))?;
                let router =
                    build_agent_grpc_router(server, svcs, self.metrics.clone(), &plugin_manager)?;
                tracing::info!("coord-agent gRPC server serving over TLS (mTLS per tls.ca_path)");
                router
                    .add_service(health_service)
                    .add_service(reflection_service)
                    .serve_with_shutdown(addr, shutdown)
                    .await?;
            }
            None => {
                let server = tonic::transport::Server::builder()
                    .layer(crate::plugin::PluginGatewayLayer::new(Arc::clone(
                        &plugin_gateway,
                    )))
                    .layer(crate::auth::interceptor::AuthLayer::new(Arc::clone(
                        &auth_interceptor,
                    )))
                    .layer(crate::metrics::AgentGrpcMetricsLayer::new(
                        metrics_layer_value,
                    ));
                let router =
                    build_agent_grpc_router(server, svcs, self.metrics.clone(), &plugin_manager)?;
                router
                    .add_service(health_service)
                    .add_service(reflection_service)
                    .serve_with_shutdown(addr, shutdown)
                    .await?;
            }
        }

        // 优雅停止：内建插件（原生服务，含心跳/后台任务）与脚本插件一视同仁，
        // 由插件管理器逆序停止（单个失败不阻塞其余）。
        plugin_manager.stop_all().await;

        Ok(())
    }
}

// ──── run_agent ────

/// 启动 Agent 守护进程
///
/// 由 `coord agent` 子命令调用。此函数阻塞直到收到终止信号。
pub async fn run_agent(
    config: AgentConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    run_agent_inner(config, None).await
}

/// 同 [`run_agent`]，但额外传入配置文件路径：启用 SIGHUP 触发的**插件集 diff**
/// 热重载（配置文件读取失败仅告警，不影响启动）。
pub async fn run_agent_with_config_path(
    config: AgentConfig,
    config_path: Option<std::path::PathBuf>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let watcher = config_path.and_then(|p| match crate::config_watcher::ConfigWatcher::new(&p) {
        Ok(w) => Some(w),
        Err(e) => {
            tracing::warn!(
                "failed to init config watcher for {} (SIGHUP plugin reload disabled): {e}",
                p.display()
            );
            None
        }
    });
    run_agent_inner(config, watcher).await
}

async fn run_agent_inner(
    config: AgentConfig,
    config_watcher: Option<crate::config_watcher::ConfigWatcher>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::health::start_health_server;
    use crate::metrics::AgentMetrics;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    tracing::info!(
        "coord-agent starting on {}, http on {}",
        config.agent_addr,
        config.http_addr
    );

    // C3: 启动 HTTP health/metrics 端点
    let metrics = AgentMetrics::new();
    // R-AGT-20：就绪标志（连接探针实时回写，非启动快照）
    let ready_flag = Arc::new(AtomicBool::new(false));
    let _health_handle =
        start_health_server(&config.http_addr, metrics.clone(), Arc::clone(&ready_flag));

    // R-AGT-20：连接状态探针——每 5s 探测全部 static_peers（TCP connect，1s 超时），
    // 实时回写 ready 与 coord_agent_connected（此前 has_peers 为启动快照，
    // 连接成功与否均不回写 readiness）。
    {
        let peers = config.static_peers.clone();
        let flag = Arc::clone(&ready_flag);
        let metrics_for_probe = metrics.clone();
        tokio::spawn(async move {
            loop {
                let alive = if peers.is_empty() {
                    false // 无 static_peers（骨架模式）不视为已连接集群
                } else {
                    let mut any = false;
                    for peer in &peers {
                        if let Ok(Ok(_)) = tokio::time::timeout(
                            std::time::Duration::from_secs(1),
                            tokio::net::TcpStream::connect(peer.clone()),
                        )
                        .await
                        {
                            any = true;
                            break;
                        }
                    }
                    any
                };
                flag.store(alive, Ordering::Relaxed);
                metrics_for_probe.set_connected(alive);
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        });
    }

    // R-AGT-20：资源隔离线程池接线（此前 AgentThreadPools 为死代码）——
    // registry/config watch 与探测后台任务经 background 池 spawn。
    let thread_pools = Arc::new(crate::threadpool::AgentThreadPools::new(
        config.thread_pools.clone(),
    ));

    let server = AgentServer::new(config)
        .with_thread_pools(Arc::clone(&thread_pools))
        .with_metrics(metrics)
        .with_ready_flag(Arc::clone(&ready_flag));
    // SIGHUP 插件集热重载（仅在提供配置文件路径时启用）
    let server = match config_watcher {
        Some(w) => server.with_config_watcher(w),
        None => server,
    };

    // 启动 gRPC server（带优雅关闭）
    tracing::info!("coord-agent: starting gRPC services (KV/Txn/Lease/Watch/Maintenance)");
    server.serve_with_shutdown(shutdown_signal()).await?;

    Ok(())
}

/// 优雅关闭信号（Ctrl+C / SIGTERM）
async fn shutdown_signal() {
    let ctrl_c = async {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {}
            Err(e) => {
                tracing::warn!("failed to install Ctrl+C handler: {e}");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                let _ = sig.recv().await;
            }
            Err(e) => {
                tracing::warn!("failed to install SIGTERM handler: {e}");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            tracing::info!("Received SIGINT (Ctrl+C), shutting down agent...");
        }
        _ = terminate => {
            tracing::info!("Received SIGTERM, shutting down agent...");
        }
    }
}

/// run_agent 返回类型别名（测试签名校验用）
pub type RunAgentFuture =
    Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send>>;

/// 用于测试的 run_agent 类型别名，验证函数签名兼容性
#[doc(hidden)]
pub fn __run_agent_type_check(config: AgentConfig) -> RunAgentFuture {
    Box::pin(run_agent(config))
}
