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
// 参见 docs/client-agent-architecture-v3.md。

pub mod auth;
pub mod cache;
pub mod config_watcher;
mod discovery;
pub mod feature_flags;
pub mod health;
pub mod key_util;
pub mod metrics;
mod proxy;
pub mod pki;
pub mod saga;
pub mod service;
pub mod services;
pub mod threadpool;
pub mod tls;

use std::future::Future;
use std::pin::Pin;

use coord_proto::kv::kv_server::KvServer;
use coord_proto::txn::txn_server::TxnServer;
use coord_proto::lease::lease_server::LeaseServer;
use coord_proto::watch::watch_server::WatchServer;
use coord_proto::maintenance::maintenance_server::MaintenanceServer;

// 重新导出公共类型
pub use discovery::StaticDiscovery;
pub use service::{BaseService, ServiceConfig, ServiceManager, ServiceResult};
pub use threadpool::{AgentThreadPools, ThreadPoolConfig};
pub use key_util::{KeyUtil, KeyUtilConfig, KeyStore, KeyStoreBackend, FileKeyStore, KeyStoreError};
pub use pki::{PkiService, PkiConfig, CertInfo, PkiError};
pub use tls::{AgentTlsConfig, build_agent_tls_channel, build_agent_tls_server_config};

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
    /// KV 读缓存 TTL（秒，默认 30）
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

    // Phase D+: 可插拔服务配置
    /// 高级基础服务启用配置（v3.0 可插拔服务框架）
    #[serde(default)]
    pub services: ServiceConfig,

    // TLS/mTLS 传输安全
    /// TLS 证书配置（None = 禁用 TLS，仅用于开发环境）
    #[serde(default)]
    pub tls: Option<AgentTlsConfig>,

    // 线程池资源隔离
    /// 线程池配置（v8.2 §3.2）
    #[serde(default)]
    pub thread_pools: ThreadPoolConfig,
}

// Serde default functions
fn default_agent_addr() -> String { "127.0.0.1:19527".into() }
fn default_http_addr() -> String { "127.0.0.1:19528".into() }
fn default_data_dir() -> String { "/var/lib/coord-agent".into() }
fn default_cache_kv_max_entries() -> usize { 10000 }
fn default_cache_kv_ttl_secs() -> u64 { 30 }
fn default_cache_catalog_ttl_secs() -> u64 { 10 }
fn default_cache_route_ttl_secs() -> u64 { 60 }
fn default_proxy_max_retries() -> u32 { 3 }
fn default_proxy_request_timeout_secs() -> u64 { 5 }

impl AgentConfig {
    /// 从 TOML 文件加载配置
    pub fn from_file(path: &std::path::Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
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
            cache_kv_ttl_secs: 30,
            cache_catalog_ttl_secs: 10,
            cache_route_ttl_secs: 60,
            proxy_max_retries: 3,
            proxy_request_timeout_secs: 5,
            services: ServiceConfig::default(),
            tls: None,
            thread_pools: ThreadPoolConfig::default(),
        }
    }
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
}

impl AgentServer {
    /// 创建新的 AgentServer 实例
    pub fn new(config: AgentConfig) -> Self {
        Self { config }
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
        use std::sync::Arc;
        use proxy::{AgentInner, KvProxy, TxnProxy, LeaseProxy, WatchProxy, MaintenanceProxy};
        #[allow(deprecated)]
        use crate::cache::AgentCache;
        use crate::service::ServiceManager;

        let addr = self.config.agent_addr.parse()
            .map_err(|e| format!("invalid agent_addr {}: {e}", self.config.agent_addr))?;

        // 创建本地缓存（KV 读缓存，Registry 缓存已迁移到 RegistryService）
        let agent_cache = AgentCache::new(
            self.config.cache_kv_max_entries,
            self.config.cache_kv_ttl_secs,
            500,  // [已废弃] registry max entries，保留向后兼容
            self.config.cache_catalog_ttl_secs,
        );

        // 若配置了 Server 端点，创建内部 Client 用于请求转发
        let inner = if !self.config.static_peers.is_empty() {
            tracing::info!(
                "coord-agent connecting to server cluster: {:?}",
                self.config.static_peers
            );
            match AgentInner::new(self.config.static_peers.clone(), agent_cache).await {
                Ok(inner) => {
                    tracing::info!("coord-agent connected to server cluster");
                    Some(Arc::new(inner))
                }
                Err(e) => {
                    tracing::warn!("coord-agent failed to connect to server cluster: {e}; running in skeleton mode");
                    None
                }
            }
        } else {
            tracing::warn!("coord-agent: no static_peers configured, running in skeleton mode");
            None
        };

        // Phase D: 初始化可插拔服务框架
        let service_manager = ServiceManager::new(self.config.services.clone());

        // 按配置启用高级服务（Phase E）
        // 保存 RegistryService 的 Arc 句柄，用于后续注册 gRPC RegistryServer
        let mut registry_grpc_svc: Option<Arc<crate::services::registry::RegistryService>> = None;

        if self.config.services.registry {
            if let Some(ref inner) = inner {
                let registry_svc = Arc::new(
                    crate::services::registry::RegistryService::new(
                        inner.clone(),
                        500, // max cached instances
                    )
                );
                registry_grpc_svc = Some(registry_svc.clone());
                if let Err(e) = service_manager.register(registry_svc).await {
                    tracing::error!("failed to register registry service: {e}");
                    registry_grpc_svc = None;
                } else {
                    tracing::info!("Registry service registered (v3.0 pluggable architecture)");
                }
            } else {
                tracing::warn!("Registry service enabled but no server connection; skipping");
            }
        }

        // 保存 ConfigCenterService 的 Arc 句柄，用于后续注册 gRPC ConfigServer
        let mut config_grpc_svc: Option<Arc<crate::services::config_center::ConfigCenterService>> = None;

        if self.config.services.config_center {
            if let Some(ref inner) = inner {
                let config_svc = Arc::new(
                    crate::services::config_center::ConfigCenterService::new(inner.clone())
                );
                config_grpc_svc = Some(config_svc.clone());
                if let Err(e) = service_manager.register(config_svc).await {
                    tracing::error!("failed to register config_center service: {e}");
                    config_grpc_svc = None;
                } else {
                    tracing::info!("ConfigCenter service registered (v3.0 pluggable architecture)");
                }
            } else {
                tracing::warn!("ConfigCenter service enabled but no server connection; skipping");
            }
        }

        if self.config.services.lock {
            if let Some(ref inner) = inner {
                let lock_svc = Arc::new(
                    crate::services::lock::LockService::new(inner.clone())
                );
                if let Err(e) = service_manager.register(lock_svc).await {
                    tracing::error!("failed to register lock service: {e}");
                } else {
                    tracing::info!("Lock service registered (v3.0 pluggable architecture)");
                }
            } else {
                tracing::warn!("Lock service enabled but no server connection; skipping");
            }
        }

        if self.config.services.idgen {
            if let Some(ref inner) = inner {
                let idgen_svc = Arc::new(
                    crate::services::idgen::IdGenService::new(inner.clone(), 1000)
                );
                if let Err(e) = service_manager.register(idgen_svc).await {
                    tracing::error!("failed to register idgen service: {e}");
                } else {
                    tracing::info!("ID Generator service registered (v3.0 pluggable architecture)");
                }
            } else {
                tracing::warn!("ID Generator service enabled but no server connection; skipping");
            }
        }

        if self.config.services.event_notification {
            if let Some(ref inner) = inner {
                let event_svc = Arc::new(
                    crate::services::event_notification::EventNotificationService::new(
                        inner.clone(), 1000, 256,
                    )
                );
                if let Err(e) = service_manager.register(event_svc).await {
                    tracing::error!("failed to register event_notification service: {e}");
                } else {
                    tracing::info!("EventNotification service registered (v3.0 pluggable architecture)");
                }
            } else {
                tracing::warn!("EventNotification service enabled but no server connection; skipping");
            }
        }

        if self.config.services.leader_election {
            if let Some(ref inner) = inner {
                let election_svc = Arc::new(
                    crate::services::leader_election::LeaderElectionService::new(
                        inner.clone(), 256,
                    )
                );
                if let Err(e) = service_manager.register(election_svc).await {
                    tracing::error!("failed to register leader_election service: {e}");
                } else {
                    tracing::info!("LeaderElection service registered (v3.0 pluggable architecture)");
                }
            } else {
                tracing::warn!("LeaderElection service enabled but no server connection; skipping");
            }
        }

        if self.config.services.workflow {
            if let Some(ref inner) = inner {
                let workflow_svc = Arc::new(
                    crate::services::workflow::WorkflowService::new(inner.clone())
                );
                if let Err(e) = service_manager.register(workflow_svc).await {
                    tracing::error!("failed to register workflow service: {e}");
                } else {
                    tracing::info!("Workflow service registered (v3.0 pluggable architecture)");
                }
            } else {
                tracing::warn!("Workflow service enabled but no server connection; skipping");
            }
        }

        // Phase F: 数据面服务（Cache + MQ，基于 redb 本地引擎，无需 Server 连接）
        if self.config.services.cache {
            let data_dir = std::path::PathBuf::from(&self.config.data_dir);
            let cache_svc = Arc::new(
                crate::services::cache::CacheService::new(
                    data_dir.clone(),
                    1024 * 1024 * 1024, // 1GB max
                    3600,               // default TTL 1 hour
                )
            );
            if let Err(e) = service_manager.register(cache_svc).await {
                tracing::error!("failed to register cache service: {e}");
            } else {
                tracing::info!("Cache data-plane service registered (v3.0, redb backend)");
            }
        }

        if self.config.services.mq {
            let data_dir = std::path::PathBuf::from(&self.config.data_dir);
            let mq_svc = Arc::new(
                crate::services::mq::MessageQueueService::new(
                    data_dir.clone(),
                    1024 * 1024 * 1024, // 1GB max
                )
            );
            if let Err(e) = service_manager.register(mq_svc).await {
                tracing::error!("failed to register mq service: {e}");
            } else {
                tracing::info!("MQ data-plane service registered (v3.0, redb backend)");
            }
        }

        // Phase G: 安全策略引擎（本地 RBAC/ABAC，可扩展至 OPA）
        if self.config.services.policy {
            let policy_svc = Arc::new(
                crate::services::policy::PolicyService::new(1024)
            );
            if let Err(e) = service_manager.register(policy_svc).await {
                tracing::error!("failed to register policy service: {e}");
            } else {
                tracing::info!("Policy service registered (v3.0, RBAC/ABAC engine)");
            }
        }

        // 启动所有已启用的可插拔服务
        if let Err(e) = service_manager.start_all().await {
            tracing::error!("failed to start pluggable services: {e}");
        }

        tracing::info!("coord-agent gRPC server listening on {}", self.config.agent_addr);

        // 构建 gRPC router：核心服务 + Registry + Config + 可插拔服务
        let router = tonic::transport::Server::builder()
            .add_service(KvServer::new(KvProxy::new(inner.clone())))
            .add_service(TxnServer::new(TxnProxy::new(inner.clone())))
            .add_service(LeaseServer::new(LeaseProxy::new(inner.clone())))
            .add_service(WatchServer::new(WatchProxy::new(inner.clone())))
            .add_service(MaintenanceServer::new(MaintenanceProxy::new(inner)));

        // 注册 Registry gRPC 服务（若 registry 已启用且成功初始化）
        let router = if let Some(registry_svc) = registry_grpc_svc {
            router.add_service(
                coord_proto::agent::registry_server::RegistryServer::from_arc(registry_svc)
            )
        } else {
            router
        };

        // 注册 Config gRPC 服务（若 config_center 已启用且成功初始化）
        let router = if let Some(config_svc) = config_grpc_svc {
            router.add_service(
                coord_proto::agent::config_server::ConfigServer::from_arc(config_svc)
            )
        } else {
            router
        };

        let router = service_manager.build_grpc_router(router);

        router
            .serve_with_shutdown(addr, shutdown)
            .await?;

        // 优雅停止可插拔服务
        let _ = service_manager.stop_all().await;

        Ok(())
    }
}

// ──── run_agent ────

/// 启动 Agent 守护进程
///
/// 由 `coord agent` 子命令调用。此函数阻塞直到收到终止信号。
pub async fn run_agent(config: AgentConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::health::start_health_server;
    use crate::metrics::AgentMetrics;

    tracing::info!(
        "coord-agent starting on {}, http on {}",
        config.agent_addr,
        config.http_addr
    );

    // C3: 启动 HTTP health/metrics 端点
    let metrics = AgentMetrics::new();
    let has_peers = !config.static_peers.is_empty();
    let _health_handle = start_health_server(&config.http_addr, metrics, has_peers);

    let server = AgentServer::new(config);

    // 启动 gRPC server（带优雅关闭）
    tracing::info!("coord-agent: starting gRPC services (KV/Txn/Lease/Watch/Maintenance)");
    server.serve_with_shutdown(shutdown_signal()).await?;

    Ok(())
}

/// 优雅关闭信号（Ctrl+C / SIGTERM）
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
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

/// 用于测试的 run_agent 类型别名，验证函数签名兼容性
#[doc(hidden)]
pub fn __run_agent_type_check(
    config: AgentConfig,
) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send>> {
    Box::pin(run_agent(config))
}
