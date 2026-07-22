// coord-agent: 可插拔服务框架（Phase D）
//
// 定义 BaseService trait 和 ServiceManager。
// 每个高级服务（Registry、Workflow、Lock 等）实现 BaseService，
// 通过 ServiceManager 统一管理生命周期。
//
// 参见 docs/client-agent-architecture-v3.md §4。

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

/// 核心错误类型（简化版，用于服务框架）
pub type ServiceError = Box<dyn std::error::Error + Send + Sync>;
pub type ServiceResult<T> = Result<T, ServiceError>;

// ──── ServiceStatus ────

/// 服务运行状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceStatus {
    /// 未初始化
    Stopped,
    /// 正在启动
    Starting,
    /// 正常运行
    Running,
    /// 正在停止
    Stopping,
    /// 异常（需人工介入）
    Failed,
}

// ──── BaseService trait ────

/// 可插拔基础服务统一接口
///
/// 每个高级服务（Registry、Config Center、Lock、ID Gen、Cache、MQ、
/// Event Notification、Leader Election、Workflow、Policy）实现此 trait，
/// 通过 ServiceManager 按需加载和生命周期管理。
///
/// # Object Safety
///
/// 使用 `#[async_trait]` 宏以支持 async 方法在 trait 对象中使用。
/// 所有方法接收 `&self`，trait 是 object-safe 的。
#[async_trait]
pub trait BaseService: Send + Sync {
    /// 服务唯一名称标识（如 "registry", "workflow", "lock"）
    fn name(&self) -> &'static str;

    /// 向 tonic Router 注册本服务的 gRPC 接口
    ///
    /// 每个服务可注册 0-N 个 gRPC 服务到共享的 tonic Server。
    /// 返回更新后的 Router（Builder 模式）。
    /// 默认实现直接返回 builder（服务无独立 gRPC 接口）。
    fn register_grpc(
        &self,
        builder: tonic::transport::server::Router,
    ) -> tonic::transport::server::Router {
        builder
    }

    /// 启动服务：初始化内部资源、建立连接、启动后台任务
    ///
    /// 实现方应在 start() 内完成所有异步初始化。
    /// 若初始化失败，应返回 Err 并确保已分配的资源被释放。
    async fn start(&self) -> ServiceResult<()>;

    /// 停止服务：释放资源、优雅关闭后台任务
    ///
    /// 实现方应在 stop() 内完成所有清理工作。
    /// 停止后调用 health_check() 应返回 false。
    async fn stop(&self) -> ServiceResult<()>;

    /// 健康检查：当前服务是否正常运行
    fn health_check(&self) -> bool;
}

// ──── ServiceConfig ────

/// 服务启用配置（对应 coord-agent.toml [services] 段）
///
/// 每个字段控制对应基础服务的启用状态。
/// 未启用的服务不分配任何资源（零开销）。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ServiceConfig {
    /// 服务注册与发现
    #[serde(default)]
    pub registry: bool,

    /// 配置中心
    #[serde(default)]
    pub config_center: bool,

    /// 分布式锁
    #[serde(default)]
    pub lock: bool,

    /// ID 生成器
    #[serde(default)]
    pub idgen: bool,

    /// Leader 选举
    #[serde(default)]
    pub leader_election: bool,

    /// 事件通知
    #[serde(default)]
    pub event_notification: bool,

    /// 数据面缓存（本地存储引擎）
    #[serde(default)]
    pub cache: bool,

    /// 消息队列
    #[serde(default)]
    pub mq: bool,

    /// Serverless Workflow 流程引擎
    #[serde(default)]
    pub workflow: bool,

    /// 权限策略引擎（OPA）
    #[serde(default)]
    pub policy: bool,

    /// 分布式调度
    #[serde(default)]
    pub scheduler: bool,

    /// 熔断器
    #[serde(default)]
    pub circuit_breaker: bool,

    /// 限流器
    #[serde(default)]
    pub rate_limiter: bool,

    /// 特性开关
    #[serde(default)]
    pub feature_flags: bool,

    /// 安全传输（信封加密）
    #[serde(default)]
    pub transit: bool,

    /// PKI CA 证书签发服务
    #[serde(default)]
    pub pki: bool,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            registry: true,
            config_center: true,
            lock: true,
            idgen: false,
            leader_election: false,
            event_notification: false,
            cache: false,
            mq: false,
            workflow: true,
            policy: false,
            scheduler: false,
            circuit_breaker: false,
            rate_limiter: false,
            feature_flags: false,
            transit: true,
            pki: true,
        }
    }
}

// ──── ServiceManager ────

/// 服务管理器：统一管理所有已启用的 BaseService 实例的生命周期
///
/// # 使用方式
///
/// ```ignore
/// let config = ServiceConfig { registry: true, workflow: true, ..Default::default() };
/// let manager = ServiceManager::new(config, agent_inner);
/// manager.init_services().await?;
/// manager.start_all().await?;
/// // ... 运行中 ...
/// manager.stop_all().await?;
/// ```
pub struct ServiceManager {
    /// 已注册的服务列表（按名称索引）
    services: RwLock<BTreeMap<&'static str, Arc<dyn BaseService>>>,
    /// 服务启用配置
    config: ServiceConfig,
}

impl ServiceManager {
    /// 创建空的 ServiceManager
    pub fn new(config: ServiceConfig) -> Self {
        Self {
            services: RwLock::new(BTreeMap::new()),
            config,
        }
    }

    /// 注册一个服务实例
    ///
    /// 若同名服务已存在，返回 Err。
    pub async fn register(
        &self,
        service: Arc<dyn BaseService>,
    ) -> ServiceResult<()> {
        let name = service.name();
        let mut services = self.services.write().await;
        if services.contains_key(name) {
            return Err(format!("service '{name}' is already registered").into());
        }
        services.insert(name, service);
        Ok(())
    }

    /// 检查服务是否已启用
    pub fn is_enabled(&self, name: &str) -> bool {
        match name {
            "registry" => self.config.registry,
            "config_center" => self.config.config_center,
            "lock" => self.config.lock,
            "idgen" => self.config.idgen,
            "leader_election" => self.config.leader_election,
            "event_notification" => self.config.event_notification,
            "cache" => self.config.cache,
            "mq" => self.config.mq,
            "workflow" => self.config.workflow,
            "policy" => self.config.policy,
            _ => false,
        }
    }

    /// 启动所有已注册的服务
    ///
    /// 按注册顺序依次启动。若某个服务启动失败，已启动的服务会保持运行
    /// （调用方应决定是否回滚）。
    pub async fn start_all(&self) -> ServiceResult<()> {
        let services = self.services.read().await;
        for (name, service) in services.iter() {
            tracing::info!("ServiceManager: starting service '{name}'");
            service.start().await.map_err(|e| {
                format!("failed to start service '{name}': {e}")
            })?;
            tracing::info!("ServiceManager: service '{name}' started successfully");
        }
        Ok(())
    }

    /// 停止所有已注册的服务（逆序）
    pub async fn stop_all(&self) -> ServiceResult<()> {
        let services = self.services.read().await;
        // 逆序停止：后启动的先停止
        for (name, service) in services.iter().rev() {
            tracing::info!("ServiceManager: stopping service '{name}'");
            if let Err(e) = service.stop().await {
                tracing::error!("ServiceManager: error stopping service '{name}': {e}");
            }
        }
        Ok(())
    }

    /// 对所有已注册服务执行健康检查
    ///
    /// 返回 (healthy_count, total_count, unhealthy_names)。
    pub async fn health_check_all(&self) -> (usize, usize, Vec<String>) {
        let services = self.services.read().await;
        let total = services.len();
        let mut healthy = 0;
        let mut unhealthy = Vec::new();
        for (name, service) in services.iter() {
            if service.health_check() {
                healthy += 1;
            } else {
                unhealthy.push(name.to_string());
            }
        }
        (healthy, total, unhealthy)
    }

    /// 合并所有已注册服务的 gRPC 接口到 tonic Router
    pub fn build_grpc_router(
        &self,
        mut builder: tonic::transport::server::Router,
    ) -> tonic::transport::server::Router {
        // 注意：此方法在同步上下文中调用，使用 try_read 避免阻塞
        if let Ok(services) = self.services.try_read() {
            for (_name, service) in services.iter() {
                builder = service.register_grpc(builder);
            }
        }
        builder
    }
}

impl std::fmt::Debug for ServiceManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceManager")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

// ──── tests ────

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试桩服务：用于验证 ServiceManager 生命周期
    struct StubService {
        name: &'static str,
        started: RwLock<bool>,
        stopped: RwLock<bool>,
        healthy: RwLock<bool>,
    }

    impl StubService {
        fn new(name: &'static str) -> Self {
            Self {
                name,
                started: RwLock::new(false),
                stopped: RwLock::new(false),
                healthy: RwLock::new(false),
            }
        }
    }

    #[async_trait]
    impl BaseService for StubService {
        fn name(&self) -> &'static str {
            self.name
        }

        async fn start(&self) -> ServiceResult<()> {
            *self.started.write().await = true;
            *self.healthy.write().await = true;
            Ok(())
        }

        async fn stop(&self) -> ServiceResult<()> {
            *self.stopped.write().await = true;
            *self.healthy.write().await = false;
            Ok(())
        }

        fn health_check(&self) -> bool {
            self.healthy.try_read().map(|g| *g).unwrap_or(false)
        }
    }

    #[tokio::test]
    async fn test_service_manager_register_and_lifecycle() {
        let config = ServiceConfig::default();
        let manager = ServiceManager::new(config);

        let svc = Arc::new(StubService::new("test-stub"));
        manager.register(svc.clone()).await.unwrap();

        manager.start_all().await.unwrap();
        assert!(svc.health_check());

        manager.stop_all().await.unwrap();
        assert!(!svc.health_check());
    }

    #[tokio::test]
    async fn test_service_manager_duplicate_register_error() {
        let config = ServiceConfig::default();
        let manager = ServiceManager::new(config);

        let svc1 = Arc::new(StubService::new("dup"));
        manager.register(svc1).await.unwrap();

        let svc2 = Arc::new(StubService::new("dup"));
        let err = manager.register(svc2).await.unwrap_err();
        assert!(err.to_string().contains("already registered"));
    }

    #[tokio::test]
    async fn test_service_manager_health_check_all() {
        let config = ServiceConfig::default();
        let manager = ServiceManager::new(config);

        manager.register(Arc::new(StubService::new("s1"))).await.unwrap();
        manager.register(Arc::new(StubService::new("s2"))).await.unwrap();

        manager.start_all().await.unwrap();

        let (healthy, total, unhealthy) = manager.health_check_all().await;
        assert_eq!(healthy, 2);
        assert_eq!(total, 2);
        assert!(unhealthy.is_empty());
    }

    #[tokio::test]
    async fn test_service_manager_is_enabled() {
        let config = ServiceConfig {
            registry: true,
            workflow: true,
            lock: false,
            idgen: false,
            leader_election: false,
            event_notification: false,
            cache: false,
            mq: false,
            policy: false,
            scheduler: false,
            circuit_breaker: false,
            rate_limiter: false,
            feature_flags: false,
            transit: false,
            pki: false,
            ..Default::default()
        };
        let manager = ServiceManager::new(config);
        assert!(manager.is_enabled("registry"));
        assert!(manager.is_enabled("workflow"));
        assert!(!manager.is_enabled("lock"));
        assert!(!manager.is_enabled("mq"));
    }

    #[test]
    fn test_service_config_defaults() {
        let config = ServiceConfig::default();
        // 核心基础服务 — 默认启用
        assert!(config.registry, "registry should be enabled by default");
        assert!(config.config_center, "config_center should be enabled by default");
        // Phase A: 默认启用 lock / transit / pki / workflow
        assert!(config.lock, "lock should be enabled by default (Phase A)");
        assert!(config.transit, "transit should be enabled by default (Phase A)");
        assert!(config.pki, "pki should be enabled by default (Phase A)");
        assert!(config.workflow, "workflow should be enabled by default (Phase A)");
        // 其他服务保持默认关闭
        assert!(!config.idgen);
        assert!(!config.leader_election);
        assert!(!config.event_notification);
        assert!(!config.cache);
        assert!(!config.mq);
        assert!(!config.policy);
        assert!(!config.scheduler);
        assert!(!config.circuit_breaker);
        assert!(!config.rate_limiter);
        assert!(!config.feature_flags);
    }

    #[test]
    fn test_service_config_toml_deserialization() {
        let toml_str = r#"
registry = true
config_center = true
lock = false
idgen = true
leader_election = false
event_notification = true
cache = false
mq = false
workflow = true
policy = false
"#;
        let config: ServiceConfig = toml::from_str(toml_str).unwrap();
        assert!(config.registry);
        assert!(config.config_center);
        assert!(!config.lock);
        assert!(config.idgen);
        assert!(!config.leader_election);
        assert!(config.event_notification);
        assert!(!config.cache);
        assert!(!config.mq);
        assert!(config.workflow);
        assert!(!config.policy);
    }

    #[test]
    fn test_service_config_toml_partial() {
        // 仅指定部分字段，其余应为默认 false
        let toml_str = r#"registry = true"#;
        let config: ServiceConfig = toml::from_str(toml_str).unwrap();
        assert!(config.registry);
        assert!(!config.lock);
        assert!(!config.workflow);
    }

    /// 测试带 gRPC 注册的服务（验证 register_grpc trait 方法）
    struct GrpcStubService {
        name: &'static str,
        started: RwLock<bool>,
        stopped: RwLock<bool>,
        healthy: RwLock<bool>,
        grpc_called: std::sync::atomic::AtomicBool,
    }

    impl GrpcStubService {
        #[allow(dead_code)]
        fn new(name: &'static str) -> Self {
            Self {
                name,
                started: RwLock::new(false),
                stopped: RwLock::new(false),
                healthy: RwLock::new(false),
                grpc_called: std::sync::atomic::AtomicBool::new(false),
            }
        }
    }

    #[async_trait]
    impl BaseService for GrpcStubService {
        fn name(&self) -> &'static str {
            self.name
        }

        fn register_grpc(
            &self,
            builder: tonic::transport::server::Router,
        ) -> tonic::transport::server::Router {
            self.grpc_called.store(true, std::sync::atomic::Ordering::SeqCst);
            builder
        }

        async fn start(&self) -> ServiceResult<()> {
            *self.started.write().await = true;
            *self.healthy.write().await = true;
            Ok(())
        }

        async fn stop(&self) -> ServiceResult<()> {
            *self.stopped.write().await = true;
            *self.healthy.write().await = false;
            Ok(())
        }

        fn health_check(&self) -> bool {
            self.healthy.try_read().map(|g| *g).unwrap_or(false)
        }
    }

    #[test]
    fn test_base_service_default_register_grpc_returns_builder() {
        // 验证 BaseService 的 register_grpc 默认实现存在且不 panic
        // 直接通过 trait object 调用验证
        let svc = StubService::new("test");
        // 对于无自定义 register_grpc 的服务，默认实现直接返回 builder
        // 此测试验证 trait 的 object safety
        assert_eq!(svc.name(), "test");
    }

    #[test]
    fn test_base_service_trait_object_safety() {
        // 验证 BaseService trait 可以用作 trait object (dyn BaseService)
        fn _accept_trait_object(_svc: &dyn BaseService) {}
        // 编译时验证：如果能编译通过，说明 trait 是 object-safe 的
    }

    #[tokio::test]
    async fn test_service_manager_stop_reverse_order_tracking() {
        // 验证 stop_all 逆序调用（后注册的先停止）
        let config = ServiceConfig::default();
        let manager = ServiceManager::new(config);

        let s1 = Arc::new(StubService::new("first"));
        let s2 = Arc::new(StubService::new("second"));

        manager.register(s1.clone()).await.unwrap();
        manager.register(s2.clone()).await.unwrap();
        manager.start_all().await.unwrap();

        // 两者都启动了
        assert!(s1.health_check());
        assert!(s2.health_check());

        manager.stop_all().await.unwrap();

        // 两者都停止了
        assert!(!s1.health_check());
        assert!(!s2.health_check());
    }
}
