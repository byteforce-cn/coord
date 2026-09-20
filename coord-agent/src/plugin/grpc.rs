// coord-agent: 插件 gRPC 服务面（类型擦除注册表）
//
// 背景：`BaseService::register_grpc` 曾以 `Router` 泛型入参表达「服务注册自己的
// gRPC 接口」，但 `Router<L>` 的层类型 `L`（明文 `Identity` / TLS `TlsAcceptor`）
// 使该签名无法 object-safe —— `Arc<dyn BaseService>` 存不进注册表，于是历史上
// 全部实现都是 no-op，真实注册散落在 `lib.rs` 的 40 行 `add_optional_service` 硬编码链，
// 服务生命周期（ServiceManager）与 gRPC 注册（硬编码链）成为**两套并行清单**。
//
// 本模块消除第二套清单：把「服务句柄」做成语义等价的类型擦除枚举
// [`AgentGrpcService`]，注册动作收敛为 `add_to<L>`（`L` 只在**调用点**确定，
// 因此对 `Plugin` SPI 保持 object-safe）。插件通过
// `Plugin::grpc_service()` 暴露自己的服务面，`PluginManager::build_grpc_router`
// 动态组装 —— 新增/移除服务不再需要改路由组装代码。

use std::sync::Arc;

/// 可注册进 agent gRPC 服务链的原生服务句柄（类型擦除）。
///
/// 每个变体持有一个 `Arc<具体服务>`，`add_to` 负责按变体选择对应的
/// tonic `*Server` 包装 —— 与旧 `build_agent_grpc_router` 中的
/// `add_optional_service(...::from_arc)` 逐条一一对应（语义零变更）。
#[derive(Clone)]
pub enum AgentGrpcService {
    Registry(Arc<crate::services::registry::RegistryService>),
    ConfigCenter(Arc<crate::services::config_center::ConfigCenterService>),
    Lock(Arc<crate::services::lock::LockService>),
    IdGen(Arc<crate::services::idgen::IdGenService>),
    LeaderElection(Arc<crate::services::leader_election::LeaderElectionService>),
    EventNotification(Arc<crate::services::event_notification::EventNotificationService>),
    Cache(Arc<crate::services::cache::CacheService>),
    Mq(Arc<crate::services::mq::MessageQueueService>),
    Replication(Arc<crate::services::grpc_handlers::ReplicaRouter>),
    Scheduler(Arc<crate::services::scheduler::SchedulerService>),
    Workflow(Arc<crate::services::workflow::phase4::WorkflowEngineService>),
    Policy(Arc<crate::services::policy::PolicyService>),
    Transit(Arc<crate::services::transit::TransitService>),
    CircuitBreaker(Arc<crate::services::circuit_breaker::CircuitBreakerService>),
    RateLimiter(Arc<crate::services::rate_limiter::RateLimiterService>),
    FeatureFlags(Arc<crate::feature_flags::FeatureFlagService>),
    Pki(Arc<crate::pki::PkiService>),
    /// 协议版本协商（`coord.agent.Handshake`）—— P0-4 / D6：此前两端皆空头
    Handshake(Arc<crate::services::handshake::HandshakeService>),
    /// 插件引擎自身的通用调用面（`coord.plugin.Plugin`）
    PluginApi(Arc<crate::plugin::PluginService>),
}

impl AgentGrpcService {
    /// 供日志/诊断使用的稳定名称（与 gRPC 服务名无关，仅用于可读性）。
    pub fn name(&self) -> &'static str {
        match self {
            Self::Registry(_) => "registry",
            Self::ConfigCenter(_) => "config_center",
            Self::Lock(_) => "lock",
            Self::IdGen(_) => "idgen",
            Self::LeaderElection(_) => "leader_election",
            Self::EventNotification(_) => "event_notification",
            Self::Cache(_) => "cache",
            Self::Mq(_) => "mq",
            Self::Replication(_) => "replication",
            Self::Scheduler(_) => "scheduler",
            Self::Workflow(_) => "workflow",
            Self::Policy(_) => "policy",
            Self::Transit(_) => "transit",
            Self::CircuitBreaker(_) => "circuit_breaker",
            Self::RateLimiter(_) => "rate_limiter",
            Self::FeatureFlags(_) => "feature_flags",
            Self::Pki(_) => "pki",
            Self::Handshake(_) => "handshake",
            Self::PluginApi(_) => "plugin_api",
        }
    }

    /// 把本服务加入 tonic 服务链（`L` = 明文/TLS 层类型，由调用点决定）。
    pub fn add_to<L>(
        self,
        router: tonic::transport::server::Router<L>,
    ) -> tonic::transport::server::Router<L> {
        use coord_proto::agent as proto;
        match self {
            Self::Registry(s) => {
                router.add_service(proto::registry_server::RegistryServer::from_arc(s))
            }
            Self::ConfigCenter(s) => {
                router.add_service(proto::config_server::ConfigServer::from_arc(s))
            }
            Self::Lock(s) => router.add_service(proto::lock_server::LockServer::from_arc(s)),
            Self::IdGen(s) => router.add_service(proto::id_gen_server::IdGenServer::from_arc(s)),
            Self::LeaderElection(s) => {
                router.add_service(proto::leader_election_server::LeaderElectionServer::from_arc(s))
            }
            Self::EventNotification(s) => {
                router.add_service(proto::event_server::EventServer::from_arc(s))
            }
            Self::Cache(s) => router.add_service(proto::cache_server::CacheServer::from_arc(s)),
            Self::Mq(s) => router.add_service(proto::mq_server::MqServer::from_arc(s)),
            Self::Replication(s) => {
                router.add_service(proto::replica_server::ReplicaServer::from_arc(s))
            }
            Self::Scheduler(s) => {
                router.add_service(proto::scheduler_server::SchedulerServer::from_arc(s))
            }
            Self::Workflow(s) => {
                router.add_service(proto::workflow_server::WorkflowServer::from_arc(s))
            }
            Self::Policy(s) => router.add_service(proto::policy_server::PolicyServer::from_arc(s)),
            Self::Transit(s) => {
                router.add_service(proto::transit_server::TransitServer::from_arc(s))
            }
            Self::CircuitBreaker(s) => {
                router.add_service(proto::circuit_breaker_server::CircuitBreakerServer::from_arc(s))
            }
            Self::RateLimiter(s) => {
                router.add_service(proto::rate_limiter_server::RateLimiterServer::from_arc(s))
            }
            Self::FeatureFlags(s) => {
                router.add_service(proto::feature_flags_server::FeatureFlagsServer::from_arc(s))
            }
            Self::Pki(s) => router.add_service(proto::pki_server::PkiServer::from_arc(s)),
            Self::Handshake(s) => {
                router.add_service(proto::handshake_server::HandshakeServer::from_arc(s))
            }
            Self::PluginApi(s) => router.add_service(
                coord_proto::plugin::plugin_server::PluginServer::from_arc(s),
            ),
        }
    }
}

impl std::fmt::Debug for AgentGrpcService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AgentGrpcService({})", self.name())
    }
}
