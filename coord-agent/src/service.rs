// coord-agent: 服务契约（`BaseService`）
//
// 原生服务（Registry、Workflow、Lock、Cache、MQ ...）实现 `BaseService`，由
// **`PluginManager`** 统一托管：`PluginManager` 把它们包装成内建插件
// （`NativePluginAdapter`），生命周期（init/start/stop）、gRPC 服务面
// （`Plugin::grpc_service`）与健康检查三件事都走同一条路径。
//
// 历史（已删除）：曾有一个并行的 `ServiceManager` 注册表，加上 `lib.rs` 里硬编码的
// `add_optional_service` 长链——同一批服务存在三份清单（ServiceManager 注册表 /
// 路由链 / 插件注册表），且 `BaseService::register_grpc` 因 `Router<L>` 的泛型层类型
// 无法 object-safe 而永远是 no-op。两者均已移除：注册表统一为 `PluginManager`，
// gRPC 面由 `Plugin::grpc_service()` + `plugin/grpc.rs` 表达。

use async_trait::async_trait;

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
/// Event Notification、Leader Election、Workflow、Policy ...）实现此 trait，
/// 由 `PluginManager` 包装为内建插件并按需加载、统一托管生命周期。
///
/// 本 trait **不**声明 gRPC 注册方法：`tonic::transport::server::Router<L>` 的层类型
/// `L` 使这种签名无法 object-safe（这正是历史上 `register_grpc` 恒为 no-op 的原因）。
/// 服务的 gRPC 面改由插件 SPI 表达——`Plugin::grpc_service()` 返回类型擦除的
/// [`crate::plugin::AgentGrpcService`]，由 `PluginManager::build_grpc_router` 组装。
///
/// # Object Safety
///
/// 使用 `#[async_trait]` 宏以支持 async 方法在 trait 对象中使用。
/// 所有方法接收 `&self`，trait 是 object-safe 的。
#[async_trait]
pub trait BaseService: Send + Sync {
    /// 服务唯一名称标识（如 "registry", "workflow", "lock"）
    fn name(&self) -> &'static str;

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

    /// ID 生成器实现模式："snowflake"（默认，nodeid 雪花）| "segment"（KV 号段，opt-in）
    #[serde(default = "default_idgen_mode")]
    pub idgen_mode: String,

    /// 雪花节点 ID 显式覆盖（0-1023）；为空时按主机名派生 + Server 注册
    #[serde(default)]
    pub idgen_node_id: Option<u64>,

    /// Leader 选举
    #[serde(default)]
    pub leader_election: bool,

    /// 事件通知
    #[serde(default)]
    pub event_notification: bool,

    /// 数据面缓存（本地存储引擎）
    ///
    /// **默认关闭**（2026-09-21 裁定，见 `Default for ServiceConfig` 注释）：
    /// 跨节点提交原子性仍是「显式声明的边界」（B-07），故不默认对外。
    #[serde(default)]
    pub cache: bool,

    /// 消息队列
    #[serde(default)]
    pub mq: bool,

    /// Serverless Workflow 流程引擎
    ///
    /// **默认关闭**（2026-09-21 裁定）：持久化/补偿语义的端到端验收未闭合，
    /// 故不默认开启未整改面（G9）。
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

    /// 跨 Agent ISR 数据复制（Cache/MQ 数据面高可用；默认关闭 = 单 agent 零破坏）
    #[serde(default)]
    pub replication: bool,
}

impl Default for ServiceConfig {
    /// 默认开关口径（2026-09-21 裁定，`production-readiness-plan-2026-09-21.md` §8 U-03 / W0-5）：
    ///
    /// **默认关，显式启用即可用**（承诺面与未整改面一致）——不得"默认开启未整改面"（G9）。
    /// 因此 `cache` / `workflow` 由 `true` 改为 `false`：两者的整改项未闭合
    /// （cache 跨节点提交非原子 = 已声明边界 B-07；workflow 持久化 + 补偿语义尚无端到端验收）。
    /// `transit` 保持 `true` —— 其整改项（DEK 持久化）已在 2026-09-19 闭合，取"启用即可用"。
    ///
    /// 与 TOML 路径的一致性：各字段为 `#[serde(default)]`（缺省 = `bool::default()` = `false`），
    /// 即配置文件未列出的服务本就是关的；本次改动让 `ServiceConfig::default()`（代码默认，
    /// 如 `dev` 模式与不带 `--agent-config` 启动）与之一致，不再有"只写代码就默认开着"的口子。
    fn default() -> Self {
        Self {
            registry: true,
            config_center: true,
            lock: true,
            idgen: true,
            idgen_mode: default_idgen_mode(),
            idgen_node_id: None,
            leader_election: false,
            event_notification: false,
            cache: false,
            mq: false,
            workflow: false,
            policy: true,
            scheduler: false,
            circuit_breaker: false,
            rate_limiter: false,
            feature_flags: false,
            transit: true,
            pki: true,
            replication: false,
        }
    }
}

// ──── ServiceConfig 辅助 ────

/// ID 生成器默认实现模式（雪花，决策）
fn default_idgen_mode() -> String {
    "snowflake".to_string()
}

// ──── tests ────
//
// 注册表/生命周期/健康检查的行为测试已随 `ServiceManager` 一起迁到
// `plugin::PluginManager`（`plugin/mod.rs` 与 `plugin/native.rs` 的单测、
// 以及 `tests/agent_plugin_grpc_test.rs` 的端到端断言）。
// 本模块只保留服务契约自身（trait object 安全 + 配置解析）的断言。

#[cfg(test)]
mod tests {
    use super::*;

    struct StubService;

    #[async_trait]
    impl BaseService for StubService {
        fn name(&self) -> &'static str {
            "stub"
        }

        async fn start(&self) -> ServiceResult<()> {
            Ok(())
        }

        async fn stop(&self) -> ServiceResult<()> {
            Ok(())
        }

        fn health_check(&self) -> bool {
            true
        }
    }

    #[test]
    fn test_base_service_trait_object_safety() {
        // 验证 BaseService 可用作 trait object（`Arc<dyn BaseService>`）：
        // 内建插件的 gRPC 面与生命周期都建立在这一点上。
        fn _accept_trait_object(_svc: &dyn BaseService) {}
        fn _accept_arc(_svc: std::sync::Arc<dyn BaseService>) {}
        assert_eq!(StubService.name(), "stub");
    }

    #[test]
    fn test_service_config_defaults() {
        let config = ServiceConfig::default();
        // 核心基础服务 — 默认启用
        assert!(config.registry, "registry should be enabled by default");
        assert!(
            config.config_center,
            "config_center should be enabled by default"
        );
        // 默认启用 lock / transit / pki
        assert!(config.lock, "lock should be enabled by default (Phase A)");
        assert!(
            config.transit,
            "transit should be enabled by default (Phase A; remediation closed 2026-09-19)"
        );
        assert!(config.pki, "pki should be enabled by default (Phase A)");
        // IdGen 为数据面服务 — 默认启用（无 Server 时本地雪花降级）
        assert!(
            config.idgen,
            "idgen should be enabled by default (data-plane)"
        );
        // policy 为数据面服务（RBAC 本地内存 + OPA bundle 走 KV）
        assert!(
            config.policy,
            "policy should be enabled by default (data-plane)"
        );
        // 2026-09-21 裁定（W0-5 / U-03）：未整改面**不得默认开启** ⇒
        // cache / workflow 由默认 `true` 改为默认 `false`，显式启用即可用。
        assert!(
            !config.cache,
            "cache must NOT be on by default: cross-node commit atomicity is a declared boundary (B-07)"
        );
        assert!(
            !config.workflow,
            "workflow must NOT be on by default: persistence/compensation e2e acceptance is open (G9)"
        );
        // 其他服务保持默认关闭
        assert!(!config.leader_election);
        assert!(!config.event_notification);
        assert!(!config.mq);
        assert!(!config.scheduler);
        assert!(!config.circuit_breaker);
        assert!(!config.rate_limiter);
        assert!(!config.feature_flags);
    }

    /// 逐字段 `#[serde(default)]` 的缺省与 `Default` impl 必须一致：
    /// 否则「配置文件未列出」与「代码默认」两条路径会给出不同的服务集合。
    #[test]
    fn test_service_config_toml_missing_fields_match_code_defaults() {
        let from_empty_toml: ServiceConfig = toml::from_str("idgen = true\n").unwrap();
        let code_default = ServiceConfig::default();
        assert_eq!(from_empty_toml.cache, code_default.cache);
        assert_eq!(from_empty_toml.workflow, code_default.workflow);
        assert_eq!(from_empty_toml.mq, code_default.mq);
        assert_eq!(from_empty_toml.scheduler, code_default.scheduler);
        assert_eq!(
            from_empty_toml.leader_election,
            code_default.leader_election
        );
        assert_eq!(
            from_empty_toml.event_notification,
            code_default.event_notification
        );
        assert_eq!(
            from_empty_toml.circuit_breaker,
            code_default.circuit_breaker
        );
        assert_eq!(from_empty_toml.rate_limiter, code_default.rate_limiter);
        assert_eq!(from_empty_toml.feature_flags, code_default.feature_flags);
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
}
