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
    /// **默认关闭**：跨节点提交原子性未承诺，是显式声明的边界（见 ADR-0002）；
    /// 显式启用即可用。
    #[serde(default)]
    pub cache: bool,

    /// 消息队列
    #[serde(default)]
    pub mq: bool,

    /// Serverless Workflow 流程引擎
    ///
    /// **默认关闭**：持久化/补偿语义的端到端验收未闭合，属未经生产验收的能力面
    /// （见 ADR-0001）；显式启用即可用。
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
    ///
    /// **默认关闭**：启用 **必须**注入 32 字节 KEK 材料（`COORD_TRANSIT_KEK` 或
    /// `<data_dir>/transit-kek.bin`），否则 agent 拒绝启动（fail-closed）——
    /// 不满足「默认开」的前提（见 ADR-0001）。显式启用 + 注入材料才可用。
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
    /// 默认开关口径（见 ADR-0001）：
    ///
    /// **默认关，显式启用即可用**——不得默认开启未经生产验收的能力面。
    /// · `cache` / `workflow`：验收未闭合（cache 跨节点提交非原子 = 边界
    ///   ADR-0002；workflow 持久化 + 补偿语义尚无端到端验收）。
    /// · `transit`：启用必须注入 KEK 材料（fail-closed），不满足默认开的前提。
    /// · `registry` / `config_center` / `lock` / `idgen` / `policy` / `pki`：
    ///   同样缺验收产物 ⇒ 一律默认关；代码默认与 TOML 缺省必须同为 `false`。
    ///   能力面补齐验收产物后若要改回默认开，须新增 ADR 记录新的决策。
    ///
    /// 与 TOML 路径的一致性：所有字段的 `#[serde(default)]` 缺省 ≡ 本 `Default`
    /// 实现，两条路径给出**同一服务集合**——
    /// `test_service_config_toml_missing_fields_match_code_defaults` 逐字段断言。
    fn default() -> Self {
        Self {
            registry: false,
            config_center: false,
            lock: false,
            idgen: false,
            idgen_mode: default_idgen_mode(),
            idgen_node_id: None,
            leader_election: false,
            event_notification: false,
            cache: false,
            mq: false,
            workflow: false,
            policy: false,
            scheduler: false,
            circuit_breaker: false,
            rate_limiter: false,
            feature_flags: false,
            transit: false,
            pki: false,
            replication: false,
        }
    }
}

impl ServiceConfig {
    /// dev 模式（`coord dev`）的内建服务集：**除 `replication` 外全部开启**
    /// （见 ADR-0009）。
    ///
    /// - `transit`：开启，但由 dev 专用默认 KEK 支撑——`coord dev` 经
    ///   `AgentServer::with_dev_default_transit_kek(true)` 在未注入材料时回退到
    ///   内建 dev 默认材料（启动 WARN；生产路径不可达，缺材料仍 fail-closed）。
    /// - `replication` 排除：跨 Agent ISR 复制；dev 是单 Agent 拓扑，
    ///   没有复制对端。
    ///
    /// 这是 `coord dev` 的**显式预设**（进程内构造，不进 serde 面）：
    /// 生产默认口径 `ServiceConfig::default()` / TOML 缺省仍全关（见
    /// ADR-0001），本方法不得被用作任何配置路径的缺省值。
    ///
    /// 字段按穷举方式书写：`ServiceConfig` 新增字段时本构造触发编译错误，
    /// 强制对新服务的 dev 归属做出显式决定（开 / 不开 + 理由）。
    pub fn dev_mode() -> Self {
        Self {
            registry: true,
            config_center: true,
            lock: true,
            idgen: true,
            idgen_mode: default_idgen_mode(),
            idgen_node_id: None,
            leader_election: true,
            event_notification: true,
            cache: true,
            mq: true,
            workflow: true,
            policy: true,
            scheduler: true,
            circuit_breaker: true,
            rate_limiter: true,
            feature_flags: true,
            // dev 默认 KEK 支撑（回退逻辑在 AgentServer，见 ADR-0009）
            transit: true,
            pki: true,
            // 例外集（ADR-0009）：单 Agent 拓扑无复制对端。
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
        // registry / config_center / lock / idgen / policy / pki 必须默认关：
        // 同样缺验收产物，属未经生产验收的能力面（见 ADR-0001）；TOML 缺省本就是 `false`，
        // 两条默认路径从此一致。
        assert!(!config.registry, "registry must NOT be on by default");
        assert!(
            !config.config_center,
            "config_center must NOT be on by default"
        );
        assert!(!config.lock, "lock must NOT be on by default");
        // transit 必须默认关：启用它必须注入 KEK 材料（fail-closed）。
        assert!(
            !config.transit,
            "transit must NOT be on by default: enabling it requires injected KEK material"
        );
        assert!(!config.pki, "pki must NOT be on by default");
        assert!(!config.idgen, "idgen must NOT be on by default");
        assert!(!config.policy, "policy must NOT be on by default");
        // 未经生产验收的能力面**不得默认开启**（见 ADR-0001）：
        // cache / workflow 默认 `false`，显式启用即可用。
        assert!(
            !config.cache,
            "cache must NOT be on by default: cross-node commit atomicity is a declared boundary"
        );
        assert!(
            !config.workflow,
            "workflow must NOT be on by default: persistence/compensation e2e acceptance is open"
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
    /// 否则「配置文件未列出」与「代码默认」两条路径会给出不同的服务集合
    /// （见 ADR-0001）。本测试对**全部字段**逐字段断言。
    #[test]
    fn test_service_config_toml_missing_fields_match_code_defaults() {
        let from_empty_toml: ServiceConfig = toml::from_str("").unwrap();
        let code_default = ServiceConfig::default();
        assert_eq!(from_empty_toml.registry, code_default.registry);
        assert_eq!(from_empty_toml.config_center, code_default.config_center);
        assert_eq!(from_empty_toml.lock, code_default.lock);
        assert_eq!(from_empty_toml.idgen, code_default.idgen);
        assert_eq!(from_empty_toml.idgen_mode, code_default.idgen_mode);
        assert_eq!(from_empty_toml.idgen_node_id, code_default.idgen_node_id);
        assert_eq!(from_empty_toml.cache, code_default.cache);
        assert_eq!(from_empty_toml.workflow, code_default.workflow);
        assert_eq!(from_empty_toml.transit, code_default.transit);
        assert_eq!(from_empty_toml.pki, code_default.pki);
        assert_eq!(from_empty_toml.policy, code_default.policy);
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
        assert_eq!(from_empty_toml.replication, code_default.replication);
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

    /// `dev_mode()` 预设 = 全部内建服务开启 − {replication}（见 ADR-0009）。
    /// 破坏任一侧（漏开一个 / 误开排除项）⇒ 必红；同口径的端到端锚点在
    /// `coord/tests/dev_mode_services_test.rs`（Plugin.List 清单集合相等 +
    /// Registry / Config / Lock / Transit 真实调用）。
    #[test]
    fn test_dev_mode_preset_enables_all_but_replication() {
        let dev = ServiceConfig::dev_mode();

        // ── 开启集：与 README「17 内建服务」清单逐项对应（除下方例外）──
        assert!(dev.registry, "dev must enable registry");
        assert!(dev.config_center, "dev must enable config_center");
        assert!(dev.lock, "dev must enable lock");
        assert!(dev.idgen, "dev must enable idgen");
        assert!(dev.leader_election, "dev must enable leader_election");
        assert!(dev.event_notification, "dev must enable event_notification");
        assert!(dev.cache, "dev must enable cache");
        assert!(dev.mq, "dev must enable mq");
        assert!(dev.workflow, "dev must enable workflow");
        assert!(dev.policy, "dev must enable policy");
        assert!(dev.scheduler, "dev must enable scheduler");
        assert!(dev.circuit_breaker, "dev must enable circuit_breaker");
        assert!(dev.rate_limiter, "dev must enable rate_limiter");
        assert!(dev.feature_flags, "dev must enable feature_flags");
        assert!(
            dev.transit,
            "dev must enable transit (backed by the dev-only default KEK; see ADR-0009)"
        );
        assert!(dev.pki, "dev must enable pki");

        // ── 例外集：dev 单 Agent 拓扑无复制对端（ADR-0009）──
        assert!(
            !dev.replication,
            "dev must NOT enable replication: dev is a single-agent topology"
        );

        // idgen 参数沿用默认（雪花 + 节点 ID 自动派生）：预设只翻转开关，
        // 不动取值类字段。
        assert_eq!(dev.idgen_mode, default_idgen_mode());
        assert_eq!(dev.idgen_node_id, None);

        // 生产默认口径不受本预设影响（另由 test_service_config_defaults 钉住）。
        assert!(!ServiceConfig::default().registry);
    }
}
