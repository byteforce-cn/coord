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
    ///
    /// **默认关闭**（2026-09-22 裁定，见 §8.5 U-11）：U-04 落地后，启用 transit
    /// **必须**注入 32 字节 KEK 材料（`COORD_TRANSIT_KEK` 或
    /// `<data_dir>/transit-kek.bin`），否则 agent 拒绝启动（fail-closed）。
    /// 因此它不再满足 U-03「启用即可用」的条件 —— 按 G9「不得默认开启未整改面」，
    /// 默认改为 `false`，显式启用 + 注入材料才可用。
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
    /// 默认开关口径（2026-09-21 裁定 U-03 / W0-5；**2026-09-24 由 U-12 收口**，见
    /// `production-readiness-plan-2026-09-21.md` §8.6）：
    ///
    /// **默认关，显式启用即可用**——不得"默认开启未整改面"（G9）。
    /// · `cache` / `workflow`（U-03）：整改项未闭合（cache 跨节点提交非原子 =
    ///   已声明边界 B-07；workflow 持久化 + 补偿语义尚无端到端验收）。
    /// · `transit`（U-11）：启用必须注入 KEK 材料（fail-closed），不再满足「启用即可用」。
    /// · `registry` / `config_center` / `lock` / `idgen` / `policy` / `pki`（U-12）：
    ///   U-03 之后的遗留 —— 它们的**代码默认**曾是 `true`，而字段是普通
    ///   `#[serde(default)]`（配置文件路径缺省 `false`）⇒「走不走 `--agent-config`」
    ///   会得到**不同的服务集合**（§3 D-19）。U-12 取「代码默认改 `false`」一侧：
    ///   六个面的 G8（soak 覆盖）都没有产物 ⇒ 同属未整改面，按 G9 一律默认关。
    ///   能力面随 G1–G9 逐个转绿之后，若要改回默认开，须走新的裁定（记入 §8.6）。
    ///
    /// 与 TOML 路径的一致性（U-12 的验收判据）：所有字段的 `#[serde(default)]` 缺省
    /// ≡ 本 `Default` 实现，两条路径给出**同一服务集合**——
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
        // 2026-09-24 裁定（U-12，见计划 §8.6）：registry / config_center / lock /
        // idgen / policy / pki 六项由默认 `true` 改为 `false` —— 它们的 G8（soak 覆盖）
        // 尚无产物，同属"未整改面" ⇒ 按 G9「不得默认开启未整改面」。
        // 这同时消掉了 §3 D-19：TOML 缺省本来就是 `false`，两条路径从此一致。
        assert!(
            !config.registry,
            "registry must NOT be on by default (U-12/G9)"
        );
        assert!(
            !config.config_center,
            "config_center must NOT be on by default (U-12/G9)"
        );
        assert!(!config.lock, "lock must NOT be on by default (U-12/G9)");
        // 2026-09-22 裁定（U-11）：transit 由默认 `true` 改为 `false` ——
        // U-04 落地后启用它必须注入 KEK 材料（fail-closed），不再满足"启用即可用"。
        assert!(
            !config.transit,
            "transit must NOT be on by default: enabling it requires injected KEK material (U-04/U-11)"
        );
        assert!(!config.pki, "pki must NOT be on by default (U-12/G9)");
        assert!(!config.idgen, "idgen must NOT be on by default (U-12/G9)");
        assert!(!config.policy, "policy must NOT be on by default (U-12/G9)");
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
    ///
    /// 2026-09-24（U-12）：本测试从"部分字段"扩到**全部字段** —— 它是 D-19 的
    /// 验收判据。旧版**刻意不比** `registry` / `config_center` / `lock` / `idgen` /
    /// `policy` / `pki` 六项，因为它们**真的不一致**（代码 `true` vs TOML 缺省
    /// `false`）；U-12 把代码默认改为 `false` 后，一致性变成可断言的事实。
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
        // 2026-09-22 新增：`transit` 也纳入比对（当时改了它的默认值）。
        assert_eq!(from_empty_toml.transit, code_default.transit);
        // 2026-09-24（U-12）：`pki` 与上面五项一并纳入比对（D-19 收口）。
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
}
