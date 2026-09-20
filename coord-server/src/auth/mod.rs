// Auth module — Authentication & RBAC Authorization
//
// Security layers: TLS → Authentication → Authorization (RBAC)
//
// Authentication methods:
// - Simple Token: `Authorization: Bearer <token>`
// - mTLS: Extract identity from client certificate CN
//
// RBAC model: User → Role → Permission (Read/Write/ReadWrite on Key prefix)
//
// Auth lifecycle: default off, enable/disable via AuthEnable/AuthDisable RPC.

pub mod capability;
pub mod interceptor;
pub mod manager;
pub mod revocation;
pub mod service;
pub mod token;
pub mod token_signing;

pub use capability::CapabilityRegistry;
pub use capability::CapabilityRegistryService;
pub use interceptor::ServerAuthInterceptor;
pub use interceptor::ServerAuthLayer;
pub use interceptor::MAX_GRPC_DECODING_BYTES;
pub use interceptor::MAX_SCOPE_BODY_BYTES;
pub use manager::AuthManager;
pub use manager::ROOT_ROLE;
pub use service::AuthService;
pub use token::{AuthToken, TokenManager};

/// Agent 注册引导角色：`Auth.Bootstrap` 签发的 CCT 携带该角色。
///
/// 该角色**不预置任何能力**（`AuthManager` 只内置 `root`）：由 operator 用
/// 管理员身份显式授予 [`AGENT_BOOTSTRAP_CAPABILITY_GRANTS`]，可审计、可撤销。
pub const AGENT_BOOTSTRAP_ROLE: &str = "agent-bootstrap";

/// `agent-bootstrap` 角色的**最小能力集**（能力 id 与内置清单逐字一致）。
///
/// 用途：agent 拿一次性 bootstrap token 换到该角色后，代插件开通受限账户
/// （创建 `plugin/{id}` 用户 + 角色 + 逐能力授权）。
///
/// **刻意不含任何数据面能力**（kv/txn/lease/watch/storage）：引导身份只能建账户与
/// 授权，不能读写协调数据；插件数据面权限由 `plugin/{id}` 账户自己的角色承载。
///
/// A3 补充：`admin:auth:role_list` 是**只读**的元数据读取能力，用于 agent 侧
/// 本地授权缓存（`RoleCache`）的角色映射同步。没有它，agent 无法把 CCT 里的
/// `roles` 解析成能力/scope，本地授权只能全拒。它不授予任何数据面访问。
pub const AGENT_BOOTSTRAP_CAPABILITY_GRANTS: &[(&str, &str)] = &[
    ("admin:auth:user_add", ""),
    ("admin:auth:role_add", ""),
    ("admin:auth:role_grant", ""),
    ("admin:auth:user_grant_role", ""),
    ("admin:auth:role_list", ""),
];

/// **agent 自身身份**的角色名（F-50 修复，2026-09-19）。
///
/// 与 [`AGENT_BOOTSTRAP_ROLE`] 的分工：
/// - `agent-bootstrap`：**代他人开通账户**（插件身份），刻意**无数据面能力**；
/// - `agent-self`：**agent 为自己做事**（锁自动续期、registry 目录加载与订阅、
///   idgen nodeid 注册等后台流量），只有**内部键空间**的数据面能力。
///
/// 角色名与 agent 侧 `coord_agent::plugin::identity::SELF_ROLE` 逐字一致
/// （漂移由 `coord/tests/plugin_credentials_process_test.rs` 断言）。
pub const AGENT_SELF_ROLE: &str = "agent-self-role";

/// **agent 自身维护的内部键空间**（`/_<domain>/…`）。
///
/// 这些键空间**全部由 agent / server 自身构建**（不是用户数据）：调用方的数据键由
/// 调用方在自己的 CCT 下读写，agent **不得**用自身身份代劳。
///
/// 清单来源：`coord-agent/src` 与 `coord-core/src` 中出现的全部 `/_<domain>/` 字面量
/// 前缀。**这不是手工枚举可以可靠维护的东西** —— 因此
/// `coord/tests/plugin_credentials_process_test.rs` 有一条
/// "源码里出现的每个 `/_<domain>/` 都必须被本清单覆盖" 的漂移卡口：
/// 新增内部键空间却忘了登记时，该测试置红，而不是等到运行期以
/// "静默失效"（F-50 的形态）暴露。
pub const AGENT_SELF_KEYSPACES: &[&str] = &[
    "lock",
    "registry",
    "config",
    "idgen",
    "election",
    "workflow",
    "pki",
    "policy",
    "transit",
    "events",
    // 2026-09-19 由漂移卡口抓出（`every_internal_keyspace_in_source_is_granted`）：
    // 前两轮给 `feature_flags` 与 `scheduler` 加了 KV 持久化
    // （`feature_flags_store.rs` 的 `/_featureflags/v1/flag/{key}`、
    //  `scheduler_store.rs` 的任务记录键），**但没同步扩展本清单** ⇒
    // agent 自身身份在这两个键空间上只是一张没有 scope 的令牌，
    // auth 开启时其后台读写会以 `permission denied` **静默失效**（F-50 同型）。
    // 卡口当场置红是对的：它的存在就是为了让"加了键空间忘记登记"不变成运行期幽灵。
    "featureflags",
    "scheduler",
];

/// agent 自身身份在每个内部键空间上需要的**数据面操作**。
///
/// 为什么是这几个（而不是按调用点逐条挑）：
/// - `kv:read`：锁回查、目录/定义全量对账、CA 与策略读取；
/// - `kv:write` / `kv:delete`：CA 落盘、DEK 落盘与清扫、定义写入；
/// - `txn:execute`：idgen nodeid 的 CAS 注册、工作流定义的条件写。
///
/// 逐调用点挑授权**正是 F-50 的成因**（漏一处 ⇒ 静默失效），故改为按
/// "键空间 × 操作" 的封闭规则派生：规则可审查、覆盖面自动完整。
const AGENT_SELF_KEYSPACE_OPERATIONS: &[&str] = &[
    "data:kv:read",
    "data:kv:write",
    "data:kv:delete",
    "data:txn:execute",
];

/// 不按 key 约束的自身身份能力（`(能力, scope)`，scope 恒为空）。
///
/// - `lease:keepalive` / `lease:revoke`：锁与选举的后台自动续期、以及清理；
/// - `watch:subscribe`：registry / config / workflow 的订阅与断线重连
///   （Watch 的资源键在**首帧**里，中间件不预提取，故无法用 scope 约束）。
const AGENT_SELF_UNSCOPED_GRANTS: &[&str] = &[
    "data:lease:keepalive",
    "data:lease:revoke",
    "data:watch:subscribe",
];

/// `agent-self` 角色的最小能力集（**派生**，勿手写枚举）。
///
/// 背景：agent 出站凭据是**任务局部量**，只装"调用方转发进来的 CCT"
/// （`coord-agent/src/auth/interceptor.rs`）。于是有调用方的**代理路径**全通，
/// 而 agent 自己发起的后台流量（无调用方）在 `auth.enabled=true` 下一律
/// `missing CCT token` ⇒ 锁自动续期失败（调用方以为仍持有）、registry 以空目录启动
/// 且永不订阅、idgen nodeid 注册永远失败。这就是 F-50
/// （`jepsen/docs/coord-findings.md:1577`，`confirmed-by-run`），也是此前所有
/// agent 面绿灯的共同盲区。
///
/// **边界**（本清单最重要的性质）：授权范围限定在 [`AGENT_SELF_KEYSPACES`]，
/// 即 agent 自己管理的协调簿记，**不含用户数据键**，也**不含任何 `admin:*`** ——
/// agent 不得用自身身份改动账户/角色（那是 provisioner 的职责）。
///
/// 实测（负向对照）表明自发流量面比结论文档原先列出的四处更宽：除
/// 锁续期 / registry 目录与订阅 / idgen 注册外，还有 **pki CA 自举**、
/// **transit DEK 清扫**、**config 订阅**、**workflow 存储初始化**。
/// 这正是"按调用点枚举必漏"的直接证据。
pub fn agent_self_capability_grants() -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> =
        Vec::with_capacity(AGENT_SELF_KEYSPACES.len() * AGENT_SELF_KEYSPACE_OPERATIONS.len() + 3);
    for ns in AGENT_SELF_KEYSPACES {
        for cap in AGENT_SELF_KEYSPACE_OPERATIONS {
            out.push(((*cap).to_string(), format!("/_{ns}/*")));
        }
    }
    for cap in AGENT_SELF_UNSCOPED_GRANTS {
        out.push(((*cap).to_string(), String::new()));
    }
    out
}

#[cfg(test)]
mod bootstrap_role_tests {
    use super::*;

    /// 引导角色所需能力必须全部是内置能力（否则 operator 无法授予）。
    #[test]
    fn agent_bootstrap_grants_are_builtin_capabilities() {
        let registry = CapabilityRegistry::new();
        registry.bootstrap_builtin();
        for (id, scope) in AGENT_BOOTSTRAP_CAPABILITY_GRANTS {
            assert!(
                registry.get(id).is_some(),
                "{id} must be a built-in capability"
            );
            assert!(scope.is_empty(), "{id} must use an empty scope");
        }
    }

    /// 引导角色不得携带数据面能力（越权面最小化）。
    #[test]
    fn agent_bootstrap_grants_exclude_data_plane() {
        for (id, _) in AGENT_BOOTSTRAP_CAPABILITY_GRANTS {
            assert!(
                !id.starts_with("data:"),
                "{id} must not grant data-plane access"
            );
        }
    }

    // ──── agent-self（F-50） ────

    /// 自身身份的能力必须是**内置**能力（否则 operator / agent 都授不出去）。
    #[test]
    fn agent_self_grants_are_builtin_capabilities() {
        let registry = CapabilityRegistry::new();
        registry.bootstrap_builtin();
        for (id, _) in agent_self_capability_grants() {
            assert!(
                registry.get(&id).is_some(),
                "{id} must be a built-in capability"
            );
        }
    }

    /// 自身身份**不得**含任何 `admin:*` 能力：agent 不能改账户/角色。
    #[test]
    fn agent_self_grants_exclude_admin() {
        for (id, _) in agent_self_capability_grants() {
            assert!(
                !id.starts_with("admin:"),
                "{id} must not grant administrative access"
            );
        }
    }

    /// 数据面能力的 scope 必须落在**内部键空间**（`/_…/*`）或为空（非键资源）。
    ///
    /// 这是"agent 自身身份不得触碰用户数据"这一边界的机器判据：任何一条
    /// 放宽到用户键空间的授权（例如 scope 为空却授 `data:kv:read`）都会在此置红。
    #[test]
    fn agent_self_scopes_stay_inside_internal_keyspace() {
        for (id, scope) in agent_self_capability_grants() {
            if !id.starts_with("data:kv:") && !id.starts_with("data:txn:") {
                // lease / watch 等非按 key 的资源本就无 scope 可约束。
                assert!(
                    scope.is_empty(),
                    "{id} takes no key scope; expected empty, got {scope:?}"
                );
                continue;
            }
            assert!(
                scope.starts_with("/_") && scope.ends_with("/*"),
                "{id} must be scoped to the internal keyspace (/_<domain>/*), got {scope:?}"
            );
            let ns = scope
                .trim_start_matches("/_")
                .trim_end_matches("/*")
                .to_string();
            assert!(
                AGENT_SELF_KEYSPACES.contains(&ns.as_str()),
                "{scope:?} must come from AGENT_SELF_KEYSPACES"
            );
        }
    }

    /// 每个**无调用方**的表面（负向对照实测到的全部）都必须被覆盖 ——
    /// 漏授一处的失败形态是"静默失效"，正是 F-50 要消除的那一类。
    #[test]
    fn agent_self_covers_every_no_caller_surface() {
        let grants = agent_self_capability_grants();
        let has = |cap: &str, scope: &str| grants.iter().any(|(i, s)| i == cap && s == scope);
        // 锁：回查 + 续期 + 撤销
        assert!(has("data:kv:read", "/_lock/*"));
        assert!(has("data:lease:keepalive", ""));
        assert!(has("data:lease:revoke", ""));
        // registry：目录加载 + 订阅
        assert!(has("data:kv:read", "/_registry/*"));
        assert!(has("data:watch:subscribe", ""));
        // idgen：nodeid CAS 注册 + 回查
        assert!(has("data:txn:execute", "/_idgen/*"));
        assert!(has("data:kv:read", "/_idgen/*"));
        // 负向对照实测的额外表面：pki CA 自举 / transit DEK 清扫 / config 订阅 /
        // workflow 存储初始化
        assert!(has("data:kv:read", "/_pki/*"));
        assert!(has("data:kv:write", "/_pki/*"));
        assert!(has("data:kv:read", "/_transit/*"));
        assert!(has("data:kv:delete", "/_transit/*"));
        assert!(has("data:kv:read", "/_config/*"));
        assert!(has("data:kv:read", "/_workflow/*"));
    }
}
