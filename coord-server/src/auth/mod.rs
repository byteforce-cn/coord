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
pub use interceptor::MAX_GRPC_DECODING_BYTES;
pub use interceptor::MAX_SCOPE_BODY_BYTES;
pub use interceptor::ServerAuthInterceptor;
pub use interceptor::ServerAuthLayer;
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
}
