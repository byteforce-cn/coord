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
pub use manager::AuthManager;
pub use service::AuthService;
pub use token::{AuthToken, TokenManager};
