// Auth module — Authentication & RBAC Authorization (ADP §14)
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

pub mod manager;
pub mod token;
pub mod service;

pub use manager::AuthManager;
pub use token::{TokenManager, AuthToken};
pub use service::AuthService;
