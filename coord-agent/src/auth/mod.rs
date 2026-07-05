// Agent Auth Module — CCT-based authentication & authorization for Agent
//
// The Agent is the security boundary. All business requests must pass Agent
// authorization before being forwarded to the Server.
//
// Components:
// - role_cache: Local Role→Capability mapping cache (5-min sync)
// - interceptor: Auth interceptor for all gRPC requests
// - bootstrap: Agent bootstrap token mechanism
// - circuit_breaker: Auth circuit breaker + Prometheus metrics
// - rate_limiter: IP-based rate limiting for login endpoint (Phase 3.6)
//
// See docs/capability-auth-implementation.md §3, §4, §6.

pub mod bootstrap;
pub mod circuit_breaker;
pub mod interceptor;
pub mod rate_limiter;
pub mod role_cache;
pub mod sync;
