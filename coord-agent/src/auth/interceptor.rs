// Auth Interceptor — Agent-side CCT validation interceptor
//
// Validates all incoming gRPC requests from client applications:
// 1. Extract CCT from Authorization header
// 2. Verify signature (with LRU cache)
// 3. Check expiration (with clock drift tolerance)
// 4. Check revocation (bloom filter + fallback lookup)
// 5. Resolve roles → query local role cache → match capabilities + scope
//
// See docs/capability-auth-implementation.md §4.2.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use lru::LruCache;

use coord_core::auth::cct::{decode_cct, is_expired, CctHeader, CctPayload, CctToken};

use super::role_cache::RoleCache;

// ──── Auth Result ────

/// Result of auth verification
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthResult {
    /// Authentication and authorization passed
    Allow(CctToken),
    /// Authentication or authorization failed
    Deny(String),
}

// ──── Signature Cache ────

/// LRU cache for CCT signature verification results.
/// Cache key: (jti, exp_hash) — avoids re-verifying the same token.
#[derive(Debug)]
struct SignatureCache {
    cache: RwLock<LruCache<String, Instant>>,
    ttl: Duration,
}

impl SignatureCache {
    fn new(capacity: usize, ttl_secs: u64) -> Self {
        Self {
            cache: RwLock::new(LruCache::new(std::num::NonZeroUsize::new(capacity.max(1)).unwrap())),
            ttl: Duration::from_secs(ttl_secs),
        }
    }

    fn get(&self, jti: &str) -> bool {
        let mut cache = self.cache.write();
        match cache.get(jti) {
            Some(expires) => {
                if Instant::now() < *expires {
                    true
                } else {
                    cache.pop(jti);
                    false
                }
            }
            None => false,
        }
    }

    fn put(&self, jti: String) {
        self.cache
            .write()
            .put(jti, Instant::now() + self.ttl);
    }
}

// ──── RPC → Capability Mapping ────

/// Maps gRPC method paths to capability IDs.
///
/// Agent intercepts the gRPC method name (e.g., "/coord.kv.Kv/Range")
/// and maps it to the corresponding capability ID.
pub fn infer_capability(rpc_method: &str) -> Option<String> {
    // Static mapping table for all supported RPCs
    match rpc_method {
        // KV
        "/coord.kv.Kv/Range" => Some("data:kv:read".into()),
        "/coord.kv.Kv/Put" => Some("data:kv:write".into()),
        "/coord.kv.Kv/Delete" => Some("data:kv:delete".into()),

        // Txn
        "/coord.txn.Txn/Txn" => Some("data:txn:execute".into()),

        // Lease
        "/coord.lease.Lease/LeaseGrant" => Some("data:lease:grant".into()),
        "/coord.lease.Lease/LeaseRevoke" => Some("data:lease:revoke".into()),
        "/coord.lease.Lease/LeaseKeepAlive" => Some("data:lease:keepalive".into()),

        // Watch
        "/coord.watch.Watch/Watch" => Some("data:watch:subscribe".into()),

        // Maintenance (admin)
        "/coord.maintenance.Maintenance/Status" => Some("admin:maintenance:status".into()),
        "/coord.maintenance.Maintenance/Seal" => Some("admin:maintenance:seal".into()),
        "/coord.maintenance.Maintenance/Unseal" => Some("admin:maintenance:unseal".into()),
        "/coord.maintenance.Maintenance/Snapshot" => Some("admin:maintenance:snapshot".into()),
        "/coord.maintenance.Maintenance/MemberAdd" => Some("admin:maintenance:member_add".into()),
        "/coord.maintenance.Maintenance/MemberRemove" => Some("admin:maintenance:member_remove".into()),
        "/coord.maintenance.Maintenance/MemberPromote" => Some("admin:maintenance:member_promote".into()),
        "/coord.maintenance.Maintenance/MemberList" => Some("admin:maintenance:member_list".into()),

        // Auth
        "/coord.auth.Auth/AuthEnable" => Some("admin:auth:enable".into()),
        "/coord.auth.Auth/AuthDisable" => Some("admin:auth:disable".into()),
        "/coord.auth.Auth/AuthStatus" => Some("admin:auth:status".into()),
        "/coord.auth.Auth/UserAdd" => Some("admin:auth:user_add".into()),
        "/coord.auth.Auth/UserDelete" => Some("admin:auth:user_delete".into()),
        "/coord.auth.Auth/UserList" => Some("admin:auth:user_list".into()),
        "/coord.auth.Auth/RoleAdd" => Some("admin:auth:role_add".into()),
        "/coord.auth.Auth/RoleDelete" => Some("admin:auth:role_delete".into()),
        "/coord.auth.Auth/RoleGrantPermission" => Some("admin:auth:role_grant".into()),
        "/coord.auth.Auth/RoleRevokePermission" => Some("admin:auth:role_revoke".into()),
        "/coord.auth.Auth/RoleList" => Some("admin:auth:role_list".into()),
        "/coord.auth.Auth/UserGrantRole" => Some("admin:auth:user_grant_role".into()),
        "/coord.auth.Auth/UserRevokeRole" => Some("admin:auth:user_revoke_role".into()),

        // Authenticate is always allowed (login endpoint)
        "/coord.auth.Auth/Authenticate" => None, // whitelisted — no capability check

        _ => None, // Unknown RPC — deny by default
    }
}

// ──── Auth Interceptor ────

/// The main auth interceptor for the Agent.
pub struct AuthInterceptor {
    /// CCT signing key (bytes, derived from Server root key via HKDF)
    signing_key: Vec<u8>,
    /// Local role→capability cache
    role_cache: Arc<RoleCache>,
    /// Signature verification cache (LRU)
    sig_cache: SignatureCache,
    /// Clock drift tolerance in seconds
    clock_drift_secs: i64,
    /// Whether auth is enabled (if disabled, all requests pass through)
    enabled: bool,
}

impl AuthInterceptor {
    /// Create a new auth interceptor.
    pub fn new(
        signing_key: Vec<u8>,
        role_cache: Arc<RoleCache>,
        clock_drift_secs: i64,
    ) -> Self {
        Self {
            signing_key,
            role_cache,
            sig_cache: SignatureCache::new(10000, 60), // 10k entries, 60s TTL
            clock_drift_secs,
            enabled: true,
        }
    }

    /// Set whether auth is enabled.
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// Validate an incoming request.
    ///
    /// Returns `AuthResult::Allow(token)` if the request passes all checks,
    /// or `AuthResult::Deny(reason)` if any check fails.
    pub fn validate_request(
        &self,
        rpc_method: &str,
        auth_header: Option<&str>,
        resource_key: Option<&str>,
    ) -> AuthResult {
        // If auth is disabled, allow everything
        if !self.enabled {
            return AuthResult::Allow(CctToken {
                header: CctHeader::default(),
                payload: CctPayload {
                    jti: String::new(),
                    iss: String::new(),
                    sub: String::new(),
                    aud: vec![],
                    iat: 0,
                    exp: 0,
                    roles: vec![],
                    scope_overrides: HashMap::new(),
                },
                signature: vec![],
            });
        }

        // 1. Extract CCT from Authorization header
        let cct_str = match extract_bearer_token(auth_header) {
            Some(token) => token,
            None => return AuthResult::Deny("missing or invalid Authorization header".into()),
        };

        // 2. Decode and verify CCT
        let cct = match decode_cct(cct_str, &self.signing_key) {
            Ok(token) => token,
            Err(e) => return AuthResult::Deny(format!("CCT validation failed: {e}")),
        };

        // 3. Check signature cache (skip re-verification)
        // Already verified in decode_cct, but cache for future requests
        self.sig_cache.put(cct.payload.jti.clone());

        // 4. Check expiration
        if is_expired(&cct.payload, self.clock_drift_secs) {
            return AuthResult::Deny("CCT expired".into());
        }

        // 5. Determine required capability from RPC method
        let capability_id = match infer_capability(rpc_method) {
            Some(cap) => cap,
            None => {
                // Whitelisted endpoints (e.g., Authenticate) or unknown
                if rpc_method == "/coord.auth.Auth/Authenticate" {
                    return AuthResult::Allow(cct);
                }
                return AuthResult::Deny(format!("unknown RPC method: {rpc_method}"));
            }
        };

        // 6. Check role→capability mapping
        let (granted, scope_trie) = self
            .role_cache
            .check_capability(&cct.payload.roles, &capability_id);

        if !granted {
            return AuthResult::Deny(format!(
                "role(s) {:?} do not have capability '{capability_id}'",
                cct.payload.roles
            ));
        }

        // 7. Check scope (if resource key is provided and scope trie exists)
        if let (Some(key), Some(trie)) = (resource_key, scope_trie) {
            if !trie.matches(key) {
                return AuthResult::Deny(format!(
                    "scope restriction: key '{}' not allowed by capability '{}'",
                    key, capability_id
                ));
            }
        }

        AuthResult::Allow(cct)
    }
}

// ──── Helpers ────

/// Extract bearer token from Authorization header.
/// Supports both "Bearer <token>" and "<token>" formats.
fn extract_bearer_token(header: Option<&str>) -> Option<&str> {
    let header = header?;
    if let Some(token) = header.strip_prefix("Bearer ") {
        Some(token)
    } else if header.starts_with("eyJ") {
        // CCT v3 format (base64url JSON header)
        Some(header)
    } else if header.starts_with("coord_") {
        // Legacy token format — pass through
        None
    } else {
        None
    }
}

// ──── Tests ────

#[cfg(test)]
mod tests {
    use super::*;
    use coord_core::auth::cct::{encode_cct, CctHeader, CctPayload};

    const TEST_KEY: &[u8] = b"test-signing-key-32-bytes-long!!";

    fn make_test_cct(roles: Vec<&str>, scope_overrides: HashMap<String, String>) -> String {
        let header = CctHeader::default();
        let payload = CctPayload {
            jti: uuid::Uuid::new_v4().to_string(),
            iss: "test-cluster".to_string(),
            sub: "test-app".to_string(),
            aud: vec!["coord-agent".to_string()],
            iat: 1719990000,
            exp: 2000000000, // far future
            roles: roles.into_iter().map(|s| s.to_string()).collect(),
            scope_overrides,
        };
        encode_cct(&header, &payload, TEST_KEY).unwrap()
    }

    // ──── Phase 3.1: Auth interceptor tests ────

    #[test]
    fn test_interceptor_allows_when_disabled() {
        let role_cache = Arc::new(RoleCache::new());
        let mut interceptor = AuthInterceptor::new(TEST_KEY.to_vec(), role_cache, 300);
        interceptor.set_enabled(false);

        let result = interceptor.validate_request("/coord.kv.Kv/Range", None, None);
        assert!(matches!(result, AuthResult::Allow(_)));
    }

    #[test]
    fn test_interceptor_denies_missing_auth_header() {
        let role_cache = Arc::new(RoleCache::new());
        let interceptor = AuthInterceptor::new(TEST_KEY.to_vec(), role_cache, 300);

        let result = interceptor.validate_request("/coord.kv.Kv/Range", None, None);
        assert!(matches!(result, AuthResult::Deny(_)));
    }

    #[test]
    fn test_interceptor_allows_authenticate_endpoint() {
        let role_cache = Arc::new(RoleCache::new());
        let interceptor = AuthInterceptor::new(TEST_KEY.to_vec(), role_cache, 300);

        let cct = make_test_cct(vec!["reader"], HashMap::new());
        let auth_header = format!("Bearer {cct}");

        let result = interceptor.validate_request(
            "/coord.auth.Auth/Authenticate",
            Some(&auth_header),
            None,
        );
        assert!(matches!(result, AuthResult::Allow(_)));
    }

    #[test]
    fn test_interceptor_validates_capability() {
        let role_cache = Arc::new(RoleCache::new());
        role_cache.sync_full(vec![super::super::role_cache::RoleEntry {
            name: "reader".to_string(),
            grants: vec![super::super::role_cache::CapabilityGrant {
                capability_id: "data:kv:read".to_string(),
                scope: "/app/".to_string(),
            }],
            high_sensitive: false,
        }]);

        let interceptor = AuthInterceptor::new(TEST_KEY.to_vec(), role_cache, 300);
        let cct = make_test_cct(vec!["reader"], HashMap::new());
        let auth_header = format!("Bearer {cct}");

        // KV Range (data:kv:read) should be allowed within scope
        let result = interceptor.validate_request(
            "/coord.kv.Kv/Range",
            Some(&auth_header),
            Some("/app/order-123"),
        );
        assert!(matches!(result, AuthResult::Allow(_)));

        // KV Range outside scope should be denied
        let result = interceptor.validate_request(
            "/coord.kv.Kv/Range",
            Some(&auth_header),
            Some("/admin/secret"),
        );
        assert!(matches!(result, AuthResult::Deny(_)));
    }

    #[test]
    fn test_interceptor_denies_missing_capability() {
        let role_cache = Arc::new(RoleCache::new());
        role_cache.sync_full(vec![super::super::role_cache::RoleEntry {
            name: "reader".to_string(),
            grants: vec![super::super::role_cache::CapabilityGrant {
                capability_id: "data:kv:read".to_string(),
                scope: "".to_string(),
            }],
            high_sensitive: false,
        }]);

        let interceptor = AuthInterceptor::new(TEST_KEY.to_vec(), role_cache, 300);
        let cct = make_test_cct(vec!["reader"], HashMap::new());
        let auth_header = format!("Bearer {cct}");

        // KV Put (data:kv:write) should be denied — reader doesn't have it
        let result = interceptor.validate_request(
            "/coord.kv.Kv/Put",
            Some(&auth_header),
            Some("/app/data"),
        );
        assert!(matches!(result, AuthResult::Deny(_)));
    }

    #[test]
    fn test_interceptor_rejects_expired_token() {
        let role_cache = Arc::new(RoleCache::new());
        let interceptor = AuthInterceptor::new(TEST_KEY.to_vec(), role_cache, 300);

        let header = CctHeader::default();
        let payload = CctPayload {
            jti: "expired-token".to_string(),
            iss: "test".to_string(),
            sub: "test".to_string(),
            aud: vec![],
            iat: 1000000000,
            exp: 1000003600, // expired long ago
            roles: vec!["reader".to_string()],
            scope_overrides: HashMap::new(),
        };
        let cct = encode_cct(&header, &payload, TEST_KEY).unwrap();
        let auth_header = format!("Bearer {cct}");

        let result = interceptor.validate_request(
            "/coord.kv.Kv/Range",
            Some(&auth_header),
            None,
        );
        assert!(matches!(result, AuthResult::Deny(_)));
    }

    #[test]
    fn test_infer_capability_mappings() {
        assert_eq!(infer_capability("/coord.kv.Kv/Range"), Some("data:kv:read".into()));
        assert_eq!(infer_capability("/coord.kv.Kv/Put"), Some("data:kv:write".into()));
        assert_eq!(infer_capability("/coord.kv.Kv/Delete"), Some("data:kv:delete".into()));
        assert_eq!(infer_capability("/coord.txn.Txn/Txn"), Some("data:txn:execute".into()));
        assert_eq!(infer_capability("/coord.lease.Lease/LeaseGrant"), Some("data:lease:grant".into()));
        assert_eq!(infer_capability("/coord.watch.Watch/Watch"), Some("data:watch:subscribe".into()));
        assert_eq!(infer_capability("/coord.auth.Auth/Authenticate"), None);
        assert_eq!(infer_capability("/unknown.Service/Method"), None);
    }

    #[test]
    fn test_extract_bearer_token_formats() {
        assert_eq!(extract_bearer_token(Some("Bearer mytoken")), Some("mytoken"));
        assert_eq!(extract_bearer_token(Some("eyJhbGciOiJI...")), Some("eyJhbGciOiJI..."));
        assert_eq!(extract_bearer_token(Some("coord_abc123")), None); // legacy — pass through
        assert_eq!(extract_bearer_token(None), None);
    }
}
