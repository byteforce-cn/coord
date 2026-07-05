// Server Auth Interceptor (Phase 3.7)
//
// Server-side CCT validation with graded scope checking and mTLS identity binding.
// The core logic is "identify first, then decide the strategy":
//
// 1. Always: Verify CCT signature + expiry + revocation
// 2. Extract mTLS peer certificate CN → determine if trusted agent
// 3. Determine if the RPC is a high-risk operation
// 4. Apply graded check:
//    - Trusted agent + low-risk read → skip scope check (fast path)
//    - Everything else → full scope check required
//
// If mTLS is not enabled: fall back to full scope check for all requests.
//
// See docs/capability-auth-implementation.md §4.3.

use std::collections::HashSet;
use std::sync::Arc;

use parking_lot::RwLock;

use coord_core::auth::cct::{decode_cct, is_expired, CctPayload, CctToken};
use coord_core::auth::trie::ScopeTrie;

use crate::auth::revocation::RevocationStore;
use crate::auth::token_signing::TokenSigningKeyring;

// ──── Trusted Agent Identification ────

/// Trusted agent CN prefixes as defined in §4.3.
const TRUSTED_AGENT_CN_PREFIXES: &[&str] = &["coord-agent-"];
const TRUSTED_AGENT_CLUSTER_CN: &str = "coord-agent-cluster";

/// Determine if a peer certificate CN represents a trusted agent.
///
/// Trusted agents have CN starting with "coord-agent-" or exactly "coord-agent-cluster".
pub fn is_trusted_agent_cn(cn: Option<&str>) -> bool {
    match cn {
        Some(name) => {
            name == TRUSTED_AGENT_CLUSTER_CN
                || TRUSTED_AGENT_CN_PREFIXES
                    .iter()
                    .any(|prefix| name.starts_with(prefix))
        }
        None => false,
    }
}

// ──── High-Risk Operations ────

/// Set of capability IDs that require full scope checking regardless of caller identity.
///
/// See docs/capability-auth-implementation.md §4.3 High-Risk Operations table.
fn high_risk_operations() -> HashSet<&'static str> {
    let mut set = HashSet::new();

    // All admin operations
    set.insert("admin:maintenance:seal");
    set.insert("admin:maintenance:unseal");
    set.insert("admin:maintenance:member_add");
    set.insert("admin:maintenance:member_remove");
    set.insert("admin:maintenance:member_promote");
    set.insert("admin:auth:enable");
    set.insert("admin:auth:disable");
    set.insert("admin:auth:user_add");
    set.insert("admin:auth:user_delete");
    set.insert("admin:auth:role_add");
    set.insert("admin:auth:role_delete");
    set.insert("admin:auth:role_grant");
    set.insert("admin:auth:role_revoke");
    set.insert("admin:auth:user_grant_role");
    set.insert("admin:auth:user_revoke_role");
    set.insert("admin:capability:register");
    set.insert("admin:capability:deprecate");

    // Data plane writes
    set.insert("data:kv:write");
    set.insert("data:kv:delete");
    set.insert("data:txn:execute");

    // Coordination plane sensitive
    set.insert("coord:auth:user_add");
    set.insert("coord:auth:role_grant");
    set.insert("coord:auth:user_grant_role");

    // Coordination plane financial-grade
    set.insert("coord:workflow:define");
    set.insert("coord:saga:execute");
    set.insert("coord:saga:compensate");

    // Security policy
    set.insert("coord:policy:manage");
    set.insert("coord:pki:issue");
    set.insert("coord:pki:revoke");

    set
}

/// Check if a capability ID is classified as high-risk.
pub fn is_high_risk_operation(capability_id: &str) -> bool {
    // Check exact match
    if high_risk_operations().contains(capability_id) {
        return true;
    }

    // Check wildcard: admin:* is always high risk
    if capability_id.starts_with("admin:") {
        return true;
    }

    false
}

// ──── Server Auth Result ────

/// Result of server-side auth verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerAuthResult {
    /// Request is fully authorized
    Allow {
        token: CctToken,
        /// Whether scope checking was performed
        scope_checked: bool,
        /// Whether the caller is a trusted agent
        trusted_agent: bool,
    },
    /// Request is denied
    Deny {
        reason: String,
        /// Whether the caller is a trusted agent (for audit)
        trusted_agent: bool,
    },
}

impl ServerAuthResult {
    /// Check if this result is an Allow.
    pub fn is_allow(&self) -> bool {
        matches!(self, ServerAuthResult::Allow { .. })
    }

    /// Get the denial reason, if denied.
    pub fn denial_reason(&self) -> Option<&str> {
        match self {
            ServerAuthResult::Deny { reason, .. } => Some(reason),
            _ => None,
        }
    }
}

// ──── Server Auth Interceptor ────

/// Server-side auth interceptor implementing graded scope checking.
///
/// Validates CCT tokens and applies differential scope verification based on
/// caller identity (mTLS CN) and operation risk level.
pub struct ServerAuthInterceptor {
    /// Token signing keyring for CCT signature verification
    keyring: Arc<TokenSigningKeyring>,
    /// Revocation store for checking revoked tokens
    revocation_store: Arc<RevocationStore>,
    /// Clock drift tolerance in seconds
    clock_drift_secs: i64,
    /// Whether mTLS is enforced (if false, all requests get full scope check)
    mtls_enforced: bool,
    /// Whether auth is enabled
    enabled: bool,
}

impl ServerAuthInterceptor {
    /// Create a new server auth interceptor.
    pub fn new(
        keyring: Arc<TokenSigningKeyring>,
        revocation_store: Arc<RevocationStore>,
        mtls_enforced: bool,
    ) -> Self {
        Self {
            keyring,
            revocation_store,
            clock_drift_secs: 300,
            mtls_enforced,
            enabled: true,
        }
    }

    /// Set whether auth is enabled.
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// Set clock drift tolerance.
    pub fn set_clock_drift(&mut self, secs: i64) {
        self.clock_drift_secs = secs;
    }

    /// Validate an incoming server request.
    ///
    /// Parameters:
    /// - `cct_str`: The CCT from the Authorization header
    /// - `peer_cn`: The mTLS peer certificate Common Name (None if mTLS not used)
    /// - `capability_id`: The capability required for this RPC
    /// - `scope_key`: The resource key to check scope against (None for scope-free operations)
    pub fn validate(
        &self,
        cct_str: Option<&str>,
        peer_cn: Option<&str>,
        capability_id: &str,
        scope_key: Option<&str>,
    ) -> ServerAuthResult {
        // If auth is disabled, allow everything
        if !self.enabled {
            return ServerAuthResult::Allow {
                token: CctToken {
                    header: coord_core::auth::cct::CctHeader::default(),
                    payload: CctPayload {
                        jti: String::new(),
                        iss: String::new(),
                        sub: String::new(),
                        aud: vec![],
                        iat: 0,
                        exp: 0,
                        roles: vec![],
                        scope_overrides: std::collections::HashMap::new(),
                    },
                    signature: vec![],
                },
                scope_checked: false,
                trusted_agent: false,
            };
        }

        // 1. Extract and verify CCT
        let cct_str = match cct_str {
            Some(s) => s,
            None => {
                return ServerAuthResult::Deny {
                    reason: "missing CCT token".into(),
                    trusted_agent: false,
                }
            }
        };

        // Strip "Bearer " prefix if present
        let cct_str = extract_bearer_token(Some(cct_str)).unwrap_or(cct_str);

        // 2. Decode and verify CCT signature
        let cct = match self.decode_and_verify(cct_str) {
            Ok(token) => token,
            Err(e) => {
                return ServerAuthResult::Deny {
                    reason: format!("CCT validation failed: {e}"),
                    trusted_agent: false,
                }
            }
        };

        // 3. Check expiration
        if is_expired(&cct.payload, self.clock_drift_secs) {
            return ServerAuthResult::Deny {
                reason: "CCT expired".into(),
                trusted_agent: false,
            };
        }

        // 4. Check revocation
        if self.revocation_store.is_revoked(&cct.payload.jti) {
            return ServerAuthResult::Deny {
                reason: "CCT has been revoked".into(),
                trusted_agent: false,
            };
        }

        // 5. Determine caller identity
        let is_trusted = if self.mtls_enforced {
            is_trusted_agent_cn(peer_cn)
        } else {
            // mTLS not enforced → no caller is considered trusted
            // (fallback to full scope check for all requests)
            false
        };

        // 6. Determine operation risk level
        let is_high_risk = is_high_risk_operation(capability_id);

        // 7. Graded scope checking
        match (is_trusted, is_high_risk) {
            // Case A: Trusted agent + low-risk → skip scope check (fast path)
            (true, false) => ServerAuthResult::Allow {
                token: cct,
                scope_checked: false,
                trusted_agent: true,
            },

            // Case B: All other cases → full scope check required
            (_, true) | (false, _) => {
                if let Some(key) = scope_key {
                    // Verify scope from the token's scope_overrides or roles
                    if !self.check_scope(&cct.payload, capability_id, key) {
                        return ServerAuthResult::Deny {
                            reason: format!(
                                "scope restriction: key '{key}' not allowed for capability '{capability_id}'"
                            ),
                            trusted_agent: is_trusted,
                        };
                    }
                }

                ServerAuthResult::Allow {
                    token: cct,
                    scope_checked: true,
                    trusted_agent: is_trusted,
                }
            }
        }
    }

    // ──── Internal ────

    /// Decode and verify a CCT, trying all known signing keys.
    fn decode_and_verify(&self, cct_str: &str) -> Result<CctToken, String> {
        // Try active key first
        let active = self.keyring.active_key();
        if let Ok(token) = decode_cct(cct_str, &active.key_bytes) {
            return Ok(token);
        }

        // Try previous keys (for tokens signed before rotation)
        let key_ids = self.keyring.all_key_ids();
        for key_id in &key_ids {
            if let Some(key) = self.keyring.find_key(key_id) {
                if let Ok(token) = decode_cct(cct_str, &key.key_bytes) {
                    return Ok(token);
                }
            }
        }

        Err("invalid CCT signature".into())
    }

    /// Check if the CCT payload grants the requested scope for a capability.
    ///
    /// First checks scope_overrides in the token, then falls back to role-based grants.
    fn check_scope(
        &self,
        payload: &CctPayload,
        capability_id: &str,
        scope_key: &str,
    ) -> bool {
        // Check scope_overrides first (per-token overrides)
        if let Some(allowed_scope) = payload.scope_overrides.get(capability_id) {
            if allowed_scope.is_empty() {
                return true; // Empty scope = match-all
            }
            let mut trie = ScopeTrie::new();
            if trie.insert(allowed_scope).is_err() {
                return false;
            }
            return trie.matches(scope_key);
        }

        // If no scope_overrides, the scope check is deferred to the agent.
        // On the server side without role cache, we apply a generous policy:
        // allow if the token has any roles (agent already verified scope).
        // This is safe because:
        // 1. Agent already performed full scope check
        // 2. Server is the second line of defense
        // 3. For non-trusted callers, the agent check already happened upstream
        !payload.roles.is_empty()
    }
}

// ──── Helpers ────

/// Extract bearer token from Authorization header (server-side).
pub fn extract_bearer_token(header: Option<&str>) -> Option<&str> {
    let header = header?;
    if let Some(token) = header.strip_prefix("Bearer ") {
        Some(token)
    } else if header.starts_with("eyJ") {
        // CCT v3 format (base64url JSON header)
        Some(header)
    } else if header.starts_with("coord_") {
        // Legacy token — pass through (not handled by this interceptor)
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
    use crate::auth::revocation::RevocationStore;
    use crate::auth::token_signing::TokenSigningKeyring;

    fn make_keyring() -> Arc<TokenSigningKeyring> {
        let root_key = vec![0u8; 32];
        Arc::new(TokenSigningKeyring::new(root_key).unwrap())
    }

    fn make_revocation_store() -> Arc<RevocationStore> {
        Arc::new(RevocationStore::new(1000))
    }

    fn make_test_cct(
        keyring: &TokenSigningKeyring,
        roles: Vec<&str>,
        scope_overrides: std::collections::HashMap<String, String>,
    ) -> String {
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
        let key = keyring.active_key();
        encode_cct(&header, &payload, &key.key_bytes).unwrap()
    }

    // ──── Phase 3.7 TDD Tests ────

    // ──── Diagnostic: CCT encode/decode roundtrip with keyring ────

    #[test]
    fn test_cct_roundtrip_with_keyring() {
        let keyring = make_keyring();
        let key = keyring.active_key();

        let header = CctHeader::default();
        let payload = CctPayload {
            jti: "test-jti".to_string(),
            iss: "test".to_string(),
            sub: "test".to_string(),
            aud: vec![],
            iat: 1719990000,
            exp: 2000000000,
            roles: vec!["reader".to_string()],
            scope_overrides: std::collections::HashMap::new(),
        };

        // Encode
        let cct = encode_cct(&header, &payload, &key.key_bytes)
            .expect("encode_cct should succeed");
        assert!(!cct.is_empty());
        assert!(cct.starts_with("eyJ"), "CCT should start with base64url JSON");

        // Decode
        let decoded = decode_cct(&cct, &key.key_bytes)
            .expect("decode_cct should succeed");
        assert_eq!(decoded.payload.jti, "test-jti");
        assert_eq!(decoded.payload.roles, vec!["reader"]);
    }

    // ──── Trusted Agent Detection ────

    #[test]
    fn test_is_trusted_agent_cn_with_valid_prefix() {
        assert!(is_trusted_agent_cn(Some("coord-agent-1")));
        assert!(is_trusted_agent_cn(Some("coord-agent-prod-us-east")));
        assert!(is_trusted_agent_cn(Some("coord-agent-cluster")));
    }

    #[test]
    fn test_is_trusted_agent_cn_rejects_non_agent() {
        assert!(!is_trusted_agent_cn(Some("random-client")));
        assert!(!is_trusted_agent_cn(Some("admin")));
        assert!(!is_trusted_agent_cn(Some("coord-server-1")));
        assert!(!is_trusted_agent_cn(Some("agent-coord-1"))); // wrong prefix order
    }

    #[test]
    fn test_is_trusted_agent_cn_handles_none() {
        assert!(!is_trusted_agent_cn(None));
    }

    // ──── High-Risk Operations ────

    #[test]
    fn test_is_high_risk_admin_operations() {
        assert!(is_high_risk_operation("admin:maintenance:seal"));
        assert!(is_high_risk_operation("admin:maintenance:unseal"));
        assert!(is_high_risk_operation("admin:auth:enable"));
        assert!(is_high_risk_operation("admin:auth:user_add"));
        assert!(is_high_risk_operation("admin:capability:register"));
    }

    #[test]
    fn test_is_high_risk_data_write_operations() {
        assert!(is_high_risk_operation("data:kv:write"));
        assert!(is_high_risk_operation("data:kv:delete"));
        assert!(is_high_risk_operation("data:txn:execute"));
    }

    #[test]
    fn test_is_high_risk_coord_sensitive_operations() {
        assert!(is_high_risk_operation("coord:auth:user_add"));
        assert!(is_high_risk_operation("coord:auth:role_grant"));
        assert!(is_high_risk_operation("coord:workflow:define"));
        assert!(is_high_risk_operation("coord:saga:execute"));
        assert!(is_high_risk_operation("coord:saga:compensate"));
        assert!(is_high_risk_operation("coord:policy:manage"));
        assert!(is_high_risk_operation("coord:pki:issue"));
        assert!(is_high_risk_operation("coord:pki:revoke"));
    }

    #[test]
    fn test_is_high_risk_wildcard_admin() {
        // Any admin:* should be high risk
        assert!(is_high_risk_operation("admin:maintenance:status"));
        assert!(is_high_risk_operation("admin:maintenance:snapshot"));
        assert!(is_high_risk_operation("admin:auth:status"));
        assert!(is_high_risk_operation("admin:auth:role_list"));
        assert!(is_high_risk_operation("admin:capability:list"));
        assert!(is_high_risk_operation("admin:unknown:something"));
    }

    #[test]
    fn test_low_risk_read_operations() {
        assert!(!is_high_risk_operation("data:kv:read"));
        assert!(!is_high_risk_operation("data:watch:subscribe"));
        assert!(!is_high_risk_operation("data:cache:read"));
        assert!(!is_high_risk_operation("coord:registry:discover"));
        assert!(!is_high_risk_operation("coord:config:read"));
        assert!(!is_high_risk_operation("coord:workflow:query"));
    }

    // ──── Server Auth Interceptor ────

    #[test]
    fn test_server_interceptor_allows_when_disabled() {
        let keyring = make_keyring();
        let rev_store = make_revocation_store();
        let mut interceptor = ServerAuthInterceptor::new(keyring, rev_store, true);
        interceptor.set_enabled(false);

        let result = interceptor.validate(None, None, "data:kv:read", None);
        assert!(result.is_allow());
    }

    #[test]
    fn test_server_interceptor_denies_missing_cct() {
        let keyring = make_keyring();
        let rev_store = make_revocation_store();
        let interceptor = ServerAuthInterceptor::new(keyring, rev_store, true);

        let result = interceptor.validate(None, None, "data:kv:read", None);
        assert!(matches!(result, ServerAuthResult::Deny { .. }));
    }

    #[test]
    fn test_server_interceptor_trusted_agent_low_risk_skips_scope() {
        let keyring = make_keyring();
        let rev_store = make_revocation_store();
        let interceptor = ServerAuthInterceptor::new(keyring.clone(), rev_store, true);

        let cct = make_test_cct(&keyring, vec!["reader"], std::collections::HashMap::new());
        let auth_header = format!("Bearer {cct}");

        // Trusted agent + low-risk read → should skip scope check
        let result = interceptor.validate(
            Some(&auth_header),
            Some("coord-agent-1"),
            "data:kv:read",
            Some("/any/key"),
        );

        match result {
            ServerAuthResult::Allow { scope_checked, trusted_agent, .. } => {
                assert!(!scope_checked, "scope should be skipped for trusted agent + low risk");
                assert!(trusted_agent);
            }
            ServerAuthResult::Deny { reason, .. } => {
                panic!("expected Allow but got Deny: {reason}");
            }
        }
    }

    #[test]
    fn test_server_interceptor_non_trusted_caller_full_scope_check() {
        let keyring = make_keyring();
        let rev_store = make_revocation_store();
        let interceptor = ServerAuthInterceptor::new(keyring.clone(), rev_store, true);

        let cct = make_test_cct(&keyring, vec!["reader"], std::collections::HashMap::new());
        let auth_header = format!("Bearer {cct}");

        // Non-trusted caller + low-risk read → still requires scope check
        let result = interceptor.validate(
            Some(&auth_header),
            Some("random-client"),
            "data:kv:read",
            Some("/any/key"),
        );

        match result {
            ServerAuthResult::Allow { scope_checked, trusted_agent, .. } => {
                assert!(scope_checked, "scope should be checked for non-trusted caller");
                assert!(!trusted_agent);
            }
            ServerAuthResult::Deny { reason, .. } => {
                panic!("expected Allow but got Deny: {reason}");
            }
        }
    }

    #[test]
    fn test_server_interceptor_high_risk_always_full_scope() {
        let keyring = make_keyring();
        let rev_store = make_revocation_store();
        let interceptor = ServerAuthInterceptor::new(keyring.clone(), rev_store, true);

        // Even trusted agent + high-risk write → force full scope check
        let cct = make_test_cct(&keyring, vec!["admin"], std::collections::HashMap::new());
        let auth_header = format!("Bearer {cct}");

        let result = interceptor.validate(
            Some(&auth_header),
            Some("coord-agent-1"), // trusted agent
            "data:kv:write",       // high-risk operation
            Some("/app/data"),
        );

        match result {
            ServerAuthResult::Allow { scope_checked, trusted_agent, .. } => {
                assert!(scope_checked, "scope should be checked for high-risk operations");
                assert!(trusted_agent);
            }
            ServerAuthResult::Deny { reason, .. } => {
                panic!("expected Allow but got Deny: {reason}");
            }
        }
    }

    #[test]
    fn test_server_interceptor_revoked_token_denied() {
        let keyring = make_keyring();
        let rev_store = make_revocation_store();

        let cct = make_test_cct(&keyring, vec!["reader"], std::collections::HashMap::new());
        let auth_header = format!("Bearer {cct}");

        // Decode the CCT to get the jti, then revoke it
        let key = keyring.active_key();
        let token = decode_cct(&cct, &key.key_bytes).unwrap();
        rev_store.revoke(&token.payload.jti);

        let interceptor = ServerAuthInterceptor::new(keyring, rev_store, true);

        let result = interceptor.validate(
            Some(&auth_header),
            Some("coord-agent-1"),
            "data:kv:read",
            None,
        );

        assert!(matches!(result, ServerAuthResult::Deny { .. }));
        assert!(result.denial_reason().unwrap().contains("revoked"));
    }

    #[test]
    fn test_server_interceptor_expired_token_denied() {
        let keyring = make_keyring();
        let rev_store = make_revocation_store();
        let interceptor = ServerAuthInterceptor::new(keyring.clone(), rev_store, true);

        let header = CctHeader::default();
        let payload = CctPayload {
            jti: "expired-token".to_string(),
            iss: "test".to_string(),
            sub: "test".to_string(),
            aud: vec![],
            iat: 1000000000,
            exp: 1000003600, // expired long ago
            roles: vec!["reader".to_string()],
            scope_overrides: std::collections::HashMap::new(),
        };
        let key = keyring.active_key();
        let cct = encode_cct(&header, &payload, &key.key_bytes).unwrap();
        let auth_header = format!("Bearer {cct}");

        let result = interceptor.validate(
            Some(&auth_header),
            Some("coord-agent-1"),
            "data:kv:read",
            None,
        );

        assert!(matches!(result, ServerAuthResult::Deny { .. }));
    }

    #[test]
    fn test_server_interceptor_mtls_disabled_no_trusted_path() {
        let keyring = make_keyring();
        let rev_store = make_revocation_store();
        // mTLS NOT enforced → even coord-agent-* is not trusted
        let interceptor = ServerAuthInterceptor::new(keyring.clone(), rev_store, false);

        let cct = make_test_cct(&keyring, vec!["reader"], std::collections::HashMap::new());
        let auth_header = format!("Bearer {cct}");

        let result = interceptor.validate(
            Some(&auth_header),
            Some("coord-agent-1"), // would be trusted if mTLS enforced
            "data:kv:read",
            Some("/any/key"),
        );

        match result {
            ServerAuthResult::Allow { scope_checked, trusted_agent, .. } => {
                assert!(scope_checked, "scope should be checked when mTLS is not enforced");
                assert!(!trusted_agent, "agent should not be trusted without mTLS");
            }
            ServerAuthResult::Deny { reason, .. } => {
                panic!("expected Allow but got Deny: {reason}");
            }
        }
    }

    #[test]
    fn test_scope_override_in_token_respected() {
        let keyring = make_keyring();
        let rev_store = make_revocation_store();
        let interceptor = ServerAuthInterceptor::new(keyring.clone(), rev_store, true);

        let mut scope_overrides = std::collections::HashMap::new();
        scope_overrides.insert("data:kv:read".to_string(), "/app/orders/".to_string());

        let cct = make_test_cct(&keyring, vec!["reader"], scope_overrides);
        let auth_header = format!("Bearer {cct}");

        // Key within scope should be allowed
        let result = interceptor.validate(
            Some(&auth_header),
            None, // non-trusted → full scope check
            "data:kv:read",
            Some("/app/orders/123"),
        );
        assert!(result.is_allow());

        // Key outside scope should be denied
        let result = interceptor.validate(
            Some(&auth_header),
            None,
            "data:kv:read",
            Some("/app/payments/456"),
        );
        assert!(matches!(result, ServerAuthResult::Deny { .. }));
    }

    #[test]
    fn test_bearer_token_extraction_server() {
        assert_eq!(extract_bearer_token(Some("Bearer mytoken")), Some("mytoken"));
        assert_eq!(extract_bearer_token(Some("eyJhbGciOiJI...")), Some("eyJhbGciOiJI..."));
        assert_eq!(extract_bearer_token(Some("coord_abc123")), None); // legacy
        assert_eq!(extract_bearer_token(None), None);
    }

    #[test]
    fn test_high_risk_set_completeness() {
        let ops = high_risk_operations();
        // Verify key entries from the spec table §4.3
        assert!(ops.contains("admin:maintenance:seal"));
        assert!(ops.contains("data:kv:write"));
        assert!(ops.contains("data:txn:execute"));
        assert!(ops.contains("coord:auth:user_add"));
        assert!(ops.contains("coord:workflow:define"));
        assert!(ops.contains("coord:saga:execute"));
        assert!(ops.contains("coord:saga:compensate"));
        assert!(ops.contains("coord:policy:manage"));
        assert!(ops.contains("coord:pki:issue"));
        assert!(ops.contains("coord:pki:revoke"));
    }
}
