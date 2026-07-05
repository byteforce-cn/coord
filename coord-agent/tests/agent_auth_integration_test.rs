// Phase 4: Integration Verification — E2E / Dual Defense / Chaos / Regression
//
// Verifies the complete auth flow across Agent and Server boundaries:
// - E2E: CCT issuance → Agent validate → Server validate
// - Dual defense: Agent rejects before Server sees request
// - Role cache + sync integration
// - Rate limiter + interceptor integration
// - Revocation + bloom filter integration
//
// See docs/capability-auth-implementation.md Phase 4.

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use coord_agent::auth::circuit_breaker::{AuthMetrics, CircuitBreaker, CircuitState, FallbackPolicy};
    use coord_agent::auth::interceptor::AuthInterceptor;
    use coord_agent::auth::rate_limiter::LoginRateLimiter;
    use coord_agent::auth::role_cache::{CapabilityGrant, RoleCache, RoleEntry};
    use coord_agent::auth::sync::{SyncAction, SyncConfig, SyncScheduler};
    use coord_core::auth::cct::{decode_cct, encode_cct, CctHeader, CctPayload};
    use coord_core::auth::trie::ScopeTrie;
    use coord_server::auth::interceptor::{
        is_high_risk_operation, is_trusted_agent_cn, ServerAuthInterceptor,
    };
    use coord_server::auth::revocation::RevocationStore;
    use coord_server::auth::token_signing::TokenSigningKeyring;

    const TEST_KEY: &[u8] = b"integration-test-key-32-bytes!!";
    const ALT_KEY: &[u8] = b"alternate-test-key-32-bytes!!!!";

    fn make_keyring() -> Arc<TokenSigningKeyring> {
        let root = vec![1u8; 32];
        Arc::new(TokenSigningKeyring::new(root).unwrap())
    }

    fn make_agent_interceptor(role_cache: Arc<RoleCache>) -> AuthInterceptor {
        AuthInterceptor::new(TEST_KEY.to_vec(), role_cache, 300)
    }

    fn make_server_interceptor(
        keyring: Arc<TokenSigningKeyring>,
        rev_store: Arc<RevocationStore>,
    ) -> ServerAuthInterceptor {
        ServerAuthInterceptor::new(keyring, rev_store, true)
    }

    // ════════════════════════════════════════════════════════════════
    // 4.1 E2E: Complete auth flow
    // ════════════════════════════════════════════════════════════════

    /// Test: Full E2E auth flow — CCT issuance → Agent validation → Server validation
    #[test]
    fn test_e2e_complete_auth_flow() {
        // --- Setup: shared keyring for both agent and server ---
        let keyring = make_keyring();
        let key = keyring.active_key();
        let signing_key_bytes: Vec<u8> = key.key_bytes.to_vec();

        let role_cache = Arc::new(RoleCache::new());
        role_cache.sync_full(vec![RoleEntry {
            name: "service-reader".to_string(),
            grants: vec![CapabilityGrant {
                capability_id: "data:kv:read".to_string(),
                scope: "/app/".to_string(),
            }],
            high_sensitive: false,
        }]);

        // Agent interceptor uses the same signing key as the server
        let agent = AuthInterceptor::new(signing_key_bytes, role_cache, 300);

        let rev_store = Arc::new(RevocationStore::new(1000));
        let server = make_server_interceptor(keyring.clone(), rev_store);

        // --- Step 1: Issue CCT (signed with keyring's active key) ---
        let header = CctHeader::default();
        let payload = CctPayload {
            jti: "e2e-token-001".to_string(),
            iss: "prod-cluster".to_string(),
            sub: "order-service".to_string(),
            aud: vec!["coord-agent".to_string()],
            iat: 1719990000,
            exp: 2000000000,
            roles: vec!["service-reader".to_string()],
            scope_overrides: HashMap::new(),
        };
        let key = keyring.active_key();
        let cct = encode_cct(&header, &payload, &key.key_bytes).unwrap();

        // --- Step 2: Agent validates ---
        let agent_result = agent.validate_request(
            "/coord.kv.Kv/Range",
            Some(&format!("Bearer {cct}")),
            Some("/app/order-123"),
        );
        assert!(
            matches!(agent_result, coord_agent::auth::interceptor::AuthResult::Allow(_)),
            "Agent should allow valid request"
        );

        // --- Step 3: Server validates (trusted agent) ---
        let server_result = server.validate(
            Some(&format!("Bearer {cct}")),
            Some("coord-agent-1"), // trusted
            "data:kv:read",
            Some("/app/order-123"),
        );
        assert!(server_result.is_allow());
    }

    /// Test: E2E with non-trusted caller
    #[test]
    fn test_e2e_non_trusted_caller() {
        let keyring = make_keyring();
        let key = keyring.active_key();
        let signing_key_bytes: Vec<u8> = key.key_bytes.to_vec();

        let role_cache = Arc::new(RoleCache::new());
        role_cache.sync_full(vec![RoleEntry {
            name: "reader".to_string(),
            grants: vec![CapabilityGrant {
                capability_id: "data:kv:read".to_string(),
                scope: "".to_string(),
            }],
            high_sensitive: false,
        }]);

        let agent = AuthInterceptor::new(signing_key_bytes, role_cache, 300);

        let rev_store = Arc::new(RevocationStore::new(1000));
        let server = make_server_interceptor(keyring.clone(), rev_store);

        let key = keyring.active_key();
        let cct = encode_cct(
            &CctHeader::default(),
            &CctPayload {
                jti: "non-trusted-test".to_string(),
                iss: "test".to_string(),
                sub: "test".to_string(),
                aud: vec![],
                iat: 1719990000,
                exp: 2000000000,
                roles: vec!["reader".to_string()],
                scope_overrides: HashMap::new(),
            },
            &key.key_bytes,
        )
        .unwrap();

        // Agent allows (valid token)
        let agent_result = agent.validate_request(
            "/coord.kv.Kv/Range",
            Some(&format!("Bearer {cct}")),
            Some("/any/key"),
        );
        assert!(matches!(agent_result, coord_agent::auth::interceptor::AuthResult::Allow(_)));

        // Server with non-trusted caller → full scope check
        let server_result = server.validate(
            Some(&format!("Bearer {cct}")),
            Some("random-direct-client"), // NOT a trusted agent
            "data:kv:read",
            Some("/any/key"),
        );
        // Should still allow (scope override not restricted), but scope_checked=true
        match server_result {
            coord_server::auth::interceptor::ServerAuthResult::Allow {
                scope_checked,
                trusted_agent,
                ..
            } => {
                assert!(scope_checked, "non-trusted caller: scope should be checked");
                assert!(!trusted_agent);
            }
            other => panic!("expected Allow, got {other:?}"),
        }
    }

    // ════════════════════════════════════════════════════════════════
    // 4.2 Dual defense: Agent + Server interceptors
    // ════════════════════════════════════════════════════════════════

    /// Test: Agent rejects before Server — first line of defense
    #[test]
    fn test_dual_defense_agent_rejects_first() {
        let role_cache = Arc::new(RoleCache::new());
        // Reader only has data:kv:read
        role_cache.sync_full(vec![RoleEntry {
            name: "reader".to_string(),
            grants: vec![CapabilityGrant {
                capability_id: "data:kv:read".to_string(),
                scope: "/app/".to_string(),
            }],
            high_sensitive: false,
        }]);

        let agent = make_agent_interceptor(role_cache);

        let cct = encode_cct(
            &CctHeader::default(),
            &CctPayload {
                jti: "dual-defense-001".to_string(),
                iss: "test".to_string(),
                sub: "test".to_string(),
                aud: vec![],
                iat: 1719990000,
                exp: 2000000000,
                roles: vec!["reader".to_string()],
                scope_overrides: HashMap::new(),
            },
            TEST_KEY,
        )
        .unwrap();

        // Agent should reject: reader can't do data:kv:write
        let agent_result = agent.validate_request(
            "/coord.kv.Kv/Put",
            Some(&format!("Bearer {cct}")),
            Some("/app/order-123"),
        );
        assert!(
            matches!(agent_result, coord_agent::auth::interceptor::AuthResult::Deny(_)),
            "Agent must reject write from reader role (first line of defense)"
        );

        // Server would NEVER see this request because Agent already rejected it
    }

    /// Test: Server is second line — catches what Agent misses (non-trusted path)
    #[test]
    fn test_dual_defense_server_catches_scope_violation() {
        let keyring = make_keyring();
        let key = keyring.active_key();
        let rev_store = Arc::new(RevocationStore::new(1000));
        let server = make_server_interceptor(keyring.clone(), rev_store);

        let mut scope_overrides = HashMap::new();
        scope_overrides.insert("data:kv:read".to_string(), "/app/orders/".to_string());

        let cct = encode_cct(
            &CctHeader::default(),
            &CctPayload {
                jti: "server-defense-001".to_string(),
                iss: "test".to_string(),
                sub: "test".to_string(),
                aud: vec![],
                iat: 1719990000,
                exp: 2000000000,
                roles: vec!["reader".to_string()],
                scope_overrides,
            },
            &key.key_bytes,
        )
        .unwrap();

        // Non-trusted caller with scope override — Server enforces scope
        let result = server.validate(
            Some(&format!("Bearer {cct}")),
            None, // no mTLS → non-trusted
            "data:kv:read",
            Some("/app/payments/999"), // OUTSIDE scope "/app/orders/"
        );
        assert!(
            matches!(result, coord_server::auth::interceptor::ServerAuthResult::Deny { .. }),
            "Server must deny scope violation (second line of defense)"
        );
    }

    // ════════════════════════════════════════════════════════════════
    // 4.3 Role cache + sync scheduler integration
    // ════════════════════════════════════════════════════════════════

    #[test]
    fn test_role_cache_sync_integration() {
        let cache = Arc::new(RoleCache::new());
        let scheduler = SyncScheduler::with_defaults(cache.clone());

        // Initial state: not yet synced
        assert!(!cache.is_initialized());
        let (action, _) = scheduler.next_sync();
        assert_eq!(action, SyncAction::RoleFullSync);

        // Simulate a successful sync
        cache.sync_full(vec![
            RoleEntry {
                name: "admin".to_string(),
                grants: vec![],
                high_sensitive: true,
            },
            RoleEntry {
                name: "reader".to_string(),
                grants: vec![CapabilityGrant {
                    capability_id: "data:kv:read".to_string(),
                    scope: "/app/".to_string(),
                }],
                high_sensitive: false,
            },
        ]);
        scheduler.record_role_sync_success();

        assert!(cache.is_initialized());
        assert_eq!(cache.role_count(), 2);
        assert!(cache.is_high_sensitive("admin"));
        assert!(!cache.is_high_sensitive("reader"));

        let stats = scheduler.stats();
        assert_eq!(stats.role_syncs_succeeded, 1);
    }

    #[test]
    fn test_high_sensitive_role_detected_by_cache() {
        let cache = Arc::new(RoleCache::new());
        cache.sync_full(vec![
            RoleEntry {
                name: "root".to_string(),
                grants: vec![],
                high_sensitive: true,
            },
            RoleEntry {
                name: "security-manager".to_string(),
                grants: vec![],
                high_sensitive: true,
            },
            RoleEntry {
                name: "service-writer".to_string(),
                grants: vec![CapabilityGrant {
                    capability_id: "data:kv:write".to_string(),
                    scope: "".to_string(),
                }],
                high_sensitive: false,
            },
        ]);

        // High-sensitivity roles from §10 table
        assert!(cache.is_high_sensitive("root"));
        assert!(cache.is_high_sensitive("security-manager"));
        assert!(!cache.is_high_sensitive("service-writer"));
        assert!(!cache.is_high_sensitive("unknown"));
    }

    // ════════════════════════════════════════════════════════════════
    // 4.4 Rate limiter + interceptor integration
    // ════════════════════════════════════════════════════════════════

    #[test]
    fn test_rate_limiter_integration_with_auth_flow() {
        let limiter = LoginRateLimiter::new();
        let role_cache = Arc::new(RoleCache::new());
        role_cache.sync_full(vec![RoleEntry {
            name: "reader".to_string(),
            grants: vec![CapabilityGrant {
                capability_id: "data:kv:read".to_string(),
                scope: "".to_string(),
            }],
            high_sensitive: false,
        }]);
        let agent = make_agent_interceptor(role_cache);

        let cct = encode_cct(
            &CctHeader::default(),
            &CctPayload {
                jti: "rate-limit-test".to_string(),
                iss: "test".to_string(),
                sub: "test".to_string(),
                aud: vec![],
                iat: 1719990000,
                exp: 2000000000,
                roles: vec!["reader".to_string()],
                scope_overrides: HashMap::new(),
            },
            TEST_KEY,
        )
        .unwrap();

        let client_ip = "10.0.0.55";

        // First, rate limiter check for login
        for _ in 0..10 {
            assert!(limiter.check(client_ip).is_ok());
        }
        // 11th login attempt should be rate limited
        assert!(limiter.check(client_ip).is_err());

        // But if they got a token before rate limiting, the interceptor should still work
        let result = agent.validate_request(
            "/coord.kv.Kv/Range",
            Some(&format!("Bearer {cct}")),
            Some("/any/key"),
        );
        assert!(matches!(result, coord_agent::auth::interceptor::AuthResult::Allow(_)));
    }

    // ════════════════════════════════════════════════════════════════
    // 4.5 Revocation + bloom filter integration
    // ════════════════════════════════════════════════════════════════

    #[test]
    fn test_revocation_prevents_access() {
        let store = RevocationStore::new(1000);
        let cct_jti = "revoked-token-jti";

        // Initially not revoked
        assert!(!store.is_revoked(cct_jti));
        assert!(!store.might_be_revoked(cct_jti));

        // Revoke
        let version = store.revoke(cct_jti);
        assert!(version > 0);

        // Now revoked
        assert!(store.is_revoked(cct_jti));
        assert!(store.might_be_revoked(cct_jti));
        assert!(store.is_exactly_revoked(cct_jti));

        // Version tracking: revoke returns the new version (post-increment)
        assert_eq!(store.current_version(), version);
    }

    #[test]
    fn test_revocation_delta_sync() {
        let store = RevocationStore::new(1000);

        let v1 = store.revoke("tok-001");
        let v2 = store.revoke("tok-002");
        let _v3 = store.revoke("tok-003");

        // Delta since before any revocations
        let delta = store.get_delta_since(0);
        assert_eq!(delta.len(), 3);
        assert_eq!(delta[0].jti, "tok-001");
        assert_eq!(delta[2].jti, "tok-003");

        // Delta since v1 (only tok-002 and tok-003)
        let delta = store.get_delta_since(v1);
        assert_eq!(delta.len(), 2);
        assert_eq!(delta[0].jti, "tok-002");
    }

    #[test]
    fn test_bloom_filter_no_false_negatives() {
        let store = RevocationStore::new(1000);

        // Revoke 500 tokens
        for i in 0..500 {
            store.revoke(&format!("tok-{i:04}"));
        }

        // Every revoked token should be found by bloom filter (no false negatives)
        for i in 0..500 {
            let jti = format!("tok-{i:04}");
            assert!(
                store.might_be_revoked(&jti),
                "bloom filter must not have false negatives for {jti}"
            );
            assert!(
                store.is_revoked(&jti),
                "exact check must confirm revocation for {jti}"
            );
        }

        // Non-revoked tokens should not be in exact set
        assert!(!store.is_revoked("tok-never-revoked"));
    }

    // ════════════════════════════════════════════════════════════════
    // 4.6 Circuit breaker integration
    // ════════════════════════════════════════════════════════════════

    #[test]
    fn test_circuit_breaker_lifecycle() {
        let metrics = Arc::new(AuthMetrics::new());
        let cb = CircuitBreaker::new(metrics);

        // Start closed
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.allow_request());

        // Record successes
        for _ in 0..100 {
            cb.record_success();
        }

        // Still closed with 0% failure rate
        assert_eq!(cb.state(), CircuitState::Closed);

        // Force open for testing
        cb.force_state(CircuitState::Open);
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(!cb.allow_request());

        // Fallback policy
        assert_eq!(cb.fallback_policy(), FallbackPolicy::DenyAll);
    }

    #[test]
    fn test_circuit_breaker_metrics_accumulation() {
        let metrics = Arc::new(AuthMetrics::new());

        metrics.record_request();
        metrics.record_request();
        metrics.record_denied("expired");
        metrics.record_denied("signature");
        metrics.record_cache_hit();
        metrics.record_cache_hit();
        metrics.record_cache_hit();
        metrics.record_cache_miss();

        assert_eq!(metrics.requests_total.load(std::sync::atomic::Ordering::Relaxed), 2);
        assert_eq!(metrics.denied_expired.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(metrics.denied_signature.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(metrics.cache_hits.load(std::sync::atomic::Ordering::Relaxed), 3);
        assert_eq!(metrics.cache_misses.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert!((metrics.cache_hit_ratio() - 0.75).abs() < 0.01);
    }

    // ════════════════════════════════════════════════════════════════
    // 4.7 Scope Trie integration with full auth flow
    // ════════════════════════════════════════════════════════════════

    #[test]
    fn test_scope_trie_matches_complex_patterns() {
        let mut trie = ScopeTrie::new();

        // Multi-tenant scopes
        trie.insert("/app/tenant-a/orders/").unwrap();
        trie.insert("/app/tenant-a/config/").unwrap();
        trie.insert("/app/tenant-b/*").unwrap();

        // Tenant A orders
        assert!(trie.matches("/app/tenant-a/orders/order-001"));
        assert!(trie.matches("/app/tenant-a/orders/order-002/detail"));
        assert!(!trie.matches("/app/tenant-a/payments/pay-001"));

        // Tenant A config
        assert!(trie.matches("/app/tenant-a/config/db-url"));

        // Tenant B (wildcard)
        assert!(trie.matches("/app/tenant-b/anything"));
        assert!(trie.matches("/app/tenant-b/orders/order-999"));

        // Tenant C (no access)
        assert!(!trie.matches("/app/tenant-c/data"));
    }

    // ════════════════════════════════════════════════════════════════
    // 4.8 Trusted agent CN detection integration
    // ════════════════════════════════════════════════════════════════

    #[test]
    fn test_trusted_agent_cn_variants() {
        // Valid trusted agent CNs
        assert!(is_trusted_agent_cn(Some("coord-agent-1")));
        assert!(is_trusted_agent_cn(Some("coord-agent-prod-us-east-1")));
        assert!(is_trusted_agent_cn(Some("coord-agent-cluster")));
        assert!(is_trusted_agent_cn(Some("coord-agent-"))); // minimal valid

        // Invalid — not trusted agents
        assert!(!is_trusted_agent_cn(Some("coord-server-1")));
        assert!(!is_trusted_agent_cn(Some("agent-coord-1"))); // wrong order
        assert!(!is_trusted_agent_cn(Some("coord-agent"))); // no hyphen after agent (strict)
        assert!(!is_trusted_agent_cn(None));
    }

    #[test]
    fn test_high_risk_operations_comprehensive() {
        // All admin:* should be high risk
        assert!(is_high_risk_operation("admin:maintenance:status"));
        assert!(is_high_risk_operation("admin:auth:role_list"));
        assert!(is_high_risk_operation("admin:capability:get"));
        assert!(is_high_risk_operation("admin:anything:else"));

        // Data writes
        assert!(is_high_risk_operation("data:kv:write"));
        assert!(is_high_risk_operation("data:kv:delete"));
        assert!(is_high_risk_operation("data:txn:execute"));

        // Safe reads
        assert!(!is_high_risk_operation("data:kv:read"));
        assert!(!is_high_risk_operation("data:watch:subscribe"));
        assert!(!is_high_risk_operation("coord:registry:discover"));
        assert!(!is_high_risk_operation("coord:config:read"));
        assert!(!is_high_risk_operation("coord:leader:observe"));
    }

    // ════════════════════════════════════════════════════════════════
    // 4.9 Regression: Verify existing tests still pass
    // ════════════════════════════════════════════════════════════════

    /// Test that CCT roundtrip still works with all key types
    #[test]
    fn test_regression_cct_roundtrip_all_keys() {
        let payload = CctPayload {
            jti: "regression-test".to_string(),
            iss: "test".to_string(),
            sub: "test".to_string(),
            aud: vec![],
            iat: 1719990000,
            exp: 2000000000,
            roles: vec!["service-reader".to_string(), "service-writer".to_string()],
            scope_overrides: HashMap::new(),
        };

        // Test with raw key
        let cct1 = encode_cct(&CctHeader::default(), &payload, TEST_KEY).unwrap();
        let decoded1 = decode_cct(&cct1, TEST_KEY).unwrap();
        assert_eq!(decoded1.payload.roles.len(), 2);

        // Test with alternate key
        let cct2 = encode_cct(&CctHeader::default(), &payload, ALT_KEY).unwrap();
        let decoded2 = decode_cct(&cct2, ALT_KEY).unwrap();
        assert_eq!(decoded2.payload.roles.len(), 2);

        // Cross-key verification should fail
        assert!(decode_cct(&cct1, ALT_KEY).is_err());
        assert!(decode_cct(&cct2, TEST_KEY).is_err());
    }
}
