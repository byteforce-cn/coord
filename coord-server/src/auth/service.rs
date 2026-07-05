// Auth gRPC Service — implements auth.proto Auth service (ADP §14)
//
// Provides:
// - Auth enable/disable/status
// - User CRUD + password management
// - Role CRUD + permission management
// - User-role assignment
// - Authentication (password → token + CCT)

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use coord_proto::auth::auth_server::Auth as AuthTrait;
use coord_proto::auth::*;
use parking_lot::RwLock;
use uuid::Uuid;

use coord_core::auth::cct::{encode_cct, CctHeader, CctPayload};

use crate::auth::manager::AuthManager;
use crate::auth::token::TokenManager;
use crate::auth::token_signing::TokenSigningKeyring;

/// gRPC Auth service implementation
pub struct AuthService {
    auth_manager: Arc<AuthManager>,
    token_manager: Arc<TokenManager>,
    /// Token signing keyring for CCT issuance (Phase 2.3)
    signing_keyring: Option<Arc<TokenSigningKeyring>>,
    /// Bootstrap token whitelist (Phase 2.6)
    bootstrap_tokens: RwLock<HashSet<String>>,
}

impl AuthService {
    pub fn new(auth_manager: Arc<AuthManager>, token_manager: Arc<TokenManager>) -> Self {
        Self {
            auth_manager,
            token_manager,
            signing_keyring: None,
            bootstrap_tokens: RwLock::new(HashSet::new()),
        }
    }

    /// Create an AuthService with CCT signing capability.
    pub fn with_cct_signing(
        auth_manager: Arc<AuthManager>,
        token_manager: Arc<TokenManager>,
        signing_keyring: Arc<TokenSigningKeyring>,
    ) -> Self {
        Self {
            auth_manager,
            token_manager,
            signing_keyring: Some(signing_keyring),
            bootstrap_tokens: RwLock::new(HashSet::new()),
        }
    }

    /// Add a bootstrap token to the whitelist.
    pub fn add_bootstrap_token(&self, token: &str) {
        self.bootstrap_tokens.write().insert(token.to_string());
    }

    /// Remove (consume) a bootstrap token (one-time use).
    pub fn consume_bootstrap_token(&self, token: &str) -> bool {
        self.bootstrap_tokens.write().remove(token)
    }
}

// ──── Auth state management ────

#[tonic::async_trait]
impl AuthTrait for AuthService {
    async fn auth_enable(
        &self,
        _request: tonic::Request<AuthEnableRequest>,
    ) -> Result<tonic::Response<AuthEnableResponse>, tonic::Status> {
        self.auth_manager.enable();
        tracing::info!("Auth enabled");
        Ok(tonic::Response::new(AuthEnableResponse {}))
    }

    async fn auth_disable(
        &self,
        _request: tonic::Request<AuthDisableRequest>,
    ) -> Result<tonic::Response<AuthDisableResponse>, tonic::Status> {
        self.auth_manager.disable();
        tracing::info!("Auth disabled");
        Ok(tonic::Response::new(AuthDisableResponse {}))
    }

    async fn auth_status(
        &self,
        _request: tonic::Request<AuthStatusRequest>,
    ) -> Result<tonic::Response<AuthStatusResponse>, tonic::Status> {
        Ok(tonic::Response::new(AuthStatusResponse {
            enabled: self.auth_manager.is_enabled(),
        }))
    }

    // ──── User management ────

    async fn user_add(
        &self,
        request: tonic::Request<UserAddRequest>,
    ) -> Result<tonic::Response<UserAddResponse>, tonic::Status> {
        let req = request.into_inner();
        self.auth_manager
            .user_add(&req.name, &req.password)
            .map_err(|e| tonic::Status::already_exists(e.to_string()))?;
        tracing::info!("User added: {}", req.name);
        Ok(tonic::Response::new(UserAddResponse {}))
    }

    async fn user_delete(
        &self,
        request: tonic::Request<UserDeleteRequest>,
    ) -> Result<tonic::Response<UserDeleteResponse>, tonic::Status> {
        let req = request.into_inner();
        self.auth_manager
            .user_delete(&req.name)
            .map_err(|e| tonic::Status::not_found(e.to_string()))?;
        tracing::info!("User deleted: {}", req.name);
        Ok(tonic::Response::new(UserDeleteResponse {}))
    }

    async fn user_change_password(
        &self,
        request: tonic::Request<UserChangePasswordRequest>,
    ) -> Result<tonic::Response<UserChangePasswordResponse>, tonic::Status> {
        let req = request.into_inner();
        self.auth_manager
            .user_change_password(&req.name, &req.password)
            .map_err(|e| tonic::Status::not_found(e.to_string()))?;
        tracing::info!("Password changed for user: {}", req.name);
        Ok(tonic::Response::new(UserChangePasswordResponse {}))
    }

    async fn user_list(
        &self,
        _request: tonic::Request<UserListRequest>,
    ) -> Result<tonic::Response<UserListResponse>, tonic::Status> {
        let usernames = self.auth_manager.user_list();
        let users = usernames
            .into_iter()
            .map(|name| {
                let roles = self
                    .auth_manager
                    .user_get_roles(&name)
                    .unwrap_or_default();
                User {
                    name,
                    roles,
                }
            })
            .collect();
        Ok(tonic::Response::new(UserListResponse { users }))
    }

    async fn user_get(
        &self,
        request: tonic::Request<UserGetRequest>,
    ) -> Result<tonic::Response<UserGetResponse>, tonic::Status> {
        let req = request.into_inner();
        let roles = self
            .auth_manager
            .user_get_roles(&req.name)
            .map_err(|e| tonic::Status::not_found(e.to_string()))?;
        Ok(tonic::Response::new(UserGetResponse { roles }))
    }

    // ──── Role management ────

    async fn role_add(
        &self,
        request: tonic::Request<RoleAddRequest>,
    ) -> Result<tonic::Response<RoleAddResponse>, tonic::Status> {
        let req = request.into_inner();
        self.auth_manager
            .role_add(&req.name)
            .map_err(|e| tonic::Status::already_exists(e.to_string()))?;
        tracing::info!("Role added: {}", req.name);
        Ok(tonic::Response::new(RoleAddResponse {}))
    }

    async fn role_delete(
        &self,
        request: tonic::Request<RoleDeleteRequest>,
    ) -> Result<tonic::Response<RoleDeleteResponse>, tonic::Status> {
        let req = request.into_inner();
        self.auth_manager
            .role_delete(&req.name)
            .map_err(|e| tonic::Status::not_found(e.to_string()))?;
        tracing::info!("Role deleted: {}", req.name);
        Ok(tonic::Response::new(RoleDeleteResponse {}))
    }

    async fn role_grant_permission(
        &self,
        request: tonic::Request<RoleGrantPermissionRequest>,
    ) -> Result<tonic::Response<RoleGrantPermissionResponse>, tonic::Status> {
        let req = request.into_inner();
        let perm = req
            .permission
            .ok_or_else(|| tonic::Status::invalid_argument("missing permission"))?;

        let perm_type = match PermissionType::try_from(perm.r#type) {
            Ok(PermissionType::Read) => crate::auth::manager::PermissionType::Read,
            Ok(PermissionType::Write) => crate::auth::manager::PermissionType::Write,
            Ok(PermissionType::Readwrite) => crate::auth::manager::PermissionType::ReadWrite,
            Err(_) => return Err(tonic::Status::invalid_argument("invalid permission type")),
        };

        self.auth_manager
            .role_grant_permission(&req.name, perm_type, perm.key, perm.range_end)
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(tonic::Response::new(RoleGrantPermissionResponse {}))
    }

    async fn role_revoke_permission(
        &self,
        request: tonic::Request<RoleRevokePermissionRequest>,
    ) -> Result<tonic::Response<RoleRevokePermissionResponse>, tonic::Status> {
        let req = request.into_inner();
        self.auth_manager
            .role_revoke_permission(&req.name, &req.key, &req.range_end)
            .map_err(|e| tonic::Status::not_found(e.to_string()))?;
        Ok(tonic::Response::new(RoleRevokePermissionResponse {}))
    }

    async fn role_list(
        &self,
        _request: tonic::Request<RoleListRequest>,
    ) -> Result<tonic::Response<RoleListResponse>, tonic::Status> {
        let roles = self.auth_manager.role_list();
        let proto_roles = roles
            .into_iter()
            .map(|r| {
                let grants: Vec<CapabilityGrant> = r
                    .capability_grants
                    .iter()
                    .map(|g| CapabilityGrant {
                        capability_id: g.capability_id.clone(),
                        scope: g.scope.clone(),
                    })
                    .collect();
                Role {
                    name: r.name,
                    permissions: r
                        .permissions
                        .into_iter()
                        .map(|p| Permission {
                            r#type: match p.perm_type {
                                crate::auth::manager::PermissionType::Read => PermissionType::Read as i32,
                                crate::auth::manager::PermissionType::Write => PermissionType::Write as i32,
                                crate::auth::manager::PermissionType::ReadWrite => PermissionType::Readwrite as i32,
                            },
                            key: p.key_prefix,
                            range_end: p.range_end,
                        })
                        .collect(),
                    capability_grants: grants,
                    high_sensitive: r.high_sensitive,
                }
            })
            .collect();
        Ok(tonic::Response::new(RoleListResponse { roles: proto_roles }))
    }

    // ──── User-Role assignment ────

    async fn user_grant_role(
        &self,
        request: tonic::Request<UserGrantRoleRequest>,
    ) -> Result<tonic::Response<UserGrantRoleResponse>, tonic::Status> {
        let req = request.into_inner();
        self.auth_manager
            .user_grant_role(&req.user, &req.role)
            .map_err(|e| tonic::Status::not_found(e.to_string()))?;
        tracing::info!("Role '{}' granted to user '{}'", req.role, req.user);
        Ok(tonic::Response::new(UserGrantRoleResponse {}))
    }

    async fn user_revoke_role(
        &self,
        request: tonic::Request<UserRevokeRoleRequest>,
    ) -> Result<tonic::Response<UserRevokeRoleResponse>, tonic::Status> {
        let req = request.into_inner();
        self.auth_manager
            .user_revoke_role(&req.user, &req.role)
            .map_err(|e| tonic::Status::not_found(e.to_string()))?;
        tracing::info!("Role '{}' revoked from user '{}'", req.role, req.user);
        Ok(tonic::Response::new(UserRevokeRoleResponse {}))
    }

    // ──── Authentication ────

    async fn authenticate(
        &self,
        request: tonic::Request<AuthenticateRequest>,
    ) -> Result<tonic::Response<AuthenticateResponse>, tonic::Status> {
        let req = request.into_inner();

        // Verify password
        self.auth_manager
            .authenticate(&req.name, &req.password)
            .map_err(|e| tonic::Status::unauthenticated(e.to_string()))?;

        // Issue simple token (legacy)
        let auth_token = self.token_manager.issue_token(&req.name);

        // Get user's roles
        let roles = self
            .auth_manager
            .user_get_roles(&req.name)
            .unwrap_or_default();

        // Issue CCT if signing keyring is available
        let (cct, expires_at) = if let Some(ref keyring) = self.signing_keyring {
            let active_key = keyring.active_key();
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            let exp = now + 3600; // 1 hour TTL

            let header = CctHeader {
                alg: "HMAC-SHA256".to_string(),
                typ: "CCT".to_string(),
                kid: active_key.key_id.clone(),
            };

            let payload = CctPayload {
                jti: format!("tok_{}", Uuid::new_v4()),
                iss: "coord-cluster".to_string(),
                sub: req.name.clone(),
                aud: vec!["coord-agent".to_string()],
                iat: now,
                exp,
                roles: roles.clone(),
                scope_overrides: std::collections::HashMap::new(),
            };

            match encode_cct(&header, &payload, &active_key.key_bytes) {
                Ok(cct) => (cct, exp),
                Err(e) => {
                    tracing::error!("CCT encoding failed: {e}");
                    (String::new(), 0)
                }
            }
        } else {
            (String::new(), 0)
        };

        tracing::info!(
            "User '{}' authenticated, token issued, cct={}",
            req.name,
            if cct.is_empty() { "none" } else { "issued" }
        );

        Ok(tonic::Response::new(AuthenticateResponse {
            token: auth_token.token,
            cct,
            expires_at,
            roles,
        }))
    }

    // ──── CCT v3: Agent role sync ────

    async fn list_roles(
        &self,
        _request: tonic::Request<ListRolesRequest>,
    ) -> Result<tonic::Response<ListRolesResponse>, tonic::Status> {
        let roles = self.auth_manager.role_list();
        let proto_roles = roles
            .into_iter()
            .map(|r| {
                let grants: Vec<CapabilityGrant> = r
                    .capability_grants
                    .iter()
                    .map(|g| CapabilityGrant {
                        capability_id: g.capability_id.clone(),
                        scope: g.scope.clone(),
                    })
                    .collect();
                Role {
                    name: r.name,
                    permissions: r
                        .permissions
                        .into_iter()
                        .map(|p| Permission {
                            r#type: match p.perm_type {
                                crate::auth::manager::PermissionType::Read => PermissionType::Read as i32,
                                crate::auth::manager::PermissionType::Write => PermissionType::Write as i32,
                                crate::auth::manager::PermissionType::ReadWrite => PermissionType::Readwrite as i32,
                            },
                            key: p.key_prefix,
                            range_end: p.range_end,
                        })
                        .collect(),
                    capability_grants: grants,
                    high_sensitive: r.high_sensitive,
                }
            })
            .collect();
        Ok(tonic::Response::new(ListRolesResponse {
            roles: proto_roles,
            version: 1,
        }))
    }

    // ──── CCT v3: Token revocation delta sync ────

    async fn get_revocation_delta(
        &self,
        _request: tonic::Request<GetRevocationDeltaRequest>,
    ) -> Result<tonic::Response<GetRevocationDeltaResponse>, tonic::Status> {
        // Phase 2: Implement bloom filter + delta sync
        Ok(tonic::Response::new(GetRevocationDeltaResponse {
            revoked_jtis: vec![],
            current_version: 0,
        }))
    }

    // ──── CCT v3: Agent Bootstrap (Phase 2.6) ────

    async fn bootstrap(
        &self,
        request: tonic::Request<BootstrapRequest>,
    ) -> Result<tonic::Response<BootstrapResponse>, tonic::Status> {
        let req = request.into_inner();

        if req.bootstrap_token.is_empty() {
            return Err(tonic::Status::invalid_argument("bootstrap_token is required"));
        }

        // Validate bootstrap token against whitelist
        if !self.consume_bootstrap_token(&req.bootstrap_token) {
            return Err(tonic::Status::permission_denied(
                "invalid or already consumed bootstrap token",
            ));
        }

        // Issue a short-lived CCT for the agent
        let (cct, expires_at) = if let Some(ref keyring) = self.signing_keyring {
            let active_key = keyring.active_key();
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            let exp = now + 600; // 10 minutes TTL for bootstrap CCT

            let header = CctHeader {
                alg: "HMAC-SHA256".to_string(),
                typ: "CCT".to_string(),
                kid: active_key.key_id.clone(),
            };

            let payload = CctPayload {
                jti: format!("bootstrap_{}", Uuid::new_v4()),
                iss: "coord-cluster".to_string(),
                sub: "coord-agent".to_string(),
                aud: vec!["coord-agent".to_string()],
                iat: now,
                exp,
                roles: vec!["agent-bootstrap".to_string()],
                scope_overrides: std::collections::HashMap::new(),
            };

            match encode_cct(&header, &payload, &active_key.key_bytes) {
                Ok(cct) => (cct, exp),
                Err(e) => {
                    tracing::error!("Bootstrap CCT encoding failed: {e}");
                    return Err(tonic::Status::internal("CCT encoding failed"));
                }
            }
        } else {
            return Err(tonic::Status::internal(
                "CCT signing not configured on server",
            ));
        };

        tracing::info!("Agent bootstrapped successfully");

        Ok(tonic::Response::new(BootstrapResponse { cct, expires_at }))
    }
}

// ──── Tests (Phase 2.3: CCT issuance) ────

#[cfg(test)]
mod cct_tests {
    use super::*;

    /// Build a test AuthService with CCT signing enabled.
    fn build_service_with_cct() -> AuthService {
        let auth_manager = Arc::new(AuthManager::new());
        let token_manager = Arc::new(TokenManager::with_defaults());

        // Create test root key material (32 bytes)
        let root_key = vec![0xABu8; 32];
        let signing_keyring = Arc::new(
            TokenSigningKeyring::new(root_key).expect("keyring creation should succeed"),
        );

        AuthService::with_cct_signing(auth_manager, token_manager, signing_keyring)
    }

    /// Verify that calling authenticate without a signing keyring still works
    /// (returns empty CCT).
    #[test]
    fn test_authenticate_without_cct_signing() {
        let auth_manager = Arc::new(AuthManager::new());
        let token_manager = Arc::new(TokenManager::with_defaults());
        let svc = AuthService::new(auth_manager, token_manager);

        // Create a user first
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            // Add a test user via AuthManager directly
            svc.auth_manager.user_add("testuser", "password123").unwrap();

            let req = tonic::Request::new(AuthenticateRequest {
                name: "testuser".to_string(),
                password: "password123".to_string(),
            });

            let resp = svc.authenticate(req).await.expect("authenticate should succeed");
            let inner = resp.into_inner();

            // Legacy token should always be present
            assert!(!inner.token.is_empty(), "legacy token should be present");
            assert!(inner.token.starts_with("coord_"), "legacy token should have coord_ prefix");

            // CCT should be empty when no signing keyring
            assert!(inner.cct.is_empty(), "CCT should be empty without signing keyring");
        });
    }

    /// Verify that authenticate with CCT signing returns a valid CCT.
    #[test]
    fn test_authenticate_returns_cct() {
        let svc = build_service_with_cct();
        let signing_keyring = svc.signing_keyring.as_ref().unwrap();
        let active_key = signing_keyring.active_key();

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            svc.auth_manager.user_add("cctuser", "secret").unwrap();
            svc.auth_manager.role_add("reader").unwrap();
            svc.auth_manager
                .user_grant_role("cctuser", "reader")
                .unwrap();

            let req = tonic::Request::new(AuthenticateRequest {
                name: "cctuser".to_string(),
                password: "secret".to_string(),
            });

            let resp = svc.authenticate(req).await.expect("authenticate should succeed");
            let inner = resp.into_inner();

            // CCT should be present
            assert!(!inner.cct.is_empty(), "CCT should be issued when signing keyring is configured");
            assert!(inner.cct.starts_with("eyJ"), "CCT should start with base64url JSON header");

            // expires_at should be in the future
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            assert!(inner.expires_at > now, "expires_at should be in the future");

            // Roles should be present
            assert!(inner.roles.contains(&"reader".to_string()), "roles should contain 'reader'");
        });
    }

    /// Verify that the issued CCT can be decoded and verified.
    #[test]
    fn test_issued_cct_is_verifiable() {
        let svc = build_service_with_cct();
        let signing_keyring = svc.signing_keyring.as_ref().unwrap();
        let active_key = signing_keyring.active_key();

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            svc.auth_manager.user_add("verifyuser", "pass").unwrap();
            svc.auth_manager.role_add("writer").unwrap();
            svc.auth_manager
                .user_grant_role("verifyuser", "writer")
                .unwrap();

            let req = tonic::Request::new(AuthenticateRequest {
                name: "verifyuser".to_string(),
                password: "pass".to_string(),
            });

            let resp = svc.authenticate(req).await.unwrap();
            let inner = resp.into_inner();
            let cct = inner.cct;

            // Decode and verify the CCT
            let decoded = coord_core::auth::cct::decode_cct(&cct, &active_key.key_bytes)
                .expect("CCT should be decodable and verifiable");

            assert_eq!(decoded.header.kid, active_key.key_id);
            assert_eq!(decoded.payload.sub, "verifyuser");
            assert!(decoded.payload.roles.contains(&"writer".to_string()));
            assert_eq!(decoded.payload.iss, "coord-cluster");
            assert!(!decoded.payload.jti.is_empty());
        });
    }

    /// Verify authentication fails with wrong password.
    #[test]
    fn test_authenticate_wrong_password_fails() {
        let svc = build_service_with_cct();

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            svc.auth_manager.user_add("wrongpwuser", "correct").unwrap();

            let req = tonic::Request::new(AuthenticateRequest {
                name: "wrongpwuser".to_string(),
                password: "wrong".to_string(),
            });

            let result = svc.authenticate(req).await;
            assert!(result.is_err(), "wrong password should fail authentication");
        });
    }

    // ──── Phase 2.4: Role→Capability storage ────

    #[test]
    fn test_role_grant_capability() {
        let svc = build_service_with_cct();

        svc.auth_manager.role_add("svc-writer").unwrap();
        svc.auth_manager
            .role_grant_capability("svc-writer", "data:kv:write", "/app/order/")
            .unwrap();

        let grants = svc
            .auth_manager
            .role_get_capability_grants("svc-writer")
            .unwrap();
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].capability_id, "data:kv:write");
        assert_eq!(grants[0].scope, "/app/order/");
    }

    #[test]
    fn test_role_grant_duplicate_capability_fails() {
        let svc = build_service_with_cct();

        svc.auth_manager.role_add("dup-test").unwrap();
        svc.auth_manager
            .role_grant_capability("dup-test", "data:kv:read", "")
            .unwrap();
        let result = svc
            .auth_manager
            .role_grant_capability("dup-test", "data:kv:read", "");
        assert!(result.is_err(), "duplicate capability grant should fail");
    }

    #[test]
    fn test_role_revoke_capability() {
        let svc = build_service_with_cct();

        svc.auth_manager.role_add("temp-role").unwrap();
        svc.auth_manager
            .role_grant_capability("temp-role", "data:kv:write", "/tmp/")
            .unwrap();
        svc.auth_manager
            .role_revoke_capability("temp-role", "data:kv:write", "/tmp/")
            .unwrap();

        let grants = svc
            .auth_manager
            .role_get_capability_grants("temp-role")
            .unwrap();
        assert!(grants.is_empty());
    }

    #[test]
    fn test_role_high_sensitive_flag() {
        let svc = build_service_with_cct();

        svc.auth_manager.role_add("admin-role").unwrap();
        assert!(!svc.auth_manager.role_list().iter().any(|r| r.name == "admin-role" && r.high_sensitive));

        svc.auth_manager
            .role_set_high_sensitive("admin-role", true)
            .unwrap();

        let admin_role = svc.auth_manager.role_list()
            .into_iter()
            .find(|r| r.name == "admin-role")
            .unwrap();
        assert!(admin_role.high_sensitive);
    }

    #[test]
    fn test_list_roles_includes_capability_grants() {
        let svc = build_service_with_cct();

        svc.auth_manager.role_add("full-role").unwrap();
        svc.auth_manager
            .role_grant_capability("full-role", "data:kv:read", "/app/")
            .unwrap();
        svc.auth_manager
            .role_grant_capability("full-role", "data:kv:write", "/app/")
            .unwrap();
        svc.auth_manager
            .role_set_high_sensitive("full-role", true)
            .unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let req = tonic::Request::new(ListRolesRequest {});
            let resp = svc.list_roles(req).await.unwrap();
            let inner = resp.into_inner();

            let full_role = inner.roles.iter().find(|r| r.name == "full-role").unwrap();
            assert_eq!(full_role.capability_grants.len(), 2);
            assert!(full_role.high_sensitive);
        });
    }

    // ──── Phase 2.6: Bootstrap RPC ────

    #[test]
    fn test_bootstrap_with_valid_token() {
        let svc = build_service_with_cct();
        svc.add_bootstrap_token("my-bootstrap-secret");

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let req = tonic::Request::new(BootstrapRequest {
                bootstrap_token: "my-bootstrap-secret".to_string(),
            });

            let resp = svc.bootstrap(req).await.expect("bootstrap should succeed");
            let inner = resp.into_inner();

            assert!(!inner.cct.is_empty(), "CCT should be returned");
            assert!(inner.cct.starts_with("eyJ"), "CCT should be base64url JSON");
            assert!(inner.expires_at > 0);
        });
    }

    #[test]
    fn test_bootstrap_with_invalid_token_fails() {
        let svc = build_service_with_cct();
        svc.add_bootstrap_token("valid-token");

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let req = tonic::Request::new(BootstrapRequest {
                bootstrap_token: "wrong-token".to_string(),
            });

            let result = svc.bootstrap(req).await;
            assert!(result.is_err(), "invalid bootstrap token should fail");
        });
    }

    #[test]
    fn test_bootstrap_token_is_one_time_use() {
        let svc = build_service_with_cct();
        svc.add_bootstrap_token("onetime-token");

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            // First use: succeeds
            let req = tonic::Request::new(BootstrapRequest {
                bootstrap_token: "onetime-token".to_string(),
            });
            assert!(svc.bootstrap(req).await.is_ok());

            // Second use: fails (already consumed)
            let req = tonic::Request::new(BootstrapRequest {
                bootstrap_token: "onetime-token".to_string(),
            });
            let result = svc.bootstrap(req).await;
            assert!(result.is_err(), "one-time token should be consumed");
        });
    }

    #[test]
    fn test_bootstrap_with_empty_token_fails() {
        let svc = build_service_with_cct();

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let req = tonic::Request::new(BootstrapRequest {
                bootstrap_token: "".to_string(),
            });
            let result = svc.bootstrap(req).await;
            assert!(result.is_err(), "empty token should fail");
        });
    }

    #[test]
    fn test_bootstrap_cct_is_verifiable() {
        let svc = build_service_with_cct();
        let signing_keyring = svc.signing_keyring.as_ref().unwrap();
        let active_key = signing_keyring.active_key();
        svc.add_bootstrap_token("verify-bootstrap");

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let req = tonic::Request::new(BootstrapRequest {
                bootstrap_token: "verify-bootstrap".to_string(),
            });
            let resp = svc.bootstrap(req).await.unwrap();
            let inner = resp.into_inner();

            // Verify the bootstrap CCT
            let decoded = coord_core::auth::cct::decode_cct(&inner.cct, &active_key.key_bytes)
                .expect("bootstrap CCT should be verifiable");
            assert_eq!(decoded.header.kid, active_key.key_id);
            assert_eq!(decoded.payload.sub, "coord-agent");
            assert!(decoded.payload.roles.contains(&"agent-bootstrap".to_string()));
            // Bootstrap CCT should be short-lived (10 min)
            assert!(decoded.payload.exp - decoded.payload.iat <= 600);
        });
    }
}

// ──── 测试 ────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_service_new() {
        let auth_mgr = Arc::new(crate::auth::manager::AuthManager::new());
        let token_mgr = Arc::new(crate::auth::token::TokenManager::with_defaults());
        let _service = AuthService::new(auth_mgr, token_mgr);
    }

    #[test]
    fn test_auth_service_new_with_custom_token_manager() {
        let auth_mgr = Arc::new(crate::auth::manager::AuthManager::new());
        // 0-second TTL for testing immediate expiry
        let token_mgr = Arc::new(crate::auth::token::TokenManager::new(0, 0));
        let _service = AuthService::new(auth_mgr, token_mgr);
    }
}
