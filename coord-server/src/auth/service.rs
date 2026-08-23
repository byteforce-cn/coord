// Auth gRPC Service — implements auth.proto Auth service (ADP §14)
//
// Provides:
// - Auth enable/disable/status
// - User CRUD + password management
// - Role CRUD + permission management
// - User-role assignment
// - Authentication (password → token + CCT)

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use coord_proto::auth::auth_server::Auth as AuthTrait;
use coord_proto::auth::*;
use parking_lot::RwLock;
use uuid::Uuid;

use coord_core::auth::cct::{encode_cct, CctHeader, CctPayload};

use crate::auth::manager::{hash_password_argon2id, AuthManager};
use crate::auth::revocation::RevocationStore;
use crate::auth::token::TokenManager;
use crate::auth::token_signing::TokenSigningKeyring;
use crate::raft::type_config::AuthOp;

// ──── AuthOp 提案器（P0-C.2：管理操作入 raft 日志）────

/// 将 AuthOp 经 raft 提案（`Command::Auth`）执行；由持有 Raft 句柄的层实现
/// （`coord-server/src/server/mod.rs` 对 `CoordNode` 实现）。
#[async_trait::async_trait]
pub trait AuthOpProposer: Send + Sync {
    /// 提案并等待应用，返回分配到的 revision。
    async fn propose_auth_op(&self, op: AuthOp) -> Result<u64, String>;
}

// ──── 登录限流（P0-C.6，F1：per-user + per-IP 内存 token bucket）────

/// 简单令牌桶：容量 5，每 2s 补充 1 个（防爆破，同时不误伤正常登录重试）。
const BUCKET_CAPACITY: f64 = 5.0;
const BUCKET_REFILL_PER_SEC: f64 = 0.5;

struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new() -> Self {
        Self {
            tokens: BUCKET_CAPACITY,
            last_refill: Instant::now(),
        }
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * BUCKET_REFILL_PER_SEC).min(BUCKET_CAPACITY);
        self.last_refill = now;
    }

    /// 消费一个令牌；返回 false 表示已耗尽（应拒绝本次尝试）。
    fn try_take(&mut self) -> bool {
        self.refill();
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// 登录失败限流器：key = `u:{username}` / `ip:{addr}`，成功后清除该用户计数。
struct LoginRateLimiter {
    buckets: RwLock<HashMap<String, TokenBucket>>,
}

impl LoginRateLimiter {
    fn new() -> Self {
        Self {
            buckets: RwLock::new(HashMap::new()),
        }
    }

    /// 尝试消费（用户名 + IP 双重）。任一耗尽 → false（拒绝）。
    fn allow_attempt(&self, username: &str, ip: Option<std::net::SocketAddr>) -> bool {
        let mut buckets = self.buckets.write();
        let user_ok = buckets
            .entry(format!("u:{username}"))
            .or_insert_with(TokenBucket::new)
            .try_take();
        let ip_ok = match ip {
            Some(addr) => buckets
                .entry(format!("ip:{addr}"))
                .or_insert_with(TokenBucket::new)
                .try_take(),
            None => true,
        };
        user_ok && ip_ok
    }

    /// 登录成功后清除该用户失败计数（不惩罚正常用户）。
    fn clear_user(&self, username: &str) {
        self.buckets.write().remove(&format!("u:{username}"));
    }

    /// 定期清理过期桶（防止内存无限增长）。
    fn _prune(&self) {
        let cutoff = Instant::now() - Duration::from_secs(600);
        self.buckets.write().retain(|_, b| b.last_refill > cutoff);
    }
}

/// gRPC Auth service implementation
#[derive(Clone)]
pub struct AuthService {
    auth_manager: Arc<AuthManager>,
    token_manager: Arc<TokenManager>,
    /// Token signing keyring for CCT issuance (Phase 2.3)
    signing_keyring: Option<Arc<TokenSigningKeyring>>,
    /// Bootstrap token whitelist (Phase 2.6)
    bootstrap_tokens: Arc<RwLock<HashSet<String>>>,
    /// 登录失败限流（P0-C.6）
    login_limiter: Arc<LoginRateLimiter>,
    /// AuthOp 提案器（P0-C.2：Some = raft 模式，管理操作入日志）
    auth_proposer: Option<Arc<dyn AuthOpProposer>>,
    /// 吊销登记存储（P0-C.5：delta 同步 + 无 proposer 时的直接吊销）
    revocation_store: Option<Arc<RevocationStore>>,
    /// P2-08：审计日志（认证/refresh 成功与失败事件；可选）
    audit: Option<Arc<crate::audit::AuditLogger>>,
}

impl AuthService {
    pub fn new(auth_manager: Arc<AuthManager>, token_manager: Arc<TokenManager>) -> Self {
        Self {
            auth_manager,
            token_manager,
            signing_keyring: None,
            bootstrap_tokens: Arc::new(RwLock::new(HashSet::new())),
            login_limiter: Arc::new(LoginRateLimiter::new()),
            auth_proposer: None,
            revocation_store: None,
            audit: None,
        }
    }

    /// P2-08：挂载审计日志器。
    pub fn with_audit_logger(mut self, logger: Arc<crate::audit::AuditLogger>) -> Self {
        self.audit = Some(logger);
        self
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
            bootstrap_tokens: Arc::new(RwLock::new(HashSet::new())),
            login_limiter: Arc::new(LoginRateLimiter::new()),
            auth_proposer: None,
            revocation_store: None,
            audit: None,
        }
    }

    /// 挂载 AuthOp 提案器（P0-C.2）：设置后管理操作经 raft 提交。
    pub fn with_proposer(mut self, proposer: Arc<dyn AuthOpProposer>) -> Self {
        self.auth_proposer = Some(proposer);
        self
    }

    /// 挂载吊销登记存储（P0-C.5）。
    pub fn with_revocation_store(mut self, store: Arc<RevocationStore>) -> Self {
        self.revocation_store = Some(store);
        self
    }

    /// 提案 AuthOp（有 proposer）或直接改内存视图（无 proposer，兼容测试/单机）。
    async fn apply_auth_op(&self, op: AuthOp) -> Result<(), tonic::Status> {
        match &self.auth_proposer {
            Some(proposer) => proposer
                .propose_auth_op(op.clone())
                .await
                .map(|_| ())
                .map_err(|e| tonic::Status::internal(format!("raft auth write failed: {e}"))),
            None => {
                self.auth_manager.apply_auth_op_to_view(&op);
                Ok(())
            }
        }
    }

    /// 吊销 token（P0-C.5）：CCT（`eyJ` 前缀）按 jti 经 raft 登记吊销；
    /// 遗留 token 走 token_manager。
    pub async fn revoke_token(&self, token: &str) -> Result<(), String> {
        if token.starts_with("eyJ") {
            let keyring = self
                .signing_keyring
                .as_ref()
                .ok_or_else(|| "CCT signing not configured on server".to_string())?;
            let cct = coord_core::auth::cct::decode_cct(token, &keyring.active_key().key_bytes)
                .map_err(|e| format!("decode CCT: {e}"))?;
            let op = AuthOp::RevokeJti {
                jti: cct.payload.jti.clone(),
            };
            match &self.auth_proposer {
                Some(proposer) => {
                    proposer
                        .propose_auth_op(op)
                        .await
                        .map(|_| ())
                        .map_err(|e| format!("raft revoke failed: {e}"))?;
                }
                None => {
                    if let Some(ref store) = self.revocation_store {
                        store.revoke(&cct.payload.jti);
                    }
                }
            }
        } else {
            self.token_manager.revoke(token);
        }
        Ok(())
    }

    /// Add a bootstrap token to the whitelist.
    pub fn add_bootstrap_token(&self, token: &str) {
        self.bootstrap_tokens.write().insert(token.to_string());
    }

    /// Remove (consume) a bootstrap token (one-time use).
    pub fn consume_bootstrap_token(&self, token: &str) -> bool {
        self.bootstrap_tokens.write().remove(token)
    }

    /// P2-07：将签发的会话经 raft 持久化（proposer None 时仅本地视图）。
    async fn persist_session(
        &self,
        hash_hex: &str,
        username: &str,
        expires_at_unix: u64,
        is_refresh: bool,
    ) -> Result<(), tonic::Status> {
        match &self.auth_proposer {
            Some(p) => p
                .propose_auth_op(AuthOp::IssueSession {
                    hash_hex: hash_hex.to_string(),
                    username: username.to_string(),
                    expires_at_unix,
                    is_refresh,
                })
                .await
                .map(|_| ())
                .map_err(|e| tonic::Status::internal(format!("persist session: {e}"))),
            None => {
                self.token_manager.register_session(
                    hash_hex,
                    username,
                    expires_at_unix,
                    is_refresh,
                );
                Ok(())
            }
        }
    }

    /// 签发 CCT（signing keyring 可用时），返回 (cct, expires_at)。
    fn issue_cct_for(&self, username: &str, roles: &[String]) -> (String, i64) {
        let Some(ref keyring) = self.signing_keyring else {
            return (String::new(), 0);
        };
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
            sub: username.to_string(),
            aud: vec!["coord-agent".to_string()],
            iat: now,
            exp,
            roles: roles.to_vec(),
            scope_overrides: std::collections::HashMap::new(),
        };

        match encode_cct(&header, &payload, &active_key.key_bytes) {
            Ok(cct) => (cct, exp),
            Err(e) => {
                tracing::error!("CCT encoding failed: {e}");
                (String::new(), 0)
            }
        }
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
        // 判重（视图由 apply 同步保持）
        if self.auth_manager.user_list().iter().any(|u| u == &req.name) {
            return Err(tonic::Status::already_exists(format!(
                "user '{}' already exists",
                req.name
            )));
        }
        let hash = String::from_utf8_lossy(
            &hash_password_argon2id(&req.password).map_err(tonic::Status::internal)?,
        )
        .to_string();
        self.apply_auth_op(AuthOp::UserAdd {
            name: req.name.clone(),
            hash,
            roles: vec![],
        })
        .await?;
        tracing::info!("User added: {}", req.name);
        Ok(tonic::Response::new(UserAddResponse {}))
    }

    async fn user_delete(
        &self,
        request: tonic::Request<UserDeleteRequest>,
    ) -> Result<tonic::Response<UserDeleteResponse>, tonic::Status> {
        let req = request.into_inner();
        if !self.auth_manager.user_list().iter().any(|u| u == &req.name) {
            return Err(tonic::Status::not_found(format!(
                "user '{}' not found",
                req.name
            )));
        }
        self.apply_auth_op(AuthOp::UserDelete {
            name: req.name.clone(),
        })
        .await?;
        tracing::info!("User deleted: {}", req.name);
        Ok(tonic::Response::new(UserDeleteResponse {}))
    }

    async fn user_change_password(
        &self,
        request: tonic::Request<UserChangePasswordRequest>,
    ) -> Result<tonic::Response<UserChangePasswordResponse>, tonic::Status> {
        let req = request.into_inner();
        if !self.auth_manager.user_list().iter().any(|u| u == &req.name) {
            return Err(tonic::Status::not_found(format!(
                "user '{}' not found",
                req.name
            )));
        }
        let hash = String::from_utf8_lossy(
            &hash_password_argon2id(&req.password).map_err(tonic::Status::internal)?,
        )
        .to_string();
        self.apply_auth_op(AuthOp::UserSetPassword {
            name: req.name.clone(),
            hash,
        })
        .await?;
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
                let roles = self.auth_manager.user_get_roles(&name).unwrap_or_default();
                User { name, roles }
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
        if self
            .auth_manager
            .role_list()
            .iter()
            .any(|r| r.name == req.name)
        {
            return Err(tonic::Status::already_exists(format!(
                "role '{}' already exists",
                req.name
            )));
        }
        self.apply_auth_op(AuthOp::RoleAdd {
            role: req.name.clone(),
        })
        .await?;
        tracing::info!("Role added: {}", req.name);
        Ok(tonic::Response::new(RoleAddResponse {}))
    }

    async fn role_delete(
        &self,
        request: tonic::Request<RoleDeleteRequest>,
    ) -> Result<tonic::Response<RoleDeleteResponse>, tonic::Status> {
        let req = request.into_inner();
        if !self
            .auth_manager
            .role_list()
            .iter()
            .any(|r| r.name == req.name)
        {
            return Err(tonic::Status::not_found(format!(
                "role '{}' not found",
                req.name
            )));
        }
        self.apply_auth_op(AuthOp::RoleDelete {
            role: req.name.clone(),
        })
        .await?;
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

        let perm_type: u8 = match PermissionType::try_from(perm.r#type) {
            Ok(PermissionType::Read) => 0,
            Ok(PermissionType::Write) => 1,
            Ok(PermissionType::Readwrite) => 2,
            Err(_) => return Err(tonic::Status::invalid_argument("invalid permission type")),
        };

        self.apply_auth_op(AuthOp::RoleGrantPermission {
            role: req.name.clone(),
            perm_type,
            key: perm.key,
            range_end: perm.range_end,
        })
        .await?;
        Ok(tonic::Response::new(RoleGrantPermissionResponse {}))
    }

    async fn role_revoke_permission(
        &self,
        request: tonic::Request<RoleRevokePermissionRequest>,
    ) -> Result<tonic::Response<RoleRevokePermissionResponse>, tonic::Status> {
        let req = request.into_inner();
        self.apply_auth_op(AuthOp::RoleRevokePermission {
            role: req.name.clone(),
            key: req.key,
            range_end: req.range_end,
        })
        .await?;
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
                                crate::auth::manager::PermissionType::Read => {
                                    PermissionType::Read as i32
                                }
                                crate::auth::manager::PermissionType::Write => {
                                    PermissionType::Write as i32
                                }
                                crate::auth::manager::PermissionType::ReadWrite => {
                                    PermissionType::Readwrite as i32
                                }
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
        Ok(tonic::Response::new(RoleListResponse {
            roles: proto_roles,
        }))
    }

    // ──── User-Role assignment ────

    async fn user_grant_role(
        &self,
        request: tonic::Request<UserGrantRoleRequest>,
    ) -> Result<tonic::Response<UserGrantRoleResponse>, tonic::Status> {
        let req = request.into_inner();
        if !self.auth_manager.user_list().iter().any(|u| u == &req.user) {
            return Err(tonic::Status::not_found(format!(
                "user '{}' not found",
                req.user
            )));
        }
        if !self
            .auth_manager
            .role_list()
            .iter()
            .any(|r| r.name == req.role)
        {
            return Err(tonic::Status::not_found(format!(
                "role '{}' not found",
                req.role
            )));
        }
        self.apply_auth_op(AuthOp::UserGrantRole {
            name: req.user.clone(),
            role: req.role.clone(),
        })
        .await?;
        tracing::info!("Role '{}' granted to user '{}'", req.role, req.user);
        Ok(tonic::Response::new(UserGrantRoleResponse {}))
    }

    async fn user_revoke_role(
        &self,
        request: tonic::Request<UserRevokeRoleRequest>,
    ) -> Result<tonic::Response<UserRevokeRoleResponse>, tonic::Status> {
        let req = request.into_inner();
        if !self.auth_manager.user_list().iter().any(|u| u == &req.user) {
            return Err(tonic::Status::not_found(format!(
                "user '{}' not found",
                req.user
            )));
        }
        self.apply_auth_op(AuthOp::UserRevokeRole {
            name: req.user.clone(),
            role: req.role.clone(),
        })
        .await?;
        tracing::info!("Role '{}' revoked from user '{}'", req.role, req.user);
        Ok(tonic::Response::new(UserRevokeRoleResponse {}))
    }

    // ──── Authentication ────

    async fn authenticate(
        &self,
        request: tonic::Request<AuthenticateRequest>,
    ) -> Result<tonic::Response<AuthenticateResponse>, tonic::Status> {
        // 登录限流（P0-C.6）：per-user + per-IP 令牌桶，失败消费、成功清除
        let peer_ip = request.remote_addr();
        let req = request.into_inner();

        // Verify password
        if let Err(e) = self.auth_manager.authenticate(&req.name, &req.password) {
            if let Some(ref audit) = self.audit {
                audit.record_event(
                    &req.name,
                    "auth.authenticate",
                    &req.name,
                    crate::audit::RESULT_DENIED,
                    &e.to_string(),
                );
            }
            if !self.login_limiter.allow_attempt(&req.name, peer_ip) {
                return Err(tonic::Status::resource_exhausted(
                    "too many failed login attempts; retry later",
                ));
            }
            return Err(tonic::Status::unauthenticated(e.to_string()));
        }
        self.login_limiter.clear_user(&req.name);

        // Issue simple token (legacy) + refresh token（P2-07：单次使用、落盘）
        let auth_token = self.token_manager.issue_token(&req.name);
        let refresh_token = self.token_manager.issue_refresh_token(&req.name);

        // P2-07：会话经 raft 持久化（重启不失效；proposer None 时本地视图）
        self.persist_session(
            &auth_token.hash_hex,
            &req.name,
            auth_token.expires_at_unix,
            false,
        )
        .await?;
        self.persist_session(
            &refresh_token.hash_hex,
            &req.name,
            refresh_token.expires_at_unix,
            true,
        )
        .await?;

        // Get user's roles
        let roles = self
            .auth_manager
            .user_get_roles(&req.name)
            .unwrap_or_default();

        // Issue CCT if signing keyring is available
        let (cct, expires_at) = self.issue_cct_for(&req.name, &roles);

        tracing::info!(
            "User '{}' authenticated, token issued, cct={}",
            req.name,
            if cct.is_empty() { "none" } else { "issued" }
        );
        if let Some(ref audit) = self.audit {
            audit.record_event(
                &req.name,
                "auth.authenticate",
                &req.name,
                crate::audit::RESULT_SUCCESS,
                "",
            );
        }

        Ok(tonic::Response::new(AuthenticateResponse {
            token: auth_token.token,
            cct,
            expires_at,
            roles,
            refresh_token: refresh_token.token,
            refresh_expires_at: refresh_token.expires_at_unix as i64,
        }))
    }

    /// P2-07：refresh token 换新（单次使用，旧 refresh 消费后作废）。
    async fn refresh_token(
        &self,
        request: tonic::Request<RefreshTokenRequest>,
    ) -> Result<tonic::Response<RefreshTokenResponse>, tonic::Status> {
        let req = request.into_inner();

        // 校验 refresh token（存在、未过期、确为 refresh 类型）
        let (hash_hex, username, _exp) = self
            .token_manager
            .session_of_refresh(&req.refresh_token)
            .map_err(|e| {
                if let Some(ref audit) = self.audit {
                    audit.record_event(
                        "anonymous",
                        "auth.refresh",
                        "",
                        crate::audit::RESULT_DENIED,
                        &e.to_string(),
                    );
                }
                tonic::Status::unauthenticated(e.to_string())
            })?;

        // 单次使用：先消费旧 refresh（raft 持久化删除 + 各节点视图同步）
        match &self.auth_proposer {
            Some(p) => p
                .propose_auth_op(AuthOp::ConsumeSession {
                    hash_hex: hash_hex.clone(),
                })
                .await
                .map(|_| ())
                .map_err(|e| tonic::Status::internal(format!("consume session: {e}")))?,
            None => self.token_manager.remove_session(&hash_hex),
        }

        // 签发新 access + refresh 并落盘
        let access = self.token_manager.issue_token(&username);
        let refresh = self.token_manager.issue_refresh_token(&username);
        self.persist_session(&access.hash_hex, &username, access.expires_at_unix, false)
            .await?;
        self.persist_session(&refresh.hash_hex, &username, refresh.expires_at_unix, true)
            .await?;

        let roles = self
            .auth_manager
            .user_get_roles(&username)
            .unwrap_or_default();
        let (cct, expires_at) = self.issue_cct_for(&username, &roles);

        tracing::info!("User '{}' refreshed session", username);
        if let Some(ref audit) = self.audit {
            audit.record_event(
                &username,
                "auth.refresh",
                &username,
                crate::audit::RESULT_SUCCESS,
                "",
            );
        }
        Ok(tonic::Response::new(RefreshTokenResponse {
            token: access.token,
            cct,
            expires_at,
            roles,
            refresh_token: refresh.token,
            refresh_expires_at: refresh.expires_at_unix as i64,
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
                                crate::auth::manager::PermissionType::Read => {
                                    PermissionType::Read as i32
                                }
                                crate::auth::manager::PermissionType::Write => {
                                    PermissionType::Write as i32
                                }
                                crate::auth::manager::PermissionType::ReadWrite => {
                                    PermissionType::Readwrite as i32
                                }
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
        request: tonic::Request<GetRevocationDeltaRequest>,
    ) -> Result<tonic::Response<GetRevocationDeltaResponse>, tonic::Status> {
        // P0-C.5：从吊销登记存储取增量（agent 缓存按 delta 同步）
        let since = request.into_inner().since_version.max(0) as u64;
        let (events, current_version) = match &self.revocation_store {
            Some(store) => (store.get_delta_since(since), store.current_version()),
            None => (Vec::new(), 0),
        };
        let revoked_jtis: Vec<String> = events.into_iter().map(|e| e.jti).collect();
        Ok(tonic::Response::new(GetRevocationDeltaResponse {
            revoked_jtis,
            current_version: current_version as i64,
        }))
    }

    // ──── CCT v3: Agent Bootstrap (Phase 2.6) ────

    async fn bootstrap(
        &self,
        request: tonic::Request<BootstrapRequest>,
    ) -> Result<tonic::Response<BootstrapResponse>, tonic::Status> {
        let req = request.into_inner();

        if req.bootstrap_token.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "bootstrap_token is required",
            ));
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

    /// 登录限流：连续失败耗尽令牌 → RESOURCE_EXHAUSTED；成功登录清空计数。
    #[test]
    fn test_login_rate_limiter_exhausts_and_resets() {
        let limiter = LoginRateLimiter::new();
        let ip = "127.0.0.1:1234".parse().unwrap();
        // 容量 5：前 5 次失败允许，第 6 次开始拒绝
        for i in 0..5 {
            assert!(
                limiter.allow_attempt("attacker", Some(ip)),
                "attempt {i} should be allowed"
            );
        }
        assert!(
            !limiter.allow_attempt("attacker", Some(ip)),
            "bucket exhausted"
        );
        // 成功登录后清除该用户计数（但同 IP 仍被限流）
        limiter.clear_user("attacker");
        let ip2 = "127.0.0.1:9999".parse().unwrap();
        assert!(limiter.allow_attempt("attacker", Some(ip2)));
    }

    /// 不同用户名互不影响。
    #[test]
    fn test_login_rate_limiter_per_user_isolation() {
        let limiter = LoginRateLimiter::new();
        let ip_a = "127.0.0.1:1234".parse().unwrap();
        let ip_b = "127.0.0.1:1235".parse().unwrap();
        for _ in 0..5 {
            limiter.allow_attempt("user-a", Some(ip_a));
        }
        assert!(!limiter.allow_attempt("user-a", Some(ip_a)));
        // 不同用户 + 不同 IP 不受影响
        assert!(limiter.allow_attempt("user-b", Some(ip_b)));
    }

    /// Build a test AuthService with CCT signing enabled.
    fn build_service_with_cct() -> AuthService {
        let auth_manager = Arc::new(AuthManager::new());
        let token_manager = Arc::new(TokenManager::with_defaults());

        // Create test root key material (32 bytes)
        let root_key = vec![0xABu8; 32];
        let signing_keyring =
            Arc::new(TokenSigningKeyring::new(root_key).expect("keyring creation should succeed"));

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
            svc.auth_manager
                .user_add("testuser", "password123")
                .unwrap();

            let req = tonic::Request::new(AuthenticateRequest {
                name: "testuser".to_string(),
                password: "password123".to_string(),
            });

            let resp = svc
                .authenticate(req)
                .await
                .expect("authenticate should succeed");
            let inner = resp.into_inner();

            // Legacy token should always be present
            assert!(!inner.token.is_empty(), "legacy token should be present");
            assert!(
                inner.token.starts_with("coord_"),
                "legacy token should have coord_ prefix"
            );

            // CCT should be empty when no signing keyring
            assert!(
                inner.cct.is_empty(),
                "CCT should be empty without signing keyring"
            );
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

            let resp = svc
                .authenticate(req)
                .await
                .expect("authenticate should succeed");
            let inner = resp.into_inner();

            // CCT should be present
            assert!(
                !inner.cct.is_empty(),
                "CCT should be issued when signing keyring is configured"
            );
            assert!(
                inner.cct.starts_with("eyJ"),
                "CCT should start with base64url JSON header"
            );

            // expires_at should be in the future
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            assert!(inner.expires_at > now, "expires_at should be in the future");

            // Roles should be present
            assert!(
                inner.roles.contains(&"reader".to_string()),
                "roles should contain 'reader'"
            );
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
        assert!(!svc
            .auth_manager
            .role_list()
            .iter()
            .any(|r| r.name == "admin-role" && r.high_sensitive));

        svc.auth_manager
            .role_set_high_sensitive("admin-role", true)
            .unwrap();

        let admin_role = svc
            .auth_manager
            .role_list()
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
            assert!(decoded
                .payload
                .roles
                .contains(&"agent-bootstrap".to_string()));
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
