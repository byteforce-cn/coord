// Auth gRPC Service — implements auth.proto Auth service
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

use coord_core::auth::cct::{encode_cct_ed25519, is_expired, CctHeader, CctPayload};

use crate::auth::manager::{hash_password_argon2id, AuthManager};
use crate::auth::revocation::RevocationStore;
use crate::auth::token::TokenManager;
use crate::auth::token_signing::TokenSigningKeyring;
use crate::raft::type_config::AuthOp;

// ──── AuthOp 提案器（管理操作入 raft 日志）────

/// 将 AuthOp 经 raft 提案（`Command::Auth`）执行；由持有 Raft 句柄的层实现
/// （`coord-server/src/server/mod.rs` 对 `CoordNode` 实现）。
#[async_trait::async_trait]
pub trait AuthOpProposer: Send + Sync {
    /// 提案并等待应用，返回分配到的 revision。
    ///
    /// 错误为 gRPC Status：follower 上收到提案返回 `UNAVAILABLE`（携带
    /// leader hint，同 KV 写路径 R-SVC-08），客户端据此重定向到当前 leader；
    /// 超时返回 `DEADLINE_EXCEEDED`，其余为 `INTERNAL`。
    async fn propose_auth_op(&self, op: AuthOp) -> Result<u64, tonic::Status>;
}

// ──── 登录限流（per-user + per-IP 内存 token bucket）────

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

    /// 查看是否有可用令牌（先 refill；不消费）。
    fn has_token(&mut self) -> bool {
        self.refill();
        self.tokens >= 1.0
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

    /// 预检查本次登录尝试是否被允许（仅查看，不消耗令牌）。
    /// 用户名 + IP 任一桶耗尽 → false（拒绝发生在 argon2 之前，防爆破）。
    ///
    /// 修复（2026-08-30）：此前 allow_attempt 在密码校验**之前**对每次
    /// 尝试都消耗令牌，且成功登录只清用户桶、IP 桶永不返还——follower
    /// 转发（UNAVAILABLE）、无 quorum 超时、正常登录突发都会耗尽 IP 桶，
    /// 导致合法客户端（如 Jepsen control 节点）被长期锁定
    /// （RESOURCE_EXHAUSTED: too many failed login attempts）。
    /// 现改为：只有密码校验失败（record_failure）才消耗令牌。
    fn allow_attempt(&self, username: &str, ip: Option<std::net::SocketAddr>) -> bool {
        let mut buckets = self.buckets.write();
        let user_ok = buckets
            .entry(format!("u:{username}"))
            .or_insert_with(TokenBucket::new)
            .has_token();
        let ip_ok = match ip {
            Some(addr) => buckets
                .entry(format!("ip:{addr}"))
                .or_insert_with(TokenBucket::new)
                .has_token(),
            None => true,
        };
        user_ok && ip_ok
    }

    /// 登录失败（密码校验未通过）：消耗用户名 + IP 各一个令牌。
    fn record_failure(&self, username: &str, ip: Option<std::net::SocketAddr>) {
        let mut buckets = self.buckets.write();
        buckets
            .entry(format!("u:{username}"))
            .or_insert_with(TokenBucket::new)
            .try_take();
        if let Some(addr) = ip {
            buckets
                .entry(format!("ip:{addr}"))
                .or_insert_with(TokenBucket::new)
                .try_take();
        }
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

/// 动态 bootstrap 令牌默认有效期（秒）。
const BOOTSTRAP_TOKEN_DEFAULT_TTL_SECS: i64 = 3_600;
/// 动态 bootstrap 令牌有效期上限（30 天）。
const BOOTSTRAP_TOKEN_MAX_TTL_SECS: i64 = 30 * 24 * 3_600;
/// bootstrap 令牌明文前缀（便于识别；非密文，仅用于人眼区分）。
const BOOTSTRAP_TOKEN_PREFIX: &str = "cbt_";

/// 当前 Unix 秒。
fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// bootstrap 令牌的 SHA256 hex（**明文不落盘/不入日志**）。
fn bootstrap_token_hash(token: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    hex::encode(hasher.finalize())
}

/// 生成明文 bootstrap 令牌（256 bit CSPRNG；仅在签发响应中返回一次）。
fn generate_bootstrap_token() -> String {
    format!(
        "{BOOTSTRAP_TOKEN_PREFIX}{}{}",
        Uuid::new_v4().simple(),
        Uuid::new_v4().simple()
    )
}

/// gRPC Auth service implementation
#[derive(Clone)]
pub struct AuthService {
    auth_manager: Arc<AuthManager>,
    token_manager: Arc<TokenManager>,
    /// Token signing keyring for CCT issuance
    signing_keyring: Option<Arc<TokenSigningKeyring>>,
    /// Bootstrap token whitelist
    bootstrap_tokens: Arc<RwLock<HashSet<String>>>,
    /// 登录失败限流
    login_limiter: Arc<LoginRateLimiter>,
    /// AuthOp 提案器（Some = raft 模式，管理操作入日志）
    auth_proposer: Option<Arc<dyn AuthOpProposer>>,
    /// 吊销登记存储（delta 同步 + 无 proposer 时的直接吊销）
    revocation_store: Option<Arc<RevocationStore>>,
    /// 审计日志（认证/refresh 成功与失败事件；可选）
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

    /// 挂载审计日志器。
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

    /// 挂载 AuthOp 提案器：设置后管理操作经 raft 提交。
    pub fn with_proposer(mut self, proposer: Arc<dyn AuthOpProposer>) -> Self {
        self.auth_proposer = Some(proposer);
        self
    }

    /// 挂载吊销登记存储。
    pub fn with_revocation_store(mut self, store: Arc<RevocationStore>) -> Self {
        self.revocation_store = Some(store);
        self
    }

    /// 提案 AuthOp（有 proposer）或直接改内存视图（无 proposer，兼容测试/单机）。
    async fn apply_auth_op(&self, op: AuthOp) -> Result<(), tonic::Status> {
        match &self.auth_proposer {
            Some(proposer) => proposer.propose_auth_op(op.clone()).await.map(|_| ()),
            None => {
                self.auth_manager.apply_auth_op_to_view(&op);
                Ok(())
            }
        }
    }

    /// 管理操作调用者上下文解析（defense-in-depth + 审计身份）。
    ///
    /// 解析并校验 authorization CCT（签名/过期/吊销）：
    /// - auth 未启用 / 未配置 CCT 签发（测试/单机路径）→ 返回 None（视为放行，actor="local"）；
    /// - 解析/校验失败 → Err(permission_denied)。
    fn admin_context(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<Option<coord_core::auth::cct::CctToken>, tonic::Status> {
        if !self.auth_manager.is_enabled() {
            return Ok(None);
        }
        let Some(keyring) = &self.signing_keyring else {
            return Ok(None);
        };
        let cct_str = metadata
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| {
                tonic::Status::permission_denied("management operation requires an admin CCT")
            })?;
        // 剥 "Bearer " 前缀（与 interceptor 语义一致）
        let cct_str =
            crate::auth::interceptor::extract_bearer_token(Some(cct_str)).unwrap_or(cct_str);

        // 双算法验证（HMAC 历史密钥 + Ed25519 公钥）
        let cct = keyring
            .decode_any(cct_str)
            .map_err(|e| tonic::Status::permission_denied(format!("invalid CCT: {e}")))?;
        if is_expired(&cct.payload, 300) {
            return Err(tonic::Status::permission_denied("CCT expired"));
        }
        if let Some(rev) = &self.revocation_store {
            if rev.is_revoked(&cct.payload.jti) {
                return Err(tonic::Status::permission_denied("CCT has been revoked"));
            }
        }
        Ok(Some(cct))
    }

    /// 管理操作二次校验（defense-in-depth）。
    ///
    /// 即使绕过 interceptor 直连服务实现，管理操作也须校验调用者 CCT：
    /// 调用者须为 `root` 角色或持有指定 admin 能力（无匹配 → 拒绝）。
    /// 拒绝时记录审计事件。
    fn require_admin(
        &self,
        metadata: &tonic::metadata::MetadataMap,
        capability: &str,
    ) -> Result<(), tonic::Status> {
        let cct = match self.admin_context(metadata) {
            Ok(Some(cct)) => cct,
            Ok(None) => return Ok(()),
            Err(e) => {
                self.record_audit(
                    metadata,
                    capability,
                    "auth.mgmt",
                    crate::audit::RESULT_DENIED,
                    e.message(),
                );
                return Err(e);
            }
        };

        let has_root = cct
            .payload
            .roles
            .iter()
            .any(|r| r == crate::auth::manager::ROOT_ROLE);
        if has_root
            || self
                .auth_manager
                .check_capability(&cct.payload.roles, capability, None)
        {
            Ok(())
        } else {
            let detail = format!("management operation requires admin capability '{capability}'");
            self.record_audit(
                metadata,
                capability,
                "auth.mgmt",
                crate::audit::RESULT_DENIED,
                &detail,
            );
            Err(tonic::Status::permission_denied(detail))
        }
    }

    /// 管理操作审计——actor 从 CCT 尽力解析（失败回落 "anonymous"）。
    ///
    /// resource 承载操作对象（用户名/角色名），detail 承载补充信息。
    fn record_audit(
        &self,
        metadata: &tonic::metadata::MetadataMap,
        action: &str,
        resource: &str,
        result: &str,
        detail: &str,
    ) {
        let Some(audit) = &self.audit else {
            return;
        };
        let actor = self
            .admin_context(metadata)
            .ok()
            .flatten()
            .map(|cct| cct.payload.sub)
            .unwrap_or_else(|| "anonymous".to_string());
        audit.record_event(&actor, action, resource, result, detail);
    }

    /// 吊销 token：CCT（`eyJ` 前缀）按 jti 经 raft 登记吊销；
    /// 遗留 token 走 token_manager。
    pub async fn revoke_token(&self, token: &str) -> Result<(), String> {
        if token.starts_with("eyJ") {
            let keyring = self
                .signing_keyring
                .as_ref()
                .ok_or_else(|| "CCT signing not configured on server".to_string())?;
            let cct = keyring
                .decode_any(token)
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

    /// 将签发的会话经 raft 持久化（proposer None 时仅本地视图）。
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
                .map(|_| ()),
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
        // 签发改用 Ed25519（server 持私钥；agent 仅持公钥验证）
        let signing_key = match keyring.ed25519_signing_key() {
            Ok(sk) => sk,
            Err(e) => {
                tracing::error!("Ed25519 CCT key derivation failed: {e}");
                return (String::new(), 0);
            }
        };
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let exp = now + 3600; // 1 hour TTL

        let header = CctHeader::ed25519();

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

        match encode_cct_ed25519(&header, &payload, &signing_key) {
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
        request: tonic::Request<AuthEnableRequest>,
    ) -> Result<tonic::Response<AuthEnableResponse>, tonic::Status> {
        // 管理操作二次校验（auth 未启用时放行，保证首次开启可执行）
        self.require_admin(request.metadata(), "admin:auth:enable")?;
        let caller_md = request.metadata().clone();
        self.auth_manager.enable();
        self.record_audit(
            &caller_md,
            "auth.mgmt.enable",
            "auth",
            crate::audit::RESULT_SUCCESS,
            "",
        );
        tracing::info!("Auth enabled");
        Ok(tonic::Response::new(AuthEnableResponse {}))
    }

    async fn auth_disable(
        &self,
        request: tonic::Request<AuthDisableRequest>,
    ) -> Result<tonic::Response<AuthDisableResponse>, tonic::Status> {
        // 管理操作二次校验
        self.require_admin(request.metadata(), "admin:auth:disable")?;
        let caller_md = request.metadata().clone();
        self.auth_manager.disable();
        self.record_audit(
            &caller_md,
            "auth.mgmt.disable",
            "auth",
            crate::audit::RESULT_SUCCESS,
            "",
        );
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
        // 管理操作二次校验
        self.require_admin(request.metadata(), "admin:auth:user_add")?;
        let caller_md = request.metadata().clone();
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
        self.record_audit(
            &caller_md,
            "auth.mgmt.user_add",
            &req.name,
            crate::audit::RESULT_SUCCESS,
            "",
        );
        tracing::info!("User added: {}", req.name);
        Ok(tonic::Response::new(UserAddResponse {}))
    }

    async fn user_delete(
        &self,
        request: tonic::Request<UserDeleteRequest>,
    ) -> Result<tonic::Response<UserDeleteResponse>, tonic::Status> {
        // 管理操作二次校验
        self.require_admin(request.metadata(), "admin:auth:user_delete")?;
        let caller_md = request.metadata().clone();
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
        self.record_audit(
            &caller_md,
            "auth.mgmt.user_delete",
            &req.name,
            crate::audit::RESULT_SUCCESS,
            "",
        );
        tracing::info!("User deleted: {}", req.name);
        Ok(tonic::Response::new(UserDeleteResponse {}))
    }

    async fn user_change_password(
        &self,
        request: tonic::Request<UserChangePasswordRequest>,
    ) -> Result<tonic::Response<UserChangePasswordResponse>, tonic::Status> {
        // 管理操作二次校验
        self.require_admin(request.metadata(), "admin:auth:user_add")?;
        let caller_md = request.metadata().clone();
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
        self.record_audit(
            &caller_md,
            "auth.mgmt.user_change_password",
            &req.name,
            crate::audit::RESULT_SUCCESS,
            "",
        );
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
        // 管理操作二次校验
        self.require_admin(request.metadata(), "admin:auth:role_add")?;
        let caller_md = request.metadata().clone();
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
        self.record_audit(
            &caller_md,
            "auth.mgmt.role_add",
            &req.name,
            crate::audit::RESULT_SUCCESS,
            "",
        );
        tracing::info!("Role added: {}", req.name);
        Ok(tonic::Response::new(RoleAddResponse {}))
    }

    async fn role_delete(
        &self,
        request: tonic::Request<RoleDeleteRequest>,
    ) -> Result<tonic::Response<RoleDeleteResponse>, tonic::Status> {
        // 管理操作二次校验
        self.require_admin(request.metadata(), "admin:auth:role_delete")?;
        let caller_md = request.metadata().clone();
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
        self.record_audit(
            &caller_md,
            "auth.mgmt.role_delete",
            &req.name,
            crate::audit::RESULT_SUCCESS,
            "",
        );
        tracing::info!("Role deleted: {}", req.name);
        Ok(tonic::Response::new(RoleDeleteResponse {}))
    }

    async fn role_grant_permission(
        &self,
        request: tonic::Request<RoleGrantPermissionRequest>,
    ) -> Result<tonic::Response<RoleGrantPermissionResponse>, tonic::Status> {
        // 管理操作二次校验
        self.require_admin(request.metadata(), "admin:auth:role_grant")?;
        let caller_md = request.metadata().clone();
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
        self.record_audit(
            &caller_md,
            "auth.mgmt.role_grant_permission",
            &req.name,
            crate::audit::RESULT_SUCCESS,
            "",
        );
        Ok(tonic::Response::new(RoleGrantPermissionResponse {}))
    }

    async fn role_revoke_permission(
        &self,
        request: tonic::Request<RoleRevokePermissionRequest>,
    ) -> Result<tonic::Response<RoleRevokePermissionResponse>, tonic::Status> {
        // 管理操作二次校验
        self.require_admin(request.metadata(), "admin:auth:role_revoke")?;
        let caller_md = request.metadata().clone();
        let req = request.into_inner();
        self.apply_auth_op(AuthOp::RoleRevokePermission {
            role: req.name.clone(),
            key: req.key,
            range_end: req.range_end,
        })
        .await?;
        self.record_audit(
            &caller_md,
            "auth.mgmt.role_revoke_permission",
            &req.name,
            crate::audit::RESULT_SUCCESS,
            "",
        );
        Ok(tonic::Response::new(RoleRevokePermissionResponse {}))
    }

    async fn role_grant_capability(
        &self,
        request: tonic::Request<RoleGrantCapabilityRequest>,
    ) -> Result<tonic::Response<RoleGrantCapabilityResponse>, tonic::Status> {
        // 管理操作二次校验（与 RoleGrantPermission 同能力门槛）
        self.require_admin(request.metadata(), "admin:auth:role_grant")?;
        let caller_md = request.metadata().clone();
        let req = request.into_inner();
        if req.capability_id.trim().is_empty() {
            return Err(tonic::Status::invalid_argument(
                "capability_id must not be empty",
            ));
        }

        self.apply_auth_op(AuthOp::RoleGrantCapability {
            role: req.role.clone(),
            capability_id: req.capability_id.clone(),
            scope: req.scope.clone(),
        })
        .await?;
        self.record_audit(
            &caller_md,
            "auth.mgmt.role_grant_capability",
            &req.role,
            crate::audit::RESULT_SUCCESS,
            &req.capability_id,
        );
        tracing::info!(
            "Capability '{}' granted to role '{}' (scope='{}')",
            req.capability_id,
            req.role,
            req.scope
        );
        Ok(tonic::Response::new(RoleGrantCapabilityResponse {}))
    }

    async fn role_revoke_capability(
        &self,
        request: tonic::Request<RoleRevokeCapabilityRequest>,
    ) -> Result<tonic::Response<RoleRevokeCapabilityResponse>, tonic::Status> {
        self.require_admin(request.metadata(), "admin:auth:role_revoke")?;
        let caller_md = request.metadata().clone();
        let req = request.into_inner();
        if req.capability_id.trim().is_empty() {
            return Err(tonic::Status::invalid_argument(
                "capability_id must not be empty",
            ));
        }

        self.apply_auth_op(AuthOp::RoleRevokeCapability {
            role: req.role.clone(),
            capability_id: req.capability_id.clone(),
            scope: req.scope.clone(),
        })
        .await?;
        self.record_audit(
            &caller_md,
            "auth.mgmt.role_revoke_capability",
            &req.role,
            crate::audit::RESULT_SUCCESS,
            &req.capability_id,
        );
        tracing::info!(
            "Capability '{}' revoked from role '{}' (scope='{}')",
            req.capability_id,
            req.role,
            req.scope
        );
        Ok(tonic::Response::new(RoleRevokeCapabilityResponse {}))
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
        // 管理操作二次校验
        self.require_admin(request.metadata(), "admin:auth:user_grant_role")?;
        let caller_md = request.metadata().clone();
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
        self.record_audit(
            &caller_md,
            "auth.mgmt.user_grant_role",
            &format!("{}:{}", req.user, req.role),
            crate::audit::RESULT_SUCCESS,
            "",
        );
        tracing::info!("Role '{}' granted to user '{}'", req.role, req.user);
        Ok(tonic::Response::new(UserGrantRoleResponse {}))
    }

    async fn user_revoke_role(
        &self,
        request: tonic::Request<UserRevokeRoleRequest>,
    ) -> Result<tonic::Response<UserRevokeRoleResponse>, tonic::Status> {
        // 管理操作二次校验
        self.require_admin(request.metadata(), "admin:auth:user_revoke_role")?;
        let caller_md = request.metadata().clone();
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
        self.record_audit(
            &caller_md,
            "auth.mgmt.user_revoke_role",
            &format!("{}:{}", req.user, req.role),
            crate::audit::RESULT_SUCCESS,
            "",
        );
        tracing::info!("Role '{}' revoked from user '{}'", req.role, req.user);
        Ok(tonic::Response::new(UserRevokeRoleResponse {}))
    }

    // ──── Authentication ────

    async fn authenticate(
        &self,
        request: tonic::Request<AuthenticateRequest>,
    ) -> Result<tonic::Response<AuthenticateResponse>, tonic::Status> {
        // 登录限流判定前置到 argon2 之前——防爆破时消耗 CPU 进行哈希。
        // （失败消费、成功清除用户计数；IP 桶随尝试消费，防单 IP 分布式爆破。）
        let peer_ip = request.remote_addr();
        let req = request.into_inner();

        if !self.login_limiter.allow_attempt(&req.name, peer_ip) {
            if let Some(ref audit) = self.audit {
                audit.record_event(
                    &req.name,
                    "auth.authenticate",
                    &req.name,
                    crate::audit::RESULT_DENIED,
                    "rate limited before password verification",
                );
            }
            return Err(tonic::Status::resource_exhausted(
                "too many failed login attempts; retry later",
            ));
        }

        // Verify password
        if let Err(e) = self.auth_manager.authenticate(&req.name, &req.password) {
            // 只有密码校验失败才消耗限流令牌（成功/转发/超时不消耗）。
            self.login_limiter.record_failure(&req.name, peer_ip);
            if let Some(ref audit) = self.audit {
                audit.record_event(
                    &req.name,
                    "auth.authenticate",
                    &req.name,
                    crate::audit::RESULT_DENIED,
                    &e.to_string(),
                );
            }
            return Err(tonic::Status::unauthenticated(e.to_string()));
        }
        self.login_limiter.clear_user(&req.name);

        // Issue simple token (legacy) + refresh token（单次使用、落盘）
        let auth_token = self.token_manager.issue_token(&req.name);
        let refresh_token = self.token_manager.issue_refresh_token(&req.name);

        // 会话经 raft 持久化（重启不失效；proposer None 时本地视图）
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

    /// refresh token 换新（单次使用，旧 refresh 消费后作废）。
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
                .map(|_| ())?,
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
        // 从吊销登记存储取增量（agent 缓存按 delta 同步）
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

    // ──── CCT v3: Agent Bootstrap ────

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
        //   1) 动态注册表（TTL + 一次性 + raft 持久化；跨节点一致）
        //   2) 静态配置白名单（`[security].agent_bootstrap_tokens`，向后兼容）
        let now_unix = unix_now_secs();
        let hash_hex = bootstrap_token_hash(&req.bootstrap_token);
        match self
            .auth_manager
            .consume_bootstrap_token_atomic(&hash_hex, now_unix)
        {
            Some(rec) => {
                // 一次性语义**先持久化再签发**：提案失败则回滚本地消费，
                // 保证「拿到 CCT 的令牌一定已被记录为已消费」（fail-closed）。
                if let Err(e) = self
                    .apply_auth_op(AuthOp::ConsumeBootstrapToken {
                        id: rec.id.clone(),
                        consumed_at_unix: now_unix,
                    })
                    .await
                {
                    self.auth_manager
                        .revert_bootstrap_token_consumption(&rec.id, now_unix);
                    tracing::warn!("bootstrap token consume proposal failed: {e}");
                    return Err(e);
                }
                tracing::info!(
                    "bootstrap token '{}' (label='{}') consumed",
                    rec.id,
                    rec.label
                );
            }
            None => {
                if !self.consume_bootstrap_token(&req.bootstrap_token) {
                    return Err(tonic::Status::permission_denied(
                        "invalid or already consumed bootstrap token",
                    ));
                }
            }
        }

        // Issue a short-lived CCT for the agent
        let (cct, expires_at) = if let Some(ref keyring) = self.signing_keyring {
            // bootstrap CCT 同样 Ed25519 签发（agent 仅存公钥验证）
            let signing_key = keyring.ed25519_signing_key().map_err(|e| {
                tonic::Status::internal(format!("Ed25519 CCT key derivation failed: {e}"))
            })?;
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            let exp = now + 600; // 10 minutes TTL for bootstrap CCT

            let header = CctHeader::ed25519();

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

            match encode_cct_ed25519(&header, &payload, &signing_key) {
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

    // ──── Bootstrap 令牌动态签发（TTL + 一次性；raft 持久化） ────

    async fn bootstrap_token_issue(
        &self,
        request: tonic::Request<BootstrapTokenIssueRequest>,
    ) -> Result<tonic::Response<BootstrapTokenIssueResponse>, tonic::Status> {
        self.require_admin(request.metadata(), "admin:auth:bootstrap_token")?;
        let caller_md = request.metadata().clone();
        let req = request.into_inner();

        let ttl = if req.ttl_secs <= 0 {
            BOOTSTRAP_TOKEN_DEFAULT_TTL_SECS
        } else {
            req.ttl_secs
        };
        if ttl > BOOTSTRAP_TOKEN_MAX_TTL_SECS {
            return Err(tonic::Status::invalid_argument(format!(
                "ttl_secs too large (max {BOOTSTRAP_TOKEN_MAX_TTL_SECS})"
            )));
        }
        let label = req.label.trim().to_string();
        if label.len() > 128 {
            return Err(tonic::Status::invalid_argument(
                "label too long (max 128)".to_string(),
            ));
        }

        let created_by = self
            .admin_context(&caller_md)
            .ok()
            .flatten()
            .map(|cct| cct.payload.sub)
            .unwrap_or_else(|| "local".to_string());

        let now = unix_now_secs();
        let id = Uuid::new_v4().to_string();
        let token = generate_bootstrap_token();
        let expires_at = now + ttl as u64;

        self.apply_auth_op(AuthOp::IssueBootstrapToken {
            id: id.clone(),
            hash_hex: bootstrap_token_hash(&token),
            label: label.clone(),
            created_by: created_by.clone(),
            created_at_unix: now,
            expires_at_unix: expires_at,
        })
        .await?;

        self.record_audit(
            &caller_md,
            "auth.mgmt.bootstrap_token_issue",
            &id,
            crate::audit::RESULT_SUCCESS,
            &format!("label={label} ttl={ttl}s"),
        );
        // 明文令牌**只在此响应中出现一次**（服务端仅存 SHA256）。
        tracing::info!(
            "bootstrap token '{id}' issued by '{created_by}' (label='{label}', ttl={ttl}s)"
        );
        Ok(tonic::Response::new(BootstrapTokenIssueResponse {
            id,
            token,
            expires_at: expires_at as i64,
        }))
    }

    async fn bootstrap_token_list(
        &self,
        request: tonic::Request<BootstrapTokenListRequest>,
    ) -> Result<tonic::Response<BootstrapTokenListResponse>, tonic::Status> {
        self.require_admin(request.metadata(), "admin:auth:bootstrap_token")?;
        let tokens = self
            .auth_manager
            .bootstrap_tokens()
            .into_iter()
            .map(|r| {
                let consumed = r.is_consumed();
                BootstrapTokenInfo {
                    id: r.id,
                    label: r.label,
                    created_by: r.created_by,
                    created_at: r.created_at_unix as i64,
                    expires_at: r.expires_at_unix as i64,
                    consumed,
                }
            })
            .collect();
        Ok(tonic::Response::new(BootstrapTokenListResponse { tokens }))
    }

    async fn bootstrap_token_revoke(
        &self,
        request: tonic::Request<BootstrapTokenRevokeRequest>,
    ) -> Result<tonic::Response<BootstrapTokenRevokeResponse>, tonic::Status> {
        self.require_admin(request.metadata(), "admin:auth:bootstrap_token")?;
        let caller_md = request.metadata().clone();
        let req = request.into_inner();
        if req.id.trim().is_empty() {
            return Err(tonic::Status::invalid_argument("id must not be empty"));
        }

        // 幂等：不存在不算错误，`revoked=false` 告知调用方。
        let existed = self.auth_manager.bootstrap_token(&req.id).is_some();
        self.apply_auth_op(AuthOp::RevokeBootstrapToken { id: req.id.clone() })
            .await?;
        self.record_audit(
            &caller_md,
            "auth.mgmt.bootstrap_token_revoke",
            &req.id,
            crate::audit::RESULT_SUCCESS,
            if existed { "revoked" } else { "not_found" },
        );
        Ok(tonic::Response::new(BootstrapTokenRevokeResponse {
            revoked: existed,
        }))
    }
}

// ──── Tests (CCT issuance) ────

#[cfg(test)]
mod cct_tests {
    use super::*;

    /// 登录限流：连续失败耗尽令牌 → RESOURCE_EXHAUSTED；成功登录清空计数。
    #[test]
    fn test_login_rate_limiter_exhausts_and_resets() {
        let limiter = LoginRateLimiter::new();
        let ip = "127.0.0.1:1234".parse().unwrap();
        // 容量 5：前 4 次失败后仍允许，第 5 次失败耗尽令牌，之后被拒绝
        for i in 0..4 {
            limiter.record_failure("attacker", Some(ip));
            assert!(
                limiter.allow_attempt("attacker", Some(ip)),
                "attempt {i} should still be allowed"
            );
        }
        limiter.record_failure("attacker", Some(ip));
        assert!(
            !limiter.allow_attempt("attacker", Some(ip)),
            "bucket exhausted"
        );
        // 成功登录后清除该用户计数（IP 桶仍被限流）；换 IP 后可再尝试
        limiter.clear_user("attacker");
        let ip2 = "127.0.0.1:9999".parse().unwrap();
        assert!(limiter.allow_attempt("attacker", Some(ip2)));
        assert!(!limiter.allow_attempt("attacker", Some(ip)));
    }

    /// 成功登录不消耗任何令牌：合法客户端登录突发/重连不应被锁定。
    #[test]
    fn test_login_rate_limiter_success_does_not_consume() {
        let limiter = LoginRateLimiter::new();
        let ip = "127.0.0.1:1234".parse().unwrap();
        for _ in 0..20 {
            assert!(limiter.allow_attempt("root", Some(ip)));
            limiter.clear_user("root"); // 模拟成功登录
        }
        // 从未发生失败：桶仍是满的
        assert!(limiter.allow_attempt("root", Some(ip)));
    }

    /// 不同用户名互不影响。
    #[test]
    fn test_login_rate_limiter_per_user_isolation() {
        let limiter = LoginRateLimiter::new();
        let ip_a = "127.0.0.1:1234".parse().unwrap();
        let ip_b = "127.0.0.1:1235".parse().unwrap();
        for _ in 0..5 {
            limiter.record_failure("user-a", Some(ip_a));
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
        let _active_key = signing_keyring.active_key();

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

            // Decode and verify the CCT（Ed25519 签发，双算法验证）
            let decoded = signing_keyring
                .decode_any(&cct)
                .expect("CCT should be decodable and verifiable");

            assert_eq!(decoded.header.alg, "Ed25519");
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

    // ──── Role→Capability storage ────

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

    // ──── Bootstrap RPC ────

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
        let _active_key = signing_keyring.active_key();
        svc.add_bootstrap_token("verify-bootstrap");

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let req = tonic::Request::new(BootstrapRequest {
                bootstrap_token: "verify-bootstrap".to_string(),
            });
            let resp = svc.bootstrap(req).await.unwrap();
            let inner = resp.into_inner();

            // Verify the bootstrap CCT（Ed25519 签发）
            let decoded = signing_keyring
                .decode_any(&inner.cct)
                .expect("bootstrap CCT should be verifiable");
            assert_eq!(decoded.header.alg, "Ed25519");
            assert_eq!(decoded.payload.sub, "coord-agent");
            assert!(decoded
                .payload
                .roles
                .contains(&"agent-bootstrap".to_string()));
            // Bootstrap CCT should be short-lived (10 min)
            assert!(decoded.payload.exp - decoded.payload.iat <= 600);
        });
    }

    // ──── 动态 Bootstrap 令牌（TTL + 一次性） ────

    /// 签发 → 列表可见 → 仅能消费一次 → 列表标记已消费。
    #[test]
    fn test_dynamic_bootstrap_token_issue_and_single_use() {
        let svc = build_service_with_cct();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let issued = svc
                .bootstrap_token_issue(tonic::Request::new(BootstrapTokenIssueRequest {
                    label: "cluster-a".into(),
                    ttl_secs: 3600,
                }))
                .await
                .expect("issue should succeed")
                .into_inner();
            assert!(issued.token.starts_with("cbt_"), "token must be prefixed");
            assert!(!issued.id.is_empty());
            assert!(issued.expires_at > 0);

            // 列表：未消费
            let list = svc
                .bootstrap_token_list(tonic::Request::new(BootstrapTokenListRequest {}))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(list.tokens.len(), 1);
            assert_eq!(list.tokens[0].label, "cluster-a");
            assert!(!list.tokens[0].consumed);

            // 首次引导成功
            let resp = svc
                .bootstrap(tonic::Request::new(BootstrapRequest {
                    bootstrap_token: issued.token.clone(),
                }))
                .await
                .expect("dynamic token should be accepted");
            assert!(!resp.into_inner().cct.is_empty());

            // 一次性：第二次被拒
            let second = svc
                .bootstrap(tonic::Request::new(BootstrapRequest {
                    bootstrap_token: issued.token.clone(),
                }))
                .await;
            assert!(second.is_err(), "dynamic token must be single-use");

            // 列表：已消费（记录保留供审计）
            let list = svc
                .bootstrap_token_list(tonic::Request::new(BootstrapTokenListRequest {}))
                .await
                .unwrap()
                .into_inner();
            assert!(list.tokens[0].consumed);
        });
    }

    /// TTL 上限校验 + 默认 TTL 生效。
    #[test]
    fn test_dynamic_bootstrap_token_ttl_bounds() {
        let svc = build_service_with_cct();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let too_long = svc
                .bootstrap_token_issue(tonic::Request::new(BootstrapTokenIssueRequest {
                    label: String::new(),
                    ttl_secs: BOOTSTRAP_TOKEN_MAX_TTL_SECS + 1,
                }))
                .await;
            assert_eq!(
                too_long.unwrap_err().code(),
                tonic::Code::InvalidArgument,
                "ttl above cap must be rejected"
            );

            let issued = svc
                .bootstrap_token_issue(tonic::Request::new(BootstrapTokenIssueRequest {
                    label: String::new(),
                    ttl_secs: 0,
                }))
                .await
                .unwrap()
                .into_inner();
            let now = unix_now_secs() as i64;
            let ttl = issued.expires_at - now;
            assert!(
                (BOOTSTRAP_TOKEN_DEFAULT_TTL_SECS - ttl).abs() <= 5,
                "ttl_secs=0 must fall back to default ({ttl})"
            );
        });
    }

    /// 撤销后不可用；撤销不存在的 ID 幂等（revoked=false）。
    #[test]
    fn test_dynamic_bootstrap_token_revoke() {
        let svc = build_service_with_cct();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let issued = svc
                .bootstrap_token_issue(tonic::Request::new(BootstrapTokenIssueRequest {
                    label: String::new(),
                    ttl_secs: 600,
                }))
                .await
                .unwrap()
                .into_inner();

            let revoked = svc
                .bootstrap_token_revoke(tonic::Request::new(BootstrapTokenRevokeRequest {
                    id: issued.id.clone(),
                }))
                .await
                .unwrap()
                .into_inner();
            assert!(revoked.revoked);

            let denied = svc
                .bootstrap(tonic::Request::new(BootstrapRequest {
                    bootstrap_token: issued.token.clone(),
                }))
                .await;
            assert!(denied.is_err(), "revoked token must not be accepted");

            // 幂等
            let again = svc
                .bootstrap_token_revoke(tonic::Request::new(BootstrapTokenRevokeRequest {
                    id: issued.id,
                }))
                .await
                .unwrap()
                .into_inner();
            assert!(!again.revoked);

            // 空 ID → invalid_argument
            let empty = svc
                .bootstrap_token_revoke(tonic::Request::new(BootstrapTokenRevokeRequest {
                    id: "  ".into(),
                }))
                .await;
            assert_eq!(empty.unwrap_err().code(), tonic::Code::InvalidArgument);
        });
    }
}

// ──── 测试 ────

#[cfg(test)]
mod tests {
    use super::*;
    use coord_core::auth::cct::encode_cct;

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

    // ──── 管理操作二次校验 ────

    fn build_admin_service() -> (AuthService, Arc<TokenSigningKeyring>) {
        let auth_manager = Arc::new(crate::auth::manager::AuthManager::new());
        auth_manager.enable();
        let token_manager = Arc::new(TokenManager::with_defaults());
        let signing_keyring =
            Arc::new(TokenSigningKeyring::new(vec![0xCDu8; 32]).expect("keyring creation"));
        let svc =
            AuthService::with_cct_signing(auth_manager, token_manager, signing_keyring.clone());
        (svc, signing_keyring)
    }

    fn cct_with_roles(keyring: &TokenSigningKeyring, roles: &[&str]) -> String {
        let header = CctHeader::default();
        let payload = CctPayload {
            jti: uuid::Uuid::new_v4().to_string(),
            iss: "test".to_string(),
            sub: "test".to_string(),
            aud: vec![],
            iat: 1719990000,
            exp: 2000000000,
            roles: roles.iter().map(|s| s.to_string()).collect(),
            scope_overrides: HashMap::new(),
        };
        let key = keyring.active_key();
        encode_cct(&header, &payload, &key.key_bytes).unwrap()
    }

    #[test]
    fn test_require_admin_skips_when_auth_disabled() {
        let auth_manager = Arc::new(crate::auth::manager::AuthManager::new()); // 默认禁用
        let token_manager = Arc::new(TokenManager::with_defaults());
        let svc = AuthService::new(auth_manager, token_manager);
        assert!(svc
            .require_admin(&Default::default(), "admin:auth:user_add")
            .is_ok());
    }

    #[test]
    fn test_require_admin_rejects_missing_cct() {
        let (svc, _keyring) = build_admin_service();
        let md = tonic::metadata::MetadataMap::new();
        assert!(svc.require_admin(&md, "admin:auth:user_add").is_err());
    }

    #[test]
    fn test_require_admin_accepts_root_cct() {
        let (svc, keyring) = build_admin_service();
        let cct = cct_with_roles(&keyring, &["root"]);
        let mut md = tonic::metadata::MetadataMap::new();
        md.insert("authorization", cct.parse().unwrap());
        assert!(svc.require_admin(&md, "admin:auth:user_add").is_ok());
        assert!(svc.require_admin(&md, "admin:auth:disable").is_ok());
    }

    #[test]
    fn test_require_admin_rejects_non_admin_cct() {
        let (svc, keyring) = build_admin_service();
        let cct = cct_with_roles(&keyring, &["reader"]);
        let mut md = tonic::metadata::MetadataMap::new();
        md.insert("authorization", cct.parse().unwrap());
        assert!(svc.require_admin(&md, "admin:auth:user_add").is_err());
        assert!(svc.require_admin(&md, "admin:auth:disable").is_err());
    }

    // ──── 管理操作审计 ────

    struct MemAuditStore {
        events: parking_lot::Mutex<Vec<crate::audit::AuditEvent>>,
    }

    impl crate::audit::AuditStore for MemAuditStore {
        fn append(&self, event: &crate::audit::AuditEvent) -> coord_core::error::Result<()> {
            self.events.lock().push(event.clone());
            Ok(())
        }

        fn recent(&self, limit: usize) -> Vec<crate::audit::AuditEvent> {
            self.events
                .lock()
                .iter()
                .rev()
                .take(limit)
                .cloned()
                .collect()
        }
    }

    #[test]
    fn test_management_op_audits_success_and_denial() {
        let (mut svc, keyring) = build_admin_service();
        let store = Arc::new(MemAuditStore {
            events: parking_lot::Mutex::new(Vec::new()),
        });
        let logger = Arc::new(crate::audit::AuditLogger::new(store.clone()));
        svc = svc.with_audit_logger(logger);

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            // root CCT：成功路径 → success 事件（actor 从 CCT 解析）
            let root_cct = cct_with_roles(&keyring, &["root"]);
            let mut md = tonic::metadata::MetadataMap::new();
            md.insert(
                "authorization",
                format!("Bearer {root_cct}").parse().unwrap(),
            );
            let mut req = tonic::Request::new(AuthEnableRequest {});
            *req.metadata_mut() = md;
            assert!(svc.auth_enable(req).await.is_ok());

            // 非 root CCT：拒绝路径 → denied 事件
            let reader_cct = cct_with_roles(&keyring, &["reader"]);
            let mut md = tonic::metadata::MetadataMap::new();
            md.insert(
                "authorization",
                format!("Bearer {reader_cct}").parse().unwrap(),
            );
            let mut req = tonic::Request::new(AuthDisableRequest {});
            *req.metadata_mut() = md;
            assert!(svc.auth_disable(req).await.is_err());
        });

        let events = store.events.lock().clone();
        assert!(
            events.iter().any(|e| e.action == "auth.mgmt.enable"
                && e.result == crate::audit::RESULT_SUCCESS
                && e.actor == "test"),
            "成功管理操作应记录 success 审计事件且 actor 为 CCT sub: {events:?}"
        );
        assert!(
            events.iter().any(|e| e.action == "admin:auth:disable"
                && e.result == crate::audit::RESULT_DENIED
                && e.actor == "test"),
            "被拒管理操作应记录 denied 审计事件: {events:?}"
        );
    }
}
