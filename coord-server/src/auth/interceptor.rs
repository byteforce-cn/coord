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
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use coord_core::auth::cct::{decode_cct, is_expired, CctPayload, CctToken};
use coord_core::auth::trie::ScopeTrie;
use tonic::Status;
use tower::{Layer, Service};

use crate::auth::manager::AuthManager;
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
    set.insert("admin:maintenance:snapshot");
    set.insert("admin:maintenance:compact");
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
/// 服务端鉴权结果
// Box 化 CctToken 会改变全调用点匹配模式；Allow 载荷大但不频繁（每次请求
// 一次），接受大小差异（P1-03 clippy 治理评估）。
#[allow(clippy::large_enum_variant)]
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
///
/// P0-C.4：能力判定已收紧 —— 无 scope_overrides 时按服务端角色授权
/// （`AuthManager::check_capability`）判定，无匹配即拒绝（fail-closed），
/// 删除"有任意 role 即放行"的宽泛兜底。
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
    /// Server-side role → capability grants（P0-C.4；None = fail-closed）
    role_provider: Option<Arc<AuthManager>>,
    /// P2-08：审计日志（拒绝路径记录；可选）
    audit: Option<Arc<crate::audit::AuditLogger>>,
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
            role_provider: None,
            audit: None,
        }
    }

    /// P2-08：挂载审计日志器（拒绝路径记录鉴权拒绝事件）。
    pub fn with_audit_logger(mut self, logger: Arc<crate::audit::AuditLogger>) -> Self {
        self.audit = Some(logger);
        self
    }

    /// Attach a server-side role provider for capability authorization.
    ///
    /// Without a provider any token without scope_overrides is denied (fail-closed).
    pub fn with_role_provider(mut self, manager: Arc<AuthManager>) -> Self {
        self.role_provider = Some(manager);
        self
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

            // Case B: All other cases → full capability + scope check (P0-C.4 fail-closed)
            (_, true) | (false, _) => {
                if !self.authorize(&cct.payload, capability_id, scope_key) {
                    return ServerAuthResult::Deny {
                        reason: format!(
                            "capability '{capability_id}' not granted to roles {:?} (scope key: {scope_key:?})",
                            cct.payload.roles
                        ),
                        trusted_agent: is_trusted,
                    };
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

    /// Authorize a capability request for a CCT payload (P0-C.4).
    ///
    /// 1. scope_overrides（token 内嵌覆盖）优先：空 scope = 全放行；
    ///    非空 scope 须 `scope_key` 存在且 ScopeTrie 命中（否则拒绝）。
    /// 2. 无覆盖时按服务端角色授权（`AuthManager::check_capability`）；
    ///    无角色提供方 → 拒绝（fail-closed）。
    fn authorize(
        &self,
        payload: &CctPayload,
        capability_id: &str,
        scope_key: Option<&str>,
    ) -> bool {
        // Check scope_overrides first (per-token overrides)
        if let Some(allowed_scope) = payload.scope_overrides.get(capability_id) {
            if allowed_scope.is_empty() {
                return true; // Empty scope = match-all
            }
            // 非空 override 必须能对 scope_key 验证；无 scope_key → fail-closed
            return match scope_key {
                Some(key) => {
                    let mut trie = ScopeTrie::new();
                    trie.insert(allowed_scope).is_ok() && trie.matches(key)
                }
                None => false,
            };
        }

        // Server-side role grants (P0-C.4: no more "any role passes" fallback)
        match &self.role_provider {
            Some(provider) => provider.check_capability(&payload.roles, capability_id, scope_key),
            None => false,
        }
    }

    /// 仅验证 CCT 有效性（签名/过期/吊销），不做能力授权。
    ///
    /// 用于角色同步等"需合法凭据但不走角色授权"的端点（`GetRevocationDelta`）。
    pub fn verify_token_only(&self, cct_str: Option<&str>) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }
        let cct_str = cct_str.ok_or_else(|| "missing CCT token".to_string())?;
        let cct_str = extract_bearer_token(Some(cct_str)).unwrap_or(cct_str);
        let cct = self.decode_and_verify(cct_str)?;
        if is_expired(&cct.payload, self.clock_drift_secs) {
            return Err("CCT expired".into());
        }
        if self.revocation_store.is_revoked(&cct.payload.jti) {
            return Err("CCT has been revoked".into());
        }
        Ok(())
    }
}

// ──── RPC → Capability 映射（服务端，P0-C.1）────

/// 服务端 gRPC 方法 → 能力 ID 映射（与 agent 侧映射表一致）。
///
/// 返回 `None` 的路径属于白名单（health check / Authenticate）或未知 RPC；
/// 白名单判定见 [`is_whitelisted`]。
pub fn infer_capability(rpc_method: &str) -> Option<String> {
    match rpc_method {
        // KV
        "/coord.kv.KV/Range" => Some("data:kv:read".into()),
        "/coord.kv.KV/Put" => Some("data:kv:write".into()),
        "/coord.kv.KV/Delete" => Some("data:kv:delete".into()),

        // Txn
        "/coord.txn.Txn/Txn" => Some("data:txn:execute".into()),

        // Lease
        "/coord.lease.Lease/LeaseGrant" => Some("data:lease:grant".into()),
        "/coord.lease.Lease/LeaseRevoke" => Some("data:lease:revoke".into()),
        "/coord.lease.Lease/LeaseKeepAlive" => Some("data:lease:keepalive".into()),

        // Watch
        "/coord.watch.Watch/Watch" => Some("data:watch:subscribe".into()),

        // Maintenance（集群管理，P0-D.5 归 cluster:admin 权限点）
        "/coord.maintenance.Maintenance/Status" => Some("admin:maintenance:status".into()),
        "/coord.maintenance.Maintenance/Seal" => Some("admin:maintenance:seal".into()),
        "/coord.maintenance.Maintenance/Unseal" => Some("admin:maintenance:unseal".into()),
        "/coord.maintenance.Maintenance/Snapshot" => Some("admin:maintenance:snapshot".into()),
        "/coord.maintenance.Maintenance/Compact" => Some("admin:maintenance:compact".into()),
        "/coord.maintenance.Maintenance/MemberAdd" => Some("admin:maintenance:member_add".into()),
        "/coord.maintenance.Maintenance/MemberRemove" => {
            Some("admin:maintenance:member_remove".into())
        }
        "/coord.maintenance.Maintenance/MemberPromote" => {
            Some("admin:maintenance:member_promote".into())
        }
        "/coord.maintenance.Maintenance/MemberList" => Some("admin:maintenance:member_list".into()),

        // Auth 管理
        "/coord.auth.Auth/AuthEnable" => Some("admin:auth:enable".into()),
        "/coord.auth.Auth/AuthDisable" => Some("admin:auth:disable".into()),
        "/coord.auth.Auth/AuthStatus" => Some("admin:auth:status".into()),
        "/coord.auth.Auth/UserAdd" => Some("admin:auth:user_add".into()),
        "/coord.auth.Auth/UserDelete" => Some("admin:auth:user_delete".into()),
        "/coord.auth.Auth/UserList" => Some("admin:auth:user_list".into()),
        "/coord.auth.Auth/UserGet" => Some("admin:auth:user_list".into()),
        "/coord.auth.Auth/UserChangePassword" => Some("admin:auth:user_add".into()),
        "/coord.auth.Auth/RoleAdd" => Some("admin:auth:role_add".into()),
        "/coord.auth.Auth/RoleDelete" => Some("admin:auth:role_delete".into()),
        "/coord.auth.Auth/RoleGrantPermission" => Some("admin:auth:role_grant".into()),
        "/coord.auth.Auth/RoleRevokePermission" => Some("admin:auth:role_revoke".into()),
        "/coord.auth.Auth/RoleList" => Some("admin:auth:role_list".into()),
        "/coord.auth.Auth/ListRoles" => Some("admin:auth:role_list".into()),
        "/coord.auth.Auth/UserGrantRole" => Some("admin:auth:user_grant_role".into()),
        "/coord.auth.Auth/UserRevokeRole" => Some("admin:auth:user_revoke_role".into()),

        // Capability 查询
        "/coord.capability.CapabilityRegistry/List" => Some("admin:capability:list".into()),
        "/coord.capability.CapabilityRegistry/Get" => Some("admin:capability:list".into()),
        "/coord.capability.CapabilityRegistry/Register" => Some("admin:capability:register".into()),
        "/coord.capability.CapabilityRegistry/Deprecate" => {
            Some("admin:capability:deprecate".into())
        }

        // Authenticate 为登录端点，白名单放行（见 is_whitelisted）
        "/coord.auth.Auth/Authenticate" => None,
        "/coord.auth.Auth/Bootstrap" => None, // bootstrap 需一次性 token，服务内自校验
        "/coord.auth.Auth/GetRevocationDelta" => None, // agent 角色同步，依赖 CCT（见 ServerAuthService 特判）

        _ => None, // 未知 RPC —— 默认拒绝（fail-closed）
    }
}

/// 匿名白名单（规格 C.4.1）：仅健康检查与登录端点匿名可访问。
///
/// `GetRevocationDelta` 需要合法 CCT 但属于公开的角色同步端点，
/// 由服务自身校验（agent 高频调用，不走角色授权）。
pub fn is_whitelisted(rpc_method: &str) -> bool {
    matches!(
        rpc_method,
        "/grpc.health.v1.Health/Check"
            | "/coord.auth.Auth/Authenticate"
            // P2-07：refresh 与登录同为认证前置端点（凭 refresh token 自证，
            // 服务端校验单次使用语义）
            | "/coord.auth.Auth/RefreshToken"
    )
}

// ──── Tower Layer / Service（接入服务端 gRPC 生产路由，P0-C.1）────
//
// tonic 0.14 的 `tonic::service::Interceptor` 拿不到方法路径（Request 不保留
// URI），故与 agent 侧一致使用 tower 中间件：从 http::Request 的 URI path 提取
// gRPC 方法，校验 CCT + capability，无凭据 / 未授权一律拒绝（fail-closed）。
//
// 注：tower 层无法读取 TLS 对端证书（tonic 仅在 `tonic::Request` 层暴露
// `peer_certs()`），因此 mTLS CN 信任快速路径当前不可用 —— 所有请求走全量
// 能力校验（peer_cn=None），比快速路径更严格，符合验收口径。

/// ServerAuthInterceptor 的 tower Layer（`tonic::Server::builder().layer(...)`）
#[derive(Clone)]
pub struct ServerAuthLayer {
    interceptor: Arc<ServerAuthInterceptor>,
}

impl ServerAuthLayer {
    pub fn new(interceptor: Arc<ServerAuthInterceptor>) -> Self {
        Self { interceptor }
    }
}

impl<S> Layer<S> for ServerAuthLayer {
    type Service = ServerAuthService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ServerAuthService {
            inner,
            interceptor: self.interceptor.clone(),
        }
    }
}

/// 服务端鉴权中间件：包一层 inner 服务，先鉴权后转发。
#[derive(Clone)]
pub struct ServerAuthService<S> {
    inner: S,
    interceptor: Arc<ServerAuthInterceptor>,
}

impl<S> Service<http::Request<tonic::body::Body>> for ServerAuthService<S>
where
    S: Service<http::Request<tonic::body::Body>, Response = http::Response<tonic::body::Body>>,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = S::Error;
    type Future = ServerAuthFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<tonic::body::Body>) -> Self::Future {
        let rpc_method = req.uri().path().to_string();

        // 白名单：健康检查 + 登录端点匿名可访问（规格 C.4.1）
        if is_whitelisted(&rpc_method) {
            return ServerAuthFuture::Allow(self.inner.call(req));
        }

        // GetRevocationDelta：agent 角色同步端点，需合法 CCT 但不做角色授权
        if rpc_method == "/coord.auth.Auth/GetRevocationDelta" {
            let auth_header = req
                .headers()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
            return match self.interceptor.verify_token_only(auth_header.as_deref()) {
                Ok(()) => ServerAuthFuture::Allow(self.inner.call(req)),
                Err(reason) => {
                    self.audit_deny(&rpc_method, &reason);
                    ServerAuthFuture::Deny(Some(classify_denial(&reason)))
                }
            };
        }

        let auth_header = req
            .headers()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // 未知 RPC：拒绝（fail-closed）
        let Some(capability_id) = infer_capability(&rpc_method) else {
            let reason = format!("unknown RPC method: {rpc_method}");
            self.audit_deny(&rpc_method, &reason);
            return ServerAuthFuture::Deny(Some(Status::permission_denied(reason)));
        };

        match self.interceptor.validate(
            auth_header.as_deref(),
            None, // tower 层无法读取 TLS 对端证书 → 全量校验
            &capability_id,
            None,
        ) {
            ServerAuthResult::Allow { .. } => ServerAuthFuture::Allow(self.inner.call(req)),
            ServerAuthResult::Deny { reason, .. } => {
                self.audit_deny(&rpc_method, &reason);
                ServerAuthFuture::Deny(Some(classify_denial(&reason)))
            }
        }
    }
}

impl<S> ServerAuthService<S> {
    /// P2-08：记录鉴权拒绝审计事件（v1：tower 层无对端地址与主体身份，actor 记 anonymous）。
    fn audit_deny(&self, rpc_method: &str, reason: &str) {
        if let Some(ref audit) = self.interceptor.audit {
            audit.record_event(
                "anonymous",
                rpc_method,
                rpc_method,
                crate::audit::RESULT_DENIED,
                reason,
            );
        }
    }
}

/// 将拒绝原因映射为 gRPC 状态码：
/// 认证类问题（缺 token/签名/过期/吊销）→ `UNAUTHENTICATED`；
/// 授权类问题（能力/scope 不足）→ `PERMISSION_DENIED`。
pub fn classify_denial(reason: &str) -> Status {
    if reason.contains("missing")
        || reason.contains("validation")
        || reason.contains("expired")
        || reason.contains("revoked")
    {
        Status::unauthenticated(reason.to_string())
    } else {
        Status::permission_denied(reason.to_string())
    }
}

/// 鉴权中间件 future：放行转发 inner；拒绝返回 gRPC 错误响应。
pub enum ServerAuthFuture<F> {
    Allow(F),
    Deny(Option<Status>),
}

impl<F, E> Future for ServerAuthFuture<F>
where
    F: Future<Output = Result<http::Response<tonic::body::Body>, E>>,
{
    type Output = Result<http::Response<tonic::body::Body>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: 不移动字段；AuthFuture 无 pin 投影约定，字段级 pin 由我们手动保证
        let this = unsafe { self.get_unchecked_mut() };
        match this {
            ServerAuthFuture::Allow(fut) => unsafe { Pin::new_unchecked(fut) }.poll(cx),
            ServerAuthFuture::Deny(status) => match status.take() {
                Some(status) => {
                    let (parts, ()) = status.into_http::<()>().into_parts();
                    let response = http::Response::from_parts(parts, tonic::body::Body::empty());
                    Poll::Ready(Ok(response))
                }
                // 已就绪后重复 poll 属 Future 契约外行为：保持 Pending，避免 panic
                None => Poll::Pending,
            },
        }
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
    use crate::auth::revocation::RevocationStore;
    use crate::auth::token_signing::TokenSigningKeyring;
    use coord_core::auth::cct::{encode_cct, CctHeader, CctPayload};

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
        let cct = encode_cct(&header, &payload, &key.key_bytes).expect("encode_cct should succeed");
        assert!(!cct.is_empty());
        assert!(
            cct.starts_with("eyJ"),
            "CCT should start with base64url JSON"
        );

        // Decode
        let decoded = decode_cct(&cct, &key.key_bytes).expect("decode_cct should succeed");
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
            ServerAuthResult::Allow {
                scope_checked,
                trusted_agent,
                ..
            } => {
                assert!(
                    !scope_checked,
                    "scope should be skipped for trusted agent + low risk"
                );
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
        let auth_manager = Arc::new(AuthManager::new());
        auth_manager.role_add("reader").unwrap();
        auth_manager
            .role_grant_capability("reader", "data:kv:read", "")
            .unwrap_or(());
        let interceptor = ServerAuthInterceptor::new(keyring.clone(), rev_store, true)
            .with_role_provider(auth_manager);

        let cct = make_test_cct(&keyring, vec!["reader"], std::collections::HashMap::new());
        let auth_header = format!("Bearer {cct}");

        // Non-trusted caller + low-risk read → still requires full check
        let result = interceptor.validate(
            Some(&auth_header),
            Some("random-client"),
            "data:kv:read",
            Some("/any/key"),
        );

        match result {
            ServerAuthResult::Allow {
                scope_checked,
                trusted_agent,
                ..
            } => {
                assert!(
                    scope_checked,
                    "scope should be checked for non-trusted caller"
                );
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
        let auth_manager = Arc::new(AuthManager::new());
        auth_manager.role_add("admin").unwrap();
        auth_manager
            .role_grant_capability("admin", "data:kv:write", "")
            .unwrap_or(());
        let interceptor = ServerAuthInterceptor::new(keyring.clone(), rev_store, true)
            .with_role_provider(auth_manager);

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
            ServerAuthResult::Allow {
                scope_checked,
                trusted_agent,
                ..
            } => {
                assert!(
                    scope_checked,
                    "scope should be checked for high-risk operations"
                );
                assert!(trusted_agent);
            }
            ServerAuthResult::Deny { reason, .. } => {
                panic!("expected Allow but got Deny: {reason}");
            }
        }
    }

    // ──── P0-C.4：收紧后的 fail-closed 行为 ────

    #[test]
    fn test_no_scope_override_no_role_grant_is_denied() {
        // 删除"有任意 role 即放行"兕底后：有 roles 但无授权 → 拒绝
        let keyring = make_keyring();
        let rev_store = make_revocation_store();
        let auth_manager = Arc::new(AuthManager::new());
        let interceptor = ServerAuthInterceptor::new(keyring.clone(), rev_store, true)
            .with_role_provider(auth_manager);

        let cct = make_test_cct(&keyring, vec!["reader"], std::collections::HashMap::new());
        let auth_header = format!("Bearer {cct}");

        let result = interceptor.validate(
            Some(&auth_header),
            None,
            "data:kv:write", // reader 未授权写
            None,
        );
        assert!(matches!(result, ServerAuthResult::Deny { .. }));
    }

    #[test]
    fn test_role_grant_without_scope_allows_capability() {
        let keyring = make_keyring();
        let rev_store = make_revocation_store();
        let auth_manager = Arc::new(AuthManager::new());
        auth_manager.role_add("reader").unwrap();
        auth_manager
            .role_grant_capability("reader", "data:kv:read", "")
            .unwrap_or(());
        let interceptor = ServerAuthInterceptor::new(keyring.clone(), rev_store, true)
            .with_role_provider(auth_manager);

        let cct = make_test_cct(&keyring, vec!["reader"], std::collections::HashMap::new());
        let auth_header = format!("Bearer {cct}");

        let result = interceptor.validate(Some(&auth_header), None, "data:kv:read", None);
        assert!(
            result.is_allow(),
            "capability-level grant should pass: {result:?}"
        );

        // 未授权的能力仍被拒
        let result = interceptor.validate(Some(&auth_header), None, "data:kv:write", None);
        assert!(matches!(result, ServerAuthResult::Deny { .. }));
    }

    #[test]
    fn test_role_grant_with_scope_requires_scope_key() {
        let keyring = make_keyring();
        let rev_store = make_revocation_store();
        let auth_manager = Arc::new(AuthManager::new());
        auth_manager.role_add("reader").unwrap();
        auth_manager
            .role_grant_capability("reader", "data:kv:read", "/app/orders/")
            .unwrap_or(());
        let interceptor = ServerAuthInterceptor::new(keyring.clone(), rev_store, true)
            .with_role_provider(auth_manager);

        let cct = make_test_cct(&keyring, vec!["reader"], std::collections::HashMap::new());
        let auth_header = format!("Bearer {cct}");

        // scope_key 命中 → 放行
        let result = interceptor.validate(
            Some(&auth_header),
            None,
            "data:kv:read",
            Some("/app/orders/123"),
        );
        assert!(result.is_allow());

        // scope_key 越界 → 拒绝
        let result = interceptor.validate(
            Some(&auth_header),
            None,
            "data:kv:read",
            Some("/app/payments/1"),
        );
        assert!(matches!(result, ServerAuthResult::Deny { .. }));

        // scope_key 缺失 → fail-closed
        let result = interceptor.validate(Some(&auth_header), None, "data:kv:read", None);
        assert!(matches!(result, ServerAuthResult::Deny { .. }));
    }

    #[test]
    fn test_root_role_grants_everything() {
        let keyring = make_keyring();
        let rev_store = make_revocation_store();
        let auth_manager = Arc::new(AuthManager::new());
        let interceptor = ServerAuthInterceptor::new(keyring.clone(), rev_store, true)
            .with_role_provider(auth_manager);

        let cct = make_test_cct(&keyring, vec!["root"], std::collections::HashMap::new());
        let auth_header = format!("Bearer {cct}");

        for cap in [
            "data:kv:read",
            "data:kv:write",
            "admin:auth:user_add",
            "admin:maintenance:member_add",
        ] {
            let result = interceptor.validate(Some(&auth_header), None, cap, None);
            assert!(result.is_allow(), "root role should pass {cap}");
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
        let auth_manager = Arc::new(AuthManager::new());
        auth_manager.role_add("reader").unwrap();
        auth_manager
            .role_grant_capability("reader", "data:kv:read", "")
            .unwrap_or(());
        // mTLS NOT enforced → even coord-agent-* is not trusted
        let interceptor = ServerAuthInterceptor::new(keyring.clone(), rev_store, false)
            .with_role_provider(auth_manager);

        let cct = make_test_cct(&keyring, vec!["reader"], std::collections::HashMap::new());
        let auth_header = format!("Bearer {cct}");

        let result = interceptor.validate(
            Some(&auth_header),
            Some("coord-agent-1"), // would be trusted if mTLS enforced
            "data:kv:read",
            Some("/any/key"),
        );

        match result {
            ServerAuthResult::Allow {
                scope_checked,
                trusted_agent,
                ..
            } => {
                assert!(
                    scope_checked,
                    "scope should be checked when mTLS is not enforced"
                );
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
        assert_eq!(
            extract_bearer_token(Some("Bearer mytoken")),
            Some("mytoken")
        );
        assert_eq!(
            extract_bearer_token(Some("eyJhbGciOiJI...")),
            Some("eyJhbGciOiJI...")
        );
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
