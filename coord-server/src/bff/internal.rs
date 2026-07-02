// BFF Internal Routes — Core 内部 API
//
// 这些路由处理 Core 内部 API 调用（/v1/*），
// 由 BFF 的 CoreClient（基于 reqwest loopback）调用，
// 直接在进程内操作 AuthManager/TokenManager，避免网络开销。
//
// 路由：
// - POST /v1/auth/approle/login   — AppRole 登录
// - GET  /v1/auth/token/lookup-self  — Token 自查
// - POST /v1/auth/token/renew-self   — Token 续期
// - ANY  /v1/{*path}                 — 未实现路径，返回 JSON 错误（防穿透 SPA fallback）

use std::sync::Arc;

use axum::{
    Json,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use serde_json::{json, Value};

use crate::auth::{AuthManager, TokenManager};
use crate::server::CoordNode;

// ──── Internal State ────

/// 内部路由的共享状态
pub struct InternalState {
    pub auth_manager: Arc<AuthManager>,
    pub token_manager: Arc<TokenManager>,
    /// 服务端核心节点（提供 KV 读写能力）
    pub coord_node: Arc<CoordNode>,
}

// ──── Handlers ────

/// POST /v1/auth/approle/login
///
/// 请求体: { "role_id": "...", "secret_id": "..." }
/// 响应:   Vault-style { "auth": { "client_token": "...", "policies": [...], ... } }
pub async fn approle_login(
    State(state): State<Arc<InternalState>>,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    let role_id = body
        .get("role_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let secret_id = body
        .get("secret_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    // AppRole 认证：将 role_id 作为用户名，secret_id 作为密码进行认证
    match state.auth_manager.authenticate(role_id, secret_id) {
        Ok(()) => {
            // 签发 Token
            let token = state.token_manager.issue_token(role_id);

            // 获取用户角色
            let roles = state.auth_manager.user_get_roles(role_id).unwrap_or_default();

            let response = json!({
                "auth": {
                    "client_token": token.token,
                    "accessor": "internal",
                    "policies": roles,
                    "token_policies": roles,
                    "lease_duration": 28800,
                    "renewable": true,
                }
            });

            (StatusCode::OK, Json(response)).into_response()
        }
        Err(_) => {
            let response = json!({
                "errors": ["invalid role_id or secret_id"]
            });
            (StatusCode::BAD_REQUEST, Json(response)).into_response()
        }
    }
}

/// GET /v1/auth/token/lookup-self
///
/// 请求头: X-Vault-Token: <token>
/// 响应:   { "data": { "policies": [...], ... } }
pub async fn token_lookup(
    State(state): State<Arc<InternalState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let token = extract_bearer_token(&headers);

    match token {
        Some(t) => match state.token_manager.validate(&t) {
            Ok(username) => {
                let roles = state.auth_manager.user_get_roles(&username).unwrap_or_default();
                let response = json!({
                    "data": {
                        "accessor": "internal",
                        "creation_time": "2026-01-01T00:00:00Z",
                        "display_name": format!("approle-{}", username),
                        "policies": roles,
                        "token_policies": roles,
                        "ttl": 28800,
                    }
                });
                (StatusCode::OK, Json(response)).into_response()
            }
            Err(_) => {
                let response = json!({
                    "errors": ["permission denied"]
                });
                (StatusCode::FORBIDDEN, Json(response)).into_response()
            }
        },
        None => {
            let response = json!({
                "errors": ["missing token"]
            });
            (StatusCode::BAD_REQUEST, Json(response)).into_response()
        }
    }
}

/// POST /v1/auth/token/renew-self
///
/// 请求头: X-Vault-Token: <token>
pub async fn token_renew(
    State(state): State<Arc<InternalState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let token = extract_bearer_token(&headers);

    match token {
        Some(t) => match state.token_manager.validate(&t) {
            Ok(username) => {
                state.token_manager.revoke(&t);
                let new_token = state.token_manager.issue_token(&username);
                let response = json!({
                    "auth": {
                        "client_token": new_token.token,
                        "lease_duration": 28800,
                        "renewable": true,
                    }
                });
                (StatusCode::OK, Json(response)).into_response()
            }
            Err(_) => {
                let response = json!({
                    "errors": ["permission denied"]
                });
                (StatusCode::FORBIDDEN, Json(response)).into_response()
            }
        },
        None => {
            let response = json!({
                "errors": ["missing token"]
            });
            (StatusCode::BAD_REQUEST, Json(response)).into_response()
        }
    }
}

/// 从请求头中提取 Bearer Token（优先 X-Vault-Token，其次 Authorization: Bearer）
pub fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    // 优先 X-Vault-Token（Vault 风格）
    if let Some(val) = headers.get("x-vault-token") {
        return val.to_str().ok().map(|s| s.to_string());
    }
    // 其次 Authorization: Bearer <token>
    if let Some(val) = headers.get("authorization") {
        if let Ok(auth) = val.to_str() {
            if let Some(token) = auth.strip_prefix("Bearer ") {
                return Some(token.to_string());
            }
        }
    }
    None
}

/// ANY /v1/{*path} — 未实现路径的 catch-all
///
/// 返回 JSON 格式的 404 错误，防止未实现的 API 调用穿透到 SPA fallback 返回 HTML。
pub async fn not_found(
    axum::extract::Path(path): axum::extract::Path<String>,
) -> impl IntoResponse {
    let response = json!({
        "errors": [format!("unsupported path: /v1/{}", path)]
    });
    (StatusCode::NOT_FOUND, Json(response)).into_response()
}
