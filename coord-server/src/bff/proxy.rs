// BFF 代理路由处理函数
//
// 实现以下端点：
// - POST /api/v1/auth/login      → 转发 Core AppRole 登录
// - POST /api/v1/auth/renew      → 转发 Token 续期
// - POST /api/v1/auth/revoke     → 吊销 Token
// - GET  /api/v1/auth/userinfo   → 查询当前 Token 信息
// - ANY  /api/v1/{*path}         → 通用转发（注入 X-Vault-Token）

use std::sync::Arc;

use axum::{
    Json,
    extract::State,
    http::{StatusCode, HeaderMap},
    response::Response,
    body::Body,
};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::CoreClient;

// ──── 请求/响应类型 ────

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    #[serde(rename = "roleId")]
    pub role_id: String,
    #[serde(rename = "secretId")]
    pub secret_id: String,
}

#[derive(Debug, Serialize)]
pub struct ApiResponse<T: Serialize> {
    pub code: i32,
    pub data: T,
    pub message: String,
}

fn ok_response<T: Serialize>(data: T) -> Json<ApiResponse<T>> {
    Json(ApiResponse {
        code: 0,
        data,
        message: "success".to_string(),
    })
}

fn error_response(code: i32, message: &str) -> (StatusCode, Json<Value>) {
    (
        StatusCode::OK, // BFF 始终返回 200，业务错误在 JSON body 中
        Json(json!({ "code": code, "data": Value::Null, "message": message })),
    )
}

// ──── 认证端点 ────

/// POST /api/v1/auth/login
///
/// 将 RoleID + SecretID 转发 Core AppRole 登录，获取 client_token，
/// 写入 HttpOnly Cookie 后返回用户信息。
pub async fn login(
    State(core): State<Arc<dyn CoreClient>>,
    Json(creds): Json<LoginRequest>,
) -> Result<Response, (StatusCode, Json<Value>)> {
    // 构建 AppRole 登录请求体
    let body = json!({
        "role_id": creds.role_id,
        "secret_id": creds.secret_id,
    });

    let (status, resp_body, _content_type) = core
        .forward("POST", "/v1/auth/approle/login", &serde_json::to_vec(&body).unwrap(), None)
        .await
        .map_err(|e| error_response(500, &format!("Core 通信失败: {e}")))?;

    if status != 200 {
        let err: Value = serde_json::from_slice(&resp_body).unwrap_or(json!({}));
        let msg = err["errors"].as_array()
            .and_then(|a| a.first())
            .and_then(|e| e.as_str())
            .unwrap_or("登录失败");
        return Err(error_response(status as i32, msg));
    }

    let resp: Value = serde_json::from_slice(&resp_body)
        .map_err(|e| error_response(500, &format!("解析响应失败: {e}")))?;

    let token = resp["auth"]["client_token"]
        .as_str()
        .ok_or_else(|| error_response(500, "响应中未找到 client_token"))?;

    let policies: Vec<String> = resp["auth"]["policies"]
        .as_array()
        .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default();

    let user_data = json!({
        "policies": policies,
        "role": creds.role_id,
        "displayName": format!("AppRole:{}", &creds.role_id[..8.min(creds.role_id.len())]),
        "tokenAccessor": resp["auth"]["accessor"].as_str().unwrap_or(""),
        "tokenTtl": resp["auth"]["lease_duration"].as_u64().unwrap_or(28800),
        "tokenMaxTtl": 86400,
    });

    // 构建响应（带 Set-Cookie）
    let cookie = format!(
        "coord_token={}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=86400",
        token
    );

    let body = serde_json::to_string(&json!({
        "code": 0,
        "data": user_data,
        "message": "success"
    }))
    .unwrap();

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .header("Set-Cookie", cookie)
        .body(Body::from(body))
        .unwrap())
}

/// POST /api/v1/auth/renew
pub async fn renew_token(
    State(_core): State<Arc<dyn CoreClient>>,
    _headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    // 占位：后续迭代实现 Token 续期
    error_response(501, "Token 续期尚未实现")
}

/// POST /api/v1/auth/revoke
pub async fn revoke_token(
    State(_core): State<Arc<dyn CoreClient>>,
    _headers: HeaderMap,
) -> Result<Response, (StatusCode, Json<Value>)> {
    // 清除 Cookie
    let cookie = "coord_token=; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=0";
    let body = serde_json::to_string(&json!({
        "code": 0,
        "data": {},
        "message": "success"
    }))
    .unwrap();

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .header("Set-Cookie", cookie)
        .body(Body::from(body))
        .unwrap())
}

/// GET /api/v1/auth/userinfo
pub async fn userinfo(
    State(core): State<Arc<dyn CoreClient>>,
    headers: HeaderMap,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    // 从 Cookie 提取 Token
    let token = extract_token(&headers);

    let (status, resp_body, _) = core
        .forward("GET", "/v1/auth/token/lookup-self", &[], token.as_deref())
        .await
        .map_err(|e| error_response(500, &format!("Core 通信失败: {e}")))?;

    if status != 200 {
        return Err(error_response(status as i32, "Token 验证失败"));
    }

    let resp: Value = serde_json::from_slice(&resp_body)
        .map_err(|e| error_response(500, &format!("解析响应失败: {e}")))?;

    let policies: Vec<String> = resp["data"]["policies"]
        .as_array()
        .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default();

    Ok(Json(json!({
        "code": 0,
        "data": {
            "policies": policies,
            "role": "user",
            "displayName": resp["data"]["display_name"].as_str().unwrap_or("用户"),
            "tokenAccessor": resp["data"]["accessor"].as_str().unwrap_or(""),
            "tokenTtl": resp["data"]["ttl"].as_u64().unwrap_or(0),
        },
        "message": "success"
    })))
}

/// ANY /api/v1/{*path} — 通用转发
///
/// 提取 Cookie 中的 Token，注入 X-Vault-Token 头后转发至 Core 内部 API。
/// 保留原始查询字符串，确保搜索/过滤/分页参数不丢失。
pub async fn forward(
    method: axum::http::Method,
    State(core): State<Arc<dyn CoreClient>>,
    headers: HeaderMap,
    axum::extract::Path(path): axum::extract::Path<String>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    body: axum::body::Bytes,
) -> Result<Response, (StatusCode, Json<Value>)> {
    let token = extract_token(&headers);

    // 转发路径映射：/api/v1/* → /v1/*，保留查询参数
    let core_path = if let Some(qs) = raw_query {
        format!("/v1/{}?{}", path, qs)
    } else {
        format!("/v1/{}", path)
    };

    let (status, resp_body, content_type) = core
        .forward(method.as_str(), &core_path, &body, token.as_deref())
        .await
        .map_err(|e| error_response(500, &format!("Core 通信失败: {e}")))?;

    Ok(Response::builder()
        .status(StatusCode::from_u16(status as u16).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
        .header("Content-Type", content_type)
        .body(Body::from(resp_body))
        .unwrap())
}

// ──── 辅助函数 ────

/// 从请求头中提取 Token（优先 Cookie，其次 Authorization）
fn extract_token(headers: &HeaderMap) -> Option<String> {
    // 尝试从 Cookie 提取
    if let Some(cookie_header) = headers.get("cookie") {
        if let Ok(cookie_str) = cookie_header.to_str() {
            for part in cookie_str.split(';') {
                let part = part.trim();
                if let Some(value) = part.strip_prefix("coord_token=") {
                    return Some(value.to_string());
                }
            }
        }
    }

    // 回退到 Authorization 头
    if let Some(auth_header) = headers.get("authorization") {
        if let Ok(auth_str) = auth_header.to_str() {
            if let Some(token) = auth_str.strip_prefix("Bearer ") {
                return Some(token.to_string());
            }
        }
    }

    None
}
