// BFF (Backend For Frontend) 模块
//
// 为 Coord UI 控制台提供极简 HTTP 代理层。
// 职责（ADP/UI 开发文档 §3.2）：
// 1. 端口监听 — 复用 Server 的 HTTP 端口（由 ui_enabled 开关控制）
// 2. 静态资源服务 — 编译时通过 rust-embed 将前端 dist/ 嵌入二进制
// 3. SPA Fallback — 对非 /api 路径返回 index.html
// 4. API 转发 — 提取 Cookie Token 注入 X-Vault-Token 头后转发 Core
// 5. 健康检查 — /healthz, /ready, /metrics 端点

mod proxy;
pub mod internal;
pub mod static_files;
pub mod registry_api;
pub mod config_api;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::{
    Router,
    body::Body,
    extract::{Path, State},
    http::{Request, StatusCode},
    response::IntoResponse,
    routing::{any, delete, get, post, put},
};

use crate::metrics::Metrics;

pub use proxy::{forward, login, renew_token, revoke_token, userinfo};

/// BFF 配置
#[derive(Debug, Clone)]
pub struct BffConfig {
    /// 是否启用 UI
    pub ui_enabled: bool,
    /// HTTP 监听地址
    pub http_addr: String,
    /// Core API 内部地址（用于代理转发 loopback）
    pub core_addr: String,
}

/// 健康检查共享状态
#[derive(Clone)]
pub struct HealthState {
    pub metrics: Arc<Metrics>,
    pub raft_ready: Arc<AtomicBool>,
}

/// 构建 BFF axum Router
pub fn build_router(
    config: &BffConfig,
    core_client: Arc<dyn CoreClient>,
    internal_state: Option<Arc<internal::InternalState>>,
    health_state: Option<Arc<HealthState>>,
) -> Router {
    // 主路由（BFF 代理）
    let mut app = Router::new()
        // BFF 认证路由（精确匹配，优先于通配符）
        .route("/api/v1/auth/login", post(proxy::login))
        .route("/api/v1/auth/renew", post(proxy::renew_token))
        .route("/api/v1/auth/revoke", post(proxy::revoke_token))
        .route("/api/v1/auth/userinfo", get(proxy::userinfo))
        // BFF 通用转发
        .route("/api/v1/{*path}", any(proxy::forward))
        .with_state(Arc::clone(&core_client));

    // 健康检查路由（独立 state）
    if let Some(hs) = health_state {
        let health_router = Router::new()
            .route("/healthz", get(healthz_handler))
            .route("/ready", get(ready_handler))
            .route("/metrics", get(metrics_handler))
            .with_state(hs);
        app = app.merge(health_router);
    } else {
        app = app
            .route("/healthz", get(healthz_placeholder))
            .route("/ready", get(ready_placeholder))
            .route("/metrics", get(metrics_placeholder));
    }

    // 合并 Core 内部 API 路由（由 CoreClient loopback 调用）
    if let Some(state) = internal_state {
        let internal_router = Router::new()
            // Auth 内部 API
            .route("/v1/auth/approle/login", post(internal::approle_login))
            .route("/v1/auth/token/lookup-self", get(internal::token_lookup))
            .route("/v1/auth/token/renew-self", post(internal::token_renew))
            // Registry 内部 API
            .route("/v1/registry/services", get(registry_api::list_services))
            .route("/v1/registry/services/{name}", get(registry_api::get_service))
            .route("/v1/registry/services/{name}/instances/{id}", put(registry_api::update_instance))
            .route("/v1/registry/services/{name}/health-check", post(registry_api::health_check))
            // Config 内部 API（精确路由优先于通配符）
            .route("/v1/configs", get(config_api::list_configs))
            .route("/v1/configs", post(config_api::create_config))
            .route("/v1/configs/{group}/{key}/versions", get(config_api::list_versions))
            .route("/v1/configs/{group}/{key}/versions/{version}", get(config_api::get_version))
            .route("/v1/configs/{group}/{key}/rollback", post(config_api::rollback))
            .route("/v1/configs/{group}/{key}", get(config_api::get_config))
            .route("/v1/configs/{group}/{key}", put(config_api::update_config))
            .route("/v1/configs/{group}/{key}", delete(config_api::delete_config))
            // Catch-all（必须放在最后）
            .route("/v1/{*path}", any(internal::not_found))
            .with_state(state);
        app = app.merge(internal_router);
    }

    // UI 静态资源（仅 ui_enabled 时）
    if config.ui_enabled {
        app = app.route("/assets/{*path}", get(serve_static_asset));
        app = app.fallback_service(get(spa_fallback_handler));
    }

    app
}

// ──── 健康检查处理器（有 HealthState 时使用） ────

/// GET /healthz — 进程存活检查（含 Raft 就绪状态）
async fn healthz_handler(State(hs): State<Arc<HealthState>>) -> impl IntoResponse {
    let status = if hs.raft_ready.load(Ordering::Relaxed) {
        r#"{"status":"SERVING"}"#
    } else {
        r#"{"status":"NOT_SERVING"}"#
    };
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        status,
    )
}

/// GET /ready — 就绪检查
async fn ready_handler(State(hs): State<Arc<HealthState>>) -> impl IntoResponse {
    let code = if hs.raft_ready.load(Ordering::Relaxed) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    let status = if hs.raft_ready.load(Ordering::Relaxed) {
        r#"{"status":"READY"}"#
    } else {
        r#"{"status":"NOT_READY"}"#
    };
    (
        code,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        status,
    )
}

/// GET /metrics — Prometheus 指标
async fn metrics_handler(State(hs): State<Arc<HealthState>>) -> impl IntoResponse {
    let body = hs.metrics.render_prometheus_text();
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
}

/// 降级健康检查（无 HealthState 时使用）
async fn healthz_placeholder() -> impl IntoResponse {
    (StatusCode::OK, [(axum::http::header::CONTENT_TYPE, "application/json")], r#"{"status":"ok"}"#)
}
async fn ready_placeholder() -> impl IntoResponse {
    (StatusCode::OK, [(axum::http::header::CONTENT_TYPE, "application/json")], r#"{"status":"ready"}"#)
}
async fn metrics_placeholder() -> impl IntoResponse {
    (StatusCode::OK, [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")], "# placeholder\n")
}

// ──── 静态资源处理器 ────

/// GET /assets/{*path} — 提供内嵌前端静态资源
async fn serve_static_asset(Path(path): Path<String>) -> impl IntoResponse {
    // axum 的 /assets/{*path} 路由会去掉 /assets/ 前缀，
    // 但 rust-embed 嵌入的文件保留了 assets/ 目录结构，
    // 因此需要补回前缀以匹配嵌入文件路径。
    let full_path = format!("assets/{}", path);
    match static_files::serve_static(&full_path) {
        Some(response) => response,
        None => (
            StatusCode::NOT_FOUND,
            "Static asset not found",
        )
            .into_response(),
    }
}

/// SPA fallback — 对非 API、非资源路径返回 index.html
async fn spa_fallback_handler(_req: Request<Body>) -> impl IntoResponse {
    static_files::serve_index_html()
}

// ──── CoreClient trait ────

/// Core API 客户端 trait（用于 BFF 代理转发）
#[async_trait::async_trait]
pub trait CoreClient: Send + Sync {
    /// 转发 HTTP 请求到 Core API
    async fn forward(
        &self,
        method: &str,
        path: &str,
        body: &[u8],
        token: Option<&str>,
    ) -> Result<(u16, Vec<u8>, String), String>;
}

// ──── Reqwest-based CoreClient 实现 ────

/// 基于 reqwest 的 CoreClient 实现
///
/// 通过 HTTP loopback 调用同一进程内的 Core 内部 API（/v1/*）。
/// BFF 代理处理器通过此 client 将请求转发至内部路由。
#[derive(Clone)]
pub struct ReqwestCoreClient {
    client: reqwest::Client,
    base_url: String,
}

impl ReqwestCoreClient {
    /// 创建新的 ReqwestCoreClient
    pub fn new(base_url: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url,
        }
    }
}

#[async_trait::async_trait]
impl CoreClient for ReqwestCoreClient {
    async fn forward(
        &self,
        method: &str,
        path: &str,
        body: &[u8],
        token: Option<&str>,
    ) -> Result<(u16, Vec<u8>, String), String> {
        let url = format!("{}{}", self.base_url, path);

        let mut req = match method {
            "GET" => self.client.get(&url),
            "POST" => self.client.post(&url),
            "PUT" => self.client.put(&url),
            "DELETE" => self.client.delete(&url),
            _ => self.client.get(&url),
        };

        if !body.is_empty() {
            req = req.header("Content-Type", "application/json").body(body.to_vec());
        }

        if let Some(t) = token {
            req = req.header("X-Vault-Token", t);
        }

        let resp = req.send().await.map_err(|e| format!("HTTP forward error: {e}"))?;

        let status = resp.status().as_u16();
        let content_type = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/json")
            .to_string();
        let resp_body = resp.bytes().await.map_err(|e| format!("read body: {e}"))?.to_vec();

        Ok((status, resp_body, content_type))
    }
}
