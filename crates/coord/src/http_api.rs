//! HTTP control-plane API.
//!
//! This module assembles the axum `Router` and delegates handler logic
//! to domain-specific submodules. After the T-P1-05 split the hub file
//! contains only:
//!
//! * shared state types (`HttpApiState`)
//! * route registration (`build_http_router`)
//! * infrastructure endpoints (healthz, readyz, role, metrics)
//! * trace-context middleware

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Redirect;
use axum::routing::{get, post};

use crate::application::config_app::ConfigApp;
use crate::application::pki_app::PkiApp;
use crate::application::transit_app::TransitApp;
use crate::raft_runtime::RaftRuntime;
use coord_core::clock::{Clock, SystemClock};
use coord_core::rate_limit::{RateLimitConfig, RateLimiter};
use coord_core::state::CoordinatorState;

mod audit;
mod auth;
mod cluster;
mod config_http;
mod console_roles;
mod error;
mod helpers;
mod lock_http;
mod pki_http;
mod registry_http;
mod security_http;
mod transit_http;
mod ui;

pub use ui::resolve_ui_dist_dir;

use ui::{ui_index, ui_path};

use std::path::PathBuf;

#[derive(Clone)]
pub struct HttpApiState {
    pub(super) state: CoordinatorState,
    pub(super) raft: RaftRuntime,
    pub(super) config_app: ConfigApp,
    pub(super) transit_app: TransitApp,
    pub(super) pki_app: PkiApp,
    pub(super) ui_dist_dir: PathBuf,
    pub(super) high_risk_limiter: std::sync::Arc<RateLimiter>,
}

/// Rate-limit budget for high-risk endpoints (login / seal / backup / restore).
pub(super) const HIGH_RISK_CAPACITY: u32 = 5;
pub(super) const HIGH_RISK_REFILL_PER_SEC: f64 = 0.5;

pub fn build_http_router(
    state: CoordinatorState,
    raft: RaftRuntime,
    config_app: ConfigApp,
    transit_app: TransitApp,
    pki_app: PkiApp,
    ui_dist_dir: PathBuf,
) -> Router {
    let clock: std::sync::Arc<dyn Clock> = std::sync::Arc::new(SystemClock);
    let high_risk_limiter = std::sync::Arc::new(RateLimiter::new(
        RateLimitConfig::new(HIGH_RISK_CAPACITY, HIGH_RISK_REFILL_PER_SEC),
        clock,
    ));
    let app_state = HttpApiState {
        state,
        raft,
        config_app,
        transit_app,
        pki_app,
        ui_dist_dir,
        high_risk_limiter,
    };

    Router::new()
        .route("/", get(|| async { Redirect::temporary("/ui") }))
        // ── Infrastructure ───────────────────────────────────────────────
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/api/v1/role", get(role))
        .route("/metrics", get(metrics))
        // ── Cluster / Admin ──────────────────────────────────────────────
        .route("/api/v1/overview", get(cluster::overview))
        .route("/api/v1/cluster/status", get(cluster::cluster_status))
        .route(
            "/api/v1/cluster/member-add",
            post(cluster::cluster_member_add),
        )
        .route(
            "/api/v1/cluster/member-remove",
            post(cluster::cluster_member_remove),
        )
        .route("/api/v1/admin/backup/create", post(cluster::backup_create))
        .route(
            "/api/v1/admin/backup/restore",
            post(cluster::backup_restore),
        )
        // ── Service registry ─────────────────────────────────────────────
        .route("/api/v1/services", get(registry_http::services))
        // ── Config ───────────────────────────────────────────────────────
        .route("/api/v1/configs", get(config_http::configs))
        .route("/api/v1/configs/put", post(config_http::configs_put))
        // ── Locks ────────────────────────────────────────────────────────
        .route("/api/v1/locks", get(lock_http::locks))
        // ── Workflow ─────────────────────────────────────────────────────
        // v1 workflow HTTP endpoints removed (see ADR-001)
        // ── Transit ──────────────────────────────────────────────────────
        .route("/api/v1/transit/keys", get(transit_http::transit_keys))
        .route(
            "/api/v1/transit/keys/create",
            post(transit_http::transit_key_create),
        )
        .route(
            "/api/v1/transit/keys/rotate",
            post(transit_http::transit_key_rotate),
        )
        // ── PKI ──────────────────────────────────────────────────────────
        .route("/api/v1/pki/certificates", get(pki_http::pki_certificates))
        .route("/api/v1/pki/issue", post(pki_http::pki_issue))
        .route("/api/v1/pki/renew", post(pki_http::pki_renew))
        .route("/api/v1/pki/status", post(pki_http::pki_status))
        .route("/api/v1/pki/revoke", post(pki_http::pki_revoke))
        .route(
            "/api/v1/pki/auto-renew/policy",
            post(pki_http::pki_auto_renew_policy_update),
        )
        .route(
            "/api/v1/pki/auto-renew/run",
            post(pki_http::pki_auto_renew_run),
        )
        .route(
            "/api/v1/pki/acme/order",
            post(pki_http::pki_acme_order_create),
        )
        .route(
            "/api/v1/pki/acme/challenge",
            post(pki_http::pki_acme_challenge_complete),
        )
        .route(
            "/api/v1/pki/acme/finalize",
            post(pki_http::pki_acme_finalize),
        )
        // ── Security ─────────────────────────────────────────────────────
        .route(
            "/api/v1/security/status",
            get(security_http::security_status),
        )
        .route(
            "/api/v1/security/login",
            post(security_http::security_login),
        )
        .route("/api/v1/security/seal", post(security_http::security_seal))
        .route("/api/v1/security/token", get(security_http::security_token))
        .route("/api/v1/security/roles", get(security_http::security_roles))
        .route(
            "/api/v1/security/roles/bootstrap-console",
            post(security_http::security_bootstrap_console_roles),
        )
        // ── UI ───────────────────────────────────────────────────────────
        .route("/ui", get(ui_index))
        .route("/ui/*path", get(ui_path))
        // ── Middleware ───────────────────────────────────────────────────
        .layer(axum::extract::DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
        .layer(axum::middleware::from_fn(trace_context_layer))
        .with_state(app_state)
}

// ── Infrastructure endpoints ─────────────────────────────────────────────────

/// HTTP request body cap for all control-plane endpoints (2 MiB).
pub const MAX_REQUEST_BODY_BYTES: usize = 2 * 1024 * 1024;

async fn healthz() -> &'static str {
    "ok"
}

async fn readyz(State(app): State<HttpApiState>) -> (StatusCode, &'static str) {
    let security_status = app.state.security().seal_status().await;
    if security_status.initialized && security_status.sealed {
        return (StatusCode::SERVICE_UNAVAILABLE, "sealed");
    }
    let role = app.raft.role_label().await.to_ascii_lowercase();
    if role == "unknown" {
        return (StatusCode::SERVICE_UNAVAILABLE, "raft not ready");
    }
    if app.raft.current_commit_index() == 0 {
        return (StatusCode::SERVICE_UNAVAILABLE, "raft election pending");
    }
    (StatusCode::OK, "ok")
}

async fn role(State(app): State<HttpApiState>) -> (StatusCode, String) {
    let role = app.raft.role_label().await.to_ascii_lowercase();
    let node_id = app.state.runtime().node_id.clone();
    (StatusCode::OK, format!("{role} {node_id}\n"))
}

async fn metrics(State(app): State<HttpApiState>) -> (StatusCode, String) {
    match app.state.metrics().render_text() {
        Ok(body) => (StatusCode::OK, body),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err),
    }
}

// ── Trace-context middleware ─────────────────────────────────────────────────

async fn trace_context_layer(
    req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use tracing::Instrument;
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let trace_ctx = req
        .headers()
        .get("traceparent")
        .and_then(|v| v.to_str().ok())
        .and_then(crate::telemetry::TraceContext::parse);
    let span = match &trace_ctx {
        Some(tc) => tracing::info_span!(
            "http.request",
            method = %method,
            path = %path,
            trace_id = %tc.trace_id,
            parent_span_id = %tc.parent_span_id,
        ),
        None => tracing::info_span!("http.request", method = %method, path = %path),
    };
    next.run(req).instrument(span).await
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod readyz_tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use crate::raft_runtime::RaftRuntime;
    use crate::raft_store::RaftStore;
    use coord_core::state::{CoordinatorState, RuntimeConfig};

    fn test_state(tag: &str) -> CoordinatorState {
        let dir =
            std::env::temp_dir().join(format!("readyz-{tag}-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        CoordinatorState::new(RuntimeConfig {
            node_id: tag.to_string(),
            data_dir: dir,
            dev_mode: true,
        })
        .expect("state")
    }

    fn test_raft(state: CoordinatorState) -> RaftRuntime {
        let dir = state.runtime().data_dir.clone();
        let store =
            RaftStore::open(&dir, &state.runtime().node_id, "127.0.0.1:9090").expect("raft store");
        RaftRuntime::new(state, store, "127.0.0.1:9090".to_string())
    }

    fn build_app(state: CoordinatorState, raft: RaftRuntime) -> axum::Router {
        use crate::application::config_app::ConfigApp;
        use crate::application::pki_app::PkiApp;
        use crate::application::transit_app::TransitApp;
        use coord_core::proposer::RaftProposer;
        use std::path::PathBuf;
        use std::sync::Arc;
        let raft_proposer: Arc<dyn RaftProposer> = Arc::new(raft.clone());
        let config_app = ConfigApp::new(
            state.config().clone(),
            state.metrics().clone(),
            raft_proposer.clone(),
        );
        let transit_app = TransitApp::new(
            state.transit().clone(),
            state.metrics().clone(),
            raft_proposer.clone(),
        );
        let pki_app = PkiApp::new(
            state.pki().clone(),
            state.metrics().clone(),
            raft_proposer.clone(),
        );
        super::build_http_router(
            state,
            raft,
            config_app,
            transit_app,
            pki_app,
            PathBuf::from("/nonexistent"),
        )
    }

    #[tokio::test]
    async fn readyz_returns_503_when_raft_commit_index_is_zero() {
        let state = test_state("readyz-zero");
        let raft = test_raft(state.clone());
        let app = build_app(state, raft);

        let resp = app
            .oneshot(Request::get("/readyz").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn healthz_always_returns_200() {
        let state = test_state("healthz-ok");
        let raft = test_raft(state.clone());
        let app = build_app(state, raft);

        let resp = app
            .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
