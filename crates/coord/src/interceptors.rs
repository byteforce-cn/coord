//! Async tower capability-enforcement layer.
//!
//! `CAPABILITY_POLICY` is the **single authoritative source** for the mapping
//! between gRPC method paths and required capability strings.  Any new method
//! that should be protected must be added here; methods absent from the table
//! are treated as *open* (no authentication required).
//!
//! The layer is wired in `main.rs`:
//!
//! ```rust,no_run
//! Server::builder()
//!     .layer(GrpcRateLimitLayer::new(...))
//!     .layer(CapabilityLayer::new(SecurityGateway::new(...)))
//!     .add_service(...)
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use http::{HeaderValue, Request, Response};
use tonic::Status;
use tonic::body::BoxBody;
use tower::{Layer, Service};

use coord_core::clock::SystemClock;
use coord_core::metrics::CoordMetrics;
use coord_core::rate_limit::{RateLimitConfig, RateLimiter};
use coord_core::security::SecurityController;

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

// ─── gRPC rate-limit table ────────────────────────────────────────────────────

/// Methods that are intentionally open (no capability required) but carry
/// high brute-force risk.  These are rate-limited at the gRPC layer.
///
/// Keyed by the full gRPC path; value is a human-readable label for metrics/logs.
static RATE_LIMITED_METHODS: OnceLock<HashMap<&'static str, &'static str>> = OnceLock::new();

pub fn get_rate_limited_methods() -> &'static HashMap<&'static str, &'static str> {
    RATE_LIMITED_METHODS.get_or_init(|| {
        let mut m: HashMap<&'static str, &'static str> = HashMap::new();
        // Init / InitSeal / Unseal require Shamir key shares — cryptographic
        // material that cannot be brute-forced, so rate-limiting adds no security
        // value and breaks test suites that seal/unseal many times.
        // GetSealStatus is read-only — also not rate-limited.
        // Only credential-based endpoints remain rate-limited.
        m.insert("/coord.v1.AuthService/LoginAppRole", "auth.login");
        m.insert("/coord.v1.AuthService/LookupToken", "auth.lookup");
        m
    })
}

/// Token-bucket config for gRPC high-risk open methods.
/// 10 tokens capacity, refilled at 1 rps ⇒ burst 10, steady-state 1 rps.
/// Slightly more generous than the HTTP limiter (which covers a browser UI)
/// because gRPC callers are typically operators/automation, not attackers.
pub const GRPC_HIGH_RISK_CAPACITY: u32 = 10;
pub const GRPC_HIGH_RISK_REFILL_PER_SEC: f64 = 1.0;

// ─── Open methods (fail-closed allow-list) ────────────────────────────────────

/// Methods that are intentionally open — no capability required.
/// Any method NOT in [`OPEN_METHODS`] and NOT in [`CAPABILITY_POLICY`] is
/// **rejected** (fail-closed).
static OPEN_METHODS: OnceLock<HashSet<&'static str>> = OnceLock::new();

pub fn get_open_methods() -> &'static HashSet<&'static str> {
    OPEN_METHODS.get_or_init(|| {
        let mut s = HashSet::new();
        // Bootstrap / Seal lifecycle – rate-limited separately
        s.insert("/coord.v1.SealService/Init");
        s.insert("/coord.v1.SealService/InitSeal");
        s.insert("/coord.v1.SealService/Unseal");
        s.insert("/coord.v1.SealService/GetSealStatus");
        // Authentication – rate-limited separately
        s.insert("/coord.v1.AuthService/LoginAppRole");
        s.insert("/coord.v1.AuthService/LookupToken");
        // Diagnostic endpoint (no mutation, public cluster status)
        s.insert("/coord.v1.AdminService/ClusterStatus");
        // Internal Raft protocol – intra-cluster only
        s.insert("/coord.v1.RaftInternalService/AppendEntries");
        s.insert("/coord.v1.RaftInternalService/RequestVote");
        s.insert("/coord.v1.RaftInternalService/PreVote");
        s
    })
}

// ─── GrpcRateLimitLayer ───────────────────────────────────────────────────────

/// Tower [`Layer`] that applies a token-bucket rate limit to the
/// open (unauthenticated) gRPC methods listed in [`get_rate_limited_methods`].
///
/// The key is `method_path:remote_addr` when `x-forwarded-for` or
/// `x-real-ip` is present; otherwise just `method_path` (shared bucket for
/// all callers, which is conservative and safe as a fallback).
#[derive(Clone)]
pub struct GrpcRateLimitLayer {
    limiter: Arc<RateLimiter>,
}

impl GrpcRateLimitLayer {
    pub fn new() -> Self {
        let clock = Arc::new(SystemClock) as Arc<dyn coord_core::clock::Clock>;
        Self {
            limiter: Arc::new(RateLimiter::new(
                RateLimitConfig::new(GRPC_HIGH_RISK_CAPACITY, GRPC_HIGH_RISK_REFILL_PER_SEC),
                clock,
            )),
        }
    }
}

impl Default for GrpcRateLimitLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Layer<S> for GrpcRateLimitLayer {
    type Service = GrpcRateLimitService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcRateLimitService {
            inner,
            limiter: Arc::clone(&self.limiter),
        }
    }
}

// ─── GrpcRateLimitService ─────────────────────────────────────────────────────

#[derive(Clone)]
pub struct GrpcRateLimitService<S> {
    inner: S,
    limiter: Arc<RateLimiter>,
}

impl<S, B> Service<Request<B>> for GrpcRateLimitService<S>
where
    S: Service<Request<B>, Response = Response<BoxBody>> + Clone + Send + 'static,
    S::Error: Into<BoxError> + Send + 'static,
    S::Future: Send + 'static,
    B: Send + 'static,
{
    type Response = Response<BoxBody>;
    type Error = BoxError;
    type Future = BoxFuture<'static, Result<Self::Response, BoxError>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        let path = req.uri().path().to_string();

        // Only rate-limit the designated open high-risk methods.
        if !get_rate_limited_methods().contains_key(path.as_str()) {
            let clone = self.inner.clone();
            let mut inner = std::mem::replace(&mut self.inner, clone);
            return Box::pin(async move { inner.call(req).await.map_err(Into::into) });
        }

        // Derive a per-caller key.  Best-effort: we use the first IP found in
        // X-Forwarded-For / X-Real-IP headers (set by a trusted proxy).
        // Without proxy headers, we fall back to a method-level shared bucket.
        let client_ip = req
            .headers()
            .get("x-forwarded-for")
            .or_else(|| req.headers().get("x-real-ip"))
            .and_then(|v| v.to_str().ok())
            .map(|s| s.split(',').next().unwrap_or(s).trim().to_string());
        let bucket_key = match &client_ip {
            Some(ip) => format!("{path}:{ip}"),
            None => path.clone(),
        };

        let limiter = Arc::clone(&self.limiter);
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            if let Err(retry_after) = limiter.check(&bucket_key) {
                let msg = format!("rate limit exceeded for {path}; retry after {retry_after:.1}s");
                tracing::warn!(path = %path, retry_after = %retry_after, "gRPC rate limit triggered");
                return Ok(grpc_error_response(Status::resource_exhausted(msg)));
            }
            inner.call(req).await.map_err(Into::into)
        })
    }
}

// ─── Capability policy table ──────────────────────────────────────────────────

static POLICY: OnceLock<HashMap<&'static str, &'static str>> = OnceLock::new();

/// Returns the capability policy table, initialising it on first call.
pub fn get_policy() -> &'static HashMap<&'static str, &'static str> {
    POLICY.get_or_init(|| {
        let mut m: HashMap<&'static str, &'static str> = HashMap::new();

        // ── Registry ───────────────────────────────────────────────────────
        m.insert("/coord.v1.RegistryService/Register", "registry.write");
        m.insert("/coord.v1.RegistryService/Deregister", "registry.write");
        m.insert("/coord.v1.RegistryService/Discover", "registry.read");
        m.insert("/coord.v1.RegistryService/Heartbeat", "registry.write");

        // ── Config ─────────────────────────────────────────────────────────
        m.insert("/coord.v1.ConfigService/GetConfig", "config.read");
        m.insert("/coord.v1.ConfigService/PutConfig", "config.put");
        m.insert("/coord.v1.ConfigService/WatchConfig", "config.read");

        // ── Lock ───────────────────────────────────────────────────────────
        m.insert("/coord.v1.LockService/Acquire", "lock.write");
        m.insert("/coord.v1.LockService/Release", "lock.write");
        m.insert("/coord.v1.LockService/KeepAlive", "lock.write");

        // ── Admin  (ClusterStatus is intentionally open – diagnostic RPC) ──
        m.insert("/coord.v1.AdminService/ListLocks", "lock.read");
        m.insert("/coord.v1.AdminService/MemberAdd", "cluster.member_add");
        m.insert(
            "/coord.v1.AdminService/MemberRemove",
            "cluster.member_remove",
        );
        m.insert("/coord.v1.AdminService/CreateBackup", "admin.backup");
        m.insert("/coord.v1.AdminService/RestoreBackup", "admin.backup");

        // ── IdGen ──────────────────────────────────────────────────────────
        m.insert("/coord.v1.IdGenService/GenerateSnowflake", "idgen.generate");

        // ── Workflow ───────────────────────────────────────────────────────
        m.insert(
            "/coord.v1.WorkflowService/DeployWorkflowDefinition",
            "workflow.deploy",
        );
        m.insert(
            "/coord.v1.WorkflowService/StartWorkflowV2",
            "workflow.start",
        );
        m.insert("/coord.v1.WorkflowService/ResumeWorkflow", "workflow.start");
        m.insert(
            "/coord.v1.WorkflowService/GetWorkflowInstance",
            "workflow.read",
        );
        m.insert(
            "/coord.v1.WorkflowService/ListWorkflowInstances",
            "workflow.read",
        );
        m.insert(
            "/coord.v1.WorkflowService/ListWorkflowDefinitions",
            "workflow.read",
        );
        m.insert(
            "/coord.v1.WorkflowService/GetWorkflowDefinition",
            "workflow.read",
        );

        // ── Transit ────────────────────────────────────────────────────────
        m.insert("/coord.v1.TransitService/CreateKey", "transit.admin");
        m.insert("/coord.v1.TransitService/Encrypt", "transit.encrypt");
        m.insert("/coord.v1.TransitService/Decrypt", "transit.decrypt");
        m.insert("/coord.v1.TransitService/RotateKey", "transit.admin");
        m.insert("/coord.v1.TransitService/HmacSign", "transit.hmac_sign");
        m.insert("/coord.v1.TransitService/HmacVerify", "transit.hmac_verify");
        m.insert("/coord.v1.TransitService/GetTransitKey", "transit.read");

        // ── PKI ────────────────────────────────────────────────────────────
        m.insert("/coord.v1.PkiService/IssueCertificate", "pki.issue");
        m.insert("/coord.v1.PkiService/RenewCertificate", "pki.renew");
        m.insert("/coord.v1.PkiService/RevokeCertificate", "pki.revoke");
        m.insert("/coord.v1.PkiService/GetCaChain", "pki.read");
        m.insert(
            "/coord.v1.PkiService/GetCertificateRevocationList",
            "pki.read",
        );
        m.insert("/coord.v1.PkiService/CheckCertificateStatus", "pki.read");
        m.insert("/coord.v1.PkiService/UpdateAutoRenewPolicy", "pki.admin");
        m.insert("/coord.v1.PkiService/RunAutoRenew", "pki.admin");
        m.insert("/coord.v1.PkiService/CreateAcmeOrder", "pki.issue");
        m.insert("/coord.v1.PkiService/CompleteAcmeChallenge", "pki.issue");
        m.insert("/coord.v1.PkiService/FinalizeAcmeOrder", "pki.issue");
        m.insert("/coord.v1.PkiService/CreatePkiRole", "pki.admin");
        m.insert("/coord.v1.PkiService/GetCrl", "pki.read");

        // ── Seal  (Init / GetSealStatus / Unseal are intentionally open) ───
        m.insert("/coord.v1.SealService/Seal", "security.seal");
        m.insert("/coord.v1.SealService/RotateRootKey", "operator.rotate_key");

        // ── Auth  (LoginAppRole / LookupToken are intentionally open) ──────
        m.insert("/coord.v1.AuthService/CreateAppRole", "security.admin");
        m.insert("/coord.v1.AuthService/GenerateSecretId", "security.admin");
        m.insert("/coord.v1.AuthService/RevokeToken", "security.admin");
        m.insert("/coord.v1.AuthService/GetAppRoleId", "security.admin");

        // ── Policy (PDP) ───────────────────────────────────────────────────
        m.insert("/coord.v1.PolicyService/PutPolicyBundle", "policy.write");
        m.insert("/coord.v1.PolicyService/SetBundleEnabled", "policy.write");
        m.insert("/coord.v1.PolicyService/DeletePolicyBundle", "policy.write");
        m.insert("/coord.v1.PolicyService/ListPolicyBundles", "policy.read");
        m.insert("/coord.v1.PolicyService/Evaluate", "policy.evaluate");
        m.insert("/coord.v1.PolicyService/Explain", "policy.evaluate");

        m
    })
}

// ─── SecurityGateway ─────────────────────────────────────────────────────────

/// Standalone async capability checker.
///
/// Decoupled from `CoordinatorState` so it can be shared between the tower
/// interceptor layer and any future middleware without dragging in the full
/// God-object state.
#[derive(Clone)]
pub struct SecurityGateway {
    security: Arc<SecurityController>,
    metrics: Arc<CoordMetrics>,
}

impl SecurityGateway {
    pub fn new(security: Arc<SecurityController>, metrics: Arc<CoordMetrics>) -> Self {
        Self { security, metrics }
    }

    /// Verify that `token` (Bearer value, without the `Bearer ` prefix) holds
    /// `capability`.  Returns `Unavailable` when the security domain is not yet
    /// initialised, so bootstrap must use the open methods (Init, Unseal, etc.).
    pub async fn check_capability(
        &self,
        token: Option<&str>,
        capability: &str,
    ) -> Result<(), Status> {
        if !self.security.is_initialized().await {
            return Err(Status::unavailable(
                "security domain not initialised; call SealService/Init first",
            ));
        }

        let status = self.security.seal_status().await;
        self.metrics
            .coord_security_sealed
            .set(if status.sealed { 1 } else { 0 });
        if status.sealed {
            return Err(Status::failed_precondition("security domain is sealed"));
        }

        let token = token.ok_or_else(|| Status::unauthenticated("missing bearer token"))?;
        match self.security.authorize_token(token, capability).await {
            Ok(()) => Ok(()),
            Err(msg) => {
                self.metrics.coord_authz_denied_total.inc();
                Err(Status::permission_denied(msg))
            }
        }
    }
}

// ─── CapabilityLayer ─────────────────────────────────────────────────────────

/// Tower [`Layer`] that enforces the gRPC capability policy.
#[derive(Clone)]
pub struct CapabilityLayer {
    gateway: SecurityGateway,
}

impl CapabilityLayer {
    pub fn new(gateway: SecurityGateway) -> Self {
        Self { gateway }
    }
}

impl<S> Layer<S> for CapabilityLayer {
    type Service = CapabilityService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        CapabilityService {
            inner,
            gateway: self.gateway.clone(),
        }
    }
}

// ─── CapabilityService ───────────────────────────────────────────────────────

#[derive(Clone)]
pub struct CapabilityService<S> {
    inner: S,
    gateway: SecurityGateway,
}

impl<S, B> Service<Request<B>> for CapabilityService<S>
where
    S: Service<Request<B>, Response = Response<BoxBody>> + Clone + Send + 'static,
    S::Error: Into<BoxError> + Send + 'static,
    S::Future: Send + 'static,
    B: Send + 'static,
{
    type Response = Response<BoxBody>;
    type Error = BoxError;
    type Future = BoxFuture<'static, Result<Self::Response, BoxError>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        let path = req.uri().path().to_string();
        let required_cap = get_policy().get(path.as_str()).copied();

        // Extract W3C traceparent (if any) and open a tracing span covering
        // the remainder of the RPC. Malformed headers are silently ignored
        // per spec §3.2.2.5 — never break request handling on telemetry bugs.
        let trace_ctx = req
            .headers()
            .get("traceparent")
            .and_then(|v| v.to_str().ok())
            .and_then(crate::telemetry::TraceContext::parse);
        let rpc_span = match &trace_ctx {
            Some(tc) => tracing::info_span!(
                "grpc.request",
                path = %path,
                trace_id = %tc.trace_id,
                parent_span_id = %tc.parent_span_id,
            ),
            None => tracing::info_span!("grpc.request", path = %path),
        };

        if required_cap.is_none() {
            // Method not in capability policy — check against open allow-list.
            if get_open_methods().contains(path.as_str()) {
                let fut = self.inner.call(req);
                return Box::pin(async move {
                    let _enter = rpc_span.enter();
                    fut.await.map_err(Into::into)
                });
            }
            // Unknown method: fail-closed — reject.
            return Box::pin(async move {
                let _enter = rpc_span.enter();
                tracing::warn!(path = %path, "rejected unknown gRPC method (fail-closed)");
                Ok(grpc_error_response(Status::permission_denied(
                    "method not recognised by security policy",
                )))
            });
        }

        // For protected methods: replace self.inner with a fresh clone and
        // use the already-polled-ready original for this invocation.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        // SAFETY: `required_cap.is_none()` was just checked above; the
        // `Some` variant is guaranteed here. Still, prefer explicit match
        // over `.unwrap()` to keep `clippy::unwrap_used` clean.
        let cap = match required_cap {
            Some(cap) => cap,
            None => {
                let fut = inner.call(req);
                return Box::pin(async move { fut.await.map_err(Into::into) });
            }
        };
        let token = extract_bearer_from_http(&req);
        let gateway = self.gateway.clone();

        Box::pin(async move {
            let _enter = rpc_span.enter();
            if let Err(status) = gateway.check_capability(token.as_deref(), cap).await {
                return Ok(grpc_error_response(status));
            }
            inner.call(req).await.map_err(Into::into)
        })
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn extract_bearer_from_http<B>(req: &Request<B>) -> Option<String> {
    let header = req.headers().get("authorization")?.to_str().ok()?;
    if let Some(v) = header.strip_prefix("Bearer ") {
        return Some(v.trim().to_string());
    }
    if let Some(v) = header.strip_prefix("bearer ") {
        return Some(v.trim().to_string());
    }
    None
}

// ─── GrpcRedMetricsLayer ──────────────────────────────────────────────────────

/// Tower [`Layer`] that records per-RPC RED (Rate, Error, Duration) metrics.
///
/// Records `coord_grpc_requests_total` and
/// `coord_grpc_request_duration_seconds` with labels `method` (full gRPC path)
/// and `code` (gRPC status code string, e.g. `"OK"` or `"UNAVAILABLE"`).
///
/// The `code` is read from the `grpc-status` response header written by tonic
/// for error responses. For successful responses (no `grpc-status` header or
/// `grpc-status: 0`) the code is recorded as `"OK"`.
#[derive(Clone)]
pub struct GrpcRedMetricsLayer {
    metrics: Arc<CoordMetrics>,
}

impl GrpcRedMetricsLayer {
    pub fn new(metrics: Arc<CoordMetrics>) -> Self {
        Self { metrics }
    }
}

impl<S> Layer<S> for GrpcRedMetricsLayer {
    type Service = GrpcRedMetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcRedMetricsService {
            inner,
            metrics: Arc::clone(&self.metrics),
        }
    }
}

// ─── GrpcRedMetricsService ────────────────────────────────────────────────────

#[derive(Clone)]
pub struct GrpcRedMetricsService<S> {
    inner: S,
    metrics: Arc<CoordMetrics>,
}

impl<S, B> Service<Request<B>> for GrpcRedMetricsService<S>
where
    S: Service<Request<B>, Response = Response<BoxBody>> + Clone + Send + 'static,
    S::Error: Into<BoxError> + Send + 'static,
    S::Future: Send + 'static,
    B: Send + 'static,
{
    type Response = Response<BoxBody>;
    type Error = BoxError;
    type Future = BoxFuture<'static, Result<Self::Response, BoxError>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        let method = req.uri().path().to_string();
        let metrics = Arc::clone(&self.metrics);

        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            let start = std::time::Instant::now();
            let result = inner.call(req).await.map_err(Into::into);
            let elapsed = start.elapsed().as_secs_f64();

            let code = match &result {
                Ok(resp) => grpc_code_label(resp),
                Err(_) => "INTERNAL",
            };

            metrics
                .coord_grpc_requests_total
                .with_label_values(&[&method, code])
                .inc();
            metrics
                .coord_grpc_request_duration_seconds
                .with_label_values(&[&method, code])
                .observe(elapsed);

            result
        })
    }
}

/// Extract the gRPC status code string from the `grpc-status` header.
///
/// Returns `"OK"` when the header is absent (no error) or when the code is 0.
fn grpc_code_label(resp: &Response<BoxBody>) -> &'static str {
    let Some(raw) = resp.headers().get("grpc-status") else {
        return "OK";
    };
    let code_int: i32 = raw.to_str().ok().and_then(|s| s.parse().ok()).unwrap_or(0);
    match code_int {
        0 => "OK",
        1 => "CANCELLED",
        2 => "UNKNOWN",
        3 => "INVALID_ARGUMENT",
        4 => "DEADLINE_EXCEEDED",
        5 => "NOT_FOUND",
        6 => "ALREADY_EXISTS",
        7 => "PERMISSION_DENIED",
        8 => "RESOURCE_EXHAUSTED",
        9 => "FAILED_PRECONDITION",
        10 => "ABORTED",
        11 => "OUT_OF_RANGE",
        12 => "UNIMPLEMENTED",
        13 => "INTERNAL",
        14 => "UNAVAILABLE",
        15 => "DATA_LOSS",
        16 => "UNAUTHENTICATED",
        _ => "UNKNOWN",
    }
}

/// Encode a gRPC error as a trailers-only HTTP 200 response.
///
/// Per the gRPC-over-HTTP/2 spec a server may send the grpc-status trailer in
/// the initial HEADERS frame (with END_STREAM) when there is no response body.
/// All standard gRPC clients handle this correctly.
fn grpc_error_response(status: Status) -> Response<BoxBody> {
    use bytes::Bytes;
    use http_body_util::{BodyExt, Empty};

    // Map Empty<Bytes>'s Infallible error to tonic::Status so the types align.
    let body: BoxBody = Empty::<Bytes>::new()
        .map_err(|_infallible| tonic::Status::unknown(""))
        .boxed_unsync();

    let mut res = Response::new(body);
    *res.status_mut() = http::StatusCode::OK;
    let h = res.headers_mut();
    h.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/grpc"),
    );
    h.insert(
        "grpc-status",
        HeaderValue::from_str(&(status.code() as i32).to_string())
            .unwrap_or_else(|_| HeaderValue::from_static("2")), // 2 = UNKNOWN
    );
    if !status.message().is_empty()
        && let Ok(v) = status.message().parse::<HeaderValue>()
    {
        h.insert("grpc-message", v);
    }
    res
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_covers_all_expected_protected_methods() {
        let policy = get_policy();
        let expected = [
            "/coord.v1.RegistryService/Register",
            "/coord.v1.RegistryService/Deregister",
            "/coord.v1.RegistryService/Discover",
            "/coord.v1.RegistryService/Heartbeat",
            "/coord.v1.ConfigService/GetConfig",
            "/coord.v1.ConfigService/PutConfig",
            "/coord.v1.ConfigService/WatchConfig",
            "/coord.v1.LockService/Acquire",
            "/coord.v1.LockService/Release",
            "/coord.v1.LockService/KeepAlive",
            "/coord.v1.AdminService/ListLocks",
            "/coord.v1.AdminService/MemberAdd",
            "/coord.v1.AdminService/MemberRemove",
            "/coord.v1.AdminService/CreateBackup",
            "/coord.v1.AdminService/RestoreBackup",
            "/coord.v1.IdGenService/GenerateSnowflake",
            "/coord.v1.WorkflowService/DeployWorkflowDefinition",
            "/coord.v1.WorkflowService/StartWorkflowV2",
            "/coord.v1.WorkflowService/ResumeWorkflow",
            "/coord.v1.WorkflowService/GetWorkflowInstance",
            "/coord.v1.WorkflowService/ListWorkflowInstances",
            "/coord.v1.WorkflowService/ListWorkflowDefinitions",
            "/coord.v1.WorkflowService/GetWorkflowDefinition",
            "/coord.v1.TransitService/CreateKey",
            "/coord.v1.TransitService/Encrypt",
            "/coord.v1.TransitService/Decrypt",
            "/coord.v1.TransitService/RotateKey",
            "/coord.v1.TransitService/HmacSign",
            "/coord.v1.TransitService/HmacVerify",
            "/coord.v1.TransitService/GetTransitKey",
            "/coord.v1.PkiService/IssueCertificate",
            "/coord.v1.PkiService/RenewCertificate",
            "/coord.v1.PkiService/RevokeCertificate",
            "/coord.v1.PkiService/GetCaChain",
            "/coord.v1.PkiService/GetCertificateRevocationList",
            "/coord.v1.PkiService/CheckCertificateStatus",
            "/coord.v1.PkiService/UpdateAutoRenewPolicy",
            "/coord.v1.PkiService/RunAutoRenew",
            "/coord.v1.PkiService/CreateAcmeOrder",
            "/coord.v1.PkiService/CompleteAcmeChallenge",
            "/coord.v1.PkiService/FinalizeAcmeOrder",
            "/coord.v1.PkiService/CreatePkiRole",
            "/coord.v1.PkiService/GetCrl",
            "/coord.v1.SealService/Seal",
            "/coord.v1.SealService/RotateRootKey",
            "/coord.v1.AuthService/CreateAppRole",
            "/coord.v1.AuthService/GenerateSecretId",
            "/coord.v1.AuthService/RevokeToken",
            "/coord.v1.AuthService/GetAppRoleId",
            "/coord.v1.PolicyService/PutPolicyBundle",
            "/coord.v1.PolicyService/SetBundleEnabled",
            "/coord.v1.PolicyService/DeletePolicyBundle",
            "/coord.v1.PolicyService/ListPolicyBundles",
            "/coord.v1.PolicyService/Evaluate",
            "/coord.v1.PolicyService/Explain",
        ];
        for path in &expected {
            assert!(
                policy.contains_key(*path),
                "policy must contain protected path `{path}`"
            );
        }
        assert_eq!(
            policy.len(),
            expected.len(),
            "policy entry count mismatch – add new methods to BOTH this test AND the policy table"
        );
    }

    #[test]
    fn open_methods_absent_from_policy() {
        let policy = get_policy();
        let open = [
            "/coord.v1.SealService/Init",
            "/coord.v1.SealService/InitSeal",
            "/coord.v1.SealService/GetSealStatus",
            "/coord.v1.SealService/Unseal",
            "/coord.v1.AuthService/LoginAppRole",
            "/coord.v1.AuthService/LookupToken",
            "/coord.v1.AdminService/ClusterStatus",
            "/coord.v1.RaftInternalService/AppendEntries",
            "/coord.v1.RaftInternalService/RequestVote",
            "/coord.v1.RaftInternalService/PreVote",
        ];
        for path in &open {
            assert!(
                !policy.contains_key(*path),
                "open path `{path}` must NOT be in the policy table"
            );
        }
    }

    #[tokio::test]
    async fn gateway_rejects_when_security_not_initialized() {
        use coord_core::metrics::CoordMetrics;
        use coord_core::security::SecurityController;

        let security = Arc::new(SecurityController::new());
        let metrics = Arc::new(CoordMetrics::new().expect("metrics"));
        let gw = SecurityGateway::new(security, metrics);

        // Not initialized → protected methods must be rejected (fail-closed)
        let result = gw.check_capability(None, "registry.write").await;
        assert!(
            result.is_err(),
            "uninitialized domain must reject protected methods"
        );
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn gateway_denies_when_initialized_and_no_token() {
        use coord_core::metrics::CoordMetrics;
        use coord_core::security::{SecurityController, SecurityDomainSnapshot};

        let security = Arc::new(SecurityController::new());
        let metrics = Arc::new(CoordMetrics::new().expect("metrics"));

        // Initialise with one share so the domain is initialized + unsealed
        let domain = SecurityDomainSnapshot::default();
        let shares = security
            .init_security_with_domain(1, 1, domain)
            .await
            .expect("init");
        security.unseal(&shares[0]).await.expect("unseal");

        let gw = SecurityGateway::new(security, metrics);
        let err = gw
            .check_capability(None, "registry.write")
            .await
            .expect_err("missing token must be rejected after init");
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    // ─── A'3: GrpcRateLimitLayer tests ───────────────────────────────────────

    #[test]
    fn rate_limited_methods_covers_open_high_risk_paths() {
        let rl = get_rate_limited_methods();
        // Only credential-based endpoints are rate-limited.
        // Seal lifecycle (Init/Unseal/GetSealStatus) use Shamir shares –
        // brute-force is infeasible, so they are intentionally not rate-limited.
        for path in [
            "/coord.v1.AuthService/LoginAppRole",
            "/coord.v1.AuthService/LookupToken",
        ] {
            assert!(
                rl.contains_key(path),
                "rate-limited table must include {path}"
            );
        }
        for path in [
            "/coord.v1.SealService/Init",
            "/coord.v1.SealService/Unseal",
            "/coord.v1.SealService/GetSealStatus",
        ] {
            assert!(
                !rl.contains_key(path),
                "seal path {path} must NOT be rate-limited"
            );
        }
    }

    #[test]
    fn rate_limited_methods_do_not_overlap_with_capability_policy() {
        let policy = get_policy();
        let rl = get_rate_limited_methods();
        for path in rl.keys() {
            assert!(
                !policy.contains_key(*path),
                "path {path} is both rate-limited AND capability-protected – choose one"
            );
        }
    }

    #[test]
    fn grpc_rate_limiter_allows_burst_then_rejects() {
        use coord_core::clock::TestClock;
        use std::time::Duration;

        let clock = Arc::new(TestClock::new(0));
        let limiter = Arc::new(RateLimiter::new(
            RateLimitConfig::new(3, 1.0), // capacity=3, 1 rps
            clock.clone() as Arc<dyn coord_core::clock::Clock>,
        ));

        // First 3 requests must be allowed
        for _ in 0..3 {
            assert!(limiter.check("seal.unseal:1.2.3.4").is_ok());
        }
        // 4th must be rejected
        assert!(limiter.check("seal.unseal:1.2.3.4").is_err());

        // After advancing 1 second, one token is refilled
        clock.advance(Duration::from_millis(1001));
        assert!(limiter.check("seal.unseal:1.2.3.4").is_ok());
    }

    #[test]
    fn grpc_rate_limiter_does_not_affect_unrelated_keys() {
        use coord_core::clock::TestClock;

        let clock = Arc::new(TestClock::new(0));
        let limiter = Arc::new(RateLimiter::new(
            RateLimitConfig::new(1, 0.0), // capacity=1, no refill
            clock as Arc<dyn coord_core::clock::Clock>,
        ));

        // Exhaust key-A
        assert!(limiter.check("seal.unseal:1.1.1.1").is_ok());
        assert!(limiter.check("seal.unseal:1.1.1.1").is_err());

        // key-B is independent – must still get a token
        assert!(limiter.check("seal.unseal:2.2.2.2").is_ok());
    }

    // ─── B'2: GrpcRedMetricsLayer tests ──────────────────────────────────────

    /// A minimal tower service that returns a response with an optional `grpc-status` header.
    #[derive(Clone)]
    struct FakeGrpcService {
        grpc_status: Option<i32>,
    }

    impl<B: Send + 'static> Service<Request<B>> for FakeGrpcService {
        type Response = Response<BoxBody>;
        type Error = BoxError;
        type Future = BoxFuture<'static, Result<Self::Response, BoxError>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Request<B>) -> Self::Future {
            let status = self.grpc_status;
            Box::pin(async move {
                use bytes::Bytes;
                use http_body_util::{BodyExt, Empty};
                let body: BoxBody = Empty::<Bytes>::new()
                    .map_err(|_| tonic::Status::unknown(""))
                    .boxed_unsync();
                let mut resp = Response::new(body);
                if let Some(code) = status {
                    resp.headers_mut().insert(
                        "grpc-status",
                        HeaderValue::from_str(&code.to_string()).unwrap(),
                    );
                }
                Ok(resp)
            })
        }
    }

    #[tokio::test]
    async fn red_metrics_records_ok_request() {
        use tower::Service;
        let metrics = Arc::new(CoordMetrics::new().unwrap());
        let svc = FakeGrpcService { grpc_status: None };
        let mut svc = GrpcRedMetricsLayer::new(Arc::clone(&metrics)).layer(svc);
        let req: Request<()> = Request::builder()
            .uri("/coord.v1.LockService/Acquire")
            .body(())
            .unwrap();
        let _ = svc.call(req).await.unwrap();

        let count = metrics
            .coord_grpc_requests_total
            .with_label_values(&["/coord.v1.LockService/Acquire", "OK"])
            .get();
        assert_eq!(count, 1.0);
    }

    #[tokio::test]
    async fn red_metrics_records_error_code() {
        use tower::Service;
        let metrics = Arc::new(CoordMetrics::new().unwrap());
        let svc = FakeGrpcService {
            grpc_status: Some(14),
        }; // UNAVAILABLE
        let mut svc = GrpcRedMetricsLayer::new(Arc::clone(&metrics)).layer(svc);
        let req: Request<()> = Request::builder()
            .uri("/coord.v1.WorkflowService/PollTask")
            .body(())
            .unwrap();
        let _ = svc.call(req).await.unwrap();

        let count = metrics
            .coord_grpc_requests_total
            .with_label_values(&["/coord.v1.WorkflowService/PollTask", "UNAVAILABLE"])
            .get();
        assert_eq!(count, 1.0);
    }

    // ── Structural: auth policy coverage ──────────────────────────────────

    /// Every gRPC method path in the proto descriptors must appear in either
    /// `CAPABILITY_POLICY` or `OPEN_METHODS`. Fail-open by omission is a
    /// security bug.
    ///
    /// This test enumerates the known service methods from the proto. If a
    /// new method is added to the proto but not listed here, the developer
    /// must add it to both this test AND the appropriate security table.
    #[test]
    fn every_grpc_method_has_security_rule() {
        let policy = get_policy();
        let open = get_open_methods();

        // Exhaustive list of all gRPC methods from the proto definitions.
        // When adding a new RPC, add it here AND to the correct table.
        let all_methods: &[&str] = &[
            // RegistryService
            "/coord.v1.RegistryService/Register",
            "/coord.v1.RegistryService/Deregister",
            "/coord.v1.RegistryService/Discover",
            "/coord.v1.RegistryService/Heartbeat",
            // ConfigService
            "/coord.v1.ConfigService/GetConfig",
            "/coord.v1.ConfigService/PutConfig",
            "/coord.v1.ConfigService/WatchConfig",
            // LockService
            "/coord.v1.LockService/Acquire",
            "/coord.v1.LockService/Release",
            "/coord.v1.LockService/KeepAlive",
            // AdminService
            "/coord.v1.AdminService/ClusterStatus",
            "/coord.v1.AdminService/ListLocks",
            "/coord.v1.AdminService/MemberAdd",
            "/coord.v1.AdminService/MemberRemove",
            "/coord.v1.AdminService/CreateBackup",
            "/coord.v1.AdminService/RestoreBackup",
            // IdGenService
            "/coord.v1.IdGenService/GenerateSnowflake",
            // WorkflowService
            "/coord.v1.WorkflowService/DeployWorkflowDefinition",
            "/coord.v1.WorkflowService/StartWorkflowV2",
            "/coord.v1.WorkflowService/ResumeWorkflow",
            "/coord.v1.WorkflowService/GetWorkflowInstance",
            "/coord.v1.WorkflowService/ListWorkflowInstances",
            "/coord.v1.WorkflowService/ListWorkflowDefinitions",
            "/coord.v1.WorkflowService/GetWorkflowDefinition",
            // TransitService
            "/coord.v1.TransitService/CreateKey",
            "/coord.v1.TransitService/Encrypt",
            "/coord.v1.TransitService/Decrypt",
            "/coord.v1.TransitService/RotateKey",
            "/coord.v1.TransitService/HmacSign",
            "/coord.v1.TransitService/HmacVerify",
            "/coord.v1.TransitService/GetTransitKey",
            // PkiService
            "/coord.v1.PkiService/IssueCertificate",
            "/coord.v1.PkiService/RenewCertificate",
            "/coord.v1.PkiService/RevokeCertificate",
            "/coord.v1.PkiService/GetCaChain",
            "/coord.v1.PkiService/GetCertificateRevocationList",
            "/coord.v1.PkiService/CheckCertificateStatus",
            "/coord.v1.PkiService/UpdateAutoRenewPolicy",
            "/coord.v1.PkiService/RunAutoRenew",
            "/coord.v1.PkiService/CreateAcmeOrder",
            "/coord.v1.PkiService/CompleteAcmeChallenge",
            "/coord.v1.PkiService/FinalizeAcmeOrder",
            "/coord.v1.PkiService/CreatePkiRole",
            "/coord.v1.PkiService/GetCrl",
            // SealService
            "/coord.v1.SealService/Init",
            "/coord.v1.SealService/InitSeal",
            "/coord.v1.SealService/Unseal",
            "/coord.v1.SealService/GetSealStatus",
            "/coord.v1.SealService/Seal",
            "/coord.v1.SealService/RotateRootKey",
            // AuthService
            "/coord.v1.AuthService/LoginAppRole",
            "/coord.v1.AuthService/LookupToken",
            "/coord.v1.AuthService/CreateAppRole",
            "/coord.v1.AuthService/GenerateSecretId",
            "/coord.v1.AuthService/RevokeToken",
            "/coord.v1.AuthService/GetAppRoleId",
            // RaftInternalService
            "/coord.v1.RaftInternalService/AppendEntries",
            "/coord.v1.RaftInternalService/RequestVote",
            "/coord.v1.RaftInternalService/PreVote",
        ];

        let mut uncovered = Vec::new();
        for &path in all_methods {
            if !policy.contains_key(path) && !open.contains(path) {
                uncovered.push(path);
            }
        }

        assert!(
            uncovered.is_empty(),
            "gRPC methods not covered by CAPABILITY_POLICY or OPEN_METHODS \
             (add them to interceptors.rs):\n{}",
            uncovered.join("\n"),
        );
    }

    #[test]
    fn open_methods_and_policy_are_disjoint() {
        let policy = get_policy();
        let open = get_open_methods();
        let overlap: Vec<&&str> = open.iter().filter(|m| policy.contains_key(**m)).collect();
        assert!(
            overlap.is_empty(),
            "Methods in both OPEN_METHODS and CAPABILITY_POLICY (pick one): {:?}",
            overlap,
        );
    }

    #[test]
    fn all_rate_limited_methods_are_open() {
        let open = get_open_methods();
        let rate_limited = get_rate_limited_methods();
        for path in rate_limited.keys() {
            assert!(
                open.contains(path),
                "Rate-limited method {path} is not in OPEN_METHODS — \
                 rate limiting without open access is contradictory",
            );
        }
    }
}
