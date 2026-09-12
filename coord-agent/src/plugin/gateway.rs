// coord-agent: 网关层拦截点（Phase 2.1 / 计划 §9.1）
//
// 位置：tower 链中 **auth 之后、router 之前**（`Metrics → Auth → Gateway → Router`），
// 因此只看到已通过鉴权的请求，并能读到 auth 层写入的身份扩展。
//
// 可见面（[`GatewayRequestCtx`]）：RPC path / HTTP method / headers / 身份 /
// body 前缀。**body 前缀仅在注册的钩子声明 `wants_body()` 时才缓冲**
// （默认零缓冲：热路径不触碰 body 字节流）。
//
// 延迟预算（§9.1）：观察模式 p99 增量目标 < 1%。
// - 无钩子且无观察路径 → 完全直通（`is_passthrough`，不计数、不分配）；
// - 仅观察路径 → 一次原子加 + 一次 HashMap 查（无分配）；
// - 有钩子 → 逐个 `before`（同步判定，拒绝即短路，fail-closed）。
//
// 注意：**代理透传的客户端流量也经此处**（网关层是全局观察/拒绝面）；
// 插件发起的协调调用另走调用面 typed 钩子（`plugin::hooks`，Phase 2.2），
// 以保护代理热路径（D6）。

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use http_body_util::BodyExt;
use parking_lot::RwLock;
use tonic::Status;
use tower::{Layer, Service};

/// 网关层可见的身份（由 auth 层写入请求扩展）。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GatewayIdentity {
    /// 主体（CCT `sub`）
    pub subject: String,
    /// 角色列表
    pub roles: Vec<String>,
}

/// 网关层请求上下文（钩子入参）。
#[derive(Debug, Clone, Default)]
pub struct GatewayRequestCtx {
    /// gRPC 方法路径（如 `/coord.kv.KV/Put`）
    pub path: String,
    /// HTTP 方法（gRPC 恒为 POST）
    pub method: String,
    /// 请求头（名小写）
    pub headers: Vec<(String, String)>,
    /// body 前缀（`None` = 未缓冲）
    pub body_prefix: Option<Vec<u8>>,
    /// 身份（鉴权关闭 / 匿名 → 默认值）
    pub identity: GatewayIdentity,
}

impl GatewayRequestCtx {
    /// 取请求头（名小写精确匹配）。
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    /// body 前缀长度（未缓冲 → 0）。
    pub fn body_len(&self) -> usize {
        self.body_prefix.as_ref().map(Vec::len).unwrap_or(0)
    }
}

/// 网关层钩子判定。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GatewayDecision {
    /// 放行
    Allow,
    /// 拒绝：映射为 gRPC 状态（默认 `PERMISSION_DENIED`）
    Deny {
        /// 拒绝原因（回传调用方）
        reason: String,
        /// 覆盖 gRPC code
        code: Option<tonic::Code>,
    },
}

impl GatewayDecision {
    /// 拒绝（`PERMISSION_DENIED`）。
    pub fn deny(reason: impl Into<String>) -> Self {
        GatewayDecision::Deny {
            reason: reason.into(),
            code: None,
        }
    }

    /// 按指定 code 拒绝。
    pub fn deny_with(code: tonic::Code, reason: impl Into<String>) -> Self {
        GatewayDecision::Deny {
            reason: reason.into(),
            code: Some(code),
        }
    }
}

/// 网关层钩子。
pub trait GatewayHook: Send + Sync + 'static {
    /// 钩子名（观测 / 同名覆盖）。
    fn name(&self) -> &str;

    /// 是否需要在 `before` 前缓冲 body（默认 false = 零缓冲热路径）。
    fn wants_body(&self) -> bool {
        false
    }

    /// 请求放行判定（router 之前）。
    fn before(&self, _ctx: &GatewayRequestCtx) -> GatewayDecision {
        GatewayDecision::Allow
    }

    /// 响应观察（状态码；不可修改响应）。
    fn after(&self, _ctx: &GatewayRequestCtx, _status: u16) {}
}

// ──── 网关注册表与指标 ────

/// 网关层注册表与指标。
#[derive(Default)]
pub struct PluginGateway {
    hooks: RwLock<Vec<Arc<dyn GatewayHook>>>,
    /// 逐路径计数（仅登记的观察路径；未登记路径不查表也不计数）
    path_counts: RwLock<HashMap<String, Arc<AtomicU64>>>,
    requests_total: AtomicU64,
    denied_total: AtomicU64,
}

impl std::fmt::Debug for PluginGateway {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginGateway")
            .field("hooks", &self.hooks.read().len())
            .field(
                "requests_total",
                &self.requests_total.load(Ordering::Relaxed),
            )
            .field("denied_total", &self.denied_total.load(Ordering::Relaxed))
            .finish()
    }
}

impl PluginGateway {
    /// 空网关（直通）。
    pub fn new() -> Self {
        Self::default()
    }

    /// 注册钩子（同名覆盖）。
    pub fn register_hook(&self, hook: Arc<dyn GatewayHook>) {
        let name = hook.name().to_string();
        let mut hooks = self.hooks.write();
        hooks.retain(|h| h.name() != name);
        hooks.push(hook);
        tracing::info!("plugin gateway hook registered: {name}");
    }

    /// 钩子数。
    pub fn hook_count(&self) -> usize {
        self.hooks.read().len()
    }

    /// 纯直通（无钩子且无观察路径）→ 中间件零开销。
    pub fn is_passthrough(&self) -> bool {
        self.hooks.read().is_empty() && self.path_counts.read().is_empty()
    }

    /// 登记观察路径（逐路径计数）。
    pub fn watch_path(&self, path: impl Into<String>) {
        let path = path.into();
        self.path_counts
            .write()
            .entry(path)
            .or_insert_with(|| Arc::new(AtomicU64::new(0)));
    }

    /// 总请求数（不含直通路径）。
    pub fn requests_total(&self) -> u64 {
        self.requests_total.load(Ordering::Relaxed)
    }

    /// 被网关拒绝数。
    pub fn denied_total(&self) -> u64 {
        self.denied_total.load(Ordering::Relaxed)
    }

    /// 指定观察路径计数（未登记 → `None`）。
    pub fn path_count(&self, path: &str) -> Option<u64> {
        self.path_counts
            .read()
            .get(path)
            .map(|c| c.load(Ordering::Relaxed))
    }

    /// 是否需要缓冲 body。
    fn needs_body(&self) -> bool {
        self.hooks.read().iter().any(|h| h.wants_body())
    }

    fn note_request(&self, path: &str) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
        if let Some(c) = self.path_counts.read().get(path) {
            c.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// 依次调用 `before`；任一钩子拒绝即短路（fail-closed）。
    pub fn evaluate_before(&self, ctx: &GatewayRequestCtx) -> GatewayDecision {
        let hooks = self.hooks.read();
        for hook in hooks.iter() {
            let decision = hook.before(ctx);
            if let GatewayDecision::Deny { .. } = decision {
                self.denied_total.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    "plugin gateway: hook '{}' denied {} (subject='{}')",
                    hook.name(),
                    ctx.path,
                    ctx.identity.subject
                );
                return decision;
            }
        }
        GatewayDecision::Allow
    }

    /// 响应观察（`after` 钩子；不修改响应）。
    fn note_after(&self, ctx: &GatewayRequestCtx, status: u16) {
        let hooks = self.hooks.read();
        for hook in hooks.iter() {
            hook.after(ctx, status);
        }
    }
}

// ──── 上下文构造 ────

/// 由请求 parts 构造上下文（`body` = 已缓冲前缀）。
fn ctx_from_parts(parts: &http::request::Parts, body: Option<Vec<u8>>) -> GatewayRequestCtx {
    let headers = parts
        .headers
        .iter()
        .map(|(k, v)| {
            (
                k.as_str().to_ascii_lowercase(),
                v.to_str().unwrap_or_default().to_string(),
            )
        })
        .collect();
    let identity = parts
        .extensions
        .get::<GatewayIdentity>()
        .cloned()
        .unwrap_or_default();
    GatewayRequestCtx {
        path: parts.uri.path().to_string(),
        method: parts.method.as_str().to_string(),
        headers,
        body_prefix: body,
        identity,
    }
}

/// 拒绝响应：gRPC 状态 → `http::Response`（`grpc-status` / `grpc-message` 头）。
fn status_response(code: Option<tonic::Code>, reason: String) -> http::Response<tonic::body::Body> {
    let status = Status::new(code.unwrap_or(tonic::Code::PermissionDenied), reason);
    let (parts, ()) = status.into_http::<()>().into_parts();
    http::Response::from_parts(parts, tonic::body::Body::empty())
}

// ──── Tower Layer / Service ────

/// 网关层 tower Layer（`Server::builder().layer(...)`）。
#[derive(Clone)]
pub struct PluginGatewayLayer {
    gateway: Arc<PluginGateway>,
}

impl PluginGatewayLayer {
    pub fn new(gateway: Arc<PluginGateway>) -> Self {
        Self { gateway }
    }
}

impl<S> Layer<S> for PluginGatewayLayer {
    type Service = PluginGatewayService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        PluginGatewayService {
            inner,
            gateway: Arc::clone(&self.gateway),
        }
    }
}

/// 包一层 inner 服务的插件拦截中间件。
#[derive(Clone)]
pub struct PluginGatewayService<S> {
    inner: S,
    gateway: Arc<PluginGateway>,
}

type BoxGatewayFuture<E> =
    Pin<Box<dyn Future<Output = Result<http::Response<tonic::body::Body>, E>> + Send>>;

impl<S, E> Service<http::Request<tonic::body::Body>> for PluginGatewayService<S>
where
    S: Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
            Error = E,
        > + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    E: Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = E;
    type Future = GatewayFuture<E>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<tonic::body::Body>) -> Self::Future {
        // 纯直通：不做任何额外工作
        if self.gateway.is_passthrough() {
            return GatewayFuture::Passthrough(Box::pin(self.inner.call(req)));
        }

        let gateway = Arc::clone(&self.gateway);
        let (parts, body) = req.into_parts();
        let ctx = ctx_from_parts(&parts, None);
        gateway.note_request(&ctx.path);

        // 仅观察：不进入判定路径
        if gateway.hook_count() == 0 {
            let fut = self.inner.call(http::Request::from_parts(parts, body));
            return GatewayFuture::Observed {
                inner: Box::pin(fut),
                gateway,
                ctx,
            };
        }

        // 有钩子且需要 body：先缓冲再判定（显式 opt-in，热路径默认不启用）
        if gateway.needs_body() {
            let mut inner = self.inner.clone();
            let gw = Arc::clone(&gateway);
            return GatewayFuture::Buffering(Box::pin(async move {
                let collected = match body.collect().await {
                    Ok(c) => c.to_bytes(),
                    Err(e) => {
                        tracing::warn!("plugin gateway: failed to buffer request body: {e}");
                        return Ok(status_response(
                            Some(tonic::Code::Internal),
                            format!("plugin gateway could not buffer request body: {e}"),
                        ));
                    }
                };
                let bytes = collected.to_vec();
                let ctx = ctx_from_parts(&parts, Some(bytes));
                let request = http::Request::from_parts(
                    parts,
                    tonic::body::Body::new(http_body_util::Full::new(collected)),
                );
                // body 前缀已在 ctx 中（identity 已从 parts 读出）
                match gw.evaluate_before(&ctx) {
                    GatewayDecision::Allow => match inner.call(request).await {
                        Ok(resp) => {
                            gw.note_after(&ctx, resp.status().as_u16());
                            Ok(resp)
                        }
                        Err(e) => Err(e),
                    },
                    GatewayDecision::Deny { reason, code } => Ok(status_response(code, reason)),
                }
            }));
        }

        // 判定（同步）
        match gateway.evaluate_before(&ctx) {
            GatewayDecision::Allow => {
                let fut = self.inner.call(http::Request::from_parts(parts, body));
                GatewayFuture::Guarded {
                    inner: Box::pin(fut),
                    gateway,
                    ctx,
                }
            }
            GatewayDecision::Deny { reason, code } => {
                GatewayFuture::Deny(Some(status_response(code, reason)))
            }
        }
    }
}

/// 判定需要一个可变 ctx（body 分支），此处仅为可读性保留别名。
fn _assert_mut(_: &mut GatewayRequestCtx) {}

/// 网关 future。
pub enum GatewayFuture<E> {
    /// 纯直通
    Passthrough(BoxGatewayFuture<E>),
    /// 仅观察计数
    Observed {
        inner: BoxGatewayFuture<E>,
        gateway: Arc<PluginGateway>,
        ctx: GatewayRequestCtx,
    },
    /// 已判定放行
    Guarded {
        inner: BoxGatewayFuture<E>,
        gateway: Arc<PluginGateway>,
        ctx: GatewayRequestCtx,
    },
    /// 立即拒绝
    Deny(Option<http::Response<tonic::body::Body>>),
    /// 先缓冲 body 再判定
    Buffering(BoxGatewayFuture<E>),
}

impl<E> Future for GatewayFuture<E> {
    type Output = Result<http::Response<tonic::body::Body>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: 不移动任何字段；Pin<Box<..>> 分支按可变引用 poll，其余按值取用
        let this = unsafe { self.get_unchecked_mut() };
        match this {
            GatewayFuture::Passthrough(fut) => fut.as_mut().poll(cx),
            GatewayFuture::Buffering(fut) => fut.as_mut().poll(cx),
            GatewayFuture::Observed {
                inner,
                gateway,
                ctx,
            } => match inner.as_mut().poll(cx) {
                Poll::Ready(Ok(resp)) => {
                    gateway.note_after(ctx, resp.status().as_u16());
                    Poll::Ready(Ok(resp))
                }
                other => other,
            },
            GatewayFuture::Guarded {
                inner,
                gateway,
                ctx,
            } => match inner.as_mut().poll(cx) {
                Poll::Ready(Ok(resp)) => {
                    gateway.note_after(ctx, resp.status().as_u16());
                    Poll::Ready(Ok(resp))
                }
                other => other,
            },
            GatewayFuture::Deny(resp) => match resp.take() {
                Some(resp) => Poll::Ready(Ok(resp)),
                // 契约外重复 poll：保持 Pending 而非 panic
                None => Poll::Pending,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;
    use std::sync::Mutex;

    /// 记录被调用路径的 stub inner 服务。
    #[derive(Clone, Default)]
    struct StubInner {
        seen: Arc<Mutex<Vec<String>>>,
    }

    impl Service<http::Request<tonic::body::Body>> for StubInner {
        type Response = http::Response<tonic::body::Body>;
        type Error = Infallible;
        type Future = BoxGatewayFuture<Infallible>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: http::Request<tonic::body::Body>) -> Self::Future {
            let seen = Arc::clone(&self.seen);
            Box::pin(async move {
                seen.lock()
                    .expect("lock")
                    .push(req.uri().path().to_string());
                Ok(http::Response::builder()
                    .status(200)
                    .body(tonic::body::Body::empty())
                    .expect("response"))
            })
        }
    }

    fn request(path: &str) -> http::Request<tonic::body::Body> {
        http::Request::builder()
            .method("POST")
            .uri(path)
            .body(tonic::body::Body::empty())
            .expect("request")
    }

    struct DenyWrites {
        touched: Arc<Mutex<Vec<String>>>,
    }

    impl GatewayHook for DenyWrites {
        fn name(&self) -> &str {
            "deny-writes"
        }
        fn before(&self, ctx: &GatewayRequestCtx) -> GatewayDecision {
            self.touched.lock().expect("lock").push(ctx.path.clone());
            if ctx.path.ends_with("/Put") {
                GatewayDecision::deny("writes are blocked by plugin policy")
            } else {
                GatewayDecision::Allow
            }
        }
        fn after(&self, ctx: &GatewayRequestCtx, status: u16) {
            self.touched
                .lock()
                .expect("lock")
                .push(format!("after:{}:{status}", ctx.path));
        }
    }

    /// 声明需要 body 的钩子。
    struct BodyTap {
        seen_len: Arc<AtomicU64>,
    }

    impl GatewayHook for BodyTap {
        fn name(&self) -> &str {
            "body-tap"
        }
        fn wants_body(&self) -> bool {
            true
        }
        fn before(&self, ctx: &GatewayRequestCtx) -> GatewayDecision {
            self.seen_len
                .fetch_add(ctx.body_len() as u64, Ordering::Relaxed);
            GatewayDecision::Allow
        }
    }

    fn service(gateway: Arc<PluginGateway>, inner: StubInner) -> PluginGatewayService<StubInner> {
        PluginGatewayLayer::new(gateway).layer(inner)
    }

    #[tokio::test]
    async fn passthrough_when_no_hooks_and_no_watched_paths() {
        let gateway = Arc::new(PluginGateway::new());
        assert!(gateway.is_passthrough());
        let inner = StubInner::default();
        let seen = Arc::clone(&inner.seen);
        let mut svc = service(Arc::clone(&gateway), inner);

        let resp = svc.call(request("/coord.kv.KV/Put")).await.expect("call");
        assert_eq!(resp.status().as_u16(), 200);
        assert_eq!(seen.lock().expect("lock").as_slice(), ["/coord.kv.KV/Put"]);
        // 直通路径不计数（零开销）
        assert_eq!(gateway.requests_total(), 0);
    }

    #[tokio::test]
    async fn hook_denies_write_and_allows_read() {
        let gateway = Arc::new(PluginGateway::new());
        let touched = Arc::new(Mutex::new(Vec::new()));
        gateway.register_hook(Arc::new(DenyWrites {
            touched: Arc::clone(&touched),
        }));

        let inner = StubInner::default();
        let seen = Arc::clone(&inner.seen);
        let mut svc = service(Arc::clone(&gateway), inner);

        // 读放行 → 到达 inner，并记录 after
        let resp = svc.call(request("/coord.kv.KV/Range")).await.expect("call");
        assert_eq!(resp.status().as_u16(), 200);

        // 写拒绝 → 未到达 inner，返回 PERMISSION_DENIED(7)
        let resp = svc.call(request("/coord.kv.KV/Put")).await.expect("call");
        assert_eq!(
            resp.headers()
                .get("grpc-status")
                .and_then(|v| v.to_str().ok()),
            Some("7"),
            "deny must surface gRPC PERMISSION_DENIED"
        );

        assert_eq!(
            seen.lock().expect("lock").as_slice(),
            ["/coord.kv.KV/Range"]
        );
        assert_eq!(gateway.requests_total(), 2);
        assert_eq!(gateway.denied_total(), 1);
        let log = touched.lock().expect("lock").clone();
        assert!(log.contains(&"/coord.kv.KV/Range".to_string()), "{log:?}");
        assert!(
            log.contains(&"after:/coord.kv.KV/Range:200".to_string()),
            "{log:?}"
        );
    }

    #[tokio::test]
    async fn watched_path_counting_works_without_hooks() {
        let gateway = Arc::new(PluginGateway::new());
        gateway.watch_path("/coord.kv.KV/Range");
        assert!(!gateway.is_passthrough());
        let mut svc = service(Arc::clone(&gateway), StubInner::default());

        svc.call(request("/coord.kv.KV/Range")).await.expect("call");
        svc.call(request("/coord.kv.KV/Put")).await.expect("call");

        assert_eq!(gateway.requests_total(), 2);
        assert_eq!(gateway.path_count("/coord.kv.KV/Range"), Some(1));
        assert_eq!(gateway.path_count("/coord.kv.KV/Put"), None);
    }

    #[tokio::test]
    async fn body_tap_buffers_and_forwards_intact() {
        let gateway = Arc::new(PluginGateway::new());
        let seen_len = Arc::new(AtomicU64::new(0));
        gateway.register_hook(Arc::new(BodyTap {
            seen_len: Arc::clone(&seen_len),
        }));

        let inner = StubInner::default();
        let seen = Arc::clone(&inner.seen);
        let mut svc = service(Arc::clone(&gateway), inner);

        let resp = svc
            .call(
                http::Request::builder()
                    .method("POST")
                    .uri("/coord.kv.KV/Put")
                    .body(tonic::body::Body::new(http_body_util::Full::new(
                        bytes::Bytes::from_static(b"12345"),
                    )))
                    .expect("request"),
            )
            .await
            .expect("call");

        assert_eq!(resp.status().as_u16(), 200);
        assert_eq!(
            seen_len.load(Ordering::Relaxed),
            5,
            "body-tap hook must observe the body"
        );
        assert_eq!(seen.lock().expect("lock").as_slice(), ["/coord.kv.KV/Put"]);
    }

    #[test]
    fn identity_extension_is_visible_to_hooks() {
        let mut req = request("/coord.kv.KV/Range");
        req.extensions_mut().insert(GatewayIdentity {
            subject: "plugin/counter".into(),
            roles: vec!["plugin/counter-role".into()],
        });
        let (parts, body) = req.into_parts();
        let ctx = ctx_from_parts(&parts, None);
        assert_eq!(ctx.identity.subject, "plugin/counter");
        assert_eq!(ctx.identity.roles, ["plugin/counter-role"]);
        assert_eq!(ctx.body_len(), 0);
        assert_eq!(ctx.method, "POST");
        // 无 body 缓冲时 ctx 不携带字节
        assert!(ctx.body_prefix.is_none());
        drop(body);
    }
}
