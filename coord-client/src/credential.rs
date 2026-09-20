// coord-client: 出站凭据注入（CCT / Bearer Token）
//
// coord-server 启用鉴权后，所有 coord-client 出站请求都必须携带 CCT。
// 本模块提供：
// - `TokenProvider`：可插拔的凭据来源抽象（持有缓存 token，外部刷新循环写入）；
// - `CachedTokenProvider`：内存持有 + 可更新（供 agent 自动续期循环使用）；
// - `CredentialInterceptor`：tonic 出站拦截器，为每个请求盖上 `authorization` metadata；
// - `AuthedChannel`：`Channel` + 拦截器 的类型别名（连接池对外统一返回该类型）。
//
// 凭据缺失（`current_token() == None`）时拦截器为 no-op，保持明文开发模式零破坏。

use std::fmt;
use std::future::Future;
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::task::futures::TaskLocalFuture;
use tonic::metadata::MetadataValue;
use tonic::service::interceptor::InterceptedService;
use tonic::service::Interceptor;
use tonic::transport::Channel;

tokio::task_local! {
    /// 当前**请求作用域**内要附加的凭据（"按请求凭据"）。
    ///
    /// 由 [`scoped_request_token`] 建立作用域；未建立作用域时读取返回 `None`
    /// （等价于 [`NoopTokenProvider`]，即不附加 `authorization` 头）。
    ///
    /// 动机（第四轮 §3.2）：agent 作为代理必须把**调用方自己的凭据**转发给服务端，
    /// 而此前 `coord-client` 只支持构造期 `with_token_provider`，结构上做不到
    /// "按请求补票"，导致生产默认配置（`auth_enabled = true`）下经 agent 的调用
    /// 一律被服务端以 `missing CCT token` 拒绝。
    static REQUEST_TOKEN: Option<String>;

    /// 当前是否处于**代理转发上下文**（出站调用是"替某个入站请求"发的）。
    ///
    /// 由 [`scoped_proxied_request`] 置位。存在意义（F-50 的**安全前提**）：
    /// 回退凭据（agent 自身身份）只允许用于 agent **自己发起**的调用；在代理
    /// 上下文里，调用方没带凭据就必须以"无凭据"上报（服务端 fail-closed），
    /// 而**不能**拿 agent 身份顶替 —— 否则会形成 confused deputy：入站鉴权关闭
    /// 但服务端开启鉴权的部署里，任何匿名调用方都能借用 agent 身份操作内部键空间。
    static PROXY_CONTEXT: bool;
}

/// 出站凭据提供者。
///
/// 实现方需保证 `current_token` 为**非阻塞**读取（通常读一个内存缓存）；
/// 实际续期由后台任务完成并写入缓存，避免在请求热路径上做网络调用。
pub trait TokenProvider: Send + Sync + fmt::Debug {
    /// 当前有效凭据（不含 `Bearer ` 前缀）。`None` 表示此请求不附加鉴权头。
    fn current_token(&self) -> Option<String>;
}

/// 无凭据提供者（明文开发模式 / 未启用鉴权）。
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopTokenProvider;

impl TokenProvider for NoopTokenProvider {
    fn current_token(&self) -> Option<String> {
        None
    }
}

/// 内存凭据持有者：读取廉价（读锁），由外部刷新循环调用 `set` / `clear` 更新。
#[derive(Debug, Clone, Default)]
pub struct CachedTokenProvider {
    token: Arc<RwLock<Option<String>>>,
}

impl CachedTokenProvider {
    /// 以初始凭据创建。
    pub fn new(token: Option<String>) -> Self {
        Self {
            token: Arc::new(RwLock::new(token)),
        }
    }

    /// 写入（或覆盖）当前凭据。
    pub fn set(&self, token: impl Into<String>) {
        *self.token.write() = Some(token.into());
    }

    /// 清除当前凭据（续期失败 / 登出）。
    pub fn clear(&self) {
        *self.token.write() = None;
    }

    /// 当前是否有凭据。
    pub fn is_set(&self) -> bool {
        self.token.read().is_some()
    }
}

impl TokenProvider for CachedTokenProvider {
    fn current_token(&self) -> Option<String> {
        self.token.read().clone()
    }
}

/// **按请求**凭据提供者：凭据来自任务局部量（见 [`scoped_request_token`]）。
///
/// 用途：作为"代理转发调用方凭据"的机制。agent 在放行一个入站请求后，把该请求的
/// CCT 放进任务作用域，于是**该请求触发的全部出站调用**都会带上调用方凭据。
///
/// 反过来说：不在作用域内发起的调用（agent 自身的后台任务）不会带任何凭据，
/// 与传入 `None` 的 [`NoopTokenProvider`] 行为一致。
#[derive(Debug, Default, Clone, Copy)]
pub struct RequestScopedTokenProvider;

impl TokenProvider for RequestScopedTokenProvider {
    fn current_token(&self) -> Option<String> {
        current_request_token()
    }
}

/// 当前请求作用域内的凭据（未建立作用域时为 `None`）。
pub fn current_request_token() -> Option<String> {
    REQUEST_TOKEN.try_with(|t| t.clone()).ok().flatten()
}

/// **两级**凭据提供者：先取 `primary`，为空时回退 `secondary`。
///
/// 用途（F-50 修复，2026-09-19）：agent 的出站客户端既要转发**调用方**凭据
/// （代理数据面，见 [`RequestScopedTokenProvider`]），又要在**没有调用方**时用
/// **agent 自己的服务账户**凭据访问服务端（后台保活 / 目录加载 / 订阅 / nodeid 注册）。
///
/// 此前只装了前者 ⇒ agent 自发流量一律 `missing CCT token`，锁自动续期、
/// registry 目录加载与订阅、idgen nodeid 注册全部失效（`jepsen/docs/coord-findings.md`
/// 的 F-50）。修复方式是**不动凭据注入路径**，只把"最终解析不到的凭据"换成
/// agent 自身身份。
///
/// 回退的**前提**（两条都是安全属性，不可省）：
/// 1. `primary` 恒优先 ⇒ 调用方凭据在场时逐字不变；
/// 2. 处于**代理上下文**时**绝不回退**（[`in_proxy_context`]）⇒ 入站请求没带凭据
///    就必须以"无凭据"上报，由服务端 fail-closed 拒绝，不会借用 agent 身份执行
///    （否则在"入站鉴权关、服务端鉴权开"的部署里构成 confused deputy）。
#[derive(Debug)]
pub struct FallbackTokenProvider {
    primary: Arc<dyn TokenProvider>,
    secondary: Arc<dyn TokenProvider>,
}

impl FallbackTokenProvider {
    /// `primary` 优先，缺失时用 `secondary`（仅在**非**代理上下文）。
    pub fn new(primary: Arc<dyn TokenProvider>, secondary: Arc<dyn TokenProvider>) -> Self {
        Self { primary, secondary }
    }
}

impl TokenProvider for FallbackTokenProvider {
    fn current_token(&self) -> Option<String> {
        if let Some(token) = self.primary.current_token() {
            return Some(token);
        }
        if in_proxy_context() {
            // 代理转发但调用方无凭据：不得使用 agent 自身身份（见类型文档第 2 条）。
            return None;
        }
        self.secondary.current_token()
    }
}

/// 在 `fut` 执行期间，为**所有**出站请求附加 `token`（按请求凭据）。
///
/// 典型用法（agent 代理层）：
///
/// ```ignore
/// let token = inbound_authorization_header();
/// scoped_request_token(token, inner_service.call(req)).await
/// ```
///
/// 作用域是**任务局部**的：`fut` 内部 `await` 的任意深度都会观察到它，
/// 不受 future 在线程间迁移的影响。
///
/// 注意：**不**置位代理上下文标记。要表达"这是替入站请求发的出站调用"，
/// 用 [`scoped_proxied_request`]（agent 代理层应使用后者）。
pub fn scoped_request_token<F>(token: Option<String>, fut: F) -> TaskLocalFuture<Option<String>, F>
where
    F: Future,
{
    REQUEST_TOKEN.scope(token, fut)
}

/// 当前是否处于代理转发上下文（见 [`PROXY_CONTEXT`]）。
pub fn in_proxy_context() -> bool {
    PROXY_CONTEXT.try_with(|v| *v).unwrap_or(false)
}

/// 代理转发作用域：既附加调用方凭据，又**标记**该出站调用属于代理上下文。
///
/// agent 入站鉴权中间件必须用本函数（而不是 [`scoped_request_token`]）包裹对
/// 内层服务的调用 —— 回退凭据（agent 自身身份）据此判断"这是替调用方发的请求，
/// 调用方没凭据就不能用 agent 身份顶上"（F-50 的安全前提，见
/// [`FallbackTokenProvider`]）。
pub fn scoped_proxied_request<F>(
    token: Option<String>,
    fut: F,
) -> TaskLocalFuture<bool, TaskLocalFuture<Option<String>, F>>
where
    F: Future,
{
    PROXY_CONTEXT.scope(true, REQUEST_TOKEN.scope(token, fut))
}

/// tonic 出站拦截器：为每个请求盖上 `authorization: Bearer <token>`。
#[derive(Clone)]
pub struct CredentialInterceptor {
    provider: Arc<dyn TokenProvider>,
}

impl CredentialInterceptor {
    /// 由凭据提供者构建。
    pub fn new(provider: Arc<dyn TokenProvider>) -> Self {
        Self { provider }
    }
}

impl fmt::Debug for CredentialInterceptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CredentialInterceptor")
            .field("has_token", &self.provider.current_token().is_some())
            .finish()
    }
}

impl Interceptor for CredentialInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        if let Some(token) = self.provider.current_token() {
            let header = format!("Bearer {token}");
            // 非 ASCII/非法 metadata 值属配置错误：fail-closed，显式报错而非静默跳过。
            let value = MetadataValue::try_from(header).map_err(|_| {
                tonic::Status::internal("credential contains invalid metadata characters")
            })?;
            request.metadata_mut().insert("authorization", value);
        }
        Ok(request)
    }
}

/// 已注入凭据的 gRPC 通道：`Channel` + `CredentialInterceptor`。
///
/// 所有 tonic 生成的客户端均为 `Client<T: GrpcService>` 泛型，
/// 因此把连接池的通道类型从 `Channel` 换成 `AuthedChannel` 后，
/// 既有 `Stub::new(channel)` 调用点**无需改动**即可自动携带凭据。
pub type AuthedChannel = InterceptedService<Channel, CredentialInterceptor>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cached_provider_set_clear() {
        let p = CachedTokenProvider::new(None);
        assert_eq!(p.current_token(), None);
        p.set("abc");
        assert_eq!(p.current_token().as_deref(), Some("abc"));
        assert!(p.is_set());
        p.clear();
        assert_eq!(p.current_token(), None);
    }

    #[test]
    fn interceptor_noop_without_token() {
        let mut interceptor = CredentialInterceptor::new(Arc::new(NoopTokenProvider));
        let req = interceptor.call(tonic::Request::new(())).unwrap();
        assert!(req.metadata().get("authorization").is_none());
    }

    #[test]
    fn interceptor_injects_bearer_token() {
        let provider = Arc::new(CachedTokenProvider::new(Some("cct-value".into())));
        let mut interceptor = CredentialInterceptor::new(provider);
        let req = interceptor.call(tonic::Request::new(())).unwrap();
        assert_eq!(
            req.metadata()
                .get("authorization")
                .and_then(|v| v.to_str().ok()),
            Some("Bearer cct-value")
        );
    }

    // ──── FallbackTokenProvider（F-50 修复）────

    /// 调用方凭据在场时**恒优先**（安全属性：agent 身份不得顶替调用方身份）。
    #[test]
    fn fallback_prefers_primary() {
        let primary = Arc::new(CachedTokenProvider::new(Some("caller".into())));
        let secondary = Arc::new(CachedTokenProvider::new(Some("agent".into())));
        let p = FallbackTokenProvider::new(primary, secondary);
        assert_eq!(p.current_token().as_deref(), Some("caller"));
    }

    /// 无调用方凭据（agent 自发流量）→ 回退 agent 自身身份。
    #[test]
    fn fallback_uses_secondary_when_primary_empty() {
        let primary = Arc::new(CachedTokenProvider::new(None));
        let secondary = Arc::new(CachedTokenProvider::new(Some("agent".into())));
        let p = FallbackTokenProvider::new(primary, secondary);
        assert_eq!(p.current_token().as_deref(), Some("agent"));
    }

    /// 两级都空 → 不附加鉴权头（明文开发模式零破坏）。
    #[test]
    fn fallback_none_when_both_empty() {
        let p = FallbackTokenProvider::new(
            Arc::new(NoopTokenProvider),
            Arc::new(CachedTokenProvider::new(None)),
        );
        assert_eq!(p.current_token(), None);
    }

    /// 任务作用域里的调用方凭据压过 agent 身份（按请求生效，非构造期快照）。
    #[tokio::test]
    async fn fallback_secondary_is_used_only_outside_request_scope() {
        use std::sync::Arc as StdArc;
        let agent_identity = StdArc::new(CachedTokenProvider::new(Some("agent".into())));
        let p = StdArc::new(FallbackTokenProvider::new(
            Arc::new(RequestScopedTokenProvider),
            StdArc::clone(&agent_identity) as Arc<dyn TokenProvider>,
        ));

        // 作用域外：agent 自发流量 → agent 身份
        assert_eq!(p.current_token().as_deref(), Some("agent"));

        // 作用域内：转发调用方凭据 → 调用方身份
        let inner = StdArc::clone(&p);
        let observed =
            scoped_request_token(Some("caller".into()), async move { inner.current_token() }).await;
        assert_eq!(observed.as_deref(), Some("caller"));

        // 作用域退出后回到 agent 身份
        assert_eq!(p.current_token().as_deref(), Some("agent"));
    }

    /// **confused deputy 卡口**：代理上下文内调用方无凭据时**不得**回退到 agent
    /// 身份 —— 必须以"无凭据"上报，由服务端 fail-closed 拒绝。
    #[tokio::test]
    async fn fallback_must_not_apply_inside_proxy_context() {
        use std::sync::Arc as StdArc;
        let agent_identity = StdArc::new(CachedTokenProvider::new(Some("agent".into())));
        let p = StdArc::new(FallbackTokenProvider::new(
            Arc::new(RequestScopedTokenProvider),
            StdArc::clone(&agent_identity) as Arc<dyn TokenProvider>,
        ));

        // 代理上下文 + 无调用方凭据 → None（而不是 "agent"）
        let inner = StdArc::clone(&p);
        let observed = scoped_proxied_request(None, async move { inner.current_token() }).await;
        assert_eq!(
            observed, None,
            "代理上下文内不得用 agent 自身身份顶替缺失的调用方凭据"
        );

        // 代理上下文 + 有调用方凭据 → 调用方身份
        let inner = StdArc::clone(&p);
        let observed =
            scoped_proxied_request(Some("caller".into()), async move { inner.current_token() })
                .await;
        assert_eq!(observed.as_deref(), Some("caller"));

        // 上下文退出后仍回到 agent 身份（后台任务不受影响）
        assert_eq!(p.current_token().as_deref(), Some("agent"));
    }

    /// 代理上下文标记可被单独观测（供 `new_request` 等调用点自检）。
    #[tokio::test]
    async fn proxy_context_flag_is_scoped() {
        assert!(!in_proxy_context());
        let observed = scoped_proxied_request(None, async { in_proxy_context() }).await;
        assert!(observed);
        assert!(!in_proxy_context());
    }
}
