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
pub fn scoped_request_token<F>(token: Option<String>, fut: F) -> TaskLocalFuture<Option<String>, F>
where
    F: Future,
{
    REQUEST_TOKEN.scope(token, fut)
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
}
