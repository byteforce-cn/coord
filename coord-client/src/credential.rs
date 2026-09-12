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
use std::sync::Arc;

use parking_lot::RwLock;
use tonic::metadata::MetadataValue;
use tonic::service::interceptor::InterceptedService;
use tonic::service::Interceptor;
use tonic::transport::Channel;

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
