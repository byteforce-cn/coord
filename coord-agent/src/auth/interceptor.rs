// Auth Interceptor — Agent-side CCT validation interceptor
//
// Validates all incoming gRPC requests from client applications:
// 1. Extract CCT from Authorization header
// 2. Verify signature (with LRU cache)
// 3. Check expiration (with clock drift tolerance)
// 4. Check revocation (bloom filter + fallback lookup)
// 5. Resolve roles → query local role cache → match capabilities + scope

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use lru::LruCache;
use parking_lot::RwLock;
use tonic::Status;
use tower::{Layer, Service};

use coord_core::auth::cct::{decode_cct_any, is_expired, CctHeader, CctPayload, CctToken};
use coord_core::auth::trie::ScopeTrie;
use coord_core::grpc_auth::{ScopeAccess, MAX_SCOPE_BODY_BYTES};
use http_body_util::BodyExt;
use tower::ServiceExt;

use super::role_cache::RoleCache;

// ──── Auth Result ────

/// Result of auth verification
#[derive(Debug, Clone, PartialEq, Eq)]
/// 鉴权结果（Allow 载荷大；Box 会改变全调用点匹配模式，接受大小差异）
#[allow(clippy::large_enum_variant)]
pub enum AuthResult {
    /// Authentication and authorization passed
    Allow(CctToken),
    /// Authentication or authorization failed
    Deny(String),
}

// ──── Signature Cache ────

/// LRU cache for CCT signature verification results.
/// Cache key: (jti, exp_hash) — avoids re-verifying the same token.
#[derive(Debug)]
struct SignatureCache {
    cache: RwLock<LruCache<String, Instant>>,
    ttl: Duration,
}

impl SignatureCache {
    fn new(capacity: usize, ttl_secs: u64) -> Self {
        Self {
            cache: RwLock::new(LruCache::new(
                std::num::NonZeroUsize::new(capacity.max(1)).unwrap_or(std::num::NonZeroUsize::MIN),
            )),
            ttl: Duration::from_secs(ttl_secs),
        }
    }

    fn _get(&self, jti: &str) -> bool {
        let mut cache = self.cache.write();
        match cache.get(jti) {
            Some(expires) => {
                if Instant::now() < *expires {
                    true
                } else {
                    cache.pop(jti);
                    false
                }
            }
            None => false,
        }
    }

    fn put(&self, jti: String) {
        self.cache.write().put(jti, Instant::now() + self.ttl);
    }
}

// ──── Capability Registry（动态能力表）────

/// scope 提取器：从入站请求（方法 + header + **body**）派生本次请求触碰的资源键/区间。
///
/// 第四轮 §3.3：原签名只接收 `&http::HeaderMap`，而 KV/Txn/Watch 的资源键在
/// **protobuf body** 里——所以它结构上无法产出真实资源键，生产路径于是从不注册它，
/// scope 检查从不执行（fail-open）。现改为可读 body，生产路径注册真实提取器。
///
/// 返回 `Err` 表示 body 畸形/超限（调用方按失败关闭拒绝）。
/// 缺省（`None`）表示该 RPC 不做 scope 校验。
pub type ScopeExtractor =
    Arc<dyn Fn(&str, &http::HeaderMap, &[u8]) -> Result<Vec<ScopeAccess>, String> + Send + Sync>;

/// 需要从 body 提取 scope 的 RPC（与 `coord_core::grpc_auth` 的定义一致）。
///
/// 该列表**不是**第二份能力表：能力 ID 仍从
/// [`coord_core::grpc_auth::rpc_capability`] 取，这里只声明「哪些方法要读 body」。
///
/// # ⚠️ 只允许**一元** RPC（第四轮回归事故的修复）
///
/// 本列表的每个方法都会被 [`AuthInterceptor::call`] 走
/// [`buffer_request_body`] 路径：**先读完整个 body 再转发**。对**流式** RPC 这个
/// 前提不成立——流的 body 在客户端 half-close 前永不结束，缓存整段 body 等于把
/// 请求永久挡在 handler 之外。
///
/// 第四轮曾把 `/coord.watch.Watch/Watch` 加进来（目标是让 Watch 既可用又受
/// scope 约束），实际后果是 **watch 彻底不通**：请求永不转发，客户端收不到事件也
/// 不报错（Rust `message().await` 永久挂起；Java 5s 超时失败）。而且本层**无论
/// `auth.enabled` 开不开都挂载**，所以与鉴权开关无关——纯属把可用性修没了。
///
/// 卡口：`scope_bearing_rpcs_are_unary_only`（本文件）+ `coord/tests/agent_watch_test.rs`。
/// Watch 的 scope 约束应由 handler 拿到**首帧** `WatchCreateRequest` 后判定，
/// 不得依赖 body 缓存。
const SCOPE_BEARING_RPCS: &[&str] = &[
    "/coord.kv.KV/Put",
    "/coord.kv.KV/Range",
    "/coord.kv.KV/Delete",
    "/coord.txn.Txn/Txn",
];

/// 生产用 scope 提取器：委托给两侧共用的 [`coord_core::grpc_auth`]。
fn body_scope_extractor(
    rpc_method: &str,
    _headers: &http::HeaderMap,
    body: &[u8],
) -> Result<Vec<ScopeAccess>, String> {
    coord_core::grpc_auth::extract_scope_access(rpc_method, body)
}

/// 能力表条目：RPC → capability_id + 可选 scope 提取器。
#[derive(Clone)]
pub struct CapabilityEntry {
    /// 需要的 capability ID（如 "data:kv:read"）
    pub capability_id: String,
    /// scope 资源键提取器（None = 不校验 scope）
    pub scope_extractor: Option<ScopeExtractor>,
}

/// 能力表查询结果。
pub enum CapabilityLookup {
    /// 已注册：需要能力（+ 可选 scope）校验
    Required(CapabilityEntry),
    /// 白名单：直接放行（如登录端点）
    Allowlisted,
    /// 未注册：拒绝（fail-closed）
    Unknown,
}

/// RPC → 能力映射注册表。
///
/// 内置表由 [`default_rpc_capability`] 提供（历史静态映射），
/// 插件/扩展可通过 [`CapabilityTable::register`] 动态追加或覆盖条目；
/// 未注册且非白名单的 RPC 一律拒绝，保持 fail-closed 语义。
#[derive(Default)]
pub struct CapabilityTable {
    entries: RwLock<HashMap<String, CapabilityEntry>>,
    allowlist: RwLock<std::collections::HashSet<String>>,
}

impl CapabilityTable {
    /// 空表（仅内置默认映射 + 无白名单）。
    pub fn new() -> Self {
        Self::default()
    }

    /// 内置 agent 能力表：**与 server 共用**的静态映射（`coord_core::grpc_auth`）
    /// + `Authenticate` 白名单 + **生产路径注册的 scope 提取器**。
    ///
    /// 第四轮 §3.3：此前 `register_with_scope` 全仓只有一个调用点、且在一个
    /// 测试里，生产装配下 `resource_key` 恒为 `None` → scope 检查从不执行。
    /// 现在默认表自身携带提取器（能力 ID 仍取自共享表，不会漂移）。
    pub fn default_agent() -> Self {
        let table = Self::new();
        table.allowlist("/coord.auth.Auth/Authenticate");
        // 协议版本协商（P0-4 / D6）：**必须**白名单化 —— 否则会形成死循环：
        // "要问服务端支持哪个协议版本，先得持有该版本的能力"。而协商端点存在的
        // 全部意义就是让版本不匹配**可诊断**，所以它不能要求调用方先证明自己已经
        // 匹配。返回内容仅为支持版本列表，无敏感信息。
        table.allowlist("/coord.agent.Handshake/Negotiate");
        for rpc in SCOPE_BEARING_RPCS {
            match coord_core::grpc_auth::rpc_capability(rpc) {
                Some(capability_id) => table.register_with_scope(
                    *rpc,
                    capability_id,
                    Some(Arc::new(body_scope_extractor)),
                ),
                None => tracing::error!(
                    rpc,
                    "scope-bearing RPC is missing from the shared capability table"
                ),
            }
        }
        table
    }

    /// 注册/覆盖一个 RPC → capability 映射（无 scope 提取器）。
    pub fn register(&self, rpc_method: impl Into<String>, capability_id: impl Into<String>) {
        self.register_with_scope(rpc_method, capability_id, None);
    }

    /// 注册/覆盖一个 RPC → capability 映射 + scope 提取器。
    pub fn register_with_scope(
        &self,
        rpc_method: impl Into<String>,
        capability_id: impl Into<String>,
        scope_extractor: Option<ScopeExtractor>,
    ) {
        self.entries.write().insert(
            rpc_method.into(),
            CapabilityEntry {
                capability_id: capability_id.into(),
                scope_extractor,
            },
        );
    }

    /// 将 RPC 加入白名单（不要求能力校验）。
    pub fn allowlist(&self, rpc_method: impl Into<String>) {
        self.allowlist.write().insert(rpc_method.into());
    }

    /// 查询某 RPC 的能力要求。
    pub fn lookup(&self, rpc_method: &str) -> CapabilityLookup {
        if let Some(entry) = self.entries.read().get(rpc_method) {
            return CapabilityLookup::Required(entry.clone());
        }
        if self.allowlist.read().contains(rpc_method) {
            return CapabilityLookup::Allowlisted;
        }
        match default_rpc_capability(rpc_method) {
            Some(capability_id) => CapabilityLookup::Required(CapabilityEntry {
                capability_id,
                scope_extractor: None,
            }),
            None => CapabilityLookup::Unknown,
        }
    }

    /// 已动态注册的条目数。
    pub fn registered_len(&self) -> usize {
        self.entries.read().len()
    }
}

/// 进程级默认能力表（`infer_capability` 的委托目标）。
fn default_capability_table() -> &'static CapabilityTable {
    static TABLE: std::sync::OnceLock<CapabilityTable> = std::sync::OnceLock::new();
    TABLE.get_or_init(CapabilityTable::default_agent)
}

/// Maps gRPC method paths to capability IDs（向后兼容入口）。
///
/// Agent intercepts the gRPC method name (e.g., "/coord.kv.Kv/Range")
/// and maps it to the corresponding capability ID.
/// 返回 `None` 表示白名单或未知 RPC（调用方按 fail-closed 处理）。
pub fn infer_capability(rpc_method: &str) -> Option<String> {
    match default_capability_table().lookup(rpc_method) {
        CapabilityLookup::Required(entry) => Some(entry.capability_id),
        CapabilityLookup::Allowlisted | CapabilityLookup::Unknown => None,
    }
}

/// 内置 RPC → capability 静态映射。
///
/// 第四轮 §3.4：本表**不再在 crate 内维护**——它此前写的是 `/coord.kv.Kv/*`
/// （proto 的真实路径是 `/coord.kv.KV/*`，服务名全大写），且只登记了 9 个服务，
/// `Registry`/`Config`/`Lock`/`LeaderElection`/`IdGen`/`Event` 等**全部缺失**
/// → 开启 agent 鉴权时目标场景 ①②③ 的服务调用全部被 `_ => None` 拒绝。
///
/// 现在收敛到 [`coord_core::grpc_auth::rpc_capability`]，与 server **共用同一份表**。
fn default_rpc_capability(rpc_method: &str) -> Option<String> {
    coord_core::grpc_auth::rpc_capability(rpc_method).map(str::to_string)
}

// ──── Auth Interceptor ────

/// The main auth interceptor for the Agent.
pub struct AuthInterceptor {
    /// CCT HMAC 签名密钥（历史对称方案，宽限期验证存量 token；可为空）
    signing_key: Vec<u8>,
    /// CCT Ed25519 验证公钥（32 字节；提供时验证非对称签发 token）
    verifying_key: Option<Vec<u8>>,
    /// Local role→capability cache
    role_cache: Arc<RoleCache>,
    /// Signature verification cache (LRU)
    sig_cache: SignatureCache,
    /// Clock drift tolerance in seconds
    clock_drift_secs: i64,
    /// Whether auth is enabled (if disabled, all requests pass through)
    enabled: bool,
    /// RPC → capability 注册表（可动态扩展；默认内置表）
    capability_table: Arc<CapabilityTable>,
}

impl AuthInterceptor {
    /// Create a new auth interceptor.
    pub fn new(signing_key: Vec<u8>, role_cache: Arc<RoleCache>, clock_drift_secs: i64) -> Self {
        Self {
            signing_key,
            verifying_key: None,
            role_cache,
            sig_cache: SignatureCache::new(10000, 60), // 10k entries, 60s TTL
            clock_drift_secs,
            enabled: true,
            capability_table: Arc::new(CapabilityTable::default_agent()),
        }
    }

    /// 挂载自定义能力表（插件/扩展动态注册入口）。
    pub fn with_capability_table(mut self, table: Arc<CapabilityTable>) -> Self {
        self.capability_table = table;
        self
    }

    /// 返回能力表句柄（供运行时动态注册）。
    pub fn capability_table(&self) -> Arc<CapabilityTable> {
        Arc::clone(&self.capability_table)
    }

    /// 从入站请求提取本次触碰的 scope 访问（按能力表注册的提取器）。
    ///
    /// 返回 `Ok(None)` = 该 RPC 未注册提取器（不做 scope 校验）；
    /// 返回 `Ok(Some(accesses))` = 提取成功；`Err` = body 畸形/超限。
    pub fn scope_accesses(
        &self,
        rpc_method: &str,
        headers: &http::HeaderMap,
        body: &[u8],
    ) -> Result<Option<Vec<ScopeAccess>>, String> {
        match self.capability_table.lookup(rpc_method) {
            CapabilityLookup::Required(entry) => match entry.scope_extractor.as_ref() {
                Some(f) => f(rpc_method, headers, body).map(Some),
                None => Ok(None),
            },
            _ => Ok(None),
        }
    }

    /// 该 RPC 是否需要读 body 才能做 scope 校验。
    pub fn has_scope_extractor(&self, rpc_method: &str) -> bool {
        matches!(
            self.capability_table.lookup(rpc_method),
            CapabilityLookup::Required(entry) if entry.scope_extractor.is_some()
        )
    }

    /// 挂载 Ed25519 验证公钥（server 持私钥签发，agent 仅存公钥）。
    pub fn with_verifying_key(mut self, verifying_key: Vec<u8>) -> Self {
        self.verifying_key = Some(verifying_key);
        self
    }

    /// Set whether auth is enabled.
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// Validate an incoming request（点查兼容入口：单一资源键）。
    ///
    /// Returns `AuthResult::Allow(token)` if the request passes all checks,
    /// or `AuthResult::Deny(reason)` if any check fails.
    pub fn validate_request(
        &self,
        rpc_method: &str,
        auth_header: Option<&str>,
        resource_key: Option<&str>,
    ) -> AuthResult {
        let accesses: Vec<ScopeAccess> = resource_key
            .map(|k| vec![ScopeAccess::point(k.as_bytes().to_vec())])
            .unwrap_or_default();
        self.validate_request_accesses(rpc_method, auth_header, &accesses)
    }

    /// Validate an incoming request（区间感知入口）。
    ///
    /// `accesses` 为本次请求触碰的全部 key/区间（由能力表注册的 scope 提取器从
    /// body 解析）；为空表示**无法提取资源键**——此时带 scope 的授权一律拒绝
    /// （fail-closed，与服务端 `authorize(.., None)` 同口径）。
    pub fn validate_request_accesses(
        &self,
        rpc_method: &str,
        auth_header: Option<&str>,
        accesses: &[ScopeAccess],
    ) -> AuthResult {
        // If auth is disabled, allow everything
        if !self.enabled {
            return AuthResult::Allow(CctToken {
                header: CctHeader::default(),
                payload: CctPayload {
                    jti: String::new(),
                    iss: String::new(),
                    sub: String::new(),
                    aud: vec![],
                    iat: 0,
                    exp: 0,
                    roles: vec![],
                    scope_overrides: HashMap::new(),
                },
                signature: vec![],
            });
        }

        // 1. Extract CCT from Authorization header
        let cct_str = match extract_bearer_token(auth_header) {
            Some(token) => token,
            None => return AuthResult::Deny("missing or invalid Authorization header".into()),
        };

        // 2. Decode and verify CCT（HMAC 历史密钥 + Ed25519 公钥双算法）
        let cct = match decode_cct_any(cct_str, &[&self.signing_key], self.verifying_key.as_deref())
        {
            Ok(token) => token,
            Err(e) => return AuthResult::Deny(format!("CCT validation failed: {e}")),
        };

        // 3. Check signature cache (skip re-verification)
        // Already verified in decode_cct, but cache for future requests
        self.sig_cache.put(cct.payload.jti.clone());

        // 4. Check expiration
        if is_expired(&cct.payload, self.clock_drift_secs) {
            return AuthResult::Deny("CCT expired".into());
        }

        // 5. Determine required capability from RPC method（动态注册表，未注册即拒绝）
        let capability_id = match self.capability_table.lookup(rpc_method) {
            CapabilityLookup::Required(entry) => entry.capability_id,
            CapabilityLookup::Allowlisted => return AuthResult::Allow(cct),
            CapabilityLookup::Unknown => {
                return AuthResult::Deny(format!("unknown RPC method: {rpc_method}"))
            }
        };

        // 5b. 引导管理员（root）旁路 —— 放在**路由登记之后**、能力/scope 判定之前。
        //
        // 契约语义：root 是"全能力"，而不是"恰好被授了这些能力"。因此它必须跳过
        // 第 6 步（能力）与第 7 步（scope）；但**不跳过**第 5 步的"未登记 RPC 即
        // 拒绝"—— 那条是**路由层面**的 fail-closed 防线（防"新增路由忘了登记能力"
        // 时悄悄放行），与"root 有没有这项能力"是两件事。服务端同构：请求先要命中
        // 真实路由，才轮到 `check_capability` 里的 root 放行。
        //
        // jepsen F-32：修复前 agent 侧**没有任何** root 旁路，而 server 侧
        // `check_capability` 对 root 直接放行 ⇒ 同一张 root CCT 直连 server 成功、
        // 经 agent 必然被拒：`role(s) ["root"] do not have capability 'data:kv:read'`
        // ——因为 root 的角色记录里本来就不逐项列举能力（全靠 server 的旁路兜着）。
        // 影响面：Java SDK / 运维脚本 / 一切以本机 agent 为入口的管理员调用，
        // 即 **auth 开启时 agent 作为应用入口的整条路径对 root 不可用**，
        // 而进程内测试看不到（它们直接调 server）。
        if coord_core::auth::is_root(&cct.payload.roles) {
            return AuthResult::Allow(cct);
        }

        // 6. Check role→capability mapping（授权 scope 列表；空 = 未授予）
        let grant_scopes = self
            .role_cache
            .scopes_for_capability(&cct.payload.roles, &capability_id);

        if grant_scopes.is_empty() {
            return AuthResult::Deny(format!(
                "role(s) {:?} do not have capability '{capability_id}'",
                cct.payload.roles
            ));
        }

        // 7. Scope 判定 —— **fail-closed**。
        //
        // 第四轮 §3.3：此前是 `if let (Some(key), Some(trie)) = (resource_key, ...)`，
        // 即两个 Option 任一为 `None` 就**直接放行**。而生产路径从不注册
        // scope 提取器（唯一注册点在一个测试里）→ `resource_key` 恒为 None
        // → scope 检查从不执行。现在改为：无法提取资源键 + 授权带非空 scope
        // → 拒绝（与服务端 `authorize(.., None)` 完全同口径）。
        if !scope_allows(&grant_scopes, accesses) {
            return AuthResult::Deny(format!(
                "scope restriction: capability '{capability_id}' not granted for the \
                 requested resource(s); roles {:?}, accesses {:?} (fail-closed)",
                cct.payload.roles, accesses
            ));
        }

        AuthResult::Allow(cct)
    }
}

/// scope 判定（fail-closed）：本次请求触碰的**全部**访问都必须被授权覆盖。
///
/// - `accesses` 为空（未能提取资源键）→ 只有**无约束**授权（存在空 scope）放行；
///   存在非空 scope 限制时拒绝。
/// - 否则逐条判定：单键走 `ScopeTrie::matches`；区间走
///   [`coord_core::auth::trie::scope_covers_interval`]（要求**整体包含**，
///   与服务端 A1 的区间语义一致）。
fn scope_allows(grant_scopes: &[String], accesses: &[ScopeAccess]) -> bool {
    if accesses.is_empty() {
        return grant_scopes.iter().any(|s| s.is_empty());
    }
    accesses.iter().all(|access| {
        grant_scopes.iter().any(|scope| {
            if scope.is_empty() {
                return true; // 无约束授权覆盖一切
            }
            if access.range_end.is_empty() {
                match std::str::from_utf8(&access.key) {
                    Ok(key) => {
                        let mut trie = ScopeTrie::new();
                        trie.insert(scope).is_ok() && trie.matches(key)
                    }
                    // 非 UTF-8 key 无法与字符串 scope 比对 → 拒绝（fail-closed）
                    Err(_) => false,
                }
            } else {
                coord_core::auth::trie::scope_covers_interval(scope, &access.key, &access.range_end)
            }
        })
    })
}

// ──── Tower Layer / Service（接入 agent gRPC 生产路由）────
//
// tonic 0.14 的 Interceptor 拿不到方法路径（Request 不保留 URI），
// 故使用 tower 中间件：从 http::Request 的 URI path 提取 gRPC 方法，
// 校验 CCT + capability，未知 RPC / 无凭据 / 无 capability 一律拒绝（fail-closed）。

/// AuthInterceptor 的 tower Layer（用于 `tonic::Server::builder().layer(...)`）
#[derive(Clone)]
pub struct AuthLayer {
    interceptor: Arc<AuthInterceptor>,
}

impl AuthLayer {
    pub fn new(interceptor: Arc<AuthInterceptor>) -> Self {
        Self { interceptor }
    }
}

impl<S> Layer<S> for AuthLayer {
    type Service = AuthService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AuthService {
            inner,
            interceptor: self.interceptor.clone(),
        }
    }
}

/// 包一层 inner 服务的鉴权中间件
#[derive(Clone)]
pub struct AuthService<S> {
    inner: S,
    interceptor: Arc<AuthInterceptor>,
}

impl<S> Service<http::Request<tonic::body::Body>> for AuthService<S>
where
    S: Service<http::Request<tonic::body::Body>, Response = http::Response<tonic::body::Body>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = S::Error;
    type Future = AuthFuture<S::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<tonic::body::Body>) -> Self::Future {
        let rpc_method = req.uri().path().to_string();
        let auth_header = req
            .headers()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        // 第四轮 §3.2：把**调用方自己的凭据**转发给服务端。agent 是最后一跳代理，
        // 服务端才是权威授权点；此前 agent 出站客户端 `token_provider = None`，
        // 生产默认配置（auth_enabled=true）下经 agent 的调用一律 `missing CCT token`。
        // 凭据放进任务局部量，由出站 `CredentialInterceptor` 在**每个**出站请求上读取。
        let forward_token = extract_bearer_token(auth_header.as_deref()).map(str::to_string);

        // 第四轮 §3.3：scope 提取器需要读 body（资源键在 protobuf 里）→ 异步路径。
        if self.interceptor.has_scope_extractor(&rpc_method) {
            let interceptor = Arc::clone(&self.interceptor);
            let mut inner = self.inner.clone();
            let fut: BoxedAuthFuture<S::Error> = Box::pin(async move {
                let (req, body) = match buffer_request_body(req, MAX_SCOPE_BODY_BYTES).await {
                    Ok(v) => v,
                    Err(reason) => {
                        return Ok(deny_response(Some(Status::resource_exhausted(reason))))
                    }
                };
                let accesses = match interceptor.scope_accesses(&rpc_method, req.headers(), &body) {
                    Ok(Some(accesses)) => accesses,
                    Ok(None) => Vec::new(),
                    Err(reason) => {
                        return Ok(deny_response(Some(Status::invalid_argument(reason))))
                    }
                };
                match interceptor.validate_request_accesses(
                    &rpc_method,
                    auth_header.as_deref(),
                    &accesses,
                ) {
                    AuthResult::Allow(cct) => {
                        let mut req = req;
                        req.extensions_mut().insert(crate::plugin::GatewayIdentity {
                            subject: cct.payload.sub.clone(),
                            roles: cct.payload.roles.clone(),
                        });
                        match inner.ready().await {
                            Ok(svc) => {
                                // 代理上下文（`scoped_proxied_request`）：既转发调用方凭据，
                                // 又标记"这是替入站请求发的"⇒ 出站回退凭据（agent 自身身份）
                                // 在本上下文内**不生效**（调用方没带凭据就必须以无凭据上报）。
                                coord_client::credential::scoped_proxied_request(
                                    forward_token,
                                    svc.call(req),
                                )
                                .await
                            }
                            Err(e) => Err(e),
                        }
                    }
                    AuthResult::Deny(reason) => {
                        Ok(deny_response(Some(Status::unauthenticated(reason))))
                    }
                }
            });
            return AuthFuture::Allow(fut);
        }

        match self
            .interceptor
            .validate_request_accesses(&rpc_method, auth_header.as_deref(), &[])
        {
            AuthResult::Allow(cct) => {
                // Phase 2.1：把身份发布到请求扩展，供内层（插件网关层）观察。
                // 鉴权关闭时 validate_request 返回占位 CCT（roles 为空）。
                let mut req = req;
                req.extensions_mut().insert(crate::plugin::GatewayIdentity {
                    subject: cct.payload.sub.clone(),
                    roles: cct.payload.roles.clone(),
                });
                // 转发调用方凭据（见上）——出站 CredentialInterceptor 读任务局部量；
                // 同时置位代理上下文标记（禁止回退到 agent 自身身份）。
                let fut = coord_client::credential::scoped_proxied_request(
                    forward_token,
                    self.inner.call(req),
                );
                AuthFuture::Allow(Box::pin(fut))
            }
            AuthResult::Deny(reason) => AuthFuture::Deny(Some(coord_core::error_code::attach(
                Status::unauthenticated(reason),
                coord_core::error_code::CoordErrorCode::Unauthenticated,
            ))),
        }
    }
}

/// 装箱的鉴权后置 future（需要读 body 时用）。
type BoxedAuthFuture<E> =
    Pin<Box<dyn Future<Output = Result<http::Response<tonic::body::Body>, E>> + Send>>;

/// 鉴权中间件 future：放行转发给 inner，拒绝立即返回 gRPC 错误响应
pub enum AuthFuture<E> {
    /// 判定已完成/将异步完成，最终把 inner 的响应透传。
    Allow(BoxedAuthFuture<E>),
    Deny(Option<Status>),
}

impl<E> Future for AuthFuture<E> {
    type Output = Result<http::Response<tonic::body::Body>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: 不移动任何字段；`Allow` 内的 future 由 `Box::pin` 固定，
        // 因此手动投影是安全的（与 ServerAuthFuture 采用同一约定）。
        let this = unsafe { self.get_unchecked_mut() };
        match this {
            AuthFuture::Allow(fut) => fut.as_mut().poll(cx),
            AuthFuture::Deny(status) => match status.take() {
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

/// 将 gRPC 状态码转换为 HTTP 拒绝响应。
///
/// 兜底拒绝路径也必须带错误码（第四轮 §3.14.2）：否则 Java 侧对这类失败只能
/// 落到有损的状态码表上。
fn deny_response(status: Option<Status>) -> http::Response<tonic::body::Body> {
    let status = status.unwrap_or_else(|| {
        coord_core::error_code::attach(
            Status::permission_denied("denied"),
            coord_core::error_code::CoordErrorCode::PermissionDenied,
        )
    });
    let (parts, ()) = status.into_http::<()>().into_parts();
    http::Response::from_parts(parts, tonic::body::Body::empty())
}

/// 缓存请求 body 字节后重建请求（scope 提取用）。
///
/// 与 server 侧同一取舍：body 是流式的，提取资源键前必须先收集（**受硬上限保护**），
/// 解析后再以 `Full` 重建，保证 inner 看到的请求与原始请求等价。
async fn buffer_request_body(
    req: http::Request<tonic::body::Body>,
    max_bytes: usize,
) -> Result<(http::Request<tonic::body::Body>, Vec<u8>), String> {
    let (parts, body) = req.into_parts();
    let limited = http_body_util::Limited::new(body, max_bytes);
    match limited.collect().await {
        Ok(collected) => {
            let bytes = collected.to_bytes();
            let body_bytes = bytes.to_vec();
            let rebuilt = tonic::body::Body::new(http_body_util::Full::new(bytes));
            Ok((http::Request::from_parts(parts, rebuilt), body_bytes))
        }
        Err(_) => Err(format!(
            "request body exceeds scope-extraction limit of {max_bytes} bytes"
        )),
    }
}

// ──── Helpers ────

/// Extract bearer token from Authorization header.
/// Supports both "Bearer <token>" and "<token>" formats.
fn extract_bearer_token(header: Option<&str>) -> Option<&str> {
    let header = header?;
    if let Some(token) = header.strip_prefix("Bearer ") {
        Some(token)
    } else if header.starts_with("eyJ") {
        // CCT v3 format (base64url JSON header)
        Some(header)
    } else if header.starts_with("coord_") {
        // Legacy token format — pass through
        None
    } else {
        None
    }
}

// ──── Tests ────

#[cfg(test)]
mod tests {
    use super::*;
    use coord_core::auth::cct::{encode_cct, CctHeader, CctPayload};

    const TEST_KEY: &[u8] = b"test-signing-key-32-bytes-long!!";

    fn make_test_cct(roles: Vec<&str>, scope_overrides: HashMap<String, String>) -> String {
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
        encode_cct(&header, &payload, TEST_KEY).unwrap()
    }

    // ──── Auth interceptor tests ────

    #[test]
    fn test_interceptor_allows_when_disabled() {
        let role_cache = Arc::new(RoleCache::new());
        let mut interceptor = AuthInterceptor::new(TEST_KEY.to_vec(), role_cache, 300);
        interceptor.set_enabled(false);

        let result = interceptor.validate_request("/coord.kv.KV/Range", None, None);
        assert!(matches!(result, AuthResult::Allow(_)));
    }

    #[test]
    fn test_interceptor_denies_missing_auth_header() {
        let role_cache = Arc::new(RoleCache::new());
        let interceptor = AuthInterceptor::new(TEST_KEY.to_vec(), role_cache, 300);

        let result = interceptor.validate_request("/coord.kv.KV/Range", None, None);
        assert!(matches!(result, AuthResult::Deny(_)));
    }

    #[test]
    fn test_interceptor_allows_authenticate_endpoint() {
        let role_cache = Arc::new(RoleCache::new());
        let interceptor = AuthInterceptor::new(TEST_KEY.to_vec(), role_cache, 300);

        let cct = make_test_cct(vec!["reader"], HashMap::new());
        let auth_header = format!("Bearer {cct}");

        let result =
            interceptor.validate_request("/coord.auth.Auth/Authenticate", Some(&auth_header), None);
        assert!(matches!(result, AuthResult::Allow(_)));
    }

    #[test]
    fn test_interceptor_validates_capability() {
        let role_cache = Arc::new(RoleCache::new());
        role_cache.sync_full(vec![super::super::role_cache::RoleEntry {
            name: "reader".to_string(),
            grants: vec![super::super::role_cache::CapabilityGrant {
                capability_id: "data:kv:read".to_string(),
                scope: "/app/".to_string(),
            }],
            high_sensitive: false,
        }]);

        let interceptor = AuthInterceptor::new(TEST_KEY.to_vec(), role_cache, 300);
        let cct = make_test_cct(vec!["reader"], HashMap::new());
        let auth_header = format!("Bearer {cct}");

        // KV Range (data:kv:read) should be allowed within scope
        let result = interceptor.validate_request(
            "/coord.kv.KV/Range",
            Some(&auth_header),
            Some("/app/order-123"),
        );
        assert!(matches!(result, AuthResult::Allow(_)));

        // KV Range outside scope should be denied
        let result = interceptor.validate_request(
            "/coord.kv.KV/Range",
            Some(&auth_header),
            Some("/admin/secret"),
        );
        assert!(matches!(result, AuthResult::Deny(_)));
    }

    /// F-32 [P0]：**root 必须能经 agent 走通全路径**。
    ///
    /// 修复前 agent 侧没有任何 root 旁路，而 root 的角色记录里不逐项列能力
    /// （全靠 server 的旁路兜着）⇒ 同一张 root CCT 直连 server 成功、
    /// 经 agent 必然被拒：`role(s) ["root"] do not have capability 'data:kv:read'`。
    /// 也就是 auth 开启时 **agent 作为应用入口的整条路径对 root 不可用**。
    ///
    /// 这个用例刻意**不**给 root 同步任何 RoleEntry —— 正是"角色记录里没有逐项能力"
    /// 的真实形态；若修复回退成"按显式能力集判定"，这里会立刻红。
    #[test]
    fn test_interceptor_root_bypasses_capability_without_explicit_grants() {
        let role_cache = Arc::new(RoleCache::new());
        // 刻意不同步任何角色：root 的能力不来自缓存里的逐项授权
        let interceptor = AuthInterceptor::new(TEST_KEY.to_vec(), role_cache, 300);
        let cct = make_test_cct(vec!["root"], HashMap::new());
        let auth_header = format!("Bearer {cct}");

        // 数据面（能力 + scope 两道关都该被 root 跳过），且不带资源键
        for rpc in [
            "/coord.kv.KV/Range",
            "/coord.kv.KV/Put",
            "/coord.lock.v1.Lock/Release",
            "/coord.registry.v1.Registry/Discover",
        ] {
            let result = interceptor.validate_request(rpc, Some(&auth_header), None);
            assert!(
                matches!(result, AuthResult::Allow(_)),
                "root 经 agent 调用 {rpc} 必须放行，实际: {result:?}"
            );
        }

        // 但**路由层**的 fail-closed 防线不因 root 而失效：
        // 未登记能力的 RPC 仍然拒绝（root 是"全能力"，不是"绕过路由登记"）
        let result = interceptor.validate_request(
            "/coord.agent.NotRegistered/Whatever",
            Some(&auth_header),
            None,
        );
        assert!(
            matches!(result, AuthResult::Deny(_)),
            "未登记能力的 RPC 对 root 也应拒绝（fail-closed），实际: {result:?}"
        );
    }

    /// 反向对照：F-32 的修法是"root 旁路"，**不是**"放宽能力判定"。
    /// 非 root 角色在没有对应授权时仍必须被拒。
    #[test]
    fn test_interceptor_non_root_still_denied_without_grant() {
        let role_cache = Arc::new(RoleCache::new());
        let interceptor = AuthInterceptor::new(TEST_KEY.to_vec(), role_cache, 300);
        let cct = make_test_cct(vec!["reader"], HashMap::new());
        let auth_header = format!("Bearer {cct}");

        let result = interceptor.validate_request("/coord.kv.KV/Range", Some(&auth_header), None);
        assert!(
            matches!(result, AuthResult::Deny(_)),
            "非 root 且未授权时必须拒绝，实际: {result:?}"
        );
    }

    /// 第四轮 §3.3（A2）：授权带**非空 scope** 但请求未提供资源键时，必须
    /// **fail-closed 拒绝**。这是此前 fail-open 的具体形状：
    /// `if let (Some(key), Some(trie)) = ...` 在两个 Option 任一为 None 时直接放行。
    #[test]
    fn test_interceptor_scope_restricted_capability_fails_closed_without_resource_key() {
        let role_cache = Arc::new(RoleCache::new());
        role_cache.sync_full(vec![super::super::role_cache::RoleEntry {
            name: "reader".to_string(),
            grants: vec![super::super::role_cache::CapabilityGrant {
                capability_id: "data:kv:read".to_string(),
                scope: "/app/".to_string(),
            }],
            high_sensitive: false,
        }]);
        let interceptor = AuthInterceptor::new(TEST_KEY.to_vec(), role_cache, 300);
        let cct = make_test_cct(vec!["reader"], HashMap::new());
        let auth_header = format!("Bearer {cct}");

        // 无资源键 → 拒绝（fail-closed）
        let result = interceptor.validate_request("/coord.kv.KV/Range", Some(&auth_header), None);
        assert!(
            matches!(result, AuthResult::Deny(_)),
            "带非空 scope 的授权在无资源键时必须拒绝，实际: {result:?}"
        );

        // 显式传入提取到的资源键 → 命中 scope，放行
        let result = interceptor.validate_request(
            "/coord.kv.KV/Range",
            Some(&auth_header),
            Some("/app/order-1"),
        );
        assert!(matches!(result, AuthResult::Allow(_)));
    }

    /// A2：**无约束**授权（空 scope）在无资源键时仍应放行。
    #[test]
    fn test_interceptor_unrestricted_capability_allows_without_resource_key() {
        let role_cache = Arc::new(RoleCache::new());
        role_cache.sync_full(vec![super::super::role_cache::RoleEntry {
            name: "reader".to_string(),
            grants: vec![super::super::role_cache::CapabilityGrant {
                capability_id: "data:kv:read".to_string(),
                scope: String::new(),
            }],
            high_sensitive: false,
        }]);
        let interceptor = AuthInterceptor::new(TEST_KEY.to_vec(), role_cache, 300);
        let cct = make_test_cct(vec!["reader"], HashMap::new());
        let auth_header = format!("Bearer {cct}");
        let result = interceptor.validate_request("/coord.kv.KV/Range", Some(&auth_header), None);
        assert!(matches!(result, AuthResult::Allow(_)));
    }

    #[test]
    fn test_interceptor_denies_missing_capability() {
        let role_cache = Arc::new(RoleCache::new());
        role_cache.sync_full(vec![super::super::role_cache::RoleEntry {
            name: "reader".to_string(),
            grants: vec![super::super::role_cache::CapabilityGrant {
                capability_id: "data:kv:read".to_string(),
                scope: "".to_string(),
            }],
            high_sensitive: false,
        }]);

        let interceptor = AuthInterceptor::new(TEST_KEY.to_vec(), role_cache, 300);
        let cct = make_test_cct(vec!["reader"], HashMap::new());
        let auth_header = format!("Bearer {cct}");

        // KV Put (data:kv:write) should be denied — reader doesn't have it
        let result =
            interceptor.validate_request("/coord.kv.KV/Put", Some(&auth_header), Some("/app/data"));
        assert!(matches!(result, AuthResult::Deny(_)));
    }

    #[test]
    fn test_interceptor_rejects_expired_token() {
        let role_cache = Arc::new(RoleCache::new());
        let interceptor = AuthInterceptor::new(TEST_KEY.to_vec(), role_cache, 300);

        let header = CctHeader::default();
        let payload = CctPayload {
            jti: "expired-token".to_string(),
            iss: "test".to_string(),
            sub: "test".to_string(),
            aud: vec![],
            iat: 1000000000,
            exp: 1000003600, // expired long ago
            roles: vec!["reader".to_string()],
            scope_overrides: HashMap::new(),
        };
        let cct = encode_cct(&header, &payload, TEST_KEY).unwrap();
        let auth_header = format!("Bearer {cct}");

        let result = interceptor.validate_request("/coord.kv.KV/Range", Some(&auth_header), None);
        assert!(matches!(result, AuthResult::Deny(_)));
    }

    #[test]
    fn test_infer_capability_mappings() {
        assert_eq!(
            infer_capability("/coord.kv.KV/Range"),
            Some("data:kv:read".into())
        );
        assert_eq!(
            infer_capability("/coord.kv.KV/Put"),
            Some("data:kv:write".into())
        );
        assert_eq!(
            infer_capability("/coord.kv.KV/Delete"),
            Some("data:kv:delete".into())
        );
        // 旧拼写（`Kv`）必须**不再**被识别：它正是「开启 agent 鉴权即拒绝全部 KV」
        // 的根因（真实路径由 `package coord.kv; service KV` 决定，服务名全大写）。
        assert_eq!(infer_capability("/coord.kv.Kv/Range"), None);
        // 目标场景 ①② 的服务面必须在表内（否则开启鉴权即被拒）。
        assert_eq!(
            infer_capability("/coord.registry.v1.Registry/Register"),
            Some("coord:registry:register".into())
        );
        assert_eq!(
            infer_capability("/coord.config.v1.Config/Get"),
            Some("coord:config:read".into())
        );
        assert_eq!(
            infer_capability("/coord.lock.v1.Lock/Acquire"),
            Some("coord:lock:acquire".into())
        );
        assert_eq!(
            infer_capability("/coord.election.v1.LeaderElection/Campaign"),
            Some("coord:election:campaign".into())
        );
        assert_eq!(
            infer_capability("/coord.txn.Txn/Txn"),
            Some("data:txn:execute".into())
        );
        assert_eq!(
            infer_capability("/coord.lease.Lease/LeaseGrant"),
            Some("data:lease:grant".into())
        );
        assert_eq!(
            infer_capability("/coord.watch.Watch/Watch"),
            Some("data:watch:subscribe".into())
        );
        assert_eq!(infer_capability("/coord.auth.Auth/Authenticate"), None);
        assert_eq!(infer_capability("/unknown.Service/Method"), None);
    }

    /// P0b：动态注册条目对 auth 决策即时生效；scope 提取器进入 scope 校验。
    #[test]
    fn test_dynamic_capability_registration() {
        let role_cache = Arc::new(RoleCache::new());
        role_cache.sync_full(vec![crate::auth::role_cache::RoleEntry {
            name: "plugin".to_string(),
            grants: vec![crate::auth::role_cache::CapabilityGrant {
                capability_id: "plugin:echo".to_string(),
                scope: "/app/plugin/".to_string(),
            }],
            high_sensitive: false,
        }]);

        let table = Arc::new(CapabilityTable::default_agent());
        // 动态注册：RPC → capability + 从 header 提取 scope 访问（新签名为 3 参）
        table.register_with_scope(
            "/coord.plugin.Plugin/Invoke",
            "plugin:echo",
            Some(Arc::new(
                |_rpc: &str, headers: &http::HeaderMap, _body: &[u8]| {
                    Ok(headers
                        .get("x-coord-scope-key")
                        .and_then(|v| v.to_str().ok())
                        .map(|s| vec![ScopeAccess::point(s.as_bytes().to_vec())])
                        .unwrap_or_default())
                },
            )),
        );
        let interceptor = AuthInterceptor::new(TEST_KEY.to_vec(), role_cache, 300)
            .with_capability_table(Arc::clone(&table));

        // 注册前未知 RPC：拒绝
        let unknown = AuthInterceptor::new(TEST_KEY.to_vec(), Arc::new(RoleCache::new()), 300);
        let cct = make_test_cct(vec!["plugin"], HashMap::new());
        let auth_header = format!("Bearer {cct}");
        assert!(matches!(
            unknown.validate_request("/coord.plugin.Plugin/Invoke", Some(&auth_header), None),
            AuthResult::Deny(_)
        ));

        // 注册后：scope 命中放行
        assert!(matches!(
            interceptor.validate_request(
                "/coord.plugin.Plugin/Invoke",
                Some(&auth_header),
                Some("/app/plugin/x")
            ),
            AuthResult::Allow(_)
        ));
        // scope 越界拒绝
        assert!(matches!(
            interceptor.validate_request(
                "/coord.plugin.Plugin/Invoke",
                Some(&auth_header),
                Some("/other/x")
            ),
            AuthResult::Deny(_)
        ));

        // scope 提取器从 header 取值
        let mut headers = http::HeaderMap::new();
        headers.insert("x-coord-scope-key", "/app/plugin/k".parse().unwrap());
        assert_eq!(
            interceptor
                .scope_accesses("/coord.plugin.Plugin/Invoke", &headers, &[])
                .expect("提取器不应失败")
                .expect("提取器已注册"),
            vec![ScopeAccess::point(b"/app/plugin/k".to_vec())]
        );
        // 默认表已携带全部 scope 承载 RPC 的提取器（生产路径注册）+ 本用例注册的 1 个
        assert_eq!(table.registered_len(), SCOPE_BEARING_RPCS.len() + 1);
    }

    /// 第四轮回归卡口：**流式 RPC 绝不能进入\"先读完 body 再转发\"的集合**。
    ///
    /// 事故形态：`/coord.watch.Watch/Watch` 被加进 `SCOPE_BEARING_RPCS` 后，鉴权层
    /// 对它走 `buffer_request_body`；而流式 body 在客户端 half-close 前永不结束，
    /// 请求于是永久挡在 handler 之外 —— watch 彻底不通，且客户端既不收事件也不报错
    /// （Rust `message().await` 永久挂起，Java 5s 超时失败）。本层无论 auth 开关都挂载，
    /// 因此这是个纯可用性回归。
    ///
    /// 该测试会在\"有人再次把流式方法加进集合\"时立刻变红。
    #[test]
    fn scope_bearing_rpcs_are_unary_only() {
        for rpc in SCOPE_BEARING_RPCS {
            assert!(
                !coord_core::grpc_auth::is_streaming_rpc(rpc),
                "{rpc} 是流式 RPC：其 body 在客户端 half-close 前不会结束，\
                 不得进入需要缓存 body 的 scope 提取集合（会导致该 RPC 永久挂起）"
            );
            assert!(
                coord_core::grpc_auth::needs_scope_extraction(rpc),
                "{rpc} 在 core 的 needs_scope_extraction 中也应为 true（两侧同源）"
            );
        }

        // 反向：这些流式方法必须被识别为流式，且不在缓存集合里
        for rpc in [
            "/coord.watch.Watch/Watch",
            "/coord.lease.Lease/LeaseKeepAlive",
            "/coord.mq.MQ/Subscribe",
        ] {
            assert!(
                coord_core::grpc_auth::is_streaming_rpc(rpc),
                "{rpc} 应被识别为流式 RPC"
            );
            assert!(
                !SCOPE_BEARING_RPCS.contains(&rpc),
                "{rpc} 不得出现在 SCOPE_BEARING_RPCS 中"
            );
            assert!(
                !coord_core::grpc_auth::needs_scope_extraction(rpc),
                "{rpc} 不得被判定为需要缓存 body"
            );
        }
    }

    /// **Watch 的 scope 真实语义**（第四轮 P0 修复之后），必须机械验证而不是写在文档里。
    ///
    /// agent 层看不到 Watch 的 prefix —— 它在**流式 body** 里，而提取器只读 header
    /// （这正是第四轮试图缓存 body 结果把 watch 弄死的那个位置）。因此 Watch 的 scope
    /// 判定只能以"未提取到任何访问"（`accesses = []`）进入，而 `scope_allows` 对空
    /// accesses 是 **fail-closed** 的：只有存在**无约束**（空 scope）授权才放行。
    ///
    /// 于是真实结论是：
    /// * 带非空 scope 限制的角色**不能**借 Watch 越权订阅 —— 它被**直接拒绝**，
    ///   不存在"预检查缺失 = 可以绕过"；
    /// * 实际代价是**功能受限**：这类角色用不了 Watch。要放行合法订阅，必须到 handler
    ///   侧解码首帧 `WatchCreateRequest`（prefix 在那里才可见）再判 scope。
    ///
    /// 本测试把这两条钉住：它既防止"scope 被悄悄放宽成放行"，也防止有人误以为
    /// 这里存在漏洞而去加一个会再次挂起 watch 的 body 缓存。
    #[test]
    fn watch_scope_is_fail_closed_not_bypassed() {
        use super::super::role_cache::{CapabilityGrant, RoleEntry};

        let watch_rpc = "/coord.watch.Watch/Watch";

        // ① 带非空 scope 的 watch 能力 → **拒绝**（不是放行）
        let role_cache = Arc::new(RoleCache::new());
        role_cache.sync_full(vec![RoleEntry {
            name: "scoped-watcher".to_string(),
            grants: vec![CapabilityGrant {
                capability_id: "data:watch:subscribe".to_string(),
                scope: "/app/counter/".to_string(),
            }],
            high_sensitive: false,
        }]);
        let interceptor = AuthInterceptor::new(TEST_KEY.to_vec(), role_cache, 300);
        let cct = make_test_cct(vec!["scoped-watcher"], HashMap::new());
        let auth_header = format!("Bearer {cct}");

        let result = interceptor.validate_request_accesses(watch_rpc, Some(&auth_header), &[]);
        assert!(
            matches!(result, AuthResult::Deny(_)),
            "带 scope 限制的角色订阅 Watch 必须 fail-closed 拒绝；\
             若这里是 Allow，那就是**越权订阅**（可读 scope 之外的数据）"
        );

        // ② 无约束（空 scope）授权 → 放行（这是 watch 生产可用的前提，别误伤）
        let role_cache = Arc::new(RoleCache::new());
        role_cache.sync_full(vec![RoleEntry {
            name: "unrestricted-watcher".to_string(),
            grants: vec![CapabilityGrant {
                capability_id: "data:watch:subscribe".to_string(),
                scope: String::new(),
            }],
            high_sensitive: false,
        }]);
        let interceptor = AuthInterceptor::new(TEST_KEY.to_vec(), role_cache, 300);
        let cct = make_test_cct(vec!["unrestricted-watcher"], HashMap::new());
        let auth_header = format!("Bearer {cct}");

        let result = interceptor.validate_request_accesses(watch_rpc, Some(&auth_header), &[]);
        assert!(
            matches!(result, AuthResult::Allow(_)),
            "无 scope 约束的 data:watch:subscribe 必须能订阅（否则 watch 对普通角色不可用）"
        );
    }

    /// PKI RPC 必须映射到 capability（私钥集中存储前上鉴权）
    #[test]
    fn test_infer_capability_pki_mappings() {
        assert_eq!(
            infer_capability("/coord.pki.v1.Pki/InitCa"),
            Some("pki:ca:init".into())
        );
        assert_eq!(
            infer_capability("/coord.pki.v1.Pki/IssueCert"),
            Some("pki:cert:issue".into())
        );
        assert_eq!(
            infer_capability("/coord.pki.v1.Pki/RenewCert"),
            Some("pki:cert:issue".into())
        );
        assert_eq!(
            infer_capability("/coord.pki.v1.Pki/RotateCert"),
            Some("pki:cert:rotate".into())
        );
        assert_eq!(
            infer_capability("/coord.pki.v1.Pki/ListCerts"),
            Some("pki:cert:read".into())
        );
        assert_eq!(
            infer_capability("/coord.pki.v1.Pki/GetCertByCN"),
            Some("pki:cert:read".into())
        );
        assert_eq!(
            infer_capability("/coord.pki.v1.Pki/GetCaCert"),
            Some("pki:cert:read".into())
        );
        assert_eq!(
            infer_capability("/coord.pki.v1.Pki/VerifyCert"),
            Some("pki:cert:read".into())
        );
        // 未知 PKI RPC 默认 deny（fail-closed）
        assert_eq!(infer_capability("/coord.pki.v1.Pki/UnknownRpc"), None);
    }

    // ──── tower 中间件测试 ────

    /// 测试用透传 inner 服务
    #[derive(Clone)]
    struct Passthrough;
    impl Service<http::Request<tonic::body::Body>> for Passthrough {
        type Response = http::Response<tonic::body::Body>;
        type Error = tonic::Status;
        type Future = std::future::Ready<Result<http::Response<tonic::body::Body>, tonic::Status>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: http::Request<tonic::body::Body>) -> Self::Future {
            std::future::ready(Ok(http::Response::new(tonic::body::Body::empty())))
        }
    }

    fn make_auth_service(interceptor: AuthInterceptor) -> AuthService<Passthrough> {
        AuthService {
            inner: Passthrough,
            interceptor: Arc::new(interceptor),
        }
    }

    fn make_http_request(
        path: &str,
        auth_header: Option<&str>,
    ) -> http::Request<tonic::body::Body> {
        let mut req = http::Request::builder()
            .uri(path)
            .body(tonic::body::Body::empty())
            .expect("build request");
        if let Some(h) = auth_header {
            req.headers_mut()
                .insert("authorization", h.parse().expect("valid header"));
        }
        req
    }

    /// 无凭据调用 PKI RPC → 拒绝（grpc-status=UNAUTHENTICATED=16）
    #[tokio::test]
    async fn test_auth_service_denies_pki_without_token() {
        let role_cache = Arc::new(RoleCache::new());
        let mut svc = make_auth_service(AuthInterceptor::new(TEST_KEY.to_vec(), role_cache, 300));

        let resp = svc
            .call(make_http_request("/coord.pki.v1.Pki/IssueCert", None))
            .await
            .expect("service 不应报传输错误");
        let grpc_status = resp
            .headers()
            .get("grpc-status")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        assert_eq!(
            grpc_status.as_deref(),
            Some("16"),
            "无凭据必须 grpc-status=UNAUTHENTICATED(16)"
        );
    }

    /// 有效 CCT + 具备 capability → 放行（透传到 inner）
    #[tokio::test]
    async fn test_auth_service_allows_pki_with_capability() {
        let role_cache = Arc::new(RoleCache::new());
        role_cache.sync_full(vec![super::super::role_cache::RoleEntry {
            name: "pki_issuer".to_string(),
            grants: vec![super::super::role_cache::CapabilityGrant {
                capability_id: "pki:cert:issue".to_string(),
                scope: "".to_string(),
            }],
            high_sensitive: false,
        }]);
        let mut svc = make_auth_service(AuthInterceptor::new(TEST_KEY.to_vec(), role_cache, 300));

        let cct = make_test_cct(vec!["pki_issuer"], HashMap::new());
        let header = format!("Bearer {cct}");
        let resp = svc
            .call(make_http_request(
                "/coord.pki.v1.Pki/IssueCert",
                Some(&header),
            ))
            .await
            .expect("service 不应报传输错误");
        assert_eq!(
            resp.status(),
            http::StatusCode::OK,
            "有效 CCT + pki:cert:issue 应放行"
        );
    }

    /// 只读角色调用签发 RPC → 拒绝（分级授权）
    #[tokio::test]
    async fn test_auth_service_denies_pki_write_with_read_only_role() {
        let role_cache = Arc::new(RoleCache::new());
        role_cache.sync_full(vec![super::super::role_cache::RoleEntry {
            name: "pki_reader".to_string(),
            grants: vec![super::super::role_cache::CapabilityGrant {
                capability_id: "pki:cert:read".to_string(),
                scope: "".to_string(),
            }],
            high_sensitive: false,
        }]);
        let mut svc = make_auth_service(AuthInterceptor::new(TEST_KEY.to_vec(), role_cache, 300));

        let cct = make_test_cct(vec!["pki_reader"], HashMap::new());
        let header = format!("Bearer {cct}");
        let resp = svc
            .call(make_http_request(
                "/coord.pki.v1.Pki/IssueCert",
                Some(&header),
            ))
            .await
            .expect("service 不应报传输错误");
        let grpc_status = resp
            .headers()
            .get("grpc-status")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        assert_eq!(
            grpc_status.as_deref(),
            Some("16"),
            "只读角色调用签发必须拒绝（grpc-status=UNAUTHENTICATED(16)）"
        );
    }

    #[test]
    fn test_extract_bearer_token_formats() {
        assert_eq!(
            extract_bearer_token(Some("Bearer mytoken")),
            Some("mytoken")
        );
        assert_eq!(
            extract_bearer_token(Some("eyJhbGciOiJI...")),
            Some("eyJhbGciOiJI...")
        );
        assert_eq!(extract_bearer_token(Some("coord_abc123")), None); // legacy — pass through
        assert_eq!(extract_bearer_token(None), None);
    }
    // ──── Ed25519 非对称验证（agent 仅存公钥）───

    fn reader_role_cache() -> Arc<RoleCache> {
        let role_cache = Arc::new(RoleCache::new());
        role_cache.sync_full(vec![super::super::role_cache::RoleEntry {
            name: "reader".to_string(),
            grants: vec![super::super::role_cache::CapabilityGrant {
                capability_id: "data:kv:read".to_string(),
                scope: "".to_string(),
            }],
            high_sensitive: false,
        }]);
        role_cache
    }

    fn ed_test_cct(signing_key: &ed25519_dalek::SigningKey, roles: Vec<&str>) -> String {
        let header = CctHeader::ed25519();
        let payload = CctPayload {
            jti: uuid::Uuid::new_v4().to_string(),
            iss: "coord-cluster".to_string(),
            sub: "test-app".to_string(),
            aud: vec!["coord-agent".to_string()],
            iat: 1719990000,
            exp: 2000000000,
            roles: roles.into_iter().map(|s| s.to_string()).collect(),
            scope_overrides: HashMap::new(),
        };
        coord_core::auth::cct::encode_cct_ed25519(&header, &payload, signing_key).unwrap()
    }

    #[test]
    fn test_interceptor_verifies_ed25519_cct() {
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&[9u8; 32]);
        let pub_key = signing_key.verifying_key().to_bytes().to_vec();
        let interceptor =
            AuthInterceptor::new(Vec::new(), reader_role_cache(), 300).with_verifying_key(pub_key);

        let cct = ed_test_cct(&signing_key, vec!["reader"]);
        let auth_header = format!("Bearer {cct}");
        let result = interceptor.validate_request("/coord.kv.KV/Range", Some(&auth_header), None);
        assert!(matches!(result, AuthResult::Allow(_)));
    }

    #[test]
    fn test_interceptor_ed25519_rejects_forged_token() {
        // 攻击者无 server 私钥：用自己的密钥签发的 token 必须被拒
        let legit_key = ed25519_dalek::SigningKey::from_bytes(&[9u8; 32]);
        let attacker_key = ed25519_dalek::SigningKey::from_bytes(&[42u8; 32]);
        let pub_key = legit_key.verifying_key().to_bytes().to_vec();
        let interceptor =
            AuthInterceptor::new(Vec::new(), reader_role_cache(), 300).with_verifying_key(pub_key);

        let forged = ed_test_cct(&attacker_key, vec!["root"]);
        let auth_header = format!("Bearer {forged}");
        let result = interceptor.validate_request("/coord.kv.KV/Range", Some(&auth_header), None);
        assert!(matches!(result, AuthResult::Deny(_)));
    }

    #[test]
    fn test_interceptor_ed25519_fail_closed_without_pubkey() {
        // 未配置公钥时 Ed25519 token 必须被拒（fail-closed）
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&[9u8; 32]);
        let interceptor = AuthInterceptor::new(Vec::new(), reader_role_cache(), 300);

        let cct = ed_test_cct(&signing_key, vec!["reader"]);
        let auth_header = format!("Bearer {cct}");
        let result = interceptor.validate_request("/coord.kv.KV/Range", Some(&auth_header), None);
        assert!(matches!(result, AuthResult::Deny(_)));
    }

    #[test]
    fn test_interceptor_hmac_still_accepted_grace_period() {
        // 宽限期：存量 HMAC token 仍可用（双算法并行）
        let interceptor = AuthInterceptor::new(TEST_KEY.to_vec(), reader_role_cache(), 300);
        let cct = make_test_cct(vec!["reader"], HashMap::new());
        let auth_header = format!("Bearer {cct}");
        let result = interceptor.validate_request("/coord.kv.KV/Range", Some(&auth_header), None);
        assert!(matches!(result, AuthResult::Allow(_)));
    }
}
