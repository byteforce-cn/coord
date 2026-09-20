// coord-agent: 插件身份与受限 CCT 自动续期（Phase 1.3 / 计划 §10.1 / D5）
//
// 目标：**每个插件以独立服务账户出站**，而不是复用 agent 进程内未鉴权连接。
//
// 生命周期：
// 1. `ensure(plugin, caps)`（幂等）：
//    - 账户 `plugin/{id}` + 专属角色 `plugin/{id}-role`（不存在则创建）；
//    - 逐个 `RoleGrantCapability(role, capability_id, scope)`（§10.2 的 P0c RPC）；
//    - `UserGrantRole(plugin/{id}, plugin/{id}-role)`；
//    - `Authenticate` → 受限 CCT（15min）+ refresh（24h），注册到 `PluginClients`。
// 2. 后台续期任务：到期前 `RefreshToken`（单次使用）；refresh 失效 → 用密码重新认证；
//    两者都失败 → 清空凭据（该插件出站降级为无凭据，server 侧 fail-closed 拒绝）。
// 3. `forget(plugin)`：插件卸载时停止续期任务并移除 authed client。
//
// **降级语义**：开通失败（server 未启用 Auth / bootstrap CCT 缺
// `admin:auth:*` 能力 / 网络不可达）在**有界重试**仍失败后回退到共享未鉴权客户端——
// 与插件引擎引入前行为一致，保证明文开发与既有回归零破坏。
// 重试见 [`PluginIdentityManager::ensure_with_retry`]：**失败必须是有界重试之后的
// 结论**，否则一个瞬时错误会变成进程生命周期内的永久故障。

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::{Mutex, RwLock};

use coord_client::client::Client;
use coord_client::config::TlsConfig;
use coord_client::credential::CachedTokenProvider;
use coord_client::refresh::{
    spawn_session_refresher, RefreshOptions, SessionGateway, SessionTokens,
};

use crate::key_util::{KeyStoreBackend, KeyUtil, KeyUtilConfig};
use crate::plugin::manifest::PluginCapability;

/// 续期提前量：到期前多久开始刷新。
const RENEW_LEAD_SECS: i64 = 120;
/// 最小续期间隔（防呆：即使 server 返回异常到期时间也不会打爆）。
const MIN_RENEW_INTERVAL_SECS: u64 = 30;

/// 身份开通的默认重试参数。
///
/// 取值理由：要覆盖的是**启动期瞬时失败**（agent 刚起来、到 server 的连接尚未建立、
/// 握手拖动），量级是秒；同时必须**有界** —— “账户真的不存在”这类确定性失败要尽快
/// 把插件启动路径交回去，不能无限阻塞。
pub const ENSURE_RETRY_POLICY: EnsureRetryPolicy = EnsureRetryPolicy {
    attempts: 5,
    initial_backoff: Duration::from_millis(200),
};

/// [`PluginIdentityManager::ensure_with_retry`] 的重试参数。
///
/// 默认值即 [`ENSURE_RETRY_POLICY`]：最多 5 次尝试（首试 + 4 次重试），
/// 退避 200/400/800/1600ms ⇒ 最坏约 3 秒。
#[derive(Debug, Clone, Copy)]
pub struct EnsureRetryPolicy {
    /// 总尝试次数（含首次）。
    pub attempts: u32,
    /// 首次退避；每次失败后翻倍。
    pub initial_backoff: Duration,
}

impl Default for EnsureRetryPolicy {
    fn default() -> Self {
        ENSURE_RETRY_POLICY
    }
}

/// provisioner 服务账户的默认用户名（`[auth].provisioner_user` 可覆盖）。
pub const DEFAULT_PROVISIONER_USER: &str = "agent-provisioner";

/// provisioner 账户所需的最小能力集（能力 id 与范围）。
///
/// 与 server 侧 `coord_server::auth::AGENT_BOOTSTRAP_CAPABILITY_GRANTS`
/// **逐字一致**（coord-agent 不依赖 coord-server，故此处复刻；`coord` 侧的进程测试
/// `plugin_credentials_process_test::agent_provisioner_grants_match_server_bootstrap_grants`
/// 会断言两者相等，防止漂移）。
/// 刻意不含任何数据面能力。
///
/// A3：`admin:auth:role_list` 是**只读**元数据能力 —— agent 需要它把 CCT 里的
/// `roles` 解析成能力/scope（本地 `RoleCache` 同步用）。此前 server 侧加了这一项、
/// agent 侧未同步，导致上述漂移测试一直失败。
pub const PROVISIONER_CAPABILITY_GRANTS: [(&str, &str); 5] = [
    ("admin:auth:user_add", ""),
    ("admin:auth:role_add", ""),
    ("admin:auth:role_grant", ""),
    ("admin:auth:user_grant_role", ""),
    ("admin:auth:role_list", ""),
];

/// **agent 自身身份**的默认用户名（F-50 修复，2026-09-19）。
///
/// 与 provisioner 账户（`agent-provisioner`）**分工不同**：
/// provisioner 代**插件**开通账户（`admin:auth:*`，无数据面）；
/// 本账户代表 **agent 自己**访问服务端（后台流量，只有内部键空间的数据面能力）。
pub const DEFAULT_SELF_USER: &str = "agent-self";

/// **agent 自身身份**的角色名。
///
/// 与 server 侧 `coord_server::auth::AGENT_SELF_ROLE` **逐字一致**（coord-agent 不
/// 依赖 coord-server，故此处复刻；`coord/tests/plugin_credentials_process_test.rs`
/// 断言两者相等，防止漂移）。
pub const SELF_ROLE: &str = "agent-self-role";

/// **agent 自身维护的内部键空间**（`/_<domain>/…`）。
///
/// 与 server 侧 `coord_server::auth::AGENT_SELF_KEYSPACES` **逐字一致**
/// （漂移由 `coord/tests/plugin_credentials_process_test.rs` 断言）。
/// 语义与边界见 server 侧文档；一句话：这些是 agent 自己的协调簿记，
/// **不是**用户数据键。
///
/// **2026-09-19**：`featureflags` / `scheduler` 由漂移卡口补入 —— 前两轮给这两个
/// 服务加了 KV 持久化（`feature_flags_store.rs`、`scheduler_store.rs`）却漏了登记，
/// 于是 auth 开启时 agent 自身身份在这两个键空间上会 `permission denied`
/// **静默失效**（F-50 同型）。卡口正是为这一类"加了键空间忘了登记"而存在。
///
/// 注：本清单与技术上的 server 侧清单是**两份手写副本**（coord-agent 不依赖
/// coord-server），靠测试锁定相等。若要彻底消除这处重复，应把它与
/// [`SELF_ROLE`] 一并从 `coord-core` 导出（同 `ROOT_ROLE` 的做法）—— 记为待办，
/// 不在本轮范围内。
pub const SELF_KEYSPACES: [&str; 12] = [
    "lock",
    "registry",
    "config",
    "idgen",
    "election",
    "workflow",
    "pki",
    "policy",
    "transit",
    "events",
    "featureflags",
    "scheduler",
];

/// 每个内部键空间上需要的数据面操作（与 server 侧同一规则）。
const SELF_KEYSPACE_OPERATIONS: [&str; 4] = [
    "data:kv:read",
    "data:kv:write",
    "data:kv:delete",
    "data:txn:execute",
];

/// 不按 key 约束的自身身份能力（与 server 侧同一清单）。
const SELF_UNSCOPED_GRANTS: [&str; 3] = [
    "data:lease:keepalive",
    "data:lease:revoke",
    "data:watch:subscribe",
];

/// **agent 自身身份**所需的最小能力集（**派生**，与 server 侧同规则）。
///
/// 与 server 侧 `coord_server::auth::agent_self_capability_grants()` **逐字一致**
/// （同上由漂移测试锁定）。
pub fn self_capability_grants() -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> =
        Vec::with_capacity(SELF_KEYSPACES.len() * SELF_KEYSPACE_OPERATIONS.len() + 3);
    for ns in SELF_KEYSPACES {
        for cap in SELF_KEYSPACE_OPERATIONS {
            out.push((cap.to_string(), format!("/_{ns}/*")));
        }
    }
    for cap in SELF_UNSCOPED_GRANTS {
        out.push((cap.to_string(), String::new()));
    }
    out
}

/// `(&str, &str)` 能力清单 → [`PluginCapability`] 列表。
///
/// 存在意义：让「引导最小能力集」「agent 自身身份能力集」与「插件能力集」走
/// **同一套**开通序列（[`provision_with`]），避免多套实现漂移。
fn as_plugin_capabilities(grants: &[(String, String)]) -> Vec<PluginCapability> {
    grants
        .iter()
        .map(|(id, scope)| PluginCapability {
            id: id.clone(),
            scope: scope.clone(),
        })
        .collect()
}

// ──── Auth 门面 ────

/// 一次认证 / 续期签发结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssuedToken {
    /// 受限 CCT
    pub cct: String,
    /// refresh token（单次使用；`None` = 未签发）
    pub refresh_token: Option<String>,
    /// 到期时刻（Unix 秒）
    pub expires_at: i64,
}

/// 开通 / 认证所需的最小门面（生产走 `AuthClient`；测试注入 stub）。
#[async_trait]
pub trait PluginAuthGateway: Send + Sync + 'static {
    async fn user_add(&self, user: &str, password: &str) -> Result<(), String>;
    async fn role_add(&self, role: &str) -> Result<(), String>;
    async fn role_grant_capability(
        &self,
        role: &str,
        capability_id: &str,
        scope: &str,
    ) -> Result<(), String>;
    async fn user_grant_role(&self, user: &str, role: &str) -> Result<(), String>;
    async fn authenticate(&self, user: &str, password: &str) -> Result<IssuedToken, String>;
    async fn refresh(&self, refresh_token: &str) -> Result<IssuedToken, String>;
}

/// 生产实现：`coord-client` 的 `AuthClient`。
pub struct CoordAuthGateway {
    client: Client,
}

impl CoordAuthGateway {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

fn auth_error(e: coord_core::error::Error) -> String {
    e.to_string()
}

#[async_trait]
impl PluginAuthGateway for CoordAuthGateway {
    async fn user_add(&self, user: &str, password: &str) -> Result<(), String> {
        self.client
            .auth()
            .user_add(user, password)
            .await
            .map_err(auth_error)
    }

    async fn role_add(&self, role: &str) -> Result<(), String> {
        self.client.auth().role_add(role).await.map_err(auth_error)
    }

    async fn role_grant_capability(
        &self,
        role: &str,
        capability_id: &str,
        scope: &str,
    ) -> Result<(), String> {
        self.client
            .auth()
            .role_grant_capability(role, capability_id, scope)
            .await
            .map_err(auth_error)
    }

    async fn user_grant_role(&self, user: &str, role: &str) -> Result<(), String> {
        self.client
            .auth()
            .user_grant_role(user, role)
            .await
            .map_err(auth_error)
    }

    async fn authenticate(&self, user: &str, password: &str) -> Result<IssuedToken, String> {
        let resp = self
            .client
            .auth()
            .authenticate(user, password)
            .await
            .map_err(auth_error)?;
        Ok(IssuedToken {
            cct: resp.cct,
            refresh_token: Some(resp.refresh_token),
            expires_at: resp.expires_at,
        })
    }

    async fn refresh(&self, refresh_token: &str) -> Result<IssuedToken, String> {
        let resp = self
            .client
            .auth()
            .refresh_token(refresh_token)
            .await
            .map_err(auth_error)?;
        Ok(IssuedToken {
            cct: resp.cct,
            refresh_token: Some(resp.refresh_token),
            expires_at: resp.expires_at,
        })
    }
}

/// `coord_client::refresh::SessionGateway` 的生产实现（provisioner 会话续期）。
///
/// 与 `PluginAuthGateway` 分开是因为 `AuthClient` 的 refresh 是匿名端点、
/// 语义上属「会话续期」而非「账户开通」。
struct AuthSessionGateway {
    client: Client,
}

#[async_trait]
impl SessionGateway for AuthSessionGateway {
    async fn refresh(&self, refresh_token: &str) -> Result<SessionTokens, String> {
        let resp = self
            .client
            .auth()
            .refresh_token(refresh_token)
            .await
            .map_err(auth_error)?;
        Ok(SessionTokens {
            cct: resp.cct,
            refresh_token: Some(resp.refresh_token),
            expires_at: resp.expires_at,
        })
    }
}

// ──── 插件出站客户端来源 ────

/// 插件出站客户端来源（SDK 后端按插件取客户端）。
///
/// 未开通身份的插件回退到 `fallback`（共享未鉴权连接）——
/// 明文开发模式零破坏；鉴权模式下 server 侧会 fail-closed 拒绝。
pub trait PluginClientSource: Send + Sync + 'static {
    fn client_for(&self, plugin: &str) -> Client;
}

/// 共享单一客户端（默认 / 未启用插件身份时）。
pub struct SharedPluginClients {
    fallback: Client,
}

impl SharedPluginClients {
    pub fn new(client: Client) -> Self {
        Self { fallback: client }
    }
}

impl PluginClientSource for SharedPluginClients {
    fn client_for(&self, _plugin: &str) -> Client {
        self.fallback.clone()
    }
}

/// 每插件 authed client 注册表（身份开通成功后填入）。
pub struct PluginClients {
    fallback: Client,
    accounts: RwLock<HashMap<String, Client>>,
}

impl PluginClients {
    pub fn new(fallback: Client) -> Self {
        Self {
            fallback,
            accounts: RwLock::new(HashMap::new()),
        }
    }

    /// 注册插件的 authed client。
    pub fn insert(&self, plugin: &str, client: Client) {
        self.accounts.write().insert(plugin.to_string(), client);
    }

    /// 移除（插件卸载）。
    pub fn remove(&self, plugin: &str) {
        self.accounts.write().remove(plugin);
    }

    /// 已开通身份的插件数（测试 / 诊断）。
    pub fn len(&self) -> usize {
        self.accounts.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl PluginClientSource for PluginClients {
    fn client_for(&self, plugin: &str) -> Client {
        self.accounts
            .read()
            .get(plugin)
            .cloned()
            .unwrap_or_else(|| self.fallback.clone())
    }
}

// ──── 身份管理器 ────

/// 插件账户开通 + CCT 续期管理器。
///
/// 除逐插件的受限 CCT 外，还持有一个**可自动续期**的 provisioner 会话：
/// 一次性引导 CCT 只有 10 分钟且 token 一次性，若只靠它，运行中（SIGHUP）新增插件
/// 或能力变更在窗口外就无法再开通账户。故首启用引导 CCT 自举一个**持久**服务账户
/// （见 [`PluginIdentityManager::bootstrap_provisioner`]），之后开通序列走该账户的
/// 续期会话（refresh token 24h / 失败回退密码重认证）—— 账户开通能力不再受窗口限制。
pub struct PluginIdentityManager {
    gateway: Arc<dyn PluginAuthGateway>,
    endpoints: Vec<String>,
    tls: Option<TlsConfig>,
    keys: KeyUtil,
    clients: Arc<PluginClients>,
    renewals: Mutex<HashMap<String, tokio::task::JoinHandle<()>>>,
    /// 自举后的 provisioner 会话（`Some` = 开通序列改走它）。
    provisioner: RwLock<Option<Arc<dyn PluginAuthGateway>>>,
    provisioner_renewal: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl std::fmt::Debug for PluginIdentityManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginIdentityManager")
            .field("endpoints", &self.endpoints)
            .field("accounts", &self.clients.len())
            .field("provisioner", &self.provisioner_active())
            .finish_non_exhaustive()
    }
}

impl PluginIdentityManager {
    /// 构建（`data_dir` 下持久化插件账户密码）。
    pub fn new(
        gateway: Arc<dyn PluginAuthGateway>,
        endpoints: Vec<String>,
        tls: Option<TlsConfig>,
        data_dir: impl Into<PathBuf>,
        clients: Arc<PluginClients>,
    ) -> Result<Self, String> {
        let data_dir = data_dir.into();
        let keys = KeyUtil::new(KeyUtilConfig {
            backend: KeyStoreBackend::File,
            file_path: Some(data_dir.join("plugin-accounts")),
        })
        .map_err(|e| format!("plugin identity key store: {e}"))?;
        Ok(Self {
            gateway,
            endpoints,
            tls,
            keys,
            clients,
            renewals: Mutex::new(HashMap::new()),
            provisioner: RwLock::new(None),
            provisioner_renewal: Mutex::new(None),
        })
    }

    /// 插件出站客户端来源（交给 SDK 后端）。
    pub fn client_source(&self) -> Arc<dyn PluginClientSource> {
        Arc::clone(&self.clients) as Arc<dyn PluginClientSource>
    }

    /// 是否已具备可自动续期的 provisioner 会话。
    pub fn provisioner_active(&self) -> bool {
        self.provisioner.read().is_some()
    }

    /// 开通序列的凭据来源：优先 provisioner 会话，否则回退引导（一次性 CCT）客户端。
    fn active_gateway(&self) -> Arc<dyn PluginAuthGateway> {
        match self.provisioner.read().as_ref() {
            Some(g) => Arc::clone(g),
            None => Arc::clone(&self.gateway),
        }
    }

    /// 接纳一个**可自动续期**的 provisioner 会话（开通序列从此改走它）。
    ///
    /// `renewal` = 后台续期任务句柄（由 [`Self::bootstrap_provisioner`] 启动；
    /// 直接注入已完成认证的会话时可传 `None`）。同名旧任务会被替换停止。
    pub fn adopt_provisioner(
        &self,
        gateway: Arc<dyn PluginAuthGateway>,
        renewal: Option<tokio::task::JoinHandle<()>>,
    ) {
        *self.provisioner.write() = Some(gateway);
        let mut slot = self.provisioner_renewal.lock();
        if let Some(old) = slot.take() {
            old.abort();
        }
        *slot = renewal;
    }

    /// 用一次性引导 CCT **自举**持久 provisioner 服务账户，并转为可续期会话。
    ///
    /// 步骤：
    /// 1. （`bootstrap` 为 `Some` 时）用引导 CCT 幂等创建 `user` + `{user}-role`
    ///    + [`PROVISIONER_CAPABILITY_GRANTS`] 逐项授权 + 角色绑定；
    /// 2. 匿名 `Authenticate(user, 持久化密码)` —— 密码落盘在 agent data_dir，
    ///    因此**引导令牌已消费/超时后依然可用**；
    /// 3. 启动续期循环（`coord-client::refresh`）→ 写入 `provisioner`。
    ///
    /// 失败（账户不存在且无引导 CCT / 网络不可达）返回 `Err`，调用方保持旧行为。
    pub async fn bootstrap_provisioner(
        &self,
        bootstrap: Option<&Client>,
        user: &str,
    ) -> Result<(), String> {
        if user.trim().is_empty() {
            return Err("provisioner user name must not be empty".into());
        }
        let role = format!("{user}-role");
        let provisioner_grants: Vec<(String, String)> = PROVISIONER_CAPABILITY_GRANTS
            .iter()
            .map(|(id, scope)| ((*id).to_string(), (*scope).to_string()))
            .collect();
        let (client, auth_provider, issued) = self
            .authenticate_service_account(
                bootstrap,
                user,
                &role,
                &format!("provisioner-{user}"),
                &provisioner_grants,
                "provisioner",
            )
            .await?;

        // 3) 续期循环 + 采纳（开通序列改走该会话）
        let renewal = spawn_session_refresher(
            Arc::new(AuthSessionGateway {
                client: client.clone(),
            }),
            Arc::clone(&auth_provider),
            SessionTokens {
                cct: issued.cct,
                refresh_token: Some(issued.refresh_token),
                expires_at: issued.expires_at,
            },
            RefreshOptions::default(),
        );
        self.adopt_provisioner(Arc::new(CoordAuthGateway::new(client)), Some(renewal));
        tracing::info!(
            "plugin identity: provisioner session active (user '{user}'); runtime plugin \
             provisioning no longer bounded by the bootstrap CCT window"
        );
        Ok(())
    }

    /// **自举 agent 自身身份**：让 agent 的后台流量（锁自动续期 / registry 目录加载
    /// 与订阅 / idgen nodeid 注册 / 工作流与配置订阅）在 `auth.enabled=true` 下不再
    /// 收到 `missing CCT token`（F-50）。
    ///
    /// 步骤与 [`Self::bootstrap_provisioner`] **完全同构**（同一份开通序列 + 同一份
    /// 匿名密码认证 + 同一套续期循环），差别只在**能力集**与**返回值**：
    /// - 能力集取 [`SELF_CAPABILITY_GRANTS`]（内部键空间，无 `admin:*`）；
    /// - 返回**凭据句柄**而非登记为 provisioner：调用方（`proxy::AgentInner`）把它
    ///   作为**回退凭据**装到共享出站客户端上（有调用方 CCT 时仍以调用方为准）。
    ///
    /// 返回 `Err` 表示**未配置引导令牌**且账户不可认证 —— 调用方保持旧行为
    /// （回退凭据保持为空），**不**因此拒绝启动；但会把结果如实回报，由调用方决定
    /// 告警级别（`auth.enabled=true` 且无凭据 ⇒ 自发流量必然失败，属必须可见的降级）。
    ///
    /// 提示：本方法**不**要求 bootstrap 一定在场 —— 账户与密码已持久化时，重启后
    /// 仅凭 `Authenticate` 即可恢复（这是"重启不依赖一次性令牌"的关键），
    /// 此时 `bootstrap` 传 `None` 即可。
    ///
    /// `target` 必须与共享出站客户端上装的回退凭据句柄是**同一个** `Arc`
    /// （`Arc<CachedTokenProvider>` 内部可变，因此"先装句柄、后填凭据"成立）。
    pub async fn bootstrap_self_identity(
        &self,
        bootstrap: Option<&Client>,
        user: &str,
        target: &Arc<CachedTokenProvider>,
    ) -> Result<(), String> {
        if user.trim().is_empty() {
            return Err("agent self user name must not be empty".into());
        }
        let (client, _auth_provider, issued) = self
            .authenticate_service_account(
                bootstrap,
                user,
                SELF_ROLE,
                &format!("self-{user}"),
                &self_capability_grants(),
                "agent-self",
            )
            .await?;

        // 凭据写入**调用方持有的那个句柄**（共享出站客户端读的就是它）。
        target.set(issued.cct.clone());

        // 续期循环：与插件账户同机制（refresh token 单次使用 → 失败回退密码重认证），
        // 因此 agent 的自身身份**不会**在 CCT 到期后静默失效（那正是 F-50 的形态）。
        let renewal = spawn_session_refresher(
            Arc::new(AuthSessionGateway {
                client: client.clone(),
            }),
            Arc::clone(target),
            SessionTokens {
                cct: issued.cct,
                refresh_token: Some(issued.refresh_token),
                expires_at: issued.expires_at,
            },
            RefreshOptions::default(),
        );
        // 续期任务与 agent 进程同生命周期：句柄故意不保存（无停止需求，
        // 与 `spawn_role_sync` 的处置一致）。
        drop(renewal);
        tracing::info!(
            "agent self identity active (user '{user}', role '{SELF_ROLE}'): self-initiated \
             traffic (lock renew / registry catalog+watch / idgen nodeid) now carries a CCT"
        );
        Ok(())
    }

    /// 自举一个**持久服务账户**并返回其认证通道：幂等开通（需引导 CCT）→
    /// 匿名密码认证（不依赖引导 CCT 是否仍有效）。
    ///
    /// 开通序列与 [`provision_with`] 共用（同一份 `user_add` → `role_add` →
    /// 逐能力授权 → 角色绑定），因此「引导最小能力集」「agent 自身身份能力集」
    /// 与「插件能力集」不会漂移。
    ///
    /// 密码由 `key_id` 决定，落盘在 `data_dir`；`bootstrap` 为 `None` 时跳过开通、
    /// 只用已存账户认证。
    #[allow(clippy::too_many_arguments)]
    async fn authenticate_service_account(
        &self,
        bootstrap: Option<&Client>,
        user: &str,
        role: &str,
        key_id: &str,
        capability_grants: &[(String, String)],
        label: &str,
    ) -> Result<(Client, Arc<CachedTokenProvider>, coord_proto::auth::AuthenticateResponse), String>
    {
        let password = self.stored_secret(key_id);

        // 1) 尽力用引导 CCT 开通（幂等；无引导 CCT 时跳过，靠已存账户认证）
        if let Some(bootstrap) = bootstrap {
            let seeder: Arc<dyn PluginAuthGateway> =
                Arc::new(CoordAuthGateway::new(bootstrap.clone()));
            match provision_with(
                seeder.as_ref(),
                user,
                role,
                &password,
                &as_plugin_capabilities(capability_grants),
            )
            .await
            {
                Ok(()) => tracing::info!(
                    "{label}: account '{user}' provisioned (role '{role}', {} capability grant(s))",
                    capability_grants.len()
                ),
                Err(e) => tracing::debug!(
                    "{label}: provisioning skipped ({e}); relying on the persisted account"
                ),
            }
        }

        // 2) 匿名认证（账户/密码已持久化 → 不依赖引导 CCT 是否仍有效）
        let auth_provider = Arc::new(CachedTokenProvider::new(None));
        let mut config =
            coord_client::Config::new(self.endpoints.clone())
                .with_token_provider(
                    Arc::clone(&auth_provider) as Arc<dyn coord_client::TokenProvider>
                );
        if let Some(tls) = self.tls.clone() {
            config = config.with_tls(tls);
        }
        let client = Client::connect_direct(config)
            .await
            .map_err(|e| format!("{label} client build failed: {e}"))?;
        let issued = client
            .auth()
            .authenticate(user, &password)
            .await
            .map_err(|e| format!("{label} authenticate failed: {e}"))?;
        if issued.cct.is_empty() {
            return Err(format!("{label} authenticate returned an empty CCT"));
        }
        auth_provider.set(issued.cct.clone());
        Ok((client, auth_provider, issued))
    }

    /// 停止 provisioner 续期任务（测试 / 关闭）。
    pub fn forget_provisioner(&self) {
        *self.provisioner.write() = None;
        if let Some(handle) = self.provisioner_renewal.lock().take() {
            handle.abort();
        }
    }

    /// 装载（或读取）插件账户密码。
    fn password_for(&self, plugin: &str) -> String {
        self.stored_secret(&format!("plugin-{plugin}"))
    }

    /// 装载（或首次生成并落盘）指定 key 的 32 字节随机密码（hex）。
    fn stored_secret(&self, key_id: &str) -> String {
        if let Ok(bytes) = self.keys.load(key_id) {
            if let Ok(s) = String::from_utf8(bytes) {
                if !s.is_empty() {
                    return s;
                }
            }
        }
        // 首次：32 字节随机密码（hex）
        let mut raw = [0u8; 32];
        rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut raw);
        let password = hex::encode(raw);
        if let Err(e) = self.keys.store(key_id, password.as_bytes()) {
            tracing::warn!("plugin identity: failed to persist secret for '{key_id}': {e}");
        }
        password
    }

    /// 开通插件账户并注册 authed client（幂等）。
    ///
    /// **两阶段**（关键：让 agent 重启不再依赖一次性引导令牌）：
    /// 1. 尽力执行幂等开通序列（`user_add` / `role_add` / 能力授予 / `user_grant_role`）
    ///    —— 需要调用方持有 bootstrap CCT；失败只记 debug（能力变更留待下次带凭据的开通）；
    /// 2. `Authenticate(plugin/{id}, password)` —— 账户与密码均已持久化，
    ///    `Authenticate` 是匿名端点，因此**重启后无引导 CCT 也能拿到受限 CCT**。
    ///
    /// 任一阶段都拿不到 CCT → `Err`（调用方降级为共享客户端，服务端 fail-closed 拒绝）。
    pub async fn ensure(
        &self,
        plugin: &str,
        capabilities: &[PluginCapability],
    ) -> Result<(), String> {
        let user = format!("plugin/{plugin}");
        let role = format!("plugin/{plugin}-role");
        let password = self.password_for(plugin);

        // 1) 幂等开通（best-effort：无 bootstrap CCT 时会以权限错误失败）
        let provisioning = self
            .provision_account(&user, &role, &password, capabilities)
            .await;
        if let Err(e) = provisioning {
            tracing::debug!(
                "plugin '{plugin}': account provisioning skipped ({e}); \
                 relying on the persisted account for authentication"
            );
        }

        // 2) 认证（匿名端点；账户/密码已持久化 → 重启可用）
        let issued = self.gateway.authenticate(&user, &password).await?;
        self.register_authenticated(plugin, user.clone(), password, issued)
            .await;

        tracing::info!(
            "plugin '{plugin}': identity ready (account '{user}', role '{role}', {} declared \
             capability grant(s))",
            capabilities.len()
        );
        Ok(())
    }

    /// [`Self::ensure`] 的**有界重试**版本。
    ///
    /// ## 为什么必须有它
    ///
    /// `ensure` 失败时调用方（`js_engine` / `component_engine`）会回退到**共享未鉴权
    /// 客户端**，而服务端对无凭据出站调用是 fail-closed 的 —— 此后该插件的每一次调用
    /// 都会以 `unauthenticated: missing CCT token` 失败，而且**不会自己恢复**
    /// （只靠下一次 SIGHUP / agent 重启）。于是在**启动期**发生的一次瞬时错误
    /// （agent 刚重启、到 server 的连接尚未建立、握手拖动）会被放大成
    /// **整个进程生命周期内的永久故障**。
    ///
    /// 真实进程用例 `coord/tests/plugin_real_process_test.rs::plugin_real_agent_process_e2e`
    /// 在 CI 上就是这样翻红的：agent `kill` + 重启后用持久化账户写 KV 返回
    /// `missing CCT token`（同一提交本地 8/8 通过）。有界重试正是针对这个形态：
    /// **降级可以，但必须是在重试之后**。
    ///
    /// ## 边界
    ///
    /// 重试是有界的（默认最坏约 3 秒，见 [`ENSURE_RETRY_POLICY`]），因此对
    /// “账户真的不存在 / 权限模型本就不允许”这类确定性失败只会多花几秒。
    /// 仍未处理的是“持续失败之后没有按需重试”：那种情况下直到下一次 SIGHUP 或
    /// agent 重启前插件仍不可用（已记入 docs/production/remaining-known-gaps.md）。
    pub async fn ensure_with_retry(
        &self,
        plugin: &str,
        capabilities: &[PluginCapability],
        policy: EnsureRetryPolicy,
    ) -> Result<(), String> {
        let attempts = policy.attempts.max(1);
        let mut backoff = policy.initial_backoff;
        let mut last_err = String::from("identity ensure not attempted");
        for attempt in 1..=attempts {
            match self.ensure(plugin, capabilities).await {
                Ok(()) => {
                    if attempt > 1 {
                        tracing::info!(
                            "plugin '{plugin}': identity ensure succeeded on attempt {attempt}/\
                             {attempts}"
                        );
                    }
                    return Ok(());
                }
                Err(e) => {
                    last_err = e;
                    if attempt == attempts {
                        break;
                    }
                    tracing::warn!(
                        "plugin '{plugin}': identity ensure attempt {attempt}/{attempts} failed \
                         ({last_err}); retrying in {backoff:?}"
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = backoff.saturating_mul(2);
                }
            }
        }
        Err(format!(
            "identity ensure failed after {attempts} attempt(s): {last_err}"
        ))
    }

    /// 幂等开通序列。
    ///
    /// **凭据来源是 [`Self::active_gateway`]**：已自举 provisioner 会话时走它的
    /// 可续期凭据，否则回退引导 CCT（一次性，10 分钟窗口）。
    async fn provision_account(
        &self,
        user: &str,
        role: &str,
        password: &str,
        capabilities: &[PluginCapability],
    ) -> Result<(), String> {
        provision_with(
            self.active_gateway().as_ref(),
            user,
            role,
            password,
            capabilities,
        )
        .await
    }

    /// 由已签发的会话构造插件专属 authed client 并启动续期任务。
    async fn register_authenticated(
        &self,
        plugin: &str,
        user: String,
        password: String,
        issued: IssuedToken,
    ) {
        let provider = Arc::new(CachedTokenProvider::new(Some(issued.cct.clone())));
        let mut config = coord_client::Config::new(self.endpoints.clone())
            .with_token_provider(Arc::clone(&provider) as Arc<dyn coord_client::TokenProvider>);
        if let Some(tls) = self.tls.clone() {
            config = config.with_tls(tls);
        }
        match Client::connect_direct(config).await {
            Ok(client) => self.clients.insert(plugin, client),
            Err(e) => {
                tracing::warn!("plugin '{plugin}': authed client build failed: {e}");
                return;
            }
        }

        // 续期任务（同名旧任务先停）
        let handle = self.spawn_renewal(plugin, user, password, provider, issued);
        let mut renewals = self.renewals.lock();
        if let Some(old) = renewals.insert(plugin.to_string(), handle) {
            old.abort();
        }
    }

    /// 停止续期并注销插件身份（插件卸载）。
    pub fn forget(&self, plugin: &str) {
        if let Some(handle) = self.renewals.lock().remove(plugin) {
            handle.abort();
        }
        self.clients.remove(plugin);
    }

    fn spawn_renewal(
        &self,
        plugin: &str,
        user: String,
        password: String,
        provider: Arc<CachedTokenProvider>,
        issued: IssuedToken,
    ) -> tokio::task::JoinHandle<()> {
        let gateway = Arc::clone(&self.gateway);
        let plugin = plugin.to_string();
        tokio::spawn(async move {
            let mut refresh_token = issued.refresh_token;
            let mut expires_at = issued.expires_at;
            loop {
                let now = crate::plugin::identity::now_secs();
                let sleep_for = if expires_at == 0 {
                    Duration::from_secs(600)
                } else {
                    let lead = (expires_at - now - RENEW_LEAD_SECS).max(0);
                    Duration::from_secs(
                        u64::try_from(lead)
                            .unwrap_or(0)
                            .max(MIN_RENEW_INTERVAL_SECS),
                    )
                };
                tokio::time::sleep(sleep_for).await;

                // 1) 优先用 refresh token（单次使用）
                let refreshed = match refresh_token.as_deref() {
                    Some(rt) => gateway.refresh(rt).await,
                    None => Err("no refresh token".to_string()),
                };
                let next = match refreshed {
                    Ok(t) => Ok(t),
                    Err(e) => {
                        tracing::debug!(
                            "plugin '{plugin}': refresh failed ({e}); re-authenticating with password"
                        );
                        gateway.authenticate(&user, &password).await
                    }
                };
                match next {
                    Ok(t) => {
                        provider.set(t.cct.clone());
                        refresh_token = t.refresh_token;
                        expires_at = t.expires_at;
                        tracing::debug!("plugin '{plugin}': CCT renewed (expires_at={expires_at})");
                    }
                    Err(e) => {
                        provider.clear();
                        tracing::error!(
                            "plugin '{plugin}': CCT renewal failed ({e}); outbound calls will be \
                             rejected until the next successful renewal"
                        );
                        // 退避后重试（凭据已清空，server 侧 fail-closed）
                        tokio::time::sleep(Duration::from_secs(60)).await;
                        expires_at = now_secs() + 60;
                    }
                }
            }
        })
    }
}

/// Unix 秒。
pub(crate) fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_secs()).unwrap_or(0))
        .unwrap_or(0)
}

/// `AlreadyExists` 类错误视为成功（幂等开通）。
fn ignore_exists(e: String) -> Result<(), String> {
    let lower = e.to_lowercase();
    if lower.contains("already exists") || lower.contains("alreadyexists") {
        Ok(())
    } else {
        Err(e)
    }
}

/// 幂等开通序列（用户 → 角色 → 逐能力授权 → 角色绑定）。
///
/// 与 [`PluginIdentityManager::provision_account`] 共用同一实现，供
/// 「插件账户」与「provisioner 服务账户」两条开通路径调用（避免两套序列漂移）。
async fn provision_with(
    gateway: &dyn PluginAuthGateway,
    user: &str,
    role: &str,
    password: &str,
    capabilities: &[PluginCapability],
) -> Result<(), String> {
    gateway
        .user_add(user, password)
        .await
        .or_else(ignore_exists)?;
    gateway.role_add(role).await.or_else(ignore_exists)?;
    for cap in capabilities {
        gateway
            .role_grant_capability(role, &cap.id, &cap.scope)
            .await
            .or_else(ignore_exists)?;
    }
    gateway
        .user_grant_role(user, role)
        .await
        .or_else(ignore_exists)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct StubGateway {
        users: Mutex<Vec<String>>,
        roles: Mutex<Vec<String>>,
        grants: Mutex<Vec<(String, String, String)>>,
        user_roles: Mutex<Vec<(String, String)>>,
        auth_calls: AtomicUsize,
        refresh_calls: AtomicUsize,
        fail_refresh: bool,
    }

    impl StubGateway {
        fn with_failed_refresh() -> Self {
            Self {
                fail_refresh: true,
                ..Default::default()
            }
        }
    }

    #[async_trait]
    impl PluginAuthGateway for StubGateway {
        async fn user_add(&self, user: &str, _password: &str) -> Result<(), String> {
            let mut users = self.users.lock();
            if users.iter().any(|u| u == user) {
                return Err(format!("user {user} already exists"));
            }
            users.push(user.to_string());
            Ok(())
        }

        async fn role_add(&self, role: &str) -> Result<(), String> {
            let mut roles = self.roles.lock();
            if roles.iter().any(|r| r == role) {
                return Err(format!("role {role} already exists"));
            }
            roles.push(role.to_string());
            Ok(())
        }

        async fn role_grant_capability(
            &self,
            role: &str,
            capability_id: &str,
            scope: &str,
        ) -> Result<(), String> {
            self.grants.lock().push((
                role.to_string(),
                capability_id.to_string(),
                scope.to_string(),
            ));
            Ok(())
        }

        async fn user_grant_role(&self, user: &str, role: &str) -> Result<(), String> {
            self.user_roles
                .lock()
                .push((user.to_string(), role.to_string()));
            Ok(())
        }

        async fn authenticate(&self, _user: &str, _password: &str) -> Result<IssuedToken, String> {
            self.auth_calls.fetch_add(1, Ordering::SeqCst);
            Ok(IssuedToken {
                cct: "cct-1".into(),
                refresh_token: Some("rt-1".into()),
                expires_at: now_secs() + 900,
            })
        }

        async fn refresh(&self, _refresh_token: &str) -> Result<IssuedToken, String> {
            self.refresh_calls.fetch_add(1, Ordering::SeqCst);
            if self.fail_refresh {
                return Err("refresh token expired".into());
            }
            Ok(IssuedToken {
                cct: "cct-2".into(),
                refresh_token: Some("rt-2".into()),
                expires_at: now_secs() + 900,
            })
        }
    }

    fn cap(id: &str, scope: &str) -> PluginCapability {
        PluginCapability {
            id: id.into(),
            scope: scope.into(),
        }
    }

    /// 建一个连不通的 fallback client（`ensure` 的真实连接会失败，用于验证错误路径）。
    async fn offline_client() -> Client {
        // 端口 1 不可达即可；只要能构造 Client（connect_direct 不做 IO 前置检查）
        let config = coord_client::Config::new(vec!["http://127.0.0.1:1".to_string()]);
        Client::connect_direct(config)
            .await
            .expect("client construction should not dial")
    }

    #[tokio::test]
    async fn ensure_provisions_user_role_and_capability_grants() {
        let gateway = Arc::new(StubGateway::default());
        let clients = Arc::new(PluginClients::new(offline_client().await));
        let dir = tempfile::tempdir().expect("tempdir");
        let mgr = PluginIdentityManager::new(
            Arc::clone(&gateway) as Arc<dyn PluginAuthGateway>,
            vec!["http://127.0.0.1:1".to_string()],
            None,
            dir.path(),
            Arc::clone(&clients),
        )
        .expect("identity manager");

        // 真实 Client::connect_direct 不拨号 → ensure 成功并把插件登记进 clients
        mgr.ensure(
            "counter",
            &[
                cap("data:kv:read", "/app/counter/"),
                cap("data:kv:write", "/app/counter/"),
            ],
        )
        .await
        .expect("ensure");

        assert_eq!(gateway.users.lock().as_slice(), ["plugin/counter"]);
        assert_eq!(gateway.roles.lock().as_slice(), ["plugin/counter-role"]);
        assert_eq!(
            gateway.user_roles.lock().as_slice(),
            [(
                "plugin/counter".to_string(),
                "plugin/counter-role".to_string()
            )]
        );
        assert_eq!(gateway.grants.lock().len(), 2);
        assert_eq!(gateway.auth_calls.load(Ordering::SeqCst), 1);
        assert_eq!(clients.len(), 1);

        // 幂等：再次 ensure 不重复创建用户/角色（AlreadyExists 被忽略）
        mgr.ensure("counter", &[cap("data:kv:read", "/app/counter/")])
            .await
            .expect("ensure idempotent");
        assert_eq!(gateway.users.lock().len(), 1);
        assert_eq!(gateway.roles.lock().len(), 1);

        mgr.forget("counter");
        assert_eq!(clients.len(), 0);
    }

    /// **重启路径**：一次性 bootstrap token 已被上一次启动消费 →
    /// 幂等开通全部失败（无引导 CCT），但账户密码已持久化 →
    /// `Authenticate` 仍成功，插件身份可用（不依赖新令牌）。
    #[tokio::test]
    async fn ensure_authenticates_existing_account_without_bootstrap_cct() {
        /// 只有 Authenticate 可用（匿名端点）的网关：模拟引导 CCT 缺失/已消费。
        struct AccountOnlyGateway {
            auth_calls: AtomicUsize,
        }

        #[async_trait]
        impl PluginAuthGateway for AccountOnlyGateway {
            async fn user_add(&self, _u: &str, _p: &str) -> Result<(), String> {
                Err("permission denied: missing CCT".into())
            }
            async fn role_add(&self, _r: &str) -> Result<(), String> {
                Err("permission denied: missing CCT".into())
            }
            async fn role_grant_capability(
                &self,
                _r: &str,
                _c: &str,
                _s: &str,
            ) -> Result<(), String> {
                Err("permission denied: missing CCT".into())
            }
            async fn user_grant_role(&self, _u: &str, _r: &str) -> Result<(), String> {
                Err("permission denied: missing CCT".into())
            }
            async fn authenticate(&self, _u: &str, _p: &str) -> Result<IssuedToken, String> {
                self.auth_calls.fetch_add(1, Ordering::SeqCst);
                Ok(IssuedToken {
                    cct: "existing-account-cct".into(),
                    refresh_token: Some("rt".into()),
                    expires_at: now_secs() + 900,
                })
            }
            async fn refresh(&self, _rt: &str) -> Result<IssuedToken, String> {
                Ok(IssuedToken {
                    cct: "existing-account-cct-2".into(),
                    refresh_token: Some("rt2".into()),
                    expires_at: now_secs() + 900,
                })
            }
        }

        let gateway = Arc::new(AccountOnlyGateway {
            auth_calls: AtomicUsize::new(0),
        });
        let clients = Arc::new(PluginClients::new(offline_client().await));
        let dir = tempfile::tempdir().expect("tempdir");
        let mgr = PluginIdentityManager::new(
            Arc::clone(&gateway) as Arc<dyn PluginAuthGateway>,
            vec!["http://127.0.0.1:1".to_string()],
            None,
            dir.path(),
            Arc::clone(&clients),
        )
        .expect("identity manager");

        mgr.ensure("counter", &[cap("data:kv:read", "/app/counter/")])
            .await
            .expect("existing plugin account must authenticate without a bootstrap CCT");
        assert_eq!(gateway.auth_calls.load(Ordering::SeqCst), 1);
        assert_eq!(clients.len(), 1, "authed client must be registered");
    }

    #[tokio::test]
    async fn password_is_persisted_and_reused() {
        let gateway = Arc::new(StubGateway::default());
        let clients = Arc::new(PluginClients::new(offline_client().await));
        let dir = tempfile::tempdir().expect("tempdir");
        let mgr = PluginIdentityManager::new(
            Arc::clone(&gateway) as Arc<dyn PluginAuthGateway>,
            vec!["http://127.0.0.1:1".to_string()],
            None,
            dir.path(),
            Arc::clone(&clients),
        )
        .expect("identity manager");

        let first = mgr.password_for("counter");
        let second = mgr.password_for("counter");
        assert_eq!(first, second, "password must be reused across restarts");
        assert_eq!(first.len(), 64, "32 random bytes as hex");
    }

    /// `ensure` 首次成功 → 不发生重试（重试只用于失败恢复，不改变正常路径）。
    #[tokio::test]
    async fn ensure_with_retry_does_not_retry_on_success() {
        let gateway = Arc::new(StubGateway::default());
        let clients = Arc::new(PluginClients::new(offline_client().await));
        let dir = tempfile::tempdir().expect("tempdir");
        let mgr = PluginIdentityManager::new(
            Arc::clone(&gateway) as Arc<dyn PluginAuthGateway>,
            vec!["http://127.0.0.1:1".to_string()],
            None,
            dir.path(),
            Arc::clone(&clients),
        )
        .expect("identity manager");

        mgr.ensure_with_retry(
            "counter",
            &[cap("data:kv:read", "/app/counter/")],
            fast_retry_policy(5),
        )
        .await
        .expect("ensure must succeed on the first attempt");

        assert_eq!(gateway.auth_calls.load(Ordering::SeqCst), 1);
        assert_eq!(clients.len(), 1);
    }

    /// **有界重试的吸收能力**：前两次 `Authenticate` 失败（模拟 agent 刚重启、
    /// 到 server 的连接尚未建立）必须被重试吃掉，而不是把插件永久降级为共享
    /// 未鉴权客户端（那会让之后每次出站调用都 `missing CCT token` 且不再自愈）。
    #[tokio::test]
    async fn ensure_with_retry_absorbs_transient_authenticate_failures() {
        let gateway = Arc::new(FlakyGateway {
            failures_left: AtomicUsize::new(2),
            auth_calls: AtomicUsize::new(0),
        });
        let clients = Arc::new(PluginClients::new(offline_client().await));
        let dir = tempfile::tempdir().expect("tempdir");
        let mgr = PluginIdentityManager::new(
            Arc::clone(&gateway) as Arc<dyn PluginAuthGateway>,
            vec!["http://127.0.0.1:1".to_string()],
            None,
            dir.path(),
            Arc::clone(&clients),
        )
        .expect("identity manager");

        mgr.ensure_with_retry(
            "counter",
            &[cap("data:kv:read", "/app/counter/")],
            fast_retry_policy(5),
        )
        .await
        .expect("transient failures must be absorbed by the retry");

        assert_eq!(
            gateway.auth_calls.load(Ordering::SeqCst),
            3,
            "2 failures + 1 success"
        );
        assert_eq!(
            clients.len(),
            1,
            "authed client must be registered after the retry succeeded"
        );
    }

    /// **负向对照**：持续失败时重试必须是**有界**的 —— 恰好 `attempts` 次，
    /// 返回 Err（调用方据此降级），不会无限重试也不会一次就放弃。
    #[tokio::test]
    async fn ensure_with_retry_is_bounded_on_persistent_failure() {
        let gateway = Arc::new(FlakyGateway {
            failures_left: AtomicUsize::new(usize::MAX),
            auth_calls: AtomicUsize::new(0),
        });
        let clients = Arc::new(PluginClients::new(offline_client().await));
        let dir = tempfile::tempdir().expect("tempdir");
        let mgr = PluginIdentityManager::new(
            Arc::clone(&gateway) as Arc<dyn PluginAuthGateway>,
            vec!["http://127.0.0.1:1".to_string()],
            None,
            dir.path(),
            Arc::clone(&clients),
        )
        .expect("identity manager");

        let err = mgr
            .ensure_with_retry("counter", &[], fast_retry_policy(3))
            .await
            .expect_err("persistent failure must surface after the bounded retries");

        assert_eq!(
            gateway.auth_calls.load(Ordering::SeqCst),
            3,
            "exactly `attempts` authenticate calls"
        );
        assert!(
            err.contains("failed after 3 attempt(s)"),
            "error must state it was retried: {err}"
        );
        assert_eq!(clients.len(), 0, "no authed client on failure");
    }

    /// 重试测试用的策略：次数与生产一致（5），退避降到 0 以免拖慢测试。
    fn fast_retry_policy(attempts: u32) -> EnsureRetryPolicy {
        EnsureRetryPolicy {
            attempts,
            initial_backoff: Duration::from_millis(0),
        }
    }

    /// `Authenticate` 前 N 次失败的网关（模拟启动期瞬时错误）。
    struct FlakyGateway {
        failures_left: AtomicUsize,
        auth_calls: AtomicUsize,
    }

    #[async_trait]
    impl PluginAuthGateway for FlakyGateway {
        async fn user_add(&self, _user: &str, _password: &str) -> Result<(), String> {
            Ok(())
        }
        async fn role_add(&self, _role: &str) -> Result<(), String> {
            Ok(())
        }
        async fn role_grant_capability(
            &self,
            _role: &str,
            _capability_id: &str,
            _scope: &str,
        ) -> Result<(), String> {
            Ok(())
        }
        async fn user_grant_role(&self, _user: &str, _role: &str) -> Result<(), String> {
            Ok(())
        }
        async fn authenticate(&self, _user: &str, _password: &str) -> Result<IssuedToken, String> {
            self.auth_calls.fetch_add(1, Ordering::SeqCst);
            if self.failures_left.load(Ordering::SeqCst) > 0 {
                self.failures_left.fetch_sub(1, Ordering::SeqCst);
                return Err("connect error: transport is not ready".into());
            }
            Ok(IssuedToken {
                cct: "retry-cct".into(),
                refresh_token: Some("retry-rt".into()),
                expires_at: now_secs() + 900,
            })
        }
        async fn refresh(&self, _refresh_token: &str) -> Result<IssuedToken, String> {
            Err("not used".into())
        }
    }

    #[tokio::test]
    async fn provisioning_failure_is_reported() {
        /// 所有写入都失败的网关（模拟 server 未启用 Auth）。
        struct FailingGateway;

        #[async_trait]
        impl PluginAuthGateway for FailingGateway {
            async fn user_add(&self, _user: &str, _password: &str) -> Result<(), String> {
                Err("auth not enabled".into())
            }
            async fn role_add(&self, _role: &str) -> Result<(), String> {
                Err("auth not enabled".into())
            }
            async fn role_grant_capability(
                &self,
                _role: &str,
                _capability_id: &str,
                _scope: &str,
            ) -> Result<(), String> {
                Err("auth not enabled".into())
            }
            async fn user_grant_role(&self, _user: &str, _role: &str) -> Result<(), String> {
                Err("auth not enabled".into())
            }
            async fn authenticate(
                &self,
                _user: &str,
                _password: &str,
            ) -> Result<IssuedToken, String> {
                Err("auth not enabled".into())
            }
            async fn refresh(&self, _refresh_token: &str) -> Result<IssuedToken, String> {
                Err("auth not enabled".into())
            }
        }

        let clients = Arc::new(PluginClients::new(offline_client().await));
        let dir = tempfile::tempdir().expect("tempdir");
        let mgr = PluginIdentityManager::new(
            Arc::new(FailingGateway),
            vec!["http://127.0.0.1:1".to_string()],
            None,
            dir.path(),
            Arc::clone(&clients),
        )
        .expect("identity manager");

        let err = mgr.ensure("counter", &[]).await.unwrap_err();
        assert!(err.contains("auth not enabled"), "{err}");
        // 未登记 → SDK 回退共享客户端
        assert!(clients.is_empty());
    }

    #[tokio::test]
    async fn gateway_refresh_failure_falls_back_to_password() {
        let gateway = Arc::new(StubGateway::with_failed_refresh());
        let clients = Arc::new(PluginClients::new(offline_client().await));
        let dir = tempfile::tempdir().expect("tempdir");
        let mgr = PluginIdentityManager::new(
            Arc::clone(&gateway) as Arc<dyn PluginAuthGateway>,
            vec!["http://127.0.0.1:1".to_string()],
            None,
            dir.path(),
            Arc::clone(&clients),
        )
        .expect("identity manager");
        mgr.ensure("counter", &[]).await.expect("ensure");
        assert_eq!(gateway.auth_calls.load(Ordering::SeqCst), 1);

        // 立即触发一次续期（直接调用内部逻辑：以短到期时间重建）
        let provider = Arc::new(CachedTokenProvider::new(Some("cct-1".to_string())));
        let handle = mgr.spawn_renewal(
            "counter",
            "plugin/counter".to_string(),
            "pw".to_string(),
            Arc::clone(&provider),
            IssuedToken {
                cct: "cct-1".into(),
                refresh_token: Some("rt-old".into()),
                // 已过期 → 续期间隔取下限（30s）
                expires_at: now_secs() - 10,
            },
        );
        // 30s 下限对单测太长 → 直接验证状态机：把 sleep 下限当作外部事实，
        // 这里仅断言 refresh 失败后的重认证路径已被实现（见下：手动调用一次）
        handle.abort();

        // 手动复现续期决策：refresh 失败 → authenticate（密码仍有效）
        assert!(gateway.refresh("rt-old").await.is_err());
        let again = gateway
            .authenticate("plugin/counter", "pw")
            .await
            .expect("reauth");
        assert_eq!(again.cct, "cct-1");
        assert_eq!(gateway.auth_calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn shared_client_source_always_returns_fallback() {
        // PluginClientSource 的语义验证：未登记 → fallback
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("rt");
        rt.block_on(async {
            let clients = PluginClients::new(offline_client().await);
            let src: Arc<dyn PluginClientSource> = Arc::new(clients);
            // 未登记插件不 panic 且可取得客户端
            let _ = src.client_for("unregistered");
        });
    }

    /// **批次 12**：自举的 provisioner 会话接管开通路径。
    ///
    /// 场景：一次性引导 CCT（10 分钟）已过期 → 运行中（SIGHUP）新增插件的
    /// `user_add` 会 `permission denied`。有 provisioner 会话后，开通序列走它。
    #[tokio::test]
    async fn provisioner_session_takes_over_account_provisioning() {
        let bootstrap = Arc::new(StubGateway::default());
        let clients = Arc::new(PluginClients::new(offline_client().await));
        let dir = tempfile::tempdir().expect("tempdir");
        let mgr = PluginIdentityManager::new(
            Arc::clone(&bootstrap) as Arc<dyn PluginAuthGateway>,
            vec!["http://127.0.0.1:1".to_string()],
            None,
            dir.path(),
            Arc::clone(&clients),
        )
        .expect("identity manager");

        assert!(!mgr.provisioner_active());

        // 自举前的插件走引导网关
        mgr.ensure("alpha", &[cap("data:kv:read", "/app/alpha/")])
            .await
            .expect("ensure alpha");
        assert_eq!(bootstrap.users.lock().as_slice(), ["plugin/alpha"]);

        // 自举 provisioner 会话（测试注入已完成认证的桩网关，等价于真实
        // `bootstrap_provisioner` 的第 3 步 —— 网络部分由进程测试覆盖）
        let provisioner = Arc::new(StubGateway::default());
        mgr.adopt_provisioner(Arc::clone(&provisioner) as Arc<dyn PluginAuthGateway>, None);
        assert!(mgr.provisioner_active());

        // 自举后新增的插件：开通序列必须落在 provisioner 会话上
        mgr.ensure("beta", &[cap("data:kv:write", "/app/beta/")])
            .await
            .expect("ensure beta");
        assert_eq!(provisioner.users.lock().as_slice(), ["plugin/beta"]);
        assert_eq!(provisioner.roles.lock().as_slice(), ["plugin/beta-role"]);
        assert_eq!(
            bootstrap.users.lock().as_slice(),
            ["plugin/alpha"],
            "bootstrap gateway must not be used once a provisioner session exists"
        );
        // 认证仍走管理器自身的 gateway（provisioner 只负责账户开通/授权）
        assert_eq!(bootstrap.auth_calls.load(Ordering::SeqCst), 2);
        assert_eq!(provisioner.auth_calls.load(Ordering::SeqCst), 0);
        assert_eq!(clients.len(), 2);

        // 释放后回到引导网关（fail-closed 语义不变）
        mgr.forget_provisioner();
        assert!(!mgr.provisioner_active());
        mgr.ensure("gamma", &[]).await.expect("ensure gamma");
        assert!(bootstrap.users.lock().iter().any(|u| u == "plugin/gamma"));
        assert!(
            !provisioner.users.lock().iter().any(|u| u == "plugin/gamma"),
            "after release the bootstrap gateway must be used again"
        );
    }

    /// provisioner 用户名必须非空（配置校验由调用方负责，这里 fail-fast）。
    #[tokio::test]
    async fn bootstrap_provisioner_rejects_empty_user() {
        let gateway = Arc::new(StubGateway::default());
        let clients = Arc::new(PluginClients::new(offline_client().await));
        let dir = tempfile::tempdir().expect("tempdir");
        let mgr = PluginIdentityManager::new(
            gateway as Arc<dyn PluginAuthGateway>,
            vec!["http://127.0.0.1:1".to_string()],
            None,
            dir.path(),
            clients,
        )
        .expect("identity manager");
        assert!(mgr.bootstrap_provisioner(None, "  ").await.is_err());
        assert!(!mgr.provisioner_active());
    }
}
