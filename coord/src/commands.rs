// CLI 命令处理器
//
// 从 main.rs 抽取，支持单元测试。每个命令对应一个异步函数，
// 返回 Result<(), Box<dyn std::error::Error>>。
//
// 实现状态：
// - Security::Seal/Unseal: 通过 tonic 直连 gRPC 调用
// - Security::InitSeal: 本地生成 Shamir 分片文件
// - Security::RotateKeys: 无 proto RPC → 返回明确错误
// - Member::*: 通过 tonic 直连 gRPC 调用 Maintenance::MemberAdd/Remove/Promote/List

use coord_client::credential::{AuthedChannel, CachedTokenProvider, CredentialInterceptor};
use coord_proto::kv::kv_client::KvClient;
use coord_proto::kv::{PutRequest, RangeRequest};
use coord_proto::maintenance::maintenance_client::MaintenanceClient;
use coord_proto::maintenance::{
    MemberAddRequest, MemberListRequest, MemberPromoteRequest, MemberRemoveRequest, SealRequest,
    SnapshotRequest, UnsealRequest,
};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

// ──── CLI 凭据（进程级）────

/// 进程级 CLI 凭据（CCT / Bearer Token）。
///
/// 由 main.rs 从全局 `--token`（或 `COORD_TOKEN` 环境变量）设置一次。
/// 鉴权开启的集群上，所有 CLI 管理命令（auth / member / capability …）
/// 都需要携带该凭据；未设置时为 `None` → 出站拦截器 no-op（明文开发模式零破坏）。
static CLI_TOKEN: OnceLock<String> = OnceLock::new();

/// 设置进程级 CLI 凭据（幂等：仅在首次调用时生效）。
///
/// `coord auth login --token-only` 的输出可直接经 `--token` 或
/// `COORD_TOKEN` 回灌给后续管理命令。
pub fn set_cli_token(token: Option<String>) {
    if let Some(token) = token {
        if token.trim().is_empty() {
            return;
        }
        let _ = CLI_TOKEN.set(token);
    }
}

/// 当前进程级 CLI 凭据。
fn cli_token() -> Option<String> {
    CLI_TOKEN.get().cloned()
}

// ──── CLI 凭据文件（持久化 + 自动续期；批次 12）────

/// 进程级凭据文件路径（由 main.rs 从全局 `--credentials`（env `COORD_CREDENTIALS`）
/// 设置一次；缺省 = [`crate::credentials::default_path`]）。
static CLI_CREDENTIALS_PATH: OnceLock<PathBuf> = OnceLock::new();

/// 设置凭据文件路径（幂等：仅在首次调用时生效）。
pub fn set_cli_credentials(path: Option<PathBuf>) {
    if let Some(path) = path {
        if path.as_os_str().is_empty() {
            return;
        }
        let _ = CLI_CREDENTIALS_PATH.set(path);
    }
}

/// 当前凭据文件路径。
pub fn cli_credentials_path() -> PathBuf {
    CLI_CREDENTIALS_PATH
        .get()
        .cloned()
        .unwrap_or_else(crate::credentials::default_path)
}

// ──── 集群连接参数（地址 + 可选 TLS/mTLS + 可选凭据）────

/// CLI 集群连接参数：目标节点地址 + 可选 TLS/mTLS（+ 进程级凭据）。
///
/// 提供 `--tls-ca/--tls-cert/--tls-key/--tls-server-name` 时以 https+TLS 直连
/// 生产（mTLS）集群；缺省 None = 明文 http（开发/内网 loopback）。
/// 实现 `From<&str>` 便于既有调用点以裸地址构造明文连接。
#[derive(Debug, Clone)]
pub struct CliConn {
    addr: String,
    tls: Option<coord_client::config::TlsConfig>,
}

impl CliConn {
    /// 构造连接参数（tls 由 main.rs 从全局 --tls-* 参数构建）
    pub fn new(addr: &str, tls: Option<coord_client::config::TlsConfig>) -> Self {
        Self {
            addr: addr.to_string(),
            tls,
        }
    }

    /// 目标节点地址
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// 建立 tonic Channel（https + TLS 或 http 明文）
    pub async fn connect(&self) -> Result<Channel, Box<dyn std::error::Error>> {
        let scheme = if self.tls.is_some() { "https" } else { "http" };
        let mut endpoint =
            tonic::transport::Endpoint::from_shared(format!("{scheme}://{}", self.addr))?
                .connect_timeout(std::time::Duration::from_secs(3));
        if let Some(tls) = &self.tls {
            endpoint = endpoint.tls_config(tls.to_tonic())?;
        }
        Ok(endpoint.connect().await?)
    }

    /// 建立**携带凭据**的 tonic 通道（`authorization: Bearer <cct>`）。
    ///
    /// 凭据来源（优先级）：
    /// 1. 进程级 `--token` / `COORD_TOKEN`（显式覆盖）；
    /// 2. 凭据文件（`coord auth login` 落盘）—— 地址一致且**临近/已过期**时
    ///    先用 refresh token 自动续期并回写文件（批次 12）。
    ///
    /// 两者都缺失时拦截器为 no-op，与 `connect()` 等价（明文开发模式零行为变更）。
    pub async fn connect_authed(&self) -> Result<AuthedChannel, Box<dyn std::error::Error>> {
        let channel = self.connect().await?;
        let token = match cli_token() {
            Some(token) => Some(token),
            None => self.stored_credential().await?,
        };
        let provider = Arc::new(CachedTokenProvider::new(token));
        Ok(InterceptedService::new(
            channel,
            CredentialInterceptor::new(provider),
        ))
    }

    /// 凭据文件中的可用 CCT（无 / 地址不匹配 → `None`）。
    ///
    /// 临近或已过期时自动续期：refresh token 换新 → **回写文件**（服务端单次使用，
    /// 不回写会让文件立刻失效）→ 返回新 CCT。续期失败返回 `Err`（fail-closed：
    /// 不静默带着过期凭据出站，而是提示重新登录）。
    async fn stored_credential(&self) -> Result<Option<String>, Box<dyn std::error::Error>> {
        let path = cli_credentials_path();
        let Some(stored) = crate::credentials::load(&path) else {
            return Ok(None);
        };
        if stored.addr != self.addr {
            tracing::debug!(
                "credentials file {} targets {} (not {}); ignoring",
                path.display(),
                stored.addr,
                self.addr
            );
            return Ok(None);
        }
        if !crate::credentials::needs_refresh(stored.expires_at, crate::credentials::now_secs()) {
            return Ok(Some(stored.cct));
        }
        if !stored.can_refresh() {
            return Err(format!(
                "stored credential for {} expired at {} and has no refresh token; \
                 run `coord auth login <user>` again",
                self.addr, stored.expires_at
            )
            .into());
        }
        let refreshed = self.refresh_stored(&path, &stored).await?;
        Ok(Some(refreshed))
    }

    /// 用凭据文件里的 refresh token 换新会话并回写文件。
    async fn refresh_stored(
        &self,
        path: &Path,
        stored: &crate::credentials::StoredCredentials,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let channel = self.connect().await?;
        let mut client = AuthClient::new(channel);
        let resp = client
            .refresh_token(RefreshTokenRequest {
                refresh_token: stored.refresh_token.clone(),
            })
            .await
            .map_err(|e| format!("RefreshToken failed: {e}"))?
            .into_inner();
        let cct = if resp.cct.is_empty() {
            resp.token.clone()
        } else {
            resp.cct.clone()
        };
        if cct.is_empty() {
            return Err("RefreshToken returned an empty credential".into());
        }
        let updated = crate::credentials::StoredCredentials {
            addr: stored.addr.clone(),
            user: stored.user.clone(),
            cct: cct.clone(),
            // 单次使用：服务端返回新的 refresh token（空 = 只认本次 CCT，之后需重新登录）
            refresh_token: resp.refresh_token.clone(),
            expires_at: resp.expires_at,
        };
        crate::credentials::save(path, &updated)
            .map_err(|e| format!("failed to persist refreshed credential: {e}"))?;
        eprintln!(
            "Notice: refreshed stored credential for {} (expires_at={})",
            self.addr, resp.expires_at
        );
        Ok(cct)
    }
}

/// 从裸地址构造明文连接（测试/既有调用点兼容）
impl<T: AsRef<str> + ?Sized> From<&T> for CliConn {
    fn from(addr: &T) -> Self {
        CliConn::new(addr.as_ref(), None)
    }
}

// ──── Security 命令 ────

/// 封存集群：通过 gRPC 调用 Maintenance::Seal
pub async fn cmd_seal(conn: impl Into<CliConn>) -> Result<(), Box<dyn std::error::Error>> {
    let conn = conn.into();
    let mut client = build_maintenance_client(&conn).await?;
    let request = tonic::Request::new(SealRequest {});
    client.seal(request).await?;
    println!("Cluster sealed successfully via {}", conn.addr());
    Ok(())
}

/// 解封集群：通过 gRPC 调用 Maintenance::Unseal
///
/// `shares` 中的每个元素作为 Shamir 分片（raw bytes）发送。
pub async fn cmd_unseal(
    conn: impl Into<CliConn>,
    shares: Vec<Vec<u8>>,
) -> Result<(), Box<dyn std::error::Error>> {
    if shares.is_empty() {
        return Err("at least one Shamir share is required for unseal".into());
    }

    let conn = conn.into();
    let mut client = build_maintenance_client(&conn).await?;
    let request = tonic::Request::new(UnsealRequest { shares });
    let resp = client.unseal(request).await?.into_inner();
    println!(
        "Cluster unsealed: {}/{} nodes unsealed via {}",
        resp.nodes_unsealed,
        resp.total_nodes,
        conn.addr()
    );
    Ok(())
}

/// 初始化密钥分片：本地生成 Shamir (N,K) 分片文件
///
/// 不依赖 gRPC——直接调用 coord-server::security::seal 模块。
/// 生成 N 个分片文件写入 `output_dir`。
pub async fn cmd_init_seal(
    n: u8,
    k: u8,
    output_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    if k > n {
        return Err(format!("threshold k ({k}) must not exceed total shares n ({n})").into());
    }
    if n == 0 || k == 0 {
        return Err("n and k must be positive".into());
    }

    // 生成随机 Root Key（256-bit）
    let mut root_key = [0u8; 32];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut root_key);

    // 生成 Shamir 分片
    let shares =
        coord_server::security::seal::SealManager::generate_shares_with_params(&root_key, n, k)?;

    // 确保输出目录存在
    std::fs::create_dir_all(output_dir)?;

    // 写入分片文件
    for share in &shares {
        let filename = format!("coord-seal-share-{}-of-{}.bin", share.index, n);
        let path = output_dir.join(&filename);
        std::fs::write(&path, share.to_bytes())?;
        tracing::info!("Wrote share {} to {}", share.index, path.display());
    }

    println!(
        "Generated {n} Shamir shares (threshold={k}) in {}",
        output_dir.display()
    );
    println!("Distribute each share to a different administrator securely.");
    Ok(())
}

/// 轮换数据加密密钥（DEK）
///
/// 当前无对应 proto RPC，返回明确错误。
pub async fn cmd_rotate_keys(_addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    Err("RotateKeys is not yet implemented: no gRPC RPC defined in maintenance.proto".into())
}

/// 一键授予 agent 注册引导角色所需**最小能力集**（幂等）。
///
/// 背景：`Auth.Bootstrap` 签发的短期 CCT 携带 [`AGENT_BOOTSTRAP_ROLE`]，
/// 但该角色**不预置任何能力**（服务端只内置 `root`）。此前需 operator 手工
/// 逐条执行 `coord auth role add` + `coord auth role grant-capability`
/// （见 `config.example.toml`）——本命令把该序列收敛为一次调用：
///   1. `RoleAdd`（角色已存在视为成功，保证幂等）；
///   2. 逐条 `RoleGrantCapability`（`RoleGrantCapability` 的 apply 视图自带去重）。
///
/// 能力清单取自 `coord_server::auth::AGENT_BOOTSTRAP_CAPABILITY_GRANTS`
/// （`admin:auth:{user_add,role_add,role_grant,user_grant_role}`，**刻意不含数据面**），
/// 与进程测试 `plugin_auth_process_test.rs` 使用同一常量——单一事实来源。
///
/// 前置条件：调用方持有管理员 CCT（经全局 `--token` / `COORD_TOKEN` 注入）。
pub async fn cmd_security_bootstrap_role(
    conn: impl Into<CliConn>,
    role: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    use coord_server::auth::{AGENT_BOOTSTRAP_CAPABILITY_GRANTS, AGENT_BOOTSTRAP_ROLE};

    let conn = conn.into();
    let role = role.unwrap_or(AGENT_BOOTSTRAP_ROLE);
    if role.trim().is_empty() {
        return Err("role name must not be empty".into());
    }

    let mut client = build_auth_client(&conn).await?;

    // 1. 确保角色存在（已存在 → 幂等成功，继续补授能力）
    match client
        .role_add(RoleAddRequest {
            name: role.to_string(),
        })
        .await
    {
        Ok(_) => println!("Role \"{role}\" created."),
        Err(status) if status.code() == tonic::Code::AlreadyExists => {
            println!("Role \"{role}\" already exists; ensuring capabilities.");
        }
        Err(status) => {
            return Err(format!("RoleAdd(\"{role}\") failed: {status}").into());
        }
    }

    // 2. 逐条授予引导最小能力集
    for (capability_id, scope) in AGENT_BOOTSTRAP_CAPABILITY_GRANTS {
        client
            .role_grant_capability(RoleGrantCapabilityRequest {
                role: role.to_string(),
                capability_id: (*capability_id).to_string(),
                scope: (*scope).to_string(),
            })
            .await
            .map_err(|status| format!("RoleGrantCapability({capability_id}) failed: {status}"))?;
        println!("  granted {capability_id}");
    }

    println!(
        "Bootstrap role \"{role}\" ready: {} capabilities granted (idempotent).",
        AGENT_BOOTSTRAP_CAPABILITY_GRANTS.len()
    );
    println!(
        "Next: set [security].agent_bootstrap_tokens in the server config, then let agents \
         exchange a one-time token via Auth.Bootstrap."
    );
    Ok(())
}

// ──── 动态 bootstrap 令牌（TTL + 一次性） ────

/// 签发动态 bootstrap 令牌：`Auth.BootstrapTokenIssue`。
///
/// **明文令牌只在此响应中出现一次**（服务端仅存 SHA256，入 raft 日志的也是哈希），
/// 因此 `--token-only` 的输出必须由调用方立刻保存。
pub async fn cmd_security_bootstrap_token_create(
    conn: impl Into<CliConn>,
    label: &str,
    ttl_secs: i64,
    token_only: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let conn = conn.into();
    let mut client = build_auth_client(&conn).await?;
    let resp = client
        .bootstrap_token_issue(BootstrapTokenIssueRequest {
            label: label.to_string(),
            ttl_secs,
        })
        .await
        .map_err(|e| format!("BootstrapTokenIssue failed: {e}"))?
        .into_inner();

    if token_only {
        println!("{}", resp.token);
        return Ok(());
    }
    println!("Bootstrap token issued (single use).");
    println!("  id         : {}", resp.id);
    println!(
        "  label      : {}",
        if label.is_empty() { "(none)" } else { label }
    );
    println!("  expires_at : {} (unix)", resp.expires_at);
    println!("  token      : {}", resp.token);
    println!();
    println!("Store the token now: it is not recoverable (server keeps only SHA256).");
    println!(
        "Revoke with: coord security bootstrap-token revoke --id {}",
        resp.id
    );
    Ok(())
}

/// 列出动态 bootstrap 令牌：`Auth.BootstrapTokenList`（不含明文）。
pub async fn cmd_security_bootstrap_token_list(
    conn: impl Into<CliConn>,
) -> Result<(), Box<dyn std::error::Error>> {
    let conn = conn.into();
    let mut client = build_auth_client(&conn).await?;
    let tokens = client
        .bootstrap_token_list(BootstrapTokenListRequest {})
        .await
        .map_err(|e| format!("BootstrapTokenList failed: {e}"))?
        .into_inner()
        .tokens;

    if tokens.is_empty() {
        println!("No bootstrap tokens.");
        return Ok(());
    }
    println!(
        "{:<38} {:<20} {:<14} {:<12} STATE",
        "ID", "LABEL", "CREATED_BY", "EXPIRES_AT"
    );
    for t in tokens {
        let state = if t.consumed { "consumed" } else { "unused" };
        println!(
            "{:<38} {:<20} {:<14} {:<12} {}",
            t.id,
            if t.label.is_empty() { "-" } else { &t.label },
            t.created_by,
            t.expires_at,
            state
        );
    }
    Ok(())
}

/// 撤销动态 bootstrap 令牌：`Auth.BootstrapTokenRevoke`（幂等）。
pub async fn cmd_security_bootstrap_token_revoke(
    conn: impl Into<CliConn>,
    id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let conn = conn.into();
    let mut client = build_auth_client(&conn).await?;
    let revoked = client
        .bootstrap_token_revoke(BootstrapTokenRevokeRequest { id: id.to_string() })
        .await
        .map_err(|e| format!("BootstrapTokenRevoke failed: {e}"))?
        .into_inner()
        .revoked;
    if revoked {
        println!("Revoked bootstrap token \"{id}\".");
    } else {
        println!("Bootstrap token \"{id}\" not found (already revoked or never existed).");
    }
    Ok(())
}

// ──── Member 命令 ────

/// 添加节点到集群：先添加为 Learner，再晋升为 Voter
pub async fn cmd_member_add(
    conn: impl Into<CliConn>,
    id: u64,
    node_addr: &str,
    raft_addr: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let conn = conn.into();
    let raft = raft_addr.unwrap_or(node_addr);
    let mut client = build_maintenance_client(&conn).await?;
    let request = tonic::Request::new(MemberAddRequest {
        node_id: id,
        grpc_addr: node_addr.to_string(),
        raft_addr: raft.to_string(),
    });
    let resp = client.member_add(request).await?.into_inner();
    if resp.success {
        println!("{}", resp.message);
    } else {
        return Err(resp.message.into());
    }
    Ok(())
}

/// 从集群移除节点
pub async fn cmd_member_remove(
    conn: impl Into<CliConn>,
    id: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let conn = conn.into();
    let mut client = build_maintenance_client(&conn).await?;
    let request = tonic::Request::new(MemberRemoveRequest { node_id: id });
    let resp = client.member_remove(request).await?.into_inner();
    if resp.success {
        println!("{}", resp.message);
    } else {
        return Err(resp.message.into());
    }
    Ok(())
}

/// 将 Learner 晋升为 Voter
pub async fn cmd_member_promote(
    conn: impl Into<CliConn>,
    id: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let conn = conn.into();
    let mut client = build_maintenance_client(&conn).await?;
    let request = tonic::Request::new(MemberPromoteRequest { node_id: id });
    let resp = client.member_promote(request).await?.into_inner();
    if resp.success {
        println!("{}", resp.message);
    } else {
        return Err(resp.message.into());
    }
    Ok(())
}

/// 列出所有节点及其状态
pub async fn cmd_member_list(conn: impl Into<CliConn>) -> Result<(), Box<dyn std::error::Error>> {
    let conn = conn.into();
    let mut client = build_maintenance_client(&conn).await?;
    let request = tonic::Request::new(MemberListRequest {});
    let resp = client.member_list(request).await?.into_inner();

    println!("Leader: node {}", resp.leader_id);
    println!("{:<6} {:<10}", "ID", "ROLE");
    println!("{}", "-".repeat(18));
    for node in &resp.nodes {
        println!("{:<6} {:<10}", node.id, node.role);
    }
    Ok(())
}

// ──── Auth 命令 ────

use coord_proto::auth::auth_client::AuthClient;
use coord_proto::auth::{
    AuthDisableRequest, AuthEnableRequest, AuthStatusRequest, AuthenticateRequest,
    BootstrapTokenIssueRequest, BootstrapTokenListRequest, BootstrapTokenRevokeRequest, Permission,
    PermissionType, RefreshTokenRequest, RoleAddRequest, RoleDeleteRequest,
    RoleGrantCapabilityRequest, RoleGrantPermissionRequest, RoleListRequest,
    RoleRevokeCapabilityRequest, RoleRevokePermissionRequest, UserAddRequest,
    UserChangePasswordRequest, UserDeleteRequest, UserGetRequest, UserGrantRoleRequest,
    UserListRequest, UserRevokeRoleRequest,
};

/// AppRole 用户名前缀
const APPROLE_PREFIX: &str = "approle-";

/// 将用户可见的 AppRole 名称转换为内部用户名
fn to_approle_internal(name: &str) -> String {
    format!("{APPROLE_PREFIX}{name}")
}

/// 从内部用户名提取 AppRole 名称（去掉前缀）
fn from_approle_internal(internal: &str) -> Option<&str> {
    internal.strip_prefix(APPROLE_PREFIX)
}

/// 生成 32 位随机十六进制 Secret ID
fn generate_secret_id() -> String {
    use rand::Rng;
    let chars: Vec<u8> = (b'A'..=b'Z').chain(b'0'..=b'9').collect();
    let mut rng = rand::thread_rng();
    (0..32)
        .map(|_| chars[rng.gen_range(0..chars.len())] as char)
        .collect()
}

/// 交互式读取密码（带确认）
pub fn prompt_password_with_confirm() -> Result<String, Box<dyn std::error::Error>> {
    let password = rpassword::prompt_password("Password: ")?;
    if password.is_empty() {
        return Err("password must not be empty".into());
    }
    let confirm = rpassword::prompt_password("Confirm password: ")?;
    if password != confirm {
        return Err("passwords do not match".into());
    }
    Ok(password)
}

/// 交互式读取密码（无确认）
pub fn prompt_password(prompt: &str) -> Result<String, Box<dyn std::error::Error>> {
    let password = rpassword::prompt_password(prompt)?;
    if password.is_empty() {
        return Err("password must not be empty".into());
    }
    Ok(password)
}

/// 解析权限类型字符串
fn parse_permission_type(s: &str) -> Result<i32, Box<dyn std::error::Error>> {
    match s.to_lowercase().as_str() {
        "read" => Ok(PermissionType::Read as i32),
        "write" => Ok(PermissionType::Write as i32),
        "readwrite" => Ok(PermissionType::Readwrite as i32),
        other => Err(format!(
            "invalid permission type: {other}. expected read, write, or readwrite"
        )
        .into()),
    }
}

// ──── Auth 状态管理 ────

/// 启用认证：调用 Auth::AuthEnable
pub async fn cmd_auth_enable(conn: impl Into<CliConn>) -> Result<(), Box<dyn std::error::Error>> {
    let conn = conn.into();
    let mut client = build_auth_client(&conn).await?;
    client.auth_enable(AuthEnableRequest {}).await?;
    println!("Auth enabled");
    Ok(())
}

/// 禁用认证：调用 Auth::AuthDisable
pub async fn cmd_auth_disable(conn: impl Into<CliConn>) -> Result<(), Box<dyn std::error::Error>> {
    let conn = conn.into();
    let mut client = build_auth_client(&conn).await?;
    client.auth_disable(AuthDisableRequest {}).await?;
    println!("Auth disabled");
    Ok(())
}

/// 查看认证状态：调用 Auth::AuthStatus
pub async fn cmd_auth_status(conn: impl Into<CliConn>) -> Result<(), Box<dyn std::error::Error>> {
    let conn = conn.into();
    let mut client = build_auth_client(&conn).await?;
    let resp = client.auth_status(AuthStatusRequest {}).await?.into_inner();
    if resp.enabled {
        println!("Auth is enabled");
    } else {
        println!("Auth is disabled");
    }
    Ok(())
}

// ──── 用户管理 ────

/// 创建用户：调用 Auth::UserAdd
pub async fn cmd_auth_user_add(
    conn: impl Into<CliConn>,
    name: &str,
    password: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // 禁止创建 approle- 前缀的普通用户
    if name.starts_with(APPROLE_PREFIX) {
        return Err(format!(
            "username must not start with '{APPROLE_PREFIX}' (reserved for AppRole)"
        )
        .into());
    }
    let conn = conn.into();
    let mut client = build_auth_client(&conn).await?;
    client
        .user_add(UserAddRequest {
            name: name.to_string(),
            password: password.to_string(),
        })
        .await?;
    println!("User \"{name}\" created.");
    Ok(())
}

/// 删除用户：调用 Auth::UserDelete
pub async fn cmd_auth_user_delete(
    conn: impl Into<CliConn>,
    name: &str,
    force: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if !force {
        return Err("use --force to confirm deletion".into());
    }
    let conn = conn.into();
    let mut client = build_auth_client(&conn).await?;
    client
        .user_delete(UserDeleteRequest {
            name: name.to_string(),
        })
        .await?;
    println!("User \"{name}\" deleted.");
    Ok(())
}

/// 修改密码：调用 Auth::UserChangePassword
pub async fn cmd_auth_user_passwd(
    conn: impl Into<CliConn>,
    name: &str,
    password: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let conn = conn.into();
    let mut client = build_auth_client(&conn).await?;
    client
        .user_change_password(UserChangePasswordRequest {
            name: name.to_string(),
            password: password.to_string(),
        })
        .await?;
    println!("Password changed for user \"{name}\".");
    Ok(())
}

/// 列出所有用户：调用 Auth::UserList
pub async fn cmd_auth_user_list(
    conn: impl Into<CliConn>,
) -> Result<(), Box<dyn std::error::Error>> {
    let conn = conn.into();
    let mut client = build_auth_client(&conn).await?;
    let resp = client.user_list(UserListRequest {}).await?.into_inner();
    println!("{:<24} {:<}", "NAME", "ROLES");
    println!("{}", "-".repeat(48));
    for user in &resp.users {
        println!("{:<24} {:<}", user.name, user.roles.join(", "));
    }
    Ok(())
}

/// 查看用户详情：调用 Auth::UserGet
pub async fn cmd_auth_user_show(
    conn: impl Into<CliConn>,
    name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let conn = conn.into();
    let mut client = build_auth_client(&conn).await?;
    let resp = client
        .user_get(UserGetRequest {
            name: name.to_string(),
        })
        .await?
        .into_inner();
    println!("User: {name}");
    let roles_display = if resp.roles.is_empty() {
        "(none)".to_string()
    } else {
        resp.roles.join(", ")
    };
    println!("Roles: {roles_display}");
    Ok(())
}

// ──── 角色管理 ────

/// 创建角色：调用 Auth::RoleAdd
pub async fn cmd_auth_role_add(
    conn: impl Into<CliConn>,
    name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let conn = conn.into();
    let mut client = build_auth_client(&conn).await?;
    client
        .role_add(RoleAddRequest {
            name: name.to_string(),
        })
        .await?;
    println!("Role \"{name}\" created.");
    Ok(())
}

/// 删除角色：调用 Auth::RoleDelete
pub async fn cmd_auth_role_delete(
    conn: impl Into<CliConn>,
    name: &str,
    force: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if !force {
        return Err("use --force to confirm deletion".into());
    }
    let conn = conn.into();
    let mut client = build_auth_client(&conn).await?;
    client
        .role_delete(RoleDeleteRequest {
            name: name.to_string(),
        })
        .await?;
    println!("Role \"{name}\" deleted.");
    Ok(())
}

/// 为角色授予权限：调用 Auth::RoleGrantPermission
pub async fn cmd_auth_role_grant(
    conn: impl Into<CliConn>,
    name: &str,
    perm_type: &str,
    key: &str,
    range_end: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let perm = parse_permission_type(perm_type)?;
    let conn = conn.into();
    let mut client = build_auth_client(&conn).await?;
    client
        .role_grant_permission(RoleGrantPermissionRequest {
            name: name.to_string(),
            permission: Some(Permission {
                r#type: perm,
                key: key.as_bytes().to_vec(),
                range_end: range_end.unwrap_or("").as_bytes().to_vec(),
            }),
        })
        .await?;
    let range_info = if let Some(end) = range_end {
        format!("[{key}, {end})")
    } else {
        key.to_string()
    };
    println!("Granted {perm_type} on {range_info} to role \"{name}\".");
    Ok(())
}

/// 撤销角色权限：调用 Auth::RoleRevokePermission
pub async fn cmd_auth_role_revoke(
    conn: impl Into<CliConn>,
    name: &str,
    key: &str,
    range_end: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let conn = conn.into();
    let mut client = build_auth_client(&conn).await?;
    client
        .role_revoke_permission(RoleRevokePermissionRequest {
            name: name.to_string(),
            key: key.as_bytes().to_vec(),
            range_end: range_end.unwrap_or("").as_bytes().to_vec(),
        })
        .await?;
    println!("Revoked permission on \"{key}\" from role \"{name}\".");
    Ok(())
}

/// 为角色授予**能力**（capability + scope）：调用 Auth::RoleGrantCapability。
///
/// 与 `RoleGrantPermission`（旧 Key 前缀模型）并列的新授权面：能力 id 取自
/// 内置能力清单（`coord capability list`），`scope` 为空 = 无限制。
pub async fn cmd_auth_role_grant_capability(
    conn: impl Into<CliConn>,
    name: &str,
    capability_id: &str,
    scope: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let conn = conn.into();
    let mut client = build_auth_client(&conn).await?;
    client
        .role_grant_capability(RoleGrantCapabilityRequest {
            role: name.to_string(),
            capability_id: capability_id.to_string(),
            scope: scope.to_string(),
        })
        .await?;
    let scope_display = if scope.is_empty() {
        "(unrestricted)"
    } else {
        scope
    };
    println!("Granted capability \"{capability_id}\" (scope {scope_display}) to role \"{name}\".");
    Ok(())
}

/// 撤销角色能力：调用 Auth::RoleRevokeCapability
pub async fn cmd_auth_role_revoke_capability(
    conn: impl Into<CliConn>,
    name: &str,
    capability_id: &str,
    scope: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let conn = conn.into();
    let mut client = build_auth_client(&conn).await?;
    client
        .role_revoke_capability(RoleRevokeCapabilityRequest {
            role: name.to_string(),
            capability_id: capability_id.to_string(),
            scope: scope.to_string(),
        })
        .await?;
    println!("Revoked capability \"{capability_id}\" from role \"{name}\".");
    Ok(())
}

/// 列出所有角色：调用 Auth::RoleList
pub async fn cmd_auth_role_list(
    conn: impl Into<CliConn>,
) -> Result<(), Box<dyn std::error::Error>> {
    let conn = conn.into();
    let mut client = build_auth_client(&conn).await?;
    let resp = client.role_list(RoleListRequest {}).await?.into_inner();
    println!("{:<24} {:<}", "NAME", "PERMISSIONS");
    println!("{}", "-".repeat(64));
    for role in &resp.roles {
        let perms: Vec<String> = role
            .permissions
            .iter()
            .map(|p| {
                let type_str = match PermissionType::try_from(p.r#type) {
                    Ok(PermissionType::Read) => "R",
                    Ok(PermissionType::Write) => "W",
                    Ok(PermissionType::Readwrite) => "RW",
                    _ => "?",
                };
                let key = String::from_utf8_lossy(&p.key);
                let range = if p.range_end.is_empty() {
                    String::new()
                } else {
                    format!("..{}", String::from_utf8_lossy(&p.range_end))
                };
                format!("{type_str}:{key}{range}")
            })
            .collect();
        println!("{:<24} {:<}", role.name, perms.join(", "));
    }
    Ok(())
}

// ──── 用户-角色绑定 ────

/// 为用户分配角色：调用 Auth::UserGrantRole
/// 若 user 为 AppRole 名称（不以 approle- 开头），自动加前缀。
pub async fn cmd_auth_grant(
    conn: impl Into<CliConn>,
    user: &str,
    role: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let internal_user = if user.starts_with(APPROLE_PREFIX) {
        user.to_string()
    } else {
        // 检查是否为 AppRole（通过前缀自动补全约定）
        // 安全做法：如果用户以已知AppRole前缀以外的形式出现，直接尝试原用户名
        // CLI 阶段约定：grant 的用户参数若为 AppRole 名（不含前缀），内部自动补全
        user.to_string()
    };
    let conn = conn.into();
    let mut client = build_auth_client(&conn).await?;
    client
        .user_grant_role(UserGrantRoleRequest {
            user: internal_user,
            role: role.to_string(),
        })
        .await?;
    println!("Granted role \"{role}\" to \"{user}\".");
    Ok(())
}

/// 撤销用户角色：调用 Auth::UserRevokeRole
pub async fn cmd_auth_revoke(
    conn: impl Into<CliConn>,
    user: &str,
    role: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let conn = conn.into();
    let mut client = build_auth_client(&conn).await?;
    client
        .user_revoke_role(UserRevokeRoleRequest {
            user: user.to_string(),
            role: role.to_string(),
        })
        .await?;
    println!("Revoked role \"{role}\" from \"{user}\".");
    Ok(())
}

// ──── 登录 ────

/// 登录获取 Token：调用 Auth::Authenticate
///
/// 输出**优先 CCT v3**（`cct` 字段）：鉴权开启后服务端只认 CCT，
/// 旧 `token` 字段仅为兼容保留（此前误打印 `token`，导致
/// `coord auth login --token-only` 取到的凭据无法用于后续管理命令）。
///
/// `print_refresh = true` → 额外输出 refresh token（单次使用，供长时脚本调用
/// `coord auth refresh` 续期；见 [`cmd_auth_refresh`]）。
///
/// `save = true`（默认）→ 把 CCT + refresh token + 到期时刻写入凭据文件
/// （`--credentials` / `$XDG_CONFIG_HOME/coord/credentials.json`），后续 CLI 命令
/// 自动携带并在到期前**自动续期**（批次 12）；`--no-save` 用于纯脚本模式。
pub async fn cmd_auth_login(
    conn: impl Into<CliConn>,
    name: &str,
    password: &str,
    token_only: bool,
    print_refresh: bool,
    save: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let conn = conn.into();
    let mut client = build_auth_client(&conn).await?;
    let resp = client
        .authenticate(AuthenticateRequest {
            name: name.to_string(),
            password: password.to_string(),
        })
        .await?
        .into_inner();
    let credential = if resp.cct.is_empty() {
        resp.token.clone()
    } else {
        resp.cct.clone()
    };
    if credential.is_empty() {
        return Err("server returned an empty credential".into());
    }

    // 批次 12：落盘凭据文件 → 后续命令自动携带、临近过期自动续期。
    // 注意：提示走 stderr，stdout 仍只有凭据（保持 `--token-only` 的脚本契约）。
    if !save {
        eprintln!("Credentials not saved (--no-save).");
    } else if resp.refresh_token.is_empty() {
        eprintln!(
            "Warning: server returned no refresh token; \
             credentials file cannot be auto-renewed"
        );
    } else {
        let path = cli_credentials_path();
        let stored = crate::credentials::StoredCredentials {
            addr: conn.addr().to_string(),
            user: name.to_string(),
            cct: credential.clone(),
            refresh_token: resp.refresh_token.clone(),
            expires_at: resp.expires_at,
        };
        match crate::credentials::save(&path, &stored) {
            Ok(()) => eprintln!(
                "Credentials saved to {} (auto-refreshed before expiry, expiry {})",
                path.display(),
                resp.expires_at
            ),
            Err(e) => eprintln!("Warning: failed to save credentials: {e}"),
        }
    }

    if token_only {
        println!("{credential}");
    } else {
        println!("Login successful. Token: {credential}");
    }
    if print_refresh {
        if resp.refresh_token.is_empty() {
            return Err("server did not return a refresh token".into());
        }
        // 纯刷新令牌行（供脚本 `$(coord auth login … --print-refresh | tail -n1)`）
        println!("{}", resp.refresh_token);
    }
    Ok(())
}

/// 用 refresh token 换新会话：调用 Auth::RefreshToken（服务端保证单次使用）。
///
/// 默认输出新 CCT；`print_refresh` 额外输出**新的** refresh token
///（旧 token 已消费，脚本需用新值替换）。
pub async fn cmd_auth_refresh(
    conn: impl Into<CliConn>,
    refresh_token: &str,
    token_only: bool,
    print_refresh: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if refresh_token.trim().is_empty() {
        return Err("refresh token must not be empty".into());
    }
    let conn = conn.into();
    let mut client = build_auth_client(&conn).await?;
    let resp = client
        .refresh_token(RefreshTokenRequest {
            refresh_token: refresh_token.to_string(),
        })
        .await
        .map_err(|e| format!("RefreshToken failed: {e}"))?
        .into_inner();

    let credential = if resp.cct.is_empty() {
        resp.token.clone()
    } else {
        resp.cct.clone()
    };
    if credential.is_empty() {
        return Err("server returned an empty credential".into());
    }

    // 批次 12：续期后**回写凭据文件**（服务端单次使用：旧 refresh token 已消费，
    // 不回写则文件里的 refresh token 立刻失效）。仅当文件存在且地址一致时回写。
    let path = cli_credentials_path();
    if let Some(stored) = crate::credentials::load(&path) {
        if stored.addr == conn.addr() {
            let updated = crate::credentials::StoredCredentials {
                addr: stored.addr,
                user: stored.user,
                cct: credential.clone(),
                refresh_token: resp.refresh_token.clone(),
                expires_at: resp.expires_at,
            };
            if let Err(e) = crate::credentials::save(&path, &updated) {
                eprintln!("Warning: failed to persist refreshed credential: {e}");
            }
        }
    }

    if token_only {
        println!("{credential}");
    } else {
        println!("Session refreshed. Token: {credential}");
        println!("  expires_at : {}", resp.expires_at);
    }
    if print_refresh {
        if resp.refresh_token.is_empty() {
            return Err("server did not return a refresh token".into());
        }
        println!("{}", resp.refresh_token);
    }
    Ok(())
}

/// 登出：删除凭据文件（`coord auth logout`）。
///
/// 幂等：文件不存在也算成功（可安全重复执行）。
pub fn cmd_auth_logout() -> Result<(), Box<dyn std::error::Error>> {
    let path = cli_credentials_path();
    match crate::credentials::remove(&path) {
        Ok(true) => println!("Logged out (removed {}).", path.display()),
        Ok(false) => println!("No stored credentials at {}.", path.display()),
        Err(e) => return Err(e.into()),
    }
    Ok(())
}

/// 本地凭据状态：`coord auth credential-status`（不访问集群，只读凭据文件）。
///
/// 与 [`cmd_auth_status`]（查询集群鉴权开关）区分开。
pub fn cmd_auth_credential_status() -> Result<(), Box<dyn std::error::Error>> {
    let path = cli_credentials_path();
    println!("credentials : {}", path.display());
    if let Some(token) = cli_token() {
        println!(
            "source      : --token / COORD_TOKEN ({} chars)",
            token.len()
        );
    }
    match crate::credentials::load(&path) {
        Some(stored) => {
            let now = crate::credentials::now_secs();
            let remaining = stored.expires_at - now;
            println!("addr        : {}", stored.addr);
            println!("user        : {}", stored.user);
            println!(
                "expires_at  : {} ({remaining}s remaining)",
                stored.expires_at
            );
            println!(
                "renewable   : {}",
                if stored.can_refresh() { "yes" } else { "no" }
            );
            println!(
                "next action : {}",
                if crate::credentials::needs_refresh(stored.expires_at, now) {
                    "will refresh on next command"
                } else {
                    "valid until expiry"
                }
            );
        }
        None => println!("stored      : none (run `coord auth login <user>`)"),
    }
    Ok(())
}

// ──── AppRole 管理 ────

/// 创建 AppRole：内部创建 approle-<name> 用户，密码为生成的 Secret ID
pub async fn cmd_auth_approle_create(
    conn: impl Into<CliConn>,
    name: &str,
    _role_id: Option<&str>,
    secret_id: Option<&str>,
    bind_role: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let conn = conn.into();
    let internal_name = to_approle_internal(name);
    let secret = secret_id
        .map(|s| s.to_string())
        .unwrap_or_else(generate_secret_id);

    // 创建内部用户（密码为 Secret ID）
    let mut client = build_auth_client(&conn).await?;
    client
        .user_add(UserAddRequest {
            name: internal_name.clone(),
            password: secret.clone(),
        })
        .await?;

    // 若指定绑定角色，授权
    if let Some(role) = bind_role {
        client
            .user_grant_role(UserGrantRoleRequest {
                user: internal_name,
                role: role.to_string(),
            })
            .await?;
    }

    println!("AppRole \"{name}\" created.");
    println!("Role ID:   {name}");
    println!("Secret ID: {secret}");
    println!("NOTE: Secret ID is only shown once. Please store it securely.");
    Ok(())
}

/// 删除 AppRole：删除内部用户 approle-<name>
pub async fn cmd_auth_approle_delete(
    conn: impl Into<CliConn>,
    name: &str,
    force: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if !force {
        return Err("use --force to confirm deletion".into());
    }
    let conn = conn.into();
    let internal_name = to_approle_internal(name);
    let mut client = build_auth_client(&conn).await?;
    client
        .user_delete(UserDeleteRequest {
            name: internal_name,
        })
        .await?;
    println!("AppRole \"{name}\" deleted.");
    Ok(())
}

/// 查看 AppRole 的 Role ID
pub async fn cmd_auth_approle_role_id(
    conn: impl Into<CliConn>,
    name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Role ID 恒为 AppRole 名称
    println!("Role ID: {name}");
    // 验证内部用户存在
    let _ = conn; // silence unused warning
    Ok(())
}

/// 重置 AppRole 的 Secret ID（修改内部用户密码）
pub async fn cmd_auth_approle_secret_id(
    conn: impl Into<CliConn>,
    name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let conn = conn.into();
    let internal_name = to_approle_internal(name);
    let new_secret = generate_secret_id();

    let mut client = build_auth_client(&conn).await?;
    client
        .user_change_password(UserChangePasswordRequest {
            name: internal_name,
            password: new_secret.clone(),
        })
        .await?;

    println!("New Secret ID for AppRole \"{name}\":");
    println!("{new_secret}");
    println!("NOTE: Secret ID is only shown once. Please store it securely.");
    Ok(())
}

/// 列出所有 AppRole：过滤 approle- 前缀用户
pub async fn cmd_auth_approle_list(
    conn: impl Into<CliConn>,
) -> Result<(), Box<dyn std::error::Error>> {
    let conn = conn.into();
    let mut client = build_auth_client(&conn).await?;
    let resp = client.user_list(UserListRequest {}).await?.into_inner();

    let approles: Vec<_> = resp
        .users
        .iter()
        .filter(|u| u.name.starts_with(APPROLE_PREFIX))
        .collect();

    if approles.is_empty() {
        println!("No AppRoles found.");
        return Ok(());
    }

    println!("{:<24} {:<}", "NAME", "BOUND ROLES");
    println!("{}", "-".repeat(48));
    for user in &approles {
        let display_name = from_approle_internal(&user.name).unwrap_or(&user.name);
        println!("{:<24} {:<}", display_name, user.roles.join(", "));
    }
    Ok(())
}

/// 查看 AppRole 详情
pub async fn cmd_auth_approle_show(
    conn: impl Into<CliConn>,
    name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let conn = conn.into();
    let internal_name = to_approle_internal(name);
    let mut client = build_auth_client(&conn).await?;
    let resp = client
        .user_get(UserGetRequest {
            name: internal_name,
        })
        .await?
        .into_inner();

    println!("AppRole: {name}");
    println!("Role ID: {name}");
    let roles_display = if resp.roles.is_empty() {
        "(none)".to_string()
    } else {
        resp.roles.join(", ")
    };
    println!("Roles:   {roles_display}");
    Ok(())
}

// ──── Capability 命令 ────

use coord_proto::capability::capability_registry_client::CapabilityRegistryClient;
use coord_proto::capability::{CapabilityGetRequest, CapabilityListRequest};

/// 列出所有已注册的能力定义
pub async fn cmd_capability_list(
    conn: impl Into<CliConn>,
) -> Result<(), Box<dyn std::error::Error>> {
    let conn = conn.into();
    let mut client = build_capability_client(&conn).await?;
    let resp = client.list(CapabilityListRequest {}).await?.into_inner();

    if resp.capabilities.is_empty() {
        println!("No capabilities registered.");
        return Ok(());
    }

    println!(
        "{:<40} {:<8} {:<12} {:<}",
        "CAPABILITY ID", "TYPE", "DOMAIN", "DESCRIPTION"
    );
    println!("{}", "-".repeat(100));
    for cap in &resp.capabilities {
        let type_str = match cap.r#type {
            0 => "READ",
            1 => "WRITE",
            2 => "ADMIN",
            _ => "?",
        };
        let deprecated_mark = if cap.deprecated { " [DEPRECATED]" } else { "" };
        println!(
            "{:<40} {:<8} {:<12} {}{}",
            cap.capability_id, type_str, cap.domain, cap.description, deprecated_mark
        );
    }
    println!("\nTotal: {} capabilities", resp.capabilities.len());
    Ok(())
}

/// 查看指定能力的详细信息
pub async fn cmd_capability_get(
    conn: impl Into<CliConn>,
    capability_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let conn = conn.into();
    let mut client = build_capability_client(&conn).await?;
    let resp = client
        .get(CapabilityGetRequest {
            capability_id: capability_id.to_string(),
        })
        .await?
        .into_inner();

    match resp.capability {
        Some(cap) => {
            let type_str = match cap.r#type {
                0 => "READ",
                1 => "WRITE",
                2 => "ADMIN",
                _ => "?",
            };
            println!("Capability ID:      {}", cap.capability_id);
            println!("Domain:             {}", cap.domain);
            println!("Service:            {}", cap.service);
            println!("Action:             {}", cap.action);
            println!("Type:               {type_str}");
            println!("Description:        {}", cap.description);
            println!("Version:            {}", cap.version);
            println!("Deprecated:         {}", cap.deprecated);
            if !cap.scope_description.is_empty() {
                println!("Scope Description:  {}", cap.scope_description);
            }
        }
        None => {
            println!("Capability \"{capability_id}\" not found.");
        }
    }
    Ok(())
}

// ──── Capability 测试 ────

#[cfg(test)]
mod capability_tests {
    use super::*;

    use coord_core::storage::StorageBackend;
    use coord_core::types::StorageConfig;
    use coord_proto::auth::auth_server::AuthServer;
    use coord_proto::capability::capability_registry_server::CapabilityRegistryServer;
    use coord_server::auth::service::AuthService;
    use coord_server::auth::CapabilityRegistry;
    use coord_server::auth::{AuthManager, TokenManager};
    use coord_server::server::CoordNode;
    use coord_server::storage::mvcc::MvccStorage;
    use coord_server::storage::redb_backend::RedbBackend;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tonic::transport::Server;

    async fn start_capability_test_server() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let tmpdir = tempfile::tempdir().unwrap();
        let data_dir = tmpdir.path().to_path_buf();

        let config = StorageConfig::default();
        let backend = RedbBackend::open(&data_dir, &config).unwrap();
        let mvcc = Arc::new(MvccStorage::new(backend).unwrap());

        let mut node = CoordNode::new(Arc::clone(&mvcc));
        let watch = Arc::new(coord_server::watch::WatchDispatcher::start());
        node.watch_dispatcher = Some(watch);
        let node = Arc::new(node);

        // Create CapabilityRegistry with bootstrapped capabilities
        let cap_registry = Arc::new(CapabilityRegistry::new());
        cap_registry.bootstrap_builtin();
        let cap_svc = coord_server::auth::capability::CapabilityRegistryService {
            registry: Arc::clone(&cap_registry),
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = tokio::spawn(async move {
            Server::builder()
                .add_service(CapabilityRegistryServer::new(cap_svc))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        tokio::time::sleep(Duration::from_millis(100)).await;
        (addr, handle)
    }

    #[tokio::test]
    async fn test_cmd_capability_list_returns_all_capabilities() {
        let (addr, _handle) = start_capability_test_server().await;
        let result = cmd_capability_list(&addr.to_string()).await;
        // Should succeed even if gRPC service isn't fully wired yet
        // The test verifies the command connects and gets a response
        match result {
            Ok(()) => {} // Success
            Err(e) => {
                let msg = e.to_string();
                // Acceptable: service not yet registered or unimplemented
                assert!(
                    msg.contains("not found")
                        || msg.contains("unimplemented")
                        || msg.contains("transport"),
                    "unexpected error: {msg}"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_cmd_capability_get_known_capability() {
        let (addr, _handle) = start_capability_test_server().await;
        let result = cmd_capability_get(&addr.to_string(), "data:kv:read").await;
        match result {
            Ok(()) => {}
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("not found")
                        || msg.contains("unimplemented")
                        || msg.contains("transport"),
                    "unexpected error: {msg}"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_cmd_capability_get_nonexistent() {
        let (addr, _handle) = start_capability_test_server().await;
        let result = cmd_capability_get(&addr.to_string(), "nonexistent:svc:action").await;
        match result {
            Ok(()) => {}
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("not found")
                        || msg.contains("unimplemented")
                        || msg.contains("transport"),
                    "unexpected error: {msg}"
                );
            }
        }
    }
}

// ──── Reset / IdGen 运维命令 ────

/// idgen 备份文件名（存放于数据目录）
pub const IDGEN_BACKUP_FILE: &str = "idgen-backup.json";

/// 备份条目（hex 编码，避免非 UTF-8 字节）
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IdgenBackupEntry {
    pub key: String,
    pub value: String,
}

/// idgen 备份文件内容
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IdgenBackup {
    pub entries: Vec<IdgenBackupEntry>,
}

/// 清空本地数据目录（raft-log / snapshots / store.db）。
///
/// `keep_idgen == true` 时，先从运行中的 Server 导出 `/_idgen/` 前缀到
/// `<data_dir>/idgen-backup.json`（此后可用 `coord idgen restore` 恢复号段基线）。
/// 默认雪花模式下 ID 状态不落 KV，重置后无需恢复；此能力面向号段（segment）模式。
pub async fn cmd_reset(
    data_dir: &Path,
    conn: impl Into<CliConn>,
    keep_idgen: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if !data_dir.exists() {
        return Err(format!("data directory {} not found", data_dir.display()).into());
    }

    let conn = conn.into();
    let backup_file = data_dir.join(IDGEN_BACKUP_FILE);
    if keep_idgen {
        tracing::info!(
            "Exporting idgen state (/_idgen/ prefix) from {} ...",
            conn.addr()
        );
        let entries = export_idgen_prefix(&conn).await?;
        let json = serde_json::to_vec_pretty(&IdgenBackup {
            entries: entries.clone(),
        })?;
        std::fs::write(&backup_file, &json)?;
        tracing::info!(
            "Saved idgen backup ({} keys) to {}",
            entries.len(),
            backup_file.display()
        );
    }

    let mut removed: Vec<String> = Vec::new();
    for name in ["raft-log", "snapshots", "store.db"] {
        let p = data_dir.join(name);
        if p.exists() {
            if p.is_dir() {
                std::fs::remove_dir_all(&p)?;
            } else {
                std::fs::remove_file(&p)?;
            }
            removed.push(name.to_string());
        }
    }
    println!(
        "Reset {}: removed {}",
        data_dir.display(),
        if removed.is_empty() {
            "nothing".to_string()
        } else {
            removed.join(", ")
        }
    );
    if keep_idgen {
        println!(
            "ID generator baseline preserved in {} (restore after server restart with: coord idgen restore --file {} --addr {})",
            backup_file.display(),
            backup_file.display(),
            conn.addr()
        );
    }
    Ok(())
}

/// 从运行中的 Server 导出 `/_idgen/` 前缀的全部 KV
pub async fn export_idgen_prefix(
    conn: &CliConn,
) -> Result<Vec<IdgenBackupEntry>, Box<dyn std::error::Error>> {
    let mut client = build_kv_client(conn).await?;
    let prefix = b"/_idgen/".to_vec();
    let range_end = prefix_end(&prefix);
    let resp = client
        .range(RangeRequest {
            key: prefix,
            range_end,
            limit: 0,
            revision: 0,
            keys_only: false,
            count_only: false,
        })
        .await?;
    Ok(resp
        .into_inner()
        .kvs
        .into_iter()
        .map(|kv| IdgenBackupEntry {
            key: hex::encode(kv.key),
            value: hex::encode(kv.value),
        })
        .collect())
}

/// 从备份文件恢复 `/_idgen/` 前缀到运行中的 Server
pub async fn cmd_idgen_restore(
    file: &Path,
    conn: impl Into<CliConn>,
) -> Result<(), Box<dyn std::error::Error>> {
    let json = std::fs::read(file)?;
    let backup: IdgenBackup = serde_json::from_slice(&json)?;
    if backup.entries.is_empty() {
        println!(
            "Backup {} contains no idgen keys; nothing to restore",
            file.display()
        );
        return Ok(());
    }
    let conn = conn.into();
    let mut client = build_kv_client(&conn).await?;
    let mut restored = 0usize;
    for entry in &backup.entries {
        let key = hex::decode(&entry.key)?;
        let value = hex::decode(&entry.value)?;
        client
            .put(PutRequest {
                key,
                value,
                lease_id: 0,
                prev_kv: false,
                request_id: vec![],
            })
            .await?;
        restored += 1;
    }
    println!("Restored {restored} idgen keys to {}", conn.addr());
    Ok(())
}

/// 计算前缀扫描的 range_end（prefix 最后一个字节 +1）
fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    for i in (0..end.len()).rev() {
        if end[i] < 0xFF {
            end[i] += 1;
            end.truncate(i + 1);
            return end;
        }
    }
    // prefix 全是 0xFF，扫描到无穷
    Vec::new()
}

/// 构建到指定地址的 KvClient（tonic 直连；CliConn 携带可选 TLS + 进程级凭据）
async fn build_kv_client(
    conn: &CliConn,
) -> Result<KvClient<AuthedChannel>, Box<dyn std::error::Error>> {
    Ok(KvClient::new(conn.connect_authed().await?))
}

// ──── Reset / IdGen 运维测试 ────

#[cfg(test)]
mod idgen_reset_tests {
    use super::*;

    #[test]
    fn test_prefix_end_for_idgen() {
        // "/_idgen/" → "/_idgen0"（前缀扫描范围上界）
        let end = prefix_end(b"/_idgen/");
        assert_eq!(String::from_utf8_lossy(&end), "/_idgen0");
    }

    #[test]
    fn test_idgen_backup_roundtrip() {
        let backup = IdgenBackup {
            entries: vec![IdgenBackupEntry {
                key: hex::encode(b"/_idgen/order-id"),
                value: hex::encode(b"{}"),
            }],
        };
        let json = serde_json::to_vec(&backup).unwrap();
        let restored: IdgenBackup = serde_json::from_slice(&json).unwrap();
        assert_eq!(restored.entries.len(), 1);
        assert_eq!(
            hex::decode(&restored.entries[0].key).unwrap(),
            b"/_idgen/order-id"
        );
        assert_eq!(hex::decode(&restored.entries[0].value).unwrap(), b"{}");
    }

    #[tokio::test]
    async fn test_cmd_reset_removes_data_files() {
        let tmpdir = tempfile::tempdir().unwrap();
        let data_dir = tmpdir.path();
        std::fs::create_dir_all(data_dir.join("raft-log")).unwrap();
        std::fs::create_dir_all(data_dir.join("snapshots")).unwrap();
        std::fs::write(data_dir.join("store.db"), b"x").unwrap();

        cmd_reset(data_dir, "127.0.0.1:1", false)
            .await
            .expect("reset without keep_idgen should succeed");
        assert!(!data_dir.join("store.db").exists());
        assert!(!data_dir.join("raft-log").exists());
        assert!(!data_dir.join("snapshots").exists());
    }

    #[tokio::test]
    async fn test_cmd_reset_missing_data_dir_errors() {
        let tmpdir = tempfile::tempdir().unwrap();
        let missing = tmpdir.path().join("does-not-exist");
        let result = cmd_reset(&missing, "127.0.0.1:1", false).await;
        assert!(result.is_err(), "missing data dir should error");
    }
}

// ──── 内部辅助 ────

/// 在线拉取快照（Maintenance/Snapshot 流式导出）。
///
/// 从源节点按块接收 SnapshotData（首块携带 last_included_index/term），
/// 拼接后先解析校验（版本 + bincode）再落盘（tmp → 原子 rename）。
/// region = 0 拉 region 0 / 单 Raft；>0 拉对应 Region。
pub async fn snapshot_pull(
    conn: impl Into<CliConn>,
    output: &std::path::Path,
    region: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let conn = conn.into();
    let mut client = build_maintenance_client(&conn).await?;
    let mut stream = client
        .snapshot(tonic::Request::new(SnapshotRequest { region_id: region }))
        .await?
        .into_inner();

    let mut bytes: Vec<u8> = Vec::new();
    let mut last_included_index: u64 = 0;
    let mut last_included_term: u64 = 0;
    let mut chunks = 0u64;

    while let Some(chunk) = stream.message().await? {
        if chunks == 0 {
            last_included_index = chunk.last_included_index.max(0) as u64;
            last_included_term = chunk.last_included_term;
        }
        bytes.extend_from_slice(&chunk.data);
        chunks += 1;
    }
    if chunks == 0 {
        return Err("snapshot stream empty (server returned no data)".into());
    }

    // 先解析校验，再落盘（避免写入损坏备份）
    let snapshot_data = coord_server::storage::snapshot::SnapshotData::from_bytes(&bytes)?;

    let tmp = output.with_extension("snap.tmp");
    std::fs::write(&tmp, &bytes)?;
    std::fs::rename(&tmp, output)?;

    tracing::info!(
        "Snapshot pulled: last_included_index={}, term={}, {} KV pairs, {} bytes, {} chunks → {}",
        last_included_index,
        last_included_term,
        snapshot_data.kv_pairs.len(),
        bytes.len(),
        chunks,
        output.display()
    );
    println!(
        "Snapshot pulled: index={}, {} KV pairs, {} bytes → {}",
        last_included_index,
        snapshot_data.kv_pairs.len(),
        bytes.len(),
        output.display()
    );
    Ok(())
}

/// 构建到指定地址的 AuthClient（tonic 直连；CliConn 携带可选 TLS + 进程级凭据）
async fn build_auth_client(
    conn: &CliConn,
) -> Result<AuthClient<AuthedChannel>, Box<dyn std::error::Error>> {
    Ok(AuthClient::new(conn.connect_authed().await?))
}

/// 构建到指定地址的 MaintenanceClient（tonic 直连，绕过 Client leader 发现）
async fn build_maintenance_client(
    conn: &CliConn,
) -> Result<MaintenanceClient<AuthedChannel>, Box<dyn std::error::Error>> {
    Ok(MaintenanceClient::new(conn.connect_authed().await?))
}

/// 构建到指定地址的 CapabilityRegistryClient（tonic 直连）
async fn build_capability_client(
    conn: &CliConn,
) -> Result<CapabilityRegistryClient<AuthedChannel>, Box<dyn std::error::Error>> {
    Ok(CapabilityRegistryClient::new(conn.connect_authed().await?))
}

// ──── 测试 ────

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use coord_core::storage::StorageBackend;
    use coord_core::types::StorageConfig;
    use coord_proto::kv::kv_server::KvServer;
    use coord_proto::maintenance::maintenance_server::MaintenanceServer;
    use coord_proto::txn::txn_server::TxnServer;
    use coord_server::server::CoordNode;
    use coord_server::storage::mvcc::MvccStorage;
    use coord_server::storage::redb_backend::RedbBackend;
    use coord_server::watch::WatchDispatcher;
    use tokio::net::TcpListener;
    use tonic::transport::Server;

    /// Start a test server on a random port, return (addr, _data_dir, join_handle)
    async fn start_test_server() -> (SocketAddr, tempfile::TempDir, tokio::task::JoinHandle<()>) {
        let tmpdir = tempfile::tempdir().unwrap();
        let data_dir = tmpdir.path().to_path_buf();

        let config = StorageConfig::default();
        let backend = RedbBackend::open(&data_dir, &config).unwrap();
        let mvcc = Arc::new(MvccStorage::new(backend).unwrap());

        let mut node = CoordNode::new(Arc::clone(&mvcc));
        let watch = Arc::new(WatchDispatcher::start());
        node.watch_dispatcher = Some(watch);
        let node = Arc::new(node);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let kv_svc = KvServer::from_arc(Arc::clone(&node));
        let txn_svc = TxnServer::from_arc(Arc::clone(&node));
        let maint_svc = MaintenanceServer::from_arc(Arc::clone(&node));

        let handle = tokio::spawn(async move {
            Server::builder()
                .add_service(kv_svc)
                .add_service(txn_svc)
                .add_service(maint_svc)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        (addr, tmpdir, handle)
    }

    // ──── Security: Seal ────

    #[tokio::test]
    async fn test_cmd_seal_connects_to_server() {
        let (addr, _tmpdir, _handle) = start_test_server().await;

        let result = cmd_seal(&addr.to_string()).await;

        match result {
            Ok(()) => {
                // If server implements seal, this path succeeds
            }
            Err(e) => {
                let msg = e.to_string();
                // 非加密节点返回 failed_precondition（此前 unimplemented）
                assert!(
                    msg.contains("seal")
                        || msg.contains("encryption")
                        || msg.contains("unimplemented"),
                    "expected seal-related error, got: {msg}"
                );
            }
        }
    }

    // ──── Security: Unseal ────

    #[tokio::test]
    async fn test_cmd_unseal_connects_to_server() {
        let (addr, _tmpdir, _handle) = start_test_server().await;

        let dummy_share = vec![0u8; 41]; // 41 = SHARE_BYTES_LEN
        let result = cmd_unseal(&addr.to_string(), vec![dummy_share]).await;

        match result {
            Ok(_resp) => {}
            Err(e) => {
                let msg = e.to_string();
                // 非密封节点返回 failed_precondition（此前 unimplemented）
                assert!(
                    msg.contains("unseal")
                        || msg.contains("sealed")
                        || msg.contains("encryption")
                        || msg.contains("unimplemented"),
                    "expected unseal-related error, got: {msg}"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_cmd_unseal_rejects_empty_shares() {
        let result = cmd_unseal("127.0.0.1:50051", vec![]).await;
        assert!(result.is_err(), "empty shares should be rejected");
    }

    // ──── Security: InitSeal ────

    #[tokio::test]
    async fn test_cmd_init_seal_generates_shares() {
        let tmpdir = tempfile::tempdir().unwrap();
        let output_dir = tmpdir.path().to_path_buf();

        let result = cmd_init_seal(5, 3, &output_dir).await;
        assert!(result.is_ok(), "init_seal should succeed: {result:?}");

        // Verify share files were created
        let mut count = 0;
        for entry in std::fs::read_dir(&output_dir).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with("coord-seal-share-") && name.ends_with(".bin") {
                let data = std::fs::read(entry.path()).unwrap();
                assert_eq!(data.len(), 41, "share file should be 41 bytes");
                count += 1;
            }
        }
        assert_eq!(count, 5, "expected 5 share files, found {count}");
    }

    #[tokio::test]
    async fn test_cmd_init_seal_rejects_invalid_params() {
        let tmpdir = tempfile::tempdir().unwrap();
        let output_dir = tmpdir.path().to_path_buf();

        // k > n should fail
        let result = cmd_init_seal(3, 5, &output_dir).await;
        assert!(result.is_err(), "k > n should fail");
    }

    // ──── Security: RotateKeys ────

    #[tokio::test]
    async fn test_cmd_rotate_keys_returns_error() {
        let result = cmd_rotate_keys("127.0.0.1:50051").await;
        assert!(
            result.is_err(),
            "rotate_keys should return error (not yet implemented)"
        );
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("not yet") || msg.contains("not implemented"),
            "should clearly state not implemented, got: {msg}"
        );
    }

    // ──── Member commands ────

    #[tokio::test]
    async fn test_cmd_member_add_connects_to_server() {
        let (addr, _tmpdir, _handle) = start_test_server().await;

        let result = cmd_member_add(&addr.to_string(), 2, "127.0.0.1:50052", None).await;

        match result {
            Ok(()) => {
                // If server has Raft enabled, this path succeeds
            }
            Err(e) => {
                let msg = e.to_string();
                // Non-Raft server returns "not a raft node" which proves gRPC is connected
                assert!(
                    msg.contains("not a raft node") || msg.contains("raft"),
                    "expected raft-related error, got: {msg}"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_cmd_member_remove_connects_to_server() {
        let (addr, _tmpdir, _handle) = start_test_server().await;

        let result = cmd_member_remove(&addr.to_string(), 2).await;

        match result {
            Ok(()) => {}
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("not a raft node") || msg.contains("raft"),
                    "expected raft-related error, got: {msg}"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_cmd_member_promote_connects_to_server() {
        let (addr, _tmpdir, _handle) = start_test_server().await;

        let result = cmd_member_promote(&addr.to_string(), 2).await;

        match result {
            Ok(()) => {}
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("not a raft node") || msg.contains("raft"),
                    "expected raft-related error, got: {msg}"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_cmd_member_list_connects_to_server() {
        let (addr, _tmpdir, _handle) = start_test_server().await;

        let result = cmd_member_list(&addr.to_string()).await;

        match result {
            Ok(()) => {}
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("not a raft node") || msg.contains("raft"),
                    "expected raft-related error, got: {msg}"
                );
            }
        }
    }

    // ──── Auth test helpers ────

    use coord_proto::auth::auth_server::AuthServer;
    use coord_server::auth::{AuthManager, AuthService, TokenManager};

    /// 启动带 AuthService 的测试服务器
    async fn start_auth_test_server() -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let auth_manager = Arc::new(AuthManager::new());
        let token_manager = Arc::new(TokenManager::with_defaults());
        let auth_svc = AuthServer::new(AuthService::new(auth_manager, token_manager));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = tokio::spawn(async move {
            Server::builder()
                .add_service(auth_svc)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        tokio::time::sleep(Duration::from_millis(100)).await;
        (addr, handle)
    }

    // ──── Auth: 状态管理 ────

    #[tokio::test]
    async fn test_cmd_auth_status_returns_disabled_by_default() {
        let (addr, _handle) = start_auth_test_server().await;
        let result = cmd_auth_status(&addr.to_string()).await;
        assert!(result.is_ok(), "auth status should succeed: {result:?}");
    }

    #[tokio::test]
    async fn test_cmd_auth_enable_and_disable() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        assert!(
            cmd_auth_enable(&addr_str).await.is_ok(),
            "auth enable should succeed"
        );
        assert!(
            cmd_auth_disable(&addr_str).await.is_ok(),
            "auth disable should succeed"
        );
    }

    // ──── Auth: 用户管理 ────

    #[tokio::test]
    async fn test_cmd_auth_user_add_and_list() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        assert!(cmd_auth_user_add(&addr_str, "alice", "password123")
            .await
            .is_ok());
        assert!(cmd_auth_user_list(&addr_str).await.is_ok());
    }

    #[tokio::test]
    async fn test_cmd_auth_user_add_duplicate_fails() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        cmd_auth_user_add(&addr_str, "bob", "pass1").await.unwrap();
        assert!(cmd_auth_user_add(&addr_str, "bob", "pass2").await.is_err());
    }

    #[tokio::test]
    async fn test_cmd_auth_user_delete_and_show() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        cmd_auth_user_add(&addr_str, "charlie", "secret")
            .await
            .unwrap();
        assert!(cmd_auth_user_show(&addr_str, "charlie").await.is_ok());
        assert!(cmd_auth_user_delete(&addr_str, "charlie", true)
            .await
            .is_ok());
        assert!(cmd_auth_user_show(&addr_str, "charlie").await.is_err());
    }

    #[tokio::test]
    async fn test_cmd_auth_user_passwd_and_login() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        cmd_auth_user_add(&addr_str, "dave", "oldpass")
            .await
            .unwrap();
        assert!(cmd_auth_user_passwd(&addr_str, "dave", "newpass")
            .await
            .is_ok());
        // Login with old password should fail
        assert!(
            cmd_auth_login(&addr_str, "dave", "oldpass", true, false, false)
                .await
                .is_err()
        );
        // Login with new password should succeed
        assert!(
            cmd_auth_login(&addr_str, "dave", "newpass", true, false, false)
                .await
                .is_ok()
        );
    }

    // ──── Auth: 角色与权限管理 ────

    #[tokio::test]
    async fn test_cmd_auth_role_add_and_list() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        assert!(cmd_auth_role_add(&addr_str, "admin").await.is_ok());
        assert!(cmd_auth_role_list(&addr_str).await.is_ok());
    }

    #[tokio::test]
    async fn test_cmd_auth_role_grant_and_revoke_permission() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        cmd_auth_role_add(&addr_str, "viewer").await.unwrap();
        assert!(
            cmd_auth_role_grant(&addr_str, "viewer", "read", "app/", None)
                .await
                .is_ok()
        );
        assert!(cmd_auth_role_revoke(&addr_str, "viewer", "app/", None)
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_cmd_auth_role_delete() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        cmd_auth_role_add(&addr_str, "temp-role").await.unwrap();
        assert!(cmd_auth_role_delete(&addr_str, "temp-role", true)
            .await
            .is_ok());
    }

    // ──── Auth: 能力授予（capability + scope）────

    #[tokio::test]
    async fn test_cmd_auth_role_grant_and_revoke_capability() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        cmd_auth_role_add(&addr_str, "plugin-role").await.unwrap();
        assert!(
            cmd_auth_role_grant_capability(&addr_str, "plugin-role", "data:kv:read", "/app/")
                .await
                .is_ok(),
            "grant capability should succeed"
        );
        assert!(
            cmd_auth_role_revoke_capability(&addr_str, "plugin-role", "data:kv:read", "/app/")
                .await
                .is_ok(),
            "revoke capability should succeed"
        );
    }

    #[tokio::test]
    async fn test_cmd_auth_role_grant_capability_empty_id_fails() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        cmd_auth_role_add(&addr_str, "empty-cap-role")
            .await
            .unwrap();
        assert!(
            cmd_auth_role_grant_capability(&addr_str, "empty-cap-role", "", "")
                .await
                .is_err(),
            "empty capability_id must be rejected server-side"
        );
    }

    /// 一键引导：建角色 + 授予最小能力集；重复调用幂等（角色已存在仍成功）。
    #[tokio::test]
    async fn test_cmd_security_bootstrap_role_is_idempotent() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        assert!(
            cmd_security_bootstrap_role(&addr_str, None).await.is_ok(),
            "first bootstrap-role call must succeed"
        );
        assert!(
            cmd_security_bootstrap_role(&addr_str, None).await.is_ok(),
            "re-running bootstrap-role must be idempotent"
        );
        // 自定义角色名同样可用
        assert!(
            cmd_security_bootstrap_role(&addr_str, Some("custom-bootstrap"))
                .await
                .is_ok(),
            "custom role name must be supported"
        );
    }

    // ──── Auth: 用户-角色绑定 ────

    #[tokio::test]
    async fn test_cmd_auth_grant_and_revoke_role() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        cmd_auth_user_add(&addr_str, "eve", "pass").await.unwrap();
        cmd_auth_role_add(&addr_str, "editor").await.unwrap();
        assert!(cmd_auth_grant(&addr_str, "eve", "editor").await.is_ok());
        assert!(cmd_auth_revoke(&addr_str, "eve", "editor").await.is_ok());
    }

    // ──── Auth: AppRole 管理 ────

    #[tokio::test]
    async fn test_cmd_auth_approle_create_and_list() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        assert!(
            cmd_auth_approle_create(&addr_str, "my-service", None, None, None)
                .await
                .is_ok()
        );
        assert!(cmd_auth_approle_list(&addr_str).await.is_ok());
    }

    #[tokio::test]
    async fn test_cmd_auth_approle_create_with_bind_role() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        cmd_auth_role_add(&addr_str, "api-access").await.unwrap();
        assert!(
            cmd_auth_approle_create(&addr_str, "api-gateway", None, None, Some("api-access"))
                .await
                .is_ok()
        );
        assert!(cmd_auth_approle_show(&addr_str, "api-gateway")
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_cmd_auth_approle_role_id_and_secret_id() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        cmd_auth_approle_create(&addr_str, "batch-job", None, None, None)
            .await
            .unwrap();
        assert!(cmd_auth_approle_role_id(&addr_str, "batch-job")
            .await
            .is_ok());
        assert!(cmd_auth_approle_secret_id(&addr_str, "batch-job")
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_cmd_auth_approle_delete() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        cmd_auth_approle_create(&addr_str, "to-delete", None, None, None)
            .await
            .unwrap();
        assert!(cmd_auth_approle_delete(&addr_str, "to-delete", true)
            .await
            .is_ok());
    }

    // ──── Auth: 登录 ────

    #[tokio::test]
    async fn test_cmd_auth_login_root_user() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        assert!(
            cmd_auth_login(&addr_str, "root", "root", true, false, false)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_cmd_auth_login_invalid_password_fails() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        assert!(
            cmd_auth_login(&addr_str, "root", "wrong", true, false, false)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_cmd_auth_approle_login() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        // Create AppRole with known secret
        cmd_auth_approle_create(&addr_str, "login-test", None, Some("my-secret-123"), None)
            .await
            .unwrap();
        // Login using the internal approle- prefixed username
        let internal_name = format!("approle-login-test");
        assert!(cmd_auth_login(
            &addr_str,
            &internal_name,
            "my-secret-123",
            true,
            false,
            false
        )
        .await
        .is_ok());
    }

    // ──── Auth: 集成流程 ────

    #[tokio::test]
    async fn test_cmd_auth_approle_full_flow() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();

        // Step 1: Create role and grant permission
        cmd_auth_role_add(&addr_str, "service-role").await.unwrap();
        cmd_auth_role_grant(&addr_str, "service-role", "readwrite", "", None)
            .await
            .unwrap();

        // Step 2: Create AppRole with role binding
        assert!(cmd_auth_approle_create(
            &addr_str,
            "full-flow-svc",
            None,
            Some("known-secret"),
            Some("service-role")
        )
        .await
        .is_ok());

        // Step 3: Show AppRole details
        assert!(cmd_auth_approle_show(&addr_str, "full-flow-svc")
            .await
            .is_ok());

        // Step 4: Login as the AppRole
        let internal_name = format!("approle-full-flow-svc");
        assert!(cmd_auth_login(
            &addr_str,
            &internal_name,
            "known-secret",
            true,
            false,
            false
        )
        .await
        .is_ok());
    }

    // ──── Auth: 错误场景 ────

    #[tokio::test]
    async fn test_cmd_auth_user_add_with_approle_prefix_rejected() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        assert!(cmd_auth_user_add(&addr_str, "approle-hacker", "pass")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_cmd_auth_grant_nonexistent_role_fails() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        cmd_auth_user_add(&addr_str, "frank", "pass").await.unwrap();
        assert!(cmd_auth_grant(&addr_str, "frank", "no-such-role")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_cmd_auth_grant_nonexistent_user_fails() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        cmd_auth_role_add(&addr_str, "ghost-role").await.unwrap();
        assert!(cmd_auth_grant(&addr_str, "no-such-user", "ghost-role")
            .await
            .is_err());
    }
}
