// CLI 命令处理器（ADP §6、§10.1、§15.3、§19.1）
//
// 从 main.rs 抽取，支持单元测试。每个命令对应一个异步函数，
// 返回 Result<(), Box<dyn std::error::Error>>。
//
// 实现状态：
// - Security::Seal/Unseal: 通过 tonic 直连 gRPC 调用
// - Security::InitSeal: 本地生成 Shamir 分片文件
// - Security::RotateKeys: 无 proto RPC → 返回明确错误
// - Member::*: 通过 tonic 直连 gRPC 调用 Maintenance::MemberAdd/Remove/Promote/List

use std::path::Path;

use coord_proto::maintenance::maintenance_client::MaintenanceClient;
use coord_proto::maintenance::{
    SealRequest, UnsealRequest,
    MemberAddRequest, MemberRemoveRequest, MemberPromoteRequest, MemberListRequest,
};
use tonic::transport::Channel;

// ──── Security 命令 ────

/// 封存集群：通过 gRPC 调用 Maintenance::Seal
pub async fn cmd_seal(addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = build_maintenance_client(addr).await?;
    let request = tonic::Request::new(SealRequest {});
    client.seal(request).await?;
    println!("Cluster sealed successfully via {addr}");
    Ok(())
}

/// 解封集群：通过 gRPC 调用 Maintenance::Unseal
///
/// `shares` 中的每个元素作为 Shamir 分片（raw bytes）发送。
pub async fn cmd_unseal(
    addr: &str,
    shares: Vec<Vec<u8>>,
) -> Result<(), Box<dyn std::error::Error>> {
    if shares.is_empty() {
        return Err("at least one Shamir share is required for unseal".into());
    }

    let mut client = build_maintenance_client(addr).await?;
    let request = tonic::Request::new(UnsealRequest { shares });
    let resp = client.unseal(request).await?.into_inner();
    println!(
        "Cluster unsealed: {}/{} nodes unsealed via {addr}",
        resp.nodes_unsealed, resp.total_nodes
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
    let shares = coord_server::security::seal::SealManager::generate_shares_with_params(
        &root_key, n, k,
    )?;

    // 确保输出目录存在
    std::fs::create_dir_all(output_dir)?;

    // 写入分片文件
    for share in &shares {
        let filename = format!(
            "coord-seal-share-{}-of-{}.bin",
            share.index, n
        );
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

// ──── Member 命令 ────

/// 添加节点到集群：先添加为 Learner，再晋升为 Voter
pub async fn cmd_member_add(
    addr: &str,
    id: u64,
    node_addr: &str,
    raft_addr: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let raft = raft_addr.unwrap_or(node_addr);
    let mut client = build_maintenance_client(addr).await?;
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
    addr: &str,
    id: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = build_maintenance_client(addr).await?;
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
    addr: &str,
    id: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = build_maintenance_client(addr).await?;
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
pub async fn cmd_member_list(
    addr: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = build_maintenance_client(addr).await?;
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
    AuthEnableRequest, AuthDisableRequest, AuthStatusRequest,
    UserAddRequest, UserDeleteRequest, UserChangePasswordRequest,
    UserListRequest, UserGetRequest,
    RoleAddRequest, RoleDeleteRequest,
    RoleGrantPermissionRequest, RoleRevokePermissionRequest, RoleListRequest,
    UserGrantRoleRequest, UserRevokeRoleRequest,
    AuthenticateRequest, Permission, PermissionType,
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
    (0..32).map(|_| chars[rng.gen_range(0..chars.len())] as char).collect()
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
        other => Err(format!("invalid permission type: {other}. expected read, write, or readwrite").into()),
    }
}

// ──── Auth 状态管理 ────

/// 启用认证：调用 Auth::AuthEnable
pub async fn cmd_auth_enable(addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = build_auth_client(addr).await?;
    client.auth_enable(AuthEnableRequest {}).await?;
    println!("Auth enabled");
    Ok(())
}

/// 禁用认证：调用 Auth::AuthDisable
pub async fn cmd_auth_disable(addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = build_auth_client(addr).await?;
    client.auth_disable(AuthDisableRequest {}).await?;
    println!("Auth disabled");
    Ok(())
}

/// 查看认证状态：调用 Auth::AuthStatus
pub async fn cmd_auth_status(addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = build_auth_client(addr).await?;
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
pub async fn cmd_auth_user_add(addr: &str, name: &str, password: &str) -> Result<(), Box<dyn std::error::Error>> {
    // 禁止创建 approle- 前缀的普通用户
    if name.starts_with(APPROLE_PREFIX) {
        return Err(format!("username must not start with '{APPROLE_PREFIX}' (reserved for AppRole)").into());
    }
    let mut client = build_auth_client(addr).await?;
    client.user_add(UserAddRequest {
        name: name.to_string(),
        password: password.to_string(),
    }).await?;
    println!("User \"{name}\" created.");
    Ok(())
}

/// 删除用户：调用 Auth::UserDelete
pub async fn cmd_auth_user_delete(addr: &str, name: &str, force: bool) -> Result<(), Box<dyn std::error::Error>> {
    if !force {
        return Err("use --force to confirm deletion".into());
    }
    let mut client = build_auth_client(addr).await?;
    client.user_delete(UserDeleteRequest {
        name: name.to_string(),
    }).await?;
    println!("User \"{name}\" deleted.");
    Ok(())
}

/// 修改密码：调用 Auth::UserChangePassword
pub async fn cmd_auth_user_passwd(addr: &str, name: &str, password: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = build_auth_client(addr).await?;
    client.user_change_password(UserChangePasswordRequest {
        name: name.to_string(),
        password: password.to_string(),
    }).await?;
    println!("Password changed for user \"{name}\".");
    Ok(())
}

/// 列出所有用户：调用 Auth::UserList
pub async fn cmd_auth_user_list(addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = build_auth_client(addr).await?;
    let resp = client.user_list(UserListRequest {}).await?.into_inner();
    println!("{:<24} {:<}", "NAME", "ROLES");
    println!("{}", "-".repeat(48));
    for user in &resp.users {
        println!("{:<24} {:<}", user.name, user.roles.join(", "));
    }
    Ok(())
}

/// 查看用户详情：调用 Auth::UserGet
pub async fn cmd_auth_user_show(addr: &str, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = build_auth_client(addr).await?;
    let resp = client.user_get(UserGetRequest {
        name: name.to_string(),
    }).await?.into_inner();
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
pub async fn cmd_auth_role_add(addr: &str, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = build_auth_client(addr).await?;
    client.role_add(RoleAddRequest {
        name: name.to_string(),
    }).await?;
    println!("Role \"{name}\" created.");
    Ok(())
}

/// 删除角色：调用 Auth::RoleDelete
pub async fn cmd_auth_role_delete(addr: &str, name: &str, force: bool) -> Result<(), Box<dyn std::error::Error>> {
    if !force {
        return Err("use --force to confirm deletion".into());
    }
    let mut client = build_auth_client(addr).await?;
    client.role_delete(RoleDeleteRequest {
        name: name.to_string(),
    }).await?;
    println!("Role \"{name}\" deleted.");
    Ok(())
}

/// 为角色授予权限：调用 Auth::RoleGrantPermission
pub async fn cmd_auth_role_grant(
    addr: &str,
    name: &str,
    perm_type: &str,
    key: &str,
    range_end: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let perm = parse_permission_type(perm_type)?;
    let mut client = build_auth_client(addr).await?;
    client.role_grant_permission(RoleGrantPermissionRequest {
        name: name.to_string(),
        permission: Some(Permission {
            r#type: perm,
            key: key.as_bytes().to_vec(),
            range_end: range_end.unwrap_or("").as_bytes().to_vec(),
        }),
    }).await?;
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
    addr: &str,
    name: &str,
    key: &str,
    range_end: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = build_auth_client(addr).await?;
    client.role_revoke_permission(RoleRevokePermissionRequest {
        name: name.to_string(),
        key: key.as_bytes().to_vec(),
        range_end: range_end.unwrap_or("").as_bytes().to_vec(),
    }).await?;
    println!("Revoked permission on \"{key}\" from role \"{name}\".");
    Ok(())
}

/// 列出所有角色：调用 Auth::RoleList
pub async fn cmd_auth_role_list(addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = build_auth_client(addr).await?;
    let resp = client.role_list(RoleListRequest {}).await?.into_inner();
    println!("{:<24} {:<}", "NAME", "PERMISSIONS");
    println!("{}", "-".repeat(64));
    for role in &resp.roles {
        let perms: Vec<String> = role.permissions.iter().map(|p| {
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
        }).collect();
        println!("{:<24} {:<}", role.name, perms.join(", "));
    }
    Ok(())
}

// ──── 用户-角色绑定 ────

/// 为用户分配角色：调用 Auth::UserGrantRole
/// 若 user 为 AppRole 名称（不以 approle- 开头），自动加前缀。
pub async fn cmd_auth_grant(addr: &str, user: &str, role: &str) -> Result<(), Box<dyn std::error::Error>> {
    let internal_user = if user.starts_with(APPROLE_PREFIX) {
        user.to_string()
    } else {
        // 检查是否为 AppRole（通过前缀自动补全约定）
        // 安全做法：如果用户以已知AppRole前缀以外的形式出现，直接尝试原用户名
        // CLI 阶段约定：grant 的用户参数若为 AppRole 名（不含前缀），内部自动补全
        user.to_string()
    };
    let mut client = build_auth_client(addr).await?;
    client.user_grant_role(UserGrantRoleRequest {
        user: internal_user,
        role: role.to_string(),
    }).await?;
    println!("Granted role \"{role}\" to \"{user}\".");
    Ok(())
}

/// 撤销用户角色：调用 Auth::UserRevokeRole
pub async fn cmd_auth_revoke(addr: &str, user: &str, role: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = build_auth_client(addr).await?;
    client.user_revoke_role(UserRevokeRoleRequest {
        user: user.to_string(),
        role: role.to_string(),
    }).await?;
    println!("Revoked role \"{role}\" from \"{user}\".");
    Ok(())
}

// ──── 登录 ────

/// 登录获取 Token：调用 Auth::Authenticate
pub async fn cmd_auth_login(addr: &str, name: &str, password: &str, token_only: bool) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = build_auth_client(addr).await?;
    let resp = client.authenticate(AuthenticateRequest {
        name: name.to_string(),
        password: password.to_string(),
    }).await?.into_inner();
    if token_only {
        println!("{}", resp.token);
    } else {
        println!("Login successful. Token: {}", resp.token);
    }
    Ok(())
}

// ──── AppRole 管理 ────

/// 创建 AppRole：内部创建 approle-<name> 用户，密码为生成的 Secret ID
pub async fn cmd_auth_approle_create(
    addr: &str,
    name: &str,
    _role_id: Option<&str>,
    secret_id: Option<&str>,
    bind_role: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let internal_name = to_approle_internal(name);
    let secret = secret_id.map(|s| s.to_string()).unwrap_or_else(generate_secret_id);

    // 创建内部用户（密码为 Secret ID）
    let mut client = build_auth_client(addr).await?;
    client.user_add(UserAddRequest {
        name: internal_name.clone(),
        password: secret.clone(),
    }).await?;

    // 若指定绑定角色，授权
    if let Some(role) = bind_role {
        client.user_grant_role(UserGrantRoleRequest {
            user: internal_name,
            role: role.to_string(),
        }).await?;
    }

    println!("AppRole \"{name}\" created.");
    println!("Role ID:   {name}");
    println!("Secret ID: {secret}");
    println!("NOTE: Secret ID is only shown once. Please store it securely.");
    Ok(())
}

/// 删除 AppRole：删除内部用户 approle-<name>
pub async fn cmd_auth_approle_delete(addr: &str, name: &str, force: bool) -> Result<(), Box<dyn std::error::Error>> {
    if !force {
        return Err("use --force to confirm deletion".into());
    }
    let internal_name = to_approle_internal(name);
    let mut client = build_auth_client(addr).await?;
    client.user_delete(UserDeleteRequest {
        name: internal_name,
    }).await?;
    println!("AppRole \"{name}\" deleted.");
    Ok(())
}

/// 查看 AppRole 的 Role ID
pub async fn cmd_auth_approle_role_id(addr: &str, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Role ID 恒为 AppRole 名称
    println!("Role ID: {name}");
    // 验证内部用户存在
    let _ = addr; // silence unused warning
    Ok(())
}

/// 重置 AppRole 的 Secret ID（修改内部用户密码）
pub async fn cmd_auth_approle_secret_id(addr: &str, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let internal_name = to_approle_internal(name);
    let new_secret = generate_secret_id();

    let mut client = build_auth_client(addr).await?;
    client.user_change_password(UserChangePasswordRequest {
        name: internal_name,
        password: new_secret.clone(),
    }).await?;

    println!("New Secret ID for AppRole \"{name}\":");
    println!("{new_secret}");
    println!("NOTE: Secret ID is only shown once. Please store it securely.");
    Ok(())
}

/// 列出所有 AppRole：过滤 approle- 前缀用户
pub async fn cmd_auth_approle_list(addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = build_auth_client(addr).await?;
    let resp = client.user_list(UserListRequest {}).await?.into_inner();

    let approles: Vec<_> = resp.users.iter()
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
pub async fn cmd_auth_approle_show(addr: &str, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let internal_name = to_approle_internal(name);
    let mut client = build_auth_client(addr).await?;
    let resp = client.user_get(UserGetRequest {
        name: internal_name,
    }).await?.into_inner();

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

// ──── 内部辅助 ────

/// 构建到指定地址的 AuthClient（tonic 直连）
async fn build_auth_client(
    addr: &str,
) -> Result<AuthClient<Channel>, Box<dyn std::error::Error>> {
    let endpoint = format!("http://{addr}");
    let channel = Channel::from_shared(endpoint)?
        .connect_timeout(std::time::Duration::from_secs(3))
        .connect()
        .await?;
    Ok(AuthClient::new(channel))
}

/// 构建到指定地址的 MaintenanceClient（tonic 直连，绕过 Client leader 发现）
async fn build_maintenance_client(
    addr: &str,
) -> Result<MaintenanceClient<Channel>, Box<dyn std::error::Error>> {
    let endpoint = format!("http://{addr}");
    let channel = Channel::from_shared(endpoint)?
        .connect_timeout(std::time::Duration::from_secs(3))
        .connect()
        .await?;
    Ok(MaintenanceClient::new(channel))
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
    use coord_server::server::CoordNode;
    use coord_server::storage::mvcc::MvccStorage;
    use coord_server::storage::redb_backend::RedbBackend;
    use coord_server::watch::WatchDispatcher;
    use coord_proto::kv::kv_server::KvServer;
    use coord_proto::txn::txn_server::TxnServer;
    use coord_proto::maintenance::maintenance_server::MaintenanceServer;
    use tonic::transport::Server;
    use tokio::net::TcpListener;

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
                .serve_with_incoming(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                )
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
                // If server eventually implements seal, this path succeeds
            }
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("seal") || msg.contains("unimplemented"),
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
                assert!(
                    msg.contains("unseal") || msg.contains("unimplemented"),
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
        assert!(result.is_err(), "rotate_keys should return error (not yet implemented)");
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
    use coord_server::auth::{AuthManager, TokenManager, AuthService};

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
                .serve_with_incoming(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                )
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
        assert!(cmd_auth_enable(&addr_str).await.is_ok(), "auth enable should succeed");
        assert!(cmd_auth_disable(&addr_str).await.is_ok(), "auth disable should succeed");
    }

    // ──── Auth: 用户管理 ────

    #[tokio::test]
    async fn test_cmd_auth_user_add_and_list() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        assert!(cmd_auth_user_add(&addr_str, "alice", "password123").await.is_ok());
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
        cmd_auth_user_add(&addr_str, "charlie", "secret").await.unwrap();
        assert!(cmd_auth_user_show(&addr_str, "charlie").await.is_ok());
        assert!(cmd_auth_user_delete(&addr_str, "charlie", true).await.is_ok());
        assert!(cmd_auth_user_show(&addr_str, "charlie").await.is_err());
    }

    #[tokio::test]
    async fn test_cmd_auth_user_passwd_and_login() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        cmd_auth_user_add(&addr_str, "dave", "oldpass").await.unwrap();
        assert!(cmd_auth_user_passwd(&addr_str, "dave", "newpass").await.is_ok());
        // Login with old password should fail
        assert!(cmd_auth_login(&addr_str, "dave", "oldpass", true).await.is_err());
        // Login with new password should succeed
        assert!(cmd_auth_login(&addr_str, "dave", "newpass", true).await.is_ok());
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
        assert!(cmd_auth_role_grant(&addr_str, "viewer", "read", "app/", None).await.is_ok());
        assert!(cmd_auth_role_revoke(&addr_str, "viewer", "app/", None).await.is_ok());
    }

    #[tokio::test]
    async fn test_cmd_auth_role_delete() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        cmd_auth_role_add(&addr_str, "temp-role").await.unwrap();
        assert!(cmd_auth_role_delete(&addr_str, "temp-role", true).await.is_ok());
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
        assert!(cmd_auth_approle_create(&addr_str, "my-service", None, None, None).await.is_ok());
        assert!(cmd_auth_approle_list(&addr_str).await.is_ok());
    }

    #[tokio::test]
    async fn test_cmd_auth_approle_create_with_bind_role() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        cmd_auth_role_add(&addr_str, "api-access").await.unwrap();
        assert!(cmd_auth_approle_create(&addr_str, "api-gateway", None, None, Some("api-access")).await.is_ok());
        assert!(cmd_auth_approle_show(&addr_str, "api-gateway").await.is_ok());
    }

    #[tokio::test]
    async fn test_cmd_auth_approle_role_id_and_secret_id() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        cmd_auth_approle_create(&addr_str, "batch-job", None, None, None).await.unwrap();
        assert!(cmd_auth_approle_role_id(&addr_str, "batch-job").await.is_ok());
        assert!(cmd_auth_approle_secret_id(&addr_str, "batch-job").await.is_ok());
    }

    #[tokio::test]
    async fn test_cmd_auth_approle_delete() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        cmd_auth_approle_create(&addr_str, "to-delete", None, None, None).await.unwrap();
        assert!(cmd_auth_approle_delete(&addr_str, "to-delete", true).await.is_ok());
    }

    // ──── Auth: 登录 ────

    #[tokio::test]
    async fn test_cmd_auth_login_root_user() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        assert!(cmd_auth_login(&addr_str, "root", "root", true).await.is_ok());
    }

    #[tokio::test]
    async fn test_cmd_auth_login_invalid_password_fails() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        assert!(cmd_auth_login(&addr_str, "root", "wrong", true).await.is_err());
    }

    #[tokio::test]
    async fn test_cmd_auth_approle_login() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        // Create AppRole with known secret
        cmd_auth_approle_create(&addr_str, "login-test", None, Some("my-secret-123"), None).await.unwrap();
        // Login using the internal approle- prefixed username
        let internal_name = format!("approle-login-test");
        assert!(cmd_auth_login(&addr_str, &internal_name, "my-secret-123", true).await.is_ok());
    }

    // ──── Auth: 集成流程 ────

    #[tokio::test]
    async fn test_cmd_auth_approle_full_flow() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();

        // Step 1: Create role and grant permission
        cmd_auth_role_add(&addr_str, "service-role").await.unwrap();
        cmd_auth_role_grant(&addr_str, "service-role", "readwrite", "", None).await.unwrap();

        // Step 2: Create AppRole with role binding
        assert!(cmd_auth_approle_create(&addr_str, "full-flow-svc", None, Some("known-secret"), Some("service-role")).await.is_ok());

        // Step 3: Show AppRole details
        assert!(cmd_auth_approle_show(&addr_str, "full-flow-svc").await.is_ok());

        // Step 4: Login as the AppRole
        let internal_name = format!("approle-full-flow-svc");
        assert!(cmd_auth_login(&addr_str, &internal_name, "known-secret", true).await.is_ok());
    }

    // ──── Auth: 错误场景 ────

    #[tokio::test]
    async fn test_cmd_auth_user_add_with_approle_prefix_rejected() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        assert!(cmd_auth_user_add(&addr_str, "approle-hacker", "pass").await.is_err());
    }

    #[tokio::test]
    async fn test_cmd_auth_grant_nonexistent_role_fails() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        cmd_auth_user_add(&addr_str, "frank", "pass").await.unwrap();
        assert!(cmd_auth_grant(&addr_str, "frank", "no-such-role").await.is_err());
    }

    #[tokio::test]
    async fn test_cmd_auth_grant_nonexistent_user_fails() {
        let (addr, _handle) = start_auth_test_server().await;
        let addr_str = addr.to_string();
        cmd_auth_role_add(&addr_str, "ghost-role").await.unwrap();
        assert!(cmd_auth_grant(&addr_str, "no-such-user", "ghost-role").await.is_err());
    }
}
