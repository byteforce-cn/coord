// coord CLI 入口
//
// 组合 Server/Client 模式启动。支持以下子命令（ADP §6、§10.1、§15.3、§19.1）：
// - server:    启动 Server 节点（单节点或加入集群）
// - agent:     启动 Agent 守护进程（本地代理，Java 应用入口）
// - dev:       开发模式：同时启动 Server + Agent（对标 consul agent -dev）
// - security:  封存/解封/初始化密钥分片/轮换密钥
// - member:    动态成员管理（添加/移除/晋升/列表）
// - snapshot:  快照管理（保存/恢复）

mod config;
pub mod commands;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use openraft::rt::WatchReceiver;

use coord_core::storage::StorageBackend;
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcServer, RaftRpcService};
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::server::CoordNode;
use coord_server::storage::compaction::{CompactionConfig, CompactionManager};
use coord_server::storage::mvcc::MvccStorage;
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::watch::WatchDispatcher;
use coord_server::metrics::Metrics;
use coord_server::health;
use coord_server::tls::{self, TlsConfig};
use coord_proto::kv::kv_server::KvServer;
use coord_proto::txn::txn_server::TxnServer;
use coord_proto::lease::lease_server::LeaseServer;
use coord_proto::watch::watch_server::WatchServer;
use coord_proto::maintenance::maintenance_server::MaintenanceServer;
use coord_proto::auth::auth_server::AuthServer;
use coord_server::auth::{AuthManager, TokenManager, AuthService};
use coord_server::lease::LeaseManager;
use coord_server::timer::TimerWheel;
use coord_server::bff::{
    BffConfig, ReqwestCoreClient, HealthState, build_router,
    internal::InternalState,
};

#[derive(Parser)]
#[command(
    name = "coord",
    about = "Distributed coordination service",
    version = env!("CARGO_PKG_VERSION"),
    long_about = "Coord 是一个分布式协调服务，提供类 etcd 的强一致性键值存储与协调原语。"
)]
struct Cli {
    /// 数据目录路径（默认 /var/lib/coord）
    #[arg(long, global = true, default_value = "/var/lib/coord")]
    data_dir: PathBuf,

    /// 配置文件路径（TOML 格式）
    #[arg(long, global = true)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// 启动 Server 节点
    Server {
        /// 节点 ID
        #[arg(long, default_value = "1")]
        id: u64,

        /// gRPC 监听地址
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,

        /// Raft 内部通信地址（默认与 gRPC 同端口 +1）
        #[arg(long)]
        raft_addr: Option<String>,

        /// 加入已有集群（Leader 节点地址）
        #[arg(long)]
        join: Option<String>,

        /// Bootstrap 模式：初始化单节点集群
        #[arg(long, default_value = "false")]
        bootstrap: bool,

        /// 集群名称
        #[arg(long, default_value = "coord-cluster")]
        cluster_name: String,
    },

    /// 安全运维（封存/解封/初始化密钥分片）
    #[command(subcommand)]
    Security(SecurityCmd),

    /// 动态成员管理
    #[command(subcommand)]
    Member(MemberCmd),

    /// 快照管理
    #[command(subcommand)]
    Snapshot(SnapshotCmd),

    /// 认证与授权管理
    #[command(subcommand)]
    Auth(AuthCmd),

    /// 能力注册中心管理（查看/检索能力定义）
    #[command(subcommand)]
    Capability(CapabilityCmd),

    /// 启动 Agent 守护进程（本地代理，Java 应用入口）
    Agent {
        /// Agent 本地 gRPC 监听地址（默认 127.0.0.1:19527）
        #[arg(long, default_value = "127.0.0.1:19527")]
        agent_addr: String,

        /// HTTP 可观测性监听地址（默认 127.0.0.1:19528）
        #[arg(long, default_value = "127.0.0.1:19528")]
        http_addr: String,

        /// 成员发现模式（默认 "static"）
        #[arg(long, default_value = "static")]
        discovery: String,

        /// 静态配置的 Server 节点列表（逗号分隔）
        #[arg(long, value_delimiter = ',')]
        static_peers: Vec<String>,
    },

    /// 开发模式：同时启动 Server + Agent（单节点集群）
    Dev {
        /// Server gRPC 端口（默认 50051）
        #[arg(long, default_value = "50051")]
        grpc_port: u16,

        /// Agent gRPC 端口（默认 19527）
        #[arg(long, default_value = "19527")]
        agent_port: u16,

        /// 集群名称（默认 "coord-dev"）
        #[arg(long, default_value = "coord-dev")]
        cluster_name: String,
    },
}

// ──── Security 子命令 ────

#[derive(Subcommand)]
enum SecurityCmd {
    /// 封存集群（所有数据不可读写）
    Seal {
        /// 目标节点地址
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
    },

    /// 解封集群（需提供 ≥K 个 Shamir 分片文件路径）
    Unseal {
        /// 目标节点地址
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,

        /// Shamir 分片文件路径（可多次指定，至少需要 K 个）
        #[arg(long, required = true, num_args = 1..)]
        shares: Vec<PathBuf>,
    },

    /// 初始化密钥分片（首次 Bootstrap 后调用，生成 N 个分片文件）
    InitSeal {
        /// 总分片数（默认 5）
        #[arg(long, default_value = "5")]
        n: u8,

        /// 门限（默认 3）
        #[arg(long, default_value = "3")]
        k: u8,

        /// 分片输出目录
        #[arg(long, default_value = ".")]
        output_dir: PathBuf,
    },

    /// 轮换数据加密密钥（DEK）
    RotateKeys {
        /// 目标节点地址
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
    },
}

// ──── Member 子命令 ────

#[derive(Subcommand)]
enum MemberCmd {
    /// 添加节点到集群
    Add {
        /// 目标节点地址
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,

        /// 新节点 ID
        #[arg(long)]
        id: u64,

        /// 新节点 gRPC 地址
        #[arg(long)]
        node_addr: String,

        /// 新节点 Raft 地址（默认与 gRPC 端口 +1）
        #[arg(long)]
        raft_addr: Option<String>,
    },

    /// 从集群移除节点
    Remove {
        /// 目标节点地址
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,

        /// 要移除的节点 ID
        #[arg(long)]
        id: u64,
    },

    /// 将 Learner 晋升为 Voter
    Promote {
        /// 目标节点地址
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,

        /// 要晋升的节点 ID
        #[arg(long)]
        id: u64,
    },

    /// 列出所有节点及其状态
    List {
        /// 目标节点地址
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
    },
}

// ──── Snapshot 子命令 ────

#[derive(Subcommand)]
enum SnapshotCmd {
    /// 导出当前状态机快照
    Save {
        /// 目标节点地址
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,

        /// 快照输出文件路径
        #[arg(long, default_value = "coord-snapshot.snap")]
        output: PathBuf,
    },

    /// 从快照恢复节点数据
    Restore {
        /// 快照文件路径
        #[arg(long)]
        snapshot: PathBuf,

        /// 目标数据目录
        #[arg(long, default_value = "/var/lib/coord")]
        data_dir: PathBuf,
    },
}

// ──── Auth 子命令 ────

#[derive(Subcommand)]
enum AuthCmd {
    /// 启用认证
    Enable {
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
    },

    /// 禁用认证
    Disable {
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
    },

    /// 查看认证状态
    Status {
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
    },

    /// 用户管理
    #[command(subcommand)]
    User(AuthUserCmd),

    /// AppRole（机器身份）管理
    #[command(subcommand)]
    AppRole(AuthAppRoleCmd),

    /// 角色与权限管理
    #[command(subcommand)]
    Role(AuthRoleCmd),

    /// 为用户/AppRole 分配角色
    Grant {
        /// 用户名或 AppRole 名称
        user: String,
        /// 角色名
        role: String,
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
    },

    /// 撤销用户/AppRole 的角色
    Revoke {
        /// 用户名或 AppRole 名称
        user: String,
        /// 角色名
        role: String,
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
    },

    /// 登录获取 Token
    Login {
        /// 用户名
        name: String,
        /// 仅输出 Token（便于脚本集成）
        #[arg(long, default_value = "false")]
        token_only: bool,
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
    },
}

#[derive(Subcommand)]
enum AuthUserCmd {
    /// 创建用户
    Add {
        /// 用户名
        name: String,
        /// 密码（非交互式，适用于脚本）
        #[arg(long)]
        password: Option<String>,
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
    },

    /// 删除用户
    Delete {
        /// 用户名
        name: String,
        /// 跳过确认
        #[arg(long, default_value = "false")]
        force: bool,
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
    },

    /// 修改用户密码
    Passwd {
        /// 用户名
        name: String,
        /// 新密码（非交互式）
        #[arg(long)]
        password: Option<String>,
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
    },

    /// 列出所有用户
    List {
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
    },

    /// 查看用户详情
    Show {
        /// 用户名
        name: String,
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
    },
}

#[derive(Subcommand)]
enum AuthAppRoleCmd {
    /// 创建 AppRole
    Create {
        /// AppRole 名称
        name: String,
        /// 自定义 Role ID（暂不支持，预留）
        #[arg(long)]
        role_id: Option<String>,
        /// 自定义 Secret ID（若不提供则自动生成）
        #[arg(long)]
        secret_id: Option<String>,
        /// 创建后绑定的角色
        #[arg(long)]
        bind_role: Option<String>,
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
    },

    /// 删除 AppRole
    Delete {
        /// AppRole 名称
        name: String,
        /// 跳过确认
        #[arg(long, default_value = "false")]
        force: bool,
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
    },

    /// 查看 Role ID
    RoleId {
        /// AppRole 名称
        name: String,
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
    },

    /// 重置 Secret ID
    SecretId {
        /// AppRole 名称
        name: String,
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
    },

    /// 列出所有 AppRole
    List {
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
    },

    /// 查看 AppRole 详情
    Show {
        /// AppRole 名称
        name: String,
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
    },
}

#[derive(Subcommand)]
enum AuthRoleCmd {
    /// 创建角色
    Add {
        /// 角色名
        name: String,
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
    },

    /// 删除角色
    Delete {
        /// 角色名
        name: String,
        /// 跳过确认
        #[arg(long, default_value = "false")]
        force: bool,
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
    },

    /// 为角色授予权限
    Grant {
        /// 角色名
        name: String,
        /// 权限类型: read, write, readwrite
        perm: String,
        /// Key 前缀
        key: String,
        /// Key 范围结束
        #[arg(long)]
        range_end: Option<String>,
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
    },

    /// 撤销角色权限
    Revoke {
        /// 角色名
        name: String,
        /// Key 前缀
        key: String,
        /// Key 范围结束
        #[arg(long)]
        range_end: Option<String>,
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
    },

    /// 列出所有角色
    List {
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
    },
}

// ──── Capability 子命令 ────

#[derive(Subcommand)]
enum CapabilityCmd {
    /// 列出所有已注册的能力定义
    List {
        /// 目标节点地址
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
    },

    /// 查看指定能力的详细信息
    Get {
        /// 能力 ID（如 data:kv:read, coord:lock:acquire）
        capability_id: String,
        /// 目标节点地址
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
    },
}

// ──── 入口 ────

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "coord=info".into()),
        )
        .with_target(false)
        .init();

    let cli = Cli::parse();

    // 加载配置文件（如果指定）
    let mut file_config = None;
    if let Some(ref config_path) = cli.config {
        match config::Config::from_file(config_path) {
            Ok(cfg) => {
                tracing::info!("Loaded config from {}", config_path.display());
                file_config = Some(cfg);
            }
            Err(e) => {
                tracing::error!(
                    "Failed to load config from {}: {e}",
                    config_path.display()
                );
                std::process::exit(1);
            }
        }
    }

    match cli.command {
        Commands::Server {
            id,
            addr,
            raft_addr,
            join,
            bootstrap,
            cluster_name,
        } => {
            // 构建完整配置：CLI > 配置文件 > 默认值
            let mut cfg = file_config.unwrap_or_default();
            cfg.apply_cli_overrides(
                Some(id),
                Some(&addr),
                raft_addr.as_deref(),
                Some(&cli.data_dir),
                Some(&cluster_name),
                join.as_deref(),
            );

            let raft_addr = cfg.resolve_raft_addr();
            tracing::info!(
                "Starting coord server v{}: id={}, grpc={}, raft={}, cluster={}, bootstrap={}",
                env!("CARGO_PKG_VERSION"),
                cfg.node.id,
                cfg.resolve_grpc_addr(),
                raft_addr,
                cfg.cluster.cluster_name,
                cfg.cluster.bootstrap || bootstrap
            );

            if let Some(ref join_addr) = cfg.cluster.join_addr {
                tracing::info!("Joining cluster via {}", join_addr);
            }

            // 启动服务端（带优雅关闭）
            if let Err(e) = run_server(
                &cfg,
                &raft_addr,
                bootstrap || cfg.cluster.bootstrap,
            )
            .await
            {
                tracing::error!("Server exited with error: {e}");
                std::process::exit(1);
            }
        }

        Commands::Security(cmd) => match cmd {
            SecurityCmd::Seal { addr } => {
                tracing::info!("Sealing cluster via {}", addr);
                if let Err(e) = commands::cmd_seal(&addr).await {
                    tracing::error!("Seal failed: {e}");
                    eprintln!("Error: {e}");
                    std::process::exit(1);
                }
            }
            SecurityCmd::Unseal { addr, shares } => {
                tracing::info!(
                    "Unsealing cluster via {} with {} shares",
                    addr,
                    shares.len()
                );
                let share_data: Vec<Vec<u8>> = shares
                    .iter()
                    .map(|p| {
                        std::fs::read(p).unwrap_or_else(|e| {
                            tracing::error!("Failed to read share {}: {e}", p.display());
                            std::process::exit(1);
                        })
                    })
                    .collect();
                if let Err(e) = commands::cmd_unseal(&addr, share_data).await {
                    tracing::error!("Unseal failed: {e}");
                    eprintln!("Error: {e}");
                    std::process::exit(1);
                }
            }
            SecurityCmd::InitSeal { n, k, output_dir } => {
                tracing::info!(
                    "Initializing Shamir shares: n={}, k={}, output={}",
                    n,
                    k,
                    output_dir.display()
                );
                if let Err(e) = commands::cmd_init_seal(n, k, &output_dir).await {
                    tracing::error!("InitSeal failed: {e}");
                    eprintln!("Error: {e}");
                    std::process::exit(1);
                }
            }
            SecurityCmd::RotateKeys { addr } => {
                tracing::info!("Rotating DEK via {}", addr);
                if let Err(e) = commands::cmd_rotate_keys(&addr).await {
                    tracing::error!("RotateKeys failed: {e}");
                    eprintln!("Error: {e}");
                    std::process::exit(1);
                }
            }
        },

        Commands::Member(cmd) => match cmd {
            MemberCmd::Add {
                addr,
                id,
                node_addr,
                raft_addr,
            } => {
                tracing::info!(
                    "Adding member: id={}, addr={}, raft={:?}, via {}",
                    id,
                    node_addr,
                    raft_addr,
                    addr
                );
                if let Err(e) = commands::cmd_member_add(&addr, id, &node_addr, raft_addr.as_deref()).await {
                    tracing::error!("MemberAdd failed: {e}");
                    eprintln!("Error: {e}");
                    std::process::exit(1);
                }
            }
            MemberCmd::Remove { addr, id } => {
                tracing::info!("Removing member: id={} via {}", id, addr);
                if let Err(e) = commands::cmd_member_remove(&addr, id).await {
                    tracing::error!("MemberRemove failed: {e}");
                    eprintln!("Error: {e}");
                    std::process::exit(1);
                }
            }
            MemberCmd::Promote { addr, id } => {
                tracing::info!("Promoting member: id={} via {}", id, addr);
                if let Err(e) = commands::cmd_member_promote(&addr, id).await {
                    tracing::error!("MemberPromote failed: {e}");
                    eprintln!("Error: {e}");
                    std::process::exit(1);
                }
            }
            MemberCmd::List { addr } => {
                tracing::info!("Listing members via {}", addr);
                if let Err(e) = commands::cmd_member_list(&addr).await {
                    tracing::error!("MemberList failed: {e}");
                    eprintln!("Error: {e}");
                    std::process::exit(1);
                }
            }
        },

        Commands::Snapshot(cmd) => match cmd {
            SnapshotCmd::Save { addr, output } => {
                tracing::info!("Saving snapshot via {} to {}", addr, output.display());
                if let Err(e) = snapshot_save(&addr, &output).await {
                    tracing::error!("Snapshot save failed: {e}");
                    std::process::exit(1);
                }
            }
            SnapshotCmd::Restore { snapshot, data_dir } => {
                tracing::info!(
                    "Restoring snapshot {} to {}",
                    snapshot.display(),
                    data_dir.display()
                );
                if !snapshot.exists() {
                    tracing::error!("Snapshot file not found: {}", snapshot.display());
                    std::process::exit(1);
                }
                if let Err(e) = snapshot_restore(&snapshot, &data_dir).await {
                    tracing::error!("Snapshot restore failed: {e}");
                    std::process::exit(1);
                }
            }
        },

        Commands::Auth(cmd) => match cmd {
            AuthCmd::Enable { addr } => {
                tracing::info!("Enabling auth via {}", addr);
                if let Err(e) = commands::cmd_auth_enable(&addr).await {
                    tracing::error!("AuthEnable failed: {e}");
                    eprintln!("Error: {e}");
                    std::process::exit(1);
                }
            }
            AuthCmd::Disable { addr } => {
                tracing::info!("Disabling auth via {}", addr);
                if let Err(e) = commands::cmd_auth_disable(&addr).await {
                    tracing::error!("AuthDisable failed: {e}");
                    eprintln!("Error: {e}");
                    std::process::exit(1);
                }
            }
            AuthCmd::Status { addr } => {
                tracing::info!("Checking auth status via {}", addr);
                if let Err(e) = commands::cmd_auth_status(&addr).await {
                    tracing::error!("AuthStatus failed: {e}");
                    eprintln!("Error: {e}");
                    std::process::exit(1);
                }
            }
            AuthCmd::User(cmd) => match cmd {
                AuthUserCmd::Add { name, password, addr } => {
                    let pass = match password {
                        Some(p) => p,
                        None => match commands::prompt_password_with_confirm() {
                            Ok(p) => p,
                            Err(e) => { eprintln!("Error: {e}"); std::process::exit(1); }
                        }
                    };
                    if let Err(e) = commands::cmd_auth_user_add(&addr, &name, &pass).await {
                        tracing::error!("UserAdd failed: {e}");
                        eprintln!("Error: {e}");
                        std::process::exit(1);
                    }
                }
                AuthUserCmd::Delete { name, force, addr } => {
                    if let Err(e) = commands::cmd_auth_user_delete(&addr, &name, force).await {
                        tracing::error!("UserDelete failed: {e}");
                        eprintln!("Error: {e}");
                        std::process::exit(1);
                    }
                }
                AuthUserCmd::Passwd { name, password, addr } => {
                    let pass = match password {
                        Some(p) => p,
                        None => match commands::prompt_password_with_confirm() {
                            Ok(p) => p,
                            Err(e) => { eprintln!("Error: {e}"); std::process::exit(1); }
                        }
                    };
                    if let Err(e) = commands::cmd_auth_user_passwd(&addr, &name, &pass).await {
                        tracing::error!("UserPasswd failed: {e}");
                        eprintln!("Error: {e}");
                        std::process::exit(1);
                    }
                }
                AuthUserCmd::List { addr } => {
                    if let Err(e) = commands::cmd_auth_user_list(&addr).await {
                        tracing::error!("UserList failed: {e}");
                        eprintln!("Error: {e}");
                        std::process::exit(1);
                    }
                }
                AuthUserCmd::Show { name, addr } => {
                    if let Err(e) = commands::cmd_auth_user_show(&addr, &name).await {
                        tracing::error!("UserShow failed: {e}");
                        eprintln!("Error: {e}");
                        std::process::exit(1);
                    }
                }
            },
            AuthCmd::AppRole(cmd) => match cmd {
                AuthAppRoleCmd::Create { name, role_id, secret_id, bind_role, addr } => {
                    if let Err(e) = commands::cmd_auth_approle_create(
                        &addr, &name, role_id.as_deref(), secret_id.as_deref(), bind_role.as_deref(),
                    ).await {
                        tracing::error!("AppRoleCreate failed: {e}");
                        eprintln!("Error: {e}");
                        std::process::exit(1);
                    }
                }
                AuthAppRoleCmd::Delete { name, force, addr } => {
                    if let Err(e) = commands::cmd_auth_approle_delete(&addr, &name, force).await {
                        tracing::error!("AppRoleDelete failed: {e}");
                        eprintln!("Error: {e}");
                        std::process::exit(1);
                    }
                }
                AuthAppRoleCmd::RoleId { name, addr } => {
                    if let Err(e) = commands::cmd_auth_approle_role_id(&addr, &name).await {
                        tracing::error!("AppRoleRoleId failed: {e}");
                        eprintln!("Error: {e}");
                        std::process::exit(1);
                    }
                }
                AuthAppRoleCmd::SecretId { name, addr } => {
                    if let Err(e) = commands::cmd_auth_approle_secret_id(&addr, &name).await {
                        tracing::error!("AppRoleSecretId failed: {e}");
                        eprintln!("Error: {e}");
                        std::process::exit(1);
                    }
                }
                AuthAppRoleCmd::List { addr } => {
                    if let Err(e) = commands::cmd_auth_approle_list(&addr).await {
                        tracing::error!("AppRoleList failed: {e}");
                        eprintln!("Error: {e}");
                        std::process::exit(1);
                    }
                }
                AuthAppRoleCmd::Show { name, addr } => {
                    if let Err(e) = commands::cmd_auth_approle_show(&addr, &name).await {
                        tracing::error!("AppRoleShow failed: {e}");
                        eprintln!("Error: {e}");
                        std::process::exit(1);
                    }
                }
            },
            AuthCmd::Role(cmd) => match cmd {
                AuthRoleCmd::Add { name, addr } => {
                    if let Err(e) = commands::cmd_auth_role_add(&addr, &name).await {
                        tracing::error!("RoleAdd failed: {e}");
                        eprintln!("Error: {e}");
                        std::process::exit(1);
                    }
                }
                AuthRoleCmd::Delete { name, force, addr } => {
                    if let Err(e) = commands::cmd_auth_role_delete(&addr, &name, force).await {
                        tracing::error!("RoleDelete failed: {e}");
                        eprintln!("Error: {e}");
                        std::process::exit(1);
                    }
                }
                AuthRoleCmd::Grant { name, perm, key, range_end, addr } => {
                    if let Err(e) = commands::cmd_auth_role_grant(
                        &addr, &name, &perm, &key, range_end.as_deref(),
                    ).await {
                        tracing::error!("RoleGrant failed: {e}");
                        eprintln!("Error: {e}");
                        std::process::exit(1);
                    }
                }
                AuthRoleCmd::Revoke { name, key, range_end, addr } => {
                    if let Err(e) = commands::cmd_auth_role_revoke(
                        &addr, &name, &key, range_end.as_deref(),
                    ).await {
                        tracing::error!("RoleRevoke failed: {e}");
                        eprintln!("Error: {e}");
                        std::process::exit(1);
                    }
                }
                AuthRoleCmd::List { addr } => {
                    if let Err(e) = commands::cmd_auth_role_list(&addr).await {
                        tracing::error!("RoleList failed: {e}");
                        eprintln!("Error: {e}");
                        std::process::exit(1);
                    }
                }
            },
            AuthCmd::Grant { user, role, addr } => {
                if let Err(e) = commands::cmd_auth_grant(&addr, &user, &role).await {
                    tracing::error!("Grant failed: {e}");
                    eprintln!("Error: {e}");
                    std::process::exit(1);
                }
            }
            AuthCmd::Revoke { user, role, addr } => {
                if let Err(e) = commands::cmd_auth_revoke(&addr, &user, &role).await {
                    tracing::error!("Revoke failed: {e}");
                    eprintln!("Error: {e}");
                    std::process::exit(1);
                }
            }
            AuthCmd::Login { name, token_only, addr } => {
                let pass = match commands::prompt_password(&format!("Password for {name}: ")) {
                    Ok(p) => p,
                    Err(e) => { eprintln!("Error: {e}"); std::process::exit(1); }
                };
                if let Err(e) = commands::cmd_auth_login(&addr, &name, &pass, token_only).await {
                    tracing::error!("Login failed: {e}");
                    eprintln!("Error: {e}");
                    std::process::exit(1);
                }
            }
        },

        Commands::Capability(cmd) => match cmd {
            CapabilityCmd::List { addr } => {
                tracing::info!("Listing capabilities via {}", addr);
                if let Err(e) = commands::cmd_capability_list(&addr).await {
                    tracing::error!("CapabilityList failed: {e}");
                    eprintln!("Error: {e}");
                    std::process::exit(1);
                }
            }
            CapabilityCmd::Get { capability_id, addr } => {
                tracing::info!("Getting capability {} via {}", capability_id, addr);
                if let Err(e) = commands::cmd_capability_get(&addr, &capability_id).await {
                    tracing::error!("CapabilityGet failed: {e}");
                    eprintln!("Error: {e}");
                    std::process::exit(1);
                }
            }
        },

        Commands::Agent {
            agent_addr,
            http_addr,
            discovery,
            static_peers,
        } => {
            tracing::info!(
                "Starting coord-agent v{}: agent={}, http={}, discovery={}",
                env!("CARGO_PKG_VERSION"),
                agent_addr,
                http_addr,
                discovery
            );

            let agent_config = coord_agent::AgentConfig {
                agent_addr,
                http_addr,
                data_dir: cli.data_dir.to_string_lossy().to_string(),
                discovery_mode: match discovery.as_str() {
                    "static" => coord_agent::DiscoveryMode::Static,
                    "gossip" => coord_agent::DiscoveryMode::Gossip,
                    other => {
                        tracing::error!("Unknown discovery mode: {other}");
                        std::process::exit(1);
                    }
                },
                static_peers,
                ..Default::default()
            };

            if let Err(e) = coord_agent::run_agent(agent_config).await {
                tracing::error!("Agent exited with error: {e}");
                std::process::exit(1);
            }
        },

        Commands::Dev {
            grpc_port,
            agent_port,
            cluster_name,
        } => {
            tracing::info!(
                "Starting coord dev mode v{}: server=127.0.0.1:{}, agent=127.0.0.1:{}, cluster={}",
                env!("CARGO_PKG_VERSION"),
                grpc_port,
                agent_port,
                cluster_name
            );

            if let Err(e) = run_dev(grpc_port, agent_port, &cli.data_dir, &cluster_name).await {
                tracing::error!("Dev mode exited with error: {e}");
                std::process::exit(1);
            }
        },
    }
}

// ──── Snapshot CLI 实现 ────

/// 通过网络从运行中的节点导出快照
async fn snapshot_save(
    _addr: &str,
    output: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    // 直接访问本地存储导出快照（无需 gRPC）
    tracing::info!("Exporting snapshot to {}", output.display());

    // 尝试从本地数据目录读取
    let data_dir = PathBuf::from("/var/lib/coord");
    if !data_dir.exists() {
        return Err(format!("Data directory {} not found. Is the server running?", data_dir.display()).into());
    }

    let storage_config = coord_core::types::StorageConfig::default();
    let backend = RedbBackend::open(&data_dir, &storage_config)?;
    let mvcc = MvccStorage::new(backend)?;

    let snapshot_data = coord_server::storage::snapshot::export_snapshot_data(&mvcc, 0, 0)?;
    let bytes = snapshot_data.to_bytes()?;
    std::fs::write(output, &bytes)?;

    let kv_count = snapshot_data.kv_pairs.len();
    tracing::info!(
        "Snapshot saved: {} KV pairs, {} bytes → {}",
        kv_count,
        bytes.len(),
        output.display()
    );
    println!("Snapshot saved: {} KV pairs → {}", kv_count, output.display());
    Ok(())
}

/// 从快照文件恢复到本地数据目录
async fn snapshot_restore(
    snapshot_path: &PathBuf,
    data_dir: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let bytes = std::fs::read(snapshot_path)?;
    let snapshot_data = coord_server::storage::snapshot::SnapshotData::from_bytes(&bytes)?;

    tracing::info!(
        "Restoring snapshot v{}: last_included_index={}, {} KV pairs",
        snapshot_data.version,
        snapshot_data.last_included_index,
        snapshot_data.kv_pairs.len()
    );

    // 创建新的数据目录和存储实例
    std::fs::create_dir_all(data_dir)?;
    let storage_config = coord_core::types::StorageConfig::default();
    let backend = RedbBackend::open(data_dir, &storage_config)?;
    let mvcc = MvccStorage::new(backend)?;

    coord_server::storage::snapshot::import_snapshot_data(&mvcc, &snapshot_data)?;

    let kv_count = snapshot_data.kv_pairs.len();
    tracing::info!(
        "Snapshot restored: {} KV pairs → {}",
        kv_count,
        data_dir.display()
    );
    println!("Snapshot restored: {} KV pairs → {}", kv_count, data_dir.display());
    Ok(())
}

// ──── Server 启动逻辑 ────

async fn run_server(
    cfg: &config::Config,
    raft_addr: &str,
    bootstrap: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let node_id = cfg.node.id;
    let grpc_addr = cfg.resolve_grpc_addr();
    let data_dir = cfg.resolve_data_dir();

    // 1. 创建数据目录
    std::fs::create_dir_all(&data_dir)?;

    // 2. 初始化 Redb 存储后端（共享实例）
    let storage_config = coord_core::types::StorageConfig::default();
    let backend = RedbBackend::open(&data_dir, &storage_config)?;

    // 3. 创建两个 MvccStorage 实例共享同一 Redb 后端：
    //    - mvcc_read: 用于 CoordNode 读取路径
    //    - mvcc_raft: 用于 Raft StateMachine 写入路径
    let mvcc_read = Arc::new(MvccStorage::new(backend.clone())?);
    let mvcc_raft = MvccStorage::new(backend)?;

    // 4. 初始化 Watch 分发器
    let watch_dispatcher = Arc::new(WatchDispatcher::start());

    // 5. 构建 Raft 栈
    // 5a. Raft LogStore（Redb 持久化，独立实例 raft-log/log.db）
    let log_store = LogStore::new(&data_dir).await.map_err(|e| {
        format!("create raft log store: {e}")
    })?;

    // 5b. Raft StateMachine（使用 mvcc_raft 写入存储）
    //     与 CoordNode 共享同一个 WatchDispatcher，确保 apply 路径的
    //     事件分发与 gRPC Watch 订阅者使用同一订阅表。
    let mut sm_store = StateMachineStore::new(mvcc_raft);
    sm_store.set_watch_dispatcher(Arc::clone(&watch_dispatcher));

    // 5c. Raft Network Factory（支持 Raft 节点间 TLS）
    let mut network_factory = RaftNetworkFactoryImpl::new(node_id);
    network_factory.register_node(node_id, raft_addr.to_string());
    if let Some(ref join) = cfg.cluster.join_addr {
        network_factory.register_node(0, join.to_string());
    }
    // 注册配置中的初始集群节点
    for node in &cfg.cluster.initial_nodes {
        if node.id != node_id {
            network_factory.register_node(node.id, node.raft.clone());
        }
    }

    // 配置 Raft 节点间 TLS（若安全配置中指定了证书，ADP §14.1）
    let raft_tls_config = if cfg.security.tls_cert.is_some() && cfg.security.tls_key.is_some() {
        let tls_cfg = TlsConfig::new(
            cfg.security.tls_cert.clone().unwrap(),
            cfg.security.tls_key.clone().unwrap(),
            cfg.security.tls_ca.clone(),
        );
        if tls_cfg.is_configured() {
            network_factory.set_raft_tls(tls_cfg.clone());
            tracing::info!(
                "Raft inter-node TLS enabled: cert={}, mTLS={}",
                tls_cfg.cert_path.display(),
                tls_cfg.ca_path.is_some()
            );
            Some(tls_cfg)
        } else {
            None
        }
    } else {
        None
    };

    // 5d. Raft 配置
    let raft_config = Arc::new(openraft::Config::default());

    // 5e. Raft RPC 服务
    let raft_rpc_service = RaftRpcService::new();

    // 5f. 在创建 Raft 实例之前检查是否已初始化
    //     raft.metrics() 在 Raft::new() 返回后可能尚未被异步 core task 填充，
    //     因此直接查询 LogStore 更为可靠。
    let already_initialized = if bootstrap {
        log_store.is_initialized().unwrap_or(false)
    } else {
        false
    };

    // 5g. 创建 Raft 实例
    let raft = openraft::Raft::new(
        node_id,
        raft_config,
        network_factory,
        log_store,
        sm_store,
    )
    .await
    .map_err(|e| format!("create raft instance: {e}"))?;

    // 5h. 设置 Raft 到 RPC 服务
    raft_rpc_service.set_raft(raft.clone());

    // 5i. Bootstrap 或加入集群
    if bootstrap {
        if already_initialized {
            tracing::info!("Raft cluster already initialized, skipping bootstrap");
        } else {
            tracing::info!("Bootstrapping Raft cluster");
            let mut members = BTreeMap::new();
            members.insert(node_id, openraft::impls::BasicNode::new(raft_addr));
            // 注册配置中的初始节点
            for node in &cfg.cluster.initial_nodes {
                if node.id != node_id {
                    members.insert(node.id, openraft::impls::BasicNode::new(&node.raft));
                }
            }
            raft.initialize(members)
                .await
                .map_err(|e| format!("raft initialize: {e}"))?;
        }
    } else if cfg.cluster.join_addr.is_some() {
        tracing::info!("Joining cluster as non-voter, will be promoted after sync");
        // Openraft 0.10 的 join 流程：先以 non-voter 加入，后续通过 change_membership 晋升
        if let Err(e) = raft.add_learner(node_id, openraft::impls::BasicNode::new(raft_addr), true).await {
            tracing::warn!("add_learner returned: {e} (may be normal in some scenarios)");
        }
    }

    let raft = Arc::new(raft);

    // 6. 构建 CoordNode
    let mut node = CoordNode::new(Arc::clone(&mvcc_read));
    node.watch_dispatcher = Some(Arc::clone(&watch_dispatcher));
    node.raft = Some(Arc::clone(&raft));
    // 初始化 Lease 管理器（Leader 独占；Follower 上不激活到期检测）
    let timer_handle = TimerWheel::start();
    node.lease_manager = Some(Arc::new(LeaseManager::new(timer_handle)));
    let node = Arc::new(node);

    // 启动 Lease 过期轮询后台任务（每 200ms 清理过期 Lease 绑定的 KV key）
    node.start_lease_expiry_worker();

    // 6.5. 初始化 Auth 组件（默认禁用，通过 gRPC 启用）
    let auth_manager = Arc::new(AuthManager::new());
    let token_manager = Arc::new(TokenManager::with_defaults());
    let auth_svc = AuthServer::new(AuthService::new(
        Arc::clone(&auth_manager),
        Arc::clone(&token_manager),
    ));

    // 7. 构建客户端 gRPC 服务
    let kv_svc = KvServer::from_arc(Arc::clone(&node));
    let txn_svc = TxnServer::from_arc(Arc::clone(&node));
    let lease_svc = LeaseServer::from_arc(Arc::clone(&node));
    let watch_svc = WatchServer::from_arc(Arc::clone(&node));
    let maintenance_svc = MaintenanceServer::from_arc(Arc::clone(&node));

    // 8. 初始化 Metrics 和 Raft 就绪状态
    let metrics = Arc::new(Metrics::new());
    let raft_ready = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // 8a. 构建 BFF axum 路由器（统一 HTTP 入口：健康检查 + API 代理 + UI 静态资源）
    let grpc_port: u16 = grpc_addr
        .rsplit(':')
        .next()
        .and_then(|p| p.parse().ok())
        .unwrap_or(50051);
    let http_port = grpc_port + 10; // HTTP 端口 = gRPC 端口 + 10
    let http_addr = format!("0.0.0.0:{}", http_port);
    let core_http_addr = format!("http://127.0.0.1:{}", http_port);

    let bff_config = BffConfig {
        ui_enabled: cfg.network.ui_enabled,
        http_addr: http_addr.clone(),
        core_addr: core_http_addr.clone(),
    };

    let core_client = Arc::new(ReqwestCoreClient::new(core_http_addr));
    let internal_state = Arc::new(InternalState {
        auth_manager: Arc::clone(&auth_manager),
        token_manager: Arc::clone(&token_manager),
        coord_node: Arc::clone(&node),
    });
    let health_state = Arc::new(HealthState {
        metrics: Arc::clone(&metrics),
        raft_ready: Arc::clone(&raft_ready),
    });

    let bff_router = build_router(
        &bff_config,
        core_client,
        Some(internal_state),
        Some(health_state),
    );

    // 启动 axum HTTP 服务器（替代原 native TCP health server）
    let http_listener = tokio::net::TcpListener::bind(&http_addr).await?;
    tracing::info!(
        "HTTP server (health/BFF/UI) listening on http://{} (ui_enabled={})",
        http_addr,
        cfg.network.ui_enabled
    );
    let _http_handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(http_listener, bff_router).await {
            tracing::error!("HTTP server error: {e}");
        }
    });

    // 后台任务：周期性更新 Raft 指标和就绪状态
    let raft_for_metrics = Arc::clone(&raft);
    let metrics_for_raft = Arc::clone(&metrics);
    let ready_for_raft = Arc::clone(&raft_ready);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            ticker.tick().await;
            let m = raft_for_metrics.metrics().borrow_watched().clone();
            let leader = raft_for_metrics.current_leader().await;

            metrics_for_raft.set_raft_term(m.current_term);
            metrics_for_raft.set_raft_commit_index(
                m.last_log_index.unwrap_or(0),
            );
            metrics_for_raft.set_raft_applied_index(
                m.last_applied.as_ref().map(|id| id.index).unwrap_or(0),
            );
            metrics_for_raft.set_raft_leader_id(leader.unwrap_or(0));
            metrics_for_raft.set_seal_status(0); // 默认 Unsealed

            // 更新 Raft 就绪状态
            let ready = health::check_raft_ready(
                leader.unwrap_or(0) as i64,
                m.last_log_index.unwrap_or(0),
                m.last_applied.as_ref().map(|id| id.index).unwrap_or(0),
            );
            ready_for_raft.store(ready, std::sync::atomic::Ordering::Relaxed);
        }
    });

    // 8. 启动 Changelog Compaction 后台任务
    let compaction_config = CompactionConfig::default();
    let retention = compaction_config.changelog_retention_revisions;
    let compaction_interval = compaction_config.interval;
    let auto_compact = compaction_config.auto_compact;
    let _compaction_mgr = CompactionManager::start(Arc::clone(&mvcc_read), compaction_config);
    tracing::info!(
        "Compaction manager started: auto_compact={}, interval={:?}, retention={} revs",
        auto_compact,
        compaction_interval,
        retention
    );

    // 8.5. 启动自动快照调度器（ADP §19.2）
    let snapshot_dir = data_dir.join("snapshots");
    let snapshot_scheduler_config = coord_server::storage::snapshot_scheduler::SnapshotSchedulerConfig {
        interval: std::time::Duration::from_secs(3600),      // 1 hour
        retention: std::time::Duration::from_secs(7 * 86400), // 7 days
        snapshot_dir: snapshot_dir.clone(),
        auto_snapshot: true,
    };
    let snapshot_scheduler = Arc::new(
        coord_server::storage::snapshot_scheduler::SnapshotScheduler::new(
            Arc::clone(&mvcc_read),
            snapshot_scheduler_config,
        ),
    );
    let _snapshot_handle = snapshot_scheduler.start();
    tracing::info!(
        "Snapshot scheduler started: interval=1h, retention=7d, dir={}",
        snapshot_dir.display()
    );

    // 9. 启动 Raft RPC gRPC Server（内部节点间通信，raft_addr 端口，可选 TLS）
    let raft_socket_addr: std::net::SocketAddr = raft_addr.parse()?;
    let raft_rpc_svc = RaftRpcServer::new(raft_rpc_service);
    let raft_tls_for_server = raft_tls_config.clone();
    let raft_handle = tokio::spawn(async move {
        let mut builder = tonic::transport::Server::builder();

        let serve_result = if let Some(ref tls_cfg) = raft_tls_for_server {
            match tls::build_server_tls(tls_cfg) {
                Ok(server_tls) => {
                    tracing::info!("Raft RPC server TLS enabled on {}", raft_socket_addr);
                    match builder.tls_config(server_tls) {
                        Ok(mut tls_builder) => {
                            tls_builder
                                .add_service(raft_rpc_svc)
                                .serve(raft_socket_addr)
                                .await
                        }
                        Err(e) => {
                            tracing::error!("Raft RPC TLS config error: {}", e);
                            return;
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Raft RPC TLS build error: {}, falling back to plaintext", e);
                    builder
                        .add_service(raft_rpc_svc)
                        .serve(raft_socket_addr)
                        .await
                }
            }
        } else {
            builder
                .add_service(raft_rpc_svc)
                .serve(raft_socket_addr)
                .await
        };

        if let Err(e) = serve_result {
            tracing::error!("Raft RPC server error: {e}");
        }
    });

    // 10. 启动客户端 gRPC Server（grpc_addr 端口，可选 TLS）
    let grpc_socket_addr: std::net::SocketAddr = grpc_addr.parse()?;
    
    // 检查 TLS 配置
    let use_tls = cfg.security.tls_cert.is_some() && cfg.security.tls_key.is_some();
    if use_tls {
        let tls_cfg = TlsConfig::new(
            cfg.security.tls_cert.clone().unwrap(),
            cfg.security.tls_key.clone().unwrap(),
            cfg.security.tls_ca.clone(),
        );
        let server_tls = tls::build_server_tls(&tls_cfg)?;
        tracing::info!(
            "TLS enabled for gRPC server on {}, cert={}, mTLS={}",
            grpc_socket_addr,
            tls_cfg.cert_path.display(),
            tls_cfg.ca_path.is_some()
        );

        let grpc_future = tonic::transport::Server::builder()
            .tls_config(server_tls)
            .map_err(|e| format!("TLS server config: {e}"))?
            .add_service(kv_svc)
            .add_service(txn_svc)
            .add_service(lease_svc)
            .add_service(watch_svc)
            .add_service(maintenance_svc)
            .add_service(auth_svc)
            .serve_with_shutdown(grpc_socket_addr, shutdown_signal());

        grpc_future.await?;
    } else {
        tracing::info!(
            "Coord server v{} started: node_id={}, grpc_addr={}, raft_addr={} (no TLS)",
            env!("CARGO_PKG_VERSION"),
            node_id,
            grpc_socket_addr,
            raft_addr
        );

        let grpc_future = tonic::transport::Server::builder()
            .add_service(kv_svc)
            .add_service(txn_svc)
            .add_service(lease_svc)
            .add_service(watch_svc)
            .add_service(maintenance_svc)
            .add_service(auth_svc)
            .serve_with_shutdown(grpc_socket_addr, shutdown_signal());

        grpc_future.await?;
    }

    // 12. 清理：终止 Raft RPC server
    raft_handle.abort();
    tracing::info!("Coord server shutdown complete");

    Ok(())
}

/// 优雅关闭信号处理
///
/// 监听 SIGTERM（K8s 终止）和 SIGINT（Ctrl+C），
/// 收到信号后触发 tonic graceful shutdown。
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            tracing::info!("Received SIGINT (Ctrl+C), shutting down gracefully...");
        }
        _ = terminate => {
            tracing::info!("Received SIGTERM, shutting down gracefully...");
        }
    }
}

// ──── Dev 模式启动逻辑 ────

/// 开发模式：同时启动单节点 Server + Agent
///
/// 对标 Consul `consul agent -dev`，一键启动本地开发环境。
/// Server 以 bootstrap 模式启动单节点 Raft 集群，
/// Agent 以 Direct 模式连接 Server 并提供本地代理。
///
/// 优雅关闭：Ctrl+C 同时触发 Server 和 Agent 的 graceful shutdown。
async fn run_dev(
    grpc_port: u16,
    agent_port: u16,
    data_dir: &PathBuf,
    cluster_name: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let server_addr = format!("127.0.0.1:{}", grpc_port);
    let raft_port = grpc_port + 1;
    let raft_addr = format!("127.0.0.1:{}", raft_port);
    let agent_addr = format!("127.0.0.1:{}", agent_port);
    let http_port = agent_port + 1;

    // 1. 确定数据目录（开发模式使用项目本地目录，避免权限问题）
    let dev_data_dir = if data_dir.to_string_lossy() == "/var/lib/coord" {
        // 使用默认全局 --data-dir 值时，dev 模式改用本地目录
        let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        cwd.join("coord-dev-data")
    } else {
        data_dir.clone()
    };
    std::fs::create_dir_all(&dev_data_dir)?;

    // 2. 构建 Server 配置
    let mut server_cfg = config::Config::default();
    server_cfg.node.id = 1;
    server_cfg.network.grpc_addr = server_addr.clone();
    server_cfg.network.raft_addr = raft_addr.clone();
    server_cfg.network.ui_enabled = true; // dev 模式默认开启 UI 控制台
    server_cfg.storage.data_dir = dev_data_dir.clone();
    server_cfg.cluster.cluster_name = cluster_name.to_string();
    server_cfg.cluster.bootstrap = true;

    // BFF HTTP 端口（与 run_server 保持一致：grpc_port + 10）
    let bff_http_port = grpc_port + 10;

    tracing::info!(
        "Dev mode: starting server on {} (raft: {}, data: {}, http: {})",
        server_addr, raft_addr, dev_data_dir.display(), bff_http_port
    );

    // 3. 后台启动 Server（内部有自己的 shutdown_signal，Ctrl+C 时自动关闭）
    let server_addr_for_agent = server_addr.clone();
    let raft_addr_for_display = raft_addr.clone();
    let server_handle = tokio::spawn(async move {
        if let Err(e) = run_server(&server_cfg, &raft_addr, true).await {
            tracing::error!("Dev server exited with error: {e}");
        }
    });

    // 4. 等待 Server 端口就绪
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if tokio::time::Instant::now() > deadline {
            server_handle.abort();
            return Err(format!(
                "Server did not become ready on {} within 30s",
                server_addr
            )
            .into());
        }
        if tokio::net::TcpStream::connect(&server_addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    }

    tracing::info!("Dev server ready on {}", server_addr);

    // 4.5. 等待 Raft Leader 选举完成（避免 Agent 启动时 RegistryService Watch 订阅因 Leader 未就绪而失败）
    {
        use coord_proto::maintenance::maintenance_client::MaintenanceClient;
        let leader_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
        let server_url = format!("http://{server_addr}");
        loop {
            if tokio::time::Instant::now() > leader_deadline {
                server_handle.abort();
                return Err(format!(
                    "Raft leader not elected on {} within 15s after port ready",
                    server_addr
                )
                .into());
            }
            match tonic::transport::Endpoint::from_shared(server_url.clone()) {
                Ok(ep) => {
                    match ep.connect_timeout(std::time::Duration::from_secs(2)).connect().await {
                        Ok(channel) => {
                            let mut client = MaintenanceClient::new(channel);
                            let request = tonic::Request::new(
                                coord_proto::maintenance::StatusRequest {},
                            );
                            if let Ok(resp) = client.status(request).await {
                                let status = resp.into_inner();
                                if !status.raft_leader.is_empty() {
                                    tracing::info!(
                                        "Raft leader elected: node {} on {}",
                                        status.raft_leader,
                                        server_addr
                                    );
                                    break;
                                }
                            }
                        }
                        Err(_) => {}
                    }
                }
                Err(_) => {}
            }
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        }
    }

    // 5. 构建 Agent 配置并启动
    let agent_config = coord_agent::AgentConfig {
        agent_addr: agent_addr.clone(),
        http_addr: format!("127.0.0.1:{}", http_port),
        data_dir: dev_data_dir.join("agent").to_string_lossy().to_string(),
        static_peers: vec![server_addr_for_agent],
        ..Default::default()
    };

    tracing::info!("Dev mode: starting agent on {} (http: {})", agent_addr, http_port);

    // 启动 Agent HTTP health/metrics 端点（对标 run_agent 的行为）
    let agent_metrics = coord_agent::metrics::AgentMetrics::new();
    let agent_has_peers = !agent_config.static_peers.is_empty();
    let _agent_health_handle = coord_agent::health::start_health_server(
        &agent_config.http_addr,
        agent_metrics,
        agent_has_peers,
    );

    let agent_server = coord_agent::AgentServer::new(agent_config);
    let (agent_shutdown_tx, agent_shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let agent_handle = tokio::spawn(async move {
        if let Err(e) = agent_server
            .serve_with_shutdown(async {
                let _ = agent_shutdown_rx.await;
            })
            .await
        {
            tracing::error!("Dev agent exited with error: {e}");
        }
    });

    // 6. 等待 Agent 端口就绪
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        if tokio::time::Instant::now() > deadline {
            drop(agent_shutdown_tx);
            server_handle.abort();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(3), agent_handle).await;
            return Err(format!(
                "Agent did not become ready on {} within 15s",
                agent_addr
            )
            .into());
        }
        if tokio::net::TcpStream::connect(&agent_addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    // 7. 打印连接信息
    println!();
    println!("  ✓ Server 启动完成: {} (Raft: {})", server_addr, raft_addr_for_display);
    println!("  ✓ Agent 启动完成:  {} (HTTP: 127.0.0.1:{})", agent_addr, http_port);
    println!();
    println!("  连接方式:");
    println!("    UI 控制台 → http://127.0.0.1:{}", bff_http_port);
    println!("    Java 应用  → {}", agent_addr);
    println!("    Rust SDK  → {} (Agent 模式) 或 {} (Direct 模式)", agent_addr, server_addr);
    println!("    gRPC 工具 → {}", server_addr);
    println!();
    println!("  按 Ctrl+C 停止所有服务");
    println!();
    println!("  默认用户凭据:");
    println!("    username: root");
    println!("    password: root");
    println!("  登录: coord auth login root");
    println!();

    // 8. 等待关闭信号
    shutdown_signal().await;

    tracing::info!("Dev mode: shutting down...");

    // 9. 触发 Agent 优雅关闭
    drop(agent_shutdown_tx);

    // 10. 等待 Agent 和 Server 退出
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), agent_handle).await;
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), server_handle).await;

    tracing::info!("Dev mode: shutdown complete");
    Ok(())
}
