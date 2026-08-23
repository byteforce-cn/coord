// coord CLI 入口
//
// 组合 Server/Client 模式启动。支持以下子命令（ADP §6、§10.1、§15.3、§19.1）：
// - server:    启动 Server 节点（单节点或加入集群）
// - agent:     启动 Agent 守护进程（本地代理，Java 应用入口）
// - dev:       开发模式：同时启动 Server + Agent（对标 consul agent -dev）
// - security:  封存/解封/初始化密钥分片/轮换密钥
// - member:    动态成员管理（添加/移除/晋升/列表）
// - snapshot:  快照管理（保存/恢复）

pub mod commands;
mod config;

use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};

use coord_server::raft::WatchReceiver;

use coord_core::storage::StorageBackend;
use coord_proto::auth::auth_server::AuthServer;
use coord_proto::capability::capability_registry_server::CapabilityRegistryServer;
use coord_proto::kv::kv_server::KvServer;
use coord_proto::lease::lease_server::LeaseServer;
use coord_proto::maintenance::maintenance_server::MaintenanceServer;
use coord_proto::txn::txn_server::TxnServer;
use coord_proto::watch::watch_server::WatchServer;
use coord_server::auth::manager::hash_password_argon2id;
use coord_server::auth::revocation::RevocationStore;
use coord_server::auth::token_signing::TokenSigningKeyring;
use coord_server::auth::{
    AuthManager, AuthService, ServerAuthInterceptor, ServerAuthLayer, TokenManager,
};
use coord_server::auth::{CapabilityRegistry, CapabilityRegistryService};
use coord_server::bff::{
    build_router, internal::InternalState, BffConfig, HealthState, ReqwestCoreClient,
};
use coord_server::health;
use coord_server::lease::LeaseManager;
use coord_server::metrics::Metrics;
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcServer, RaftRpcService};
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::server::CoordNode;
use coord_server::storage::compaction::{CompactionConfig, CompactionManager};
use coord_server::storage::mvcc::MvccStorage;
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::timer::TimerWheel;
use coord_server::tls::{self, TlsConfig};
use coord_server::watch::WatchDispatcher;

/// P1-02：慢 follower 告警阈值 —— follower 已确认日志落后 leader 超过该条目数时 WARN
const SLOW_FOLLOWER_LAG_ENTRIES: u64 = 1000;

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

    /// 日志格式（P1-09：json | pretty，默认 pretty）
    #[arg(long, global = true, default_value = "pretty")]
    log_format: String,

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

        /// 鉴权开关（默认取配置 security.auth_enabled=true；
        /// false 仅限开发/测试环境，P0-C.1）
        #[arg(long)]
        auth_enabled: Option<bool>,
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
        /// 监听地址（默认 127.0.0.1，容器化部署需设为 0.0.0.0）
        #[arg(long, default_value = "127.0.0.1")]
        bind_addr: String,

        /// Server gRPC 端口（默认 50051）
        #[arg(long, default_value = "50051")]
        grpc_port: u16,

        /// Agent gRPC 端口（默认 19527）
        #[arg(long, default_value = "19527")]
        agent_port: u16,

        /// 集群名称（默认 "coord-dev"）
        #[arg(long, default_value = "coord-dev")]
        cluster_name: String,

        /// 启动前清空数据目录（确保干净状态）
        #[arg(long, default_value = "false")]
        fresh: bool,
    },

    /// 清空本地数据目录（环境重置）
    Reset {
        /// 目标 Server 地址（--keep-idgen 时用于导出 /_idgen/ 前缀）
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,

        /// 保留 ID 生成器状态：重置前导出 /_idgen/ 前缀，重置后可用 coord idgen restore 恢复
        #[arg(long, default_value = "false")]
        keep_idgen: bool,
    },

    /// ID 生成器运维
    #[command(subcommand)]
    Idgen(IdgenCmd),
}

// ──── IdGen 子命令 ────

#[derive(Subcommand)]
enum IdgenCmd {
    /// 从备份文件恢复 ID 生成器号段状态（coord reset --keep-idgen 导出的备份）
    Restore {
        /// 备份文件路径
        #[arg(long)]
        file: PathBuf,

        /// 目标 Server 地址
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
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
        /// 目标节点地址（预留，当前导出直接读本地数据目录）
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,

        /// 快照输出文件路径
        #[arg(long, default_value = "coord-snapshot.snap")]
        output: PathBuf,

        /// 本地数据目录（导出源）
        #[arg(long, default_value = "/var/lib/coord")]
        data_dir: PathBuf,
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

    /// 在线拉取快照（P1-08：Maintenance/Snapshot 流式导出，备份用）
    Pull {
        /// 源节点 gRPC 地址
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,

        /// 快照输出文件路径
        #[arg(long, default_value = "coord-snapshot-pull.snap")]
        output: PathBuf,
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
    // P0-F.3：panic hook —— 输出完整栈与关键状态（生产路径 panic 显式化）
    std::panic::set_hook(Box::new(|info| {
        tracing::error!("PANIC: {info}");
        let backtrace = std::backtrace::Backtrace::force_capture();
        tracing::error!("backtrace:\n{backtrace}");
    }));

    // P1-09：JSON 日志开关（`--log-format json`，采集环境用）
    let cli = Cli::parse();
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "coord=info".into()),
        )
        .with_target(false);
    if cli.log_format.eq_ignore_ascii_case("json") {
        subscriber.json().init();
    } else {
        subscriber.init();
    }

    // 加载配置文件（如果指定）
    let mut file_config = None;
    if let Some(ref config_path) = cli.config {
        match config::Config::from_file(config_path) {
            Ok(cfg) => {
                tracing::info!("Loaded config from {}", config_path.display());
                file_config = Some(cfg);
            }
            Err(e) => {
                tracing::error!("Failed to load config from {}: {e}", config_path.display());
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
            auth_enabled,
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
            // P0-C.1：鉴权唯一开关（CLI 覆盖仅限显式指定；默认 true）
            if let Some(auth_enabled) = auth_enabled {
                cfg.security.auth_enabled = auth_enabled;
            }

            let raft_addr = cfg.resolve_raft_addr();
            tracing::info!(
                "Starting coord server v{}: id={}, grpc={}, raft={}, cluster={}, bootstrap={}, auth_enabled={}",
                env!("CARGO_PKG_VERSION"),
                cfg.node.id,
                cfg.resolve_grpc_addr(),
                raft_addr,
                cfg.cluster.cluster_name,
                cfg.cluster.bootstrap || bootstrap,
                cfg.security.auth_enabled,
            );

            if let Some(ref join_addr) = cfg.cluster.join_addr {
                tracing::info!("Joining cluster via {}", join_addr);
            }

            // 启动服务端（带优雅关闭；server 模式非 dev；P2-02 传入配置文件路径供 SIGHUP 热更新）
            if let Err(e) = run_server(
                &cfg,
                &raft_addr,
                bootstrap || cfg.cluster.bootstrap,
                false,
                cli.config.clone(),
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
                if let Err(e) =
                    commands::cmd_member_add(&addr, id, &node_addr, raft_addr.as_deref()).await
                {
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
            SnapshotCmd::Save {
                addr,
                output,
                data_dir,
            } => {
                tracing::info!(
                    "Saving snapshot from {} to {}",
                    data_dir.display(),
                    output.display()
                );
                if let Err(e) = snapshot_save(&addr, &output, &data_dir).await {
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
            SnapshotCmd::Pull { addr, output } => {
                tracing::info!("Pulling snapshot from {} to {}", addr, output.display());
                if let Err(e) = commands::snapshot_pull(&addr, &output).await {
                    tracing::error!("Snapshot pull failed: {e}");
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
                AuthUserCmd::Add {
                    name,
                    password,
                    addr,
                } => {
                    let pass = match password {
                        Some(p) => p,
                        None => match commands::prompt_password_with_confirm() {
                            Ok(p) => p,
                            Err(e) => {
                                eprintln!("Error: {e}");
                                std::process::exit(1);
                            }
                        },
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
                AuthUserCmd::Passwd {
                    name,
                    password,
                    addr,
                } => {
                    let pass = match password {
                        Some(p) => p,
                        None => match commands::prompt_password_with_confirm() {
                            Ok(p) => p,
                            Err(e) => {
                                eprintln!("Error: {e}");
                                std::process::exit(1);
                            }
                        },
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
                AuthAppRoleCmd::Create {
                    name,
                    role_id,
                    secret_id,
                    bind_role,
                    addr,
                } => {
                    if let Err(e) = commands::cmd_auth_approle_create(
                        &addr,
                        &name,
                        role_id.as_deref(),
                        secret_id.as_deref(),
                        bind_role.as_deref(),
                    )
                    .await
                    {
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
                AuthRoleCmd::Grant {
                    name,
                    perm,
                    key,
                    range_end,
                    addr,
                } => {
                    if let Err(e) = commands::cmd_auth_role_grant(
                        &addr,
                        &name,
                        &perm,
                        &key,
                        range_end.as_deref(),
                    )
                    .await
                    {
                        tracing::error!("RoleGrant failed: {e}");
                        eprintln!("Error: {e}");
                        std::process::exit(1);
                    }
                }
                AuthRoleCmd::Revoke {
                    name,
                    key,
                    range_end,
                    addr,
                } => {
                    if let Err(e) =
                        commands::cmd_auth_role_revoke(&addr, &name, &key, range_end.as_deref())
                            .await
                    {
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
            AuthCmd::Login {
                name,
                token_only,
                addr,
            } => {
                let pass = match commands::prompt_password(&format!("Password for {name}: ")) {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!("Error: {e}");
                        std::process::exit(1);
                    }
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
            CapabilityCmd::Get {
                capability_id,
                addr,
            } => {
                tracing::info!("Getting capability {} via {}", capability_id, addr);
                if let Err(e) = commands::cmd_capability_get(&addr, &capability_id).await {
                    tracing::error!("CapabilityGet failed: {e}");
                    eprintln!("Error: {e}");
                    std::process::exit(1);
                }
            }
        },

        Commands::Reset { addr, keep_idgen } => {
            tracing::info!(
                "Resetting local data dir {} (keep_idgen={}) via {}",
                cli.data_dir.display(),
                keep_idgen,
                addr
            );
            if let Err(e) = commands::cmd_reset(&cli.data_dir, &addr, keep_idgen).await {
                tracing::error!("Reset failed: {e}");
                eprintln!("Error: {e}");
                std::process::exit(1);
            }
        }

        Commands::Idgen(cmd) => match cmd {
            IdgenCmd::Restore { file, addr } => {
                tracing::info!("Restoring idgen state from {} via {}", file.display(), addr);
                if !file.exists() {
                    tracing::error!("Backup file not found: {}", file.display());
                    eprintln!("Error: backup file not found: {}", file.display());
                    std::process::exit(1);
                }
                if let Err(e) = commands::cmd_idgen_restore(&file, &addr).await {
                    tracing::error!("Idgen restore failed: {e}");
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
        }

        Commands::Dev {
            bind_addr,
            grpc_port,
            agent_port,
            cluster_name,
            fresh,
        } => {
            tracing::info!(
                "Starting coord dev mode v{}: bind={}, server={}:{}, agent={}:{}, cluster={}",
                env!("CARGO_PKG_VERSION"),
                bind_addr,
                bind_addr,
                grpc_port,
                bind_addr,
                agent_port,
                cluster_name
            );

            if let Err(e) = run_dev(
                &bind_addr,
                grpc_port,
                agent_port,
                &cli.data_dir,
                &cluster_name,
                fresh,
            )
            .await
            {
                tracing::error!("Dev mode exited with error: {e}");
                std::process::exit(1);
            }
        }
    }
}

// ──── Snapshot CLI 实现 ────

/// 从本地数据目录导出快照（P0-A.3 过渡工具；`addr` 预留未来远程导出）
async fn snapshot_save(
    _addr: &str,
    output: &std::path::Path,
    data_dir: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    tracing::info!("Exporting snapshot to {}", output.display());

    if !data_dir.exists() {
        return Err(format!(
            "Data directory {} not found. Is the server running?",
            data_dir.display()
        )
        .into());
    }

    let storage_config = coord_core::types::StorageConfig::default();
    let backend = RedbBackend::open(data_dir, &storage_config)?;
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
    println!("Snapshot saved: {kv_count} KV pairs → {}", output.display());
    Ok(())
}

/// 从快照文件恢复到本地数据目录
async fn snapshot_restore(
    snapshot_path: &PathBuf,
    data_dir: &std::path::Path,
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
    println!(
        "Snapshot restored: {} KV pairs → {}",
        kv_count,
        data_dir.display()
    );
    Ok(())
}

// ──── Server 启动逻辑 ────

async fn run_server(
    cfg: &config::Config,
    raft_addr: &str,
    bootstrap: bool,
    dev_mode: bool,
    config_path: Option<std::path::PathBuf>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // P2-02：启动配置校验（一次报清全部问题；TLS 缺配不再静默降级明文）
    if let Err(errs) = cfg.validate() {
        return Err(format!("invalid configuration:\n  - {}", errs.join("\n  - ")).into());
    }
    let node_id = cfg.node.id;
    let grpc_addr = cfg.resolve_grpc_addr();
    let data_dir = cfg.resolve_data_dir();

    // 1. 创建数据目录
    std::fs::create_dir_all(&data_dir)?;

    // 1.5. 预绑定 gRPC 端口（在 Raft 初始化之前）：
    //      确保 dev 模式的端口就绪检查不会因为 Raft 日志重放耗时过长而超时。
    let grpc_socket_addr: std::net::SocketAddr = grpc_addr.parse()?;
    let mut grpc_listener = Some(tokio::net::TcpListener::bind(grpc_socket_addr).await?);

    // 2. 初始化 Redb 存储后端（共享实例）
    let storage_config = coord_core::types::StorageConfig::default();
    let backend = RedbBackend::open(&data_dir, &storage_config)?;

    // 3. 单一 MvccStorage 实例（D-A1）：CoordNode 读路径、StateMachine 写路径、
    //    Compaction、Snapshot 全链路共享；revision ≡ log index（D-A2）。
    let mvcc = Arc::new(MvccStorage::new(backend)?);

    // 3.5 M0-3 启动一致性校验：META_LAST_APPLIED 与 changelog 尾部一致
    {
        let (applied, changelog_tail) = mvcc.verify_consistency()?;
        match (applied, changelog_tail) {
            (0, None) => tracing::info!("Fresh store: no applied state, no changelog"),
            (a, Some(t)) if a >= t => {
                tracing::info!("Store consistency OK: applied={a}, changelog_tail={t}")
            }
            (a, Some(t)) => {
                tracing::warn!(
                    "Store consistency mismatch: applied={a} < changelog_tail={t}; \
                     will replay from applied+1 (idempotence guard active)"
                );
            }
            (a, None) => {
                tracing::warn!(
                    "Store consistency mismatch: applied={a} but changelog empty; \
                     continuing with replay"
                );
            }
        }
    }

    // 3.6 M0-4/M0-5 快照目录与 purge 守卫（与 LogStore/StateMachine 共享）
    let snapshot_dir = data_dir.join("snapshots");
    std::fs::create_dir_all(&snapshot_dir)?;
    let snapshot_tracker = Arc::new(coord_server::storage::snapshot::SnapshotTracker::default());

    // 4. 初始化 Watch 分发器
    let watch_dispatcher = Arc::new(WatchDispatcher::start());

    // 5. 构建 Raft 栈
    // 5a. Raft LogStore（Redb 持久化，独立实例 raft-log/log.db）
    let log_store = LogStore::new(&data_dir)
        .await
        .map_err(|e| format!("create raft log store: {e}"))?
        .with_snapshot_tracker(Arc::clone(&snapshot_tracker));

    // 5a.5 M0-5 启动检查：日志已被 purge 但无覆盖快照 → 拒绝启动（不可恢复态）
    if let Some(purged) = log_store
        .last_purged()
        .map_err(|e| format!("read last_purged: {e}"))?
    {
        if !snapshot_tracker.durable_covers(purged.index) {
            return Err(format!(
                "unrecoverable state: raft logs purged up to index {} but no durable \
                 snapshot covers it (snapshot dir: {}); restore a snapshot before starting",
                purged.index,
                snapshot_dir.display()
            )
            .into());
        }
        tracing::info!(
            "Purge guard OK: logs purged up to {}, durable snapshot covers it",
            purged.index
        );
    }

    // 5b. Raft StateMachine（与 CoordNode 共享同一存储实例与快照守卫）
    //     与 CoordNode 共享同一个 WatchDispatcher，确保 apply 路径的
    //     事件分发与 gRPC Watch 订阅者使用同一订阅表。
    let mut sm_store = StateMachineStore::new(
        Arc::clone(&mvcc),
        snapshot_dir.clone(),
        Arc::clone(&snapshot_tracker),
    );
    sm_store.set_watch_dispatcher(Arc::clone(&watch_dispatcher));

    // 6.5. 初始化 Auth 组件（P0-C.1：`security.auth_enabled` 唯一开关，默认 true）
    let auth_enabled = cfg.security.auth_enabled;
    if !auth_enabled && !dev_mode {
        // P0-G.1：无鉴权 + 非 loopback 绑定 → 拒绝启动（防止裸奔暴露）
        let non_loopback = |addr: &str| {
            !addr.starts_with("127.")
                && !addr.starts_with("localhost")
                && !addr.starts_with("[::1]")
        };
        if non_loopback(&grpc_addr) || non_loopback(raft_addr) {
            return Err(format!(
                "refusing to start with auth disabled on non-loopback bind (grpc={grpc_addr}, \
                 raft={raft_addr}); enable security.auth_enabled or bind loopback"
            )
            .into());
        }
        tracing::warn!(
            "Auth DISABLED in server mode — insecure; intended for dev/test environments only"
        );
    }

    let auth_manager: Arc<AuthManager> = if dev_mode {
        // dev 模式：root/root 默认凭据（仅限 dev，规格 C.4.8）
        Arc::new(AuthManager::new())
    } else {
        // server 模式：不创建 root/root；先装载持久化状态（用户/角色）
        let manager = Arc::new(AuthManager::new_empty());
        let auth_entries = mvcc
            .list_raw_prefix(b"/_sys/auth/")
            .map_err(|e| format!("load auth state: {e}"))?;
        manager.load_from_entries(auth_entries);
        manager
    };

    // root 引导（P0-C.2/C.6）：server 模式且视图无 root 时——
    //   bootstrap 节点：创建内存视图 + 稍后经 raft 持久化（5i.5）；
    //   join 节点：等待 leader 复制（apply 钩子同步视图）。
    let mut root_to_persist: Option<String> = None;
    if !dev_mode && !auth_manager.user_list().iter().any(|u| u == "root") {
        if bootstrap {
            let root_password = match &cfg.security.root_password {
                Some(pw) if !pw.is_empty() => Some(pw.clone()),
                _ => match std::env::var("COORD_ROOT_PASSWORD") {
                    Ok(pw) if !pw.is_empty() => Some(pw),
                    _ => None,
                },
            };
            let pw = root_password.unwrap_or_else(|| {
                let pw = generate_random_password(24);
                tracing::warn!(
                    "Generated random root password (shown ONCE; store it securely): {}",
                    pw
                );
                pw
            });
            auth_manager
                .user_add("root", &pw)
                .map_err(|e| format!("create root user: {e}"))?;
            auth_manager
                .user_grant_role("root", "root")
                .map_err(|e| format!("grant root role: {e}"))?;
            root_to_persist = Some(
                String::from_utf8_lossy(
                    &hash_password_argon2id(&pw).map_err(|e| format!("hash root password: {e}"))?,
                )
                .to_string(),
            );
        } else {
            tracing::info!(
                "Join node: no root user in view yet — will sync from leader via raft apply"
            );
        }
    }
    if auth_enabled {
        auth_manager.enable();
    }

    let revocation_store = Arc::new(RevocationStore::new(10_000));

    // 启动装载（P0-C.5）：吊销登记回填 RevocationStore
    {
        use coord_server::auth::manager::{AuthRevocationRecord, AUTH_REVOKED_PREFIX};
        let revoked_entries = mvcc
            .list_raw_prefix(AUTH_REVOKED_PREFIX)
            .map_err(|e| format!("load revocation state: {e}"))?;
        for (_key, value) in revoked_entries {
            if let Some(rec) = AuthRevocationRecord::from_bytes(&value) {
                revocation_store.revoke(&rec.jti);
            }
        }
        tracing::info!(
            "Auth state loaded: {} users, {} roles, {} revoked tokens",
            auth_manager.user_list().len(),
            auth_manager.role_list().len(),
            revocation_store.revoked_count(),
        );
    }

    // P0-C.2/C.5：apply 后同步内存视图与吊销登记
    let token_manager = Arc::new(TokenManager::with_defaults());
    // P2-08：审计日志（<data_dir>/audit/ 文件追加 + 最近 1024 条环形查询）
    let audit_logger = Arc::new(
        coord_server::audit::AuditLogger::file_logger(&data_dir)
            .map_err(|e| format!("init audit logger: {e}"))?,
    );
    // P2-07：启动装载持久化会话（重启不失效）
    {
        let session_entries = mvcc
            .list_raw_prefix(b"/_sys/auth/sessions/")
            .map_err(|e| format!("load auth sessions: {e}"))?;
        token_manager.load_sessions(session_entries);
    }

    sm_store.set_auth_manager(Arc::clone(&auth_manager));
    sm_store.set_revocation_store(Arc::clone(&revocation_store));
    // P2-07：会话表视图（apply IssueSession/ConsumeSession 后各节点同步）
    sm_store.set_session_manager(Arc::clone(&token_manager));

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
    let raft_tls_config = if let (Some(cert), Some(key)) =
        (cfg.security.tls_cert.clone(), cfg.security.tls_key.clone())
    {
        let tls_cfg = TlsConfig::new(cert, key, cfg.security.tls_ca.clone());
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

    // 5d. Raft 配置（P1-06：openraft 类型隔离，经 `coord_server::raft` 门面）
    let raft_config = Arc::new(coord_server::raft::RaftConfig::default());

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

    // 5g. 创建 Raft 实例（P1-06 门面：new_raft）
    let raft =
        coord_server::raft::new_raft(node_id, raft_config, network_factory, log_store, sm_store)
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
            members.insert(node_id, coord_server::raft::new_basic_node(raft_addr));
            // 注册配置中的初始节点
            for node in &cfg.cluster.initial_nodes {
                if node.id != node_id {
                    members.insert(node.id, coord_server::raft::new_basic_node(&node.raft));
                }
            }
            raft.initialize(members)
                .await
                .map_err(|e| format!("raft initialize: {e}"))?;
        }
    } else if let Some(join_addr) = cfg.cluster.join_addr.clone() {
        // P0-D.1：JoinRequest 流程 —— 新节点向 join_addr 发送 JoinRequest，
        // 非 leader 返回 forward_to 重试到 leader（不再自调 add_learner 吞错）。
        tracing::info!(
            "Joining cluster via {} (JoinRequest, leader redirect)",
            join_addr
        );
        let mut target = join_addr.clone();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
        loop {
            match send_join_request(&target, node_id, raft_addr, &grpc_addr).await {
                Ok(resp) if resp.success => {
                    tracing::info!("Joined cluster via {}: {}", target, resp.message);
                    break;
                }
                Ok(resp) if !resp.forward_to.is_empty() => {
                    tracing::info!(
                        "Join redirected from {} to leader at {}",
                        target,
                        resp.forward_to
                    );
                    target = resp.forward_to;
                }
                Ok(resp) => {
                    return Err(format!("join rejected by {}: {}", target, resp.message).into());
                }
                Err(e) => {
                    if tokio::time::Instant::now() > deadline {
                        return Err(format!("join failed after retries (last error: {e})").into());
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
            }
        }
    }

    // 5i.5 P0-C.2：root 引导用户经 raft 持久化（bootstrap 节点且 store 无 root）。
    //        单节点 bootstrap 立即可写；多节点等待领导权，限时重试。
    if let Some(root_hash) = root_to_persist {
        let root_op = coord_server::raft::type_config::Command::Auth(
            coord_server::raft::type_config::AuthOp::UserAdd {
                name: "root".to_string(),
                hash: root_hash,
                roles: vec!["root".to_string()],
            },
        );
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            match raft.client_write(root_op.clone()).await {
                Ok(_) => {
                    tracing::info!("root user persisted via raft");
                    break;
                }
                Err(e) => {
                    if tokio::time::Instant::now() > deadline {
                        return Err(format!(
                            "persist root user via raft failed (cluster may need a leader): {e}"
                        )
                        .into());
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
            }
        }
    }

    let raft = Arc::new(raft);

    // 6. 构建 CoordNode
    let mut node = CoordNode::new(Arc::clone(&mvcc));
    node.node_id = cfg.node.id;
    node.watch_dispatcher = Some(Arc::clone(&watch_dispatcher));
    node.raft = Some(Arc::clone(&raft));
    // P0-D.1：注册已知节点的 gRPC 地址（leader 重定向用，best-effort）
    node.register_grpc_addr(node_id, &grpc_addr);
    for n in &cfg.cluster.initial_nodes {
        node.register_grpc_addr(n.id, &n.grpc);
    }
    // 初始化 Lease 管理器（Leader 独占；Follower 上不激活到期检测）
    let timer_handle = TimerWheel::start();
    node.lease_manager = Some(Arc::new(LeaseManager::new(timer_handle)));
    // P1-02：每 watcher 事件队列长度（配置可调，最小 16）
    node.set_watch_buffer(cfg.network.watch_buffer);
    let node = Arc::new(node);

    // 启动 Lease 过期轮询后台任务（每 200ms 清理过期 Lease 绑定的 KV key；仅 leader 执行）
    node.start_lease_expiry_worker();
    // 启动 Lease failover reconciler（P0-B B.4.4：成为 leader 时从状态机重建）
    node.start_lease_leader_reconciler();

    // 6.6 Auth 根密钥：HKDF 派生 CCT 签名密钥（规格 C.4.2）。
    //     配置/环境优先，否则 <data_dir>/auth-root-key.bin 首启生成（0600）并复用。
    let root_key_material =
        load_or_create_root_key(&data_dir, cfg.security.auth_root_key.as_deref())?;
    if cfg.cluster.initial_nodes.len() > 1 || cfg.cluster.join_addr.is_some() {
        tracing::warn!(
            "Multi-node cluster: all nodes MUST share the same auth root key \
             ({} or security.auth_root_key)",
            data_dir.join("auth-root-key.bin").display()
        );
    }
    let signing_keyring = Arc::new(
        TokenSigningKeyring::new(root_key_material)
            .map_err(|e| format!("init token signing keyring: {e}"))?,
    );

    let token_manager = Arc::new(TokenManager::with_defaults());
    // CCT 生产签发接线（P0-C.3）+ AuthOp 提案器（P0-C.2）+ 吊销登记（P0-C.5）
    let auth_proposer: Arc<dyn coord_server::auth::service::AuthOpProposer> = node.clone();
    let auth_service: Arc<AuthService> = Arc::new(
        (if auth_enabled {
            AuthService::with_cct_signing(
                Arc::clone(&auth_manager),
                Arc::clone(&token_manager),
                Arc::clone(&signing_keyring),
            )
        } else {
            AuthService::new(Arc::clone(&auth_manager), Arc::clone(&token_manager))
        })
        .with_proposer(auth_proposer)
        .with_revocation_store(Arc::clone(&revocation_store))
        .with_audit_logger(Arc::clone(&audit_logger)),
    );
    let auth_svc = AuthServer::new(auth_service.as_ref().clone());

    // 6a. Capability 注册中心（内置能力引导 + gRPC 服务）
    let capability_registry = Arc::new(CapabilityRegistry::new());
    capability_registry.bootstrap_builtin();
    tracing::info!(
        "Capability registry bootstrapped: {} built-in capabilities",
        capability_registry.list().len()
    );
    let capability_svc = CapabilityRegistryServer::new(CapabilityRegistryService {
        registry: capability_registry,
    });

    // 7. 构建客户端 gRPC 服务（P1-02：消息解码上限显式 4MiB，对齐 13 号文档 §二.2）
    const MAX_DECODING_MSG: usize = 4 * 1024 * 1024;
    let kv_svc = KvServer::from_arc(Arc::clone(&node)).max_decoding_message_size(MAX_DECODING_MSG);
    let txn_svc =
        TxnServer::from_arc(Arc::clone(&node)).max_decoding_message_size(MAX_DECODING_MSG);
    let lease_svc =
        LeaseServer::from_arc(Arc::clone(&node)).max_decoding_message_size(MAX_DECODING_MSG);
    let watch_svc =
        WatchServer::from_arc(Arc::clone(&node)).max_decoding_message_size(MAX_DECODING_MSG);
    let maintenance_svc =
        MaintenanceServer::from_arc(Arc::clone(&node)).max_decoding_message_size(MAX_DECODING_MSG);

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
        auth_service: Some(Arc::clone(&auth_service)),
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

    // 后台任务：周期性更新 Raft 指标和就绪状态（P1-02 增加慢 follower 告警；
    // P1-09 健康检查真实语义随就绪状态切换）
    // 10a'（提前）：gRPC Health Check 服务（标准 grpc.health.v1.Health）——
    // 初始 NOT_SERVING，由本循环随就绪状态切换（P1-09）。
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_not_serving::<KvServer<Arc<CoordNode>>>()
        .await;
    health_reporter
        .set_not_serving::<TxnServer<Arc<CoordNode>>>()
        .await;
    health_reporter
        .set_not_serving::<LeaseServer<Arc<CoordNode>>>()
        .await;
    health_reporter
        .set_not_serving::<WatchServer<Arc<CoordNode>>>()
        .await;
    health_reporter
        .set_not_serving::<MaintenanceServer<Arc<CoordNode>>>()
        .await;
    health_reporter
        .set_not_serving::<AuthServer<AuthService>>()
        .await;
    health_reporter
        .set_not_serving::<CapabilityRegistryServer<CapabilityRegistryService>>()
        .await;

    let raft_for_metrics = Arc::clone(&raft);
    let metrics_for_raft = Arc::clone(&metrics);
    let ready_for_raft = Arc::clone(&raft_ready);
    let health_reporter_for_task = health_reporter.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            ticker.tick().await;
            let m = raft_for_metrics.metrics().borrow_watched().clone();
            let leader = raft_for_metrics.current_leader().await;

            metrics_for_raft.set_raft_term(m.current_term);
            metrics_for_raft.set_raft_commit_index(m.last_log_index.unwrap_or(0));
            metrics_for_raft
                .set_raft_applied_index(m.last_applied.as_ref().map(|id| id.index).unwrap_or(0));
            metrics_for_raft.set_raft_leader_id(leader.unwrap_or(0));
            metrics_for_raft.set_seal_status(0); // 默认 Unsealed

            // P1-02：慢 follower 告警 —— leader 视角，follower matched 滞后
            // 超过阈值（1000 条目或 >30s 无确认）时 WARN
            if leader == Some(node_id) {
                if let Some(ref replication) = m.replication {
                    let leader_idx = m.last_log_index.unwrap_or(0);
                    for (target, matched) in replication {
                        let lag = leader_idx
                            .saturating_sub(matched.as_ref().map(|l| l.index).unwrap_or(0));
                        if lag >= SLOW_FOLLOWER_LAG_ENTRIES {
                            tracing::warn!(
                                "slow follower: node {target} replication lag = {lag} entries"
                            );
                        }
                    }
                }
            }

            // 更新 Raft 就绪状态
            let ready = health::check_raft_ready(
                leader.unwrap_or(0) as i64,
                m.last_log_index.unwrap_or(0),
                m.last_applied.as_ref().map(|id| id.index).unwrap_or(0),
            );
            ready_for_raft.store(ready, std::sync::atomic::Ordering::Relaxed);

            // P1-09：健康检查真实语义 —— raft 就绪前 NOT_SERVING，随状态更新
            if ready {
                health_reporter_for_task
                    .set_serving::<KvServer<Arc<CoordNode>>>()
                    .await;
                health_reporter_for_task
                    .set_serving::<TxnServer<Arc<CoordNode>>>()
                    .await;
                health_reporter_for_task
                    .set_serving::<LeaseServer<Arc<CoordNode>>>()
                    .await;
                health_reporter_for_task
                    .set_serving::<WatchServer<Arc<CoordNode>>>()
                    .await;
                health_reporter_for_task
                    .set_serving::<MaintenanceServer<Arc<CoordNode>>>()
                    .await;
                health_reporter_for_task
                    .set_serving::<AuthServer<AuthService>>()
                    .await;
                health_reporter_for_task
                    .set_serving::<CapabilityRegistryServer<CapabilityRegistryService>>()
                    .await;
            } else {
                health_reporter_for_task
                    .set_not_serving::<KvServer<Arc<CoordNode>>>()
                    .await;
                health_reporter_for_task
                    .set_not_serving::<TxnServer<Arc<CoordNode>>>()
                    .await;
                health_reporter_for_task
                    .set_not_serving::<LeaseServer<Arc<CoordNode>>>()
                    .await;
                health_reporter_for_task
                    .set_not_serving::<WatchServer<Arc<CoordNode>>>()
                    .await;
                health_reporter_for_task
                    .set_not_serving::<MaintenanceServer<Arc<CoordNode>>>()
                    .await;
                health_reporter_for_task
                    .set_not_serving::<AuthServer<AuthService>>()
                    .await;
                health_reporter_for_task
                    .set_not_serving::<CapabilityRegistryServer<CapabilityRegistryService>>()
                    .await;
            }
        }
    });

    // P2-02：SIGHUP 热更新安全子集通道（磁盘水位阈值 + watch 缓冲）
    let (reload_tx, reload_rx) = tokio::sync::watch::channel(cfg.reloadable());

    // P1-02：磁盘水位监控（30s 周期）—— <warn 比例 WARN 告警，<readonly 比例置只读闸
    // （写请求 RESOURCE_EXHAUSTED，读仍可用）；阈值经 reload_rx 支持 SIGHUP 热更新
    {
        let node_for_disk = Arc::clone(&node);
        let metrics_for_disk = Arc::clone(&metrics);
        let data_dir_for_disk = data_dir.clone();
        let mut reload_rx_for_disk = reload_rx;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(30));
            let mut prev_watermark = coord_server::storage::disk_watermark::DiskWatermark::Ok;
            loop {
                ticker.tick().await;
                let rc = *reload_rx_for_disk.borrow_and_update();
                use coord_server::storage::disk_watermark as dw;
                match dw::check_disk_space(&data_dir_for_disk) {
                    Some(space) => {
                        metrics_for_disk.set_disk_available_bytes(space.available_bytes);
                        metrics_for_disk.set_disk_total_bytes(space.total_bytes);
                        let watermark = dw::classify_with(
                            space.available_ratio(),
                            rc.disk_warn_ratio,
                            rc.disk_readonly_ratio,
                        );
                        if watermark != prev_watermark {
                            match watermark {
                                dw::DiskWatermark::Ok => {
                                    tracing::info!("disk watermark recovered to Ok");
                                    node_for_disk.set_disk_read_only(false);
                                }
                                dw::DiskWatermark::Warn => {
                                    tracing::warn!(
                                        "disk watermark WARN: {:.1}% free (below {:.1}%)",
                                        space.available_ratio() * 100.0,
                                        rc.disk_warn_ratio * 100.0
                                    );
                                    node_for_disk.set_disk_read_only(false);
                                }
                                dw::DiskWatermark::ReadOnly => {
                                    tracing::error!(
                                        "disk watermark READ-ONLY: {:.1}% free (below {:.1}%); \
                                         write requests now return RESOURCE_EXHAUSTED",
                                        space.available_ratio() * 100.0,
                                        rc.disk_readonly_ratio * 100.0
                                    );
                                    node_for_disk.set_disk_read_only(true);
                                }
                            }
                            prev_watermark = watermark;
                        }
                    }
                    None => {
                        tracing::debug!("disk watermark check unavailable on this platform");
                    }
                }
            }
        });
    }

    // P2-02：SIGHUP 配置热更新（安全子集）。
    // 仅磁盘水位阈值（下一监控周期生效）与 watch 缓冲（新订阅生效）；
    // 监听地址/TLS/集群拓扑等结构性配置不支持热更新，需重启。
    #[cfg(unix)]
    if let Some(config_path) = config_path {
        let node_for_reload = Arc::clone(&node);
        let reload_tx_for_hup = reload_tx;
        tokio::spawn(async move {
            let mut sig =
                match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()) {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!("SIGHUP handler unavailable: {e}");
                        return;
                    }
                };
            loop {
                if sig.recv().await.is_none() {
                    break;
                }
                match config::Config::from_file(&config_path) {
                    Ok(new_cfg) => match new_cfg.validate() {
                        Ok(()) => {
                            let rc = new_cfg.reloadable();
                            let _ = reload_tx_for_hup.send_replace(rc);
                            node_for_reload.set_watch_buffer(rc.watch_buffer);
                            tracing::info!(
                                "SIGHUP: config reloaded from {} (watch_buffer={}, \
                                 disk_warn_ratio={:.2}, disk_readonly_ratio={:.2})",
                                config_path.display(),
                                rc.watch_buffer,
                                rc.disk_warn_ratio,
                                rc.disk_readonly_ratio
                            );
                        }
                        Err(errs) => {
                            tracing::warn!(
                                "SIGHUP: reloaded config invalid, keeping current config: {}",
                                errs.join("; ")
                            );
                        }
                    },
                    Err(e) => {
                        tracing::warn!(
                            "SIGHUP: failed to read {}: {e}, keeping current config",
                            config_path.display()
                        );
                    }
                }
            }
        });
    }
    #[cfg(not(unix))]
    {
        let _ = config_path;
        tracing::info!("SIGHUP config reload is not supported on this platform");
    }

    // 8. 启动 Changelog Compaction 后台任务（P1-01：leader 经 raft 提案
    //    compact revision（节点一致）+ 定时 redb 文件级 compact 空间回收）
    let compaction_config = CompactionConfig::default();
    let retention = compaction_config.changelog_retention_revisions;
    let compaction_interval = compaction_config.interval;
    let auto_compact = compaction_config.auto_compact;
    let compaction_proposer: Arc<dyn coord_server::storage::compaction::CompactProposer> =
        node.clone();
    let _compaction_mgr = CompactionManager::start(
        Arc::clone(&mvcc),
        compaction_config,
        Some(compaction_proposer),
    );
    tracing::info!(
        "Compaction manager started: auto_compact={}, interval={:?}, retention={} revs",
        auto_compact,
        compaction_interval,
        retention
    );

    // 8.5. 启动自动快照调度器（ADP §19.2）
    let snapshot_scheduler_config =
        coord_server::storage::snapshot_scheduler::SnapshotSchedulerConfig {
            interval: std::time::Duration::from_secs(3600), // 1 hour
            retention: std::time::Duration::from_secs(7 * 86400), // 7 days
            snapshot_dir: data_dir.join("snapshots"),
            auto_snapshot: true,
        };
    let snapshot_scheduler = Arc::new(
        coord_server::storage::snapshot_scheduler::SnapshotScheduler::new(
            Arc::clone(&mvcc),
            snapshot_scheduler_config,
        ),
    );
    let _snapshot_handle = snapshot_scheduler.start();
    tracing::info!(
        "Snapshot scheduler started: interval=1h, retention=7d, dir={}",
        snapshot_dir.display()
    );

    // 9. 启动 Raft RPC gRPC Server（内部节点间通信，raft_addr 端口，可选 TLS）
    //    P0-C.7（F4）：raft 端口 mTLS fail-closed —— TLS 配置存在但构建失败/
    //    缺 CA 时拒绝启动（删除明文降级分支）。
    let raft_socket_addr: std::net::SocketAddr = raft_addr.parse()?;
    let raft_rpc_svc = RaftRpcServer::new(raft_rpc_service);
    let raft_tls_for_server = raft_tls_config.clone();
    if let Some(ref tls_cfg) = raft_tls_for_server {
        if tls_cfg.ca_path.is_none() {
            return Err(
                "Raft inter-node TLS requires a client CA (security.tls_ca) for mTLS; \
                 refusing to start without raft-port mutual authentication (fail-closed)"
                    .to_string()
                    .into(),
            );
        }
    }
    let raft_server_tls = match raft_tls_for_server {
        Some(tls_cfg) => match tls::build_server_tls(&tls_cfg) {
            Ok(server_tls) => Some(server_tls),
            Err(e) => {
                return Err(format!(
                    "Raft RPC TLS build failed (fail-closed, no plaintext fallback): {e}"
                )
                .into())
            }
        },
        None => None,
    };
    let raft_handle = tokio::spawn(async move {
        // P1-02：raft RPC 单连接并发流上限
        let mut builder = tonic::transport::Server::builder().max_concurrent_streams(256);

        let serve_result = match raft_server_tls {
            Some(server_tls) => match builder.tls_config(server_tls) {
                Ok(mut tls_builder) => {
                    tracing::info!("Raft RPC server TLS (mTLS) enabled on {}", raft_socket_addr);
                    tls_builder
                        .add_service(raft_rpc_svc)
                        .serve(raft_socket_addr)
                        .await
                }
                Err(e) => {
                    tracing::error!("Raft RPC TLS config error: {e}");
                    return;
                }
            },
            None => {
                tracing::info!(
                    "Raft RPC server on {} WITHOUT TLS (inter-node auth disabled)",
                    raft_socket_addr
                );
                builder
                    .add_service(raft_rpc_svc)
                    .serve(raft_socket_addr)
                    .await
            }
        };

        if let Err(e) = serve_result {
            tracing::error!("Raft RPC server error: {e}");
        }
    });

    // 10. 启动客户端 gRPC Server（grpc_addr 端口，可选 TLS；端口已在步骤 1.5 预绑定）

    // 10a. gRPC Health Check 服务已在指标循环前初始化（P1-09 真实语义）
    let _ = &health_service;

    // 10b. gRPC Server Reflection：生产默认关闭（规格 C.4.1，配置开关）
    let reflection_service = if cfg.security.reflection_enabled {
        Some(
            tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
                .register_encoded_file_descriptor_set(coord_proto::FILE_DESCRIPTOR_SET)
                .build_v1()
                .map_err(|e| format!("failed to build reflection service: {e}"))?,
        )
    } else {
        tracing::info!("gRPC reflection disabled (security.reflection_enabled=false)");
        None
    };

    // 检查 TLS 配置（P2-02：validate() 已保证 cert/key 成对，此处不再隐式降级）
    let use_tls = cfg.security.tls_cert.is_some() && cfg.security.tls_key.is_some();

    // 10c. P0-C.1：服务端鉴权拦截器挂载（TLS/非 TLS 两分支统一）
    // P1-09：MetricsLayer 挂最外层（覆盖全部服务，含鉴权拒绝路径）
    let metrics_layer = coord_server::metrics::MetricsLayer::new(Arc::clone(&metrics));
    let mut auth_interceptor = ServerAuthInterceptor::new(
        Arc::clone(&signing_keyring),
        Arc::clone(&revocation_store),
        use_tls && cfg.security.tls_ca.is_some(),
    )
    .with_role_provider(Arc::clone(&auth_manager))
    .with_audit_logger(Arc::clone(&audit_logger));
    auth_interceptor.set_enabled(auth_enabled);
    let auth_layer = ServerAuthLayer::new(Arc::new(auth_interceptor));

    // 取出预绑定的 listener 并转换为 tonic 可接受的 stream
    // （P2-05：Option 包装——TLS 热加载滚动重启时首轮复用预绑定流，后续同端口重绑）
    let grpc_listener = grpc_listener
        .take()
        .ok_or("grpc_listener already consumed")?;
    let mut grpc_stream = Some(tokio_stream::wrappers::TcpListenerStream::new(
        grpc_listener,
    ));

    // P1-07：优雅停机序列 —— 摘流（tonic 排空在飞请求）→ 领导权移交 → raft 关闭。
    // 移交任务独立监听信号（与 serve 的 shutdown future 各注册一次信号监听）。
    let shutdown_signal_future = shutdown_signal();
    let node_for_transfer = Arc::clone(&node);
    let transfer_task = tokio::spawn(async move {
        shutdown_signal().await;
        if let Some(ref raft) = node_for_transfer.raft {
            if raft.current_leader().await == Some(node_for_transfer.node_id) {
                if let Some(target) = node_for_transfer.pick_transfer_target().await {
                    tracing::info!("Graceful shutdown: transferring leadership to node {target}");
                    match tokio::time::timeout(
                        std::time::Duration::from_secs(5),
                        node_for_transfer.transfer_leadership(target),
                    )
                    .await
                    {
                        Ok(Ok(())) => {
                            let deadline =
                                tokio::time::Instant::now() + std::time::Duration::from_secs(5);
                            while tokio::time::Instant::now() < deadline
                                && raft.current_leader().await == Some(node_for_transfer.node_id)
                            {
                                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                            }
                            if raft.current_leader().await == Some(node_for_transfer.node_id) {
                                tracing::warn!("Leadership transfer did not complete in time");
                            }
                        }
                        Ok(Err(e)) => tracing::warn!("transfer_leader failed: {e}"),
                        Err(_) => tracing::warn!("transfer_leader timed out"),
                    }
                }
            }
        }
    });

    if let (Some(tls_cert), Some(tls_key)) =
        (cfg.security.tls_cert.clone(), cfg.security.tls_key.clone())
    {
        // P2-05：TLS 证书热加载——watcher 每 60s 检测 cert/key/ca 变化；
        // 变化时优雅排空当前 accept 循环，重建 identity 后同端口重绑滚动重启。
        let mut tls_cfg = TlsConfig::new(
            tls_cert.clone(),
            tls_key.clone(),
            cfg.security.tls_ca.clone(),
        );
        let tls_reload_requested = Arc::new(std::sync::atomic::AtomicBool::new(false));
        {
            let watcher_cfg = tls_cfg.clone();
            let reload_flag = Arc::clone(&tls_reload_requested);
            tokio::spawn(async move {
                let mut fingerprint: Option<Vec<coord_server::tls::FileFingerprint>> = None;
                let mut ticker = tokio::time::interval(std::time::Duration::from_secs(60));
                loop {
                    ticker.tick().await;
                    if watcher_cfg.files_changed(&mut fingerprint) {
                        tracing::info!("TLS certificate files changed; requesting hot reload");
                        reload_flag.store(true, std::sync::atomic::Ordering::SeqCst);
                    }
                }
            });
        }

        loop {
            let server_tls = tls::build_server_tls(&tls_cfg)?;
            tracing::info!(
                "TLS enabled for gRPC server on {}, cert={}, mTLS={}",
                grpc_socket_addr,
                tls_cfg.cert_path.display(),
                tls_cfg.ca_path.is_some()
            );

            // 本次 serve 的流：首轮复用预绑定 listener（dev 就绪探测依赖），
            // 热加载后重绑同端口（监听 socket 无 TIME_WAIT，重绑安全）
            let stream = match grpc_stream.take() {
                Some(s) => s,
                None => {
                    let listener = tokio::net::TcpListener::bind(grpc_socket_addr).await?;
                    tokio_stream::wrappers::TcpListenerStream::new(listener)
                }
            };

            // 本次 serve 的退出条件：SIGINT/SIGTERM 或证书热加载请求（500ms 轮询标志位）
            let reload_flag_for_serve = Arc::clone(&tls_reload_requested);
            let serve_future = async move {
                loop {
                    tokio::select! {
                        _ = shutdown_signal() => break,
                        _ = tokio::time::sleep(std::time::Duration::from_millis(500)) => {
                            if reload_flag_for_serve.load(std::sync::atomic::Ordering::SeqCst) {
                                break;
                            }
                        }
                    }
                }
            };

            let grpc_future = tonic::transport::Server::builder()
                .max_concurrent_streams(cfg.network.max_concurrent_streams)
                .layer(metrics_layer.clone())
                .layer(auth_layer.clone())
                .tls_config(server_tls)
                .map_err(|e| format!("TLS server config: {e}"))?
                .add_optional_service(reflection_service.clone())
                .add_service(health_service.clone())
                .add_service(kv_svc.clone())
                .add_service(txn_svc.clone())
                .add_service(lease_svc.clone())
                .add_service(watch_svc.clone())
                .add_service(maintenance_svc.clone())
                .add_service(auth_svc.clone())
                .add_service(capability_svc.clone())
                .serve_with_incoming_shutdown(stream, serve_future);

            // P1-07：信号后 tonic 优雅排空在飞请求，返回后才继续清理
            grpc_future.await?;

            // 判定退出原因：热加载 → 重建证书配置并继续；否则为真实停机
            if tls_reload_requested.swap(false, std::sync::atomic::Ordering::SeqCst) {
                let new_cfg = TlsConfig::new(
                    tls_cert.clone(),
                    tls_key.clone(),
                    cfg.security.tls_ca.clone(),
                );
                // 新证书无效时保留旧配置继续服务（不中断在线集群）
                match tls::build_server_tls(&new_cfg) {
                    Ok(_) => {
                        tls_cfg = new_cfg;
                        tracing::info!("TLS certificates hot-reloaded");
                    }
                    Err(e) => {
                        tracing::warn!("TLS hot reload failed, keeping previous certificate: {e}");
                    }
                }
                continue;
            }
            break;
        }
    } else {
        tracing::info!(
            "Coord server v{} started: node_id={}, grpc_addr={}, raft_addr={} (no TLS, auth_enabled={})",
            env!("CARGO_PKG_VERSION"),
            node_id,
            grpc_socket_addr,
            raft_addr,
            auth_enabled,
        );

        let grpc_future = tonic::transport::Server::builder()
            .max_concurrent_streams(cfg.network.max_concurrent_streams)
            .layer(metrics_layer)
            .layer(auth_layer)
            .add_optional_service(reflection_service)
            .add_service(health_service)
            .add_service(kv_svc)
            .add_service(txn_svc)
            .add_service(lease_svc)
            .add_service(watch_svc)
            .add_service(maintenance_svc)
            .add_service(auth_svc)
            .add_service(capability_svc)
            .serve_with_incoming_shutdown(
                grpc_stream.take().ok_or("grpc_stream already consumed")?,
                shutdown_signal_future,
            );

        // P1-07：信号后 tonic 优雅排空在飞请求，返回后才继续清理
        grpc_future.await?;
    }

    // 12. 清理（P1-07）：raft 排空关闭（openraft 等待 core task 退出）→
    //     等待移交任务结束 → 终止 raft RPC server。
    tracing::info!("gRPC drained; shutting down raft instance");
    raft.shutdown()
        .await
        .map_err(|e| format!("raft shutdown: {e}"))?;
    let _ = transfer_task.await;
    raft_handle.abort();
    tracing::info!("Coord server shutdown complete");

    Ok(())
}

/// 向指定节点发送 JoinRequest（P0-D.1）
async fn send_join_request(
    addr: &str,
    node_id: u64,
    raft_addr: &str,
    grpc_addr: &str,
) -> Result<coord_proto::maintenance::JoinResponse, Box<dyn std::error::Error + Send + Sync>> {
    use coord_proto::maintenance::maintenance_client::MaintenanceClient;
    let endpoint = format!("http://{addr}");
    let channel = tonic::transport::Endpoint::from_shared(endpoint)?
        .connect_timeout(std::time::Duration::from_secs(3))
        .connect()
        .await?;
    let mut client = MaintenanceClient::new(channel);
    let resp = client
        .join(coord_proto::maintenance::JoinRequest {
            node_id,
            raft_addr: raft_addr.to_string(),
            grpc_addr: grpc_addr.to_string(),
        })
        .await?
        .into_inner();
    Ok(resp)
}

/// 生成随机密码（root 密码强制用，规格 C.4.8）：24 位无易混淆字符。
fn generate_random_password(len: usize) -> String {
    use rand::Rng;
    const CHARS: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz23456789!@#%+=_";
    let mut rng = rand::thread_rng();
    (0..len)
        .map(|_| CHARS[rng.gen_range(0..CHARS.len())] as char)
        .collect()
}

/// Auth 根密钥加载/生成（规格 C.4.2）。
///
/// 优先级：`security.auth_root_key`（hex）→ `<data_dir>/auth-root-key.bin`
/// → 首次生成 32 随机字节并以 0600 写入。
fn load_or_create_root_key(
    data_dir: &std::path::Path,
    configured_hex: Option<&str>,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    use rand::RngCore;

    if let Some(hex_str) = configured_hex {
        let key = hex::decode(hex_str.trim())
            .map_err(|e| format!("security.auth_root_key is not valid hex: {e}"))?;
        if key.len() != 32 {
            return Err(format!(
                "security.auth_root_key must be 32 bytes (got {} bytes)",
                key.len()
            )
            .into());
        }
        return Ok(key);
    }

    let path = data_dir.join("auth-root-key.bin");
    if path.exists() {
        let key = std::fs::read(&path)
            .map_err(|e| format!("read auth root key {}: {e}", path.display()))?;
        if key.len() != 32 {
            return Err(format!(
                "auth root key {} must be 32 bytes (got {})",
                path.display(),
                key.len()
            )
            .into());
        }
        return Ok(key);
    }

    // 首次启动：生成并持久化（0600）
    let mut key = vec![0u8; 32];
    rand::thread_rng().fill_bytes(&mut key);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let mut opts = std::fs::OpenOptions::new();
        opts.create(true).write(true).truncate(true).mode(0o600);
        opts.open(&path)
            .map_err(|e| format!("create auth root key {}: {e}", path.display()))?
            .write_all(&key)
            .map_err(|e| format!("write auth root key {}: {e}", path.display()))?;
    }
    #[cfg(not(unix))]
    std::fs::write(&path, &key)
        .map_err(|e| format!("write auth root key {}: {e}", path.display()))?;

    tracing::info!(
        "Generated new auth root key at {} (mode 0600)",
        path.display()
    );
    Ok(key)
}

/// 优雅关闭信号处理
///
/// 监听 SIGTERM（K8s 终止）和 SIGINT（Ctrl+C），
/// 收到信号后触发 tonic graceful shutdown。
async fn shutdown_signal() {
    let ctrl_c = async {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {}
            Err(e) => {
                tracing::error!("failed to install Ctrl+C handler: {e}");
                std::process::exit(1);
            }
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                let _ = sig.recv().await;
            }
            Err(e) => {
                tracing::error!("failed to install SIGTERM handler: {e}");
                std::process::exit(1);
            }
        }
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
    bind_addr: &str,
    grpc_port: u16,
    agent_port: u16,
    data_dir: &std::path::Path,
    cluster_name: &str,
    fresh: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let server_addr = format!("{}:{}", bind_addr, grpc_port);
    let raft_port = grpc_port + 1;
    let raft_addr = format!("{}:{}", bind_addr, raft_port);
    let agent_addr = format!("{}:{}", bind_addr, agent_port);
    let http_port = agent_port + 1;

    // 1. 确定数据目录（开发模式使用项目本地目录，避免权限问题）
    let dev_data_dir = if data_dir.to_string_lossy() == "/var/lib/coord" {
        // 使用默认全局 --data-dir 值时，dev 模式改用本地目录
        let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        cwd.join("coord-dev-data")
    } else {
        data_dir.to_path_buf()
    };

    // 1.5. --fresh: 启动前清空数据目录
    if fresh && dev_data_dir.exists() {
        tracing::info!(
            "--fresh: removing existing data directory {}",
            dev_data_dir.display()
        );
        std::fs::remove_dir_all(&dev_data_dir)?;
    }
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
    // P0-C.1：dev 模式强制关闭鉴权（root/root 默认凭据），反射开启便于调试
    server_cfg.security.auth_enabled = false;
    server_cfg.security.reflection_enabled = true;

    // BFF HTTP 端口（与 run_server 保持一致：grpc_port + 10）
    let bff_http_port = grpc_port + 10;

    tracing::info!(
        "Dev mode: starting server on {} (raft: {}, data: {}, http: {})",
        server_addr,
        raft_addr,
        dev_data_dir.display(),
        bff_http_port
    );

    // 3. 后台启动 Server（内部有自己的 shutdown_signal，Ctrl+C 时自动关闭）
    let server_addr_for_agent = server_addr.clone();
    let raft_addr_for_display = raft_addr.clone();
    let server_handle = tokio::spawn(async move {
        if let Err(e) = run_server(&server_cfg, &raft_addr, true, true, None).await {
            tracing::error!("Dev server exited with error: {e}");
        }
    });

    // 4. 等待 Server 端口就绪
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if tokio::time::Instant::now() > deadline {
            server_handle.abort();
            return Err(
                format!("Server did not become ready on {} within 30s", server_addr).into(),
            );
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
            if let Ok(ep) = tonic::transport::Endpoint::from_shared(server_url.clone()) {
                if let Ok(channel) = ep
                    .connect_timeout(std::time::Duration::from_secs(2))
                    .connect()
                    .await
                {
                    let mut client = MaintenanceClient::new(channel);
                    let request = tonic::Request::new(coord_proto::maintenance::StatusRequest {});
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
            }
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        }
    }

    // 5. 构建 Agent 配置并启动
    let agent_config = coord_agent::AgentConfig {
        agent_addr: agent_addr.clone(),
        http_addr: format!("{}:{}", bind_addr, http_port),
        data_dir: dev_data_dir.join("agent").to_string_lossy().to_string(),
        static_peers: vec![server_addr_for_agent],
        ..Default::default()
    };

    tracing::info!(
        "Dev mode: starting agent on {} (http: {})",
        agent_addr,
        http_port
    );

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
            return Err(format!("Agent did not become ready on {} within 15s", agent_addr).into());
        }
        if tokio::net::TcpStream::connect(&agent_addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    // 7. 打印连接信息
    println!();
    println!(
        "  ✓ Server 启动完成: {} (Raft: {})",
        server_addr, raft_addr_for_display
    );
    println!(
        "  ✓ Agent 启动完成:  {} (HTTP: {}:{})",
        agent_addr, bind_addr, http_port
    );
    println!();
    println!("  连接方式:");
    println!("    UI 控制台 → http://{}:{}", bind_addr, bff_http_port);
    println!("    Java 应用  → {}", agent_addr);
    println!(
        "    Rust SDK  → {} (Agent 模式) 或 {} (Direct 模式)",
        agent_addr, server_addr
    );
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
