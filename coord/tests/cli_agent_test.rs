// TDD: coord CLI agent 子命令测试
//
// Phase A3 — 验证 `coord agent` 子命令能被 clap 正确解析。

use clap::Parser;

// 复用 main.rs 中的 Cli 定义（通过 include! 或复制结构体）
// 这里直接复制最小化的 CLI 结构体来测试解析

#[derive(Parser)]
#[command(name = "coord", about = "test")]
struct TestCli {
    #[arg(long, global = true, default_value = "/var/lib/coord")]
    data_dir: String,

    #[arg(long, global = true)]
    config: Option<String>,

    #[command(subcommand)]
    command: TestCommands,
}

#[derive(clap::Subcommand)]
enum TestCommands {
    /// 启动 Server 节点
    Server {
        #[arg(long, default_value = "1")]
        id: u64,
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
        #[arg(long)]
        raft_addr: Option<String>,
        #[arg(long)]
        join: Option<String>,
        #[arg(long, default_value = "false")]
        bootstrap: bool,
        #[arg(long, default_value = "coord-cluster")]
        cluster_name: String,
    },
    /// 启动 Agent 守护进程
    Agent {
        /// Agent 本地 gRPC 监听地址
        #[arg(long, default_value = "127.0.0.1:19527")]
        agent_addr: String,
        /// HTTP 可观测性监听地址
        #[arg(long, default_value = "127.0.0.1:19528")]
        http_addr: String,
        /// 数据目录路径
        #[arg(long, default_value = "/var/lib/coord-agent")]
        data_dir: String,
        /// 成员发现模式
        #[arg(long, default_value = "static")]
        discovery: String,
        /// 静态配置的 Server 节点列表（逗号分隔）
        #[arg(long, value_delimiter = ',')]
        static_peers: Vec<String>,
    },
}

#[test]
fn test_agent_subcommand_defaults() {
    let args = vec!["coord", "agent"];
    let cli = TestCli::try_parse_from(args).expect("should parse agent subcommand");

    match cli.command {
        TestCommands::Agent {
            agent_addr,
            http_addr,
            data_dir,
            discovery,
            static_peers,
        } => {
            assert_eq!(agent_addr, "127.0.0.1:19527");
            assert_eq!(http_addr, "127.0.0.1:19528");
            assert_eq!(data_dir, "/var/lib/coord-agent");
            assert_eq!(discovery, "static");
            assert!(static_peers.is_empty());
        }
        _ => panic!("expected Agent subcommand"),
    }
}

#[test]
fn test_agent_subcommand_with_peers() {
    let args = vec![
        "coord",
        "agent",
        "--agent-addr",
        "0.0.0.0:19527",
        "--http-addr",
        "0.0.0.0:19528",
        "--discovery",
        "static",
        "--static-peers",
        "10.0.1.1:50051,10.0.1.2:50051,10.0.1.3:50051",
    ];
    let cli = TestCli::try_parse_from(args).expect("should parse agent subcommand with peers");

    match cli.command {
        TestCommands::Agent {
            agent_addr,
            http_addr,
            static_peers,
            ..
        } => {
            assert_eq!(agent_addr, "0.0.0.0:19527");
            assert_eq!(http_addr, "0.0.0.0:19528");
            assert_eq!(static_peers.len(), 3);
            assert_eq!(static_peers[0], "10.0.1.1:50051");
        }
        _ => panic!("expected Agent subcommand"),
    }
}

#[test]
fn test_server_subcommand_still_works() {
    // 确保 Server 子命令仍然可用
    let args = vec!["coord", "server", "--id", "1", "--bootstrap"];
    let cli = TestCli::try_parse_from(args).expect("should parse server subcommand");

    match cli.command {
        TestCommands::Server { id, bootstrap, .. } => {
            assert_eq!(id, 1);
            assert!(bootstrap);
        }
        _ => panic!("expected Server subcommand"),
    }
}
