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
        /// Agent 本地 gRPC 监听地址（缺省 127.0.0.1:19527；覆盖 --agent-config）
        #[arg(long)]
        agent_addr: Option<String>,
        /// HTTP 可观测性监听地址（缺省 127.0.0.1:19528；覆盖 --agent-config）
        #[arg(long)]
        http_addr: Option<String>,
        /// 成员发现模式（默认 "static"；覆盖 --agent-config）
        #[arg(long)]
        discovery: Option<String>,
        /// 静态配置的 Server 节点列表（逗号分隔；覆盖 --agent-config）
        #[arg(long, value_delimiter = ',')]
        static_peers: Vec<String>,
        /// Agent TOML 配置文件（生产用：可含 [tls]/[services]/[replication] 等段）
        #[arg(long)]
        agent_config: Option<String>,
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
            discovery,
            static_peers,
            agent_config,
        } => {
            assert_eq!(agent_addr, None);
            assert_eq!(http_addr, None);
            assert_eq!(discovery, None);
            assert!(static_peers.is_empty());
            assert_eq!(agent_config, None);
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
            assert_eq!(agent_addr.as_deref(), Some("0.0.0.0:19527"));
            assert_eq!(http_addr.as_deref(), Some("0.0.0.0:19528"));
            assert_eq!(static_peers.len(), 3);
            assert_eq!(static_peers[0], "10.0.1.1:50051");
        }
        _ => panic!("expected Agent subcommand"),
    }
}

#[test]
fn test_agent_subcommand_with_config_file() {
    let args = vec![
        "coord",
        "agent",
        "--agent-config",
        "/etc/coord/agent.toml",
        "--agent-addr",
        "0.0.0.0:19527",
    ];
    let cli = TestCli::try_parse_from(args).expect("should parse agent config file");

    match cli.command {
        TestCommands::Agent {
            agent_addr,
            agent_config,
            ..
        } => {
            assert_eq!(agent_addr.as_deref(), Some("0.0.0.0:19527"));
            assert_eq!(agent_config.as_deref(), Some("/etc/coord/agent.toml"));
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

// ──── E2E：`coord agent --agent-config` 生产配置路径（含 TLS）────

use std::process::{Command, Stdio};
use std::time::Duration;

fn find_free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// 生成 CA + agent 证书（IP SAN 127.0.0.1），写入 tmpdir，返回 (ca.crt, agent.crt, agent.key) 路径
fn gen_agent_tls(
    dir: &std::path::Path,
) -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "cli-agent-test-ca");
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();

    let key = rcgen::KeyPair::generate().unwrap();
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "coord-agent");
    params.subject_alt_names = vec![rcgen::SanType::IpAddress("127.0.0.1".parse().unwrap())];
    let issuer = rcgen::Issuer::from_params(&ca_params, &ca_key);
    let cert = params.signed_by(&key, &issuer).unwrap();

    let ca_path = dir.join("ca.crt");
    let cert_path = dir.join("agent.crt");
    let key_path = dir.join("agent.key");
    std::fs::write(&ca_path, ca_cert.pem().as_bytes()).unwrap();
    std::fs::write(&cert_path, cert.pem().as_bytes()).unwrap();
    std::fs::write(&key_path, key.serialize_pem().as_bytes()).unwrap();
    (ca_path, cert_path, key_path)
}

async fn tls_health_check(port: u16, ca_path: &std::path::Path, cert: &[u8], key: &[u8]) {
    use tonic::transport::{Certificate, ClientTlsConfig, Identity};
    let ca = Certificate::from_pem(std::fs::read(ca_path).unwrap());
    let tls = ClientTlsConfig::new()
        .ca_certificate(ca)
        .identity(Identity::from_pem(cert, key)); // ca_path → mTLS，客户端需携带身份
    let channel = tonic::transport::Endpoint::from_shared(format!("https://127.0.0.1:{port}"))
        .unwrap()
        .tls_config(tls)
        .unwrap()
        .connect_timeout(Duration::from_secs(3))
        .connect()
        .await
        .unwrap();
    let mut client = tonic_health::pb::health_client::HealthClient::new(channel);
    let resp = client
        .check(tonic_health::pb::HealthCheckRequest::default())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        resp.status,
        tonic_health::pb::health_check_response::ServingStatus::Serving as i32
    );
}

#[tokio::test]
async fn test_coord_agent_subcommand_serves_tls_from_config() {
    let tmpdir = tempfile::tempdir().unwrap();
    let port = find_free_port();
    let http_port = find_free_port();
    let (ca_path, cert_path, key_path) = gen_agent_tls(tmpdir.path());
    let data_dir = tmpdir.path().join("agent-data");

    let config_toml = format!(
        r#"
agent_addr = "127.0.0.1:{port}"
http_addr = "127.0.0.1:{http_port}"
data_dir = "{data_dir}"

[tls]
cert_path = "{cert}"
key_path = "{key}"
ca_path = "{ca}"
"#,
        data_dir = data_dir.display(),
        cert = cert_path.display(),
        key = key_path.display(),
        ca = ca_path.display(),
    );
    let config_path = tmpdir.path().join("agent.toml");
    std::fs::write(&config_path, config_toml).unwrap();

    let bin = env!("CARGO_BIN_EXE_coord");
    let log = std::fs::File::create(tmpdir.path().join("agent.log")).unwrap();
    let mut child = Command::new(bin)
        .arg("agent")
        .arg("--agent-config")
        .arg(&config_path)
        .arg("--data-dir")
        .arg(&data_dir) // 全局 --data-dir 带默认值，会覆盖配置文件同名字段，必须显式传
        .env("RUST_LOG", "coord_agent=info")
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log))
        .spawn()
        .expect("spawn coord agent");

    // 等待端口监听
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .is_ok()
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "coord agent TLS port never became reachable"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // 配置了 tls → 入站必须真实 TLS 服务（标准健康检查经 mTLS 通道）
    let cert_pem = std::fs::read(&cert_path).unwrap();
    let key_pem = std::fs::read(&key_path).unwrap();
    tls_health_check(port, &ca_path, &cert_pem, &key_pem).await;

    let _ = child.kill();
    let _ = child.wait();
}
