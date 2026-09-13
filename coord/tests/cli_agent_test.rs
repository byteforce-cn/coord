// TDD: coord CLI agent 子命令测试
//
// — 验证 `coord agent` 子命令能被 clap 正确解析。

use clap::Parser;

// 复用 main.rs 中的 Cli 定义（通过 include! 或复制结构体）
// 这里直接复制最小化的 CLI 结构体来测试解析

#[derive(Parser)]
#[command(name = "coord", about = "test")]
struct TestCli {
    // 与 coord/src/main.rs 保持一致：**不能**设 clap `default_value`。带上默认值会让
    // 「用户没传」与「用户传了 /var/lib/coord」不可区分，从而永远覆盖配置文件里的
    // `data_dir`（agent 因此把插件账户写到 /var/lib/coord）。
    #[arg(long, global = true)]
    data_dir: Option<String>,

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

    // 未给出 `--data-dir` 时必须为 None（即真实 CLI 不再带 default_value），
    // 否则子命令无法把「用户没传」与「用户传了默认路径」区分开。
    assert_eq!(cli.data_dir, None);

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

// ──── 回归：`--data-dir` 未显式给出时必须沿用配置文件的 `data_dir` ────

fn log_tail(path: &std::path::Path, lines: usize) -> String {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| format!("<读不到日志: {e}>"));
    let all: Vec<&str> = text.lines().collect();
    let start = all.len().saturating_sub(lines);
    all[start..].join("\n")
}

/// 启动 `coord agent`，等待 `expected` 数据目录出现；失败时把 agent 日志尾部带进断言。
async fn spawn_agent_and_expect_data_dir(
    cfg_path: &std::path::Path,
    explicit_data_dir: Option<&std::path::Path>,
    expected: &std::path::Path,
) {
    let bin = env!("CARGO_BIN_EXE_coord");
    let log_path = cfg_path.with_extension("agent.log");
    let log = std::fs::File::create(&log_path).unwrap();

    let mut cmd = Command::new(bin);
    cmd.arg("agent").arg("--agent-config").arg(cfg_path);
    if let Some(dir) = explicit_data_dir {
        cmd.arg("--data-dir").arg(dir);
    }
    let mut child = cmd
        .env("RUST_LOG", "coord=info")
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log))
        .spawn()
        .expect("spawn coord agent");

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while !expected.exists() && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let created = expected.exists();

    let _ = child.kill();
    let _ = child.wait();

    assert!(
        created,
        "agent 必须在 {} 下创建数据目录（显式 --data-dir: {}）\n\
         --- agent.log 尾部 ---\n{}",
        expected.display(),
        explicit_data_dir.is_some(),
        log_tail(&log_path, 25)
    );
}

/// 回归用例（两个方向都铉住）：
///
/// 1. **只给配置、不给 `--data-dir`** → 必须用配置文件里的 `data_dir`；
/// 2. **显式给 `--data-dir`** → 必须优先于配置文件（`CLI > 配置` 的优先级不得被破坏）。
///
/// 背景（为什么需要这个用例）：全局 `--data-dir` 曾经带 clap
/// `default_value = "/var/lib/coord"` 且类型是 `PathBuf`，因此「用户没传」与
/// 「用户传了 /var/lib/coord」不可区分；`coord/src/main.rs` 用它**无条件**覆盖
/// `agent_config.data_dir`，于是配置文件里的 `data_dir` 永远失效，agent 把插件账户
/// 密码与缓存写到 `/var/lib/coord`。
///
/// 在 CI 上（runner 非 root，`/var/lib` 不可写）这表现为
/// `Permission denied (os error 13)`：插件身份无法持久化 → agent 重启后
/// `unauthenticated: invalid credentials` → 回退共享未鉴权客户端 → 服务端 fail-closed
/// 拒绝（`plugin_real_agent_process_e2e` 就是这么红的）。
///
/// 关键属性：该用例在 **root 与非 root 下都会失败**（旧行为下配置里指定的目录根本
/// 不会被创建）—— 这正是此前「本地全绿、CI 全红」的原因，所以本地用 root 跑测试
/// 也不会漏掉它。
#[tokio::test]
async fn test_agent_uses_config_data_dir_unless_flag_is_explicit() {
    let tmpdir = tempfile::tempdir().unwrap();

    // 只写配置；`--data-dir` 是否传由调用方决定。
    let write_cfg = |name: &str, data_dir: &std::path::Path| {
        let port = find_free_port();
        let http_port = find_free_port();
        let peer = find_free_port();
        let cfg = tmpdir.path().join(name);
        std::fs::write(
            &cfg,
            format!(
                "agent_addr = \"127.0.0.1:{port}\"\n\
                 http_addr = \"127.0.0.1:{http_port}\"\n\
                 data_dir = \"{dir}\"\n\
                 discovery_mode = \"static\"\n\
                 static_peers = [\"127.0.0.1:{peer}\"]\n",
                dir = data_dir.display()
            ),
        )
        .unwrap();
        cfg
    };

    // (1) 只给配置：agent 必须自己创建配置里指定的那个数据目录。
    let from_config = tmpdir.path().join("data-from-config");
    let cfg1 = write_cfg("agent-config-only.toml", &from_config);
    spawn_agent_and_expect_data_dir(&cfg1, None, &from_config).await;

    // (2) 显式 --data-dir：必须优先于配置文件里的 data_dir。
    let in_config = tmpdir.path().join("data-in-config");
    let explicit = tmpdir.path().join("data-explicit");
    let cfg2 = write_cfg("agent-explicit.toml", &in_config);
    spawn_agent_and_expect_data_dir(&cfg2, Some(&explicit), &explicit).await;
    assert!(
        !in_config.exists(),
        "显式 --data-dir 存在时不得再使用配置文件里的 data_dir {}",
        in_config.display()
    );
}
