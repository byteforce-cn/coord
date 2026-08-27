// CLI 直连 TLS/mTLS 集群集成测试（生产上线收口项 #1）
//
// 背景：`coord` CLI 此前所有管理命令（auth/member/snapshot pull/reset/idgen）
// 均以明文 http:// 直连，无法管理启用了 TLS/mTLS 的生产集群。
// 本套件验证 CLI 全局 `--tls-ca/--tls-cert/--tls-key/--tls-server-name`
// 参数可对真实 TLS 集群执行管理命令，且明文/缺证书路径 fail-closed。
//
// 实现方式：spawn 真实 `coord server`（CARGO_BIN_EXE_coord，mTLS 配置），
// 再以子进程方式运行真实 `coord` CLI 命令断言退出码与行为。

use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair, SanType};

/// CA 密钥对（用于签发服务端与客户端证书）
struct TestCa {
    key: KeyPair,
    params: CertificateParams,
    cert_pem: Vec<u8>,
}

/// 生成测试 CA（self-signed，IsCa）
fn generate_ca() -> TestCa {
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "coord-test-ca");
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let key = KeyPair::generate().unwrap();
    let cert = params.self_signed(&key).unwrap();
    TestCa {
        key,
        params,
        cert_pem: cert.pem().into_bytes(),
    }
}

/// 由 CA 签发证书；SAN 仅含 IP（CLI 经 127.0.0.1 连接）
fn issue_cert(ca: &TestCa, cn: &str, ips: Vec<&str>) -> (Vec<u8>, Vec<u8>) {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, cn);
    params.subject_alt_names = ips
        .into_iter()
        .map(|ip| SanType::IpAddress(ip.parse().unwrap()))
        .collect();
    let issuer = rcgen::Issuer::from_params(&ca.params, &ca.key);
    let cert = params.signed_by(&key, &issuer).unwrap();
    (cert.pem().into_bytes(), key.serialize_pem().into_bytes())
}

fn write_file(dir: &Path, name: &str, data: &[u8]) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, data).unwrap();
    path
}

/// 找一个空闲端口（bind :0 后释放）
fn find_free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// 等待 TCP 端口可连接（TLS 端口同样先完成 TCP 握手）
fn wait_port(port: u16, timeout: std::time::Duration) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "server port {port} not listening within {timeout:?}"
        );
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
}

/// 生成 mTLS 测试集群：CA + server/client 证书 + 服务器 TOML 配置
fn mtls_setup() -> (tempfile::TempDir, u16, u16) {
    let dir = tempfile::tempdir().unwrap();
    let ca = generate_ca();
    write_file(dir.path(), "ca.crt", &ca.cert_pem);
    let (server_cert, server_key) = issue_cert(&ca, "coord-server", vec!["127.0.0.1"]);
    let server_cert_path = write_file(dir.path(), "server.crt", &server_cert);
    let server_key_path = write_file(dir.path(), "server.key", &server_key);
    let (client_cert, client_key) = issue_cert(&ca, "coord-cli", vec![]);
    write_file(dir.path(), "client.crt", &client_cert);
    write_file(dir.path(), "client.key", &client_key);

    let grpc_port = find_free_port();
    let raft_port = find_free_port();
    let auth_root_key = "ab".repeat(32); // 64 hex chars
    let config = format!(
        r#"
[node]
id = 1

[network]
grpc_addr = "127.0.0.1:{grpc_port}"
raft_addr = "127.0.0.1:{raft_port}"

[cluster]
cluster_name = "cli-tls-test"
bootstrap = true

[storage]
data_dir = "{data_dir}"

[security]
auth_enabled = false
auth_root_key = "{auth_root_key}"
tls_cert = "{cert}"
tls_key = "{key}"
tls_ca = "{ca_path}"
"#,
        data_dir = dir.path().join("data").display(),
        cert = server_cert_path.display(),
        key = server_key_path.display(),
        ca_path = dir.path().join("ca.crt").display(),
    );
    std::fs::write(dir.path().join("server.toml"), config).unwrap();
    (dir, grpc_port, raft_port)
}

/// spawn 真实 coord server 子进程（mTLS 配置），返回 Child
///
/// 注意：CLI 全局参数带默认值会覆盖配置文件同名字段（--addr/--data-dir 等），
/// 必须显式传参（与 m0_recovery_suite/auth_enforcement_test 同口径）。
fn spawn_server(dir: &Path, grpc_port: u16, raft_port: u16) -> std::process::Child {
    let bin = env!("CARGO_BIN_EXE_coord");
    let log = std::fs::File::create(dir.join("server.log")).unwrap();
    Command::new(bin)
        .arg("server")
        .arg("--config")
        .arg(dir.join("server.toml"))
        .arg("--addr")
        .arg(format!("127.0.0.1:{grpc_port}"))
        .arg("--raft-addr")
        .arg(format!("127.0.0.1:{raft_port}"))
        .arg("--data-dir")
        .arg(dir.join("data"))
        .env("COORD_ROOT_PASSWORD", "tls-test-root-password")
        .env("RUST_LOG", "coord=info")
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log))
        .spawn()
        .unwrap_or_else(|e| panic!("spawn coord server on {grpc_port}: {e}"))
}

/// 运行 CLI 子命令，返回退出码与输出
fn run_cli(dir: &Path, args: &[&str]) -> Output {
    let bin = env!("CARGO_BIN_EXE_coord");
    Command::new(bin)
        .args(args)
        .current_dir(dir)
        .output()
        .unwrap_or_else(|e| panic!("run coord CLI {:?}: {e}", args))
}

/// 带 mTLS 身份的管理命令参数组（addr 由调用方拼接）
fn tls_flags(dir: &Path) -> [String; 6] {
    [
        "--tls-ca".into(),
        dir.join("ca.crt").display().to_string(),
        "--tls-cert".into(),
        dir.join("client.crt").display().to_string(),
        "--tls-key".into(),
        dir.join("client.key").display().to_string(),
    ]
}

fn kill_child(child: &mut std::process::Child) {
    let _ = child.kill();
    let _ = child.wait();
}

#[test]
fn cli_manages_mtls_cluster_over_tls() {
    let (dir, grpc_port, raft_port) = mtls_setup();
    let addr = format!("127.0.0.1:{grpc_port}");
    let mut server = spawn_server(dir.path(), grpc_port, raft_port);
    wait_port(grpc_port, std::time::Duration::from_secs(30));

    let tls = tls_flags(dir.path());

    // 1. auth status（AuthClient，经 TLS/mTLS）
    let mut args: Vec<&str> = vec!["auth", "status", "--addr", &addr];
    args.extend(tls.iter().map(|s| s.as_str()));
    let out = run_cli(dir.path(), &args);
    assert!(
        out.status.success(),
        "auth status over mTLS should succeed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // 2. member list（MaintenanceClient）
    let mut args: Vec<&str> = vec!["member", "list", "--addr", &addr];
    args.extend(tls.iter().map(|s| s.as_str()));
    let out = run_cli(dir.path(), &args);
    assert!(
        out.status.success(),
        "member list over mTLS should succeed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // 3. capability list（CapabilityRegistryClient）
    let mut args: Vec<&str> = vec!["capability", "list", "--addr", &addr];
    args.extend(tls.iter().map(|s| s.as_str()));
    let out = run_cli(dir.path(), &args);
    assert!(
        out.status.success(),
        "capability list over mTLS should succeed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // 4. reset --keep-idgen（KvClient 导出 /_idgen/ 前缀）
    // 复用：先 reset 目标目录需存在
    let reset_target = dir.path().join("reset-target");
    std::fs::create_dir_all(&reset_target).unwrap();
    let reset_target_str = reset_target.display().to_string();
    let mut args: Vec<&str> = vec![
        "reset",
        "--addr",
        &addr,
        "--keep-idgen",
        "--data-dir",
        &reset_target_str,
    ];
    args.extend(tls.iter().map(|s| s.as_str()));
    let out = run_cli(dir.path(), &args);
    assert!(
        out.status.success(),
        "reset --keep-idgen over mTLS should succeed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    kill_child(&mut server);
}

#[test]
fn cli_without_tls_cannot_reach_mtls_cluster() {
    let (dir, grpc_port, raft_port) = mtls_setup();
    let addr = format!("127.0.0.1:{grpc_port}");
    let mut server = spawn_server(dir.path(), grpc_port, raft_port);
    wait_port(grpc_port, std::time::Duration::from_secs(30));

    // 无任何 TLS 参数：明文直连 TLS 端口 → 失败
    let out = run_cli(dir.path(), &["auth", "status", "--addr", &addr]);
    assert!(
        !out.status.success(),
        "plaintext CLI must fail against a TLS-only cluster"
    );

    // 仅 --tls-ca（无客户端证书）：mTLS 服务端拒绝握手 → 失败
    let ca = dir.path().join("ca.crt");
    let out = run_cli(
        dir.path(),
        &[
            "auth",
            "status",
            "--addr",
            &addr,
            "--tls-ca",
            &ca.display().to_string(),
        ],
    );
    assert!(
        !out.status.success(),
        "CLI without client cert must fail against an mTLS cluster"
    );

    // --tls-ca 与 --tls-cert 缺 key 的组合：参数校验直接拒绝
    let cert = dir.path().join("client.crt");
    let out = run_cli(
        dir.path(),
        &[
            "auth",
            "status",
            "--addr",
            &addr,
            "--tls-ca",
            &ca.display().to_string(),
            "--tls-cert",
            &cert.display().to_string(),
        ],
    );
    assert!(
        !out.status.success(),
        "--tls-cert without --tls-key must be rejected at argument validation"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("together"),
        "expected cert/key pairing error, got: {stderr}"
    );

    kill_child(&mut server);
}
