// `coord pki` 端到端：不装 grpcurl 也能从空目录拿到可用证书。
//
// 判据：
//   1. `coord pki ca/issue/list` 对真实 `coord dev` 可用；
//   2. 证书 SAN 与请求一致，`openssl verify -verify_hostname` 通过；
//   3. 负控制：错误名字必须失败（proof SAN 生效，而非 CN 回退）；
//   4. 非 ASCII CN（无显式 SAN）与非法 TTL 退出码稳定（2），连接失败为 1。

use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

fn find_free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn bindable(port: u16) -> bool {
    std::net::TcpListener::bind(("127.0.0.1", port)).is_ok()
}

/// dev 派生端口：grpc、grpc+1（Raft）、grpc+10（BFF/UI）、agent、agent+1（HTTP）。
fn pick_dev_ports() -> (u16, u16) {
    for _ in 0..50 {
        let grpc = find_free_port();
        let agent = find_free_port();
        let ports = [grpc, grpc + 1, grpc + 10, agent, agent + 1];
        let distinct: std::collections::BTreeSet<u16> = ports.iter().copied().collect();
        if distinct.len() == ports.len() && ports.iter().all(|&p| bindable(p)) {
            return (grpc, agent);
        }
    }
    panic!("no free port set found for dev mode");
}

struct DevInstance {
    child: Child,
    stdout_path: std::path::PathBuf,
    _data_dir: tempfile::TempDir,
    _log_dir: tempfile::TempDir,
}

impl Drop for DevInstance {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn_dev(grpc_port: u16, agent_port: u16) -> DevInstance {
    let data_dir = tempfile::tempdir().unwrap();
    let log_dir = tempfile::tempdir().unwrap();
    let stdout_path = log_dir.path().join("stdout.log");
    let stdout = std::fs::File::create(&stdout_path).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_coord"))
        .arg("dev")
        .arg("--bind-addr")
        .arg("127.0.0.1")
        .arg("--grpc-port")
        .arg(grpc_port.to_string())
        .arg("--agent-port")
        .arg(agent_port.to_string())
        .arg("--data-dir")
        .arg(data_dir.path())
        .arg("--fresh")
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn coord dev");
    DevInstance {
        child,
        stdout_path,
        _data_dir: data_dir,
        _log_dir: log_dir,
    }
}

fn wait_agent(dev: &mut DevInstance, addr: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if std::net::TcpStream::connect(addr).is_ok() {
            return;
        }
        if let Ok(Some(status)) = dev.child.try_wait() {
            panic!("coord dev exited early ({status})");
        }
        if Instant::now() > deadline {
            panic!("agent {addr} not ready within 30s");
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn coord(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_coord"))
        .args(args)
        .output()
        .expect("run coord")
}

fn assert_success(output: &Output, what: &str) {
    assert!(
        output.status.success(),
        "{what} failed (status {:?})\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn openssl(args: &[&str]) -> Output {
    Command::new("openssl")
        .args(args)
        .output()
        .expect("run openssl (required by this e2e)")
}

fn assert_openssl_ok(output: &Output, what: &str) {
    assert_success(output, what);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(": OK"),
        "{what}: unexpected output:\n{stdout}"
    );
}

/// 从 dev 启动日志提取 `sha256=<hex>`（轮询到出现或超时）。
fn wait_log_fingerprint(dev: &DevInstance, timeout: Duration) -> String {
    let deadline = Instant::now() + timeout;
    loop {
        let log = std::fs::read_to_string(&dev.stdout_path).unwrap_or_default();
        if let Some(rest) = log.split("sha256=").nth(1) {
            let fp = rest
                .split_whitespace()
                .next()
                .unwrap_or("")
                .trim_end_matches([',', ';'])
                .to_string();
            if fp.len() >= 64 {
                return fp;
            }
        }
        if Instant::now() > deadline {
            panic!("startup log has no CA SHA-256 fingerprint; log so far:\n{log}");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn pki_args<'a>(agent_addr: &'a str, tail: &[&'a str]) -> Vec<&'a str> {
    let mut args = vec!["pki"];
    args.extend_from_slice(tail);
    args.push("--addr");
    args.push(agent_addr);
    args
}

#[test]
fn pki_cli_issues_verifiable_cert_without_grpcurl() {
    let (grpc_port, agent_port) = pick_dev_ports();
    let agent_addr = format!("127.0.0.1:{agent_port}");
    let mut dev = spawn_dev(grpc_port, agent_port);
    wait_agent(&mut dev, &agent_addr);

    let work = tempfile::tempdir().unwrap();
    let ca_path = work.path().join("ca.pem");
    let out_dir = work.path().join("certs");

    // 1) 取根证书
    let ca_arg = ca_path.to_string_lossy().to_string();
    let out_arg = out_dir.to_string_lossy().to_string();
    let output = coord(&pki_args(&agent_addr, &["ca", "--out", ca_arg.as_str()]));
    assert_success(&output, "coord pki ca");
    assert!(ca_path.exists(), "CA PEM must be written");

    // 启动日志必须打印 CA SHA-256 指纹，且与 openssl 对导出的 ca.pem 计算结果一致。
    let fingerprint_log = wait_log_fingerprint(&dev, Duration::from_secs(5));
    let fp_output = openssl(&[
        "x509",
        "-in",
        ca_arg.as_str(),
        "-noout",
        "-fingerprint",
        "-sha256",
    ]);
    assert_success(&fp_output, "openssl x509 -fingerprint");
    let fp_openssl = String::from_utf8_lossy(&fp_output.stdout)
        .split('=')
        .nth(1)
        .expect("fingerprint output")
        .trim()
        .to_string();
    assert_eq!(
        fingerprint_log.to_ascii_uppercase(),
        fp_openssl.to_ascii_uppercase(),
        "startup log fingerprint must match the exported CA"
    );

    // 2) 签发（显式 SAN；1h 便于命中到期告警窗口）
    let output = coord(&pki_args(
        &agent_addr,
        &[
            "issue",
            "--cn",
            "orders-api",
            "--dns",
            "orders-api,orders-api.svc",
            "--ip",
            "127.0.0.1",
            "--ttl",
            "1h",
            "--out-dir",
            out_arg.as_str(),
            "--json",
        ],
    ));
    assert_success(&output, "coord pki issue");
    let meta: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("issue --json must emit parseable JSON");
    let serial = meta["serial"].as_str().expect("serial").to_string();
    assert_eq!(meta["common_name"], "orders-api");
    assert_eq!(meta["status"], "active");

    let leaf = out_dir.join("orders-api.pem");
    let key = out_dir.join("orders-api.key");
    assert!(leaf.exists() && key.exists(), "cert/key files must exist");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&key).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "private key must be 0600");
    }

    // 3) SAN 必须真的写进证书（openssl verify 单独通过不算数——CN 回退会假通过）
    let leaf_arg = leaf.to_string_lossy().to_string();
    let san = openssl(&["x509", "-in", &leaf_arg, "-noout", "-ext", "subjectAltName"]);
    assert_success(&san, "openssl x509 -ext subjectAltName");
    let san_text = String::from_utf8_lossy(&san.stdout);
    assert!(san_text.contains("DNS:orders-api"), "{san_text}");
    assert!(san_text.contains("DNS:orders-api.svc"), "{san_text}");
    assert!(san_text.contains("IP Address:127.0.0.1"), "{san_text}");

    // 4) 正例 + 负控制
    assert_openssl_ok(
        &openssl(&[
            "verify",
            "-CAfile",
            ca_arg.as_str(),
            "-verify_hostname",
            "orders-api",
            leaf_arg.as_str(),
        ]),
        "openssl verify (orders-api)",
    );
    let wrong = openssl(&[
        "verify",
        "-CAfile",
        ca_arg.as_str(),
        "-verify_hostname",
        "orders-api.evil",
        leaf_arg.as_str(),
    ]);
    assert!(
        !wrong.status.success(),
        "negative control: orders-api.evil must NOT verify\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&wrong.stdout),
        String::from_utf8_lossy(&wrong.stderr)
    );

    // 5) list 能取回同一 serial
    let output = coord(&pki_args(
        &agent_addr,
        &["list", "--cn", "orders-api", "--json"],
    ));
    assert_success(&output, "coord pki list");
    let rows: serde_json::Value = serde_json::from_slice(&output.stdout).expect("list JSON");
    let rows = rows.as_array().expect("list JSON array");
    assert_eq!(rows.len(), 1, "exactly one active certificate: {rows:?}");
    assert_eq!(rows[0]["serial"], serial);
    assert_eq!(rows[0]["status"], "active");
}

#[test]
fn pki_cli_exit_codes_are_stable() {
    let (grpc_port, agent_port) = pick_dev_ports();
    let agent_addr = format!("127.0.0.1:{agent_port}");
    let mut dev = spawn_dev(grpc_port, agent_port);
    wait_agent(&mut dev, &agent_addr);

    // 用法错误（TTL）在连接前失败 → 2
    let output = coord(&pki_args(
        &agent_addr,
        &["issue", "--cn", "x", "--ttl", "nope"],
    ));
    assert_eq!(output.status.code(), Some(2), "invalid --ttl must exit 2");
    assert!(String::from_utf8_lossy(&output.stderr).contains("invalid --ttl"));

    // 服务端 INVALID_ARGUMENT（非 ASCII CN 且未显式传 SAN）→ 2
    let work = tempfile::tempdir().unwrap();
    let out_dir = work.path().to_string_lossy().to_string();
    let output = coord(&pki_args(
        &agent_addr,
        &["issue", "--cn", "订单-api", "--out-dir", out_dir.as_str()],
    ));
    assert_eq!(
        output.status.code(),
        Some(2),
        "invalid CN must be a usage error (2); stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    // 连接失败 → 1（不是用法错误）
    let closed = find_free_port();
    let closed_addr = format!("127.0.0.1:{closed}");
    let output = coord(&pki_args(&closed_addr, &["ca"]));
    assert_eq!(
        output.status.code(),
        Some(1),
        "connection failure must exit 1; stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
