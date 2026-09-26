// W4-1（R-SEC-04）：gRPC 侧 TLS fail-closed 进程级验收
//
// 规则（2026-09-26 落地，`coord/src/main.rs` 的 6.5 节）：`auth_enabled = true`
// 且 `grpc_addr` 绑定非 loopback 且未配置 gRPC TLS（`security.tls_cert/tls_key`）
// ⇒ **拒绝启动**；唯一逃生阀是显式 `security.allow_plaintext_remote = true`
// （默认 false，dev/test 专用）。修前该形态会静默明文启动（README
// 「Not fail-closed」的第三条路径）。
//
// 断言（正反双向，全部进程级、spawn 真实二进制）：
// 1. 反例：非 loopback gRPC + 鉴权 + 无 TLS + 无逃生阀 ⇒ 拒绝启动，错误信息
//    必须同时含 `R-SEC-04` 与逃生阀名 `allow_plaintext_remote`；
// 2. 正例：同配置 + `allow_plaintext_remote = true` ⇒ 启动到 serve 阶段，
//    且启动日志以 WARN 明示"显式允许明文远端"；
// 3. 正例：同配置 + CA 签发的 `tls_cert/tls_key/tls_ca`（raft TLS 启用时强制 mTLS）
//    ⇒ 启动到 serve 阶段；
// 4. 豁免：loopback gRPC + 鉴权 + 无 TLS ⇒ 不因本规则拒绝（不误伤 dev/test）。
//
// 注意：CLI 全局参数带默认值会覆盖配置文件同名字段（--addr/--raft-addr/
// --data-dir），必须显式传参（与 cli_tls_test / m0_recovery_suite 同口径）。
// 就绪信号用日志标记 `started: node_id=`：gRPC listener 在启动第 1.5 步就预绑定，
// 仅凭 TCP 可连接并不能说明启动成功。

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn find_free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// 写最小可启动配置；`extra_security` 追加到 `[security]` 段（逃生阀 / TLS 路径）。
fn write_config(
    dir: &Path,
    grpc_addr: &str,
    raft_port: u16,
    data_dir: &Path,
    extra_security: &str,
) -> PathBuf {
    let config = format!(
        r#"
[node]
id = 1

[network]
grpc_addr = "{grpc_addr}"
raft_addr = "127.0.0.1:{raft_port}"

[cluster]
cluster_name = "plaintext-remote-test"
bootstrap = true

[storage]
data_dir = "{data_dir}"

[security]
auth_enabled = true
{extra_security}
"#,
        data_dir = data_dir.display()
    );
    let path = dir.join("server.toml");
    std::fs::write(&path, config).unwrap();
    path
}

fn spawn_server(config: &Path, addr: &str, raft_port: u16, data_dir: &Path, log: &Path) -> Child {
    let bin = env!("CARGO_BIN_EXE_coord");
    let log_file = std::fs::File::create(log).unwrap();
    Command::new(bin)
        .arg("server")
        .arg("--config")
        .arg(config)
        .arg("--addr")
        .arg(addr)
        .arg("--raft-addr")
        .arg(format!("127.0.0.1:{raft_port}"))
        .arg("--data-dir")
        .arg(data_dir)
        .env("COORD_ROOT_PASSWORD", "plaintext-remote-test-root-password")
        .env("RUST_LOG", "coord=info")
        .stdout(Stdio::from(log_file.try_clone().unwrap()))
        .stderr(Stdio::from(log_file))
        .spawn()
        .unwrap_or_else(|e| panic!("spawn coord server on {addr}: {e}"))
}

/// 等待进程退出（超时则杀进程并 panic），返回（退出码, 日志文本）。
fn wait_exit(mut child: Child, log: &Path, timeout: Duration) -> (Option<i32>, String) {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            let out = std::fs::read_to_string(log).unwrap_or_default();
            return (status.code(), out);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "server did not exit within {timeout:?} (log: {})",
                log.display()
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// 等待启动完成标记；若进程提前退出则 panic（附日志）。
///
/// 明文分支的完成日志是 `started: node_id=`；TLS 分支没有对应行，
/// 以 `TLS enabled for gRPC server`（gRPC TLS 配置成功、即将进入 serve）
/// 作为等价就绪信号。
fn wait_started(mut child: Child, log: &Path, timeout: Duration) -> Child {
    const MARKERS: [&str; 2] = ["started: node_id=", "TLS enabled for gRPC server"];
    let deadline = Instant::now() + timeout;
    loop {
        let out = std::fs::read_to_string(log).unwrap_or_default();
        if MARKERS.iter().any(|m| out.contains(m)) {
            return child;
        }
        if let Some(status) = child.try_wait().expect("try_wait") {
            panic!("server exited early ({status}); log:\n{out}");
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("server did not reach serve phase within {timeout:?}; log:\n{out}");
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn kill_child(mut child: Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// 生成测试 CA 并签发服务端证书（rcgen；与 cli_tls_test 同源用法）。
/// 返回（server.crt, server.key, ca.crt）。
/// 注意：raft TLS 启用时 coord 强制要求 `tls_ca`（raft 端口 mTLS fail-closed），
/// 因此本函数必须同时产出 CA 文件。
fn write_ca_and_server_cert(dir: &Path) -> (PathBuf, PathBuf, PathBuf) {
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "coord-test-ca");
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();

    let server_key = rcgen::KeyPair::generate().unwrap();
    let mut server_params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    server_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "coord-plaintext-remote-test");
    let issuer = rcgen::Issuer::from_params(&ca_params, &ca_key);
    let server_cert = server_params.signed_by(&server_key, &issuer).unwrap();

    let cert_path = dir.join("server.crt");
    let key_path = dir.join("server.key");
    let ca_path = dir.join("ca.crt");
    std::fs::write(&cert_path, server_cert.pem()).unwrap();
    std::fs::write(&key_path, server_key.serialize_pem()).unwrap();
    std::fs::write(&ca_path, ca_cert.pem()).unwrap();
    (cert_path, key_path, ca_path)
}

#[test]
fn test_non_loopback_grpc_without_tls_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let grpc_port = find_free_port();
    let raft_port = find_free_port();
    let addr = format!("0.0.0.0:{grpc_port}");
    let cfg = write_config(dir.path(), &addr, raft_port, &data_dir, "");
    let log = dir.path().join("server.log");
    let child = spawn_server(&cfg, &addr, raft_port, &data_dir, &log);

    let (code, out) = wait_exit(child, &log, Duration::from_secs(60));
    assert!(
        matches!(code, Some(c) if c != 0),
        "non-loopback gRPC without TLS must refuse to start (exit code {code:?}); log:\n{out}"
    );
    assert!(
        out.contains("R-SEC-04"),
        "refusal must cite R-SEC-04 fail-closed; log:\n{out}"
    );
    assert!(
        out.contains("allow_plaintext_remote"),
        "refusal must name the explicit escape hatch; log:\n{out}"
    );
    // 进程已退出：gRPC 端口不残留监听
    assert!(
        std::net::TcpListener::bind(("0.0.0.0", grpc_port)).is_ok(),
        "grpc port must not remain bound after refusal"
    );
}

#[test]
fn test_non_loopback_grpc_with_explicit_escape_hatch_starts() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let grpc_port = find_free_port();
    let raft_port = find_free_port();
    let addr = format!("0.0.0.0:{grpc_port}");
    let cfg = write_config(
        dir.path(),
        &addr,
        raft_port,
        &data_dir,
        "allow_plaintext_remote = true\n",
    );
    let log = dir.path().join("server.log");
    let child = spawn_server(&cfg, &addr, raft_port, &data_dir, &log);
    let child = wait_started(child, &log, Duration::from_secs(60));
    kill_child(child);

    let out = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(
        out.contains("allow_plaintext_remote"),
        "startup log must WARN that plaintext remote was explicitly allowed; log:\n{out}"
    );
}

#[test]
fn test_non_loopback_grpc_with_tls_starts() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let grpc_port = find_free_port();
    let raft_port = find_free_port();
    let addr = format!("0.0.0.0:{grpc_port}");
    let (cert, key, ca) = write_ca_and_server_cert(dir.path());
    let extra = format!(
        "tls_cert = \"{}\"\ntls_key = \"{}\"\ntls_ca = \"{}\"\n",
        cert.display(),
        key.display(),
        ca.display()
    );
    let cfg = write_config(dir.path(), &addr, raft_port, &data_dir, &extra);
    let log = dir.path().join("server.log");
    let child = spawn_server(&cfg, &addr, raft_port, &data_dir, &log);
    let child = wait_started(child, &log, Duration::from_secs(60));
    kill_child(child);
}

#[test]
fn test_loopback_grpc_without_tls_is_exempt() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let grpc_port = find_free_port();
    let raft_port = find_free_port();
    let addr = format!("127.0.0.1:{grpc_port}");
    let cfg = write_config(dir.path(), &addr, raft_port, &data_dir, "");
    let log = dir.path().join("server.log");
    let child = spawn_server(&cfg, &addr, raft_port, &data_dir, &log);
    let child = wait_started(child, &log, Duration::from_secs(60));
    kill_child(child);
}
