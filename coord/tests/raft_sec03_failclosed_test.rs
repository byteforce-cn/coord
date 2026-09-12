// A4 / R-SEC-03：raft 端口认证 fail-closed 验收
//
// 复核结论（2026-09-12）：`coord/src/main.rs` 在 raft_addr 非 loopback 且
// 既无 mTLS 也无 `security.raft_shared_secret` 时**拒绝启动**。这是**已有**行为，
// 但此前没有可证伪的自动化测试固化（`dev_insecure_bind_test.rs` 测的是 dev 模式
// 的 `--allow-insecure`，与本约束无关）。
//
// 本文件补齐两条断言：
// 1. 非 loopback raft + 无密钥 → 启动失败，错误信息含 R-SEC-03；
// 2. loopback raft + 无密钥 → 明确豁免（dev/test 路径），不因此拒绝。

use std::process::Command;

fn find_free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn combined_output(out: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

#[test]
fn test_non_loopback_raft_without_secret_refuses_start() {
    let bin = env!("CARGO_BIN_EXE_coord");
    let data_dir = tempfile::tempdir().unwrap();
    let grpc_port = find_free_port();
    let raft_port = find_free_port();

    let out = Command::new(bin)
        .arg("server")
        .arg("--id")
        .arg("1")
        .arg("--addr")
        .arg(format!("127.0.0.1:{grpc_port}"))
        // 非 loopback raft 通告地址 + 无 mTLS + 无共享密钥 → 必须 fail-closed
        .arg("--raft-addr")
        .arg(format!("0.0.0.0:{raft_port}"))
        .arg("--data-dir")
        .arg(data_dir.path())
        .arg("--bootstrap")
        .output()
        .expect("run coord server");

    assert!(
        !out.status.success(),
        "non-loopback raft without mTLS/shared-secret must refuse to start"
    );
    let combined = combined_output(&out);
    assert!(
        combined.contains("R-SEC-03"),
        "error must cite R-SEC-03 fail-closed: {combined}"
    );
    // 拒绝发生在监听之前：raft 端口保持空闲
    assert!(
        std::net::TcpListener::bind(("127.0.0.1", raft_port)).is_ok(),
        "raft port must remain free after refusal"
    );
}

#[test]
fn test_loopback_raft_without_secret_is_dev_exempt() {
    let bin = env!("CARGO_BIN_EXE_coord");
    let data_dir = tempfile::tempdir().unwrap();
    let grpc_port = find_free_port();
    let raft_port = find_free_port();

    let mut child = Command::new(bin)
        .arg("server")
        .arg("--id")
        .arg("1")
        .arg("--addr")
        .arg(format!("127.0.0.1:{grpc_port}"))
        .arg("--raft-addr")
        .arg(format!("127.0.0.1:{raft_port}"))
        .arg("--data-dir")
        .arg(data_dir.path())
        .arg("--bootstrap")
        .env("RUST_LOG", "coord=warn")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn coord server");

    // loopback 豁免：应当开始监听 raft 端口（不因 R-SEC-03 退出）。
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(25);
    let mut listening = false;
    while std::time::Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("try_wait") {
            let _ = child.wait();
            panic!("loopback raft must not be refused by R-SEC-03 (exited: {status})");
        }
        if std::net::TcpStream::connect(("127.0.0.1", raft_port)).is_ok() {
            listening = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(300));
    }
    let _ = child.kill();
    let _ = child.wait();
    assert!(
        listening,
        "loopback raft server did not start listening within 25s"
    );
}
