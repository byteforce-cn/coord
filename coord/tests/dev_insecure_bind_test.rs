// dev 模式非 loopback 绑定行为测试
//
// dev 模式强制关闭鉴权（root/root）。绑定非 loopback 地址必须显式传
// --allow-insecure 否则拒绝启动（拒绝发生在任何监听之前，端口保持空闲）；
// 显式放行后 Server/Agent 均可启动（ADR-0008：Raft 收敛 loopback、
// Agent 经 dev builder 放行）。

use std::process::Command;

fn find_free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// 读取子进程日志（超时/早退时诊断用）。
fn read_logs(out_path: &std::path::Path, err_path: &std::path::Path) -> String {
    format!(
        "--- stdout ---\n{}\n--- stderr ---\n{}",
        std::fs::read_to_string(out_path).unwrap_or_default(),
        std::fs::read_to_string(err_path).unwrap_or_default()
    )
}

/// 端口是否绑定在通配地址（0.0.0.0）且处于 LISTEN（Linux /proc 判据）。
/// 宿主上 127.0.0.1 也能连通 loopback 绑定，必须用监听地址区分——这是
/// 容器端口映射可达的前提（ADR-0008）。
#[cfg(target_os = "linux")]
fn is_bound_wildcard(port: u16) -> bool {
    let needle = format!("00000000:{:04X}", port);
    std::fs::read_to_string("/proc/net/tcp")
        .map(|content| {
            content.lines().any(|line| {
                let mut it = line.split_whitespace();
                it.next(); // sl
                it.next() == Some(needle.as_str()) && it.nth(1) == Some("0A") // rem, st
            })
        })
        .unwrap_or(false)
}

#[test]
fn test_dev_non_loopback_bind_refused_without_flag() {
    let bin = env!("CARGO_BIN_EXE_coord");
    let data_dir = tempfile::tempdir().unwrap();
    let grpc_port = find_free_port();
    let agent_port = find_free_port();

    let out = Command::new(bin)
        .arg("dev")
        .arg("--bind-addr")
        .arg("0.0.0.0")
        .arg("--grpc-port")
        .arg(grpc_port.to_string())
        .arg("--agent-port")
        .arg(agent_port.to_string())
        .arg("--data-dir")
        .arg(data_dir.path())
        .output()
        .expect("run coord dev");

    assert!(
        !out.status.success(),
        "dev on 0.0.0.0 without --allow-insecure must fail"
    );
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        combined.contains("--allow-insecure"),
        "error must mention --allow-insecure: {combined}"
    );
    // 拒绝发生在 bind 之前：gRPC 端口未被占用
    assert!(
        std::net::TcpListener::bind(("127.0.0.1", grpc_port)).is_ok(),
        "grpc port must remain free after refusal"
    );
}

#[test]
fn test_dev_loopback_bind_ok_without_flag() {
    // 默认 loopback 路径不受影响：dev（loopback）正常启动并监听 gRPC 端口。
    let bin = env!("CARGO_BIN_EXE_coord");
    let data_dir = tempfile::tempdir().unwrap();
    let grpc_port = find_free_port();
    let agent_port = find_free_port();

    let mut child = Command::new(bin)
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
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn coord dev");

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        if std::net::TcpStream::connect(("127.0.0.1", grpc_port)).is_ok() {
            break;
        }
        if std::time::Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("dev server did not bind gRPC port within 20s");
        }
        std::thread::sleep(std::time::Duration::from_millis(300));
    }
    let _ = child.kill();
    let _ = child.wait();
}

/// ADR-0008：非 loopback + --allow-insecure ⇒ Server（含 BFF/UI 端口）与 Agent
/// 均可启动（Raft 收敛 loopback；BFF/UI HTTP 随 bind；Agent 由 dev builder 放行）。
/// 容器场景正例锚点。
#[test]
fn test_dev_non_loopback_with_allow_insecure_starts() {
    let bin = env!("CARGO_BIN_EXE_coord");
    let data_dir = tempfile::tempdir().unwrap();
    let log_dir = tempfile::tempdir().unwrap();
    let grpc_port = find_free_port();
    let agent_port = find_free_port();

    let out_path = log_dir.path().join("stdout.log");
    let err_path = log_dir.path().join("stderr.log");
    let out_file = std::fs::File::create(&out_path).unwrap();
    let err_file = std::fs::File::create(&err_path).unwrap();

    let mut child = std::process::Command::new(bin)
        .arg("dev")
        .arg("--bind-addr")
        .arg("0.0.0.0")
        .arg("--allow-insecure")
        .arg("--grpc-port")
        .arg(grpc_port.to_string())
        .arg("--agent-port")
        .arg(agent_port.to_string())
        .arg("--data-dir")
        .arg(data_dir.path())
        .arg("--fresh")
        .stdout(std::process::Stdio::from(out_file))
        .stderr(std::process::Stdio::from(err_file))
        .spawn()
        .expect("spawn coord dev");

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let ui_port = grpc_port + 10;
    let mut server_ready = false;
    let mut agent_ready = false;
    let mut ui_ready = false;
    loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            let logs = read_logs(&out_path, &err_path);
            panic!("coord dev exited early ({status}) — logs:\n{logs}");
        }
        server_ready =
            server_ready || std::net::TcpStream::connect(("127.0.0.1", grpc_port)).is_ok();
        agent_ready =
            agent_ready || std::net::TcpStream::connect(("127.0.0.1", agent_port)).is_ok();
        ui_ready = ui_ready || std::net::TcpStream::connect(("127.0.0.1", ui_port)).is_ok();
        if server_ready && agent_ready && ui_ready {
            break;
        }
        if std::time::Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            let logs = read_logs(&out_path, &err_path);
            panic!(
                "dev server+agent+UI not ready within 60s (server_ready={server_ready}, \
                 agent_ready={agent_ready}, ui_ready={ui_ready}) — logs:\n{logs}"
            );
        }
        std::thread::sleep(std::time::Duration::from_millis(300));
    }

    // 容器可达前提（ADR-0008）：进程必须绑到通配地址而非 loopback
    // （宿主直跑时两者都能连通，只有监听地址能区分）。
    #[cfg(target_os = "linux")]
    {
        assert!(
            is_bound_wildcard(grpc_port),
            "server gRPC must bind 0.0.0.0 for container port mapping"
        );
        assert!(
            is_bound_wildcard(agent_port),
            "agent gRPC must bind 0.0.0.0 for container port mapping"
        );
        assert!(
            is_bound_wildcard(ui_port),
            "BFF/UI must bind 0.0.0.0 for container port mapping"
        );
    }

    let _ = child.kill();
    let _ = child.wait();
}
