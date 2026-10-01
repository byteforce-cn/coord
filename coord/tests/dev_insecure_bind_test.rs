// dev 模式非 loopback 绑定防护测试
//
// dev 模式强制关闭鉴权（root/root 默认凭据），绑定非 loopback 地址必须显式
// 传 --allow-insecure，否则拒绝启动。拒绝发生在任何监听之前（快速失败，
// 端口保持空闲）。

use std::process::Command;

fn find_free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
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
