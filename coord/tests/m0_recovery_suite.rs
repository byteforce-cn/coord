// M0 验收套件（L3 进程级，真实二进制 + kill -9）：
// - M0-3：kill -9 重启后 applied 恢复、revision 不重复、数据不丢
// - M0-5：写入 >5000 条触发自动快照，purge 后重启可正常启动并服务
//
// 对应文档：`docs/production/12-test-strategy.md` §4.1（M0 浸泡标准）、
// `docs/production/15-milestone-task-breakdown.md` M0-7。
// 迭代次数可用环境变量 M0_KILL9_ITERATIONS 调节（默认 3；里程碑出口按 10/节点执行）。

use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use coord_client::config::Config;
use coord_client::Client;

fn find_free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// 找一对（grpc, grpc+10）都空闲的端口（HTTP BFF 端口 = grpc 端口 + 10）
fn find_port_pair() -> u16 {
    loop {
        let p = find_free_port();
        if TcpListener::bind(("127.0.0.1", p + 10)).is_ok() {
            return p;
        }
    }
}

/// 以 server 模式启动真实 `coord` 二进制（单节点 bootstrap）
fn spawn_server(data_dir: &std::path::Path, grpc_port: u16, raft_port: u16) -> Child {
    let bin = env!("CARGO_BIN_EXE_coord");
    Command::new(bin)
        .arg("server")
        .arg("--id")
        .arg("1")
        .arg("--bootstrap")
        .arg("--auth-enabled")
        .arg("false")
        .arg("--addr")
        .arg(format!("127.0.0.1:{grpc_port}"))
        .arg("--raft-addr")
        .arg(format!("127.0.0.1:{raft_port}"))
        .arg("--data-dir")
        .arg(data_dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn coord server")
}

/// 等待服务就绪：只读轮询 status（写入探针会推进 revision，破坏 revision 断言）
async fn wait_ready(addr: &str, timeout: Duration) -> Client {
    let deadline = Instant::now() + timeout;
    loop {
        match Client::connect_direct(Config::new(vec![addr.to_string()])).await {
            Ok(client) => {
                if client.maintenance().status().await.is_ok() {
                    return client;
                }
            }
            Err(_) => {}
        }
        assert!(
            Instant::now() < deadline,
            "server {addr} did not become ready within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// M0-3/M0-7：kill -9 重启循环 —— revision 不重复、数据不丢、重启后从 applied+1 续
#[tokio::test]
async fn m0_kill9_restart_revision_stable() {
    let iterations: u64 = std::env::var("M0_KILL9_ITERATIONS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);

    let tmp = tempfile::tempdir().unwrap();
    let data_dir: PathBuf = tmp.path().to_path_buf();
    let grpc_port = find_port_pair();
    let raft_port = find_free_port();
    let addr = format!("127.0.0.1:{grpc_port}");

    let mut expected_rev = 0u64;

    for round in 0..iterations {
        let mut child = spawn_server(&data_dir, grpc_port, raft_port);
        let client = wait_ready(&addr, Duration::from_secs(90)).await;

        // 写入 3 条并记录 revision
        for i in 0..3u64 {
            let key = format!("/m0/kill9/r{round}/k{i}");
            let rev = client
                .kv()
                .put(key.as_bytes(), format!("v{round}-{i}").as_bytes())
                .await
                .unwrap();
            if round == 0 && i == 0 {
                expected_rev = rev;
            } else {
                assert_eq!(
                    rev,
                    expected_rev + 1,
                    "revision must be monotonic +1 (round {round}, key {i})"
                );
                expected_rev = rev;
            }
        }

        // kill -9（不经优雅停机）
        child.kill().expect("kill -9");
        let _ = child.wait();

        // 重启并校验：数据保留、revision 不变（重启不得重新分配 revision）
        let mut child = spawn_server(&data_dir, grpc_port, raft_port);
        let client2 = wait_ready(&addr, Duration::from_secs(90)).await;
        let status = client2.maintenance().status().await.expect("status");
        assert_eq!(
            status.revision as u64, expected_rev,
            "restart must preserve revision (round {round})"
        );

        // 首轮写入的 key 仍可读
        let kvs = client2
            .kv()
            .range(b"/m0/kill9/r0/k0", b"", 0, 0)
            .await
            .expect("range");
        assert_eq!(kvs.len(), 1);
        // 新写入从 expected_rev+1 续
        let rev = client2
            .kv()
            .put(b"/m0/kill9/after-restart", format!("r{round}").as_bytes())
            .await
            .unwrap();
        assert_eq!(
            rev,
            expected_rev + 1,
            "post-restart write must continue from applied+1"
        );
        expected_rev = rev;

        child.kill().expect("kill -9");
        let _ = child.wait();
    }
}

/// M0-5：写入 >5000 条触发自动快照 + purge，kill -9 后重启可正常启动并服务
#[tokio::test]
async fn m0_snapshot_purge_then_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir: PathBuf = tmp.path().to_path_buf();
    let grpc_port = find_port_pair();
    let raft_port = find_free_port();
    let addr = format!("127.0.0.1:{grpc_port}");

    let snap_dir = data_dir.join("snapshots");

    let mut child = spawn_server(&data_dir, grpc_port, raft_port);
    let client = wait_ready(&addr, Duration::from_secs(90)).await;

    // 写入 5100 条（openraft 默认 snapshot_policy = LogsSinceLast(5000)）
    let n = 5100u64;
    let mut last_rev = 0;
    for i in 0..n {
        let key = format!("/m0/snap/{i:05}");
        last_rev = client
            .kv()
            .put(key.as_bytes(), format!("v{i}").as_bytes())
            .await
            .unwrap();
    }

    // 等待快照落盘（build_snapshot → 临时文件 → fsync → rename → .snap）
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let found = std::fs::read_dir(&snap_dir)
            .map(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .any(|e| e.path().extension().map(|x| x == "snap").unwrap_or(false))
            })
            .unwrap_or(false);
        if found {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "no snapshot file appeared in {} within 120s after {n} writes",
            snap_dir.display()
        );
        // 继续写入少量数据确保 apply 推进、快照触发
        let key = format!(
            "/m0/snap-extra/{i}",
            i = Instant::now().elapsed().as_millis()
        );
        let _ = client.kv().put(key.as_bytes(), b"x").await;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // 等待 purge（快照构建成功后 openraft 驱动 purge；给 15s 余量）
    tokio::time::sleep(Duration::from_secs(15)).await;

    // kill -9 后重启：快照加载 + purge 前置守卫放行 → 必须正常启动并服务
    child.kill().expect("kill -9");
    let _ = child.wait();

    let mut child = spawn_server(&data_dir, grpc_port, raft_port);
    let client2 = wait_ready(&addr, Duration::from_secs(120)).await;

    let status = client2
        .maintenance()
        .status()
        .await
        .expect("status after snapshot+purge restart");
    assert!(
        status.revision as u64 >= last_rev,
        "revision must be >= last write after restart"
    );

    // 抽样验证数据完整（快照内的 KV）
    let kvs = client2
        .kv()
        .range(b"/m0/snap/00000", b"", 0, 0)
        .await
        .expect("range k0");
    assert_eq!(kvs.len(), 1);
    assert_eq!(kvs[0].1, b"v0".to_vec());
    let kvs = client2
        .kv()
        .range(b"/m0/snap/05099", b"", 0, 0)
        .await
        .expect("range k5099");
    assert_eq!(kvs.len(), 1);

    // 重启后可继续写入
    let rev = client2
        .kv()
        .put(b"/m0/snap/post-restart", b"ok")
        .await
        .unwrap();
    assert!(rev > last_rev, "post-restart write must continue");

    child.kill().expect("kill -9");
    let _ = child.wait();
}
