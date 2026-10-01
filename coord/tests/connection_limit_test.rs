// 客户端口连接上限的进程级实测（B-CX-1）
//
// 口径：spawn 真实 `coord server`（`--config` 注入 `network.max_connections = 2`），
// 只开**裸 TCP 静默连接**（不发任何字节），证明四件事：
//   ① 上限内连接被接受并保持打开；
//   ② 第 3 条连接在 accept 后**立即断开**（读返回 EOF/RST），而不是排队挂起；
//   ③ 断开一条后配额归还，新连接重新可被接受；
//   ④ `/metrics` 的 `coord_grpc_connections_active` 始终 ≤ 上限，
//      `coord_grpc_connections_rejected_total` 随拒绝递增。
//
// 为什么用裸 TCP 而不是 gRPC 客户端：闸门在 accept 层，与协议无关；静默连接
// 正是 slowloris 形态——闸门要覆盖的就是这种「连上不发数据」的占用。
//
// 负控制：去掉 accept 层闸门（或让上限配置失效）⇒ ② 的 `probe_read` 会挂起
// （`panic!` 分支命中）⇒ 本测试必红。提交前实测过该变异。
//
// 轻量进程套件（秒级），不进 `#[ignore]`：与 auth_enforcement_test /
// plaintext_remote_failclosed_test 同列，随 workspace 测试在 CI 上跑。

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// 注入给被测 server 的连接上限（取小值以在秒级内实测边界）。
const MAX_CONNECTIONS: usize = 2;

fn find_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// kill + wait 守卫：断言 panic / 提前返回也不留孤儿进程。
struct ServerProc(Child);

impl Drop for ServerProc {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_server(
    cfg_path: &std::path::Path,
    data_dir: &std::path::Path,
    grpc_port: u16,
    raft_port: u16,
) -> ServerProc {
    let bin = env!("CARGO_BIN_EXE_coord");
    let log_file = std::fs::File::create(data_dir.join("server.log")).unwrap();
    let child = Command::new(bin)
        .arg("server")
        .arg("--id")
        .arg("1")
        .arg("--bootstrap")
        .arg("--addr")
        .arg(format!("127.0.0.1:{grpc_port}"))
        .arg("--raft-addr")
        .arg(format!("127.0.0.1:{raft_port}"))
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--config")
        .arg(cfg_path)
        .env("COORD_ROOT_PASSWORD", "test-root-password-123")
        .env("RUST_LOG", "coord=warn")
        .stdout(Stdio::from(log_file.try_clone().unwrap()))
        .stderr(Stdio::from(log_file))
        .spawn()
        .expect("spawn coord server");
    ServerProc(child)
}

/// GET http://127.0.0.1:{port}{path}（BFF 匿名健康路由；读到 EOF 或闲置即返回）。
async fn http_get(port: u16, path: &str) -> Result<String, String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .map_err(|e| e.to_string())?;
    let req = format!("GET {path} HTTP/1.0\r\nHost: localhost\r\n\r\n");
    stream
        .write_all(req.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    let mut body = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        match tokio::time::timeout(Duration::from_millis(500), stream.read(&mut chunk)).await {
            Ok(Ok(0)) | Err(_) => break,
            Ok(Ok(n)) => body.extend_from_slice(&chunk[..n]),
            Ok(Err(e)) => return Err(e.to_string()),
        }
    }
    Ok(String::from_utf8_lossy(&body).to_string())
}

/// 从 /metrics 文本取指标值（`name value` 行）。
fn parse_metric(body: &str, name: &str) -> Option<i64> {
    body.lines().find_map(|line| {
        line.strip_prefix(name)
            .and_then(|rest| rest.strip_prefix(' '))
            .and_then(|v| v.trim().parse().ok())
    })
}

/// 轮询 /metrics 直到指标满足条件；超时即 panic（附最近一次读数）。
async fn wait_metric(port: u16, name: &str, pred: impl Fn(i64) -> bool, what: &str) -> i64 {
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut last = None;
    loop {
        if let Ok(body) = http_get(port, "/metrics").await {
            if let Some(v) = parse_metric(&body, name) {
                if pred(v) {
                    return v;
                }
                last = Some(v);
            }
        }
        assert!(
            Instant::now() < deadline,
            "等待超时：{what}（{name} 最近读数：{last:?}）"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// 探测连接是否已被关闭：读到 EOF/RST ⇒ 已关闭；读到字节（如服务端 h2 SETTINGS）
/// 或超时挂起 ⇒ 仍打开。
async fn is_closed(conn: &mut TcpStream, wait: Duration) -> bool {
    let mut buf = [0u8; 1];
    match tokio::time::timeout(wait, conn.read(&mut buf)).await {
        Err(_) => false,
        Ok(Ok(0)) | Ok(Err(_)) => true,
        Ok(Ok(_)) => false,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_port_connection_count_is_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let grpc_port = find_free_port();
    let raft_port = find_free_port();
    let http_port = grpc_port + 10;

    let cfg_path = dir.path().join("server.toml");
    std::fs::write(
        &cfg_path,
        format!("[network]\nmax_connections = {MAX_CONNECTIONS}\n"),
    )
    .unwrap();

    let _server = spawn_server(&cfg_path, dir.path(), grpc_port, raft_port);

    // 就绪：BFF /healthz（匿名；端口 = grpc + 10）
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(resp) = http_get(http_port, "/healthz").await {
            if resp.contains("200") {
                break;
            }
        }
        assert!(Instant::now() < deadline, "server 未在 30s 内就绪");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // ① 上限内两条静默连接：被接受并保持打开
    let mut c1 = TcpStream::connect(("127.0.0.1", grpc_port)).await.unwrap();
    let mut c2 = TcpStream::connect(("127.0.0.1", grpc_port)).await.unwrap();
    let active = wait_metric(
        http_port,
        "coord_grpc_connections_active",
        |v| v == MAX_CONNECTIONS as i64,
        "上限内连接被接受",
    )
    .await;
    assert!(active <= MAX_CONNECTIONS as i64, "活跃数不得超上限");
    assert!(
        !is_closed(&mut c1, Duration::from_millis(300)).await,
        "上限内连接不得被闸门关闭"
    );
    assert!(
        !is_closed(&mut c2, Duration::from_millis(300)).await,
        "上限内连接不得被闸门关闭"
    );

    // ② 第 3 条：accept 后立即断开（排队/接受会令 read 挂起或读到 h2 数据）
    let mut c3 = TcpStream::connect(("127.0.0.1", grpc_port)).await.unwrap();
    assert!(
        is_closed(&mut c3, Duration::from_secs(3)).await,
        "超限连接必须被立即断开（不排队）"
    );

    // ④ 拒绝计数递增；活跃数维持在上限（不是 3）
    wait_metric(
        http_port,
        "coord_grpc_connections_rejected_total",
        |v| v >= 1,
        "超限拒绝被计数",
    )
    .await;
    let active = wait_metric(
        http_port,
        "coord_grpc_connections_active",
        |v| v == MAX_CONNECTIONS as i64,
        "活跃数维持在上限",
    )
    .await;
    assert!(
        active <= MAX_CONNECTIONS as i64,
        "活跃数必须 <= 上限，实得 {active}"
    );

    // ③ 断开一条 ⇒ 配额归还 ⇒ 新连接可被接受；既有连接不被挤断
    drop(c1);
    wait_metric(
        http_port,
        "coord_grpc_connections_active",
        |v| v == MAX_CONNECTIONS as i64 - 1,
        "配额随断开归还",
    )
    .await;
    let mut c4 = TcpStream::connect(("127.0.0.1", grpc_port)).await.unwrap();
    wait_metric(
        http_port,
        "coord_grpc_connections_active",
        |v| v == MAX_CONNECTIONS as i64,
        "新连接占用归还的配额",
    )
    .await;
    assert!(
        !is_closed(&mut c4, Duration::from_millis(300)).await,
        "配额归还后新连接必须保持打开"
    );
    assert!(
        !is_closed(&mut c2, Duration::from_millis(300)).await,
        "上限内既有连接不得被新连接挤断"
    );
}
