// TDD: Agent HTTP Health/Metrics 测试 (RED)
//
// 验证 Agent 可观测性端点：
// - /health         → 200 OK（进程存活）
// - /health?ready=true → 200 OK（已连接 Server 集群）
// - /metrics        → Prometheus 文本格式
//
// RED 阶段：health/metrics 模块尚不存在，此测试预期编译失败。

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use coord_agent::health::{start_health_server, start_health_server_with_limits, HealthLimits};
use coord_agent::metrics::AgentMetrics;

/// /health 端点返回 200 OK
#[tokio::test]
async fn test_health_endpoint_live() {
    let port = find_port();
    let metrics = AgentMetrics::new();

    // 启动 health server
    let addr = format!("127.0.0.1:{}", port);
    let handle = start_health_server(&addr, metrics, Arc::new(AtomicBool::new(false)));

    // 等待 server 启动
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 发起 HTTP GET /health
    let response = http_get(&addr, "/health").await;
    assert!(
        response.contains("200 OK"),
        "expected 200 OK, got: {response}"
    );
    assert!(
        response.contains("SERVING"),
        "expected SERVING, got: {response}"
    );

    handle.abort();
}

/// /health?ready=true 就绪检查
#[tokio::test]
async fn test_health_endpoint_ready() {
    let port = find_port();
    let metrics = AgentMetrics::new();

    let addr = format!("127.0.0.1:{}", port);
    let handle = start_health_server(&addr, metrics, Arc::new(AtomicBool::new(false)));

    tokio::time::sleep(Duration::from_millis(100)).await;

    // 未连接 Server 时返回 503
    let response = http_get(&addr, "/health?ready=true").await;
    assert!(
        response.contains("503"),
        "expected 503 when not ready, got: {response}"
    );

    handle.abort();
}

/// /metrics 端点返回 Prometheus 格式
#[tokio::test]
async fn test_metrics_endpoint() {
    let port = find_port();
    let metrics = AgentMetrics::new();

    let addr = format!("127.0.0.1:{}", port);
    let handle = start_health_server(&addr, metrics, Arc::new(AtomicBool::new(true)));

    tokio::time::sleep(Duration::from_millis(100)).await;

    let response = http_get(&addr, "/metrics").await;
    assert!(
        response.contains("200 OK"),
        "expected 200 OK, got: {response}"
    );
    // Prometheus 格式特征
    assert!(
        response.contains("coord_agent"),
        "expected coord_agent metric, got: {response}"
    );

    handle.abort();
}

/// 慢连接（不发任何字节）必须被读超时断开。
///
/// 负控制：移除 health.rs 的读超时（恢复为无超时的一次 read）⇒ 客户端读不到
/// EOF，外层 timeout 令本测试必红。
#[tokio::test]
async fn test_health_slow_client_disconnected_by_read_timeout() {
    let port = find_port();
    let addr = format!("127.0.0.1:{}", port);
    let limits = HealthLimits {
        max_connections: 4,
        read_timeout: Duration::from_millis(200),
        write_timeout: Duration::from_millis(500),
    };
    let handle = start_health_server_with_limits(
        &addr,
        AgentMetrics::new(),
        Arc::new(AtomicBool::new(false)),
        limits,
    );
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut stream = TcpStream::connect(&addr).await.unwrap();
    // 故意不发任何字节
    let mut buf = [0u8; 8];
    let read = tokio::time::timeout(Duration::from_secs(3), stream.read(&mut buf))
        .await
        .expect("slow client was not disconnected: health read timeout is not enforced");
    assert!(
        matches!(read, Ok(0) | Err(_)),
        "expected EOF/reset after read timeout, got {read:?}"
    );

    handle.abort();
}

/// 达到并发上限后，新连接必须被立即关闭而非被服务。
///
/// 负控制：移除 health.rs 的连接配额（恢复为无 Semaphore 的 accept 循环）⇒
/// 第三个连接会读到 HTTP 响应（Ok(n) 且 n > 0），本测试必红。
#[tokio::test]
async fn test_health_connection_cap_closes_excess() {
    let port = find_port();
    let addr = format!("127.0.0.1:{}", port);
    // read_timeout 拉长：前两个连接在测试窗口内稳定占用配额
    let limits = HealthLimits {
        max_connections: 2,
        read_timeout: Duration::from_secs(30),
        write_timeout: Duration::from_millis(500),
    };
    let handle = start_health_server_with_limits(
        &addr,
        AgentMetrics::new(),
        Arc::new(AtomicBool::new(false)),
        limits,
    );
    tokio::time::sleep(Duration::from_millis(100)).await;

    let _held1 = TcpStream::connect(&addr).await.unwrap();
    let _held2 = TcpStream::connect(&addr).await.unwrap();
    // 等服务端 accept 前两个连接并占满配额
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut excess = TcpStream::connect(&addr).await.unwrap();
    let mut buf = [0u8; 16];
    let read = tokio::time::timeout(Duration::from_secs(2), excess.read(&mut buf))
        .await
        .expect("excess connection was neither served nor closed within 2s");
    assert!(
        matches!(read, Ok(0) | Err(_)),
        "excess connection must be closed without a response, got {read:?}"
    );

    handle.abort();
}

// ──── Helpers ────

fn find_port() -> u16 {
    use std::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

async fn http_get(host: &str, path: &str) -> String {
    let mut stream = TcpStream::connect(host).await.unwrap();
    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        path, host
    );
    stream.write_all(request.as_bytes()).await.unwrap();

    let mut response = String::new();
    let _ = stream.read_to_string(&mut response).await;
    response
}
