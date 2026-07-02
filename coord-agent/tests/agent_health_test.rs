// TDD: Agent HTTP Health/Metrics 测试 (Phase C3 — RED)
//
// 验证 Agent 可观测性端点：
// - /health         → 200 OK（进程存活）
// - /health?ready=true → 200 OK（已连接 Server 集群）
// - /metrics        → Prometheus 文本格式
//
// RED 阶段：health/metrics 模块尚不存在，此测试预期编译失败。

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use coord_agent::health::start_health_server;
use coord_agent::metrics::AgentMetrics;

/// C3.1: /health 端点返回 200 OK
#[tokio::test]
async fn test_health_endpoint_live() {
    let port = find_port();
    let metrics = AgentMetrics::new();

    // 启动 health server
    let addr = format!("127.0.0.1:{}", port);
    let handle = start_health_server(&addr, metrics, false);

    // 等待 server 启动
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 发起 HTTP GET /health
    let response = http_get(&addr, "/health").await;
    assert!(response.contains("200 OK"), "expected 200 OK, got: {response}");
    assert!(response.contains("SERVING"), "expected SERVING, got: {response}");

    handle.abort();
}

/// C3.2: /health?ready=true 就绪检查
#[tokio::test]
async fn test_health_endpoint_ready() {
    let port = find_port();
    let metrics = AgentMetrics::new();

    let addr = format!("127.0.0.1:{}", port);
    let handle = start_health_server(&addr, metrics, false);

    tokio::time::sleep(Duration::from_millis(100)).await;

    // 未连接 Server 时返回 503
    let response = http_get(&addr, "/health?ready=true").await;
    assert!(response.contains("503"), "expected 503 when not ready, got: {response}");

    handle.abort();
}

/// C3.3: /metrics 端点返回 Prometheus 格式
#[tokio::test]
async fn test_metrics_endpoint() {
    let port = find_port();
    let metrics = AgentMetrics::new();

    let addr = format!("127.0.0.1:{}", port);
    let handle = start_health_server(&addr, metrics, true);

    tokio::time::sleep(Duration::from_millis(100)).await;

    let response = http_get(&addr, "/metrics").await;
    assert!(response.contains("200 OK"), "expected 200 OK, got: {response}");
    // Prometheus 格式特征
    assert!(response.contains("coord_agent"), "expected coord_agent metric, got: {response}");

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
