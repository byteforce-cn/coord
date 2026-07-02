// coord-agent: HTTP Health/Metrics 端点
//
// 提供轻量级 HTTP 端点用于 K8s 探活和 Prometheus 指标采集。
// 使用原生 tokio TcpListener，与 coord-server health 模块对等。
//
// 端点：
// - /health             → 进程存活检查（200 OK）
// - /health?ready=true  → 就绪检查（已连接 Server 集群则 200）
// - /metrics            → Prometheus 文本格式指标
//
// 参见 docs/client-agent-architecture.md §4.6。

use std::collections::HashMap;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::metrics::AgentMetrics;

// ──── 公共 API ────

/// 启动轻量级 HTTP Health/Metrics 端点
///
/// 监听指定地址，处理 /health 和 /metrics 请求。
///
/// - `addr`: 监听地址（如 "127.0.0.1:19528"）
/// - `metrics`: AgentMetrics 实例
/// - `ready`: 初始就绪状态（通常 false，连接 Server 后更新）
///
/// 返回 JoinHandle，可 abort 以优雅关闭。
pub fn start_health_server(
    addr: &str,
    metrics: AgentMetrics,
    ready: bool,
) -> tokio::task::JoinHandle<()> {
    let metrics = Arc::new(metrics);
    let ready = Arc::new(std::sync::atomic::AtomicBool::new(ready));
    let addr = addr.to_string();

    tokio::spawn(async move {
        let listener = match TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!("Health server failed to bind {}: {e}", addr);
                return;
            }
        };
        tracing::info!("Agent health/metrics HTTP server listening on http://{addr}");

        loop {
            match listener.accept().await {
                Ok((mut socket, _)) => {
                    let metrics = Arc::clone(&metrics);
                    let ready = Arc::clone(&ready);
                    tokio::spawn(async move {
                        let mut buf = [0u8; 4096];
                        let n = match socket.read(&mut buf).await {
                            Ok(n) if n > 0 => n,
                            _ => return,
                        };

                        let request = String::from_utf8_lossy(&buf[..n]);
                        let first_line = request.lines().next().unwrap_or("");
                        let parts: Vec<&str> = first_line.split_whitespace().collect();
                        let raw_path = parts.get(1).unwrap_or(&"/");

                        let (path, query_params) = parse_path_and_query(raw_path);

                        let (status, content_type, body) = match path.as_str() {
                            "/health" => {
                                let is_ready = query_params.get("ready").map(|v| v.as_str()) == Some("true");
                                if is_ready {
                                    handle_health_ready(&ready)
                                } else {
                                    ("200 OK", "application/json", r#"{"status":"SERVING"}"#.to_string())
                                }
                            }
                            "/metrics" => {
                                let body = metrics.render_prometheus_text();
                                ("200 OK", "text/plain; version=0.0.4", body)
                            }
                            _ => {
                                ("404 Not Found", "text/plain", "Not Found".to_string())
                            }
                        };

                        let response = format!(
                            "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\n\r\n{}",
                            status,
                            content_type,
                            body.len(),
                            body
                        );

                        let _ = socket.write_all(response.as_bytes()).await;
                    });
                }
                Err(e) => {
                    tracing::error!("Health server accept error: {e}");
                }
            }
        }
    })
}

// ──── 查询参数解析 ────

fn parse_path_and_query(raw: &str) -> (String, HashMap<String, String>) {
    let mut params = HashMap::new();
    if let Some((path, query_str)) = raw.split_once('?') {
        for pair in query_str.split('&') {
            if let Some((k, v)) = pair.split_once('=') {
                params.insert(k.to_string(), v.to_string());
            }
        }
        (path.to_string(), params)
    } else {
        (raw.to_string(), params)
    }
}

// ──── Health Handlers ────

fn handle_health_ready(
    ready: &std::sync::atomic::AtomicBool,
) -> (&'static str, &'static str, String) {
    use std::sync::atomic::Ordering;
    if ready.load(Ordering::Relaxed) {
        ("200 OK", "application/json", r#"{"status":"READY"}"#.to_string())
    } else {
        ("503 Service Unavailable", "application/json", r#"{"status":"NOT_READY"}"#.to_string())
    }
}
