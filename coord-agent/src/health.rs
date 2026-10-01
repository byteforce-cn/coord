// coord-agent: HTTP Health/Metrics 端点
//
// 提供轻量级 HTTP 端点用于 K8s 探活和 Prometheus 指标采集。
// 使用原生 tokio TcpListener，与 coord-server health 模块对等。
//
// 端点：
// - /health             → 进程存活检查（200 OK）
// - /health?ready=true  → 就绪检查（已连接 Server 集群则 200）
// - /metrics            → Prometheus 文本格式指标

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;

use crate::metrics::AgentMetrics;

// ──── 端点资源上限 ────

/// health 端点默认最大并发连接数。
///
/// 每个连接占用一个 fd 与一个任务，且该端点**不带鉴权**：
/// 无上限时慢连接（slowloris）可把两者耗尽。
const DEFAULT_MAX_HEALTH_CONNECTIONS: usize = 64;

/// 默认请求读超时：慢客户端超过此时限仍未发出请求即被断开，
/// 使 fd/任务占用是**有界时长**而非无限。
const DEFAULT_HEALTH_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// 默认响应写超时：对端不读时任务与配额不得无限挂起。
const DEFAULT_HEALTH_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// health 监听器的资源上限（连接数 + 读/写超时）。
///
/// 生产入口使用 [`HealthLimits::default`]；测试注入小上限/短超时，
/// 以在毫秒级断言限流与超时行为真实生效。
#[derive(Debug, Clone, Copy)]
pub struct HealthLimits {
    /// 最大并发连接数（含已接受但未完成的慢连接）
    pub max_connections: usize,
    /// 单连接请求读超时
    pub read_timeout: Duration,
    /// 单连接响应写超时
    pub write_timeout: Duration,
}

impl Default for HealthLimits {
    fn default() -> Self {
        Self {
            max_connections: DEFAULT_MAX_HEALTH_CONNECTIONS,
            read_timeout: DEFAULT_HEALTH_READ_TIMEOUT,
            write_timeout: DEFAULT_HEALTH_WRITE_TIMEOUT,
        }
    }
}

// ──── 公共 API ────

/// 启动轻量级 HTTP Health/Metrics 端点（资源上限见 [`HealthLimits::default`]）
///
/// 监听指定地址，处理 /health 和 /metrics 请求。
///
/// - `addr`: 监听地址（如 "127.0.0.1:19528"）
/// - `metrics`: AgentMetrics 实例
/// - `ready`: 共享就绪标志（R-AGT-20：连接状态探针实时回写，非启动快照）
///
/// 返回 JoinHandle，可 abort 以优雅关闭。
pub fn start_health_server(
    addr: &str,
    metrics: AgentMetrics,
    ready: Arc<std::sync::atomic::AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    start_health_server_with_limits(addr, metrics, ready, HealthLimits::default())
}

/// 同 [`start_health_server`]，但可注入连接上限与读/写超时。
///
/// 上限语义：接受连接前先取配额，**超限连接立即关闭**（不排队、不回响应），
/// 避免慢连接经 accept 队列或任务堆积占用资源。
pub fn start_health_server_with_limits(
    addr: &str,
    metrics: AgentMetrics,
    ready: Arc<std::sync::atomic::AtomicBool>,
    limits: HealthLimits,
) -> tokio::task::JoinHandle<()> {
    let metrics = Arc::new(metrics);
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

        // 连接配额：与读/写超时共同保证 fd 与任务占用有界。
        let connections = Arc::new(Semaphore::new(limits.max_connections));

        loop {
            match listener.accept().await {
                Ok((mut socket, _)) => {
                    let permit = match Arc::clone(&connections).try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => {
                            // 超限：立即断开，不排队也不回响应——慢连接不得借排队占位。
                            // 用 debug 而非 warn：被扫描/攻击时 warn 日志本身会变成洪泛面。
                            tracing::debug!(
                                max_connections = limits.max_connections,
                                "health connection limit reached; closing new connection"
                            );
                            continue;
                        }
                    };
                    let metrics = Arc::clone(&metrics);
                    let ready = Arc::clone(&ready);
                    tokio::spawn(async move {
                        // 处理期间持有配额；连接结束（读完/超时/写完）即释放
                        let _permit = permit;
                        let mut buf = [0u8; 4096];
                        let n =
                            match tokio::time::timeout(limits.read_timeout, socket.read(&mut buf))
                                .await
                            {
                                Ok(Ok(n)) if n > 0 => n,
                                // EOF / 读错误：对端已关闭，直接结束
                                Ok(_) => return,
                                Err(_) => {
                                    // 读超时：slowloris 防护——慢客户端不得无限占用 fd 与配额
                                    tracing::debug!(
                                        "health request read timed out; closing connection"
                                    );
                                    return;
                                }
                            };

                        let request = String::from_utf8_lossy(&buf[..n]);
                        let first_line = request.lines().next().unwrap_or("");
                        let parts: Vec<&str> = first_line.split_whitespace().collect();
                        let raw_path = parts.get(1).unwrap_or(&"/");

                        let (path, query_params) = parse_path_and_query(raw_path);

                        let (status, content_type, body) = match path.as_str() {
                            "/health" => {
                                let is_ready =
                                    query_params.get("ready").map(|v| v.as_str()) == Some("true");
                                if is_ready {
                                    handle_health_ready(&ready)
                                } else {
                                    (
                                        "200 OK",
                                        "application/json",
                                        r#"{"status":"SERVING"}"#.to_string(),
                                    )
                                }
                            }
                            "/metrics" => {
                                let body = metrics.render_prometheus_text();
                                ("200 OK", "text/plain; version=0.0.4", body)
                            }
                            _ => ("404 Not Found", "text/plain", "Not Found".to_string()),
                        };

                        let response = format!(
                            "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\n\r\n{}",
                            status,
                            content_type,
                            body.len(),
                            body
                        );

                        // 写超时：对端不读时任务与配额不得无限挂起
                        let _ = tokio::time::timeout(
                            limits.write_timeout,
                            socket.write_all(response.as_bytes()),
                        )
                        .await;
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
        (
            "200 OK",
            "application/json",
            r#"{"status":"READY"}"#.to_string(),
        )
    } else {
        (
            "503 Service Unavailable",
            "application/json",
            r#"{"status":"NOT_READY"}"#.to_string(),
        )
    }
}

// ──── gRPC Health 服务（coord.agent.Health）────

/// 自定义 `coord.agent.Health/Check` gRPC 服务实现。
///
/// # 背景（误报修复）
/// coord agent 不实现标准 gRPC health 协议（grpc.health.v1.Health），
/// Java SDK `healthCheck()` 实际调用的是 agent_api.proto 中自定义的
/// `coord.agent.Health/Check`——若不注册该服务，SDK 会收到 UNIMPLEMENTED →
/// 返回 NOT_SERVING 误报，而注册/ID 生成等服务实际可用。
///
/// 因此：注册本服务并返回 SERVING（存活语义，与 HTTP `/health` 一致），
/// 消除该误报指示器；健康/就绪状态以 `/api/v1/health` 为准。
#[derive(Debug, Default)]
pub struct GrpcHealthService;

#[tonic::async_trait]
impl coord_proto::agent::health_server::Health for GrpcHealthService {
    async fn check(
        &self,
        _request: tonic::Request<coord_proto::agent::HealthCheckRequest>,
    ) -> Result<tonic::Response<coord_proto::agent::HealthCheckResponse>, tonic::Status> {
        Ok(tonic::Response::new(
            coord_proto::agent::HealthCheckResponse {
                status: coord_proto::agent::health_check_response::ServingStatus::Serving as i32,
            },
        ))
    }
}
