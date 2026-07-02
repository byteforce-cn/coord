// Health Check — HTTP /health 和 /metrics 端点
//
// 提供轻量级 HTTP 端点用于 K8s 探活和 Prometheus 指标采集。
// 使用原生 tokio TcpListener，不引入额外 HTTP 框架依赖。
//
// ADP §16.4 要求：
// - Liveness:  /health 返回 200 OK（进程存活）
// - Readiness: /health?ready=true 检查 Raft 就绪状态及 Per-Region 就绪
// - Verbose:   /health?verbose=true 返回每个 Region 的详细状态
// - Metrics:   /metrics 返回 Prometheus 文本格式

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use coord_core::types::RegionId;

use crate::metrics::Metrics;

// ============================================================================
// RegionHealthRegistry — Per-Region 健康状态注册表
// ============================================================================

/// 单个 Region 的健康状态
#[derive(Debug, Clone)]
pub struct RegionHealth {
    /// Region ID
    pub region_id: RegionId,
    /// Raft 是否就绪（Leader 已选出且 Applied >= Committed）
    pub raft_ready: bool,
    /// 当前角色（Leader/Follower/Candidate）
    pub role: String,
    /// Raft commit index
    pub commit_index: u64,
    /// Raft applied index
    pub applied_index: u64,
    /// 是否为本节点上的副本
    pub has_local_replica: bool,
}

/// Per-Region 健康状态注册表
///
/// 由 RegionManager 在 Region 状态变更时更新。
/// Health HTTP handler 读取此注册表以生成就绪检查和详细状态。
pub struct RegionHealthRegistry {
    /// Region ID → 健康状态
    regions: RwLock<HashMap<RegionId, RegionHealth>>,
    /// 本节点上 Region 副本总数
    total_regions: AtomicU64,
    /// 本节点上已就绪的 Region 副本数
    ready_regions: AtomicU64,
}

impl RegionHealthRegistry {
    /// 创建新的健康注册表
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            regions: RwLock::new(HashMap::new()),
            total_regions: AtomicU64::new(0),
            ready_regions: AtomicU64::new(0),
        })
    }

    /// 更新或插入一个 Region 的健康状态
    pub fn update_region(&self, health: RegionHealth) {
        let region_id = health.region_id;
        let is_ready = health.raft_ready;
        let mut regions = self.regions.write();

        if regions.contains_key(&region_id) {
            // 更新已存在的 Region：调整就绪计数
            let was_ready = regions.get(&region_id).map(|h| h.raft_ready).unwrap_or(false);
            if was_ready && !is_ready {
                self.ready_regions.fetch_sub(1, Ordering::Relaxed);
            } else if !was_ready && is_ready {
                self.ready_regions.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            // 新 Region
            self.total_regions.fetch_add(1, Ordering::Relaxed);
            if is_ready {
                self.ready_regions.fetch_add(1, Ordering::Relaxed);
            }
        }
        regions.insert(region_id, health);
    }

    /// 移除一个 Region 的健康状态
    pub fn remove_region(&self, region_id: RegionId) {
        let mut regions = self.regions.write();
        if let Some(health) = regions.remove(&region_id) {
            self.total_regions.fetch_sub(1, Ordering::Relaxed);
            if health.raft_ready {
                self.ready_regions.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    /// 获取就绪状态摘要
    pub fn readiness_summary(&self) -> RegionReadiness {
        let total = self.total_regions.load(Ordering::Relaxed);
        let ready = self.ready_regions.load(Ordering::Relaxed);
        let regions = self.regions.read();
        let pending: Vec<RegionId> = regions
            .iter()
            .filter(|(_, h)| !h.raft_ready)
            .map(|(id, _)| *id)
            .collect();

        RegionReadiness {
            ready: ready == total && total > 0,
            regions_ready: ready,
            regions_total: total,
            pending_regions: pending,
        }
    }

    /// 获取所有 Region 的详细健康状态
    pub fn verbose_status(&self) -> Vec<RegionHealth> {
        self.regions.read().values().cloned().collect()
    }

    /// 检查所有 Region 是否就绪
    pub fn is_all_ready(&self) -> bool {
        let total = self.total_regions.load(Ordering::Relaxed);
        let ready = self.ready_regions.load(Ordering::Relaxed);
        total > 0 && ready == total
    }
}

/// Region 就绪状态摘要
#[derive(Debug, Clone, serde::Serialize)]
pub struct RegionReadiness {
    pub ready: bool,
    pub regions_ready: u64,
    pub regions_total: u64,
    pub pending_regions: Vec<RegionId>,
}

// ──── Health Server ────

/// 启动轻量级 HTTP Health/Metrics 端点
///
/// 监听指定地址，处理 /health 和 /metrics 请求。
/// 支持查询参数：
/// - `/health` — 进程存活检查
/// - `/health?ready=true` — 就绪检查（含 Per-Region）
/// - `/health?verbose=true` — 详细 Region 状态
/// - `/metrics` — Prometheus 指标
///
/// 返回 JoinHandle，可 abort 以优雅关闭。
pub async fn start_health_server(
    addr: &str,
    metrics: Arc<Metrics>,
    raft_ready: Arc<AtomicBool>,
) -> Result<tokio::task::JoinHandle<()>, Box<dyn std::error::Error + Send + Sync>> {
    start_health_server_with_registry(addr, metrics, raft_ready, None).await
}

/// 启动带 Region 健康注册表的 HTTP Health/Metrics 端点
pub async fn start_health_server_with_registry(
    addr: &str,
    metrics: Arc<Metrics>,
    raft_ready: Arc<AtomicBool>,
    region_registry: Option<Arc<RegionHealthRegistry>>,
) -> Result<tokio::task::JoinHandle<()>, Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("Health/Metrics HTTP server listening on http://{}", addr);

    let handle = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((mut socket, _)) => {
                    let metrics = Arc::clone(&metrics);
                    let raft_ready = Arc::clone(&raft_ready);
                    let registry = region_registry.clone();
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

                        // 解析路径和查询参数
                        let (path, query_params) = parse_path_and_query(raw_path);

                        let (status, content_type, body) = match path.as_str() {
                            "/health" => {
                                let is_ready_query = query_params.get("ready").map(|v| v.as_str()) == Some("true");
                                let is_verbose = query_params.get("verbose").map(|v| v.as_str()) == Some("true");

                                if is_verbose {
                                    // 详细状态
                                    handle_health_verbose(&registry)
                                } else if is_ready_query {
                                    // 就绪检查
                                    handle_health_ready(&raft_ready, &registry)
                                } else {
                                    // 存活检查
                                    handle_health_live(&raft_ready)
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
    });

    Ok(handle)
}

// ──── 查询参数解析 ────

/// 解析 URL 路径和查询参数
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

// ──── Health Handler ────

/// 存活检查：进程存活即返回 200
fn handle_health_live(
    raft_ready: &AtomicBool,
) -> (&'static str, &'static str, String) {
    if raft_ready.load(Ordering::Relaxed) {
        ("200 OK", "application/json", r#"{"status":"SERVING"}"#.to_string())
    } else {
        ("503 Service Unavailable", "application/json", r#"{"status":"NOT_SERVING"}"#.to_string())
    }
}

/// 就绪检查：Raft 就绪 + 所有 Region 副本就绪
fn handle_health_ready(
    raft_ready: &AtomicBool,
    registry: &Option<Arc<RegionHealthRegistry>>,
) -> (&'static str, &'static str, String) {
    let raft_ok = raft_ready.load(Ordering::Relaxed);

    match registry {
        Some(reg) => {
            let summary = reg.readiness_summary();
            if raft_ok && summary.ready {
                let body = serde_json::json!({
                    "status": "READY",
                    "regions_ready": summary.regions_ready,
                    "regions_total": summary.regions_total
                }).to_string();
                ("200 OK", "application/json", body)
            } else {
                let body = serde_json::json!({
                    "status": "NOT_READY",
                    "raft_ready": raft_ok,
                    "regions_ready": summary.regions_ready,
                    "regions_total": summary.regions_total,
                    "pending_regions": summary.pending_regions
                }).to_string();
                ("503 Service Unavailable", "application/json", body)
            }
        }
        None => {
            // 无 Region 注册表：仅检查 Raft 就绪
            if raft_ok {
                ("200 OK", "application/json", r#"{"status":"READY"}"#.to_string())
            } else {
                ("503 Service Unavailable", "application/json", r#"{"status":"NOT_READY"}"#.to_string())
            }
        }
    }
}

/// 详细状态：列出每个 Region 的健康信息
fn handle_health_verbose(
    registry: &Option<Arc<RegionHealthRegistry>>,
) -> (&'static str, &'static str, String) {
    match registry {
        Some(reg) => {
            let regions = reg.verbose_status();
            let summary = reg.readiness_summary();
            let body = serde_json::json!({
                "status": if summary.ready { "READY" } else { "NOT_READY" },
                "regions_ready": summary.regions_ready,
                "regions_total": summary.regions_total,
                "regions": regions.iter().map(|h| serde_json::json!({
                    "region_id": h.region_id,
                    "raft_ready": h.raft_ready,
                    "role": h.role,
                    "commit_index": h.commit_index,
                    "applied_index": h.applied_index,
                    "has_local_replica": h.has_local_replica,
                })).collect::<Vec<_>>()
            }).to_string();
            ("200 OK", "application/json", body)
        }
        None => {
            ("200 OK", "application/json", r#"{"status":"NO_REGIONS","regions":[]}"#.to_string())
        }
    }
}

// ──── Raft Readiness ────

/// 检查 Raft 是否就绪（Leader 已选出且状态机已追上日志）
pub fn check_raft_ready(
    raft_leader_id: i64,
    raft_commit_index: u64,
    raft_applied_index: u64,
) -> bool {
    // Leader 存在且状态机已应用全部已提交日志
    raft_leader_id > 0 && raft_applied_index >= raft_commit_index
}

// ──── 测试 ────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_path_no_query() {
        let (path, params) = parse_path_and_query("/health");
        assert_eq!(path, "/health");
        assert!(params.is_empty());
    }

    #[test]
    fn test_parse_path_with_query() {
        let (path, params) = parse_path_and_query("/health?ready=true");
        assert_eq!(path, "/health");
        assert_eq!(params.get("ready").unwrap(), "true");
    }

    #[test]
    fn test_parse_path_with_multiple_params() {
        let (path, params) = parse_path_and_query("/health?ready=true&verbose=true");
        assert_eq!(path, "/health");
        assert_eq!(params.get("ready").unwrap(), "true");
        assert_eq!(params.get("verbose").unwrap(), "true");
    }

    #[test]
    fn test_region_health_registry_new() {
        let registry = RegionHealthRegistry::new();
        assert!(!registry.is_all_ready());
        let summary = registry.readiness_summary();
        assert_eq!(summary.regions_total, 0);
        assert_eq!(summary.regions_ready, 0);
        assert!(!summary.ready);
    }

    #[test]
    fn test_region_health_registry_update() {
        let registry = RegionHealthRegistry::new();
        registry.update_region(RegionHealth {
            region_id: 1,
            raft_ready: true,
            role: "Leader".into(),
            commit_index: 100,
            applied_index: 100,
            has_local_replica: true,
        });
        let summary = registry.readiness_summary();
        assert_eq!(summary.regions_total, 1);
        assert_eq!(summary.regions_ready, 1);
        assert!(summary.ready);
    }

    #[test]
    fn test_region_health_registry_multiple_regions() {
        let registry = RegionHealthRegistry::new();
        registry.update_region(RegionHealth {
            region_id: 1,
            raft_ready: true,
            role: "Leader".into(),
            commit_index: 100,
            applied_index: 100,
            has_local_replica: true,
        });
        registry.update_region(RegionHealth {
            region_id: 2,
            raft_ready: false,
            role: "Follower".into(),
            commit_index: 100,
            applied_index: 95,
            has_local_replica: true,
        });
        let summary = registry.readiness_summary();
        assert_eq!(summary.regions_total, 2);
        assert_eq!(summary.regions_ready, 1);
        assert!(!summary.ready);
        assert!(summary.pending_regions.contains(&2));
    }

    #[test]
    fn test_region_health_registry_ready_transition() {
        let registry = RegionHealthRegistry::new();
        // Initially not ready
        registry.update_region(RegionHealth {
            region_id: 1,
            raft_ready: false,
            role: "Follower".into(),
            commit_index: 100,
            applied_index: 90,
            has_local_replica: true,
        });
        assert!(!registry.is_all_ready());

        // Transition to ready
        registry.update_region(RegionHealth {
            region_id: 1,
            raft_ready: true,
            role: "Leader".into(),
            commit_index: 100,
            applied_index: 100,
            has_local_replica: true,
        });
        assert!(registry.is_all_ready());
    }

    #[test]
    fn test_region_health_registry_remove() {
        let registry = RegionHealthRegistry::new();
        registry.update_region(RegionHealth {
            region_id: 1,
            raft_ready: true,
            role: "Leader".into(),
            commit_index: 100,
            applied_index: 100,
            has_local_replica: true,
        });
        registry.remove_region(1);
        let summary = registry.readiness_summary();
        assert_eq!(summary.regions_total, 0);
        assert_eq!(summary.regions_ready, 0);
    }

    #[test]
    fn test_region_health_registry_verbose() {
        let registry = RegionHealthRegistry::new();
        registry.update_region(RegionHealth {
            region_id: 1,
            raft_ready: true,
            role: "Leader".into(),
            commit_index: 100,
            applied_index: 100,
            has_local_replica: true,
        });
        registry.update_region(RegionHealth {
            region_id: 2,
            raft_ready: true,
            role: "Follower".into(),
            commit_index: 100,
            applied_index: 100,
            has_local_replica: true,
        });
        let status = registry.verbose_status();
        assert_eq!(status.len(), 2);
    }

    #[test]
    fn test_check_raft_ready() {
        // Leader exists, applied >= committed
        assert!(check_raft_ready(1, 100, 100));
        // Leader exists, applied < committed (not ready)
        assert!(!check_raft_ready(1, 100, 99));
        // No leader
        assert!(!check_raft_ready(0, 100, 100));
    }
}
