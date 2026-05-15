#![allow(dead_code)] // P4D-06 健康检查模块尚未接入主流程，待后续 sprint 完成后移除
//! P4D-06: `HealthScheduler` — 周期性健康检查，结果通过 Gossip 广播。
//!
//! 每隔 `interval` 秒对 `targets` 中的每个实例执行 TCP 探针；
//! 成功则广播 `HealthStatus::Healthy`，失败则广播 `HealthStatus::Unhealthy`。

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use coord_core::gossip_types::{GossipAgent, HealthPayload, HealthStatus, ServiceDelta};
use tokio::net::TcpStream;
use tokio::time::interval;
use tracing::{debug, warn};

/// 单个待检目标描述。
#[derive(Debug, Clone)]
pub struct HealthTarget {
    pub service_name: String,
    pub instance_id: String,
    /// TCP 探针地址（host:port）。
    pub addr: SocketAddr,
}

/// 后台健康检查调度器。
pub struct HealthScheduler {
    targets: Vec<HealthTarget>,
    interval: Duration,
    gossip: Arc<dyn GossipAgent>,
}

impl HealthScheduler {
    pub fn new(
        targets: Vec<HealthTarget>,
        interval_secs: u64,
        gossip: Arc<dyn GossipAgent>,
    ) -> Self {
        Self {
            targets,
            interval: Duration::from_secs(interval_secs),
            gossip,
        }
    }

    /// 启动后台检查循环（tokio task）。
    pub fn spawn(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut ticker = interval(self.interval);
            loop {
                ticker.tick().await;
                for target in &self.targets {
                    self.check_and_report(target).await;
                }
            }
        });
    }

    async fn check_and_report(&self, target: &HealthTarget) {
        let status = if tcp_probe(target.addr).await {
            HealthStatus::Healthy
        } else {
            HealthStatus::Unhealthy
        };
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let payload = HealthPayload {
            service_name: target.service_name.clone(),
            instance_id: target.instance_id.clone(),
            status,
            checked_unix_ms: now_ms,
        };
        debug!(
            service = %target.service_name,
            instance = %target.instance_id,
            ?status,
            "health check result"
        );

        // 将健康状态更新到 Gossip 的 self_node_state（ServiceDelta.healthy 字段）
        // 此处仅更新本节点广播的 delta；其他节点通过 Scuttlebutt 同步
        let _ = self
            .gossip
            .put_service_delta(ServiceDelta {
                service_name: payload.service_name.clone(),
                instance_id: payload.instance_id.clone(),
                host: target.addr.ip().to_string(),
                port: target.addr.port() as u32,
                healthy: matches!(status, HealthStatus::Healthy),
                expires_unix_ms: now_ms + 300_000, // 5 min 默认续期
            })
            .await
            .inspect_err(|e| {
                warn!(error = %e, "failed to update gossip health delta");
            });
    }
}

/// TCP 连接探针（2 秒超时）。
async fn tcp_probe(addr: SocketAddr) -> bool {
    tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(addr))
        .await
        .map(|r| r.is_ok())
        .unwrap_or(false)
}

// ─── 测试 ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::net::{SocketAddr, TcpListener};
    use std::sync::Arc;

    use coord_core::gossip_types::NullGossipAgent;

    use super::{HealthScheduler, HealthTarget};

    /// 起一个本地 TCP listener，验证探针成功
    #[tokio::test]
    async fn probe_succeeds_on_open_port() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr: SocketAddr = listener.local_addr().expect("local addr");
        assert!(super::tcp_probe(addr).await);
    }

    #[tokio::test]
    async fn probe_fails_on_closed_port() {
        // 端口 1 通常不可用
        let addr: SocketAddr = "127.0.0.1:1".parse().expect("parse");
        assert!(!super::tcp_probe(addr).await);
    }

    #[test]
    fn scheduler_stores_targets() {
        let gossip = Arc::new(NullGossipAgent::new("n1", "127.0.0.1:9090"));
        let target = HealthTarget {
            service_name: "svc".to_string(),
            instance_id: "node-1".to_string(),
            addr: "127.0.0.1:8080".parse().expect("parse"),
        };
        let sched = HealthScheduler::new(vec![target], 10, gossip);
        assert_eq!(sched.targets.len(), 1);
    }
}
