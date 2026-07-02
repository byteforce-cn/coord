// coord-agent: Agent 指标收集
//
// 使用原子计数器实现轻量级指标收集，与 coord-server 的 metrics 模块对等。
// 通过 HTTP /metrics 端点暴露 Prometheus 文本格式。
//
// 指标：
// - agent_uptime_seconds: Agent 进程启动时间
// - agent_connected: 是否已连接 Server 集群（0/1）
// - agent_cache_hits_total: 缓存命中总次数
// - agent_cache_misses_total: 缓存未命中总次数
// - agent_grpc_requests_total: gRPC 请求总数（按方法分）
// - agent_watch_subscribers_total: 当前 Watch 订阅者数量
//
// 参见 docs/client-agent-architecture.md §4.6。

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

// ──── AgentMetrics ────

/// Agent 全局指标注册表
#[derive(Clone)]
pub struct AgentMetrics {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    /// 进程启动时间
    pub start_time: Instant,
    /// 是否已连接 Server 集群（0=未连接, 1=已连接）
    pub connected: AtomicI64,
    /// 缓存命中总次数
    pub cache_hits: AtomicU64,
    /// 缓存未命中总次数
    pub cache_misses: AtomicU64,
    /// gRPC 请求总数（按方法：put/range/delete/txn/status）
    pub grpc_requests: [AtomicU64; 5],
    /// Watch 订阅者数量
    pub watch_subscribers: AtomicI64,
}

impl AgentMetrics {
    /// 创建新的 AgentMetrics 实例
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MetricsInner {
                start_time: Instant::now(),
                connected: AtomicI64::new(0),
                cache_hits: AtomicU64::new(0),
                cache_misses: AtomicU64::new(0),
                grpc_requests: Default::default(),
                watch_subscribers: AtomicI64::new(0),
            }),
        }
    }

    /// 标记已连接 Server 集群
    pub fn set_connected(&self, connected: bool) {
        self.inner.connected.store(if connected { 1 } else { 0 }, Ordering::Relaxed);
    }

    /// 记录缓存命中
    pub fn record_cache_hit(&self) {
        self.inner.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录缓存未命中
    pub fn record_cache_miss(&self) {
        self.inner.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录 gRPC 请求
    pub fn record_grpc_request(&self, method_idx: usize) {
        if method_idx < self.inner.grpc_requests.len() {
            self.inner.grpc_requests[method_idx].fetch_add(1, Ordering::Relaxed);
        }
    }

    /// 渲染 Prometheus 文本格式
    pub fn render_prometheus_text(&self) -> String {
        let uptime = self.inner.start_time.elapsed().as_secs_f64();
        let connected = self.inner.connected.load(Ordering::Relaxed);
        let cache_hits = self.inner.cache_hits.load(Ordering::Relaxed);
        let cache_misses = self.inner.cache_misses.load(Ordering::Relaxed);
        let subscribers = self.inner.watch_subscribers.load(Ordering::Relaxed);

        let mut out = String::new();

        // HELP/TYPE lines
        out.push_str("# HELP coord_agent_uptime_seconds Agent process uptime in seconds\n");
        out.push_str("# TYPE coord_agent_uptime_seconds gauge\n");
        out.push_str(&format!("coord_agent_uptime_seconds {:.2}\n", uptime));

        out.push_str("# HELP coord_agent_connected 1 if connected to server cluster, 0 otherwise\n");
        out.push_str("# TYPE coord_agent_connected gauge\n");
        out.push_str(&format!("coord_agent_connected {}\n", connected));

        out.push_str("# HELP coord_agent_cache_hits_total Total cache hits\n");
        out.push_str("# TYPE coord_agent_cache_hits_total counter\n");
        out.push_str(&format!("coord_agent_cache_hits_total {}\n", cache_hits));

        out.push_str("# HELP coord_agent_cache_misses_total Total cache misses\n");
        out.push_str("# TYPE coord_agent_cache_misses_total counter\n");
        out.push_str(&format!("coord_agent_cache_misses_total {}\n", cache_misses));

        let method_names = ["put", "range", "delete", "txn", "status"];
        out.push_str("# HELP coord_agent_grpc_requests_total Total gRPC requests by method\n");
        out.push_str("# TYPE coord_agent_grpc_requests_total counter\n");
        for (i, name) in method_names.iter().enumerate() {
            let count = self.inner.grpc_requests[i].load(Ordering::Relaxed);
            out.push_str(&format!("coord_agent_grpc_requests_total{{method=\"{}\"}} {}\n", name, count));
        }

        out.push_str("# HELP coord_agent_watch_subscribers Current watch subscriber count\n");
        out.push_str("# TYPE coord_agent_watch_subscribers gauge\n");
        out.push_str(&format!("coord_agent_watch_subscribers {}\n", subscribers));

        // 末尾必须有换行
        out.push('\n');
        out
    }
}

impl Default for AgentMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn test_metrics_new() {
        let m = AgentMetrics::new();
        assert_eq!(m.inner.connected.load(Ordering::Relaxed), 0);
        assert_eq!(m.inner.cache_hits.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_metrics_record() {
        let m = AgentMetrics::new();
        m.record_cache_hit();
        m.record_cache_hit();
        m.record_cache_miss();
        assert_eq!(m.inner.cache_hits.load(Ordering::Relaxed), 2);
        assert_eq!(m.inner.cache_misses.load(Ordering::Relaxed), 1);

        m.set_connected(true);
        assert_eq!(m.inner.connected.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_render_prometheus_text() {
        let m = AgentMetrics::new();
        m.set_connected(true);
        m.record_cache_hit();

        let text = m.render_prometheus_text();
        assert!(text.contains("coord_agent_uptime_seconds"));
        assert!(text.contains("coord_agent_connected 1"));
        assert!(text.contains("coord_agent_cache_hits_total 1"));
        assert!(text.contains("coord_agent_cache_misses_total 0"));
        assert!(text.contains("coord_agent_grpc_requests_total"));
        assert!(text.contains("coord_agent_watch_subscribers"));
    }
}
