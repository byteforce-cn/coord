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
// - agent_plugin_invocations_total: 插件调用总数（plugin/engine/outcome）
// - agent_plugin_traps_total: 插件沙箱 trap 总数（plugin/reason：fuel/epoch/…）
// - agent_plugin_load_failures_total: 插件加载/启动失败总数（plugin）

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::RwLock;

// ──── AgentMetrics ────

/// Agent 全局指标注册表
#[derive(Clone)]
pub struct AgentMetrics {
    inner: Arc<MetricsInner>,
}

impl std::fmt::Debug for AgentMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgentMetrics")
            .field("connected", &self.inner.connected.load(Ordering::Relaxed))
            .field("cache_hits", &self.inner.cache_hits.load(Ordering::Relaxed))
            .field(
                "cache_misses",
                &self.inner.cache_misses.load(Ordering::Relaxed),
            )
            .field(
                "watch_subscribers",
                &self.inner.watch_subscribers.load(Ordering::Relaxed),
            )
            .finish()
    }
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
    /// 插件调用计数（键 = (plugin, engine, outcome)；outcome ∈ {ok, error}）
    pub plugin_invocations: RwLock<BTreeMap<(String, String, String), u64>>,
    /// 插件沙箱 trap 计数（键 = (plugin, reason)）
    pub plugin_traps: RwLock<BTreeMap<(String, String), u64>>,
    /// 插件加载/启动失败计数（键 = plugin）
    pub plugin_load_failures: RwLock<BTreeMap<String, u64>>,
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
                plugin_invocations: RwLock::new(BTreeMap::new()),
                plugin_traps: RwLock::new(BTreeMap::new()),
                plugin_load_failures: RwLock::new(BTreeMap::new()),
            }),
        }
    }

    /// 标记已连接 Server 集群
    pub fn set_connected(&self, connected: bool) {
        self.inner
            .connected
            .store(if connected { 1 } else { 0 }, Ordering::Relaxed);
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

    /// R-AGT-20：按 gRPC URI 路径计数（/coord.agent.KV/Put 等核心方法）。
    /// 非核心/未知路径不计入（保持 5 槽位语义）。
    pub fn record_grpc_method(&self, path: &str) {
        let idx = match path {
            p if p.ends_with("/Put") => Some(0),
            p if p.ends_with("/Range") => Some(1),
            p if p.ends_with("/Delete") => Some(2),
            p if p.ends_with("/Txn") => Some(3),
            p if p.ends_with("/Status") || p.ends_with("/MemberList") => Some(4),
            _ => None,
        };
        if let Some(i) = idx {
            self.record_grpc_request(i);
        }
    }

    /// R-AGT-20：设置当前 Watch 订阅者数量（WatchProxy 订阅/退订时调用）。
    pub fn set_watch_subscribers(&self, count: i64) {
        self.inner.watch_subscribers.store(count, Ordering::Relaxed);
    }

    /// R-AGT-20：Watch 订阅 +1。
    pub fn inc_watch_subscribers(&self) {
        self.inner.watch_subscribers.fetch_add(1, Ordering::Relaxed);
    }

    /// R-AGT-20：Watch 订阅 -1。
    pub fn dec_watch_subscribers(&self) {
        self.inner.watch_subscribers.fetch_sub(1, Ordering::Relaxed);
    }

    // ──── 插件指标（Phase 5 观测面）────

    /// 记录一次插件调用（`engine` ∈ {"js","wasm"}；`ok` = 未返回错误）。
    pub fn record_plugin_invocation(&self, plugin: &str, engine: &str, ok: bool) {
        let outcome = if ok { "ok" } else { "error" };
        let key = (plugin.to_string(), engine.to_string(), outcome.to_string());
        *self
            .inner
            .plugin_invocations
            .write()
            .entry(key)
            .or_insert(0) += 1;
    }

    /// 记录一次插件沙箱 trap（`reason` ∈ {"fuel","epoch","memory","other"}）。
    pub fn record_plugin_trap(&self, plugin: &str, reason: &str) {
        let key = (plugin.to_string(), reason.to_string());
        *self.inner.plugin_traps.write().entry(key).or_insert(0) += 1;
    }

    /// 记录一次插件加载/启动失败。
    pub fn record_plugin_load_failure(&self, plugin: &str) {
        *self
            .inner
            .plugin_load_failures
            .write()
            .entry(plugin.to_string())
            .or_insert(0) += 1;
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

        out.push_str(
            "# HELP coord_agent_connected 1 if connected to server cluster, 0 otherwise\n",
        );
        out.push_str("# TYPE coord_agent_connected gauge\n");
        out.push_str(&format!("coord_agent_connected {}\n", connected));

        out.push_str("# HELP coord_agent_cache_hits_total Total cache hits\n");
        out.push_str("# TYPE coord_agent_cache_hits_total counter\n");
        out.push_str(&format!("coord_agent_cache_hits_total {}\n", cache_hits));

        out.push_str("# HELP coord_agent_cache_misses_total Total cache misses\n");
        out.push_str("# TYPE coord_agent_cache_misses_total counter\n");
        out.push_str(&format!(
            "coord_agent_cache_misses_total {}\n",
            cache_misses
        ));

        let method_names = ["put", "range", "delete", "txn", "status"];
        out.push_str("# HELP coord_agent_grpc_requests_total Total gRPC requests by method\n");
        out.push_str("# TYPE coord_agent_grpc_requests_total counter\n");
        for (i, name) in method_names.iter().enumerate() {
            let count = self.inner.grpc_requests[i].load(Ordering::Relaxed);
            out.push_str(&format!(
                "coord_agent_grpc_requests_total{{method=\"{}\"}} {}\n",
                name, count
            ));
        }

        out.push_str("# HELP coord_agent_watch_subscribers Current watch subscriber count\n");
        out.push_str("# TYPE coord_agent_watch_subscribers gauge\n");
        out.push_str(&format!("coord_agent_watch_subscribers {}\n", subscribers));

        // ──── 插件指标（Phase 5）────
        let invocations = self.inner.plugin_invocations.read();
        out.push_str(
            "# HELP coord_agent_plugin_invocations_total Total plugin invocations by outcome\n",
        );
        out.push_str("# TYPE coord_agent_plugin_invocations_total counter\n");
        for ((plugin, engine, outcome), count) in invocations.iter() {
            out.push_str(&format!(
                "coord_agent_plugin_invocations_total{{plugin=\"{}\",engine=\"{}\",outcome=\"{}\"}} {}\n",
                escape_label(plugin),
                escape_label(engine),
                escape_label(outcome),
                count
            ));
        }

        let traps = self.inner.plugin_traps.read();
        out.push_str(
            "# HELP coord_agent_plugin_traps_total Total plugin sandbox traps by reason\n",
        );
        out.push_str("# TYPE coord_agent_plugin_traps_total counter\n");
        for ((plugin, reason), count) in traps.iter() {
            out.push_str(&format!(
                "coord_agent_plugin_traps_total{{plugin=\"{}\",reason=\"{}\"}} {}\n",
                escape_label(plugin),
                escape_label(reason),
                count
            ));
        }

        let failures = self.inner.plugin_load_failures.read();
        out.push_str(
            "# HELP coord_agent_plugin_load_failures_total Total plugin load/start failures\n",
        );
        out.push_str("# TYPE coord_agent_plugin_load_failures_total counter\n");
        for (plugin, count) in failures.iter() {
            out.push_str(&format!(
                "coord_agent_plugin_load_failures_total{{plugin=\"{}\"}} {}\n",
                escape_label(plugin),
                count
            ));
        }

        // 末尾必须有换行
        out.push('\n');
        out
    }
}

/// 转义 Prometheus 标签值（`\` / `"` / 换行）。
fn escape_label(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
}

impl Default for AgentMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// ──── R-AGT-20：gRPC 请求计数中间件 ────

/// tower Layer：挂到 Agent gRPC router 外层，按方法路径计数（此前
/// `record_grpc_request` 为死代码、`coord_agent_grpc_requests_total` 恒为 0）。
#[derive(Clone)]
pub struct AgentGrpcMetricsLayer {
    metrics: AgentMetrics,
}

impl AgentGrpcMetricsLayer {
    pub fn new(metrics: AgentMetrics) -> Self {
        Self { metrics }
    }
}

impl<S> tower::Layer<S> for AgentGrpcMetricsLayer {
    type Service = AgentGrpcMetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AgentGrpcMetricsService {
            inner,
            metrics: self.metrics.clone(),
        }
    }
}

/// tower Service 包装：计数后透传。
#[derive(Clone)]
pub struct AgentGrpcMetricsService<S> {
    inner: S,
    metrics: AgentMetrics,
}

impl<S, ReqBody, ResBody> tower::Service<http::Request<ReqBody>> for AgentGrpcMetricsService<S>
where
    S: tower::Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        self.metrics.record_grpc_method(req.uri().path());
        self.inner.call(req)
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
    fn test_record_grpc_method_mapping() {
        let m = AgentMetrics::new();
        m.record_grpc_method("/coord.agent.KV/Put");
        m.record_grpc_method("/coord.agent.KV/Range");
        m.record_grpc_method("/coord.agent.KV/Put");
        m.record_grpc_method("/coord.agent.Unknown/Foo"); // 不计入
        let text = m.render_prometheus_text();
        assert!(
            text.contains("coord_agent_grpc_requests_total{method=\"put\"} 2"),
            "put 计数 2: {text}"
        );
        assert!(text.contains("coord_agent_grpc_requests_total{method=\"range\"} 1"));
        assert!(text.contains("coord_agent_grpc_requests_total{method=\"txn\"} 0"));
    }

    #[test]
    fn test_watch_subscribers_setter() {
        let m = AgentMetrics::new();
        m.inc_watch_subscribers();
        m.inc_watch_subscribers();
        m.dec_watch_subscribers();
        let text = m.render_prometheus_text();
        assert!(text.contains("coord_agent_watch_subscribers 1"));
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

    #[test]
    fn test_plugin_metrics_render() {
        let m = AgentMetrics::new();
        m.record_plugin_invocation("echo", "js", true);
        m.record_plugin_invocation("echo", "js", true);
        m.record_plugin_invocation("checksum", "wasm", false);
        m.record_plugin_trap("checksum", "fuel");
        m.record_plugin_load_failure("broken");

        let text = m.render_prometheus_text();
        assert!(
            text.contains(
                "coord_agent_plugin_invocations_total{plugin=\"echo\",engine=\"js\",outcome=\"ok\"} 2"
            ),
            "{text}"
        );
        assert!(text.contains(
            "coord_agent_plugin_invocations_total{plugin=\"checksum\",engine=\"wasm\",outcome=\"error\"} 1"
        ));
        assert!(
            text.contains("coord_agent_plugin_traps_total{plugin=\"checksum\",reason=\"fuel\"} 1")
        );
        assert!(text.contains("coord_agent_plugin_load_failures_total{plugin=\"broken\"} 1"));

        // 空注册表也应有 HELP/TYPE（Grafana 无数据时不断线）
        let empty = AgentMetrics::new().render_prometheus_text();
        assert!(empty.contains("# TYPE coord_agent_plugin_invocations_total counter"));
        assert!(empty.contains("# TYPE coord_agent_plugin_traps_total counter"));
    }

    #[test]
    fn test_escape_label() {
        assert_eq!(escape_label("plain"), "plain");
        assert_eq!(escape_label("a\"b"), "a\\\"b");
        assert_eq!(escape_label("a\\b"), "a\\\\b");
        assert_eq!(escape_label("a\nb"), "a\\nb");
    }
}
