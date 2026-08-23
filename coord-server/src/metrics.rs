// 可观测性 — Metrics 指标收集与 Prometheus 导出
//
// 使用原子计数器实现轻量级指标收集，不引入额外依赖。
// 通过 HTTP /metrics 端点暴露 Prometheus 文本格式。
//
// ADP §16.2 定义的指标类别：
// - Raft:  raft_leader_id, raft_term, raft_commit_index, raft_applied_index
// - gRPC:  grpc_requests_total, grpc_request_duration_seconds
// - Storage: storage_size_bytes, storage_keys_total
// - Lease: lease_active_total, lease_expired_total
// - Seal:  seal_status

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use std::collections::HashMap;

use coord_core::types::RegionId;
use parking_lot::RwLock;
use tower::util::ServiceExt;

/// 慢请求阈值（微秒）：超过则计入 slow 并 WARN（P1-09）
pub const SLOW_REQUEST_US: u64 = 1_000_000;

// ──── 指标注册表 ────

/// 全局指标注册表
#[derive(Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    // ── Raft 指标 ──
    pub raft_leader_id: AtomicI64,
    pub raft_term: AtomicU64,
    pub raft_commit_index: AtomicU64,
    pub raft_applied_index: AtomicU64,

    // ── gRPC 指标 ──
    pub grpc_requests_total: [AtomicU64; 5], // 按方法：put/range/delete/txn/status
    pub grpc_request_duration_us: [AtomicU64; 5], // 累计耗时（微秒）

    // ── Storage 指标 ──
    pub storage_size_bytes: AtomicU64,
    pub storage_keys_total: AtomicU64,

    // ── 磁盘水位（P1-02）──
    pub disk_available_bytes: AtomicU64,
    pub disk_total_bytes: AtomicU64,

    // ── Lease 指标 ──
    pub lease_active_total: AtomicI64,
    pub lease_expired_total: AtomicU64,

    // ── Seal 指标 ──
    pub seal_status: AtomicI64, // 0=Unsealed, 1=SealInProgress, 2=Sealed

    // ── Multi-Raft 指标（v6.0） ──
    /// Region 总数（全局）
    pub regions_total: AtomicU64,
    /// 在线节点数
    pub nodes_online: AtomicU64,
    /// Region Split 总次数
    pub region_split_total: AtomicU64,
    /// Region Merge 总次数
    pub region_merge_total: AtomicU64,
    /// PD 调度操作总次数
    pub pd_operator_total: AtomicU64,
    /// 本节点 Leader 数量
    pub local_leader_count: AtomicU64,
    /// 本节点 Region 副本数
    pub local_region_count: AtomicU64,

    // ── Per-Region 指标 ──
    /// Region ID → Arc<RegionMetrics>
    pub region_metrics: RwLock<Vec<Arc<RegionMetrics>>>,

    // ── Per-Method gRPC 指标（P1-09：请求计数/延迟/错误/慢请求）──
    /// gRPC 方法路径 → 指标
    pub method_metrics: RwLock<HashMap<String, Arc<MethodMetrics>>>,

    // ── 启动时间 ──
    pub start_time: Instant,
}

/// 单个 gRPC 方法的指标（P1-09）
#[derive(Debug, Default)]
pub struct MethodMetrics {
    /// 请求总数
    pub count: AtomicU64,
    /// 累计耗时（微秒）
    pub duration_us: AtomicU64,
    /// 非 OK 响应数（HTTP 状态 >= 400；gRPC 错误映射为 2xx 之外的状态码）
    pub errors: AtomicU64,
    /// 慢请求数（> SLOW_REQUEST_US）
    pub slow: AtomicU64,
}

// ============================================================================
// Per-Region 指标
// ============================================================================

/// 单个 Region 的运行时指标
#[derive(Debug)]
pub struct RegionMetrics {
    /// Region ID
    pub region_id: RegionId,
    /// 数据量（字节）
    pub size_bytes: AtomicU64,
    /// Key 数量
    pub keys_total: AtomicU64,
    /// Raft commit index
    pub raft_log_index: AtomicU64,
    /// 是否为 Leader（0=否, 1=是）
    pub is_leader: AtomicU64,
    /// Put 操作累计耗时（微秒）
    pub put_latency_us: AtomicU64,
    /// Put 操作调用次数
    pub put_count: AtomicU64,
}

impl RegionMetrics {
    /// 创建新的 Region 指标
    pub fn new(region_id: RegionId) -> Self {
        Self {
            region_id,
            size_bytes: AtomicU64::new(0),
            keys_total: AtomicU64::new(0),
            raft_log_index: AtomicU64::new(0),
            is_leader: AtomicU64::new(0),
            put_latency_us: AtomicU64::new(0),
            put_count: AtomicU64::new(0),
        }
    }
}

impl Default for MetricsInner {
    fn default() -> Self {
        Self {
            raft_leader_id: AtomicI64::new(0),
            raft_term: AtomicU64::new(0),
            raft_commit_index: AtomicU64::new(0),
            raft_applied_index: AtomicU64::new(0),
            grpc_requests_total: Default::default(),
            grpc_request_duration_us: Default::default(),
            storage_size_bytes: AtomicU64::new(0),
            storage_keys_total: AtomicU64::new(0),
            disk_available_bytes: AtomicU64::new(0),
            disk_total_bytes: AtomicU64::new(0),
            lease_active_total: AtomicI64::new(0),
            lease_expired_total: AtomicU64::new(0),
            seal_status: AtomicI64::new(0),
            regions_total: AtomicU64::new(0),
            nodes_online: AtomicU64::new(0),
            region_split_total: AtomicU64::new(0),
            region_merge_total: AtomicU64::new(0),
            pd_operator_total: AtomicU64::new(0),
            local_leader_count: AtomicU64::new(0),
            local_region_count: AtomicU64::new(0),
            region_metrics: RwLock::new(Vec::new()),
            method_metrics: RwLock::new(HashMap::new()),
            start_time: Instant::now(),
        }
    }
}

// ──── gRPC 方法索引 ────

/// gRPC 方法枚举（用于指标数组索引）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrpcMethod {
    Put = 0,
    Range = 1,
    Delete = 2,
    Txn = 3,
    Status = 4,
}

impl GrpcMethod {
    pub fn as_index(self) -> usize {
        self as usize
    }
}

// ──── Metrics API ────

impl Metrics {
    /// 创建新的指标注册表
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MetricsInner::default()),
        }
    }

    // ── Raft 指标更新 ──

    pub fn set_raft_leader_id(&self, id: u64) {
        self.inner
            .raft_leader_id
            .store(id as i64, Ordering::Relaxed);
    }

    pub fn set_raft_term(&self, term: u64) {
        self.inner.raft_term.store(term, Ordering::Relaxed);
    }

    pub fn set_raft_commit_index(&self, index: u64) {
        self.inner.raft_commit_index.store(index, Ordering::Relaxed);
    }

    pub fn set_raft_applied_index(&self, index: u64) {
        self.inner
            .raft_applied_index
            .store(index, Ordering::Relaxed);
    }

    // ── gRPC 指标更新 ──

    /// 记录一次 gRPC 请求
    pub fn record_grpc_request(&self, method: GrpcMethod, duration_us: u64) {
        let idx = method.as_index();
        self.inner.grpc_requests_total[idx].fetch_add(1, Ordering::Relaxed);
        self.inner.grpc_request_duration_us[idx].fetch_add(duration_us, Ordering::Relaxed);
    }

    /// 记录一次按完整方法路径的 gRPC 请求（P1-09：MetricsLayer 调用）。
    ///
    /// `code` 为 HTTP 状态码（gRPC 错误响应非 2xx）；慢请求（> SLOW_REQUEST_US）
    /// 额外 WARN 日志。
    pub fn record_grpc_request_by_method(&self, method: &str, duration_us: u64, code: u16) {
        let mm = {
            let mut map = self.inner.method_metrics.write();
            Arc::clone(
                map.entry(method.to_string())
                    .or_insert_with(|| Arc::new(MethodMetrics::default())),
            )
        };
        mm.count.fetch_add(1, Ordering::Relaxed);
        mm.duration_us.fetch_add(duration_us, Ordering::Relaxed);
        if code >= 400 {
            mm.errors.fetch_add(1, Ordering::Relaxed);
        }
        if duration_us > SLOW_REQUEST_US {
            mm.slow.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                "slow gRPC request: method={method} duration={}ms",
                duration_us / 1000
            );
        }
    }

    // ── Storage 指标更新 ──

    pub fn set_storage_size_bytes(&self, bytes: u64) {
        self.inner
            .storage_size_bytes
            .store(bytes, Ordering::Relaxed);
    }

    pub fn set_storage_keys_total(&self, count: u64) {
        self.inner
            .storage_keys_total
            .store(count, Ordering::Relaxed);
    }

    // ── 磁盘水位（P1-02）──

    pub fn set_disk_available_bytes(&self, bytes: u64) {
        self.inner
            .disk_available_bytes
            .store(bytes, Ordering::Relaxed);
    }

    pub fn set_disk_total_bytes(&self, bytes: u64) {
        self.inner.disk_total_bytes.store(bytes, Ordering::Relaxed);
    }

    // ── Lease 指标更新 ──

    pub fn inc_lease_active(&self) {
        self.inner
            .lease_active_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_lease_active(&self) {
        self.inner
            .lease_active_total
            .fetch_sub(1, Ordering::Relaxed);
    }

    pub fn inc_lease_expired(&self) {
        self.inner
            .lease_expired_total
            .fetch_add(1, Ordering::Relaxed);
    }

    // ── Seal 指标 ──

    pub fn set_seal_status(&self, status: i64) {
        self.inner.seal_status.store(status, Ordering::Relaxed);
    }

    // ── Multi-Raft 指标（v6.0） ──

    /// 设置集群 Region 总数
    pub fn set_regions_total(&self, count: u64) {
        self.inner.regions_total.store(count, Ordering::Relaxed);
    }

    /// 设置在线节点数
    pub fn set_nodes_online(&self, count: u64) {
        self.inner.nodes_online.store(count, Ordering::Relaxed);
    }

    /// Region Split 计数 +1
    pub fn inc_region_split(&self) {
        self.inner
            .region_split_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Region Merge 计数 +1
    pub fn inc_region_merge(&self) {
        self.inner
            .region_merge_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// PD Operator 计数 +1
    pub fn inc_pd_operator(&self) {
        self.inner.pd_operator_total.fetch_add(1, Ordering::Relaxed);
    }

    /// 设置本节点 Leader 数量
    pub fn set_local_leader_count(&self, count: u64) {
        self.inner
            .local_leader_count
            .store(count, Ordering::Relaxed);
    }

    /// 设置本节点 Region 副本数
    pub fn set_local_region_count(&self, count: u64) {
        self.inner
            .local_region_count
            .store(count, Ordering::Relaxed);
    }

    /// 获取或创建 Per-Region 指标
    pub fn get_or_create_region_metrics(&self, region_id: RegionId) -> Arc<RegionMetrics> {
        let metrics = self.inner.region_metrics.read();
        if let Some(m) = metrics.iter().find(|m| m.region_id == region_id) {
            return Arc::clone(m);
        }
        drop(metrics);

        let rm = Arc::new(RegionMetrics::new(region_id));
        let mut metrics = self.inner.region_metrics.write();
        // 双重检查
        if let Some(m) = metrics.iter().find(|m| m.region_id == region_id) {
            return Arc::clone(m);
        }
        metrics.push(Arc::clone(&rm));
        rm
    }

    /// 清除指定 Region 的指标（Region 被合并/删除时调用）
    pub fn remove_region_metrics(&self, region_id: RegionId) {
        let mut metrics = self.inner.region_metrics.write();
        metrics.retain(|m| m.region_id != region_id);
    }

    /// 导出所有 Per-Region 指标的 Prometheus 文本
    fn render_region_metrics(&self) -> String {
        let mut out = String::new();
        let metrics = self.inner.region_metrics.read();

        out.push_str("\n# HELP coord_region_size_bytes Region data size in bytes\n");
        out.push_str("# TYPE coord_region_size_bytes gauge\n");
        for m in metrics.iter() {
            out.push_str(&format!(
                "coord_region_size_bytes{{region_id=\"{}\"}} {}\n",
                m.region_id,
                m.size_bytes.load(Ordering::Relaxed)
            ));
        }

        out.push_str("\n# HELP coord_region_keys_total Region key count\n");
        out.push_str("# TYPE coord_region_keys_total gauge\n");
        for m in metrics.iter() {
            out.push_str(&format!(
                "coord_region_keys_total{{region_id=\"{}\"}} {}\n",
                m.region_id,
                m.keys_total.load(Ordering::Relaxed)
            ));
        }

        out.push_str("\n# HELP coord_region_is_leader 1 if this node is the region leader\n");
        out.push_str("# TYPE coord_region_is_leader gauge\n");
        for m in metrics.iter() {
            out.push_str(&format!(
                "coord_region_is_leader{{region_id=\"{}\"}} {}\n",
                m.region_id,
                m.is_leader.load(Ordering::Relaxed)
            ));
        }

        out.push_str("\n# HELP coord_region_raft_log_index Raft commit index per region\n");
        out.push_str("# TYPE coord_region_raft_log_index gauge\n");
        for m in metrics.iter() {
            out.push_str(&format!(
                "coord_region_raft_log_index{{region_id=\"{}\"}} {}\n",
                m.region_id,
                m.raft_log_index.load(Ordering::Relaxed)
            ));
        }

        out
    }

    // ── 导出 Prometheus 文本格式 ──

    /// 生成 Prometheus 文本格式的指标输出
    pub fn render_prometheus_text(&self) -> String {
        let inner = &self.inner;
        let uptime = inner.start_time.elapsed().as_secs_f64();
        let mut out = String::with_capacity(2048);

        // HELP/TYPE + metrics
        out.push_str("# HELP coord_uptime_seconds Server uptime in seconds\n");
        out.push_str("# TYPE coord_uptime_seconds gauge\n");
        out.push_str(&format!("coord_uptime_seconds {:.3}\n", uptime));

        out.push_str("\n# HELP raft_leader_id Current Raft leader ID (0 = no leader)\n");
        out.push_str("# TYPE raft_leader_id gauge\n");
        out.push_str(&format!(
            "raft_leader_id {}\n",
            inner.raft_leader_id.load(Ordering::Relaxed)
        ));

        out.push_str("\n# HELP raft_term Current Raft term\n");
        out.push_str("# TYPE raft_term gauge\n");
        out.push_str(&format!(
            "raft_term {}\n",
            inner.raft_term.load(Ordering::Relaxed)
        ));

        out.push_str("\n# HELP raft_commit_index Raft log commit index\n");
        out.push_str("# TYPE raft_commit_index gauge\n");
        out.push_str(&format!(
            "raft_commit_index {}\n",
            inner.raft_commit_index.load(Ordering::Relaxed)
        ));

        out.push_str("\n# HELP raft_applied_index Raft log applied index\n");
        out.push_str("# TYPE raft_applied_index gauge\n");
        out.push_str(&format!(
            "raft_applied_index {}\n",
            inner.raft_applied_index.load(Ordering::Relaxed)
        ));

        // gRPC 请求计数
        out.push_str("\n# HELP grpc_requests_total Total gRPC requests by method\n");
        out.push_str("# TYPE grpc_requests_total counter\n");
        let method_names = ["put", "range", "delete", "txn", "status"];
        for (i, name) in method_names.iter().enumerate() {
            out.push_str(&format!(
                "grpc_requests_total{{method=\"{}\"}} {}\n",
                name,
                inner.grpc_requests_total[i].load(Ordering::Relaxed)
            ));
        }

        // gRPC 延迟
        out.push_str(
            "\n# HELP grpc_request_duration_us_total Total gRPC request duration in microseconds\n",
        );
        out.push_str("# TYPE grpc_request_duration_us_total counter\n");
        for (i, name) in method_names.iter().enumerate() {
            out.push_str(&format!(
                "grpc_request_duration_us_total{{method=\"{}\"}} {}\n",
                name,
                inner.grpc_request_duration_us[i].load(Ordering::Relaxed)
            ));
        }

        // Storage
        out.push_str("\n# HELP storage_size_bytes Storage size on disk in bytes\n");
        out.push_str("# TYPE storage_size_bytes gauge\n");
        out.push_str(&format!(
            "storage_size_bytes {}\n",
            inner.storage_size_bytes.load(Ordering::Relaxed)
        ));

        out.push_str("\n# HELP storage_keys_total Total number of keys\n");
        out.push_str("# TYPE storage_keys_total gauge\n");
        out.push_str(&format!(
            "storage_keys_total {}\n",
            inner.storage_keys_total.load(Ordering::Relaxed)
        ));

        // 磁盘水位（P1-02）
        out.push_str("\n# HELP disk_available_bytes Available bytes on the data volume\n");
        out.push_str("# TYPE disk_available_bytes gauge\n");
        out.push_str(&format!(
            "disk_available_bytes {}\n",
            inner.disk_available_bytes.load(Ordering::Relaxed)
        ));

        out.push_str("\n# HELP disk_total_bytes Total bytes on the data volume\n");
        out.push_str("# TYPE disk_total_bytes gauge\n");
        out.push_str(&format!(
            "disk_total_bytes {}\n",
            inner.disk_total_bytes.load(Ordering::Relaxed)
        ));

        // Lease
        out.push_str("\n# HELP lease_active_total Number of active leases\n");
        out.push_str("# TYPE lease_active_total gauge\n");
        out.push_str(&format!(
            "lease_active_total {}\n",
            inner.lease_active_total.load(Ordering::Relaxed)
        ));

        out.push_str("\n# HELP lease_expired_total Total expired leases\n");
        out.push_str("# TYPE lease_expired_total counter\n");
        out.push_str(&format!(
            "lease_expired_total {}\n",
            inner.lease_expired_total.load(Ordering::Relaxed)
        ));

        // Seal
        out.push_str("\n# HELP seal_status Seal status (0=unsealed, 1=in_progress, 2=sealed)\n");
        out.push_str("# TYPE seal_status gauge\n");
        out.push_str(&format!(
            "seal_status {}\n",
            inner.seal_status.load(Ordering::Relaxed)
        ));

        // Multi-Raft 全局指标（v6.0）
        out.push_str("\n# HELP coord_regions_total Total number of regions in cluster\n");
        out.push_str("# TYPE coord_regions_total gauge\n");
        out.push_str(&format!(
            "coord_regions_total {}\n",
            inner.regions_total.load(Ordering::Relaxed)
        ));

        out.push_str("\n# HELP coord_nodes_online Number of online nodes\n");
        out.push_str("# TYPE coord_nodes_online gauge\n");
        out.push_str(&format!(
            "coord_nodes_online {}\n",
            inner.nodes_online.load(Ordering::Relaxed)
        ));

        out.push_str("\n# HELP coord_region_split_total Total region splits\n");
        out.push_str("# TYPE coord_region_split_total counter\n");
        out.push_str(&format!(
            "coord_region_split_total {}\n",
            inner.region_split_total.load(Ordering::Relaxed)
        ));

        out.push_str("\n# HELP coord_region_merge_total Total region merges\n");
        out.push_str("# TYPE coord_region_merge_total counter\n");
        out.push_str(&format!(
            "coord_region_merge_total {}\n",
            inner.region_merge_total.load(Ordering::Relaxed)
        ));

        out.push_str("\n# HELP coord_pd_operator_total Total PD scheduling operations\n");
        out.push_str("# TYPE coord_pd_operator_total counter\n");
        out.push_str(&format!(
            "coord_pd_operator_total {}\n",
            inner.pd_operator_total.load(Ordering::Relaxed)
        ));

        out.push_str("\n# HELP coord_local_leader_count Leader count on this node\n");
        out.push_str("# TYPE coord_local_leader_count gauge\n");
        out.push_str(&format!(
            "coord_local_leader_count {}\n",
            inner.local_leader_count.load(Ordering::Relaxed)
        ));

        out.push_str("\n# HELP coord_local_region_count Region replica count on this node\n");
        out.push_str("# TYPE coord_local_region_count gauge\n");
        out.push_str(&format!(
            "coord_local_region_count {}\n",
            inner.local_region_count.load(Ordering::Relaxed)
        ));

        // Per-Region 指标
        out.push_str(&self.render_region_metrics());

        // Per-Method gRPC 指标（P1-09）
        {
            let methods: Vec<(String, Arc<MethodMetrics>)> = {
                let map = inner.method_metrics.read();
                let mut v: Vec<(String, Arc<MethodMetrics>)> = map
                    .iter()
                    .map(|(k, v)| (k.clone(), Arc::clone(v)))
                    .collect();
                v.sort_by(|a, b| a.0.cmp(&b.0));
                v
            };
            out.push_str(
                "\n# HELP grpc_method_requests_total Total gRPC requests by full method path\n",
            );
            out.push_str("# TYPE grpc_method_requests_total counter\n");
            for (name, m) in &methods {
                out.push_str(&format!(
                    "grpc_method_requests_total{{method=\"{name}\"}} {}\n",
                    m.count.load(Ordering::Relaxed)
                ));
            }
            out.push_str(
                "\n# HELP grpc_method_request_duration_us_total Total duration by method\n",
            );
            out.push_str("# TYPE grpc_method_request_duration_us_total counter\n");
            for (name, m) in &methods {
                out.push_str(&format!(
                    "grpc_method_request_duration_us_total{{method=\"{name}\"}} {}\n",
                    m.duration_us.load(Ordering::Relaxed)
                ));
            }
            out.push_str("\n# HELP grpc_method_errors_total Non-OK responses by method\n");
            out.push_str("# TYPE grpc_method_errors_total counter\n");
            for (name, m) in &methods {
                out.push_str(&format!(
                    "grpc_method_errors_total{{method=\"{name}\"}} {}\n",
                    m.errors.load(Ordering::Relaxed)
                ));
            }
            out.push_str(
                "\n# HELP grpc_method_slow_requests_total Slow requests (>1s) by method\n",
            );
            out.push_str("# TYPE grpc_method_slow_requests_total counter\n");
            for (name, m) in &methods {
                out.push_str(&format!(
                    "grpc_method_slow_requests_total{{method=\"{name}\"}} {}\n",
                    m.slow.load(Ordering::Relaxed)
                ));
            }
        }

        out
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

// ──── MetricsLayer（P1-09：tower 中间件，gRPC 全方法指标接线）────

/// gRPC 指标中间件：对每个请求记录（方法路径、耗时、HTTP 状态码）。
///
/// 挂载于 `tonic::Server::builder().layer(MetricsLayer::new(metrics))`，
/// 覆盖全部 gRPC 服务（KV/Txn/Lease/Watch/Maintenance/Auth/Capability），
/// 修复"`record_grpc_request` 零调用方、`/metrics` 恒 0"的 OBS-1 问题。
#[derive(Clone)]
pub struct MetricsLayer {
    metrics: Arc<Metrics>,
}

impl MetricsLayer {
    pub fn new(metrics: Arc<Metrics>) -> Self {
        Self { metrics }
    }
}

impl<S> tower::Layer<S> for MetricsLayer {
    type Service = MetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MetricsService {
            inner,
            metrics: Arc::clone(&self.metrics),
        }
    }
}

/// MetricsLayer 的 Service 包装
#[derive(Clone)]
pub struct MetricsService<S> {
    inner: S,
    metrics: Arc<Metrics>,
}

impl<S, ReqBody, ResBody> tower::Service<http::Request<ReqBody>> for MetricsService<S>
where
    S: tower::Service<http::Request<ReqBody>, Response = http::Response<ResBody>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Send + 'static,
{
    type Response = http::Response<ResBody>;
    type Error = S::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<ReqBody>) -> Self::Future {
        let method = request.uri().path().to_string();
        let start = Instant::now();
        let metrics = Arc::clone(&self.metrics);
        let mut inner = self.inner.clone();
        Box::pin(async move {
            let response: Result<http::Response<ResBody>, S::Error> = match inner.ready().await {
                Ok(svc) => svc.call(request).await,
                Err(e) => Err(e),
            };
            let code = match &response {
                Ok(resp) => resp.status().as_u16(),
                Err(_) => 500,
            };
            metrics.record_grpc_request_by_method(
                &method,
                start.elapsed().as_micros() as u64,
                code,
            );
            response
        })
    }
}

// ──── 测试 ────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_basic() {
        let m = Metrics::new();
        m.record_grpc_request(GrpcMethod::Put, 100);
        m.record_grpc_request(GrpcMethod::Put, 200);
        m.set_raft_term(5);
        m.inc_lease_active();
        m.inc_lease_expired();

        let output = m.render_prometheus_text();
        assert!(output.contains("raft_term 5"));
        assert!(output.contains("grpc_requests_total{method=\"put\"} 2"));
        assert!(output.contains("lease_active_total 1"));
        assert!(output.contains("lease_expired_total 1"));
    }

    #[test]
    fn test_metrics_grpc_methods() {
        let m = Metrics::new();
        m.record_grpc_request(GrpcMethod::Range, 50);
        m.record_grpc_request(GrpcMethod::Txn, 150);
        m.record_grpc_request(GrpcMethod::Delete, 75);

        let output = m.render_prometheus_text();
        assert!(output.contains("grpc_requests_total{method=\"range\"} 1"));
        assert!(output.contains("grpc_requests_total{method=\"txn\"} 1"));
        assert!(output.contains("grpc_requests_total{method=\"delete\"} 1"));
        assert!(output.contains("grpc_requests_total{method=\"put\"} 0"));
    }

    #[test]
    fn test_metrics_raft() {
        let m = Metrics::new();
        m.set_raft_leader_id(3);
        m.set_raft_term(42);
        m.set_raft_commit_index(100);
        m.set_raft_applied_index(99);

        let output = m.render_prometheus_text();
        assert!(output.contains("raft_leader_id 3"));
        assert!(output.contains("raft_term 42"));
        assert!(output.contains("raft_commit_index 100"));
        assert!(output.contains("raft_applied_index 99"));
    }

    #[test]
    fn test_metrics_seal() {
        let m = Metrics::new();
        m.set_seal_status(2);

        let output = m.render_prometheus_text();
        assert!(output.contains("seal_status 2"));
    }

    #[test]
    fn test_metrics_lease_dec() {
        let m = Metrics::new();
        m.inc_lease_active();
        m.inc_lease_active();
        m.dec_lease_active();

        let output = m.render_prometheus_text();
        assert!(output.contains("lease_active_total 1"));
    }

    #[test]
    fn test_metrics_multi_raft_global() {
        let m = Metrics::new();
        m.set_regions_total(42);
        m.set_nodes_online(3);
        m.inc_region_split();
        m.inc_region_split();
        m.inc_region_merge();
        m.inc_pd_operator();
        m.set_local_leader_count(5);
        m.set_local_region_count(10);

        let output = m.render_prometheus_text();
        assert!(output.contains("coord_regions_total 42"));
        assert!(output.contains("coord_nodes_online 3"));
        assert!(output.contains("coord_region_split_total 2"));
        assert!(output.contains("coord_region_merge_total 1"));
        assert!(output.contains("coord_pd_operator_total 1"));
        assert!(output.contains("coord_local_leader_count 5"));
        assert!(output.contains("coord_local_region_count 10"));
    }

    #[test]
    fn test_metrics_per_region() {
        let m = Metrics::new();

        let rm1 = m.get_or_create_region_metrics(1);
        rm1.size_bytes.store(1024 * 1024, Ordering::Relaxed);
        rm1.keys_total.store(5000, Ordering::Relaxed);
        rm1.is_leader.store(1, Ordering::Relaxed);
        rm1.raft_log_index.store(100, Ordering::Relaxed);

        let rm2 = m.get_or_create_region_metrics(2);
        rm2.size_bytes.store(512 * 1024, Ordering::Relaxed);
        rm2.keys_total.store(2000, Ordering::Relaxed);
        rm2.is_leader.store(0, Ordering::Relaxed);
        rm2.raft_log_index.store(95, Ordering::Relaxed);

        let output = m.render_prometheus_text();
        assert!(output.contains("coord_region_size_bytes{region_id=\"1\"} 1048576"));
        assert!(output.contains("coord_region_size_bytes{region_id=\"2\"} 524288"));
        assert!(output.contains("coord_region_is_leader{region_id=\"1\"} 1"));
        assert!(output.contains("coord_region_is_leader{region_id=\"2\"} 0"));
        assert!(output.contains("coord_region_raft_log_index{region_id=\"1\"} 100"));
    }

    #[test]
    fn test_metrics_remove_region() {
        let m = Metrics::new();
        let _ = m.get_or_create_region_metrics(1);
        let _ = m.get_or_create_region_metrics(2);

        m.remove_region_metrics(1);

        let output = m.render_prometheus_text();
        assert!(!output.contains("region_id=\"1\""));
        assert!(output.contains("region_id=\"2\""));
    }

    #[test]
    fn test_metrics_get_or_create_cached() {
        let m = Metrics::new();
        let rm1 = m.get_or_create_region_metrics(1);
        let rm1_again = m.get_or_create_region_metrics(1);
        // 应该复用同一个 Arc 实例
        rm1.size_bytes.store(123, Ordering::Relaxed);
        assert_eq!(rm1_again.size_bytes.load(Ordering::Relaxed), 123);
    }

    #[test]
    fn test_method_metrics_registry() {
        let m = Metrics::new();
        m.record_grpc_request_by_method("/coord.kv.KV/Put", 500_000, 200);
        m.record_grpc_request_by_method("/coord.kv.KV/Put", 2_000_000, 500);
        m.record_grpc_request_by_method("/coord.kv.KV/Range", 10_000, 200);

        let output = m.render_prometheus_text();
        assert!(output.contains("grpc_method_requests_total{method=\"/coord.kv.KV/Put\"} 2"));
        assert!(output.contains("grpc_method_errors_total{method=\"/coord.kv.KV/Put\"} 1"));
        assert!(output.contains("grpc_method_slow_requests_total{method=\"/coord.kv.KV/Put\"} 1"));
        assert!(output.contains("grpc_method_requests_total{method=\"/coord.kv.KV/Range\"} 1"));
        assert!(output.contains("grpc_method_errors_total{method=\"/coord.kv.KV/Range\"} 0"));
    }
}
