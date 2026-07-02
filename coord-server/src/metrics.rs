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

use coord_core::types::RegionId;
use parking_lot::RwLock;

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

    // ── 启动时间 ──
    pub start_time: Instant,
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
        self.inner.raft_leader_id.store(id as i64, Ordering::Relaxed);
    }

    pub fn set_raft_term(&self, term: u64) {
        self.inner.raft_term.store(term, Ordering::Relaxed);
    }

    pub fn set_raft_commit_index(&self, index: u64) {
        self.inner.raft_commit_index.store(index, Ordering::Relaxed);
    }

    pub fn set_raft_applied_index(&self, index: u64) {
        self.inner.raft_applied_index.store(index, Ordering::Relaxed);
    }

    // ── gRPC 指标更新 ──

    /// 记录一次 gRPC 请求
    pub fn record_grpc_request(&self, method: GrpcMethod, duration_us: u64) {
        let idx = method.as_index();
        self.inner.grpc_requests_total[idx].fetch_add(1, Ordering::Relaxed);
        self.inner.grpc_request_duration_us[idx].fetch_add(duration_us, Ordering::Relaxed);
    }

    // ── Storage 指标更新 ──

    pub fn set_storage_size_bytes(&self, bytes: u64) {
        self.inner.storage_size_bytes.store(bytes, Ordering::Relaxed);
    }

    pub fn set_storage_keys_total(&self, count: u64) {
        self.inner.storage_keys_total.store(count, Ordering::Relaxed);
    }

    // ── Lease 指标更新 ──

    pub fn inc_lease_active(&self) {
        self.inner.lease_active_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_lease_active(&self) {
        self.inner.lease_active_total.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn inc_lease_expired(&self) {
        self.inner.lease_expired_total.fetch_add(1, Ordering::Relaxed);
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
        self.inner.region_split_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Region Merge 计数 +1
    pub fn inc_region_merge(&self) {
        self.inner.region_merge_total.fetch_add(1, Ordering::Relaxed);
    }

    /// PD Operator 计数 +1
    pub fn inc_pd_operator(&self) {
        self.inner.pd_operator_total.fetch_add(1, Ordering::Relaxed);
    }

    /// 设置本节点 Leader 数量
    pub fn set_local_leader_count(&self, count: u64) {
        self.inner.local_leader_count.store(count, Ordering::Relaxed);
    }

    /// 设置本节点 Region 副本数
    pub fn set_local_region_count(&self, count: u64) {
        self.inner.local_region_count.store(count, Ordering::Relaxed);
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
        out.push_str(&format!("raft_term {}\n", inner.raft_term.load(Ordering::Relaxed)));

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
        out.push_str("\n# HELP grpc_request_duration_us_total Total gRPC request duration in microseconds\n");
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

        out
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
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
}
