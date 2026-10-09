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
// - agent_cache_active_bytes / agent_cache_limit_bytes: 缓存活跃字节与上界（B-PL-3）
// - agent_cache_reaped_entries_total / agent_cache_evicted_entries_total /
//   agent_cache_reaper_faults_total: reaper 回收/淘汰/失败累计（B-PL-3）
// - agent_mq_active_bytes / agent_mq_limit_bytes: MQ 活跃字节与上界（B-PL-4）
// - agent_mq_purged_entries_total / agent_mq_publish_rejected_total /
//   agent_mq_reaper_faults_total: 保留回收/配额拒绝/失败累计（B-PL-4）

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
    /// 工作流后台 worker 的在飞数（`WorkerLiveness::live_oneshot`）
    pub workflow_workers_live: AtomicI64,
    /// 工作流后台 worker 未正常收尾的**累计**次数（单调 ⇒ counter 语义）
    pub workflow_worker_faults: AtomicU64,
    /// 已结束的**循环型** worker 数（>0 =  本该永不结束的循环死了）
    pub workflow_loops_finished: AtomicI64,
    /// 出站凭据健康度（0 = 已失效/恢复中；1 = 有效）
    pub credential_alive: AtomicI64,
    /// 凭据恢复尝试次数（单调；与 `credential_recoveries` 同读：
    /// attempts 持续增长而 recoveries 不增长 = 凭据持续死亡）
    pub credential_recovery_attempts: AtomicU64,
    /// 凭据恢复成功次数（单调）
    pub credential_recoveries: AtomicU64,
    /// 缓存活跃字节（记账口径；B-PL-3）
    pub cache_active_bytes: AtomicU64,
    /// 缓存容量上界（字节；0 = 不限）
    pub cache_limit_bytes: AtomicU64,
    /// 缓存 reaper 累计 TTL 过期回收条目数（单调）
    pub cache_reaped_entries: AtomicU64,
    /// 缓存 reaper 累计超界淘汰条目数（单调）
    pub cache_evicted_entries: AtomicU64,
    /// 缓存 reaper 累计失败轮数（单调；>0 = 至少有一轮回收失败）
    pub cache_reaper_faults: AtomicU64,
    /// MQ 活跃字节（记账口径；B-PL-4）
    pub mq_active_bytes: AtomicU64,
    /// MQ 容量上界（字节；0 = 不限）
    pub mq_limit_bytes: AtomicU64,
    /// MQ reaper 累计保留回收条目数（消息 + DLQ；单调）
    pub mq_purged_entries: AtomicU64,
    /// MQ publish 因配额被拒绝的累计次数（单调；背压生效可观测）
    pub mq_publish_rejected: AtomicU64,
    /// MQ reaper 累计失败轮数（单调；>0 = 至少有一轮回收失败）
    pub mq_reaper_faults: AtomicU64,
    /// 消费组已提交位点：(topic, group, partition) → committed offset（G-MQ-3）
    pub mq_consumer_offsets: RwLock<BTreeMap<(String, String, u32), u64>>,
    /// 消费组滞后量：(topic, group, partition) → next_offset - committed（G-MQ-3）
    pub mq_consumer_lags: RwLock<BTreeMap<(String, String, u32), u64>>,
    /// 各 (topic, partition) 的 next offset（已生产条数；G-MQ-3）
    pub mq_next_offsets: RwLock<BTreeMap<(String, u32), u64>>,
    /// 已加载 enabled OPA bundle 数（G-POL-1；gauge）
    pub policy_bundles_loaded: AtomicI64,
    /// 最近一次成功 bundle 加载/对账的 unix 秒（0 = 从未成功；gauge）
    pub policy_bundle_last_sync_unix: AtomicI64,
    /// bundle 加载/对账成功累计（单调）
    pub policy_bundle_sync_ok: AtomicU64,
    /// bundle 加载/对账/事件应用失败累计（单调）
    pub policy_bundle_sync_errors: AtomicU64,
    /// PKI active 证书总数（G-PKI-2；gauge）
    pub pki_certs_active: AtomicI64,
    /// PKI 窗口内即将到期的 active 证书数（G-PKI-2；gauge）
    pub pki_certs_expiring_soon: AtomicI64,
    /// PKI 到期告警窗口（小时；G-PKI-2）
    pub pki_expiry_warn_window_hours: AtomicI64,
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
                workflow_workers_live: AtomicI64::new(0),
                workflow_worker_faults: AtomicU64::new(0),
                workflow_loops_finished: AtomicI64::new(0),
                credential_alive: AtomicI64::new(0),
                credential_recovery_attempts: AtomicU64::new(0),
                credential_recoveries: AtomicU64::new(0),
                cache_active_bytes: AtomicU64::new(0),
                cache_limit_bytes: AtomicU64::new(0),
                cache_reaped_entries: AtomicU64::new(0),
                cache_evicted_entries: AtomicU64::new(0),
                cache_reaper_faults: AtomicU64::new(0),
                mq_active_bytes: AtomicU64::new(0),
                mq_limit_bytes: AtomicU64::new(0),
                mq_purged_entries: AtomicU64::new(0),
                mq_publish_rejected: AtomicU64::new(0),
                mq_reaper_faults: AtomicU64::new(0),
                mq_consumer_offsets: RwLock::new(BTreeMap::new()),
                mq_consumer_lags: RwLock::new(BTreeMap::new()),
                mq_next_offsets: RwLock::new(BTreeMap::new()),
                policy_bundles_loaded: AtomicI64::new(0),
                policy_bundle_last_sync_unix: AtomicI64::new(0),
                policy_bundle_sync_ok: AtomicU64::new(0),
                policy_bundle_sync_errors: AtomicU64::new(0),
                pki_certs_active: AtomicI64::new(0),
                pki_certs_expiring_soon: AtomicI64::new(0),
                pki_expiry_warn_window_hours: AtomicI64::new(0),
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

    /// 写入工作流后台 worker 的存活事实（由采样任务周期性调用）。
    ///
    /// 三个值语义不同，**不要**合成一个：
    /// * `live` —— 在飞的一次性任务数（正常波动）；
    /// * `faults_total` —— 未正常收尾的**累计**次数（单调，counter）；
    /// * `finished_loops` —— 已结束的**循环型** worker 数（本该恒为 0，c>0 即缺陷）。
    ///
    /// 为什么要分：把"正常结束"也计进故障会让指标长期噪声化，最终没人看
    /// （见 `coord_core::workflow::runtime::WorkerLiveness` 的讨论）。
    pub fn set_workflow_worker_liveness(&self, live: i64, faults_total: u64, finished_loops: i64) {
        self.inner
            .workflow_workers_live
            .store(live, Ordering::Relaxed);
        self.inner
            .workflow_worker_faults
            .store(faults_total, Ordering::Relaxed);
        self.inner
            .workflow_loops_finished
            .store(finished_loops, Ordering::Relaxed);
    }

    // ──── 出站凭据健康（能力死亡必须可观测）────

    /// 记录一次凭据恢复尝试（每次都对应「刷新失败 → 恢复」事件的一轮尝试）。
    pub fn record_credential_recovery_attempt(&self) {
        self.inner
            .credential_recovery_attempts
            .fetch_add(1, Ordering::Relaxed);
    }

    /// 记录一次凭据恢复成功。
    pub fn record_credential_recovered(&self) {
        self.inner
            .credential_recoveries
            .fetch_add(1, Ordering::Relaxed);
    }

    /// 写入凭据健康度（`false` = 已失效/恢复中；`true` = 有效）。
    ///
    /// 告警口径：`coord_agent_outbound_credential_alive == 0` 持续超过阈值
    /// （或 `recovery_attempts_total` 持续增长而 `recoveries_total` 不增长）
    /// ⇒ 自流量能力（锁续期 / 角色同步 / registry 订阅等）正在死亡。
    pub fn set_credential_alive(&self, alive: bool) {
        self.inner
            .credential_alive
            .store(if alive { 1 } else { 0 }, Ordering::Relaxed);
    }

    /// 写入缓存容量上界事实（B-PL-3）；由周期采样任务从 CacheService 拉取。
    ///
    /// 语义：
    /// - `active_bytes > limit_bytes`（且 limit > 0）说明处于 reaper 周期内的
    ///   短暂超界窗口 —— 周期收敛是**已承诺语义**（不是缺陷）；
    /// - `faults_total` 单调：>0 表示至少有一轮回收失败（缓存可能持续超界）。
    pub fn set_cache_reaper_stats(
        &self,
        active_bytes: u64,
        limit_bytes: u64,
        reaped_entries_total: u64,
        evicted_entries_total: u64,
        faults_total: u64,
    ) {
        self.inner
            .cache_active_bytes
            .store(active_bytes, Ordering::Relaxed);
        self.inner
            .cache_limit_bytes
            .store(limit_bytes, Ordering::Relaxed);
        self.inner
            .cache_reaped_entries
            .store(reaped_entries_total, Ordering::Relaxed);
        self.inner
            .cache_evicted_entries
            .store(evicted_entries_total, Ordering::Relaxed);
        self.inner
            .cache_reaper_faults
            .store(faults_total, Ordering::Relaxed);
    }

    /// 写入 MQ 容量上界事实（B-PL-4）；由周期采样任务从 MessageQueueService 拉取。
    ///
    /// 语义：
    /// - `active_bytes ≤ limit_bytes`（limit > 0）是写路径强制的**严格不变量**
    ///   （与 cache 的周期收敛不同：超界发生在 publish 之前而不是事后再收敛）；
    /// - `publish_rejected_total` 单调：>0 表示至少一次 publish 因配额被拒
    ///   （背压生效 —— 消费/保留回收跟不上生产）；
    /// - `faults_total` 单调：>0 表示至少有一轮回收失败。
    pub fn set_mq_reaper_stats(
        &self,
        active_bytes: u64,
        limit_bytes: u64,
        purged_entries_total: u64,
        publish_rejected_total: u64,
        faults_total: u64,
    ) {
        self.inner
            .mq_active_bytes
            .store(active_bytes, Ordering::Relaxed);
        self.inner
            .mq_limit_bytes
            .store(limit_bytes, Ordering::Relaxed);
        self.inner
            .mq_purged_entries
            .store(purged_entries_total, Ordering::Relaxed);
        self.inner
            .mq_publish_rejected
            .store(publish_rejected_total, Ordering::Relaxed);
        self.inner
            .mq_reaper_faults
            .store(faults_total, Ordering::Relaxed);
    }

    /// 写入 OPA bundle 分发态（G-POL-1）；由周期采样任务从 PolicyService 拉取。
    ///
    /// 语义：`loaded` = 本地引擎已加载的 enabled bundle 数；
    /// `last_sync_unix` = 最近一次成功加载/对账时间（0 = 从未成功）；
    /// `errors_total` 持续增长而 `ok_total` 不增长 = watch/对账持续失败。
    pub fn set_policy_bundle_stats(
        &self,
        loaded: i64,
        last_sync_unix: i64,
        ok_total: u64,
        errors_total: u64,
    ) {
        self.inner
            .policy_bundles_loaded
            .store(loaded, Ordering::Relaxed);
        self.inner
            .policy_bundle_last_sync_unix
            .store(last_sync_unix, Ordering::Relaxed);
        self.inner
            .policy_bundle_sync_ok
            .store(ok_total, Ordering::Relaxed);
        self.inner
            .policy_bundle_sync_errors
            .store(errors_total, Ordering::Relaxed);
    }

    /// 写入 MQ 消费组位点/滞后快照（G-MQ-3）；由周期采样任务从
    /// MessageQueueService 拉取。
    ///
    /// `consumers` = (topic, group, partition, committed_offset, lag)；
    /// `next_offsets` = (topic, partition, next_offset)。
    /// lag = next_offset - committed（同分区上下文；无生产记录时取 0 下限）。
    pub fn set_mq_consumer_lag_stats(
        &self,
        consumers: Vec<(String, String, u32, u64, u64)>,
        next_offsets: Vec<(String, u32, u64)>,
    ) {
        {
            let mut offsets = self.inner.mq_consumer_offsets.write();
            let mut lags = self.inner.mq_consumer_lags.write();
            offsets.clear();
            lags.clear();
            for (topic, group, partition, offset, lag) in consumers {
                let key = (topic, group, partition);
                offsets.insert(key.clone(), offset);
                lags.insert(key, lag);
            }
        }
        {
            let mut next = self.inner.mq_next_offsets.write();
            next.clear();
            for (topic, partition, offset) in next_offsets {
                next.insert((topic, partition), offset);
            }
        }
    }

    /// 写入 PKI 到期观测（G-PKI-2）；由周期采样任务从 PkiService 拉取。
    ///
    /// `expiring_soon` = 仍有效且剩余有效期 < `window_hours` 的 active 证书数。
    pub fn set_pki_cert_stats(&self, total: i64, expiring_soon: i64, window_hours: i64) {
        self.inner.pki_certs_active.store(total, Ordering::Relaxed);
        self.inner
            .pki_certs_expiring_soon
            .store(expiring_soon, Ordering::Relaxed);
        self.inner
            .pki_expiry_warn_window_hours
            .store(window_hours, Ordering::Relaxed);
    }

    // ──── 插件指标（观测面）────

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

        // ──── 工作流后台 worker 存活（能力死亡必须可观测）────
        out.push_str(
            "# HELP coord_agent_workflow_workers_live In-flight workflow background tasks (drive)\n",
        );
        out.push_str("# TYPE coord_agent_workflow_workers_live gauge\n");
        out.push_str(&format!(
            "coord_agent_workflow_workers_live {}\n",
            self.inner.workflow_workers_live.load(Ordering::Relaxed)
        ));
        out.push_str(
            "# HELP coord_agent_workflow_worker_faults_total Workflow background tasks that ended \
             without completing (panic/abort); monotonic\n",
        );
        out.push_str("# TYPE coord_agent_workflow_worker_faults_total counter\n");
        out.push_str(&format!(
            "coord_agent_workflow_worker_faults_total {}\n",
            self.inner.workflow_worker_faults.load(Ordering::Relaxed)
        ));
        out.push_str(
            "# HELP coord_agent_workflow_loops_finished Long-lived workflow loops that ended \
             (should always be 0)\n",
        );
        out.push_str("# TYPE coord_agent_workflow_loops_finished gauge\n");
        out.push_str(&format!(
            "coord_agent_workflow_loops_finished {}\n",
            self.inner.workflow_loops_finished.load(Ordering::Relaxed)
        ));

        // ──── 出站凭据健康（能力死亡必须可观测）────
        out.push_str(
            "# HELP coord_agent_outbound_credential_alive 1 if the agent outbound credential \
             is valid, 0 if it is dead/recovering\n",
        );
        out.push_str("# TYPE coord_agent_outbound_credential_alive gauge\n");
        out.push_str(&format!(
            "coord_agent_outbound_credential_alive {}\n",
            self.inner.credential_alive.load(Ordering::Relaxed)
        ));
        out.push_str(
            "# HELP coord_agent_outbound_credential_recovery_attempts_total Session recovery \
             attempts after credential failures; monotonic\n",
        );
        out.push_str("# TYPE coord_agent_outbound_credential_recovery_attempts_total counter\n");
        out.push_str(&format!(
            "coord_agent_outbound_credential_recovery_attempts_total {}\n",
            self.inner
                .credential_recovery_attempts
                .load(Ordering::Relaxed)
        ));
        out.push_str(
            "# HELP coord_agent_outbound_credential_recoveries_total Successful credential \
             recoveries; monotonic\n",
        );
        out.push_str("# TYPE coord_agent_outbound_credential_recoveries_total counter\n");
        out.push_str(&format!(
            "coord_agent_outbound_credential_recoveries_total {}\n",
            self.inner.credential_recoveries.load(Ordering::Relaxed)
        ));

        // ──── 缓存容量上界（B-PL-3；周期采样自 CacheService）────
        out.push_str(
            "# HELP coord_agent_cache_active_bytes Accounted active cache bytes (data tables)\n",
        );
        out.push_str("# TYPE coord_agent_cache_active_bytes gauge\n");
        out.push_str(&format!(
            "coord_agent_cache_active_bytes {}\n",
            self.inner.cache_active_bytes.load(Ordering::Relaxed)
        ));
        out.push_str(
            "# HELP coord_agent_cache_limit_bytes Configured cache capacity limit in bytes \
             (0 = unlimited)\n",
        );
        out.push_str("# TYPE coord_agent_cache_limit_bytes gauge\n");
        out.push_str(&format!(
            "coord_agent_cache_limit_bytes {}\n",
            self.inner.cache_limit_bytes.load(Ordering::Relaxed)
        ));
        out.push_str(
            "# HELP coord_agent_cache_reaped_entries_total TTL-expired cache entries reclaimed \
             by the reaper; monotonic\n",
        );
        out.push_str("# TYPE coord_agent_cache_reaped_entries_total counter\n");
        out.push_str(&format!(
            "coord_agent_cache_reaped_entries_total {}\n",
            self.inner.cache_reaped_entries.load(Ordering::Relaxed)
        ));
        out.push_str(
            "# HELP coord_agent_cache_evicted_entries_total Cache entries evicted to enforce \
             max_size_bytes; monotonic\n",
        );
        out.push_str("# TYPE coord_agent_cache_evicted_entries_total counter\n");
        out.push_str(&format!(
            "coord_agent_cache_evicted_entries_total {}\n",
            self.inner.cache_evicted_entries.load(Ordering::Relaxed)
        ));
        out.push_str(
            "# HELP coord_agent_cache_reaper_faults_total Failed cache reaper passes; monotonic\n",
        );
        out.push_str("# TYPE coord_agent_cache_reaper_faults_total counter\n");
        out.push_str(&format!(
            "coord_agent_cache_reaper_faults_total {}\n",
            self.inner.cache_reaper_faults.load(Ordering::Relaxed)
        ));

        // ──── MQ 容量上界（B-PL-4；周期采样自 MessageQueueService）────
        out.push_str(
            "# HELP coord_agent_mq_active_bytes Accounted active MQ bytes (messages + DLQ)\n",
        );
        out.push_str("# TYPE coord_agent_mq_active_bytes gauge\n");
        out.push_str(&format!(
            "coord_agent_mq_active_bytes {}\n",
            self.inner.mq_active_bytes.load(Ordering::Relaxed)
        ));
        out.push_str(
            "# HELP coord_agent_mq_limit_bytes Configured MQ capacity limit in bytes \
             (0 = unlimited)\n",
        );
        out.push_str("# TYPE coord_agent_mq_limit_bytes gauge\n");
        out.push_str(&format!(
            "coord_agent_mq_limit_bytes {}\n",
            self.inner.mq_limit_bytes.load(Ordering::Relaxed)
        ));
        out.push_str(
            "# HELP coord_agent_mq_purged_entries_total Retention-expired MQ entries \
             (messages + DLQ) purged by the reaper; monotonic\n",
        );
        out.push_str("# TYPE coord_agent_mq_purged_entries_total counter\n");
        out.push_str(&format!(
            "coord_agent_mq_purged_entries_total {}\n",
            self.inner.mq_purged_entries.load(Ordering::Relaxed)
        ));
        out.push_str(
            "# HELP coord_agent_mq_publish_rejected_total MQ publishes rejected by the \
             byte quota (backpressure); monotonic\n",
        );
        out.push_str("# TYPE coord_agent_mq_publish_rejected_total counter\n");
        out.push_str(&format!(
            "coord_agent_mq_publish_rejected_total {}\n",
            self.inner.mq_publish_rejected.load(Ordering::Relaxed)
        ));
        out.push_str(
            "# HELP coord_agent_mq_reaper_faults_total Failed MQ reaper passes; monotonic\n",
        );
        out.push_str("# TYPE coord_agent_mq_reaper_faults_total counter\n");
        out.push_str(&format!(
            "coord_agent_mq_reaper_faults_total {}\n",
            self.inner.mq_reaper_faults.load(Ordering::Relaxed)
        ));

        // ──── MQ 消费组位点 / 滞后（G-MQ-3；周期采样自 MessageQueueService）────
        out.push_str(
            "# HELP coord_agent_mq_consumer_offset Committed consumer-group offset per \
             (topic, group, partition)\n",
        );
        out.push_str("# TYPE coord_agent_mq_consumer_offset gauge\n");
        {
            let offsets = self.inner.mq_consumer_offsets.read();
            for ((topic, group, partition), value) in offsets.iter() {
                out.push_str(&format!(
                    "coord_agent_mq_consumer_offset{{topic=\"{topic}\",group=\"{group}\",\
                     partition=\"{partition}\"}} {value}\n"
                ));
            }
        }
        out.push_str(
            "# HELP coord_agent_mq_consumer_lag Messages behind the producer per \
             (topic, group, partition) = next_offset - committed, floored at 0\n",
        );
        out.push_str("# TYPE coord_agent_mq_consumer_lag gauge\n");
        {
            let lags = self.inner.mq_consumer_lags.read();
            for ((topic, group, partition), value) in lags.iter() {
                out.push_str(&format!(
                    "coord_agent_mq_consumer_lag{{topic=\"{topic}\",group=\"{group}\",\
                     partition=\"{partition}\"}} {value}\n"
                ));
            }
        }
        out.push_str(
            "# HELP coord_agent_mq_next_offset Next offset (produced count) per \
             (topic, partition)\n",
        );
        out.push_str("# TYPE coord_agent_mq_next_offset gauge\n");
        {
            let next = self.inner.mq_next_offsets.read();
            for ((topic, partition), value) in next.iter() {
                out.push_str(&format!(
                    "coord_agent_mq_next_offset{{topic=\"{topic}\",partition=\"{partition}\"}} \
                     {value}\n"
                ));
            }
        }

        // ──── OPA bundle 分发态（G-POL-1；周期采样自 PolicyService）────
        out.push_str(
            "# HELP coord_agent_policy_bundles_loaded Enabled OPA bundles loaded in the \
             local engine\n",
        );
        out.push_str("# TYPE coord_agent_policy_bundles_loaded gauge\n");
        out.push_str(&format!(
            "coord_agent_policy_bundles_loaded {}\n",
            self.inner.policy_bundles_loaded.load(Ordering::Relaxed)
        ));
        out.push_str(
            "# HELP coord_agent_policy_bundle_last_sync_timestamp Unix time of the last \
             successful bundle load/reconcile (0 = never)\n",
        );
        out.push_str("# TYPE coord_agent_policy_bundle_last_sync_timestamp gauge\n");
        out.push_str(&format!(
            "coord_agent_policy_bundle_last_sync_timestamp {}\n",
            self.inner.policy_bundle_last_sync_unix.load(Ordering::Relaxed)
        ));
        out.push_str(
            "# HELP coord_agent_policy_bundle_sync_total Bundle load/reconcile events by result\n",
        );
        out.push_str("# TYPE coord_agent_policy_bundle_sync_total counter\n");
        out.push_str(&format!(
            "coord_agent_policy_bundle_sync_total{{result=\"ok\"}} {}\n",
            self.inner.policy_bundle_sync_ok.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "coord_agent_policy_bundle_sync_total{{result=\"error\"}} {}\n",
            self.inner.policy_bundle_sync_errors.load(Ordering::Relaxed)
        ));

        // ──── PKI 到期观测（G-PKI-2；周期采样自 PkiService）────
        out.push_str(
            "# HELP coord_agent_pki_certs_active Currently active PKI certificates\n",
        );
        out.push_str("# TYPE coord_agent_pki_certs_active gauge\n");
        out.push_str(&format!(
            "coord_agent_pki_certs_active {}\n",
            self.inner.pki_certs_active.load(Ordering::Relaxed)
        ));
        out.push_str(
            "# HELP coord_agent_pki_certs_expiring_soon Active certificates expiring within \
             the configured window (still valid)\n",
        );
        out.push_str("# TYPE coord_agent_pki_certs_expiring_soon gauge\n");
        out.push_str(&format!(
            "coord_agent_pki_certs_expiring_soon {}\n",
            self.inner.pki_certs_expiring_soon.load(Ordering::Relaxed)
        ));
        out.push_str(
            "# HELP coord_agent_pki_expiry_warn_window_hours Configured expiry warning window \
             in hours\n",
        );
        out.push_str("# TYPE coord_agent_pki_expiry_warn_window_hours gauge\n");
        out.push_str(&format!(
            "coord_agent_pki_expiry_warn_window_hours {}\n",
            self.inner.pki_expiry_warn_window_hours.load(Ordering::Relaxed)
        ));

        // ──── 插件指标 ────
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

/// tower Layer：挂到 Agent gRPC router 外层，按方法路径计数 —— 否则
/// `record_grpc_request` 是死代码、`coord_agent_grpc_requests_total` 恒为 0。
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

    /// G-MQ-3 / G-POL-1：新指标必须真的出现在抓取面上（含标签序列）
    #[test]
    fn test_render_mq_consumer_lag_and_policy_bundle_metrics() {
        let m = AgentMetrics::new();
        m.set_mq_consumer_lag_stats(
            vec![(
                "orders".to_string(),
                "cg".to_string(),
                0u32,
                2u64,
                1u64,
            )],
            vec![("orders".to_string(), 0u32, 3u64)],
        );
        m.set_policy_bundle_stats(2, 1_700_000_000, 5, 1);
        let text = m.render_prometheus_text();
        assert!(
            text.contains(
                "coord_agent_mq_consumer_offset{topic=\"orders\",group=\"cg\",partition=\"0\"} 2"
            ),
            "{text}"
        );
        assert!(
            text.contains(
                "coord_agent_mq_consumer_lag{topic=\"orders\",group=\"cg\",partition=\"0\"} 1"
            ),
            "{text}"
        );
        assert!(
            text.contains("coord_agent_mq_next_offset{topic=\"orders\",partition=\"0\"} 3"),
            "{text}"
        );
        assert!(text.contains("coord_agent_policy_bundles_loaded 2"), "{text}");
        assert!(
            text.contains("coord_agent_policy_bundle_last_sync_timestamp 1700000000"),
            "{text}"
        );
        assert!(
            text.contains("coord_agent_policy_bundle_sync_total{result=\"ok\"} 5"),
            "{text}"
        );
        assert!(
            text.contains("coord_agent_policy_bundle_sync_total{result=\"error\"} 1"),
            "{text}"
        );
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

    /// 工作流后台 worker 的三个指标必须真的出现在抓取面上。
    ///
    /// 只测 `set_*` 的存储、不测渲染，是"指标存在但 Prometheus 看不到"的经典漏检
    /// —— 而本条判据的全部意义就是"**能力死亡时有人看得见**"。
    #[test]
    fn test_workflow_worker_liveness_metrics_render() {
        let m = AgentMetrics::new();
        m.set_workflow_worker_liveness(3, 2, 1);
        let text = m.render_prometheus_text();

        assert!(
            text.contains("# TYPE coord_agent_workflow_workers_live gauge"),
            "在飞数必须是 gauge（正常波动，不是累计）：{text}"
        );
        assert!(text.contains("coord_agent_workflow_workers_live 3"));
        assert!(
            text.contains("# TYPE coord_agent_workflow_worker_faults_total counter"),
            "未正常收尾的次数是**单调累计** ⇒ counter（用 increase() 做告警）：{text}"
        );
        assert!(text.contains("coord_agent_workflow_worker_faults_total 2"));
        assert!(
            text.contains("coord_agent_workflow_loops_finished 1"),
            "循环型死亡必须是独立 gauge（与一次性任务口径不同）：{text}"
        );

        // 健康态：全部为 0，且 HELP/TYPE 仍在（Grafana 无数据时不断线）
        let healthy = AgentMetrics::new().render_prometheus_text();
        assert!(healthy.contains("coord_agent_workflow_worker_faults_total 0"));
        assert!(healthy.contains("coord_agent_workflow_loops_finished 0"));
        assert!(healthy.contains("# TYPE coord_agent_workflow_loops_finished gauge"));
    }

    /// 缓存容量上界（B-PL-3）的五个事实必须真的出现在抓取面上：
    /// 与 workflow liveness 同一判据 —— 指标的全部意义是"能力可观测"，
    /// 只测 setter 不测渲染会漏掉"存了但看不到"。
    #[test]
    fn test_cache_reaper_metrics_render() {
        let m = AgentMetrics::new();
        m.set_cache_reaper_stats(2048, 1024 * 1024, 7, 13, 1);
        let text = m.render_prometheus_text();

        assert!(text.contains("# TYPE coord_agent_cache_active_bytes gauge"));
        assert!(text.contains("coord_agent_cache_active_bytes 2048"));
        assert!(text.contains("coord_agent_cache_limit_bytes 1048576"));
        assert!(
            text.contains("# TYPE coord_agent_cache_reaped_entries_total counter"),
            "过期回收条目是单调累计 ⇒ counter：{text}"
        );
        assert!(text.contains("coord_agent_cache_reaped_entries_total 7"));
        assert!(text.contains("# TYPE coord_agent_cache_evicted_entries_total counter"));
        assert!(text.contains("coord_agent_cache_evicted_entries_total 13"));
        assert!(text.contains("coord_agent_cache_reaper_faults_total 1"));

        // 健康态：HELP/TYPE 仍在（Grafana 无数据时不断线），值为 0
        let healthy = AgentMetrics::new().render_prometheus_text();
        assert!(healthy.contains("coord_agent_cache_active_bytes 0"));
        assert!(healthy.contains("coord_agent_cache_reaper_faults_total 0"));
    }

    /// MQ 容量上界（B-PL-4）的五个事实必须真的出现在抓取面上
    /// （与 cache reaper 同一判据：指标的全部意义是"能力可观测"）。
    #[test]
    fn test_mq_reaper_metrics_render() {
        let m = AgentMetrics::new();
        m.set_mq_reaper_stats(512, 1024 * 1024, 9, 3, 1);
        let text = m.render_prometheus_text();

        assert!(text.contains("# TYPE coord_agent_mq_active_bytes gauge"));
        assert!(text.contains("coord_agent_mq_active_bytes 512"));
        assert!(text.contains("coord_agent_mq_limit_bytes 1048576"));
        assert!(
            text.contains("# TYPE coord_agent_mq_purged_entries_total counter"),
            "保留回收条目是单调累计 ⇒ counter：{text}"
        );
        assert!(text.contains("coord_agent_mq_purged_entries_total 9"));
        assert!(
            text.contains("# TYPE coord_agent_mq_publish_rejected_total counter"),
            "配额拒绝次数是单调累计 ⇒ counter：{text}"
        );
        assert!(text.contains("coord_agent_mq_publish_rejected_total 3"));
        assert!(text.contains("coord_agent_mq_reaper_faults_total 1"));

        // 健康态：HELP/TYPE 仍在（Grafana 无数据时不断线），值为 0
        let healthy = AgentMetrics::new().render_prometheus_text();
        assert!(healthy.contains("coord_agent_mq_active_bytes 0"));
        assert!(healthy.contains("coord_agent_mq_reaper_faults_total 0"));
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
