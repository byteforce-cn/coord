// Placement Driver — Multi-Raft 全局调度器
//
// 演进说明：本模块在 2026-08-31 → 09-05 完成生产化改造——
//   - PdMetaStore redb 落盘（<data_dir>/pd/pd-meta.db）；
//   - Region 心跳 → 实时调度状态（leader 视图/统计）；
//   - Operator 执行器映射真实 Region raft 成员变更；
//   - `EmbeddedPd`（本模块 `embedded.rs`）——main.rs 内嵌接线（心跳源、
//     成员对账、调度/执行循环；`[multi_raft].enabled=true` + `[multi_raft.pd]
//     .enabled=true` 时装配）。
//
// Placement Driver — Multi-Raft 全局调度器
//
// PD（Placement Driver）是 Coord Multi-Raft 体系的核心调度组件。
// 负责 Region 元数据管理、副本放置决策、Split/Merge 触发、热点检测与 Leader 均衡。
//
// 设计要点：
// - PD 内嵌于 Coord 进程，通过 Raft 共识保证 PD 元数据一致性
// - PD 可作为独立进程部署（3 节点 PD 集群）

pub mod embedded;
pub mod executor;
pub mod meta_store;
pub mod operator;
pub mod scheduler;
pub mod types;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use coord_core::error::Result;
use coord_core::types::{NodeID, RegionId};
use parking_lot::RwLock;
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;

use self::meta_store::PdMetaStore;
use self::scheduler::{create_default_schedulers, ScheduleContext, Scheduler};
use crate::audit::AuditLogger;
use crate::metrics::Metrics;
use crate::raft::system_raft::SystemRaftHandle;
use crate::raft::type_config::{PdOp, PdQueueEntry};

// Re-export 主要类型
pub use embedded::{EmbeddedPd, NodeInfo};
pub use executor::OperatorExecutor;
pub use operator::Operator;
pub use types::{NodeState, PdConfig, PdMode};

// ============================================================================
// PlacementDriver
// ============================================================================

/// Placement Driver：全局调度器
///
/// 负责所有调度决策，包括 Region Split/Merge、副本均衡、Leader 均衡、热点处理。
pub struct PlacementDriver {
    /// PD 配置
    config: PdConfig,
    /// Region 元数据持久化存储（内存模式）
    meta_store: Arc<PdMetaStore>,
    /// 活跃节点的心跳状态
    node_states: RwLock<HashMap<NodeID, NodeState>>,
    /// Region 心跳上报的当前 leader 视图（内存瞬态）
    ///
    /// 由各节点 Region 心跳填充；leader 是运行时事实（调度输入），非持久元数据，
    /// 与 `PdMetaStore` 落盘的成员/epoch/range 分离。节点离线或选举窗口（上报
    /// leader=0）时清除对应条目，调度器不得基于陈旧 leader 决策。
    region_leaders: RwLock<HashMap<RegionId, NodeID>>,
    /// 调度器集合
    schedulers: Vec<Box<dyn Scheduler>>,
    /// 优雅关闭信号
    shutdown_rx: watch::Receiver<bool>,
    /// 调度暂停开关（true = scheduler tick 不入队新 operator；已排队的仍
    /// 由 executor drain）。初始值取 `PdConfig.scheduler_paused`，可运行时切换。
    scheduler_paused: AtomicBool,
    /// operator 审计/指标钩子（EmbeddedPd 装配后经
    /// `attach_observability` 接线；None = 不记录，行为与现状一致）
    observability: RwLock<Option<PdObservability>>,
    /// 本节点 ID（调度收敛闸——operator 生成只发生在
    /// region 0 leader == 本节点的 PD；operator 认领 requester/归属）
    node_id: NodeID,
    /// region 0 system raft 治理句柄。
    ///
    /// - `Some`：**全局队列模式**——operator 队列经 region 0 raft 承载
    ///   （`/_pd/ops/*`）。调度收敛到 region 0 leader（唯一生成源），执行由
    ///   目标 Region leader 节点从全局队列认领（apply CAS 防双认领）。
    /// - `None`：未装配 system raft（P4b 退役本地队列后仅剩的防御分支——纯
    ///   元数据/心跳类装配用）；调度 tick 无可入队对象直接返回，不产生 operator。
    system: RwLock<Option<Arc<dyn SystemRaftHandle>>>,
}

#[derive(Clone, Default)]
pub struct PdObservability {
    /// operator 审计日志（actor=pd；enqueue/complete/requeue 各记一条）
    pub audit: Option<Arc<AuditLogger>>,
    /// 指标（operator 计数 `inc_pd_operator`）
    pub metrics: Option<Arc<Metrics>>,
}

impl PdObservability {
    /// 新建可观测性钩子（None 字段 = 不记录对应维度）
    pub fn new(audit: Option<Arc<AuditLogger>>, metrics: Option<Arc<Metrics>>) -> Self {
        Self { audit, metrics }
    }
}

impl PlacementDriver {
    /// 创建新的 PlacementDriver（内嵌模式）
    pub fn new(
        config: PdConfig,
        meta_store: Arc<PdMetaStore>,
        shutdown_rx: watch::Receiver<bool>,
        node_id: NodeID,
    ) -> Self {
        let schedulers = create_default_schedulers(&config);
        let scheduler_paused = config.scheduler_paused;
        Self {
            config,
            meta_store,
            node_states: RwLock::new(HashMap::new()),
            region_leaders: RwLock::new(HashMap::new()),
            schedulers,
            shutdown_rx,
            scheduler_paused: AtomicBool::new(scheduler_paused),
            observability: RwLock::new(None),
            node_id,
            system: RwLock::new(None),
        }
    }

    /// 装配 region 0 system raft 治理句柄（进入全局队列
    /// 模式）。应在任何后台循环启动前调用；幂等（重复装配覆盖）。
    pub fn attach_system_raft(&self, system: Arc<dyn SystemRaftHandle>) {
        *self.system.write() = Some(system);
        tracing::info!(
            "PD: operator queue switched to region 0 system raft (global queue mode) \
             on node {}",
            self.node_id
        );
    }

    /// 当前 region 0 system raft 治理句柄（None = 未装配，调度/执行无可入队
    /// 对象——P4b 退役本地队列后仅剩的防御分支）
    pub fn system_raft(&self) -> Option<Arc<dyn SystemRaftHandle>> {
        self.system.read().clone()
    }

    /// 本节点 ID
    pub fn node_id(&self) -> NodeID {
        self.node_id
    }

    // ──── 调度暂停开关 ────

    /// 运行时切换调度暂停（true = scheduler tick 不再产生新 operator）
    pub fn set_scheduler_paused(&self, paused: bool) {
        self.scheduler_paused.store(paused, Ordering::Relaxed);
        tracing::info!(
            "PD: scheduler {}",
            if paused { "PAUSED" } else { "RESUMED" }
        );
    }

    /// 当前调度是否暂停
    pub fn is_scheduler_paused(&self) -> bool {
        self.scheduler_paused.load(Ordering::Relaxed)
    }

    // ──── operator 审计/指标 ────

    /// 装配可观测性钩子（audit logger / metrics；None = 不记录）
    pub fn attach_observability(&self, obs: PdObservability) {
        *self.observability.write() = Some(obs);
    }

    /// 记录一条 operator 审计事件（actor=pd）+ 指标计数
    fn record_operator_event(&self, op: &Operator, result: &str, detail: &str) {
        let obs = self.observability.read().clone();
        let Some(obs) = obs else { return };
        if let Some(audit) = &obs.audit {
            audit.record_event(
                "pd",
                &format!("operator.{}", op.name()),
                &format!("region/{}", op.region_id()),
                result,
                detail,
            );
        }
    }

    /// operator 入队指标计数（仅调度产生/管理面注入的**新增** operator；
    /// 幂等去重命中时不计数——见 `enqueue_generated_global` 的预去重分支）
    fn count_operator_enqueued(&self) {
        let obs = self.observability.read().clone();
        if let Some(metrics) = obs.and_then(|o| o.metrics) {
            metrics.inc_pd_operator();
        }
    }

    /// P3：operator 超时重认领指标计数（region 0 leader 成功 propose Requeue 时）
    fn count_operator_requeued(&self) {
        let obs = self.observability.read().clone();
        if let Some(metrics) = obs.and_then(|o| o.metrics) {
            metrics.inc_pd_operator_requeued();
        }
    }

    /// 获取 PdMetaStore 引用（供外部查询 Region 路由表）
    pub fn meta_store(&self) -> &Arc<PdMetaStore> {
        &self.meta_store
    }

    /// 获取 PD 配置
    pub fn config(&self) -> &PdConfig {
        &self.config
    }

    // ──── 节点心跳管理 ────

    /// 处理节点心跳上报
    pub fn handle_node_heartbeat(&self, state: NodeState) {
        let mut nodes = self.node_states.write();
        let node_id = state.node_id;
        let mut node = state;
        node.last_heartbeat = Some(Instant::now());
        node.online = true;
        nodes.insert(node_id, node);
    }

    /// 获取节点状态
    pub fn get_node_state(&self, node_id: NodeID) -> Option<NodeState> {
        self.node_states.read().get(&node_id).cloned()
    }

    /// 列出所有节点
    pub fn list_nodes(&self) -> Vec<NodeState> {
        self.node_states.read().values().cloned().collect()
    }

    /// 检查并标记离线节点
    pub fn check_offline_nodes(&self) -> Vec<NodeID> {
        let timeout = Duration::from_secs(self.config.node_heartbeat_timeout);
        let now = Instant::now();
        let mut offline = Vec::new();
        let mut nodes = self.node_states.write();

        for (id, state) in nodes.iter_mut() {
            if let Some(last_hb) = state.last_heartbeat {
                if now.duration_since(last_hb) > timeout {
                    state.online = false;
                    offline.push(*id);
                }
            }
        }

        offline
    }

    // ──── Region 心跳管理 ────

    /// 处理 Region 心跳上报
    ///
    /// - 统计字段（size/keys）是派生瞬态数据 → 走 `update_region_stats` 内存视图，
    ///   不写穿落盘（避免每拍心跳 commit+fsync 写放大，见 PdMetaStore 文档）；
    /// - 对象存储字节（`storage_bytes`，coord.storage chunk 文件）同为内存视图；
    /// - `leader_node_id` 记入内存 leader 视图（调度输入）：0 = 选举窗口 leader
    ///   未知 → 清除该 Region 的陈旧视图，调度器不得基于它决策。
    pub fn handle_region_heartbeat(
        &self,
        region_id: RegionId,
        size: u64,
        keys: u64,
        storage_bytes: u64,
        leader_node_id: NodeID,
    ) -> Result<()> {
        self.meta_store.update_region_stats(region_id, size, keys)?;
        self.meta_store
            .update_region_storage_bytes(region_id, storage_bytes)?;

        {
            let mut leaders = self.region_leaders.write();
            if leader_node_id == 0 {
                leaders.remove(&region_id);
            } else {
                leaders.insert(region_id, leader_node_id);
            }
        }

        tracing::trace!(
            "PD: region {} heartbeat: size={}, keys={}, storage_bytes={}, leader={}",
            region_id,
            size,
            keys,
            storage_bytes,
            leader_node_id
        );
        Ok(())
    }

    /// 查询 Region 心跳上报的当前 leader（无上报 / 选举窗口为 None）
    pub fn region_leader(&self, region_id: RegionId) -> Option<NodeID> {
        self.region_leaders.read().get(&region_id).copied()
    }

    // ──── 调度循环 ────

    /// 启动 PD 调度循环（后台 tokio task）
    ///
    /// 按配置的间隔周期性地运行所有调度器，收集 Operator 并放入待执行队列。
    pub fn start_scheduler_loop(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let pd = Arc::clone(self);
        let mut shutdown_rx = pd.shutdown_rx.clone();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs(pd.config.balance_interval));
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

            let mut tick: u64 = 0;

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        tick = tick.wrapping_add(1);
                    }
                    _ = shutdown_rx.changed() => {
                        tracing::info!("PD scheduler loop: shutdown signal received");
                        break;
                    }
                }

                pd.run_schedule_tick(tick).await;
            }
        })
    }

    /// 执行一次调度 tick
    async fn run_schedule_tick(&self, _tick: u64) {
        // P4b：本地队列路径已退役——operator 队列恒为 region 0 raft 全局队列
        // （生产自 P2 起接线；本方法仅剩全局队列模式）。未装配 system raft 的
        // 装配（纯元数据/心跳类测试）无可入队对象，直接返回。
        let Some(system) = self.system_raft() else {
            tracing::debug!("PD: no region 0 system raft; skip schedule tick");
            return;
        };

        // region 0 leader 每 tick 先做 Running 超时重认领
        // （认领者失联/Complete 丢失 → 卡死 failover 兜底）与队列深度上报。
        // 该维护**不随 scheduler_paused 冻结**——暂停只冻结"新 operator
        // 生成"（见下），活性恢复（防卡死）属保障语义须持续生效。
        if system.current_leader().await == Some(self.node_id) {
            self.requeue_stale_running(system.as_ref()).await;
        }

        // 调度暂停——维护窗口/演练时冻结调度行为（不产生新 operator）。
        // 已排队的 operator 仍由 executor 循环 drain（见模块文档）。
        if self.is_scheduler_paused() {
            tracing::debug!("PD: scheduler paused; skipping schedule tick");
            return;
        }

        // 1. 检查离线节点
        let offline = self.check_offline_nodes();
        for node_id in &offline {
            tracing::warn!("PD: node {} marked offline", node_id);
        }

        // 1b. 离线节点的 leader 视图已过期：从调度视角清除（避免把 Region 的
        //     "当前 leader" 指向已离线节点，或据此向它转移）。
        if !offline.is_empty() {
            let mut leaders = self.region_leaders.write();
            for node_id in &offline {
                leaders.retain(|_, leader| leader != node_id);
            }
        }

        // operator 生成只发生在 region 0 raft leader 所在
        // 节点的 PD——生成源全局唯一（follower 的 PD 仍做心跳维护与离线判定，
        // 但不生成 operator；执行器在每节点照常运行，从全局队列认领「目标
        // Region leader == 本节点」的条目）。
        match system.current_leader().await {
            Some(l) if l == self.node_id => {}
            Some(l) => {
                tracing::debug!(
                    "PD: node {} is not region 0 leader (leader={}); skip scheduling \
                     (global queue mode)",
                    self.node_id,
                    l
                );
                return;
            }
            None => {
                tracing::debug!("PD: region 0 leader unknown; skip scheduling");
                return;
            }
        }

        // 2. 构建调度上下文（含心跳上报的 Region leader 视图 + 对象存储字节）
        let regions = self.meta_store.list_regions();
        let nodes: Vec<NodeState> = self.node_states.read().values().cloned().collect();
        let mut ctx = ScheduleContext::new(regions, nodes);
        ctx.leaders = self.region_leaders.read().clone();
        ctx.region_storage_bytes = self.meta_store.all_region_storage_bytes();

        // 3. 运行所有调度器；并发上限 = 全局队列 Pending/Running 数。
        let max_ops = self.config.max_concurrent_operators;
        let pending_count = match system.pd_queue() {
            Ok(entries) => entries
                .iter()
                .filter(|e| e.is_pending() || e.is_running())
                .count(),
            Err(e) => {
                tracing::warn!("PD: read region 0 pd queue failed: {e}; skip tick");
                return;
            }
        };
        if pending_count >= max_ops {
            tracing::debug!(
                "PD: {} pending operators (global queue mode), skipping schedule tick",
                pending_count
            );
            return;
        }

        let remaining = max_ops - pending_count;

        // 运行调度器收集候选 operator
        let mut generated: Vec<Operator> = Vec::new();
        for scheduler in &self.schedulers {
            if generated.len() >= remaining {
                break;
            }
            let ops = scheduler.schedule(&ctx);
            let count = ops.len();
            if count > 0 {
                tracing::info!("PD: {} generated {} operator(s)", scheduler.name(), count);
                for op in ops {
                    generated.push(op);
                    if generated.len() >= remaining {
                        break;
                    }
                }
            }
        }
        if generated.is_empty() {
            return;
        }

        self.enqueue_generated_global(&system, generated).await;
    }

    /// 全局队列模式入队：候选 operator 与全局队列预去重后
    /// 经 raft `Enqueue` 写入 region 0（apply 层幂等去重兜底；单生成源下预过滤
    /// 即权威，避免每 tick 对同一 operator 重复 Enqueue 的日志噪音）。
    async fn enqueue_generated_global(
        &self,
        system: &Arc<dyn SystemRaftHandle>,
        generated: Vec<Operator>,
    ) {
        let queue = match system.pd_queue() {
            Ok(q) => q,
            Err(e) => {
                tracing::warn!("PD: read region 0 pd queue failed: {e}; drop generated ops");
                return;
            }
        };
        let now = now_unix_secs();
        for op in generated {
            let dup = queue
                .iter()
                .any(|e| (e.is_pending() || e.is_running()) && e.op == op);
            if dup {
                continue;
            }
            match system
                .propose_pd(PdOp::Enqueue {
                    op: op.clone(),
                    requester: self.node_id,
                    proposed_at_unix: now,
                })
                .await
            {
                Ok(rev) => {
                    tracing::info!(
                        "PD: enqueued operator {} to region 0 queue (log {rev})",
                        op_summary(&op)
                    );
                    // 新增（非去重命中）operator 审计 + 指标
                    self.count_operator_enqueued();
                    self.record_operator_event(&op, "pending", &op_summary(&op));
                }
                Err(e) => {
                    tracing::warn!(
                        "PD: enqueue operator {} via region 0 failed: {e}",
                        op_summary(&op)
                    );
                }
            }
        }
    }

    /// region 0 leader 周期扫描全局队列，把 **Running 超时**
    /// （认领者失联 / Complete 提出丢失 → 条目卡死）的 operator 经 raft
    /// `Requeue` 放回 Pending，由当前存活的目标 Region leader 重认领执行——
    /// 认领者故障的自愈兜底（failover）。
    ///
    /// 调用方保证本节点是 region 0 leader（唯一 propose 源）。判定基于条目内
    /// `claimed_at_unix`（`PdOp::Claim` 命令由认领者填墙钟，apply 期不读墙钟的
    /// 确定性约束——见 type_config.rs）；超时阈值 `operator_running_timeout`
    /// （秒，须大于单次 operator 正常执行时长，避免误伤在途执行）。执行**失败**
    /// 走 `Complete{Failed}` 终态（不重认领）；此处只救"卡在 Running"的活性
    /// 故障。顺带以上报全局队列深度 gauge（leader 唯一上报）。
    async fn requeue_stale_running(&self, system: &dyn SystemRaftHandle) {
        let queue = match system.pd_queue() {
            Ok(q) => q,
            Err(e) => {
                tracing::warn!("PD: read region 0 pd queue failed (requeue scan): {e}");
                return;
            }
        };
        let now = now_unix_secs();
        let timeout = self.config.operator_running_timeout as i64;
        for entry in &queue {
            if !entry.is_running() {
                continue;
            }
            let age = entry.running_for_secs(now);
            if age < timeout {
                continue;
            }
            let summary = op_summary(&entry.op);
            match system
                .propose_pd(PdOp::Requeue { op_id: entry.op_id })
                .await
            {
                Ok(_) => {
                    tracing::warn!(
                        "PD: requeue op {} ({}): running {age}s >= timeout {timeout}s; \
                         claimant node {} lost/failed to complete",
                        entry.op_id,
                        summary,
                        entry.claimed_by
                    );
                    self.count_operator_requeued();
                    self.record_operator_event(
                        &entry.op,
                        "requeued",
                        &format!(
                            "{}; running={age}s >= timeout={timeout}s; claimant={} lost",
                            summary, entry.claimed_by
                        ),
                    );
                }
                Err(e) => {
                    tracing::warn!("PD: requeue op {} via region 0 failed: {e}", entry.op_id);
                }
            }
        }
        self.report_queue_depth(&queue);
    }

    /// P3：上报全局队列深度 gauge（pending/running/terminal 计数；观测未接线
    /// no-op）。leader 每 tick 经 `requeue_stale_running` 顺带调用。
    fn report_queue_depth(&self, queue: &[PdQueueEntry]) {
        let (mut pending, mut running, mut terminal) = (0u64, 0u64, 0u64);
        for e in queue {
            if e.is_pending() {
                pending += 1;
            } else if e.is_running() {
                running += 1;
            } else if e.is_terminal() {
                terminal += 1;
            }
        }
        let obs = self.observability.read().clone();
        if let Some(metrics) = obs.and_then(|o| o.metrics) {
            metrics.set_pd_queue_depth(pending, running, terminal);
        }
    }
}

/// 生成 operator 的审计摘要（动作 + region + 目标，人可读且稳定）。
fn op_summary(op: &Operator) -> String {
    match op {
        Operator::AddPeer {
            region_id,
            node_id,
            raft_addr,
        } => format!("add-peer region={region_id} node={node_id} raft_addr={raft_addr}"),
        Operator::RemovePeer { region_id, node_id } => {
            format!("remove-peer region={region_id} node={node_id}")
        }
        Operator::TransferLeader { region_id, to_node } => {
            format!("transfer-leader region={region_id} to={to_node}")
        }
        Operator::SplitRegion {
            region_id,
            split_key,
            new_region_id,
        } => format!(
            "split-region region={region_id} split_key={} new_region={new_region_id}",
            String::from_utf8_lossy(split_key)
        ),
        Operator::MergeRegion { left, right } => format!("merge-region left={left} right={right}"),
    }
}

/// 当前墙钟 Unix 秒（operator 审计/入队时间戳；apply 期不读墙钟的约束由
/// raft 命令携带时间戳满足——见 `PdOp::Enqueue.proposed_at_unix`/
/// `PdOp::Claim.claimed_at_unix`）。executor 模块复用（Claim 填认领墙钟）。
pub(super) fn now_unix_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

// ============================================================================
// 测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionMeta};
    use std::sync::{Arc, Mutex};

    use crate::pd::operator::OperatorStatus;
    use crate::raft::type_config::{PdOp, PdQueueEntry};

    fn make_test_pd() -> (Arc<PlacementDriver>, watch::Sender<bool>) {
        let config = PdConfig::default();
        let meta_store = Arc::new(PdMetaStore::new());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let pd = Arc::new(PlacementDriver::new(config, meta_store, shutdown_rx, 1));
        (pd, shutdown_tx)
    }

    fn make_region_meta(id: RegionId, start: Vec<u8>, end: Vec<u8>) -> RegionMeta {
        RegionMeta {
            region_id: id,
            start_key: start,
            end_key: end,
            epoch: RegionEpoch::initial(),
            peers: vec![Peer {
                node_id: 1,
                raft_addr: "node1:50052".into(),
                role: PeerRole::Voter,
            }],
            approximate_size: 0,
            approximate_keys: 0,
        }
    }

    fn make_node_state(id: NodeID, online: bool) -> NodeState {
        let mut state = NodeState {
            node_id: id,
            raft_addr: format!("node{}:50052", id),
            grpc_addr: format!("node{}:50051", id),
            labels: HashMap::new(),
            last_heartbeat: None,
            online,
            capacity_bytes: 1024 * 1024 * 1024,
            used_bytes: 0,
            leader_count: 0,
            region_count: 0,
        };
        if online {
            state.last_heartbeat = Some(Instant::now());
        }
        state
    }

    // ──── PlacementDriver 创建与配置测试 ────

    #[test]
    fn test_pd_creation() {
        let (pd, _tx) = make_test_pd();
        assert_eq!(pd.config().mode, PdMode::Embedded);
        assert_eq!(pd.meta_store().region_count(), 0);
    }

    #[test]
    fn test_pd_default_config() {
        let (pd, _tx) = make_test_pd();
        let cfg = pd.config();
        assert_eq!(cfg.region_split_size_mb, 256);
        assert_eq!(cfg.target_replicas, 3);
        assert_eq!(cfg.balance_interval, 120);
    }

    // ──── 节点心跳测试 ────

    #[test]
    fn test_node_heartbeat() {
        let (pd, _tx) = make_test_pd();
        let node = make_node_state(1, true);
        pd.handle_node_heartbeat(node);

        let state = pd.get_node_state(1).unwrap();
        assert!(state.online);
        assert_eq!(state.node_id, 1);
    }

    #[test]
    fn test_node_heartbeat_multiple() {
        let (pd, _tx) = make_test_pd();
        for i in 1..=5 {
            pd.handle_node_heartbeat(make_node_state(i, true));
        }
        assert_eq!(pd.list_nodes().len(), 5);
    }

    #[test]
    fn test_check_offline_nodes() {
        let (pd, _tx) = make_test_pd();
        // 直接插入一个 last_heartbeat 为 60s 前的节点，绕过 handle_node_heartbeat 的重置
        let mut node = make_node_state(1, true);
        node.last_heartbeat = Some(Instant::now() - Duration::from_secs(60));
        pd.node_states.write().insert(1, node);

        let offline = pd.check_offline_nodes();
        assert!(offline.contains(&1), "node 1 should be offline after 60s");
    }

    #[test]
    fn test_node_not_found() {
        let (pd, _tx) = make_test_pd();
        assert!(pd.get_node_state(999).is_none());
    }

    // ──── Region 心跳测试 ────

    #[test]
    fn test_region_heartbeat_updates_stats() {
        let (pd, _tx) = make_test_pd();
        let region = make_region_meta(1, vec![0x00], vec![0xFF]);
        pd.meta_store().create_region(region).unwrap();

        pd.handle_region_heartbeat(1, 1024 * 1024, 5000, 0, 1)
            .unwrap();

        let updated = pd.meta_store().get_region(1).unwrap();
        assert_eq!(updated.approximate_size, 1024 * 1024);
        assert_eq!(updated.approximate_keys, 5000);
        assert_eq!(pd.meta_store().region_storage_bytes(1), 0);

        // 对象存储字节维度（内存视图，独立于 redb 文件大小）
        pd.handle_region_heartbeat(1, 100, 10, 3 * 1024 * 1024, 1)
            .unwrap();
        assert_eq!(pd.meta_store().region_storage_bytes(1), 3 * 1024 * 1024);
    }

    #[test]
    fn test_region_heartbeat_not_found() {
        let (pd, _tx) = make_test_pd();
        let result = pd.handle_region_heartbeat(999, 0, 0, 0, 1);
        assert!(result.is_err());
    }

    // ──── Region 心跳 leader 视图 ────

    #[test]
    fn test_region_heartbeat_tracks_leader() {
        let (pd, _tx) = make_test_pd();
        let region = make_region_meta(1, vec![0x00], vec![0xFF]);
        pd.meta_store().create_region(region).unwrap();

        // 初始无心跳 → 无 leader 视图
        assert_eq!(pd.region_leader(1), None);

        // 心跳上报 leader=node2
        pd.handle_region_heartbeat(1, 100, 10, 0, 2).unwrap();
        assert_eq!(pd.region_leader(1), Some(2));

        // leader 变更（选举切换到 node3）
        pd.handle_region_heartbeat(1, 100, 10, 0, 3).unwrap();
        assert_eq!(pd.region_leader(1), Some(3));

        // 选举窗口 leader 未知（上报 0）→ 清除陈旧视图，调度器不得基于它决策
        pd.handle_region_heartbeat(1, 100, 10, 0, 0).unwrap();
        assert_eq!(pd.region_leader(1), None);
    }

    #[test]
    fn test_region_heartbeat_stats_not_durable() {
        // 心跳统计更新走内存视图（update_region_stats），不写穿落盘
        let dir = tempfile::tempdir().unwrap();
        let meta_store = Arc::new(PdMetaStore::open(dir.path()).unwrap());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let pd = Arc::new(PlacementDriver::new(
            PdConfig::default(),
            meta_store,
            shutdown_rx,
            1,
        ));
        let mut region = make_region_meta(1, vec![0x00], vec![0xFF]);
        region.peers = vec![
            Peer {
                node_id: 1,
                raft_addr: "node1:50052".into(),
                role: PeerRole::Voter,
            },
            Peer {
                node_id: 2,
                raft_addr: "node2:50052".into(),
                role: PeerRole::Voter,
            },
        ];
        pd.meta_store().create_region(region).unwrap();

        pd.handle_region_heartbeat(1, 8 * 1024 * 1024, 12345, 0, 1)
            .unwrap();
        drop(pd);
        drop(shutdown_tx);

        // 重启恢复：region 仍在但统计回落（心跳未落盘）
        let store = PdMetaStore::open(dir.path()).unwrap();
        let r = store.get_region(1).unwrap();
        assert_eq!(r.approximate_size, 0);
        assert_eq!(r.approximate_keys, 0);
    }

    // ──── 调度 tick 使用心跳 leader ────

    /// 构造 2 节点 × 2 Region 的 PD：region peers 首 Voter = node2（旧"首
    /// Voter=leader"猜测会误判），但心跳上报真实 leader = node1。
    /// 返回 (pd, shutdown_tx) 便于启动真实调度循环后优雅停止。
    fn make_leader_imbalance_pd(cfg: PdConfig) -> (Arc<PlacementDriver>, watch::Sender<bool>) {
        let meta_store = Arc::new(PdMetaStore::new());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let pd = Arc::new(PlacementDriver::new(cfg, meta_store, shutdown_rx, 1));

        pd.handle_node_heartbeat(make_node_state(1, true));
        pd.handle_node_heartbeat(make_node_state(2, true));

        for (rid, start, end) in [
            (10u64, vec![0x00u8], vec![0x55u8]),
            (11, vec![0x55], vec![]),
        ] {
            let mut region = make_region_meta(rid, start, end);
            // 关键：peers 顺序使"第一个 Voter"= node2，而真实 leader = node1
            region.peers = vec![
                Peer {
                    node_id: 2,
                    raft_addr: "node2:50052".into(),
                    role: PeerRole::Voter,
                },
                Peer {
                    node_id: 1,
                    raft_addr: "node1:50052".into(),
                    role: PeerRole::Voter,
                },
            ];
            pd.meta_store().create_region(region).unwrap();
            // 两 Region 心跳 leader 均为 node1 → node1 负载 2、node2 空载
            pd.handle_region_heartbeat(rid, 100, 10, 0, 1).unwrap();
        }
        (pd, shutdown_tx)
    }

    #[test]
    fn test_schedule_tick_leader_balance_uses_heartbeat_leader() {
        // LeaderScheduler 的 leader 判定来自 Region 心跳（ctx.leaders），而非
        // "第一个 Voter peer"猜测。两 Region 真实 leader 都是 node1 → 均衡应
        // 把其中一个 leader 转移给真实空载的 node2（peers 首 Voter 是 node2，
        // 旧猜测会把"leader"算在 node2 上 → 错误地转移到 node1）。
        let mut cfg = PdConfig::default();
        cfg.target_replicas = 2; // 2 voters 恰好达标，屏蔽 ReplicaChecker 噪音
        cfg.balance_interval = 1;
        let (pd, _shutdown_tx) = make_leader_imbalance_pd(cfg);
        let system = Arc::new(FakeSystemRaft::new(Some(1))); // region 0 leader = 本节点
        pd.attach_system_raft(system.clone());

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            pd.run_schedule_tick(0).await;
        });

        // P4b：调度产物经 raft Enqueue 入全局队列（不再有本地队列）
        let queue = system.pd_queue().unwrap();
        let mut found = false;
        for e in &queue {
            if let Operator::TransferLeader { to_node, .. } = &e.op {
                assert_eq!(
                    *to_node, 2,
                    "心跳 leader=node1 → 应转移到真实空载 node2（而非被首-Voter 猜测误导）"
                );
                found = true;
            }
        }
        assert!(
            found,
            "leader 均衡应产出 TransferLeader operator（全局队列）"
        );
    }

    #[test]
    fn test_scheduler_loop_ticks_and_shuts_down() {
        // start_scheduler_loop 真实启动（balance_interval=1s）：产出 operator、
        // watch 关闭信号优雅停止（handle 正常结束）。
        let mut cfg = PdConfig::default();
        cfg.target_replicas = 2;
        cfg.balance_interval = 1;
        let (pd, shutdown_tx) = make_leader_imbalance_pd(cfg);
        let system = Arc::new(FakeSystemRaft::new(Some(1))); // region 0 leader = 本节点
        pd.attach_system_raft(system.clone());

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let handle = pd.start_scheduler_loop();

            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
            let mut found = false;
            loop {
                // P4b：调度产物出现在全局队列（raft Enqueue）
                let queue = system.pd_queue().unwrap();
                if queue
                    .iter()
                    .any(|e| matches!(&e.op, Operator::TransferLeader { to_node: 2, .. }))
                {
                    found = true;
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            assert!(found, "scheduler loop 应产出 TransferLeader（全局队列）");

            // 优雅关闭：watch 置位 → 循环退出、task 结束
            shutdown_tx.send(true).unwrap();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(3), handle)
                .await
                .expect("scheduler loop should exit on shutdown signal");
        });
    }

    // ──── 调度 tick 测试 ────

    #[test]
    fn test_schedule_tick_no_regions() {
        let (pd, _tx) = make_test_pd();
        let system = Arc::new(FakeSystemRaft::new(Some(1)));
        pd.attach_system_raft(system.clone());
        // 添加在线节点
        pd.handle_node_heartbeat(make_node_state(1, true));
        pd.handle_node_heartbeat(make_node_state(2, true));

        // 运行一次调度 tick
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            pd.run_schedule_tick(0).await;
        });

        // 没有 Region，不应该产生 Operator（全局队列空、无 Enqueue）
        assert!(system.pd_queue().unwrap().is_empty());
        assert!(system.proposed_ops().is_empty());
    }

    #[test]
    fn test_schedule_tick_with_regions() {
        let (pd, _tx) = make_test_pd();
        let system = Arc::new(FakeSystemRaft::new(Some(1)));
        pd.attach_system_raft(system.clone());

        // 添加节点
        pd.handle_node_heartbeat(make_node_state(1, true));
        pd.handle_node_heartbeat(make_node_state(2, true));
        pd.handle_node_heartbeat(make_node_state(3, true));

        // 添加一个只有 1 个 Voter 的 Region（触发 ReplicaChecker）
        let mut region = make_region_meta(1, vec![0x00], vec![0xFF]);
        region.peers = vec![Peer {
            node_id: 1,
            raft_addr: "node1:50052".into(),
            role: PeerRole::Voter,
        }];
        pd.meta_store().create_region(region).unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            pd.run_schedule_tick(0).await;
        });

        // ReplicaChecker 应该生成 AddPeer Operator（经 raft Enqueue 入全局队列）
        let queue = system.pd_queue().unwrap();
        assert!(
            queue
                .iter()
                .any(|e| matches!(&e.op, Operator::AddPeer { .. })),
            "should have scheduled add-peer operator: {queue:?}"
        );
    }

    #[test]
    fn test_schedule_tick_respects_max_operators() {
        let (pd, _tx) = make_test_pd();
        let system = Arc::new(FakeSystemRaft::new(Some(1)));
        pd.attach_system_raft(system.clone());

        // 添加节点
        pd.handle_node_heartbeat(make_node_state(1, true));

        // 添加多个只有 1 个 Voter 的 Region（都会触发 ReplicaChecker）
        for i in 0..20 {
            let mut region = make_region_meta(i, vec![i as u8], vec![i as u8 + 1]);
            region.peers = vec![Peer {
                node_id: 1,
                raft_addr: "node1:50052".into(),
                role: PeerRole::Voter,
            }];
            pd.meta_store().create_region(region).unwrap();
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            pd.run_schedule_tick(0).await;
        });

        // 待处理 Operator 数（全局队列 Pending）不应超过 max_concurrent_operators
        let queue = system.pd_queue().unwrap();
        let pending = queue.iter().filter(|e| e.is_pending()).count();
        assert!(
            pending <= pd.config().max_concurrent_operators,
            "pending {} should be <= {}",
            pending,
            pd.config().max_concurrent_operators
        );
    }

    // ──── 全局队列模式调度（region 0 raft 承载）────

    /// 可编程 region 0 system raft 替身：内存队列 + 与 `apply_pd_op` 同构的
    /// CAS 状态机（Pending→Running(claimed_by)→Success/Failed、Enqueue 幂等
    /// 去重、Requeue），记录全部 propose 命令供断言。
    struct FakeSystemRaft {
        leader: Mutex<Option<NodeID>>,
        queue: Mutex<Vec<PdQueueEntry>>,
        proposed: Mutex<Vec<PdOp>>,
        next_op_id: std::sync::atomic::AtomicU64,
    }

    impl FakeSystemRaft {
        fn new(leader: Option<NodeID>) -> Self {
            Self {
                leader: Mutex::new(leader),
                queue: Mutex::new(Vec::new()),
                proposed: Mutex::new(Vec::new()),
                next_op_id: std::sync::atomic::AtomicU64::new(1),
            }
        }

        /// 全部已 propose 的命令（按序）
        fn proposed_ops(&self) -> Vec<PdOp> {
            self.proposed.lock().unwrap().clone()
        }

        /// 按 op_id 读取当前条目（P3 超时重认领测试断言用）
        fn queue_entry(&self, op_id: u64) -> Option<PdQueueEntry> {
            self.queue
                .lock()
                .unwrap()
                .iter()
                .find(|e| e.op_id == op_id)
                .cloned()
        }

        /// 预置一条 Running 条目（认领者/认领墙钟可指定；P3 超时重认领测试用）
        #[allow(clippy::too_many_arguments)]
        fn seed_running(
            &self,
            op_id: u64,
            op: Operator,
            requester: NodeID,
            claimed_by: NodeID,
            claimed_at_unix: i64,
        ) {
            self.queue.lock().unwrap().push(PdQueueEntry {
                op_id,
                op,
                status: OperatorStatus::Running,
                requester,
                claimed_by,
                proposed_at_unix: claimed_at_unix,
                claimed_at_unix,
                error: String::new(),
            });
        }
    }

    #[async_trait]
    impl SystemRaftHandle for FakeSystemRaft {
        async fn current_leader(&self) -> Option<NodeID> {
            *self.leader.lock().unwrap()
        }

        async fn propose_pd(&self, op: PdOp) -> coord_core::error::Result<u64> {
            // 与 MvccStorage::apply_pd_op 同构的内存模拟（Enqueue 幂等去重、
            // Claim/Complete CAS、Requeue）
            self.proposed.lock().unwrap().push(op.clone());
            let mut queue = self.queue.lock().unwrap();
            match &op {
                PdOp::Enqueue {
                    op,
                    requester,
                    proposed_at_unix,
                } => {
                    let dup = queue
                        .iter()
                        .any(|e| (e.is_pending() || e.is_running()) && &e.op == op);
                    if !dup {
                        let id = self
                            .next_op_id
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        queue.push(PdQueueEntry::new_pending(
                            id,
                            op.clone(),
                            *requester,
                            *proposed_at_unix,
                        ));
                    }
                    Ok(1) // 日志 index 非断言点
                }
                PdOp::Claim {
                    op_id,
                    node_id,
                    claimed_at_unix,
                } => {
                    if let Some(e) = queue.iter_mut().find(|e| e.op_id == *op_id) {
                        e.try_claim(*node_id, *claimed_at_unix);
                    }
                    Ok(1)
                }
                PdOp::Complete {
                    op_id,
                    node_id,
                    success,
                    error,
                } => {
                    if let Some(e) = queue.iter_mut().find(|e| e.op_id == *op_id) {
                        e.try_complete(*node_id, *success, error);
                    }
                    Ok(1)
                }
                PdOp::Requeue { op_id } => {
                    if let Some(e) = queue.iter_mut().find(|e| e.op_id == *op_id) {
                        e.try_requeue();
                    }
                    Ok(1)
                }
            }
        }

        fn pd_queue(&self) -> coord_core::error::Result<Vec<PdQueueEntry>> {
            Ok(self.queue.lock().unwrap().clone())
        }
    }

    /// 全局队列模式 follower（region 0 leader = node2 ≠ 本节点 node1）：
    /// 调度 tick 不生成、不 Enqueue、不碰本地队列。
    #[tokio::test]
    async fn test_raft_mode_scheduler_skips_when_not_region0_leader() {
        let mut cfg = PdConfig::default();
        cfg.target_replicas = 2;
        cfg.balance_interval = 1;
        let (pd, _shutdown_tx) = make_leader_imbalance_pd(cfg);
        let system = Arc::new(FakeSystemRaft::new(Some(2)));
        pd.attach_system_raft(system.clone());

        pd.run_schedule_tick(0).await;

        assert_eq!(
            system.proposed_ops().len(),
            0,
            "follower 不得 Enqueue（region 0 leader=2，本节点=1）"
        );
    }

    /// 全局队列模式 leader（region 0 leader = 本节点）：调度生成的 operator
    /// 经 raft `Enqueue` 入全局队列（本地队列不参与）；重复 tick 预去重——同一
    /// operator 已在队列 Pending → 不再重复 Enqueue（无日志噪音）。
    #[tokio::test]
    async fn test_raft_mode_scheduler_enqueues_on_leader_and_dedups_across_ticks() {
        let mut cfg = PdConfig::default();
        cfg.target_replicas = 2;
        cfg.balance_interval = 1;
        let (pd, _shutdown_tx) = make_leader_imbalance_pd(cfg);
        let system = Arc::new(FakeSystemRaft::new(Some(1)));
        pd.attach_system_raft(system.clone());

        // tick 1：leader 均衡生成 TransferLeader → raft Enqueue
        pd.run_schedule_tick(0).await;
        let proposed1 = system.proposed_ops();
        let enq1 = proposed1
            .iter()
            .filter(|op| matches!(op, PdOp::Enqueue { .. }))
            .count();
        assert!(enq1 >= 1, "leader 应把生成的 operator Enqueue 到全局队列");
        assert!(
            proposed1
                .iter()
                .all(|op| matches!(op, PdOp::Enqueue { .. })),
            "调度只应提出 Enqueue"
        );

        // tick 2：同一 operator 仍在队列 Pending → 预去重，不重复 Enqueue
        pd.run_schedule_tick(0).await;
        let proposed2 = system.proposed_ops();
        let enq2 = proposed2
            .iter()
            .filter(|op| matches!(op, PdOp::Enqueue { .. }))
            .count();
        assert_eq!(
            enq1, enq2,
            "重复 tick 不得重复 Enqueue 相同 operator（预去重）"
        );
    }

    /// P3：region 0 leader 把 Running 超时条目（认领者失联/Complete 丢失 → 卡死）
    /// 经 raft Requeue 放回 Pending；新鲜 Running / Pending 不受影响；不 Enqueue
    /// 新 operator（本 driver 无 region 元数据 → 调度器无产出）。
    #[tokio::test]
    async fn test_raft_mode_requeues_stale_running_on_region0_leader() {
        let (pd, _tx) = make_test_pd();
        let system = Arc::new(FakeSystemRaft::new(Some(1)));
        let now = now_unix_secs();
        // stale Running（认领 1000s 前 >> 默认 300s 超时）→ 应 Requeue
        system.seed_running(
            1,
            Operator::TransferLeader {
                region_id: 10,
                to_node: 3,
            },
            1,
            2,
            now - 1000,
        );
        // 新鲜 Running（刚认领）→ 不应 Requeue
        system.seed_running(
            2,
            Operator::RemovePeer {
                region_id: 11,
                node_id: 4,
            },
            1,
            3,
            now,
        );
        // Pending → 不涉及超时扫描
        system.queue.lock().unwrap().push(PdQueueEntry::new_pending(
            3,
            Operator::AddPeer {
                region_id: 12,
                node_id: 5,
                raft_addr: "node5:50052".into(),
            },
            1,
            now,
        ));
        pd.attach_system_raft(system.clone());

        pd.run_schedule_tick(0).await;

        // 只有 op 1 被 Requeue
        let proposed = system.proposed_ops();
        let requeues: Vec<u64> = proposed
            .iter()
            .filter_map(|op| match op {
                PdOp::Requeue { op_id } => Some(*op_id),
                _ => None,
            })
            .collect();
        assert_eq!(
            requeues,
            vec![1],
            "仅超时 Running 条目被 Requeue: {proposed:?}"
        );
        assert!(
            system.queue_entry(1).unwrap().is_pending(),
            "stale running op 1 应回 Pending（claimed_by 清除）"
        );
        assert_eq!(system.queue_entry(1).unwrap().claimed_by, 0);
        assert!(
            system.queue_entry(2).unwrap().is_running(),
            "fresh running op 2 不得被误 Requeue"
        );
        assert!(system.queue_entry(3).unwrap().is_pending());
    }

    /// P3：region 0 follower 不执行 Running 超时重认领（region 0 leader 是唯一
    /// Requeue 提出源；follower 仍保留条目不动）。
    #[tokio::test]
    async fn test_raft_mode_follower_does_not_requeue_stale_running() {
        let (pd, _tx) = make_test_pd();
        let system = Arc::new(FakeSystemRaft::new(Some(2))); // region 0 leader = node 2
        let now = now_unix_secs();
        system.seed_running(
            1,
            Operator::TransferLeader {
                region_id: 10,
                to_node: 3,
            },
            1,
            2,
            now - 1000,
        );
        pd.attach_system_raft(system.clone());

        pd.run_schedule_tick(0).await;

        assert!(
            system.proposed_ops().is_empty(),
            "follower 不得提出 Requeue"
        );
        let e = system.queue_entry(1).unwrap();
        assert!(e.is_running(), "follower 不得改动条目状态");
        assert_eq!(e.claimed_by, 2);
    }

    /// P3：Running 超时重认领不受 scheduler_paused 冻结（暂停只冻结"新 operator
    /// 生成"；活性恢复持续生效）。
    #[tokio::test]
    async fn test_raft_mode_requeue_runs_while_scheduler_paused() {
        let (pd, _tx) = make_test_pd();
        pd.set_scheduler_paused(true);
        let system = Arc::new(FakeSystemRaft::new(Some(1)));
        let now = now_unix_secs();
        system.seed_running(
            1,
            Operator::TransferLeader {
                region_id: 10,
                to_node: 3,
            },
            1,
            2,
            now - 1000,
        );
        pd.attach_system_raft(system.clone());

        pd.run_schedule_tick(0).await;

        let requeues: Vec<u64> = system
            .proposed_ops()
            .iter()
            .filter_map(|op| match op {
                PdOp::Requeue { op_id } => Some(*op_id),
                _ => None,
            })
            .collect();
        assert_eq!(requeues, vec![1], "暂停期间 failover 重认领仍应执行");
        assert!(system.queue_entry(1).unwrap().is_pending());
    }
}
