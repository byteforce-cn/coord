// Placement Driver — Multi-Raft 全局调度器（Phase 3 生产化进行中）
//
// 演进说明：本模块在 Phase 0–3（2026-08-31 → 09-05）完成生产化改造——
//   - T3.1（coord 554a1fe）：PdMetaStore redb 落盘（<data_dir>/pd/pd-meta.db）；
//   - T3.2（coord 2fd8ec0）：Region 心跳 → 实时调度状态（leader 视图/统计）；
//   - T3.3（coord d510fa9）：Operator 执行器映射真实 Region raft 成员变更；
//   - T3.4：`EmbeddedPd`（本模块 `embedded.rs`）——main.rs 内嵌接线（心跳源、
//     成员对账、调度/执行循环；`[multi_raft].enabled=true` + `[multi_raft.pd]
//     .enabled=true` 时装配）。见 `docs/coord-multi-raft-plan-2026-08-31.md`。
//
// Placement Driver — Multi-Raft 全局调度器
//
// PD（Placement Driver）是 Coord Multi-Raft 体系的核心调度组件。
// 负责 Region 元数据管理、副本放置决策、Split/Merge 触发、热点检测与 Leader 均衡。
//
// 设计要点（ADP §4）：
// - Phase 1-2：PD 内嵌于 Coord 进程，通过 Raft 共识保证 PD 元数据一致性
// - Phase 3+：PD 可作为独立进程部署（3 节点 PD 集群）

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
use self::operator::{OperatorEntry, OperatorStatus};
use self::scheduler::{create_default_schedulers, ScheduleContext, Scheduler};
use crate::audit::AuditLogger;
use crate::metrics::Metrics;
use crate::raft::system_raft::SystemRaftHandle;
use crate::raft::type_config::PdOp;

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
    /// Region 元数据持久化存储（内存模式，Phase 3+ 持久化）
    meta_store: Arc<PdMetaStore>,
    /// 活跃节点的心跳状态
    node_states: RwLock<HashMap<NodeID, NodeState>>,
    /// Region 心跳上报的当前 leader 视图（T3.2，内存瞬态）
    ///
    /// 由各节点 Region 心跳填充；leader 是运行时事实（调度输入），非持久元数据，
    /// 与 `PdMetaStore` 落盘的成员/epoch/range 分离。节点离线或选举窗口（上报
    /// leader=0）时清除对应条目，调度器不得基于陈旧 leader 决策。
    region_leaders: RwLock<HashMap<RegionId, NodeID>>,
    /// 调度器集合
    schedulers: Vec<Box<dyn Scheduler>>,
    /// 待执行/执行中的 Operator
    pending_operators: RwLock<Vec<OperatorEntry>>,
    /// 优雅关闭信号
    shutdown_rx: watch::Receiver<bool>,
    /// T5.12：调度暂停开关（true = scheduler tick 不入队新 operator；已排队的仍
    /// 由 executor drain）。初始值取 `PdConfig.scheduler_paused`，可运行时切换。
    scheduler_paused: AtomicBool,
    /// T5.12：operator 审计/指标钩子（EmbeddedPd 装配后经
    /// `attach_observability` 接线；None = 不记录，行为与现状一致）
    observability: RwLock<Option<PdObservability>>,
    /// 本节点 ID（R-MR-08 D1-a P2：调度收敛闸——operator 生成只发生在
    /// region 0 leader == 本节点的 PD；operator 认领 requester/归属）
    node_id: NodeID,
    /// R-MR-08（D1-a P2）：region 0 system raft 治理句柄。
    ///
    /// - `Some`：**全局队列模式**——operator 队列经 region 0 raft 承载
    ///   （`/_pd/ops/*`）。调度收敛到 region 0 leader（唯一生成源），执行由
    ///   目标 Region leader 节点从全局队列认领（apply CAS 防双认领）；本节点
    ///   本地 `pending_operators` 队列不参与（P4 退役前保留为测试/回退路径）。
    /// - `None`：**legacy 本地队列模式**（region 0 raft 不可得——无 multi_raft
    ///   的测试装配/单元测试沿用）：现行为——每节点本地生成 + 执行器 Leader
    ///   守卫 + 一次性 Failed(not leader)。
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
            pending_operators: RwLock::new(Vec::new()),
            shutdown_rx,
            scheduler_paused: AtomicBool::new(scheduler_paused),
            observability: RwLock::new(None),
            node_id,
            system: RwLock::new(None),
        }
    }

    /// R-MR-08（D1-a P2）：装配 region 0 system raft 治理句柄（进入全局队列
    /// 模式）。应在任何后台循环启动前调用；幂等（重复装配覆盖）。
    pub fn attach_system_raft(&self, system: Arc<dyn SystemRaftHandle>) {
        *self.system.write() = Some(system);
        tracing::info!(
            "PD: operator queue switched to region 0 system raft (global queue mode) \
             on node {}",
            self.node_id
        );
    }

    /// 当前 region 0 system raft 治理句柄（None = legacy 本地队列模式）
    pub fn system_raft(&self) -> Option<Arc<dyn SystemRaftHandle>> {
        self.system.read().clone()
    }

    /// 本节点 ID
    pub fn node_id(&self) -> NodeID {
        self.node_id
    }

    // ──── T5.12：调度暂停开关 ────

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

    // ──── T5.12：operator 审计/指标 ────

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

    /// operator 入队指标计数（仅调度产生/管理面注入的**新增** operator；幂等去重
    /// 命中时不计——见 `enqueue_operator` 的 dup 分支）
    fn count_operator_enqueued(&self) {
        let obs = self.observability.read().clone();
        if let Some(metrics) = obs.and_then(|o| o.metrics) {
            metrics.inc_pd_operator();
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

    /// 处理 Region 心跳上报（T3.2）
    ///
    /// - 统计字段（size/keys）是派生瞬态数据 → 走 `update_region_stats` 内存视图，
    ///   不写穿落盘（避免每拍心跳 commit+fsync 写放大，见 PdMetaStore 文档）；
    /// - `leader_node_id` 记入内存 leader 视图（调度输入）：0 = 选举窗口 leader
    ///   未知 → 清除该 Region 的陈旧视图，调度器不得基于它决策。
    pub fn handle_region_heartbeat(
        &self,
        region_id: RegionId,
        size: u64,
        keys: u64,
        leader_node_id: NodeID,
    ) -> Result<()> {
        self.meta_store.update_region_stats(region_id, size, keys)?;

        {
            let mut leaders = self.region_leaders.write();
            if leader_node_id == 0 {
                leaders.remove(&region_id);
            } else {
                leaders.insert(region_id, leader_node_id);
            }
        }

        tracing::trace!(
            "PD: region {} heartbeat: size={}, keys={}, leader={}",
            region_id,
            size,
            keys,
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
        // T5.12：调度暂停——维护窗口/演练时冻结调度行为（不产生新 operator）。
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

        // R-MR-08（D1-a P2）：全局队列模式下 operator 生成只发生在 region 0
        // raft leader 所在节点的 PD——生成源全局唯一（follower 的 PD 仍做心跳
        // 维护与离线判定，但不生成 operator；执行器在每节点照常运行，从全局
        // 队列认领「目标 Region leader == 本节点」的条目）。legacy 本地模式
        // 无此闸（每节点本地生成，执行器 Leader 守卫过滤为 Failed）。
        let system = self.system_raft();
        if let Some(system) = &system {
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
        }

        // 2. 构建调度上下文（含心跳上报的 Region leader 视图）
        let regions = self.meta_store.list_regions();
        let nodes: Vec<NodeState> = self.node_states.read().values().cloned().collect();
        let mut ctx = ScheduleContext::new(regions, nodes);
        ctx.leaders = self.region_leaders.read().clone();

        // 3. 运行所有调度器
        let max_ops = self.config.max_concurrent_operators;

        // 并发上限：全局队列模式数全局队列 Pending/Running；本地模式数本地队列。
        let pending_count = if let Some(system) = &system {
            match system.pd_queue() {
                Ok(entries) => entries
                    .iter()
                    .filter(|e| e.is_pending() || e.is_running())
                    .count(),
                Err(e) => {
                    tracing::warn!("PD: read region 0 pd queue failed: {e}; skip tick");
                    return;
                }
            }
        } else {
            self.pending_operators.read().len()
        };
        if pending_count >= max_ops {
            tracing::debug!(
                "PD: {} pending operators (mode={}), skipping schedule tick",
                pending_count,
                if system.is_some() { "global" } else { "local" }
            );
            return;
        }

        let remaining = max_ops - pending_count;

        // 运行调度器收集候选 operator（生成逻辑两种模式共用；入队方式不同）
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

        if let Some(system) = &system {
            self.enqueue_generated_global(system, generated).await;
        } else {
            self.enqueue_generated_local(generated);
        }
    }

    /// 全局队列模式入队（R-MR-08 D1-a P2）：候选 operator 与全局队列预去重后
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
                    // T5.12：新增（非去重命中）operator 审计 + 指标
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

    /// 本地队列模式入队（legacy）：候选 operator 直接进本节点内存队列
    /// （无 region 0 raft 的测试/回退路径；P4 退役）。
    fn enqueue_generated_local(&self, generated: Vec<Operator>) {
        let now = now_unix_secs();
        let mut pending = self.pending_operators.write();
        for op in generated {
            pending.push(OperatorEntry {
                op,
                status: OperatorStatus::Pending,
                created_at: now,
            });
        }
    }

    /// 入队一个待执行 Operator（T3.3：执行器/管理面注入；**本地队列模式专用**）
    ///
    /// 幂等：与队列中 Pending/Running 的相同 operator 去重（相同 region +
    /// 相同动作 + 相同目标视为重复），避免调度/重试风暴。
    /// T5.12：新增（非去重命中）operator 记审计事件 + 指标计数。
    ///
    /// R-MR-08（D1-a P2）：全局队列模式（`attach_system_raft` 已装配）下生产
    /// 入队走调度器 → `PdOp::Enqueue`（raft 复制、apply 层幂等去重），本方法
    /// 只用于本地模式/测试注入；raft 模式下直接调用只会进本地内存队列（不被
    /// 执行器消费），调用方应改用 raft 通道。
    pub fn enqueue_operator(&self, op: Operator) {
        let mut pending = self.pending_operators.write();
        let dup = pending.iter().any(|e| {
            matches!(
                e.status,
                OperatorStatus::Pending | OperatorStatus::Running
            ) && e.op == op
        });
        if dup {
            return;
        }
        pending.push(OperatorEntry::new(op.clone()));
        drop(pending);

        self.count_operator_enqueued();
        self.record_operator_event(&op, "pending", &op_summary(&op));
    }

    /// 查询某 operator 的当前执行状态（队列无记录 = None）
    pub fn operator_status(&self, op: &Operator) -> Option<OperatorStatus> {
        let pending = self.pending_operators.read();
        pending.iter().find(|e| e.op == *op).map(|e| e.status.clone())
    }

    /// 获取并锁定下一个待执行的 Operator（标记为 Running）
    pub fn take_next_operator(&self) -> Option<Operator> {
        let mut pending = self.pending_operators.write();
        if let Some(pos) = pending
            .iter()
            .position(|e| e.status == OperatorStatus::Pending)
        {
            pending[pos].status = OperatorStatus::Running;
            Some(pending[pos].op.clone())
        } else {
            None
        }
    }

    /// 标记 Operator 执行结果
    ///
    /// T5.12：success/failed 均记审计事件（failed 携带原因，供事后追溯）。
    pub fn complete_operator(&self, op: &Operator, success: bool, error_msg: Option<String>) {
        let mut pending = self.pending_operators.write();
        if let Some(entry) = pending.iter_mut().find(|e| e.op == *op) {
            if success {
                entry.status = OperatorStatus::Success;
            } else {
                entry.status = OperatorStatus::Failed(error_msg.clone().unwrap_or_default());
            }
        }
        drop(pending);

        if success {
            self.record_operator_event(op, "success", &op_summary(op));
        } else {
            self.record_operator_event(
                op,
                "failed",
                &format!("{}; error: {}", op_summary(op), error_msg.unwrap_or_default()),
            );
        }

        // 清理已完成的 Operator（保留最近 1000 个）
        let mut pending = self.pending_operators.write();
        if pending.len() > 1000 {
            pending.retain(|e| {
                e.status == OperatorStatus::Pending || e.status == OperatorStatus::Running
            });
        }
    }

    /// 把 Running 的 operator 重新置回 Pending（T3.4 执行暂不可行时稍后重试）
    ///
    /// 与 `complete_operator(Failed)` 的区别：不产生 Failed 记录、不触发调度
    /// 器重新生成同一 operator（去重仍命中 Pending/Running）——用于 AddPeer
    /// 占位目标暂无可选节点等**可重试**场景，避免无谓 churn。
    pub fn requeue_operator(&self, op: &Operator) {
        let mut pending = self.pending_operators.write();
        if let Some(entry) = pending
            .iter_mut()
            .find(|e| e.op == *op && e.status == OperatorStatus::Running)
        {
            entry.status = OperatorStatus::Pending;
        }
        drop(pending);

        self.record_operator_event(op, "requeued", &op_summary(op));
    }

    /// 获取 Operator 队列状态
    pub fn operator_stats(&self) -> OperatorStats {
        let pending = self.pending_operators.read();
        OperatorStats {
            total: pending.len(),
            pending: pending
                .iter()
                .filter(|e| e.status == OperatorStatus::Pending)
                .count(),
            running: pending
                .iter()
                .filter(|e| e.status == OperatorStatus::Running)
                .count(),
            success: pending
                .iter()
                .filter(|e| e.status == OperatorStatus::Success)
                .count(),
            failed: pending
                .iter()
                .filter(|e| matches!(e.status, OperatorStatus::Failed(_)))
                .count(),
            cancelled: pending
                .iter()
                .filter(|e| e.status == OperatorStatus::Cancelled)
                .count(),
        }
    }
}

/// Operator 队列统计
#[derive(Debug, Clone, Default)]
pub struct OperatorStats {
    pub total: usize,
    pub pending: usize,
    pub running: usize,
    pub success: usize,
    pub failed: usize,
    pub cancelled: usize,
}

/// 生成 operator 的审计摘要（动作 + region + 目标，人可读且稳定）。
fn op_summary(op: &Operator) -> String {
    match op {
        Operator::AddPeer {
            region_id,
            node_id,
            raft_addr,
        } => format!(
            "add-peer region={region_id} node={node_id} raft_addr={raft_addr}"
        ),
        Operator::RemovePeer {
            region_id,
            node_id,
        } => format!("remove-peer region={region_id} node={node_id}"),
        Operator::TransferLeader {
            region_id,
            to_node,
        } => format!("transfer-leader region={region_id} to={to_node}"),
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
/// raft 命令携带时间戳满足——见 `PdOp::Enqueue.proposed_at_unix`）。
fn now_unix_secs() -> i64 {
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

        pd.handle_region_heartbeat(1, 1024 * 1024, 5000, 1).unwrap();

        let updated = pd.meta_store().get_region(1).unwrap();
        assert_eq!(updated.approximate_size, 1024 * 1024);
        assert_eq!(updated.approximate_keys, 5000);
    }

    #[test]
    fn test_region_heartbeat_not_found() {
        let (pd, _tx) = make_test_pd();
        let result = pd.handle_region_heartbeat(999, 0, 0, 1);
        assert!(result.is_err());
    }

    // ──── T3.2：Region 心跳 leader 视图 ────

    #[test]
    fn test_region_heartbeat_tracks_leader() {
        let (pd, _tx) = make_test_pd();
        let region = make_region_meta(1, vec![0x00], vec![0xFF]);
        pd.meta_store().create_region(region).unwrap();

        // 初始无心跳 → 无 leader 视图
        assert_eq!(pd.region_leader(1), None);

        // 心跳上报 leader=node2
        pd.handle_region_heartbeat(1, 100, 10, 2).unwrap();
        assert_eq!(pd.region_leader(1), Some(2));

        // leader 变更（选举切换到 node3）
        pd.handle_region_heartbeat(1, 100, 10, 3).unwrap();
        assert_eq!(pd.region_leader(1), Some(3));

        // 选举窗口 leader 未知（上报 0）→ 清除陈旧视图，调度器不得基于它决策
        pd.handle_region_heartbeat(1, 100, 10, 0).unwrap();
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

        pd.handle_region_heartbeat(1, 8 * 1024 * 1024, 12345, 1)
            .unwrap();
        drop(pd);
        drop(shutdown_tx);

        // 重启恢复：region 仍在但统计回落（心跳未落盘）
        let store = PdMetaStore::open(dir.path()).unwrap();
        let r = store.get_region(1).unwrap();
        assert_eq!(r.approximate_size, 0);
        assert_eq!(r.approximate_keys, 0);
    }

    // ──── T3.2：调度 tick 使用心跳 leader ────

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
            pd.handle_region_heartbeat(rid, 100, 10, 1).unwrap();
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

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            pd.run_schedule_tick(0).await;
        });

        let mut found = false;
        while let Some(op) = pd.take_next_operator() {
            if let Operator::TransferLeader { to_node, .. } = op {
                assert_eq!(
                    to_node, 2,
                    "心跳 leader=node1 → 应转移到真实空载 node2（而非被首-Voter 猜测误导）"
                );
                found = true;
            }
        }
        assert!(found, "leader 均衡应产出 TransferLeader operator");
    }

    #[test]
    fn test_scheduler_loop_ticks_and_shuts_down() {
        // start_scheduler_loop 真实启动（balance_interval=1s）：产出 operator、
        // watch 关闭信号优雅停止（handle 正常结束）。
        let mut cfg = PdConfig::default();
        cfg.target_replicas = 2;
        cfg.balance_interval = 1;
        let (pd, shutdown_tx) = make_leader_imbalance_pd(cfg);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let handle = pd.start_scheduler_loop();

            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
            let mut found = false;
            loop {
                if let Some(op) = pd.take_next_operator() {
                    if let Operator::TransferLeader { to_node, .. } = op {
                        assert_eq!(to_node, 2);
                        found = true;
                        break;
                    }
                }
                if tokio::time::Instant::now() >= deadline {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            assert!(found, "scheduler loop 应产出 TransferLeader");

            // 优雅关闭：watch 置位 → 循环退出、task 结束
            shutdown_tx.send(true).unwrap();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(3), handle)
                .await
                .expect("scheduler loop should exit on shutdown signal");
        });
    }

    // ──── Operator 队列测试 ────

    #[test]
    fn test_operator_queue_empty() {
        let (pd, _tx) = make_test_pd();
        assert!(pd.take_next_operator().is_none());
    }

    #[test]
    fn test_operator_stats() {
        let (pd, _tx) = make_test_pd();
        let stats = pd.operator_stats();
        assert_eq!(stats.total, 0);
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.running, 0);
    }

    // ──── 调度 tick 测试 ────

    #[test]
    fn test_schedule_tick_no_regions() {
        let (pd, _tx) = make_test_pd();
        // 添加在线节点
        pd.handle_node_heartbeat(make_node_state(1, true));
        pd.handle_node_heartbeat(make_node_state(2, true));

        // 运行一次调度 tick
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            pd.run_schedule_tick(0).await;
        });

        // 没有 Region，不应该产生 Operator
        assert!(pd.take_next_operator().is_none());
    }

    #[test]
    fn test_schedule_tick_with_regions() {
        let (pd, _tx) = make_test_pd();

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

        // ReplicaChecker 应该生成 AddPeer Operator
        let op = pd.take_next_operator();
        assert!(op.is_some(), "should have scheduled add-peer operator");
        assert!(matches!(op.unwrap(), Operator::AddPeer { .. }));
    }

    #[test]
    fn test_schedule_tick_respects_max_operators() {
        let (pd, _tx) = make_test_pd();

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

        // 待处理 Operator 数不应超过 max_concurrent_operators
        let stats = pd.operator_stats();
        assert!(
            stats.pending <= pd.config().max_concurrent_operators,
            "pending {} should be <= {}",
            stats.pending,
            pd.config().max_concurrent_operators
        );
    }

    // ──── R-MR-08（D1-a P2）：全局队列模式调度（region 0 raft 承载）────

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
                PdOp::Claim { op_id, node_id } => {
                    if let Some(e) = queue.iter_mut().find(|e| e.op_id == *op_id) {
                        e.try_claim(*node_id);
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
        assert!(pd.take_next_operator().is_none(), "全局模式不使用本地队列");
        assert_eq!(pd.operator_stats().total, 0);
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
            proposed1.iter().all(|op| matches!(op, PdOp::Enqueue { .. })),
            "调度只应提出 Enqueue"
        );
        assert!(pd.take_next_operator().is_none(), "全局模式不使用本地队列");

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

    // ──── Operator 生命周期测试 ────

    #[test]
    fn test_operator_complete_success() {
        let (pd, _tx) = make_test_pd();
        let op = Operator::AddPeer {
            region_id: 1,
            node_id: 2,
            raft_addr: "node2:50052".into(),
        };

        // 先加入队列
        pd.pending_operators
            .write()
            .push(OperatorEntry::new(op.clone()));
        // 标记完成
        pd.complete_operator(&op, true, None);

        let stats = pd.operator_stats();
        assert_eq!(stats.success, 1);
    }

    #[test]
    fn test_operator_complete_failure() {
        let (pd, _tx) = make_test_pd();
        let op = Operator::TransferLeader {
            region_id: 1,
            to_node: 2,
        };

        pd.pending_operators
            .write()
            .push(OperatorEntry::new(op.clone()));
        pd.complete_operator(&op, false, Some("timeout".into()));

        let stats = pd.operator_stats();
        assert_eq!(stats.failed, 1);
    }
}
