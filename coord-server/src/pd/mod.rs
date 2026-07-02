// Placement Driver — Multi-Raft 全局调度器
//
// PD（Placement Driver）是 Coord Multi-Raft 体系的核心调度组件。
// 负责 Region 元数据管理、副本放置决策、Split/Merge 触发、热点检测与 Leader 均衡。
//
// 设计要点（ADP §4）：
// - Phase 1-2：PD 内嵌于 Coord 进程，通过 Raft 共识保证 PD 元数据一致性
// - Phase 3+：PD 可作为独立进程部署（3 节点 PD 集群）

pub mod meta_store;
pub mod operator;
pub mod scheduler;
pub mod types;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use coord_core::error::{Error, Result};
use coord_core::types::{NodeID, RegionId};
use parking_lot::RwLock;
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;

use self::meta_store::PdMetaStore;
use self::operator::{OperatorEntry, OperatorStatus};
use self::scheduler::{create_default_schedulers, ScheduleContext, Scheduler};

// Re-export 主要类型
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
    /// 调度器集合
    schedulers: Vec<Box<dyn Scheduler>>,
    /// 待执行/执行中的 Operator
    pending_operators: RwLock<Vec<OperatorEntry>>,
    /// 优雅关闭信号
    shutdown_rx: watch::Receiver<bool>,
}

impl PlacementDriver {
    /// 创建新的 PlacementDriver（内嵌模式）
    pub fn new(
        config: PdConfig,
        meta_store: Arc<PdMetaStore>,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        let schedulers = create_default_schedulers(&config);
        Self {
            config,
            meta_store,
            node_states: RwLock::new(HashMap::new()),
            schedulers,
            pending_operators: RwLock::new(Vec::new()),
            shutdown_rx,
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
    /// 更新 Region 的 approximate_size、approximate_keys、Leader 信息等。
    pub fn handle_region_heartbeat(
        &self,
        region_id: RegionId,
        size: u64,
        keys: u64,
        leader_node_id: NodeID,
    ) -> Result<()> {
        let mut meta = self
            .meta_store
            .get_region(region_id)
            .ok_or(Error::RegionNotFound { region_id })?;

        meta.approximate_size = size;
        meta.approximate_keys = keys;
        // Leader 信息在 RegionHandle 中维护，此处仅更新统计数据
        self.meta_store.update_region(meta)?;

        tracing::trace!(
            "PD: region {} heartbeat: size={}, keys={}, leader={}",
            region_id,
            size,
            keys,
            leader_node_id
        );
        Ok(())
    }

    // ──── 调度循环 ────

    /// 启动 PD 调度循环（后台 tokio task）
    ///
    /// 按配置的间隔周期性地运行所有调度器，收集 Operator 并放入待执行队列。
    pub fn start_scheduler_loop(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let pd = Arc::clone(self);
        let mut shutdown_rx = pd.shutdown_rx.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(
                pd.config.balance_interval,
            ));
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
        // 1. 检查离线节点
        let offline = self.check_offline_nodes();
        for node_id in &offline {
            tracing::warn!("PD: node {} marked offline", node_id);
        }

        // 2. 构建调度上下文
        let regions = self.meta_store.list_regions();
        let nodes: Vec<NodeState> = self.node_states.read().values().cloned().collect();
        let ctx = ScheduleContext::new(regions, nodes);

        // 3. 运行所有调度器
        let mut total_ops = 0;
        let max_ops = self.config.max_concurrent_operators;

        // 检查待处理 Operator 数量，控制并发
        let pending_count = self.pending_operators.read().len();
        if pending_count >= max_ops {
            tracing::debug!(
                "PD: {} pending operators, skipping schedule tick",
                pending_count
            );
            return;
        }

        let remaining = max_ops - pending_count;

        for scheduler in &self.schedulers {
            if total_ops >= remaining {
                break;
            }

            let ops = scheduler.schedule(&ctx);
            let count = ops.len();
            if count > 0 {
                tracing::info!(
                    "PD: {} generated {} operator(s)",
                    scheduler.name(),
                    count
                );
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;

                let mut pending = self.pending_operators.write();
                for op in ops {
                    pending.push(OperatorEntry {
                        op,
                        status: OperatorStatus::Pending,
                        created_at: now,
                    });
                    total_ops += 1;
                    if total_ops >= remaining {
                        break;
                    }
                }
            }
        }
    }

    /// 获取并锁定下一个待执行的 Operator（标记为 Running）
    pub fn take_next_operator(&self) -> Option<Operator> {
        let mut pending = self.pending_operators.write();
        if let Some(pos) = pending.iter().position(|e| e.status == OperatorStatus::Pending) {
            pending[pos].status = OperatorStatus::Running;
            Some(pending[pos].op.clone())
        } else {
            None
        }
    }

    /// 标记 Operator 执行结果
    pub fn complete_operator(&self, op: &Operator, success: bool, error_msg: Option<String>) {
        let mut pending = self.pending_operators.write();
        if let Some(entry) = pending.iter_mut().find(|e| e.op == *op) {
            if success {
                entry.status = OperatorStatus::Success;
            } else {
                entry.status = OperatorStatus::Failed(error_msg.unwrap_or_default());
            }
        }

        // 清理已完成的 Operator（保留最近 1000 个）
        if pending.len() > 1000 {
            pending.retain(|e| e.status == OperatorStatus::Pending || e.status == OperatorStatus::Running);
        }
    }

    /// 获取 Operator 队列状态
    pub fn operator_stats(&self) -> OperatorStats {
        let pending = self.pending_operators.read();
        OperatorStats {
            total: pending.len(),
            pending: pending.iter().filter(|e| e.status == OperatorStatus::Pending).count(),
            running: pending.iter().filter(|e| e.status == OperatorStatus::Running).count(),
            success: pending.iter().filter(|e| e.status == OperatorStatus::Success).count(),
            failed: pending.iter().filter(|e| matches!(e.status, OperatorStatus::Failed(_))).count(),
            cancelled: pending.iter().filter(|e| e.status == OperatorStatus::Cancelled).count(),
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

// ============================================================================
// 测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionMeta};
    use std::sync::Arc;

    fn make_test_pd() -> (Arc<PlacementDriver>, watch::Sender<bool>) {
        let config = PdConfig::default();
        let meta_store = Arc::new(PdMetaStore::new());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let pd = Arc::new(PlacementDriver::new(config, meta_store, shutdown_rx));
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

        pd.handle_region_heartbeat(1, 1024 * 1024, 5000, 1)
            .unwrap();

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
        pd.pending_operators.write().push(OperatorEntry::new(op.clone()));
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

        pd.pending_operators.write().push(OperatorEntry::new(op.clone()));
        pd.complete_operator(&op, false, Some("timeout".into()));

        let stats = pd.operator_stats();
        assert_eq!(stats.failed, 1);
    }
}

