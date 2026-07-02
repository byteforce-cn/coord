// PD 核心类型定义
//
// 包含 PD 配置、节点状态、调度上下文等核心类型。

use std::collections::HashMap;
use std::time::{Duration, Instant};

use coord_core::types::NodeID;
use serde::{Deserialize, Serialize};

// ============================================================================
// PD 运行模式
// ============================================================================

/// PD 运行模式
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PdMode {
    /// 内嵌模式：PD 作为 Coord 进程的一部分运行
    Embedded,
    /// 外部模式：PD 作为独立集群运行
    External,
}

// ============================================================================
// PD 配置
// ============================================================================

/// PD 配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PdConfig {
    /// PD 运行模式
    pub mode: PdMode,
    /// 外部 PD 地址（mode=External 时使用）
    #[serde(default)]
    pub external_addrs: Vec<String>,
    /// Split Checker 检查间隔（秒）
    pub split_check_interval: u64,
    /// Merge Checker 检查间隔（秒）
    pub merge_check_interval: u64,
    /// Balance 调度间隔（秒）
    pub balance_interval: u64,
    /// 最大并发调度 Operator 数
    pub max_concurrent_operators: usize,
    /// Region 分裂大小阈值（字节）
    pub region_split_size_mb: u64,
    /// Region 分裂 Key 数阈值
    pub region_split_keys: u64,
    /// Region 合并大小阈值（字节）
    pub region_merge_size_mb: u64,
    /// 副本数目标值
    pub target_replicas: usize,
    /// 节点心跳超时（秒）
    pub node_heartbeat_timeout: u64,
    /// 副本放置约束
    #[serde(default)]
    pub placement: PlacementConstraint,
    /// 节点维护模式配置
    #[serde(default)]
    pub maintenance: MaintenanceConfig,
}

impl Default for PdConfig {
    fn default() -> Self {
        Self {
            mode: PdMode::Embedded,
            external_addrs: vec![],
            split_check_interval: 30,
            merge_check_interval: 60,
            balance_interval: 120,
            max_concurrent_operators: 10,
            region_split_size_mb: 256,
            region_split_keys: 1_000_000,
            region_merge_size_mb: 16,
            target_replicas: 3,
            node_heartbeat_timeout: 30,
            placement: PlacementConstraint::default(),
            maintenance: MaintenanceConfig::default(),
        }
    }
}

// ============================================================================
// 节点状态
// ============================================================================

/// 节点状态（PD 视角）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeState {
    /// 节点唯一标识
    pub node_id: NodeID,
    /// Raft 通信地址
    pub raft_addr: String,
    /// gRPC 服务地址
    pub grpc_addr: String,
    /// 节点标签（用于拓扑感知调度）
    #[serde(default)]
    pub labels: HashMap<String, String>,
    /// 最近心跳时间（不持久化，仅内存）
    #[serde(skip)]
    pub last_heartbeat: Option<Instant>,
    /// 是否在线（心跳超时 > 30s 标记为离线）
    pub online: bool,
    /// 磁盘容量（字节）
    pub capacity_bytes: u64,
    /// 已用磁盘（字节）
    pub used_bytes: u64,
    /// 当前 Leader 数量
    pub leader_count: u32,
    /// 当前 Region 副本数
    pub region_count: u32,
}

impl NodeState {
    /// 创建新的节点状态
    pub fn new(node_id: NodeID, raft_addr: String, grpc_addr: String) -> Self {
        Self {
            node_id,
            raft_addr,
            grpc_addr,
            labels: HashMap::new(),
            last_heartbeat: None,
            online: false,
            capacity_bytes: 0,
            used_bytes: 0,
            leader_count: 0,
            region_count: 0,
        }
    }

    /// 节点是否在线
    pub fn is_online(&self, timeout: Duration) -> bool {
        match self.last_heartbeat {
            Some(last) => last.elapsed() < timeout,
            None => false,
        }
    }

    /// 可用磁盘容量（字节）
    pub fn available_bytes(&self) -> u64 {
        self.capacity_bytes.saturating_sub(self.used_bytes)
    }

    /// 记录心跳
    pub fn record_heartbeat(&mut self) {
        self.last_heartbeat = Some(Instant::now());
        self.online = true;
    }

    /// 标记离线
    pub fn mark_offline(&mut self) {
        self.online = false;
    }

    /// 节点是否处于维护模式
    pub fn is_under_maintenance(&self) -> bool {
        self.labels.get("maintenance").map_or(false, |v| v == "true")
    }
}

// ============================================================================
// 副本放置约束
// ============================================================================

/// 副本放置约束
///
/// 控制 Region 副本在不同故障域级别的分布策略。
/// 用于实现拓扑感知调度（ADP §14.2）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementConstraint {
    /// 同一 Region 的副本不能在同一 host
    pub forbid_same_host: bool,
    /// 同一 Region 的副本不能在同一 rack
    pub forbid_same_rack: bool,
    /// 同一 Region 的副本不能在同一 zone
    pub forbid_same_zone: bool,
    /// 优先将副本分布在不同 zone
    pub prefer_diverse_zones: bool,
}

impl Default for PlacementConstraint {
    fn default() -> Self {
        Self {
            forbid_same_host: true,
            forbid_same_rack: false,
            forbid_same_zone: false,
            prefer_diverse_zones: true,
        }
    }
}

impl PlacementConstraint {
    /// 检查目标节点是否可以放置指定 Region 的副本
    ///
    /// # Arguments
    /// * `target_node` - 候选目标节点
    /// * `existing_peers` - 当前 Region 已有的副本所在节点
    /// * `all_nodes` - 所有节点状态（用于获取标签）
    ///
    /// # Returns
    /// `true` 如果可以在目标节点放置副本，否则 `false`
    pub fn can_place(
        &self,
        target_node: &NodeState,
        existing_peers: &[NodeID],
        all_nodes: &HashMap<NodeID, NodeState>,
    ) -> bool {
        // 不能在已存在副本的节点上放置
        if existing_peers.contains(&target_node.node_id) {
            return false;
        }

        for &peer_id in existing_peers {
            if let Some(peer_node) = all_nodes.get(&peer_id) {
                if self.forbid_same_host {
                    let peer_host = peer_node.labels.get("host");
                    let target_host = target_node.labels.get("host");
                    if peer_host.is_some() && peer_host == target_host {
                        return false;
                    }
                }

                if self.forbid_same_rack {
                    let peer_rack = peer_node.labels.get("rack");
                    let target_rack = target_node.labels.get("rack");
                    if peer_rack.is_some() && peer_rack == target_rack {
                        return false;
                    }
                }

                if self.forbid_same_zone {
                    let peer_zone = peer_node.labels.get("zone");
                    let target_zone = target_node.labels.get("zone");
                    if peer_zone.is_some() && peer_zone == target_zone {
                        return false;
                    }
                }
            }
        }

        true
    }

    /// 选择最优副本放置节点
    ///
    /// 从候选节点中选择与已有副本隔离程度最高的节点。
    pub fn select_best_node<'a>(
        &self,
        candidates: &[&'a NodeState],
        existing_peers: &[NodeID],
        all_nodes: &HashMap<NodeID, NodeState>,
    ) -> Option<&'a NodeState> {
        candidates
            .iter()
            .filter(|n| self.can_place(n, existing_peers, all_nodes))
            .max_by_key(|n| {
                // 优先选择不同 zone 的节点
                if self.prefer_diverse_zones {
                    let existing_zones: std::collections::HashSet<&str> = existing_peers
                        .iter()
                        .filter_map(|pid| all_nodes.get(pid))
                        .filter_map(|n| n.labels.get("zone").map(|s| s.as_str()))
                        .collect();
                    let target_zone = n.labels.get("zone").map(|s| s.as_str()).unwrap_or("");
                    if !existing_zones.contains(target_zone) {
                        return 2u32; // 高优先级：不同 zone
                    }
                }
                1u32 // 普通优先级
            })
            .copied()
    }
}

// ============================================================================
// 节点维护模式
// ============================================================================

/// 节点维护模式配置（PD 配置的一部分）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenanceConfig {
    /// 是否启用自动 Leader 转移（进入维护模式时）
    pub auto_transfer_leader: bool,
    /// 维护前等待 Leader 转移完成的超时（秒）
    pub leader_transfer_timeout_secs: u64,
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            auto_transfer_leader: true,
            leader_transfer_timeout_secs: 30,
        }
    }
}

// ============================================================================
// 测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(id: NodeID, zone: &str, host: &str) -> NodeState {
        let mut labels = HashMap::new();
        labels.insert("zone".to_string(), zone.to_string());
        labels.insert("host".to_string(), host.to_string());
        NodeState {
            node_id: id,
            raft_addr: format!("node{}:50052", id),
            grpc_addr: format!("node{}:50051", id),
            labels,
            last_heartbeat: None,
            online: true,
            capacity_bytes: 1024 * 1024 * 1024,
            used_bytes: 0,
            leader_count: 0,
            region_count: 0,
        }
    }

    #[test]
    fn test_placement_default_constraint() {
        let constraint = PlacementConstraint::default();
        assert!(constraint.forbid_same_host);
        assert!(!constraint.forbid_same_rack);
        assert!(!constraint.forbid_same_zone);
        assert!(constraint.prefer_diverse_zones);
    }

    #[test]
    fn test_placement_can_place_same_host_forbidden() {
        let constraint = PlacementConstraint {
            forbid_same_host: true,
            forbid_same_rack: false,
            forbid_same_zone: false,
            prefer_diverse_zones: false,
        };

        let node1 = make_node(1, "zone-a", "host-1");
        let node2 = make_node(2, "zone-a", "host-1"); // 同一 host
        let node3 = make_node(3, "zone-a", "host-2");
        let all_nodes: HashMap<_, _> = [(1, node1.clone()), (2, node2.clone()), (3, node3.clone())]
            .into_iter()
            .collect();

        // node2 和 node1 同一 host，应被拒绝
        assert!(!constraint.can_place(&node2, &[1], &all_nodes));
        // node3 不同 host，应被允许
        assert!(constraint.can_place(&node3, &[1], &all_nodes));
    }

    #[test]
    fn test_placement_can_place_same_zone_forbidden() {
        let constraint = PlacementConstraint {
            forbid_same_host: false,
            forbid_same_rack: false,
            forbid_same_zone: true,
            prefer_diverse_zones: false,
        };

        let node1 = make_node(1, "zone-a", "host-1");
        let node2 = make_node(2, "zone-a", "host-2"); // 同一 zone
        let node3 = make_node(3, "zone-b", "host-3");
        let all_nodes: HashMap<_, _> = [(1, node1.clone()), (2, node2.clone()), (3, node3.clone())]
            .into_iter()
            .collect();

        assert!(!constraint.can_place(&node2, &[1], &all_nodes));
        assert!(constraint.can_place(&node3, &[1], &all_nodes));
    }

    #[test]
    fn test_placement_can_place_existing_peer_rejected() {
        let constraint = PlacementConstraint::default();
        let node1 = make_node(1, "zone-a", "host-1");
        let all_nodes: HashMap<_, _> = [(1, node1.clone())].into_iter().collect();
        // 不能在已有副本的节点上再放置
        assert!(!constraint.can_place(&node1, &[1], &all_nodes));
    }

    #[test]
    fn test_placement_select_best_node_prefers_different_zone() {
        let constraint = PlacementConstraint {
            forbid_same_host: false,
            forbid_same_rack: false,
            forbid_same_zone: false,
            prefer_diverse_zones: true,
        };

        let node1 = make_node(1, "zone-a", "host-1");
        let node2 = make_node(2, "zone-a", "host-2");
        let node3 = make_node(3, "zone-b", "host-3");
        let all_nodes: HashMap<_, _> = [
            (1, node1.clone()),
            (2, node2.clone()),
            (3, node3.clone()),
        ]
        .into_iter()
        .collect();

        let candidates: Vec<&NodeState> = vec![&node2, &node3];
        // node3 在不同 zone，应被优先选择
        let best = constraint.select_best_node(&candidates, &[1], &all_nodes);
        assert!(best.is_some());
        assert_eq!(best.unwrap().node_id, 3);
    }

    #[test]
    fn test_node_maintenance_label() {
        let mut node = make_node(1, "zone-a", "host-1");
        assert!(!node.is_under_maintenance());

        node.labels.insert("maintenance".to_string(), "true".to_string());
        assert!(node.is_under_maintenance());
    }

    #[test]
    fn test_maintenance_config_default() {
        let config = MaintenanceConfig::default();
        assert!(config.auto_transfer_leader);
        assert_eq!(config.leader_transfer_timeout_secs, 30);
    }
}
