// PD 调度操作定义
//
// Operator 是 PD 对 Region 执行的具体调度动作。
// 每个 Operator 由某个 Scheduler 产生，由 PD 的执行引擎应用到集群。

use coord_core::types::{NodeID, RegionId};
use serde::{Deserialize, Serialize};

/// 调度操作：对 Region 执行的具体动作
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Operator {
    /// 在目标节点添加 Region 副本（先作为 Learner，同步完成后晋升 Voter）
    AddPeer {
        region_id: RegionId,
        node_id: NodeID,
        raft_addr: String,
    },
    /// 移除 Region 副本
    RemovePeer {
        region_id: RegionId,
        node_id: NodeID,
    },
    /// 将 Leader 转移到目标节点
    TransferLeader {
        region_id: RegionId,
        to_node: NodeID,
    },
    /// 分裂 Region
    SplitRegion {
        region_id: RegionId,
        split_key: Vec<u8>,
        new_region_id: RegionId,
    },
    /// 合并两个相邻 Region
    MergeRegion { left: RegionId, right: RegionId },
}

impl Operator {
    /// 获取操作涉及的 Region ID（首个）
    pub fn region_id(&self) -> RegionId {
        match self {
            Operator::AddPeer { region_id, .. } => *region_id,
            Operator::RemovePeer { region_id, .. } => *region_id,
            Operator::TransferLeader { region_id, .. } => *region_id,
            Operator::SplitRegion { region_id, .. } => *region_id,
            Operator::MergeRegion { left, .. } => *left,
        }
    }

    /// 操作名称（用于日志和指标）
    pub fn name(&self) -> &'static str {
        match self {
            Operator::AddPeer { .. } => "add-peer",
            Operator::RemovePeer { .. } => "remove-peer",
            Operator::TransferLeader { .. } => "transfer-leader",
            Operator::SplitRegion { .. } => "split-region",
            Operator::MergeRegion { .. } => "merge-region",
        }
    }
}

/// Operator 执行状态
///
/// 入 raft 日志（`PdQueueEntry.status`）后需 serde。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperatorStatus {
    /// 待执行
    Pending,
    /// 执行中
    Running,
    /// 执行成功
    Success,
    /// 执行失败
    Failed(String),
    /// 已取消
    Cancelled,
}

// ──── 测试 ────

#[cfg(test)]
mod tests {
    use super::*;

    // ──── Operator::region_id ────

    #[test]
    fn test_operator_region_id_add_peer() {
        let op = Operator::AddPeer {
            region_id: 1,
            node_id: 10,
            raft_addr: "127.0.0.1:9000".into(),
        };
        assert_eq!(op.region_id(), 1);
    }

    #[test]
    fn test_operator_region_id_remove_peer() {
        let op = Operator::RemovePeer {
            region_id: 2,
            node_id: 20,
        };
        assert_eq!(op.region_id(), 2);
    }

    #[test]
    fn test_operator_region_id_transfer_leader() {
        let op = Operator::TransferLeader {
            region_id: 3,
            to_node: 30,
        };
        assert_eq!(op.region_id(), 3);
    }

    #[test]
    fn test_operator_region_id_split_region() {
        let op = Operator::SplitRegion {
            region_id: 4,
            split_key: b"split".to_vec(),
            new_region_id: 400,
        };
        assert_eq!(op.region_id(), 4);
    }

    #[test]
    fn test_operator_region_id_merge_region() {
        let op = Operator::MergeRegion { left: 5, right: 6 };
        assert_eq!(op.region_id(), 5);
    }

    // ──── Operator::name ────

    #[test]
    fn test_operator_name_add_peer() {
        let op = Operator::AddPeer {
            region_id: 1,
            node_id: 10,
            raft_addr: String::new(),
        };
        assert_eq!(op.name(), "add-peer");
    }

    #[test]
    fn test_operator_name_remove_peer() {
        let op = Operator::RemovePeer {
            region_id: 1,
            node_id: 10,
        };
        assert_eq!(op.name(), "remove-peer");
    }

    #[test]
    fn test_operator_name_transfer_leader() {
        let op = Operator::TransferLeader {
            region_id: 1,
            to_node: 10,
        };
        assert_eq!(op.name(), "transfer-leader");
    }

    #[test]
    fn test_operator_name_split_region() {
        let op = Operator::SplitRegion {
            region_id: 1,
            split_key: vec![],
            new_region_id: 2,
        };
        assert_eq!(op.name(), "split-region");
    }

    #[test]
    fn test_operator_name_merge_region() {
        let op = Operator::MergeRegion { left: 1, right: 2 };
        assert_eq!(op.name(), "merge-region");
    }

    // ──── Operator serde ────

    #[test]
    fn test_operator_serde_roundtrip_add_peer() {
        let op = Operator::AddPeer {
            region_id: 7,
            node_id: 70,
            raft_addr: "10.0.0.1:8000".into(),
        };
        let json = serde_json::to_string(&op).unwrap();
        let decoded: Operator = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, op);
    }

    #[test]
    fn test_operator_serde_roundtrip_split_region() {
        let op = Operator::SplitRegion {
            region_id: 8,
            split_key: b"mid".to_vec(),
            new_region_id: 800,
        };
        let json = serde_json::to_string(&op).unwrap();
        let decoded: Operator = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, op);
    }

    #[test]
    fn test_operator_serde_roundtrip_merge_region() {
        let op = Operator::MergeRegion { left: 9, right: 10 };
        let json = serde_json::to_string(&op).unwrap();
        let decoded: Operator = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, op);
    }

    // ──── OperatorStatus ────

    #[test]
    fn test_operator_status_equality() {
        assert_eq!(OperatorStatus::Pending, OperatorStatus::Pending);
        assert_eq!(OperatorStatus::Running, OperatorStatus::Running);
        assert_eq!(OperatorStatus::Success, OperatorStatus::Success);
        assert_eq!(
            OperatorStatus::Failed("oops".into()),
            OperatorStatus::Failed("oops".into())
        );
        assert_eq!(OperatorStatus::Cancelled, OperatorStatus::Cancelled);
    }

    #[test]
    fn test_operator_status_not_equal() {
        assert_ne!(OperatorStatus::Pending, OperatorStatus::Running);
        assert_ne!(
            OperatorStatus::Failed("a".into()),
            OperatorStatus::Failed("b".into())
        );
    }
}
