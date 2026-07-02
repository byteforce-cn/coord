// pd_test.rs — Phase 2: Placement Driver 测试
//
// TDD: 测试 PD 核心调度逻辑

use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionId, RegionMeta};

// ============================================================================
// Node 状态管理测试
// ============================================================================

#[derive(Debug, Clone)]
struct NodeState {
    node_id: u64,
    raft_addr: String,
    grpc_addr: String,
    online: bool,
    capacity_bytes: u64,
    used_bytes: u64,
    leader_count: u32,
    region_count: u32,
    labels: std::collections::HashMap<String, String>,
}

impl NodeState {
    fn new(node_id: u64, addr: &str) -> Self {
        Self {
            node_id,
            raft_addr: format!("{}:50052", addr),
            grpc_addr: format!("{}:50051", addr),
            online: true,
            capacity_bytes: 1024 * 1024 * 1024 * 1024, // 1 TB
            used_bytes: 0,
            leader_count: 0,
            region_count: 0,
            labels: std::collections::HashMap::new(),
        }
    }
}

#[test]
fn test_node_state_creation() {
    let node = NodeState::new(1, "192.168.1.1");
    assert_eq!(node.node_id, 1);
    assert!(node.online);
    assert_eq!(node.region_count, 0);
}

#[test]
fn test_node_available_capacity() {
    let node = NodeState {
        node_id: 1,
        raft_addr: "addr".to_string(),
        grpc_addr: "addr".to_string(),
        online: true,
        capacity_bytes: 1000,
        used_bytes: 300,
        leader_count: 0,
        region_count: 0,
        labels: std::collections::HashMap::new(),
    };
    assert_eq!(node.capacity_bytes - node.used_bytes, 700);
}

// ============================================================================
// Split Checker 测试
// ============================================================================

struct SplitChecker {
    split_size_threshold: u64, // bytes
    split_keys_threshold: u64, // key count
}

impl SplitChecker {
    fn new() -> Self {
        Self {
            split_size_threshold: 256 * 1024 * 1024, // 256 MB
            split_keys_threshold: 1_000_000,
        }
    }

    fn should_split(&self, region: &RegionMeta) -> bool {
        region.approximate_size >= self.split_size_threshold
            || region.approximate_keys >= self.split_keys_threshold
    }
}

#[test]
fn test_split_checker_not_exceeding() {
    let checker = SplitChecker::new();
    let region = RegionMeta {
        region_id: 1,
        start_key: vec![0x00],
        end_key: vec![0x55],
        epoch: RegionEpoch::initial(),
        peers: vec![],
        approximate_size: 100 * 1024 * 1024, // 100 MB
        approximate_keys: 500_000,
    };
    assert!(!checker.should_split(&region));
}

#[test]
fn test_split_checker_size_exceeds() {
    let checker = SplitChecker::new();
    let region = RegionMeta {
        region_id: 1,
        start_key: vec![0x00],
        end_key: vec![0x55],
        epoch: RegionEpoch::initial(),
        peers: vec![],
        approximate_size: 300 * 1024 * 1024, // 300 MB > 256 MB
        approximate_keys: 500_000,
    };
    assert!(checker.should_split(&region));
}

#[test]
fn test_split_checker_keys_exceeds() {
    let checker = SplitChecker::new();
    let region = RegionMeta {
        region_id: 1,
        start_key: vec![0x00],
        end_key: vec![0x55],
        epoch: RegionEpoch::initial(),
        peers: vec![],
        approximate_size: 100 * 1024 * 1024,
        approximate_keys: 1_500_000, // > 1M
    };
    assert!(checker.should_split(&region));
}

#[test]
fn test_split_checker_both_exceed() {
    let checker = SplitChecker::new();
    let region = RegionMeta {
        region_id: 1,
        start_key: vec![0x00],
        end_key: vec![0x55],
        epoch: RegionEpoch::initial(),
        peers: vec![],
        approximate_size: 500 * 1024 * 1024,
        approximate_keys: 2_000_000,
    };
    assert!(checker.should_split(&region));
}

// ============================================================================
// Merge Checker 测试
// ============================================================================

struct MergeChecker {
    merge_size_threshold: u64, // below this, candidate for merge
    max_merge_size: u64,       // merged total must be below this
}

impl MergeChecker {
    fn new() -> Self {
        Self {
            merge_size_threshold: 16 * 1024 * 1024, // 16 MB
            max_merge_size: 256 * 1024 * 1024,       // 256 MB
        }
    }

    fn should_merge(&self, left: &RegionMeta, right: &RegionMeta) -> bool {
        // Both regions must be below merge threshold
        let left_small = left.approximate_size < self.merge_size_threshold;
        let right_small = right.approximate_size < self.merge_size_threshold;

        // Merged total must be below max size
        let total_size = left.approximate_size + right.approximate_size;
        let total_ok = total_size < self.max_merge_size;

        // Ranges must be adjacent
        let adjacent = left.end_key == right.start_key;

        left_small && right_small && total_ok && adjacent
    }
}

#[test]
fn test_merge_checker_both_small() {
    let checker = MergeChecker::new();
    let left = RegionMeta {
        region_id: 1,
        start_key: vec![0x00],
        end_key: vec![0x55],
        epoch: RegionEpoch::initial(),
        peers: vec![],
        approximate_size: 10 * 1024 * 1024, // 10 MB < 16 MB
        approximate_keys: 100_000,
    };
    let right = RegionMeta {
        region_id: 2,
        start_key: vec![0x55],
        end_key: vec![0xFF],
        epoch: RegionEpoch::initial(),
        peers: vec![],
        approximate_size: 5 * 1024 * 1024, // 5 MB < 16 MB
        approximate_keys: 50_000,
    };

    assert!(checker.should_merge(&left, &right));
}

#[test]
fn test_merge_checker_one_too_large() {
    let checker = MergeChecker::new();
    let left = RegionMeta {
        region_id: 1,
        start_key: vec![0x00],
        end_key: vec![0x55],
        epoch: RegionEpoch::initial(),
        peers: vec![],
        approximate_size: 100 * 1024 * 1024, // 100 MB > 16 MB
        approximate_keys: 500_000,
    };
    let right = RegionMeta {
        region_id: 2,
        start_key: vec![0x55],
        end_key: vec![0xFF],
        epoch: RegionEpoch::initial(),
        peers: vec![],
        approximate_size: 5 * 1024 * 1024,
        approximate_keys: 50_000,
    };

    assert!(!checker.should_merge(&left, &right));
}

#[test]
fn test_merge_checker_not_adjacent() {
    let checker = MergeChecker::new();
    let left = RegionMeta {
        region_id: 1,
        start_key: vec![0x00],
        end_key: vec![0x55],
        epoch: RegionEpoch::initial(),
        peers: vec![],
        approximate_size: 10 * 1024 * 1024,
        approximate_keys: 50_000,
    };
    let right = RegionMeta {
        region_id: 2,
        start_key: vec![0x60], // 不连续：0x55 ≠ 0x60
        end_key: vec![0xFF],
        epoch: RegionEpoch::initial(),
        peers: vec![],
        approximate_size: 5 * 1024 * 1024,
        approximate_keys: 50_000,
    };

    assert!(!checker.should_merge(&left, &right));
}

#[test]
fn test_merge_checker_total_too_large() {
    let checker = MergeChecker::new();
    let left = RegionMeta {
        region_id: 1,
        start_key: vec![0x00],
        end_key: vec![0x55],
        epoch: RegionEpoch::initial(),
        peers: vec![],
        approximate_size: 15 * 1024 * 1024, // just under threshold
        approximate_keys: 50_000,
    };
    let right = RegionMeta {
        region_id: 2,
        start_key: vec![0x55],
        end_key: vec![0xFF],
        epoch: RegionEpoch::initial(),
        peers: vec![],
        approximate_size: 14 * 1024 * 1024 * 1024, // 14 GB - 合并后远超 256 MB
        approximate_keys: 50_000,
    };

    assert!(!checker.should_merge(&left, &right));
}

// ============================================================================
// Replica Checker 测试
// ============================================================================

struct ReplicaChecker {
    target_replicas: usize,
}

#[derive(Debug, PartialEq, Eq)]
enum ReplicaAction {
    None,
    AddReplica,
    RemoveReplica(usize), // index of peer to remove
}

impl ReplicaChecker {
    fn new(target_replicas: usize) -> Self {
        Self { target_replicas }
    }

    fn check(&self, region: &RegionMeta) -> ReplicaAction {
        let voter_count = region.peers.iter().filter(|p| p.role == PeerRole::Voter).count();

        if voter_count < self.target_replicas {
            ReplicaAction::AddReplica
        } else if voter_count > self.target_replicas {
            // 移除多余的 Follower（保留 Leader 所需的 Voter）
            ReplicaAction::RemoveReplica(voter_count - 1) // 简化：移除最后一个
        } else {
            ReplicaAction::None
        }
    }
}

#[test]
fn test_replica_checker_healthy() {
    let checker = ReplicaChecker::new(3);
    let region = RegionMeta {
        region_id: 1,
        start_key: vec![],
        end_key: vec![],
        epoch: RegionEpoch::initial(),
        peers: vec![
            Peer { node_id: 1, raft_addr: "a".to_string(), role: PeerRole::Voter },
            Peer { node_id: 2, raft_addr: "b".to_string(), role: PeerRole::Voter },
            Peer { node_id: 3, raft_addr: "c".to_string(), role: PeerRole::Voter },
        ],
        approximate_size: 0,
        approximate_keys: 0,
    };

    assert_eq!(checker.check(&region), ReplicaAction::None);
}

#[test]
fn test_replica_checker_needs_replica() {
    let checker = ReplicaChecker::new(3);
    let region = RegionMeta {
        region_id: 1,
        start_key: vec![],
        end_key: vec![],
        epoch: RegionEpoch::initial(),
        peers: vec![
            Peer { node_id: 1, raft_addr: "a".to_string(), role: PeerRole::Voter },
        ],
        approximate_size: 0,
        approximate_keys: 0,
    };

    assert_eq!(checker.check(&region), ReplicaAction::AddReplica);
}

#[test]
fn test_replica_checker_too_many_replicas() {
    let checker = ReplicaChecker::new(3);
    let region = RegionMeta {
        region_id: 1,
        start_key: vec![],
        end_key: vec![],
        epoch: RegionEpoch::initial(),
        peers: vec![
            Peer { node_id: 1, raft_addr: "a".to_string(), role: PeerRole::Voter },
            Peer { node_id: 2, raft_addr: "b".to_string(), role: PeerRole::Voter },
            Peer { node_id: 3, raft_addr: "c".to_string(), role: PeerRole::Voter },
            Peer { node_id: 4, raft_addr: "d".to_string(), role: PeerRole::Voter },
            Peer { node_id: 5, raft_addr: "e".to_string(), role: PeerRole::Voter },
        ],
        approximate_size: 0,
        approximate_keys: 0,
    };

    assert!(matches!(checker.check(&region), ReplicaAction::RemoveReplica(_)));
}

#[test]
fn test_replica_checker_learners_not_counted() {
    let checker = ReplicaChecker::new(3);
    let region = RegionMeta {
        region_id: 1,
        start_key: vec![],
        end_key: vec![],
        epoch: RegionEpoch::initial(),
        peers: vec![
            Peer { node_id: 1, raft_addr: "a".to_string(), role: PeerRole::Voter },
            Peer { node_id: 2, raft_addr: "b".to_string(), role: PeerRole::Voter },
            Peer { node_id: 3, raft_addr: "c".to_string(), role: PeerRole::Learner },
        ],
        approximate_size: 0,
        approximate_keys: 0,
    };

    // 只有 2 个 Voter，需要添加
    assert_eq!(checker.check(&region), ReplicaAction::AddReplica);
}

// ============================================================================
// Balance Scheduler 测试（副本均衡）
// ============================================================================

#[test]
fn test_balance_finds_overloaded_node() {
    // 模拟节点负载：Node1 有 10 个 Region，Node2 有 2 个，Node3 有 3 个
    let node_loads = vec![
        (1u64, 10u32),
        (2u64, 2u32),
        (3u64, 3u32),
    ];

    // 平均负载 = (10 + 2 + 3) / 3 = 5
    let avg: f64 = node_loads.iter().map(|(_, c)| *c as f64).sum::<f64>() / node_loads.len() as f64;

    let overloaded: Vec<_> = node_loads
        .iter()
        .filter(|(_, c)| *c as f64 > avg * 1.2) // 超过均值 20%
        .collect();

    assert_eq!(overloaded.len(), 1);
    assert_eq!(overloaded[0].0, 1); // Node1 过载 (10 > 6.0)

    let underloaded: Vec<_> = node_loads
        .iter()
        .filter(|(_, c)| (*c as f64) < (avg * 0.8)) // 低于均值 20%
        .collect();

    // Node2 (2 < 4.0) 和 Node3 (3 < 4.0) 都欠载
    assert_eq!(underloaded.len(), 2);
    assert_eq!(underloaded[0].0, 2); // Node2
    assert_eq!(underloaded[1].0, 3); // Node3
}

#[test]
fn test_balance_no_action_when_balanced() {
    let node_loads = vec![
        (1u64, 5u32),
        (2u64, 5u32),
        (3u64, 5u32),
    ];

    let avg: f64 = node_loads.iter().map(|(_, c)| *c as f64).sum::<f64>() / node_loads.len() as f64;

    let overloaded: Vec<_> = node_loads
        .iter()
        .filter(|(_, c)| *c as f64 > avg * 1.2)
        .collect();

    assert!(overloaded.is_empty());
}

// ============================================================================
// Operator 调度操作测试
// ============================================================================

#[derive(Debug, PartialEq, Eq)]
enum Operator {
    AddPeer { region_id: RegionId, node_id: u64 },
    RemovePeer { region_id: RegionId, node_id: u64 },
    TransferLeader { region_id: RegionId, to_node: u64 },
    SplitRegion { region_id: RegionId, split_key: Vec<u8> },
    MergeRegion { left: RegionId, right: RegionId },
}

#[test]
fn test_operator_split_region() {
    let op = Operator::SplitRegion {
        region_id: 1,
        split_key: vec![0x55],
    };
    assert!(matches!(op, Operator::SplitRegion { region_id: 1, .. }));
}

#[test]
fn test_operator_add_peer() {
    let op = Operator::AddPeer {
        region_id: 1,
        node_id: 2,
    };
    assert!(matches!(op, Operator::AddPeer { region_id: 1, node_id: 2 }));
}

#[test]
fn test_operator_merge_region() {
    let op = Operator::MergeRegion { left: 1, right: 2 };
    assert!(matches!(op, Operator::MergeRegion { left: 1, right: 2 }));
}

// ============================================================================
// PD Region Meta Store 基本操作测试
// ============================================================================

#[test]
fn test_pd_meta_store_basic_crud() {
    // 模拟内存 PD Meta Store
    let mut store: std::collections::HashMap<RegionId, RegionMeta> = std::collections::HashMap::new();

    // Create
    let meta = RegionMeta {
        region_id: 1,
        start_key: vec![0x00],
        end_key: vec![0x55],
        epoch: RegionEpoch::initial(),
        peers: vec![],
        approximate_size: 0,
        approximate_keys: 0,
    };
    store.insert(1, meta.clone());

    // Read
    assert!(store.get(&1).is_some());
    assert!(store.get(&999).is_none());

    // Update
    if let Some(m) = store.get_mut(&1) {
        m.approximate_size = 1024;
    }
    assert_eq!(store.get(&1).unwrap().approximate_size, 1024);

    // Delete
    store.remove(&1);
    assert!(store.get(&1).is_none());
}

#[test]
fn test_pd_route_by_key() {
    // 模拟 PD 根据 key 查找 Region
    let mut regions: Vec<RegionMeta> = Vec::new();
    regions.push(RegionMeta {
        region_id: 1,
        start_key: vec![0x00],
        end_key: vec![0x55],
        epoch: RegionEpoch::initial(),
        peers: vec![],
        approximate_size: 0,
        approximate_keys: 0,
    });
    regions.push(RegionMeta {
        region_id: 2,
        start_key: vec![0x55],
        end_key: vec![],
        epoch: RegionEpoch::initial(),
        peers: vec![],
        approximate_size: 0,
        approximate_keys: 0,
    });

    // 按 start_key 排序
    regions.sort_by(|a, b| a.start_key.cmp(&b.start_key));

    // 二分查找
    fn find_region(regions: &[RegionMeta], key: &[u8]) -> Option<RegionId> {
        let pos = regions.binary_search_by(|r| r.start_key.as_slice().cmp(key));
        match pos {
            Ok(idx) => Some(regions[idx].region_id),
            Err(0) => None,
            Err(idx) => Some(regions[idx - 1].region_id),
        }
    }

    assert_eq!(find_region(&regions, &[0x00]), Some(1));
    assert_eq!(find_region(&regions, &[0x54]), Some(1));
    assert_eq!(find_region(&regions, &[0x55]), Some(2));
    assert_eq!(find_region(&regions, &[0xFF]), Some(2));
}
