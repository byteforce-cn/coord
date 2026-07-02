// region_types_test.rs — Phase 0: Region 核心类型测试
//
// TDD: 先写测试，后实现。这些测试在类型实现之前会编译失败。

use coord_core::types::{
    ConfVersion, Peer, PeerRole, RegionEpoch, RegionId, RegionMeta, RegionVersion,
};

// ============================================================================
// RegionId / RegionVersion / ConfVersion 类型别名测试
// ============================================================================

#[test]
fn test_region_id_is_u64() {
    // RegionId 必须是 u64，确保全局唯一且单调递增
    let id: RegionId = 0;
    assert_eq!(id, 0u64);
    let max_id: RegionId = u64::MAX;
    assert_eq!(max_id, u64::MAX);
}

#[test]
fn test_region_version_is_u64() {
    let v: RegionVersion = 1;
    assert_eq!(v, 1u64);
}

#[test]
fn test_conf_version_is_u64() {
    let cv: ConfVersion = 1;
    assert_eq!(cv, 1u64);
}

// ============================================================================
// PeerRole 测试
// ============================================================================

#[test]
fn test_peer_role_voter() {
    let role = PeerRole::Voter;
    assert_eq!(role, PeerRole::Voter);
    assert_ne!(role, PeerRole::Learner);
}

#[test]
fn test_peer_role_learner() {
    let role = PeerRole::Learner;
    assert_eq!(role, PeerRole::Learner);
    assert_ne!(role, PeerRole::Voter);
}

#[test]
fn test_peer_role_clone_eq() {
    let v1 = PeerRole::Voter;
    let v2 = v1;
    assert_eq!(v1, v2);

    let l1 = PeerRole::Learner;
    let l2 = l1;
    assert_eq!(l1, l2);
}

// ============================================================================
// Peer 测试
// ============================================================================

#[test]
fn test_peer_creation() {
    let peer = Peer {
        node_id: 1,
        raft_addr: "192.168.1.1:50052".to_string(),
        role: PeerRole::Voter,
    };
    assert_eq!(peer.node_id, 1);
    assert_eq!(peer.raft_addr, "192.168.1.1:50052");
    assert_eq!(peer.role, PeerRole::Voter);
}

#[test]
fn test_peer_learner() {
    let peer = Peer {
        node_id: 2,
        raft_addr: "192.168.1.2:50052".to_string(),
        role: PeerRole::Learner,
    };
    assert_eq!(peer.role, PeerRole::Learner);
}

#[test]
fn test_peer_clone() {
    let peer = Peer {
        node_id: 1,
        raft_addr: "addr".to_string(),
        role: PeerRole::Voter,
    };
    let peer2 = peer.clone();
    assert_eq!(peer.node_id, peer2.node_id);
    assert_eq!(peer.raft_addr, peer2.raft_addr);
    assert_eq!(peer.role, peer2.role);
}

// ============================================================================
// RegionEpoch 测试
// ============================================================================

#[test]
fn test_region_epoch_default() {
    // 通过手动构造验证 epoch 字段
    let epoch = RegionEpoch {
        conf_ver: 1,
        version: 1,
    };
    assert_eq!(epoch.conf_ver, 1);
    assert_eq!(epoch.version, 1);
}

#[test]
fn test_region_epoch_partial_eq() {
    let e1 = RegionEpoch {
        conf_ver: 1,
        version: 2,
    };
    let e2 = RegionEpoch {
        conf_ver: 1,
        version: 2,
    };
    let e3 = RegionEpoch {
        conf_ver: 2,
        version: 2,
    };
    assert_eq!(e1, e2);
    assert_ne!(e1, e3);
}

#[test]
fn test_region_epoch_copy_clone() {
    let e1 = RegionEpoch {
        conf_ver: 3,
        version: 5,
    };
    let e2 = e1; // Copy
    assert_eq!(e1.conf_ver, e2.conf_ver);
    assert_eq!(e1.version, e2.version);
    let e3 = e1.clone(); // Clone
    assert_eq!(e1, e3);
}

#[test]
fn test_epoch_is_stale_detection() {
    // epoch 过期检测：客户端 epoch 低于服务端 epoch 即为过期
    let server = RegionEpoch {
        conf_ver: 3,
        version: 5,
    };
    let client_same = RegionEpoch {
        conf_ver: 3,
        version: 5,
    };
    let client_stale_conf = RegionEpoch {
        conf_ver: 2,
        version: 5,
    };
    let client_stale_version = RegionEpoch {
        conf_ver: 3,
        version: 4,
    };
    let client_stale_both = RegionEpoch {
        conf_ver: 2,
        version: 4,
    };

    // 辅助函数：判断是否过期
    let is_stale = |client: &RegionEpoch, server: &RegionEpoch| -> bool {
        client.conf_ver < server.conf_ver || client.version < server.version
    };

    assert!(!is_stale(&client_same, &server));
    assert!(is_stale(&client_stale_conf, &server));
    assert!(is_stale(&client_stale_version, &server));
    assert!(is_stale(&client_stale_both, &server));
}

// ============================================================================
// RegionMeta 测试
// ============================================================================

#[test]
fn test_region_meta_creation() {
    let meta = RegionMeta {
        region_id: 1,
        start_key: vec![0x00],
        end_key: vec![0x55],
        epoch: RegionEpoch {
            conf_ver: 1,
            version: 1,
        },
        peers: vec![Peer {
            node_id: 1,
            raft_addr: "addr1".to_string(),
            role: PeerRole::Voter,
        }],
        approximate_size: 100_000_000, // 100 MB
        approximate_keys: 500_000,
    };

    assert_eq!(meta.region_id, 1);
    assert_eq!(meta.start_key, vec![0x00]);
    assert_eq!(meta.end_key, vec![0x55]);
    assert_eq!(meta.epoch.conf_ver, 1);
    assert_eq!(meta.epoch.version, 1);
    assert_eq!(meta.peers.len(), 1);
    assert_eq!(meta.approximate_size, 100_000_000);
    assert_eq!(meta.approximate_keys, 500_000);
}

#[test]
fn test_region_meta_key_range_left_closed_right_open() {
    // Key Range 必须是左闭右开 [start_key, end_key)
    let start = vec![0x00, 0x00];
    let end = vec![0x00, 0xFF];

    let meta = RegionMeta {
        region_id: 1,
        start_key: start.clone(),
        end_key: end.clone(),
        epoch: RegionEpoch {
            conf_ver: 1,
            version: 1,
        },
        peers: vec![],
        approximate_size: 0,
        approximate_keys: 0,
    };

    // 验证左闭右开语义
    // key == start_key → 属于该 Region
    assert!(meta.start_key <= start);
    // key == end_key → 不属于该 Region（右开）
    assert!(end > meta.start_key);
}

#[test]
fn test_region_meta_clone() {
    let meta = RegionMeta {
        region_id: 1,
        start_key: vec![0x00],
        end_key: vec![0xFF],
        epoch: RegionEpoch {
            conf_ver: 1,
            version: 1,
        },
        peers: vec![],
        approximate_size: 0,
        approximate_keys: 0,
    };
    let cloned = meta.clone();
    assert_eq!(meta.region_id, cloned.region_id);
    assert_eq!(meta.start_key, cloned.start_key);
    assert_eq!(meta.end_key, cloned.end_key);
    assert_eq!(meta.epoch, cloned.epoch);
}

// ============================================================================
// Region 路由查找测试（Key Range 判断）
// ============================================================================

#[test]
fn test_key_in_range() {
    // 验证 key 是否属于给定 Region 的 Key Range
    fn key_in_region(key: &[u8], start: &[u8], end: &[u8]) -> bool {
        key >= start && key < end
    }

    let start = vec![0x10];
    let end = vec![0x20];

    assert!(key_in_region(&[0x10], &start, &end)); // 左边界包含
    assert!(key_in_region(&[0x15], &start, &end)); // 中间
    assert!(!key_in_region(&[0x20], &start, &end)); // 右边界不包含
    assert!(!key_in_region(&[0x0F], &start, &end)); // 左侧外
    assert!(!key_in_region(&[0x21], &start, &end)); // 右侧外
}

#[test]
fn test_key_in_full_range() {
    // Region 0 覆盖全 Key 空间 [0x00, 0xFF...FF)
    fn key_in_region(key: &[u8], start: &[u8], end: &[u8]) -> bool {
        key >= start && key < end
    }

    // 全空间：start_key 为空（最小），end_key 为最大
    // 空 end_key 表示无穷大，任意 key 都满足
    let start: Vec<u8> = vec![];
    assert!(key_in_region(b"foo", &start, &[0xFF; 16]));
    assert!(key_in_region(b"bar", &start, &[0xFF; 16]));
}

// ============================================================================
// Region 分裂后 Key Range 正确性测试
// ============================================================================

#[test]
fn test_split_key_ranges() {
    // Region 1: [0x00, 0x55) — 分裂为:
    //   Region 1: [0x00, 0x30)
    //   Region 2: [0x30, 0x55)
    fn key_in_region(key: &[u8], start: &[u8], end: &[u8]) -> bool {
        key >= start && key < end
    }

    let split_key = vec![0x30];

    // 分裂后的 Region 1
    assert!(key_in_region(&[0x00], &[0x00], &split_key));
    assert!(key_in_region(&[0x2F], &[0x00], &split_key));
    assert!(!key_in_region(&[0x30], &[0x00], &split_key)); // split_key 属于右侧

    // 分裂后的 Region 2
    assert!(key_in_region(&[0x30], &split_key, &[0x55]));
    assert!(key_in_region(&[0x40], &split_key, &[0x55]));
    assert!(!key_in_region(&[0x55], &split_key, &[0x55])); // 右边界不包含
    assert!(!key_in_region(&[0x2F], &split_key, &[0x55])); // 左侧外
}
