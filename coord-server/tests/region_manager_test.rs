// region_manager_test.rs — RegionManager 测试
//
// TDD: 测试 RegionManager 的核心功能（路由、注册、Epoch 校验）
// 测试 coord_server::raft::region 模块的 RegionManager 和 RegionHandle

use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionId, RegionMeta};
use coord_server::raft::region::{RegionHandle, RegionManager, RegionRole};

// ============================================================================
// RegionManager 创建与基本操作测试
// ============================================================================

fn make_test_meta(region_id: RegionId, start: Vec<u8>, end: Vec<u8>) -> RegionMeta {
    RegionMeta {
        region_id,
        start_key: start,
        end_key: end,
        epoch: RegionEpoch::initial(),
        peers: vec![Peer {
            node_id: 1,
            raft_addr: "127.0.0.1:50052".to_string(),
            role: PeerRole::Voter,
        }],
        approximate_size: 0,
        approximate_keys: 0,
    }
}

#[test]
fn test_region_manager_create() {
    let rm = RegionManager::new(1);
    assert_eq!(rm.node_id(), 1);
    assert_eq!(rm.region_count(), 0);
    assert_eq!(rm.leader_count(), 0);
}

#[test]
fn test_register_and_get_region() {
    let rm = RegionManager::new(1);
    let meta = make_test_meta(1, vec![0x00], vec![0x55]);
    let handle = rm.register_region(meta).unwrap();
    assert_eq!(handle.region_id(), 1);
    assert_eq!(rm.region_count(), 1);
    assert!(rm.get_region(1).is_some());
    assert!(rm.get_region(999).is_none());
}

#[test]
fn test_register_duplicate_region_fails() {
    let rm = RegionManager::new(1);
    rm.register_region(make_test_meta(1, vec![0x00], vec![0x55]))
        .unwrap();
    let result = rm.register_region(make_test_meta(1, vec![0x00], vec![0x55]));
    assert!(result.is_err());
}

#[test]
fn test_unregister_region() {
    let rm = RegionManager::new(1);
    rm.register_region(make_test_meta(1, vec![0x00], vec![0x55]))
        .unwrap();
    assert_eq!(rm.region_count(), 1);
    rm.unregister_region(1).unwrap();
    assert_eq!(rm.region_count(), 0);
    assert!(rm.get_region(1).is_none());
}

#[test]
fn test_unregister_nonexistent_fails() {
    let rm = RegionManager::new(1);
    let result = rm.unregister_region(999);
    assert!(result.is_err());
}

#[test]
fn test_route_single_region() {
    let rm = RegionManager::new(1);
    rm.register_region(make_test_meta(1, vec![], vec![]))
        .unwrap();
    assert!(rm.route(b"hello").is_ok());
}

#[test]
fn test_route_multiple_regions() {
    let rm = RegionManager::new(1);
    rm.register_region(make_test_meta(1, vec![0x00], vec![0x55]))
        .unwrap();
    rm.register_region(make_test_meta(2, vec![0x55], vec![0xFF]))
        .unwrap();

    assert_eq!(rm.route(&[0x00]).unwrap().region_id(), 1);
    assert_eq!(rm.route(&[0x54]).unwrap().region_id(), 1);
    assert_eq!(rm.route(&[0x55]).unwrap().region_id(), 2);
    assert_eq!(rm.route(&[0xFE]).unwrap().region_id(), 2);
}

#[test]
fn test_route_empty_registry_fails() {
    let rm = RegionManager::new(1);
    assert!(rm.route(b"hello").is_err());
}

#[test]
fn test_route_before_first_region() {
    // 如果 key < 最小的 start_key，应返回错误
    let rm = RegionManager::new(1);
    rm.register_region(make_test_meta(1, vec![0x10], vec![0x20]))
        .unwrap();
    // key 0x05 < start_key 0x10
    assert!(rm.route(&[0x05]).is_err());
}

#[test]
fn test_list_regions() {
    let rm = RegionManager::new(1);
    rm.register_region(make_test_meta(1, vec![0x00], vec![0x55]))
        .unwrap();
    rm.register_region(make_test_meta(2, vec![0x55], vec![0xFF]))
        .unwrap();

    let regions = rm.list_regions();
    assert_eq!(regions.len(), 2);
}

#[test]
fn test_leader_count() {
    let rm = RegionManager::new(1);
    let h1 = rm
        .register_region(make_test_meta(1, vec![0x00], vec![0x55]))
        .unwrap();
    let _h2 = rm
        .register_region(make_test_meta(2, vec![0x55], vec![0xFF]))
        .unwrap();

    assert_eq!(rm.leader_count(), 0);
    h1.set_role(RegionRole::Leader);
    assert_eq!(rm.leader_count(), 1);
}

#[test]
fn test_region_handle_check_epoch() {
    let meta = make_test_meta(1, vec![0x00], vec![0x55]);
    let handle = RegionHandle::new(meta);

    // 相同的 epoch
    assert!(handle
        .check_epoch(&RegionEpoch {
            conf_ver: 1,
            version: 1
        })
        .is_ok());

    // 过期的 conf_ver
    assert!(handle
        .check_epoch(&RegionEpoch {
            conf_ver: 0,
            version: 1
        })
        .is_err());

    // 过期的 version
    assert!(handle
        .check_epoch(&RegionEpoch {
            conf_ver: 1,
            version: 0
        })
        .is_err());
}

#[test]
fn test_region_handle_increment_epoch() {
    let meta = make_test_meta(1, vec![0x00], vec![0x55]);
    let handle = RegionHandle::new(meta);

    assert_eq!(handle.epoch().conf_ver, 1);
    handle.increment_conf_ver();
    assert_eq!(handle.epoch().conf_ver, 2);

    assert_eq!(handle.epoch().version, 1);
    handle.increment_version();
    assert_eq!(handle.epoch().version, 2);
}

#[test]
fn test_update_region_key_range() {
    let rm = RegionManager::new(1);
    rm.register_region(make_test_meta(1, vec![0x00], vec![0xFF]))
        .unwrap();

    // 分裂后更新 Region 1 的范围
    rm.update_region_key_range(1, vec![0x00], vec![0x55])
        .unwrap();

    let region = rm.get_region(1).unwrap();
    assert_eq!(region.meta.read().start_key, vec![0x00]);
    assert_eq!(region.meta.read().end_key, vec![0x55]);
}

// ============================================================================
// Region 路由查找测试（BTreeMap + 二分查找）
// ============================================================================

#[test]
fn test_route_order_independent() {
    // 注册顺序不应影响路由结果
    let rm = RegionManager::new(1);
    rm.register_region(make_test_meta(3, vec![0x80], vec![0xFF]))
        .unwrap();
    rm.register_region(make_test_meta(1, vec![0x00], vec![0x40]))
        .unwrap();
    rm.register_region(make_test_meta(2, vec![0x40], vec![0x80]))
        .unwrap();

    assert_eq!(rm.route(&[0x00]).unwrap().region_id(), 1);
    assert_eq!(rm.route(&[0x3F]).unwrap().region_id(), 1);
    assert_eq!(rm.route(&[0x40]).unwrap().region_id(), 2);
    assert_eq!(rm.route(&[0x7F]).unwrap().region_id(), 2);
    assert_eq!(rm.route(&[0x80]).unwrap().region_id(), 3);
}

#[test]
fn test_route_after_split() {
    // 模拟分裂：Region 1 [0x00, max) → Region 1 [0x00, 0x55) + Region 2 [0x55, max)
    let rm = RegionManager::new(1);
    rm.register_region(make_test_meta(1, vec![0x00], vec![]))
        .unwrap();

    // 先更新 Region 1 的范围
    rm.update_region_key_range(1, vec![0x00], vec![0x55])
        .unwrap();

    // 注册 Region 2
    rm.register_region(make_test_meta(2, vec![0x55], vec![]))
        .unwrap();

    assert_eq!(rm.route(&[0x00]).unwrap().region_id(), 1);
    assert_eq!(rm.route(&[0x54]).unwrap().region_id(), 1);
    assert_eq!(rm.route(&[0x55]).unwrap().region_id(), 2);
    assert_eq!(rm.route(&[0xFF]).unwrap().region_id(), 2);
}

// ============================================================================
// Region 分裂后的 Key Range 非重叠测试
// ============================================================================

#[test]
fn test_split_creates_non_overlapping_ranges() {
    let rm = RegionManager::new(1);
    rm.register_region(make_test_meta(1, vec![0x00], vec![0x55]))
        .unwrap();
    rm.register_region(make_test_meta(2, vec![0x55], vec![0xFF]))
        .unwrap();

    // 确保每个 key 恰好属于一个 Region
    for key in 0x00u8..=0xFE {
        let k = vec![key];
        let in_r1 = rm.get_region(1).unwrap().contains_key(&k);
        let in_r2 = rm.get_region(2).unwrap().contains_key(&k);
        assert!(
            in_r1 ^ in_r2,
            "key {:02x}: r1={}, r2={} (should be exactly one)",
            key,
            in_r1,
            in_r2
        );
    }
}

// ============================================================================
// Peer Voter 过滤测试
// ============================================================================

#[test]
fn test_voter_peers_filter() {
    let meta = RegionMeta {
        region_id: 1,
        start_key: vec![],
        end_key: vec![],
        epoch: RegionEpoch::initial(),
        peers: vec![
            Peer {
                node_id: 1,
                raft_addr: "a".to_string(),
                role: PeerRole::Voter,
            },
            Peer {
                node_id: 2,
                raft_addr: "b".to_string(),
                role: PeerRole::Voter,
            },
            Peer {
                node_id: 3,
                raft_addr: "c".to_string(),
                role: PeerRole::Learner,
            },
        ],
        approximate_size: 0,
        approximate_keys: 0,
    };

    let voters: Vec<_> = meta.voter_peers().collect();
    assert_eq!(voters.len(), 2);
    assert_eq!(voters[0].node_id, 1);
    assert_eq!(voters[1].node_id, 2);
}

// ============================================================================
// RegionHandle 生命周期测试
// ============================================================================

#[test]
fn test_region_handle_update_stats() {
    let meta = make_test_meta(1, vec![0x00], vec![0x55]);
    let handle = RegionHandle::new(meta);

    assert_eq!(handle.meta.read().approximate_size, 0);
    assert_eq!(handle.meta.read().approximate_keys, 0);

    handle.update_stats(1024 * 1024, 1000);
    assert_eq!(handle.meta.read().approximate_size, 1024 * 1024);
    assert_eq!(handle.meta.read().approximate_keys, 1000);
}

#[test]
fn test_region_handle_role_transition() {
    let meta = make_test_meta(1, vec![0x00], vec![0x55]);
    let handle = RegionHandle::new(meta);

    assert_eq!(handle.role(), RegionRole::Unknown);
    assert!(!handle.role().is_leader());

    handle.set_role(RegionRole::Leader);
    assert_eq!(handle.role(), RegionRole::Leader);
    assert!(handle.role().is_leader());

    handle.set_role(RegionRole::Follower);
    assert_eq!(handle.role(), RegionRole::Follower);
    assert!(!handle.role().is_leader());
}

#[test]
fn test_region_handle_contains_key() {
    let meta = make_test_meta(1, vec![0x10], vec![0x20]);
    let handle = RegionHandle::new(meta);

    assert!(handle.contains_key(&[0x10]));
    assert!(handle.contains_key(&[0x15]));
    assert!(!handle.contains_key(&[0x20]));
    assert!(!handle.contains_key(&[0x0F]));
}

// ============================================================================
// 跨 Region Range 分段（v2 扇出路由）
// ============================================================================

use coord_core::error::Error;
use coord_server::raft::region::RegionRangeSegment;

fn seg(region_id: RegionId, start: &[u8], end: &[u8]) -> RegionRangeSegment {
    RegionRangeSegment {
        region_id,
        start_key: start.to_vec(),
        end_key: end.to_vec(),
    }
}

fn tiled_manager() -> RegionManager {
    let rm = RegionManager::new(1);
    rm.register_region(make_test_meta(1, b"".to_vec(), b"d".to_vec()))
        .unwrap();
    rm.register_region(make_test_meta(2, b"d".to_vec(), b"n".to_vec()))
        .unwrap();
    rm.register_region(make_test_meta(3, b"n".to_vec(), b"".to_vec()))
        .unwrap();
    rm
}

#[test]
fn test_resolve_range_splits_at_region_boundaries() {
    let rm = tiled_manager();
    assert_eq!(
        rm.resolve_range(b"a", b"z").unwrap(),
        vec![seg(1, b"a", b"d"), seg(2, b"d", b"n"), seg(3, b"n", b"z")]
    );
    assert_eq!(
        rm.resolve_range(b"d", b"n").unwrap(),
        vec![seg(2, b"d", b"n")]
    );
    assert_eq!(
        rm.resolve_range(b"", b"c").unwrap(),
        vec![seg(1, b"", b"c")]
    );
    // 无界末 Region：分段终点 = 请求终点。
    assert_eq!(
        rm.resolve_range(b"n", b"zzz").unwrap(),
        vec![seg(3, b"n", b"zzz")]
    );
    // 空区间不路由。
    assert!(rm.resolve_range(b"n", b"d").unwrap().is_empty());
    // 调用方必须先把单键换算成非空上界。
    assert!(matches!(
        rm.resolve_range(b"d", b""),
        Err(Error::InvalidArgument(_))
    ));
}

#[test]
fn test_resolve_range_rejects_gap_and_unroutable_start() {
    let gap = RegionManager::new(1);
    gap.register_region(make_test_meta(1, b"".to_vec(), b"d".to_vec()))
        .unwrap();
    gap.register_region(make_test_meta(3, b"n".to_vec(), b"".to_vec()))
        .unwrap();
    assert!(matches!(
        gap.resolve_range(b"a", b"z"),
        Err(Error::RouteNotReady)
    ));

    let starts_at_d = RegionManager::new(1);
    starts_at_d
        .register_region(make_test_meta(2, b"d".to_vec(), b"".to_vec()))
        .unwrap();
    assert!(matches!(
        starts_at_d.resolve_range(b"a", b"z"),
        Err(Error::RouteNotReady)
    ));
}

// ============================================================================
// 权威 Region 表收敛（#159）
// ============================================================================

#[test]
fn test_apply_region_table_adds_updates_and_removes() {
    use coord_server::storage::region_table::RegionTable;

    let rm = RegionManager::new(1);
    rm.register_region(make_test_meta(1, vec![], vec![]))
        .unwrap();

    let mut table = RegionTable::default();
    table.init(vec![make_test_meta(1, vec![], vec![])]).unwrap();
    assert!(rm.apply_region_table(&table).unwrap());
    assert_eq!(rm.table_version(), 1);

    let split = RegionTable {
        version: 2,
        regions: vec![
            make_test_meta(1, vec![], b"m".to_vec()),
            make_test_meta(2, b"m".to_vec(), vec![]),
        ],
    };
    assert!(rm.apply_region_table(&split).unwrap());
    assert_eq!(rm.route(b"a").unwrap().region_id(), 1);
    assert_eq!(rm.route(b"z").unwrap().region_id(), 2);
    assert_eq!(rm.get_region(1).unwrap().meta.read().end_key, b"m".to_vec());

    let merged = RegionTable {
        version: 3,
        regions: vec![make_test_meta(1, vec![], vec![])],
    };
    assert!(rm.apply_region_table(&merged).unwrap());
    assert!(rm.get_region(2).is_none());
    assert_eq!(rm.route(b"z").unwrap().region_id(), 1);
    assert!(
        !rm.apply_region_table(&merged).unwrap(),
        "stale version is a no-op"
    );
}

#[test]
fn test_apply_region_table_keeps_local_approximate_stats() {
    use coord_server::storage::region_table::RegionTable;

    let rm = RegionManager::new(1);
    let handle = rm
        .register_region(make_test_meta(1, vec![], vec![]))
        .unwrap();
    handle.update_stats(4_096, 12);

    let mut table = RegionTable::default();
    table
        .init(vec![
            make_test_meta(1, vec![], b"k".to_vec()),
            make_test_meta(2, b"k".to_vec(), vec![]),
        ])
        .unwrap();
    let mut updated = table.regions[0].clone();
    updated.epoch.conf_ver = 2;
    let table = RegionTable {
        version: 1,
        regions: vec![updated, table.regions[1].clone()],
    };
    assert!(rm.apply_region_table(&table).unwrap());

    let meta = rm.get_region(1).unwrap().meta.read().clone();
    assert_eq!(meta.end_key, b"k".to_vec());
    assert_eq!(meta.approximate_size, 4_096);
    assert_eq!(meta.approximate_keys, 12);
}
