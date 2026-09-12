// pd_test.rs — Placement Driver **真实实现**测试（E3 整改）
//
// 整改背景（2026-09-12 架构评审复核 E3）：
// 本文件此前自带一份 `NodeState` 副本 + 自洽断言 —— 验证的是"测试文件自己的
// 副本模型"，与 `coord_server::pd` 的真实实现完全无关。即使 PD 实现被删空，
// 这些测试依然全绿（假验证）。
//
// 现在全部改为直连真实 PD 类型：
//   - `coord_server::pd::types`（NodeState / PlacementConstraint / PdConfig）
//   - `coord_server::pd::scheduler`（SplitChecker / MergeChecker / KeySampler）
//   - `coord_server::pd::meta_store::PdMetaStore`（持久化 Region 元数据）

use std::collections::HashMap;
use std::time::Duration;

use coord_core::types::{RegionEpoch, RegionId, RegionMeta};
use coord_server::pd::meta_store::PdMetaStore;
use coord_server::pd::scheduler::{KeySampler, MergeChecker, SplitChecker};
use coord_server::pd::types::{NodeState, PdConfig, PlacementConstraint};

const MIB: u64 = 1024 * 1024;

// ──── 夹具（构造**真实** PD 类型）────

fn node(id: u64, host: &str, zone: &str) -> NodeState {
    let mut n = NodeState::new(id, format!("{host}:50052"), format!("{host}:50051"));
    n.labels.insert("host".to_string(), host.to_string());
    n.labels.insert("zone".to_string(), zone.to_string());
    n.capacity_bytes = 1 << 40; // 1 TiB
    n
}

fn region(id: RegionId, start: &[u8], end: &[u8], size: u64, keys: u64) -> RegionMeta {
    RegionMeta {
        region_id: id,
        start_key: start.to_vec(),
        end_key: end.to_vec(),
        epoch: RegionEpoch::initial(),
        peers: vec![],
        approximate_size: size,
        approximate_keys: keys,
    }
}

/// 阈值收紧后的 PD 配置：split 阈值 = 1 MiB，merge 候选阈值 = 1 MiB
/// （`MergeChecker` 的 max_merge_size 取自 region_split_size_mb）。
fn pd_config() -> PdConfig {
    PdConfig {
        region_split_size_mb: 1,
        region_split_keys: 1_000,
        region_merge_size_mb: 1,
        ..PdConfig::default()
    }
}

// ──── NodeState（真实类型）────

#[test]
fn node_state_available_capacity() {
    let mut n = node(1, "h1", "z1");
    assert_eq!(n.node_id, 1);
    assert_eq!(n.available_bytes(), 1 << 40);

    n.used_bytes = 300;
    assert_eq!(n.available_bytes(), (1 << 40) - 300);
}

#[test]
fn node_state_heartbeat_drives_online_check() {
    let mut n = node(1, "h1", "z1");
    // 从未收到心跳 → 不在线
    assert!(!n.is_online(Duration::from_secs(30)));

    n.record_heartbeat();
    assert!(n.online, "record_heartbeat must set online=true");
    assert!(n.is_online(Duration::from_secs(30)));

    // 真实语义（由本测试固化）：`is_online(timeout)` 只看**心跳新鲜度**；
    // `mark_offline()` 只置 `online` 标志位（供 operator 显式置离线），
    // 不会让最近有过心跳的节点立即被判离线。两者语义不同，不可混用。
    n.mark_offline();
    assert!(!n.online);
    assert!(
        n.is_online(Duration::from_secs(30)),
        "is_online is heartbeat-recency based, distinct from the online flag"
    );

    // 心跳超出容忍窗口 → 判离线
    assert!(!n.is_online(Duration::from_nanos(0)));
}

// ──── PlacementConstraint（真实拓扑约束）────

#[test]
fn placement_rejects_existing_replica_node() {
    let c = PlacementConstraint::default();
    let nodes: HashMap<u64, NodeState> = [(1, node(1, "h1", "z1")), (2, node(2, "h2", "z2"))]
        .into_iter()
        .collect();
    let target = nodes.get(&1).unwrap();
    assert!(!c.can_place(target, &[1], &nodes), "peer node must be rejected");
}

#[test]
fn placement_forbids_same_host_by_default() {
    let c = PlacementConstraint::default();
    assert!(c.forbid_same_host);

    let nodes: HashMap<u64, NodeState> = [(1, node(1, "h1", "z1")), (2, node(2, "h1", "z2"))]
        .into_iter()
        .collect();
    let target = nodes.get(&2).unwrap();
    assert!(
        !c.can_place(target, &[1], &nodes),
        "same host must be rejected when forbid_same_host=true"
    );
}

#[test]
fn placement_allows_different_host() {
    let c = PlacementConstraint::default();
    let nodes: HashMap<u64, NodeState> = [(1, node(1, "h1", "z1")), (2, node(2, "h2", "z1"))]
        .into_iter()
        .collect();
    let target = nodes.get(&2).unwrap();
    assert!(c.can_place(target, &[1], &nodes));
}

#[test]
fn placement_forbids_same_zone_when_enabled() {
    let c = PlacementConstraint {
        forbid_same_host: false,
        forbid_same_zone: true,
        ..PlacementConstraint::default()
    };
    let nodes: HashMap<u64, NodeState> = [(1, node(1, "h1", "z1")), (2, node(2, "h2", "z1"))]
        .into_iter()
        .collect();
    let target = nodes.get(&2).unwrap();
    assert!(
        !c.can_place(target, &[1], &nodes),
        "same zone must be rejected when forbid_same_zone=true"
    );
}

#[test]
fn select_best_node_prefers_diverse_zone() {
    let c = PlacementConstraint::default();
    let nodes: HashMap<u64, NodeState> = [
        (1, node(1, "h1", "z1")),
        (2, node(2, "h2", "z1")), // same zone as peer
        (3, node(3, "h3", "z2")), // different zone
    ]
    .into_iter()
    .collect();

    let candidates: Vec<&NodeState> = nodes.values().collect();
    let best = c
        .select_best_node(&candidates, &[1], &nodes)
        .expect("a placement candidate must exist");
    assert_ne!(best.node_id, 1, "existing peer must not be selected");
    assert_eq!(
        best.node_id, 3,
        "diverse-zone node must be preferred (got {})",
        best.node_id
    );
}

// ──── SplitChecker（真实阈值判定）────

#[test]
fn split_checker_below_threshold_no_op() {
    let checker = SplitChecker::new(&pd_config());
    let r = region(1, b"a", b"z", MIB - 1, 999);
    assert!(checker.check(&r, b"m".to_vec()).is_none());
}

#[test]
fn split_checker_size_threshold_triggers() {
    let checker = SplitChecker::new(&pd_config());
    let r = region(7, b"a", b"z", MIB, 0); // >= 阈值
    let op = checker.check(&r, b"m".to_vec()).expect("must split");
    match op {
        coord_server::pd::Operator::SplitRegion {
            region_id,
            split_key,
            ..
        } => {
            assert_eq!(region_id, 7);
            assert_eq!(split_key, b"m".to_vec(), "must use the provided split key");
        }
        other => panic!("expected SplitRegion, got {other:?}"),
    }
}

#[test]
fn split_checker_keys_threshold_triggers() {
    let checker = SplitChecker::new(&pd_config());
    let r = region(8, b"a", b"z", 1, 1_000); // >= keys 阈值
    assert!(checker.check(&r, b"m".to_vec()).is_some());
}

// ──── MergeChecker（真实相邻/大小判定）────

#[test]
fn merge_checker_rejects_non_adjacent() {
    let checker = MergeChecker::new(&pd_config());
    let left = region(1, b"a", b"m", MIB / 4, 10);
    let right = region(2, b"n", b"z", MIB / 4, 10); // 不连续（m != n）
    assert!(checker.check(&left, &right).is_none());
}

#[test]
fn merge_checker_merges_small_adjacent() {
    let checker = MergeChecker::new(&pd_config());
    let left = region(1, b"a", b"m", MIB / 4, 10);
    let right = region(2, b"m", b"z", MIB / 4, 10);
    let op = checker.check(&left, &right).expect("must merge");
    match op {
        coord_server::pd::Operator::MergeRegion { left, right } => {
            assert_eq!((left, right), (1, 2));
        }
        other => panic!("expected MergeRegion, got {other:?}"),
    }
}

#[test]
fn merge_checker_rejects_when_one_side_over_threshold() {
    let checker = MergeChecker::new(&pd_config());
    let left = region(1, b"a", b"m", MIB, 10); // >= merge 阈值
    let right = region(2, b"m", b"z", MIB / 4, 10);
    assert!(checker.check(&left, &right).is_none());
}

#[test]
fn merge_checker_rejects_when_combined_too_large() {
    let checker = MergeChecker::new(&pd_config());
    // 各自 < 1 MiB，但合计 1.2 MiB >= max_merge_size（= region_split_size_mb）
    let left = region(1, b"a", b"m", (MIB * 6) / 10, 10);
    let right = region(2, b"m", b"z", (MIB * 6) / 10, 10);
    assert!(checker.check(&left, &right).is_none());
}

// ──── KeySampler（真实采样/中位数）────

#[test]
fn key_sampler_selects_median() {
    let sampler = KeySampler::new(100);
    let samples = vec![b"c".to_vec(), b"a".to_vec(), b"b".to_vec()];
    assert_eq!(sampler.select_split_key(&samples), Some(b"b".to_vec()));

    assert_eq!(sampler.select_split_key(&[]), None);
}

#[test]
fn key_sampler_or_fallback_stays_within_range() {
    let sampler = KeySampler::new(100);
    let start = b"a".to_vec();
    let end = b"z".to_vec();

    // 无样本 → 数学中点，必须落在 [start, end)
    let mid = sampler.select_or_fallback(&[], &start, &end);
    assert!(mid >= start && mid < end, "mid key must stay in range");

    // 有样本 → 中位数（不得越过区间）
    let samples = vec![b"b".to_vec(), b"m".to_vec(), b"y".to_vec()];
    let picked = sampler.select_or_fallback(&samples, &start, &end);
    assert!(picked >= start && picked < end);
}

#[test]
fn key_sampler_reservoir_sample_bounded_and_from_input() {
    let sampler = KeySampler::new(100);
    let keys: Vec<Vec<u8>> = (0..10_000u32).map(|i| i.to_string().into_bytes()).collect();
    let sampled = sampler.reservoir_sample(keys.clone());

    assert_eq!(sampled.len(), 100, "must respect max_samples");
    for s in &sampled {
        assert!(keys.contains(s), "sample must come from the input keys");
    }
}

// ──── PdMetaStore（真实持久化 Region 元数据）────

fn open_store() -> (tempfile::TempDir, PdMetaStore) {
    let dir = tempfile::tempdir().unwrap();
    let store = PdMetaStore::open(dir.path()).expect("PdMetaStore::open");
    (dir, store)
}

#[test]
fn meta_store_create_and_lookup_by_key() {
    let (_dir, store) = open_store();
    store.create_region(region(1, b"", b"m", 0, 0)).unwrap();
    store.create_region(region(2, b"m", b"", 0, 0)).unwrap();

    assert_eq!(store.region_count(), 2);
    assert_eq!(store.get_region_by_key(b"apple").map(|r| r.region_id), Some(1));
    assert_eq!(store.get_region_by_key(b"peach").map(|r| r.region_id), Some(2));
    assert!(store.get_region_by_key(b"zebra").is_some());
    assert_eq!(store.get_region(1).map(|r| r.end_key), Some(b"m".to_vec()));
}

#[test]
fn meta_store_rejects_duplicate_start_key() {
    let (_dir, store) = open_store();
    store.create_region(region(1, b"a", b"m", 0, 0)).unwrap();
    assert!(
        store.create_region(region(2, b"a", b"z", 0, 0)).is_err(),
        "duplicate start_key must be rejected"
    );
}

#[test]
fn meta_store_scan_and_adjacent_pairs() {
    let (_dir, store) = open_store();
    store.create_region(region(1, b"", b"m", 0, 0)).unwrap();
    store.create_region(region(2, b"m", b"t", 0, 0)).unwrap();
    store.create_region(region(3, b"t", b"", 0, 0)).unwrap();

    let scanned = store.scan_regions(b"m", 10);
    assert_eq!(
        scanned.iter().map(|r| r.region_id).collect::<Vec<_>>(),
        vec![2, 3],
        "scan must return regions at/after the start key, in order"
    );

    let pairs = store.get_adjacent_pairs();
    assert_eq!(pairs.len(), 2, "3 regions → 2 adjacent pairs");
    let ids: Vec<(RegionId, RegionId)> = pairs
        .iter()
        .map(|(l, r)| (l.region_id, r.region_id))
        .collect();
    assert_eq!(ids, vec![(1, 2), (2, 3)]);
}

#[test]
fn meta_store_delete_removes_region_and_key_index() {
    let (_dir, store) = open_store();
    store.create_region(region(1, b"", b"z", 0, 0)).unwrap();
    store.delete_region(1).unwrap();

    assert_eq!(store.region_count(), 0);
    assert!(store.get_region(1).is_none());
    assert!(
        store.get_region_by_key(b"anything").is_none(),
        "key index must be cleaned up on delete"
    );
}

#[test]
fn meta_store_allocate_region_id_follows_max_region_id() {
    let (_dir, store) = open_store();
    // 空 store：当前实现（基于最大 Region ID + 1）返回 1
    assert_eq!(store.allocate_region_id(), 1);

    store.create_region(region(5, b"a", b"m", 0, 0)).unwrap();
    assert_eq!(store.allocate_region_id(), 6);

    store.create_region(region(7, b"m", b"z", 0, 0)).unwrap();
    assert_eq!(store.allocate_region_id(), 8);

    // 分配仅是**建议值**：尚未落库，由调用方 create/update（failover 场景需
    // 走 raft 共识，见 PdMetaStore 文档注释）。
    assert!(store.get_region(8).is_none());
}
