// split_merge_test.rs — Region Split/Merge 正确性测试（**真实 PD 实现**，E3 整改）
//
// 整改背景（2026-09-12 架构评审复核 E3）：
// 本文件此前只 import `coord_core::types`，用自带的局部函数重新实现 split/merge
// 规则并对其断言 —— 验证的是"测试自己的模型"，`coord_server::pd` 的真实实现即使
// 被删空也不会红。现在全部改为驱动真实 PD 组件：
//   - `SplitChecker` / `MergeChecker` 做真实阈值判定；
//   - `KeySampler` 选择 split key；
//   - `PdMetaStore` 落真实 Region 元数据，用于断言**区间连续性与全覆盖**。

use coord_core::types::{RegionEpoch, RegionId, RegionMeta};
use coord_server::pd::meta_store::PdMetaStore;
use coord_server::pd::scheduler::{KeySampler, MergeChecker, SplitChecker};
use coord_server::pd::types::PdConfig;

const MIB: u64 = 1024 * 1024;

fn pd_config() -> PdConfig {
    PdConfig {
        region_split_size_mb: 1,
        region_split_keys: 1_000,
        region_merge_size_mb: 1,
        ..PdConfig::default()
    }
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

fn open_store() -> (tempfile::TempDir, PdMetaStore) {
    let dir = tempfile::tempdir().unwrap();
    let store = PdMetaStore::open(dir.path()).expect("PdMetaStore::open");
    (dir, store)
}

// ══════════════════════════════════════════════════════════════════
// Split
// ══════════════════════════════════════════════════════════════════

#[test]
fn split_key_must_be_strictly_inside_range() {
    // split key 由 KeySampler 从真实样本中选出，必须落在 [start, end)
    let sampler = KeySampler::new(100);
    let start = vec![0x10u8];
    let end = vec![0x50u8];
    let samples = vec![vec![0x20u8], vec![0x30u8], vec![0x40u8]];

    let split_key = sampler.select_or_fallback(&samples, &start, &end);
    assert!(split_key.as_slice() >= start.as_slice());
    assert!(split_key.as_slice() < end.as_slice());
}

#[test]
fn split_creates_two_adjacent_regions_covering_original_range() {
    let (_dir, store) = open_store();

    // 源 Region：["", "z")，已超过真实 split 阈值
    let original = region(1, b"", b"z", MIB, 0);
    let checker = SplitChecker::new(&pd_config());
    let op = checker
        .check(&original, b"m".to_vec())
        .expect("over-threshold region must split");
    let split_key = match op {
        coord_server::pd::Operator::SplitRegion { split_key, .. } => split_key,
        other => panic!("expected SplitRegion, got {other:?}"),
    };

    // 落库：原 Region 收窄为左半，新 Region 承接右半（真实元数据）
    store.create_region(original).unwrap();
    let mut left = store.get_region(1).unwrap();
    left.end_key = split_key.clone();
    store.update_region(left).unwrap();
    store
        .create_region(region(2, &split_key, b"z", (MIB * 3) / 4, 0))
        .unwrap();

    let mut regions = store.list_regions();
    regions.sort_by(|a, b| a.start_key.cmp(&b.start_key));
    assert_eq!(regions.len(), 2);

    // 相邻 + 拼接后覆盖原区间 ["", "z")
    assert_eq!(regions[0].end_key, regions[1].start_key, "must be adjacent");
    assert!(regions[0].start_key.is_empty());
    assert_eq!(regions[1].end_key, b"z".to_vec());

    // 每个 key 恰好归属一个 Region
    assert_eq!(store.get_region_by_key(b"").unwrap().region_id, 1);
    assert_eq!(store.get_region_by_key(b"m").unwrap().region_id, 2);
    assert_eq!(store.get_region_by_key(b"y").unwrap().region_id, 2);
}

#[test]
fn split_region_ids_are_unique_in_store() {
    let (_dir, store) = open_store();
    store.create_region(region(1, b"", b"", 0, 0)).unwrap();
    let new_id = store.allocate_region_id();

    let mut left = store.get_region(1).unwrap();
    left.end_key = b"m".to_vec();
    store.update_region(left).unwrap();
    store
        .create_region(region(new_id, b"m", b"", 0, 0))
        .unwrap();

    let ids: Vec<RegionId> = store.list_regions().iter().map(|r| r.region_id).collect();
    assert_eq!(ids.len(), 2);
    assert_ne!(ids[0], ids[1], "split must allocate a distinct region id");
}

#[test]
fn split_not_triggered_below_both_thresholds() {
    let checker = SplitChecker::new(&pd_config());
    let r = region(1, b"", b"z", MIB - 1, 999);
    assert!(checker.check(&r, b"m".to_vec()).is_none());
}

#[test]
fn split_triggered_by_key_count_alone() {
    let checker = SplitChecker::new(&pd_config());
    let r = region(1, b"", b"z", 1, 1_000); // 大小很小但 key 数达阈值
    assert!(checker.check(&r, b"m".to_vec()).is_some());
}

// ══════════════════════════════════════════════════════════════════
// Merge
// ══════════════════════════════════════════════════════════════════

#[test]
fn merge_requires_adjacency() {
    let checker = MergeChecker::new(&pd_config());
    let left = region(1, b"", b"m", MIB / 4, 10);
    let right = region(2, b"n", b"z", MIB / 4, 10); // 不连续
    assert!(checker.check(&left, &right).is_none());
}

#[test]
fn merge_small_adjacent_regions_restores_single_range() {
    let checker = MergeChecker::new(&pd_config());
    let (_dir, store) = open_store();
    store.create_region(region(1, b"", b"m", MIB / 4, 10)).unwrap();
    store.create_region(region(2, b"m", b"z", MIB / 4, 10)).unwrap();

    let mut regions = store.list_regions();
    regions.sort_by(|a, b| a.start_key.cmp(&b.start_key));
    let op = checker
        .check(&regions[0], &regions[1])
        .expect("small adjacent regions must merge");

    let (left_id, right_id) = match op {
        coord_server::pd::Operator::MergeRegion { left, right } => (left, right),
        other => panic!("expected MergeRegion, got {other:?}"),
    };
    assert_eq!((left_id, right_id), (1, 2));

    // 执行合并：左 Region 吞掉右 Region 的区间，右 Region 下架
    let mut left = store.get_region(left_id).unwrap();
    let right = store.get_region(right_id).unwrap();
    left.end_key = right.end_key.clone();
    store.update_region(left).unwrap();
    store.delete_region(right_id).unwrap();

    let remaining = store.list_regions();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].start_key, b"".to_vec());
    assert_eq!(remaining[0].end_key, b"z".to_vec());
}

#[test]
fn merge_rejected_when_either_side_over_threshold() {
    let checker = MergeChecker::new(&pd_config());
    let left = region(1, b"", b"m", MIB, 10); // >= merge 阈值
    let right = region(2, b"m", b"z", MIB / 4, 10);
    assert!(checker.check(&left, &right).is_none());
}

#[test]
fn merge_rejected_when_combined_exceeds_max() {
    let checker = MergeChecker::new(&pd_config());
    let left = region(1, b"", b"m", (MIB * 6) / 10, 10);
    let right = region(2, b"m", b"z", (MIB * 6) / 10, 10);
    assert!(checker.check(&left, &right).is_none());
}

// ══════════════════════════════════════════════════════════════════
// 边界场景
// ══════════════════════════════════════════════════════════════════

#[test]
fn single_key_region_can_still_split_by_size() {
    // 单 key Region（start="k", end="k\x00"）在对象存储场景下也可能超大
    let checker = SplitChecker::new(&pd_config());
    let r = region(1, b"k", b"k\x00", MIB * 4, 1);
    assert!(checker.check(&r, b"k\x00".to_vec()).is_some());
}

#[test]
fn whole_keyspace_region_is_addressable_by_any_key() {
    let (_dir, store) = open_store();
    store.create_region(region(1, b"", b"", 0, 0)).unwrap();

    for key in [b"".as_slice(), b"a", b"\xff\xff"] {
        assert_eq!(
            store.get_region_by_key(key).map(|r| r.region_id),
            Some(1),
            "full-keyspace region must own every key"
        );
    }
}

#[test]
fn region_scan_stops_at_requested_limit() {
    let (_dir, store) = open_store();
    store.create_region(region(1, b"", b"b", 0, 0)).unwrap();
    store.create_region(region(2, b"b", b"n", 0, 0)).unwrap();
    store.create_region(region(3, b"n", b"", 0, 0)).unwrap();

    let scanned = store.scan_regions(b"", 2);
    assert_eq!(scanned.len(), 2, "limit must be honored");
    assert_eq!(scanned[0].region_id, 1);
    assert_eq!(scanned[1].region_id, 2);
}

#[test]
fn adjacent_pairs_track_split_then_merge_lifecycle() {
    let (_dir, store) = open_store();
    store.create_region(region(1, b"", b"", 0, 0)).unwrap();
    assert!(store.get_adjacent_pairs().is_empty());

    // split
    let mut left = store.get_region(1).unwrap();
    left.end_key = b"m".to_vec();
    store.update_region(left).unwrap();
    store.create_region(region(2, b"m", b"", 0, 0)).unwrap();
    assert_eq!(store.get_adjacent_pairs().len(), 1);

    // merge back
    let mut left = store.get_region(1).unwrap();
    left.end_key = b"".to_vec();
    store.update_region(left).unwrap();
    store.delete_region(2).unwrap();
    assert!(store.get_adjacent_pairs().is_empty());
    assert_eq!(store.region_count(), 1);
}
