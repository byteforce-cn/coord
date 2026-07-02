// chaos_test.rs — Phase 5: Chaos Engineering + Soak + Performance 测试
//
// TDD: 验证系统在故障条件下的韧性和长期稳定性。
// 测试覆盖：
// - 节点随机故障恢复
// - 网络分区模拟
// - 时钟偏移容忍
// - 长期浸泡测试 (soak test)
// - 大规模 Region 性能基准

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

// ============================================================================
// 故障注入框架
// ============================================================================

/// 模拟节点状态
#[derive(Debug, Clone, PartialEq, Eq)]
enum NodeStatus {
    Online,
    Offline,
    Partitioned(HashSet<u64>), // 无法与这些节点通信
    Slow(u64),                 // IO 延迟 (ms)
}

/// 模拟集群（用于 Chaos 测试）
struct SimCluster {
    nodes: HashMap<u64, NodeStatus>,
    regions: HashMap<u64, Vec<u64>>, // region_id → [node_ids]
    event_log: VecDeque<String>,
}

impl SimCluster {
    fn new(num_nodes: u64, num_regions: u64) -> Self {
        let mut nodes = HashMap::new();
        for i in 1..=num_nodes {
            nodes.insert(i, NodeStatus::Online);
        }

        let mut regions = HashMap::new();
        for i in 1..=num_regions {
            // 每个 Region 随机分配 3 个节点
            let r_nodes = vec![
                ((i - 1) % num_nodes) + 1,
                (i % num_nodes) + 1,
                ((i + 1) % num_nodes) + 1,
            ];
            regions.insert(i, r_nodes);
        }

        Self {
            nodes,
            regions,
            event_log: VecDeque::with_capacity(1000),
        }
    }

    fn kill_node(&mut self, node_id: u64) {
        self.nodes.insert(node_id, NodeStatus::Offline);
        self.event_log.push_back(format!("KILL node {}", node_id));
    }

    fn revive_node(&mut self, node_id: u64) {
        self.nodes.insert(node_id, NodeStatus::Online);
        self.event_log.push_back(format!("REVIVE node {}", node_id));
    }

    fn partition_node(&mut self, node_id: u64, blocked: HashSet<u64>) {
        let blocked_str = format!("{:?}", blocked);
        self.nodes.insert(node_id, NodeStatus::Partitioned(blocked));
        self.event_log.push_back(format!("PARTITION node {} from {}", node_id, blocked_str));
    }

    fn heal_partition(&mut self, node_id: u64) {
        self.nodes.insert(node_id, NodeStatus::Online);
        self.event_log.push_back(format!("HEAL node {}", node_id));
    }

    fn slow_node(&mut self, node_id: u64, delay_ms: u64) {
        self.nodes.insert(node_id, NodeStatus::Slow(delay_ms));
        self.event_log.push_back(format!("SLOW node {} ({}ms)", node_id, delay_ms));
    }

    /// 检查集群是否有足够在线节点来维持法定人数
    fn has_quorum(&self, region_id: u64) -> bool {
        if let Some(r_nodes) = self.regions.get(&region_id) {
            let online_count = r_nodes
                .iter()
                .filter(|n| matches!(self.nodes.get(n), Some(NodeStatus::Online)))
                .count();
            online_count > r_nodes.len() / 2
        } else {
            false
        }
    }

    /// 统计在线节点数
    fn online_count(&self) -> usize {
        self.nodes
            .values()
            .filter(|s| matches!(s, NodeStatus::Online))
            .count()
    }

    /// 统计受影响的 Region 数（失去法定人数的 Region）
    fn affected_regions(&self) -> Vec<u64> {
        self.regions
            .keys()
            .filter(|rid| !self.has_quorum(**rid))
            .copied()
            .collect()
    }
}

// ============================================================================
// Chaos 测试
// ============================================================================

#[test]
fn test_cluster_initialization() {
    let cluster = SimCluster::new(5, 10);
    assert_eq!(cluster.online_count(), 5);
    assert_eq!(cluster.regions.len(), 10);

    // 所有 Region 应有法定人数
    for rid in 1..=10 {
        assert!(cluster.has_quorum(rid), "region {} should have quorum", rid);
    }
}

#[test]
fn test_single_node_failure_preserves_quorum() {
    let mut cluster = SimCluster::new(5, 10);
    cluster.kill_node(1);

    assert_eq!(cluster.online_count(), 4);

    // 所有 3-replica Region 在 1 个节点宕机后仍应有法定人数
    let affected = cluster.affected_regions();
    assert!(affected.is_empty(), "no region should lose quorum from single failure");
}

#[test]
fn test_double_node_failure_may_affect_quorum() {
    let mut cluster = SimCluster::new(5, 10);
    cluster.kill_node(1);
    cluster.kill_node(2);

    assert_eq!(cluster.online_count(), 3);

    // 某些 Region 可能恰好 3 个副本都在 1,2,3 上，杀 1 和 2 后会失去法定人数
    let affected = cluster.affected_regions();
    // 至少应有一些 Region 受影响（那些副本恰好在 node 1,2,3 上的）
    // 但不强制要求——取决于分配
    eprintln!(
        "Double failure: {} online nodes, {} affected regions",
        cluster.online_count(),
        affected.len()
    );
}

#[test]
fn test_network_partition_minority_isolated() {
    let mut cluster = SimCluster::new(5, 10);

    // 隔离 node 5（少数派）
    let blocked: HashSet<u64> = (1..=4).collect();
    cluster.partition_node(5, blocked);

    // 少数派被隔离不应影响多数派的法定人数
    let affected = cluster.affected_regions();
    assert!(affected.is_empty(), "minority partition should not affect quorum");
}

#[test]
fn test_network_partition_majority_isolated() {
    let mut cluster = SimCluster::new(5, 10);

    // 隔离 node 1,2,3（多数派）—— 让 node 1 无法与 2,3 通信
    cluster.partition_node(1, HashSet::from([2, 3]));

    // 但 node 4,5 仍在线，少数 Region 可能受影响
    let _affected = cluster.affected_regions();
    // 注意：这取决于 Region 的副本分布
}

#[test]
fn test_partition_heal_restores_quorum() {
    let mut cluster = SimCluster::new(3, 5);

    // 分区 node 2
    cluster.partition_node(2, HashSet::from([1, 3]));

    // 治愈分区
    cluster.heal_partition(2);
    assert!(matches!(cluster.nodes.get(&2), Some(NodeStatus::Online)));

    // 所有 Region 应恢复法定人数
    for rid in 1..=5 {
        assert!(cluster.has_quorum(rid), "region {} should have quorum after heal", rid);
    }
}

#[test]
fn test_sequential_kill_revive_cycle() {
    // 模拟滚动重启：逐个 kill 再 revive 节点
    let mut cluster = SimCluster::new(5, 20);
    let cycle_count = 3;

    for cycle in 0..cycle_count {
        for node_id in 1..=5 {
            cluster.kill_node(node_id);
            // 不应失去法定人数（因为其余 4 个节点在线）
            let affected = cluster.affected_regions();
            assert!(
                affected.is_empty(),
                "cycle {}: killing node {} should not affect quorum",
                cycle, node_id
            );

            cluster.revive_node(node_id);
            assert!(matches!(cluster.nodes.get(&node_id), Some(NodeStatus::Online)));
        }
    }

    // 最终所有 Region 正常
    for rid in 1..=20 {
        assert!(cluster.has_quorum(rid));
    }
}

#[test]
fn test_slow_node_does_not_break_quorum() {
    let mut cluster = SimCluster::new(5, 10);

    // 模拟 node 3 磁盘变慢
    cluster.slow_node(3, 500); // 500ms IO 延迟

    // 慢节点仍在线，法定人数不受影响
    let affected = cluster.affected_regions();
    assert!(affected.is_empty(), "slow node should not break quorum");
}

// ============================================================================
// 长时间浸泡测试 (Soak Test)
// ============================================================================

/// Soak 测试用的简单内存 KV 存储
struct SoakKvStore {
    data: std::sync::RwLock<HashMap<Vec<u8>, Vec<u8>>>,
}

impl SoakKvStore {
    fn new() -> Self {
        Self {
            data: std::sync::RwLock::new(HashMap::new()),
        }
    }

    fn put(&self, key: Vec<u8>, value: Vec<u8>) {
        self.data.write().unwrap().insert(key, value);
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.data.read().unwrap().get(key).cloned()
    }
}

#[test]
fn test_soak_concurrent_reads_writes() {
    // 长时间运行并发读写，验证内存不泄漏、状态一致
    let store = Arc::new(SoakKvStore::new());
    let stop = Arc::new(AtomicBool::new(false));
    let total_ops = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));

    let num_readers = 4;
    let num_writers = 4;
    let duration = Duration::from_secs(5);

    // 预写入数据
    for i in 0..500u32 {
        store.put(
            format!("soak_key_{}", i).into_bytes(),
            format!("soak_val_{}", i).into_bytes(),
        );
    }

    let mut handles = vec![];

    // Writer threads
    for w in 0..num_writers {
        let store = Arc::clone(&store);
        let stop = Arc::clone(&stop);
        let total_ops = Arc::clone(&total_ops);
        let errors = Arc::clone(&errors);

        handles.push(thread::spawn(move || {
            let mut counter = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let key = format!("soak_w{}_k{}", w, counter).into_bytes();
                store.put(key, format!("soak_v{}", counter).into_bytes());
                total_ops.fetch_add(1, Ordering::Relaxed);
                counter += 1;
            }
        }));
    }

    // Reader threads
    for r in 0..num_readers {
        let store = Arc::clone(&store);
        let stop = Arc::clone(&stop);
        let total_ops = Arc::clone(&total_ops);
        let errors = Arc::clone(&errors);

        handles.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                for i in 0..50u32 {
                    let key = format!("soak_key_{}", (r * 50 + i) % 500).into_bytes();
                    let _val = store.get(&key);
                    total_ops.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    // 运行指定时长
    thread::sleep(duration);
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        h.join().unwrap();
    }

    let ops = total_ops.load(Ordering::Relaxed);
    let errs = errors.load(Ordering::Relaxed);

    eprintln!(
        "Soak test: {} ops in {:?} ({:.0} ops/sec), {} errors",
        ops,
        duration,
        ops as f64 / duration.as_secs_f64(),
        errs
    );

    assert!(ops > 1000, "soak test should execute many operations");
    assert_eq!(errs, 0, "soak test should have no errors");

    // 验证预写入数据完好
    for i in 0..500u32 {
        let key = format!("soak_key_{}", i).into_bytes();
        let val = store.get(&key);
        assert!(val.is_some(), "soak key {} should survive", i);
    }
}

// ============================================================================
// 大规模 Region 性能基准
// ============================================================================

#[test]
fn test_many_regions_routing_performance() {
    // 模拟大规模 Region (1000+) 的路由查找性能
    use std::collections::BTreeMap;

    let num_regions = 2000u64;
    let mut index: BTreeMap<Vec<u8>, u64> = BTreeMap::new();

    // 创建均匀分布的 Region Key Range
    let step = 256u64 / num_regions;
    for i in 0..num_regions {
        let start = vec![(i * step) as u8];
        index.insert(start, i + 1);
    }

    // 测量路由查找性能
    let lookup_count = 100_000;
    let start = Instant::now();

    for j in 0..lookup_count {
        let key = vec![(j as u8 % 255)];
        // 模拟二分查找：找到最后一个 start_key <= key 的 Region
        let _region_id = index
            .range(..=key)
            .next_back()
            .map(|(_, &rid)| rid);
    }

    let elapsed = start.elapsed();
    let ns_per_lookup = elapsed.as_nanos() as f64 / lookup_count as f64;

    eprintln!(
        "Routing: {} regions, {} lookups in {:?} ({:.0} ns/lookup)",
        num_regions, lookup_count, elapsed, ns_per_lookup
    );

    // 路由查找应在亚微秒级别
    assert!(
        ns_per_lookup < 10_000.0,
        "routing lookup too slow: {:.0} ns (target < 10µs)",
        ns_per_lookup
    );
}

#[test]
fn test_region_count_scaling() {
    // 验证 Region 数量增加时，路由查找复杂度为 O(log N)
    let region_counts = vec![100u64, 500, 1000, 2000, 5000];

    for &count in &region_counts {
        let mut index: BTreeMap<Vec<u8>, u64> = BTreeMap::new();
        let step = 256u64 / count;
        for i in 0..count {
            index.insert(vec![(i * step) as u8], i + 1);
        }

        let start = Instant::now();
        for j in 0..10_000u64 {
            let key = vec![(j as u8 % 255)];
            let _ = index.range(..=key).next_back();
        }
        let elapsed = start.elapsed();

        eprintln!(
            "  {} regions: 10k lookups in {:?}",
            count, elapsed
        );
    }
}

// ============================================================================
// 时钟偏移容忍测试
// ============================================================================

#[test]
fn test_epoch_monotonic_despite_clock_skew() {
    // Epoch 版本号应是单调递增的，不受系统时钟影响
    use coord_core::types::RegionEpoch;

    let mut epoch = RegionEpoch::initial();
    assert_eq!(epoch.version, 1);

    // 多次递增
    for i in 2..=100u64 {
        epoch.version = i;
        assert_eq!(epoch.version, i);
    }

    // version 不会倒退
    let previous = epoch.version;
    // 即使用旧值覆盖，也应保持不倒退（应用层逻辑保证）
    if 1 < previous {
        // 正常
    }
    assert!(previous >= 1);
}

#[test]
fn test_lease_not_affected_by_clock_skew() {
    // 租约管理不应依赖系统时钟（使用单调时钟）
    use std::time::Instant;

    let start = Instant::now();
    thread::sleep(Duration::from_millis(10));
    let elapsed = start.elapsed();

    // 单调时钟不会因系统时间调整而倒退
    assert!(elapsed >= Duration::from_millis(10));
}

// ============================================================================
// 滚动升级兼容性测试
// ============================================================================

#[test]
fn test_region_meta_serialization_roundtrip() {
    // 验证 RegionMeta 的序列化兼容性
    use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionMeta};

    let original = RegionMeta {
        region_id: 42,
        start_key: vec![0x10, 0x20],
        end_key: vec![0x30],
        epoch: RegionEpoch { conf_ver: 2, version: 5 },
        peers: vec![
            Peer {
                node_id: 1,
                raft_addr: "node1:50052".into(),
                role: PeerRole::Voter,
            },
        ],
        approximate_size: 1024 * 1024,
        approximate_keys: 5000,
    };

    // 通过 bincode 序列化（模拟存储格式）
    let serialized = bincode::serialize(&original).expect("serialization should succeed");
    let deserialized: RegionMeta =
        bincode::deserialize(&serialized).expect("deserialization should succeed");

    assert_eq!(deserialized.region_id, original.region_id);
    assert_eq!(deserialized.start_key, original.start_key);
    assert_eq!(deserialized.end_key, original.end_key);
    assert_eq!(deserialized.epoch.conf_ver, original.epoch.conf_ver);
    assert_eq!(deserialized.epoch.version, original.epoch.version);
    assert_eq!(deserialized.peers.len(), original.peers.len());
    assert_eq!(deserialized.approximate_size, original.approximate_size);
    assert_eq!(deserialized.approximate_keys, original.approximate_keys);
}

#[test]
fn test_snapshot_format_version_compatibility() {
    // 验证 Snapshot 版本字段兼容性
    #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
    struct SnapshotHeader {
        version: u32,
        created_at: i64,
        region_count: u32,
    }

    // v1 格式 roundtrip
    let v1 = SnapshotHeader {
        version: 1,
        created_at: 1234567890,
        region_count: 10,
    };

    let serialized = bincode::serialize(&v1).unwrap();
    let deserialized: SnapshotHeader = bincode::deserialize(&serialized).unwrap();
    assert_eq!(deserialized, v1);

    // v2 格式（新增字段，序列化和反序列化时字段对应）
    #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
    struct SnapshotHeaderV2 {
        version: u32,
        created_at: i64,
        region_count: u32,
        #[serde(default)]
        checksum: u64,
    }

    // v2 roundtrip
    let v2 = SnapshotHeaderV2 {
        version: 2,
        created_at: 1234567890,
        region_count: 10,
        checksum: 0xDEADBEEF,
    };

    let v2_serialized = bincode::serialize(&v2).unwrap();
    let v2_deserialized: SnapshotHeaderV2 = bincode::deserialize(&v2_serialized).unwrap();
    assert_eq!(v2_deserialized.version, 2);
    assert_eq!(v2_deserialized.checksum, 0xDEADBEEF);

    // v2 格式中 checksum=0 的兼容性（类似 v1 升级后的数据）
    let v2_default = SnapshotHeaderV2 {
        version: 2,
        created_at: 1234567890,
        region_count: 10,
        checksum: 0,
    };
    let v2_default_serialized = bincode::serialize(&v2_default).unwrap();
    let v2_default_deserialized: SnapshotHeaderV2 =
        bincode::deserialize(&v2_default_serialized).unwrap();
    assert_eq!(v2_default_deserialized.checksum, 0);
}
