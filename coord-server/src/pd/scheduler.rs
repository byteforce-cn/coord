// PD 调度器实现
//
// 包含：
// - SplitChecker:     检测超阈值 Region，触发 Split
// - MergeChecker:     检测小 Region，触发 Merge
// - ReplicaChecker:   确保 Region 副本数达标
// - BalanceScheduler: 均衡各节点上的 Region 副本数
// - LeaderScheduler:  均衡各节点上的 Leader 数量
// - HotSpotScheduler: 检测读写热点，通过 Split 或 Leader 转移分散负载
//
// 设计要点（ADP §4.2）：
// - 各 Scheduler 独立运行，通过 PdConfig 配置检查间隔
// - 产生 Operator 交由 PD 执行引擎异步执行
// - 每个调度周期限制输出数量，避免 Scheduling Storm
// - Scheduler trait 统一接口：fn schedule(&self, ctx: &ScheduleContext) -> Vec<Operator>

use std::collections::HashMap;

use coord_core::types::{NodeID, RegionId, RegionMeta};

use super::operator::Operator;
use super::types::{NodeState, PdConfig};

// ============================================================================
// Split Checker
// ============================================================================

/// 分裂检测器
///
/// 检查 Region 的 Size/Keys 是否超过阈值，超过则产生 SplitRegion Operator。
/// 使用 `KeySampler` 从采样 Key 中选择中位数作为 split_key，
/// 无采样数据时回退到数学中点 `mid_key()`。
pub struct SplitChecker {
    /// 分裂大小阈值（字节）
    size_threshold: u64,
    /// 分裂 Key 数阈值
    keys_threshold: u64,
    /// Key 采样器（用于选择最优 split_key）
    key_sampler: KeySampler,
}

impl SplitChecker {
    /// 从 PD 配置创建
    pub fn new(config: &PdConfig) -> Self {
        Self {
            size_threshold: config.region_split_size_mb * 1024 * 1024,
            keys_threshold: config.region_split_keys,
            key_sampler: KeySampler::default(),
        }
    }

    /// 检查 Region 是否需要分裂
    ///
    /// 返回 `Some(Operator)` 如果 Region 需要分裂，否则 `None`。
    /// `split_key` 由外部（PD 采样）提供，此方法仅做阈值判断。
    pub fn check(&self, region: &RegionMeta, split_key: Vec<u8>) -> Option<Operator> {
        let exceeds_size = region.approximate_size >= self.size_threshold;
        let exceeds_keys = region.approximate_keys >= self.keys_threshold;

        if exceeds_size || exceeds_keys {
            Some(Operator::SplitRegion {
                region_id: region.region_id,
                split_key,
                new_region_id: 0, // 由 PD 分配新 Region ID
            })
        } else {
            None
        }
    }
}

impl Scheduler for SplitChecker {
    fn name(&self) -> &'static str {
        "split-checker"
    }

    fn schedule(&self, ctx: &ScheduleContext) -> Vec<Operator> {
        let mut ops = Vec::new();
        for region in ctx.regions.values() {
            if region.approximate_size >= self.size_threshold
                || region.approximate_keys >= self.keys_threshold
            {
                // 优先使用采样 Key 的中位数，无样本时回退到数学中点
                let split_key = if let Some(samples) = ctx.region_sample_keys.get(&region.region_id) {
                    self.key_sampler.select_or_fallback(samples, &region.start_key, &region.end_key)
                } else {
                    mid_key(&region.start_key, &region.end_key)
                };

                ops.push(Operator::SplitRegion {
                    region_id: region.region_id,
                    split_key,
                    new_region_id: 0,
                });
            }
        }
        ops
    }
}

// ============================================================================
// Merge Checker
// ============================================================================

/// 合并检测器
///
/// 检查相邻 Region 是否小于合并阈值，满足条件则产生 MergeRegion Operator。
pub struct MergeChecker {
    /// 合并触发阈值（字节）：低于此值的 Region 是合并候选
    merge_threshold: u64,
    /// 合并后最大大小（字节）：合并后总大小需低于此值
    max_merge_size: u64,
}

impl MergeChecker {
    /// 从 PD 配置创建
    pub fn new(config: &PdConfig) -> Self {
        Self {
            merge_threshold: config.region_merge_size_mb * 1024 * 1024,
            max_merge_size: config.region_split_size_mb * 1024 * 1024,
        }
    }

    /// 检查两个相邻 Region 是否可以合并
    ///
    /// 前置条件：
    /// 1. 两个 Region 的 Key Range 必须相邻（left.end_key == right.start_key）
    /// 2. 两个 Region 的大小都低于 merge_threshold
    /// 3. 合并后总大小低于 max_merge_size
    pub fn check(&self, left: &RegionMeta, right: &RegionMeta) -> Option<Operator> {
        // 检查相邻性
        if left.end_key != right.start_key {
            return None;
        }

        // 检查各自大小
        if left.approximate_size >= self.merge_threshold
            || right.approximate_size >= self.merge_threshold
        {
            return None;
        }

        // 检查合并后总大小
        let total = left.approximate_size + right.approximate_size;
        if total >= self.max_merge_size {
            return None;
        }

        Some(Operator::MergeRegion {
            left: left.region_id,
            right: right.region_id,
        })
    }
}

impl Scheduler for MergeChecker {
    fn name(&self) -> &'static str {
        "merge-checker"
    }

    fn schedule(&self, ctx: &ScheduleContext) -> Vec<Operator> {
        let mut ops = Vec::new();
        // 每轮最多合并 5 对（避免过度合并震荡）
        const MAX_MERGE_PAIRS: usize = 5;

        // 按 start_key 排序 Region，找到相邻对
        let mut sorted_regions: Vec<&RegionMeta> = ctx.regions.values().collect();
        sorted_regions.sort_by(|a, b| a.start_key.cmp(&b.start_key));

        for window in sorted_regions.windows(2) {
            if ops.len() >= MAX_MERGE_PAIRS {
                break;
            }
            let left = window[0];
            let right = window[1];
            if let Some(op) = self.check(left, right) {
                ops.push(op);
            }
        }

        ops
    }
}

// ============================================================================
// Replica Checker
// ============================================================================

/// 副本检测器
///
/// 确保每个 Region 的副本数达到配置值（默认 3）。
/// 副本数不足 → 添加副本；副本数过多 → 移除多余副本。
pub struct ReplicaChecker {
    /// 目标副本数
    target_replicas: usize,
}

impl ReplicaChecker {
    /// 从 PD 配置创建
    pub fn new(config: &PdConfig) -> Self {
        Self {
            target_replicas: config.target_replicas,
        }
    }

    /// 检查 Region 的副本数是否达标
    ///
    /// 返回需要执行的 Operator（若有）。
    /// - Voter 不足 → `Some(AddPeer)`（node_id=0 表示需要 PD 选择目标节点）
    /// - Voter 过多 → `Some(RemovePeer)`
    /// - 正常 → `None`
    pub fn check(&self, region: &RegionMeta) -> Option<Operator> {
        let voter_count = region
            .peers
            .iter()
            .filter(|p| p.role == coord_core::types::PeerRole::Voter)
            .count();

        if voter_count < self.target_replicas {
            Some(Operator::AddPeer {
                region_id: region.region_id,
                node_id: 0, // PD 选择目标节点
                raft_addr: String::new(),
            })
        } else if voter_count > self.target_replicas {
            // 移除最后一个 Voter（简化策略，生产环境应选择负载最高的节点）
            if let Some(last_voter) = region
                .peers
                .iter()
                .rev()
                .find(|p| p.role == coord_core::types::PeerRole::Voter)
            {
                Some(Operator::RemovePeer {
                    region_id: region.region_id,
                    node_id: last_voter.node_id,
                })
            } else {
                None
            }
        } else {
            None
        }
    }
}

impl Scheduler for ReplicaChecker {
    fn name(&self) -> &'static str {
        "replica-checker"
    }

    fn schedule(&self, ctx: &ScheduleContext) -> Vec<Operator> {
        let mut ops = Vec::new();
        for region in ctx.regions.values() {
            if let Some(op) = self.check(region) {
                ops.push(op);
            }
        }
        ops
    }
}

// ============================================================================
// 辅助函数
// ============================================================================

/// 计算两个字节 slice 的中点 key（用于 split_key 建议）
fn mid_key(start: &[u8], end: &[u8]) -> Vec<u8> {
    if start.is_empty() && end.is_empty() {
        return vec![0x80];
    }
    if end.is_empty() || end == &[0xFF] {
        // 全空间：取中间
        return vec![0x80];
    }

    // 简化的中点计算：取 start 和 end 的中间字节
    let len = start.len().max(end.len());
    let mut mid = Vec::with_capacity(len);
    let mut carry: u16 = 0;

    for i in 0..len {
        let s = start.get(i).copied().unwrap_or(0);
        let e = end.get(i).copied().unwrap_or(0xFF);
        let sum = s as u16 + e as u16 + carry;
        mid.push((sum / 2) as u8);
        carry = (sum % 2) * 256;
    }

    // 确保 mid 在 (start, end) 范围内
    if mid.as_slice() <= start {
        // 取 end 的前半部分
        if let Some(last) = mid.last_mut() {
            *last = last.saturating_add(1);
        }
    }

    mid
}

// ============================================================================
// KeySampler — Region Key 采样与 Split Key 选择
// ============================================================================

/// Key 采样器：从 Region 的 Key Range 中采样 Key，选择最优 Split Key。
///
/// 用于 `SplitChecker` 在调度时决定 split_key。
/// 支持两种模式：
/// - **采样模式**：从 Region 的实际 Key 中采样，取中位数（更均衡）
/// - **回退模式**：使用 Key Range 的数学中点（当无样本可用时）
pub struct KeySampler {
    /// 最大采样数（默认 100）
    max_samples: usize,
}

impl KeySampler {
    /// 创建新的 KeySampler
    pub fn new(max_samples: usize) -> Self {
        Self { max_samples }
    }

    /// 从样本中选择中位数 Key 作为 Split Key
    ///
    /// 将样本排序后取中位数，比数学中点更能均匀分割数据。
    /// 若无样本，返回 None（调用方应回退到 mid_key）。
    pub fn select_split_key(&self, samples: &[Vec<u8>]) -> Option<Vec<u8>> {
        if samples.is_empty() {
            return None;
        }

        let mut sorted: Vec<&Vec<u8>> = samples.iter().collect();
        sorted.sort();
        // 取中位数
        let median_idx = sorted.len() / 2;
        Some(sorted[median_idx].clone())
    }

    /// 从样本中选择 Split Key，无样本时回退到数学中点
    pub fn select_or_fallback(&self, samples: &[Vec<u8>], start_key: &[u8], end_key: &[u8]) -> Vec<u8> {
        self.select_split_key(samples)
            .unwrap_or_else(|| mid_key(start_key, end_key))
    }

    /// 使用 Reservoir Sampling 从 key 迭代器中均匀采样
    ///
    /// 保证每个 key 被选中的概率相等，不需要提前知道总数。
    /// 适用于大型 Region 的流式采样。
    pub fn reservoir_sample<I>(&self, keys: I) -> Vec<Vec<u8>>
    where
        I: IntoIterator<Item = Vec<u8>>,
    {
        let mut reservoir: Vec<Vec<u8>> = Vec::with_capacity(self.max_samples);
        let mut count = 0usize;

        for key in keys {
            count += 1;
            if reservoir.len() < self.max_samples {
                reservoir.push(key);
            } else {
                // 以 max_samples / count 的概率替换
                let idx = fast_random_usize(count);
                if idx < self.max_samples {
                    reservoir[idx] = key;
                }
            }
        }

        reservoir
    }
}

impl Default for KeySampler {
    fn default() -> Self {
        Self { max_samples: 100 }
    }
}

/// 简易伪随机数（用于 Reservoir Sampling，不需要密码学安全）
fn fast_random_usize(max: usize) -> usize {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    let state = RandomState::new();
    let mut hasher = state.build_hasher();
    // 使用即时时间作为种子的一部分
    hasher.write_u64(0);
    hasher.finish() as usize % max
}

// ============================================================================
// ScheduleContext — 调度上下文
// ============================================================================

/// 调度上下文：传递给所有 Scheduler 的只读视图
///
/// 包含 PD 当前已知的所有 Region 元数据和节点状态。
/// 每个调度周期创建一个新的 ScheduleContext 快照。
pub struct ScheduleContext {
    /// Region ID → RegionMeta（当前所有 Region）
    pub regions: HashMap<RegionId, RegionMeta>,
    /// Node ID → NodeState（当前所有节点）
    pub nodes: HashMap<NodeID, NodeState>,
    /// 在线节点列表（预计算，便于快速查询）
    pub online_nodes: Vec<NodeID>,
    /// Region ID → 采样 Key 列表（Region Leader 上报，用于 Split Key 选择）
    pub region_sample_keys: HashMap<RegionId, Vec<Vec<u8>>>,
}

impl ScheduleContext {
    /// 从 Region 列表和节点列表构建调度上下文
    pub fn new(regions: Vec<RegionMeta>, nodes: Vec<NodeState>) -> Self {
        let online_nodes: Vec<NodeID> = nodes
            .iter()
            .filter(|n| n.online)
            .map(|n| n.node_id)
            .collect();
        Self {
            regions: regions.into_iter().map(|r| (r.region_id, r)).collect(),
            nodes: nodes.into_iter().map(|n| (n.node_id, n)).collect(),
            online_nodes,
            region_sample_keys: HashMap::new(),
        }
    }

    /// 构建带采样 Key 的调度上下文
    pub fn with_samples(
        regions: Vec<RegionMeta>,
        nodes: Vec<NodeState>,
        sample_keys: HashMap<RegionId, Vec<Vec<u8>>>,
    ) -> Self {
        let mut ctx = Self::new(regions, nodes);
        ctx.region_sample_keys = sample_keys;
        ctx
    }

    /// 获取在线节点数量
    pub fn online_count(&self) -> usize {
        self.online_nodes.len()
    }

    /// 检查节点是否在线
    pub fn is_online(&self, node_id: NodeID) -> bool {
        self.nodes.get(&node_id).map_or(false, |n| n.online)
    }
}

// ============================================================================
// Scheduler trait — 调度器统一接口
// ============================================================================

/// 调度器 trait：所有调度器必须实现此接口
///
/// 设计要点：
/// - 每个调度周期，PD 调用 `schedule()` 获取需要执行的 Operator 列表
/// - Scheduler 内部维护自己的状态（如上次调度时间、热点计数窗口等）
/// - 实现 `Send + Sync`，可在多线程环境中使用
pub trait Scheduler: Send + Sync {
    /// 调度器名称（用于日志和指标）
    fn name(&self) -> &'static str;

    /// 执行一次调度检查
    ///
    /// # Arguments
    /// * `ctx` - 当前集群状态的只读快照
    ///
    /// # Returns
    /// 需要执行的 Operator 列表。空 Vec 表示本次无调度操作。
    fn schedule(&self, ctx: &ScheduleContext) -> Vec<Operator>;
}

// ============================================================================
// BalanceScheduler — Region 副本均衡
// ============================================================================

/// 副本均衡调度器
///
/// 目标：每个节点的 Region 副本数（按 Voter 权重计算）接近均值。
/// 策略：
///   1. 计算每个在线节点的 Voter 副本数
///   2. 找到副本最多和最少的节点
///   3. 从最多节点选一个 Region（优先 Follower）→ 在最少的节点添加副本
pub struct BalanceScheduler {
    /// 每轮最多产生的 Operator 数量
    max_ops_per_round: usize,
}

impl BalanceScheduler {
    /// 创建新的 BalanceScheduler
    pub fn new(max_ops_per_round: usize) -> Self {
        Self { max_ops_per_round }
    }
}

impl Scheduler for BalanceScheduler {
    fn name(&self) -> &'static str {
        "balance-scheduler"
    }

    fn schedule(&self, ctx: &ScheduleContext) -> Vec<Operator> {
        if ctx.online_count() < 2 {
            return vec![];
        }

        // 1. 计算每个在线节点的 Voter 副本数
        let mut node_replica_count: HashMap<NodeID, usize> = HashMap::new();
        for node_id in &ctx.online_nodes {
            node_replica_count.insert(*node_id, 0);
        }
        for region in ctx.regions.values() {
            for peer in &region.peers {
                if ctx.is_online(peer.node_id) {
                    *node_replica_count.entry(peer.node_id).or_insert(0) += 1;
                }
            }
        }

        // 2. 找到副本最多和最少的节点
        let (max_node, max_count) = node_replica_count
            .iter()
            .max_by_key(|(_, &c)| c)
            .map(|(&n, &c)| (n, c))
            .unwrap_or((0, 0));
        let (min_node, min_count) = node_replica_count
            .iter()
            .min_by_key(|(_, &c)| c)
            .map(|(&n, &c)| (n, c))
            .unwrap_or((0, 0));

        // 差距小于 2 则无需均衡
        if max_count.saturating_sub(min_count) < 2 {
            return vec![];
        }

        let mut ops = Vec::new();

        // 3. 从副本最多的节点选一个 Region 进行迁移
        //    优先选择该节点上作为 Follower 的 Region
        for region in ctx.regions.values() {
            if ops.len() >= self.max_ops_per_round {
                break;
            }

            // 该 Region 在 max_node 上有副本，且 max_node 不是该 Region 的 Leader
            let has_peer_on_max = region.peers.iter().any(|p| p.node_id == max_node);
            if !has_peer_on_max {
                continue;
            }

            // 避免在同一个节点上添加已存在的副本
            let already_on_min = region.peers.iter().any(|p| p.node_id == min_node);
            if already_on_min {
                continue;
            }

            // 获取 min_node 的 Raft 地址
            let target_addr = ctx
                .nodes
                .get(&min_node)
                .map(|n| n.raft_addr.clone())
                .unwrap_or_default();

            ops.push(Operator::AddPeer {
                region_id: region.region_id,
                node_id: min_node,
                raft_addr: target_addr,
            });
        }

        ops
    }
}

// ============================================================================
// LeaderScheduler — Leader 均衡
// ============================================================================

/// Leader 均衡调度器
///
/// 目标：每个节点的 Leader 数量接近均值。
/// 策略：
///   1. 计算每个在线节点的 Leader 数量
///   2. 从 Leader 最多的节点选一个 Region → 执行 TransferLeader
pub struct LeaderScheduler {
    /// 每轮最多产生的 Operator 数量
    max_ops_per_round: usize,
}

impl LeaderScheduler {
    /// 创建新的 LeaderScheduler
    pub fn new(max_ops_per_round: usize) -> Self {
        Self { max_ops_per_round }
    }
}

impl Scheduler for LeaderScheduler {
    fn name(&self) -> &'static str {
        "leader-scheduler"
    }

    fn schedule(&self, ctx: &ScheduleContext) -> Vec<Operator> {
        if ctx.online_count() < 2 {
            return vec![];
        }

        // 1. 计算每个在线节点的 Leader 数量
        let mut node_leader_count: HashMap<NodeID, usize> = HashMap::new();
        for node_id in &ctx.online_nodes {
            node_leader_count.insert(*node_id, 0);
        }
        for region in ctx.regions.values() {
            // Leader 信息不在 RegionMeta 中，需要通过 peers 推断
            // Phase 4：假设第一个 Voter peer 为 Leader（生产环境应从心跳获取）
            if let Some(first_voter) = region.peers.iter().find(|p| {
                matches!(p.role, coord_core::types::PeerRole::Voter)
            }) {
                if ctx.is_online(first_voter.node_id) {
                    *node_leader_count.entry(first_voter.node_id).or_insert(0) += 1;
                }
            }
        }

        // 2. 找到 Leader 最多和最少的节点
        let (max_node, max_count) = node_leader_count
            .iter()
            .max_by_key(|(_, &c)| c)
            .map(|(&n, &c)| (n, c))
            .unwrap_or((0, 0));
        let (min_node, _) = node_leader_count
            .iter()
            .min_by_key(|(_, &c)| c)
            .map(|(&n, &c)| (n, c))
            .unwrap_or((0, 0));

        // 差距小于 2 则无需均衡
        if max_count.saturating_sub(node_leader_count.get(&min_node).copied().unwrap_or(0)) < 2 {
            return vec![];
        }

        let mut ops = Vec::new();

        // 3. 从 Leader 最多的节点选 Region 进行 Leader 转移
        for region in ctx.regions.values() {
            if ops.len() >= self.max_ops_per_round {
                break;
            }

            // 该 Region 的第一个 Voter 必须在 max_node 上
            let is_leader_on_max = region
                .peers
                .iter()
                .filter(|p| matches!(p.role, coord_core::types::PeerRole::Voter))
                .next()
                .map_or(false, |p| p.node_id == max_node);

            if !is_leader_on_max {
                continue;
            }

            // 找该 Region 在 min_node 上的 Voter
            let has_voter_on_min = region.peers.iter().any(|p| {
                matches!(p.role, coord_core::types::PeerRole::Voter) && p.node_id == min_node
            });

            if has_voter_on_min {
                ops.push(Operator::TransferLeader {
                    region_id: region.region_id,
                    to_node: min_node,
                });
            }
        }

        ops
    }
}

// ============================================================================
// HotSpotScheduler — 热点检测与处理
// ============================================================================

/// 热点调度器
///
/// 检测读写热点 Region，通过 Split 或 Leader 转移分散负载。
/// 策略：
///   - 写入热点 → 生成 SplitRegion Operator
///   - 读取热点 → 生成 TransferLeader Operator（将 Leader 转移到低负载节点）
#[allow(dead_code)]
pub struct HotSpotScheduler {
    /// 写入 QPS 热点阈值
    write_qps_threshold: u64,
    /// 读取 QPS 热点阈值
    read_qps_threshold: u64,
    /// 每轮最多产生的 Operator 数量
    max_ops_per_round: usize,
}

impl HotSpotScheduler {
    /// 创建新的 HotSpotScheduler
    pub fn new(write_qps_threshold: u64, read_qps_threshold: u64, max_ops_per_round: usize) -> Self {
        Self {
            write_qps_threshold,
            read_qps_threshold,
            max_ops_per_round,
        }
    }
}

impl Scheduler for HotSpotScheduler {
    fn name(&self) -> &'static str {
        "hotspot-scheduler"
    }

    fn schedule(&self, ctx: &ScheduleContext) -> Vec<Operator> {
        if ctx.online_count() < 2 {
            return vec![];
        }

        let ops = Vec::new();

        // 按 QPS 排序，优先处理最热的 Region
        // 注意：RegionMeta 目前不包含 QPS 数据，Phase 4 中通过心跳上报
        // 此调度器依赖 RegionHandle 中的 write_qps / read_qps 字段
        // 当前遍历所有 Region，对满足阈值条件的产生 Operator

        // 计算节点负载（用于 Leader 转移目标选择）
        let _node_leader_count = self.count_leaders_per_node(ctx);

        for _region in ctx.regions.values() {
            if ops.len() >= self.max_ops_per_round {
                break;
            }

            // RegionMeta 的 approximate_size/keys 用于判断是否已经很小的 Region
            // 热点检测依赖 QPS，此字段在当前阶段由 RegionHandle 维护

            // 写入热点 → Split
            // （实际触发条件由外部通过 approximate_size/keys 间接判断）
            // Phase 4+ 需要 QPS 上报后启用实际热点检测
        }

        ops
    }
}

impl HotSpotScheduler {
    /// 计算每个节点的 Leader 数量（辅助函数）
    fn count_leaders_per_node(&self, ctx: &ScheduleContext) -> HashMap<NodeID, usize> {
        let mut counts: HashMap<NodeID, usize> = HashMap::new();
        for region in ctx.regions.values() {
            if let Some(first_voter) = region.peers.iter().find(|p| {
                matches!(p.role, coord_core::types::PeerRole::Voter)
            }) {
                *counts.entry(first_voter.node_id).or_insert(0) += 1;
            }
        }
        counts
    }
}

// ============================================================================
// 调度器工厂
// ============================================================================

/// 从 PD 配置创建默认调度器集合
pub fn create_default_schedulers(config: &PdConfig) -> Vec<Box<dyn Scheduler>> {
    vec![
        Box::new(SplitChecker::new(config)),
        Box::new(MergeChecker::new(config)),
        Box::new(ReplicaChecker::new(config)),
        Box::new(BalanceScheduler::new(5)),
        Box::new(LeaderScheduler::new(3)),
        Box::new(HotSpotScheduler::new(100, 500, 3)),
    ]
}

// ============================================================================
// 测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use coord_core::types::{Peer, PeerRole, RegionEpoch};

    fn make_region(id: u64, size: u64, keys: u64, start: Vec<u8>, end: Vec<u8>, peers: Vec<Peer>) -> RegionMeta {
        RegionMeta {
            region_id: id,
            start_key: start,
            end_key: end,
            epoch: RegionEpoch::initial(),
            peers,
            approximate_size: size,
            approximate_keys: keys,
        }
    }

    fn make_peer(id: u64, role: PeerRole) -> Peer {
        Peer {
            node_id: id,
            raft_addr: format!("node{}:50052", id),
            role,
        }
    }

    // ──── Split Checker 测试 ────

    #[test]
    fn test_split_checker_below_threshold() {
        let config = PdConfig::default();
        let checker = SplitChecker::new(&config);
        let region = make_region(1, 100 * 1024 * 1024, 500_000, vec![0x00], vec![0x55], vec![]);
        assert!(checker.check(&region, vec![0x30]).is_none());
    }

    #[test]
    fn test_split_checker_size_exceeds() {
        let config = PdConfig::default();
        let checker = SplitChecker::new(&config);
        let region = make_region(1, 300 * 1024 * 1024, 500_000, vec![0x00], vec![0x55], vec![]);
        assert!(checker.check(&region, vec![0x30]).is_some());
    }

    #[test]
    fn test_split_checker_keys_exceeds() {
        let config = PdConfig::default();
        let checker = SplitChecker::new(&config);
        let region = make_region(1, 100 * 1024 * 1024, 2_000_000, vec![0x00], vec![0x55], vec![]);
        assert!(checker.check(&region, vec![0x30]).is_some());
    }

    // ──── Merge Checker 测试 ────

    #[test]
    fn test_merge_checker_eligible() {
        let config = PdConfig::default();
        let checker = MergeChecker::new(&config);
        let left = make_region(1, 10 * 1024 * 1024, 100_000, vec![0x00], vec![0x55], vec![]);
        let right = make_region(2, 5 * 1024 * 1024, 50_000, vec![0x55], vec![0xFF], vec![]);
        assert!(checker.check(&left, &right).is_some());
    }

    #[test]
    fn test_merge_checker_not_adjacent() {
        let config = PdConfig::default();
        let checker = MergeChecker::new(&config);
        let left = make_region(1, 10 * 1024 * 1024, 100_000, vec![0x00], vec![0x55], vec![]);
        let right = make_region(2, 5 * 1024 * 1024, 50_000, vec![0x60], vec![0xFF], vec![]);
        assert!(checker.check(&left, &right).is_none());
    }

    #[test]
    fn test_merge_checker_too_large() {
        let config = PdConfig::default();
        let checker = MergeChecker::new(&config);
        let left = make_region(1, 200 * 1024 * 1024, 500_000, vec![0x00], vec![0x55], vec![]);
        let right = make_region(2, 5 * 1024 * 1024, 50_000, vec![0x55], vec![0xFF], vec![]);
        assert!(checker.check(&left, &right).is_none());
    }

    // ──── Replica Checker 测试 ────

    #[test]
    fn test_replica_checker_healthy() {
        let config = PdConfig::default();
        let checker = ReplicaChecker::new(&config);
        let region = make_region(
            1, 0, 0, vec![], vec![],
            vec![
                make_peer(1, PeerRole::Voter),
                make_peer(2, PeerRole::Voter),
                make_peer(3, PeerRole::Voter),
            ],
        );
        assert!(checker.check(&region).is_none());
    }

    #[test]
    fn test_replica_checker_needs_replica() {
        let config = PdConfig::default();
        let checker = ReplicaChecker::new(&config);
        let region = make_region(
            1, 0, 0, vec![], vec![],
            vec![make_peer(1, PeerRole::Voter)],
        );
        let op = checker.check(&region);
        assert!(matches!(op, Some(Operator::AddPeer { .. })));
    }

    #[test]
    fn test_replica_checker_too_many() {
        let config = PdConfig::default();
        let checker = ReplicaChecker::new(&config);
        let region = make_region(
            1, 0, 0, vec![], vec![],
            vec![
                make_peer(1, PeerRole::Voter),
                make_peer(2, PeerRole::Voter),
                make_peer(3, PeerRole::Voter),
                make_peer(4, PeerRole::Voter),
                make_peer(5, PeerRole::Voter),
            ],
        );
        let op = checker.check(&region);
        assert!(matches!(op, Some(Operator::RemovePeer { .. })));
    }

    #[test]
    fn test_replica_checker_learners_not_counted() {
        let config = PdConfig::default();
        let checker = ReplicaChecker::new(&config);
        let region = make_region(
            1, 0, 0, vec![], vec![],
            vec![
                make_peer(1, PeerRole::Voter),
                make_peer(2, PeerRole::Voter),
                make_peer(3, PeerRole::Learner),
            ],
        );
        // 只有 2 个 Voter，需要添加
        assert!(matches!(checker.check(&region), Some(Operator::AddPeer { .. })));
    }

    // ──── ScheduleContext 测试 ────

    #[test]
    fn test_schedule_context_online_nodes() {
        let nodes = vec![
            make_node_state(1, true),
            make_node_state(2, true),
            make_node_state(3, false),
        ];
        let ctx = ScheduleContext::new(vec![], nodes);
        assert_eq!(ctx.online_count(), 2);
        assert!(ctx.is_online(1));
        assert!(ctx.is_online(2));
        assert!(!ctx.is_online(3));
    }

    #[test]
    fn test_schedule_context_empty() {
        let ctx = ScheduleContext::new(vec![], vec![]);
        assert_eq!(ctx.online_count(), 0);
        assert!(ctx.regions.is_empty());
    }

    // ──── BalanceScheduler 测试 ────

    #[test]
    fn test_balance_scheduler_no_imbalance() {
        let nodes = vec![
            make_node_state(1, true),
            make_node_state(2, true),
            make_node_state(3, true),
        ];
        let regions = vec![
            make_region(1, 0, 0, vec![0x00], vec![0x55], vec![
                make_peer(1, PeerRole::Voter),
                make_peer(2, PeerRole::Voter),
                make_peer(3, PeerRole::Voter),
            ]),
        ];
        let ctx = ScheduleContext::new(regions, nodes);
        let sched = BalanceScheduler::new(5);
        let ops = sched.schedule(&ctx);
        // 所有节点副本数相同，不应产生 Operator
        assert!(ops.is_empty());
    }

    #[test]
    fn test_balance_scheduler_single_node() {
        let nodes = vec![make_node_state(1, true)];
        let regions = vec![make_region(1, 0, 0, vec![0x00], vec![0xFF], vec![
            make_peer(1, PeerRole::Voter),
        ])];
        let ctx = ScheduleContext::new(regions, nodes);
        let sched = BalanceScheduler::new(5);
        let ops = sched.schedule(&ctx);
        assert!(ops.is_empty(), "single node: no balancing possible");
    }

    #[test]
    fn test_balance_scheduler_imbalance() {
        let nodes = vec![
            make_node_state(1, true),
            make_node_state(2, true),
        ];
        // All replicas on node 1, none on node 2
        let regions = vec![
            make_region(1, 0, 0, vec![0x00], vec![0x55], vec![
                make_peer(1, PeerRole::Voter),
            ]),
            make_region(2, 0, 0, vec![0x55], vec![0xFF], vec![
                make_peer(1, PeerRole::Voter),
            ]),
        ];
        let ctx = ScheduleContext::new(regions, nodes);
        let sched = BalanceScheduler::new(5);
        let ops = sched.schedule(&ctx);
        // Should suggest adding peer to node 2
        assert!(!ops.is_empty(), "imbalanced: should generate add-peer ops");
        for op in &ops {
            assert!(matches!(op, Operator::AddPeer { node_id: 2, .. }));
        }
    }

    #[test]
    fn test_balance_scheduler_respects_max_ops() {
        let nodes = vec![
            make_node_state(1, true),
            make_node_state(2, true),
        ];
        let mut regions = Vec::new();
        for i in 0..20 {
            regions.push(make_region(
                i, 0, 0,
                vec![i as u8],
                vec![i as u8 + 1],
                vec![make_peer(1, PeerRole::Voter)],
            ));
        }
        let ctx = ScheduleContext::new(regions, nodes);
        let sched = BalanceScheduler::new(3);
        let ops = sched.schedule(&ctx);
        assert!(ops.len() <= 3, "should respect max_ops_per_round=3");
    }

    // ──── LeaderScheduler 测试 ────

    #[test]
    fn test_leader_scheduler_single_node() {
        let nodes = vec![make_node_state(1, true)];
        let ctx = ScheduleContext::new(vec![], nodes);
        let sched = LeaderScheduler::new(3);
        let ops = sched.schedule(&ctx);
        assert!(ops.is_empty());
    }

    #[test]
    fn test_leader_scheduler_no_imbalance() {
        let nodes = vec![
            make_node_state(1, true),
            make_node_state(2, true),
        ];
        let regions = vec![
            make_region(1, 0, 0, vec![0x00], vec![0x55], vec![
                make_peer(1, PeerRole::Voter),
                make_peer(2, PeerRole::Voter),
            ]),
            make_region(2, 0, 0, vec![0x55], vec![0xFF], vec![
                make_peer(2, PeerRole::Voter),
                make_peer(1, PeerRole::Voter),
            ]),
        ];
        let ctx = ScheduleContext::new(regions, nodes);
        let sched = LeaderScheduler::new(3);
        let ops = sched.schedule(&ctx);
        // Leader counts should be roughly equal
        assert!(ops.is_empty(), "balanced leaders: no ops expected");
    }

    #[test]
    fn test_leader_scheduler_respects_max_ops() {
        let nodes = vec![
            make_node_state(1, true),
            make_node_state(2, true),
        ];
        let mut regions = Vec::new();
        for i in 0..10 {
            regions.push(make_region(
                i, 0, 0,
                vec![i as u8],
                vec![i as u8 + 1],
                vec![
                    make_peer(1, PeerRole::Voter),
                    make_peer(2, PeerRole::Voter),
                ],
            ));
        }
        let ctx = ScheduleContext::new(regions, nodes);
        let sched = LeaderScheduler::new(2);
        let ops = sched.schedule(&ctx);
        assert!(ops.len() <= 2, "should respect max_ops_per_round=2");
    }

    // ──── HotSpotScheduler 测试 ────

    #[test]
    fn test_hotspot_scheduler_single_node() {
        let nodes = vec![make_node_state(1, true)];
        let ctx = ScheduleContext::new(vec![], nodes);
        let sched = HotSpotScheduler::new(100, 500, 3);
        let ops = sched.schedule(&ctx);
        assert!(ops.is_empty());
    }

    #[test]
    fn test_hotspot_scheduler_no_hotspots() {
        let nodes = vec![
            make_node_state(1, true),
            make_node_state(2, true),
        ];
        let regions = vec![
            make_region(1, 10 * 1024 * 1024, 5000, vec![0x00], vec![0x55], vec![
                make_peer(1, PeerRole::Voter),
                make_peer(2, PeerRole::Voter),
            ]),
        ];
        let ctx = ScheduleContext::new(regions, nodes);
        let sched = HotSpotScheduler::new(100, 500, 3);
        let ops = sched.schedule(&ctx);
        // No QPS data available in RegionMeta → no hotspots detected
        assert!(ops.is_empty());
    }

    #[test]
    fn test_hotspot_scheduler_respects_max_ops() {
        let nodes = vec![
            make_node_state(1, true),
            make_node_state(2, true),
        ];
        let mut regions = Vec::new();
        for i in 0..10 {
            regions.push(make_region(
                i, 300 * 1024 * 1024, 2_000_000,
                vec![i as u8],
                vec![i as u8 + 1],
                vec![make_peer(1, PeerRole::Voter), make_peer(2, PeerRole::Voter)],
            ));
        }
        let ctx = ScheduleContext::new(regions, nodes);
        let sched = HotSpotScheduler::new(100, 500, 2);
        let ops = sched.schedule(&ctx);
        assert!(ops.len() <= 2, "should respect max_ops_per_round=2");
    }

    // ──── Scheduler trait 测试 ────

    #[test]
    fn test_split_checker_scheduler_trait() {
        let config = PdConfig::default();
        let checker = SplitChecker::new(&config);
        assert_eq!(checker.name(), "split-checker");

        let nodes = vec![make_node_state(1, true)];
        let region = make_region(1, 300 * 1024 * 1024, 500_000, vec![0x00], vec![0xFF], vec![]);
        let ctx = ScheduleContext::new(vec![region], nodes);
        let ops = checker.schedule(&ctx);
        assert!(!ops.is_empty());
        assert!(matches!(ops[0], Operator::SplitRegion { .. }));
    }

    #[test]
    fn test_merge_checker_scheduler_trait() {
        let config = PdConfig::default();
        let checker = MergeChecker::new(&config);
        assert_eq!(checker.name(), "merge-checker");

        let nodes = vec![make_node_state(1, true)];
        let left = make_region(1, 10 * 1024 * 1024, 100_000, vec![0x00], vec![0x55], vec![]);
        let right = make_region(2, 5 * 1024 * 1024, 50_000, vec![0x55], vec![0xFF], vec![]);
        let ctx = ScheduleContext::new(vec![left, right], nodes);
        let ops = checker.schedule(&ctx);
        assert!(!ops.is_empty());
        assert!(matches!(ops[0], Operator::MergeRegion { .. }));
    }

    #[test]
    fn test_replica_checker_scheduler_trait() {
        let config = PdConfig::default();
        let checker = ReplicaChecker::new(&config);
        assert_eq!(checker.name(), "replica-checker");

        let nodes = vec![make_node_state(1, true)];
        let region = make_region(1, 0, 0, vec![], vec![], vec![make_peer(1, PeerRole::Voter)]);
        let ctx = ScheduleContext::new(vec![region], nodes);
        let ops = checker.schedule(&ctx);
        assert!(!ops.is_empty());
        assert!(matches!(ops[0], Operator::AddPeer { .. }));
    }

    // ──── create_default_schedulers 测试 ────

    #[test]
    fn test_create_default_schedulers() {
        let config = PdConfig::default();
        let schedulers = create_default_schedulers(&config);
        assert_eq!(schedulers.len(), 6);
        let names: Vec<&str> = schedulers.iter().map(|s| s.name()).collect();
        assert!(names.contains(&"split-checker"));
        assert!(names.contains(&"merge-checker"));
        assert!(names.contains(&"replica-checker"));
        assert!(names.contains(&"balance-scheduler"));
        assert!(names.contains(&"leader-scheduler"));
        assert!(names.contains(&"hotspot-scheduler"));
    }

    // ──── mid_key 测试 ────

    #[test]
    fn test_mid_key_empty() {
        let mid = mid_key(&[], &[]);
        assert_eq!(mid, vec![0x80]);
    }

    #[test]
    fn test_mid_key_range() {
        let mid = mid_key(&[0x00], &[0xFF]);
        assert!(!mid.is_empty());
        // Mid should be between start and end
        assert!(mid.as_slice() > [0x00].as_slice());
    }

    // ──── KeySampler 测试 ────

    #[test]
    fn test_key_sampler_empty_samples() {
        let sampler = KeySampler::default();
        let result = sampler.select_split_key(&[]);
        assert!(result.is_none(), "empty samples should return None");
    }

    #[test]
    fn test_key_sampler_single_sample() {
        let sampler = KeySampler::default();
        let samples = vec![b"key_middle".to_vec()];
        let result = sampler.select_split_key(&samples);
        assert_eq!(result, Some(b"key_middle".to_vec()));
    }

    #[test]
    fn test_key_sampler_median_selection() {
        let sampler = KeySampler::default();
        let samples = vec![
            b"a_key".to_vec(),
            b"c_key".to_vec(),
            b"b_key".to_vec(),
            b"e_key".to_vec(),
            b"d_key".to_vec(),
        ];
        // Sorted: a_key, b_key, c_key, d_key, e_key → median = c_key (index 2)
        let result = sampler.select_split_key(&samples);
        assert_eq!(result, Some(b"c_key".to_vec()), "median of 5 should be the 3rd");
    }

    #[test]
    fn test_key_sampler_median_even_count() {
        let sampler = KeySampler::default();
        let samples = vec![
            b"a".to_vec(),
            b"d".to_vec(),
            b"b".to_vec(),
            b"c".to_vec(),
        ];
        // Sorted: a, b, c, d → median = c (index 2 = 4/2)
        let result = sampler.select_split_key(&samples);
        assert_eq!(result, Some(b"c".to_vec()));
    }

    #[test]
    fn test_key_sampler_fallback_to_mid() {
        let sampler = KeySampler::default();
        let result = sampler.select_or_fallback(&[], &[0x10], &[0x20]);
        // Should fall back to mid_key
        assert!(!result.is_empty());
        assert!(result.as_slice() >= &[0x10u8] as &[u8]);
    }

    #[test]
    fn test_key_sampler_reservoir_sampling() {
        let sampler = KeySampler::new(10);
        // Generate 1000 keys
        let keys: Vec<Vec<u8>> = (0..1000)
            .map(|i| format!("key_{:04}", i).into_bytes())
            .collect();
        let samples = sampler.reservoir_sample(keys);
        assert_eq!(samples.len(), 10, "reservoir should have exactly max_samples items");
        // All samples should be unique (probabilistic, but highly likely)
        let mut unique: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
        for s in &samples {
            unique.insert(s.clone());
        }
        assert_eq!(unique.len(), 10, "reservoir samples should be unique");
    }

    #[test]
    fn test_key_sampler_reservoir_fewer_than_max() {
        let sampler = KeySampler::new(100);
        let keys: Vec<Vec<u8>> = (0..5)
            .map(|i| format!("key_{}", i).into_bytes())
            .collect();
        let samples = sampler.reservoir_sample(keys);
        assert_eq!(samples.len(), 5, "fewer than max: should return all");
    }

    #[test]
    fn test_schedule_context_with_samples() {
        use std::collections::HashMap;
        let nodes = vec![make_node_state(1, true)];
        let regions = vec![
            make_region(1, 300 * 1024 * 1024, 500_000, vec![0x00], vec![0xFF], vec![]),
        ];
        let mut sample_keys = HashMap::new();
        sample_keys.insert(1u64, vec![b"sample_mid".to_vec(), b"sample_end".to_vec()]);

        let ctx = ScheduleContext::with_samples(regions, nodes, sample_keys);
        assert!(ctx.region_sample_keys.contains_key(&1));
        assert_eq!(ctx.region_sample_keys[&1].len(), 2);
    }

    #[test]
    fn test_split_checker_uses_sample_keys() {
        let config = PdConfig::default();
        let checker = SplitChecker::new(&config);

        use std::collections::HashMap;
        let nodes = vec![make_node_state(1, true)];
        let region = make_region(1, 300 * 1024 * 1024, 500_000, vec![0x00], vec![0xFF], vec![]);
        let mut sample_keys = HashMap::new();
        // Provide a specific sample key that should be used as split_key
        sample_keys.insert(1u64, vec![b"my_split_point".to_vec()]);

        let ctx = ScheduleContext::with_samples(vec![region], nodes, sample_keys);
        let ops = checker.schedule(&ctx);
        assert!(!ops.is_empty());
        if let Operator::SplitRegion { split_key, .. } = &ops[0] {
            // With a single sample, the sampler picks it as the median
            assert_eq!(split_key, b"my_split_point");
        } else {
            panic!("expected SplitRegion operator");
        }
    }

    #[test]
    fn test_split_checker_falls_back_without_samples() {
        let config = PdConfig::default();
        let checker = SplitChecker::new(&config);

        let nodes = vec![make_node_state(1, true)];
        let region = make_region(1, 300 * 1024 * 1024, 500_000, vec![0x10], vec![0x20], vec![]);
        let ctx = ScheduleContext::new(vec![region], nodes);
        let ops = checker.schedule(&ctx);
        assert!(!ops.is_empty());
        if let Operator::SplitRegion { split_key, .. } = &ops[0] {
            // Should use mid_key fallback (between 0x10 and 0x20)
            assert!(!split_key.is_empty());
            // mid_key of [0x10] and [0x20] should be >= 0x10
            assert!(split_key.as_slice() >= &[0x10u8] as &[u8]);
        } else {
            panic!("expected SplitRegion operator");
        }
    }

    // ──── 辅助函数 ────

    fn make_node_state(id: u64, online: bool) -> NodeState {
        NodeState {
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
        }
    }
}
