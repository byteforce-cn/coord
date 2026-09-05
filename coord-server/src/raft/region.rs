// ⚠️ Multi-Raft（默认关闭 opt-in）：本模块是 Multi-Raft 生产路径的 Region 生命周期
// 管理核心——`coord/src/main.rs` 在 `multi_raft.enabled=true` 时经
// `spawn_configured_regions` 装配生产路径（T2.3/T2.4），`RegionManager` 同时被
// `pd/embedded.rs`（内嵌 PD，T3.4）引用。
// 默认关闭：未配置 `[multi_raft]` 时走单 Raft 退化路径，region 0 根目录布局字节级不变
// （T2.6）；已接线路径与 v1 边界（静态 Region 表、不支持 join/Split-Merge、无跨 Region
// 语义、per-Region 快照/压缩等）见 `docs/multi-raft-limits.md`，
// 成熟度演进见 `docs/coord-multi-raft-production-plan-2026-09-05.md`。
//
// Region Manager — Multi-Raft Region 生命周期管理
//
// 本模块定义：
// - RegionHandle：  单个 Region 的运行时句柄（包含 Raft 实例引用）
// - RegionManager： 单节点内所有 Region 的管理器（路由、注册、注销）
//
// 设计要点（ADP §2.2, §3）：
// - 使用 BTreeMap 而非 HashMap：按 start_key 有序排列，路由时二分查找 O(log N)
// - 每个 Region 维护独立的 Raft 实例（共享存储引擎、共享网络层）
// - 路由表 key_index：start_key → RegionId，支持高效 key → Region 查找

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use coord_core::error::{Error, Result};
use coord_core::types::{NodeID, Peer, RegionEpoch, RegionId, RegionMeta};
use parking_lot::RwLock;

use crate::raft::network::{RaftNetworkFactoryImpl, RaftRpcService};
use crate::raft::region_runtime::{
    region_data_dir, spawn_region_runtime, RegionRuntime, RegionRuntimeSpec,
};
use crate::raft::RaftConfig;

// ============================================================================
// RegionHandle
// ============================================================================

/// Region 运行时角色
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RegionRole {
    /// 角色未知（初始化中）
    Unknown = 0,
    /// Leader：处理读写请求
    Leader = 1,
    /// Follower：复制日志，可处理只读请求
    Follower = 2,
    /// Candidate：选举中
    Candidate = 3,
}

impl RegionRole {
    /// 从 u8 解码
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => RegionRole::Leader,
            2 => RegionRole::Follower,
            3 => RegionRole::Candidate,
            _ => RegionRole::Unknown,
        }
    }

    /// 是否为 Leader
    pub fn is_leader(&self) -> bool {
        matches!(self, RegionRole::Leader)
    }
}

/// 单个 Region 的运行时句柄
///
/// 每个 RegionHandle 对应一个 Raft Group，维护该 Region 的元数据、
/// Raft 实例引用和运行时状态。
pub struct RegionHandle {
    /// Region 元数据（Key Range、Epoch、Peers）
    pub meta: RwLock<RegionMeta>,
    /// 当前角色（Leader / Follower / Candidate）
    pub role: AtomicU64, // 使用 AtomicU64 存储 RegionRole (u8)
    /// 当前 Leader 地址（若已知）
    pub leader_addr: RwLock<Option<String>>,
    /// 本节点在该 Region 中是否为 Leader
    pub is_leader: AtomicU64, // 0=false, 1=true
    /// Region 级别的 Compaction 水位
    pub compaction_watermark: AtomicU64,
    /// 近似写入 QPS（用于热点检测，滑动窗口）
    pub write_qps: AtomicU64,
    /// 近似读取 QPS（用于热点检测，滑动窗口）
    pub read_qps: AtomicU64,
}

impl RegionHandle {
    /// 创建新的 RegionHandle
    pub fn new(meta: RegionMeta) -> Self {
        Self {
            meta: RwLock::new(meta),
            role: AtomicU64::new(RegionRole::Unknown as u64),
            leader_addr: RwLock::new(None),
            is_leader: AtomicU64::new(0),
            compaction_watermark: AtomicU64::new(0),
            write_qps: AtomicU64::new(0),
            read_qps: AtomicU64::new(0),
        }
    }

    /// 获取 Region ID
    pub fn region_id(&self) -> RegionId {
        self.meta.read().region_id
    }

    /// 获取当前 Epoch
    pub fn epoch(&self) -> RegionEpoch {
        self.meta.read().epoch
    }

    /// 获取当前角色
    pub fn role(&self) -> RegionRole {
        RegionRole::from_u8(self.role.load(Ordering::Relaxed) as u8)
    }

    /// 设置角色
    pub fn set_role(&self, role: RegionRole) {
        self.role.store(role as u64, Ordering::Relaxed);
        self.is_leader
            .store(if role.is_leader() { 1 } else { 0 }, Ordering::Relaxed);
    }

    /// 校验客户端 Epoch 是否过期
    ///
    /// 若客户端 Epoch 已过期，返回相应的 Error。
    pub fn check_epoch(&self, client_epoch: &RegionEpoch) -> Result<()> {
        let current = self.epoch();

        if client_epoch.conf_ver < current.conf_ver {
            return Err(Error::EpochStale {
                region_id: self.region_id(),
                client_conf_ver: client_epoch.conf_ver,
                client_version: client_epoch.version,
                server_conf_ver: current.conf_ver,
                server_version: current.version,
            });
        }

        if client_epoch.version < current.version {
            return Err(Error::EpochStale {
                region_id: self.region_id(),
                client_conf_ver: client_epoch.conf_ver,
                client_version: client_epoch.version,
                server_conf_ver: current.conf_ver,
                server_version: current.version,
            });
        }

        Ok(())
    }

    /// 判断 key 是否属于此 Region
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.meta.read().contains_key(key)
    }

    /// 递增 Epoch 的 conf_ver（成员变更时调用）
    pub fn increment_conf_ver(&self) {
        let mut meta = self.meta.write();
        meta.epoch.conf_ver += 1;
    }

    /// 递增 Epoch 的 version（Region 分裂时调用）
    pub fn increment_version(&self) {
        let mut meta = self.meta.write();
        meta.epoch.version += 1;
    }

    /// 更新近似统计数据
    pub fn update_stats(&self, size: u64, keys: u64) {
        let mut meta = self.meta.write();
        meta.approximate_size = size;
        meta.approximate_keys = keys;
    }
}

// ============================================================================
// RegionManager
// ============================================================================

/// 单节点内所有 Region 的管理器
///
/// 职责：
/// - 维护 RegionHandle 注册表（BTreeMap<RegionId, Arc<RegionHandle>>）
/// - 维护 key → Region 路由索引（BTreeMap<start_key, RegionId>）
/// - 提供 O(log N) 的 key 路由查找
/// - 管理 Region 的注册、注销和生命周期
pub struct RegionManager {
    /// 本节点 ID
    node_id: NodeID,
    /// RegionId → RegionHandle 映射
    regions: RwLock<BTreeMap<RegionId, Arc<RegionHandle>>>,
    /// start_key → RegionId 有序索引（用于路由）
    key_index: RwLock<BTreeMap<Vec<u8>, RegionId>>,
    /// Region 运行时注册表（T2.3：装配后的 raft/存储句柄；路由见 regions/key_index）
    runtimes: RwLock<BTreeMap<RegionId, Arc<RegionRuntime>>>,
}

impl RegionManager {
    /// 创建新的 RegionManager
    pub fn new(node_id: NodeID) -> Self {
        Self {
            node_id,
            regions: RwLock::new(BTreeMap::new()),
            key_index: RwLock::new(BTreeMap::new()),
            runtimes: RwLock::new(BTreeMap::new()),
        }
    }

    /// 获取本节点 ID
    pub fn node_id(&self) -> NodeID {
        self.node_id
    }

    // ──── Region 注册 ────

    /// 注册一个新 Region
    ///
    /// 将 Region 加入 regions 表和 key_index 路由索引。
    /// 若 region_id 已存在或 start_key 冲突则返回错误。
    pub fn register_region(&self, meta: RegionMeta) -> Result<Arc<RegionHandle>> {
        let region_id = meta.region_id;
        let start_key = meta.start_key.clone();

        // 检查 region_id 冲突
        {
            let regions = self.regions.read();
            if regions.contains_key(&region_id) {
                return Err(Error::AlreadyExists {
                    resource: "region",
                    key: region_id.to_string(),
                });
            }
        }

        // 检查 start_key 冲突
        {
            let key_index = self.key_index.read();
            if key_index.contains_key(&start_key) {
                return Err(Error::AlreadyExists {
                    resource: "region_start_key",
                    key: format!("{:?}", start_key),
                });
            }
        }

        let handle = Arc::new(RegionHandle::new(meta));

        // 原子注册
        {
            let mut regions = self.regions.write();
            regions.insert(region_id, Arc::clone(&handle));
        }
        {
            let mut key_index = self.key_index.write();
            key_index.insert(start_key, region_id);
        }

        tracing::info!(
            "Region {} registered: start_key={:?}, peers={}",
            region_id,
            handle.meta.read().start_key,
            handle.meta.read().peers.len()
        );

        Ok(handle)
    }

    /// 注销一个 Region
    ///
    /// 从 regions 表和 key_index 中移除。通常在 Region Merge 或强制移除时调用。
    pub fn unregister_region(&self, region_id: RegionId) -> Result<()> {
        let handle = {
            let regions = self.regions.read();
            regions
                .get(&region_id)
                .cloned()
                .ok_or(Error::RegionNotFound { region_id })?
        };

        let start_key = handle.meta.read().start_key.clone();

        {
            let mut regions = self.regions.write();
            regions.remove(&region_id);
        }
        {
            let mut key_index = self.key_index.write();
            key_index.remove(&start_key);
        }

        tracing::info!("Region {} unregistered", region_id);
        Ok(())
    }

    // ──── Region 查询 ────

    /// 通过 region_id 获取 RegionHandle
    pub fn get_region(&self, region_id: RegionId) -> Option<Arc<RegionHandle>> {
        self.regions.read().get(&region_id).cloned()
    }

    /// 通过 key 路由到目标 Region
    ///
    /// 使用 BTreeMap 的二分查找，时间复杂度 O(log N)。
    /// 返回 key 所属 Region 的 RegionHandle。
    pub fn route(&self, key: &[u8]) -> Result<Arc<RegionHandle>> {
        let key_index = self.key_index.read();

        if key_index.is_empty() {
            return Err(Error::RouteNotReady);
        }

        // 找到最后一个 start_key <= key 的 Region
        // BTreeMap::range(..=key) 返回所有 start_key <= key 的条目，
        // 取最后一个即为目标 Region
        let region_id = key_index
            .range(..=key.to_vec())
            .next_back()
            .map(|(_, &rid)| rid)
            .ok_or(Error::RouteNotReady)?;

        // 释放 key_index 锁，然后获取 Region
        drop(key_index);

        let region = self
            .get_region(region_id)
            .ok_or(Error::RegionNotFound { region_id })?;

        // 验证 key 确实在 Region 的范围内
        if !region.contains_key(key) {
            return Err(Error::KeyNotInRegion { region_id });
        }

        Ok(region)
    }

    /// 列出所有 Region
    pub fn list_regions(&self) -> Vec<Arc<RegionHandle>> {
        self.regions.read().values().cloned().collect()
    }

    /// 获取 Region 总数
    pub fn region_count(&self) -> usize {
        self.regions.read().len()
    }

    /// 获取本节点是 Leader 的 Region 数量
    pub fn leader_count(&self) -> usize {
        self.regions
            .read()
            .values()
            .filter(|h| h.role().is_leader())
            .count()
    }

    /// 获取本节点的 Region 副本总数
    pub fn replica_count(&self) -> usize {
        self.regions.read().len()
    }

    // ──── 路由索引维护 ────

    /// 更新 Region 的 start_key（分裂后调用）
    ///
    /// 在 Region 分裂后，原 Region 的 end_key 缩小，新 Region 注册到路由表。
    /// 此方法更新 key_index 中该 Region 的 start_key → region_id 映射。
    pub fn update_region_key_range(
        &self,
        region_id: RegionId,
        new_start_key: Vec<u8>,
        new_end_key: Vec<u8>,
    ) -> Result<()> {
        let region = self
            .get_region(region_id)
            .ok_or(Error::RegionNotFound { region_id })?;

        let old_start_key = region.meta.read().start_key.clone();

        // 更新元数据
        {
            let mut meta = region.meta.write();
            meta.start_key = new_start_key.clone();
            meta.end_key = new_end_key;
        }

        // 更新 key_index
        {
            let mut key_index = self.key_index.write();
            if old_start_key != new_start_key {
                key_index.remove(&old_start_key);
                key_index.insert(new_start_key, region_id);
            }
        }

        Ok(())
    }

    // ──── Region 运行时装配（T2.3 生产接线）────

    /// 装配一个 Region 运行时并注册
    ///
    /// 一步完成：Region 存储目录 + MvccStorage/LogStore/SnapshotTracker 打开 +
    /// per-region Raft 创建（RegionRaftNetworkFactory 共享节点连接池）+
    /// RaftRpcService 网络注册 + 路由（regions/key_index）与运行时注册。
    ///
    /// - `initialize=true`：本节点为 bootstrap，用 region meta 的 voter peers
    ///   初始化成员（仅集群首个节点）；其余节点传 `false`，靠复制追赶。
    /// - 存储隔离为目录级（`region_data_dir`），region 0 = legacy 根目录布局，
    ///   见 `raft/region_runtime.rs` 模块文档。
    pub async fn spawn_region(
        &self,
        shared_factory: &RaftNetworkFactoryImpl,
        rpc: &RaftRpcService,
        spec: RegionRuntimeSpec,
        initialize: bool,
    ) -> Result<Arc<RegionRuntime>> {
        let region_id = spec.meta.region_id;

        // 1) 路由注册：重复 region_id / start_key 在此拒绝
        let handle = self.register_region(spec.meta.clone())?;

        // 2) 存储 + Raft + 网络注册
        let runtime = spawn_region_runtime(
            self.node_id,
            shared_factory,
            rpc,
            spec,
            handle,
            initialize,
        )
        .await?;

        // 3) 运行时注册
        self.runtimes
            .write()
            .insert(region_id, Arc::clone(&runtime));
        tracing::info!(
            "node {}: region {region_id} runtime spawned (data_dir={}, {} peers)",
            self.node_id,
            runtime.data_dir.display(),
            runtime.meta().peers.len()
        );
        Ok(runtime)
    }

    /// 移除一个 Region 运行时（注销路由 + 运行时；Raft 停机由调用方负责）
    pub fn remove_region_runtime(&self, region_id: RegionId) -> Result<()> {
        self.unregister_region(region_id)?;
        self.runtimes.write().remove(&region_id);
        tracing::info!("node {}: region {region_id} runtime removed", self.node_id);
        Ok(())
    }

    /// 通过 region_id 获取已装配的 Region 运行时
    pub fn runtime(&self, region_id: RegionId) -> Option<Arc<RegionRuntime>> {
        self.runtimes.read().get(&region_id).cloned()
    }

    /// 通过 key 路由到 Region 运行时（T2.4 服务端路由的基础）
    ///
    /// 先经 `route` 按 key range 二分定位 Region，再取其运行时；
    /// Region 已注册但尚未装配（无运行时）时返回 RegionNotFound。
    pub fn route_runtime(&self, key: &[u8]) -> Result<Arc<RegionRuntime>> {
        let handle = self.route(key)?;
        let region_id = handle.region_id();
        self.runtime(region_id)
            .ok_or(Error::RegionNotFound { region_id })
    }
}

// ──── 配置驱动批量装配（T3.4 静态装配；main.rs 在 `[multi_raft].enabled=true`
//      时调用，测试套件同路径）────

/// 一个待装配 Region 的静态种子（v1 来自 `[multi_raft].initial_regions` 配置；
/// 每个 Region 复制到同一批集群成员节点）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionSeed {
    /// Region ID（必须 > 0 且唯一；0 保留给单 Raft/system raft）
    pub region_id: RegionId,
    /// Key range 起始（包含）；空 = keyspace 起点（仅允许第一个 region）
    pub start_key: Vec<u8>,
    /// Key range 结束（不包含）；空 = 无上界（仅允许最后一个 region）
    pub end_key: Vec<u8>,
}

/// 装配配置声明的全部 Region，返回已装配并注册的 RegionManager。
///
/// - `data_dir`：节点数据根目录；每 Region 存储目录 =
///   `region_data_dir(data_dir, id)`（region 0 保留给单 Raft/system raft，不在此表）。
/// - `peers`：集群成员（v1 = `cluster.initial_nodes` 全部）；每个 Region 的
///   `RegionMeta.peers = peers` 全量（v1 静态复制，无按 Region 差异化分布）。
/// - `initialize=true`（仅集群 bootstrap 节点）：对每个 Region 用其 voter peers
///   初始化成员；其余节点传 `false`，靠 leader 复制追赶。
/// - 本函数对 region 表做防御性平铺校验（首 region start_key 为空、相邻首尾相接、
///   末 region end_key 无上界），并拒绝非成员节点装配——不合法时在创建任何
///   存储之前返回 `Err`（配置层 `Config::validate` 已先行校验，此为双保险）。
pub async fn spawn_configured_regions(
    node_id: NodeID,
    data_dir: &Path,
    shared_factory: &RaftNetworkFactoryImpl,
    rpc: &RaftRpcService,
    raft_config: Arc<RaftConfig>,
    seeds: &[RegionSeed],
    peers: &[Peer],
    initialize: bool,
) -> Result<Arc<RegionManager>> {
    // 防御性平铺校验（全部在 IO 之前）
    let mut sorted = seeds.to_vec();
    sorted.sort_by(|a, b| a.start_key.cmp(&b.start_key));
    if let Some(first) = sorted.first() {
        if !first.start_key.is_empty() {
            return Err(Error::InvalidArgument(format!(
                "region table must start at empty start_key (got region {} start {:?})",
                first.region_id, first.start_key
            )));
        }
    }
    for w in sorted.windows(2) {
        if w[0].end_key != w[1].start_key {
            return Err(Error::InvalidArgument(format!(
                "region table must be contiguous with no gap/overlap: \
                 region {} end {:?} != region {} start {:?}",
                w[0].region_id, w[0].end_key, w[1].region_id, w[1].start_key
            )));
        }
    }
    if let Some(last) = sorted.last() {
        if !last.end_key.is_empty() {
            return Err(Error::InvalidArgument(format!(
                "region table must end with an unbounded end_key (region {} end {:?})",
                last.region_id, last.end_key
            )));
        }
    }

    let member_ids: std::collections::HashSet<u64> =
        peers.iter().map(|p| p.node_id).collect();
    if !member_ids.contains(&node_id) {
        return Err(Error::InvalidArgument(format!(
            "node {node_id} is not a member of the region peers; cannot assemble regions"
        )));
    }
    if peers.is_empty() {
        return Err(Error::InvalidArgument(
            "region peers must not be empty".to_string(),
        ));
    }

    let manager = Arc::new(RegionManager::new(node_id));
    for seed in seeds {
        let meta = RegionMeta {
            region_id: seed.region_id,
            start_key: seed.start_key.clone(),
            end_key: seed.end_key.clone(),
            epoch: RegionEpoch::initial(),
            peers: peers.to_vec(),
            approximate_size: 0,
            approximate_keys: 0,
        };
        let spec = RegionRuntimeSpec {
            meta,
            data_dir: region_data_dir(data_dir, seed.region_id),
            raft_config: Arc::clone(&raft_config),
        };
        manager.spawn_region(shared_factory, rpc, spec, initialize).await?;
        tracing::info!(
            "node {node_id}: configured region {} assembled (range {:?}..{:?})",
            seed.region_id,
            seed.start_key,
            seed.end_key
        );
    }
    Ok(manager)
}

// ============================================================================
// 测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use coord_core::types::{Peer, PeerRole};

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
}
