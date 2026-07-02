// 客户端路由缓存 — Region → Leader Node 映射
//
// 客户端 SDK 维护 Region → (Key Range, Leader Node) 的路由缓存。
// 缓存命中时直接发送到目标节点，未命中时查询 PD。
// Epoch 保护确保客户端不会向过期 Leader 或已分裂 Region 发送请求。
//
// 设计要点（ADP §8）：
// - 缓存策略：LRU + TTL，默认 1000 条，60s TTL
// - Epoch 校验：服务端拒绝过期 Epoch 的请求后，客户端自动更新缓存
// - NotLeader 处理：自动重试到新 Leader

use std::collections::HashMap;
use std::time::{Duration, Instant};

use coord_core::types::{RegionEpoch, RegionId, RegionMeta};
use parking_lot::RwLock;

// ============================================================================
// 缓存条目
// ============================================================================

/// 缓存的一条 Region 路由信息
#[derive(Debug, Clone)]
pub struct CachedRegion {
    /// Region 元数据（Key Range、Epoch、Peers）
    pub meta: RegionMeta,
    /// 当前 Leader 的 gRPC 地址
    pub leader_addr: String,
    /// 缓存时间
    pub cached_at: Instant,
}

impl CachedRegion {
    /// 是否已过期（超过 TTL）
    pub fn is_expired(&self, ttl: Duration) -> bool {
        self.cached_at.elapsed() > ttl
    }
}

// ============================================================================
// RouteCache
// ============================================================================

/// 客户端路由缓存
///
/// 维护 Region ID → (Key Range, Leader Node) 的映射。
/// 线程安全（内部 RwLock 保护）。
pub struct RouteCache {
    /// Region ID → CachedRegion
    regions: RwLock<HashMap<RegionId, CachedRegion>>,
    /// 缓存 TTL
    ttl: Duration,
    /// 最大缓存条目数
    max_entries: usize,
}

impl RouteCache {
    /// 创建新的路由缓存
    pub fn new(max_entries: usize, ttl: Duration) -> Self {
        Self {
            regions: RwLock::new(HashMap::new()),
            ttl,
            max_entries,
        }
    }

    /// 默认配置：1000 条、60s TTL
    pub fn default_config() -> Self {
        Self::new(1000, Duration::from_secs(60))
    }

    /// 根据 key 查找目标 Region 的 Leader 地址
    ///
    /// 返回 `Some((region_id, leader_addr))` 如果命中缓存且未过期。
    /// 返回 `None` 如果未命中或已过期。
    pub fn lookup(&self, key: &[u8]) -> Option<(RegionId, String)> {
        let regions = self.regions.read();

        // 找到 key 所属的 Region（遍历所有缓存条目，检查 Key Range）
        // 实际生产环境应使用二分查找，这里为简化实现采用线性扫描
        for (region_id, cached) in regions.iter() {
            if cached.is_expired(self.ttl) {
                continue;
            }
            if cached.meta.contains_key(key) {
                return Some((*region_id, cached.leader_addr.clone()));
            }
        }

        None
    }

    /// 根据 Region ID 获取缓存的 Leader 地址
    pub fn get_leader(&self, region_id: RegionId) -> Option<String> {
        let regions = self.regions.read();
        regions
            .get(&region_id)
            .filter(|c| !c.is_expired(self.ttl))
            .map(|c| c.leader_addr.clone())
    }

    /// 更新缓存（插入或替换）
    pub fn update(&self, meta: RegionMeta, leader_addr: String) {
        let mut regions = self.regions.write();

        // LRU 淘汰：若超过最大条目数，删除最旧的条目
        if regions.len() >= self.max_entries {
            // 找到最旧的条目并删除
            let oldest_key = regions
                .iter()
                .min_by_key(|(_, v)| v.cached_at)
                .map(|(k, _)| *k);
            if let Some(key) = oldest_key {
                regions.remove(&key);
            }
        }

        let region_id = meta.region_id;
        regions.insert(
            region_id,
            CachedRegion {
                meta,
                leader_addr,
                cached_at: Instant::now(),
            },
        );
    }

    /// 使指定 Region 的缓存失效
    pub fn invalidate(&self, region_id: RegionId) {
        self.regions.write().remove(&region_id);
    }

    /// 处理 NotLeader 响应：更新缓存中的 Leader 地址
    ///
    /// 参数：
    /// - `region_id`：Region ID
    /// - `new_leader_hint`：服务端返回的新 Leader 地址（可选）
    pub fn handle_not_leader(&self, region_id: RegionId, new_leader_hint: Option<String>) {
        if let Some(addr) = new_leader_hint {
            // 更新 Leader 地址
            let mut regions = self.regions.write();
            if let Some(cached) = regions.get_mut(&region_id) {
                cached.leader_addr = addr;
                cached.cached_at = Instant::now();
                return;
            }
        }

        // 无 Leader 提示，直接失效缓存
        self.invalidate(region_id);
    }

    /// 处理 Region Split：更新缓存
    ///
    /// 当服务端返回 RegionSplit 错误时，用新的 RegionMeta 更新缓存。
    pub fn handle_region_split(&self, new_meta: RegionMeta, leader_addr: String) {
        self.invalidate(new_meta.region_id);
        self.update(new_meta, leader_addr);
    }

    /// 获取当前缓存的 Epoch（用于在请求中携带）
    pub fn get_epoch(&self, region_id: RegionId) -> Option<RegionEpoch> {
        let regions = self.regions.read();
        regions.get(&region_id).map(|c| c.meta.epoch)
    }

    /// 检查缓存中是否包含指定 Region
    pub fn contains(&self, region_id: RegionId) -> bool {
        self.regions
            .read()
            .get(&region_id)
            .map(|c| !c.is_expired(self.ttl))
            .unwrap_or(false)
    }

    /// 获取缓存条目数
    pub fn len(&self) -> usize {
        self.regions.read().len()
    }

    /// 缓存是否为空
    pub fn is_empty(&self) -> bool {
        self.regions.read().is_empty()
    }

    /// 清空所有缓存
    pub fn clear(&self) {
        self.regions.write().clear();
    }

    /// 清除所有过期条目
    pub fn evict_expired(&self) -> usize {
        let mut regions = self.regions.write();
        let before = regions.len();
        regions.retain(|_, c| !c.is_expired(self.ttl));
        before - regions.len()
    }
}

// ============================================================================
// 测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use coord_core::types::{Peer, PeerRole};

    fn make_meta(region_id: RegionId, start: Vec<u8>, end: Vec<u8>) -> RegionMeta {
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
    fn test_route_cache_create() {
        let cache = RouteCache::default_config();
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_route_cache_update_and_lookup() {
        let cache = RouteCache::default_config();
        let meta = make_meta(1, vec![0x00], vec![0x55]);
        cache.update(meta, "node1:50051".to_string());

        let result = cache.lookup(&[0x10]);
        assert!(result.is_some());
        let (region_id, leader_addr) = result.unwrap();
        assert_eq!(region_id, 1);
        assert_eq!(leader_addr, "node1:50051");
    }

    #[test]
    fn test_route_cache_lookup_miss() {
        let cache = RouteCache::default_config();
        let meta = make_meta(1, vec![0x00], vec![0x55]);
        cache.update(meta, "node1:50051".to_string());

        // key 不在 Region 1 的范围内
        let result = cache.lookup(&[0x80]);
        assert!(result.is_none());
    }

    #[test]
    fn test_route_cache_get_leader() {
        let cache = RouteCache::default_config();
        let meta = make_meta(1, vec![], vec![]);
        cache.update(meta, "node1:50051".to_string());

        assert_eq!(cache.get_leader(1), Some("node1:50051".to_string()));
        assert_eq!(cache.get_leader(999), None);
    }

    #[test]
    fn test_route_cache_invalidate() {
        let cache = RouteCache::default_config();
        cache.update(make_meta(1, vec![0x00], vec![0x55]), "node1".to_string());
        assert_eq!(cache.len(), 1);

        cache.invalidate(1);
        assert_eq!(cache.len(), 0);
        assert!(cache.lookup(&[0x10]).is_none());
    }

    #[test]
    fn test_route_cache_handle_not_leader() {
        let cache = RouteCache::default_config();
        cache.update(make_meta(1, vec![0x00], vec![0x55]), "node1".to_string());

        // 有 Leader hint
        cache.handle_not_leader(1, Some("node3:50051".to_string()));
        assert_eq!(cache.get_leader(1), Some("node3:50051".to_string()));

        // 无 Leader hint
        cache.handle_not_leader(1, None);
        assert_eq!(cache.get_leader(1), None);
    }

    #[test]
    fn test_route_cache_epoch() {
        let cache = RouteCache::default_config();
        cache.update(make_meta(1, vec![0x00], vec![0x55]), "node1".to_string());

        let epoch = cache.get_epoch(1);
        assert!(epoch.is_some());
        assert_eq!(epoch.unwrap().conf_ver, 1);
    }

    #[test]
    fn test_route_cache_clear() {
        let cache = RouteCache::default_config();
        cache.update(make_meta(1, vec![0x00], vec![0x55]), "n1".to_string());
        cache.update(make_meta(2, vec![0x55], vec![0xFF]), "n2".to_string());
        assert_eq!(cache.len(), 2);

        cache.clear();
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_route_cache_lru_eviction() {
        // 创建仅 2 条容量的缓存
        let cache = RouteCache::new(2, Duration::from_secs(60));

        cache.update(make_meta(1, vec![0x00], vec![0x55]), "n1".to_string());
        std::thread::sleep(Duration::from_millis(1));
        cache.update(make_meta(2, vec![0x55], vec![0x80]), "n2".to_string());
        assert_eq!(cache.len(), 2);

        // 添加第 3 条，最旧的（region 1）应被淘汰
        cache.update(make_meta(3, vec![0x80], vec![0xFF]), "n3".to_string());
        assert_eq!(cache.len(), 2);
        assert!(!cache.contains(1)); // Region 1 被淘汰
        assert!(cache.contains(2));
        assert!(cache.contains(3));
    }

    #[test]
    fn test_route_cache_multi_region_lookup() {
        let cache = RouteCache::default_config();
        cache.update(make_meta(1, vec![0x00], vec![0x40]), "n1".to_string());
        cache.update(make_meta(2, vec![0x40], vec![0x80]), "n2".to_string());
        cache.update(make_meta(3, vec![0x80], vec![]), "n3".to_string()); // empty end = full range

        assert_eq!(cache.lookup(&[0x00]).unwrap().0, 1);
        assert_eq!(cache.lookup(&[0x40]).unwrap().0, 2);
        assert_eq!(cache.lookup(&[0x80]).unwrap().0, 3);
        assert_eq!(cache.lookup(&[0xFE]).unwrap().0, 3); // 0xFE is inside Region 3
    }
}
