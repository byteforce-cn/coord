// PD Meta Store — Region 元数据持久化与内存索引
//
// PdMetaStore 负责：
// - 持久化 Region 元数据到共享存储
// - 维护内存索引（start_key → RegionId 二分查找）
// - 提供 Region 元数据的 CRUD 操作
//
// 设计要点（ADP §4.1）：
// - 持久化 Key: /pd/region/{region_id:016x}
// - 内存索引: BTreeMap<start_key, RegionId>（O(log N) 查找）
// - 并发安全：RwLock 保护

use std::collections::BTreeMap;

use coord_core::error::{Error, Result};
use coord_core::types::{RegionId, RegionMeta};
use parking_lot::RwLock;

// ============================================================================
// PdMetaStore
// ============================================================================

/// PD 元数据存储
///
/// 内存优先的 Region 元数据仓库。Phase 1-2 期间为内存模式，
/// Phase 3+ 通过 Raft 共识持久化到共享存储。
pub struct PdMetaStore {
    /// Region 元数据：RegionId → RegionMeta
    regions: RwLock<BTreeMap<RegionId, RegionMeta>>,
    /// start_key → RegionId 有序索引（用于路由查找）
    key_index: RwLock<BTreeMap<Vec<u8>, RegionId>>,
}

impl PdMetaStore {
    /// 创建新的 PdMetaStore
    pub fn new() -> Self {
        Self {
            regions: RwLock::new(BTreeMap::new()),
            key_index: RwLock::new(BTreeMap::new()),
        }
    }

    // ──── Region 元数据 CRUD ────

    /// 创建 Region 元数据
    ///
    /// 若 region_id 或 start_key 已存在则返回错误。
    pub fn create_region(&self, meta: RegionMeta) -> Result<()> {
        let region_id = meta.region_id;
        let start_key = meta.start_key.clone();

        {
            let regions = self.regions.read();
            if regions.contains_key(&region_id) {
                return Err(Error::AlreadyExists {
                    resource: "region",
                    key: region_id.to_string(),
                });
            }
        }

        {
            let key_index = self.key_index.read();
            if key_index.contains_key(&start_key) {
                return Err(Error::AlreadyExists {
                    resource: "region_start_key",
                    key: format!("{:?}", start_key),
                });
            }
        }

        {
            let mut regions = self.regions.write();
            regions.insert(region_id, meta);
        }
        {
            let mut key_index = self.key_index.write();
            key_index.insert(start_key.clone(), region_id);
        }

        tracing::info!(
            "PD: created region {} [start={:?}, end={:?}]",
            region_id,
            start_key,
            self.get_region(region_id)
                .map(|r| r.end_key.clone())
                .unwrap_or_default()
        );

        Ok(())
    }

    /// 获取 Region 元数据
    pub fn get_region(&self, region_id: RegionId) -> Option<RegionMeta> {
        self.regions.read().get(&region_id).cloned()
    }

    /// 根据 key 查找 Region
    ///
    /// 使用 BTreeMap 的二分查找，时间复杂度 O(log N)。
    pub fn get_region_by_key(&self, key: &[u8]) -> Option<RegionMeta> {
        let key_index = self.key_index.read();
        let region_id = key_index
            .range(..=key.to_vec())
            .next_back()
            .map(|(_, &rid)| rid)?;

        drop(key_index);
        self.get_region(region_id)
    }

    /// 更新 Region 元数据
    ///
    /// 若 start_key 发生变化，自动更新 key_index。
    pub fn update_region(&self, meta: RegionMeta) -> Result<()> {
        let region_id = meta.region_id;
        let new_start_key = meta.start_key.clone();

        let old_start_key = {
            let regions = self.regions.read();
            regions
                .get(&region_id)
                .map(|r| r.start_key.clone())
                .ok_or(Error::RegionNotFound { region_id })?
        };

        // 更新数据
        {
            let mut regions = self.regions.write();
            regions.insert(region_id, meta);
        }

        // 如果 start_key 变化，更新索引
        if old_start_key != new_start_key {
            let mut key_index = self.key_index.write();
            key_index.remove(&old_start_key);
            key_index.insert(new_start_key, region_id);
        }

        Ok(())
    }

    /// 删除 Region 元数据
    pub fn delete_region(&self, region_id: RegionId) -> Result<()> {
        let region = {
            let regions = self.regions.read();
            regions
                .get(&region_id)
                .cloned()
                .ok_or(Error::RegionNotFound { region_id })?
        };

        let start_key = region.start_key;

        {
            let mut regions = self.regions.write();
            regions.remove(&region_id);
        }
        {
            let mut key_index = self.key_index.write();
            key_index.remove(&start_key);
        }

        tracing::info!("PD: deleted region {}", region_id);
        Ok(())
    }

    /// 列出所有 Region
    pub fn list_regions(&self) -> Vec<RegionMeta> {
        self.regions.read().values().cloned().collect()
    }

    /// 扫描 start_key 范围内的 Region（最多 limit 个）
    pub fn scan_regions(&self, start_key: &[u8], limit: usize) -> Vec<RegionMeta> {
        let key_index = self.key_index.read();
        let region_ids: Vec<RegionId> = key_index
            .range(start_key.to_vec()..)
            .take(limit)
            .map(|(_, &rid)| rid)
            .collect();
        drop(key_index);

        let regions = self.regions.read();
        region_ids
            .into_iter()
            .filter_map(|rid| regions.get(&rid).cloned())
            .collect()
    }

    /// 获取 Region 总数
    pub fn region_count(&self) -> usize {
        self.regions.read().len()
    }

    /// 获取相邻的 Region（用于 Merge Checker）
    ///
    /// 返回按 start_key 排序的 Region 元数据列表。
    pub fn get_adjacent_pairs(&self) -> Vec<(RegionMeta, RegionMeta)> {
        let regions = self.regions.read();
        // 按 start_key 排序
        let mut by_key: Vec<&RegionMeta> = regions.values().collect();
        by_key.sort_by(|a, b| a.start_key.cmp(&b.start_key));

        let mut pairs = Vec::new();
        for window in by_key.windows(2) {
            if window[0].end_key == window[1].start_key {
                pairs.push((window[0].clone(), window[1].clone()));
            }
        }
        pairs
    }

    /// 分配新的 Region ID（单调递增）
    ///
    /// Phase 1：基于当前最大 Region ID + 1。
    /// Phase 3+：通过 Raft 共识分配。
    pub fn allocate_region_id(&self) -> RegionId {
        let regions = self.regions.read();
        regions
            .last_key_value()
            .map(|(&id, _)| id + 1)
            .unwrap_or(1)
    }
}

impl Default for PdMetaStore {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// 测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use coord_core::types::{Peer, PeerRole, RegionEpoch};

    fn make_meta(id: RegionId, start: Vec<u8>, end: Vec<u8>) -> RegionMeta {
        RegionMeta {
            region_id: id,
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
    fn test_create_and_get_region() {
        let store = PdMetaStore::new();
        store.create_region(make_meta(1, vec![0x00], vec![0x55])).unwrap();
        assert!(store.get_region(1).is_some());
        assert!(store.get_region(999).is_none());
    }

    #[test]
    fn test_create_duplicate_fails() {
        let store = PdMetaStore::new();
        store.create_region(make_meta(1, vec![0x00], vec![0x55])).unwrap();
        assert!(store.create_region(make_meta(1, vec![0x00], vec![0x55])).is_err());
    }

    #[test]
    fn test_get_region_by_key() {
        let store = PdMetaStore::new();
        store.create_region(make_meta(1, vec![0x00], vec![0x55])).unwrap();
        store.create_region(make_meta(2, vec![0x55], vec![0xFF])).unwrap();

        assert_eq!(store.get_region_by_key(&[0x00]).unwrap().region_id, 1);
        assert_eq!(store.get_region_by_key(&[0x54]).unwrap().region_id, 1);
        assert_eq!(store.get_region_by_key(&[0x55]).unwrap().region_id, 2);
        assert_eq!(store.get_region_by_key(&[0xFE]).unwrap().region_id, 2);
    }

    #[test]
    fn test_delete_region() {
        let store = PdMetaStore::new();
        store.create_region(make_meta(1, vec![0x00], vec![0x55])).unwrap();
        assert_eq!(store.region_count(), 1);

        store.delete_region(1).unwrap();
        assert_eq!(store.region_count(), 0);
        assert!(store.get_region(1).is_none());
    }

    #[test]
    fn test_update_region() {
        let store = PdMetaStore::new();
        store.create_region(make_meta(1, vec![0x00], vec![0x55])).unwrap();

        let mut updated = store.get_region(1).unwrap();
        updated.approximate_size = 1024;
        store.update_region(updated).unwrap();

        assert_eq!(store.get_region(1).unwrap().approximate_size, 1024);
    }

    #[test]
    fn test_update_region_start_key() {
        let store = PdMetaStore::new();
        store.create_region(make_meta(1, vec![0x00], vec![0xFF])).unwrap();

        // 分裂后缩小范围
        let mut updated = store.get_region(1).unwrap();
        updated.end_key = vec![0x55];
        store.update_region(updated).unwrap();

        // 添加新 Region
        store.create_region(make_meta(2, vec![0x55], vec![0xFF])).unwrap();

        // 路由应正确
        assert_eq!(store.get_region_by_key(&[0x00]).unwrap().region_id, 1);
        assert_eq!(store.get_region_by_key(&[0x55]).unwrap().region_id, 2);
    }

    #[test]
    fn test_allocate_region_id() {
        let store = PdMetaStore::new();
        assert_eq!(store.allocate_region_id(), 1);

        store.create_region(make_meta(1, vec![0x00], vec![0x55])).unwrap();
        assert_eq!(store.allocate_region_id(), 2);

        store.create_region(make_meta(5, vec![0x55], vec![0xFF])).unwrap();
        assert_eq!(store.allocate_region_id(), 6);
    }

    #[test]
    fn test_scan_regions() {
        let store = PdMetaStore::new();
        store.create_region(make_meta(1, vec![0x00], vec![0x40])).unwrap();
        store.create_region(make_meta(2, vec![0x40], vec![0x80])).unwrap();
        store.create_region(make_meta(3, vec![0x80], vec![0xFF])).unwrap();

        let regions = store.scan_regions(&[0x40], 10);
        assert_eq!(regions.len(), 2); // Region 2 + Region 3

        let regions = store.scan_regions(&[0x00], 1);
        assert_eq!(regions.len(), 1); // 仅 Region 1
    }

    #[test]
    fn test_adjacent_pairs() {
        let store = PdMetaStore::new();
        store.create_region(make_meta(1, vec![0x00], vec![0x40])).unwrap();
        store.create_region(make_meta(2, vec![0x40], vec![0x80])).unwrap();
        store.create_region(make_meta(3, vec![0x80], vec![0xFF])).unwrap();

        let pairs = store.get_adjacent_pairs();
        assert_eq!(pairs.len(), 2); // (1,2) and (2,3)
        assert_eq!(pairs[0].0.region_id, 1);
        assert_eq!(pairs[0].1.region_id, 2);
        assert_eq!(pairs[1].0.region_id, 2);
        assert_eq!(pairs[1].1.region_id, 3);
    }
}
