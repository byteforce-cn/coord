// PD Meta Store — Region 元数据持久化与内存索引
//
// PdMetaStore 负责：
// - 持久化 Region 元数据（redb 落盘，`/pd/region/{region_id:016x}` key 前缀；
//   重启时从磁盘完整恢复）
// - 维护内存索引（start_key → RegionId 二分查找）
// - 提供 Region 元数据的 CRUD 操作
//
// 设计要点（持久化落地）：
// - 持久化 Key: /pd/region/{region_id:016x}（coord_core::region::encode_pd_region_key，
//   与共享存储前缀规范一致——#6 存储决策：key 前缀编码保留给 PD 元数据）
// - 内存索引: BTreeMap<start_key, RegionId>（O(log N) 查找）
// - 并发安全：RwLock 保护
// - 持久化模式：`PdMetaStore::open(data_dir)` 打开/创建 `<data_dir>/pd/pd-meta.db`
//   （redb 独立文件，表 `pd_region`）；磁盘为真源——每次 create/update/delete 先
//   同步落盘（redb commit 即 fsync）再更新内存缓存，启动时从磁盘全量恢复。
//   `new()` 保持纯内存模式（测试 / 未启用持久化路径时使用）。
// - 例外：心跳统计更新走 `update_region_stats`（仅内存视图，不写穿落盘）。
//   Region 心跳的 size/keys 是派生瞬态数据（下一拍重新上报），写穿会在
//   control-plane 制造每拍 commit+fsync 写放大；磁盘为真源只约束**持久元数据**
//   （成员/epoch/key range）变更。重启后统计回落，由首拍心跳重新填充。
// - 对象存储字节维度（`update_region_storage_bytes`）同为派生瞬态内存视图：
//   coord.storage chunk 文件落 Region 数据目录 `objects/`（不进 redb
//   store.db），需第二个容量维度供 Split 阈值纳入与「存储重 Region 不参与
//   自动均衡」决策；不写穿落盘、重启回落，由首拍心跳重新填充。

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use coord_core::error::{Error, Result};
use coord_core::region::encode_pd_region_key;
use coord_core::types::{RegionId, RegionMeta};
use parking_lot::RwLock;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

// ──── Redb 表定义 ────

/// PD Region 元数据表：Key = `/pd/region/{region_id:016x}`，Value = bincode(RegionMeta)
const TABLE_PD_REGION: TableDefinition<&[u8], &[u8]> = TableDefinition::new("pd_region");

// ============================================================================
// PdMetaDurable（redb 持久化后端）
// ============================================================================

/// PD 元数据 redb 持久化句柄
///
/// 物理布局：`<data_dir>/pd/pd-meta.db`（目录自动创建）。
/// 表 `pd_region` 的 Key 遵循 coord_core::region 前缀规范 `/pd/region/{id:016x}`。
///
/// 单写者说明：每个 PdMetaStore 独占一个文件，redb 写事务内部串行化，
/// 调用方（PdMetaStore 变更方法）在各自 RwLock 临界区外调用本句柄，
/// 每次变更独立 commit（控制面频率，无批量压力）。
struct PdMetaDurable {
    db: Database,
    #[allow(dead_code)]
    db_path: PathBuf,
}

impl PdMetaDurable {
    /// 打开（或创建）PD 元数据 redb 数据库并确保表存在
    fn open(data_dir: &Path) -> Result<Self> {
        let pd_dir = data_dir.join("pd");
        std::fs::create_dir_all(&pd_dir)
            .map_err(|e| Error::Storage(format!("create pd meta dir {}: {e}", pd_dir.display())))?;
        let db_path = pd_dir.join("pd-meta.db");
        let db = if db_path.exists() {
            Database::open(&db_path).map_err(|e| {
                Error::Storage(format!("open pd meta db {}: {e}", db_path.display()))
            })?
        } else {
            Database::create(&db_path).map_err(|e| {
                Error::Storage(format!("create pd meta db {}: {e}", db_path.display()))
            })?
        };

        // 确保表已创建
        {
            let write_tx = db
                .begin_write()
                .map_err(|e| Error::Storage(format!("begin pd meta init tx: {e}")))?;
            {
                let _ = write_tx.open_table(TABLE_PD_REGION);
            }
            write_tx
                .commit()
                .map_err(|e| Error::Storage(format!("commit pd meta init tx: {e}")))?;
        }

        Ok(Self { db, db_path })
    }

    /// 写入/更新一个 Region 元数据（key = `/pd/region/{region_id:016x}`）
    fn put_region(&self, meta: &RegionMeta) -> Result<()> {
        let key = encode_pd_region_key(meta.region_id);
        let value = bincode::serialize(meta)
            .map_err(|e| Error::Internal(format!("serialize pd region meta: {e}")))?;

        let write_tx = self
            .db
            .begin_write()
            .map_err(|e| Error::Storage(format!("begin pd region write tx: {e}")))?;
        {
            let mut table = write_tx
                .open_table(TABLE_PD_REGION)
                .map_err(|e| Error::Storage(format!("open pd_region table: {e}")))?;
            table
                .insert(key.as_slice(), value.as_slice())
                .map_err(|e| Error::Storage(format!("insert pd region meta: {e}")))?;
        }
        write_tx
            .commit()
            .map_err(|e| Error::Storage(format!("commit pd region meta write: {e}")))?;
        Ok(())
    }

    /// 删除一个 Region 元数据
    fn remove_region(&self, region_id: RegionId) -> Result<()> {
        let key = encode_pd_region_key(region_id);

        let write_tx = self
            .db
            .begin_write()
            .map_err(|e| Error::Storage(format!("begin pd region delete tx: {e}")))?;
        {
            let mut table = write_tx
                .open_table(TABLE_PD_REGION)
                .map_err(|e| Error::Storage(format!("open pd_region table: {e}")))?;
            table
                .remove(key.as_slice())
                .map_err(|e| Error::Storage(format!("remove pd region meta: {e}")))?;
        }
        write_tx
            .commit()
            .map_err(|e| Error::Storage(format!("commit pd region meta delete: {e}")))?;
        Ok(())
    }

    /// 启动恢复：读取磁盘上全部 Region 元数据
    fn load_all_regions(&self) -> Result<Vec<RegionMeta>> {
        let read_tx = self
            .db
            .begin_read()
            .map_err(|e| Error::Storage(format!("begin pd region read tx: {e}")))?;
        let table = read_tx
            .open_table(TABLE_PD_REGION)
            .map_err(|e| Error::Storage(format!("open pd_region table: {e}")))?;

        let mut out = Vec::new();
        let iter = table
            .iter()
            .map_err(|e| Error::Storage(format!("scan pd_region table: {e}")))?;
        for entry in iter {
            let (_key, value) =
                entry.map_err(|e| Error::Storage(format!("read pd region row: {e}")))?;
            let meta: RegionMeta = bincode::deserialize(value.value())
                .map_err(|e| Error::DataCorruption(format!("deserialize pd region meta: {e}")))?;
            out.push(meta);
        }
        Ok(out)
    }
}

// ============================================================================
// PdMetaStore
// ============================================================================

/// PD 元数据存储
///
/// Region 元数据仓库，两种形态：
/// - `new()`：纯内存（测试 / 未启用持久化的调用方）；
/// - `open(data_dir)`：redb 持久化，磁盘为真源，变更同步落盘，
///   启动时从磁盘全量恢复（重启后 region 表完整）。
///
/// 多 Region 场景下 PD 元数据最终的一致性（raft 复制 / 由 region 0 system raft
/// 承载 PD 命令）是后续接线层的职责；本层保证单节点
/// 重启后本地 region 元数据不丢失。
pub struct PdMetaStore {
    /// Region 元数据：RegionId → RegionMeta
    regions: RwLock<BTreeMap<RegionId, RegionMeta>>,
    /// start_key → RegionId 有序索引（用于路由查找）
    key_index: RwLock<BTreeMap<Vec<u8>, RegionId>>,
    /// Region 心跳上报的对象存储字节（RegionId → bytes；内存视图，不落盘）
    region_storage_bytes: RwLock<HashMap<RegionId, u64>>,
    /// 持久化后端：None = 纯内存模式；Some = redb 文件后端（变更即落盘）
    durable: Option<PdMetaDurable>,
}

impl PdMetaStore {
    /// 创建纯内存 PdMetaStore（不落盘）
    pub fn new() -> Self {
        Self {
            regions: RwLock::new(BTreeMap::new()),
            key_index: RwLock::new(BTreeMap::new()),
            region_storage_bytes: RwLock::new(HashMap::new()),
            durable: None,
        }
    }

    /// 打开（或创建）持久化 PD 元数据存储
    ///
    /// 物理布局：`<data_dir>/pd/pd-meta.db`；落盘 key =
    /// `/pd/region/{region_id:016x}`（coord_core::region 前缀规范）。
    /// 打开时把磁盘上全部 region 元数据加载进内存（重启恢复），此后每次
    /// 变更（create/update/delete）先落盘再更新内存缓存。
    pub fn open(data_dir: &Path) -> Result<Self> {
        let durable = PdMetaDurable::open(data_dir)?;
        let loaded = durable.load_all_regions()?;

        let store = Self {
            regions: RwLock::new(BTreeMap::new()),
            key_index: RwLock::new(BTreeMap::new()),
            region_storage_bytes: RwLock::new(HashMap::new()),
            durable: Some(durable),
        };
        for meta in loaded {
            let region_id = meta.region_id;
            let start_key = meta.start_key.clone();
            {
                let mut regions = store.regions.write();
                regions.insert(region_id, meta);
            }
            {
                let mut key_index = store.key_index.write();
                key_index.insert(start_key, region_id);
            }
        }
        tracing::info!(
            "PD: loaded {} region(s) from durable store",
            store.region_count()
        );
        Ok(store)
    }

    /// 变更写穿：持久化模式下一次变更的落盘（成功后才允许更新内存）
    fn durable_put(&self, meta: &RegionMeta) -> Result<()> {
        match &self.durable {
            Some(d) => d.put_region(meta),
            None => Ok(()),
        }
    }

    /// 变更删除写穿
    fn durable_remove(&self, region_id: RegionId) -> Result<()> {
        match &self.durable {
            Some(d) => d.remove_region(region_id),
            None => Ok(()),
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

        // 先落盘再更新内存（磁盘为真源；持久化失败则整体失败，内存不变）
        self.durable_put(&meta)?;

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

        // 先落盘再更新内存（磁盘为真源；持久化失败则整体失败，内存不变）
        self.durable_put(&meta)?;

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

    /// 仅更新 Region 统计字段的内存视图
    ///
    /// Region 心跳携带的 `approximate_size` / `approximate_keys` 是**派生瞬态**
    /// 数据——下一拍心跳即重新上报，重启后由首拍心跳重新填充。因此心跳更新走
    /// 本方法（只改内存、不写穿落盘），避免高频心跳在 control-plane 上制造
    /// commit+fsync 写放大；`update_region`（成员/epoch/key range 等**持久元数据**
    /// 变更）保持写穿语义不变。统计字段含在持久化的 RegionMeta 中仅为
    /// create/update 时顺带快照，不作为心跳真源。
    pub fn update_region_stats(&self, region_id: RegionId, size: u64, keys: u64) -> Result<()> {
        let mut regions = self.regions.write();
        let meta = regions
            .get_mut(&region_id)
            .ok_or(Error::RegionNotFound { region_id })?;
        meta.approximate_size = size;
        meta.approximate_keys = keys;
        Ok(())
    }

    /// 更新 Region 的对象存储字节维度（内存视图，语义同 `update_region_stats`：
    /// 派生瞬态数据，不写穿落盘；重启后由首拍心跳重新填充）。
    ///
    /// coord.storage chunk 文件落该 Region 数据目录 `objects/`（不进 redb
    /// store.db），心跳单独承载该容量维度供 Split 阈值纳入与「存储重 Region
    /// 不参与自动均衡」决策使用。
    pub fn update_region_storage_bytes(&self, region_id: RegionId, bytes: u64) -> Result<()> {
        {
            let regions = self.regions.read();
            if !regions.contains_key(&region_id) {
                return Err(Error::RegionNotFound { region_id });
            }
        }
        self.region_storage_bytes.write().insert(region_id, bytes);
        Ok(())
    }

    /// 查询 Region 的对象存储字节（无上报返回 0）
    pub fn region_storage_bytes(&self, region_id: RegionId) -> u64 {
        self.region_storage_bytes
            .read()
            .get(&region_id)
            .copied()
            .unwrap_or(0)
    }

    /// 全量快照（RegionId → 对象存储字节；调度上下文构建用）
    pub fn all_region_storage_bytes(&self) -> HashMap<RegionId, u64> {
        self.region_storage_bytes.read().clone()
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

        // 先删除落盘 key，成功后再更新内存
        self.durable_remove(region_id)?;

        {
            let mut regions = self.regions.write();
            regions.remove(&region_id);
        }
        {
            let mut key_index = self.key_index.write();
            key_index.remove(&start_key);
        }
        self.region_storage_bytes.write().remove(&region_id);

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
    /// 当前实现：基于当前最大 Region ID + 1。
    /// 后续可经 Raft 共识分配。
    pub fn allocate_region_id(&self) -> RegionId {
        let regions = self.regions.read();
        regions.last_key_value().map(|(&id, _)| id + 1).unwrap_or(1)
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
    use redb::ReadableDatabase;

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
        store
            .create_region(make_meta(1, vec![0x00], vec![0x55]))
            .unwrap();
        assert!(store.get_region(1).is_some());
        assert!(store.get_region(999).is_none());
    }

    #[test]
    fn test_create_duplicate_fails() {
        let store = PdMetaStore::new();
        store
            .create_region(make_meta(1, vec![0x00], vec![0x55]))
            .unwrap();
        assert!(store
            .create_region(make_meta(1, vec![0x00], vec![0x55]))
            .is_err());
    }

    #[test]
    fn test_get_region_by_key() {
        let store = PdMetaStore::new();
        store
            .create_region(make_meta(1, vec![0x00], vec![0x55]))
            .unwrap();
        store
            .create_region(make_meta(2, vec![0x55], vec![0xFF]))
            .unwrap();

        assert_eq!(store.get_region_by_key(&[0x00]).unwrap().region_id, 1);
        assert_eq!(store.get_region_by_key(&[0x54]).unwrap().region_id, 1);
        assert_eq!(store.get_region_by_key(&[0x55]).unwrap().region_id, 2);
        assert_eq!(store.get_region_by_key(&[0xFE]).unwrap().region_id, 2);
    }

    #[test]
    fn test_delete_region() {
        let store = PdMetaStore::new();
        store
            .create_region(make_meta(1, vec![0x00], vec![0x55]))
            .unwrap();
        assert_eq!(store.region_count(), 1);

        store.delete_region(1).unwrap();
        assert_eq!(store.region_count(), 0);
        assert!(store.get_region(1).is_none());
    }

    #[test]
    fn test_update_region() {
        let store = PdMetaStore::new();
        store
            .create_region(make_meta(1, vec![0x00], vec![0x55]))
            .unwrap();

        let mut updated = store.get_region(1).unwrap();
        updated.approximate_size = 1024;
        store.update_region(updated).unwrap();

        assert_eq!(store.get_region(1).unwrap().approximate_size, 1024);
    }

    #[test]
    fn test_update_region_start_key() {
        let store = PdMetaStore::new();
        store
            .create_region(make_meta(1, vec![0x00], vec![0xFF]))
            .unwrap();

        // 分裂后缩小范围
        let mut updated = store.get_region(1).unwrap();
        updated.end_key = vec![0x55];
        store.update_region(updated).unwrap();

        // 添加新 Region
        store
            .create_region(make_meta(2, vec![0x55], vec![0xFF]))
            .unwrap();

        // 路由应正确
        assert_eq!(store.get_region_by_key(&[0x00]).unwrap().region_id, 1);
        assert_eq!(store.get_region_by_key(&[0x55]).unwrap().region_id, 2);
    }

    #[test]
    fn test_allocate_region_id() {
        let store = PdMetaStore::new();
        assert_eq!(store.allocate_region_id(), 1);

        store
            .create_region(make_meta(1, vec![0x00], vec![0x55]))
            .unwrap();
        assert_eq!(store.allocate_region_id(), 2);

        store
            .create_region(make_meta(5, vec![0x55], vec![0xFF]))
            .unwrap();
        assert_eq!(store.allocate_region_id(), 6);
    }

    #[test]
    fn test_scan_regions() {
        let store = PdMetaStore::new();
        store
            .create_region(make_meta(1, vec![0x00], vec![0x40]))
            .unwrap();
        store
            .create_region(make_meta(2, vec![0x40], vec![0x80]))
            .unwrap();
        store
            .create_region(make_meta(3, vec![0x80], vec![0xFF]))
            .unwrap();

        let regions = store.scan_regions(&[0x40], 10);
        assert_eq!(regions.len(), 2); // Region 2 + Region 3

        let regions = store.scan_regions(&[0x00], 1);
        assert_eq!(regions.len(), 1); // 仅 Region 1
    }

    #[test]
    fn test_adjacent_pairs() {
        let store = PdMetaStore::new();
        store
            .create_region(make_meta(1, vec![0x00], vec![0x40]))
            .unwrap();
        store
            .create_region(make_meta(2, vec![0x40], vec![0x80]))
            .unwrap();
        store
            .create_region(make_meta(3, vec![0x80], vec![0xFF]))
            .unwrap();

        let pairs = store.get_adjacent_pairs();
        assert_eq!(pairs.len(), 2); // (1,2) and (2,3)
        assert_eq!(pairs[0].0.region_id, 1);
        assert_eq!(pairs[0].1.region_id, 2);
        assert_eq!(pairs[1].0.region_id, 2);
        assert_eq!(pairs[1].1.region_id, 3);
    }

    // ──── 持久化测试（redb 落盘 + 重启恢复）────

    #[test]
    fn test_memory_mode_creates_no_durable_file() {
        // 纯内存模式（new()）不产生任何磁盘文件
        let dir = tempfile::tempdir().unwrap();
        let store = PdMetaStore::new();
        store.create_region(make_meta(1, vec![], vec![])).unwrap();
        assert!(!dir.path().join("pd/pd-meta.db").exists());
    }

    #[test]
    fn test_open_persists_and_recovers_full_meta_after_restart() {
        let dir = tempfile::tempdir().unwrap();

        // 第一次"运行"：创建持久化 store，写入 2 个 Region（含完整字段）
        {
            let store = PdMetaStore::open(dir.path()).unwrap();
            let mut r1 = make_meta(1, vec![], vec![0x55]);
            r1.peers = vec![
                Peer {
                    node_id: 1,
                    raft_addr: "n1:50052".into(),
                    role: PeerRole::Voter,
                },
                Peer {
                    node_id: 2,
                    raft_addr: "n2:50052".into(),
                    role: PeerRole::Voter,
                },
                Peer {
                    node_id: 3,
                    raft_addr: "n3:50052".into(),
                    role: PeerRole::Learner,
                },
            ];
            r1.epoch = RegionEpoch {
                conf_ver: 3,
                version: 2,
            };
            r1.approximate_size = 4096;
            r1.approximate_keys = 123;
            store.create_region(r1).unwrap();
            store
                .create_region(make_meta(2, vec![0x55], vec![]))
                .unwrap();
            // 落盘文件应已创建
            assert!(dir.path().join("pd/pd-meta.db").exists());
        } // drop → 模拟进程结束

        // 第二次"运行"（重启恢复）：open 重新加载磁盘上的 region 元数据
        {
            let store = PdMetaStore::open(dir.path()).unwrap();
            assert_eq!(store.region_count(), 2);

            let r1 = store.get_region(1).expect("region 1 recovered");
            assert_eq!(r1.start_key, Vec::<u8>::new());
            assert_eq!(r1.end_key, vec![0x55]);
            assert_eq!(r1.peers.len(), 3);
            assert_eq!(r1.peers[2].role, PeerRole::Learner);
            assert_eq!(r1.epoch.conf_ver, 3);
            assert_eq!(r1.epoch.version, 2);
            assert_eq!(r1.approximate_size, 4096);
            assert_eq!(r1.approximate_keys, 123);

            // key 路由索引也应随恢复重建（region1=[∅,0x55)、region2=[0x55,∅)）
            assert_eq!(store.get_region_by_key(&[0x00]).unwrap().region_id, 1);
            assert_eq!(store.get_region_by_key(&[0x54]).unwrap().region_id, 1);
            assert_eq!(store.get_region_by_key(&[0x55]).unwrap().region_id, 2);
            assert_eq!(store.get_region_by_key(&[0xAA]).unwrap().region_id, 2);
        }
    }

    #[test]
    fn test_persisted_delete_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = PdMetaStore::open(dir.path()).unwrap();
            store
                .create_region(make_meta(1, vec![], vec![0x55]))
                .unwrap();
            store
                .create_region(make_meta(2, vec![0x55], vec![]))
                .unwrap();
            store.delete_region(1).unwrap();
        }
        let store = PdMetaStore::open(dir.path()).unwrap();
        assert_eq!(store.region_count(), 1);
        assert!(store.get_region(1).is_none());
        assert!(store.get_region(2).is_some());
    }

    #[test]
    fn test_persisted_update_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = PdMetaStore::open(dir.path()).unwrap();
            store
                .create_region(make_meta(1, vec![], vec![0xFF]))
                .unwrap();
            // 分裂：缩小 region1 range 并新增 region2（start_key 索引随 update 迁移）
            let mut r1 = store.get_region(1).unwrap();
            r1.end_key = vec![0x55];
            store.update_region(r1).unwrap();
            store
                .create_region(make_meta(2, vec![0x55], vec![0xFF]))
                .unwrap();
        }
        let store = PdMetaStore::open(dir.path()).unwrap();
        assert_eq!(store.region_count(), 2);
        assert_eq!(store.get_region_by_key(&[0x00]).unwrap().region_id, 1);
        assert_eq!(store.get_region_by_key(&[0x55]).unwrap().region_id, 2);
        assert_eq!(store.get_region_by_key(&[0x77]).unwrap().region_id, 2);
    }

    // ──── 心跳统计：内存瞬态更新，不写穿落盘 ────

    #[test]
    fn test_update_region_stats_memory_only_not_durable() {
        // Region 心跳的 size/keys 是派生瞬态数据（下一拍即重新上报），
        // 只更新内存视图、不写穿落盘（避免每拍心跳一次 commit+fsync 写放大）。
        // 重启后统计回落到持久化变更写入的值，由首拍心跳重新填充。
        let dir = tempfile::tempdir().unwrap();
        {
            let store = PdMetaStore::open(dir.path()).unwrap();
            store.create_region(make_meta(1, vec![], vec![])).unwrap();

            // 心跳路径（模拟 handle_region_heartbeat）：更新统计
            store.update_region_stats(1, 4096, 500).unwrap();
            assert_eq!(store.get_region(1).unwrap().approximate_size, 4096);
            assert_eq!(store.get_region(1).unwrap().approximate_keys, 500);
        }

        // 重启恢复：region 元数据仍在（成员/range），但统计回落到 create 时的持久值
        let store = PdMetaStore::open(dir.path()).unwrap();
        assert_eq!(store.region_count(), 1);
        let r = store.get_region(1).unwrap();
        assert_eq!(r.approximate_size, 0, "heartbeat stats must not persist");
        assert_eq!(r.approximate_keys, 0, "heartbeat stats must not persist");
    }

    #[test]
    fn test_update_region_stats_not_found() {
        let store = PdMetaStore::new();
        let err = store.update_region_stats(99, 1, 1).unwrap_err();
        assert!(matches!(err, Error::RegionNotFound { region_id: 99 }));
    }

    #[test]
    fn test_persisted_key_uses_pd_region_prefix() {
        // 白盒：落盘 key 遵循 coord_core::region 前缀规范
        // `/pd/region/{region_id:016x}`
        let dir = tempfile::tempdir().unwrap();
        {
            let store = PdMetaStore::open(dir.path()).unwrap();
            store
                .create_region(make_meta(0xABC, vec![], vec![]))
                .unwrap();
        }

        // 用裸 redb 句柄校验磁盘上的 key
        let db = redb::Database::open(dir.path().join("pd/pd-meta.db")).unwrap();
        let read_tx = db.begin_read().unwrap();
        let table = read_tx.open_table(super::TABLE_PD_REGION).unwrap();
        let keys: Vec<Vec<u8>> = table
            .iter()
            .unwrap()
            .map(|entry| entry.unwrap().0.value().to_vec())
            .collect();
        assert_eq!(keys, vec![coord_core::region::encode_pd_region_key(0xABC)]);
    }
}
