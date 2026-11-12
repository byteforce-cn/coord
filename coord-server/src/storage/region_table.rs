// region 0 权威 Region 表（ADR-0015 D6 / issue #159）。
//
// 表是**单键整表**（`/_sys/pd/regions`）：Region 元数据总量小（每 split/merge
// 一条），覆盖写比逐条增量更容易保证“区间平铺 + 无重叠/空洞”这一表级不变量，
// 也让一次 raft 条目原子生效（split = 旧区缩界 + 新区插入）。
//
// 不变量由 `validate` 单点定义：
//   - 按 `start_key` 升序；
//   - 首区 `start_key` 为空，末区 `end_key` 为空（无上界）；
//   - 相邻区 `end_key == 下一区 start_key`（半开区间，平铺且无空洞）；
//   - `region_id` 唯一且 > 0，每区 `start < end`。
//
// 本模块只做纯计算/校验，不做 IO、不读时钟；持久化与 apply 幂等由
// `MvccStorage::apply_region_meta_op` 负责。

use serde::{Deserialize, Serialize};

use coord_core::error::{Error, Result};
use coord_core::types::{RegionEpoch, RegionId, RegionMeta};

use crate::storage::object_store::{key_in_object_space, OBJECT_PREFIX, OBJECT_SPACE_END};

/// region 0 存整张表的单键。
pub const REGION_TABLE_KEY: &[u8] = b"/_sys/pd/regions";

/// MQ v2 的固定保留空间（`[/_mq/, /_mq0)`；ADR-0016 D2）。
///
/// `/` 的下一个字节是 `0`，因此 `/_mq0` 是 `/` 前缀区间的最小上界——与
/// `[/obj/, /obj0)` 同一手法。日志分片与消费组分片都住在这块空间里，**不参与**
/// 任意 KV split/merge，也不由用户写入。
pub const MQ_PREFIX: &[u8] = b"/_mq/";
/// MQ 保留空间的上界（不含）。
pub const MQ_SPACE_END: &[u8] = b"/_mq0";
/// 日志分片的前缀（`/_mq/log/<shard>/`）。
pub const MQ_LOG_PREFIX: &[u8] = b"/_mq/log/";
/// 消费组分片的前缀（`/_mq/group/<shard>/`）。
pub const MQ_GROUP_PREFIX: &[u8] = b"/_mq/group/";
/// 日志分片区间上界前缀（`/_mq/log0`）。
const MQ_LOG_SPACE_END: &[u8] = b"/_mq/log0";
/// 消费组分片区间上界前缀（`/_mq/group0`）。
const MQ_GROUP_SPACE_END: &[u8] = b"/_mq/group0";

/// 分片 id 的定宽（`00`..`63`）：区间按 `start_key` 字典序平铺，宽度可变会
/// 让 `/_mq/log/10/` 排在 `/_mq/log/2/` 前面而破坏相邻性。
const MQ_SHARD_WIDTH: usize = 2;

/// 一次 Region 表变更（由 `RegionMetaOp::Apply` 成批承载）。
/// Split/Merge 计划的方向（journal 与执行器共用）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SplitMergeKind {
    /// 源 Region 缩界，新 Region 接管右半区间。
    Split,
    /// 左 Region 吸收右 Region 的区间，右 Region 删除。
    Merge,
}

/// 一次 Split/Merge 的表级计划（纯计算，可序列化进 intent journal）。
///
/// `expected_table_version` 是计算计划时的表版本：cutover 只在表仍停在该版本时
/// 整批生效，避免把基于过期视图算出的边界写进权威表。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitMergePlan {
    pub kind: SplitMergeKind,
    /// Split = 被缩界的父 Region；Merge = 吸收方（左）Region。
    pub source_region: RegionId,
    /// Split = 新 Region；Merge = None。
    pub target_region: Option<RegionId>,
    /// 与 `apply` 直接兼容的原子变更批。
    pub changes: Vec<RegionChange>,
    pub expected_table_version: u64,
}

/// 计算 split 计划：父区间收缩到 `split_key`，新 Region 接管右半段。
///
/// 拒绝空侧、表外 key、`/obj/` 内 key 与 id 复用——任何一条都意味着调用方的
/// 视图与权威表不一致，不能靠执行器「修一修」继续。
pub fn plan_split(
    table: &RegionTable,
    region_id: RegionId,
    split_key: &[u8],
    new_region_id: RegionId,
) -> Result<SplitMergePlan> {
    if table.version == 0 {
        return Err(Error::InvalidArgument(
            "region table is not initialized".to_string(),
        ));
    }
    let parent = table
        .get(region_id)
        .cloned()
        .ok_or_else(|| Error::InvalidArgument(format!("region {region_id} is not in the table")))?;
    if new_region_id == 0 || new_region_id == region_id || table.get(new_region_id).is_some() {
        return Err(Error::InvalidArgument(format!(
            "new region id {new_region_id} is invalid or already in use"
        )));
    }
    if split_key <= parent.start_key.as_slice() {
        return Err(Error::InvalidArgument(format!(
            "split key {split_key:?} must be strictly inside region {region_id}"
        )));
    }
    if !parent.end_key.is_empty() && split_key >= parent.end_key.as_slice() {
        return Err(Error::InvalidArgument(format!(
            "split key {split_key:?} must be strictly inside region {region_id}"
        )));
    }
    if key_in_object_space(split_key) {
        return Err(Error::InvalidArgument(
            "split key is inside the reserved /obj/ namespace".to_string(),
        ));
    }
    if key_in_mq_space(split_key) {
        return Err(Error::InvalidArgument(
            "split key is inside the reserved /_mq/ namespace".to_string(),
        ));
    }

    let mut shrunk = parent.clone();
    shrunk.end_key = split_key.to_vec();
    shrunk.epoch.version += 1;
    let child = RegionMeta {
        region_id: new_region_id,
        start_key: split_key.to_vec(),
        end_key: parent.end_key.clone(),
        epoch: RegionEpoch {
            conf_ver: parent.epoch.conf_ver,
            version: parent.epoch.version + 1,
        },
        peers: parent.peers.clone(),
        approximate_size: 0,
        approximate_keys: 0,
    };
    Ok(SplitMergePlan {
        kind: SplitMergeKind::Split,
        source_region: region_id,
        target_region: Some(new_region_id),
        changes: vec![
            RegionChange::Update { meta: shrunk },
            RegionChange::Add { meta: child },
        ],
        expected_table_version: table.version,
    })
}

/// 计算 merge 计划：左 Region 吸收右 Region 的区间，右 Region 删除。
///
/// 只接受真正相邻的半开区间；并集覆盖 `/obj/` 固定区间时拒绝（对象 Region 不得
/// 被任意 KV merge 消化掉）。
pub fn plan_merge(table: &RegionTable, left: RegionId, right: RegionId) -> Result<SplitMergePlan> {
    if table.version == 0 {
        return Err(Error::InvalidArgument(
            "region table is not initialized".to_string(),
        ));
    }
    if left == right {
        return Err(Error::InvalidArgument(
            "cannot merge a region with itself".to_string(),
        ));
    }
    let left_meta = table
        .get(left)
        .cloned()
        .ok_or_else(|| Error::InvalidArgument(format!("region {left} is not in the table")))?;
    let right_meta = table
        .get(right)
        .cloned()
        .ok_or_else(|| Error::InvalidArgument(format!("region {right} is not in the table")))?;
    if left_meta.end_key.is_empty() || left_meta.end_key != right_meta.start_key {
        return Err(Error::InvalidArgument(format!(
            "regions {left} and {right} are not adjacent"
        )));
    }
    if is_object_region(&left_meta) || is_object_region(&right_meta) {
        return Err(Error::InvalidArgument(
            "merge would absorb the fixed /obj/ region".to_string(),
        ));
    }
    if is_mq_region(&left_meta) || is_mq_region(&right_meta) {
        return Err(Error::InvalidArgument(
            "merge would absorb a fixed /_mq/ shard region".to_string(),
        ));
    }

    let mut merged = left_meta;
    merged.end_key = right_meta.end_key.clone();
    // 吸收更高 version 的右 Region 时，合并结果必须高于两者（否则 EPOCH_STALE
    // 判据会在后续写入路径看到回退的版本）。
    merged.epoch.version = merged.epoch.version.max(right_meta.epoch.version) + 1;
    Ok(SplitMergePlan {
        kind: SplitMergeKind::Merge,
        source_region: left,
        target_region: None,
        changes: vec![
            RegionChange::Update { meta: merged },
            RegionChange::Remove {
                region_id: right,
                expected_epoch: right_meta.epoch,
            },
        ],
        expected_table_version: table.version,
    })
}

/// 是否是对象存储的固定 Region 区间（`[/obj/, /obj0)`）。
///
/// 调度器用它跳过对象 Region，`RegionTable::has_object_region` 用它做启用对象存储
/// 时的启动校验。只保护「已按 ADR 固定出来的对象 Region」：还没有对象区间时，普通
/// KV 区间跨过 `/obj/` 是合法形态（对象存储未启用），不能被 merge 守卫无故锁死。
pub fn is_object_region(meta: &RegionMeta) -> bool {
    meta.start_key == OBJECT_PREFIX && meta.end_key == OBJECT_SPACE_END
}

/// 把 `[/obj/, /obj0)` 从启动种子区间里切成独立固定 Region。
///
/// 对象 manifest 与 chunk 不参与任意 KV split/merge，而运行时与 PD 都以 Region
/// 区间为单位工作——保留空间必须在拓扑里自带边界（ADR-0015 D6/D11.9）。纯计算：
/// `first_new_id` 由调用方给（种子最大 id + 1，所有节点算出的值相同）；父 Region 收缩
/// 为左段并保留自身 id/epoch/peers（父本身就是保留区间的左端点时不留左段，父 id
/// 随之退役），右段拿 `first_new_id + 1`，空段不插入。已有固定对象 Region 时幂等
/// 返回它。
pub fn carve_object_region(
    regions: &mut Vec<RegionMeta>,
    first_new_id: RegionId,
) -> Result<RegionId> {
    if first_new_id == 0 {
        return Err(Error::InvalidArgument(
            "region id 0 is reserved for the system raft".to_string(),
        ));
    }
    if let Some(existing) = regions.iter().find(|m| is_object_region(m)) {
        return Ok(existing.region_id);
    }
    if regions.iter().any(|m| m.region_id >= first_new_id) {
        return Err(Error::InvalidArgument(format!(
            "object region id {first_new_id} must be greater than every seeded region id"
        )));
    }
    let parent_index = regions
        .iter()
        .position(|m| {
            m.start_key.as_slice() <= OBJECT_PREFIX
                && (m.end_key.is_empty() || m.end_key.as_slice() >= OBJECT_SPACE_END)
        })
        .ok_or_else(|| {
            Error::InvalidArgument(format!(
                "region seeds do not cover the fixed object interval [{}, {})",
                String::from_utf8_lossy(OBJECT_PREFIX),
                String::from_utf8_lossy(OBJECT_SPACE_END)
            ))
        })?;

    let parent = regions[parent_index].clone();
    let mut carved = Vec::with_capacity(regions.len() + 2);
    for (index, meta) in regions.iter().enumerate() {
        if index != parent_index {
            carved.push(meta.clone());
            continue;
        }
        if parent.start_key.as_slice() < OBJECT_PREFIX {
            let mut left = meta.clone();
            left.end_key = OBJECT_PREFIX.to_vec();
            carved.push(left);
        }
        carved.push(RegionMeta {
            region_id: first_new_id,
            start_key: OBJECT_PREFIX.to_vec(),
            end_key: OBJECT_SPACE_END.to_vec(),
            epoch: parent.epoch,
            peers: parent.peers.clone(),
            approximate_size: 0,
            approximate_keys: 0,
        });
        if parent.end_key.as_slice() != OBJECT_SPACE_END {
            carved.push(RegionMeta {
                region_id: first_new_id + 1,
                start_key: OBJECT_SPACE_END.to_vec(),
                end_key: parent.end_key.clone(),
                epoch: parent.epoch,
                peers: parent.peers.clone(),
                approximate_size: 0,
                approximate_keys: 0,
            });
        }
    }
    carved.sort_by(|a, b| a.start_key.cmp(&b.start_key));
    *regions = carved;
    Ok(first_new_id)
}

/// 日志分片的前缀（`/_mq/log/<shard>/`，分片 id 定宽两位）。
pub fn mq_log_shard_prefix(shard: u32) -> Vec<u8> {
    format!("/_mq/log/{shard:0width$}/", width = MQ_SHARD_WIDTH).into_bytes()
}

/// 消费组分片的前缀（`/_mq/group/<shard>/`）。
pub fn mq_group_shard_prefix(shard: u32) -> Vec<u8> {
    format!("/_mq/group/{shard:0width$}/", width = MQ_SHARD_WIDTH).into_bytes()
}

/// 日志分片 Region 的区间 `[start, end)`（`total` = `[mq_v2].log_shards`）。
///
/// 区间按分片 id 升序**首尾相接**，末端到 `/_mq/log0`：Region 表要求平铺无空洞，
/// 因此不能用"前缀 + `0`"式的独立区间（中间会留下无人认领的键）。
pub fn mq_log_shard_range(shard: u32, total: u32) -> (Vec<u8>, Vec<u8>) {
    let start = mq_log_shard_prefix(shard);
    let end = if shard + 1 < total {
        mq_log_shard_prefix(shard + 1)
    } else {
        MQ_LOG_SPACE_END.to_vec()
    };
    (start, end)
}

/// 消费组分片 Region 的区间 `[start, end)`（`total` = `[mq_v2].group_shards`）。
pub fn mq_group_shard_range(shard: u32, total: u32) -> (Vec<u8>, Vec<u8>) {
    let start = mq_group_shard_prefix(shard);
    let end = if shard + 1 < total {
        mq_group_shard_prefix(shard + 1)
    } else {
        MQ_GROUP_SPACE_END.to_vec()
    };
    (start, end)
}

/// 是否是 MQ v2 的固定分片 Region。
///
/// 判据是分片前缀 `/_mq/log/<id>/` / `/_mq/group/<id>/`，不是整个 `/_mq/`：
/// 切分后保留空间两端的残余区间（如 `[/_mq/log0, …)`）起点也带 `/_mq`，但它们是
/// 普通 KV 区间，必须仍可被调度（否则表里会留下无法 split/merge 的死区）。
pub fn is_mq_region(meta: &RegionMeta) -> bool {
    meta.start_key.starts_with(MQ_LOG_PREFIX) || meta.start_key.starts_with(MQ_GROUP_PREFIX)
}

/// key 是否落在 MQ v2 保留空间内（`/_mq/...`）。
pub fn key_in_mq_space(key: &[u8]) -> bool {
    key.starts_with(MQ_PREFIX)
}

/// 把 MQ v2 的日志/消费组分片区间从启动种子区间里切出来（ADR-0016 D2）。
///
/// 纯计算：`first_new_id` 由调用方给（种子最大 id + 1，所有节点算出的值相同）。
/// 返回 `(log_shard_ids, group_shard_ids)`，按分片 id 升序。全部区间已存在时幂等
/// 返回现有 id；**部分存在** ⇒ 配置与拓扑不一致（fail-closed，不做静默补齐——
/// 补齐会在旧分片上留下另一半数据）。
pub fn carve_mq_regions(
    regions: &mut Vec<RegionMeta>,
    first_new_id: RegionId,
    log_shards: u32,
    group_shards: u32,
) -> Result<(Vec<RegionId>, Vec<RegionId>)> {
    if first_new_id == 0 {
        return Err(Error::InvalidArgument(
            "region id 0 is reserved for the system raft".to_string(),
        ));
    }
    if !(1..=64).contains(&log_shards) || !(1..=32).contains(&group_shards) {
        return Err(Error::InvalidArgument(format!(
            "MQ shard counts out of range: log_shards={log_shards} (1..=64), \
             group_shards={group_shards} (1..=32)"
        )));
    }
    let mut next_id = first_new_id;
    let log_ranges = (0..log_shards)
        .map(|shard| mq_log_shard_range(shard, log_shards))
        .collect();
    let log = carve_fixed_ranges(
        regions,
        &mut next_id,
        MQ_LOG_PREFIX,
        MQ_LOG_SPACE_END,
        "log",
        log_ranges,
    )?;
    let group_ranges = (0..group_shards)
        .map(|shard| mq_group_shard_range(shard, group_shards))
        .collect();
    let group = carve_fixed_ranges(
        regions,
        &mut next_id,
        MQ_GROUP_PREFIX,
        MQ_GROUP_SPACE_END,
        "group",
        group_ranges,
    )?;
    Ok((log, group))
}

/// 把一个保留空间切成若干首尾相接的固定 Region。
///
/// 语义与 [`carve_object_region`] 一致：父区间离开原地，左/右残余各自保留原 id
/// 与 epoch；分片 Region 拿 `next_id` 递增。父区间正好等于保留空间时不留残余。
fn carve_fixed_ranges(
    regions: &mut Vec<RegionMeta>,
    next_id: &mut RegionId,
    space_start: &[u8],
    space_end: &[u8],
    label: &str,
    ranges: Vec<(Vec<u8>, Vec<u8>)>,
) -> Result<Vec<RegionId>> {
    let mut claimed = Vec::new();
    for (start, end) in &ranges {
        if let Some(meta) = regions
            .iter()
            .find(|m| &m.start_key == start && &m.end_key == end)
        {
            claimed.push(meta.region_id);
        }
    }
    if claimed.len() == ranges.len() {
        return Ok(claimed);
    }
    if !claimed.is_empty() {
        return Err(Error::InvalidArgument(format!(
            "MQ {label} shard regions are only partially seeded ({} of {}); \
             [mq_v2] shard counts must not change on an initialized cluster",
            claimed.len(),
            ranges.len()
        )));
    }

    let parent_index = regions
        .iter()
        .position(|m| {
            m.start_key.as_slice() <= space_start
                && (m.end_key.is_empty() || m.end_key.as_slice() >= space_end)
        })
        .ok_or_else(|| {
            Error::InvalidArgument(format!(
                "region seeds do not cover the fixed MQ {label} interval [{}, {})",
                String::from_utf8_lossy(space_start),
                String::from_utf8_lossy(space_end)
            ))
        })?;
    let parent = regions[parent_index].clone();
    // 残余区间必须与分片区间**首尾相接**：`[/_mq/log/, /_mq/log/00/)` 这类缝隙
    // 若留给分片外的空白会破坏 Region 表的平铺不变量，所以左残余收在第一个分片
    // 的起点、右残余从最后一个分片的终点开始。
    let first_start = ranges[0].0.clone();
    let last_end = ranges[ranges.len() - 1].1.clone();

    let mut carved = Vec::with_capacity(regions.len() + ranges.len() + 2);
    for (index, meta) in regions.iter().enumerate() {
        if index != parent_index {
            carved.push(meta.clone());
            continue;
        }
        if parent.start_key.as_slice() < first_start.as_slice() {
            let mut left = meta.clone();
            left.end_key = first_start.clone();
            carved.push(left);
        }
        for (start, end) in &ranges {
            carved.push(RegionMeta {
                region_id: *next_id,
                start_key: start.clone(),
                end_key: end.clone(),
                epoch: parent.epoch,
                peers: parent.peers.clone(),
                approximate_size: 0,
                approximate_keys: 0,
            });
            *next_id += 1;
        }
        if parent.end_key.is_empty() || parent.end_key.as_slice() > last_end.as_slice() {
            carved.push(RegionMeta {
                region_id: *next_id,
                start_key: last_end.clone(),
                end_key: parent.end_key.clone(),
                epoch: parent.epoch,
                peers: parent.peers.clone(),
                approximate_size: 0,
                approximate_keys: 0,
            });
            *next_id += 1;
        }
    }
    carved.sort_by(|a, b| a.start_key.cmp(&b.start_key));
    *regions = carved;
    Ok(ranges
        .iter()
        .map(|(start, _)| {
            regions
                .iter()
                .find(|m| &m.start_key == start)
                .expect("carved shard region must exist")
                .region_id
        })
        .collect())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RegionChange {
    /// 插入一个新区间；同 id 且字段完全一致时幂等 no-op。
    Add { meta: RegionMeta },
    /// 替换 range/epoch/peers；epoch 必须分量不减且对应维度严格增大。
    Update { meta: RegionMeta },
    /// 删除一个区间；`expected_epoch` 必须等于当前 epoch（CAS）。
    Remove {
        region_id: RegionId,
        expected_epoch: RegionEpoch,
    },
}

/// region 0 的权威 Region 表。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegionTable {
    /// 表版本：每次真正改变表 +1；0 = 尚未初始化。
    pub version: u64,
    /// 按 `start_key` 升序、平铺整个 keyspace。
    pub regions: Vec<RegionMeta>,
}

impl RegionTable {
    pub fn encode(&self) -> Result<Vec<u8>> {
        postcard::to_allocvec(self)
            .map_err(|err| Error::Storage(format!("encode region table: {err}")))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let (table, remainder) = postcard::take_from_bytes::<Self>(bytes)
            .map_err(|err| Error::Storage(format!("decode region table: {err}")))?;
        if !remainder.is_empty() {
            return Err(Error::Storage(format!(
                "region table has {} trailing bytes",
                remainder.len()
            )));
        }
        table.validate()?;
        Ok(table)
    }

    /// 表里是否已有固定的 `/obj/` Region（对象存储启用时的启动校验）。
    pub fn has_object_region(&self) -> bool {
        self.regions.iter().any(is_object_region)
    }

    pub fn get(&self, region_id: RegionId) -> Option<&RegionMeta> {
        self.regions.iter().find(|meta| meta.region_id == region_id)
    }

    /// `key` 所属 Region（表已平铺；空表返回 None）。
    pub fn route(&self, key: &[u8]) -> Option<&RegionMeta> {
        let index = self
            .regions
            .partition_point(|meta| meta.start_key.as_slice() <= key);
        let meta = self.regions.get(index.checked_sub(1)?)?;
        meta.contains_key(key).then_some(meta)
    }

    /// 初始化空表：只允许 `version == 0`。已有表 → `Ok(false)`（重启/重复种子
    /// 的幂等路径，绝不覆盖 split/merge 后的现状）。
    pub fn init(&mut self, mut regions: Vec<RegionMeta>) -> Result<bool> {
        if self.version != 0 {
            return Ok(false);
        }
        regions.sort_by(|a, b| a.start_key.cmp(&b.start_key));
        let next = Self {
            version: 1,
            regions,
        };
        next.validate()?;
        *self = next;
        Ok(true)
    }

    /// 原子应用一批变更。任一变更非法 → 整批拒绝且 `self` 不变。
    pub fn apply(&mut self, changes: &[RegionChange]) -> Result<bool> {
        if changes.is_empty() {
            return Ok(false);
        }
        let mut next = self.clone();
        for change in changes {
            next.apply_one(change)?;
        }
        next.regions.sort_by(|a, b| a.start_key.cmp(&b.start_key));
        next.validate()?;
        next.version = self.version + 1;
        *self = next;
        Ok(true)
    }

    fn apply_one(&mut self, change: &RegionChange) -> Result<()> {
        match change {
            RegionChange::Add { meta } => {
                if let Some(existing) = self.get(meta.region_id) {
                    if existing == meta {
                        return Ok(());
                    }
                    return Err(Error::InvalidArgument(format!(
                        "region {} already exists with different metadata",
                        meta.region_id
                    )));
                }
                if meta.start_key.as_slice() >= meta.end_key.as_slice() && !meta.end_key.is_empty()
                {
                    return Err(Error::InvalidArgument(format!(
                        "region {} has start_key >= end_key",
                        meta.region_id
                    )));
                }
                self.regions.push(meta.clone());
                Ok(())
            }
            RegionChange::Update { meta } => {
                let index = self
                    .regions
                    .iter()
                    .position(|existing| existing.region_id == meta.region_id)
                    .ok_or_else(|| {
                        Error::InvalidArgument(format!(
                            "region {} does not exist; cannot update",
                            meta.region_id
                        ))
                    })?;
                check_update_epoch(&self.regions[index], meta)?;
                self.regions[index] = meta.clone();
                Ok(())
            }
            RegionChange::Remove {
                region_id,
                expected_epoch,
            } => {
                let index = self
                    .regions
                    .iter()
                    .position(|existing| existing.region_id == *region_id)
                    .ok_or_else(|| {
                        Error::InvalidArgument(format!(
                            "region {region_id} does not exist; cannot remove"
                        ))
                    })?;
                let current = self.regions[index].epoch;
                if current != *expected_epoch {
                    return Err(Error::InvalidArgument(format!(
                        "region {region_id} epoch {:?} is stale (expected {:?})",
                        current, expected_epoch
                    )));
                }
                self.regions.remove(index);
                Ok(())
            }
        }
    }

    /// 表级不变量（见模块头）。
    pub fn validate(&self) -> Result<()> {
        if self.version == 0 {
            if !self.regions.is_empty() {
                return Err(Error::Storage(
                    "region table with version 0 must be empty".to_string(),
                ));
            }
            return Ok(());
        }
        let Some(first) = self.regions.first() else {
            return Err(Error::Storage(
                "initialized region table must contain at least one region".to_string(),
            ));
        };
        if !first.start_key.is_empty() {
            return Err(Error::Storage(format!(
                "region table must start at empty start_key (got region {} start {:?})",
                first.region_id, first.start_key
            )));
        }

        let mut seen = std::collections::BTreeSet::new();
        for (index, meta) in self.regions.iter().enumerate() {
            if meta.region_id == 0 {
                return Err(Error::Storage(
                    "region table must not contain region id 0".to_string(),
                ));
            }
            if !seen.insert(meta.region_id) {
                return Err(Error::Storage(format!(
                    "region id {} appears more than once",
                    meta.region_id
                )));
            }
            if meta.epoch.conf_ver == 0 || meta.epoch.version == 0 {
                return Err(Error::Storage(format!(
                    "region {} has an invalid epoch (conf_ver={}, version={})",
                    meta.region_id, meta.epoch.conf_ver, meta.epoch.version
                )));
            }
            if !meta.end_key.is_empty() && meta.end_key <= meta.start_key {
                return Err(Error::Storage(format!(
                    "region {} has end_key <= start_key",
                    meta.region_id
                )));
            }
            if index == 0 && !meta.start_key.is_empty() {
                return Err(Error::Storage(
                    "only the first region may start at the keyspace head".to_string(),
                ));
            }
            if index + 1 == self.regions.len() {
                if !meta.end_key.is_empty() {
                    return Err(Error::Storage(format!(
                        "region table must end with an unbounded end_key (region {} end {:?})",
                        meta.region_id, meta.end_key
                    )));
                }
            } else if meta.end_key.is_empty() {
                return Err(Error::Storage(format!(
                    "only the last region may be unbounded (region {})",
                    meta.region_id
                )));
            } else if let Some(next) = self.regions.get(index + 1) {
                if meta.end_key != next.start_key {
                    return Err(Error::Storage(format!(
                        "region table must be contiguous: region {} end {:?} != region {} start {:?}",
                        meta.region_id, meta.end_key, next.region_id, next.start_key
                    )));
                }
            }
        }
        Ok(())
    }
}

/// epoch 单调规则：分量不减；key range 变化必须 bump `version`，members 变化
/// 必须 bump `conf_ver`。纯统计字段变化不要求 bump（表不承载心跳统计）。
fn check_update_epoch(old: &RegionMeta, new: &RegionMeta) -> Result<()> {
    if new.region_id != old.region_id {
        return Err(Error::InvalidArgument(
            "update must not change region_id".to_string(),
        ));
    }
    if new.epoch.conf_ver < old.epoch.conf_ver || new.epoch.version < old.epoch.version {
        return Err(Error::InvalidArgument(format!(
            "region {} epoch must not regress (old {:?}, new {:?})",
            new.region_id, old.epoch, new.epoch
        )));
    }
    if new.epoch == old.epoch {
        return Err(Error::InvalidArgument(format!(
            "region {} epoch must strictly increase on update",
            new.region_id
        )));
    }
    let range_changed = new.start_key != old.start_key || new.end_key != old.end_key;
    if range_changed && new.epoch.version <= old.epoch.version {
        return Err(Error::InvalidArgument(format!(
            "region {} key range change requires a version bump",
            new.region_id
        )));
    }
    let members_changed = new.peers != old.peers;
    if members_changed && new.epoch.conf_ver <= old.epoch.conf_ver {
        return Err(Error::InvalidArgument(format!(
            "region {} member change requires a conf_ver bump",
            new.region_id
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use coord_core::types::{Peer, PeerRole};

    fn peers() -> Vec<Peer> {
        vec![Peer {
            node_id: 1,
            raft_addr: "127.0.0.1:50052".to_string(),
            role: PeerRole::Voter,
        }]
    }

    fn meta(region_id: RegionId, start: &[u8], end: &[u8]) -> RegionMeta {
        RegionMeta {
            region_id,
            start_key: start.to_vec(),
            end_key: end.to_vec(),
            epoch: RegionEpoch::initial(),
            peers: peers(),
            approximate_size: 0,
            approximate_keys: 0,
        }
    }

    fn meta_v(region_id: RegionId, start: &[u8], end: &[u8]) -> RegionMeta {
        meta_epoch(region_id, start, end, 1, 2)
    }

    fn meta_epoch(
        region_id: RegionId,
        start: &[u8],
        end: &[u8],
        conf_ver: u64,
        version: u64,
    ) -> RegionMeta {
        let mut meta = meta(region_id, start, end);
        meta.epoch = RegionEpoch { conf_ver, version };
        meta
    }

    fn ranges(regions: &[RegionMeta]) -> Vec<(RegionId, Vec<u8>, Vec<u8>)> {
        regions
            .iter()
            .map(|m| (m.region_id, m.start_key.clone(), m.end_key.clone()))
            .collect()
    }

    #[test]
    fn has_object_region_tracks_the_fixed_interval() {
        let carved = table_with(vec![
            meta(1, b"", b"/obj/"),
            meta(2, b"/obj/", b"/obj0"),
            meta(3, b"/obj0", b""),
        ]);
        assert!(carved.has_object_region());
        let plain = table_with(vec![meta(1, b"", b"")]);
        assert!(
            !plain.has_object_region(),
            "a spanning region is not the fixed interval"
        );
    }

    #[test]
    fn carve_splits_the_seed_at_the_object_space() {
        let mut regions = vec![meta(1, b"", b"")];
        assert_eq!(carve_object_region(&mut regions, 2).unwrap(), 2);
        assert_eq!(
            ranges(&regions),
            vec![
                (1, b"".to_vec(), b"/obj/".to_vec()),
                (2, b"/obj/".to_vec(), b"/obj0".to_vec()),
                (3, b"/obj0".to_vec(), b"".to_vec()),
            ]
        );
        assert!(
            is_object_region(&regions[1]),
            "middle slice is the fixed region"
        );
        let table = RegionTable {
            version: 1,
            regions: regions.clone(),
        };
        table.validate().expect("carved table still tiles");
    }

    #[test]
    fn carve_keeps_the_object_region_when_it_is_already_fixed() {
        let mut regions = vec![
            meta(1, b"", b"/obj/"),
            meta(2, b"/obj/", b"/obj0"),
            meta(3, b"/obj0", b""),
        ];
        let before = regions.clone();
        assert_eq!(carve_object_region(&mut regions, 9).unwrap(), 2);
        assert_eq!(regions, before, "fixed region keeps its id and epoch");
    }

    #[test]
    fn carve_handles_a_seed_that_starts_at_the_object_space() {
        let mut regions = vec![meta(1, b"", b"/obj/"), meta(2, b"/obj/", b"")];
        assert_eq!(carve_object_region(&mut regions, 3).unwrap(), 3);
        assert_eq!(
            ranges(&regions),
            vec![
                (1, b"".to_vec(), b"/obj/".to_vec()),
                (3, b"/obj/".to_vec(), b"/obj0".to_vec()),
                (4, b"/obj0".to_vec(), b"".to_vec()),
            ]
        );
    }

    #[test]
    fn carve_rejects_seeds_that_do_not_cover_the_object_space() {
        let mut regions = vec![meta(1, b"", b"/obj/m"), meta(2, b"/obj/m", b"")];
        let before = regions.clone();
        let err = carve_object_region(&mut regions, 3).unwrap_err();
        assert!(err.to_string().contains("/obj/"), "err: {err}");
        assert_eq!(regions, before, "rejected carve changes nothing");
    }

    #[test]
    fn carve_rejects_zero_or_reused_ids() {
        let mut regions = vec![meta(1, b"", b"")];
        assert!(
            carve_object_region(&mut regions, 0).is_err(),
            "0 is reserved"
        );
        assert!(
            carve_object_region(&mut regions, 1).is_err(),
            "id already used"
        );
        assert_eq!(ranges(&regions), vec![(1, b"".to_vec(), b"".to_vec())]);
    }

    #[test]
    fn init_tiles_and_is_idempotent() {
        let mut table = RegionTable::default();
        assert!(table
            .init(vec![meta(1, b"", b"m"), meta(2, b"m", b"")])
            .unwrap());
        assert_eq!(table.version, 1);
        assert!(!table.init(vec![meta(9, b"", b"")]).unwrap());
        assert_eq!(table.regions.len(), 2);
    }

    #[test]
    fn invalid_tiling_is_rejected_atomically() {
        let mut table = RegionTable::default();
        table.init(vec![meta(1, b"", b"")]).unwrap();
        let before = table.clone();
        assert!(table
            .apply(&[RegionChange::Add {
                meta: meta(2, b"zz", b"")
            }])
            .is_err());
        assert_eq!(table, before);
        assert!(table
            .apply(&[RegionChange::Add {
                meta: meta(3, b"", b"")
            }])
            .is_err());
    }

    #[test]
    fn epoch_must_be_monotonic_per_dimension() {
        let mut table = RegionTable::default();
        table.init(vec![meta(1, b"", b"")]).unwrap();
        let same = meta(1, b"", b"");
        assert!(table
            .apply(&[RegionChange::Update { meta: same.clone() }])
            .is_err());
        let mut bumped = same;
        bumped.epoch = RegionEpoch {
            conf_ver: 2,
            version: 2,
        };
        assert!(table
            .apply(&[RegionChange::Update { meta: bumped }])
            .unwrap());
    }

    #[test]
    fn range_change_requires_version_bump_and_member_change_requires_conf_ver() {
        let mut table = RegionTable::default();
        table.init(vec![meta(1, b"", b"")]).unwrap();
        // 缩界 + 补新区是一个合法批；缺 version bump 时整批拒绝。
        assert!(table
            .apply(&[
                RegionChange::Update {
                    meta: meta(1, b"", b"m")
                },
                RegionChange::Add {
                    meta: meta(2, b"m", b"")
                },
            ])
            .is_err());
        assert!(table
            .apply(&[
                RegionChange::Update {
                    meta: meta_v(1, b"", b"m")
                },
                RegionChange::Add {
                    meta: meta(2, b"m", b"")
                },
            ])
            .unwrap());

        let mut peers_only = meta_epoch(2, b"m", b"", 1, 2);
        peers_only.peers.push(Peer {
            node_id: 2,
            raft_addr: "127.0.0.1:1".to_string(),
            role: PeerRole::Voter,
        });
        assert!(table
            .apply(&[RegionChange::Update {
                meta: peers_only.clone()
            }])
            .is_err());
        peers_only.epoch = RegionEpoch {
            conf_ver: 2,
            version: 2,
        };
        assert!(table
            .apply(&[RegionChange::Update { meta: peers_only }])
            .unwrap());
    }

    #[test]
    fn split_batch_and_merge_batch_are_atomic() {
        let mut table = RegionTable::default();
        table.init(vec![meta(1, b"", b"")]).unwrap();
        let split = vec![
            RegionChange::Update {
                meta: meta_v(1, b"", b"m"),
            },
            RegionChange::Add {
                meta: meta(2, b"m", b""),
            },
        ];
        assert!(table.apply(&split).unwrap());
        assert_eq!(table.route(b"a").unwrap().region_id, 1);
        assert_eq!(table.route(b"z").unwrap().region_id, 2);

        let merge = vec![
            RegionChange::Update {
                meta: meta_epoch(1, b"", b"", 1, 3),
            },
            RegionChange::Remove {
                region_id: 2,
                expected_epoch: RegionEpoch::initial(),
            },
        ];
        assert!(table.apply(&merge).unwrap());
        assert_eq!(table.route(b"z").unwrap().region_id, 1);
    }

    #[test]
    fn remove_requires_matching_epoch() {
        let mut table = RegionTable::default();
        table
            .init(vec![meta(1, b"", b"m"), meta(2, b"m", b"")])
            .unwrap();
        let stale = RegionEpoch {
            conf_ver: 2,
            version: 1,
        };
        assert!(table
            .apply(&[RegionChange::Remove {
                region_id: 2,
                expected_epoch: stale
            }])
            .is_err());
    }

    fn table_with(regions: Vec<RegionMeta>) -> RegionTable {
        let mut table = RegionTable::default();
        table.init(regions).expect("init table");
        table
    }

    #[test]
    fn plan_split_computes_contiguous_child_range() {
        let table = table_with(vec![meta(1, b"", b"")]);
        let plan = plan_split(&table, 1, b"m", 2).expect("split plan");
        assert_eq!(plan.kind, SplitMergeKind::Split);
        assert_eq!(plan.source_region, 1);
        assert_eq!(plan.target_region, Some(2));
        assert_eq!(plan.expected_table_version, table.version);

        let mut next = table.clone();
        next.apply(&plan.changes).expect("plan applies");
        let parent = next.get(1).unwrap();
        let child = next.get(2).unwrap();
        assert_eq!(parent.end_key, b"m");
        assert_eq!(child.start_key, b"m");
        assert_eq!(child.end_key, b"");
        assert_eq!(
            parent.epoch.version,
            table.get(1).unwrap().epoch.version + 1
        );
        assert_eq!(child.epoch.version, parent.epoch.version);
        assert_eq!(child.epoch.conf_ver, parent.epoch.conf_ver);
        assert_eq!(child.peers, parent.peers);
    }

    #[test]
    fn plan_split_rejects_boundaries_object_keys_and_id_reuse() {
        let table = table_with(vec![
            meta(1, b"", b"a"),
            meta(2, b"a", b"z"),
            meta(3, b"z", b""),
        ]);
        for key in [b"a".as_slice(), b"z".as_slice()] {
            assert!(plan_split(&table, 2, key, 9).is_err(), "boundary {key:?}");
        }
        let spanning = table_with(vec![meta(1, b"", b"")]);
        for key in [b"/obj/".as_slice(), b"/obj/m/x".as_slice()] {
            assert!(plan_split(&spanning, 1, key, 2).is_err(), "object {key:?}");
        }
        assert!(
            plan_split(&table, 2, b"m", 2).is_err(),
            "id reuse of source"
        );
        assert!(
            plan_split(&table, 2, b"m", 1).is_err(),
            "id already in table"
        );
        assert!(plan_split(&table, 2, b"m", 9).is_ok(), "unused id accepted");
        assert!(
            plan_split(&RegionTable::default(), 1, b"m", 9).is_err(),
            "uninitialized table"
        );
        assert!(plan_split(&table, 9, b"m", 10).is_err(), "unknown region");
    }

    #[test]
    fn plan_merge_requires_adjacency_and_no_object_space() {
        let table = table_with(vec![meta(1, b"", b"m"), meta(2, b"m", b"")]);
        let plan = plan_merge(&table, 1, 2).expect("merge plan");
        assert_eq!(plan.kind, SplitMergeKind::Merge);
        assert_eq!(plan.source_region, 1);
        assert_eq!(plan.target_region, None);
        let mut next = table.clone();
        next.apply(&plan.changes).expect("plan applies");
        assert_eq!(next.get(1).unwrap().end_key, b"");
        assert!(next.get(2).is_none());
        assert!(plan_merge(&table, 2, 1).is_err(), "wrong order");
        assert!(plan_merge(&table, 1, 1).is_err(), "self merge");

        let obj = table_with(vec![
            meta(1, b"", b"/obj/"),
            meta(2, b"/obj/", b"/obj0"),
            meta(3, b"/obj0", b""),
        ]);
        assert!(plan_merge(&obj, 1, 2).is_err(), "object region merged away");
        assert!(plan_merge(&obj, 2, 3).is_err(), "object region merged away");
        assert!(plan_merge(&obj, 3, 1).is_err(), "non-adjacent");

        // 未固定对象区间时，普通区间跨过 /obj/ 仍可 merge（对象存储未启用）。
        let plain = table_with(vec![meta(1, b"", b"/obj0"), meta(2, b"/obj0", b"")]);
        assert!(plan_merge(&plain, 1, 2).is_ok(), "no fixed object region");

        // 吸收更高 version 的右 Region 时，合并后的 version 不得回退。
        let uneven = table_with(vec![
            meta_epoch(1, b"", b"m", 1, 2),
            meta_epoch(2, b"m", b"", 1, 5),
        ]);
        let plan = plan_merge(&uneven, 1, 2).expect("merge plan");
        let mut next = uneven.clone();
        next.apply(&plan.changes).expect("plan applies");
        assert_eq!(
            next.get(1).unwrap().epoch.version,
            6,
            "absorbed version must not regress"
        );
    }

    #[test]
    fn mq_shard_ranges_tile_the_reserved_space() {
        for (log_shards, group_shards) in [(1u32, 1u32), (3, 2), (8, 4), (64, 32)] {
            let mut regions = vec![meta(1, b"", b"")];
            let (log, group) = carve_mq_regions(&mut regions, 2, log_shards, group_shards)
                .expect("carve mq regions");
            assert_eq!(log.len() as u32, log_shards);
            assert_eq!(group.len() as u32, group_shards);

            // 平铺/无重叠/无空洞由 `init` 的 validate 单点断言。
            let mut table = RegionTable::default();
            table.init(regions.clone()).expect("carved table must tile");
            assert_eq!(
                table.regions.iter().filter(|m| is_mq_region(m)).count() as u32,
                log_shards + group_shards
            );

            // 每个分片前缀都恰好落在自己的区间里。
            for shard in 0..log_shards {
                let (start, end) = mq_log_shard_range(shard, log_shards);
                let prefix = mq_log_shard_prefix(shard);
                assert!(prefix.starts_with(&start) && prefix.as_slice() < end.as_slice());
                let owner = table
                    .regions
                    .iter()
                    .find(|m| {
                        m.start_key.as_slice() <= prefix.as_slice()
                            && (m.end_key.is_empty() || prefix.as_slice() < m.end_key.as_slice())
                    })
                    .expect("shard prefix must have exactly one owner");
                assert_eq!(owner.start_key, start, "shard {shard} owner drift");
            }
            for shard in 0..group_shards {
                let (start, _) = mq_group_shard_range(shard, group_shards);
                let prefix = mq_group_shard_prefix(shard);
                assert!(prefix.starts_with(&start));
            }
        }
    }

    #[test]
    fn carve_mq_regions_is_idempotent() {
        let mut regions = vec![meta(1, b"", b"")];
        let (log, group) = carve_mq_regions(&mut regions, 2, 3, 2).expect("first carve");
        let snapshot = regions.clone();
        let (log_again, group_again) =
            carve_mq_regions(&mut regions, 2, 3, 2).expect("restart on the same topology");
        assert_eq!(log, log_again);
        assert_eq!(group, group_again);
        assert_eq!(
            regions, snapshot,
            "idempotent carve must not rewrite regions"
        );
    }

    #[test]
    fn carve_mq_regions_rejects_partial_or_changed_shard_counts() {
        let mut regions = vec![meta(1, b"", b"")];
        carve_mq_regions(&mut regions, 2, 3, 2).expect("first carve");
        let err = carve_mq_regions(&mut regions, 2, 2, 2).unwrap_err();
        assert!(err.to_string().contains("partially seeded"), "err: {err}");
        let err = carve_mq_regions(&mut regions, 2, 3, 3).unwrap_err();
        assert!(err.to_string().contains("partially seeded"), "err: {err}");
        assert!(carve_mq_regions(&mut regions, 2, 65, 2).is_err());
        assert!(carve_mq_regions(&mut regions, 0, 1, 1).is_err());
    }

    #[test]
    fn mq_space_is_closed_to_kv_split_and_merge() {
        // 遗留表：普通 Region 跨越保留空间（分片尚未切出）时，split key 落进
        // /_mq/ 一律拒绝——否则会在分片区里造出 KV 边界。
        let legacy = table_with(vec![meta(1, b"", b"")]);
        let err = plan_split(&legacy, 1, b"/_mq/log/00/x", 100).unwrap_err();
        assert!(err.to_string().contains("/_mq/"), "err: {err}");
        assert!(plan_split(&legacy, 1, b"/kv/key", 100).is_ok());

        // 切出分片后：分片 Region 既不能被 split 制造边界，也不能被 merge 吸收。
        let mut regions = vec![meta(1, b"", b"")];
        carve_mq_regions(&mut regions, 2, 2, 1).expect("carve");
        let table = table_with(regions);
        let shard_meta = table
            .regions
            .iter()
            .find(|m| is_mq_region(m))
            .expect("mq region")
            .clone();
        let shard = shard_meta.region_id;
        let inside = [shard_meta.start_key.clone(), b"x".to_vec()].concat();
        let err = plan_split(&table, shard, &inside, 100).unwrap_err();
        assert!(err.to_string().contains("/_mq/"), "err: {err}");
        let err = plan_merge(&table, 1, shard).unwrap_err();
        assert!(err.to_string().contains("/_mq/"), "err: {err}");
    }

    #[test]
    fn decode_rejects_trailing_bytes_and_invalid_tables() {
        let mut table = RegionTable::default();
        table.init(vec![meta(1, b"", b"")]).unwrap();
        let mut bytes = table.encode().unwrap();
        bytes.push(0);
        assert!(RegionTable::decode(&bytes).is_err());

        let broken = RegionTable {
            version: 1,
            regions: Vec::new(),
        };
        assert!(broken.encode().is_ok());
        assert!(RegionTable::decode(&broken.encode().unwrap()).is_err());
    }
}
