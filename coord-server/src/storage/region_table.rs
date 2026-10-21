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

use crate::storage::object_store::{key_in_object_space, OBJECT_PREFIX};

/// region 0 存整张表的单键。
pub const REGION_TABLE_KEY: &[u8] = b"/_sys/pd/regions";

/// 对象空间的字典序上界（半开区间 `[/obj/, /obj0)`，与对象存储守卫一致）。
const OBJECT_SPACE_END: &[u8] = b"/obj0";

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
pub fn plan_merge(
    table: &RegionTable,
    left: RegionId,
    right: RegionId,
) -> Result<SplitMergePlan> {
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

    let mut merged = left_meta;
    merged.end_key = right_meta.end_key.clone();
    merged.epoch.version += 1;
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
/// 只保护「已按 ADR 固定出来的对象 Region」：还没有对象区间时，普通 KV 区间
/// 跨过 `/obj/` 是合法形态（对象存储未启用），不能被 merge 守卫无故锁死。
fn is_object_region(meta: &RegionMeta) -> bool {
    meta.start_key == OBJECT_PREFIX && meta.end_key == OBJECT_SPACE_END
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
            assert!(
                plan_split(&spanning, 1, key, 2).is_err(),
                "object {key:?}"
            );
        }
        assert!(plan_split(&table, 2, b"m", 2).is_err(), "id reuse of source");
        assert!(plan_split(&table, 2, b"m", 1).is_err(), "id already in table");
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
