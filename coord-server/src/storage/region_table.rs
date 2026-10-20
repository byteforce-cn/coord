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

/// region 0 存整张表的单键。
pub const REGION_TABLE_KEY: &[u8] = b"/_sys/pd/regions";

/// 一次 Region 表变更（由 `RegionMetaOp::Apply` 成批承载）。
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

    #[test]
    fn decode_rejects_trailing_bytes_and_invalid_tables() {
        let mut table = RegionTable::default();
        table.init(vec![meta(1, b"", b"")]).unwrap();
        let mut bytes = table.encode().unwrap();
        bytes.push(0);
        assert!(RegionTable::decode(&bytes).is_err());

        let mut broken = RegionTable::default();
        broken.version = 1;
        assert!(broken.encode().is_ok());
        assert!(RegionTable::decode(&broken.encode().unwrap()).is_err());
    }
}
