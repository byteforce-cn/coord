// Commit Log shard 表与 D9 阈值策略（ADR-0015 D5/D9，#154）。
//
// 表是 region 0 的权威元数据：shard 以 `[start_ts, end_ts)` 划分 commit_ts
// 空间，`end_ts == 0` 表示未定界（只能是最后一个 shard）。边界只由「前一 shard
// 的 end」派生，构造性保证无重叠、无空洞；`start_ts` 从 1 开始，使建表之前
// 已提交的决策也有归属（否则冷启动/重启后残留的 outbox 记录永远无法路由）。
//
// 阈值逐字来自 D9：1,000,000 条或 1 小时先到先 seal，80% 预创建下一个，
// seal 超时 5 分钟。本模块只做纯计算，不做 IO / 不读时钟——调用方注入 now_ms。

use serde::{Deserialize, Serialize};

use coord_core::error::{Error, Result};

/// region 0 存整张表的单键（表 ≤ 每小时一项，整表覆盖写最简单且原子）。
pub const COMMIT_LOG_SHARD_TABLE_KEY: &[u8] = b"/_sys/commit_log/shards";

/// shard raft 组的 region id 保留前缀。
///
/// data Region 的 id 从 1 递增（v1 静态表只有个位数编号）；用高位命名空间把
/// 「shard 组」与「数据 Region」在同一张 `RaftRpcService` 路由表里分开，二者
/// 不会互相覆盖。shard_id 必须落在低 62 位。
pub const COMMIT_LOG_SHARD_REGION_ID_BASE: u64 = 0x4000_0000_0000_0000;

/// shard_id → raft 组 region id（网络解复用键）。
pub fn shard_region_id(shard_id: u64) -> u64 {
    COMMIT_LOG_SHARD_REGION_ID_BASE | shard_id
}

/// 单个 Commit Log shard 的元数据。
///
/// `end_ts == 0` = 未定界（只能是最后一个 shard）；`ended_at_ms`/`sealed_at_ms`
/// 为 0 表示尚未定界/尚未 seal。时间戳只是可观测与超时判定用，不参与路由。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitLogShardMeta {
    pub shard_id: u64,
    pub start_ts: u64,
    pub end_ts: u64,
    pub opened_at_ms: u64,
    pub ended_at_ms: u64,
    pub sealed_at_ms: u64,
}

impl CommitLogShardMeta {
    /// `ts` 是否落在本 shard 的半开区间内。
    pub fn covers(&self, ts: u64) -> bool {
        ts >= self.start_ts && (self.end_ts == 0 || ts < self.end_ts)
    }
}

/// 整张 shard 表（按 `shard_id` 升序）。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitLogShardTable {
    pub shards: Vec<CommitLogShardMeta>,
}

impl CommitLogShardTable {
    pub fn encode(&self) -> Result<Vec<u8>> {
        postcard::to_allocvec(self)
            .map_err(|err| Error::Storage(format!("encode commit log shard table: {err}")))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let (table, remainder) = postcard::take_from_bytes::<Self>(bytes)
            .map_err(|err| Error::Storage(format!("decode commit log shard table: {err}")))?;
        if !remainder.is_empty() {
            return Err(Error::Storage(format!(
                "commit log shard table has {} trailing bytes",
                remainder.len()
            )));
        }
        table.validate()?;
        Ok(table)
    }

    /// `ts` 所属 shard（表小，线性扫描；边界为半开区间）。
    pub fn covering(&self, ts: u64) -> Option<&CommitLogShardMeta> {
        self.shards.iter().find(|meta| meta.covers(ts))
    }

    pub fn latest(&self) -> Option<&CommitLogShardMeta> {
        self.shards.last()
    }

    /// 打开一个新的未定界 shard。
    ///
    /// 表空时用 `first_start_ts`（调用方恒传 1，覆盖建表前的历史决策）；否则
    /// 起点**只能**取前一 shard 的 `end_ts`——不采信调用方给的起始值，杜绝
    /// 重叠/空洞。已有未定界 shard 时返回 `Ok(false)`（幂等 no-op）。
    pub fn open(&mut self, first_start_ts: u64, now_ms: u64) -> Result<bool> {
        let (shard_id, start_ts) = match self.latest() {
            Some(last) => {
                if last.end_ts == 0 {
                    return Ok(false);
                }
                (last.shard_id + 1, last.end_ts)
            }
            None => {
                if first_start_ts == 0 {
                    return Err(Error::InvalidArgument(
                        "first commit log shard must start at ts > 0".to_string(),
                    ));
                }
                (1, first_start_ts)
            }
        };
        self.shards.push(CommitLogShardMeta {
            shard_id,
            start_ts,
            end_ts: 0,
            opened_at_ms: now_ms,
            ended_at_ms: 0,
            sealed_at_ms: 0,
        });
        Ok(true)
    }

    /// 给**最后一个**未定界 shard 定界。
    ///
    /// 已定界后不接受改写（只允许同值/更小值 no-op）：边界一旦写进 raft 日志，
    /// 各节点的 outbox 路由必须收敛到同一结果。
    pub fn set_end(&mut self, shard_id: u64, end_ts: u64, now_ms: u64) -> Result<bool> {
        let Some(last) = self.shards.last_mut() else {
            return Err(Error::InvalidArgument(format!(
                "commit log shard {shard_id} does not exist"
            )));
        };
        if last.shard_id != shard_id {
            return Err(Error::InvalidArgument(format!(
                "commit log shard {shard_id} is not the latest shard ({})",
                last.shard_id
            )));
        }
        if last.end_ts != 0 {
            return Ok(false);
        }
        if end_ts <= last.start_ts {
            return Err(Error::InvalidArgument(format!(
                "commit log shard {shard_id} end_ts {end_ts} must exceed start_ts {}",
                last.start_ts
            )));
        }
        last.end_ts = end_ts;
        last.ended_at_ms = now_ms;
        Ok(true)
    }

    /// 标记已定界 shard 为 sealed；未定界 shard 拒绝（seal 后不得再有新边界）。
    pub fn seal(&mut self, shard_id: u64, now_ms: u64) -> Result<bool> {
        let Some(meta) = self
            .shards
            .iter_mut()
            .find(|meta| meta.shard_id == shard_id)
        else {
            return Err(Error::InvalidArgument(format!(
                "commit log shard {shard_id} does not exist"
            )));
        };
        if meta.end_ts == 0 {
            return Err(Error::InvalidArgument(format!(
                "commit log shard {shard_id} has no end_ts; set the boundary before sealing"
            )));
        }
        if meta.sealed_at_ms != 0 {
            return Ok(false);
        }
        meta.sealed_at_ms = now_ms;
        Ok(true)
    }

    /// 表不变量：id 连续、start > 0、半开区间首尾相接、最多一个未定界 shard。
    pub fn validate(&self) -> Result<()> {
        for (index, meta) in self.shards.iter().enumerate() {
            if meta.start_ts == 0 {
                return Err(Error::Storage(format!(
                    "commit log shard {} starts at ts 0",
                    meta.shard_id
                )));
            }
            if meta.shard_id != index as u64 + 1 {
                return Err(Error::Storage(format!(
                    "commit log shard ids must be consecutive: expected {}, got {}",
                    index + 1,
                    meta.shard_id
                )));
            }
            if meta.end_ts != 0 && meta.end_ts <= meta.start_ts {
                return Err(Error::Storage(format!(
                    "commit log shard {} has end_ts {} <= start_ts {}",
                    meta.shard_id, meta.end_ts, meta.start_ts
                )));
            }
            if let Some(next) = self.shards.get(index + 1) {
                if meta.end_ts == 0 {
                    return Err(Error::Storage(format!(
                        "unbounded commit log shard {} must be the last shard",
                        meta.shard_id
                    )));
                }
                if next.start_ts != meta.end_ts {
                    return Err(Error::Storage(format!(
                        "commit log shard table has a gap/overlap: shard {} end {} != shard {} start {}",
                        meta.shard_id, meta.end_ts, next.shard_id, next.start_ts
                    )));
                }
            }
        }
        Ok(())
    }
}

/// 定界点：`max(ship_cursor, sequencer_last_ts, start_ts) + 1`。
///
/// 取 sequencer 最后决策 ts：它之后的新决策一定更大，保证「新 shard 只接新
/// 决策、旧 shard 只接旧决策」；取 ship cursor：已在 shard 里的最大 ts。
pub fn split_end_ts(ship_cursor_ts: u64, sequencer_last_ts: u64, start_ts: u64) -> u64 {
    ship_cursor_ts
        .max(sequencer_last_ts)
        .max(start_ts)
        .saturating_add(1)
}

/// 80% 预创建：条数或时长任一达到 4/5。
pub fn precreate_due(records: u64, age_ms: u64, shard_records: u64, shard_secs: u64) -> bool {
    u128::from(records) * 5 >= u128::from(shard_records) * 4
        || u128::from(age_ms) * 5 >= u128::from(shard_secs) * 1_000 * 4
}

/// D9 seal 阈值：1M 条或 1 小时先到先 seal。
pub fn seal_due(records: u64, age_ms: u64, shard_records: u64, shard_secs: u64) -> bool {
    records >= shard_records || u128::from(age_ms) >= u128::from(shard_secs) * 1_000
}

/// seal 超时：定界后等待 in-flight 记录的上限（#155 的 watermark 到位前的兜底）。
pub fn seal_timeout_due(ended_at_ms: u64, now_ms: u64, timeout_secs: u64) -> bool {
    ended_at_ms != 0
        && u128::from(now_ms) >= u128::from(ended_at_ms) + u128::from(timeout_secs) * 1_000
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard(shard_id: u64, start_ts: u64, end_ts: u64) -> CommitLogShardMeta {
        CommitLogShardMeta {
            shard_id,
            start_ts,
            end_ts,
            opened_at_ms: 1_000,
            ended_at_ms: 0,
            sealed_at_ms: 0,
        }
    }

    #[test]
    fn open_first_shard_starts_at_one_and_covers_history() {
        let mut table = CommitLogShardTable::default();
        assert!(table.open(1, 1_000).is_ok());
        assert_eq!(table.shards.len(), 1);
        assert_eq!(table.shards[0].start_ts, 1);
        assert!(
            table.covering(1).is_some(),
            "historical commit_ts must be covered"
        );
        assert!(
            table.covering(u64::MAX / 2).is_some(),
            "open-ended covers the future"
        );
        assert!(
            !table.open(7, 1_100).unwrap(),
            "no second open while unbounded"
        );
        assert_eq!(table.latest().unwrap().shard_id, 1);
    }

    #[test]
    fn split_opens_next_shard_without_gap_or_overlap() {
        let mut table = CommitLogShardTable::default();
        table.open(1, 1_000).unwrap();
        assert!(table.set_end(1, 101, 2_000).unwrap());
        assert!(table.open(1, 2_100).unwrap());
        assert_eq!(table.shards.len(), 2);
        assert_eq!(table.shards[1].start_ts, 101);
        assert_eq!(table.shards[1].shard_id, 2);
        assert_eq!(table.covering(100).unwrap().shard_id, 1, "end is half-open");
        assert_eq!(table.covering(101).unwrap().shard_id, 2);
        table.validate().expect("contiguous table");
    }

    #[test]
    fn set_end_is_monotonic_and_requires_the_latest_shard() {
        let mut table = CommitLogShardTable::default();
        table.open(1, 1_000).unwrap();
        assert!(
            table.set_end(1, 1, 2_300).is_err(),
            "end must exceed the shard start (1)"
        );
        assert!(table.set_end(1, 50, 2_000).unwrap());
        assert!(!table.set_end(1, 40, 2_100).unwrap(), "must not lower end");
        assert!(
            !table.set_end(1, 50, 2_200).unwrap(),
            "same value is a no-op"
        );
        assert!(
            table.set_end(9, 60, 2_200).is_err(),
            "unknown shard rejected"
        );
    }

    #[test]
    fn seal_requires_a_bounded_shard_and_is_idempotent() {
        let mut table = CommitLogShardTable::default();
        table.open(1, 1_000).unwrap();
        assert!(
            table.seal(1, 2_000).is_err(),
            "open-ended shard cannot be sealed"
        );
        table.set_end(1, 100, 1_500).unwrap();
        assert!(table.seal(1, 2_000).unwrap());
        assert_eq!(table.shards[0].sealed_at_ms, 2_000);
        assert!(!table.seal(1, 2_100).unwrap(), "second seal is a no-op");
    }

    #[test]
    fn policy_thresholds_follow_d9_percentages() {
        assert!(!precreate_due(799_999, 0, 1_000_000, 3_600));
        assert!(precreate_due(800_000, 0, 1_000_000, 3_600));
        assert!(precreate_due(0, 2_880_000, 1_000_000, 3_600));
        assert!(!seal_due(999_999, 3_599_999, 1_000_000, 3_600));
        assert!(seal_due(1_000_000, 0, 1_000_000, 3_600));
        assert!(seal_due(0, 3_600_000, 1_000_000, 3_600));
        assert!(!seal_timeout_due(1_000_000, 1_299_999, 300));
        assert!(seal_timeout_due(1_000_000, 1_300_000, 300));
        assert!(
            !seal_timeout_due(0, 10_000_000, 300),
            "unbounded shard is not sealable"
        );
        assert_eq!(split_end_ts(0, 0, 7), 8);
        assert_eq!(split_end_ts(105, 109, 7), 110);
        assert_eq!(split_end_ts(3, 4, 9), 10);
    }

    #[test]
    fn encode_decode_roundtrip_and_validation_rejects_bad_tables() {
        let mut table = CommitLogShardTable::default();
        table.open(1, 1_000).unwrap();
        table.set_end(1, 10, 1_500).unwrap();
        assert_eq!(
            CommitLogShardTable::decode(&table.encode().unwrap()).unwrap(),
            table
        );

        let mut zero_start = table.clone();
        zero_start.shards[0].start_ts = 0;
        assert!(zero_start.validate().is_err());

        let mut gap = table.clone();
        gap.shards.push(shard(2, 11, 0));
        assert!(gap.validate().is_err(), "start must equal previous end");

        let mut overlap = table.clone();
        overlap.shards.push(shard(2, 9, 0));
        assert!(overlap.validate().is_err());

        let mut two_unbounded = table.clone();
        two_unbounded.shards[0].end_ts = 0;
        two_unbounded.shards.push(shard(2, 10, 0));
        assert!(two_unbounded.validate().is_err());

        let mut duplicate_id = table.clone();
        duplicate_id.shards.push(shard(1, 10, 0));
        assert!(duplicate_id.validate().is_err());
    }

    #[test]
    fn shard_region_ids_live_in_the_reserved_namespace() {
        assert_eq!(shard_region_id(1), COMMIT_LOG_SHARD_REGION_ID_BASE | 1);
        assert!(
            shard_region_id(1) > u32::MAX as u64,
            "must not collide with data region ids"
        );
    }
}
