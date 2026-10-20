//! CommitRecord 格式与写入（ADR-0015 D5/D9，#153）。
//!
//! 记录是**按 Region 的完整变更分片**：prev 值与区间删除的 tombstone 只有在数据
//! Region apply 时才知道；同一 `commit_ts` 的分片由 Watch Coordinator 合并为一个
//! 原子事件（#156）。值与 MVCC 落盘字节完全一致（同一 Barrier 信封），因此 24h
//! 保留的 Commit Log 不引入新的明文面。

use serde::{Deserialize, Serialize};

use coord_core::error::{Error, Result};
use coord_core::storage::WriteTx;

use crate::raft::type_config::SequencerMutation;
use crate::storage::mvcc::TABLE_COMMIT_LOG;

/// 帧魔数：控制字节 `0x01` + `CLR`（便于 hexdump 辨识，且不会与 postcard 首字节混淆）。
pub const COMMIT_LOG_MAGIC: [u8; 4] = [0x01, b'C', b'L', b'R'];

/// 帧版本（只增不改）。
pub const COMMIT_LOG_FORMAT_VERSION: u8 = 1;

/// 载荷编码：zstd。
pub const COMMIT_LOG_CODEC_ZSTD: u8 = 1;

/// #154 接管 shard 路由前的唯一 open shard。
pub const COMMIT_LOG_STAGING_SHARD_ID: u64 = 1;

/// 单帧解压上界：4MiB 写集 + prev 值/信封开销的硬上界（防 zip bomb）。
pub const COMMIT_LOG_MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

/// Commit Log 键空间：`/_sys/commit_log/{shard:u64be}/{commit_ts:u64be}/{region:u64be}`。
///
/// 选择 `/_sys/`：与决策日志/校验索引同域；shard 在最外层，便于 #154 按 shard
/// 整段迁移到独立 raft group。
pub const COMMIT_LOG_PREFIX: &[u8] = b"/_sys/commit_log/";

/// `append` 的结果：新写入 vs 同内容重放。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitLogAppend {
    Inserted,
    Duplicate,
}

/// 帧头：MAGIC(4) | VERSION(1) | CODEC(1) | uncompressed_len(4 BE) | compressed_len(4 BE)。
const COMMIT_LOG_HEADER_LEN: usize = 14;
const COMMIT_LOG_UNCOMPRESSED_OFFSET: usize = 6;
const COMMIT_LOG_COMPRESSED_OFFSET: usize = 10;

/// zstd 压缩级别（3 = 参考实现的默认档；记录可回放，优先写路径延迟）。
const COMMIT_LOG_ZSTD_LEVEL: i32 = 3;

/// 一次事务在某个 Region 的完整 change record。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitRecord {
    pub shard_id: u64,
    pub commit_ts: u64,
    pub txn_id: Vec<u8>,
    pub start_ts: u64,
    pub region_id: u64,
    /// 决策明文写集字节数（不含 prev；与 Commit 阶段 `max_txn_bytes` 同口径）。
    pub txn_bytes: u32,
    pub changes: Vec<CommitChange>,
}

/// 单个 key 的变更：`value=None` 是 tombstone。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitChange {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    /// 此前存在（且未被逻辑删除）时的完整旧值；否则 `None`。
    pub prev: Option<CommitPrevValue>,
}

/// Watch `prev_kv` 所需的完整旧值（落盘字节 + KV 元数据）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitPrevValue {
    pub value: Vec<u8>,
    pub create_revision: i64,
    pub mod_revision: i64,
    pub version: i64,
    pub lease_id: i64,
}

impl CommitRecord {
    /// 决策明文的写集字节数。
    ///
    /// 与 `CommitCandidate::wire_bytes` 的 writes 部分同一口径：密文信封与 prev
    /// 值不计入，否则合法的 4MiB 事务会因加密开销被误拒。
    pub fn txn_write_bytes(mutations: &[SequencerMutation]) -> u64 {
        mutations
            .iter()
            .map(|mutation| match mutation {
                SequencerMutation::Put { key, value, .. } => key.len() + value.len() + 16,
                SequencerMutation::Delete { key } => key.len() + 8,
                SequencerMutation::DeleteRange { start, end } => {
                    start.len().saturating_add(end.len()).saturating_add(32)
                }
            })
            .sum::<usize>() as u64
    }

    /// 编码为可持久化的单帧（zstd）。
    ///
    /// `max_txn_bytes` 校验的是事务**明文写集**（`txn_bytes`），不是压缩后大小；
    /// 密文信封与 prev 值不应把合法事务挤成超限。
    pub fn encode(&self, max_txn_bytes: u64) -> Result<Vec<u8>> {
        if self.shard_id == 0 {
            return Err(Error::InvalidArgument(
                "commit record shard_id must be > 0".to_string(),
            ));
        }
        if self.commit_ts == 0 {
            return Err(Error::InvalidArgument(
                "commit record commit_ts must be > 0".to_string(),
            ));
        }
        if self.txn_id.is_empty() {
            return Err(Error::InvalidArgument(
                "commit record txn_id must not be empty".to_string(),
            ));
        }
        if u64::from(self.txn_bytes) > max_txn_bytes {
            return Err(Error::InvalidArgument(format!(
                "commit record write set {} exceeds max_txn_bytes {max_txn_bytes}",
                self.txn_bytes
            )));
        }

        let payload = postcard::to_allocvec(self)
            .map_err(|err| Error::Storage(format!("encode commit record: {err}")))?;
        if payload.len() > COMMIT_LOG_MAX_FRAME_BYTES {
            return Err(Error::Storage(format!(
                "commit record payload {} exceeds frame cap {COMMIT_LOG_MAX_FRAME_BYTES}",
                payload.len()
            )));
        }
        let compressed = zstd::bulk::compress(&payload, COMMIT_LOG_ZSTD_LEVEL)
            .map_err(|err| Error::Storage(format!("compress commit record: {err}")))?;

        let mut framed = Vec::with_capacity(COMMIT_LOG_HEADER_LEN + compressed.len());
        framed.extend_from_slice(&COMMIT_LOG_MAGIC);
        framed.push(COMMIT_LOG_FORMAT_VERSION);
        framed.push(COMMIT_LOG_CODEC_ZSTD);
        framed.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        framed.extend_from_slice(&(compressed.len() as u32).to_be_bytes());
        framed.extend_from_slice(&compressed);
        Ok(framed)
    }

    /// 解码单帧；损坏/未知版本/尾随字节/超上界一律显式拒绝。
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < COMMIT_LOG_HEADER_LEN {
            return Err(Error::Storage(
                "commit record frame shorter than header".to_string(),
            ));
        }
        if bytes[..COMMIT_LOG_MAGIC.len()] != COMMIT_LOG_MAGIC {
            return Err(Error::Storage(
                "commit record frame has invalid magic".to_string(),
            ));
        }
        let version = bytes[COMMIT_LOG_MAGIC.len()];
        if version != COMMIT_LOG_FORMAT_VERSION {
            return Err(Error::Storage(format!(
                "commit record frame has unsupported version {version}"
            )));
        }
        let codec = bytes[COMMIT_LOG_MAGIC.len() + 1];
        if codec != COMMIT_LOG_CODEC_ZSTD {
            return Err(Error::Storage(format!(
                "commit record frame has unsupported codec {codec}"
            )));
        }

        let uncompressed_len = read_u32(bytes, COMMIT_LOG_UNCOMPRESSED_OFFSET) as usize;
        if uncompressed_len > COMMIT_LOG_MAX_FRAME_BYTES {
            return Err(Error::Storage(format!(
                "commit record frame declares {uncompressed_len} bytes, above cap \
                 {COMMIT_LOG_MAX_FRAME_BYTES}"
            )));
        }
        let compressed_len = read_u32(bytes, COMMIT_LOG_COMPRESSED_OFFSET) as usize;
        let payload_start = COMMIT_LOG_HEADER_LEN;
        let expected = payload_start
            .checked_add(compressed_len)
            .ok_or_else(|| Error::Storage("commit record frame length overflow".to_string()))?;
        if bytes.len() != expected {
            return Err(Error::Storage(format!(
                "commit record frame length mismatch: declared {compressed_len} compressed bytes, \
                 got {} trailing bytes",
                bytes.len().saturating_sub(payload_start)
            )));
        }

        let payload = zstd::bulk::decompress(&bytes[payload_start..], uncompressed_len)
            .map_err(|err| Error::Storage(format!("decompress commit record: {err}")))?;
        if payload.len() != uncompressed_len {
            return Err(Error::Storage(format!(
                "commit record decompressed {} bytes, expected {uncompressed_len}",
                payload.len()
            )));
        }
        let (record, remainder) = postcard::take_from_bytes::<Self>(&payload)
            .map_err(|err| Error::Storage(format!("decode commit record: {err}")))?;
        if !remainder.is_empty() {
            return Err(Error::Storage(format!(
                "commit record has {} trailing payload bytes",
                remainder.len()
            )));
        }
        Ok(record)
    }
}

/// 一个 shard 的键前缀（`commit_records_after` 的扫描边界）。
pub(crate) fn commit_log_shard_prefix(shard_id: u64) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(COMMIT_LOG_PREFIX.len() + 8);
    prefix.extend_from_slice(COMMIT_LOG_PREFIX);
    prefix.extend_from_slice(&shard_id.to_be_bytes());
    prefix
}

pub(crate) fn encode_commit_log_key(shard_id: u64, commit_ts: u64, region_id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(COMMIT_LOG_PREFIX.len() + 24);
    key.extend_from_slice(COMMIT_LOG_PREFIX);
    key.extend_from_slice(&shard_id.to_be_bytes());
    key.extend_from_slice(&commit_ts.to_be_bytes());
    key.extend_from_slice(&region_id.to_be_bytes());
    key
}

/// 幂等写入：同键同内容 = `Duplicate`，同键不同内容 = fail-closed。
///
/// 与数据 apply 同一条 redb 事务调用，因此「数据可见 ⇒ 记录已落盘」；重放走
/// changelog 守卫，不会重复写。
pub(crate) fn append_commit_record_in_tx(
    tx: &mut dyn WriteTx,
    record: &CommitRecord,
    max_txn_bytes: u64,
) -> Result<CommitLogAppend> {
    let key = encode_commit_log_key(record.shard_id, record.commit_ts, record.region_id);
    let encoded = record.encode(max_txn_bytes)?;
    match tx.get(TABLE_COMMIT_LOG, &key)? {
        Some(existing) => {
            let existing = CommitRecord::decode(&existing).map_err(|err| {
                Error::Storage(format!(
                    "existing commit record {}:{}:{} is unreadable: {err}",
                    record.shard_id, record.commit_ts, record.region_id
                ))
            })?;
            if existing == *record {
                Ok(CommitLogAppend::Duplicate)
            } else {
                Err(Error::Storage(format!(
                    "conflicting commit record for shard {} commit_ts {} region {}",
                    record.shard_id, record.commit_ts, record.region_id
                )))
            }
        }
        None => {
            tx.insert(TABLE_COMMIT_LOG, &key, &encoded)?;
            Ok(CommitLogAppend::Inserted)
        }
    }
}

/// 解码一行；坏行带键上下文显式失败。
pub(crate) fn decode_commit_log_row(key: &[u8], bytes: &[u8]) -> Result<CommitRecord> {
    CommitRecord::decode(bytes)
        .map_err(|err| Error::Storage(format!("commit log row {}: {err}", hex_key(key))))
}

fn hex_key(key: &[u8]) -> String {
    key.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prev(value: &[u8], mod_revision: i64) -> CommitPrevValue {
        CommitPrevValue {
            value: value.to_vec(),
            create_revision: mod_revision.saturating_sub(1),
            mod_revision,
            version: 2,
            lease_id: 0,
        }
    }

    fn change(key: &[u8], value: Option<&[u8]>, prev: Option<CommitPrevValue>) -> CommitChange {
        CommitChange {
            key: key.to_vec(),
            value: value.map(<[u8]>::to_vec),
            prev,
        }
    }

    fn record(commit_ts: u64, txn_bytes: u32, changes: Vec<CommitChange>) -> CommitRecord {
        CommitRecord {
            shard_id: COMMIT_LOG_STAGING_SHARD_ID,
            commit_ts,
            txn_id: format!("txn-{commit_ts}").into_bytes(),
            start_ts: commit_ts.saturating_sub(1),
            region_id: 7,
            txn_bytes,
            changes,
        }
    }

    fn sample() -> CommitRecord {
        record(
            10,
            64,
            vec![
                change(b"c", Some(b"new"), Some(prev(b"old", 5))),
                change(b"a", None, Some(prev(b"gone", 4))),
                change(b"b", None, None),
            ],
        )
    }

    #[test]
    fn test_commit_record_roundtrip_full_values_and_tombstones() {
        let encoded = sample().encode(4 * 1024 * 1024).unwrap();
        assert_eq!(&encoded[..4], &COMMIT_LOG_MAGIC);
        assert_eq!(CommitRecord::decode(&encoded).unwrap(), sample());
    }

    #[test]
    fn test_commit_record_zstd_compresses_repetitive_values() {
        let value = vec![b'x'; 64 * 1024];
        let record = record(11, 65_552, vec![change(b"big", Some(&value), None)]);
        let encoded = record.encode(4 * 1024 * 1024).unwrap();
        assert!(
            encoded.len() < 4 * 1024,
            "repetitive 64KiB value must compress below 4KiB, got {} bytes",
            encoded.len()
        );
        assert_eq!(CommitRecord::decode(&encoded).unwrap(), record);
    }

    #[test]
    fn test_commit_record_rejects_txn_over_limit() {
        let limit = 4 * 1024 * 1024u64;
        let over = record(12, (limit + 1) as u32, vec![change(b"k", Some(b"v"), None)]);
        assert!(
            over.encode(limit).is_err(),
            "txn_bytes above max_txn_bytes must be rejected"
        );
        let exactly = record(13, limit as u32, vec![change(b"k", Some(b"v"), None)]);
        assert!(
            exactly.encode(limit).is_ok(),
            "the limit itself is inclusive"
        );
    }

    #[test]
    fn test_commit_record_decode_rejects_corruption() {
        let encoded = sample().encode(4 * 1024 * 1024).unwrap();

        let mut bad_magic = encoded.clone();
        bad_magic[0] = 0x7f;
        assert!(CommitRecord::decode(&bad_magic).is_err());

        let mut bad_version = encoded.clone();
        bad_version[4] = COMMIT_LOG_FORMAT_VERSION + 1;
        assert!(CommitRecord::decode(&bad_version).is_err());

        assert!(CommitRecord::decode(&encoded[..encoded.len() - 1]).is_err());

        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(CommitRecord::decode(&trailing).is_err());
    }

    #[test]
    fn test_commit_record_decode_bounds_decompression() {
        let encoded = sample().encode(4 * 1024 * 1024).unwrap();
        let mut bomb = encoded;
        let declared = (COMMIT_LOG_MAX_FRAME_BYTES as u32 + 1).to_be_bytes();
        // 布局：MAGIC(4) | VERSION(1) | CODEC(1) | uncompressed_len(4 BE) | ...
        bomb[6..10].copy_from_slice(&declared);
        assert!(
            CommitRecord::decode(&bomb).is_err(),
            "frames declaring more than the cap must be rejected before decompression"
        );
    }
}
