// MVCC 版本化存储层
//
// 在 StorageBackend 之上提供版本化 KV 存储能力：
// - 全局单调递增 Revision
// - Key 空间编码（/kv/ 用户数据、/_meta/ 元数据、/_changelog/ 变更日志）
// - 基于 Revision 的快照读
// - 写入时自动生成 Changelog 条目
//
// 直接复用 Redb 内置 MVCC，本层只负责应用层语义（Revision 分配、Key 编码、
// Changelog 写入），不额外建立版本管理。

use parking_lot::RwLock;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use coord_core::error::{Error, Result};
use coord_core::kv_range::RangeSemantics;
use coord_core::storage::{ReadTx, StorageBackend, WriteTx};
use coord_core::types::{LeaseID, Revision};

use crate::raft::type_config::CommitDecisionApplyLog;
use crate::security::barrier::Barrier;
use crate::storage::commit_log::{
    CommitChange, CommitLogAppend, CommitLogAppendResult, CommitPrevValue, CommitRecord,
    COMMIT_LOG_COUNT_KEY, COMMIT_LOG_CURSOR_KEY,
};
use crate::storage::commit_log_shard::{
    CommitLogShardMeta, CommitLogShardTable, COMMIT_LOG_SHARD_TABLE_KEY,
};

// ──── Key 空间常量 ────

/// 用户 KV 数据的 Key 前缀
const KV_PREFIX: &[u8] = b"/kv/";

/// 内部元数据的 Key 前缀（阶段使用）
#[allow(dead_code)]
const META_PREFIX: &[u8] = b"/_meta/";

/// Lease 绑定的 Key 前缀（阶段使用）
#[allow(dead_code)]
const LEASE_PREFIX: &[u8] = b"/_lease/";

/// 变更日志的 Key 前缀
pub(crate) const CHANGELOG_PREFIX: &[u8] = b"/_changelog/";

/// 认证数据的 Key 前缀（阶段使用）
#[allow(dead_code)]
const AUTH_PREFIX: &[u8] = b"/_auth/";

// ──── Meta 子键 ────

/// 已 Apply 的最大 Raft LogId（崩溃恢复检查点；与命令写入同一事务）
pub(crate) const META_LAST_APPLIED: &[u8] = b"/_meta/last_applied";

/// 已提交的最大 revision（u64 BE；与命令写入同一事务）。
///
/// v1（`RevisionMode::LogIndex`）下等于已 apply 的 log index；v2（`CommitTs`）
/// 下等于已 apply 的最大 `commit_ts`，与 raft log index 无关。
pub const META_CURRENT_REVISION: &[u8] = b"/_meta/current_revision";

/// `AppliedLogId` 持久化编码的魔数（标识 `ALI1` 定长编码；非该魔数按水位缺失处理）。
const APPLIED_LOG_ID_MAGIC: &[u8; 4] = b"ALI1";

/// 已持久化快照元数据（last_log_id/checksum/path）
pub(crate) const META_SNAPSHOT: &[u8] = b"/_meta/snapshot";

/// 已持久化的 Raft membership（与 applied 持久化配套：重启后 leader 选举依赖它）
pub(crate) const META_MEMBERSHIP: &[u8] = b"/_meta/membership";

/// 已持久化的 compacted revision（raft 下发，节点一致；
/// 小于等于它的 changelog/tombstone 已被物理删除）
pub(crate) const META_COMPACT_REVISION: &[u8] = b"/_meta/compacted_revision";

/// Seal 状态：0=Unsealed, 1=Sealed, 2=Unsealing（阶段使用）
#[allow(dead_code)]
const META_SEAL_STATUS: &[u8] = b"/_meta/seal_status";

/// Auth 是否启用（阶段使用）
#[allow(dead_code)]
const META_AUTH_ENABLED: &[u8] = b"/_meta/auth_enabled";

// ──── 表名常量 ────

pub(crate) const TABLE_KV: &str = "kv";
pub(crate) const TABLE_META: &str = "meta";
pub(crate) const TABLE_CHANGELOG: &str = "changelog";
pub(crate) const TABLE_KV_META: &str = "kv_meta";
/// Commit Log（#153）：`(shard_id, commit_ts, region_id)` → zstd(postcard(CommitRecord))。
pub(crate) const TABLE_COMMIT_LOG: &str = "commit_log";

// ──── KV 元数据 Key 前缀 ────

/// KV 元数据的 Key 前缀：/_kv_meta/{user_key}
const KV_META_PREFIX: &[u8] = b"/_kv_meta/";

/// 将用户 Key 编码为元数据存储 Key
pub(crate) fn encode_kv_meta_key(user_key: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(KV_META_PREFIX.len() + user_key.len());
    encoded.extend_from_slice(KV_META_PREFIX);
    encoded.extend_from_slice(user_key);
    encoded
}

// ──── PD 全局 operator 队列（/ docs region 0 raft）────

/// PD 队列内部记录前缀（region 0 MVCC，`TABLE_KV` 原始行）。
///
/// 非 `/kv/` 前缀 → 不加密（同 `/_lease/`、`/_sys/auth/` 先例）；不上 Watch；
/// 无 KvMetadata → 不经 compaction tombstone 清理（同节点重放/重启安全）。
/// 快照覆盖已落地（SnapshotData v4 导出 `/_pd/` 域——见
/// docs 与 `storage/snapshot.rs`）。
pub const PD_QUEUE_PREFIX: &[u8] = b"/_pd/ops/";

/// 终态条目保留上限（超出删最旧终态；apply 期确定性裁剪，只依赖持久状态）
const PD_QUEUE_MAX_TERMINAL: usize = 512;

// ──── TSO（region 0 system raft，#136）────

// 记录选 `/_sys/` 而不是 `/_meta/`：快照导出按 `/_sys/*` 整域携带
// （`/_sys/auth/` 之外），高水位随快照/恢复一起走——装快照后不得回退（否则会
// 重发旧批）。非 `/kv/` 前缀 → 不加密；无 KvMetadata → 不经 compaction 清理。

/// 下一个未发放的时间戳（u64 BE；缺失 = 1）。0 是协议里的「由服务端决定」
/// 哨兵，不得发放。
const TSO_HIGH_WATER: &[u8] = b"/_sys/tso/high_water";

/// 最近一次分配的批次（信封 V2；重放同一 revision 时原样返回）。
const TSO_LAST_BATCH: &[u8] = b"/_sys/tso/last_batch";

// ──── Commit Sequencer（region 0 system raft，#146）────
//
// 全部选 `/_sys/seq/`：快照按 `/_sys/*` 整域导出，决策日志/校验索引/幂等键
// 随 region 0 快照恢复。非 `/kv/` 前缀 → 不加密、不进 compaction 清理。

/// 决策日志：`/_sys/seq/decision/{commit_ts:u64be}` → 信封 V2 `SequencerDecision`
const SEQUENCER_DECISION_PREFIX: &[u8] = b"/_sys/seq/decision/";

/// 已决策的最大 `commit_ts`（u64 BE；缺失 = 0）。D11.1 的单调水位。
const SEQUENCER_LAST_COMMIT_TS: &[u8] = b"/_sys/seq/last_commit_ts";

/// 单次提交索引：`sha256("txn", txn_id)` → commit_ts BE
const SEQUENCER_TXN_PREFIX: &[u8] = b"/_sys/seq/txn/";

/// 请求幂等索引：`sha256("req", txn_id, request_id)` → commit_ts BE
const SEQUENCER_REQUEST_PREFIX: &[u8] = b"/_sys/seq/req/";

/// point-key 校验索引：`sha256("key", key)` → 最大 commit_ts BE
const SEQUENCER_KEY_INDEX_PREFIX: &[u8] = b"/_sys/seq/vidx/";

/// 有序 range 校验索引：`/_sys/seq/ridx/{key}` → 最大 commit_ts BE。
///
/// key 不哈希：前缀固定后，redb B-tree 的键序 = 用户 key 字典序，`[start,end)`
/// 只需一次（有界）区间扫描。
const SEQUENCER_RANGE_INDEX_PREFIX: &[u8] = b"/_sys/seq/ridx/";

/// 区间写校验索引：`sha256("interval", start, end)` → `SequencerIntervalWrite`。
///
/// 区间写没有可枚举的 key，不能进点索引；OCC 的 point/range 校验都在这里
/// 扫（有界），命中条件见 `sequencer_interval_*_in`。
const SEQUENCER_INTERVAL_INDEX_PREFIX: &[u8] = b"/_sys/seq/iidx/";

/// 两个校验索引共同覆盖到的最大 commit_ts（u64 BE；缺失 = 0）。
///
/// 正常提交在同一事务里推进它；落后时按它追赶，重建时先清它。
const SEQUENCER_INDEX_LAST_TS: &[u8] = b"/_sys/seq/vidx_last_ts";

/// 数据 Region 已应用的决策游标（u64 BE；缺失 = 0）。
///
/// 选 `/_sys/`：随 Region 快照整域导出/恢复；只由 `ApplyCommitDecisions` 的
/// 同一条 redb 事务推进，崩溃后从游标续跑。
const DECISION_APPLIED_TS: &[u8] = b"/_sys/kv_t/decision_applied";

/// 完整决策记录（#148 应用、#153 CommitRecord 的上游输入）。
///
/// 只增不改：同一 `commit_ts` 不重写；`txn_id`/`request_id` 用于幂等排查。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequencerDecision {
    pub txn_id: Vec<u8>,
    pub request_id: Vec<u8>,
    pub start_ts: u64,
    pub commit_ts: u64,
    pub mutations: Vec<crate::raft::type_config::SequencerMutation>,
}

/// apply 的两种来源：旧日志（无记录元数据）与 #153 新日志（带记录元数据）。
enum DecisionView<'a> {
    Plain(&'a crate::raft::type_config::CommitDecisionApply),
    Logged(&'a crate::raft::type_config::CommitDecisionApplyLog),
}

impl DecisionView<'_> {
    fn commit_ts(&self) -> Revision {
        match self {
            Self::Plain(decision) => decision.commit_ts,
            Self::Logged(decision) => decision.commit_ts,
        }
    }

    fn mutations(&self) -> &[crate::raft::type_config::SequencerMutation] {
        match self {
            Self::Plain(decision) => &decision.mutations,
            Self::Logged(decision) => &decision.mutations,
        }
    }

    fn logged(&self) -> Option<&crate::raft::type_config::CommitDecisionApplyLog> {
        match self {
            Self::Plain(_) => None,
            Self::Logged(decision) => Some(decision),
        }
    }
}

/// 记录旧值（MVCC 落盘字节，加密时即同一份密文）；逻辑删除的 key 没有 prev。
fn commit_prev_value(
    tx: &dyn ReadTx,
    internal_key: &[u8],
    meta: &KvMetadata,
) -> Result<Option<CommitPrevValue>> {
    if meta.deleted {
        return Ok(None);
    }
    Ok(tx
        .get(TABLE_KV, internal_key)?
        .map(|value| CommitPrevValue {
            value,
            create_revision: meta.create_revision,
            mod_revision: meta.mod_revision,
            version: meta.version,
            lease_id: meta.lease_id,
        }))
}

/// `apply_commit_decisions` 的进度：游标推进到哪、本次推进了几条决策。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitDecisionApplyResult {
    pub through_ts: Revision,
    pub decisions: u64,
}

/// 一次命中校验索引的写（该 key 的最大 `commit_ts`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequencerIndexHit {
    pub key: Vec<u8>,
    pub commit_ts: Revision,
}

/// 区间写索引项：`[start, end)` 在 `commit_ts` 被整段写（删除）。
///
/// 存完整边界而不只是哈希，冲突诊断能回报区间起点；同一 `(start, end)` 只保留
/// 最大 `commit_ts`。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct SequencerIntervalWrite {
    start: Vec<u8>,
    end: Vec<u8>,
    commit_ts: Revision,
}

/// range 谓词校验查询的结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SequencerRangeQuery {
    Hit(SequencerIndexHit),
    Clear,
    /// 区间内的索引项超过扫描预算：调用方必须 fail-closed（#149 拒绝事务），
    /// 不能当作 `Clear`。`scanned` 是本次实际检查的上界数。
    Exceeded {
        scanned: usize,
    },
}

/// 索引追赶/重建的统计（启动日志与运维排查）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SequencerIndexRebuild {
    pub from_ts: u64,
    pub to_ts: u64,
    pub decisions: u64,
    pub keys: u64,
}

#[cfg(test)]
thread_local! {
    /// 区间索引扫描次数：钉住「每事务最多扫一次」的复杂度契约。
    static INTERVAL_INDEX_SCANS: std::cell::Cell<usize> = std::cell::Cell::new(0);
}

/// 带域分隔与长度前缀的 SHA-256（避免 `("ab","c")` 与 `("a","bc")` 撞键）。
fn sequencer_hash(domain: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    for part in parts {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part);
    }
    hasher.finalize().into()
}

fn prefixed_hash_key(prefix: &[u8], hash: &[u8; 32]) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + hash.len());
    key.extend_from_slice(prefix);
    key.extend_from_slice(hash);
    key
}

fn prefixed_key(prefix: &[u8], key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(prefix.len() + key.len());
    out.extend_from_slice(prefix);
    out.extend_from_slice(key);
    out
}

/// 前缀的字典序上界（不含）：把最后一个非 `0xFF` 字节加一。
///
/// 空结果表示「无上界」（仅当整个前缀都是 `0xFF`）；校验索引前缀以 `/`
/// 结尾，因此总有界。
fn prefix_upper_bound(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if let Some(next) = last.checked_add(1) {
            end.push(next);
            return end;
        }
    }
    Vec::new()
}

fn encode_sequencer_decision_key(commit_ts: Revision) -> Vec<u8> {
    let mut key = Vec::with_capacity(SEQUENCER_DECISION_PREFIX.len() + 8);
    key.extend_from_slice(SEQUENCER_DECISION_PREFIX);
    key.extend_from_slice(&commit_ts.to_be_bytes());
    key
}

/// 决策重放判定：同 `commit_ts` 的 changelog 必须与决策内容逐项一致。
///
/// 含 `DeleteRange` 的决策一律返回 false：区间删除产生的 change 条数取决于
/// apply 时存活的 key，无法从 mutation 反推。游标与 changelog 同事务写入，
/// 正常重放会在 `commit_ts <= 游标` 处跳过；能走到这里的只可能是同 ts 已被
/// 非决策写占用，停在该决策前（fail-closed）比误判「已应用」安全。
fn event_matches_mutations(
    event: &ChangeEvent,
    mutations: &[crate::raft::type_config::SequencerMutation],
) -> bool {
    use crate::raft::type_config::SequencerMutation;

    if mutations
        .iter()
        .any(|mutation| matches!(mutation, SequencerMutation::DeleteRange { .. }))
    {
        return false;
    }
    event.changes.len() == mutations.len()
        && event
            .changes
            .iter()
            .zip(mutations)
            .all(|(change, mutation)| match mutation {
                SequencerMutation::Put { key, value, .. } => {
                    change.key == *key && change.value.as_deref() == Some(value.as_slice())
                }
                SequencerMutation::Delete { key } => change.key == *key && change.value.is_none(),
                SequencerMutation::DeleteRange { .. } => false,
            })
}

// ──── SafeTs fence（region 0 system raft，#142）────

/// 已发布的安全点 fence（u64 BE；缺失 = 0）。
///
/// 选 `/_sys/`：快照按 `/_sys/*` 整域携带，恢复后不得回退（否则已经拒绝过的
/// 旧 `commit_ts` 会在恢复后重新被接受）。不在 `/_meta/` 是因为快照只逐项挑选
/// meta 键，`/_sys/` 是整域导出。
const SAFE_TS_FENCE: &[u8] = b"/_sys/kv_t/safe_ts_fence";
const COMMIT_WATERMARK: &[u8] = b"/_sys/commit_log/watermark";

/// 单次 TSO 批次分配记录（重放幂等：同 revision 必须返回同一区间）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct TsoBatchRecord {
    revision: u64,
    start_ts: u64,
    end_ts: u64,
}

/// 队列键：`/_pd/ops/{op_id:u64be}`（定宽，前缀迭代即 op_id 升序）
pub(crate) fn encode_pd_op_key(op_id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(PD_QUEUE_PREFIX.len() + 8);
    key.extend_from_slice(PD_QUEUE_PREFIX);
    key.extend_from_slice(&op_id.to_be_bytes());
    key
}

// ──── Key 编码工具 ────

/// 将用户 Key 编码为内部存储格式：/kv/{user_key}
pub fn encode_kv_key(user_key: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(KV_PREFIX.len() + user_key.len());
    encoded.extend_from_slice(KV_PREFIX);
    encoded.extend_from_slice(user_key);
    encoded
}

/// 将内部存储格式解码为用户 Key。若非 /kv/ 前缀则返回 None。
pub fn decode_kv_key(internal_key: &[u8]) -> Option<&[u8]> {
    internal_key.strip_prefix(KV_PREFIX)
}

/// 将 Revision 编码为 Changelog Key：/_changelog/{revision_be}
pub(crate) fn encode_changelog_key(revision: Revision) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(CHANGELOG_PREFIX.len() + 8);
    encoded.extend_from_slice(CHANGELOG_PREFIX);
    encoded.extend_from_slice(&revision.to_be_bytes());
    encoded
}

/// 将 Revision 编码为大端字节
pub fn revision_to_bytes(revision: Revision) -> [u8; 8] {
    revision.to_be_bytes()
}

// ──── ChangeEvent ────

/// 变更事件类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    Put = 0,
    Delete = 1,
    Txn = 2,
    /// Lease 生命周期事件（Grant/KeepAlive/Revoke）
    Lease = 3,
}

/// 单条 Key-Value 变更记录
#[derive(Debug, Clone)]
pub struct KeyValueChange {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub prev_value: Option<Vec<u8>>,
}

/// Changelog 条目：一条 Apply 操作产生的所有变更
#[derive(Debug, Clone)]
pub struct ChangeEvent {
    pub revision: Revision,
    pub changes: Vec<KeyValueChange>,
    pub event_type: EventType,
}

/// Changelog 格式版本（版本号 +1，0.1.x 数据不承诺兼容）
const CHANGELOG_FORMAT_VERSION: u8 = 2;

impl ChangeEvent {
    /// 序列化为字节
    ///
    /// v2 格式：version(1) | revision(8BE) | event_type(1) | num_changes(4BE) | [key_len(4BE)|key|has_value(1)|value...]
    /// v1（旧）格式：revision(8BE) | event_type(1) | ...，首个字节 0/1/2 可判别。
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(CHANGELOG_FORMAT_VERSION);
        buf.extend_from_slice(&self.revision.to_be_bytes());
        buf.push(self.event_type as u8);
        buf.extend_from_slice(&(self.changes.len() as u32).to_be_bytes());
        for change in &self.changes {
            buf.extend_from_slice(&(change.key.len() as u32).to_be_bytes());
            buf.extend_from_slice(&change.key);
            match &change.value {
                Some(v) => {
                    buf.push(1);
                    buf.extend_from_slice(&(v.len() as u32).to_be_bytes());
                    buf.extend_from_slice(v);
                }
                None => {
                    buf.push(0);
                }
            }
        }
        buf
    }

    /// 从字节反序列化（兼容 v1 旧格式）
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < 13 {
            return Err(Error::DataCorruption("change event too short".into()));
        }
        // 判别格式：v2 首个字节为版本号 2；v1 旧格式首个字节为 event_type（0/1/2）
        let (revision_start, event_type_pos) = if data[0] == CHANGELOG_FORMAT_VERSION {
            (1usize, 9usize)
        } else {
            (0usize, 8usize)
        };
        let revision = match data[revision_start..revision_start + 8].try_into() {
            Ok(bytes) => Revision::from_be_bytes(bytes),
            Err(_) => return Err(Error::DataCorruption("truncated revision".into())),
        };
        let event_type = match data[event_type_pos] {
            0 => EventType::Put,
            1 => EventType::Delete,
            2 => EventType::Txn,
            3 => EventType::Lease,
            t => return Err(Error::DataCorruption(format!("unknown event type: {}", t))),
        };
        let num_changes = match data[event_type_pos + 1..event_type_pos + 5].try_into() {
            Ok(bytes) => u32::from_be_bytes(bytes) as usize,
            Err(_) => return Err(Error::DataCorruption("truncated change count".into())),
        };

        let mut changes = Vec::with_capacity(num_changes);
        let mut offset = event_type_pos + 5;
        for _ in 0..num_changes {
            if offset + 4 > data.len() {
                return Err(Error::DataCorruption("truncated change".into()));
            }
            let key_len = match data[offset..offset + 4].try_into() {
                Ok(bytes) => u32::from_be_bytes(bytes) as usize,
                Err(_) => return Err(Error::DataCorruption("truncated key length".into())),
            };
            offset += 4;
            if offset + key_len > data.len() {
                return Err(Error::DataCorruption("truncated key".into()));
            }
            let key = data[offset..offset + key_len].to_vec();
            offset += key_len;

            if offset >= data.len() {
                return Err(Error::DataCorruption("missing value flag".into()));
            }
            let has_value = data[offset] == 1;
            offset += 1;

            let value = if has_value {
                if offset + 4 > data.len() {
                    return Err(Error::DataCorruption("truncated value len".into()));
                }
                let val_len = match data[offset..offset + 4].try_into() {
                    Ok(bytes) => u32::from_be_bytes(bytes) as usize,
                    Err(_) => return Err(Error::DataCorruption("truncated value length".into())),
                };
                offset += 4;
                if offset + val_len > data.len() {
                    return Err(Error::DataCorruption("truncated value".into()));
                }
                let v = data[offset..offset + val_len].to_vec();
                offset += val_len;
                Some(v)
            } else {
                None
            };

            changes.push(KeyValueChange {
                key,
                value,
                prev_value: None,
            });
        }

        Ok(Self {
            revision,
            changes,
            event_type,
        })
    }
}

// ──── KvMetadata ────

/// 单个 Key 的元数据，用于 Version / Revision 追踪和 Lease 绑定
#[derive(Debug, Clone, Copy)]
pub struct KvMetadata {
    /// Key 被修改次数（从 1 开始）
    pub version: i64,
    /// Key 创建时的 Revision
    pub create_revision: i64,
    /// Key 最后修改的 Revision
    pub mod_revision: i64,
    /// 关联的 Lease ID（0 表示无 Lease）
    pub lease_id: i64,
    /// 是否已删除（true 表示该 Key 已被逻辑删除）
    pub deleted: bool,
}

/// 历史/实时 Range 返回的一行：`(key, value, metadata)`。
pub type VersionedKv = (Vec<u8>, Vec<u8>, KvMetadata);

impl KvMetadata {
    /// 序列化为 33 字节固定格式：
    /// version(8BE) | create_revision(8BE) | mod_revision(8BE) | lease_id(8BE) | deleted(1)
    pub fn to_bytes(&self) -> [u8; 33] {
        let mut buf = [0u8; 33];
        buf[0..8].copy_from_slice(&self.version.to_be_bytes());
        buf[8..16].copy_from_slice(&self.create_revision.to_be_bytes());
        buf[16..24].copy_from_slice(&self.mod_revision.to_be_bytes());
        buf[24..32].copy_from_slice(&self.lease_id.to_be_bytes());
        buf[32] = if self.deleted { 1 } else { 0 };
        buf
    }

    /// 从字节反序列化（兼容 32 字节旧格式：默认 deleted=false）
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 32 {
            return None;
        }
        let deleted = if bytes.len() >= 33 {
            bytes[32] == 1
        } else {
            false
        };
        Some(Self {
            version: i64::from_be_bytes(bytes[0..8].try_into().ok()?),
            create_revision: i64::from_be_bytes(bytes[8..16].try_into().ok()?),
            mod_revision: i64::from_be_bytes(bytes[16..24].try_into().ok()?),
            lease_id: i64::from_be_bytes(bytes[24..32].try_into().ok()?),
            deleted,
        })
    }

    /// 创建新 Key 的初始元数据
    pub fn new_key(revision: Revision, lease_id: i64) -> Self {
        Self {
            version: 1,
            create_revision: revision as i64,
            mod_revision: revision as i64,
            lease_id,
            deleted: false,
        }
    }

    /// 更新已有 Key 的元数据（递增 version，更新 mod_revision）
    pub fn update(&self, revision: Revision, lease_id: i64) -> Self {
        Self {
            version: self.version + 1,
            create_revision: self.create_revision,
            mod_revision: revision as i64,
            lease_id,
            deleted: false,
        }
    }

    /// 标记 Key 为已删除
    pub fn mark_deleted(&self, revision: Revision) -> Self {
        Self {
            version: self.version + 1,
            create_revision: self.create_revision,
            mod_revision: revision as i64,
            lease_id: self.lease_id,
            deleted: true,
        }
    }
}

// ──── AppliedLogId ────

/// 持久化的已 Apply LogId（与命令写入同一事务）
///
/// raft apply 路径写入 `{term, node_id}` 来自 `entry.log_id`；
/// 单节点模式（无 raft）写入 `{0, 0, revision}`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedLogId {
    pub term: u64,
    pub node_id: u64,
    pub index: u64,
}

impl AppliedLogId {
    /// 单节点模式（无 raft）的合成 LogId
    pub fn standalone(index: u64) -> Self {
        Self {
            term: 0,
            node_id: 0,
            index,
        }
    }

    pub(crate) fn to_bytes(self) -> Vec<u8> {
        // **手写定长编码**（4B 魔数 + 3×8B 大端），不得有失败路径。
        //
        // 若采用「序列化失败降级为空值」的写法：
        // 失败会写入**空字节**，而这段字节会成为 `META_LAST_APPLIED`
        // 的水位；`from_bytes` 对空字节返回 `None` → 重启时水位报 0 →
        // **从 0 重放整个 raft 日志**——把可检测的错误转成静默的状态损坏。
        //
        // 手写编码既不需要 `unwrap/expect`（生产代码 panic 校验禁止），
        // 也不可能退化成空值。
        let mut out = Vec::with_capacity(28);
        out.extend_from_slice(APPLIED_LOG_ID_MAGIC);
        out.extend_from_slice(&self.term.to_be_bytes());
        out.extend_from_slice(&self.node_id.to_be_bytes());
        out.extend_from_slice(&self.index.to_be_bytes());
        out
    }

    /// 解码。P3：只接受 `ALI1` 手写定长编码（旧 bincode 回退已随退场删除；
    /// 魔数/长度不符 ⇒ None，读取方按「未持久化」处理并由日志重放兜底）。
    pub(crate) fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let body = bytes.strip_prefix(APPLIED_LOG_ID_MAGIC)?;
        let term = u64::from_be_bytes(body.get(0..8)?.try_into().ok()?);
        let node_id = u64::from_be_bytes(body.get(8..16)?.try_into().ok()?);
        let index = u64::from_be_bytes(body.get(16..24)?.try_into().ok()?);
        Some(Self {
            term,
            node_id,
            index,
        })
    }
}

// ──── LeaseRecord（raft 状态机内持久化 lease 表） ────

/// `/_lease/{id}` 的持久化记录
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseRecord {
    /// 租约 TTL（秒）
    pub ttl: i64,
    /// 租约过期墙钟时刻（epoch 毫秒，grant/keepalive 时由 leader 计算后入日志）
    pub deadline_wall_ms: i64,
    /// 最后一次续约的 revision（log index）
    pub keepalive_revision: i64,
}

impl LeaseRecord {
    fn to_bytes(self) -> [u8; 24] {
        let mut buf = [0u8; 24];
        buf[0..8].copy_from_slice(&self.ttl.to_be_bytes());
        buf[8..16].copy_from_slice(&self.deadline_wall_ms.to_be_bytes());
        buf[16..24].copy_from_slice(&self.keepalive_revision.to_be_bytes());
        buf
    }

    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 24 {
            return None;
        }
        Some(Self {
            ttl: i64::from_be_bytes(bytes[0..8].try_into().ok()?),
            deadline_wall_ms: i64::from_be_bytes(bytes[8..16].try_into().ok()?),
            keepalive_revision: i64::from_be_bytes(bytes[16..24].try_into().ok()?),
        })
    }
}

/// v2 租约表前缀（与 v1 `/_lease/` 分离：v1 记录携带 log-index revision，
/// v2 记录不参与 revision，清理使用 TSO `commit_ts`）。
const LEASE_V2_PREFIX: &[u8] = b"/_leasev2/";

/// v2 Grant 的 request_id 幂等索引前缀（独立前缀，避免混入租约记录枚举）。
const LEASE_V2_REQ_PREFIX: &[u8] = b"/_leasev2req/";

/// 将 Lease ID 编码为内部存储 Key：/_lease/{id_be}
pub(crate) fn encode_lease_key(lease_id: i64) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(LEASE_PREFIX.len() + 8);
    encoded.extend_from_slice(LEASE_PREFIX);
    encoded.extend_from_slice(&lease_id.to_be_bytes());
    encoded
}

// ──── LeaseV2Record（region 0 的 v2 租约表） ────

/// `/_leasev2/{id}` 的持久化记录。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseV2Record {
    /// 租约 TTL（秒）
    pub ttl: i64,
    /// 发放时的 TSO（只读参考，不参与 revision 语义）
    pub granted_ts: u64,
    /// 过期墙钟时刻（epoch 毫秒；leader 计算后入日志，apply 确定性）
    pub deadline_wall_ms: i64,
    /// 是否已吊销/已过期清理
    pub revoked: bool,
    /// 吊销决策的 `commit_ts`（0 = 未吊销；清理重试可推进）
    pub revoked_commit_ts: u64,
    /// 首次 Grant 的 request_id（幂等回放；空 = 客户端未提供）
    pub grant_request_id: Vec<u8>,
}

impl LeaseV2Record {
    fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(35 + self.grant_request_id.len());
        buf.extend_from_slice(&self.ttl.to_be_bytes());
        buf.extend_from_slice(&self.granted_ts.to_be_bytes());
        buf.extend_from_slice(&self.deadline_wall_ms.to_be_bytes());
        buf.push(u8::from(self.revoked));
        buf.extend_from_slice(&self.revoked_commit_ts.to_be_bytes());
        let len = self.grant_request_id.len().min(u16::MAX as usize);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
        buf.extend_from_slice(&self.grant_request_id[..len]);
        buf
    }

    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 35 {
            return None;
        }
        let req_len = u16::from_be_bytes(bytes[33..35].try_into().ok()?) as usize;
        if bytes.len() < 35 + req_len {
            return None;
        }
        Some(Self {
            ttl: i64::from_be_bytes(bytes[0..8].try_into().ok()?),
            granted_ts: u64::from_be_bytes(bytes[8..16].try_into().ok()?),
            deadline_wall_ms: i64::from_be_bytes(bytes[16..24].try_into().ok()?),
            revoked: bytes[24] != 0,
            revoked_commit_ts: u64::from_be_bytes(bytes[25..33].try_into().ok()?),
            grant_request_id: bytes[35..35 + req_len].to_vec(),
        })
    }
}

fn encode_lease_v2_key(lease_id: i64) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(LEASE_V2_PREFIX.len() + 8);
    encoded.extend_from_slice(LEASE_V2_PREFIX);
    encoded.extend_from_slice(&lease_id.to_be_bytes());
    encoded
}

fn encode_lease_v2_req_key(request_id: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(LEASE_V2_REQ_PREFIX.len() + request_id.len());
    encoded.extend_from_slice(LEASE_V2_REQ_PREFIX);
    encoded.extend_from_slice(request_id);
    encoded
}

/// v2 租约 apply 结果（状态机据此构造 response）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseV2Apply {
    Grant {
        lease_id: i64,
        granted_ts: u64,
        ttl: i64,
        created: bool,
    },
    KeepAlive {
        ok: bool,
    },
    Revoke {
        commit_ts: u64,
    },
}

// ──── ApplyOutcome ────

/// apply 结果：是否因幂等守卫跳过了实际写入
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApplyOutcome {
    /// true = 该 revision 的 changelog 已存在，本次为重放，未产生副作用
    pub replayed: bool,
}

impl ApplyOutcome {
    pub fn applied() -> Self {
        Self { replayed: false }
    }

    pub fn replayed() -> Self {
        Self { replayed: true }
    }
}

/// 范围删除（DeleteRange，R-SVC-07）apply 结果
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeleteRangeOutcome {
    /// 实际被标记删除的 Key 列表（不含已删除/不存在的 Key）
    pub deleted_keys: Vec<Vec<u8>>,
    /// true = 该 revision 的 changelog 已存在，本次为重放，未产生副作用
    pub replayed: bool,
}

/// 历史 Range 回放的中间视图：值 + 元数据重建所需计数。
struct HistoricalRangeView {
    view: std::collections::BTreeMap<Vec<u8>, Vec<u8>>,
    /// key → 最后一次 <= target revision 的写入 revision（含 delete）
    last_write: std::collections::BTreeMap<Vec<u8>, Revision>,
    /// key → target revision 之后的写入事件 `(revision, is_delete)`（version 回推用）
    post_events: std::collections::HashMap<Vec<u8>, Vec<(Revision, bool)>>,
}

// ──── Compact ────

/// 单次 compaction 批删除上限（单写事务分片删除，避免巨型事务）
pub(crate) const COMPACT_BATCH_SIZE: usize = 500;

/// Compact apply 结果统计
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactOutcome {
    /// 被删除的 changelog 条目数
    pub deleted_changelog: usize,
    /// 被物理删除的 tombstone 数（KV 行 + 元数据行）
    pub deleted_tombstones: usize,
}

/// TSO 批次分配 apply 结果
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TsoBatchOutcome {
    /// 批次左闭右开起点（`start_ts` 可用）
    pub start_ts: u64,
    /// 批次终点（`end_ts` 不可用）
    pub end_ts: u64,
    /// true = 该 revision 已 apply，本次为重放（区间不变、高水位不推进）
    pub replayed: bool,
}

/// 定长 u64 BE 解码（`META_CURRENT_REVISION` / `META_COMPACT_REVISION`）。
fn decode_u64(bytes: &[u8]) -> Option<u64> {
    <[u8; 8]>::try_from(bytes).ok().map(u64::from_be_bytes)
}

// ──── MvccStorage ────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RevisionPolicy {
    LogIndex,
    CommitTs,
}

/// revision 来源（ADR-0015 D3）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RevisionMode {
    /// v1：revision ≡ raft log index（默认；ADR-0001 行为字节级不变）。
    LogIndex,
    /// v2：revision = 显式 `commit_ts`；log-index 记录只推进 applied。
    CommitTs,
}

/// MVCC 版本化存储
///
/// 在 StorageBackend 之上提供应用层 MVCC 语义：
/// - v1：revision ≡ raft log index；v2：revision = `commit_ts`
/// - Changelog 自动写入
/// - applied 状态同事务持久化
/// - Lease 状态表
///
/// 单实例语义：全链路共享一个实例；读走 redb 读事务（天然读已提交）。
pub struct MvccStorage<B: StorageBackend> {
    backend: B,
    /// 可选的存储屏障（用于 Value 加密/解密）
    barrier: RwLock<Option<Barrier>>,
    /// 单节点模式（无 raft）的 revision 分配锁：保证并发写入分配不同 revision
    /// （raft 模式下 apply 以 log index 为 revision，不需要此锁）
    standalone_lock: parking_lot::Mutex<()>,
    revision_mode: RevisionMode,
}

impl<B: StorageBackend> MvccStorage<B> {
    /// 创建 v1 实例（revision ≡ raft log index）。
    ///
    /// revision 不再从元数据恢复（revision 由 raft apply 传入）。
    /// 启动一致性校验由 `verify_consistency` 显式执行。
    pub fn new(backend: B) -> Result<Self> {
        Self::new_with_revision_mode(backend, RevisionMode::LogIndex)
    }

    /// 创建指定 revision 语义的实例（v2 集群用 `CommitTs`）。
    pub fn new_with_revision_mode(backend: B, revision_mode: RevisionMode) -> Result<Self> {
        Ok(Self {
            backend,
            barrier: RwLock::new(None),
            standalone_lock: parking_lot::Mutex::new(()),
            revision_mode,
        })
    }

    pub fn revision_mode(&self) -> RevisionMode {
        self.revision_mode
    }

    /// 设置存储屏障（在 Keyring 初始化后调用）
    ///
    /// 设置后，所有 `/kv/` 下的 Value 写入前加密、读取后解密。
    /// 元数据（/_meta/）不受屏障影响。
    pub fn set_barrier(&self, barrier: Barrier) {
        *self.barrier.write() = Some(barrier);
    }

    /// 加密 Value（如果 Barrier 已设置）。仅加密 `/kv/` 用户数据——
    /// `/_lease/`、`/_sys/` 等内部结构化记录（TABLE_KV 内）不加密，否则
    /// `LeaseRecord::from_bytes` 等解析会因密文长度/内容不符而失败。
    fn encrypt_value(&self, internal_key: &[u8], value: &[u8]) -> Result<Vec<u8>> {
        if !internal_key.starts_with(KV_PREFIX) {
            return Ok(value.to_vec());
        }
        match self.barrier.read().as_ref() {
            Some(barrier) => barrier.encrypt(value),
            None => Ok(value.to_vec()),
        }
    }

    /// 解密 Value（如果 Barrier 已设置）。仅解密 `/kv/` 用户数据；
    /// 短密文（<32B，legacy 明文或内部记录）透传。
    fn decrypt_value(&self, internal_key: &[u8], encrypted: &[u8]) -> Result<Vec<u8>> {
        if !internal_key.starts_with(KV_PREFIX) {
            return Ok(encrypted.to_vec());
        }
        match self.barrier.read().as_ref() {
            Some(barrier) => {
                // Check if this looks like encrypted data (has key_id prefix)
                if encrypted.len() >= 32 {
                    barrier.decrypt(encrypted)
                } else {
                    // Plaintext (legacy data or meta), return as-is
                    Ok(encrypted.to_vec())
                }
            }
            None => Ok(encrypted.to_vec()),
        }
    }

    /// 获取底层 StorageBackend 的引用（用于只读操作）
    pub fn backend(&self) -> &B {
        &self.backend
    }

    /// 获取当前 Revision（已提交的最大 Revision）。
    ///
    /// 读盘上 `META_CURRENT_REVISION`。既有 v1 库没有该行：回退到
    /// `META_LAST_APPLIED.index`（revision 曾等于 log index），下一次写入补齐。
    pub fn current_revision(&self) -> Revision {
        let persisted = self
            .backend
            .read(|tx| tx.get(TABLE_META, META_CURRENT_REVISION))
            .ok()
            .flatten()
            .and_then(|bytes| decode_u64(&bytes));
        if let Some(revision) = persisted {
            return revision;
        }
        match self.revision_mode {
            RevisionMode::LogIndex => self
                .get_applied_log_id()
                .ok()
                .flatten()
                .map(|a| a.index)
                .unwrap_or(0),
            RevisionMode::CommitTs => 0,
        }
    }

    /// 写事务内读取当前 revision（缺失时按持久化 applied 回退；CommitTs 模式为 0）。
    fn current_revision_in_tx(tx: &dyn ReadTx, mode: RevisionMode) -> Result<Revision> {
        if let Some(bytes) = tx.get(TABLE_META, META_CURRENT_REVISION)? {
            if let Some(revision) = decode_u64(&bytes) {
                return Ok(revision);
            }
        }
        match mode {
            RevisionMode::LogIndex => Ok(tx
                .get(TABLE_META, META_LAST_APPLIED)?
                .and_then(|bytes| AppliedLogId::from_bytes(&bytes))
                .map(|a| a.index)
                .unwrap_or(0)),
            RevisionMode::CommitTs => Ok(0),
        }
    }

    /// v1 语义：revision 必须是 log index 且单调；v2 下 log-index 记录
    /// （auth/lease/PD/TSO/object store）只推进 applied，不进入 revision 空间。
    fn accept_log_index_revision(&self, tx: &mut dyn WriteTx, revision: Revision) -> Result<()> {
        if self.revision_mode == RevisionMode::CommitTs {
            return Ok(());
        }
        let current = Self::current_revision_in_tx(tx, self.revision_mode)?;
        if revision <= current {
            return Err(Error::CommitRejected {
                commit_ts: revision,
                current_revision: current,
            });
        }
        tx.insert(TABLE_META, META_CURRENT_REVISION, &revision.to_be_bytes())
    }

    /// v2 语义：`commit_ts` 必须大于 current revision（D11.4；重放已由
    /// changelog 守卫提前返回）。
    fn accept_commit_ts(&self, tx: &mut dyn WriteTx, commit_ts: Revision) -> Result<()> {
        let current = Self::current_revision_in_tx(tx, self.revision_mode)?;
        if commit_ts <= current {
            return Err(Error::CommitRejected {
                commit_ts,
                current_revision: current,
            });
        }
        tx.insert(TABLE_META, META_CURRENT_REVISION, &commit_ts.to_be_bytes())
    }

    fn accept_revision(
        &self,
        tx: &mut dyn WriteTx,
        revision: Revision,
        policy: RevisionPolicy,
    ) -> Result<()> {
        match policy {
            RevisionPolicy::LogIndex => self.accept_log_index_revision(tx, revision),
            RevisionPolicy::CommitTs => self.accept_commit_ts(tx, revision),
        }
    }

    /// 内部 log-index 记录（lease/auth/PD/TSO/compact 等）：v1 下同步推进
    /// revision（保持 revision == applied index），v2 下不触碰 revision。
    ///
    /// 只前进不后退：重放/合成测试可能传入更低的 applied，revision 单调是
    /// ADR-0015 D11.2 的不变量。
    fn note_log_index_revision(&self, tx: &mut dyn WriteTx, revision: Revision) -> Result<()> {
        if self.revision_mode == RevisionMode::LogIndex {
            let current = Self::current_revision_in_tx(tx, RevisionMode::LogIndex)?;
            if revision > current {
                tx.insert(TABLE_META, META_CURRENT_REVISION, &revision.to_be_bytes())?;
            }
        }
        Ok(())
    }

    /// 读取持久化的已 Apply LogId（`META_LAST_APPLIED`）
    pub fn get_applied_log_id(&self) -> Result<Option<AppliedLogId>> {
        self.backend
            .read(|tx| tx.get(TABLE_META, META_LAST_APPLIED))
            .map(|opt| opt.and_then(|bytes| AppliedLogId::from_bytes(&bytes)))
    }

    /// 单独持久化 applied LogId（仅用于 Membership/Blank 等不写 KV 事务的条目）
    pub fn set_last_applied(&self, applied: AppliedLogId) -> Result<()> {
        self.backend.write(|tx| {
            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;
            self.note_log_index_revision(tx, applied.index)
        })
    }

    /// 检查某 revision 的 changelog 条目是否已存在（幂等守卫）
    pub fn changelog_contains_revision(&self, revision: Revision) -> Result<bool> {
        self.backend.read(|tx| {
            tx.get(TABLE_CHANGELOG, &encode_changelog_key(revision))
                .map(|opt| opt.is_some())
        })
    }

    /// 读取某 revision 的 changelog 事件（重放匹配用）。
    fn changelog_event_at(&self, revision: Revision) -> Result<Option<ChangeEvent>> {
        let Some(bytes) = self
            .backend
            .read(|tx| tx.get(TABLE_CHANGELOG, &encode_changelog_key(revision)))?
        else {
            return Ok(None);
        };
        ChangeEvent::from_bytes(&bytes).map(Some)
    }

    /// `[start, range_end)` 内仍存活（未删除）的 Key 数（上限 1 即可）。
    fn range_has_live_keys(&self, start: &[u8], range_end: &[u8]) -> Result<bool> {
        Ok(!self.range_in(start, range_end, 1)?.is_empty())
    }

    /// `[start, range_end)` 内 `mod_revision == revision` 且已删除的 Key 数。
    ///
    /// 用于 DeleteRange 重放校验：同一 commit_ts 的重复删除必须对应**同一组**
    /// tombstone，不能把“新范围里的其他 key”静默吞掉。
    fn count_deleted_at_revision(
        &self,
        start: &[u8],
        range_end: &[u8],
        revision: Revision,
    ) -> Result<usize> {
        self.backend.read(|tx| {
            let mut count = 0usize;
            for (meta_key, meta_value) in tx.iter_prefix(TABLE_KV_META, KV_META_PREFIX)? {
                let Some(user_key) = meta_key.strip_prefix(KV_META_PREFIX) else {
                    continue;
                };
                if user_key < start {
                    continue;
                }
                if !range_end.is_empty() && user_key >= range_end {
                    continue;
                }
                if let Some(meta) = KvMetadata::from_bytes(&meta_value) {
                    if meta.deleted && meta.mod_revision as u64 == revision {
                        count += 1;
                    }
                }
            }
            Ok(count)
        })
    }

    /// 启动一致性校验：`META_LAST_APPLIED` 与 changelog 尾部一致
    ///
    /// 返回（持久化 applied 索引、changelog 最大 revision）。不一致时调用方显式告警
    /// 并按"快照 → 日志"顺序恢复（重放由幂等守卫兜底）。
    pub fn verify_consistency(&self) -> Result<(u64, Option<u64>)> {
        let applied = self.get_applied_log_id()?.map(|a| a.index).unwrap_or(0);
        let changelog_tail = self.backend.read(|tx| {
            let entries = tx.iter_prefix(TABLE_CHANGELOG, CHANGELOG_PREFIX)?;
            let mut tail: Option<u64> = None;
            for (key, _) in entries {
                if key.len() >= CHANGELOG_PREFIX.len() + 8 {
                    if let Ok(rev_bytes) = key[key.len() - 8..].try_into() {
                        tail = Some(Revision::from_be_bytes(rev_bytes));
                    }
                }
            }
            Ok(tail)
        })?;
        Ok((applied, changelog_tail))
    }

    /// Put 操作（单节点模式入口）
    ///
    /// revision 取 `current_revision + 1`（在独立锁内分配，防止并发同 revision 冲突）；
    /// 集群模式应走 `put_at_revision`（raft apply 传入）。
    pub fn put(&self, key: &[u8], value: &[u8], lease_id: Option<LeaseID>) -> Result<Revision> {
        let _guard = self.standalone_lock.lock();
        let revision = self.current_revision().saturating_add(1);
        let applied = AppliedLogId::standalone(revision);
        self.put_at_revision(key, value, lease_id, revision, applied)?;
        Ok(revision)
    }

    /// Put 操作（raft apply 路径）：revision ≡ log index
    ///
    /// 幂等守卫：该 revision 的 changelog 已存在则跳过写入。
    /// 业务写入 + changelog + META_LAST_APPLIED 在同一写事务内原子完成。
    pub fn put_at_revision(
        &self,
        key: &[u8],
        value: &[u8],
        lease_id: Option<LeaseID>,
        revision: Revision,
        applied: AppliedLogId,
    ) -> Result<ApplyOutcome> {
        self.put_at_key(
            key,
            value,
            lease_id,
            revision,
            applied,
            RevisionPolicy::LogIndex,
        )
    }

    /// Put 操作（v2 直写路径）：revision = 调用方提供的 `commit_ts`。
    ///
    /// 同一 `commit_ts` 重放返回 replayed；`commit_ts <= current_revision`
    /// （且未命中已有写入）→ `CommitRejected`，不静默写（D11.4/D11.6）。
    pub fn put_at_commit_ts(
        &self,
        key: &[u8],
        value: &[u8],
        lease_id: Option<LeaseID>,
        commit_ts: Revision,
        applied: AppliedLogId,
    ) -> Result<ApplyOutcome> {
        if commit_ts == 0 {
            return Err(Error::InvalidArgument("commit_ts must be > 0".into()));
        }
        self.put_at_key(
            key,
            value,
            lease_id,
            commit_ts,
            applied,
            RevisionPolicy::CommitTs,
        )
    }

    fn put_at_key(
        &self,
        key: &[u8],
        value: &[u8],
        lease_id: Option<LeaseID>,
        revision: Revision,
        applied: AppliedLogId,
        policy: RevisionPolicy,
    ) -> Result<ApplyOutcome> {
        if policy == RevisionPolicy::CommitTs {
            if let Some(event) = self.changelog_event_at(revision)? {
                let same = event.changes.len() == 1
                    && event.changes[0].key.as_slice() == key
                    && event.changes[0].value.as_deref() == Some(value);
                if same {
                    return Ok(ApplyOutcome::replayed());
                }
                return Err(Error::CommitRejected {
                    commit_ts: revision,
                    current_revision: self.current_revision(),
                });
            }
        } else if self.changelog_contains_revision(revision)? {
            return Ok(ApplyOutcome::replayed());
        }
        let lid = lease_id.unwrap_or(0);
        self.backend.write(|tx| {
            self.accept_revision(tx, revision, policy)?;
            let internal_key = encode_kv_key(key);
            let meta_key = encode_kv_meta_key(key);

            // 读取已有元数据
            let existing_meta = tx
                .get(TABLE_KV_META, &meta_key)?
                .and_then(|bytes| KvMetadata::from_bytes(&bytes));

            // 更新元数据
            let meta = match existing_meta {
                Some(m) => m.update(revision, lid),
                None => KvMetadata::new_key(revision, lid),
            };

            // 写入用户数据（经过 Barrier 加密，仅 /kv/ 前缀）
            let encrypted = self.encrypt_value(&internal_key, value)?;
            tx.insert(TABLE_KV, &internal_key, &encrypted)?;

            // 写入 KV 元数据
            tx.insert(TABLE_KV_META, &meta_key, &meta.to_bytes())?;

            // 写入 Changelog
            let event = ChangeEvent {
                revision,
                changes: vec![KeyValueChange {
                    key: key.to_vec(),
                    value: Some(value.to_vec()),
                    prev_value: None,
                }],
                event_type: EventType::Put,
            };
            tx.insert(
                TABLE_CHANGELOG,
                &encode_changelog_key(revision),
                &event.to_bytes(),
            )?;

            // 持久化 applied 状态（与命令写入同一事务）
            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;

            Ok(ApplyOutcome::applied())
        })
    }

    /// Delete 操作（单节点模式入口）
    pub fn delete(&self, key: &[u8]) -> Result<Revision> {
        let _guard = self.standalone_lock.lock();
        let revision = self.current_revision().saturating_add(1);
        let applied = AppliedLogId::standalone(revision);
        self.delete_at_revision(key, revision, applied)?;
        Ok(revision)
    }

    /// 范围删除（单节点模式入口，R-SVC-07-3）：原子删除 `[start, range_end)` 内所有 Key
    ///
    /// 返回（revision, 实际删除的 Key 数）。
    pub fn delete_range(&self, start: &[u8], range_end: &[u8]) -> Result<(Revision, usize)> {
        let _guard = self.standalone_lock.lock();
        let revision = self.current_revision().saturating_add(1);
        let applied = AppliedLogId::standalone(revision);
        let outcome = self.delete_range_at_revision(start, range_end, revision, applied)?;
        Ok((revision, outcome.deleted_keys.len()))
    }

    /// Delete 操作（raft apply 路径）：revision ≡ log index
    ///
    /// no-op delete 统一语义：无论 Key 是否存在，始终消耗一个 revision
    /// 并写 changelog（与 etcd 一致），消除"回滚计数器"分支。
    pub fn delete_at_revision(
        &self,
        key: &[u8],
        revision: Revision,
        applied: AppliedLogId,
    ) -> Result<ApplyOutcome> {
        self.delete_at_key(key, revision, applied, RevisionPolicy::LogIndex)
    }

    /// Delete 操作（v2 直写路径）：revision = `commit_ts`；拒绝语义同
    /// [`Self::put_at_commit_ts`]。
    pub fn delete_at_commit_ts(
        &self,
        key: &[u8],
        commit_ts: Revision,
        applied: AppliedLogId,
    ) -> Result<ApplyOutcome> {
        if commit_ts == 0 {
            return Err(Error::InvalidArgument("commit_ts must be > 0".into()));
        }
        self.delete_at_key(key, commit_ts, applied, RevisionPolicy::CommitTs)
    }

    fn delete_at_key(
        &self,
        key: &[u8],
        revision: Revision,
        applied: AppliedLogId,
        policy: RevisionPolicy,
    ) -> Result<ApplyOutcome> {
        if policy == RevisionPolicy::CommitTs {
            if let Some(event) = self.changelog_event_at(revision)? {
                let same = event.changes.len() == 1
                    && event.changes[0].key.as_slice() == key
                    && event.changes[0].value.is_none();
                if same {
                    return Ok(ApplyOutcome::replayed());
                }
                return Err(Error::CommitRejected {
                    commit_ts: revision,
                    current_revision: self.current_revision(),
                });
            }
        } else if self.changelog_contains_revision(revision)? {
            return Ok(ApplyOutcome::replayed());
        }
        self.backend.write(|tx| {
            self.accept_revision(tx, revision, policy)?;
            let meta_key = encode_kv_meta_key(key);

            // 读取已有元数据（检查 Key 是否存在且未被删除）
            let existing_meta = tx
                .get(TABLE_KV_META, &meta_key)?
                .and_then(|bytes| KvMetadata::from_bytes(&bytes));

            if let Some(m) = existing_meta.filter(|m| !m.deleted) {
                // Key 存在且未被删除：标记为已删除
                let meta = m.mark_deleted(revision);
                tx.insert(TABLE_KV_META, &meta_key, &meta.to_bytes())?;
            }
            // 无论是否存在，均写 changelog（始终消耗 revision）

            let event = ChangeEvent {
                revision,
                changes: vec![KeyValueChange {
                    key: key.to_vec(),
                    value: None,
                    prev_value: None,
                }],
                event_type: EventType::Delete,
            };
            tx.insert(
                TABLE_CHANGELOG,
                &encode_changelog_key(revision),
                &event.to_bytes(),
            )?;

            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;

            Ok(ApplyOutcome::applied())
        })
    }

    /// 仅推进 `META_LAST_APPLIED`（小写事务；无状态变更的 Normal 条目
    /// （对象存储 no-op/冲突分支等）用它保证 applied 水位 == revision，
    /// 避免重启后整段重放）。
    pub fn persist_applied(&self, revision: Revision, applied: AppliedLogId) -> Result<()> {
        self.backend.write(|tx| {
            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;
            self.note_log_index_revision(tx, revision)
        })
    }

    /// Get 操作：读取单个 Key 的最新值（经过 Barrier 解密）
    ///
    /// 通过元数据的 deleted 标志区分空 value put 和删除 tombstone。
    /// 所有读取在单个读事务中完成。
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let internal_key = encode_kv_key(key);
        let meta_key = encode_kv_meta_key(key);
        self.backend.read(|tx| {
            let raw = tx.get(TABLE_KV, &internal_key)?;
            match raw {
                Some(data) => {
                    // 在同一事务内检查元数据的 deleted 标志
                    let meta = tx.get(TABLE_KV_META, &meta_key)?;
                    let is_deleted = meta
                        .and_then(|bytes| KvMetadata::from_bytes(&bytes))
                        .map(|m| m.deleted)
                        .unwrap_or(false);
                    if is_deleted {
                        Ok(None)
                    } else if data.is_empty() {
                        Ok(Some(Vec::new()))
                    } else {
                        Ok(Some(self.decrypt_value(&internal_key, &data)?))
                    }
                }
                None => Ok(None),
            }
        })
    }

    /// Range 操作：前缀扫描（返回解密后的 Value）
    ///
    /// 扫描以 prefix 为前缀的所有 Key，按 Key 字典序返回。
    /// 通过元数据的 deleted 标志过滤已删除的 Key。
    /// 所有读取在单个读事务中完成，避免嵌套事务问题。
    pub fn range(&self, prefix: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let internal_prefix = encode_kv_key(prefix);
        self.backend.read(|tx| {
            let all = tx.iter_prefix(TABLE_KV, &internal_prefix)?;
            let mut results = Vec::new();
            for (ik, v) in all {
                if results.len() >= limit && limit > 0 {
                    break;
                }
                if let Some(user_key) = decode_kv_key(&ik) {
                    // 在同一事务内检查元数据的 deleted 标志
                    let meta_key = encode_kv_meta_key(user_key);
                    let is_deleted = tx
                        .get(TABLE_KV_META, &meta_key)?
                        .and_then(|bytes| KvMetadata::from_bytes(&bytes))
                        .map(|m| m.deleted)
                        .unwrap_or(false);
                    if is_deleted {
                        continue;
                    }
                    let plaintext = if v.is_empty() {
                        Vec::new()
                    } else {
                        self.decrypt_value(&ik, &v)?
                    };
                    results.push((user_key.to_vec(), plaintext));
                }
            }
            Ok(results)
        })
    }

    /// Range 操作：半开区间 `[start, range_end)` 扫描（R-SVC-07，etcd 语义）
    ///
    /// 返回满足 `start <= user_key < range_end` 的所有 Key（按字典序）。
    /// 与 `range()`（前缀扫描）不同，本方法使用底层 `iter_range` 从 `start`
    /// 扫描到 `range_end`，区间内不共享 `start` 前缀的 Key 也能命中。
    /// 通过元数据的 deleted 标志过滤已删除的 Key。
    pub fn range_in(
        &self,
        start: &[u8],
        range_end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let internal_start = encode_kv_key(start);
        let internal_end = encode_kv_key(range_end);
        self.backend.read(|tx| {
            let all = tx.iter_range(TABLE_KV, &internal_start, &internal_end)?;
            let mut results = Vec::new();
            for (ik, v) in all {
                if results.len() >= limit && limit > 0 {
                    break;
                }
                if let Some(user_key) = decode_kv_key(&ik) {
                    // iter_range 已保证 [start, range_end)，此处防御性校验
                    if !range_end.is_empty() && user_key >= range_end {
                        break;
                    }
                    // 在同一事务内检查元数据的 deleted 标志
                    let meta_key = encode_kv_meta_key(user_key);
                    let is_deleted = tx
                        .get(TABLE_KV_META, &meta_key)?
                        .and_then(|bytes| KvMetadata::from_bytes(&bytes))
                        .map(|m| m.deleted)
                        .unwrap_or(false);
                    if is_deleted {
                        continue;
                    }
                    let plaintext = if v.is_empty() {
                        Vec::new()
                    } else {
                        self.decrypt_value(&ik, &v)?
                    };
                    results.push((user_key.to_vec(), plaintext));
                }
            }
            Ok(results)
        })
    }

    /// Range 实时读（含 revision 元数据）：值、元数据与 header revision 取自
    /// **同一个读事务**。
    ///
    /// 分两次读（先 current_revision、再 range_in）会让并发提交落在两者之间：
    /// header 说读到了 R，数据却缺 R 的写入（或读到了 header 之后才提交的数据）。
    /// v2 Range 的 `read_ts=0` 承诺多 key 原子可见，因此三者必须同快照。
    pub fn range_in_with_metadata(
        &self,
        start: &[u8],
        range_end: &[u8],
        limit: usize,
    ) -> Result<(Vec<VersionedKv>, Revision)> {
        let internal_start = encode_kv_key(start);
        let internal_end = encode_kv_key(range_end);
        self.backend.read(|tx| {
            let revision = Self::current_revision_in_tx(tx, self.revision_mode)?;
            let all = tx.iter_range(TABLE_KV, &internal_start, &internal_end)?;
            let mut results = Vec::new();
            for (ik, v) in all {
                if limit > 0 && results.len() >= limit {
                    break;
                }
                let Some(user_key) = decode_kv_key(&ik) else {
                    continue;
                };
                if !range_end.is_empty() && user_key >= range_end {
                    break;
                }
                let meta_key = encode_kv_meta_key(user_key);
                let meta = tx
                    .get(TABLE_KV_META, &meta_key)?
                    .and_then(|bytes| KvMetadata::from_bytes(&bytes))
                    .ok_or_else(|| {
                        Error::Internal(format!(
                            "key {} has no metadata; refusing to serve a v2 range read",
                            String::from_utf8_lossy(user_key)
                        ))
                    })?;
                if meta.deleted {
                    continue;
                }
                let plaintext = if v.is_empty() {
                    Vec::new()
                } else {
                    self.decrypt_value(&ik, &v)?
                };
                results.push((user_key.to_vec(), plaintext, meta));
            }
            Ok((results, revision))
        })
    }

    /// Range 历史读：返回 `[start, range_end)` 在目标 revision 时刻的视图（R-SVC-07-2）
    ///
    /// 通过回放 Changelog 重建历史视图（与 `get_at_revision` 同一模式）。
    /// changelog 中 value 为明文（put 时直接写入），故无需解密。
    ///
    /// 压缩之后必须补齐"保留窗口内未被写过"的 Key —— 它们的历史写入已被物理
    /// 删除，但**状态与当前一致**（窗口内没变）；漏掉会得到"历史读少给 Key、
    /// 实时读却有"的静默残缺视图。无法复原的 Key（T 之后被改、T 之前被压缩）
    /// 必须报错，不得静默返回残缺视图。
    pub fn range_at_revision(
        &self,
        start: &[u8],
        range_end: &[u8],
        limit: usize,
        target_revision: Revision,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let historical = self.replay_historical_range(start, range_end, target_revision)?;
        let mut results = Vec::new();
        for (k, v) in historical.view {
            if limit > 0 && results.len() >= limit {
                break;
            }
            results.push((k, v));
        }
        Ok(results)
    }

    /// Range 历史读（含 revision 元数据）：返回 `[start, range_end)` 在目标
    /// revision 时刻的 `(key, value, metadata)`。
    ///
    /// v2 的 `mod_revision` 必须是“最后一次 <= read_ts 的写入”，不能拿
    /// `read_ts` 顶替；窗口内没有写入时当前元数据就是 T 时刻状态。
    ///
    /// `lease_id` 取当前元数据：G1 的 v2 写路径拒绝非零 lease（#145 才交付
    /// v2 Lease），因此当前历史 lease 恒为 0；#145 必须让 changelog 携带
    /// lease 或提供按 revision 查询 lease 的来源。
    pub fn range_at_revision_with_metadata(
        &self,
        start: &[u8],
        range_end: &[u8],
        limit: usize,
        target_revision: Revision,
    ) -> Result<Vec<VersionedKv>> {
        let historical = self.replay_historical_range(start, range_end, target_revision)?;
        let mut results = Vec::new();
        for (key, value) in historical.view {
            if limit > 0 && results.len() >= limit {
                break;
            }
            let current = self.get_kv_metadata(&key)?.ok_or_else(|| {
                Error::Internal(format!(
                    "historical metadata for key {} at revision {target_revision} is not \
                     reconstructable: key metadata is missing",
                    String::from_utf8_lossy(&key)
                ))
            })?;
            // version 只能被“生效的”写入推进；对不存在 key 的 delete 会写
            // changelog 但不增加 version。从 T 时刻的存在状态向后模拟，才能
            // 把 no-op delete 排除掉（只数事件条数会低估 version）。
            let effective_after = historical
                .post_events
                .get(&key)
                .map(|events| {
                    let mut exists = true; // 调用方只对 T 时刻存在的 key 建元数据
                    let mut count = 0i64;
                    for &(_, is_delete) in events {
                        if is_delete {
                            if exists {
                                count += 1;
                                exists = false;
                            }
                        } else {
                            count += 1;
                            exists = true;
                        }
                    }
                    count
                })
                .unwrap_or(0);
            let metadata = if effective_after == 0 {
                if current.deleted {
                    return Err(Error::Internal(format!(
                        "historical metadata for key {} at revision {target_revision} is not \
                         reconstructable: key was deleted after that revision but no write was \
                         found in the changelog",
                        String::from_utf8_lossy(&key)
                    )));
                }
                current
            } else {
                let version = current
                    .version
                    .checked_sub(effective_after)
                    .ok_or_else(|| {
                        Error::Internal(format!(
                            "historical metadata for key {} at revision {target_revision} is not \
                         reconstructable: {} write(s) after the target exceed current version {}",
                            String::from_utf8_lossy(&key),
                            effective_after,
                            current.version
                        ))
                    })?;
                let mod_revision = historical.last_write.get(&key).copied().ok_or_else(|| {
                    Error::Internal(format!(
                        "historical metadata for key {} at revision {target_revision} is \
                             not reconstructable: last write at or before the target is missing",
                        String::from_utf8_lossy(&key)
                    ))
                })? as i64;
                if current.create_revision as u64 > target_revision {
                    return Err(Error::Internal(format!(
                        "historical metadata for key {} at revision {target_revision} is not \
                         reconstructable: current create_revision {} is after the target",
                        String::from_utf8_lossy(&key),
                        current.create_revision
                    )));
                }
                KvMetadata {
                    version,
                    create_revision: current.create_revision,
                    mod_revision,
                    lease_id: current.lease_id,
                    deleted: false,
                }
            };
            results.push((key, value, metadata));
        }
        Ok(results)
    }

    /// 回放 changelog 得到 T 时刻的值视图，并保留元数据重建所需信息。
    fn replay_historical_range(
        &self,
        start: &[u8],
        range_end: &[u8],
        target_revision: Revision,
    ) -> Result<HistoricalRangeView> {
        self.ensure_history_reconstructable(target_revision)?;
        let end_key = encode_changelog_key(target_revision.saturating_add(1));

        let (mut view, changed_after, last_write, post_events) = self.backend.read(|tx| {
            let entries = tx.iter_prefix(TABLE_CHANGELOG, CHANGELOG_PREFIX)?;
            // BTreeMap 保持 Key 字典序
            let mut view: std::collections::BTreeMap<Vec<u8>, Vec<u8>> =
                std::collections::BTreeMap::new();
            // T 之后有写入的 Key（用于判断"窗口内未变"是否成立）
            let mut changed_after: std::collections::HashSet<Vec<u8>> =
                std::collections::HashSet::new();
            // key → 最后一次 <= T 的写入 revision（含 delete）
            let mut last_write: std::collections::BTreeMap<Vec<u8>, Revision> =
                std::collections::BTreeMap::new();
            // key → T 之后的写入事件（version 回推用；必须保留 delete 顺序）
            let mut post_events: std::collections::HashMap<Vec<u8>, Vec<(Revision, bool)>> =
                std::collections::HashMap::new();
            for (ch_key, ch_value) in entries {
                let is_after = ch_key.as_slice() >= end_key.as_slice();
                if let Ok(event) = ChangeEvent::from_bytes(&ch_value) {
                    for change in &event.changes {
                        if change.key.as_slice() < start {
                            continue;
                        }
                        if !range_end.is_empty() && change.key.as_slice() >= range_end {
                            continue;
                        }
                        if is_after {
                            changed_after.insert(change.key.clone());
                            post_events
                                .entry(change.key.clone())
                                .or_default()
                                .push((event.revision, change.value.is_none()));
                            continue;
                        }
                        last_write.insert(change.key.clone(), event.revision);
                        match &change.value {
                            Some(v) => {
                                view.insert(change.key.clone(), v.clone());
                            }
                            None => {
                                view.remove(&change.key);
                            }
                        }
                    }
                }
            }
            Ok((view, changed_after, last_write, post_events))
        })?;

        // 补齐：在保留窗口内没有任何写入的 Key，其 T 时刻状态 = 当前状态。
        //
        // 候选集合必须是 **当前存活的 Key** ∪ **T 之后被写过的 Key**，而不只是
        // 前者。只遍历存活 Key 会漏掉这样一类 Key：
        //   「T 时刻存在 → T 之后被删除 → T 之前的历史已被压缩」
        // 它既不进 `view`（历史条目已物理删除）、也不在存活集合里（已被删除）、
        // 更不会进 `unreconstructable` → **静默漏报**。而同一条 Key 走单键路径
        // （`get_at_revision`）会明确报 `Internal(... not reconstructable ...)`——
        // 同一数据两条路径结论必须一致，不得静默返回残缺视图。
        let mut unreconstructable: Vec<Vec<u8>> = Vec::new();

        // key → 当前值（None = 当前不存在）
        let mut candidates: std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>> =
            std::collections::BTreeMap::new();
        for key in &changed_after {
            candidates.insert(key.clone(), None);
        }
        for (key, value) in self.range_in(start, range_end, 0)? {
            candidates.insert(key, Some(value));
        }

        for (key, live_value) in candidates {
            if view.contains_key(&key) {
                continue; // 已有 T 时刻的确切值
            }
            if !changed_after.contains(&key) {
                // 保留窗口内从未写过（当前必存活）→ T 时刻状态 == 当前状态
                if let Some(value) = live_value {
                    view.entry(key).or_insert(value);
                }
                continue;
            }
            // T 之后改过（含"T 之后被删除"）、且 T 之前的历史已被压缩：
            // 若无元数据可证它当时还不存在，该 Key 在 T 时刻的值不可复原。
            let created_after_target = self
                .get_kv_metadata(&key)?
                .map(|m| m.create_revision as u64 > target_revision)
                .unwrap_or(false);
            if !created_after_target {
                unreconstructable.push(key);
            }
        }
        if !unreconstructable.is_empty() {
            return Err(Error::HistoryUnavailable {
                revision: target_revision,
                detail: format!(
                    "{} key(s) were written after that revision but their earlier history \
                     was compacted (first: {}); read the live data or a revision after the \
                     last write",
                    unreconstructable.len(),
                    String::from_utf8_lossy(&unreconstructable[0])
                ),
            });
        }

        Ok(HistoricalRangeView {
            view,
            last_write,
            post_events,
        })
    }

    /// 范围删除（raft apply 路径，R-SVC-07-3）：单个写事务内原子标记
    /// `[start, range_end)` 内所有未删除 Key 为 tombstone。
    ///
    /// 与 `delete_at_revision` 相同的幂等守卫与 applied 持久化。
    pub fn delete_range_at_revision(
        &self,
        start: &[u8],
        range_end: &[u8],
        revision: Revision,
        applied: AppliedLogId,
    ) -> Result<DeleteRangeOutcome> {
        self.delete_range_at_key(
            start,
            range_end,
            revision,
            applied,
            RevisionPolicy::LogIndex,
        )
    }

    /// 范围删除（v2 直写路径）：`[start, range_end)` 共享同一 `commit_ts`。
    pub fn delete_range_at_commit_ts(
        &self,
        start: &[u8],
        range_end: &[u8],
        commit_ts: Revision,
        applied: AppliedLogId,
    ) -> Result<DeleteRangeOutcome> {
        if commit_ts == 0 {
            return Err(Error::InvalidArgument("commit_ts must be > 0".into()));
        }
        self.delete_range_at_key(
            start,
            range_end,
            commit_ts,
            applied,
            RevisionPolicy::CommitTs,
        )
    }

    fn delete_range_at_key(
        &self,
        start: &[u8],
        range_end: &[u8],
        revision: Revision,
        applied: AppliedLogId,
        policy: RevisionPolicy,
    ) -> Result<DeleteRangeOutcome> {
        if policy == RevisionPolicy::CommitTs {
            if let Some(event) = self.changelog_event_at(revision)? {
                let all_in_range = event.changes.iter().all(|change| {
                    change.value.is_none()
                        && change.key.as_slice() >= start
                        && (range_end.is_empty() || change.key.as_slice() < range_end)
                });
                let no_live_keys = !self.range_has_live_keys(start, range_end)?;
                let tombstones = self.count_deleted_at_revision(start, range_end, revision)?;
                if all_in_range && no_live_keys && tombstones == event.changes.len() {
                    return Ok(DeleteRangeOutcome {
                        deleted_keys: Vec::new(),
                        replayed: true,
                    });
                }
                return Err(Error::CommitRejected {
                    commit_ts: revision,
                    current_revision: self.current_revision(),
                });
            }
        } else if self.changelog_contains_revision(revision)? {
            return Ok(DeleteRangeOutcome {
                deleted_keys: Vec::new(),
                replayed: true,
            });
        }
        let internal_start = encode_kv_key(start);
        let internal_end = encode_kv_key(range_end);
        self.backend.write(|tx| {
            self.accept_revision(tx, revision, policy)?;
            let all = tx.iter_range(TABLE_KV, &internal_start, &internal_end)?;
            let mut deleted_keys = Vec::new();
            for (ik, _v) in all {
                if let Some(user_key) = decode_kv_key(&ik) {
                    if !range_end.is_empty() && user_key >= range_end {
                        break;
                    }
                    let meta_key = encode_kv_meta_key(user_key);
                    let existing_meta = tx
                        .get(TABLE_KV_META, &meta_key)?
                        .and_then(|bytes| KvMetadata::from_bytes(&bytes));
                    if let Some(m) = existing_meta.filter(|m| !m.deleted) {
                        let meta = m.mark_deleted(revision);
                        tx.insert(TABLE_KV_META, &meta_key, &meta.to_bytes())?;
                        deleted_keys.push(user_key.to_vec());
                    }
                }
            }

            // 始终写 changelog（始终消耗 revision）
            let event = ChangeEvent {
                revision,
                changes: deleted_keys
                    .iter()
                    .map(|k| KeyValueChange {
                        key: k.clone(),
                        value: None,
                        prev_value: None,
                    })
                    .collect(),
                event_type: EventType::Delete,
            };
            tx.insert(
                TABLE_CHANGELOG,
                &encode_changelog_key(revision),
                &event.to_bytes(),
            )?;

            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;

            Ok(DeleteRangeOutcome {
                deleted_keys,
                replayed: false,
            })
        })
    }

    /// 读取 Key 的元数据（version, create_revision, mod_revision, lease_id）
    pub fn get_kv_metadata(&self, key: &[u8]) -> Result<Option<KvMetadata>> {
        let meta_key = encode_kv_meta_key(key);
        self.backend
            .read(|tx| tx.get(TABLE_KV_META, &meta_key))
            .map(|opt| opt.and_then(|bytes| KvMetadata::from_bytes(&bytes)))
    }

    /// 在写事务内标记删除所有绑定到指定 Lease 的 Key（不写 changelog，由调用方统一写入）
    ///
    /// 返回被标记删除的 Key 列表（用于构造 Lease Revoke 的 changelog 事件）。
    /// 仅用于 raft apply 路径（任何路径不得直写本地存储）。
    fn delete_keys_by_lease_in_tx(
        tx: &mut dyn WriteTx,
        target_lease_id: i64,
        revision: Revision,
    ) -> Result<Vec<Vec<u8>>> {
        let all_meta = tx.iter_prefix(TABLE_KV_META, KV_META_PREFIX)?;
        let mut deleted_keys = Vec::new();
        for (meta_key_bytes, meta_value) in &all_meta {
            if let Some(meta) = KvMetadata::from_bytes(meta_value) {
                if meta.lease_id == target_lease_id
                    && !meta.deleted
                    && meta_key_bytes.starts_with(KV_META_PREFIX)
                {
                    let user_key = &meta_key_bytes[KV_META_PREFIX.len()..];
                    let meta_key = encode_kv_meta_key(user_key);
                    let tombstone = meta.mark_deleted(revision);
                    tx.insert(TABLE_KV_META, &meta_key, &tombstone.to_bytes())?;
                    deleted_keys.push(user_key.to_vec());
                }
            }
        }
        Ok(deleted_keys)
    }

    /// Txn 原子事务执行（单节点模式入口）
    pub fn execute_txn(
        &self,
        compares: &[crate::txn::TxnCompare],
        success_ops: &[crate::txn::TxnOp],
        failure_ops: &[crate::txn::TxnOp],
    ) -> Result<crate::txn::TxnResult> {
        let _guard = self.standalone_lock.lock();
        let revision = self.current_revision().saturating_add(1);
        let applied = AppliedLogId::standalone(revision);
        self.execute_txn_at_revision(compares, success_ops, failure_ops, revision, applied)
    }

    /// Txn 原子事务执行（raft apply 路径）：revision ≡ log index
    ///
    /// 幂等守卫：该 revision 的 changelog 已存在则跳过并返回重放标记。
    pub fn execute_txn_at_revision(
        &self,
        compares: &[crate::txn::TxnCompare],
        success_ops: &[crate::txn::TxnOp],
        failure_ops: &[crate::txn::TxnOp],
        revision: Revision,
        applied: AppliedLogId,
    ) -> Result<crate::txn::TxnResult> {
        use crate::txn::TxnResult;

        if self.changelog_contains_revision(revision)? {
            return Ok(TxnResult {
                succeeded: false,
                revision,
                responses: Vec::new(),
            });
        }

        self.backend.write(|tx| {
            self.accept_revision(tx, revision, RevisionPolicy::LogIndex)?;

            // 1. 评估所有比较条件
            let succeeded = self.evaluate_compares_in_tx(tx, compares)?;

            // 2. 选择执行分支
            let ops = if succeeded { success_ops } else { failure_ops };

            // 3. 执行操作并收集变更
            let mut responses = Vec::with_capacity(ops.len());
            let mut changes = Vec::new();

            for op in ops {
                let (resp, change) = self.execute_op_in_tx(tx, op, revision)?;
                responses.push(resp);
                if let Some(c) = change {
                    changes.push(c);
                }
            }

            // 4. 写入 Changelog
            let event = ChangeEvent {
                revision,
                changes,
                event_type: EventType::Txn,
            };
            tx.insert(
                TABLE_CHANGELOG,
                &encode_changelog_key(revision),
                &event.to_bytes(),
            )?;

            // 5. 持久化 applied 状态
            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;

            Ok(TxnResult {
                succeeded,
                revision,
                responses,
            })
        })
    }

    // ──── 内部辅助方法 ────

    /// 在写事务内评估所有比较条件
    fn evaluate_compares_in_tx(
        &self,
        tx: &mut dyn WriteTx,
        compares: &[crate::txn::TxnCompare],
    ) -> Result<bool> {
        use crate::txn::CompareTarget;

        for cmp in compares {
            let meta_key = encode_kv_meta_key(&cmp.key);
            let kv_key = encode_kv_key(&cmp.key);

            let raw_meta = tx
                .get(TABLE_KV_META, &meta_key)?
                .and_then(|bytes| KvMetadata::from_bytes(&bytes));

            // 软删除的 Key 应视为不存在：过滤掉 deleted=true 的元数据
            let meta = raw_meta.filter(|m| !m.deleted);

            // Value 读取同样受软删除影响：deleted=true 时视为无值
            let is_deleted = raw_meta.map(|m| m.deleted).unwrap_or(false);
            let value = if is_deleted {
                None
            } else {
                tx.get(TABLE_KV, &kv_key)?
            };

            let matched = match &cmp.target {
                CompareTarget::Version => {
                    let actual_version = meta.map(|m| m.version).unwrap_or(0);
                    if let crate::txn::CompareValue::Version(target_v) = &cmp.target_value {
                        let actual = crate::txn::CompareValue::Version(actual_version);
                        let target = crate::txn::CompareValue::Version(*target_v);
                        actual.compare(&target, &cmp.op)
                    } else {
                        false
                    }
                }
                CompareTarget::Value => {
                    let actual_value = value.unwrap_or_default();
                    if let crate::txn::CompareValue::Value(target_v) = &cmp.target_value {
                        let actual = crate::txn::CompareValue::Value(actual_value);
                        let target = crate::txn::CompareValue::Value(target_v.clone());
                        actual.compare(&target, &cmp.op)
                    } else {
                        false
                    }
                }
                CompareTarget::ModRevision => {
                    let actual_mod_rev = meta.map(|m| m.mod_revision).unwrap_or(0);
                    if let crate::txn::CompareValue::ModRevision(target_v) = &cmp.target_value {
                        let actual = crate::txn::CompareValue::ModRevision(actual_mod_rev);
                        let target = crate::txn::CompareValue::ModRevision(*target_v);
                        actual.compare(&target, &cmp.op)
                    } else {
                        false
                    }
                }
                CompareTarget::CreateRevision => {
                    let actual_create_rev = meta.map(|m| m.create_revision).unwrap_or(0);
                    if let crate::txn::CompareValue::CreateRevision(target_v) = &cmp.target_value {
                        let actual = crate::txn::CompareValue::CreateRevision(actual_create_rev);
                        let target = crate::txn::CompareValue::CreateRevision(*target_v);
                        actual.compare(&target, &cmp.op)
                    } else {
                        false
                    }
                }
            };

            if !matched {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// 在写事务内执行单个 Txn 操作
    fn execute_op_in_tx(
        &self,
        tx: &mut dyn WriteTx,
        op: &crate::txn::TxnOp,
        revision: Revision,
    ) -> Result<(crate::txn::TxnOpResponse, Option<KeyValueChange>)> {
        use crate::txn::{TxnOp, TxnOpResponse};

        match op {
            TxnOp::Put {
                key,
                value,
                lease_id,
            } => {
                let lid = lease_id.unwrap_or(0);
                let internal_key = encode_kv_key(key);
                let meta_key = encode_kv_meta_key(key);

                // 读取已有元数据
                let existing_meta = tx
                    .get(TABLE_KV_META, &meta_key)?
                    .and_then(|bytes| KvMetadata::from_bytes(&bytes));

                // 更新元数据
                let meta = match existing_meta {
                    Some(m) => m.update(revision, lid),
                    None => KvMetadata::new_key(revision, lid),
                };

                // 写入用户数据（经过 Barrier 加密，仅 /kv/ 前缀）
                let encrypted = self.encrypt_value(&internal_key, value)?;
                tx.insert(TABLE_KV, &internal_key, &encrypted)?;
                tx.insert(TABLE_KV_META, &meta_key, &meta.to_bytes())?;

                let change = KeyValueChange {
                    key: key.to_vec(),
                    value: Some(value.to_vec()),
                    prev_value: None,
                };

                Ok((TxnOpResponse::Put { revision }, Some(change)))
            }
            TxnOp::Delete { key } => {
                let meta_key = encode_kv_meta_key(key);

                // 读取已有元数据并标记删除
                let existing_meta = tx
                    .get(TABLE_KV_META, &meta_key)?
                    .and_then(|bytes| KvMetadata::from_bytes(&bytes));

                if let Some(m) = existing_meta {
                    let meta = m.mark_deleted(revision);
                    tx.insert(TABLE_KV_META, &meta_key, &meta.to_bytes())?;
                }

                let change = KeyValueChange {
                    key: key.to_vec(),
                    value: None,
                    prev_value: None,
                };

                Ok((TxnOpResponse::Delete { revision }, Some(change)))
            }
            TxnOp::Range {
                key,
                range_end,
                limit,
            } => {
                // R-SVC-07-4：语义判定必须与顶层 `KV/Range` **同一份**
                // 定义（`coord_core::kv_range::RangeSemantics`），否则鉴权层建模的
                // 区间与服务端实际扫描的区间不一致 → 跨 scope 越权读：
                // ① `range_end` 为空（或 == key）→ **单键精确查询**（etcd 语义；
                //    与顶层 `KV/Range`、插件 ABI 文档「range-end 空 = 单键精确查询」
                //    一致）。回退「字节前缀扫描」时鉴权层按点查放行，于是
                //    `Txn{Range(key="/app/a", range_end="")}` 能读到
                //    `/app/abc/config` 等 scope 外的 Key；
                // ② 非空且 != key → 半开区间 `[key, range_end)`；
                // ③ 必须检查 KV_META 的 deleted 标志（值非空的软删 key 不得复活）。
                let internal_start = encode_kv_key(key);
                let semantics = RangeSemantics::of(key, range_end);
                let all = match semantics {
                    RangeSemantics::SingleKey => match tx.get(TABLE_KV, &internal_start)? {
                        Some(v) => vec![(internal_start.clone(), v)],
                        None => Vec::new(),
                    },
                    RangeSemantics::Interval => {
                        let internal_end = encode_kv_key(range_end);
                        tx.iter_range(TABLE_KV, &internal_start, &internal_end)?
                    }
                };

                let mut kvs = Vec::new();
                let max = if *limit > 0 {
                    *limit as usize
                } else {
                    usize::MAX
                };

                for (ik, v) in all {
                    if kvs.len() >= max {
                        break;
                    }
                    if let Some(user_key) = decode_kv_key(&ik) {
                        // 半开区间上界（iter_range 已保证，防御性校验）。
                        // 注意：仅在**区间**语义下成立——`range_end == key` 属单键语义，
                        // 此处若照旧按 `!range_end.is_empty()` 判定会把唯一的命中提前 break 掉。
                        if semantics.is_interval() && user_key >= range_end.as_slice() {
                            break;
                        }
                        // 检查元数据 deleted 标志（R-SVC-07-4）
                        let meta_key = encode_kv_meta_key(user_key);
                        let is_deleted = tx
                            .get(TABLE_KV_META, &meta_key)?
                            .and_then(|bytes| KvMetadata::from_bytes(&bytes))
                            .map(|m| m.deleted)
                            .unwrap_or(false);
                        if is_deleted {
                            continue;
                        }
                        let plaintext = if v.is_empty() {
                            Vec::new()
                        } else {
                            self.decrypt_value(&ik, &v)?
                        };
                        kvs.push((user_key.to_vec(), plaintext));
                    }
                }

                let count = kvs.len() as i64;
                Ok((
                    TxnOpResponse::Range {
                        kvs,
                        count,
                        revision,
                    },
                    None, // Range 不产生变更
                ))
            }
        }
    }

    // ──── Lease 状态表（raft 状态机内持久化） ────

    /// 读取 `/_lease/{id}` 记录
    pub fn get_lease_record(&self, lease_id: i64) -> Result<Option<LeaseRecord>> {
        let lease_key = encode_lease_key(lease_id);
        self.backend
            .read(|tx| tx.get(TABLE_KV, &lease_key))
            .map(|opt| opt.and_then(|bytes| LeaseRecord::from_bytes(&bytes)))
    }

    /// 列出全部持久化 Lease 记录（`/_lease/` 前缀），返回 `(lease_id, record)`。
    ///
    /// 供 failover 重建使用：新 leader 从状态机读出全部 Lease
    /// 重建内存 TTL 视图；损坏条目跳过并计数（由调用方决定日志级别）。
    pub fn list_lease_records(&self) -> Result<Vec<(i64, LeaseRecord)>> {
        let rows = self
            .backend
            .read(|tx| tx.iter_prefix(TABLE_KV, LEASE_PREFIX))?;
        let mut records = Vec::with_capacity(rows.len());
        for (key, value) in rows {
            let Some(id_bytes) = key.strip_prefix(LEASE_PREFIX) else {
                continue;
            };
            let Some(id_bytes): Option<[u8; 8]> = id_bytes.try_into().ok() else {
                tracing::warn!("corrupt lease key in storage: {} bytes", key.len());
                continue;
            };
            let id = i64::from_be_bytes(id_bytes);
            if let Some(record) = LeaseRecord::from_bytes(&value) {
                records.push((id, record));
            } else {
                tracing::warn!("corrupt lease record for lease id {id}, skipped");
            }
        }
        Ok(records)
    }

    /// 应用 LeaseOp（单节点模式入口）：锁内分配 revision，防止并发同 revision 冲突
    pub fn apply_lease_op_standalone(
        &self,
        op: &crate::raft::type_config::LeaseOp,
    ) -> Result<Revision> {
        let _guard = self.standalone_lock.lock();
        let revision = self.current_revision().saturating_add(1);
        self.apply_lease_op(op, revision, AppliedLogId::standalone(revision))?;
        Ok(revision)
    }

    /// 应用 LeaseOp（raft apply 路径）：revision ≡ log index
    ///
    /// Grant/KeepAlive 写 `/_lease/{id}`；Revoke 删除记录并按 `KvMetadata.lease_id`
    /// 扫描删除绑定 Key。全部与 changelog + META_LAST_APPLIED 同事务原子完成。
    /// 返回 apply 结果与被删除的绑定 Key（供 Watch 事件构造）。
    pub fn apply_lease_op(
        &self,
        op: &crate::raft::type_config::LeaseOp,
        revision: Revision,
        applied: AppliedLogId,
    ) -> Result<(ApplyOutcome, Vec<KeyValueChange>)> {
        use crate::raft::type_config::LeaseOp;

        if self.changelog_contains_revision(revision)? {
            return Ok((ApplyOutcome::replayed(), Vec::new()));
        }

        self.backend.write(|tx| {
            self.note_log_index_revision(tx, revision)?;
            let mut changes: Vec<KeyValueChange> = Vec::new();

            match op {
                LeaseOp::Grant {
                    id,
                    ttl,
                    deadline_wall_ms,
                } => {
                    let lease_key = encode_lease_key(*id);
                    // **拒绝覆盖**已存在的租约记录。
                    //
                    // 对 `/_lease/{id}` 无条件 upsert 时，「leader 切换后、新主
                    // 尚未 apply 到前任已提交的 Grant」窗口里，另一个客户端可以用
                    // 同一显式 ID 通过 leader 侧的存在性检查，状态机再把原租约
                    // **静默覆盖** → 两个客户端共享 lease → 到期时互相删除对方
                    // 绑定的 Key。
                    //
                    // 状态机是 ID 归属的最终权威：这里拒绝覆盖后，即使 leader 侧
                    // 检查因任何原因漏判，也只会得到"本次 Grant 未生效"，而不会
                    // 破坏已有租约；leader 侧通过 apply 后校验发现并回滚（见
                    // `lease_grant`）。语义与 etcd `LeaseGrant` 指定已存在 ID 报
                    // `LeaseExist` 一致。
                    let existing = tx
                        .get(TABLE_KV, &lease_key)?
                        .and_then(|bytes| LeaseRecord::from_bytes(&bytes));
                    if let Some(prev) = existing {
                        tracing::error!(
                            lease_id = *id,
                            revision,
                            existing_keepalive_revision = prev.keepalive_revision,
                            "LeaseOp::Grant refused: lease id already exists \
                             (refusing to silently overwrite an existing lease)"
                        );
                    } else {
                        let record = LeaseRecord {
                            ttl: *ttl,
                            deadline_wall_ms: *deadline_wall_ms,
                            keepalive_revision: revision as i64,
                        };
                        tx.insert(TABLE_KV, &lease_key, &record.to_bytes())?;
                    }
                }
                LeaseOp::KeepAlive {
                    id,
                    deadline_wall_ms,
                } => {
                    let lease_key = encode_lease_key(*id);
                    if let Some(record) = tx
                        .get(TABLE_KV, &lease_key)?
                        .and_then(|bytes| LeaseRecord::from_bytes(&bytes))
                    {
                        let updated = LeaseRecord {
                            deadline_wall_ms: *deadline_wall_ms,
                            keepalive_revision: revision as i64,
                            ..record
                        };
                        tx.insert(TABLE_KV, &lease_key, &updated.to_bytes())?;
                    }
                }
                LeaseOp::Revoke { id, delete_keys } => {
                    tx.remove(TABLE_KV, &encode_lease_key(*id))?;
                    if *delete_keys {
                        let deleted = Self::delete_keys_by_lease_in_tx(tx, *id, revision)?;
                        for key in deleted {
                            changes.push(KeyValueChange {
                                key,
                                value: None,
                                prev_value: None,
                            });
                        }
                    }
                }
            }

            // Changelog（Lease 事件；Revoke 时携带被删 Key 供 Watch 分发）
            let event = ChangeEvent {
                revision,
                changes: changes.clone(),
                event_type: EventType::Lease,
            };
            tx.insert(
                TABLE_CHANGELOG,
                &encode_changelog_key(revision),
                &event.to_bytes(),
            )?;

            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;

            Ok((ApplyOutcome::applied(), changes))
        })
    }

    /// per-Region 删除绑定到某 Lease 的全部 Key（raft apply 路径）
    ///
    /// Multi-Raft 模式下由各 Region 的 raft 经 `Command::DeleteKeysByLease` 下发
    /// （region 0 的 `LeaseOp::Revoke` 只清全局租约表 `/_lease/{id}`，业务 Key 在
    /// 各 Region 的 MVCC）。在**apply 期事务内**按 `KvMetadata.lease_id` 索引扫描
    /// 并标记删除（与 `apply_lease_op` Revoke 同口径，无 leader 侧扫描 TOCTOU）：
    /// 返回被删 Key 供 Watch 事件构造；changelog + META_LAST_APPLIED 同事务原子；
    /// 幂等守卫同 lease 路径（replayed → 无副作用、无事件）。
    pub fn apply_delete_keys_by_lease(
        &self,
        lease_id: i64,
        revision: Revision,
        applied: AppliedLogId,
    ) -> Result<(ApplyOutcome, Vec<KeyValueChange>)> {
        if self.changelog_contains_revision(revision)? {
            return Ok((ApplyOutcome::replayed(), Vec::new()));
        }

        self.backend.write(|tx| {
            self.note_log_index_revision(tx, revision)?;
            let deleted = Self::delete_keys_by_lease_in_tx(tx, lease_id, revision)?;
            let changes: Vec<KeyValueChange> = deleted
                .into_iter()
                .map(|key| KeyValueChange {
                    key,
                    value: None,
                    prev_value: None,
                })
                .collect();

            // Changelog（Lease 事件；携带被删 Key 供 Watch 分发——与 apply_lease_op
            // Revoke 同口径 EventType::Lease）
            let event = ChangeEvent {
                revision,
                changes: changes.clone(),
                event_type: EventType::Lease,
            };
            tx.insert(
                TABLE_CHANGELOG,
                &encode_changelog_key(revision),
                &event.to_bytes(),
            )?;

            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;

            Ok((ApplyOutcome::applied(), changes))
        })
    }

    // ──── v2 Lease 状态表（region 0 system raft） ────

    /// 读取 `/_leasev2/{id}` 记录
    pub fn get_lease_v2_record(&self, lease_id: i64) -> Result<Option<LeaseV2Record>> {
        self.backend
            .read(|tx| tx.get(TABLE_KV, &encode_lease_v2_key(lease_id)))
            .map(|opt| opt.and_then(|bytes| LeaseV2Record::from_bytes(&bytes)))
    }

    /// 列出全部 v2 租约记录（revoker/expiry 启动扫描用）。
    pub fn list_lease_v2_records(&self) -> Result<Vec<(i64, LeaseV2Record)>> {
        let rows = self
            .backend
            .read(|tx| tx.iter_prefix(TABLE_KV, LEASE_V2_PREFIX))?;
        let mut records = Vec::with_capacity(rows.len());
        for (key, value) in rows {
            let Some(id_bytes) = key.strip_prefix(LEASE_V2_PREFIX) else {
                continue;
            };
            let Some(id_bytes): Option<[u8; 8]> = id_bytes.try_into().ok() else {
                continue;
            };
            let id = i64::from_be_bytes(id_bytes);
            if let Some(record) = LeaseV2Record::from_bytes(&value) {
                records.push((id, record));
            } else {
                tracing::warn!(lease_id = id, "corrupt lease.v2 record skipped");
            }
        }
        Ok(records)
    }

    /// 应用 v2 租约操作（region 0 raft apply 路径）。
    ///
    /// 租约表不是业务 keyspace：不写 changelog、不推进 `META_CURRENT_REVISION`
    /// （v2 下 `note_log_index_revision` 为 no-op），只推进 applied 水位。
    /// 显式 id 冲突/重复 revoke 是用户级结果，不返回 `Err`（D11.5）。
    pub fn apply_lease_v2_op(
        &self,
        op: &crate::raft::type_config::LeaseV2Op,
        revision: Revision,
        applied: AppliedLogId,
    ) -> Result<(LeaseV2Apply, ApplyOutcome)> {
        use crate::raft::type_config::LeaseV2Op;

        let outcome = self.backend.write(|tx| {
            self.note_log_index_revision(tx, revision)?;
            let result = match op {
                LeaseV2Op::Grant {
                    lease_id,
                    ttl_seconds,
                    granted_ts,
                    deadline_wall_ms,
                    request_id,
                } => {
                    if !request_id.is_empty() {
                        if let Some(existing) = tx
                            .get(TABLE_KV, &encode_lease_v2_req_key(request_id))?
                            .and_then(|bytes| <[u8; 8]>::try_from(bytes.as_slice()).ok())
                        {
                            let existing_id = i64::from_be_bytes(existing);
                            if let Some(record) = tx
                                .get(TABLE_KV, &encode_lease_v2_key(existing_id))?
                                .and_then(|bytes| LeaseV2Record::from_bytes(&bytes))
                            {
                                // request_id 命中 = 同一逻辑请求的回放：返回
                                // 首次结果且不覆盖记录（created=true 让 handler
                                // 与「显式 id 冲突」区分开）。
                                tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;
                                tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;
                                return Ok(LeaseV2Apply::Grant {
                                    lease_id: existing_id,
                                    granted_ts: record.granted_ts,
                                    ttl: record.ttl,
                                    created: true,
                                });
                            }
                        }
                    }

                    if let Some(record) = tx
                        .get(TABLE_KV, &encode_lease_v2_key(*lease_id))?
                        .and_then(|bytes| LeaseV2Record::from_bytes(&bytes))
                    {
                        // 状态机是 id 归属的最终权威：绝不静默覆盖已有租约。
                        let same_request =
                            !request_id.is_empty() && record.grant_request_id == *request_id;
                        tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;
                        return Ok(LeaseV2Apply::Grant {
                            lease_id: *lease_id,
                            granted_ts: record.granted_ts,
                            ttl: record.ttl,
                            created: same_request,
                        });
                    }

                    let record = LeaseV2Record {
                        ttl: *ttl_seconds,
                        granted_ts: *granted_ts,
                        deadline_wall_ms: *deadline_wall_ms,
                        revoked: false,
                        revoked_commit_ts: 0,
                        grant_request_id: request_id.clone(),
                    };
                    tx.insert(
                        TABLE_KV,
                        &encode_lease_v2_key(*lease_id),
                        &record.to_bytes(),
                    )?;
                    if !request_id.is_empty() {
                        tx.insert(
                            TABLE_KV,
                            &encode_lease_v2_req_key(request_id),
                            &lease_id.to_be_bytes(),
                        )?;
                    }
                    LeaseV2Apply::Grant {
                        lease_id: *lease_id,
                        granted_ts: *granted_ts,
                        ttl: *ttl_seconds,
                        created: true,
                    }
                }
                LeaseV2Op::KeepAlive {
                    lease_id,
                    deadline_wall_ms,
                } => {
                    let key = encode_lease_v2_key(*lease_id);
                    let mut ok = false;
                    if let Some(mut record) = tx
                        .get(TABLE_KV, &key)?
                        .and_then(|bytes| LeaseV2Record::from_bytes(&bytes))
                    {
                        if !record.revoked {
                            record.deadline_wall_ms = *deadline_wall_ms;
                            tx.insert(TABLE_KV, &key, &record.to_bytes())?;
                            ok = true;
                        }
                    }
                    LeaseV2Apply::KeepAlive { ok }
                }
                LeaseV2Op::Revoke {
                    lease_id,
                    commit_ts,
                    expected_deadline_wall_ms,
                    request_id: _,
                } => {
                    let key = encode_lease_v2_key(*lease_id);
                    let Some(mut record) = tx
                        .get(TABLE_KV, &key)?
                        .and_then(|bytes| LeaseV2Record::from_bytes(&bytes))
                    else {
                        tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;
                        return Ok(LeaseV2Apply::Revoke { commit_ts: 0 });
                    };
                    if !record.revoked {
                        // 到期清理与 KeepAlive 的竞争由 apply 确定性裁决：
                        // KeepAlive 先 apply（deadline 更晚）⇒ 本次到期不成立。
                        if *expected_deadline_wall_ms != 0
                            && record.deadline_wall_ms > *expected_deadline_wall_ms
                        {
                            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;
                            return Ok(LeaseV2Apply::Revoke { commit_ts: 0 });
                        }
                        record.revoked = true;
                        record.revoked_commit_ts = *commit_ts;
                        tx.insert(TABLE_KV, &key, &record.to_bytes())?;
                        LeaseV2Apply::Revoke {
                            commit_ts: *commit_ts,
                        }
                    } else if *commit_ts > record.revoked_commit_ts {
                        // 清理重试：`COMMIT_REJECTED` 后重新取 TSO，旧时间戳在
                        // 某些 Region 仍可能未 apply。
                        record.revoked_commit_ts = *commit_ts;
                        tx.insert(TABLE_KV, &key, &record.to_bytes())?;
                        LeaseV2Apply::Revoke {
                            commit_ts: *commit_ts,
                        }
                    } else {
                        LeaseV2Apply::Revoke { commit_ts: 0 }
                    }
                }
            };

            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;
            Ok(result)
        })?;
        Ok((outcome, ApplyOutcome::applied()))
    }

    /// 按 `KvMetadata.lease_id` 收集未删除的 key（TimeToLive(keys=true)）。
    pub fn keys_by_lease(&self, lease_id: i64, limit: usize) -> Result<Vec<Vec<u8>>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows = self
            .backend
            .read(|tx| tx.iter_prefix(TABLE_KV_META, KV_META_PREFIX))?;
        let mut keys = Vec::new();
        for (meta_key_bytes, meta_value) in rows {
            let Some(meta) = KvMetadata::from_bytes(&meta_value) else {
                continue;
            };
            if meta.lease_id == lease_id
                && !meta.deleted
                && meta_key_bytes.starts_with(KV_META_PREFIX)
            {
                keys.push(meta_key_bytes[KV_META_PREFIX.len()..].to_vec());
                if keys.len() >= limit {
                    break;
                }
            }
        }
        Ok(keys)
    }

    /// per-Region 删除绑定到某 Lease 的全部 Key（v2：显式 `commit_ts`）。
    ///
    /// 与 v1 的 log-index 版本分开：`commit_ts <= current_revision` 且未命中
    /// 已有 changelog ⇒ `CommitRejected`，由 revoker 重新取 TSO 后重试。
    pub fn delete_keys_by_lease_at_commit_ts(
        &self,
        lease_id: i64,
        commit_ts: Revision,
        applied: AppliedLogId,
    ) -> Result<(ApplyOutcome, Vec<Vec<u8>>)> {
        if commit_ts == 0 {
            return Err(Error::InvalidArgument("commit_ts must be > 0".into()));
        }
        if let Some(event) = self.changelog_event_at(commit_ts)? {
            // 同一 commit_ts 只有同一租约清理命令算重放；其他类型的既有写入
            // 是时间戳碰撞（D11.4），必须显式拒绝而不是静默跳过删除。
            if event.event_type == EventType::Lease {
                return Ok((ApplyOutcome::replayed(), Vec::new()));
            }
            return Err(Error::CommitRejected {
                commit_ts,
                current_revision: self.current_revision(),
            });
        }

        self.backend.write(|tx| {
            self.accept_commit_ts(tx, commit_ts)?;
            let deleted = Self::delete_keys_by_lease_in_tx(tx, lease_id, commit_ts)?;
            let changes: Vec<KeyValueChange> = deleted
                .iter()
                .map(|key| KeyValueChange {
                    key: key.clone(),
                    value: None,
                    prev_value: None,
                })
                .collect();
            let event = ChangeEvent {
                revision: commit_ts,
                changes,
                event_type: EventType::Lease,
            };
            tx.insert(
                TABLE_CHANGELOG,
                &encode_changelog_key(commit_ts),
                &event.to_bytes(),
            )?;
            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;
            Ok((ApplyOutcome::applied(), deleted))
        })
    }

    /// 应用 AuthOp（raft apply 路径）：revision ≡ log index
    ///
    /// 用户/角色/吊销登记写入 `/_sys/auth/` 前缀（原始信封 V2 行值，不经 Barrier
    /// 加密——auth 元数据非密文，与 Lease 记录同口径），与 changelog +
    /// META_LAST_APPLIED 同事务原子完成；幂等守卫同 lease 路径。
    pub fn apply_auth_op(
        &self,
        op: &crate::raft::type_config::AuthOp,
        revision: Revision,
        applied: AppliedLogId,
    ) -> Result<ApplyOutcome> {
        use crate::auth::manager::{
            AuthBootstrapTokenRecord, AuthRevocationRecord, AuthRoleRecord, AuthSessionRecord,
            AuthUserRecord, AUTH_BOOTSTRAP_PREFIX, AUTH_REVOKED_PREFIX, AUTH_ROLE_PREFIX,
            AUTH_SESSION_PREFIX, AUTH_USER_PREFIX,
        };
        use crate::raft::type_config::AuthOp;

        if self.changelog_contains_revision(revision)? {
            return Ok(ApplyOutcome::replayed());
        }

        self.backend.write(|tx| {
            self.note_log_index_revision(tx, revision)?;
            match op {
                AuthOp::UserAdd { name, hash, roles } => {
                    let key = [AUTH_USER_PREFIX, name.as_bytes()].concat();
                    let rec = AuthUserRecord {
                        name: name.clone(),
                        password_hash: hash.as_bytes().to_vec(),
                        roles: roles.clone(),
                    };
                    tx.insert(TABLE_KV, &key, &rec.to_bytes()?)?;
                }
                AuthOp::UserDelete { name } => {
                    let key = [AUTH_USER_PREFIX, name.as_bytes()].concat();
                    tx.remove(TABLE_KV, &key)?;
                }
                AuthOp::UserSetPassword { name, hash } => {
                    let key = [AUTH_USER_PREFIX, name.as_bytes()].concat();
                    if let Some(rec) = tx
                        .get(TABLE_KV, &key)?
                        .and_then(|bytes| AuthUserRecord::from_bytes(&bytes))
                    {
                        let updated = AuthUserRecord {
                            password_hash: hash.as_bytes().to_vec(),
                            ..rec
                        };
                        tx.insert(TABLE_KV, &key, &updated.to_bytes()?)?;
                    }
                }
                AuthOp::UserGrantRole { name, role } => {
                    let key = [AUTH_USER_PREFIX, name.as_bytes()].concat();
                    if let Some(rec) = tx
                        .get(TABLE_KV, &key)?
                        .and_then(|bytes| AuthUserRecord::from_bytes(&bytes))
                    {
                        let mut roles = rec.roles;
                        if !roles.iter().any(|r| r == role) {
                            roles.push(role.clone());
                        }
                        let updated = AuthUserRecord { roles, ..rec };
                        tx.insert(TABLE_KV, &key, &updated.to_bytes()?)?;
                    }
                }
                AuthOp::UserRevokeRole { name, role } => {
                    let key = [AUTH_USER_PREFIX, name.as_bytes()].concat();
                    if let Some(rec) = tx
                        .get(TABLE_KV, &key)?
                        .and_then(|bytes| AuthUserRecord::from_bytes(&bytes))
                    {
                        let roles: Vec<String> =
                            rec.roles.into_iter().filter(|r| r != role).collect();
                        let updated = AuthUserRecord { roles, ..rec };
                        tx.insert(TABLE_KV, &key, &updated.to_bytes()?)?;
                    }
                }
                AuthOp::RoleAdd { role } => {
                    let key = [AUTH_ROLE_PREFIX, role.as_bytes()].concat();
                    if tx.get(TABLE_KV, &key)?.is_none() {
                        let rec = AuthRoleRecord {
                            name: role.clone(),
                            permissions: Vec::new(),
                            capability_grants: Vec::new(),
                            high_sensitive: false,
                        };
                        tx.insert(TABLE_KV, &key, &rec.to_bytes()?)?;
                    }
                }
                AuthOp::RoleDelete { role } => {
                    let key = [AUTH_ROLE_PREFIX, role.as_bytes()].concat();
                    tx.remove(TABLE_KV, &key)?;
                    // 同时从所有用户移除该角色
                    let users = tx.iter_prefix(TABLE_KV, AUTH_USER_PREFIX)?;
                    let mut updates: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
                    for (k, v) in users {
                        if let Some(rec) = AuthUserRecord::from_bytes(&v) {
                            let before = rec.roles.len();
                            let roles: Vec<String> =
                                rec.roles.into_iter().filter(|r| r != role).collect();
                            if roles.len() != before {
                                let updated = AuthUserRecord { roles, ..rec };
                                updates.push((k, updated.to_bytes()?));
                            }
                        }
                    }
                    for (k, v) in updates {
                        tx.insert(TABLE_KV, &k, &v)?;
                    }
                }
                AuthOp::RoleGrantPermission {
                    role,
                    perm_type,
                    key,
                    range_end,
                } => {
                    let role_key = [AUTH_ROLE_PREFIX, role.as_bytes()].concat();
                    if let Some(rec) = tx
                        .get(TABLE_KV, &role_key)?
                        .and_then(|bytes| AuthRoleRecord::from_bytes(&bytes))
                    {
                        use crate::auth::manager::AuthPermissionRecord;
                        let is_dup = rec.permissions.iter().any(|p| {
                            p.perm_type == *perm_type
                                && p.key_prefix == *key
                                && p.range_end == *range_end
                        });
                        if !is_dup {
                            let mut updated = rec;
                            updated.permissions.push(AuthPermissionRecord {
                                perm_type: *perm_type,
                                key_prefix: key.clone(),
                                range_end: range_end.clone(),
                            });
                            tx.insert(TABLE_KV, &role_key, &updated.to_bytes()?)?;
                        }
                    }
                }
                AuthOp::RoleRevokePermission {
                    role,
                    key,
                    range_end,
                } => {
                    let role_key = [AUTH_ROLE_PREFIX, role.as_bytes()].concat();
                    if let Some(rec) = tx
                        .get(TABLE_KV, &role_key)?
                        .and_then(|bytes| AuthRoleRecord::from_bytes(&bytes))
                    {
                        let mut updated = rec;
                        updated
                            .permissions
                            .retain(|p| !(p.key_prefix == *key && p.range_end == *range_end));
                        tx.insert(TABLE_KV, &role_key, &updated.to_bytes()?)?;
                    }
                }
                AuthOp::RevokeJti { jti } => {
                    let key = [AUTH_REVOKED_PREFIX, jti.as_bytes()].concat();
                    if tx.get(TABLE_KV, &key)?.is_none() {
                        let rec = AuthRevocationRecord {
                            jti: jti.clone(),
                            revoked_at: crate::lease::wall_clock_now_ms() / 1000,
                        };
                        tx.insert(TABLE_KV, &key, &rec.to_bytes()?)?;
                    }
                }
                AuthOp::IssueSession {
                    hash_hex,
                    username,
                    expires_at_unix,
                    is_refresh,
                } => {
                    // 会话落盘（键为 token 哈希，值不含明文 token）
                    let key = [AUTH_SESSION_PREFIX, hash_hex.as_bytes()].concat();
                    let rec = AuthSessionRecord {
                        username: username.clone(),
                        expires_at_unix: *expires_at_unix,
                        is_refresh: *is_refresh,
                    };
                    tx.insert(TABLE_KV, &key, &rec.to_bytes()?)?;
                }
                AuthOp::ConsumeSession { hash_hex } => {
                    // 会话消费（refresh 单次使用 / 登出 / 吊销）
                    let key = [AUTH_SESSION_PREFIX, hash_hex.as_bytes()].concat();
                    tx.remove(TABLE_KV, &key)?;
                }
                AuthOp::ConsumeSessions { hash_hexes } => {
                    // 定期清理：批量删除已过期会话（与逐条 ConsumeSession 同路径）
                    for hash_hex in hash_hexes {
                        let key = [AUTH_SESSION_PREFIX, hash_hex.as_bytes()].concat();
                        tx.remove(TABLE_KV, &key)?;
                    }
                }
                AuthOp::RoleGrantCapability {
                    role,
                    capability_id,
                    scope,
                } => {
                    let role_key = [AUTH_ROLE_PREFIX, role.as_bytes()].concat();
                    if let Some(rec) = tx
                        .get(TABLE_KV, &role_key)?
                        .and_then(|bytes| AuthRoleRecord::from_bytes(&bytes))
                    {
                        use crate::auth::manager::AuthGrantRecord;
                        let is_dup = rec
                            .capability_grants
                            .iter()
                            .any(|g| g.capability_id == *capability_id && g.scope == *scope);
                        if !is_dup {
                            let mut updated = rec;
                            updated.capability_grants.push(AuthGrantRecord {
                                capability_id: capability_id.clone(),
                                scope: scope.clone(),
                            });
                            tx.insert(TABLE_KV, &role_key, &updated.to_bytes()?)?;
                        }
                    }
                }
                AuthOp::RoleRevokeCapability {
                    role,
                    capability_id,
                    scope,
                } => {
                    let role_key = [AUTH_ROLE_PREFIX, role.as_bytes()].concat();
                    if let Some(rec) = tx
                        .get(TABLE_KV, &role_key)?
                        .and_then(|bytes| AuthRoleRecord::from_bytes(&bytes))
                    {
                        let mut updated = rec;
                        updated
                            .capability_grants
                            .retain(|g| !(g.capability_id == *capability_id && g.scope == *scope));
                        tx.insert(TABLE_KV, &role_key, &updated.to_bytes()?)?;
                    }
                }
                AuthOp::IssueBootstrapToken {
                    id,
                    hash_hex,
                    label,
                    created_by,
                    created_at_unix,
                    expires_at_unix,
                } => {
                    let key = [AUTH_BOOTSTRAP_PREFIX, id.as_bytes()].concat();
                    if tx.get(TABLE_KV, &key)?.is_none() {
                        let rec = AuthBootstrapTokenRecord {
                            id: id.clone(),
                            hash_hex: hash_hex.clone(),
                            label: label.clone(),
                            created_by: created_by.clone(),
                            created_at_unix: *created_at_unix,
                            expires_at_unix: *expires_at_unix,
                            consumed_at_unix: None,
                        };
                        tx.insert(TABLE_KV, &key, &rec.to_bytes()?)?;
                    }
                }
                AuthOp::ConsumeBootstrapToken {
                    id,
                    consumed_at_unix,
                } => {
                    let key = [AUTH_BOOTSTRAP_PREFIX, id.as_bytes()].concat();
                    if let Some(mut rec) = tx
                        .get(TABLE_KV, &key)?
                        .and_then(|bytes| AuthBootstrapTokenRecord::from_bytes(&bytes))
                    {
                        if rec.consumed_at_unix.is_none() {
                            rec.consumed_at_unix = Some(*consumed_at_unix);
                            tx.insert(TABLE_KV, &key, &rec.to_bytes()?)?;
                        }
                    }
                }
                AuthOp::RevokeBootstrapToken { id } => {
                    let key = [AUTH_BOOTSTRAP_PREFIX, id.as_bytes()].concat();
                    tx.remove(TABLE_KV, &key)?;
                }
            }

            // Changelog（Auth 事件：无 Key 变更，仅记录 revision 供 Watch 水位推进）
            let event = ChangeEvent {
                revision,
                changes: Vec::new(),
                event_type: EventType::Put,
            };
            tx.insert(
                TABLE_CHANGELOG,
                &encode_changelog_key(revision),
                &event.to_bytes(),
            )?;

            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;

            Ok(ApplyOutcome::applied())
        })
    }

    // ──── PD 全局 operator 队列（/ docs region 0 raft）────

    /// apply PD 命令（region 0 raft 状态机路径）。
    ///
    /// 幂等守卫：该 revision 已 apply（changelog 存在）→ 直接返回 replayed
    /// （无副作用）。写路径与 `apply_auth_op` 同构：raw `TABLE_KV` 记录 + 空 changes
    /// 的 changelog 标记 + `META_LAST_APPLIED` 同一事务；不上 Watch、不产生用户
    /// 变更事件（队列为内部控制面记录）。所有判定只依赖持久状态（确定性，
    /// 约束 1）。
    pub fn apply_pd_op(
        &self,
        op: &crate::raft::type_config::PdOp,
        revision: Revision,
        applied: AppliedLogId,
    ) -> Result<ApplyOutcome> {
        use crate::raft::type_config::PdOp;
        use crate::raft::type_config::PdQueueEntry;

        if self.changelog_contains_revision(revision)? {
            return Ok(ApplyOutcome::replayed());
        }

        self.backend.write(|tx| {
            self.note_log_index_revision(tx, revision)?;
            match op {
                PdOp::Enqueue {
                    op,
                    requester,
                    proposed_at_unix,
                } => {
                    // 全局去重：存在相同 Pending/Running operator → no-op
                    let mut dup = false;
                    for (_, v) in tx.iter_prefix(TABLE_KV, PD_QUEUE_PREFIX)? {
                        if let Some(e) = PdQueueEntry::from_bytes(&v) {
                            if (e.is_pending() || e.is_running()) && e.op == *op {
                                dup = true;
                                break;
                            }
                        }
                    }
                    if !dup {
                        let entry = PdQueueEntry::new_pending(
                            revision,
                            op.clone(),
                            *requester,
                            *proposed_at_unix,
                        );
                        tx.insert(TABLE_KV, &encode_pd_op_key(revision), &entry.to_bytes()?)?;
                    }
                }
                PdOp::Claim {
                    op_id,
                    node_id,
                    claimed_at_unix,
                } => {
                    let key = encode_pd_op_key(*op_id);
                    if let Some(mut e) = tx
                        .get(TABLE_KV, &key)?
                        .and_then(|v| PdQueueEntry::from_bytes(&v))
                    {
                        if e.try_claim(*node_id, *claimed_at_unix) {
                            tx.insert(TABLE_KV, &key, &e.to_bytes()?)?;
                        }
                    }
                }
                PdOp::Complete {
                    op_id,
                    node_id,
                    success,
                    error,
                } => {
                    let key = encode_pd_op_key(*op_id);
                    if let Some(mut e) = tx
                        .get(TABLE_KV, &key)?
                        .and_then(|v| PdQueueEntry::from_bytes(&v))
                    {
                        if e.try_complete(*node_id, *success, error) {
                            tx.insert(TABLE_KV, &key, &e.to_bytes()?)?;
                        }
                    }
                }
                PdOp::Requeue { op_id } => {
                    let key = encode_pd_op_key(*op_id);
                    if let Some(mut e) = tx
                        .get(TABLE_KV, &key)?
                        .and_then(|v| PdQueueEntry::from_bytes(&v))
                    {
                        if e.try_requeue() {
                            tx.insert(TABLE_KV, &key, &e.to_bytes()?)?;
                        }
                    }
                }
            }

            // 终态裁剪（确定性：键序 = op_id 升序 = 入队序，删最旧终态）
            let mut terminal: Vec<Vec<u8>> = Vec::new();
            for (k, v) in tx.iter_prefix(TABLE_KV, PD_QUEUE_PREFIX)? {
                if let Some(e) = PdQueueEntry::from_bytes(&v) {
                    if e.is_terminal() {
                        terminal.push(k);
                    }
                }
            }
            let excess = terminal.len().saturating_sub(PD_QUEUE_MAX_TERMINAL);
            for k in terminal.into_iter().take(excess) {
                tx.remove(TABLE_KV, &k)?;
            }

            // Changelog 标记（无 Key 变更，仅推进幂等守卫/水位）
            let event = ChangeEvent {
                revision,
                changes: Vec::new(),
                event_type: EventType::Put,
            };
            tx.insert(
                TABLE_CHANGELOG,
                &encode_changelog_key(revision),
                &event.to_bytes(),
            )?;

            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;

            Ok(ApplyOutcome::applied())
        })
    }

    // ──── TSO（#136）────

    /// 读取 TSO 高水位（下一个未发放值；缺失 = 1）。
    pub fn tso_high_water(&self) -> Result<u64> {
        self.backend.read(|tx| {
            Ok(tx
                .get(TABLE_KV, TSO_HIGH_WATER)?
                .and_then(|b| <[u8; 8]>::try_from(b.as_slice()).ok())
                .map(u64::from_be_bytes)
                .unwrap_or(1))
        })
    }

    /// apply 一条 TSO 批次分配（region 0 system raft）。
    ///
    /// 批次 = `[start, start + batch_size)`，`batch_size = 0` 视为 1。高水位与
    /// `META_LAST_APPLIED` 同事务持久化：apply 返回后即使进程崩溃，重启也不会
    /// 复用已发放区间（只增不减、允许空洞）。
    pub fn apply_tso_allocate(
        &self,
        batch_size: u32,
        revision: Revision,
        applied: AppliedLogId,
    ) -> Result<TsoBatchOutcome> {
        if self.changelog_contains_revision(revision)? {
            let last: Option<TsoBatchRecord> = self.backend.read(|tx| {
                Ok(tx
                    .get(TABLE_KV, TSO_LAST_BATCH)?
                    .and_then(|b| crate::storage::envelope::decode(&b).ok()))
            })?;
            return Ok(match last {
                Some(rec) if rec.revision == revision => TsoBatchOutcome {
                    start_ts: rec.start_ts,
                    end_ts: rec.end_ts,
                    replayed: true,
                },
                // 只可能是更旧的重复条目（raft 不会产生）；返回当前高水位空区间，
                // 保证“重放不推进高水位”而不是伪造成新批。
                _ => {
                    let high_water = self.tso_high_water()?;
                    TsoBatchOutcome {
                        start_ts: high_water,
                        end_ts: high_water,
                        replayed: true,
                    }
                }
            });
        }

        self.backend.write(|tx| {
            self.note_log_index_revision(tx, revision)?;
            let start_ts = tx
                .get(TABLE_KV, TSO_HIGH_WATER)?
                .and_then(|b| <[u8; 8]>::try_from(b.as_slice()).ok())
                .map(u64::from_be_bytes)
                .unwrap_or(1);
            let end_ts = start_ts
                .checked_add(u64::from(batch_size.max(1)))
                .ok_or_else(|| Error::Internal("TSO high-water overflow".to_string()))?;

            let record = TsoBatchRecord {
                revision,
                start_ts,
                end_ts,
            };
            let record_bytes = crate::storage::envelope::encode(&record)
                .map_err(|e| Error::Internal(format!("encode TSO batch record: {e}")))?;

            tx.insert(TABLE_KV, TSO_HIGH_WATER, &end_ts.to_be_bytes())?;
            tx.insert(TABLE_KV, TSO_LAST_BATCH, &record_bytes)?;

            // 幂等守卫/水位：与批次记录同一事务（重放由 changelog 判定）。
            let event = ChangeEvent {
                revision,
                changes: Vec::new(),
                event_type: EventType::Put,
            };
            tx.insert(
                TABLE_CHANGELOG,
                &encode_changelog_key(revision),
                &event.to_bytes(),
            )?;
            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;

            Ok(TsoBatchOutcome {
                start_ts,
                end_ts,
                replayed: false,
            })
        })
    }

    // ──── Commit Sequencer（#146）────

    /// apply 一批 `ValidateAndCommit`（region 0 system raft）。
    ///
    /// 批内顺序 = leader 分配的 `commit_ts` 顺序。每位事务独立给出
    /// [`crate::raft::type_config::SequencerOutcome`]：冲突/重复/乱序/非法
    /// 都是用户级结果，不返回 `Err`（D11.5）。决策、幂等索引、校验索引与
    /// `META_LAST_APPLIED` 在同一 redb 写事务里原子落盘。
    ///
    /// 旧 `ValidateAndCommit` 没有 range 谓词与上限，继续按 #146 语义执行；
    /// range 校验只在追加的变体里生效（旧日志无需重写）。
    pub fn apply_sequencer_op(
        &self,
        op: &crate::raft::type_config::SequencerOp,
        revision: Revision,
        applied: AppliedLogId,
    ) -> Result<Vec<crate::raft::type_config::SequencerOutcome>> {
        use crate::raft::type_config::SequencerOp;

        self.backend.write(|tx| {
            self.note_log_index_revision(tx, revision)?;
            let outcomes = match op {
                SequencerOp::ValidateAndCommit { txns } => txns
                    .iter()
                    .map(|txn| Self::sequencer_validate_in_tx(tx, txn, &[], None))
                    .collect::<Result<Vec<_>>>()?,
                SequencerOp::ValidateAndCommitWithRanges { txns, limits } => txns
                    .iter()
                    .map(|entry| {
                        Self::sequencer_validate_in_tx(
                            tx,
                            &entry.txn,
                            &entry.read_ranges,
                            Some(limits),
                        )
                    })
                    .collect::<Result<Vec<_>>>()?,
            };
            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;
            Ok(outcomes)
        })
    }

    // ──── Commit Log shard 元数据（#154）────

    /// 应用一条 shard 元数据操作（region 0 system raft）。
    ///
    /// 非法操作（未知 shard、回退边界、seal 未定界 shard）只记 warning 并保持
    /// 原表：这些都是用户级/元数据级拒绝，不得让 raft apply 返回 `Err`。三个
    /// 操作本身幂等，因此同一日志重放不会改变表。
    pub fn apply_commit_log_shard_op(
        &self,
        op: &crate::raft::type_config::CommitLogShardOp,
        revision: Revision,
        applied: AppliedLogId,
    ) -> Result<Vec<CommitLogShardMeta>> {
        use crate::raft::type_config::CommitLogShardOp;

        self.backend.write(|tx| {
            self.note_log_index_revision(tx, revision)?;
            let mut table = match tx.get(TABLE_KV, COMMIT_LOG_SHARD_TABLE_KEY)? {
                Some(bytes) => CommitLogShardTable::decode(&bytes)?,
                None => CommitLogShardTable::default(),
            };
            let outcome = match op {
                CommitLogShardOp::Open {
                    start_ts,
                    opened_at_ms,
                } => table.open(*start_ts, *opened_at_ms),
                CommitLogShardOp::SetEnd {
                    shard_id,
                    end_ts,
                    now_ms,
                } => table.set_end(*shard_id, *end_ts, *now_ms),
                CommitLogShardOp::Seal { shard_id, now_ms } => table.seal(*shard_id, *now_ms),
                CommitLogShardOp::Gc { through_ts } => table.advance_gc(*through_ts),
            };
            let changed = match outcome {
                Ok(changed) => changed,
                Err(err) => {
                    tracing::warn!(
                        revision,
                        op = ?op,
                        error = %err,
                        "commit log shard op rejected; table unchanged"
                    );
                    false
                }
            };
            if changed {
                tx.insert(TABLE_KV, COMMIT_LOG_SHARD_TABLE_KEY, &table.encode()?)?;
            }
            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;
            Ok(table.shards)
        })
    }

    /// 读取 region 0 的 Commit Log shard 表（缺失 = 空表）。
    pub fn commit_log_shard_table(&self) -> Result<CommitLogShardTable> {
        self.backend
            .read(|tx| match tx.get(TABLE_KV, COMMIT_LOG_SHARD_TABLE_KEY)? {
                Some(bytes) => CommitLogShardTable::decode(&bytes),
                None => Ok(CommitLogShardTable::default()),
            })
    }

    // ──── Commit Log shard 组记录追加（#154）────

    /// 向 shard 组追加一批记录（shard raft 组 apply）。
    ///
    /// 幂等：同键同内容 = Duplicate；越界/编码失败只计数（D11.5 不得失败
    /// apply），由 shipper 刷新路由后重试。游标、计数与每 Region 的 shipped
    /// 水位同事务推进，shipper 据此断点续传且不跳过边界 `commit_ts`。
    #[allow(clippy::too_many_arguments)]
    pub fn apply_commit_log_append(
        &self,
        shard_id: u64,
        start_ts: u64,
        end_ts: u64,
        records: &[CommitRecord],
        max_txn_bytes: u64,
        revision: Revision,
        applied: AppliedLogId,
    ) -> Result<CommitLogAppendResult> {
        self.backend.write(|tx| {
            self.note_log_index_revision(tx, revision)?;
            let previous_cursor = tx
                .get(TABLE_KV, COMMIT_LOG_CURSOR_KEY)?
                .and_then(|bytes| decode_u64(&bytes))
                .unwrap_or(0);
            let previous_count = tx
                .get(TABLE_KV, COMMIT_LOG_COUNT_KEY)?
                .and_then(|bytes| decode_u64(&bytes))
                .unwrap_or(0);
            let mut result = CommitLogAppendResult {
                cursor_ts: previous_cursor,
                ..CommitLogAppendResult::default()
            };
            let mut shipped: std::collections::BTreeMap<u64, u64> =
                std::collections::BTreeMap::new();
            for record in records {
                if record.shard_id != shard_id
                    || record.commit_ts < start_ts
                    || (end_ts != 0 && record.commit_ts >= end_ts)
                {
                    result.rejected += 1;
                    tracing::warn!(
                        shard_id,
                        commit_ts = record.commit_ts,
                        start_ts,
                        end_ts,
                        record_shard = record.shard_id,
                        "commit log record outside the shard range; retry after routing refresh"
                    );
                    continue;
                }
                let appended = crate::storage::commit_log::append_commit_record_in_tx(
                    tx,
                    record,
                    max_txn_bytes,
                );
                match appended {
                    Ok(CommitLogAppend::Inserted) => {
                        result.inserted += 1;
                        result.cursor_ts = result.cursor_ts.max(record.commit_ts);
                    }
                    Ok(CommitLogAppend::Duplicate) => {
                        result.duplicates += 1;
                        result.cursor_ts = result.cursor_ts.max(record.commit_ts);
                    }
                    Err(err) => {
                        result.rejected += 1;
                        tracing::warn!(
                            shard_id,
                            commit_ts = record.commit_ts,
                            error = %err,
                            "commit log record rejected; keeping the existing row"
                        );
                        continue;
                    }
                }
                shipped
                    .entry(record.region_id)
                    .and_modify(|ts| *ts = (*ts).max(record.commit_ts))
                    .or_insert(record.commit_ts);
            }
            if result.inserted > 0 {
                tx.insert(
                    TABLE_KV,
                    COMMIT_LOG_COUNT_KEY,
                    &(previous_count + result.inserted).to_be_bytes(),
                )?;
            }
            if result.cursor_ts > previous_cursor {
                tx.insert(
                    TABLE_KV,
                    COMMIT_LOG_CURSOR_KEY,
                    &result.cursor_ts.to_be_bytes(),
                )?;
            }
            for (region_id, ts) in shipped {
                let key = crate::storage::commit_log::shipped_key(region_id);
                let previous = tx
                    .get(TABLE_KV, &key)?
                    .and_then(|bytes| decode_u64(&bytes))
                    .unwrap_or(0);
                if ts > previous {
                    tx.insert(TABLE_KV, &key, &ts.to_be_bytes())?;
                }
            }
            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;
            Ok(result)
        })
    }

    /// shard 组内已追加的最大 `commit_ts`（缺失 = 0）。
    pub fn commit_log_cursor(&self) -> Result<u64> {
        self.backend.read(|tx| {
            Ok(tx
                .get(TABLE_KV, COMMIT_LOG_CURSOR_KEY)?
                .and_then(|bytes| decode_u64(&bytes))
                .unwrap_or(0))
        })
    }

    /// shard 组内已插入的记录总数（缺失 = 0；重复写入不增加）。
    pub fn commit_log_record_count(&self) -> Result<u64> {
        self.backend.read(|tx| {
            Ok(tx
                .get(TABLE_KV, COMMIT_LOG_COUNT_KEY)?
                .and_then(|bytes| decode_u64(&bytes))
                .unwrap_or(0))
        })
    }

    /// 某数据 Region 在本 shard 组内已追加的最大 `commit_ts`（缺失 = 0）。
    pub fn commit_log_shipped_through(&self, region_id: u64) -> Result<u64> {
        let key = crate::storage::commit_log::shipped_key(region_id);
        self.backend.read(|tx| {
            Ok(tx
                .get(TABLE_KV, &key)?
                .and_then(|bytes| decode_u64(&bytes))
                .unwrap_or(0))
        })
    }

    /// 单条事务的 OCC 校验与决策落盘。
    ///
    /// 顺序不能调换：幂等命中先于「乱序」判断（重试可能带更大的新时间戳），
    /// 乱序先于读集校验（旧时间戳的决策不能改写已确定的总序）。读集上限先于
    /// 冲突校验：输入非法时不必逐条扫一个超大的读集。
    ///
    /// range 谓词与 point 读集共用当前 `WriteTx`：同批前序事务的索引写入必须
    /// 对后序校验可见，否则会漏掉本应冲突的事务（假阴性）。
    fn sequencer_validate_in_tx(
        tx: &mut dyn WriteTx,
        txn: &crate::raft::type_config::SequencerTxn,
        read_ranges: &[crate::raft::type_config::SequencerReadRange],
        limits: Option<&crate::raft::type_config::SequencerValidationLimits>,
    ) -> Result<crate::raft::type_config::SequencerOutcome> {
        use crate::raft::type_config::{SequencerInvalid, SequencerOutcome};

        if txn.txn_id.is_empty() {
            return Ok(SequencerOutcome::Invalid {
                commit_ts: txn.commit_ts,
                reason: SequencerInvalid::MissingTxnId,
            });
        }

        // 幂等优先于时间戳合法性：重试可能带一个新的、甚至非法的提案时间戳，
        // 但已决策的事务必须原样返回首次结果。
        let txn_index = prefixed_hash_key(
            SEQUENCER_TXN_PREFIX,
            &sequencer_hash(b"txn", &[&txn.txn_id]),
        );
        if let Some(existing) = tx.get(TABLE_KV, &txn_index)?.and_then(|b| decode_u64(&b)) {
            return Ok(SequencerOutcome::Duplicate {
                commit_ts: existing,
            });
        }
        // request_id 的作用域是 txn（服务端会话）：重跑事务会换 txn_id，
        // 不能被上一轮同名字符串误判为重复提交。
        if !txn.request_id.is_empty() {
            let request_index = prefixed_hash_key(
                SEQUENCER_REQUEST_PREFIX,
                &sequencer_hash(b"req", &[&txn.txn_id, &txn.request_id]),
            );
            if let Some(existing) = tx
                .get(TABLE_KV, &request_index)?
                .and_then(|b| decode_u64(&b))
            {
                return Ok(SequencerOutcome::Duplicate {
                    commit_ts: existing,
                });
            }
        }

        if txn.commit_ts <= txn.start_ts {
            return Ok(SequencerOutcome::Invalid {
                commit_ts: txn.commit_ts,
                reason: SequencerInvalid::CommitTsNotAboveStart,
            });
        }

        let last_commit_ts = tx
            .get(TABLE_KV, SEQUENCER_LAST_COMMIT_TS)?
            .and_then(|b| decode_u64(&b))
            .unwrap_or(0);
        if txn.commit_ts <= last_commit_ts {
            return Ok(SequencerOutcome::OutOfOrder {
                commit_ts: txn.commit_ts,
                last_commit_ts,
            });
        }

        if let Some(limits) = limits {
            let entries = txn.read_set.len().saturating_add(read_ranges.len());
            if limits.max_read_set > 0 && entries > limits.max_read_set as usize {
                return Ok(SequencerOutcome::Invalid {
                    commit_ts: txn.commit_ts,
                    reason: SequencerInvalid::ReadSetTooLarge,
                });
            }
        }

        // `max_range = 0` 沿用区间查询约定（不限制）；超预算不是冲突，
        // 而是「无法证明无冲突」：调用方必须收窄谓词/拆分事务后重试。
        //
        // 区间索引每个事务只扫一次：按 read_set 逐 key 重复全量扫描会让最热的
        // region0 apply 变成 O(read_set × 索引项)。读集为空时不扫，避免无读
        // 事务因索引总量被拒。
        let max_range = limits.map_or(0, |limits| limits.max_range as usize);
        let interval_entries = if txn.read_set.is_empty() {
            Vec::new()
        } else {
            match Self::sequencer_interval_rows_in(tx, max_range)? {
                Some(entries) => entries,
                None => {
                    return Ok(SequencerOutcome::Invalid {
                        commit_ts: txn.commit_ts,
                        reason: SequencerInvalid::RangeScanExceeded,
                    })
                }
            }
        };
        for key in &txn.read_set {
            let index_key =
                prefixed_hash_key(SEQUENCER_KEY_INDEX_PREFIX, &sequencer_hash(b"key", &[key]));
            if let Some(write_ts) = tx.get(TABLE_KV, &index_key)?.and_then(|b| decode_u64(&b)) {
                if write_ts > txn.start_ts {
                    return Ok(SequencerOutcome::Conflict {
                        commit_ts: txn.commit_ts,
                        conflict_key: key.clone(),
                        conflict_ts: write_ts,
                    });
                }
            }
            if let Some(hit) =
                Self::sequencer_interval_point_hit(&interval_entries, key, txn.start_ts)
            {
                return Ok(SequencerOutcome::Conflict {
                    commit_ts: txn.commit_ts,
                    conflict_key: key.clone(),
                    conflict_ts: hit.commit_ts,
                });
            }
        }

        for range in read_ranges {
            match Self::sequencer_range_query_in(
                tx,
                &range.start,
                &range.end,
                txn.start_ts,
                max_range,
            )? {
                SequencerRangeQuery::Clear => {}
                SequencerRangeQuery::Hit(hit) => {
                    return Ok(SequencerOutcome::Conflict {
                        commit_ts: txn.commit_ts,
                        conflict_key: hit.key,
                        conflict_ts: hit.commit_ts,
                    });
                }
                SequencerRangeQuery::Exceeded { .. } => {
                    return Ok(SequencerOutcome::Invalid {
                        commit_ts: txn.commit_ts,
                        reason: SequencerInvalid::RangeScanExceeded,
                    });
                }
            }
        }

        let decision = SequencerDecision {
            txn_id: txn.txn_id.clone(),
            request_id: txn.request_id.clone(),
            start_ts: txn.start_ts,
            commit_ts: txn.commit_ts,
            mutations: txn.mutations.clone(),
        };
        let bytes = crate::storage::envelope::encode(&decision)
            .map_err(|e| Error::Internal(format!("encode sequencer decision: {e}")))?;
        tx.insert(
            TABLE_KV,
            &encode_sequencer_decision_key(txn.commit_ts),
            &bytes,
        )?;
        tx.insert(TABLE_KV, &txn_index, &txn.commit_ts.to_be_bytes())?;
        if !txn.request_id.is_empty() {
            let request_index = prefixed_hash_key(
                SEQUENCER_REQUEST_PREFIX,
                &sequencer_hash(b"req", &[&txn.txn_id, &txn.request_id]),
            );
            tx.insert(TABLE_KV, &request_index, &txn.commit_ts.to_be_bytes())?;
        }
        Self::sequencer_index_writes_in_tx(tx, &decision)?;
        tx.insert(
            TABLE_KV,
            SEQUENCER_INDEX_LAST_TS,
            &txn.commit_ts.to_be_bytes(),
        )?;
        tx.insert(
            TABLE_KV,
            SEQUENCER_LAST_COMMIT_TS,
            &txn.commit_ts.to_be_bytes(),
        )?;
        Ok(SequencerOutcome::Committed {
            commit_ts: txn.commit_ts,
        })
    }

    /// 推进 point 哈希索引、有序 range 索引与区间写索引；返回本次处理的
    /// mutation 数。
    ///
    /// 三个索引都是派生数据：同一 key/区间只保留最大 `commit_ts`，重放/同批
    /// 重复写只前进不回退。删除也是写，必须让并发谓词读冲突。
    fn sequencer_index_writes_in_tx(
        tx: &mut dyn WriteTx,
        decision: &SequencerDecision,
    ) -> Result<u64> {
        use crate::raft::type_config::SequencerMutation;

        for mutation in &decision.mutations {
            match mutation {
                SequencerMutation::Put { .. } | SequencerMutation::Delete { .. } => {
                    let Some(key) = mutation.point_key() else {
                        continue;
                    };
                    let hash_key = prefixed_hash_key(
                        SEQUENCER_KEY_INDEX_PREFIX,
                        &sequencer_hash(b"key", &[key]),
                    );
                    let range_key = prefixed_key(SEQUENCER_RANGE_INDEX_PREFIX, key);
                    for index_key in [hash_key, range_key] {
                        let prev = tx
                            .get(TABLE_KV, &index_key)?
                            .and_then(|b| decode_u64(&b))
                            .unwrap_or(0);
                        if decision.commit_ts > prev {
                            tx.insert(TABLE_KV, &index_key, &decision.commit_ts.to_be_bytes())?;
                        }
                    }
                }
                SequencerMutation::DeleteRange { start, end } => {
                    let index_key = prefixed_key(
                        SEQUENCER_INTERVAL_INDEX_PREFIX,
                        &sequencer_hash(b"interval", &[start, end]),
                    );
                    let prev = tx
                        .get(TABLE_KV, &index_key)?
                        .and_then(|b| {
                            crate::storage::envelope::decode::<SequencerIntervalWrite>(&b).ok()
                        })
                        .unwrap_or(SequencerIntervalWrite {
                            start: Vec::new(),
                            end: Vec::new(),
                            commit_ts: 0,
                        });
                    if decision.commit_ts > prev.commit_ts {
                        let entry = SequencerIntervalWrite {
                            start: start.clone(),
                            end: end.clone(),
                            commit_ts: decision.commit_ts,
                        };
                        let bytes = crate::storage::envelope::encode(&entry).map_err(|e| {
                            Error::Internal(format!("encode sequencer interval index: {e}"))
                        })?;
                        tx.insert(TABLE_KV, &index_key, &bytes)?;
                    }
                }
            }
        }
        Ok(decision.mutations.len() as u64)
    }

    /// 读取决策日志中的一条记录（#148 应用、运维排查）。
    pub fn get_sequencer_decision(&self, commit_ts: Revision) -> Result<Option<SequencerDecision>> {
        self.backend
            .read(|tx| tx.get(TABLE_KV, &encode_sequencer_decision_key(commit_ts)))
            .map(|opt| opt.and_then(|bytes| crate::storage::envelope::decode(&bytes).ok()))
    }

    /// 数据 Region 已应用的决策游标（缺失 = 0）。
    pub fn last_applied_decision_ts(&self) -> Result<Revision> {
        self.backend.read(|tx| {
            Ok(tx
                .get(TABLE_KV, DECISION_APPLIED_TS)?
                .and_then(|b| decode_u64(&b))
                .unwrap_or(0))
        })
    }

    /// 按 `commit_ts` 升序读取 `after_ts` 之后的决策（有界）。
    ///
    /// 决策日志是 applier 的唯一事实源；`limit` 是内存上界，`0` 按 1。
    pub fn sequencer_decisions_after(
        &self,
        after_ts: Revision,
        limit: usize,
    ) -> Result<Vec<SequencerDecision>> {
        let limit = limit.max(1);
        let rows = self.backend.read(|tx| {
            let start = encode_sequencer_decision_key(after_ts.saturating_add(1));
            let end = prefix_upper_bound(SEQUENCER_DECISION_PREFIX);
            tx.iter_range_limited(TABLE_KV, &start, &end, limit)
        })?;
        rows.into_iter()
            .map(|(key, value)| {
                let commit_ts = key
                    .strip_prefix(SEQUENCER_DECISION_PREFIX)
                    .filter(|suffix| suffix.len() == 8)
                    .and_then(|suffix| <[u8; 8]>::try_from(suffix).ok())
                    .map(u64::from_be_bytes)
                    .ok_or_else(|| {
                        Error::Internal("decode sequencer decision key: bad suffix".to_string())
                    })?;
                let decision: SequencerDecision = crate::storage::envelope::decode(&value)
                    .map_err(|e| Error::Internal(format!("decode sequencer decision: {e}")))?;
                if decision.commit_ts != commit_ts {
                    return Err(Error::Internal(format!(
                        "sequencer decision key/record mismatch: key={commit_ts} record={}",
                        decision.commit_ts
                    )));
                }
                Ok(decision)
            })
            .collect()
    }

    /// 该 Region 游标之后的下一条决策（无 = None）。safe_ts 用它逐 Region 封顶。
    pub fn next_sequencer_decision_ts(&self, after_ts: Revision) -> Result<Option<Revision>> {
        Ok(self
            .sequencer_decisions_after(after_ts, 1)?
            .first()
            .map(|decision| decision.commit_ts))
    }

    /// 把一批已决策事务应用到本 Region 的 MVCC（#148）。
    ///
    /// 一个命令内的所有 mutation 在同一个 redb 写事务里完成，因此同一 Region
    /// 的多 key 不会部分可见；跨 Region 的可见性由 safe_ts 收敛。
    ///
    /// 幂等/漂移处理：
    /// - `commit_ts <= 游标` 或 changelog 已存在且内容一致 → 跳过（重放）；
    /// - changelog 已存在但内容不一致、或 `commit_ts <= current_revision` 且无
    ///   changelog → 停在该决策之前并保留游标（fence 漂移时 fail-closed，
    ///   由 safe_ts 封顶保证正常路径不会发生）；
    /// - 无本 Region mutation 的决策也推进游标与 `META_CURRENT_REVISION`。
    pub fn apply_commit_decisions(
        &self,
        decisions: &[crate::raft::type_config::CommitDecisionApply],
        revision: Revision,
        applied: AppliedLogId,
    ) -> Result<CommitDecisionApplyResult> {
        let views: Vec<DecisionView<'_>> = decisions.iter().map(DecisionView::Plain).collect();
        self.apply_commit_decisions_core(&views, revision, applied, 0)
    }

    /// 应用带 Commit Log 元数据的决策，并在同一 redb 事务里写 CommitRecord（#153）。
    ///
    /// 记录写失败（编码/校验）只记 error、不阻断 apply：数据可见性与 safe_ts 游标
    /// 优先于 Watch 历史；正常路径上限校验在 Commit 阶段已经通过。
    pub fn apply_commit_decisions_with_log(
        &self,
        decisions: &[CommitDecisionApplyLog],
        revision: Revision,
        applied: AppliedLogId,
        max_txn_bytes: u64,
    ) -> Result<CommitDecisionApplyResult> {
        let views: Vec<DecisionView<'_>> = decisions.iter().map(DecisionView::Logged).collect();
        self.apply_commit_decisions_core(&views, revision, applied, max_txn_bytes)
    }

    fn apply_commit_decisions_core(
        &self,
        decisions: &[DecisionView<'_>],
        revision: Revision,
        applied: AppliedLogId,
        max_txn_bytes: u64,
    ) -> Result<CommitDecisionApplyResult> {
        let cursor = self.last_applied_decision_ts()?;
        let result = self.backend.write(|tx| {
            let mut current = Self::current_revision_in_tx(tx, self.revision_mode)?;
            let mut through = cursor;
            let mut count = 0u64;
            let mut previous = None;

            for view in decisions {
                let commit_ts = view.commit_ts();
                let mutations = view.mutations();
                let logged = view.logged();
                if commit_ts <= through {
                    continue; // 已应用/游标之前的重放
                }
                if previous.is_some_and(|ts| commit_ts <= ts) {
                    break; // 载荷必须按 commit_ts 严格递增
                }
                previous = Some(commit_ts);

                let existing = tx
                    .get(TABLE_CHANGELOG, &encode_changelog_key(commit_ts))?
                    .map(|bytes| ChangeEvent::from_bytes(&bytes))
                    .transpose()?;
                if let Some(event) = existing {
                    if event_matches_mutations(&event, mutations) {
                        through = commit_ts;
                        count += 1;
                        continue;
                    }
                    break;
                }
                if commit_ts <= current {
                    if mutations.is_empty() {
                        // 空决策没有 changelog 可作重放证据；游标丢失后只要水位
                        // 已越过它就说明当时已应用（或本来就没有本 Region 的写）。
                        through = commit_ts;
                        count += 1;
                        continue;
                    }
                    break;
                }

                let mut changes = Vec::with_capacity(mutations.len());
                let mut record_changes = Vec::new();
                for mutation in mutations {
                    match mutation {
                        crate::raft::type_config::SequencerMutation::Put {
                            key,
                            value,
                            lease_id,
                        } => {
                            let internal_key = encode_kv_key(key);
                            let meta_key = encode_kv_meta_key(key);
                            let lease_id = lease_id.unwrap_or(0);
                            let existing_meta = tx
                                .get(TABLE_KV_META, &meta_key)?
                                .and_then(|bytes| KvMetadata::from_bytes(&bytes));
                            let prev = match (logged.is_some(), existing_meta.as_ref()) {
                                (true, Some(meta)) => commit_prev_value(tx, &internal_key, meta)?,
                                _ => None,
                            };
                            let meta = match existing_meta {
                                Some(meta) => meta.update(commit_ts, lease_id),
                                None => KvMetadata::new_key(commit_ts, lease_id),
                            };
                            let encrypted = self.encrypt_value(&internal_key, value)?;
                            if logged.is_some() {
                                record_changes.push(CommitChange {
                                    key: key.clone(),
                                    value: Some(encrypted.clone()),
                                    prev,
                                });
                            }
                            tx.insert(TABLE_KV, &internal_key, &encrypted)?;
                            tx.insert(TABLE_KV_META, &meta_key, &meta.to_bytes())?;
                            changes.push(KeyValueChange {
                                key: key.clone(),
                                value: Some(value.clone()),
                                prev_value: None,
                            });
                        }
                        crate::raft::type_config::SequencerMutation::Delete { key } => {
                            let internal_key = encode_kv_key(key);
                            let meta_key = encode_kv_meta_key(key);
                            let existing_meta = tx
                                .get(TABLE_KV_META, &meta_key)?
                                .and_then(|bytes| KvMetadata::from_bytes(&bytes));
                            let prev = match (logged.is_some(), existing_meta.as_ref()) {
                                (true, Some(meta)) => commit_prev_value(tx, &internal_key, meta)?,
                                _ => None,
                            };
                            if let Some(meta) = existing_meta.filter(|meta| !meta.deleted) {
                                tx.insert(
                                    TABLE_KV_META,
                                    &meta_key,
                                    &meta.mark_deleted(commit_ts).to_bytes(),
                                )?;
                            }
                            if logged.is_some() {
                                record_changes.push(CommitChange {
                                    key: key.clone(),
                                    value: None,
                                    prev,
                                });
                            }
                            changes.push(KeyValueChange {
                                key: key.clone(),
                                value: None,
                                prev_value: None,
                            });
                        }
                        crate::raft::type_config::SequencerMutation::DeleteRange { start, end } => {
                            let internal_start = encode_kv_key(start);
                            let internal_end = if end.is_empty() {
                                prefix_upper_bound(KV_PREFIX)
                            } else {
                                encode_kv_key(end)
                            };
                            let rows = tx.iter_range(TABLE_KV, &internal_start, &internal_end)?;
                            for (internal_key, value) in rows {
                                let Some(user_key) = decode_kv_key(&internal_key) else {
                                    continue;
                                };
                                if !end.is_empty() && user_key >= end.as_slice() {
                                    break;
                                }
                                let meta_key = encode_kv_meta_key(user_key);
                                let existing_meta = tx
                                    .get(TABLE_KV_META, &meta_key)?
                                    .and_then(|bytes| KvMetadata::from_bytes(&bytes));
                                if let Some(meta) = existing_meta.filter(|meta| !meta.deleted) {
                                    if logged.is_some() {
                                        record_changes.push(CommitChange {
                                            key: user_key.to_vec(),
                                            value: None,
                                            prev: Some(CommitPrevValue {
                                                value,
                                                create_revision: meta.create_revision,
                                                mod_revision: meta.mod_revision,
                                                version: meta.version,
                                                lease_id: meta.lease_id,
                                            }),
                                        });
                                    }
                                    tx.insert(
                                        TABLE_KV_META,
                                        &meta_key,
                                        &meta.mark_deleted(commit_ts).to_bytes(),
                                    )?;
                                    changes.push(KeyValueChange {
                                        key: user_key.to_vec(),
                                        value: None,
                                        prev_value: None,
                                    });
                                }
                            }
                        }
                    }
                }
                if !changes.is_empty() {
                    let event = ChangeEvent {
                        revision: commit_ts,
                        changes,
                        event_type: EventType::Txn,
                    };
                    tx.insert(
                        TABLE_CHANGELOG,
                        &encode_changelog_key(commit_ts),
                        &event.to_bytes(),
                    )?;
                }
                if let Some(log) = logged {
                    if !record_changes.is_empty() {
                        let record = CommitRecord {
                            shard_id: log.shard_id,
                            commit_ts,
                            txn_id: log.txn_id.clone(),
                            start_ts: log.start_ts,
                            region_id: log.region_id,
                            txn_bytes: u32::try_from(CommitRecord::txn_write_bytes(mutations))
                                .unwrap_or(u32::MAX),
                            changes: record_changes,
                        };
                        if let Err(err) = crate::storage::commit_log::append_commit_record_in_tx(
                            &mut *tx,
                            &record,
                            max_txn_bytes,
                        ) {
                            tracing::error!(
                                commit_ts,
                                region_id = log.region_id,
                                error = %err,
                                "commit record write failed; data apply continues"
                            );
                        }
                    }
                }
                current = commit_ts;
                through = commit_ts;
                count += 1;
            }

            if through > cursor {
                tx.insert(TABLE_KV, DECISION_APPLIED_TS, &through.to_be_bytes())?;
            }
            let effective = current.max(through);
            if effective > Self::current_revision_in_tx(tx, self.revision_mode)? {
                tx.insert(TABLE_META, META_CURRENT_REVISION, &effective.to_be_bytes())?;
            }
            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;
            self.note_log_index_revision(tx, revision)?;
            Ok(CommitDecisionApplyResult {
                through_ts: through,
                decisions: count,
            })
        })?;
        Ok(result)
    }

    /// 追加一条 CommitRecord（维护/测试入口；apply 走同事务的 `append_in_tx`）。
    pub fn append_commit_record(
        &self,
        record: &CommitRecord,
        max_txn_bytes: u64,
    ) -> Result<CommitLogAppend> {
        self.backend.write(|tx| {
            crate::storage::commit_log::append_commit_record_in_tx(tx, record, max_txn_bytes)
        })
    }

    /// 同一 `commit_ts` 的全部 Region 分片，按 `(commit_ts, region_id)` 升序。
    pub fn commit_records_at(&self, shard_id: u64, commit_ts: u64) -> Result<Vec<CommitRecord>> {
        self.backend.read(|tx| {
            let prefix = crate::storage::commit_log::encode_commit_log_key(shard_id, commit_ts, 0);
            let prefix = &prefix[..prefix.len() - 8];
            tx.iter_prefix(TABLE_COMMIT_LOG, prefix)?
                .into_iter()
                .map(|(key, value)| crate::storage::commit_log::decode_commit_log_row(&key, &value))
                .collect()
        })
    }

    /// `commit_ts > after_ts` 的分片，最多 `limit` 行（`0` = 不限）。
    ///
    /// `after_ts` 是**严格**下界：limit 可能截断同一 `commit_ts` 的剩余 Region
    /// 分片，调用方必须先整组消费该 ts（[`Self::commit_records_at`]）再推进游标，
    /// 否则会跳过同事务的其它 key。
    pub fn commit_records_after(
        &self,
        shard_id: u64,
        after_ts: u64,
        limit: usize,
    ) -> Result<Vec<CommitRecord>> {
        self.backend.read(|tx| {
            let start = crate::storage::commit_log::encode_commit_log_key(
                shard_id,
                after_ts.saturating_add(1),
                0,
            );
            let shard_prefix = crate::storage::commit_log::commit_log_shard_prefix(shard_id);
            let end = prefix_upper_bound(&shard_prefix);
            tx.iter_range_limited(TABLE_COMMIT_LOG, &start, &end, limit)?
                .into_iter()
                .map(|(key, value)| crate::storage::commit_log::decode_commit_log_row(&key, &value))
                .collect()
        })
    }

    /// 已决策的最大 `commit_ts`（缺失 = 0）。
    pub fn sequencer_last_commit_ts(&self) -> Result<u64> {
        self.backend.read(|tx| {
            Ok(tx
                .get(TABLE_KV, SEQUENCER_LAST_COMMIT_TS)?
                .and_then(|b| decode_u64(&b))
                .unwrap_or(0))
        })
    }

    /// point 读集校验：`(start_ts, ·]` 内该 key 是否被写。
    ///
    /// 只比较 `ts > start_ts`：sequencer apply 在调用前已保证 `commit_ts >
    /// last_commit_ts`，索引里不会出现更晚的写（见 [`Self::sequencer_range_query`]）。
    pub fn sequencer_point_query(
        &self,
        key: &[u8],
        start_ts: Revision,
    ) -> Result<Option<SequencerIndexHit>> {
        self.backend.read(|tx| {
            let index_key =
                prefixed_hash_key(SEQUENCER_KEY_INDEX_PREFIX, &sequencer_hash(b"key", &[key]));
            let point = tx
                .get(TABLE_KV, &index_key)?
                .and_then(|b| decode_u64(&b))
                .filter(|ts| *ts > start_ts)
                .map(|commit_ts| SequencerIndexHit {
                    key: key.to_vec(),
                    commit_ts,
                });
            let interval = match Self::sequencer_interval_point_in(tx, key, start_ts, 0)? {
                SequencerRangeQuery::Hit(hit) => Some(hit),
                SequencerRangeQuery::Clear => None,
                SequencerRangeQuery::Exceeded { .. } => {
                    return Err(Error::Internal(
                        "unbounded interval index scan reported a budget overflow".to_string(),
                    ))
                }
            };
            Ok(Self::best_index_hit(point, interval))
        })
    }

    /// 取 `commit_ts` 更大的命中；同 ts 取字典序更小的 key（结果确定）。
    fn best_index_hit(
        left: Option<SequencerIndexHit>,
        right: Option<SequencerIndexHit>,
    ) -> Option<SequencerIndexHit> {
        match (left, right) {
            (None, hit) | (hit, None) => hit,
            (Some(left), Some(right)) => {
                if right.commit_ts > left.commit_ts
                    || (right.commit_ts == left.commit_ts && right.key < left.key)
                {
                    Some(right)
                } else {
                    Some(left)
                }
            }
        }
    }

    /// 扫描区间写索引；返回 `None` 表示超过 `max_entries` 预算（0 = 不限）。
    fn sequencer_interval_rows_in(
        tx: &(impl ReadTx + ?Sized),
        max_entries: usize,
    ) -> Result<Option<Vec<SequencerIntervalWrite>>> {
        #[cfg(test)]
        INTERVAL_INDEX_SCANS.with(|scans| scans.set(scans.get() + 1));
        let fetch_limit = if max_entries == 0 {
            0
        } else {
            max_entries.saturating_add(1)
        };
        let rows = tx.iter_range_limited(
            TABLE_KV,
            SEQUENCER_INTERVAL_INDEX_PREFIX,
            &prefix_upper_bound(SEQUENCER_INTERVAL_INDEX_PREFIX),
            fetch_limit,
        )?;
        if max_entries > 0 && rows.len() > max_entries {
            return Ok(None);
        }
        let mut entries = Vec::with_capacity(rows.len());
        for (_key, value) in rows {
            let entry: SequencerIntervalWrite = crate::storage::envelope::decode(&value)
                .map_err(|e| Error::Internal(format!("decode sequencer interval index: {e}")))?;
            entries.push(entry);
        }
        Ok(Some(entries))
    }

    /// point 读是否落在某条区间写内：`start <= key < end && ts > start_ts`。
    fn sequencer_interval_point_in(
        tx: &(impl ReadTx + ?Sized),
        key: &[u8],
        start_ts: Revision,
        max_entries: usize,
    ) -> Result<SequencerRangeQuery> {
        let Some(entries) = Self::sequencer_interval_rows_in(tx, max_entries)? else {
            return Ok(SequencerRangeQuery::Exceeded {
                scanned: max_entries,
            });
        };
        Ok(
            match Self::sequencer_interval_point_hit(&entries, key, start_ts) {
                Some(hit) => SequencerRangeQuery::Hit(hit),
                None => SequencerRangeQuery::Clear,
            },
        )
    }

    /// 在已读到的区间索引项里查 point 命中（不再重复扫描索引）。
    fn sequencer_interval_point_hit(
        entries: &[SequencerIntervalWrite],
        key: &[u8],
        start_ts: Revision,
    ) -> Option<SequencerIndexHit> {
        let mut best: Option<SequencerIndexHit> = None;
        for entry in entries {
            if entry.commit_ts <= start_ts {
                continue;
            }
            if entry.start.as_slice() <= key && key < entry.end.as_slice() {
                let hit = SequencerIndexHit {
                    key: entry.start.clone(),
                    commit_ts: entry.commit_ts,
                };
                best = Self::best_index_hit(best, Some(hit));
            }
        }
        best
    }

    /// 读区间是否与某条区间写相交。
    ///
    /// 相交条件：`write.start < read.end && read.start < write.end`（空 end = +∞）；
    /// 相邻区间（`[a,c)` 与 `[c,e)`）不算相交。
    fn sequencer_interval_range_in(
        tx: &(impl ReadTx + ?Sized),
        start: &[u8],
        end: &[u8],
        start_ts: Revision,
        max_entries: usize,
    ) -> Result<SequencerRangeQuery> {
        let Some(entries) = Self::sequencer_interval_rows_in(tx, max_entries)? else {
            return Ok(SequencerRangeQuery::Exceeded {
                scanned: max_entries,
            });
        };
        let mut best: Option<SequencerIndexHit> = None;
        for entry in entries {
            if entry.commit_ts <= start_ts {
                continue;
            }
            let write_starts_before_end = end.is_empty() || entry.start.as_slice() < end;
            let read_starts_before_write_end = entry.end.is_empty() || start < entry.end.as_slice();
            if write_starts_before_end && read_starts_before_write_end {
                let hit = SequencerIndexHit {
                    key: entry.start,
                    commit_ts: entry.commit_ts,
                };
                best = Self::best_index_hit(best, Some(hit));
            }
        }
        Ok(match best {
            Some(hit) => SequencerRangeQuery::Hit(hit),
            None => SequencerRangeQuery::Clear,
        })
    }

    /// range 谓词校验：`[start, end)` 在 `(start_ts, ·]` 内是否有写。
    ///
    /// `end` 为空表示无上界；`start >= end` 是空区间。返回命中的最大
    /// `commit_ts`（同 ts 取字典序最小 key，与有序扫描一致）。
    ///
    /// `max_entries` 限制扫描的索引项数（0 = 不限）；超预算返回
    /// [`SequencerRangeQuery::Exceeded`]，调用方必须 fail-closed，不能当 `Clear`。
    pub fn sequencer_range_query(
        &self,
        start: &[u8],
        end: &[u8],
        start_ts: Revision,
        max_entries: usize,
    ) -> Result<SequencerRangeQuery> {
        self.backend
            .read(|tx| Self::sequencer_range_query_in(tx, start, end, start_ts, max_entries))
    }

    /// 在给定读事务/写事务里执行区间校验。
    ///
    /// sequencer apply 必须用当前写事务调用它：同批前序事务的索引写入尚未提交，
    /// 用独立读事务会看不到，产生假阴性。
    fn sequencer_range_query_in(
        tx: &(impl ReadTx + ?Sized),
        start: &[u8],
        end: &[u8],
        start_ts: Revision,
        max_entries: usize,
    ) -> Result<SequencerRangeQuery> {
        if !end.is_empty() && start >= end {
            return Ok(SequencerRangeQuery::Clear);
        }
        let scan_start = prefixed_key(SEQUENCER_RANGE_INDEX_PREFIX, start);
        let scan_end = if end.is_empty() {
            prefix_upper_bound(SEQUENCER_RANGE_INDEX_PREFIX)
        } else {
            prefixed_key(SEQUENCER_RANGE_INDEX_PREFIX, end)
        };
        let fetch_limit = if max_entries == 0 {
            0
        } else {
            max_entries.saturating_add(1)
        };
        let rows = tx.iter_range_limited(TABLE_KV, &scan_start, &scan_end, fetch_limit)?;
        if max_entries > 0 && rows.len() > max_entries {
            return Ok(SequencerRangeQuery::Exceeded {
                scanned: max_entries,
            });
        }

        let mut best: Option<SequencerIndexHit> = None;
        for (index_key, value) in rows {
            let key = index_key.strip_prefix(SEQUENCER_RANGE_INDEX_PREFIX);
            let write_ts = decode_u64(&value);
            let (Some(key), Some(write_ts)) = (key, write_ts) else {
                continue;
            };
            if write_ts <= start_ts {
                continue;
            }
            let replace = match &best {
                None => true,
                Some(hit) => write_ts > hit.commit_ts,
            };
            if replace {
                best = Some(SequencerIndexHit {
                    key: key.to_vec(),
                    commit_ts: write_ts,
                });
            }
        }
        match best {
            Some(hit) => Ok(SequencerRangeQuery::Hit(hit)),
            None => Self::sequencer_interval_range_in(tx, start, end, start_ts, max_entries),
        }
    }

    /// 校验索引已覆盖的最大 `commit_ts`（缺失 = 0）。
    pub fn sequencer_index_last_ts(&self) -> Result<u64> {
        self.backend.read(|tx| {
            Ok(tx
                .get(TABLE_KV, SEQUENCER_INDEX_LAST_TS)?
                .and_then(|b| decode_u64(&b))
                .unwrap_or(0))
        })
    }

    /// 从决策日志按批补齐校验索引。
    ///
    /// 正常提交与索引在同一事务里推进，这里通常无事可做；索引丢失、旧快照或
    /// 人工清理后由它恢复。`batch` 是每批处理的决策数（内存上界），返回追赶量。
    ///
    /// 只能在开始服务前（或写入已静默）调用：索引落后的窗口里，OCC 校验会把
    /// 已有冲突看成无冲突（假阴性），所以启动路径用它在对外服务前 fail-closed。
    pub fn catch_up_sequencer_index(&self, batch: usize) -> Result<SequencerIndexRebuild> {
        let batch = batch.max(1);
        let last_commit_ts = self.sequencer_last_commit_ts()?;
        let from_ts = self.sequencer_index_last_ts()?;
        let mut stats = SequencerIndexRebuild {
            from_ts,
            to_ts: from_ts,
            decisions: 0,
            keys: 0,
        };
        let mut marker = from_ts;

        while marker < last_commit_ts {
            let rows = self.backend.read(|tx| {
                let start = encode_sequencer_decision_key(marker.saturating_add(1));
                let end = prefix_upper_bound(SEQUENCER_DECISION_PREFIX);
                tx.iter_range_limited(TABLE_KV, &start, &end, batch)
            })?;
            if rows.is_empty() {
                break;
            }

            let mut keys = 0u64;
            let mut last_ts = marker;
            self.backend.write(|tx| {
                for (key, value) in &rows {
                    let commit_ts = key
                        .strip_prefix(SEQUENCER_DECISION_PREFIX)
                        .filter(|suffix| suffix.len() == 8)
                        .and_then(|suffix| <[u8; 8]>::try_from(suffix).ok())
                        .map(u64::from_be_bytes)
                        .ok_or_else(|| {
                            Error::Internal("decode sequencer decision key: bad suffix".to_string())
                        })?;
                    let decision: SequencerDecision = crate::storage::envelope::decode(value)
                        .map_err(|e| Error::Internal(format!("decode sequencer decision: {e}")))?;
                    if decision.commit_ts != commit_ts {
                        return Err(Error::Internal(format!(
                            "sequencer decision key/record mismatch: key={commit_ts} record={}",
                            decision.commit_ts
                        )));
                    }
                    keys += Self::sequencer_index_writes_in_tx(tx, &decision)?;
                    last_ts = commit_ts;
                }
                // 并发提交可能已把水位推得更高；只前进，不回退。
                let current = tx
                    .get(TABLE_KV, SEQUENCER_INDEX_LAST_TS)?
                    .and_then(|b| decode_u64(&b))
                    .unwrap_or(0);
                if last_ts > current {
                    tx.insert(TABLE_KV, SEQUENCER_INDEX_LAST_TS, &last_ts.to_be_bytes())?;
                }
                Ok(())
            })?;

            stats.decisions += rows.len() as u64;
            stats.keys += keys;
            stats.to_ts = last_ts;
            marker = last_ts;
            if rows.len() < batch {
                break;
            }
        }
        Ok(stats)
    }

    /// 清空三个校验索引后从决策日志全量重建。
    ///
    /// 索引是派生数据，重建必须与原始索引等价；正常启动只做追赶，不调用本方法。
    /// 与追赶相同，必须在写入静默时执行：清空后的空窗会让并发校验漏判冲突。
    pub fn rebuild_sequencer_validation_index(
        &self,
        batch: usize,
    ) -> Result<SequencerIndexRebuild> {
        self.backend.write(|tx| {
            for prefix in [
                SEQUENCER_KEY_INDEX_PREFIX,
                SEQUENCER_RANGE_INDEX_PREFIX,
                SEQUENCER_INTERVAL_INDEX_PREFIX,
            ] {
                for (key, _) in tx.iter_prefix(TABLE_KV, prefix)? {
                    tx.remove(TABLE_KV, &key)?;
                }
            }
            tx.remove(TABLE_KV, SEQUENCER_INDEX_LAST_TS)?;
            Ok(())
        })?;
        self.catch_up_sequencer_index(batch)
    }

    // ──── SafeTs fence / applied watermark（#142）────

    /// 读取已发布的安全点 fence（缺失 = 0）。
    pub fn safe_ts_fence(&self) -> Result<Revision> {
        self.backend.read(|tx| {
            Ok(tx
                .get(TABLE_KV, SAFE_TS_FENCE)?
                .and_then(|bytes| decode_u64(&bytes))
                .unwrap_or(0))
        })
    }

    /// 发布安全点 fence（region 0 system raft）。
    ///
    /// `fence_ts` 是 TSO 分配边界（下一未发放值），不是某个 key 的
    /// `commit_ts`：它只声明「fence 之后不再接受 `commit_ts <= fence_ts` 的写」。
    /// 只前进；同一 applied 重放返回当前值。
    pub fn set_safe_ts_fence(&self, fence_ts: Revision, applied: AppliedLogId) -> Result<Revision> {
        self.backend.write(|tx| {
            let prev = tx
                .get(TABLE_KV, SAFE_TS_FENCE)?
                .and_then(|bytes| decode_u64(&bytes))
                .unwrap_or(0);
            if Self::applied_contains(tx, applied)? {
                return Ok(prev);
            }
            let effective = prev.max(fence_ts);
            if effective > prev {
                tx.insert(TABLE_KV, SAFE_TS_FENCE, &effective.to_be_bytes())?;
            }
            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;
            Ok(effective)
        })
    }

    /// 读取已发布的 CommitWatermark（缺失 = 0）。
    pub fn commit_watermark(&self) -> Result<u64> {
        self.backend.read(|tx| {
            Ok(tx
                .get(TABLE_KV, COMMIT_WATERMARK)?
                .and_then(|bytes| decode_u64(&bytes))
                .unwrap_or(0))
        })
    }

    /// 发布 CommitWatermark（region 0 system raft）。
    ///
    /// 只前进：watermark 是「<= 它的 `commit_ts` 不会再有新记录」的承诺，回退
    /// 会让 Watch 重新暴露已跳过的时间段。同一 applied 重放返回当前值。
    pub fn publish_commit_watermark(&self, watermark: u64, applied: AppliedLogId) -> Result<u64> {
        self.backend.write(|tx| {
            let prev = tx
                .get(TABLE_KV, COMMIT_WATERMARK)?
                .and_then(|bytes| decode_u64(&bytes))
                .unwrap_or(0);
            if Self::applied_contains(tx, applied)? {
                return Ok(prev);
            }
            let effective = prev.max(watermark);
            if effective > prev {
                tx.insert(TABLE_KV, COMMIT_WATERMARK, &effective.to_be_bytes())?;
            }
            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;
            Ok(effective)
        })
    }

    /// 把已 apply 水位抬到 `commit_ts`（不写数据/changelog）。
    ///
    /// 这是 safe-point fence 在数据 Region 的落地：之后 `commit_ts <= 水位` 的
    /// 写入会被 `accept_commit_ts` 拒绝，因此 fence 一旦被 `safe_ts` 覆盖，
    /// 该 Region 的历史在 fence 之前就固定了。只前进；同一 applied 重放幂等。
    pub fn advance_commit_ts_watermark(
        &self,
        commit_ts: Revision,
        applied: AppliedLogId,
    ) -> Result<Revision> {
        self.backend.write(|tx| {
            let current_before = Self::current_revision_in_tx(tx, self.revision_mode)?;
            if Self::applied_contains(tx, applied)? {
                return Ok(current_before);
            }
            self.note_log_index_revision(tx, applied.index)?;
            let current = Self::current_revision_in_tx(tx, self.revision_mode)?;
            let effective = if self.revision_mode == RevisionMode::CommitTs {
                current.max(commit_ts)
            } else {
                current
            };
            if effective > current {
                tx.insert(TABLE_META, META_CURRENT_REVISION, &effective.to_be_bytes())?;
            }
            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;
            Ok(effective)
        })
    }

    /// 该 applied 是否已持久化（同一命令重放时直接返回当前水位，不再抬）。
    fn applied_contains(tx: &dyn ReadTx, applied: AppliedLogId) -> Result<bool> {
        Ok(tx
            .get(TABLE_META, META_LAST_APPLIED)?
            .and_then(|bytes| AppliedLogId::from_bytes(&bytes))
            .map(|last| applied.index <= last.index)
            .unwrap_or(false))
    }

    /// 读取 region 0 PD 全局队列快照（按 op_id 升序；执行器/测试/审计读取）
    pub fn pd_queue_entries(&self) -> Result<Vec<crate::raft::type_config::PdQueueEntry>> {
        use crate::raft::type_config::PdQueueEntry;

        self.backend.read(|tx| {
            let mut out = Vec::new();
            for (_, v) in tx.iter_prefix(TABLE_KV, PD_QUEUE_PREFIX)? {
                if let Some(e) = PdQueueEntry::from_bytes(&v) {
                    out.push(e);
                }
            }
            out.sort_by_key(|e| e.op_id);
            Ok(out)
        })
    }

    // ──── Compact（raft 下发 compact revision，节点一致）────

    /// 读取已持久化的 compacted revision（`META_COMPACT_REVISION`）。
    ///
    /// 无持久化条目时返回 0（从未压缩）。
    pub fn compacted_revision(&self) -> Result<Revision> {
        self.backend
            .read(|tx| tx.get(TABLE_META, META_COMPACT_REVISION))
            .map(|opt| {
                opt.and_then(|bytes| {
                    let arr: [u8; 8] = bytes.as_slice().try_into().ok()?;
                    Some(u64::from_be_bytes(arr))
                })
                .unwrap_or(0)
            })
    }

    /// 应用 Compact：物理删除 `< revision` 的 changelog 条目与过期 tombstone，
    /// 并持久化 `META_COMPACT_REVISION`（与 `META_LAST_APPLIED` 同事务）。
    ///
    /// 语义（设计决策）：
    /// - **确定性**：所有节点 apply 同一命令得到相同删除集合（删除条件只依赖
    ///   revision 与持久化状态，不读墙钟/随机数，约束 1）；
    /// - **分片删除**：每批 `COMPACT_BATCH_SIZE` 条一个写事务，避免巨型事务；
    /// - **幂等**：`revision <= 已持久化 compacted_revision` 为 no-op（重启重放安全）；
    /// - **不得失败**：openraft 将 apply 错误视为致命，故 `revision > current_revision`
    ///   时钳制为 current revision（未来 revision 的拒绝由 RPC/提案层负责，错误码
    ///   `INVALID_ARGUMENT`）。水位是 TSO current revision，**不得**用 raft
    ///   `applied.index` 钳制（ADR-0015 D3）。
    pub fn apply_compact(
        &self,
        revision: Revision,
        applied: AppliedLogId,
    ) -> Result<CompactOutcome> {
        let effective = revision.min(self.current_revision());
        if effective == 0 {
            return Ok(CompactOutcome {
                deleted_changelog: 0,
                deleted_tombstones: 0,
            });
        }
        let prev = self.compacted_revision()?;
        if effective <= prev {
            return Ok(CompactOutcome {
                deleted_changelog: 0,
                deleted_tombstones: 0,
            });
        }

        let mut deleted_changelog = 0usize;
        let mut deleted_tombstones = 0usize;

        // 分片删除：每批一个写事务（单写事务分片删除）
        loop {
            let mut batch_changelog = 0usize;
            let mut batch_tombstones = 0usize;
            self.backend.write(|tx| {
                // 1. changelog 条目（rev < effective；key 编码 /_changelog/{rev_be}）
                let mut stale: Vec<Vec<u8>> = Vec::new();
                for (key, _) in tx.iter_prefix(TABLE_CHANGELOG, CHANGELOG_PREFIX)? {
                    if stale.len() >= COMPACT_BATCH_SIZE {
                        break;
                    }
                    let rev = key
                        .get(CHANGELOG_PREFIX.len()..)
                        .and_then(|tail| <[u8; 8]>::try_from(tail).ok())
                        .map(u64::from_be_bytes);
                    if rev.is_some_and(|r| r < effective) {
                        stale.push(key.to_vec());
                    }
                }
                for key in &stale {
                    tx.remove(TABLE_CHANGELOG, key)?;
                }
                batch_changelog = stale.len();

                // 2. 过期 tombstone（deleted && mod_revision < effective）：
                //    物理删除 KV 行 + 元数据行
                let mut stale_tomb: Vec<Vec<u8>> = Vec::new();
                for (meta_key, meta_value) in tx.iter_prefix(TABLE_KV_META, KV_META_PREFIX)? {
                    if stale_tomb.len() >= COMPACT_BATCH_SIZE {
                        break;
                    }
                    if let Some(meta) = KvMetadata::from_bytes(&meta_value) {
                        if meta.deleted && (meta.mod_revision as u64) < effective {
                            stale_tomb.push(meta_key.to_vec());
                        }
                    }
                }
                for meta_key in &stale_tomb {
                    if let Some(user_key) = meta_key.strip_prefix(KV_META_PREFIX) {
                        let internal_key = encode_kv_key(user_key);
                        tx.remove(TABLE_KV, &internal_key)?;
                    }
                    tx.remove(TABLE_KV_META, meta_key)?;
                }
                batch_tombstones = stale_tomb.len();
                Ok(())
            })?;

            deleted_changelog += batch_changelog;
            deleted_tombstones += batch_tombstones;
            if batch_changelog == 0 && batch_tombstones == 0 {
                break;
            }
        }

        // 3. 持久化 compacted revision + applied（同一事务）
        self.backend.write(|tx| {
            tx.insert(TABLE_META, META_COMPACT_REVISION, &effective.to_be_bytes())?;
            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;
            // v1：compact 也消耗一个 log index，revision 跟随 applied；
            // v2：log-index 记录不进入 revision 空间（水位由 safe_ts 决定）。
            self.note_log_index_revision(tx, applied.index)
        })?;

        tracing::info!(
            "Compaction applied: revision={effective}, deleted_changelog={deleted_changelog}, \
             deleted_tombstones={deleted_tombstones}"
        );

        Ok(CompactOutcome {
            deleted_changelog,
            deleted_tombstones,
        })
    }

    /// 原始前缀扫描（不经 Barrier 解密）：供 `/_sys/auth/`、`/_lease/` 等
    /// 内部元数据前缀的启动装载使用。
    pub fn list_raw_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.backend.read(|tx| {
            let rows = tx.iter_prefix(TABLE_KV, prefix)?;
            let mut out = Vec::new();
            for (k, v) in rows {
                out.push((k.to_vec(), v.to_vec()));
            }
            Ok(out)
        })
    }

    /// 从指定 Revision 开始读取 Changelog 条目（含 start_revision）
    ///
    /// 用于 Watch 历史回放：新订阅者通过此方法获取 [start_revision, ∞) 的变更事件。
    pub fn read_changelog_entries(&self, start_revision: Revision) -> Result<Vec<ChangeEvent>> {
        let start_key = encode_changelog_key(start_revision);
        self.backend.read(|tx| {
            let entries = tx.iter_prefix(TABLE_CHANGELOG, CHANGELOG_PREFIX)?;
            let mut events = Vec::new();
            for (key, value) in entries {
                // 只读取 >= start_revision 的条目
                if key.as_slice() < start_key.as_slice() {
                    continue;
                }
                if let Ok(event) = ChangeEvent::from_bytes(&value) {
                    events.push(event);
                }
            }
            Ok(events)
        })
    }

    /// 严格读取 Changelog：损坏条目返回 Err（不再静默跳过）。
    ///
    /// Watch 历史回放使用此方法：损坏即中止回放并下发 `HistoryUnavailable`，
    /// 客户端不再收到静默缺洞。
    pub fn read_changelog_entries_strict(
        &self,
        start_revision: Revision,
    ) -> Result<Vec<ChangeEvent>> {
        let start_key = encode_changelog_key(start_revision);
        self.backend.read(|tx| {
            let entries = tx.iter_prefix(TABLE_CHANGELOG, CHANGELOG_PREFIX)?;
            let mut events = Vec::new();
            for (key, value) in entries {
                if key.as_slice() < start_key.as_slice() {
                    continue;
                }
                match ChangeEvent::from_bytes(&value) {
                    Ok(event) => events.push(event),
                    Err(_) => {
                        return Err(coord_core::error::Error::DataCorruption(format!(
                            "changelog entry at key {} is corrupt",
                            String::from_utf8_lossy(&key)
                        )))
                    }
                }
            }
            Ok(events)
        })
    }

    /// 读取 Key 在指定历史 Revision 时的值
    ///
    /// 通过扫描 Changelog 找到该 Key 在 <= target_revision 时的最后一次写入值。
    /// 如果 Key 在 target_revision 时不存在或已被删除，返回 None。
    ///
    /// 压缩（`apply_compact` 删除 `rev < compacted_revision` 的 changelog
    /// 条目）之后，单纯"扫 changelog 取最后一条"会**静默返回错误值**：
    ///
    /// - `target_revision <= compacted_revision` → 需要的条目已被物理删除
    ///   → 返回 `RevisionCompacted`（与 Watch 历史回放同口径）；
    /// - `target_revision > compacted_revision` 但该 Key 在**保留窗口内无任何写入**
    ///   → 它在 T 时刻的状态就等于当前状态（窗口内没变过）→ 返回实时值
    ///   （不得返回 `None`：那会让历史读说"不存在"、实时读却能读到）；
    /// - 该 Key 在 T 之后有写入、且 T 之前的写入已被压缩（值不可复原）
    ///   → 返回 `HistoryUnavailable`（gRPC `OUT_OF_RANGE`），拒绝给出不可信的答案。
    pub fn get_at_revision(
        &self,
        key: &[u8],
        target_revision: Revision,
    ) -> Result<Option<Vec<u8>>> {
        self.ensure_history_reconstructable(target_revision)?;
        let end_key = encode_changelog_key(target_revision.saturating_add(1));

        // 第一遍：定位 <= target_revision 的最后一次写入，并记录 (T, ∞) 是否有写入。
        let (last_at_or_before, written_after) = self.backend.read(|tx| {
            let mut last_value: Option<Option<Vec<u8>>> = None;
            let mut written_after = false;
            for (ch_key, ch_value) in tx.iter_prefix(TABLE_CHANGELOG, CHANGELOG_PREFIX)? {
                let is_after = ch_key.as_slice() >= end_key.as_slice();
                if let Ok(event) = ChangeEvent::from_bytes(&ch_value) {
                    for change in &event.changes {
                        if change.key != key {
                            continue;
                        }
                        if is_after {
                            written_after = true;
                        } else {
                            last_value = Some(change.value.clone());
                        }
                    }
                }
                // 已过目标 revision 且已确认有更新写入 → 无需继续
                if is_after && written_after {
                    break;
                }
            }
            Ok((last_value, written_after))
        })?;

        if let Some(value) = last_at_or_before {
            return Ok(value);
        }

        // 保留窗口内没有该 Key 的写入：T 时刻的状态 = 当前状态（窗口内未变）。
        if !written_after {
            return self.get(key);
        }

        // T 之后被改过，而 T 之前的写入已被压缩 → 无法复原，拒绝静默给错值。
        let created_after_target = self
            .get_kv_metadata(key)?
            .map(|m| m.create_revision as u64 > target_revision)
            .unwrap_or(false);
        if created_after_target {
            return Ok(None); // T 时刻该 Key 尚不存在
        }
        Err(Error::HistoryUnavailable {
            revision: target_revision,
            detail: format!(
                "historical read of key {} is not reconstructable: the key was written \
                 after that revision but its earlier history was compacted (read the \
                 live value or a revision after the last write)",
                String::from_utf8_lossy(key)
            ),
        })
    }

    /// 历史读的压缩水位守卫。
    ///
    /// `apply_compact` 删除 `rev < effective` 的 changelog 与过期 tombstone，因此
    /// `target_revision <= compacted_revision` 时的历史**不可复原**——直接返回
    /// 扫描结果（往往落入"早期无写入"分支）等于静默返回错误值。
    fn ensure_history_reconstructable(&self, target_revision: Revision) -> Result<()> {
        let compacted = self.compacted_revision()?;
        if target_revision <= compacted {
            return Err(Error::RevisionCompacted {
                revision: target_revision,
                oldest: compacted.saturating_add(1),
            });
        }
        Ok(())
    }
}

// ──── ChangelogReader impl ────

use crate::watch::ChangelogReader;

impl<B: StorageBackend> ChangelogReader for MvccStorage<B> {
    fn read_changelog_from(
        &self,
        start_revision: Revision,
    ) -> std::result::Result<Vec<ChangeEvent>, String> {
        // 严格读取，损坏条目报错（不静默跳过）
        MvccStorage::read_changelog_entries_strict(self, start_revision)
            .map_err(|e| format!("changelog read error: {e}"))
    }

    fn compacted_revision(&self) -> std::result::Result<Revision, String> {
        // 压缩水位（回放起点低于它时历史不可达）
        MvccStorage::compacted_revision(self).map_err(|e| format!("compacted revision: {e}"))
    }
}

// ──── 测试 ────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::redb_backend::RedbBackend;
    use coord_core::types::StorageConfig;
    use tempfile::TempDir;

    fn create_storage() -> (TempDir, MvccStorage<RedbBackend>) {
        let dir = TempDir::new().unwrap();
        let config = StorageConfig {
            data_dir: dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let backend = RedbBackend::open(dir.path(), &config).unwrap();
        let storage = MvccStorage::new(backend).unwrap();
        (dir, storage)
    }

    fn create_commit_ts_storage() -> (TempDir, MvccStorage<RedbBackend>) {
        let dir = TempDir::new().unwrap();
        let backend = RedbBackend::open(dir.path(), &StorageConfig::default()).unwrap();
        let storage = MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs).unwrap();
        (dir, storage)
    }

    // ──── Revision 与 log index 解耦（#138）────

    #[test]
    fn test_current_revision_uses_meta_not_log_index() {
        let (_dir, storage) = create_commit_ts_storage();
        storage
            .put_at_commit_ts(b"k", b"v", None, 100, applied_for(7))
            .unwrap();
        assert_eq!(storage.current_revision(), 100);
        assert_eq!(storage.get_applied_log_id().unwrap().unwrap().index, 7);
    }

    #[test]
    fn test_put_at_rejects_revision_regression() {
        let (_dir, storage) = create_commit_ts_storage();
        storage
            .put_at_commit_ts(b"k", b"v", None, 100, applied_for(7))
            .unwrap();
        let err = storage
            .put_at_commit_ts(b"k2", b"v", None, 99, applied_for(8))
            .expect_err("commit_ts below current revision must be rejected");
        match err {
            Error::CommitRejected {
                commit_ts,
                current_revision,
            } => assert_eq!((commit_ts, current_revision), (99, 100)),
            other => panic!("expected CommitRejected, got {other:?}"),
        }
        assert_eq!(storage.get(b"k2").unwrap(), None, "拒绝不得落盘");
        assert_eq!(storage.current_revision(), 100);
    }

    #[test]
    fn test_commit_ts_not_clamped_by_applied_index() {
        let (_dir, storage) = create_commit_ts_storage();
        storage
            .put_at_commit_ts(b"k", b"v", None, 1_000_000, applied_for(3))
            .unwrap();
        assert_eq!(storage.current_revision(), 1_000_000);
        assert_eq!(
            storage.get_kv_metadata(b"k").unwrap().unwrap().mod_revision,
            1_000_000
        );
        // 内部 log-index 记录不得把 revision 钳回 log index
        storage
            .put_at_revision(b"/_sys/x", b"v", None, 4, applied_for(4))
            .unwrap();
        assert_eq!(storage.current_revision(), 1_000_000);
    }

    #[test]
    fn test_commit_ts_replay_is_idempotent() {
        let (_dir, storage) = create_commit_ts_storage();
        let first = storage
            .put_at_commit_ts(b"k", b"v1", None, 100, applied_for(7))
            .unwrap();
        let replay = storage
            .put_at_commit_ts(b"k", b"v1", None, 100, applied_for(7))
            .unwrap();
        assert!(!first.replayed);
        assert!(
            replay.replayed,
            "同一 (key, value, commit_ts) 必须判定为重放"
        );
        assert_eq!(storage.get(b"k").unwrap().as_deref(), Some(&b"v1"[..]));
        assert_eq!(
            storage.get_kv_metadata(b"k").unwrap().unwrap().version,
            1,
            "重放不得重复写元数据"
        );
        assert_eq!(storage.current_revision(), 100);

        // 同一 commit_ts 承载另一条命令不是重放：静默成功会让第二次写丢失。
        let err = storage
            .put_at_commit_ts(b"k", b"v2", None, 100, applied_for(8))
            .expect_err("different value at the same commit_ts must be rejected");
        assert!(matches!(err, Error::CommitRejected { commit_ts: 100, .. }));
        assert_eq!(storage.get(b"k").unwrap().as_deref(), Some(&b"v1"[..]));
    }

    #[test]
    fn test_advance_commit_ts_watermark_is_monotonic_persistent_and_replay_safe() {
        let (dir, storage) = create_commit_ts_storage();
        storage
            .put_at_commit_ts(b"k", b"v", None, 100, applied_for(1))
            .unwrap();

        assert_eq!(
            storage
                .advance_commit_ts_watermark(150, applied_for(2))
                .unwrap(),
            150
        );
        assert_eq!(storage.current_revision(), 150);
        assert_eq!(storage.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
        assert!(
            !storage.changelog_contains_revision(150).unwrap(),
            "fence 不是 commit，不得写 changelog"
        );

        // 只前进不后退；同一 applied 重放返回当前水位。
        assert_eq!(
            storage
                .advance_commit_ts_watermark(120, applied_for(3))
                .unwrap(),
            150
        );
        assert_eq!(
            storage
                .advance_commit_ts_watermark(120, applied_for(3))
                .unwrap(),
            150
        );
        assert_eq!(storage.current_revision(), 150);

        let err = storage
            .put_at_commit_ts(b"k2", b"v", None, 149, applied_for(4))
            .expect_err("commit_ts below the fence must be rejected");
        assert!(matches!(
            err,
            Error::CommitRejected {
                commit_ts: 149,
                current_revision: 150
            }
        ));

        drop(storage);
        let backend = RedbBackend::open(dir.path(), &StorageConfig::default()).unwrap();
        let reopened =
            MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs).unwrap();
        assert_eq!(reopened.current_revision(), 150);
        assert_eq!(reopened.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
    }

    #[test]
    fn test_safe_ts_fence_is_monotonic_and_persistent() {
        let (dir, storage) = create_storage();
        assert_eq!(storage.safe_ts_fence().unwrap(), 0);

        assert_eq!(storage.set_safe_ts_fence(900, applied_for(1)).unwrap(), 900);
        assert_eq!(
            storage.set_safe_ts_fence(800, applied_for(2)).unwrap(),
            900,
            "fence 只前进"
        );
        assert_eq!(storage.safe_ts_fence().unwrap(), 900);

        drop(storage);
        let backend = RedbBackend::open(dir.path(), &StorageConfig::default()).unwrap();
        let reopened = MvccStorage::new(backend).unwrap();
        assert_eq!(reopened.safe_ts_fence().unwrap(), 900);
    }

    #[test]
    fn test_commit_watermark_is_monotonic_persistent_and_replay_safe() {
        let (dir, storage) = create_storage();
        assert_eq!(storage.commit_watermark().unwrap(), 0);

        assert_eq!(
            storage
                .publish_commit_watermark(900, applied_for(1))
                .unwrap(),
            900
        );
        assert_eq!(
            storage
                .publish_commit_watermark(800, applied_for(2))
                .unwrap(),
            900,
            "watermark 只前进"
        );
        assert_eq!(storage.commit_watermark().unwrap(), 900);

        // 同一 applied 重放：返回当前值，不改写。
        assert_eq!(
            storage
                .publish_commit_watermark(950, applied_for(2))
                .unwrap(),
            900
        );

        drop(storage);
        let backend = RedbBackend::open(dir.path(), &StorageConfig::default()).unwrap();
        let reopened = MvccStorage::new(backend).unwrap();
        assert_eq!(reopened.commit_watermark().unwrap(), 900);
    }

    #[test]
    fn test_put_at_commit_ts_rejects_different_key_at_same_revision() {
        let (_dir, storage) = create_commit_ts_storage();
        storage
            .put_at_commit_ts(b"a", b"1", None, 100, applied_for(1))
            .unwrap();
        let err = storage
            .put_at_commit_ts(b"b", b"2", None, 100, applied_for(2))
            .expect_err("a different key at the same commit_ts is not a replay");
        assert!(matches!(err, Error::CommitRejected { commit_ts: 100, .. }));
        assert_eq!(storage.get(b"b").unwrap(), None, "不得静默丢写");
        assert_eq!(storage.get(b"a").unwrap().as_deref(), Some(&b"1"[..]));
        assert_eq!(storage.current_revision(), 100);
    }

    #[test]
    fn test_delete_at_commit_ts_rejects_put_at_same_revision() {
        let (_dir, storage) = create_commit_ts_storage();
        storage
            .put_at_commit_ts(b"k", b"v", None, 100, applied_for(1))
            .unwrap();
        let err = storage
            .delete_at_commit_ts(b"k", 100, applied_for(2))
            .expect_err("a delete at a revision that recorded a put is not a replay");
        assert!(matches!(err, Error::CommitRejected { commit_ts: 100, .. }));
        assert_eq!(storage.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
    }

    #[test]
    fn test_delete_range_replay_requires_exact_deleted_set() {
        let (_dir, storage) = create_commit_ts_storage();
        storage
            .put_at_commit_ts(b"a", b"1", None, 100, applied_for(1))
            .unwrap();
        storage
            .put_at_commit_ts(b"b", b"2", None, 101, applied_for(2))
            .unwrap();
        let first = storage
            .delete_range_at_commit_ts(b"a", b"c", 102, applied_for(3))
            .unwrap();
        assert_eq!(first.deleted_keys.len(), 2);

        let replay = storage
            .delete_range_at_commit_ts(b"a", b"c", 102, applied_for(3))
            .unwrap();
        assert!(replay.replayed, "同 range/commit_ts 的重复删除才是重放");

        // 同 commit_ts 的“更大范围”若包含新的存活 key，不能当重放静默吞掉。
        storage
            .put_at_commit_ts(b"c", b"3", None, 103, applied_for(4))
            .unwrap();
        let err = storage
            .delete_range_at_commit_ts(b"a", b"z", 102, applied_for(5))
            .expect_err("a wider range at the same commit_ts is not a replay");
        assert!(matches!(err, Error::CommitRejected { commit_ts: 102, .. }));
        assert_eq!(storage.get(b"c").unwrap().as_deref(), Some(&b"3"[..]));
    }

    #[test]
    fn test_delete_range_noop_replay_rejects_when_keys_appeared() {
        let (_dir, storage) = create_commit_ts_storage();
        let first = storage
            .delete_range_at_commit_ts(b"a", b"c", 100, applied_for(1))
            .unwrap();
        assert!(first.deleted_keys.is_empty());

        let replay = storage
            .delete_range_at_commit_ts(b"a", b"c", 100, applied_for(2))
            .unwrap();
        assert!(replay.replayed, "no-op range delete replay stays a no-op");

        storage
            .put_at_commit_ts(b"b", b"2", None, 101, applied_for(3))
            .unwrap();
        let err = storage
            .delete_range_at_commit_ts(b"a", b"c", 100, applied_for(4))
            .expect_err("new keys in the range must make the old no-op non-replayable");
        assert!(matches!(err, Error::CommitRejected { commit_ts: 100, .. }));
        assert_eq!(storage.get(b"b").unwrap().as_deref(), Some(&b"2"[..]));
    }

    #[test]
    fn test_commit_ts_survives_reopen() {
        let (dir, storage) = create_commit_ts_storage();
        storage
            .put_at_commit_ts(b"k", b"v", None, 100, applied_for(7))
            .unwrap();
        drop(storage);
        let reopened = MvccStorage::new_with_revision_mode(
            RedbBackend::open(dir.path(), &StorageConfig::default()).unwrap(),
            RevisionMode::CommitTs,
        )
        .unwrap();
        assert_eq!(reopened.current_revision(), 100, "重启后不倒退");
        let err = reopened
            .put_at_commit_ts(b"k2", b"v", None, 99, applied_for(8))
            .expect_err("重启后仍必须拒绝回退");
        assert!(matches!(err, Error::CommitRejected { .. }));
    }

    /// #139：compact 水位跟 current revision（commit_ts），不再被 applied index 钳制。
    #[test]
    fn test_compact_watermark_uses_current_revision_not_applied_index() {
        let (_dir, storage) = create_commit_ts_storage();
        storage
            .put_at_commit_ts(b"k", b"v", None, 1_000, applied_for(3))
            .unwrap();
        storage.apply_compact(1_000, applied_for(5)).unwrap();
        assert_eq!(
            storage.compacted_revision().unwrap(),
            1_000,
            "水位必须取 current revision，不是 applied.index=5"
        );
        assert_eq!(storage.current_revision(), 1_000);
        assert!(storage.changelog_contains_revision(1_000).unwrap());
    }

    /// #139：compact(1000) 删除 <1000 的 changelog，保留 1000 本身。
    #[test]
    fn test_compact_deletes_only_below_commit_ts_watermark() {
        let (_dir, storage) = create_commit_ts_storage();
        storage
            .put_at_commit_ts(b"a", b"1", None, 900, applied_for(3))
            .unwrap();
        storage
            .put_at_commit_ts(b"b", b"2", None, 1_000, applied_for(4))
            .unwrap();
        storage.apply_compact(1_000, applied_for(5)).unwrap();
        assert!(
            !storage.changelog_contains_revision(900).unwrap(),
            "低于水位的 changelog 必须删除"
        );
        assert!(
            storage.changelog_contains_revision(1_000).unwrap(),
            "水位自身的 changelog 不得删除"
        );
        assert_eq!(storage.compacted_revision().unwrap(), 1_000);
    }

    /// #139：未来 revision 的防御性钳制只到 current revision（不得 fail-open 到 log index）。
    #[test]
    fn test_compact_future_revision_clamped_to_current_revision() {
        let (_dir, storage) = create_commit_ts_storage();
        storage
            .put_at_commit_ts(b"k", b"v", None, 1_000, applied_for(3))
            .unwrap();
        storage.apply_compact(5_000, applied_for(5)).unwrap();
        assert_eq!(
            storage.compacted_revision().unwrap(),
            1_000,
            "未来 revision 只能钳到 current revision"
        );
    }

    #[test]
    fn test_log_index_mode_revision_matches_applied_index() {
        let (_dir, storage) = create_storage();
        storage
            .put_at_revision(b"k", b"v", None, 7, applied_for(7))
            .unwrap();
        assert_eq!(storage.current_revision(), 7);
        let err = storage
            .put_at_revision(b"k2", b"v", None, 6, applied_for(8))
            .expect_err("v1 revision 回退必须显式拒绝");
        assert!(matches!(err, Error::CommitRejected { .. }));
    }

    #[test]
    fn test_initial_revision_is_zero() {
        let (_dir, storage) = create_storage();
        assert_eq!(storage.current_revision(), 0);
    }

    #[test]
    fn test_put_and_get() {
        let (_dir, storage) = create_storage();
        let rev = storage.put(b"hello", b"world", None).unwrap();
        assert_eq!(rev, 1);
        assert_eq!(storage.current_revision(), 1);

        let value = storage.get(b"hello").unwrap();
        assert_eq!(value, Some(b"world".to_vec()));
    }

    #[test]
    fn test_revision_monotonic() {
        let (_dir, storage) = create_storage();
        let rev1 = storage.put(b"key1", b"val1", None).unwrap();
        let rev2 = storage.put(b"key2", b"val2", None).unwrap();
        let rev3 = storage.put(b"key3", b"val3", None).unwrap();
        assert!(rev1 < rev2);
        assert!(rev2 < rev3);
        assert_eq!(storage.current_revision(), 3);
    }

    #[test]
    fn test_delete() {
        let (_dir, storage) = create_storage();
        storage.put(b"key1", b"val1", None).unwrap();
        assert!(storage.get(b"key1").unwrap().is_some());

        storage.delete(b"key1").unwrap();
        // Delete 后 get 返回 None（tombstone 被过滤）
        assert_eq!(storage.get(b"key1").unwrap(), None);
    }

    #[test]
    fn test_range_prefix() {
        let (_dir, storage) = create_storage();
        storage.put(b"/app/config/a", b"1", None).unwrap();
        storage.put(b"/app/config/b", b"2", None).unwrap();
        storage.put(b"/app/data/x", b"3", None).unwrap();

        let results = storage.range(b"/app/config/", 0).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_range_limit() {
        let (_dir, storage) = create_storage();
        storage.put(b"/app/a", b"1", None).unwrap();
        storage.put(b"/app/b", b"2", None).unwrap();
        storage.put(b"/app/c", b"3", None).unwrap();

        let results = storage.range(b"/app/", 2).unwrap();
        assert_eq!(results.len(), 2);
    }

    // ──── R-SVC-07：半开区间 [key, range_end) 语义 ────

    #[test]
    fn test_range_in_half_open_interval() {
        let (_dir, storage) = create_storage();
        storage.put(b"a", b"1", None).unwrap();
        storage.put(b"ab", b"2", None).unwrap();
        storage.put(b"abc", b"3", None).unwrap();
        storage.put(b"b", b"4", None).unwrap();
        storage.put(b"c", b"5", None).unwrap();

        // [a, b) = a, ab, abc（不含 b）
        let results = storage.range_in(b"a", b"b", 0).unwrap();
        let keys: Vec<&[u8]> = results.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(
            keys,
            vec![b"a".as_slice(), b"ab".as_slice(), b"abc".as_slice()]
        );

        // [b, c) = b（不含 c）
        let results = storage.range_in(b"b", b"c", 0).unwrap();
        let keys: Vec<&[u8]> = results.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(keys, vec![b"b".as_slice()]);

        // 区间内 key 不共享 start 前缀（start=ab, end=c 应含 abc、b 不含 ab）
        let results = storage.range_in(b"ab", b"c", 0).unwrap();
        let keys: Vec<&[u8]> = results.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(
            keys,
            vec![b"ab".as_slice(), b"abc".as_slice(), b"b".as_slice()]
        );

        // limit 生效
        let results = storage.range_in(b"a", b"b", 2).unwrap();
        assert_eq!(results.len(), 2);

        // 软删除的 key 不可见
        storage.delete(b"ab").unwrap();
        let results = storage.range_in(b"a", b"b", 0).unwrap();
        let keys: Vec<&[u8]> = results.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(keys, vec![b"a".as_slice(), b"abc".as_slice()]);
    }

    #[test]
    fn test_delete_range_at_revision_atomic() {
        let (_dir, storage) = create_storage();
        storage.put(b"a", b"1", None).unwrap();
        storage.put(b"ab", b"2", None).unwrap();
        storage.put(b"b", b"3", None).unwrap();

        // 原子删除 [a, b)
        let outcome = storage
            .delete_range_at_revision(
                b"a",
                b"b",
                4,
                AppliedLogId {
                    term: 1,
                    node_id: 1,
                    index: 4,
                },
            )
            .unwrap();
        assert!(!outcome.replayed);
        assert_eq!(outcome.deleted_keys, vec![b"a".to_vec(), b"ab".to_vec()]);

        // 范围内已删除、范围外保留
        assert_eq!(storage.get(b"a").unwrap(), None);
        assert_eq!(storage.get(b"ab").unwrap(), None);
        assert_eq!(storage.get(b"b").unwrap(), Some(b"3".to_vec()));

        // 幂等守卫：同 revision 重放不产生副作用
        let replay = storage
            .delete_range_at_revision(
                b"a",
                b"b",
                4,
                AppliedLogId {
                    term: 1,
                    node_id: 1,
                    index: 4,
                },
            )
            .unwrap();
        assert!(replay.replayed);

        // 范围读确认无残留
        assert!(storage.range_in(b"a", b"b", 0).unwrap().is_empty());
    }

    #[test]
    fn test_range_at_revision_historical_view() {
        let (_dir, storage) = create_storage();
        storage.put(b"a", b"v1", None).unwrap(); // rev 1
        storage.put(b"b", b"v1", None).unwrap(); // rev 2
        storage.put(b"a", b"v2", None).unwrap(); // rev 3

        // rev 2 历史视图：a=v1, b=v1
        let results = storage.range_at_revision(b"a", b"c", 0, 2).unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.contains(&(b"a".to_vec(), b"v1".to_vec())));
        assert!(results.contains(&(b"b".to_vec(), b"v1".to_vec())));

        // rev 3 历史视图：a=v2, b=v1
        let results = storage.range_at_revision(b"a", b"c", 0, 3).unwrap();
        assert!(results.contains(&(b"a".to_vec(), b"v2".to_vec())));

        // 删除后的历史视图：rev 3 前 b 存在，rev 4（删除 b）后不可见
        storage.delete(b"b").unwrap(); // rev 4
        let results = storage.range_at_revision(b"a", b"c", 0, 3).unwrap();
        assert!(results.contains(&(b"b".to_vec(), b"v1".to_vec())));
        let results = storage.range_at_revision(b"a", b"c", 0, 4).unwrap();
        assert!(!results.iter().any(|(k, _)| k == b"b"));
    }

    // ──── 压缩后的历史读不得静默返回错误值 ────

    #[test]
    fn test_range_at_revision_metadata_reflects_state_at_target() {
        let (_dir, storage) = create_commit_ts_storage();
        storage
            .put_at_commit_ts(b"k", b"v1", None, 10, applied_for(1))
            .unwrap();
        storage
            .put_at_commit_ts(b"k", b"v2", None, 20, applied_for(2))
            .unwrap();
        storage
            .delete_at_commit_ts(b"k", 30, applied_for(3))
            .unwrap();

        let at_15 = storage
            .range_at_revision_with_metadata(b"a", b"z", 0, 15)
            .unwrap();
        assert_eq!(at_15.len(), 1);
        let (key, value, meta) = &at_15[0];
        assert_eq!(key, b"k");
        assert_eq!(value, b"v1");
        assert_eq!(meta.version, 1);
        assert_eq!(meta.create_revision, 10);
        assert_eq!(meta.mod_revision, 10);

        let at_25 = storage
            .range_at_revision_with_metadata(b"a", b"z", 0, 25)
            .unwrap();
        assert_eq!(at_25.len(), 1);
        let (_, value, meta) = &at_25[0];
        assert_eq!(value, b"v2");
        assert_eq!(meta.version, 2);
        assert_eq!(meta.create_revision, 10);
        assert_eq!(meta.mod_revision, 20);

        assert!(
            storage
                .range_at_revision_with_metadata(b"a", b"z", 0, 35)
                .unwrap()
                .is_empty(),
            "delete@30 must remove the key from the revision-35 view"
        );
    }

    #[test]
    fn test_range_at_revision_metadata_ignores_noop_deletes_after_target() {
        let (_dir, storage) = create_commit_ts_storage();
        storage
            .put_at_commit_ts(b"k", b"v1", None, 10, applied_for(1))
            .unwrap();
        // 第一次 delete 生效（version 2），第二次对不存在 key 的 delete 是
        // no-op：写 changelog 但不增加 version。
        storage
            .delete_at_commit_ts(b"k", 20, applied_for(2))
            .unwrap();
        storage
            .delete_at_commit_ts(b"k", 30, applied_for(3))
            .unwrap();

        let results = storage
            .range_at_revision_with_metadata(b"a", b"z", 0, 15)
            .unwrap();
        assert_eq!(results.len(), 1);
        let (_, value, meta) = &results[0];
        assert_eq!(value, b"v1");
        assert_eq!(
            meta.version, 1,
            "no-op deletes after the target must not be subtracted from version"
        );
        assert_eq!(meta.create_revision, 10);
        assert_eq!(meta.mod_revision, 10);
    }

    #[test]
    fn test_range_at_revision_metadata_after_compaction_uses_live_state() {
        let (_dir, storage) = create_commit_ts_storage();
        storage
            .put_at_commit_ts(b"k", b"v1", None, 10, applied_for(1))
            .unwrap();
        storage
            .put_at_commit_ts(b"k", b"v2", None, 20, applied_for(2))
            .unwrap();
        storage
            .apply_compact(15, AppliedLogId::standalone(3))
            .unwrap();

        let results = storage
            .range_at_revision_with_metadata(b"a", b"z", 0, 25)
            .unwrap();
        assert_eq!(results.len(), 1);
        let (_, value, meta) = &results[0];
        assert_eq!(value, b"v2");
        assert_eq!(meta.version, 2);
        assert_eq!(meta.create_revision, 10);
        assert_eq!(meta.mod_revision, 20);
    }

    /// 压缩水位之前的修订不可复原 → 必须报 `RevisionCompacted`（与 Watch 同口径），
    /// 而不是"扫不到就返回 None"（历史读说"不存在"，实时读却能读到）。
    #[test]
    fn test_get_at_revision_rejects_compacted_revision() {
        let (_dir, storage) = create_storage();
        storage.put(b"k", b"v1", None).unwrap(); // rev 1
        storage.put(b"k", b"v2", None).unwrap(); // rev 2
        storage.put(b"k", b"v3", None).unwrap(); // rev 3
        storage
            .apply_compact(2, AppliedLogId::standalone(5))
            .unwrap();

        // target <= compacted → 明确报错
        let err = storage.get_at_revision(b"k", 2).expect_err("must Err");
        assert!(
            matches!(
                err,
                Error::RevisionCompacted {
                    revision: 2,
                    oldest: 3
                }
            ),
            "unexpected error: {err:?}"
        );
        // 范围历史读同口径
        let err = storage
            .range_at_revision(b"a", b"z", 0, 1)
            .expect_err("must Err");
        assert!(matches!(err, Error::RevisionCompacted { .. }));
        // 水位之上的修订仍可读
        assert_eq!(
            storage.get_at_revision(b"k", 3).unwrap(),
            Some(b"v3".to_vec())
        );
    }

    /// 压缩后，**保留窗口内未被写过**的 Key 在历史视图中必须仍然可见
    /// （其历史写入虽被删除，但状态与当前一致）。
    #[test]
    fn test_historical_read_keeps_keys_untouched_since_compaction() {
        let (_dir, storage) = create_storage();
        storage.put(b"a", b"a-old", None).unwrap(); // rev 1
        storage.put(b"b", b"b-old", None).unwrap(); // rev 2
        storage.put(b"b", b"b-new", None).unwrap(); // rev 3
        storage
            .apply_compact(2, AppliedLogId::standalone(5))
            .unwrap();

        // 单键：a 在窗口内没被写过 → T=3 时刻状态 = 当前状态
        assert_eq!(
            storage.get_at_revision(b"a", 3).unwrap(),
            Some(b"a-old".to_vec())
        );
        // 范围：a 必须在历史视图里（不得被静默漏掉）
        let results = storage.range_at_revision(b"a", b"c", 0, 3).unwrap();
        assert!(
            results.contains(&(b"a".to_vec(), b"a-old".to_vec())),
            "keys untouched since compaction must stay visible: {results:?}"
        );
        assert!(results.contains(&(b"b".to_vec(), b"b-new".to_vec())));
    }

    /// T 之后有写入、且 T 之前的历史已被压缩 → 值不可复原：必须报错，
    /// 不得返回 None 或别的猜测值。
    #[test]
    fn test_historical_read_rejects_unreconstructable_key() {
        let (_dir, storage) = create_storage();
        storage.put(b"k", b"v1", None).unwrap(); // rev 1（k 的写入）
        storage.put(b"filler", b"x", None).unwrap(); // rev 2
                                                     // 水位 2：删除 rev < 2 的 changelog → k 的 rev1 条目被物理删除
        storage
            .apply_compact(2, AppliedLogId::standalone(5))
            .unwrap();
        storage.put(b"k", b"v2", None).unwrap(); // rev 6

        // T=4：在水位之上（守卫放行），但 k 在 [2,4] 无写入、T 之后（rev6）有写入，
        // 且 k 在 T 时刻已存在（create_revision=1）→ 值不可复原 → 拒绝猜测。
        let err = storage
            .get_at_revision(b"k", 4)
            .expect_err("must not silently guess");
        assert!(
            matches!(err, Error::HistoryUnavailable { .. }),
            "err: {err:?}"
        );

        // 范围历史读同口径（不得给出残缺视图）
        assert!(storage
            .range_at_revision(b"k", b"l", 0, 4)
            .expect_err("must not silently return a partial view")
            .to_string()
            .contains("not reconstructable"));

        // 保留窗口内的修订仍可读
        assert_eq!(
            storage.get_at_revision(b"k", 6).unwrap(),
            Some(b"v2".to_vec())
        );
        // 水位之下的修订明确报 RevisionCompacted
        let err = storage.get_at_revision(b"k", 2).expect_err("must Err");
        assert!(
            matches!(err, Error::RevisionCompacted { .. }),
            "err: {err:?}"
        );
    }

    /// 回归（**范围路径**）：T 时刻存在、T 之后**被删除**、T 之前的历史已被压缩的
    /// Key 若不进 `view`、也不在存活集合、更不进 `unreconstructable`，就会被
    /// **静默漏报**，范围读返回"残缺但看起来正常"的视图；必须与单键路径
    /// （`get_at_revision`）同口径报错。
    #[test]
    fn test_historical_range_read_rejects_key_deleted_after_target() {
        let (_dir, storage) = create_storage();
        storage.put(b"g", b"g1", None).unwrap(); // rev 1
        storage.put(b"filler", b"x", None).unwrap(); // rev 2
                                                     // 水位 2：物理删除 rev < 2 的 changelog → g 的 rev1 条目消失
        storage
            .apply_compact(2, AppliedLogId::standalone(5))
            .unwrap();
        storage.delete(b"g").unwrap(); // rev 6：T 之后被删除

        // 单键：明确报错
        let single = storage
            .get_at_revision(b"g", 4)
            .expect_err("single-key read must Err");
        assert!(
            matches!(single, Error::HistoryUnavailable { .. }),
            "err: {single:?}"
        );

        // 范围：必须同口径报错——不得静默返回"不含 g"的空视图
        let range = storage
            .range_at_revision(b"g", b"h", 0, 4)
            .expect_err("range read must not silently drop a key that existed at T");
        assert!(
            matches!(range, Error::HistoryUnavailable { .. }),
            "err: {range:?}"
        );
        assert!(
            range.to_string().contains("not reconstructable"),
            "err: {range}"
        );
    }

    /// R-SVC-07-4：Txn 内 Range 必须过滤已软删除的 Key（与顶层 range() 对齐）
    #[test]
    fn test_txn_range_filters_deleted_keys() {
        use crate::txn::{TxnOp, TxnOpResponse};

        let (_dir, storage) = create_storage();
        storage.put(b"a", b"1", None).unwrap();
        storage.put(b"b", b"2", None).unwrap();
        // 软删除 a（值仍在 /kv/ 中，仅 KV_META 标记 deleted）
        storage.delete(b"a").unwrap();

        let result = storage
            .execute_txn(
                &[],
                &[TxnOp::Range {
                    key: b"a".to_vec(),
                    range_end: b"c".to_vec(),
                    limit: 0,
                }],
                &[],
            )
            .unwrap();
        assert_eq!(result.responses.len(), 1);
        match &result.responses[0] {
            TxnOpResponse::Range { kvs, count, .. } => {
                assert_eq!(*count, 1, "已删除 key a 不得在 Txn Range 中复活");
                assert_eq!(kvs.len(), 1);
                assert_eq!(kvs[0].0, b"b".to_vec());
            }
            other => panic!("expected Range response, got {:?}", other),
        }
    }

    #[test]
    fn test_applied_log_id() {
        let (_dir, storage) = create_storage();
        assert_eq!(storage.get_applied_log_id().unwrap(), None);

        storage
            .set_last_applied(AppliedLogId::standalone(42))
            .unwrap();
        assert_eq!(
            storage.get_applied_log_id().unwrap(),
            Some(AppliedLogId::standalone(42))
        );
        assert_eq!(storage.current_revision(), 42);
    }

    /// `AppliedLogId` 的持久化编码**不得有失败路径**（手写定长）：编码绝不为
    /// 空（空字节会让重启把水位读回 `None` → **从 0 重放整个 raft 日志**）；
    /// 非 `ALI1` 前缀/截断字节一律 `None`。
    #[test]
    fn test_applied_log_id_encoding_is_total_and_strict() {
        let id = AppliedLogId {
            term: 7,
            node_id: 3,
            index: 1234567890123,
        };
        let bytes = id.to_bytes();
        assert_eq!(bytes.len(), 28, "4B 魔数 + 3×8B 定长");
        assert!(!bytes.is_empty(), "编码绝不允许为空");
        assert_eq!(AppliedLogId::from_bytes(&bytes), Some(id));

        // 退役格式（旧 bincode 定长 24B）⇒ None（P3：不再回落解码，
        // 调用方按「水位缺失」处理并由日志重放兜底）
        let retired = [0u8; 24];
        assert_eq!(
            AppliedLogId::from_bytes(&retired),
            None,
            "退役 bincode 行必须显式丢弃（不得误读为水位）"
        );

        // 空字节 / 截断字节一律 None（调用方据此判定水位缺失），
        // 而**新编码永远不会产生空字节**。
        assert_eq!(AppliedLogId::from_bytes(&[]), None);
        assert_eq!(AppliedLogId::from_bytes(b"ALI1"), None);
    }

    // ──── 静态加密接线（Barrier/Seal/Unseal） ────

    #[test]
    fn test_barrier_encrypts_user_values_on_disk() {
        use crate::security::key_management::Keyring;
        use std::sync::Arc;

        let (_dir, storage) = create_storage();
        let (keyring, _dek) = Keyring::bootstrap_from_root_key(&[0xABu8; 32]).unwrap();
        storage.set_barrier(crate::security::barrier::Barrier::new(Arc::new(keyring)));

        storage.put(b"secret", b"super-secret-value", None).unwrap();

        // 落盘原始字节不得含明文用户值
        let raw = storage
            .backend()
            .read(|tx| tx.get(TABLE_KV, &encode_kv_key(b"secret")))
            .unwrap()
            .unwrap();
        assert!(
            !raw.windows(b"super-secret-value".len())
                .any(|w| w == b"super-secret-value"),
            "on-disk value must not contain plaintext"
        );
        assert!(raw.len() >= 32, "ciphertext must carry barrier header");

        // 读取仍返回明文（Barrier 透明解密）
        assert_eq!(
            storage.get(b"secret").unwrap(),
            Some(b"super-secret-value".to_vec())
        );
        // 范围读同样解密
        let results = storage.range_in(b"sec", b"secx", 0).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, b"super-secret-value".to_vec());
    }

    #[test]
    fn test_barrier_seal_blocks_writes_after_unseal_restores() {
        use crate::security::key_management::Keyring;
        use std::sync::Arc;

        let (_dir, storage) = create_storage();
        let root_key = [0xCDu8; 32];
        let (keyring, dek) = Keyring::bootstrap_from_root_key(&root_key).unwrap();
        let keyring = Arc::new(keyring);
        storage.set_barrier(crate::security::barrier::Barrier::new(Arc::clone(&keyring)));
        storage.put(b"a", b"v1", None).unwrap();

        // Seal → 写入被拒（active_dek 返回错误，非全零密钥）
        keyring.seal();
        assert!(keyring.is_sealed());
        assert!(
            storage.put(b"b", b"v2", None).is_err(),
            "sealed: writes refused"
        );

        // Unseal（root 密钥重建）→ 读取正常
        let recovered = Keyring::from_root_key(&root_key, &[dek]).unwrap();
        assert!(!recovered.is_sealed());
        storage.set_barrier(crate::security::barrier::Barrier::new(Arc::new(recovered)));
        assert_eq!(storage.get(b"a").unwrap(), Some(b"v1".to_vec()));
        let rev = storage.put(b"c", b"v3", None).unwrap();
        assert!(rev > 0);
        assert_eq!(storage.get(b"c").unwrap(), Some(b"v3".to_vec()));
    }

    /// 内部记录（lease/auth 等非 /kv/ 前缀）不得被 Barrier 加密，
    /// 否则结构化记录（如 LeaseRecord 24B）解析会失败。
    #[test]
    fn test_barrier_does_not_encrypt_internal_records() {
        use crate::security::key_management::Keyring;
        use std::sync::Arc;

        let (_dir, storage) = create_storage();
        let (keyring, _dek) = Keyring::bootstrap_from_root_key(&[0x11u8; 32]).unwrap();
        storage.set_barrier(crate::security::barrier::Barrier::new(Arc::new(keyring)));

        // 直接写一条 /_lease/ 内部记录（绕过 raft，模拟内部记录落盘）
        let lease_key = crate::storage::mvcc::encode_lease_key(42);
        let record = [0u8; 24];
        storage
            .backend()
            .write(|tx| tx.insert(TABLE_KV, &lease_key, &record))
            .unwrap();

        // 内部记录以明文读取（长度与内容不变，无需解密）
        let raw = storage
            .backend()
            .read(|tx| tx.get(TABLE_KV, &lease_key))
            .unwrap()
            .unwrap();
        assert_eq!(raw.len(), 24, "internal records must stay plaintext");
    }

    // ──── 新语义：revision ≡ log index + 幂等守卫 + applied 持久化 ────

    #[test]
    fn test_put_at_revision_uses_log_index_as_revision() {
        let (_dir, storage) = create_storage();
        // raft log index 7 → revision 7（不再从内存计数器分配）
        let outcome = storage
            .put_at_revision(
                b"k",
                b"v",
                None,
                7,
                AppliedLogId {
                    term: 1,
                    node_id: 1,
                    index: 7,
                },
            )
            .unwrap();
        assert!(!outcome.replayed);
        assert_eq!(storage.current_revision(), 7);
        assert_eq!(
            storage.get_applied_log_id().unwrap(),
            Some(AppliedLogId {
                term: 1,
                node_id: 1,
                index: 7
            })
        );
        assert_eq!(storage.get(b"k").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn test_apply_idempotence_guard() {
        let (_dir, storage) = create_storage();
        let applied = AppliedLogId {
            term: 1,
            node_id: 1,
            index: 7,
        };
        let outcome1 = storage
            .put_at_revision(b"k", b"v1", None, 7, applied)
            .unwrap();
        assert!(!outcome1.replayed);

        // 同 revision 重复 apply（重启重放/重复 apply）→ 跳过，无副作用
        let outcome2 = storage
            .put_at_revision(b"k", b"v2", None, 7, applied)
            .unwrap();
        assert!(outcome2.replayed);
        assert_eq!(storage.get(b"k").unwrap(), Some(b"v1".to_vec()));

        // changelog 只有一条记录
        let entries = storage.read_changelog_entries(1).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].revision, 7);
    }

    #[test]
    fn test_delete_always_consumes_revision() {
        let (_dir, storage) = create_storage();
        // 不存在的 Key 也消耗 revision 并写 changelog
        let outcome = storage
            .delete_at_revision(b"missing", 5, AppliedLogId::standalone(5))
            .unwrap();
        assert!(!outcome.replayed);
        assert_eq!(storage.current_revision(), 5);
        let entries = storage.read_changelog_entries(5).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].event_type, EventType::Delete);
    }

    #[test]
    fn test_verify_consistency() {
        let (_dir, storage) = create_storage();
        // 空库
        let (applied, tail) = storage.verify_consistency().unwrap();
        assert_eq!(applied, 0);
        assert_eq!(tail, None);

        storage
            .put_at_revision(b"a", b"1", None, 1, AppliedLogId::standalone(1))
            .unwrap();
        storage
            .put_at_revision(b"b", b"2", None, 3, AppliedLogId::standalone(3))
            .unwrap();
        let (applied, tail) = storage.verify_consistency().unwrap();
        assert_eq!(applied, 3);
        assert_eq!(tail, Some(3));
    }

    #[test]
    fn test_standalone_concurrent_puts_allocate_distinct_revisions() {
        let (_dir, storage) = create_storage();
        let storage = std::sync::Arc::new(storage);
        let mut handles = Vec::new();
        for i in 0..8usize {
            let storage = std::sync::Arc::clone(&storage);
            handles.push(std::thread::spawn(move || {
                let key = format!("/conc/{i}");
                storage.put(key.as_bytes(), b"v", None).unwrap()
            }));
        }
        let mut revs: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        revs.sort_unstable();
        revs.dedup();
        assert_eq!(
            revs.len(),
            8,
            "concurrent standalone puts must get distinct revisions"
        );
        assert_eq!(storage.current_revision(), 8);
        for i in 0..8usize {
            let key = format!("/conc/{i}");
            assert_eq!(storage.get(key.as_bytes()).unwrap(), Some(b"v".to_vec()));
        }
    }

    #[test]
    fn test_changelog_v2_roundtrip_and_legacy_compat() {
        let event = ChangeEvent {
            revision: 5,
            changes: vec![KeyValueChange {
                key: b"k".to_vec(),
                value: Some(b"v".to_vec()),
                prev_value: None,
            }],
            event_type: EventType::Put,
        };
        let bytes = event.to_bytes();
        assert_eq!(bytes[0], 2); // v2 版本号
        let decoded = ChangeEvent::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.revision, 5);
        assert_eq!(decoded.event_type, EventType::Put);

        // v1 旧格式（无版本号字节）仍可读
        let mut legacy = Vec::new();
        legacy.extend_from_slice(&5u64.to_be_bytes());
        legacy.push(0u8); // Put
        legacy.extend_from_slice(&1u32.to_be_bytes());
        legacy.extend_from_slice(&1u32.to_be_bytes());
        legacy.extend_from_slice(b"k");
        legacy.push(1u8);
        legacy.extend_from_slice(&1u32.to_be_bytes());
        legacy.push(b'v');
        let decoded = ChangeEvent::from_bytes(&legacy).unwrap();
        assert_eq!(decoded.revision, 5);
        assert_eq!(decoded.event_type, EventType::Put);
        assert_eq!(decoded.changes.len(), 1);
    }

    // ──── Lease 状态表 ────

    #[test]
    fn test_lease_op_grant_keepalive_revoke() {
        let (_dir, storage) = create_storage();
        use crate::raft::type_config::LeaseOp;

        // Grant @rev 1
        let (outcome, changes) = storage
            .apply_lease_op(
                &LeaseOp::Grant {
                    id: 10,
                    ttl: 60,
                    deadline_wall_ms: 1000,
                },
                1,
                AppliedLogId::standalone(1),
            )
            .unwrap();
        assert!(!outcome.replayed);
        assert!(changes.is_empty());
        let record = storage.get_lease_record(10).unwrap().unwrap();
        assert_eq!(record.ttl, 60);
        assert_eq!(record.deadline_wall_ms, 1000);
        assert_eq!(record.keepalive_revision, 1);

        // KeepAlive @rev 2
        let (outcome, _) = storage
            .apply_lease_op(
                &LeaseOp::KeepAlive {
                    id: 10,
                    deadline_wall_ms: 2000,
                },
                2,
                AppliedLogId::standalone(2),
            )
            .unwrap();
        assert!(!outcome.replayed);
        let record = storage.get_lease_record(10).unwrap().unwrap();
        assert_eq!(record.deadline_wall_ms, 2000);
        assert_eq!(record.keepalive_revision, 2);

        // 绑定 Key 后 Revoke @rev 3 → 记录删除 + 绑定 Key 标记删除
        storage
            .put_at_revision(b"k", b"v", Some(10), 3, AppliedLogId::standalone(3))
            .unwrap();
        let (outcome, changes) = storage
            .apply_lease_op(
                &LeaseOp::Revoke {
                    id: 10,
                    delete_keys: true,
                },
                4,
                AppliedLogId::standalone(4),
            )
            .unwrap();
        assert!(!outcome.replayed);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].key, b"k");
        assert!(storage.get_lease_record(10).unwrap().is_none());
        assert_eq!(storage.get(b"k").unwrap(), None);
    }

    /// 回归：状态机对已存在的 `/_lease/{id}` **拒绝覆盖**。
    ///
    /// 无条件 upsert 时，leader 切换窗口里的第二个 Grant（同一显式 ID）会把
    /// 前一个租约**静默覆盖** → 两个客户端共享 lease → 到期互相删除对方绑定的
    /// Key。状态机是 ID 归属的最终权威：第二个 Grant 不产生任何变更，原租约完好。
    #[test]
    fn test_lease_grant_refuses_to_overwrite_existing_id() {
        let (_dir, storage) = create_storage();
        use crate::raft::type_config::LeaseOp;

        // 客户端 A：Grant(id=42) 并绑定一个 Key
        storage
            .apply_lease_op(
                &LeaseOp::Grant {
                    id: 42,
                    ttl: 60,
                    deadline_wall_ms: 1000,
                },
                1,
                AppliedLogId::standalone(1),
            )
            .unwrap();
        storage
            .put_at_revision(
                b"/owner-a/key",
                b"v",
                Some(42),
                2,
                AppliedLogId::standalone(2),
            )
            .unwrap();

        // 客户端 B 用**同一显式 ID** 再 Grant（模拟跨节点竞态下的重复分配）
        let (outcome, _) = storage
            .apply_lease_op(
                &LeaseOp::Grant {
                    id: 42,
                    ttl: 9999,
                    deadline_wall_ms: 9_999_999,
                },
                3,
                AppliedLogId::standalone(3),
            )
            .unwrap();
        assert!(!outcome.replayed);

        let record = storage
            .get_lease_record(42)
            .unwrap()
            .expect("原租约必须存活");
        assert_eq!(record.ttl, 60, "第二个 Grant 不得覆盖已有租约");
        assert_eq!(record.deadline_wall_ms, 1000);
        assert_eq!(record.keepalive_revision, 1);

        // 归属关系未被篡改：Revoke(id=42) 只删除原持有者绑定的 Key
        let (_, changes) = storage
            .apply_lease_op(
                &LeaseOp::Revoke {
                    id: 42,
                    delete_keys: true,
                },
                4,
                AppliedLogId::standalone(4),
            )
            .unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].key, b"/owner-a/key");
    }

    /// 拒绝覆盖 ≠ 永久占用——`Revoke` 之后同一 ID 必须可重新 Grant。
    #[test]
    fn test_lease_id_reusable_after_revoke() {
        let (_dir, storage) = create_storage();
        use crate::raft::type_config::LeaseOp;

        storage
            .apply_lease_op(
                &LeaseOp::Grant {
                    id: 7,
                    ttl: 30,
                    deadline_wall_ms: 100,
                },
                1,
                AppliedLogId::standalone(1),
            )
            .unwrap();
        storage
            .apply_lease_op(
                &LeaseOp::Revoke {
                    id: 7,
                    delete_keys: false,
                },
                2,
                AppliedLogId::standalone(2),
            )
            .unwrap();
        assert!(storage.get_lease_record(7).unwrap().is_none());

        storage
            .apply_lease_op(
                &LeaseOp::Grant {
                    id: 7,
                    ttl: 45,
                    deadline_wall_ms: 200,
                },
                3,
                AppliedLogId::standalone(3),
            )
            .unwrap();
        let record = storage.get_lease_record(7).unwrap().unwrap();
        assert_eq!(record.ttl, 45, "撤销后同 ID 必须可复用");
        assert_eq!(record.deadline_wall_ms, 200);
        assert_eq!(record.keepalive_revision, 3);
    }

    #[test]
    fn test_list_lease_records() {
        let (_dir, storage) = create_storage();
        use crate::raft::type_config::LeaseOp;

        for (i, id) in [7i64, 8, 9].into_iter().enumerate() {
            storage
                .apply_lease_op(
                    &LeaseOp::Grant {
                        id,
                        ttl: 60,
                        deadline_wall_ms: 1000 + i as i64,
                    },
                    (i + 1) as u64,
                    AppliedLogId::standalone((i + 1) as u64),
                )
                .unwrap();
        }
        let mut records = storage.list_lease_records().unwrap();
        records.sort_by_key(|(id, _)| *id);
        assert_eq!(
            records.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![7, 8, 9]
        );
        assert_eq!(records[0].1.deadline_wall_ms, 1000);
        assert_eq!(records[2].1.deadline_wall_ms, 1002);
    }

    #[test]
    fn test_changelog_roundtrip() {
        let event = ChangeEvent {
            revision: 5,
            changes: vec![
                KeyValueChange {
                    key: b"key1".to_vec(),
                    value: Some(b"val1".to_vec()),
                    prev_value: None,
                },
                KeyValueChange {
                    key: b"key2".to_vec(),
                    value: None,
                    prev_value: Some(b"old".to_vec()),
                },
            ],
            event_type: EventType::Txn,
        };

        let bytes = event.to_bytes();
        let decoded = ChangeEvent::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.revision, 5);
        assert_eq!(decoded.event_type, EventType::Txn);
        assert_eq!(decoded.changes.len(), 2);
        assert_eq!(decoded.changes[0].key, b"key1");
        assert_eq!(decoded.changes[0].value, Some(b"val1".to_vec()));
        assert_eq!(decoded.changes[1].key, b"key2");
        assert_eq!(decoded.changes[1].value, None);
    }

    #[test]
    fn test_persistence_across_restart() {
        let dir = TempDir::new().unwrap();
        let config = StorageConfig {
            data_dir: dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };

        // 第一次启动：写入数据
        {
            let backend = RedbBackend::open(dir.path(), &config).unwrap();
            let storage = MvccStorage::new(backend).unwrap();
            storage.put(b"persist", b"data", None).unwrap();
            assert_eq!(storage.current_revision(), 1);
        }

        // 第二次启动：读取数据
        {
            let backend = RedbBackend::open(dir.path(), &config).unwrap();
            let storage = MvccStorage::new(backend).unwrap();
            assert_eq!(storage.current_revision(), 1);
            assert_eq!(storage.get(b"persist").unwrap(), Some(b"data".to_vec()));
        }
    }

    // ──── Txn 测试 ────

    use crate::txn::{CompareOp, CompareTarget, CompareValue, TxnCompare, TxnOp, TxnOpResponse};

    /// 辅助：创建 Value 相等比较
    fn cmp_value_eq(key: &[u8], value: &[u8]) -> TxnCompare {
        TxnCompare {
            key: key.to_vec(),
            target: CompareTarget::Value,
            op: CompareOp::Equal,
            target_value: CompareValue::Value(value.to_vec()),
        }
    }

    /// 辅助：创建 Put 操作
    fn txn_put(key: &[u8], value: &[u8]) -> TxnOp {
        TxnOp::Put {
            key: key.to_vec(),
            value: value.to_vec(),
            lease_id: None,
        }
    }

    /// 辅助：创建 Delete 操作
    fn txn_delete(key: &[u8]) -> TxnOp {
        TxnOp::Delete { key: key.to_vec() }
    }

    #[test]
    fn test_txn_cas_success() {
        let (_dir, storage) = create_storage();
        storage.put(b"lock", b"unlocked", None).unwrap();

        // CAS: 如果 lock == "unlocked"，则改为 "locked"
        let compares = vec![cmp_value_eq(b"lock", b"unlocked")];
        let success_ops = vec![txn_put(b"lock", b"locked")];
        let failure_ops = vec![];

        let result = storage
            .execute_txn(&compares, &success_ops, &failure_ops)
            .unwrap();

        assert!(result.succeeded);
        assert_eq!(result.revision, 2);
        assert_eq!(result.responses.len(), 1);

        // 验证实际值已变更
        assert_eq!(storage.get(b"lock").unwrap(), Some(b"locked".to_vec()));
    }

    #[test]
    fn test_txn_cas_failure() {
        let (_dir, storage) = create_storage();
        storage.put(b"lock", b"locked", None).unwrap();

        // CAS: 如果 lock == "unlocked"，则改为 "locked"（会失败）
        let compares = vec![cmp_value_eq(b"lock", b"unlocked")];
        let success_ops = vec![txn_put(b"lock", b"acquired")];
        let failure_ops = vec![txn_put(b"lock", b"still_locked")];

        let result = storage
            .execute_txn(&compares, &success_ops, &failure_ops)
            .unwrap();

        assert!(!result.succeeded);
        assert_eq!(result.revision, 2);
        assert_eq!(result.responses.len(), 1);

        // 验证执行了 failure 分支
        assert_eq!(
            storage.get(b"lock").unwrap(),
            Some(b"still_locked".to_vec())
        );
    }

    #[test]
    fn test_txn_version_compare() {
        let (_dir, storage) = create_storage();
        storage.put(b"key", b"v1", None).unwrap(); // version=1
        storage.put(b"key", b"v2", None).unwrap(); // version=2

        // 比较 version == 2
        let compares = vec![TxnCompare {
            key: b"key".to_vec(),
            target: CompareTarget::Version,
            op: CompareOp::Equal,
            target_value: CompareValue::Version(2),
        }];
        let success_ops = vec![txn_put(b"key", b"v3")];
        let failure_ops = vec![];

        let result = storage
            .execute_txn(&compares, &success_ops, &failure_ops)
            .unwrap();

        assert!(result.succeeded);
        assert_eq!(storage.get(b"key").unwrap(), Some(b"v3".to_vec()));
    }

    #[test]
    fn test_txn_version_compare_failure() {
        let (_dir, storage) = create_storage();
        storage.put(b"key", b"v1", None).unwrap(); // version=1

        // 比较 version == 99（不存在的版本）
        let compares = vec![TxnCompare {
            key: b"key".to_vec(),
            target: CompareTarget::Version,
            op: CompareOp::Equal,
            target_value: CompareValue::Version(99),
        }];
        let success_ops = vec![txn_put(b"key", b"should_not_write")];
        let failure_ops = vec![txn_put(b"key", b"version_mismatch")];

        let result = storage
            .execute_txn(&compares, &success_ops, &failure_ops)
            .unwrap();

        assert!(!result.succeeded);
        assert_eq!(
            storage.get(b"key").unwrap(),
            Some(b"version_mismatch".to_vec())
        );
    }

    #[test]
    fn test_txn_mod_revision_compare() {
        let (_dir, storage) = create_storage();
        storage.put(b"key", b"v1", None).unwrap(); // mod_revision=1
        storage.put(b"key", b"v2", None).unwrap(); // mod_revision=2

        // 比较 mod_revision > 1
        let compares = vec![TxnCompare {
            key: b"key".to_vec(),
            target: CompareTarget::ModRevision,
            op: CompareOp::Greater,
            target_value: CompareValue::ModRevision(1),
        }];
        let success_ops = vec![txn_put(b"key", b"v3")];
        let failure_ops = vec![];

        let result = storage
            .execute_txn(&compares, &success_ops, &failure_ops)
            .unwrap();

        assert!(result.succeeded);
        assert_eq!(storage.get(b"key").unwrap(), Some(b"v3".to_vec()));
    }

    #[test]
    fn test_txn_create_revision_compare() {
        let (_dir, storage) = create_storage();
        let rev1 = storage.put(b"key", b"v1", None).unwrap(); // create_revision=1
        storage.put(b"key", b"v2", None).unwrap(); // create_revision stays 1

        // 比较 create_revision == 1
        let compares = vec![TxnCompare {
            key: b"key".to_vec(),
            target: CompareTarget::CreateRevision,
            op: CompareOp::Equal,
            target_value: CompareValue::CreateRevision(rev1 as i64),
        }];
        let success_ops = vec![txn_put(b"key", b"v3")];
        let failure_ops = vec![];

        let result = storage
            .execute_txn(&compares, &success_ops, &failure_ops)
            .unwrap();

        assert!(result.succeeded);
    }

    #[test]
    fn test_txn_multiple_compares_and() {
        let (_dir, storage) = create_storage();
        storage.put(b"a", b"1", None).unwrap();
        storage.put(b"b", b"2", None).unwrap();

        // AND 条件：a=="1" AND b=="2" → 全部满足
        let compares = vec![cmp_value_eq(b"a", b"1"), cmp_value_eq(b"b", b"2")];
        let success_ops = vec![txn_put(b"a", b"ok")];
        let failure_ops = vec![];

        let result = storage
            .execute_txn(&compares, &success_ops, &failure_ops)
            .unwrap();

        assert!(result.succeeded);
        assert_eq!(storage.get(b"a").unwrap(), Some(b"ok".to_vec()));
    }

    #[test]
    fn test_txn_multiple_ops_in_branch() {
        let (_dir, storage) = create_storage();
        storage.put(b"key1", b"v1", None).unwrap();
        storage.put(b"key2", b"v2", None).unwrap();

        // 在 success 分支执行多个操作
        let compares = vec![cmp_value_eq(b"key1", b"v1")];
        let success_ops = vec![
            txn_put(b"key1", b"updated"),
            txn_put(b"key2", b"also_updated"),
            txn_delete(b"key3"), // key3 不存在，删除也是合法的
        ];
        let failure_ops = vec![];

        let result = storage
            .execute_txn(&compares, &success_ops, &failure_ops)
            .unwrap();

        assert!(result.succeeded);
        assert_eq!(result.responses.len(), 3);
        assert_eq!(storage.get(b"key1").unwrap(), Some(b"updated".to_vec()));
        assert_eq!(
            storage.get(b"key2").unwrap(),
            Some(b"also_updated".to_vec())
        );
    }

    #[test]
    fn test_txn_range_inside() {
        let (_dir, storage) = create_storage();
        storage.put(b"/svc/a", b"addr1", None).unwrap();
        storage.put(b"/svc/b", b"addr2", None).unwrap();
        storage.put(b"/svc/c", b"addr3", None).unwrap();

        // 在 Txn 内执行 Range 读取（显式区间：range_end = prefix_successor("/svc/")）
        let compares = vec![cmp_value_eq(b"/svc/a", b"addr1")];
        let success_ops = vec![TxnOp::Range {
            key: b"/svc/".to_vec(),
            range_end: b"/svc0".to_vec(),
            limit: 10,
        }];
        let failure_ops = vec![];

        let result = storage
            .execute_txn(&compares, &success_ops, &failure_ops)
            .unwrap();

        assert!(result.succeeded);
        assert_eq!(result.responses.len(), 1);
        if let TxnOpResponse::Range { kvs, count, .. } = &result.responses[0] {
            assert_eq!(*count, 3);
            assert_eq!(kvs.len(), 3);
        } else {
            panic!("expected Range response");
        }
    }

    /// 回归：Txn 内层 `Range` 的 `range_end` 为空必须是**单键精确查询**
    /// （与顶层 `KV/Range`、插件 ABI 文档一致），不得回退为字节前缀扫描。
    ///
    /// 前缀扫描时：`key="/svc/"`、`range_end=""` 会返回 `/svc/a`、`/svc/b`、`/svc/c`，
    /// 而鉴权层把同一请求建模为「对 `/svc/` 的单键访问」——持 `scope="/svc/"` 之外的
    /// 凭据即可越权读到兄弟命名空间。
    #[test]
    fn test_txn_range_empty_end_is_single_key() {
        let (_dir, storage) = create_storage();
        storage.put(b"/app/a", b"mine", None).unwrap();
        storage
            .put(b"/app/abc/config", b"SECRET-abc", None)
            .unwrap();
        storage
            .put(b"/app/admin/root-token", b"SECRET-root", None)
            .unwrap();

        let result = storage
            .execute_txn(
                &[],
                &[TxnOp::Range {
                    key: b"/app/a".to_vec(),
                    range_end: Vec::new(),
                    limit: 0,
                }],
                &[],
            )
            .unwrap();

        match &result.responses[0] {
            TxnOpResponse::Range { kvs, count, .. } => {
                assert_eq!(*count, 1, "空 range_end 不得退化为前缀扫描");
                assert_eq!(kvs.len(), 1);
                assert_eq!(kvs[0].0, b"/app/a".to_vec());
                assert_eq!(kvs[0].1, b"mine".to_vec());
            }
            other => panic!("expected Range response, got {other:?}"),
        }
    }

    /// 回归：Txn 内层 `Range` 与顶层 `Range` 对**同一** `(key, range_end)`
    /// 必须给出同一结果集——二者语义由 `coord_core::kv_range::RangeSemantics` 单点定义。
    #[test]
    fn test_txn_range_matches_top_level_range_semantics() {
        use coord_core::kv_range::prefix_successor;

        let (_dir, storage) = create_storage();
        for k in [b"/ns/a".as_slice(), b"/ns/b", b"/nsx/c", b"/other"] {
            storage.put(k, b"v", None).unwrap();
        }

        // 覆盖：空 range_end（单键）、range_end == key（单键）、显式前缀区间、无界区间
        let cases: Vec<(Vec<u8>, Vec<u8>)> = vec![
            (b"/ns/a".to_vec(), Vec::new()),
            (b"/ns/a".to_vec(), b"/ns/a".to_vec()),
            (b"/ns/".to_vec(), prefix_successor(b"/ns/").unwrap()),
            (b"/ns/".to_vec(), b"\0".to_vec()),
        ];

        for (key, range_end) in cases {
            let txn_out = storage
                .execute_txn(
                    &[],
                    &[TxnOp::Range {
                        key: key.clone(),
                        range_end: range_end.clone(),
                        limit: 0,
                    }],
                    &[],
                )
                .unwrap();
            let txn_keys: Vec<Vec<u8>> = match &txn_out.responses[0] {
                TxnOpResponse::Range { kvs, .. } => kvs.iter().map(|(k, _)| k.clone()).collect(),
                other => panic!("expected Range response, got {other:?}"),
            };

            let top_level: Vec<Vec<u8>> = if RangeSemantics::of(&key, &range_end).is_single_key() {
                storage
                    .get(&key)
                    .unwrap()
                    .map(|_| vec![key.clone()])
                    .unwrap_or_default()
            } else {
                storage
                    .range_in(&key, &range_end, 0)
                    .unwrap()
                    .into_iter()
                    .map(|(k, _)| k)
                    .collect()
            };

            assert_eq!(
                txn_keys,
                top_level,
                "Txn 内层 Range 与顶层 Range 语义分叉: key={:?} range_end={:?}",
                String::from_utf8_lossy(&key),
                String::from_utf8_lossy(&range_end)
            );
        }
    }

    #[test]
    fn test_txn_on_nonexistent_key() {
        let (_dir, storage) = create_storage();

        // 比较不存在的 key：value 为默认空
        let compares = vec![TxnCompare {
            key: b"nonexistent".to_vec(),
            target: CompareTarget::Value,
            op: CompareOp::Equal,
            target_value: CompareValue::Value(vec![]),
        }];
        let success_ops = vec![txn_put(b"nonexistent", b"created")];
        let failure_ops = vec![];

        let result = storage
            .execute_txn(&compares, &success_ops, &failure_ops)
            .unwrap();

        assert!(result.succeeded);
        assert_eq!(
            storage.get(b"nonexistent").unwrap(),
            Some(b"created".to_vec())
        );
    }

    #[test]
    fn test_txn_revision_monotonic() {
        let (_dir, storage) = create_storage();
        storage.put(b"k", b"v", None).unwrap(); // rev=1

        // 执行 Txn
        let compares = vec![cmp_value_eq(b"k", b"v")];
        let result = storage
            .execute_txn(&compares, &[txn_put(b"k", b"v2")], &[])
            .unwrap();
        assert_eq!(result.revision, 2);

        // 再次 Put
        let rev3 = storage.put(b"k", b"v3", None).unwrap();
        assert_eq!(rev3, 3);
    }

    #[test]
    fn test_kv_metadata_tracking() {
        let (_dir, storage) = create_storage();

        // 首次 Put：version=1, create_revision=mod_revision=rev1
        let rev1 = storage.put(b"key", b"v1", None).unwrap();
        let meta = storage.get_kv_metadata(b"key").unwrap().unwrap();
        assert_eq!(meta.version, 1);
        assert_eq!(meta.create_revision, rev1 as i64);
        assert_eq!(meta.mod_revision, rev1 as i64);

        // 第二次 Put：version=2, create_revision 不变, mod_revision=rev2
        let rev2 = storage.put(b"key", b"v2", None).unwrap();
        let meta = storage.get_kv_metadata(b"key").unwrap().unwrap();
        assert_eq!(meta.version, 2);
        assert_eq!(meta.create_revision, rev1 as i64);
        assert_eq!(meta.mod_revision, rev2 as i64);

        // Delete：version=3
        let _rev3 = storage.delete(b"key").unwrap();
        let meta = storage.get_kv_metadata(b"key").unwrap().unwrap();
        assert_eq!(meta.version, 3);
        assert_eq!(meta.create_revision, rev1 as i64);
    }

    /// Txn CAS Version==0 对软删除的 Key 应返回成功（视为不存在）
    /// 这是分布式锁 release/expire 后重新 acquire 的核心语义。
    #[test]
    fn test_txn_version_zero_on_deleted_key_succeeds() {
        let (_dir, storage) = create_storage();

        // 1. 创建 Key：version=1
        storage.put(b"lock", b"holder-a", None).unwrap();
        assert_eq!(storage.get(b"lock").unwrap(), Some(b"holder-a".to_vec()));

        // 2. 删除 Key（软删除：version=2, deleted=true）
        storage.delete(b"lock").unwrap();
        // get 返回 None（因为 deleted=true 过滤）
        assert_eq!(storage.get(b"lock").unwrap(), None);
        // 但元数据仍然存在且 version>0
        let meta = storage.get_kv_metadata(b"lock").unwrap().unwrap();
        assert!(meta.deleted);
        assert_eq!(meta.version, 2);

        // 3. Txn CAS Version==0 → 应成功（软删除视为不存在）
        let compares = vec![TxnCompare {
            key: b"lock".to_vec(),
            target: CompareTarget::Version,
            op: CompareOp::Equal,
            target_value: CompareValue::Version(0),
        }];
        let success_ops = vec![txn_put(b"lock", b"holder-b")];
        let failure_ops = vec![txn_put(b"lock", b"conflict")];

        let result = storage
            .execute_txn(&compares, &success_ops, &failure_ops)
            .unwrap();

        // 应成功执行 success 分支
        assert!(
            result.succeeded,
            "Txn CAS Version==0 should succeed on soft-deleted key"
        );
        assert_eq!(
            storage.get(b"lock").unwrap(),
            Some(b"holder-b".to_vec()),
            "holder-b should have re-acquired the lock"
        );
    }

    /// 软删除 Key 的 ModRevision/CreateRevision 比较也应视为 0
    #[test]
    fn test_txn_revision_on_deleted_key_is_zero() {
        let (_dir, storage) = create_storage();

        storage.put(b"key", b"val", None).unwrap();
        let rev1 = storage.delete(b"key").unwrap();

        // 软删除后 ModRevision==0（视为不存在）
        let compares = vec![TxnCompare {
            key: b"key".to_vec(),
            target: CompareTarget::ModRevision,
            op: CompareOp::Equal,
            target_value: CompareValue::ModRevision(0),
        }];
        let success_ops = vec![txn_put(b"key", b"recreated")];
        let failure_ops = vec![];

        let result = storage
            .execute_txn(&compares, &success_ops, &failure_ops)
            .unwrap();

        assert!(
            result.succeeded,
            "ModRevision==0 should match soft-deleted key"
        );
        assert_eq!(storage.get(b"key").unwrap(), Some(b"recreated".to_vec()));
    }

    /// 软删除 Key 的 Value 比较应视为空
    #[test]
    fn test_txn_value_on_deleted_key_is_empty() {
        let (_dir, storage) = create_storage();

        storage.put(b"key", b"original", None).unwrap();
        storage.delete(b"key").unwrap();

        // 软删除后 Value==空
        let compares = vec![TxnCompare {
            key: b"key".to_vec(),
            target: CompareTarget::Value,
            op: CompareOp::Equal,
            target_value: CompareValue::Value(vec![]),
        }];
        let success_ops = vec![txn_put(b"key", b"new-val")];
        let failure_ops = vec![];

        let result = storage
            .execute_txn(&compares, &success_ops, &failure_ops)
            .unwrap();

        assert!(
            result.succeeded,
            "Value==empty should match soft-deleted key"
        );
        assert_eq!(storage.get(b"key").unwrap(), Some(b"new-val".to_vec()));
    }

    // ──── Compaction ────

    #[test]
    fn test_apply_compact_deletes_changelog_below_revision() {
        let (_dir, storage) = create_storage();
        for i in 0..10u32 {
            storage
                .put(format!("/ck{}", i).as_bytes(), b"v", None)
                .unwrap();
        }
        assert_eq!(storage.current_revision(), 10);

        let outcome = storage
            .apply_compact(5, AppliedLogId::standalone(5))
            .unwrap();
        assert!(
            outcome.deleted_changelog > 0,
            "entries below cutoff deleted"
        );
        assert_eq!(storage.compacted_revision().unwrap(), 5);

        for rev in 1..5u64 {
            assert!(
                !storage.changelog_contains_revision(rev).unwrap(),
                "changelog entry at rev {rev} should be compacted away"
            );
        }
        for rev in 5..=10u64 {
            assert!(
                storage.changelog_contains_revision(rev).unwrap(),
                "changelog entry at rev {rev} must survive compaction"
            );
        }
        // KV 数据不受影响
        assert_eq!(storage.get(b"/ck0").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn test_apply_compact_removes_old_tombstones() {
        let (_dir, storage) = create_storage();
        for i in 0..20u32 {
            storage
                .put(format!("/tk{}", i).as_bytes(), b"v", None)
                .unwrap();
        }
        for i in 0..10u32 {
            storage.delete(format!("/tk{}", i).as_bytes()).unwrap();
        }
        let current = storage.current_revision();
        assert!(current >= 30);

        let outcome = storage
            .apply_compact(current, AppliedLogId::standalone(current))
            .unwrap();
        // 注意：mod_revision == current 的 tombstone 恰好在截止线上，必须保留
        // （删除条件为 mod_revision < revision），其余 9 个被物理删除。
        assert!(
            outcome.deleted_tombstones >= 9,
            "old tombstones should be physically removed, got {}",
            outcome.deleted_tombstones
        );
        // 物理删除后元数据不存在（不再有 deleted 标记）
        assert!(storage.get_kv_metadata(b"/tk0").unwrap().is_none());
        // 截止线上的 tombstone 保留（deleted 标记仍在）
        let meta = storage.get_kv_metadata(b"/tk9").unwrap();
        assert!(meta.is_some_and(|m| m.deleted), "cutoff tombstone retained");
        // 未删除的 key 保留
        assert_eq!(storage.get(b"/tk15").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn test_apply_compact_idempotent_and_monotonic() {
        let (_dir, storage) = create_storage();
        for i in 0..5u32 {
            storage
                .put(format!("/id{}", i).as_bytes(), b"v", None)
                .unwrap();
        }
        storage
            .apply_compact(3, AppliedLogId::standalone(3))
            .unwrap();

        let again = storage
            .apply_compact(3, AppliedLogId::standalone(3))
            .unwrap();
        assert_eq!(again.deleted_changelog, 0);
        assert_eq!(again.deleted_tombstones, 0);

        let lower = storage
            .apply_compact(2, AppliedLogId::standalone(2))
            .unwrap();
        assert_eq!(lower.deleted_changelog, 0);

        // 未来 revision 只钳到 current revision（#139：不得用 log index 钳制；
        // apply 不得失败——真正的"未来 revision"拒绝由 RPC/提案层返回
        // INVALID_ARGUMENT）。
        let future = storage
            .apply_compact(999, AppliedLogId::standalone(6))
            .unwrap();
        assert_eq!(future.deleted_changelog, 2); // rev 3,4（< current=5）
        assert_eq!(storage.compacted_revision().unwrap(), 5);
        assert_eq!(
            storage.current_revision(),
            6,
            "applied 前进，revision 不回退"
        );
    }

    #[test]
    fn test_apply_compact_sharded_batches() {
        let (_dir, storage) = create_storage();
        // 超过单批上限（COMPACT_BATCH_SIZE）的条目数，验证分片删除
        let total = super::COMPACT_BATCH_SIZE as u32 + 500;
        for i in 0..total {
            storage
                .put(format!("/shard{}", i).as_bytes(), b"v", None)
                .unwrap();
        }
        let current = storage.current_revision();
        assert_eq!(current, total as u64);

        let outcome = storage
            .apply_compact(current, AppliedLogId::standalone(current))
            .unwrap();
        assert_eq!(
            outcome.deleted_changelog as u64,
            current - 1,
            "all entries below cutoff must be deleted across batches"
        );
        assert!(storage.changelog_contains_revision(current).unwrap());
        assert_eq!(storage.compacted_revision().unwrap(), current);
    }

    #[test]
    fn test_compacted_revision_persists_across_reopen() {
        let (dir, storage) = create_storage();
        for i in 0..5u32 {
            storage
                .put(format!("/p{}", i).as_bytes(), b"v", None)
                .unwrap();
        }
        storage
            .apply_compact(3, AppliedLogId::standalone(3))
            .unwrap();
        drop(storage);

        let config = StorageConfig::default();
        let backend = RedbBackend::open(dir.path(), &config).unwrap();
        let reopened = MvccStorage::new(backend).unwrap();
        assert_eq!(reopened.compacted_revision().unwrap(), 3);
    }

    // ──── PD 全局 operator 队列状态机（apply_pd_op）────

    use crate::pd::operator::{Operator, OperatorStatus};
    use crate::raft::type_config::PdOp;

    fn make_add_peer(region_id: u64, node_id: u64) -> Operator {
        Operator::AddPeer {
            region_id,
            node_id,
            raft_addr: format!("node{node_id}:50052"),
        }
    }

    fn enqueue_at(storage: &MvccStorage<RedbBackend>, op: &Operator, rev: u64) {
        storage
            .apply_pd_op(
                &PdOp::Enqueue {
                    op: op.clone(),
                    requester: 1,
                    proposed_at_unix: 1_700_000_000 + rev as i64,
                },
                rev,
                AppliedLogId::standalone(rev),
            )
            .unwrap();
    }

    #[test]
    fn test_pd_enqueue_claim_complete_roundtrip() {
        let (_dir, storage) = create_storage();
        let op = make_add_peer(1, 2);

        // Enqueue @ rev1：op_id == revision，Pending
        enqueue_at(&storage, &op, 1);
        let entries = storage.pd_queue_entries().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].op_id, 1);
        assert!(entries[0].is_pending());
        assert_eq!(entries[0].op, op);
        assert_eq!(storage.current_revision(), 1);

        // Claim 由 node 3（@rev2）→ Running + claimed_by=3 + claimed_at 落定
        storage
            .apply_pd_op(
                &PdOp::Claim {
                    op_id: 1,
                    node_id: 3,
                    claimed_at_unix: 1_700_000_200,
                },
                2,
                AppliedLogId::standalone(2),
            )
            .unwrap();
        let e = &storage.pd_queue_entries().unwrap()[0];
        assert!(e.is_running());
        assert_eq!(e.claimed_by, 3);
        assert_eq!(e.claimed_at_unix, 1_700_000_200, "claim 墙钟随命令落定");

        // 非认领者 node 2 完成 → no-op（防他节点误 Complete）
        storage
            .apply_pd_op(
                &PdOp::Complete {
                    op_id: 1,
                    node_id: 2,
                    success: true,
                    error: String::new(),
                },
                3,
                AppliedLogId::standalone(3),
            )
            .unwrap();
        assert!(storage.pd_queue_entries().unwrap()[0].is_running());

        // 认领者 node 3 完成 → Success（@rev4）
        storage
            .apply_pd_op(
                &PdOp::Complete {
                    op_id: 1,
                    node_id: 3,
                    success: true,
                    error: String::new(),
                },
                4,
                AppliedLogId::standalone(4),
            )
            .unwrap();
        let e = &storage.pd_queue_entries().unwrap()[0];
        assert_eq!(e.status, OperatorStatus::Success);
        assert!(e.is_terminal());
        assert_eq!(storage.current_revision(), 4);
    }

    #[test]
    fn test_pd_enqueue_dedup_and_distinct_ops() {
        let (_dir, storage) = create_storage();
        let op = make_add_peer(1, 2);

        // 相同 operator 再次入队 → no-op（全局去重）
        enqueue_at(&storage, &op, 1);
        enqueue_at(&storage, &op, 5);
        let entries = storage.pd_queue_entries().unwrap();
        assert_eq!(
            entries.len(),
            1,
            "duplicate enqueue must be deduped globally"
        );
        assert_eq!(entries[0].op_id, 1);

        // 不同目标 node 的 AddPeer = 不同 operator → 可入队
        enqueue_at(&storage, &make_add_peer(1, 4), 9);
        let entries = storage.pd_queue_entries().unwrap();
        assert_eq!(entries.len(), 2);

        // 终态（已 Success）后同 operator 可再次入队（去重只覆盖 Pending/Running）
        storage
            .apply_pd_op(
                &PdOp::Claim {
                    op_id: 1,
                    node_id: 9,
                    claimed_at_unix: 1_700_000_300,
                },
                10,
                AppliedLogId::standalone(10),
            )
            .unwrap();
        storage
            .apply_pd_op(
                &PdOp::Complete {
                    op_id: 1,
                    node_id: 9,
                    success: true,
                    error: String::new(),
                },
                11,
                AppliedLogId::standalone(11),
            )
            .unwrap();
        assert!(storage.pd_queue_entries().unwrap()[0].is_terminal());

        enqueue_at(&storage, &op, 12);
        let entries = storage.pd_queue_entries().unwrap();
        assert_eq!(entries.len(), 3, "same op re-enqueued after terminal");
        assert_eq!(entries[2].op_id, 12);
    }

    #[test]
    fn test_pd_claim_is_exclusive() {
        let (_dir, storage) = create_storage();
        enqueue_at(&storage, &make_add_peer(2, 5), 1);

        // node 7 认领成功
        storage
            .apply_pd_op(
                &PdOp::Claim {
                    op_id: 1,
                    node_id: 7,
                    claimed_at_unix: 1_700_000_400,
                },
                2,
                AppliedLogId::standalone(2),
            )
            .unwrap();
        // node 9 再认领 → no-op（claimed_by 保持 7）
        storage
            .apply_pd_op(
                &PdOp::Claim {
                    op_id: 1,
                    node_id: 9,
                    claimed_at_unix: 1_700_000_500,
                },
                3,
                AppliedLogId::standalone(3),
            )
            .unwrap();
        let e = &storage.pd_queue_entries().unwrap()[0];
        assert!(e.is_running());
        assert_eq!(e.claimed_by, 7);

        // 认领者 7 完成失败 → Failed 携带原因
        storage
            .apply_pd_op(
                &PdOp::Complete {
                    op_id: 1,
                    node_id: 7,
                    success: false,
                    error: "region 2 not leader".into(),
                },
                4,
                AppliedLogId::standalone(4),
            )
            .unwrap();
        let e = &storage.pd_queue_entries().unwrap()[0];
        assert_eq!(
            e.status,
            OperatorStatus::Failed("region 2 not leader".into())
        );
        assert_eq!(e.error, "region 2 not leader");
    }

    #[test]
    fn test_pd_requeue_resets_running() {
        let (_dir, storage) = create_storage();
        enqueue_at(&storage, &make_add_peer(1, 3), 1);
        storage
            .apply_pd_op(
                &PdOp::Claim {
                    op_id: 1,
                    node_id: 4,
                    claimed_at_unix: 1_700_000_600,
                },
                2,
                AppliedLogId::standalone(2),
            )
            .unwrap();
        let running = storage.pd_queue_entries().unwrap()[0].clone();
        assert!(running.is_running());
        assert_eq!(running.claimed_at_unix, 1_700_000_600);

        // Requeue → Pending + 清认领者/认领墙钟（认领者失联/可重试路径）
        storage
            .apply_pd_op(&PdOp::Requeue { op_id: 1 }, 3, AppliedLogId::standalone(3))
            .unwrap();
        let e = &storage.pd_queue_entries().unwrap()[0];
        assert!(e.is_pending());
        assert_eq!(e.claimed_by, 0);
        assert_eq!(e.claimed_at_unix, 0, "requeue 清认领墙钟");

        // Requeue 对 Pending 再执行 → no-op（仍 Pending）
        storage
            .apply_pd_op(&PdOp::Requeue { op_id: 1 }, 4, AppliedLogId::standalone(4))
            .unwrap();
        assert!(storage.pd_queue_entries().unwrap()[0].is_pending());
    }

    #[test]
    fn test_pd_apply_is_replay_idempotent() {
        let (_dir, storage) = create_storage();
        let op = make_add_peer(3, 1);
        enqueue_at(&storage, &op, 1);

        // 同 revision 重放 → replayed，不重复入队
        let outcome = storage
            .apply_pd_op(
                &PdOp::Enqueue {
                    op: op.clone(),
                    requester: 1,
                    proposed_at_unix: 1_700_000_000,
                },
                1,
                AppliedLogId::standalone(1),
            )
            .unwrap();
        assert!(outcome.replayed);
        assert_eq!(storage.pd_queue_entries().unwrap().len(), 1);
        assert_eq!(storage.current_revision(), 1);
    }

    #[test]
    fn test_pd_queue_prunes_oldest_terminal() {
        let (_dir, storage) = create_storage();
        // 造 515 个唯一 operator（不同目标 node_id）→ 全部 enqueue；
        // 前 513 个认领并完成（terminal），尾部 2 个保持 Pending。
        for i in 0..515u64 {
            enqueue_at(&storage, &make_add_peer(1, 100 + i), 1 + i);
        }
        for i in 0..513u64 {
            storage
                .apply_pd_op(
                    &PdOp::Claim {
                        op_id: 1 + i,
                        node_id: 1,
                        claimed_at_unix: 1_700_000_700,
                    },
                    1000 + i,
                    AppliedLogId::standalone(1000 + i),
                )
                .unwrap();
            storage
                .apply_pd_op(
                    &PdOp::Complete {
                        op_id: 1 + i,
                        node_id: 1,
                        success: true,
                        error: String::new(),
                    },
                    2000 + i,
                    AppliedLogId::standalone(2000 + i),
                )
                .unwrap();
        }
        let entries = storage.pd_queue_entries().unwrap();
        assert_eq!(entries.len(), 514, "one oldest terminal pruned");
        assert_eq!(entries[0].op_id, 2, "oldest terminal op 1 pruned");
        let terminal = entries.iter().filter(|e| e.is_terminal()).count();
        assert_eq!(terminal, 512, "terminal retained at cap");
        // 尾部两个（op_id 514/515）仍 Pending（未完成、不可裁剪）
        assert!(entries[entries.len() - 2].is_pending());
        assert!(entries[entries.len() - 1].is_pending());
    }

    // ──── TSO（#136）────

    fn applied_for(index: u64) -> AppliedLogId {
        AppliedLogId {
            term: 1,
            node_id: 1,
            index,
        }
    }

    #[test]
    fn test_tso_allocate_monotonic_and_batch_bounds() {
        let (_dir, storage) = create_storage();
        let first = storage
            .apply_tso_allocate(1024, 10, applied_for(10))
            .unwrap();
        assert_eq!((first.start_ts, first.end_ts), (1, 1025));
        let second = storage.apply_tso_allocate(0, 11, applied_for(11)).unwrap();
        assert_eq!((second.start_ts, second.end_ts), (1025, 1026));
        assert_eq!(storage.tso_high_water().unwrap(), 1026);
    }

    #[test]
    fn test_tso_restart_does_not_reuse_batch() {
        let (dir, storage) = create_storage();
        let first = storage.apply_tso_allocate(512, 8, applied_for(8)).unwrap();
        assert_eq!(first.start_ts, 1);
        let first_end = first.end_ts;
        drop(storage);

        let config = StorageConfig::default();
        let backend = RedbBackend::open(dir.path(), &config).unwrap();
        let reopened = MvccStorage::new(backend).unwrap();
        assert_eq!(
            reopened.tso_high_water().unwrap(),
            first_end,
            "重启后高水位必须从持久化记录恢复"
        );
        let second = reopened.apply_tso_allocate(512, 9, applied_for(9)).unwrap();
        assert_eq!(second.start_ts, first_end, "新批起点接续旧上界");
        assert!(second.end_ts > first_end);
    }

    #[test]
    fn test_tso_replay_returns_same_batch() {
        let (_dir, storage) = create_storage();
        let first = storage.apply_tso_allocate(16, 42, applied_for(42)).unwrap();
        let replay = storage.apply_tso_allocate(16, 42, applied_for(42)).unwrap();
        assert!(replay.replayed);
        assert_eq!(
            (replay.start_ts, replay.end_ts),
            (first.start_ts, first.end_ts)
        );
        assert_eq!(
            storage.tso_high_water().unwrap(),
            first.end_ts,
            "重放不得推进高水位"
        );
    }

    #[test]
    fn test_pd_queue_persists_across_reopen() {
        let (dir, storage) = create_storage();
        enqueue_at(&storage, &make_add_peer(4, 2), 1);
        storage
            .apply_pd_op(
                &PdOp::Claim {
                    op_id: 1,
                    node_id: 2,
                    claimed_at_unix: 1_700_000_800,
                },
                2,
                AppliedLogId::standalone(2),
            )
            .unwrap();
        drop(storage);

        let config = StorageConfig::default();
        let backend = RedbBackend::open(dir.path(), &config).unwrap();
        let reopened = MvccStorage::new(backend).unwrap();
        let entries = reopened.pd_queue_entries().unwrap();
        assert_eq!(entries.len(), 1, "queue survives reopen (redb durable)");
        assert_eq!(entries[0].op_id, 1);
        assert!(entries[0].is_running());
        assert_eq!(entries[0].claimed_by, 2);
        assert_eq!(entries[0].claimed_at_unix, 1_700_000_800);
    }

    // ──── v2 Lease 表（#145）────

    use crate::raft::type_config::LeaseV2Op;

    fn lease_v2_grant(id: i64, ttl: i64, granted_ts: u64, req: &[u8]) -> LeaseV2Op {
        LeaseV2Op::Grant {
            lease_id: id,
            ttl_seconds: ttl,
            granted_ts,
            deadline_wall_ms: 1_000_000,
            request_id: req.to_vec(),
        }
    }

    #[test]
    fn test_lease_v2_grant_request_id_idempotent_and_id_conflict() {
        let (_dir, storage) = create_commit_ts_storage();

        let (result, _) = storage
            .apply_lease_v2_op(&lease_v2_grant(100, 30, 10, b"req-1"), 1, applied_for(1))
            .unwrap();
        assert_eq!(
            result,
            LeaseV2Apply::Grant {
                lease_id: 100,
                granted_ts: 10,
                ttl: 30,
                created: true,
            }
        );

        // 同 request_id（即使客户端换了显式 id）回放首次结果。
        let (result, _) = storage
            .apply_lease_v2_op(&lease_v2_grant(200, 30, 11, b"req-1"), 2, applied_for(2))
            .unwrap();
        assert_eq!(
            result,
            LeaseV2Apply::Grant {
                lease_id: 100,
                granted_ts: 10,
                ttl: 30,
                created: true,
            }
        );
        assert!(storage.get_lease_v2_record(200).unwrap().is_none());

        // 显式 id 被不同 request_id 占用：不覆盖，created=false 供 handler 报冲突。
        let (result, _) = storage
            .apply_lease_v2_op(&lease_v2_grant(100, 30, 12, b"req-2"), 3, applied_for(3))
            .unwrap();
        assert_eq!(
            result,
            LeaseV2Apply::Grant {
                lease_id: 100,
                granted_ts: 10,
                ttl: 30,
                created: false,
            }
        );
        assert_eq!(
            storage
                .get_lease_v2_record(100)
                .unwrap()
                .unwrap()
                .grant_request_id,
            b"req-1".to_vec()
        );

        // 幂等回放也必须推进 processed applied 水位（否则重启后会从旧
        // checkpoint 重放后续命令）。
        storage
            .apply_lease_v2_op(&lease_v2_grant(100, 30, 10, b"req-1"), 4, applied_for(4))
            .unwrap();
        assert_eq!(storage.get_applied_log_id().unwrap(), Some(applied_for(4)));

        // v1 的 `/_lease/` 枚举不受 v2 表影响。
        assert!(storage.list_lease_records().unwrap().is_empty());
    }

    #[test]
    fn test_lease_v2_keepalive_and_revoke_are_idempotent() {
        let (_dir, storage) = create_commit_ts_storage();
        storage
            .apply_lease_v2_op(&lease_v2_grant(7, 30, 10, b""), 1, applied_for(1))
            .unwrap();

        let (result, _) = storage
            .apply_lease_v2_op(
                &LeaseV2Op::KeepAlive {
                    lease_id: 7,
                    deadline_wall_ms: 2_000_000,
                },
                2,
                applied_for(2),
            )
            .unwrap();
        assert_eq!(result, LeaseV2Apply::KeepAlive { ok: true });
        assert_eq!(
            storage
                .get_lease_v2_record(7)
                .unwrap()
                .unwrap()
                .deadline_wall_ms,
            2_000_000
        );

        let (result, _) = storage
            .apply_lease_v2_op(
                &LeaseV2Op::Revoke {
                    lease_id: 7,
                    commit_ts: 50,
                    expected_deadline_wall_ms: 0,
                    request_id: Vec::new(),
                },
                3,
                applied_for(3),
            )
            .unwrap();
        assert_eq!(result, LeaseV2Apply::Revoke { commit_ts: 50 });

        // 重复/更旧时间戳的 revoke = 无新决策。
        for (revision, commit_ts) in [(4, 50), (5, 40)] {
            let (result, _) = storage
                .apply_lease_v2_op(
                    &LeaseV2Op::Revoke {
                        lease_id: 7,
                        commit_ts,
                        expected_deadline_wall_ms: 0,
                        request_id: Vec::new(),
                    },
                    revision,
                    applied_for(revision),
                )
                .unwrap();
            assert_eq!(result, LeaseV2Apply::Revoke { commit_ts: 0 });
        }

        // 清理重试可以推进决策时间戳。
        let (result, _) = storage
            .apply_lease_v2_op(
                &LeaseV2Op::Revoke {
                    lease_id: 7,
                    commit_ts: 60,
                    expected_deadline_wall_ms: 0,
                    request_id: Vec::new(),
                },
                6,
                applied_for(6),
            )
            .unwrap();
        assert_eq!(result, LeaseV2Apply::Revoke { commit_ts: 60 });
        assert_eq!(
            storage
                .get_lease_v2_record(7)
                .unwrap()
                .unwrap()
                .revoked_commit_ts,
            60
        );

        // 到期清理的 deadline 守卫：记录已被续期（deadline 更晚）时，
        // 旧到期观测不得吊销。
        let (_dir2, storage2) = create_commit_ts_storage();
        storage2
            .apply_lease_v2_op(&lease_v2_grant(9, 30, 20, b""), 1, applied_for(1))
            .unwrap();
        let (result, _) = storage2
            .apply_lease_v2_op(
                &LeaseV2Op::Revoke {
                    lease_id: 9,
                    commit_ts: 80,
                    expected_deadline_wall_ms: 999_999,
                    request_id: Vec::new(),
                },
                2,
                applied_for(2),
            )
            .unwrap();
        assert_eq!(result, LeaseV2Apply::Revoke { commit_ts: 0 });
        assert!(!storage2.get_lease_v2_record(9).unwrap().unwrap().revoked);
        let (result, _) = storage2
            .apply_lease_v2_op(
                &LeaseV2Op::Revoke {
                    lease_id: 9,
                    commit_ts: 81,
                    expected_deadline_wall_ms: 1_000_000,
                    request_id: Vec::new(),
                },
                3,
                applied_for(3),
            )
            .unwrap();
        assert_eq!(result, LeaseV2Apply::Revoke { commit_ts: 81 });

        // 已吊销记录上的 KeepAlive 不生效（handler 报 NOT_FOUND）。
        let (result, _) = storage
            .apply_lease_v2_op(
                &LeaseV2Op::KeepAlive {
                    lease_id: 7,
                    deadline_wall_ms: 9_000_000,
                },
                7,
                applied_for(7),
            )
            .unwrap();
        assert_eq!(result, LeaseV2Apply::KeepAlive { ok: false });

        // 未知 lease 的 revoke 幂等成功。
        let (result, _) = storage
            .apply_lease_v2_op(
                &LeaseV2Op::Revoke {
                    lease_id: 999,
                    commit_ts: 70,
                    expected_deadline_wall_ms: 0,
                    request_id: Vec::new(),
                },
                8,
                applied_for(8),
            )
            .unwrap();
        assert_eq!(result, LeaseV2Apply::Revoke { commit_ts: 0 });
    }

    #[test]
    fn test_lease_v2_delete_keys_at_commit_ts_replay_and_reject() {
        let (_dir, storage) = create_commit_ts_storage();
        storage
            .put_at_commit_ts(b"a", b"1", Some(7), 100, applied_for(1))
            .unwrap();
        storage
            .put_at_commit_ts(b"b", b"2", Some(7), 101, applied_for(2))
            .unwrap();
        storage
            .put_at_commit_ts(b"c", b"3", None, 102, applied_for(3))
            .unwrap();

        let mut keys = storage.keys_by_lease(7, 10).unwrap();
        keys.sort();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);

        let (outcome, mut deleted) = storage
            .delete_keys_by_lease_at_commit_ts(7, 103, applied_for(4))
            .unwrap();
        assert!(!outcome.replayed);
        deleted.sort();
        assert_eq!(deleted, vec![b"a".to_vec(), b"b".to_vec()]);
        assert!(storage.get(b"a").unwrap().is_none());
        assert!(storage.get(b"b").unwrap().is_none());
        assert_eq!(storage.get(b"c").unwrap(), Some(b"3".to_vec()));

        // 同一 commit_ts 重放：不重复写 changelog、不报错。
        let (outcome, deleted) = storage
            .delete_keys_by_lease_at_commit_ts(7, 103, applied_for(4))
            .unwrap();
        assert!(outcome.replayed);
        assert!(deleted.is_empty());

        // 更旧的时间戳（已 apply 更高 revision）→ CommitRejected，由 revoker
        // 重新取 TSO；不得静默删除。
        let err = storage
            .delete_keys_by_lease_at_commit_ts(7, 102, applied_for(5))
            .unwrap_err();
        assert!(matches!(
            err,
            Error::CommitRejected {
                commit_ts: 102,
                current_revision: 103,
            }
        ));
    }

    // ──── Commit Sequencer（#146）────

    fn seq_txn(
        txn_id: &[u8],
        start_ts: u64,
        commit_ts: u64,
        request_id: &[u8],
        reads: &[&[u8]],
        writes: &[(&[u8], &[u8])],
    ) -> crate::raft::type_config::SequencerTxn {
        use crate::raft::type_config::{SequencerMutation, SequencerTxn};
        SequencerTxn {
            txn_id: txn_id.to_vec(),
            start_ts,
            commit_ts,
            request_id: request_id.to_vec(),
            read_set: reads.iter().map(|key| key.to_vec()).collect(),
            mutations: writes
                .iter()
                .map(|(key, value)| SequencerMutation::Put {
                    key: key.to_vec(),
                    value: value.to_vec(),
                    lease_id: None,
                })
                .collect(),
        }
    }

    fn seq_op(
        txns: Vec<crate::raft::type_config::SequencerTxn>,
    ) -> crate::raft::type_config::SequencerOp {
        crate::raft::type_config::SequencerOp::ValidateAndCommit { txns }
    }

    #[test]
    fn test_sequencer_orders_conflicting_txns() {
        use crate::raft::type_config::{SequencerInvalid, SequencerOutcome};
        let (_dir, storage) = create_commit_ts_storage();

        // txn2 的 start_ts (2) 早于 txn1 的 commit_ts (10)，且两者都读写 k：
        // 日志顺序下 txn1 先决策，txn2 必须看到它并冲突。
        let batch = seq_op(vec![
            seq_txn(b"t1", 1, 10, b"r1", &[b"k"], &[(b"k", b"v1")]),
            seq_txn(b"t2", 2, 11, b"r2", &[b"k"], &[(b"k", b"v2")]),
        ]);
        let outcomes = storage
            .apply_sequencer_op(&batch, 1, applied_for(1))
            .unwrap();
        assert_eq!(
            outcomes,
            vec![
                SequencerOutcome::Committed { commit_ts: 10 },
                SequencerOutcome::Conflict {
                    commit_ts: 11,
                    conflict_key: b"k".to_vec(),
                    conflict_ts: 10,
                },
            ]
        );
        assert!(storage.get_sequencer_decision(10).unwrap().is_some());
        assert!(
            storage.get_sequencer_decision(11).unwrap().is_none(),
            "conflict must not write a decision"
        );
        assert_eq!(storage.sequencer_last_commit_ts().unwrap(), 10);

        // 同一批里后一条的读集若在 start_ts 之后被写，也一样冲突。
        let batch = seq_op(vec![
            seq_txn(b"t3", 12, 20, b"r3", &[b"k"], &[(b"k", b"v3")]),
            seq_txn(b"t4", 11, 21, b"r4", &[b"k"], &[(b"k", b"v4")]),
        ]);
        let outcomes = storage
            .apply_sequencer_op(&batch, 2, applied_for(2))
            .unwrap();
        assert_eq!(outcomes[0], SequencerOutcome::Committed { commit_ts: 20 });
        assert_eq!(
            outcomes[1],
            SequencerOutcome::Conflict {
                commit_ts: 21,
                conflict_key: b"k".to_vec(),
                conflict_ts: 20,
            }
        );

        // 非法时间戳不是用户级冲突的替代品：单独断言变体存在且不写决策。
        let invalid = seq_op(vec![seq_txn(b"t5", 30, 30, b"r5", &[], &[(b"z", b"v")])]);
        let outcomes = storage
            .apply_sequencer_op(&invalid, 3, applied_for(3))
            .unwrap();
        assert_eq!(
            outcomes,
            vec![SequencerOutcome::Invalid {
                commit_ts: 30,
                reason: SequencerInvalid::CommitTsNotAboveStart,
            }]
        );
        assert!(storage.get_sequencer_decision(30).unwrap().is_none());
    }

    #[test]
    fn test_sequencer_batch_rejections_do_not_block_later_txns() {
        use crate::raft::type_config::{SequencerInvalid, SequencerOutcome};
        let (_dir, storage) = create_commit_ts_storage();

        // 同一个命令里混入乱序/冲突/非法：每条只跳过自身，后续合法事务仍提交。
        let batch = seq_op(vec![
            seq_txn(b"t1", 1, 10, b"r1", &[], &[(b"b", b"v1")]),
            seq_txn(b"t2", 1, 9, b"r2", &[], &[(b"x", b"v2")]),
            seq_txn(b"t3", 5, 12, b"r3", &[b"b"], &[]),
            seq_txn(b"t4", 20, 20, b"r4", &[], &[(b"c", b"v4")]),
            seq_txn(b"t5", 12, 13, b"r5", &[], &[(b"d", b"v5")]),
        ]);
        let outcomes = storage
            .apply_sequencer_op(&batch, 1, applied_for(1))
            .unwrap();
        assert_eq!(
            outcomes,
            vec![
                SequencerOutcome::Committed { commit_ts: 10 },
                SequencerOutcome::OutOfOrder {
                    commit_ts: 9,
                    last_commit_ts: 10,
                },
                SequencerOutcome::Conflict {
                    commit_ts: 12,
                    conflict_key: b"b".to_vec(),
                    conflict_ts: 10,
                },
                SequencerOutcome::Invalid {
                    commit_ts: 20,
                    reason: SequencerInvalid::CommitTsNotAboveStart,
                },
                SequencerOutcome::Committed { commit_ts: 13 },
            ]
        );

        // 被拒条目不写决策；决策扫描跨过时间戳空洞继续前进。
        let decisions = storage.sequencer_decisions_after(0, 16).unwrap();
        assert_eq!(
            decisions
                .iter()
                .map(|decision| decision.commit_ts)
                .collect::<Vec<_>>(),
            vec![10, 13]
        );
        assert_eq!(storage.sequencer_last_commit_ts().unwrap(), 13);
    }

    #[test]
    fn test_sequencer_duplicate_in_same_batch_writes_one_decision() {
        use crate::raft::type_config::SequencerOutcome;
        let (_dir, storage) = create_commit_ts_storage();

        // 同一命令内重复提交同一 txn_id：第二条返回首次 commit_ts，不写第二个决策。
        let batch = seq_op(vec![
            seq_txn(b"t1", 1, 10, b"r1", &[], &[(b"a", b"v1")]),
            seq_txn(b"t1", 1, 11, b"r1", &[], &[(b"a", b"v2")]),
        ]);
        let outcomes = storage
            .apply_sequencer_op(&batch, 1, applied_for(1))
            .unwrap();
        assert_eq!(
            outcomes,
            vec![
                SequencerOutcome::Committed { commit_ts: 10 },
                SequencerOutcome::Duplicate { commit_ts: 10 },
            ]
        );
        assert!(storage.get_sequencer_decision(11).unwrap().is_none());
        assert_eq!(storage.sequencer_last_commit_ts().unwrap(), 10);
        assert_eq!(storage.sequencer_decisions_after(0, 8).unwrap().len(), 1);
    }

    #[test]
    fn test_sequencer_conflict_writes_no_decision() {
        use crate::raft::type_config::SequencerOutcome;
        let (_dir, storage) = create_commit_ts_storage();
        storage
            .apply_sequencer_op(
                &seq_op(vec![seq_txn(
                    b"t1",
                    1,
                    10,
                    b"r1",
                    &[b"k"],
                    &[(b"k", b"v1")],
                )]),
                1,
                applied_for(1),
            )
            .unwrap();

        let outcomes = storage
            .apply_sequencer_op(
                &seq_op(vec![seq_txn(
                    b"t2",
                    2,
                    11,
                    b"r2",
                    &[b"k"],
                    &[(b"other", b"v2")],
                )]),
                2,
                applied_for(2),
            )
            .unwrap();
        assert_eq!(
            outcomes,
            vec![SequencerOutcome::Conflict {
                commit_ts: 11,
                conflict_key: b"k".to_vec(),
                conflict_ts: 10,
            }]
        );
        assert!(storage.get_sequencer_decision(11).unwrap().is_none());
        assert_eq!(storage.sequencer_last_commit_ts().unwrap(), 10);
    }

    #[test]
    fn test_sequencer_retry_is_idempotent() {
        use crate::raft::type_config::SequencerOutcome;
        let (_dir, storage) = create_commit_ts_storage();
        storage
            .apply_sequencer_op(
                &seq_op(vec![seq_txn(
                    b"t1",
                    1,
                    10,
                    b"r1",
                    &[b"k"],
                    &[(b"k", b"v1")],
                )]),
                1,
                applied_for(1),
            )
            .unwrap();

        // 同 txn_id + request_id 的重试即使带了更大的 commit_ts，也返回首次决策。
        let outcomes = storage
            .apply_sequencer_op(
                &seq_op(vec![seq_txn(
                    b"t1",
                    2,
                    20,
                    b"r1",
                    &[b"k"],
                    &[(b"k", b"v2")],
                )]),
                2,
                applied_for(2),
            )
            .unwrap();
        assert_eq!(
            outcomes,
            vec![SequencerOutcome::Duplicate { commit_ts: 10 }]
        );
        assert!(storage.get_sequencer_decision(20).unwrap().is_none());
        assert_eq!(storage.sequencer_last_commit_ts().unwrap(), 10);

        // 同一 txn_id 换 request_id 也不能产生第二个决策。
        let outcomes = storage
            .apply_sequencer_op(
                &seq_op(vec![seq_txn(b"t1", 3, 21, b"r2", &[], &[(b"k", b"v3")])]),
                3,
                applied_for(3),
            )
            .unwrap();
        assert_eq!(
            outcomes,
            vec![SequencerOutcome::Duplicate { commit_ts: 10 }]
        );

        // request_id 只在同一 txn_id 内去重：另一个事务复用同一个字符串是
        // 新请求（D12.2 的幂等范围是会话），正常提交。
        let outcomes = storage
            .apply_sequencer_op(
                &seq_op(vec![seq_txn(b"t3", 4, 22, b"r1", &[], &[(b"k", b"v4")])]),
                4,
                applied_for(4),
            )
            .unwrap();
        assert_eq!(
            outcomes,
            vec![SequencerOutcome::Committed { commit_ts: 22 }]
        );
        assert!(storage.get_sequencer_decision(22).unwrap().is_some());
    }

    #[test]
    fn test_sequencer_rejects_out_of_order_commit_ts() {
        use crate::raft::type_config::SequencerOutcome;
        let (_dir, storage) = create_commit_ts_storage();
        storage
            .apply_sequencer_op(
                &seq_op(vec![seq_txn(b"t1", 1, 20, b"r1", &[], &[(b"a", b"v1")])]),
                1,
                applied_for(1),
            )
            .unwrap();

        let outcomes = storage
            .apply_sequencer_op(
                &seq_op(vec![seq_txn(b"t2", 2, 19, b"r2", &[], &[(b"b", b"v2")])]),
                2,
                applied_for(2),
            )
            .unwrap();
        assert_eq!(
            outcomes,
            vec![SequencerOutcome::OutOfOrder {
                commit_ts: 19,
                last_commit_ts: 20,
            }]
        );
        assert!(storage.get_sequencer_decision(19).unwrap().is_none());
        assert_eq!(storage.sequencer_last_commit_ts().unwrap(), 20);
    }

    #[test]
    fn test_sequencer_decision_and_index_survive_reopen() {
        use crate::raft::type_config::SequencerOutcome;
        let dir = TempDir::new().unwrap();
        let storage = MvccStorage::new_with_revision_mode(
            RedbBackend::open(dir.path(), &StorageConfig::default()).unwrap(),
            RevisionMode::CommitTs,
        )
        .unwrap();
        storage
            .apply_sequencer_op(
                &seq_op(vec![seq_txn(
                    b"t1",
                    1,
                    10,
                    b"r1",
                    &[b"k"],
                    &[(b"k", b"v1")],
                )]),
                1,
                applied_for(1),
            )
            .unwrap();
        drop(storage);

        let reopened = MvccStorage::new_with_revision_mode(
            RedbBackend::open(dir.path(), &StorageConfig::default()).unwrap(),
            RevisionMode::CommitTs,
        )
        .unwrap();
        let decision = reopened.get_sequencer_decision(10).unwrap().unwrap();
        assert_eq!(decision.txn_id, b"t1".to_vec());
        assert_eq!(decision.commit_ts, 10);
        assert_eq!(reopened.sequencer_last_commit_ts().unwrap(), 10);

        // 索引也恢复：新事务读到旧写时冲突；重复请求仍是 Duplicate。
        let outcomes = reopened
            .apply_sequencer_op(
                &seq_op(vec![
                    seq_txn(b"t2", 2, 11, b"r2", &[b"k"], &[]),
                    seq_txn(b"t1", 2, 12, b"r1", &[], &[]),
                ]),
                2,
                applied_for(2),
            )
            .unwrap();
        assert_eq!(
            outcomes,
            vec![
                SequencerOutcome::Conflict {
                    commit_ts: 11,
                    conflict_key: b"k".to_vec(),
                    conflict_ts: 10,
                },
                SequencerOutcome::Duplicate { commit_ts: 10 },
            ]
        );
    }

    #[test]
    fn test_sequencer_decisions_survive_snapshot() {
        use crate::storage::snapshot::{export_snapshot_data, import_snapshot_data};
        let (_dir, source) = create_commit_ts_storage();
        source
            .apply_sequencer_op(
                &seq_op(vec![seq_txn(
                    b"t1",
                    1,
                    10,
                    b"r1",
                    &[b"k"],
                    &[(b"k", b"v1")],
                )]),
                1,
                applied_for(1),
            )
            .unwrap();

        let data = export_snapshot_data(&source, 1, 1).unwrap();
        assert!(
            data.sys_entries
                .iter()
                .any(|entry| entry.internal_key.starts_with(b"/_sys/seq/")),
            "sequencer records must travel in the /_sys/ snapshot domain"
        );

        let (_dir, restored) = create_commit_ts_storage();
        import_snapshot_data(&restored, &data).unwrap();
        assert_eq!(
            restored.get_sequencer_decision(10).unwrap().unwrap().txn_id,
            b"t1".to_vec()
        );
        assert_eq!(restored.sequencer_last_commit_ts().unwrap(), 10);
    }

    // ──── OCC 谓词校验（#149）────

    fn seq_txn_with_ranges(
        txn_id: &[u8],
        start_ts: u64,
        commit_ts: u64,
        request_id: &[u8],
        reads: &[&[u8]],
        ranges: &[(&[u8], &[u8])],
        writes: &[(&[u8], &[u8])],
    ) -> crate::raft::type_config::SequencerTxnWithRanges {
        use crate::raft::type_config::{SequencerReadRange, SequencerTxnWithRanges};
        SequencerTxnWithRanges {
            txn: seq_txn(txn_id, start_ts, commit_ts, request_id, reads, writes),
            read_ranges: ranges
                .iter()
                .map(|(start, end)| SequencerReadRange {
                    start: start.to_vec(),
                    end: end.to_vec(),
                })
                .collect(),
        }
    }

    fn seq_op_with_ranges(
        txns: Vec<crate::raft::type_config::SequencerTxnWithRanges>,
        max_read_set: u32,
        max_range: u32,
    ) -> crate::raft::type_config::SequencerOp {
        use crate::raft::type_config::{SequencerOp, SequencerValidationLimits};
        SequencerOp::ValidateAndCommitWithRanges {
            txns,
            limits: SequencerValidationLimits {
                max_read_set,
                max_range,
            },
        }
    }

    #[test]
    fn test_sequencer_occ_sees_prior_txn_in_same_batch() {
        use crate::raft::type_config::SequencerOutcome;
        let (_dir, storage) = create_commit_ts_storage();

        // 同一批内 txn1 先写 b；txn2 的 range 谓词必须看见同批前序写入，
        // 不能因为读快照看不到未提交的索引而假阴性。
        let batch = seq_op_with_ranges(
            vec![
                seq_txn_with_ranges(b"t1", 1, 10, b"r1", &[], &[], &[(b"b", b"v1")]),
                seq_txn_with_ranges(b"t2", 5, 20, b"r2", &[], &[(b"a", b"c")], &[]),
            ],
            4_096,
            10_000,
        );
        let outcomes = storage
            .apply_sequencer_op(&batch, 1, applied_for(1))
            .unwrap();
        assert_eq!(
            outcomes,
            vec![
                SequencerOutcome::Committed { commit_ts: 10 },
                SequencerOutcome::Conflict {
                    commit_ts: 20,
                    conflict_key: b"b".to_vec(),
                    conflict_ts: 10,
                },
            ]
        );
        assert!(storage.get_sequencer_decision(20).unwrap().is_none());
    }

    #[test]
    fn test_sequencer_range_phantom_and_retry() {
        use crate::raft::type_config::SequencerOutcome;
        let (_dir, storage) = create_commit_ts_storage();
        storage
            .apply_sequencer_op(
                &seq_op(vec![seq_txn(b"t1", 1, 10, b"r1", &[], &[(b"b", b"v1")])]),
                1,
                applied_for(1),
            )
            .unwrap();

        // 读谓词 [a,m) 在 start_ts=5 时为空；b 在 10 提交 => 幻读必须冲突。
        let outcomes = storage
            .apply_sequencer_op(
                &seq_op_with_ranges(
                    vec![seq_txn_with_ranges(
                        b"t2",
                        5,
                        20,
                        b"r2",
                        &[],
                        &[(b"a", b"m")],
                        &[],
                    )],
                    4_096,
                    10_000,
                ),
                2,
                applied_for(2),
            )
            .unwrap();
        assert_eq!(
            outcomes,
            vec![SequencerOutcome::Conflict {
                commit_ts: 20,
                conflict_key: b"b".to_vec(),
                conflict_ts: 10,
            }]
        );
        assert!(storage.get_sequencer_decision(20).unwrap().is_none());

        // 换新 start_ts（已包含 b）重试：成功。
        let outcomes = storage
            .apply_sequencer_op(
                &seq_op_with_ranges(
                    vec![seq_txn_with_ranges(
                        b"t3",
                        10,
                        21,
                        b"r3",
                        &[],
                        &[(b"a", b"m")],
                        &[],
                    )],
                    4_096,
                    10_000,
                ),
                3,
                applied_for(3),
            )
            .unwrap();
        assert_eq!(
            outcomes,
            vec![SequencerOutcome::Committed { commit_ts: 21 }]
        );
    }

    #[test]
    fn test_sequencer_range_empty_and_unbounded() {
        use crate::raft::type_config::SequencerOutcome;
        let (_dir, storage) = create_commit_ts_storage();
        storage
            .apply_sequencer_op(
                &seq_op(vec![seq_txn(b"t1", 1, 10, b"r1", &[], &[(b"z", b"v")])]),
                1,
                applied_for(1),
            )
            .unwrap();

        let outcomes = storage
            .apply_sequencer_op(
                &seq_op_with_ranges(
                    vec![seq_txn_with_ranges(
                        b"t2",
                        5,
                        20,
                        b"r2",
                        &[],
                        &[(b"c", b"c"), (b"e", b"b")],
                        &[],
                    )],
                    4_096,
                    10_000,
                ),
                2,
                applied_for(2),
            )
            .unwrap();
        assert_eq!(
            outcomes,
            vec![SequencerOutcome::Committed { commit_ts: 20 }]
        );

        // end 为空 = 无上界；必须能命中 start 之后的写。
        let outcomes = storage
            .apply_sequencer_op(
                &seq_op_with_ranges(
                    vec![seq_txn_with_ranges(
                        b"t3",
                        5,
                        21,
                        b"r3",
                        &[],
                        &[(b"m", b"")],
                        &[],
                    )],
                    4_096,
                    10_000,
                ),
                3,
                applied_for(3),
            )
            .unwrap();
        assert_eq!(
            outcomes,
            vec![SequencerOutcome::Conflict {
                commit_ts: 21,
                conflict_key: b"z".to_vec(),
                conflict_ts: 10,
            }]
        );
    }

    #[test]
    fn test_sequencer_range_scan_budget_is_fail_closed() {
        use crate::raft::type_config::{SequencerInvalid, SequencerOutcome};
        let (_dir, storage) = create_commit_ts_storage();
        storage
            .apply_sequencer_op(
                &seq_op(vec![seq_txn(
                    b"t1",
                    1,
                    10,
                    b"r1",
                    &[],
                    &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")],
                )]),
                1,
                applied_for(1),
            )
            .unwrap();

        let outcomes = storage
            .apply_sequencer_op(
                &seq_op_with_ranges(
                    vec![seq_txn_with_ranges(
                        b"t2",
                        5,
                        20,
                        b"r2",
                        &[],
                        &[(b"a", b"z")],
                        &[],
                    )],
                    4_096,
                    2,
                ),
                2,
                applied_for(2),
            )
            .unwrap();
        assert_eq!(
            outcomes,
            vec![SequencerOutcome::Invalid {
                commit_ts: 20,
                reason: SequencerInvalid::RangeScanExceeded,
            }]
        );
        assert!(
            storage.get_sequencer_decision(20).unwrap().is_none(),
            "scan budget exhaustion must not write a decision"
        );

        // 0 = 不限制（与 ValidationIndex 查询约定一致）；此时能看到真实写入，
        // 结果应是冲突而不是「超预算」。
        let outcomes = storage
            .apply_sequencer_op(
                &seq_op_with_ranges(
                    vec![seq_txn_with_ranges(
                        b"t3",
                        5,
                        21,
                        b"r3",
                        &[],
                        &[(b"a", b"z")],
                        &[],
                    )],
                    4_096,
                    0,
                ),
                3,
                applied_for(3),
            )
            .unwrap();
        assert_eq!(
            outcomes,
            vec![SequencerOutcome::Conflict {
                commit_ts: 21,
                conflict_key: b"a".to_vec(),
                conflict_ts: 10,
            }]
        );
    }

    #[test]
    fn test_sequencer_read_set_limit_rejected() {
        use crate::raft::type_config::{SequencerInvalid, SequencerOutcome};
        let (_dir, storage) = create_commit_ts_storage();

        let outcomes = storage
            .apply_sequencer_op(
                &seq_op_with_ranges(
                    vec![seq_txn_with_ranges(
                        b"t1",
                        1,
                        10,
                        b"r1",
                        &[b"a", b"b", b"c"],
                        &[],
                        &[],
                    )],
                    2,
                    10_000,
                ),
                1,
                applied_for(1),
            )
            .unwrap();
        assert_eq!(
            outcomes,
            vec![SequencerOutcome::Invalid {
                commit_ts: 10,
                reason: SequencerInvalid::ReadSetTooLarge,
            }]
        );
        assert!(storage.get_sequencer_decision(10).unwrap().is_none());

        // 放开上限后同一读集正常提交。
        let outcomes = storage
            .apply_sequencer_op(
                &seq_op_with_ranges(
                    vec![seq_txn_with_ranges(
                        b"t2",
                        1,
                        11,
                        b"r2",
                        &[b"a", b"b", b"c"],
                        &[],
                        &[],
                    )],
                    3,
                    10_000,
                ),
                2,
                applied_for(2),
            )
            .unwrap();
        assert_eq!(
            outcomes,
            vec![SequencerOutcome::Committed { commit_ts: 11 }]
        );
    }

    #[test]
    fn test_sequencer_legacy_op_unchanged() {
        use crate::raft::type_config::SequencerOutcome;
        let (_dir, storage) = create_commit_ts_storage();

        // 新变体写入的决策，旧变体仍必须看得见（同一索引/决策日志）。
        storage
            .apply_sequencer_op(
                &seq_op_with_ranges(
                    vec![seq_txn_with_ranges(
                        b"t1",
                        1,
                        10,
                        b"r1",
                        &[],
                        &[],
                        &[(b"k", b"v1")],
                    )],
                    4_096,
                    10_000,
                ),
                1,
                applied_for(1),
            )
            .unwrap();

        let outcomes = storage
            .apply_sequencer_op(
                &seq_op(vec![seq_txn(b"t2", 5, 11, b"r2", &[b"k"], &[])]),
                2,
                applied_for(2),
            )
            .unwrap();
        assert_eq!(
            outcomes,
            vec![SequencerOutcome::Conflict {
                commit_ts: 11,
                conflict_key: b"k".to_vec(),
                conflict_ts: 10,
            }]
        );

        let outcomes = storage
            .apply_sequencer_op(
                &seq_op(vec![seq_txn(
                    b"t3",
                    10,
                    12,
                    b"r3",
                    &[b"k"],
                    &[(b"k2", b"v2")],
                )]),
                3,
                applied_for(3),
            )
            .unwrap();
        assert_eq!(
            outcomes,
            vec![SequencerOutcome::Committed { commit_ts: 12 }]
        );
    }

    // ──── ValidationIndex（#147）────

    fn seq_index_writes(
        txn_id: &[u8],
        start_ts: u64,
        commit_ts: u64,
        writes: &[(&[u8], Option<&[u8]>)],
    ) -> crate::raft::type_config::SequencerTxn {
        use crate::raft::type_config::{SequencerMutation, SequencerTxn};
        SequencerTxn {
            txn_id: txn_id.to_vec(),
            start_ts,
            commit_ts,
            request_id: Vec::new(),
            read_set: Vec::new(),
            mutations: writes
                .iter()
                .map(|(key, value)| match value {
                    Some(value) => SequencerMutation::Put {
                        key: key.to_vec(),
                        value: value.to_vec(),
                        lease_id: None,
                    },
                    None => SequencerMutation::Delete { key: key.to_vec() },
                })
                .collect(),
        }
    }

    fn seq_index_seed(storage: &MvccStorage<RedbBackend>) {
        let batch = seq_op(vec![
            seq_index_writes(
                b"i1",
                1,
                10,
                &[(b"a", Some(b"1")), (b"b", Some(b"1")), (b"d", Some(b"1"))],
            ),
            seq_index_writes(b"i2", 1, 12, &[(b"a", Some(b"2")), (b"c", None)]),
            seq_index_writes(b"i3", 1, 15, &[(b"e", Some(b"3"))]),
        ]);
        storage
            .apply_sequencer_op(&batch, 1, applied_for(1))
            .unwrap();
    }

    #[test]
    fn test_sequencer_index_point_query() {
        let (_dir, storage) = create_commit_ts_storage();
        seq_index_seed(&storage);

        assert_eq!(
            storage.sequencer_point_query(b"a", 9).unwrap(),
            Some(SequencerIndexHit {
                key: b"a".to_vec(),
                commit_ts: 12,
            })
        );
        assert_eq!(
            storage.sequencer_point_query(b"a", 12).unwrap(),
            None,
            "the window is (start_ts, commit_ts]; ts == start_ts is not a conflict"
        );
        assert_eq!(storage.sequencer_point_query(b"z", 0).unwrap(), None);
        assert_eq!(
            storage.sequencer_point_query(b"c", 11).unwrap(),
            Some(SequencerIndexHit {
                key: b"c".to_vec(),
                commit_ts: 12,
            }),
            "Delete is a write for predicate validation"
        );
    }

    #[test]
    fn test_sequencer_range_query_boundaries_and_deletes() {
        let (_dir, storage) = create_commit_ts_storage();
        seq_index_seed(&storage);

        let query = |start: &[u8], end: &[u8], start_ts: u64| {
            storage
                .sequencer_range_query(start, end, start_ts, 0)
                .unwrap()
        };
        let hit = |key: &[u8], commit_ts: u64| {
            SequencerRangeQuery::Hit(SequencerIndexHit {
                key: key.to_vec(),
                commit_ts,
            })
        };

        assert_eq!(query(b"a", b"b", 0), hit(b"a", 12));
        assert_eq!(
            query(b"b", b"d", 11),
            hit(b"c", 12),
            "deletes must be visible to predicate checks"
        );
        assert_eq!(
            query(b"a", b"e", 0),
            hit(b"a", 12),
            "tie on max ts resolves to the lexicographically smallest key"
        );
        assert_eq!(query(b"a", b"", 0), hit(b"e", 15), "empty end = unbounded");
        assert_eq!(query(b"c", b"c", 0), SequencerRangeQuery::Clear);
        assert_eq!(query(b"e", b"b", 0), SequencerRangeQuery::Clear);
        assert_eq!(query(b"b", b"f", 13), hit(b"e", 15));
    }

    #[test]
    fn test_sequencer_range_query_scan_limit_is_fail_closed() {
        let (_dir, storage) = create_commit_ts_storage();
        for i in 0..5u64 {
            let key = format!("k{i}").into_bytes();
            let txn = seq_index_writes(
                format!("l{i}").as_bytes(),
                1,
                10 + i,
                &[(key.as_slice(), Some(b"v"))],
            );
            storage
                .apply_sequencer_op(&seq_op(vec![txn]), i + 1, applied_for(i + 1))
                .unwrap();
        }

        assert_eq!(
            storage.sequencer_range_query(b"k0", b"k9", 0, 3).unwrap(),
            SequencerRangeQuery::Exceeded { scanned: 3 }
        );
        let hit = SequencerRangeQuery::Hit(SequencerIndexHit {
            key: b"k4".to_vec(),
            commit_ts: 14,
        });
        assert_eq!(
            storage.sequencer_range_query(b"k0", b"k9", 0, 5).unwrap(),
            hit
        );
        assert_eq!(
            storage.sequencer_range_query(b"k0", b"k9", 0, 0).unwrap(),
            hit,
            "limit 0 means unbounded"
        );
        assert_eq!(
            storage.sequencer_range_query(b"k0", b"k9", 14, 3).unwrap(),
            SequencerRangeQuery::Exceeded { scanned: 3 },
            "an exhausted budget must never be reported as Clear"
        );
    }

    fn lcg_next(state: &mut u64) -> u64 {
        *state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        *state
    }

    fn brute_point(
        writes: &[(Vec<u8>, u64)],
        key: &[u8],
        start_ts: u64,
    ) -> Option<SequencerIndexHit> {
        let ts = writes
            .iter()
            .filter(|(written, _)| written.as_slice() == key)
            .map(|(_, ts)| *ts)
            .max()?;
        (ts > start_ts).then(|| SequencerIndexHit {
            key: key.to_vec(),
            commit_ts: ts,
        })
    }

    fn brute_range(
        writes: &[(Vec<u8>, u64)],
        start: &[u8],
        end: &[u8],
        start_ts: u64,
    ) -> SequencerRangeQuery {
        let mut best: Option<(Vec<u8>, u64)> = None;
        for (key, ts) in writes {
            if *ts <= start_ts || key.as_slice() < start {
                continue;
            }
            if !end.is_empty() && key.as_slice() >= end {
                continue;
            }
            let replace = match &best {
                None => true,
                Some((best_key, best_ts)) => *ts > *best_ts || (*ts == *best_ts && key < best_key),
            };
            if replace {
                best = Some((key.clone(), *ts));
            }
        }
        match best {
            Some((key, commit_ts)) => {
                SequencerRangeQuery::Hit(SequencerIndexHit { key, commit_ts })
            }
            None => SequencerRangeQuery::Clear,
        }
    }

    #[test]
    fn test_sequencer_index_matches_brute_force() {
        use crate::raft::type_config::{SequencerMutation, SequencerOutcome, SequencerTxn};

        let (_dir, storage) = create_commit_ts_storage();
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut writes: Vec<(Vec<u8>, u64)> = Vec::new();
        let mut txns = Vec::new();
        for i in 0..200u64 {
            let commit_ts = i + 1;
            let key_count = 1 + (lcg_next(&mut state) % 3) as usize;
            let mut mutations = Vec::new();
            for _ in 0..key_count {
                let key = format!("k{:02}", lcg_next(&mut state) % 20).into_bytes();
                writes.push((key.clone(), commit_ts));
                mutations.push(SequencerMutation::Put {
                    key,
                    value: b"v".to_vec(),
                    lease_id: None,
                });
            }
            txns.push(SequencerTxn {
                txn_id: format!("b{i}").into_bytes(),
                start_ts: lcg_next(&mut state) % commit_ts,
                commit_ts,
                request_id: Vec::new(),
                read_set: Vec::new(),
                mutations,
            });
        }
        let outcomes = storage
            .apply_sequencer_op(&seq_op(txns), 1, applied_for(1))
            .unwrap();
        assert!(
            outcomes
                .iter()
                .all(|outcome| matches!(outcome, SequencerOutcome::Committed { .. })),
            "the generated transaction stream must be conflict-free"
        );

        let bounds: [&[u8]; 5] = [b"k00", b"k05", b"k10", b"k15", b"k20"];
        for (i, start) in bounds.iter().enumerate() {
            for end in &bounds[i + 1..] {
                for _ in 0..5 {
                    let start_ts = lcg_next(&mut state) % 201;
                    assert_eq!(
                        storage
                            .sequencer_range_query(start, end, start_ts, 0)
                            .unwrap(),
                        brute_range(&writes, start, end, start_ts),
                        "range [{}..{}) start_ts={start_ts}",
                        String::from_utf8_lossy(start),
                        String::from_utf8_lossy(end)
                    );
                }
            }
        }
        for _ in 0..20 {
            let start_ts = lcg_next(&mut state) % 201;
            assert_eq!(
                storage
                    .sequencer_range_query(b"k00", b"", start_ts, 0)
                    .unwrap(),
                brute_range(&writes, b"k00", b"", start_ts)
            );
        }
        for n in 0..20 {
            let key = format!("k{n:02}").into_bytes();
            for _ in 0..5 {
                let start_ts = lcg_next(&mut state) % 201;
                assert_eq!(
                    storage.sequencer_point_query(&key, start_ts).unwrap(),
                    brute_point(&writes, &key, start_ts),
                    "point {} start_ts={start_ts}",
                    String::from_utf8_lossy(&key)
                );
            }
        }
    }

    fn clear_sequencer_indexes(storage: &MvccStorage<RedbBackend>) {
        storage
            .backend()
            .write(|tx| {
                for prefix in [SEQUENCER_KEY_INDEX_PREFIX, SEQUENCER_RANGE_INDEX_PREFIX] {
                    for (key, _) in tx.iter_prefix(TABLE_KV, prefix)? {
                        tx.remove(TABLE_KV, &key)?;
                    }
                }
                tx.remove(TABLE_KV, SEQUENCER_INDEX_LAST_TS)?;
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn test_sequencer_index_catch_up_after_lag() {
        let (_dir, storage) = create_commit_ts_storage();
        seq_index_seed(&storage);
        let seed_writes = vec![
            (b"a".to_vec(), 10),
            (b"b".to_vec(), 10),
            (b"d".to_vec(), 10),
            (b"a".to_vec(), 12),
            (b"c".to_vec(), 12),
            (b"e".to_vec(), 15),
        ];
        let assert_index_matches = || {
            for (start, end, start_ts) in [
                (&b"a"[..], &b"e"[..], 0),
                (&b"b"[..], &b"d"[..], 11),
                (&b"a"[..], &b""[..], 0),
            ] {
                assert_eq!(
                    storage
                        .sequencer_range_query(start, end, start_ts, 0)
                        .unwrap(),
                    brute_range(&seed_writes, start, end, start_ts)
                );
            }
        };
        assert_index_matches();

        clear_sequencer_indexes(&storage);
        assert_eq!(
            storage.sequencer_range_query(b"a", b"", 0, 0).unwrap(),
            SequencerRangeQuery::Clear,
            "an index that lost its rows must not report stale hits"
        );
        assert_eq!(storage.sequencer_index_last_ts().unwrap(), 0);

        let first = storage.catch_up_sequencer_index(2).unwrap();
        assert_eq!(
            first,
            SequencerIndexRebuild {
                from_ts: 0,
                to_ts: 15,
                decisions: 3,
                keys: 6,
            }
        );
        assert_index_matches();
        assert_eq!(
            storage.sequencer_point_query(b"e", 14).unwrap(),
            Some(SequencerIndexHit {
                key: b"e".to_vec(),
                commit_ts: 15,
            })
        );
        assert_eq!(storage.sequencer_index_last_ts().unwrap(), 15);

        let caught_up = storage.catch_up_sequencer_index(2).unwrap();
        assert_eq!(caught_up.decisions, 0);
        assert_eq!(caught_up.from_ts, 15);
        assert_eq!(caught_up.to_ts, 15);

        clear_sequencer_indexes(&storage);
        let rebuilt = storage.rebuild_sequencer_validation_index(2).unwrap();
        assert_eq!(
            rebuilt,
            SequencerIndexRebuild {
                from_ts: 0,
                to_ts: 15,
                decisions: 3,
                keys: 6,
            }
        );
        assert_index_matches();
        assert_eq!(storage.sequencer_index_last_ts().unwrap(), 15);

        let rebuilt_again = storage.rebuild_sequencer_validation_index(2).unwrap();
        assert_eq!(
            rebuilt_again,
            SequencerIndexRebuild {
                from_ts: 0,
                to_ts: 15,
                decisions: 3,
                keys: 6,
            },
            "rebuild is deterministic and does not depend on the previous index state"
        );
        assert_index_matches();
    }

    #[test]
    fn test_sequencer_index_survives_reopen_and_snapshot() {
        use crate::storage::snapshot::{export_snapshot_data, import_snapshot_data};

        let (dir, storage) = create_commit_ts_storage();
        seq_index_seed(&storage);
        drop(storage);

        let backend = RedbBackend::open(dir.path(), &StorageConfig::default()).unwrap();
        let reopened =
            MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs).unwrap();
        assert_eq!(
            reopened.sequencer_range_query(b"a", b"e", 0, 0).unwrap(),
            SequencerRangeQuery::Hit(SequencerIndexHit {
                key: b"a".to_vec(),
                commit_ts: 12,
            })
        );
        assert_eq!(reopened.sequencer_index_last_ts().unwrap(), 15);

        let data = export_snapshot_data(&reopened, 1, 1).unwrap();
        assert!(
            data.sys_entries
                .iter()
                .any(|entry| entry.internal_key.starts_with(SEQUENCER_RANGE_INDEX_PREFIX)),
            "the ordered index must travel in the /_sys/ snapshot domain"
        );
        let (_restored_dir, restored) = create_commit_ts_storage();
        import_snapshot_data(&restored, &data).unwrap();
        assert_eq!(
            restored.sequencer_range_query(b"b", b"d", 11, 0).unwrap(),
            SequencerRangeQuery::Hit(SequencerIndexHit {
                key: b"c".to_vec(),
                commit_ts: 12,
            })
        );
        assert_eq!(restored.sequencer_index_last_ts().unwrap(), 15);
    }

    // ──── CommitDecision 应用（#148）────

    fn decision(
        commit_ts: Revision,
        mutations: Vec<crate::raft::type_config::SequencerMutation>,
    ) -> crate::raft::type_config::CommitDecisionApply {
        crate::raft::type_config::CommitDecisionApply {
            commit_ts,
            mutations,
        }
    }

    fn put(key: &[u8], value: &[u8]) -> crate::raft::type_config::SequencerMutation {
        crate::raft::type_config::SequencerMutation::Put {
            key: key.to_vec(),
            value: value.to_vec(),
            lease_id: None,
        }
    }

    fn delete(key: &[u8]) -> crate::raft::type_config::SequencerMutation {
        crate::raft::type_config::SequencerMutation::Delete { key: key.to_vec() }
    }

    #[test]
    fn test_apply_commit_decisions_multi_key_is_atomic() {
        use crate::storage::mvcc::EventType;

        let (_dir, storage) = create_commit_ts_storage();
        storage
            .put_at_commit_ts(b"c", b"old", None, 5, applied_for(1))
            .unwrap();

        let result = storage
            .apply_commit_decisions(
                &[decision(
                    10,
                    vec![put(b"a", b"1"), put(b"b", b"2"), delete(b"c")],
                )],
                3,
                applied_for(3),
            )
            .unwrap();
        assert_eq!(result.through_ts, 10);
        assert_eq!(result.decisions, 1);
        assert_eq!(storage.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(storage.get(b"b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(storage.get(b"c").unwrap(), None);
        assert_eq!(storage.current_revision(), 10);
        assert_eq!(storage.last_applied_decision_ts().unwrap(), 10);

        let events = storage.read_changelog_entries(10).unwrap();
        let event = events.iter().find(|e| e.revision == 10).unwrap();
        assert_eq!(event.event_type, EventType::Txn);
        assert_eq!(event.changes.len(), 3, "one event carries the whole txn");
        assert_eq!(storage.get_kv_metadata(b"b").unwrap().unwrap().version, 1);
    }

    #[test]
    fn test_apply_commit_decisions_replay_is_idempotent() {
        let (_dir, storage) = create_commit_ts_storage();
        let first = [
            decision(10, vec![put(b"a", b"1")]),
            decision(20, vec![put(b"b", b"2")]),
        ];
        assert_eq!(
            storage
                .apply_commit_decisions(&first, 1, applied_for(1))
                .unwrap()
                .decisions,
            2
        );

        let replay = storage
            .apply_commit_decisions(&first, 2, applied_for(2))
            .unwrap();
        assert_eq!(replay.through_ts, 20);
        assert_eq!(replay.decisions, 0);
        assert_eq!(storage.get_kv_metadata(b"a").unwrap().unwrap().version, 1);
        assert_eq!(storage.read_changelog_entries(10).unwrap().len(), 2);

        // 崩溃后从旧游标重跑时，旧决策与新决策可能同批出现：旧的跳过、新的生效。
        let result = storage
            .apply_commit_decisions(
                &[
                    decision(20, vec![put(b"b", b"2")]),
                    decision(30, vec![put(b"c", b"3")]),
                ],
                3,
                applied_for(3),
            )
            .unwrap();
        assert_eq!(result.through_ts, 30);
        assert_eq!(result.decisions, 1);
        assert_eq!(storage.get_kv_metadata(b"b").unwrap().unwrap().version, 1);
        assert_eq!(storage.get(b"c").unwrap(), Some(b"3".to_vec()));
    }

    #[test]
    fn test_apply_commit_decisions_empty_advances_watermark() {
        let (_dir, storage) = create_commit_ts_storage();
        let result = storage
            .apply_commit_decisions(&[decision(10, Vec::new())], 1, applied_for(1))
            .unwrap();
        assert_eq!(result.through_ts, 10);
        assert_eq!(result.decisions, 1);
        assert_eq!(storage.current_revision(), 10);
        assert_eq!(storage.last_applied_decision_ts().unwrap(), 10);
        assert!(!storage.changelog_contains_revision(10).unwrap());

        // 游标丢失后重放空决策：没有 changelog 可对账，但 current_revision
        // 已越过它，视为已应用，不得把 safe_ts 永久钉住。
        storage
            .backend
            .write(|tx| tx.remove(TABLE_KV, DECISION_APPLIED_TS))
            .unwrap();
        assert_eq!(storage.last_applied_decision_ts().unwrap(), 0);
        let replay = storage
            .apply_commit_decisions(&[decision(10, Vec::new())], 2, applied_for(2))
            .unwrap();
        assert_eq!(replay.through_ts, 10);
        assert_eq!(replay.decisions, 1);
    }

    #[test]
    fn test_apply_commit_decisions_cursor_survives_reopen_and_snapshot() {
        use crate::storage::snapshot::{export_snapshot_data, import_snapshot_data};

        let (dir, storage) = create_commit_ts_storage();
        storage
            .apply_commit_decisions(
                &[
                    decision(10, vec![put(b"a", b"1")]),
                    decision(20, vec![put(b"b", b"2")]),
                ],
                1,
                applied_for(1),
            )
            .unwrap();
        drop(storage);

        let backend = RedbBackend::open(dir.path(), &StorageConfig::default()).unwrap();
        let reopened =
            MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs).unwrap();
        assert_eq!(reopened.last_applied_decision_ts().unwrap(), 20);

        let data = export_snapshot_data(&reopened, 1, 1).unwrap();
        let (_restored_dir, restored) = create_commit_ts_storage();
        import_snapshot_data(&restored, &data).unwrap();
        assert_eq!(restored.last_applied_decision_ts().unwrap(), 20);
        let replay = restored
            .apply_commit_decisions(&[decision(10, vec![put(b"a", b"1")])], 2, applied_for(2))
            .unwrap();
        assert_eq!(replay.decisions, 0);
    }

    #[test]
    fn test_apply_commit_decisions_stops_before_fence_overtake() {
        let (_dir, storage) = create_commit_ts_storage();
        storage
            .advance_commit_ts_watermark(100, applied_for(1))
            .unwrap();

        let result = storage
            .apply_commit_decisions(&[decision(50, vec![put(b"a", b"1")])], 2, applied_for(2))
            .unwrap();
        assert_eq!(
            result.through_ts, 0,
            "must not skip past an unapplied decision"
        );
        assert_eq!(result.decisions, 0);
        assert_eq!(storage.get(b"a").unwrap(), None);
        assert_eq!(storage.last_applied_decision_ts().unwrap(), 0);
        assert!(storage.read_changelog_entries(50).unwrap().is_empty());
    }

    #[test]
    fn test_sequencer_decisions_after_scans_ordered() {
        let (_dir, storage) = create_commit_ts_storage();
        let op = seq_op(vec![
            seq_txn(b"t1", 1, 10, b"r1", &[], &[(b"a", b"1")]),
            seq_txn(b"t2", 2, 20, b"r2", &[], &[(b"b", b"2")]),
        ]);
        storage.apply_sequencer_op(&op, 1, applied_for(1)).unwrap();

        let first = storage.sequencer_decisions_after(0, 1).unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].commit_ts, 10);
        let second = storage.sequencer_decisions_after(10, 10).unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].commit_ts, 20);
        assert!(storage
            .sequencer_decisions_after(20, 10)
            .unwrap()
            .is_empty());
        assert_eq!(storage.next_sequencer_decision_ts(0).unwrap(), Some(10));
        assert_eq!(storage.next_sequencer_decision_ts(20).unwrap(), None);
    }

    // ──── OCC 区间写（#198）────

    fn range_delete(start: &[u8], end: &[u8]) -> crate::raft::type_config::SequencerMutation {
        crate::raft::type_config::SequencerMutation::DeleteRange {
            start: start.to_vec(),
            end: end.to_vec(),
        }
    }

    fn range_delete_txn(
        txn_id: &[u8],
        start_ts: u64,
        commit_ts: u64,
        start: &[u8],
        end: &[u8],
    ) -> crate::raft::type_config::SequencerTxn {
        crate::raft::type_config::SequencerTxn {
            txn_id: txn_id.to_vec(),
            start_ts,
            commit_ts,
            request_id: Vec::new(),
            read_set: Vec::new(),
            mutations: vec![range_delete(start, end)],
        }
    }

    #[test]
    fn test_sequencer_range_delete_conflicts_with_point_and_range_read() {
        use crate::raft::type_config::SequencerOutcome;
        let (_dir, storage) = create_commit_ts_storage();

        let outcomes = storage
            .apply_sequencer_op(
                &seq_op(vec![range_delete_txn(b"t1", 1, 10, b"b", b"f")]),
                1,
                applied_for(1),
            )
            .unwrap();
        assert_eq!(
            outcomes,
            vec![SequencerOutcome::Committed { commit_ts: 10 }]
        );

        let hit = |key: &[u8], commit_ts: u64| {
            Some(SequencerIndexHit {
                key: key.to_vec(),
                commit_ts,
            })
        };
        assert_eq!(
            storage.sequencer_point_query(b"c", 5).unwrap(),
            hit(b"b", 10)
        );
        assert_eq!(
            storage.sequencer_point_query(b"c", 10).unwrap(),
            None,
            "ts == start_ts is outside the conflict window"
        );
        assert_eq!(storage.sequencer_point_query(b"a", 5).unwrap(), None);
        assert_eq!(
            storage.sequencer_point_query(b"f", 5).unwrap(),
            None,
            "the deleted range is half-open"
        );

        let range_hit = |key: &[u8], commit_ts: u64| {
            SequencerRangeQuery::Hit(SequencerIndexHit {
                key: key.to_vec(),
                commit_ts,
            })
        };
        assert_eq!(
            storage.sequencer_range_query(b"a", b"c", 5, 0).unwrap(),
            range_hit(b"b", 10)
        );
        assert_eq!(
            storage.sequencer_range_query(b"a", b"b", 5, 0).unwrap(),
            SequencerRangeQuery::Clear,
            "adjacent ranges do not overlap"
        );
        assert_eq!(
            storage.sequencer_range_query(b"f", b"z", 5, 0).unwrap(),
            SequencerRangeQuery::Clear
        );
        assert_eq!(
            storage.sequencer_range_query(b"a", b"", 5, 0).unwrap(),
            range_hit(b"b", 10),
            "an empty range end is unbounded"
        );
    }

    #[test]
    fn test_sequencer_range_delete_same_batch_conflicts() {
        use crate::raft::type_config::{SequencerOutcome, SequencerTxnWithRanges};
        let (_dir, storage) = create_commit_ts_storage();

        let batch = seq_op_with_ranges(
            vec![
                SequencerTxnWithRanges {
                    txn: range_delete_txn(b"t1", 1, 10, b"b", b"f"),
                    read_ranges: Vec::new(),
                },
                seq_txn_with_ranges(b"t2", 5, 20, b"r2", &[], &[(b"a", b"z")], &[]),
            ],
            4_096,
            10_000,
        );
        let outcomes = storage
            .apply_sequencer_op(&batch, 1, applied_for(1))
            .unwrap();
        assert_eq!(
            outcomes,
            vec![
                SequencerOutcome::Committed { commit_ts: 10 },
                SequencerOutcome::Conflict {
                    commit_ts: 20,
                    conflict_key: b"b".to_vec(),
                    conflict_ts: 10,
                },
            ]
        );
        assert!(storage.get_sequencer_decision(20).unwrap().is_none());
    }

    #[test]
    fn test_sequencer_interval_index_rebuild_is_equivalent() {
        let (_dir, storage) = create_commit_ts_storage();
        let batch = seq_op(vec![
            range_delete_txn(b"t1", 1, 10, b"a", b"f"),
            seq_txn(b"t2", 1, 12, b"r2", &[], &[(b"e", b"v")]),
        ]);
        storage
            .apply_sequencer_op(&batch, 1, applied_for(1))
            .unwrap();

        let point = Some(SequencerIndexHit {
            key: b"e".to_vec(),
            commit_ts: 12,
        });
        let interval = SequencerRangeQuery::Hit(SequencerIndexHit {
            key: b"a".to_vec(),
            commit_ts: 10,
        });
        assert_eq!(
            storage.sequencer_point_query(b"c", 5).unwrap(),
            Some(SequencerIndexHit {
                key: b"a".to_vec(),
                commit_ts: 10,
            })
        );
        assert_eq!(
            storage.sequencer_range_query(b"a", b"e", 0, 0).unwrap(),
            interval
        );
        assert_eq!(
            storage.sequencer_range_query(b"a", b"z", 0, 0).unwrap(),
            SequencerRangeQuery::Hit(SequencerIndexHit {
                key: b"e".to_vec(),
                commit_ts: 12,
            })
        );
        assert_eq!(storage.sequencer_point_query(b"e", 0).unwrap(), point);

        let stats = storage.rebuild_sequencer_validation_index(16).unwrap();
        assert_eq!(stats.decisions, 2);
        assert_eq!(storage.sequencer_index_last_ts().unwrap(), 12);
        assert_eq!(
            storage.sequencer_range_query(b"a", b"e", 0, 0).unwrap(),
            SequencerRangeQuery::Hit(SequencerIndexHit {
                key: b"a".to_vec(),
                commit_ts: 10,
            })
        );
        assert_eq!(storage.sequencer_point_query(b"e", 0).unwrap(), point);
    }

    #[test]
    fn test_sequencer_interval_index_survives_snapshot() {
        use crate::storage::snapshot::{export_snapshot_data, import_snapshot_data};

        let (_dir, storage) = create_commit_ts_storage();
        storage
            .apply_sequencer_op(
                &seq_op(vec![range_delete_txn(b"t1", 1, 10, b"a", b"f")]),
                1,
                applied_for(1),
            )
            .unwrap();
        let data = export_snapshot_data(&storage, 1, 1).unwrap();
        let (_restored_dir, restored) = create_commit_ts_storage();
        import_snapshot_data(&restored, &data).unwrap();

        assert_eq!(
            restored.sequencer_point_query(b"c", 5).unwrap(),
            Some(SequencerIndexHit {
                key: b"a".to_vec(),
                commit_ts: 10,
            })
        );
        assert_eq!(
            restored.sequencer_range_query(b"a", b"c", 5, 0).unwrap(),
            SequencerRangeQuery::Hit(SequencerIndexHit {
                key: b"a".to_vec(),
                commit_ts: 10,
            })
        );
    }

    #[test]
    fn test_sequencer_interval_scan_budget_is_fail_closed() {
        use crate::raft::type_config::{SequencerInvalid, SequencerOutcome};
        let (_dir, storage) = create_commit_ts_storage();
        for i in 0..5u64 {
            let start = format!("k{i}");
            let end = format!("k{i}z");
            storage
                .apply_sequencer_op(
                    &seq_op(vec![range_delete_txn(
                        format!("t{i}").as_bytes(),
                        1,
                        10 + i,
                        start.as_bytes(),
                        end.as_bytes(),
                    )]),
                    i + 1,
                    applied_for(i + 1),
                )
                .unwrap();
        }

        let outcomes = storage
            .apply_sequencer_op(
                &seq_op_with_ranges(
                    vec![seq_txn_with_ranges(
                        b"t9",
                        1,
                        20,
                        b"r9",
                        &[b"nope"],
                        &[],
                        &[],
                    )],
                    4_096,
                    3,
                ),
                7,
                applied_for(7),
            )
            .unwrap();
        assert_eq!(
            outcomes,
            vec![SequencerOutcome::Invalid {
                commit_ts: 20,
                reason: SequencerInvalid::RangeScanExceeded,
            }]
        );
        assert!(storage.get_sequencer_decision(20).unwrap().is_none());
    }

    #[test]
    fn test_sequencer_interval_scan_is_not_per_read_key() {
        use crate::raft::type_config::SequencerOutcome;
        let (_dir, storage) = create_commit_ts_storage();
        storage
            .apply_sequencer_op(
                &seq_op(vec![range_delete_txn(b"t1", 1, 10, b"a", b"z")]),
                1,
                applied_for(1),
            )
            .unwrap();

        INTERVAL_INDEX_SCANS.with(|scans| scans.set(0));
        let outcomes = storage
            .apply_sequencer_op(
                &seq_op_with_ranges(
                    vec![seq_txn_with_ranges(
                        b"t2",
                        5,
                        20,
                        b"r2",
                        &[b"0", b"1", b"a1"],
                        &[],
                        &[],
                    )],
                    4_096,
                    10_000,
                ),
                2,
                applied_for(2),
            )
            .unwrap();
        assert!(matches!(
            outcomes.as_slice(),
            [SequencerOutcome::Conflict {
                conflict_key,
                conflict_ts: 10,
                ..
            }] if conflict_key == b"a1"
        ));
        assert_eq!(
            INTERVAL_INDEX_SCANS.with(std::cell::Cell::get),
            1,
            "the interval index must be scanned once per txn, not once per read key"
        );
    }

    #[test]
    fn test_apply_commit_decisions_delete_range_is_atomic_and_replays() {
        use crate::storage::mvcc::EventType;

        let (_dir, storage) = create_commit_ts_storage();
        storage
            .put_at_commit_ts(b"a", b"1", None, 3, applied_for(1))
            .unwrap();
        storage
            .put_at_commit_ts(b"b", b"2", None, 4, applied_for(2))
            .unwrap();
        storage
            .put_at_commit_ts(b"c", b"3", None, 5, applied_for(3))
            .unwrap();

        let applied = [decision(
            10,
            vec![range_delete(b"a", b"d"), put(b"d", b"v")],
        )];
        let result = storage
            .apply_commit_decisions(&applied, 4, applied_for(4))
            .unwrap();
        assert_eq!(result.through_ts, 10);
        assert_eq!(result.decisions, 1);
        assert_eq!(storage.get(b"a").unwrap(), None);
        assert_eq!(storage.get(b"b").unwrap(), None);
        assert_eq!(storage.get(b"c").unwrap(), None);
        assert_eq!(storage.get(b"d").unwrap(), Some(b"v".to_vec()));
        assert_eq!(storage.current_revision(), 10);
        assert_eq!(storage.last_applied_decision_ts().unwrap(), 10);

        let events = storage.read_changelog_entries(10).unwrap();
        let event = events.iter().find(|event| event.revision == 10).unwrap();
        assert_eq!(event.event_type, EventType::Txn);
        assert_eq!(event.changes.len(), 4, "three tombstones and one put");

        let replay = storage
            .apply_commit_decisions(&applied, 5, applied_for(5))
            .unwrap();
        assert_eq!(replay.through_ts, 10);
        assert_eq!(replay.decisions, 0);
        assert_eq!(storage.get_kv_metadata(b"d").unwrap().unwrap().version, 1);
    }

    #[test]
    fn test_apply_commit_decisions_delete_range_stops_at_occupied_ts() {
        let (_dir, storage) = create_commit_ts_storage();
        storage
            .put_at_commit_ts(b"x", b"1", None, 5, applied_for(1))
            .unwrap();
        storage
            .delete_range_at_commit_ts(b"a", b"d", 10, applied_for(2))
            .unwrap();

        let result = storage
            .apply_commit_decisions(
                &[decision(10, vec![range_delete(b"x", b"z")])],
                3,
                applied_for(3),
            )
            .unwrap();
        assert_eq!(
            result.through_ts, 0,
            "must not treat a foreign write as applied"
        );
        assert_eq!(result.decisions, 0);
        assert_eq!(storage.get(b"x").unwrap(), Some(b"1".to_vec()));
    }
    // ──── Commit Log（#153）────

    fn commit_record(
        commit_ts: Revision,
        region_id: u64,
        value: &[u8],
    ) -> crate::storage::commit_log::CommitRecord {
        use crate::storage::commit_log::{CommitChange, CommitRecord, COMMIT_LOG_STAGING_SHARD_ID};
        CommitRecord {
            shard_id: COMMIT_LOG_STAGING_SHARD_ID,
            commit_ts,
            txn_id: format!("txn-{commit_ts}-{region_id}").into_bytes(),
            start_ts: commit_ts.saturating_sub(1),
            region_id,
            txn_bytes: 32,
            changes: vec![CommitChange {
                key: b"k".to_vec(),
                value: Some(value.to_vec()),
                prev: None,
            }],
        }
    }

    fn commit_log_rows(storage: &MvccStorage<RedbBackend>) -> Vec<(Vec<u8>, Vec<u8>)> {
        storage
            .backend()
            .read(|tx| {
                tx.iter_prefix(
                    TABLE_COMMIT_LOG,
                    crate::storage::commit_log::COMMIT_LOG_PREFIX,
                )
            })
            .unwrap()
    }

    #[test]
    fn test_commit_log_append_is_idempotent_and_rejects_conflicts() {
        let (_dir, storage) = create_commit_ts_storage();
        let record = commit_record(10, 1, b"v1");

        assert_eq!(
            storage
                .append_commit_record(&record, 4 * 1024 * 1024)
                .unwrap(),
            crate::storage::commit_log::CommitLogAppend::Inserted
        );
        assert_eq!(
            storage
                .append_commit_record(&record, 4 * 1024 * 1024)
                .unwrap(),
            crate::storage::commit_log::CommitLogAppend::Duplicate
        );
        assert_eq!(commit_log_rows(&storage).len(), 1, "exactly one row");

        let conflicting = commit_record(10, 1, b"v2");
        assert!(
            storage
                .append_commit_record(&conflicting, 4 * 1024 * 1024)
                .is_err(),
            "a different record for the same (shard, ts, region) must fail closed"
        );
        let rows = commit_log_rows(&storage);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            crate::storage::commit_log::CommitRecord::decode(&rows[0].1).unwrap(),
            record,
            "the stored row must not be overwritten"
        );
    }

    #[test]
    fn test_commit_log_reads_fragments_in_commit_ts_order() {
        let (_dir, storage) = create_commit_ts_storage();
        let shard = crate::storage::commit_log::COMMIT_LOG_STAGING_SHARD_ID;
        for (ts, region, value) in [(10u64, 3u64, b"c".as_slice()), (20, 2, b"b"), (10, 1, b"a")] {
            storage
                .append_commit_record(&commit_record(ts, region, value), 4 * 1024 * 1024)
                .unwrap();
        }

        let all = storage.commit_records_after(shard, 0, 0).unwrap();
        let order: Vec<(u64, u64)> = all
            .iter()
            .map(|record| (record.commit_ts, record.region_id))
            .collect();
        assert_eq!(order, vec![(10, 1), (10, 3), (20, 2)]);

        let later = storage.commit_records_after(shard, 10, 0).unwrap();
        assert_eq!(later.len(), 1);
        assert_eq!((later[0].commit_ts, later[0].region_id), (20, 2));

        let at = storage.commit_records_at(shard, 10).unwrap();
        assert_eq!(at.len(), 2, "both region fragments for commit_ts=10");
        assert_eq!(at[0].changes[0].value.as_deref(), Some(b"a".as_slice()));

        assert_eq!(storage.commit_records_after(shard, 0, 1).unwrap().len(), 1);
    }

    #[test]
    fn test_commit_log_survives_reopen() {
        let dir = TempDir::new().unwrap();
        let record = commit_record(15, 4, b"kept");
        {
            let storage = MvccStorage::new_with_revision_mode(
                RedbBackend::open(dir.path(), &StorageConfig::default()).unwrap(),
                RevisionMode::CommitTs,
            )
            .unwrap();
            storage
                .append_commit_record(&record, 4 * 1024 * 1024)
                .unwrap();
        }

        let reopened = MvccStorage::new_with_revision_mode(
            RedbBackend::open(dir.path(), &StorageConfig::default()).unwrap(),
            RevisionMode::CommitTs,
        )
        .unwrap();
        let shard = crate::storage::commit_log::COMMIT_LOG_STAGING_SHARD_ID;
        assert_eq!(
            reopened.commit_records_after(shard, 0, 0).unwrap(),
            vec![record]
        );
    }
    // ──── Commit Log shard 元数据（#154）────

    #[test]
    fn test_commit_log_shard_ops_persist_and_replay_idempotently() {
        use crate::raft::type_config::CommitLogShardOp;

        let (_dir, storage) = create_storage();
        let open = CommitLogShardOp::Open {
            start_ts: 1,
            opened_at_ms: 1_000,
        };
        let shards = storage
            .apply_commit_log_shard_op(&open, 7, applied_for(7))
            .unwrap();
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0].shard_id, 1);
        assert_eq!(shards[0].start_ts, 1);
        assert!(shards[0].covers(1) && shards[0].covers(u64::MAX / 2));

        // 同一 revision 重放：不重复建表，也不报错（raft 重放路径）。
        let replayed = storage
            .apply_commit_log_shard_op(&open, 7, applied_for(7))
            .unwrap();
        assert_eq!(replayed, shards);

        let end = CommitLogShardOp::SetEnd {
            shard_id: 1,
            end_ts: 100,
            now_ms: 2_000,
        };
        storage
            .apply_commit_log_shard_op(&end, 8, applied_for(8))
            .unwrap();
        let seal = CommitLogShardOp::Seal {
            shard_id: 1,
            now_ms: 3_000,
        };
        let shards = storage
            .apply_commit_log_shard_op(&seal, 9, applied_for(9))
            .unwrap();
        assert_eq!(shards[0].end_ts, 100);
        assert_eq!(shards[0].sealed_at_ms, 3_000);
        assert_eq!(
            storage.commit_log_shard_table().unwrap().shards,
            shards,
            "the same table is readable through the public accessor"
        );
    }

    #[test]
    fn test_invalid_commit_log_shard_ops_do_not_fail_apply() {
        use crate::raft::type_config::CommitLogShardOp;

        let (_dir, storage) = create_storage();
        storage
            .apply_commit_log_shard_op(
                &CommitLogShardOp::Seal {
                    shard_id: 9,
                    now_ms: 1,
                },
                1,
                applied_for(1),
            )
            .expect("unknown shard seal is a no-op, not an apply error");
        assert!(storage.commit_log_shard_table().unwrap().shards.is_empty());
    }

    #[test]
    fn test_apply_commit_log_gc_is_monotonic_and_structural() {
        use crate::raft::type_config::CommitLogShardOp;

        let (_dir, storage) = create_storage();
        let ops = [
            CommitLogShardOp::Open {
                start_ts: 1,
                opened_at_ms: 1_000,
            },
            CommitLogShardOp::SetEnd {
                shard_id: 1,
                end_ts: 101,
                now_ms: 2_000,
            },
            CommitLogShardOp::Seal {
                shard_id: 1,
                now_ms: 3_000,
            },
            CommitLogShardOp::Open {
                start_ts: 101,
                opened_at_ms: 3_000,
            },
            CommitLogShardOp::SetEnd {
                shard_id: 2,
                end_ts: 201,
                now_ms: 4_000,
            },
        ];
        for (index, op) in ops.iter().enumerate() {
            let revision = index as u64 + 1;
            storage
                .apply_commit_log_shard_op(op, revision, applied_for(revision))
                .unwrap();
        }

        let gc = CommitLogShardOp::Gc { through_ts: 101 };
        storage
            .apply_commit_log_shard_op(&gc, 6, applied_for(6))
            .unwrap();
        assert_eq!(storage.commit_log_shard_table().unwrap().gc_before_ts, 101);

        // Raft replay of the same command must not move the watermark again.
        storage
            .apply_commit_log_shard_op(&gc, 6, applied_for(6))
            .unwrap();
        assert_eq!(storage.commit_log_shard_table().unwrap().gc_before_ts, 101);

        // Non-boundary and still-unsealed targets are metadata-level rejections:
        // they must not fail the raft apply (D11.5).
        storage
            .apply_commit_log_shard_op(&CommitLogShardOp::Gc { through_ts: 150 }, 7, applied_for(7))
            .unwrap();
        storage
            .apply_commit_log_shard_op(&CommitLogShardOp::Gc { through_ts: 201 }, 8, applied_for(8))
            .unwrap();
        assert_eq!(
            storage.commit_log_shard_table().unwrap().gc_before_ts,
            101,
            "rejected gc ops leave the table unchanged"
        );
    }

    // ──── Commit Log shard 组记录追加（#154）────

    fn shard_record(
        commit_ts: Revision,
        shard_id: u64,
        region_id: u64,
        key: &[u8],
        value: &[u8],
    ) -> crate::storage::commit_log::CommitRecord {
        use crate::storage::commit_log::{CommitChange, CommitRecord};
        CommitRecord {
            shard_id,
            commit_ts,
            txn_id: format!("txn-{commit_ts}-{region_id}").into_bytes(),
            start_ts: commit_ts.saturating_sub(1),
            region_id,
            txn_bytes: 32,
            changes: vec![CommitChange {
                key: key.to_vec(),
                value: Some(value.to_vec()),
                prev: None,
            }],
        }
    }

    #[test]
    fn test_commit_log_append_is_idempotent_and_rejects_out_of_range() {
        let (_dir, storage) = create_commit_ts_storage();
        let in_range = shard_record(150, 2, 7, b"k", b"v");
        let out_of_range = shard_record(250, 2, 7, b"k", b"v");
        let result = storage
            .apply_commit_log_append(
                2,
                100,
                200,
                &[in_range.clone(), out_of_range],
                4 * 1024 * 1024,
                1,
                applied_for(1),
            )
            .unwrap();
        assert_eq!(
            (result.inserted, result.rejected, result.cursor_ts),
            (1, 1, 150)
        );
        assert_eq!(storage.commit_log_record_count().unwrap(), 1);
        assert_eq!(storage.commit_log_shipped_through(7).unwrap(), 150);

        let replay = storage
            .apply_commit_log_append(2, 100, 200, &[in_range], 4 * 1024 * 1024, 2, applied_for(2))
            .unwrap();
        assert_eq!((replay.inserted, replay.duplicates), (0, 1));
        assert_eq!(
            storage.commit_log_record_count().unwrap(),
            1,
            "duplicates must not double count"
        );
        assert_eq!(storage.commit_log_cursor().unwrap(), 150);
        assert_eq!(storage.commit_records_after(2, 0, 0).unwrap().len(), 1);
    }

    #[test]
    fn test_commit_log_shipped_watermark_is_per_region_and_monotonic() {
        let (_dir, storage) = create_commit_ts_storage();
        storage
            .apply_commit_log_append(
                2,
                100,
                200,
                &[shard_record(150, 2, 7, b"k", b"v")],
                4 * 1024 * 1024,
                1,
                applied_for(1),
            )
            .unwrap();
        storage
            .apply_commit_log_append(
                2,
                100,
                200,
                &[
                    shard_record(120, 2, 7, b"k", b"v"),
                    shard_record(130, 2, 8, b"k", b"v"),
                ],
                4 * 1024 * 1024,
                2,
                applied_for(2),
            )
            .unwrap();
        assert_eq!(
            storage.commit_log_shipped_through(7).unwrap(),
            150,
            "watermark never goes backwards"
        );
        assert_eq!(storage.commit_log_shipped_through(8).unwrap(), 130);
        assert_eq!(storage.commit_log_shipped_through(9).unwrap(), 0);
    }

    // ──── CommitRecord 写入（#153）────

    fn logged_decision(
        commit_ts: Revision,
        region_id: u64,
        txn_id: &[u8],
        mutations: Vec<crate::raft::type_config::SequencerMutation>,
    ) -> crate::raft::type_config::CommitDecisionApplyLog {
        crate::raft::type_config::CommitDecisionApplyLog {
            shard_id: crate::storage::commit_log::COMMIT_LOG_STAGING_SHARD_ID,
            region_id,
            commit_ts,
            txn_id: txn_id.to_vec(),
            start_ts: commit_ts.saturating_sub(1),
            mutations,
        }
    }

    #[test]
    fn test_apply_decision_with_log_writes_full_record_and_prev() {
        use crate::security::key_management::Keyring;
        use std::sync::Arc;

        let (_dir, storage) = create_commit_ts_storage();
        let (keyring, _dek) = Keyring::bootstrap_from_root_key(&[0xABu8; 32]).unwrap();
        let barrier = Barrier::new(Arc::new(keyring));
        storage.set_barrier(barrier.clone());

        storage
            .put_at_commit_ts(b"a", b"old", None, 5, applied_for(1))
            .unwrap();
        storage
            .put_at_commit_ts(b"d", b"dv", None, 6, applied_for(2))
            .unwrap();
        storage
            .put_at_commit_ts(b"e", b"ev", None, 7, applied_for(3))
            .unwrap();

        let entry = logged_decision(
            10,
            7,
            b"txn-10",
            vec![put(b"c", b"cv"), delete(b"a"), range_delete(b"d", b"f")],
        );
        let result = storage
            .apply_commit_decisions_with_log(&[entry], 4, applied_for(4), 4 * 1024 * 1024)
            .unwrap();
        assert_eq!(result.through_ts, 10);
        assert_eq!(result.decisions, 1);

        let records = storage.commit_records_at(1, 10).unwrap();
        assert_eq!(records.len(), 1, "one fragment per region");
        let record = &records[0];
        assert_eq!(record.region_id, 7);
        assert_eq!(record.txn_id, b"txn-10");
        assert_eq!(record.start_ts, 9);
        let keys: Vec<&[u8]> = record.changes.iter().map(|c| c.key.as_slice()).collect();
        assert_eq!(keys, vec![b"c".as_slice(), b"a", b"d", b"e"]);
        let put_value = record.changes[0]
            .value
            .as_deref()
            .expect("put carries a value");
        assert_ne!(
            put_value, b"cv",
            "the record stores the same ciphertext as MVCC"
        );
        assert_eq!(barrier.decrypt(put_value).unwrap(), b"cv");
        assert!(record.changes[0].prev.is_none());
        assert!(record.changes[1].value.is_none(), "delete is a tombstone");
        assert_eq!(
            record.changes[1].prev.as_ref().unwrap().mod_revision,
            5,
            "prev carries the pre-image metadata"
        );
        assert_eq!(
            barrier
                .decrypt(&record.changes[1].prev.as_ref().unwrap().value)
                .unwrap(),
            b"old"
        );
        assert_eq!(record.changes[2].prev.as_ref().unwrap().mod_revision, 6);
        assert_eq!(record.changes[3].prev.as_ref().unwrap().mod_revision, 7);

        let rows = commit_log_rows(&storage);
        assert_eq!(rows.len(), 1);
        for plain in [b"cv".as_slice(), b"old", b"dv", b"ev"] {
            assert!(
                !rows[0].1.windows(plain.len()).any(|window| window == plain),
                "commit log row must not contain plaintext {:?}",
                String::from_utf8_lossy(plain)
            );
        }
        assert!(storage.get(b"a").unwrap().is_none());
        assert_eq!(storage.get(b"c").unwrap(), Some(b"cv".to_vec()));
        assert!(storage.get(b"d").unwrap().is_none());
        assert!(storage.get(b"e").unwrap().is_none());
    }

    #[test]
    fn test_apply_decision_with_log_replay_writes_record_once() {
        let (_dir, storage) = create_commit_ts_storage();
        let entry = logged_decision(10, 1, b"txn-10", vec![put(b"k", b"v")]);
        storage
            .apply_commit_decisions_with_log(
                std::slice::from_ref(&entry),
                1,
                applied_for(1),
                4 * 1024 * 1024,
            )
            .unwrap();
        storage
            .apply_commit_decisions_with_log(
                std::slice::from_ref(&entry),
                2,
                applied_for(2),
                4 * 1024 * 1024,
            )
            .unwrap();

        assert_eq!(commit_log_rows(&storage).len(), 1);
        assert_eq!(storage.commit_records_at(1, 10).unwrap().len(), 1);
        assert_eq!(storage.get_kv_metadata(b"k").unwrap().unwrap().version, 1);
    }

    #[test]
    fn test_apply_decision_with_log_skips_empty_fragment() {
        let (_dir, storage) = create_commit_ts_storage();
        let result = storage
            .apply_commit_decisions_with_log(
                &[logged_decision(11, 7, b"txn-11", Vec::new())],
                1,
                applied_for(1),
                4 * 1024 * 1024,
            )
            .unwrap();

        assert_eq!(result.through_ts, 11);
        assert!(storage.commit_records_after(1, 0, 10).unwrap().is_empty());
        assert_eq!(storage.last_applied_decision_ts().unwrap(), 11);
    }

    #[test]
    fn test_apply_decision_with_log_oversize_record_keeps_raft_alive() {
        let (_dir, storage) = create_commit_ts_storage();
        let value = vec![7u8; 2048];
        let result = storage
            .apply_commit_decisions_with_log(
                &[logged_decision(10, 7, b"txn-10", vec![put(b"z", &value)])],
                1,
                applied_for(1),
                1,
            )
            .unwrap();

        assert_eq!(result.through_ts, 10);
        assert_eq!(storage.get(b"z").unwrap(), Some(value));
        assert!(
            storage.commit_records_at(1, 10).unwrap().is_empty(),
            "an unencodable record must not block data apply"
        );
    }

    #[test]
    fn test_apply_commit_decisions_plain_path_writes_no_records() {
        let (_dir, storage) = create_commit_ts_storage();
        storage
            .apply_commit_decisions(&[decision(10, vec![put(b"k", b"v")])], 1, applied_for(1))
            .unwrap();
        assert!(
            commit_log_rows(&storage).is_empty(),
            "the legacy command path must not write commit records"
        );
    }
}
