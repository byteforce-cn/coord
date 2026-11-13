//! MQ v2 消费组状态机（ADR-0016 D2/D6/D13，M1-3）。
//!
//! 语义单一归属：`docs/adr/0016-mq-v2.md`；工作项与验收见 MQ v2 设计文档 §6/M1-3
//! （`docs/superpowers/specs/` 下的 mq-v2-design）。
//!
//! 每个消费组分片是独立 Region 的 `MvccStorage`；本模块只做**确定性 apply**：
//! 时间由命令携带的 `now_ms` 决定，新成员 id 由 apply 内的 `next_member_seq`
//! 生成，订阅的 topic_id/分区数随命令携带——重放节点不读本地配置/时钟/随机数。
//!
//! 存储布局（组行/位点行落在 `mq_meta` 表：与段日志同表但分属不同 Region 的
//! storage，key 用独立标签，快照/备份走既有 `mq_meta_entries` 通道）：
//!
//! | key | value |
//! |:--|:--|
//! | `'S' ++ shard_id(8)` | 分片 apply 水位 + 最近一次结果（重复 apply 幂等） |
//! | `'G' ++ group` | `MqGroupState`（代际 + 成员/订阅/分配） |
//! | `'O' ++ len(group)(4) ++ group ++ topic_id(8) ++ partition(4)` | `MqGroupOffset` |
//!
//! 组名按长度前缀编码，`'G'`/`'O'` 的扫描不会因为组名前缀互相吞并。位点单独成行，
//! 使 `Ack` 不必重写整张组表（组 shard 是 acks 的热路径，D6）。

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use coord_core::error::{Error, Result};
use coord_core::storage::{ReadTx, StorageBackend, WriteTx};

use crate::storage::envelope;
use crate::storage::mq_log::TABLE_MQ_META;
use crate::storage::mvcc::{AppliedLogId, MvccStorage, META_LAST_APPLIED, TABLE_META};

/// 组名上限（与 topic 名同口径；契约未另立字节规则）。
const GROUP_NAME_MAX: usize = 255;

/// 元数据 key 的命名空间标签（与 `mq_log` 的 `S`/`P`/`T` 不冲突）。
const META_GROUP: u8 = b'G';
const META_GROUP_OFFSET: u8 = b'O';
const META_SHARD: u8 = b'S';

// ──── 组状态（持久化）────

/// 一个订阅项：topic 名字 + 身份 + 分区数（join 时的权威值，随命令携带）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqGroupTopic {
    pub topic: String,
    pub topic_id: u64,
    pub partitions: u32,
}

/// 一个成员在一个代际内持有的分区。
///
/// `topic_id` 是位点身份：同名 topic 删除重建后 id 前进，旧位点不会被新 topic
/// 复用（ADR-0016 D10）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqGroupAssignment {
    pub topic: String,
    pub topic_id: u64,
    pub partition: u32,
}

/// 组成员：订阅、会话超时、最近心跳与当前代际的分配。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqGroupMember {
    pub member_id: String,
    /// 按 topic 名排序（apply 内 canonical，便于比较订阅是否变化）。
    pub topics: Vec<MqGroupTopic>,
    pub session_timeout_ms: u32,
    pub last_heartbeat_ms: i64,
    /// 按 (topic 名, partition) 升序。
    pub assignments: Vec<MqGroupAssignment>,
}

/// 一个组的权威状态（一个组只属于一个 group shard）。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqGroupState {
    /// 每次成员变化/会话过期前进；Ack/Heartbeat 必须匹配。
    pub generation: u64,
    /// 自动生成 `member_id` 的单调序号（由 apply 分配，重放一致）。
    pub next_member_seq: u64,
    /// 按 `member_id` 升序。
    pub members: Vec<MqGroupMember>,
}

impl MqGroupState {
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    pub fn member(&self, member_id: &str) -> Option<&MqGroupMember> {
        self.members
            .iter()
            .find(|member| member.member_id == member_id)
    }
}

/// 已提交位点（`committed_offset` = **下一条待消费**）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqGroupOffset {
    pub topic_id: u64,
    pub partition: u32,
    pub committed_offset: u64,
    pub updated_at_ms: i64,
}

// ──── 命令载荷（写进 raft 日志；重放时不得依赖本地配置/时钟）────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqGroupJoin {
    pub shard_id: u32,
    pub group: String,
    /// 空 = 新成员（由 apply 生成 id）；非空但不认识 = 以该 id 重新加入。
    pub member_id: String,
    pub topics: Vec<MqGroupTopic>,
    /// 0 由 handler 展开成 `[mq_v2].group_session_timeout_ms`；apply 拒绝 0
    /// （重放节点不读本地配置）。
    pub session_timeout_ms: u32,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqGroupHeartbeat {
    pub shard_id: u32,
    pub group: String,
    pub member_id: String,
    pub generation: u64,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqGroupLeave {
    pub shard_id: u32,
    pub group: String,
    pub member_id: String,
    pub generation: u64,
    pub now_ms: i64,
}

/// 分片级过期扫描（Leader 周期提案；apply 只按命令携带的时间判断）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqGroupReap {
    pub shard_id: u32,
    pub now_ms: i64,
}

/// 空组 + 位点过 TTL 的回收（Leader 周期提案；apply 只按命令携带的时间判断）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqGroupGc {
    pub shard_id: u32,
    pub now_ms: i64,
    /// 位点 `updated_at_ms` 早于 `now_ms - offset_ttl_ms` 才算过期。
    pub offset_ttl_ms: u64,
}

/// 批量提交位点（`Ack` = 一条）。契约要求整批校验、不部分提交。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqGroupAck {
    pub shard_id: u32,
    pub group: String,
    pub member_id: String,
    pub generation: u64,
    pub entries: Vec<MqGroupAckEntry>,
    pub now_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqGroupAckEntry {
    pub topic_id: u64,
    pub partition: u32,
    /// 已处理完的最后一条 offset（含）；committed 推进到 `offset + 1`。
    pub offset: u64,
}

/// 仅消费组分片 raft 提出的组命令。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MqGroupOp {
    Join(MqGroupJoin),
    Heartbeat(MqGroupHeartbeat),
    Leave(MqGroupLeave),
    Reap(MqGroupReap),
    Ack(MqGroupAck),
    /// 末尾追加（postcard 兼容既有日志）：空组 + 过 TTL 位点的回收。
    Gc(MqGroupGc),
}

impl MqGroupOp {
    pub fn shard_id(&self) -> u32 {
        match self {
            Self::Join(op) => op.shard_id,
            Self::Heartbeat(op) => op.shard_id,
            Self::Leave(op) => op.shard_id,
            Self::Reap(op) => op.shard_id,
            Self::Ack(op) => op.shard_id,
            Self::Gc(op) => op.shard_id,
        }
    }
}

// ──── apply 结果 ────

/// 加入结果：该成员在当前代际的分配 + 每条分配的已提交位点。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqGroupJoined {
    pub member_id: String,
    pub generation: u64,
    pub assignments: Vec<MqGroupAssignmentView>,
    pub session_timeout_ms: u32,
    pub heartbeat_interval_ms: u32,
    /// true = 本次加入推进了代际并重算分配；false = 同订阅续约。
    pub rebalanced: bool,
}

/// 对外分配视图（含已提交位点；无记录时 0）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqGroupAssignmentView {
    pub topic: String,
    pub topic_id: u64,
    pub partition: u32,
    pub committed_offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqGroupHeartbeatReply {
    pub generation: u64,
    /// v2.0 在 apply 内直接重算分配，成员不需要等 SyncGroup；保留字段给契约。
    pub rebalance_required: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqGroupLeft {
    pub generation: u64,
    /// false = 重复 Leave（成员已不在组里），幂等成功。
    pub removed: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqGroupReaped {
    pub groups_scanned: u32,
    pub members_expired: u32,
    pub groups_changed: u32,
}

/// 组 TTL 回收的产出。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqGroupGcResult {
    pub groups_removed: u32,
    pub offsets_removed: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqGroupAcked {
    pub results: Vec<MqGroupAckResult>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqGroupAckResult {
    pub topic_id: u64,
    pub partition: u32,
    pub committed_offset: u64,
}

/// 用户级拒绝（不失败 apply；handler 映射到契约错误码）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MqGroupRejection {
    /// 组不存在（未 Join 过或已回收）→ `GROUP_NOT_FOUND`。
    GroupNotFound,
    /// 代际已过期 → `GROUP_EPOCH_STALE`。
    StaleGeneration {
        current_generation: u64,
    },
    /// member_id 不在该组，或该分区未分配给该成员 → `NOT_GROUP_MEMBER`。
    NotMember {
        current_generation: u64,
    },
    NotAssigned {
        topic_id: u64,
        partition: u32,
    },
    /// 非法参数（handler 映射 `INVALID_ARGUMENT`）。
    Invalid {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MqGroupResult {
    Joined(MqGroupJoined),
    Heartbeat(MqGroupHeartbeatReply),
    Left(MqGroupLeft),
    Reaped(MqGroupReaped),
    Acked(MqGroupAcked),
    Rejected(MqGroupRejection),
    /// 末尾追加：组 TTL 回收（与 `MqGroupOp::Gc` 对应）。
    Gced(MqGroupGcResult),
}

/// 分片级 apply 水位 + 最近一次结果（同一 Raft 条目重复 apply 返回首次结果）。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct MqGroupShardMeta {
    applied_index: u64,
    last_result: Option<MqGroupResult>,
}

// ──── key 编码 ────

fn shard_meta_key(shard_id: u32) -> [u8; 9] {
    let mut key = [0u8; 9];
    key[0] = META_SHARD;
    key[1..].copy_from_slice(&u64::from(shard_id).to_be_bytes());
    key
}

fn group_key(group: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + group.len());
    key.push(META_GROUP);
    key.extend_from_slice(group.as_bytes());
    key
}

fn offset_prefix(group: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 4 + group.len());
    key.push(META_GROUP_OFFSET);
    key.extend_from_slice(&(group.len() as u32).to_be_bytes());
    key.extend_from_slice(group.as_bytes());
    key
}

fn offset_key(group: &str, topic_id: u64, partition: u32) -> Vec<u8> {
    let mut key = offset_prefix(group);
    key.extend_from_slice(&topic_id.to_be_bytes());
    key.extend_from_slice(&partition.to_be_bytes());
    key
}

// ──── 编解码 ────

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    envelope::encode(value).map_err(|err| Error::Storage(format!("encode mq group row: {err}")))
}

fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T> {
    envelope::decode(bytes).map_err(|err| Error::Storage(format!("decode mq group row: {err}")))
}

fn read_meta<T: for<'de> Deserialize<'de>>(tx: &dyn ReadTx, key: &[u8]) -> Result<Option<T>> {
    tx.get(TABLE_MQ_META, key)?
        .map(|bytes| decode(&bytes))
        .transpose()
}

fn write_meta<T: Serialize>(tx: &mut dyn WriteTx, key: &[u8], value: &T) -> Result<()> {
    let bytes = encode(value)?;
    tx.insert(TABLE_MQ_META, key, &bytes)
}

fn read_group(tx: &dyn ReadTx, group: &str) -> Result<Option<MqGroupState>> {
    read_meta(tx, &group_key(group))
}

fn write_group(tx: &mut dyn WriteTx, group: &str, state: &MqGroupState) -> Result<()> {
    write_meta(tx, &group_key(group), state)
}

fn read_offset(
    tx: &dyn ReadTx,
    group: &str,
    topic_id: u64,
    partition: u32,
) -> Result<Option<MqGroupOffset>> {
    read_meta(tx, &offset_key(group, topic_id, partition))
}

fn rejected(reason: MqGroupRejection) -> MqGroupResult {
    MqGroupResult::Rejected(reason)
}

// ──── MvccStorage 上的 apply / 读取入口 ────

impl<B: StorageBackend> MvccStorage<B> {
    /// 应用一条组命令（消费组分片 raft）。
    ///
    /// 用户级拒绝只体现在 outcome 里；成功路径在同一 redb 事务里写组行/位点行、
    /// 分片水位与 `META_LAST_APPLIED`（崩溃后要么全部可见，要么全部不可见）。
    pub fn apply_mq_group_op(
        &self,
        op: &MqGroupOp,
        applied: AppliedLogId,
    ) -> Result<MqGroupResult> {
        let shard_key = shard_meta_key(op.shard_id());
        self.backend().write(|tx| {
            let mut shard = read_meta::<MqGroupShardMeta>(tx, &shard_key)?.unwrap_or_default();
            if applied.index <= shard.applied_index {
                // 同一 Raft 条目的重复 apply：返回首次结果，不重复加成员/推进代际。
                return Ok(shard.last_result.unwrap_or_else(|| {
                    rejected(MqGroupRejection::Invalid {
                        reason: "replayed group command has no cached result".to_string(),
                    })
                }));
            }

            let result = apply_group_op(tx, op)?;
            shard.applied_index = applied.index;
            shard.last_result = Some(result.clone());
            write_meta(tx, &shard_key, &shard)?;
            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;
            Ok(result)
        })
    }

    /// 读一个组的权威状态；不存在 = `None`。
    pub fn mq_group(&self, group: &str) -> Result<Option<MqGroupState>> {
        self.backend().read(|tx| read_group(tx, group))
    }

    /// 读一个组的全部已提交位点（按 key 升序 = topic_id/partition 升序）。
    pub fn mq_group_offsets(&self, group: &str) -> Result<Vec<MqGroupOffset>> {
        self.backend().read(|tx| {
            let rows = tx.iter_prefix(TABLE_MQ_META, &offset_prefix(group))?;
            let mut offsets = Vec::with_capacity(rows.len());
            for (_, value) in rows {
                offsets.push(decode::<MqGroupOffset>(&value)?);
            }
            Ok(offsets)
        })
    }

    /// 列出本分片的组名（可选前缀；`ListGroups` 用）。
    pub fn mq_group_names(&self, prefix: &str) -> Result<Vec<String>> {
        self.backend().read(|tx| {
            let rows = tx.iter_prefix(TABLE_MQ_META, &[META_GROUP])?;
            let mut names = Vec::with_capacity(rows.len());
            for (key, _) in rows {
                let Some(name) = key.strip_prefix(&[META_GROUP]) else {
                    continue;
                };
                let Ok(name) = std::str::from_utf8(name) else {
                    continue;
                };
                if name.starts_with(prefix) {
                    names.push(name.to_string());
                }
            }
            Ok(names)
        })
    }
}

fn apply_group_op(tx: &mut dyn WriteTx, op: &MqGroupOp) -> Result<MqGroupResult> {
    match op {
        MqGroupOp::Join(join) => apply_join(tx, join),
        MqGroupOp::Heartbeat(heartbeat) => apply_heartbeat(tx, heartbeat),
        MqGroupOp::Leave(leave) => apply_leave(tx, leave),
        MqGroupOp::Reap(reap) => apply_reap(tx, reap),
        MqGroupOp::Ack(ack) => apply_ack(tx, ack),
        MqGroupOp::Gc(gc) => apply_gc(tx, gc),
    }
}

/// 回收空组：成员为空且位点全部过 TTL（没有位点 ⇒ 立即回收）。
///
/// 删组行与位点行在同一条 Raft 条目的 apply 事务里，重放幂等（第二次扫不到）。
fn apply_gc(tx: &mut dyn WriteTx, op: &MqGroupGc) -> Result<MqGroupResult> {
    let rows = tx.iter_prefix(TABLE_MQ_META, &[META_GROUP])?;
    let mut summary = MqGroupGcResult::default();
    let ttl_ms = i64::try_from(op.offset_ttl_ms).unwrap_or(i64::MAX);
    let deadline = op.now_ms.saturating_sub(ttl_ms);
    for (key, value) in rows {
        let state = decode::<MqGroupState>(&value)?;
        if !state.members.is_empty() {
            continue;
        }
        let Some(name) = key
            .strip_prefix(&[META_GROUP])
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
        else {
            continue;
        };
        let offsets = tx.iter_prefix(TABLE_MQ_META, &offset_prefix(name))?;
        let mut offset_rows = Vec::with_capacity(offsets.len());
        let mut all_expired = true;
        for (offset_key_bytes, offset_value) in offsets {
            let offset = decode::<MqGroupOffset>(&offset_value)?;
            if offset.updated_at_ms >= deadline {
                all_expired = false;
            }
            offset_rows.push((offset_key_bytes, offset));
        }
        if !all_expired {
            continue;
        }
        for (offset_key_bytes, _) in &offset_rows {
            tx.remove(TABLE_MQ_META, offset_key_bytes)?;
        }
        tx.remove(TABLE_MQ_META, &key)?;
        summary.groups_removed = summary.groups_removed.saturating_add(1);
        summary.offsets_removed = summary
            .offsets_removed
            .saturating_add(u32::try_from(offset_rows.len()).unwrap_or(u32::MAX));
    }
    Ok(MqGroupResult::Gced(summary))
}

/// 校验并把订阅 canonical 化（按 topic 名排序）；非法输入 = 用户级拒绝。
fn validate_join(join: &MqGroupJoin) -> std::result::Result<Vec<MqGroupTopic>, String> {
    if join.group.is_empty() || join.group.len() > GROUP_NAME_MAX {
        return Err(format!(
            "group name must be 1..={GROUP_NAME_MAX} bytes (got {})",
            join.group.len()
        ));
    }
    if join.session_timeout_ms == 0 {
        return Err("session_timeout_ms must be > 0 (handler expands the default)".to_string());
    }
    if join.topics.is_empty() {
        return Err("group must subscribe to at least one topic".to_string());
    }
    let mut topics = join.topics.clone();
    topics.sort_by(|left, right| left.topic.cmp(&right.topic));
    for pair in topics.windows(2) {
        if pair[0].topic == pair[1].topic {
            return Err(format!("topic {} is subscribed twice", pair[0].topic));
        }
    }
    for topic in &topics {
        if topic.topic.is_empty() || topic.topic.len() > GROUP_NAME_MAX {
            return Err(format!(
                "topic name must be 1..={GROUP_NAME_MAX} bytes (got {})",
                topic.topic.len()
            ));
        }
        if topic.partitions == 0 {
            return Err(format!("topic {} has zero partitions", topic.topic));
        }
    }
    Ok(topics)
}

fn apply_join(tx: &mut dyn WriteTx, join: &MqGroupJoin) -> Result<MqGroupResult> {
    let topics = match validate_join(join) {
        Ok(topics) => topics,
        Err(reason) => return Ok(rejected(MqGroupRejection::Invalid { reason })),
    };

    let mut state = read_group(tx, &join.group)?.unwrap_or_default();
    let member_id = if join.member_id.is_empty() {
        let Some((seq, member_id)) = next_member_id(&state) else {
            return Ok(rejected(MqGroupRejection::Invalid {
                reason: "no member id available in this group".to_string(),
            }));
        };
        state.next_member_seq = seq;
        member_id
    } else {
        join.member_id.clone()
    };

    let existing = state.member(&member_id).map(|member| member.topics.clone());
    let rebalanced = match existing {
        Some(current) => {
            let index = state
                .members
                .iter()
                .position(|member| member.member_id == member_id)
                .ok_or_else(|| {
                    Error::Storage("mq group member vanished during join".to_string())
                })?;
            let member = &mut state.members[index];
            member.topics = topics;
            member.session_timeout_ms = join.session_timeout_ms;
            member.last_heartbeat_ms = join.now_ms;
            if current == member.topics {
                false
            } else {
                reassign(&mut state);
                true
            }
        }
        None => {
            state.members.push(MqGroupMember {
                member_id: member_id.clone(),
                topics,
                session_timeout_ms: join.session_timeout_ms,
                last_heartbeat_ms: join.now_ms,
                assignments: Vec::new(),
            });
            reassign(&mut state);
            true
        }
    };

    write_group(tx, &join.group, &state)?;
    let generation = state.generation;
    let member = state
        .member(&member_id)
        .ok_or_else(|| Error::Storage("mq group member missing after join".to_string()))?;
    let mut assignments = Vec::with_capacity(member.assignments.len());
    for assignment in &member.assignments {
        let committed_offset =
            read_offset(tx, &join.group, assignment.topic_id, assignment.partition)?
                .map(|offset| offset.committed_offset)
                .unwrap_or(0);
        assignments.push(MqGroupAssignmentView {
            topic: assignment.topic.clone(),
            topic_id: assignment.topic_id,
            partition: assignment.partition,
            committed_offset,
        });
    }

    Ok(MqGroupResult::Joined(MqGroupJoined {
        member_id,
        generation,
        assignments,
        session_timeout_ms: join.session_timeout_ms,
        heartbeat_interval_ms: heartbeat_interval_ms(join.session_timeout_ms),
        rebalanced,
    }))
}

/// 生成下一个自动 `member_id`；空间耗尽 = `None`（不会无限循环）。
fn next_member_id(state: &MqGroupState) -> Option<(u64, String)> {
    let mut seq = state.next_member_seq;
    for _ in 0..=state.members.len() {
        seq = seq.saturating_add(1);
        let candidate = format!("m{seq:016x}");
        if state.member(&candidate).is_none() {
            return Some((seq, candidate));
        }
    }
    None
}

fn heartbeat_interval_ms(session_timeout_ms: u32) -> u32 {
    (session_timeout_ms / 3).max(1)
}

/// 成员变化/会话过期：代际前进并整体重算分配（range 策略）。
///
/// 订阅按 topic 名归并（同名 topic 删除重建后 id 前进，取最大 id 作为位点身份；
/// 分区数取订阅里的最大值，避免更新期间少分配）。成员按 `member_id` 排序后
/// 轮次 range：`per = ceil(partitions / subscribers)`。
fn reassign(state: &mut MqGroupState) {
    state.generation = state.generation.saturating_add(1);
    state
        .members
        .sort_by(|left, right| left.member_id.cmp(&right.member_id));
    for member in &mut state.members {
        member.assignments.clear();
    }

    let mut topics: BTreeMap<String, (u64, u32)> = BTreeMap::new();
    for member in &state.members {
        for topic in &member.topics {
            let entry = topics
                .entry(topic.topic.clone())
                .or_insert((topic.topic_id, topic.partitions));
            entry.0 = entry.0.max(topic.topic_id);
            entry.1 = entry.1.max(topic.partitions);
        }
    }

    for (name, topic_id, partitions) in topics
        .into_iter()
        .map(|(name, (topic_id, partitions))| (name, topic_id, partitions))
    {
        let subscribers: Vec<usize> = state
            .members
            .iter()
            .enumerate()
            .filter(|(_, member)| member.topics.iter().any(|topic| topic.topic == name))
            .map(|(index, _)| index)
            .collect();
        if subscribers.is_empty() || partitions == 0 {
            continue;
        }
        let total = partitions as usize;
        let per = total.div_ceil(subscribers.len());
        for (rank, index) in subscribers.iter().enumerate() {
            let start = rank.saturating_mul(per);
            if start >= total {
                break;
            }
            let end = start.saturating_add(per).min(total);
            for partition in start..end {
                state.members[*index].assignments.push(MqGroupAssignment {
                    topic: name.clone(),
                    topic_id,
                    partition: partition as u32,
                });
            }
        }
    }
}

fn apply_heartbeat(tx: &mut dyn WriteTx, op: &MqGroupHeartbeat) -> Result<MqGroupResult> {
    let Some(mut state) = read_group(tx, &op.group)? else {
        return Ok(rejected(MqGroupRejection::GroupNotFound));
    };
    if op.generation != state.generation {
        return Ok(rejected(MqGroupRejection::StaleGeneration {
            current_generation: state.generation,
        }));
    }
    let Some(index) = state
        .members
        .iter()
        .position(|member| member.member_id == op.member_id)
    else {
        return Ok(rejected(MqGroupRejection::NotMember {
            current_generation: state.generation,
        }));
    };
    state.members[index].last_heartbeat_ms = op.now_ms;
    let generation = state.generation;
    write_group(tx, &op.group, &state)?;
    Ok(MqGroupResult::Heartbeat(MqGroupHeartbeatReply {
        generation,
        rebalance_required: false,
    }))
}

fn apply_leave(tx: &mut dyn WriteTx, op: &MqGroupLeave) -> Result<MqGroupResult> {
    let Some(mut state) = read_group(tx, &op.group)? else {
        return Ok(rejected(MqGroupRejection::GroupNotFound));
    };
    let Some(index) = state
        .members
        .iter()
        .position(|member| member.member_id == op.member_id)
    else {
        // 重复 Leave / 已被 reaper 回收：幂等成功，不推进代际。
        return Ok(MqGroupResult::Left(MqGroupLeft {
            generation: state.generation,
            removed: false,
        }));
    };
    if op.generation != state.generation {
        return Ok(rejected(MqGroupRejection::StaleGeneration {
            current_generation: state.generation,
        }));
    }
    state.members.remove(index);
    reassign(&mut state);
    let generation = state.generation;
    write_group(tx, &op.group, &state)?;
    Ok(MqGroupResult::Left(MqGroupLeft {
        generation,
        removed: true,
    }))
}

fn apply_reap(tx: &mut dyn WriteTx, op: &MqGroupReap) -> Result<MqGroupResult> {
    let rows = tx.iter_prefix(TABLE_MQ_META, &[META_GROUP])?;
    let mut summary = MqGroupReaped::default();
    for (key, value) in rows {
        let mut state = decode::<MqGroupState>(&value)?;
        summary.groups_scanned = summary.groups_scanned.saturating_add(1);
        let before = state.members.len();
        state
            .members
            .retain(|member| !session_expired(member, op.now_ms));
        let expired = before.saturating_sub(state.members.len());
        if expired == 0 {
            continue;
        }
        summary.members_expired = summary
            .members_expired
            .saturating_add(u32::try_from(expired).unwrap_or(u32::MAX));
        summary.groups_changed = summary.groups_changed.saturating_add(1);
        reassign(&mut state);
        write_meta(tx, &key, &state)?;
    }
    Ok(MqGroupResult::Reaped(summary))
}

/// 会话过期：`now >= last_heartbeat + session_timeout`（时间由命令携带）。
fn session_expired(member: &MqGroupMember, now_ms: i64) -> bool {
    let deadline = member
        .last_heartbeat_ms
        .saturating_add(i64::from(member.session_timeout_ms));
    now_ms >= deadline
}

fn apply_ack(tx: &mut dyn WriteTx, op: &MqGroupAck) -> Result<MqGroupResult> {
    let Some(state) = read_group(tx, &op.group)? else {
        return Ok(rejected(MqGroupRejection::GroupNotFound));
    };
    if op.entries.is_empty() {
        return Ok(rejected(MqGroupRejection::Invalid {
            reason: "ack batch must contain at least one entry".to_string(),
        }));
    }
    if op.generation != state.generation {
        return Ok(rejected(MqGroupRejection::StaleGeneration {
            current_generation: state.generation,
        }));
    }
    let Some(member) = state.member(&op.member_id) else {
        return Ok(rejected(MqGroupRejection::NotMember {
            current_generation: state.generation,
        }));
    };

    // 整批先校验：任一未分配 ⇒ 整批拒绝，一条位点都不写（契约明文）。
    for entry in &op.entries {
        if !member.assignments.iter().any(|assignment| {
            assignment.topic_id == entry.topic_id && assignment.partition == entry.partition
        }) {
            return Ok(rejected(MqGroupRejection::NotAssigned {
                topic_id: entry.topic_id,
                partition: entry.partition,
            }));
        }
    }

    let mut results = Vec::with_capacity(op.entries.len());
    for entry in &op.entries {
        let existing = read_offset(tx, &op.group, entry.topic_id, entry.partition)?;
        let current = existing.map(|offset| offset.committed_offset).unwrap_or(0);
        // 重复/更旧 Ack 幂等成功：committed 只前进，不回退。
        let committed = current.max(entry.offset.saturating_add(1));
        if existing.is_none() || committed != current {
            let row = MqGroupOffset {
                topic_id: entry.topic_id,
                partition: entry.partition,
                committed_offset: committed,
                updated_at_ms: op.now_ms,
            };
            write_meta(
                tx,
                &offset_key(&op.group, entry.topic_id, entry.partition),
                &row,
            )?;
        }
        results.push(MqGroupAckResult {
            topic_id: entry.topic_id,
            partition: entry.partition,
            committed_offset: committed,
        });
    }
    Ok(MqGroupResult::Acked(MqGroupAcked { results }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::redb_backend::RedbBackend;
    use crate::storage::snapshot::{export_snapshot_data, import_snapshot_data, SnapshotData};
    use coord_core::types::StorageConfig;
    use tempfile::TempDir;

    fn setup() -> (TempDir, MvccStorage<RedbBackend>) {
        let dir = TempDir::new().unwrap();
        let storage = open(&dir);
        (dir, storage)
    }

    /// 打开（或重开）同一目录上的存储；重启测试复用。
    fn open(dir: &TempDir) -> MvccStorage<RedbBackend> {
        let backend = RedbBackend::open(dir.path(), &StorageConfig::default()).unwrap();
        MvccStorage::new(backend).unwrap()
    }

    fn applied(index: u64) -> AppliedLogId {
        AppliedLogId {
            term: 1,
            node_id: 1,
            index,
        }
    }

    fn topic(name: &str, topic_id: u64, partitions: u32) -> MqGroupTopic {
        MqGroupTopic {
            topic: name.to_string(),
            topic_id,
            partitions,
        }
    }

    fn join(
        group: &str,
        member_id: &str,
        topics: Vec<MqGroupTopic>,
        session_timeout_ms: u32,
        now_ms: i64,
    ) -> MqGroupJoin {
        MqGroupJoin {
            shard_id: 0,
            group: group.to_string(),
            member_id: member_id.to_string(),
            topics,
            session_timeout_ms,
            now_ms,
        }
    }

    fn apply(storage: &MvccStorage<RedbBackend>, op: MqGroupOp, index: u64) -> MqGroupResult {
        storage.apply_mq_group_op(&op, applied(index)).unwrap()
    }

    fn joined(result: MqGroupResult) -> MqGroupJoined {
        match result {
            MqGroupResult::Joined(joined) => joined,
            other => panic!("expected Joined, got {other:?}"),
        }
    }

    fn rejection(result: MqGroupResult) -> MqGroupRejection {
        match result {
            MqGroupResult::Rejected(rejection) => rejection,
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    fn partitions_of(joined: &MqGroupJoined, name: &str) -> Vec<u32> {
        joined
            .assignments
            .iter()
            .filter(|assignment| assignment.topic == name)
            .map(|assignment| assignment.partition)
            .collect()
    }

    fn ack(
        group: &str,
        member_id: &str,
        generation: u64,
        entries: Vec<MqGroupAckEntry>,
        now_ms: i64,
    ) -> MqGroupAck {
        MqGroupAck {
            shard_id: 0,
            group: group.to_string(),
            member_id: member_id.to_string(),
            generation,
            entries,
            now_ms,
        }
    }

    #[test]
    fn join_assigns_every_partition_to_exactly_one_member() {
        let (_dir, storage) = setup();
        let first = joined(apply(
            &storage,
            MqGroupOp::Join(join(
                "billing",
                "",
                vec![topic("orders", 1, 4)],
                30_000,
                1_000,
            )),
            1,
        ));
        assert_eq!(first.generation, 1);
        assert!(first.rebalanced);
        assert_eq!(partitions_of(&first, "orders"), vec![0, 1, 2, 3]);
        assert!(first
            .assignments
            .iter()
            .all(|assignment| assignment.committed_offset == 0));

        let second = joined(apply(
            &storage,
            MqGroupOp::Join(join(
                "billing",
                "",
                vec![topic("orders", 1, 4)],
                30_000,
                1_001,
            )),
            2,
        ));
        assert_eq!(second.generation, 2);
        assert_ne!(second.member_id, first.member_id);

        let state = storage.mq_group("billing").unwrap().unwrap();
        assert_eq!(state.generation, 2);
        assert_eq!(state.members.len(), 2);
        let mut seen = Vec::new();
        for member in &state.members {
            assert_eq!(member.assignments.len(), 2);
            for assignment in &member.assignments {
                assert!(
                    !seen.contains(&assignment.partition),
                    "partition {} assigned twice",
                    assignment.partition
                );
                seen.push(assignment.partition);
            }
        }
        seen.sort_unstable();
        assert_eq!(seen, vec![0, 1, 2, 3]);
        assert_eq!(partitions_of(&second, "orders"), vec![2, 3]);
    }

    #[test]
    fn rejoin_with_the_same_subscription_keeps_the_generation() {
        let (_dir, storage) = setup();
        let first = joined(apply(
            &storage,
            MqGroupOp::Join(join(
                "billing",
                "",
                vec![topic("orders", 1, 2)],
                30_000,
                1_000,
            )),
            1,
        ));
        let again = joined(apply(
            &storage,
            MqGroupOp::Join(join(
                "billing",
                &first.member_id,
                vec![topic("orders", 1, 2)],
                30_000,
                2_000,
            )),
            2,
        ));
        assert_eq!(again.generation, first.generation);
        assert!(!again.rebalanced);
        assert_eq!(again.member_id, first.member_id);
        assert_eq!(again.assignments, first.assignments);

        let state = storage.mq_group("billing").unwrap().unwrap();
        assert_eq!(state.members.len(), 1);
        assert_eq!(state.members[0].last_heartbeat_ms, 2_000);
    }

    #[test]
    fn subscription_change_bumps_the_generation_and_reassigns() {
        let (_dir, storage) = setup();
        let first = joined(apply(
            &storage,
            MqGroupOp::Join(join(
                "billing",
                "",
                vec![topic("orders", 1, 2)],
                30_000,
                1_000,
            )),
            1,
        ));
        let second = joined(apply(
            &storage,
            MqGroupOp::Join(join(
                "billing",
                "",
                vec![topic("orders", 1, 2)],
                30_000,
                1_001,
            )),
            2,
        ));
        assert_eq!(second.generation, 2);

        let changed = joined(apply(
            &storage,
            MqGroupOp::Join(join(
                "billing",
                &first.member_id,
                vec![topic("orders", 1, 2), topic("events", 2, 2)],
                30_000,
                1_002,
            )),
            3,
        ));
        assert_eq!(changed.generation, 3);
        assert!(changed.rebalanced);
        assert_eq!(partitions_of(&changed, "events"), vec![0, 1]);

        let state = storage.mq_group("billing").unwrap().unwrap();
        let mut events = Vec::new();
        for member in &state.members {
            for assignment in member.assignments.iter().filter(|a| a.topic == "events") {
                assert!(!events.contains(&assignment.partition));
                events.push(assignment.partition);
            }
        }
        events.sort_unstable();
        assert_eq!(events, vec![0, 1]);
    }

    #[test]
    fn join_without_member_id_generates_unique_member_ids() {
        let (_dir, storage) = setup();
        let first = joined(apply(
            &storage,
            MqGroupOp::Join(join(
                "billing",
                "",
                vec![topic("orders", 1, 2)],
                30_000,
                1_000,
            )),
            1,
        ));
        let second = joined(apply(
            &storage,
            MqGroupOp::Join(join(
                "billing",
                "",
                vec![topic("orders", 1, 2)],
                30_000,
                1_001,
            )),
            2,
        ));
        assert_ne!(first.member_id, second.member_id);
        assert_eq!(second.generation, 2);
        let state = storage.mq_group("billing").unwrap().unwrap();
        assert_eq!(state.members.len(), 2);
        assert_eq!(state.next_member_seq, 2);
    }

    #[test]
    fn heartbeat_rejects_stale_generation_and_unknown_member() {
        let (_dir, storage) = setup();
        let member = joined(apply(
            &storage,
            MqGroupOp::Join(join(
                "billing",
                "",
                vec![topic("orders", 1, 2)],
                30_000,
                1_000,
            )),
            1,
        ));
        let stale = rejection(apply(
            &storage,
            MqGroupOp::Heartbeat(MqGroupHeartbeat {
                shard_id: 0,
                group: "billing".to_string(),
                member_id: member.member_id.clone(),
                generation: 0,
                now_ms: 1_500,
            }),
            2,
        ));
        assert_eq!(
            stale,
            MqGroupRejection::StaleGeneration {
                current_generation: 1
            }
        );

        let unknown = rejection(apply(
            &storage,
            MqGroupOp::Heartbeat(MqGroupHeartbeat {
                shard_id: 0,
                group: "billing".to_string(),
                member_id: "nobody".to_string(),
                generation: 1,
                now_ms: 1_500,
            }),
            3,
        ));
        assert_eq!(
            unknown,
            MqGroupRejection::NotMember {
                current_generation: 1
            }
        );

        assert_eq!(
            rejection(apply(
                &storage,
                MqGroupOp::Heartbeat(MqGroupHeartbeat {
                    shard_id: 0,
                    group: "missing".to_string(),
                    member_id: member.member_id.clone(),
                    generation: 1,
                    now_ms: 1_500,
                }),
                4,
            )),
            MqGroupRejection::GroupNotFound
        );

        let reply = match apply(
            &storage,
            MqGroupOp::Heartbeat(MqGroupHeartbeat {
                shard_id: 0,
                group: "billing".to_string(),
                member_id: member.member_id.clone(),
                generation: 1,
                now_ms: 1_500,
            }),
            5,
        ) {
            MqGroupResult::Heartbeat(reply) => reply,
            other => panic!("expected Heartbeat, got {other:?}"),
        };
        assert_eq!(reply.generation, 1);
        assert!(!reply.rebalance_required);
        assert_eq!(
            storage
                .mq_group("billing")
                .unwrap()
                .unwrap()
                .member(&member.member_id)
                .unwrap()
                .last_heartbeat_ms,
            1_500
        );
    }

    #[test]
    fn ack_is_monotonic_and_rejects_unassigned_partitions() {
        let (_dir, storage) = setup();
        let member = joined(apply(
            &storage,
            MqGroupOp::Join(join(
                "billing",
                "",
                vec![topic("orders", 1, 2)],
                30_000,
                1_000,
            )),
            1,
        ));
        let member_id = member.member_id.clone();

        let acked = match apply(
            &storage,
            MqGroupOp::Ack(ack(
                "billing",
                &member_id,
                1,
                vec![MqGroupAckEntry {
                    topic_id: 1,
                    partition: 0,
                    offset: 3,
                }],
                2_000,
            )),
            2,
        ) {
            MqGroupResult::Acked(acked) => acked,
            other => panic!("expected Acked, got {other:?}"),
        };
        assert_eq!(acked.results[0].committed_offset, 4);

        // 更旧的 Ack 幂等成功，不回退。
        let older = match apply(
            &storage,
            MqGroupOp::Ack(ack(
                "billing",
                &member_id,
                1,
                vec![MqGroupAckEntry {
                    topic_id: 1,
                    partition: 0,
                    offset: 1,
                }],
                2_100,
            )),
            3,
        ) {
            MqGroupResult::Acked(acked) => acked,
            other => panic!("expected Acked, got {other:?}"),
        };
        assert_eq!(older.results[0].committed_offset, 4);
        let offsets = storage.mq_group_offsets("billing").unwrap();
        assert_eq!(offsets.len(), 1);
        assert_eq!(offsets[0].committed_offset, 4);
        assert_eq!(offsets[0].updated_at_ms, 2_000, "回退不刷新时间戳");

        // 未分配的分区 ⇒ NOT_GROUP_MEMBER 口径的拒绝。
        assert_eq!(
            rejection(apply(
                &storage,
                MqGroupOp::Ack(ack(
                    "billing",
                    &member_id,
                    1,
                    vec![MqGroupAckEntry {
                        topic_id: 1,
                        partition: 9,
                        offset: 1,
                    }],
                    2_200,
                )),
                4,
            )),
            MqGroupRejection::NotAssigned {
                topic_id: 1,
                partition: 9
            }
        );
        assert_eq!(
            rejection(apply(
                &storage,
                MqGroupOp::Ack(ack(
                    "billing",
                    &member_id,
                    0,
                    vec![MqGroupAckEntry {
                        topic_id: 1,
                        partition: 0,
                        offset: 9,
                    }],
                    2_300,
                )),
                5,
            )),
            MqGroupRejection::StaleGeneration {
                current_generation: 1
            }
        );
        assert_eq!(
            rejection(apply(
                &storage,
                MqGroupOp::Ack(ack("missing", &member_id, 1, vec![], 2_400)),
                6,
            )),
            MqGroupRejection::GroupNotFound
        );
        assert!(matches!(
            rejection(apply(
                &storage,
                MqGroupOp::Ack(ack("billing", "nobody", 1, vec![], 2_500)),
                7,
            )),
            MqGroupRejection::Invalid { .. }
        ));
    }

    #[test]
    fn ack_batch_rejects_without_partial_writes() {
        let (_dir, storage) = setup();
        let first = joined(apply(
            &storage,
            MqGroupOp::Join(join(
                "billing",
                "",
                vec![topic("orders", 1, 2)],
                30_000,
                1_000,
            )),
            1,
        ));
        let second = joined(apply(
            &storage,
            MqGroupOp::Join(join(
                "billing",
                "",
                vec![topic("orders", 1, 2)],
                30_000,
                1_001,
            )),
            2,
        ));
        assert_eq!(first.generation, 1);
        assert_eq!(second.generation, 2);

        let mut mine = second.assignments[0].partition;
        let theirs = first.assignments[0].partition;
        if mine == theirs {
            mine = second.assignments[1].partition;
        }
        // second 只持有部分分区；拿 first 的分区提交 ⇒ 整批拒绝。
        let rejected_result = rejection(apply(
            &storage,
            MqGroupOp::Ack(ack(
                "billing",
                &second.member_id,
                2,
                vec![
                    MqGroupAckEntry {
                        topic_id: 1,
                        partition: mine,
                        offset: 5,
                    },
                    MqGroupAckEntry {
                        topic_id: 1,
                        partition: theirs,
                        offset: 7,
                    },
                ],
                2_000,
            )),
            3,
        ));
        assert_eq!(
            rejected_result,
            MqGroupRejection::NotAssigned {
                topic_id: 1,
                partition: theirs
            }
        );
        assert!(
            storage.mq_group_offsets("billing").unwrap().is_empty(),
            "整批拒绝时不得写入任何位点"
        );
    }

    #[test]
    fn leave_is_idempotent_and_rebalances() {
        let (_dir, storage) = setup();
        let first = joined(apply(
            &storage,
            MqGroupOp::Join(join(
                "billing",
                "",
                vec![topic("orders", 1, 4)],
                30_000,
                1_000,
            )),
            1,
        ));
        let second = joined(apply(
            &storage,
            MqGroupOp::Join(join(
                "billing",
                "",
                vec![topic("orders", 1, 4)],
                30_000,
                1_001,
            )),
            2,
        ));
        let left = match apply(
            &storage,
            MqGroupOp::Leave(MqGroupLeave {
                shard_id: 0,
                group: "billing".to_string(),
                member_id: second.member_id.clone(),
                generation: 2,
                now_ms: 1_500,
            }),
            3,
        ) {
            MqGroupResult::Left(left) => left,
            other => panic!("expected Left, got {other:?}"),
        };
        assert!(left.removed);
        assert_eq!(left.generation, 3);

        let state = storage.mq_group("billing").unwrap().unwrap();
        assert_eq!(state.members.len(), 1);
        assert_eq!(state.members[0].member_id, first.member_id);
        assert_eq!(state.members[0].assignments.len(), 4);

        // 二次 Leave（成员已不在）：幂等成功，不推进代际。
        let again = match apply(
            &storage,
            MqGroupOp::Leave(MqGroupLeave {
                shard_id: 0,
                group: "billing".to_string(),
                member_id: second.member_id.clone(),
                generation: 2,
                now_ms: 1_600,
            }),
            4,
        ) {
            MqGroupResult::Left(left) => left,
            other => panic!("expected Left, got {other:?}"),
        };
        assert!(!again.removed);
        assert_eq!(again.generation, 3);
    }

    #[test]
    fn reap_expires_members_and_transfers_their_partitions() {
        let (_dir, storage) = setup();
        let first = joined(apply(
            &storage,
            MqGroupOp::Join(join(
                "billing",
                "",
                vec![topic("orders", 1, 4)],
                1_000,
                1_000,
            )),
            1,
        ));
        let second = joined(apply(
            &storage,
            MqGroupOp::Join(join(
                "billing",
                "",
                vec![topic("orders", 1, 4)],
                1_000,
                1_900,
            )),
            2,
        ));
        assert_eq!(second.generation, 2);

        let reaped = match apply(
            &storage,
            MqGroupOp::Reap(MqGroupReap {
                shard_id: 0,
                now_ms: 2_000,
            }),
            3,
        ) {
            MqGroupResult::Reaped(reaped) => reaped,
            other => panic!("expected Reaped, got {other:?}"),
        };
        assert_eq!(reaped.groups_scanned, 1);
        assert_eq!(reaped.members_expired, 1);
        assert_eq!(reaped.groups_changed, 1);

        let state = storage.mq_group("billing").unwrap().unwrap();
        assert_eq!(state.generation, 3);
        assert_eq!(state.members.len(), 1);
        assert_eq!(state.members[0].member_id, second.member_id);
        assert_eq!(state.members[0].assignments.len(), 4);
        assert!(state.member(&first.member_id).is_none());

        // 同一时间重复 reap：无成员过期，不再推进代际。
        let again = match apply(
            &storage,
            MqGroupOp::Reap(MqGroupReap {
                shard_id: 0,
                now_ms: 2_000,
            }),
            4,
        ) {
            MqGroupResult::Reaped(reaped) => reaped,
            other => panic!("expected Reaped, got {other:?}"),
        };
        assert_eq!(again.members_expired, 0);
        assert_eq!(again.groups_changed, 0);
        assert_eq!(storage.mq_group("billing").unwrap().unwrap().generation, 3);
    }

    #[test]
    fn replay_of_the_same_applied_index_does_not_duplicate_members() {
        let (_dir, storage) = setup();
        let op = MqGroupOp::Join(join(
            "billing",
            "",
            vec![topic("orders", 1, 2)],
            30_000,
            1_000,
        ));
        let first = joined(apply(&storage, op.clone(), 1));
        let replay = joined(apply(&storage, op, 1));
        assert_eq!(first, replay);
        let state = storage.mq_group("billing").unwrap().unwrap();
        assert_eq!(state.members.len(), 1);
        assert_eq!(state.generation, 1);
    }

    #[test]
    fn gc_removes_empty_groups_only_after_the_offset_ttl() {
        let (_dir, storage) = setup();
        // 组 A：join → ack → leave（空组，位点在 TTL 内 → 不回收）。
        let member_a = joined(apply(
            &storage,
            MqGroupOp::Join(join("gc-a", "", vec![topic("t", 1, 1)], 30_000, 1_000)),
            1,
        ));
        assert_eq!(
            apply(
                &storage,
                MqGroupOp::Ack(MqGroupAck {
                    shard_id: 0,
                    group: "gc-a".to_string(),
                    member_id: member_a.member_id.clone(),
                    generation: member_a.generation,
                    entries: vec![MqGroupAckEntry {
                        topic_id: 1,
                        partition: 0,
                        offset: 4,
                    }],
                    now_ms: 2_000,
                }),
                2,
            ),
            MqGroupResult::Acked(MqGroupAcked {
                results: vec![MqGroupAckResult {
                    topic_id: 1,
                    partition: 0,
                    committed_offset: 5,
                }],
            })
        );
        let left = apply(
            &storage,
            MqGroupOp::Leave(MqGroupLeave {
                shard_id: 0,
                group: "gc-a".to_string(),
                member_id: member_a.member_id.clone(),
                generation: member_a.generation,
                now_ms: 3_000,
            }),
            3,
        );
        assert!(matches!(left, MqGroupResult::Left(_)));

        // 组 B：只有成员（无位点）——leave 后立即回收（无位点可等）。
        let member_b = joined(apply(
            &storage,
            MqGroupOp::Join(join("gc-b", "", vec![topic("t", 1, 1)], 30_000, 1_000)),
            4,
        ));
        assert!(matches!(
            apply(
                &storage,
                MqGroupOp::Leave(MqGroupLeave {
                    shard_id: 0,
                    group: "gc-b".to_string(),
                    member_id: member_b.member_id,
                    generation: member_b.generation,
                    now_ms: 3_000,
                }),
                5,
            ),
            MqGroupResult::Left(_)
        ));

        // TTL 内：空组的位点还在 → 不回收。
        let kept = apply(
            &storage,
            MqGroupOp::Gc(MqGroupGc {
                shard_id: 0,
                now_ms: 10_000,
                offset_ttl_ms: 60_000,
            }),
            6,
        );
        assert_eq!(
            kept,
            MqGroupResult::Gced(MqGroupGcResult {
                groups_removed: 1,
                offsets_removed: 0,
            }),
            "no offsets ⇒ collected immediately; fresh offsets are kept"
        );
        assert!(storage.mq_group("gc-a").unwrap().is_some());
        assert_eq!(storage.mq_group_offsets("gc-a").unwrap().len(), 1);
        assert!(storage.mq_group("gc-b").unwrap().is_none());

        // TTL 外：空组与其位点一起回收。
        let collected = apply(
            &storage,
            MqGroupOp::Gc(MqGroupGc {
                shard_id: 0,
                now_ms: 100_000,
                offset_ttl_ms: 60_000,
            }),
            7,
        );
        assert_eq!(
            collected,
            MqGroupResult::Gced(MqGroupGcResult {
                groups_removed: 1,
                offsets_removed: 1,
            })
        );
        assert!(storage.mq_group("gc-a").unwrap().is_none());
        assert!(storage.mq_group_offsets("gc-a").unwrap().is_empty());

        // 重放同一条命令：幂等（没有可回收的组）。
        assert_eq!(
            storage
                .apply_mq_group_op(
                    &MqGroupOp::Gc(MqGroupGc {
                        shard_id: 0,
                        now_ms: 100_000,
                        offset_ttl_ms: 60_000,
                    }),
                    applied(7),
                )
                .unwrap(),
            collected
        );
    }

    #[test]
    fn group_state_and_offsets_survive_snapshot_roundtrip() {
        let (_dir, storage) = setup();
        let member = joined(apply(
            &storage,
            MqGroupOp::Join(join(
                "billing",
                "",
                vec![topic("orders", 1, 2)],
                30_000,
                1_000,
            )),
            1,
        ));
        apply(
            &storage,
            MqGroupOp::Ack(ack(
                "billing",
                &member.member_id,
                1,
                vec![MqGroupAckEntry {
                    topic_id: 1,
                    partition: 0,
                    offset: 4,
                }],
                1_500,
            )),
            2,
        );
        let state_before = storage.mq_group("billing").unwrap().unwrap();
        let offsets_before = storage.mq_group_offsets("billing").unwrap();

        let data = export_snapshot_data(&storage, 2, 1).unwrap();
        let restored = SnapshotData::from_bytes(&data.to_bytes().unwrap()).unwrap();
        let (_dir2, storage2) = setup();
        import_snapshot_data(&storage2, &restored).unwrap();

        assert_eq!(storage2.mq_group("billing").unwrap().unwrap(), state_before);
        assert_eq!(
            storage2.mq_group_offsets("billing").unwrap(),
            offsets_before
        );

        // 恢复后位点继续前进，不倒退、不复用。
        let acked = match apply(
            &storage2,
            MqGroupOp::Ack(ack(
                "billing",
                &member.member_id,
                1,
                vec![MqGroupAckEntry {
                    topic_id: 1,
                    partition: 0,
                    offset: 9,
                }],
                2_000,
            )),
            3,
        ) {
            MqGroupResult::Acked(acked) => acked,
            other => panic!("expected Acked, got {other:?}"),
        };
        assert_eq!(acked.results[0].committed_offset, 10);
        assert_eq!(storage2.mq_group_names("").unwrap(), vec!["billing"]);
    }

    #[test]
    fn group_state_and_offsets_survive_reopen() {
        let dir = TempDir::new().unwrap();
        let (member_id, state_before, offsets_before) = {
            let storage = open(&dir);
            let member = joined(apply(
                &storage,
                MqGroupOp::Join(join(
                    "billing",
                    "",
                    vec![topic("orders", 1, 2)],
                    30_000,
                    1_000,
                )),
                1,
            ));
            apply(
                &storage,
                MqGroupOp::Ack(ack(
                    "billing",
                    &member.member_id,
                    member.generation,
                    vec![MqGroupAckEntry {
                        topic_id: 1,
                        partition: 0,
                        offset: 4,
                    }],
                    1_500,
                )),
                2,
            );
            (
                member.member_id,
                storage.mq_group("billing").unwrap().unwrap(),
                storage.mq_group_offsets("billing").unwrap(),
            )
        };

        let storage = open(&dir);
        assert_eq!(storage.mq_group("billing").unwrap().unwrap(), state_before);
        assert_eq!(storage.mq_group_offsets("billing").unwrap(), offsets_before);

        // 重开后的下一条 Ack 继续推进位点，不回退/不复用。
        let acked = match apply(
            &storage,
            MqGroupOp::Ack(ack(
                "billing",
                &member_id,
                state_before.generation,
                vec![MqGroupAckEntry {
                    topic_id: 1,
                    partition: 0,
                    offset: 9,
                }],
                2_000,
            )),
            3,
        ) {
            MqGroupResult::Acked(acked) => acked,
            other => panic!("expected Acked, got {other:?}"),
        };
        assert_eq!(acked.results[0].committed_offset, 10);
    }

    #[test]
    fn group_state_is_deterministic_across_two_stores() {
        let (_dir_a, a) = setup();
        let (_dir_b, b) = setup();
        let ops = vec![
            MqGroupOp::Join(join(
                "billing",
                "",
                vec![topic("orders", 1, 4)],
                30_000,
                1_000,
            )),
            MqGroupOp::Join(join(
                "billing",
                "",
                vec![topic("orders", 1, 4)],
                30_000,
                1_001,
            )),
            MqGroupOp::Join(join(
                "billing",
                "",
                vec![topic("orders", 1, 4), topic("events", 2, 2)],
                30_000,
                1_002,
            )),
        ];
        let mut member = String::new();
        for (index, op) in ops.into_iter().enumerate() {
            let result_a = apply(&a, op.clone(), index as u64 + 1);
            let result_b = apply(&b, op, index as u64 + 1);
            assert_eq!(result_a, result_b);
            if let MqGroupResult::Joined(joined) = result_a {
                member = joined.member_id;
            }
        }
        assert_eq!(
            a.mq_group("billing").unwrap(),
            b.mq_group("billing").unwrap()
        );
        let ack_op = MqGroupOp::Ack(ack(
            "billing",
            &member,
            3,
            vec![
                MqGroupAckEntry {
                    topic_id: 1,
                    partition: 0,
                    offset: 2,
                },
                MqGroupAckEntry {
                    topic_id: 2,
                    partition: 1,
                    offset: 5,
                },
            ],
            2_000,
        ));
        assert_eq!(apply(&a, ack_op.clone(), 4), apply(&b, ack_op, 4));
        assert_eq!(
            a.mq_group_offsets("billing").unwrap(),
            b.mq_group_offsets("billing").unwrap()
        );
    }
}
