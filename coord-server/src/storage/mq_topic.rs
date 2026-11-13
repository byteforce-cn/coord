//! MQ v2 topic 元数据（ADR-0016 D2/D4/D7，M1-4）。
//!
//! 表是 region 0 的权威元数据（单键整表，随 `/_sys/*` 进快照）：topic 身份
//! （`topic_id` 永不复用）、代际 `epoch`、配置、partition → 日志分片映射与
//! 删除记录。所有变更只由 region 0 system raft 提出，apply 只使用命令携带的
//! `now_ms`/分片数，重放节点不读本地配置。
//!
//! 用户级拒绝（重名不同配置、代际过期、非法名字/分区数）降级为 outcome，
//! 不失败 apply（与 ADR-0015 D11.5 同口径）。

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use coord_core::error::{Error, Result};
use coord_core::storage::{ReadTx, StorageBackend};
use coord_core::types::Revision;

use crate::storage::mvcc::{AppliedLogId, MvccStorage, META_LAST_APPLIED, TABLE_KV, TABLE_META};

/// region 0 存整张 topic 表的单键。
pub const MQ_TOPIC_TABLE_KEY: &[u8] = b"/_sys/mq/topics";

/// topic 名字上限（契约：≤255 字节，`[A-Za-z0-9._-]`）。
const MQ_TOPIC_NAME_MAX: usize = 255;

/// topic 配置（逐字对应契约 `TopicConfig`；0 值由 handler 展开成默认值后再进命令）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqTopicConfig {
    pub retention_secs: u64,
    pub max_bytes: u64,
    pub max_message_bytes: u32,
    pub segment_bytes: u64,
    pub dlq_retention_secs: u64,
}

/// 一个 topic 的权威元数据。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqTopicMeta {
    pub topic_id: u64,
    pub name: String,
    /// 代际：Update/Delete 推进；数据面请求携带，用于拒绝旧客户端。
    pub epoch: u64,
    pub partitions: u32,
    pub config: MqTopicConfig,
    pub created_at_ms: i64,
    /// 创建幂等键（空 = 不去重）。
    pub request_id: String,
    /// partition → 日志分片 id（长度 = `partitions`）。
    pub shard_of_partition: Vec<u32>,
}

/// 删除进度（`DeleteTopic` 标记后立即 `TOPIC_NOT_FOUND`；回收异步推进）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MqTopicDeletionState {
    Pending,
    Done,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqTopicDeletion {
    pub topic_id: u64,
    pub name: String,
    pub delete_id: u64,
    pub deleted_at_ms: i64,
    pub state: MqTopicDeletionState,
    /// 删除后的代际（旧代际之后一律 `TOPIC_NOT_FOUND`）。
    pub deleted_epoch: u64,
    /// 分区总数（回收进度分母）。
    pub partitions_total: u32,
    /// 尚未确认回收的分区（M4-2 逐个推进）。
    pub pending_partitions: Vec<u32>,
    /// 删除时的 partition → 日志分片映射（topic 行已移除，回收仍需路由）。
    pub shard_of_partition: Vec<u32>,
    pub messages_removed: u64,
    pub bytes_reclaimed: u64,
}

/// 整张 topic 表。
///
/// `topics` 按 name 升序、`deletions` 按 topic_id 升序；`name_epochs` 记录
/// 已删除名字的代际水位，保证同名校删重建后代际继续前进（不回到 0）。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqTopicTable {
    pub next_topic_id: u64,
    pub topics: Vec<MqTopicMeta>,
    pub deletions: Vec<MqTopicDeletion>,
    pub name_epochs: BTreeMap<String, u64>,
}

impl MqTopicTable {
    pub fn encode(&self) -> Result<Vec<u8>> {
        postcard::to_allocvec(self)
            .map_err(|err| Error::Storage(format!("encode mq topics: {err}")))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let (table, remainder) = postcard::take_from_bytes::<Self>(bytes)
            .map_err(|err| Error::Storage(format!("decode mq topics: {err}")))?;
        if !remainder.is_empty() {
            return Err(Error::Storage(format!(
                "mq topic table has {} trailing bytes",
                remainder.len()
            )));
        }
        table.validate()?;
        Ok(table)
    }

    fn validate(&self) -> Result<()> {
        let mut seen_names: Vec<&str> = Vec::with_capacity(self.topics.len());
        let mut seen_ids: Vec<u64> = Vec::with_capacity(self.topics.len());
        for topic in &self.topics {
            if topic.name.is_empty() || topic.name.len() > MQ_TOPIC_NAME_MAX {
                return Err(Error::Storage(
                    "mq topic table has an invalid name".to_string(),
                ));
            }
            if topic.partitions == 0 || topic.shard_of_partition.len() != topic.partitions as usize
            {
                return Err(Error::Storage(format!(
                    "mq topic {} has {} partitions but {} shard entries",
                    topic.name,
                    topic.partitions,
                    topic.shard_of_partition.len()
                )));
            }
            if seen_ids.contains(&topic.topic_id) {
                return Err(Error::Storage(format!(
                    "mq topic id {} is duplicated",
                    topic.topic_id
                )));
            }
            seen_ids.push(topic.topic_id);
            if seen_names
                .last()
                .is_some_and(|last| *last >= topic.name.as_str())
            {
                return Err(Error::Storage(
                    "mq topics must be sorted by unique name".to_string(),
                ));
            }
            seen_names.push(topic.name.as_str());
        }
        for pair in self.deletions.windows(2) {
            if pair[0].topic_id >= pair[1].topic_id {
                return Err(Error::Storage(
                    "mq deletions must be sorted by topic_id".to_string(),
                ));
            }
        }
        Ok(())
    }

    pub fn topic(&self, name: &str) -> Option<&MqTopicMeta> {
        self.topics.iter().find(|topic| topic.name == name)
    }

    pub fn topic_by_id(&self, topic_id: u64) -> Option<&MqTopicMeta> {
        self.topics.iter().find(|topic| topic.topic_id == topic_id)
    }

    pub fn deletion(&self, topic_id: u64) -> Option<&MqTopicDeletion> {
        self.deletions
            .iter()
            .find(|deletion| deletion.topic_id == topic_id)
    }

    /// 按回收任务 id 查进度（`GetTopicDeletion`）。
    pub fn deletion_by_id(&self, delete_id: u64) -> Option<&MqTopicDeletion> {
        self.deletions
            .iter()
            .find(|deletion| deletion.delete_id == delete_id)
    }

    fn create(&mut self, op: &MqTopicCreate) -> MqTopicOutcome {
        if !op.request_id.is_empty() {
            if let Some(existing) = self
                .topics
                .iter()
                .find(|topic| topic.request_id == op.request_id)
            {
                return MqTopicOutcome::Created {
                    topic_id: existing.topic_id,
                    epoch: existing.epoch,
                    partitions: existing.partitions,
                    config: existing.config,
                    shard_of_partition: existing.shard_of_partition.clone(),
                    replayed: true,
                };
            }
        }
        if let Some(existing) = self.topic(&op.name) {
            if existing.partitions == op.partitions && existing.config == op.config {
                return MqTopicOutcome::Created {
                    topic_id: existing.topic_id,
                    epoch: existing.epoch,
                    partitions: existing.partitions,
                    config: existing.config,
                    shard_of_partition: existing.shard_of_partition.clone(),
                    replayed: true,
                };
            }
            return MqTopicOutcome::Exists {
                topic_id: existing.topic_id,
                epoch: existing.epoch,
            };
        }
        if op.partitions == 0 || op.log_shards == 0 {
            return MqTopicOutcome::Invalid {
                reason: "partitions and log_shards must be > 0".to_string(),
            };
        }
        let max_partitions =
            u64::from(op.log_shards).saturating_mul(u64::from(op.max_partitions_per_shard));
        if u64::from(op.partitions) > max_partitions {
            return MqTopicOutcome::Invalid {
                reason: format!(
                    "partitions {} exceeds log_shards {} * max_partitions_per_shard {}",
                    op.partitions, op.log_shards, op.max_partitions_per_shard
                ),
            };
        }
        if !valid_topic_name(&op.name) {
            return MqTopicOutcome::Invalid {
                reason: "topic name must be 1..=255 bytes of [A-Za-z0-9._-]".to_string(),
            };
        }

        let topic_id = self.next_topic_id.max(1);
        self.next_topic_id = topic_id.saturating_add(1);
        let epoch = self
            .name_epochs
            .get(&op.name)
            .copied()
            .unwrap_or(0)
            .saturating_add(1);
        self.name_epochs.insert(op.name.clone(), epoch);

        let shard_of_partition = (0..op.partitions)
            .map(|partition| {
                // 身份 + 分区号做稳定散列：重建同名 topic 得到新 topic_id，
                // 分区落点随之变化；同一 topic 的映射跨重启稳定。
                ((topic_id.wrapping_add(u64::from(partition))) % u64::from(op.log_shards)) as u32
            })
            .collect();

        let topic = MqTopicMeta {
            topic_id,
            name: op.name.clone(),
            epoch,
            partitions: op.partitions,
            config: op.config,
            created_at_ms: op.now_ms,
            request_id: op.request_id.clone(),
            shard_of_partition,
        };
        let outcome = MqTopicOutcome::Created {
            topic_id: topic.topic_id,
            epoch: topic.epoch,
            partitions: topic.partitions,
            config: topic.config,
            shard_of_partition: topic.shard_of_partition.clone(),
            replayed: false,
        };
        self.topics.push(topic);
        self.topics
            .sort_by(|left, right| left.name.cmp(&right.name));
        outcome
    }

    fn deletion_progress(&mut self, op: &MqTopicDeletionProgress) -> MqTopicOutcome {
        let Some(index) = self
            .deletions
            .iter()
            .position(|deletion| deletion.delete_id == op.delete_id)
        else {
            return MqTopicOutcome::DeletionNotFound;
        };
        let deletion = &mut self.deletions[index];
        if deletion.state == MqTopicDeletionState::Pending {
            if let Some(position) = deletion
                .pending_partitions
                .iter()
                .position(|partition| *partition == op.partition)
            {
                deletion.pending_partitions.remove(position);
                deletion.messages_removed = deletion
                    .messages_removed
                    .saturating_add(op.messages_removed);
                deletion.bytes_reclaimed =
                    deletion.bytes_reclaimed.saturating_add(op.bytes_reclaimed);
            }
            if deletion.pending_partitions.is_empty() {
                deletion.state = MqTopicDeletionState::Done;
            }
        }
        MqTopicOutcome::DeletionProgressed {
            delete_id: deletion.delete_id,
            state: deletion.state,
            partitions_reclaimed: deletion
                .partitions_total
                .saturating_sub(deletion.pending_partitions.len() as u32),
            partitions_total: deletion.partitions_total,
            messages_removed: deletion.messages_removed,
            bytes_reclaimed: deletion.bytes_reclaimed,
        }
    }

    fn update(&mut self, op: &MqTopicUpdate) -> MqTopicOutcome {
        let Some(index) = self.topics.iter().position(|topic| topic.name == op.name) else {
            return MqTopicOutcome::NotFound;
        };
        let current_epoch = self.topics[index].epoch;
        if op.expected_epoch != 0 && op.expected_epoch != current_epoch {
            return MqTopicOutcome::EpochStale { current_epoch };
        }
        let epoch = current_epoch.saturating_add(1);
        let topic_id = self.topics[index].topic_id;
        self.topics[index].config = op.config;
        self.topics[index].epoch = epoch;
        self.name_epochs.insert(op.name.clone(), epoch);
        MqTopicOutcome::Updated {
            topic_id,
            epoch,
            config: op.config,
        }
    }

    fn delete(&mut self, op: &MqTopicDelete) -> MqTopicOutcome {
        if let Some(deletion) = self.deletions.iter().find(|d| d.delete_id == op.delete_id) {
            return MqTopicOutcome::Deleted {
                topic_id: deletion.topic_id,
                delete_id: deletion.delete_id,
                partitions: deletion.pending_partitions.len() as u32,
                deleted_epoch: deletion.deleted_epoch,
                replayed: true,
            };
        }
        let Some(index) = self.topics.iter().position(|topic| topic.name == op.name) else {
            return MqTopicOutcome::NotFound;
        };
        let topic = self.topics.remove(index);
        let epoch = topic.epoch.saturating_add(1);
        self.name_epochs.insert(op.name.clone(), epoch);
        let deletion = MqTopicDeletion {
            topic_id: topic.topic_id,
            name: topic.name,
            delete_id: op.delete_id,
            deleted_at_ms: op.now_ms,
            state: MqTopicDeletionState::Pending,
            deleted_epoch: epoch,
            partitions_total: topic.partitions,
            pending_partitions: (0..topic.partitions).collect(),
            shard_of_partition: topic.shard_of_partition.clone(),
            messages_removed: 0,
            bytes_reclaimed: 0,
        };
        let outcome = MqTopicOutcome::Deleted {
            topic_id: topic.topic_id,
            delete_id: op.delete_id,
            partitions: topic.partitions,
            deleted_epoch: epoch,
            replayed: false,
        };
        self.deletions.push(deletion);
        self.deletions.sort_by_key(|deletion| deletion.topic_id);
        outcome
    }
}

/// 契约的 topic 名字规则（`[A-Za-z0-9._-]`，1–255 字节）。
fn valid_topic_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MQ_TOPIC_NAME_MAX
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

// ──── 命令载荷 ────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqTopicCreate {
    pub name: String,
    pub partitions: u32,
    pub config: MqTopicConfig,
    pub request_id: String,
    pub now_ms: i64,
    /// `[mq_v2].log_shards`（分区映射的分母，随命令携带以保持确定性）。
    pub log_shards: u32,
    pub max_partitions_per_shard: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqTopicUpdate {
    pub name: String,
    /// 0 = 不校验代际。
    pub expected_epoch: u64,
    pub config: MqTopicConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqTopicDelete {
    pub name: String,
    /// 幂等键：同 delete_id 重放返回首次结果。
    pub delete_id: u64,
    pub now_ms: i64,
}

/// 仅 region 0 system raft 提出的 topic 元数据操作。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MqTopicOp {
    Create(MqTopicCreate),
    Update(MqTopicUpdate),
    Delete(MqTopicDelete),
    /// 末尾追加：回收器确认一个分区已物理回收。
    DeletionProgress(MqTopicDeletionProgress),
}

/// 删除回收进度推进（每个分区一条；清空 pending 时 state 转 Done）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqTopicDeletionProgress {
    pub delete_id: u64,
    pub partition: u32,
    pub messages_removed: u64,
    pub bytes_reclaimed: u64,
}

/// 用户级结果（拒绝不失败 apply）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MqTopicOutcome {
    Created {
        topic_id: u64,
        epoch: u64,
        partitions: u32,
        config: MqTopicConfig,
        shard_of_partition: Vec<u32>,
        replayed: bool,
    },
    /// 同名 topic 已存在且配置/分区数不同（handler 映射 `TOPIC_EXISTS`）。
    Exists {
        topic_id: u64,
        epoch: u64,
    },
    Updated {
        topic_id: u64,
        epoch: u64,
        config: MqTopicConfig,
    },
    Deleted {
        topic_id: u64,
        delete_id: u64,
        partitions: u32,
        /// 删除后的代际（`DeleteTopicResponse.deleted_topic_epoch`）。
        deleted_epoch: u64,
        replayed: bool,
    },
    /// 回收进度推进（`MqTopicOp::DeletionProgress` 的 apply 结果）。
    DeletionProgressed {
        delete_id: u64,
        state: MqTopicDeletionState,
        partitions_reclaimed: u32,
        partitions_total: u32,
        messages_removed: u64,
        bytes_reclaimed: u64,
    },
    /// `delete_id` 不存在（已回收/从未存在）。
    DeletionNotFound,
    /// 目标不存在（已删除也算；handler 映射 `TOPIC_NOT_FOUND`）。
    NotFound,
    EpochStale {
        current_epoch: u64,
    },
    Invalid {
        reason: String,
    },
}

// ──── apply / 读取 ────

impl<B: StorageBackend> MvccStorage<B> {
    /// 应用一条 topic 元数据命令（region 0 system raft）。
    ///
    /// 表级拒绝只体现在 outcome 里；无论 applied/invalid 都推进 applied 水位，
    /// 避免重启后同一条命令被无限重放。不写 changelog、不上 Watch。
    pub fn apply_mq_topic_op(
        &self,
        op: &MqTopicOp,
        revision: Revision,
        applied: AppliedLogId,
    ) -> Result<MqTopicOutcome> {
        self.backend().write(|tx| {
            self.note_log_index_revision(tx, revision)?;
            let mut table = read_topic_table(tx)?;
            let outcome = match op {
                MqTopicOp::Create(create) => table.create(create),
                MqTopicOp::Update(update) => table.update(update),
                MqTopicOp::Delete(delete) => table.delete(delete),
                MqTopicOp::DeletionProgress(progress) => table.deletion_progress(progress),
            };
            if matches!(
                outcome,
                MqTopicOutcome::Created { .. }
                    | MqTopicOutcome::Updated { .. }
                    | MqTopicOutcome::Deleted { .. }
                    | MqTopicOutcome::DeletionProgressed { .. }
            ) {
                tx.insert(TABLE_KV, MQ_TOPIC_TABLE_KEY, &table.encode()?)?;
            }
            tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;
            Ok(outcome)
        })
    }

    /// 读取 region 0 的 topic 表（缺失 = 空表）。
    pub fn mq_topic_table(&self) -> Result<MqTopicTable> {
        self.backend().read(read_topic_table)
    }
}

fn read_topic_table(tx: &dyn ReadTx) -> Result<MqTopicTable> {
    match tx.get(TABLE_KV, MQ_TOPIC_TABLE_KEY)? {
        Some(bytes) => MqTopicTable::decode(&bytes),
        None => Ok(MqTopicTable::default()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::redb_backend::RedbBackend;
    use coord_core::types::StorageConfig;
    use tempfile::TempDir;

    fn setup() -> (TempDir, MvccStorage<RedbBackend>) {
        let dir = TempDir::new().unwrap();
        let backend = RedbBackend::open(dir.path(), &StorageConfig::default()).unwrap();
        let storage = MvccStorage::new(backend).unwrap();
        (dir, storage)
    }

    fn applied(index: u64) -> AppliedLogId {
        AppliedLogId {
            term: 1,
            node_id: 1,
            index,
        }
    }

    fn config(max_bytes: u64) -> MqTopicConfig {
        MqTopicConfig {
            retention_secs: 604_800,
            max_bytes,
            max_message_bytes: 1024,
            segment_bytes: 1 << 20,
            dlq_retention_secs: 604_800,
        }
    }

    fn create(name: &str, partitions: u32, request_id: &str, max_bytes: u64) -> MqTopicOp {
        MqTopicOp::Create(MqTopicCreate {
            name: name.to_string(),
            partitions,
            config: config(max_bytes),
            request_id: request_id.to_string(),
            now_ms: 1_700_000_000_000,
            log_shards: 4,
            max_partitions_per_shard: 1024,
        })
    }

    fn apply(storage: &MvccStorage<RedbBackend>, op: MqTopicOp, index: u64) -> MqTopicOutcome {
        storage
            .apply_mq_topic_op(&op, index, applied(index))
            .unwrap()
    }

    #[test]
    fn create_is_idempotent_by_request_id_and_rejects_conflicting_config() {
        let (_dir, storage) = setup();
        let created = apply(&storage, create("orders", 4, "req-1", 100), 1);
        let (topic_id, epoch, shards) = match created {
            MqTopicOutcome::Created {
                topic_id,
                epoch,
                ref shard_of_partition,
                replayed,
                ..
            } => {
                assert!(!replayed);
                (topic_id, epoch, shard_of_partition.clone())
            }
            other => panic!("expected Created, got {other:?}"),
        };
        assert_eq!(shards.len(), 4);
        assert!(shards.iter().all(|shard| *shard < 4));

        let replay = apply(&storage, create("orders", 4, "req-1", 100), 2);
        match replay {
            MqTopicOutcome::Created {
                topic_id: replay_id,
                epoch: replay_epoch,
                replayed,
                ..
            } => {
                assert!(replayed);
                assert_eq!(replay_id, topic_id);
                assert_eq!(replay_epoch, epoch);
            }
            other => panic!("expected replayed Created, got {other:?}"),
        }

        // 同名同配置：幂等；同名不同配置：Exists。
        assert!(matches!(
            apply(&storage, create("orders", 4, "", 100), 3),
            MqTopicOutcome::Created { replayed: true, .. }
        ));
        assert!(matches!(
            apply(&storage, create("orders", 4, "", 999), 4),
            MqTopicOutcome::Exists { .. }
        ));
    }

    #[test]
    fn update_checks_epoch_and_rejects_unknown_topic() {
        let (_dir, storage) = setup();
        apply(&storage, create("orders", 1, "", 100), 1);
        let update = |expected_epoch: u64, max_bytes: u64| {
            MqTopicOp::Update(MqTopicUpdate {
                name: "orders".to_string(),
                expected_epoch,
                config: config(max_bytes),
            })
        };
        match apply(&storage, update(0, 200), 2) {
            MqTopicOutcome::Updated { epoch, config, .. } => {
                assert_eq!(epoch, 2);
                assert_eq!(config.max_bytes, 200);
            }
            other => panic!("expected Updated, got {other:?}"),
        }
        assert!(matches!(
            apply(&storage, update(1, 300), 3),
            MqTopicOutcome::EpochStale { current_epoch: 2 }
        ));
        let unknown = MqTopicOp::Update(MqTopicUpdate {
            name: "missing".to_string(),
            expected_epoch: 0,
            config: config(1),
        });
        assert!(matches!(
            apply(&storage, unknown, 4),
            MqTopicOutcome::NotFound
        ));
    }

    #[test]
    fn delete_marks_pending_and_recreate_gets_a_new_identity() {
        let (_dir, storage) = setup();
        let first = match apply(&storage, create("orders", 2, "", 100), 1) {
            MqTopicOutcome::Created { topic_id, .. } => topic_id,
            other => panic!("expected Created, got {other:?}"),
        };
        let deleted = apply(
            &storage,
            MqTopicOp::Delete(MqTopicDelete {
                name: "orders".to_string(),
                delete_id: 7,
                now_ms: 2,
            }),
            2,
        );
        match deleted {
            MqTopicOutcome::Deleted {
                topic_id,
                delete_id,
                partitions,
                deleted_epoch,
                replayed,
            } => {
                assert_eq!(topic_id, first);
                assert_eq!(delete_id, 7);
                assert_eq!(partitions, 2);
                assert_eq!(deleted_epoch, 2);
                assert!(!replayed);
            }
            other => panic!("expected Deleted, got {other:?}"),
        }

        let table = storage.mq_topic_table().unwrap();
        assert!(table.topic("orders").is_none());
        let deletion = table.deletion(first).expect("deletion record");
        assert_eq!(deletion.state, MqTopicDeletionState::Pending);
        assert_eq!(deletion.pending_partitions, vec![0, 1]);

        // 同 delete_id 重放幂等。
        assert!(matches!(
            apply(
                &storage,
                MqTopicOp::Delete(MqTopicDelete {
                    name: "orders".to_string(),
                    delete_id: 7,
                    now_ms: 3,
                }),
                3
            ),
            MqTopicOutcome::Deleted { replayed: true, .. }
        ));

        // 同名重建：新 topic_id、代际继续前进，旧数据按旧 id 隔离。
        match apply(&storage, create("orders", 2, "", 100), 4) {
            MqTopicOutcome::Created {
                topic_id, epoch, ..
            } => {
                assert_ne!(topic_id, first);
                assert_eq!(epoch, 3);
            }
            other => panic!("expected Created, got {other:?}"),
        }
        assert!(matches!(
            apply(
                &storage,
                MqTopicOp::Delete(MqTopicDelete {
                    name: "missing".to_string(),
                    delete_id: 8,
                    now_ms: 4,
                }),
                5
            ),
            MqTopicOutcome::NotFound
        ));
    }

    #[test]
    fn invalid_names_and_partition_counts_are_outcomes_not_apply_errors() {
        let (_dir, storage) = setup();
        assert!(matches!(
            apply(&storage, create("bad name", 1, "", 10), 1),
            MqTopicOutcome::Invalid { .. }
        ));
        assert!(matches!(
            apply(&storage, create("orders", 0, "", 10), 2),
            MqTopicOutcome::Invalid { .. }
        ));
        assert!(matches!(
            apply(&storage, create("orders", 4097, "", 10), 3),
            MqTopicOutcome::Invalid { .. }
        ));
        // 拒绝也要推进 applied 水位（否则重启后同一条命令被无限重放）。
        assert_eq!(storage.get_applied_log_id().unwrap().unwrap().index, 3);
        assert!(storage.mq_topic_table().unwrap().topics.is_empty());
    }

    #[test]
    fn table_survives_snapshot_roundtrip() {
        use crate::storage::snapshot::{export_snapshot_data, import_snapshot_data, SnapshotData};

        let (_dir, storage) = setup();
        apply(&storage, create("orders", 2, "", 100), 1);
        let data = export_snapshot_data(&storage, 1, 1).unwrap();
        let restored = SnapshotData::from_bytes(&data.to_bytes().unwrap()).unwrap();
        let (_dir2, storage2) = setup();
        import_snapshot_data(&storage2, &restored).unwrap();

        let table = storage2.mq_topic_table().unwrap();
        let topic = table.topic("orders").expect("topic survives snapshot");
        assert_eq!(topic.partitions, 2);
        assert_eq!(topic.topic_id, 1);
    }
}
