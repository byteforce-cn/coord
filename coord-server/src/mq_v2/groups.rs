//! MQ v2 消费组：组 shard 路由、命令提案与只读面（ADR-0016 D6；M3-2/M3-3）。
//!
//! 组状态机在 `storage::mq_group`（M1-3）已实现：成员/代际/range 分配/位点全部由
//! apply 计算，handler 只负责「组名 → group shard → Leader → 提案 → 结果映射」。
//!
//! **组 shard 哈希必须跨版本稳定**：组的成员/位点只存在一个 shard。用
//! `DefaultHasher`（SipHash，std 不保证跨版本稳定）会让一次 Rust 升级把组状态
//! 「漂」到另一个分片，读不到位点却没有任何错误。这里固定 FNV-1a 64。

use std::sync::Arc;

use coord_core::error_code::{attach, CoordErrorCode};
use coord_proto::mq::v2 as mq_v2;
use tonic::Status;

use crate::raft::region_runtime::RegionRuntime;
use crate::raft::type_config::{Command, Response as RaftResponse};
use crate::server::{map_err, CoordNode};
use crate::storage::mq_group::{
    MqGroupAck, MqGroupAckEntry, MqGroupOp, MqGroupRejection, MqGroupResult,
};
use crate::storage::mq_topic::MqTopicTable;
use crate::storage::region_table::mq_group_shard_prefix;

use super::{mq_status, now_ms};

/// 契约上限：单次 `AckBatch` 1–256 条。
pub const MAX_ACK_BATCH: usize = 256;

/// 组名 → group shard（FNV-1a 64 取模；跨版本稳定，见模块头注）。
pub fn mq_group_shard(group: &str, group_shards: u32) -> u32 {
    if group_shards == 0 {
        return 0;
    }
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in group.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash % u64::from(group_shards)) as u32
}

impl CoordNode {
    /// 组所属的 group shard 与其 Region 运行时。
    pub(crate) fn mq_group_runtime(
        &self,
        group: &str,
    ) -> Result<(u32, Arc<RegionRuntime>), Status> {
        let shard_id = mq_group_shard(group, self.mq_v2().config().group_shards);
        Ok((shard_id, self.mq_group_shard_runtime(shard_id)?))
    }

    pub(crate) fn mq_group_shard_runtime(
        &self,
        shard_id: u32,
    ) -> Result<Arc<RegionRuntime>, Status> {
        let manager = self.region_manager.as_ref().ok_or_else(|| {
            attach(
                Status::failed_precondition(
                    "mq_v2 consumer groups require multi-raft region assembly \
                     (region_manager missing); enable [multi_raft] + [kv_t]",
                ),
                CoordErrorCode::FailedPrecondition,
            )
        })?;
        manager
            .route_runtime(&mq_group_shard_prefix(shard_id))
            .map_err(mq_status)
    }

    /// 组命令必须由 group shard 的 Leader 处理（与日志分片同口径）。
    async fn mq_group_ensure_leader(&self, runtime: &RegionRuntime) -> Result<(), Status> {
        let leader = runtime.raft.current_leader().await;
        if leader == Some(self.node_id) {
            return Ok(());
        }
        Err(attach(
            crate::server::map_err(coord_core::error::Error::RegionNotLeader {
                region_id: runtime.region_id(),
                leader_addr: leader.and_then(|id| self.grpc_addr_of(id)),
            }),
            CoordErrorCode::NotLeader,
        ))
    }

    /// 提交一条组命令（`Command::MqGroup`），返回 apply 结果。
    pub(crate) async fn mq_propose_group(&self, op: MqGroupOp) -> Result<MqGroupResult, Status> {
        let shard_id = op.shard_id();
        let runtime = self.mq_group_shard_runtime(shard_id)?;
        self.mq_group_ensure_leader(&runtime).await?;
        let region_id = runtime.region_id();
        let response = self
            .client_write_with_timeout(&runtime.raft, Command::MqGroup(op), region_id)
            .await?;
        match response.response() {
            RaftResponse::MqGroup { result } => Ok(result.clone()),
            other => Err(Status::internal(format!(
                "unexpected raft response for MqGroup: {other:?}"
            ))),
        }
    }

    /// `Ack`：一条 `(topic, partition, offset)`（契约：committed 推进到 `offset + 1`）。
    pub(crate) async fn mq_ack(
        &self,
        request: mq_v2::AckRequest,
    ) -> Result<mq_v2::AckResponse, Status> {
        let (shard_id, runtime) = self.mq_group_runtime(&request.group)?;
        self.mq_group_ensure_leader(&runtime).await?;
        let table = self.mq_topic_table()?;
        let entry = ack_entry(
            &table,
            &request.topic,
            request.partition,
            request.topic_epoch,
            request.offset,
        )?;
        let result = self
            .mq_propose_group(MqGroupOp::Ack(MqGroupAck {
                shard_id,
                group: request.group,
                member_id: request.member_id,
                generation: request.generation,
                entries: vec![entry],
                now_ms: now_ms(),
            }))
            .await?;
        let acked = expect_acked(result)?;
        let first = acked
            .results
            .first()
            .ok_or_else(|| Status::internal("ack applied no offset result"))?;
        if let Some(metrics) = self.metrics() {
            metrics.record_mq_v2_ack(1, false);
        }
        Ok(mq_v2::AckResponse {
            committed_offset: i64::try_from(first.committed_offset).unwrap_or(i64::MAX),
        })
    }

    /// `AckBatch`：1–256 条，同一组；任一条未分配/代际过期 ⇒ 整批不写（apply 内保证）。
    pub(crate) async fn mq_ack_batch(
        &self,
        request: mq_v2::AckBatchRequest,
    ) -> Result<mq_v2::AckBatchResponse, Status> {
        if request.acks.is_empty() || request.acks.len() > MAX_ACK_BATCH {
            return Err(attach(
                Status::invalid_argument(format!(
                    "AckBatch accepts 1..={MAX_ACK_BATCH} entries (got {})",
                    request.acks.len()
                )),
                CoordErrorCode::InvalidArgument,
            ));
        }
        let (shard_id, runtime) = self.mq_group_runtime(&request.group)?;
        self.mq_group_ensure_leader(&runtime).await?;
        let table = self.mq_topic_table()?;
        let mut entries = Vec::with_capacity(request.acks.len());
        for ack in &request.acks {
            entries.push(ack_entry(
                &table,
                &ack.topic,
                ack.partition,
                ack.topic_epoch,
                ack.offset,
            )?);
        }
        let results = self
            .mq_propose_group(MqGroupOp::Ack(MqGroupAck {
                shard_id,
                group: request.group,
                member_id: request.member_id,
                generation: request.generation,
                entries,
                now_ms: now_ms(),
            }))
            .await?;
        let acked = expect_acked(results)?;
        if acked.results.len() != request.acks.len() {
            return Err(Status::internal(format!(
                "ack batch applied {} entries but {} were submitted",
                acked.results.len(),
                request.acks.len()
            )));
        }
        if let Some(metrics) = self.metrics() {
            metrics.record_mq_v2_ack(request.acks.len() as u64, false);
        }
        let table = self.mq_topic_table()?;
        let mut responses = Vec::with_capacity(acked.results.len());
        for result in &acked.results {
            let topic = table
                .topic_by_id(result.topic_id)
                .map(|topic| topic.name.clone())
                .unwrap_or_default();
            responses.push(mq_v2::AckResult {
                topic,
                partition: result.partition as i32,
                committed_offset: i64::try_from(result.committed_offset).unwrap_or(i64::MAX),
            });
        }
        Ok(mq_v2::AckBatchResponse { results: responses })
    }
}

impl CoordNode {
    /// `JoinGroup`：订阅的 topic 必须已存在（fail-closed，不自动建）。
    pub(crate) async fn mq_join_group(
        &self,
        request: mq_v2::JoinGroupRequest,
    ) -> Result<mq_v2::JoinGroupResponse, Status> {
        if request.group.is_empty() {
            return Err(group_invalid("group must not be empty"));
        }
        if request.topics.is_empty() {
            return Err(group_invalid("topics must not be empty"));
        }
        let table = self.mq_topic_table()?;
        let mut topics = Vec::with_capacity(request.topics.len());
        let mut seen = std::collections::BTreeSet::new();
        for name in &request.topics {
            if !seen.insert(name.clone()) {
                return Err(group_invalid(format!(
                    "duplicate topic {name} in the subscription"
                )));
            }
            let Some(meta) = table.topic(name) else {
                return Err(attach(
                    Status::not_found(format!("topic not found: {name}")),
                    CoordErrorCode::TopicNotFound,
                ));
            };
            topics.push(crate::storage::mq_group::MqGroupTopic {
                topic: name.clone(),
                topic_id: meta.topic_id,
                partitions: meta.partitions,
            });
        }
        let config = self.mq_v2().config();
        let session_timeout_ms = if request.session_timeout_ms <= 0 {
            config.group_session_timeout_ms
        } else {
            u32::try_from(request.session_timeout_ms)
                .map_err(|_| group_invalid("session_timeout_ms exceeds u32"))?
        };
        if !(1_000..=300_000).contains(&session_timeout_ms) {
            return Err(group_invalid(format!(
                "session_timeout_ms must be in 1000..=300000 (got {session_timeout_ms})"
            )));
        }
        let shard_id = mq_group_shard(&request.group, config.group_shards);
        let result = self
            .mq_propose_group(MqGroupOp::Join(crate::storage::mq_group::MqGroupJoin {
                shard_id,
                group: request.group,
                member_id: request.member_id,
                topics,
                session_timeout_ms,
                now_ms: now_ms(),
            }))
            .await?;
        match result {
            MqGroupResult::Joined(joined) => {
                if let Some(metrics) = self.metrics() {
                    metrics.record_mq_v2_group_join(joined.rebalanced);
                }
                Ok(mq_v2::JoinGroupResponse {
                    member_id: joined.member_id,
                    generation: joined.generation,
                    assignments: joined
                        .assignments
                        .into_iter()
                        .map(|assignment| mq_v2::GroupAssignment {
                            topic: assignment.topic,
                            partition: assignment.partition as i32,
                            committed_offset: i64::try_from(assignment.committed_offset)
                                .unwrap_or(i64::MAX),
                        })
                        .collect(),
                    heartbeat_interval_ms: i32::try_from(joined.heartbeat_interval_ms)
                        .unwrap_or(i32::MAX),
                    session_timeout_ms: i32::try_from(joined.session_timeout_ms)
                        .unwrap_or(i32::MAX),
                })
            }
            MqGroupResult::Rejected(rejection) => Err(group_rejection_status(&rejection)),
            other => Err(Status::internal(format!(
                "unexpected group result for JoinGroup: {other:?}"
            ))),
        }
    }

    /// `Heartbeat`：续期；过期代际/非成员由 apply 拒绝。
    pub(crate) async fn mq_heartbeat(
        &self,
        request: mq_v2::HeartbeatRequest,
    ) -> Result<mq_v2::HeartbeatResponse, Status> {
        let shard_id = mq_group_shard(&request.group, self.mq_v2().config().group_shards);
        let result = self
            .mq_propose_group(MqGroupOp::Heartbeat(
                crate::storage::mq_group::MqGroupHeartbeat {
                    shard_id,
                    group: request.group,
                    member_id: request.member_id,
                    generation: request.generation,
                    now_ms: now_ms(),
                },
            ))
            .await?;
        match result {
            MqGroupResult::Heartbeat(reply) => Ok(mq_v2::HeartbeatResponse {
                generation: reply.generation,
                rebalance_required: reply.rebalance_required,
            }),
            MqGroupResult::Rejected(rejection) => Err(group_rejection_status(&rejection)),
            other => Err(Status::internal(format!(
                "unexpected group result for Heartbeat: {other:?}"
            ))),
        }
    }

    /// `LeaveGroup`：移除成员并推进代际（重复 Leave 幂等成功）。
    pub(crate) async fn mq_leave_group(
        &self,
        request: mq_v2::LeaveGroupRequest,
    ) -> Result<mq_v2::LeaveGroupResponse, Status> {
        let shard_id = mq_group_shard(&request.group, self.mq_v2().config().group_shards);
        let result = self
            .mq_propose_group(MqGroupOp::Leave(crate::storage::mq_group::MqGroupLeave {
                shard_id,
                group: request.group,
                member_id: request.member_id,
                generation: request.generation,
                now_ms: now_ms(),
            }))
            .await?;
        match result {
            MqGroupResult::Left(_) => Ok(mq_v2::LeaveGroupResponse {}),
            MqGroupResult::Rejected(rejection) => Err(group_rejection_status(&rejection)),
            other => Err(Status::internal(format!(
                "unexpected group result for LeaveGroup: {other:?}"
            ))),
        }
    }

    /// `GetCommittedOffsets`：读组 shard 的已 apply 位点；空请求 = 全部分配。
    pub(crate) async fn mq_get_committed_offsets(
        &self,
        request: mq_v2::GetCommittedOffsetsRequest,
    ) -> Result<mq_v2::GetCommittedOffsetsResponse, Status> {
        let runtime = {
            let (_, runtime) = self.mq_group_runtime(&request.group)?;
            self.mq_group_ensure_leader(&runtime).await?;
            runtime
        };
        let state = runtime.mvcc.mq_group(&request.group).map_err(map_err)?;
        if state.is_none() {
            return Err(group_rejection_status(&MqGroupRejection::GroupNotFound));
        }
        let offsets = runtime
            .mvcc
            .mq_group_offsets(&request.group)
            .map_err(map_err)?;
        let table = self.mq_topic_table()?;
        let mut response = Vec::new();
        if request.partitions.is_empty() {
            for offset in offsets {
                let Some(meta) = table.topic_by_id(offset.topic_id) else {
                    // topic 已删除：位点由 DeleteTopic 的异步回收清理（M4-2）。
                    continue;
                };
                response.push(mq_v2::CommittedOffset {
                    topic: meta.name.clone(),
                    partition: offset.partition as i32,
                    offset: i64::try_from(offset.committed_offset).unwrap_or(i64::MAX),
                    committed: true,
                });
            }
        } else {
            for requested in &request.partitions {
                let (meta, partition) =
                    resolve_topic_partition(&table, &requested.topic, requested.partition)?;
                let committed = offsets.iter().find(|offset| {
                    offset.topic_id == meta.topic_id && offset.partition == partition
                });
                response.push(mq_v2::CommittedOffset {
                    topic: meta.name.clone(),
                    partition: partition as i32,
                    offset: committed
                        .map(|offset| i64::try_from(offset.committed_offset).unwrap_or(i64::MAX))
                        .unwrap_or(0),
                    committed: committed.is_some(),
                });
            }
        }
        Ok(mq_v2::GetCommittedOffsetsResponse { offsets: response })
    }

    /// `DescribeGroup`：成员/分配/位点/lag（lag 读日志分片本地副本，可滞后）。
    pub(crate) async fn mq_describe_group(
        &self,
        request: mq_v2::DescribeGroupRequest,
    ) -> Result<mq_v2::DescribeGroupResponse, Status> {
        let runtime = {
            let (_, runtime) = self.mq_group_runtime(&request.group)?;
            self.mq_group_ensure_leader(&runtime).await?;
            runtime
        };
        let Some(state) = runtime.mvcc.mq_group(&request.group).map_err(map_err)? else {
            return Err(group_rejection_status(&MqGroupRejection::GroupNotFound));
        };
        let offsets = runtime
            .mvcc
            .mq_group_offsets(&request.group)
            .map_err(map_err)?;
        let committed_of = |topic_id: u64, partition: u32| -> u64 {
            offsets
                .iter()
                .find(|offset| offset.topic_id == topic_id && offset.partition == partition)
                .map(|offset| offset.committed_offset)
                .unwrap_or(0)
        };
        let mut members = Vec::with_capacity(state.members.len());
        let mut partitions = Vec::new();
        for member in &state.members {
            let assignments = member
                .assignments
                .iter()
                .map(|assignment| mq_v2::GroupAssignment {
                    topic: assignment.topic.clone(),
                    partition: assignment.partition as i32,
                    committed_offset: i64::try_from(committed_of(
                        assignment.topic_id,
                        assignment.partition,
                    ))
                    .unwrap_or(i64::MAX),
                })
                .collect();
            for assignment in &member.assignments {
                let committed = committed_of(assignment.topic_id, assignment.partition);
                let next_offset = self
                    .mq_leader_partition_next(assignment.topic_id, assignment.partition)
                    .unwrap_or(0);
                partitions.push(mq_v2::GroupPartitionLag {
                    topic: assignment.topic.clone(),
                    partition: assignment.partition as i32,
                    committed_offset: i64::try_from(committed).unwrap_or(i64::MAX),
                    next_offset: i64::try_from(next_offset).unwrap_or(i64::MAX),
                    lag: i64::try_from(next_offset.saturating_sub(committed)).unwrap_or(i64::MAX),
                });
            }
            members.push(mq_v2::GroupMember {
                member_id: member.member_id.clone(),
                topics: member
                    .topics
                    .iter()
                    .map(|topic| topic.topic.clone())
                    .collect(),
                assignments,
                last_heartbeat_ms: member.last_heartbeat_ms,
            });
        }
        let state_code = if state.members.is_empty() {
            mq_v2::GroupState::Empty as i32
        } else {
            mq_v2::GroupState::Stable as i32
        };
        Ok(mq_v2::DescribeGroupResponse {
            group: request.group,
            generation: state.generation,
            state: state_code,
            members,
            partitions,
        })
    }

    /// 读分区的最新 offset（按日志分片本地副本；topic_id 来自组分配）。
    fn mq_leader_partition_next(&self, topic_id: u64, partition: u32) -> Option<u64> {
        let table = self.mq_topic_table().ok()?;
        let topic = table.topic_by_id(topic_id)?;
        let shard_id = topic.shard_of_partition.get(partition as usize).copied()?;
        let runtime = self.mq_shard_runtime(shard_id).ok()?;
        runtime
            .mvcc
            .mq_partition_bounds(topic_id, partition)
            .ok()
            .flatten()
            .map(|bounds| bounds.next_offset)
    }

    /// `ListGroups`：逐 group shard 合并排序；`page_token` = 上一页最后一个组名。
    pub(crate) async fn mq_list_groups(
        &self,
        request: mq_v2::ListGroupsRequest,
    ) -> Result<mq_v2::ListGroupsResponse, Status> {
        let mut names = std::collections::BTreeSet::new();
        for shard_id in 0..self.mq_v2().config().group_shards {
            let Ok(runtime) = self.mq_group_shard_runtime(shard_id) else {
                continue;
            };
            if let Ok(list) = runtime.mvcc.mq_group_names(&request.prefix) {
                names.extend(list);
            }
        }
        let page_size = if request.page_size <= 0 {
            100usize
        } else {
            (request.page_size as usize).min(1_000)
        };
        let mut iterator = names.into_iter().filter(|name| {
            request.page_token.is_empty() || name.as_str() > request.page_token.as_str()
        });
        let groups: Vec<String> = iterator.by_ref().take(page_size).collect();
        let next_page_token = if iterator.next().is_some() {
            groups.last().cloned().unwrap_or_default()
        } else {
            String::new()
        };
        Ok(mq_v2::ListGroupsResponse {
            groups,
            next_page_token,
        })
    }
}

fn group_invalid(reason: impl Into<String>) -> Status {
    attach(
        Status::invalid_argument(reason.into()),
        CoordErrorCode::InvalidArgument,
    )
}

/// topic 名 + 契约分区号 → `(meta, u32 partition)`（`PARTITION_NOT_FOUND` 口径）。
fn resolve_topic_partition<'a>(
    table: &'a MqTopicTable,
    topic: &str,
    partition: i32,
) -> Result<(&'a crate::storage::mq_topic::MqTopicMeta, u32), Status> {
    let Some(meta) = table.topic(topic) else {
        return Err(attach(
            Status::not_found(format!("topic not found: {topic}")),
            CoordErrorCode::TopicNotFound,
        ));
    };
    let partition_u32 = u32::try_from(partition)
        .ok()
        .filter(|value| *value < meta.partitions)
        .ok_or_else(|| {
            attach(
                Status::not_found(format!(
                    "partition {partition} does not exist on topic {topic} \
                     ({} partitions)",
                    meta.partitions
                )),
                CoordErrorCode::PartitionNotFound,
            )
        })?;
    Ok((meta, partition_u32))
}

/// 契约 `AckEntry` → apply 的 `(topic_id, partition, offset)`（含代际/范围校验）。
fn ack_entry(
    table: &MqTopicTable,
    topic: &str,
    partition: i32,
    topic_epoch: u64,
    offset: i64,
) -> Result<MqGroupAckEntry, Status> {
    let Some(meta) = table.topic(topic) else {
        return Err(attach(
            Status::not_found(format!("topic not found: {topic}")),
            CoordErrorCode::TopicNotFound,
        ));
    };
    if topic_epoch != 0 && topic_epoch != meta.epoch {
        return Err(attach(
            Status::aborted(format!(
                "topic {topic} epoch {topic_epoch} is stale (current epoch {})",
                meta.epoch
            )),
            CoordErrorCode::TopicEpochStale,
        ));
    }
    let partition_u32 = u32::try_from(partition)
        .ok()
        .filter(|value| *value < meta.partitions)
        .ok_or_else(|| {
            attach(
                Status::not_found(format!(
                    "partition {partition} does not exist on topic {topic} \
                     ({} partitions)",
                    meta.partitions
                )),
                CoordErrorCode::PartitionNotFound,
            )
        })?;
    let offset = u64::try_from(offset).map_err(|_| {
        attach(
            Status::invalid_argument(format!("ack offset must not be negative (got {offset})")),
            CoordErrorCode::InvalidArgument,
        )
    })?;
    Ok(MqGroupAckEntry {
        topic_id: meta.topic_id,
        partition: partition_u32,
        offset,
    })
}

fn expect_acked(result: MqGroupResult) -> Result<crate::storage::mq_group::MqGroupAcked, Status> {
    match result {
        MqGroupResult::Acked(acked) => Ok(acked),
        MqGroupResult::Rejected(rejection) => Err(group_rejection_status(&rejection)),
        other => Err(Status::internal(format!(
            "unexpected group result for Ack: {other:?}"
        ))),
    }
}

/// 组命令的用户级拒绝 → 契约错误码（只按码分支）。
pub(crate) fn group_rejection_status(rejection: &MqGroupRejection) -> Status {
    match rejection {
        MqGroupRejection::GroupNotFound => attach(
            Status::not_found("consumer group not found (join the group first)"),
            CoordErrorCode::GroupNotFound,
        ),
        MqGroupRejection::StaleGeneration { current_generation } => attach(
            Status::aborted(format!(
                "generation is stale (current generation {current_generation}); \
                 rejoin the group"
            )),
            CoordErrorCode::GroupEpochStale,
        ),
        MqGroupRejection::NotMember { current_generation } => attach(
            Status::failed_precondition(format!(
                "member is not in the group (current generation {current_generation}); \
                 rejoin the group"
            )),
            CoordErrorCode::NotGroupMember,
        ),
        MqGroupRejection::NotAssigned {
            topic_id,
            partition,
        } => attach(
            Status::failed_precondition(format!(
                "partition {partition} of topic {topic_id} is not assigned to this member"
            )),
            CoordErrorCode::NotGroupMember,
        ),
        MqGroupRejection::Invalid { reason } => attach(
            Status::invalid_argument(reason.clone()),
            CoordErrorCode::InvalidArgument,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use coord_core::error_code::error_code_of;
    use coord_core::storage::StorageBackend;
    use coord_core::types::StorageConfig;
    use tempfile::TempDir;
    use tonic::Code;

    use crate::storage::mq_topic::{MqTopicConfig, MqTopicCreate, MqTopicOp, MqTopicOutcome};
    use crate::storage::mvcc::{AppliedLogId, MvccStorage};
    use crate::storage::redb_backend::RedbBackend;

    fn node_with_topic() -> (TempDir, CoordNode) {
        let dir = TempDir::new().expect("tempdir");
        let backend =
            RedbBackend::open(dir.path(), &StorageConfig::default()).expect("open backend");
        let mvcc = Arc::new(MvccStorage::new(backend).expect("mvcc"));
        let outcome = mvcc
            .apply_mq_topic_op(
                &MqTopicOp::Create(MqTopicCreate {
                    name: "orders".to_string(),
                    partitions: 2,
                    config: MqTopicConfig {
                        retention_secs: 604_800,
                        max_bytes: 0,
                        max_message_bytes: 1024,
                        segment_bytes: 1024 * 1024,
                        dlq_retention_secs: 604_800,
                    },
                    request_id: String::new(),
                    now_ms: 1,
                    log_shards: 2,
                    max_partitions_per_shard: 1024,
                }),
                1,
                AppliedLogId {
                    term: 1,
                    node_id: 1,
                    index: 1,
                },
            )
            .expect("seed topic");
        assert!(matches!(outcome, MqTopicOutcome::Created { .. }));
        let mut node = CoordNode::new(mvcc);
        node.node_id = 1;
        (dir, node)
    }

    #[test]
    fn group_shard_is_stable_and_within_bounds() {
        // 固定期望值：改动哈希实现会让组的位点失去归属（M3 计划「语义决定」）。
        assert_eq!(mq_group_shard("orders", 4), mq_group_shard("orders", 4));
        for group in ["orders", "payments", "group-1", "a", ""] {
            for shards in [1u32, 2, 4, 32] {
                assert!(mq_group_shard(group, shards) < shards, "{group}/{shards}");
            }
        }
        assert_eq!(mq_group_shard("orders", 1), 0);
        // FNV-1a("orders") % 4 —— 固定下来，跨版本不得变化。
        assert_eq!(mq_group_shard("orders", 4), 0);
        assert_eq!(mq_group_shard("payments", 4), 2);
    }

    #[test]
    fn ack_entry_maps_contract_errors_to_stable_codes() {
        let (_dir, node) = node_with_topic();
        let table = node.mq_topic_table().expect("topic table");
        let meta = table.topic("orders").expect("row").clone();

        let unknown = ack_entry(&table, "missing", 0, 0, 0).expect_err("unknown topic");
        assert_eq!(code_of(&unknown).as_deref(), Some("TOPIC_NOT_FOUND"));

        let stale = ack_entry(&table, "orders", 0, meta.epoch + 1, 0).expect_err("stale epoch");
        assert_eq!(code_of(&stale).as_deref(), Some("TOPIC_EPOCH_STALE"));

        for partition in [-1, 2] {
            let out = ack_entry(&table, "orders", partition, 0, 0).expect_err("bad partition");
            assert_eq!(code_of(&out).as_deref(), Some("PARTITION_NOT_FOUND"));
        }

        let negative = ack_entry(&table, "orders", 0, 0, -1).expect_err("negative offset");
        assert_eq!(code_of(&negative).as_deref(), Some("INVALID_ARGUMENT"));

        let ok = ack_entry(&table, "orders", 1, meta.epoch, 7).expect("valid ack entry");
        assert_eq!(ok.topic_id, meta.topic_id);
        assert_eq!(ok.partition, 1);
        assert_eq!(ok.offset, 7);
    }

    #[test]
    fn group_rejections_keep_their_wire_codes() {
        let cases = [
            (
                MqGroupRejection::GroupNotFound,
                Code::NotFound,
                "GROUP_NOT_FOUND",
            ),
            (
                MqGroupRejection::StaleGeneration {
                    current_generation: 3,
                },
                Code::Aborted,
                "GROUP_EPOCH_STALE",
            ),
            (
                MqGroupRejection::NotMember {
                    current_generation: 3,
                },
                Code::FailedPrecondition,
                "NOT_GROUP_MEMBER",
            ),
            (
                MqGroupRejection::NotAssigned {
                    topic_id: 1,
                    partition: 2,
                },
                Code::FailedPrecondition,
                "NOT_GROUP_MEMBER",
            ),
            (
                MqGroupRejection::Invalid {
                    reason: "bad".to_string(),
                },
                Code::InvalidArgument,
                "INVALID_ARGUMENT",
            ),
        ];
        for (rejection, code, wire) in cases {
            let status = group_rejection_status(&rejection);
            assert_eq!(status.code(), code, "{rejection:?}");
            assert_eq!(
                error_code_of(&status).as_deref(),
                Some(wire),
                "{rejection:?}"
            );
        }
    }

    fn code_of(status: &Status) -> Option<String> {
        error_code_of(status)
    }
}
