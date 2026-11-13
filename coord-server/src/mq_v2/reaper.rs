//! 保留回收循环（ADR-0016 D7；M1-5 的段级 `MqTruncate` 的周期入口）。
//!
//! 只在**分片 Leader** 上提案（非 Leader 直接跳过，由新 Leader 在下一个周期接手）；
//! `now_ms` 与保留期都来自命令外的输入：段边界与 `opened_at_ms` 是 apply 写入的
//! 确定性状态，topic 保留期读 region 0 的权威表。没有到期段时**不提案**，不给
//! Raft 增加空写入。
//!
//! 关闭态（`[mq_v2].enabled=false`）零副作用：`mq_v2_reaper_pass` 直接返回空统计，
//! `main.rs` 也不启动周期任务（ADR-0001）。

use std::time::Duration;

use crate::raft::type_config::{Command, Response as RaftResponse};
use crate::server::CoordNode;
use crate::storage::mq_group::{
    MqGroupDropTopic, MqGroupGc, MqGroupOp, MqGroupReap, MqGroupResult,
};
use crate::storage::mq_log::{mq_retention_cutoff, MqDropPartition, MqTruncate};
use crate::storage::mq_topic::{MqTopicDeletionProgress, MqTopicOp};

/// 回收扫描周期。
///
/// 段年龄的最小粒度是秒；30s 让「过期整段」在一个周期内被回收，又不给分片 Leader
/// 带来持续写入压力（无到期段不提案）。该值不是契约面，调参不需要配置键。
pub const MQ_V2_REAPER_INTERVAL: Duration = Duration::from_secs(30);

/// 空组位点的保留期（组 TTL）：7 天没有成员、且位点 `updated_at_ms` 早于
/// `now_ms - 7d` 的组在 `MqGroupOp::Gc` 中回收。值不是契约面（后续可按需加配置键）。
pub const GROUP_OFFSET_TTL_SECS: u64 = 604_800;

/// 单次回收扫描的统计（日志/测试断言用）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MqReaperStats {
    pub shards_scanned: u32,
    pub partitions_scanned: u32,
    pub truncate_proposals: u32,
    pub dropped_segments: u64,
    pub dropped_bytes: u64,
    pub group_shards_scanned: u32,
    pub group_members_expired: u32,
    pub groups_removed: u32,
    pub group_offsets_removed: u32,
    pub partitions_dropped: u32,
    pub deletion_messages_removed: u64,
    pub deletion_bytes_reclaimed: u64,
}

impl CoordNode {
    /// 单次回收扫描：对本节点为 Leader 的每个日志分片，按 topic 保留期整段回收。
    ///
    /// 已删除 topic（不在 `topics` 里，只在 `deletions` 里）不在这里回收：数据回收
    /// 归 `DeleteTopic` 的异步流程（M4-2）。DLQ 的保留期随 M4-1。
    pub async fn mq_v2_reaper_pass(&self, now_ms: i64) -> MqReaperStats {
        let mut stats = MqReaperStats::default();
        if !self.mq_v2().is_enabled() {
            return stats;
        }
        let Ok(table) = self.mq_topic_table() else {
            return stats;
        };
        let log_shards = self.mq_v2().config().log_shards;
        for shard_id in 0..log_shards {
            let Ok(runtime) = self.mq_shard_runtime(shard_id) else {
                continue;
            };
            if runtime.raft.current_leader().await != Some(self.node_id) {
                continue;
            }
            stats.shards_scanned = stats.shards_scanned.saturating_add(1);
            let Ok(partitions) = runtime.mvcc.mq_partitions() else {
                continue;
            };
            let region_id = runtime.region_id();
            for (topic_id, partition) in partitions {
                let Some(topic) = table.topic_by_id(topic_id) else {
                    continue;
                };
                let retention_secs = topic.config.retention_secs;
                if retention_secs == 0 {
                    continue;
                }
                stats.partitions_scanned = stats.partitions_scanned.saturating_add(1);
                let Ok(Some(meta)) = runtime.mvcc.mq_partition_meta(topic_id, partition) else {
                    continue;
                };
                let Some(up_to_offset) =
                    mq_retention_cutoff(&meta.segments, now_ms, retention_secs)
                else {
                    continue;
                };
                let command = Command::MqTruncate(MqTruncate {
                    shard_id,
                    topic_id,
                    partition,
                    up_to_offset,
                });
                match self
                    .client_write_with_timeout(&runtime.raft, command, region_id)
                    .await
                {
                    Ok(response) => {
                        if let RaftResponse::MqTruncate { result } = response.response() {
                            stats.truncate_proposals = stats.truncate_proposals.saturating_add(1);
                            stats.dropped_segments = stats
                                .dropped_segments
                                .saturating_add(result.dropped_segments);
                            stats.dropped_bytes =
                                stats.dropped_bytes.saturating_add(result.dropped_bytes);
                        }
                    }
                    // 领导权变化/无 quorum：下一个周期重试；不能把回收失败变成
                    // 分片停止服务（回收是后台维护，不是写路径）。
                    Err(status) => tracing::warn!(
                        shard_id,
                        topic_id,
                        partition,
                        error = %status,
                        "mq_v2 retention truncate proposal failed; retrying next interval"
                    ),
                }
            }
        }
        if let Some(metrics) = self.metrics() {
            metrics.record_mq_v2_retention(
                u64::from(stats.truncate_proposals),
                stats.dropped_segments,
            );
        }
        self.mq_v2_group_reap_pass(now_ms, &mut stats).await;
        self.mq_v2_deletion_pass(&mut stats).await;
        if let Some(metrics) = self.metrics() {
            metrics.record_mq_v2_deletion(
                u64::from(stats.partitions_dropped),
                stats.deletion_messages_removed,
                stats.deletion_bytes_reclaimed,
            );
        }
        if let Some(metrics) = self.metrics() {
            metrics.record_mq_v2_group_reap(
                u64::from(stats.group_members_expired),
                u64::from(stats.groups_removed),
            );
        }
        stats
    }

    /// 删除回收：对每个 Pending 的删除，清理组位点（各 group shard Leader）并逐个
    /// 分区提案 `MqDropPartition`（日志分片 Leader），成功后推进
    /// `MqTopicOp::DeletionProgress`（region 0）。
    ///
    /// 失败/非 Leader 的分区留在 `pending_partitions`，下一个周期重试；
    /// `GetTopicDeletion` 因此不会提前报告 Done。计数是 best-effort：若 drop 成功而
    /// 进度提案失败，下一轮的重放 drop 返回 0 计数（进度字段可能少算，不影响回收）。
    async fn mq_v2_deletion_pass(&self, stats: &mut MqReaperStats) {
        let Ok(table) = self.mq_topic_table() else {
            return;
        };
        let deletions: Vec<crate::storage::mq_topic::MqTopicDeletion> = table
            .deletions
            .iter()
            .filter(|deletion| {
                deletion.state == crate::storage::mq_topic::MqTopicDeletionState::Pending
            })
            .cloned()
            .collect();
        let group_shards = self.mq_v2().config().group_shards;
        for deletion in deletions {
            // 组位点清理：每个 group shard 的 Leader 清自己的分片（幂等，逐轮重试）。
            for shard_id in 0..group_shards {
                let Ok(runtime) = self.mq_group_shard_runtime(shard_id) else {
                    continue;
                };
                if runtime.raft.current_leader().await != Some(self.node_id) {
                    continue;
                }
                if let Err(status) = self
                    .mq_propose_group(MqGroupOp::DropTopic(MqGroupDropTopic {
                        shard_id,
                        topic_id: deletion.topic_id,
                    }))
                    .await
                {
                    tracing::warn!(
                        shard_id,
                        topic_id = deletion.topic_id,
                        error = %status,
                        "mq_v2 topic-offset cleanup failed; retrying next interval"
                    );
                }
            }
            for partition in deletion.pending_partitions.clone() {
                let shard_id = deletion
                    .shard_of_partition
                    .get(partition as usize)
                    .copied()
                    .unwrap_or(0);
                let Ok(runtime) = self.mq_shard_runtime(shard_id) else {
                    continue;
                };
                if runtime.raft.current_leader().await != Some(self.node_id) {
                    continue;
                }
                let region_id = runtime.region_id();
                let command = Command::MqDropPartition(MqDropPartition {
                    shard_id,
                    topic_id: deletion.topic_id,
                    partition,
                });
                let response = match self
                    .client_write_with_timeout(&runtime.raft, command, region_id)
                    .await
                {
                    Ok(response) => response,
                    Err(status) => {
                        tracing::warn!(
                            shard_id,
                            topic_id = deletion.topic_id,
                            partition,
                            error = %status,
                            "mq_v2 partition drop failed; retrying next interval"
                        );
                        continue;
                    }
                };
                let result = match response.response() {
                    RaftResponse::MqDropPartition { result } => *result,
                    other => {
                        tracing::warn!(
                            shard_id,
                            partition,
                            ?other,
                            "mq_v2 partition drop returned an unexpected response"
                        );
                        continue;
                    }
                };
                stats.partitions_dropped = stats.partitions_dropped.saturating_add(1);
                stats.deletion_messages_removed = stats
                    .deletion_messages_removed
                    .saturating_add(result.records_removed);
                stats.deletion_bytes_reclaimed = stats
                    .deletion_bytes_reclaimed
                    .saturating_add(result.bytes_freed);

                let Some(raft) = self.raft.as_ref() else {
                    continue;
                };
                let progress =
                    Command::MqTopic(MqTopicOp::DeletionProgress(MqTopicDeletionProgress {
                        delete_id: deletion.delete_id,
                        partition,
                        messages_removed: result.records_removed,
                        bytes_reclaimed: result.bytes_freed,
                    }));
                if let Err(status) = self.client_write_with_timeout(raft, progress, 0).await {
                    tracing::warn!(
                        delete_id = deletion.delete_id,
                        partition,
                        error = %status,
                        "mq_v2 deletion progress write failed; retrying next interval"
                    );
                }
            }
        }
    }

    /// 组会话过期（`MqGroupOp::Reap`）+ 空组位点 TTL（`MqGroupOp::Gc`），逐 group
    /// shard 只在 Leader 上提案。两个命令都用同一个 `now_ms`（apply 不读时钟）。
    async fn mq_v2_group_reap_pass(&self, now_ms: i64, stats: &mut MqReaperStats) {
        let group_shards = self.mq_v2().config().group_shards;
        let offset_ttl_ms = GROUP_OFFSET_TTL_SECS.saturating_mul(1_000);
        for shard_id in 0..group_shards {
            let Ok(runtime) = self.mq_group_shard_runtime(shard_id) else {
                continue;
            };
            if runtime.raft.current_leader().await != Some(self.node_id) {
                continue;
            }
            stats.group_shards_scanned = stats.group_shards_scanned.saturating_add(1);
            match self
                .mq_propose_group(MqGroupOp::Reap(MqGroupReap { shard_id, now_ms }))
                .await
            {
                Ok(MqGroupResult::Reaped(reaped)) => {
                    stats.group_members_expired = stats
                        .group_members_expired
                        .saturating_add(reaped.members_expired);
                }
                Ok(other) => tracing::warn!(
                    shard_id,
                    ?other,
                    "mq_v2 group reap returned an unexpected result"
                ),
                Err(status) => tracing::warn!(
                    shard_id,
                    error = %status,
                    "mq_v2 group reap proposal failed; retrying next interval"
                ),
            }
            match self
                .mq_propose_group(MqGroupOp::Gc(MqGroupGc {
                    shard_id,
                    now_ms,
                    offset_ttl_ms,
                }))
                .await
            {
                Ok(MqGroupResult::Gced(gc)) => {
                    stats.groups_removed = stats.groups_removed.saturating_add(gc.groups_removed);
                    stats.group_offsets_removed = stats
                        .group_offsets_removed
                        .saturating_add(gc.offsets_removed);
                }
                Ok(other) => tracing::warn!(
                    shard_id,
                    ?other,
                    "mq_v2 group gc returned an unexpected result"
                ),
                Err(status) => tracing::warn!(
                    shard_id,
                    error = %status,
                    "mq_v2 group gc proposal failed; retrying next interval"
                ),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use coord_core::storage::StorageBackend;
    use coord_core::types::StorageConfig;
    use tempfile::TempDir;

    use crate::storage::mvcc::MvccStorage;
    use crate::storage::redb_backend::RedbBackend;

    /// 关闭态零副作用：不读 topic 表、不路由分片、不提案。
    #[tokio::test]
    async fn disabled_state_scans_nothing() {
        let dir = TempDir::new().expect("tempdir");
        let backend =
            RedbBackend::open(dir.path(), &StorageConfig::default()).expect("open backend");
        let node = CoordNode::new(Arc::new(MvccStorage::new(backend).expect("mvcc")));
        assert_eq!(
            node.mq_v2_reaper_pass(1_000).await,
            MqReaperStats::default()
        );
    }
}
