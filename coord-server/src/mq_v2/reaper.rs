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
use crate::storage::mq_log::{mq_retention_cutoff, MqTruncate};

/// 回收扫描周期。
///
/// 段年龄的最小粒度是秒；30s 让「过期整段」在一个周期内被回收，又不给分片 Leader
/// 带来持续写入压力（无到期段不提案）。该值不是契约面，调参不需要配置键。
pub const MQ_V2_REAPER_INTERVAL: Duration = Duration::from_secs(30);

/// 单次回收扫描的统计（日志/测试断言用）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MqReaperStats {
    pub shards_scanned: u32,
    pub partitions_scanned: u32,
    pub truncate_proposals: u32,
    pub dropped_segments: u64,
    pub dropped_bytes: u64,
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
        stats
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
