//! MQ v2 写路径：路由、每分片批量提交、apply 结果回填（ADR-0016 D4；M2-1/M2-2/M2-3）。
//!
//! 语义单一归属：`docs/adr/0016-mq-v2.md`；offset 分配/幂等/配额/段封口的唯一实现是
//! [`crate::storage::mq_log::MvccStorage::apply_mq_append_batch`]（M1-2）。本模块只负责：
//!
//! 1. 把 `(topic, partition)` 解析成日志分片的 Region 运行时（topic 表 → 分片）；
//! 2. 分片 Leader 判定（非 Leader 返回 `NOT_LEADER` + `coord-leader-hint`）；
//! 3. 把同分片的多个请求合成一条 `Command::MqAppendBatch`（**响应即已提交**）；
//! 4. 把 apply 结果/拒绝映射成契约错误码。
//!
//! 批量窗口：D4 的「最多 64 条或 2 ms，先到先发」按「先到先发」落地——**不引入人为
//! linger**。同分片请求经 `ShardBatcher::flush` 的异步锁串行化：持锁者立即取走队列
//! 里已有的请求（≤64 条）合成一条 Raft 条目；Raft 往返期间到达的请求自然积累成下一批。
//! 批深度用 `coord_mq_v2_append_batches_total` / `coord_mq_v2_append_requests_total`
//! 取证（M2-5）。

use std::collections::HashMap;
use std::sync::Arc;

use coord_core::error_code::{attach, CoordErrorCode};
use coord_core::types::RegionId;
use coord_proto::mq::v2 as mq_v2;
use tonic::Status;

use crate::raft::region_runtime::RegionRuntime;
use crate::raft::type_config::{Command, Response as RaftResponse};
use crate::server::CoordNode;
use crate::storage::mq_log::{
    MqAppendBatch, MqAppendEntry, MqAppendOutcome, MqAppendRejection, MqAppendRequest, MqAppended,
};

use super::now_ms;

/// 一条 `Command::MqAppendBatch` 最多承载的请求数（契约：单次 `PublishBatch` ≤ 64 条）。
pub(crate) const MAX_APPEND_REQUESTS: usize = 64;

/// 已入队、等待同一次 Raft 提交的单个请求。
pub(crate) struct PendingAppend {
    pub(crate) request: MqAppendRequest,
    pub(crate) reply: tokio::sync::oneshot::Sender<Result<MqAppended, Status>>,
}

/// 单个日志分片的批处理器。
///
/// `queue` 只保护入队/取批（短临界区）；`flush` 是**异步**锁，持锁者独占该分片的
/// Raft 提案，保证同一分片的提交严格串行（offset 顺序 = 入队顺序），不同分片互不
/// 阻塞。
#[derive(Default)]
pub(crate) struct ShardBatcher {
    queue: parking_lot::Mutex<Vec<PendingAppend>>,
    flush: tokio::sync::Mutex<()>,
    /// 提交完成通知：长轮询（`Poll`）据此在本分片有新消息时立即醒来。
    /// `notify_waiters` 不存 permit，消费端用兜底超时（见 `consume.rs`）避免丢唤醒。
    appended: tokio::sync::Notify,
}

impl ShardBatcher {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn push(&self, pending: PendingAppend) {
        self.queue.lock().push(pending);
    }

    /// 取最多 [`MAX_APPEND_REQUESTS`] 条（FIFO）。
    pub(crate) fn take_batch(&self) -> Vec<PendingAppend> {
        let mut queue = self.queue.lock();
        let take = queue.len().min(MAX_APPEND_REQUESTS);
        queue.drain(..take).collect()
    }

    /// 串行化同分片的 Raft 提案。
    pub(crate) async fn flush(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.flush.lock().await
    }

    /// 等待本分片的下一次提交（长轮询唤醒）。
    pub(crate) async fn appended(&self) {
        self.appended.notified().await;
    }
}

/// 懒创建的「分片 id → 批处理器」表。
///
/// 关闭态不会被创建（门禁在 handler 入口返回）；不启后台任务，进程退出即释放。
#[derive(Default)]
pub(crate) struct PublishBatchers {
    shards: parking_lot::RwLock<HashMap<u32, Arc<ShardBatcher>>>,
}

impl PublishBatchers {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn shard(&self, shard_id: u32) -> Arc<ShardBatcher> {
        if let Some(batcher) = self.shards.read().get(&shard_id) {
            return Arc::clone(batcher);
        }
        let mut shards = self.shards.write();
        Arc::clone(
            shards
                .entry(shard_id)
                .or_insert_with(|| Arc::new(ShardBatcher::new())),
        )
    }
}

/// 一次 publish 的路由与上限（topic 配置已展开，apply 直接可用）。
pub(crate) struct MqPartitionTarget {
    pub(crate) topic_name: String,
    pub(crate) topic_id: u64,
    pub(crate) partition: u32,
    pub(crate) shard_id: u32,
    pub(crate) region_id: RegionId,
    pub(crate) runtime: Arc<RegionRuntime>,
    pub(crate) max_message_bytes: u32,
    pub(crate) max_bytes: u64,
    pub(crate) segment_bytes: u64,
}

impl std::fmt::Debug for MqPartitionTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqPartitionTarget")
            .field("topic_name", &self.topic_name)
            .field("topic_id", &self.topic_id)
            .field("partition", &self.partition)
            .field("shard_id", &self.shard_id)
            .field("region_id", &self.region_id)
            .field("max_message_bytes", &self.max_message_bytes)
            .field("max_bytes", &self.max_bytes)
            .field("segment_bytes", &self.segment_bytes)
            .finish_non_exhaustive()
    }
}

impl CoordNode {
    /// 解析写目标：topic 存在 / 代际匹配 / 分区范围 / 分区所属分片的运行时。
    ///
    /// 只读 region 0 的 topic 表与 Region 路由表，不读分片状态、不提案。
    pub(crate) fn mq_partition_target(
        &self,
        topic: &str,
        partition: i32,
        topic_epoch: u64,
    ) -> Result<MqPartitionTarget, Status> {
        let table = self.mq_topic_table()?;
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
        let shard_id = meta
            .shard_of_partition
            .get(partition_u32 as usize)
            .copied()
            .ok_or_else(|| {
                Status::internal(format!(
                    "topic {topic} is missing the shard mapping of partition {partition_u32}"
                ))
            })?;
        let runtime = self.mq_shard_runtime(shard_id)?;
        let region_id = runtime.region_id();
        Ok(MqPartitionTarget {
            topic_name: meta.name.clone(),
            topic_id: meta.topic_id,
            partition: partition_u32,
            shard_id,
            region_id,
            runtime,
            max_message_bytes: meta.config.max_message_bytes,
            max_bytes: meta.config.max_bytes,
            segment_bytes: meta.config.segment_bytes,
        })
    }

    /// 把一条分区请求排进分片 batcher，等一次 Raft 提交 + apply 的结果。
    ///
    /// 返回即已提交（`client_write` 在多数派提交并 apply 后才返回）。指标在这一层
    /// 记录：请求数/条数/字节/幂等命中/延迟与拒绝码都在同一次调用内闭合。
    pub(crate) async fn mq_append(
        &self,
        target: &MqPartitionTarget,
        entries: Vec<MqAppendEntry>,
    ) -> Result<MqAppended, Status> {
        let started = std::time::Instant::now();
        let entry_count = entries.len() as u64;
        let bytes = entries
            .iter()
            .map(|entry| (entry.key.len() + entry.payload.len()) as u64)
            .sum::<u64>();
        let result = self.mq_append_committed(target, entries).await;
        if let Some(metrics) = self.metrics() {
            let latency_us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
            match &result {
                Ok(appended) => {
                    let deduplicated = appended
                        .entries
                        .iter()
                        .filter(|entry| entry.deduplicated)
                        .count() as u64;
                    metrics.record_mq_v2_publish(entry_count, bytes, deduplicated, latency_us);
                }
                Err(status) => metrics.record_mq_v2_publish_error(
                    coord_core::error_code::error_code_of(status).as_deref(),
                    latency_us,
                ),
            }
        }
        result
    }

    async fn mq_append_committed(
        &self,
        target: &MqPartitionTarget,
        entries: Vec<MqAppendEntry>,
    ) -> Result<MqAppended, Status> {
        self.mq_ensure_shard_leader(target).await?;
        let request = MqAppendRequest {
            topic_id: target.topic_id,
            partition: target.partition,
            max_message_bytes: target.max_message_bytes,
            max_bytes: target.max_bytes,
            segment_bytes: target.segment_bytes,
            entries,
        };
        let batcher = self.mq_v2_batchers().shard(target.shard_id);
        let (reply, response) = tokio::sync::oneshot::channel();
        batcher.push(PendingAppend { request, reply });
        // 持 flush 锁直到队列耗尽：排在同一分片后续的请求由本次或下一次 flush 覆盖，
        // 不会滞留在队列里（调用方本身也在锁上排队）。
        self.mq_flush_shard(&batcher, target).await;
        match response.await {
            Ok(result) => result,
            Err(_) => Err(attach(
                Status::unavailable(
                    "mq publish batch was cancelled before the commit completed; retry",
                ),
                CoordErrorCode::Unavailable,
            )),
        }
    }

    /// 非 Leader 快速失败（hint 指向该 Region 的当前 Leader）。
    ///
    /// 与真实提案之间的领导权变化由 `client_write` 的 `ForwardToLeader` 兜底。
    pub(crate) async fn mq_ensure_shard_leader(
        &self,
        target: &MqPartitionTarget,
    ) -> Result<(), Status> {
        let leader = target.runtime.raft.current_leader().await;
        if leader == Some(self.node_id) {
            return Ok(());
        }
        Err(attach(
            crate::server::map_err(coord_core::error::Error::RegionNotLeader {
                region_id: target.region_id,
                leader_addr: leader.and_then(|id| self.grpc_addr_of(id)),
            }),
            CoordErrorCode::NotLeader,
        ))
    }

    /// 取批 → 提案 → 回填结果，直到队列为空。
    async fn mq_flush_shard(&self, batcher: &ShardBatcher, target: &MqPartitionTarget) {
        let _guard = batcher.flush().await;
        loop {
            let batch = batcher.take_batch();
            if batch.is_empty() {
                break;
            }
            let requests_in_batch = batch.len() as u64;
            let (requests, replies): (Vec<_>, Vec<_>) = batch
                .into_iter()
                .map(|pending| (pending.request, pending.reply))
                .unzip();
            let command = Command::MqAppendBatch(MqAppendBatch {
                shard_id: target.shard_id,
                now_ms: now_ms(),
                requests,
            });
            let result = self
                .client_write_with_timeout(&target.runtime.raft, command, target.region_id)
                .await;
            if let Some(metrics) = self.metrics() {
                metrics.record_mq_v2_append_batch(requests_in_batch);
            }
            let outcomes = match result {
                Ok(response) => match response.response() {
                    RaftResponse::MqAppendBatch { response } => {
                        if response.requests.len() == replies.len() {
                            Ok(response.requests.clone())
                        } else {
                            Err(Status::internal(format!(
                                "mq append batch applied {} requests but {} were submitted",
                                response.requests.len(),
                                replies.len()
                            )))
                        }
                    }
                    other => Err(Status::internal(format!(
                        "unexpected raft response for MqAppendBatch: {other:?}"
                    ))),
                },
                Err(status) => Err(status),
            };
            let committed = outcomes.is_ok();
            match outcomes {
                Ok(outcomes) => {
                    for (reply, outcome) in replies.into_iter().zip(outcomes) {
                        let _ = reply.send(match outcome {
                            MqAppendOutcome::Applied(appended) => Ok(appended),
                            MqAppendOutcome::Rejected(rejection) => {
                                Err(append_rejection_status(&rejection))
                            }
                        });
                    }
                }
                Err(status) => {
                    // 领导权变化/无 quorum：整批失败，绝不让某个请求「半个成功」。
                    for reply in replies {
                        let _ = reply.send(Err(status.clone()));
                    }
                }
            }
            if committed {
                // 唤醒本分片的长轮询（`notify_waiters` 不排队 permit，Poll 侧有兜底）。
                batcher.appended.notify_waiters();
            }
        }
    }
}

/// `PublishBatch` 的条数上限（契约 1–64 条）。
pub(crate) fn validate_batch_entries(len: usize) -> Result<(), Status> {
    if len == 0 || len > MAX_APPEND_REQUESTS {
        return Err(attach(
            Status::invalid_argument(format!(
                "PublishBatch accepts 1..={MAX_APPEND_REQUESTS} entries (got {len})"
            )),
            CoordErrorCode::InvalidArgument,
        ));
    }
    Ok(())
}

/// apply 的整批拒绝 → 契约错误码（`MESSAGE_TOO_LARGE` / `QUOTA_EXCEEDED`）。
pub(crate) fn append_rejection_status(rejection: &MqAppendRejection) -> Status {
    match rejection {
        MqAppendRejection::MessageTooLarge { entry, size, limit } => attach(
            Status::invalid_argument(format!(
                "message entry {entry} encodes to {size} bytes, exceeding \
                 max_message_bytes {limit}; the whole batch was not appended"
            )),
            CoordErrorCode::MessageTooLarge,
        ),
        MqAppendRejection::QuotaExceeded {
            used,
            requested,
            limit,
        } => attach(
            Status::resource_exhausted(format!(
                "topic quota exceeded: used {used} + requested {requested} > limit \
                 {limit}; the whole batch was not appended"
            )),
            CoordErrorCode::QuotaExceeded,
        ),
    }
}

/// apply 的单条结果 → 契约响应（offset 超出 int64 时显式失败，不回绕）。
pub(crate) fn publish_response(
    entry: &crate::storage::mq_log::MqAppendEntryResult,
) -> Result<mq_v2::PublishResponse, Status> {
    Ok(mq_v2::PublishResponse {
        offset: i64::try_from(entry.offset).map_err(|_| {
            Status::internal(format!(
                "offset {} exceeds the int64 wire range",
                entry.offset
            ))
        })?,
        timestamp_ms: entry.timestamp_ms,
        deduplicated: entry.deduplicated,
    })
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

    use crate::mq_v2::{MqV2RuntimeConfig, MqV2State};
    use crate::storage::mq_log::MqAppendEntryResult;
    use crate::storage::mq_topic::{MqTopicConfig, MqTopicCreate, MqTopicOp};
    use crate::storage::mvcc::{AppliedLogId, MvccStorage};
    use crate::storage::redb_backend::RedbBackend;

    fn node() -> (TempDir, CoordNode) {
        let dir = TempDir::new().expect("tempdir");
        let backend =
            RedbBackend::open(dir.path(), &StorageConfig::default()).expect("open backend");
        let mvcc = Arc::new(MvccStorage::new(backend).expect("mvcc"));
        let mut node = CoordNode::new(mvcc);
        node.node_id = 1;
        node.set_mq_v2_state(MqV2State::new(
            true,
            MqV2RuntimeConfig {
                log_shards: 2,
                group_shards: 1,
                ..MqV2RuntimeConfig::default()
            },
        ));
        (dir, node)
    }

    fn seed_topic(node: &CoordNode, name: &str, partitions: u32) {
        node.storage
            .apply_mq_topic_op(
                &MqTopicOp::Create(MqTopicCreate {
                    name: name.to_string(),
                    partitions,
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
    }

    #[test]
    fn publish_target_rejects_unknown_topic() {
        let (_dir, node) = node();
        let status = node
            .mq_partition_target("missing", 0, 0)
            .expect_err("unknown topic must be rejected");
        assert_eq!(status.code(), Code::NotFound);
        assert_eq!(
            error_code_of(&status).as_deref(),
            Some("TOPIC_NOT_FOUND"),
            "{status:?}"
        );
    }

    #[test]
    fn publish_target_rejects_a_stale_epoch() {
        let (_dir, node) = node();
        seed_topic(&node, "orders", 2);
        let meta = node
            .mq_topic_table()
            .expect("table")
            .topic("orders")
            .expect("row")
            .clone();
        let status = node
            .mq_partition_target("orders", 0, meta.epoch + 1)
            .expect_err("stale epoch must be rejected");
        assert_eq!(status.code(), Code::Aborted);
        assert_eq!(
            error_code_of(&status).as_deref(),
            Some("TOPIC_EPOCH_STALE"),
            "{status:?}"
        );
    }

    #[test]
    fn publish_target_rejects_an_out_of_range_partition() {
        let (_dir, node) = node();
        seed_topic(&node, "orders", 2);
        for partition in [-1, 2] {
            let status = node
                .mq_partition_target("orders", partition, 0)
                .expect_err("partition must be rejected");
            assert_eq!(status.code(), Code::NotFound, "partition {partition}");
            assert_eq!(
                error_code_of(&status).as_deref(),
                Some("PARTITION_NOT_FOUND"),
                "{status:?}"
            );
        }
    }

    #[test]
    fn publish_target_requires_multi_raft_assembly() {
        let (_dir, node) = node();
        seed_topic(&node, "orders", 2);
        let status = node
            .mq_partition_target("orders", 0, 0)
            .expect_err("missing region assembly must fail closed");
        assert_eq!(status.code(), Code::FailedPrecondition);
        assert_eq!(
            error_code_of(&status).as_deref(),
            Some("FAILED_PRECONDITION")
        );
    }

    #[test]
    fn take_batch_is_fifo_and_capped_at_64() {
        let batcher = ShardBatcher::new();
        for index in 0..70u32 {
            let (reply, _rx) = tokio::sync::oneshot::channel();
            batcher.push(PendingAppend {
                request: MqAppendRequest {
                    topic_id: 1,
                    partition: index,
                    max_message_bytes: 0,
                    max_bytes: 0,
                    segment_bytes: 0,
                    entries: Vec::new(),
                },
                reply,
            });
        }
        let first = batcher.take_batch();
        assert_eq!(first.len(), MAX_APPEND_REQUESTS);
        assert!(
            first
                .iter()
                .enumerate()
                .all(|(index, pending)| pending.request.partition == index as u32),
            "batch must drain in FIFO order"
        );
        let second = batcher.take_batch();
        assert_eq!(second.len(), 70 - MAX_APPEND_REQUESTS);
        assert!(batcher.take_batch().is_empty());
    }

    #[test]
    fn rejection_statuses_keep_their_wire_codes() {
        let too_large = append_rejection_status(&MqAppendRejection::MessageTooLarge {
            entry: 0,
            size: 2048,
            limit: 1024,
        });
        assert_eq!(too_large.code(), Code::InvalidArgument);
        assert_eq!(
            error_code_of(&too_large).as_deref(),
            Some("MESSAGE_TOO_LARGE")
        );

        let quota = append_rejection_status(&MqAppendRejection::QuotaExceeded {
            used: 10,
            requested: 5,
            limit: 12,
        });
        assert_eq!(quota.code(), Code::ResourceExhausted);
        assert_eq!(error_code_of(&quota).as_deref(), Some("QUOTA_EXCEEDED"));
    }

    #[test]
    fn batch_entries_are_limited_to_64() {
        assert!(validate_batch_entries(1).is_ok());
        assert!(validate_batch_entries(MAX_APPEND_REQUESTS).is_ok());
        for len in [0, MAX_APPEND_REQUESTS + 1] {
            let status = validate_batch_entries(len).expect_err("must reject");
            assert_eq!(status.code(), Code::InvalidArgument);
            assert_eq!(error_code_of(&status).as_deref(), Some("INVALID_ARGUMENT"));
        }
    }

    #[test]
    fn publish_response_rejects_offsets_beyond_int64() {
        let status = publish_response(&MqAppendEntryResult {
            offset: u64::MAX,
            timestamp_ms: 1,
            deduplicated: false,
        })
        .expect_err("offset must not wrap");
        assert_eq!(status.code(), Code::Internal);
    }
}
