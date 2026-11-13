//! MQ v2 topic 管理面接线（ADR-0016 D10；M2 只交付写路径需要的两条 RPC）。
//!
//! 语义单一归属：`docs/adr/0016-mq-v2.md`；命令载荷与表结构在
//! [`crate::storage::mq_topic`]（M1-4 已落地的状态机）。本模块只做两件事：
//!
//! 1. 把契约请求展开成**确定性**的 `MqTopicOp::Create` 命令（0 值口径见
//!    [`CoordNode::mq_expand_topic_config`]），由 region 0 的 Raft 提交；
//! 2. 把分区 → 日志分片 → 分片 Leader 的现状读出来（`GetTopicLeader`），供客户端
//!    在写入前刷新路由。
//!
//! `DescribeTopic`/`ListTopics`/`UpdateTopic`/`DeleteTopic`/`GetTopicDeletion` 属
//! M4-2/M4-3，本模块不接线（门禁放行后落 `UNIMPLEMENTED`，见 `services.rs`）。

use coord_core::error_code::{attach, CoordErrorCode};
use coord_proto::mq::v2 as mq_v2;
use tonic::Status;

use crate::raft::type_config::Command;
use crate::raft::WatchReceiver;
use crate::server::{map_err, CoordNode};
use crate::storage::mq_topic::{
    MqTopicConfig, MqTopicCreate, MqTopicMeta, MqTopicOp, MqTopicTable,
};

use super::now_ms;

/// topic 保留期 / DLQ 保留期的上界（30 天，契约 `TopicConfig.retention_secs`）。
const RETENTION_MAX_SECS: u64 = 2_592_000;
/// topic 保留期的下界（1 小时）。
const RETENTION_MIN_SECS: u64 = 3_600;
/// 单条消息上限的契约上界（8 MiB）。
const MESSAGE_MAX_BYTES: u32 = 8 * 1024 * 1024;
/// 段封口大小的边界（与 `[mq_v2].segment_bytes` 的配置校验逐字一致）。
const SEGMENT_MIN_BYTES: u64 = 1024 * 1024;
const SEGMENT_MAX_BYTES: u64 = 1024 * 1024 * 1024;

fn invalid(reason: impl Into<String>) -> Status {
    attach(
        Status::invalid_argument(reason.into()),
        CoordErrorCode::InvalidArgument,
    )
}

fn topic_not_found(name: &str) -> Status {
    attach(
        Status::not_found(format!("topic not found: {name}")),
        CoordErrorCode::TopicNotFound,
    )
}

impl CoordNode {
    /// 读 region 0 权威 topic 表（缺失 = 空表）。
    pub(crate) fn mq_topic_table(&self) -> Result<MqTopicTable, Status> {
        self.storage.mq_topic_table().map_err(map_err)
    }

    /// 把契约 `TopicConfig` 展开成表里保存的具体值。
    ///
    /// 0 值口径（契约 + `config.example.toml` 的合解读法，见 M2 计划 §语义决定 2）：
    /// - `retention_secs = 0` → `[mq_v2].default_retention_secs`（合法区间 1h–30d）；
    /// - `max_message_bytes = 0` → `[mq_v2].max_message_bytes`（上限 8 MiB）；
    /// - `segment_bytes = 0` → `[mq_v2].segment_bytes`（1 MiB–1 GiB）；
    /// - `dlq_retention_secs = 0` → 展开后的 `retention_secs`；
    /// - `max_bytes`：请求**未带** `config` 时 → `[mq_v2].max_topic_bytes`（配置项
    ///   语义是「topic 默认容量配额」）；带了 `config` 且 `max_bytes = 0` → 0 = 不限
    ///   （proto `TopicConfig.max_bytes` 的明文语义）。
    pub(crate) fn mq_expand_topic_config(
        &self,
        requested: Option<mq_v2::TopicConfig>,
    ) -> Result<MqTopicConfig, Status> {
        let defaults = self.mq_v2().config();
        let config_supplied = requested.is_some();
        let requested = requested.unwrap_or_default();

        let retention_secs = if requested.retention_secs == 0 {
            defaults.default_retention_secs
        } else {
            u64::try_from(requested.retention_secs)
                .map_err(|_| invalid("retention_secs must not be negative"))?
        };
        if !(RETENTION_MIN_SECS..=RETENTION_MAX_SECS).contains(&retention_secs) {
            return Err(invalid(format!(
                "retention_secs must be in {RETENTION_MIN_SECS}..={RETENTION_MAX_SECS} \
                 seconds (1h–30d, got {retention_secs})"
            )));
        }

        let dlq_retention_secs = if requested.dlq_retention_secs == 0 {
            retention_secs
        } else {
            let value = u64::try_from(requested.dlq_retention_secs)
                .map_err(|_| invalid("dlq_retention_secs must not be negative"))?;
            if value > RETENTION_MAX_SECS {
                return Err(invalid(format!(
                    "dlq_retention_secs must be <= {RETENTION_MAX_SECS} seconds (30d, \
                     got {value})"
                )));
            }
            value
        };

        let max_message_bytes = if requested.max_message_bytes == 0 {
            defaults.max_message_bytes
        } else {
            requested.max_message_bytes
        };
        if !(1..=MESSAGE_MAX_BYTES).contains(&max_message_bytes) {
            return Err(invalid(format!(
                "max_message_bytes must be in 1..={MESSAGE_MAX_BYTES} (1B–8MiB, got \
                 {max_message_bytes})"
            )));
        }

        let segment_bytes = if requested.segment_bytes == 0 {
            defaults.segment_bytes
        } else {
            requested.segment_bytes
        };
        if !(SEGMENT_MIN_BYTES..=SEGMENT_MAX_BYTES).contains(&segment_bytes) {
            return Err(invalid(format!(
                "segment_bytes must be in {SEGMENT_MIN_BYTES}..={SEGMENT_MAX_BYTES} \
                 (1MiB–1GiB, got {segment_bytes})"
            )));
        }

        let max_bytes = if config_supplied {
            requested.max_bytes
        } else {
            defaults.max_topic_bytes
        };

        Ok(MqTopicConfig {
            retention_secs,
            max_bytes,
            max_message_bytes,
            segment_bytes,
            dlq_retention_secs,
        })
    }

    /// 构造 `Command::MqTopic(MqTopicOp::Create)`。
    ///
    /// `now_ms` 与 `log_shards`/`max_partitions_per_shard` 随命令携带：apply 不读
    /// 本地时钟/配置（ADR-0016 D10 的确定性要求）。
    pub(crate) fn mq_topic_create_command(
        &self,
        req: &mq_v2::CreateTopicRequest,
    ) -> Result<Command, Status> {
        let config = self.mq_expand_topic_config(req.config.clone())?;
        let partitions = u32::try_from(req.partitions)
            .map_err(|_| invalid("partitions must not be negative"))?;
        let cfg = self.mq_v2().config();
        Ok(Command::MqTopic(MqTopicOp::Create(MqTopicCreate {
            name: req.topic.clone(),
            partitions,
            config,
            request_id: req.request_id.clone(),
            now_ms: now_ms(),
            log_shards: cfg.log_shards,
            max_partitions_per_shard: cfg.max_partitions_per_shard,
        })))
    }

    /// 分区的日志分片 + Leader 快照（`GetTopicLeader` 的响应体）。
    ///
    /// `earliest_offset`/`next_offset` 读的是**本节点**分片副本的已 apply 状态：
    /// 该 RPC 是路由发现用途，不是一致性读（契约只承诺给客户端一个可刷新的
    /// Leader/分片视图）。
    pub(crate) fn mq_partition_leaders(&self, meta: &MqTopicMeta) -> Vec<mq_v2::PartitionLeader> {
        let mut leaders = Vec::with_capacity(meta.partitions as usize);
        for partition in 0..meta.partitions {
            let shard_id = meta
                .shard_of_partition
                .get(partition as usize)
                .copied()
                .unwrap_or(0);
            let mut leader_addr = String::new();
            let mut earliest_offset = 0i64;
            let mut next_offset = 0i64;
            if let Ok(runtime) = self.mq_shard_runtime(shard_id) {
                let leader = runtime.raft.metrics().borrow_watched().current_leader;
                leader_addr = leader
                    .and_then(|id| self.grpc_addr_of(id))
                    .unwrap_or_default();
                if let Ok(Some(bounds)) = runtime.mvcc.mq_partition_bounds(meta.topic_id, partition)
                {
                    earliest_offset = i64::try_from(bounds.earliest_offset).unwrap_or(i64::MAX);
                    next_offset = i64::try_from(bounds.next_offset).unwrap_or(i64::MAX);
                }
            }
            leaders.push(mq_v2::PartitionLeader {
                partition: partition as i32,
                shard_id,
                leader_addr,
                earliest_offset,
                next_offset,
            });
        }
        leaders
    }
}

/// `CreateTopic` 的 outcome → gRPC 响应/错误。
pub(crate) fn create_topic_response(
    topic: &str,
    outcome: &crate::storage::mq_topic::MqTopicOutcome,
) -> Result<mq_v2::CreateTopicResponse, Status> {
    use crate::storage::mq_topic::MqTopicOutcome;
    match outcome {
        MqTopicOutcome::Created {
            topic_id,
            epoch,
            partitions,
            config,
            ..
        } => Ok(mq_v2::CreateTopicResponse {
            topic: topic.to_string(),
            topic_id: *topic_id,
            topic_epoch: *epoch,
            partition_count: i32::try_from(*partitions).unwrap_or(i32::MAX),
            config: Some(topic_config_to_proto(*config)),
        }),
        MqTopicOutcome::Exists { topic_id, epoch } => Err(attach(
            Status::already_exists(format!(
                "topic {topic} already exists with a different configuration \
                 (topic_id {topic_id}, epoch {epoch})"
            )),
            CoordErrorCode::TopicExists,
        )),
        MqTopicOutcome::Invalid { reason } => Err(invalid(reason.clone())),
        other => Err(Status::internal(format!(
            "unexpected CreateTopic outcome: {other:?}"
        ))),
    }
}

/// 表里的具体配置 → 契约回显（`CreateTopicResponse.config`）。
pub(crate) fn topic_config_to_proto(config: MqTopicConfig) -> mq_v2::TopicConfig {
    mq_v2::TopicConfig {
        retention_secs: i64::try_from(config.retention_secs).unwrap_or(i64::MAX),
        max_bytes: config.max_bytes,
        max_message_bytes: config.max_message_bytes,
        segment_bytes: config.segment_bytes,
        dlq_retention_secs: i64::try_from(config.dlq_retention_secs).unwrap_or(i64::MAX),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use coord_core::error_code::error_code_of;
    use coord_core::storage::StorageBackend;
    use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionMeta, StorageConfig};
    use tempfile::TempDir;
    use tonic::Code;

    use crate::mq_v2::{MqV2RuntimeConfig, MqV2State};
    use crate::raft::region::RegionManager;
    use crate::storage::mq_topic::{MqTopicCreate, MqTopicOutcome};
    use crate::storage::mvcc::{AppliedLogId, MvccStorage};
    use crate::storage::redb_backend::RedbBackend;
    use crate::storage::region_table::RegionTable;

    fn config() -> MqV2RuntimeConfig {
        MqV2RuntimeConfig {
            log_shards: 2,
            group_shards: 1,
            ..MqV2RuntimeConfig::default()
        }
    }

    fn topic_create(name: &str, partitions: u32) -> MqTopicCreate {
        MqTopicCreate {
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
        }
    }

    fn apply(op: &MqTopicOp, storage: &MvccStorage<RedbBackend>) -> MqTopicOutcome {
        storage
            .apply_mq_topic_op(
                op,
                1,
                AppliedLogId {
                    term: 1,
                    node_id: 1,
                    index: 1,
                },
            )
            .expect("apply topic op")
    }

    fn node() -> (TempDir, CoordNode) {
        let dir = TempDir::new().expect("tempdir");
        let backend =
            RedbBackend::open(dir.path(), &StorageConfig::default()).expect("open backend");
        let mvcc = Arc::new(MvccStorage::new(backend).expect("mvcc"));
        let mut node = CoordNode::new(mvcc);
        node.node_id = 1;
        node.set_mq_v2_state(MqV2State::new(true, config()));
        (dir, node)
    }

    #[test]
    fn expand_topic_config_applies_the_documented_defaults() {
        let (_dir, node) = node();
        let expanded = node
            .mq_expand_topic_config(None)
            .expect("empty request must expand");
        let defaults = config();
        assert_eq!(expanded.retention_secs, defaults.default_retention_secs);
        assert_eq!(expanded.max_bytes, defaults.max_topic_bytes);
        assert_eq!(expanded.max_message_bytes, defaults.max_message_bytes);
        assert_eq!(expanded.segment_bytes, defaults.segment_bytes);
        assert_eq!(expanded.dlq_retention_secs, expanded.retention_secs);
    }

    #[test]
    fn expand_topic_config_keeps_an_explicit_zero_max_bytes() {
        let (_dir, node) = node();
        let expanded = node
            .mq_expand_topic_config(Some(mq_v2::TopicConfig {
                max_bytes: 0,
                ..Default::default()
            }))
            .expect("explicit config must expand");
        assert_eq!(
            expanded.max_bytes, 0,
            "0 = unlimited when config is present"
        );
        assert_eq!(
            expanded.retention_secs,
            config().default_retention_secs,
            "other zero fields still take the runtime defaults"
        );
    }

    #[test]
    fn expand_topic_config_rejects_out_of_range_values() {
        let (_dir, node) = node();
        let cases = [
            mq_v2::TopicConfig {
                retention_secs: 1,
                ..Default::default()
            },
            mq_v2::TopicConfig {
                retention_secs: -5,
                ..Default::default()
            },
            mq_v2::TopicConfig {
                max_message_bytes: 9 * 1024 * 1024,
                ..Default::default()
            },
            mq_v2::TopicConfig {
                segment_bytes: 1024,
                ..Default::default()
            },
            mq_v2::TopicConfig {
                dlq_retention_secs: i64::try_from(RETENTION_MAX_SECS).unwrap_or(i64::MAX) + 1,
                ..Default::default()
            },
        ];
        for case in cases {
            let status = node
                .mq_expand_topic_config(Some(case))
                .expect_err("out-of-range config must be rejected");
            assert_eq!(status.code(), Code::InvalidArgument, "{case:?}");
            assert_eq!(
                error_code_of(&status).as_deref(),
                Some("INVALID_ARGUMENT"),
                "{case:?}"
            );
        }
    }

    #[test]
    fn create_topic_response_maps_outcomes_to_stable_codes() {
        let config = MqTopicConfig {
            retention_secs: 604_800,
            max_bytes: 1024,
            max_message_bytes: 4096,
            segment_bytes: 1024 * 1024,
            dlq_retention_secs: 604_800,
        };
        let created = create_topic_response(
            "orders",
            &MqTopicOutcome::Created {
                topic_id: 7,
                epoch: 3,
                partitions: 2,
                config,
                shard_of_partition: vec![0, 1],
                replayed: false,
            },
        )
        .expect("created must map");
        assert_eq!(created.topic, "orders");
        assert_eq!(created.topic_id, 7);
        assert_eq!(created.topic_epoch, 3);
        assert_eq!(created.partition_count, 2);
        assert_eq!(created.config.expect("config echo").max_bytes, 1024);

        let exists = create_topic_response(
            "orders",
            &MqTopicOutcome::Exists {
                topic_id: 7,
                epoch: 3,
            },
        )
        .expect_err("exists must be an error");
        assert_eq!(exists.code(), Code::AlreadyExists);
        assert_eq!(
            error_code_of(&exists).as_deref(),
            Some("TOPIC_EXISTS"),
            "{exists:?}"
        );

        let invalid = create_topic_response(
            "orders",
            &MqTopicOutcome::Invalid {
                reason: "bad".into(),
            },
        )
        .expect_err("invalid must be an error");
        assert_eq!(invalid.code(), Code::InvalidArgument);
        assert_eq!(error_code_of(&invalid).as_deref(), Some("INVALID_ARGUMENT"));
    }

    #[test]
    fn partition_leaders_report_the_shard_of_each_partition() {
        let (_dir, mut node) = node();
        apply(&MqTopicOp::Create(topic_create("orders", 4)), &node.storage);

        // 只登记路由（不装配 runtime）：leader 地址必须是空串（选举中），
        // 分区 → 分片的映射来自权威 topic 表。
        let peer = Peer {
            node_id: 1,
            raft_addr: "127.0.0.1:1".to_string(),
            role: PeerRole::Voter,
        };
        let mut table = RegionTable::default();
        table
            .init(vec![
                RegionMeta {
                    region_id: 1,
                    start_key: Vec::new(),
                    end_key: b"/_mq/log/00/".to_vec(),
                    epoch: RegionEpoch::initial(),
                    peers: vec![peer.clone()],
                    approximate_size: 0,
                    approximate_keys: 0,
                },
                RegionMeta {
                    region_id: 2,
                    start_key: b"/_mq/log/00/".to_vec(),
                    end_key: b"/_mq/log/01/".to_vec(),
                    epoch: RegionEpoch::initial(),
                    peers: vec![peer.clone()],
                    approximate_size: 0,
                    approximate_keys: 0,
                },
                RegionMeta {
                    region_id: 3,
                    start_key: b"/_mq/log/01/".to_vec(),
                    end_key: Vec::new(),
                    epoch: RegionEpoch::initial(),
                    peers: vec![peer],
                    approximate_size: 0,
                    approximate_keys: 0,
                },
            ])
            .expect("tiling table");
        let manager = Arc::new(RegionManager::new(1));
        manager
            .apply_region_table(&table)
            .expect("register carved regions");
        node.region_manager = Some(manager);

        let table = node.mq_topic_table().expect("topic table");
        let meta = table.topic("orders").expect("topic row");
        let leaders = node.mq_partition_leaders(meta);
        assert_eq!(leaders.len(), 4);
        for (partition, leader) in leaders.iter().enumerate() {
            assert_eq!(leader.partition, partition as i32);
            assert_eq!(leader.shard_id, meta.shard_of_partition[partition]);
            assert!(
                leader.leader_addr.is_empty(),
                "no runtime assembled ⇒ no leader address"
            );
            assert_eq!(leader.earliest_offset, 0);
            assert_eq!(leader.next_offset, 0);
        }
    }
}
