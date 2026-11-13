//! MQ v2 M2 验收（#334 / #284）：写路径的路由、批量、offset、幂等、上限与原子性。
//!
//! 判据（设计 §6 M2-1–M2-3）：
//! - 响应即已提交：`Publish` 返回的 offset 在分片副本上立即可见（`next_offset`）；
//! - 同分区 offset 严格递增、绝不复用（并发 64 次 ⇒ 恰好 `0..64`）；
//! - 同分片的并发发布被合成更少的 Raft 条目（`coord_mq_v2_append_*` 计数器）；
//! - 幂等键返回首次 offset；保留回收后同键可重新使用（窗口 = 保留期）；
//! - 超限/超配额整批不追加（`next_offset` 不变），错误码稳定。

#[path = "support/mq_v2_node.rs"]
mod mq_v2_node;

use std::sync::Arc;

use coord_core::error_code::error_code_of;
use coord_core::storage::StorageBackend;
use coord_core::types::StorageConfig;
use coord_proto::mq::v2 as mq_v2;
use coord_proto::mq::v2::mq_server::Mq;
use coord_server::mq_v2::{MqV2Capability, MqV2RuntimeConfig, MqV2State};
use coord_server::server::CoordNode;
use coord_server::storage::mq_topic::{MqTopicConfig, MqTopicOp, MqTopicUpdate};
use coord_server::storage::mvcc::MvccStorage;
use coord_server::storage::redb_backend::RedbBackend;
use mq_v2_node::{create_topic, publish, MqV2Node};
use tonic::{Code, Request};

const TOPIC: &str = "orders";

fn code_of(status: &tonic::Status) -> Option<String> {
    error_code_of(status)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_topic_then_publish_returns_a_committed_offset() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;

    let created = create_topic(&mut client, TOPIC, 4, None).await;
    assert_eq!(created.topic, TOPIC);
    assert_eq!(created.partition_count, 4);

    let first = publish(&mut client, TOPIC, 0, b"hello", "")
        .await
        .expect("first publish");
    assert_eq!(first.offset, 0);
    assert!(!first.deduplicated);

    let second = publish(&mut client, TOPIC, 0, b"world", "")
        .await
        .expect("second publish");
    assert_eq!(second.offset, 1);

    // 响应即已提交：分片副本上已能看到两个 offset。
    let meta = node.topic_row(TOPIC);
    let bounds = node
        .bounds(&meta, 0)
        .expect("partition bounds after append");
    assert_eq!(bounds.earliest_offset, 0);
    assert_eq!(bounds.next_offset, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publish_offsets_are_unique_and_monotonic_under_concurrency() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    create_topic(&mut client, TOPIC, 2, None).await;

    let mut tasks = Vec::new();
    for index in 0..64u64 {
        let mut client = node.client().await;
        tasks.push(tokio::spawn(async move {
            publish(
                &mut client,
                TOPIC,
                1,
                format!("payload-{index}").as_bytes(),
                "",
            )
            .await
            .map(|response| response.offset)
            .unwrap_or_else(|err| panic!("publish {index} failed: {err}"))
        }));
    }
    let mut offsets = Vec::with_capacity(tasks.len());
    for task in tasks {
        offsets.push(task.await.expect("concurrent publish task"));
    }
    offsets.sort_unstable();
    assert_eq!(
        offsets,
        (0..64i64).collect::<Vec<_>>(),
        "offset 必须严格递增、绝不复用"
    );

    let meta = node.topic_row(TOPIC);
    assert_eq!(node.bounds(&meta, 1).expect("bounds").next_offset, 64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_publishes_are_coalesced_into_fewer_raft_entries() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    create_topic(&mut client, TOPIC, 1, None).await;

    let mut tasks = Vec::new();
    for index in 0..64u64 {
        let mut client = node.client().await;
        tasks.push(tokio::spawn(async move {
            publish(
                &mut client,
                TOPIC,
                0,
                format!("payload-{index}").as_bytes(),
                "",
            )
            .await
            .is_ok()
        }));
    }
    for task in tasks {
        assert!(
            task.await.expect("publish task"),
            "all publishes must succeed"
        );
    }

    let (batches, requests) = node.append_stats();
    assert_eq!(requests, 64, "every publish must be committed exactly once");
    assert!(
        batches > 0 && batches < requests,
        "同分片的并发发布必须摊薄成更少的 Raft 条目 (batches={batches}, requests={requests})"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_idempotency_key_returns_the_first_offset() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    create_topic(&mut client, TOPIC, 1, None).await;

    let first = publish(&mut client, TOPIC, 0, b"one", "idem-1")
        .await
        .expect("first publish");
    assert!(!first.deduplicated);
    let replay = publish(&mut client, TOPIC, 0, b"one", "idem-1")
        .await
        .expect("replayed publish");
    assert!(replay.deduplicated);
    assert_eq!(replay.offset, first.offset);

    // 同批内重复键：首次分配 offset，重复条目复用该 offset。
    let batch = client
        .publish_batch(mq_v2::PublishBatchRequest {
            topic: TOPIC.to_string(),
            partition: 0,
            topic_epoch: 0,
            entries: vec![
                mq_v2::BatchEntry {
                    payload: b"a".to_vec(),
                    idempotency_key: "idem-2".to_string(),
                    ..Default::default()
                },
                mq_v2::BatchEntry {
                    payload: b"b".to_vec(),
                    idempotency_key: "idem-2".to_string(),
                    ..Default::default()
                },
            ],
        })
        .await
        .expect("batch publish")
        .into_inner();
    assert_eq!(batch.results.len(), 2);
    assert_eq!(batch.results[0].offset, batch.results[1].offset);
    assert!(!batch.results[0].deduplicated);
    assert!(batch.results[1].deduplicated);

    let meta = node.topic_row(TOPIC);
    assert_eq!(
        node.bounds(&meta, 0).expect("bounds").next_offset,
        u64::try_from(first.offset + 2).expect("non-negative offset"),
        "重复键不得重复追加"
    );
    // 指标写入点：两次额外去重（重放一次 + 同批重复一次）必须被计入。
    assert!(
        node.metric("coord_mq_v2_publish_duplicates_total") >= 2,
        "idempotency hits must be counted: {}",
        node.metric("coord_mq_v2_publish_duplicates_total")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publish_batch_is_atomic_when_one_entry_is_oversized() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    create_topic(
        &mut client,
        TOPIC,
        1,
        Some(mq_v2::TopicConfig {
            max_message_bytes: 64,
            ..Default::default()
        }),
    )
    .await;

    let status = client
        .publish_batch(mq_v2::PublishBatchRequest {
            topic: TOPIC.to_string(),
            partition: 0,
            topic_epoch: 0,
            entries: vec![
                mq_v2::BatchEntry {
                    payload: b"small".to_vec(),
                    ..Default::default()
                },
                mq_v2::BatchEntry {
                    payload: vec![b'x'; 200],
                    ..Default::default()
                },
            ],
        })
        .await
        .expect_err("oversized entry must reject the whole batch");
    assert_eq!(status.code(), Code::InvalidArgument, "{status:?}");
    assert_eq!(code_of(&status).as_deref(), Some("MESSAGE_TOO_LARGE"));

    let meta = node.topic_row(TOPIC);
    assert_eq!(
        node.bounds(&meta, 0),
        None,
        "整批拒绝不得留下任何消息（连段元数据都不该出现）"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publish_batch_over_the_quota_writes_nothing() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    create_topic(
        &mut client,
        TOPIC,
        1,
        Some(mq_v2::TopicConfig {
            max_bytes: 2048,
            ..Default::default()
        }),
    )
    .await;

    let status = client
        .publish_batch(mq_v2::PublishBatchRequest {
            topic: TOPIC.to_string(),
            partition: 0,
            topic_epoch: 0,
            entries: (0..8)
                .map(|_| mq_v2::BatchEntry {
                    payload: vec![b'q'; 400],
                    ..Default::default()
                })
                .collect(),
        })
        .await
        .expect_err("quota must reject the whole batch");
    assert_eq!(status.code(), Code::ResourceExhausted, "{status:?}");
    assert_eq!(code_of(&status).as_deref(), Some("QUOTA_EXCEEDED"));

    let meta = node.topic_row(TOPIC);
    assert_eq!(node.bounds(&meta, 0), None, "超配额整批不追加");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publish_after_retention_truncation_reuses_the_idempotency_key() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    // 小段大小（1 KiB）让一次截断就能回收带幂等键的那条消息：生产路径的
    // `CreateTopic` 有 1 MiB 下限，这里经 region 0 raft 直接下发命令（下界只属于
    // RPC 校验，存储层语义与生产一致）。
    let outcome = node
        .propose_topic(MqTopicOp::Create(
            coord_server::storage::mq_topic::MqTopicCreate {
                name: TOPIC.to_string(),
                partitions: 1,
                config: MqTopicConfig {
                    retention_secs: 604_800,
                    max_bytes: 0,
                    max_message_bytes: 1024 * 1024,
                    segment_bytes: 1024,
                    dlq_retention_secs: 604_800,
                },
                request_id: String::new(),
                now_ms: 1,
                log_shards: 2,
                max_partitions_per_shard: 1024,
            },
        ))
        .await;
    assert!(matches!(
        outcome,
        coord_server::storage::mq_topic::MqTopicOutcome::Created { .. }
    ));

    let first = publish(&mut client, TOPIC, 0, &vec![b'k'; 100], "window-key")
        .await
        .expect("first publish");
    assert_eq!(first.offset, 0);
    for index in 1..40u32 {
        publish(&mut client, TOPIC, 0, &vec![b'p'; 100], "")
            .await
            .unwrap_or_else(|err| panic!("publish {index} failed: {err}"));
    }

    let meta = node.topic_row(TOPIC);
    let truncated = node.truncate(&meta, 0, 32).await;
    assert!(
        truncated.dropped_segments > 0,
        "the window test needs at least one reclaimed segment: {truncated:?}"
    );
    assert!(truncated.earliest_offset > 0);

    let reuse = publish(&mut client, TOPIC, 0, b"again", "window-key")
        .await
        .expect("publish after retention");
    assert_ne!(
        reuse.offset, first.offset,
        "窗口外（幂等索引已随段回收）同键必须重新分配 offset"
    );
    assert!(!reuse.deduplicated);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retention_reaper_reclaims_expired_segments_on_the_leader() {
    let node = MqV2Node::boot(2, 1).await;
    node.propose_topic(MqTopicOp::Create(
        coord_server::storage::mq_topic::MqTopicCreate {
            name: TOPIC.to_string(),
            partitions: 1,
            config: MqTopicConfig {
                retention_secs: 3_600,
                max_bytes: 0,
                max_message_bytes: 1024 * 1024,
                segment_bytes: 1024,
                dlq_retention_secs: 3_600,
            },
            request_id: String::new(),
            now_ms: 1,
            log_shards: 2,
            max_partitions_per_shard: 1024,
        },
    ))
    .await;
    let meta = node.topic_row(TOPIC);
    // 两小时前写入 40 条 100B 消息（段大小 1 KiB ⇒ 多个已封口段）。
    let old_ms = coord_server::mq_v2::now_ms() - 2 * 3_600 * 1_000;
    let payloads: Vec<Vec<u8>> = (0..40).map(|_| vec![b'p'; 100]).collect();
    assert_eq!(node.append_at(&meta, 0, old_ms, &payloads).await, 40);

    let stats = node
        .node
        .mq_v2_reaper_pass(coord_server::mq_v2::now_ms())
        .await;
    assert!(
        stats.truncate_proposals > 0 && stats.dropped_segments > 0,
        "the retention pass must reclaim expired segments: {stats:?}"
    );
    let bounds = node.bounds(&meta, 0).expect("bounds");
    assert!(
        bounds.earliest_offset > 0,
        "earliest_offset must advance after reclamation: {bounds:?}"
    );
    assert!(
        bounds.next_offset >= 40,
        "reclamation must never drop the newest segment: {bounds:?}"
    );
    assert!(
        node.metric("coord_mq_v2_retention_segments_total") > 0,
        "the reaper must publish its reclaim counter"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn topic_epoch_is_enforced_on_the_write_path() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    let created = create_topic(&mut client, TOPIC, 1, None).await;

    let outcome = node
        .propose_topic(MqTopicOp::Update(MqTopicUpdate {
            name: TOPIC.to_string(),
            expected_epoch: created.topic_epoch,
            config: MqTopicConfig {
                retention_secs: 604_800,
                max_bytes: 0,
                max_message_bytes: 1024,
                segment_bytes: 1024 * 1024,
                dlq_retention_secs: 604_800,
            },
        }))
        .await;
    assert!(matches!(
        outcome,
        coord_server::storage::mq_topic::MqTopicOutcome::Updated { .. }
    ));

    let status = client
        .publish(mq_v2::PublishRequest {
            topic: TOPIC.to_string(),
            partition: 0,
            topic_epoch: created.topic_epoch,
            payload: b"stale".to_vec(),
            ..Default::default()
        })
        .await
        .expect_err("stale epoch must be rejected");
    assert_eq!(status.code(), Code::Aborted, "{status:?}");
    assert_eq!(code_of(&status).as_deref(), Some("TOPIC_EPOCH_STALE"));

    // 带上当前代际即可写入。
    let fresh_epoch = node.topic_row(TOPIC).epoch;
    client
        .publish(mq_v2::PublishRequest {
            topic: TOPIC.to_string(),
            partition: 0,
            topic_epoch: fresh_epoch,
            payload: b"fresh".to_vec(),
            ..Default::default()
        })
        .await
        .expect("current epoch must publish");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publish_batch_rejects_empty_and_over_64_entries() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = RedbBackend::open(dir.path(), &StorageConfig::default()).expect("open backend");
    let mvcc = Arc::new(MvccStorage::new(backend).expect("mvcc"));
    let mut node = CoordNode::new(mvcc);
    node.set_mq_v2_state(
        MqV2State::new(true, MqV2RuntimeConfig::default())
            .with_implemented([MqV2Capability::Publish]),
    );

    for len in [0usize, 65] {
        let entries = (0..len)
            .map(|_| mq_v2::BatchEntry {
                payload: b"x".to_vec(),
                ..Default::default()
            })
            .collect();
        let status = node
            .publish_batch(Request::new(mq_v2::PublishBatchRequest {
                topic: TOPIC.to_string(),
                partition: 0,
                topic_epoch: 0,
                entries,
            }))
            .await
            .expect_err("entry count must be validated");
        assert_eq!(status.code(), Code::InvalidArgument, "len {len}");
        assert_eq!(code_of(&status).as_deref(), Some("INVALID_ARGUMENT"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publish_is_gated_by_the_publish_capability() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = RedbBackend::open(dir.path(), &StorageConfig::default()).expect("open backend");
    let mvcc = Arc::new(MvccStorage::new(backend).expect("mvcc"));
    let mut node = CoordNode::new(mvcc);
    // Topic 已交付但写路径未交付：Publish 必须留在 FAILED_PRECONDITION。
    node.set_mq_v2_state(
        MqV2State::new(true, MqV2RuntimeConfig::default())
            .with_implemented([MqV2Capability::Topic]),
    );

    let status = node
        .publish(Request::new(mq_v2::PublishRequest {
            topic: TOPIC.to_string(),
            partition: 0,
            payload: b"x".to_vec(),
            ..Default::default()
        }))
        .await
        .expect_err("publish must be gated");
    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert_eq!(code_of(&status).as_deref(), Some("FAILED_PRECONDITION"));
    assert!(
        status.message().contains("coord.mq.v2"),
        "错误必须点名契约包: {status:?}"
    );
}
