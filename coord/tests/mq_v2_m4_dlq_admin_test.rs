//! MQ v2 M4 验收（#336 / #286）：死信（M4-1）与 topic 管理读面/更新（M4-3）。
//!
//! 判据（设计 §6 M4-1/M4-3）：
//! - `MoveToDlq` 复制 + 主日志 tombstone；重复调用幂等（`already_moved`）；
//! - `Poll` 跳过已移入的消息，`PollDlq` 返回原 offset + reason/detail；
//! - `DescribeTopic` 给出配置/分区/Leader/边界/记账字节；`ListTopics` 分页；
//! - `UpdateTopic` 0 = 保持当前值、CAS 代际、非法值拒绝。

#[path = "support/mq_v2_node.rs"]
mod mq_v2_node;

use coord_core::error_code::error_code_of;
use coord_proto::mq::v2 as mq_v2;
use coord_proto::mq::v2::mq_client::MqClient;
use mq_v2_node::{create_topic, publish, MqV2Node};
use tonic::transport::Channel;
use tonic::Code;

const TOPIC: &str = "orders";
const EARLIEST: &str = "coord-earliest-offset";
const LATEST: &str = "coord-latest-offset";

fn wire_code(status: &tonic::Status) -> Option<String> {
    error_code_of(status)
}

async fn move_to_dlq(
    client: &mut MqClient<Channel>,
    offset: i64,
    reason: &str,
) -> Result<mq_v2::MoveToDlqResponse, tonic::Status> {
    client
        .move_to_dlq(mq_v2::MoveToDlqRequest {
            topic: TOPIC.to_string(),
            partition: 0,
            topic_epoch: 0,
            offset,
            reason: reason.to_string(),
            detail: format!("detail-{offset}"),
        })
        .await
        .map(|response| response.into_inner())
}

async fn poll(
    client: &mut MqClient<Channel>,
    start: Option<i64>,
    max_messages: i32,
) -> mq_v2::PollResponse {
    client
        .poll(mq_v2::PollRequest {
            topic: TOPIC.to_string(),
            partition: 0,
            topic_epoch: 0,
            start_offset: start,
            max_messages,
            max_bytes: 0,
            wait_ms: 0,
        })
        .await
        .expect("poll")
        .into_inner()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn move_to_dlq_hides_the_message_from_poll_and_is_idempotent() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    create_topic(&mut client, TOPIC, 1, None).await;
    for index in 0..3u32 {
        publish(&mut client, TOPIC, 0, format!("m-{index}").as_bytes(), "")
            .await
            .expect("publish");
    }

    let moved = move_to_dlq(&mut client, 1, "poison")
        .await
        .expect("move to dlq");
    assert_eq!(moved.dlq_offset, 0);
    assert!(!moved.already_moved);

    // 重复调用：幂等，返回首次 dlq_offset。
    let replay = move_to_dlq(&mut client, 1, "poison")
        .await
        .expect("replayed move");
    assert!(replay.already_moved);
    assert_eq!(replay.dlq_offset, moved.dlq_offset);

    // Poll 跳过 tombstone（M3-1）；PollDlq 返回原 offset + reason/detail。
    let visible = poll(&mut client, Some(0), 10).await;
    let offsets: Vec<i64> = visible.messages.iter().map(|m| m.offset).collect();
    assert_eq!(
        offsets,
        vec![0, 2],
        "moved message must be hidden from Poll"
    );

    let dlq = client
        .poll_dlq(mq_v2::PollDlqRequest {
            topic: TOPIC.to_string(),
            partition: 0,
            topic_epoch: 0,
            start_offset: None,
            max_messages: 10,
        })
        .await
        .expect("poll dlq")
        .into_inner();
    assert_eq!(dlq.messages.len(), 1);
    assert_eq!(dlq.messages[0].offset, 1, "DLQ keeps the original offset");
    assert_eq!(dlq.messages[0].payload, b"m-1");
    assert_eq!(dlq.messages[0].dlq_reason, "poison");
    assert_eq!(dlq.messages[0].dlq_detail, "detail-1");
    assert_eq!(dlq.next_dlq_offset, 1);
    assert!(node.metric("coord_mq_v2_dlq_moved_total") >= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn move_to_dlq_rejects_an_unknown_offset_and_poll_dlq_paginates() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    create_topic(&mut client, TOPIC, 1, None).await;
    publish(&mut client, TOPIC, 0, b"a", "")
        .await
        .expect("publish");
    publish(&mut client, TOPIC, 0, b"b", "")
        .await
        .expect("publish");

    let status = move_to_dlq(&mut client, 99, "poison")
        .await
        .expect_err("unknown offset must be rejected");
    assert_eq!(status.code(), Code::OutOfRange, "{status:?}");
    assert_eq!(wire_code(&status).as_deref(), Some("OFFSET_OUT_OF_RANGE"));
    assert!(status.metadata().get(EARLIEST).is_some());
    assert!(status.metadata().get(LATEST).is_some());

    move_to_dlq(&mut client, 0, "poison").await.expect("move 0");
    move_to_dlq(&mut client, 1, "poison").await.expect("move 1");

    let first = client
        .poll_dlq(mq_v2::PollDlqRequest {
            topic: TOPIC.to_string(),
            partition: 0,
            topic_epoch: 0,
            start_offset: None,
            max_messages: 1,
        })
        .await
        .expect("poll dlq")
        .into_inner();
    assert_eq!(first.messages.len(), 1);
    assert_eq!(first.next_dlq_offset, 1);

    let second = client
        .poll_dlq(mq_v2::PollDlqRequest {
            topic: TOPIC.to_string(),
            partition: 0,
            topic_epoch: 0,
            start_offset: Some(first.next_dlq_offset),
            max_messages: 1,
        })
        .await
        .expect("poll dlq page 2")
        .into_inner();
    assert_eq!(second.messages.len(), 1);
    assert_eq!(second.messages[0].offset, 1);
    assert_eq!(second.next_dlq_offset, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn describe_topic_reports_config_partitions_and_bounds() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    let created = create_topic(&mut client, TOPIC, 2, None).await;
    for index in 0..2u32 {
        publish(&mut client, TOPIC, 0, format!("m-{index}").as_bytes(), "")
            .await
            .expect("publish");
    }

    let described = client
        .describe_topic(mq_v2::DescribeTopicRequest {
            topic: TOPIC.to_string(),
        })
        .await
        .expect("describe")
        .into_inner();
    assert_eq!(described.topic, TOPIC);
    assert_eq!(described.topic_id, created.topic_id);
    assert_eq!(described.topic_epoch, created.topic_epoch);
    assert_eq!(described.partition_count, 2);
    let config = described.config.expect("config echo");
    assert_eq!(config.retention_secs, 604_800);
    assert_eq!(described.partitions.len(), 2);
    let partition_zero = described
        .partitions
        .iter()
        .find(|partition| partition.partition == 0)
        .expect("partition 0");
    assert_eq!(partition_zero.earliest_offset, 0);
    assert_eq!(partition_zero.next_offset, 2);
    assert!(partition_zero.log_bytes > 0);
    assert!(
        !partition_zero.leader_addr.is_empty(),
        "a shard leader must be reported once elected"
    );
    let partition_one = described
        .partitions
        .iter()
        .find(|partition| partition.partition == 1)
        .expect("partition 1");
    assert_eq!(partition_one.next_offset, 0, "no writes on partition 1");

    let missing = client
        .describe_topic(mq_v2::DescribeTopicRequest {
            topic: "nope".to_string(),
        })
        .await
        .expect_err("unknown topic");
    assert_eq!(wire_code(&missing).as_deref(), Some("TOPIC_NOT_FOUND"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_topics_paginates_and_filters_by_prefix() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    for topic in ["app-1", "app-2", "app-3", "other"] {
        create_topic(&mut client, topic, 1, None).await;
    }

    let mut collected = Vec::new();
    let mut token = String::new();
    loop {
        let page = client
            .list_topics(mq_v2::ListTopicsRequest {
                prefix: "app-".to_string(),
                page_size: 2,
                page_token: token.clone(),
            })
            .await
            .expect("list topics")
            .into_inner();
        collected.extend(page.topics.iter().map(|topic| topic.topic.clone()));
        if page.next_page_token.is_empty() {
            break;
        }
        token = page.next_page_token;
    }
    assert_eq!(collected, vec!["app-1", "app-2", "app-3"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_topic_merges_fields_and_enforces_the_epoch() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    let created = create_topic(&mut client, TOPIC, 1, None).await;

    let updated = client
        .update_topic(mq_v2::UpdateTopicRequest {
            topic: TOPIC.to_string(),
            expected_topic_epoch: created.topic_epoch,
            config: Some(mq_v2::TopicConfig {
                retention_secs: 86_400,
                max_bytes: 0,
                max_message_bytes: 0,
                segment_bytes: 0,
                dlq_retention_secs: 0,
            }),
        })
        .await
        .expect("update")
        .into_inner();
    assert_eq!(updated.topic_epoch, created.topic_epoch + 1);
    let config = updated.config.expect("config echo");
    assert_eq!(config.retention_secs, 86_400);
    assert_eq!(
        config.max_message_bytes,
        created.config.expect("created config").max_message_bytes,
        "0 = keep the current value"
    );

    // 过期代际：CAS 拒绝。
    let stale = client
        .update_topic(mq_v2::UpdateTopicRequest {
            topic: TOPIC.to_string(),
            expected_topic_epoch: created.topic_epoch,
            config: Some(mq_v2::TopicConfig {
                retention_secs: 172_800,
                ..Default::default()
            }),
        })
        .await
        .expect_err("stale epoch");
    assert_eq!(stale.code(), Code::Aborted, "{stale:?}");
    assert_eq!(wire_code(&stale).as_deref(), Some("TOPIC_EPOCH_STALE"));

    // 非法值。
    let invalid = client
        .update_topic(mq_v2::UpdateTopicRequest {
            topic: TOPIC.to_string(),
            expected_topic_epoch: 0,
            config: Some(mq_v2::TopicConfig {
                retention_secs: 1,
                ..Default::default()
            }),
        })
        .await
        .expect_err("invalid retention");
    assert_eq!(invalid.code(), Code::InvalidArgument, "{invalid:?}");
    assert_eq!(wire_code(&invalid).as_deref(), Some("INVALID_ARGUMENT"));

    // 描述里能看到新配置。
    let described = client
        .describe_topic(mq_v2::DescribeTopicRequest {
            topic: TOPIC.to_string(),
        })
        .await
        .expect("describe")
        .into_inner();
    assert_eq!(described.topic_epoch, updated.topic_epoch);
    assert_eq!(described.config.expect("config").retention_secs, 86_400);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dlq_is_gated_by_the_dlq_capability() {
    use std::sync::Arc;

    use coord_core::storage::StorageBackend;
    use coord_core::types::StorageConfig;
    use coord_proto::mq::v2::mq_server::Mq;
    use coord_server::mq_v2::{MqV2Capability, MqV2RuntimeConfig, MqV2State};
    use coord_server::server::CoordNode;
    use coord_server::storage::mvcc::MvccStorage;
    use coord_server::storage::redb_backend::RedbBackend;

    let dir = tempfile::tempdir().expect("tempdir");
    let backend = RedbBackend::open(dir.path(), &StorageConfig::default()).expect("open backend");
    let mvcc = Arc::new(MvccStorage::new(backend).expect("mvcc"));
    let mut node = CoordNode::new(mvcc);
    // Topic 已交付、Dlq 未交付：死信面必须 FAILED_PRECONDITION。
    node.set_mq_v2_state(
        MqV2State::new(true, MqV2RuntimeConfig::default())
            .with_implemented([MqV2Capability::Topic]),
    );

    let status = node
        .move_to_dlq(tonic::Request::new(mq_v2::MoveToDlqRequest {
            topic: TOPIC.to_string(),
            partition: 0,
            topic_epoch: 0,
            offset: 0,
            reason: "poison".to_string(),
            detail: String::new(),
        }))
        .await
        .expect_err("Dlq capability is not delivered");
    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert_eq!(wire_code(&status).as_deref(), Some("FAILED_PRECONDITION"));
}
