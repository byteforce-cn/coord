//! MQ v2 M4-2 验收（#336 / #286）：`DeleteTopic` 的立即隐藏与异步回收。
//!
//! 判据（设计 §6 M4-2）：
//! - 删除后旧 topic 的读写**立即** `TOPIC_NOT_FOUND`（不等回收）；
//! - reaper 逐个分区回收（段/DLQ/幂等索引/分区元数据）并清理组位点，
//!   `GetTopicDeletion` 从 Pending 推进到 Done；
//! - 同名重建得到新 `topic_id`，旧数据不复活；
//! - 重复删除/重放幂等（不重复扣账）。

#[path = "support/mq_v2_node.rs"]
mod mq_v2_node;

use coord_core::error_code::error_code_of;
use coord_proto::mq::v2 as mq_v2;
use coord_proto::mq::v2::mq_client::MqClient;
use mq_v2_node::{create_topic, publish, MqV2Node};
use tonic::transport::Channel;
use tonic::Code;

const TOPIC: &str = "orders";
const GROUP: &str = "billing";

fn wire_code(status: &tonic::Status) -> Option<String> {
    error_code_of(status)
}

async fn delete_topic(
    client: &mut MqClient<Channel>,
    topic: &str,
    expected_epoch: u64,
) -> Result<mq_v2::DeleteTopicResponse, tonic::Status> {
    client
        .delete_topic(mq_v2::DeleteTopicRequest {
            topic: topic.to_string(),
            expected_topic_epoch: expected_epoch,
        })
        .await
        .map(|response| response.into_inner())
}

async fn deletion(
    client: &mut MqClient<Channel>,
    delete_id: &str,
) -> mq_v2::GetTopicDeletionResponse {
    client
        .get_topic_deletion(mq_v2::GetTopicDeletionRequest {
            delete_id: delete_id.to_string(),
        })
        .await
        .expect("get topic deletion")
        .into_inner()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_topic_hides_immediately_and_reclaims_asynchronously() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    let created = create_topic(&mut client, TOPIC, 2, None).await;
    for partition in 0..2i32 {
        for index in 0..2u32 {
            publish(
                &mut client,
                TOPIC,
                partition,
                format!("p{partition}-{index}").as_bytes(),
                "",
            )
            .await
            .expect("publish");
        }
    }
    // 组位点（删除后必须一并清理）。
    let joined = node.join_direct(GROUP, "m1", TOPIC, 2).await;
    client
        .ack(mq_v2::AckRequest {
            topic: TOPIC.to_string(),
            partition: 0,
            topic_epoch: created.topic_epoch,
            group: GROUP.to_string(),
            member_id: joined.member_id.clone(),
            generation: joined.generation,
            offset: 1,
        })
        .await
        .expect("ack");
    assert_eq!(node.group_offsets(GROUP).len(), 1);

    let deleted = delete_topic(&mut client, TOPIC, created.topic_epoch)
        .await
        .expect("delete topic");
    assert_eq!(deleted.deleted_topic_epoch, created.topic_epoch + 1);
    let delete_id = deleted.delete_id.clone();
    assert!(!delete_id.is_empty());

    // 立即隐藏：读/写都 TOPIC_NOT_FOUND（不等回收）。
    let describe = client
        .describe_topic(mq_v2::DescribeTopicRequest {
            topic: TOPIC.to_string(),
        })
        .await
        .expect_err("deleted topic must be hidden immediately");
    assert_eq!(wire_code(&describe).as_deref(), Some("TOPIC_NOT_FOUND"));
    let write = publish(&mut client, TOPIC, 0, b"late", "").await;
    assert_eq!(
        write.expect_err("deleted topic must reject writes").code(),
        Code::NotFound
    );
    let pending = deletion(&mut client, &delete_id).await;
    assert_eq!(
        pending.state,
        mq_v2::TopicDeletionState::Pending as i32,
        "reclamation has not run yet"
    );
    assert_eq!(pending.partitions_total, 2);
    assert_eq!(pending.partitions_reclaimed, 0);

    // 跑一次回收：分区全部丢弃、组位点清空、进度转 Done。
    let stats = node
        .node
        .mq_v2_reaper_pass(coord_server::mq_v2::now_ms())
        .await;
    assert_eq!(stats.partitions_dropped, 2, "{stats:?}");
    let done = deletion(&mut client, &delete_id).await;
    assert_eq!(done.state, mq_v2::TopicDeletionState::Done as i32);
    assert_eq!(done.partitions_reclaimed, 2);
    assert!(done.messages_removed >= 4, "{done:?}");
    assert!(done.bytes_reclaimed > 0, "{done:?}");
    assert!(
        node.group_offsets(GROUP).is_empty(),
        "deleted topic offsets must be reclaimed"
    );
    assert!(
        node.metric("coord_mq_v2_deletion_partitions_total") >= 2,
        "the reaper must publish its reclaim counters"
    );

    // 再次跑回收：幂等（没有可回收的分区）。
    let again = node
        .node
        .mq_v2_reaper_pass(coord_server::mq_v2::now_ms())
        .await;
    assert_eq!(again.partitions_dropped, 0, "{again:?}");

    // 同名重建：新 topic_id，旧消息不复活。
    let rebuilt = create_topic(&mut client, TOPIC, 1, None).await;
    assert_ne!(rebuilt.topic_id, created.topic_id);
    let first = publish(&mut client, TOPIC, 0, b"fresh", "")
        .await
        .expect("publish");
    assert_eq!(first.offset, 0, "a rebuilt topic starts from offset 0");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_topic_enforces_the_epoch_and_maps_unknown_topics() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    let created = create_topic(&mut client, TOPIC, 1, None).await;

    let stale = delete_topic(&mut client, TOPIC, created.topic_epoch + 5)
        .await
        .expect_err("stale epoch must be rejected");
    assert_eq!(stale.code(), Code::Aborted, "{stale:?}");
    assert_eq!(wire_code(&stale).as_deref(), Some("TOPIC_EPOCH_STALE"));

    let missing = delete_topic(&mut client, "nope", 0)
        .await
        .expect_err("unknown topic");
    assert_eq!(wire_code(&missing).as_deref(), Some("TOPIC_NOT_FOUND"));

    // 无代际校验（expected = 0）也能删。
    delete_topic(&mut client, TOPIC, 0)
        .await
        .expect("delete without epoch check");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_topic_deletion_validates_the_delete_id() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;

    let invalid = client
        .get_topic_deletion(mq_v2::GetTopicDeletionRequest {
            delete_id: "not-a-number".to_string(),
        })
        .await
        .expect_err("invalid delete_id");
    assert_eq!(invalid.code(), Code::InvalidArgument, "{invalid:?}");
    assert_eq!(wire_code(&invalid).as_deref(), Some("INVALID_ARGUMENT"));

    let unknown = client
        .get_topic_deletion(mq_v2::GetTopicDeletionRequest {
            delete_id: "12345".to_string(),
        })
        .await
        .expect_err("unknown delete_id");
    assert_eq!(wire_code(&unknown).as_deref(), Some("TOPIC_NOT_FOUND"));
}
