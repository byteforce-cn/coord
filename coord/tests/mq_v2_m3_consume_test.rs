//! MQ v2 M3-1 验收（#335 / #285）：`Poll` 的已提交读、边界与长轮询。
//!
//! 判据（设计 §6 M3-1）：
//! - 只返回已提交（已 apply）消息；`next_offset` = 最后一条 + 1，空批 = 请求起点；
//! - 显式 `start_offset` 低于最早可用 ⇒ `OFFSET_OUT_OF_RANGE`，trailer 带
//!   earliest/latest，**不静默 clamp**；
//! - 未设置 `start_offset` ⇒ 从当前最早可用开始（调用方没指定任何值）；
//! - 长轮询在分片有新提交时被唤醒；超时返回空批（不是错误）；
//! - `max_messages`/`max_bytes` 按整条截断，第一条可见消息总是返回。

#[path = "support/mq_v2_node.rs"]
mod mq_v2_node;

use std::time::{Duration, Instant};

use coord_core::error_code::error_code_of;
use coord_proto::mq::v2 as mq_v2;
use coord_server::storage::mq_topic::{MqTopicConfig, MqTopicOp};
use mq_v2_node::{create_topic, publish, MqV2Node};
use tonic::Code;

const TOPIC: &str = "orders";
const EARLIEST: &str = "coord-earliest-offset";
const LATEST: &str = "coord-latest-offset";

async fn poll(
    client: &mut coord_proto::mq::v2::mq_client::MqClient<tonic::transport::Channel>,
    start: Option<i64>,
    max_messages: i32,
    max_bytes: i32,
    wait_ms: i32,
) -> Result<mq_v2::PollResponse, tonic::Status> {
    client
        .poll(mq_v2::PollRequest {
            topic: TOPIC.to_string(),
            partition: 0,
            topic_epoch: 0,
            start_offset: start,
            max_messages,
            max_bytes,
            wait_ms,
        })
        .await
        .map(|response| response.into_inner())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn poll_returns_committed_messages_and_next_offset() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    create_topic(&mut client, TOPIC, 1, None).await;
    for index in 0..3u32 {
        publish(&mut client, TOPIC, 0, format!("m-{index}").as_bytes(), "")
            .await
            .expect("publish");
    }

    let response = poll(&mut client, Some(0), 2, 0, 0)
        .await
        .expect("poll must succeed");
    assert_eq!(response.messages.len(), 2);
    assert_eq!(response.messages[0].offset, 0);
    assert_eq!(response.messages[1].offset, 1);
    assert_eq!(response.messages[1].payload, b"m-1");
    assert_eq!(response.next_offset, 2);
    assert_eq!(response.earliest_offset, 0);
    assert_eq!(response.latest_offset, 3);

    let response = poll(&mut client, Some(2), 0, 0, 0)
        .await
        .expect("poll must succeed");
    assert_eq!(response.messages.len(), 1);
    assert_eq!(response.next_offset, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn poll_skips_dlq_tombstones_but_advances_next_offset() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    create_topic(&mut client, TOPIC, 1, None).await;
    for index in 0..3u32 {
        publish(&mut client, TOPIC, 0, format!("m-{index}").as_bytes(), "")
            .await
            .expect("publish");
    }
    let meta = node.topic_row(TOPIC);
    let outcome = node.move_to_dlq(&meta, 0, 0).await;
    assert!(
        matches!(
            outcome,
            coord_server::storage::mq_log::MqMoveToDlqOutcome::Moved { .. }
        ),
        "move to dlq: {outcome:?}"
    );

    let response = poll(&mut client, Some(0), 10, 0, 0)
        .await
        .expect("poll must succeed");
    let offsets: Vec<i64> = response.messages.iter().map(|m| m.offset).collect();
    assert_eq!(offsets, vec![1, 2], "tombstone must not be returned");
    assert_eq!(
        response.next_offset, 3,
        "next_offset still crosses tombstones"
    );
}

/// 小段 + 回收，把 `earliest_offset` 推过 0；同时覆盖未设置起点与显式越界。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn poll_boundaries_follow_the_earliest_available_offset() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    node.propose_topic(MqTopicOp::Create(
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
    let meta = node.topic_row(TOPIC);
    let payloads: Vec<Vec<u8>> = (0..40).map(|_| vec![b'p'; 100]).collect();
    assert_eq!(
        node.append_at(&meta, 0, coord_server::mq_v2::now_ms(), &payloads)
            .await,
        40
    );
    let truncated = node.truncate(&meta, 0, 32).await;
    assert!(truncated.dropped_segments > 0, "{truncated:?}");

    let status = poll(&mut client, Some(0), 0, 0, 0)
        .await
        .expect_err("an explicit offset below earliest must be rejected");
    assert_eq!(status.code(), Code::OutOfRange, "{status:?}");
    assert_eq!(
        error_code_of(&status).as_deref(),
        Some("OFFSET_OUT_OF_RANGE")
    );
    let earliest = status
        .metadata()
        .get(EARLIEST)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<i64>().ok())
        .expect("earliest trailer");
    let latest = status
        .metadata()
        .get(LATEST)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<i64>().ok())
        .expect("latest trailer");
    assert_eq!(earliest, truncated.earliest_offset as i64);
    assert_eq!(latest, 40);

    // 未设置起点：从最早可用开始，不报错、不 clamp 到 0。
    let response = poll(&mut client, None, 1, 0, 0)
        .await
        .expect("unset start_offset must start at the earliest available");
    assert_eq!(response.messages.len(), 1);
    assert_eq!(response.messages[0].offset, earliest);
    assert_eq!(response.earliest_offset, earliest);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn poll_long_polls_until_an_append_arrives() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    create_topic(&mut client, TOPIC, 1, None).await;

    let mut poller = node.client().await;
    let started = Instant::now();
    // 5s 窗口 + 4s 上限：CI 上多套件并发时也能区分「被唤醒」与「等满窗口」。
    let polling = tokio::spawn(async move { poll(&mut poller, Some(0), 10, 0, 5_000).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    publish(&mut client, TOPIC, 0, b"wake", "")
        .await
        .expect("publish");

    let response = polling
        .await
        .expect("poll task")
        .expect("long poll must return the appended message");
    assert_eq!(response.messages.len(), 1);
    assert_eq!(response.messages[0].payload, b"wake");
    assert!(
        started.elapsed() < Duration::from_millis(4_000),
        "the append must wake the poller instead of waiting out the window: {:?}",
        started.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn poll_times_out_with_an_empty_batch() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    create_topic(&mut client, TOPIC, 1, None).await;

    let started = Instant::now();
    let response = poll(&mut client, Some(0), 10, 0, 100)
        .await
        .expect("a timeout is not an error");
    assert!(response.messages.is_empty());
    assert_eq!(
        response.next_offset, 0,
        "an empty batch keeps the start offset"
    );
    assert!(started.elapsed() >= Duration::from_millis(80));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn poll_caps_messages_and_bytes_whole_message_at_a_time() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    create_topic(&mut client, TOPIC, 1, None).await;
    for _ in 0..3 {
        publish(&mut client, TOPIC, 0, &vec![b'x'; 100], "")
            .await
            .expect("publish");
    }

    let response = poll(&mut client, Some(0), 2, 0, 0).await.expect("poll");
    assert_eq!(response.messages.len(), 2, "max_messages caps the batch");
    assert_eq!(response.next_offset, 2);

    // 一条 ~100B；max_bytes=150 只够一条（第二条不切半条返回）。
    let response = poll(&mut client, Some(2), 10, 150, 0).await.expect("poll");
    assert_eq!(response.messages.len(), 1);
    assert_eq!(response.next_offset, 3);

    // 第一条就超限时仍然返回它（否则这条消息永远无法被消费）。
    let response = poll(&mut client, Some(2), 10, 1, 0).await.expect("poll");
    assert_eq!(response.messages.len(), 1);
}

// ──── M3-5：Subscribe（best-effort 推送）────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscribe_pushes_new_messages_and_never_replays_history() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    create_topic(&mut client, TOPIC, 1, None).await;
    publish(&mut client, TOPIC, 0, b"old", "")
        .await
        .expect("publish old");

    // start_offset = 0（未设置）⇒ 各分区最新：旧消息不推送。
    let mut stream = client
        .subscribe(mq_v2::SubscribeRequest {
            topic: TOPIC.to_string(),
            topic_epoch: 0,
            group: String::new(),
            partitions: Vec::new(),
            start_offset: 0,
        })
        .await
        .expect("subscribe")
        .into_inner();

    // 不读流，先灌 100 × 4 KiB：h2 流控窗口（~64 KiB）挡住接收端后，服务端队列
    // （64）填满，其余必须丢弃而不是阻塞写路径。
    let mut payload = vec![b'x'; 4096];
    payload[..7].copy_from_slice(b"new-000");
    for index in 0..100u32 {
        payload[4..7].copy_from_slice(format!("{index:03}").as_bytes());
        publish(&mut client, TOPIC, 0, &payload, "")
            .await
            .expect("publish new");
    }
    // 给推送任务时间把队列填满并丢弃其余：best-effort 的丢弃必须可观测。
    tokio::time::sleep(Duration::from_millis(600)).await;

    let mut received = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_millis(300), stream.message()).await {
            Ok(Ok(Some(message))) => received.push(message),
            _ => break,
        }
        if received.len() >= 64 {
            break;
        }
    }
    assert!(
        !received.is_empty(),
        "the subscription must push at least one message"
    );
    assert!(
        received.iter().all(|message| message.payload.len() == 4096),
        "start_offset = 0 means latest: no history"
    );
    assert_eq!(
        received[0].offset, 1,
        "offsets start after the skipped history"
    );
    // 背压丢弃的计数语义由 `consume::tests::subscribe_drops_when_the_queue_is_full...`
    // 直接钉住（真实 h2 运输会把服务端队列抽干，端到端断言「一定丢弃」不可靠）。
    assert!(
        node.metric("coord_mq_v2_subscribe_messages_total") > 0,
        "the subscription must publish its delivered-message counter"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscribe_rejects_an_explicit_offset_below_earliest() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    node.propose_topic(MqTopicOp::Create(
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
    let meta = node.topic_row(TOPIC);
    let payloads: Vec<Vec<u8>> = (0..40).map(|_| vec![b'p'; 100]).collect();
    node.append_at(&meta, 0, coord_server::mq_v2::now_ms(), &payloads)
        .await;
    let truncated = node.truncate(&meta, 0, 32).await;
    assert!(truncated.earliest_offset > 0);

    let status = client
        .subscribe(mq_v2::SubscribeRequest {
            topic: TOPIC.to_string(),
            topic_epoch: 0,
            group: String::new(),
            partitions: Vec::new(),
            start_offset: 1,
        })
        .await
        .expect_err("an explicit offset below earliest must be rejected");
    assert_eq!(status.code(), Code::OutOfRange, "{status:?}");
    assert_eq!(
        coord_core::error_code::error_code_of(&status).as_deref(),
        Some("OFFSET_OUT_OF_RANGE")
    );
}
