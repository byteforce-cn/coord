//! MQ v2 浸泡（M6-3）：真实 Raft 提交 + 组消费，持续 `MQ_V2_SOAK_SECS`（默认 600s）。
//!
//! 不变量（任何一条不成立即失败）：
//! 1. 每个 partition 的 offset 恒为 `0..published-1`（不丢、不重、不复用）；
//! 2. 消费到的 `(partition, offset)` 唯一集合 == 发布集合；
//! 3. 结束时 committed == published 且 `DescribeGroup` 的 lag 全 0；
//! 4. 进程 RSS 只记录不判定：夹具在进程内跑 Raft/MQ 状态机，默认 retain 7d 且
//!    idempotency 索引按保留窗口保存，10 分钟内 RSS 线性增长是**容量行为**而不是
//!    泄漏判据；泄漏检测需要真实进程 + 稳态窗口对比（M6-3 报告 §6 边界）。
//!
//! 运行（release；`--test-threads=1` 避免与其他基准抢 fsync）：
//! ```bash
//! PERF_GATE=1 MQ_V2_SOAK_SECS=600 cargo test --release -p coord \
//!   --test mq_v2_soak -- --ignored --nocapture --test-threads=1
//! ```
//! 没有 `PERF_GATE=1` 直接失败：未跑不能伪装成通过。

#[path = "support/mq_v2_node.rs"]
mod mq_v2_node;

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use coord_proto::mq::v2 as mq_v2;
use coord_proto::mq::v2::mq_client::MqClient;
use mq_v2_node::{create_topic, publish, MqV2Node};
use tonic::transport::Channel;

const TOPIC: &str = "soak";
const GROUP: &str = "soak-group";
const PARTITIONS: i32 = 2;
const PRODUCERS: usize = 4;
const PAYLOAD_BYTES: usize = 128;
const POLL_BATCH: i32 = 200;
const ACK_BATCH: usize = 256;

fn require_gate() {
    assert_eq!(
        std::env::var("PERF_GATE").ok().as_deref(),
        Some("1"),
        "perf suites are gated: set PERF_GATE=1 (missing gate must not look like a pass)"
    );
}

fn soak_secs() -> u64 {
    std::env::var("MQ_V2_SOAK_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(600)
}

fn rss_kib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find_map(|line| {
        line.strip_prefix("VmRSS:")?
            .trim()
            .trim_end_matches(" kB")
            .trim()
            .parse()
            .ok()
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "soak; run with PERF_GATE=1 (MQ_V2_SOAK_SECS overrides duration)"]
async fn soak_mq_v2_publish_consume_keeps_every_offset_exactly_once() {
    require_gate();
    let duration = Duration::from_secs(soak_secs());

    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    create_topic(&mut client, TOPIC, PARTITIONS, None).await;

    let joined = join(&mut client, GROUP).await;
    let assigned: Vec<i32> = joined
        .assignments
        .iter()
        .map(|assignment| assignment.partition)
        .collect();
    assert_eq!(assigned.len(), PARTITIONS as usize);
    let generation = Arc::new(std::sync::atomic::AtomicU64::new(joined.generation));
    let member_id = joined.member_id.clone();

    // 心跳：会话不能在长跑中过期（过期会推进代际，消费端的 Ack 会被拒绝）。
    let stop_heartbeat = Arc::new(AtomicBool::new(false));
    let heartbeat = {
        let mut client = node.client().await;
        let group = GROUP.to_string();
        let member_id = member_id.clone();
        let generation = Arc::clone(&generation);
        let stop = Arc::clone(&stop_heartbeat);
        tokio::spawn(async move {
            let mut failures = 0u32;
            while !stop.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(5)).await;
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                match client
                    .heartbeat(mq_v2::HeartbeatRequest {
                        group: group.clone(),
                        member_id: member_id.clone(),
                        generation: generation.load(Ordering::Relaxed),
                    })
                    .await
                {
                    Ok(response) => {
                        failures = 0;
                        let response = response.into_inner();
                        if response.rebalance_required {
                            panic!("soak member was rebalanced (single member must stay stable)");
                        }
                        generation.store(response.generation, Ordering::Relaxed);
                    }
                    Err(error) => {
                        failures += 1;
                        assert!(failures < 3, "heartbeat failed repeatedly: {error}");
                    }
                }
            }
        })
    };

    // 生产：每个 producer 独立连接，按 partition 轮转。
    let stop_producers = Arc::new(AtomicBool::new(false));
    let started = Instant::now();
    let rss_before = rss_kib();
    let mut producers = Vec::with_capacity(PRODUCERS);
    for producer in 0..PRODUCERS {
        let mut client = node.client().await;
        let stop = Arc::clone(&stop_producers);
        producers.push(tokio::spawn(async move {
            let payload = vec![b'x'; PAYLOAD_BYTES];
            let mut published: HashMap<i32, Vec<i64>> = HashMap::new();
            let mut sent = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let partition = (sent as i32) % PARTITIONS;
                let key = format!("soak-{producer}-{sent}");
                let response = publish(&mut client, TOPIC, partition, &payload, &key)
                    .await
                    .unwrap_or_else(|error| panic!("soak publish failed: {error}"));
                published
                    .entry(partition)
                    .or_default()
                    .push(response.offset);
                sent += 1;
            }
            published
        }));
    }

    // 消费：单 member 持有全部分区；每轮 poll + AckBatch。
    let mut consumer = node.client().await;
    let mut next_offsets: HashMap<i32, i64> = HashMap::new();
    let mut seen: HashSet<(i32, i64)> = HashSet::new();
    let mut acks = 0u64;
    let deadline = started + duration;
    while Instant::now() < deadline {
        let mut round_messages = 0usize;
        for partition in &assigned {
            let response = poll(
                &mut consumer,
                TOPIC,
                *partition,
                next_offsets.get(partition).copied(),
            )
            .await;
            round_messages += response.messages.len();
            for message in &response.messages {
                seen.insert((message.partition, message.offset));
            }
            next_offsets.insert(*partition, response.next_offset);
            if response.messages.is_empty() {
                continue;
            }
            for chunk in response.messages.chunks(ACK_BATCH) {
                let entries = chunk
                    .iter()
                    .map(|message| mq_v2::AckEntry {
                        topic: TOPIC.to_string(),
                        partition: message.partition,
                        topic_epoch: 0,
                        offset: message.offset,
                    })
                    .collect();
                consumer
                    .ack_batch(mq_v2::AckBatchRequest {
                        group: GROUP.to_string(),
                        member_id: member_id.clone(),
                        generation: generation.load(Ordering::Relaxed),
                        acks: entries,
                    })
                    .await
                    .expect("soak AckBatch must succeed");
                acks += chunk.len() as u64;
            }
        }
        let _ = round_messages;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    stop_producers.store(true, Ordering::Relaxed);

    // 先等生产者收尾，再排空：生产者最后一条可能在 stop 标志之后才返回。
    let mut published: HashMap<i32, Vec<i64>> = HashMap::new();
    for producer in producers {
        let result = producer.await.expect("producer task");
        for (partition, offsets) in result {
            published.entry(partition).or_default().extend(offsets);
        }
    }
    let published_total: usize = published.values().map(Vec::len).sum();
    let drain_deadline = Instant::now() + Duration::from_secs(60);
    while seen.len() < published_total && Instant::now() < drain_deadline {
        for partition in &assigned {
            let response = poll(
                &mut consumer,
                TOPIC,
                *partition,
                next_offsets.get(partition).copied(),
            )
            .await;
            for message in &response.messages {
                seen.insert((message.partition, message.offset));
            }
            next_offsets.insert(*partition, response.next_offset);
            for chunk in response.messages.chunks(ACK_BATCH) {
                let entries = chunk
                    .iter()
                    .map(|message| mq_v2::AckEntry {
                        topic: TOPIC.to_string(),
                        partition: message.partition,
                        topic_epoch: 0,
                        offset: message.offset,
                    })
                    .collect();
                consumer
                    .ack_batch(mq_v2::AckBatchRequest {
                        group: GROUP.to_string(),
                        member_id: member_id.clone(),
                        generation: generation.load(Ordering::Relaxed),
                        acks: entries,
                    })
                    .await
                    .expect("soak drain AckBatch must succeed");
                acks += chunk.len() as u64;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    stop_heartbeat.store(true, Ordering::Relaxed);
    heartbeat.abort();

    let rss_after = rss_kib();
    let elapsed = started.elapsed();

    assert!(published_total > 0, "soak published nothing");
    for (partition, offsets) in &published {
        let mut sorted = offsets.clone();
        sorted.sort_unstable();
        let expected: Vec<i64> = (0..sorted.len() as i64).collect();
        assert_eq!(
            sorted, expected,
            "partition {partition} has gaps or duplicates"
        );
    }
    assert_eq!(
        seen.len(),
        published_total,
        "consumed unique offsets != published count"
    );
    for (partition, offsets) in &published {
        for offset in offsets {
            assert!(
                seen.contains(&(*partition, *offset)),
                "published ({partition},{offset}) was never consumed"
            );
        }
    }

    let description = consumer
        .describe_group(mq_v2::DescribeGroupRequest {
            group: GROUP.to_string(),
        })
        .await
        .expect("DescribeGroup")
        .into_inner();
    assert!(
        description
            .partitions
            .iter()
            .all(|partition| partition.lag == 0),
        "lag must be 0 after the drain: {description:?}"
    );
    for partition in &assigned {
        let committed = consumer
            .get_committed_offsets(mq_v2::GetCommittedOffsetsRequest {
                group: GROUP.to_string(),
                partitions: vec![mq_v2::TopicPartition {
                    topic: TOPIC.to_string(),
                    partition: *partition,
                }],
            })
            .await
            .expect("GetCommittedOffsets")
            .into_inner()
            .offsets
            .first()
            .map(|offset| offset.offset)
            .unwrap_or(0);
        assert_eq!(
            committed,
            published.get(partition).map_or(0, |offsets| offsets.len()) as i64,
            "committed != published on partition {partition}"
        );
    }

    let rate = published_total as f64 / duration.as_secs_f64();
    println!("\n| soak | 时长 | 发布 | 消费 | ack | 吞吐 | RSS |");
    println!("|:--|--:|--:|--:|--:|--:|--:|");
    println!(
        "| mq_v2 publish→poll→ack | {:.0}s | {published_total} | {} | {acks} | {rate:.0} msgs/s | {} → {} KiB |\n",
        elapsed.as_secs_f64(),
        seen.len(),
        rss_before.map_or("?".to_string(), |kib| kib.to_string()),
        rss_after.map_or("?".to_string(), |kib| kib.to_string()),
    );
}

async fn join(client: &mut MqClient<Channel>, group: &str) -> mq_v2::JoinGroupResponse {
    client
        .join_group(mq_v2::JoinGroupRequest {
            group: group.to_string(),
            member_id: String::new(),
            topics: vec![TOPIC.to_string()],
            session_timeout_ms: 0,
        })
        .await
        .expect("JoinGroup must succeed")
        .into_inner()
}

async fn poll(
    client: &mut MqClient<Channel>,
    topic: &str,
    partition: i32,
    start_offset: Option<i64>,
) -> mq_v2::PollResponse {
    client
        .poll(mq_v2::PollRequest {
            topic: topic.to_string(),
            partition,
            topic_epoch: 0,
            start_offset,
            max_messages: POLL_BATCH,
            max_bytes: 0,
            wait_ms: 0,
        })
        .await
        .expect("Poll must succeed")
        .into_inner()
}
