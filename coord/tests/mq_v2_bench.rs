//! MQ v2 基准（M2-5 写路径 + M6-3 读/组/再平衡）：单节点多 Region 夹具上的
//! 真实 Raft 提交与 gRPC 数据面。
//!
//! 与 `perf_bench.rs` 同一口径：Markdown 行 + `ops/s` / 延迟百分位，**不进**默认测试
//! 门禁（`#[ignore]`）。运行：
//!
//! ```bash
//! PERF_GATE=1 cargo test --release -p coord --test mq_v2_bench \
//!   -- --ignored --nocapture --test-threads=1
//! ```
//!
//! `PERF_GATE=1` 是 fail-closed 的：没有它就直接失败（未跑不能伪装成通过）；
//! 阈值是**本机回归下限**（基线 ×0.6 左右），不是 SLO 校准。
//!
//! 口径说明：
//! - 顺序段测「单条 publish 的端到端延迟」（含 Raft 多数派 + apply）；
//! - 并发段测「同分片批量摊薄」：64 个客户端并发、每个串行 50 条 ⇒ 批深度 = 提交的
//!   请求数 / `MqAppendBatch` 条目数；
//! - 读路径（poll/ack）与再平衡段：M6-3 的正式数字；阈值断言只在
//!   `PERF_GATE=1` 下执行，报告记录基线、阈值与本机结论。

#[path = "support/mq_v2_node.rs"]
mod mq_v2_node;

use std::time::{Duration, Instant};

use coord_proto::mq::v2 as mq_v2;
use coord_proto::mq::v2::mq_client::MqClient;
use mq_v2_node::{create_topic, publish, MqV2Node};
use tonic::transport::Channel;

/// 性能门禁：没有 `PERF_GATE=1` 就当「未跑」，直接失败而不是静默跳过。
fn require_gate() {
    assert_eq!(
        std::env::var("PERF_GATE").ok().as_deref(),
        Some("1"),
        "perf suites are gated: set PERF_GATE=1 (missing gate must not look like a pass)"
    );
}

/// 本机回归下限（基线 ×0.6 左右；见 `docs/production/ops/mq-v2-performance.md`）。
const MIN_SEQUENTIAL_OPS: f64 = 200.0;
const MIN_CONCURRENT_OPS: f64 = 1_200.0;
const MIN_BATCH_DEPTH: f64 = 3.0;
const MIN_POLL_MSGS: f64 = 1_000.0;
const MIN_ACKS: f64 = 1_000.0;
const MAX_REBALANCE: Duration = Duration::from_secs(20);

const PAYLOAD_BYTES: usize = 128;
const SEQUENTIAL: usize = 1_000;
const CONCURRENCY: usize = 64;
const PER_TASK: usize = 50;

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let index = (((p / 100.0) * (sorted.len() - 1) as f64) as usize).min(sorted.len() - 1);
    sorted[index]
}

fn micros(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000_000.0
}

fn report(name: &str, count: usize, elapsed: Duration, latencies: &mut [Duration]) {
    latencies.sort_unstable();
    let ops_per_sec = count as f64 / elapsed.as_secs_f64();
    println!(
        "| {name} | {count} | {:.0} ops/s | p50 {:.0}µs | p99 {:.0}µs |",
        ops_per_sec,
        micros(percentile(latencies, 50.0)),
        micros(percentile(latencies, 99.0)),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "release-mode benchmark; run with --release --ignored"]
async fn bench_mq_v2_publish_throughput_and_latency() {
    require_gate();
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    let payload = vec![b'x'; PAYLOAD_BYTES];
    create_topic(&mut client, "bench", 1, None).await;

    // 预热：建段、建 raft 快照路径、填连接池。
    for _ in 0..50 {
        publish(&mut client, "bench", 0, &payload, "")
            .await
            .expect("warmup publish");
    }

    println!("\n| 场景 | 条数 | 吞吐 | 延迟 | 延迟 |");
    println!("|:--|--:|--:|--:|--:|");

    let mut sequential = Vec::with_capacity(SEQUENTIAL);
    let started = Instant::now();
    for _ in 0..SEQUENTIAL {
        let begin = Instant::now();
        publish(&mut client, "bench", 0, &payload, "")
            .await
            .expect("sequential publish");
        sequential.push(begin.elapsed());
    }
    let sequential_elapsed = started.elapsed();
    report(
        &format!("mq_v2_publish sequential ({PAYLOAD_BYTES}B, 1 client)"),
        SEQUENTIAL,
        sequential_elapsed,
        &mut sequential,
    );

    let batches_before = node.append_stats();
    let started = Instant::now();
    let mut tasks = Vec::with_capacity(CONCURRENCY);
    for _ in 0..CONCURRENCY {
        let mut client = node.client().await;
        let payload = payload.clone();
        tasks.push(tokio::spawn(async move {
            let mut latencies = Vec::with_capacity(PER_TASK);
            for _ in 0..PER_TASK {
                let begin = Instant::now();
                publish(&mut client, "bench", 0, &payload, "")
                    .await
                    .expect("concurrent publish");
                latencies.push(begin.elapsed());
            }
            latencies
        }));
    }
    let mut concurrent = Vec::with_capacity(CONCURRENCY * PER_TASK);
    for task in tasks {
        concurrent.extend(task.await.expect("publish task"));
    }
    let concurrent_elapsed = started.elapsed();
    report(
        &format!("mq_v2_publish concurrent ({CONCURRENCY} clients × {PER_TASK})"),
        CONCURRENCY * PER_TASK,
        concurrent_elapsed,
        &mut concurrent,
    );

    let (batches, requests) = node.append_stats();
    let batch_delta = batches.saturating_sub(batches_before.0);
    let request_delta = requests.saturating_sub(batches_before.1);
    let depth = if batch_delta == 0 {
        0.0
    } else {
        request_delta as f64 / batch_delta as f64
    };
    println!(
        "| mq_v2_append_batch depth (concurrent段) | {request_delta} requests | {batch_delta} raft entries | {depth:.2} req/entry | — |"
    );
    println!(
        "\n累计 `MqAppendBatch` 条目 {}、publish 请求 {}（含顺序段）\n",
        batches, requests
    );

    let sequential_ops = SEQUENTIAL as f64 / sequential_elapsed.as_secs_f64();
    let concurrent_ops = (CONCURRENCY * PER_TASK) as f64 / concurrent_elapsed.as_secs_f64();
    assert!(
        sequential_ops >= MIN_SEQUENTIAL_OPS,
        "sequential publish {sequential_ops:.0} ops/s below {MIN_SEQUENTIAL_OPS}"
    );
    assert!(
        concurrent_ops >= MIN_CONCURRENT_OPS,
        "concurrent publish {concurrent_ops:.0} ops/s below {MIN_CONCURRENT_OPS}"
    );
    assert!(
        depth >= MIN_BATCH_DEPTH,
        "append batch depth {depth:.2} req/entry below {MIN_BATCH_DEPTH}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "release-mode benchmark; run with --release --ignored"]
async fn bench_mq_v2_poll_and_ack_throughput() {
    require_gate();
    const MESSAGES: usize = 2_000;
    const POLL_BATCH: i32 = 200;
    const ACK_BATCH: usize = 256;

    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    let payload = vec![b'x'; PAYLOAD_BYTES];
    create_topic(&mut client, "pollbench", 1, None).await;
    for i in 0..MESSAGES {
        publish(&mut client, "pollbench", 0, &payload, &format!("idem-{i}"))
            .await
            .expect("publish for poll bench");
    }

    let joined = join(&mut client, "pollbench-group", &["pollbench"]).await;
    let mut next_offset = None;
    let mut offsets = Vec::with_capacity(MESSAGES);
    let polls_started = Instant::now();
    let mut polls = 0usize;
    loop {
        let response = poll(&mut client, "pollbench", 0, next_offset, POLL_BATCH, 0, 0).await;
        if response.messages.is_empty() {
            break;
        }
        for message in &response.messages {
            offsets.push(message.offset);
        }
        next_offset = Some(response.next_offset);
        polls += 1;
    }
    let polls_elapsed = polls_started.elapsed();
    assert_eq!(
        offsets.len(),
        MESSAGES,
        "poll must return every message once"
    );

    let acks_started = Instant::now();
    for chunk in offsets.chunks(ACK_BATCH) {
        let entries = chunk
            .iter()
            .map(|offset| mq_v2::AckEntry {
                topic: "pollbench".to_string(),
                partition: 0,
                topic_epoch: 0,
                offset: *offset,
            })
            .collect();
        let response = client
            .ack_batch(mq_v2::AckBatchRequest {
                group: "pollbench-group".to_string(),
                member_id: joined.member_id.clone(),
                generation: joined.generation,
                acks: entries,
            })
            .await
            .expect("AckBatch must succeed")
            .into_inner();
        assert_eq!(response.results.len(), chunk.len());
    }
    let acks_elapsed = acks_started.elapsed();

    let poll_rate = MESSAGES as f64 / polls_elapsed.as_secs_f64();
    let ack_rate = MESSAGES as f64 / acks_elapsed.as_secs_f64();
    println!("\n| 场景 | 条数 | 吞吐 | 延迟 | 延迟 |");
    println!("|:--|--:|--:|--:|--:|");
    println!(
        "| mq_v2_poll ({POLL_BATCH}/批, {polls} 批) | {MESSAGES} | {poll_rate:.0} msgs/s | — | — |"
    );
    println!("| mq_v2_ack_batch ({ACK_BATCH}/批) | {MESSAGES} | {ack_rate:.0} acks/s | — | — |\n");
    assert!(
        poll_rate >= MIN_POLL_MSGS,
        "poll {poll_rate:.0} msgs/s below {MIN_POLL_MSGS}"
    );
    assert!(
        ack_rate >= MIN_ACKS,
        "ack_batch {ack_rate:.0} acks/s below {MIN_ACKS}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "release-mode benchmark; run with --release --ignored"]
async fn bench_mq_v2_rebalance_time() {
    require_gate();
    const MEMBERS: usize = 4;

    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    create_topic(&mut client, "rebalancebench", MEMBERS as i32, None).await;

    let started = Instant::now();
    for expected in 1..=MEMBERS {
        join(&mut client, "rebalance-group", &["rebalancebench"]).await;
        wait_group_settled(&mut client, "rebalance-group", expected).await;
    }
    let elapsed = started.elapsed();

    let description = client
        .describe_group(mq_v2::DescribeGroupRequest {
            group: "rebalance-group".to_string(),
        })
        .await
        .expect("DescribeGroup")
        .into_inner();
    let assigned: usize = description
        .members
        .iter()
        .map(|member| member.assignments.len())
        .sum();
    assert_eq!(assigned, MEMBERS, "every partition assigned exactly once");
    println!(
        "\n| mq_v2_rebalance | {MEMBERS} members | {MEMBERS} partitions | {:.2}s | — |\n",
        elapsed.as_secs_f64()
    );
    assert!(
        elapsed < MAX_REBALANCE,
        "rebalance took {elapsed:?} (limit {MAX_REBALANCE:?})"
    );
}

async fn join(
    client: &mut MqClient<Channel>,
    group: &str,
    topics: &[&str],
) -> mq_v2::JoinGroupResponse {
    client
        .join_group(mq_v2::JoinGroupRequest {
            group: group.to_string(),
            member_id: String::new(),
            topics: topics.iter().map(|topic| topic.to_string()).collect(),
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
    max_messages: i32,
    max_bytes: i32,
    wait_ms: i32,
) -> mq_v2::PollResponse {
    client
        .poll(mq_v2::PollRequest {
            topic: topic.to_string(),
            partition,
            topic_epoch: 0,
            start_offset,
            max_messages,
            max_bytes,
            wait_ms,
        })
        .await
        .expect("Poll must succeed")
        .into_inner()
}

async fn wait_group_settled(client: &mut MqClient<Channel>, group: &str, expected_members: usize) {
    let deadline = tokio::time::Instant::now() + mq_v2_node::DEADLINE;
    loop {
        let description = client
            .describe_group(mq_v2::DescribeGroupRequest {
                group: group.to_string(),
            })
            .await
            .expect("DescribeGroup")
            .into_inner();
        // 分区数可以少于成员数：稳定态允许成员分到 0 个分区，但每个分区必须
        // 恰好被分配一次。
        let assigned: usize = description
            .members
            .iter()
            .map(|member| member.assignments.len())
            .sum();
        let settled = description.state == mq_v2::GroupState::Stable as i32
            && description.members.len() == expected_members
            && assigned == description.partitions.len();
        if settled {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "group did not settle with {expected_members} members: {description:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
