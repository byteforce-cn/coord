//! MQ v2 写路径基准（M2-5）：单节点多 Region 夹具上的真实 Raft 提交。
//!
//! 与 `perf_bench.rs` 同一口径：Markdown 行 + `ops/s` / 延迟百分位，**不进**默认测试
//! 门禁（`#[ignore]`）。运行：
//!
//! ```bash
//! cargo test --release -p coord --test mq_v2_bench -- --ignored --nocapture --test-threads=1
//! ```
//!
//! 口径说明：
//! - 顺序段测「单条 publish 的端到端延迟」（含 Raft 多数派 + apply）；
//! - 并发段测「同分片批量摊薄」：64 个客户端并发、每个串行 50 条 ⇒ 批深度 = 提交的
//!   请求数 / `MqAppendBatch` 条目数；
//! - 数字只作 M6-3 正式报告的输入，不在这里断言阈值（阈值属性能 gate）。

#[path = "support/mq_v2_node.rs"]
mod mq_v2_node;

use std::time::{Duration, Instant};

use mq_v2_node::{create_topic, publish, MqV2Node};

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
}
