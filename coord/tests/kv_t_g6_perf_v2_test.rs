//! #174 GA 证据：v2 数据面的**实测入口**（真实 3 进程；`PERF_GATE=1` 才跑）。
//!
//! 只测、不改语义：每个用例起自己的集群（端口/目录隔离），打印一行可直接抄进
//! `docs/production/ops/performance.md` 的 `PERF ...` 结果。未跑 = 文件里没有
//! 对应数字，不允许用「应该差不多」补空。
//!
//! 运行（release；`--test-threads=1` 避免多个集群抢同一条 fsync 路径）：
//!   PERF_GATE=1 cargo test -p coord --release --test kv_t_g6_perf_v2_test \
//!     -- --ignored --nocapture --test-threads=1
//!
//! 覆盖：
//! 1. `perf_sequencer_throughput`：并发事务吞吐 / 平均批大小 / 冲突率（指标面）；
//! 2. `perf_occ_conflict_rate`：热 key 争用下的冲突率与重试放大；
//! 3. `perf_watch_end_to_end_lag`：commit 返回到客户端收到 watch 事件的延迟；
//! 4. `perf_write_amplification`：用户字节 → 磁盘字节（3 副本合计，含 raft 日志）。

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[path = "common/mod.rs"]
mod common;

use common::v2_cluster::{
    commit_txn, runtime, V2Cluster,
};

/// 证书/权限门禁：没有 `PERF_GATE=1` 就当「未跑」，直接失败而不是静默跳过。
fn require_gate() {
    assert_eq!(
        std::env::var("PERF_GATE").ok().as_deref(),
        Some("1"),
        "perf suites are gated: set PERF_GATE=1 (missing gate must not look like a pass)"
    );
}

/// 最小 HTTP GET：只用于抓本机节点的 `/metrics`（避免给测试引入 HTTP 依赖）。
fn metrics_text(http_port: u16) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", http_port)).expect("connect metrics port");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    write!(
        stream,
        "GET /metrics HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    let mut out = String::new();
    stream.read_to_string(&mut out).expect("read metrics");
    out
}

fn metric_value(text: &str, name: &str) -> Option<u64> {
    text.lines()
        .find_map(|line| line.strip_prefix(name)?.strip_prefix(' '))
        .and_then(|value| value.trim().parse::<u64>().ok())
}

fn dir_bytes(path: &Path) -> u64 {
    let mut total = 0u64;
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    for entry in entries.flatten() {
        let meta = entry.metadata().ok();
        if meta.as_ref().is_some_and(|m| m.is_dir()) {
            total += dir_bytes(&entry.path());
        } else {
            total += meta.map(|m| m.len()).unwrap_or(0);
        }
    }
    total
}

fn percentile(sorted: &[u128], p: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[index.min(sorted.len() - 1)]
}

/// 一次「不重试」的提交：冲突/故障原样返回，供 OCC 冲突统计与重试封装使用。
async fn try_commit_value(
    addrs: &[String],
    left: &[u8],
    right: &[u8],
    value: &[u8],
) -> Result<i64, String> {
    use coord_proto::txn::v2::txn_client::TxnClient;
    use coord_proto::txn::v2::{BeginRequest, CommitRequest, Mutation, ReadRequest};

    let mut last = String::from("no node");
    for addr in addrs {
        let Ok(endpoint) = tonic::transport::Channel::from_shared(format!("http://{addr}"))
        else {
            continue;
        };
        let Ok(channel) = endpoint
            .connect_timeout(Duration::from_secs(2))
            .connect()
            .await
        else {
            continue;
        };
        let mut txn = TxnClient::new(channel);
        let begin = match tokio::time::timeout(
            Duration::from_secs(5),
            txn.begin(BeginRequest {
                start_ts: 0,
                ttl_ms: 0,
            }),
        )
        .await
        {
            Ok(Ok(resp)) => resp.into_inner(),
            Ok(Err(status)) => {
                last = status.to_string();
                continue;
            }
            Err(_) => {
                last = "begin timed out".to_string();
                continue;
            }
        };
        let read = tokio::time::timeout(
            Duration::from_secs(5),
            txn.read(ReadRequest {
                txn_id: begin.txn_id.clone(),
                key: left.to_vec(),
                range_end: Vec::new(),
                limit: 0,
                keys_only: false,
            }),
        )
        .await
        .ok()
        .and_then(|r| r.ok());
        if read.is_none() {
            last = "read failed".to_string();
            continue;
        }
        let mutations = [left, right]
            .iter()
            .map(|key| Mutation {
                op: coord_proto::txn::v2::mutation::Op::Put as i32,
                key: key.to_vec(),
                range_end: Vec::new(),
                value: value.to_vec(),
                lease_id: 0,
            })
            .collect();
        return match tokio::time::timeout(
            Duration::from_secs(5),
            txn.commit(CommitRequest {
                txn_id: begin.txn_id.clone(),
                mutations,
                request_id: Vec::new(),
            }),
        )
        .await
        {
            Ok(Ok(resp)) => Ok(resp.into_inner().commit_ts),
            Ok(Err(status)) => Err(status.to_string()),
            Err(_) => Err("commit timed out".to_string()),
        };
    }
    Err(last)
}

fn try_commit(addrs: &[String], left: &[u8], right: &[u8], tag: u64) -> Result<i64, String> {
    runtime().block_on(try_commit_value(
        addrs,
        left,
        right,
        format!("occ-{tag}").as_bytes(),
    ))
}

/// 冲突/换届可重试的提交封装（限时；其它错误直接失败）。
fn commit_pair_value(addrs: &[String], left: &[u8], right: &[u8], value: &[u8]) -> i64 {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        match runtime().block_on(try_commit_value(addrs, left, right, value)) {
            Ok(ts) => return ts,
            Err(err) => {
                let retryable = err.contains("TXN_CONFLICT")
                    || err.to_lowercase().contains("conflict")
                    || err.to_lowercase().contains("not leader")
                    || err.to_lowercase().contains("unavailable")
                    || err.to_lowercase().contains("timed out");
                assert!(
                    retryable,
                    "unexpected commit error (not conflict/leadership): {err}"
                );
                assert!(
                    Instant::now() < deadline,
                    "commit never succeeded within 120s; last error: {err}"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
}

#[test]
#[ignore = "PERF_GATE=1; real 3-process v2 cluster"]
fn perf_sequencer_throughput() {
    require_gate();
    const CLIENTS: usize = 32;
    const TXNS_PER_CLIENT: usize = 25;

    let cluster = V2Cluster::start();
    let addrs = cluster.addrs();
    let leader: usize = cluster
        .wait_leader(Duration::from_secs(60))
        .parse()
        .expect("leader id");
    let http_port = cluster.nodes[leader - 1].http_port;

    // 预热：把 TSO/装配/fence 的首次成本排除在窗口外。
    let _ = runtime().block_on(commit_txn(&addrs, b"/perf/warm/a", b"zz-perf/warm/a", 0));
    let before = metrics_text(http_port);
    let seq_before = metric_value(&before, "coord_kvt_sequencer_txns_total").unwrap_or(0);
    let batches_before = metric_value(&before, "coord_kvt_sequencer_batches_total").unwrap_or(0);
    let conflicts_before =
        metric_value(&before, "coord_kvt_sequencer_conflicts_total").unwrap_or(0);

    let started = Instant::now();
    let latencies: Arc<std::sync::Mutex<Vec<u128>>> =
        Arc::new(std::sync::Mutex::new(Vec::with_capacity(CLIENTS * TXNS_PER_CLIENT)));
    let tag = Arc::new(AtomicU64::new(10_000));
    std::thread::scope(|scope| {
        for _ in 0..CLIENTS {
            let addrs = addrs.clone();
            let latencies = Arc::clone(&latencies);
            let tag = Arc::clone(&tag);
            scope.spawn(move || {
                runtime().block_on(async {
                    for _ in 0..TXNS_PER_CLIENT {
                        let id = tag.fetch_add(1, Ordering::Relaxed);
                        let left = format!("/perf/t/{id:08}").into_bytes();
                        let right = format!("zz-perf/t/{id:08}").into_bytes();
                        let started = Instant::now();
                        let _ = commit_txn(&addrs, &left, &right, id).await;
                        latencies.lock().unwrap().push(started.elapsed().as_micros());
                    }
                });
            });
        }
    });
    let elapsed = started.elapsed();
    let total = CLIENTS * TXNS_PER_CLIENT;

    let after = metrics_text(http_port);
    let seq_after = metric_value(&after, "coord_kvt_sequencer_txns_total").unwrap_or(0);
    let batches_after = metric_value(&after, "coord_kvt_sequencer_batches_total").unwrap_or(0);
    let conflicts_after = metric_value(&after, "coord_kvt_sequencer_conflicts_total").unwrap_or(0);
    let txns = seq_after.saturating_sub(seq_before);
    let batches = batches_after.saturating_sub(batches_before);
    let conflicts = conflicts_after.saturating_sub(conflicts_before);

    let mut sorted = latencies.lock().unwrap().clone();
    sorted.sort_unstable();
    println!(
        "PERF sequencer_throughput clients={CLIENTS} txns={total} elapsed_ms={} txn_per_s={:.0} \
         sequencer_txns={txns} sequencer_batches={batches} batch_avg={:.1} conflicts={conflicts} \
         conflict_pct={:.2} p50_ms={:.1} p95_ms={:.1} max_ms={:.1}",
        elapsed.as_millis(),
        total as f64 / elapsed.as_secs_f64(),
        txns as f64 / batches.max(1) as f64,
        conflicts as f64 * 100.0 / txns.max(1) as f64,
        percentile(&sorted, 0.50) as f64 / 1000.0,
        percentile(&sorted, 0.95) as f64 / 1000.0,
        percentile(&sorted, 1.0) as f64 / 1000.0,
    );
}

#[test]
#[ignore = "PERF_GATE=1; real 3-process v2 cluster"]
fn perf_occ_conflict_rate() {
    require_gate();
    const CLIENTS: usize = 16;
    const ATTEMPTS: usize = 12;

    let cluster = V2Cluster::start();
    let addrs = cluster.addrs();
    let hot = b"/perf/hot";
    let other = b"zz-perf/hot";

    let attempts = Arc::new(AtomicU64::new(0));
    let conflicts = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    std::thread::scope(|scope| {
        for _ in 0..CLIENTS {
            let addrs = addrs.clone();
            let attempts = Arc::clone(&attempts);
            let conflicts = Arc::clone(&conflicts);
            scope.spawn(move || {
                for tag in 0..ATTEMPTS {
                    attempts.fetch_add(1, Ordering::Relaxed);
                    match try_commit(&addrs, hot, other, tag as u64 + 1_000_000) {
                        Ok(_) => {}
                        Err(err) => {
                            let conflict = err.contains("TXN_CONFLICT")
                                || err.to_lowercase().contains("conflict");
                            if conflict {
                                conflicts.fetch_add(1, Ordering::Relaxed);
                            } else {
                                panic!("unexpected commit error under contention: {err}");
                            }
                        }
                    }
                }
            });
        }
    });
    let elapsed = started.elapsed();
    let attempts = attempts.load(Ordering::Relaxed);
    let conflicts = conflicts.load(Ordering::Relaxed);
    println!(
        "PERF occ_conflict_rate clients={CLIENTS} attempts={attempts} conflicts={conflicts} \
         conflict_pct={:.1} committed={} elapsed_ms={} attempts_per_s={:.1}",
        conflicts as f64 * 100.0 / attempts.max(1) as f64,
        attempts - conflicts,
        elapsed.as_millis(),
        attempts as f64 / elapsed.as_secs_f64(),
    );
}

#[test]
#[ignore = "PERF_GATE=1; real 3-process v2 cluster"]
fn perf_watch_end_to_end_lag() {
    use coord_proto::watch::v2::watch_client::WatchClient;
    use coord_proto::watch::v2::watch_request::Request as WatchFrame;
    use coord_proto::watch::v2::WatchCreateRequest;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::Request;

    require_gate();
    const EVENTS: usize = 25;

    let cluster = V2Cluster::start();
    let addrs = cluster.addrs();
    let addr = addrs[0].clone();

    // watch.v2 消费 Commit Log（事务决策）：直写（`kv.v2` + 显式 commit_ts）不写
    // CommitRecord，不会出现在 v2 watch 流里（ADR D12.1 的内部/高级路径）。因此
    // 这里用事务提交测「commit 返回 → 客户端收到同 commit_ts 事件」的延迟。
    let start_rev = runtime().block_on(commit_txn(&addrs, b"/perf/watch/start", b"zz-perf/watch/start", 0));
    let create = WatchCreateRequest {
        key: Vec::new(),   // 空 key = 全 keyspace
        range_end: Vec::new(),
        start_revision: start_rev + 1,
        start_cursor: None,
        prev_kv: false,
        filter: 1, // ALL
    };

    let handle = std::thread::spawn(move || {
        runtime().block_on(async move {
            let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
                .unwrap()
                .connect()
                .await
                .expect("connect watch");
            let mut client = WatchClient::new(channel);
            let (tx, rx) = tokio::sync::mpsc::channel(4);
            tx.send(coord_proto::watch::v2::WatchRequest {
                request: Some(WatchFrame::Create(create)),
            })
            .await
            .expect("send create");
            let mut stream = client
                .watch(Request::new(ReceiverStream::new(rx)))
                .await
                .expect("open watch")
                .into_inner();

            let mut lags = Vec::with_capacity(EVENTS);
            for index in 0..EVENTS {
                let left = format!("/perf/watch/{index:04}").into_bytes();
                let right = format!("zz-perf/watch/{index:04}").into_bytes();
                let commit_ts = commit_txn(&addrs, &left, &right, index as u64).await;
                let committed_at = Instant::now();
                loop {
                    let response =
                        tokio::time::timeout(Duration::from_secs(20), stream.message())
                            .await
                            .expect("watch response timed out")
                            .expect("watch stream error")
                            .expect("watch stream closed early");
                    if response.events.iter().any(|event| event.commit_ts == commit_ts) {
                        lags.push(committed_at.elapsed().as_micros());
                        break;
                    }
                }
            }
            drop(tx);
            lags.sort_unstable();
            println!(
                "PERF watch_end_to_end_lag events={} p50_ms={:.2} p95_ms={:.2} max_ms={:.2}",
                lags.len(),
                percentile(&lags, 0.50) as f64 / 1000.0,
                percentile(&lags, 0.95) as f64 / 1000.0,
                percentile(&lags, 1.0) as f64 / 1000.0,
            );
        });
    });
    handle.join().expect("watch probe");
}

#[test]
#[ignore = "PERF_GATE=1; real 3-process v2 cluster"]
fn perf_write_amplification() {
    require_gate();
    const WARMUP: usize = 300;
    const TXNS: usize = 300;
    const VALUE_BYTES: usize = 256;

    let cluster = V2Cluster::start();
    let addrs = cluster.addrs();
    let value = vec![b'x'; VALUE_BYTES];

    // 预热让 raft 日志/redb 增长进入稳态（冷启动的预分配会污染放大比）。
    for tag in 0..WARMUP {
        let left = format!("/perf/amp/warm/{tag:08}").into_bytes();
        let right = format!("zz-perf/amp/warm/{tag:08}").into_bytes();
        let _ = commit_pair_value(&addrs, &left, &right, &value);
    }
    let before: u64 = cluster
        .nodes
        .iter()
        .map(|node| dir_bytes(&node.data_dir))
        .sum();
    for tag in 0..TXNS {
        let left = format!("/perf/amp/{tag:08}").into_bytes();
        let right = format!("zz-perf/amp/{tag:08}").into_bytes();
        let _ = commit_pair_value(&addrs, &left, &right, &value);
    }
    let after: u64 = cluster
        .nodes
        .iter()
        .map(|node| dir_bytes(&node.data_dir))
        .sum();
    let user_bytes = (TXNS * 2 * VALUE_BYTES) as u64;
    let written = after.saturating_sub(before);
    println!(
        "PERF write_amplification txns={TXNS} user_bytes_total={user_bytes} disk_bytes={written} \
         amplification={:.2}x disk_bytes_per_txn={:.0}",
        written as f64 / user_bytes as f64,
        written as f64 / TXNS as f64,
    );
}
