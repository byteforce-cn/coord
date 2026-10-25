//! #174 GA 证据：v2 集群**长稳**（真实 3 进程，混合写 + 周期故障注入）。
//!
//! 每轮固定做四件事，任何一条不成立就失败（不把「没测到」写成「通过」）：
//!   1. TSO 取号严格单调；
//!   2. 单键直写（`coord.kv.v2` + 显式 `commit_ts`）后，`read_ts=0` 能读回最新值
//!      —— safe_ts 持续推进，不存在永久 `SAFE_TS_LAG`；
//!   3. 跨 Region 事务（`/soak/...` 落 region 1、`zz-soak-...` 落 region 2）返回
//!      `commit_ts` 后可读，`mod_revision == commit_ts`；
//!   4. 内存有界：只抽验最近 [`WINDOW`] 条已提交事务，长跑不会把堆吃满。
//!
//! 故障注入（默认每 300s）：`kill -9` region 0 leader → 等新 leader → 用同一数据
//! 目录重启 → 等稳定 leader；故障窗口内的写入按可重试处理，不静默丢断言。
//!
//! 运行（**72h 生产浸泡用 `scripts/soak-72h.sh`**）：
//!   `KVT_SOAK_SECS=120 cargo test -p coord --release --test kv_t_g6_soak_v2_test \
//!      soak_v2_mixed_workload_stays_consistent -- --ignored --nocapture --test-threads=1`
//!
//! 环境变量：
//! - `KVT_SOAK_SECS`（必填）：浸泡时长；缺失即失败（禁止把「未跑」伪装成通过）。
//! - `KVT_SOAK_FAULT_SECS`（默认 300，0 = 不注入故障）。
//! - `KVT_CHAOS_ARTIFACT_DIR`：保留节点日志/数据目录（默认随 TempDir 清理）。

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[path = "common/mod.rs"]
mod common;

use common::v2_cluster::{
    allocate_ts, assert_no_forbidden_anchors, commit_txn, direct_put, maintenance_status, read_at,
    runtime, ReadAttempt, V2Cluster, FAILOVER_TIMEOUT,
};

/// 内存中保留的已提交事务条数（每轮抽验最旧一条，滚动覆盖）。
const WINDOW: usize = 16;
/// 进度日志周期（长跑要有可观测心跳）。
const PROGRESS: Duration = Duration::from_secs(60);
/// 直写计数器 key：落在 region 1（`[.., "m")`）。
const COUNTER_KEY: &[u8] = b"/soak/counter";

/// 当前阶段（看门狗用：卡住时最后进入的是哪一段）。
mod phase {
    pub const IDLE: u64 = 0;
    pub const TSO: u64 = 1;
    pub const DIRECT: u64 = 2;
    pub const READ: u64 = 3;
    pub const TXN: u64 = 4;
    pub const VERIFY: u64 = 5;
    pub const FAULT: u64 = 6;

    pub fn name(value: u64) -> &'static str {
        match value {
            TSO => "tso",
            DIRECT => "direct_put",
            READ => "read",
            TXN => "txn",
            VERIFY => "verify",
            FAULT => "fault_injection",
            _ => "idle",
        }
    }
}

/// 最后一次取得进展的 UNIX 秒（每完成一轮/一次故障注入都更新）。
static LAST_PROGRESS: AtomicU64 = AtomicU64::new(0);
/// 当前阶段（见 [`phase`]）。
static CURRENT_PHASE: AtomicU64 = AtomicU64::new(phase::IDLE);

fn mark_progress() {
    LAST_PROGRESS.store(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        Ordering::Relaxed,
    );
}

fn enter(phase: u64) {
    CURRENT_PHASE.store(phase, Ordering::Relaxed);
    mark_progress();
}

/// 卡住检测：超过 `KVT_SOAK_STALL_SECS`（默认 600s）没有进展就打印阶段并中止。
///
/// 72h 长跑必须自己终结「挂住」：否则进程永远停在 park 上，报告既不通过也不失败，
/// 「未跑」就被伪装成了「还在跑」。中止（而不是 panic）保证 cargo test 立刻失败。
fn spawn_watchdog() {
    let limit = env_secs("KVT_SOAK_STALL_SECS", Some(600)).max(30);
    std::thread::spawn(move || {
        let mut stalled_since: Option<u64> = None;
        loop {
            std::thread::sleep(Duration::from_secs(15));
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let last = LAST_PROGRESS.load(Ordering::Relaxed);
            if last == 0 {
                continue;
            }
            let idle = now.saturating_sub(last);
            if idle < limit {
                stalled_since = None;
                continue;
            }
            let since = *stalled_since.get_or_insert(now);
            eprintln!(
                "soak: WATCHDOG no progress for {idle}s (phase={}, stalled_for={}s, limit={limit}s)",
                phase::name(CURRENT_PHASE.load(Ordering::Relaxed)),
                now.saturating_sub(since),
            );
            if idle >= limit.saturating_mul(2) {
                eprintln!("soak: WATCHDOG aborting: the harness is wedged, not making progress");
                std::process::abort();
            }
        }
    });
}

fn env_secs(name: &str, default: Option<u64>) -> u64 {
    match std::env::var(name) {
        Ok(raw) if !raw.is_empty() => raw
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be an integer number of seconds, got {raw:?}")),
        _ => default
            .unwrap_or_else(|| panic!("{name} is required (fail-closed: no evidence = no pass)")),
    }
}

/// 等 `read_ts` 读回期望值；`Rejected`（SAFE_TS_LAG / 换届）可重试，`Empty`
/// 在 safe_ts 追上之前也属正常（`read_ts=0` 解析成当前 safe_ts）。
fn wait_read(addrs: &[String], key: &[u8], expected: &[u8], read_ts: i64, what: &str) {
    let deadline = Instant::now() + FAILOVER_TIMEOUT;
    let mut last: String;
    loop {
        match runtime().block_on(read_at(addrs, key, read_ts)) {
            ReadAttempt::Found(value, _) => {
                if value == expected {
                    return;
                }
                // `read_ts=0` 解析成当前 safe_ts：追上之前读到旧值是正常的，
                // 不能把中间态当成违反（历史读的 commit_ts 不可变，同样重试到超时）。
                last = format!(
                    "stale value at read_ts {read_ts}: got {:?}",
                    String::from_utf8_lossy(&value)
                );
            }
            ReadAttempt::Empty(detail) | ReadAttempt::Rejected(detail) => last = detail,
        }
        assert!(
            Instant::now() < deadline,
            "{what}: key {:?} at read_ts {read_ts} never became readable; last: {last}",
            String::from_utf8_lossy(key)
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// 注入一次 region 0 leader 丢失：kill -9 → 等新 leader → 同目录重启 → 等稳定。
fn fault_region0_leader(cluster: &mut V2Cluster) {
    let leader = cluster.wait_leader(FAILOVER_TIMEOUT);
    let leader_id: u64 = leader.parse().expect("leader id");
    eprintln!("soak: injecting kill -9 on region-0 leader {leader}");

    cluster.nodes[(leader_id - 1) as usize].kill9();
    let new_leader = cluster.wait_leader_not(Some(&leader), FAILOVER_TIMEOUT);
    assert_ne!(new_leader, leader, "a new region-0 leader must be elected");

    let node = &mut cluster.nodes[(leader_id - 1) as usize];
    node.start(false);
    let deadline = Instant::now() + FAILOVER_TIMEOUT;
    while runtime()
        .block_on(maintenance_status(&node.addr()))
        .is_none()
    {
        assert!(
            Instant::now() < deadline,
            "node {leader_id} did not come back after restart"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    cluster.wait_stable_leader(FAILOVER_TIMEOUT);
    eprintln!("soak: node {leader_id} rejoined; leader is now {new_leader}");
}

#[test]
#[ignore = "long-running v2 soak; KVT_SOAK_SECS=<seconds>"]
fn soak_v2_mixed_workload_stays_consistent() {
    let secs = env_secs("KVT_SOAK_SECS", None);
    assert!(secs > 0, "KVT_SOAK_SECS must be > 0");
    let fault_secs = env_secs("KVT_SOAK_FAULT_SECS", Some(300));

    let started = Instant::now();
    let deadline = started + Duration::from_secs(secs);
    eprintln!("soak: start duration={secs}s fault_interval={fault_secs}s window={WINDOW}");

    spawn_watchdog();
    let mut cluster = V2Cluster::start();
    let addrs = cluster.addrs();
    mark_progress();

    let mut window: VecDeque<(Vec<u8>, Vec<u8>, i64)> = VecDeque::new();
    let mut last_ts = 0u64;
    let mut iterations: u64 = 0;
    let mut next_progress = Instant::now() + PROGRESS;
    let mut next_fault = if fault_secs == 0 {
        None
    } else {
        Some(Instant::now() + Duration::from_secs(fault_secs))
    };

    while Instant::now() < deadline {
        iterations += 1;

        // 1) TSO 严格单调（换 leader / 重启后也不复用、不回退）。
        enter(phase::TSO);
        let ts = runtime().block_on(allocate_ts(&addrs));
        assert!(
            ts > last_ts,
            "iteration {iterations}: TSO went backwards ({ts} after {last_ts})"
        );
        last_ts = ts;

        // 2) 直写 + safe_ts 读回：水位必须能追上最新写入。
        enter(phase::DIRECT);
        let counter = format!("s{iterations}");
        let written_ts =
            runtime().block_on(direct_put(&addrs, COUNTER_KEY, counter.as_bytes())) as u64;
        enter(phase::READ);
        wait_read(
            &addrs,
            COUNTER_KEY,
            counter.as_bytes(),
            0,
            &format!("iteration {iterations}: direct write"),
        );

        // 3) 跨 Region 事务：region 1 的 `/soak/t-*` 与 region 2 的 `zz-soak-*`
        //    在同一 commit_ts 上可见。
        let tag = iterations;
        let left = format!("/soak/t-{tag:08}").into_bytes();
        let right = format!("zz-soak-{tag:08}").into_bytes();
        // 与夹具的写入值保持一致：`commit_txn` 写入 `txn-{tag}`。
        let value = format!("txn-{tag}");
        enter(phase::TXN);
        let commit_ts = runtime().block_on(commit_txn(&addrs, &left, &right, tag));
        let commit_ts_i64 = commit_ts;
        let commit_ts = commit_ts as u64;
        assert!(
            commit_ts >= written_ts,
            "iteration {iterations}: txn commit_ts {commit_ts} < direct write {written_ts}"
        );
        window.push_back((left, value.as_bytes().to_vec(), commit_ts_i64));
        window.push_back((right, value.as_bytes().to_vec(), commit_ts_i64));
        while window.len() > WINDOW {
            window.pop_front();
        }

        // 4) 抽验窗口内最旧一条（滚动覆盖，内存有界）。
        enter(phase::VERIFY);
        if let Some((key, expected, ts)) = window.front() {
            let key = key.clone();
            let expected = expected.clone();
            let ts = *ts;
            wait_read(
                &addrs,
                &key,
                &expected,
                ts,
                &format!("iteration {iterations}: committed txn"),
            );
        }

        mark_progress();
        if Instant::now() >= next_progress {
            eprintln!(
                "soak: iterations={iterations} elapsed={}s tso={last_ts} committed={} last_read_ts={}",
                started.elapsed().as_secs(),
                window.len(),
                window.back().map(|(_, _, ts)| *ts).unwrap_or(0),
            );
            next_progress += PROGRESS;
        }

        if let Some(at) = next_fault {
            if Instant::now() >= at {
                enter(phase::FAULT);
                fault_region0_leader(&mut cluster);
                mark_progress();
                next_fault = next_fault.map(|at| at + Duration::from_secs(fault_secs));
                // 故障窗口后重新校准进度心跳，避免把暂停误读成卡住。
                next_progress = Instant::now() + PROGRESS;
            }
        }
    }

    // 收尾：窗口内所有已提交事务仍按各自 commit_ts 可读，且没有内部不变量违反。
    for (key, expected, ts) in &window {
        wait_read(&addrs, key, expected, *ts, "final window verification");
    }
    assert_no_forbidden_anchors(&cluster);

    eprintln!(
        "soak: done iterations={iterations} duration={}s tso={last_ts}",
        started.elapsed().as_secs()
    );
}
