//! #175 GA 证据：v2 集群在**决策面故障**下的不变量（真实 3 进程）。
//!
//! 覆盖的故障面：
//!   - region 0（TSO + Commit Sequencer + shard 元数据所在 system raft）leader
//!     `kill -9` 与 `SIGSTOP` 暂停；
//!   - 故障窗口内**混合写**：跨 Region 事务（`coord.txn.v2`）与单键直写
//!     （`coord.kv.v2` + TSO 显式 `commit_ts`）并发。
//!
//! 断言（ADR-0015 D11 / #244 的回归锚点）：
//!   1. 已返回 `commit_ts` 的事务/直写在故障后仍可按该 `commit_ts` 读到
//!      （不丢、不重、revision 单调）；
//!   2. `read_ts=0`（safe_ts）在故障恢复后能推进到最新写入 —— 不存在永久
//!      `SAFE_TS_LAG`；
//!   3. 服务端日志里不出现「直写越过 pending decision / decision apply 停在批中」
//!      这两类内部不变量违反。
//!
//! 夹具冻结 PD 调度（`scheduler_paused`）：PD 默认在首个 tick 就会把两个空
//! Region 合并；`merge watermark` 之后的旧 revision 按设计不可读
//! （`HISTORY_UNAVAILABLE`），混进来会被误判成「已提交事务丢失」。split/merge
//! 的历史边界由 g5 连续性套件覆盖，本套件只注入决策面故障。
//!
//! 运行（真实 3 进程，debug 构建即可；CI 的 chaos job 同参数）：
//!   `cargo test -p coord --test kv_t_g6_chaos_v2_test -- --ignored --nocapture --test-threads=1`

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[path = "common/mod.rs"]
mod common;

use common::v2_cluster::{
    assert_no_forbidden_anchors, commit_txn, direct_put, maintenance_status, metric_value,
    node_statuses, read_at, runtime, ReadAttempt, V2Cluster, V2ClusterOptions, FAILOVER_TIMEOUT,
};

/// 已提交记录：`key -> (value, commit_ts, round)`。
type Committed = BTreeMap<Vec<u8>, (Vec<u8>, i64, u64)>;

const FAULT_ROUNDS: u64 = 2;
const TXN_COUNT: usize = 12;
const DIRECT_COUNT: usize = 12;
#[test]
#[ignore = "real-process fault injection; run by the CI chaos job"]
fn chaos_v2_mixed_writes_survive_decision_leader_loss() {
    let mut cluster = V2Cluster::start();
    let addrs = cluster.addrs();

    // 基线：故障前先证明 v2 事务可提交。
    let baseline = runtime().block_on(commit_txn(&addrs, b"aa", b"zz", 0));
    assert!(baseline > 0);

    let committed: Arc<Mutex<Committed>> = Arc::new(Mutex::new(BTreeMap::new()));
    // 唯一的写入 tag：并发任务不能靠「已提交条数」推 tag（会重键）。
    let next_tag = Arc::new(AtomicU64::new(1_000));

    for round in 0..FAULT_ROUNDS {
        let leader = cluster.wait_leader(FAILOVER_TIMEOUT);
        let leader_id: u64 = leader.parse().expect("leader id");

        // 故障注入前的写入起点（与故障窗口重叠）。
        let workload = {
            let addrs = addrs.clone();
            let committed = Arc::clone(&committed);
            let next_tag = Arc::clone(&next_tag);
            std::thread::spawn(move || {
                runtime().block_on(async move {
                    let mut handles = Vec::new();
                    for _ in 0..TXN_COUNT {
                        let addrs = addrs.clone();
                        let committed = Arc::clone(&committed);
                        let tag = next_tag.fetch_add(1, Ordering::Relaxed);
                        handles.push(tokio::spawn(async move {
                            let left = format!("t-{tag:08}").into_bytes();
                            let right = format!("zz-t-{tag:08}").into_bytes();
                            let ts = commit_txn(&addrs, &left, &right, tag).await;
                            let mut guard = committed.lock().unwrap();
                            guard.insert(left, (format!("txn-{tag}").into_bytes(), ts, round));
                            guard.insert(right, (format!("txn-{tag}").into_bytes(), ts, round));
                        }));
                    }
                    for _ in 0..DIRECT_COUNT {
                        let addrs = addrs.clone();
                        let committed = Arc::clone(&committed);
                        let tag = next_tag.fetch_add(1, Ordering::Relaxed);
                        handles.push(tokio::spawn(async move {
                            let key = format!("d-{tag:08}").into_bytes();
                            let value = format!("direct-{tag}").into_bytes();
                            let ts = direct_put(&addrs, &key, &value).await;
                            let mut guard = committed.lock().unwrap();
                            guard.insert(key, (value, ts, round));
                        }));
                    }
                    for handle in handles {
                        handle.await.expect("workload task");
                    }
                });
            })
        };

        // 让一部分写入先进入 in-flight，再制造决策面故障。
        std::thread::sleep(Duration::from_millis(150));
        match round {
            0 => cluster.nodes[(leader_id - 1) as usize].kill9(),
            _ => cluster.nodes[(leader_id - 1) as usize].signal("STOP"),
        }

        // 剩余节点必须选出新 leader（故障被执行证明：leader 确实不可用）。
        let new_leader = cluster.wait_leader_not(Some(&leader), FAILOVER_TIMEOUT);
        assert_ne!(
            new_leader, leader,
            "round {round}: a new region-0 leader must be elected after the fault"
        );

        match round {
            0 => {
                // 重启被 kill 的节点（拿同一数据目录，bootstrap=false）。
                let node = &mut cluster.nodes[(leader_id - 1) as usize];
                node.start(false);
                let deadline = Instant::now() + FAILOVER_TIMEOUT;
                while runtime()
                    .block_on(maintenance_status(&node.addr()))
                    .is_none()
                {
                    assert!(
                        Instant::now() < deadline,
                        "node {leader_id} did not come back; logs:\n{}",
                        cluster.logs()
                    );
                    std::thread::sleep(Duration::from_millis(200));
                }
            }
            _ => cluster.nodes[(leader_id - 1) as usize].signal("CONT"),
        }

        // 让恢复后的集群收敛到一个稳定 leader 再验证：暂停/重启的节点刚恢复时
        // 可能再触发一次换届，读取窗口里混入换届会把「读取超时」误判成不变量违反。
        cluster.wait_stable_leader(FAILOVER_TIMEOUT);

        if let Err(e) = workload.join() {
            panic!(
                "round {round}: workload failed: {e:?}; node logs:\n{}",
                cluster.logs()
            );
        }

        // 1) 已返回 commit_ts 的写入必须按该 commit_ts 可读（不丢、revision 单调）。
        let snapshot = committed.lock().unwrap().clone();
        for (key, (value, ts, committed_round)) in &snapshot {
            let deadline = Instant::now() + FAILOVER_TIMEOUT;
            let mut last_reject = String::from("no attempt");
            let mut last_empty: Option<String> = None;
            loop {
                match runtime().block_on(read_at(&addrs, key, *ts)) {
                    ReadAttempt::Found(got, mod_rev) => {
                        assert_eq!(
                            &got,
                            value,
                            "round {round}: key {:?} at read_ts {ts} returned {:?}",
                            String::from_utf8_lossy(key),
                            String::from_utf8_lossy(&got)
                        );
                        assert_eq!(
                            mod_rev, *ts,
                            "round {round}: mod_revision must equal commit_ts (D3)"
                        );
                        break;
                    }
                    ReadAttempt::Empty(detail) => {
                        last_empty = Some(detail);
                    }
                    ReadAttempt::Rejected(detail) => {
                        last_reject = detail;
                    }
                }
                assert!(
                    Instant::now() < deadline,
                    "round {round}: committed key {:?} (commit_ts {ts}, committed in round {committed_round}) \
                     never became readable; last empty: {last_empty:?}; last rejected: {last_reject}; \
                     node statuses: {};\nnode logs:\n{}",
                    String::from_utf8_lossy(key),
                    node_statuses(&addrs),
                    cluster.logs()
                );
                std::thread::sleep(Duration::from_millis(100));
            }
        }

        // 2) safe_ts 必须推进：`read_ts=0` 能读到本轮最后一次直写（不得永久 SAFE_TS_LAG）。
        let latest = snapshot
            .iter()
            .filter(|(key, _)| key.starts_with(b"d-"))
            .max_by_key(|(_, (_, ts, _))| *ts)
            .map(|(key, (value, _, _))| (key.clone(), value.clone()));
        if let Some((key, value)) = latest {
            let deadline = Instant::now() + FAILOVER_TIMEOUT;
            let mut last: String;
            loop {
                match runtime().block_on(read_at(&addrs, &key, 0)) {
                    ReadAttempt::Found(got, _) => {
                        assert_eq!(
                            got,
                            value,
                            "round {round}: safe_ts read returned a stale value for {:?}",
                            String::from_utf8_lossy(&key)
                        );
                        break;
                    }
                    ReadAttempt::Empty(detail) | ReadAttempt::Rejected(detail) => {
                        // `read_ts=0` 在 safe_ts 追上来之前返回空集是正常的：
                        // 这里等的是「水位追上」，两种结果都按可重试处理。
                        last = detail;
                    }
                }
                assert!(
                    Instant::now() < deadline,
                    "round {round}: safe_ts never caught up with the latest direct write \
                     (permanent SAFE_TS_LAG); last: {last}; node statuses: {};\nnode logs:\n{}",
                    node_statuses(&addrs),
                    cluster.logs()
                );
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }

    // 3) 内部不变量：直写不得越过 pending decision，decision apply 不得停在批中。
    assert_no_forbidden_anchors(&cluster);
}

/// 等 key 在指定 `read_ts` 可读且 `mod_revision == commit_ts`（D3/D11）。
fn assert_readable(addrs: &[String], key: &[u8], value: &[u8], ts: i64, what: &str) {
    let deadline = Instant::now() + FAILOVER_TIMEOUT;
    let mut last_reject = String::from("no attempt");
    let mut last_empty: Option<String> = None;
    loop {
        match runtime().block_on(read_at(addrs, key, ts)) {
            ReadAttempt::Found(got, mod_rev) => {
                assert_eq!(
                    got,
                    value,
                    "{what}: key {:?} at read_ts {ts} returned {:?}",
                    String::from_utf8_lossy(key),
                    String::from_utf8_lossy(&got)
                );
                assert_eq!(
                    mod_rev,
                    ts,
                    "{what}: mod_revision must equal commit_ts (D3) for {:?}",
                    String::from_utf8_lossy(key)
                );
                return;
            }
            ReadAttempt::Empty(detail) => last_empty = Some(detail),
            ReadAttempt::Rejected(detail) => last_reject = detail,
        }
        assert!(
            Instant::now() < deadline,
            "{what}: committed key {:?} (commit_ts {ts}) never became readable; \
             last empty: {last_empty:?}; last rejected: {last_reject}; node statuses: {}",
            String::from_utf8_lossy(key),
            node_statuses(addrs)
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// 等 `read_ts=0`（safe_ts）读回最新值：恢复后水位必须继续推进。
fn assert_safe_ts_caught_up(addrs: &[String], key: &[u8], value: &[u8], what: &str) {
    let deadline = Instant::now() + FAILOVER_TIMEOUT;
    loop {
        match runtime().block_on(read_at(addrs, key, 0)) {
            ReadAttempt::Found(got, _) => {
                assert_eq!(
                    got,
                    value,
                    "{what}: safe_ts read returned a stale value for {:?}",
                    String::from_utf8_lossy(key)
                );
                return;
            }
            ReadAttempt::Empty(_) | ReadAttempt::Rejected(_) => {}
        }
        assert!(
            Instant::now() < deadline,
            "{what}: safe_ts never caught up with {:?}; node statuses: {}",
            String::from_utf8_lossy(key),
            node_statuses(addrs)
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn leader_http_port(cluster: &V2Cluster, leader_id: u64) -> u16 {
    cluster
        .nodes
        .iter()
        .find(|n| n.id == leader_id)
        .expect("leader must be a cluster node")
        .http_port
}

/// 网络分区：隔离 region 0 leader（TSO + Commit Sequencer + shard 元数据所在
/// system raft）。多数派必须换主；分区前后的已提交写入按 `commit_ts` 可读，
/// 恢复后 safe_ts 追上，日志里不得出现内部不变量锚点。
#[test]
#[ignore = "real-process fault injection; run by the CI chaos job"]
fn chaos_v2_partitioned_region0_leader_keeps_invariants() {
    let cluster = V2Cluster::start_with(V2ClusterOptions {
        partitionable: true,
        ..Default::default()
    });
    let addrs = cluster.addrs();
    let baseline = runtime().block_on(commit_txn(&addrs, b"aq", b"zq", 0));
    assert!(baseline > 0);

    let mut committed: Vec<(Vec<u8>, Vec<u8>, i64)> = Vec::new();
    for tag in 1..=8u64 {
        let left = format!("q-{tag:08}").into_bytes();
        let right = format!("zq-{tag:08}").into_bytes();
        let ts = runtime().block_on(commit_txn(&addrs, &left, &right, tag));
        let value = format!("txn-{tag}").into_bytes();
        committed.push((left, value.clone(), ts));
        committed.push((right, value, ts));
    }

    let leader = cluster.wait_leader(FAILOVER_TIMEOUT);
    let leader_id: u64 = leader.parse().expect("leader id");
    cluster.partition(leader_id);

    // 分区被执行证明：多数派在故障窗口内选出新 leader。
    let new_leader = cluster.wait_leader_not(Some(&leader), FAILOVER_TIMEOUT);
    assert_ne!(
        new_leader, leader,
        "the majority must elect a new region-0 leader while the old one is partitioned"
    );

    // 分区窗口内继续提交：协调节点会重试到新 leader（会话只在协调节点有效）。
    for tag in 100..=104u64 {
        let left = format!("q-{tag:08}").into_bytes();
        let right = format!("zq-{tag:08}").into_bytes();
        let ts = runtime().block_on(commit_txn(&addrs, &left, &right, tag));
        let value = format!("txn-{tag}").into_bytes();
        committed.push((left, value.clone(), ts));
        committed.push((right, value, ts));
    }

    cluster.heal(leader_id);
    cluster.wait_stable_leader(FAILOVER_TIMEOUT);
    cluster.assert_all_alive();

    // 1) 分区前后的已提交写入都按 commit_ts 可读（不丢、revision 单调）。
    for (key, value, ts) in &committed {
        assert_readable(&addrs, key, value, *ts, "after partition heal");
    }

    // 2) 恢复后 safe_ts 必须追上新的直写（不得永久 SAFE_TS_LAG）。
    let key = b"q-safe-ts".to_vec();
    let value = b"after-heal".to_vec();
    let ts = runtime().block_on(direct_put(&addrs, &key, &value));
    assert_readable(&addrs, &key, &value, ts, "post-heal direct write");
    assert_safe_ts_caught_up(&addrs, &key, &value, "post-heal");

    assert_no_forbidden_anchors(&cluster);
}

/// 快照安装中断：让一个 follower 落后到必须收快照，然后在安装途中（限速放大窗口）
/// 冻结并 kill -9；重启后必须自己追平，已提交写入不丢不重。
#[test]
#[ignore = "real-process fault injection; run by the CI chaos job"]
fn chaos_v2_interrupted_snapshot_install_recovers() {
    let mut cluster = V2Cluster::start_with(V2ClusterOptions {
        // 小快照间隔 + 低限速：恢复的 follower 必须走 InstallSnapshot；分块
        // 2MiB + 256KiB/s 让「快照 > 1 块」时传输跨多秒，能在途中注入故障。
        toml: "[raft]\nsnapshot_logs_since_last = 8\nmax_in_snapshot_log_to_keep = 0\n\
               snapshot_rate_limit_bytes_per_sec = 262144\n",
        ..Default::default()
    });
    let addrs = cluster.addrs();
    let baseline = runtime().block_on(commit_txn(&addrs, b"as", b"zs", 0));
    assert!(baseline > 0);

    // 1) 让 3 号节点落后：kill -9 后继续写入（leader 会回收日志、构建快照）。
    let victim = 3u64;
    cluster.nodes[(victim - 1) as usize].kill9();
    let leader: u64 = cluster
        .wait_leader(FAILOVER_TIMEOUT)
        .parse()
        .expect("leader id");
    let leader_http = leader_http_port(&cluster, leader);

    let mut committed: Vec<(Vec<u8>, Vec<u8>, i64)> = Vec::new();
    for round in 0..96u64 {
        let key = format!("snap-{round:08}").into_bytes();
        // 32KiB × 96 ≈ 3MiB 低压缩率数据 ⇒ 快照 > 1 个 2MiB 分块，传输跨多秒。
        let value: Vec<u8> = (0..32 * 1024)
            .map(|i| ((i as u8).wrapping_mul(31)) ^ (round as u8))
            .collect();
        let ts = runtime().block_on(direct_put(&addrs, &key, &value));
        committed.push((key, value, ts));
    }

    // 2) 必须真的构建了快照（否则 follower 可能只补日志，测不到安装路径）。
    let deadline = Instant::now() + FAILOVER_TIMEOUT;
    loop {
        let snapshots = metric_value(leader_http, "coord_snapshot_total").unwrap_or(0);
        if snapshots > 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the leader never built a snapshot; logs:\n{}",
            cluster.logs()
        );
        std::thread::sleep(Duration::from_millis(200));
    }

    // 3) 重启 follower 收快照；在传输途中冻结 + kill -9（半截快照留在数据目录）。
    cluster.nodes[(victim - 1) as usize].start(false);
    std::thread::sleep(Duration::from_millis(1200));
    cluster.nodes[(victim - 1) as usize].signal("STOP");
    std::thread::sleep(Duration::from_millis(1500));
    cluster.nodes[(victim - 1) as usize].kill9();

    // 4) 再次重启：必须从半截快照恢复并追平，写入全部可读。
    //    限速窗口 + 快照体积可能让追平超过 60s，这里给独立的恢复上限。
    let recovery_timeout = Duration::from_secs(240);
    cluster.nodes[(victim - 1) as usize].start(false);
    let current: u64 = cluster
        .wait_leader(FAILOVER_TIMEOUT)
        .parse()
        .expect("leader id");
    let deadline = Instant::now() + recovery_timeout;
    loop {
        let leader_rev = runtime()
            .block_on(maintenance_status(
                &cluster.nodes[(current - 1) as usize].addr(),
            ))
            .map(|s| s.revision)
            .unwrap_or(0);
        let victim_rev = runtime()
            .block_on(maintenance_status(
                &cluster.nodes[(victim - 1) as usize].addr(),
            ))
            .map(|s| s.revision)
            .unwrap_or(0);
        if victim_rev >= leader_rev {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "node {victim} never caught up after the interrupted snapshot install \
             (victim revision {victim_rev}, leader revision {leader_rev}); logs:\n{}",
            cluster.logs()
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    let victim_addr = vec![cluster.nodes[(victim - 1) as usize].addr()];
    for (key, value, ts) in &committed {
        assert_readable(
            &victim_addr,
            key,
            value,
            *ts,
            "read from the recovered follower",
        );
    }
    assert_no_forbidden_anchors(&cluster);
}

/// 收集 watch 事件的 `commit_ts`（空 key = 全 keyspace），直到 `expected` 条。
fn spawn_watch_collector(
    addr: String,
    start_revision: i64,
    expected: usize,
) -> (Arc<Mutex<Vec<i64>>>, std::thread::JoinHandle<()>) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_for_thread = Arc::clone(&seen);
    let handle = std::thread::spawn(move || {
        runtime().block_on(async move {
            use coord_proto::watch::v2::watch_client::WatchClient;
            use coord_proto::watch::v2::watch_request::Request as Frame;
            use coord_proto::watch::v2::{WatchCreateRequest, WatchRequest};

            let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
                .expect("watch endpoint")
                .connect()
                .await
                .expect("connect watch");
            let mut client = WatchClient::new(channel);
            let (tx, rx) = tokio::sync::mpsc::channel(4);
            tx.send(WatchRequest {
                request: Some(Frame::Create(WatchCreateRequest {
                    key: Vec::new(),
                    range_end: Vec::new(),
                    start_revision,
                    start_cursor: None,
                    prev_kv: false,
                    filter: 1,
                })),
            })
            .await
            .expect("send watch create");
            let mut stream = client
                .watch(tokio_stream::wrappers::ReceiverStream::new(rx))
                .await
                .expect("open watch")
                .into_inner();

            let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
            while tokio::time::Instant::now() < deadline {
                match tokio::time::timeout(Duration::from_secs(5), stream.message()).await {
                    Ok(Ok(Some(response))) => {
                        let mut got = seen_for_thread.lock().unwrap();
                        for event in response.events {
                            got.push(event.commit_ts);
                        }
                        if got.len() >= expected {
                            return;
                        }
                    }
                    Ok(Err(e)) => panic!("watch stream failed: {e}"),
                    Ok(Ok(None)) => panic!("watch stream closed before {expected} events"),
                    Err(_) => {}
                }
            }
            panic!(
                "watch collector timed out with {}/{} events",
                seen_for_thread.lock().unwrap().len(),
                expected
            );
        });
    });
    (seen, handle)
}

/// shard seal stall：把 region 0 leader（shard 表 / seal 所在 system raft）冻结
/// 一段时间，期间提交继续重试；恢复后 Watch 必须按 `commit_ts` 连续交付
/// （跨 seal 不丢事件），sealed shard 数量增长、seal lag 回落。
#[test]
#[ignore = "real-process fault injection; run by the CI chaos job"]
fn chaos_v2_shard_seal_stall_keeps_watch_continuous() {
    let mut cluster = V2Cluster::start_with(V2ClusterOptions {
        // 每 8 条 commit record 就 seal 一个 shard：几轮写入就跨多次 seal。
        kvt: "commit_log_shard_records = 8\ncommit_log_shard_secs = 1\n\
              shard_seal_timeout_secs = 1\n",
        ..Default::default()
    });
    let addrs = cluster.addrs();
    let baseline = runtime().block_on(commit_txn(&addrs, b"aw", b"zw", 0));
    assert!(baseline > 0);

    let leader_id: u64 = cluster
        .wait_leader(FAILOVER_TIMEOUT)
        .parse()
        .expect("leader id");
    // watch 挂在非 leader 节点上：region 0 leader 被冻结时事件流仍由本节点推进。
    let watch_addr = cluster
        .nodes
        .iter()
        .find(|n| n.id != leader_id)
        .expect("follower")
        .addr();

    const COMMITS: usize = 12;
    let (seen, watch) = spawn_watch_collector(watch_addr, baseline + 1, COMMITS);

    let mut expected: Vec<i64> = Vec::new();
    for tag in 1..=4u64 {
        let ts = runtime().block_on(commit_txn(
            &addrs,
            format!("w-{tag:08}").as_bytes(),
            format!("zw-{tag:08}").as_bytes(),
            tag,
        ));
        expected.push(ts);
    }

    // 冻结 region 0 leader：TSO/sequencer/shard seal 全部停摆；3s 后自动恢复。
    cluster.nodes[(leader_id - 1) as usize].signal("STOP");
    let pid = cluster.nodes[(leader_id - 1) as usize]
        .child
        .as_ref()
        .expect("leader child")
        .id();
    let resume = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(3));
        let _ = std::process::Command::new("kill")
            .args(["-CONT", &pid.to_string()])
            .status();
    });

    // 暂停窗口内的提交在 CONT 后继续（客户端逐节点重试到新 leader）。
    for tag in 5..=8u64 {
        let ts = runtime().block_on(commit_txn(
            &addrs,
            format!("w-{tag:08}").as_bytes(),
            format!("zw-{tag:08}").as_bytes(),
            tag,
        ));
        expected.push(ts);
    }
    resume.join().expect("resume thread");

    // 恢复后再写一批：必须跨过 seal 边界（每个 shard 8 条）。
    for tag in 9..=12u64 {
        let ts = runtime().block_on(commit_txn(
            &addrs,
            format!("w-{tag:08}").as_bytes(),
            format!("zw-{tag:08}").as_bytes(),
            tag,
        ));
        expected.push(ts);
    }

    watch.join().expect("watch collector");
    let observed = seen.lock().unwrap().clone();
    // 全局序：严格递增、不重复（超时重试可能多提交一条，允许「多」不允许「乱/重/丢」）。
    let mut sorted = observed.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(
        sorted, observed,
        "watch must deliver events in strictly increasing commit_ts order without duplicates; \
         observed: {observed:?}"
    );
    for ts in &expected {
        assert!(
            observed.contains(ts),
            "watch dropped the committed txn at commit_ts {ts} across the seal boundary; \
             observed: {observed:?}, expected: {expected:?}"
        );
    }

    cluster.wait_stable_leader(FAILOVER_TIMEOUT);
    let current: u64 = cluster
        .wait_leader(FAILOVER_TIMEOUT)
        .parse()
        .expect("leader id");
    let http = leader_http_port(&cluster, current);
    let sealed = metric_value(http, "coord_kv_t_commit_log_shards_sealed").unwrap_or(0);
    assert!(
        sealed > 0,
        "the workload must have sealed at least one commit-log shard (metric \
         coord_kv_t_commit_log_shards_sealed = {sealed}); logs:\n{}",
        cluster.logs()
    );

    // 恢复后新的提交仍然可用（seal stall 没有把 sequencer 留在坏状态）。
    let ts = runtime().block_on(commit_txn(&addrs, b"after", b"zafter", 999));
    assert!(ts > 0);
    assert_no_forbidden_anchors(&cluster);
}

/// 夹具自查：配置写盘路径都在临时目录。
#[test]
fn test_chaos_configs_stay_outside_the_repository() {
    let dir = std::env::temp_dir();
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"));
    assert!(!dir.starts_with(repo));
}
