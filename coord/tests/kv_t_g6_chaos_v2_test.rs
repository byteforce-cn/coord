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
    assert_no_forbidden_anchors, commit_txn, direct_put, maintenance_status, node_statuses,
    read_at, runtime, ReadAttempt, V2Cluster, FAILOVER_TIMEOUT,
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

/// 夹具自查：配置写盘路径都在临时目录。
#[test]
fn test_chaos_configs_stay_outside_the_repository() {
    let dir = std::env::temp_dir();
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"));
    assert!(!dir.starts_with(repo));
}
