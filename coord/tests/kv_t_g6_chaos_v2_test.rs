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
//! 由 CI 的真实故障注入 job 触发（`#[ignore]`）：
//!   `cargo test -p coord --test kv_t_g6_chaos_v2_test -- --ignored --nocapture --test-threads=1`

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[path = "common/mod.rs"]
mod common;

use common::find_port;

const NODES: u64 = 3;
const FAULT_ROUNDS: u64 = 2;
const TXN_COUNT: usize = 12;
const DIRECT_COUNT: usize = 12;
/// 故障后等待新 leader / 可见性的上限。
const FAILOVER_TIMEOUT: Duration = Duration::from_secs(60);
/// 单次 RPC 的超时。被 SIGSTOP 的节点仍会接受 TCP 连接但永不回复：没有超时的
/// 调用会把一次重试拖成整轮测试超时，故障窗口就变成了「卡住」而不是「重试」。
const RPC_TIMEOUT: Duration = Duration::from_secs(2);

/// 带超时的 RPC：超时与传输错误都折成 `Err(String)`，调用方按可重试处理。
async fn rpc<T, F>(fut: F) -> Result<T, String>
where
    F: std::future::Future<Output = Result<T, tonic::Status>>,
{
    match tokio::time::timeout(RPC_TIMEOUT, fut).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(status)) => Err(status.to_string()),
        Err(_) => Err(format!("rpc timed out after {RPC_TIMEOUT:?}")),
    }
}

struct V2Node {
    id: u64,
    grpc_port: u16,
    raft_port: u16,
    data_dir: PathBuf,
    config_path: PathBuf,
    log_path: PathBuf,
    child: Option<Child>,
    initial_nodes: Vec<(u64, String, String)>,
    bootstrapped: bool,
}

impl V2Node {
    fn addr(&self) -> String {
        format!("127.0.0.1:{}", self.grpc_port)
    }

    fn config_toml(&self, bootstrap: bool) -> String {
        let nodes: String = self
            .initial_nodes
            .iter()
            .map(|(id, grpc, raft)| {
                format!(
                    "[[cluster.initial_nodes]]\nid = {id}\ngrpc = \"{grpc}\"\nraft = \"{raft}\"\n"
                )
            })
            .collect();
        format!(
            "[node]\nid = {id}\n\
             [network]\ngrpc_addr = \"{grpc}\"\nraft_addr = \"{raft}\"\n\
             http_addr = \"127.0.0.1:{http}\"\n\
             [security]\nauth_enabled = false\nauth_root_key = \"{root_key}\"\n\
             [cluster]\ncluster_name = \"kvt-g6-chaos-v2\"\nbootstrap = {bootstrap}\n{nodes}\
             [multi_raft]\nenabled = true\n\
             [[multi_raft.initial_regions]]\nid = 1\nstart_key = \"\"\nend_key = \"m\"\n\
             [[multi_raft.initial_regions]]\nid = 2\nstart_key = \"m\"\nend_key = \"\"\n\
             [multi_raft.pd]\nenabled = true\ntarget_replicas = {NODES}\n\
             [kv_t]\nenabled = true\n",
            id = self.id,
            grpc = format!("127.0.0.1:{}", self.grpc_port),
            raft = format!("127.0.0.1:{}", self.raft_port),
            http = find_port(),
            // R-SEC-06：多节点集群需共享根密钥（测试用固定值）。
            root_key = "ab".repeat(32),
            bootstrap = bootstrap,
        )
    }

    fn start(&mut self, bootstrap: bool) {
        std::fs::create_dir_all(&self.data_dir).unwrap();
        std::fs::write(&self.config_path, self.config_toml(bootstrap)).unwrap();
        self.bootstrapped |= bootstrap;

        let log = std::fs::File::create(&self.log_path).unwrap();
        let log_err = log.try_clone().unwrap();
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_coord"));
        cmd.arg("server")
            .arg("--id")
            .arg(self.id.to_string())
            .arg("--addr")
            .arg(self.addr())
            .arg("--raft-addr")
            .arg(format!("127.0.0.1:{}", self.raft_port))
            .arg("--data-dir")
            .arg(&self.data_dir)
            .arg("--config")
            .arg(&self.config_path)
            .env("RUST_LOG", "coord=info")
            .env("RUST_BACKTRACE", "1")
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(log_err));
        if bootstrap {
            cmd.arg("--bootstrap");
        }
        self.child = Some(cmd.spawn().expect("spawn coord server"));
    }

    fn kill9(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    fn signal(&mut self, signal: &str) {
        if let Some(child) = self.child.as_ref() {
            let ok = Command::new("kill")
                .arg(format!("-{signal}"))
                .arg(child.id().to_string())
                .status()
                .map(|s| s.success())
                .unwrap_or(false);
            assert!(ok, "failed to send SIG{signal} to node {}", self.id);
        }
    }

    fn logs(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
    }
}

impl Drop for V2Node {
    fn drop(&mut self) {
        self.kill9();
    }
}

struct Cluster {
    nodes: Vec<V2Node>,
    _dir: tempfile::TempDir,
}

impl Cluster {
    fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut ports = Vec::new();
        for _ in 0..NODES {
            ports.push((find_port(), find_port()));
        }
        let initial_nodes: Vec<(u64, String, String)> = ports
            .iter()
            .enumerate()
            .map(|(i, (grpc, raft))| {
                (
                    i as u64 + 1,
                    format!("127.0.0.1:{grpc}"),
                    format!("127.0.0.1:{raft}"),
                )
            })
            .collect();

        let mut nodes = Vec::new();
        for (i, (grpc, raft)) in ports.iter().enumerate() {
            let id = i as u64 + 1;
            nodes.push(V2Node {
                id,
                grpc_port: *grpc,
                raft_port: *raft,
                data_dir: dir.path().join(format!("node-{id}")),
                config_path: dir.path().join(format!("node-{id}.toml")),
                log_path: dir.path().join(format!("node-{id}.log")),
                child: None,
                initial_nodes: initial_nodes.clone(),
                bootstrapped: false,
            });
        }
        for node in nodes.iter_mut() {
            node.start(node.id == 1);
        }

        let cluster = Self { nodes, _dir: dir };
        cluster.wait_leader(FAILOVER_TIMEOUT);
        cluster
    }

    fn addrs(&self) -> Vec<String> {
        self.nodes.iter().map(|n| n.addr()).collect()
    }

    fn logs(&self) -> String {
        self.nodes
            .iter()
            .map(|n| format!("--- node {} ---\n{}", n.id, tail(&n.logs(), 5000)))
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn wait_leader(&self, timeout: Duration) -> String {
        self.wait_leader_not(None, timeout)
    }

    /// 等所有存活节点都报告同一个 leader（连续 3 次采样一致）。
    fn wait_stable_leader(&self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        let mut last = String::new();
        let mut same = 0;
        loop {
            let live: Vec<&V2Node> = self.nodes.iter().filter(|n| n.child.is_some()).collect();
            let leaders = runtime().block_on(async {
                let mut out = Vec::new();
                for node in &live {
                    out.push(
                        maintenance_status(&node.addr())
                            .await
                            .map(|s| s.raft_leader),
                    );
                }
                out
            });
            let all_agree = leaders.first().map(|l| l.is_some()).unwrap_or(false)
                && leaders.iter().all(|l| *l == leaders[0]);
            if let (true, Some(Some(leader))) = (all_agree, leaders.first()) {
                if *leader == last {
                    same += 1;
                } else {
                    last = leader.clone();
                    same = 1;
                }
                if same >= 3 {
                    return;
                }
            } else {
                same = 0;
            }
            assert!(
                Instant::now() < deadline,
                "cluster did not converge to a stable region-0 leader within {timeout:?}; logs:\n{}",
                self.logs()
            );
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    /// 等一个**不同于 `exclude`** 的 leader。
    ///
    /// 故障刚注入时，存活节点可能还缓存着旧 leader（选举超时最长 ~1.5s），
    /// 只等「非空 leader」会立刻拿到旧值、把故障窗口判成「没发生」。
    fn wait_leader_not(&self, exclude: Option<&str>, timeout: Duration) -> String {
        let deadline = Instant::now() + timeout;
        loop {
            let live: Vec<&V2Node> = self.nodes.iter().filter(|n| n.child.is_some()).collect();
            if let Some(leader) = runtime().block_on(region0_leader(&live)) {
                if exclude != Some(leader.as_str()) {
                    return leader;
                }
            }
            assert!(
                Instant::now() < deadline,
                "no new region-0 leader within {timeout:?} (excluded {exclude:?}); logs:\n{}",
                self.logs()
            );
            std::thread::sleep(Duration::from_millis(200));
        }
    }
}

fn tail(text: &str, lines: usize) -> String {
    let all: Vec<&str> = text.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

/// 夹具共用 runtime（tonic Channel 的驱动挂在创建它的 runtime 上）。
static RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();

fn runtime() -> &'static tokio::runtime::Runtime {
    RT.get_or_init(|| tokio::runtime::Runtime::new().expect("build fixture runtime"))
}

/// 返回 region 0 leader 的节点 id（`Maintenance.Status.raft_leader`）。
async fn region0_leader(nodes: &[&V2Node]) -> Option<String> {
    for node in nodes {
        if let Some(status) = maintenance_status(&node.addr()).await {
            if !status.raft_leader.is_empty() {
                return Some(status.raft_leader);
            }
        }
    }
    None
}

async fn maintenance_status(addr: &str) -> Option<coord_proto::maintenance::StatusResponse> {
    let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .ok()?
        .connect_timeout(Duration::from_secs(1))
        .connect()
        .await
        .ok()?;
    let mut client = coord_proto::maintenance::maintenance_client::MaintenanceClient::new(channel);
    // 被 SIGSTOP 的进程仍会接受 TCP 连接但永不回复：没有超时的 status 调用会把
    // 故障检测拖到测试超时。
    tokio::time::timeout(
        Duration::from_secs(1),
        client.status(coord_proto::maintenance::StatusRequest {}),
    )
    .await
    .ok()?
    .ok()
    .map(|r| r.into_inner())
}

/// 取一个 `commit_ts`：TSO 只在 region 0 leader 上可用，逐节点重试。
async fn allocate_ts(addrs: &[String]) -> u64 {
    let deadline = Instant::now() + FAILOVER_TIMEOUT;
    loop {
        for addr in addrs {
            if let Ok(ch) = tonic::transport::Channel::from_shared(format!("http://{addr}"))
                .unwrap()
                .connect_timeout(Duration::from_secs(2))
                .connect()
                .await
            {
                let mut tso = coord_proto::tso::v2::tso_client::TsoClient::new(ch);
                if let Ok(resp) = rpc(tso.get_ts(coord_proto::tso::v2::GetTsRequest {})).await {
                    return resp.into_inner().ts as u64;
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "TSO never became available after the fault"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// 提交一个跨 Region 事务（写 region 1 与 region 2 各一键）。
///
/// Begin 的会话只在协调节点有效，所以 Begin/Commit 必须打到**同一**节点；逐节点
/// 重试直到某节点接受。`TXN_CONFLICT` 换新会话重跑。
async fn commit_txn(addrs: &[String], left: &[u8], right: &[u8], tag: u64) -> i64 {
    use coord_proto::txn::v2::txn_client::TxnClient;
    use coord_proto::txn::v2::{BeginRequest, CommitRequest, Mutation, ReadRequest};

    let deadline = Instant::now() + FAILOVER_TIMEOUT;
    let mut last_error = String::new();
    loop {
        for addr in addrs {
            let Ok(ch) = tonic::transport::Channel::from_shared(format!("http://{addr}"))
                .unwrap()
                .connect_timeout(Duration::from_secs(2))
                .connect()
                .await
            else {
                continue;
            };
            let mut txn = TxnClient::new(ch);
            let begin = match txn
                .begin(BeginRequest {
                    start_ts: 0,
                    ttl_ms: 0,
                })
                .await
            {
                Ok(resp) => resp.into_inner(),
                Err(e) => {
                    last_error = format!("Begin on {addr}: {e}");
                    continue;
                }
            };
            // 读集：两个 key 的 point read，让 sequencer 校验冲突。
            let mut read_failed = None;
            for key in [left, right] {
                if let Err(e) = rpc(txn.read(ReadRequest {
                    txn_id: begin.txn_id.clone(),
                    key: key.to_vec(),
                    range_end: Vec::new(),
                    limit: 0,
                    keys_only: false,
                }))
                .await
                {
                    read_failed = Some(e);
                    break;
                }
            }
            if let Some(e) = read_failed {
                last_error = format!("Read on {addr}: {e}");
                let _ = rpc(txn.abort(coord_proto::txn::v2::AbortRequest {
                    txn_id: begin.txn_id.clone(),
                }))
                .await;
                continue;
            }
            let mutations = vec![
                Mutation {
                    op: coord_proto::txn::v2::mutation::Op::Put as i32,
                    key: left.to_vec(),
                    range_end: Vec::new(),
                    value: format!("txn-{tag}").into_bytes(),
                    lease_id: 0,
                },
                Mutation {
                    op: coord_proto::txn::v2::mutation::Op::Put as i32,
                    key: right.to_vec(),
                    range_end: Vec::new(),
                    value: format!("txn-{tag}").into_bytes(),
                    lease_id: 0,
                },
            ];
            match rpc(txn.commit(CommitRequest {
                txn_id: begin.txn_id.clone(),
                mutations,
                request_id: Vec::new(),
            }))
            .await
            {
                Ok(resp) => return resp.into_inner().commit_ts,
                Err(e) => {
                    // 冲突不是故障：换新 start_ts 重跑（D12.2）。超时/传输错误也
                    // 换节点重试（会话只在协调节点有效，不能复用）。
                    last_error = format!("Commit on {addr}: {e}");
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "txn never committed after the fault; last error: {last_error}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// 单键直写：TSO 取 `commit_ts` 后写 region 1 的 key（`< "m"`）。
async fn direct_put(addrs: &[String], key: &[u8], value: &[u8]) -> i64 {
    use coord_proto::kv::v2::kv_client::KvClient;

    let deadline = Instant::now() + FAILOVER_TIMEOUT;
    let mut last_error = String::new();
    loop {
        let ts = allocate_ts(addrs).await;
        for addr in addrs {
            let Ok(ch) = tonic::transport::Channel::from_shared(format!("http://{addr}"))
                .unwrap()
                .connect_timeout(Duration::from_secs(2))
                .connect()
                .await
            else {
                continue;
            };
            let mut kv = KvClient::new(ch);
            match rpc(kv.put(coord_proto::kv::v2::PutRequest {
                key: key.to_vec(),
                value: value.to_vec(),
                lease_id: 0,
                prev_kv: false,
                request_id: Vec::new(),
                commit_ts: ts as i64,
            }))
            .await
            {
                Ok(resp) => return resp.into_inner().commit_ts,
                // `CommitRejected`（取到的批次落后于已 apply 的 revision）也是
                // 可重试：下一轮换新 `commit_ts`。
                Err(e) => last_error = format!("Put on {addr}: {e}"),
            }
        }
        assert!(
            Instant::now() < deadline,
            "direct write never committed after the fault; last error: {last_error}"
        );
        // `CommitRejected`（并发写先 apply 了更大的 commit_ts）是正常的可重试
        // 结果：立刻换新的 `commit_ts` 重试，不睡——旧批次/并发跳号都应该在
        // 毫秒级收敛。
        tokio::task::yield_now().await;
    }
}

/// 以 `read_ts` 读一个 key；`read_ts=0` 走 safe_ts（可能 `SAFE_TS_LAG`，属于
/// 可重试，不算失败）。
async fn read_at(addrs: &[String], key: &[u8], read_ts: i64) -> Option<(Vec<u8>, i64)> {
    use coord_proto::kv::v2::kv_client::KvClient;

    for addr in addrs {
        let Ok(ch) = tonic::transport::Channel::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect_timeout(Duration::from_secs(2))
            .connect()
            .await
        else {
            continue;
        };
        let mut kv = KvClient::new(ch);
        if let Ok(resp) = rpc(kv.range(coord_proto::kv::v2::RangeRequest {
            key: key.to_vec(),
            range_end: Vec::new(),
            limit: 0,
            read_ts,
            keys_only: false,
            count_only: false,
        }))
        .await
        {
            let resp = resp.into_inner();
            if let Some(kv) = resp.kvs.first() {
                return Some((kv.value.clone(), kv.mod_revision));
            }
        }
    }
    None
}

/// 不变量违反的日志锚点（逐字，与 runbook/测试断言联动）。
const FORBIDDEN_LOG_ANCHORS: &[&str] = &[
    "passed unapplied decision",
    "commit decision apply stopped before the end of the batch",
    "decision apply stopped at",
];

#[test]
#[ignore = "real-process fault injection; run by the CI chaos job"]
fn chaos_v2_mixed_writes_survive_decision_leader_loss() {
    let mut cluster = Cluster::start();
    let addrs = cluster.addrs();

    // 基线：故障前先证明 v2 事务可提交。
    let baseline = runtime().block_on(commit_txn(&addrs, b"aa", b"zz", 0));
    assert!(baseline > 0);

    let committed: Arc<Mutex<BTreeMap<Vec<u8>, (Vec<u8>, i64)>>> =
        Arc::new(Mutex::new(BTreeMap::new()));
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
            let round = round;
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
                            guard.insert(left, (format!("txn-{tag}").into_bytes(), ts));
                            guard.insert(right, (format!("txn-{tag}").into_bytes(), ts));
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
                            guard.insert(key, (value, ts));
                        }));
                    }
                    for handle in handles {
                        handle.await.expect("workload task");
                    }
                });
                let _ = round;
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
        for (key, (value, ts)) in &snapshot {
            let deadline = Instant::now() + FAILOVER_TIMEOUT;
            loop {
                if let Some((got, mod_rev)) = runtime().block_on(read_at(&addrs, key, *ts)) {
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
                assert!(
                    Instant::now() < deadline,
                    "round {round}: committed key {:?} (commit_ts {ts}) never became readable; \
                     node logs:\n{}",
                    String::from_utf8_lossy(key),
                    cluster.logs()
                );
                std::thread::sleep(Duration::from_millis(100));
            }
        }

        // 2) safe_ts 必须推进：`read_ts=0` 能读到本轮最后一次直写（不得永久 SAFE_TS_LAG）。
        let latest = snapshot
            .iter()
            .filter(|(key, _)| key.starts_with(b"d-"))
            .max_by_key(|(_, (_, ts))| *ts)
            .map(|(key, (value, _))| (key.clone(), value.clone()));
        if let Some((key, value)) = latest {
            let deadline = Instant::now() + FAILOVER_TIMEOUT;
            loop {
                if let Some((got, _)) = runtime().block_on(read_at(&addrs, &key, 0)) {
                    assert_eq!(
                        got,
                        value,
                        "round {round}: safe_ts read returned a stale value for {:?}",
                        String::from_utf8_lossy(&key)
                    );
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "round {round}: safe_ts never caught up with the latest direct write \
                     (permanent SAFE_TS_LAG)"
                );
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }

    // 3) 内部不变量：直写不得越过 pending decision，decision apply 不得停在批中。
    for node in &cluster.nodes {
        let logs = node.logs();
        for anchor in FORBIDDEN_LOG_ANCHORS {
            assert!(
                !logs.contains(anchor),
                "node {} logged the invariant violation {:?}:\n{}",
                node.id,
                anchor,
                tail(&logs, 40)
            );
        }
    }
}

/// 夹具自查：配置写盘路径都在临时目录。
#[test]
fn test_chaos_configs_stay_outside_the_repository() {
    let dir = std::env::temp_dir();
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"));
    assert!(!dir.starts_with(repo));
}
