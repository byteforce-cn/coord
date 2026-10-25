//! KV-T v2 三进程集群夹具（chaos / soak 共用）。
//!
//! 真实 `coord server` 进程 + 独立数据目录/端口；每个用例自己起集群、结束时
//! kill。夹具只做**装配与观测**：故障注入、断言留在各自用例里。
//!
//! 两条硬约束（都会把「假红/假绿」变成可读失败）：
//! - 端口/目录必须 fail-closed：端口冲突时 `spawn` 失败而不是复用别人的进程；
//!   目录随 `TempDir` 清理，失败现场可用 `KVT_CHAOS_ARTIFACT_DIR` 保留。
//! - 等待 leader 期间节点退出必须立刻失败（`assert_all_alive`），否则一次慢启动
//!   会被误报成「60s 没选出 leader」。

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use super::find_port;

/// 测试集群节点数（3 副本即可在单节点故障下保住 quorum）。
pub const NODES: u64 = 3;
/// 故障后等待新 leader / 可见性的上限。
pub const FAILOVER_TIMEOUT: Duration = Duration::from_secs(60);
/// 单次 RPC 的超时。被 SIGSTOP 的节点仍会接受 TCP 连接但永不回复：没有超时的
/// 调用会把一次重试拖成整轮测试超时，故障窗口就变成了「卡住」而不是「重试」。
pub const RPC_TIMEOUT: Duration = Duration::from_secs(2);

/// 带超时的 RPC：超时与传输错误都折成 `Err(String)`，调用方按可重试处理。
pub async fn rpc<T, F>(fut: F) -> Result<T, String>
where
    F: std::future::Future<Output = Result<T, tonic::Status>>,
{
    match tokio::time::timeout(RPC_TIMEOUT, fut).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(status)) => Err(status.to_string()),
        Err(_) => Err(format!("rpc timed out after {RPC_TIMEOUT:?}")),
    }
}

pub struct V2Node {
    pub id: u64,
    pub grpc_port: u16,
    pub raft_port: u16,
    /// HTTP（health/BFF/`/metrics`）端口。
    pub http_port: u16,
    pub data_dir: PathBuf,
    pub config_path: PathBuf,
    pub log_path: PathBuf,
    pub child: Option<Child>,
    pub initial_nodes: Vec<(u64, String, String)>,
    pub bootstrapped: bool,
    partition_file: Option<PathBuf>,
    pd_scheduler_file: Option<PathBuf>,
    kvt_extra: String,
    pd_extra: String,
    toml_extra: String,
}

/// 集群装配选项：默认值与历史夹具逐字一致（不可分区、无额外配置）。
#[derive(Clone, Copy, Default)]
pub struct V2ClusterOptions {
    /// 允许用 [`V2Cluster::partition`] 注入对称网络分区。
    ///
    /// 依赖 **debug 构建**的 `COORD_TEST_RAFT_PARTITION_FILE` 钩子（release 二进制
    /// 不含该开关）：分区在 raft RPC 层阻断，进程、TSO、sequencer、shard 表都是
    /// 真实的。非 debug 构建下该选项无效（夹具会因换主等待失败而暴露）。
    pub partitionable: bool,
    /// 追加到 `[kv_t]` 段的键（每行自带换行；空 = 默认值）。
    pub kvt: &'static str,
    /// 追加到配置末尾的其它段（例如 `[raft]` 调参）。
    pub toml: &'static str,
    /// 允许用 [`V2Cluster::resume_scheduler`] / [`V2Cluster::pause_scheduler`]
    /// 运行时切换 PD 调度（debug 构建的 `COORD_TEST_PD_SCHEDULER_FILE` 钩子）。
    pub pd_control: bool,
    /// 追加到 `[multi_raft.pd]` 段的键（例如 split/merge 阈值）。
    pub pd: &'static str,
}

impl V2Node {
    pub fn addr(&self) -> String {
        format!("127.0.0.1:{}", self.grpc_port)
    }

    pub fn config_toml(&self, bootstrap: bool) -> String {
        let nodes: String = self
            .initial_nodes
            .iter()
            .map(|(id, grpc, raft)| {
                format!(
                    "[[cluster.initial_nodes]]\nid = {id}\ngrpc = \"{grpc}\"\nraft = \"{raft}\"\n"
                )
            })
            .collect();
        let grpc = self.addr();
        let raft = format!("127.0.0.1:{}", self.raft_port);
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
             # 只注入决策面故障：split/merge 的 merge watermark 会让旧 revision 按\
             # 设计变成 HISTORY_UNAVAILABLE（属 split/merge 连续性套件），混进来\
             # 会被误判成数据丢失。\n\
             scheduler_paused = true\n{pd}\
             [kv_t]\nenabled = true\n{kvt}\
{toml}",
            id = self.id,
            http = self.http_port,
            // R-SEC-06：多节点集群需共享根密钥（测试用固定值）。
            root_key = "ab".repeat(32),
            bootstrap = bootstrap,
            kvt = self.kvt_extra,
            pd = self.pd_extra,
            toml = self.toml_extra,
        )
    }

    pub fn start(&mut self, bootstrap: bool) {
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
            // 失败现场排查可临时放大日志：KVT_NODE_LOG="coord=info,openraft=debug"。
            .env(
                "RUST_LOG",
                std::env::var("KVT_NODE_LOG").unwrap_or_else(|_| "coord=info".to_string()),
            )
            .env("RUST_BACKTRACE", "1")
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(log_err));
        if let Some(path) = &self.partition_file {
            // debug 构建的 raft-RPC 分区钩子（release 二进制忽略该变量）。
            cmd.env("COORD_TEST_RAFT_PARTITION_FILE", path);
        }
        if let Some(path) = &self.pd_scheduler_file {
            // debug 构建的 PD 调度暂停开关（release 二进制忽略该变量）。
            cmd.env("COORD_TEST_PD_SCHEDULER_FILE", path);
        }
        if bootstrap {
            cmd.arg("--bootstrap");
        }
        self.child = Some(cmd.spawn().expect("spawn coord server"));
    }

    pub fn kill9(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    pub fn signal(&mut self, signal: &str) {
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

    pub fn logs(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
    }
}

impl Drop for V2Node {
    fn drop(&mut self) {
        self.kill9();
    }
}

pub struct V2Cluster {
    pub nodes: Vec<V2Node>,
    root: PathBuf,
    _dir: Option<tempfile::TempDir>,
}

impl V2Cluster {
    pub fn start() -> Self {
        Self::start_with(V2ClusterOptions::default())
    }

    pub fn start_with(options: V2ClusterOptions) -> Self {
        // 失败现场必须能留证据：默认临时目录随 TempDir drop 清空（panic 时也一样），
        // 排查「偶发不可读」需要节点完整日志/数据目录时显式指定保留目录。
        let (root, dir) = match std::env::var("KVT_CHAOS_ARTIFACT_DIR") {
            Ok(path) if !path.is_empty() => {
                let root = PathBuf::from(path);
                std::fs::create_dir_all(&root).unwrap();
                (root, None)
            }
            _ => {
                let dir = tempfile::tempdir().unwrap();
                let root = dir.path().to_path_buf();
                (root, Some(dir))
            }
        };
        let mut ports = Vec::new();
        for _ in 0..NODES {
            ports.push((find_port(), find_port(), find_port()));
        }
        let initial_nodes: Vec<(u64, String, String)> = ports
            .iter()
            .enumerate()
            .map(|(i, (grpc, raft, _http))| {
                (
                    i as u64 + 1,
                    format!("127.0.0.1:{grpc}"),
                    format!("127.0.0.1:{raft}"),
                )
            })
            .collect();

        let partition_file = options
            .partitionable
            .then(|| root.join("raft-partition.txt"));
        let pd_scheduler_file = options.pd_control.then(|| root.join("pd-scheduler.txt"));
        let mut nodes = Vec::new();
        for (i, (grpc, raft, http)) in ports.iter().enumerate() {
            let id = i as u64 + 1;
            nodes.push(V2Node {
                id,
                grpc_port: *grpc,
                raft_port: *raft,
                http_port: *http,
                data_dir: root.join(format!("node-{id}")),
                config_path: root.join(format!("node-{id}.toml")),
                log_path: root.join(format!("node-{id}.log")),
                child: None,
                initial_nodes: initial_nodes.clone(),
                bootstrapped: false,
                partition_file: partition_file.clone(),
                pd_scheduler_file: pd_scheduler_file.clone(),
                kvt_extra: options.kvt.to_string(),
                pd_extra: options.pd.to_string(),
                toml_extra: options.toml.to_string(),
            });
        }
        for node in nodes.iter_mut() {
            node.start(node.id == 1);
        }

        let cluster = Self {
            nodes,
            root,
            _dir: dir,
        };
        cluster.wait_leader(FAILOVER_TIMEOUT);
        cluster
    }

    pub fn addrs(&self) -> Vec<String> {
        self.nodes.iter().map(|n| n.addr()).collect()
    }

    /// 对称隔离一个节点：被隔离节点阻断全部 peer，其余节点阻断它。
    ///
    /// 策略经 `raft-partition.txt` 交给 debug 构建的钩子；文件每行
    /// `<node_id>:<blocked_id,...>`。写入后 ~50ms 生效。
    pub fn partition(&self, node_id: u64) {
        let mut policy = String::new();
        for node in &self.nodes {
            let peers: Vec<String> = if node.id == node_id {
                self.nodes
                    .iter()
                    .filter(|n| n.id != node_id)
                    .map(|n| n.id.to_string())
                    .collect()
            } else {
                vec![node_id.to_string()]
            };
            policy.push_str(&format!("{}:{}\n", node.id, peers.join(",")));
        }
        std::fs::write(self.partition_path(), policy).expect("write raft partition policy");
    }

    /// 解除全部分区；raft 重新握手后集群自动收敛。
    pub fn heal(&self, _node_id: u64) {
        let mut policy = String::new();
        for node in &self.nodes {
            policy.push_str(&format!("{}:\n", node.id));
        }
        std::fs::write(self.partition_path(), policy).expect("write raft partition policy");
    }

    fn partition_path(&self) -> PathBuf {
        self.root.join("raft-partition.txt")
    }

    /// 运行时解冻 PD 调度（依赖 debug 构建的 `COORD_TEST_PD_SCHEDULER_FILE` 钩子）。
    pub fn resume_scheduler(&self) {
        std::fs::write(self.root.join("pd-scheduler.txt"), "running\n")
            .expect("write PD scheduler control file");
    }

    /// 运行时冻结 PD 调度。
    pub fn pause_scheduler(&self) {
        std::fs::write(self.root.join("pd-scheduler.txt"), "paused\n")
            .expect("write PD scheduler control file");
    }

    pub fn logs(&self) -> String {
        let mut out = format!("artifact dir: {}\n", self.root.display());
        out.push_str(
            &self
                .nodes
                .iter()
                .map(|n| format!("--- node {} ---\n{}", n.id, tail(&n.logs(), 5000)))
                .collect::<Vec<_>>()
                .join("\n"),
        );
        out
    }

    pub fn wait_leader(&self, timeout: Duration) -> String {
        self.wait_leader_not(None, timeout)
    }

    /// 启动/故障等待期间任何节点退出都立刻失败：否则一次慢启动会被误报成
    /// 「集群 60s 内没选出 leader」，掩盖真实原因（进程已退出/端口被占）。
    pub fn assert_all_alive(&self) {
        for node in &self.nodes {
            let Some(child) = node.child.as_ref() else {
                continue;
            };
            // `try_wait` 需要可变引用；用 `/proc` 判活，避免把夹具改成 Mutex。
            let pid = child.id();
            let alive = Path::new(&format!("/proc/{pid}")).exists();
            assert!(
                alive,
                "node {} exited during the scenario (pid {pid}); logs:\n{}",
                node.id,
                tail(&node.logs(), 60)
            );
        }
    }

    /// 等所有存活节点都报告同一个 leader（连续 3 次采样一致）。
    pub fn wait_stable_leader(&self, timeout: Duration) {
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
            self.assert_all_alive();
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    /// 等一个**不同于 `exclude`** 的 leader。
    ///
    /// 故障刚注入时，存活节点可能还缓存着旧 leader（选举超时最长 ~1.5s），
    /// 只等「非空 leader」会立刻拿到旧值、把故障窗口判成「没发生」。
    pub fn wait_leader_not(&self, exclude: Option<&str>, timeout: Duration) -> String {
        let deadline = Instant::now() + timeout;
        loop {
            let live: Vec<&V2Node> = self.nodes.iter().filter(|n| n.child.is_some()).collect();
            if let Some(leader) = runtime().block_on(region0_leader_not(&live, exclude)) {
                return leader;
            }
            assert!(
                Instant::now() < deadline,
                "no new region-0 leader within {timeout:?} (excluded {exclude:?}); logs:\n{}",
                self.logs()
            );
            self.assert_all_alive();
            std::thread::sleep(Duration::from_millis(200));
        }
    }
}

pub fn tail(text: &str, lines: usize) -> String {
    let all: Vec<&str> = text.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

/// 夹具共用 runtime（tonic Channel 的驱动挂在创建它的 runtime 上）。
pub static RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();

pub fn runtime() -> &'static tokio::runtime::Runtime {
    RT.get_or_init(|| tokio::runtime::Runtime::new().expect("build fixture runtime"))
}

/// 返回 region 0 leader 的节点 id（`Maintenance.Status.raft_leader`）。
pub async fn region0_leader(nodes: &[&V2Node]) -> Option<String> {
    region0_leader_not(nodes, None).await
}

/// 同 [`region0_leader`]，但跳过报告 `exclude` 的节点。
///
/// 分区场景下被隔离的旧 leader 仍会（在本地）报告自己是 leader；不跳过它就会
/// 一直返回旧值，把「多数派已换主」误报成「没有新 leader」。
async fn region0_leader_not(nodes: &[&V2Node], exclude: Option<&str>) -> Option<String> {
    for node in nodes {
        if let Some(status) = maintenance_status(&node.addr()).await {
            if !status.raft_leader.is_empty() && Some(status.raft_leader.as_str()) != exclude {
                return Some(status.raft_leader);
            }
        }
    }
    None
}

pub async fn maintenance_status(addr: &str) -> Option<coord_proto::maintenance::StatusResponse> {
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
pub async fn allocate_ts(addrs: &[String]) -> u64 {
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
pub async fn commit_txn(addrs: &[String], left: &[u8], right: &[u8], tag: u64) -> i64 {
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
pub async fn direct_put(addrs: &[String], key: &[u8], value: &[u8]) -> i64 {
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

/// 一次读尝试的结果。
///
/// 「读不到」有两种完全不同的原因，混在一起会把 apply 落后误判成数据丢失：
/// - `Rejected`：副本还没 apply 到 `read_ts`（`SAFE_TS_LAG`）或传输失败，可重试；
/// - `Empty`：副本已 apply 到 `read_ts` 却没有这一行 —— 违反「已提交即可见」。
pub enum ReadAttempt {
    Found(Vec<u8>, i64),
    Empty(String),
    Rejected(String),
}

/// 以 `read_ts` 读一个 key；`read_ts=0` 走 safe_ts。
pub async fn read_at(addrs: &[String], key: &[u8], read_ts: i64) -> ReadAttempt {
    use coord_proto::kv::v2::kv_client::KvClient;

    let mut last = String::from("no reachable node");
    for addr in addrs {
        let Ok(ch) = tonic::transport::Channel::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect_timeout(Duration::from_secs(2))
            .connect()
            .await
        else {
            last = format!("{addr}: connect failed");
            continue;
        };
        let mut kv = KvClient::new(ch);
        match rpc(kv.range(coord_proto::kv::v2::RangeRequest {
            key: key.to_vec(),
            range_end: Vec::new(),
            limit: 0,
            read_ts,
            keys_only: false,
            count_only: false,
        }))
        .await
        {
            Ok(resp) => {
                let resp = resp.into_inner();
                match resp.kvs.first() {
                    Some(kv) => return ReadAttempt::Found(kv.value.clone(), kv.mod_revision),
                    None => {
                        // 服务端在该副本 applied revision >= read_ts 时才可能返回空集。
                        return ReadAttempt::Empty(format!(
                            "{addr}: empty result, server read_ts {}",
                            resp.read_ts
                        ));
                    }
                }
            }
            Err(e) => last = format!("{addr}: {e}"),
        }
    }
    ReadAttempt::Rejected(last)
}

/// 各存活节点的 region 0 状态（失败现场必须能区分「apply 落后」与「数据不见」）。
pub fn node_statuses(addrs: &[String]) -> String {
    runtime().block_on(async {
        let mut out = Vec::new();
        for addr in addrs {
            match maintenance_status(addr).await {
                Some(status) => out.push(format!(
                    "{addr}: revision={} raft_index={} leader={:?}",
                    status.revision, status.raft_index, status.raft_leader
                )),
                None => out.push(format!("{addr}: unreachable")),
            }
        }
        out.join("; ")
    })
}

/// 不变量违反的日志锚点（逐字，与 runbook/测试断言联动）。
pub const FORBIDDEN_LOG_ANCHORS: &[&str] = &[
    "passed unapplied decision",
    "commit decision apply stopped before the end of the batch",
    "commit decision is behind the applied revision without a changelog record",
    "decision apply stopped at",
];

/// 断言整个集群的节点日志里没有内部不变量违反。
pub fn assert_no_forbidden_anchors(cluster: &V2Cluster) {
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

/// 读本机节点 `/metrics` 的一个 counter/gauge（简单 HTTP GET，避免引入客户端依赖）。
pub fn metric_value(http_port: u16, name: &str) -> Option<u64> {
    let mut stream = TcpStream::connect(("127.0.0.1", http_port)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    write!(
        stream,
        "GET /metrics HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
    )
    .ok()?;
    let mut out = String::new();
    stream.read_to_string(&mut out).ok()?;
    out.lines()
        .find_map(|line| line.strip_prefix(name)?.strip_prefix(' '))
        .and_then(|value| value.trim().parse::<u64>().ok())
}
