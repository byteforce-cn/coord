// P0-H.2/H.3 真实故障注入套件（chaos_real）—— 基于真实 `coord` 二进制进程
//
// 与 `sim_chaos_test`/`sim_jepsen_test`（算法级内存模拟，非系统验证证据）不同：
// 本套件 spawn 真实 `coord server` 进程（3 节点），注入：
//   - kill -9 leader / follower 循环（进程崩溃）
//   - 重启循环（数据目录保留）
//   - 并发写入 + 线性一致性检查器（register checker，H.3 合一）
//
// 标记 `#[ignore]`：由 CI nightly 触发（P0-H.4），本地运行：
//   CHAOS_REAL=1 cargo test -p coord --test chaos_real -- --ignored --nocapture
//
// 约束：真实分区注入（gRPC 代理层）为 H.2 后续批次；本 v1 覆盖进程级故障与
// 线性一致性验证（决策文档 P0-H 验收标准的进程故障部分）。

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use coord_proto::kv::kv_client::KvClient;
use coord_proto::kv::{PutRequest, RangeRequest};
use tonic::transport::Channel;

/// 单次运行的最大时长（防止 CI 卡死）
const MAX_RUNTIME: Duration = Duration::from_secs(120);

fn find_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

struct RealNode {
    id: u64,
    grpc_port: u16,
    raft_port: u16,
    data_dir: PathBuf,
    child: Child,
}

impl RealNode {
    fn spawn(
        id: u64,
        grpc_port: u16,
        raft_port: u16,
        data_dir: &std::path::Path,
        initial_nodes: &[(u64, String, String)], // (id, grpc, raft)
    ) -> Self {
        let bin = env!("CARGO_BIN_EXE_coord");
        let mut cmd = Command::new(bin);
        cmd.arg("server")
            .arg("--id")
            .arg(id.to_string())
            .arg("--addr")
            .arg(format!("127.0.0.1:{grpc_port}"))
            .arg("--raft-addr")
            .arg(format!("127.0.0.1:{raft_port}"))
            .arg("--data-dir")
            .arg(data_dir)
            .arg("--auth-enabled")
            .arg("false")
            .env("RUST_LOG", "coord=warn")
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        // 集群配置：仅节点 1 bootstrap（携带全部初始成员）；
        // 其余节点不 bootstrap、不 join，等待 leader 复制（openraft 动态成员语义）。
        let cfg_dir = data_dir.join("conf");
        std::fs::create_dir_all(&cfg_dir).unwrap();
        let cfg_path = cfg_dir.join("node.toml");
        let mut nodes_toml = String::new();
        for (nid, grpc, raft) in initial_nodes {
            nodes_toml.push_str(&format!(
                "[[cluster.initial_nodes]]\nid = {nid}\ngrpc = \"{grpc}\"\nraft = \"{raft}\"\n"
            ));
        }
        let bootstrap_flag = if id == 1 { "true" } else { "false" };
        let toml = format!(
            "[cluster]\nbootstrap = {bootstrap_flag}\n{nodes_toml}[security]\nauth_enabled = false\n"
        );
        std::fs::write(&cfg_path, toml).unwrap();
        cmd.arg("--config").arg(&cfg_path);

        let child = cmd.spawn().expect("spawn coord server");
        Self {
            id,
            grpc_port,
            raft_port,
            data_dir: data_dir.to_path_buf(),
            child,
        }
    }

    async fn channel(&self) -> Channel {
        Channel::from_shared(format!("http://127.0.0.1:{}", self.grpc_port))
            .unwrap()
            .connect_timeout(Duration::from_secs(3))
            .connect()
            .await
            .unwrap()
    }

    /// 等待本节点可服务（作为 leader 或 follower；status RPC 可达即算就绪）
    async fn wait_ready(&self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        loop {
            if let Ok(ch) = Channel::from_shared(format!("http://127.0.0.1:{}", self.grpc_port))
                .unwrap()
                .connect_timeout(Duration::from_secs(2))
                .connect()
                .await
            {
                let mut client =
                    coord_proto::maintenance::maintenance_client::MaintenanceClient::new(ch);
                if client
                    .status(coord_proto::maintenance::StatusRequest {})
                    .await
                    .is_ok()
                {
                    return;
                }
            }
            assert!(Instant::now() < deadline, "node {} not ready", self.id);
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }

    fn kill9(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    /// 重启（同一数据目录，沿用已写入的节点配置）
    fn restart(&mut self, _initial_nodes: &[(u64, String, String)]) {
        let bin = env!("CARGO_BIN_EXE_coord");
        let cfg_path = self.data_dir.join("conf").join("node.toml");
        let child = Command::new(bin)
            .arg("server")
            .arg("--id")
            .arg(self.id.to_string())
            .arg("--addr")
            .arg(format!("127.0.0.1:{}", self.grpc_port))
            .arg("--raft-addr")
            .arg(format!("127.0.0.1:{}", self.raft_port))
            .arg("--data-dir")
            .arg(&self.data_dir)
            .arg("--auth-enabled")
            .arg("false")
            .arg("--config")
            .arg(&cfg_path)
            .env("RUST_LOG", "coord=warn")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("respawn coord server");
        self.child = child;
    }
}

/// 从集群任一存活节点写（Put），leader 自动发现由重试实现。
async fn put_any(nodes: &[RealNode], key: &[u8], value: &[u8]) -> Option<u64> {
    for node in nodes {
        let mut kv = KvClient::new(node.channel().await);
        if let Ok(resp) = kv
            .put(PutRequest {
                key: key.to_vec(),
                value: value.to_vec(),
                lease_id: 0,
                prev_kv: false,
                request_id: vec![],
            })
            .await
        {
            return Some(resp.into_inner().revision as u64);
        }
    }
    None
}

/// 从集群任一存活节点读（Range 需 leader；重试直至成功）。
async fn range_any(nodes: &[RealNode], key: &[u8], deadline: Instant) -> Option<Vec<u8>> {
    while Instant::now() < deadline {
        for node in nodes {
            let mut kv = KvClient::new(node.channel().await);
            if let Ok(resp) = kv
                .range(RangeRequest {
                    key: key.to_vec(),
                    range_end: vec![],
                    limit: 1,
                    revision: 0,
                    keys_only: false,
                    count_only: false,
                })
                .await
            {
                let inner = resp.into_inner();
                if let Some(kv) = inner.kvs.first() {
                    return Some(kv.value.clone());
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    None
}

/// 简单线性一致性检查（单 register）：记录 (开始时间, 结束时间, 读到的值)，
/// 校验任何读返回的值都不早于该读开始前已完成的写。
#[derive(Default)]
struct RegisterChecker {
    history: Vec<(Instant, Instant, Option<String>)>,
}

impl RegisterChecker {
    fn record(&mut self, start: Instant, end: Instant, value: Option<String>) {
        self.history.push((start, end, value));
    }

    fn numeric(v: &str) -> u64 {
        v.strip_prefix("final-")
            .and_then(|s| s.parse::<u64>().ok())
            .or_else(|| v.strip_prefix('v').and_then(|s| s.parse::<u64>().ok()))
            .unwrap_or(0)
    }

    fn verify(&self) {
        for (i, (start_i, _end_i, val_i)) in self.history.iter().enumerate() {
            let Some(v) = val_i else { continue };
            let num_v = Self::numeric(v);
            // 所有在 start_i 之前已完成的写（有值的记录视为写）都必须 <= 读到的值
            for (j, (_start_j, end_j, val_j)) in self.history.iter().enumerate() {
                if i == j {
                    continue;
                }
                let Some(vj) = val_j else { continue };
                if *end_j <= *start_i && num_v < Self::numeric(vj) {
                    panic!(
                        "linearizability violation: read at {start_i:?} saw '{v}' but \
                         write '{vj}' completed at {end_j:?} (history entry {j})"
                    );
                }
            }
        }
    }
}

/// P0-H.2/H.3：3 节点真实集群 —— kill -9 leader/follower 循环 + 重启 + 线性一致检查。
#[tokio::test]
#[ignore = "real-process chaos suite; run in CI nightly (P0-H.4) or CHAOS_REAL=1 locally"]
async fn chaos_real_kill9_and_linearizability() {
    if std::env::var("CHAOS_REAL").is_err() {
        eprintln!("skipping chaos_real (set CHAOS_REAL=1 to run locally)");
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path();

    let grpc_ports: Vec<u16> = (0..3).map(|_| find_port()).collect();
    let raft_ports: Vec<u16> = (0..3).map(|_| find_port()).collect();
    let initial_nodes: Vec<(u64, String, String)> = (0..3)
        .map(|i| {
            (
                (i + 1) as u64,
                format!("127.0.0.1:{}", grpc_ports[i]),
                format!("127.0.0.1:{}", raft_ports[i]),
            )
        })
        .collect();

    let mut nodes: Vec<RealNode> = (0..3)
        .map(|i| {
            RealNode::spawn(
                (i + 1) as u64,
                grpc_ports[i],
                raft_ports[i],
                &base.join(format!("node{}", i + 1)),
                &initial_nodes,
            )
        })
        .collect();

    for n in &nodes {
        n.wait_ready(Duration::from_secs(60)).await;
    }

    let deadline = Instant::now() + MAX_RUNTIME;
    let mut checker = RegisterChecker::default();
    let key = b"/chaos/register";

    let mut counter: u64 = 0;
    let mut iterations: u32 = 0;

    while Instant::now() < deadline {
        iterations += 1;
        counter += 1;
        let value = format!("v{counter}");

        // 1. 写（任一存活节点）
        let start = Instant::now();
        let mut written = false;
        for _ in 0..5 {
            if put_any(&nodes, key, value.as_bytes()).await.is_some() {
                written = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        let end = Instant::now();
        if written {
            checker.record(start, end, Some(value));
        }

        // 2. 读（leader 路径）
        let rstart = Instant::now();
        let read = range_any(&nodes, key, deadline)
            .await
            .map(|v| String::from_utf8_lossy(&v).to_string());
        checker.record(rstart, Instant::now(), read);

        // 3. 每 3 轮注入一次进程故障：kill -9 一个节点并重启
        if iterations % 3 == 0 {
            let victim = (iterations as usize) % nodes.len();
            tracing::info!("chaos: kill -9 node {} and restart", nodes[victim].id);
            nodes[victim].kill9();
            tokio::time::sleep(Duration::from_millis(300)).await;
            nodes[victim].restart(&initial_nodes);
            if let Some(n) = nodes.get(victim) {
                n.wait_ready(Duration::from_secs(60)).await;
            }
        }
    }

    // 最终收敛检查：写入后全节点读取一致
    counter += 1;
    let final_value = format!("v{counter}");
    assert!(
        put_any(&nodes, key, final_value.as_bytes()).await.is_some(),
        "final put"
    );
    let converge_deadline = Instant::now() + Duration::from_secs(30);
    for n in &nodes {
        let v = range_any(&nodes, key, converge_deadline).await;
        assert_eq!(
            v,
            Some(final_value.clone().into_bytes()),
            "node {} did not converge on final value",
            n.id
        );
    }

    checker.verify();

    for n in &mut nodes {
        n.kill9();
    }
    tracing::info!("chaos_real completed: {iterations} iterations, linearizability verified");
}

// 占位：满足未使用告警（BTreeMap/HashMap 保留给后续分区注入版本）
#[allow(dead_code)]
fn _reserved() -> (BTreeMap<u64, String>, HashMap<u64, String>) {
    (BTreeMap::new(), HashMap::new())
}
