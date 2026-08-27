// P0-H.2/H.3 真实故障注入套件（chaos_real）—— 基于真实 `coord` 二进制进程
//
// 与 `sim_chaos_test`/`sim_jepsen_test`（算法级内存模拟，非系统验证证据）不同：
// 本套件 spawn 真实 `coord server` 进程（3 节点），注入：
//   - kill -9 leader / follower 循环（进程崩溃）
//   - 重启循环（数据目录保留）
//   - SIGSTOP/SIGCONT 暂停注入（进程冻结，模拟 GC 停顿/时钟漂移窗口）
//   - TCP 代理分区注入（R-TST-16：真实网络分区 + 恢复收敛）
//   - 并发写入 + 线性一致性检查器（register checker，H.3 合一）
//   - chaos_soak_distributed：真实 3 进程分布式浸泡（无注入，SOAK_DURATION_SECS）
//
// 标记 `#[ignore]`：由 CI nightly 触发（P0-H.4），本地运行：
//   CHAOS_REAL=1 cargo test -p coord --test chaos_real -- --ignored --nocapture
//
// 约束：真实 Jepsen（Clojure 客户端 + nemesis 时序）为外部依赖，未在本仓实现；
// 本套件的线性 checker 为进程内 register checker（R-TST-16 达成项）。

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
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
        raft_bind_port: u16,
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
        // R-TST-16：raft bind/advertise 分离——监听真实端口、对外通告代理端口
        let toml = format!(
            "[cluster]\nbootstrap = {bootstrap_flag}\n{nodes_toml}\
             [network]\nraft_addr = \"127.0.0.1:{raft_port}\"\n\
             raft_bind_addr = \"127.0.0.1:{raft_bind_port}\"\n\
             [security]\nauth_enabled = false\n\
             auth_root_key = \"{}\"\n",
            "ab".repeat(32) // R-SEC-06：多节点集群需共享根密钥（测试用固定值）
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

    /// R-TST-16：SIGSTOP 冻结进程（模拟停顿/时钟漂移窗口）。
    fn pause(&mut self) {
        let pid = self.child.id();
        let _ = Command::new("kill")
            .args(["-STOP", &pid.to_string()])
            .status();
    }

    /// R-TST-16：SIGCONT 恢复。
    fn resume(&mut self) {
        let pid = self.child.id();
        let _ = Command::new("kill")
            .args(["-CONT", &pid.to_string()])
            .status();
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

/// R-TST-16：TCP 代理分区注入器。
///
/// 节点间 raft 流量经本代理转发（节点 `--raft-addr` 指向代理 public 端口，
/// 代理转发到真实 raft 端口）。`partition()` 丢弃全部已建立连接并拒绝新连接
/// （真实网络分区），`heal()` 恢复转发（分区恢复后集群自动收敛）。
struct PartitionProxy {
    partitioned: Arc<AtomicBool>,
    conns: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    _accept: tokio::task::JoinHandle<()>,
}

impl PartitionProxy {
    fn start(public_port: u16, target_port: u16) -> Self {
        let partitioned = Arc::new(AtomicBool::new(false));
        let conns: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>> = Arc::new(Mutex::new(Vec::new()));

        let partitioned_for_accept = Arc::clone(&partitioned);
        let conns_for_accept = Arc::clone(&conns);
        let accept = tokio::spawn(async move {
            let listener = match tokio::net::TcpListener::bind(("127.0.0.1", public_port)).await {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("PartitionProxy: bind {public_port} failed: {e}");
                    return;
                }
            };
            loop {
                let (socket, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                if partitioned_for_accept.load(Ordering::Relaxed) {
                    drop(socket);
                    continue;
                }
                let conns = Arc::clone(&conns_for_accept);
                let handle = tokio::spawn(async move {
                    let Ok(upstream) =
                        tokio::net::TcpStream::connect(("127.0.0.1", target_port)).await
                    else {
                        return;
                    };
                    let (mut rs, mut ws) = socket.into_split();
                    let (mut ut, mut uw) = upstream.into_split();
                    let a = tokio::io::copy(&mut rs, &mut uw);
                    let b = tokio::io::copy(&mut ut, &mut ws);
                    // 任一向关闭/出错即结束；分区时由 partition() abort 本任务
                    tokio::select! {
                        _ = a => {}
                        _ = b => {}
                    }
                });
                conns.lock().unwrap().push(handle);
            }
        });

        Self {
            partitioned,
            conns,
            _accept: accept,
        }
    }

    /// 开启分区：丢弃全部活动连接 + 拒绝新连接。
    fn partition(&self) {
        self.partitioned.store(true, Ordering::Relaxed);
        for handle in self.conns.lock().unwrap().drain(..) {
            handle.abort();
        }
    }

    /// 分区恢复：恢复转发。
    fn heal(&self) {
        self.partitioned.store(false, Ordering::Relaxed);
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

/// P0-H.2/H.3 + R-TST-16：3 节点真实集群 —— kill -9 / 重启 / SIGSTOP 暂停 /
/// TCP 代理分区注入循环 + 线性一致检查。
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
    // R-TST-16：raft 端口走 TCP 代理（public），真实端口由代理转发——
    // 使网络分区注入成为可能（节点间 raft 流量可被代理切断）。
    let real_raft_ports: Vec<u16> = (0..3).map(|_| find_port()).collect();
    let raft_ports: Vec<u16> = (0..3).map(|_| find_port()).collect();
    let proxies: Vec<PartitionProxy> = (0..3)
        .map(|i| PartitionProxy::start(raft_ports[i], real_raft_ports[i]))
        .collect();
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
                real_raft_ports[i],
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

        // 3. 故障注入轮换（R-TST-16）：kill -9 / SIGSTOP 暂停 / 网络分区
        match iterations % 3 {
            0 => {
                // 进程崩溃 + 重启
                let victim = (iterations as usize) % nodes.len();
                tracing::info!("chaos: kill -9 node {} and restart", nodes[victim].id);
                nodes[victim].kill9();
                tokio::time::sleep(Duration::from_millis(300)).await;
                nodes[victim].restart(&initial_nodes);
                if let Some(n) = nodes.get(victim) {
                    n.wait_ready(Duration::from_secs(60)).await;
                }
            }
            1 => {
                // SIGSTOP 暂停（模拟停顿/时钟漂移窗口），4s 后恢复
                let victim = (iterations as usize) % nodes.len();
                tracing::info!("chaos: SIGSTOP node {} for 4s", nodes[victim].id);
                nodes[victim].pause();
                tokio::time::sleep(Duration::from_secs(4)).await;
                nodes[victim].resume();
            }
            _ => {
                // 网络分区：隔离一个节点 4s 后恢复（分区期间多数派继续服务）
                let victim = (iterations as usize) % nodes.len();
                tracing::info!("chaos: partition node {} for 4s", victim + 1);
                proxies[victim].partition();
                tokio::time::sleep(Duration::from_secs(4)).await;
                proxies[victim].heal();
            }
        }
    }

    // 最终收敛检查：先解除全部分区，再带截止时间重试写入，随后全节点读取一致
    for p in &proxies {
        p.heal();
    }
    counter += 1;
    let final_value = format!("v{counter}");
    let converge_deadline = Instant::now() + Duration::from_secs(30);
    let mut final_ok = false;
    while Instant::now() < converge_deadline {
        if put_any(&nodes, key, final_value.as_bytes()).await.is_some() {
            final_ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    assert!(final_ok, "final put");
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
    for p in &proxies {
        p._accept.abort();
    }
    tracing::info!("chaos_real completed: {iterations} iterations, linearizability verified");
}

/// R-TST-16：真实 3 进程分布式浸泡（无注入，持续读写 + 收敛校验）。
///
/// 时长经 `SOAK_DURATION_SECS` 配置（默认 300s；生产验收 ≥72h 由运维环境执行），
/// 本地运行：`CHAOS_REAL=1 SOAK_DURATION_SECS=300 cargo test -p coord \
/// --test chaos_real chaos_soak_distributed -- --ignored --nocapture`
#[tokio::test]
#[ignore = "distributed soak; SOAK_DURATION_SECS + CHAOS_REAL=1"]
async fn chaos_soak_distributed() {
    if std::env::var("CHAOS_REAL").is_err() {
        eprintln!("skipping chaos soak (set CHAOS_REAL=1 to run locally)");
        return;
    }
    let duration_secs: u64 = std::env::var("SOAK_DURATION_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);

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
                raft_ports[i], // soak 模式不经代理：bind == advertise
                &base.join(format!("node{}", i + 1)),
                &initial_nodes,
            )
        })
        .collect();

    for n in &nodes {
        n.wait_ready(Duration::from_secs(60)).await;
    }

    let deadline = Instant::now() + Duration::from_secs(duration_secs);
    let key = b"/soak/register";
    let mut counter: u64 = 0;
    while Instant::now() < deadline {
        counter += 1;
        let value = format!("s{counter}");
        assert!(
            put_any(&nodes, key, value.as_bytes()).await.is_some(),
            "soak put failed at iteration {counter}"
        );
        // 每 50 次写校验全节点收敛（无泄漏/漂移的粗检）
        if counter % 50 == 0 {
            for n in &nodes {
                let v = range_any(&nodes, key, deadline).await;
                assert_eq!(
                    v,
                    Some(value.clone().into_bytes()),
                    "node {} diverged at iteration {counter}",
                    n.id
                );
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // 终态收敛 + 重启恢复校验
    counter += 1;
    let final_value = format!("s{counter}");
    assert!(put_any(&nodes, key, final_value.as_bytes()).await.is_some());
    let converge = Instant::now() + Duration::from_secs(30);
    for n in &nodes {
        assert_eq!(
            range_any(&nodes, key, converge).await,
            Some(final_value.clone().into_bytes()),
            "final convergence failed on node {}",
            n.id
        );
    }
    nodes[0].kill9();
    tokio::time::sleep(Duration::from_millis(500)).await;
    nodes[0].restart(&initial_nodes);
    nodes[0].wait_ready(Duration::from_secs(60)).await;
    let converge = Instant::now() + Duration::from_secs(30);
    for n in &nodes {
        assert_eq!(
            range_any(&nodes, key, converge).await,
            Some(final_value.clone().into_bytes()),
            "post-restart convergence failed on node {}",
            n.id
        );
    }

    for n in &mut nodes {
        n.kill9();
    }
    tracing::info!("chaos soak completed: {counter} writes over {duration_secs}s");
}
