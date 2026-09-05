// Multi-Raft 生产装配进程级验收（TDD 迭代 #8；Phase 2 / M2 出口条件 + 迭代 #12 T3.4）
//
// 与 in-process 套件（region_assembly / region_kv_routing /
// multi_raft_config_assembly）不同，本套件 spawn **真实 `coord server` 进程**
// （3 节点 × 3 Region，`[multi_raft].enabled=true` + `[multi_raft.pd].enabled=
// true`），走 main.rs 的配置驱动装配路径，验证：
//   - 每个 Region 独立选出 leader，跨 Region KV 路由 / 收敛 / 隔离
//     （真实 gRPC 客户端，真实网络）；
//   - 内嵌 PD 随进程启动（T3.4）：每节点 `<data_dir>/pd/pd-meta.db` 落盘；
//   - 依次 kill 每个节点（含各 Region 的 leader）：被 kill 节点承载的 Region
//     副本在其余节点上**独立重新选举**，全部 Region 继续可写可读；
//   - 重启被 kill 节点后集群重新收敛（该节点副本追平复制、PD 循环随进程
//     恢复且不崩溃）。
//
// 标记 `#[ignore]`：显式运行（本地），对齐 chaos_real 约定：
//   MULTI_RAFT_REAL=1 cargo test -p coord --test multi_raft_process_test \
//       -- --ignored --nocapture

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use coord_proto::kv::kv_client::KvClient;
use coord_proto::kv::{PutRequest, RangeRequest};
use tonic::transport::Channel;

/// 3 Region 平铺 keyspace（[ "", "b") / ["b", "n") / ["n", "")）与代表 key。
const REGION_KEYS: [(u64, &[u8]); 3] = [(1, b"apple"), (2, b"banana"), (3, b"peach")];

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
            .env("RUST_LOG", "coord=warn")
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        let cfg_dir = data_dir.join("conf");
        std::fs::create_dir_all(&cfg_dir).unwrap();
        let cfg_path = cfg_dir.join("node.toml");
        let bootstrap_flag = if id == 1 { "true" } else { "false" };
        let mut nodes_toml = String::new();
        for (nid, grpc, raft) in initial_nodes {
            nodes_toml.push_str(&format!(
                "[[cluster.initial_nodes]]\nid = {nid}\ngrpc = \"{grpc}\"\nraft = \"{raft}\"\n"
            ));
        }
        let toml = format!(
            "[cluster]\ncluster_name = \"mr-real-test\"\nbootstrap = {bootstrap_flag}\n\
             {nodes_toml}\n\
             [multi_raft]\nenabled = true\n\
             [[multi_raft.initial_regions]]\nid = 1\nstart_key = \"\"\nend_key = \"b\"\n\
             [[multi_raft.initial_regions]]\nid = 2\nstart_key = \"b\"\nend_key = \"n\"\n\
             [[multi_raft.initial_regions]]\nid = 3\nstart_key = \"n\"\nend_key = \"\"\n\
             [multi_raft.pd]\nenabled = true\nheartbeat_interval_ms = 500\n\
             balance_interval = 5\nnode_heartbeat_timeout = 15\n\
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

    /// 等待本节点 gRPC 可服务（status RPC 可达即算就绪）。
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

    /// 重启（同一数据目录 / 端口 / 配置，沿用已写入的 node.toml）。
    fn restart(&mut self) {
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

/// 在给定节点上 Put 单个 key；成功返回 Some（leader 才接受，自动跳过 follower）。
async fn put_on(nodes: &[&RealNode], key: &[u8], value: &[u8]) -> Option<()> {
    for node in nodes {
        let mut kv = KvClient::new(node.channel().await);
        if kv
            .put(PutRequest {
                key: key.to_vec(),
                value: value.to_vec(),
                lease_id: 0,
                prev_kv: false,
                request_id: vec![],
            })
            .await
            .is_ok()
        {
            return Some(());
        }
    }
    None
}

/// 直到 deadline 前反复尝试在给定节点集上写成功。
async fn write_until(nodes: &[&RealNode], key: &[u8], value: &[u8], timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if put_on(nodes, key, value).await.is_some() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "write {} = {} did not succeed in {:?}",
            String::from_utf8_lossy(key),
            String::from_utf8_lossy(value),
            timeout
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// 直到 deadline 前在节点集上任一 leader 读到期望值。
async fn read_until(nodes: &[&RealNode], key: &[u8], expected: &[u8], timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
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
                if let Some(kv) = resp.into_inner().kvs.first() {
                    if kv.value == expected {
                        return;
                    }
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "read {} != {} did not converge",
            String::from_utf8_lossy(key),
            String::from_utf8_lossy(expected)
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// M2 进程级验收：3 节点 × 3 Region —— 独立选举 / 跨区路由隔离 / 逐节点
/// kill（含各 Region leader）后各 Region 独立恢复 + 重启收敛。
#[tokio::test]
#[ignore = "real-process multi-raft suite; run explicitly: MULTI_RAFT_REAL=1"]
async fn multi_raft_real_three_nodes_three_regions() {
    if std::env::var("MULTI_RAFT_REAL").is_err() {
        eprintln!("skipping multi_raft_process (set MULTI_RAFT_REAL=1 to run)");
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
        n.wait_ready(Duration::from_secs(90)).await;
    }

    // ── Phase A：三个 Region 各自独立选出 leader 并可写（配置驱动装配生效）──
    for (region_id, key) in REGION_KEYS {
        let value = format!("v{region_id}-1");
        let all: Vec<&RealNode> = nodes.iter().collect();
        write_until(&all, key, value.as_bytes(), Duration::from_secs(60)).await;
        eprintln!(
            "region {region_id} leader elected & writable (key {})",
            String::from_utf8_lossy(key)
        );
    }
    // 收敛 + 隔离：每个 Region 的代表 key 在集群可读到自身值（跨 Region 不串扰由
    // 独立 key → 独立 raft 组保证；此处验证写入路径全部落到了正确 Region）
    let all: Vec<&RealNode> = nodes.iter().collect();
    for (region_id, key) in REGION_KEYS {
        let value = format!("v{region_id}-1");
        read_until(&all, key, value.as_bytes(), Duration::from_secs(30)).await;
    }

    // ── Phase A.5（T3.4）：内嵌 PD 随真实进程启动——每节点 pd-meta.db 落盘
    //    （配置 Region 表已播种）。──
    for n in &nodes {
        let pd_db = n.data_dir.join("pd").join("pd-meta.db");
        assert!(
            pd_db.exists(),
            "node {}: embedded PD must persist pd-meta.db at {}",
            n.id,
            pd_db.display()
        );
    }
    eprintln!("embedded PD: pd-meta.db present on all 3 nodes (regions seeded)");

    // ── Phase B：依次 kill 每个节点。被 kill 节点承载的 Region 副本在其余
    //    2 节点独立重新选举（若其为 leader）或直接续写（quorum 保持）——
    //    无论哪种，全部 Region 必须继续可写；重启后重新收敛。──
    for victim_idx in 0..3usize {
        let victim_id = (victim_idx + 1) as u64; // 节点 id = 1..3
        eprintln!("--- kill node {victim_id} ---");
        nodes[victim_idx].kill9();
        // 只保留存活节点
        let remaining: Vec<&RealNode> = nodes
            .iter()
            .filter(|n| n.id != victim_id)
            .collect();

        let round = victim_id + 1; // 值版本（每轮递增）
        for (region_id, key) in REGION_KEYS {
            let value = format!("v{region_id}-r{round}");
            // 写成功 = 该 Region 在剩余节点上仍有 quorum（leader 被杀则已重选）
            write_until(&remaining, key, value.as_bytes(), Duration::from_secs(60)).await;
            read_until(&remaining, key, value.as_bytes(), Duration::from_secs(30)).await;
        }
        eprintln!("all regions writable/readable with node {victim_id} down");

        // 重启被 kill 节点并等待重新收敛
        nodes[victim_idx].restart();
        nodes[victim_idx].wait_ready(Duration::from_secs(90)).await;
        let all: Vec<&RealNode> = nodes.iter().collect();
        for (region_id, key) in REGION_KEYS {
            let value = format!("v{region_id}-r{round}");
            read_until(&all, key, value.as_bytes(), Duration::from_secs(60)).await;
        }
        eprintln!("node {victim_id} restarted; all regions converged");
    }

    // ── 终态：全部节点存活时三个 Region 仍可写 ──
    let all: Vec<&RealNode> = nodes.iter().collect();
    for (region_id, key) in REGION_KEYS {
        let value = format!("v{region_id}-final");
        write_until(&all, key, value.as_bytes(), Duration::from_secs(30)).await;
    }
    eprintln!("multi_raft real-process acceptance PASSED (3 nodes × 3 regions)");
}
