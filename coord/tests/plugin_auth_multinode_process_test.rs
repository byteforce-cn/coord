// 进程级验收：插件账户（capability + scope）在 **多节点（双 raft）** 集群上的
// 复制与「整集群重启」存续（计划 §19「待推进（批次 4 后）」收口项）。
//
// 与 `plugin_auth_process_test.rs`（单节点）互补：单节点只证明「同进程数据目录
// 重启 → 重放存续」；本套件引入**第二个 raft 节点**，证明授权是经 **raft 复制**
// 落到对端状态机的（而非单机内存/单机落盘），并且整集群停机重启后仍然存续。
//
// 链路：
//   1. 双节点集群组建（node1 bootstrap + node2；共享 `security.auth_root_key`）；
//   2. 在 region 0 **leader** 上 `RoleAdd` + `RoleGrantCapability(role, cap, scope)`
//      + `UserAdd` + `UserGrantRole`；
//   3. 在 **follower** 上 `RoleList` 读回该角色与 capability/scope
//      → 证明授权经 raft 复制；
//   4. 插件账户在 leader 上认证：scope 内 KV 写通过 / 越界被拒；
//   5. **双节点全量 kill + 同数据目录重启**（重新选举）
//      → 重新认证：授权存续、scope 仍被强制；follower 侧复制状态仍在。
//
// 与 multi_raft_process_test 的串行约定一致——重型进程套件（每用例 2 个
// `coord server` 进程 + raft fsync）**必须串行**，避免互相拖慢造成伪超时。
//
// 标记 `#[ignore]`：重型进程套件，显式运行：
//   cargo test -p coord --test plugin_auth_multinode_process_test -- --ignored --nocapture

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::Request;

use coord_proto::auth::auth_client::AuthClient;
use coord_proto::auth::{
    AuthenticateRequest, RoleAddRequest, RoleGrantCapabilityRequest, RoleListRequest,
    UserAddRequest, UserGrantRoleRequest,
};
use coord_proto::kv::kv_client::KvClient;
use coord_proto::kv::{PutRequest, RangeRequest};

/// root 密码（经 `COORD_ROOT_PASSWORD` 注入子进程）。
const ROOT_PASSWORD: &str = "plugin-multinode-root-pw-123";
/// 多节点集群共享根密钥（R-SEC-06：CCT 签发/校验密钥须一致；测试用固定 32 字节 hex）。
const AUTH_ROOT_KEY_HEX: &str = "abababababababababababababababababababababababababababababababab";
/// 插件角色/账户与 scope。
const PLUGIN_ROLE: &str = "plugin/counter-role";
const PLUGIN_USER: &str = "plugin/counter";
const PLUGIN_PASSWORD: &str = "counter-pw-123456";
const PLUGIN_SCOPE: &str = "/app/counter/";

/// 本文件进程用例串行锁（见文件头注释）。
static PROCESS_SUITE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn find_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// 选一对 (grpc, raft)：保证 grpc、grpc+10（HTTP 端点）、raft 互不相同且空闲。
fn find_ports() -> (u16, u16) {
    for _ in 0..500 {
        let grpc = find_free_port();
        let Some(http) = grpc.checked_add(10) else {
            continue;
        };
        let raft = find_free_port();
        if http == raft {
            continue;
        }
        if std::net::TcpListener::bind(("127.0.0.1", http)).is_err() {
            continue;
        }
        return (grpc, raft);
    }
    panic!("could not find a free (grpc, raft) port pair");
}

/// 集群节点进程句柄（drop 即杀，避免测试失败时泄漏常驻进程）。
struct ClusterNode {
    id: u64,
    grpc_port: u16,
    raft_port: u16,
    data_dir: PathBuf,
    cfg_path: PathBuf,
    child: Child,
}

impl Drop for ClusterNode {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl ClusterNode {
    /// 写入本节点 node.toml（cluster.initial_nodes 全量 + bootstrap 标志）。
    fn write_config(
        data_dir: &Path,
        initial_nodes: &[(u64, u16, u16)],
        bootstrap: bool,
    ) -> PathBuf {
        std::fs::create_dir_all(data_dir).expect("create data dir");
        let mut nodes_toml = String::new();
        for (nid, grpc, raft) in initial_nodes {
            nodes_toml.push_str(&format!(
                "[[cluster.initial_nodes]]\nid = {nid}\n\
                 grpc = \"127.0.0.1:{grpc}\"\nraft = \"127.0.0.1:{raft}\"\n"
            ));
        }
        let toml = format!(
            "[cluster]\ncluster_name = \"plugin-auth-2n\"\nbootstrap = {bootstrap}\n\
             {nodes_toml}\n\
             [security]\nauth_enabled = true\nauth_root_key = \"{AUTH_ROOT_KEY_HEX}\"\n"
        );
        let cfg_path = data_dir.join("node.toml");
        std::fs::write(&cfg_path, toml).expect("write node config");
        cfg_path
    }

    fn spawn(
        id: u64,
        grpc_port: u16,
        raft_port: u16,
        data_dir: &Path,
        initial_nodes: &[(u64, u16, u16)],
        bootstrap: bool,
    ) -> Self {
        let cfg_path = Self::write_config(data_dir, initial_nodes, bootstrap);
        let child = Self::spawn_process(id, grpc_port, raft_port, data_dir, &cfg_path);
        Self {
            id,
            grpc_port,
            raft_port,
            data_dir: data_dir.to_path_buf(),
            cfg_path,
            child,
        }
    }

    fn spawn_process(
        id: u64,
        grpc_port: u16,
        raft_port: u16,
        data_dir: &Path,
        cfg_path: &Path,
    ) -> Child {
        let bin = env!("CARGO_BIN_EXE_coord");
        let log_file = std::fs::File::create(data_dir.join("server.log")).expect("create node log");
        Command::new(bin)
            .arg("server")
            .arg("--id")
            .arg(id.to_string())
            .arg("--addr")
            .arg(format!("127.0.0.1:{grpc_port}"))
            .arg("--raft-addr")
            .arg(format!("127.0.0.1:{raft_port}"))
            .arg("--data-dir")
            .arg(data_dir)
            .arg("--config")
            .arg(cfg_path)
            .env("COORD_ROOT_PASSWORD", ROOT_PASSWORD)
            .env("RUST_LOG", "coord=warn")
            .stdout(Stdio::from(log_file.try_clone().unwrap()))
            .stderr(Stdio::from(log_file))
            .spawn()
            .expect("spawn coord server")
    }

    /// 重启（同一数据目录 / 端口 / 配置；不重新 bootstrap）。
    fn restart(&mut self) {
        self.child = Self::spawn_process(
            self.id,
            self.grpc_port,
            self.raft_port,
            &self.data_dir,
            &self.cfg_path,
        );
    }

    /// 等待本节点 HTTP `/healthz` 可达（axum BFF，默认 127.0.0.1:{grpc+10}）。
    async fn wait_ready(&self, timeout: Duration) {
        let healthz_port = self.grpc_port + 10;
        let deadline = Instant::now() + timeout;
        loop {
            if let Ok(mut stream) =
                tokio::net::TcpStream::connect(("127.0.0.1", healthz_port)).await
            {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let req = b"GET /healthz HTTP/1.0\r\nHost: localhost\r\n\r\n";
                if stream.write_all(req).await.is_ok() {
                    let mut buf = [0u8; 128];
                    if let Ok(Ok(n)) =
                        tokio::time::timeout(Duration::from_millis(500), stream.read(&mut buf))
                            .await
                    {
                        if n > 0 && buf.starts_with(b"HTTP/1.") {
                            return;
                        }
                    }
                }
            }
            assert!(
                Instant::now() < deadline,
                "node {} did not become ready within {timeout:?}",
                self.id
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    fn addr(&self) -> String {
        format!("127.0.0.1:{}", self.grpc_port)
    }

    fn log_text(&self) -> String {
        std::fs::read_to_string(self.data_dir.join("server.log")).unwrap_or_default()
    }
}

/// 拉取节点 HTTP `/metrics` 文本。
async fn fetch_metrics(port: u16) -> Option<String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let addr = format!("127.0.0.1:{port}");
    let mut stream = tokio::net::TcpStream::connect(&addr).await.ok()?;
    let req = format!("GET /metrics HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.ok()?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.ok()?;
    String::from_utf8(buf).ok()
}

/// 解析 Prometheus 文本中名为 `name` 的数值（形如 `name 3`）。
fn metric_value(text: &str, name: &str) -> Option<i64> {
    for line in text.lines() {
        let mut parts = line.split_whitespace();
        if parts.next() == Some(name) {
            return parts.next().and_then(|v| v.parse().ok());
        }
    }
    None
}

/// 等待 region 0 选出 leader，返回 leader 节点序号（nodes 下标）。
async fn wait_for_leader(nodes: &[ClusterNode], timeout: Duration) -> usize {
    let deadline = Instant::now() + timeout;
    loop {
        for (idx, n) in nodes.iter().enumerate() {
            if let Some(text) = fetch_metrics(n.grpc_port + 10).await {
                if let Some(l) = metric_value(&text, "raft_leader_id") {
                    if l > 0 && l as u64 == n.id {
                        return idx;
                    }
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "region 0 did not elect a leader within {timeout:?}; logs: {:#?}",
            nodes.iter().map(|n| n.log_text()).collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

async fn channel(addr: &str) -> Channel {
    Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect_timeout(Duration::from_secs(3))
        .connect()
        .await
        .unwrap()
}

fn with_token<T>(req: T, token: &str) -> Request<T> {
    let mut req = Request::new(req);
    req.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {token}")).unwrap(),
    );
    req
}

async fn root_cct(addr: &str) -> String {
    let mut auth = AuthClient::new(channel(addr).await);
    auth.authenticate(AuthenticateRequest {
        name: "root".into(),
        password: ROOT_PASSWORD.into(),
    })
    .await
    .expect("root authenticate")
    .into_inner()
    .cct
}

async fn plugin_cct(addr: &str) -> String {
    let mut auth = AuthClient::new(channel(addr).await);
    auth.authenticate(AuthenticateRequest {
        name: PLUGIN_USER.into(),
        password: PLUGIN_PASSWORD.into(),
    })
    .await
    .expect("plugin account may authenticate")
    .into_inner()
    .cct
}

/// 在 leader 上完成插件账户开通（角色 + capability/scope + 用户 + 绑定）。
async fn provision_plugin_account(leader_addr: &str, root: &str) {
    let mut auth = AuthClient::new(channel(leader_addr).await);
    auth.role_add(with_token(
        RoleAddRequest {
            name: PLUGIN_ROLE.to_string(),
        },
        root,
    ))
    .await
    .expect("root may add role");

    for (cap, scope) in [
        ("data:kv:read", PLUGIN_SCOPE),
        ("data:kv:write", PLUGIN_SCOPE),
    ] {
        auth.role_grant_capability(with_token(
            RoleGrantCapabilityRequest {
                role: PLUGIN_ROLE.to_string(),
                capability_id: cap.to_string(),
                scope: scope.to_string(),
            },
            root,
        ))
        .await
        .unwrap_or_else(|e| panic!("grant {cap}: {e}"));
    }

    auth.user_add(with_token(
        UserAddRequest {
            name: PLUGIN_USER.to_string(),
            password: PLUGIN_PASSWORD.to_string(),
        },
        root,
    ))
    .await
    .expect("root may add user");

    auth.user_grant_role(with_token(
        UserGrantRoleRequest {
            user: PLUGIN_USER.to_string(),
            role: PLUGIN_ROLE.to_string(),
        },
        root,
    ))
    .await
    .expect("root may grant role");
}

/// 在任意节点上读回角色的 capability 授予（follower 亦服务此只读 RPC）。
async fn role_has_grant(addr: &str, root: &str, cap: &str, scope: &str) -> bool {
    let mut auth = AuthClient::new(channel(addr).await);
    let resp = auth
        .role_list(with_token(RoleListRequest {}, root))
        .await
        .unwrap_or_else(|e| panic!("RoleList on {addr} failed: {e}"))
        .into_inner();
    resp.roles.iter().any(|r| {
        r.name == PLUGIN_ROLE
            && r.capability_grants
                .iter()
                .any(|g| g.capability_id == cap && g.scope == scope)
    })
}

async fn put(addr: &str, token: &str, key: &[u8], value: &[u8]) -> tonic::Status {
    let mut kv = KvClient::new(channel(addr).await);
    match kv
        .put(with_token(
            PutRequest {
                key: key.to_vec(),
                value: value.to_vec(),
                lease_id: 0,
                prev_kv: false,
                request_id: vec![],
            },
            token,
        ))
        .await
    {
        Ok(_) => tonic::Status::ok(""),
        Err(status) => status,
    }
}

/// 断言插件 CCT 在指定 leader 上的 scope 语义：内可写、外被拒。
async fn assert_scope_enforced(leader_addr: &str, token: &str, suffix: &str) {
    let in_scope = put(
        leader_addr,
        token,
        format!("/app/counter/{suffix}").as_bytes(),
        b"1",
    )
    .await;
    assert_eq!(
        in_scope.code(),
        tonic::Code::Ok,
        "in-scope write must be allowed ({suffix}): {in_scope}"
    );
    let out_of_scope = put(
        leader_addr,
        token,
        format!("/outside/{suffix}").as_bytes(),
        b"1",
    )
    .await;
    assert_eq!(
        out_of_scope.code(),
        tonic::Code::PermissionDenied,
        "out-of-scope write must be denied ({suffix}): {out_of_scope}"
    );
}

/// 主场景：双节点集群上 capability+scope 的 **raft 复制** 与整集群重启存续。
#[ignore = "real-process auth/plugin multi-node suite; run explicitly with --ignored"]
#[tokio::test(flavor = "multi_thread")]
async fn plugin_capability_survives_multinode_cluster_restart() {
    let _guard = PROCESS_SUITE_LOCK.lock().await;

    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path();

    // 端口互斥（两节点共 4 个端口，两两不同）
    let mut used = std::collections::HashSet::new();
    let mut ports = Vec::new();
    for _ in 0..2 {
        loop {
            let p = find_ports();
            if used.insert(p.0) && used.insert(p.1) {
                ports.push(p);
                break;
            }
        }
    }
    let initial_nodes: Vec<(u64, u16, u16)> = (0..2)
        .map(|i| ((i + 1) as u64, ports[i].0, ports[i].1))
        .collect();

    let mut nodes: Vec<ClusterNode> = (0..2)
        .map(|i| {
            ClusterNode::spawn(
                (i + 1) as u64,
                ports[i].0,
                ports[i].1,
                &base.join(format!("node{}", i + 1)),
                &initial_nodes,
                i == 0, // node1 bootstrap
            )
        })
        .collect();

    for n in &nodes {
        n.wait_ready(Duration::from_secs(60)).await;
    }
    let leader_idx = wait_for_leader(&nodes, Duration::from_secs(60)).await;
    let follower_idx = 1 - leader_idx;
    eprintln!(
        "cluster ready: leader=node{}, follower=node{}",
        nodes[leader_idx].id, nodes[follower_idx].id
    );

    // ── 1. 在 leader 上开通插件账户（授权经 raft 提交）──
    let leader_addr = nodes[leader_idx].addr();
    let follower_addr = nodes[follower_idx].addr();
    let root = root_cct(&leader_addr).await;
    provision_plugin_account(&leader_addr, &root).await;

    // ── 2. follower 读回角色与 capability/scope → 证明 raft 复制 ──
    assert!(
        role_has_grant(&follower_addr, &root, "data:kv:read", PLUGIN_SCOPE).await,
        "follower must observe the granted capability replicated via raft"
    );

    // ── 3. 插件账户在 leader 上认证：scope 语义生效 ──
    let token = plugin_cct(&leader_addr).await;
    assert_scope_enforced(&leader_addr, &token, "before-restart").await;

    // ── 4. 整集群 kill + 同数据目录重启（重新选举）──
    eprintln!("--- restarting both nodes (same data dirs) ---");
    for n in nodes.iter_mut() {
        let _ = n.child.kill();
        let _ = n.child.wait();
    }
    for n in nodes.iter_mut() {
        n.restart();
    }
    for n in &nodes {
        n.wait_ready(Duration::from_secs(60)).await;
    }
    let new_leader_idx = wait_for_leader(&nodes, Duration::from_secs(90)).await;
    let new_follower_idx = 1 - new_leader_idx;
    let new_leader_addr = nodes[new_leader_idx].addr();
    let new_follower_addr = nodes[new_follower_idx].addr();
    eprintln!(
        "cluster restarted: leader=node{}, follower=node{}",
        nodes[new_leader_idx].id, nodes[new_follower_idx].id
    );

    // ── 5. 重启后：账户可重认证、授权存续、scope 仍被强制 ──
    let token = plugin_cct(&new_leader_addr).await;
    assert_scope_enforced(&new_leader_addr, &token, "after-restart").await;

    // ── 6. 重启后 follower 侧复制状态仍在 ──
    let root = root_cct(&new_leader_addr).await;
    assert!(
        role_has_grant(&new_follower_addr, &root, "data:kv:write", PLUGIN_SCOPE).await,
        "follower must still observe the replicated capability after cluster restart"
    );

    // ── 7. 重启前的写入真的落盘（raft 日志 + 状态机），读回可见 ──
    let mut kv = KvClient::new(channel(&new_leader_addr).await);
    let range = kv
        .range(with_token(
            RangeRequest {
                key: b"/app/counter/".to_vec(),
                range_end: b"/app/counter0".to_vec(),
                limit: 10,
                revision: 0,
                keys_only: false,
                count_only: false,
            },
            &token,
        ))
        .await
        .expect("in-scope read must be allowed after cluster restart");
    assert_eq!(
        range.into_inner().kvs.len(),
        2,
        "both writes (before + after restart) must be visible"
    );
}
