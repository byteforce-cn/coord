// 进程级验收（第四轮 A4）：**「应用 → agent → server」在 `auth_enabled = true`
// 下的完整链路**。
//
// 为什么必须有这一条：第四轮复核 §0.2 指出，本仓所有经 agent 的进程测试一律
// `auth_enabled = false`，而所有 `auth_enabled = true` 的进程测试都让 `KvClient`
// **直连 server**。两个集合没有交集——这正是下列矛盾能长期存在的原因：
//
//   1. agent 出站客户端不带凭据（§3.2）→ 生产默认配置下经 agent 的调用一律
//      `missing CCT token`；
//   2. agent 自身 scope 校验 fail-open（§3.3）→ 资源级授权在最后一跳被关闭；
//   3. agent 能力表 KV 路径拼写错误且缺少目标场景服务（§3.4）→ 开启 agent 鉴权
//      时 KV 调用被 `unknown RPC method` 拒绝。
//
// 本套件是防止上述三项重新回归的**唯一结构性保障**：它同时要求
//   - agent **转发**调用方 CCT（否则 server 拒绝：A1）；
//   - agent 的 scope 提取器在**生产装配路径**上可用（否则带 scope 的授权被
//     fail-closed 拒绝：A2）；
//   - 能力表中 `/coord.kv.KV/*` 拼写正确（否则 agent 以 unknown RPC 拒绝：A3）。
//
// 断言的不是"链路能连上"，而是**授权语义正确**：
//   - scope 内 KV 读 → 放行（三处修复同时成立才可能通过）；
//   - scope 外 KV 读 → 拒绝（agent 本地 scope 校验真的在跑）；
//   - 无凭据 → 拒绝。
//
// 标记 `#[ignore]`（重型进程套件），显式运行：
//   AGENT_AUTH_REAL=1 cargo test -p coord --test agent_auth_process_test -- --ignored --nocapture

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::Request;

use coord_proto::auth::auth_client::AuthClient;
use coord_proto::auth::{
    AuthenticateRequest, RoleAddRequest, RoleGrantCapabilityRequest, UserAddRequest,
    UserGrantRoleRequest,
};
use coord_proto::agent::handshake_client::HandshakeClient;
use coord_proto::agent::HandshakeRequest;
use coord_proto::kv::kv_client::KvClient;
use coord_proto::kv::RangeRequest;

/// root 密码（经 `COORD_ROOT_PASSWORD` 注入 server 子进程）。
const ROOT_PASSWORD: &str = "agent-auth-process-root-pw-123";
/// agent 一次性引导令牌（server `[security].agent_bootstrap_tokens`；≥16 字符）。
const BOOTSTRAP_TOKEN: &str = "agent-auth-process-bootstrap-9f3c";
/// server 根密钥（`[security].auth_root_key`，32 字节 hex）。
/// agent 侧的 Ed25519 **公钥**由同一根密钥经 HKDF 派生，见 `agent_verifying_key_hex`。
const ROOT_KEY_HEX: &str = "7f1c4c0d5b2a9e8364d1f0a7c3b58e29d4f6a1b0c7e2538496ab0d3f1e2c4a57";

fn find_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// 选一对 (grpc, raft)：保证 grpc、grpc+10（server 的 HTTP 端点）、raft 互不相同且空闲。
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

/// 由 server 根密钥派生 agent 侧需要的 Ed25519 公钥（与服务端
/// `TokenSigningKeyring::ed25519_signing_key` 同一 HKDF 派生路径）。
fn agent_verifying_key_hex() -> String {
    let root = hex::decode(ROOT_KEY_HEX).expect("ROOT_KEY_HEX is valid hex");
    let keyring = coord_server::auth::token_signing::TokenSigningKeyring::new(root)
        .expect("build token signing keyring");
    let vk = keyring
        .ed25519_signing_key()
        .expect("derive ed25519 signing key")
        .verifying_key()
        .to_bytes();
    hex::encode(vk)
}

// ──── 子进程句柄（drop 即杀，避免测试失败时泄漏常驻进程）────

struct Proc {
    child: Child,
    log: PathBuf,
}

impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct ServerProc {
    grpc_port: u16,
    proc: Proc,
}

impl ServerProc {
    fn spawn(data_dir: &Path, grpc_port: u16, raft_port: u16, config_toml: &str) -> Self {
        let bin = env!("CARGO_BIN_EXE_coord");
        std::fs::create_dir_all(data_dir).expect("create server data dir");
        let cfg_path = data_dir.join("server.toml");
        std::fs::write(&cfg_path, config_toml).expect("write server config");
        let log = data_dir.join("server.log");
        let log_file = std::fs::File::create(&log).expect("create server log");
        let child = Command::new(bin)
            .arg("server")
            .arg("--id")
            .arg("1")
            .arg("--addr")
            .arg(format!("127.0.0.1:{grpc_port}"))
            .arg("--raft-addr")
            .arg(format!("127.0.0.1:{raft_port}"))
            .arg("--data-dir")
            .arg(data_dir)
            .arg("--config")
            .arg(&cfg_path)
            .arg("--bootstrap")
            .env("COORD_ROOT_PASSWORD", ROOT_PASSWORD)
            .env("RUST_LOG", "coord=warn")
            .stdout(Stdio::from(log_file.try_clone().unwrap()))
            .stderr(Stdio::from(log_file))
            .spawn()
            .expect("spawn coord server");
        Self {
            grpc_port,
            proc: Proc { child, log },
        }
    }

    async fn wait_ready(&self, timeout: Duration) {
        let healthz_port = self.grpc_port + 10;
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
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
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        panic!(
            "server did not become ready within {timeout:?}; tail of {}:\n{}",
            self.proc.log.display(),
            tail(&self.proc.log, 40)
        );
    }
}

struct AgentProc {
    agent_port: u16,
    proc: Proc,
}

impl AgentProc {
    fn spawn(cfg_path: &Path) -> Self {
        let bin = env!("CARGO_BIN_EXE_coord");
        let log = cfg_path.with_extension("agent.log");
        let log_file = std::fs::File::create(&log).expect("create agent log");
        let child = Command::new(bin)
            .arg("agent")
            .arg("--agent-config")
            .arg(cfg_path)
            .env("RUST_LOG", "coord=info")
            .stdout(Stdio::from(log_file.try_clone().unwrap()))
            .stderr(Stdio::from(log_file))
            .spawn()
            .expect("spawn coord agent");
        let text = std::fs::read_to_string(cfg_path).expect("read agent config");
        let agent_port = text
            .lines()
            .find_map(|l| {
                let l = l.trim();
                l.strip_prefix("agent_addr")
                    .and_then(|v| v.split(':').next_back())
                    .map(|p| p.trim().trim_matches('"').parse::<u16>().expect("port"))
            })
            .expect("agent_addr present in agent config");
        Self {
            agent_port,
            proc: Proc { child, log },
        }
    }

    fn addr(&self) -> String {
        format!("127.0.0.1:{}", self.agent_port)
    }

    async fn wait_listening(&self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if tokio::net::TcpStream::connect(self.addr()).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        panic!(
            "agent did not start listening on {}; tail of {}:\n{}",
            self.addr(),
            self.proc.log.display(),
            tail(&self.proc.log, 40)
        );
    }
}

fn tail(path: &Path, lines: usize) -> String {
    let text = std::fs::read_to_string(path).unwrap_or_default();
    let all: Vec<&str> = text.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

fn run_cli(args: &[&str]) -> std::process::Output {
    let bin = env!("CARGO_BIN_EXE_coord");
    let home = std::env::temp_dir().join(format!("coord-agent-auth-home-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&home);
    Command::new(bin)
        .args(args)
        .env("RUST_LOG", "coord=error")
        .env("XDG_CONFIG_HOME", home)
        .env_remove("COORD_TOKEN")
        .env_remove("COORD_CREDENTIALS")
        .output()
        .expect("run coord CLI")
}

// ──── gRPC 工具 ────

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

fn range_req(key: &[u8]) -> RangeRequest {
    RangeRequest {
        key: key.to_vec(),
        range_end: Vec::new(),
        limit: 0,
        revision: 0,
        keys_only: false,
        count_only: false,
    }
}

/// 主场景：`应用 → agent → server`，`auth_enabled = true`。
#[ignore = "real-process agent+auth suite; run explicitly: AGENT_AUTH_REAL=1"]
#[tokio::test(flavor = "multi_thread")]
async fn agent_forwards_credentials_and_enforces_scope_under_auth() {
    // E1：拒绝把「未跑」伪装成「通过」——门控变量缺失即失败。
    assert!(
        std::env::var("AGENT_AUTH_REAL")
            .map(|v| !v.is_empty())
            .unwrap_or(false),
        "AGENT_AUTH_REAL must be set to run this real-process suite (E1)"
    );

    let (grpc_port, raft_port) = find_ports();
    let tmp = tempfile::tempdir().unwrap();
    let server_dir = tmp.path().join("server");
    let agent_dir = tmp.path().join("agent");
    std::fs::create_dir_all(&agent_dir).unwrap();
    let server_addr = format!("127.0.0.1:{grpc_port}");

    // ──── 1. 真实 server：鉴权开启 + 一次性 agent 引导令牌 ────
    let server = ServerProc::spawn(
        &server_dir,
        grpc_port,
        raft_port,
        &format!(
            "[security]\nauth_enabled = true\nauth_root_key = \"{ROOT_KEY_HEX}\"\n\
             agent_bootstrap_tokens = [\"{BOOTSTRAP_TOKEN}\"]\n"
        ),
    );
    server.wait_ready(Duration::from_secs(60)).await;

    let mut auth = AuthClient::new(channel(&server_addr).await);

    let root_cct = auth
        .authenticate(AuthenticateRequest {
            name: "root".into(),
            password: ROOT_PASSWORD.into(),
        })
        .await
        .expect("root authenticate")
        .into_inner()
        .cct;

    // ──── 2. 授予 `agent-bootstrap` 最小能力集（否则 agent 的 RoleCache 同步恒被拒）────
    let bootstrap_role = run_cli(&[
        "security",
        "bootstrap-role",
        "--token",
        &root_cct,
        "--addr",
        &server_addr,
    ]);
    assert!(
        bootstrap_role.status.success(),
        "bootstrap-role must succeed: {}",
        String::from_utf8_lossy(&bootstrap_role.stderr)
    );

    // ──── 3. 建应用角色/账户（**必须在 agent 启动前**创建：agent 启动时做一次全量同步）────
    auth.role_add(with_token(
        RoleAddRequest {
            name: "app-reader".into(),
        },
        &root_cct,
    ))
    .await
    .expect("root may add role");
    auth.role_grant_capability(with_token(
        RoleGrantCapabilityRequest {
            role: "app-reader".into(),
            capability_id: "data:kv:read".into(),
            // 非空 scope：这样 agent 侧的 scope 校验**必须真的执行**
            // （若无约束，fail-open 与 fail-closed 都无法区分）。
            scope: "/app/".into(),
        },
        &root_cct,
    ))
    .await
    .expect("root may grant capability");
    auth.user_add(with_token(
        UserAddRequest {
            name: "app".into(),
            password: "app-pw-123456".into(),
        },
        &root_cct,
    ))
    .await
    .expect("root may add user");
    auth.user_grant_role(with_token(
        UserGrantRoleRequest {
            user: "app".into(),
            role: "app-reader".into(),
        },
        &root_cct,
    ))
    .await
    .expect("root may grant role");

    // ──── 4. 真实 agent 进程：**auth.enabled = true** ────
    let agent_port = find_free_port();
    let agent_http_port = find_free_port();
    let agent_cfg = agent_dir.join("agent.toml");
    std::fs::write(
        &agent_cfg,
        format!(
            r#"agent_addr = "127.0.0.1:{agent_port}"
http_addr = "127.0.0.1:{agent_http_port}"
data_dir = "{agent_data}"
discovery_mode = "static"
static_peers = ["{server_addr}"]

[auth]
enabled = true
verifying_key_hex = "{vk}"
bootstrap_token = "{BOOTSTRAP_TOKEN}"
"#,
            agent_data = agent_dir.display(),
            vk = agent_verifying_key_hex(),
        ),
    )
    .unwrap();

    let agent = AgentProc::spawn(&agent_cfg);
    agent.wait_listening(Duration::from_secs(45)).await;
    let agent_addr = agent.addr();

    let app_cct = auth
        .authenticate(AuthenticateRequest {
            name: "app".into(),
            password: "app-pw-123456".into(),
        })
        .await
        .expect("app authenticate")
        .into_inner()
        .cct;

    // ──── 5. 经 agent 的 scope 内 KV 读 → 放行 ────
    //
    // 这一条同时要求 A1（agent 转发 CCT，否则 server 报 missing CCT token）、
    // A2（agent scope 提取器在生产路径注册且 fail-closed 不会误杀合法请求）、
    // A3（能力表命中 `/coord.kv.KV/Range`，否则 agent 报 unknown RPC method）。
    let mut kv_via_agent = KvClient::new(channel(&agent_addr).await);
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut last_err: Option<tonic::Status> = None;
    loop {
        match kv_via_agent
            .range(with_token(range_req(b"/app/order-1"), &app_cct))
            .await
        {
            Ok(resp) => {
                // 无此 key 也应返回 Ok（空结果），不是权限错误。
                let _ = resp.into_inner();
                break;
            }
            Err(status) => {
                let retriable = matches!(
                    status.code(),
                    tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
                ) || status.message().contains("unknown RPC method");
                last_err = Some(status);
                assert!(
                    retriable && Instant::now() < deadline,
                    "in-scope read through agent must be allowed, got: {:?}",
                    last_err
                );
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // ──── 6. 经 agent 的 scope 外 KV 读 → 拒绝（agent 本地 scope 校验真的在跑）────
    let denied = kv_via_agent
        .range(with_token(range_req(b"/other/secret"), &app_cct))
        .await;
    let status = denied.expect_err("out-of-scope read must be denied at the agent");
    assert!(
        matches!(
            status.code(),
            tonic::Code::PermissionDenied | tonic::Code::Unauthenticated
        ),
        "out-of-scope read must be denied with PERMISSION_DENIED, got: {status:?}"
    );

    // ──── 7. 无凭据经 agent → 拒绝 ────
    let no_cred = kv_via_agent
        .range(Request::new(range_req(b"/app/order-1")))
        .await;
    let status = no_cred.expect_err("request without credential must be denied");
    assert!(
        matches!(
            status.code(),
            tonic::Code::PermissionDenied | tonic::Code::Unauthenticated
        ),
        "missing credential must be denied, got: {status:?}"
    );

    // ──── 8. F-50：agent **自发**流量在鉴权下必须成功 ────
    //
    // 上面 5–7 覆盖的是"**替调用方**发请求"的路径；这一节覆盖"**agent 为自己**
    // 发请求"的路径 —— 它此前在结构上没有任何凭据通道，于是锁自动续期 / registry
    // 目录加载与订阅 / idgen nodeid 注册**全线** `missing CCT token`
    // （`jepsen/docs/coord-findings.md` F-50，`confirmed-by-run`）。
    //
    // 判据**两半缺一不可**：
    //   ① **正面证据**：idgen 雪花 nodeid 的注册键 `/_idgen/nodes/{id}` 真的落到了
    //      server KV —— 这条键只能由 agent 的后台任务写入，调用方路径不会碰它；
    //   ② **反面证据**：只断言①会漏掉"恰好注册成功但订阅仍失败"，
    //      故同时要求 agent 日志里**不再出现** `missing CCT token`。
    let mut kv_as_root = KvClient::new(channel(&server_addr).await);
    let nodes_prefix = b"/_idgen/nodes/".to_vec();
    let range_end = prefix_successor(&nodes_prefix);
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut registered = false;
    while Instant::now() < deadline {
        let resp = kv_as_root
            .range(with_token(
                RangeRequest {
                    key: nodes_prefix.clone(),
                    range_end: range_end.clone(),
                    limit: 0,
                    revision: 0,
                    keys_only: false,
                    count_only: false,
                },
                &root_cct,
            ))
            .await
            .expect("root may range the idgen node registry");
        if !resp.into_inner().kvs.is_empty() {
            registered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        registered,
        "idgen nodeid registration is agent-initiated (no caller) and must land in the \
         server KV under /_idgen/nodes/ — F-50 regression. Agent log tail:\n{}",
        tail(&agent.proc.log, 40)
    );

    // ② 反面证据：agent 日志**不得**出现任何"自发流量被拒"的痕迹。
    //
    // 三类一起查，缺一不可：
    // - `missing CCT token`：**服务端**拒绝无凭据出站调用时的措辞（F-50 的原症状）；
    // - `scope restriction` / `do not have capability`：凭据在但**能力不足**时的措辞。
    //   只查第一类会漏掉"凭据装上了、但能力集不全"的半修复态（症状更隐蔽：
    //   错误从"未认证"变成"无权"，仍然静默失效）；
    // - 下面 4–9 是**逐表面**命名，只为可诊断性（负向对照实测到的表面清单）。
    //
    // 措辞说明：第 7 步的匿名调用在 agent 本地就被拒（agent 自己的措辞是
    // `missing or invalid Authorization header`），不会产生上述任何字符串。
    let agent_log = std::fs::read_to_string(&agent.proc.log).unwrap_or_default();
    for marker in [
        "missing CCT token",
        "scope restriction",
        "do not have capability",
        "PKI CA auto-init failed",
        "KvWorkflowStore init failed",
        "failed to subscribe Watch",
        "failed to load initial catalog",
        "failed to register node_id",
        "sweep persisted DEKs failed",
    ] {
        let offending: Vec<&str> = agent_log.lines().filter(|l| l.contains(marker)).collect();
        assert!(
            offending.is_empty(),
            "agent-initiated traffic must fully succeed after F-50; {marker:?} still \
             appears {} time(s):\n{}",
            offending.len(),
            offending.join("\n")
        );
    }

    // ──── 9. P0-4 / D6：协议协商**真的可达**且**真的给出可诊断答复** ────
    //
    // 三条一起断言（缺一就会回到"空头承诺"）：
    //   ① `Handshake.Negotiate` 在**生产装配路径**注册成功（否则 UNIMPLEMENTED）；
    //   ② 用**低权限** CCT（`app-reader`，只有 `data:kv:read@/app/`）也能问到结果
    //      —— 这是"协商必须白名单化"的机器判据：它不能要求调用方先持有新协议
    //      的能力（那是死循环），也不能要求调用方先"已经是新客户端"；
    //   ③ 返回值**不含** v1 —— 一次性改名（D2）没有双服务期，老客户端必须能据此
    //      产出 `PROTOCOL_MISMATCH`，而不是被静默当作可用。
    let mut handshake = HandshakeClient::new(channel(&agent_addr).await);
    let resp = handshake
        .negotiate(with_token(
            HandshakeRequest {
                client_version: "coord-agent-api-v1".into(),
            },
            &app_cct,
        ))
        .await
        .expect(
            "Handshake.Negotiate must be reachable through the agent's production wiring \
             (and allowlisted: a low-privilege CCT with no handshake capability must still \
             be able to ask)",
        )
        .into_inner();
    assert!(
        !resp
            .supported_versions
            .iter()
            .any(|v| v == "coord-agent-api-v1"),
        "the agent must not advertise the retired v1 protocol (one-shot rename, D2); \
         got {:?}",
        resp.supported_versions
    );
    assert!(
        resp.supported_versions
            .iter()
            .any(|v| v == "coord-agent-api-v2"),
        "the agent must advertise the version the SDK speaks (v2); got {:?}",
        resp.supported_versions
    );
}

/// 前缀区间上界（末字节 +1）；全 `0xFF` 时返回空（= 到无穷）。
///
/// 与仓库既有的 `prefix_end()` 惯例一致（见 `services/workflow_store.rs`）。
fn prefix_successor(prefix: &[u8]) -> Vec<u8> {
    let mut out = prefix.to_vec();
    while let Some(last) = out.pop() {
        if last < 0xFF {
            out.push(last + 1);
            return out;
        }
    }
    Vec::new()
}
