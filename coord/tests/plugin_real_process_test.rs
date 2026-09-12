// 插件引擎**进程级**端到端验收（Phase 5 遗留项：`PLUGIN_REAL=1`）。
//
// 与前两类插件测试的区别：
// - `coord-agent/tests/agent_plugin_js_test.rs`：server + agent 同进程（in-process）；
// - `coord/tests/plugin_auth_process_test.rs`：真实 `coord server` 进程，但插件链路仅到
//   服务端鉴权面（无 agent 进程）；
// - **本套件**：真实 `coord server` 进程 + 真实 `coord agent` 进程 +
//   磁盘上的 JS 插件，走完整链路：
//
//   root CCT ──▶ `security bootstrap-role`（agent-bootstrap 最小能力集）
//             ──▶ agent 进程启动时用一次性 bootstrap token 换引导 CCT
//             ──▶ 插件加载时开通 `plugin/{id}` 服务账户（能力 + scope 授予）
//             ──▶ 插件经 `coord.kv.put` 写 server（server 侧 capability/scope 强制）
//             ──▶ 客户端经 `coord.plugin.Plugin/Invoke` 观测结果
//
// 覆盖：
// ① `List` 报告 runtime=js / status=started（真实进程加载）；
// ② `Invoke` → 插件写 KV → 经 server 读回（插件身份 + 服务端授权全链路）；
// ③ scope 越界写被拒（声明 scope `/app/counter/` 之外）；
// ④ **SIGHUP 内容级热重载**：不改 version 就地替换插件文件 → 行为变更
//   （`content_fingerprint` 生效，批次 9 特性）；
// ⑤ **agent 进程 kill + 重启**（一次性令牌已消费）→ 插件仍以持久化账户认证、
//   KV 写入仍成功（重启鲁棒性）；
// ⑥ **运行中新增插件**（批次 12）：重启后的 agent 已无可用引导 CCT（令牌一次性），
//   SIGHUP 加入的第二个插件仍需**新建** `plugin/counter2` 账户 —— 靠自举的
//   provisioner 会话（可自动续期）完成，证明账户开通能力不再受
//   「引导 CCT 10 分钟 + 令牌一次性」窗口限制。
//
// 标记 `#[ignore]` + `PLUGIN_REAL=1` 门控（对齐 MULTI_RAFT_REAL / CHAOS_REAL 惯例）：
//   PLUGIN_REAL=1 cargo test -p coord --test plugin_real_process_test -- --ignored --nocapture

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::Request;

use coord_proto::auth::auth_client::AuthClient;
use coord_proto::auth::UserListRequest;
use coord_proto::kv::kv_client::KvClient;
use coord_proto::kv::RangeRequest;
use coord_proto::plugin::plugin_client::PluginClient;
use coord_proto::plugin::{InvokeRequest, ListPluginsRequest};

/// root 密码（经 `COORD_ROOT_PASSWORD` 注入 server 子进程）。
const ROOT_PASSWORD: &str = "plugin-real-root-pw-789";
/// 一次性 agent 引导令牌（config `[security].agent_bootstrap_tokens`；≥16 字符）。
const BOOTSTRAP_TOKEN: &str = "plugin-real-bootstrap-token-71ab";
/// 固定 auth 根密钥（hex 32 字节）——让测试可推导 agent 侧验证公钥。
const ROOT_KEY_HEX: &str = "0b1c2d3e4f5061728394a5b6c7d8e9f0a1b2c3d4e5f60718293a4b5c6d7e8f90";

/// 插件源码模板（`__MARK__` 由测试替换；`MARK` 用于观测 SIGHUP 内容级热重载）。
const PLUGIN_JS_TEMPLATE: &str = r#"
const MARK = "__MARK__";

export async function handleInvoke(method, payload) {
  if (method === "mark") {
    return coord.util.encode(MARK);
  }
  if (method === "put") {
    const r = await coord.kv.put("/app/counter/a", payload);
    return coord.util.encode("rev-" + String(r.revision));
  }
  if (method === "get") {
    const r = await coord.kv.range("/app/counter/a");
    return r.kvs.length ? r.kvs[0].value : coord.util.encode("");
  }
  if (method === "outside") {
    await coord.kv.put("/outside/x", "1");
    return coord.util.encode("ALLOWED");
  }
  throw new Error("unknown method " + method);
}
"#;

/// 插件源码（`MARK` 用于观测 SIGHUP 内容级热重载）。
fn plugin_source(mark: &str) -> String {
    PLUGIN_JS_TEMPLATE.replace("__MARK__", mark)
}

// ──── 端口 ────

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
        if http == raft || http == grpc {
            continue;
        }
        if std::net::TcpListener::bind(("127.0.0.1", http)).is_err() {
            continue;
        }
        return (grpc, raft);
    }
    panic!("could not find a free (grpc, raft) port pair");
}

// ──── server 进程 ────

struct ServerProc {
    grpc_port: u16,
    raft_port: u16,
    child: Child,
}

impl Drop for ServerProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl ServerProc {
    fn spawn(data_dir: &Path, grpc_port: u16, raft_port: u16, config_toml: &str) -> Self {
        let bin = env!("CARGO_BIN_EXE_coord");
        std::fs::create_dir_all(data_dir).expect("create data dir");
        let cfg_path = data_dir.join("server.toml");
        std::fs::write(&cfg_path, config_toml).expect("write server config");
        let log_file = std::fs::File::create(data_dir.join("server.log")).expect("create log");

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
            raft_port,
            child,
        }
    }

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
                "server did not become ready within {timeout:?} (grpc={}, raft={})",
                self.grpc_port,
                self.raft_port
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

// ──── agent 进程 ────

struct AgentProc {
    agent_port: u16,
    cfg_path: PathBuf,
    child: Child,
}

impl Drop for AgentProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl AgentProc {
    fn spawn(cfg_path: &Path) -> Self {
        let (child, agent_port) = spawn_agent(cfg_path);
        Self {
            agent_port,
            cfg_path: cfg_path.to_path_buf(),
            child,
        }
    }

    fn pid(&self) -> u32 {
        self.child.id()
    }

    /// 发送 SIGHUP（触发配置重载 + 插件集 diff）。
    fn sighup(&self) {
        let status = Command::new("kill")
            .arg("-HUP")
            .arg(self.pid().to_string())
            .status()
            .expect("send SIGHUP");
        assert!(status.success(), "kill -HUP must succeed");
    }

    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    fn restart(&mut self) {
        self.kill();
        let (child, agent_port) = spawn_agent(&self.cfg_path);
        self.child = child;
        self.agent_port = agent_port;
    }
}

/// 启动 `coord agent` 子进程；返回 (句柄, agent gRPC 端口)。
fn spawn_agent(cfg_path: &Path) -> (Child, u16) {
    let bin = env!("CARGO_BIN_EXE_coord");
    let log_file =
        std::fs::File::create(cfg_path.with_extension("agent.log")).expect("create agent log");
    let child = Command::new(bin)
        .arg("agent")
        .arg("--agent-config")
        .arg(cfg_path)
        .env("RUST_LOG", "coord=info")
        .stdout(Stdio::from(log_file.try_clone().unwrap()))
        .stderr(Stdio::from(log_file))
        .spawn()
        .expect("spawn coord agent");
    // 端口从配置文件解析（测试自建，格式已知）
    let text = std::fs::read_to_string(cfg_path).expect("read agent config");
    let agent_port = text
        .lines()
        .find_map(|l| {
            let l = l.trim();
            l.strip_prefix("agent_addr")
                .and_then(|v| v.split(':').next_back())
                .map(|p| p.trim().trim_matches('"').parse::<u16>().expect("port"))
        })
        .expect("agent_addr present in config");
    (child, agent_port)
}

// ──── 通用工具 ────

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

fn run_cli(args: &[&str]) -> std::process::Output {
    let bin = env!("CARGO_BIN_EXE_coord");
    Command::new(bin)
        .args(args)
        .env("RUST_LOG", "coord=error")
        // 隔离凭据文件（批次 12：CLI 默认读/写凭据文件；不得污染真实 HOME）
        .env("XDG_CONFIG_HOME", isolated_config_home())
        .env_remove("COORD_TOKEN")
        .env_remove("COORD_CREDENTIALS")
        .output()
        .expect("run coord CLI")
}

/// 每个测试进程独立的配置目录（凭据文件落在此处）。
fn isolated_config_home() -> PathBuf {
    static DIR: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    DIR.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("coord-cli-home-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        dir
    })
    .clone()
}

/// 读取插件返回的 payload 为 UTF-8 字符串。
fn payload_str(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).to_string()
}

/// 轮询 `Invoke(mark)` 直到返回期望标记（SIGHUP 重载是异步的）。
async fn wait_for_mark(
    plugin: &mut PluginClient<Channel>,
    plugin_id: &str,
    expected: &str,
    timeout: Duration,
) -> String {
    let deadline = Instant::now() + timeout;
    let mut last;
    loop {
        match plugin
            .invoke(InvokeRequest {
                plugin_id: plugin_id.to_string(),
                method: "mark".to_string(),
                payload: vec![],
            })
            .await
        {
            Ok(resp) => {
                last = payload_str(&resp.into_inner().payload);
                if last == expected {
                    return last;
                }
            }
            Err(e) => last = format!("<error: {e}>"),
        }
        assert!(
            Instant::now() < deadline,
            "plugin '{plugin_id}' did not report mark '{expected}' within {timeout:?} \
             (last: {last:?})"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// 全链路：真实 server + 真实 agent 进程 + JS 插件（PLUGIN_REAL=1）。
#[ignore = "real-process plugin suite; run explicitly: PLUGIN_REAL=1"]
#[tokio::test(flavor = "multi_thread")]
async fn plugin_real_agent_process_e2e() {
    // E1：拒绝把「未跑」伪装成「通过」——门控变量缺失即失败。
    assert!(
        std::env::var("PLUGIN_REAL").map(|v| !v.is_empty()).unwrap_or(false),
        "PLUGIN_REAL must be set to run this real-process suite (E1)"
    );

    let (grpc_port, raft_port) = find_ports();
    let tmp = tempfile::tempdir().unwrap();
    let server_dir = tmp.path().join("server");
    let agent_dir = tmp.path().join("agent");
    let plugins_dir = tmp.path().join("plugins");
    std::fs::create_dir_all(&plugins_dir).unwrap();
    std::fs::create_dir_all(&agent_dir).unwrap();
    let addr = format!("127.0.0.1:{grpc_port}");

    // 插件入口（初始 MARK=v1）
    let plugin_path = plugins_dir.join("counter.js");
    std::fs::write(&plugin_path, plugin_source("v1")).unwrap();

    // ──── 1. 真实 server（鉴权开启 + 一次性引导令牌）────
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

    // ──── 2. root 登录 + 一键授予 agent-bootstrap 最小能力集 ────
    let login = run_cli(&[
        "auth",
        "login",
        "root",
        "--password",
        ROOT_PASSWORD,
        "--token-only",
        "--addr",
        &addr,
    ]);
    assert!(
        login.status.success(),
        "root login must succeed: {}",
        String::from_utf8_lossy(&login.stderr)
    );
    let root_cct = String::from_utf8_lossy(&login.stdout).trim().to_string();

    let bootstrap_role = run_cli(&[
        "security",
        "bootstrap-role",
        "--token",
        &root_cct,
        "--addr",
        &addr,
    ]);
    assert!(
        bootstrap_role.status.success(),
        "bootstrap-role must succeed: {}",
        String::from_utf8_lossy(&bootstrap_role.stderr)
    );

    // ──── 3. 真实 agent 进程（[plugins] 指向临时插件目录）────
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
static_peers = ["127.0.0.1:{grpc_port}"]

[auth]
enabled = false
bootstrap_token = "{BOOTSTRAP_TOKEN}"

[plugins]
enabled = true
dir = "{plugins}"

[[plugins.entries]]
name = "counter"
version = "1.0.0"
runtime = "js"
trust = "first_party"
entry = "counter.js"
capabilities = [{{ id = "data:kv:read", scope = "/app/counter/" }}, {{ id = "data:kv:write", scope = "/app/counter/" }}]
"#,
            agent_data = agent_dir.display(),
            plugins = plugins_dir.display(),
        ),
    )
    .unwrap();

    let mut agent = AgentProc::spawn(&agent_cfg);
    let agent_addr = format!("127.0.0.1:{}", agent.agent_port);

    // 等待 agent gRPC 就绪
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        if tokio::net::TcpStream::connect(&agent_addr).await.is_ok() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "agent did not start listening on {agent_addr}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let mut plugin = PluginClient::new(channel(&agent_addr).await);

    // ──── 4. List：真实进程加载了 JS 插件 ────
    let list_deadline = Instant::now() + Duration::from_secs(45);
    loop {
        let listed = plugin
            .list(ListPluginsRequest {})
            .await
            .expect("List must succeed")
            .into_inner()
            .plugins;
        if let Some(p) = listed.iter().find(|p| p.name == "counter") {
            assert_eq!(p.runtime, "js");
            assert_eq!(p.status, "started", "plugin must be started: {p:?}");
            break;
        }
        assert!(
            Instant::now() < list_deadline,
            "plugin 'counter' did not appear in List within 45s: {listed:?}"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // ──── 5. Invoke → 插件写 KV → server 侧读回（插件身份 + 服务端授权）────
    let mark = wait_for_mark(&mut plugin, "counter", "v1", Duration::from_secs(30)).await;
    assert_eq!(mark, "v1");

    let put = plugin
        .invoke(InvokeRequest {
            plugin_id: "counter".into(),
            method: "put".into(),
            payload: b"plugin-real-42".to_vec(),
        })
        .await
        .expect("plugin KV write must succeed (plugin account + capability + scope)")
        .into_inner();
    assert!(
        String::from_utf8_lossy(&put.payload).starts_with("rev-"),
        "put must return a revision, got {:?}",
        payload_str(&put.payload)
    );

    let mut kv = KvClient::new(channel(&addr).await);
    let ranged = kv
        .range(with_token(
            RangeRequest {
                key: b"/app/counter/a".to_vec(),
                range_end: vec![],
                limit: 0,
                revision: 0,
                keys_only: false,
                count_only: false,
            },
            &root_cct,
        ))
        .await
        .expect("root may read the plugin-written key")
        .into_inner();
    assert_eq!(
        ranged.kvs.first().map(|kv| kv.value.clone()),
        Some(b"plugin-real-42".to_vec()),
        "plugin write must be visible on the server"
    );

    // ──── 6. scope 越界：插件声明 scope 之外必须被拒 ────
    let outside = plugin
        .invoke(InvokeRequest {
            plugin_id: "counter".into(),
            method: "outside".into(),
            payload: vec![],
        })
        .await;
    assert!(
        outside.is_err(),
        "out-of-scope write must be rejected (agent guard / server capability), got {outside:?}"
    );
    // 越界拒绝不得拖垮插件：后续调用仍可用
    assert_eq!(
        payload_str(
            &plugin
                .invoke(InvokeRequest {
                    plugin_id: "counter".into(),
                    method: "get".into(),
                    payload: vec![],
                })
                .await
                .expect("plugin must stay healthy after a denied call")
                .into_inner()
                .payload
        ),
        "plugin-real-42"
    );

    // ──── 7. SIGHUP 内容级热重载（同 version、内容变更）────
    std::fs::write(&plugin_path, plugin_source("v2")).unwrap();
    agent.sighup();
    let mark = wait_for_mark(&mut plugin, "counter", "v2", Duration::from_secs(30)).await;
    assert_eq!(mark, "v2", "SIGHUP must hot-reload changed plugin content");

    // ──── 8. agent 进程 kill + 重启（一次性引导令牌已消费）────
    let mut plugin = {
        agent.restart();
        let agent_addr = format!("127.0.0.1:{}", agent.agent_port);
        let deadline = Instant::now() + Duration::from_secs(45);
        loop {
            if tokio::net::TcpStream::connect(&agent_addr).await.is_ok() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "restarted agent did not listen on {agent_addr}"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        PluginClient::new(channel(&agent_addr).await)
    };

    let list_deadline = Instant::now() + Duration::from_secs(45);
    loop {
        let listed = plugin
            .list(ListPluginsRequest {})
            .await
            .expect("List must succeed after restart")
            .into_inner()
            .plugins;
        if listed
            .iter()
            .any(|p| p.name == "counter" && p.status == "started")
        {
            break;
        }
        assert!(
            Instant::now() < list_deadline,
            "plugin did not restart within 45s: {listed:?}"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // 重启后仍是 v2（文件未变）+ 插件账户用持久化密码认证（无需新引导令牌）
    let mark = wait_for_mark(&mut plugin, "counter", "v2", Duration::from_secs(30)).await;
    assert_eq!(mark, "v2");

    let put2 = plugin
        .invoke(InvokeRequest {
            plugin_id: "counter".into(),
            method: "put".into(),
            payload: b"after-restart".to_vec(),
        })
        .await
        .expect("plugin KV write must still succeed after agent restart (persisted account)")
        .into_inner();
    assert!(String::from_utf8_lossy(&put2.payload).starts_with("rev-"));

    let ranged = kv
        .range(with_token(
            RangeRequest {
                key: b"/app/counter/a".to_vec(),
                range_end: vec![],
                limit: 0,
                revision: 0,
                keys_only: false,
                count_only: false,
            },
            &root_cct,
        ))
        .await
        .expect("root may read after restart")
        .into_inner();
    assert_eq!(
        ranged.kvs.first().map(|kv| kv.value.clone()),
        Some(b"after-restart".to_vec()),
        "post-restart plugin write must be visible on the server"
    );

    // ──── 9. 运行中新增插件（引导 CCT 已不可用）→ provisioner 会话开通账户 ────
    //
    // 前置事实：一次性 bootstrap token 已在**第一次** agent 启动时被消费，重启后的
    // agent 拿不到引导 CCT（`bootstrap_provisioner(None, ..)` 分支）。插入一个全
    // 新插件后，它的 `plugin/counter2` 账户必须被**新建** —— 只能靠首启自举的
    // provisioner 会话（持久账户 + refresh/密码自动续期）完成。
    // 若没有该会话（旧行为），开通会 permission denied → 插件退回共享未鉴权
    // 客户端 → 下面的 KV 写在 server 侧被拒绝，用例即失败。
    let plugin2_path = plugins_dir.join("counter2.js");
    std::fs::write(&plugin2_path, plugin_source("p2")).unwrap();
    let cfg_before = std::fs::read_to_string(&agent_cfg).expect("read agent config");
    std::fs::write(
        &agent_cfg,
        format!(
            "{cfg_before}\n[[plugins.entries]]\nname = \"counter2\"\nversion = \"1.0.0\"\n\
             runtime = \"js\"\ntrust = \"first_party\"\nentry = \"counter2.js\"\n\
             capabilities = [{{ id = \"data:kv:read\", scope = \"/app/counter/\" }}, \
             {{ id = \"data:kv:write\", scope = \"/app/counter/\" }}]\n"
        ),
    )
    .unwrap();
    agent.sighup();

    let list_deadline = Instant::now() + Duration::from_secs(45);
    loop {
        let listed = plugin
            .list(ListPluginsRequest {})
            .await
            .expect("List must succeed after runtime add")
            .into_inner()
            .plugins;
        if listed
            .iter()
            .any(|p| p.name == "counter2" && p.status == "started")
        {
            break;
        }
        assert!(
            Instant::now() < list_deadline,
            "plugin 'counter2' did not start within 45s after SIGHUP: {listed:?}"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let mark2 = wait_for_mark(&mut plugin, "counter2", "p2", Duration::from_secs(30)).await;
    assert_eq!(mark2, "p2");

    // 服务端确实存在新开的账户（而非“插件跑在未鉴权客户端上”）
    let mut auth_cli = AuthClient::new(channel(&addr).await);
    let users = auth_cli
        .user_list(with_token(UserListRequest {}, &root_cct))
        .await
        .expect("root may list users")
        .into_inner()
        .users;
    assert!(
        users.iter().any(|u| u.name == "plugin/counter2"),
        "runtime-added plugin account must be provisioned on the server: {users:?}"
    );

    // 新插件用**自己的**服务账户写 KV（经 server capability + scope 强制）
    let put3 = plugin
        .invoke(InvokeRequest {
            plugin_id: "counter2".into(),
            method: "put".into(),
            payload: b"second-plugin".to_vec(),
        })
        .await
        .expect("runtime-added plugin must write with its own provisioned account")
        .into_inner();
    assert!(String::from_utf8_lossy(&put3.payload).starts_with("rev-"));
    let ranged = kv
        .range(with_token(
            RangeRequest {
                key: b"/app/counter/a".to_vec(),
                range_end: vec![],
                limit: 0,
                revision: 0,
                keys_only: false,
                count_only: false,
            },
            &root_cct,
        ))
        .await
        .expect("root may read the new plugin's write")
        .into_inner();
    assert_eq!(
        ranged.kvs.first().map(|kv| kv.value.clone()),
        Some(b"second-plugin".to_vec()),
        "runtime-added plugin write must be visible on the server"
    );

    // 清理（Drop 亦会兜底）
    agent.kill();
}
