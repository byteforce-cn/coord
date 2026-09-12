// 进程级验收：插件账户（capability + scope 授权）在真实 `coord server` 上的
// 端到端链路与 **raft 重启存续**（计划 §11 P0c 遗留项 + §13 鉴权集群冒烟）。
//
// 与 `coord-server` 内的 in-process 单测不同，本套件 spawn 真实 `coord server`
// 进程，走完整链路：
//   1. root 认证 → 建角色 → `RoleGrantCapability(role, capability_id, scope)`
//      → 建用户 → `UserGrantRole` → 插件账户 `Authenticate` 取得受限 CCT；
//   2. 受限 CCT 在 scope 内可写 KV，scope 外被 server 拒绝（PERMISSION_DENIED）；
//   3. **kill + 重启**同一数据目录后，授权（角色/能力/scope）与账户仍然存续
//      （AuthOp 经 raft 持久化 + 启动重放）；
//   4. `Bootstrap` 引导令牌 → 短期 `agent-bootstrap` CCT → 在 operator 显式授予
//      后自助开通插件账户；该 CCT **无数据面能力**，直接写 KV 必须被拒。
//
// 标记 `#[ignore]`：重型进程套件，显式运行：
//   cargo test -p coord --test plugin_auth_process_test -- --ignored --nocapture

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::Request;

use coord_proto::auth::auth_client::AuthClient;
use coord_proto::auth::{
    AuthenticateRequest, BootstrapRequest, RoleAddRequest, RoleGrantCapabilityRequest,
    RoleListRequest, UserAddRequest, UserGrantRoleRequest,
};
use coord_proto::kv::kv_client::KvClient;
use coord_proto::kv::{PutRequest, RangeRequest};

/// root 密码（经 `COORD_ROOT_PASSWORD` 注入子进程）。
const ROOT_PASSWORD: &str = "plugin-process-root-pw-123";
/// 一次性 agent 引导令牌（config `[security].agent_bootstrap_tokens`；≥16 字符）。
const BOOTSTRAP_TOKEN: &str = "plugin-process-bootstrap-token-9f3c";

/// 找一个空闲端口（bind:0 后释放）。
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
        // HTTP 端口需可用（server 自行绑定 grpc+10）
        if std::net::TcpListener::bind(("127.0.0.1", http)).is_err() {
            continue;
        }
        return (grpc, raft);
    }
    panic!("could not find a free (grpc, raft) port pair");
}

/// server 进程句柄（drop 即杀，避免测试失败时泄漏常驻进程）。
struct ServerProc {
    grpc_port: u16,
    raft_port: u16,
    data_dir: PathBuf,
    cfg_path: PathBuf,
    child: Child,
}

impl Drop for ServerProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl ServerProc {
    /// 首次启动（`--bootstrap`）。
    fn spawn(data_dir: &Path, grpc_port: u16, raft_port: u16, config_toml: &str) -> Self {
        Self::start(data_dir, grpc_port, raft_port, config_toml, true)
    }

    fn start(
        data_dir: &Path,
        grpc_port: u16,
        raft_port: u16,
        config_toml: &str,
        bootstrap: bool,
    ) -> Self {
        let bin = env!("CARGO_BIN_EXE_coord");
        std::fs::create_dir_all(data_dir).expect("create data dir");
        let cfg_path = data_dir.join("server.toml");
        std::fs::write(&cfg_path, config_toml).expect("write server config");
        let log_file = std::fs::File::create(data_dir.join("server.log")).expect("create log");

        let mut cmd = Command::new(bin);
        cmd.arg("server")
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
            .env("COORD_ROOT_PASSWORD", ROOT_PASSWORD)
            .env("RUST_LOG", "coord=warn")
            .stdout(Stdio::from(log_file.try_clone().unwrap()))
            .stderr(Stdio::from(log_file));
        if bootstrap {
            cmd.arg("--bootstrap");
        }
        let child = cmd.spawn().expect("spawn coord server");
        Self {
            grpc_port,
            raft_port,
            data_dir: data_dir.to_path_buf(),
            cfg_path,
            child,
        }
    }

    /// 重启（同一数据目录 / 端口 / 配置；不再 `--bootstrap`）。
    fn restart(&mut self) {
        let bin = env!("CARGO_BIN_EXE_coord");
        let log_file = std::fs::File::create(self.data_dir.join("server.log")).expect("create log");
        let child = Command::new(bin)
            .arg("server")
            .arg("--id")
            .arg("1")
            .arg("--addr")
            .arg(format!("127.0.0.1:{}", self.grpc_port))
            .arg("--raft-addr")
            .arg(format!("127.0.0.1:{}", self.raft_port))
            .arg("--data-dir")
            .arg(&self.data_dir)
            .arg("--config")
            .arg(&self.cfg_path)
            .env("COORD_ROOT_PASSWORD", ROOT_PASSWORD)
            .env("RUST_LOG", "coord=warn")
            .stdout(Stdio::from(log_file.try_clone().unwrap()))
            .stderr(Stdio::from(log_file))
            .spawn()
            .expect("respawn coord server");
        self.child = child;
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
                "server did not become ready within {timeout:?}"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
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

/// 打开插件账户的受限 CCT（`plugin/{id}` + 随机密码）。
async fn provision_plugin_account(
    auth: &mut AuthClient<Channel>,
    root_cct: &str,
    plugin: &str,
    capabilities: &[(&str, &str)],
) -> String {
    let role = format!("plugin/{plugin}-role");
    let user = format!("plugin/{plugin}");
    let password = format!("{plugin}-pw-123456");

    auth.role_add(with_token(RoleAddRequest { name: role.clone() }, root_cct))
        .await
        .expect("root may add role");

    for (id, scope) in capabilities {
        auth.role_grant_capability(with_token(
            RoleGrantCapabilityRequest {
                role: role.clone(),
                capability_id: (*id).to_string(),
                scope: (*scope).to_string(),
            },
            root_cct,
        ))
        .await
        .unwrap_or_else(|e| panic!("root may grant capability {id}: {e}"));
    }

    auth.user_add(with_token(
        UserAddRequest {
            name: user.clone(),
            password: password.clone(),
        },
        root_cct,
    ))
    .await
    .expect("root may add user");

    auth.user_grant_role(with_token(
        UserGrantRoleRequest {
            user: user.clone(),
            role: role.clone(),
        },
        root_cct,
    ))
    .await
    .expect("root may grant role");

    auth.authenticate(AuthenticateRequest {
        name: user,
        password,
    })
    .await
    .expect("plugin account may authenticate")
    .into_inner()
    .cct
}

async fn put(kv: &mut KvClient<Channel>, token: &str, key: &[u8], value: &[u8]) -> tonic::Status {
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

/// 主场景：插件账户的 capability + scope 授权在真实 server 上生效，
/// 且 **kill + 重启**后存续（P0c 的进程级验收）。
#[ignore = "real-process auth/plugin suite; run explicitly with --ignored"]
#[tokio::test(flavor = "multi_thread")]
async fn plugin_capability_scope_survives_raft_restart() {
    let (grpc_port, raft_port) = find_ports();
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();
    let addr = format!("127.0.0.1:{grpc_port}");

    let mut server = ServerProc::spawn(
        &data_dir,
        grpc_port,
        raft_port,
        // 单节点：不开放 agent 自助注册
        "[security]\nauth_enabled = true\n",
    );
    server.wait_ready(Duration::from_secs(60)).await;

    let mut auth = AuthClient::new(channel(&addr).await);
    let mut kv = KvClient::new(channel(&addr).await);

    let root_cct = auth
        .authenticate(AuthenticateRequest {
            name: "root".into(),
            password: ROOT_PASSWORD.into(),
        })
        .await
        .expect("root authenticate")
        .into_inner()
        .cct;

    let plugin_cct = provision_plugin_account(
        &mut auth,
        &root_cct,
        "counter",
        &[
            ("data:kv:read", "/app/counter/"),
            ("data:kv:write", "/app/counter/"),
        ],
    )
    .await;

    // scope 内允许
    assert_eq!(
        put(&mut kv, &plugin_cct, b"/app/counter/a", b"1")
            .await
            .code(),
        tonic::Code::Ok,
        "in-scope write must be allowed"
    );
    // scope 外拒绝
    let denied = put(&mut kv, &plugin_cct, b"/outside/x", b"1").await;
    assert_eq!(
        denied.code(),
        tonic::Code::PermissionDenied,
        "out-of-scope write must be denied: {denied}"
    );

    // ──── kill + 重启（同数据目录）────
    server.restart();
    server.wait_ready(Duration::from_secs(60)).await;

    let mut auth = AuthClient::new(channel(&addr).await);
    let mut kv = KvClient::new(channel(&addr).await);

    // 账户与密码存续 → 重新认证成功
    let plugin_cct = auth
        .authenticate(AuthenticateRequest {
            name: "plugin/counter".into(),
            password: "counter-pw-123456".into(),
        })
        .await
        .expect("plugin account must survive restart")
        .into_inner()
        .cct;

    // 授权（角色 + capability + scope）存续
    assert_eq!(
        put(&mut kv, &plugin_cct, b"/app/counter/b", b"2")
            .await
            .code(),
        tonic::Code::Ok,
        "in-scope write must still be allowed after restart"
    );
    let denied_after = put(&mut kv, &plugin_cct, b"/outside/y", b"2").await;
    assert_eq!(
        denied_after.code(),
        tonic::Code::PermissionDenied,
        "scope restriction must still hold after restart: {denied_after}"
    );

    // 首次写入的值真的落盘（range 需 read 能力；plugin 账户 scope 内可读）
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
            &plugin_cct,
        ))
        .await
        .expect("in-scope read must be allowed after restart");
    assert_eq!(
        range.into_inner().kvs.len(),
        2,
        "both in-scope writes must be visible"
    );
}

/// 引导链路：`Bootstrap` → 短期 `agent-bootstrap` CCT → （operator 显式授予后）
/// 自助开通插件账户；该 CCT 无数据面能力，直接写 KV 必须被拒（最小权限）。
#[ignore = "real-process auth/plugin suite; run explicitly with --ignored"]
#[tokio::test(flavor = "multi_thread")]
async fn agent_bootstrap_enrollment_provisions_plugin_accounts() {
    let (grpc_port, raft_port) = find_ports();
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();
    let addr = format!("127.0.0.1:{grpc_port}");

    let server = ServerProc::spawn(
        &data_dir,
        grpc_port,
        raft_port,
        &format!(
            "[security]\nauth_enabled = true\nagent_bootstrap_tokens = [\"{BOOTSTRAP_TOKEN}\"]\n"
        ),
    );
    server.wait_ready(Duration::from_secs(60)).await;

    let mut auth = AuthClient::new(channel(&addr).await);
    let mut kv = KvClient::new(channel(&addr).await);

    let root_cct = auth
        .authenticate(AuthenticateRequest {
            name: "root".into(),
            password: ROOT_PASSWORD.into(),
        })
        .await
        .expect("root authenticate")
        .into_inner()
        .cct;

    // operator 显式授予 agent-bootstrap 角色最小能力（server 不预置）。
    auth.role_add(with_token(
        RoleAddRequest {
            name: coord_server::auth::AGENT_BOOTSTRAP_ROLE.to_string(),
        },
        &root_cct,
    ))
    .await
    .expect("root may add agent-bootstrap role");
    for (id, scope) in coord_server::auth::AGENT_BOOTSTRAP_CAPABILITY_GRANTS {
        auth.role_grant_capability(with_token(
            RoleGrantCapabilityRequest {
                role: coord_server::auth::AGENT_BOOTSTRAP_ROLE.to_string(),
                capability_id: (*id).to_string(),
                scope: (*scope).to_string(),
            },
            &root_cct,
        ))
        .await
        .unwrap_or_else(|e| panic!("grant {id}: {e}"));
    }

    // 一次性引导令牌 → 短期 agent-bootstrap CCT
    let bootstrap_cct = auth
        .bootstrap(BootstrapRequest {
            bootstrap_token: BOOTSTRAP_TOKEN.to_string(),
        })
        .await
        .expect("bootstrap token must be accepted")
        .into_inner()
        .cct;
    assert!(!bootstrap_cct.is_empty());

    // 引导 CCT 具备 admin:auth:* → 可自助开通插件账户（等价 agent 侧 ensure）
    let plugin_cct =
        provision_plugin_account(&mut auth, &bootstrap_cct, "echo", &[("data:kv:write", "")]).await;
    assert!(!plugin_cct.is_empty());

    // 最小权限：引导 CCT 本身没有任何数据面能力 → KV 写必须被拒
    let denied = put(&mut kv, &bootstrap_cct, b"/app/echo/a", b"1").await;
    assert_eq!(
        denied.code(),
        tonic::Code::PermissionDenied,
        "agent-bootstrap CCT must not carry data-plane capabilities: {denied}"
    );

    // 一次性：令牌已被消费，二次引导必须失败
    assert!(
        auth.bootstrap(BootstrapRequest {
            bootstrap_token: BOOTSTRAP_TOKEN.to_string(),
        })
        .await
        .is_err(),
        "bootstrap token must be single-use"
    );
}

// ──── CLI 面验收（批次 5）：凭据注入 + 一键引导 ────

/// 运行 `coord` CLI 子进程（非交互；捕获 stdout/stderr；静默日志便于取 token）。
///
/// **隔离凭据环境**：批次 12 起 CLI 默认读/写凭据文件（`auth login` 落盘）。
/// 不隔离就会读写开发者真实的 `~/.config/coord/credentials.json`，
/// 让「无凭据必须 fail-closed」类断言受宿主机残留文件影响。
fn run_cli(args: &[&str]) -> std::process::Output {
    let bin = env!("CARGO_BIN_EXE_coord");
    Command::new(bin)
        .args(args)
        .env("RUST_LOG", "coord=error")
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

fn stdout_of(out: &std::process::Output) -> String {
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn stderr_of(out: &std::process::Output) -> String {
    String::from_utf8_lossy(&out.stderr).trim().to_string()
}

/// 鉴权开启时 CLI 管理命令的行为：
/// ① 未携带凭据 → fail-closed（服务端 `missing CCT token`）；
/// ② `auth login --password … --token-only` 取管理员 CCT（非交互）；
/// ③ `security bootstrap-role --token <cct>` 一键授予引导最小能力集（幂等）；
/// ④ `auth role grant-capability` / `revoke-capability` 单条授予可用。
#[ignore = "real-process auth/plugin suite; run explicitly with --ignored"]
#[tokio::test(flavor = "multi_thread")]
async fn cli_admin_commands_inject_token_on_auth_enabled_server() {
    let (grpc_port, raft_port) = find_ports();
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();
    let addr = format!("127.0.0.1:{grpc_port}");

    let server = ServerProc::spawn(
        &data_dir,
        grpc_port,
        raft_port,
        "[security]\nauth_enabled = true\n",
    );
    server.wait_ready(Duration::from_secs(60)).await;

    // ① 无凭据：管理命令必须 fail-closed
    let no_token = run_cli(&["auth", "role", "list", "--addr", &addr]);
    assert!(
        !no_token.status.success(),
        "admin command without token must fail closed: {}",
        stdout_of(&no_token)
    );
    assert!(
        stderr_of(&no_token).contains("CCT"),
        "error should mention missing CCT: {}",
        stderr_of(&no_token)
    );

    // ② 非交互登录取管理员 CCT
    //
    // `--no-save`：本用例专门验证「未显式带凭据 → fail-closed」。批次 12 起
    // `auth login` 默认把凭据落盘（后续命令自动携带 + 到期自动续期），若在此
    // 落盘，下面的 ③ 就会**合理地**用上隐式凭据而不再 fail-closed；隐式凭据
    // 行为由 ②′ 单独验证。
    let login = run_cli(&[
        "auth",
        "login",
        "root",
        "--password",
        ROOT_PASSWORD,
        "--token-only",
        "--no-save",
        "--addr",
        &addr,
    ]);
    assert!(
        login.status.success(),
        "non-interactive login must succeed: {}",
        stderr_of(&login)
    );
    let admin_cct = stdout_of(&login);
    assert!(
        !admin_cct.is_empty() && !admin_cct.contains(' '),
        "login --token-only must print a bare CCT, got: {admin_cct:?}"
    );

    // ②′ 默认登录落盘的凭据会被后续管理命令隐式采用（批次 12 契约）。
    let saving_login = run_cli(&[
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
        saving_login.status.success(),
        "saving login must succeed: {}",
        stderr_of(&saving_login)
    );
    let credential_file = isolated_config_home().join("coord/credentials.json");
    assert!(
        credential_file.exists(),
        "auth login must persist a credentials file by default ({})",
        credential_file.display()
    );
    let implicit = run_cli(&["auth", "role", "list", "--addr", &addr]);
    assert!(
        implicit.status.success(),
        "stored credentials must be used implicitly by admin commands: {}",
        stderr_of(&implicit)
    );
    // 清掉隐式凭据，让 ③ 回到「无凭据 ⇒ fail-closed」场景。
    std::fs::remove_file(&credential_file).expect("remove saved credentials");

    // ③ 一键引导：未带 token 失败；带 token 成功且幂等
    let bare = run_cli(&["security", "bootstrap-role", "--addr", &addr]);
    assert!(
        !bare.status.success(),
        "bootstrap-role without token must fail closed: {}",
        stdout_of(&bare)
    );
    for attempt in 0..2 {
        let out = run_cli(&[
            "security",
            "bootstrap-role",
            "--token",
            &admin_cct,
            "--addr",
            &addr,
        ]);
        assert!(
            out.status.success(),
            "bootstrap-role attempt {attempt} must succeed: {}",
            stderr_of(&out)
        );
        assert!(
            stdout_of(&out).contains("granted admin:auth:user_add"),
            "bootstrap-role must report granted capabilities: {}",
            stdout_of(&out)
        );
    }

    // 服务端确认：agent-bootstrap 角色已具备最小能力集
    let mut auth = AuthClient::new(channel(&addr).await);
    let roles = auth
        .role_list(with_token(RoleListRequest {}, &admin_cct))
        .await
        .expect("root CCT may list roles")
        .into_inner()
        .roles;
    let bootstrap_role = roles
        .iter()
        .find(|r| r.name == coord_server::auth::AGENT_BOOTSTRAP_ROLE)
        .expect("agent-bootstrap role must exist after bootstrap-role");
    for (id, scope) in coord_server::auth::AGENT_BOOTSTRAP_CAPABILITY_GRANTS {
        assert!(
            bootstrap_role
                .capability_grants
                .iter()
                .any(|g| g.capability_id == *id && g.scope == *scope),
            "capability {id} (scope {scope:?}) must be granted"
        );
    }

    // ④ 单条能力授予 / 撤销 CLI
    assert!(
        run_cli(&[
            "auth",
            "role",
            "add",
            "cli-test-role",
            "--token",
            &admin_cct,
            "--addr",
            &addr,
        ])
        .status
        .success(),
        "role add with token must succeed"
    );
    assert!(
        run_cli(&[
            "auth",
            "role",
            "grant-capability",
            "cli-test-role",
            "data:kv:read",
            "--scope",
            "/cli/",
            "--token",
            &admin_cct,
            "--addr",
            &addr,
        ])
        .status
        .success(),
        "grant-capability with token must succeed"
    );
    assert!(
        run_cli(&[
            "auth",
            "role",
            "revoke-capability",
            "cli-test-role",
            "data:kv:read",
            "--scope",
            "/cli/",
            "--token",
            &admin_cct,
            "--addr",
            &addr,
        ])
        .status
        .success(),
        "revoke-capability with token must succeed"
    );

    // ⑤ CLI 凭据续期（批次 9）：login --print-refresh → auth refresh 换新 CCT；
    //    refresh token 单次使用（旧值二次使用必须失败）。
    let login_rt = run_cli(&[
        "auth",
        "login",
        "root",
        "--password",
        ROOT_PASSWORD,
        "--token-only",
        "--print-refresh",
        "--addr",
        &addr,
    ]);
    assert!(
        login_rt.status.success(),
        "login --print-refresh must succeed: {}",
        stderr_of(&login_rt)
    );
    let lines: Vec<String> = stdout_of(&login_rt)
        .lines()
        .map(|l| l.trim().to_string())
        .collect();
    assert_eq!(
        lines.len(),
        2,
        "--token-only --print-refresh must print CCT then refresh token: {lines:?}"
    );
    let refresh_token = lines[1].clone();
    assert!(
        !refresh_token.is_empty() && !refresh_token.contains(' '),
        "refresh token must be a bare token, got {refresh_token:?}"
    );

    let refreshed = run_cli(&[
        "auth",
        "refresh",
        "--refresh-token",
        &refresh_token,
        "--token-only",
        "--addr",
        &addr,
    ]);
    assert!(
        refreshed.status.success(),
        "auth refresh must succeed: {}",
        stderr_of(&refreshed)
    );
    let new_cct = stdout_of(&refreshed);
    assert!(
        !new_cct.is_empty() && new_cct != lines[0],
        "refresh must yield a new CCT"
    );
    // 新 CCT 可用于管理命令
    assert!(
        run_cli(&["auth", "role", "list", "--token", &new_cct, "--addr", &addr,])
            .status
            .success(),
        "refreshed CCT must be usable for admin commands"
    );
    // 单次使用：旧 refresh token 二次使用必须失败
    let reused = run_cli(&[
        "auth",
        "refresh",
        "--refresh-token",
        &refresh_token,
        "--token-only",
        "--addr",
        &addr,
    ]);
    assert!(
        !reused.status.success(),
        "refresh token must be single-use: {}",
        stdout_of(&reused)
    );
}

// ──── 动态 bootstrap 令牌（批次 9）：TTL + 一次性 + raft 重启存续 ────

/// 从 `security bootstrap-token create` 的人类可读输出中取出 `id`。
fn extract_field(stdout: &str, key: &str) -> String {
    stdout
        .lines()
        .find_map(|l| {
            let l = l.trim();
            l.strip_prefix(key)
                .map(|v| v.trim().trim_start_matches(':').trim().to_string())
        })
        .unwrap_or_else(|| panic!("field {key:?} not found in output:\n{stdout}"))
}

/// 动态 bootstrap 令牌全链路（真实 `coord` 二进制）：
/// ① CLI 签发（明文仅一次）→ 列表可见（unused）；
/// ② `Auth.Bootstrap` 消费成功，二次消费被拒（一次性）；
/// ③ 列表标记 consumed；CLI 撤销后令牌不可用（幂等：二次撤销 revoked=false）；
/// ④ **kill + 同数据目录重启** 后，重启前签发的令牌仍可用
///    （证明令牌经 raft 日志持久化 + 启动重放，而非内存态）。
#[ignore = "real-process auth/plugin suite; run explicitly with --ignored"]
#[tokio::test(flavor = "multi_thread")]
async fn dynamic_bootstrap_token_cli_one_time_and_restart_persistence() {
    let (grpc_port, raft_port) = find_ports();
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();
    let addr = format!("127.0.0.1:{grpc_port}");

    let mut server = ServerProc::spawn(
        &data_dir,
        grpc_port,
        raft_port,
        "[security]\nauth_enabled = true\n",
    );
    server.wait_ready(Duration::from_secs(60)).await;

    // 管理员 CCT（CLI 非交互登录）
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
        stderr_of(&login)
    );
    let admin_cct = stdout_of(&login);

    let mut auth = AuthClient::new(channel(&addr).await);

    // ① CLI 签发明文令牌（--token-only）
    let created = run_cli(&[
        "security",
        "bootstrap-token",
        "create",
        "--label",
        "site-a",
        "--ttl-secs",
        "600",
        "--token-only",
        "--token",
        &admin_cct,
        "--addr",
        &addr,
    ]);
    assert!(
        created.status.success(),
        "bootstrap-token create must succeed: {}",
        stderr_of(&created)
    );
    let token = stdout_of(&created);
    assert!(
        token.starts_with("cbt_") && !token.contains(char::is_whitespace),
        "--token-only must print a bare dynamic token, got {token:?}"
    );

    // 列表：unused
    let listed = run_cli(&[
        "security",
        "bootstrap-token",
        "list",
        "--token",
        &admin_cct,
        "--addr",
        &addr,
    ]);
    assert!(listed.status.success(), "list must succeed");
    let listing = stdout_of(&listed);
    assert!(
        listing.contains("site-a") && listing.contains("unused"),
        "listing must show the token as unused:\n{listing}"
    );

    // ② 一次性引导
    let first = auth
        .bootstrap(BootstrapRequest {
            bootstrap_token: token.clone(),
        })
        .await
        .expect("dynamic token must be accepted");
    assert!(!first.into_inner().cct.is_empty());
    let second = auth
        .bootstrap(BootstrapRequest {
            bootstrap_token: token.clone(),
        })
        .await;
    assert_eq!(
        second.unwrap_err().code(),
        tonic::Code::PermissionDenied,
        "dynamic token must be single-use"
    );

    // ③ 列表标记 consumed
    let listed = run_cli(&[
        "security",
        "bootstrap-token",
        "list",
        "--token",
        &admin_cct,
        "--addr",
        &addr,
    ]);
    assert!(
        stdout_of(&listed).contains("consumed"),
        "listing must mark the token consumed:\n{}",
        stdout_of(&listed)
    );

    // ④ 撤销：签发第二枚（从人类可读输出解析 id）→ revoke → 引导必须失败
    let created2 = run_cli(&[
        "security",
        "bootstrap-token",
        "create",
        "--label",
        "to-revoke",
        "--ttl-secs",
        "600",
        "--token",
        &admin_cct,
        "--addr",
        &addr,
    ]);
    assert!(
        created2.status.success(),
        "second create must succeed: {}",
        stderr_of(&created2)
    );
    let out2 = stdout_of(&created2);
    let id2 = extract_field(&out2, "id");
    let token2 = extract_field(&out2, "token");

    let revoked = run_cli(&[
        "security",
        "bootstrap-token",
        "revoke",
        "--id",
        &id2,
        "--token",
        &admin_cct,
        "--addr",
        &addr,
    ]);
    assert!(
        revoked.status.success() && stdout_of(&revoked).contains("Revoked"),
        "revoke must succeed: {} / {}",
        stdout_of(&revoked),
        stderr_of(&revoked)
    );
    assert!(
        auth.bootstrap(BootstrapRequest {
            bootstrap_token: token2,
        })
        .await
        .is_err(),
        "revoked token must not be accepted"
    );
    // 幂等：再次撤销 → revoked=false（CLI 报 not found）
    let again = run_cli(&[
        "security",
        "bootstrap-token",
        "revoke",
        "--id",
        &id2,
        "--token",
        &admin_cct,
        "--addr",
        &addr,
    ]);
    assert!(
        again.status.success() && stdout_of(&again).contains("not found"),
        "second revoke must be idempotent: {}",
        stdout_of(&again)
    );

    // ⑤ 重启存续：重启前签发 → kill + 同数据目录重启 → 令牌仍可用
    let created3 = run_cli(&[
        "security",
        "bootstrap-token",
        "create",
        "--label",
        "survives-restart",
        "--ttl-secs",
        "1800",
        "--token-only",
        "--token",
        &admin_cct,
        "--addr",
        &addr,
    ]);
    assert!(
        created3.status.success(),
        "third create must succeed: {}",
        stderr_of(&created3)
    );
    let token3 = stdout_of(&created3);

    server.restart();
    server.wait_ready(Duration::from_secs(60)).await;

    let mut auth_after = AuthClient::new(channel(&addr).await);
    let listed_after = run_cli(&[
        "security",
        "bootstrap-token",
        "list",
        "--token",
        &admin_cct,
        "--addr",
        &addr,
    ]);
    assert!(
        listed_after.status.success() && stdout_of(&listed_after).contains("survives-restart"),
        "token must survive raft restart:\n{} / {}",
        stdout_of(&listed_after),
        stderr_of(&listed_after)
    );
    let boot = auth_after
        .bootstrap(BootstrapRequest {
            bootstrap_token: token3,
        })
        .await
        .expect("persisted dynamic token must still be accepted after restart");
    assert!(!boot.into_inner().cct.is_empty());
}
