// 批次 12 追加：CLI 凭据文件（登录落盘 → 自动携带 → 到期自动续期 → 登出）
//
// 与 `plugin_auth_process_test.rs` 同属鉴权/插件进程套件，独立成文件便于定位。
//
// 标记 `#[ignore]`：重型进程套件，显式运行：
//   cargo test -p coord --test plugin_credentials_process_test -- --ignored --nocapture

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// root 密码（经 `COORD_ROOT_PASSWORD` 注入子进程）。
const ROOT_PASSWORD: &str = "plugin-credentials-root-pw-123";

fn find_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

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

struct ServerProc {
    grpc_port: u16,
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
        Self { grpc_port, child }
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

fn read_creds(path: &PathBuf) -> serde_json::Value {
    let raw = std::fs::read_to_string(path).expect("credentials file must exist");
    serde_json::from_str(&raw).expect("credentials file must be valid JSON")
}

/// **漂移守卫**（普通单测，无需进程）：coord-agent 里复刻的 provisioner 能力清单
/// 必须与 server 侧的引导能力常量**逐字一致** —— agent 不依赖 coord-server，
/// 故只能在此跨 crate 断言。
#[test]
fn agent_provisioner_grants_match_server_bootstrap_grants() {
    let agent: Vec<(&str, &str)> =
        coord_agent::plugin::identity::PROVISIONER_CAPABILITY_GRANTS.to_vec();
    let server: Vec<(&str, &str)> = coord_server::auth::AGENT_BOOTSTRAP_CAPABILITY_GRANTS.to_vec();
    assert_eq!(
        agent, server,
        "coord-agent 的 PROVISIONER_CAPABILITY_GRANTS 与 server 侧引导能力清单漂移"
    );
}

/// **漂移守卫（F-50）**：agent 自身身份的能力清单、键空间清单与角色名必须与
/// server 侧**逐字一致** —— 否则会出现"agent 申请了 A、服务端按 B 授权"的静默失效
/// （F-50 的形态：唯一症状是 `missing CCT token` / 能力不足）。
#[test]
fn agent_self_grants_match_server_self_grants() {
    let agent: Vec<(String, String)> = coord_agent::plugin::identity::self_capability_grants();
    let server: Vec<(String, String)> = coord_server::auth::agent_self_capability_grants();
    assert_eq!(
        agent, server,
        "coord-agent 的 self_capability_grants() 与 server 侧 agent-self 能力清单漂移"
    );
    assert_eq!(
        coord_agent::plugin::identity::SELF_KEYSPACES.to_vec(),
        coord_server::auth::AGENT_SELF_KEYSPACES.to_vec(),
        "内部键空间清单漂移"
    );
    assert_eq!(
        coord_agent::plugin::identity::SELF_ROLE,
        coord_server::auth::AGENT_SELF_ROLE,
        "agent 自身身份的角色名与 server 侧常量漂移"
    );
}

/// **边界守卫（F-50）**：自身身份**只**覆盖内部键空间，且**不含**任何 `admin:*`。
///
/// 这是"agent 不得用自身身份读写调用方数据 / 改账户"这一安全边界的机器判据。
#[test]
fn agent_self_grants_stay_internal_and_non_admin() {
    for (id, scope) in coord_server::auth::agent_self_capability_grants() {
        assert!(
            !id.starts_with("admin:"),
            "{id} must not grant administrative access to the agent's own identity"
        );
        if id.starts_with("data:kv:") || id.starts_with("data:txn:") {
            assert!(
                scope.starts_with("/_") && scope.ends_with("/*"),
                "{id} must be scoped to the internal keyspace (/_<domain>/*), got {scope:?}"
            );
        }
    }
}

/// **覆盖守卫（F-50 的真正卡口）**：源码里出现的**每一个** `/_<domain>/` 键空间
/// 都必须被 `AGENT_SELF_KEYSPACES` 覆盖。
///
/// 为什么需要它：F-50 的失败形态是"漏授一处 ⇒ 静默失效"，而手写枚举**必然**会漏。
/// 负向对照（关掉本修复后重跑 `agent_auth_process_test`）实测到的自发流量面就比
/// 结论文档原先列出的四处更宽 —— 还有 pki CA 自举 / transit DEK 清扫 / config 订阅 /
/// workflow 存储初始化。本测试把"新增内部键空间"与"补授权"绑成一步：
/// 只做前者，这里就红。
///
/// 与仓库既有的源码扫描型卡口（`scripts/check-error-code-contract.sh`、
/// `scripts/check-panics.sh`）同族：把"人记得做"变成"机器拦得住"。
#[test]
fn every_internal_keyspace_in_source_is_granted() {
    use std::collections::BTreeSet;

    // `CARGO_MANIFEST_DIR` = <repo>/coord
    let repo_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("coord crate has a parent directory")
        .to_path_buf();

    let mut found: BTreeSet<String> = BTreeSet::new();
    for dir in ["coord-agent/src", "coord-core/src", "coord-proto/src"] {
        collect_keyspaces(&repo_root.join(dir), &mut found);
    }

    assert!(
        !found.is_empty(),
        "source scan found no internal keyspace at all — the scan itself is broken"
    );

    let granted: BTreeSet<&str> = coord_server::auth::AGENT_SELF_KEYSPACES
        .iter()
        .copied()
        .collect();
    let missing: Vec<&String> = found
        .iter()
        .filter(|ns| !granted.contains(ns.as_str()))
        .collect();
    assert!(
        missing.is_empty(),
        "源码使用了未被 AGENT_SELF_KEYSPACES 覆盖的内部键空间 {missing:?}；\
         agent 自身身份会在运行期以 `permission denied` 静默失效（F-50 同型）。\
         请把它们加入 `AGENT_SELF_KEYSPACES`（server 与 agent 两侧同名常量）。"
    );
}

/// 递归扫描 `*.rs`，抽取形如 `/_<domain>/` 的键空间前缀。
fn collect_keyspaces(dir: &std::path::Path, out: &mut std::collections::BTreeSet<String>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_keyspaces(&path, out);
            continue;
        }
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        for (idx, _) in text.match_indices("\"/_") {
            let rest = &text[idx + 3..];
            let Some(slash) = rest.find('/') else {
                continue;
            };
            let ns = &rest[..slash];
            if !ns.is_empty()
                && ns.chars().all(|c| c.is_ascii_lowercase() || c == '_')
                && !ns.starts_with('_')
            {
                out.insert(ns.to_string());
            }
        }
    }
}

/// CLI 凭据文件全链路（批次 12）：
/// ① `auth login` 落盘（权限 0600）；
/// ② 仅凭据文件（无 `--token`）→ 管理命令成功；
/// ③ 模拟 CCT 过期（保留 refresh token）→ **下一条命令自动续期**并回写（token 轮换）；
/// ④ refresh token 不可用 → **fail-closed**（不静默使用过期凭据）；
/// ⑤ `auth logout` 幂等 + 登出后 fail-closed；`auth credential-status` 只读本地。
#[ignore = "real-process auth/plugin suite; run explicitly with --ignored"]
#[tokio::test(flavor = "multi_thread")]
async fn cli_credentials_file_auto_refreshes_before_expiry() {
    let (grpc_port, raft_port) = find_ports();
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();
    let addr = format!("127.0.0.1:{grpc_port}");
    let creds = data_dir.join("cli").join("credentials.json");
    let creds_arg = creds.to_str().unwrap().to_string();

    let server = ServerProc::spawn(
        &data_dir,
        grpc_port,
        raft_port,
        "[security]\nauth_enabled = true\n",
    );
    server.wait_ready(Duration::from_secs(60)).await;

    // ① 登录 → 凭据落盘
    let login = run_cli(&[
        "auth",
        "login",
        "root",
        "--password",
        ROOT_PASSWORD,
        "--credentials",
        &creds_arg,
        "--addr",
        &addr,
    ]);
    assert!(
        login.status.success(),
        "login must succeed: {} / {}",
        stdout_of(&login),
        stderr_of(&login)
    );
    assert!(creds.exists(), "login must persist a credentials file");
    assert!(
        stderr_of(&login).contains("Credentials saved to"),
        "login must report the credentials path on stderr: {}",
        stderr_of(&login)
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(&creds).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "credentials file must be 0600, got {mode:o}");
    }
    let stored = read_creds(&creds);
    assert_eq!(stored["addr"], addr, "addr must be recorded");
    assert_eq!(stored["user"], "root");
    assert!(
        !stored["refresh_token"].as_str().unwrap_or("").is_empty(),
        "refresh token must be recorded for auto-renewal"
    );

    // ② 无 --token：仅凭据文件即可执行管理命令
    let listed = run_cli(&[
        "auth",
        "role",
        "list",
        "--credentials",
        &creds_arg,
        "--addr",
        &addr,
    ]);
    assert!(
        listed.status.success(),
        "stored credential must authenticate admin commands: {} / {}",
        stdout_of(&listed),
        stderr_of(&listed)
    );

    // ③ 模拟 CCT 过期（保留 refresh token）→ 自动续期
    let mut expired = stored.clone();
    expired["expires_at"] = serde_json::json!(1);
    std::fs::write(&creds, serde_json::to_vec_pretty(&expired).unwrap()).unwrap();
    let old_refresh = expired["refresh_token"].as_str().unwrap().to_string();

    let renewed = run_cli(&[
        "auth",
        "role",
        "list",
        "--credentials",
        &creds_arg,
        "--addr",
        &addr,
    ]);
    assert!(
        renewed.status.success(),
        "expired stored credential must be auto-refreshed: {} / {}",
        stdout_of(&renewed),
        stderr_of(&renewed)
    );
    assert!(
        stderr_of(&renewed).contains("refreshed stored credential"),
        "auto-refresh must be reported on stderr: {}",
        stderr_of(&renewed)
    );
    let after = read_creds(&creds);
    assert!(
        after["expires_at"].as_i64().unwrap_or(0) > 1,
        "refreshed expiry must be written back: {after}"
    );
    assert_ne!(
        after["refresh_token"].as_str().unwrap_or(""),
        old_refresh,
        "refresh token must rotate (server single-use semantics) and be persisted"
    );

    // ④ refresh token 不可用 → fail-closed（明确报错而非静默失败）
    let mut bogus = after.clone();
    bogus["expires_at"] = serde_json::json!(1);
    bogus["refresh_token"] = serde_json::json!("bogus-refresh-token");
    std::fs::write(&creds, serde_json::to_vec_pretty(&bogus).unwrap()).unwrap();
    let failed = run_cli(&[
        "auth",
        "role",
        "list",
        "--credentials",
        &creds_arg,
        "--addr",
        &addr,
    ]);
    assert!(
        !failed.status.success(),
        "unusable refresh token must fail closed: {}",
        stdout_of(&failed)
    );
    assert!(
        stderr_of(&failed).contains("RefreshToken"),
        "error must name the failing step: {}",
        stderr_of(&failed)
    );

    // ⑤ 登出幂等 + 登出后 fail-closed + 本地状态只读
    let out = run_cli(&["auth", "logout", "--credentials", &creds_arg]);
    assert!(
        out.status.success() && !creds.exists(),
        "logout must remove the file: {} / {}",
        stdout_of(&out),
        stderr_of(&out)
    );
    let again = run_cli(&["auth", "logout", "--credentials", &creds_arg]);
    assert!(
        again.status.success() && stdout_of(&again).contains("No stored credentials"),
        "logout must be idempotent: {}",
        stdout_of(&again)
    );
    let no_cred = run_cli(&[
        "auth",
        "role",
        "list",
        "--credentials",
        &creds_arg,
        "--addr",
        &addr,
    ]);
    assert!(
        !no_cred.status.success(),
        "after logout admin commands must fail closed"
    );
    let status = run_cli(&["auth", "credential-status", "--credentials", &creds_arg]);
    assert!(
        status.status.success() && stdout_of(&status).contains("none"),
        "credential-status must report the missing file: {} / {}",
        stdout_of(&status),
        stderr_of(&status)
    );
}
