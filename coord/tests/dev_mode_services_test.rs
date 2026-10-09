// dev 模式内建服务装配（端到端，见 ADR-0009）
//
// `coord dev` 的 agent 必须注册全套内建服务并真实挂载 gRPC 面：registry /
// config_center / lock 等协调能力在本地开发中直接可用——只注册 handshake
// 的退化形态必红。
//
// 判据：
//   1. `coord.plugin.Plugin/List` 清单与 dev 预设**集合相等**
//      （16 内建服务 + 无条件注册的 handshake；replication 除外）；
//   2. 清单内每项 builtin=true / runtime=native / status=started / healthy=true；
//   3. Registry.Register+Discover、Config.Put+Get、Lock.Acquire+Release、
//      Transit.Encrypt+Decrypt 各一次真实调用成功——不是「登记进注册表就算数」。

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};

/// dev 预设期望注册的内建插件（含无条件注册的 handshake）。
const EXPECTED_BUILTIN: &[&str] = &[
    "handshake",
    "registry",
    "config_center",
    "lock",
    "idgen",
    "leader_election",
    "event_notification",
    "cache",
    "mq",
    // 服务字段名是 `workflow`，插件清单名（`BaseService::name()`）为
    // "workflow-engine"（真实注册名，逐字）。
    "workflow-engine",
    "policy",
    "scheduler",
    "circuit_breaker",
    "rate_limiter",
    "feature_flags",
    // transit 由 dev 专用默认 KEK 支撑（builder 开关；见 ADR-0009）。
    "transit",
    "pki",
];

/// dev 预设的例外集：不得出现在清单里（ADR-0009）。
const EXCLUDED_BUILTIN: &[&str] = &["replication"];

fn bindable(port: u16) -> bool {
    std::net::TcpListener::bind(("127.0.0.1", port)).is_ok()
}

fn find_free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// 选一对 (grpc_port, agent_port)，保证 dev 模式派生的全部端口当前空闲：
/// grpc、grpc+1（Raft）、grpc+10（BFF/UI）、agent、agent+1（Agent HTTP）。
fn pick_dev_ports() -> (u16, u16) {
    for _ in 0..50 {
        let grpc = find_free_port();
        let agent = find_free_port();
        let ports = [grpc, grpc + 1, grpc + 10, agent, agent + 1];
        let distinct: BTreeSet<u16> = ports.iter().copied().collect();
        if distinct.len() == ports.len() && ports.iter().all(|&p| bindable(p)) {
            return (grpc, agent);
        }
    }
    panic!("no free port set found for dev mode");
}

/// dev 子进程句柄：Drop 时 kill，防止用例失败泄漏进程；
/// 数据/日志目录随句柄保活。
struct DevInstance {
    child: Child,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    _data_dir: tempfile::TempDir,
    _log_dir: tempfile::TempDir,
}

impl DevInstance {
    fn logs(&self) -> String {
        format!(
            "--- stdout ---\n{}\n--- stderr ---\n{}",
            std::fs::read_to_string(&self.stdout_path).unwrap_or_default(),
            std::fs::read_to_string(&self.stderr_path).unwrap_or_default()
        )
    }
}

impl Drop for DevInstance {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn_dev(grpc_port: u16, agent_port: u16) -> DevInstance {
    let data_dir = tempfile::tempdir().unwrap();
    let log_dir = tempfile::tempdir().unwrap();
    let stdout_path = log_dir.path().join("stdout.log");
    let stderr_path = log_dir.path().join("stderr.log");
    let stdout = std::fs::File::create(&stdout_path).unwrap();
    let stderr = std::fs::File::create(&stderr_path).unwrap();

    let child = Command::new(env!("CARGO_BIN_EXE_coord"))
        .arg("dev")
        .arg("--bind-addr")
        .arg("127.0.0.1")
        .arg("--grpc-port")
        .arg(grpc_port.to_string())
        .arg("--agent-port")
        .arg(agent_port.to_string())
        .arg("--data-dir")
        .arg(data_dir.path())
        .arg("--fresh")
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .expect("spawn coord dev");

    DevInstance {
        child,
        stdout_path,
        stderr_path,
        _data_dir: data_dir,
        _log_dir: log_dir,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn dev_mode_agent_serves_full_builtin_service_set() {
    let (grpc_port, agent_port) = pick_dev_ports();
    let agent_addr = format!("127.0.0.1:{agent_port}");
    let mut dev = spawn_dev(grpc_port, agent_port);

    // 端口就绪（dev 启动含 Raft 选举，给 30s 上限）+ 早退诊断。
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if tokio::net::TcpStream::connect(&agent_addr).await.is_ok() {
            break;
        }
        if let Ok(Some(status)) = dev.child.try_wait() {
            panic!("coord dev exited early ({status}); logs:\n{}", dev.logs());
        }
        if tokio::time::Instant::now() > deadline {
            panic!(
                "agent gRPC {agent_addr} not ready within 30s; logs:\n{}",
                dev.logs()
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{agent_addr}"))
        .unwrap()
        .connect_timeout(std::time::Duration::from_secs(5))
        .connect()
        .await
        .expect("connect to dev agent");

    // 1) 插件清单：与 dev 预设集合相等（漏注册 / 误注册都红）。
    let mut plugin = coord_proto::plugin::plugin_client::PluginClient::new(channel.clone());
    let plugins = plugin
        .list(coord_proto::plugin::ListPluginsRequest {})
        .await
        .expect("Plugin.List must be served by the dev agent")
        .into_inner()
        .plugins;
    let names: BTreeSet<&str> = plugins.iter().map(|p| p.name.as_str()).collect();
    let expected: BTreeSet<&str> = EXPECTED_BUILTIN.iter().copied().collect();
    assert_eq!(
        names,
        expected,
        "dev builtin plugin inventory must equal the dev preset (ADR-0009); logs:\n{}",
        dev.logs()
    );
    for name in EXCLUDED_BUILTIN {
        assert!(
            !names.contains(name),
            "{name} is excluded from the dev preset and must not surface (ADR-0009)"
        );
    }
    for p in &plugins {
        assert_eq!(p.runtime, "native", "{} must be a native adapter", p.name);
        assert!(p.builtin, "{} must be a builtin plugin", p.name);
        assert_eq!(p.status, "started", "{} must be started", p.name);
        assert!(p.healthy, "{} must report live health", p.name);
    }

    // 2) 协调能力真实调用：Registry / Config / Lock（「可用」的直接判据）。
    let mut registry =
        coord_proto::registry::v1::registry_client::RegistryClient::new(channel.clone());
    let registered = registry
        .register(coord_proto::registry::v1::RegisterRequest {
            service_name: "dev-mode-e2e".into(),
            instance_id: "instance-1".into(),
            metadata: "{}".into(),
            ttl_seconds: 30,
        })
        .await
        .expect("Registry.Register must succeed in dev mode")
        .into_inner();
    assert!(registered.lease_id > 0, "Register must allocate a lease");
    let discovered = registry
        .discover(coord_proto::registry::v1::DiscoverRequest {
            service_name: "dev-mode-e2e".into(),
            filter_mode: coord_proto::registry::v1::FilterMode::Exact as i32,
        })
        .await
        .expect("Registry.Discover must succeed in dev mode")
        .into_inner();
    assert_eq!(
        discovered.instances.len(),
        1,
        "registered instance must be discoverable"
    );

    let mut config = coord_proto::config::v1::config_client::ConfigClient::new(channel.clone());
    config
        .put(coord_proto::config::v1::ConfigPutRequest {
            key: "dev-mode-e2e/key".into(),
            value: "v1".into(),
        })
        .await
        .expect("Config.Put must succeed in dev mode");
    let got = config
        .get(coord_proto::config::v1::ConfigGetRequest {
            key: "dev-mode-e2e/key".into(),
        })
        .await
        .expect("Config.Get must succeed in dev mode")
        .into_inner();
    assert!(got.found && got.value == "v1", "config roundtrip: {got:?}");

    let mut lock = coord_proto::lock::v1::lock_client::LockClient::new(channel.clone());
    let acquired = lock
        .acquire(coord_proto::lock::v1::LockAcquireRequest {
            name: "dev-mode-e2e/lock".into(),
            holder_id: "holder-1".into(),
            ttl_seconds: 30,
        })
        .await
        .expect("Lock.Acquire must succeed in dev mode")
        .into_inner();
    assert!(acquired.acquired, "lock must be acquired: {acquired:?}");
    let released = lock
        .release(coord_proto::lock::v1::LockReleaseRequest {
            name: "dev-mode-e2e/lock".into(),
            holder_id: "holder-1".into(),
            lease_id: acquired.lease_id,
        })
        .await
        .expect("Lock.Release must succeed in dev mode")
        .into_inner();
    assert!(released.released, "lock must be released");

    // transit：dev 默认 KEK 必须真的可用（缺材料回退仅限 dev；见 ADR-0009）。
    let mut transit = coord_proto::transit::v1::transit_client::TransitClient::new(channel.clone());
    let plaintext = b"dev-mode-e2e-secret";
    let ciphertext = transit
        .encrypt(coord_proto::transit::v1::TransitEncryptRequest {
            plaintext: plaintext.to_vec(),
            context: vec![],
        })
        .await
        .expect("Transit.Encrypt must succeed in dev mode (dev-only default KEK)")
        .into_inner()
        .ciphertext;
    assert!(!ciphertext.is_empty(), "ciphertext must not be empty");
    let decrypted = transit
        .decrypt(coord_proto::transit::v1::TransitDecryptRequest {
            ciphertext,
            context: vec![],
        })
        .await
        .expect("Transit.Decrypt must succeed in dev mode")
        .into_inner()
        .plaintext;
    assert_eq!(
        decrypted, plaintext,
        "transit roundtrip must preserve the plaintext"
    );
}
