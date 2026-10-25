//! #170 GA 证据：v2 集群（multi-raft + `[kv_t]`）上的内建服务冒烟。
//!
//! `coord dev` 的服务预设覆盖 16 个内建服务（ADR-0009），但 dev 是 v1 数据面。
//! v2 集群只服务 `coord.*.v2`、且 MQ v1 冻结（ADR-0015 D13 / B-KVT-6），所以在 v2
//! 上必须证明的是：**15 个不依赖 MQ 的内建服务能注册、能真实调用**，而不是
//! 「登记进注册表就算数」。
//!
//! 判据：
//!   1. `coord.plugin.Plugin/List` 集合与 v2 预设**集合相等**（15 服务 + 无条件
//!      注册的 handshake；mq/replication 不在其列）；
//!   2. 每项 `builtin=true` / `runtime=native` / `status=started` / `healthy=true`；
//!   3. 每个服务一次真实 RPC（不是骨架），其中 idgen 用 **segment 模式**：号段经
//!      `coord.txn.v2` 的 CAS 分配，ID 必须落在号段空间（雪花 ID 是 10^18 量级），
//!      否则说明它退回本地雪花、冒烟没有覆盖 v2 数据面。
//!
//! 覆盖矩阵与未覆盖项的单一事实源：`docs/production/ops/kv-t-ga-gate.md`。

use std::collections::BTreeSet;
use std::io::Write;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

#[path = "common/mod.rs"]
mod common;

use common::{find_port, wait_tcp_ready};

/// 夹具共用的 tokio runtime。
///
/// tonic 的 `Channel` 把连接驱动挂在**创建它的 runtime** 上：在临时 runtime 里
/// `connect()`、再到另一个 runtime 里发 RPC，driver 已随第一个 runtime 销毁，
/// 只会拿到 `transport error`。所以 server/agent 就绪等待与全部 RPC 必须共用
/// 同一个 runtime。
static RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();

fn runtime() -> &'static tokio::runtime::Runtime {
    RT.get_or_init(|| tokio::runtime::Runtime::new().expect("build fixture runtime"))
}

/// v2 预设期望注册的内建插件（15 服务 + 无条件注册的 handshake）。
const EXPECTED_BUILTIN: &[&str] = &[
    "handshake",
    "registry",
    "config_center",
    "lock",
    "idgen",
    "leader_election",
    "event_notification",
    "cache",
    // 服务字段名是 `workflow`，插件清单名（`BaseService::name()`）为
    // "workflow-engine"（真实注册名，逐字）。
    "workflow-engine",
    "policy",
    "scheduler",
    "circuit_breaker",
    "rate_limiter",
    "feature_flags",
    "transit",
    "pki",
];

/// v2 上按策略不可用的服务：必须**不在**清单里（fail-closed 由
/// `agent_workflow_durable_gate_test` 覆盖）。
const EXCLUDED_BUILTIN: &[&str] = &["mq", "replication"];

struct V2Cluster {
    server: Child,
    agent: Child,
    grpc_port: u16,
    agent_port: u16,
    dir: tempfile::TempDir,
}

impl V2Cluster {
    fn agent_addr(&self) -> String {
        format!("127.0.0.1:{}", self.agent_port)
    }

    fn server_addr(&self) -> String {
        format!("127.0.0.1:{}", self.grpc_port)
    }

    fn logs(&self) -> String {
        let read = |name: &str| {
            std::fs::read_to_string(self.dir.path().join(name))
                .unwrap_or_else(|e| format!("<{name} unavailable: {e}>"))
        };
        format!(
            "--- server.log ---\n{}\n--- agent.log ---\n{}",
            tail(&read("server.log"), 40),
            tail(&read("agent.log"), 60)
        )
    }
}

impl Drop for V2Cluster {
    fn drop(&mut self) {
        let _ = self.agent.kill();
        let _ = self.agent.wait();
        let _ = self.server.kill();
        let _ = self.server.wait();
    }
}

fn tail(text: &str, lines: usize) -> String {
    let all: Vec<&str> = text.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

fn write_file(path: &Path, body: &str) {
    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(body.as_bytes()).unwrap();
}

/// 起一套真实 v2 集群：单节点 multi-raft + `[kv_t]` + object storage，两个数据
/// Region 以 `m` 为界（跨区路径可达）；Agent 指向它并开满 15 个内建服务。
fn start_v2_cluster() -> V2Cluster {
    let _ = runtime();
    let dir = tempfile::tempdir().unwrap();
    let grpc_port = find_port();
    let raft_port = find_port();
    let http_port = find_port();
    let agent_port = find_port();
    let agent_http_port = find_port();

    let server_toml = format!(
        "[node]\nid = 1\n\
         [network]\ngrpc_addr = \"127.0.0.1:{grpc_port}\"\n\
         raft_addr = \"127.0.0.1:{raft_port}\"\n\
         http_addr = \"127.0.0.1:{http_port}\"\n\
         [security]\nauth_enabled = false\n\
         [cluster]\ncluster_name = \"kvt-g6-services-v2\"\nbootstrap = true\n\
         [[cluster.initial_nodes]]\nid = 1\ngrpc = \"127.0.0.1:{grpc_port}\"\n\
         raft = \"127.0.0.1:{raft_port}\"\n\
         [multi_raft]\nenabled = true\n\
         [[multi_raft.initial_regions]]\nid = 1\nstart_key = \"\"\nend_key = \"m\"\n\
         [[multi_raft.initial_regions]]\nid = 2\nstart_key = \"m\"\nend_key = \"\"\n\
         [multi_raft.pd]\nenabled = true\ntarget_replicas = 1\n\
         [object_storage]\nenabled = true\n\
         [kv_t]\nenabled = true\n"
    );
    write_file(&dir.path().join("server.toml"), &server_toml);

    // 除 mq / replication / workflow_durable 外全开：这 15 个服务是 v2 能力矩阵
    // （ADR-0015 D13）里承诺「随 Agent 迁移到 v2 后可用」的面。
    let agent_toml = format!(
        "agent_addr = \"127.0.0.1:{agent_port}\"\n\
         http_addr = \"127.0.0.1:{agent_http_port}\"\n\
         data_dir = \"{}\"\n\
         discovery_mode = \"static\"\n\
         static_peers = [\"127.0.0.1:{grpc_port}\"]\n\
         data_plane = \"v2\"\n\
         [auth]\nenabled = false\n\
         [services]\nidgen_mode = \"segment\"\n\
         registry = true\nconfig_center = true\nlock = true\nidgen = true\n\
         leader_election = true\nevent_notification = true\ncache = true\n\
         workflow = true\npolicy = true\nscheduler = true\ncircuit_breaker = true\n\
         rate_limiter = true\nfeature_flags = true\ntransit = true\npki = true\n",
        dir.path().join("agent").display()
    );
    write_file(&dir.path().join("agent.toml"), &agent_toml);

    let bin = env!("CARGO_BIN_EXE_coord");
    let server_log = std::fs::File::create(dir.path().join("server.log")).unwrap();
    let server_err = server_log.try_clone().unwrap();
    let server = Command::new(bin)
        .arg("server")
        .arg("--id")
        .arg("1")
        .arg("--addr")
        .arg(format!("127.0.0.1:{grpc_port}"))
        .arg("--raft-addr")
        .arg(format!("127.0.0.1:{raft_port}"))
        .arg("--data-dir")
        .arg(dir.path().join("server-data"))
        .arg("--config")
        .arg(dir.path().join("server.toml"))
        .arg("--bootstrap")
        .env("RUST_LOG", "coord=warn")
        .stdout(Stdio::from(server_log))
        .stderr(Stdio::from(server_err))
        .spawn()
        .expect("spawn coord server");

    let agent_log = std::fs::File::create(dir.path().join("agent.log")).unwrap();
    let agent_err = agent_log.try_clone().unwrap();
    let agent = Command::new(bin)
        .arg("agent")
        .arg("--agent-config")
        .arg(dir.path().join("agent.toml"))
        .arg("--data-dir")
        .arg(dir.path().join("agent-data"))
        // transit 在非 dev 装配下缺材料即拒绝启动（fail-closed）；冒烟给固定
        // 测试材料（hex64 = 32 字节）。
        .env("COORD_TRANSIT_KEK", "11".repeat(32))
        .env("RUST_LOG", "coord_agent=warn,coord=warn")
        .stdout(Stdio::from(agent_log))
        .stderr(Stdio::from(agent_err))
        .spawn()
        .expect("spawn coord agent");

    let cluster = V2Cluster {
        server,
        agent,
        grpc_port,
        agent_port,
        dir,
    };

    wait_server_ready(&cluster);
    cluster
}

/// 等 server 选出 Raft leader：`Maintenance.Status` 可达且 `raft_leader` 非空。
fn wait_server_ready(cluster: &V2Cluster) {
    let deadline = Instant::now() + Duration::from_secs(60);
    runtime().block_on(async {
        loop {
            if let Ok(ch) =
                tonic::transport::Channel::from_shared(format!("http://{}", cluster.server_addr()))
                    .unwrap()
                    .connect_timeout(Duration::from_secs(2))
                    .connect()
                    .await
            {
                let mut client =
                    coord_proto::maintenance::maintenance_client::MaintenanceClient::new(ch);
                if let Ok(resp) = client
                    .status(coord_proto::maintenance::StatusRequest {})
                    .await
                {
                    if !resp.into_inner().raft_leader.is_empty() {
                        return;
                    }
                }
            }
            assert!(
                Instant::now() < deadline,
                "v2 server never elected a leader within 60s; logs:\n{}",
                cluster.logs()
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });
}

fn connect_agent_blocking(addr: &str) -> tonic::transport::Channel {
    runtime().block_on(async {
        wait_tcp_ready(addr, Duration::from_secs(60)).await;
        tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await
            .expect("connect to v2 agent")
    })
}

/// `coord.plugin.Plugin/List` 直到 Agent 的 15 个服务全部 `started` + `healthy`，
/// 返回插件清单。
fn wait_services_started(
    channel: &tonic::transport::Channel,
    cluster: &V2Cluster,
) -> Vec<coord_proto::plugin::PluginInfo> {
    runtime().block_on(async {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let mut plugin = coord_proto::plugin::plugin_client::PluginClient::new(channel.clone());
            let plugins = match plugin
                .list(coord_proto::plugin::ListPluginsRequest {})
                .await
            {
                Ok(resp) => resp.into_inner().plugins,
                Err(e) => {
                    assert!(
                        Instant::now() < deadline,
                        "Plugin.List failed before services started: {e}; logs:\n{}",
                        cluster.logs()
                    );
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
            };
            let all_ready = plugins.len() >= EXPECTED_BUILTIN.len()
                && plugins.iter().all(|p| p.status == "started" && p.healthy);
            if all_ready {
                return plugins;
            }
            assert!(
                Instant::now() < deadline,
                "builtin services not all started+healthy within 60s: {:?}; logs:\n{}",
                plugins
                    .iter()
                    .map(|p| (p.name.as_str(), p.status.as_str(), p.healthy))
                    .collect::<Vec<_>>(),
                cluster.logs()
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
}

#[test]
fn test_v2_cluster_serves_all_builtin_services() {
    let cluster = start_v2_cluster();
    let channel = connect_agent_blocking(&cluster.agent_addr());

    // 1) 插件清单与 v2 预设集合相等：漏注册、误注册（mq 出现在 v2 上）都红。
    let plugins = wait_services_started(&channel, &cluster);
    let names: BTreeSet<&str> = plugins.iter().map(|p| p.name.as_str()).collect();
    let expected: BTreeSet<&str> = EXPECTED_BUILTIN.iter().copied().collect();
    assert_eq!(
        names,
        expected,
        "v2 builtin inventory must equal the v2 preset; logs:\n{}",
        cluster.logs()
    );
    for name in EXCLUDED_BUILTIN {
        assert!(
            !names.contains(name),
            "{name} must not be registered on a v2 data plane (ADR-0015 D13)"
        );
    }
    for p in &plugins {
        assert!(p.builtin, "{} must be a builtin plugin", p.name);
        assert_eq!(p.runtime, "native", "{} must be a native adapter", p.name);
        assert_eq!(p.status, "started", "{} must be started", p.name);
        assert!(p.healthy, "{} must report live health", p.name);
    }

    runtime().block_on(async {
        exercise_registry(channel.clone()).await;
        exercise_config(channel.clone()).await;
        exercise_lock(channel.clone()).await;
        exercise_idgen(channel.clone()).await;
        exercise_election(channel.clone()).await;
        exercise_event(channel.clone()).await;
        exercise_cache(channel.clone()).await;
        exercise_workflow(channel.clone()).await;
        exercise_policy(channel.clone()).await;
        exercise_scheduler(channel.clone()).await;
        exercise_circuit_breaker(channel.clone()).await;
        exercise_rate_limiter(channel.clone()).await;
        exercise_feature_flags(channel.clone()).await;
        exercise_transit(channel.clone()).await;
        exercise_pki(channel.clone()).await;
    });
}

/// 注册 → 发现：`Registry` 的租约与实例记录都落 server KV。
async fn exercise_registry(channel: tonic::transport::Channel) {
    use coord_proto::registry::v1::registry_client::RegistryClient;
    use coord_proto::registry::v1::{DiscoverRequest, FilterMode, RegisterRequest};

    let mut registry = RegistryClient::new(channel);
    let registered = registry
        .register(RegisterRequest {
            service_name: "v2-smoke".into(),
            instance_id: "instance-1".into(),
            metadata: "{\"addr\":\"127.0.0.1:1\"}".into(),
            ttl_seconds: 30,
        })
        .await
        .expect("Registry.Register must succeed on the v2 data plane")
        .into_inner();
    assert!(registered.lease_id > 0, "Register must allocate a lease");

    let discovered = registry
        .discover(DiscoverRequest {
            service_name: "v2-smoke".into(),
            filter_mode: FilterMode::Exact as i32,
        })
        .await
        .expect("Registry.Discover must succeed on the v2 data plane")
        .into_inner();
    assert_eq!(
        discovered.instances.len(),
        1,
        "registered instance must be discoverable"
    );
}

async fn exercise_config(channel: tonic::transport::Channel) {
    use coord_proto::config::v1::config_client::ConfigClient;
    use coord_proto::config::v1::{ConfigGetRequest, ConfigPutRequest};

    let mut config = ConfigClient::new(channel);
    config
        .put(ConfigPutRequest {
            key: "v2-smoke/key".into(),
            value: "v1".into(),
        })
        .await
        .expect("Config.Put must succeed on the v2 data plane");
    let got = config
        .get(ConfigGetRequest {
            key: "v2-smoke/key".into(),
        })
        .await
        .expect("Config.Get must succeed on the v2 data plane")
        .into_inner();
    assert!(got.found && got.value == "v1", "config roundtrip: {got:?}");
}

async fn exercise_lock(channel: tonic::transport::Channel) {
    use coord_proto::lock::v1::lock_client::LockClient;
    use coord_proto::lock::v1::{LockAcquireRequest, LockReleaseRequest};

    let mut lock = LockClient::new(channel);
    let acquired = lock
        .acquire(LockAcquireRequest {
            name: "v2-smoke/lock".into(),
            holder_id: "holder-1".into(),
            ttl_seconds: 30,
        })
        .await
        .expect("Lock.Acquire must succeed on the v2 data plane")
        .into_inner();
    assert!(acquired.acquired, "lock must be acquired: {acquired:?}");
    let released = lock
        .release(LockReleaseRequest {
            name: "v2-smoke/lock".into(),
            holder_id: "holder-1".into(),
            lease_id: acquired.lease_id,
        })
        .await
        .expect("Lock.Release must succeed on the v2 data plane")
        .into_inner();
    assert!(released.released, "lock must be released");
}

/// segment 模式：号段经 `coord.txn.v2` 的 CAS 分配 ⇒ ID 落在号段空间。
/// 雪花模式是 10^18 量级；退回雪花会让本断言失败（冒烟不得假绿）。
async fn exercise_idgen(channel: tonic::transport::Channel) {
    use coord_proto::idgen::v1::id_gen_client::IdGenClient;
    use coord_proto::idgen::v1::IdGenNextIdRequest;

    let mut idgen = IdGenClient::new(channel);
    let first = idgen
        .next_id(IdGenNextIdRequest {
            name: "v2-smoke".into(),
            step: 1,
        })
        .await
        .expect("IdGen.NextId must succeed on the v2 data plane")
        .into_inner()
        .id;
    let second = idgen
        .next_id(IdGenNextIdRequest {
            name: "v2-smoke".into(),
            step: 1,
        })
        .await
        .expect("IdGen.NextId must succeed on the v2 data plane")
        .into_inner()
        .id;
    assert!(
        second > first,
        "segment ids must increase: {first} -> {second}"
    );
    assert!(
        first > 0 && first < 1_000_000,
        "idgen must allocate from a segment (v2 Txn CAS), not the snowflake fallback: {first}"
    );
}

async fn exercise_election(channel: tonic::transport::Channel) {
    use coord_proto::election::v1::leader_election_client::LeaderElectionClient;
    use coord_proto::election::v1::{LeaderCampaignRequest, LeaderGetLeaderRequest};

    let mut election = LeaderElectionClient::new(channel);
    let campaign = election
        .campaign(LeaderCampaignRequest {
            group_name: "v2-smoke".into(),
            candidate_id: "candidate-1".into(),
            ttl_seconds: 30,
        })
        .await
        .expect("LeaderElection.Campaign must succeed on the v2 data plane")
        .into_inner();
    let leader = election
        .get_leader(LeaderGetLeaderRequest {
            group_name: "v2-smoke".into(),
        })
        .await
        .expect("LeaderElection.GetLeader must succeed on the v2 data plane")
        .into_inner();
    assert!(
        leader.exists,
        "campaign must publish a leader: {campaign:?}"
    );
    assert_eq!(leader.leader_id, "candidate-1");
}

async fn exercise_event(channel: tonic::transport::Channel) {
    use coord_proto::event::v1::event_client::EventClient;
    use coord_proto::event::v1::EventPublishRequest;

    let mut event = EventClient::new(channel);
    let published = event
        .publish(EventPublishRequest {
            event_type: "v2.smoke.published".into(),
            source: "v2-smoke".into(),
            data: br#"{"ok":true}"#.to_vec(),
            data_content_type: "application/json".into(),
            subject: "v2-smoke".into(),
        })
        .await
        .expect("Event.Publish must succeed on the v2 data plane")
        .into_inner();
    assert!(
        !published.event_id.is_empty(),
        "published event must carry an id"
    );
}

async fn exercise_cache(channel: tonic::transport::Channel) {
    use coord_proto::cache::v1::cache_client::CacheClient;
    use coord_proto::cache::v1::{CacheGetRequest, CacheSetRequest};

    let mut cache = CacheClient::new(channel);
    cache
        .set(CacheSetRequest {
            key: "v2-smoke/key".into(),
            value: b"cached".to_vec(),
            ttl_seconds: 30,
        })
        .await
        .expect("Cache.Set must succeed");
    let got = cache
        .get(CacheGetRequest {
            key: "v2-smoke/key".into(),
        })
        .await
        .expect("Cache.Get must succeed")
        .into_inner();
    assert!(got.found && got.value == b"cached");
}

async fn exercise_workflow(channel: tonic::transport::Channel) {
    use coord_proto::workflow::v1::workflow_client::WorkflowClient;
    use coord_proto::workflow::v1::{WorkflowGetStatusRequest, WorkflowStartRequest};

    let definition = r#"{"id":"v2-smoke","version":"1.0","start":"init","states":[{"name":"init","type":"inject","data":{"ok":true},"end":true}]}"#;
    let mut workflow = WorkflowClient::new(channel);
    let started = workflow
        .start(WorkflowStartRequest {
            definition_dsl: definition.into(),
            input: b"{}".to_vec(),
            definition_id: String::new(),
        })
        .await
        .expect("Workflow.Start must succeed on the v2 data plane (pure KV, no MQ)")
        .into_inner();
    assert!(!started.workflow_id.is_empty());

    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let status = workflow
            .get_status(WorkflowGetStatusRequest {
                workflow_id: started.workflow_id.clone(),
            })
            .await
            .expect("Workflow.GetStatus must succeed on the v2 data plane")
            .into_inner()
            .status;
        if status == "COMPLETED" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "workflow must reach COMPLETED through the v2 KV store, last status: {status}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn exercise_policy(channel: tonic::transport::Channel) {
    use coord_proto::policy::v1::policy_client::PolicyClient;
    use coord_proto::policy::v1::{PolicyListBundlesRequest, PolicyPutBundleRequest};

    let mut policy = PolicyClient::new(channel);
    policy
        .put_bundle(PolicyPutBundleRequest {
            tenant_id: "v2-smoke".into(),
            namespace: "default".into(),
            name: "smoke".into(),
            rego_content: "package coord.smoke\n\ndefault allow = true\n".into(),
        })
        .await
        .expect("Policy.PutBundle must succeed on the v2 data plane");
    let bundles = policy
        .list_bundles(PolicyListBundlesRequest {
            tenant_id: "v2-smoke".into(),
        })
        .await
        .expect("Policy.ListBundles must succeed on the v2 data plane")
        .into_inner();
    assert!(
        bundles.bundles.iter().any(|b| b.name == "smoke"),
        "bundle must be visible after PutBundle: {:?}",
        bundles.bundles
    );
}

async fn exercise_scheduler(channel: tonic::transport::Channel) {
    use coord_proto::scheduler::v1::scheduler_client::SchedulerClient;
    use coord_proto::scheduler::v1::{SchedulerClaimJobRequest, SchedulerRegisterJobRequest};

    let mut scheduler = SchedulerClient::new(channel);
    scheduler
        .register_job(SchedulerRegisterJobRequest {
            name: "v2-smoke-job".into(),
            cron_expression: "0 * * * *".into(),
            payload: b"{}".to_vec(),
        })
        .await
        .expect("Scheduler.RegisterJob must succeed on the v2 data plane");
    let claim = scheduler
        .claim_job(SchedulerClaimJobRequest {
            name: "v2-smoke-job".into(),
        })
        .await
        .expect("Scheduler.ClaimJob must succeed on the v2 data plane")
        .into_inner();
    assert!(claim.found, "registered job must be claimable: {claim:?}");
}

async fn exercise_circuit_breaker(channel: tonic::transport::Channel) {
    use coord_proto::circuitbreaker::v1::circuit_breaker_client::CircuitBreakerClient;
    use coord_proto::circuitbreaker::v1::{
        CircuitBreakerGetStateRequest, CircuitBreakerReportFailureRequest,
        CircuitBreakerReportSuccessRequest,
    };

    let mut breaker = CircuitBreakerClient::new(channel);
    let state = breaker
        .get_state(CircuitBreakerGetStateRequest {
            name: "v2-smoke".into(),
        })
        .await
        .expect("CircuitBreaker.GetState must succeed")
        .into_inner();
    // 线上的状态串是枚举 Debug 名（`Closed`），不随 proto 注释的大小写变化。
    assert!(
        state.state.eq_ignore_ascii_case("closed"),
        "fresh breaker must start closed: {:?}",
        state.state
    );
    breaker
        .report_failure(CircuitBreakerReportFailureRequest {
            name: "v2-smoke".into(),
        })
        .await
        .expect("CircuitBreaker.ReportFailure must succeed");
    breaker
        .report_success(CircuitBreakerReportSuccessRequest {
            name: "v2-smoke".into(),
        })
        .await
        .expect("CircuitBreaker.ReportSuccess must succeed");
}

async fn exercise_rate_limiter(channel: tonic::transport::Channel) {
    use coord_proto::ratelimiter::v1::rate_limiter_client::RateLimiterClient;
    use coord_proto::ratelimiter::v1::RateLimiterAllowRequest;

    let mut limiter = RateLimiterClient::new(channel);
    let allowed = limiter
        .allow(RateLimiterAllowRequest {
            key: "v2-smoke".into(),
            permits: 1,
        })
        .await
        .expect("RateLimiter.Allow must succeed")
        .into_inner();
    assert!(allowed.allowed, "first permit must be allowed: {allowed:?}");
}

async fn exercise_feature_flags(channel: tonic::transport::Channel) {
    use coord_proto::featureflags::v1::feature_flags_client::FeatureFlagsClient;
    use coord_proto::featureflags::v1::{FeatureFlagIsEnabledRequest, FeatureFlagPutFlagRequest};

    let mut flags = FeatureFlagsClient::new(channel);
    flags
        .put_flag(FeatureFlagPutFlagRequest {
            flag_name: "v2-smoke".into(),
            enabled: true,
            percentage: None,
        })
        .await
        .expect("FeatureFlags.PutFlag must succeed on the v2 data plane");
    let state = flags
        .is_enabled(FeatureFlagIsEnabledRequest {
            flag_name: "v2-smoke".into(),
            context: Vec::new(),
        })
        .await
        .expect("FeatureFlags.IsEnabled must succeed on the v2 data plane")
        .into_inner();
    assert!(state.enabled, "flag must be enabled after PutFlag");
}

async fn exercise_transit(channel: tonic::transport::Channel) {
    use coord_proto::transit::v1::transit_client::TransitClient;
    use coord_proto::transit::v1::{TransitDecryptRequest, TransitEncryptRequest};

    let mut transit = TransitClient::new(channel);
    let plaintext = b"v2-smoke-secret";
    let ciphertext = transit
        .encrypt(TransitEncryptRequest {
            plaintext: plaintext.to_vec(),
            context: Vec::new(),
        })
        .await
        .expect("Transit.Encrypt must succeed")
        .into_inner()
        .ciphertext;
    assert!(!ciphertext.is_empty(), "ciphertext must not be empty");
    let decrypted = transit
        .decrypt(TransitDecryptRequest {
            ciphertext,
            context: Vec::new(),
        })
        .await
        .expect("Transit.Decrypt must succeed")
        .into_inner()
        .plaintext;
    assert_eq!(
        decrypted, plaintext,
        "transit roundtrip must preserve bytes"
    );
}

async fn exercise_pki(channel: tonic::transport::Channel) {
    use coord_proto::pki::v1::pki_client::PkiClient;
    use coord_proto::pki::v1::{PkiGetCaCertRequest, PkiIssueCertRequest};

    let mut pki = PkiClient::new(channel);
    let ca = pki
        .get_ca_cert(PkiGetCaCertRequest {})
        .await
        .expect("Pki.GetCaCert must succeed")
        .into_inner();
    assert!(
        !ca.ca_cert_pem.is_empty(),
        "PKI CA must be auto-initialized at service start"
    );
    let issued = pki
        .issue_cert(PkiIssueCertRequest {
            common_name: "v2-smoke.local".into(),
            ttl_seconds: 3600,
            dns_names: vec!["v2-smoke.local".into()],
            ip_addresses: Vec::new(),
        })
        .await
        .expect("Pki.IssueCert must succeed on the v2 data plane")
        .into_inner();
    assert!(
        !issued.cert_pem.is_empty() && !issued.serial.is_empty(),
        "issued cert must carry pem + serial"
    );
}

/// 夹具自查：数据目录必须落在临时目录（不污染仓库工作区）。
#[test]
fn test_cluster_data_dir_is_outside_the_repository() {
    let dir = std::env::temp_dir();
    assert!(dir.is_absolute());
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"));
    assert!(
        !dir.starts_with(repo),
        "test fixtures must not write under the repository"
    );
}
