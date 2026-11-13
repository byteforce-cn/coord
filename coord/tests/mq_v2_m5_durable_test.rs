//! MQ v2 M5 证据：durable 工作流在 v2 数据面上的同事务提交与 kill -9 续跑。
//!
//! 判据（设计 §6 M5-1/M5-2）：
//! - 实例状态与伴生 outbox 记录在同一次 `coord.txn.v2` 提交里落库（同一 `mod_revision`）；
//! - Agent（`data_plane="v2"` + `services.mq/workflow/workflow_durable=true`）启动成功，
//!   outbox relay 经 `coord.mq.v2` 投递事件；`idempotency_key = event_id`（重投被去重）；
//! - kill -9 后重启：实例状态与 outbox 不丢，已完成事件不重复投递；
//! - v2 durable 装配下审批桥可用（`ListPendingTasks` 不是 `FAILED_PRECONDITION`）。
//!
//! 运行：`cargo test -p coord --test mq_v2_m5_durable_test -- --test-threads=1`
//! （真实进程：3 个 `coord server`；第二个用例另起一个 `coord agent`）。

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

#[path = "common/mod.rs"]
mod common;

use common::v2_cluster::{runtime, V2Cluster, V2ClusterOptions};
use common::{find_port, wait_tcp_ready};

use coord_agent::cache::AgentCache;
use coord_agent::service::BaseService;
use coord_agent::services::outbox_kv::KvOutboxStore;
use coord_agent::services::workflow::phase4::{DurableV2EngineOptions, WorkflowEngineService};
use coord_agent::{AgentInner, WorkflowDispatchConfig};
use coord_client::config::Config;
use coord_client::{Client, DataPlane, MqPoll, MqV2Client};
use coord_core::workflow::durable::OutboxStore;
use coord_core::workflow::model::InstanceStatus;
use coord_proto::workflow::v1::workflow_client::WorkflowClient;
use coord_proto::workflow::v1::{
    WorkflowGetStatusRequest, WorkflowListPendingTasksRequest, WorkflowStartRequest,
};
use serde_json::json;
use tonic::transport::Channel;

const TOPIC: &str = "com.china-workflow.events";
const COMPLETED_EVENT: &str = "com.china-workflow.workflow.completed.v1";
const RELAY_STARTED: &str = "Workflow outbox relay started";
const DEFINITION: &str = r#"{"id":"m5-durable","version":"1.0","start":"init","states":[{"name":"init","type":"inject","data":{"ok":true},"end":true}]}"#;

fn tail(text: &str, lines: usize) -> String {
    let all: Vec<&str> = text.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

fn write_file(path: &Path, body: &str) {
    let mut file = std::fs::File::create(path).expect("create file");
    file.write_all(body.as_bytes()).expect("write file");
}

async fn connect(addr: &str) -> Channel {
    Channel::from_shared(format!("http://{addr}"))
        .expect("endpoint")
        .connect_timeout(Duration::from_secs(5))
        .connect()
        .await
        .unwrap_or_else(|err| panic!("connect {addr}: {err}"))
}

fn mq_cluster() -> V2Cluster {
    V2Cluster::start_with(V2ClusterOptions {
        toml: "[mq_v2]\nenabled = true\nlog_shards = 2\ngroup_shards = 1\n",
        ..Default::default()
    })
}

async fn v2_client(addrs: Vec<String>) -> Client {
    Client::connect_direct(Config::new(addrs).with_data_plane(DataPlane::V2))
        .await
        .expect("v2 coord client")
}

/// 第一个用例：状态 + 伴生事件同一次 v2 事务提交。
///
/// relay 刻意关闭：`mark_delivered` 会重写 outbox 记录并推进 `mod_revision`，
/// 那之后就无法再证明「记录与实例状态同一次提交」。
#[test]
fn durable_state_and_outbox_commit_in_one_v2_transaction() {
    let _ = runtime();
    let cluster = mq_cluster();
    let addrs = cluster.addrs();

    runtime().block_on(async {
        let client = v2_client(addrs).await;
        let inner = std::sync::Arc::new(AgentInner {
            client: client.clone(),
            cache: AgentCache::new(256, 0, 32, 0),
        });
        let svc = WorkflowEngineService::new_with_kv_store_durable_v2(
            std::sync::Arc::clone(&inner),
            &WorkflowDispatchConfig::default(),
            DurableV2EngineOptions {
                topic: TOPIC.to_string(),
                relay_enabled: false,
                relay_interval_ms: 50,
                approval_roles: std::collections::HashMap::new(),
                on_behalf_services: Vec::new(),
            },
        );
        svc.start()
            .await
            .expect("durable v2 engine must start (event topic is bootstrapped via coord.mq.v2)");

        let def_id = svc
            .deploy_definition("m5", DEFINITION)
            .await
            .expect("deploy definition");
        let inst = svc
            .start_instance(&def_id, json!({}))
            .await
            .expect("start instance");

        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if matches!(
                svc.get_instance(&inst.id).await,
                Ok(Some(ref current)) if current.status == InstanceStatus::Completed
            ) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "instance {} never completed on v2; cluster logs:\n{}",
                inst.id,
                tail(&cluster.logs(), 40)
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let outbox = KvOutboxStore::new(std::sync::Arc::clone(&inner));
        let completed = OutboxStore::list_all(&outbox)
            .await
            .expect("list outbox")
            .into_iter()
            .find(|rec| rec.aggregate_id == inst.id && rec.event_type == COMPLETED_EVENT)
            .expect("completion event must be in the outbox");

        // 键布局见 `KvWorkflowStore::instance_key` / `KvOutboxStore::record_key`
        // （coord-agent 内部 `pub(crate)`，测试按同一布局读取权威行）。
        let instance_key = format!("/_workflow/v3/instances/{}", inst.id);
        let record_key = format!(
            "/_workflow/v3/outbox/records/{}/{:020}",
            completed.aggregate_id, completed.sequence
        );
        let instance_rev = read_mod_revision(&client, &instance_key).await;
        let record_rev = read_mod_revision(&client, &record_key).await;
        assert_eq!(
            instance_rev, record_rev,
            "state and outbox record must share one coord.txn.v2 commit \
             (instance rev {instance_rev}, outbox rev {record_rev})"
        );
    });
}

async fn read_mod_revision(client: &Client, key: &str) -> i64 {
    let (pairs, _, _) = client
        .kv_v2()
        .range(key.as_bytes(), &[], 1, 0, false, false)
        .await
        .unwrap_or_else(|err| panic!("read {key}: {err}"));
    pairs
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("key {key} must exist"))
        .mod_revision
}

// ─── 真实 Agent 进程（durable 装配）───

struct AgentFixture {
    child: Child,
    addr: String,
    config_path: PathBuf,
    data_dir: PathBuf,
    log_path: PathBuf,
    _dir: tempfile::TempDir,
}

impl AgentFixture {
    fn start(peer: &str) -> Self {
        let dir = tempfile::tempdir().expect("agent tempdir");
        let port = find_port();
        let http_port = find_port();
        let config_path = dir.path().join("agent.toml");
        let log_path = dir.path().join("agent.log");
        let data_dir = dir.path().join("agent-data");
        // v2 durable：投递面是 coord.mq.v2（services.mq 只代表代理开关），
        // 事件 topic 用默认值 `com.china-workflow.events`。
        let config = format!(
            "agent_addr = \"127.0.0.1:{port}\"\n\
             http_addr = \"127.0.0.1:{http_port}\"\n\
             data_dir = \"{}\"\n\
             discovery_mode = \"static\"\n\
             static_peers = [\"{peer}\"]\n\
             data_plane = \"v2\"\n\
             [auth]\nenabled = false\n\
             [services]\nworkflow = true\nworkflow_durable = true\nmq = true\n",
            data_dir.display()
        );
        write_file(&config_path, &config);
        let child = spawn_agent(&config_path, &data_dir, &log_path);
        Self {
            child,
            addr: format!("127.0.0.1:{port}"),
            config_path,
            data_dir,
            log_path,
            _dir: dir,
        }
    }

    fn logs(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
    }

    /// SIGKILL（`kill -9`）并回收僵尸进程。
    fn kill9(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    /// 用**同一**配置/数据目录重启（进程内状态全部丢弃，只剩服务端持久化面）。
    fn restart(&mut self) {
        self.child = spawn_agent(&self.config_path, &self.data_dir, &self.log_path);
    }

    fn assert_alive(&mut self) {
        if let Ok(Some(status)) = self.child.try_wait() {
            panic!(
                "agent exited with {status} (durable v2 assembly must start); logs:\n{}",
                tail(&self.logs(), 40)
            );
        }
    }
}

impl Drop for AgentFixture {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn_agent(config_path: &Path, data_dir: &Path, log_path: &Path) -> Child {
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .expect("agent log");
    let log_err = log.try_clone().expect("clone log");
    Command::new(env!("CARGO_BIN_EXE_coord"))
        .arg("agent")
        .arg("--agent-config")
        .arg(config_path)
        .arg("--data-dir")
        .arg(data_dir)
        .env("RUST_LOG", "coord_agent=info,coord=warn")
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log_err))
        .spawn()
        .expect("spawn coord agent")
}

/// 第二个用例：真实 Agent 进程 kill -9 后状态/事件不丢不重。
#[test]
fn durable_workflow_survives_agent_kill_and_never_duplicates_events() {
    let _ = runtime();
    let cluster = mq_cluster();
    let addrs = cluster.addrs();
    let mut agent = AgentFixture::start(&addrs[0]);

    runtime().block_on(async {
        wait_tcp_ready(&agent.addr, Duration::from_secs(30)).await;
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            agent.assert_alive();
            if agent.logs().contains(RELAY_STARTED) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "durable engine never started its outbox relay; logs:\n{}",
                tail(&agent.logs(), 40)
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        assert!(
            agent.logs().contains("coord.mq.v2"),
            "durable engine must bootstrap the events topic through coord.mq.v2; logs:\n{}",
            tail(&agent.logs(), 40)
        );

        let mut workflow = WorkflowClient::new(connect(&agent.addr).await);
        let workflow_id = start_on_agent(&mut workflow).await;
        await_status(&mut workflow, &workflow_id).await;

        // 审批桥随 durable 装配注入：不是 FAILED_PRECONDITION。
        workflow
            .list_pending_tasks(WorkflowListPendingTasksRequest {
                page_size: 10,
                page_token: String::new(),
                assignee: String::new(),
            })
            .await
            .expect("approval bridge must be available under v2 durable assembly");

        let client = v2_client(addrs.clone()).await;
        let mq = client.mq_v2();
        let completed = poll_completed(&mq, Duration::from_secs(30)).await;
        assert_eq!(
            completed.len(),
            1,
            "exactly one completion event must be delivered to coord.mq.v2"
        );
        let event_id = completed[0]
            .headers
            .get("ce-id")
            .expect("completion event carries ce-id")
            .clone();

        // 投递键就是 event_id：同键重发被 broker 幂等索引吸收（relay 重投不会产生第二条）。
        let replay = mq
            .publish(&coord_client::MqPublish {
                topic: TOPIC.to_string(),
                partition: 0,
                payload: completed[0].payload.clone(),
                idempotency_key: event_id.clone(),
                ..Default::default()
            })
            .await
            .expect("replay publish must succeed");
        assert!(
            replay.deduplicated,
            "event id must be the MQ idempotency key (replay must deduplicate)"
        );

        // kill -9：进程内缓存/租约全部丢失，只剩 coord.txn.v2 里的状态与 outbox。
        let relay_before = agent.logs().matches(RELAY_STARTED).count();
        agent.kill9();
        agent.restart();
        wait_tcp_ready(&agent.addr, Duration::from_secs(30)).await;
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            agent.assert_alive();
            if agent.logs().matches(RELAY_STARTED).count() > relay_before {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "agent did not resume the durable outbox relay after kill -9; logs:\n{}",
                tail(&agent.logs(), 60)
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let mut workflow = WorkflowClient::new(connect(&agent.addr).await);
        await_status(&mut workflow, &workflow_id).await;
        // 重启后 relay 会重扫 outbox；已完成事件不得被再投一次。
        let after_restart = poll_completed(&mq, Duration::from_secs(30)).await;
        assert_eq!(
            after_restart.len(),
            1,
            "restart must not duplicate already-delivered completion events \
             (found {})",
            after_restart.len()
        );
    });
}

async fn start_on_agent(workflow: &mut WorkflowClient<Channel>) -> String {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match workflow
            .start(WorkflowStartRequest {
                definition_dsl: DEFINITION.to_string(),
                input: b"{}".to_vec(),
                definition_id: String::new(),
            })
            .await
        {
            Ok(response) => {
                let id = response.into_inner().workflow_id;
                assert!(!id.is_empty(), "Start must return a workflow id");
                return id;
            }
            Err(status) => {
                assert!(
                    Instant::now() < deadline,
                    "Workflow.Start never succeeded on the v2 agent: {status}"
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

async fn await_status(workflow: &mut WorkflowClient<Channel>, workflow_id: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let status = workflow
            .get_status(WorkflowGetStatusRequest {
                workflow_id: workflow_id.to_string(),
            })
            .await
            .expect("Workflow.GetStatus must succeed")
            .into_inner()
            .status;
        if status.eq_ignore_ascii_case("COMPLETED") {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "workflow {workflow_id} never completed on v2 (last status: {status})"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// 读 topic 上全部 `instance.completed` 事件（轮询到至少一条或超时）。
async fn poll_completed(mq: &MqV2Client, timeout: Duration) -> Vec<coord_client::MqMessage> {
    let deadline = Instant::now() + timeout;
    loop {
        let batch = mq
            .poll(&MqPoll {
                topic: TOPIC.to_string(),
                partition: 0,
                topic_epoch: 0,
                start_offset: Some(0),
                max_messages: 0,
                max_bytes: 0,
                wait_ms: 0,
            })
            .await
            .expect("Poll on the durable events topic");
        let completed: Vec<_> = batch
            .messages
            .into_iter()
            .filter(|message| {
                message
                    .headers
                    .get("ce-type")
                    .map(|kind| kind == COMPLETED_EVENT)
                    .unwrap_or(false)
            })
            .collect();
        if !completed.is_empty() || Instant::now() >= deadline {
            return completed;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
