//! MQ v2 M5-3 证据：workflow 定义的 MQ 触发器（真实 3 节点 v2 集群 + 真实 Agent 进程）。
//!
//! 判据（设计 §6 M5-3；ADR-0016 D12）：
//! - 定义声明 `trigger` ⇒ durable 引擎按定义加入消费组（`wf-trigger:<definition_id>`）；
//! - `Poll` 到的消息以 `mq:<topic_id>:<partition>:<offset>` 为 Start 幂等键启动实例；
//! - 过滤不命中的消息照常 Ack（不启动实例，也不阻塞分区）；
//! - 组位点推进到已 Ack offset（至少一次路径）；`kill -9` 后重启不产生重复实例；
//! - 触发器不建 topic（`PollDlq` 在正常路径为空 = 没有毒消息被误移）。
//!
//! 运行：`cargo test -p coord --test mq_v2_m5_trigger_test -- --test-threads=1`

use std::collections::BTreeSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

#[path = "common/mod.rs"]
mod common;

use common::v2_cluster::{runtime, V2Cluster, V2ClusterOptions};
use common::{find_port, wait_tcp_ready};

use coord_client::{Client, DataPlane, MqPublish, MqV2Client};
use coord_proto::workflow::v1::workflow_client::WorkflowClient;
use coord_proto::workflow::v1::{
    WorkflowDeployRequest, WorkflowListInstancesRequest, WorkflowListInstancesResponse,
};
use tonic::transport::Channel;

const TOPIC: &str = "orders";
const EVENTS_TOPIC: &str = "com.china-workflow.events";
const PROCESSED: &str = "order.processed";
const TRIGGER_STARTED: &str = "Workflow MQ triggers started";
const TRIGGER_JOINED: &str = "workflow trigger joined group";
const CREATED: &str = "order.created";
const CANCELLED: &str = "order.cancelled";

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

use coord_client::config::Config;

/// 触发器定义：CNCF SW `inject → end`（秒级完成，无外部依赖）。
fn triggered_definition(name: &str, trigger: &str) -> String {
    format!(
        r#"{{"id":"{name}","version":"1.0","trigger":{trigger},"start":"init",
            "states":[{{"name":"init","type":"inject","data":{{"ok":true}},"end":true}}]}}"#
    )
}

// ─── 真实 Agent 进程（v2 durable + MQ 代理）───

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

    fn kill9(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    fn restart(&mut self) {
        self.child = spawn_agent(&self.config_path, &self.data_dir, &self.log_path);
    }

    fn assert_alive(&mut self) {
        if let Ok(Some(status)) = self.child.try_wait() {
            panic!(
                "agent exited with {status} (v2 durable trigger assembly must start); logs:\n{}",
                tail(&self.logs(), 60)
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

async fn deploy(workflow: &mut WorkflowClient<Channel>, yaml: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match workflow
            .deploy(WorkflowDeployRequest {
                namespace: "default".to_string(),
                definition_yaml: yaml.to_string(),
            })
            .await
        {
            Ok(response) => return response.into_inner().workflow_id,
            Err(status) => {
                assert!(
                    Instant::now() < deadline,
                    "Workflow.Deploy never succeeded: {status}"
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

async fn list_instances(
    workflow: &mut WorkflowClient<Channel>,
    definition_id: &str,
) -> WorkflowListInstancesResponse {
    workflow
        .list_instances(WorkflowListInstancesRequest {
            workflow_id: definition_id.to_string(),
            namespace: String::new(),
            page_size: 100,
            page_token: String::new(),
            status_filter: String::new(),
        })
        .await
        .expect("Workflow.ListInstances")
        .into_inner()
}

async fn wait_instances(
    workflow: &mut WorkflowClient<Channel>,
    definition_id: &str,
    expected: usize,
    timeout: Duration,
) -> Vec<String> {
    let deadline = Instant::now() + timeout;
    loop {
        let listed = list_instances(workflow, definition_id).await;
        let ids: Vec<String> = listed
            .instances
            .iter()
            .map(|instance| instance.instance_id.clone())
            .collect();
        if ids.len() >= expected {
            return ids;
        }
        assert!(
            Instant::now() < deadline,
            "definition {definition_id}: expected {expected} instance(s), saw {}",
            ids.len()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_all_completed(
    workflow: &mut WorkflowClient<Channel>,
    definition_id: &str,
    expected: usize,
) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let listed = list_instances(workflow, definition_id).await;
        let completed = listed
            .instances
            .iter()
            .filter(|instance| instance.state.eq_ignore_ascii_case("COMPLETED"))
            .count();
        if completed >= expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "definition {definition_id}: only {completed}/{expected} instances completed \
             (states: {:?})",
            listed
                .instances
                .iter()
                .map(|instance| instance.state.clone())
                .collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn publish(mq: &MqV2Client, partition: i32, event_type: &str, body: &str) {
    let mut headers = std::collections::BTreeMap::new();
    headers.insert("ce-type".to_string(), event_type.to_string());
    headers.insert("ce-source".to_string(), "erp".to_string());
    mq.publish(&MqPublish {
        topic: TOPIC.to_string(),
        partition,
        topic_epoch: 0,
        key: Vec::new(),
        payload: body.as_bytes().to_vec(),
        idempotency_key: String::new(),
        headers,
        producer: "m5-trigger-test".to_string(),
    })
    .await
    .expect("publish order event");
}

/// 消费组位点总和（两个分区之和 = 已 Ack 的消息数）。
async fn committed_total(mq: &MqV2Client, group: &str) -> u64 {
    let described = mq
        .describe_group(group)
        .await
        .unwrap_or_else(|err| panic!("DescribeGroup {group}: {err}"));
    described
        .partitions
        .iter()
        .map(|partition| partition.committed_offset)
        .sum()
}

#[test]
fn published_messages_start_the_triggered_workflow() {
    let _ = runtime();
    let cluster = mq_cluster();
    let addrs = cluster.addrs();
    let mut agent = AgentFixture::start(&addrs[0]);

    runtime().block_on(async {
        wait_tcp_ready(&agent.addr, Duration::from_secs(30)).await;
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            agent.assert_alive();
            if agent.logs().contains(TRIGGER_STARTED) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "trigger dispatcher never started; logs:\n{}",
                tail(&agent.logs(), 60)
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let client = v2_client(addrs.clone()).await;
        let mq = client.mq_v2();
        mq.create_topic(&coord_client::MqCreateTopic {
            topic: TOPIC.to_string(),
            partitions: 2,
            request_id: "m5-trigger-test".to_string(),
            config: None,
        })
        .await
        .expect("create orders topic");

        let mut workflow = WorkflowClient::new(connect(&agent.addr).await);
        let all_id = deploy(
            &mut workflow,
            &triggered_definition("trigger-all", &format!("{{\"topic\":\"{TOPIC}\"}}")),
        )
        .await;
        let created_id = deploy(
            &mut workflow,
            &triggered_definition(
                "trigger-created",
                &format!("{{\"topic\":\"{TOPIC}\",\"filter\":{{\"event_type\":\"{CREATED}\"}}}}"),
            ),
        )
        .await;

        // 两个消费组都加入（各自独立消费同一 topic）。
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let logs = agent.logs();
            if logs.matches(TRIGGER_JOINED).count() >= 2 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "trigger consumers never joined their groups; logs:\n{}",
                tail(&logs, 60)
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        publish(&mq, 0, CREATED, r#"{"orderId":"o-1"}"#).await;
        publish(&mq, 1, CREATED, r#"{"orderId":"o-2"}"#).await;
        publish(&mq, 0, CANCELLED, r#"{"orderId":"o-3"}"#).await;

        // 无过滤定义：3 条消息各一个实例；有过滤定义：只有 2 条命中。
        let all = wait_instances(&mut workflow, &all_id, 3, Duration::from_secs(60)).await;
        let created = wait_instances(&mut workflow, &created_id, 2, Duration::from_secs(60)).await;
        assert_eq!(
            all.iter().collect::<BTreeSet<_>>().len(),
            3,
            "each message must map to its own instance"
        );
        assert_eq!(created.iter().collect::<BTreeSet<_>>().len(), 2);
        wait_all_completed(&mut workflow, &all_id, 3).await;
        wait_all_completed(&mut workflow, &created_id, 2).await;

        // 组位点推进到已 Ack：两个组都消费了 3 条（不命中的那条也 Ack）。
        let all_group = format!("wf-trigger:{all_id}");
        let created_group = format!("wf-trigger:{created_id}");
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let all_committed = committed_total(&mq, &all_group).await;
            let created_committed = committed_total(&mq, &created_group).await;
            if all_committed == 3 && created_committed == 3 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "committed offsets did not reach 3 (all: {all_committed}, filtered: \
                 {created_committed})"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // 正常路径没有毒消息：DLQ 为空（毒消息进 DLQ 的调用/reason/Ack 由分发器
        // 单测 `a_poison_message_moves_to_dlq_with_the_stable_reason_and_is_acked` 钉住；
        // 服务端 MoveToDlq/PollDlq 的 wire 行为由 M4 用例钉住）。
        let mut raw = coord_proto::mq::v2::mq_client::MqClient::new(connect(&addrs[0]).await);
        for partition in 0..2 {
            let dlq = raw
                .poll_dlq(coord_proto::mq::v2::PollDlqRequest {
                    topic: TOPIC.to_string(),
                    partition,
                    topic_epoch: 0,
                    start_offset: None,
                    max_messages: 10,
                })
                .await
                .expect("PollDlq")
                .into_inner();
            assert!(
                dlq.messages.is_empty(),
                "no message should reach the DLQ on the happy path (partition {partition})"
            );
        }
    });
}

#[test]
fn a_killed_agent_redelivers_without_duplicate_instances() {
    let _ = runtime();
    let cluster = mq_cluster();
    let addrs = cluster.addrs();
    let mut agent = AgentFixture::start(&addrs[0]);

    runtime().block_on(async {
        wait_tcp_ready(&agent.addr, Duration::from_secs(30)).await;
        let client = v2_client(addrs.clone()).await;
        let mq = client.mq_v2();
        mq.create_topic(&coord_client::MqCreateTopic {
            topic: TOPIC.to_string(),
            partitions: 1,
            request_id: "m5-trigger-crash".to_string(),
            config: None,
        })
        .await
        .expect("create orders topic");

        let mut workflow = WorkflowClient::new(connect(&agent.addr).await);
        let definition_id = deploy(
            &mut workflow,
            &triggered_definition("trigger-crash", &format!("{{\"topic\":\"{TOPIC}\"}}")),
        )
        .await;
        let group = format!("wf-trigger:{definition_id}");

        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            agent.assert_alive();
            if agent.logs().contains(TRIGGER_JOINED) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "trigger consumer never joined; logs:\n{}",
                tail(&agent.logs(), 60)
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // 三条消息：在第一条实例出现后立刻 kill -9，把「已 Start、未 Ack」的窗口
        // 撞出来（重投必须命中同一幂等键 ⇒ 实例数不增）。
        publish(&mq, 0, CREATED, r#"{"orderId":"c-1"}"#).await;
        publish(&mq, 0, CREATED, r#"{"orderId":"c-2"}"#).await;
        publish(&mq, 0, CREATED, r#"{"orderId":"c-3"}"#).await;
        wait_instances(&mut workflow, &definition_id, 1, Duration::from_secs(60)).await;
        agent.kill9();
        agent.restart();
        wait_tcp_ready(&agent.addr, Duration::from_secs(30)).await;

        let mut workflow = WorkflowClient::new(connect(&agent.addr).await);
        let ids = wait_instances(&mut workflow, &definition_id, 3, Duration::from_secs(120)).await;
        wait_all_completed(&mut workflow, &definition_id, 3).await;

        // 再等一个对账/重投周期，确认没有第 4 个实例出现。
        tokio::time::sleep(Duration::from_secs(2)).await;
        let ids_after =
            wait_instances(&mut workflow, &definition_id, 3, Duration::from_secs(30)).await;
        assert_eq!(
            ids_after.len(),
            3,
            "kill -9 must not duplicate instances (got {}: {:?})",
            ids_after.len(),
            ids_after
        );
        assert_eq!(ids.iter().collect::<BTreeSet<_>>().len(), 3);

        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let committed = committed_total(&mq, &group).await;
            if committed == 3 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "group {group} committed {committed}/3 after restart"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
}

/// 链路 A：收到订单消息 → 发出 `order.processed` 事件（经 durable outbox 投递）。
fn emit_definition(name: &str, trigger: &str) -> String {
    format!(
        r#"document:
  dsl: "1.0.3"
  namespace: default
  name: {name}
  version: "1.0"
do:
  - notify:
      emit:
        event:
          with:
            type: {PROCESSED}
            source: {name}
            data:
              ok: true
trigger:
  topic: {trigger}
"#
    )
}

#[test]
fn the_chain_publish_trigger_workflow_outbox_next_trigger_completes() {
    let _ = runtime();
    let cluster = mq_cluster();
    let addrs = cluster.addrs();
    let mut agent = AgentFixture::start(&addrs[0]);

    runtime().block_on(async {
        wait_tcp_ready(&agent.addr, Duration::from_secs(30)).await;
        let client = v2_client(addrs.clone()).await;
        let mq = client.mq_v2();
        mq.create_topic(&coord_client::MqCreateTopic {
            topic: TOPIC.to_string(),
            partitions: 1,
            request_id: "m5-chain".to_string(),
            config: None,
        })
        .await
        .expect("create orders topic");

        let mut workflow = WorkflowClient::new(connect(&agent.addr).await);
        // A：业务消息触发；完成时由 outbox 发 `order.processed`。
        let a_id = deploy(&mut workflow, &emit_definition("chain-a", TOPIC)).await;
        // B：消费 `order.processed`（事件 topic 上的其他事件被过滤掉，含 B 自己的
        // `workflow.completed.v1` —— 否则会自激）。
        let b_id = deploy(
            &mut workflow,
            &triggered_definition(
                "chain-b",
                &format!(
                    "{{\"topic\":\"{EVENTS_TOPIC}\",\"filter\":{{\"event_type\":\"{PROCESSED}\"}}}}"
                ),
            ),
        )
        .await;

        publish(&mq, 0, CREATED, r#"{"orderId":"chain-1"}"#).await;

        // 第一跳：publish → trigger → A 完成。
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let a_instances = list_instances(&mut workflow, &a_id).await.instances;
            if a_instances
                .iter()
                .any(|instance| instance.state.eq_ignore_ascii_case("COMPLETED"))
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "chain-a never completed; agent logs:\n{}",
                tail(&agent.logs(), 60)
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // 第二跳：A 的 `order.processed`（outbox → coord.mq.v2）→ B 完成。
        let b_ids = wait_instances(&mut workflow, &b_id, 1, Duration::from_secs(120)).await;
        wait_all_completed(&mut workflow, &b_id, 1).await;
        assert_eq!(
            b_ids.len(),
            1,
            "the processed event must start exactly one B instance"
        );

        // 断链点重启：kill -9 后 A/B 各自仍只有一个实例（不重复触发）。
        agent.kill9();
        agent.restart();
        wait_tcp_ready(&agent.addr, Duration::from_secs(30)).await;
        tokio::time::sleep(Duration::from_secs(2)).await;

        let mut workflow = WorkflowClient::new(connect(&agent.addr).await);
        let a_after = list_instances(&mut workflow, &a_id).await.instances;
        let b_after = list_instances(&mut workflow, &b_id).await.instances;
        assert_eq!(a_after.len(), 1, "restart must not re-trigger chain-a");
        assert_eq!(b_after.len(), 1, "restart must not re-trigger chain-b");
        wait_all_completed(&mut workflow, &a_id, 1).await;
        wait_all_completed(&mut workflow, &b_id, 1).await;
    });
}
