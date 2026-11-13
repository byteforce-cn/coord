//! MQ v2 M2-4 验收：真实 3 节点 v2 集群上的写路径（#334 / #284）。
//!
//! 判据（设计 §6 M2-4）：
//! - 非 Leader 的分片副本对 `Publish` 返回 `UNAVAILABLE + NOT_LEADER`，trailer
//!   `coord-leader-hint` 指向该分片当前 Leader；
//! - `coord-client` 按分区缓存 Leader，`NOT_LEADER` 自动重路由后发布成功；
//! - Agent（`data_plane = "v2"` + `services.mq = true`）代理 `coord.mq.v2`：
//!   CreateTopic/Publish 经 Agent 成功（不启动 v1 本地 MQ，无 v1 回退）。
//!
//! 运行：`cargo test -p coord --test mq_v2_m2_cluster_test -- --test-threads=1`
//! （真实进程：3 个 `coord server` + 1 个 `coord agent`）。

use std::io::Write;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

#[path = "common/mod.rs"]
mod common;

use common::v2_cluster::{runtime, V2Cluster, V2ClusterOptions};
use common::{find_port, wait_tcp_ready};
use coord_core::error_code::error_code_of;
use coord_proto::mq::v2 as mq_v2;
use coord_proto::mq::v2::mq_client::MqClient;
use tonic::transport::Channel;
use tonic::Code;

const LEADER_HINT: &str = "coord-leader-hint";

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

/// 建 topic：region 0 的 follower 会返回 `NOT_LEADER`，逐节点重试即可（客户端 SDK
/// 会自动跟随 hint；这里用原始客户端是为了单独钉住 hint 契约）。
async fn create_topic(
    addrs: &[String],
    topic: &str,
    partitions: i32,
) -> mq_v2::CreateTopicResponse {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last = String::new();
    loop {
        for addr in addrs {
            let mut client = MqClient::new(connect(addr).await);
            match client
                .create_topic(mq_v2::CreateTopicRequest {
                    topic: topic.to_string(),
                    partitions,
                    request_id: String::new(),
                    config: None,
                })
                .await
            {
                Ok(response) => return response.into_inner(),
                Err(status) => last = format!("{addr}: {status}"),
            }
        }
        assert!(
            Instant::now() < deadline,
            "CreateTopic never reached the region 0 leader; last error: {last}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// 等分区 Leader 就绪（分片 raft 组的选举比 region 0 稍慢）。
async fn wait_partition_leader(addrs: &[String], topic: &str, partition: i32) -> String {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        for addr in addrs {
            let mut client = MqClient::new(connect(addr).await);
            if let Ok(response) = client
                .get_topic_leader(mq_v2::GetTopicLeaderRequest {
                    topic: topic.to_string(),
                })
                .await
            {
                if let Some(leader) = response
                    .into_inner()
                    .partitions
                    .into_iter()
                    .find(|entry| entry.partition == partition)
                    .filter(|entry| !entry.leader_addr.is_empty())
                {
                    return leader.leader_addr;
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "shard leader for {topic}/{partition} never became visible"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

struct AgentFixture {
    child: Child,
    addr: String,
    _dir: tempfile::TempDir,
}

impl Drop for AgentFixture {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// 起一个只开 `services.mq` 的 v2 Agent（指向集群第一个节点）。
fn start_agent(peer: &str) -> AgentFixture {
    let dir = tempfile::tempdir().expect("agent tempdir");
    let port = find_port();
    let http_port = find_port();
    let config = format!(
        "agent_addr = \"127.0.0.1:{port}\"\n\
         http_addr = \"127.0.0.1:{http_port}\"\n\
         data_dir = \"{}\"\n\
         discovery_mode = \"static\"\n\
         static_peers = [\"{peer}\"]\n\
         data_plane = \"v2\"\n\
         [auth]\nenabled = false\n\
         [services]\nmq = true\n",
        dir.path().join("agent").display()
    );
    write_file(&dir.path().join("agent.toml"), &config);
    let log = std::fs::File::create(dir.path().join("agent.log")).expect("agent log");
    let log_err = log.try_clone().expect("clone log");
    let child = Command::new(env!("CARGO_BIN_EXE_coord"))
        .arg("agent")
        .arg("--agent-config")
        .arg(dir.path().join("agent.toml"))
        .arg("--data-dir")
        .arg(dir.path().join("agent-data"))
        .env("RUST_LOG", "coord_agent=warn,coord=warn")
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log_err))
        .spawn()
        .expect("spawn coord agent");
    AgentFixture {
        child,
        addr: format!("127.0.0.1:{port}"),
        _dir: dir,
    }
}

fn assert_agent_alive(agent: &mut AgentFixture) {
    if let Ok(Some(status)) = agent.child.try_wait() {
        let logs = std::fs::read_to_string(agent._dir.path().join("agent.log")).unwrap_or_default();
        panic!(
            "agent exited with {status} (services.mq = true on a v2 data plane must start); \
             logs:\n{}",
            tail(&logs, 40)
        );
    }
}

#[test]
fn mq_v2_publish_on_a_three_node_cluster() {
    let _ = runtime();
    let cluster = V2Cluster::start_with(V2ClusterOptions {
        toml: "[mq_v2]\nenabled = true\nlog_shards = 2\ngroup_shards = 1\n",
        ..Default::default()
    });
    assert!(
        cluster.logs().contains("MQ v2 enabled"),
        "server must log the MQ v2 assembly:\n{}",
        cluster.logs()
    );
    let addrs = cluster.addrs();

    runtime().block_on(async {
        // 1. topic 元数据（region 0）。
        let created = create_topic(&addrs, "orders", 2).await;
        assert_eq!(created.partition_count, 2);
        assert!(created.topic_id > 0);

        // 2. 分片 Leader 可见。
        let leader = wait_partition_leader(&addrs, "orders", 0).await;
        let follower = addrs
            .iter()
            .find(|addr| **addr != leader)
            .expect("a 3-node cluster has a non-leader")
            .clone();

        // 3. 非 Leader：UNAVAILABLE + NOT_LEADER + hint 指向真实 Leader。
        let mut raw = MqClient::new(connect(&follower).await);
        let err = raw
            .publish(mq_v2::PublishRequest {
                topic: "orders".to_string(),
                partition: 0,
                topic_epoch: created.topic_epoch,
                payload: b"to-follower".to_vec(),
                ..Default::default()
            })
            .await
            .expect_err("a shard follower must not accept writes");
        assert_eq!(err.code(), Code::Unavailable, "{err:?}");
        assert_eq!(
            error_code_of(&err).as_deref(),
            Some("NOT_LEADER"),
            "{err:?}"
        );
        let hint = err
            .metadata()
            .get(LEADER_HINT)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        assert_eq!(
            hint.as_deref(),
            Some(leader.as_str()),
            "trailer must point at the shard leader: {err:?}"
        );

        // 4. Rust 客户端：按分区缓存 Leader，NOT_LEADER 自动重路由。
        let client = coord_client::Client::new(coord_client::Config::new(addrs.clone()))
            .await
            .expect("coord client");
        let mq = client.mq_v2();
        let first = mq
            .publish(&coord_client::MqPublish {
                topic: "orders".to_string(),
                partition: 0,
                topic_epoch: created.topic_epoch,
                payload: b"via-client".to_vec(),
                ..Default::default()
            })
            .await
            .expect("client publish must reroute to the shard leader");
        assert_eq!(first.offset, 0);
        assert!(!first.deduplicated);
        let second = mq
            .publish(&coord_client::MqPublish {
                topic: "orders".to_string(),
                partition: 0,
                topic_epoch: created.topic_epoch,
                payload: b"via-client-2".to_vec(),
                idempotency_key: "cluster-idem".to_string(),
                ..Default::default()
            })
            .await
            .expect("second client publish");
        assert_eq!(second.offset, 1);
        let replay = mq
            .publish(&coord_client::MqPublish {
                topic: "orders".to_string(),
                partition: 0,
                topic_epoch: created.topic_epoch,
                payload: b"via-client-2".to_vec(),
                idempotency_key: "cluster-idem".to_string(),
                ..Default::default()
            })
            .await
            .expect("idempotent replay");
        assert!(replay.deduplicated);
        assert_eq!(replay.offset, second.offset);

        // 5. Agent 代理：services.mq=true 在 v2 上启动并转发 coord.mq.v2。
        let mut agent = start_agent(&addrs[0]);
        let agent_addr = agent.addr.clone();
        wait_tcp_ready(&agent_addr, Duration::from_secs(60)).await;
        assert_agent_alive(&mut agent);
        let mut via_agent = MqClient::new(connect(&agent_addr).await);
        let agent_topic = via_agent
            .create_topic(mq_v2::CreateTopicRequest {
                topic: "agent-orders".to_string(),
                partitions: 1,
                request_id: String::new(),
                config: None,
            })
            .await
            .expect("CreateTopic through the agent")
            .into_inner();
        let published = via_agent
            .publish(mq_v2::PublishRequest {
                topic: "agent-orders".to_string(),
                partition: 0,
                topic_epoch: agent_topic.topic_epoch,
                key: b"agent-key".to_vec(),
                payload: b"via-agent".to_vec(),
                ..Default::default()
            })
            .await
            .expect("Publish through the agent")
            .into_inner();
        assert_eq!(published.offset, 0, "new topic ⇒ first offset");
        assert_agent_alive(&mut agent);
    });

    cluster.assert_all_alive();
}
