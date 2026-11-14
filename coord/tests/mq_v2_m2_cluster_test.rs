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

        // 6. 读面/组协调/DLQ/管理面同样经 Agent 代理（M3/M4 的 RPC 不再 `UNIMPLEMENTED`）：
        //    Java SDK 走 Agent 通道，所以这条路径就是它的 wire 判据。
        let described = via_agent
            .describe_topic(mq_v2::DescribeTopicRequest {
                topic: "agent-orders".to_string(),
            })
            .await
            .expect("DescribeTopic through the agent")
            .into_inner();
        assert_eq!(described.partition_count, 1);
        assert_eq!(described.topic_epoch, agent_topic.topic_epoch);
        assert_eq!(described.partitions.len(), 1);
        assert_eq!(described.partitions[0].next_offset, 1);

        let listed = via_agent
            .list_topics(mq_v2::ListTopicsRequest {
                prefix: "agent-".to_string(),
                page_size: 10,
                page_token: String::new(),
            })
            .await
            .expect("ListTopics through the agent")
            .into_inner();
        assert_eq!(listed.topics.len(), 1);
        assert_eq!(listed.topics[0].topic, "agent-orders");

        let joined = via_agent
            .join_group(mq_v2::JoinGroupRequest {
                group: "agent-g".to_string(),
                member_id: String::new(),
                topics: vec!["agent-orders".to_string()],
                session_timeout_ms: 0,
            })
            .await
            .expect("JoinGroup through the agent")
            .into_inner();
        assert!(!joined.member_id.is_empty());
        assert_eq!(joined.assignments.len(), 1);
        assert_eq!(joined.assignments[0].committed_offset, 0);

        let polled = via_agent
            .poll(mq_v2::PollRequest {
                topic: "agent-orders".to_string(),
                partition: 0,
                topic_epoch: agent_topic.topic_epoch,
                start_offset: Some(0),
                max_messages: 10,
                max_bytes: 0,
                wait_ms: 0,
            })
            .await
            .expect("Poll through the agent")
            .into_inner();
        assert_eq!(polled.messages.len(), 1);
        assert_eq!(polled.messages[0].payload, b"via-agent");
        assert_eq!(polled.messages[0].key, b"agent-key");

        let acked = via_agent
            .ack(mq_v2::AckRequest {
                topic: "agent-orders".to_string(),
                partition: 0,
                topic_epoch: agent_topic.topic_epoch,
                group: "agent-g".to_string(),
                member_id: joined.member_id.clone(),
                generation: joined.generation,
                offset: 0,
            })
            .await
            .expect("Ack through the agent")
            .into_inner();
        assert_eq!(acked.committed_offset, 1);

        let committed = via_agent
            .get_committed_offsets(mq_v2::GetCommittedOffsetsRequest {
                group: "agent-g".to_string(),
                partitions: Vec::new(),
            })
            .await
            .expect("GetCommittedOffsets through the agent")
            .into_inner();
        assert_eq!(committed.offsets.len(), 1);
        assert!(committed.offsets[0].committed);
        assert_eq!(committed.offsets[0].offset, 1);

        let group = via_agent
            .describe_group(mq_v2::DescribeGroupRequest {
                group: "agent-g".to_string(),
            })
            .await
            .expect("DescribeGroup through the agent")
            .into_inner();
        assert_eq!(group.group, "agent-g");
        assert_eq!(group.members.len(), 1);
        assert_eq!(group.partitions.len(), 1);
        assert_eq!(group.partitions[0].committed_offset, 1);
        assert_eq!(group.partitions[0].lag, 0);

        let groups = via_agent
            .list_groups(mq_v2::ListGroupsRequest {
                prefix: "agent-".to_string(),
                page_size: 10,
                page_token: String::new(),
            })
            .await
            .expect("ListGroups through the agent")
            .into_inner();
        assert_eq!(groups.groups, vec!["agent-g".to_string()]);

        let heartbeat = via_agent
            .heartbeat(mq_v2::HeartbeatRequest {
                group: "agent-g".to_string(),
                member_id: joined.member_id.clone(),
                generation: joined.generation,
            })
            .await
            .expect("Heartbeat through the agent")
            .into_inner();
        assert_eq!(heartbeat.generation, joined.generation);
        assert!(!heartbeat.rebalance_required);

        let empty_dlq = via_agent
            .poll_dlq(mq_v2::PollDlqRequest {
                topic: "agent-orders".to_string(),
                partition: 0,
                topic_epoch: agent_topic.topic_epoch,
                start_offset: None,
                max_messages: 10,
            })
            .await
            .expect("PollDlq through the agent")
            .into_inner();
        assert!(empty_dlq.messages.is_empty());

        let moved = via_agent
            .move_to_dlq(mq_v2::MoveToDlqRequest {
                topic: "agent-orders".to_string(),
                partition: 0,
                topic_epoch: agent_topic.topic_epoch,
                offset: 0,
                reason: "poison".to_string(),
                detail: "proxied move".to_string(),
            })
            .await
            .expect("MoveToDlq through the agent")
            .into_inner();
        assert_eq!(moved.dlq_offset, 0);
        assert!(!moved.already_moved);

        let dlq = via_agent
            .poll_dlq(mq_v2::PollDlqRequest {
                topic: "agent-orders".to_string(),
                partition: 0,
                topic_epoch: agent_topic.topic_epoch,
                start_offset: None,
                max_messages: 10,
            })
            .await
            .expect("PollDlq after move")
            .into_inner();
        assert_eq!(dlq.messages.len(), 1);
        assert_eq!(dlq.messages[0].offset, 0, "DLQ keeps the original offset");
        assert_eq!(dlq.messages[0].dlq_reason, "poison");
        assert_eq!(dlq.messages[0].dlq_detail, "proxied move");

        let updated = via_agent
            .update_topic(mq_v2::UpdateTopicRequest {
                topic: "agent-orders".to_string(),
                expected_topic_epoch: agent_topic.topic_epoch,
                config: Some(mq_v2::TopicConfig {
                    retention_secs: 7200,
                    ..Default::default()
                }),
            })
            .await
            .expect("UpdateTopic through the agent")
            .into_inner();
        assert_eq!(
            updated.config.as_ref().map(|config| config.retention_secs),
            Some(7200)
        );

        let doomed = via_agent
            .create_topic(mq_v2::CreateTopicRequest {
                topic: "agent-doomed".to_string(),
                partitions: 1,
                request_id: String::new(),
                config: None,
            })
            .await
            .expect("create doomed topic")
            .into_inner();
        let deleted = via_agent
            .delete_topic(mq_v2::DeleteTopicRequest {
                topic: "agent-doomed".to_string(),
                expected_topic_epoch: doomed.topic_epoch,
            })
            .await
            .expect("DeleteTopic through the agent")
            .into_inner();
        assert!(!deleted.delete_id.is_empty());
        let deletion = via_agent
            .get_topic_deletion(mq_v2::GetTopicDeletionRequest {
                delete_id: deleted.delete_id.clone(),
            })
            .await
            .expect("GetTopicDeletion through the agent")
            .into_inner();
        assert_eq!(deletion.delete_id, deleted.delete_id);
        assert_eq!(deletion.topic, "agent-doomed");

        via_agent
            .leave_group(mq_v2::LeaveGroupRequest {
                group: "agent-g".to_string(),
                member_id: joined.member_id,
                generation: joined.generation,
            })
            .await
            .expect("LeaveGroup through the agent");
        assert_agent_alive(&mut agent);
    });

    cluster.assert_all_alive();
}
