//! MQ v2 M6-4：真实 3 进程集群上的故障矩阵（ADR-0016 D13）。
//!
//! 覆盖：
//! 1. 日志分片 Leader `kill -9` 后继续发布：offset 唯一、连续、不复用，重启后
//!    仍可读回；
//! 2. 消费组 shard Leader `kill -9` 后 Ack 重试：committed 单调、不丢已 Ack；
//! 3. 配额打满：整批原子拒绝，已提交消息不受影响、offset 不出现空洞；
//! 4. follower 对称网络分区：多数派仍可写，heal 后不产生重复/回退 offset。
//!
//! 运行（debug 构建，需 `COORD_TEST_RAFT_PARTITION_FILE` 分区钩子）：
//!   `cargo test -p coord --test mq_v2_m6_chaos_test -- --ignored --nocapture --test-threads=1`
//!
//! 故障矩阵只在 CI chaos job 与本地显式 `--ignored` 时执行；未跑项在
//! `docs/production/ops/mq-v2-performance.md` §7 与 gate M6-4 里明说。

use std::time::{Duration, Instant};

#[path = "common/mod.rs"]
mod common;

use common::v2_cluster::{assert_no_forbidden_anchors, runtime, V2Cluster, V2ClusterOptions};
use coord_client::{Client, Config as ClientConfig, MqAck, MqJoinGroup, MqPoll, MqPublish};
use coord_proto::mq::v2 as mq_v2;
use coord_proto::mq::v2::mq_client::MqClient;
use tonic::transport::Channel;

const FAILOVER: Duration = Duration::from_secs(30);

fn cluster() -> V2Cluster {
    V2Cluster::start_with(V2ClusterOptions {
        // 分区测试依赖 debug 构建的 raft-RPC 分区钩子；release 下该选项无效。
        partitionable: true,
        toml: "[mq_v2]\nenabled = true\nlog_shards = 2\ngroup_shards = 1\n",
        ..Default::default()
    })
}

async fn connect(addr: &str) -> Channel {
    Channel::from_shared(format!("http://{addr}"))
        .expect("endpoint")
        .connect_timeout(Duration::from_secs(5))
        .connect()
        .await
        .unwrap_or_else(|err| panic!("connect {addr}: {err}"))
}

async fn create_topic(
    addrs: &[String],
    topic: &str,
    partitions: i32,
    config: Option<mq_v2::TopicConfig>,
) -> mq_v2::CreateTopicResponse {
    let deadline = Instant::now() + FAILOVER;
    let mut last: Option<String> = None;
    loop {
        for addr in addrs {
            let Ok(channel) = Channel::from_shared(format!("http://{addr}"))
                .expect("endpoint")
                .connect_timeout(Duration::from_secs(2))
                .connect()
                .await
            else {
                continue;
            };
            let mut client = MqClient::new(channel);
            match client
                .create_topic(mq_v2::CreateTopicRequest {
                    topic: topic.to_string(),
                    partitions,
                    request_id: String::new(),
                    config: config.clone(),
                })
                .await
            {
                Ok(response) => return response.into_inner(),
                Err(status) => last = Some(format!("{addr}: {status}")),
            }
        }
        assert!(
            Instant::now() < deadline,
            "CreateTopic never reached the metadata leader; last error: {}",
            last.unwrap_or_default()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_partition_leader(addrs: &[String], topic: &str, partition: i32) -> String {
    let deadline = Instant::now() + FAILOVER;
    loop {
        for addr in addrs {
            // 被 kill 的节点仍在地址列表里：连接失败就试下一个。
            let Ok(channel) = Channel::from_shared(format!("http://{addr}"))
                .expect("endpoint")
                .connect_timeout(Duration::from_secs(2))
                .connect()
                .await
            else {
                continue;
            };
            let mut client = MqClient::new(channel);
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
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn node_index(cluster: &V2Cluster, addr: &str) -> usize {
    cluster
        .nodes
        .iter()
        .position(|node| node.addr() == addr)
        .unwrap_or_else(|| panic!("leader addr {addr} does not belong to the cluster"))
}

/// 重试到成功：故障窗口内的 NOT_LEADER/UNAVAILABLE 由 SDK 刷新 Leader 后吸收。
async fn publish_until_ok(
    client: &Client,
    request: &MqPublish,
    deadline: Duration,
) -> coord_client::MqPublishResult {
    let mq = client.mq_v2();
    let stop = Instant::now() + deadline;
    let mut last: Option<String> = None;
    loop {
        match mq.publish(request).await {
            Ok(result) => return result,
            Err(error) => last = Some(error.to_string()),
        }
        assert!(
            Instant::now() < stop,
            "publish never succeeded within {deadline:?}; last error: {}",
            last.as_deref().unwrap_or("")
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[test]
#[ignore = "real-process fault injection; run by the CI chaos job"]
fn chaos_mq_v2_shard_leader_kill9_preserves_offsets() {
    let _ = runtime();
    let mut cluster = cluster();
    let addrs = cluster.addrs();

    runtime().block_on(async {
        let created = create_topic(&addrs, "chaos-shard", 1, None).await;
        let leader = wait_partition_leader(&addrs, "chaos-shard", 0).await;
        let client = Client::new(ClientConfig::new(addrs.clone()))
            .await
            .expect("coord client");
        let mq = client.mq_v2();

        let mut offsets = Vec::new();
        for index in 0..5u64 {
            let response = mq
                .publish(&MqPublish {
                    topic: "chaos-shard".to_string(),
                    partition: 0,
                    topic_epoch: created.topic_epoch,
                    payload: b"before-fault".to_vec(),
                    idempotency_key: format!("before-{index}"),
                    ..Default::default()
                })
                .await
                .expect("warmup publish");
            offsets.push(response.offset);
        }

        // 故障被执行证明：被 kill 的正是当前分片 Leader。
        let leader_index = node_index(&cluster, &leader);
        cluster.nodes[leader_index].kill9();
        assert!(
            cluster.nodes[leader_index].child.is_none(),
            "the killed shard leader process must be gone"
        );

        // Jepsen 风格的样本门槛：故障窗口内至少 100 次发布，offset 全部入样本。
        for index in 0..100u64 {
            let response = publish_until_ok(
                &client,
                &MqPublish {
                    topic: "chaos-shard".to_string(),
                    partition: 0,
                    topic_epoch: created.topic_epoch,
                    payload: b"during-fault".to_vec(),
                    idempotency_key: format!("during-{index}"),
                    ..Default::default()
                },
                FAILOVER,
            )
            .await;
            offsets.push(response.offset);
        }

        // 重启被 kill 的节点（同一数据目录），新写入仍落在同一分区日志。
        cluster.nodes[leader_index].start(false);
        let new_leader = wait_partition_leader(&addrs, "chaos-shard", 0).await;
        assert!(!new_leader.is_empty());

        let response = mq
            .publish(&MqPublish {
                topic: "chaos-shard".to_string(),
                partition: 0,
                topic_epoch: created.topic_epoch,
                payload: b"after-restart".to_vec(),
                idempotency_key: "after-restart".to_string(),
                ..Default::default()
            })
            .await
            .expect("publish after restart");
        offsets.push(response.offset);

        let mut sorted = offsets.clone();
        sorted.sort_unstable();
        assert_eq!(
            sorted,
            (0..offsets.len() as u64).collect::<Vec<_>>(),
            "offsets must stay unique and contiguous across leader failover"
        );
        assert!(
            offsets.len() >= 100,
            "fault matrix must sample at least 100 publishes (got {})",
            offsets.len()
        );

        // 全部分区消息仍可按 offset 读回（含被 kill 的 leader 上已提交的部分）。
        let polled = mq
            .poll(&MqPoll {
                topic: "chaos-shard".to_string(),
                partition: 0,
                topic_epoch: created.topic_epoch,
                start_offset: Some(0),
                max_messages: 1000,
                ..Default::default()
            })
            .await
            .expect("poll after failover");
        assert_eq!(
            polled.messages.len(),
            offsets.len(),
            "every committed message must survive the shard leader kill"
        );
    });

    assert_no_forbidden_anchors(&cluster);
}

#[test]
#[ignore = "real-process fault injection; run by the CI chaos job"]
fn chaos_mq_v2_group_leader_kill9_keeps_committed_monotonic() {
    let _ = runtime();
    let mut cluster = cluster();
    let addrs = cluster.addrs();

    runtime().block_on(async {
        let created = create_topic(&addrs, "chaos-group", 1, None).await;
        let client = Client::new(ClientConfig::new(addrs.clone()))
            .await
            .expect("coord client");
        let mq = client.mq_v2();
        for index in 0..10u64 {
            mq.publish(&MqPublish {
                topic: "chaos-group".to_string(),
                partition: 0,
                topic_epoch: created.topic_epoch,
                payload: format!("m-{index}").into_bytes(),
                idempotency_key: format!("group-{index}"),
                ..Default::default()
            })
            .await
            .expect("publish for group chaos");
        }

        // group shard Leader 用原始客户端逐个探测（非 Leader 返回 NOT_LEADER）。
        let group = "chaos-group-g";
        let mut group_leader = None;
        let deadline = Instant::now() + FAILOVER;
        // 探测用独立的 group（`group_shards = 1` ⇒ 同一个组 shard），
        // 避免探测用的 Join 先占掉真实组的唯一分区。
        while group_leader.is_none() {
            for addr in &addrs {
                let mut raw = MqClient::new(connect(addr).await);
                if raw
                    .join_group(mq_v2::JoinGroupRequest {
                        group: format!("{group}-probe"),
                        member_id: String::new(),
                        topics: vec!["chaos-group".to_string()],
                        session_timeout_ms: 0,
                    })
                    .await
                    .is_ok()
                {
                    group_leader = Some(addr.clone());
                    break;
                }
            }
            assert!(
                Instant::now() < deadline,
                "group shard leader never accepted JoinGroup"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let group_leader = group_leader.expect("group leader");

        let joined = mq
            .join_group(&MqJoinGroup {
                group: group.to_string(),
                member_id: String::new(),
                topics: vec!["chaos-group".to_string()],
                session_timeout_ms: 0,
            })
            .await
            .expect("join group");

        let ack = |offset: u64| MqAck {
            topic: "chaos-group".to_string(),
            partition: 0,
            topic_epoch: created.topic_epoch,
            offset,
        };
        for offset in 0..5u64 {
            mq.ack(group, &joined.member_id, joined.generation, &ack(offset))
                .await
                .expect("warmup ack");
        }

        let group_leader_index = node_index(&cluster, &group_leader);
        cluster.nodes[group_leader_index].kill9();

        // 代际/复本切换窗口内的 Ack 自动重试；已提交位点只能前进。
        let stop = Instant::now() + FAILOVER;
        for offset in 5..10u64 {
            loop {
                match mq
                    .ack(group, &joined.member_id, joined.generation, &ack(offset))
                    .await
                {
                    Ok(result) => {
                        assert!(result >= offset + 1, "committed must not go backwards");
                        break;
                    }
                    Err(error) => assert!(
                        Instant::now() < stop,
                        "ack({offset}) never succeeded after group leader kill: {error}"
                    ),
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }

        let committed = mq
            .get_committed_offsets(group, &[("chaos-group".to_string(), 0)])
            .await
            .expect("committed offsets after failover");
        assert_eq!(
            committed[0].offset, 10,
            "committed must cover every acked offset after the group leader kill"
        );

        // 重启组 Leader，位点必须仍在。
        cluster.nodes[group_leader_index].start(false);
        tokio::time::sleep(Duration::from_millis(500)).await;
        let committed = mq
            .get_committed_offsets(group, &[("chaos-group".to_string(), 0)])
            .await
            .expect("committed offsets after restart");
        assert_eq!(
            committed[0].offset, 10,
            "committed offsets must survive the group shard leader restart"
        );
    });

    assert_no_forbidden_anchors(&cluster);
}

#[test]
#[ignore = "real-process fault injection; run by the CI chaos job"]
fn chaos_mq_v2_quota_full_rejects_the_whole_batch() {
    let _ = runtime();
    let cluster = cluster();
    let addrs = cluster.addrs();

    runtime().block_on(async {
        // 4 KiB 配额 + 1 KiB 单条：第 5 条开始必然超配额。
        let created = create_topic(
            &addrs,
            "chaos-quota",
            1,
            Some(mq_v2::TopicConfig {
                retention_secs: 3600,
                max_bytes: 4096,
                max_message_bytes: 1024,
                ..Default::default()
            }),
        )
        .await;
        let client = Client::new(ClientConfig::new(addrs.clone()))
            .await
            .expect("coord client");
        let mq = client.mq_v2();
        let payload = vec![b'x'; 700];

        let mut offsets = Vec::new();
        for index in 0..3u64 {
            let response = mq
                .publish(&MqPublish {
                    topic: "chaos-quota".to_string(),
                    partition: 0,
                    topic_epoch: created.topic_epoch,
                    payload: payload.clone(),
                    idempotency_key: format!("quota-{index}"),
                    ..Default::default()
                })
                .await
                .expect("publish within quota");
            offsets.push(response.offset);
        }

        // 批量发布跨过配额：整批拒绝，不产生部分提交。
        let mut batch_entries = Vec::new();
        for index in 0..3u64 {
            batch_entries.push(coord_client::MqBatchEntry {
                payload: vec![b'y'; 900],
                idempotency_key: format!("quota-batch-{index}"),
                ..Default::default()
            });
        }
        let error = mq
            .publish_batch(&coord_client::MqPublishBatch {
                topic: "chaos-quota".to_string(),
                partition: 0,
                topic_epoch: created.topic_epoch,
                entries: batch_entries,
            })
            .await
            .expect_err("batch over quota must be rejected");
        let error = error.to_string();
        assert!(
            error.contains("QUOTA_EXCEEDED") || error.contains("RESOURCE_EXHAUSTED"),
            "unexpected quota rejection: {error}"
        );

        // 配额拒绝后 offset 不出现空洞：下一条仍从 3 开始。
        let response = mq
            .publish(&MqPublish {
                topic: "chaos-quota".to_string(),
                partition: 0,
                topic_epoch: created.topic_epoch,
                payload: b"after-quota".to_vec(),
                idempotency_key: "after-quota".to_string(),
                ..Default::default()
            })
            .await
            .expect("publish after quota rejection");
        assert_eq!(
            response.offset, 3,
            "a rejected batch must not allocate offsets"
        );
        offsets.push(response.offset);

        let polled = mq
            .poll(&MqPoll {
                topic: "chaos-quota".to_string(),
                partition: 0,
                topic_epoch: created.topic_epoch,
                start_offset: Some(0),
                max_messages: 100,
                ..Default::default()
            })
            .await
            .expect("poll after quota rejection");
        assert_eq!(
            polled.messages.len(),
            4,
            "quota rejection must not hide or truncate committed messages"
        );
        assert_eq!(offsets, vec![0, 1, 2, 3]);
    });

    assert_no_forbidden_anchors(&cluster);
}

#[test]
#[ignore = "real-process fault injection; run by the CI chaos job"]
fn chaos_mq_v2_follower_partition_keeps_the_majority_writable() {
    let _ = runtime();
    let cluster = cluster();
    let addrs = cluster.addrs();

    runtime().block_on(async {
        let created = create_topic(&addrs, "chaos-partition", 1, None).await;
        let leader = wait_partition_leader(&addrs, "chaos-partition", 0).await;
        let follower = addrs
            .iter()
            .find(|addr| **addr != leader)
            .expect("3-node cluster has a follower")
            .clone();
        let follower_id = cluster.nodes[node_index(&cluster, &follower)].id;
        let client = Client::new(ClientConfig::new(addrs.clone()))
            .await
            .expect("coord client");
        let mq = client.mq_v2();

        for index in 0..5u64 {
            mq.publish(&MqPublish {
                topic: "chaos-partition".to_string(),
                partition: 0,
                topic_epoch: created.topic_epoch,
                payload: b"before-partition".to_vec(),
                idempotency_key: format!("partition-before-{index}"),
                ..Default::default()
            })
            .await
            .expect("publish before partition");
        }

        // 隔离 follower（不是 Leader）：2/3 多数派仍可提交。
        cluster.partition(follower_id);
        for index in 0..5u64 {
            let _ = publish_until_ok(
                &client,
                &MqPublish {
                    topic: "chaos-partition".to_string(),
                    partition: 0,
                    topic_epoch: created.topic_epoch,
                    payload: b"during-partition".to_vec(),
                    idempotency_key: format!("partition-during-{index}"),
                    ..Default::default()
                },
                FAILOVER,
            )
            .await;
        }

        cluster.heal(follower_id);
        // heal 后 follower 追平：再写一条，并确认 11 条都可读且 offset 连续。
        let response = mq
            .publish(&MqPublish {
                topic: "chaos-partition".to_string(),
                partition: 0,
                topic_epoch: created.topic_epoch,
                payload: b"after-heal".to_vec(),
                idempotency_key: "partition-after-heal".to_string(),
                ..Default::default()
            })
            .await
            .expect("publish after heal");
        assert_eq!(response.offset, 10);

        let polled = mq
            .poll(&MqPoll {
                topic: "chaos-partition".to_string(),
                partition: 0,
                topic_epoch: created.topic_epoch,
                start_offset: Some(0),
                max_messages: 100,
                ..Default::default()
            })
            .await
            .expect("poll after heal");
        let offsets: Vec<u64> = polled.messages.iter().map(|m| m.offset).collect();
        assert_eq!(offsets, (0..11).collect::<Vec<_>>());
    });

    assert_no_forbidden_anchors(&cluster);
}
