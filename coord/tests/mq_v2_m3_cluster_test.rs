//! MQ v2 M3-4 验收：真实 3 节点集群上的 Rust consumer helper（#335 / #285）。
//!
//! 判据（设计 §6 M3-4）：
//! - 多消费者按 range 分配分片消费，一个分区只有一个成员；
//! - 成员变化（加入/离开）后心跳发现代际过期 → 自动重新 Join，cursor 从已提交位点
//!   继续（不重复提交已 Ack 的消息）；
//! - `Poll` + `Ack` 是至少一次路径：重复投递可接受，但已提交位点不回退。

use std::time::{Duration, Instant};

#[path = "common/mod.rs"]
mod common;

use common::v2_cluster::{runtime, V2Cluster, V2ClusterOptions};
use coord_client::{MqConsumer, MqConsumerOptions, MqPublish, ResetPolicy};
use coord_proto::mq::v2 as mq_v2;
use coord_proto::mq::v2::mq_client::MqClient;
use tonic::transport::Channel;

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

fn options(group: &str) -> MqConsumerOptions {
    MqConsumerOptions {
        group: group.to_string(),
        member_id: String::new(),
        topics: vec!["orders".to_string()],
        session_timeout_ms: 1_000,
        reset_policy: ResetPolicy::Fail,
        max_messages: 10,
        max_bytes: 0,
        wait_ms: 0,
    }
}

fn partitions_of(consumer: &MqConsumer) -> Vec<i32> {
    let mut partitions: Vec<i32> = consumer
        .assignments()
        .into_iter()
        .map(|(_, partition)| partition)
        .collect();
    partitions.sort_unstable();
    partitions
}

#[test]
fn consumer_helper_rebalances_and_resumes_from_committed_offsets() {
    let _ = runtime();
    let cluster = V2Cluster::start_with(V2ClusterOptions {
        toml: "[mq_v2]\nenabled = true\nlog_shards = 2\ngroup_shards = 1\n",
        ..Default::default()
    });
    let addrs = cluster.addrs();

    runtime().block_on(async {
        let created = create_topic(&addrs, "orders", 2).await;
        assert_eq!(created.partition_count, 2);
        let client = coord_client::Client::new(coord_client::Config::new(addrs.clone()))
            .await
            .expect("coord client");
        let mq = client.mq_v2();

        // 单成员：同时拥有两个分区。
        let mut a = MqConsumer::join(mq.clone(), options("billing"))
            .await
            .expect("join consumer A");
        assert_eq!(partitions_of(&a), vec![0, 1]);

        // 第二个成员加入：代际推进，A 在下一次 poll/心跳后收敛到单个分区。
        let mut b = MqConsumer::join(mq.clone(), options("billing"))
            .await
            .expect("join consumer B");
        let deadline = Instant::now() + Duration::from_secs(15);
        while partitions_of(&a).len() != 1 {
            assert!(
                Instant::now() < deadline,
                "consumer A never observed the rebalance: {:?}",
                a.assignments()
            );
            let _ = a.poll().await.expect("poll A during rebalance");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let deadline = Instant::now() + Duration::from_secs(15);
        while partitions_of(&b).len() != 1 {
            assert!(
                Instant::now() < deadline,
                "consumer B never converged to a single partition: {:?}",
                b.assignments()
            );
            let _ = b.poll().await.expect("poll B during rebalance");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let a_partition = partitions_of(&a)[0];
        let b_partition = partitions_of(&b)[0];
        assert_ne!(
            a_partition, b_partition,
            "a partition must belong to exactly one member"
        );

        // 两个分区各写 3 条；各自只消费自己的分区并提交位点。
        let tag = |partition: i32, index: u32| format!("p{partition}-m{index}");
        for partition in [a_partition, b_partition] {
            for index in 0..3u32 {
                mq.publish(&MqPublish {
                    topic: "orders".to_string(),
                    partition,
                    topic_epoch: created.topic_epoch,
                    payload: tag(partition, index).into_bytes(),
                    ..Default::default()
                })
                .await
                .expect("publish");
            }
        }
        let mut acked_a = 0usize;
        let deadline = Instant::now() + Duration::from_secs(10);
        while acked_a < 3 {
            assert!(
                Instant::now() < deadline,
                "consumer A never read its messages"
            );
            for message in a.poll().await.expect("poll A") {
                assert_eq!(
                    message.partition, a_partition,
                    "A must only consume its assignment"
                );
                a.ack(&message).await.expect("ack A");
                acked_a += 1;
            }
        }
        let mut acked_b = 0usize;
        while acked_b < 3 {
            assert!(
                Instant::now() < deadline,
                "consumer B never read its messages"
            );
            for message in b.poll().await.expect("poll B") {
                assert_eq!(
                    message.partition, b_partition,
                    "B must only consume its assignment"
                );
                b.ack(&message).await.expect("ack B");
                acked_b += 1;
            }
        }

        // A 主动离组：B 重新 Join 后接管两个分区，并从已提交位点继续 —— 之前 Ack 过的
        // 消息不会再投递；新写入的 1 条能被读到。
        a.close().await.expect("close consumer A");
        mq.publish(&MqPublish {
            topic: "orders".to_string(),
            partition: a_partition,
            topic_epoch: created.topic_epoch,
            payload: b"after-rebalance".to_vec(),
            ..Default::default()
        })
        .await
        .expect("publish after rebalance");

        // 接管过程中**不能丢弃** poll 结果：新分区上的消息可能在任何一次 poll 到达。
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut delivered_after = Vec::new();
        loop {
            delivered_after.extend(b.poll().await.expect("poll B after takeover"));
            let took_over = partitions_of(&b).len() == 2;
            let saw_new_message = delivered_after
                .iter()
                .any(|message| message.payload == b"after-rebalance");
            if took_over && saw_new_message {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "consumer B never took over both partitions / read the new message: {:?}, \
                 delivered: {delivered_after:?}",
                b.assignments()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let reassigned: Vec<_> = delivered_after
            .iter()
            .filter(|message| message.partition == a_partition)
            .map(|message| (message.offset, message.payload.clone()))
            .collect();
        assert!(
            reassigned
                .iter()
                .any(|(_, payload)| payload == b"after-rebalance"),
            "the new message on the transferred partition must be delivered: {reassigned:?}"
        );
        assert!(
            reassigned.iter().all(|(offset, _)| *offset >= 3),
            "already acked offsets must not be redelivered: {reassigned:?}"
        );
        b.close().await.expect("close consumer B");
    });

    cluster.assert_all_alive();
}
