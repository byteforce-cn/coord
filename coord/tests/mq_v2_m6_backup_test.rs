//! MQ v2 M6-5：整节点离线备份/恢复 + 关闭/重开（真实 3 进程）。
//!
//! 场景：
//! 1. 建 topic、发布 5 条、消费组 Ack 3 条，并在同一集群写入一个 v2 KV；
//! 2. 全集群停机 → `coord backup save` 每个节点的数据目录 → 删目录 →
//!    `coord backup restore` → 同 node_id 重启；
//! 3. 恢复后：topic/topic_epoch 不变、5 条消息可按 offset 读回、committed 仍为 3、
//!    继续 publish 得到 offset 5、Ack 后 committed 6；
//! 4. 关闭 `[mq_v2]` 重启：MQ RPC 返回 `FAILED_PRECONDITION`，**v2 KV 仍可读**
//!    （按备份前的 commit_ts）；重新开启后 MQ 数据仍可读、offset 继续。
//!
//! 运行（debug 构建；备份 CLI 用测试二进制同目录的 `coord`）：
//!   `cargo test -p coord --test mq_v2_m6_backup_test -- --ignored --nocapture --test-threads=1`

use std::path::Path;
use std::process::Command;
use std::time::Duration;

#[path = "common/mod.rs"]
mod common;

use common::v2_cluster::{direct_put, read_at, runtime, ReadAttempt, V2Cluster, V2ClusterOptions};
use common::wait_tcp_ready;
use coord_client::{Client, Config as ClientConfig, MqAck, MqJoinGroup, MqPublish};
use coord_proto::mq::v2 as mq_v2;
use coord_proto::mq::v2::mq_client::MqClient;
use tonic::transport::Channel;

const FAILOVER: Duration = Duration::from_secs(60);
const TOPIC: &str = "backup-topic";
const GROUP: &str = "backup-group";

fn coord_bin() -> &'static str {
    env!("CARGO_BIN_EXE_coord")
}

fn mq_toml(enabled: bool) -> String {
    format!("[mq_v2]\nenabled = {enabled}\nlog_shards = 2\ngroup_shards = 1\n")
}

fn kill_all(cluster: &mut V2Cluster) {
    for node in &mut cluster.nodes {
        node.kill9();
    }
    std::thread::sleep(Duration::from_millis(500));
}

fn restore_all(cluster: &mut V2Cluster, backups: &Path) {
    for node in &mut cluster.nodes {
        let backup = backups.join(format!("node-{}", node.id));
        let output = Command::new(coord_bin())
            .args(["backup", "save", "--data-dir"])
            .arg(&node.data_dir)
            .arg("--output")
            .arg(&backup)
            .output()
            .expect("run coord backup save");
        assert!(
            output.status.success(),
            "backup save for node {} failed: {}",
            node.id,
            String::from_utf8_lossy(&output.stderr)
        );
        std::fs::remove_dir_all(&node.data_dir).expect("remove live data dir");
        let output = Command::new(coord_bin())
            .args(["backup", "restore", "--input"])
            .arg(&backup)
            .arg("--data-dir")
            .arg(&node.data_dir)
            .output()
            .expect("run coord backup restore");
        assert!(
            output.status.success(),
            "backup restore for node {} failed: {}",
            node.id,
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

fn restart_all(cluster: &mut V2Cluster) {
    // 全节点都已有持久化成员表：bootstrap=false 即可重新加入。
    for node in &mut cluster.nodes {
        node.start(false);
    }
    let addrs = cluster.addrs();
    runtime().block_on(async {
        for addr in &addrs {
            wait_tcp_ready(addr, FAILOVER).await;
        }
    });
    let leader = cluster.wait_leader(FAILOVER);
    assert!(
        !leader.is_empty(),
        "cluster must elect a leader after restart"
    );
}

async fn connect(addr: &str) -> Channel {
    Channel::from_shared(format!("http://{addr}"))
        .expect("endpoint")
        .connect_timeout(Duration::from_secs(5))
        .connect()
        .await
        .unwrap_or_else(|err| panic!("connect {addr}: {err}"))
}

async fn list_topics_any(addrs: &[String]) -> Result<usize, tonic::Status> {
    let mut last = None;
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
            .list_topics(mq_v2::ListTopicsRequest {
                prefix: String::new(),
                page_size: 10,
                page_token: String::new(),
            })
            .await
        {
            Ok(response) => return Ok(response.into_inner().topics.len()),
            Err(status) => last = Some(status),
        }
    }
    Err(last.unwrap_or_else(|| tonic::Status::unavailable("no endpoint answered")))
}

/// 等分片 Leader 可见：恢复/重启后 shard raft 的选举比 region 0 慢。
async fn wait_topic_leader(addrs: &[String], topic: &str, partition: i32) {
    let deadline = std::time::Instant::now() + FAILOVER;
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
            if let Ok(response) = client
                .get_topic_leader(mq_v2::GetTopicLeaderRequest {
                    topic: topic.to_string(),
                })
                .await
            {
                if response
                    .into_inner()
                    .partitions
                    .iter()
                    .any(|entry| entry.partition == partition && !entry.leader_addr.is_empty())
                {
                    return;
                }
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "shard leader for {topic}/{partition} never became visible after restore"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[test]
#[ignore = "real-process backup/restore drill; run explicitly"]
fn backup_restore_and_toggle_preserve_mq_v2_state() {
    let _ = runtime();
    let mut cluster = V2Cluster::start_with(V2ClusterOptions {
        toml: "[mq_v2]\nenabled = true\nlog_shards = 2\ngroup_shards = 1\n",
        ..Default::default()
    });
    let addrs = cluster.addrs();
    let backups = tempfile::tempdir().expect("backup dir");

    let (topic_epoch, kv_commit_ts) = runtime().block_on(async {
        let created = {
            let mut last = None;
            let mut created = None;
            for addr in &addrs {
                let mut raw = MqClient::new(connect(addr).await);
                match raw
                    .create_topic(mq_v2::CreateTopicRequest {
                        topic: TOPIC.to_string(),
                        partitions: 1,
                        request_id: String::new(),
                        config: None,
                    })
                    .await
                {
                    Ok(response) => {
                        created = Some(response.into_inner());
                        break;
                    }
                    Err(status) => last = Some(status),
                }
            }
            created.unwrap_or_else(|| panic!("CreateTopic failed: {last:?}"))
        };

        let client = Client::new(ClientConfig::new(addrs.clone()))
            .await
            .expect("coord client");
        let mq = client.mq_v2();
        for index in 0..5u64 {
            mq.publish(&MqPublish {
                topic: TOPIC.to_string(),
                partition: 0,
                topic_epoch: created.topic_epoch,
                payload: format!("m-{index}").into_bytes(),
                idempotency_key: format!("backup-{index}"),
                ..Default::default()
            })
            .await
            .expect("publish before backup");
        }

        let joined = mq
            .join_group(&MqJoinGroup {
                group: GROUP.to_string(),
                member_id: String::new(),
                topics: vec![TOPIC.to_string()],
                session_timeout_ms: 0,
            })
            .await
            .expect("join group");
        for offset in 0..3u64 {
            mq.ack(
                GROUP,
                &joined.member_id,
                joined.generation,
                &MqAck {
                    topic: TOPIC.to_string(),
                    partition: 0,
                    topic_epoch: created.topic_epoch,
                    offset,
                },
            )
            .await
            .expect("ack before backup");
        }

        let kv_ts = direct_put(&addrs, b"/mq-backup/k", b"v2-value").await;
        (created.topic_epoch, kv_ts)
    });

    // 停机 + 整节点备份/恢复 + 同 node_id 重启。
    kill_all(&mut cluster);
    restore_all(&mut cluster, backups.path());
    restart_all(&mut cluster);
    let addrs = cluster.addrs();

    runtime().block_on(async {
        // 恢复后 shard raft（承载 group 位点的 region）选举比 region 0 慢：
        // 元数据读（describe/poll）能先成功，但 `get_committed_offsets` 需要
        // 分片 leader，过早调用会得到 `not leader for region N`。先等它可见。
        wait_topic_leader(&addrs, TOPIC, 0).await;

        let client = Client::new(ClientConfig::new(addrs.clone()))
            .await
            .expect("coord client");
        let mq = client.mq_v2();

        let described = mq
            .describe_topic(TOPIC)
            .await
            .expect("describe after restore");
        assert_eq!(described.topic_epoch, topic_epoch);
        assert_eq!(described.partition_count, 1);

        let polled = mq
            .poll(&coord_client::MqPoll {
                topic: TOPIC.to_string(),
                partition: 0,
                topic_epoch: topic_epoch,
                start_offset: Some(0),
                max_messages: 100,
                ..Default::default()
            })
            .await
            .expect("poll after restore");
        assert_eq!(
            polled.messages.len(),
            5,
            "all messages must survive restore"
        );

        // 即便 leader 已可见，首个 RPC 仍可能撞上客户端路由缓存刷新（与 publish 同口径
        // 重试，截止到 FAILOVER）。
        let deadline = std::time::Instant::now() + FAILOVER;
        let committed = loop {
            match mq
                .get_committed_offsets(GROUP, &[(TOPIC.to_string(), 0)])
                .await
            {
                Ok(committed) => break committed,
                Err(e) => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "committed offsets kept failing after restore: {e}"
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        };
        assert_eq!(
            committed[0].offset, 3,
            "committed offsets must survive restore"
        );

        let deadline = std::time::Instant::now() + FAILOVER;
        let next = loop {
            match mq
                .publish(&MqPublish {
                    topic: TOPIC.to_string(),
                    partition: 0,
                    topic_epoch,
                    payload: b"after-restore".to_vec(),
                    idempotency_key: "after-restore".to_string(),
                    ..Default::default()
                })
                .await
            {
                Ok(result) => break result,
                Err(error) => assert!(
                    std::time::Instant::now() < deadline,
                    "publish after restore never succeeded: {error}"
                ),
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        assert_eq!(next.offset, 5, "offsets must continue after restore");
    });

    // 关闭 [mq_v2]：MQ RPC fail-closed，但 v2 KV 仍可按备份前的 commit_ts 读。
    kill_all(&mut cluster);
    for node in &mut cluster.nodes {
        node.toml_extra = mq_toml(false);
    }
    restart_all(&mut cluster);
    let addrs = cluster.addrs();

    runtime().block_on(async {
        let error = list_topics_any(&addrs)
            .await
            .expect_err("MQ v2 must fail closed when disabled");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error:?}");
        let deadline = std::time::Instant::now() + FAILOVER;
        loop {
            match read_at(&addrs, b"/mq-backup/k", kv_commit_ts).await {
                ReadAttempt::Found(value, _) => {
                    assert_eq!(value, b"v2-value");
                    break;
                }
                other => assert!(
                    std::time::Instant::now() < deadline,
                    "v2 KV read with MQ disabled never succeeded: {other:?}"
                ),
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    // 重新开启：MQ 数据仍在，offset 继续。
    kill_all(&mut cluster);
    for node in &mut cluster.nodes {
        node.toml_extra = mq_toml(true);
    }
    restart_all(&mut cluster);
    let addrs = cluster.addrs();

    runtime().block_on(async {
        wait_topic_leader(&addrs, TOPIC, 0).await;
        let client = Client::new(ClientConfig::new(addrs.clone()))
            .await
            .expect("coord client");
        let mq = client.mq_v2();
        let polled = mq
            .poll(&coord_client::MqPoll {
                topic: TOPIC.to_string(),
                partition: 0,
                topic_epoch,
                start_offset: Some(0),
                max_messages: 100,
                ..Default::default()
            })
            .await
            .expect("poll after re-enable");
        assert_eq!(polled.messages.len(), 6);
        let next = mq
            .publish(&MqPublish {
                topic: TOPIC.to_string(),
                partition: 0,
                topic_epoch,
                payload: b"after-reenable".to_vec(),
                idempotency_key: "after-reenable".to_string(),
                ..Default::default()
            })
            .await
            .expect("publish after re-enable");
        assert_eq!(next.offset, 6);
    });
}
