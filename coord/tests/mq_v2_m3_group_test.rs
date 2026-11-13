//! MQ v2 M3-2 验收（#335 / #285）：`Ack`/`AckBatch` 经 group shard 的 Raft 提交。
//!
//! 判据（设计 §6 M3-2）：
//! - 位点单调：重复/更旧的 Ack 不回退；
//! - 代际 fencing 在 apply 内：过期代际 ⇒ `GROUP_EPOCH_STALE`；
//! - 整批原子：任一条未分配给该成员 ⇒ `NOT_GROUP_MEMBER`，一条位点都不写；
//! - 未知 topic/partition 映射到稳定错误码。

#[path = "support/mq_v2_node.rs"]
mod mq_v2_node;

use coord_core::error_code::error_code_of;
use coord_proto::mq::v2 as mq_v2;
use coord_proto::mq::v2::mq_client::MqClient;
use mq_v2_node::{create_topic, publish, MqV2Node};
use tonic::transport::Channel;
use tonic::Code;

const TOPIC: &str = "orders";
const GROUP: &str = "billing";

fn wire_code(status: &tonic::Status) -> Option<String> {
    error_code_of(status)
}

async fn ack(
    client: &mut MqClient<Channel>,
    member_id: &str,
    generation: u64,
    partition: i32,
    offset: i64,
) -> Result<mq_v2::AckResponse, tonic::Status> {
    client
        .ack(mq_v2::AckRequest {
            topic: TOPIC.to_string(),
            partition,
            topic_epoch: 0,
            group: GROUP.to_string(),
            member_id: member_id.to_string(),
            generation,
            offset,
        })
        .await
        .map(|response| response.into_inner())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ack_advances_committed_and_is_monotonic() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    create_topic(&mut client, TOPIC, 2, None).await;
    for index in 0..3u32 {
        publish(&mut client, TOPIC, 0, format!("m-{index}").as_bytes(), "")
            .await
            .expect("publish");
    }
    let joined = node.join_direct(GROUP, "m1", TOPIC, 2).await;
    assert_eq!(joined.generation, 1);

    let first = ack(&mut client, &joined.member_id, joined.generation, 0, 0)
        .await
        .expect("ack offset 0");
    assert_eq!(first.committed_offset, 1);

    // 重复/更旧的 Ack 幂等成功，不回退。
    let replay = ack(&mut client, &joined.member_id, joined.generation, 0, 0)
        .await
        .expect("replayed ack");
    assert_eq!(replay.committed_offset, 1);

    let advance = ack(&mut client, &joined.member_id, joined.generation, 0, 2)
        .await
        .expect("ack offset 2");
    assert_eq!(advance.committed_offset, 3);

    let offsets = node.group_offsets(GROUP);
    assert_eq!(offsets.len(), 1, "one partition committed");
    assert_eq!(offsets[0].partition, 0);
    assert_eq!(offsets[0].committed_offset, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ack_batch_is_atomic_when_one_partition_is_unassigned() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    create_topic(&mut client, TOPIC, 2, None).await;
    publish(&mut client, TOPIC, 0, b"a", "")
        .await
        .expect("publish");
    publish(&mut client, TOPIC, 1, b"b", "")
        .await
        .expect("publish");

    // range 分配：m1 → partition 0，m2 → partition 1。
    let first = node.join_direct(GROUP, "m1", TOPIC, 2).await;
    let second = node.join_direct(GROUP, "m2", TOPIC, 2).await;
    assert_eq!(first.member_id, "m1");
    assert_eq!(second.member_id, "m2");
    assert_eq!(second.generation, 2);
    let assigned: Vec<u32> = second
        .assignments
        .iter()
        .map(|assignment| assignment.partition)
        .collect();
    assert_eq!(assigned, vec![1], "each member owns one partition");

    // m1 只有 partition 0：批内含 1 ⇒ 整批拒绝，0 也不写。
    let status = client
        .ack_batch(mq_v2::AckBatchRequest {
            group: GROUP.to_string(),
            member_id: "m1".to_string(),
            generation: second.generation,
            acks: vec![
                mq_v2::AckEntry {
                    topic: TOPIC.to_string(),
                    partition: 0,
                    topic_epoch: 0,
                    offset: 0,
                },
                mq_v2::AckEntry {
                    topic: TOPIC.to_string(),
                    partition: 1,
                    topic_epoch: 0,
                    offset: 0,
                },
            ],
        })
        .await
        .expect_err("an unassigned partition must reject the whole batch");
    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert_eq!(wire_code(&status).as_deref(), Some("NOT_GROUP_MEMBER"));
    assert!(
        node.group_offsets(GROUP).is_empty(),
        "整批拒绝不得写入任何位点"
    );

    // 只 ack 自己的分区：成功。
    let response = client
        .ack_batch(mq_v2::AckBatchRequest {
            group: GROUP.to_string(),
            member_id: "m1".to_string(),
            generation: second.generation,
            acks: vec![mq_v2::AckEntry {
                topic: TOPIC.to_string(),
                partition: 0,
                topic_epoch: 0,
                offset: 0,
            }],
        })
        .await
        .expect("assigned partition must commit")
        .into_inner();
    assert_eq!(response.results.len(), 1);
    assert_eq!(response.results[0].committed_offset, 1);
    assert_eq!(response.results[0].partition, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ack_rejects_a_stale_generation() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    create_topic(&mut client, TOPIC, 1, None).await;
    let first = node.join_direct(GROUP, "m1", TOPIC, 1).await;
    let second = node.join_direct(GROUP, "m2", TOPIC, 1).await;
    assert!(second.generation > first.generation);

    let status = ack(&mut client, "m1", first.generation, 0, 0)
        .await
        .expect_err("a stale generation must be rejected");
    assert_eq!(status.code(), Code::Aborted, "{status:?}");
    assert_eq!(wire_code(&status).as_deref(), Some("GROUP_EPOCH_STALE"));
    assert!(node.group_offsets(GROUP).is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ack_maps_unknown_topic_and_partition_to_stable_codes() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    create_topic(&mut client, TOPIC, 1, None).await;
    let joined = node.join_direct(GROUP, "m1", TOPIC, 1).await;

    let unknown_topic = client
        .ack(mq_v2::AckRequest {
            topic: "missing".to_string(),
            partition: 0,
            topic_epoch: 0,
            group: GROUP.to_string(),
            member_id: joined.member_id.clone(),
            generation: joined.generation,
            offset: 0,
        })
        .await
        .expect_err("unknown topic");
    assert_eq!(
        wire_code(&unknown_topic).as_deref(),
        Some("TOPIC_NOT_FOUND")
    );

    let unknown_partition = ack(&mut client, &joined.member_id, joined.generation, 5, 0)
        .await
        .expect_err("partition out of range");
    assert_eq!(
        wire_code(&unknown_partition).as_deref(),
        Some("PARTITION_NOT_FOUND")
    );

    // 空批/超上限在路由前就拒绝。
    for count in [0usize, 257] {
        let acks = (0..count)
            .map(|_| mq_v2::AckEntry {
                topic: TOPIC.to_string(),
                partition: 0,
                topic_epoch: 0,
                offset: 0,
            })
            .collect();
        let status = client
            .ack_batch(mq_v2::AckBatchRequest {
                group: GROUP.to_string(),
                member_id: joined.member_id.clone(),
                generation: joined.generation,
                acks,
            })
            .await
            .expect_err("ack batch size must be validated");
        assert_eq!(status.code(), Code::InvalidArgument, "count {count}");
        assert_eq!(wire_code(&status).as_deref(), Some("INVALID_ARGUMENT"));
    }
}

// ──── M3-3：组协调 + reaper ────

use coord_server::storage::mq_group::{MqGroupOp, MqGroupReap, MqGroupResult};

async fn join(
    client: &mut MqClient<Channel>,
    group: &str,
    member_id: &str,
    topics: &[&str],
    session_timeout_ms: i32,
) -> Result<mq_v2::JoinGroupResponse, tonic::Status> {
    client
        .join_group(mq_v2::JoinGroupRequest {
            group: group.to_string(),
            member_id: member_id.to_string(),
            topics: topics.iter().map(|topic| topic.to_string()).collect(),
            session_timeout_ms,
        })
        .await
        .map(|response| response.into_inner())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_assigns_partitions_and_reports_committed_offsets() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    create_topic(&mut client, TOPIC, 2, None).await;
    for index in 0..3u32 {
        publish(&mut client, TOPIC, 0, format!("m-{index}").as_bytes(), "")
            .await
            .expect("publish");
    }

    let joined = join(&mut client, GROUP, "", &[TOPIC], 0)
        .await
        .expect("join");
    assert_eq!(joined.member_id, "m0000000000000001");
    assert_eq!(joined.generation, 1);
    assert_eq!(
        joined.assignments.len(),
        2,
        "single member owns all partitions"
    );
    assert_eq!(joined.session_timeout_ms, 30_000);
    assert_eq!(joined.heartbeat_interval_ms, 10_000);

    ack(&mut client, &joined.member_id, joined.generation, 0, 1)
        .await
        .expect("ack partition 0");

    // 同订阅重连：代际不变，分配里带回已提交位点。
    let rejoin = join(&mut client, GROUP, &joined.member_id, &[TOPIC], 0)
        .await
        .expect("rejoin");
    assert_eq!(rejoin.generation, joined.generation);
    let partition_zero = rejoin
        .assignments
        .iter()
        .find(|assignment| assignment.partition == 0)
        .expect("partition 0 assignment");
    assert_eq!(partition_zero.committed_offset, 2);

    let offsets = client
        .get_committed_offsets(mq_v2::GetCommittedOffsetsRequest {
            group: GROUP.to_string(),
            partitions: Vec::new(),
        })
        .await
        .expect("committed offsets")
        .into_inner();
    assert_eq!(offsets.offsets.len(), 1);
    assert_eq!(offsets.offsets[0].topic, TOPIC);
    assert_eq!(offsets.offsets[0].partition, 0);
    assert_eq!(offsets.offsets[0].offset, 2);
    assert!(offsets.offsets[0].committed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_rejects_unknown_topics_and_heartbeat_rejects_stale_generations() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    create_topic(&mut client, TOPIC, 1, None).await;

    let status = join(&mut client, GROUP, "", &["missing"], 0)
        .await
        .expect_err("unknown topic must fail closed");
    assert_eq!(wire_code(&status).as_deref(), Some("TOPIC_NOT_FOUND"));

    let joined = join(&mut client, GROUP, "", &[TOPIC], 0)
        .await
        .expect("join");
    let stale = client
        .heartbeat(mq_v2::HeartbeatRequest {
            group: GROUP.to_string(),
            member_id: joined.member_id.clone(),
            generation: joined.generation + 1,
        })
        .await
        .expect_err("stale generation");
    assert_eq!(stale.code(), Code::Aborted, "{stale:?}");
    assert_eq!(wire_code(&stale).as_deref(), Some("GROUP_EPOCH_STALE"));

    let ok = client
        .heartbeat(mq_v2::HeartbeatRequest {
            group: GROUP.to_string(),
            member_id: joined.member_id.clone(),
            generation: joined.generation,
        })
        .await
        .expect("heartbeat")
        .into_inner();
    assert_eq!(ok.generation, joined.generation);
    assert!(!ok.rebalance_required);

    // Leave 幂等：重复调用成功（成员已不在组里）。
    for _ in 0..2 {
        client
            .leave_group(mq_v2::LeaveGroupRequest {
                group: GROUP.to_string(),
                member_id: joined.member_id.clone(),
                generation: joined.generation,
            })
            .await
            .expect("leave is idempotent");
    }
    assert!(node
        .group_state(GROUP)
        .expect("group row")
        .members
        .is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expired_members_are_reaped_and_partitions_transfer() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    create_topic(&mut client, TOPIC, 2, None).await;

    let first = join(&mut client, GROUP, "", &[TOPIC], 1_000)
        .await
        .expect("join");
    let second = join(&mut client, GROUP, "", &[TOPIC], 1_000)
        .await
        .expect("second join");
    assert_eq!(second.generation, first.generation + 1);

    // 用命令携带的时间驱动过期（apply 不读时钟）：now + 5s > session timeout 1s。
    let now = coord_server::mq_v2::now_ms() + 5_000;
    let reaped = node
        .propose_group(MqGroupOp::Reap(MqGroupReap {
            shard_id: 0,
            now_ms: now,
        }))
        .await;
    match reaped {
        MqGroupResult::Reaped(summary) => {
            assert_eq!(summary.members_expired, 2);
            assert_eq!(summary.groups_changed, 1);
        }
        other => panic!("unexpected reap result: {other:?}"),
    }
    let state = node.group_state(GROUP).expect("group row survives");
    assert!(state.members.is_empty(), "expired members removed");
    assert!(state.generation > second.generation);

    // 新成员拿到全部分区（旧分配随代际整体重算）。
    let third = join(&mut client, GROUP, "", &[TOPIC], 0)
        .await
        .expect("rejoin after reap");
    assert_eq!(third.assignments.len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn describe_group_reports_lag_and_members() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    create_topic(&mut client, TOPIC, 1, None).await;
    for index in 0..3u32 {
        publish(&mut client, TOPIC, 0, format!("m-{index}").as_bytes(), "")
            .await
            .expect("publish");
    }
    let joined = join(&mut client, GROUP, "", &[TOPIC], 0)
        .await
        .expect("join");
    ack(&mut client, &joined.member_id, joined.generation, 0, 1)
        .await
        .expect("ack");

    let described = client
        .describe_group(mq_v2::DescribeGroupRequest {
            group: GROUP.to_string(),
        })
        .await
        .expect("describe")
        .into_inner();
    assert_eq!(described.group, GROUP);
    assert_eq!(described.state, mq_v2::GroupState::Stable as i32);
    assert_eq!(described.members.len(), 1);
    assert_eq!(described.members[0].member_id, joined.member_id);
    assert_eq!(described.members[0].topics, vec![TOPIC.to_string()]);
    assert_eq!(described.partitions.len(), 1);
    let lag = &described.partitions[0];
    assert_eq!(lag.topic, TOPIC);
    assert_eq!(lag.committed_offset, 2);
    assert_eq!(lag.next_offset, 3);
    assert_eq!(lag.lag, 1);

    let missing = client
        .describe_group(mq_v2::DescribeGroupRequest {
            group: "nope".to_string(),
        })
        .await
        .expect_err("unknown group");
    assert_eq!(wire_code(&missing).as_deref(), Some("GROUP_NOT_FOUND"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_groups_merges_every_group_shard_and_paginates() {
    let node = MqV2Node::boot(2, 2).await;
    let mut client = node.client().await;
    create_topic(&mut client, TOPIC, 1, None).await;
    for group in ["g-1", "g-2", "g-3", "other-1"] {
        join(&mut client, group, "", &[TOPIC], 0)
            .await
            .expect("join");
    }

    let mut collected = Vec::new();
    let mut token = String::new();
    loop {
        let page = client
            .list_groups(mq_v2::ListGroupsRequest {
                prefix: "g-".to_string(),
                page_size: 2,
                page_token: token.clone(),
            })
            .await
            .expect("list groups")
            .into_inner();
        collected.extend(page.groups.iter().cloned());
        if page.next_page_token.is_empty() {
            break;
        }
        token = page.next_page_token;
    }
    assert_eq!(collected, vec!["g-1", "g-2", "g-3"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_ttl_reaper_removes_empty_groups_and_offsets() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    create_topic(&mut client, TOPIC, 1, None).await;
    publish(&mut client, TOPIC, 0, b"payload", "")
        .await
        .expect("publish");
    let joined = join(&mut client, GROUP, "", &[TOPIC], 0)
        .await
        .expect("join");
    ack(&mut client, &joined.member_id, joined.generation, 0, 0)
        .await
        .expect("ack");
    client
        .leave_group(mq_v2::LeaveGroupRequest {
            group: GROUP.to_string(),
            member_id: joined.member_id.clone(),
            generation: joined.generation,
        })
        .await
        .expect("leave");
    assert!(
        node.group_state(GROUP).is_some(),
        "empty group keeps offsets"
    );

    // 7d TTL 之外跑一次 reaper：空组 + 位点一起回收。
    let future = coord_server::mq_v2::now_ms()
        + i64::try_from(coord_server::mq_v2::reaper::GROUP_OFFSET_TTL_SECS * 1_000 + 60_000)
            .expect("ttl fits i64");
    let stats = node.node.mq_v2_reaper_pass(future).await;
    assert!(
        stats.groups_removed >= 1,
        "the TTL reaper must remove the empty group: {stats:?}"
    );
    assert!(node.group_state(GROUP).is_none());
    assert!(node.group_offsets(GROUP).is_empty());
    assert!(
        node.metric("coord_mq_v2_group_gc_removed_total") >= 1,
        "the reaper must publish its GC counter"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_committed_offsets_marks_uncommitted_partitions() {
    let node = MqV2Node::boot(2, 1).await;
    let mut client = node.client().await;
    create_topic(&mut client, TOPIC, 2, None).await;
    let joined = join(&mut client, GROUP, "", &[TOPIC], 0)
        .await
        .expect("join");
    ack(&mut client, &joined.member_id, joined.generation, 0, 0)
        .await
        .expect("ack partition 0");

    let offsets = client
        .get_committed_offsets(mq_v2::GetCommittedOffsetsRequest {
            group: GROUP.to_string(),
            partitions: vec![
                mq_v2::TopicPartition {
                    topic: TOPIC.to_string(),
                    partition: 0,
                },
                mq_v2::TopicPartition {
                    topic: TOPIC.to_string(),
                    partition: 1,
                },
            ],
        })
        .await
        .expect("committed offsets")
        .into_inner();
    assert_eq!(offsets.offsets.len(), 2);
    assert_eq!(offsets.offsets[0].offset, 1);
    assert!(offsets.offsets[0].committed);
    assert_eq!(offsets.offsets[1].offset, 0);
    assert!(!offsets.offsets[1].committed);
}
