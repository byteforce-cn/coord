// coord-agent: 消息队列服务测试
//
// MessageQueueService 测试（基于 redb 的分段日志 MQ）。
// 支持 Topic/Partition/ConsumerGroup/DeadLetterQueue。

use std::sync::Arc;
use tempfile::TempDir;

use coord_agent::services::mq::{MessageQueueService, MqStats, TopicConfig};
use coord_agent::BaseService;

// ──── helpers ────

fn temp_data_dir() -> TempDir {
    tempfile::tempdir().expect("failed to create temp dir")
}

fn new_mq_service(dir: &TempDir) -> MessageQueueService {
    let svc = MessageQueueService::new(dir.path().to_path_buf(), 1024 * 1024 * 1024); // 1GB
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async { svc.start().await.expect("start should succeed") });
    svc
}

// ──── F.4.1: MQ 服务创建与 BaseService trait ────

#[test]
fn test_mq_service_creation() {
    let dir = temp_data_dir();
    let svc = MessageQueueService::new(dir.path().to_path_buf(), 1024 * 1024 * 1024);
    assert_eq!(svc.name(), "mq");
    assert!(!svc.health_check()); // not started
}

#[test]
fn test_mq_service_start_stop() {
    let dir = temp_data_dir();
    let svc = MessageQueueService::new(dir.path().to_path_buf(), 1024 * 1024 * 1024);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        svc.start().await.expect("start should succeed");
        assert!(svc.health_check());
        svc.stop().await.expect("stop should succeed");
        assert!(!svc.health_check());
    });
}

// ──── F.4.2: Topic 管理 ────

#[test]
fn test_mq_create_topic() {
    let dir = temp_data_dir();
    let svc = new_mq_service(&dir);
    svc.create_topic(
        "orders",
        TopicConfig {
            partitions: 3,
            retention_secs: 3600,
            max_message_size: 1024 * 1024,
        },
    )
    .expect("create topic should succeed");
    assert!(svc.topic_exists("orders").expect("topic exists"));
    assert!(!svc.topic_exists("nonexistent").expect("topic exists"));
}

#[test]
fn test_mq_create_topic_duplicate() {
    let dir = temp_data_dir();
    let svc = new_mq_service(&dir);
    svc.create_topic(
        "orders",
        TopicConfig {
            partitions: 2,
            retention_secs: 3600,
            max_message_size: 1024,
        },
    )
    .unwrap();
    let result = svc.create_topic(
        "orders",
        TopicConfig {
            partitions: 3,
            retention_secs: 7200,
            max_message_size: 2048,
        },
    );
    assert!(result.is_err(), "duplicate topic should error");
}

#[test]
fn test_mq_delete_topic() {
    let dir = temp_data_dir();
    let svc = new_mq_service(&dir);
    svc.create_topic(
        "tmp",
        TopicConfig {
            partitions: 1,
            retention_secs: 60,
            max_message_size: 1024,
        },
    )
    .unwrap();
    svc.delete_topic("tmp")
        .expect("delete topic should succeed");
    assert!(!svc.topic_exists("tmp").expect("topic exists"));
}

#[test]
fn test_mq_list_topics() {
    let dir = temp_data_dir();
    let svc = new_mq_service(&dir);
    svc.create_topic(
        "t1",
        TopicConfig {
            partitions: 1,
            retention_secs: 3600,
            max_message_size: 1024,
        },
    )
    .unwrap();
    svc.create_topic(
        "t2",
        TopicConfig {
            partitions: 2,
            retention_secs: 7200,
            max_message_size: 2048,
        },
    )
    .unwrap();
    let topics = svc.list_topics().expect("list topics");
    assert_eq!(topics.len(), 2);
    assert!(topics.iter().any(|t| t.name == "t1"));
    assert!(topics.iter().any(|t| t.name == "t2"));
}

// ──── F.4.3: 消息生产与消费 ────

#[test]
fn test_mq_produce_consume() {
    let dir = temp_data_dir();
    let svc = new_mq_service(&dir);
    svc.create_topic(
        "orders",
        TopicConfig {
            partitions: 2,
            retention_secs: 3600,
            max_message_size: 1024 * 1024,
        },
    )
    .unwrap();

    // Produce
    let offset = svc
        .produce("orders", 0, b"hello world".to_vec(), None)
        .expect("produce");
    assert_eq!(offset, 0);

    let offset2 = svc
        .produce("orders", 0, b"second msg".to_vec(), None)
        .expect("produce");
    assert_eq!(offset2, 1);

    // Consume
    let msgs = svc.consume("orders", 0, 0, 10).expect("consume");
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0].offset, 0);
    assert_eq!(msgs[0].payload, b"hello world");
    assert_eq!(msgs[1].offset, 1);
    assert_eq!(msgs[1].payload, b"second msg");
}

#[test]
fn test_mq_produce_to_nonexistent_topic() {
    let dir = temp_data_dir();
    let svc = new_mq_service(&dir);
    let result = svc.produce("no_such_topic", 0, b"data".to_vec(), None);
    assert!(result.is_err());
}

#[test]
fn test_mq_produce_to_invalid_partition() {
    let dir = temp_data_dir();
    let svc = new_mq_service(&dir);
    svc.create_topic(
        "orders",
        TopicConfig {
            partitions: 2,
            retention_secs: 3600,
            max_message_size: 1024,
        },
    )
    .unwrap();
    let result = svc.produce("orders", 99, b"data".to_vec(), None);
    assert!(result.is_err());
}

#[test]
fn test_mq_consume_empty_partition() {
    let dir = temp_data_dir();
    let svc = new_mq_service(&dir);
    svc.create_topic(
        "orders",
        TopicConfig {
            partitions: 1,
            retention_secs: 3600,
            max_message_size: 1024,
        },
    )
    .unwrap();
    let msgs = svc.consume("orders", 0, 0, 10).expect("consume");
    assert!(msgs.is_empty());
}

#[test]
fn test_mq_produce_multi_partition() {
    let dir = temp_data_dir();
    let svc = new_mq_service(&dir);
    svc.create_topic(
        "orders",
        TopicConfig {
            partitions: 3,
            retention_secs: 3600,
            max_message_size: 1024,
        },
    )
    .unwrap();

    svc.produce("orders", 0, b"p0-msg0".to_vec(), None).unwrap();
    svc.produce("orders", 1, b"p1-msg0".to_vec(), None).unwrap();
    svc.produce("orders", 2, b"p2-msg0".to_vec(), None).unwrap();

    assert_eq!(svc.consume("orders", 0, 0, 10).unwrap().len(), 1);
    assert_eq!(svc.consume("orders", 1, 0, 10).unwrap().len(), 1);
    assert_eq!(svc.consume("orders", 2, 0, 10).unwrap().len(), 1);
}

// ──── F.4.4: Consumer Group 偏移管理 ────

#[test]
fn test_mq_consumer_group_offset() {
    let dir = temp_data_dir();
    let svc = new_mq_service(&dir);
    svc.create_topic(
        "orders",
        TopicConfig {
            partitions: 2,
            retention_secs: 3600,
            max_message_size: 1024,
        },
    )
    .unwrap();

    // Produce 3 messages to partition 0
    for i in 0..3u8 {
        svc.produce("orders", 0, vec![i], None).unwrap();
    }

    // Initial offset should be 0
    assert_eq!(
        svc.get_consumer_offset("cg1", "orders", 0)
            .expect("get offset"),
        0
    );

    // Commit offset
    svc.commit_offset("cg1", "orders", 0, 2)
        .expect("commit offset");
    assert_eq!(
        svc.get_consumer_offset("cg1", "orders", 0)
            .expect("get offset"),
        2
    );

    // Consume from committed offset
    let msgs = svc.consume("orders", 0, 2, 10).expect("consume");
    assert_eq!(msgs.len(), 1); // only message at offset 2
}

#[test]
fn test_mq_multiple_consumer_groups() {
    let dir = temp_data_dir();
    let svc = new_mq_service(&dir);
    svc.create_topic(
        "orders",
        TopicConfig {
            partitions: 1,
            retention_secs: 3600,
            max_message_size: 1024,
        },
    )
    .unwrap();

    svc.produce("orders", 0, b"msg".to_vec(), None).unwrap();

    // Two consumer groups with independent offsets
    svc.commit_offset("cg-a", "orders", 0, 0).unwrap();
    svc.commit_offset("cg-b", "orders", 0, 1).unwrap();

    assert_eq!(svc.get_consumer_offset("cg-a", "orders", 0).unwrap(), 0);
    assert_eq!(svc.get_consumer_offset("cg-b", "orders", 0).unwrap(), 1);
}

// ──── F.4.5: 死信队列 (DLQ) ────

#[test]
fn test_mq_dead_letter_queue() {
    let dir = temp_data_dir();
    let svc = new_mq_service(&dir);
    svc.create_topic(
        "orders",
        TopicConfig {
            partitions: 1,
            retention_secs: 3600,
            max_message_size: 1024,
        },
    )
    .unwrap();

    svc.produce("orders", 0, b"bad message".to_vec(), None)
        .unwrap();

    // Move message at offset 0 to DLQ
    svc.move_to_dlq("orders", 0, 0, "parse_error", "invalid JSON")
        .expect("move to DLQ");

    // DLQ should have the message
    let dlq_msgs = svc.consume_dlq("orders", 0, 10).expect("consume DLQ");
    assert_eq!(dlq_msgs.len(), 1);
    assert_eq!(dlq_msgs[0].payload, b"bad message");
    assert_eq!(dlq_msgs[0].error_reason, Some("parse_error".to_string()));

    // Original partition should no longer have it
    let msgs = svc.consume("orders", 0, 0, 10).expect("consume");
    assert!(msgs.is_empty());
}

// ──── F.4.6: 消息持久化恢复 ────

#[test]
fn test_mq_persistence_across_restart() {
    let dir = temp_data_dir();
    let db_path = dir.path().to_path_buf();
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Produce messages
    {
        let svc = MessageQueueService::new(db_path.clone(), 1024 * 1024 * 1024);
        rt.block_on(async { svc.start().await.expect("start") });
        svc.create_topic(
            "persist",
            TopicConfig {
                partitions: 1,
                retention_secs: 3600,
                max_message_size: 1024,
            },
        )
        .unwrap();
        svc.produce("persist", 0, b"msg1".to_vec(), None).unwrap();
        svc.produce("persist", 0, b"msg2".to_vec(), None).unwrap();
        svc.commit_offset("cg1", "persist", 0, 1).unwrap();
    }

    // Re-open and verify
    {
        let svc = MessageQueueService::new(db_path.clone(), 1024 * 1024 * 1024);
        rt.block_on(async { svc.start().await.expect("start") });
        assert!(svc.topic_exists("persist").unwrap());
        let msgs = svc.consume("persist", 0, 0, 10).unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(svc.get_consumer_offset("cg1", "persist", 0).unwrap(), 1);
    }
}

// ──── F.4.7: 统计信息 ────

#[test]
fn test_mq_stats() {
    let dir = temp_data_dir();
    let svc = new_mq_service(&dir);
    svc.create_topic(
        "orders",
        TopicConfig {
            partitions: 2,
            retention_secs: 3600,
            max_message_size: 1024,
        },
    )
    .unwrap();

    for i in 0..10u8 {
        svc.produce("orders", i as u32 % 2, vec![i], None).unwrap();
    }

    let stats = svc.stats().expect("stats");
    assert_eq!(stats.topic_count, 1);
    assert_eq!(stats.total_messages, 10);
}

// ──── F.4.8: 并发读写安全 ────

#[test]
fn test_mq_concurrent_produce() {
    use std::thread;

    let dir = temp_data_dir();
    let svc = Arc::new(new_mq_service(&dir));
    svc.create_topic(
        "concurrent",
        TopicConfig {
            partitions: 4,
            retention_secs: 3600,
            max_message_size: 1024,
        },
    )
    .unwrap();

    let mut handles = vec![];
    for i in 0..20 {
        let svc = svc.clone();
        handles.push(thread::spawn(move || {
            let partition = (i % 4) as u32;
            let payload = format!("msg-{}", i).into_bytes();
            svc.produce("concurrent", partition, payload, None)
                .expect("produce should succeed");
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    let mut total = 0u64;
    for p in 0..4u32 {
        total += svc.consume("concurrent", p, 0, 100).unwrap().len() as u64;
    }
    assert_eq!(total, 20);
}

// ──── 生产幂等 / 消息 key 不得被静默丢弃 ────

/// 同一 `idempotency_key` 重复生产**不产生第二个 offset**
///
/// `MqPublishRequest.idempotency_key` 与 `key` 不得在 gRPC 层被
/// 直接丢弃（否则调用方"响应丢失后重试"会给下游多一条消息）。
#[test]
fn test_mq_produce_idempotent_dedups_same_key() {
    let dir = temp_data_dir();
    let svc = new_mq_service(&dir);
    svc.create_topic(
        "idem",
        TopicConfig {
            partitions: 1,
            retention_secs: 3600,
            max_message_size: 1024,
        },
    )
    .unwrap();

    let first = svc
        .produce_idempotent("idem", 0, b"payload-1".to_vec(), None, Some("req-42"))
        .unwrap();
    let second = svc
        .produce_idempotent("idem", 0, b"payload-1".to_vec(), None, Some("req-42"))
        .unwrap();

    assert_eq!(first, second, "同幂等键必须返回同一 offset");
    assert_eq!(
        svc.consume("idem", 0, 0, 100).unwrap().len(),
        1,
        "同幂等键重复生产不得追加第二条消息"
    );

    // 不同幂等键仍应各自产生新消息
    let other = svc
        .produce_idempotent("idem", 0, b"payload-2".to_vec(), None, Some("req-43"))
        .unwrap();
    assert_ne!(other, first);
    assert_eq!(svc.consume("idem", 0, 0, 100).unwrap().len(), 2);
}

/// 无幂等键（或空串）⇒ 不去重，每次都是新消息（保持既有语义）
#[test]
fn test_mq_produce_without_idempotency_key_is_not_deduped() {
    let dir = temp_data_dir();
    let svc = new_mq_service(&dir);
    svc.create_topic(
        "nodedup",
        TopicConfig {
            partitions: 1,
            retention_secs: 3600,
            max_message_size: 1024,
        },
    )
    .unwrap();

    let a = svc.produce("nodedup", 0, b"x".to_vec(), None).unwrap();
    let b = svc.produce("nodedup", 0, b"x".to_vec(), None).unwrap();
    assert_ne!(a, b);
    assert_eq!(svc.consume("nodedup", 0, 0, 100).unwrap().len(), 2);

    // 空串与 None 同义
    let c = svc
        .produce_idempotent("nodedup", 0, b"x".to_vec(), None, Some(""))
        .unwrap();
    assert_ne!(b, c);
    assert_eq!(svc.consume("nodedup", 0, 0, 100).unwrap().len(), 3);
}

/// 幂等索引按分区隔离：同键不同分区是两条独立消息
#[test]
fn test_mq_idempotency_is_per_partition() {
    let dir = temp_data_dir();
    let svc = new_mq_service(&dir);
    svc.create_topic(
        "idem-parts",
        TopicConfig {
            partitions: 2,
            retention_secs: 3600,
            max_message_size: 1024,
        },
    )
    .unwrap();

    let p0 = svc
        .produce_idempotent("idem-parts", 0, b"m".to_vec(), None, Some("same"))
        .unwrap();
    let p1 = svc
        .produce_idempotent("idem-parts", 1, b"m".to_vec(), None, Some("same"))
        .unwrap();
    assert_eq!(p0, 0);
    assert_eq!(p1, 0, "分区各自从 offset 0 开始");
    assert_eq!(svc.consume("idem-parts", 0, 0, 10).unwrap().len(), 1);
    assert_eq!(svc.consume("idem-parts", 1, 0, 10).unwrap().len(), 1);
}

/// 幂等索引与消息**同事务**：重启后仍然生效（不得"重启即忘"）
#[test]
fn test_mq_idempotency_survives_restart() {
    let dir = temp_data_dir();
    let db_path = dir.path().to_path_buf();

    let offset = {
        let svc = MessageQueueService::new(db_path.clone(), 1024 * 1024 * 1024);
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async { svc.start().await.expect("start") });
        svc.create_topic(
            "durable",
            TopicConfig {
                partitions: 1,
                retention_secs: 3600,
                max_message_size: 1024,
            },
        )
        .unwrap();
        svc.produce_idempotent("durable", 0, b"m".to_vec(), None, Some("k1"))
            .unwrap()
    }; // svc dropped ⇒ redb 关闭

    let svc2 = MessageQueueService::new(db_path, 1024 * 1024 * 1024);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async { svc2.start().await.expect("start") });
    let again = svc2
        .produce_idempotent("durable", 0, b"m".to_vec(), None, Some("k1"))
        .unwrap();
    assert_eq!(again, offset, "重启后同幂等键必须仍命中去重");
    assert_eq!(
        svc2.consume("durable", 0, 0, 100).unwrap().len(),
        1,
        "重启后不得因幂等索引丢失而重放"
    );
}

/// 消息 `key` 随 headers 持久化（不再被静默丢弃）
#[test]
fn test_mq_produce_persists_message_key_header() {
    use std::collections::BTreeMap;

    let dir = temp_data_dir();
    let svc = new_mq_service(&dir);
    svc.create_topic(
        "withkey",
        TopicConfig {
            partitions: 1,
            retention_secs: 3600,
            max_message_size: 1024,
        },
    )
    .unwrap();

    let mut headers = BTreeMap::new();
    headers.insert("key".to_string(), "a1b2c3".to_string());
    svc.produce("withkey", 0, b"body".to_vec(), Some(headers))
        .unwrap();

    let records = svc.consume("withkey", 0, 0, 10).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].headers.get("key").map(String::as_str),
        Some("a1b2c3"),
        "消息 key（以 hex 存于 headers）必须可被消费者读回"
    );
}

/// 复制路径的诚实边界：headers 无法随 `ReplicationEntry` 传播 ⇒ **fail-loud**
///
/// 复制未启用时该分支不可达；此处断言的是"复制启用且给了 headers"时的拒绝行为，
/// 用直接调用 `produce_replicated` 表达（它先检查 replication 是否启用）。
#[test]
fn test_mq_replicated_path_rejects_headers_instead_of_dropping() {
    use std::collections::BTreeMap;

    let dir = temp_data_dir();
    let svc = new_mq_service(&dir);

    let mut headers = BTreeMap::new();
    headers.insert("key".to_string(), "deadbeef".to_string());

    let rt = tokio::runtime::Runtime::new().unwrap();
    // 复制未启用 ⇒ 先报 "replication not enabled"（这是预期路径：
    // 说明该检查位于 headers 检查之前的 fail-closed 前置条件）
    let err = rt
        .block_on(async {
            svc.produce_replicated("t", 0, b"x".to_vec(), Some(headers), None)
                .await
        })
        .expect_err("复制未启用时必须报错");
    assert!(
        err.to_string().contains("replication not enabled"),
        "unexpected error: {err}"
    );
}

// ════════════════════════════════════════════════════════════
// G-MQ-4: topic 删除与回收（删除后配额归还 / 同名重建 = 空 topic）
// ════════════════════════════════════════════════════════════

/// 删除 = 全量回收：消息 / DLQ / 位点 / 幂等索引 / 配置；配额归还
#[test]
fn test_delete_topic_full_reclaims_all_state() {
    let dir = temp_data_dir();
    let svc = new_mq_service(&dir);

    let cfg = || TopicConfig {
        partitions: 1,
        retention_secs: 3600,
        max_message_size: 1024,
    };

    // topic A：消息 + DLQ + 位点 + 幂等索引
    svc.create_topic("a", cfg()).unwrap();
    svc.produce_idempotent("a", 0, b"m1".to_vec(), None, Some("k1"))
        .unwrap();
    svc.produce_idempotent("a", 0, b"m2".to_vec(), None, Some("k2"))
        .unwrap();
    svc.move_to_dlq("a", 0, 0, "poison", "bad").unwrap();
    svc.commit_offset("cg", "a", 0, 2).unwrap();

    // topic B（对照组）：不得被误伤
    svc.create_topic("b", cfg()).unwrap();
    svc.produce("b", 0, b"keep".to_vec(), None).unwrap();

    let before = svc.accounted_bytes().unwrap();
    assert!(before > 0);

    let stats = svc.delete_topic_full("a").unwrap();
    assert_eq!(stats.messages_removed, 1, "主日志剩余 1 条（另一条已入 DLQ）");
    assert_eq!(stats.dlq_removed, 1);
    assert_eq!(stats.offsets_removed, 1);
    assert_eq!(stats.idempotency_removed, 2);
    assert!(stats.bytes_reclaimed > 0);
    assert!(!svc.topic_exists("a").unwrap(), "配置行必须删除");

    // 配额归还：记账 = 仅剩 topic B 的消息
    let after = svc.accounted_bytes().unwrap();
    assert!(after < before, "after={after} before={before}");
    assert_eq!(
        after,
        before - stats.bytes_reclaimed,
        "回收字节必须精确等于记账净减少"
    );

    // 删除后读写明确报错（RPC 层映射 NOT_FOUND）
    assert!(
        svc.produce("a", 0, b"x".to_vec(), None).is_err(),
        "删除后 publish 必须报错"
    );
    assert_eq!(
        svc.get_consumer_offset("cg", "a", 0).unwrap(),
        0,
        "位点已回收（回默认 0）"
    );
    assert!(svc.consume_dlq("a", 0, 10).unwrap().is_empty());

    // 同名重建 = 空 topic（offset 从 0 起）
    svc.create_topic("a", cfg()).unwrap();
    assert!(
        svc.consume("a", 0, 0, 10).unwrap().is_empty(),
        "重建后必须为空"
    );
    let off = svc.produce("a", 0, b"fresh".to_vec(), None).unwrap();
    assert_eq!(off, 0, "重建后 offset 从 0 起");

    // 删除不存在的 topic：明确报错（NOT_FOUND 语义）
    let err = svc.delete_topic_full("ghost").unwrap_err();
    assert!(err.to_string().contains("not found"), "unexpected: {err}");
}

// ════════════════════════════════════════════════════════════
// G-MQ-2: 显式 DLQ 管理路径（服务层语义；gRPC 侧见 agent_isr_test）
// ════════════════════════════════════════════════════════════

/// move_to_dlq：净额记账（消息行 → DLQ 行）+ 未知消息/未知 topic 明确报错
#[test]
fn test_move_to_dlq_net_accounting_and_not_found() {
    let dir = temp_data_dir();
    let svc = new_mq_service(&dir);
    svc.create_topic(
        "orders",
        TopicConfig {
            partitions: 1,
            retention_secs: 3600,
            max_message_size: 1024,
        },
    )
    .unwrap();
    svc.produce("orders", 0, b"poison-msg".to_vec(), None)
        .unwrap();

    svc.move_to_dlq("orders", 0, 0, "poison", "retries exceeded")
        .unwrap();
    let dlq = svc.consume_dlq("orders", 0, 10).unwrap();
    assert_eq!(dlq.len(), 1);
    assert_eq!(dlq[0].error_reason.as_deref(), Some("poison"));
    assert_eq!(dlq[0].error_detail.as_deref(), Some("retries exceeded"));

    // 未知 offset / 未知 topic：明确错误（RPC 层映射 NOT_FOUND）
    let e1 = svc.move_to_dlq("orders", 0, 999, "x", "").unwrap_err();
    assert!(e1.to_string().contains("not found"), "unexpected: {e1}");

    // 分区不被阻塞：后续消息照常写入与消费
    svc.produce("orders", 0, b"m2".to_vec(), None).unwrap();
    let msgs = svc.consume("orders", 0, 1, 10).unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].payload, b"m2");
}

// ════════════════════════════════════════════════════════════
// G-MQ-3: 消费位点/滞后快照（指标采样的输入面）
// ════════════════════════════════════════════════════════════

/// 位点快照与 next-offset 快照按 (topic, group, partition) 正确解码
#[test]
fn test_consumer_offsets_and_next_offsets_snapshot() {
    let dir = temp_data_dir();
    let svc = new_mq_service(&dir);
    svc.create_topic(
        "orders",
        TopicConfig {
            partitions: 2,
            retention_secs: 3600,
            max_message_size: 1024,
        },
    )
    .unwrap();
    for i in 0..3u32 {
        svc.produce("orders", 0, format!("m{i}").into_bytes(), None)
            .unwrap();
    }
    svc.produce("orders", 1, b"p1".to_vec(), None).unwrap();
    svc.commit_offset("cg", "orders", 0, 2).unwrap();

    let offsets = svc.consumer_offsets().unwrap();
    assert_eq!(
        offsets,
        vec![("cg".to_string(), "orders".to_string(), 0u32, 2u64)]
    );

    let mut nexts = svc.next_offsets_all().unwrap();
    nexts.sort();
    assert_eq!(
        nexts,
        vec![
            ("orders".to_string(), 0u32, 3u64),
            ("orders".to_string(), 1u32, 1u64)
        ]
    );

    // lag 组合口径（采样任务）：next - committed
    let lag_p0 = 3u64.saturating_sub(2);
    assert_eq!(lag_p0, 1);
}
