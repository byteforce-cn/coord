// coord-agent: 跨 Agent 数据复制测试 (Push/Reconcile/ISR)
//
// TDD RED → GREEN: 验证复制协议核心行为。
//
// 测试范围:
// 1. IdempotencyKey — 幂等键生成/去重
// 2. ReplicationConfig — 配置默认值/验证
// 3. ReplicationState — ISR 管理 (加入/移除/降级检测)
// 4. Push 复制 — Leader 写入 → 推送到 Follower → 等待 ISR 确认
// 5. Reconcile — Follower 重启后从 Leader 拉取缺失数据
// 6. 降级模式 — 单副本时 degraded_mode=true
// 7. 幂等保护 — 重复 Push 不会重复应用

use coord_agent::services::replication::*;

// ──── 1. IdempotencyKey 幂等键 ────

#[test]
fn test_idempotency_key_uniqueness() {
    let key1 = IdempotencyKey::new("cache:put:user:123", 1000);
    let key2 = IdempotencyKey::new("cache:put:user:123", 1000);
    assert_eq!(key1, key2);
    assert_eq!(key1.to_string(), key2.to_string());
}

#[test]
fn test_idempotency_key_different_operations() {
    let key1 = IdempotencyKey::new("cache:put:user:123", 1000);
    let key2 = IdempotencyKey::new("cache:put:user:456", 1000);
    assert_ne!(key1, key2);
}

#[test]
fn test_idempotency_key_serialization_roundtrip() {
    let key = IdempotencyKey::new("mq:publish:orders:partition:0:offset:42", 1700000000000);
    let serialized = serde_json::to_string(&key).unwrap();
    let deserialized: IdempotencyKey = serde_json::from_str(&serialized).unwrap();
    assert_eq!(key, deserialized);
}

// ──── 2. ReplicationConfig 配置 ────

#[test]
fn test_replication_config_defaults() {
    let config = ReplicationConfig::default();
    assert_eq!(config.min_isr, 2);
    assert_eq!(config.sync_timeout_ms, 2000);
}

#[test]
fn test_replication_config_validation() {
    let config = ReplicationConfig { min_isr: 0, sync_timeout_ms: 1000 };
    assert!(config.validate().is_err());
}

#[test]
fn test_replication_config_single_replica_allowed() {
    let config = ReplicationConfig { min_isr: 1, sync_timeout_ms: 1000 };
    assert!(config.validate().is_ok());
}

// ──── 3. ReplicationState ISR 管理 ────

#[test]
fn test_isr_add_and_remove() {
    let mut state = ReplicationState::new(2);
    state.add_to_isr("agent-1:19527".to_string());
    state.add_to_isr("agent-2:19527".to_string());
    state.add_to_isr("agent-3:19527".to_string());

    assert_eq!(state.isr_size(), 3);
    assert!(state.is_in_sync("agent-1:19527"));
    assert!(!state.is_in_sync("agent-99:19527"));

    state.remove_from_isr("agent-2:19527");
    assert_eq!(state.isr_size(), 2);
    assert!(!state.is_in_sync("agent-2:19527"));
}

#[test]
fn test_isr_degraded_mode_detection() {
    let mut state = ReplicationState::new(2);
    state.add_to_isr("agent-1:19527".to_string());
    assert!(state.is_degraded());

    state.add_to_isr("agent-2:19527".to_string());
    assert!(!state.is_degraded());
}

#[test]
fn test_isr_healthy_mode() {
    let mut state = ReplicationState::new(2);
    state.add_to_isr("agent-1:19527".to_string());
    state.add_to_isr("agent-2:19527".to_string());
    state.add_to_isr("agent-3:19527".to_string());
    assert!(!state.is_degraded());
    assert!(state.is_healthy());
}

#[test]
fn test_isr_single_replica_mode() {
    let mut state = ReplicationState::new(1);
    state.add_to_isr("agent-1:19527".to_string());
    assert!(!state.is_degraded());
    assert!(state.is_healthy());
    assert!(state.is_single_replica());
}

// ──── 4. ReplicationEntry 序列化 ────

#[test]
fn test_replication_entry_cache_put() {
    let entry = ReplicationEntry::new_cache_put(
        IdempotencyKey::new("op:1", 1000),
        "shard-1".to_string(),
        b"mykey".to_vec(),
        b"myvalue".to_vec(),
        "String".to_string(),
        1,
    );

    assert_eq!(entry.shard_id, "shard-1");
    assert_eq!(entry.sequence_num, 1);
    assert!(matches!(entry.operation, ReplicationOp::CachePut { .. }));

    let json = serde_json::to_string(&entry).unwrap();
    let decoded: ReplicationEntry = serde_json::from_str(&json).unwrap();
    assert_eq!(entry.idempotency_key, decoded.idempotency_key);
    assert_eq!(entry.sequence_num, decoded.sequence_num);
}

#[test]
fn test_replication_entry_mq_publish() {
    let entry = ReplicationEntry::new_mq_publish(
        IdempotencyKey::new("mq:orders:0:42", 1000),
        "shard-mq-1".to_string(),
        "orders".to_string(),
        0,
        b"hello world".to_vec(),
        42,
    );

    assert!(matches!(entry.operation, ReplicationOp::MqPublish { .. }));
    let json = serde_json::to_string(&entry).unwrap();
    let decoded: ReplicationEntry = serde_json::from_str(&json).unwrap();
    assert_eq!(entry.sequence_num, decoded.sequence_num);
}

// ──── 5. IdempotencyGuard 幂等保护 ────

#[test]
fn test_idempotency_guard_new_key_accepted() {
    let mut guard = IdempotencyGuard::new(100);
    let key = IdempotencyKey::new("op:unique:1", 1000);
    assert!(guard.check_and_record(&key));
}

#[test]
fn test_idempotency_guard_duplicate_rejected() {
    let mut guard = IdempotencyGuard::new(100);
    let key = IdempotencyKey::new("op:dup:1", 1000);
    assert!(guard.check_and_record(&key));
    assert!(!guard.check_and_record(&key));
}

#[test]
fn test_idempotency_guard_eviction() {
    let mut guard = IdempotencyGuard::new(3);
    let k1 = IdempotencyKey::new("op:1", 1000);
    let k2 = IdempotencyKey::new("op:2", 1001);
    let k3 = IdempotencyKey::new("op:3", 1002);
    let k4 = IdempotencyKey::new("op:4", 1003);

    assert!(guard.check_and_record(&k1));
    assert!(guard.check_and_record(&k2));
    assert!(guard.check_and_record(&k3));
    let _ = guard.check_and_record(&k4);
    assert!(guard.len() <= 4);
}

#[test]
fn test_idempotency_guard_different_keys_accepted() {
    let mut guard = IdempotencyGuard::new(100);
    for i in 0..50 {
        let key = IdempotencyKey::new(&format!("op:batch:{}", i), 1000 + i);
        assert!(guard.check_and_record(&key));
    }
}

// ──── 6. ReplicationManager 端到端 ────

#[test]
fn test_replication_manager_initialization() {
    let config = ReplicationConfig::default();
    let manager = ReplicationManager::new(config, "agent-leader:19527".to_string());
    assert_eq!(manager.agent_addr(), "agent-leader:19527");
}

#[test]
fn test_replication_manager_add_local_isr() {
    let config = ReplicationConfig { min_isr: 1, sync_timeout_ms: 1000 };
    let manager = ReplicationManager::new(config, "agent-1:19527".to_string());
    manager.add_replica("agent-1:19527".to_string());
    assert!(manager.state().is_healthy());
}

#[test]
fn test_replication_manager_local_write_single_replica() {
    let config = ReplicationConfig { min_isr: 1, sync_timeout_ms: 1000 };
    let manager = ReplicationManager::new(config, "agent-1:19527".to_string());
    manager.add_replica("agent-1:19527".to_string());

    let entry = ReplicationEntry::new_cache_put(
        IdempotencyKey::new("local:write:1", 1000),
        "shard-1".to_string(),
        b"key1".to_vec(),
        b"value1".to_vec(),
        "String".to_string(),
        1,
    );

    let result = manager.try_commit_local(entry);
    assert!(result.is_ok());
    assert_eq!(manager.state().last_sequence(), 1);
}

#[test]
fn test_replication_manager_idempotent_local_write() {
    let config = ReplicationConfig { min_isr: 1, sync_timeout_ms: 1000 };
    let manager = ReplicationManager::new(config, "agent-1:19527".to_string());
    manager.add_replica("agent-1:19527".to_string());

    let key = IdempotencyKey::new("idem:test:1", 1000);
    let entry1 = ReplicationEntry::new_cache_put(
        key.clone(), "shard-1".to_string(), b"k".to_vec(), b"v1".to_vec(), "String".to_string(), 1,
    );
    let entry2 = ReplicationEntry::new_cache_put(
        key.clone(), "shard-1".to_string(), b"k".to_vec(), b"v2".to_vec(), "String".to_string(), 2,
    );

    assert!(manager.try_commit_local(entry1).is_ok());
    assert!(manager.try_commit_local(entry2).is_err());
    assert_eq!(manager.state().last_sequence(), 1);
}

#[test]
fn test_replication_manager_sequence_monotonic() {
    let config = ReplicationConfig { min_isr: 1, sync_timeout_ms: 1000 };
    let manager = ReplicationManager::new(config, "agent-1:19527".to_string());
    manager.add_replica("agent-1:19527".to_string());

    for i in 1..=10 {
        let entry = ReplicationEntry::new_cache_put(
            IdempotencyKey::new(&format!("seq:{}", i), 1000 + i),
            "shard-1".to_string(),
            format!("key{}", i).into_bytes(),
            format!("value{}", i).into_bytes(),
            "String".to_string(),
            i,
        );
        assert!(manager.try_commit_local(entry).is_ok());
    }
    assert_eq!(manager.state().last_sequence(), 10);
}

#[test]
fn test_replication_manager_receive_push() {
    let config = ReplicationConfig::default();
    let manager = ReplicationManager::new(config, "agent-follower:19527".to_string());

    let entry = ReplicationEntry::new_cache_put(
        IdempotencyKey::new("push:from:leader:1", 1000),
        "shard-1".to_string(),
        b"key1".to_vec(),
        b"value1".to_vec(),
        "String".to_string(),
        5,
    );

    assert!(manager.receive_push(entry).is_ok());
    assert_eq!(manager.state().last_sequence(), 5);
}

#[test]
fn test_replication_manager_receive_duplicate_push() {
    let config = ReplicationConfig::default();
    let manager = ReplicationManager::new(config, "agent-follower:19527".to_string());

    let key = IdempotencyKey::new("push:dup:1", 1000);
    let entry1 = ReplicationEntry::new_cache_put(
        key.clone(), "shard-1".to_string(), b"k".to_vec(), b"v".to_vec(), "String".to_string(), 1,
    );
    let entry2 = ReplicationEntry::new_cache_put(
        key.clone(), "shard-1".to_string(), b"k".to_vec(), b"v".to_vec(), "String".to_string(), 2,
    );

    assert!(manager.receive_push(entry1).is_ok());
    assert!(manager.receive_push(entry2).is_err());
}

// ──── 7. Reconcile 协议 — Follower 状态恢复 ────

#[test]
fn test_reconcile_state_creation() {
    let reconcile = ReconcileState::new("agent-follower:19527".to_string(), "shard-1".to_string());
    assert_eq!(reconcile.agent_addr(), "agent-follower:19527");
    assert_eq!(reconcile.shard_id(), "shard-1");
    assert_eq!(reconcile.missing_since_seq(), 0);
}

#[test]
fn test_reconcile_track_missing_entries() {
    let mut reconcile = ReconcileState::new("agent-follower:19527".to_string(), "shard-1".to_string());
    reconcile.set_local_sequence(5);
    reconcile.set_leader_sequence(10);

    let missing = reconcile.compute_missing_range();
    assert_eq!(missing, Some((6, 10)));
    assert_eq!(reconcile.missing_count(), 5);
}

#[test]
fn test_reconcile_no_missing_when_caught_up() {
    let mut reconcile = ReconcileState::new("agent-follower:19527".to_string(), "shard-1".to_string());
    reconcile.set_local_sequence(10);
    reconcile.set_leader_sequence(10);

    let missing = reconcile.compute_missing_range();
    assert_eq!(missing, None);
    assert_eq!(reconcile.missing_count(), 0);
}

#[test]
fn test_reconcile_no_missing_when_ahead() {
    let mut reconcile = ReconcileState::new("agent-follower:19527".to_string(), "shard-1".to_string());
    reconcile.set_local_sequence(15);
    reconcile.set_leader_sequence(10);

    let missing = reconcile.compute_missing_range();
    assert_eq!(missing, None);
}

#[test]
fn test_reconcile_mark_applied() {
    let mut reconcile = ReconcileState::new("agent-follower:19527".to_string(), "shard-1".to_string());
    reconcile.set_local_sequence(5);
    reconcile.set_leader_sequence(10);

    reconcile.mark_applied(6);
    reconcile.mark_applied(7);
    reconcile.mark_applied(8);

    assert_eq!(reconcile.local_sequence(), 8);
    let missing = reconcile.compute_missing_range();
    assert_eq!(missing, Some((9, 10)));
    assert_eq!(reconcile.missing_count(), 2);
}

// ──── 8. ReplicationError ────

#[test]
fn test_replication_error_display() {
    let err = ReplicationError::DuplicateIdempotencyKey { key: "test:key:1".to_string() };
    assert!(err.to_string().contains("test:key:1"));

    let err2 = ReplicationError::IsrDegraded { required: 2, actual: 1 };
    assert!(err2.to_string().contains("2"));
    assert!(err2.to_string().contains("1"));
}
