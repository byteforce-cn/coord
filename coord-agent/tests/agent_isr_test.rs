// coord-agent: 跨 Agent ISR 复制集成测试（Phase D，v2.1 已落地）
//
// 覆盖（docs/cache-mq-isr-evaluation.md Phase A-D）:
// 1. ReplicationManager — peers / Leader 静态分配（min-addr）/ effective_min_isr / isr_satisfied
// 2. ReplicationEntry — proto 往返
// 3. MQ 数据面复制 — Follower apply 保留 offset（C1）/ 幂等 / 自动建 topic
// 4. Cache 数据面复制 — Put/Delete 复制 / 绝对 TTL（C5）/ pop 仅 Leader（C2）
// 5. 双 agent gRPC 集成 — MQ produce→follower poll；Cache set→follower get；
//    降级（follower 宕机 → 写拒绝）；Follower 写被拒（Q4）
// 6. Reconcile — Follower 从 Leader 拉取缺失序列号区间重放

use std::time::Duration;

use coord_agent::services::replication::{
    IdempotencyKey, ReplicationConfig, ReplicationEntry, ReplicationManager, ReplicationOp,
    ReplicatedStore,
};
use coord_agent::{AgentConfig, AgentServer, BaseService};

fn find_port() -> u16 {
    use std::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// ISR 测试配置：启用 mq/cache/replication，隔离数据目录
fn isr_config(port: u16, peers: Vec<String>, min_isr: usize, tag: &str) -> AgentConfig {
    let mut config = AgentConfig {
        agent_addr: format!("127.0.0.1:{port}"),
        http_addr: format!("127.0.0.1:{}", find_port()),
        data_dir: std::env::temp_dir()
            .join(format!("coord-isr-{tag}-{port}-{}", std::process::id()))
            .to_string_lossy()
            .into_owned(),
        static_peers: vec![], // 骨架模式：无需 Server
        ..Default::default()
    };
    config.services.mq = true;
    config.services.cache = true;
    config.services.replication = true;
    config.replication = ReplicationConfig {
        min_isr,
        sync_timeout_ms: 2000,
    };
    config.replication_peers = peers;
    config
}

/// 启动 Agent gRPC server（后台）
async fn spawn_agent(config: AgentConfig) -> (tokio::task::JoinHandle<()>, String) {
    let addr = config.agent_addr.clone();
    let server = AgentServer::new(config);
    let handle = tokio::spawn(async move {
        let _ = server.serve().await;
    });
    // 等待端口监听
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    (handle, addr)
}

// ════════════════════════════════════════════════════════════
// 1. ReplicationManager — Leader 分配 / min_isr / peers
// ════════════════════════════════════════════════════════════

#[test]
fn test_leader_assignment_min_addr() {
    let manager = ReplicationManager::new(ReplicationConfig::default(), "127.0.0.1:20002".into());
    manager.set_peers(vec![
        "127.0.0.1:10001".to_string(),
        "127.0.0.1:30003".to_string(),
    ]);
    // 显式覆盖优先
    manager.set_shard_leader("mq:t", "127.0.0.1:30003".into());
    assert_eq!(manager.shard_leader("mq:t"), "127.0.0.1:30003");
    // 未覆盖 → min-addr
    assert_eq!(manager.shard_leader("mq:other"), "127.0.0.1:10001");
    assert!(!manager.is_leader("mq:other"));
    assert_eq!(manager.isr_members().len(), 3); // 自身 + 2 对端
}

#[test]
fn test_effective_min_isr_single_agent_zero_break() {
    // C6：单 agent（无对端）min_isr 自动降级为 1，零破坏
    let manager = ReplicationManager::new(ReplicationConfig::default(), "a:1".into());
    assert_eq!(manager.effective_min_isr(), 1);
    assert!(manager.isr_satisfied(1));
    assert!(!manager.is_degraded());
}

#[test]
fn test_effective_min_isr_two_agents() {
    let manager = ReplicationManager::new(ReplicationConfig::default(), "a:1".into());
    manager.add_peer("b:2".into());
    assert_eq!(manager.effective_min_isr(), 2);
    assert!(manager.isr_satisfied(2));
    assert!(!manager.isr_satisfied(1));
    assert!(!manager.is_degraded());
}

#[test]
fn test_peers_add_remove() {
    let manager = ReplicationManager::new(ReplicationConfig::default(), "a:1".into());
    manager.add_peer("b:2".into());
    manager.add_peer("b:2".into()); // 幂等
    manager.add_peer("a:1".into()); // 忽略自身
    assert_eq!(manager.peers(), vec!["b:2".to_string()]);
    manager.remove_peer("b:2");
    assert!(manager.peers().is_empty());
}

// ════════════════════════════════════════════════════════════
// 2. ReplicationEntry — proto 往返
// ════════════════════════════════════════════════════════════

#[test]
fn test_replication_entry_proto_roundtrip() {
    let entry = ReplicationEntry::new_mq_publish(
        IdempotencyKey::new("mq:publish:orders:0:42", 1700000000000),
        "mq:orders".to_string(),
        "orders".to_string(),
        0,
        42, // C1: Leader 分配的 offset
        b"hello".to_vec(),
        7,
    );
    let proto = entry.to_proto();
    let decoded = ReplicationEntry::from_proto(&proto).unwrap();
    assert_eq!(entry, decoded);
    assert!(matches!(
        decoded.operation,
        ReplicationOp::MqPublish { offset: 42, .. }
    ));

    let cache = ReplicationEntry::new_cache_put(
        IdempotencyKey::new("cache:put:k", 1),
        "cache".to_string(),
        b"k".to_vec(),
        b"v".to_vec(),
        "string".to_string(),
        1,
    );
    assert_eq!(cache, ReplicationEntry::from_proto(&cache.to_proto()).unwrap());
}

// ════════════════════════════════════════════════════════════
// 3. MQ 数据面复制（服务级）
// ════════════════════════════════════════════════════════════

async fn mq_svc(dir: &std::path::Path) -> coord_agent::services::mq::MessageQueueService {
    let svc = coord_agent::services::mq::MessageQueueService::new(
        dir.to_path_buf(),
        1024 * 1024 * 1024,
    );
    svc.start().await.expect("start mq");
    svc
}

async fn cache_svc(dir: &std::path::Path) -> coord_agent::services::cache::CacheService {
    let svc = coord_agent::services::cache::CacheService::new(dir.to_path_buf(), 1024 * 1024 * 1024, 3600);
    svc.start().await.expect("start cache");
    svc
}

#[tokio::test]
async fn test_mq_follower_apply_preserves_offset() {
    let dir = tempfile::tempdir().unwrap();
    let leader = mq_svc(dir.path()).await;
    leader
        .create_topic(
            "orders",
            coord_agent::services::mq::TopicConfig {
                partitions: 1,
                retention_secs: 86400,
                max_message_size: 1024 * 1024,
            },
        )
        .unwrap();

    // Leader 单 agent（min_isr=1）：复制生产
    let leader_mgr = ReplicationManager::new(
        ReplicationConfig { min_isr: 1, sync_timeout_ms: 1000 },
        "leader:1".into(),
    );
    leader.set_replication(Some(std::sync::Arc::new(leader_mgr)));
    let offset = leader
        .produce_replicated("orders", 0, b"msg-1".to_vec(), None)
        .await
        .unwrap();
    assert_eq!(offset, 0);

    // Follower 本地服务：直接应用 Leader 的复制条目
    let follower = mq_svc(&tempfile::tempdir().unwrap().path().to_path_buf()).await;
    let entry = ReplicationEntry::new_mq_publish(
        IdempotencyKey::new("mq:publish:orders:0:0", 1),
        "mq:orders".to_string(),
        "orders".to_string(),
        0,
        0, // Leader 分配的 offset
        b"msg-1".to_vec(),
        1,
    );
    follower.apply_entry(&entry).unwrap();

    // Follower 可按相同 offset 读到消息（C1：offset 全局一致）
    let msgs = follower.consume("orders", 0, 0, 100).unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].offset, 0);
    assert_eq!(msgs[0].payload, b"msg-1".to_vec());

    // 幂等：重复应用不重复写入
    follower.apply_entry(&entry).unwrap();
    let msgs2 = follower.consume("orders", 0, 0, 100).unwrap();
    assert_eq!(msgs2.len(), 1);

    // 本地序列号已推进
    assert_eq!(follower.last_local_sequence("mq:orders"), 1);
}

#[tokio::test]
async fn test_mq_apply_auto_creates_topic() {
    let follower = mq_svc(&tempfile::tempdir().unwrap().path().to_path_buf()).await;
    // topic 未创建时 apply 也应成功（自动建 topic）
    let entry = ReplicationEntry::new_mq_publish(
        IdempotencyKey::new("mq:publish:newtopic:1:0", 1),
        "mq:newtopic".to_string(),
        "newtopic".to_string(),
        1,
        0,
        b"data".to_vec(),
        1,
    );
    follower.apply_entry(&entry).unwrap();
    let msgs = follower.consume("newtopic", 1, 0, 100).unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].payload, b"data".to_vec());
}

#[tokio::test]
async fn test_mq_idempotency_persists_across_restart() {
    // Q2：幂等键持久化到 redb；服务重启后重复 apply 不重复写入
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    let entry = ReplicationEntry::new_mq_publish(
        IdempotencyKey::new("mq:publish:restart:0:0", 1),
        "mq:restart".to_string(),
        "restart".to_string(),
        0,
        0,
        b"once".to_vec(),
        1,
    );

    {
        let svc = mq_svc(&path).await;
        svc.apply_entry(&entry).unwrap();
        assert_eq!(svc.last_local_sequence("mq:restart"), 1);
    }
    // 重启（同一 redb）
    let svc = mq_svc(&path).await;
    svc.apply_entry(&entry).unwrap(); // 重复 apply → 幂等跳过
    let msgs = svc.consume("restart", 0, 0, 100).unwrap();
    assert_eq!(msgs.len(), 1, "duplicate apply after restart must be skipped");
    assert_eq!(svc.last_local_sequence("mq:restart"), 1);
}

// ════════════════════════════════════════════════════════════
// 4. Cache 数据面复制（服务级）
// ════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_cache_replicated_put_and_delete() {
    let leader = cache_svc(&tempfile::tempdir().unwrap().path().to_path_buf()).await;
    let leader_mgr = ReplicationManager::new(
        ReplicationConfig { min_isr: 1, sync_timeout_ms: 1000 },
        "leader:1".into(),
    );
    leader.set_replication(Some(std::sync::Arc::new(leader_mgr)));

    let follower = cache_svc(&tempfile::tempdir().unwrap().path().to_path_buf()).await;

    // Leader 复制写 string
    leader.string_put_replicated("k1", b"v1".to_vec(), None).await.unwrap();
    // 读取复制日志 → 应用到 Follower
    let entries = leader.read_entries("cache", 1, 0);
    assert!(!entries.is_empty());
    for e in &entries {
        follower.apply_entry(e).unwrap();
    }
    assert_eq!(follower.string_get("k1").unwrap(), Some(b"v1".to_vec()));

    // 复制删除
    leader.string_delete_replicated("k1").await.unwrap();
    let entries = leader.read_entries("cache", entries.len() as u64 + 1, 0);
    for e in &entries {
        follower.apply_entry(e).unwrap();
    }
    assert_eq!(follower.string_get("k1").unwrap(), None);
}

#[tokio::test]
async fn test_cache_replicated_ttl_absolute() {
    // C5：复制 Leader 计算的绝对到期时间戳；Follower 原样应用，独立到期
    let leader = cache_svc(&tempfile::tempdir().unwrap().path().to_path_buf()).await;
    let leader_mgr = ReplicationManager::new(
        ReplicationConfig { min_isr: 1, sync_timeout_ms: 1000 },
        "leader:1".into(),
    );
    leader.set_replication(Some(std::sync::Arc::new(leader_mgr)));

    let follower = cache_svc(&tempfile::tempdir().unwrap().path().to_path_buf()).await;
    leader.string_put_replicated("ttl-key", b"v".to_vec(), Some(60)).await.unwrap();
    let entries = leader.read_entries("cache", 1, 0);
    for e in &entries {
        follower.apply_entry(e).unwrap();
    }
    // Follower 立即可读（未到期）
    assert_eq!(follower.string_get("ttl-key").unwrap(), Some(b"v".to_vec()));
}

#[tokio::test]
async fn test_cache_pop_leader_only() {
    // C2：pop 仅 Leader；非 Leader 拒绝
    let leader = cache_svc(&tempfile::tempdir().unwrap().path().to_path_buf()).await;
    let leader_mgr = ReplicationManager::new(
        ReplicationConfig { min_isr: 1, sync_timeout_ms: 1000 },
        "a:1".into(),
    );
    leader_mgr.add_peer("b:2".into());
    // 显式指定 b 为 Leader（本服务是 Follower）
    leader_mgr.set_shard_leader("cache", "b:2".into());
    leader.set_replication(Some(std::sync::Arc::new(leader_mgr)));

    let err = leader.list_pop_replicated("q", true).await.unwrap_err();
    assert!(err.to_string().contains("not leader"), "err: {err}");
}

// ════════════════════════════════════════════════════════════
// 5. 双 agent gRPC 集成（真实 Replica 服务）
// ════════════════════════════════════════════════════════════

/// 启动两个 agent，返回 (leader_addr, follower_addr)
async fn spawn_pair(min_isr: usize, tag: &str) -> (tokio::task::JoinHandle<()>, tokio::task::JoinHandle<()>, String, String) {
    let pa = find_port();
    let pb = find_port();
    let addr_a = format!("127.0.0.1:{pa}");
    let addr_b = format!("127.0.0.1:{pb}");
    let (leader_addr, follower_addr) = if addr_a < addr_b {
        (addr_a.clone(), addr_b.clone())
    } else {
        (addr_b.clone(), addr_a.clone())
    };
    let ca = isr_config(pa, vec![addr_b.clone()], min_isr, tag);
    let cb = isr_config(pb, vec![addr_a.clone()], min_isr, tag);
    let ha = spawn_agent(ca).await;
    let hb = spawn_agent(cb).await;
    (ha.0, hb.0, leader_addr, follower_addr)
}

async fn mq_client(addr: &str) -> coord_proto::agent::mq_client::MqClient<tonic::transport::Channel> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    coord_proto::agent::mq_client::MqClient::new(channel)
}

async fn cache_client(addr: &str) -> coord_proto::agent::cache_client::CacheClient<tonic::transport::Channel> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    coord_proto::agent::cache_client::CacheClient::new(channel)
}

#[tokio::test]
async fn test_two_agent_mq_replication() {
    let (ha, hb, leader, follower) = spawn_pair(2, "mq2").await;

    // 在 Leader 上建 topic
    let mut lc = mq_client(&leader).await;
    lc.create_topic(coord_proto::agent::MqCreateTopicRequest {
        topic: "orders".to_string(),
        partitions: 1,
    })
    .await
    .unwrap();

    // Leader 发布 → 同步复制到 Follower
    let resp = lc
        .publish(coord_proto::agent::MqPublishRequest {
            topic: "orders".to_string(),
            partition: 0,
            key: Vec::new(),
            payload: b"replicated-msg".to_vec(),
            idempotency_key: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.offset, 0);

    // Follower 可 poll 到（读本地副本，offset 一致）
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut fc = mq_client(&follower).await;
    let poll = fc
        .poll(coord_proto::agent::MqPollRequest {
            topic: "orders".to_string(),
            partition: 0,
            consumer_group: "cg".to_string(),
            start_offset: 0,
            max_count: 100,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(poll.messages.len(), 1, "follower should have the replicated message");
    assert_eq!(poll.messages[0].payload, b"replicated-msg".to_vec());
    assert_eq!(poll.messages[0].offset, 0);

    ha.abort();
    hb.abort();
}

#[tokio::test]
async fn test_two_agent_cache_replication() {
    let (ha, hb, leader, follower) = spawn_pair(2, "cache2").await;

    // Leader 写 Cache → 同步复制到 Follower
    let mut lc = cache_client(&leader).await;
    lc.set(coord_proto::agent::CacheSetRequest {
        key: "shared-key".to_string(),
        value: b"shared-value".to_vec(),
        ttl_seconds: 0,
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut fc = cache_client(&follower).await;
    let resp = fc
        .get(coord_proto::agent::CacheGetRequest {
            key: "shared-key".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(resp.found, "follower should read replicated cache value");
    assert_eq!(resp.value, b"shared-value".to_vec());

    ha.abort();
    hb.abort();
}

#[tokio::test]
async fn test_follower_write_rejected_not_leader() {
    // Q4：非 Leader 的写请求返回明确「非 Leader」错误
    let (ha, hb, leader, follower) = spawn_pair(2, "notleader").await;

    let mut lc = mq_client(&leader).await;
    lc.create_topic(coord_proto::agent::MqCreateTopicRequest {
        topic: "t".to_string(),
        partitions: 1,
    })
    .await
    .unwrap();

    // 向 Follower 发布 → 应拒绝
    let mut fc = mq_client(&follower).await;
    let result = fc
        .publish(coord_proto::agent::MqPublishRequest {
            topic: "t".to_string(),
            partition: 0,
            key: Vec::new(),
            payload: b"x".to_vec(),
            idempotency_key: String::new(),
        })
        .await;
    assert!(result.is_err(), "follower publish should be rejected");
    let status = result.unwrap_err();
    assert!(
        status.message().contains("not leader"),
        "status: {status}"
    );

    ha.abort();
    hb.abort();
}

#[tokio::test]
async fn test_two_agent_degraded_when_follower_down() {
    // R3：Follower 宕机（无法确认）→ 同步复制降级 → Leader 拒绝写（min_isr=2）
    let pa = find_port();
    let pb = find_port();
    let addr_a = format!("127.0.0.1:{pa}");
    let addr_b = format!("127.0.0.1:{pb}");
    let (leader_addr, _down_addr) = if addr_a < addr_b {
        (addr_a.clone(), addr_b.clone())
    } else {
        (addr_b.clone(), addr_a.clone())
    };
    let config = isr_config(pa, vec![addr_b], 2, "degraded");
    let (handle, addr) = spawn_agent(config).await;
    assert_eq!(addr, leader_addr);

    // 建 topic 并发布 → Follower 不在线 → IsrDegraded
    let mut lc = mq_client(&addr).await;
    lc.create_topic(coord_proto::agent::MqCreateTopicRequest {
        topic: "d".to_string(),
        partitions: 1,
    })
    .await
    .unwrap();
    let result = lc
        .publish(coord_proto::agent::MqPublishRequest {
            topic: "d".to_string(),
            partition: 0,
            key: Vec::new(),
            payload: b"x".to_vec(),
            idempotency_key: String::new(),
        })
        .await;
    assert!(result.is_err(), "write should be rejected when ISR degraded");

    handle.abort();
}

// ════════════════════════════════════════════════════════════
// 6. Reconcile — Follower 从 Leader 拉取缺失序列号区间重放
// ════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_reconcile_catch_up() {
    // Leader：单 agent（min_isr=1）持续生产 → 日志中有 N 条条目
    let pa = find_port();
    let leader_addr = format!("127.0.0.1:{pa}");
    let config = isr_config(pa, vec![], 1, "reconcile");
    let (handle, _) = spawn_agent(config).await;

    let mut lc = mq_client(&leader_addr).await;
    lc.create_topic(coord_proto::agent::MqCreateTopicRequest {
        topic: "recon".to_string(),
        partitions: 1,
    })
    .await
    .unwrap();
    for i in 0..5u32 {
        lc.publish(coord_proto::agent::MqPublishRequest {
            topic: "recon".to_string(),
            partition: 0,
            key: Vec::new(),
            payload: format!("m{i}").into_bytes(),
            idempotency_key: String::new(),
        })
        .await
        .unwrap();
    }

    // Follower：全新本地服务 + 复制管理器（peers=[Leader]），通过 Reconcile 追赶
    let dir = tempfile::tempdir().unwrap();
    let follower = mq_svc(dir.path()).await;
    let follower_mgr = std::sync::Arc::new(ReplicationManager::new(
        ReplicationConfig { min_isr: 1, sync_timeout_ms: 2000 },
        format!("follower:{}", find_port()),
    ));
    follower_mgr.add_peer(leader_addr.clone());

    let applied = follower_mgr
        .pull_and_catch_up(&follower, "mq:recon", 1)
        .await
        .unwrap();
    assert_eq!(applied, 5, "follower should pull and apply 5 entries");

    let msgs = follower.consume("recon", 0, 0, 100).unwrap();
    assert_eq!(msgs.len(), 5);
    assert_eq!(msgs[0].payload, b"m0".to_vec());
    assert_eq!(msgs[4].payload, b"m4".to_vec());

    handle.abort();
}
