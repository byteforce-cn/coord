//! Spike 2026-10 · Spike 3（Outbox + 崩溃恢复）机制级集成测试。
//!
//! 覆盖：状态 + outbox 同事务原子性、relay 投递 + event_id 去重、持久化定时器
//! 到期扫描、以及「引擎重建后按 KV 恢复挂起实例」两条路径（wait / listen）。
//!
//! 进程级 kill -9 证据见 `spike/2026-10/spike-3-recovery/scripts/repro.sh`
//! （真实二进制 + SIGKILL，本文件是机制级的可重复判据）。

mod common;

use std::sync::Arc;
use std::time::Duration;

use coord_agent::cache::AgentCache;
use coord_agent::service::BaseService;
use coord_agent::services::mq::MessageQueueService;
use coord_agent::services::mq_event_provider::{CursorStore, MqEventProvider};
use coord_agent::services::outbox_kv::{KvCursorStore, KvOutboxStore};
use coord_agent::services::outbox_relay::{drain_topic, MqOutboxSink};
use coord_agent::services::workflow::phase4::{DurableEngineOptions, WorkflowEngineService};
use coord_agent::services::workflow_store::KvWorkflowStore;
use coord_agent::{AgentInner, WorkflowDispatchConfig};
use coord_client::config::Config;
use coord_client::Client;
use coord_core::workflow::durable::{
    AtomicOutboxCommit, DurableTimerStore, OutboxRecord, OutboxRelay, OutboxSink, OutboxStatus,
    OutboxStore, RelayPolicy, TimerKind, TimerRecord,
};
use coord_core::workflow::model::{
    InstanceStatus, SuspensionMeta, TaskFrame, TaskStatus, WorkflowInstance,
};
use coord_core::workflow::ports::{EventProvider, WorkflowStore};

const TOPIC: &str = "com.china-workflow.spike3.events";

/// 进程内单节点服务 + 客户端；**必须持有到测试结束**（drop 会关闭 gRPC 服务）。
struct TestEnv {
    inner: Arc<AgentInner>,
    tmp: tempfile::TempDir,
    _shutdown: tokio::sync::oneshot::Sender<()>,
    _grpc: tokio::task::JoinHandle<()>,
    _raft: tokio::task::JoinHandle<()>,
}

impl TestEnv {
    fn inner(&self) -> Arc<AgentInner> {
        Arc::clone(&self.inner)
    }

    fn mq_dir(&self) -> std::path::PathBuf {
        self.tmp.path().to_path_buf()
    }
}

async fn setup() -> TestEnv {
    let (addr, shutdown, grpc, raft, tmp) = common::start_test_server().await;
    // `start_test_server` 等到选出 Leader，但监听器可能尚未接管：先确认 TCP 可连。
    common::wait_tcp_ready(&addr, Duration::from_secs(10)).await;
    let client = Client::connect_direct(Config::new(vec![addr]))
        .await
        .expect("connect direct");
    let inner = Arc::new(AgentInner {
        client,
        cache: AgentCache::new(256, 0, 32, 0),
    });
    TestEnv {
        inner,
        tmp,
        _shutdown: shutdown,
        _grpc: grpc,
        _raft: raft,
    }
}

async fn start_mq(dir: &std::path::Path) -> Arc<MessageQueueService> {
    let mq = Arc::new(MessageQueueService::new(
        dir.to_path_buf(),
        1024 * 1024 * 1024,
    ));
    mq.start().await.expect("start mq");
    mq
}

fn instance_json(id: &str) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "definition_ns": "spike3",
        "definition_name": "atomic",
        "definition_version": "1.0.0",
        "status": "running",
        "context": {},
        "current_task_index": 0,
        "created_at": 1,
        "updated_at": 1
    })
}

fn inst(id: &str) -> WorkflowInstance {
    serde_json::from_value(instance_json(id)).expect("workflow instance json")
}

fn outbox_rec(aggregate: &str, event_type: &str, id: &str, now: i64) -> OutboxRecord {
    OutboxRecord::lifecycle(
        aggregate,
        event_type,
        serde_json::json!({"k": id}),
        now,
        id.to_string(),
    )
}

/// 判据 1（问题 1）：状态写入 + outbox 写入在**同一个 KV Txn** 原子完成。
///
/// 做法：同一 Txn 的 compare 失败 ⇒ 状态与 outbox 都**不可见**；compare 通过 ⇒
/// 两者**同时**可见。这就是「不存在状态已改而事件缺失的窗口」的机器判据。
#[tokio::test]
async fn state_and_outbox_commit_is_all_or_nothing() {
    let env = setup().await;
    let inner = env.inner();
    let store = Arc::new(KvOutboxStore::new(Arc::clone(&inner)));
    let kv = CoordKv::new(&inner);

    // ── 反向：CAS 预期版本不匹配 ⇒ 事务整体回滚 ──
    let inst_a = inst("atomic-a");
    let rec_a = outbox_rec(
        "atomic-a",
        "com.china-workflow.spike3.created",
        "evt-atomic-a",
        1,
    );
    let committed = store
        .commit_state_and_outbox(&inst_a, &rec_a, Some(999_999))
        .await
        .expect("txn should not error");
    assert!(!committed, "mod_rev CAS 不匹配 ⇒ 事务不应提交");
    assert!(
        kv.get(&kv_instance_key("atomic-a")).await.is_none(),
        "事务回滚 ⇒ 状态不可见（不允许半提交）"
    );
    assert!(
        OutboxStore::list_all(&*store)
            .await
            .unwrap()
            .iter()
            .all(|r| r.id != "evt-atomic-a"),
        "事务回滚 ⇒ outbox 记录不可见（不允许半提交）"
    );

    // ── 正向：CAS 通过 ⇒ 状态与 outbox 同一事务落库 ──
    let inst_b = inst("atomic-b");
    let rec_b = outbox_rec(
        "atomic-b",
        "com.china-workflow.spike3.created",
        "evt-atomic-b",
        2,
    );
    let committed = store
        .commit_state_and_outbox(&inst_b, &rec_b, None)
        .await
        .expect("txn");
    assert!(committed, "无 CAS 约束 ⇒ 事务应提交");

    let raw = kv
        .get(&kv_instance_key("atomic-b"))
        .await
        .expect("state visible");
    let read_back: WorkflowInstance = serde_json::from_slice(&raw).expect("state decodes");
    assert_eq!(read_back.id, "atomic-b");

    let all = OutboxStore::list_all(&*store).await.unwrap();
    let rec = all
        .iter()
        .find(|r| r.id == "evt-atomic-b")
        .expect("outbox record visible");
    assert_eq!(rec.sequence, 1, "per-instance sequence 从 1 起单调");
    assert_eq!(rec.aggregate_id, "atomic-b");
    assert_eq!(rec.status, OutboxStatus::Pending);
    assert_eq!(rec.specversion, "1.0");
    assert!(
        rec.time.ends_with('Z'),
        "CloudEvents time 为 RFC3339: {}",
        rec.time
    );
    assert!(rec.data["k"] == "evt-atomic-b");

    // 同一实例继续追加 ⇒ 序号单调递增（不依赖全局 revision）
    let rec_c = outbox_rec(
        "atomic-b",
        "com.china-workflow.spike3.waiting",
        "evt-atomic-c",
        3,
    );
    store.append(&rec_c).await.unwrap();
    let seq_c = OutboxStore::list_all(&*store)
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.id == "evt-atomic-c")
        .unwrap()
        .sequence;
    assert_eq!(seq_c, 2, "同一 aggregate 的 sequence 单调 +1");
}

/// 批量同事务：一次提交实例状态 + N 条伴生事件，两条 `sequence` 连续、各一条记录。
#[tokio::test]
async fn state_and_multiple_events_commit_atomically() {
    let env = setup().await;
    let inner = env.inner();
    let store = Arc::new(KvOutboxStore::new(Arc::clone(&inner)));
    let kv = CoordKv::new(&inner);

    let inst_a = inst("batch-a");
    let recs = vec![
        outbox_rec(
            "batch-a",
            "com.china-workflow.spike3.created",
            "evt-batch-a-1",
            10,
        ),
        outbox_rec(
            "batch-a",
            "com.china-workflow.spike3.step",
            "evt-batch-a-2",
            11,
        ),
    ];
    let committed = store
        .commit_state_and_outbox_batch(&inst_a, &recs, None)
        .await
        .expect("batch txn");
    assert!(committed, "无 CAS 约束 ⇒ 批量事务应提交");

    let read_back: WorkflowInstance = kv
        .get_instance("batch-a")
        .await
        .expect("state visible with the batch");
    assert_eq!(read_back.id, "batch-a");

    let all = OutboxStore::list_all(&*store).await.unwrap();
    let mut got: Vec<OutboxRecord> = all
        .into_iter()
        .filter(|r| r.aggregate_id == "batch-a")
        .collect();
    got.sort_by_key(|r| r.sequence);
    assert_eq!(got.len(), 2, "同批两条事件各一条记录");
    assert_eq!(got[0].id, "evt-batch-a-1");
    assert_eq!(got[0].sequence, 1);
    assert_eq!(got[1].id, "evt-batch-a-2");
    assert_eq!(got[1].sequence, 2, "sequence 连续；数组顺序即序号顺序");
}

/// 同批重放：全部 id 已存在 ⇒ 幂等 `Ok(true)`，不新增记录。
#[tokio::test]
async fn batch_commit_is_idempotent_on_replay() {
    let env = setup().await;
    let inner = env.inner();
    let store = Arc::new(KvOutboxStore::new(Arc::clone(&inner)));

    let inst = inst("batch-replay");
    let recs = vec![
        outbox_rec(
            "batch-replay",
            "com.china-workflow.spike3.created",
            "evt-replay-1",
            20,
        ),
        outbox_rec(
            "batch-replay",
            "com.china-workflow.spike3.step",
            "evt-replay-2",
            21,
        ),
    ];
    assert!(
        store
            .commit_state_and_outbox_batch(&inst, &recs, None)
            .await
            .unwrap(),
        "首次提交成功"
    );
    let count = |all: Vec<OutboxRecord>| {
        all.into_iter()
            .filter(|r| r.aggregate_id == "batch-replay")
            .count()
    };
    assert_eq!(count(OutboxStore::list_all(&*store).await.unwrap()), 2);

    // 重放同一批 ⇒ 全部 id 已存在 ⇒ 幂等成功，不新增记录
    assert!(
        store
            .commit_state_and_outbox_batch(&inst, &recs, None)
            .await
            .unwrap(),
        "重放应返回幂等成功"
    );
    assert_eq!(
        count(OutboxStore::list_all(&*store).await.unwrap()),
        2,
        "重放不新增记录"
    );
}

/// 空批次（纯状态提交）：只写实例状态，不碰 outbox 的 seq/记录。
#[tokio::test]
async fn empty_batch_commits_state_without_touching_outbox() {
    let env = setup().await;
    let inner = env.inner();
    let store = Arc::new(KvOutboxStore::new(Arc::clone(&inner)));
    let kv = CoordKv::new(&inner);

    // 先落一条已有事件：聚合的 seq 计数器已在 KV 里。
    store
        .append(&outbox_rec(
            "empty-batch",
            "com.china-workflow.spike3.step",
            "evt-seed",
            5,
        ))
        .await
        .unwrap();

    let inst = inst("empty-batch");
    assert!(
        store
            .commit_state_and_outbox_batch(&inst, &[], None)
            .await
            .unwrap(),
        "空批次应提交实例状态"
    );

    let read_back = kv.get_instance("empty-batch").await.expect("state visible");
    assert_eq!(read_back.id, "empty-batch");

    let all = OutboxStore::list_all(&*store).await.unwrap();
    let mine: Vec<OutboxRecord> = all
        .into_iter()
        .filter(|r| r.aggregate_id == "empty-batch")
        .collect();
    assert_eq!(mine.len(), 1, "空批次不新增 outbox 记录");
    assert_eq!(mine[0].id, "evt-seed");
    assert_eq!(mine[0].sequence, 1, "空批次不重写 seq 计数器");
}

/// `Arc<dyn WorkflowStore>` 委托必须转发 CAS 原语。
///
/// 默认 `load_instance_with_rev` 恒返回 `Ok(None)`、`save_instance_atomic` 退化为
/// 普通写并恒返回 `Ok(true)`。若委托不覆盖它们，经 trait object 装配的运行时永远
/// 拿不到修订号，写入不带 `MOD_REV` 比较，并发驱动者互相覆盖（丢更新）。
#[tokio::test]
async fn dyn_workflow_store_forwards_revision_and_cas_primitives() {
    let env = setup().await;
    let store: Arc<dyn WorkflowStore + Send + Sync> = Arc::new(KvWorkflowStore::new(env.inner()));

    let mut inst = inst("dyn-cas");
    store.save_instance(&inst).await.expect("save instance");

    // (a) 经 dyn 路径读回修订号（默认实现会返回 None）。
    let (loaded, rev) = store
        .load_instance_with_rev("dyn-cas")
        .await
        .expect("load_instance_with_rev")
        .expect("dyn path must return a revision, not None");
    assert_eq!(loaded.id, "dyn-cas");

    // (b) 陈旧修订号 CAS 被拒绝，当前修订号 CAS 成功。
    inst.context = serde_json::json!({"v": 1});
    let stale = store
        .save_instance_atomic(&inst, rev + 1)
        .await
        .expect("stale save_instance_atomic");
    assert!(!stale, "dyn path must reject a stale revision CAS");

    inst.context = serde_json::json!({"v": 2});
    let ok = store
        .save_instance_atomic(&inst, rev)
        .await
        .expect("current save_instance_atomic");
    assert!(ok, "dyn path must accept the current revision CAS");

    let (after, new_rev) = store
        .load_instance_with_rev("dyn-cas")
        .await
        .expect("reload")
        .expect("revision after CAS");
    assert!(new_rev > rev, "CAS must advance the revision");
    assert_eq!(
        after.context,
        serde_json::json!({"v": 2}),
        "CAS write visible"
    );
}

/// 判据 2（问题 2）：Relay 投递 + 标位 + 失败退避，事件不丢、重复靠 event_id 去重。
#[tokio::test]
async fn relay_delivers_all_records_and_consumer_dedups_by_event_id() {
    let env = setup().await;
    let inner = env.inner();
    let mq = start_mq(&env.mq_dir()).await;
    let store = Arc::new(KvOutboxStore::new(Arc::clone(&inner)));

    for i in 0..3 {
        store
            .append(&outbox_rec(
                "inst-relay",
                "com.china-workflow.spike3.step",
                &format!("evt-{i}"),
                10 + i as i64,
            ))
            .await
            .unwrap();
    }

    let sink: Arc<dyn OutboxSink> = Arc::new(MqOutboxSink::new(Arc::clone(&mq), TOPIC));
    let relay = OutboxRelay::new(
        Arc::clone(&store) as Arc<dyn OutboxStore>,
        sink,
        RelayPolicy::default(),
    );
    let report = relay.tick(100).await.unwrap();
    assert_eq!(report.delivered, 3, "三条记录一次 tick 全部投递");
    assert_eq!(
        relay.tick(200).await.unwrap().polled,
        0,
        "投递成功后不再轮询"
    );

    // 消费者按 event_id 去重（at-least-once + 幂等）
    let records = drain_topic(&mq, TOPIC, 0).expect("drain topic");
    assert_eq!(records.len(), 3, "MQ 侧不重不漏: {}", records.len());
    let mut seen = std::collections::BTreeSet::new();
    for rec in &records {
        let value: serde_json::Value = serde_json::from_slice(&rec.payload).expect("payload");
        let id = value["id"].as_str().expect("event id").to_string();
        assert!(seen.insert(id), "event_id 重复: 消费者去重未生效");
    }
    assert_eq!(seen.len(), 3);

    // 重复投递（模拟 relay 崩溃重放）⇒ broker 幂等键吸收，offset 不增长
    let again: Arc<dyn OutboxSink> = Arc::new(MqOutboxSink::new(Arc::clone(&mq), TOPIC));
    let rec0 = outbox_rec("inst-relay", "com.china-workflow.spike3.step", "evt-0", 10);
    again.deliver(&rec0).await.expect("redeliver");
    assert_eq!(
        drain_topic(&mq, TOPIC, 0).unwrap().len(),
        3,
        "重投不产生第 4 条"
    );
}

// ─── outbox 保留清理（守卫式 prune） ───

/// 未投递不删：`Pending` 记录无论多旧都不在清理范围内。
#[tokio::test]
async fn prune_never_deletes_pending_records() {
    let env = setup().await;
    let inner = env.inner();
    let store = Arc::new(KvOutboxStore::new(Arc::clone(&inner)));
    let kv = CoordKv::new(&inner);

    let ancient = now_ms() - 365 * 24 * 3600 * 1000;
    store
        .append(&outbox_rec(
            "prune-pending",
            "com.china-workflow.spike3.step",
            "evt-ancient-pending",
            ancient,
        ))
        .await
        .unwrap();

    let pruned = store.prune(now_ms(), 1_000, 64).await.expect("prune");
    assert_eq!(pruned, 0, "Pending 记录永不删（无论多旧）");
    assert!(
        kv.get(&kv_outbox_id_key("evt-ancient-pending"))
            .await
            .is_some(),
        "pending 的幂等索引仍在"
    );
    assert!(
        !store.pending(now_ms(), 64).await.unwrap().is_empty(),
        "pending 仍可被 relay 轮询到"
    );
}

/// 已投递 + 过保留窗口 ⇒ 删记录键、幂等索引与残留 pending 索引；`Pending` 邻居保留。
#[tokio::test]
async fn prune_deletes_delivered_records_past_retention() {
    let env = setup().await;
    let inner = env.inner();
    let store = Arc::new(KvOutboxStore::new(Arc::clone(&inner)));
    let kv = CoordKv::new(&inner);

    let now = now_ms();
    let created = now - 10 * 60 * 1000;
    for (i, id) in ["evt-prune-1", "evt-prune-2"].iter().enumerate() {
        store
            .append(&outbox_rec(
                "prune-delivered",
                "com.china-workflow.spike3.step",
                id,
                created + i as i64,
            ))
            .await
            .unwrap();
        store.mark_delivered(id, now - 5 * 60 * 1000).await.unwrap();
    }
    store
        .append(&outbox_rec(
            "prune-delivered",
            "com.china-workflow.spike3.step",
            "evt-prune-keep",
            created,
        ))
        .await
        .unwrap();

    let pruned = store.prune(now, 60_000, 64).await.expect("prune");
    assert_eq!(pruned, 2, "两条已投递且过窗的记录被回收");

    let mine: Vec<OutboxRecord> = OutboxStore::list_all(&*store)
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.aggregate_id == "prune-delivered")
        .collect();
    assert_eq!(mine.len(), 1, "只剩未投递的那条: {mine:?}");
    assert_eq!(mine[0].id, "evt-prune-keep");
    assert!(
        kv.get(&kv_outbox_id_key("evt-prune-1")).await.is_none(),
        "幂等索引 ids/ 随记录一起删"
    );
    assert!(
        kv.get(&kv_outbox_record_key("prune-delivered", 1))
            .await
            .is_none(),
        "权威归档 records/ 已删（直读 KV）"
    );
    assert!(
        kv.get(&kv_outbox_seq_key("prune-delivered"))
            .await
            .is_some(),
        "seq/ 计数器不随清理删除"
    );
}

/// `Dead` 以末次尝试时刻计时（无重试时即入库时刻）：刚重试过的死信未过窗不删。
#[tokio::test]
async fn prune_dead_records_age_from_last_attempt() {
    let env = setup().await;
    let inner = env.inner();
    let store = Arc::new(KvOutboxStore::new(Arc::clone(&inner)));

    let now = now_ms();
    let created = now - 24 * 3600 * 1000;
    // 入库很旧且末次尝试也已过窗 ⇒ 删
    store
        .append(&outbox_rec(
            "prune-dead",
            "com.china-workflow.spike3.step",
            "evt-dead-old",
            created,
        ))
        .await
        .unwrap();
    store
        .mark_retry("evt-dead-old", 3, now - 3600 * 1000)
        .await
        .unwrap();
    store.mark_dead("evt-dead-old", 4).await.unwrap();
    // 入库同样很旧，但刚重试过 ⇒ 保留（不因 created_at 老就提前删）
    store
        .append(&outbox_rec(
            "prune-dead",
            "com.china-workflow.spike3.step",
            "evt-dead-fresh",
            created,
        ))
        .await
        .unwrap();
    store
        .mark_retry("evt-dead-fresh", 3, now - 10_000)
        .await
        .unwrap();
    store.mark_dead("evt-dead-fresh", 4).await.unwrap();

    let pruned = store.prune(now, 60_000, 64).await.expect("prune");
    assert_eq!(pruned, 1, "只回收末次尝试已过窗的死信");
    let ids: Vec<String> = OutboxStore::list_all(&*store)
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.aggregate_id == "prune-dead")
        .map(|r| r.id)
        .collect();
    assert_eq!(ids, vec!["evt-dead-fresh".to_string()]);
}

/// 序号不回退：清理后同一聚合的新事件接着已删序号继续 +1。
#[tokio::test]
async fn prune_does_not_reset_sequence_counter() {
    let env = setup().await;
    let inner = env.inner();
    let store = Arc::new(KvOutboxStore::new(Arc::clone(&inner)));
    let kv = CoordKv::new(&inner);

    let now = now_ms();
    let created = now - 3600 * 1000;
    for i in 1..=3u64 {
        store
            .append(&outbox_rec(
                "prune-seq",
                "com.china-workflow.spike3.step",
                &format!("evt-seq-{i}"),
                created + i as i64,
            ))
            .await
            .unwrap();
    }
    let seqs: Vec<u64> = OutboxStore::list_all(&*store)
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.aggregate_id == "prune-seq")
        .map(|r| r.sequence)
        .collect();
    assert_eq!(seqs, vec![1, 2, 3], "初始序号 1..3");

    for i in 1..=3u64 {
        store
            .mark_delivered(&format!("evt-seq-{i}"), now - 1800 * 1000)
            .await
            .unwrap();
    }
    assert_eq!(
        store.prune(now, 60_000, 64).await.unwrap(),
        3,
        "三条已投递记录全部回收"
    );
    assert_eq!(
        kv.get(&kv_outbox_seq_key("prune-seq")).await,
        Some(b"3".to_vec()),
        "seq 计数器在清理后仍是 3"
    );

    store
        .append(&outbox_rec(
            "prune-seq",
            "com.china-workflow.spike3.step",
            "evt-seq-4",
            now,
        ))
        .await
        .unwrap();
    let next = OutboxStore::list_all(&*store)
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.id == "evt-seq-4")
        .expect("新事件已入库");
    assert_eq!(next.sequence, 4, "清理后新事件接着 4，序号不回退/不重叠");
}

/// 幂等 + 单批上限：重复 prune 返回 0，每轮最多删 `limit` 条。
#[tokio::test]
async fn repeated_prune_is_idempotent_and_bounded_by_limit() {
    let env = setup().await;
    let inner = env.inner();
    let store = Arc::new(KvOutboxStore::new(Arc::clone(&inner)));

    let now = now_ms();
    let created = now - 10 * 60 * 1000;
    for i in 1..=3u64 {
        let id = format!("evt-idem-{i}");
        store
            .append(&outbox_rec(
                "prune-idem",
                "com.china-workflow.spike3.step",
                &id,
                created + i as i64,
            ))
            .await
            .unwrap();
        store
            .mark_delivered(&id, now - 5 * 60 * 1000)
            .await
            .unwrap();
    }

    assert_eq!(store.prune(now, 60_000, 1).await.unwrap(), 1, "单批上限");
    assert_eq!(store.prune(now, 60_000, 1).await.unwrap(), 1);
    assert_eq!(store.prune(now, 60_000, 1).await.unwrap(), 1);
    let after = store.prune(now, 60_000, 1).await.unwrap();
    assert_eq!(after, 0, "同日重复 tick 幂等：已无过窗记录");
    assert!(
        OutboxStore::list_all(&*store)
            .await
            .unwrap()
            .into_iter()
            .all(|r| r.aggregate_id != "prune-idem"),
        "三次清理后聚合记录已空"
    );
}

/// relay tick 出口：投递与回收同轮完成，`RelayReport.pruned` 是清理的可观测出口。
#[tokio::test]
async fn relay_tick_reports_pruned_after_delivery() {
    let env = setup().await;
    let inner = env.inner();
    let mq = start_mq(&env.mq_dir()).await;
    let store = Arc::new(KvOutboxStore::new(Arc::clone(&inner)));

    let now = now_ms();
    // 早先 tick 已投递、现已过窗的记录：本轮应被回收
    store
        .append(&outbox_rec(
            "prune-relay",
            "com.china-workflow.spike3.step",
            "evt-relay-old",
            now - 600_000,
        ))
        .await
        .unwrap();
    store
        .mark_delivered("evt-relay-old", now - 300_000)
        .await
        .unwrap();
    // 本轮新投递的记录：按本 tick 时间计时，同一轮内不回收
    store
        .append(&outbox_rec(
            "prune-relay",
            "com.china-workflow.spike3.step",
            "evt-relay-new",
            now,
        ))
        .await
        .unwrap();

    let sink: Arc<dyn OutboxSink> = Arc::new(MqOutboxSink::new(Arc::clone(&mq), TOPIC));
    let relay = OutboxRelay::new(
        Arc::clone(&store) as Arc<dyn OutboxStore>,
        sink,
        RelayPolicy {
            retention_ms: 60_000,
            ..RelayPolicy::default()
        },
    );

    let first = relay.tick(now).await.unwrap();
    assert_eq!(first.delivered, 1, "本轮投递新记录");
    assert_eq!(first.pruned, 1, "同轮回收过窗的旧已投递记录");
    let ids: Vec<String> = OutboxStore::list_all(&*store)
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.aggregate_id == "prune-relay")
        .map(|r| r.id)
        .collect();
    assert_eq!(ids, vec!["evt-relay-new".to_string()], "旧记录已回收");

    let within = relay.tick(now + 30_000).await.unwrap();
    assert_eq!(within.delivered, 0);
    assert_eq!(within.pruned, 0, "还在保留窗内：不回收");

    let after = relay.tick(now + 61_000).await.unwrap();
    assert_eq!(after.pruned, 1, "过窗后由后续 tick 回收");
}

/// 判据 3（问题 4）：持久化定时器可按到期时间扫描与移除。
#[tokio::test]
async fn durable_timers_scan_by_due_time() {
    let env = setup().await;
    let inner = env.inner();
    let store = Arc::new(KvOutboxStore::new(Arc::clone(&inner)));

    store
        .schedule(&TimerRecord::auto_resume("inst-t", "wait", 5_000, 0))
        .await
        .unwrap();
    store
        .schedule(&TimerRecord::timeout("inst-t2", Some(0), 1_000, 0))
        .await
        .unwrap();

    let all = DurableTimerStore::list_all(&*store).await.unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].fire_at_ms, 1_000, "list_all 按到期时间升序");

    let due = store.due(2_000, 16).await.unwrap();
    assert_eq!(due.len(), 1, "只取已到期项");
    assert_eq!(due[0].instance_id, "inst-t2");
    assert!(matches!(
        due[0].kind,
        TimerKind::Timeout {
            expected_index: Some(0)
        }
    ));

    let due = store.due(9_999, 16).await.unwrap();
    assert_eq!(due.len(), 2, "时间推进后第二条也到期");

    store.remove(&all[0].id).await.unwrap();
    assert_eq!(DurableTimerStore::list_all(&*store).await.unwrap().len(), 1);
}

// ─── 引擎重建（模拟崩溃后重启）恢复挂起实例 ───

async fn deploy(
    yaml: &str,
    inner: &Arc<AgentInner>,
    mq: Arc<MessageQueueService>,
) -> (WorkflowEngineService, String) {
    let svc = WorkflowEngineService::new_with_kv_store_durable(
        Arc::clone(inner),
        &WorkflowDispatchConfig::default(),
        DurableEngineOptions {
            mq,
            topic: TOPIC.to_string(),
            relay_enabled: true,
            relay_interval_ms: 100,
            approval_roles: std::collections::HashMap::new(),
            on_behalf_services: Vec::new(),
        },
    );
    svc.start().await.expect("engine start");
    let def_id = svc.deploy_definition("spike3", yaml).await.expect("deploy");
    (svc, def_id)
}

const WAIT_YAML: &str = r#"
document:
  dsl: "1.0.3"
  namespace: spike3
  name: wait-recover
  version: "1.0.0"
id: wait-recover
version: "1.0.0"
start: pause
states:
  - name: pause
    type: delay
    duration: PT1H
    transition: done
  - name: done
    type: inject
    data:
      completed: true
    end: true
"#;

const LISTEN_YAML: &str = r#"
document:
  dsl: "1.0.3"
  namespace: spike3
  name: listen-recover
  version: "1.0.0"
id: listen-recover
version: "1.0.0"
start: await
states:
  - name: await
    type: event
    onEvents:
      - eventRefs:
          - eventRef: goAhead
    transition: done
  - name: done
    type: inject
    data:
      completed: true
    end: true
events:
  - name: goAhead
    type: com.china-workflow.spike3.go-ahead
    source: /spike3
"#;

async fn wait_for<F, Fut>(what: &str, timeout: Duration, mut cond: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if cond().await {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            panic!("条件在 {timeout:?} 内未成立: {what}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// 构造一个「进程已死、时间已过、状态留在 KV」的挂起实例：
/// 用真实引擎产出 wait 实例，然后把它写入 `Waiting` + **已到期**的
/// `until_ms`（模拟崩溃期间时间流逝）。持久化定时器由恢复路径补写。
#[tokio::test]
async fn wait_instance_recovers_after_engine_restart() {
    let env = setup().await;
    let inner = env.inner();
    let mq = start_mq(&env.mq_dir()).await;
    let (svc, def_id) = deploy(WAIT_YAML, &inner, Arc::clone(&mq)).await;

    // 用真实引擎产出实例，并把它的挂起原因改成「wait 已到期」
    let inst = svc
        .start_instance(&def_id, serde_json::json!({}))
        .await
        .expect("start instance");
    let kv = CoordKv::new(&inner);
    wait_for(
        "instance suspends on wait",
        Duration::from_secs(10),
        || async {
            matches!(
                kv.get_instance(&inst.id).await.map(|i| i.status),
                Some(InstanceStatus::Waiting)
            )
        },
    )
    .await;
    // 停掉原引擎的后台（模拟进程退出：原 runtime 不再驱动）
    drop(svc);

    // 覆盖为「挂起 + 到期」的上游状态：真实崩溃场景里，进程死后时间照样在走
    let mut current = kv.get_instance(&inst.id).await.expect("instance exists");
    current.status = InstanceStatus::Waiting;
    current.current_task_index = 0;
    current.task_stack = vec![TaskFrame {
        task_name: "pause".to_string(),
        task_type: "wait".to_string(),
        status: TaskStatus::Pending,
        input: None,
        output: None,
        started_at: Some(current.created_at),
        ended_at: None,
        retry_count: 0,
        pending_branches: None,
    }];
    current.suspension_meta = Some(SuspensionMeta {
        reason: "wait".to_string(),
        until_ms: Some(now_ms() - 1_000),
        service: None,
        payload: None,
        event_filter: None,
        expected_signal: None,
        approval_instance_id: None,
        retry_count: None,
        error: None,
    });
    kv.put_instance(&current).await;

    // 「重启」：全新引擎在**同一 data-dir（同一 KV）**上启动
    let svc2 = WorkflowEngineService::new_with_kv_store_durable(
        Arc::clone(&inner),
        &WorkflowDispatchConfig::default(),
        DurableEngineOptions {
            mq: Arc::clone(&mq),
            topic: TOPIC.to_string(),
            relay_enabled: true,
            relay_interval_ms: 100,
            approval_roles: std::collections::HashMap::new(),
            on_behalf_services: Vec::new(),
        },
    );
    svc2.start().await.expect("restart engine");

    wait_for(
        "instance completes after recovery",
        Duration::from_secs(20),
        || async {
            matches!(
                kv.get_instance(&inst.id).await.map(|i| i.status),
                Some(InstanceStatus::Completed)
            )
        },
    )
    .await;
    let done = kv.get_instance(&inst.id).await.expect("instance");
    assert!(done.suspension_meta.is_none(), "恢复后不再挂起");

    // 已到期的定时器触发后不再重放。
    //
    // 注意：崩溃前那次 run 还留下一个**远期**定时器（PT1H 的到期项），它不在恢复
    // 窗口内；到期时 `fire_timer` 看到实例已非 Waiting(reason="wait") 会当陈旧记录
    // 移除——这是设计内的幂等清理，不是残骸。
    let timers = Arc::new(KvOutboxStore::new(Arc::clone(&inner)));
    let leftover: Vec<_> = DurableTimerStore::list_all(&*timers)
        .await
        .unwrap()
        .into_iter()
        .filter(|t| t.instance_id == inst.id)
        .collect();
    assert!(
        leftover.iter().all(|t| t.fire_at_ms > now_ms()),
        "不得留下已到期的定时器（会重复触发）: {leftover:?}"
    );
    assert!(
        timers
            .due(now_ms() + 1_000, 64)
            .await
            .unwrap()
            .iter()
            .all(|t| t.instance_id != inst.id),
        "已触发的定时器必须在触发时被移除"
    );
}

/// Topic 生命周期回归：真实 `serve_with_shutdown` 中 durable 引擎**先构造**、
/// MQ 插件**后 `start()`**——构造时的 `create_topic` 因 MQ 未起而失败，topic
/// 必须由 `start()` 在 MQ 就绪后补建，否则 relay 产出/ listen 订阅都报
/// topic not found。此处刻意反向于 harness（先造引擎、后起 MQ）复现该顺序。
#[tokio::test]
async fn durable_service_creates_events_topic_after_late_mq_start() {
    let env = setup().await;
    let inner = env.inner();

    // MQ 构造但**不** start。
    let mq = Arc::new(MessageQueueService::new(env.mq_dir(), 1024 * 1024 * 1024));
    let svc = WorkflowEngineService::new_with_kv_store_durable(
        Arc::clone(&inner),
        &WorkflowDispatchConfig::default(),
        DurableEngineOptions {
            mq: Arc::clone(&mq),
            topic: TOPIC.to_string(),
            relay_enabled: true,
            relay_interval_ms: 100,
            approval_roles: std::collections::HashMap::new(),
            on_behalf_services: Vec::new(),
        },
    );
    // 构造时 MQ 未起（`get_topic_config` 读表需 MQ 已 start，故此断言只能验证
    // 未启动这一前置本身）：构造时的 `create_topic` 因而失败。
    assert!(
        !mq.is_started(),
        "MQ must not be started at engine construction"
    );

    // MQ 就绪后再启引擎：`start()` 必须补建 topic。
    mq.start().await.expect("start mq");
    svc.start().await.expect("engine start");
    assert!(
        mq.get_topic_config(TOPIC).unwrap().is_some(),
        "start() must create the durable events topic once MQ is up"
    );
}

/// listen 挂起实例在引擎重启后重新订阅，事件到达即恢复。
#[tokio::test]
async fn listen_instance_resubscribes_after_engine_restart() {
    let env = setup().await;
    let inner = env.inner();
    let mq = start_mq(&env.mq_dir()).await;
    let (svc, def_id) = deploy(LISTEN_YAML, &inner, Arc::clone(&mq)).await;

    let inst = svc
        .start_instance(&def_id, serde_json::json!({}))
        .await
        .expect("start instance");
    let kv = CoordKv::new(&inner);
    wait_for(
        "instance suspends on listen",
        Duration::from_secs(10),
        || async {
            matches!(
                kv.get_instance(&inst.id).await.map(|i| i.status),
                Some(InstanceStatus::Waiting)
            )
        },
    )
    .await;
    drop(svc);

    // 「重启」：全新引擎重订阅
    let svc2 = WorkflowEngineService::new_with_kv_store_durable(
        Arc::clone(&inner),
        &WorkflowDispatchConfig::default(),
        DurableEngineOptions {
            mq: Arc::clone(&mq),
            topic: TOPIC.to_string(),
            relay_enabled: true,
            relay_interval_ms: 100,
            approval_roles: std::collections::HashMap::new(),
            on_behalf_services: Vec::new(),
        },
    );
    svc2.start().await.expect("restart engine");

    // 外部事件到达（重启之后才发生）
    let emitter = MqEventProvider::new_with_cursor_store(
        Arc::clone(&mq),
        TOPIC,
        Arc::new(KvCursorStore::new(Arc::clone(&inner))) as Arc<dyn CursorStore>,
    );
    emitter
        .emit(
            "com.china-workflow.spike3.go-ahead",
            Some("/spike3"),
            &serde_json::json!({"ok": true}),
        )
        .await;

    wait_for(
        "listen instance completes after event",
        Duration::from_secs(20),
        || async {
            matches!(
                kv.get_instance(&inst.id).await.map(|i| i.status),
                Some(InstanceStatus::Completed)
            )
        },
    )
    .await;
}

/// Outbox 记录在崩溃前落 KV、崩溃后由新引擎的 relay 投递 —— 事件不丢。
///
/// 第一阶段 `relay_enabled = false`：只写不投（模拟「进程在投递前被 kill」）。
#[tokio::test]
async fn outbox_events_pending_at_crash_are_delivered_after_restart() {
    let env = setup().await;
    let inner = env.inner();
    let mq = start_mq(&env.mq_dir()).await;

    // 阶段 1：引擎只写 outbox（relay 关闭）
    let svc = WorkflowEngineService::new_with_kv_store_durable(
        Arc::clone(&inner),
        &WorkflowDispatchConfig::default(),
        DurableEngineOptions {
            mq: Arc::clone(&mq),
            topic: TOPIC.to_string(),
            relay_enabled: false,
            relay_interval_ms: 100,
            approval_roles: std::collections::HashMap::new(),
            on_behalf_services: Vec::new(),
        },
    );
    svc.start().await.expect("start");
    let def_id = svc
        .deploy_definition("spike3", LISTEN_YAML)
        .await
        .expect("deploy");
    let inst = svc
        .start_instance(&def_id, serde_json::json!({}))
        .await
        .expect("start instance");
    let outbox = Arc::new(KvOutboxStore::new(Arc::clone(&inner)));
    let kv_for_poll = CoordKv::new(&inner);
    // 等实例真的挂起（此后不再产生新的生命周期事件，避免比较窗口漂移）
    wait_for(
        "instance suspends on listen",
        Duration::from_secs(15),
        || async {
            matches!(
                kv_for_poll.get_instance(&inst.id).await.map(|i| i.status),
                Some(InstanceStatus::Waiting)
            )
        },
    )
    .await;
    let ids_before: std::collections::BTreeSet<String> = OutboxStore::list_all(&*outbox)
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.aggregate_id == inst.id)
        .map(|r| r.id)
        .collect();
    assert!(
        ids_before.len() >= 2,
        "崩溃前至少有 STARTED + WAITING 两条生命周期事件，实际 {}",
        ids_before.len()
    );
    assert!(
        !outbox
            .pending(now_ms() + 1_000_000, 64)
            .await
            .unwrap()
            .is_empty(),
        "relay 关闭 ⇒ 事件停在 outbox"
    );
    assert!(
        drain_topic(&mq, TOPIC, 0).unwrap().is_empty(),
        "relay 关闭时事件未进 MQ"
    );
    drop(svc); // 「崩溃」：状态与 outbox 留在 KV

    // 阶段 2：新引擎拉起 relay ⇒ 补投全部 pending
    let svc2 = WorkflowEngineService::new_with_kv_store_durable(
        Arc::clone(&inner),
        &WorkflowDispatchConfig::default(),
        DurableEngineOptions {
            mq: Arc::clone(&mq),
            topic: TOPIC.to_string(),
            relay_enabled: true,
            relay_interval_ms: 50,
            approval_roles: std::collections::HashMap::new(),
            on_behalf_services: Vec::new(),
        },
    );
    svc2.start().await.expect("restart");
    wait_for("all pending delivered", Duration::from_secs(20), || async {
        outbox
            .pending(now_ms() + 1_000_000, 64)
            .await
            .unwrap()
            .is_empty()
    })
    .await;

    let delivered = drain_topic(&mq, TOPIC, 0).unwrap();
    let mut ids = std::collections::BTreeSet::new();
    for rec in &delivered {
        let v: serde_json::Value = serde_json::from_slice(&rec.payload).unwrap();
        assert_eq!(v["aggregate_id"].as_str(), Some(inst.id.as_str()));
        assert!(
            ids.insert(v["id"].as_str().unwrap().to_string()),
            "同一事件被投递两次（event_id 重复）"
        );
    }
    assert_eq!(
        ids, ids_before,
        "崩溃前 pending 的事件全部投递且不重（消费者集 = outbox 集）"
    );
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn kv_instance_key(id: &str) -> Vec<u8> {
    format!("/_workflow/v3/instances/{id}").into_bytes()
}

fn kv_outbox_id_key(id: &str) -> Vec<u8> {
    format!("/_workflow/v3/outbox/ids/{id}").into_bytes()
}

fn kv_outbox_record_key(aggregate: &str, sequence: u64) -> Vec<u8> {
    format!("/_workflow/v3/outbox/records/{aggregate}/{sequence:020}").into_bytes()
}

fn kv_outbox_seq_key(aggregate: &str) -> Vec<u8> {
    format!("/_workflow/v3/outbox/seq/{aggregate}").into_bytes()
}

/// 直读 KV（证据断言用）。
struct CoordKv {
    client: Client,
}

impl CoordKv {
    fn new(inner: &Arc<AgentInner>) -> Self {
        Self {
            client: inner.client.clone(),
        }
    }

    async fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.client
            .kv()
            .range(key, &[], 0, 0)
            .await
            .ok()
            .and_then(|pairs| pairs.into_iter().next().map(|(_, v)| v))
    }

    /// 直读实例（绕过任何本地缓存，快照必来自 KV）。
    async fn get_instance(&self, id: &str) -> Option<WorkflowInstance> {
        let raw = self.get(&kv_instance_key(id)).await?;
        serde_json::from_slice(&raw).ok()
    }

    /// 直写实例（模拟「崩溃前留在 KV 的状态」）。
    async fn put_instance(&self, inst: &WorkflowInstance) {
        let raw = serde_json::to_vec(inst).expect("instance encodes");
        self.client
            .kv()
            .put(&kv_instance_key(&inst.id), &raw)
            .await
            .expect("put instance");
    }
}
