//! v2 审批存储在**真实 Raft KV + gRPC** 上的集成验收（M2-B 首条端到端执行覆盖）。
//!
//! 覆盖 `KvApprovalStore` 的唯一写路径 `commit`：CreateOnly 恰好一次、Exact CAS
//! 冲突零写入、状态与事件同 Txn、重载后语义（含子实体 id 不重复）、丢失响应后的
//! 幂等重放、空事件批次只写状态、以及 v4 键空间与 v1 扫描面双向隔离。
//!
//! 每个用例独立起单节点服务（`common::start_test_server`）与独立 TempDir；测试以
//! `--test-threads=1` 运行。内存实现（`MemoryApprovalStore`）不是本套件对象：它与 KV
//! 实现的 per-aggregate 序号来源不同（max vs 持久化计数器），outbox 被 prune 后合法
//! 发散，故不做跨实现序号相等断言。

mod common;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use coord_agent::cache::AgentCache;
use coord_agent::services::approval_store::KvApprovalStore;
use coord_agent::services::outbox_kv::KvOutboxStore;
use coord_agent::services::workflow_store::KvWorkflowStore;
use coord_agent::AgentInner;
use coord_client::config::Config;
use coord_client::Client;
use coord_core::workflow::approval::engine::{Engine, StartSpec};
use coord_core::workflow::approval::events::event_record;
use coord_core::workflow::approval::keys;
use coord_core::workflow::approval::model::{JoinPolicy, RejectPolicy, Scope, SeatSpec};
use coord_core::workflow::approval::store::{ApprovalStore, ExpectedRev};
use coord_core::workflow::durable::{OutboxRecord, OutboxStatus, OutboxStore};
use coord_core::workflow::model::WorkflowInstance;
use coord_core::workflow::ports::WorkflowStore;
use serde_json::json;

/// 进程内单节点服务 + 客户端；**必须持有到测试结束**（drop 会关闭 gRPC 服务）。
struct TestEnv {
    inner: Arc<AgentInner>,
    _tmp: tempfile::TempDir,
    _shutdown: tokio::sync::oneshot::Sender<()>,
    _grpc: tokio::task::JoinHandle<()>,
    _raft: tokio::task::JoinHandle<()>,
}

impl TestEnv {
    fn inner(&self) -> Arc<AgentInner> {
        Arc::clone(&self.inner)
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
        _tmp: tmp,
        _shutdown: shutdown,
        _grpc: grpc,
        _raft: raft,
    }
}

fn start_engine(id: &str, initiator: &str, definition_hash: &str) -> Engine {
    Engine::start(
        StartSpec {
            id: id.into(),
            definition_ns: "eis".into(),
            definition_name: "leave-approval".into(),
            definition_version: "1.0.0".into(),
            definition_hash: definition_hash.into(),
            initiator: initiator.into(),
            context: Scope::new(),
        },
        1_000,
    )
    .expect("start instance")
}

/// 一条 v2 审批生命周期事件（类型取自 `events::APPROVAL_EVENT_TYPES`）。
fn event(instance_id: &str, event_id: &str, now: i64) -> OutboxRecord {
    event_record(
        instance_id,
        "com.china-workflow.instance.completed",
        &json!({ "instance_id": instance_id, "event_id": event_id }),
        now,
        event_id.into(),
    )
    .expect("known event type")
}

/// 某实例在 outbox 库中的记录，按分配序号升序。
async fn records_for(outbox: &KvOutboxStore, instance_id: &str) -> Vec<OutboxRecord> {
    let mut recs: Vec<OutboxRecord> = OutboxStore::list_all(outbox)
        .await
        .expect("list outbox")
        .into_iter()
        .filter(|r| r.aggregate_id == instance_id)
        .collect();
    recs.sort_by_key(|r| r.sequence);
    recs
}

/// 一个 v1 实例（`KvWorkflowStore` 写 `/_workflow/v3/instances/`）。
fn v1_instance(id: &str) -> WorkflowInstance {
    serde_json::from_value(json!({
        "id": id,
        "definition_ns": "t6",
        "definition_name": "legacy",
        "definition_version": "1.0.0",
        "status": "running",
        "context": {},
        "current_task_index": 0,
        "created_at": 1,
        "updated_at": 1
    }))
    .expect("v1 instance json")
}

// ───────────────────────────────────────────────────────────── 判据 1：恰好一次

/// `CreateOnly` 在真实 Raft Txn 的 `VERSION==0` CAS 下保证**恰好一个**写者赢。
///
/// 两个内容不同的 `Engine`（同 id）并发 `commit`：赢家的状态落库、其事件全部在库；
/// 输家 `Ok(false)`、其事件一条都不在。10 轮不同 id 重复（抓抖动）。
#[tokio::test]
async fn create_only_exactly_one_winner_under_concurrency() {
    let env = setup().await;
    let inner = env.inner();
    let outbox = Arc::new(KvOutboxStore::new(Arc::clone(&inner)));
    let store = KvApprovalStore::new(Arc::clone(&inner), Arc::clone(&outbox));

    for round in 0..10u32 {
        let id = format!("inst-t6-race-{round:02}");
        let a = start_engine(&id, "alice", "sha256:t6-race-a");
        let b = start_engine(&id, "bob", "sha256:t6-race-b");
        // 同 id、内容不同 ⇒ 两者都不是对方的逐字重放，CAS 必须分出唯一赢家。
        assert_ne!(a, b);

        let ev_a = vec![
            event(&id, &format!("evt-t6-race-{round:02}-a1"), 1_001),
            event(&id, &format!("evt-t6-race-{round:02}-a2"), 1_002),
        ];
        let ev_b = vec![
            event(&id, &format!("evt-t6-race-{round:02}-b1"), 1_001),
            event(&id, &format!("evt-t6-race-{round:02}-b2"), 1_002),
        ];

        // 两个 commit 都在任一完成前启动（同为单任务上的并发 future）。
        let (res_a, res_b) = tokio::join!(
            store.commit(&a, &ev_a, ExpectedRev::CreateOnly),
            store.commit(&b, &ev_b, ExpectedRev::CreateOnly),
        );
        let won_a = res_a.expect("commit a");
        let won_b = res_b.expect("commit b");
        assert!(
            won_a ^ won_b,
            "round {round}: exactly one CreateOnly must win (a={won_a}, b={won_b})"
        );

        let (winner, winner_events, loser_events) = if won_a {
            (&a, &ev_a, &ev_b)
        } else {
            (&b, &ev_b, &ev_a)
        };

        let (loaded, rev) = store.load(&id).await.unwrap().expect("winner persisted");
        assert_eq!(
            &loaded, winner,
            "round {round}: stored state must equal the winner"
        );
        assert!(rev >= 1, "round {round}: committed instance has a revision");

        let recs = records_for(&outbox, &id).await;
        let ids: HashSet<&str> = recs.iter().map(|r| r.id.as_str()).collect();
        for ev in winner_events {
            assert!(
                ids.contains(ev.id.as_str()),
                "round {round}: winner event {} must be persisted",
                ev.id
            );
        }
        for ev in loser_events {
            assert!(
                !ids.contains(ev.id.as_str()),
                "round {round}: loser event {} must be absent",
                ev.id
            );
        }
        assert_eq!(
            recs.len(),
            winner_events.len(),
            "round {round}: only the winner batch is persisted"
        );
    }
}

// ───────────────────────────────────────────────────── 判据 2：CAS 冲突零写入

/// 过期 `Exact(rev)` 提交不同状态 + 新事件 ⇒ `Ok(false)`，状态、修订号、事件三者逐字不变。
#[tokio::test]
async fn exact_rev_conflict_is_zero_write() {
    let env = setup().await;
    let inner = env.inner();
    let outbox = Arc::new(KvOutboxStore::new(Arc::clone(&inner)));
    let store = KvApprovalStore::new(Arc::clone(&inner), Arc::clone(&outbox));
    let id = "inst-t6-cas";

    let created = start_engine(id, "alice", "sha256:t6-cas");
    let ev0 = vec![event(id, "evt-t6-cas-0", 1_001)];
    assert!(store
        .commit(&created, &ev0, ExpectedRev::CreateOnly)
        .await
        .unwrap());
    let (_, rev0) = store.load(id).await.unwrap().expect("created");

    let mut second = created.clone();
    let _ = second
        .enter_branch(
            second.instance.revision,
            "apply",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            1_002,
        )
        .expect("enter branch");
    let ev1 = vec![event(id, "evt-t6-cas-1", 1_002)];
    assert!(store
        .commit(&second, &ev1, ExpectedRev::Exact(rev0))
        .await
        .unwrap());
    let (state1, rev1) = store.load(id).await.unwrap().expect("advanced");
    assert_eq!(state1, second);
    assert!(rev1 > rev0, "successful Exact CAS advances the revision");

    // 用已过期的 rev0 提交另一份状态：谓词不成立 ⇒ 整笔零写入。
    let mut stale = second.clone();
    stale.instance.context.insert("spoof".into(), json!(true));
    let ev2 = vec![event(id, "evt-t6-cas-2", 1_003)];
    assert!(
        !store
            .commit(&stale, &ev2, ExpectedRev::Exact(rev0))
            .await
            .unwrap(),
        "stale Exact(rev) must be rejected"
    );

    let (state_after, rev_after) = store.load(id).await.unwrap().expect("still present");
    assert_eq!(state_after, second, "stale CAS must not overwrite state");
    assert_eq!(rev_after, rev1, "stale CAS must not advance the revision");
    let recs = records_for(&outbox, id).await;
    assert_eq!(recs.len(), 2, "stale CAS must not add outbox records");
    assert!(
        recs.iter().all(|r| r.id != "evt-t6-cas-2"),
        "stale CAS event must be absent"
    );
}

// ────────────────────────────────────────────────── 判据 3：状态与事件同 Txn

/// 一次提交携带的一批事件要么全部生效、要么一条不生效。
///
/// 正向：2 条事件同批落库，序号连续、`Pending`、聚合根 = 实例 id。
/// 反向：对已存在 id 的 `CreateOnly` 提交（不同状态 + 2 条新事件）失败 ⇒ 新事件都不在、
/// 状态与修订号不变。
#[tokio::test]
async fn commit_persists_state_and_events_in_one_txn() {
    let env = setup().await;
    let inner = env.inner();
    let outbox = Arc::new(KvOutboxStore::new(Arc::clone(&inner)));
    let store = KvApprovalStore::new(Arc::clone(&inner), Arc::clone(&outbox));
    let id = "inst-t6-atomic";

    let mut eng = start_engine(id, "alice", "sha256:t6-atomic");
    let _ = eng
        .enter_branch(
            eng.instance.revision,
            "apply",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            1_001,
        )
        .expect("enter branch");

    let batch = vec![
        event(id, "evt-t6-atomic-1", 1_001),
        event(id, "evt-t6-atomic-2", 1_002),
    ];
    assert!(store
        .commit(&eng, &batch, ExpectedRev::CreateOnly)
        .await
        .unwrap());

    let (stored, rev) = store.load(id).await.unwrap().expect("committed");
    assert_eq!(stored, eng);

    let recs = records_for(&outbox, id).await;
    assert_eq!(recs.len(), 2, "both events land in the same commit");
    assert_eq!(recs[0].id, "evt-t6-atomic-1");
    assert_eq!(recs[1].id, "evt-t6-atomic-2");
    assert_eq!(
        recs[0].sequence + 1,
        recs[1].sequence,
        "batch sequences are consecutive"
    );
    for r in &recs {
        assert_eq!(r.status, OutboxStatus::Pending);
        assert_eq!(r.aggregate_id, id);
    }

    // 反向：同 id 的 CreateOnly 已失效（键存在）⇒ 整笔失败且零写入。
    let mut other = eng.clone();
    other.instance.context.insert("spoof".into(), json!(true));
    let new_batch = vec![
        event(id, "evt-t6-atomic-3", 1_003),
        event(id, "evt-t6-atomic-4", 1_004),
    ];
    assert!(
        !store
            .commit(&other, &new_batch, ExpectedRev::CreateOnly)
            .await
            .unwrap(),
        "CreateOnly on an existing id must fail"
    );
    let recs_after = records_for(&outbox, id).await;
    assert_eq!(recs_after.len(), 2, "failed commit must not add events");
    assert!(recs_after.iter().all(|r| r.id != "evt-t6-atomic-3"));
    assert!(recs_after.iter().all(|r| r.id != "evt-t6-atomic-4"));
    let (state_after, rev_after) = store.load(id).await.unwrap().expect("present");
    assert_eq!(state_after, eng);
    assert_eq!(rev_after, rev);
}

// ────────────────────────────────────────────────── 判据 4：重载后语义不丢

/// 提交后由**新的** `KvApprovalStore` 读回（模拟进程重启）：快照逐字相等、修订号不倒退；
/// 继续 `enter_branch` 产生的子实体 id 不与任何已存 id 重复（`seq` 随聚合持久化）。
#[tokio::test]
async fn load_after_reload_preserves_semantics() {
    let env = setup().await;
    let inner = env.inner();
    let outbox = Arc::new(KvOutboxStore::new(Arc::clone(&inner)));
    let store = KvApprovalStore::new(Arc::clone(&inner), Arc::clone(&outbox));
    let id = "inst-t6-reload";

    let mut eng = start_engine(id, "alice", "sha256:t6-reload");
    let (branch, _) = eng
        .enter_branch(
            eng.instance.revision,
            "apply",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            1_001,
        )
        .expect("enter branch");
    let (_, tasks, _) = eng
        .open_approval(
            eng.instance.revision,
            &branch,
            vec![SeatSpec::approver("alice")],
            None,
            1_002,
        )
        .expect("open approval");
    let _ = eng
        .approve(eng.instance.revision, &tasks[0], "alice", None, 1_003)
        .expect("approve");

    let before = eng.snapshot_json().unwrap();
    let stored_ids: HashSet<String> = eng
        .branches
        .keys()
        .chain(eng.tasks.keys())
        .chain(eng.rounds.keys())
        .cloned()
        .collect();
    assert!(
        !stored_ids.is_empty(),
        "aggregate has sub-entities to collide with"
    );

    assert!(store
        .commit(&eng, &[], ExpectedRev::CreateOnly)
        .await
        .unwrap());
    let (_, rev_before) = store.load(id).await.unwrap().expect("persisted");

    // 新的存储实例 + 重新 load：模拟重启后从 KV 复原。
    let reloaded_store = KvApprovalStore::new(
        Arc::clone(&inner),
        Arc::new(KvOutboxStore::new(Arc::clone(&inner))),
    );
    let (mut loaded, rev_after) = reloaded_store.load(id).await.unwrap().expect("reloaded");
    assert_eq!(
        loaded.snapshot_json().unwrap(),
        before,
        "reload must preserve the aggregate verbatim"
    );
    assert!(
        rev_after >= rev_before && rev_before >= 1,
        "revision must not regress across reload"
    );

    // 继续执行：新生成的 branch id 必须落在已存 id之外。
    let (new_branch, _) = loaded
        .enter_branch(
            loaded.instance.revision,
            "apply-next",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            1_004,
        )
        .expect("enter branch after reload");
    assert!(
        !stored_ids.contains(&new_branch),
        "generated id {new_branch} must not collide with a stored sub-entity"
    );
    assert!(new_branch.starts_with(&format!("{id}:")));

    // 继续执行的结果可再次提交并被读回（重载后语义可继续推进）。
    assert!(reloaded_store
        .commit(&loaded, &[], ExpectedRev::Exact(rev_after))
        .await
        .unwrap());
    let (again, rev_final) = reloaded_store.load(id).await.unwrap().expect("reloaded");
    assert_eq!(
        again.snapshot_json().unwrap(),
        loaded.snapshot_json().unwrap()
    );
    assert_eq!(rev_final, rev_after + 1);
}

// ─────────────────────────────────────────────── 判据 5：v1 列表看不见 v2

/// v1 的实例扫描面（`/_workflow/v3/instances/`）不吞 v2 实例；v4 前缀扫描只返回 v4 键。
#[tokio::test]
async fn v1_list_does_not_see_v2_instances() {
    let env = setup().await;
    let inner = env.inner();
    let outbox = Arc::new(KvOutboxStore::new(Arc::clone(&inner)));
    let store = KvApprovalStore::new(Arc::clone(&inner), Arc::clone(&outbox));
    let id_v2 = "inst-t6-v2-isolation";
    let id_v1 = "inst-t6-v1-alongside";

    assert!(store
        .commit(
            &start_engine(id_v2, "alice", "sha256:t6"),
            &[],
            ExpectedRev::CreateOnly
        )
        .await
        .unwrap());
    let wf = KvWorkflowStore::new(Arc::clone(&inner));
    wf.save_instance(&v1_instance(id_v1)).await.unwrap();

    let (insts, _token) = wf
        .list_instances(None, None, usize::MAX, None)
        .await
        .unwrap();
    assert!(
        insts.iter().any(|i| i.id == id_v1),
        "v1 list must still surface v1 instances"
    );
    assert!(
        insts.iter().all(|i| i.id != id_v2),
        "v1 list must never return a v2 instance"
    );

    let prefix = keys::instance_prefix();
    let end = keys::prefix_range_end(prefix);
    let pairs = inner.client.kv().range(prefix, &end, 0, 0).await.unwrap();
    assert!(
        pairs.iter().any(|(k, _)| k == &keys::instance_key(id_v2)),
        "v4 scan must contain the v2 instance key"
    );
    assert!(
        pairs.iter().all(|(k, _)| k.starts_with(prefix)),
        "v4 scan must return only v4 keys"
    );
}

// ─────────────────────────────────────────────── 判据 6：v4 扫描看不见 v1

/// v1 实例写 `/_workflow/v3/instances/`；v4 的 `instance_prefix()` 扫描不包含它。
#[tokio::test]
async fn v4_scan_does_not_see_v1_instances() {
    let env = setup().await;
    let inner = env.inner();
    let outbox = Arc::new(KvOutboxStore::new(Arc::clone(&inner)));
    let store = KvApprovalStore::new(Arc::clone(&inner), Arc::clone(&outbox));
    let id_v1 = "inst-t6-v1-scan";
    let id_v2 = "inst-t6-v2-scan";

    let wf = KvWorkflowStore::new(Arc::clone(&inner));
    wf.save_instance(&v1_instance(id_v1)).await.unwrap();
    assert!(store
        .commit(
            &start_engine(id_v2, "alice", "sha256:t6"),
            &[],
            ExpectedRev::CreateOnly
        )
        .await
        .unwrap());

    let prefix = keys::instance_prefix();
    let end = keys::prefix_range_end(prefix);
    let pairs = inner.client.kv().range(prefix, &end, 0, 0).await.unwrap();
    let v1_key = format!("/_workflow/v3/instances/{id_v1}").into_bytes();
    assert!(
        pairs.iter().any(|(k, _)| k == &keys::instance_key(id_v2)),
        "v4 scan must contain the v2 instance key"
    );
    assert!(
        !pairs.iter().any(|(k, _)| k == &v1_key),
        "v4 scan must not include a v1 instance key"
    );

    let (insts, _token) = wf
        .list_instances(None, None, usize::MAX, None)
        .await
        .unwrap();
    assert!(insts.iter().any(|i| i.id == id_v1));
    assert!(insts.iter().all(|i| i.id != id_v2));
}

// ────────────────────────────────────────────── 判据 7：丢失响应后的幂等重放

/// 提交成功后原样重发同一请求（模拟响应丢失后的重试）：`Ok(true)` 且 outbox 逐字不变。
#[tokio::test]
async fn replay_after_lost_response_is_idempotent() {
    let env = setup().await;
    let inner = env.inner();
    let outbox = Arc::new(KvOutboxStore::new(Arc::clone(&inner)));
    let store = KvApprovalStore::new(Arc::clone(&inner), Arc::clone(&outbox));
    let id = "inst-t6-replay";

    let mut eng = start_engine(id, "alice", "sha256:t6-replay");
    let _ = eng
        .enter_branch(
            eng.instance.revision,
            "apply",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            1_001,
        )
        .expect("enter branch");
    let batch = vec![
        event(id, "evt-t6-replay-1", 1_001),
        event(id, "evt-t6-replay-2", 1_002),
    ];
    assert!(store
        .commit(&eng, &batch, ExpectedRev::CreateOnly)
        .await
        .unwrap());
    let first = records_for(&outbox, id).await;
    assert_eq!(first.len(), 2);

    // 同一状态 + 同一批事件 + 同一请求谓词：重放路径幂等成功，零写入。
    assert!(
        store
            .commit(&eng, &batch, ExpectedRev::CreateOnly)
            .await
            .unwrap(),
        "re-sending the same request must report success"
    );
    assert_eq!(
        records_for(&outbox, id).await,
        first,
        "replay must not add, renumber or rewrite outbox records"
    );
    let (loaded, _) = store.load(id).await.unwrap().expect("present");
    assert_eq!(loaded, eng);
}

// ─────────────────────────────────────────────── 判据 8：空批次只写状态

/// 空事件批次只提交实例状态（不写任何 outbox 记录）；并证明「部分批次不能重放」。
#[tokio::test]
async fn empty_event_batch_commits_state_only() {
    let env = setup().await;
    let inner = env.inner();
    let outbox = Arc::new(KvOutboxStore::new(Arc::clone(&inner)));
    let store = KvApprovalStore::new(Arc::clone(&inner), Arc::clone(&outbox));

    // 空批次：CreateOnly 与 Exact 都成功，状态推进，不产生 outbox 记录。
    let id = "inst-t6-empty";
    let mut eng = start_engine(id, "alice", "sha256:t6-empty");
    assert!(store
        .commit(&eng, &[], ExpectedRev::CreateOnly)
        .await
        .unwrap());
    let (state0, rev0) = store.load(id).await.unwrap().expect("created");
    assert_eq!(state0, eng);
    assert!(
        records_for(&outbox, id).await.is_empty(),
        "a state-only commit writes no outbox records"
    );

    let _ = eng
        .enter_branch(
            eng.instance.revision,
            "apply",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            1_001,
        )
        .expect("enter branch");
    assert!(store
        .commit(&eng, &[], ExpectedRev::Exact(rev0))
        .await
        .unwrap());
    let (state1, rev1) = store.load(id).await.unwrap().expect("advanced");
    assert_eq!(state1, eng);
    assert!(rev1 > rev0);
    assert!(
        records_for(&outbox, id).await.is_empty(),
        "later state-only commits still write no outbox records"
    );

    // 部分批次不能重放：先做一次成功的 2 事件提交，再提交**不同状态** + 仅第一条事件。
    //
    // 为何用不同状态：状态逐字相同的子集会命中重放路径（本批事件全在库 ⇒ 幂等成功），
    // 语义上可辩护却无法区分「完整重放」与「部分批次」。换成不同状态后不再走重放路径，
    // 结果唯一：该批事件 id 已存在（outbox 侧 `VERSION==0` 谓词失败）⇒ 整笔零写入。
    let id2 = "inst-t6-partial";
    let mut eng2 = start_engine(id2, "alice", "sha256:t6-partial");
    assert!(store
        .commit(&eng2, &[], ExpectedRev::CreateOnly)
        .await
        .unwrap());
    let (_, rev2) = store.load(id2).await.unwrap().expect("created");
    let _ = eng2
        .enter_branch(
            eng2.instance.revision,
            "apply",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            1_001,
        )
        .expect("enter branch");
    let batch = vec![
        event(id2, "evt-t6-partial-1", 1_001),
        event(id2, "evt-t6-partial-2", 1_002),
    ];
    assert!(store
        .commit(&eng2, &batch, ExpectedRev::Exact(rev2))
        .await
        .unwrap());
    let (stored, rev) = store.load(id2).await.unwrap().expect("present");
    let recs_before = records_for(&outbox, id2).await;
    assert_eq!(recs_before.len(), 2);

    let mut partial = eng2.clone();
    partial.instance.context.insert("spoof".into(), json!(true));
    assert!(
        !store
            .commit(&partial, &batch[..1], ExpectedRev::Exact(rev))
            .await
            .unwrap(),
        "a partial event batch must not be accepted as a replay"
    );
    let (after, rev_after) = store.load(id2).await.unwrap().expect("present");
    assert_eq!(after, stored, "rejected partial batch must not write state");
    assert_eq!(
        rev_after, rev,
        "rejected partial batch must not advance the revision"
    );
    assert_eq!(
        records_for(&outbox, id2).await,
        recs_before,
        "rejected partial batch must not touch the outbox"
    );
}
