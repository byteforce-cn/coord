//! Task 4 —— 非 drive 保存点收口：控制面恢复的 CAS 语义。
//!
//! 在进程内真实单节点 Raft KV（`common::start_test_server`）上验证：控制面（无租约）
//! 恢复经「线性一致读修订号 + CAS 提交」；两个并发 `resume` 恰一个生效，另一个返回
//! `AlreadyCompleted`/`StaleRevision`，且 outbox 中 `workflow.resumed` 恰好一条
//! （裸写路径会让两条事件都落库）。
//!
//! 复现：`CARGO_BUILD_JOBS=3 cargo test -p coord --test workflow_foundation -- --test-threads=1`

mod common;

use std::sync::Arc;
use std::time::Duration;

use coord_agent::services::outbox_kv::KvOutboxStore;
use coord_agent::services::workflow_store::KvWorkflowStore;
use coord_core::workflow::durable::{
    AtomicOutboxCommit, DurableSupport, DurableTimerStore, OutboxStore,
};
use coord_core::workflow::engine::WorkflowExecutor;
use coord_core::workflow::expression::ExpressionEvaluator;
use coord_core::workflow::model::{
    Document, EventFilter, InstanceStatus, ListenTask, NamedTask, Task, WorkflowDefinition,
};
use coord_core::workflow::ports::{
    DispatchResult, NoopEventProvider, SystemClock, TaskDispatcher, WorkflowStore,
};
use coord_core::workflow::runtime::{RuntimeError, WorkflowRuntime};

/// 本定义只用 listen 任务，派发器不会被调用；占位以满足运行时类型参数。
struct NoopDispatcher;

#[tonic::async_trait]
impl TaskDispatcher for NoopDispatcher {
    async fn dispatch(
        &self,
        _service: &str,
        _with: Option<&serde_json::Value>,
        _input: &serde_json::Value,
        _idempotency_key: Option<&str>,
    ) -> DispatchResult {
        DispatchResult::Success {
            data: serde_json::Value::Null,
        }
    }
}

/// listen 挂起定义：`start` 后实例停在 `Waiting`（reason=`listen`），等待
/// `approval.requested`。
fn listen_definition() -> WorkflowDefinition {
    WorkflowDefinition {
        id: None,
        document: Document {
            dsl: "1.0.0".into(),
            namespace: "foundation".into(),
            name: "suspend-listen".into(),
            version: "1.0".into(),
            title: None,
            summary: None,
            tags: None,
        },
        do_tasks: vec![NamedTask {
            name: "approve".into(),
            task: Task::Listen(ListenTask {
                listen: EventFilter {
                    event_type: Some("approval.requested".into()),
                    event_types: vec![],
                    source: None,
                    subject: None,
                },
            }),
        }],
        input: None,
        output: None,
        timeout: None,
        use_components: None,
        schedule: Default::default(),
        auth: Default::default(),
        secrets: Default::default(),
        constants: Default::default(),
        task_meta: Default::default(),
        raw_yaml: None,
    }
}

async fn agent_inner(addr: &str) -> Arc<coord_agent::AgentInner> {
    Arc::new(
        coord_agent::AgentInner::new(
            vec![addr.to_string()],
            coord_agent::cache::AgentCache::new(128, 0, 128, 0),
            None,
            // 测试环境不开服务端鉴权：回退凭据留空。
            Arc::new(coord_client::credential::CachedTokenProvider::new(None)),
        )
        .await
        .expect("connect agent inner"),
    )
}

/// 装配真实 KV 运行时：store / outbox / 原子提交全部落在同一 Raft KV 上，
/// 使 `commit_instance` 走 CAS 的 `commit_state_and_outbox_batch`（生产路径）。
fn make_runtime(
    inner: Arc<coord_agent::AgentInner>,
) -> WorkflowRuntime<
    ExpressionEvaluator,
    SystemClock,
    KvWorkflowStore,
    NoopDispatcher,
    NoopEventProvider,
> {
    let store = KvWorkflowStore::new(inner.clone());
    let executor = WorkflowExecutor::new(ExpressionEvaluator::new(), SystemClock::new());
    let outbox = Arc::new(KvOutboxStore::new(inner.clone()));
    let durable = DurableSupport::new(
        Arc::clone(&outbox) as Arc<dyn DurableTimerStore>,
        Arc::clone(&outbox) as Arc<dyn OutboxStore>,
    )
    .with_atomic_commit(Arc::clone(&outbox) as Arc<dyn AtomicOutboxCommit>);
    WorkflowRuntime::new(
        executor,
        SystemClock::new(),
        store,
        NoopDispatcher,
        NoopEventProvider,
    )
    .with_durable(durable)
}

/// 有界轮询直到实例进入 `Waiting`（线性一致读，绕过本地缓存）。
async fn wait_until_waiting(store: &KvWorkflowStore, id: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(Some((inst, _))) = store.load_instance_with_rev(id).await {
            if inst.status == InstanceStatus::Waiting {
                return;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "instance {id} never reached Waiting"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_resume_exactly_one_takes_effect() {
    let (addr, _sd, _g, _r, _tmp) = common::start_test_server().await;
    let inner = agent_inner(&addr).await;
    let rt = make_runtime(inner.clone());
    let store = KvWorkflowStore::new(inner.clone());

    let def = listen_definition();
    store.save_definition(&def).await.expect("save definition");
    let inst = rt.start(&def, serde_json::json!({})).await.expect("start");
    wait_until_waiting(&store, &inst.id).await;

    // 两个并发 resume（不同幂等键）：只有一次恢复能提交。
    let (a, b) = tokio::join!(
        rt.resume(&inst.id, Some("approval.requested"), None, Some("key-A")),
        rt.resume(&inst.id, Some("approval.requested"), None, Some("key-B")),
    );

    let outcomes = [a, b];
    let ok = outcomes.iter().filter(|r| r.is_ok()).count();
    assert_eq!(ok, 1, "恰一个 resume 必须生效: {outcomes:?}");
    let loser = outcomes.iter().find(|r| r.is_err()).expect("a loser");
    match loser.as_ref().unwrap_err() {
        RuntimeError::AlreadyCompleted(_) | RuntimeError::StaleRevision(_) => {}
        other => panic!("输的一方必须是 AlreadyCompleted/StaleRevision，实际: {other:?}"),
    }

    // outbox：workflow.resumed 恰好一条（裸写会让两条事件都落库）。
    let outbox = Arc::new(KvOutboxStore::new(inner.clone()));
    let all = OutboxStore::list_all(&*outbox).await.expect("list outbox");
    let resumed = all
        .iter()
        .filter(|r| r.aggregate_id == inst.id && r.event_type.ends_with("workflow.resumed.v1"))
        .count();
    assert_eq!(
        resumed, 1,
        "并发 resume 只允许一条 workflow.resumed: {all:?}"
    );
}
