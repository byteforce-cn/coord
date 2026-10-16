//! Task 1 集成测试：step token 存储（KV create-if-absent）与幂等键经 dispatcher 透传。
//!
//! 覆盖：`register` 的 create-if-absent 语义、`mark_done` 可见性，以及重试时
//! dispatcher 收到**同一非空**幂等键（外部副作用可被下游去重）。

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use coord_agent::cache::AgentCache;
use coord_agent::services::outbox_kv::KvOutboxStore;
use coord_agent::services::step_kv::KvStepStore;
use coord_agent::services::workflow_store::KvWorkflowStore;
use coord_agent::AgentInner;
use coord_client::config::Config;
use coord_client::Client;
use coord_core::workflow::durable::{
    DurableSupport, DurableTimerStore, OutboxStore, StepState, StepTokenStore,
};
use coord_core::workflow::engine::WorkflowExecutor;
use coord_core::workflow::expression::ExpressionEvaluator;
use coord_core::workflow::model::{
    CallTask, CallType, Document, InstanceStatus, NamedTask, RetryPolicy, Task, TaskMeta,
    WorkflowDefinition, WorkflowInstance,
};
use coord_core::workflow::ports::{
    DispatchResult, NoopEventProvider, SystemClock, TaskDispatcher, WorkflowStore,
};
use coord_core::workflow::runtime::WorkflowRuntime;

/// 进程内单节点服务 + 客户端；**必须持有到测试结束**（drop 会关闭 gRPC 服务）。
struct TestEnv {
    inner: Arc<AgentInner>,
    _tmp: tempfile::TempDir,
    _shutdown: tokio::sync::oneshot::Sender<()>,
    _grpc: tokio::task::JoinHandle<()>,
    _raft: tokio::task::JoinHandle<()>,
}

async fn setup() -> TestEnv {
    let (addr, shutdown, grpc, raft, tmp) = common::start_test_server().await;
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

/// 判据 1：`register` 是 create-if-absent（后来者读到既有 token，不覆盖），
/// `mark_done` 之后 `get` 可见 `Done`。
#[tokio::test(flavor = "multi_thread")]
async fn register_is_create_if_absent_and_mark_done_is_visible() {
    let env = setup().await;
    let steps = KvStepStore::new(env.inner.clone());

    let (first, created) = steps
        .register("inst-1", "0:callApi", "wf:inst-1:0:callApi", 1000)
        .await
        .expect("first register");
    assert!(created, "首次登记必须 create");
    assert_eq!(first.token, "wf:inst-1:0:callApi");
    assert_eq!(first.state, StepState::Pending);
    assert_eq!(first.at_ms, 1000);

    // 第二次登记传入不同 token：已存在 ⇒ created=false，返回既有记录的 token。
    let (second, created2) = steps
        .register("inst-1", "0:callApi", "wf:DIFFERENT", 2000)
        .await
        .expect("second register");
    assert!(!created2, "已存在 ⇒ created=false");
    assert_eq!(second.token, "wf:inst-1:0:callApi", "token 以既有记录为准");
    assert_eq!(second.at_ms, 1000, "登记时刻不被覆盖");

    steps
        .mark_done("inst-1", "0:callApi")
        .await
        .expect("mark_done");
    let got = steps
        .get("inst-1", "0:callApi")
        .await
        .expect("get")
        .expect("record exists");
    assert_eq!(got.state, StepState::Done);
    assert_eq!(got.token, "wf:inst-1:0:callApi");
}

/// 记录每次派发的幂等键；前 `fail_first` 次返回可重试失败，之后成功。
struct CaptureDispatcher {
    fail_first: usize,
    calls: AtomicUsize,
    keys: Mutex<Vec<Option<String>>>,
}

#[tonic::async_trait]
impl TaskDispatcher for CaptureDispatcher {
    async fn dispatch(
        &self,
        _service: &str,
        _with: Option<&serde_json::Value>,
        _input: &serde_json::Value,
        idempotency_key: Option<&str>,
    ) -> DispatchResult {
        self.keys
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(idempotency_key.map(str::to_string));
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        if n < self.fail_first {
            DispatchResult::Failure {
                error: "temporary outage".into(),
                retryable: true,
            }
        } else {
            DispatchResult::Success {
                data: serde_json::json!({"ok": true}),
            }
        }
    }
}

fn retry_definition() -> WorkflowDefinition {
    let mut task_meta = std::collections::HashMap::new();
    task_meta.insert(
        "callApi".to_string(),
        TaskMeta {
            retry: Some(RetryPolicy {
                delay: "PT0.05S".into(),
                backoff: None,
                limit: 3,
                jitter: None,
            }),
            ..Default::default()
        },
    );
    WorkflowDefinition {
        id: None,
        document: Document {
            dsl: "1.0.0".into(),
            namespace: "steptoken".into(),
            name: "retry-call".into(),
            version: "1.0".into(),
            title: None,
            summary: None,
            tags: None,
        },
        do_tasks: vec![NamedTask {
            name: "callApi".into(),
            task: Task::Call(CallTask {
                call: CallType::Http,
                with: None,
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
        task_meta,
        raw_yaml: None,
    }
}

fn make_runtime(
    inner: Arc<AgentInner>,
    dispatcher: Arc<CaptureDispatcher>,
) -> WorkflowRuntime<
    ExpressionEvaluator,
    SystemClock,
    KvWorkflowStore,
    Arc<CaptureDispatcher>,
    NoopEventProvider,
> {
    let store = KvWorkflowStore::new(inner.clone());
    let executor = WorkflowExecutor::new(ExpressionEvaluator::new(), SystemClock::new());
    let timers: Arc<dyn DurableTimerStore> = Arc::new(KvOutboxStore::new(inner.clone()));
    let outbox: Arc<dyn OutboxStore> = Arc::new(KvOutboxStore::new(inner.clone()));
    let steps: Arc<dyn StepTokenStore> = Arc::new(KvStepStore::new(inner.clone()));
    WorkflowRuntime::new(
        executor,
        SystemClock::new(),
        store,
        dispatcher,
        NoopEventProvider,
    )
    .with_durable(DurableSupport::new(timers, outbox).with_steps(steps))
}

/// 判据 2：call 首次可重试失败 + 一次重试后成功 ⇒ 两次派发携带**同一非空**幂等键。
#[tokio::test(flavor = "multi_thread")]
async fn retry_reuses_same_idempotency_key() {
    let env = setup().await;
    let inner = env.inner.clone();

    let def = retry_definition();
    let store = KvWorkflowStore::new(inner.clone());
    store.save_definition(&def).await.expect("save definition");
    let mut inst = WorkflowInstance::new(&def, serde_json::json!({}), 1000);
    inst.status = InstanceStatus::Running;
    store.save_instance(&inst).await.expect("seed instance");

    let dispatcher = Arc::new(CaptureDispatcher {
        fail_first: 1,
        calls: AtomicUsize::new(0),
        keys: Mutex::new(Vec::new()),
    });
    let runtime = make_runtime(inner.clone(), Arc::clone(&dispatcher));
    runtime.drive_instance(&inst.id).await.expect("drive");

    // 首次失败 → 定时重试；等实例进入终态。
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let loaded = store
            .load_instance_with_rev(&inst.id)
            .await
            .ok()
            .flatten()
            .map(|(i, _)| i);
        if let Some(loaded) = loaded {
            if loaded.status.is_terminal() {
                assert_eq!(loaded.status, InstanceStatus::Completed);
                break;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "workflow did not reach terminal state"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let keys = dispatcher
        .keys
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    assert_eq!(keys.len(), 2, "首次失败 + 一次重试 = 两次派发");
    for key in &keys {
        assert!(
            key.as_deref().is_some_and(|k| !k.is_empty()),
            "每次派发都必须携带非空幂等键: {key:?}"
        );
    }
    assert_eq!(keys[0], keys[1], "重试必须复用同一幂等键");
    // step token 已被标 Done（成功那次）。
    let steps = KvStepStore::new(inner.clone());
    let rec = steps
        .get(&inst.id, "0:callApi")
        .await
        .expect("get step")
        .expect("step exists");
    assert_eq!(rec.state, StepState::Done);
}
