//! 工作流基础里程碑 —— 启动恢复与控制面 CAS 语义。
//!
//! 在进程内真实单节点 Raft KV（`common::start_test_server`）上验证：
//! - 控制面（无租约）恢复经「线性一致读修订号 + CAS 提交」；两个并发 `resume` 恰一个生效，
//!   另一个返回 `AlreadyCompleted`/`StaleRevision`，且 outbox 中 `workflow.resumed` 恰好一条；
//! - 重启对账把非终态的 `Running` 实例重新驱动至终态，终态事件不丢。
//!
//! 复现：`CARGO_BUILD_JOBS=3 cargo test -p coord --test workflow_foundation -- --test-threads=1`

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use coord_agent::service::BaseService;
use coord_agent::services::mq::MessageQueueService;
use coord_agent::services::outbox_kv::KvOutboxStore;
use coord_agent::services::workflow::phase4::{DurableEngineOptions, WorkflowEngineService};
use coord_agent::services::workflow_store::KvWorkflowStore;
use coord_agent::WorkflowDispatchConfig;
use coord_core::workflow::durable::{
    AtomicOutboxCommit, DurableSupport, DurableTimerStore, OutboxStore,
};
use coord_core::workflow::engine::WorkflowExecutor;
use coord_core::workflow::expression::ExpressionEvaluator;
use coord_core::workflow::model::{
    definition_hash, Document, EventFilter, InstanceStatus, ListenTask, NamedTask, Task,
    WorkflowDefinition, WorkflowInstance,
};
use coord_core::workflow::ports::{
    DispatchResult, EventDelivery, EventProvider, NoopEventProvider, SystemClock, TaskDispatcher,
    WorkflowStore,
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

/// 一步到位的定义：`start` 即 inject（展开为 set 任务），无挂起 ⇒ 只要驱动起来就到终态。
const RUN_YAML: &str = r#"
document:
  dsl: "1.0.3"
  namespace: foundation
  name: run-recover
  version: "1.0.0"
id: run-recover
version: "1.0.0"
start: work
states:
  - name: work
    type: inject
    data:
      completed: true
    end: true
"#;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// 处于 `Running` 的实例（崩溃快照：无 `drivers/` 租约键、无人驱动）在引擎重启后由启动
/// 对账重新驱动至终态，且终态事件不丢。
#[tokio::test(flavor = "multi_thread")]
async fn running_instance_is_redriven_after_restart() {
    let (addr, _sd, _g, _r, tmp) = common::start_test_server().await;
    let inner = agent_inner(&addr).await;
    let mq = Arc::new(MessageQueueService::new(
        tmp.path().to_path_buf(),
        64 * 1024 * 1024,
    ));
    mq.start().await.expect("start mq");

    let opts = || DurableEngineOptions {
        mq: Arc::clone(&mq),
        topic: "com.china-workflow.foundation.events".to_string(),
        relay_enabled: true,
        relay_interval_ms: 50,
        approval_roles: std::collections::HashMap::new(),
        on_behalf_services: Vec::new(),
    };

    // 起一个引擎部署定义，并取回已部署定义（快照要按它构造）。
    let svc = WorkflowEngineService::new_with_kv_store_durable(
        Arc::clone(&inner),
        &WorkflowDispatchConfig::default(),
        opts(),
    );
    svc.start().await.expect("engine start");
    let def_id = svc
        .deploy_definition("foundation", RUN_YAML)
        .await
        .expect("deploy");
    let def = svc
        .get_definition(&def_id)
        .await
        .expect("get definition")
        .expect("definition exists");

    // 「崩溃快照」：直接落一个 `Running`、任务栈为空、**无** `drivers/` 租约键的实例
    //（上个进程被杀后 KV 里遗留的状态）。
    let mut snapshot = WorkflowInstance::new(&def, serde_json::json!({}), now_ms());
    snapshot.status = InstanceStatus::Running;
    snapshot.updated_at = now_ms();
    let store = KvWorkflowStore::new(Arc::clone(&inner));
    store
        .save_instance(&snapshot)
        .await
        .expect("seed running snapshot");
    drop(svc); // 上个进程的 runtime 随之消失

    // 重启：全新引擎在同一 KV 上 start() ⇒ recover_durable_state 对账重驱动。
    let svc2 = WorkflowEngineService::new_with_kv_store_durable(
        Arc::clone(&inner),
        &WorkflowDispatchConfig::default(),
        opts(),
    );
    svc2.start().await.expect("restart engine");

    // 断言：超时内到达终态。
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(Some((inst, _))) = store.load_instance_with_rev(&snapshot.id).await {
            if inst.status == InstanceStatus::Completed {
                break;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Running 实例重启后未被对账重驱动至终态"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // 事件不丢：该实例的 workflow.completed 生命周期事件已落 outbox。
    let outbox = Arc::new(KvOutboxStore::new(Arc::clone(&inner)));
    let all = OutboxStore::list_all(&*outbox).await.expect("list outbox");
    assert!(
        all.iter().any(|r| {
            r.aggregate_id == snapshot.id && r.event_type.ends_with("workflow.completed.v1")
        }),
        "重驱动后的终态事件必须落 outbox: {all:?}"
    );
}

/// ack 记录：(事件类型, 投递 offset, ack 时读回的实例状态)
type AckRecord = (String, u64, Option<InstanceStatus>);

/// 事件提供者：`wait_for_event` 立即交付；`ack_event` 时按 id 读回实例状态并记录。
///
/// 用来断言 ack **晚于**状态写：投递原子化要求「位点提交延后到实例状态写成功之后」，
/// 读回 `Waiting` 即意味着 ack 抢在状态写之前（崩溃窗口会丢投递）。
struct AckAfterStateProvider {
    store: KvWorkflowStore,
    instance_id: String,
    acks: Arc<Mutex<Vec<AckRecord>>>,
}

#[tonic::async_trait]
impl EventProvider for AckAfterStateProvider {
    async fn emit(&self, _event_type: &str, _source: Option<&str>, _data: &serde_json::Value) {}

    async fn wait_for_event(
        &self,
        _event_types: &[&str],
        _source: Option<&str>,
        _subject: Option<&str>,
        _timeout_ms: u64,
    ) -> Option<EventDelivery> {
        Some(EventDelivery {
            event_type: "approval.requested".into(),
            subscription: "foundation-sub".into(),
            offset: 7,
        })
    }

    async fn ack_event(&self, delivery: &EventDelivery) {
        let status = self
            .store
            .load_instance(&self.instance_id)
            .await
            .ok()
            .flatten()
            .map(|i| i.status);
        self.acks
            .lock()
            .unwrap()
            .push((delivery.event_type.clone(), delivery.offset, status));
    }
}

/// listen 的事件在实例状态写成功之后才被 ack，且 ack 携带原投递的信封。
#[tokio::test(flavor = "multi_thread")]
async fn listen_event_is_acked_after_state_commit() {
    let (addr, _sd, _g, _r, _tmp) = common::start_test_server().await;
    let inner = agent_inner(&addr).await;
    let store = KvWorkflowStore::new(inner.clone());

    let def = listen_definition();
    store.save_definition(&def).await.expect("save definition");

    // 已知 id 的 Pending 快照由本测试驱动：provider 才能在 ack 时按 id 读回状态。
    let mut snapshot = WorkflowInstance::new(&def, serde_json::json!({}), now_ms());
    snapshot.id = "foundation-listen-ack-order".into();
    store.save_instance(&snapshot).await.expect("seed instance");

    let acks = Arc::new(Mutex::new(Vec::new()));
    let provider = AckAfterStateProvider {
        store: KvWorkflowStore::new(inner.clone()),
        instance_id: snapshot.id.clone(),
        acks: Arc::clone(&acks),
    };

    let executor = WorkflowExecutor::new(ExpressionEvaluator::new(), SystemClock::new());
    let outbox = Arc::new(KvOutboxStore::new(inner.clone()));
    let durable = DurableSupport::new(
        Arc::clone(&outbox) as Arc<dyn DurableTimerStore>,
        Arc::clone(&outbox) as Arc<dyn OutboxStore>,
    )
    .with_atomic_commit(Arc::clone(&outbox) as Arc<dyn AtomicOutboxCommit>);
    let rt = WorkflowRuntime::new(
        executor,
        SystemClock::new(),
        store,
        NoopDispatcher,
        provider,
    )
    .with_durable(durable);

    rt.drive_instance(&snapshot.id).await.expect("drive");

    // provider 立即交付 ⇒ listen 恢复并驱动到终态。
    let probe = KvWorkflowStore::new(inner.clone());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(Some((inst, _))) = probe.load_instance_with_rev(&snapshot.id).await {
            if inst.status == InstanceStatus::Completed {
                break;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "listen 实例在事件到达后未完成"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let recorded = acks.lock().unwrap();
    assert_eq!(recorded.len(), 1, "listen 事件恰 ack 一次: {recorded:?}");
    assert_eq!(recorded[0].0, "approval.requested");
    assert_eq!(recorded[0].1, 7, "ack 必须携带原投递的 offset");
    match recorded[0].2 {
        Some(InstanceStatus::Running) | Some(InstanceStatus::Completed) => {}
        other => panic!("ack 必须晚于实例状态写（ack 时状态 = {other:?}）"),
    }
}

/// 同 `(namespace, name, version)` 重部署成不同内容后 `resume`：
/// 实例钉死的定义哈希与当前加载的定义不匹配 ⇒ fail-closed。
///
/// 语义：定义内容覆盖（重部署）不改变实例的钉死哈希，实例会看出「加载到的不是启动时那一份」，
/// 因此不得在新语义上继续推进 —— 控制面返回 `DefinitionDrift`，实例置 `Faulted` 并落
/// `workflow.faulted`。
#[tokio::test(flavor = "multi_thread")]
async fn resume_after_redeploy_same_version_fails_closed() {
    let (addr, _sd, _g, _r, _tmp) = common::start_test_server().await;
    let inner = agent_inner(&addr).await;
    let rt = make_runtime(inner.clone());
    let store = KvWorkflowStore::new(inner.clone());

    let def = listen_definition();
    store
        .save_definition(&def)
        .await
        .expect("deploy definition");
    let inst = rt.start(&def, serde_json::json!({})).await.expect("start");
    wait_until_waiting(&store, &inst.id).await;

    // 同版本、同键重部署，但内容变了（改文档标题即可改变内容哈希）。
    let mut redeployed = def.clone();
    redeployed.document.title = Some("redeployed-content".into());
    store
        .save_definition(&redeployed)
        .await
        .expect("redeploy same version");

    let err = rt
        .resume(
            &inst.id,
            Some("approval.requested"),
            None,
            Some("key-drift"),
        )
        .await
        .expect_err("定义漂移时 resume 必须 fail-closed");
    let (expected, actual) = match err {
        RuntimeError::DefinitionDrift {
            instance_id,
            expected,
            actual,
        } => {
            assert_eq!(instance_id, inst.id);
            // expected = 实例创建时钉死的原始内容哈希；actual = 重部署后的内容哈希
            assert_eq!(expected, definition_hash(&def));
            assert_ne!(expected, actual, "漂移的 expected/actual 必须不同");
            (expected, actual)
        }
        other => panic!("期望 RuntimeError::DefinitionDrift，实际: {other:?}"),
    };

    // 实例置 Faulted，fault 文本含运行时的逐字诊断锚点。
    let failed = store
        .load_instance(&inst.id)
        .await
        .expect("load instance")
        .expect("instance exists");
    assert_eq!(failed.status, InstanceStatus::Failed);
    let fault = failed.fault.expect("faulted 实例必须带 fault");
    assert!(
        fault.detail.contains(&format!(
            "definition drift: instance pinned {expected}, loaded {actual}"
        )),
        "fault 文本必须含稳定诊断锚点: {fault:?}"
    );

    // outbox：只允许漂移 fault —— 钉死校验在状态写之前，不得落 `workflow.resumed`。
    let outbox = Arc::new(KvOutboxStore::new(inner.clone()));
    let all = OutboxStore::list_all(&*outbox).await.expect("list outbox");
    assert!(
        all.iter().any(|r| {
            r.aggregate_id == inst.id && r.event_type.ends_with("workflow.faulted.v1")
        }),
        "漂移 fault 必须落 workflow.faulted: {all:?}"
    );
    assert!(
        !all.iter().any(|r| {
            r.aggregate_id == inst.id && r.event_type.ends_with("workflow.resumed.v1")
        }),
        "钉死漂移不得提交 resume 转换（workflow.resumed）: {all:?}"
    );
}

/// 钉死哈希与当前定义不一致的 `Running` 实例：启动对账经 `spawn_drive` 进入 drive，
/// drive 的定义钉死守卫在**执行任何任务前** fail-closed（置 Failed + 稳定诊断锚点）。
///
/// 与 `resume_after_redeploy_same_version_fails_closed` 互补：那条走控制面入口，
/// 这条走 drive 本体，覆盖「对账重驱动一个钉死已过期的实例」。
#[tokio::test(flavor = "multi_thread")]
async fn stale_pin_running_instance_faults_on_restart_reconcile() {
    let (addr, _sd, _g, _r, tmp) = common::start_test_server().await;
    let inner = agent_inner(&addr).await;
    let mq = Arc::new(MessageQueueService::new(
        tmp.path().to_path_buf(),
        64 * 1024 * 1024,
    ));
    mq.start().await.expect("start mq");

    let opts = || DurableEngineOptions {
        mq: Arc::clone(&mq),
        topic: "com.china-workflow.foundation.events".to_string(),
        relay_enabled: true,
        relay_interval_ms: 50,
        approval_roles: std::collections::HashMap::new(),
        on_behalf_services: Vec::new(),
    };
    let svc = WorkflowEngineService::new_with_kv_store_durable(
        Arc::clone(&inner),
        &WorkflowDispatchConfig::default(),
        opts(),
    );
    svc.start().await.expect("engine start");
    let def_id = svc
        .deploy_definition("foundation", RUN_YAML)
        .await
        .expect("deploy");
    let def = svc
        .get_definition(&def_id)
        .await
        .expect("get definition")
        .expect("definition exists");

    // 崩溃快照：`Running`、无人驱动，但钉死哈希指向一份**已不在定义键上**的内容。
    let stale = "f".repeat(64);
    let mut snapshot = WorkflowInstance::new(&def, serde_json::json!({}), now_ms());
    snapshot.status = InstanceStatus::Running;
    snapshot.definition_hash = Some(stale.clone());
    snapshot.updated_at = now_ms();
    let store = KvWorkflowStore::new(Arc::clone(&inner));
    store
        .save_instance(&snapshot)
        .await
        .expect("seed stale-pin snapshot");
    drop(svc);

    let svc2 = WorkflowEngineService::new_with_kv_store_durable(
        Arc::clone(&inner),
        &WorkflowDispatchConfig::default(),
        opts(),
    );
    svc2.start().await.expect("restart engine");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let fault = loop {
        if let Ok(Some((inst, _))) = store.load_instance_with_rev(&snapshot.id).await {
            if inst.status == InstanceStatus::Failed {
                break inst.fault.expect("faulted 实例必须带 fault");
            }
            assert!(
                !inst.status.is_terminal(),
                "钉死漂移的实例不得被推进到 Completed: {inst:?}"
            );
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "钉死漂移的 Running 实例未被对账 fail-closed"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(
        fault.detail.contains(&format!(
            "definition drift: instance pinned {stale}, loaded {}",
            definition_hash(&def)
        )),
        "fault 文本必须含运行时的逐字诊断锚点: {fault:?}"
    );
}
