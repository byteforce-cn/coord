// coord-core/workflow/runtime.rs
// WorkflowRuntime —— 异步驱动循环
//
// 协调 WorkflowExecutor（纯状态机）与外部依赖（Store、Dispatcher、EventProvider），
// 实现工作流实例的完整生命周期管理：
// - start: 创建并启动实例
// - resume: 从挂起状态恢复
// - drive: 异步驱动循环（spawn 后独立运行）
//
// 实现基本驱动循环，支持 call/do/switch/wait 任务
// 补充 任务类型（fork/for-each/listen 等）
// 对接 coord-agent WorkflowService
//
// 实例写路径（收口）：
// - 对**已存在实例**的状态写一律经 [`WorkflowRuntime::commit_instance`]
//   （CAS/fencing + 状态与伴生事件同事务）；控制面（无租约）另经
//   [`WorkflowRuntime::commit_control_plane`] 做「线性一致读修订号 + 冲突重放」。
// - 仅有的裸写点（新键创建，不存在并发写同一键的问题）：
//   ① `start` 的首次实例写；② `start` 输入校验失败时的 faulted 实例写；
//   ③ `run` 子流程内联创建子实例的写。
//   除这三处（及 `commit_instance` 内部的降级分支）外，任何 `save_instance(` 都是回归。

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

use super::approval::bridge::ApprovalBridge;
use super::durable::{DurableSupport, OutboxRecord, TimerKind, TimerRecord};
use super::engine::WorkflowExecutor;
use super::model::{
    EventFilter, InstanceStatus, StepResult, SuspendReason, SuspensionMeta, Task, TaskFrame,
    TaskStatus, WorkflowDefinition, WorkflowFault, WorkflowInstance,
};
use super::ports::{
    Clock, DispatchResult, EventProvider, ExpressionEval, InstanceLease, LeaseGuard,
    TaskDispatcher, WorkflowStore,
};
use super::retry::{RetryConfig, RetryScheduler};

/// 子流程恢复扫描器的 tick 间隔（秒）。
///
/// tick 本身不做全量列举（见 `pending_subflows` 字段注释），
/// 所以固定 5s 不再有实例规模相关的代价。
const SUBFLOW_SCAN_INTERVAL_SECS: u64 = 5;

/// 持久化定时器扫描器的 tick 间隔（毫秒）。
///
/// 进程内快速路径仍由 `schedule_auto_resume`/`schedule_timeout` 的 `sleep` 承担；
/// 扫描器只负责「进程内定时器随进程消失」之后的那部分（重启补齐），间隔取小值以
/// 让到期触发足够及时（补偿延迟 = tick 间隔上界）。
const DURABLE_TIMER_SCAN_INTERVAL_MS: u64 = 200;

/// `call` 的 step 标识（v1 临时口径：任务索引 + 任务名）。
///
/// v2 由分支/轮次/席位替换（见 ADR D2）。retry 时任务索引不前移，故重试派发
/// 派生出的 `step_id`/`token` 逐字相同 —— 这是「重试复用同一幂等键」的依据。
fn step_id(task_index: usize, task_name: &str) -> String {
    format!("{task_index}:{task_name}")
}

/// 由实例 + step 确定性派生的幂等键（跨重试/崩溃重建不变）。
fn step_token(instance_id: &str, step_id: &str) -> String {
    format!("wf:{instance_id}:{step_id}")
}

// ─── 后台任务存活登记（能力死亡必须可观测） ───

/// 长活后台任务的**存活事实**（只读快照，供出口/指标拉取）。
///
/// 为什么需要它：`coord-core` 没有 supervisor，也没有 `tracing`，所以一个 `tokio::spawn`
/// 出去的后台循环一旦结束（或 panic），**没有任何路径会告诉任何人** —— 这种形态
/// 叫「能力静默死亡」。本结构把"死亡"变成可拉取的事实。
///
/// 两类判据（**不能混为一谈**）：
///
/// * [`Self::finished_loops`] —— **循环型** worker（如子流程扫描器）：它们的正常行为是
///   永不结束，所以"已结束"**本身**就是缺陷（`JoinHandle::is_finished()` 即可判定，
///   且它同时覆盖"返回了"与"panic 了"两种死法）。
/// * [`Self::panicked`] —— **一次性** worker（如某实例的 `drive`）：正常结束是它的**预期**
///   行为（挂起 / 终态即返回），所以"结束"不是证据。判据是"结束了 **但没跑到最后一行**"
///   —— 任务的末尾会置一个 `completed` 标志，没置位就说明中途死了。
///   把两者合成一个数字会让指标长期噪声化，反而没人看（"红色的校验没人看，就等于没有校验"）。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WorkerLiveness {
    /// 当前在跑的一次性 worker 数（`drive` 等）。
    pub live_oneshot: usize,
    /// 循环型 worker 中**已结束**的名字（非空 = 缺陷：该循环本该永不结束）。
    pub finished_loops: Vec<String>,
    /// 因中途死亡未正常收尾的一次性 worker：类别 → 累计次数。
    pub panicked: Vec<(String, u64)>,
    /// 最近死亡的具体 worker 标签（**有界**，仅用于定位是哪个实例）。
    pub recent_faults: Vec<String>,
}

impl WorkerLiveness {
    /// 是否存在**确定**的缺陷形态（循环已结束 或 有 worker 未正常收尾）。
    ///
    /// 出口方（agent 指标/告警）应该用这个而不是自己拼条件。
    pub fn has_fault(&self) -> bool {
        !self.finished_loops.is_empty() || !self.panicked.is_empty()
    }

    /// 未正常收尾的累计次数（指标用）。
    pub fn panic_total(&self) -> u64 {
        self.panicked.iter().map(|(_, n)| *n).sum()
    }
}

/// 最近死亡 worker 标签的保留上限（内存有界：登记表不得成为新的无界增长点）。
const MAX_RECENT_FAULTS: usize = 8;

/// 后台任务存活登记表（[`WorkflowRuntime`] 内部持有；`Arc` 共享给各 worker）。
#[derive(Debug, Default)]
struct WorkerRegistry {
    inner: std::sync::Mutex<WorkerRegistryInner>,
}

/// 一次性 worker 的登记项。
struct OneshotEntry {
    /// 形如 `workflow_drive[<instance_id>]`（类别用于聚合计数，后缀用于定位）。
    label: String,
    handle: tokio::task::JoinHandle<()>,
    /// 任务跑到最后一行时置位；未置位而任务已结束 ⇒ 中途死亡。
    completed: Arc<std::sync::atomic::AtomicBool>,
}

impl std::fmt::Debug for OneshotEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OneshotEntry")
            .field("label", &self.label)
            .field("finished", &self.handle.is_finished())
            .field(
                "completed",
                &self.completed.load(std::sync::atomic::Ordering::SeqCst),
            )
            .finish()
    }
}

#[derive(Debug, Default)]
struct WorkerRegistryInner {
    /// 循环型 worker：名字 → 判定句柄。
    loops: Vec<(String, tokio::task::JoinHandle<()>)>,
    /// 一次性 worker（在飞 + 尚未结算的已结束项）。
    oneshots: Vec<OneshotEntry>,
    /// 中途死亡计数：**类别** → 次数（按类别聚合，避免按实例 id 无界增长）。
    panicked: std::collections::BTreeMap<String, u64>,
    /// 最近死亡的具体标签（有界环形缓冲）。
    recent_faults: std::collections::VecDeque<String>,
}

impl WorkerRegistry {
    fn snapshot(&self) -> WorkerLiveness {
        let mut g = match self.inner.lock() {
            Ok(g) => g,
            // 登记表被 poison（持锁线程 panic）：**不能**静默返回"一切正常" ——
            // 那会把一次真实故障伪装成健康。返回一个"有故障"的快照并点名。
            Err(_) => {
                return WorkerLiveness {
                    live_oneshot: 0,
                    finished_loops: vec!["<worker registry poisoned>".to_string()],
                    panicked: Vec::new(),
                    recent_faults: Vec::new(),
                }
            }
        };

        // 结算：把已结束的一次性 worker 分成"正常收尾"（丢弃登记）与"中途死亡"
        //（计数 + 保留标签）。**这一步同时是内存回收点** —— 否则登记表会随实例数增长。
        let mut live = 0usize;
        let mut still_running: Vec<OneshotEntry> = Vec::with_capacity(g.oneshots.len());
        for entry in std::mem::take(&mut g.oneshots) {
            if !entry.handle.is_finished() {
                live += 1;
                still_running.push(entry);
                continue;
            }
            if entry.completed.load(std::sync::atomic::Ordering::SeqCst) {
                continue; // 正常收尾：不留痕、不占内存
            }
            let category = entry
                .label
                .split('[')
                .next()
                .unwrap_or(&entry.label)
                .to_string();
            *g.panicked.entry(category).or_insert(0) += 1;
            if g.recent_faults.len() == MAX_RECENT_FAULTS {
                g.recent_faults.pop_front();
            }
            g.recent_faults.push_back(entry.label);
        }
        g.oneshots = still_running;

        WorkerLiveness {
            live_oneshot: live,
            finished_loops: g
                .loops
                .iter()
                .filter(|(_, h)| h.is_finished())
                .map(|(n, _)| n.clone())
                .collect(),
            panicked: g.panicked.iter().map(|(k, v)| (k.clone(), *v)).collect(),
            recent_faults: g.recent_faults.iter().cloned().collect(),
        }
    }

    fn register_loop(&self, name: impl Into<String>, handle: tokio::task::JoinHandle<()>) {
        if let Ok(mut g) = self.inner.lock() {
            g.loops.push((name.into(), handle));
        }
    }

    /// 登记一个一次性 worker；`completed` 由调用方创建并交给任务闭包，任务**正常收尾**时置位。
    fn register_oneshot(
        &self,
        label: String,
        handle: tokio::task::JoinHandle<()>,
        completed: Arc<std::sync::atomic::AtomicBool>,
    ) {
        if let Ok(mut g) = self.inner.lock() {
            g.oneshots.push(OneshotEntry {
                label,
                handle,
                completed,
            });
        }
    }

    /// 负控制入口：注入一个"已死"的循环 worker，从而不必真的把生产任务弄死
    /// 就能验证"死亡可观测"这条判据本身有效。
    #[cfg(test)]
    fn register_loop_for_test(&self, name: &str, handle: tokio::task::JoinHandle<()>) {
        self.register_loop(name, handle);
    }
}

// ─── 生命周期事件（标准 Lifecycle Events） ───

/// 生命周期 CloudEvent 类型（`io.serverlessworkflow.*`）
pub mod lifecycle {
    pub const WORKFLOW_STARTED: &str = "io.serverlessworkflow.workflow.started.v1";
    pub const WORKFLOW_COMPLETED: &str = "io.serverlessworkflow.workflow.completed.v1";
    pub const WORKFLOW_FAULTED: &str = "io.serverlessworkflow.workflow.faulted.v1";
    pub const WORKFLOW_CANCELLED: &str = "io.serverlessworkflow.workflow.cancelled.v1";
    pub const WORKFLOW_SUSPENDED: &str = "io.serverlessworkflow.workflow.suspended.v1";
    pub const WORKFLOW_RESUMED: &str = "io.serverlessworkflow.workflow.resumed.v1";
    pub const WORKFLOW_WAITING: &str = "io.serverlessworkflow.workflow.waiting.v1";
    pub const TASK_STARTED: &str = "io.serverlessworkflow.task.started.v1";
    pub const TASK_COMPLETED: &str = "io.serverlessworkflow.task.completed.v1";
    pub const TASK_FAULTED: &str = "io.serverlessworkflow.task.faulted.v1";
    pub const TASK_RETRIED: &str = "io.serverlessworkflow.task.retried.v1";
}

/// 生命周期事件负载（CloudEvents `data`）—— 纯函数，供提交时构造事件记录。
fn lifecycle_data(inst: &WorkflowInstance) -> Value {
    serde_json::json!({
        "workflowId": inst.id,
        "workflowName": inst.definition_name,
        "workflowVersion": inst.definition_version,
        "namespace": inst.definition_ns,
        "status": format!("{:?}", inst.status).to_lowercase(),
        "output": inst.output,
        "fault": inst.fault,
    })
}

/// 生命周期事件的 outbox 形态。`id` 由调用方生成（投递面去重键）。
fn lifecycle_record(
    inst: &WorkflowInstance,
    event_type: &str,
    now_ms: i64,
    id: String,
) -> OutboxRecord {
    OutboxRecord::lifecycle(&inst.id, event_type, lifecycle_data(inst), now_ms, id)
}

// ─── 运行时错误 ───
/// 控制面（无租约）CAS 写的重放次数上界；耗尽即 [`RuntimeError::StaleRevision`]。
const CONTROL_PLANE_CAS_ATTEMPTS: usize = 3;

/// [`WorkflowRuntime::commit_control_plane`] 的重放结果。
enum ControlPlaneOutcome {
    /// 变更已提交；携带提交后的实例（供调用方加载定义 / 驱动）。
    Committed(Box<WorkflowInstance>),
    /// 守卫未满足（状态已被他人推进 / 挂起原因不再匹配）：本次不写。
    Skipped,
}

/// 运行时错误
#[derive(Debug, Clone, PartialEq)]
pub enum RuntimeError {
    /// 实例未找到
    NotFound(String),
    /// 实例已处于终端状态
    AlreadyCompleted(String),
    /// 工作流定义未找到
    DefinitionNotFound(String),
    /// 存储错误
    StoreError(String),
    /// switch Goto 目标不存在
    GotoTargetNotFound(String),
    /// signal/事件名与挂起实例期望不匹配
    InvalidSignal(String),
    /// 控制面 CAS 冲突重放耗尽：实例修订号已被并发写者推进，本次写被拒绝
    StaleRevision(String),
    /// 定义内容漂移：实例钉死的定义哈希与当前加载的定义不一致（fail-closed）
    DefinitionDrift {
        instance_id: String,
        expected: String,
        actual: String,
    },
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RuntimeError::NotFound(id) => write!(f, "instance not found: {id}"),
            RuntimeError::AlreadyCompleted(id) => {
                write!(f, "instance already in terminal state: {id}")
            }
            RuntimeError::DefinitionNotFound(id) => {
                write!(f, "workflow definition not found: {id}")
            }
            RuntimeError::StoreError(msg) => write!(f, "store error: {msg}"),
            RuntimeError::GotoTargetNotFound(target) => {
                write!(f, "goto target not found in do_tasks: {target}")
            }
            RuntimeError::InvalidSignal(msg) => write!(f, "invalid signal: {msg}"),
            RuntimeError::StaleRevision(id) => {
                write!(f, "stale revision for instance: {id}")
            }
            RuntimeError::DefinitionDrift {
                instance_id,
                expected,
                actual,
            } => write!(
                f,
                "definition drift: instance {instance_id} pinned {expected}, loaded {actual}"
            ),
        }
    }
}

/// 定义钉死守卫 —— 实例钉死的定义内容哈希必须等于当前加载的定义。
///
/// `None`（旧快照未钉死）放行：首次成功驱动时回填。返回的 `(expected, actual)` 供
/// 调用方构造稳定诊断锚点 `definition drift: instance pinned {expected}, loaded {actual}`。
pub fn assert_definition_pin(
    inst: &WorkflowInstance,
    def: &WorkflowDefinition,
) -> Result<(), RuntimeError> {
    match definition_content_drift(inst, def) {
        Some((expected, actual)) => Err(RuntimeError::DefinitionDrift {
            instance_id: inst.id.clone(),
            expected,
            actual,
        }),
        None => Ok(()),
    }
}

/// 钉死哈希与加载定义不一致时返回 `(expected, actual)`；未钉死则 `None`。
fn definition_content_drift(
    inst: &WorkflowInstance,
    def: &WorkflowDefinition,
) -> Option<(String, String)> {
    let expected = inst.definition_hash.as_ref()?;
    let actual = super::model::definition_hash(def);
    // 哈希不可计算（类型上不可达）不得放行：按漂移 fail-closed。
    if actual == super::model::DEFINITION_HASH_UNAVAILABLE {
        return Some((expected.clone(), actual));
    }
    if expected == &actual {
        None
    } else {
        Some((expected.clone(), actual))
    }
}

/// 定义漂移 fault —— detail 为跨模块稳定诊断锚点（runbook / 测试逐字断言）。
fn definition_drift_fault(expected: &str, actual: &str) -> WorkflowFault {
    crate::workflow::errors::WorkflowFault::conflict(
        "definition drift",
        format!("definition drift: instance pinned {expected}, loaded {actual}"),
    )
}

impl std::error::Error for RuntimeError {}

// ─── WorkflowRuntime ───

/// 工作流运行时 —— 管理实例的完整生命周期
pub struct WorkflowRuntime<E, C, S, D, B>
where
    E: ExpressionEval,
    C: Clock,
    S: WorkflowStore,
    D: TaskDispatcher,
    B: EventProvider,
{
    executor: Arc<WorkflowExecutor<E, C>>,
    clock: Arc<C>,
    store: Arc<S>,
    dispatcher: Arc<D>,
    event_provider: Arc<B>,
    /// 待恢复的父实例登记表（`parent_id → subflow_id`）。
    ///
    /// 子流程恢复**不得**靠每 5 秒 `list_instances(None, None, usize::MAX, None)`
    /// 全量列举 —— 稳态下每次 tick 都把**全部**工作流实例（包括与子流程完全无关
    /// 的）列出并反序列化/克隆一遍，代价随实例总数**线性增长**。本登记表：父流程因 `RunSubflow` 挂起时**登记**，
    /// 每 tick 只检查登记表里的父子对（登记表为空 ⇒ O(1)）。启动时仍做**一次**
    /// 全量对账，用于恢复上个进程遗留的挂起实例。
    pending_subflows: Arc<std::sync::Mutex<std::collections::HashMap<String, String>>>,
    /// 长活后台任务的存活登记（见 [`WorkerLiveness`]）。
    ///
    /// 没有它，`spawn` 出去的后台任务死亡时**没有任何出口**：`coord-core` 无 supervisor、
    /// 无 `tracing`，调用方也拿不到 join handle。出口由上层（`coord-agent`）周期性拉取。
    workers: Arc<WorkerRegistry>,
    /// 实例驱动租约（可选注入）：`Some` 时 `spawn_drive` 先取租约，保证一实例一驱动。
    lease: Option<Arc<dyn InstanceLease>>,
    /// 因租约被他人持有 / 租约后端不可用而**跳过**驱动的次数（可观测信号）。
    lease_skipped: Arc<std::sync::atomic::AtomicU64>,
    /// CAS 写回失败的次数（修订号冲突或 fencing 拒绝，可观测信号，不静默吞掉）。
    cas_conflicts: Arc<std::sync::atomic::AtomicU64>,
    /// 持久化能力（outbox + 定时器）。`None` = 进程内语义（旧行为）。
    ///
    /// 注入后：生命周期事件 append 进 outbox（实例状态仍只由写路径负责）；
    /// wait/retry/timeout 的到期时间落 KV，重启后由扫描器补齐进程内定时器丢失的部分。
    durable: Option<Arc<DurableSupport>>,
    /// 审批桥（可选注入）：approve 节点挂起前用它开启审批实例。
    ///
    /// `None` = 未接线：approve 节点 fail-closed（实例 `Failed` + `workflow.faulted`），
    /// 绝不静默降级为「无审批继续」。
    approval: Option<Arc<ApprovalBridge>>,
}

impl<E, C, S, D, B> WorkflowRuntime<E, C, S, D, B>
where
    E: ExpressionEval + 'static,
    C: Clock + 'static,
    S: WorkflowStore + 'static,
    D: TaskDispatcher + 'static,
    B: EventProvider + 'static,
{
    /// 创建新的运行时，同时启动子流程监控扫描器
    pub fn new(
        executor: WorkflowExecutor<E, C>,
        clock: C,
        store: S,
        dispatcher: D,
        event_provider: B,
    ) -> Self {
        let rt = Self {
            executor: Arc::new(executor),
            clock: Arc::new(clock),
            store: Arc::new(store),
            dispatcher: Arc::new(dispatcher),
            event_provider: Arc::new(event_provider),
            pending_subflows: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            workers: Arc::new(WorkerRegistry::default()),
            lease: None,
            lease_skipped: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            cas_conflicts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            durable: None,
            approval: None,
        };
        // 启动后台扫描器，定期检查 RunSubflow 挂起的父流程并恢复
        rt.start_subflow_scanner();
        rt
    }

    /// 注入持久化能力（outbox + 持久化定时器）。
    ///
    /// builder 形态：不改变 [`Self::new`] 的构造签名，内存路径行为逐字不变。
    pub fn with_durable(mut self, durable: DurableSupport) -> Self {
        self.durable = Some(Arc::new(durable));
        self
    }

    /// 注入审批桥：approve 节点挂起前经它开启审批实例。
    ///
    /// 不注入时 approve 节点 fail-closed（见字段说明）。builder 形态：不改变
    /// [`Self::new`] 的构造签名，非 approve 定义的驱动路径逐字不变。
    pub fn with_approval_bridge(mut self, bridge: Arc<ApprovalBridge>) -> Self {
        self.approval = Some(bridge);
        self
    }

    /// 长活后台任务的存活快照（能力死亡必须可观测）。
    ///
    /// 语义见 [`WorkerLiveness`]：循环型 worker 的"已结束"、一次性 worker 的 panic
    /// 才是缺陷；一次性 worker 的正常结束**不是**。
    ///
    /// 出口归上层调用方（`coord-agent` 的指标/告警）——`coord-core` 不持有任何
    /// 遥测依赖，也不该持有。
    pub fn worker_liveness(&self) -> WorkerLiveness {
        self.workers.snapshot()
    }

    /// 注入实例驱动租约（“一实例一驱动”）。
    ///
    /// 不注入时引擎不做租约去重，行为与现状一致。builder 形式不破坏现有
    /// `new` 构造函数。
    pub fn with_instance_lease(mut self, lease: Arc<dyn InstanceLease>) -> Self {
        self.lease = Some(lease);
        self
    }

    /// 因租约被持有 / 后端不可用而跳过驱动的累计次数。
    pub fn lease_skipped_total(&self) -> u64 {
        self.lease_skipped.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// CAS 写回失败的累计次数（修订号冲突或 fencing 拒绝）。
    pub fn cas_conflict_total(&self) -> u64 {
        self.cas_conflicts.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// 启动后台子流程扫描器
    ///
    /// 启动时先做一次**全量对账**（恢复上个进程遗留的挂起实例），之后每 tick 只
    /// 检查 [`Self::pending_subflows`] 登记表 —— 稳态代价与实例总数无关。
    fn start_subflow_scanner(&self) {
        let rt = self.clone_runtime();
        let store = Arc::clone(&self.store);
        let handle = tokio::spawn(async move {
            rt.reconcile_suspended_subflows(Arc::clone(&store)).await;
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(SUBFLOW_SCAN_INTERVAL_SECS))
                    .await;
                rt.scan_pending_subflows(Arc::clone(&store)).await;
            }
        });
        // 循环型 worker：它的正常行为是**永不结束** ⇒ "已结束"本身就是缺陷。
        // 不需要捕获 panic：任务 panic 时 `is_finished()` 也变成 true，同一判据覆盖两种死法。
        self.workers
            .register_loop("workflow_subflow_scanner", handle);
    }

    // ─── 持久化定时器 / 重启恢复（Spike 2026-10 Spike 3） ───

    /// 启动持久化定时器扫描器（未注入 [`DurableSupport`] 时是空操作）。
    ///
    /// 进程内快速路径仍在 `schedule_auto_resume` / `schedule_timeout` 里；
    /// 扫描器补的是「进程内定时器随进程消失」那半 —— 重启后按 KV 到期时间重放。
    pub fn start_durable_workers(&self) {
        if self.durable.is_none() {
            return;
        }
        let rt = self.clone_runtime();
        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(DURABLE_TIMER_SCAN_INTERVAL_MS)).await;
                rt.fire_due_timers().await;
            }
        });
        self.workers
            .register_loop("workflow_durable_timer_scanner", handle);
    }

    /// 启动恢复（应在 `start()` 时调用一次）：先触发到期定时器，再对账挂起实例。
    ///
    /// 顺序很重要：到期定时器优先，避免同一实例同时被「到期触发」与「对账重订阅」
    /// 两条路径驱动。两条路径都是幂等的（`auto_resume` 有状态守卫）。
    pub async fn recover_durable_state(&self) {
        if self.durable.is_none() {
            return;
        }
        self.fire_due_timers().await;
        self.reconcile_resumable_instances(Arc::clone(&self.store))
            .await;
    }

    /// 派发前登记 step token（幂等键）。
    ///
    /// - `Ok(None)`：未注入 [`StepTokenStore`](super::durable::StepTokenStore) ⇒ 本次派发不带幂等键（旧行为）。
    /// - `Ok(Some(key))`：要透传给 dispatcher 的幂等键。
    /// - `Err`：存储不可用 ⇒ 调用方折叠为**可重试**的派发失败（由有界重试预算
    ///   吸收瞬时 KV 抖动）；错误文本带 `step token register failed: ` 前缀，避免
    ///   被误读为下游调用失败。fail-closed：没有已提交 token 就不派发。
    async fn register_step(
        &self,
        instance_id: &str,
        task_index: usize,
        task_name: &str,
    ) -> Result<Option<String>, String> {
        let Some(steps) = self.durable.as_ref().and_then(|d| d.steps.as_ref()) else {
            return Ok(None);
        };
        let sid = step_id(task_index, task_name);
        let token = step_token(instance_id, &sid);
        steps
            .register(instance_id, &sid, &token, self.clock.now_ms())
            .await
            .map(|(rec, _)| Some(rec.token))
            .map_err(|e| format!("step token register failed: {e}"))
    }

    /// 派发成功后把 step token 标 `Done`（未注入 steps 时空操作）。
    ///
    /// 标位失败不致命：token 仍为 `Pending` ⇒ 重试复用同一 token，下游按幂等键
    /// 去重即可（at-least-once）；所以吞掉错误，不把已成功的调用翻成失败。
    async fn mark_step_done(&self, instance_id: &str, task_index: usize, task_name: &str) {
        let Some(steps) = self.durable.as_ref().and_then(|d| d.steps.as_ref()) else {
            return;
        };
        let sid = step_id(task_index, task_name);
        let _ = steps.mark_done(instance_id, &sid).await;
    }

    /// 建立（或重启后重建）一个 listen 挂起实例的事件等待。
    ///
    /// 重启后原订阅随进程消失，必须重订阅 —— 否则实例永远等不到事件。
    fn spawn_event_wait(
        &self,
        instance_id: String,
        definition: WorkflowDefinition,
        store: Arc<S>,
        filter: EventFilter,
        event_timeout_ms: Option<u64>,
    ) {
        let rt = self.clone_runtime();
        // 多事件类型：event 状态多 onEvents / eventConditions 多条件
        let filter_types: Vec<String> = if !filter.event_types.is_empty() {
            filter.event_types.clone()
        } else if let Some(et) = filter.event_type.clone() {
            vec![et]
        } else {
            vec![]
        };
        let filter_source = filter.source.clone();
        let filter_subject = filter.subject.clone();
        let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let done = Arc::clone(&completed);
        let handle = tokio::spawn(async move {
            let timeout_ms = event_timeout_ms.unwrap_or(60 * 60 * 1000);
            let type_refs: Vec<&str> = filter_types.iter().map(|s| s.as_str()).collect();
            let delivery = rt
                .event_provider
                .wait_for_event(
                    &type_refs,
                    filter_source.as_deref(),
                    filter_subject.as_deref(),
                    timeout_ms,
                )
                .await;
            if let Some(delivery) = delivery {
                // 背景路径无消费方：CAS 冲突已计入 `cas_conflict_total`，其余错误（存储/校验/NotFound）在此丢弃、不计入该计数器。
                let _ = rt
                    .resume_by_event(instance_id, definition, store, delivery.event_type.clone())
                    .await;
                // 状态写成功后才提交消费位点：这一步之前崩溃只会重复投递，
                // 由 `resume_by_event` 的状态守卫（非 `Waiting`/reason 不匹配即返回）
                // 吸收；反过来（先提交位点）崩溃就是丢投递。守卫 no-op 也照样 ack ——
                // 那条事件与本实例已无关，位点仍必须推进。
                rt.event_provider.ack_event(&delivery).await;
            }
            // 走到这一行才算正常收尾；中途 panic 则不置位（由 `WorkerLiveness` 报出）。
            done.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        // 一次性 worker：正常结束（事件到达/超时）是预期行为，不记为故障。
        self.workers
            .register_oneshot("workflow_event_wait".to_string(), handle, completed);
    }

    /// 触发所有到期的持久化定时器。
    async fn fire_due_timers(&self) {
        let Some(durable) = &self.durable else {
            return;
        };
        let now = self.clock.now_ms();
        let due = match durable.timers.due(now, 64).await {
            Ok(v) => v,
            Err(_) => return,
        };
        for rec in due {
            self.fire_timer(rec).await;
        }
    }

    /// 根据一条到期定时器记录触发对应处理（状态守卫在 `auto_resume`/`apply_timeout`）。
    async fn fire_timer(&self, rec: TimerRecord) {
        let Some(durable) = &self.durable else {
            return;
        };
        let store = Arc::clone(&self.store);
        match rec.kind.clone() {
            TimerKind::AutoResume { reason } => {
                let inst = match store.load_instance(&rec.instance_id).await {
                    Ok(Some(i)) => i,
                    // 实例已删除/读不到：定时器成陈旧记录，移除
                    _ => {
                        let _ = durable.timers.remove(&rec.id).await;
                        return;
                    }
                };
                // 陈旧定时器守卫：`fire_at_ms` 必须等于**当前**挂起的 `until_ms`。
                // 不相等说明这条记录对应的是一个已被取代的挂起（重复键、人工改期、
                // 时钟回拨）。没有它，retry 会被陈旧定时器多驱动一轮 —— 重试预算
                // 提前耗尽（RECOVERY.md 记录了修复前的实测）。
                let current_until = inst.suspension_meta.as_ref().and_then(|m| m.until_ms);
                let still = inst.status == InstanceStatus::Waiting
                    && inst.suspension_meta.as_ref().map(|m| m.reason.as_str())
                        == Some(reason.as_str())
                    && current_until == Some(rec.fire_at_ms);
                if !still {
                    let _ = durable.timers.remove(&rec.id).await;
                    return;
                }
                let def = match store
                    .load_definition(
                        &inst.definition_ns,
                        &inst.definition_name,
                        &inst.definition_version,
                    )
                    .await
                {
                    Ok(Some(d)) => d,
                    // 定义暂读不到：保留定时器，下一 tick 再试
                    _ => return,
                };
                let _ = durable.timers.remove(&rec.id).await;
                // 背景路径无消费方：CAS 冲突已计入 `cas_conflict_total`，其余错误（存储/校验/NotFound）在此丢弃、不计入该计数器。
                let _ = self
                    .auto_resume(rec.instance_id.clone(), def, store, &reason)
                    .await;
            }
            TimerKind::Timeout { expected_index } => {
                let _ = durable.timers.remove(&rec.id).await;
                // 背景路径无消费方：CAS 冲突已计入 `cas_conflict_total`，其余错误（存储/校验/NotFound）在此丢弃、不计入该计数器。
                let _ = self
                    .apply_timeout(rec.instance_id.clone(), store, expected_index)
                    .await;
            }
        }
    }

    /// 对账所有非终态可恢复实例：重启后重新驱动/重订阅。
    ///
    /// - `Pending` / `Running`：上个进程的 drive 任务随进程消失（进程内任务无跨进程存活，
    ///   `drivers/` 租约键也无主）⇒ 重驱动一个。**未注入租约时**同进程重叠驱动是可容忍的：
    ///   每次 drive 都重读最新实例状态，CAS 失败的
    ///   提交直接退出驱动循环 —— 不会丢更新，只是白跑一轮。注入租约后（经 [`Self::spawn_drive`]
    ///   先取租约）由租约提供互斥：多进程并发时只有一个真正驱动（其余计入 `lease_skipped`）。
    /// - `Waiting`：`listen` 重建事件订阅；`wait` / `retry` 补写持久化定时器（已到期立即触发）。
    /// - `Suspended`（signal / approve 挂起）：人工/审批恢复语义 —— 没有外部 signal 或审批
    ///   结论就自动重驱动会把人工审批直接跑过去，故**不**自动驱动（ADR 的「所有非终态」在此
    ///   显式排除挂起）。
    /// - 终态：跳过。
    ///
    /// `coord-core` 无 `tracing`，挂起实例的跳过是设计内非动作（不是故障），故只以注释
    /// 记录原因、不新增日志/计数出口（观测出口在上层 `WorkerLiveness` 等）。
    async fn reconcile_resumable_instances(&self, store: Arc<S>) {
        let Ok((instances, _next)) = store.list_instances(None, None, usize::MAX, None).await
        else {
            return;
        };
        let Some(durable) = &self.durable else {
            return;
        };
        let now = self.clock.now_ms();
        for inst in instances {
            match inst.status {
                InstanceStatus::Pending | InstanceStatus::Running => {
                    let def = match store
                        .load_definition(
                            &inst.definition_ns,
                            &inst.definition_name,
                            &inst.definition_version,
                        )
                        .await
                    {
                        Ok(Some(d)) => d,
                        // 定义暂读不到：留待下次启动对账（不在此处造状态）。
                        _ => continue,
                    };
                    self.spawn_drive(inst.id.clone(), def, Arc::clone(&store));
                }
                InstanceStatus::Waiting => {
                    let Some(meta) = inst.suspension_meta.clone() else {
                        continue;
                    };
                    match meta.reason.as_str() {
                        "listen" => {
                            let def = match store
                                .load_definition(
                                    &inst.definition_ns,
                                    &inst.definition_name,
                                    &inst.definition_version,
                                )
                                .await
                            {
                                Ok(Some(d)) => d,
                                _ => continue,
                            };
                            let filter = meta.event_filter.clone().unwrap_or(EventFilter {
                                event_type: None,
                                event_types: vec![],
                                source: None,
                                subject: None,
                            });
                            self.spawn_event_wait(
                                inst.id.clone(),
                                def,
                                Arc::clone(&store),
                                filter,
                                None,
                            );
                        }
                        "wait" | "retry" => {
                            let due = meta.until_ms.unwrap_or(now);
                            let rec = TimerRecord::auto_resume(&inst.id, &meta.reason, due, now);
                            let _ = durable.timers.schedule(&rec).await;
                        }
                        _ => {}
                    }
                }
                // 终态跳过；`Suspended`（signal / approve 挂起）是人工恢复语义，无外部
                // signal 或审批结论就不自动驱动（见函数文档，ADR 的「所有非终态」在此显式
                // 排除挂起）。
                _ => {}
            }
        }
        // 补写的定时器里可能已经到期（崩溃窗口 / 恢复耗时），统一再触发一轮。
        self.fire_due_timers().await;
    }

    /// spawn 一个**一次性** drive 任务，并把它"是否正常收尾"变成可拉取的事实。
    ///
    /// 为什么不用 `is_finished()` 直接判定故障：drive 的**正常**结束（实例挂起 / 终态）
    /// 是预期行为，只有"结束了但没跑到最后一行"才是缺陷。所以在任务最后一行置一个
    /// `completed` 标志，由 [`WorkerLiveness`] 在结算时区分两者。
    ///
    /// 不用 `catch_unwind`：那需要 `futures::FutureExt`，而 `coord-core` 的依赖面要过
    /// `deny.toml`（为一个可观测性需求引入新依赖面不划算）；标志位是零依赖的等价判据。
    ///
    /// `instance_id` 进标签，是为了让出口能回答"**哪个**实例的驱动死了"而不只是报总数。
    fn spawn_drive(&self, instance_id: String, definition: WorkflowDefinition, store: Arc<S>) {
        let rt = self.clone_runtime();
        let label = format!("workflow_drive[{instance_id}]");
        let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = Arc::clone(&completed);
        let handle = tokio::spawn(async move {
            // 实例租约：一实例一驱动（未注入租约时跳过此段，退化为现状）。
            // 获取失败（他人持有 / 后端不可用）→ 跳过驱动，**不得**退化为无租约驱动。
            let guard: Option<Arc<dyn LeaseGuard>> = match rt.lease.as_ref() {
                Some(lease) => match lease.acquire(&instance_id).await {
                    Ok(Some(g)) => Some(g),
                    // 已被其他驱动者持有：跳过（正常收尾，不计为故障）。
                    Ok(None) => {
                        rt.lease_skipped
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        flag.store(true, std::sync::atomic::Ordering::SeqCst);
                        return;
                    }
                    // 后端不可用：fail-closed 跳过，并留下可观测信号。
                    Err(_) => {
                        rt.lease_skipped
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        flag.store(true, std::sync::atomic::Ordering::SeqCst);
                        return;
                    }
                },
                None => None,
            };
            // 租约有效期内的后台续租（drive 期间）；租约丢失时停止。
            let renewer = guard.as_ref().map(|g| {
                let g = Arc::clone(g);
                let interval = g.renew_interval();
                tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(interval).await;
                        match g.renew().await {
                            Ok(true) => {}
                            _ => break,
                        }
                    }
                })
            });
            rt.drive(instance_id, definition, store, guard.as_deref())
                .await;
            if let Some(h) = renewer {
                h.abort();
            }
            if let Some(g) = guard {
                g.release().await;
            }
            // ⚠️ 这一行是判据的一部分：`drive` 中途死亡时它不会被执行，
            // 结算时该 worker 即被判为"未正常收尾"。不要把它移出 async 块。
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        self.workers.register_oneshot(label, handle, completed);
    }

    /// 登记一个待恢复的父子对（父因 `RunSubflow` 挂起时调用）。
    fn register_pending_subflow(&self, parent_id: String, subflow_id: String) {
        if let Ok(mut map) = self.pending_subflows.lock() {
            map.insert(parent_id, subflow_id);
        }
    }

    /// 注销一个父子对（已恢复或已不再挂起）。
    fn unregister_pending_subflow(&self, parent_id: &str) {
        if let Ok(mut map) = self.pending_subflows.lock() {
            map.remove(parent_id);
        }
    }

    /// 启动期全量对账：列出所有挂起实例，登记其中的 `RunSubflow` 父子对，
    /// 然后立刻检查一轮。**只在启动时跑一次**（稳态不再全量列举）。
    async fn reconcile_suspended_subflows(&self, store: Arc<S>) {
        let instances = match store.list_instances(None, None, usize::MAX, None).await {
            Ok((list, _next)) => list,
            Err(_) => return,
        };
        for inst in instances {
            if !matches!(
                inst.status,
                InstanceStatus::Suspended | InstanceStatus::Waiting
            ) {
                continue;
            }
            if let Some(subflow_id) = inst
                .context
                .get("_subflow_instance_id")
                .and_then(|v| v.as_str())
            {
                self.register_pending_subflow(inst.id.clone(), subflow_id.to_string());
            }
        }
        self.scan_pending_subflows(store).await;
    }

    /// 检查登记表里的父子对：子流程已完成 ⇒ 恢复父流程并注销。
    ///
    /// 登记表为空时是 O(1)（不再每 5 秒克隆全部实例）。
    async fn scan_pending_subflows(&self, store: Arc<S>) {
        // 快照登记表（不持锁跨 await）。
        let pairs: Vec<(String, String)> = match self.pending_subflows.lock() {
            Ok(map) => map.iter().map(|(p, c)| (p.clone(), c.clone())).collect(),
            Err(_) => return,
        };
        if pairs.is_empty() {
            return;
        }
        for (parent_id, subflow_id) in pairs {
            // 子流程是否完成
            let sub_inst = match store.load_instance(&subflow_id).await {
                Ok(Some(si)) => si,
                // 读失败/不存在：保留登记，下一 tick 再看
                _ => continue,
            };
            if !sub_inst.status.is_terminal() {
                continue;
            }

            // 父实例是否仍挂在这条子流程上（已恢复/已取消 ⇒ 注销）
            let parent = match store.load_instance(&parent_id).await {
                Ok(Some(p)) => p,
                _ => {
                    self.unregister_pending_subflow(&parent_id);
                    continue;
                }
            };
            let still_waiting = matches!(
                parent.status,
                InstanceStatus::Suspended | InstanceStatus::Waiting
            ) && parent
                .context
                .get("_subflow_instance_id")
                .and_then(|v| v.as_str())
                == Some(subflow_id.as_str());
            if !still_waiting {
                self.unregister_pending_subflow(&parent_id);
                continue;
            }

            if self
                .resume_parent_after_subflow(&parent_id, &sub_inst, &store)
                .await
            {
                self.unregister_pending_subflow(&parent_id);
            }
        }
    }

    /// 子流程已终结：把父流程从挂起恢复并重新驱动。
    ///
    /// 返回 `true` 表示本次已处理（可从登记表注销）；`false` 表示持久化失败，
    /// 应保留登记、下一 tick 重试。
    async fn resume_parent_after_subflow(
        &self,
        parent_id: &str,
        sub_inst: &WorkflowInstance,
        store: &Arc<S>,
    ) -> bool {
        let signal_payload = match sub_inst.status {
            InstanceStatus::Completed => sub_inst.output.clone().unwrap_or(Value::Null),
            InstanceStatus::Failed => {
                serde_json::json!({
                    "_subflow_error": sub_inst.fault.clone().map(|f| f.title).unwrap_or_default(),
                })
            }
            _ => Value::Null,
        };
        let subflow_id = sub_inst.id.clone();

        let outcome = match self
            .commit_control_plane(
                store,
                parent_id,
                &[lifecycle::TASK_COMPLETED, lifecycle::WORKFLOW_RESUMED],
                |parent| {
                    // 仅当父流程仍挂在这条子流程上才恢复（否则已被他人恢复/取消）。
                    let still_waiting = matches!(
                        parent.status,
                        InstanceStatus::Suspended | InstanceStatus::Waiting
                    ) && parent
                        .context
                        .get("_subflow_instance_id")
                        .and_then(|v| v.as_str())
                        == Some(subflow_id.as_str());
                    if !still_waiting {
                        return Ok(false);
                    }
                    // 标记 run 任务帧为已完成并推进索引
                    if let Some(last_frame) = parent.task_stack.last_mut() {
                        last_frame.status = TaskStatus::Completed;
                        last_frame.output = Some(signal_payload.clone());
                        last_frame.ended_at = Some(self.clock.now_ms());
                    }
                    parent.current_task_index += 1;
                    parent.status = InstanceStatus::Running;
                    parent.suspension_meta = None;
                    parent.context["_signal"] = serde_json::json!({
                        "name": "_subflow_completed",
                        "payload": signal_payload.clone(),
                    });
                    parent.updated_at = self.clock.now_ms();
                    Ok(true)
                },
            )
            .await
        {
            Ok(o) => o,
            // CAS 冲突耗尽 / 存储错误：保留登记，下一 tick 重试。
            Err(_) => return false,
        };

        let parent = match outcome {
            ControlPlaneOutcome::Committed(parent) => *parent,
            // 守卫未满足：父流程已不再挂在这条子流程上 —— 视为已处理（可注销登记）。
            ControlPlaneOutcome::Skipped => return true,
        };

        // 加载父流程定义并重新驱动
        let parent_def = match store
            .load_definition(
                &parent.definition_ns,
                &parent.definition_name,
                &parent.definition_version,
            )
            .await
        {
            Ok(Some(def)) => def,
            _ => {
                // 定义暂时读不到：保留登记，下一 tick 重试（与旧实现的 `continue` 同向，
                // 但不再依赖全量扫描）
                return false;
            }
        };

        // 复用受监督的一次性 drive（见 [`Self::spawn_drive`]）：panic 会被记成可拉取的
        // 事实，而正常结束（该父实例再次挂起/终态）**不计**为故障。
        self.spawn_drive(parent.id.clone(), parent_def, Arc::clone(store));
        true
    }

    /// 启动工作流实例
    ///
    /// 1. 应用工作流 `input.default` / `input.from` / `input.schema`（标准 Data Flow）
    ///    —— 校验失败 → 实例直接 faulted（validation 错误）
    /// 2. 创建实例（标准相位 `Pending`）
    /// 3. 持久化到存储
    /// 4. 异步驱动执行（drive 首步推进为 `Running`）
    pub async fn start(
        &self,
        definition: &WorkflowDefinition,
        input: Value,
    ) -> Result<WorkflowInstance, RuntimeError> {
        let now_ms = self.clock.now_ms();

        // 工作流级输入变换/校验
        let input_val = match self.apply_workflow_input(definition, input) {
            Ok(v) => v,
            Err(fault) => {
                // 输入校验失败 → faulted 实例（RFC 7807 validation 错误）
                let mut inst = WorkflowInstance::new(definition, Value::Null, now_ms);
                inst.status = InstanceStatus::Failed;
                inst.fault = Some(fault);
                inst.updated_at = now_ms;
                self.store
                    .save_instance(&inst)
                    .await
                    .map_err(|e| RuntimeError::StoreError(e.to_string()))?;
                return Ok(inst);
            }
        };

        let inst = WorkflowInstance::new(definition, input_val, now_ms);

        self.store
            .save_instance(&inst)
            .await
            .map_err(|e| RuntimeError::StoreError(e.to_string()))?;

        // 后台 drive（受监督：见 [`Self::spawn_drive`]）
        self.spawn_drive(inst.id.clone(), definition.clone(), Arc::clone(&self.store));

        // 工作流级超时接线（标准 Fault Tolerance）：超时 → timeout 错误（408）→ faulted
        if let Some(t) = &definition.timeout {
            if let Some(ms) = crate::workflow::engine::parse_iso8601_duration_ms(&t.after) {
                self.schedule_timeout(
                    inst.id.clone(),
                    definition.clone(),
                    Arc::clone(&self.store),
                    ms.max(0) as u64,
                    None,
                );
            }
        }

        Ok(inst)
    }

    /// 应用工作流级输入配置：`default` → `from` 变换 → `schema` 校验
    fn apply_workflow_input(
        &self,
        definition: &WorkflowDefinition,
        input: Value,
    ) -> Result<Value, WorkflowFault> {
        let cfg = match &definition.input {
            Some(c) => c,
            None => return Ok(input),
        };

        // default：输入缺失/为空时使用默认值
        let mut value = input;
        let empty = value.is_null() || value.as_object().map(|o| o.is_empty()).unwrap_or(false);
        if empty {
            if let Some(default) = &cfg.default {
                value = default.clone();
            }
        }

        // from：原始输入 → 变换后初始 context
        if let Some(from) = &cfg.from {
            let mut vars = super::engine::build_expression_vars(
                &WorkflowInstance::new(definition, value.clone(), self.clock.now_ms()),
                definition,
            );
            vars.insert("input".to_string(), value.clone());
            match self.executor.expr.evaluate_with_vars(from, &value, &vars) {
                Ok(v) => value = v,
                Err(e) => {
                    return Err(crate::workflow::errors::WorkflowFault::expression(
                        "workflow input.from evaluation failed",
                        e.to_string(),
                    ))
                }
            }
        }

        // schema：输入校验（失败 → faulted，validation 错误）
        if let Some(schema) = &cfg.schema {
            if let Err(errs) = crate::workflow::jsonschema::validate(schema, &value) {
                return Err(crate::workflow::errors::WorkflowFault::validation(
                    "workflow input failed schema validation",
                    errs.join("; "),
                )
                .with_instance("/input"));
            }
        }

        Ok(value)
    }

    /// 任务完成后的输出/导出管线（标准 Data Flow，对 Completed 帧调用）
    ///
    /// 1. `output.as` 变换帧输出 + `output.schema` 校验
    /// 2. `export.as` 变换结果替换 context + `export.schema` 校验
    ///
    /// 返回 Err(fault) 时调用方应将实例置为 faulted。
    fn apply_task_output(
        &self,
        inst: &mut WorkflowInstance,
        definition: &WorkflowDefinition,
        frame: &mut TaskFrame,
    ) -> Result<(), WorkflowFault> {
        let meta = match definition.task_meta.get(&frame.task_name) {
            Some(m) => m,
            None => return Ok(()),
        };
        if meta.output.is_none() && meta.export.is_none() {
            return Ok(());
        }

        // 变量绑定：$input / $output / $task / $context
        let mut vars = super::engine::build_expression_vars(inst, definition);
        vars.insert(
            "input".to_string(),
            frame.input.clone().unwrap_or(Value::Null),
        );
        vars.insert(
            "output".to_string(),
            frame.output.clone().unwrap_or(Value::Null),
        );
        vars.insert(
            "task".to_string(),
            serde_json::json!({"name": frame.task_name, "type": frame.task_type}),
        );

        // output.as / output.schema
        if let Some(out) = &meta.output {
            if let Some(as_expr) = &out.as_expr {
                let raw_output = frame.output.clone().unwrap_or(Value::Null);
                vars.insert("output".to_string(), raw_output);
                match self
                    .executor
                    .expr
                    .evaluate_with_vars(as_expr, &inst.context, &vars)
                {
                    Ok(v) => frame.output = Some(v),
                    Err(e) => {
                        return Err(crate::workflow::errors::WorkflowFault::expression(
                            format!("task '{}' output.as evaluation failed", frame.task_name),
                            e.to_string(),
                        ))
                    }
                }
            }
            if let Some(schema) = &out.schema {
                let val = frame.output.as_ref().unwrap_or(&Value::Null);
                if let Err(errs) = crate::workflow::jsonschema::validate(schema, val) {
                    return Err(crate::workflow::errors::WorkflowFault::validation(
                        format!("task '{}' output failed schema validation", frame.task_name),
                        errs.join("; "),
                    )
                    .with_instance(format!("/tasks/{}/output", frame.task_name)));
                }
            }
        }

        // export.as / export.schema —— 结果替换 context
        if let Some(exp) = &meta.export {
            if let Some(as_expr) = &exp.as_expr {
                vars.insert(
                    "output".to_string(),
                    frame.output.clone().unwrap_or(Value::Null),
                );
                match self
                    .executor
                    .expr
                    .evaluate_with_vars(as_expr, &inst.context, &vars)
                {
                    Ok(v) => inst.context = v,
                    Err(e) => {
                        return Err(crate::workflow::errors::WorkflowFault::expression(
                            format!("task '{}' export.as evaluation failed", frame.task_name),
                            e.to_string(),
                        ))
                    }
                }
            }
            if let Some(schema) = &exp.schema {
                if let Err(errs) = crate::workflow::jsonschema::validate(schema, &inst.context) {
                    return Err(crate::workflow::errors::WorkflowFault::validation(
                        format!(
                            "task '{}' exported context failed schema validation",
                            frame.task_name
                        ),
                        errs.join("; "),
                    )
                    .with_instance(format!("/tasks/{}/export", frame.task_name)));
                }
            }
        }

        Ok(())
    }

    /// 应用工作流级 `output.as` / `output.schema`（Completed 时调用）
    fn apply_workflow_output(
        &self,
        definition: &WorkflowDefinition,
        inst: &WorkflowInstance,
        raw_output: Value,
    ) -> Result<Value, WorkflowFault> {
        let out = match &definition.output {
            Some(o) => o,
            None => return Ok(raw_output),
        };

        let mut value = raw_output;
        if let Some(as_expr) = &out.as_expr {
            let mut vars = super::engine::build_expression_vars(inst, definition);
            vars.insert("output".to_string(), value.clone());
            vars.insert("context".to_string(), inst.context.clone());
            match self
                .executor
                .expr
                .evaluate_with_vars(as_expr, &inst.context, &vars)
            {
                Ok(v) => value = v,
                Err(e) => {
                    return Err(crate::workflow::errors::WorkflowFault::expression(
                        "workflow output.as evaluation failed",
                        e.to_string(),
                    ))
                }
            }
        }
        if let Some(schema) = &out.schema {
            if let Err(errs) = crate::workflow::jsonschema::validate(schema, &value) {
                return Err(crate::workflow::errors::WorkflowFault::validation(
                    "workflow output failed schema validation",
                    errs.join("; "),
                )
                .with_instance("/output"));
            }
        }
        Ok(value)
    }

    /// 发布生命周期 CloudEvent（标准 Lifecycle Events）。
    ///
    /// 仅有[`Self::commit_instance`]的 legacy 降级分支（无 outbox / 无同事务能力）
    /// 复用它：注入持久化 outbox 时只 append（**不**写实例状态），未注入时直发进程内
    /// `event_provider`。生产路径的生命周期事件与状态在同一提交里落——见
    /// [`Self::commit_instance`]。
    ///
    /// 发布出口只有一个：注入 outbox 时**不**再直发进程内 `event_provider`
    /// （否则同一事件会有两条 ID 不同的副本，下游按 `event_id` 去重失效）。
    async fn emit_lifecycle(&self, event_type: &str, inst: &WorkflowInstance) {
        let data = lifecycle_data(inst);
        if let Some(durable) = &self.durable {
            let record = lifecycle_record(
                inst,
                event_type,
                self.clock.now_ms(),
                format!("evt-{}", uuid::Uuid::new_v4()),
            );
            // 只 append，**不**写实例状态：emit 若另行写实例会绕过调用方的
            // CAS/fencing，并推进 mod_revision 使随后的 CAS 失配。
            let _ = durable.outbox.append(&record).await;
            // 注入 outbox 后**不再**直发进程内事件总线：发布职责归 relay 一个出口，
            // 否则同一生命周期事件会出现两条 ID 不同的副本（去重失效）。
        } else {
            self.event_provider
                .emit(event_type, Some("coord/workflow"), &data)
                .await;
        }
    }

    /// 调度自动恢复：`due_ms` 到期后按 reason 恢复挂起实例
    ///
    /// - reason="wait"：完成当前 wait 帧并推进到下一任务
    /// - reason="retry"：重新执行当前任务（重试）
    ///
    /// `due_ms` 必须是**写进 `suspension_meta.until_ms` 的同一个值**。用
    /// 「当前时间 + delay」重新推导会让持久化定时器的主键（含到期时间）与
    /// 挂起元信息里的到期时间错开几毫秒 —— 重启对账就会再写一个**同语义不同键**
    /// 的定时器，同一挂起被触发两次（retry 会因此多跑一轮，见 RECOVERY.md）。
    fn schedule_auto_resume(
        &self,
        instance_id: String,
        definition: WorkflowDefinition,
        store: Arc<S>,
        due_ms: i64,
        reason: String,
    ) {
        let now_ms = self.clock.now_ms();
        let delay_ms = (due_ms - now_ms).max(0) as u64;
        if let Some(durable) = &self.durable {
            // 先落 KV：进程内定时器随进程消失，持久化定时器才是重启后能补上的那半。
            let timers = Arc::clone(&durable.timers);
            let rec = TimerRecord::auto_resume(&instance_id, &reason, due_ms, now_ms);
            tokio::spawn(async move {
                let _ = timers.schedule(&rec).await;
            });
        }
        let rt = self.clone_runtime();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            // 背景路径无消费方：CAS 冲突已计入 `cas_conflict_total`，其余错误（存储/校验/NotFound）在此丢弃、不计入该计数器。
            let _ = rt
                .auto_resume(instance_id, definition, store, &reason)
                .await;
        });
    }

    /// 自动恢复处理（wait 完成推进 / retry 重新执行）
    async fn auto_resume(
        &self,
        instance_id: String,
        definition: WorkflowDefinition,
        store: Arc<S>,
        reason: &str,
    ) -> Result<(), RuntimeError> {
        // 定义漂移守卫：自动恢复不得在新语义下推进/重试（fail-closed）。
        self.fail_closed_on_definition_drift(&store, &instance_id, &definition)
            .await?;

        if reason == "retry" {
            // 重试：直接重新 drive（重新执行当前任务），本路径**不写实例**；
            // 保留「仅当仍处于同一挂起原因才驱动」的守卫（陈旧定时器不得多驱动一轮）。
            //
            // 守卫必须走**线性一致读**（`load_instance_with_rev`）：durable 写路径
            // 绕过本地读缓存，若 Watch 滞后，cache-first 的 `load_instance` 会把已推进/
            // 已取消的实例读成陈旧快照，本次重试于是被错误地放弃——定时器已在触发前
            // 移除，实例就永久停在 Waiting 而没有任何驱动者。
            let still = {
                let is_still = |inst: &WorkflowInstance| {
                    inst.status == InstanceStatus::Waiting
                        && inst.suspension_meta.as_ref().map(|m| m.reason.as_str()) == Some(reason)
                };
                match store.load_instance_with_rev(&instance_id).await {
                    Ok(Some((inst, _))) => is_still(&inst),
                    // 存储不提供修订号：回退普通读（与 [`Self::commit_control_plane`] 同口径）。
                    Ok(None) => match store.load_instance(&instance_id).await {
                        Ok(Some(inst)) => is_still(&inst),
                        _ => false,
                    },
                    // 线性一致读失败：不驱动（fail-closed，不回退到可能陈旧的缓存）。
                    Err(_) => false,
                }
            };
            if still {
                self.spawn_drive(instance_id, definition, store);
            }
            return Ok(());
        }

        let outcome = self
            .commit_control_plane(
                &store,
                &instance_id,
                &[lifecycle::TASK_COMPLETED, lifecycle::WORKFLOW_RESUMED],
                |inst| {
                    // 仅当仍处于同一挂起原因才恢复（避免竞态覆盖已完成的实例）
                    if inst.status != InstanceStatus::Waiting {
                        return Ok(false);
                    }
                    let Some(meta) = inst.suspension_meta.as_ref() else {
                        return Ok(false);
                    };
                    if meta.reason != reason {
                        return Ok(false);
                    }
                    // 完成 wait 帧并推进
                    if let Some(last) = inst.task_stack.last_mut() {
                        last.status = TaskStatus::Completed;
                        last.ended_at = Some(self.clock.now_ms());
                    }
                    inst.current_task_index += 1;
                    inst.status = InstanceStatus::Running;
                    inst.suspension_meta = None;
                    inst.updated_at = self.clock.now_ms();
                    Ok(true)
                },
            )
            .await?;

        if matches!(outcome, ControlPlaneOutcome::Committed(_)) {
            self.spawn_drive(instance_id, definition, store);
        }
        Ok(())
    }

    /// 事件驱动的自动恢复（标准 Events：listen 主动订阅，事件到达 → 恢复）
    ///
    /// `arrived_event_type`：实际到达的事件类型（多类型监听时用于路由）
    async fn resume_by_event(
        &self,
        instance_id: String,
        definition: WorkflowDefinition,
        store: Arc<S>,
        arrived_event_type: String,
    ) -> Result<(), RuntimeError> {
        // 定义漂移守卫：事件恢复不得在新语义下推进（fail-closed）。
        self.fail_closed_on_definition_drift(&store, &instance_id, &definition)
            .await?;

        let outcome = self
            .commit_control_plane(
                &store,
                &instance_id,
                &[lifecycle::TASK_COMPLETED, lifecycle::WORKFLOW_RESUMED],
                |inst| {
                    if inst.status != InstanceStatus::Waiting {
                        return Ok(false);
                    }
                    let Some(meta) = inst.suspension_meta.as_ref() else {
                        return Ok(false);
                    };
                    if meta.reason != "listen" {
                        return Ok(false);
                    }
                    // 事件到达：完成 listen 帧、推进、注入事件上下文
                    if let Some(last) = inst.task_stack.last_mut() {
                        last.status = TaskStatus::Completed;
                        last.ended_at = Some(self.clock.now_ms());
                    }
                    inst.current_task_index += 1;
                    inst.context["_event"] = serde_json::json!({
                        "arrived": true,
                        "eventType": arrived_event_type,
                    });
                    inst.status = InstanceStatus::Running;
                    inst.suspension_meta = None;
                    inst.updated_at = self.clock.now_ms();
                    Ok(true)
                },
            )
            .await?;

        // 后台 drive（与子流程扫描器同模式，避免在事件等待任务内嵌套 drive；
        // 受监督：见 [`Self::spawn_drive`]）
        if matches!(outcome, ControlPlaneOutcome::Committed(_)) {
            self.spawn_drive(instance_id, definition, store);
        }
        Ok(())
    }

    /// 调度超时：after_ms 后若实例仍非终端 → faulted（timeout 错误 408）
    ///
    /// `expected_index`: None = 工作流级超时（任意非终端即 fault）；
    /// Some(idx) = 任务级超时（仅当实例仍停留该任务索引才 fault）。
    fn schedule_timeout(
        &self,
        instance_id: String,
        _definition: WorkflowDefinition,
        store: Arc<S>,
        after_ms: u64,
        expected_index: Option<usize>,
    ) {
        if let Some(durable) = &self.durable {
            let timers = Arc::clone(&durable.timers);
            let now_ms = self.clock.now_ms();
            let rec = TimerRecord::timeout(
                &instance_id,
                expected_index,
                now_ms + after_ms as i64,
                now_ms,
            );
            tokio::spawn(async move {
                let _ = timers.schedule(&rec).await;
            });
        }
        let rt = self.clone_runtime();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(after_ms)).await;
            // 背景路径无消费方：CAS 冲突已计入 `cas_conflict_total`，其余错误（存储/校验/NotFound）在此丢弃、不计入该计数器。
            let _ = rt.apply_timeout(instance_id, store, expected_index).await;
        });
    }

    /// 超时到期处理
    async fn apply_timeout(
        &self,
        instance_id: String,
        store: Arc<S>,
        expected_index: Option<usize>,
    ) -> Result<(), RuntimeError> {
        self.commit_control_plane(
            &store,
            &instance_id,
            &[lifecycle::WORKFLOW_FAULTED],
            |inst| {
                if inst.status.is_terminal() {
                    return Ok(false);
                }
                // 任务级超时：仅当实例仍停留同一任务索引才 fault
                if let Some(idx) = expected_index {
                    if inst.current_task_index != idx {
                        return Ok(false);
                    }
                }
                let fault = crate::workflow::errors::WorkflowFault::timeout(
                    "workflow timeout",
                    "the workflow exceeded its configured timeout",
                );
                inst.status = InstanceStatus::Failed;
                inst.fault = Some(fault);
                inst.updated_at = self.clock.now_ms();
                Ok(true)
            },
        )
        .await
        .map(|_| ())
    }

    /// 将实例置为 faulted（Failed + fault + 生命周期事件）。
    ///
    /// 仅在 [`Self::drive`] 内调用：复用 drive 当轮的 `rev`/`guard`，与状态 + 事件
    /// 走同一次 CAS/fencing 提交（不另起无 CAS 的裸写）。
    ///
    /// 有界重试：首次提交失败常是并发控制面已推进修订号（CAS 冲突）。失败后以
    /// `load_instance_with_rev` 重取最新修订号并重放，至多 [`CONTROL_PLANE_CAS_ATTEMPTS`]
    /// 次 —— 不重试会让一次冲突静默丢掉终态 fault 写、把实例留在 Running。
    /// 耗尽仍未提交：实例保持非终态，由启动对账重新驱动；失败已计入
    /// `cas_conflict_total`，drop 非静默。
    async fn fail_instance(
        &self,
        inst: &mut WorkflowInstance,
        store: &Arc<S>,
        rev: &mut Option<i64>,
        guard: Option<&dyn LeaseGuard>,
        fault: WorkflowFault,
    ) {
        for _ in 0..CONTROL_PLANE_CAS_ATTEMPTS {
            inst.status = InstanceStatus::Failed;
            inst.fault = Some(fault.clone());
            inst.updated_at = self.clock.now_ms();
            if self
                .commit_instance(store, inst, rev, guard, &[lifecycle::WORKFLOW_FAULTED])
                .await
            {
                return;
            }
            // 重取最新修订号：下一次 CAS 以新值为期望。
            match store.load_instance_with_rev(&inst.id).await {
                // 只在实例仍非终态时重放：并发控制面已提交终态则不覆盖。
                Ok(Some((latest, r))) if !latest.status.is_terminal() => {
                    *inst = latest;
                    *rev = Some(r);
                }
                // 实例已终态 / 读不到 / 存储无修订号：重放无意义，停止。
                _ => break,
            }
        }
    }

    /// 定义漂移 fail-closed（控制面）：置实例 Failed + 伴生 `workflow.faulted`（有界 CAS），
    /// 并把漂移报成 [`RuntimeError::DefinitionDrift`]。
    ///
    /// 控制面没有 drive 当轮的 `rev`/`guard`，故复用 [`Self::commit_control_plane`] 的
    /// 「线性一致读修订号 + 冲突重放」；fault 提交耗尽也不掩盖返回的漂移错误
    /// （失败已计入 `cas_conflict_total`，实例保持非终态，由启动对账重新驱动）。
    async fn definition_drift_error(
        &self,
        store: &Arc<S>,
        instance_id: &str,
        expected: String,
        actual: String,
    ) -> RuntimeError {
        let fault = definition_drift_fault(&expected, &actual);
        let _ = self
            .commit_control_plane(store, instance_id, &[lifecycle::WORKFLOW_FAULTED], |inst| {
                if inst.status.is_terminal() {
                    return Ok(false);
                }
                inst.status = InstanceStatus::Failed;
                inst.fault = Some(fault.clone());
                inst.updated_at = self.clock.now_ms();
                Ok(true)
            })
            .await;
        RuntimeError::DefinitionDrift {
            instance_id: instance_id.to_string(),
            expected,
            actual,
        }
    }

    /// 控制面入口的定义钉死校验：读回实例 → [`assert_definition_pin`] → 漂移时置 Failed
    /// 并返回 [`RuntimeError::DefinitionDrift`]。
    ///
    /// 实例不存在时放行：后续路径会按 NotFound / 状态守卫处理，守卫不在此造状态。
    /// 读失败是存储错误，不得回退成「实例不存在」——那会让守卫静默失效。
    async fn fail_closed_on_definition_drift(
        &self,
        store: &Arc<S>,
        instance_id: &str,
        definition: &WorkflowDefinition,
    ) -> Result<(), RuntimeError> {
        let inst = match store.load_instance(instance_id).await {
            Ok(Some(inst)) => inst,
            Ok(None) => return Ok(()),
            Err(e) => return Err(RuntimeError::StoreError(e.to_string())),
        };
        match assert_definition_pin(&inst, definition) {
            Ok(()) => Ok(()),
            Err(RuntimeError::DefinitionDrift {
                expected, actual, ..
            }) => Err(self
                .definition_drift_error(store, instance_id, expected, actual)
                .await),
            Err(other) => Err(other),
        }
    }

    /// 恢复挂起的实例
    ///
    /// 1. 加载实例，校验状态为 Suspended
    /// 2. 注入 signal 数据到 context
    /// 3. 重置为 Running
    /// 4. 异步驱动继续执行
    ///
    /// `idempotency_key`: 可选幂等键，防止网络重试导致双重恢复。
    /// 相同 key 的重复调用将直接返回当前实例状态而不重新驱动。
    pub async fn resume(
        &self,
        instance_id: &str,
        signal_name: Option<&str>,
        payload: Option<Value>,
        idempotency_key: Option<&str>,
    ) -> Result<WorkflowInstance, RuntimeError> {
        // 幂等性检查
        if let Some(key) = idempotency_key {
            let is_new = self
                .store
                .save_resume_idempotency_key(instance_id, key)
                .await
                .map_err(|e| RuntimeError::StoreError(e.to_string()))?;
            if !is_new {
                // key 已消费，直接返回当前实例状态
                return self
                    .store
                    .load_instance(instance_id)
                    .await
                    .map_err(|e| RuntimeError::StoreError(e.to_string()))?
                    .ok_or_else(|| RuntimeError::NotFound(instance_id.to_string()));
            }
        }
        // 定义漂移守卫必须在**任何状态写之前**：先读实例 + 加载定义 + 校验钉死哈希。
        // 先提交 Waiting→Running 再发现漂移会留下一次无效果的状态转换与 `workflow.resumed`。
        let pre = self
            .store
            .load_instance(instance_id)
            .await
            .map_err(|e| RuntimeError::StoreError(e.to_string()))?
            .ok_or_else(|| RuntimeError::NotFound(instance_id.to_string()))?;
        let def = self
            .store
            .load_definition(
                &pre.definition_ns,
                &pre.definition_name,
                &pre.definition_version,
            )
            .await
            .map_err(|e| RuntimeError::StoreError(e.to_string()))?
            .ok_or_else(|| {
                RuntimeError::DefinitionNotFound(format!(
                    "{}/{}@{}",
                    pre.definition_ns, pre.definition_name, pre.definition_version
                ))
            })?;
        if let Err(drift) = assert_definition_pin(&pre, &def) {
            let (expected, actual) = match drift {
                RuntimeError::DefinitionDrift {
                    expected, actual, ..
                } => (expected, actual),
                other => return Err(other),
            };
            return Err(self
                .definition_drift_error(&self.store, instance_id, expected, actual)
                .await);
        }

        let outcome = self
            .commit_control_plane(
                &self.store,
                instance_id,
                &[lifecycle::TASK_COMPLETED, lifecycle::WORKFLOW_RESUMED],
                |inst| {
                    if !inst.status.is_resumable() {
                        return Err(RuntimeError::AlreadyCompleted(instance_id.to_string()));
                    }

                    // signal/事件校验（标准 3.4 节）：signal 名必须匹配挂起实例的期望
                    if let Some(meta) = inst.suspension_meta.as_ref() {
                        // 人工审批挂起（expected_signal）：必须精确匹配
                        if let Some(exp) = meta.expected_signal.as_deref() {
                            let got = signal_name.unwrap_or("");
                            if got != exp {
                                return Err(RuntimeError::InvalidSignal(format!(
                                    "instance '{}' is waiting for signal '{exp}', got '{got}'",
                                    instance_id
                                )));
                            }
                        }
                        // 事件监听挂起（listen）：signal 名应匹配事件过滤器（支持多事件类型）
                        if meta.reason == "listen" {
                            if let Some(filter) = meta.event_filter.as_ref() {
                                let accepted: Vec<&str> = if !filter.event_types.is_empty() {
                                    filter.event_types.iter().map(|s| s.as_str()).collect()
                                } else {
                                    filter.event_type.iter().map(|s| s.as_str()).collect()
                                };
                                if !accepted.is_empty() {
                                    if let Some(name) = signal_name {
                                        if !accepted.contains(&name) {
                                            return Err(RuntimeError::InvalidSignal(format!(
                                                "instance '{}' is listening for event types {:?}, got signal '{name}'",
                                                instance_id, accepted
                                            )));
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // 注入 signal 数据到 context
                    if let Some(name) = signal_name {
                        inst.context["_signal"] = serde_json::json!({
                            "name": name,
                            "payload": payload.clone().unwrap_or(Value::Null),
                        });
                        // 手动 signal 同时注入 `_event`，使 switch+eventConditions /
                        // 多 onEvents 路由对「事件总线自动恢复」与「手动 signal」双路径一致生效
                        inst.context["_event"] = serde_json::json!({
                            "arrived": true,
                            "eventType": name,
                        });
                    }

                    // 完成当前挂起任务帧并推进（与 resume_by_event 一致）：
                    // 否则 drive 会重新执行已挂起的 listen/signal 任务，实例回到挂起态——
                    // 这是「signal 推进审批」端到端生效的必要一步（端到端验证发现）
                    if let Some(last) = inst.task_stack.last_mut() {
                        last.status = TaskStatus::Completed;
                        last.ended_at = Some(self.clock.now_ms());
                        if let Some(p) = &payload {
                            last.output = Some(p.clone());
                        }
                    }
                    inst.current_task_index += 1;

                    inst.status = InstanceStatus::Running;
                    inst.suspension_meta = None;
                    inst.updated_at = self.clock.now_ms();
                    Ok(true)
                },
            )
            .await?;

        let inst = match outcome {
            ControlPlaneOutcome::Committed(inst) => *inst,
            // resume 的守卫不产生 Skip（非可恢复状态走 Err(AlreadyCompleted)）；
            // 为完整匹配仍按「已不可恢复」返回。
            ControlPlaneOutcome::Skipped => {
                return Err(RuntimeError::AlreadyCompleted(instance_id.to_string()))
            }
        };

        // 后台 drive（受监督：见 [`Self::spawn_drive`]）
        self.spawn_drive(inst.id.clone(), def, Arc::clone(&self.store));

        Ok(inst)
    }

    /// 取消实例：非终态 → `Cancelled` + `workflow.cancelled`（同一次提交）。
    ///
    /// 控制面路径（[`Self::commit_control_plane`]）：线性一致读修订号后在 CAS 提交里
    /// 重新校验状态。一次「取消 vs 完成」的竞态里，若完成先提交则取消读到的是终态
    /// 而拒绝写入 —— 不会用 `Cancelled` 覆盖已提交的 `Completed`（消费者已见
    /// `workflow.completed`，再发 `workflow.cancelled` 会撕裂终态）。
    ///
    /// 终态 ⇒ [`RuntimeError::AlreadyCompleted`]（服务层映射为 FailedPrecondition），
    /// 与「实例不存在」的 [`RuntimeError::NotFound`] 区分。
    ///
    /// 生命周期事件与状态写同一次提交（durable 装配下同 Txn/同事务；legacy 分步，
    /// 见 [`Self::commit_instance`]）。
    pub async fn cancel_instance(&self, instance_id: &str) -> Result<(), RuntimeError> {
        let outcome = self
            .commit_control_plane(
                &self.store,
                instance_id,
                &[lifecycle::WORKFLOW_CANCELLED],
                |inst| {
                    if inst.status.is_terminal() {
                        return Err(RuntimeError::AlreadyCompleted(instance_id.to_string()));
                    }
                    inst.status = InstanceStatus::Cancelled;
                    inst.updated_at = self.clock.now_ms();
                    Ok(true)
                },
            )
            .await?;
        match outcome {
            ControlPlaneOutcome::Committed(_) => Ok(()),
            // 取消守卫不产生 Skip（非终态即提交）；为完整匹配仍按成功处理。
            ControlPlaneOutcome::Skipped => Ok(()),
        }
    }

    /// 对一个**已存在**的实例（重）启动驱动 —— 恢复/对账入口。
    ///
    /// 与 `start`/`resume` 的区别：不创建实例、不改变实例状态，只加载定义并
    /// 经 [`Self::spawn_drive`] 受监督地驱动（受同一实例租约约束）。
    /// 上游 supervisor 在进程重启后重驱动挂起/在飞实例时使用。
    pub async fn drive_instance(&self, instance_id: &str) -> Result<(), RuntimeError> {
        let inst = self
            .store
            .load_instance(instance_id)
            .await
            .map_err(|e| RuntimeError::StoreError(e.to_string()))?
            .ok_or_else(|| RuntimeError::NotFound(instance_id.to_string()))?;
        let def = self
            .store
            .load_definition(
                &inst.definition_ns,
                &inst.definition_name,
                &inst.definition_version,
            )
            .await
            .map_err(|e| RuntimeError::StoreError(e.to_string()))?
            .ok_or_else(|| {
                RuntimeError::DefinitionNotFound(format!(
                    "{}/{}@{}",
                    inst.definition_ns, inst.definition_name, inst.definition_version
                ))
            })?;
        // 定义漂移守卫：驱动前先校验钉死哈希（fail-closed），不得在新语义下驱动。
        self.fail_closed_on_definition_drift(&self.store, &inst.id, &def)
            .await?;
        self.spawn_drive(inst.id.clone(), def, Arc::clone(&self.store));
        Ok(())
    }

    /// 写回实例状态 + 伴生事件 —— 运行时对**已存在实例**的唯一状态写路径。
    ///
    /// 除模块头列出的三处新键创建（`start` 首写/校验失败写、`run` 子流程内联建子实例）
    /// 与下方的降级分支外，任何对已存在实例的状态写都必须经此路径，以保证 CAS/fencing
    /// 与「状态 + 事件同事务」不被绕过。
    ///
    /// 选择顺序：① 有租约 guard ⇒ `fenced_commit`（状态 + 事件同一 Txn，fencing 三重比较）；
    /// ② 有 `durable.atomic_commit` ⇒ `commit_state_and_outbox_batch`（状态 + 事件同一事务）；
    /// ③ 否则 legacy 分步（仅内存 / 无 KV 装配）：CAS/普通写 + 逐条发事件。
    ///
    /// 返回 `true` = 写入生效（并刷新 `rev` 供后续 CAS 使用）；`false` = CAS 冲突 /
    /// fencing 拒绝 / 原子提交未通过 —— 计入 [`Self::cas_conflict_total`]，
    /// 调用方不得静默吞掉。
    async fn commit_instance(
        &self,
        store: &Arc<S>,
        inst: &WorkflowInstance,
        rev: &mut Option<i64>,
        guard: Option<&dyn LeaseGuard>,
        events: &[&str],
    ) -> bool {
        let now_ms = self.clock.now_ms();
        let records: Vec<OutboxRecord> = events
            .iter()
            .map(|event_type| {
                lifecycle_record(
                    inst,
                    event_type,
                    now_ms,
                    format!("evt-{}", uuid::Uuid::new_v4()),
                )
            })
            .collect();

        let ok = if let Some(g) = guard {
            matches!(
                g.fenced_commit(inst, rev.unwrap_or(-1), &records).await,
                Ok(true)
            )
        } else if let Some(atomic) = self.durable.as_ref().and_then(|d| d.atomic_commit.as_ref()) {
            matches!(
                atomic
                    .commit_state_and_outbox_batch(inst, &records, *rev)
                    .await,
                Ok(true)
            )
        } else {
            // legacy：无 fencing / 无同事务能力的内存与降级路径；
            // 状态与事件分两步写（生产路径不走这里）。
            let saved = match *rev {
                Some(r) => matches!(store.save_instance_atomic(inst, r).await, Ok(true)),
                None => store.save_instance(inst).await.is_ok(),
            };
            if saved {
                match self.durable.as_ref() {
                    // 有 outbox 但无同事务提交：尽力逐条 append（事件不丢，但非原子）。
                    Some(d) => {
                        for rec in &records {
                            let _ = d.outbox.append(rec).await;
                        }
                    }
                    None => {
                        // 先落状态再发事件（非原子，仅降级路径）：发布口复用统一的
                        // [`Self::emit_lifecycle`]（`durable` 为 `None` 时它直发进程内总线）。
                        for event_type in events {
                            self.emit_lifecycle(event_type, inst).await;
                        }
                    }
                }
            }
            saved
        };
        if ok {
            // 刷新修订号：后续 CAS 必须以写入后的新值为期望。
            if let Ok(Some((_, r))) = store.load_instance_with_rev(&inst.id).await {
                *rev = Some(r);
            }
        } else if guard.is_some() || rev.is_some() {
            self.cas_conflicts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        ok
    }

    /// 控制面（无租约）CAS 写：线性一致读修订号 → 施加变更 → 提交。
    ///
    /// `mutate` 在**每次**尝试里对刚读到的实例重新求值：`Ok(true)` = 应用并提交；
    /// `Ok(false)` = 守卫未满足、放弃本次（不写）；`Err` = 致命错误（状态/信号校验等）。
    ///
    /// CAS 冲突（[`Self::commit_instance`] 返回 `false`）至多重放
    /// [`CONTROL_PLANE_CAS_ATTEMPTS`] 次，耗尽返回 [`RuntimeError::StaleRevision`]——
    /// 绝不盲写覆盖并发写者。
    async fn commit_control_plane<F>(
        &self,
        store: &Arc<S>,
        instance_id: &str,
        events: &[&str],
        mut mutate: F,
    ) -> Result<ControlPlaneOutcome, RuntimeError>
    where
        F: FnMut(&mut WorkflowInstance) -> Result<bool, RuntimeError>,
    {
        for _ in 0..CONTROL_PLANE_CAS_ATTEMPTS {
            let (mut inst, mut rev) = match store.load_instance_with_rev(instance_id).await {
                Ok(Some((i, r))) => (i, Some(r)),
                // 存储不提供修订号（内存/降级）：回退普通读，`rev = None`。
                Ok(None) => match store.load_instance(instance_id).await {
                    Ok(Some(i)) => (i, None),
                    Ok(None) => return Err(RuntimeError::NotFound(instance_id.to_string())),
                    Err(e) => return Err(RuntimeError::StoreError(e.to_string())),
                },
                Err(e) => return Err(RuntimeError::StoreError(e.to_string())),
            };
            if !mutate(&mut inst)? {
                return Ok(ControlPlaneOutcome::Skipped);
            }
            if self
                .commit_instance(store, &inst, &mut rev, None, events)
                .await
            {
                return Ok(ControlPlaneOutcome::Committed(Box::new(inst)));
            }
            if rev.is_none() {
                // 无 CAS 能力的存储：提交失败是普通写错误，不是并发冲突；
                // 重放无意义，也不得谎报成 StaleRevision。
                return Err(RuntimeError::StoreError(format!(
                    "instance write failed: {instance_id}"
                )));
            }
        }
        Err(RuntimeError::StaleRevision(instance_id.to_string()))
    }

    /// 执行驱动循环 —— 在 spawn 后独立运行
    ///
    /// 以 BoxFuture 返回（而非 async fn），避免递归异步造成的 opaque 类型循环。
    ///
    /// `guard` 为调用方持有的实例驱动租约（未注入租约时为 `None`）；非空时状态 +
    /// 伴生事件经 [`LeaseGuard::fenced_commit`]（三重 compare）同一次提交，
    /// 并拒绝失去租约的迟到写。
    pub(crate) fn drive<'a>(
        &'a self,
        instance_id: String,
        definition: WorkflowDefinition,
        store: Arc<S>,
        guard: Option<&'a dyn LeaseGuard>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            'drive_loop: loop {
                // 加载最新实例状态；存储支持 CAS 时同时取存储修订号（线性一致读）。
                let (mut inst, mut rev) = match store.load_instance_with_rev(&instance_id).await {
                    Ok(Some((i, r))) => (i, Some(r)),
                    _ => match store.load_instance(&instance_id).await {
                        Ok(Some(i)) => (i, None),
                        Ok(None) => break,
                        Err(_) => break,
                    },
                };

                // 终端状态则退出
                if inst.status.is_terminal() {
                    break;
                }

                // 定义钉死守卫（fail-closed）：钉死的定义内容与本次加载的定义不一致
                // ⇒ 不再在新语义上推进，置 Failed + 伴生 workflow.faulted（同一次提交），
                // 由上层控制面/启动对账以 `DefinitionDrift` 报出。
                if let Some((expected, actual)) = definition_content_drift(&inst, &definition) {
                    let fault = definition_drift_fault(&expected, &actual);
                    self.fail_instance(&mut inst, &store, &mut rev, guard, fault)
                        .await;
                    break 'drive_loop;
                }

                // 未钉死的旧快照：
                // 驱动成功即视作「认下这份定义」，在内存回填，随后下一次提交随之落库。
                if inst.definition_hash.is_none() {
                    inst.definition_hash = Some(super::model::definition_hash(&definition));
                }
                if inst.engine_version.is_none() {
                    inst.engine_version = Some(super::model::ENGINE_VERSION.to_string());
                }

                // 标准相位：Pending → Running（start 后首步推进）。
                // 首写走 CAS/fencing：两个驱动者并发时恰一个赢，输者退出 drive。
                if inst.status == InstanceStatus::Pending {
                    inst.status = InstanceStatus::Running;
                    inst.updated_at = self.clock.now_ms();
                    if !self
                        .commit_instance(
                            &store,
                            &inst,
                            &mut rev,
                            guard,
                            &[lifecycle::WORKFLOW_STARTED],
                        )
                        .await
                    {
                        break 'drive_loop;
                    }
                    continue;
                }

                // 执行一步
                let step = self.executor.execute_step(&inst, &definition);

                match step {
                    StepResult::NextTask(mut frame) => {
                        // emit 任务：fire-and-forget，通过 EventProvider 发布事件
                        if frame.task_type == "emit" {
                            if let Some(ref output) = frame.output {
                                let event_type = output["event_type"].as_str().unwrap_or("unknown");
                                let source = output["source"].as_str();
                                let data = output.get("data").unwrap_or(&serde_json::Value::Null);
                                self.event_provider.emit(event_type, source, data).await;
                            }
                        }
                        // 已完成帧：应用 output.as/export.as 数据流管线（Skipped 帧跳过）
                        let events: &[&str] = if frame.status == TaskStatus::Completed {
                            if let Err(fault) =
                                self.apply_task_output(&mut inst, &definition, &mut frame)
                            {
                                inst.task_stack.push(frame);
                                self.fail_instance(&mut inst, &store, &mut rev, guard, fault)
                                    .await;
                                break;
                            }
                            &[lifecycle::TASK_COMPLETED]
                        } else {
                            &[]
                        };
                        inst.task_stack.push(frame);
                        inst.current_task_index += 1;
                        inst.updated_at = self.clock.now_ms();
                        let _ = self
                            .commit_instance(&store, &inst, &mut rev, guard, events)
                            .await;
                    }

                    StepResult::Goto { target, frame } => {
                        inst.task_stack.push(frame);
                        match find_task_index(&definition.do_tasks, &target) {
                            Some(idx) => {
                                inst.current_task_index = idx;
                                inst.updated_at = self.clock.now_ms();
                                let _ = self
                                    .commit_instance(&store, &inst, &mut rev, guard, &[])
                                    .await;
                            }
                            None => {
                                inst.status = InstanceStatus::Failed;
                                inst.fault = Some(crate::workflow::errors::WorkflowFault::not_found(
                                format!("switch goto target '{}' not found", target),
                                "The switch condition referenced a task that does not exist in do_tasks",
                            ));
                                inst.updated_at = self.clock.now_ms();
                                let _ = self
                                    .commit_instance(&store, &inst, &mut rev, guard, &[])
                                    .await;
                                break;
                            }
                        }
                    }

                    StepResult::Suspend { reason, frame } => {
                        // 任务级 timeout 接线（超时 → timeout 错误 408 → faulted，仅当仍停留该任务）
                        let task_index = inst.current_task_index;
                        if let Some(meta) = definition.task_meta.get(&frame.task_name) {
                            if let Some(t) = meta.timeout.as_ref() {
                                if let Some(ms) =
                                    crate::workflow::engine::parse_iso8601_duration_ms(&t.after)
                                {
                                    self.schedule_timeout(
                                        inst.id.clone(),
                                        definition.clone(),
                                        store.clone(),
                                        ms.max(0) as u64,
                                        Some(task_index),
                                    );
                                }
                            }
                        }
                        // ExternalCall: 先尝试通过 TaskDispatcher 同步派发
                        // 如果派发成功，直接继续执行；失败则挂起等待外部恢复
                        match &reason {
                            SuspendReason::ExternalCall {
                                service,
                                with,
                                input,
                            } => {
                                let task_index = inst.current_task_index;
                                let task_name = frame.task_name.clone();
                                let result = match self
                                    .register_step(&inst.id, task_index, &task_name)
                                    .await
                                {
                                    Ok(key) => {
                                        self.dispatcher
                                            .dispatch(service, with.as_ref(), input, key.as_deref())
                                            .await
                                    }
                                    Err(error) => DispatchResult::Failure {
                                        error,
                                        retryable: true,
                                    },
                                };

                                match result {
                                    DispatchResult::Success { data } => {
                                        self.mark_step_done(&inst.id, task_index, &task_name).await;
                                        // 派发成功 —— 不挂起，直接推进
                                        let mut completed_frame = frame;
                                        completed_frame.status = TaskStatus::Completed;
                                        completed_frame.output = Some(data);
                                        completed_frame.ended_at = Some(self.clock.now_ms());
                                        // 反映重试次数（retry_count 存于 suspension_meta）
                                        if let Some(m) = inst.suspension_meta.as_ref() {
                                            if m.reason == "retry" {
                                                completed_frame.retry_count =
                                                    m.retry_count.unwrap_or(0);
                                            }
                                        }
                                        // 应用任务 output.as/export.as 数据流管线
                                        if let Err(fault) = self.apply_task_output(
                                            &mut inst,
                                            &definition,
                                            &mut completed_frame,
                                        ) {
                                            inst.task_stack.push(completed_frame);
                                            self.fail_instance(
                                                &mut inst, &store, &mut rev, guard, fault,
                                            )
                                            .await;
                                            break 'drive_loop;
                                        }
                                        inst.task_stack.push(completed_frame);
                                        inst.current_task_index += 1;
                                        inst.updated_at = self.clock.now_ms();
                                        inst.suspension_meta = None;
                                        let _ = self
                                            .commit_instance(
                                                &store,
                                                &inst,
                                                &mut rev,
                                                guard,
                                                &[lifecycle::TASK_COMPLETED],
                                            )
                                            .await;
                                        continue;
                                    }
                                    DispatchResult::Failure { error, retryable } => {
                                        if !retryable {
                                            // 不可重试的错误 → 直接 faulted
                                            inst.task_stack.push(frame);
                                            self.fail_instance(
                                                &mut inst,
                                                &store,
                                                &mut rev,
                                                guard,
                                                crate::workflow::errors::WorkflowFault::communication(
                                                    format!("call to '{}' failed: {}", service, error),
                                                    error,
                                                ),
                                            )
                                            .await;
                                            break 'drive_loop;
                                        }
                                        // 可重试的错误 → retry 策略接线（标准 Fault Tolerance）
                                        let prev_retries = inst
                                            .suspension_meta
                                            .as_ref()
                                            .filter(|m| m.reason == "retry")
                                            .and_then(|m| m.retry_count)
                                            .unwrap_or(0);
                                        let policy = definition
                                            .task_meta
                                            .get(&frame.task_name)
                                            .and_then(|m| m.retry.as_ref())
                                            .map(|p| RetryConfig::from_policy(p, None))
                                            .unwrap_or_default();
                                        if prev_retries >= policy.max_attempts {
                                            // 重试耗尽 → faulted（communication）
                                            inst.task_stack.push(frame);
                                            self.fail_instance(
                                                &mut inst,
                                                &store,
                                                &mut rev,
                                                guard,
                                                crate::workflow::errors::WorkflowFault::communication(
                                                    format!(
                                                        "call to '{}' failed after {} retries: {}",
                                                        service, prev_retries, error
                                                    ),
                                                    error,
                                                ),
                                            )
                                            .await;
                                            break 'drive_loop;
                                        }
                                        // 计算下次重试延迟
                                        let default_delay = policy.delay_ms;
                                        let mut scheduler = RetryScheduler::new(policy);
                                        let mut delay = default_delay;
                                        for _ in 0..=prev_retries {
                                            if let Some(d) = scheduler.next_delay_ms() {
                                                delay = d;
                                            }
                                        }
                                        let until = self.clock.now_ms() + delay as i64;
                                        // 进入 Waiting（自动恢复）+ 定时重试
                                        inst.suspension_meta = Some(SuspensionMeta {
                                            reason: "retry".to_string(),
                                            until_ms: Some(until),
                                            service: Some(service.clone()),
                                            payload: Some(input.clone()),
                                            event_filter: None,
                                            expected_signal: None,
                                            approval_instance_id: None,
                                            retry_count: Some(prev_retries + 1),
                                            error: Some(error),
                                        });
                                        inst.status = InstanceStatus::Waiting;
                                        inst.updated_at = self.clock.now_ms();
                                        let _ = self
                                            .commit_instance(
                                                &store,
                                                &inst,
                                                &mut rev,
                                                guard,
                                                &[lifecycle::TASK_RETRIED],
                                            )
                                            .await;
                                        self.schedule_auto_resume(
                                            inst.id.clone(),
                                            definition.clone(),
                                            store.clone(),
                                            until,
                                            "retry".to_string(),
                                        );
                                        break 'drive_loop;
                                    }
                                }
                            }
                            SuspendReason::RunSubflow {
                                workflow,
                                input,
                                parent_instance_id: _,
                            } => {
                                // 加载子工作流定义
                                let sub_def = match store
                                    .load_definition(
                                        &workflow.namespace,
                                        &workflow.name,
                                        &workflow.version,
                                    )
                                    .await
                                {
                                    Ok(Some(def)) => def,
                                    Ok(None) => {
                                        inst.task_stack.push(frame);
                                        inst.status = InstanceStatus::Failed;
                                        inst.fault = Some(crate::workflow::errors::WorkflowFault::not_found(
                                        format!(
                                            "subflow '{}::{}@{}' not found",
                                            workflow.namespace, workflow.name, workflow.version
                                        ),
                                        "The referenced sub-workflow definition does not exist",
                                    ));
                                        inst.updated_at = self.clock.now_ms();
                                        let _ = self
                                            .commit_instance(&store, &inst, &mut rev, guard, &[])
                                            .await;
                                        break 'drive_loop;
                                    }
                                    Err(e) => {
                                        inst.task_stack.push(frame);
                                        inst.status = InstanceStatus::Failed;
                                        inst.fault =
                                            Some(crate::workflow::errors::WorkflowFault::internal(
                                                format!("failed to load subflow: {e}"),
                                                e.to_string(),
                                            ));
                                        inst.updated_at = self.clock.now_ms();
                                        let _ = self
                                            .commit_instance(&store, &inst, &mut rev, guard, &[])
                                            .await;
                                        break 'drive_loop;
                                    }
                                };

                                // 内联创建子实例（避免 self.start() 的 Send 问题）
                                let sub_input_val = input.clone().unwrap_or(Value::Null);
                                let sub_now = self.clock.now_ms();
                                let sub_inst =
                                    WorkflowInstance::new(&sub_def, sub_input_val, sub_now);
                                let sub_id = sub_inst.id.clone();

                                if let Err(e) = store.save_instance(&sub_inst).await {
                                    inst.task_stack.push(frame);
                                    inst.status = InstanceStatus::Failed;
                                    inst.fault =
                                        Some(crate::workflow::errors::WorkflowFault::internal(
                                            format!("failed to save subflow instance: {e}"),
                                            e.to_string(),
                                        ));
                                    inst.updated_at = self.clock.now_ms();
                                    let _ = self
                                        .commit_instance(&store, &inst, &mut rev, guard, &[])
                                        .await;
                                    break 'drive_loop;
                                }

                                // 挂起父流程（标准相位：子流程自动恢复 → Waiting）
                                inst.task_stack.push(frame);
                                inst.status = InstanceStatus::Waiting;
                                inst.suspension_meta = Some(
                                    SuspendReason::RunSubflow {
                                        workflow: workflow.clone(),
                                        input: input.clone(),
                                        parent_instance_id: inst.id.clone(),
                                    }
                                    .to_meta(),
                                );
                                inst.context["_subflow_instance_id"] =
                                    Value::String(sub_id.clone());
                                inst.updated_at = self.clock.now_ms();
                                let _ = self
                                    .commit_instance(
                                        &store,
                                        &inst,
                                        &mut rev,
                                        guard,
                                        &[lifecycle::WORKFLOW_WAITING],
                                    )
                                    .await;
                                // 登记父子对，交给 per-tick 的登记表扫描
                                // （不再依赖每 5 秒的全量列举）。
                                self.register_pending_subflow(inst.id.clone(), sub_id.clone());

                                // 子流程监控由后台扫描器 (start_subflow_scanner) 负责，
                                // 父流程挂起后会在子流程完成时由扫描器自动恢复。
                                break 'drive_loop;
                            }
                            SuspendReason::WaitingForDuration { until_ms } => {
                                // wait 任务：标准相位 Waiting（自动恢复）
                                inst.task_stack.push(frame);
                                inst.status = InstanceStatus::Waiting;
                                inst.suspension_meta = Some(reason.to_meta());
                                inst.updated_at = self.clock.now_ms();
                                let _ = self
                                    .commit_instance(
                                        &store,
                                        &inst,
                                        &mut rev,
                                        guard,
                                        &[lifecycle::WORKFLOW_WAITING],
                                    )
                                    .await;

                                // 自动恢复定时器（wait 到期后完成帧并推进）
                                self.schedule_auto_resume(
                                    inst.id.clone(),
                                    definition.clone(),
                                    store.clone(),
                                    *until_ms,
                                    "wait".to_string(),
                                );
                                break 'drive_loop;
                            }
                            SuspendReason::ListeningForEvent { event_filter } => {
                                // listen 任务：标准相位 Waiting（等待事件/超时，自动恢复）
                                let task_name = frame.task_name.clone();
                                let filter = event_filter.clone();
                                inst.task_stack.push(frame);
                                inst.status = InstanceStatus::Waiting;
                                inst.suspension_meta = Some(reason.to_meta());
                                inst.updated_at = self.clock.now_ms();
                                let _ = self
                                    .commit_instance(
                                        &store,
                                        &inst,
                                        &mut rev,
                                        guard,
                                        &[lifecycle::WORKFLOW_WAITING],
                                    )
                                    .await;

                                // 任务级 timeout：事件超时 → faulted（仅当仍停留该任务）
                                let mut event_timeout_ms: Option<u64> = None;
                                let listen_index = inst.current_task_index;
                                if let Some(meta) = definition.task_meta.get(&task_name) {
                                    if let Some(t) = meta.timeout.as_ref() {
                                        if let Some(ms) =
                                            crate::workflow::engine::parse_iso8601_duration_ms(
                                                &t.after,
                                            )
                                        {
                                            let ms = ms.max(0) as u64;
                                            event_timeout_ms = Some(ms);
                                            self.schedule_timeout(
                                                inst.id.clone(),
                                                definition.clone(),
                                                store.clone(),
                                                ms,
                                                Some(listen_index),
                                            );
                                        }
                                    }
                                }

                                // 主动事件等待（标准 Events：listen 升级为主动订阅 + correlation）：
                                // 每个挂起实例持有独立订阅，事件到达 → 自动恢复该实例。
                                self.spawn_event_wait(
                                    inst.id.clone(),
                                    definition.clone(),
                                    Arc::clone(&store),
                                    filter.clone(),
                                    event_timeout_ms,
                                );
                                break 'drive_loop;
                            }
                            SuspendReason::WaitingForSignal { .. } => {
                                // signal 挂起：人工恢复 → 标准相位 Suspended
                                inst.task_stack.push(frame);
                                inst.status = InstanceStatus::Suspended;
                                inst.suspension_meta = Some(reason.to_meta());
                                inst.updated_at = self.clock.now_ms();
                                let _ = self
                                    .commit_instance(
                                        &store,
                                        &inst,
                                        &mut rev,
                                        guard,
                                        &[lifecycle::WORKFLOW_SUSPENDED],
                                    )
                                    .await;
                                break 'drive_loop;
                            }
                            SuspendReason::WaitingForApproval { node, .. } => {
                                // approve 节点：先经审批桥开启审批实例（幂等），再按人工恢复
                                // 语义挂起 —— 不 spawn 任何自动恢复（审批完成由恢复路径驱动）。
                                //
                                // 开不出审批必须 fault-closed：挂起却没开审批会让实例永远
                                // 等不到「有席可批」，而静默继续执行等于跳过审批。
                                let approve_task = definition
                                    .do_tasks
                                    .get(inst.current_task_index)
                                    .and_then(|named| match &named.task {
                                        Task::Approve(task) => Some(task),
                                        _ => None,
                                    });
                                let opened = match (self.approval.as_ref(), approve_task) {
                                    (Some(bridge), Some(task)) => bridge
                                        .ensure_node_open(&inst, node, task)
                                        .await
                                        .map_err(|e| e.to_string()),
                                    (None, _) => Err(format!(
                                        "task '{}' suspended for approval, but this runtime has \
                                         no approval bridge to open an approval instance",
                                        frame.task_name
                                    )),
                                    (Some(_), None) => Err(format!(
                                        "task '{}' suspended for approval, but the pinned \
                                         definition has no approve node at index {}",
                                        frame.task_name, inst.current_task_index
                                    )),
                                };
                                if let Err(error) = opened {
                                    let fault = crate::workflow::errors::WorkflowFault::internal(
                                        "approve node has no approval instance",
                                        format!(
                                            "approve node '{}' could not be opened: {error}",
                                            node
                                        ),
                                    );
                                    self.fail_instance(&mut inst, &store, &mut rev, guard, fault)
                                        .await;
                                    break 'drive_loop;
                                }

                                inst.task_stack.push(frame);
                                inst.status = InstanceStatus::Suspended;
                                inst.suspension_meta = Some(reason.to_meta());
                                inst.updated_at = self.clock.now_ms();
                                let _ = self
                                    .commit_instance(
                                        &store,
                                        &inst,
                                        &mut rev,
                                        guard,
                                        &[lifecycle::WORKFLOW_SUSPENDED],
                                    )
                                    .await;
                                break 'drive_loop;
                            }
                        }
                    }

                    StepResult::SetVariable {
                        variable,
                        value,
                        mut frame,
                    } => {
                        inst.context[variable] = value;
                        // 应用任务 output.as/export.as 数据流管线
                        if let Err(fault) =
                            self.apply_task_output(&mut inst, &definition, &mut frame)
                        {
                            inst.task_stack.push(frame);
                            self.fail_instance(&mut inst, &store, &mut rev, guard, fault)
                                .await;
                            break;
                        }
                        inst.task_stack.push(frame);
                        inst.current_task_index += 1;
                        inst.updated_at = self.clock.now_ms();
                        let _ = self
                            .commit_instance(
                                &store,
                                &inst,
                                &mut rev,
                                guard,
                                &[lifecycle::TASK_COMPLETED],
                            )
                            .await;
                    }

                    StepResult::Fork {
                        branches,
                        compete,
                        frame,
                    } => {
                        // 并行执行分支（使用 tokio::spawn 真正并行）
                        let executor = Arc::clone(&self.executor);
                        let clock = Arc::clone(&self.clock);
                        let def = definition.clone();
                        let base_inst = inst.clone();

                        if compete {
                            // compete 模式：首个完成的分支胜出，其余取消
                            let mut join_set = tokio::task::JoinSet::new();
                            for branch in &branches {
                                let branch = branch.clone();
                                let executor = Arc::clone(&executor);
                                let def = def.clone();
                                let base_inst = base_inst.clone();

                                join_set.spawn(async move {
                                    execute_branch(branch, &executor, &def, &base_inst)
                                });
                            }

                            // 等待首个完成的分支（修复 clippy never_loop——
                            // 原 while let 恒单次迭代）
                            let mut winner_result: Option<(String, serde_json::Value)> = None;
                            if let Some(result) = join_set.join_next().await {
                                join_set.abort_all();
                                if let Ok((name, results, has_failure, fault)) = result {
                                    if has_failure {
                                        let mut completed_frame = frame;
                                        completed_frame.status = TaskStatus::Failed;
                                        completed_frame.ended_at = Some(clock.now_ms());
                                        completed_frame.output =
                                            Some(serde_json::Value::Object(serde_json::Map::new()));
                                        inst.task_stack.push(completed_frame);
                                        inst.status = InstanceStatus::Failed;
                                        inst.fault = fault;
                                        inst.updated_at = clock.now_ms();
                                        let _ = self
                                            .commit_instance(&store, &inst, &mut rev, guard, &[])
                                            .await;
                                        continue 'drive_loop;
                                    }
                                    let output = results
                                        .into_iter()
                                        .next()
                                        .unwrap_or(serde_json::Value::Null);
                                    winner_result = Some((name, output));
                                }
                                // only take first result in compete mode
                            }

                            if let Some((_name, output)) = winner_result {
                                let mut completed_frame = frame;
                                completed_frame.status = TaskStatus::Completed;
                                completed_frame.ended_at = Some(clock.now_ms());
                                completed_frame.output = Some(output);
                                inst.task_stack.push(completed_frame);
                                inst.current_task_index += 1;
                                inst.updated_at = clock.now_ms();
                                let _ = self
                                    .commit_instance(&store, &inst, &mut rev, guard, &[])
                                    .await;
                            }
                        } else {
                            // 非 compete 模式：等待所有分支完成
                            let mut join_set = tokio::task::JoinSet::new();
                            for branch in &branches {
                                let branch = branch.clone();
                                let executor = Arc::clone(&executor);
                                let def = def.clone();
                                let base_inst = base_inst.clone();

                                join_set.spawn(async move {
                                    execute_branch(branch, &executor, &def, &base_inst)
                                });
                            }

                            let mut branch_results = serde_json::Map::new();
                            let mut has_failure = false;
                            let mut fork_fault: Option<WorkflowFault> = None;

                            while let Some(result) = join_set.join_next().await {
                                match result {
                                    Ok((name, results, failed, fault)) => {
                                        if failed {
                                            has_failure = true;
                                            fork_fault = fault;
                                        }
                                        let output = results
                                            .into_iter()
                                            .next()
                                            .unwrap_or(serde_json::Value::Null);
                                        branch_results.insert(name, output);
                                    }
                                    Err(join_err) => {
                                        has_failure = true;
                                        fork_fault =
                                            Some(crate::workflow::errors::WorkflowFault::internal(
                                                format!("branch task panicked: {join_err}"),
                                                "A fork branch task panicked during execution",
                                            ));
                                    }
                                }
                            }

                            let mut completed_frame = frame;
                            completed_frame.status = if has_failure {
                                TaskStatus::Failed
                            } else {
                                TaskStatus::Completed
                            };
                            completed_frame.ended_at = Some(clock.now_ms());
                            completed_frame.output =
                                Some(serde_json::Value::Object(branch_results));

                            if has_failure {
                                inst.task_stack.push(completed_frame);
                                inst.status = InstanceStatus::Failed;
                                inst.fault = fork_fault;
                                inst.updated_at = clock.now_ms();
                                let _ = self
                                    .commit_instance(&store, &inst, &mut rev, guard, &[])
                                    .await;
                                break;
                            }

                            inst.task_stack.push(completed_frame);
                            inst.current_task_index += 1;
                            inst.updated_at = clock.now_ms();
                            let _ = self
                                .commit_instance(&store, &inst, &mut rev, guard, &[])
                                .await;
                        }
                    }

                    StepResult::ForEach {
                        input_expr,
                        iteration,
                        tasks,
                        frame,
                    } => {
                        // 求值输入表达式
                        let array = self
                            .executor
                            .expr
                            .evaluate(&input_expr, &inst.context)
                            .unwrap_or(serde_json::Value::Array(vec![]));

                        let items = match array {
                            serde_json::Value::Array(arr) => arr,
                            _ => vec![],
                        };

                        let mut results = Vec::new();
                        let mut has_failure = false;
                        let mut foreach_fault = None;

                        for item in &items {
                            let mut iter_ctx = inst.context.clone();
                            iter_ctx[&iteration] = item.clone();

                            for task in &tasks {
                                let step_result = self.executor.execute_step(
                                    &WorkflowInstance {
                                        context: iter_ctx.clone(),
                                        ..inst.clone()
                                    },
                                    &WorkflowDefinition {
                                        do_tasks: vec![task.clone()],
                                        ..definition.clone()
                                    },
                                );
                                match step_result {
                                    StepResult::NextTask(tf) => {
                                        iter_ctx = apply_frame_output(iter_ctx, &tf);
                                    }
                                    StepResult::Completed { .. } => {
                                        // completed successfully
                                    }
                                    StepResult::Failed { fault } => {
                                        has_failure = true;
                                        foreach_fault = Some(fault);
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                            if has_failure {
                                break;
                            }
                            results.push(iter_ctx.get(&iteration).cloned().unwrap_or(item.clone()));
                        }

                        let mut completed_frame = frame;
                        completed_frame.status = TaskStatus::Completed;
                        completed_frame.ended_at = Some(self.clock.now_ms());
                        completed_frame.output = Some(serde_json::Value::Array(results));

                        if has_failure {
                            inst.task_stack.push(completed_frame);
                            inst.status = InstanceStatus::Failed;
                            inst.fault = foreach_fault;
                            inst.updated_at = self.clock.now_ms();
                            let _ = self
                                .commit_instance(&store, &inst, &mut rev, guard, &[])
                                .await;
                            break;
                        }

                        inst.task_stack.push(completed_frame);
                        inst.current_task_index += 1;
                        inst.updated_at = self.clock.now_ms();
                        let _ = self
                            .commit_instance(&store, &inst, &mut rev, guard, &[])
                            .await;
                    }

                    StepResult::TryBlock {
                        try_tasks,
                        catch_clauses,
                        frame,
                    } => {
                        let mut try_failed = false;
                        let mut try_fault: Option<WorkflowFault> = None;
                        let mut try_ctx = inst.context.clone();

                        // 执行 try 块
                        //
                        // ⚠️ `Suspend` **必须**在这里处理。
                        // `call` 任务的执行器是不做同步 I/O 的：它返回
                        // `Suspend { ExternalCall }`，由 Runtime 去 `dispatch`。
                        // match 只有 `NextTask | Completed | Failed | _` 时，
                        // `Suspend` 会落进 `_ => {}` **被静默丢弃**：
                        //   ⇒ 被 `compensatedBy` / `onErrors` 包裹的状态，其动作
                        //     **一次都不会发出去**（既不成功也不失败），
                        //     而工作流继续沿正常路径前进 —— 调用方以为副作用执行了。
                        // 这是"最坏的一类"：静默丢副作用。故此处与主循环
                        // （见 `SuspendReason::ExternalCall` 分支）同口径派发。
                        //
                        // ⚠️ 内层 `execute_step` 的 `current_task_index` **必须归零**：
                        // 它按索引在单任务视图（`do_tasks: vec![task]`）里取任务，
                        // 而 `..inst.clone()` 带的是**外层包装器的索引**——索引越界时
                        // 执行器静默不做事，同样造成"try 体被跳过但实例继续前进"。
                        for task in &try_tasks {
                            let step_result = self.executor.execute_step(
                                &WorkflowInstance {
                                    context: try_ctx.clone(),
                                    current_task_index: 0,
                                    ..inst.clone()
                                },
                                &WorkflowDefinition {
                                    do_tasks: vec![task.clone()],
                                    ..definition.clone()
                                },
                            );
                            match step_result {
                                StepResult::NextTask(tf) => {
                                    try_ctx = apply_frame_output(try_ctx, &tf);
                                }
                                StepResult::Completed { .. } => {
                                    // completed successfully
                                }
                                StepResult::Failed { fault } => {
                                    try_failed = true;
                                    try_fault = Some(fault);
                                    break;
                                }
                                // `call` 任务：派发 I/O，把结果折叠成"成功"或"失败"
                                StepResult::Suspend {
                                    reason:
                                        SuspendReason::ExternalCall {
                                            service,
                                            with,
                                            input,
                                        },
                                    frame: call_frame,
                                } => {
                                    let task_index = inst.current_task_index;
                                    let task_name = call_frame.task_name.clone();
                                    let result = match self
                                        .register_step(&inst.id, task_index, &task_name)
                                        .await
                                    {
                                        Ok(key) => {
                                            self.dispatcher
                                                .dispatch(
                                                    &service,
                                                    with.as_ref(),
                                                    &input,
                                                    key.as_deref(),
                                                )
                                                .await
                                        }
                                        Err(error) => DispatchResult::Failure {
                                            error,
                                            retryable: true,
                                        },
                                    };
                                    match result {
                                        DispatchResult::Success { data } => {
                                            self.mark_step_done(&inst.id, task_index, &task_name)
                                                .await;
                                            let mut done = call_frame;
                                            done.status = TaskStatus::Completed;
                                            done.output = Some(data);
                                            done.ended_at = Some(self.clock.now_ms());
                                            try_ctx = apply_frame_output(try_ctx, &done);
                                        }
                                        DispatchResult::Failure { error, .. } => {
                                            // 有意**不使用**折叠出的 `retryable`：try 体内的路由
                                            // 由 catch 子句的 `errors` 匹配决定（catch-all 会接住）；
                                            // 这里统一建模为 communication fault 再交 catch 路由。
                                            try_failed = true;
                                            try_fault = Some(
                                                crate::workflow::errors::WorkflowFault::communication(
                                                    format!("call to '{service}' failed: {error}"),
                                                    error,
                                                ),
                                            );
                                            break;
                                        }
                                    }
                                }
                                // 其它挂起形态（等待事件 / 延时 / 信号）在 try 块内
                                // 不做特殊处理：乐观推进，语义与修前一致。
                                _ => {}
                            }
                        }

                        let mut completed_frame = frame;

                        if try_failed {
                            // 匹配 catch 子句
                            let fault_type =
                                try_fault.as_ref().map(|f| f.r#type.as_str()).unwrap_or("");
                            let mut caught = false;
                            let mut goto_target: Option<String> = None;

                            for clause in &catch_clauses {
                                let matches = match &clause.errors {
                                    Some(errors) => errors.iter().any(|e| e == fault_type),
                                    None => true, // catch-all
                                };
                                if matches {
                                    // 执行 catch 任务（onErrors 转场：Goto → 路由到目标状态）
                                    // 同样：单任务视图必须用索引 0（见 try 块的同名说明）。
                                    for task in &clause.tasks {
                                        let step_result = self.executor.execute_step(
                                            &WorkflowInstance {
                                                context: inst.context.clone(),
                                                current_task_index: 0,
                                                ..inst.clone()
                                            },
                                            &WorkflowDefinition {
                                                do_tasks: vec![task.clone()],
                                                ..definition.clone()
                                            },
                                        );
                                        match step_result {
                                            StepResult::NextTask(_)
                                            | StepResult::Completed { .. } => {}
                                            StepResult::Goto { target, .. } => {
                                                goto_target = Some(target);
                                            }
                                            StepResult::Failed { .. } => {}
                                            _ => {}
                                        }
                                    }
                                    caught = true;
                                    break;
                                }
                            }

                            if !caught {
                                // 未捕获的错误向上传播
                                inst.task_stack.push(completed_frame);
                                inst.status = InstanceStatus::Failed;
                                inst.fault = try_fault;
                                inst.updated_at = self.clock.now_ms();
                                let _ = self
                                    .commit_instance(&store, &inst, &mut rev, guard, &[])
                                    .await;
                                break;
                            }

                            // 已捕获：若 catch 指定了转场目标，路由到该状态
                            if let Some(target) = goto_target {
                                if let Some(idx) = find_task_index(&definition.do_tasks, &target) {
                                    inst.current_task_index = idx;
                                } else if target == "__end" {
                                    inst.current_task_index = definition.do_tasks.len();
                                }
                            }
                        }

                        completed_frame.status = TaskStatus::Completed;
                        completed_frame.ended_at = Some(self.clock.now_ms());
                        inst.task_stack.push(completed_frame);
                        if !try_failed {
                            inst.current_task_index += 1;
                        }
                        inst.updated_at = self.clock.now_ms();
                        let _ = self
                            .commit_instance(&store, &inst, &mut rev, guard, &[])
                            .await;
                    }

                    StepResult::Completed { output } => {
                        // 工作流级 output.as / output.schema（标准 Data Flow）
                        let final_output =
                            match self.apply_workflow_output(&definition, &inst, output) {
                                Ok(v) => v,
                                Err(fault) => {
                                    self.fail_instance(&mut inst, &store, &mut rev, guard, fault)
                                        .await;
                                    break;
                                }
                            };
                        inst.status = InstanceStatus::Completed;
                        inst.output = Some(final_output);
                        inst.updated_at = self.clock.now_ms();
                        // 终态写回不得因 CAS 冲突而“卡住”：冲突则重读重试（下一轮重算）。
                        if !self
                            .commit_instance(
                                &store,
                                &inst,
                                &mut rev,
                                guard,
                                &[lifecycle::WORKFLOW_COMPLETED],
                            )
                            .await
                        {
                            continue 'drive_loop;
                        }
                        break;
                    }

                    StepResult::Failed { fault } => {
                        self.fail_instance(&mut inst, &store, &mut rev, guard, fault)
                            .await;
                        break;
                    }
                }
            }
        })
    }

    /// 克隆运行时引用（用于 spawn）
    pub(crate) fn clone_runtime(&self) -> Self {
        Self {
            executor: Arc::clone(&self.executor),
            clock: Arc::clone(&self.clock),
            store: Arc::clone(&self.store),
            dispatcher: Arc::clone(&self.dispatcher),
            event_provider: Arc::clone(&self.event_provider),
            pending_subflows: Arc::clone(&self.pending_subflows),
            workers: Arc::clone(&self.workers),
            lease: self.lease.clone(),
            lease_skipped: Arc::clone(&self.lease_skipped),
            cas_conflicts: Arc::clone(&self.cas_conflicts),
            durable: self.durable.clone(),
            approval: self.approval.clone(),
        }
    }
}

// ─── 辅助函数 ───

/// 在 NamedTask 列表中查找指定名称的任务索引
fn find_task_index(tasks: &[super::model::NamedTask], name: &str) -> Option<usize> {
    tasks.iter().position(|t| t.name == name)
}

/// 将任务帧的输出合并到上下文中（用于 set 等会修改 context 的任务）
fn apply_frame_output(mut ctx: serde_json::Value, frame: &TaskFrame) -> serde_json::Value {
    if let Some(ref output) = frame.output {
        if let Some(obj) = ctx.as_object_mut() {
            if let Some(out_obj) = output.as_object() {
                for (k, v) in out_obj {
                    obj.insert(k.clone(), v.clone());
                }
            }
        }
    }
    ctx
}

/// 在独立 tokio 任务中执行一个 fork 分支的所有任务
///
/// 返回 `(branch_name, results, has_failure, fault)`
fn execute_branch<E, C>(
    branch: crate::workflow::model::ForkBranch,
    executor: &WorkflowExecutor<E, C>,
    definition: &WorkflowDefinition,
    base_inst: &WorkflowInstance,
) -> (String, Vec<serde_json::Value>, bool, Option<WorkflowFault>)
where
    E: ExpressionEval,
    C: Clock,
{
    let mut ctx = base_inst.context.clone();
    let mut results = Vec::new();
    let mut has_failure = false;
    let mut fault = None;

    for task in &branch.tasks {
        let step = executor.execute_step(
            &WorkflowInstance {
                context: ctx.clone(),
                ..base_inst.clone()
            },
            &WorkflowDefinition {
                do_tasks: vec![task.clone()],
                ..definition.clone()
            },
        );
        match step {
            StepResult::NextTask(tf) => {
                ctx = apply_frame_output(ctx, &tf);
                results.push(tf.output.unwrap_or(serde_json::Value::Null));
            }
            StepResult::Completed { output } => {
                results.push(output);
            }
            StepResult::Failed { fault: f } => {
                has_failure = true;
                fault = Some(f);
                break;
            }
            _ => {
                results.push(serde_json::Value::Null);
            }
        }
    }

    (branch.name, results, has_failure, fault)
}

// ─── 测试 ───

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::engine::WorkflowExecutor;
    use crate::workflow::expression::ExpressionEvaluator;
    use crate::workflow::model::{
        Document, NamedTask, SetTask, Task, WaitTask, WorkflowDefinition,
    };
    use crate::workflow::ports::test_utils::{
        MemoryWorkflowStore, NoopEventProvider, NoopTaskDispatcher, TestClock,
    };
    use crate::workflow::ports::{StoreError, WorkflowStore};

    fn make_runtime() -> WorkflowRuntime<
        ExpressionEvaluator,
        TestClock,
        MemoryWorkflowStore,
        NoopTaskDispatcher,
        NoopEventProvider,
    > {
        let expr = ExpressionEvaluator::new();
        let clock = TestClock::new(1000);
        let executor = WorkflowExecutor::new(expr, TestClock::new(1000));
        let store = MemoryWorkflowStore::new();
        let dispatcher = NoopTaskDispatcher;
        let event_provider = NoopEventProvider;
        WorkflowRuntime::new(executor, clock, store, dispatcher, event_provider)
    }

    // ── 后台任务存活登记（正 / 反双测） ──

    /// 正：刚建好的运行时健康 —— 扫描器在跑、没有故障。
    ///
    /// 这条同时是"判据不是恒真"的对照：如果 `finished_loops` 恒非空，下面的负控制
    /// 就没有意义了。
    #[tokio::test]
    async fn worker_liveness_is_healthy_on_a_fresh_runtime() {
        let rt = make_runtime();
        // 让扫描器任务真正开始执行（`tokio::spawn` 后需要一次让出）
        tokio::task::yield_now().await;
        let l = rt.worker_liveness();
        assert!(
            l.finished_loops.is_empty(),
            "刚启动的运行时不得报告已死的循环 worker：{l:?}"
        );
        assert!(!l.has_fault(), "刚启动的运行时不应有故障：{l:?}");
        assert_eq!(l.panic_total(), 0);
        assert!(l.recent_faults.is_empty());
    }

    /// 反（负控制）：把"已死的循环 worker"注入登记表 ⇒ **必须**被报出来。
    ///
    /// 不用真的弄死生产任务：判据是"`is_finished()` 为真即故障"，所以注入一个已经
    /// 结束的句柄就等价于"那个循环死了"。这条测试防的是"登记了但没人看"——
    /// 即"能力静默死亡"。
    #[tokio::test]
    async fn worker_liveness_reports_a_dead_loop_worker() {
        let rt = make_runtime();
        let finished = tokio::spawn(async {});
        // 等它真的结束
        for _ in 0..100 {
            if finished.is_finished() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(finished.is_finished(), "注入用的句柄必须已结束");
        rt.workers
            .register_loop_for_test("injected_dead_loop", finished);

        let l = rt.worker_liveness();
        assert!(
            l.finished_loops.iter().any(|n| n == "injected_dead_loop"),
            "已结束的循环 worker 必须出现在 finished_loops 里：{l:?}"
        );
        assert!(l.has_fault(), "有已死循环 worker 时 has_fault 必须为真");
    }

    /// 反（负控制）：一次性 worker "结束但没跑到最后一行" ⇒ 计入故障并保留标签；
    /// 而"正常收尾"的那一个**不得**被计入。
    ///
    /// 两个方向都在同一条测试里，因为这条判据的全部难点就是**区分**这两者。
    #[tokio::test]
    async fn worker_liveness_distinguishes_normal_completion_from_midway_death() {
        let rt = make_runtime();

        // ① 正常收尾：置位 completed
        let ok_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let f = Arc::clone(&ok_flag);
        let ok_handle = tokio::spawn(async move {
            f.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        rt.workers
            .register_oneshot("workflow_drive[ok]".to_string(), ok_handle, ok_flag);

        // ② 中途死亡：句柄结束但 completed 未置位（等价于 panic / abort）
        let dead_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let dead_handle = tokio::spawn(async {});
        rt.workers
            .register_oneshot("workflow_drive[dead]".to_string(), dead_handle, dead_flag);

        for _ in 0..100 {
            if rt.worker_liveness().live_oneshot == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }

        let l = rt.worker_liveness();
        assert_eq!(
            l.panic_total(),
            1,
            "只应把\"未正常收尾\"的那一个计为故障（正常收尾的不得计入）：{l:?}"
        );
        assert!(
            l.recent_faults.iter().any(|t| t == "workflow_drive[dead]"),
            "故障标签必须能定位到具体实例：{l:?}"
        );
        assert!(
            !l.recent_faults.iter().any(|t| t == "workflow_drive[ok]"),
            "正常收尾的 worker 不得出现在故障标签里：{l:?}"
        );
        assert_eq!(l.live_oneshot, 0, "两个一次性 worker 都已结束：{l:?}");

        // 内存有界：结算后登记表里不应残留任何一次性 worker
        assert!(
            rt.workers.inner.lock().expect("lock").oneshots.is_empty(),
            "已结束的一次性 worker 必须被结算掉（否则登记表会随实例数无界增长）"
        );
    }

    /// 内存有界：故障标签保留量有上限（登记表不得成为新的无界增长点）。
    #[tokio::test]
    async fn worker_liveness_bounds_recent_faults() {
        let rt = make_runtime();
        let n = MAX_RECENT_FAULTS + 5;
        for i in 0..n {
            let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let handle = tokio::spawn(async {});
            rt.workers
                .register_oneshot(format!("workflow_drive[f{i}]"), handle, flag);
            // 逐个结算，确保每次都进入故障分支
            for _ in 0..50 {
                if rt.worker_liveness().live_oneshot == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }
        let l = rt.worker_liveness();
        assert_eq!(
            l.panic_total(),
            n as u64,
            "计数必须完整（计数是有界的聚合，不随实例 id 增长）"
        );
        assert!(
            l.recent_faults.len() <= MAX_RECENT_FAULTS,
            "故障标签列表必须有界：{}",
            l.recent_faults.len()
        );
    }

    fn make_simple_definition() -> WorkflowDefinition {
        WorkflowDefinition {
            id: None,
            document: Document {
                dsl: "1.0.0".into(),
                namespace: "test".into(),
                name: "test-wf".into(),
                version: "1.0".into(),
                title: None,
                summary: None,
                tags: None,
            },
            do_tasks: vec![],
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

    #[test]
    fn test_find_task_index() {
        let tasks = vec![
            NamedTask {
                name: "step1".into(),
                task: Task::Wait(WaitTask {
                    wait: "PT1S".into(),
                }),
            },
            NamedTask {
                name: "step2".into(),
                task: Task::Wait(WaitTask {
                    wait: "PT2S".into(),
                }),
            },
        ];
        assert_eq!(find_task_index(&tasks, "step1"), Some(0));
        assert_eq!(find_task_index(&tasks, "step2"), Some(1));
        assert_eq!(find_task_index(&tasks, "nonexistent"), None);
    }

    #[tokio::test]
    async fn test_start_instance_saves_and_returns() {
        let runtime = make_runtime();
        let def = make_simple_definition();

        let inst = runtime
            .start(&def, serde_json::json!({"key": "value"}))
            .await
            .unwrap();

        // 标准相位：start 创建后为 Pending，drive 循环首步推进为 Running
        assert_eq!(inst.status, InstanceStatus::Pending);
        assert_eq!(inst.definition_name, "test-wf");
        assert_eq!(inst.context, serde_json::json!({"key": "value"}));
    }

    /// 负控制：legacy 降级发布口 `emit_lifecycle` 只 append 事件到 outbox，
    /// **不得**调用会写实例状态的 `AtomicOutboxCommit`（那是一条无 CAS/fencing 的
    /// 实例写通道，并会推进 mod_revision 使调用方随后的 CAS 失配）。
    #[tokio::test]
    async fn durable_emit_appends_without_writing_instance_state() {
        use crate::workflow::durable::{
            AtomicOutboxCommit, DurableSupport, MemoryOutboxStore, MemoryTimerStore, OutboxRecord,
        };
        use crate::workflow::model::WorkflowInstance;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        #[derive(Default)]
        struct RecordingCommit {
            calls: AtomicUsize,
        }

        #[async_trait::async_trait]
        impl AtomicOutboxCommit for RecordingCommit {
            async fn commit_state_and_outbox_batch(
                &self,
                _inst: &WorkflowInstance,
                _recs: &[OutboxRecord],
                _expected_mod_rev: Option<i64>,
            ) -> Result<bool, crate::workflow::ports::StoreError> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(true)
            }
        }

        let outbox = Arc::new(MemoryOutboxStore::new());
        let commit = Arc::new(RecordingCommit::default());
        let durable = DurableSupport::new(Arc::new(MemoryTimerStore::new()), outbox.clone())
            .with_atomic_commit(commit.clone());

        let rt = make_runtime().with_durable(durable);
        let def = make_simple_definition();
        let inst = WorkflowInstance::new(&def, serde_json::json!({}), 1000);
        rt.emit_lifecycle(lifecycle::WORKFLOW_STARTED, &inst).await;

        assert_eq!(
            commit.calls.load(Ordering::SeqCst),
            0,
            "emit 不得调用会写实例状态的 AtomicOutboxCommit（无 CAS/fencing 的写通道）"
        );
        assert!(
            !outbox.snapshot().is_empty(),
            "生命周期事件必须仍然写入 outbox（事件不丢）"
        );
    }

    /// 正控制：drive 的唯一写路径把状态 + 伴生事件经同一个 `AtomicOutboxCommit` 落库。
    ///
    /// 注入 `MemoryAtomicCommit` 的**计数代理**后驱动一个空工作流，断言实例到终态、
    /// outbox 拿到 started / completed 两个生命周期事件，且确实调用了原子提交
    /// （计数 ≥ 1）——若走成裸写，测试便能区分出来。
    #[tokio::test]
    async fn durable_drive_commits_state_and_events_atomically() {
        use crate::workflow::durable::{
            AtomicOutboxCommit, DurableSupport, MemoryAtomicCommit, MemoryOutboxStore,
            MemoryTimerStore, OutboxStore,
        };
        use crate::workflow::ports::{MemoryWorkflowStore, StoreError, WorkflowStore};
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        /// 计数代理：委托给 `MemoryAtomicCommit`，同时记录被调用次数，
        /// 以区分「走原子提交」与「走裸写」。
        struct CountingAtomicCommit {
            inner: MemoryAtomicCommit,
            calls: AtomicUsize,
        }

        #[async_trait::async_trait]
        impl AtomicOutboxCommit for CountingAtomicCommit {
            async fn commit_state_and_outbox_batch(
                &self,
                inst: &WorkflowInstance,
                recs: &[OutboxRecord],
                expected_mod_rev: Option<i64>,
            ) -> Result<bool, StoreError> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.inner
                    .commit_state_and_outbox_batch(inst, recs, expected_mod_rev)
                    .await
            }
        }

        let instances = Arc::new(MemoryWorkflowStore::new());
        let outbox = Arc::new(MemoryOutboxStore::new());
        let counting = Arc::new(CountingAtomicCommit {
            inner: MemoryAtomicCommit::new(Arc::clone(&instances), Arc::clone(&outbox)),
            calls: AtomicUsize::new(0),
        });
        let commit: Arc<dyn AtomicOutboxCommit> = counting.clone();
        let durable = DurableSupport::new(
            Arc::new(MemoryTimerStore::new()),
            Arc::clone(&outbox) as Arc<dyn OutboxStore>,
        )
        .with_atomic_commit(commit);

        let expr = ExpressionEvaluator::new();
        let clock = TestClock::new(1000);
        let executor = WorkflowExecutor::new(expr, TestClock::new(1000));
        let rt = WorkflowRuntime::new(
            executor,
            clock,
            Arc::clone(&instances),
            NoopTaskDispatcher,
            NoopEventProvider,
        )
        .with_durable(durable);

        let def = make_simple_definition();
        let inst = rt.start(&def, serde_json::json!({})).await.unwrap();

        // drive 是 spawn 的后台任务：等实例走到终态。
        for _ in 0..200 {
            if let Ok(Some(cur)) = instances.load_instance(&inst.id).await {
                if cur.status.is_terminal() {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let stored = instances.load_instance(&inst.id).await.unwrap().unwrap();
        assert!(
            stored.status.is_terminal(),
            "drive 必须把实例状态经原子提交落库"
        );
        let types: Vec<String> = outbox
            .snapshot()
            .into_iter()
            .map(|r| r.event_type)
            .collect();
        assert!(
            types.iter().any(|t| t.ends_with("workflow.started.v1")),
            "同一提交里必须含 started 事件: {types:?}"
        );
        assert!(
            types.iter().any(|t| t.ends_with("workflow.completed.v1")),
            "同一提交里必须含 completed 事件: {types:?}"
        );
        assert!(
            counting.calls.load(Ordering::SeqCst) >= 1,
            "drive 的状态写必须经 AtomicOutboxCommit（否则状态与事件可分离落库）"
        );
    }

    /// 取消必须把状态写与 `workflow.cancelled` 放进**同一次**提交：durable 装配下走
    /// `AtomicOutboxCommit`（状态 + outbox 同事务），而不是「先盲写状态、再补发事件」。
    ///
    /// 反（负控制）：旧实现（cache-first `load_instance` + 无条件 `save_instance`，且
    /// 不发事件）两条都不过：事件缺失，且无 CAS 的写会在并发下覆盖终态。本测试断言
    /// 事件随原子提交落地。
    #[tokio::test]
    async fn cancel_commits_state_and_cancelled_event_atomically() {
        use crate::workflow::durable::{
            AtomicOutboxCommit, DurableSupport, MemoryAtomicCommit, MemoryOutboxStore,
            MemoryTimerStore, OutboxRecord, OutboxStore,
        };
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        struct CountingAtomicCommit {
            inner: MemoryAtomicCommit,
            calls: AtomicUsize,
        }

        #[async_trait::async_trait]
        impl AtomicOutboxCommit for CountingAtomicCommit {
            async fn commit_state_and_outbox_batch(
                &self,
                inst: &WorkflowInstance,
                recs: &[OutboxRecord],
                expected_mod_rev: Option<i64>,
            ) -> Result<bool, StoreError> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.inner
                    .commit_state_and_outbox_batch(inst, recs, expected_mod_rev)
                    .await
            }
        }

        let instances = Arc::new(MemoryWorkflowStore::new());
        let outbox = Arc::new(MemoryOutboxStore::new());
        let counting = Arc::new(CountingAtomicCommit {
            inner: MemoryAtomicCommit::new(Arc::clone(&instances), Arc::clone(&outbox)),
            calls: AtomicUsize::new(0),
        });
        let durable = DurableSupport::new(
            Arc::new(MemoryTimerStore::new()),
            Arc::clone(&outbox) as Arc<dyn OutboxStore>,
        )
        .with_atomic_commit(counting.clone() as Arc<dyn AtomicOutboxCommit>);

        let expr = ExpressionEvaluator::new();
        let executor = WorkflowExecutor::new(expr, TestClock::new(1000));
        let rt = WorkflowRuntime::new(
            executor,
            TestClock::new(1000),
            Arc::clone(&instances),
            NoopTaskDispatcher,
            NoopEventProvider,
        )
        .with_durable(durable);

        // 非终态（Running）实例；扫描器只驱动 Suspended/Waiting，不会介入。
        let def = make_simple_definition();
        let mut inst = WorkflowInstance::new(&def, serde_json::json!({}), 1000);
        inst.status = InstanceStatus::Running;
        instances.save_instance(&inst).await.unwrap();

        let before = counting.calls.load(Ordering::SeqCst);
        rt.cancel_instance(&inst.id).await.unwrap();

        let stored = instances.load_instance(&inst.id).await.unwrap().unwrap();
        assert_eq!(stored.status, InstanceStatus::Cancelled);
        let types: Vec<String> = outbox
            .snapshot()
            .into_iter()
            .map(|r| r.event_type)
            .collect();
        assert!(
            types.iter().any(|t| t.ends_with("workflow.cancelled.v1")),
            "取消必须经同一原子提交发出 workflow.cancelled：{types:?}"
        );
        assert!(
            counting.calls.load(Ordering::SeqCst) > before,
            "取消的状态写必须经 AtomicOutboxCommit（状态与事件同一次提交）"
        );
    }

    /// 取消与完成的存储替身：`load_instance`（cache-first）返回陈旧快照，
    /// `load_instance_with_rev`（线性一致）返回真值。取消若走 cache-first 读，
    /// 就会用陈旧的非终态快照盲写，覆盖已提交的终态。
    struct StaleCacheStore {
        inner: Arc<MemoryWorkflowStore>,
        stale: std::sync::Mutex<Option<WorkflowInstance>>,
    }

    #[async_trait::async_trait]
    impl WorkflowStore for StaleCacheStore {
        async fn save_definition(&self, def: &WorkflowDefinition) -> Result<(), StoreError> {
            self.inner.save_definition(def).await
        }
        async fn load_definition(
            &self,
            namespace: &str,
            name: &str,
            version: &str,
        ) -> Result<Option<WorkflowDefinition>, StoreError> {
            self.inner.load_definition(namespace, name, version).await
        }
        async fn save_instance(&self, inst: &WorkflowInstance) -> Result<(), StoreError> {
            self.inner.save_instance(inst).await
        }
        async fn load_instance(&self, id: &str) -> Result<Option<WorkflowInstance>, StoreError> {
            if let Some(stale) = self.stale.lock().unwrap_or_else(|e| e.into_inner()).clone() {
                return Ok(Some(stale));
            }
            self.inner.load_instance(id).await
        }
        async fn load_instance_with_rev(
            &self,
            id: &str,
        ) -> Result<Option<(WorkflowInstance, i64)>, StoreError> {
            match self.inner.load_instance(id).await? {
                Some(inst) => Ok(Some((inst, 1))),
                None => Ok(None),
            }
        }
        async fn list_definitions(
            &self,
            namespace: &str,
            page_size: usize,
            page_token: Option<&str>,
        ) -> Result<Vec<WorkflowDefinition>, StoreError> {
            self.inner
                .list_definitions(namespace, page_size, page_token)
                .await
        }
        async fn list_instances(
            &self,
            namespace: Option<&str>,
            definition_name: Option<&str>,
            page_size: usize,
            page_token: Option<&str>,
        ) -> Result<(Vec<WorkflowInstance>, Option<String>), StoreError> {
            self.inner
                .list_instances(namespace, definition_name, page_size, page_token)
                .await
        }
        async fn save_resume_idempotency_key(
            &self,
            instance_id: &str,
            key: &str,
        ) -> Result<bool, StoreError> {
            self.inner
                .save_resume_idempotency_key(instance_id, key)
                .await
        }
    }

    /// 反（负控制）：取消不得覆盖已提交的终态。
    ///
    /// 确定性复现「取消 vs 完成」竞态：完成先提交（线性一致真值 = Completed），
    /// 而读缓存仍滞后在 Running。取消若走 cache-first `load_instance` 并盲写，就会把
    /// 已提交的 `Completed` 改成 `Cancelled`（消费者却已看到 `workflow.completed`）。
    /// 新实现经 `load_instance_with_rev` 读到真值并拒绝 ⇒ 状态不变、无事件。
    ///
    /// 反（负控制）：本测试的 store 故意让 `load_instance` 返回陈旧 Running；若取消改回
    /// cache-first 读，`inner` 会变成 Cancelled，断言失败。
    #[tokio::test]
    async fn cancel_cannot_clobber_committed_terminal_state() {
        use std::sync::Arc;

        let inner = Arc::new(MemoryWorkflowStore::new());
        let def = make_simple_definition();
        let mut running = WorkflowInstance::new(&def, serde_json::json!({}), 1000);
        running.status = InstanceStatus::Running;
        inner.save_instance(&running).await.unwrap();

        // 完成已提交（线性一致真值 = Completed），但读缓存仍滞后在 Running。
        let mut completed = running.clone();
        completed.status = InstanceStatus::Completed;
        completed.updated_at = 2000;
        inner.save_instance(&completed).await.unwrap();

        let store = StaleCacheStore {
            inner: Arc::clone(&inner),
            stale: std::sync::Mutex::new(Some(running.clone())),
        };

        let events = Arc::new(RecordingEventProvider::new());
        let expr = ExpressionEvaluator::new();
        let executor = WorkflowExecutor::new(expr, TestClock::new(1000));
        let rt = WorkflowRuntime::new(
            executor,
            TestClock::new(1000),
            store,
            NoopTaskDispatcher,
            Arc::clone(&events),
        );

        let err = rt.cancel_instance(&running.id).await.unwrap_err();
        assert!(
            matches!(err, RuntimeError::AlreadyCompleted(_)),
            "已终态实例的取消必须拒绝（AlreadyCompleted ⇒ 服务层 FailedPrecondition）：{err:?}"
        );
        let stored = inner.load_instance(&running.id).await.unwrap().unwrap();
        assert_eq!(
            stored.status,
            InstanceStatus::Completed,
            "取消不得用陈旧缓存快照覆盖已提交的终态"
        );
        assert_eq!(
            events.emitted_count(),
            0,
            "取消被拒绝时不得发出任何生命周期事件：{:?}",
            events.emitted_events.lock().unwrap()
        );
    }

    /// `register_step` 瞬时失败折叠为**可重试**失败：任务进 retry 而非立即 faulted；
    /// token 恢复后同一任务重试派发成功。
    ///
    /// fail-closed 不变量：register 未成功前不派发。
    #[tokio::test]
    async fn register_step_error_is_retried_not_faulted() {
        use crate::workflow::durable::{
            DurableSupport, MemoryOutboxStore, MemoryTimerStore, OutboxStore, StepRecord,
            StepState, StepTokenStore,
        };
        use crate::workflow::model::{CallTask, CallType, RetryPolicy, TaskMeta};
        use crate::workflow::ports::StoreError;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        #[derive(Default)]
        struct RecoveringStepStore {
            attempts: AtomicUsize,
        }

        #[async_trait::async_trait]
        impl StepTokenStore for RecoveringStepStore {
            async fn register(
                &self,
                _instance_id: &str,
                _step_id: &str,
                token: &str,
                now_ms: i64,
            ) -> Result<(StepRecord, bool), StoreError> {
                // 首次瞬时失败，之后恢复（模拟 KV 抖动）。
                if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(StoreError::IoError("kv blip".into()));
                }
                Ok((
                    StepRecord {
                        token: token.to_string(),
                        state: StepState::Pending,
                        at_ms: now_ms,
                    },
                    true,
                ))
            }
            async fn mark_done(&self, _i: &str, _s: &str) -> Result<(), StoreError> {
                Ok(())
            }
            async fn get(&self, _i: &str, _s: &str) -> Result<Option<StepRecord>, StoreError> {
                Ok(None)
            }
        }

        let steps = Arc::new(RecoveringStepStore::default());
        let durable = DurableSupport::new(
            Arc::new(MemoryTimerStore::new()),
            Arc::new(MemoryOutboxStore::new()) as Arc<dyn OutboxStore>,
        )
        .with_steps(steps.clone());

        let rt = make_runtime().with_durable(durable);
        let mut def = make_simple_definition();
        def.do_tasks = vec![NamedTask {
            name: "call".into(),
            task: Task::Call(CallTask {
                call: CallType::Function("svc".into()),
                with: None,
            }),
        }];
        def.task_meta.insert(
            "call".into(),
            TaskMeta {
                retry: Some(RetryPolicy {
                    delay: "PT0.1S".into(),
                    backoff: None,
                    limit: 2,
                    jitter: None,
                }),
                ..Default::default()
            },
        );
        rt.store.save_definition(&def).await.unwrap();

        let mut inst = WorkflowInstance::new(&def, serde_json::json!({}), 1000);
        inst.status = InstanceStatus::Running;
        rt.store.save_instance(&inst).await.unwrap();

        rt.drive_instance(&inst.id).await.unwrap();

        // 第一次 register 失败 ⇒ retry（不 fault）；恢复后重试成功。
        for _ in 0..300 {
            if let Ok(Some(cur)) = rt.store.load_instance(&inst.id).await {
                if cur.status.is_terminal() {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let cur = rt.store.load_instance(&inst.id).await.unwrap().unwrap();
        assert_eq!(
            cur.status,
            InstanceStatus::Completed,
            "register 瞬时失败必须重试后成功，而不是立即 faulted: {:?}",
            cur.fault
        );
        assert!(
            steps.attempts.load(Ordering::SeqCst) >= 2,
            "重试必须再次调用 register"
        );
    }

    #[tokio::test]
    async fn test_resume_nonexistent_instance() {
        let runtime = make_runtime();
        let result = runtime.resume("nonexistent", None, None, None).await;
        assert!(matches!(result, Err(RuntimeError::NotFound(_))));
    }

    // ─── emit EventProvider 集成测试 ───

    /// 验证 emit 任务不阻塞工作流推进，emit 帧被正确记录
    #[tokio::test]
    async fn test_emit_task_is_fire_and_forget() {
        use crate::workflow::model::{EmitEvent, EmitTask, SetTask};
        use std::sync::Arc;

        let expr = ExpressionEvaluator::new();
        let clock = TestClock::new(1000);
        let executor = WorkflowExecutor::new(expr, TestClock::new(1000));
        let store = Arc::new(MemoryWorkflowStore::new());
        let dispatcher = NoopTaskDispatcher;
        let event_provider = NoopEventProvider;

        let runtime = WorkflowRuntime::new(
            executor,
            clock,
            Arc::clone(&store),
            dispatcher,
            event_provider,
        );

        // emit → set: emit 是 fire-and-forget，不应阻塞后续 set 任务
        let def = WorkflowDefinition {
            id: None,
            document: Document {
                dsl: "1.0.0".into(),
                namespace: "test".into(),
                name: "emit-advance-wf".into(),
                version: "1.0".into(),
                title: None,
                summary: None,
                tags: None,
            },
            do_tasks: vec![
                NamedTask {
                    name: "emitEvent".into(),
                    task: Task::Emit(EmitTask {
                        emit: EmitEvent {
                            event_type: "ping".into(),
                            source: None,
                            data: None,
                        },
                    }),
                },
                NamedTask {
                    name: "setVar".into(),
                    task: Task::Set(SetTask {
                        variable: "status".into(),
                        value: "\"done\"".into(),
                    }),
                },
            ],
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
        };

        let inst = runtime
            .start(&def, serde_json::json!({}))
            .await
            .expect("start instance");

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let loaded = store
            .load_instance(&inst.id)
            .await
            .expect("load instance")
            .expect("instance exists");
        assert_eq!(loaded.status, InstanceStatus::Completed);

        // emit 帧在 task_stack[0]，set 在 task_stack[1]
        assert_eq!(loaded.task_stack.len(), 2);
        assert_eq!(loaded.task_stack[0].task_type, "emit");
        assert_eq!(loaded.task_stack[0].status, TaskStatus::Completed);
        assert_eq!(loaded.task_stack[1].task_type, "set");

        // emit 后 set 被执行，context 中应有 status 变量
        assert_eq!(loaded.context["status"], "done");
    }

    /// 验证 emit 任务帧包含正确的事件数据
    #[tokio::test]
    async fn test_emit_task_frame_contains_event_data() {
        use crate::workflow::model::{EmitEvent, EmitTask};
        use std::sync::Arc;

        let expr = ExpressionEvaluator::new();
        let clock = TestClock::new(1000);
        let executor = WorkflowExecutor::new(expr, TestClock::new(1000));
        let store = Arc::new(MemoryWorkflowStore::new());
        let dispatcher = NoopTaskDispatcher;
        let event_provider = NoopEventProvider;

        let runtime = WorkflowRuntime::new(
            executor,
            clock,
            Arc::clone(&store),
            dispatcher,
            event_provider,
        );

        let def = WorkflowDefinition {
            id: None,
            document: Document {
                dsl: "1.0.0".into(),
                namespace: "test".into(),
                name: "emit-data-wf".into(),
                version: "1.0".into(),
                title: None,
                summary: None,
                tags: None,
            },
            do_tasks: vec![NamedTask {
                name: "emitTask".into(),
                task: Task::Emit(EmitTask {
                    emit: EmitEvent {
                        event_type: "order.created".into(),
                        source: Some("/coord/orders".into()),
                        data: Some(serde_json::json!({"orderId": "ORD-123"})),
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
        };

        let inst = runtime
            .start(&def, serde_json::json!({}))
            .await
            .expect("start instance");

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let loaded = store
            .load_instance(&inst.id)
            .await
            .expect("load instance")
            .expect("instance exists");
        assert_eq!(loaded.status, InstanceStatus::Completed);

        // 验证 emit 帧的 output 包含事件数据
        let emit_frame = &loaded.task_stack[0];
        assert_eq!(emit_frame.task_type, "emit");
        let output = emit_frame.output.as_ref().expect("emit frame output");
        assert_eq!(output["event_type"], "order.created");
        assert_eq!(output["source"], "/coord/orders");
        assert_eq!(output["data"]["orderId"], "ORD-123");
    }

    // ─── fork 并行执行测试 ───

    #[tokio::test]
    async fn test_fork_parallel_execution() {
        use crate::workflow::model::{ForkBranch, ForkTask, SetTask};
        use std::sync::Arc;

        let expr = ExpressionEvaluator::new();
        let clock = TestClock::new(1000);
        let executor = WorkflowExecutor::new(expr, TestClock::new(1000));
        let store = Arc::new(MemoryWorkflowStore::new());
        let dispatcher = NoopTaskDispatcher;
        let event_provider = NoopEventProvider;

        let runtime = WorkflowRuntime::new(
            executor,
            clock,
            Arc::clone(&store),
            dispatcher,
            event_provider,
        );

        let def = WorkflowDefinition {
            id: None,
            document: Document {
                dsl: "1.0.0".into(),
                namespace: "test".into(),
                name: "fork-wf".into(),
                version: "1.0".into(),
                title: None,
                summary: None,
                tags: None,
            },
            do_tasks: vec![NamedTask {
                name: "parallelStep".into(),
                task: Task::Fork(ForkTask {
                    branches: vec![
                        ForkBranch {
                            name: "branchA".into(),
                            tasks: vec![NamedTask {
                                name: "setA".into(),
                                task: Task::Set(SetTask {
                                    variable: "a".into(),
                                    value: "\"A\"".into(),
                                }),
                            }],
                        },
                        ForkBranch {
                            name: "branchB".into(),
                            tasks: vec![NamedTask {
                                name: "setB".into(),
                                task: Task::Set(SetTask {
                                    variable: "b".into(),
                                    value: "\"B\"".into(),
                                }),
                            }],
                        },
                        ForkBranch {
                            name: "branchC".into(),
                            tasks: vec![NamedTask {
                                name: "setC".into(),
                                task: Task::Set(SetTask {
                                    variable: "c".into(),
                                    value: "\"C\"".into(),
                                }),
                            }],
                        },
                    ],
                    compete: None,
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
        };

        store.save_definition(&def).await.expect("save definition");

        let inst = runtime
            .start(&def, serde_json::json!({}))
            .await
            .expect("start instance");

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let loaded = store
            .load_instance(&inst.id)
            .await
            .expect("load instance")
            .expect("instance exists");
        assert_eq!(loaded.status, InstanceStatus::Completed);

        // 验证 fork 输出包含所有 3 个分支结果
        let fork_frame = &loaded.task_stack[0];
        assert_eq!(fork_frame.task_type, "fork");
        assert_eq!(fork_frame.status, TaskStatus::Completed);
        let output = fork_frame.output.as_ref().expect("fork output");
        let obj = output.as_object().expect("fork output should be object");
        assert!(
            obj.contains_key("branchA"),
            "missing branchA in fork results"
        );
        assert!(
            obj.contains_key("branchB"),
            "missing branchB in fork results"
        );
        assert!(
            obj.contains_key("branchC"),
            "missing branchC in fork results"
        );
    }

    #[tokio::test]
    async fn test_fork_compete_mode_first_wins() {
        use crate::workflow::model::{ForkBranch, ForkTask, SetTask};
        use std::sync::Arc;

        let expr = ExpressionEvaluator::new();
        let clock = TestClock::new(1000);
        let executor = WorkflowExecutor::new(expr, TestClock::new(1000));
        let store = Arc::new(MemoryWorkflowStore::new());
        let dispatcher = NoopTaskDispatcher;
        let event_provider = NoopEventProvider;

        let runtime = WorkflowRuntime::new(
            executor,
            clock,
            Arc::clone(&store),
            dispatcher,
            event_provider,
        );

        let def = WorkflowDefinition {
            id: None,
            document: Document {
                dsl: "1.0.0".into(),
                namespace: "test".into(),
                name: "fork-compete-wf".into(),
                version: "1.0".into(),
                title: None,
                summary: None,
                tags: None,
            },
            do_tasks: vec![NamedTask {
                name: "raceStep".into(),
                task: Task::Fork(ForkTask {
                    branches: vec![
                        ForkBranch {
                            name: "fastBranch".into(),
                            tasks: vec![NamedTask {
                                name: "fast".into(),
                                task: Task::Set(SetTask {
                                    variable: "winner".into(),
                                    value: "\"fast\"".into(),
                                }),
                            }],
                        },
                        ForkBranch {
                            name: "slowBranch".into(),
                            tasks: vec![NamedTask {
                                name: "slow".into(),
                                task: Task::Set(SetTask {
                                    variable: "winner".into(),
                                    value: "\"slow\"".into(),
                                }),
                            }],
                        },
                    ],
                    compete: Some(true),
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
        };

        store.save_definition(&def).await.expect("save definition");

        let inst = runtime
            .start(&def, serde_json::json!({}))
            .await
            .expect("start instance");

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let loaded = store
            .load_instance(&inst.id)
            .await
            .expect("load instance")
            .expect("instance exists");
        assert_eq!(loaded.status, InstanceStatus::Completed);

        // 验证 fork 已完成
        let fork_frame = &loaded.task_stack[0];
        assert_eq!(fork_frame.task_type, "fork");
        assert_eq!(fork_frame.status, TaskStatus::Completed);
    }

    #[tokio::test]
    async fn test_fork_empty_branches_completes() {
        use crate::workflow::model::ForkTask;
        use std::sync::Arc;

        let expr = ExpressionEvaluator::new();
        let clock = TestClock::new(1000);
        let executor = WorkflowExecutor::new(expr, TestClock::new(1000));
        let store = Arc::new(MemoryWorkflowStore::new());
        let dispatcher = NoopTaskDispatcher;
        let event_provider = NoopEventProvider;

        let runtime = WorkflowRuntime::new(
            executor,
            clock,
            Arc::clone(&store),
            dispatcher,
            event_provider,
        );

        let def = WorkflowDefinition {
            id: None,
            document: Document {
                dsl: "1.0.0".into(),
                namespace: "test".into(),
                name: "fork-empty-wf".into(),
                version: "1.0".into(),
                title: None,
                summary: None,
                tags: None,
            },
            do_tasks: vec![NamedTask {
                name: "emptyFork".into(),
                task: Task::Fork(ForkTask {
                    branches: vec![],
                    compete: None,
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
        };

        store.save_definition(&def).await.expect("save definition");

        let inst = runtime
            .start(&def, serde_json::json!({}))
            .await
            .expect("start instance");

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let loaded = store
            .load_instance(&inst.id)
            .await
            .expect("load instance")
            .expect("instance exists");
        assert_eq!(loaded.status, InstanceStatus::Completed);

        let fork_frame = &loaded.task_stack[0];
        assert_eq!(fork_frame.task_type, "fork");
        assert_eq!(fork_frame.status, TaskStatus::Completed);
        let output = fork_frame.output.as_ref().expect("fork output");
        let obj = output.as_object().expect("fork output should be object");
        assert!(obj.is_empty(), "empty fork should produce empty results");
    }

    // ═══ 全特性兼容测试（标准 Data Flow / Fault Tolerance / Status / Lifecycle） ═══

    use crate::workflow::model::{
        CallTask, CallType, InputConfig, OutputConfig, TaskMeta, TimeoutConfig,
    };
    use crate::workflow::ports::test_utils::RecordingEventProvider;

    fn make_definition_ext(
        name: &str,
        tasks: Vec<NamedTask>,
        input: Option<InputConfig>,
        output: Option<OutputConfig>,
        timeout: Option<TimeoutConfig>,
        task_meta: std::collections::HashMap<String, TaskMeta>,
    ) -> WorkflowDefinition {
        WorkflowDefinition {
            id: None,
            document: Document {
                dsl: "1.0.0".into(),
                namespace: "test".into(),
                name: name.into(),
                version: "1.0".into(),
                title: None,
                summary: None,
                tags: None,
            },
            do_tasks: tasks,
            input,
            output,
            timeout,
            use_components: None,
            schedule: Default::default(),
            auth: Default::default(),
            secrets: Default::default(),
            constants: Default::default(),
            task_meta,
            raw_yaml: None,
        }
    }

    /// 前 N 次失败（retryable）、之后成功的派发器
    struct FlakyTaskDispatcher {
        fail_times: u32,
        calls: std::sync::atomic::AtomicU32,
    }

    #[async_trait::async_trait]
    impl TaskDispatcher for FlakyTaskDispatcher {
        async fn dispatch(
            &self,
            _service: &str,
            _with: Option<&Value>,
            _input: &Value,
            _idempotency_key: Option<&str>,
        ) -> DispatchResult {
            let n = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n < self.fail_times {
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

    // ─── 工作流 input.default / input.from / input.schema ───

    #[tokio::test]
    async fn test_workflow_input_default_applied() {
        let runtime = make_runtime();
        let def = make_definition_ext(
            "input-default",
            vec![NamedTask {
                name: "setX".into(),
                task: Task::Set(SetTask {
                    variable: "probe".into(),
                    value: "\"set\"".into(),
                }),
            }],
            Some(InputConfig {
                schema: None,
                from: None,
                default: Some(serde_json::json!({"amount": 42})),
            }),
            None,
            None,
            Default::default(),
        );
        let inst = runtime.start(&def, serde_json::json!({})).await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let loaded = store_of(&runtime)
            .load_instance(&inst.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.status, InstanceStatus::Completed);
        assert_eq!(loaded.context["amount"], 42);
    }

    #[tokio::test]
    async fn test_workflow_input_from_transforms_context() {
        let runtime = make_runtime();
        let def = make_definition_ext(
            "input-from",
            vec![NamedTask {
                name: "setDone".into(),
                task: Task::Set(SetTask {
                    variable: "done".into(),
                    value: "\"yes\"".into(),
                }),
            }],
            Some(InputConfig {
                schema: None,
                from: Some("${ { \"amount\": (.raw + 1), \"kept\": .keep } }".into()),
                default: None,
            }),
            None,
            None,
            Default::default(),
        );
        let inst = runtime
            .start(&def, serde_json::json!({"raw": 41, "keep": true}))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let loaded = store_of(&runtime)
            .load_instance(&inst.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.status, InstanceStatus::Completed);
        // input.from 后 context = {amount: 42, kept: true}
        assert_eq!(loaded.context["amount"], 42);
        assert_eq!(loaded.context["kept"], true);
    }

    #[tokio::test]
    async fn test_workflow_input_schema_validation_faults() {
        let runtime = make_runtime();
        let def = make_definition_ext(
            "input-schema",
            vec![],
            Some(InputConfig {
                schema: Some(
                    r#"{"type":"object","required":["name"],"properties":{"name":{"type":"string"}}}"#
                        .into(),
                ),
                from: None,
                default: None,
            }),
            None,
            None,
            Default::default(),
        );
        let inst = runtime
            .start(&def, serde_json::json!({"amount": 5}))
            .await
            .unwrap();
        // 校验失败 → faulted（validation 错误）
        let loaded = store_of(&runtime)
            .load_instance(&inst.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.status, InstanceStatus::Failed);
        let fault = loaded.fault.unwrap();
        assert_eq!(
            fault.r#type,
            crate::workflow::errors::error_type(crate::workflow::errors::kind::VALIDATION)
        );
        assert_eq!(fault.status, 400);
        assert_eq!(fault.instance.as_deref(), Some("/input"));
    }

    // ─── 任务 if 条件跳过 ───

    #[tokio::test]
    async fn test_task_if_false_skips_task() {
        let runtime = make_runtime();
        let mut meta = std::collections::HashMap::new();
        meta.insert(
            "maybeSet".into(),
            TaskMeta {
                if_condition: Some("${ .approved == true }".into()),
                ..Default::default()
            },
        );
        let def = make_definition_ext(
            "if-skip",
            vec![NamedTask {
                name: "maybeSet".into(),
                task: Task::Set(SetTask {
                    variable: "flag".into(),
                    value: "\"executed\"".into(),
                }),
            }],
            None,
            None,
            None,
            meta,
        );
        let inst = runtime
            .start(&def, serde_json::json!({"approved": false}))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let loaded = store_of(&runtime)
            .load_instance(&inst.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.status, InstanceStatus::Completed);
        // 任务被跳过：flag 未设置，帧状态 Skipped
        assert!(loaded.context.get("flag").is_none());
        let frame = &loaded.task_stack[0];
        assert_eq!(frame.status, TaskStatus::Skipped);
    }

    // ─── 任务 input.from / output.as / export.as 数据流管线 ───

    #[tokio::test]
    async fn test_task_input_output_export_pipeline() {
        let runtime = make_runtime();
        let mut meta = std::collections::HashMap::new();
        // set 任务：input.from 变换输入；output.as 变换输出；export.as 合并回 context
        meta.insert(
            "transform".into(),
            TaskMeta {
                input: Some(InputConfig {
                    schema: None,
                    from: Some("${ { \"x\": (.amount + 5) } }".into()),
                    default: None,
                }),
                output: Some(OutputConfig {
                    as_expr: Some("${ { \"value\": $output } }".into()),
                    schema: None,
                }),
                export: Some(crate::workflow::model::ExportConfig {
                    as_expr: Some("${ . + {\"exported\": $output.value} }".into()),
                    schema: None,
                }),
                ..Default::default()
            },
        );
        let def = make_definition_ext(
            "pipeline",
            vec![NamedTask {
                name: "transform".into(),
                task: Task::Set(SetTask {
                    variable: "result".into(),
                    value: "${ .x }".into(),
                }),
            }],
            None,
            None,
            None,
            meta,
        );
        let inst = runtime
            .start(&def, serde_json::json!({"amount": 5}))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let loaded = store_of(&runtime)
            .load_instance(&inst.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            loaded.status,
            InstanceStatus::Completed,
            "context: {:?} fault: {:?}",
            loaded.context,
            loaded.fault
        );
        // set 任务：value = ${ .x }，但有效输入是 input.from 变换后的 {x: 10}
        // → context["result"] = 10
        assert_eq!(
            loaded.context["result"], 10,
            "context: {:?}",
            loaded.context
        );
        // export.as：context = context + {exported: 10}
        assert_eq!(
            loaded.context["exported"], 10,
            "context: {:?}",
            loaded.context
        );
        // 帧输出经 output.as 变换为 {value: 10}
        assert_eq!(
            loaded.task_stack[0].output.as_ref().unwrap(),
            &serde_json::json!({"value": 10})
        );
    }

    // ─── 工作流 output.as / output.schema ───

    #[tokio::test]
    async fn test_workflow_output_as_transforms() {
        let runtime = make_runtime();
        let def = make_definition_ext(
            "output-as",
            vec![NamedTask {
                name: "produce".into(),
                task: Task::Set(SetTask {
                    variable: "payload".into(),
                    value: "\"hello\"".into(),
                }),
            }],
            None,
            Some(OutputConfig {
                as_expr: Some("${ { \"message\": $context.payload, \"len\": 5 } }".into()),
                schema: None,
            }),
            None,
            Default::default(),
        );
        let inst = runtime.start(&def, serde_json::json!({})).await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let loaded = store_of(&runtime)
            .load_instance(&inst.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.status, InstanceStatus::Completed);
        let output = loaded.output.unwrap();
        assert_eq!(output["message"], "hello");
        assert_eq!(output["len"], 5);
    }

    // ─── retry 接线 ───

    #[tokio::test]
    async fn test_retry_wiring_on_retryable_failure() {
        use std::sync::Arc;
        let expr = ExpressionEvaluator::new();
        let clock = TestClock::new(1000);
        let executor = WorkflowExecutor::new(expr, TestClock::new(1000));
        let store = Arc::new(MemoryWorkflowStore::new());
        let dispatcher = FlakyTaskDispatcher {
            fail_times: 2,
            calls: std::sync::atomic::AtomicU32::new(0),
        };
        let event_provider = NoopEventProvider;
        let runtime = WorkflowRuntime::new(
            executor,
            clock,
            Arc::clone(&store),
            dispatcher,
            event_provider,
        );

        let mut meta = std::collections::HashMap::new();
        meta.insert(
            "callApi".into(),
            TaskMeta {
                retry: Some(crate::workflow::model::RetryPolicy {
                    delay: "PT0.01S".into(),
                    backoff: None,
                    limit: 5,
                    jitter: None,
                }),
                ..Default::default()
            },
        );
        let def = make_definition_ext(
            "retry-wf",
            vec![NamedTask {
                name: "callApi".into(),
                task: Task::Call(CallTask {
                    call: CallType::Http,
                    with: None,
                }),
            }],
            None,
            None,
            None,
            meta,
        );
        store.save_definition(&def).await.unwrap();

        let inst = runtime.start(&def, serde_json::json!({})).await.unwrap();
        // 前 2 次失败（10ms 间隔）→ 第 3 次成功
        tokio::time::sleep(Duration::from_millis(400)).await;
        let loaded = store.load_instance(&inst.id).await.unwrap().unwrap();
        assert_eq!(loaded.status, InstanceStatus::Completed);
        let frame = &loaded.task_stack[0];
        assert_eq!(frame.retry_count, 2);
    }

    #[tokio::test]
    async fn test_retry_exhausted_faults() {
        use std::sync::Arc;
        let expr = ExpressionEvaluator::new();
        let clock = TestClock::new(1000);
        let executor = WorkflowExecutor::new(expr, TestClock::new(1000));
        let store = Arc::new(MemoryWorkflowStore::new());
        let dispatcher = FlakyTaskDispatcher {
            fail_times: 100,
            calls: std::sync::atomic::AtomicU32::new(0),
        };
        let event_provider = NoopEventProvider;
        let runtime = WorkflowRuntime::new(
            executor,
            clock,
            Arc::clone(&store),
            dispatcher,
            event_provider,
        );

        let mut meta = std::collections::HashMap::new();
        meta.insert(
            "callApi".into(),
            TaskMeta {
                retry: Some(crate::workflow::model::RetryPolicy {
                    delay: "PT0.01S".into(),
                    backoff: None,
                    limit: 2,
                    jitter: None,
                }),
                ..Default::default()
            },
        );
        let def = make_definition_ext(
            "retry-exhaust",
            vec![NamedTask {
                name: "callApi".into(),
                task: Task::Call(CallTask {
                    call: CallType::Http,
                    with: None,
                }),
            }],
            None,
            None,
            None,
            meta,
        );
        store.save_definition(&def).await.unwrap();

        let inst = runtime.start(&def, serde_json::json!({})).await.unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        let loaded = store.load_instance(&inst.id).await.unwrap().unwrap();
        // 重试耗尽 → faulted（communication 错误）
        assert_eq!(loaded.status, InstanceStatus::Failed);
        let fault = loaded.fault.unwrap();
        assert_eq!(
            fault.r#type,
            crate::workflow::errors::error_type(crate::workflow::errors::kind::COMMUNICATION)
        );
        assert_eq!(fault.status, 502);
    }

    // ─── 工作流超时 ───

    #[tokio::test]
    async fn test_workflow_timeout_faults() {
        let runtime = make_runtime();
        let def = make_definition_ext(
            "timeout-wf",
            vec![NamedTask {
                name: "longWait".into(),
                task: Task::Wait(WaitTask {
                    wait: "PT10S".into(),
                }),
            }],
            None,
            None,
            Some(TimeoutConfig {
                after: "PT0.2S".into(),
            }),
            Default::default(),
        );
        let inst = runtime.start(&def, serde_json::json!({})).await.unwrap();
        // 工作流超时 200ms → 实例 faulted（timeout 错误 408）
        tokio::time::sleep(Duration::from_millis(600)).await;
        let loaded = store_of(&runtime)
            .load_instance(&inst.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.status, InstanceStatus::Failed);
        let fault = loaded.fault.unwrap();
        assert_eq!(
            fault.r#type,
            crate::workflow::errors::error_type(crate::workflow::errors::kind::TIMEOUT)
        );
        assert_eq!(fault.status, 408);
    }

    // ─── wait 任务 Waiting 相位 + 自动恢复 ───

    #[tokio::test]
    async fn test_wait_task_waiting_phase_auto_resume() {
        let runtime = make_runtime();
        let def = make_definition_ext(
            "wait-auto",
            vec![
                NamedTask {
                    name: "pause".into(),
                    task: Task::Wait(WaitTask {
                        wait: "PT0.05S".into(),
                    }),
                },
                NamedTask {
                    name: "after".into(),
                    task: Task::Set(SetTask {
                        variable: "done".into(),
                        value: "\"after-wait\"".into(),
                    }),
                },
            ],
            None,
            None,
            None,
            Default::default(),
        );
        let inst = runtime.start(&def, serde_json::json!({})).await.unwrap();

        // wait 期间：Waiting 相位
        tokio::time::sleep(Duration::from_millis(10)).await;
        let mid = store_of(&runtime)
            .load_instance(&inst.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mid.status, InstanceStatus::Waiting);
        assert_eq!(mid.suspension_meta.as_ref().unwrap().reason, "wait");

        // wait 到期后自动恢复 → 继续执行后续任务 → Completed
        tokio::time::sleep(Duration::from_millis(200)).await;
        let loaded = store_of(&runtime)
            .load_instance(&inst.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.status, InstanceStatus::Completed);
        assert_eq!(loaded.context["done"], "after-wait");
    }

    // ─── 子流程恢复的登记表路径 ───

    /// 负控制（双向）：
    /// 1. **登记表为空**时 `scan_pending_subflows` 不得改变任何实例状态
    ///    —— 这保证稳态 tick 只做登记表检查，不再全量列举/克隆实例；
    /// 2. **登记父子对**后，子流程已终结 ⇒ 父流程被恢复，并且该对被**注销**
    ///    —— 防止「恢复成功后每 5 秒重复恢复同一个父实例」。
    #[tokio::test]
    async fn test_subflow_recovery_uses_pending_registry_not_full_scan() {
        let runtime = make_runtime();
        let store = Arc::clone(store_of(&runtime));

        let parent_def =
            make_definition_ext("parent-wf", vec![], None, None, None, Default::default());
        let child_def =
            make_definition_ext("child-wf", vec![], None, None, None, Default::default());
        store.save_definition(&parent_def).await.unwrap();

        let mut parent = WorkflowInstance::new(&parent_def, serde_json::json!({}), 1000);
        parent.status = InstanceStatus::Waiting;
        parent.context["_subflow_instance_id"] = Value::String("child-1".into());
        let mut child = WorkflowInstance::new(&child_def, serde_json::json!({}), 1000);
        child.id = "child-1".into();
        child.status = InstanceStatus::Completed;
        child.output = Some(serde_json::json!({ "ok": true }));
        store.save_instance(&parent).await.unwrap();
        store.save_instance(&child).await.unwrap();

        // ① 空登记表：不触碰实例（稳态 O(1)）
        runtime.scan_pending_subflows(Arc::clone(&store)).await;
        let untouched = store.load_instance(&parent.id).await.unwrap().unwrap();
        assert_eq!(
            untouched.status,
            InstanceStatus::Waiting,
            "登记表为空时不得恢复任何实例（否则等于又回到全量扫描）"
        );

        // ② 登记父子对：子流程已终结 ⇒ 父流程恢复 + 注销
        runtime.register_pending_subflow(parent.id.clone(), child.id.clone());
        runtime.scan_pending_subflows(Arc::clone(&store)).await;

        let after = store.load_instance(&parent.id).await.unwrap().unwrap();
        assert_ne!(
            after.status,
            InstanceStatus::Waiting,
            "子流程已终结，父流程必须被恢复"
        );
        assert!(
            runtime.pending_subflows.lock().unwrap().is_empty(),
            "恢复成功后必须注销登记（否则会每 tick 重复恢复）"
        );
    }

    // ─── signal 校验 ───

    #[tokio::test]
    async fn test_signal_validation_mismatch_rejected() {
        use crate::workflow::model::ListenTask;
        let runtime = make_runtime();
        let def = make_definition_ext(
            "signal-wf",
            vec![NamedTask {
                name: "approve".into(),
                task: Task::Listen(ListenTask {
                    listen: crate::workflow::model::EventFilter {
                        event_type: Some("approval.requested".into()),
                        event_types: vec![],
                        source: None,
                        subject: None,
                    },
                }),
            }],
            None,
            None,
            None,
            Default::default(),
        );
        store_of(&runtime).save_definition(&def).await.unwrap();
        let inst = runtime.start(&def, serde_json::json!({})).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let loaded = store_of(&runtime)
            .load_instance(&inst.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.status, InstanceStatus::Waiting);
        assert_eq!(loaded.suspension_meta.as_ref().unwrap().reason, "listen");

        // signal 名与事件类型不匹配 → InvalidSignal
        let err = runtime
            .resume(&inst.id, Some("wrong.signal"), None, None)
            .await
            .unwrap_err();
        assert!(matches!(err, RuntimeError::InvalidSignal(_)));

        // 匹配的信号可恢复
        let resumed = runtime
            .resume(
                &inst.id,
                Some("approval.requested"),
                Some(serde_json::json!({"ok": true})),
                None,
            )
            .await
            .unwrap();
        assert_eq!(resumed.status, InstanceStatus::Running);
    }

    // ─── 多事件类型校验 + 手动 signal 注入 _event ───

    #[tokio::test]
    async fn test_signal_multi_event_type_validation_and_event_injection() {
        use crate::workflow::model::ListenTask;
        let runtime = make_runtime();
        let def = make_definition_ext(
            "multi-event-wf",
            vec![NamedTask {
                name: "approve".into(),
                task: Task::Listen(ListenTask {
                    listen: crate::workflow::model::EventFilter {
                        event_type: Some("icps.approval.approved".into()),
                        event_types: vec![
                            "icps.approval.approved".into(),
                            "icps.approval.rejected".into(),
                        ],
                        source: None,
                        subject: None,
                    },
                }),
            }],
            None,
            None,
            None,
            Default::default(),
        );
        store_of(&runtime).save_definition(&def).await.unwrap();
        let inst = runtime.start(&def, serde_json::json!({})).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let loaded = store_of(&runtime)
            .load_instance(&inst.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.status, InstanceStatus::Waiting);
        assert_eq!(loaded.suspension_meta.as_ref().unwrap().reason, "listen");

        // 非期望集合内的类型 → InvalidSignal
        let err = runtime
            .resume(&inst.id, Some("wrong.type"), None, None)
            .await
            .unwrap_err();
        assert!(matches!(err, RuntimeError::InvalidSignal(_)));

        // 期望集合内任一类型可恢复（多 onEvents 的 reject 分支），且同时注入 _signal 与 _event
        let resumed = runtime
            .resume(
                &inst.id,
                Some("icps.approval.rejected"),
                Some(serde_json::json!({"ok": false})),
                None,
            )
            .await
            .unwrap();
        assert_eq!(resumed.status, InstanceStatus::Running);
        assert_eq!(resumed.context["_signal"]["name"], "icps.approval.rejected");
        assert_eq!(
            resumed.context["_event"]["eventType"],
            "icps.approval.rejected"
        );
        assert_eq!(resumed.context["_event"]["arrived"], true);
    }

    // ─── signal 推进过挂起任务（端到端修复回归） ───

    #[tokio::test]
    async fn test_signal_advances_past_listen_task() {
        use crate::workflow::model::ListenTask;
        let runtime = make_runtime();
        let def = make_definition_ext(
            "advance-wf",
            vec![
                NamedTask {
                    name: "approve".into(),
                    task: Task::Listen(ListenTask {
                        listen: crate::workflow::model::EventFilter {
                            event_type: Some("icps.approval.approved".into()),
                            event_types: vec![],
                            source: None,
                            subject: None,
                        },
                    }),
                },
                NamedTask {
                    name: "after".into(),
                    task: Task::Set(SetTask {
                        variable: "done".into(),
                        value: "\"ok\"".into(),
                    }),
                },
            ],
            None,
            None,
            None,
            Default::default(),
        );
        store_of(&runtime).save_definition(&def).await.unwrap();
        let inst = runtime.start(&def, serde_json::json!({})).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let loaded = store_of(&runtime)
            .load_instance(&inst.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.status, InstanceStatus::Waiting);
        assert_eq!(loaded.suspension_meta.as_ref().unwrap().reason, "listen");

        let resumed = runtime
            .resume(
                &inst.id,
                Some("icps.approval.approved"),
                Some(serde_json::json!({"ok": true})),
                None,
            )
            .await
            .unwrap();
        assert_eq!(resumed.status, InstanceStatus::Running);

        // signal 后 drive 应从 listen 之后的下一任务继续，实例最终完成
        tokio::time::sleep(Duration::from_millis(200)).await;
        let done = store_of(&runtime)
            .load_instance(&inst.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            done.status,
            InstanceStatus::Completed,
            "signal should advance past listen and complete"
        );
        assert_eq!(done.context["done"], "ok");
    }

    // ─── listen 主动事件等待（标准 Events） ───

    #[tokio::test]
    async fn test_listen_task_active_event_resume() {
        use crate::workflow::model::ListenTask;
        use crate::workflow::ports::MemoryEventProvider;
        use std::sync::Arc;

        let expr = ExpressionEvaluator::new();
        let clock = TestClock::new(1000);
        let executor = WorkflowExecutor::new(expr, TestClock::new(1000));
        let store = Arc::new(MemoryWorkflowStore::new());
        let dispatcher = NoopTaskDispatcher;
        let events = Arc::new(MemoryEventProvider::new());
        let runtime = WorkflowRuntime::new(
            executor,
            clock,
            Arc::clone(&store),
            dispatcher,
            Arc::clone(&events),
        );

        let def = make_definition_ext(
            "listen-wf",
            vec![
                NamedTask {
                    name: "waitOrder".into(),
                    task: Task::Listen(ListenTask {
                        listen: crate::workflow::model::EventFilter {
                            event_type: Some("order.created".into()),
                            event_types: vec![],
                            source: None,
                            subject: None,
                        },
                    }),
                },
                NamedTask {
                    name: "after".into(),
                    task: Task::Set(SetTask {
                        variable: "done".into(),
                        value: "\"after-event\"".into(),
                    }),
                },
            ],
            None,
            None,
            None,
            Default::default(),
        );
        store.save_definition(&def).await.unwrap();

        let inst = runtime.start(&def, serde_json::json!({})).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        // listen 挂起：Waiting 相位
        let mid = store.load_instance(&inst.id).await.unwrap().unwrap();
        assert_eq!(mid.status, InstanceStatus::Waiting);
        assert_eq!(mid.suspension_meta.as_ref().unwrap().reason, "listen");

        // 事件到达 → 主动订阅自动恢复实例并继续执行
        events
            .emit(
                "order.created",
                Some("coord/orders"),
                &serde_json::json!({"orderId": "ORD-1"}),
            )
            .await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        let loaded = store.load_instance(&inst.id).await.unwrap().unwrap();
        assert_eq!(loaded.status, InstanceStatus::Completed);
        assert_eq!(loaded.context["done"], "after-event");
        assert_eq!(loaded.context["_event"]["arrived"], true);
    }

    // ─── 多 onEvents 路由：事件总线自动恢复按 arrived eventType 分支 ───

    #[tokio::test]
    async fn test_multi_on_events_routes_by_arrived_event_type() {
        use crate::workflow::ports::MemoryEventProvider;
        use std::sync::Arc;

        let expr = ExpressionEvaluator::new();
        let clock = TestClock::new(1000);
        let executor = WorkflowExecutor::new(expr, TestClock::new(1000));
        let store = Arc::new(MemoryWorkflowStore::new());
        let dispatcher = NoopTaskDispatcher;
        let events = Arc::new(MemoryEventProvider::new());
        let runtime = WorkflowRuntime::new(
            executor,
            clock,
            Arc::clone(&store),
            dispatcher,
            Arc::clone(&events),
        );

        // 编译多 onEvents 路由：两个目标各写不同 output，用于区分「正确路由」与「走缺省」
        let def = crate::workflow::sw::parse_cncf_sw_value(serde_json::json!({
            "id": "multi-event-wf", "version": "1.0", "start": "approve",
            "states": [
                { "name": "approve", "type": "event",
                  "onEvents": [
                      { "eventRefs": [ { "eventRef": "approved" } ], "transition": "notify" },
                      { "eventRefs": [ { "eventRef": "rejected" } ], "transition": "reject" }
                  ] },
                { "name": "notify", "type": "inject", "data": { "result": "approved" }, "end": true },
                { "name": "reject", "type": "inject", "data": { "result": "rejected" }, "end": true }
            ],
            "events": [
                { "name": "approved", "type": "icps.approval.approved" },
                { "name": "rejected", "type": "icps.approval.rejected" }
            ]
        }))
        .expect("multi-onEvents SW should compile");
        store.save_definition(&def).await.unwrap();

        let inst = runtime.start(&def, serde_json::json!({})).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mid = store.load_instance(&inst.id).await.unwrap().unwrap();
        assert_eq!(mid.status, InstanceStatus::Waiting);
        assert_eq!(mid.suspension_meta.as_ref().unwrap().reason, "listen");

        // 事件总线到达 rejected 类型 → 应路由到 reject 分支（而非缺省直通 end）
        events
            .emit("icps.approval.rejected", None, &serde_json::json!({}))
            .await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        let done = store.load_instance(&inst.id).await.unwrap().unwrap();
        assert_eq!(done.status, InstanceStatus::Completed);
        assert_eq!(
            done.context["result"], "rejected",
            "arrived event type must route to the reject branch, not the default branch"
        );
    }

    // ─── switch+eventConditions 路由：事件总线自动恢复按 arrived eventType 分支 ───

    #[tokio::test]
    async fn test_event_conditions_switch_routes_by_arrived_event_type() {
        use crate::workflow::ports::MemoryEventProvider;
        use std::sync::Arc;

        let expr = ExpressionEvaluator::new();
        let clock = TestClock::new(1000);
        let executor = WorkflowExecutor::new(expr, TestClock::new(1000));
        let store = Arc::new(MemoryWorkflowStore::new());
        let dispatcher = NoopTaskDispatcher;
        let events = Arc::new(MemoryEventProvider::new());
        let runtime = WorkflowRuntime::new(
            executor,
            clock,
            Arc::clone(&store),
            dispatcher,
            Arc::clone(&events),
        );

        let def = crate::workflow::sw::parse_cncf_sw_value(serde_json::json!({
            "id": "event-switch-wf", "version": "1.0", "start": "route",
            "states": [
                { "name": "route", "type": "switch",
                  "eventConditions": [
                      { "eventRef": "order.created", "transition": "created" },
                      { "eventRef": "order.cancelled", "transition": "cancelled" }
                  ],
                  "defaultCondition": { "end": true } },
                { "name": "created", "type": "inject", "data": { "result": "created" }, "end": true },
                { "name": "cancelled", "type": "inject", "data": { "result": "cancelled" }, "end": true }
            ]
        }))
        .expect("eventConditions switch SW should compile");
        store.save_definition(&def).await.unwrap();

        let inst = runtime.start(&def, serde_json::json!({})).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mid = store.load_instance(&inst.id).await.unwrap().unwrap();
        assert_eq!(mid.status, InstanceStatus::Waiting);

        events
            .emit("order.cancelled", None, &serde_json::json!({}))
            .await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        let done = store.load_instance(&inst.id).await.unwrap().unwrap();
        assert_eq!(done.status, InstanceStatus::Completed);
        assert_eq!(
            done.context["result"], "cancelled",
            "arrived event type must route to the matching eventCondition, not the default branch"
        );
    }

    // ─── listen 事件超时（标准 Events） ───

    #[tokio::test]
    async fn test_listen_event_timeout_faults() {
        use crate::workflow::model::ListenTask;
        use crate::workflow::ports::MemoryEventProvider;
        use std::sync::Arc;

        let expr = ExpressionEvaluator::new();
        let clock = TestClock::new(1000);
        let executor = WorkflowExecutor::new(expr, TestClock::new(1000));
        let store = Arc::new(MemoryWorkflowStore::new());
        let dispatcher = NoopTaskDispatcher;
        let events = Arc::new(MemoryEventProvider::new());
        let runtime = WorkflowRuntime::new(
            executor,
            clock,
            Arc::clone(&store),
            dispatcher,
            Arc::clone(&events),
        );

        let mut meta = std::collections::HashMap::new();
        meta.insert(
            "waitOrder".into(),
            TaskMeta {
                timeout: Some(TimeoutConfig {
                    after: "PT0.15S".into(),
                }),
                ..Default::default()
            },
        );
        let def = make_definition_ext(
            "listen-timeout-wf",
            vec![NamedTask {
                name: "waitOrder".into(),
                task: Task::Listen(ListenTask {
                    listen: crate::workflow::model::EventFilter {
                        event_type: Some("never.comes".into()),
                        event_types: vec![],
                        source: None,
                        subject: None,
                    },
                }),
            }],
            None,
            None,
            None,
            meta,
        );
        store.save_definition(&def).await.unwrap();

        let inst = runtime.start(&def, serde_json::json!({})).await.unwrap();
        // 事件超时 150ms → faulted（timeout 错误 408）
        tokio::time::sleep(Duration::from_millis(500)).await;
        let loaded = store.load_instance(&inst.id).await.unwrap().unwrap();
        assert_eq!(loaded.status, InstanceStatus::Failed);
        assert_eq!(
            loaded.fault.unwrap().r#type,
            crate::workflow::errors::error_type(crate::workflow::errors::kind::TIMEOUT)
        );
    }

    // ─── 生命周期事件 ───

    #[tokio::test]
    async fn test_lifecycle_events_emitted() {
        use std::sync::Arc;
        let expr = ExpressionEvaluator::new();
        let clock = TestClock::new(1000);
        let executor = WorkflowExecutor::new(expr, TestClock::new(1000));
        let store = Arc::new(MemoryWorkflowStore::new());
        let dispatcher = NoopTaskDispatcher;
        let events = Arc::new(RecordingEventProvider::new());
        let runtime = WorkflowRuntime::new(
            executor,
            clock,
            Arc::clone(&store),
            dispatcher,
            Arc::clone(&events),
        );

        let def = make_definition_ext(
            "lifecycle-wf",
            vec![NamedTask {
                name: "mark".into(),
                task: Task::Set(SetTask {
                    variable: "v".into(),
                    value: "1".into(),
                }),
            }],
            None,
            None,
            None,
            Default::default(),
        );
        let inst = runtime.start(&def, serde_json::json!({})).await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let loaded = store.load_instance(&inst.id).await.unwrap().unwrap();
        assert_eq!(loaded.status, InstanceStatus::Completed);

        let emitted = events.emitted_events.lock().unwrap().clone();
        let types: Vec<&str> = emitted.iter().map(|(t, _, _)| t.as_str()).collect();
        assert!(
            types.contains(&lifecycle::WORKFLOW_STARTED),
            "expected workflow.started in {types:?}"
        );
        assert!(
            types.contains(&lifecycle::WORKFLOW_COMPLETED),
            "expected workflow.completed in {types:?}"
        );
        assert!(
            types.contains(&lifecycle::TASK_COMPLETED),
            "expected task.completed in {types:?}"
        );
    }

    // ─── Pending → Running 相位 ───

    #[tokio::test]
    async fn test_pending_to_running_phase_transition() {
        use std::sync::Arc;
        let expr = ExpressionEvaluator::new();
        let clock = TestClock::new(1000);
        let executor = WorkflowExecutor::new(expr, TestClock::new(1000));
        let store = Arc::new(MemoryWorkflowStore::new());
        let dispatcher = NoopTaskDispatcher;
        let event_provider = NoopEventProvider;
        let runtime = WorkflowRuntime::new(
            executor,
            clock,
            Arc::clone(&store),
            dispatcher,
            event_provider,
        );
        // 长 wait 工作流确保实例停留在 Pending/Running 观察窗口
        let def = make_definition_ext(
            "phase-wf",
            vec![NamedTask {
                name: "pause".into(),
                task: Task::Wait(WaitTask {
                    wait: "PT10S".into(),
                }),
            }],
            None,
            None,
            None,
            Default::default(),
        );
        let inst = runtime.start(&def, serde_json::json!({})).await.unwrap();
        // start 返回 Pending（创建即 pending）
        assert_eq!(inst.status, InstanceStatus::Pending);
        // drive 首步推进为 Running，然后 wait 挂起为 Waiting
        tokio::time::sleep(Duration::from_millis(100)).await;
        let loaded = store.load_instance(&inst.id).await.unwrap().unwrap();
        assert_eq!(loaded.status, InstanceStatus::Waiting);
    }

    // ─── approve 节点 → Suspended 接线（v2 审批） ───

    use crate::workflow::approval::bridge::ApprovalBridge;
    use crate::workflow::approval::model::{BranchStatus, JoinPolicy};
    use crate::workflow::approval::store::{ApprovalStore, MemoryApprovalStore};
    use crate::workflow::model::ApproveTask;

    /// approve 节点的席位（具体审批人，保证节点有可批的席位）。
    fn approve_task(approvers: &[&str]) -> ApproveTask {
        ApproveTask {
            approvers: approvers.iter().map(|s| s.to_string()).collect(),
            pool: vec![],
            readers: vec![],
            roles: vec![],
            mode: JoinPolicy::All,
            veto: true,
            deadline: None,
            on_pass: None,
            on_reject: None,
            sign_admins: vec![],
            allow_self_approve: false,
        }
    }

    /// approve 节点 + 后继节点：后继节点用于断言挂起/故障时**未**继续执行。
    fn approve_definition(name: &str) -> WorkflowDefinition {
        make_definition_ext(
            name,
            vec![
                NamedTask {
                    name: "review".into(),
                    task: Task::Approve(approve_task(&["alice"])),
                },
                NamedTask {
                    name: "after".into(),
                    task: Task::Set(SetTask {
                        variable: "done".into(),
                        value: "\"after-approve\"".into(),
                    }),
                },
            ],
            None,
            None,
            None,
            Default::default(),
        )
    }

    /// 审批桥 + 其内存存储（固定时钟）。
    fn approval_bridge() -> (Arc<ApprovalBridge>, Arc<MemoryApprovalStore>) {
        let store = Arc::new(MemoryApprovalStore::new());
        let clock: Arc<dyn Clock> = Arc::new(TestClock::new(1000));
        let bridge = Arc::new(ApprovalBridge::new(store.clone(), clock, None));
        (bridge, store)
    }

    /// 轮询等待实例进入期望相位（drive 由 `start` spawn，等待有界）。
    async fn await_status(
        store: &MemoryWorkflowStore,
        id: &str,
        want: InstanceStatus,
    ) -> WorkflowInstance {
        for _ in 0..200 {
            let inst = store.load_instance(id).await.unwrap().expect("instance");
            if inst.status == want {
                return inst;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("instance {id} never reached {want:?}");
    }

    /// approve 节点：开审批实例 → 挂起为 `Suspended`（审批元数据 + 审批引擎落库）。
    #[tokio::test]
    async fn approve_node_suspends_with_approval_meta_and_engine_persisted() {
        let (bridge, approval_store) = approval_bridge();
        let runtime = make_runtime().with_approval_bridge(Arc::clone(&bridge));
        let def = approve_definition("approve-suspend-wf");

        let started = runtime
            .start(&def, serde_json::json!({"amount": 100}))
            .await
            .unwrap();
        let suspended =
            await_status(store_of(&runtime), &started.id, InstanceStatus::Suspended).await;

        // 标准相位 Suspended + 审批挂起元数据（恢复信号 / 审批实例 id 恒等于 workflow 实例 id）
        let meta = suspended.suspension_meta.as_ref().expect("suspension meta");
        assert_eq!(meta.reason, "approval");
        assert_eq!(meta.expected_signal.as_deref(), Some("approval.completed"));
        assert_eq!(
            meta.approval_instance_id.as_deref(),
            Some(started.id.as_str())
        );

        // 挂起帧：approve 节点仍在执行中，且未推进到后继节点
        let frame = suspended.task_stack.last().expect("approve frame");
        assert_eq!(frame.task_name, "review");
        assert_eq!(frame.task_type, "approve");
        assert_eq!(frame.status, TaskStatus::Running);
        assert_eq!(suspended.current_task_index, 0);
        assert!(
            suspended.context.get("done").is_none(),
            "approve 挂起后不得执行后继节点"
        );

        // 审批实例已持久化：一条 open 分支 + 一个可批席位
        let (engine, mod_rev) = approval_store
            .load(&started.id)
            .await
            .unwrap()
            .expect("approval instance persisted");
        assert_eq!(mod_rev, 1);
        assert_eq!(
            engine.instance.status,
            crate::workflow::approval::model::InstanceStatus::Running
        );
        // 实例未注入发起人 ⇒ 审批实例以 anonymous 起算（自审策略据此判定）
        assert_eq!(engine.instance.initiator, "anonymous");
        assert_eq!(engine.branches.len(), 1);
        let branch = engine.branches.values().next().unwrap();
        assert_eq!(branch.node, "review");
        assert_eq!(branch.status, BranchStatus::Open);
        assert_eq!(engine.rounds.len(), 1);
        let seat = engine
            .tasks
            .values()
            .find(|t| t.assignee.as_deref() == Some("alice"))
            .expect("seat for alice");
        assert!(seat.status.is_open());
    }

    /// 挂起后强制再驱动：approve 节点幂等复用既有 open 分支，不得重复开分支/轮次。
    #[tokio::test]
    async fn re_drive_after_suspend_does_not_double_open() {
        let (bridge, approval_store) = approval_bridge();
        let runtime = make_runtime().with_approval_bridge(bridge);
        let def = approve_definition("approve-redrive-wf");

        let started = runtime.start(&def, serde_json::json!({})).await.unwrap();
        await_status(store_of(&runtime), &started.id, InstanceStatus::Suspended).await;

        let (before, rev_before) = approval_store.load(&started.id).await.unwrap().unwrap();

        runtime
            .drive(
                started.id.clone(),
                def.clone(),
                Arc::clone(store_of(&runtime)),
                None,
            )
            .await;

        let (after, rev_after) = approval_store.load(&started.id).await.unwrap().unwrap();
        assert_eq!(rev_after, rev_before, "复用既有分支 ⇒ 审批实例零写入");
        assert_eq!(after.branches.len(), before.branches.len());
        assert_eq!(after.rounds.len(), before.rounds.len());
        assert_eq!(after.tasks.len(), before.tasks.len());

        // workflow 实例仍在 Suspended（重驱动不得把审批节点推进过去）
        let reloaded = store_of(&runtime)
            .load_instance(&started.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reloaded.status, InstanceStatus::Suspended);
        assert_eq!(reloaded.current_task_index, 0);
        assert!(reloaded.context.get("done").is_none());
    }

    /// 未注入审批桥：approve 节点没有可开的审批实例 ⇒ fault-closed，不得静默继续。
    #[tokio::test]
    async fn approve_node_without_bridge_faults_closed() {
        let runtime = make_runtime();
        let def = approve_definition("approve-no-bridge-wf");

        let started = runtime.start(&def, serde_json::json!({})).await.unwrap();
        let failed = await_status(store_of(&runtime), &started.id, InstanceStatus::Failed).await;

        let fault = failed.fault.as_ref().expect("fault");
        assert_eq!(
            fault.r#type,
            crate::workflow::errors::error_type(crate::workflow::errors::kind::INTERNAL)
        );
        assert!(
            fault.detail.contains("approval"),
            "fault 必须点明审批未开启：{fault:?}"
        );
        assert_eq!(failed.current_task_index, 0);
        assert!(
            failed.context.get("done").is_none(),
            "faulted 后不得执行后继节点"
        );
    }

    /// v1 回归：注入审批桥不改变非 approve 定义的语义（仍跑到 Completed）。
    #[tokio::test]
    async fn non_approve_definition_completes_with_bridge_injected() {
        let (bridge, approval_store) = approval_bridge();
        let runtime = make_runtime().with_approval_bridge(bridge);
        let def = make_definition_ext(
            "v1-unaffected-wf",
            vec![NamedTask {
                name: "finish".into(),
                task: Task::Set(SetTask {
                    variable: "done".into(),
                    value: "\"v1\"".into(),
                }),
            }],
            None,
            None,
            None,
            Default::default(),
        );

        let started = runtime.start(&def, serde_json::json!({})).await.unwrap();
        let done = await_status(store_of(&runtime), &started.id, InstanceStatus::Completed).await;

        assert_eq!(done.context["done"], "v1");
        assert!(
            done.suspension_meta.is_none(),
            "非 approve 定义不受审批接线影响"
        );
        assert!(
            approval_store.load(&started.id).await.unwrap().is_none(),
            "非 approve 定义不得开出审批实例"
        );
    }

    /// 从 runtime 中取出 store（测试辅助）
    fn store_of<E, C, S, D, B>(runtime: &WorkflowRuntime<E, C, S, D, B>) -> &Arc<S>
    where
        E: ExpressionEval,
        C: Clock,
        S: WorkflowStore,
        D: TaskDispatcher,
        B: EventProvider,
    {
        &runtime.store
    }

    // ─── fail_instance 有界 CAS 重试（可控冲突的假存储） ───

    /// 可控 CAS 冲突的存储：期望修订号匹配时，前 `conflicts_remaining` 次返回冲突并推进
    /// 存储修订号（模拟并发写者抢先提交）；其余读写转发给内存存储。
    struct ConflictOnceStore {
        inner: MemoryWorkflowStore,
        rev: std::sync::atomic::AtomicI64,
        conflicts_remaining: std::sync::atomic::AtomicUsize,
    }

    impl ConflictOnceStore {
        fn new(conflicts: usize) -> Self {
            Self {
                inner: MemoryWorkflowStore::new(),
                rev: std::sync::atomic::AtomicI64::new(0),
                conflicts_remaining: std::sync::atomic::AtomicUsize::new(conflicts),
            }
        }
    }

    #[async_trait::async_trait]
    impl WorkflowStore for ConflictOnceStore {
        async fn save_definition(&self, def: &WorkflowDefinition) -> Result<(), StoreError> {
            self.inner.save_definition(def).await
        }

        async fn load_definition(
            &self,
            namespace: &str,
            name: &str,
            version: &str,
        ) -> Result<Option<WorkflowDefinition>, StoreError> {
            self.inner.load_definition(namespace, name, version).await
        }

        async fn save_instance(&self, inst: &WorkflowInstance) -> Result<(), StoreError> {
            self.inner.save_instance(inst).await
        }

        async fn load_instance(&self, id: &str) -> Result<Option<WorkflowInstance>, StoreError> {
            self.inner.load_instance(id).await
        }

        async fn list_definitions(
            &self,
            namespace: &str,
            page_size: usize,
            page_token: Option<&str>,
        ) -> Result<Vec<WorkflowDefinition>, StoreError> {
            self.inner
                .list_definitions(namespace, page_size, page_token)
                .await
        }

        async fn list_instances(
            &self,
            namespace: Option<&str>,
            definition_name: Option<&str>,
            page_size: usize,
            page_token: Option<&str>,
        ) -> Result<(Vec<WorkflowInstance>, Option<String>), StoreError> {
            self.inner
                .list_instances(namespace, definition_name, page_size, page_token)
                .await
        }

        async fn save_resume_idempotency_key(
            &self,
            instance_id: &str,
            key: &str,
        ) -> Result<bool, StoreError> {
            self.inner
                .save_resume_idempotency_key(instance_id, key)
                .await
        }

        async fn load_instance_with_rev(
            &self,
            id: &str,
        ) -> Result<Option<(WorkflowInstance, i64)>, StoreError> {
            use std::sync::atomic::Ordering;
            match self.inner.load_instance(id).await? {
                Some(inst) => Ok(Some((inst, self.rev.load(Ordering::SeqCst)))),
                None => Ok(None),
            }
        }

        async fn save_instance_atomic(
            &self,
            inst: &WorkflowInstance,
            expected_mod_rev: i64,
        ) -> Result<bool, StoreError> {
            use std::sync::atomic::Ordering;
            if expected_mod_rev != self.rev.load(Ordering::SeqCst) {
                return Ok(false);
            }
            if self
                .conflicts_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                // 模拟并发写者抢先提交：修订号前进，本次 CAS 失去写权。
                self.rev.fetch_add(1, Ordering::SeqCst);
                return Ok(false);
            }
            self.inner.save_instance(inst).await?;
            self.rev.fetch_add(1, Ordering::SeqCst);
            Ok(true)
        }
    }

    fn runtime_with_conflict_store(
        conflicts: usize,
    ) -> WorkflowRuntime<
        ExpressionEvaluator,
        TestClock,
        ConflictOnceStore,
        NoopTaskDispatcher,
        NoopEventProvider,
    > {
        let executor = WorkflowExecutor::new(ExpressionEvaluator::new(), TestClock::new(1000));
        WorkflowRuntime::new(
            executor,
            TestClock::new(1000),
            ConflictOnceStore::new(conflicts),
            NoopTaskDispatcher,
            NoopEventProvider,
        )
    }

    fn running_instance(id: &str) -> WorkflowInstance {
        WorkflowInstance {
            id: id.into(),
            definition_ns: "test".into(),
            definition_name: "wf".into(),
            definition_version: "1.0".into(),
            status: InstanceStatus::Running,
            context: serde_json::json!({}),
            task_stack: vec![],
            current_task_index: 0,
            created_at: 0,
            updated_at: 0,
            output: None,
            fault: None,
            suspension_meta: None,
            initiator: None,
            definition_hash: None,
            engine_version: None,
        }
    }

    /// 一次 CAS 冲突后 `fail_instance` 重读修订号并重提 ⇒ 终态写落地，冲突计数为 1。
    #[tokio::test]
    async fn fail_instance_recommits_after_one_cas_conflict() {
        let rt = runtime_with_conflict_store(1);
        let mut inst = running_instance("inst-cas-retry");
        store_of(&rt).save_instance(&inst).await.unwrap();
        let mut rev = Some(0);

        rt.fail_instance(
            &mut inst,
            store_of(&rt),
            &mut rev,
            None,
            WorkflowFault::timeout("t", "d"),
        )
        .await;

        let saved = store_of(&rt)
            .load_instance(&inst.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            saved.status,
            InstanceStatus::Failed,
            "冲突重提后终态写必须落地"
        );
        assert!(saved.fault.is_some());
        assert_eq!(rt.cas_conflict_total(), 1, "一次冲突只计一次");
    }

    /// 冲突耗尽 ⇒ 实例保持非终态（由启动对账重新驱动），冲突全部计入 `cas_conflict_total`。
    #[tokio::test]
    async fn fail_instance_exhaustion_leaves_instance_non_terminal() {
        let rt = runtime_with_conflict_store(usize::MAX);
        let mut inst = running_instance("inst-cas-exhausted");
        store_of(&rt).save_instance(&inst).await.unwrap();
        let mut rev = Some(0);

        rt.fail_instance(
            &mut inst,
            store_of(&rt),
            &mut rev,
            None,
            WorkflowFault::timeout("t", "d"),
        )
        .await;

        let saved = store_of(&rt)
            .load_instance(&inst.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            saved.status,
            InstanceStatus::Running,
            "耗尽后不得留下伪终态"
        );
        assert_eq!(
            rt.cas_conflict_total(),
            CONTROL_PLANE_CAS_ATTEMPTS as u64,
            "每次失败尝试都必须计数"
        );
    }
}
