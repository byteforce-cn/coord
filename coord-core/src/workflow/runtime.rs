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

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

use super::engine::WorkflowExecutor;
use super::model::{
    InstanceStatus, StepResult, SuspendReason, SuspensionMeta, TaskFrame, TaskStatus,
    WorkflowDefinition, WorkflowFault, WorkflowInstance,
};
use super::ports::{
    Clock, DispatchResult, EventProvider, ExpressionEval, TaskDispatcher, WorkflowStore,
};
use super::retry::{RetryConfig, RetryScheduler};

/// 子流程恢复扫描器的 tick 间隔（秒）。
///
/// W1-4：tick 本身不再做全量列举（见 `pending_subflows` 字段注释），
/// 所以固定 5s 不再有实例规模相关的代价。
const SUBFLOW_SCAN_INTERVAL_SECS: u64 = 5;

// ─── 后台任务存活登记（W5-4「能力死亡必须可观测」） ───

/// 长活后台任务的**存活事实**（只读快照，供出口/指标拉取）。
///
/// 为什么需要它：`coord-core` 没有 supervisor，也没有 `tracing`，所以一个 `tokio::spawn`
/// 出去的后台循环一旦结束（或 panic），**没有任何路径会告诉任何人** —— 第四轮 §6.3.6
/// 把这种形态叫「能力静默死亡」。本结构把"死亡"变成可拉取的事实。
///
/// 两类判据（**不能混为一谈**）：
///
/// * [`Self::finished_loops`] —— **循环型** worker（如子流程扫描器）：它们的正常行为是
///   永不结束，所以"已结束"**本身**就是缺陷（`JoinHandle::is_finished()` 即可判定，
///   且它同时覆盖"返回了"与"panic 了"两种死法）。
/// * [`Self::panicked`] —— **一次性** worker（如某实例的 `drive`）：正常结束是它的**预期**
///   行为（挂起 / 终态即返回），所以"结束"不是证据。判据是"结束了 **但没跑到最后一行**"
///   —— 任务的末尾会置一个 `completed` 标志，没置位就说明中途死了。
///   把两者合成一个数字会让指标长期噪声化，反而没人看（"红色的门禁是教人忽略的门禁"）。
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

    /// W5-4 负控制入口：注入一个"已死"的循环 worker，从而不必真的把生产任务弄死
    /// 就能验证"死亡可观测"这条判据本身有效。
    #[cfg(test)]
    fn register_loop_for_test(&self, name: &str, handle: tokio::task::JoinHandle<()>) {
        self.register_loop(name, handle);
    }
}

// ─── 生命周期事件（标准 §Lifecycle Events） ───

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

// ─── 运行时错误 ───
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
        }
    }
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
    /// W1-4（第四轮 §3.10 j）：待恢复的父实例登记表（`parent_id → subflow_id`）。
    ///
    /// 此前子流程恢复靠**每 5 秒** `list_instances(None, None, usize::MAX, None)`
    /// —— 稳态下每次 tick 都把**全部**工作流实例（包括与子流程完全无关的）
    /// 列出并反序列化/克隆一遍，代价随实例总数**线性增长**，且 `WorkflowStore`
    /// 的 `list_instances` 签名不返回翻页 token，无法安全分页（截断会让超出一页的
    /// 挂起实例**永不恢复**）。现改为：父流程因 `RunSubflow` 挂起时**登记**，
    /// 每 tick 只检查登记表里的父子对（登记表为空 ⇒ O(1)）。启动时仍做**一次**
    /// 全量对账，用于恢复上个进程遗留的挂起实例。
    pending_subflows: Arc<std::sync::Mutex<std::collections::HashMap<String, String>>>,
    /// W5-4：长活后台任务的存活登记（见 [`WorkerLiveness`]）。
    ///
    /// 没有它，`spawn` 出去的后台任务死亡时**没有任何出口**：`coord-core` 无 supervisor、
    /// 无 `tracing`，调用方也拿不到 join handle。出口由上层（`coord-agent`）周期性拉取。
    workers: Arc<WorkerRegistry>,
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
        };
        // 启动后台扫描器，定期检查 RunSubflow 挂起的父流程并恢复
        rt.start_subflow_scanner();
        rt
    }

    /// 长活后台任务的存活快照（W5-4「能力死亡必须可观测」）。
    ///
    /// 语义见 [`WorkerLiveness`]：循环型 worker 的"已结束"、一次性 worker 的 panic
    /// 才是缺陷；一次性 worker 的正常结束**不是**。
    ///
    /// 出口归上层调用方（`coord-agent` 的指标/告警）——`coord-core` 不持有任何
    /// 遥测依赖，也不该持有。
    pub fn worker_liveness(&self) -> WorkerLiveness {
        self.workers.snapshot()
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
            rt.drive(instance_id, definition, store).await;
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
            Ok(list) => list,
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
                .resume_parent_after_subflow(parent, &sub_inst, &store)
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
        mut parent: WorkflowInstance,
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
            "payload": signal_payload,
        });
        parent.updated_at = self.clock.now_ms();

        if store.save_instance(&parent).await.is_err() {
            return false;
        }
        self.emit_lifecycle(lifecycle::TASK_COMPLETED, &parent)
            .await;
        self.emit_lifecycle(lifecycle::WORKFLOW_RESUMED, &parent)
            .await;

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
    /// 1. 应用工作流 `input.default` / `input.from` / `input.schema`（标准 §Data Flow）
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

        // 工作流级超时接线（标准 §Fault Tolerance）：超时 → timeout 错误（408）→ faulted
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

    /// 任务完成后的输出/导出管线（标准 §Data Flow，对 Completed 帧调用）
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

    /// 发布生命周期 CloudEvent（标准 §Lifecycle Events）
    async fn emit_lifecycle(&self, event_type: &str, inst: &WorkflowInstance) {
        let data = serde_json::json!({
            "workflowId": inst.id,
            "workflowName": inst.definition_name,
            "workflowVersion": inst.definition_version,
            "namespace": inst.definition_ns,
            "status": format!("{:?}", inst.status).to_lowercase(),
            "output": inst.output,
            "fault": inst.fault,
        });
        self.event_provider
            .emit(event_type, Some("coord/workflow"), &data)
            .await;
    }

    /// 调度自动恢复：delay 后按 reason 恢复挂起实例
    ///
    /// - reason="wait"：完成当前 wait 帧并推进到下一任务
    /// - reason="retry"：重新执行当前任务（重试）
    fn schedule_auto_resume(
        &self,
        instance_id: String,
        definition: WorkflowDefinition,
        store: Arc<S>,
        delay_ms: u64,
        reason: String,
    ) {
        let rt = self.clone_runtime();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            rt.auto_resume(instance_id, definition, store, &reason)
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
    ) {
        let mut inst = match store.load_instance(&instance_id).await {
            Ok(Some(i)) => i,
            _ => return,
        };
        // 仅当仍处于同一挂起原因才恢复（避免竞态覆盖已完成的实例）
        if inst.status != InstanceStatus::Waiting {
            return;
        }
        let meta = match &inst.suspension_meta {
            Some(m) => m.clone(),
            None => return,
        };
        if meta.reason != reason {
            return;
        }

        if reason == "wait" {
            // 完成 wait 帧并推进
            if let Some(last) = inst.task_stack.last_mut() {
                last.status = TaskStatus::Completed;
                last.ended_at = Some(self.clock.now_ms());
            }
            inst.current_task_index += 1;
            inst.status = InstanceStatus::Running;
            inst.suspension_meta = None;
            inst.updated_at = self.clock.now_ms();
            let _ = store.save_instance(&inst).await;
            self.emit_lifecycle(lifecycle::TASK_COMPLETED, &inst).await;
            self.emit_lifecycle(lifecycle::WORKFLOW_RESUMED, &inst)
                .await;
            self.drive(instance_id, definition, store).await;
        } else if reason == "retry" {
            // 重试：直接重新 drive（重新执行当前任务）
            self.drive(instance_id, definition, store).await;
        }
    }

    /// 事件驱动的自动恢复（标准 §Events：listen 主动订阅，事件到达 → 恢复）
    ///
    /// `arrived_event_type`：实际到达的事件类型（多类型监听时用于路由）
    async fn resume_by_event(
        &self,
        instance_id: String,
        definition: WorkflowDefinition,
        store: Arc<S>,
        arrived_event_type: String,
    ) {
        let mut inst = match store.load_instance(&instance_id).await {
            Ok(Some(i)) => i,
            _ => return,
        };
        if inst.status != InstanceStatus::Waiting {
            return;
        }
        let meta = match &inst.suspension_meta {
            Some(m) => m.clone(),
            None => return,
        };
        if meta.reason != "listen" {
            return;
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
        let _ = store.save_instance(&inst).await;
        self.emit_lifecycle(lifecycle::TASK_COMPLETED, &inst).await;
        self.emit_lifecycle(lifecycle::WORKFLOW_RESUMED, &inst)
            .await;

        // 后台 drive（与子流程扫描器同模式，避免在事件等待任务内嵌套 drive；
        // 受监督：见 [`Self::spawn_drive`]）
        self.spawn_drive(instance_id, definition, store);
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
        let rt = self.clone_runtime();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(after_ms)).await;
            rt.apply_timeout(instance_id, store, expected_index).await;
        });
    }

    /// 超时到期处理
    async fn apply_timeout(
        &self,
        instance_id: String,
        store: Arc<S>,
        expected_index: Option<usize>,
    ) {
        let mut inst = match store.load_instance(&instance_id).await {
            Ok(Some(i)) => i,
            _ => return,
        };
        if inst.status.is_terminal() {
            return;
        }
        // 任务级超时：仅当实例仍停留同一任务索引才 fault
        if let Some(idx) = expected_index {
            if inst.current_task_index != idx {
                return;
            }
        }
        let fault = crate::workflow::errors::WorkflowFault::timeout(
            "workflow timeout",
            "the workflow exceeded its configured timeout",
        );
        inst.status = InstanceStatus::Failed;
        inst.fault = Some(fault);
        inst.updated_at = self.clock.now_ms();
        let _ = store.save_instance(&inst).await;
        self.emit_lifecycle(lifecycle::WORKFLOW_FAULTED, &inst)
            .await;
    }

    /// 将实例置为 faulted（Failed + fault + 生命周期事件）
    async fn fail_instance(
        &self,
        inst: &mut WorkflowInstance,
        store: Arc<S>,
        fault: WorkflowFault,
    ) {
        inst.status = InstanceStatus::Failed;
        inst.fault = Some(fault);
        inst.updated_at = self.clock.now_ms();
        let _ = store.save_instance(inst).await;
        self.emit_lifecycle(lifecycle::WORKFLOW_FAULTED, inst).await;
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
        let mut inst = self
            .store
            .load_instance(instance_id)
            .await
            .map_err(|e| RuntimeError::StoreError(e.to_string()))?
            .ok_or_else(|| RuntimeError::NotFound(instance_id.to_string()))?;

        if !inst.status.is_resumable() {
            return Err(RuntimeError::AlreadyCompleted(instance_id.to_string()));
        }

        // signal/事件校验（标准 §3.4）：signal 名必须匹配挂起实例的期望
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

        self.store
            .save_instance(&inst)
            .await
            .map_err(|e| RuntimeError::StoreError(e.to_string()))?;

        self.emit_lifecycle(lifecycle::WORKFLOW_RESUMED, &inst)
            .await;

        // 需要加载 definition
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

        // 后台 drive（受监督：见 [`Self::spawn_drive`]）
        self.spawn_drive(inst.id.clone(), def, Arc::clone(&self.store));

        Ok(inst)
    }

    /// 执行驱动循环 —— 在 spawn 后独立运行
    ///
    /// 以 BoxFuture 返回（而非 async fn），避免递归异步造成的 opaque 类型循环。
    pub(crate) fn drive(
        &self,
        instance_id: String,
        definition: WorkflowDefinition,
        store: Arc<S>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            'drive_loop: loop {
                // 加载最新实例状态
                let mut inst = match store.load_instance(&instance_id).await {
                    Ok(Some(i)) => i,
                    Ok(None) => break,
                    Err(_) => break,
                };

                // 终端状态则退出
                if inst.status.is_terminal() {
                    break;
                }

                // 标准相位：Pending → Running（start 后首步推进）
                if inst.status == InstanceStatus::Pending {
                    inst.status = InstanceStatus::Running;
                    inst.updated_at = self.clock.now_ms();
                    let _ = store.save_instance(&inst).await;
                    self.emit_lifecycle(lifecycle::WORKFLOW_STARTED, &inst)
                        .await;
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
                        if frame.status == TaskStatus::Completed {
                            if let Err(fault) =
                                self.apply_task_output(&mut inst, &definition, &mut frame)
                            {
                                inst.task_stack.push(frame);
                                self.fail_instance(&mut inst, store.clone(), fault).await;
                                break;
                            }
                            self.emit_lifecycle(lifecycle::TASK_COMPLETED, &inst).await;
                        }
                        inst.task_stack.push(frame);
                        inst.current_task_index += 1;
                        inst.updated_at = self.clock.now_ms();
                        let _ = store.save_instance(&inst).await;
                    }

                    StepResult::Goto { target, frame } => {
                        inst.task_stack.push(frame);
                        match find_task_index(&definition.do_tasks, &target) {
                            Some(idx) => {
                                inst.current_task_index = idx;
                                inst.updated_at = self.clock.now_ms();
                                let _ = store.save_instance(&inst).await;
                            }
                            None => {
                                inst.status = InstanceStatus::Failed;
                                inst.fault = Some(crate::workflow::errors::WorkflowFault::not_found(
                                format!("switch goto target '{}' not found", target),
                                "The switch condition referenced a task that does not exist in do_tasks",
                            ));
                                inst.updated_at = self.clock.now_ms();
                                let _ = store.save_instance(&inst).await;
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
                                let result = self
                                    .dispatcher
                                    .dispatch(service, with.as_ref(), input)
                                    .await;

                                match result {
                                    DispatchResult::Success { data } => {
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
                                            self.fail_instance(&mut inst, store.clone(), fault)
                                                .await;
                                            break 'drive_loop;
                                        }
                                        inst.task_stack.push(completed_frame);
                                        inst.current_task_index += 1;
                                        inst.updated_at = self.clock.now_ms();
                                        inst.suspension_meta = None;
                                        let _ = store.save_instance(&inst).await;
                                        self.emit_lifecycle(lifecycle::TASK_COMPLETED, &inst).await;
                                        continue;
                                    }
                                    DispatchResult::Failure { error, retryable } => {
                                        if !retryable {
                                            // 不可重试的错误 → 直接 faulted
                                            inst.task_stack.push(frame);
                                            self.fail_instance(
                                            &mut inst,
                                            store.clone(),
                                            crate::workflow::errors::WorkflowFault::communication(
                                                format!("call to '{}' failed: {}", service, error),
                                                error,
                                            ),
                                        ).await;
                                            break 'drive_loop;
                                        }
                                        // 可重试的错误 → retry 策略接线（标准 §Fault Tolerance）
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
                                            store.clone(),
                                            crate::workflow::errors::WorkflowFault::communication(
                                                format!(
                                                    "call to '{}' failed after {} retries: {}",
                                                    service, prev_retries, error
                                                ),
                                                error,
                                            ),
                                        ).await;
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
                                            retry_count: Some(prev_retries + 1),
                                            error: Some(error),
                                        });
                                        inst.status = InstanceStatus::Waiting;
                                        inst.updated_at = self.clock.now_ms();
                                        let _ = store.save_instance(&inst).await;
                                        self.emit_lifecycle(lifecycle::TASK_RETRIED, &inst).await;
                                        self.schedule_auto_resume(
                                            inst.id.clone(),
                                            definition.clone(),
                                            store.clone(),
                                            delay,
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
                                        let _ = store.save_instance(&inst).await;
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
                                        let _ = store.save_instance(&inst).await;
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
                                    let _ = store.save_instance(&inst).await;
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
                                let _ = store.save_instance(&inst).await;
                                // W1-4：登记父子对，交给 per-tick 的登记表扫描
                                // （不再依赖每 5 秒的全量列举）。
                                self.register_pending_subflow(inst.id.clone(), sub_id.clone());
                                self.emit_lifecycle(lifecycle::WORKFLOW_WAITING, &inst)
                                    .await;

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
                                let _ = store.save_instance(&inst).await;
                                self.emit_lifecycle(lifecycle::WORKFLOW_WAITING, &inst)
                                    .await;

                                // 自动恢复定时器（wait 到期后完成帧并推进）
                                let wait_ms = (until_ms - self.clock.now_ms()).max(0) as u64;
                                self.schedule_auto_resume(
                                    inst.id.clone(),
                                    definition.clone(),
                                    store.clone(),
                                    wait_ms,
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
                                let _ = store.save_instance(&inst).await;
                                self.emit_lifecycle(lifecycle::WORKFLOW_WAITING, &inst)
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

                                // 主动事件等待（标准 §Events：listen 升级为主动订阅 + correlation）：
                                // 每个挂起实例持有独立订阅，事件到达 → 自动恢复该实例。
                                let rt = self.clone_runtime();
                                let wait_store = store.clone();
                                let wait_def = definition.clone();
                                let wait_id = inst.id.clone();
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
                                tokio::spawn(async move {
                                    let timeout_ms = event_timeout_ms.unwrap_or(60 * 60 * 1000);
                                    let type_refs: Vec<&str> =
                                        filter_types.iter().map(|s| s.as_str()).collect();
                                    let arrived_type = rt
                                        .event_provider
                                        .wait_for_event(
                                            &type_refs,
                                            filter_source.as_deref(),
                                            filter_subject.as_deref(),
                                            timeout_ms,
                                        )
                                        .await;
                                    if let Some(et) = arrived_type {
                                        rt.resume_by_event(wait_id, wait_def, wait_store, et).await;
                                    }
                                });
                                break 'drive_loop;
                            }
                            SuspendReason::WaitingForSignal { .. } => {
                                // signal 挂起：人工恢复 → 标准相位 Suspended
                                inst.task_stack.push(frame);
                                inst.status = InstanceStatus::Suspended;
                                inst.suspension_meta = Some(reason.to_meta());
                                inst.updated_at = self.clock.now_ms();
                                let _ = store.save_instance(&inst).await;
                                self.emit_lifecycle(lifecycle::WORKFLOW_SUSPENDED, &inst)
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
                            self.fail_instance(&mut inst, store.clone(), fault).await;
                            break;
                        }
                        inst.task_stack.push(frame);
                        inst.current_task_index += 1;
                        inst.updated_at = self.clock.now_ms();
                        let _ = store.save_instance(&inst).await;
                        self.emit_lifecycle(lifecycle::TASK_COMPLETED, &inst).await;
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
                                        let _ = store.save_instance(&inst).await;
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
                                let _ = store.save_instance(&inst).await;
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
                                let _ = store.save_instance(&inst).await;
                                break;
                            }

                            inst.task_stack.push(completed_frame);
                            inst.current_task_index += 1;
                            inst.updated_at = clock.now_ms();
                            let _ = store.save_instance(&inst).await;
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
                            let _ = store.save_instance(&inst).await;
                            break;
                        }

                        inst.task_stack.push(completed_frame);
                        inst.current_task_index += 1;
                        inst.updated_at = self.clock.now_ms();
                        let _ = store.save_instance(&inst).await;
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
                        // ⚠️ `Suspend` **必须**在这里处理（2026-09-19 修）。
                        // `call` 任务的执行器是不做同步 I/O 的：它返回
                        // `Suspend { ExternalCall }`，由 Runtime 去 `dispatch`。
                        // 此前这个 match 只有 `NextTask | Completed | Failed | _`，
                        // 于是 `Suspend` 落进 `_ => {}` **被静默丢弃**：
                        //   ⇒ 被 `compensatedBy` / `onErrors` 包裹的状态，其动作
                        //     **一次都不会发出去**（既不成功也不失败），
                        //     而工作流继续沿正常路径前进 —— 调用方以为副作用执行了。
                        // 这是"最坏的一类"：静默丢副作用。故此处与主循环
                        // （见 `SuspendReason::ExternalCall` 分支）同口径派发。
                        for task in &try_tasks {
                            let step_result = self.executor.execute_step(
                                &WorkflowInstance {
                                    context: try_ctx.clone(),
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
                                    let result = self
                                        .dispatcher
                                        .dispatch(&service, with.as_ref(), &input)
                                        .await;
                                    match result {
                                        DispatchResult::Success { data } => {
                                            let mut done = call_frame;
                                            done.status = TaskStatus::Completed;
                                            done.output = Some(data);
                                            done.ended_at = Some(self.clock.now_ms());
                                            try_ctx = apply_frame_output(try_ctx, &done);
                                        }
                                        DispatchResult::Failure { error, .. } => {
                                            // 可重试与否由 catch 子句的 `errors` 匹配决定：
                                            // 这里统一建模为 communication fault，交给
                                            // catch 路由（catch-all 会接住它）。
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
                                    for task in &clause.tasks {
                                        let step_result = self.executor.execute_step(
                                            &WorkflowInstance {
                                                context: inst.context.clone(),
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
                                let _ = store.save_instance(&inst).await;
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
                        let _ = store.save_instance(&inst).await;
                    }

                    StepResult::Completed { output } => {
                        // 工作流级 output.as / output.schema（标准 §Data Flow）
                        let final_output =
                            match self.apply_workflow_output(&definition, &inst, output) {
                                Ok(v) => v,
                                Err(fault) => {
                                    self.fail_instance(&mut inst, store.clone(), fault).await;
                                    break;
                                }
                            };
                        inst.status = InstanceStatus::Completed;
                        inst.output = Some(final_output);
                        inst.updated_at = self.clock.now_ms();
                        let _ = store.save_instance(&inst).await;
                        self.emit_lifecycle(lifecycle::WORKFLOW_COMPLETED, &inst)
                            .await;
                        break;
                    }

                    StepResult::Failed { fault } => {
                        self.fail_instance(&mut inst, store.clone(), fault).await;
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

    // ── W5-4：后台任务存活登记（正 / 反双测） ──

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
    /// 即第四轮 §6.3.6 的"能力静默死亡"。
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

    // ═══ 全特性兼容测试（标准 §Data Flow / §Fault Tolerance / §Status / §Lifecycle） ═══

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

    // ─── 子流程恢复的登记表路径（W1-4） ───

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

    // ─── listen 主动事件等待（标准 §Events） ───

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

    // ─── listen 事件超时（标准 §Events） ───

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
}
