// coord-core/workflow/runtime.rs
// WorkflowRuntime —— 异步驱动循环
//
// 协调 WorkflowExecutor（纯状态机）与外部依赖（Store、Dispatcher、EventProvider），
// 实现工作流实例的完整生命周期管理：
// - start: 创建并启动实例
// - resume: 从挂起状态恢复
// - drive: 异步驱动循环（spawn 后独立运行）
//
// Phase 2: 实现基本驱动循环，支持 call/do/switch/wait 任务
// Phase 3: 补充 P2 任务类型（fork/for-each/listen 等）
// Phase 4: 对接 coord-agent WorkflowService

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

use super::engine::WorkflowExecutor;
use super::model::{
    InstanceStatus, StepResult, SuspendReason, SuspensionMeta, TaskFrame, TaskStatus,
    WorkflowDefinition, WorkflowFault, WorkflowInstance,
};
use super::ports::{Clock, DispatchResult, EventProvider, ExpressionEval, TaskDispatcher, WorkflowStore};
use super::retry::{RetryConfig, RetryScheduler};

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
        };
        // 启动后台扫描器，定期检查 RunSubflow 挂起的父流程并恢复
        rt.start_subflow_scanner();
        rt
    }

    /// 启动后台子流程扫描器
    ///
    /// 定期扫描所有挂起的父流程（Suspended + RunSubflow 原因），
    /// 检查子流程是否完成，完成后恢复父流程继续执行。
    fn start_subflow_scanner(&self) {
        let rt = self.clone_runtime();
        let store = Arc::clone(&self.store);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                rt.scan_subflows(Arc::clone(&store)).await;
            }
        });
    }

    /// 扫描并恢复已完成子流程的父实例
    async fn scan_subflows(&self, store: Arc<S>) {
        // 查找所有挂起的实例
        let instances = match store.list_instances(None, None, usize::MAX, None).await {
            Ok(list) => list,
            Err(_) => return,
        };

        for inst in instances {
            // RunSubflow 挂起现为标准相位 Waiting（Suspended 兼容保留）
            if !matches!(
                inst.status,
                InstanceStatus::Suspended | InstanceStatus::Waiting
            ) {
                continue;
            }
            // 检查是否因 RunSubflow 挂起
            let subflow_id = match inst.context.get("_subflow_instance_id").and_then(|v| v.as_str()) {
                Some(id) => id.to_string(),
                None => continue,
            };

            // 检查子流程是否完成
            let sub_inst = match store.load_instance(&subflow_id).await {
                Ok(Some(si)) => si,
                _ => continue,
            };

            if !sub_inst.status.is_terminal() {
                continue;
            }

            // 子流程已完成，恢复父流程
            let mut parent = inst.clone();

            let signal_payload = match sub_inst.status {
                InstanceStatus::Completed => {
                    sub_inst.output.clone().unwrap_or(Value::Null)
                }
                InstanceStatus::Failed => {
                    serde_json::json!({
                        "_subflow_error": sub_inst.fault.map(|f| f.title).unwrap_or_default(),
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
                continue;
            }
            self.emit_lifecycle(lifecycle::TASK_COMPLETED, &parent).await;
            self.emit_lifecycle(lifecycle::WORKFLOW_RESUMED, &parent).await;

            // 加载父流程定义并重新驱动
            let parent_def = match store
                .load_definition(&parent.definition_ns, &parent.definition_name, &parent.definition_version)
                .await
            {
                Ok(Some(def)) => def,
                _ => continue,
            };

            let drive_rt = self.clone_runtime();
            let drive_store = Arc::clone(&store);
            let pid = parent.id.clone();
            tokio::spawn(async move {
                drive_rt.drive(pid, parent_def, drive_store).await;
            });
        }
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

        // 克隆 Arc 用于 spawn
        let runtime = self.clone_runtime();
        let instance_id = inst.id.clone();
        let store = Arc::clone(&self.store);
        let def = definition.clone();

        tokio::spawn(async move {
            runtime.drive(instance_id, def, store).await;
        });

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
        let empty = value.is_null()
            || value
                .as_object()
                .map(|o| o.is_empty())
                .unwrap_or(false);
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
                match self.executor.expr.evaluate_with_vars(as_expr, &inst.context, &vars) {
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
                match self.executor.expr.evaluate_with_vars(as_expr, &inst.context, &vars) {
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
                        format!("task '{}' exported context failed schema validation", frame.task_name),
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
            match self.executor.expr.evaluate_with_vars(as_expr, &inst.context, &vars) {
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
            rt.auto_resume(instance_id, definition, store, &reason).await;
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
            self.emit_lifecycle(lifecycle::WORKFLOW_RESUMED, &inst).await;
            self.drive(instance_id, definition, store).await;
        } else if reason == "retry" {
            // 重试：直接重新 drive（重新执行当前任务）
            self.drive(instance_id, definition, store).await;
        }
    }

    /// 事件驱动的自动恢复（标准 §Events：listen 主动订阅，事件到达 → 恢复）
    async fn resume_by_event(
        &self,
        instance_id: String,
        definition: WorkflowDefinition,
        store: Arc<S>,
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
            "eventType": meta.event_filter.as_ref().and_then(|f| f.event_type.clone()),
        });
        inst.status = InstanceStatus::Running;
        inst.suspension_meta = None;
        inst.updated_at = self.clock.now_ms();
        let _ = store.save_instance(&inst).await;
        self.emit_lifecycle(lifecycle::TASK_COMPLETED, &inst).await;
        self.emit_lifecycle(lifecycle::WORKFLOW_RESUMED, &inst).await;

        // 后台 drive（与子流程扫描器同模式，避免在事件等待任务内嵌套 drive）
        let rt = self.clone_runtime();
        tokio::spawn(async move {
            rt.drive(instance_id, definition, store).await;
        });
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
        self.emit_lifecycle(lifecycle::WORKFLOW_FAULTED, &inst).await;
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
            // 事件监听挂起（listen）：signal 名应匹配事件过滤器
            if meta.reason == "listen" {
                if let Some(filter) = meta.event_filter.as_ref() {
                    if let Some(et) = filter.event_type.as_deref() {
                        if let Some(name) = signal_name {
                            if name != et {
                                return Err(RuntimeError::InvalidSignal(format!(
                                    "instance '{}' is listening for event type '{et}', got signal '{name}'",
                                    instance_id
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
                "payload": payload.unwrap_or(Value::Null),
            });
        }

        inst.status = InstanceStatus::Running;
        inst.suspension_meta = None;
        inst.updated_at = self.clock.now_ms();

        self.store
            .save_instance(&inst)
            .await
            .map_err(|e| RuntimeError::StoreError(e.to_string()))?;

        self.emit_lifecycle(lifecycle::WORKFLOW_RESUMED, &inst).await;

        // 需要加载 definition
        let def = self
            .store
            .load_definition(&inst.definition_ns, &inst.definition_name, &inst.definition_version)
            .await
            .map_err(|e| RuntimeError::StoreError(e.to_string()))?
            .ok_or_else(|| {
                RuntimeError::DefinitionNotFound(format!(
                    "{}/{}@{}",
                    inst.definition_ns, inst.definition_name, inst.definition_version
                ))
            })?;

        let runtime = self.clone_runtime();
        let id = inst.id.clone();
        let store = Arc::clone(&self.store);

        tokio::spawn(async move {
            runtime.drive(id, def, store).await;
        });

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
                self.emit_lifecycle(lifecycle::WORKFLOW_STARTED, &inst).await;
                continue;
            }

            // 执行一步
            let step = self.executor.execute_step(&inst, &definition);

            match step {
                StepResult::NextTask(mut frame) => {
                    // emit 任务：fire-and-forget，通过 EventProvider 发布事件
                    if frame.task_type == "emit" {
                        if let Some(ref output) = frame.output {
                            let event_type = output["event_type"]
                                .as_str()
                                .unwrap_or("unknown");
                            let source = output["source"].as_str();
                            let data = output.get("data").unwrap_or(&serde_json::Value::Null);
                            self.event_provider.emit(event_type, source, data).await;
                        }
                    }
                    // 已完成帧：应用 output.as/export.as 数据流管线（Skipped 帧跳过）
                    if frame.status == TaskStatus::Completed {
                        if let Err(fault) = self.apply_task_output(&mut inst, &definition, &mut frame) {
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
                        SuspendReason::ExternalCall { service, with, input } => {
                            let result = self.dispatcher.dispatch(
                                service,
                                with.as_ref(),
                                input,
                            ).await;

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
                                            completed_frame.retry_count = m.retry_count.unwrap_or(0);
                                        }
                                    }
                                    // 应用任务 output.as/export.as 数据流管线
                                    if let Err(fault) = self.apply_task_output(
                                        &mut inst,
                                        &definition,
                                        &mut completed_frame,
                                    ) {
                                        inst.task_stack.push(completed_frame);
                                        self.fail_instance(&mut inst, store.clone(), fault).await;
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
                        SuspendReason::RunSubflow { workflow, input, parent_instance_id: _ } => {
                            // 加载子工作流定义
                            let sub_def = match store
                                .load_definition(&workflow.namespace, &workflow.name, &workflow.version)
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
                                    inst.fault = Some(crate::workflow::errors::WorkflowFault::internal(
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
                            let sub_inst = WorkflowInstance::new(&sub_def, sub_input_val, sub_now);
                            let sub_id = sub_inst.id.clone();

                            if let Err(e) = store.save_instance(&sub_inst).await {
                                inst.task_stack.push(frame);
                                inst.status = InstanceStatus::Failed;
                                inst.fault = Some(crate::workflow::errors::WorkflowFault::internal(
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
                                }.to_meta(),
                            );
                            inst.context["_subflow_instance_id"] =
                                Value::String(sub_id.clone());
                            inst.updated_at = self.clock.now_ms();
                            let _ = store.save_instance(&inst).await;
                            self.emit_lifecycle(lifecycle::WORKFLOW_WAITING, &inst).await;

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
                            self.emit_lifecycle(lifecycle::WORKFLOW_WAITING, &inst).await;

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
                            self.emit_lifecycle(lifecycle::WORKFLOW_WAITING, &inst).await;

                            // 任务级 timeout：事件超时 → faulted（仅当仍停留该任务）
                            let mut event_timeout_ms: Option<u64> = None;
                            let listen_index = inst.current_task_index;
                            if let Some(meta) = definition.task_meta.get(&task_name) {
                                if let Some(t) = meta.timeout.as_ref() {
                                    if let Some(ms) = crate::workflow::engine::parse_iso8601_duration_ms(&t.after) {
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
                            let filter_type = filter.event_type.clone();
                            let filter_source = filter.source.clone();
                            let filter_subject = filter.subject.clone();
                            tokio::spawn(async move {
                                let timeout_ms = event_timeout_ms.unwrap_or(60 * 60 * 1000);
                                let arrived = rt
                                    .event_provider
                                    .wait_for_event(
                                        filter_type.as_deref(),
                                        filter_source.as_deref(),
                                        filter_subject.as_deref(),
                                        timeout_ms,
                                    )
                                    .await;
                                if arrived {
                                    rt.resume_by_event(wait_id, wait_def, wait_store).await;
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
                            self.emit_lifecycle(lifecycle::WORKFLOW_SUSPENDED, &inst).await;
                            break 'drive_loop;
                        }
                    }
                }

                StepResult::SetVariable { variable, value, mut frame } => {
                    inst.context[variable] = value;
                    // 应用任务 output.as/export.as 数据流管线
                    if let Err(fault) = self.apply_task_output(&mut inst, &definition, &mut frame) {
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

                StepResult::Fork { branches, compete, frame } => {
                    // 并行执行分支（Phase 3: 使用 tokio::spawn 真正并行）
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

                        // 等待首个完成的分支
                        let mut winner_result: Option<(String, serde_json::Value)> = None;
                        while let Some(result) = join_set.join_next().await {
                            join_set.abort_all();
                            if let Ok((name, results, has_failure, fault)) = result {
                                if has_failure {
                                    let mut completed_frame = frame;
                                    completed_frame.status = TaskStatus::Failed;
                                    completed_frame.ended_at = Some(clock.now_ms());
                                    completed_frame.output = Some(serde_json::Value::Object(
                                        serde_json::Map::new(),
                                    ));
                                    inst.task_stack.push(completed_frame);
                                    inst.status = InstanceStatus::Failed;
                                    inst.fault = fault;
                                    inst.updated_at = clock.now_ms();
                                    let _ = store.save_instance(&inst).await;
                                    break 'drive_loop;
                                }
                                let output = results
                                    .into_iter()
                                    .next()
                                    .unwrap_or(serde_json::Value::Null);
                                winner_result = Some((name, output));
                            }
                            break; // only take first result in compete mode
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
                                    fork_fault = Some(crate::workflow::errors::WorkflowFault::internal(
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

                StepResult::ForEach { input_expr, iteration, tasks, frame } => {
                    // 求值输入表达式
                    let array = self.executor.expr.evaluate(&input_expr, &inst.context)
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

                StepResult::TryBlock { try_tasks, catch_clauses, frame } => {
                    let mut try_failed = false;
                    let mut try_fault: Option<WorkflowFault> = None;
                    let mut try_ctx = inst.context.clone();

                    // 执行 try 块
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
                            _ => {}
                        }
                    }

                    let mut completed_frame = frame;

                    if try_failed {
                        // 匹配 catch 子句
                        let fault_type = try_fault.as_ref().map(|f| f.r#type.as_str()).unwrap_or("");
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
                                        StepResult::NextTask(_) | StepResult::Completed { .. } => {}
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
                    let final_output = match self.apply_workflow_output(&definition, &inst, output) {
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
                    self.emit_lifecycle(lifecycle::WORKFLOW_COMPLETED, &inst).await;
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
                task: Task::Wait(WaitTask { wait: "PT1S".into() }),
            },
            NamedTask {
                name: "step2".into(),
                task: Task::Wait(WaitTask { wait: "PT2S".into() }),
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

        let inst = runtime.start(&def, serde_json::json!({"key": "value"})).await.unwrap();

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

        store
            .save_definition(&def)
            .await
            .expect("save definition");

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
        assert!(obj.contains_key("branchA"), "missing branchA in fork results");
        assert!(obj.contains_key("branchB"), "missing branchB in fork results");
        assert!(obj.contains_key("branchC"), "missing branchC in fork results");
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

        store
            .save_definition(&def)
            .await
            .expect("save definition");

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

        store
            .save_definition(&def)
            .await
            .expect("save definition");

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

    // ═══ P0 全特性兼容测试（标准 §Data Flow / §Fault Tolerance / §Status Phases / §Lifecycle） ═══

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
        let inst = runtime
            .start(&def, serde_json::json!({}))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let loaded = store_of(&runtime).load_instance(&inst.id).await.unwrap().unwrap();
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
        let loaded = store_of(&runtime).load_instance(&inst.id).await.unwrap().unwrap();
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
        let loaded = store_of(&runtime).load_instance(&inst.id).await.unwrap().unwrap();
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
        let loaded = store_of(&runtime).load_instance(&inst.id).await.unwrap().unwrap();
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
        let loaded = store_of(&runtime).load_instance(&inst.id).await.unwrap().unwrap();
        assert_eq!(loaded.status, InstanceStatus::Completed, "context: {:?} fault: {:?}", loaded.context, loaded.fault);
        // set 任务：value = ${ .x }，但有效输入是 input.from 变换后的 {x: 10}
        // → context["result"] = 10
        assert_eq!(loaded.context["result"], 10, "context: {:?}", loaded.context);
        // export.as：context = context + {exported: 10}
        assert_eq!(loaded.context["exported"], 10, "context: {:?}", loaded.context);
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
        let inst = runtime
            .start(&def, serde_json::json!({}))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let loaded = store_of(&runtime).load_instance(&inst.id).await.unwrap().unwrap();
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
        let inst = runtime
            .start(&def, serde_json::json!({}))
            .await
            .unwrap();
        // 工作流超时 200ms → 实例 faulted（timeout 错误 408）
        tokio::time::sleep(Duration::from_millis(600)).await;
        let loaded = store_of(&runtime).load_instance(&inst.id).await.unwrap().unwrap();
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
        let inst = runtime
            .start(&def, serde_json::json!({}))
            .await
            .unwrap();

        // wait 期间：Waiting 相位
        tokio::time::sleep(Duration::from_millis(10)).await;
        let mid = store_of(&runtime).load_instance(&inst.id).await.unwrap().unwrap();
        assert_eq!(mid.status, InstanceStatus::Waiting);
        assert_eq!(mid.suspension_meta.as_ref().unwrap().reason, "wait");

        // wait 到期后自动恢复 → 继续执行后续任务 → Completed
        tokio::time::sleep(Duration::from_millis(200)).await;
        let loaded = store_of(&runtime).load_instance(&inst.id).await.unwrap().unwrap();
        assert_eq!(loaded.status, InstanceStatus::Completed);
        assert_eq!(loaded.context["done"], "after-wait");
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
        let inst = runtime
            .start(&def, serde_json::json!({}))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let loaded = store_of(&runtime).load_instance(&inst.id).await.unwrap().unwrap();
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
            .resume(&inst.id, Some("approval.requested"), Some(serde_json::json!({"ok": true})), None)
            .await
            .unwrap();
        assert_eq!(resumed.status, InstanceStatus::Running);
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
            .emit("order.created", Some("coord/orders"), &serde_json::json!({"orderId": "ORD-1"}))
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
    fn store_of<E, C, S, D, B>(
        runtime: &WorkflowRuntime<E, C, S, D, B>,
    ) -> &Arc<S>
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
