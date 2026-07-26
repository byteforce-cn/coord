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

use serde_json::Value;

use super::engine::WorkflowExecutor;
use super::model::{
    InstanceStatus, StepResult, SuspendReason, TaskFrame, TaskStatus, WorkflowDefinition, WorkflowFault,
    WorkflowInstance,
};
use super::ports::{Clock, DispatchResult, EventProvider, ExpressionEval, TaskDispatcher, WorkflowStore};

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
            if inst.status != InstanceStatus::Suspended {
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
    /// 1. 创建实例（Running 状态）
    /// 2. 持久化到存储
    /// 3. 异步驱动执行
    pub async fn start(
        &self,
        definition: &WorkflowDefinition,
        input: Value,
    ) -> Result<WorkflowInstance, RuntimeError> {
        let now_ms = self.clock.now_ms();
        let inst = WorkflowInstance::new(definition, input, now_ms);

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

        Ok(inst)
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
    pub(crate) async fn drive(
        &self,
        instance_id: String,
        definition: WorkflowDefinition,
        store: Arc<S>,
    ) {
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

            // 执行一步
            let step = self.executor.execute_step(&inst, &definition);

            match step {
                StepResult::NextTask(frame) => {
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
                            inst.fault = Some(WorkflowFault {
                                r#type: "goto_target_not_found".into(),
                                title: format!("switch goto target '{}' not found", target),
                                status: 500,
                                detail: "The switch condition referenced a task that does not exist in do_tasks".into(),
                            });
                            inst.updated_at = self.clock.now_ms();
                            let _ = store.save_instance(&inst).await;
                            break;
                        }
                    }
                }

                StepResult::Suspend { reason, frame } => {
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
                                    inst.task_stack.push(completed_frame);
                                    inst.current_task_index += 1;
                                    inst.updated_at = self.clock.now_ms();
                                    let _ = store.save_instance(&inst).await;
                                    continue;
                                }
                                DispatchResult::Failure { error, retryable } => {
                                    if !retryable {
                                        // 不可重试的错误 → 直接失败
                                        inst.task_stack.push(frame);
                                        inst.status = InstanceStatus::Failed;
                                        inst.fault = Some(WorkflowFault {
                                            r#type: "external_call_failed".into(),
                                            title: format!("call to '{}' failed: {}", service, error),
                                            status: 502,
                                            detail: error,
                                        });
                                        inst.updated_at = self.clock.now_ms();
                                        let _ = store.save_instance(&inst).await;
                                        break 'drive_loop;
                                    }
                                    // 可重试的错误 → 挂起等待外部恢复
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
                                    inst.fault = Some(WorkflowFault {
                                        r#type: "subflow_not_found".into(),
                                        title: format!(
                                            "subflow '{}::{}@{}' not found",
                                            workflow.namespace, workflow.name, workflow.version
                                        ),
                                        status: 404,
                                        detail: "The referenced sub-workflow definition does not exist".into(),
                                    });
                                    inst.updated_at = self.clock.now_ms();
                                    let _ = store.save_instance(&inst).await;
                                    break 'drive_loop;
                                }
                                Err(e) => {
                                    inst.task_stack.push(frame);
                                    inst.status = InstanceStatus::Failed;
                                    inst.fault = Some(WorkflowFault {
                                        r#type: "subflow_load_error".into(),
                                        title: format!("failed to load subflow: {e}"),
                                        status: 500,
                                        detail: e.to_string(),
                                    });
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
                                inst.fault = Some(WorkflowFault {
                                    r#type: "subflow_save_failed".into(),
                                    title: format!("failed to save subflow instance: {e}"),
                                    status: 500,
                                    detail: e.to_string(),
                                });
                                inst.updated_at = self.clock.now_ms();
                                let _ = store.save_instance(&inst).await;
                                break 'drive_loop;
                            }

                            // 挂起父流程
                            inst.task_stack.push(frame);
                            inst.status = InstanceStatus::Suspended;
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

                            // 子流程监控由后台扫描器 (start_subflow_scanner) 负责，
                            // 父流程挂起后会在子流程完成时由扫描器自动恢复。
                            break 'drive_loop;
                        }
                        _ => {
                            // 其他 SuspendReason（wait/listen/signal）→ 正常挂起
                        }
                    }

                    inst.task_stack.push(frame);
                    inst.status = InstanceStatus::Suspended;
                    inst.suspension_meta = Some(reason.to_meta());
                    inst.updated_at = self.clock.now_ms();
                    let _ = store.save_instance(&inst).await;
                    break; // 等待外部信号唤醒
                }

                StepResult::SetVariable { variable, value, frame } => {
                    inst.context[variable] = value;
                    inst.task_stack.push(frame);
                    inst.current_task_index += 1;
                    inst.updated_at = self.clock.now_ms();
                    let _ = store.save_instance(&inst).await;
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
                                    fork_fault = Some(WorkflowFault {
                                        r#type: "fork_join_error".into(),
                                        title: format!("branch task panicked: {join_err}"),
                                        status: 500,
                                        detail: "A fork branch task panicked during execution".into(),
                                    });
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

                        for clause in &catch_clauses {
                            let matches = match &clause.errors {
                                Some(errors) => errors.iter().any(|e| e == fault_type),
                                None => true, // catch-all
                            };
                            if matches {
                                // 执行 catch 任务
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
                    }

                    completed_frame.status = TaskStatus::Completed;
                    completed_frame.ended_at = Some(self.clock.now_ms());
                    inst.task_stack.push(completed_frame);
                    inst.current_task_index += 1;
                    inst.updated_at = self.clock.now_ms();
                    let _ = store.save_instance(&inst).await;
                }

                StepResult::Completed { output } => {
                    inst.status = InstanceStatus::Completed;
                    inst.output = Some(output);
                    inst.updated_at = self.clock.now_ms();
                    let _ = store.save_instance(&inst).await;
                    break;
                }

                StepResult::Failed { fault } => {
                    inst.status = InstanceStatus::Failed;
                    inst.fault = Some(fault);
                    inst.updated_at = self.clock.now_ms();
                    let _ = store.save_instance(&inst).await;
                    break;
                }
            }
        }
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
        Document, NamedTask, Task, WaitTask, WorkflowDefinition,
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

        assert_eq!(inst.status, InstanceStatus::Running);
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
}
