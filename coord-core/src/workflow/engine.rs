// coord-core/workflow/engine.rs
// WorkflowExecutor —— 纯状态机，无 I/O
//
// 负责单步执行工作流任务，所有外部依赖通过 trait 注入：
// - ExpressionEval: 表达式求值（switch 条件判断）
// - Clock: 时间获取（wait 任务计算 deadline）
//
// 关键设计决策：
// - 执行器是纯函数，不执行任何 I/O 操作
// - call 任务返回 Suspend，由 Runtime 负责派发
// - switch 任务返回 Goto，由 Runtime 负责跳转

use std::collections::HashMap;

use serde_json::Value;

use super::model::{
    NamedTask, StepResult, Task, TaskFrame, TaskMeta, TaskStatus, WorkflowDefinition,
    WorkflowFault, WorkflowInstance,
};
use super::ports::{Clock, ExpressionEval};

// ─── WorkflowExecutor ───

/// 工作流执行器 —— 纯状态机，单步推进工作流实例
pub struct WorkflowExecutor<E: ExpressionEval, C: Clock> {
    pub(crate) expr: E,
    clock: C,
}

impl<E: ExpressionEval, C: Clock> WorkflowExecutor<E, C> {
    /// 创建新的执行器
    pub fn new(expr: E, clock: C) -> Self {
        Self { expr, clock }
    }

    /// 执行一步：根据当前实例状态推进一个任务
    ///
    /// 数据流管线（标准 §Data Flow）：
    /// 1. 任务 `if` 条件（为假 → 跳过）
    /// 2. 任务 `input.from` 变换 + `input.schema` 校验 → 有效任务输入
    /// 3. 执行任务（对有效任务输入）
    /// 4. 帧记录有效任务输入（供 runtime 的 output.as/export.as 绑定 `$input`）
    ///
    /// # 返回
    /// - `NextTask`: 推进到下一个任务
    /// - `Goto`: switch 条件匹配，跳转到目标任务
    /// - `Suspend`: 暂停执行，等待外部条件
    /// - `Completed`: 所有任务执行完成
    /// - `Failed`: 执行失败
    pub fn execute_step(
        &self,
        inst: &WorkflowInstance,
        definition: &WorkflowDefinition,
    ) -> StepResult {
        // 获取当前任务
        let current_task = match definition.do_tasks.get(inst.current_task_index) {
            Some(task) => task,
            None => {
                // 所有任务执行完成
                return StepResult::Completed {
                    output: inst.output.clone().unwrap_or(Value::Null),
                };
            }
        };

        let vars = build_expression_vars(inst, definition);
        let meta = definition.task_meta.get(&current_task.name);

        // 1) 任务 `if` 条件：条件为假 → 跳过任务（Skipped 帧，推进）
        if let Some(meta) = meta {
            if let Some(cond) = &meta.if_condition {
                match self.expr.evaluate_bool_with_vars(cond, &inst.context, &vars) {
                    Ok(true) => {}
                    Ok(false) => {
                        let now = self.clock.now_ms();
                        let frame = TaskFrame {
                            task_name: current_task.name.clone(),
                            task_type: task_type_name(&current_task.task),
                            status: TaskStatus::Skipped,
                            input: Some(inst.context.clone()),
                            output: None,
                            started_at: Some(now),
                            ended_at: Some(now),
                            retry_count: 0,
                            pending_branches: None,
                        };
                        return StepResult::NextTask(frame);
                    }
                    Err(e) => {
                        return StepResult::Failed {
                            fault: crate::workflow::errors::WorkflowFault::expression(
                                format!("task '{}' if condition evaluation failed", current_task.name),
                                e.to_string(),
                            ),
                        };
                    }
                }
            }
        }

        // 2) 任务 `input.from` 变换 + `input.schema` 校验 → 有效任务输入
        let effective_ctx = match self.task_input(current_task, meta, inst, &vars) {
            Ok(ctx) => ctx,
            Err(fault) => return StepResult::Failed { fault },
        };

        let exec_inst = if effective_ctx == inst.context {
            inst.clone()
        } else {
            WorkflowInstance {
                context: effective_ctx.clone(),
                ..inst.clone()
            }
        };

        // 3) 执行任务（对有效任务输入）
        let mut result = self.execute_named_task(current_task, &exec_inst);

        // 4) 记录有效任务输入到帧（供 runtime output.as/export.as 绑定 `$input`）
        set_frame_input(&mut result, effective_ctx);

        result
    }

    /// 任务级输入变换/校验（`input.from` + `input.schema`）
    fn task_input(
        &self,
        current_task: &NamedTask,
        meta: Option<&TaskMeta>,
        inst: &WorkflowInstance,
        vars: &HashMap<String, Value>,
    ) -> Result<Value, WorkflowFault> {
        let Some(input_cfg) = meta.and_then(|m| m.input.as_ref()) else {
            return Ok(inst.context.clone());
        };

        // input.from 变换
        let transformed = if let Some(from) = &input_cfg.from {
            match self.expr.evaluate_with_vars(from, &inst.context, vars) {
                Ok(v) => v,
                Err(e) => {
                    return Err(crate::workflow::errors::WorkflowFault::expression(
                        format!("task '{}' input.from evaluation failed", current_task.name),
                        e.to_string(),
                    ))
                }
            }
        } else {
            inst.context.clone()
        };

        // input.schema 校验
        if let Some(schema) = &input_cfg.schema {
            if let Err(errs) = crate::workflow::jsonschema::validate(schema, &transformed) {
                return Err(crate::workflow::errors::WorkflowFault::validation(
                    format!("task '{}' input failed schema validation", current_task.name),
                    errs.join("; "),
                )
                .with_instance(format!("/tasks/{}/input", current_task.name)));
            }
        }

        Ok(transformed)
    }

    /// 执行命名任务 —— 委托到 tasks/ 模块
    fn execute_named_task(
        &self,
        named: &NamedTask,
        inst: &WorkflowInstance,
    ) -> StepResult {
        match &named.task {
            Task::Call(call) => super::tasks::call::execute(named, call, inst, &self.clock),
            Task::Do(do_task) => super::tasks::do_task::execute(named, do_task, inst, &self.clock),
            Task::Switch(switch) => super::tasks::switch::execute(named, switch, inst, &self.expr, &self.clock),
            Task::Wait(wait) => super::tasks::wait::execute(named, wait, &self.clock),
            Task::Set(set) => super::tasks::set::execute(named, set, inst, &self.expr, &self.clock),
            Task::Raise(raise) => super::tasks::raise::execute(named, raise, inst, &self.clock),
            Task::Emit(emit) => super::tasks::emit::execute(named, emit, inst, &self.clock),
            Task::Listen(listen) => super::tasks::listen::execute(named, listen, inst, &self.clock),
            Task::Fork(fork) => super::tasks::fork::execute(named, fork, inst, &self.clock),
            Task::ForEach(for_each) => super::tasks::for_each::execute(named, for_each, inst, &self.expr, &self.clock),
            Task::TryCatch(try_catch) => super::tasks::try_catch::execute(named, try_catch, inst, &self.clock),
            Task::Run(run) => super::tasks::run::execute(named, run, inst, &self.clock),
            Task::End(end) => super::tasks::end::execute(named, end, inst, &self.clock),
        }
    }
}

// ─── 表达式变量绑定 ───

/// 构建标准运行时表达式变量绑定
///
/// `$context` 由求值器固定绑定到 context 根；此处补充：
/// `$workflow` / `$runtime` / `$authorization` / `$secrets` / `$constants`。
/// `$input` / `$output` / `$task` 由各管线阶段按需补充。
pub(crate) fn build_expression_vars(
    inst: &WorkflowInstance,
    definition: &WorkflowDefinition,
) -> HashMap<String, Value> {
    let mut vars = HashMap::new();
    vars.insert(
        "workflow".to_string(),
        serde_json::json!({
            "id": definition.id,
            "name": definition.document.name,
            "namespace": definition.document.namespace,
            "version": definition.document.version,
        }),
    );
    vars.insert(
        "runtime".to_string(),
        serde_json::json!({
            "workflowInstanceId": inst.id,
            "createdAt": inst.created_at,
            "updatedAt": inst.updated_at,
        }),
    );
    // 认证上下文（P2 注入）
    vars.insert("authorization".to_string(), Value::Object(Default::default()));
    // 密钥（P2 注入真实值；此处仅声明键）
    vars.insert("secrets".to_string(), Value::Object(Default::default()));
    if let Some(c) = &definition.constants.values {
        vars.insert("constants".to_string(), Value::Object(c.clone()));
    }
    vars
}

/// 任务类型名（用于 Skipped 帧等）
pub(crate) fn task_type_name(task: &Task) -> String {
    match task {
        Task::Call(_) => "call".into(),
        Task::Do(_) => "do".into(),
        Task::Switch(_) => "switch".into(),
        Task::Fork(_) => "fork".into(),
        Task::ForEach(_) => "for_each".into(),
        Task::Wait(_) => "wait".into(),
        Task::Listen(_) => "listen".into(),
        Task::Emit(_) => "emit".into(),
        Task::Set(_) => "set".into(),
        Task::Raise(_) => "raise".into(),
        Task::TryCatch(_) => "try".into(),
        Task::Run(_) => "run".into(),
        Task::End(_) => "end".into(),
    }
}

/// 将有效任务输入写入结果帧（所有携带帧的 StepResult 变体）
pub(crate) fn set_frame_input(result: &mut StepResult, input: Value) {
    let frame = match result {
        StepResult::NextTask(f)
        | StepResult::Goto { frame: f, .. }
        | StepResult::Suspend { frame: f, .. }
        | StepResult::SetVariable { frame: f, .. }
        | StepResult::Fork { frame: f, .. }
        | StepResult::ForEach { frame: f, .. }
        | StepResult::TryBlock { frame: f, .. } => f,
        _ => return,
    };
    frame.input = Some(input);
}

// ─── ISO 8601 Duration 解析 ───

/// 解析 ISO 8601 duration 字符串为毫秒数
///
/// 支持格式：
/// - PT{seconds}S, PT{minutes}M, PT{hours}H
/// - P{days}D, P{weeks}W
/// - 组合格式: P1DT12H30M
///
/// 返回毫秒数，解析失败返回 None。
pub fn parse_iso8601_duration_ms(duration: &str) -> Option<i64> {
    let s = duration.trim();

    // PnW (weeks)
    if let Some(rest) = s.strip_prefix('P') {
        if let Some(week_str) = rest.strip_suffix('W') {
            if let Ok(weeks) = week_str.parse::<f64>() {
                return Some((weeks * 7.0 * 24.0 * 60.0 * 60.0 * 1000.0) as i64);
            }
        }
    }

    // 解析 P...T... 格式
    let (date_part, time_part) = if let Some(t_pos) = s.find('T') {
        (&s[1..t_pos], Some(&s[t_pos + 1..]))
    } else if s.starts_with('P') {
        (&s[1..], None)
    } else {
        return None;
    };

    let mut total_ms: f64 = 0.0;

    // 日期部分: D, M(month - not supported precisely), Y(year - not supported)
    if !date_part.is_empty() {
        total_ms += extract_component(date_part, 'D', 24.0 * 60.0 * 60.0 * 1000.0);
    }

    // 时间部分: H, M, S
    if let Some(tp) = time_part {
        total_ms += extract_component(tp, 'H', 60.0 * 60.0 * 1000.0);
        total_ms += extract_component(tp, 'M', 60.0 * 1000.0);
        total_ms += extract_component(tp, 'S', 1000.0);
    }

    Some(total_ms as i64)
}

/// 从 duration 组件中提取数值
fn extract_component(s: &str, unit: char, multiplier: f64) -> f64 {
    // 在字符串中查找 unit 字符的位置
    if let Some(pos) = s.find(unit) {
        // 从 unit 位置往前扫描，收集数字和小数点
        let before = &s[..pos];
        let num_str: String = before
            .chars()
            .rev()
            .take_while(|c| c.is_ascii_digit() || *c == '.' || *c == '-')
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        if let Ok(val) = num_str.parse::<f64>() {
            return val * multiplier;
        }
    }
    0.0
}

// ─── 测试 ───

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::expression::ExpressionEvaluator;
    use crate::workflow::model::{
        Document, InstanceStatus, NamedTask, Task, WaitTask, WorkflowDefinition,
    };
    use crate::workflow::ports::test_utils::TestClock;

    fn make_executor() -> WorkflowExecutor<ExpressionEvaluator, TestClock> {
        WorkflowExecutor::new(ExpressionEvaluator::new(), TestClock::new(1000))
    }

    fn make_definition(tasks: Vec<NamedTask>) -> WorkflowDefinition {
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
            do_tasks: tasks,
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

    fn make_instance(index: usize) -> WorkflowInstance {
        WorkflowInstance {
            id: "inst-1".into(),
            definition_ns: "test".into(),
            definition_name: "test-wf".into(),
            definition_version: "1.0".into(),
            status: InstanceStatus::Running,
            context: serde_json::json!({"amount": 15000}),
            task_stack: vec![],
            current_task_index: index,
            created_at: 1000,
            updated_at: 1000,
            output: None,
            fault: None,
            suspension_meta: None,
        }
    }

    // ─── 边界条件测试 ───

    #[test]
    fn test_execute_step_all_tasks_completed() {
        let executor = make_executor();
        let def = make_definition(vec![NamedTask {
            name: "only".into(),
            task: Task::Wait(WaitTask { wait: "PT1S".into() }),
        }]);
        let inst = make_instance(1); // index past the only task

        let result = executor.execute_step(&inst, &def);
        assert!(matches!(result, StepResult::Completed { .. }));
    }

    #[test]
    fn test_empty_do_tasks_completes() {
        let executor = make_executor();
        let def = make_definition(vec![]);
        let inst = make_instance(0);

        let result = executor.execute_step(&inst, &def);
        assert!(matches!(result, StepResult::Completed { .. }));
    }

    #[test]
    fn test_execute_step_end_task_completes() {
        let executor = make_executor();
        let def = make_definition(vec![NamedTask {
            name: "end".into(),
            task: Task::End(crate::workflow::model::EndTask {}),
        }]);
        let inst = make_instance(0);

        let result = executor.execute_step(&inst, &def);
        assert!(matches!(result, StepResult::Completed { .. }));
    }

    // ─── ISO 8601 Duration 解析测试 ───

    #[test]
    fn test_parse_duration_seconds() {
        assert_eq!(parse_iso8601_duration_ms("PT30S"), Some(30_000));
        assert_eq!(parse_iso8601_duration_ms("PT0S"), Some(0));
        assert_eq!(parse_iso8601_duration_ms("PT90S"), Some(90_000));
    }

    #[test]
    fn test_parse_duration_minutes() {
        assert_eq!(parse_iso8601_duration_ms("PT5M"), Some(300_000));
        assert_eq!(parse_iso8601_duration_ms("PT1M"), Some(60_000));
    }

    #[test]
    fn test_parse_duration_hours() {
        assert_eq!(parse_iso8601_duration_ms("PT1H"), Some(3_600_000));
        assert_eq!(parse_iso8601_duration_ms("PT2H"), Some(7_200_000));
    }

    #[test]
    fn test_parse_duration_days() {
        assert_eq!(parse_iso8601_duration_ms("P1D"), Some(86_400_000));
        assert_eq!(parse_iso8601_duration_ms("P7D"), Some(604_800_000));
    }

    #[test]
    fn test_parse_duration_weeks() {
        assert_eq!(parse_iso8601_duration_ms("P1W"), Some(604_800_000));
        assert_eq!(parse_iso8601_duration_ms("P2W"), Some(1_209_600_000));
    }

    #[test]
    fn test_parse_duration_combined() {
        assert_eq!(
            parse_iso8601_duration_ms("P1DT12H30M"),
            Some(86_400_000 + 43_200_000 + 1_800_000)
        );
        assert_eq!(
            parse_iso8601_duration_ms("PT1H30M"),
            Some(3_600_000 + 1_800_000)
        );
    }

    #[test]
    fn test_parse_duration_invalid() {
        assert_eq!(parse_iso8601_duration_ms("invalid"), None);
        assert_eq!(parse_iso8601_duration_ms(""), None);
    }
}
