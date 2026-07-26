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

use serde_json::Value;

use super::model::{
    NamedTask, StepResult, Task, WorkflowDefinition, WorkflowInstance,
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
    /// # 参数
    /// - `inst`: 当前工作流实例（Running 状态）
    /// - `definition`: 关联的工作流定义
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

        // 根据任务类型分发
        self.execute_named_task(current_task, inst)
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
        }
    }
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
