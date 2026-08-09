// coord-core/workflow/tasks/end.rs
// end 任务 —— 终止当前工作流
//
// 对应 CNCF Serverless Workflow 的 end 语义：执行到该任务时立即结束工作流。
// 执行器返回 `StepResult::Completed`，Runtime 收到后标记实例 Completed 并停止驱动循环。
//
// 用途：SW→coord DSL 转换器生成的分支终端，防止 switch 分支 fall-through
// （线性 do 列表模型下，分支任务执行完会继续执行后续任务；end 任务可提前终止）。

use crate::workflow::model::{EndTask, NamedTask, StepResult, WorkflowInstance};
use crate::workflow::ports::Clock;

/// 执行 end 任务：返回 Completed（Runtime 标记实例 Completed 并停止驱动循环）
pub fn execute(
    _named: &NamedTask,
    _end: &EndTask,
    inst: &WorkflowInstance,
    _clock: &dyn Clock,
) -> StepResult {
    StepResult::Completed {
        output: inst.output.clone().unwrap_or(serde_json::Value::Null),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::model::{InstanceStatus, Task};
    use crate::workflow::ports::test_utils::TestClock;

    fn make_inst() -> WorkflowInstance {
        WorkflowInstance {
            id: "inst-1".into(),
            definition_ns: "test".into(),
            definition_name: "test-wf".into(),
            definition_version: "1.0".into(),
            status: InstanceStatus::Running,
            context: serde_json::json!({"amount": 15000}),
            task_stack: vec![],
            current_task_index: 0,
            created_at: 1000,
            updated_at: 1000,
            output: None,
            fault: None,
            suspension_meta: None,
        }
    }

    #[test]
    fn test_end_returns_completed() {
        let clock = TestClock::new(1000);
        let inst = make_inst();
        let named = NamedTask {
            name: "end".into(),
            task: Task::End(EndTask {}),
        };

        let result = execute(&named, &EndTask {}, &inst, &clock);
        assert!(matches!(result, StepResult::Completed { .. }));
    }

    #[test]
    fn test_end_preserves_output() {
        let clock = TestClock::new(1000);
        let mut inst = make_inst();
        inst.output = Some(serde_json::json!({"status": "ok"}));

        let result = execute(&NamedTask { name: "end".into(), task: Task::End(EndTask {}) },
            &EndTask {}, &inst, &clock);
        match result {
            StepResult::Completed { output } => {
                assert_eq!(output, serde_json::json!({"status": "ok"}));
            }
            other => panic!("expected Completed, got {other:?}"),
        }
    }
}
