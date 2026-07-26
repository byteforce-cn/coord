// coord-core/workflow/tasks/run.rs
// run 任务执行 —— 子流程
//
// 返回 Suspend(RunSubflow)，Runtime 负责：
// 1. 加载子工作流定义
// 2. 创建子实例并启动
// 3. 等待子实例完成
// 4. 将子实例输出合并到父 context
//
// 注：P3 优先级，当前实现为 suspend 机制，完整子流程编排需 Runtime 支持。

use crate::workflow::model::{
    NamedTask, RunTask, StepResult, SuspendReason, TaskFrame, TaskStatus, WorkflowInstance,
};
use crate::workflow::ports::Clock;

/// 执行 run 任务：构建子流程引用 → Suspend(RunSubflow)
pub fn execute(
    named: &NamedTask,
    run: &RunTask,
    inst: &WorkflowInstance,
    clock: &dyn Clock,
) -> StepResult {
    let now = clock.now_ms();

    let frame = TaskFrame {
        task_name: named.name.clone(),
        task_type: "run".to_string(),
        status: TaskStatus::Running,
        input: Some(serde_json::json!({
            "workflow": format!("{}::{}@{}", run.workflow.namespace, run.workflow.name, run.workflow.version),
            "with": run.input,
        })),
        output: None,
        started_at: Some(now),
        ended_at: None,
        retry_count: 0,
        pending_branches: None,
    };

    StepResult::Suspend {
        reason: SuspendReason::RunSubflow {
            workflow: run.workflow.clone(),
            input: run.input.clone(),
            parent_instance_id: inst.id.clone(),
        },
        frame,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::model::{InstanceStatus, Task, WorkflowRef};
    use crate::workflow::ports::test_utils::TestClock;

    fn make_inst() -> WorkflowInstance {
        WorkflowInstance {
            id: "parent-inst-1".into(),
            definition_ns: "test".into(),
            definition_name: "parent-wf".into(),
            definition_version: "1.0".into(),
            status: InstanceStatus::Running,
            context: serde_json::json!({"data": "payload"}),
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
    fn test_run_suspends_with_subflow_ref() {
        let clock = TestClock::new(1000);
        let inst = make_inst();
        let wf_ref = WorkflowRef {
            namespace: "sub".into(),
            name: "child-wf".into(),
            version: "1.0".into(),
        };
        let named = NamedTask {
            name: "runSub".into(),
            task: Task::Run(RunTask {
                workflow: wf_ref.clone(),
                input: Some(serde_json::json!({"key": "value"})),
            }),
        };

        let result = execute(&named, &RunTask {
            workflow: wf_ref,
            input: Some(serde_json::json!({"key": "value"})),
        }, &inst, &clock);

        match result {
            StepResult::Suspend { reason, frame } => {
                assert_eq!(frame.task_name, "runSub");
                assert_eq!(frame.task_type, "run");
                match reason {
                    SuspendReason::RunSubflow { workflow, input, parent_instance_id } => {
                        assert_eq!(workflow.name, "child-wf");
                        assert_eq!(workflow.namespace, "sub");
                        assert_eq!(input, Some(serde_json::json!({"key": "value"})));
                        assert_eq!(parent_instance_id, "parent-inst-1");
                    }
                    other => panic!("expected RunSubflow, got {:?}", other),
                }
            }
            other => panic!("expected Suspend, got {:?}", other),
        }
    }

    #[test]
    fn test_run_without_input() {
        let clock = TestClock::new(1000);
        let inst = make_inst();
        let wf_ref = WorkflowRef {
            namespace: "ns".into(),
            name: "simple".into(),
            version: "2.0".into(),
        };
        let named = NamedTask {
            name: "runSimple".into(),
            task: Task::Run(RunTask {
                workflow: wf_ref.clone(),
                input: None,
            }),
        };

        let result = execute(&named, &RunTask {
            workflow: wf_ref,
            input: None,
        }, &inst, &clock);

        match result {
            StepResult::Suspend { reason, .. } => {
                match reason {
                    SuspendReason::RunSubflow { input, .. } => {
                        assert!(input.is_none());
                    }
                    other => panic!("expected RunSubflow, got {:?}", other),
                }
            }
            other => panic!("expected Suspend, got {:?}", other),
        }
    }
}
