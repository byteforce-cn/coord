// coord-core/workflow/tasks/do_task.rs
// do 任务执行 —— 顺序执行子任务列表
//
// do 任务的子任务在定义时展开，执行器返回 NextTask 帧。
// Runtime 负责通过 pending_branches 追踪子任务进度。

use crate::workflow::model::{
    DoTask, NamedTask, StepResult, TaskFrame, TaskStatus, WorkflowInstance,
};
use crate::workflow::ports::Clock;
use serde_json::Value;

/// 执行 do 任务：返回 NextTask，子任务列表通过 frame.pending_branches 传递
pub fn execute(
    named: &NamedTask,
    do_task: &DoTask,
    inst: &WorkflowInstance,
    clock: &dyn Clock,
) -> StepResult {
    let now = clock.now_ms();

    if do_task.tasks.is_empty() {
        let frame = TaskFrame {
            task_name: named.name.clone(),
            task_type: "do".to_string(),
            status: TaskStatus::Completed,
            input: None,
            output: Some(Value::Null),
            started_at: Some(now),
            ended_at: Some(now),
            retry_count: 0,
            pending_branches: None,
        };
        return StepResult::NextTask(frame);
    }

    let frame = TaskFrame {
        task_name: named.name.clone(),
        task_type: "do".to_string(),
        status: TaskStatus::Running,
        input: Some(inst.context.clone()),
        output: None,
        started_at: Some(now),
        ended_at: None,
        retry_count: 0,
        pending_branches: Some(do_task.tasks.iter().map(|t| t.name.clone()).collect()),
    };

    StepResult::NextTask(frame)
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
            context: serde_json::json!({"key": "val"}),
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
    fn test_do_empty_returns_completed() {
        let clock = TestClock::new(1000);
        let inst = make_inst();
        let named = NamedTask {
            name: "emptyDo".into(),
            task: Task::Do(DoTask { tasks: vec![] }),
        };

        let result = execute(&named, &DoTask { tasks: vec![] }, &inst, &clock);
        match result {
            StepResult::NextTask(frame) => {
                assert_eq!(frame.task_name, "emptyDo");
                assert_eq!(frame.status, TaskStatus::Completed);
            }
            other => panic!("expected NextTask, got {:?}", other),
        }
    }

    #[test]
    fn test_do_with_subtasks_returns_running_with_pending() {
        let clock = TestClock::new(1000);
        let inst = make_inst();
        let named = NamedTask {
            name: "myDo".into(),
            task: Task::Do(DoTask {
                tasks: vec![
                    NamedTask { name: "sub1".into(), task: Task::Do(DoTask { tasks: vec![] }) },
                    NamedTask { name: "sub2".into(), task: Task::Do(DoTask { tasks: vec![] }) },
                ],
            }),
        };

        let result = execute(&named, &DoTask {
            tasks: vec![
                NamedTask { name: "sub1".into(), task: Task::Do(DoTask { tasks: vec![] }) },
                NamedTask { name: "sub2".into(), task: Task::Do(DoTask { tasks: vec![] }) },
            ],
        }, &inst, &clock);

        match result {
            StepResult::NextTask(frame) => {
                assert_eq!(frame.task_name, "myDo");
                assert_eq!(frame.status, TaskStatus::Running);
                assert_eq!(frame.pending_branches, Some(vec!["sub1".into(), "sub2".into()]));
            }
            other => panic!("expected NextTask, got {:?}", other),
        }
    }
}
