// coord-core/workflow/tasks/wait.rs
// wait 任务执行 —— 定时等待
//
// ISO 8601 duration 解析 → 计算 deadline → Suspend

use crate::workflow::model::{
    NamedTask, StepResult, SuspendReason, TaskFrame, TaskStatus, WaitTask,
};
use crate::workflow::ports::Clock;

/// 执行 wait 任务：解析 duration，返回 Suspend(WaitingForDuration)
pub fn execute(
    named: &NamedTask,
    wait: &WaitTask,
    clock: &dyn Clock,
) -> StepResult {
    let duration_ms = crate::workflow::engine::parse_iso8601_duration_ms(&wait.wait).unwrap_or(0);
    let until_ms = clock.now_ms() + duration_ms;

    let frame = TaskFrame {
        task_name: named.name.clone(),
        task_type: "wait".to_string(),
        status: TaskStatus::Running,
        input: Some(serde_json::json!({"duration": wait.wait})),
        output: None,
        started_at: Some(clock.now_ms()),
        ended_at: None,
        retry_count: 0,
        pending_branches: None,
    };

    StepResult::Suspend {
        reason: SuspendReason::WaitingForDuration { until_ms },
        frame,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::model::{InstanceStatus, Task, WorkflowInstance};
    use crate::workflow::ports::test_utils::TestClock;

    fn make_inst() -> WorkflowInstance {
        WorkflowInstance {
            id: "inst-1".into(),
            definition_ns: "test".into(),
            definition_name: "test-wf".into(),
            definition_version: "1.0".into(),
            status: InstanceStatus::Running,
            context: serde_json::json!({}),
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
    fn test_wait_suspends_with_correct_until() {
        let clock = TestClock::new(1000);
        let named = NamedTask {
            name: "wait1h".into(),
            task: Task::Wait(WaitTask { wait: "PT1H".into() }),
        };

        let result = execute(&named, &WaitTask { wait: "PT1H".into() }, &clock);

        match result {
            StepResult::Suspend { reason, frame } => {
                assert_eq!(frame.task_name, "wait1h");
                assert_eq!(frame.task_type, "wait");
                match reason {
                    SuspendReason::WaitingForDuration { until_ms } => {
                        // PT1H = 3600000 ms, clock is at 1000
                        assert_eq!(until_ms, 1000 + 3_600_000);
                    }
                    other => panic!("expected WaitingForDuration, got {:?}", other),
                }
            }
            other => panic!("expected Suspend, got {:?}", other),
        }
    }

    #[test]
    fn test_wait_zero_duration() {
        let clock = TestClock::new(5000);
        let named = NamedTask {
            name: "waitZero".into(),
            task: Task::Wait(WaitTask { wait: "PT0S".into() }),
        };

        let result = execute(&named, &WaitTask { wait: "PT0S".into() }, &clock);

        match result {
            StepResult::Suspend { reason, .. } => {
                assert!(matches!(reason, SuspendReason::WaitingForDuration { until_ms } if until_ms == 5000));
            }
            other => panic!("expected Suspend, got {:?}", other),
        }
    }

    #[test]
    fn test_wait_invalid_duration_defaults_to_zero() {
        let clock = TestClock::new(1000);
        let named = NamedTask {
            name: "waitBad".into(),
            task: Task::Wait(WaitTask { wait: "INVALID".into() }),
        };

        let result = execute(&named, &WaitTask { wait: "INVALID".into() }, &clock);
        // Invalid duration → duration_ms = 0

        match result {
            StepResult::Suspend { reason, .. } => {
                assert!(matches!(reason, SuspendReason::WaitingForDuration { until_ms } if until_ms == 1000));
            }
            other => panic!("expected Suspend, got {:?}", other),
        }
    }
}
