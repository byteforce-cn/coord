// coord-core/workflow/tasks/listen.rs
// listen 任务执行 —— 事件监听
//
// 注册事件过滤器 → Suspend(ListeningForEvent)
// Runtime 负责通过 EventProvider 等待事件到达后 resume

use crate::workflow::model::{
    ListenTask, NamedTask, StepResult, SuspendReason, TaskFrame, TaskStatus, WorkflowInstance,
};
use crate::workflow::ports::Clock;

/// 执行 listen 任务：构建事件过滤器 → Suspend
pub fn execute(
    named: &NamedTask,
    listen: &ListenTask,
    _inst: &WorkflowInstance,
    clock: &dyn Clock,
) -> StepResult {
    let now = clock.now_ms();

    let frame = TaskFrame {
        task_name: named.name.clone(),
        task_type: "listen".to_string(),
        status: TaskStatus::Running,
        input: Some(serde_json::json!({
            "event_type": listen.listen.event_type,
            "source": listen.listen.source,
            "subject": listen.listen.subject,
        })),
        output: None,
        started_at: Some(now),
        ended_at: None,
        retry_count: 0,
        pending_branches: None,
    };

    StepResult::Suspend {
        reason: SuspendReason::ListeningForEvent {
            event_filter: listen.listen.clone(),
        },
        frame,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::model::{EventFilter, InstanceStatus, Task};
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
    fn test_listen_suspends_with_event_filter() {
        let clock = TestClock::new(1000);
        let inst = make_inst();
        let filter = EventFilter {
            event_type: Some("approval.completed".into()),
            source: Some("/coord/icps".into()),
            subject: None,
        };
        let named = NamedTask {
            name: "waitApproval".into(),
            task: Task::Listen(ListenTask { listen: filter.clone() }),
        };

        let result = execute(&named, &ListenTask { listen: filter }, &inst, &clock);

        match result {
            StepResult::Suspend { reason, frame } => {
                assert_eq!(frame.task_name, "waitApproval");
                assert_eq!(frame.task_type, "listen");
                match reason {
                    SuspendReason::ListeningForEvent { event_filter } => {
                        assert_eq!(event_filter.event_type, Some("approval.completed".into()));
                        assert_eq!(event_filter.source, Some("/coord/icps".into()));
                    }
                    other => panic!("expected ListeningForEvent, got {:?}", other),
                }
            }
            other => panic!("expected Suspend, got {:?}", other),
        }
    }

    #[test]
    fn test_listen_minimal_filter() {
        let clock = TestClock::new(1000);
        let inst = make_inst();
        let filter = EventFilter {
            event_type: None,
            source: None,
            subject: None,
        };
        let named = NamedTask {
            name: "listenAll".into(),
            task: Task::Listen(ListenTask { listen: filter.clone() }),
        };

        let result = execute(&named, &ListenTask { listen: filter }, &inst, &clock);

        match result {
            StepResult::Suspend { reason, .. } => {
                assert!(matches!(reason, SuspendReason::ListeningForEvent { .. }));
            }
            other => panic!("expected Suspend, got {:?}", other),
        }
    }
}
