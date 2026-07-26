// coord-core/workflow/tasks/call.rs
// call 任务执行 —— HTTP/gRPC/function 调用
//
// call 任务不在执行器中同步执行 I/O，而是返回 Suspend，
// 由 Runtime 负责调用 TaskDispatcher::dispatch。

use crate::workflow::model::{
    CallTask, CallType, NamedTask, StepResult, SuspendReason, TaskFrame, TaskStatus,
    WorkflowInstance,
};
use crate::workflow::ports::Clock;

/// 执行 call 任务：返回 Suspend，由 Runtime 负责 I/O 派发
pub fn execute(
    named: &NamedTask,
    call: &CallTask,
    inst: &WorkflowInstance,
    clock: &dyn Clock,
) -> StepResult {
    let service = match &call.call {
        CallType::Http => "http",
        CallType::Grpc => "grpc",
        CallType::Function(f) => f.as_str(),
    };

    let frame = TaskFrame {
        task_name: named.name.clone(),
        task_type: "call".to_string(),
        status: TaskStatus::Running,
        input: Some(inst.context.clone()),
        output: None,
        started_at: Some(clock.now_ms()),
        ended_at: None,
        retry_count: 0,
        pending_branches: None,
    };

    StepResult::Suspend {
        reason: SuspendReason::ExternalCall {
            service: service.to_string(),
            with: call.with.clone(),
            input: inst.context.clone(),
        },
        frame,
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
    fn test_call_http_suspends() {
        let clock = TestClock::new(1000);
        let inst = make_inst();
        let named = NamedTask {
            name: "callService".into(),
            task: crate::workflow::model::Task::Call(CallTask {
                call: CallType::Http,
                with: Some(serde_json::json!({"method": "POST"})),
            }),
        };

        let result = execute(&named, &CallTask {
            call: CallType::Http,
            with: Some(serde_json::json!({"method": "POST"})),
        }, &inst, &clock);

        match result {
            StepResult::Suspend { reason, frame } => {
                assert_eq!(frame.task_name, "callService");
                assert_eq!(frame.task_type, "call");
                assert!(matches!(reason, SuspendReason::ExternalCall { service, .. } if service == "http"));
            }
            other => panic!("expected Suspend, got {:?}", other),
        }
    }

    #[test]
    fn test_call_function_suspends() {
        let clock = TestClock::new(1000);
        let inst = make_inst();
        let named = NamedTask {
            name: "notify".into(),
            task: crate::workflow::model::Task::Call(CallTask {
                call: CallType::Function("sendEmail".into()),
                with: None,
            }),
        };

        let result = execute(&named, &CallTask {
            call: CallType::Function("sendEmail".into()),
            with: None,
        }, &inst, &clock);

        match result {
            StepResult::Suspend { reason, .. } => {
                assert!(matches!(reason, SuspendReason::ExternalCall { service, .. } if service == "sendEmail"));
            }
            other => panic!("expected Suspend, got {:?}", other),
        }
    }

    #[test]
    fn test_call_grpc_suspends() {
        let clock = TestClock::new(1000);
        let inst = make_inst();
        let named = NamedTask {
            name: "grpcCall".into(),
            task: crate::workflow::model::Task::Call(CallTask {
                call: CallType::Grpc,
                with: None,
            }),
        };

        let result = execute(&named, &CallTask {
            call: CallType::Grpc,
            with: None,
        }, &inst, &clock);

        match result {
            StepResult::Suspend { reason, .. } => {
                assert!(matches!(reason, SuspendReason::ExternalCall { service, .. } if service == "grpc"));
            }
            other => panic!("expected Suspend, got {:?}", other),
        }
    }
}
