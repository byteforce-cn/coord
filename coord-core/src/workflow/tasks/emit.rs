// coord-core/workflow/tasks/emit.rs
// emit 任务执行 —— 事件发布
//
// 构建 CloudEvent 数据 → 返回 NextTask 帧（Runtime 负责调用 EventProvider.emit）

use crate::workflow::model::{
    EmitTask, NamedTask, StepResult, TaskFrame, TaskStatus, WorkflowInstance,
};
use crate::workflow::ports::Clock;

/// 执行 emit 任务：构建事件帧 → NextTask
/// Runtime 检测到 task_type="emit" 时调用 EventProvider.emit
pub fn execute(
    named: &NamedTask,
    emit: &EmitTask,
    inst: &WorkflowInstance,
    clock: &dyn Clock,
) -> StepResult {
    let now = clock.now_ms();

    let event_data = serde_json::json!({
        "event_type": emit.emit.event_type,
        "source": emit.emit.source,
        "data": emit.emit.data,
    });

    let frame = TaskFrame {
        task_name: named.name.clone(),
        task_type: "emit".to_string(),
        status: TaskStatus::Completed,
        input: Some(inst.context.clone()),
        output: Some(event_data),
        started_at: Some(now),
        ended_at: Some(now),
        retry_count: 0,
        pending_branches: None,
    };

    // emit 是 fire-and-forget，不阻塞流程
    StepResult::NextTask(frame)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::model::{EmitEvent, InstanceStatus, Task};
    use crate::workflow::ports::test_utils::TestClock;

    fn make_inst() -> WorkflowInstance {
        WorkflowInstance {
            id: "inst-1".into(),
            definition_ns: "test".into(),
            definition_name: "test-wf".into(),
            definition_version: "1.0".into(),
            status: InstanceStatus::Running,
            context: serde_json::json!({"orderId": "ORD-123"}),
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
    fn test_emit_returns_next_task() {
        let clock = TestClock::new(1000);
        let inst = make_inst();
        let named = NamedTask {
            name: "emitEvent".into(),
            task: Task::Emit(EmitTask {
                emit: EmitEvent {
                    event_type: "order.created".into(),
                    source: Some("/coord/orders".into()),
                    data: Some(serde_json::json!({"orderId": "ORD-123"})),
                },
            }),
        };

        let result = execute(&named, &EmitTask {
            emit: EmitEvent {
                event_type: "order.created".into(),
                source: Some("/coord/orders".into()),
                data: Some(serde_json::json!({"orderId": "ORD-123"})),
            },
        }, &inst, &clock);

        match result {
            StepResult::NextTask(frame) => {
                assert_eq!(frame.task_name, "emitEvent");
                assert_eq!(frame.task_type, "emit");
                assert_eq!(frame.status, TaskStatus::Completed);
                let output = frame.output.unwrap();
                assert_eq!(output["event_type"], "order.created");
            }
            other => panic!("expected NextTask, got {:?}", other),
        }
    }

    #[test]
    fn test_emit_with_minimal_event() {
        let clock = TestClock::new(1000);
        let inst = make_inst();
        let named = NamedTask {
            name: "minEmit".into(),
            task: Task::Emit(EmitTask {
                emit: EmitEvent {
                    event_type: "ping".into(),
                    source: None,
                    data: None,
                },
            }),
        };

        let result = execute(&named, &EmitTask {
            emit: EmitEvent {
                event_type: "ping".into(),
                source: None,
                data: None,
            },
        }, &inst, &clock);

        match result {
            StepResult::NextTask(frame) => {
                assert_eq!(frame.task_type, "emit");
                assert_eq!(frame.status, TaskStatus::Completed);
            }
            other => panic!("expected NextTask, got {:?}", other),
        }
    }
}
