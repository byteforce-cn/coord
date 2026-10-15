// coord-core/workflow/tasks/approve.rs
// approve 任务执行 —— 原生审批节点挂起
//
// 纯函数：返回 Suspend(WaitingForApproval)，Runtime 负责打开 v2 审批引擎、
// 持久化审批实例并提交 workflow 挂起（approval_instance_id 恒等于 workflow 实例 id）。

use crate::workflow::model::{
    ApproveTask, NamedTask, StepResult, SuspendReason, TaskFrame, TaskStatus, WorkflowInstance,
};
use crate::workflow::ports::Clock;

/// 执行 approve 任务：构造挂起原因 → Suspend(WaitingForApproval)
///
/// 定义 pin 未回填时 fail-closed（`drive` 保证执行前已钉死定义）：
/// 审批实例的 `definition_hash` 取自 pin，缺失绝不能静默降级为无审批继续。
pub fn execute(
    named: &NamedTask,
    _task: &ApproveTask,
    instance: &WorkflowInstance,
    clock: &dyn Clock,
) -> StepResult {
    if instance.definition_hash.is_none() {
        return StepResult::Failed {
            fault: crate::workflow::errors::WorkflowFault::internal(
                "approve task requires a pinned definition",
                format!(
                    "task '{}' executed without a definition pin; the drive loop must backfill \
                     definition_hash before executing an approve node",
                    named.name
                ),
            ),
        };
    }

    let frame = TaskFrame {
        task_name: named.name.clone(),
        task_type: "approve".to_string(),
        status: TaskStatus::Running,
        input: Some(instance.context.clone()),
        output: None,
        started_at: Some(clock.now_ms()),
        ended_at: None,
        retry_count: 0,
        pending_branches: None,
    };

    StepResult::Suspend {
        reason: SuspendReason::WaitingForApproval {
            approval_instance_id: instance.id.clone(),
            node: named.name.clone(),
        },
        frame,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::approval::model::JoinPolicy;
    use crate::workflow::model::{InstanceStatus, Task};
    use crate::workflow::ports::test_utils::TestClock;

    fn make_task() -> ApproveTask {
        ApproveTask {
            approvers: vec!["alice".into()],
            pool: vec![],
            readers: vec![],
            roles: vec![],
            mode: JoinPolicy::All,
            veto: true,
            deadline: None,
            on_pass: None,
            on_reject: None,
            sign_admins: vec![],
            allow_self_approve: false,
        }
    }

    fn make_inst() -> WorkflowInstance {
        WorkflowInstance {
            id: "inst-1".into(),
            definition_ns: "test".into(),
            definition_name: "test-wf".into(),
            definition_version: "1.0".into(),
            status: InstanceStatus::Running,
            context: serde_json::json!({"amount": 100}),
            task_stack: vec![],
            current_task_index: 0,
            created_at: 1000,
            updated_at: 1000,
            output: None,
            fault: None,
            suspension_meta: None,
            initiator: None,
            definition_hash: Some("sha256:test".into()),
            engine_version: None,
        }
    }

    #[test]
    fn execute_returns_waiting_for_approval_suspend() {
        let clock = TestClock::new(1000);
        let inst = make_inst();
        let named = NamedTask {
            name: "review".into(),
            task: Task::Approve(make_task()),
        };

        match execute(&named, &make_task(), &inst, &clock) {
            StepResult::Suspend { reason, frame } => {
                assert_eq!(frame.task_name, "review");
                assert_eq!(frame.task_type, "approve");
                assert_eq!(frame.status, TaskStatus::Running);
                match &reason {
                    SuspendReason::WaitingForApproval {
                        approval_instance_id,
                        node,
                    } => {
                        assert_eq!(approval_instance_id.as_str(), "inst-1");
                        assert_eq!(node.as_str(), "review");
                    }
                    other => panic!("expected WaitingForApproval, got {other:?}"),
                }
                let meta = reason.to_meta();
                assert_eq!(meta.reason, "approval");
                assert_eq!(meta.expected_signal.as_deref(), Some("approval.completed"));
                assert_eq!(meta.approval_instance_id.as_deref(), Some("inst-1"));
            }
            other => panic!("expected Suspend, got {other:?}"),
        }
    }

    #[test]
    fn execute_fails_without_definition_pin() {
        // pin 必须在 drive 内回填：缺失时 fail-closed，不得静默推进到下一任务。
        let clock = TestClock::new(1000);
        let mut inst = make_inst();
        inst.definition_hash = None;
        let named = NamedTask {
            name: "review".into(),
            task: Task::Approve(make_task()),
        };

        match execute(&named, &make_task(), &inst, &clock) {
            StepResult::Failed { fault } => {
                assert!(
                    fault.title.contains("pinned definition"),
                    "unexpected fault: {fault:?}"
                );
            }
            other => panic!("approve without pin must fail, got {other:?}"),
        }
    }
}
