// coord-core/workflow/tasks/raise.rs
// raise 任务执行 —— 错误抛出
//
// 构建 WorkflowFault → 返回 Failed（Runtime 标记实例失败）

use crate::workflow::model::{
    NamedTask, RaiseTask, StepResult, WorkflowFault, WorkflowInstance,
};
use crate::workflow::ports::Clock;

/// 执行 raise 任务：构建错误 → Failed
pub fn execute(
    _named: &NamedTask,
    raise: &RaiseTask,
    _inst: &WorkflowInstance,
    _clock: &dyn Clock,
) -> StepResult {
    let fault = WorkflowFault {
        r#type: raise.raise.r#type.clone(),
        title: raise.raise.title.clone(),
        status: raise.raise.status.unwrap_or(500),
        detail: raise.raise.detail.clone().unwrap_or_default(),
        instance: None,
    };

    StepResult::Failed { fault }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::model::{ErrorDef, InstanceStatus, Task};

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

    fn test_clock() -> crate::workflow::ports::test_utils::TestClock {
        crate::workflow::ports::test_utils::TestClock::new(1000)
    }

    #[test]
    fn test_raise_returns_failed() {
        let clock = test_clock();
        let inst = make_inst();
        let named = NamedTask {
            name: "raiseError".into(),
            task: Task::Raise(RaiseTask {
                raise: ErrorDef {
                    r#type: "business_error".into(),
                    title: "Insufficient funds".into(),
                    status: Some(422),
                    detail: Some("Account balance too low".into()),
                },
            }),
        };

        let result = execute(&named, &RaiseTask {
            raise: ErrorDef {
                r#type: "business_error".into(),
                title: "Insufficient funds".into(),
                status: Some(422),
                detail: Some("Account balance too low".into()),
            },
        }, &inst, &clock);

        match result {
            StepResult::Failed { fault } => {
                assert_eq!(fault.r#type, "business_error");
                assert_eq!(fault.title, "Insufficient funds");
                assert_eq!(fault.status, 422);
                assert_eq!(fault.detail, "Account balance too low");
            }
            other => panic!("expected Failed, got {:?}", other),
        }
    }

    #[test]
    fn test_raise_default_status() {
        let clock = test_clock();
        let inst = make_inst();
        let named = NamedTask {
            name: "raiseDefault".into(),
            task: Task::Raise(RaiseTask {
                raise: ErrorDef {
                    r#type: "generic_error".into(),
                    title: "Something went wrong".into(),
                    status: None,
                    detail: None,
                },
            }),
        };

        let result = execute(&named, &RaiseTask {
            raise: ErrorDef {
                r#type: "generic_error".into(),
                title: "Something went wrong".into(),
                status: None,
                detail: None,
            },
        }, &inst, &clock);

        match result {
            StepResult::Failed { fault } => {
                assert_eq!(fault.status, 500); // default
                assert_eq!(fault.detail, ""); // default
            }
            other => panic!("expected Failed, got {:?}", other),
        }
    }
}
