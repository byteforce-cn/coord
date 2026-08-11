// coord-core/workflow/tasks/switch.rs
// switch 任务执行 —— 条件分支
//
// jq 条件求值 → 匹配分支 → 返回 Goto（跳转到目标任务名）。
// 无匹配时返回 NextTask（继续下一任务）。

use crate::workflow::model::{
    NamedTask, StepResult, SwitchTask, TaskFrame, TaskStatus, WorkflowInstance,
};
use crate::workflow::ports::{Clock, ExpressionEval};

/// 执行 switch 任务：按顺序求值条件，返回 Goto 或 NextTask
pub fn execute(
    named: &NamedTask,
    switch: &SwitchTask,
    inst: &WorkflowInstance,
    expr: &dyn ExpressionEval,
    clock: &dyn Clock,
) -> StepResult {
    let now = clock.now_ms();

    for cond in &switch.conditions {
        let expr_str = match &cond.condition {
            Some(e) => e.as_str(),
            None => {
                let frame = TaskFrame {
                    task_name: named.name.clone(),
                    task_type: "switch".to_string(),
                    status: TaskStatus::Completed,
                    input: Some(inst.context.clone()),
                    output: Some(serde_json::json!({"matched": cond.transition})),
                    started_at: Some(now),
                    ended_at: Some(now),
                    retry_count: 0,
                    pending_branches: None,
                };
                return StepResult::Goto { target: cond.transition.clone(), frame };
            }
        };

        match expr.evaluate_bool(expr_str, &inst.context) {
            Ok(true) => {
                let frame = TaskFrame {
                    task_name: named.name.clone(),
                    task_type: "switch".to_string(),
                    status: TaskStatus::Completed,
                    input: Some(inst.context.clone()),
                    output: Some(serde_json::json!({"matched": cond.transition})),
                    started_at: Some(now),
                    ended_at: Some(now),
                    retry_count: 0,
                    pending_branches: None,
                };
                return StepResult::Goto { target: cond.transition.clone(), frame };
            }
            Ok(false) => continue,
            Err(e) => {
                return StepResult::Failed {
                    fault: crate::workflow::errors::WorkflowFault::expression(
                        "switch condition evaluation failed",
                        e.to_string(),
                    ),
                };
            }
        }
    }

    // defaultCondition
    if let Some(default) = &switch.default_condition {
        let frame = TaskFrame {
            task_name: named.name.clone(),
            task_type: "switch".to_string(),
            status: TaskStatus::Completed,
            input: Some(inst.context.clone()),
            output: Some(serde_json::json!({"matched": default.transition, "default": true})),
            started_at: Some(now),
            ended_at: Some(now),
            retry_count: 0,
            pending_branches: None,
        };
        return StepResult::Goto { target: default.transition.clone(), frame };
    }

    // No match, no default → continue
    let frame = TaskFrame {
        task_name: named.name.clone(),
        task_type: "switch".to_string(),
        status: TaskStatus::Skipped,
        input: Some(inst.context.clone()),
        output: Some(serde_json::json!({"matched": null})),
        started_at: Some(now),
        ended_at: Some(now),
        retry_count: 0,
        pending_branches: None,
    };
    StepResult::NextTask(frame)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::expression::ExpressionEvaluator;
    use crate::workflow::model::{InstanceStatus, SwitchCondition};
    use crate::workflow::ports::test_utils::TestClock;

    fn make_inst(context: serde_json::Value) -> WorkflowInstance {
        WorkflowInstance {
            id: "inst-1".into(),
            definition_ns: "test".into(),
            definition_name: "test-wf".into(),
            definition_version: "1.0".into(),
            status: InstanceStatus::Running,
            context,
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
    fn test_switch_matches_first_true_condition() {
        let expr = ExpressionEvaluator::new();
        let clock = TestClock::new(1000);
        let inst = make_inst(serde_json::json!({"amount": 15000}));
        let named = NamedTask {
            name: "checkAmount".into(),
            task: crate::workflow::model::Task::Switch(SwitchTask {
                conditions: vec![
                    SwitchCondition { condition: Some(".amount > 10000".into()), transition: "senior".into() },
                    SwitchCondition { condition: Some(".amount > 5000".into()), transition: "manager".into() },
                ],
                default_condition: None,
            }),
        };

        let result = execute(&named, &SwitchTask {
            conditions: vec![
                SwitchCondition { condition: Some(".amount > 10000".into()), transition: "senior".into() },
                SwitchCondition { condition: Some(".amount > 5000".into()), transition: "manager".into() },
            ],
            default_condition: None,
        }, &inst, &expr, &clock);

        match result {
            StepResult::Goto { target, .. } => assert_eq!(target, "senior"),
            other => panic!("expected Goto, got {:?}", other),
        }
    }

    #[test]
    fn test_switch_falls_through_to_default() {
        let expr = ExpressionEvaluator::new();
        let clock = TestClock::new(1000);
        let inst = make_inst(serde_json::json!({"amount": 100}));
        let named = NamedTask {
            name: "checkAmount".into(),
            task: crate::workflow::model::Task::Switch(SwitchTask {
                conditions: vec![
                    SwitchCondition { condition: Some(".amount > 10000".into()), transition: "senior".into() },
                ],
                default_condition: Some(SwitchCondition { condition: None, transition: "director".into() }),
            }),
        };

        let result = execute(&named, &SwitchTask {
            conditions: vec![
                SwitchCondition { condition: Some(".amount > 10000".into()), transition: "senior".into() },
            ],
            default_condition: Some(SwitchCondition { condition: None, transition: "director".into() }),
        }, &inst, &expr, &clock);

        match result {
            StepResult::Goto { target, .. } => assert_eq!(target, "director"),
            other => panic!("expected Goto, got {:?}", other),
        }
    }

    #[test]
    fn test_switch_no_match_continues() {
        let expr = ExpressionEvaluator::new();
        let clock = TestClock::new(1000);
        let inst = make_inst(serde_json::json!({"amount": 100}));
        let named = NamedTask {
            name: "checkAmount".into(),
            task: crate::workflow::model::Task::Switch(SwitchTask {
                conditions: vec![
                    SwitchCondition { condition: Some(".amount > 10000".into()), transition: "senior".into() },
                ],
                default_condition: None,
            }),
        };

        let result = execute(&named, &SwitchTask {
            conditions: vec![
                SwitchCondition { condition: Some(".amount > 10000".into()), transition: "senior".into() },
            ],
            default_condition: None,
        }, &inst, &expr, &clock);

        match result {
            StepResult::NextTask(frame) => {
                assert_eq!(frame.status, TaskStatus::Skipped);
            }
            other => panic!("expected NextTask, got {:?}", other),
        }
    }

    #[test]
    fn test_switch_unconditional_branch() {
        let expr = ExpressionEvaluator::new();
        let clock = TestClock::new(1000);
        let inst = make_inst(serde_json::json!({}));
        let named = NamedTask {
            name: "alwaysGo".into(),
            task: crate::workflow::model::Task::Switch(SwitchTask {
                conditions: vec![
                    SwitchCondition { condition: None, transition: "nextStep".into() },
                ],
                default_condition: None,
            }),
        };

        let result = execute(&named, &SwitchTask {
            conditions: vec![
                SwitchCondition { condition: None, transition: "nextStep".into() },
            ],
            default_condition: None,
        }, &inst, &expr, &clock);

        match result {
            StepResult::Goto { target, .. } => assert_eq!(target, "nextStep"),
            other => panic!("expected Goto, got {:?}", other),
        }
    }
}
