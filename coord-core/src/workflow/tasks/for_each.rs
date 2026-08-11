// coord-core/workflow/tasks/for_each.rs
// for-each 任务执行 —— 集合迭代
//
// 返回 ForEach 结果，Runtime 负责：
// 1. 求值 input_expr 得到数组
// 2. 逐元素设置 iteration 变量到 context
// 3. 顺序执行 tasks
// 4. 收集结果

use crate::workflow::model::{
    ForEachTask, NamedTask, StepResult, TaskFrame, TaskStatus, WorkflowInstance,
};
use crate::workflow::ports::{Clock, ExpressionEval};

/// 执行 for-each 任务：求值 input 表达式 → ForEach
pub fn execute(
    named: &NamedTask,
    for_each: &ForEachTask,
    inst: &WorkflowInstance,
    expr: &dyn ExpressionEval,
    clock: &dyn Clock,
) -> StepResult {
    let now = clock.now_ms();

    // 求值输入表达式，验证其返回数组
    match expr.evaluate(&for_each.input, &inst.context) {
        Ok(array) => {
            if !array.is_array() {
                return StepResult::Failed {
                    fault: crate::workflow::errors::WorkflowFault::validation(
                        "for-each input must evaluate to an array",
                        format!(
                            "expression '{}' evaluated to {}",
                            for_each.input,
                            if array.is_object() { "object" } else { "scalar" }
                        ),
                    ),
                };
            }

            let frame = TaskFrame {
                task_name: named.name.clone(),
                task_type: "for_each".to_string(),
                status: TaskStatus::Running,
                input: Some(serde_json::json!({
                    "input_expr": for_each.input,
                    "iteration": for_each.iteration,
                    "item_count": array.as_array().map(|a| a.len()).unwrap_or(0),
                })),
                output: None,
                started_at: Some(now),
                ended_at: None,
                retry_count: 0,
                pending_branches: None,
            };

            StepResult::ForEach {
                input_expr: for_each.input.clone(),
                iteration: for_each.iteration.clone(),
                tasks: for_each.tasks.clone(),
                frame,
            }
        }
        Err(e) => StepResult::Failed {
            fault: crate::workflow::errors::WorkflowFault::expression(
                "for-each input expression evaluation failed",
                e.to_string(),
            ),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::expression::ExpressionEvaluator;
    use crate::workflow::model::{InstanceStatus, Task, DoTask};
    use crate::workflow::ports::test_utils::TestClock;

    fn make_inst() -> WorkflowInstance {
        WorkflowInstance {
            id: "inst-1".into(),
            definition_ns: "test".into(),
            definition_name: "test-wf".into(),
            definition_version: "1.0".into(),
            status: InstanceStatus::Running,
            context: serde_json::json!({"items": [1, 2, 3], "name": "test"}),
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
    fn test_for_each_returns_for_each_result() {
        let expr = ExpressionEvaluator::new();
        let clock = TestClock::new(1000);
        let inst = make_inst();
        let named = NamedTask {
            name: "iterateItems".into(),
            task: Task::ForEach(ForEachTask {
                input: ".items".into(),
                iteration: "item".into(),
                tasks: vec![
                    NamedTask { name: "processItem".into(), task: Task::Do(DoTask { tasks: vec![] }) },
                ],
            }),
        };

        let result = execute(&named, &ForEachTask {
            input: ".items".into(),
            iteration: "item".into(),
            tasks: vec![
                NamedTask { name: "processItem".into(), task: Task::Do(DoTask { tasks: vec![] }) },
            ],
        }, &inst, &expr, &clock);

        match result {
            StepResult::ForEach { input_expr, iteration, tasks, frame } => {
                assert_eq!(input_expr, ".items");
                assert_eq!(iteration, "item");
                assert_eq!(tasks.len(), 1);
                assert_eq!(frame.task_type, "for_each");
                assert_eq!(frame.status, TaskStatus::Running);
            }
            other => panic!("expected ForEach, got {:?}", other),
        }
    }

    #[test]
    fn test_for_each_requires_array_input() {
        let expr = ExpressionEvaluator::new();
        let clock = TestClock::new(1000);
        let inst = make_inst();
        let named = NamedTask {
            name: "badIterate".into(),
            task: Task::ForEach(ForEachTask {
                input: ".name".into(), // not an array
                iteration: "x".into(),
                tasks: vec![],
            }),
        };

        let result = execute(&named, &ForEachTask {
            input: ".name".into(),
            iteration: "x".into(),
            tasks: vec![],
        }, &inst, &expr, &clock);

        match result {
            StepResult::Failed { fault } => {
                assert_eq!(
                    fault.r#type,
                    crate::workflow::errors::error_type(crate::workflow::errors::kind::VALIDATION)
                );
                assert_eq!(fault.status, 400);
            }
            other => panic!("expected Failed, got {:?}", other),
        }
    }

    #[test]
    fn test_for_each_empty_array() {
        let expr = ExpressionEvaluator::new();
        let clock = TestClock::new(1000);
        let mut inst = make_inst();
        inst.context = serde_json::json!({"items": []});
        let named = NamedTask {
            name: "emptyIter".into(),
            task: Task::ForEach(ForEachTask {
                input: ".items".into(),
                iteration: "x".into(),
                tasks: vec![],
            }),
        };

        let result = execute(&named, &ForEachTask {
            input: ".items".into(),
            iteration: "x".into(),
            tasks: vec![],
        }, &inst, &expr, &clock);

        match result {
            StepResult::ForEach { input_expr, .. } => {
                assert_eq!(input_expr, ".items");
            }
            other => panic!("expected ForEach, got {:?}", other),
        }
    }
}
