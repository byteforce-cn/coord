// coord-core/workflow/tasks/set.rs
// set 任务执行 —— 变量赋值
//
// jq 表达式求值 → 返回 SetVariable（Runtime 写入 context）

use crate::workflow::model::{
    NamedTask, SetTask, StepResult, TaskFrame, TaskStatus, WorkflowInstance,
};
use crate::workflow::ports::{Clock, ExpressionEval};

/// 执行 set 任务：求值表达式 → SetVariable
pub fn execute(
    named: &NamedTask,
    set: &SetTask,
    inst: &WorkflowInstance,
    expr: &dyn ExpressionEval,
    clock: &dyn Clock,
) -> StepResult {
    let now = clock.now_ms();

    match expr.evaluate(&set.value, &inst.context) {
        Ok(value) => {
            let frame = TaskFrame {
                task_name: named.name.clone(),
                task_type: "set".to_string(),
                status: TaskStatus::Completed,
                input: Some(serde_json::json!({"variable": set.variable, "expr": set.value})),
                output: Some(value.clone()),
                started_at: Some(now),
                ended_at: Some(now),
                retry_count: 0,
                pending_branches: None,
            };
            StepResult::SetVariable {
                variable: set.variable.clone(),
                value,
                frame,
            }
        }
        Err(e) => StepResult::Failed {
            fault: crate::workflow::errors::WorkflowFault::expression(
                format!("set variable '{}' evaluation failed", set.variable),
                e.to_string(),
            ),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::expression::ExpressionEvaluator;
    use crate::workflow::model::{InstanceStatus, Task};
    use crate::workflow::ports::test_utils::TestClock;

    fn make_inst() -> WorkflowInstance {
        WorkflowInstance {
            id: "inst-1".into(),
            definition_ns: "test".into(),
            definition_name: "test-wf".into(),
            definition_version: "1.0".into(),
            status: InstanceStatus::Running,
            context: serde_json::json!({"amount": 500, "name": "test"}),
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
    fn test_set_variable_literal() {
        let expr = ExpressionEvaluator::new();
        let clock = TestClock::new(1000);
        let inst = make_inst();
        let named = NamedTask {
            name: "setResult".into(),
            task: Task::Set(SetTask { variable: "result".into(), value: "42".into() }),
        };

        let result = execute(&named, &SetTask {
            variable: "result".into(), value: "42".into(),
        }, &inst, &expr, &clock);

        match result {
            StepResult::SetVariable { variable, value, frame } => {
                assert_eq!(variable, "result");
                // 整数按 i64 解析
                assert_eq!(value, serde_json::json!(42));
                assert_eq!(frame.task_type, "set");
                assert_eq!(frame.status, TaskStatus::Completed);
            }
            other => panic!("expected SetVariable, got {:?}", other),
        }
    }

    #[test]
    fn test_set_variable_from_context() {
        let expr = ExpressionEvaluator::new();
        let clock = TestClock::new(1000);
        let inst = make_inst();
        // expression evaluator supports +, -, not *; use .amount + .amount
        let named = NamedTask {
            name: "doubleAmount".into(),
            task: Task::Set(SetTask { variable: "doubled".into(), value: ".amount + .amount".into() }),
        };

        let result = execute(&named, &SetTask {
            variable: "doubled".into(), value: ".amount + .amount".into(),
        }, &inst, &expr, &clock);

        match result {
            StepResult::SetVariable { variable, value, .. } => {
                assert_eq!(variable, "doubled");
                // 整数保持
                assert_eq!(value, serde_json::json!(1000));
            }
            other => panic!("expected SetVariable, got {:?}", other),
        }
    }

    #[test]
    fn test_set_variable_concat() {
        let expr = ExpressionEvaluator::new();
        let clock = TestClock::new(1000);
        let inst = make_inst();
        let named = NamedTask {
            name: "concat".into(),
            task: Task::Set(SetTask { variable: "greeting".into(), value: "\"Hello, \" + .name".into() }),
        };

        let result = execute(&named, &SetTask {
            variable: "greeting".into(), value: "\"Hello, \" + .name".into(),
        }, &inst, &expr, &clock);

        match result {
            StepResult::SetVariable { variable, value, .. } => {
                assert_eq!(variable, "greeting");
                assert_eq!(value, serde_json::json!("Hello, test"));
            }
            other => panic!("expected SetVariable, got {:?}", other),
        }
    }

    #[test]
    fn test_set_variable_path_access() {
        let expr = ExpressionEvaluator::new();
        let clock = TestClock::new(1000);
        let inst = make_inst();
        let named = NamedTask {
            name: "getAmount".into(),
            task: Task::Set(SetTask { variable: "copied".into(), value: ".amount".into() }),
        };

        let result = execute(&named, &SetTask {
            variable: "copied".into(), value: ".amount".into(),
        }, &inst, &expr, &clock);

        match result {
            StepResult::SetVariable { variable, value, .. } => {
                assert_eq!(variable, "copied");
                assert_eq!(value, serde_json::json!(500));
            }
            other => panic!("expected SetVariable, got {:?}", other),
        }
    }

    #[test]
    fn test_set_unknown_expression_returns_context() {
        // 表达式求值器对无法识别的表达式返回上下文本身（防御性行为）
        let expr = ExpressionEvaluator::new();
        let clock = TestClock::new(1000);
        let inst = make_inst();
        let named = NamedTask {
            name: "unknownExpr".into(),
            task: Task::Set(SetTask { variable: "x".into(), value: "!!!invalid!!!".into() }),
        };

        let result = execute(&named, &SetTask {
            variable: "x".into(), value: "!!!invalid!!!".into(),
        }, &inst, &expr, &clock);

        match result {
            StepResult::SetVariable { variable, value, .. } => {
                assert_eq!(variable, "x");
                // 无法识别时返回 context
                assert_eq!(value, inst.context);
            }
            other => panic!("expected SetVariable, got {:?}", other),
        }
    }
}
