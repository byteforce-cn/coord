// coord-core/workflow/tasks/try_catch.rs
// try-catch 任务执行 —— 异常处理
//
// 返回 TryBlock 结果，Runtime 负责：
// 1. 顺序执行 try_tasks
// 2. 若任一 try 任务 Failed，按顺序匹配 catch_clauses
// 3. 执行首个匹配的 catch.tasks
// 4. 若全部 try 成功，跳过 catch 继续

use crate::workflow::model::{
    NamedTask, StepResult, TaskFrame, TaskStatus, TryCatchTask, WorkflowInstance,
};
use crate::workflow::ports::Clock;

/// 执行 try-catch 任务：构建 try/catch 结构 → TryBlock
pub fn execute(
    named: &NamedTask,
    try_catch: &TryCatchTask,
    _inst: &WorkflowInstance,
    clock: &dyn Clock,
) -> StepResult {
    let now = clock.now_ms();

    let frame = TaskFrame {
        task_name: named.name.clone(),
        task_type: "try_catch".to_string(),
        status: TaskStatus::Running,
        input: Some(serde_json::json!({
            "try_count": try_catch.r#try.len(),
            "catch_count": try_catch.catch.len(),
        })),
        output: None,
        started_at: Some(now),
        ended_at: None,
        retry_count: 0,
        pending_branches: None,
    };

    StepResult::TryBlock {
        try_tasks: try_catch.r#try.clone(),
        catch_clauses: try_catch.catch.clone(),
        frame,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::model::{CatchClause, DoTask, InstanceStatus, NamedTask, Task, WorkflowInstance};
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
    fn test_try_catch_returns_try_block() {
        let clock = TestClock::new(1000);
        let inst = make_inst();
        let named = NamedTask {
            name: "safeOperation".into(),
            task: Task::TryCatch(TryCatchTask {
                r#try: vec![
                    NamedTask { name: "doRisky".into(), task: Task::Do(DoTask { tasks: vec![] }) },
                ],
                catch: vec![
                    CatchClause {
                        errors: Some(vec!["timeout".into()]),
                        tasks: vec![
                            NamedTask { name: "onTimeout".into(), task: Task::Do(DoTask { tasks: vec![] }) },
                        ],
                    },
                ],
            }),
        };

        let result = execute(&named, &TryCatchTask {
            r#try: vec![
                NamedTask { name: "doRisky".into(), task: Task::Do(DoTask { tasks: vec![] }) },
            ],
            catch: vec![
                CatchClause {
                    errors: Some(vec!["timeout".into()]),
                    tasks: vec![
                        NamedTask { name: "onTimeout".into(), task: Task::Do(DoTask { tasks: vec![] }) },
                    ],
                },
            ],
        }, &inst, &clock);

        match result {
            StepResult::TryBlock { try_tasks, catch_clauses, frame } => {
                assert_eq!(try_tasks.len(), 1);
                assert_eq!(try_tasks[0].name, "doRisky");
                assert_eq!(catch_clauses.len(), 1);
                assert_eq!(catch_clauses[0].errors.as_ref().unwrap(), &vec!["timeout".to_string()]);
                assert_eq!(frame.task_type, "try_catch");
            }
            other => panic!("expected TryBlock, got {:?}", other),
        }
    }

    #[test]
    fn test_try_catch_multiple_catch_clauses() {
        let clock = TestClock::new(1000);
        let inst = make_inst();
        let named = NamedTask {
            name: "multiCatch".into(),
            task: Task::TryCatch(TryCatchTask {
                r#try: vec![],
                catch: vec![
                    CatchClause { errors: Some(vec!["timeout".into()]), tasks: vec![] },
                    CatchClause { errors: None, tasks: vec![] },
                ],
            }),
        };

        let result = execute(&named, &TryCatchTask {
            r#try: vec![],
            catch: vec![
                CatchClause { errors: Some(vec!["timeout".into()]), tasks: vec![] },
                CatchClause { errors: None, tasks: vec![] },
            ],
        }, &inst, &clock);

        match result {
            StepResult::TryBlock { catch_clauses, .. } => {
                assert_eq!(catch_clauses.len(), 2);
            }
            other => panic!("expected TryBlock, got {:?}", other),
        }
    }

    #[test]
    fn test_try_catch_catch_all_with_none_errors() {
        let clock = TestClock::new(1000);
        let inst = make_inst();
        let named = NamedTask {
            name: "catchAll".into(),
            task: Task::TryCatch(TryCatchTask {
                r#try: vec![],
                catch: vec![
                    CatchClause { errors: None, tasks: vec![
                        NamedTask { name: "handleAny".into(), task: Task::Do(DoTask { tasks: vec![] }) },
                    ]},
                ],
            }),
        };

        let result = execute(&named, &TryCatchTask {
            r#try: vec![],
            catch: vec![
                CatchClause { errors: None, tasks: vec![
                    NamedTask { name: "handleAny".into(), task: Task::Do(DoTask { tasks: vec![] }) },
                ]},
            ],
        }, &inst, &clock);

        match result {
            StepResult::TryBlock { catch_clauses, .. } => {
                assert_eq!(catch_clauses.len(), 1);
                assert!(catch_clauses[0].errors.is_none()); // catches all
            }
            other => panic!("expected TryBlock, got {:?}", other),
        }
    }
}
