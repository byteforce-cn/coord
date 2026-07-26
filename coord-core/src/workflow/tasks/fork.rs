// coord-core/workflow/tasks/fork.rs
// fork 任务执行 —— 并行分支
//
// 返回 Fork(Fork) 结果，Runtime 负责分支调度：
// - compete=false: 顺序执行所有分支，collect 结果
// - compete=true: 执行分支，首个完成者胜出

use crate::workflow::model::{
    ForkTask, NamedTask, StepResult, TaskFrame, TaskStatus, WorkflowInstance,
};
use crate::workflow::ports::Clock;

/// 执行 fork 任务：构建分支信息 → Fork
pub fn execute(
    named: &NamedTask,
    fork: &ForkTask,
    _inst: &WorkflowInstance,
    clock: &dyn Clock,
) -> StepResult {
    let now = clock.now_ms();

    let branch_names: Vec<String> = fork.branches.iter().map(|b| b.name.clone()).collect();
    let compete = fork.compete.unwrap_or(false);

    let frame = TaskFrame {
        task_name: named.name.clone(),
        task_type: "fork".to_string(),
        status: TaskStatus::Running,
        input: Some(serde_json::json!({
            "branch_count": fork.branches.len(),
            "compete": compete,
            "branches": branch_names,
        })),
        output: None,
        started_at: Some(now),
        ended_at: None,
        retry_count: 0,
        pending_branches: Some(branch_names),
    };

    StepResult::Fork {
        branches: fork.branches.clone(),
        compete,
        frame,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::model::{ForkBranch, InstanceStatus, Task};
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

    fn make_branches() -> Vec<ForkBranch> {
        vec![
            ForkBranch {
                name: "branchA".into(),
                tasks: vec![
                    NamedTask { name: "a1".into(), task: Task::Do(crate::workflow::model::DoTask { tasks: vec![] }) },
                ],
            },
            ForkBranch {
                name: "branchB".into(),
                tasks: vec![
                    NamedTask { name: "b1".into(), task: Task::Do(crate::workflow::model::DoTask { tasks: vec![] }) },
                ],
            },
        ]
    }

    #[test]
    fn test_fork_returns_fork_result() {
        let clock = TestClock::new(1000);
        let inst = make_inst();
        let branches = make_branches();
        let named = NamedTask {
            name: "parallel".into(),
            task: Task::Fork(ForkTask { branches: branches.clone(), compete: None }),
        };

        let result = execute(&named, &ForkTask {
            branches: branches.clone(),
            compete: None,
        }, &inst, &clock);

        match result {
            StepResult::Fork { branches: b, compete, frame } => {
                assert_eq!(b.len(), 2);
                assert_eq!(b[0].name, "branchA");
                assert_eq!(b[1].name, "branchB");
                assert!(!compete);
                assert_eq!(frame.task_type, "fork");
                assert_eq!(frame.status, TaskStatus::Running);
                assert_eq!(frame.pending_branches, Some(vec!["branchA".into(), "branchB".into()]));
            }
            other => panic!("expected Fork, got {:?}", other),
        }
    }

    #[test]
    fn test_fork_compete_mode() {
        let clock = TestClock::new(1000);
        let inst = make_inst();
        let branches = make_branches();
        let named = NamedTask {
            name: "race".into(),
            task: Task::Fork(ForkTask {
                branches: branches.clone(),
                compete: Some(true),
            }),
        };

        let result = execute(&named, &ForkTask {
            branches,
            compete: Some(true),
        }, &inst, &clock);

        match result {
            StepResult::Fork { compete, .. } => {
                assert!(compete);
            }
            other => panic!("expected Fork, got {:?}", other),
        }
    }

    #[test]
    fn test_fork_empty_branches() {
        let clock = TestClock::new(1000);
        let inst = make_inst();
        let named = NamedTask {
            name: "emptyFork".into(),
            task: Task::Fork(ForkTask { branches: vec![], compete: None }),
        };

        let result = execute(&named, &ForkTask {
            branches: vec![],
            compete: None,
        }, &inst, &clock);

        match result {
            StepResult::Fork { branches, .. } => {
                assert!(branches.is_empty());
            }
            other => panic!("expected Fork, got {:?}", other),
        }
    }
}
