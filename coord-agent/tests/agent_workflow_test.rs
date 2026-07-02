// coord-agent: Workflow DSL 解释器测试（Phase G）
//
// TDD RED phase: 测试 WorkflowInterpreter（Serverless Workflow DSL 执行引擎）。
// 支持 Operation/Delay/Event/Switch/Parallel/Terminate 状态。
//
// 参见 docs/client-agent-architecture-v3.md §5.9。

use std::collections::BTreeMap;
use std::sync::Arc;

use coord_agent::services::workflow::{
    WorkflowDsl, WorkflowStateDef, WorkflowInterpreter, WorkflowContext,
    InterpreterResult,
};

// ──── helpers ────

fn make_context() -> WorkflowContext {
    WorkflowContext {
        instance_id: "test-inst-1".into(),
        workflow_name: "test-wf".into(),
        input: b"{}".to_vec(),
        variables: BTreeMap::new(),
        current_state: String::new(), // empty = start from DSL startState
        step_count: 0,
        max_steps: 100,
        pending_branches: Vec::new(),
    }
}

fn make_simple_dsl() -> WorkflowDsl {
    WorkflowDsl {
        name: "simple-wf".into(),
        version: "1.0".into(),
        start_state: "step1".to_string(),
        states: {
            let mut m = BTreeMap::new();
            m.insert("step1".to_string(), WorkflowStateDef {
                name: "step1".into(),
                state_type: "operation".into(),
                action: Some("echo:hello".into()),
                next_state: Some("step2".into()),
                ..Default::default()
            });
            m.insert("step2".to_string(), WorkflowStateDef {
                name: "step2".into(),
                state_type: "operation".into(),
                action: Some("echo:world".into()),
                next_state: None, // terminal
                ..Default::default()
            });
            m
        },
    }
}

fn make_switch_dsl() -> WorkflowDsl {
    WorkflowDsl {
        name: "switch-wf".into(),
        version: "1.0".into(),
        start_state: "check".to_string(),
        states: {
            let mut m = BTreeMap::new();
            m.insert("check".to_string(), WorkflowStateDef {
                name: "check".into(),
                state_type: "switch".into(),
                conditions: {
                    let mut c = BTreeMap::new();
                    c.insert("$.value == 1".into(), "path_a".into());
                    c.insert("$.value == 2".into(), "path_b".into());
                    c
                },
                default_next: Some("default_path".into()),
                ..Default::default()
            });
            m.insert("path_a".to_string(), WorkflowStateDef {
                name: "path_a".into(),
                state_type: "operation".into(),
                action: Some("result:A".into()),
                next_state: None,
                ..Default::default()
            });
            m.insert("path_b".to_string(), WorkflowStateDef {
                name: "path_b".into(),
                state_type: "operation".into(),
                action: Some("result:B".into()),
                next_state: None,
                ..Default::default()
            });
            m.insert("default_path".to_string(), WorkflowStateDef {
                name: "default_path".into(),
                state_type: "operation".into(),
                action: Some("result:default".into()),
                next_state: None,
                ..Default::default()
            });
            m
        },
    }
}

fn make_parallel_dsl() -> WorkflowDsl {
    WorkflowDsl {
        name: "parallel-wf".into(),
        version: "1.0".into(),
        start_state: "fork".to_string(),
        states: {
            let mut m = BTreeMap::new();
            m.insert("fork".to_string(), WorkflowStateDef {
                name: "fork".into(),
                state_type: "parallel".into(),
                branches: vec!["branch_a".to_string(), "branch_b".to_string()],
                join_state: Some("join".into()),
                ..Default::default()
            });
            m.insert("branch_a".to_string(), WorkflowStateDef {
                name: "branch_a".into(),
                state_type: "operation".into(),
                action: Some("task:A".into()),
                next_state: Some("join".into()),
                ..Default::default()
            });
            m.insert("branch_b".to_string(), WorkflowStateDef {
                name: "branch_b".into(),
                state_type: "operation".into(),
                action: Some("task:B".into()),
                next_state: Some("join".into()),
                ..Default::default()
            });
            m.insert("join".to_string(), WorkflowStateDef {
                name: "join".into(),
                state_type: "operation".into(),
                action: Some("merge_results".into()),
                next_state: None,
                ..Default::default()
            });
            m
        },
    }
}

// ──── G.1: DSL 解析 ────

#[test]
fn test_parse_simple_dsl_json() {
    let json = r#"{
        "name": "test",
        "version": "1.0",
        "startState": "s1",
        "states": {
            "s1": { "type": "operation", "action": "do:thing", "next": "s2" },
            "s2": { "type": "operation", "action": "do:other" }
        }
    }"#;
    let dsl = WorkflowDsl::from_json(json.as_bytes()).expect("parse should succeed");
    assert_eq!(dsl.name, "test");
    assert_eq!(dsl.start_state, "s1");
    assert_eq!(dsl.states.len(), 2);
    assert_eq!(dsl.states["s1"].state_type, "operation");
    assert_eq!(dsl.states["s1"].next_state.as_deref(), Some("s2"));
    assert_eq!(dsl.states["s2"].next_state, None);
}

#[test]
fn test_parse_invalid_dsl() {
    let result = WorkflowDsl::from_json(b"not json");
    assert!(result.is_err());
}

#[test]
fn test_parse_dsl_missing_start_state() {
    let json = r#"{"name": "test", "version": "1.0", "states": {}}"#;
    let result = WorkflowDsl::from_json(json.as_bytes());
    assert!(result.is_err());
}

// ──── G.2: 顺序执行 ────

#[test]
fn test_interpreter_sequential_execution() {
    let dsl = make_simple_dsl();
    let mut ctx = make_context();
    let interpreter = WorkflowInterpreter::new(dsl);

    // Step 1: executes startState "step1" → transitions to "step2"
    let result = interpreter.step(&mut ctx).expect("step should succeed");
    assert_eq!(result, InterpreterResult::Transitioned("step2".into()));
    assert_eq!(ctx.current_state, "step2");

    // Step 2: executes "step2" → terminal state, workflow completes
    let result = interpreter.step(&mut ctx).expect("step should succeed");
    assert_eq!(result, InterpreterResult::Completed);
}

// ──── G.3: Switch 条件分支 ────

#[test]
fn test_interpreter_switch_path_a() {
    let dsl = make_switch_dsl();
    let mut ctx = make_context();
    ctx.variables.insert("value".to_string(), serde_json::Value::Number(1.into()));
    let interpreter = WorkflowInterpreter::new(dsl);

    // Step 1: executes "check" (switch) → value==1 → "path_a"
    let result = interpreter.step(&mut ctx).expect("step should succeed");
    assert_eq!(result, InterpreterResult::Transitioned("path_a".into()));

    // Step 2: executes "path_a" → terminal
    let result = interpreter.step(&mut ctx).expect("step should succeed");
    assert_eq!(result, InterpreterResult::Completed);
}

#[test]
fn test_interpreter_switch_default() {
    let dsl = make_switch_dsl();
    let mut ctx = make_context();
    ctx.variables.insert("value".to_string(), serde_json::Value::Number(99.into()));
    let interpreter = WorkflowInterpreter::new(dsl);

    // Step 1: executes "check" (switch) → no match → default → "default_path"
    let result = interpreter.step(&mut ctx).expect("step should succeed");
    assert_eq!(result, InterpreterResult::Transitioned("default_path".into()));
}

// ──── G.4: 并行分支 ────

#[test]
fn test_interpreter_parallel_execution() {
    let dsl = make_parallel_dsl();
    let mut ctx = make_context();
    let interpreter = WorkflowInterpreter::new(dsl);

    // Step 1: executes "fork" (parallel) → Forked
    let result = interpreter.step(&mut ctx).expect("step should succeed");
    assert!(matches!(result, InterpreterResult::Forked { .. }));

    // After fork, the interpreter sets current_state to first branch
    // Execute remaining steps until completion
    let mut completed = false;
    for _ in 0..20 {
        match interpreter.step(&mut ctx).expect("step should succeed") {
            InterpreterResult::Completed => { completed = true; break; }
            _ => {}
        }
    }
    assert!(completed, "parallel workflow should complete");
}

// ──── G.5: Delay 状态 ────

#[test]
fn test_interpreter_delay_state() {
    let dsl = WorkflowDsl {
        name: "delay-wf".into(),
        version: "1.0".into(),
        start_state: "wait".to_string(),
        states: {
            let mut m = BTreeMap::new();
            m.insert("wait".to_string(), WorkflowStateDef {
                name: "wait".into(),
                state_type: "delay".into(),
                delay_seconds: Some(0), // immediate for test
                next_state: Some("done".into()),
                ..Default::default()
            });
            m.insert("done".to_string(), WorkflowStateDef {
                name: "done".into(),
                state_type: "operation".into(),
                action: Some("finalize".into()),
                next_state: None,
                ..Default::default()
            });
            m
        },
    };

    let mut ctx = make_context();
    let interpreter = WorkflowInterpreter::new(dsl);

    // Step 1: executes "wait" (delay=0) → transitions to "done"
    let result = interpreter.step(&mut ctx).expect("step should succeed");
    assert_eq!(result, InterpreterResult::Transitioned("done".into()));
}

// ──── G.6: 最大步数保护 ────

#[test]
fn test_interpreter_max_steps_protection() {
    // Create a looping workflow
    let dsl = WorkflowDsl {
        name: "loop-wf".into(),
        version: "1.0".into(),
        start_state: "loop".to_string(),
        states: {
            let mut m = BTreeMap::new();
            m.insert("loop".to_string(), WorkflowStateDef {
                name: "loop".into(),
                state_type: "operation".into(),
                action: Some("tick".into()),
                next_state: Some("loop".into()), // self-loop
                ..Default::default()
            });
            m
        },
    };

    let mut ctx = make_context();
    ctx.max_steps = 5;
    let interpreter = WorkflowInterpreter::new(dsl);

    // Should error after max_steps
    for _ in 0..5 {
        interpreter.step(&mut ctx).expect("step should succeed");
    }
    let result = interpreter.step(&mut ctx);
    assert!(result.is_err(), "should error after max steps");
}

// ──── G.7: 完整工作流运行 ────

#[test]
fn test_interpreter_run_to_completion() {
    let dsl = make_simple_dsl();
    let mut ctx = make_context();
    let interpreter = WorkflowInterpreter::new(dsl);

    let result = interpreter.run(&mut ctx).expect("run should succeed");
    assert_eq!(result, InterpreterResult::Completed);
    assert!(ctx.step_count >= 2);
}

// ──── G.8: 工作流 DSL 序列化往返 ────

#[test]
fn test_dsl_roundtrip_json() {
    let dsl = make_switch_dsl();
    let json = dsl.to_json().expect("serialize");
    let parsed = WorkflowDsl::from_json(&json).expect("deserialize");
    assert_eq!(dsl.name, parsed.name);
    assert_eq!(dsl.states.len(), parsed.states.len());
}
