//! Spike 2026-10 · Spike 4 语料符合性回归 —— 未实现构造 fail-closed 判据。
//!
//! 语料：`spike/2026-10/spike-4-conformance/corpus/`（官方 1.0.3 JSON Schema 合法，
//! 逐文件判定见 `evidence/matrix.tsv`）。解析走 coord-agent 部署分发同款路由：
//! `looks_like_cncf_sw` 命中 → `parse_cncf_sw_value`，否则 `parse_and_validate_yaml`。
//!
//! 锁定「接受」/「拒绝」边界：未实现构造必须在解析期显式拒绝且**错误信息点名构造**，
//! 禁止「接受但语义不同」（Spike 4 核心发现）。
//!
//! 复现：`CARGO_BUILD_JOBS=3 cargo test -p coord --test spike4_conformance -- --test-threads=1`

use std::path::PathBuf;

use coord_core::workflow::model::{Task, WorkflowDefinition};
use serde_json::Value;

fn corpus_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../spike/2026-10/spike-4-conformance/corpus")
}

/// 与 `coord-agent/src/services/workflow.rs::deploy_definition` 的路由一致。
fn parse_corpus(file: &str) -> Result<WorkflowDefinition, Vec<String>> {
    let path = corpus_dir().join(file);
    let text =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let value: Value =
        serde_yaml::from_str(&text).unwrap_or_else(|e| panic!("{file}: yaml parse: {e}"));
    if coord_core::workflow::sw::looks_like_cncf_sw(&value) {
        coord_core::workflow::sw::parse_cncf_sw_value(value).map_err(|e| vec![e])
    } else {
        coord_core::workflow::validate::parse_and_validate_yaml(&text)
            .map_err(|errs| errs.iter().map(|e| e.message.clone()).collect())
    }
}

/// 断言任务是单变量 set 且变量名/jq 表达式逐字命中（`Task::Set` 无别的语义面）。
fn assert_set_payload(task: &Task, variable: &str, value: &str) {
    match task {
        Task::Set(set) => {
            assert_eq!(set.variable, variable, "set variable mismatch");
            assert_eq!(set.value, value, "set value mismatch");
        }
        other => panic!("expected Set({variable}), got {other:?}"),
    }
}

fn assert_accepted(file: &str) {
    match parse_corpus(file) {
        Ok(_) => {}
        Err(errs) => panic!("{file}: expected parse OK, got errors {errs:?}"),
    }
}

/// 断言被拒绝，且至少一条错误信息逐字包含构造名。
fn assert_rejected_naming(file: &str, construct: &str) {
    match parse_corpus(file) {
        Ok(_) => panic!("{file}: expected rejection naming '{construct}', but parsed OK"),
        Err(errs) => assert!(
            errs.iter().any(|e| e.contains(construct)),
            "{file}: expected an error naming '{construct}', got {errs:?}"
        ),
    }
}

#[test]
fn run_script_container_shell_fail_closed() {
    // 12/13/14：coord 无脚本/容器/shell 执行面，静默落成空 ref 子流程即为误读
    assert_rejected_naming("12-run-script.yaml", "run.script");
    assert_rejected_naming("13-run-container.yaml", "run.container");
    assert_rejected_naming("14-run-shell.yaml", "run.shell");
}

#[test]
fn run_await_return_fail_closed() {
    // 18：run.await / run.return 的时序与输出语义 coord 未实现
    assert_rejected_naming("18-run-await-return.yaml", "run.await");
    assert_rejected_naming("18-run-await-return.yaml", "run.return");
}

#[test]
fn run_workflow_subflow_accepted() {
    // 15：唯一的受支持 run 形状（子流程调用）；嵌套 `workflow.*` 必须落到 WorkflowRef，
    // 不得再把 schema-legal 文档静默编译成空 ref（Spike 4 误读）。
    // corpus 15 为冻结历史证据（无 `input`）：嵌套输入映射由单元测试锁定，不在此改语料。
    let def = parse_corpus("15-run-workflow.yaml").expect("corpus 15 must parse OK");
    match &def.do_tasks[0].task {
        Task::Run(run) => {
            assert_eq!(run.workflow.namespace, "spike");
            assert_eq!(run.workflow.name, "child-workflow");
            assert_eq!(run.workflow.version, "1.0.0");
        }
        other => panic!("corpus 15: expected Run task, got {other:?}"),
    }
}

#[test]
fn use_extensions_fail_closed() {
    // 17：官方扩展点（含 before/after 任务）不执行；静默丢弃会让审计类扩展被误信生效
    assert_rejected_naming("17-use-extensions.yaml", "use.extensions");
}

#[test]
fn supported_constructs_still_accepted() {
    // matrix.tsv PASS：grpc 调用与 use.functions 内已定义函数调用
    assert_accepted("21-call-grpc.yaml");
    assert_accepted("24-use-functions.yaml");
}

#[test]
fn matrix_gap_files_still_rejected() {
    // matrix.tsv GAP：16/23 被 `call: <未定义函数>` 的**未知函数引用校验**拒绝，
    // 不是未实现构造拒绝；fail-closed 结果可接受。
    assert_rejected_naming("16-call-function.yaml", "myFunction");
    assert_rejected_naming("23-call-openapi.yaml", "openapi");
}

#[test]
fn official_switch_when_then_shape_maps() {
    // 04：官方 case 形状 {caseName: {when, then}} 映射为 {condition, transition}；
    // `then: exit` → 终端任务 `__end`。
    let def = parse_corpus("04-switch.yaml").expect("corpus 04 must parse OK");
    match &def.do_tasks[0].task {
        Task::Switch(s) => {
            assert_eq!(s.conditions.len(), 2);
            assert_eq!(
                s.conditions[0].condition.as_deref(),
                Some("${ .counter > 10 }")
            );
            assert_eq!(s.conditions[0].transition, "__end");
            assert_eq!(
                s.conditions[1].condition.as_deref(),
                Some("${ .counter <= 10 }")
            );
            assert_eq!(s.conditions[1].transition, "__end");
        }
        other => panic!("corpus 04: expected Switch task, got {other:?}"),
    }
    assert!(
        def.do_tasks
            .iter()
            .any(|t| t.name == "__end" && matches!(&t.task, Task::End(_))),
        "then: exit must synthesize the __end terminal task"
    );
}

#[test]
fn matrix_pass_corpus_still_parses() {
    // matrix.tsv PASS 且与顺序执行语义一致：末任务的 `then: exit` 等价于工作流结束，
    // 必须继续接受（该断言同时锁住 `then` 规则的边界，防止后续收紧时静默破坏 PASS 集）。
    //
    // 05/06/07/08/09 的官方形状已逐字段适配，断言见本文件各自的 `*_maps_*` /
    // `emit_event_with_payload_is_mapped` 测试；本列表中的语料语义均已对齐。唯一仍被
    // 静默接受的是 LAX 类（有意兼容）：corpus 19 的 0.8 遗留 `states/start`，走 cncf_sw
    // 路线接受而 schema 判非法，不在 1.0.3 标准子集内，故不列于此列表。
    for file in [
        "01-minimal-set.yaml",
        "02-call-http.yaml",
        "03-do-task.yaml",
        "05-fork.yaml",
        "06-for.yaml",
        "07-wait.yaml",
        "08-listen.yaml",
        "09-emit.yaml",
        "10-raise.yaml",
        "11-try-catch.yaml",
        "22-if-condition.yaml",
    ] {
        assert_accepted(file);
    }
}

#[test]
fn emit_event_with_payload_is_mapped() {
    // 09：官方 `emit.event.with.{type,source,data}` 必须逐字段映射进 Task::Emit，
    // 不得静默丢弃载荷（Spike 4 误读之一）。
    let def = parse_corpus("09-emit.yaml").expect("corpus 09 must parse OK");
    match &def.do_tasks[0].task {
        Task::Emit(e) => {
            assert_eq!(e.emit.event_type, "com.example.notify");
            assert_eq!(e.emit.source.as_deref(), Some("https://example.com/coord"));
            assert_eq!(e.emit.data, Some(serde_json::json!({ "msg": "hi" })));
        }
        other => panic!("corpus 09: expected Emit task, got {other:?}"),
    }
}

#[test]
fn listen_official_to_one_with_maps_filter() {
    // 08：官方 `listen.to.one.with.type` 必须映射进 Task::Listen 过滤条件，
    // 不得静默丢过滤器（Spike 4 误读之一）。
    let def = parse_corpus("08-listen.yaml").expect("corpus 08 must parse OK");
    match &def.do_tasks[0].task {
        Task::Listen(l) => assert_eq!(l.listen.event_type.as_deref(), Some("com.example.approved")),
        other => panic!("corpus 08: expected Listen task, got {other:?}"),
    }
}

#[test]
fn fork_official_branch_map_maps_branches() {
    // 05：官方 `fork.branches` 是单键 map 列表 `{<branch_name>: <taskBody>}`，分支名取自
    // 键名、tasks 取自 body；不得再落成空名 0 任务分支（Spike 4 误读之一）。
    let def = parse_corpus("05-fork.yaml").expect("corpus 05 must parse OK");
    match &def.do_tasks[0].task {
        Task::Fork(fork) => {
            assert_eq!(fork.branches.len(), 2);
            assert_eq!(fork.branches[0].name, "branch_a");
            assert_eq!(fork.branches[1].name, "branch_b");
            assert_eq!(fork.branches[0].tasks.len(), 1);
            assert_eq!(fork.branches[0].tasks[0].name, "branch_a");
            assert_eq!(fork.branches[1].tasks.len(), 1);
            assert_eq!(fork.branches[1].tasks[0].name, "branch_b");
            assert_set_payload(&fork.branches[0].tasks[0].task, "x", "1");
            assert_set_payload(&fork.branches[1].tasks[0].task, "y", "2");
        }
        other => panic!("corpus 05: expected Fork task, got {other:?}"),
    }
}

#[test]
fn for_official_each_in_maps_iteration() {
    // 06：官方 `for: {each, in}` 的迭代语义必须接线：iteration 取 each、input 取 in、
    // tasks 取与 `for` 同级的 `do`；不得再把循环体当顺序任务、丢掉迭代（Spike 4 误读之一）。
    let def = parse_corpus("06-for.yaml").expect("corpus 06 must parse OK");
    match &def.do_tasks[0].task {
        Task::ForEach(fe) => {
            assert_eq!(fe.iteration, "item");
            assert_eq!(fe.input, "${ .items }");
            assert_eq!(fe.tasks.len(), 1);
            assert_eq!(fe.tasks[0].name, "process");
            assert_set_payload(&fe.tasks[0].task, "last", "${ $item }");
        }
        other => panic!("corpus 06: expected ForEach task, got {other:?}"),
    }
}

#[test]
fn wait_official_duration_object_maps_iso8601() {
    // 07：官方 `wait: {seconds, ...}` 组件对象必须合成为 ISO8601 duration 进 WaitTask，
    // 不得静默落成空 wait（Spike 4 误读之一）。
    let def = parse_corpus("07-wait.yaml").expect("corpus 07 must parse OK");
    match &def.do_tasks[0].task {
        Task::Wait(w) => assert_eq!(w.wait, "PT5S"),
        other => panic!("corpus 07: expected Wait task, got {other:?}"),
    }
}
