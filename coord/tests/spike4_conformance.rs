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

use coord_core::workflow::model::WorkflowDefinition;
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
    // 15：唯一的受支持 run 形状（子流程调用），保持现行语义
    assert_accepted("15-run-workflow.yaml");
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
fn documented_gaps_stay_fail_closed() {
    // matrix.tsv GAP：coord 按函数名解析，未定义函数被引用校验拒绝（fail-closed 可接受）
    assert_rejected_naming("16-call-function.yaml", "myFunction");
    assert_rejected_naming("23-call-openapi.yaml", "openapi");
}

#[test]
fn official_switch_then_fail_closed() {
    // 04：官方 case 形状 {caseName: {when, then}}；保留拒绝并点名被忽略的 `then`
    assert_rejected_naming("04-switch.yaml", "then");
}

#[test]
fn semantically_aligned_corpus_still_accepted() {
    // matrix.tsv PASS：仅用顺序执行 + switch transition 的语料；
    // 末任务的 `then: exit` 与工作流结束等价，必须继续接受。
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
