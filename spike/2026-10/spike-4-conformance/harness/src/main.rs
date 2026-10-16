//! SPIKE 2026-10 Spike 4 — 用 coord 真实部署分发路径扫描官方 1.0.3 语料。
//!
//! 分发逻辑与 `coord-agent/src/services/workflow.rs:2137` 一致：
//! `looks_like_cncf_sw` 命中则走 `sw::parse_cncf_sw_value`，否则走
//! `validate::parse_and_validate_yaml`（document/do 方言）。
//! 除接受/拒绝外，还导出解析后的顶层任务摘要，用于识别「接受但静默误读」。
//!
//! 用法：`spike-conformance <corpus-dir>` → stdout JSON。

use coord_core::workflow::model::{Task, WorkflowDefinition};
use serde_json::{json, Value};
use std::path::PathBuf;

fn collect_key_names(v: &Value, key: &str, out: &mut Vec<String>) {
    match v {
        Value::Object(m) => {
            for (k, val) in m {
                if k == key {
                    match val {
                        Value::Object(inner) => out.extend(inner.keys().cloned()),
                        other => out.push(other.to_string()),
                    }
                }
                collect_key_names(val, key, out);
            }
        }
        Value::Array(a) => a.iter().for_each(|x| collect_key_names(x, key, out)),
        _ => {}
    }
}

fn has_key(v: &Value, key: &str) -> bool {
    match v {
        Value::Object(m) => m.iter().any(|(k, val)| k == key || has_key(val, key)),
        Value::Array(a) => a.iter().any(|x| has_key(x, key)),
        _ => false,
    }
}

fn summarize_task(t: &Task) -> Value {
    match t {
        Task::Call(c) => json!({"kind": "call", "call_type": format!("{:?}", c.call)}),
        Task::Do(d) => json!({"kind": "do", "sub_tasks": d.tasks.len()}),
        Task::Switch(s) => json!({"kind": "switch", "conditions": s.conditions.len()}),
        Task::Fork(f) => {
            json!({"kind": "fork", "branches": f.branches.len(), "compete": f.compete})
        }
        Task::ForEach(_) => json!({"kind": "for_each"}),
        Task::Wait(_) => json!({"kind": "wait"}),
        Task::Listen(_) => json!({"kind": "listen"}),
        Task::Emit(_) => json!({"kind": "emit"}),
        Task::Set(_) => json!({"kind": "set"}),
        Task::Raise(_) => json!({"kind": "raise"}),
        Task::TryCatch(_) => json!({"kind": "try_catch"}),
        Task::Run(r) => json!({
            "kind": "run_subflow",
            "workflow_ref": {
                "namespace": r.workflow.namespace,
                "name": r.workflow.name,
                "version": r.workflow.version,
            }
        }),
        Task::End(_) => json!({"kind": "end"}),
        Task::Approve(_) => json!({"kind": "approve"}),
    }
}

fn summarize_def(def: &WorkflowDefinition) -> Value {
    Value::Array(
        def.do_tasks
            .iter()
            .map(|named| {
                let meta = def.task_meta.get(&named.name);
                json!({
                    "name": named.name,
                    "task": summarize_task(&named.task),
                    "meta_if": meta.and_then(|m| m.if_condition.clone()),
                    "meta_timeout": meta.and_then(|m| m.timeout.clone().map(|t| t.after.clone())),
                })
            })
            .collect(),
    )
}

fn main() {
    let dir = std::env::args()
        .nth(1)
        .expect("usage: spike-conformance <corpus-dir>");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("corpus dir")
        .filter_map(|e| {
            let p = e.ok()?.path();
            let ext = p.extension().and_then(|x| x.to_str()).unwrap_or("");
            matches!(ext, "yaml" | "yml" | "json").then_some(p)
        })
        .collect();
    files.sort();

    let mut results = Vec::new();
    for p in files {
        let file = p.file_name().unwrap().to_string_lossy().to_string();
        let text = std::fs::read_to_string(&p).unwrap();
        let value: Value = match serde_yaml::from_str(&text) {
            Ok(v) => v,
            Err(e) => {
                results.push(json!({"file": file, "yaml_ok": false, "error": e.to_string()}));
                continue;
            }
        };

        let looks_sw = coord_core::workflow::sw::looks_like_cncf_sw(&value);
        let route = if looks_sw {
            "cncf_sw(states/start)"
        } else {
            "document/do"
        };
        let parsed: Result<WorkflowDefinition, Vec<String>> = if looks_sw {
            coord_core::workflow::sw::parse_cncf_sw_value(value.clone()).map_err(|e| vec![e])
        } else {
            coord_core::workflow::validate::parse_and_validate_yaml(&text)
                .map_err(|errs| errs.iter().map(|e| e.message.clone()).collect())
        };
        let (parser_ok, errors, parsed_tasks) = match &parsed {
            Ok(def) => (true, Vec::new(), summarize_def(def)),
            Err(errs) => (false, errs.clone(), Value::Array(Vec::new())),
        };

        let mut run_kinds = Vec::new();
        collect_key_names(&value, "run", &mut run_kinds);
        run_kinds.sort();
        run_kinds.dedup();

        results.push(json!({
            "file": file,
            "yaml_ok": true,
            "route": route,
            "parser_ok": parser_ok,
            "errors": errors,
            "parsed_tasks": parsed_tasks,
            "has_extensions": has_key(&value, "extensions"),
            "run_kinds": run_kinds,
        }));
    }

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "tool": "spike-conformance",
            "route_dispatch": "coord-agent workflow.rs:2137",
            "results": results,
        }))
        .unwrap()
    );
}
