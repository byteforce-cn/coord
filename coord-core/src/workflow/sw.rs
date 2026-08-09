// coord-core/workflow/sw.rs
// CNCF Serverless Workflow 1.0 权威 DSL —— 原生解析与执行模型转换
//
// coord 作为协调器，直接消费 CNCF Serverless Workflow 官方格式
// （{id, version, specVersion, start, states[], functions[], ...}），
// 不引入自定义 DSL 变体，避免集成摩擦。
//
// 本模块把权威 SW 文档解析为内部执行模型（WorkflowDefinition + do 任务列表），
// 转换规则（状态 → 任务）：
// - inject                  → N 个 set 任务（data 逐 key）+ 转移 switch
// - operation（单个 action）  → call 任务 + 转移 switch
// - switch（dataBasedSwitch） → switch 任务（dataConditions + defaultCondition）
// - end（end: true / transition: "end" / default → end）→ __end 终端任务（Task::End）
//
// 关键设计：每个带 transition 的线性状态追加一个「无条件转移 switch」
// （{state}__transition: switch: [{transition: <target>}]），使每个状态显式 Goto，
// 从而在 coord 的线性 do 列表模型上保真表达 SW 的图结构（分支互斥、汇聚共享），
// 杜绝 fall-through。
//
// 严格校验（对齐权威规范）：
// - start 状态必须存在；
// - 状态类型仅支持 inject / operation / switch（本子集）；
// - 每个非 switch 状态必须恰好有 transition 或 end（二者互斥）；
// - transition / dataConditions[].transition / defaultCondition.transition
//   必须引用已存在状态或 "end"；
// - 状态图不允许成环（coord 执行模型不支持循环）。
//
// 使用：deployDefinition 先解析顶层结构，含 states/start 则走本模块（CNCF SW），
// 含 document/do 则走遗留 coord DSL 路径（兼容保留）。

use std::collections::{HashMap, HashSet};

use serde::Deserialize;
use serde_json::Value;

use super::model::{
    CallTask, CallType, Document, EndTask, FunctionDef, NamedTask, SetTask, SwitchCondition,
    SwitchTask, Task, UseComponents, WorkflowDefinition,
};

// ─── CNCF SW 文档模型（子集） ───

/// CNCF Serverless Workflow 文档（顶层）
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwWorkflowDoc {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub spec_version: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub start: Option<String>,
    #[serde(default)]
    pub states: Vec<SwState>,
    #[serde(default)]
    pub functions: Vec<SwFunction>,
}

/// CNCF SW 状态（子集：inject / operation / switch + end）
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwState {
    pub name: String,
    #[serde(rename = "type")]
    pub state_type: String,
    /// inject：注入的数据对象
    #[serde(default)]
    pub data: Option<Value>,
    /// operation：动作列表（子集要求恰好一个）
    #[serde(default)]
    pub actions: Vec<SwAction>,
    /// switch：dataBasedSwitch 的条件
    #[serde(default)]
    pub data_conditions: Vec<SwCondition>,
    /// switch：eventBasedSwitch 不支持（本子集）
    #[serde(default)]
    pub event_conditions: Vec<SwCondition>,
    /// switch：默认条件
    #[serde(default)]
    pub default_condition: Option<SwDefaultCondition>,
    /// 线性状态的后继（与 end 互斥）
    #[serde(default)]
    pub transition: Option<String>,
    /// 终端标记：true 或对象（子集仅判定是否终端）
    #[serde(default)]
    pub end: Option<Value>,
}

/// operation 动作
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwAction {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub function_ref: Option<SwFunctionRef>,
    #[serde(default)]
    pub arguments: Option<Value>,
}

/// functionRef
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwFunctionRef {
    #[serde(rename = "refName")]
    pub ref_name: String,
    #[serde(default)]
    pub arguments: Option<Value>,
}

/// switch 条件
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwCondition {
    /// jq 布尔表达式（${...}）
    #[serde(default)]
    pub condition: Option<String>,
    #[serde(default)]
    pub transition: Option<String>,
    #[serde(default)]
    pub end: Option<bool>,
}

/// switch 默认条件
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwDefaultCondition {
    #[serde(default)]
    pub transition: Option<String>,
    #[serde(default)]
    pub end: Option<bool>,
}

/// SW 函数定义
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwFunction {
    pub name: String,
    #[serde(default)]
    pub operation: Option<String>,
    #[serde(default)]
    pub r#type: Option<String>,
}

// ─── 入口 ───

/// 判断顶层结构是否为 CNCF SW 文档（含 states / start）
pub fn looks_like_cncf_sw(value: &Value) -> bool {
    value.get("states").is_some() || value.get("start").is_some()
}

/// 解析 CNCF SW 文档（serde_json::Value）为内部执行模型
pub fn parse_cncf_sw_value(root: Value) -> Result<WorkflowDefinition, String> {
    let doc: SwWorkflowDoc = serde_json::from_value(root)
        .map_err(|e| format!("invalid Serverless Workflow document: {e}"))?;
    convert(doc)
}

/// 解析 CNCF SW 文档（JSON 或 YAML 文本）为内部执行模型
pub fn parse_cncf_sw(input: &str) -> Result<WorkflowDefinition, String> {
    let value: Value = serde_yaml::from_str(input)
        .map_err(|e| format!("parse error: {e}"))?;
    if !looks_like_cncf_sw(&value) {
        return Err("not a Serverless Workflow document (missing 'start'/'states')".into());
    }
    parse_cncf_sw_value(value)
}

// ─── 转换 ───

/// 终端任务保留名（SW 状态不得占用）
const END_TASK: &str = "__end";

fn convert(doc: SwWorkflowDoc) -> Result<WorkflowDefinition, String> {
    // ── 1. 基础校验 ──
    let start = doc.start.as_deref().ok_or("missing required 'start' state")?;
    if doc.states.is_empty() {
        return Err("workflow must have at least one state".into());
    }

    let mut state_map: HashMap<&str, &SwState> = HashMap::new();
    for s in &doc.states {
        if s.name.is_empty() {
            return Err("state name must not be empty".into());
        }
        if s.name == END_TASK {
            return Err(format!("state name '{END_TASK}' is reserved").into());
        }
        if state_map.insert(s.name.as_str(), s).is_some() {
            return Err(format!("duplicate state name '{}'", s.name));
        }
    }
    if !state_map.contains_key(start) {
        return Err(format!("start state '{start}' not found in states"));
    }

    // ── 2. 状态类型 + 结构校验 ──
    for s in &doc.states {
        match s.state_type.as_str() {
            "inject" | "operation" | "switch" => {}
            other => {
                return Err(format!(
                    "state '{}': unsupported type '{other}' (supported: inject, operation, switch)",
                    s.name
                ))
            }
        }
        if !s.event_conditions.is_empty() {
            return Err(format!(
                "state '{}': eventConditions (eventBasedSwitch) not supported, use dataConditions",
                s.name
            ));
        }
        // end 与 transition 互斥
        if s.end.is_some() && s.transition.is_some() {
            return Err(format!("state '{}': 'end' and 'transition' are mutually exclusive", s.name));
        }
        if s.state_type != "switch" && s.end.is_none() && s.transition.is_none() {
            return Err(format!(
                "state '{}': must have 'transition' or 'end'",
                s.name
            ));
        }
        if s.state_type == "switch" && s.data_conditions.is_empty() && s.default_condition.is_none() {
            return Err(format!(
                "state '{}': switch must have dataConditions or defaultCondition",
                s.name
            ));
        }
        if s.state_type == "operation" && s.actions.len() != 1 {
            return Err(format!(
                "state '{}': operation must have exactly one action (subset)",
                s.name
            ));
        }
        if s.state_type == "operation" {
            let action = &s.actions[0];
            let fn_ref = action.function_ref.as_ref().ok_or_else(|| {
                format!("state '{}': operation action requires functionRef", s.name)
            })?;
            if fn_ref.ref_name.is_empty() {
                return Err(format!("state '{}': functionRef.refName must not be empty", s.name));
            }
        }
    }

    // ── 3. 引用完整性 + 环检测（状态图，排除 end） ──
    validate_graph(&doc, &state_map, start)?;

    // ── 4. 构建 do 任务列表（start 首位，其余按文档顺序） ──
    let mut tasks: Vec<NamedTask> = Vec::new();
    let mut needs_end = false;

    let mut ordered: Vec<&str> = vec![start];
    for s in &doc.states {
        if s.name != start {
            ordered.push(s.name.as_str());
        }
    }

    for sname in ordered {
        let s = state_map[sname];
        match s.state_type.as_str() {
            "inject" => {
                let data = s.data.clone().unwrap_or(Value::Object(Default::default()));
                let obj = data.as_object().ok_or_else(|| {
                    format!("state '{}': inject 'data' must be a JSON object", s.name)
                })?;
                let mut first = true;
                for (k, v) in obj {
                    let task_name = if first {
                        s.name.clone()
                    } else {
                        format!("{}__data_{}", s.name, k)
                    };
                    first = false;
                    tasks.push(NamedTask {
                        name: task_name,
                        task: Task::Set(SetTask {
                            variable: k.clone(),
                            value: json_to_jq_literal(v),
                        }),
                    });
                }
                let target = linear_transition_target(s)?;
                if target == END_TASK {
                    needs_end = true;
                }
                tasks.push(transition_task(&s.name, &target));
            }
            "operation" => {
                let action = &s.actions[0];
                let fn_ref = action.function_ref.as_ref().unwrap();
                let with = action
                    .arguments
                    .clone()
                    .or_else(|| fn_ref.arguments.clone());
                tasks.push(NamedTask {
                    name: s.name.clone(),
                    task: Task::Call(CallTask {
                        call: CallType::Function(fn_ref.ref_name.clone()),
                        with,
                    }),
                });
                let target = linear_transition_target(s)?;
                if target == END_TASK {
                    needs_end = true;
                }
                tasks.push(transition_task(&s.name, &target));
            }
            "switch" => {
                let mut conditions: Vec<SwitchCondition> = Vec::new();
                for c in &s.data_conditions {
                    let cond = c.condition.clone().ok_or_else(|| {
                        format!("state '{}': dataConditions entry requires 'condition'", s.name)
                    })?;
                    let target = condition_target(c)?;
                    if target == END_TASK {
                        needs_end = true;
                    }
                    conditions.push(SwitchCondition {
                        condition: Some(cond),
                        transition: target,
                    });
                }
                match &s.default_condition {
                    Some(d) => {
                        let target = default_target(d)?;
                        if target == END_TASK {
                            needs_end = true;
                        }
                        conditions.push(SwitchCondition {
                            condition: None,
                            transition: target,
                        });
                    }
                    None => {
                        // 权威语义：无条件匹配时终止
                        conditions.push(SwitchCondition {
                            condition: None,
                            transition: END_TASK.into(),
                        });
                        needs_end = true;
                    }
                }
                tasks.push(NamedTask {
                    name: s.name.clone(),
                    task: Task::Switch(SwitchTask {
                        conditions,
                        default_condition: None,
                    }),
                });
            }
            _ => unreachable!(),
        }
    }

    if needs_end {
        tasks.push(NamedTask {
            name: END_TASK.into(),
            task: Task::End(EndTask {}),
        });
    }

    // ── 5. document / use.functions ──
    let document = Document {
        dsl: "cncf-serverless-workflow".into(),
        namespace: "default".into(), // deploy 时以调用方参数覆盖
        name: doc.name.or(doc.id).unwrap_or_else(|| "workflow".into()),
        version: doc.version.unwrap_or_else(|| "1.0".into()),
        title: None,
        summary: doc.description,
        tags: None,
    };

    let use_components = if doc.functions.is_empty() {
        None
    } else {
        let mut functions: HashMap<String, FunctionDef> = HashMap::new();
        for f in &doc.functions {
            functions.insert(
                f.name.clone(),
                FunctionDef {
                    call: CallType::Http, // 子集默认 http；宿主 dispatcher 可另行解释
                    with: None,
                },
            );
        }
        Some(UseComponents {
            functions: Some(functions),
            retries: None,
            timeouts: None,
        })
    };

    Ok(WorkflowDefinition {
        id: None,
        document,
        do_tasks: tasks,
        input: None,
        output: None,
        timeout: None,
        use_components,
        raw_yaml: None,
    })
}

/// 校验状态图：transition 引用完整性 + 无环
fn validate_graph(
    doc: &SwWorkflowDoc,
    state_map: &HashMap<&str, &SwState>,
    _start: &str,
) -> Result<(), String> {
    let mut edges: HashMap<String, Vec<String>> = HashMap::new();

    for s in &doc.states {
        let mut targets: Vec<String> = Vec::new();
        if s.end.is_none() {
            if let Some(t) = s.transition.as_deref() {
                if t != "end" {
                    targets.push(t.to_string());
                }
            }
        }
        for c in &s.data_conditions {
            match condition_target(c) {
                Ok(t) if t != END_TASK => targets.push(t),
                Ok(_) => {}
                Err(e) => return Err(e),
            }
        }
        if let Some(d) = &s.default_condition {
            match default_target(d) {
                Ok(t) if t != END_TASK => targets.push(t),
                Ok(_) => {}
                Err(e) => return Err(e),
            }
        }
        for t in &targets {
            if !state_map.contains_key(t.as_str()) {
                return Err(format!(
                    "state '{}' references non-existent state '{}'",
                    s.name, t
                ));
            }
        }
        edges.insert(s.name.clone(), targets);
    }

    // DFS 环检测
    let mut visited: HashSet<String> = HashSet::new();
    let mut in_stack: HashSet<String> = HashSet::new();
    let mut path: Vec<String> = Vec::new();
    for node in edges.keys() {
        if !visited.contains(node) {
            if let Some(cycle) = detect_cycle(node, &edges, &mut visited, &mut in_stack, &mut path) {
                return Err(format!(
                    "cyclic dependency detected in states: {}",
                    cycle.join(" -> ")
                ));
            }
        }
    }
    Ok(())
}

fn detect_cycle(
    node: &str,
    edges: &HashMap<String, Vec<String>>,
    visited: &mut HashSet<String>,
    in_stack: &mut HashSet<String>,
    path: &mut Vec<String>,
) -> Option<Vec<String>> {
    if in_stack.contains(node) {
        let idx = path.iter().position(|x| x == node).unwrap_or(0);
        let mut cycle: Vec<String> = path[idx..].to_vec();
        cycle.push(node.to_string());
        return Some(cycle);
    }
    if visited.contains(node) {
        return None;
    }
    visited.insert(node.to_string());
    in_stack.insert(node.to_string());
    path.push(node.to_string());

    if let Some(neighbors) = edges.get(node) {
        for n in neighbors {
            if let Some(c) = detect_cycle(n, edges, visited, in_stack, path) {
                return Some(c);
            }
        }
    }

    path.pop();
    in_stack.remove(node);
    None
}

// ─── 辅助：目标解析 ───

/// 线性状态（inject/operation）的后继目标；end: true → __end
fn linear_transition_target(s: &SwState) -> Result<String, String> {
    if s.end.is_some() {
        return Ok(END_TASK.into());
    }
    let t = s
        .transition
        .clone()
        .ok_or_else(|| format!("state '{}': missing transition", s.name))?;
    if t == "end" {
        Ok(END_TASK.into())
    } else {
        Ok(t)
    }
}

/// switch 条件的后继目标
fn condition_target(c: &SwCondition) -> Result<String, String> {
    if c.end == Some(true) {
        return Ok(END_TASK.into());
    }
    let t = c
        .transition
        .clone()
        .ok_or("dataConditions entry must have 'transition' or 'end'")?;
    if t == "end" {
        Ok(END_TASK.into())
    } else {
        Ok(t)
    }
}

/// switch 默认条件的后继目标
fn default_target(d: &SwDefaultCondition) -> Result<String, String> {
    if d.end == Some(true) {
        return Ok(END_TASK.into());
    }
    let t = d
        .transition
        .clone()
        .ok_or("defaultCondition must have 'transition' or 'end'")?;
    if t == "end" {
        Ok(END_TASK.into())
    } else {
        Ok(t)
    }
}

/// 生成状态的无条件转移任务（{state}__transition → Goto target）
fn transition_task(state: &str, target: &str) -> NamedTask {
    NamedTask {
        name: format!("{state}__transition"),
        task: Task::Switch(SwitchTask {
            conditions: vec![SwitchCondition {
                condition: None,
                transition: target.into(),
            }],
            default_condition: None,
        }),
    }
}

/// JSON 值 → jq 字面量表达式字符串
fn json_to_jq_literal(v: &Value) -> String {
    match v {
        Value::Null => "null".into(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => serde_json::to_string(s).unwrap_or_else(|_| "\"\"".into()),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(v).unwrap_or_else(|_| "{}".into()),
    }
}

// ─── 测试 ───

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::model::InstanceStatus;

    // ── 样例 ──

    /// ISSUE-004 conformance 示例（SW 文档）
    fn sample_sw_json() -> &'static str {
        r#"{
          "id": "order-approval",
          "version": "1.0",
          "start": "init",
          "functions": [
            { "name": "approveOrder", "operation": "http://icps/approve" },
            { "name": "sendNotify", "operation": "http://icps/notify" }
          ],
          "states": [
            { "name": "init", "type": "inject",
              "data": { "approved": false, "level": 1 },
              "transition": "check" },
            { "name": "check", "type": "switch",
              "dataConditions": [
                { "condition": "${ .amount >= 1000 }", "transition": "senior-approve" },
                { "condition": "${ .amount < 1000 }", "transition": "notify" }
              ],
              "defaultCondition": { "transition": "end" } },
            { "name": "senior-approve", "type": "operation",
              "actions": [ { "name": "approve",
                             "functionRef": { "refName": "approveOrder" },
                             "arguments": { "orderId": "${ .orderId }" } } ],
              "transition": "notify" },
            { "name": "notify", "type": "operation",
              "actions": [ { "name": "notify",
                             "functionRef": { "refName": "sendNotify" },
                             "arguments": { "to": "${ .owner }" } } ],
              "end": true }
          ]
        }"#
    }

    fn parse_sample() -> WorkflowDefinition {
        let value: Value = serde_json::from_str(sample_sw_json()).unwrap();
        parse_cncf_sw_value(value).expect("sample SW doc should parse")
    }

    // ── 结构转换 ──

    #[test]
    fn test_looks_like_cncf_sw() {
        let sw: Value = serde_json::from_str(sample_sw_json()).unwrap();
        assert!(looks_like_cncf_sw(&sw));
        let coord: Value = serde_yaml::from_str(
            "document:\n  dsl: 1.0.0\ndo:\n  - a:\n      call: http\n",
        )
        .unwrap();
        assert!(!looks_like_cncf_sw(&coord));
    }

    #[test]
    fn test_parse_sw_document_metadata() {
        let def = parse_sample();
        assert_eq!(def.document.dsl, "cncf-serverless-workflow");
        assert_eq!(def.document.name, "order-approval");
        assert_eq!(def.document.version, "1.0");
        // use.functions 从 SW functions 转换
        assert!(def.use_components.is_some());
        let funcs = def.use_components.as_ref().unwrap().functions.as_ref().unwrap();
        assert_eq!(funcs.len(), 2);
        assert!(funcs.contains_key("approveOrder"));
        assert!(funcs.contains_key("sendNotify"));
    }

    #[test]
    fn test_parse_sw_inject_expands_to_set_tasks() {
        let def = parse_sample();
        // init(inject, 2 keys) → 2 set 任务 + 1 转移任务
        let names: Vec<&str> = def.do_tasks.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names[0], "init");
        assert!(matches!(def.do_tasks[0].task, Task::Set(_)));
        assert!(matches!(def.do_tasks[1].task, Task::Set(_)));
        assert!(matches!(def.do_tasks[2].task, Task::Switch(_)));
        // init 转移目标 = check（在后续任务中）
        let init_transition = match &def.do_tasks[2].task {
            Task::Switch(s) => &s.conditions[0].transition,
            _ => panic!("expected switch"),
        };
        assert_eq!(init_transition, "check");
    }

    #[test]
    fn test_parse_sw_operation_maps_to_call() {
        let def = parse_sample();
        let call = def.do_tasks.iter().find(|t| t.name == "senior-approve").unwrap();
        match &call.task {
            Task::Call(c) => {
                assert!(matches!(&c.call, CallType::Function(f) if f == "approveOrder"));
                assert!(c.with.is_some());
            }
            other => panic!("expected call task, got {other:?}"),
        }
        // senior-approve 转移 → notify
        let tr = def.do_tasks.iter().find(|t| t.name == "senior-approve__transition").unwrap();
        match &tr.task {
            Task::Switch(s) => assert_eq!(s.conditions[0].transition, "notify"),
            other => panic!("expected transition switch, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_sw_switch_conditions_and_default() {
        let def = parse_sample();
        let sw = def.do_tasks.iter().find(|t| t.name == "check").unwrap();
        match &sw.task {
            Task::Switch(s) => {
                assert_eq!(s.conditions.len(), 3);
                assert_eq!(s.conditions[0].condition.as_deref(), Some("${ .amount >= 1000 }"));
                assert_eq!(s.conditions[0].transition, "senior-approve");
                assert_eq!(s.conditions[1].transition, "notify");
                // defaultCondition 放最后，无条件
                assert_eq!(s.conditions[2].condition, None);
                assert_eq!(s.conditions[2].transition, "__end");
            }
            other => panic!("expected switch task, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_sw_emits_terminal_end_task() {
        let def = parse_sample();
        let last = def.do_tasks.last().unwrap();
        assert_eq!(last.name, "__end");
        assert!(matches!(last.task, Task::End(_)));
    }

    // ── 严格校验 ──

    #[test]
    fn test_rejects_unsupported_state_type() {
        let v: Value = serde_json::json!({
            "id": "x", "version": "1.0", "start": "s",
            "states": [ { "name": "s", "type": "delay", "transition": "end" } ]
        });
        let err = parse_cncf_sw_value(v).unwrap_err();
        assert!(err.contains("unsupported type 'delay'"), "err: {err}");
    }

    #[test]
    fn test_rejects_missing_start() {
        let v: Value = serde_json::json!({
            "id": "x", "version": "1.0",
            "states": [ { "name": "s", "type": "inject", "end": true } ]
        });
        assert!(parse_cncf_sw_value(v).is_err());
    }

    #[test]
    fn test_rejects_start_not_found() {
        let v: Value = serde_json::json!({
            "id": "x", "version": "1.0", "start": "nope",
            "states": [ { "name": "s", "type": "inject", "end": true } ]
        });
        let err = parse_cncf_sw_value(v).unwrap_err();
        assert!(err.contains("start state 'nope' not found"), "err: {err}");
    }

    #[test]
    fn test_rejects_state_without_transition_or_end() {
        let v: Value = serde_json::json!({
            "id": "x", "version": "1.0", "start": "s",
            "states": [ { "name": "s", "type": "operation",
                          "actions": [ { "name": "a", "functionRef": { "refName": "f" } } ] } ]
        });
        let err = parse_cncf_sw_value(v).unwrap_err();
        assert!(err.contains("must have 'transition' or 'end'"), "err: {err}");
    }

    #[test]
    fn test_rejects_end_and_transition_together() {
        let v: Value = serde_json::json!({
            "id": "x", "version": "1.0", "start": "s",
            "states": [ { "name": "s", "type": "inject", "data": { "a": 1 },
                          "transition": "end", "end": true } ]
        });
        let err = parse_cncf_sw_value(v).unwrap_err();
        assert!(err.contains("mutually exclusive"), "err: {err}");
    }

    #[test]
    fn test_rejects_broken_transition_reference() {
        let v: Value = serde_json::json!({
            "id": "x", "version": "1.0", "start": "s",
            "states": [ { "name": "s", "type": "inject", "data": { "a": 1 },
                          "transition": "missing" } ]
        });
        let err = parse_cncf_sw_value(v).unwrap_err();
        assert!(err.contains("non-existent state 'missing'"), "err: {err}");
    }

    #[test]
    fn test_rejects_cyclic_dependency() {
        let v: Value = serde_json::json!({
            "id": "x", "version": "1.0", "start": "a",
            "states": [
                { "name": "a", "type": "inject", "data": { "x": 1 }, "transition": "b" },
                { "name": "b", "type": "inject", "data": { "y": 2 }, "transition": "a" }
            ]
        });
        let err = parse_cncf_sw_value(v).unwrap_err();
        assert!(err.contains("cyclic dependency"), "err: {err}");
    }

    #[test]
    fn test_rejects_event_conditions() {
        let v: Value = serde_json::json!({
            "id": "x", "version": "1.0", "start": "s",
            "states": [ { "name": "s", "type": "switch",
                          "eventConditions": [ { "eventRef": { "triggerEventRef": "e" } } ],
                          "defaultCondition": { "transition": "end" } } ]
        });
        let err = parse_cncf_sw_value(v).unwrap_err();
        assert!(err.contains("eventConditions"), "err: {err}");
    }

    #[test]
    fn test_rejects_operation_without_function_ref() {
        let v: Value = serde_json::json!({
            "id": "x", "version": "1.0", "start": "s",
            "states": [ { "name": "s", "type": "operation",
                          "actions": [ { "name": "a" } ], "transition": "end" } ]
        });
        let err = parse_cncf_sw_value(v).unwrap_err();
        assert!(err.contains("requires functionRef"), "err: {err}");
    }

    #[test]
    fn test_accepts_sw_without_functions() {
        let v: Value = serde_json::json!({
            "id": "x", "version": "1.0", "start": "s",
            "states": [ { "name": "s", "type": "operation",
                          "actions": [ { "name": "a", "functionRef": { "refName": "f" } } ],
                          "transition": "end" } ]
        });
        let def = parse_cncf_sw_value(v).expect("should parse");
        assert!(def.use_components.is_none());
    }

    // ── 分支互斥语义（验证转换结构，执行验证在 agent 层 conformance） ──

    #[test]
    fn test_branch_states_each_have_own_end_path() {
        // check → [approve → end | reject → end]
        let v: Value = serde_json::json!({
            "id": "wf", "version": "1.0", "start": "check",
            "states": [
                { "name": "check", "type": "switch",
                  "dataConditions": [
                    { "condition": "${ .ok }", "transition": "approve" }
                  ],
                  "defaultCondition": { "transition": "reject" } },
                { "name": "approve", "type": "operation",
                  "actions": [ { "name": "a", "functionRef": { "refName": "approve" } } ],
                  "transition": "end" },
                { "name": "reject", "type": "operation",
                  "actions": [ { "name": "r", "functionRef": { "refName": "reject" } } ],
                  "transition": "end" }
            ]
        });
        let def = parse_cncf_sw_value(v).expect("should parse");
        // 两条分支都必须以 end 终止（approve__transition/reject__transition → __end）
        let approve_tr = def.do_tasks.iter()
            .find(|t| t.name == "approve__transition").unwrap();
        match &approve_tr.task {
            Task::Switch(s) => assert_eq!(s.conditions[0].transition, "__end"),
            other => panic!("expected switch, got {other:?}"),
        }
        assert!(def.do_tasks.iter().any(|t| t.name == "__end"));
        assert!(def.do_tasks.iter().any(|t| t.name == "approve"));
        assert!(def.do_tasks.iter().any(|t| t.name == "reject"));
    }

    #[test]
    fn test_shared_convergence_parses() {
        // check → [a | b]，a→c，b→c，c→end（汇聚共享）
        let v: Value = serde_json::json!({
            "id": "wf", "version": "1.0", "start": "check",
            "states": [
                { "name": "check", "type": "switch",
                  "dataConditions": [
                    { "condition": "${ .x }", "transition": "a" }
                  ],
                  "defaultCondition": { "transition": "b" } },
                { "name": "a", "type": "inject", "data": { "from": "a" }, "transition": "c" },
                { "name": "b", "type": "inject", "data": { "from": "b" }, "transition": "c" },
                { "name": "c", "type": "inject", "data": { "done": true }, "transition": "end" }
            ]
        });
        let def = parse_cncf_sw_value(v).expect("should parse");
        let c_tr = def.do_tasks.iter().find(|t| t.name == "c__transition").unwrap();
        match &c_tr.task {
            Task::Switch(s) => assert_eq!(s.conditions[0].transition, "__end"),
            other => panic!("expected switch, got {other:?}"),
        }
    }

    #[test]
    fn test_json_to_jq_literal() {
        assert_eq!(json_to_jq_literal(&Value::Bool(true)), "true");
        assert_eq!(json_to_jq_literal(&Value::Number(42.into())), "42");
        assert_eq!(json_to_jq_literal(&Value::String("hi".into())), "\"hi\"");
        assert_eq!(json_to_jq_literal(&serde_json::json!({"a": 1})), "{\"a\":1}");
    }

    // 执行模型测试（验证转换结果可被执行器消费）：
    // 直接构造 def 跑 execute_step，验证线性链 + 转移 switch 结构
    #[test]
    fn test_converted_linear_def_executes_in_order() {
        use crate::workflow::engine::WorkflowExecutor;
        use crate::workflow::expression::ExpressionEvaluator;
        use crate::workflow::ports::test_utils::TestClock;

        let def = parse_sample();
        let executor = WorkflowExecutor::new(
            ExpressionEvaluator::new(),
            TestClock::new(1000),
        );
        let mut inst = crate::workflow::model::WorkflowInstance {
            id: "i1".into(),
            definition_ns: def.document.namespace.clone(),
            definition_name: def.document.name.clone(),
            definition_version: def.document.version.clone(),
            status: InstanceStatus::Running,
            context: serde_json::json!({"orderId": "O1", "owner": "u1", "amount": 1500}),
            task_stack: vec![],
            current_task_index: 0,
            created_at: 0,
            updated_at: 0,
            output: None,
            fault: None,
            suspension_meta: None,
        };

        // 线性推进（不实际派发 call）：走到 check 前（init 的 set/转移）
        let mut steps = 0;
        loop {
            let step = executor.execute_step(&inst, &def);
            match step {
                crate::workflow::model::StepResult::NextTask(_) => {
                    inst.current_task_index += 1;
                }
                crate::workflow::model::StepResult::Goto { target, .. } => {
                    inst.current_task_index = def.do_tasks
                        .iter().position(|t| t.name == target)
                        .expect("goto target exists");
                }
                crate::workflow::model::StepResult::SetVariable { variable, value, .. } => {
                    inst.context.as_object_mut().unwrap().insert(variable, value);
                    inst.current_task_index += 1;
                }
                crate::workflow::model::StepResult::Completed { .. } => break,
                _ => break,
            }
            steps += 1;
            assert!(steps < 100, "should not loop");
        }

        // init 注入生效：approved=false, level=1 进入 context
        assert_eq!(inst.context["approved"], serde_json::json!(false));
        assert_eq!(inst.context["level"].as_f64(), Some(1.0));
    }
}
