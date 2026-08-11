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
    AuthConfig, CallTask, CallType, CatchClause, Document, EndTask, EventFilter, ForEachTask,
    ForkBranch, ForkTask, FunctionDef, ListenTask, NamedTask, RetryPolicy, SetTask,
    SwitchCondition, SwitchTask, Task, TimeoutConfig, TryCatchTask, UseComponents, WaitTask,
    WorkflowDefinition,
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
    /// 顶层事件定义（events[]）
    #[serde(default)]
    pub events: Vec<SwEvent>,
    /// 顶层重试定义（retries[]）
    #[serde(default)]
    pub retries: Vec<SwRetry>,
    /// 顶层错误定义（errors[]）
    #[serde(default)]
    pub errors: Vec<SwError>,
    /// 顶层超时定义（timeouts[]）
    #[serde(default)]
    pub timeouts: Vec<SwTimeout>,
    /// 顶层认证定义（auth[]）
    #[serde(default)]
    pub auth: Vec<SwAuth>,
}

/// CNCF SW 状态（全状态：inject / operation / delay / event / callback /
/// switch / foreach / parallel / compensate + end）
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwState {
    pub name: String,
    #[serde(rename = "type")]
    pub state_type: String,
    /// inject：注入的数据对象
    #[serde(default)]
    pub data: Option<Value>,
    /// operation：动作列表
    #[serde(default)]
    pub actions: Vec<SwAction>,
    /// operation：actionMode（sequential/parallel）
    #[serde(default)]
    pub action_mode: Option<String>,
    /// switch：dataBasedSwitch 的条件
    #[serde(default)]
    pub data_conditions: Vec<SwCondition>,
    /// switch：eventBasedSwitch 的条件
    #[serde(default)]
    pub event_conditions: Vec<SwCondition>,
    /// switch：默认条件
    #[serde(default)]
    pub default_condition: Option<SwDefaultCondition>,
    /// delay：等待时长（ISO 8601）
    #[serde(default)]
    pub duration: Option<String>,
    /// event/callback：事件定义
    #[serde(default)]
    pub on_events: Vec<SwEvent>,
    /// callback：状态级动作
    #[serde(default)]
    pub action: Option<SwEventAction>,
    /// callback：状态级返回事件引用
    #[serde(rename = "eventRef", default)]
    pub state_event_ref: Option<SwEventRef>,
    /// foreach：迭代定义
    #[serde(default)]
    pub iterate: Option<SwIterate>,
    /// parallel：并行分支
    #[serde(default)]
    pub branches: Vec<SwBranch>,
    /// parallel：完成类型（allOf/atLeastOne）
    #[serde(default)]
    pub completion_type: Option<String>,
    /// 错误处理（onErrors[]：retry/compensate/transition）
    #[serde(default)]
    pub on_errors: Vec<SwOnError>,
    /// 补偿状态引用（compensatedBy）
    #[serde(default)]
    pub compensated_by: Option<String>,
    /// 是否仅用于补偿（usedForCompensation）
    #[serde(default)]
    pub used_for_compensation: Option<bool>,
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
    /// eventConditions：事件引用（事件名或顶层 events 定义名）
    #[serde(rename = "eventRef", default)]
    pub event_ref: Option<String>,
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

/// 事件定义（顶层 events[] 与状态内 onEvents[] 共用）
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwEvent {
    /// 事件名
    #[serde(default)]
    pub name: Option<String>,
    /// event 状态：事件引用
    #[serde(default)]
    pub event_refs: Vec<SwEventRef>,
    /// callback 状态：动作
    #[serde(default)]
    pub action: Option<SwEventAction>,
    /// 事件后的后继
    #[serde(default)]
    pub transition: Option<String>,
    #[serde(default)]
    pub end: Option<bool>,
    /// 顶层事件定义字段
    #[serde(rename = "type")]
    pub r#type: Option<String>,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub correlation: Vec<SwCorrelation>,
    #[serde(default)]
    pub data: Option<Value>,
    #[serde(default)]
    pub context_attributes: Option<Value>,
}

/// 事件引用
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwEventRef {
    #[serde(rename = "eventRef", default)]
    pub event_ref: String,
    /// callback 返回事件引用（triggerEventRef）
    #[serde(rename = "triggerEventRef", default)]
    pub trigger_event_ref: Option<String>,
    #[serde(default)]
    pub data: Option<Value>,
    #[serde(default)]
    pub context_attributes: Option<Value>,
}

/// callback 动作（调用函数后等待事件）
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwEventAction {
    #[serde(default)]
    pub function_ref: Option<SwFunctionRef>,
    #[serde(default)]
    pub event_ref: Option<SwEventRef>,
}

/// foreach 迭代
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwIterate {
    #[serde(default)]
    pub input: Option<String>,
    #[serde(default)]
    pub iteration: Option<String>,
    #[serde(default)]
    pub actions: Vec<SwAction>,
    #[serde(default)]
    pub output: Option<String>,
}

/// parallel 分支
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwBranch {
    pub name: String,
    #[serde(default)]
    pub actions: Vec<SwAction>,
}

/// onErrors 错误处理
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwOnError {
    /// 错误引用（errors[] 名称）或标准错误类型
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub transition: Option<String>,
    #[serde(default)]
    pub end: Option<bool>,
    /// 重试（内联或引用顶层 retries）
    #[serde(default)]
    pub retry: Option<Value>,
}

/// 顶层重试定义
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwRetry {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(rename = "delay")]
    pub delay: Option<Value>,
    #[serde(default)]
    pub backoff: Option<Value>,
    #[serde(default)]
    pub limit: Option<Value>,
    #[serde(default)]
    pub jitter: Option<Value>,
}

/// 顶层错误定义
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwError {
    pub name: String,
    #[serde(rename = "type")]
    pub error_type: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub status: Option<u16>,
    #[serde(default)]
    pub detail: Option<String>,
}

/// 顶层超时定义
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwTimeout {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub after: Option<String>,
}

/// 顶层认证定义
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwAuth {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub scheme: Option<String>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub token: Option<String>,
}

/// 事件关联（correlation）
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwCorrelation {
    #[serde(rename = "contextAttributeName")]
    pub context_attribute_name: String,
    #[serde(rename = "contextAttributeValue")]
    pub context_attribute_value: Option<String>,
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
            "inject" | "operation" | "delay" | "event" | "callback" | "switch"
            | "foreach" | "parallel" | "compensate" => {}
            other => {
                return Err(format!(
                    "state '{}': unsupported type '{other}' (supported: inject, operation, delay, event, callback, switch, foreach, parallel, compensate)",
                    s.name
                ))
            }
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
        if s.state_type == "switch"
            && s.data_conditions.is_empty()
            && s.event_conditions.is_empty()
            && s.default_condition.is_none()
        {
            return Err(format!(
                "state '{}': switch must have dataConditions, eventConditions or defaultCondition",
                s.name
            ));
        }
        if s.state_type == "operation" && s.actions.is_empty() {
            return Err(format!(
                "state '{}': operation must have at least one action",
                s.name
            ));
        }
        if s.state_type == "operation" {
            for action in &s.actions {
                let fn_ref = action.function_ref.as_ref().ok_or_else(|| {
                    format!("state '{}': operation action requires functionRef", s.name)
                })?;
                if fn_ref.ref_name.is_empty() {
                    return Err(format!("state '{}': functionRef.refName must not be empty", s.name));
                }
            }
        }
        if s.state_type == "delay" && s.duration.is_none() {
            return Err(format!("state '{}': delay requires 'duration'", s.name));
        }
        if (s.state_type == "event") && s.on_events.is_empty() {
            return Err(format!("state '{}': event requires 'onEvents'", s.name));
        }
        if s.state_type == "callback"
            && s.on_events.is_empty()
            && s.action.is_none()
        {
            return Err(format!(
                "state '{}': callback requires 'onEvents' or 'action'",
                s.name
            ));
        }
        if s.state_type == "foreach" && s.iterate.is_none() {
            return Err(format!("state '{}': foreach requires 'iterate'", s.name));
        }
        if s.state_type == "parallel" && s.branches.is_empty() {
            return Err(format!("state '{}': parallel requires 'branches'", s.name));
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
                // 多动作支持（actionMode: sequential 默认 / parallel）
                let action_tasks: Vec<NamedTask> = s
                    .actions
                    .iter()
                    .enumerate()
                    .map(|(i, action)| {
                        let fn_ref = action.function_ref.as_ref().unwrap();
                        let with = action
                            .arguments
                            .clone()
                            .or_else(|| fn_ref.arguments.clone());
                        let name = if s.actions.len() == 1 {
                            s.name.clone()
                        } else {
                            format!("{}__action_{}", s.name, i)
                        };
                        NamedTask {
                            name,
                            task: Task::Call(CallTask {
                                call: CallType::Function(fn_ref.ref_name.clone()),
                                with,
                            }),
                        }
                    })
                    .collect();

                if s.action_mode.as_deref() == Some("parallel") && action_tasks.len() > 1 {
                    // 并行动作 → fork
                    tasks.push(NamedTask {
                        name: s.name.clone(),
                        task: Task::Fork(ForkTask {
                            branches: action_tasks
                                .iter()
                                .enumerate()
                                .map(|(i, t)| ForkBranch {
                                    name: format!("branch_{i}"),
                                    tasks: vec![t.clone()],
                                })
                                .collect(),
                            compete: None,
                        }),
                    });
                } else {
                    for t in action_tasks {
                        tasks.push(t);
                    }
                }
                let target = linear_transition_target(s)?;
                if target == END_TASK {
                    needs_end = true;
                }
                tasks.push(transition_task(&s.name, &target));
            }
            "delay" => {
                let duration = s.duration.clone().unwrap_or_default();
                tasks.push(NamedTask {
                    name: s.name.clone(),
                    task: Task::Wait(WaitTask { wait: duration }),
                });
                let target = linear_transition_target(s)?;
                if target == END_TASK {
                    needs_end = true;
                }
                tasks.push(transition_task(&s.name, &target));
            }
            "event" => {
                // onEvents → listen 任务（等待事件，主动订阅恢复）
                tasks.push(NamedTask {
                    name: s.name.clone(),
                    task: Task::Listen(ListenTask {
                        listen: EventFilter {
                            event_type: first_event_type(&doc, s),
                            source: None,
                            subject: None,
                        },
                    }),
                });
                let target = linear_transition_target(s)?;
                if target == END_TASK {
                    needs_end = true;
                }
                tasks.push(transition_task(&s.name, &target));
            }
            "callback" => {
                // callback = call（action.functionRef）+ listen（等待返回事件）
                let action = s
                    .action
                    .as_ref()
                    .or_else(|| s.on_events.first().and_then(|e| e.action.as_ref()));
                if let Some(action) = action {
                    if let Some(fn_ref) = &action.function_ref {
                        let with = fn_ref.arguments.clone();
                        tasks.push(NamedTask {
                            name: format!("{}__call", s.name),
                            task: Task::Call(CallTask {
                                call: CallType::Function(fn_ref.ref_name.clone()),
                                with,
                            }),
                        });
                    }
                }
                tasks.push(NamedTask {
                    name: s.name.clone(),
                    task: Task::Listen(ListenTask {
                        listen: EventFilter {
                            event_type: first_event_type(&doc, s)
                                .or_else(|| {
                                    s.state_event_ref
                                        .as_ref()
                                        .and_then(|r| {
                                            doc.events
                                                .iter()
                                                .find(|e| {
                                                    e.name.as_deref()
                                                        == Some(r.event_ref.as_str())
                                                })
                                                .and_then(|e| e.r#type.clone())
                                        })
                                }),
                            source: None,
                            subject: None,
                        },
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
                // eventConditions（eventBasedSwitch）：先 listen 事件，再按事件路由
                if !s.event_conditions.is_empty() {
                    tasks.push(NamedTask {
                        name: format!("{}__listen", s.name),
                        task: Task::Listen(ListenTask {
                            listen: EventFilter {
                                event_type: event_condition_type(&doc, &s.event_conditions),
                                source: None,
                                subject: None,
                            },
                        }),
                    });
                    for c in &s.event_conditions {
                        let target = condition_target(c)?;
                        if target == END_TASK {
                            needs_end = true;
                        }
                        let ev_type = event_condition_type(&doc, std::slice::from_ref(c));
                        conditions.push(SwitchCondition {
                            condition: Some(format!(
                                "${{ _event.eventType == \"{}\" }}",
                                ev_type.unwrap_or_default()
                            )),
                            transition: target,
                        });
                    }
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
            "foreach" => {
                let iterate = s.iterate.as_ref().unwrap();
                let sub_tasks: Vec<NamedTask> = iterate
                    .actions
                    .iter()
                    .enumerate()
                    .map(|(i, a)| {
                        let fn_ref = a.function_ref.as_ref().unwrap();
                        NamedTask {
                            name: format!("{}__item_{}", s.name, i),
                            task: Task::Call(CallTask {
                                call: CallType::Function(fn_ref.ref_name.clone()),
                                with: a
                                    .arguments
                                    .clone()
                                    .or_else(|| fn_ref.arguments.clone()),
                            }),
                        }
                    })
                    .collect();
                tasks.push(NamedTask {
                    name: s.name.clone(),
                    task: Task::ForEach(ForEachTask {
                        input: iterate.input.clone().unwrap_or_else(|| ".".into()),
                        iteration: iterate.iteration.clone().unwrap_or_else(|| "item".into()),
                        tasks: sub_tasks,
                    }),
                });
                let target = linear_transition_target(s)?;
                if target == END_TASK {
                    needs_end = true;
                }
                tasks.push(transition_task(&s.name, &target));
            }
            "parallel" => {
                let branches: Vec<ForkBranch> = s
                    .branches
                    .iter()
                    .map(|b| ForkBranch {
                        name: b.name.clone(),
                        tasks: b
                            .actions
                            .iter()
                            .map(|a| {
                                let fn_ref = a.function_ref.as_ref().unwrap();
                                NamedTask {
                                    name: format!("{}__{}", b.name, a.name.clone().unwrap_or_default()),
                                    task: Task::Call(CallTask {
                                        call: CallType::Function(fn_ref.ref_name.clone()),
                                        with: a
                                            .arguments
                                            .clone()
                                            .or_else(|| fn_ref.arguments.clone()),
                                    }),
                                }
                            })
                            .collect(),
                    })
                    .collect();
                tasks.push(NamedTask {
                    name: s.name.clone(),
                    task: Task::Fork(ForkTask {
                        branches,
                        compete: Some(s.completion_type.as_deref() == Some("atLeastOne")),
                    }),
                });
                let target = linear_transition_target(s)?;
                if target == END_TASK {
                    needs_end = true;
                }
                tasks.push(transition_task(&s.name, &target));
            }
            "compensate" => {
                // 补偿状态：编译为普通操作动作（由 compensatedBy 引用）
                for (i, a) in s.actions.iter().enumerate() {
                    let fn_ref = a.function_ref.as_ref().unwrap();
                    let name = if s.actions.len() == 1 {
                        s.name.clone()
                    } else {
                        format!("{}__comp_{}", s.name, i)
                    };
                    tasks.push(NamedTask {
                        name,
                        task: Task::Call(CallTask {
                            call: CallType::Function(fn_ref.ref_name.clone()),
                            with: a
                                .arguments
                                .clone()
                                .or_else(|| fn_ref.arguments.clone()),
                        }),
                    });
                }
                let target = linear_transition_target(s)?;
                if target == END_TASK {
                    needs_end = true;
                }
                tasks.push(transition_task(&s.name, &target));
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

    // ── 4b. onErrors / compensatedBy 错误处理包装（标准 §Error Handling / Compensation） ──
    for s in &doc.states {
        if s.on_errors.is_empty() && s.compensated_by.is_none() {
            continue;
        }
        // 定位本状态的任务区间（name == state.name 或 "{state}__" 前缀，排除 __transition）
        let prefix = format!("{}__", s.name);
        let transition_name = format!("{}__transition", s.name);
        let mut range: Option<(usize, usize)> = None;
        let mut running: Option<usize> = None;
        for (i, t) in tasks.iter().enumerate() {
            if t.name == s.name || t.name.starts_with(&prefix) {
                if t.name == transition_name {
                    break;
                }
                if running.is_none() {
                    running = Some(i);
                }
                range = Some((running.unwrap(), i + 1));
            }
        }
        let Some((st, en)) = range else { continue };
        if en <= st {
            continue;
        }

        let body_tasks: Vec<NamedTask> = tasks[st..en].to_vec();
        let mut catch_clauses: Vec<CatchClause> = Vec::new();

        // compensatedBy → catch-all 转场到补偿状态（补偿状态应为单动作 operation）
        if let Some(comp_state) = &s.compensated_by {
            catch_clauses.push(CatchClause {
                errors: None,
                tasks: vec![transition_task(&s.name, comp_state)],
            });
        }

        // onErrors → 按错误类型转场（retry 由任务级 retry 接线承载）
        for oe in &s.on_errors {
            let errors = oe.error.as_ref().map(|e| vec![e.clone()]);
            let target = if oe.end == Some(true) {
                END_TASK.to_string()
            } else {
                oe.transition.clone().unwrap_or_else(|| END_TASK.to_string())
            };
            if target == END_TASK {
                needs_end = true;
            }
            catch_clauses.push(CatchClause {
                errors,
                tasks: vec![transition_task(&s.name, &target)],
            });
        }

        let wrapper = NamedTask {
            name: format!("{}__try", s.name),
            task: Task::TryCatch(TryCatchTask {
                r#try: body_tasks,
                catch: catch_clauses,
            }),
        };
        tasks.splice(st..en, std::iter::once(wrapper));
    }

    if needs_end
        && !tasks
            .iter()
            .any(|t| t.name == END_TASK)
    {
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

    let use_components = if doc.functions.is_empty() && doc.retries.is_empty() && doc.timeouts.is_empty()
    {
        None
    } else {
        let mut functions: HashMap<String, FunctionDef> = HashMap::new();
        for f in &doc.functions {
            functions.insert(
                f.name.clone(),
                FunctionDef {
                    call: CallType::Http, // 宿主 dispatcher 按 operation URI 解释
                    with: None,
                },
            );
        }
        let mut retries: HashMap<String, RetryPolicy> = HashMap::new();
        for r in &doc.retries {
            if let Some(name) = &r.name {
                retries.insert(
                    name.clone(),
                    super::validate::parse_retry_policy_value(&serde_json::json!({
                        "delay": r.delay.clone().unwrap_or_else(|| serde_json::json!("PT3S")),
                        "backoff": r.backoff.clone().unwrap_or_else(|| serde_json::json!("constant")),
                        "limit": r.limit.clone().unwrap_or_else(|| serde_json::json!(3)),
                        "jitter": r.jitter.clone().unwrap_or_else(|| serde_json::json!({"factor": 0.0})),
                    }))
                    .unwrap_or(RetryPolicy {
                        delay: "PT3S".to_string(),
                        backoff: None,
                        limit: 3,
                        jitter: None,
                    }),
                );
            }
        }
        let mut timeouts: HashMap<String, TimeoutConfig> = HashMap::new();
        for t in &doc.timeouts {
            if let Some(name) = &t.name {
                if let Some(after) = &t.after {
                    timeouts.insert(
                        name.clone(),
                        TimeoutConfig { after: after.clone() },
                    );
                }
            }
        }
        Some(UseComponents {
            functions: if functions.is_empty() { None } else { Some(functions) },
            retries: if retries.is_empty() { None } else { Some(retries) },
            timeouts: if timeouts.is_empty() { None } else { Some(timeouts) },
        })
    };

    // 顶层 auth（标准 §Authentication）→ 认证映射
    let mut auth_map: std::collections::HashMap<String, AuthConfig> = Default::default();
    for a in &doc.auth {
        if let Some(name) = &a.name {
            auth_map.insert(
                name.clone(),
                AuthConfig {
                    scheme: a.scheme.clone(),
                    username: a.username.clone(),
                    password: a.password.clone(),
                    token: a.token.clone(),
                    token_url: None,
                    client_id: None,
                    client_secret: None,
                    scope: None,
                },
            );
        }
    }

    Ok(WorkflowDefinition {
        id: None,
        document,
        do_tasks: tasks,
        input: None,
        output: None,
        timeout: doc
            .timeouts
            .iter()
            .find(|t| t.name.as_deref() == Some("default"))
            .and_then(|t| t.after.clone())
            .map(|after| TimeoutConfig { after }),
        use_components,
        schedule: Default::default(),
        auth: auth_map,
        secrets: Default::default(),
        constants: Default::default(),
        task_meta: Default::default(),
        raw_yaml: None,
    })
}

/// 取 onEvents 中首个事件的类型（解析顶层 events 定义）
fn first_event_type(doc: &SwWorkflowDoc, s: &SwState) -> Option<String> {
    for ev in &s.on_events {
        if let Some(t) = &ev.r#type {
            return Some(t.clone());
        }
        if let Some(name) = &ev.name {
            // 查找顶层事件定义
            if let Some(def) = doc.events.iter().find(|e| e.name.as_deref() == Some(name)) {
                if let Some(t) = &def.r#type {
                    return Some(t.clone());
                }
            }
        }
        for ev_ref in &ev.event_refs {
            if let Some(def) = doc
                .events
                .iter()
                .find(|e| e.name.as_deref() == Some(ev_ref.event_ref.as_str()))
            {
                if let Some(t) = &def.r#type {
                    return Some(t.clone());
                }
            }
        }
    }
    None
}

/// 取 eventConditions 的监听事件类型（解析 eventRef → 顶层 events 定义 type）
fn event_condition_type(doc: &SwWorkflowDoc, conditions: &[SwCondition]) -> Option<String> {
    for c in conditions {
        if let Some(ev_ref) = &c.event_ref {
            // 直接是事件类型，或顶层 events 定义名
            if ev_ref.contains('.') || ev_ref.contains("created") {
                return Some(ev_ref.clone());
            }
            if let Some(def) = doc.events.iter().find(|e| e.name.as_deref() == Some(ev_ref.as_str())) {
                if let Some(t) = &def.r#type {
                    return Some(t.clone());
                }
            }
        }
    }
    if let Some(ev) = doc.events.first() {
        if let Some(t) = &ev.r#type {
            return Some(t.clone());
        }
    }
    None
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
        // eventBasedSwitch（eventConditions）的目标
        for c in &s.event_conditions {
            match condition_target(c) {
                Ok(t) if t != END_TASK => targets.push(t),
                Ok(_) => {}
                Err(e) => return Err(e),
            }
        }
        // event/callback 状态 onEvents 的后继
        for ev in &s.on_events {
            if let Some(t) = ev.transition.as_deref() {
                if t != "end" {
                    targets.push(t.to_string());
                }
            }
        }
        // onErrors 的后继（transition/end）
        for oe in &s.on_errors {
            if let Some(t) = oe.transition.as_deref() {
                if t != "end" {
                    targets.push(t.to_string());
                }
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
            "states": [ { "name": "s", "type": "unknown-type", "transition": "end" } ]
        });
        let err = parse_cncf_sw_value(v).unwrap_err();
        assert!(err.contains("unsupported type 'unknown-type'"), "err: {err}");
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
    fn test_accepts_event_conditions() {
        // eventConditions（eventBasedSwitch）现在支持：listen + 按事件路由
        let v: Value = serde_json::json!({
            "id": "x", "version": "1.0", "start": "s",
            "states": [
                { "name": "s", "type": "switch",
                  "events": [],
                  "eventConditions": [
                      { "eventRef": "order.created", "transition": "done" }
                  ],
                  "defaultCondition": { "end": true } },
                { "name": "done", "type": "inject", "data": { "ok": true }, "end": true }
            ]
        });
        let def = parse_cncf_sw_value(v).expect("eventBasedSwitch should parse");
        // 编译出 listen 任务 + switch 任务
        assert!(def
            .do_tasks
            .iter()
            .any(|t| matches!(t.task, Task::Listen(_))));
        assert!(def
            .do_tasks
            .iter()
            .any(|t| matches!(t.task, Task::Switch(_))));
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

    // ═══ P3：全状态编译（delay/event/callback/foreach/parallel/onErrors/compensatedBy） ═══

    fn fn_json(name: &str) -> Value {
        serde_json::json!({
            "name": name,
            "type": "operation",
            "operation": "http://example.com/op"
        })
    }

    #[test]
    fn test_delay_state_compiles_to_wait() {
        let v: Value = serde_json::json!({
            "id": "wf", "version": "1.0", "start": "wait1",
            "states": [
                { "name": "wait1", "type": "delay", "duration": "PT1H", "transition": "end" }
            ]
        });
        let def = parse_cncf_sw_value(v).expect("delay should parse");
        let task = def.do_tasks.iter().find(|t| t.name == "wait1").unwrap();
        match &task.task {
            Task::Wait(w) => assert_eq!(w.wait, "PT1H"),
            other => panic!("expected wait task, got {other:?}"),
        }
    }

    #[test]
    fn test_event_state_compiles_to_listen() {
        let v: Value = serde_json::json!({
            "id": "wf", "version": "1.0", "start": "ev",
            "states": [
                { "name": "ev", "type": "event",
                  "onEvents": [ { "eventRefs": [ { "eventRef": "orderCreated" } ] } ],
                  "transition": "end" },
                { "name": "end", "type": "inject", "data": {}, "end": true }
            ],
            "events": [ { "name": "orderCreated", "type": "order.created", "source": "/orders" } ]
        });
        let def = parse_cncf_sw_value(v).expect("event state should parse");
        let task = def.do_tasks.iter().find(|t| t.name == "ev").unwrap();
        match &task.task {
            Task::Listen(l) => {
                assert_eq!(l.listen.event_type.as_deref(), Some("order.created"));
            }
            other => panic!("expected listen task, got {other:?}"),
        }
    }

    #[test]
    fn test_callback_state_compiles_to_call_and_listen() {
        let v: Value = serde_json::json!({
            "id": "wf", "version": "1.0", "start": "cb",
            "states": [
                { "name": "cb", "type": "callback",
                  "action": { "functionRef": { "refName": "doWork" } },
                  "eventRef": { "triggerEventRef": "workDone" },
                  "transition": "end" }
            ],
            "functions": [ { "name": "doWork", "type": "operation", "operation": "http://x" } ]
        });
        let def = parse_cncf_sw_value(v).expect("callback should parse");
        // 编译出 call 任务 + listen 任务
        assert!(def.do_tasks.iter().any(|t| matches!(t.task, Task::Call(_))));
        assert!(def.do_tasks.iter().any(|t| matches!(t.task, Task::Listen(_))));
    }

    #[test]
    fn test_foreach_state_compiles_to_for_task() {
        let v: Value = serde_json::json!({
            "id": "wf", "version": "1.0", "start": "loop",
            "states": [
                { "name": "loop", "type": "foreach",
                  "iterate": {
                      "input": "${ .items }",
                      "iteration": "item",
                      "actions": [ { "name": "process", "functionRef": { "refName": "proc" } } ]
                  },
                  "transition": "end" }
            ],
            "functions": [ { "name": "proc", "type": "operation", "operation": "http://x" } ]
        });
        let def = parse_cncf_sw_value(v).expect("foreach should parse");
        let task = def.do_tasks.iter().find(|t| t.name == "loop").unwrap();
        match &task.task {
            Task::ForEach(f) => {
                assert_eq!(f.iteration, "item");
                assert_eq!(f.tasks.len(), 1);
            }
            other => panic!("expected for_each task, got {other:?}"),
        }
    }

    #[test]
    fn test_parallel_state_compiles_to_fork() {
        let v: Value = serde_json::json!({
            "id": "wf", "version": "1.0", "start": "par",
            "states": [
                { "name": "par", "type": "parallel",
                  "branches": [
                      { "name": "b1", "actions": [ { "name": "a", "functionRef": { "refName": "f1" } } ] },
                      { "name": "b2", "actions": [ { "name": "b", "functionRef": { "refName": "f2" } } ] }
                  ],
                  "completionType": "allOf",
                  "transition": "end" }
            ],
            "functions": [ fn_json("f1"), fn_json("f2") ]
        });
        let def = parse_cncf_sw_value(v).expect("parallel should parse");
        let task = def.do_tasks.iter().find(|t| t.name == "par").unwrap();
        match &task.task {
            Task::Fork(f) => {
                assert_eq!(f.branches.len(), 2);
                assert_eq!(f.compete, Some(false));
            }
            other => panic!("expected fork task, got {other:?}"),
        }
    }

    #[test]
    fn test_operation_multi_action_and_parallel_mode() {
        // actionMode: parallel → fork；sequential → 顺序 call
        let v: Value = serde_json::json!({
            "id": "wf", "version": "1.0", "start": "ops",
            "states": [
                { "name": "ops", "type": "operation", "actionMode": "parallel",
                  "actions": [
                      { "name": "a", "functionRef": { "refName": "f1" } },
                      { "name": "b", "functionRef": { "refName": "f2" } }
                  ],
                  "transition": "end" }
            ],
            "functions": [ fn_json("f1"), fn_json("f2") ]
        });
        let def = parse_cncf_sw_value(v).expect("parallel operation should parse");
        let task = def.do_tasks.iter().find(|t| t.name == "ops").unwrap();
        assert!(matches!(task.task, Task::Fork(_)), "parallel actionMode → fork");
    }

    #[test]
    fn test_on_errors_compiles_to_try_catch() {
        let v: Value = serde_json::json!({
            "id": "wf", "version": "1.0", "start": "call",
            "states": [
                { "name": "call", "type": "operation",
                  "actions": [ { "name": "a", "functionRef": { "refName": "risky" } } ],
                  "onErrors": [ { "error": "timeout", "transition": "fallback" } ],
                  "transition": "end" },
                { "name": "fallback", "type": "inject", "data": { "recovered": true }, "end": true }
            ],
            "functions": [ fn_json("risky") ]
        });
        let def = parse_cncf_sw_value(v).expect("onErrors should parse");
        let wrapper = def
            .do_tasks
            .iter()
            .find(|t| t.name == "call__try")
            .expect("try-catch wrapper");
        match &wrapper.task {
            Task::TryCatch(tc) => {
                assert_eq!(tc.r#try.len(), 1);
                assert_eq!(tc.catch.len(), 1);
                assert_eq!(tc.catch[0].errors.as_ref().unwrap()[0], "timeout");
            }
            other => panic!("expected try-catch, got {other:?}"),
        }
        // fallback 状态存在
        assert!(def.do_tasks.iter().any(|t| t.name == "fallback"));
    }

    #[test]
    fn test_compensated_by_wraps_in_catch_all() {
        let v: Value = serde_json::json!({
            "id": "wf", "version": "1.0", "start": "work",
            "states": [
                { "name": "work", "type": "operation",
                  "actions": [ { "name": "a", "functionRef": { "refName": "doWork" } } ],
                  "compensatedBy": "undo",
                  "transition": "end" },
                { "name": "undo", "type": "compensate",
                  "actions": [ { "name": "u", "functionRef": { "refName": "undoWork" } } ],
                  "end": true }
            ],
            "functions": [ fn_json("doWork"), fn_json("undoWork") ]
        });
        let def = parse_cncf_sw_value(v).expect("compensatedBy should parse");
        let wrapper = def.do_tasks.iter().find(|t| t.name == "work__try").unwrap();
        match &wrapper.task {
            Task::TryCatch(tc) => {
                // catch-all 转场到补偿状态
                assert_eq!(tc.catch.len(), 1);
                assert!(tc.catch[0].errors.is_none());
            }
            other => panic!("expected try-catch, got {other:?}"),
        }
        assert!(def.do_tasks.iter().any(|t| t.name == "undo"));
    }

    #[test]
    fn test_top_level_retries_timeouts_auth_parsed() {
        let v: Value = serde_json::json!({
            "id": "wf", "version": "1.0", "start": "s",
            "retries": [ { "name": "defaultRetry", "delay": { "seconds": 2 }, "limit": { "attempt": { "count": 4 } } } ],
            "timeouts": [ { "name": "default", "after": "PT5M" } ],
            "auth": [ { "name": "basicAuth", "scheme": "basic", "username": "admin", "password": "pass" } ],
            "states": [
                { "name": "s", "type": "inject", "data": {}, "transition": "end" },
                { "name": "end", "type": "inject", "data": {}, "end": true }
            ]
        });
        let def = parse_cncf_sw_value(v).expect("top-level defs should parse");
        let retries = def.use_components.as_ref().unwrap().retries.as_ref().unwrap();
        assert_eq!(retries["defaultRetry"].limit, 4);
        assert_eq!(def.timeout.as_ref().unwrap().after, "PT5M");
        let auth = def.auth.get("basicAuth").unwrap();
        assert_eq!(auth.scheme.as_deref(), Some("basic"));
        assert_eq!(auth.username.as_deref(), Some("admin"));
    }
}
