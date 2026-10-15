// coord-core/workflow/validate.rs
// 语义校验器 —— RawWorkflowDef → WorkflowDefinition
//
// 将宽松的中间表示（Raw IR）转为强类型的 WorkflowDefinition，同时执行：
// 1. 任务类型推断（根据 body JSON key 推断 task 类型）
// 2. 必填字段校验
// 3. switch transition 目标引用完整性校验
// 4. use.functions 函数引用完整性校验
// 5. switch 循环依赖检测
// 6. 重复任务名检测

use std::collections::{HashMap, HashSet};

use serde_json::Value;

use super::model::{
    ApproveTask, AuthConfig, BackoffStrategy, CallTask, CallType, CatchClause, ConstantsConfig,
    DoTask, Document, EmitEvent, EmitTask, EndTask, ErrorDef, EventFilter, ExportConfig,
    ForEachTask, ForkBranch, ForkTask, FunctionDef, InputConfig, JitterConfig, ListenTask,
    NamedTask, OutputConfig, RaiseTask, RetryPolicy, RunTask, ScheduleConfig, SecretsConfig,
    SetTask, Span, SwitchCondition, SwitchTask, Task, TaskMeta, TimeoutConfig, TryCatchTask,
    UseComponents, ValidationError, ValidationErrorKind, WaitTask, WorkflowDefinition, WorkflowRef,
};

use crate::workflow::approval::model::JoinPolicy;

use super::parser::{
    RawFunctionDef, RawNamedTask, RawRetryPolicy, RawTimeoutConfig, RawUseComponents,
    RawWorkflowDef,
};

// ─── 校验器 ───

/// 官方 1.0.3 switch case 形状判定：`{<case_name>: {when?, then}}` —— 单键对象，且 case 体
/// 含 `when`/`then`。按字段集而非排除键名判定，使 case 名恰为 `condition` 等 legacy 键名时
/// 仍按官方形状解析；legacy `{condition, transition}` 的值为字符串，不会命中。
fn official_switch_case(cond: &Value) -> Option<(&str, &serde_json::Map<String, Value>)> {
    let (name, case_obj) = single_key_object_case(cond)?;
    if !case_obj.contains_key("when") && !case_obj.contains_key("then") {
        return None;
    }
    Some((name, case_obj))
}

/// 单键对象 + 对象 case 体的形状判定：官方 case 的公共外壳（无论是否含 `when`/`then`）。
/// legacy 项的值均为字符串（`condition`/`transition`/`defaultCondition`），不会命中。
fn single_key_object_case(cond: &Value) -> Option<(&str, &serde_json::Map<String, Value>)> {
    let obj = cond.as_object()?;
    if obj.len() != 1 {
        return None;
    }
    let (name, body) = obj.iter().next()?;
    Some((name.as_str(), body.as_object()?))
}

/// approve `mode` DSL 字符串：`all` | `any` | `quorum:<n>` | `ratio:<num>/<den>`。
/// 其它形状返回 `None`，由调用方 fail-closed 拒绝（不猜测策略）。
///
/// 零阈值一律拒绝：`recount` 把 `Quorum(n)` 判作 `completed >= n`、`Ratio{num,den}` 判作
/// `completed*den >= total*num`——`quorum:0` 与 `ratio:0/<den>` 恒真（零审批自动通过），
/// `ratio:<num>/0` 退化（0/0 恒真，其余恒假），都在此挡在解析期。
fn parse_join_policy(s: &str) -> Option<JoinPolicy> {
    match s {
        "all" => Some(JoinPolicy::All),
        "any" => Some(JoinPolicy::Any),
        _ => {
            if let Some(n) = s.strip_prefix("quorum:") {
                match n.parse::<u32>() {
                    Ok(0) | Err(_) => None,
                    Ok(n) => Some(JoinPolicy::Quorum(n)),
                }
            } else if let Some(rest) = s.strip_prefix("ratio:") {
                let (num, den) = rest.split_once('/')?;
                let num = num.parse::<u32>().ok()?;
                let den = den.parse::<u32>().ok()?;
                if num > 0 && den > 0 {
                    Some(JoinPolicy::Ratio { num, den })
                } else {
                    None
                }
            } else {
                None
            }
        }
    }
}

/// `approve.deadline` 是否无法换算成确定的毫秒时长。
///
/// [`parse_iso8601_duration_ms`](crate::workflow::engine::parse_iso8601_duration_ms)
/// 对日期部分的 `M`(月)/`Y`(年) 不换算（月/年长度不定），`P1M`/`P1Y` 会静默返回
/// `Some(0)`——被当成「立即到期」把所有席位秒过期；解析结果 `0` 自身也无意义。故结果
/// 为 `None`/`0`，或 `T` 之前的日期部分含 `M`/`Y` 单位时拒绝；`PT...` 时间部分的
/// `M`(分钟) 不受影响（仍可换算为确定毫秒）。
fn approve_deadline_is_unsupported(duration: &str) -> bool {
    let trimmed = duration.trim();
    match crate::workflow::engine::parse_iso8601_duration_ms(trimmed) {
        None | Some(0) => true,
        Some(_) => {
            let rest = trimmed.strip_prefix('P').unwrap_or(trimmed);
            let date_part = rest.split_once('T').map_or(rest, |(d, _)| d);
            date_part.contains('M') || date_part.contains('Y')
        }
    }
}

/// 语义校验器
pub struct Validator {
    errors: Vec<ValidationError>,
    /// 收集所有已知的任务名（用于引用校验）
    task_names: HashSet<String>,
    /// 收集所有已注册的 function 名
    function_names: HashSet<String>,
    /// use.retries 已解析映射（任务级 `retry: <name>` 引用解析）
    retry_refs: HashMap<String, RetryPolicy>,
    /// 官方 switch case `then: end|exit` 命中：需确保定义中存在终端任务 `__end`
    needs_end: bool,
}

impl Validator {
    /// 执行校验，将 RawWorkflowDef 转为 WorkflowDefinition
    pub fn validate(raw: RawWorkflowDef) -> Result<WorkflowDefinition, Vec<ValidationError>> {
        let mut v = Validator {
            errors: Vec::new(),
            task_names: HashSet::new(),
            function_names: HashSet::new(),
            retry_refs: HashMap::new(),
            needs_end: false,
        };

        // 预先收集 function 名
        if let Some(ref use_comp) = raw.use_components {
            if let Some(ref funcs) = use_comp.functions {
                for name in funcs.keys() {
                    v.function_names.insert(name.clone());
                }
            }
        }

        // Step 1: 构建 Document
        let document = Document {
            dsl: raw.document.dsl,
            namespace: raw.document.namespace,
            name: raw.document.name,
            version: raw.document.version,
            title: raw.document.title,
            summary: raw.document.summary,
            tags: raw.document.tags,
        };

        // Step 2: 解析 use components（先解析，供任务级 retry/timeout 引用解析）
        let use_components = raw.use_components.map(|raw| v.resolve_use_components(raw));
        let retry_map: HashMap<String, RetryPolicy> = use_components
            .as_ref()
            .and_then(|u| u.retries.clone())
            .unwrap_or_default();
        v.retry_refs = retry_map;

        // Step 3: 解析任务类型（先收集任务名再做引用校验）
        let mut named_tasks: Vec<NamedTask> = Vec::new();
        let last_top_level = raw.tasks.len().saturating_sub(1);
        for (idx, raw_task) in raw.tasks.iter().enumerate() {
            // `then` 只有在顶层 do 列表末尾才与顺序执行等价（见方法注释）
            v.check_then_directive(&raw_task.body, idx == last_top_level, raw_task.span);
            // 重复任务名检测
            if !v.task_names.insert(raw_task.name.clone()) {
                v.errors.push(ValidationError {
                    span: Some(raw_task.span),
                    kind: ValidationErrorKind::DuplicateTaskName(raw_task.name.clone()),
                    message: format!("duplicate task name '{}'", raw_task.name),
                });
                continue;
            }

            if let Some(task) = v.resolve_task(raw_task) {
                named_tasks.push(NamedTask {
                    name: raw_task.name.clone(),
                    task,
                });
            }
        }

        // Step 4: 校验 switch transition 引用完整性
        // 官方 switch case `then: end|exit` 映射为终端任务 `__end`：用户未定义时补一个
        // Task::End，保证引用校验与运行期跳转都能解析（与 sw.rs 的终端约定同一个保留名）。
        if v.needs_end {
            let end_is_terminal = named_tasks
                .iter()
                .any(|t| t.name == super::sw::END_TASK && matches!(&t.task, Task::End(_)));
            if !v.task_names.contains(super::sw::END_TASK) {
                named_tasks.push(NamedTask {
                    name: super::sw::END_TASK.to_string(),
                    task: Task::End(EndTask {}),
                });
                v.task_names.insert(super::sw::END_TASK.to_string());
            } else if !end_is_terminal {
                // `__end` 为终端保留名：用户定义的非终端同名任务会让 `then: exit` 落到
                // 普通任务并 fall-through，fail-closed 拒绝并点名保留名。
                v.errors.push(ValidationError {
                    span: None,
                    kind: ValidationErrorKind::UnsupportedConstruct(
                        super::sw::END_TASK.to_string(),
                    ),
                    message: format!(
                        "task name '{}' is reserved for the switch terminal (then: end|exit) and must be an 'end' task",
                        super::sw::END_TASK
                    ),
                });
            }
        }
        v.check_task_refs(&named_tasks);

        // Step 5: 校验 use.functions 引用
        v.check_function_refs(&named_tasks);

        // Step 6: 循环依赖检测（switch transition 不能形成环）
        v.check_cycles(&named_tasks);

        // Step 7: 收集任务级标准字段（if/input/output/export/retry/timeout）
        let mut task_meta: HashMap<String, TaskMeta> = HashMap::new();
        v.collect_task_meta(&raw.tasks, &mut task_meta);

        // Step 8: 解析 input（schema / from / default）
        let input = raw.input.map(|rv| InputConfig {
            schema: rv
                .value
                .get("schema")
                .and_then(|v| v.as_str())
                .map(String::from),
            from: rv
                .value
                .get("from")
                .and_then(|v| v.as_str())
                .map(String::from),
            default: rv.value.get("default").cloned(),
        });

        // Step 9: 解析顶层扩展块（output / timeout / schedule / auth / secrets / constants）
        let mut output: Option<OutputConfig> = None;
        let mut timeout: Option<TimeoutConfig> = None;
        let mut schedule = ScheduleConfig::default();
        let mut auth: std::collections::HashMap<String, AuthConfig> = Default::default();
        let mut secrets = SecretsConfig::default();
        let mut constants = ConstantsConfig::default();

        if let Some(ext) = raw.ext {
            if let Some(ov) = ext.output {
                output = Some(OutputConfig {
                    as_expr: ov
                        .value
                        .get("as")
                        .and_then(|v| v.as_str())
                        .map(String::from),
                    schema: ov
                        .value
                        .get("schema")
                        .and_then(|v| v.as_str())
                        .map(String::from),
                });
            }
            if let Some(tv) = ext.timeout {
                timeout = Some(TimeoutConfig {
                    after: tv
                        .value
                        .get("after")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                });
            }
            if let Some(sv) = ext.schedule {
                schedule = ScheduleConfig {
                    every: sv
                        .value
                        .get("every")
                        .and_then(|v| v.as_str())
                        .map(String::from),
                    cron: sv
                        .value
                        .get("cron")
                        .and_then(|v| v.as_str())
                        .map(String::from),
                    after: sv
                        .value
                        .get("after")
                        .and_then(|v| v.as_str())
                        .map(String::from),
                    on: sv.value.get("on").map(|v| EventFilter {
                        event_type: v.get("type").and_then(|v| v.as_str()).map(String::from),
                        event_types: vec![],
                        source: v.get("source").and_then(|v| v.as_str()).map(String::from),
                        subject: v.get("subject").and_then(|v| v.as_str()).map(String::from),
                    }),
                };
            }
            if let Some(av) = ext.auth {
                if let Some(obj) = av.value.as_object() {
                    for (name, cfg) in obj {
                        auth.insert(name.clone(), v.parse_auth_config(cfg));
                    }
                }
            }
            if let Some(sv) = ext.secrets {
                secrets = SecretsConfig {
                    keys: sv.value.as_array().map(|a| {
                        a.iter()
                            .filter_map(|k| k.as_str().map(String::from))
                            .collect()
                    }),
                };
            }
            if let Some(cv) = ext.constants {
                constants = ConstantsConfig {
                    values: cv.value.as_object().cloned(),
                };
            }
        }

        if v.errors.is_empty() {
            Ok(WorkflowDefinition {
                id: None,
                document,
                do_tasks: named_tasks,
                input,
                output,
                timeout,
                use_components,
                schedule,
                auth,
                secrets,
                constants,
                task_meta,
                raw_yaml: None,
            })
        } else {
            Err(v.errors)
        }
    }

    /// 任务类型推断：根据 body JSON key 推断任务类型
    fn resolve_task(&mut self, raw: &RawNamedTask) -> Option<Task> {
        match &raw.body {
            Value::Object(map) if map.contains_key("call") => self.parse_call(raw, map),
            // `for` 必须先于裸 `do` 分类：官方 1.0.3 循环体 `do` 与 `for` 同级，若先命中
            // `do` 会把循环体当顺序任务、静默丢掉迭代语义（Spike 4 corpus 06）。
            Value::Object(map) if map.contains_key("for") => self.parse_for_each(raw, map),
            Value::Object(map) if map.contains_key("do") => self.parse_do(raw, map),
            Value::Object(map) if map.contains_key("switch") => self.parse_switch(raw, map),
            Value::Object(map) if map.contains_key("fork") => self.parse_fork(raw, map),
            Value::Object(map) if map.contains_key("wait") => self.parse_wait(raw, map),
            Value::Object(map) if map.contains_key("listen") => self.parse_listen(raw, map),
            Value::Object(map) if map.contains_key("emit") => self.parse_emit(raw, map),
            Value::Object(map) if map.contains_key("set") => self.parse_set(raw, map),
            Value::Object(map) if map.contains_key("raise") => self.parse_raise(raw, map),
            Value::Object(map) if map.contains_key("try") => self.parse_try_catch(raw, map),
            Value::Object(map) if map.contains_key("run") => self.parse_run(raw, map),
            Value::Object(map) if map.contains_key("approve") => self.parse_approve(raw, map),
            Value::Object(map) if map.contains_key("end") => self.parse_end(raw, map),
            _ => {
                self.errors.push(ValidationError {
                    span: Some(raw.span),
                    kind: ValidationErrorKind::UnknownTaskType,
                    message: format!("unknown task type in '{}'", raw.name),
                });
                None
            }
        }
    }

    // ─── 各任务类型解析 ───

    fn parse_call(
        &mut self,
        _raw: &RawNamedTask,
        map: &serde_json::Map<String, Value>,
    ) -> Option<Task> {
        let call_val = &map["call"];
        let call_type = match call_val {
            Value::String(s) => {
                match s.as_str() {
                    "http" => CallType::Http,
                    "grpc" => CallType::Grpc,
                    "function" => {
                        // function call 需要从 with.function 中获取函数名
                        let func_name = map
                            .get("with")
                            .and_then(|w| w.as_object())
                            .and_then(|wo| wo.get("function"))
                            .and_then(|f| f.as_str())
                            .unwrap_or("");
                        CallType::Function(func_name.to_string())
                    }
                    other => {
                        // 可能是自定义函数调用简写
                        // 检查 with.function 是否存在
                        if let Some(with_obj) = map.get("with").and_then(|w| w.as_object()) {
                            if let Some(func_name) =
                                with_obj.get("function").and_then(|f| f.as_str())
                            {
                                CallType::Function(func_name.to_string())
                            } else {
                                CallType::Function(other.to_string())
                            }
                        } else {
                            CallType::Function(other.to_string())
                        }
                    }
                }
            }
            Value::Object(call_obj) => {
                // call: { http: { ... } } or call: { grpc: { ... } }
                if call_obj.contains_key("http") {
                    CallType::Http
                } else if call_obj.contains_key("grpc") {
                    CallType::Grpc
                } else {
                    // 可能是带参数的函数调用
                    CallType::Http // fallback
                }
            }
            _ => CallType::Http, // fallback
        };

        let with = map.get("with").cloned();
        Some(Task::Call(CallTask {
            call: call_type,
            with,
        }))
    }

    fn parse_do(
        &mut self,
        raw: &RawNamedTask,
        map: &serde_json::Map<String, Value>,
    ) -> Option<Task> {
        let do_val = &map["do"];
        let tasks = self.parse_sub_tasks(do_val, raw.span);
        Some(Task::Do(DoTask { tasks }))
    }

    fn parse_switch(
        &mut self,
        raw: &RawNamedTask,
        map: &serde_json::Map<String, Value>,
    ) -> Option<Task> {
        let switch_val = &map["switch"];
        let arr = match switch_val {
            Value::Array(arr) => arr,
            _ => {
                self.errors.push(ValidationError {
                    span: Some(raw.span),
                    kind: ValidationErrorKind::TypeMismatch("switch must be an array".to_string()),
                    message: "switch must be an array of conditions".to_string(),
                });
                return None;
            }
        };

        let mut conditions = Vec::new();
        let mut default_condition: Option<SwitchCondition> = None;
        for (index, item) in arr.iter().enumerate() {
            if let Some((case_name, case_obj)) = official_switch_case(item) {
                // 官方 1.0.3 case：`when` 可选（缺失即默认 case），`then` 必需。
                if let Some(cond) = self.parse_official_switch_case(case_name, case_obj, raw.span) {
                    // 无 `when` 的官方 case 提升到 default_condition：运行期在全部 `when`
                    // 条件之后才求值，而官方按数组顺序求值 when-less case。此为已裁定偏差：
                    // 单个 default case 语义等价，多个 when-less case 由下方唯一性校验拒绝。
                    if cond.condition.is_some() {
                        conditions.push(cond);
                    } else if default_condition.is_none() {
                        default_condition = Some(cond);
                    } else {
                        // default 只能有一个：多个无 `when` case fail-closed 点名冲突项。
                        self.reject_switch_field(case_name, "when", raw.span);
                    }
                }
            } else if let Some((case_name, _)) = single_key_object_case(item) {
                // 官方单键 case 体是对象但缺 `when`/`then`（如 `{high: {}}`）：不得回退 legacy
                // 报「缺 transition」，fail-closed 点名 `switch.<case>.then`。
                self.reject_switch_field(case_name, "then", raw.span);
            } else if let Some(cond) = self.parse_switch_condition(item, raw.span) {
                // legacy `{condition, transition}` / `{defaultCondition}` 形状保持兼容。
                conditions.push(cond);
            } else if !item.is_object() {
                // `switch: ["foo"]` 非对象项：legacy 路径会静默丢弃 → fail-closed 点名 `switch[i]`。
                let construct = format!("switch[{index}]");
                self.errors.push(ValidationError {
                    span: Some(raw.span),
                    kind: ValidationErrorKind::UnsupportedConstruct(construct.clone()),
                    message: format!(
                        "unimplemented construct '{construct}': each switch item must be an object"
                    ),
                });
            }
        }

        Some(Task::Switch(SwitchTask {
            conditions,
            default_condition,
        }))
    }

    /// 官方 1.0.3 case 形状：`when`（可选）→ condition、`then`（必需，flowDirective）→ transition。
    /// `then: end|exit` 映射到终端任务 `__end`；`then: continue`（do 列表 fall-through）coord 在
    /// 不改执行器的前提下无法表达，fail-closed 拒绝并点名 `switch.<case>.then`。
    fn parse_official_switch_case(
        &mut self,
        case_name: &str,
        case_obj: &serde_json::Map<String, Value>,
        span: Span,
    ) -> Option<SwitchCondition> {
        // 未知键（schema unevaluatedProperties: false）不得静默丢弃。
        for key in case_obj.keys() {
            if key != "when" && key != "then" {
                self.reject_switch_field(case_name, key, span);
                return None;
            }
        }
        let condition = match case_obj.get("when") {
            Some(Value::String(s)) => Some(s.clone()),
            Some(_) => {
                self.reject_switch_field(case_name, "when", span);
                return None;
            }
            None => None,
        };
        let Some(then) = case_obj.get("then").and_then(Value::as_str) else {
            self.reject_switch_field(case_name, "then", span);
            return None;
        };
        let transition = match then {
            "end" | "exit" => {
                self.needs_end = true;
                super::sw::END_TASK.to_string()
            }
            "continue" => {
                self.errors.push(ValidationError {
                    span: Some(span),
                    kind: ValidationErrorKind::UnsupportedConstruct(format!(
                        "switch.{case_name}.then"
                    )),
                    message: format!(
                        "unimplemented construct 'switch.{case_name}.then': 'continue' (do-list fall-through) is not mapped"
                    ),
                });
                return None;
            }
            target => target.to_string(),
        };
        Some(SwitchCondition {
            condition,
            transition,
        })
    }

    /// 官方 switch case 缺项/未知键 fail-closed：点名 `switch.<case>.<field>`，绝不回退默认。
    fn reject_switch_field(&mut self, case_name: &str, field: &str, span: Span) {
        let construct = format!("switch.{case_name}.{field}");
        self.errors.push(ValidationError {
            span: Some(span),
            kind: ValidationErrorKind::UnsupportedConstruct(construct.clone()),
            message: format!(
                "unimplemented construct '{construct}': official switch case requires 'when'/'then' (then: <target>|end|exit)"
            ),
        });
    }

    fn parse_switch_condition(&mut self, cond: &Value, span: Span) -> Option<SwitchCondition> {
        let obj = cond.as_object()?;

        // condition (optional — defaultCondition has no condition)
        let condition = obj
            .get("condition")
            .and_then(|v| v.as_str())
            .map(String::from);

        // transition or defaultCondition
        let transition = if let Some(t) = obj.get("transition").and_then(|v| v.as_str()) {
            t.to_string()
        } else if let Some(d) = obj.get("defaultCondition").and_then(|v| v.as_str()) {
            d.to_string()
        } else if let Some(d) = obj.get("default").and_then(|v| v.as_str()) {
            d.to_string()
        } else {
            self.errors.push(ValidationError {
                span: Some(span),
                kind: ValidationErrorKind::MissingRequiredField(
                    "transition or defaultCondition".into(),
                ),
                message: "switch condition missing transition target".to_string(),
            });
            return None;
        };

        Some(SwitchCondition {
            condition,
            transition,
        })
    }

    fn parse_fork(
        &mut self,
        raw: &RawNamedTask,
        map: &serde_json::Map<String, Value>,
    ) -> Option<Task> {
        let fork_val = &map["fork"];
        // 官方 1.0.3 形状 `fork: {compete?, branches: [{<branch_name>: <taskBody>}, ...]}`
        // 与 coord 既有裸数组/`{branches}` 形状兼容：两者都规约到同一分支数组。
        let branches_arr = match fork_val {
            Value::Array(arr) => Some(arr),
            Value::Object(obj) => obj.get("branches").and_then(|v| v.as_array()),
            _ => None,
        };
        // 官方把 `compete` 放在 `fork` 对象内；coord 既有形状把它放在任务体上。
        // official-first，legacy 兄弟键回退；裸数组形状没有 `compete` 取 None。
        let compete = fork_val
            .get("compete")
            .or_else(|| map.get("compete"))
            .and_then(Value::as_bool);

        // 缺 branches / 非数组 / 空列表都不得静默产出 0 分支的 Fork。
        let Some(branches_arr) = branches_arr.filter(|arr| !arr.is_empty()) else {
            self.errors.push(ValidationError {
                span: Some(raw.span),
                kind: ValidationErrorKind::UnsupportedConstruct("fork.branches".to_string()),
                message: "unimplemented construct 'fork.branches': fork requires a non-empty \
                          'branches' list"
                    .to_string(),
            });
            return None;
        };

        let mut branches = Vec::with_capacity(branches_arr.len());
        for (index, branch) in branches_arr.iter().enumerate() {
            if let Some(parsed) = self.parse_fork_branch(branch, index, raw.span) {
                branches.push(parsed);
            }
        }
        // 有无法映射的分支项时已点名登记错误，整体 fail-closed。
        if branches.len() != branches_arr.len() {
            return None;
        }

        Some(Task::Fork(ForkTask { branches, compete }))
    }

    fn parse_fork_branch(
        &mut self,
        branch: &Value,
        index: usize,
        span: Span,
    ) -> Option<ForkBranch> {
        let Some(obj) = branch.as_object() else {
            self.reject_fork_branch(index, span);
            return None;
        };

        // coord 既有形状：`{name, do|tasks}`——`name` 必须是字符串才走此路；否则
        // `{name: <body>}` 是官方单键 map（分支名恰为 `name`），不得落成空名分支。
        if let Some(name) = obj.get("name").and_then(Value::as_str) {
            let name = name.to_string();
            let tasks = if let Some(do_val) = obj.get("do") {
                self.parse_sub_tasks(do_val, span)
            } else if let Some(tasks_val) = obj.get("tasks") {
                self.parse_sub_tasks(tasks_val, span)
            } else {
                Vec::new()
            };
            return Some(ForkBranch { name, tasks });
        }

        // 官方 1.0.3 形状：单键 map `{<branch_name>: <taskBody>}`，键名即分支名；
        // 分支项本身就是一个 taskList 项，交由 parse_sub_tasks 解析其 body。
        if obj.len() == 1 {
            if let Some(name) = obj.keys().next() {
                let tasks = self.parse_sub_tasks(&Value::Array(vec![branch.clone()]), span);
                return Some(ForkBranch {
                    name: name.clone(),
                    tasks,
                });
            }
        }

        self.reject_fork_branch(index, span);
        None
    }

    fn reject_fork_branch(&mut self, index: usize, span: Span) {
        let construct = format!("fork.branches[{index}]");
        self.errors.push(ValidationError {
            span: Some(span),
            kind: ValidationErrorKind::UnsupportedConstruct(construct.clone()),
            message: format!(
                "unimplemented construct '{construct}': each fork branch must be a single-key map \
                 '{{<branch_name>: <task>}}' or an object with a 'name' field"
            ),
        });
    }

    fn parse_for_each(
        &mut self,
        raw: &RawNamedTask,
        map: &serde_json::Map<String, Value>,
    ) -> Option<Task> {
        let for_val = &map["for"];
        let Some(obj) = for_val.as_object() else {
            self.errors.push(ValidationError {
                span: Some(raw.span),
                kind: ValidationErrorKind::UnsupportedConstruct("for".to_string()),
                message: "unimplemented construct 'for': 'for' must be an object with \
                          'each'/'in' (official) or 'input'/'iteration' (legacy)"
                    .to_string(),
            });
            return None;
        };

        // `for.at`（集合路径表达式）是官方 schema 字段，coord 未实现；即使与 each/in 同现
        // 也不能静默忽略，否则 at 指向的集合被丢弃、迭代语义错位。
        if obj.contains_key("at") {
            self.reject_for_field("for.at", raw.span);
            return None;
        }

        // `while`（forTask 的循环继续条件，与 `for` 同级）coord 未实现：当前解析不读它，
        // 条件会被静默忽略而迭代无界——点名 `while` fail-closed。`for` 内出现 `while`
        // （非 schema 形状）同样拒绝，不静默。
        if map.contains_key("while") || obj.contains_key("while") {
            self.errors.push(ValidationError {
                span: Some(raw.span),
                kind: ValidationErrorKind::UnsupportedConstruct("while".to_string()),
                message: "unimplemented construct 'while': coord for-each has no loop \
                          continuation condition"
                    .to_string(),
            });
            return None;
        }

        // 官方 1.0.3 形状 `for: {each, in}`，循环体是与 `for` 同级的 `do`；legacy 形状
        // `for: {input, iteration, do|tasks}`。出现 each/in 即按官方形状严格校验，缺项点名，
        // 不回退默认值静默丢失迭代语义。既无官方键也无 legacy 键的对象（含 `for: {}`）无
        // 迭代来源，fail-closed 而非伪造 `iteration="item"`/`input=""`。
        let official = obj.contains_key("each") || obj.contains_key("in");
        let legacy = obj.contains_key("input") || obj.contains_key("iteration");
        if !official && !legacy {
            self.reject_for_field("for.in", raw.span);
            return None;
        }
        let (iteration, input) = if official {
            let Some(each) = obj.get("each").and_then(Value::as_str) else {
                self.reject_for_field("for.each", raw.span);
                return None;
            };
            let Some(input) = obj.get("in").and_then(Value::as_str) else {
                self.reject_for_field("for.in", raw.span);
                return None;
            };
            (each.to_string(), input.to_string())
        } else {
            (
                obj.get("iteration")
                    .and_then(Value::as_str)
                    .unwrap_or("item")
                    .to_string(),
                obj.get("input")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
            )
        };

        // 循环体：官方在 `for` 同级，legacy 在 `for` 内（`do`/`tasks`）。
        let body = map
            .get("do")
            .or_else(|| obj.get("do"))
            .or_else(|| obj.get("tasks"));
        let Some(body) = body else {
            self.reject_for_field("for.do", raw.span);
            return None;
        };
        let tasks = self.parse_sub_tasks(body, raw.span);
        // 空循环体等于跑零次迭代——字段在场却无语义，fail-closed 而非静默接受。
        if tasks.is_empty() {
            self.reject_for_field("for.do", raw.span);
            return None;
        }

        Some(Task::ForEach(ForEachTask {
            input,
            iteration,
            tasks,
        }))
    }

    /// `for` 形状缺项 fail-closed：点名构造，绝不回退默认值。
    fn reject_for_field(&mut self, construct: &str, span: Span) {
        self.errors.push(ValidationError {
            span: Some(span),
            kind: ValidationErrorKind::UnsupportedConstruct(construct.to_string()),
            message: format!(
                "unimplemented construct '{construct}': coord for-each requires this field \
                 (official shape: for.each / for.in / sibling for.do)"
            ),
        });
    }

    fn parse_wait(
        &mut self,
        raw: &RawNamedTask,
        map: &serde_json::Map<String, Value>,
    ) -> Option<Task> {
        let wait_str = self.parse_duration_value(raw.span, &map["wait"], "wait")?;
        Some(Task::Wait(WaitTask { wait: wait_str }))
    }

    /// 解析 duration 值（ISO8601 字符串 / 官方相对时长组件对象 / 数字）为规范化 duration 字符串。
    ///
    /// `construct` 是错误锚点（`wait` / `approve.deadline`）。官方 1.0.3 的相对时长是
    /// `{days?, hours?, minutes?, seconds?, milliseconds?}` 组件对象（schema
    /// workflow-1.0.3.yaml `duration`）；调用方只消费 ISO8601 时长字符串，故把全部在场组件
    /// 合成一个 duration（如 `{seconds:1, milliseconds:500}` → `PT1.5S`）。`until` 是绝对时刻、
    /// 解析期无墙钟，`${...}` 是运行时表达式，未知键无语义——三者都改变语义，一律点名构造
    /// fail-closed，不得静默丢字段（Spike 4 corpus 07 误读）。
    fn parse_duration_value(&mut self, span: Span, val: &Value, construct: &str) -> Option<String> {
        match val {
            Value::String(s) => {
                if s.contains("${") {
                    self.errors.push(ValidationError {
                        span: Some(span),
                        kind: ValidationErrorKind::UnsupportedConstruct(construct.to_string()),
                        message: format!(
                            "unimplemented construct '{construct}': runtime expression durations \
                             are not supported"
                        ),
                    });
                    return None;
                }
                Some(s.clone())
            }
            Value::Number(_) => Some(val.to_string()),
            Value::Object(obj) => {
                let recognized = ["days", "hours", "minutes", "seconds", "milliseconds"];
                let unknown: Vec<String> = obj
                    .keys()
                    .filter(|k| k.as_str() != "until" && !recognized.contains(&k.as_str()))
                    .map(|k| format!("{construct}.{k}"))
                    .collect();
                if !unknown.is_empty() {
                    let joined = unknown.join(", ");
                    self.errors.push(ValidationError {
                        span: Some(span),
                        kind: ValidationErrorKind::UnsupportedConstruct(joined.clone()),
                        message: format!(
                            "unimplemented construct '{joined}': coord {construct} supports only \
                             'days'/'hours'/'minutes'/'seconds'/'milliseconds'"
                        ),
                    });
                    return None;
                }
                if obj.contains_key("until") {
                    self.errors.push(ValidationError {
                        span: Some(span),
                        kind: ValidationErrorKind::UnsupportedConstruct(format!(
                            "{construct}.until"
                        )),
                        message: format!(
                            "unimplemented construct '{construct}.until': coord has no wall-clock \
                             at parse time; use a relative duration"
                        ),
                    });
                    return None;
                }
                // 在场组件必须是非负整数；非法值不得静默忽略（fail-closed）。
                for key in recognized {
                    if obj.get(key).is_some_and(|v| v.as_u64().is_none()) {
                        self.errors.push(ValidationError {
                            span: Some(span),
                            kind: ValidationErrorKind::UnsupportedConstruct(format!(
                                "{construct}.{key}"
                            )),
                            message: format!("{construct}.{key} must be a non-negative integer"),
                        });
                        return None;
                    }
                }
                let component = |key: &str| obj.get(key).and_then(|v| v.as_u64());
                if !recognized.iter().any(|k| obj.contains_key(*k)) {
                    // 空对象 / 无从映射的时长：拒绝，不静默落成空值。
                    self.errors.push(ValidationError {
                        span: Some(span),
                        kind: ValidationErrorKind::UnsupportedConstruct(construct.to_string()),
                        message: format!(
                            "{construct} duration requires at least one of \
                             'days'/'hours'/'minutes'/'seconds'/'milliseconds'"
                        ),
                    });
                    return None;
                }
                let mut dur = String::from("P");
                if let Some(days) = component("days") {
                    dur.push_str(&format!("{days}D"));
                }
                let mut time = String::new();
                if let Some(hours) = component("hours") {
                    time.push_str(&format!("{hours}H"));
                }
                if let Some(minutes) = component("minutes") {
                    time.push_str(&format!("{minutes}M"));
                }
                if obj.contains_key("seconds") || obj.contains_key("milliseconds") {
                    // 秒与毫秒合并成一个（可带小数的）秒分量，避免 `PT1S` + `PT0.5S` 冲突。
                    let total_ms: u128 = component("seconds").unwrap_or(0) as u128 * 1000
                        + component("milliseconds").unwrap_or(0) as u128;
                    let whole = total_ms / 1000;
                    let frac = total_ms % 1000;
                    if frac == 0 {
                        time.push_str(&format!("{whole}S"));
                    } else {
                        let mut frac_digits = format!("{frac:03}");
                        while frac_digits.ends_with('0') {
                            frac_digits.pop();
                        }
                        time.push_str(&format!("{whole}.{frac_digits}S"));
                    }
                }
                if !time.is_empty() {
                    dur.push('T');
                    dur.push_str(&time);
                }
                Some(dur)
            }
            _ => {
                // 非法形状（bool/array/null）不得静默落成空任务。
                self.errors.push(ValidationError {
                    span: Some(span),
                    kind: ValidationErrorKind::UnsupportedConstruct(construct.to_string()),
                    message: format!(
                        "{construct} must be an ISO8601 duration string or a duration object"
                    ),
                });
                None
            }
        }
    }

    fn parse_listen(
        &mut self,
        raw: &RawNamedTask,
        map: &serde_json::Map<String, Value>,
    ) -> Option<Task> {
        let listen_val = &map["listen"];

        // 官方 1.0.3 形状：`listen.to.{one|all|any}.with.{type, source?, subject?}`
        // （schema workflow-1.0.3.yaml `listenTask`/`eventConsumptionStrategy`）。陷阱：把
        // `to.one.with` 过滤条件当平铺形状读会静默丢过滤器，故检测到 `to` 即按官方形状
        // 严格解析；`to` 缺失时保留 coord 既有平铺/字符串形状。
        if let Some(listen_obj) = listen_val.as_object() {
            if listen_obj.contains_key("to") {
                return self.parse_listen_official(raw, listen_obj);
            }
        }

        let obj = listen_val.as_object();

        let event_filter = if let Some(obj) = obj {
            EventFilter {
                event_type: obj.get("type").and_then(|v| v.as_str()).map(String::from),
                event_types: vec![],
                source: obj.get("source").and_then(|v| v.as_str()).map(String::from),
                subject: obj
                    .get("subject")
                    .and_then(|v| v.as_str())
                    .map(String::from),
            }
        } else if let Some(s) = listen_val.as_str() {
            EventFilter {
                event_type: Some(s.to_string()),
                event_types: vec![],
                source: None,
                subject: None,
            }
        } else {
            // 非对象也非字符串的 listen（bool/array/null/number）无过滤器可映射；
            // 不得静默落成空监听。
            self.errors.push(ValidationError {
                span: Some(raw.span),
                kind: ValidationErrorKind::UnsupportedConstruct("listen".to_string()),
                message: "listen must be an object or an event-type string".to_string(),
            });
            return None;
        };

        Some(Task::Listen(ListenTask {
            listen: event_filter,
        }))
    }

    /// 解析官方 `listen.to` 消费策略（当前仅 `one`）。返回 `None` 表示已登记错误。
    ///
    /// coord 的 `EventFilter` 只有 event_type/source/subject；官方形状里其余键
    /// （`listen.read`、`eventFilter.correlate`、`eventProperties` 的 id/time/data 等）
    /// 无映射面，一律点名构造 fail-closed，绝不静默丢字段。
    fn parse_listen_official(
        &mut self,
        raw: &RawNamedTask,
        listen_obj: &serde_json::Map<String, Value>,
    ) -> Option<Task> {
        // `ListenTaskConfiguration` 只声明 `to`/`read`；`read`（envelope/raw 读取模式）
        // 无对应字段，未知键同样不可映射。
        let listen_extras: Vec<String> = listen_obj
            .keys()
            .filter(|k| !matches!(k.as_str(), "to" | "read"))
            .map(|k| format!("listen.{k}"))
            .chain(
                listen_obj
                    .contains_key("read")
                    .then_some("listen.read".to_string()),
            )
            .collect();
        if !listen_extras.is_empty() {
            let construct = listen_extras.join(", ");
            self.errors.push(ValidationError {
                span: Some(raw.span),
                kind: ValidationErrorKind::UnsupportedConstruct(construct.clone()),
                message: format!(
                    "unimplemented construct '{construct}': coord listen supports only \
                     'listen.to.one.with'"
                ),
            });
            return None;
        }

        let Some(to) = listen_obj.get("to").and_then(|v| v.as_object()) else {
            self.errors.push(ValidationError {
                span: Some(raw.span),
                kind: ValidationErrorKind::UnsupportedConstruct("listen.to".to_string()),
                message: "unimplemented construct 'listen.to': expected a consumption strategy \
                          object"
                    .to_string(),
            });
            return None;
        };

        // `EventConsumptionStrategy` oneOf all/any/one：本期只实现 `one`。
        if let Some(strategy) = ["all", "any"].iter().find(|s| to.contains_key(**s)) {
            let construct = format!("listen.to.{strategy}");
            self.errors.push(ValidationError {
                span: Some(raw.span),
                kind: ValidationErrorKind::UnsupportedConstruct(construct.clone()),
                message: format!(
                    "unimplemented construct '{construct}': coord listen supports only 'to.one'"
                ),
            });
            return None;
        }
        let to_extras: Vec<String> = to
            .keys()
            .filter(|k| k.as_str() != "one")
            .map(|k| format!("listen.to.{k}"))
            .collect();
        if !to_extras.is_empty() {
            let construct = to_extras.join(", ");
            self.errors.push(ValidationError {
                span: Some(raw.span),
                kind: ValidationErrorKind::UnsupportedConstruct(construct.clone()),
                message: format!(
                    "unimplemented construct '{construct}': coord listen supports only 'to.one'"
                ),
            });
            return None;
        }

        let Some(one) = to.get("one").and_then(|v| v.as_object()) else {
            self.errors.push(ValidationError {
                span: Some(raw.span),
                kind: ValidationErrorKind::UnsupportedConstruct("listen.to.one".to_string()),
                message: "unimplemented construct 'listen.to.one': expected an event filter object"
                    .to_string(),
            });
            return None;
        };

        // `eventFilter` 只有 `with`（+`correlate`）；`correlate` 需把事件属性关联到
        // 输入数据属性，无映射面。
        let one_extras: Vec<String> = one
            .keys()
            .filter(|k| !matches!(k.as_str(), "with" | "correlate"))
            .map(|k| format!("listen.to.one.{k}"))
            .chain(
                one.contains_key("correlate")
                    .then_some("listen.to.one.correlate".to_string()),
            )
            .collect();
        if !one_extras.is_empty() {
            let construct = one_extras.join(", ");
            self.errors.push(ValidationError {
                span: Some(raw.span),
                kind: ValidationErrorKind::UnsupportedConstruct(construct.clone()),
                message: format!(
                    "unimplemented construct '{construct}': event filter supports only 'with'"
                ),
            });
            return None;
        }

        let with = one.get("with").and_then(|v| v.as_object());
        let with_unknown: Vec<String> = with
            .map(|w| {
                w.keys()
                    .filter(|k| !matches!(k.as_str(), "type" | "source" | "subject"))
                    .map(|k| format!("listen.to.one.with.{k}"))
                    .collect()
            })
            .unwrap_or_default();
        if !with_unknown.is_empty() {
            let construct = with_unknown.join(", ");
            self.errors.push(ValidationError {
                span: Some(raw.span),
                kind: ValidationErrorKind::UnsupportedConstruct(construct.clone()),
                message: format!(
                    "unimplemented construct '{construct}': listen filter supports only \
                     'type'/'source'/'subject'"
                ),
            });
            return None;
        }

        let Some(event_type) = with.and_then(|w| w.get("type")).and_then(|v| v.as_str()) else {
            self.errors.push(ValidationError {
                span: Some(raw.span),
                kind: ValidationErrorKind::UnsupportedConstruct(
                    "listen.to.one.with.type".to_string(),
                ),
                message: "listen requires a string 'listen.to.one.with.type'".to_string(),
            });
            return None;
        };

        Some(Task::Listen(ListenTask {
            listen: EventFilter {
                event_type: Some(event_type.to_string()),
                event_types: vec![],
                source: with
                    .and_then(|w| w.get("source"))
                    .and_then(|v| v.as_str())
                    .map(String::from),
                subject: with
                    .and_then(|w| w.get("subject"))
                    .and_then(|v| v.as_str())
                    .map(String::from),
            },
        }))
    }

    fn parse_emit(
        &mut self,
        raw: &RawNamedTask,
        map: &serde_json::Map<String, Value>,
    ) -> Option<Task> {
        let emit_val = &map["emit"];
        let obj = emit_val.as_object();

        // 官方 1.0.3 把载荷嵌在 `emit.event.with.{type, source?, data?}`
        // （schema workflow-1.0.3.yaml `emitTask`/`eventProperties`，Spike 4 corpus 09）。
        // `event` 在场即官方形状。coord 的 EmitEvent 只有 type/source/data 三个字段：
        // - schema `EmitEventDefinition` 声明 `additionalProperties: true`，`event`
        //   兄弟键（如 id）schema 合法但无对应字段；
        // - eventProperties 另定义 id/time/subject/datacontenttype/dataschema。
        // 两类超范围键都无法映射，静默丢弃即误读，故一律点名构造 fail-closed。
        // `with.type` 是必填事件类型，缺失同样点名拒绝，不得落成空 event_type。
        // 既有平铺 `{type, source, data}` 形状保持不变。
        let (event_type, source, data) = match obj {
            Some(obj) if obj.contains_key("event") => {
                // `event` 的兄弟键在 `emit` 下 schema 非法（EmitTaskConfiguration
                // `unevaluatedProperties: false`），且无 EmitEvent 映射面；静默丢弃即误读。
                let emit_siblings: Vec<String> = obj
                    .keys()
                    .filter(|k| k.as_str() != "event")
                    .map(|k| format!("emit.{k}"))
                    .collect();
                if !emit_siblings.is_empty() {
                    let construct = emit_siblings.join(", ");
                    self.errors.push(ValidationError {
                        span: Some(raw.span),
                        kind: ValidationErrorKind::UnsupportedConstruct(construct.clone()),
                        message: format!(
                            "unimplemented construct '{construct}': emit supports only \
                             'emit.event'"
                        ),
                    });
                    return None;
                }
                let event = obj.get("event").and_then(|e| e.as_object());
                if let Some(event) = event {
                    let unknown: Vec<String> = event
                        .keys()
                        .filter(|k| k.as_str() != "with")
                        .map(|k| format!("emit.event.{k}"))
                        .collect();
                    if !unknown.is_empty() {
                        let construct = unknown.join(", ");
                        self.errors.push(ValidationError {
                            span: Some(raw.span),
                            kind: ValidationErrorKind::UnsupportedConstruct(construct.clone()),
                            message: format!(
                                "unimplemented construct '{construct}': emit event supports only \
                                 'with'"
                            ),
                        });
                        return None;
                    }
                }
                let with = event
                    .and_then(|e| e.get("with"))
                    .and_then(|w| w.as_object());
                if let Some(with) = with {
                    let unknown: Vec<String> = with
                        .keys()
                        .filter(|k| !matches!(k.as_str(), "type" | "source" | "data"))
                        .map(|k| format!("emit.event.with.{k}"))
                        .collect();
                    if !unknown.is_empty() {
                        let construct = unknown.join(", ");
                        self.errors.push(ValidationError {
                            span: Some(raw.span),
                            kind: ValidationErrorKind::UnsupportedConstruct(construct.clone()),
                            message: format!(
                                "unimplemented construct '{construct}': emit event supports only \
                                 'type'/'source'/'data'"
                            ),
                        });
                        return None;
                    }
                }
                let Some(event_type) = with.and_then(|w| w.get("type")).and_then(|v| v.as_str())
                else {
                    self.errors.push(ValidationError {
                        span: Some(raw.span),
                        kind: ValidationErrorKind::UnsupportedConstruct(
                            "emit.event.with.type".to_string(),
                        ),
                        message: "emit requires a string 'emit.event.with.type'".to_string(),
                    });
                    return None;
                };
                (
                    event_type.to_string(),
                    with.and_then(|w| w.get("source"))
                        .and_then(|v| v.as_str())
                        .map(String::from),
                    with.and_then(|w| w.get("data")).cloned(),
                )
            }
            Some(obj) => (
                obj.get("type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                obj.get("source").and_then(|v| v.as_str()).map(String::from),
                obj.get("data").cloned(),
            ),
            None => {
                // 非对象 emit（bool/array/null/number）无字段可映射；不得静默落成空事件。
                self.errors.push(ValidationError {
                    span: Some(raw.span),
                    kind: ValidationErrorKind::UnsupportedConstruct("emit".to_string()),
                    message: "emit must be an object (`emit.event.with` or flat \
                              `{type,source,data}`)"
                        .to_string(),
                });
                return None;
            }
        };

        Some(Task::Emit(EmitTask {
            emit: EmitEvent {
                event_type,
                source,
                data,
            },
        }))
    }

    fn parse_set(
        &mut self,
        raw: &RawNamedTask,
        map: &serde_json::Map<String, Value>,
    ) -> Option<Task> {
        let set_val = &map["set"];
        let Some(obj) = set_val.as_object() else {
            self.errors.push(ValidationError {
                span: Some(raw.span),
                kind: ValidationErrorKind::UnsupportedConstruct("set".to_string()),
                message: "set must be an object of variable assignments".to_string(),
            });
            return None;
        };

        // coord 既有形状：`{variable, value}`（`value` 为 jq 表达式字符串）。必须两键齐备
        // 才走 native 分支：官方单键 map `set: {variable: "x"}` 的键名恰为 `variable`，
        // 只看 `variable` 会把它的值误当变量名、丢 `value`。
        if let (Some(variable), Some(value)) = (
            obj.get("variable").and_then(Value::as_str),
            obj.get("value").and_then(Value::as_str),
        ) {
            return Some(Task::Set(SetTask {
                variable: variable.to_string(),
                value: value.to_string(),
            }));
        }

        // 官方 1.0.3 形状：`set: {<var>: <literal-or-${expr}>}`（corpus 01/05/06）。
        // 单键映射成单变量 SetTask；多键超出单变量模型，逐键点名 fail-closed。
        if obj.len() == 1 {
            let Some((variable, value)) = obj.iter().next() else {
                // 不可达：`len == 1` 必有唯一键；不可达分支不得 panic（panic-path gate）。
                return None;
            };
            // 值里嵌了 `${ ... }`（trim 后不是单个完整 `${ ... }` 表达式）时 coord 无字符串
            // 插值/部分模板面，当字面量处理会静默丢模板语义或丢尾部——fail-closed 点名
            // `set.<key>`。`${ .a }${ .b }` 这类多段拼接也算非单段完整表达式。
            if let Value::String(s) = value {
                let trimmed = s.trim();
                let segments = trimmed.matches("${").count();
                let is_full_expr =
                    segments == 1 && trimmed.starts_with("${") && trimmed.ends_with('}');
                if segments > 0 && !is_full_expr {
                    let construct = format!("set.{variable}");
                    self.errors.push(ValidationError {
                        span: Some(raw.span),
                        kind: ValidationErrorKind::UnsupportedConstruct(construct.clone()),
                        message: format!(
                            "unimplemented construct '{construct}': coord set has no string \
                             interpolation; embedded '${{...}}' would be dropped as a literal"
                        ),
                    });
                    return None;
                }
            }
            // 数组字面量（含对象字面量内嵌）求值器无 `[...]` 分支，会落到 `Ok(context)`——
            // 静默把整个上下文当变量值。数组字面量支持前解析期点名拒绝。
            if set_value_contains_array(value) {
                let construct = format!("set.{variable}");
                self.errors.push(ValidationError {
                    span: Some(raw.span),
                    kind: ValidationErrorKind::UnsupportedConstruct(construct.clone()),
                    message: format!(
                        "unimplemented construct '{construct}': array values are not supported"
                    ),
                });
                return None;
            }
            return Some(Task::Set(SetTask {
                variable: variable.clone(),
                value: official_set_value_expr(value),
            }));
        }

        if obj.is_empty() {
            self.errors.push(ValidationError {
                span: Some(raw.span),
                kind: ValidationErrorKind::UnsupportedConstruct("set".to_string()),
                message: "set requires at least one variable assignment".to_string(),
            });
            return None;
        }

        for key in obj.keys() {
            let construct = format!("set.{key}");
            self.errors.push(ValidationError {
                span: Some(raw.span),
                kind: ValidationErrorKind::UnsupportedConstruct(construct.clone()),
                message: format!(
                    "unimplemented construct '{construct}': coord set binds a single variable; \
                     multi-key set is not representable"
                ),
            });
        }
        None
    }

    fn parse_raise(
        &mut self,
        _raw: &RawNamedTask,
        map: &serde_json::Map<String, Value>,
    ) -> Option<Task> {
        let raise_val = &map["raise"];
        let obj = raise_val.as_object();

        let error_def = if let Some(obj) = obj {
            ErrorDef {
                r#type: obj
                    .get("type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                title: obj
                    .get("title")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                status: obj.get("status").and_then(|v| v.as_u64()).map(|s| s as u16),
                detail: obj.get("detail").and_then(|v| v.as_str()).map(String::from),
            }
        } else {
            ErrorDef {
                r#type: String::new(),
                title: String::new(),
                status: None,
                detail: None,
            }
        };

        Some(Task::Raise(RaiseTask { raise: error_def }))
    }

    fn parse_try_catch(
        &mut self,
        raw: &RawNamedTask,
        map: &serde_json::Map<String, Value>,
    ) -> Option<Task> {
        let try_val = &map["try"];
        let try_tasks = self.parse_sub_tasks(try_val, raw.span);

        let catch_val = map.get("catch");
        let catch_clauses = if let Some(catch_val) = catch_val {
            if let Some(arr) = catch_val.as_array() {
                arr.iter()
                    .filter_map(|c| self.parse_catch_clause(c, raw.span))
                    .collect()
            } else {
                vec![CatchClause {
                    errors: None,
                    tasks: self.parse_sub_tasks(catch_val, raw.span),
                }]
            }
        } else {
            Vec::new()
        };

        Some(Task::TryCatch(TryCatchTask {
            r#try: try_tasks,
            catch: catch_clauses,
        }))
    }

    fn parse_catch_clause(&mut self, clause: &Value, span: Span) -> Option<CatchClause> {
        let obj = clause.as_object()?;
        let errors = obj.get("errors").and_then(|v| {
            v.as_array().map(|arr| {
                arr.iter()
                    .filter_map(|e| e.as_str().map(String::from))
                    .collect()
            })
        });

        let tasks = if let Some(do_val) = obj.get("do") {
            self.parse_sub_tasks(do_val, span)
        } else if let Some(tasks_val) = obj.get("tasks") {
            self.parse_sub_tasks(tasks_val, span)
        } else {
            Vec::new()
        };

        Some(CatchClause { errors, tasks })
    }

    fn parse_run(
        &mut self,
        raw: &RawNamedTask,
        map: &serde_json::Map<String, Value>,
    ) -> Option<Task> {
        let run_val = &map["run"];

        let (workflow, nested_input) = if let Some(s) = run_val.as_str() {
            // 简写: "namespace::name@version"
            (Self::parse_workflow_ref_short(s), None)
        } else if let Some(obj) = run_val.as_object() {
            // coord 只实现 `run.workflow`（子流程调用）。`run.script|container|shell`
            // 无执行面，`run.await|return` 无可表达的时序/输出语义；raw 值若原样
            // 落到 `run_subflow`（且 ref 为空）就是「接受但语义不同」的静默误读
            // （Spike 4 核心发现）。因此这些键一律在解析期显式拒绝。
            let named: Vec<String> = ["script", "container", "shell", "await", "return"]
                .iter()
                .filter(|k| obj.contains_key(**k))
                .map(|k| format!("run.{k}"))
                .collect();
            if !named.is_empty() {
                let construct = named.join(", ");
                self.errors.push(ValidationError {
                    span: Some(raw.span),
                    kind: ValidationErrorKind::UnsupportedConstruct(construct.clone()),
                    message: format!(
                        "unimplemented construct '{construct}': coord only supports 'run.workflow'"
                    ),
                });
                return None;
            }

            // 官方形状把引用嵌在 `workflow` 下（`run: {workflow: {namespace,name,version}}`）；
            // 旧方言把 namespace/name/version 直接放在 `run` 上。嵌套优先、缺失回退到
            // 扁平键，否则 schema-legal 文档会被静默编译成空 ref（Spike 4 误读）。
            let nested = obj.get("workflow").and_then(|v| v.as_object());
            let pick = |key: &str| -> String {
                nested
                    .and_then(|n| n.get(key))
                    .or_else(|| obj.get(key))
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string()
            };
            // 子流程入参嵌在 `run.workflow.input`：与 ref 同处嵌套层，不得只读任务级
            // `input` 而丢弃（Spike 4 误读）。schema 要求其为对象；present 但非对象
            // （含 null）fail-closed 点名，绝不静默忽略或遮蔽扁平回退。
            let nested_input = match nested.and_then(|n| n.get("input")) {
                None => None,
                Some(v) if v.is_object() => Some(v.clone()),
                Some(_) => {
                    self.errors.push(ValidationError {
                        span: Some(raw.span),
                        kind: ValidationErrorKind::UnsupportedConstruct(
                            "run.workflow.input".to_string(),
                        ),
                        message: "unimplemented construct 'run.workflow.input': subflow input \
                                  must be a JSON object"
                            .to_string(),
                    });
                    return None;
                }
            };
            (
                WorkflowRef {
                    namespace: pick("namespace"),
                    name: pick("name"),
                    version: pick("version"),
                },
                nested_input,
            )
        } else {
            self.errors.push(ValidationError {
                span: Some(raw.span),
                kind: ValidationErrorKind::UnsupportedConstruct("run".to_string()),
                message: "unimplemented construct 'run': coord only supports 'run.workflow'"
                    .to_string(),
            });
            return None;
        };

        // 有形状无目标（`run: {}` / `run: {workflow: {}}`）同样不许静默通过
        if workflow.name.is_empty() {
            self.errors.push(ValidationError {
                span: Some(raw.span),
                kind: ValidationErrorKind::MissingRequiredField("run.workflow.name".to_string()),
                message: "run.workflow requires a non-empty 'name' (subflow target)".to_string(),
            });
            return None;
        }

        // 子流程输入：官方嵌于 `run.workflow.input`（nested_input），旧方言放在任务级
        // `input`。嵌套优先、缺失回退扁平键，绝不静默丢弃。
        let input = nested_input.or_else(|| map.get("input").cloned());
        Some(Task::Run(RunTask { workflow, input }))
    }

    /// end 任务 —— 终端任务，无需字段（`end: true` / `end: {}` 均可）
    fn parse_end(
        &mut self,
        _raw: &RawNamedTask,
        _map: &serde_json::Map<String, Value>,
    ) -> Option<Task> {
        Some(Task::End(EndTask {}))
    }

    /// approve 任务 —— 原生审批节点。
    ///
    /// 键集合恰为 camelCase 字段名（approvers/pool/readers/roles/mode/veto/deadline/
    /// onPass/onReject/signAdmins/allowSelfApprove）；未知键 fail-closed 点名 `approve.<key>`，
    /// 不静默丢弃（拼错的 `allowSelfApprove` 会静默改变审批权）。
    fn parse_approve(
        &mut self,
        raw: &RawNamedTask,
        map: &serde_json::Map<String, Value>,
    ) -> Option<Task> {
        let body = match map.get("approve").and_then(|v| v.as_object()) {
            Some(obj) => obj,
            None => {
                self.errors.push(ValidationError {
                    span: Some(raw.span),
                    kind: ValidationErrorKind::UnsupportedConstruct("approve".to_string()),
                    message: "approve must be an object".to_string(),
                });
                return None;
            }
        };

        let known = [
            "approvers",
            "pool",
            "readers",
            "roles",
            "mode",
            "veto",
            "deadline",
            "onPass",
            "onReject",
            "signAdmins",
            "allowSelfApprove",
        ];
        let unknown: Vec<String> = body
            .keys()
            .filter(|k| !known.contains(&k.as_str()))
            .map(|k| format!("approve.{k}"))
            .collect();
        if !unknown.is_empty() {
            for construct in unknown {
                self.errors.push(ValidationError {
                    span: Some(raw.span),
                    kind: ValidationErrorKind::UnsupportedConstruct(construct.clone()),
                    message: format!(
                        "unimplemented construct '{construct}': unknown approve field"
                    ),
                });
            }
            return None;
        }

        let approvers =
            self.parse_approve_string_list(raw.span, "approve.approvers", body.get("approvers"));
        let pool = self.parse_approve_string_list(raw.span, "approve.pool", body.get("pool"));
        let readers =
            self.parse_approve_string_list(raw.span, "approve.readers", body.get("readers"));
        let roles = self.parse_approve_string_list(raw.span, "approve.roles", body.get("roles"));
        let sign_admins =
            self.parse_approve_string_list(raw.span, "approve.signAdmins", body.get("signAdmins"));

        // 四个席位来源全空：无从产生审批席位，静默接受会把节点变成 no-op。
        if approvers.is_empty() && pool.is_empty() && readers.is_empty() && roles.is_empty() {
            self.errors.push(ValidationError {
                span: Some(raw.span),
                kind: ValidationErrorKind::MissingRequiredField("approve seats".to_string()),
                message: "approve requires at least one of approvers/pool/readers/roles"
                    .to_string(),
            });
            return None;
        }

        let mode = match body.get("mode") {
            None => JoinPolicy::All,
            Some(Value::String(s)) => match parse_join_policy(s) {
                Some(policy) => policy,
                None => {
                    self.errors.push(ValidationError {
                        span: Some(raw.span),
                        kind: ValidationErrorKind::UnsupportedConstruct("approve.mode".to_string()),
                        message: format!(
                            "unimplemented construct 'approve.mode': expected \
                             all|any|quorum:<n>|ratio:<num>/<den>, got '{s}'"
                        ),
                    });
                    return None;
                }
            },
            Some(_) => {
                self.errors.push(ValidationError {
                    span: Some(raw.span),
                    kind: ValidationErrorKind::UnsupportedConstruct("approve.mode".to_string()),
                    message: "approve.mode must be a string".to_string(),
                });
                return None;
            }
        };

        let veto = match body.get("veto") {
            None => true,
            Some(Value::Bool(b)) => *b,
            Some(_) => {
                self.errors.push(ValidationError {
                    span: Some(raw.span),
                    kind: ValidationErrorKind::TypeMismatch("approve.veto".to_string()),
                    message: "approve.veto must be a boolean".to_string(),
                });
                return None;
            }
        };

        let allow_self_approve = match body.get("allowSelfApprove") {
            None => false,
            Some(Value::Bool(b)) => *b,
            Some(_) => {
                self.errors.push(ValidationError {
                    span: Some(raw.span),
                    kind: ValidationErrorKind::TypeMismatch("approve.allowSelfApprove".to_string()),
                    message: "approve.allowSelfApprove must be a boolean".to_string(),
                });
                return None;
            }
        };

        let deadline = match body.get("deadline") {
            None => None,
            Some(v) => {
                let dur = self.parse_duration_value(raw.span, v, "approve.deadline")?;
                // 解析期就必须可换算：否则运行期 `deadline_ms()==None`，
                // 无期限被当成默认（fail-open）。
                if approve_deadline_is_unsupported(&dur) {
                    self.errors.push(ValidationError {
                        span: Some(raw.span),
                        kind: ValidationErrorKind::UnsupportedConstruct(
                            "approve.deadline".to_string(),
                        ),
                        message: format!(
                            "approve.deadline must be an ISO8601 duration coord can evaluate, \
                             got '{dur}'"
                        ),
                    });
                    return None;
                }
                Some(dur)
            }
        };

        let on_pass = match body.get("onPass") {
            None => None,
            Some(Value::String(s)) => Some(s.clone()),
            Some(_) => {
                self.errors.push(ValidationError {
                    span: Some(raw.span),
                    kind: ValidationErrorKind::TypeMismatch("approve.onPass".to_string()),
                    message: "approve.onPass must be a task name string".to_string(),
                });
                return None;
            }
        };

        let on_reject = match body.get("onReject") {
            None => None,
            Some(Value::String(s)) => Some(s.clone()),
            Some(_) => {
                self.errors.push(ValidationError {
                    span: Some(raw.span),
                    kind: ValidationErrorKind::TypeMismatch("approve.onReject".to_string()),
                    message: "approve.onReject must be a task name string".to_string(),
                });
                return None;
            }
        };

        Some(Task::Approve(ApproveTask {
            approvers,
            pool,
            readers,
            roles,
            mode,
            veto,
            deadline,
            on_pass,
            on_reject,
            sign_admins,
            allow_self_approve,
        }))
    }

    /// approve 席位字段：缺省为空；在场则必须是非空字符串数组（非字符串项 fail-closed）。
    fn parse_approve_string_list(
        &mut self,
        span: Span,
        construct: &str,
        val: Option<&Value>,
    ) -> Vec<String> {
        match val {
            None => Vec::new(),
            Some(Value::Array(items)) => {
                let mut out = Vec::with_capacity(items.len());
                for item in items {
                    match item.as_str() {
                        Some(s) => out.push(s.to_string()),
                        None => self.errors.push(ValidationError {
                            span: Some(span),
                            kind: ValidationErrorKind::TypeMismatch(construct.to_string()),
                            message: format!("{construct} entries must be strings"),
                        }),
                    }
                }
                out
            }
            Some(_) => {
                self.errors.push(ValidationError {
                    span: Some(span),
                    kind: ValidationErrorKind::TypeMismatch(construct.to_string()),
                    message: format!("{construct} must be an array of strings"),
                });
                Vec::new()
            }
        }
    }

    fn parse_workflow_ref_short(s: &str) -> WorkflowRef {
        // 支持 "namespace::name@version" 格式
        let parts: Vec<&str> = s.split("::").collect();
        if parts.len() == 2 {
            let ns = parts[0];
            let name_ver: Vec<&str> = parts[1].split('@').collect();
            let name = name_ver.first().unwrap_or(&"");
            let version = name_ver.get(1).unwrap_or(&"1.0");
            WorkflowRef {
                namespace: ns.to_string(),
                name: name.to_string(),
                version: version.to_string(),
            }
        } else {
            WorkflowRef {
                namespace: String::new(),
                name: s.to_string(),
                version: "1.0".to_string(),
            }
        }
    }

    // ─── 辅助解析方法 ───

    /// 未实现的官方 `then` 流程指令（taskBase / flowDirective）fail-closed 拒绝。
    ///
    /// coord 的 document/do 方言按列表顺序执行（分支/跳转用 switch 的 `transition`
    /// 表达），`then` 一旦被忽略就会改变控制流，属「接受但语义不同」的静默误读：
    /// - `then: continue` 等价于顺序执行默认行为，接受；
    /// - `then: exit`/`end` 仅在**顶层 do 列表末尾**与工作流结束等价，接受；
    /// - 具名跳转（`then: <taskName>`）与嵌套位置（for.do / try.do / catch.do / do）
    ///   一律显式拒绝，不猜测目标。
    fn check_then_directive(&mut self, body: &Value, top_level_last: bool, span: Span) {
        let Some(obj) = body.as_object() else {
            return;
        };
        let Some(then) = obj.get("then") else {
            return;
        };
        let accepted = match then {
            Value::String(d) if d == "continue" => true,
            Value::String(d) if d == "exit" || d == "end" => top_level_last,
            _ => false,
        };
        if !accepted {
            self.errors.push(ValidationError {
                span: Some(span),
                kind: ValidationErrorKind::UnsupportedConstruct("then".to_string()),
                message: format!(
                    "unimplemented construct 'then': coord executes the do list in order and \
                     expresses branching with switch 'transition' (got {then})"
                ),
            });
        }
    }

    /// 解析子任务列表（do/try/catch 中的嵌套任务）
    fn parse_sub_tasks(&mut self, val: &Value, parent_span: Span) -> Vec<NamedTask> {
        match val {
            Value::Array(arr) => arr
                .iter()
                .filter_map(|item| {
                    let obj = item.as_object()?;
                    if obj.len() != 1 {
                        self.errors.push(ValidationError {
                            span: Some(parent_span),
                            kind: ValidationErrorKind::TypeMismatch(
                                "sub-task key count".to_string(),
                            ),
                            message: "sub-task must have exactly one key".to_string(),
                        });
                        return None;
                    }
                    let (name, body) = obj.iter().next()?;
                    let raw = RawNamedTask {
                        span: parent_span,
                        name: name.clone(),
                        body: body.clone(),
                    };
                    // 嵌套任务不在顶层 do 列表末尾：`then: exit|end` 在此处已被忽略
                    self.check_then_directive(&raw.body, false, raw.span);
                    self.resolve_task(&raw).map(|task| NamedTask {
                        name: name.clone(),
                        task,
                    })
                })
                .collect(),
            _ => Vec::new(),
        }
    }

    // ─── 引用校验 ───

    /// 校验 switch transition 目标存在性
    fn check_task_refs(&mut self, tasks: &[NamedTask]) {
        for task in tasks {
            match &task.task {
                Task::Switch(switch) => {
                    // 每个条件（含 condition=None 的 legacy defaultCondition）与
                    // default_condition 都是运行期 Goto 目标，必须存在于任务集合；
                    // 官方 default case 使该洞首次可达（此前官方形状整体被拒）。
                    let targets = switch
                        .conditions
                        .iter()
                        .map(|c| &c.transition)
                        .chain(switch.default_condition.iter().map(|c| &c.transition));
                    for target in targets {
                        if !self.task_names.contains(target) {
                            self.errors.push(ValidationError {
                                span: None,
                                kind: ValidationErrorKind::UnknownTaskReference(target.clone()),
                                message: format!(
                                    "switch in '{}' references unknown task '{}'",
                                    task.name, target
                                ),
                            });
                        }
                    }
                }
                Task::Do(do_task) => {
                    self.check_task_refs(&do_task.tasks);
                }
                Task::ForEach(fe) => {
                    self.check_task_refs(&fe.tasks);
                }
                Task::TryCatch(tc) => {
                    self.check_task_refs(&tc.r#try);
                    for clause in &tc.catch {
                        self.check_task_refs(&clause.tasks);
                    }
                }
                Task::Fork(fork) => {
                    for branch in &fork.branches {
                        self.check_task_refs(&branch.tasks);
                    }
                }
                // onPass/onReject 是运行期的分支目标，必须存在于任务集合。
                Task::Approve(approve) => {
                    for target in approve.on_pass.iter().chain(approve.on_reject.iter()) {
                        if !self.task_names.contains(target) {
                            self.errors.push(ValidationError {
                                span: None,
                                kind: ValidationErrorKind::UnknownTaskReference(target.clone()),
                                message: format!(
                                    "approve in '{}' references unknown task '{}'",
                                    task.name, target
                                ),
                            });
                        }
                    }
                }
                _ => {}
            }
        }
    }

    /// 校验 use.functions 引用完整性
    fn check_function_refs(&mut self, tasks: &[NamedTask]) {
        for task in tasks {
            match &task.task {
                Task::Call(call) => {
                    if let CallType::Function(name) = &call.call {
                        if !name.is_empty() && !self.function_names.contains(name) {
                            self.errors.push(ValidationError {
                                span: None,
                                kind: ValidationErrorKind::UnknownFunctionReference(name.clone()),
                                message: format!(
                                    "task '{}' references unknown function '{}'",
                                    task.name, name
                                ),
                            });
                        }
                    }
                }
                Task::Do(do_task) => {
                    self.check_function_refs(&do_task.tasks);
                }
                Task::ForEach(fe) => {
                    self.check_function_refs(&fe.tasks);
                }
                Task::TryCatch(tc) => {
                    self.check_function_refs(&tc.r#try);
                    for clause in &tc.catch {
                        self.check_function_refs(&clause.tasks);
                    }
                }
                Task::Fork(fork) => {
                    for branch in &fork.branches {
                        self.check_function_refs(&branch.tasks);
                    }
                }
                _ => {}
            }
        }
    }

    /// 循环依赖检测：switch transition 不能形成环
    fn check_cycles(&mut self, tasks: &[NamedTask]) {
        // 构建 transition 图。`condition` 为 None 的 legacy defaultCondition 与官方 when-less
        // case 提升出的 `default_condition` 同样是运行期 Goto 目标，必须计入：运行期无步数预算，
        // 自环会无限循环。approve 的 onPass/onReject 也是分支目标，同纳入。本图只含顶层
        // switch / approve 节点（Goto 目标只解析顶层任务，runtime `find_task_index`）；经由嵌套
        // switch 绕回顶层任务的环超出本检查范围（既有缺口，未扩大）。
        let mut edges: HashMap<&str, Vec<&str>> = HashMap::new();
        for task in tasks {
            match &task.task {
                Task::Switch(switch) => {
                    let targets = switch
                        .conditions
                        .iter()
                        .map(|c| c.transition.as_str())
                        .chain(
                            switch
                                .default_condition
                                .iter()
                                .map(|c| c.transition.as_str()),
                        );
                    edges.entry(task.name.as_str()).or_default().extend(targets);
                }
                Task::Approve(approve) => {
                    let targets = approve
                        .on_pass
                        .iter()
                        .map(String::as_str)
                        .chain(approve.on_reject.iter().map(String::as_str));
                    edges.entry(task.name.as_str()).or_default().extend(targets);
                }
                _ => {}
            }
        }

        // DFS 检测环
        let mut visited: HashSet<&str> = HashSet::new();
        let mut in_stack: HashSet<&str> = HashSet::new();
        let mut path: Vec<&str> = Vec::new();

        for &node in edges.keys() {
            if !visited.contains(node) {
                if let Some(cycle) =
                    Self::detect_cycle(node, &edges, &mut visited, &mut in_stack, &mut path)
                {
                    self.errors.push(ValidationError {
                        span: None,
                        kind: ValidationErrorKind::CyclicDependency(
                            cycle.iter().map(|s| s.to_string()).collect(),
                        ),
                        message: "cyclic dependency detected in switch transitions".to_string(),
                    });
                    return; // 检测到一个环就停止
                }
            }
        }
    }

    fn detect_cycle<'a>(
        node: &'a str,
        edges: &HashMap<&'a str, Vec<&'a str>>,
        visited: &mut HashSet<&'a str>,
        in_stack: &mut HashSet<&'a str>,
        path: &mut Vec<&'a str>,
    ) -> Option<Vec<&'a str>> {
        visited.insert(node);
        in_stack.insert(node);
        path.push(node);

        if let Some(neighbors) = edges.get(node) {
            for &neighbor in neighbors {
                if !visited.contains(neighbor) {
                    if let Some(cycle) =
                        Self::detect_cycle(neighbor, edges, visited, in_stack, path)
                    {
                        return Some(cycle);
                    }
                } else if in_stack.contains(neighbor) {
                    // 找到环（neighbor 入栈即入 path，position 必命中）
                    if let Some(cycle_start) = path.iter().position(|&x| x == neighbor) {
                        let cycle: Vec<&str> = path[cycle_start..].to_vec();
                        return Some(cycle);
                    }
                }
            }
        }

        path.pop();
        in_stack.remove(node);
        None
    }

    // ─── UseComponents 解析 ───

    fn resolve_use_components(&mut self, raw: RawUseComponents) -> UseComponents {
        let functions = raw.functions.map(|funcs| {
            funcs
                .into_iter()
                .map(|(name, raw_func)| {
                    let func_def = self.resolve_function_def(raw_func);
                    (name, func_def)
                })
                .collect()
        });

        let retries = raw.retries.map(|rets| {
            rets.into_iter()
                .map(|(name, raw_retry)| {
                    let policy = self.resolve_retry_policy(raw_retry);
                    (name, policy)
                })
                .collect()
        });

        let timeouts = raw.timeouts.map(|tos| {
            tos.into_iter()
                .map(|(name, raw_to)| {
                    let config = self.resolve_timeout_config(raw_to);
                    (name, config)
                })
                .collect()
        });

        UseComponents {
            functions,
            retries,
            timeouts,
        }
    }

    // ─── 任务级标准字段解析（if / input / output / export / retry / timeout） ───

    /// 解析任务级标准字段（`if` / `input` / `output` / `export` / `retry` / `timeout`）
    ///
    /// `retry` 支持内联策略对象或字符串引用（`use.retries` 名称）。
    fn parse_task_meta(&mut self, raw: &RawNamedTask) -> TaskMeta {
        let body = match &raw.body {
            Value::Object(m) => m,
            _ => return TaskMeta::default(),
        };
        TaskMeta {
            if_condition: body.get("if").and_then(|v| v.as_str()).map(String::from),
            input: body.get("input").map(|v| InputConfig {
                schema: v.get("schema").and_then(|v| v.as_str()).map(String::from),
                from: v.get("from").and_then(|v| v.as_str()).map(String::from),
                default: v.get("default").cloned(),
            }),
            output: body.get("output").map(|v| OutputConfig {
                as_expr: v.get("as").and_then(|v| v.as_str()).map(String::from),
                schema: v.get("schema").and_then(|v| v.as_str()).map(String::from),
            }),
            export: body.get("export").map(|v| ExportConfig {
                as_expr: v.get("as").and_then(|v| v.as_str()).map(String::from),
                schema: v.get("schema").and_then(|v| v.as_str()).map(String::from),
            }),
            retry: body.get("retry").and_then(|v| match v {
                Value::String(name) => self.retry_refs.get(name).cloned(),
                _ => parse_retry_policy_value(v),
            }),
            timeout: body.get("timeout").and_then(parse_timeout_value),
        }
    }

    /// 递归收集任务级标准字段（顶层 + do/fork/for-each/try-catch 嵌套）
    fn collect_task_meta(&mut self, raws: &[RawNamedTask], map: &mut HashMap<String, TaskMeta>) {
        for raw in raws {
            let meta = self.parse_task_meta(raw);
            if !meta.is_empty() {
                map.insert(raw.name.clone(), meta);
            }
            let obj = match &raw.body {
                Value::Object(o) => o,
                _ => continue,
            };
            // do / try 子任务
            for key in ["do", "try"] {
                if let Some(arr) = obj.get(key).and_then(|v| v.as_array()) {
                    let nested = extract_raw_sub_tasks(arr);
                    self.collect_task_meta(&nested, map);
                }
            }
            // catch 子句
            if let Some(catch_val) = obj.get("catch") {
                let clauses = catch_val
                    .as_array()
                    .map(|a| a.to_vec())
                    .unwrap_or_else(|| vec![catch_val.clone()]);
                for c in &clauses {
                    if let Some(arr) = c.get("do").and_then(|v| v.as_array()) {
                        let nested = extract_raw_sub_tasks(arr);
                        self.collect_task_meta(&nested, map);
                    }
                }
            }
            // fork 分支
            if let Some(fork) = obj.get("fork") {
                let branches = fork
                    .as_array()
                    .or_else(|| fork.get("branches").and_then(|v| v.as_array()));
                if let Some(branches) = branches {
                    for b in branches {
                        if let Some(bobj) = b.as_object() {
                            // 官方单键 map 形状 `{<branch_name>: <task>}`：整个分支项就是一个
                            // 具名任务，其标准字段（if/input/…）必须按该分支名收集。
                            if !bobj.contains_key("name") && bobj.len() == 1 {
                                let nested = extract_raw_sub_tasks(std::slice::from_ref(b));
                                self.collect_task_meta(&nested, map);
                                continue;
                            }
                        }
                        let tasks = b
                            .get("tasks")
                            .and_then(|v| v.as_array())
                            .or_else(|| b.get("do").and_then(|v| v.as_array()));
                        if let Some(arr) = tasks {
                            let nested = extract_raw_sub_tasks(arr);
                            self.collect_task_meta(&nested, map);
                        }
                    }
                }
            }
            // for-each 子任务
            if let Some(for_val) = obj.get("for") {
                let tasks = for_val
                    .get("tasks")
                    .and_then(|v| v.as_array())
                    .or_else(|| for_val.get("do").and_then(|v| v.as_array()));
                if let Some(arr) = tasks {
                    let nested = extract_raw_sub_tasks(arr);
                    self.collect_task_meta(&nested, map);
                }
            }
        }
    }

    /// 解析顶层 auth 定义
    fn parse_auth_config(&mut self, cfg: &Value) -> AuthConfig {
        let obj = cfg.as_object().cloned().unwrap_or_default();
        let get = |k: &str| obj.get(k).and_then(|v| v.as_str()).map(String::from);
        AuthConfig {
            scheme: get("scheme").or_else(|| get("type")),
            username: get("username").or_else(|| get("user")),
            password: get("password"),
            token: get("token"),
            token_url: get("tokenUrl").or_else(|| get("token_url")),
            client_id: get("clientId").or_else(|| get("client_id")),
            client_secret: get("clientSecret").or_else(|| get("client_secret")),
            scope: get("scope"),
        }
    }

    fn resolve_function_def(&mut self, raw: RawFunctionDef) -> FunctionDef {
        let body = &raw.body;
        if let Some(obj) = body.as_object() {
            let call_type = if obj.contains_key("call") {
                match &obj["call"] {
                    Value::String(s) if s == "http" => CallType::Http,
                    Value::String(s) if s == "grpc" => CallType::Grpc,
                    Value::String(s) => CallType::Function(s.clone()),
                    _ => CallType::Http,
                }
            } else {
                CallType::Http
            };
            FunctionDef {
                call: call_type,
                with: obj.get("with").cloned(),
            }
        } else {
            FunctionDef {
                call: CallType::Http,
                with: None,
            }
        }
    }

    fn resolve_retry_policy(&mut self, raw: RawRetryPolicy) -> RetryPolicy {
        let body = &raw.body;
        if let Some(obj) = body.as_object() {
            RetryPolicy {
                delay: obj
                    .get("delay")
                    .and_then(|v| v.as_str())
                    .unwrap_or("PT3S")
                    .to_string(),
                backoff: obj
                    .get("backoff")
                    .and_then(|v| v.as_str())
                    .map(|s| match s {
                        "constant" => BackoffStrategy::Constant,
                        "linear" => BackoffStrategy::Linear,
                        "exponential" => BackoffStrategy::Exponential,
                        _ => BackoffStrategy::Constant,
                    }),
                limit: obj.get("limit").and_then(|v| v.as_u64()).unwrap_or(3) as u32,
                jitter: obj.get("jitter").and_then(|v| {
                    v.as_object().map(|j| JitterConfig {
                        factor: j.get("factor").and_then(|f| f.as_f64()).unwrap_or(0.1),
                    })
                }),
            }
        } else {
            RetryPolicy {
                delay: "PT3S".to_string(),
                backoff: None,
                limit: 3,
                jitter: None,
            }
        }
    }

    fn resolve_timeout_config(&mut self, raw: RawTimeoutConfig) -> TimeoutConfig {
        let body = &raw.body;
        if let Some(obj) = body.as_object() {
            TimeoutConfig {
                after: obj
                    .get("after")
                    .and_then(|v| v.as_str())
                    .unwrap_or("P7D")
                    .to_string(),
            }
        } else {
            TimeoutConfig {
                after: "P7D".to_string(),
            }
        }
    }
}

/// 官方 1.0.3 `set` map 的 value → jq 表达式字符串。
///
/// 陷阱：官方 `<var>` 值既可能是 JSON 字面量，也可能是运行时表达式 `"${ ... }"`。
/// 若把字符串字面量直接当裸标识符传下去，`set.rs` 会当作字段访问；故 JSON 字面量一律
/// 序列化（`"hi"` → `"\"hi\""`），确保求值结果等于字面量本身。`${ ... }` 由
/// `ExpressionEvaluator::unwrap_expression` 负责剥壳，原样透传即可。
fn official_set_value_expr(value: &Value) -> String {
    match value {
        Value::String(s) if s.trim().starts_with("${") && s.trim().ends_with('}') => s.clone(),
        other => crate::workflow::sw::json_to_jq_literal(other),
    }
}

/// 官方 `set` 值是否含数组字面量（含对象字面量内嵌）。
///
/// `eval_jq_subset` 无 `[...]` 数组字面量分支，数组表达式会落到 `Ok(context.clone())`，
/// 即把整个上下文写成变量值（Spike 4 式的静默误读）。数组字面量支持前解析期点名拒绝。
fn set_value_contains_array(value: &Value) -> bool {
    match value {
        Value::Array(_) => true,
        Value::Object(map) => map.values().any(set_value_contains_array),
        _ => false,
    }
}

// ─── 便捷入口 ───

/// 一步完成解析 + 校验：YAML → WorkflowDefinition
pub fn parse_and_validate_yaml(yaml: &str) -> Result<WorkflowDefinition, Vec<ValidationError>> {
    let raw = super::parser::parse_yaml(yaml).map_err(|e| {
        vec![ValidationError {
            span: e.span,
            kind: ValidationErrorKind::SyntaxError(e.message.clone()),
            message: e.message,
        }]
    })?;
    Validator::validate(raw)
}

/// 一步完成解析 + 校验：JSON → WorkflowDefinition
pub fn parse_and_validate_json(json: &str) -> Result<WorkflowDefinition, Vec<ValidationError>> {
    let raw = super::parser::parse_json(json).map_err(|e| {
        vec![ValidationError {
            span: e.span,
            kind: ValidationErrorKind::SyntaxError(e.message.clone()),
            message: e.message,
        }]
    })?;
    Validator::validate(raw)
}

// ─── 模块级辅助：灵活 retry / timeout 值解析 ───

/// 从子任务数组提取 RawNamedTask 列表（用于元数据收集）
fn extract_raw_sub_tasks(arr: &[Value]) -> Vec<RawNamedTask> {
    arr.iter()
        .filter_map(|item| {
            let obj = item.as_object()?;
            if obj.len() != 1 {
                return None;
            }
            let (name, body) = obj.iter().next()?;
            Some(RawNamedTask {
                span: Span::new(0, 0, 0),
                name: name.clone(),
                body: body.clone(),
            })
        })
        .collect()
}

/// 解析 retry 策略值，兼容两种 DSL 形状：
/// - 简单形状: `{ delay: "PT3S", backoff: "exponential", limit: 5, jitter: {factor: 0.1} }`
/// - DSL 形状: `{ delay: { seconds: 3 }, backoff: { linear: {} }, limit: { attempt: { count: 5 } } }`
pub fn parse_retry_policy_value(v: &Value) -> Option<RetryPolicy> {
    let obj = v.as_object()?;
    let mut policy = RetryPolicy {
        delay: "PT3S".to_string(),
        backoff: None,
        limit: 3,
        jitter: None,
    };

    // delay: "PT3S" | { seconds: 3 } | { milliseconds: 3000 }
    if let Some(d) = obj.get("delay") {
        if let Some(s) = d.as_str() {
            policy.delay = s.to_string();
        } else if let Some(do_obj) = d.as_object() {
            if let Some(secs) = do_obj.get("seconds").and_then(|x| x.as_u64()) {
                policy.delay = format!("PT{secs}S");
            } else if let Some(ms) = do_obj.get("milliseconds").and_then(|x| x.as_u64()) {
                policy.delay = format!("PT{}S", ms as f64 / 1000.0);
            }
        }
    }

    // backoff: "linear" | { linear: {} } | { exponential: {} } | { constant: {} }
    if let Some(b) = obj.get("backoff") {
        if let Some(s) = b.as_str() {
            policy.backoff = match s {
                "constant" => Some(BackoffStrategy::Constant),
                "linear" => Some(BackoffStrategy::Linear),
                "exponential" => Some(BackoffStrategy::Exponential),
                _ => None,
            };
        } else if let Some(bo) = b.as_object() {
            if bo.contains_key("constant") {
                policy.backoff = Some(BackoffStrategy::Constant);
            } else if bo.contains_key("linear") {
                policy.backoff = Some(BackoffStrategy::Linear);
            } else if bo.contains_key("exponential") {
                policy.backoff = Some(BackoffStrategy::Exponential);
            }
        }
    }

    // limit: 5 | { attempt: { count: 5 } }
    if let Some(l) = obj.get("limit") {
        if let Some(n) = l.as_u64() {
            policy.limit = n as u32;
        } else if let Some(lo) = l.as_object() {
            if let Some(attempt) = lo.get("attempt") {
                if let Some(count) = attempt.get("count").and_then(|x| x.as_u64()) {
                    policy.limit = count as u32;
                }
            }
        }
    }

    // jitter: { factor: 0.1 }
    if let Some(j) = obj.get("jitter") {
        if let Some(f) = j.get("factor").and_then(|x| x.as_f64()) {
            policy.jitter = Some(JitterConfig { factor: f });
        }
    }

    Some(policy)
}

/// 解析 timeout 值：`{ after: "PT5M" }` 或 `{ after: { seconds: 300 } }`
pub fn parse_timeout_value(v: &Value) -> Option<TimeoutConfig> {
    let obj = v.as_object()?;
    if let Some(a) = obj.get("after") {
        if let Some(s) = a.as_str() {
            return Some(TimeoutConfig {
                after: s.to_string(),
            });
        }
        if let Some(ao) = a.as_object() {
            if let Some(secs) = ao.get("seconds").and_then(|x| x.as_u64()) {
                return Some(TimeoutConfig {
                    after: format!("PT{secs}S"),
                });
            }
        }
    }
    None
}

// ─── 测试 ───

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::expression::ExpressionEvaluator;

    // ─── 完整解析 + 校验流程测试 ───

    #[test]
    fn test_validate_minimal_workflow() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: minimal
  version: "1.0"
do:
  - step1:
      call: http
      with:
        method: GET
        endpoint: "https://example.com"
"#;

        let result = parse_and_validate_yaml(yaml);
        assert!(result.is_ok(), "validation failed: {:?}", result.err());
        let def = result.unwrap();
        assert_eq!(def.document.name, "minimal");
        assert_eq!(def.do_tasks.len(), 1);
        assert_eq!(def.do_tasks[0].name, "step1");
        match &def.do_tasks[0].task {
            Task::Call(c) => {
                assert_eq!(c.call, CallType::Http);
                assert!(c.with.is_some());
            }
            _ => panic!("expected Call task"),
        }
    }

    #[test]
    fn test_validate_workflow_with_switch() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: icps
  name: approval
  version: "1.0"
do:
  - checkAmount:
      switch:
        - condition: "${ .amount > 10000 }"
          transition: seniorApproval
        - condition: "${ .amount > 5000 }"
          transition: managerApproval
        - defaultCondition: directorApproval
  - managerApproval:
      call: http
      with:
        method: POST
        endpoint: "https://api/approve"
  - seniorApproval:
      call: http
      with:
        method: POST
        endpoint: "https://api/senior-approve"
  - directorApproval:
      call: http
      with:
        method: POST
        endpoint: "https://api/director-approve"
"#;

        let result = parse_and_validate_yaml(yaml);
        assert!(result.is_ok(), "validation failed: {:?}", result.err());
        let def = result.unwrap();
        assert_eq!(def.do_tasks.len(), 4);

        // 验证 switch 任务
        match &def.do_tasks[0].task {
            Task::Switch(s) => {
                assert_eq!(s.conditions.len(), 3);
                // 第一个条件
                assert_eq!(
                    s.conditions[0].condition.as_deref(),
                    Some("${ .amount > 10000 }")
                );
                assert_eq!(s.conditions[0].transition, "seniorApproval");
                // defaultCondition（第三个条件，无 condition 字段）
                assert_eq!(s.conditions[2].condition, None);
                assert_eq!(s.conditions[2].transition, "directorApproval");
            }
            _ => panic!("expected Switch task"),
        }
    }

    #[test]
    fn test_switch_official_when_then_shape() {
        // 官方 1.0.3 case 形状 `{<case>: {when, then}}`：`when` → condition、`then` → transition；
        // `then: end|exit` 映射到终端任务 `__end`（Spike 4 corpus 04）。逐字断言，不得静默丢字段。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: official-switch
  version: "1.0"
do:
  - route:
      switch:
        - high:
            when: "${ .counter > 10 }"
            then: exit
        - low:
            when: "${ .counter <= 10 }"
            then: next
  - next:
      call: http
      with:
        method: GET
        endpoint: "https://api/next"
"#;

        let def = parse_and_validate_yaml(yaml).expect("official switch must parse");
        let s = match &def.do_tasks[0].task {
            Task::Switch(s) => s,
            other => panic!("expected Switch task, got {other:?}"),
        };
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
        assert_eq!(s.conditions[1].transition, "next");
        assert!(s.default_condition.is_none());
        // `then: exit` 的终端引用必须可解析：定义中补出 `__end` 终端任务。
        assert!(
            def.do_tasks
                .iter()
                .any(|t| t.name == "__end" && matches!(t.task, Task::End(_))),
            "exit must map to a synthesized __end terminal task: {:?}",
            def.do_tasks.iter().map(|t| &t.name).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_switch_default_case_maps_to_default_condition() {
        // 官方 case 的 `when` 可选：无 `when` 的 case（含 `default`）映射到 default_condition，
        // 不得当作「缺 when」拒绝，也不得产出无名分支。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: official-switch-default
  version: "1.0"
do:
  - route:
      switch:
        - high:
            when: "${ .counter > 10 }"
            then: approved
        - default:
            then: exit
  - approved:
      call: http
      with:
        method: GET
        endpoint: "https://api/approved"
"#;

        let def = parse_and_validate_yaml(yaml).expect("official default case must parse");
        let s = match &def.do_tasks[0].task {
            Task::Switch(s) => s,
            other => panic!("expected Switch task, got {other:?}"),
        };
        assert_eq!(s.conditions.len(), 1);
        assert_eq!(
            s.conditions[0].condition.as_deref(),
            Some("${ .counter > 10 }")
        );
        assert_eq!(s.conditions[0].transition, "approved");
        let default = s.default_condition.as_ref().expect("default case mapped");
        assert_eq!(default.condition, None);
        assert_eq!(default.transition, "__end");
    }

    #[test]
    fn test_switch_multiple_default_cases_rejected() {
        // 多个无 `when` case：default 唯一，fail-closed 点名冲突项 `switch.<case>.when`。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: official-switch-multi-default
  version: "1.0"
do:
  - route:
      switch:
        - first:
            when: "${ .counter > 10 }"
            then: exit
        - a:
            then: exit
        - b:
            then: exit
"#;

        let errs = parse_and_validate_yaml(yaml).expect_err("multiple default cases must reject");
        assert!(
            error_kind_naming(&errs, "switch.b.when"),
            "errors: {errs:?}"
        );
    }

    #[test]
    fn test_switch_case_missing_then_rejected() {
        // 官方 case 缺 `then`：fail-closed 点名 `switch.<case>.then`。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: official-switch-missing-then
  version: "1.0"
do:
  - route:
      switch:
        - high:
            when: "${ .counter > 10 }"
"#;

        let errs = parse_and_validate_yaml(yaml).expect_err("missing switch.high.then must reject");
        assert!(
            error_kind_naming(&errs, "switch.high.then"),
            "errors: {errs:?}"
        );
    }

    #[test]
    fn test_switch_case_continue_rejected() {
        // `then: continue`（do 列表 fall-through）在不改执行器的前提下无法表达：
        // fail-closed 拒绝并点名 `switch.<case>.then`，不得静默误判为下一任务。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: official-switch-continue
  version: "1.0"
do:
  - route:
      switch:
        - high:
            when: "${ .counter > 10 }"
            then: continue
"#;

        let errs = parse_and_validate_yaml(yaml).expect_err("then: continue must reject");
        assert!(
            error_kind_naming(&errs, "switch.high.then"),
            "errors: {errs:?}"
        );
    }

    #[test]
    fn test_switch_default_condition_unknown_target_rejected() {
        // default 分支同样是运行期 Goto 目标：官方 default case 使其首次可达，
        // 未知目标必须 fail-closed（此前官方形状整体被拒，该洞不可达）。
        let official = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: official-switch-default-typo
  version: "1.0"
do:
  - route:
      switch:
        - default:
            then: typoTask
"#;
        let legacy = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: legacy-switch-default-typo
  version: "1.0"
do:
  - route:
      switch:
        - defaultCondition: typoTask
"#;
        for yaml in [official, legacy] {
            let errs =
                parse_and_validate_yaml(yaml).expect_err("unknown default target must reject");
            assert!(
                errs.iter().any(|e| matches!(
                    &e.kind,
                    ValidationErrorKind::UnknownTaskReference(t) if t == "typoTask"
                )),
                "errors: {errs:?}"
            );
        }
    }

    #[test]
    fn test_switch_reserved_end_non_terminal_rejected() {
        // `__end` 是 switch 终端保留名：用户定义的非终端 `__end` 不得被 `then: exit` 当作目标，
        // 否则 `exit` 会落到普通任务并 fall-through。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: reserved-end-non-terminal
  version: "1.0"
do:
  - route:
      switch:
        - high:
            when: "${ .counter > 10 }"
            then: exit
  - __end:
      set:
        variable: result
        value: "\"done\""
"#;
        let errs = parse_and_validate_yaml(yaml).expect_err("non-terminal __end must reject");
        assert!(error_kind_naming(&errs, "__end"), "errors: {errs:?}");
    }

    #[test]
    fn test_switch_user_defined_end_terminal_reused() {
        // 用户已定义 `__end` 终端（end: true）时复用之，不得再补一个重复任务。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: user-end-terminal
  version: "1.0"
do:
  - route:
      switch:
        - high:
            when: "${ .counter > 10 }"
            then: exit
  - __end:
      end: true
"#;
        let def = parse_and_validate_yaml(yaml).expect("user __end terminal must be reused");
        assert_eq!(def.do_tasks.iter().filter(|t| t.name == "__end").count(), 1);
        assert!(def
            .do_tasks
            .iter()
            .any(|t| t.name == "__end" && matches!(&t.task, Task::End(_))));
    }

    #[test]
    fn test_switch_case_named_condition_maps() {
        // case 名恰为 legacy 键名（condition/transition/defaultCondition）时，
        // 按字段集（when/then）判定为官方形状，不得误入 legacy 路径报「缺 transition」。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: official-switch-keyword-case
  version: "1.0"
do:
  - route:
      switch:
        - condition:
            when: "${ .counter > 10 }"
            then: exit
"#;
        let def = parse_and_validate_yaml(yaml).expect("case named `condition` must map");
        match &def.do_tasks[0].task {
            Task::Switch(s) => {
                assert_eq!(s.conditions.len(), 1);
                assert_eq!(
                    s.conditions[0].condition.as_deref(),
                    Some("${ .counter > 10 }")
                );
                assert_eq!(s.conditions[0].transition, "__end");
            }
            other => panic!("expected Switch task, got {other:?}"),
        }
    }

    #[test]
    fn test_switch_default_case_self_reference_rejected() {
        // 官方 when-less case（default）映射到 default_condition，同样是运行期 Goto 目标：
        // 自环会让运行期无限循环（运行期无步数预算），必须计入环检测并 fail-closed 拒绝。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: switch-default-self-loop
  version: "1.0"
do:
  - route:
      switch:
        - default:
            then: route
"#;
        let errs = parse_and_validate_yaml(yaml).expect_err("self-referential default must reject");
        assert!(
            errs.iter()
                .any(|e| matches!(e.kind, ValidationErrorKind::CyclicDependency(_))),
            "errors: {errs:?}"
        );
    }

    #[test]
    fn test_switch_non_object_item_rejected() {
        // `switch: ["foo"]` 非对象项：legacy 路径静默丢弃 → fail-closed 点名 `switch[0]`。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: switch-non-object-item
  version: "1.0"
do:
  - route:
      switch:
        - "foo"
"#;
        let errs = parse_and_validate_yaml(yaml).expect_err("non-object switch item must reject");
        assert!(error_kind_naming(&errs, "switch[0]"), "errors: {errs:?}");
    }

    #[test]
    fn test_switch_case_missing_when_and_then_rejected() {
        // 官方单键 case 体缺 `when`/`then`（`{high: {}}`）：曾回退 legacy 报「缺 transition」，
        // 现 fail-closed 点名 `switch.high.then`。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: switch-case-empty-body
  version: "1.0"
do:
  - route:
      switch:
        - high: {}
"#;
        let errs = parse_and_validate_yaml(yaml).expect_err("empty official case body must reject");
        assert!(
            error_kind_naming(&errs, "switch.high.then"),
            "errors: {errs:?}"
        );
    }

    #[test]
    fn test_validate_workflow_with_function_ref() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: func-test
  version: "1.0"
use:
  functions:
    sendNotification:
      call: http
      with:
        method: POST
        endpoint: "https://notify/api"
do:
  - notify:
      call: function
      with:
        function: sendNotification
"#;

        let result = parse_and_validate_yaml(yaml);
        assert!(result.is_ok(), "validation failed: {:?}", result.err());
        let def = result.unwrap();
        let use_comp = def.use_components.as_ref().unwrap();
        assert!(use_comp
            .functions
            .as_ref()
            .unwrap()
            .contains_key("sendNotification"));
    }

    #[test]
    fn test_validate_rejects_unknown_function_ref() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: bad-ref
  version: "1.0"
do:
  - notify:
      call: function
      with:
        function: nonExistentFunc
"#;

        let result = parse_and_validate_yaml(yaml);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors
            .iter()
            .any(|e| matches!(e.kind, ValidationErrorKind::UnknownFunctionReference(_))));
    }

    #[test]
    fn test_validate_rejects_unknown_switch_target() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: bad-switch
  version: "1.0"
do:
  - check:
      switch:
        - condition: "${ .x > 0 }"
          transition: nonExistentTask
  - step1:
      call: http
"#;

        let result = parse_and_validate_yaml(yaml);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors
            .iter()
            .any(|e| matches!(e.kind, ValidationErrorKind::UnknownTaskReference(_))));
    }

    #[test]
    fn test_validate_workflow_with_multiple_task_types() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: multi-type
  version: "1.0"
do:
  - callService:
      call: http
      with:
        method: POST
        endpoint: "https://api/service"
  - waitApproval:
      wait: PT1H
  - conditionalBranch:
      switch:
        - condition: "${ .approved }"
          transition: finalStep
        - defaultCondition: waitApproval
  - finalStep:
      call: http
      with:
        method: GET
        endpoint: "https://api/done"
"#;

        let result = parse_and_validate_yaml(yaml);
        assert!(result.is_ok(), "validation failed: {:?}", result.err());
        let def = result.unwrap();
        assert_eq!(def.do_tasks.len(), 4);

        // 验证各任务类型
        assert!(matches!(def.do_tasks[0].task, Task::Call(_)));
        assert!(matches!(def.do_tasks[1].task, Task::Wait(_)));
        assert!(matches!(def.do_tasks[2].task, Task::Switch(_)));
        assert!(matches!(def.do_tasks[3].task, Task::Call(_)));
    }

    #[test]
    fn test_validate_end_task() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: end-example
  version: "1.0"
do:
  - process:
      call: http
  - end:
      end: true
"#;

        let result = parse_and_validate_yaml(yaml);
        assert!(result.is_ok(), "validation failed: {:?}", result.err());
        let def = result.unwrap();
        assert_eq!(def.do_tasks.len(), 2);
        assert!(matches!(def.do_tasks[0].task, Task::Call(_)));
        assert!(matches!(def.do_tasks[1].task, Task::End(_)));
    }

    #[test]
    fn test_validate_rejects_duplicate_task_name() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: dup
  version: "1.0"
do:
  - step1:
      call: http
  - step1:
      call: http
"#;

        let result = parse_and_validate_yaml(yaml);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors
            .iter()
            .any(|e| matches!(e.kind, ValidationErrorKind::DuplicateTaskName(_))));
    }

    #[test]
    fn test_validate_do_task() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: do-example
  version: "1.0"
do:
  - outer:
      do:
        - inner1:
            call: http
            with:
              method: GET
              endpoint: "https://api/inner1"
        - inner2:
            call: http
            with:
              method: POST
              endpoint: "https://api/inner2"
"#;

        let result = parse_and_validate_yaml(yaml);
        assert!(result.is_ok(), "validation failed: {:?}", result.err());
        let def = result.unwrap();
        assert_eq!(def.do_tasks.len(), 1);
        match &def.do_tasks[0].task {
            Task::Do(do_task) => {
                assert_eq!(do_task.tasks.len(), 2);
                assert_eq!(do_task.tasks[0].name, "inner1");
                assert_eq!(do_task.tasks[1].name, "inner2");
            }
            _ => panic!("expected Do task"),
        }
    }

    #[test]
    fn test_validate_fork_task() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: fork-example
  version: "1.0"
do:
  - parallel:
      fork:
        branches:
          - name: branch1
            do:
              - task1:
                  call: http
                  with:
                    method: GET
                    endpoint: "https://api/task1"
          - name: branch2
            do:
              - task2:
                  call: http
                  with:
                    method: POST
                    endpoint: "https://api/task2"
"#;

        let result = parse_and_validate_yaml(yaml);
        assert!(result.is_ok(), "validation failed: {:?}", result.err());
        let def = result.unwrap();
        match &def.do_tasks[0].task {
            Task::Fork(fork) => {
                assert_eq!(fork.branches.len(), 2);
                assert_eq!(fork.branches[0].name, "branch1");
                assert_eq!(fork.branches[1].name, "branch2");
            }
            _ => panic!("expected Fork task"),
        }
    }

    #[test]
    fn test_fork_official_branch_map_shape() {
        // 官方 1.0.3 形状：branches 是单键 map 列表 `{<branch_name>: <taskBody>}`。
        // 分支名取自键名，tasks 取自该 task body；不得再落成空名 0 任务分支。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: fork-map
  version: "1.0"
do:
  - fan:
      fork:
        compete: false
        branches:
          - branch_a:
              set:
                variable: x
                value: "1"
          - branch_b:
              set:
                variable: y
                value: "2"
      then: exit
"#;
        let def = parse_and_validate_yaml(yaml).expect("official fork branch map must parse");
        match &def.do_tasks[0].task {
            Task::Fork(fork) => {
                assert_eq!(fork.branches.len(), 2);
                assert_eq!(fork.compete, Some(false));
                assert_eq!(fork.branches[0].name, "branch_a");
                assert_eq!(fork.branches[1].name, "branch_b");
                assert_eq!(
                    fork.branches[0].tasks,
                    vec![NamedTask {
                        name: "branch_a".into(),
                        task: Task::Set(SetTask {
                            variable: "x".into(),
                            value: "1".into(),
                        }),
                    }]
                );
                assert_eq!(
                    fork.branches[1].tasks,
                    vec![NamedTask {
                        name: "branch_b".into(),
                        task: Task::Set(SetTask {
                            variable: "y".into(),
                            value: "2".into(),
                        }),
                    }]
                );
            }
            other => panic!("expected Fork task, got {other:?}"),
        }
    }

    #[test]
    fn test_fork_official_compete_true_is_read_from_fork_object() {
        // 官方把 `compete` 放在 `fork` 对象内；误从任务体读会得到 None 并以非竞速模式运行。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: fork-compete
  version: "1.0"
do:
  - race:
      fork:
        compete: true
        branches:
          - branch_a:
              set:
                variable: x
                value: "1"
"#;
        let def = parse_and_validate_yaml(yaml).expect("official fork compete must parse");
        match &def.do_tasks[0].task {
            Task::Fork(fork) => assert_eq!(fork.compete, Some(true)),
            other => panic!("expected Fork task, got {other:?}"),
        }
    }

    #[test]
    fn test_fork_branch_keyed_name_is_official_map() {
        // 分支项键名为 `name` 且值为对象：不是 legacy `{name: <string>, ...}`，而是官方
        // 单键 map——分支名恰为 `name`，不得落成空名 0 任务分支。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: fork-name-key
  version: "1.0"
do:
  - fan:
      fork:
        branches:
          - name:
              set:
                variable: x
                value: "1"
"#;
        let def = parse_and_validate_yaml(yaml).expect("branch keyed name must parse");
        match &def.do_tasks[0].task {
            Task::Fork(fork) => {
                assert_eq!(fork.branches.len(), 1);
                assert_eq!(fork.branches[0].name, "name");
                assert_eq!(
                    fork.branches[0].tasks,
                    vec![NamedTask {
                        name: "name".into(),
                        task: Task::Set(SetTask {
                            variable: "x".into(),
                            value: "1".into(),
                        }),
                    }]
                );
            }
            other => panic!("expected Fork task, got {other:?}"),
        }
    }

    #[test]
    fn test_fork_empty_branches_rejected() {
        // 空 branches 不得静默产出 0 分支的 Fork：必须点名 fork.branches 拒绝。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: fork-empty
  version: "1.0"
do:
  - fan:
      fork:
        branches: []
"#;
        let errs = parse_and_validate_yaml(yaml).expect_err("empty branches must be rejected");
        assert!(errors_naming(&errs, "fork.branches"), "got {errs:?}");
    }

    #[test]
    fn test_fork_branch_item_without_name_rejected() {
        // 分支项既非单键 map 也无 `name` 时无法确定分支名/body，必须按序号点名拒绝。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: fork-bad-branch
  version: "1.0"
do:
  - fan:
      fork:
        branches:
          - branch_x:
              set:
                variable: x
                value: "1"
            branch_y:
              set:
                variable: y
                value: "2"
"#;
        let errs =
            parse_and_validate_yaml(yaml).expect_err("malformed branch item must be rejected");
        assert!(errors_naming(&errs, "fork.branches[0]"), "got {errs:?}");
    }

    #[test]
    fn test_validate_try_catch_task() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: tc-example
  version: "1.0"
do:
  - safeCall:
      try:
        - riskyStep:
            call: http
            with:
              method: POST
              endpoint: "https://api/risky"
      catch:
        - errors:
            - HTTPError
            - TimeoutError
          do:
            - compensate:
                call: http
                with:
                  method: POST
                  endpoint: "https://api/compensate"
"#;

        let result = parse_and_validate_yaml(yaml);
        assert!(result.is_ok(), "validation failed: {:?}", result.err());
        let def = result.unwrap();
        match &def.do_tasks[0].task {
            Task::TryCatch(tc) => {
                assert_eq!(tc.r#try.len(), 1);
                assert_eq!(tc.catch.len(), 1);
                assert_eq!(tc.catch[0].errors.as_ref().unwrap().len(), 2);
                assert_eq!(tc.catch[0].tasks.len(), 1);
            }
            _ => panic!("expected TryCatch task"),
        }
    }

    #[test]
    fn test_validate_for_each_task() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: fe-example
  version: "1.0"
do:
  - processItems:
      for:
        input: "${ .items }"
        iteration: item
        do:
          - process:
              call: http
              with:
                method: POST
                endpoint: "https://api/process"
"#;

        let result = parse_and_validate_yaml(yaml);
        assert!(result.is_ok(), "validation failed: {:?}", result.err());
        let def = result.unwrap();
        match &def.do_tasks[0].task {
            Task::ForEach(fe) => {
                assert_eq!(fe.input, "${ .items }");
                assert_eq!(fe.iteration, "item");
                assert_eq!(fe.tasks.len(), 1);
            }
            _ => panic!("expected ForEach task"),
        }
    }

    #[test]
    fn test_for_official_each_in_with_sibling_do() {
        // 官方 1.0.3 形状（corpus 06）：`for: {each, in}` 且循环体 `do` 与 `for` 同级。
        // `each` → iteration、`in` → input、同级 `do` → 循环体任务，逐字映射，不得回退默认值。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: official-for
  version: "1.0"
do:
  - loop:
      for:
        each: item
        in: "${ .items }"
      do:
        - process:
            call: http
            with:
              method: POST
              endpoint: "https://api/process"
"#;

        let def = parse_and_validate_yaml(yaml).expect("official for-each must parse");
        match &def.do_tasks[0].task {
            Task::ForEach(fe) => {
                assert_eq!(fe.iteration, "item");
                assert_eq!(fe.input, "${ .items }");
                assert_eq!(fe.tasks.len(), 1);
                assert_eq!(fe.tasks[0].name, "process");
            }
            other => panic!("expected ForEach task, got {other:?}"),
        }
    }

    #[test]
    fn test_for_missing_each_rejected() {
        // 官方形状缺 `each`：fail-closed 点名 `for.each`，不得静默用默认变量名。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: official-for-missing-each
  version: "1.0"
do:
  - loop:
      for:
        in: "${ .items }"
      do:
        - process:
            call: http
            with:
              method: GET
              endpoint: "https://api/process"
"#;

        let errs = parse_and_validate_yaml(yaml).expect_err("missing for.each must reject");
        assert!(error_kind_naming(&errs, "for.each"), "errors: {errs:?}");
    }

    #[test]
    fn test_for_missing_in_rejected() {
        // 官方形状缺 `in`：fail-closed 点名 `for.in`，不回退空 input。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: official-for-missing-in
  version: "1.0"
do:
  - loop:
      for:
        each: item
      do:
        - process:
            call: http
            with:
              method: GET
              endpoint: "https://api/process"
"#;

        let errs = parse_and_validate_yaml(yaml).expect_err("missing for.in must reject");
        assert!(error_kind_naming(&errs, "for.in"), "errors: {errs:?}");
    }

    #[test]
    fn test_for_missing_body_rejected() {
        // 官方形状缺循环体 `do`：fail-closed 点名 `for.do`，不得产出空迭代。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: official-for-missing-body
  version: "1.0"
do:
  - loop:
      for:
        each: item
        in: "${ .items }"
"#;

        let errs = parse_and_validate_yaml(yaml).expect_err("missing for.do must reject");
        assert!(error_kind_naming(&errs, "for.do"), "errors: {errs:?}");
    }

    #[test]
    fn test_for_non_object_rejected() {
        // 非对象 `for`：fail-closed 点名 `for`，不得静默丢掉整个任务。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: official-for-non-object
  version: "1.0"
do:
  - loop:
      for: "${ .items }"
      do:
        - process:
            call: http
            with:
              method: GET
              endpoint: "https://api/process"
"#;

        let errs = parse_and_validate_yaml(yaml).expect_err("non-object for must reject");
        assert!(error_kind_naming(&errs, "for"), "errors: {errs:?}");
    }

    #[test]
    fn test_for_empty_object_rejected() {
        // `for: {}` + 同级 `do`：既无官方键也无 legacy 键，不得伪造 `iteration="item"`
        // / `input=""`，fail-closed 点名 `for.in`。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: official-for-empty-object
  version: "1.0"
do:
  - loop:
      for: {}
      do:
        - process:
            call: http
            with:
              method: GET
              endpoint: "https://api/process"
"#;

        let errs = parse_and_validate_yaml(yaml).expect_err("empty for object must reject");
        assert!(error_kind_naming(&errs, "for.in"), "errors: {errs:?}");
    }

    #[test]
    fn test_for_at_rejected() {
        // 官方 schema 的 `for.at`（集合路径）coord 未实现：fail-closed 点名 `for.at`。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: official-for-at
  version: "1.0"
do:
  - loop:
      for:
        each: item
        at: "${ .items }"
      do:
        - process:
            call: http
            with:
              method: GET
              endpoint: "https://api/process"
"#;

        let errs = parse_and_validate_yaml(yaml).expect_err("for.at must reject");
        assert!(errors_naming(&errs, "for.at"), "errors: {errs:?}");
    }

    #[test]
    fn test_for_while_rejected() {
        // 官方 1.0.3 `while`（forTask 循环继续条件，与 `for` 同级）coord 未实现：
        // 当前不读它，条件会被静默忽略而迭代无界——fail-closed 点名 `while`。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: official-for-while
  version: "1.0"
do:
  - loop:
      for:
        each: item
        in: "${ .items }"
      while: "${ .more }"
      do:
        - process:
            call: http
            with:
              method: GET
              endpoint: "https://api/process"
"#;

        let errs = parse_and_validate_yaml(yaml).expect_err("while must reject");
        assert!(error_kind_naming(&errs, "while"), "errors: {errs:?}");
    }

    #[test]
    fn test_for_body_unknown_function_rejected() {
        // 优先级改动后 for 体不再当 Do；未知函数引用校验必须递归进 ForEach.tasks。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: official-for-body-fn
  version: "1.0"
do:
  - loop:
      for:
        each: item
        in: "${ .items }"
      do:
        - process:
            call: myFunction
"#;

        let errs = parse_and_validate_yaml(yaml).expect_err("unknown fn in for body must reject");
        assert!(
            errs.iter().any(|e| e.message.contains("myFunction")),
            "errors: {errs:?}"
        );
    }

    #[test]
    fn test_for_body_invalid_transition_rejected() {
        // 同上：for 体内 switch 的未知跳转目标必须递归校验（UnknownTaskReference）。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: official-for-body-transition
  version: "1.0"
do:
  - loop:
      for:
        each: item
        in: "${ .items }"
      do:
        - branch:
            switch:
              - condition: "${ .x > 0 }"
                transition: nowhere
"#;

        let errs =
            parse_and_validate_yaml(yaml).expect_err("unknown transition in for body must reject");
        assert!(
            errs.iter().any(|e| matches!(
                e.kind,
                ValidationErrorKind::UnknownTaskReference(ref n) if n == "nowhere"
            )),
            "errors: {errs:?}"
        );
    }

    #[test]
    fn test_detect_cyclic_dependency() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: cycle
  version: "1.0"
do:
  - taskA:
      switch:
        - condition: "${ .x > 0 }"
          transition: taskB
  - taskB:
      switch:
        - condition: "${ .y > 0 }"
          transition: taskA
"#;

        let result = parse_and_validate_yaml(yaml);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors
            .iter()
            .any(|e| matches!(e.kind, ValidationErrorKind::CyclicDependency(_))));
    }

    #[test]
    fn test_validate_unknown_task_type() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: unknown-type
  version: "1.0"
do:
  - weirdTask:
      unknownAction: something
"#;

        let result = parse_and_validate_yaml(yaml);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors
            .iter()
            .any(|e| matches!(e.kind, ValidationErrorKind::UnknownTaskType)));
    }

    #[test]
    fn test_validate_set_task() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: set-example
  version: "1.0"
do:
  - updateVar:
      set:
        variable: status
        value: "${ \"approved\" }"
"#;

        let result = parse_and_validate_yaml(yaml);
        assert!(result.is_ok(), "validation failed: {:?}", result.err());
        let def = result.unwrap();
        match &def.do_tasks[0].task {
            Task::Set(s) => {
                assert_eq!(s.variable, "status");
                assert_eq!(s.value, "${ \"approved\" }");
            }
            _ => panic!("expected Set task"),
        }
    }

    #[test]
    fn test_set_official_map_numeric_literal() {
        // 官方 1.0.3 形状 `set: {counter: 0}`（corpus 01/05）必须映射成 variable=counter、
        // value 求值为数字 0，不得落成空 SetTask。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: official-set-num
  version: "1.0"
do:
  - init:
      set:
        counter: 0
"#;
        let def = parse_and_validate_yaml(yaml).expect("official single-key set must parse");
        let s = match &def.do_tasks[0].task {
            Task::Set(s) => s,
            other => panic!("expected Set task, got {other:?}"),
        };
        assert_eq!(s.variable, "counter");
        let value = ExpressionEvaluator::new()
            .evaluate(&s.value, &serde_json::json!({}))
            .expect("evaluate set value");
        assert_eq!(value, serde_json::json!(0));
    }

    #[test]
    fn test_set_official_map_string_literal() {
        // 字符串字面量必须以 JSON 形式序列化（`"hi"`），求值为字符串而非字段访问：
        // context 里放同名 `hi` 字段，若误当字段访问会取到 "field"。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: official-set-str
  version: "1.0"
do:
  - init:
      set:
        greeting: "hi"
"#;
        let def = parse_and_validate_yaml(yaml).expect("official single-key set must parse");
        let s = match &def.do_tasks[0].task {
            Task::Set(s) => s,
            other => panic!("expected Set task, got {other:?}"),
        };
        assert_eq!(s.variable, "greeting");
        let value = ExpressionEvaluator::new()
            .evaluate(&s.value, &serde_json::json!({"hi": "field"}))
            .expect("evaluate set value");
        assert_eq!(value, serde_json::json!("hi"));
    }

    #[test]
    fn test_set_official_map_runtime_expression_passthrough() {
        // 运行时表达式 `${ ... }` 原样透传（求值器负责剥壳），corpus 06 的 `last: "${ $item }"` 走此路。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: official-set-expr
  version: "1.0"
do:
  - init:
      set:
        last: "${ .amount }"
"#;
        let def = parse_and_validate_yaml(yaml).expect("official single-key set must parse");
        let s = match &def.do_tasks[0].task {
            Task::Set(s) => s,
            other => panic!("expected Set task, got {other:?}"),
        };
        assert_eq!(s.variable, "last");
        assert_eq!(s.value, "${ .amount }");
        let value = ExpressionEvaluator::new()
            .evaluate(&s.value, &serde_json::json!({"amount": 7}))
            .expect("evaluate set value");
        assert_eq!(value, serde_json::json!(7));
    }

    #[test]
    fn test_set_official_multi_key_rejected_naming_key() {
        // 多键官方 map 超出单变量 SetTask 表达面：fail-closed 点名 `set.<key>`，
        // 绝不静默只留首键。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: official-set-multi
  version: "1.0"
do:
  - init:
      set:
        a: 1
        b: 2
"#;
        let errs = parse_and_validate_yaml(yaml).expect_err("multi-key official set must reject");
        assert!(errors_naming(&errs, "set.b"), "errors: {errs:?}");
    }

    #[test]
    fn test_set_official_embedded_expression_rejected() {
        // 官方 map 值里嵌了 `${ ... }`（trim 后不以 `${` 开头）：coord set 无字符串插值面，
        // 当字面量处理会静默丢掉模板语义，fail-closed 点名 `set.<key>`。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: official-set-embedded
  version: "1.0"
do:
  - init:
      set:
        greeting: "hello ${ .name }"
"#;

        let errs = parse_and_validate_yaml(yaml).expect_err("embedded set expression must reject");
        assert!(errors_naming(&errs, "set.greeting"), "errors: {errs:?}");
    }

    #[test]
    fn test_set_official_unbalanced_expression_rejected() {
        // 以 `${` 开头但未以 `}` 结尾：求值器不会当模板表达式剥壳，`"${ .x } tail"` 会
        // 被当作字面量、表达式静默不求值——fail-closed 点名 `set.<key>`。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: official-set-unbalanced
  version: "1.0"
do:
  - init:
      set:
        greeting: "${ .name } tail"
"#;

        let errs =
            parse_and_validate_yaml(yaml).expect_err("unbalanced set expression must reject");
        assert!(errors_naming(&errs, "set.greeting"), "errors: {errs:?}");
    }

    #[test]
    fn test_set_official_double_expression_rejected() {
        // 多于一个 `${...}` 段（拼接）无法当一个完整表达式剥壳，求值器会静默丢段或丢尾部；
        // 只有单个完整 `${...}` 才接受。fail-closed 点名 `set.<key>`。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: official-set-double-expr
  version: "1.0"
do:
  - init:
      set:
        joined: "${ .a }${ .b }"
"#;

        let errs =
            parse_and_validate_yaml(yaml).expect_err("multi-segment set expression must reject");
        assert!(errors_naming(&errs, "set.joined"), "errors: {errs:?}");
    }

    #[test]
    fn test_set_official_array_value_rejected() {
        // 数组字面量求值器无 `[...]` 分支，会落到 `Ok(context)` 静默把整个上下文写成变量值；
        // 数组字面量支持前 fail-closed 点名 `set.<key>`。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: official-set-array
  version: "1.0"
do:
  - init:
      set:
        items: [1, 2, 3]
"#;

        let errs = parse_and_validate_yaml(yaml).expect_err("array set value must reject");
        assert!(errors_naming(&errs, "set.items"), "errors: {errs:?}");
    }

    #[test]
    fn test_set_official_nested_object_array_value_rejected() {
        // 对象字面量内嵌数组同样会经 try_object_literal 落到数组求值路径：一并 fail-closed。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: official-set-nested-array
  version: "1.0"
do:
  - init:
      set:
        payload:
          list: [1, 2]
"#;

        let errs = parse_and_validate_yaml(yaml).expect_err("nested array set value must reject");
        assert!(errors_naming(&errs, "set.payload"), "errors: {errs:?}");
    }

    #[test]
    fn test_set_official_single_key_named_variable_survives() {
        // 官方单键 map 的键名恰为 `variable` 时，不得被 native `{variable, value}` 分支误读：
        // native 分支现要求两键齐备，故此例应落成 variable="variable"、value 求值为 "x"。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: official-set-var-key
  version: "1.0"
do:
  - init:
      set:
        variable: "x"
"#;

        let def = parse_and_validate_yaml(yaml).expect("single-key official set must parse");
        let s = match &def.do_tasks[0].task {
            Task::Set(s) => s,
            other => panic!("expected Set task, got {other:?}"),
        };
        assert_eq!(s.variable, "variable");
        let value = ExpressionEvaluator::new()
            .evaluate(&s.value, &serde_json::json!({}))
            .expect("evaluate set value");
        assert_eq!(value, serde_json::json!("x"));
    }

    // ═══ 任务级标准字段 / 顶层扩展块解析（P0 全特性） ═══

    #[test]
    fn test_parse_task_meta_fields() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: meta-wf
  version: "1.0"
do:
  - guarded:
      if: "${ .approved == true }"
      input:
        from: "${ { \"x\": .raw } }"
        schema: '{"type":"object"}'
      output:
        as: "${ $output }"
        schema: '{"type":"object"}'
      export:
        as: "${ . + {\"done\": true} }"
        schema: '{"type":"object"}'
      retry:
        delay: "PT1S"
        backoff: exponential
        limit: 5
      timeout:
        after: "PT1M"
      set:
        variable: v
        value: "${ .x }"
"#;
        let def = parse_and_validate_yaml(yaml).expect("parse meta-wf");
        let meta = def.task_meta.get("guarded").expect("task meta for guarded");
        assert_eq!(meta.if_condition.as_deref(), Some("${ .approved == true }"));
        let input = meta.input.as_ref().unwrap();
        assert_eq!(input.from.as_deref(), Some("${ { \"x\": .raw } }"));
        assert_eq!(input.schema.as_deref(), Some("{\"type\":\"object\"}"));
        let output = meta.output.as_ref().unwrap();
        assert_eq!(output.as_expr.as_deref(), Some("${ $output }"));
        let export = meta.export.as_ref().unwrap();
        assert!(export.as_expr.is_some());
        let retry = meta.retry.as_ref().unwrap();
        assert_eq!(retry.limit, 5);
        assert_eq!(retry.backoff, Some(BackoffStrategy::Exponential));
        let timeout = meta.timeout.as_ref().unwrap();
        assert_eq!(timeout.after, "PT1M");
    }

    #[test]
    fn test_parse_task_retry_dsl_shape() {
        // DSL 形状：delay.seconds / backoff.linear / limit.attempt.count
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: retry-dsl
  version: "1.0"
do:
  - callApi:
      call: http
      retry:
        delay:
          seconds: 3
        backoff:
          linear: {}
        limit:
          attempt:
            count: 5
"#;
        let def = parse_and_validate_yaml(yaml).expect("parse retry-dsl");
        let meta = def.task_meta.get("callApi").expect("task meta");
        let retry = meta.retry.as_ref().unwrap();
        assert_eq!(retry.delay, "PT3S");
        assert_eq!(retry.backoff, Some(BackoffStrategy::Linear));
        assert_eq!(retry.limit, 5);
    }

    #[test]
    fn test_parse_task_retry_reference_to_use_retries() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: retry-ref
  version: "1.0"
use:
  retries:
    defaultRetry:
      delay: "PT1S"
      backoff: constant
      limit: 4
do:
  - callApi:
      call: http
      retry: defaultRetry
"#;
        let def = parse_and_validate_yaml(yaml).expect("parse retry-ref");
        let meta = def.task_meta.get("callApi").expect("task meta");
        let retry = meta.retry.as_ref().unwrap();
        assert_eq!(retry.limit, 4);
        assert_eq!(retry.delay, "PT1S");
    }

    #[test]
    fn test_parse_top_level_ext_blocks() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: ext-wf
  version: "1.0"
input:
  schema: '{"type":"object"}'
  from: "${ . }"
output:
  as: "${ $output }"
  schema: '{"type":"object"}'
timeout:
  after: "PT5M"
schedule:
  every: "PT1H"
  cron: "0 0 * * *"
auth:
  myBasic:
    scheme: basic
    username: admin
    password: "${ $secrets.adminPass }"
secrets:
  - adminPass
constants:
  region: cn-north
do:
  - step:
      set:
        variable: v
        value: "1"
"#;
        let def = parse_and_validate_yaml(yaml).expect("parse ext-wf");
        let input = def.input.unwrap();
        assert_eq!(input.schema.as_deref(), Some("{\"type\":\"object\"}"));
        assert_eq!(input.from.as_deref(), Some("${ . }"));
        let output = def.output.unwrap();
        assert_eq!(output.as_expr.as_deref(), Some("${ $output }"));
        assert_eq!(def.timeout.as_ref().unwrap().after, "PT5M");
        assert_eq!(def.schedule.every.as_deref(), Some("PT1H"));
        assert_eq!(def.schedule.cron.as_deref(), Some("0 0 * * *"));
        let auth = def.auth.get("myBasic").unwrap();
        assert_eq!(auth.scheme.as_deref(), Some("basic"));
        assert_eq!(auth.username.as_deref(), Some("admin"));
        assert_eq!(
            def.secrets.keys.as_ref().unwrap(),
            &vec!["adminPass".to_string()]
        );
        assert_eq!(
            def.constants
                .values
                .as_ref()
                .unwrap()
                .get("region")
                .unwrap(),
            &Value::String("cn-north".into())
        );
    }

    #[test]
    fn test_parse_nested_task_meta() {
        // do 子任务的标准字段也会被收集
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: nested-meta
  version: "1.0"
do:
  - parent:
      do:
        - child:
            if: "${ .flag }"
            set:
              variable: c
              value: "1"
"#;
        let def = parse_and_validate_yaml(yaml).expect("parse nested-meta");
        let meta = def.task_meta.get("child").expect("child meta collected");
        assert_eq!(meta.if_condition.as_deref(), Some("${ .flag }"));
    }

    // ─── 未实现构造 fail-closed ───

    fn errors_naming(errs: &[ValidationError], construct: &str) -> bool {
        errs.iter().any(|e| {
            matches!(&e.kind, ValidationErrorKind::UnsupportedConstruct(_))
                && e.message.contains(construct)
        })
    }

    /// 判别性断言：错误 KIND 载荷里的构造名恰为 `construct`。
    /// `errors_naming` 只查消息子串，而 `reject_for_field` 的消息列出全部字段名，无法区分。
    fn error_kind_naming(errs: &[ValidationError], construct: &str) -> bool {
        errs.iter().any(
            |e| matches!(&e.kind, ValidationErrorKind::UnsupportedConstruct(c) if c == construct),
        )
    }

    #[test]
    fn test_wait_official_seconds_maps_to_duration() {
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: wait-seconds
  version: "1.0"
do:
  - pause:
      wait:
        seconds: 5
"#;
        let def = parse_and_validate_yaml(yaml).expect("official wait seconds must parse");
        match &def.do_tasks[0].task {
            Task::Wait(w) => assert_eq!(w.wait, "PT5S"),
            other => panic!("expected Wait task, got {other:?}"),
        }
    }

    #[test]
    fn test_wait_runtime_expression_rejected() {
        // 字符串含 `${` 是运行时表达式形式：会落成非法 ISO8601 → 零等待（定时变 no-op），
        // fail-closed 点名 `wait`。其它非法字符串（如 "INVALID"）仍按零等待兼容。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: wait-expr
  version: "1.0"
do:
  - pause:
      wait: "${ .delay }"
"#;

        let errs = parse_and_validate_yaml(yaml).expect_err("runtime expression wait must reject");
        assert!(errors_naming(&errs, "wait"), "errors: {errs:?}");
    }

    #[test]
    fn test_wait_until_is_rejected_with_construct_name() {
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: wait-until
  version: "1.0"
do:
  - pause:
      wait:
        until: "2026-01-01T00:00:00Z"
"#;
        let errs = parse_and_validate_yaml(yaml).expect_err("wait.until must be rejected");
        assert!(errors_naming(&errs, "wait.until"), "got {errs:?}");
    }

    #[test]
    fn test_wait_minutes_only_maps_to_duration() {
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: wait-minutes
  version: "1.0"
do:
  - pause:
      wait:
        minutes: 5
"#;
        let def = parse_and_validate_yaml(yaml).expect("official wait minutes must parse");
        match &def.do_tasks[0].task {
            Task::Wait(w) => assert_eq!(w.wait, "PT5M"),
            other => panic!("expected Wait task, got {other:?}"),
        }
    }

    #[test]
    fn test_wait_seconds_and_milliseconds_compose() {
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: wait-composite
  version: "1.0"
do:
  - pause:
      wait:
        seconds: 1
        milliseconds: 500
"#;
        let def = parse_and_validate_yaml(yaml).expect("composite wait must parse");
        match &def.do_tasks[0].task {
            Task::Wait(w) => assert_eq!(w.wait, "PT1.5S"),
            other => panic!("expected Wait task, got {other:?}"),
        }
    }

    #[test]
    fn test_wait_unknown_key_is_rejected() {
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: wait-unknown
  version: "1.0"
do:
  - pause:
      wait:
        weeks: 2
"#;
        let errs = parse_and_validate_yaml(yaml).expect_err("unknown wait key must be rejected");
        assert!(errors_naming(&errs, "wait.weeks"), "got {errs:?}");
    }

    #[test]
    fn test_wait_empty_object_is_rejected() {
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: wait-empty
  version: "1.0"
do:
  - pause:
      wait: {}
"#;
        let errs = parse_and_validate_yaml(yaml).expect_err("empty wait object must be rejected");
        // 判别性断言：构造名必须恰为 `wait`（`wait.weeks`/`wait.until` 不满足），
        // 且原因指向「至少一个组件」。
        assert!(
            errs.iter().any(|e| {
                matches!(&e.kind, ValidationErrorKind::UnsupportedConstruct(s) if s == "wait")
                    && e.message.contains("at least one")
            }),
            "got {errs:?}"
        );
    }

    #[test]
    fn test_emit_official_event_with_shape() {
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: emit-official
  version: "1.0"
do:
  - notify:
      emit:
        event:
          with:
            source: "https://example.com/coord"
            type: com.example.notify
            data:
              msg: hi
"#;
        let def = parse_and_validate_yaml(yaml).expect("official emit event shape must parse");
        match &def.do_tasks[0].task {
            Task::Emit(e) => {
                assert_eq!(e.emit.event_type, "com.example.notify");
                assert_eq!(e.emit.source.as_deref(), Some("https://example.com/coord"));
                assert_eq!(e.emit.data, Some(serde_json::json!({ "msg": "hi" })));
            }
            other => panic!("expected Emit task, got {other:?}"),
        }
    }

    #[test]
    fn test_emit_missing_type_rejected() {
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: emit-missing-type
  version: "1.0"
do:
  - notify:
      emit:
        event:
          with:
            source: "https://example.com/coord"
"#;
        let errs = parse_and_validate_yaml(yaml).expect_err("emit without type must be rejected");
        assert!(errors_naming(&errs, "emit.event.with.type"), "got {errs:?}");
    }

    #[test]
    fn test_emit_official_extra_with_key_rejected() {
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: emit-extra-with
  version: "1.0"
do:
  - notify:
      emit:
        event:
          with:
            type: com.example.notify
            source: "https://example.com/coord"
            subject: approvals
"#;
        // eventProperties 额外字段（subject 等）无对应 EmitEvent 字段，不得静默丢弃。
        let errs = parse_and_validate_yaml(yaml)
            .expect_err("out-of-range emit.event.with key must be rejected");
        assert!(
            errors_naming(&errs, "emit.event.with.subject"),
            "got {errs:?}"
        );
    }

    #[test]
    fn test_emit_official_extra_event_key_rejected() {
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: emit-extra-event
  version: "1.0"
do:
  - notify:
      emit:
        event:
          id: evt-1
          with:
            type: com.example.notify
            source: "https://example.com/coord"
"#;
        // `event` 的 `with` 兄弟键（schema additionalProperties: true）无法映射，不得静默丢弃。
        let errs = parse_and_validate_yaml(yaml)
            .expect_err("unexpected emit.event sibling key must be rejected");
        assert!(errors_naming(&errs, "emit.event.id"), "got {errs:?}");
    }

    #[test]
    fn test_emit_flat_legacy_shape_is_compatible() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: emit-flat
  version: "1.0"
do:
  - notify:
      emit:
        type: com.example.flat
        source: "/flat"
        data:
          n: 1
"#;
        let def = parse_and_validate_yaml(yaml).expect("flat emit must remain accepted");
        match &def.do_tasks[0].task {
            Task::Emit(e) => {
                assert_eq!(e.emit.event_type, "com.example.flat");
                assert_eq!(e.emit.source.as_deref(), Some("/flat"));
                assert_eq!(e.emit.data, Some(serde_json::json!({ "n": 1 })));
            }
            other => panic!("expected Emit task, got {other:?}"),
        }
    }

    #[test]
    fn test_emit_non_object_rejected() {
        // 非对象 emit 无字段可映射，不得静默落成空事件。fail-closed 点名 `emit`。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: emit-non-object
  version: "1.0"
do:
  - notify:
      emit: []
"#;

        let errs = parse_and_validate_yaml(yaml).expect_err("non-object emit must reject");
        assert!(errors_naming(&errs, "emit"), "errors: {errs:?}");
    }

    #[test]
    fn test_listen_official_to_one_with_shape() {
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: listen-official
  version: "1.0"
do:
  - await_event:
      listen:
        to:
          one:
            with:
              type: com.example.approved
              source: "https://example.com/coord"
              subject: approvals
      then: exit
"#;
        let def = parse_and_validate_yaml(yaml).expect("official listen to.one.with must parse");
        match &def.do_tasks[0].task {
            Task::Listen(l) => {
                assert_eq!(l.listen.event_type.as_deref(), Some("com.example.approved"));
                assert_eq!(
                    l.listen.source.as_deref(),
                    Some("https://example.com/coord")
                );
                assert_eq!(l.listen.subject.as_deref(), Some("approvals"));
            }
            other => panic!("expected Listen task, got {other:?}"),
        }
    }

    #[test]
    fn test_listen_non_object_rejected() {
        // 非对象也非字符串的 listen 无过滤器可映射，不得静默落成空监听。fail-closed 点名 `listen`。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: listen-non-object
  version: "1.0"
do:
  - await_event:
      listen: []
"#;

        let errs = parse_and_validate_yaml(yaml).expect_err("non-object listen must reject");
        assert!(errors_naming(&errs, "listen"), "errors: {errs:?}");
    }

    #[test]
    fn test_listen_unsupported_strategy_rejected() {
        // 消费策略 all/any 本期不支持：不得静默丢过滤器后照常接受。
        let all = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: listen-all
  version: "1.0"
do:
  - await_event:
      listen:
        to:
          all:
            - with:
                type: com.example.approved
"#;
        let any = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: listen-any
  version: "1.0"
do:
  - await_event:
      listen:
        to:
          any:
            - with:
                type: com.example.approved
"#;
        for (yaml, strategy) in [(all, "all"), (any, "any")] {
            let errs = parse_and_validate_yaml(yaml)
                .expect_err("non-one consumption strategy must be rejected");
            assert!(
                errors_naming(&errs, &format!("listen.to.{strategy}")),
                "got {errs:?}"
            );
        }
    }

    #[test]
    fn test_listen_missing_type_rejected() {
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: listen-missing-type
  version: "1.0"
do:
  - await_event:
      listen:
        to:
          one:
            with:
              source: "https://example.com/coord"
"#;
        let errs = parse_and_validate_yaml(yaml).expect_err("listen without type must be rejected");
        assert!(
            errors_naming(&errs, "listen.to.one.with.type"),
            "got {errs:?}"
        );
    }

    #[test]
    fn test_listen_read_rejected() {
        // `listen.read`（envelope/raw 读取模式）无映射面：不得静默丢字段后照常接受。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: listen-read
  version: "1.0"
do:
  - await_event:
      listen:
        read: raw
        to:
          one:
            with:
              type: com.example.approved
"#;
        let errs = parse_and_validate_yaml(yaml).expect_err("listen.read must be rejected");
        assert!(errors_naming(&errs, "listen.read"), "got {errs:?}");
    }

    #[test]
    fn test_listen_to_one_correlate_rejected() {
        // `eventFilter.correlate` 需把事件属性关联到输入数据属性，无映射面。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: listen-correlate
  version: "1.0"
do:
  - await_event:
      listen:
        to:
          one:
            correlate:
              from: "${ .context }"
            with:
              type: com.example.approved
"#;
        let errs =
            parse_and_validate_yaml(yaml).expect_err("listen.to.one.correlate must be rejected");
        assert!(
            errors_naming(&errs, "listen.to.one.correlate"),
            "got {errs:?}"
        );
    }

    #[test]
    fn test_listen_unknown_with_key_rejected() {
        // `with` 只支持 type/source/subject；其余键（如 id）无映射面，必须点名拒绝。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: listen-with-extra
  version: "1.0"
do:
  - await_event:
      listen:
        to:
          one:
            with:
              type: com.example.approved
              id: evt-1
"#;
        let errs = parse_and_validate_yaml(yaml)
            .expect_err("unknown listen.to.one.with key must be rejected");
        assert!(
            errors_naming(&errs, "listen.to.one.with.id"),
            "got {errs:?}"
        );
    }

    #[test]
    fn test_emit_extra_emit_level_key_rejected() {
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: emit-extra-emit
  version: "1.0"
do:
  - notify:
      emit:
        event:
          with:
            type: com.example.notify
        id: evt-1
"#;
        // `emit` 的 `event` 兄弟键 schema 非法（unevaluatedProperties: false），不得静默丢弃。
        let errs = parse_and_validate_yaml(yaml)
            .expect_err("unexpected emit-level sibling key must be rejected");
        assert!(errors_naming(&errs, "emit.id"), "got {errs:?}");
    }

    #[test]
    fn test_wait_rejection_messages_are_single_line() {
        // 三条 wait 拒绝路径的用户可见消息不得内嵌换行/缩进（曾因续行反斜杠缺失而断裂）。
        let unknown_key = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: wait-msg-unknown
  version: "1.0"
do:
  - pause:
      wait:
        weeks: 2
"#;
        let until = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: wait-msg-until
  version: "1.0"
do:
  - pause:
      wait:
        until: "2026-01-01T00:00:00Z"
"#;
        let bad_shape = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: wait-msg-shape
  version: "1.0"
do:
  - pause:
      wait: true
"#;
        for yaml in [unknown_key, until, bad_shape] {
            let errs = parse_and_validate_yaml(yaml).expect_err("wait variant must be rejected");
            assert!(
                errs.iter().all(|e| !e.message.contains('\n')),
                "user-visible message must be single-line: {errs:?}"
            );
        }
    }

    #[test]
    fn test_then_continue_and_top_level_last_exit_accepted() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: then-ok
  version: "1.0"
do:
  - first:
      set:
        variable: a
        value: "1"
      then: continue
  - last:
      set:
        variable: b
        value: "2"
      then: exit
"#;
        assert!(
            parse_and_validate_yaml(yaml).is_ok(),
            "continue / 末任务 exit 应与顺序执行等价"
        );
    }

    #[test]
    fn test_then_mid_list_exit_rejected() {
        // 中途 exit 会被忽略 → 控制流失真，必须拒绝
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: then-mid
  version: "1.0"
do:
  - first:
      set:
        variable: a
        value: "1"
      then: exit
  - second:
      set:
        variable: b
        value: "2"
"#;
        let errs = parse_and_validate_yaml(yaml).unwrap_err();
        assert!(errors_naming(&errs, "then"), "errors: {errs:?}");
    }

    #[test]
    fn test_then_named_jump_rejected() {
        // 具名跳转 coord 无法表达：不得静默按列表顺序继续
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: then-jump
  version: "1.0"
do:
  - first:
      set:
        variable: a
        value: "1"
      then: second
  - second:
      set:
        variable: b
        value: "2"
"#;
        let errs = parse_and_validate_yaml(yaml).unwrap_err();
        assert!(errors_naming(&errs, "then"), "errors: {errs:?}");
    }

    #[test]
    fn test_then_nested_exit_rejected() {
        // 嵌套 do 中的 exit 不在顶层列表末尾，被忽略即失真
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: then-nested
  version: "1.0"
do:
  - group:
      do:
        - inner:
            set:
              variable: a
              value: "1"
            then: exit
"#;
        let errs = parse_and_validate_yaml(yaml).unwrap_err();
        assert!(errors_naming(&errs, "then"), "errors: {errs:?}");
    }

    #[test]
    fn test_run_script_rejected_naming_construct() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: run-script
  version: "1.0"
do:
  - exec:
      run:
        script:
          language: js
          code: "return 1;"
"#;
        let errs = parse_and_validate_yaml(yaml).unwrap_err();
        assert!(errors_naming(&errs, "run.script"), "errors: {errs:?}");
    }

    #[test]
    fn test_run_workflow_keeps_subflow_semantics() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: run-workflow
  version: "1.0"
do:
  - sub:
      run:
        workflow:
          namespace: other
          name: child
          version: "1.0"
"#;
        let def = parse_and_validate_yaml(yaml).expect("run.workflow 是受支持形状");
        match &def.do_tasks[0].task {
            Task::Run(run) => {
                assert_eq!(run.workflow.namespace, "other");
                assert_eq!(run.workflow.name, "child");
                assert_eq!(run.workflow.version, "1.0");
            }
            other => panic!("expected Run task, got {other:?}"),
        }
    }

    #[test]
    fn test_run_workflow_flat_legacy_ref_extracted() {
        // 旧方言：namespace/name/version 直接放在 `run` 上（无 `workflow` 层）
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: run-workflow-flat
  version: "1.0"
do:
  - sub:
      run:
        namespace: other
        name: child
        version: "2.0"
"#;
        let def = parse_and_validate_yaml(yaml).expect("扁平 ref 是旧方言受支持形状");
        match &def.do_tasks[0].task {
            Task::Run(run) => {
                assert_eq!(run.workflow.namespace, "other");
                assert_eq!(run.workflow.name, "child");
                assert_eq!(run.workflow.version, "2.0");
            }
            other => panic!("expected Run task, got {other:?}"),
        }
    }

    #[test]
    fn test_run_workflow_without_name_rejected() {
        // 有形状无目标：`workflow: {}` 不得静默编译成空 ref
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: run-workflow-empty
  version: "1.0"
do:
  - sub:
      run:
        workflow: {}
"#;
        let errs = parse_and_validate_yaml(yaml).unwrap_err();
        assert!(
            errs.iter().any(|e| e.message.contains("run.workflow")),
            "errors: {errs:?}"
        );
    }

    #[test]
    fn test_run_workflow_nested_input_preserved() {
        // 官方把子流程输入嵌在 `run.workflow.input`：必须落到 RunTask.input（子流程的入参槽），
        // 不得只取任务级 `input` 而静默丢弃嵌套载荷（Spike 4 误读之一）。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: run-workflow-input
  version: "1.0"
do:
  - sub:
      run:
        workflow:
          namespace: other
          name: child
          version: "1.0"
          input:
            orderId: 42
"#;
        let def = parse_and_validate_yaml(yaml).expect("nested run.workflow.input must parse");
        match &def.do_tasks[0].task {
            Task::Run(run) => {
                assert_eq!(run.workflow.name, "child");
                assert_eq!(run.input, Some(serde_json::json!({ "orderId": 42 })));
            }
            other => panic!("expected Run task, got {other:?}"),
        }
    }

    #[test]
    fn test_run_workflow_nested_input_non_object_rejected() {
        // 官方 schema 要求 `workflow.input` 为对象：present 但非对象（含 null）必须
        // fail-closed 点名 `run.workflow.input`，不得静默忽略或遮蔽扁平回退。
        for bad in ["\"foo\"", "null", "[1, 2]"] {
            let yaml = format!(
                r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: run-workflow-bad-input
  version: "1.0"
do:
  - sub:
      run:
        workflow:
          namespace: other
          name: child
          version: "1.0"
          input: {bad}
"#
            );
            let errs =
                parse_and_validate_yaml(&yaml).expect_err("non-object workflow.input must reject");
            assert!(
                error_kind_naming(&errs, "run.workflow.input"),
                "input={bad}: errors: {errs:?}"
            );
        }
    }

    #[test]
    fn test_use_extensions_rejected() {
        let yaml = r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: ext
  version: "1.0"
use:
  extensions:
    - audit:
        extend: all
        before:
          - log_before:
              set:
                variable: audited
                value: "true"
do:
  - main:
      set:
        variable: x
        value: "1"
"#;
        let errs = parse_and_validate_yaml(yaml).unwrap_err();
        assert!(
            errs.iter().any(|e| e.message.contains("use.extensions")),
            "errors: {errs:?}"
        );
    }

    // ─── approve 节点（workflow v2 审批） ───

    #[test]
    fn parse_approve_minimal() {
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: approve-min
  version: "1.0"
do:
  - review:
      approve:
        approvers: ["alice"]
"#;
        let def = parse_and_validate_yaml(yaml).expect("minimal approve must parse");
        match &def.do_tasks[0].task {
            Task::Approve(a) => {
                assert_eq!(a.approvers, vec!["alice".to_string()]);
                assert_eq!(a.mode, JoinPolicy::All);
                assert!(a.veto, "default veto must be true");
                assert!(!a.allow_self_approve);
                assert!(a.pool.is_empty());
                assert!(a.readers.is_empty());
                assert!(a.roles.is_empty());
                assert!(a.deadline.is_none());
                assert!(a.on_pass.is_none());
                assert!(a.on_reject.is_none());
                assert!(a.sign_admins.is_empty());
            }
            other => panic!("expected Approve task, got {other:?}"),
        }
    }

    /// `deadline` 必须能换算成确定的毫秒时长：日期部分的月/年（`P1M`/`P1Y`/`P1Y2M`）长度
    /// 不定，`parse_iso8601_duration_ms` 会静默落成 0（= 立即到期、把所有席位秒过期），
    /// 故解析期即以 `UnsupportedConstruct("approve.deadline")` 拒绝；`PT...` 时间部分仍接受。
    #[test]
    fn parse_approve_rejects_month_year_deadlines() {
        for dur in ["P1M", "P1Y", "P1Y2M"] {
            let yaml = format!(
                r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: approve-deadline-month
  version: "1.0"
do:
  - review:
      approve:
        approvers: ["alice"]
        deadline: "{dur}"
"#
            );
            let errs = parse_and_validate_yaml(&yaml)
                .expect_err("month/year deadline must be rejected");
            assert!(
                error_kind_naming(&errs, "approve.deadline"),
                "deadline '{dur}' 必须以 approve.deadline 拒绝: {errs:?}"
            );
        }
        for dur in ["PT1H", "PT30M"] {
            let yaml = format!(
                r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: approve-deadline-time
  version: "1.0"
do:
  - review:
      approve:
        approvers: ["alice"]
        deadline: "{dur}"
"#
            );
            let def = parse_and_validate_yaml(&yaml)
                .unwrap_or_else(|e| panic!("time-part deadline '{dur}' must parse: {e:?}"));
            match &def.do_tasks[0].task {
                Task::Approve(a) => assert_eq!(a.deadline.as_deref(), Some(dur)),
                other => panic!("expected Approve task, got {other:?}"),
            }
        }
    }

    #[test]
    fn parse_approve_full() {
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: approve-full
  version: "1.0"
do:
  - review:
      approve:
        approvers: ["alice"]
        pool: ["bob", "carol"]
        readers: ["dave"]
        roles: ["manager"]
        mode: "ratio:2/3"
        veto: false
        deadline: "PT1H"
        onPass: "next"
        onReject: "rejected"
        signAdmins: ["admin"]
        allowSelfApprove: true
  - next:
      set:
        variable: a
        value: "1"
  - rejected:
      set:
        variable: b
        value: "2"
"#;
        let def = parse_and_validate_yaml(yaml).expect("full approve must parse");
        match &def.do_tasks[0].task {
            Task::Approve(a) => {
                assert_eq!(a.approvers, vec!["alice".to_string()]);
                assert_eq!(a.pool, vec!["bob".to_string(), "carol".to_string()]);
                assert_eq!(a.readers, vec!["dave".to_string()]);
                assert_eq!(a.roles, vec!["manager".to_string()]);
                assert_eq!(a.mode, JoinPolicy::Ratio { num: 2, den: 3 });
                assert!(!a.veto);
                assert_eq!(a.deadline.as_deref(), Some("PT1H"));
                assert_eq!(a.on_pass.as_deref(), Some("next"));
                assert_eq!(a.on_reject.as_deref(), Some("rejected"));
                assert_eq!(a.sign_admins, vec!["admin".to_string()]);
                assert!(a.allow_self_approve);
                // 席位数：1 指定审批 + 1 候选池 + 1 阅签（roles 不在此解析）
                assert_eq!(a.to_seats().len(), 3);
            }
            other => panic!("expected Approve task, got {other:?}"),
        }
    }

    #[test]
    fn parse_approve_rejects_unknown_key() {
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: approve-unknown
  version: "1.0"
do:
  - review:
      approve:
        approvers: ["alice"]
        bogus: 1
"#;
        let errs = parse_and_validate_yaml(yaml).expect_err("unknown approve key must reject");
        assert!(
            error_kind_naming(&errs, "approve.bogus"),
            "errors: {errs:?}"
        );
    }

    #[test]
    fn parse_approve_rejects_unknown_mode() {
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: approve-mode
  version: "1.0"
do:
  - review:
      approve:
        approvers: ["alice"]
        mode: "majority"
"#;
        let errs = parse_and_validate_yaml(yaml).expect_err("unknown approve.mode must reject");
        assert!(error_kind_naming(&errs, "approve.mode"), "errors: {errs:?}");
    }

    #[test]
    fn parse_approve_rejects_zero_threshold_modes() {
        // 零阈值在 recount 下恒真/退化：`quorum:0` 与 `ratio:0/<den>` 零审批自动通过，
        // `ratio:<num>/0` 退化。全部必须在解析期 fail-closed，不得携带任务进入运行期。
        for mode in ["quorum:0", "ratio:0/3", "ratio:3/0", "ratio:0/0"] {
            let yaml = format!(
                r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: approve-zero-mode
  version: "1.0"
do:
  - review:
      approve:
        approvers: ["alice"]
        mode: "{mode}"
"#
            );
            let errs = parse_and_validate_yaml(&yaml).expect_err("zero threshold mode must reject");
            assert!(
                error_kind_naming(&errs, "approve.mode"),
                "mode {mode} must reject as approve.mode: {errs:?}"
            );
        }
    }

    #[test]
    fn parse_approve_rejects_absolute_deadline() {
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: approve-deadline
  version: "1.0"
do:
  - review:
      approve:
        approvers: ["alice"]
        deadline:
          until: "2026-01-01T00:00:00Z"
"#;
        let errs =
            parse_and_validate_yaml(yaml).expect_err("absolute approve deadline must reject");
        assert!(
            error_kind_naming(&errs, "approve.deadline.until"),
            "errors: {errs:?}"
        );
    }

    #[test]
    fn parse_approve_rejects_unparseable_deadline() {
        // 解析期必须拒绝引擎无法换算的 deadline：否则运行期 `deadline_ms()==None`，
        // 无期限被当成默认（fail-open）。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: approve-bad-deadline
  version: "1.0"
do:
  - review:
      approve:
        approvers: ["alice"]
        deadline: "hello"
"#;
        let errs = parse_and_validate_yaml(yaml).expect_err("unparseable deadline must reject");
        assert!(
            error_kind_naming(&errs, "approve.deadline"),
            "errors: {errs:?}"
        );
    }

    #[test]
    fn parse_approve_accepts_duration_object_deadline() {
        // 官方相对时长对象路径必须仍可用（合成规范化 duration 串后仍可解析）。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: approve-object-deadline
  version: "1.0"
do:
  - review:
      approve:
        approvers: ["alice"]
        deadline:
          minutes: 5
"#;
        let def = parse_and_validate_yaml(yaml).expect("duration object deadline must parse");
        match &def.do_tasks[0].task {
            Task::Approve(a) => assert_eq!(a.deadline.as_deref(), Some("PT5M")),
            other => panic!("expected Approve task, got {other:?}"),
        }
    }

    #[test]
    fn parse_approve_requires_a_seat_source() {
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: approve-no-seats
  version: "1.0"
do:
  - review:
      approve:
        mode: all
"#;
        let errs = parse_and_validate_yaml(yaml).expect_err("seatless approve must reject");
        assert!(
            errs.iter().any(|e| matches!(
                &e.kind,
                ValidationErrorKind::MissingRequiredField(f) if f == "approve seats"
            )),
            "errors: {errs:?}"
        );
    }

    #[test]
    fn parse_approve_on_pass_target_must_exist() {
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: approve-onpass
  version: "1.0"
do:
  - review:
      approve:
        approvers: ["alice"]
        onPass: "ghost"
"#;
        let errs = parse_and_validate_yaml(yaml).expect_err("unknown onPass target must reject");
        assert!(
            errs.iter().any(|e| matches!(
                &e.kind,
                ValidationErrorKind::UnknownTaskReference(t) if t == "ghost"
            )),
            "errors: {errs:?}"
        );
    }

    #[test]
    fn parse_approve_on_reject_target_must_exist() {
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: approve-onreject
  version: "1.0"
do:
  - review:
      approve:
        approvers: ["alice"]
        onReject: "ghost"
"#;
        let errs = parse_and_validate_yaml(yaml).expect_err("unknown onReject target must reject");
        assert!(
            errs.iter().any(|e| matches!(
                &e.kind,
                ValidationErrorKind::UnknownTaskReference(t) if t == "ghost"
            )),
            "errors: {errs:?}"
        );
    }

    #[test]
    fn parse_approve_edges_participate_in_cycle_check() {
        // approve.onPass 是运行期分支目标：approve → switch → approve 构成环，必须被拒绝。
        let yaml = r#"
document:
  dsl: "1.0.3"
  namespace: test
  name: approve-cycle
  version: "1.0"
do:
  - review:
      approve:
        approvers: ["alice"]
        onPass: "route"
  - route:
      switch:
        - condition: "${ true }"
          transition: "review"
"#;
        let errs = parse_and_validate_yaml(yaml).expect_err("approve cycle must reject");
        assert!(
            errs.iter()
                .any(|e| matches!(&e.kind, ValidationErrorKind::CyclicDependency(_))),
            "errors: {errs:?}"
        );
    }
}
