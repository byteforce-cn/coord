// coord-core/workflow/validate.rs
// 语义校验器 —— Phase 2: RawWorkflowDef → WorkflowDefinition
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
    AuthConfig, BackoffStrategy, CallTask, CallType, CatchClause, ConstantsConfig, Document,
    DoTask, EmitEvent, EmitTask, EndTask, ErrorDef, EventFilter, ExportConfig, ForEachTask,
    ForkBranch, ForkTask, FunctionDef, InputConfig, JitterConfig, ListenTask, NamedTask,
    OutputConfig, RaiseTask, RetryPolicy, RunTask, ScheduleConfig, SecretsConfig, SetTask,
    Span, SwitchCondition, SwitchTask, Task, TaskMeta, TimeoutConfig, TryCatchTask,
    UseComponents, ValidationError, ValidationErrorKind, WaitTask, WorkflowDefinition,
    WorkflowRef,
};

use super::parser::{RawFunctionDef, RawNamedTask, RawRetryPolicy, RawTimeoutConfig, RawUseComponents, RawWorkflowDef};

// ─── 校验器 ───

/// 语义校验器
pub struct Validator {
    errors: Vec<ValidationError>,
    /// 收集所有已知的任务名（用于引用校验）
    task_names: HashSet<String>,
    /// 收集所有已注册的 function 名
    function_names: HashSet<String>,
    /// use.retries 已解析映射（任务级 `retry: <name>` 引用解析）
    retry_refs: HashMap<String, RetryPolicy>,
}

impl Validator {
    /// 执行校验，将 RawWorkflowDef 转为 WorkflowDefinition
    pub fn validate(raw: RawWorkflowDef) -> Result<WorkflowDefinition, Vec<ValidationError>> {
        let mut v = Validator {
            errors: Vec::new(),
            task_names: HashSet::new(),
            function_names: HashSet::new(),
            retry_refs: HashMap::new(),
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
        for raw_task in &raw.tasks {
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
        v.check_task_refs(&named_tasks);

        // Step 5: 校验 use.functions 引用
        v.check_function_refs(&named_tasks);

        // Step 6: 循环依赖检测（switch transition 不能形成环）
        v.check_cycles(&named_tasks);

        // Step 7: 收集任务级标准字段（if/input/output/export/retry/timeout）
        let mut task_meta: HashMap<String, TaskMeta> = HashMap::new();
        v.collect_task_meta(&raw.tasks, &mut task_meta);

        // Step 8: 解析 input（schema / from / default）
        let input = raw.input.map(|rv| {
            InputConfig {
                schema: rv.value.get("schema").and_then(|v| v.as_str()).map(String::from),
                from: rv.value.get("from").and_then(|v| v.as_str()).map(String::from),
                default: rv.value.get("default").cloned(),
            }
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
                    as_expr: ov.value.get("as").and_then(|v| v.as_str()).map(String::from),
                    schema: ov.value.get("schema").and_then(|v| v.as_str()).map(String::from),
                });
            }
            if let Some(tv) = ext.timeout {
                timeout = Some(TimeoutConfig {
                    after: tv.value.get("after").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                });
            }
            if let Some(sv) = ext.schedule {
                schedule = ScheduleConfig {
                    every: sv.value.get("every").and_then(|v| v.as_str()).map(String::from),
                    cron: sv.value.get("cron").and_then(|v| v.as_str()).map(String::from),
                    after: sv.value.get("after").and_then(|v| v.as_str()).map(String::from),
                    on: sv.value.get("on").map(|v| {
                        EventFilter {
                            event_type: v.get("type").and_then(|v| v.as_str()).map(String::from),
                            source: v.get("source").and_then(|v| v.as_str()).map(String::from),
                            subject: v.get("subject").and_then(|v| v.as_str()).map(String::from),
                        }
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
                        a.iter().filter_map(|k| k.as_str().map(String::from)).collect()
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
            Value::Object(map) if map.contains_key("do") => self.parse_do(raw, map),
            Value::Object(map) if map.contains_key("switch") => self.parse_switch(raw, map),
            Value::Object(map) if map.contains_key("fork") => self.parse_fork(raw, map),
            Value::Object(map) if map.contains_key("for") => self.parse_for_each(raw, map),
            Value::Object(map) if map.contains_key("wait") => self.parse_wait(raw, map),
            Value::Object(map) if map.contains_key("listen") => self.parse_listen(raw, map),
            Value::Object(map) if map.contains_key("emit") => self.parse_emit(raw, map),
            Value::Object(map) if map.contains_key("set") => self.parse_set(raw, map),
            Value::Object(map) if map.contains_key("raise") => self.parse_raise(raw, map),
            Value::Object(map) if map.contains_key("try") => self.parse_try_catch(raw, map),
            Value::Object(map) if map.contains_key("run") => self.parse_run(raw, map),
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

    fn parse_call(&mut self, _raw: &RawNamedTask, map: &serde_json::Map<String, Value>) -> Option<Task> {
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
                            if let Some(func_name) = with_obj.get("function").and_then(|f| f.as_str()) {
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
        Some(Task::Call(CallTask { call: call_type, with }))
    }

    fn parse_do(&mut self, raw: &RawNamedTask, map: &serde_json::Map<String, Value>) -> Option<Task> {
        let do_val = &map["do"];
        let tasks = self.parse_sub_tasks(do_val, raw.span);
        Some(Task::Do(DoTask { tasks }))
    }

    fn parse_switch(&mut self, raw: &RawNamedTask, map: &serde_json::Map<String, Value>) -> Option<Task> {
        let switch_val = &map["switch"];
        let conditions = match switch_val {
            Value::Array(arr) => {
                arr.iter()
                    .filter_map(|cond| self.parse_switch_condition(cond, raw.span))
                    .collect()
            }
            _ => {
                self.errors.push(ValidationError {
                    span: Some(raw.span),
                    kind: ValidationErrorKind::TypeMismatch("switch must be an array".to_string()),
                    message: "switch must be an array of conditions".to_string(),
                });
                return None;
            }
        };

        Some(Task::Switch(SwitchTask {
            conditions,
            default_condition: None, // defaultCondition 在 conditions 数组中
        }))
    }

    fn parse_switch_condition(&mut self, cond: &Value, span: Span) -> Option<SwitchCondition> {
        let obj = match cond.as_object() {
            Some(o) => o,
            None => return None,
        };

        // condition (optional — defaultCondition has no condition)
        let condition = obj.get("condition").and_then(|v| v.as_str()).map(String::from);

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
                kind: ValidationErrorKind::MissingRequiredField("transition or defaultCondition".into()),
                message: "switch condition missing transition target".to_string(),
            });
            return None;
        };

        Some(SwitchCondition {
            condition,
            transition,
        })
    }

    fn parse_fork(&mut self, raw: &RawNamedTask, map: &serde_json::Map<String, Value>) -> Option<Task> {
        let fork_val = &map["fork"];
        let branches = match fork_val {
            Value::Array(arr) => {
                arr.iter()
                    .filter_map(|b| self.parse_fork_branch(b, raw.span))
                    .collect()
            }
            Value::Object(obj) if obj.contains_key("branches") => {
                if let Some(branches_arr) = obj["branches"].as_array() {
                    branches_arr
                        .iter()
                        .filter_map(|b| self.parse_fork_branch(b, raw.span))
                        .collect()
                } else {
                    Vec::new()
                }
            }
            _ => Vec::new(),
        };

        let compete = map
            .get("compete")
            .and_then(|v| v.as_bool());

        Some(Task::Fork(ForkTask { branches, compete }))
    }

    fn parse_fork_branch(&mut self, branch: &Value, span: Span) -> Option<ForkBranch> {
        let obj = branch.as_object()?;
        let name = obj.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let tasks = if let Some(do_val) = obj.get("do") {
            self.parse_sub_tasks(do_val, span)
        } else if let Some(tasks_val) = obj.get("tasks") {
            self.parse_sub_tasks(tasks_val, span)
        } else {
            Vec::new()
        };
        Some(ForkBranch { name, tasks })
    }

    fn parse_for_each(&mut self, raw: &RawNamedTask, map: &serde_json::Map<String, Value>) -> Option<Task> {
        let for_val = &map["for"];
        let obj = for_val.as_object()?;

        let input = obj.get("input").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let iteration = obj.get("iteration").and_then(|v| v.as_str()).unwrap_or("item").to_string();
        let tasks = if let Some(do_val) = obj.get("do") {
            self.parse_sub_tasks(do_val, raw.span)
        } else if let Some(tasks_val) = obj.get("tasks") {
            self.parse_sub_tasks(tasks_val, raw.span)
        } else {
            Vec::new()
        };

        Some(Task::ForEach(ForEachTask {
            input,
            iteration,
            tasks,
        }))
    }

    fn parse_wait(&mut self, _raw: &RawNamedTask, map: &serde_json::Map<String, Value>) -> Option<Task> {
        let wait_val = &map["wait"];
        let wait_str = match wait_val {
            Value::String(s) => s.clone(),
            Value::Number(_) => wait_val.to_string(),
            _ => String::new(),
        };
        Some(Task::Wait(WaitTask { wait: wait_str }))
    }

    fn parse_listen(&mut self, _raw: &RawNamedTask, map: &serde_json::Map<String, Value>) -> Option<Task> {
        let listen_val = &map["listen"];
        let obj = listen_val.as_object();

        let event_filter = if let Some(obj) = obj {
            EventFilter {
                event_type: obj.get("type").and_then(|v| v.as_str()).map(String::from),
                source: obj.get("source").and_then(|v| v.as_str()).map(String::from),
                subject: obj.get("subject").and_then(|v| v.as_str()).map(String::from),
            }
        } else if let Some(s) = listen_val.as_str() {
            EventFilter {
                event_type: Some(s.to_string()),
                source: None,
                subject: None,
            }
        } else {
            EventFilter {
                event_type: None,
                source: None,
                subject: None,
            }
        };

        Some(Task::Listen(ListenTask { listen: event_filter }))
    }

    fn parse_emit(&mut self, _raw: &RawNamedTask, map: &serde_json::Map<String, Value>) -> Option<Task> {
        let emit_val = &map["emit"];
        let obj = emit_val.as_object();

        let (event_type, source, data) = if let Some(obj) = obj {
            (
                obj.get("type").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                obj.get("source").and_then(|v| v.as_str()).map(String::from),
                obj.get("data").cloned(),
            )
        } else {
            (String::new(), None, None)
        };

        Some(Task::Emit(EmitTask {
            emit: EmitEvent {
                event_type,
                source,
                data,
            },
        }))
    }

    fn parse_set(&mut self, _raw: &RawNamedTask, map: &serde_json::Map<String, Value>) -> Option<Task> {
        let set_val = &map["set"];
        let obj = set_val.as_object();

        let (variable, value) = if let Some(obj) = obj {
            (
                obj.get("variable").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                obj.get("value").and_then(|v| v.as_str()).unwrap_or("").to_string(),
            )
        } else {
            (String::new(), String::new())
        };

        Some(Task::Set(SetTask { variable, value }))
    }

    fn parse_raise(&mut self, _raw: &RawNamedTask, map: &serde_json::Map<String, Value>) -> Option<Task> {
        let raise_val = &map["raise"];
        let obj = raise_val.as_object();

        let error_def = if let Some(obj) = obj {
            ErrorDef {
                r#type: obj.get("type").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                title: obj.get("title").and_then(|v| v.as_str()).unwrap_or("").to_string(),
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

    fn parse_try_catch(&mut self, raw: &RawNamedTask, map: &serde_json::Map<String, Value>) -> Option<Task> {
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

    fn parse_run(&mut self, _raw: &RawNamedTask, map: &serde_json::Map<String, Value>) -> Option<Task> {
        let run_val = &map["run"];
        let obj = run_val.as_object();

        let workflow = if let Some(obj) = obj {
            WorkflowRef {
                namespace: obj.get("namespace").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                name: obj.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                version: obj.get("version").and_then(|v| v.as_str()).unwrap_or("").to_string(),
            }
        } else if let Some(s) = run_val.as_str() {
            // 简写: "namespace::name@version"
            Self::parse_workflow_ref_short(s)
        } else {
            WorkflowRef {
                namespace: String::new(),
                name: String::new(),
                version: String::new(),
            }
        };

        let input = map.get("input").cloned();
        Some(Task::Run(RunTask { workflow, input }))
    }

    /// end 任务 —— 终端任务，无需字段（`end: true` / `end: {}` 均可）
    fn parse_end(&mut self, _raw: &RawNamedTask, _map: &serde_json::Map<String, Value>) -> Option<Task> {
        Some(Task::End(EndTask {}))
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

    /// 解析子任务列表（do/try/catch 中的嵌套任务）
    fn parse_sub_tasks(&mut self, val: &Value, parent_span: Span) -> Vec<NamedTask> {
        match val {
            Value::Array(arr) => {
                arr.iter()
                    .filter_map(|item| {
                        let obj = item.as_object()?;
                        if obj.len() != 1 {
                            self.errors.push(ValidationError {
                                span: Some(parent_span),
                                kind: ValidationErrorKind::TypeMismatch("sub-task key count".to_string()),
                                message: "sub-task must have exactly one key".to_string(),
                            });
                            return None;
                        }
                        let (name, body) = obj.iter().next().unwrap();
                        let raw = RawNamedTask {
                            span: parent_span,
                            name: name.clone(),
                            body: body.clone(),
                        };
                        self.resolve_task(&raw)
                            .map(|task| NamedTask {
                                name: name.clone(),
                                task,
                            })
                    })
                    .collect()
            }
            _ => Vec::new(),
        }
    }

    // ─── 引用校验 ───

    /// 校验 switch transition 目标存在性
    fn check_task_refs(&mut self, tasks: &[NamedTask]) {
        for task in tasks {
            match &task.task {
                Task::Switch(switch) => {
                    for cond in &switch.conditions {
                        // defaultCondition 不是显式目标（它就是当前分支）
                        if cond.condition.is_none() {
                            continue;
                        }
                        if !self.task_names.contains(&cond.transition) {
                            self.errors.push(ValidationError {
                                span: None,
                                kind: ValidationErrorKind::UnknownTaskReference(cond.transition.clone()),
                                message: format!(
                                    "switch in '{}' references unknown task '{}'",
                                    task.name, cond.transition
                                ),
                            });
                        }
                    }
                }
                Task::Do(do_task) => {
                    self.check_task_refs(&do_task.tasks);
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
        // 构建 transition 图
        let mut edges: HashMap<&str, Vec<&str>> = HashMap::new();
        for task in tasks {
            if let Task::Switch(switch) = &task.task {
                for cond in &switch.conditions {
                    if cond.condition.is_none() {
                        continue; // defaultCondition 不形成边
                    }
                    edges
                        .entry(task.name.as_str())
                        .or_default()
                        .push(cond.transition.as_str());
                }
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
                        message: format!("cyclic dependency detected in switch transitions"),
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
                    // 找到环
                    let cycle_start = path.iter().position(|&x| x == neighbor).unwrap();
                    let cycle: Vec<&str> = path[cycle_start..].to_vec();
                    return Some(cycle);
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
            rets
                .into_iter()
                .map(|(name, raw_retry)| {
                    let policy = self.resolve_retry_policy(raw_retry);
                    (name, policy)
                })
                .collect()
        });

        let timeouts = raw.timeouts.map(|tos| {
            tos
                .into_iter()
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
                with: obj.get("with").cloned(),            }
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
                backoff: obj.get("backoff").and_then(|v| v.as_str()).map(|s| {
                    match s {
                        "constant" => BackoffStrategy::Constant,
                        "linear" => BackoffStrategy::Linear,
                        "exponential" => BackoffStrategy::Exponential,
                        _ => BackoffStrategy::Constant,
                    }
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
            return Some(TimeoutConfig { after: s.to_string() });
        }
        if let Some(ao) = a.as_object() {
            if let Some(secs) = ao.get("seconds").and_then(|x| x.as_u64()) {
                return Some(TimeoutConfig { after: format!("PT{secs}S") });
            }
        }
    }
    None
}

// ─── 测试 ───

#[cfg(test)]
mod tests {
    use super::*;

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
                assert_eq!(s.conditions[0].condition.as_deref(), Some("${ .amount > 10000 }"));
                assert_eq!(s.conditions[0].transition, "seniorApproval");
                // defaultCondition（第三个条件，无 condition 字段）
                assert_eq!(s.conditions[2].condition, None);
                assert_eq!(s.conditions[2].transition, "directorApproval");
            }
            _ => panic!("expected Switch task"),
        }
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
        assert!(use_comp.functions.as_ref().unwrap().contains_key("sendNotification"));
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
        assert!(errors.iter().any(|e| matches!(e.kind, ValidationErrorKind::UnknownFunctionReference(_))));
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
        assert!(errors.iter().any(|e| matches!(e.kind, ValidationErrorKind::UnknownTaskReference(_))));
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
        assert!(errors.iter().any(|e| matches!(e.kind, ValidationErrorKind::DuplicateTaskName(_))));
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
        assert!(errors.iter().any(|e| matches!(e.kind, ValidationErrorKind::CyclicDependency(_))));
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
        assert!(errors.iter().any(|e| matches!(e.kind, ValidationErrorKind::UnknownTaskType)));
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
        assert_eq!(def.secrets.keys.as_ref().unwrap(), &vec!["adminPass".to_string()]);
        assert_eq!(def.constants.values.as_ref().unwrap().get("region").unwrap(), &Value::String("cn-north".into()));
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
}
