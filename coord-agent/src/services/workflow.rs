// coord-agent: Serverless Workflow 流程引擎 (Workflow Service)
//
// 实现 BaseService trait，提供工作流定义管理与实例执行能力。
// 基于 Coord 核心原语（KV + Txn + Lease + Watch）构建。
//
// 当前状态（Phase D）: 基础工作流定义 CRUD + 实例状态管理。
// 完整的 DSL 解释器和 Saga 补偿执行器为 Phase G 蓝图。
//
// 参见 docs/client-agent-architecture-v3.md §5.9。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::RwLock as ParkingRwLock;
use tokio::sync::watch;

use crate::proxy::AgentInner;
use crate::service::{BaseService, ServiceResult};

// ──── 类型定义 ────

/// 工作流定义
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WorkflowDef {
    pub name: String,
    pub version: String,
    pub dsl_source: Vec<u8>,
    pub dsl_format: String,
    pub description: String,
    pub timeout_secs: u64,
    pub created_at: u64,
    pub updated_at: u64,
}

impl WorkflowDef {
    pub fn new(name: impl Into<String>, dsl_source: Vec<u8>, dsl_format: impl Into<String>) -> Self {
        let now = unix_ts();
        Self { name: name.into(), version: "1.0".into(), dsl_source, dsl_format: dsl_format.into(), description: String::new(), timeout_secs: 0, created_at: now, updated_at: now }
    }
    pub fn storage_key(name: &str) -> Vec<u8> {
        format!("/_workflow/defs/{name}").into_bytes()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowState {
    Pending, Running, Suspended, Completed, Failed, Compensated, Cancelled, TimedOut,
}

impl WorkflowState {
    pub fn is_terminal(&self) -> bool {
        matches!(self, WorkflowState::Completed | WorkflowState::Failed | WorkflowState::Compensated | WorkflowState::Cancelled | WorkflowState::TimedOut)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WorkflowInstance {
    pub instance_id: String,
    pub workflow_name: String,
    pub state: WorkflowState,
    pub current_step: u32,
    pub input: Vec<u8>,
    pub output: Vec<u8>,
    pub error_message: String,
    pub lease_id: i64,
    pub created_at: u64,
    pub updated_at: u64,
}

impl WorkflowInstance {
    pub fn new(instance_id: impl Into<String>, workflow_name: impl Into<String>, input: Vec<u8>) -> Self {
        let now = unix_ts();
        Self { instance_id: instance_id.into(), workflow_name: workflow_name.into(), state: WorkflowState::Pending, current_step: 0, input, output: Vec::new(), error_message: String::new(), lease_id: 0, created_at: now, updated_at: now }
    }
    pub fn storage_key(instance_id: &str) -> Vec<u8> {
        format!("/_workflow/instances/{instance_id}").into_bytes()
    }
}

fn unix_ts() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs()
}

// ──── WorkflowCache ────

pub struct WorkflowCache {
    defs: BTreeMap<String, WorkflowDef>,
    instances: BTreeMap<String, WorkflowInstance>,
}

impl WorkflowCache {
    pub fn new() -> Self { Self { defs: BTreeMap::new(), instances: BTreeMap::new() } }
    pub fn put_def(&mut self, def: WorkflowDef) { self.defs.insert(def.name.clone(), def); }
    pub fn get_def(&self, name: &str) -> Option<&WorkflowDef> { self.defs.get(name) }
    pub fn remove_def(&mut self, name: &str) -> Option<WorkflowDef> { self.defs.remove(name) }
    pub fn list_defs(&self) -> Vec<&WorkflowDef> { self.defs.values().collect() }
    pub fn put_instance(&mut self, inst: WorkflowInstance) { self.instances.insert(inst.instance_id.clone(), inst); }
    pub fn get_instance(&self, id: &str) -> Option<&WorkflowInstance> { self.instances.get(id) }
    pub fn remove_instance(&mut self, id: &str) -> Option<WorkflowInstance> { self.instances.remove(id) }
    pub fn list_instances_by_workflow(&self, wf: &str) -> Vec<&WorkflowInstance> { self.instances.values().filter(|i| i.workflow_name == wf).collect() }
}

// ──── WorkflowService ────

pub struct WorkflowService {
    inner: Arc<AgentInner>,
    cache: ParkingRwLock<WorkflowCache>,
    healthy: ParkingRwLock<bool>,
    shutdown_tx: ParkingRwLock<Option<watch::Sender<()>>>,
}

impl WorkflowService {
    pub const NAME: &'static str = "workflow";

    pub fn new(inner: Arc<AgentInner>) -> Self {
        Self { inner, cache: ParkingRwLock::new(WorkflowCache::new()), healthy: ParkingRwLock::new(false), shutdown_tx: ParkingRwLock::new(None) }
    }

    pub async fn define(&self, def: WorkflowDef) -> ServiceResult<()> {
        let key = WorkflowDef::storage_key(&def.name);
        let value = serde_json::to_vec(&def).map_err(|e| format!("serialize: {e}"))?;
        self.inner.client.kv().put(&key, &value).await.map_err(|e| format!("put: {e}"))?;
        self.cache.write().put_def(def);
        Ok(())
    }

    pub async fn get_definition(&self, name: &str) -> ServiceResult<Option<WorkflowDef>> {
        if let Some(def) = self.cache.read().get_def(name) { return Ok(Some(def.clone())); }
        let key = WorkflowDef::storage_key(name);
        let pairs = self.inner.client.kv().range(&key, &key, 1, 0).await.map_err(|e| format!("range: {e}"))?;
        if let Some((_k, v)) = pairs.into_iter().next() {
            let def: WorkflowDef = serde_json::from_slice(&v).map_err(|e| format!("deserialize: {e}"))?;
            self.cache.write().put_def(def.clone());
            Ok(Some(def))
        } else { Ok(None) }
    }

    pub async fn remove_definition(&self, name: &str) -> ServiceResult<()> {
        let key = WorkflowDef::storage_key(name);
        self.inner.client.kv().delete(&key).await.map_err(|e| format!("delete: {e}"))?;
        self.cache.write().remove_def(name);
        Ok(())
    }

    pub async fn start_instance(&self, inst: WorkflowInstance) -> ServiceResult<()> {
        let key = WorkflowInstance::storage_key(&inst.instance_id);
        let value = serde_json::to_vec(&inst).map_err(|e| format!("serialize: {e}"))?;
        self.inner.client.kv().put(&key, &value).await.map_err(|e| format!("put: {e}"))?;
        self.cache.write().put_instance(inst);
        Ok(())
    }

    pub async fn transition_state(&self, instance_id: &str, expected: WorkflowState, next: WorkflowState) -> ServiceResult<()> {
        let key = WorkflowInstance::storage_key(instance_id);
        let pairs = self.inner.client.kv().range(&key, &key, 1, 0).await.map_err(|e| format!("range: {e}"))?;
        let (_k, v) = pairs.into_iter().next().ok_or_else(|| format!("instance '{instance_id}' not found"))?;
        let mut inst: WorkflowInstance = serde_json::from_slice(&v).map_err(|e| format!("deserialize: {e}"))?;
        if inst.state != expected { return Err(format!("state mismatch: expected {expected:?}, got {:?}", inst.state).into()); }
        inst.state = next;
        inst.updated_at = unix_ts();
        let new_val = serde_json::to_vec(&inst).map_err(|e| format!("serialize: {e}"))?;
        self.inner.client.kv().put(&key, &new_val).await.map_err(|e| format!("put: {e}"))?;
        self.cache.write().put_instance(inst);
        Ok(())
    }

    pub async fn get_instance(&self, id: &str) -> ServiceResult<Option<WorkflowInstance>> {
        if let Some(inst) = self.cache.read().get_instance(id) { return Ok(Some(inst.clone())); }
        let key = WorkflowInstance::storage_key(id);
        let pairs = self.inner.client.kv().range(&key, &key, 1, 0).await.map_err(|e| format!("range: {e}"))?;
        if let Some((_k, v)) = pairs.into_iter().next() {
            let inst: WorkflowInstance = serde_json::from_slice(&v).map_err(|e| format!("deserialize: {e}"))?;
            self.cache.write().put_instance(inst.clone());
            Ok(Some(inst))
        } else { Ok(None) }
    }
}

#[async_trait]
impl BaseService for WorkflowService {
    fn name(&self) -> &'static str { Self::NAME }

    async fn start(&self) -> ServiceResult<()> {
        *self.healthy.write() = true;
        let (tx, mut rx) = watch::channel(());
        *self.shutdown_tx.write() = Some(tx);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = rx.changed() => break,
                    _ = tokio::time::sleep(Duration::from_secs(60)) => {},
                }
            }
        });
        Ok(())
    }

    async fn stop(&self) -> ServiceResult<()> {
        if let Some(tx) = self.shutdown_tx.write().take() { let _ = tx.send(()); }
        *self.healthy.write() = false;
        Ok(())
    }

    fn health_check(&self) -> bool { *self.healthy.read() }
}

impl std::fmt::Debug for WorkflowService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let c = self.cache.read();
        f.debug_struct("WorkflowService").field("defs", &c.defs.len()).field("instances", &c.instances.len()).finish()
    }
}

// ═══════════════════════════════════════════════════════════════════
// Phase G: Serverless Workflow DSL 解释器
// ═══════════════════════════════════════════════════════════════════

/// 动作类型
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActionType {
    /// 本地函数调用: "function:name"
    Function(String),
    /// HTTP 调用: "http:method:url"
    Http { method: String, url: String },
    /// 无操作
    NoOp,
}

impl ActionType {
    pub fn parse(action: &str) -> Self {
        if action.is_empty() {
            return ActionType::NoOp;
        }
        let parts: Vec<&str> = action.splitn(3, ':').collect();
        match parts.as_slice() {
            ["http", method, url] => ActionType::Http {
                method: method.to_uppercase(),
                url: url.to_string(),
            },
            ["function", name] => ActionType::Function(name.to_string()),
            _ => ActionType::Function(action.to_string()),
        }
    }
}

/// 转移类型
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransitionType {
    /// 直接转移到 next_state
    Direct(String),
    /// Switch 条件转移
    Conditional { conditions: BTreeMap<String, String>, default: Option<String> },
    /// 并行分叉
    ParallelFork { branches: Vec<String>, join: Option<String> },
    /// 终止（终端状态）
    Terminal,
}

// ──── DSL 类型 ────

/// 工作流状态定义
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct WorkflowStateDef {
    #[serde(default)]
    pub name: String,
    /// 状态类型: operation | switch | parallel | delay | event | terminate
    #[serde(default, rename = "type")]
    pub state_type: String,
    /// 动作字符串
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action: Option<String>,
    /// 直接后继状态
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "next")]
    pub next_state: Option<String>,
    /// Switch 条件: JSONPath → next_state
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub conditions: BTreeMap<String, String>,
    /// Switch 默认路径
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "default")]
    pub default_next: Option<String>,
    /// Parallel 分支列表
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub branches: Vec<String>,
    /// Parallel join 状态
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "join")]
    pub join_state: Option<String>,
    /// Delay 秒数
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "delay")]
    pub delay_seconds: Option<u64>,
    /// 事件名称（用于 event 状态）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_name: Option<String>,
}

/// 工作流 DSL 定义
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkflowDsl {
    pub name: String,
    #[serde(default = "default_version")]
    pub version: String,
    #[serde(rename = "startState")]
    pub start_state: String,
    #[serde(default)]
    pub states: BTreeMap<String, WorkflowStateDef>,
}

fn default_version() -> String { "1.0".into() }

impl WorkflowDsl {
    /// 从 JSON 字节解析 DSL
    pub fn from_json(data: &[u8]) -> Result<Self, String> {
        let dsl: WorkflowDsl = serde_json::from_slice(data)
            .map_err(|e| format!("invalid DSL JSON: {e}"))?;
        dsl.validate()?;
        Ok(dsl)
    }

    /// 序列化为 JSON
    pub fn to_json(&self) -> Result<Vec<u8>, String> {
        serde_json::to_vec(self).map_err(|e| format!("serialize DSL: {e}"))
    }

    /// 验证 DSL 合法性
    pub fn validate(&self) -> Result<(), String> {
        if self.start_state.is_empty() {
            return Err("startState is required".into());
        }
        if self.states.is_empty() {
            return Err("at least one state is required".into());
        }
        if !self.states.contains_key(&self.start_state) {
            return Err(format!("startState '{}' not found in states", self.start_state));
        }
        // Validate all referenced states exist
        for (name, state) in &self.states {
            if let Some(ref next) = state.next_state {
                if !self.states.contains_key(next) {
                    return Err(format!("state '{name}' references non-existent next state '{next}'"));
                }
            }
            for (_, target) in &state.conditions {
                if !self.states.contains_key(target) {
                    return Err(format!("state '{name}' references non-existent condition target '{target}'"));
                }
            }
            if let Some(ref default) = state.default_next {
                if !self.states.contains_key(default) {
                    return Err(format!("state '{name}' references non-existent default target '{default}'"));
                }
            }
            for branch in &state.branches {
                if !self.states.contains_key(branch) {
                    return Err(format!("state '{name}' references non-existent branch '{branch}'"));
                }
            }
            if let Some(ref join) = state.join_state {
                if !self.states.contains_key(join) {
                    return Err(format!("state '{name}' references non-existent join state '{join}'"));
                }
            }
        }
        Ok(())
    }
}

// ──── 运行时类型 ────

/// 工作流执行上下文
#[derive(Debug, Clone)]
pub struct WorkflowContext {
    pub instance_id: String,
    pub workflow_name: String,
    pub input: Vec<u8>,
    /// 运行时变量（JSON values）
    pub variables: BTreeMap<String, serde_json::Value>,
    pub current_state: String,
    pub step_count: u64,
    pub max_steps: u64,
    /// 并行分支待执行队列
    pub pending_branches: Vec<String>,
}

/// 解释器执行结果
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InterpreterResult {
    /// 已转移到新状态
    Transitioned(String),
    /// 已分叉，需执行多个分支
    Forked { branches: Vec<String>, join: Option<String> },
    /// 等待外部事件
    WaitingForEvent(String),
    /// 工作流已完成
    Completed,
}

// ──── WorkflowInterpreter ────

/// Serverless Workflow DSL 解释器
///
/// 状态机解释器，支持 Operation/Delay/Event/Switch/Parallel/Terminate 状态。
pub struct WorkflowInterpreter {
    dsl: WorkflowDsl,
}

impl WorkflowInterpreter {
    pub fn new(dsl: WorkflowDsl) -> Self {
        Self { dsl }
    }

    /// 执行一步状态转换
    pub fn step(&self, ctx: &mut WorkflowContext) -> Result<InterpreterResult, String> {
        if ctx.step_count >= ctx.max_steps {
            return Err(format!("max steps ({}) exceeded", ctx.max_steps));
        }
        ctx.step_count += 1;

        // If no current state, start from DSL startState
        if ctx.current_state.is_empty() {
            ctx.current_state = self.dsl.start_state.clone();
        }

        let state = self.dsl.states.get(&ctx.current_state)
            .ok_or_else(|| format!("state '{}' not found in DSL", ctx.current_state))?;

        match state.state_type.as_str() {
            "operation" => self.handle_operation(state, ctx),
            "switch" => self.handle_switch(state, ctx),
            "parallel" => self.handle_parallel(state, ctx),
            "delay" => self.handle_delay(state, ctx),
            "event" => self.handle_event(state, ctx),
            "terminate" => Ok(InterpreterResult::Completed),
            _ => Err(format!("unknown state type '{}'", state.state_type)),
        }
    }

    /// 运行工作流直到完成或阻塞
    pub fn run(&self, ctx: &mut WorkflowContext) -> Result<InterpreterResult, String> {
        if ctx.current_state.is_empty() {
            ctx.current_state = self.dsl.start_state.clone();
        }

        loop {
            match self.step(ctx)? {
                InterpreterResult::Completed => return Ok(InterpreterResult::Completed),
                InterpreterResult::WaitingForEvent(e) => return Ok(InterpreterResult::WaitingForEvent(e)),
                InterpreterResult::Forked { branches, .. } => {
                    // Execute branches sequentially in this simple interpreter
                    for branch in &branches {
                        ctx.current_state = branch.clone();
                        loop {
                            match self.step(ctx)? {
                                InterpreterResult::Transitioned(_) => continue,
                                InterpreterResult::Completed => break,
                                InterpreterResult::Forked { .. } => continue,
                                other => return Ok(other),
                            }
                        }
                    }
                }
                InterpreterResult::Transitioned(_) => continue,
            }
        }
    }

    fn handle_operation(&self, state: &WorkflowStateDef, ctx: &mut WorkflowContext) -> Result<InterpreterResult, String> {
        // Execute action
        if let Some(ref action) = state.action {
            let action_type = ActionType::parse(action);
            ctx.variables.insert("_last_action".to_string(),
                serde_json::Value::String(format!("{:?}", action_type)));
        }

        match &state.next_state {
            Some(next) => {
                ctx.current_state = next.clone();
                Ok(InterpreterResult::Transitioned(next.clone()))
            }
            None => Ok(InterpreterResult::Completed),
        }
    }

    fn handle_switch(&self, state: &WorkflowStateDef, ctx: &mut WorkflowContext) -> Result<InterpreterResult, String> {
        // Evaluate conditions (simple JSONPath-like matching)
        for (condition, target) in &state.conditions {
            if self.eval_condition(condition, ctx) {
                ctx.current_state = target.clone();
                return Ok(InterpreterResult::Transitioned(target.clone()));
            }
        }
        // Default path
        if let Some(ref default) = state.default_next {
            ctx.current_state = default.clone();
            Ok(InterpreterResult::Transitioned(default.clone()))
        } else {
            Err("no condition matched and no default path".into())
        }
    }

    fn handle_parallel(&self, state: &WorkflowStateDef, ctx: &mut WorkflowContext) -> Result<InterpreterResult, String> {
        if state.branches.is_empty() {
            return Err("parallel state has no branches".into());
        }
        let join = state.join_state.clone();
        let result = InterpreterResult::Forked {
            branches: state.branches.clone(),
            join,
        };
        // Start first branch immediately
        if let Some(first) = state.branches.first() {
            ctx.current_state = first.clone();
        }
        Ok(result)
    }

    fn handle_delay(&self, state: &WorkflowStateDef, ctx: &mut WorkflowContext) -> Result<InterpreterResult, String> {
        let delay = state.delay_seconds.unwrap_or(0);
        if delay > 0 {
            // In a real implementation, this would be async
            // For tests, we just proceed
        }
        match &state.next_state {
            Some(next) => {
                ctx.current_state = next.clone();
                Ok(InterpreterResult::Transitioned(next.clone()))
            }
            None => Ok(InterpreterResult::Completed),
        }
    }

    fn handle_event(&self, state: &WorkflowStateDef, _ctx: &mut WorkflowContext) -> Result<InterpreterResult, String> {
        let event_name = state.event_name.clone().unwrap_or_else(|| "unknown".into());
        Ok(InterpreterResult::WaitingForEvent(event_name))
    }

    /// 简单的条件求值：支持 "$.key == value" 格式
    fn eval_condition(&self, condition: &str, ctx: &WorkflowContext) -> bool {
        // Parse "$.key == value"
        let cond = condition.trim();
        if let Some((path, expected)) = cond.split_once("==") {
            let path = path.trim().trim_start_matches("$.");
            let expected = expected.trim().trim_matches('"').trim();
            if let Some(val) = ctx.variables.get(path) {
                match val {
                    serde_json::Value::Number(n) => {
                        if let Ok(exp_num) = expected.parse::<i64>() {
                            return n.as_i64() == Some(exp_num) || n.as_f64() == Some(expected.parse::<f64>().unwrap_or(0.0));
                        }
                    }
                    serde_json::Value::String(s) => return s == expected,
                    serde_json::Value::Bool(b) => {
                        if let Ok(exp_bool) = expected.parse::<bool>() {
                            return *b == exp_bool;
                        }
                    }
                    _ => {}
                }
            }
        }
        false
    }
}

// ──── tests ────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_def_creation() {
        let d = WorkflowDef::new("wf", vec![], "json");
        assert_eq!(d.name, "wf");
    }

    #[test]
    fn test_state_terminal() {
        assert!(WorkflowState::Completed.is_terminal());
        assert!(!WorkflowState::Pending.is_terminal());
    }

    #[test]
    fn test_instance_creation() {
        let i = WorkflowInstance::new("i1", "wf", vec![]);
        assert_eq!(i.state, WorkflowState::Pending);
    }

    #[test]
    fn test_cache_ops() {
        let mut c = WorkflowCache::new();
        c.put_def(WorkflowDef::new("a", vec![], "json"));
        assert!(c.get_def("a").is_some());
        c.put_instance(WorkflowInstance::new("i1", "a", vec![]));
        assert!(c.get_instance("i1").is_some());
        assert_eq!(c.list_instances_by_workflow("a").len(), 1);
    }
}
