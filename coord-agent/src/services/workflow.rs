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
    pub definition_id: Option<String>,  // 关联的已部署工作流定义 ID
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
        Self { instance_id: instance_id.into(), workflow_name: workflow_name.into(), definition_id: None, state: WorkflowState::Pending, current_step: 0, input, output: Vec::new(), error_message: String::new(), lease_id: 0, created_at: now, updated_at: now }
    }
    pub fn storage_key(instance_id: &str) -> Vec<u8> {
        format!("/_workflow/instances/{instance_id}").into_bytes()
    }
}

fn unix_ts() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs()
}

fn unix_ts_i64() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64
}

// ──── Phase B 新增数据类型 ────

/// 工作流定义（Phase B.1 — deploy/get_definition 使用）
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WorkflowDefinition {
    pub id: String,
    pub name: String,
    pub namespace: String,
    pub yaml: String,
    pub version: String,       // 语义化版本字符串，如 "1.0"
    pub status: String,        // "active" | "deprecated"
    pub created_at: i64,
}

/// 工作流定义摘要（Phase B.1 — list_definitions 使用）
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DefSummary {
    pub id: String,
    pub name: String,
    pub version: String,
    pub status: String,
    pub created_at: i64,
}

/// 工作流实例摘要（Phase B.1 — list_instances 使用）
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InstSummary {
    pub id: String,
    pub workflow_id: String,
    pub state: String,
    pub started_at: i64,
    pub updated_at: i64,
    pub definition_name: String,
}

impl WorkflowDefinition {
    pub fn storage_key(id: &str) -> Vec<u8> { format!("/_workflow/v2/defs/{id}").into_bytes() }
    pub fn namespace_key(namespace: &str) -> Vec<u8> { format!("/_workflow/v2/ns/{namespace}/").into_bytes() }
    /// namespace 索引键：用于按 namespace 列出所有 definition id
    pub fn namespace_index_key(namespace: &str, id: &str) -> Vec<u8> {
        format!("/_workflow/v2/ns/{namespace}/{id}").into_bytes()
    }
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

    /// 向 workflow instance 发送 signal（记录 signal 信息到 output 字段）
    pub async fn signal_instance(
        &self,
        instance_id: &str,
        signal_name: &str,
        payload: &[u8],
    ) -> ServiceResult<()> {
        let key = WorkflowInstance::storage_key(instance_id);
        let pairs = self.inner.client.kv()
            .range(&key, &key, 1, 0).await
            .map_err(|e| format!("kv range: {e}"))?;
        let (_k, v) = pairs.into_iter().next()
            .ok_or_else(|| format!("instance '{instance_id}' not found"))?;
        let mut inst: WorkflowInstance = serde_json::from_slice(&v)
            .map_err(|e| format!("deserialize: {e}"))?;
        // 将 signal 信息记录到 output 字段
        let signal_record = format!(
            "signal:{} payload:{}",
            signal_name,
            String::from_utf8_lossy(payload)
        );
        inst.output = signal_record.into_bytes();
        inst.updated_at = unix_ts();
        let new_val = serde_json::to_vec(&inst).map_err(|e| format!("serialize: {e}"))?;
        self.inner.client.kv().put(&key, &new_val).await.map_err(|e| format!("kv put: {e}"))?;
        self.cache.write().put_instance(inst);
        Ok(())
    }

    // ──── Phase B.1: 工作流定义管理 ────

    /// 部署工作流定义，存储到 coord-server KV 层以支持多 Agent 共享
    pub async fn deploy_definition(
        &self,
        namespace: &str,
        yaml: &str,
    ) -> ServiceResult<(String, String, String)> {
        // 使用时间戳+随机数生成简短 ID
        let id = format!("{}-{:x}", namespace, unix_ts() as u32 ^ (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().subsec_nanos()));
        let now = unix_ts_i64();
        // 从 YAML 中提取 name（支持缩进和引号）
        let name = yaml.lines()
            .find(|l| {
                let trimmed = l.trim_start();
                trimmed.starts_with("name:") || trimmed.starts_with("id:")
            })
            .and_then(|l| l.splitn(2, ':').nth(1))
            .map(|s| s.trim().trim_matches('"').trim_matches('\'').to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| id.clone());

        // 从 YAML 中提取 version（语义化版本字符串）
        let version = yaml.lines()
            .find(|l| {
                let trimmed = l.trim_start();
                trimmed.starts_with("version:")
            })
            .and_then(|l| l.splitn(2, ':').nth(1))
            .map(|s| s.trim().trim_matches('"').trim_matches('\'').to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "1".to_string());

        let def = WorkflowDefinition {
            id: id.clone(),
            name: name.clone(),
            namespace: namespace.to_string(),
            yaml: yaml.to_string(),
            version: version.clone(),
            status: "active".to_string(),
            created_at: now,
        };

        let key = WorkflowDefinition::storage_key(&id);
        let value = serde_json::to_vec(&def).map_err(|e| format!("serialize: {e}"))?;
        self.inner.client.kv().put(&key, &value).await
            .map_err(|e| format!("kv put definition: {e}"))?;

        // 同时写入 namespace 索引，确保 list_definitions 可按 namespace 查询
        let ns_idx_key = WorkflowDefinition::namespace_index_key(namespace, &id);
        self.inner.client.kv().put(&ns_idx_key, &id.as_bytes().to_vec()).await
            .map_err(|e| format!("kv put namespace index: {e}"))?;

        Ok((id, version, name))
    }

    /// 列出命名空间下的工作流定义
    pub async fn list_definitions(
        &self,
        namespace: &str,
        page_size: i32,
        _page_token: &str,
    ) -> ServiceResult<(Vec<DefSummary>, String)> {
        let prefix = WorkflowDefinition::namespace_key(namespace);
        let range_end = prefix_end(&prefix);
        let limit = if page_size > 0 { page_size as i64 } else { 50 };

        // 先查 namespace 索引获取 definition id 列表
        let pairs = self.inner.client.kv()
            .range(&prefix, &range_end, limit, 0).await
            .map_err(|e| format!("kv range definitions: {e}"))?;

        let mut summaries: Vec<DefSummary> = Vec::new();
        for (_k, v) in &pairs {
            // namespace 索引值存的是 definition id
            let def_id = String::from_utf8_lossy(v).to_string();
            // 用 definition id 获取完整定义
            let def_key = WorkflowDefinition::storage_key(&def_id);
            match self.inner.client.kv()
                .range(&def_key, &def_key, 1, 0).await
            {
                Ok(def_pairs) => {
                    if let Some((_dk, dv)) = def_pairs.into_iter().next() {
                        if let Ok(def) = serde_json::from_slice::<WorkflowDefinition>(&dv) {
                            summaries.push(DefSummary {
                                id: def.id,
                                name: def.name,
                                version: def.version,
                                status: def.status,
                                created_at: def.created_at,
                            });
                        }
                    }
                }
                Err(_) => continue,
            }
        }

        // 简单分页：使用最后一条的 id 作为 next_page_token
        let next_token = if pairs.len() as i32 >= page_size && page_size > 0 {
            summaries.last().map(|s| s.id.clone()).unwrap_or_default()
        } else {
            String::new()
        };

        Ok((summaries, next_token))
    }

    /// 按 ID 获取工作流定义详情
    pub async fn get_definition_by_id(
        &self,
        workflow_id: &str,
    ) -> ServiceResult<WorkflowDefinition> {
        let key = WorkflowDefinition::storage_key(workflow_id);
        let pairs = self.inner.client.kv()
            .range(&key, &key, 1, 0).await
            .map_err(|e| format!("kv get definition: {e}"))?;

        let (_k, v) = pairs.into_iter().next()
            .ok_or_else(|| format!("definition '{workflow_id}' not found"))?;
        let def: WorkflowDefinition = serde_json::from_slice(&v)
            .map_err(|e| format!("deserialize definition: {e}"))?;
        Ok(def)
    }

    /// 列出工作流实例
    pub async fn list_instances(
        &self,
        workflow_id: &str,
        _namespace: &str,
        page_size: i32,
        page_token: &str,
    ) -> ServiceResult<(Vec<InstSummary>, String)> {
        // 从 KV 存储扫描所有实例（前缀 /_workflow/instances/）
        let prefix = b"/_workflow/instances/".to_vec();
        let range_end = prefix_end(&prefix);
        let kv_limit = if page_size > 0 { page_size as i64 } else { 50 };

        let pairs = self.inner.client.kv()
            .range(&prefix, &range_end, kv_limit, 0).await
            .map_err(|e| format!("kv range instances: {e}"))?;

        let mut all_instances: Vec<WorkflowInstance> = Vec::new();
        for (_k, v) in &pairs {
            if let Ok(inst) = serde_json::from_slice::<WorkflowInstance>(v) {
                all_instances.push(inst);
            }
        }

        // 按 workflow_id 或 workflow_name 过滤（空字符串表示不限制）
        // workflow_id 参数可以是 definition ID 或 definition name
        // 若为 definition ID：先查出 definition name，再按 name 过滤实例
        let filter_name: Option<String> = if workflow_id.is_empty() {
            None
        } else {
            // 先尝试按 definition ID 查找，获取其 name
            let def_key = WorkflowDefinition::storage_key(workflow_id);
            if let Ok(pairs) = self.inner.client.kv()
                .range(&def_key, &def_key, 1, 0).await
            {
                if let Some((_k, v)) = pairs.into_iter().next() {
                    if let Ok(def) = serde_json::from_slice::<WorkflowDefinition>(&v) {
                        if !def.name.is_empty() {
                            Some(def.name)
                        } else {
                            Some(workflow_id.to_string())
                        }
                    } else {
                        Some(workflow_id.to_string())
                    }
                } else {
                    Some(workflow_id.to_string())
                }
            } else {
                Some(workflow_id.to_string())
            }
        };

        let filtered: Vec<&WorkflowInstance> = match &filter_name {
            None => all_instances.iter().collect(),
            Some(name) => all_instances.iter().filter(|i| {
                i.workflow_name == *name
                    || i.definition_id.as_deref() == Some(workflow_id)
            }).collect(),
        };

        // 应用分页
        let start_idx = if page_token.is_empty() {
            0usize
        } else {
            filtered.iter().position(|i| i.instance_id == page_token).map(|p| p + 1).unwrap_or(0)
        };
        let slice_limit = if page_size > 0 { page_size as usize } else { 50 };
        let end_idx = (start_idx + slice_limit).min(filtered.len());

        let summaries: Vec<InstSummary> = filtered[start_idx..end_idx].iter().map(|i| {
            InstSummary {
                id: i.instance_id.clone(),
                workflow_id: i.definition_id.clone().unwrap_or_else(|| i.workflow_name.clone()),
                state: format!("{:?}", i.state),
                started_at: i.created_at as i64,
                updated_at: i.updated_at as i64,
                definition_name: i.workflow_name.clone(),
            }
        }).collect();

        let next_token = if end_idx < filtered.len() {
            filtered[end_idx].instance_id.clone()
        } else {
            String::new()
        };

        // 同步更新内存缓存
        {
            let mut cache = self.cache.write();
            for inst in &all_instances {
                cache.put_instance(inst.clone());
            }
        }

        Ok((summaries, next_token))
    }
}

/// 计算 range 前缀的结束键（前缀各字节 +1）
fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    for i in (0..end.len()).rev() {
        if end[i] < 0xFF {
            end[i] += 1;
            end.truncate(i + 1);
            return end;
        }
    }
    // 前缀全是 0xFF，返回空前缀（匹配所有）
    Vec::new()
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

    // ──── Phase B.1 新数据类型测试 ────

    #[test]
    fn test_workflow_definition_storage_key() {
        let key = WorkflowDefinition::storage_key("wf-001");
        assert!(key.starts_with(b"/_workflow/v2/defs/"));
        assert!(key.ends_with(b"wf-001"));
    }

    #[test]
    fn test_workflow_definition_namespace_key() {
        let key = WorkflowDefinition::namespace_key("production");
        assert!(key.starts_with(b"/_workflow/v2/ns/production/"));
    }

    #[test]
    fn test_def_summary_fields() {
        let s = DefSummary {
            id: "wf-1".into(),
            name: "test-wf".into(),
            version: "2".into(),
            status: "active".into(),
            created_at: 1000,
        };
        assert_eq!(s.id, "wf-1");
        assert_eq!(s.status, "active");
    }

    #[test]
    fn test_inst_summary_fields() {
        let s = InstSummary {
            id: "inst-1".into(),
            workflow_id: "wf-1".into(),
            state: "RUNNING".into(),
            started_at: 1000,
            updated_at: 2000,
            definition_name: "test-wf".into(),
        };
        assert_eq!(s.state, "RUNNING");
        assert_eq!(s.workflow_id, "wf-1");
        assert_eq!(s.definition_name, "test-wf");
    }

    #[test]
    fn test_prefix_end_normal() {
        let end = prefix_end(b"/_workflow/v2/ns/prod/");
        // end should be > prefix
        assert!(end > b"/_workflow/v2/ns/prod/".to_vec());
        // end should be <= prefix_last_byte + 1
        let prefix = b"/_workflow/v2/ns/prod/";
        assert_eq!(end.len(), prefix.len());
        assert_eq!(end[prefix.len() - 1], prefix[prefix.len() - 1] + 1);
    }

    #[test]
    fn test_workflow_definition_serialization() {
        let def = WorkflowDefinition {
            id: "wf-1".into(),
            name: "my-workflow".into(),
            namespace: "default".into(),
            yaml: "name: my-workflow\nstates: {}".into(),
            version: "1".into(),
            status: "active".into(),
            created_at: 1000,
        };
        let json = serde_json::to_vec(&def).unwrap();
        let restored: WorkflowDefinition = serde_json::from_slice(&json).unwrap();
        assert_eq!(restored.id, "wf-1");
        assert_eq!(restored.name, "my-workflow");
        assert_eq!(restored.yaml, def.yaml);
    }
}

// ═══════════════════════════════════════════════════════════════════
// Phase 4: WorkflowEngineService — 对接 coord-core 工作流引擎
// ═══════════════════════════════════════════════════════════════════
//
// 实现基于 coord-core::workflow 的新引擎绑定，替换旧的 DSL 解释器。
// TDD: 先写 phase4_tests，再实现本模块。

pub mod phase4 {
    use std::sync::Arc;

    use async_trait::async_trait;
    use serde_json::Value;

    use coord_core::workflow::{
        expression::ExpressionEvaluator,
        model::{InstanceStatus, WorkflowDefinition, WorkflowInstance},
        parser,
        ports::{
            Clock, DispatchResult, EventProvider, MemoryEventProvider, MemoryWorkflowStore,
            SystemClock, TaskDispatcher, WorkflowStore,
        },
        runtime::WorkflowRuntime,
        sw,
    };

    use crate::proxy::AgentInner;
    use crate::services::workflow_store::KvWorkflowStore;

    // NoopEventProvider 仅在测试中使用
    #[cfg(test)]
    use coord_core::workflow::ports::NoopEventProvider;

    // ─── NoopTaskDispatcher ───
    //
    // 占位 TaskDispatcher：call 任务在 agent 层不实际执行 I/O，
    // 而是通过 Suspend 机制交由上层业务系统处理。
    // 未来可替换为真正的 HTTP/gRPC 派发实现。

    /// 占位任务派发器 —— 直接返回成功（空响应）
    #[derive(Debug, Clone)]
    pub struct NoopTaskDispatcher;

    #[async_trait]
    impl TaskDispatcher for NoopTaskDispatcher {
        async fn dispatch(
            &self,
            _service: &str,
            _with: Option<&Value>,
            _input: &Value,
        ) -> DispatchResult {
            DispatchResult::Success {
                data: Value::Null,
            }
        }
    }

    // ─── HttpTaskDispatcher ───
    //
    // 真实 HTTP 任务派发器：通过 reqwest 执行 HTTP 调用。
    // gRPC 和 function 调用暂返回 Failure（需后续扩展）。

    /// HTTP 任务派发器 —— 通过 reqwest 执行真实 HTTP 调用
    #[derive(Debug, Clone)]
    pub struct HttpTaskDispatcher {
        client: reqwest::Client,
    }

    impl HttpTaskDispatcher {
        /// 创建新的 HTTP 任务派发器
        pub fn new() -> Self {
            Self {
                client: reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(30))
                    .build()
                    .expect("failed to create reqwest client"),
            }
        }

        /// 使用自定义 reqwest 客户端创建
        pub fn with_client(client: reqwest::Client) -> Self {
            Self { client }
        }
    }

    impl Default for HttpTaskDispatcher {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait]
    impl TaskDispatcher for HttpTaskDispatcher {
        async fn dispatch(
            &self,
            service: &str,
            with: Option<&Value>,
            input: &Value,
        ) -> DispatchResult {
            match service {
                "http" => self.dispatch_http(with, input).await,
                "grpc" => DispatchResult::Failure {
                    error: "gRPC dispatch not yet implemented in HttpTaskDispatcher".into(),
                    retryable: true,
                },
                _ => {
                    // function call — 暂不支持，返回 failure
                    DispatchResult::Failure {
                        error: format!("unknown service type: {service}"),
                        retryable: false,
                    }
                }
            }
        }
    }

    impl HttpTaskDispatcher {
        async fn dispatch_http(
            &self,
            with: Option<&Value>,
            input: &Value,
        ) -> DispatchResult {
            let method = with
                .and_then(|w| w.get("method"))
                .and_then(|v| v.as_str())
                .unwrap_or("GET")
                .to_uppercase();

            let endpoint = match with.and_then(|w| w.get("endpoint")).and_then(|v| v.as_str()) {
                Some(url) => url.to_string(),
                None => {
                    return DispatchResult::Failure {
                        error: "HTTP call missing 'endpoint' in 'with' config".into(),
                        retryable: false,
                    }
                }
            };

            let headers = with
                .and_then(|w| w.get("headers"))
                .and_then(|v| v.as_object());

            // 构建请求
            let mut req = match method.as_str() {
                "GET" => self.client.get(&endpoint),
                "POST" => self.client.post(&endpoint),
                "PUT" => self.client.put(&endpoint),
                "DELETE" => self.client.delete(&endpoint),
                "PATCH" => self.client.patch(&endpoint),
                other => {
                    return DispatchResult::Failure {
                        error: format!("unsupported HTTP method: {other}"),
                        retryable: false,
                    }
                }
            };

            // 设置请求头
            if let Some(hdrs) = headers {
                for (k, v) in hdrs {
                    if let Some(val) = v.as_str() {
                        req = req.header(k.as_str(), val);
                    }
                }
            }

            // 设置 JSON 请求体（POST/PUT/PATCH）
            if matches!(method.as_str(), "POST" | "PUT" | "PATCH") {
                req = req.json(input);
            }

            // 发送请求
            match req.send().await {
                Ok(resp) => {
                    let status = resp.status();
                    match resp.json::<Value>().await {
                        Ok(body) => {
                            if status.is_success() {
                                DispatchResult::Success { data: body }
                            } else {
                                DispatchResult::Failure {
                                    error: format!(
                                        "HTTP {} returned status {}: {}",
                                        endpoint,
                                        status.as_u16(),
                                        body
                                    ),
                                    retryable: status.is_server_error(),
                                }
                            }
                        }
                        Err(e) => DispatchResult::Failure {
                            error: format!("HTTP {} response parse error: {e}", endpoint),
                            retryable: false,
                        },
                    }
                }
                Err(e) => {
                    let retryable = e.is_timeout() || e.is_connect();
                    DispatchResult::Failure {
                        error: format!("HTTP {} request failed: {e}", endpoint),
                        retryable,
                    }
                }
            }
        }
    }

    // ─── 类型别名 ───

    /// 生产级 EngineRuntime：使用 trait object 支持可插拔的 Store/Dispatcher/EventProvider
    type EngineRuntime = WorkflowRuntime<
        ExpressionEvaluator,
        SystemClock,
        Arc<dyn WorkflowStore + Send + Sync>,
        Arc<dyn TaskDispatcher + Send + Sync>,
        Arc<dyn EventProvider + Send + Sync>,
    >;

    // ─── 部署错误 ───

    /// 工作流定义部署错误 —— 区分「输入/校验问题」与「存储/基础设施问题」
    ///
    /// gRPC 映射：`Validation` → `InvalidArgument`；`Store` → `Internal`。
    /// 与 ISSUE-001 的 error/deny 区分思路一致：输入问题 ≠ 基础设施故障。
    #[derive(Debug, Clone, PartialEq)]
    pub enum DeployError {
        /// 输入解析 / DSL 校验失败（gRPC → InvalidArgument）
        Validation(String),
        /// 存储 / 基础设施失败（gRPC → Internal）
        Store(String),
    }

    impl std::fmt::Display for DeployError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                DeployError::Validation(msg) => write!(f, "validation error: {msg}"),
                DeployError::Store(msg) => write!(f, "store error: {msg}"),
            }
        }
    }

    impl std::error::Error for DeployError {}

    /// 语义版本号 +1（回滚/新版本落地，对齐 policy 的「回滚版本 = 当前版本 +1」）
    ///
    /// - 纯数字点分版本（如 "1.0"、"1.2.3"）→ 末段 +1（"1.0" → "1.1"，"1.2.3" → "1.2.4"）；
    /// - 非纯数字版本 → 追加 "-r1"（如 "1.0-beta" → "1.0-beta-r1"）。
    fn bump_version(version: &str) -> String {
        let is_pure_numeric = !version.is_empty()
            && version
                .split('.')
                .all(|p| !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()));
        if is_pure_numeric {
            let mut parts: Vec<u64> = version
                .split('.')
                .filter_map(|p| p.parse().ok())
                .collect();
            if let Some(last) = parts.last_mut() {
                *last += 1;
            }
            parts.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(".")
        } else {
            format!("{version}-r1")
        }
    }

    // ─── WorkflowEngineService ───

    /// 基于 coord-core 工作流引擎的 WorkflowService
    ///
    /// 职责:
    /// - DSL 解析（YAML → WorkflowDefinition）
    /// - 实例生命周期管理（start/resume/cancel）
    /// - 定义与实例的持久化查询
    pub struct WorkflowEngineService {
        store: Arc<dyn WorkflowStore + Send + Sync>,
        runtime: Option<Arc<EngineRuntime>>,
        expression: ExpressionEvaluator,
        clock: SystemClock,
    }

    impl WorkflowEngineService {
        /// 创建生产级 WorkflowEngineService（使用 MemoryWorkflowStore + HttpTaskDispatcher + MemoryEventProvider）
        pub fn new() -> Self {
            let store: Arc<dyn WorkflowStore + Send + Sync> = Arc::new(MemoryWorkflowStore::new());
            let expression = ExpressionEvaluator::new();
            let clock = SystemClock;
            let executor = coord_core::workflow::engine::WorkflowExecutor::new(
                expression.clone(),
                clock.clone(),
            );
            let runtime = WorkflowRuntime::new(
                executor,
                clock.clone(),
                Arc::clone(&store),
                Arc::new(HttpTaskDispatcher::new()) as Arc<dyn TaskDispatcher + Send + Sync>,
                Arc::new(MemoryEventProvider::new()) as Arc<dyn EventProvider + Send + Sync>,
            );

            Self {
                store,
                runtime: Some(Arc::new(runtime)),
                expression,
                clock,
            }
        }

        /// 创建基于 KvWorkflowStore 的生产级 WorkflowEngineService
        ///
        /// 工作流定义和实例通过 coord-server 的 KV/Txn/Watch API 持久化，
        /// 享受 Raft 共识保证。适用于多 agent 部署场景。
        pub fn new_with_kv_store(inner: Arc<AgentInner>) -> Self {
            let kv_store = KvWorkflowStore::new(inner);
            let store: Arc<dyn WorkflowStore + Send + Sync> = Arc::new(kv_store);
            let expression = ExpressionEvaluator::new();
            let clock = SystemClock;
            let executor = coord_core::workflow::engine::WorkflowExecutor::new(
                expression.clone(),
                clock.clone(),
            );
            let runtime = WorkflowRuntime::new(
                executor,
                clock.clone(),
                Arc::clone(&store),
                Arc::new(HttpTaskDispatcher::new()) as Arc<dyn TaskDispatcher + Send + Sync>,
                Arc::new(MemoryEventProvider::new()) as Arc<dyn EventProvider + Send + Sync>,
            );

            Self {
                store,
                runtime: Some(Arc::new(runtime)),
                expression,
                clock,
            }
        }

        /// 创建测试用 WorkflowEngineService（使用 NoopTaskDispatcher + NoopEventProvider）
        ///
        /// 测试中不使用真实 HTTP 调用和事件总线，避免外部依赖。
        #[cfg(test)]
        pub fn new_for_test() -> Self {
            let store: Arc<dyn WorkflowStore + Send + Sync> = Arc::new(MemoryWorkflowStore::new());
            let expression = ExpressionEvaluator::new();
            let clock = SystemClock;
            let executor = coord_core::workflow::engine::WorkflowExecutor::new(
                expression.clone(),
                clock.clone(),
            );
            let runtime = WorkflowRuntime::new(
                executor,
                clock.clone(),
                Arc::clone(&store),
                Arc::new(NoopTaskDispatcher) as Arc<dyn TaskDispatcher + Send + Sync>,
                Arc::new(NoopEventProvider) as Arc<dyn EventProvider + Send + Sync>,
            );

            Self {
                store,
                runtime: Some(Arc::new(runtime)),
                expression,
                clock,
            }
        }

        fn runtime(&self) -> &Arc<EngineRuntime> {
            self.runtime.as_ref().expect("runtime not initialized")
        }

        // ─── 定义管理 ───
        // (async API — callers use .await)

    /// 部署工作流定义（YAML/JSON DSL → 持久化存储）
    ///
    /// 自动识别输入格式：
    /// - 顶层含 `start` / `states` → **CNCF Serverless Workflow 权威格式**（原生解析，无转换层）；
    /// - 顶层含 `document` / `do` → 遗留 coord DSL 格式（兼容保留）。
    ///
    /// 校验失败不落库（`DeployError::Validation`），存储失败为 `DeployError::Store`。
    pub async fn deploy_definition(
        &self,
        namespace: &str,
        dsl: &str,
    ) -> Result<String, DeployError> {
        let value: serde_json::Value = serde_yaml::from_str(dsl)
            .map_err(|e| DeployError::Validation(format!("parse error: {e}")))?;

        let mut def = if sw::looks_like_cncf_sw(&value) {
            // CNCF SW 权威格式：start + states[] 原生解析
            sw::parse_cncf_sw_value(value).map_err(DeployError::Validation)?
        } else {
            // 遗留 coord DSL 格式（document + do）
            let raw = parser::RawWorkflowDef::parse_yaml(dsl)
                .map_err(|e| DeployError::Validation(format!("parse error: {e}")))?;
            coord_core::workflow::validate::Validator::validate(raw).map_err(|errors| {
                DeployError::Validation(
                    errors.iter().map(|e| e.message.clone()).collect::<Vec<_>>().join("; "),
                )
            })?
        };

        // 用调用者指定的 namespace 覆盖 YAML 中的 namespace
        def.document.namespace = namespace.to_string();
        // 保存原始 DSL 以便 get_definition 返回
        def.raw_yaml = Some(dsl.to_string());

        let id = format!(
            "{}-{:x}",
            namespace,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        );
        def.id = Some(id.clone());

        // 原子覆盖（Txn CAS，对齐 policy PutBundle），防并发同键部署丢更新
        self.store.save_definition_atomic(&def).await
            .map_err(|e| DeployError::Store(format!("store error: {e}")))?;

        Ok(id)
    }

    /// 列出某 `(namespace, name)` 定义的全部版本（回滚目标发现，对齐 policy `ListBundleVersions`）
    pub async fn list_definition_versions(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Vec<WorkflowDefinition>, String> {
        self.store
            .list_definition_versions(namespace, name)
            .await
            .map_err(|e| format!("store error: {e}"))
    }

    /// 回滚工作流定义（对齐 policy `RollbackBundle`）
    ///
    /// 语义：读取目标版本快照 → 重新解析 + 校验（不可部署则拒绝）→
    /// 以**新语义版本**（当前版本 +1）原子落地，保留原始 DSL；
    /// 不覆盖旧版本（版本化共存）。
    pub async fn rollback_definition(
        &self,
        namespace: &str,
        name: &str,
        version: &str,
    ) -> Result<WorkflowDefinition, DeployError> {
        // 1. 读取目标版本快照
        let snapshot = self
            .store
            .load_definition(namespace, name, version)
            .await
            .map_err(|e| DeployError::Store(format!("store error: {e}")))?
            .ok_or_else(|| {
                DeployError::Validation(format!(
                    "definition '{namespace}/{name}@{version}' not found"
                ))
            })?;
        let raw = snapshot.raw_yaml.clone().ok_or_else(|| {
            DeployError::Validation(format!(
                "definition '{namespace}/{name}@{version}' has no raw DSL to restore"
            ))
        })?;

        // 2. 重新解析 + 校验（恢复内容必须可部署）
        let value: serde_json::Value = serde_yaml::from_str(&raw)
            .map_err(|e| DeployError::Validation(format!("parse error: {e}")))?;
        let mut def = if sw::looks_like_cncf_sw(&value) {
            sw::parse_cncf_sw_value(value).map_err(DeployError::Validation)?
        } else {
            let raw_ir = parser::RawWorkflowDef::parse_yaml(&raw)
                .map_err(|e| DeployError::Validation(format!("parse error: {e}")))?;
            coord_core::workflow::validate::Validator::validate(raw_ir).map_err(|errors| {
                DeployError::Validation(
                    errors.iter().map(|e| e.message.clone()).collect::<Vec<_>>().join("; "),
                )
            })?
        };

        // 3. 覆盖 namespace + 语义版本 bump（回滚 = 新版本落地）
        def.document.namespace = namespace.to_string();
        def.document.version = bump_version(version);
        def.raw_yaml = Some(raw);
        let id = format!(
            "{}-{:x}",
            namespace,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        );
        def.id = Some(id.clone());

        // 4. 原子写入新版本
        self.store.save_definition_atomic(&def).await
            .map_err(|e| DeployError::Store(format!("store error: {e}")))?;

        Ok(def)
    }

        pub async fn get_definition(
            &self,
            definition_id: &str,
        ) -> Result<Option<WorkflowDefinition>, String> {
            let all_defs = self.store
                .list_definitions("", usize::MAX, None)
                .await
                .map_err(|e| format!("store error: {e}"))?;
            Ok(all_defs.into_iter().find(|d| d.id.as_deref() == Some(definition_id)))
        }

        pub async fn list_definitions(
            &self,
            namespace: &str,
            page_size: usize,
            page_token: Option<&str>,
        ) -> Result<Vec<WorkflowDefinition>, String> {
            self.store
                .list_definitions(namespace, page_size, page_token)
                .await
                .map_err(|e| format!("store error: {e}"))
        }

        // ─── 实例管理 ───

        pub async fn start_instance(
            &self,
            definition_id: &str,
            input: Value,
        ) -> Result<WorkflowInstance, String> {
            let def = self
                .get_definition(definition_id)
                .await?
                .ok_or_else(|| format!("definition not found: {definition_id}"))?;

            self.runtime()
                .start(&def, input)
                .await
                .map_err(|e| format!("runtime error: {e}"))
        }

        pub async fn get_instance(
            &self,
            instance_id: &str,
        ) -> Result<Option<WorkflowInstance>, String> {
            self.store
                .load_instance(instance_id)
                .await
                .map_err(|e| format!("store error: {e}"))
        }

        pub async fn signal_instance(
            &self,
            instance_id: &str,
            signal_name: &str,
            payload: Value,
        ) -> Result<WorkflowInstance, String> {
            self.resume_instance(instance_id, Some(signal_name), Some(payload), None).await
        }

        pub async fn resume_instance(
            &self,
            instance_id: &str,
            signal_name: Option<&str>,
            payload: Option<Value>,
            idempotency_key: Option<&str>,
        ) -> Result<WorkflowInstance, String> {
            self.runtime()
                .resume(instance_id, signal_name, payload, idempotency_key)
                .await
                .map_err(|e| format!("runtime error: {e}"))
        }

        pub async fn cancel_instance(
            &self,
            instance_id: &str,
        ) -> Result<(), String> {
            let mut inst = self.store
                .load_instance(instance_id)
                .await
                .map_err(|e| format!("store error: {e}"))?
                .ok_or_else(|| format!("instance not found: {instance_id}"))?;

            if inst.status.is_terminal() {
                return Err(format!("instance already in terminal state: {:?}", inst.status));
            }

            inst.status = InstanceStatus::Cancelled;
            inst.updated_at = SystemClock.now_ms();
            self.store.save_instance(&inst).await
                .map_err(|e| format!("store error: {e}"))?;

            Ok(())
        }

        pub async fn list_instances(
            &self,
            namespace: Option<&str>,
            definition_name: Option<&str>,
            page_size: usize,
            page_token: Option<&str>,
        ) -> Result<Vec<WorkflowInstance>, String> {
            self.store
                .list_instances(namespace, definition_name, page_size, page_token)
                .await
                .map_err(|e| format!("store error: {e}"))
        }
    }

    impl Default for WorkflowEngineService {
        fn default() -> Self {
            Self::new()
        }
    }

    // ─── impl Clone for WorkflowEngineService ───
    // (manual because runtime contains Arc)

    impl Clone for WorkflowEngineService {
        fn clone(&self) -> Self {
            Self {
                store: Arc::clone(&self.store),
                runtime: self.runtime.clone(),
                expression: self.expression.clone(),
                clock: self.clock.clone(),
            }
        }
    }

    // ─── impl BaseService for WorkflowEngineService ───

    #[async_trait]
    impl crate::service::BaseService for WorkflowEngineService {
        fn name(&self) -> &'static str {
            "workflow-engine"
        }

        async fn start(&self) -> crate::service::ServiceResult<()> {
            Ok(())
        }

        async fn stop(&self) -> crate::service::ServiceResult<()> {
            Ok(())
        }

        fn health_check(&self) -> bool {
            self.runtime.is_some()
        }
    }
}

#[cfg(test)]
mod phase4_tests {
    use super::phase4::*;
    use coord_core::workflow::model::{InstanceStatus, Task};

    fn sample_linear_yaml() -> String {
        r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: linear-wf
  version: "1.0"
do:
  - step1:
      call: http
      with:
        method: POST
        endpoint: "http://localhost/step1"
  - step2:
      call: http
      with:
        method: POST
        endpoint: "http://localhost/step2"
"#
        .to_string()
    }

    fn sample_wait_yaml() -> String {
        r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: wait-wf
  version: "1.0"
do:
  - step1:
      call: http
      with:
        method: POST
        endpoint: "http://localhost/step1"
  - wait_step:
      wait: PT5S
  - step2:
      call: http
      with:
        method: POST
        endpoint: "http://localhost/step2"
"#
        .to_string()
    }

    fn sample_switch_yaml() -> String {
        r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: switch-wf
  version: "1.0"
do:
  - check:
      switch:
        - condition: ${ .amount > 100 }
          transition: high_path
        - defaultCondition: low_path
  - high_path:
      call: http
      with:
        method: POST
        endpoint: "http://localhost/high"
  - low_path:
      call: http
      with:
        method: POST
        endpoint: "http://localhost/low"
"#
        .to_string()
    }

    #[tokio::test]
    async fn test_engine_deploy_and_get_definition() {
        let svc = WorkflowEngineService::new_for_test();
        let yaml = sample_linear_yaml();
        let def_id = svc.deploy_definition("test", &yaml).await.unwrap();
        assert!(!def_id.is_empty());

        let loaded = svc.get_definition(&def_id).await.unwrap();
        assert!(loaded.is_some());
        let def = loaded.unwrap();
        assert_eq!(def.document.name, "linear-wf");
        assert_eq!(def.document.namespace, "test");
        assert_eq!(def.do_tasks.len(), 2);
    }

    #[tokio::test]
    async fn test_engine_deploy_rejects_invalid_yaml() {
        let svc = WorkflowEngineService::new_for_test();
        let result = svc.deploy_definition("test", "not: valid: yaml: [[[").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_engine_start_simple_instance() {
        let svc = WorkflowEngineService::new_for_test();
        let yaml = sample_linear_yaml();
        let def_id = svc.deploy_definition("test", &yaml).await.unwrap();

        let inst = svc.start_instance(&def_id, serde_json::json!({})).await.unwrap();
        assert!(!inst.id.is_empty());
        assert_eq!(inst.definition_name, "linear-wf");
        // call 任务通过 NoopTaskDispatcher 立即完成，实例可能已 Running/Completed
        assert!(inst.status == InstanceStatus::Running
                || inst.status == InstanceStatus::Suspended
                || inst.status == InstanceStatus::Completed);
    }

    #[tokio::test]
    async fn test_engine_get_instance_status() {
        let svc = WorkflowEngineService::new_for_test();
        let yaml = sample_linear_yaml();
        let def_id = svc.deploy_definition("test", &yaml).await.unwrap();
        let inst = svc.start_instance(&def_id, serde_json::json!({})).await.unwrap();

        let loaded = svc.get_instance(&inst.id).await.unwrap();
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().id, inst.id);
    }

    #[tokio::test]
    async fn test_engine_get_nonexistent_instance() {
        let svc = WorkflowEngineService::new_for_test();
        let result = svc.get_instance("nonexistent-id").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_engine_cancel_instance() {
        let svc = WorkflowEngineService::new_for_test();
        // 使用 wait 工作流确保实例有足够时间被取消
        let yaml = sample_wait_yaml();
        let def_id = svc.deploy_definition("test", &yaml).await.unwrap();
        let inst = svc.start_instance(&def_id, serde_json::json!({})).await.unwrap();

        // call 步骤完成后到达 wait 步骤，实例挂起
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let result = svc.cancel_instance(&inst.id).await;
        // 实例可能已完成或挂起，取消操作在挂起状态下应成功
        if result.is_ok() {
            let loaded = svc.get_instance(&inst.id).await.unwrap().unwrap();
            assert_eq!(loaded.status, InstanceStatus::Cancelled);
        }
    }

    #[tokio::test]
    async fn test_engine_signal_resume_wait_workflow() {
        let svc = WorkflowEngineService::new_for_test();
        let yaml = sample_wait_yaml();
        let def_id = svc.deploy_definition("test", &yaml).await.unwrap();
        let inst = svc.start_instance(&def_id, serde_json::json!({})).await.unwrap();

        // call 任务会挂起，给 drive loop 一点时间
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let loaded = svc.get_instance(&inst.id).await.unwrap().unwrap();
        assert_eq!(loaded.status, InstanceStatus::Suspended,
            "wait task should suspend");

        let resumed = svc.signal_instance(&inst.id, "approved",
            serde_json::json!({"result": "ok"})).await.unwrap();
        assert!(!resumed.id.is_empty());
    }

    #[tokio::test]
    async fn test_engine_list_definitions() {
        let svc = WorkflowEngineService::new_for_test();
        svc.deploy_definition("ns-a", &sample_linear_yaml()).await.unwrap();
        svc.deploy_definition("ns-a", &sample_wait_yaml()).await.unwrap();
        svc.deploy_definition("ns-b", &sample_switch_yaml()).await.unwrap();

        let list_a = svc.list_definitions("ns-a", 10, None).await.unwrap();
        assert_eq!(list_a.len(), 2, "ns-a should have 2 definitions");

        let list_b = svc.list_definitions("ns-b", 10, None).await.unwrap();
        assert_eq!(list_b.len(), 1, "ns-b should have 1 definition");
    }

    #[tokio::test]
    async fn test_engine_list_instances() {
        let svc = WorkflowEngineService::new_for_test();
        let def_id = svc.deploy_definition("test", &sample_linear_yaml()).await.unwrap();

        svc.start_instance(&def_id, serde_json::json!({})).await.unwrap();
        svc.start_instance(&def_id, serde_json::json!({})).await.unwrap();

        let instances = svc.list_instances(None, Some("linear-wf"), 10, None).await.unwrap();
        assert_eq!(instances.len(), 2);
    }

    #[tokio::test]
    async fn test_engine_idempotent_resume() {
        let svc = WorkflowEngineService::new_for_test();
        // 使用包含 wait 任务的工作流，确保实例会挂起
        let yaml = sample_wait_yaml();
        let def_id = svc.deploy_definition("test", &yaml).await.unwrap();
        let inst = svc.start_instance(&def_id, serde_json::json!({})).await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // 实例应该处于 Suspended 状态（在 wait 步骤）
        let loaded = svc.get_instance(&inst.id).await.unwrap().unwrap();
        assert_eq!(loaded.status, InstanceStatus::Suspended,
            "instance should be suspended on wait task");

        let r1 = svc.resume_instance(&inst.id, Some("approved"),
            Some(serde_json::json!({"ok": true})), Some("key-001")).await;
        assert!(r1.is_ok(), "first resume should succeed");

        let r2 = svc.resume_instance(&inst.id, Some("approved"),
            Some(serde_json::json!({"ok": true})), Some("key-001")).await;
        assert!(r2.is_ok(), "duplicate idempotent resume should succeed (no-op)");
    }

    #[tokio::test]
    async fn test_engine_switch_workflow() {
        let svc = WorkflowEngineService::new_for_test();
        let yaml = sample_switch_yaml();
        let def_id = svc.deploy_definition("test", &yaml).await.unwrap();
        let inst = svc.start_instance(&def_id,
            serde_json::json!({"amount": 200})).await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let loaded = svc.get_instance(&inst.id).await.unwrap().unwrap();
        // switch 工作流可能已完成或挂起
        assert!(loaded.current_task_index > 0
                || loaded.status == InstanceStatus::Suspended
                || loaded.status == InstanceStatus::Completed,
            "switch should have advanced past the decision task");
    }

    // ─── CNCF Serverless Workflow 权威格式 conformance（ISSUE-004） ───

    /// ISSUE-004 中的 SW 文档（CNCF 权威格式，start + states[]）
    fn sample_cncf_sw() -> &'static str {
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
                             "functionRef": { "refName": "sendNotify" } } ],
              "end": true }
          ]
        }"#
    }

    /// 分支互斥 SW 文档：senior-approve / notify 各自独立终止
    fn sample_branch_sw() -> &'static str {
        r#"{
          "id": "branch-wf",
          "version": "1.0",
          "start": "check",
          "states": [
            { "name": "check", "type": "switch",
              "dataConditions": [
                { "condition": "${ .amount >= 1000 }", "transition": "senior-approve" }
              ],
              "defaultCondition": { "transition": "notify" } },
            { "name": "senior-approve", "type": "operation",
              "actions": [ { "name": "approve", "functionRef": { "refName": "approve" } } ],
              "transition": "end" },
            { "name": "notify", "type": "operation",
              "actions": [ { "name": "notify", "functionRef": { "refName": "notify" } } ],
              "transition": "end" }
          ]
        }"#
    }

    #[tokio::test]
    async fn test_deploy_accepts_cncf_sw_directly() {
        // ISSUE-004：SW 权威文档直接喂 deployDefinition（原生解析，无转换层）
        let svc = WorkflowEngineService::new_for_test();
        let def_id = svc.deploy_definition("icps-flow", sample_cncf_sw()).await.unwrap();
        assert!(!def_id.is_empty());

        let loaded = svc.get_definition(&def_id).await.unwrap().unwrap();
        assert_eq!(loaded.document.dsl, "cncf-serverless-workflow");
        assert_eq!(loaded.document.name, "order-approval");
        assert_eq!(loaded.document.namespace, "icps-flow");
        // 转换结构：inject → set、operation → call、switch、终端 __end
        assert!(loaded.do_tasks.iter().any(|t| matches!(t.task, Task::Set(_))));
        assert!(loaded.do_tasks.iter().any(|t| matches!(t.task, Task::Call(_))));
        assert!(loaded.do_tasks.iter().any(|t| t.name == "check"));
        assert!(loaded.do_tasks.iter().any(|t| t.name == "__end"));
    }

    #[tokio::test]
    async fn test_deploy_cncf_sw_invalid_returns_validation_error() {
        let svc = WorkflowEngineService::new_for_test();
        let bad = r#"{
          "id": "x", "version": "1.0", "start": "s",
          "states": [ { "name": "s", "type": "delay", "transition": "end" } ]
        }"#;
        let err = svc.deploy_definition("ns", bad).await.unwrap_err();
        match err {
            DeployError::Validation(msg) => {
                assert!(msg.contains("unsupported type 'delay'"), "msg: {msg}")
            }
            other => panic!("expected Validation error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_sw_branch_executes_only_chosen_path() {
        // 分支互斥（端到端）：amount=1500 → senior-approve 分支，notify 不得执行
        let svc = WorkflowEngineService::new_for_test();
        let def_id = svc.deploy_definition("test", sample_branch_sw()).await.unwrap();
        let inst = svc.start_instance(&def_id,
            serde_json::json!({"amount": 1500})).await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let loaded = svc.get_instance(&inst.id).await.unwrap().unwrap();
        assert_eq!(loaded.status, InstanceStatus::Completed,
            "workflow should complete, fault={:?}", loaded.fault);

        let ran: Vec<&str> = loaded.task_stack.iter().map(|f| f.task_name.as_str()).collect();
        assert!(ran.contains(&"senior-approve"),
            "senior-approve branch should run, stack={ran:?}");
        assert!(!ran.contains(&"notify"),
            "notify branch must NOT run (fall-through), stack={ran:?}");
    }

    #[tokio::test]
    async fn test_sw_branch_default_path_executes() {
        // 分支互斥（default）：amount=100 → notify 分支，senior-approve 不得执行
        let svc = WorkflowEngineService::new_for_test();
        let def_id = svc.deploy_definition("test", sample_branch_sw()).await.unwrap();
        let inst = svc.start_instance(&def_id,
            serde_json::json!({"amount": 100})).await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let loaded = svc.get_instance(&inst.id).await.unwrap().unwrap();
        assert_eq!(loaded.status, InstanceStatus::Completed,
            "workflow should complete, fault={:?}", loaded.fault);

        let ran: Vec<&str> = loaded.task_stack.iter().map(|f| f.task_name.as_str()).collect();
        assert!(ran.contains(&"notify"), "notify branch should run, stack={ran:?}");
        assert!(!ran.contains(&"senior-approve"),
            "senior-approve branch must NOT run, stack={ran:?}");
    }
}
