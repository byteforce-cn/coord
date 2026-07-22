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
