// coord-agent: Serverless Workflow 流程引擎 (Workflow Service)
//
// 实现 BaseService trait，提供工作流定义管理与实例执行能力。
// 基于 Coord 核心原语（KV + Txn + Lease + Watch）构建。
//
// 当前状态（与 docs/production/workflow-semantics.md / STATUS.md 同一口径）：
//   - CNCF SW DSL 全状态编译（coord-core sw.rs）+ 端到端执行（phase4
//     WorkflowEngineService）；
//   - Saga 补偿：`compensatedBy` → catch-all 转场 + 补偿动作派发；三步
//     逆序补偿端到端判据
//     phase4_tests::test_sw_three_step_failure_compensates_in_reverse_order_and_replays；
//   - 保留策略：`DeleteInstance` / `DeleteDefinition`（显式、能力门禁；
//     无自动 TTL/归档）。
// 语义细节（补偿幂等/重试/超时/信号/保留）见 docs/production/workflow-semantics.md。

use std::collections::{BTreeMap, HashSet};
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
    pub fn new(
        name: impl Into<String>,
        dsl_source: Vec<u8>,
        dsl_format: impl Into<String>,
    ) -> Self {
        let now = unix_ts();
        Self {
            name: name.into(),
            version: "1.0".into(),
            dsl_source,
            dsl_format: dsl_format.into(),
            description: String::new(),
            timeout_secs: 0,
            created_at: now,
            updated_at: now,
        }
    }
    pub fn storage_key(name: &str) -> Vec<u8> {
        format!("/_workflow/defs/{name}").into_bytes()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowState {
    Pending,
    Running,
    Suspended,
    Completed,
    Failed,
    Compensated,
    Cancelled,
    TimedOut,
}

impl WorkflowState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            WorkflowState::Completed
                | WorkflowState::Failed
                | WorkflowState::Compensated
                | WorkflowState::Cancelled
                | WorkflowState::TimedOut
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WorkflowInstance {
    pub instance_id: String,
    pub workflow_name: String,
    pub definition_id: Option<String>, // 关联的已部署工作流定义 ID
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
    pub fn new(
        instance_id: impl Into<String>,
        workflow_name: impl Into<String>,
        input: Vec<u8>,
    ) -> Self {
        let now = unix_ts();
        Self {
            instance_id: instance_id.into(),
            workflow_name: workflow_name.into(),
            definition_id: None,
            state: WorkflowState::Pending,
            current_step: 0,
            input,
            output: Vec::new(),
            error_message: String::new(),
            lease_id: 0,
            created_at: now,
            updated_at: now,
        }
    }
    pub fn storage_key(instance_id: &str) -> Vec<u8> {
        format!("/_workflow/instances/{instance_id}").into_bytes()
    }
}

fn unix_ts() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn unix_ts_i64() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

// ──── 新增数据类型 ────

/// 工作流定义（deploy/get_definition 使用）
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WorkflowDefinition {
    pub id: String,
    pub name: String,
    pub namespace: String,
    pub yaml: String,
    pub version: String, // 语义化版本字符串，如 "1.0"
    pub status: String,  // "active" | "deprecated"
    pub created_at: i64,
}

/// 工作流定义摘要（list_definitions 使用）
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DefSummary {
    pub id: String,
    pub name: String,
    pub version: String,
    pub status: String,
    pub created_at: i64,
}

/// 工作流实例摘要（list_instances 使用）
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
    pub fn storage_key(id: &str) -> Vec<u8> {
        format!("/_workflow/v2/defs/{id}").into_bytes()
    }
    pub fn namespace_key(namespace: &str) -> Vec<u8> {
        format!("/_workflow/v2/ns/{namespace}/").into_bytes()
    }
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

impl Default for WorkflowCache {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkflowCache {
    pub fn new() -> Self {
        Self {
            defs: BTreeMap::new(),
            instances: BTreeMap::new(),
        }
    }
    pub fn put_def(&mut self, def: WorkflowDef) {
        self.defs.insert(def.name.clone(), def);
    }
    pub fn get_def(&self, name: &str) -> Option<&WorkflowDef> {
        self.defs.get(name)
    }
    pub fn remove_def(&mut self, name: &str) -> Option<WorkflowDef> {
        self.defs.remove(name)
    }
    pub fn list_defs(&self) -> Vec<&WorkflowDef> {
        self.defs.values().collect()
    }
    pub fn put_instance(&mut self, inst: WorkflowInstance) {
        self.instances.insert(inst.instance_id.clone(), inst);
    }
    pub fn get_instance(&self, id: &str) -> Option<&WorkflowInstance> {
        self.instances.get(id)
    }
    pub fn remove_instance(&mut self, id: &str) -> Option<WorkflowInstance> {
        self.instances.remove(id)
    }
    pub fn list_instances_by_workflow(&self, wf: &str) -> Vec<&WorkflowInstance> {
        self.instances
            .values()
            .filter(|i| i.workflow_name == wf)
            .collect()
    }
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
        Self {
            inner,
            cache: ParkingRwLock::new(WorkflowCache::new()),
            healthy: ParkingRwLock::new(false),
            shutdown_tx: ParkingRwLock::new(None),
        }
    }

    pub async fn define(&self, def: WorkflowDef) -> ServiceResult<()> {
        let key = WorkflowDef::storage_key(&def.name);
        let value = serde_json::to_vec(&def).map_err(|e| format!("serialize: {e}"))?;
        self.inner
            .client
            .kv()
            .put(&key, &value)
            .await
            .map_err(|e| format!("put: {e}"))?;
        self.cache.write().put_def(def);
        Ok(())
    }

    pub async fn get_definition(&self, name: &str) -> ServiceResult<Option<WorkflowDef>> {
        if let Some(def) = self.cache.read().get_def(name) {
            return Ok(Some(def.clone()));
        }
        let key = WorkflowDef::storage_key(name);
        let pairs = self
            .inner
            .client
            .kv()
            .range(&key, &key, 1, 0)
            .await
            .map_err(|e| format!("range: {e}"))?;
        if let Some((_k, v)) = pairs.into_iter().next() {
            let def: WorkflowDef =
                serde_json::from_slice(&v).map_err(|e| format!("deserialize: {e}"))?;
            self.cache.write().put_def(def.clone());
            Ok(Some(def))
        } else {
            Ok(None)
        }
    }

    pub async fn remove_definition(&self, name: &str) -> ServiceResult<()> {
        let key = WorkflowDef::storage_key(name);
        self.inner
            .client
            .kv()
            .delete(&key)
            .await
            .map_err(|e| format!("delete: {e}"))?;
        self.cache.write().remove_def(name);
        Ok(())
    }

    pub async fn start_instance(&self, inst: WorkflowInstance) -> ServiceResult<()> {
        let key = WorkflowInstance::storage_key(&inst.instance_id);
        let value = serde_json::to_vec(&inst).map_err(|e| format!("serialize: {e}"))?;
        self.inner
            .client
            .kv()
            .put(&key, &value)
            .await
            .map_err(|e| format!("put: {e}"))?;
        self.cache.write().put_instance(inst);
        Ok(())
    }

    pub async fn transition_state(
        &self,
        instance_id: &str,
        expected: WorkflowState,
        next: WorkflowState,
    ) -> ServiceResult<()> {
        let key = WorkflowInstance::storage_key(instance_id);
        let pairs = self
            .inner
            .client
            .kv()
            .range(&key, &key, 1, 0)
            .await
            .map_err(|e| format!("range: {e}"))?;
        let (_k, v) = pairs
            .into_iter()
            .next()
            .ok_or_else(|| format!("instance '{instance_id}' not found"))?;
        let mut inst: WorkflowInstance =
            serde_json::from_slice(&v).map_err(|e| format!("deserialize: {e}"))?;
        if inst.state != expected {
            return Err(format!(
                "state mismatch: expected {expected:?}, got {:?}",
                inst.state
            )
            .into());
        }
        inst.state = next;
        inst.updated_at = unix_ts();
        let new_val = serde_json::to_vec(&inst).map_err(|e| format!("serialize: {e}"))?;
        self.inner
            .client
            .kv()
            .put(&key, &new_val)
            .await
            .map_err(|e| format!("put: {e}"))?;
        self.cache.write().put_instance(inst);
        Ok(())
    }

    pub async fn get_instance(&self, id: &str) -> ServiceResult<Option<WorkflowInstance>> {
        if let Some(inst) = self.cache.read().get_instance(id) {
            return Ok(Some(inst.clone()));
        }
        let key = WorkflowInstance::storage_key(id);
        let pairs = self
            .inner
            .client
            .kv()
            .range(&key, &key, 1, 0)
            .await
            .map_err(|e| format!("range: {e}"))?;
        if let Some((_k, v)) = pairs.into_iter().next() {
            let inst: WorkflowInstance =
                serde_json::from_slice(&v).map_err(|e| format!("deserialize: {e}"))?;
            self.cache.write().put_instance(inst.clone());
            Ok(Some(inst))
        } else {
            Ok(None)
        }
    }

    /// 向 workflow instance 发送 signal（记录 signal 信息到 output 字段）
    pub async fn signal_instance(
        &self,
        instance_id: &str,
        signal_name: &str,
        payload: &[u8],
    ) -> ServiceResult<()> {
        let key = WorkflowInstance::storage_key(instance_id);
        let pairs = self
            .inner
            .client
            .kv()
            .range(&key, &key, 1, 0)
            .await
            .map_err(|e| format!("kv range: {e}"))?;
        let (_k, v) = pairs
            .into_iter()
            .next()
            .ok_or_else(|| format!("instance '{instance_id}' not found"))?;
        let mut inst: WorkflowInstance =
            serde_json::from_slice(&v).map_err(|e| format!("deserialize: {e}"))?;
        // 将 signal 信息记录到 output 字段
        let signal_record = format!(
            "signal:{} payload:{}",
            signal_name,
            String::from_utf8_lossy(payload)
        );
        inst.output = signal_record.into_bytes();
        inst.updated_at = unix_ts();
        let new_val = serde_json::to_vec(&inst).map_err(|e| format!("serialize: {e}"))?;
        self.inner
            .client
            .kv()
            .put(&key, &new_val)
            .await
            .map_err(|e| format!("kv put: {e}"))?;
        self.cache.write().put_instance(inst);
        Ok(())
    }

    // ──── 工作流定义管理 ────

    /// 部署工作流定义，存储到 coord-server KV 层以支持多 Agent 共享
    pub async fn deploy_definition(
        &self,
        namespace: &str,
        yaml: &str,
    ) -> ServiceResult<(String, String, String)> {
        // 使用时间戳+随机数生成简短 ID
        let id = format!(
            "{}-{:x}",
            namespace,
            unix_ts() as u32
                ^ (std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .subsec_nanos())
        );
        let now = unix_ts_i64();
        // 从 YAML 中提取 name（支持缩进和引号）
        let name = yaml
            .lines()
            .find(|l| {
                let trimmed = l.trim_start();
                trimmed.starts_with("name:") || trimmed.starts_with("id:")
            })
            .and_then(|l| l.split_once(':').map(|x| x.1))
            .map(|s| s.trim().trim_matches('"').trim_matches('\'').to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| id.clone());

        // 从 YAML 中提取 version（语义化版本字符串）
        let version = yaml
            .lines()
            .find(|l| {
                let trimmed = l.trim_start();
                trimmed.starts_with("version:")
            })
            .and_then(|l| l.split_once(':').map(|x| x.1))
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
        self.inner
            .client
            .kv()
            .put(&key, &value)
            .await
            .map_err(|e| format!("kv put definition: {e}"))?;

        // 同时写入 namespace 索引，确保 list_definitions 可按 namespace 查询
        let ns_idx_key = WorkflowDefinition::namespace_index_key(namespace, &id);
        self.inner
            .client
            .kv()
            .put(&ns_idx_key, id.as_bytes())
            .await
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
        let pairs = self
            .inner
            .client
            .kv()
            .range(&prefix, &range_end, limit, 0)
            .await
            .map_err(|e| format!("kv range definitions: {e}"))?;

        let mut summaries: Vec<DefSummary> = Vec::new();
        for (_k, v) in &pairs {
            // namespace 索引值存的是 definition id
            let def_id = String::from_utf8_lossy(v).to_string();
            // 用 definition id 获取完整定义
            let def_key = WorkflowDefinition::storage_key(&def_id);
            match self.inner.client.kv().range(&def_key, &def_key, 1, 0).await {
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
        let pairs = self
            .inner
            .client
            .kv()
            .range(&key, &key, 1, 0)
            .await
            .map_err(|e| format!("kv get definition: {e}"))?;

        let (_k, v) = pairs
            .into_iter()
            .next()
            .ok_or_else(|| format!("definition '{workflow_id}' not found"))?;
        let def: WorkflowDefinition =
            serde_json::from_slice(&v).map_err(|e| format!("deserialize definition: {e}"))?;
        Ok(def)
    }

    /// 列出工作流实例
    pub async fn list_instances(
        &self,
        workflow_id: &str,
        namespace: &str,
        page_size: i32,
        page_token: &str,
    ) -> ServiceResult<(Vec<InstSummary>, String)> {
        // 从 KV 存储扫描所有实例（前缀 /_workflow/instances/）
        let prefix = b"/_workflow/instances/".to_vec();
        let range_end = prefix_end(&prefix);
        let kv_limit = if page_size > 0 { page_size as i64 } else { 50 };

        let pairs = self
            .inner
            .client
            .kv()
            .range(&prefix, &range_end, kv_limit, 0)
            .await
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
            if let Ok(pairs) = self.inner.client.kv().range(&def_key, &def_key, 1, 0).await {
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

        // namespace 过滤：遗留实例自身不带 namespace，需经其关联定义归属判定。
        // 先收齐该 namespace 下的定义（id 与 name），再据此过滤实例。
        // 直接扫 namespace 索引（limit 0 ⇒ server max_range_limit 兜底）：
        // legacy `list_definitions` 会把 page_size=0 解释成 50 条缺省页，
        // 定义多于 50 个时会漏掉后一半，进而误滤实例。
        let ns_refs: Option<(HashSet<String>, HashSet<String>)> = if namespace.is_empty() {
            None
        } else {
            let prefix = WorkflowDefinition::namespace_key(namespace);
            let range_end = prefix_end(&prefix);
            let pairs = self
                .inner
                .client
                .kv()
                .range(&prefix, &range_end, 0, 0)
                .await
                .map_err(|e| format!("kv range definitions: {e}"))?;
            let mut ids = HashSet::new();
            let mut names = HashSet::new();
            for (_k, v) in &pairs {
                let def_id = String::from_utf8_lossy(v).to_string();
                let def_key = WorkflowDefinition::storage_key(&def_id);
                if let Ok(def_pairs) = self.inner.client.kv().range(&def_key, &def_key, 1, 0).await
                {
                    if let Some((_dk, dv)) = def_pairs.into_iter().next() {
                        if let Ok(def) = serde_json::from_slice::<WorkflowDefinition>(&dv) {
                            ids.insert(def.id.clone());
                            names.insert(def.name.clone());
                        }
                    }
                }
            }
            Some((ids, names))
        };

        let filtered: Vec<&WorkflowInstance> = all_instances
            .iter()
            .filter(|i| match &filter_name {
                None => true,
                Some(name) => {
                    i.workflow_name == *name || i.definition_id.as_deref() == Some(workflow_id)
                }
            })
            .filter(|i| match &ns_refs {
                None => true,
                Some((ids, names)) => legacy_instance_in_namespace(i, ids, names),
            })
            .collect();

        // 应用分页
        let start_idx = if page_token.is_empty() {
            0usize
        } else {
            filtered
                .iter()
                .position(|i| i.instance_id == page_token)
                .map(|p| p + 1)
                .unwrap_or(0)
        };
        let slice_limit = if page_size > 0 {
            page_size as usize
        } else {
            50
        };
        let end_idx = (start_idx + slice_limit).min(filtered.len());

        let summaries: Vec<InstSummary> = filtered[start_idx..end_idx]
            .iter()
            .map(|i| InstSummary {
                id: i.instance_id.clone(),
                workflow_id: i
                    .definition_id
                    .clone()
                    .unwrap_or_else(|| i.workflow_name.clone()),
                state: format!("{:?}", i.state),
                started_at: i.created_at as i64,
                updated_at: i.updated_at as i64,
                definition_name: i.workflow_name.clone(),
            })
            .collect();

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

/// 遗留实例的 namespace 归属判定：实例经其关联定义（`definition_id` 优先，
/// 回退 `workflow_name`）归属到 namespace。`ns_def_ids`/`ns_def_names` 为
/// 目标 namespace 下的定义 id 与定义名集合。
fn legacy_instance_in_namespace(
    inst: &WorkflowInstance,
    ns_def_ids: &HashSet<String>,
    ns_def_names: &HashSet<String>,
) -> bool {
    inst.definition_id
        .as_deref()
        .is_some_and(|id| ns_def_ids.contains(id))
        || ns_def_names.contains(&inst.workflow_name)
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
    fn name(&self) -> &'static str {
        Self::NAME
    }

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
        if let Some(tx) = self.shutdown_tx.write().take() {
            let _ = tx.send(());
        }
        *self.healthy.write() = false;
        Ok(())
    }

    fn health_check(&self) -> bool {
        *self.healthy.read()
    }
}

impl std::fmt::Debug for WorkflowService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let c = self.cache.read();
        f.debug_struct("WorkflowService")
            .field("defs", &c.defs.len())
            .field("instances", &c.instances.len())
            .finish()
    }
}

// ═══════════════════════════════════════════════════════════════════
// Serverless Workflow DSL 解释器
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
    Conditional {
        conditions: BTreeMap<String, String>,
        default: Option<String>,
    },
    /// 并行分叉
    ParallelFork {
        branches: Vec<String>,
        join: Option<String>,
    },
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

fn default_version() -> String {
    "1.0".into()
}

impl WorkflowDsl {
    /// 从 JSON 字节解析 DSL
    pub fn from_json(data: &[u8]) -> Result<Self, String> {
        let dsl: WorkflowDsl =
            serde_json::from_slice(data).map_err(|e| format!("invalid DSL JSON: {e}"))?;
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
            return Err(format!(
                "startState '{}' not found in states",
                self.start_state
            ));
        }
        // Validate all referenced states exist
        for (name, state) in &self.states {
            if let Some(ref next) = state.next_state {
                if !self.states.contains_key(next) {
                    return Err(format!(
                        "state '{name}' references non-existent next state '{next}'"
                    ));
                }
            }
            for target in state.conditions.values() {
                if !self.states.contains_key(target) {
                    return Err(format!(
                        "state '{name}' references non-existent condition target '{target}'"
                    ));
                }
            }
            if let Some(ref default) = state.default_next {
                if !self.states.contains_key(default) {
                    return Err(format!(
                        "state '{name}' references non-existent default target '{default}'"
                    ));
                }
            }
            for branch in &state.branches {
                if !self.states.contains_key(branch) {
                    return Err(format!(
                        "state '{name}' references non-existent branch '{branch}'"
                    ));
                }
            }
            if let Some(ref join) = state.join_state {
                if !self.states.contains_key(join) {
                    return Err(format!(
                        "state '{name}' references non-existent join state '{join}'"
                    ));
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
    Forked {
        branches: Vec<String>,
        join: Option<String>,
    },
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

        let state = self
            .dsl
            .states
            .get(&ctx.current_state)
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
                InterpreterResult::WaitingForEvent(e) => {
                    return Ok(InterpreterResult::WaitingForEvent(e))
                }
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

    fn handle_operation(
        &self,
        state: &WorkflowStateDef,
        ctx: &mut WorkflowContext,
    ) -> Result<InterpreterResult, String> {
        // Execute action
        if let Some(ref action) = state.action {
            let action_type = ActionType::parse(action);
            ctx.variables.insert(
                "_last_action".to_string(),
                serde_json::Value::String(format!("{:?}", action_type)),
            );
        }

        match &state.next_state {
            Some(next) => {
                ctx.current_state = next.clone();
                Ok(InterpreterResult::Transitioned(next.clone()))
            }
            None => Ok(InterpreterResult::Completed),
        }
    }

    fn handle_switch(
        &self,
        state: &WorkflowStateDef,
        ctx: &mut WorkflowContext,
    ) -> Result<InterpreterResult, String> {
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

    fn handle_parallel(
        &self,
        state: &WorkflowStateDef,
        ctx: &mut WorkflowContext,
    ) -> Result<InterpreterResult, String> {
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

    fn handle_delay(
        &self,
        state: &WorkflowStateDef,
        ctx: &mut WorkflowContext,
    ) -> Result<InterpreterResult, String> {
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

    fn handle_event(
        &self,
        state: &WorkflowStateDef,
        _ctx: &mut WorkflowContext,
    ) -> Result<InterpreterResult, String> {
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
                            return n.as_i64() == Some(exp_num)
                                || n.as_f64() == Some(expected.parse::<f64>().unwrap_or(0.0));
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

    #[test]
    fn test_legacy_instance_namespace_membership() {
        let ids: HashSet<String> = ["def-a".to_string()].into_iter().collect();
        let names: HashSet<String> = ["alpha-wf".to_string()].into_iter().collect();
        let other_ids: HashSet<String> = ["def-b".to_string()].into_iter().collect();
        let other_names: HashSet<String> = ["beta-wf".to_string()].into_iter().collect();

        // definition_id 命中目标 namespace 的定义
        let mut inst = WorkflowInstance::new("i1", "alpha-wf", vec![]);
        inst.definition_id = Some("def-a".into());
        assert!(legacy_instance_in_namespace(&inst, &ids, &names));
        assert!(!legacy_instance_in_namespace(
            &inst,
            &other_ids,
            &other_names
        ));

        // definition_id 缺失时回退 workflow_name
        let no_id = WorkflowInstance::new("i2", "alpha-wf", vec![]);
        assert!(legacy_instance_in_namespace(
            &no_id,
            &other_ids.clone(),
            &names
        ));
        assert!(!legacy_instance_in_namespace(
            &no_id,
            &other_ids,
            &other_names
        ));
    }

    // ──── 新数据类型测试 ────

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
// WorkflowEngineService — 对接 coord-core 工作流引擎
// ═══════════════════════════════════════════════════════════════════
//
// 实现基于 coord-core::workflow 的引擎绑定（DSL 解释器已退役）。
// TDD: 先写 phase4_tests，再实现本模块。

pub mod phase4 {
    use std::collections::HashMap;
    use std::sync::Arc;

    use async_trait::async_trait;
    use serde_json::Value;

    use coord_core::workflow::{
        expression::ExpressionEvaluator,
        model::{WorkflowDefinition, WorkflowInstance},
        parser,
        ports::{
            Clock, DispatchResult, EventProvider, MemoryEventProvider, MemoryWorkflowStore,
            SystemClock, TaskDispatcher, WorkflowStore,
        },
        runtime::WorkflowRuntime,
        sw,
    };

    use coord_core::workflow::approval::bridge::{
        ApprovalBridge, ApprovalCommand, ApprovalOutcome, BridgeError, CommandOutcome,
        MapApproverResolver, NodeOutcome,
    };
    use coord_core::workflow::approval::model::{
        Branch as ApprovalBranch, Round as ApprovalRound, Task as ApprovalTask,
    };
    use coord_core::workflow::approval::store::ApprovalStore;

    use crate::proxy::AgentInner;
    use crate::service::WorkflowDispatchConfig;
    use crate::services::approval_store::KvApprovalStore;
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
            _idempotency_key: Option<&str>,
        ) -> DispatchResult {
            DispatchResult::Success { data: Value::Null }
        }
    }

    // ─── HttpTaskDispatcher ───
    //
    // 真实 HTTP 任务派发器：通过 reqwest 执行 HTTP 调用。
    // gRPC 和 function 调用暂返回 Failure（需后续扩展）。

    /// OAuth2 令牌缓存键：`client_secret` 不进键（不引入长驻凭据副本）。
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct OAuth2CacheKey {
        token_url: String,
        client_id: String,
        scope: Option<String>,
    }

    /// 已取 OAuth2 令牌（`expires_at` 为提前刷新后的失效时刻）。
    #[derive(Debug, Clone)]
    struct CachedToken {
        token: String,
        expires_at: std::time::Instant,
    }

    /// HTTP 任务派发器 —— 通过 reqwest 执行真实 HTTP 调用
    #[derive(Debug, Clone)]
    pub struct HttpTaskDispatcher {
        client: reqwest::Client,
        /// 密钥映射（${ $secrets.x } 占位符解析）
        secrets: std::collections::HashMap<String, String>,
        /// 常量映射（${ $constants.x } 占位符解析）
        constants: serde_json::Map<String, Value>,
        /// OAuth2 client_credentials 令牌缓存（进程内；按 tokenUrl/clientId/scope 分桶）
        oauth2_tokens: std::sync::Arc<
            tokio::sync::Mutex<std::collections::HashMap<OAuth2CacheKey, CachedToken>>,
        >,
    }

    impl HttpTaskDispatcher {
        /// 创建新的 HTTP 任务派发器
        pub fn new() -> Self {
            let client = match reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
            {
                Ok(client) => client,
                Err(e) => {
                    tracing::error!("failed to create reqwest client: {e}; using default client");
                    reqwest::Client::new()
                }
            };
            Self {
                client,
                secrets: std::collections::HashMap::new(),
                constants: serde_json::Map::new(),
                oauth2_tokens: std::sync::Arc::new(tokio::sync::Mutex::new(
                    std::collections::HashMap::new(),
                )),
            }
        }

        /// 使用自定义 reqwest 客户端创建
        pub fn with_client(client: reqwest::Client) -> Self {
            Self {
                client,
                secrets: std::collections::HashMap::new(),
                constants: serde_json::Map::new(),
                oauth2_tokens: std::sync::Arc::new(tokio::sync::Mutex::new(
                    std::collections::HashMap::new(),
                )),
            }
        }

        /// 注入密钥映射（`${ $secrets.x }` 安全注入）
        pub fn with_secrets(mut self, secrets: std::collections::HashMap<String, String>) -> Self {
            self.secrets = secrets;
            self
        }

        /// 注入常量映射（标准：${ $constants.x }）
        pub fn with_constants(mut self, constants: serde_json::Map<String, Value>) -> Self {
            self.constants = constants;
            self
        }

        /// 按 agent 配置（`[services.workflow_dispatch]`）构造：secrets/constants
        /// 来自配置而非仅 builder，使生产构造路径也能解析占位符。
        pub fn from_config(config: &WorkflowDispatchConfig) -> Self {
            Self::new()
                .with_secrets(config.secrets.clone())
                .with_constants(config.constants.clone())
        }

        /// 解析 `with` 中的 `${ $secrets.x }` / `${ $constants.x }` 占位符
        fn resolve_placeholders(&self, value: &Value) -> Value {
            match value {
                Value::String(s) => {
                    let mut resolved = s.clone();
                    for (key, val) in &self.secrets {
                        let pat = format!("${{ $secrets.{key} }}");
                        if resolved.contains(&pat) {
                            resolved = resolved.replace(&pat, val);
                        }
                    }
                    for (key, val) in &self.constants {
                        let pat = format!("${{ $constants.{key} }}");
                        if resolved.contains(&pat) {
                            resolved = resolved.replace(&pat, &val.to_string());
                        }
                    }
                    Value::String(resolved)
                }
                Value::Array(arr) => {
                    Value::Array(arr.iter().map(|v| self.resolve_placeholders(v)).collect())
                }
                Value::Object(obj) => Value::Object(
                    obj.iter()
                        .map(|(k, v)| (k.clone(), self.resolve_placeholders(v)))
                        .collect(),
                ),
                other => other.clone(),
            }
        }

        /// 解析占位符，并在仍有未命中 key 时返回缺失 key 名（fail-closed 用）。
        fn resolve_placeholders_checked(&self, value: &Value) -> Result<Value, Vec<String>> {
            let resolved = self.resolve_placeholders(value);
            let mut missing = Vec::new();
            collect_unresolved_placeholders(&resolved, &mut missing);
            if missing.is_empty() {
                Ok(resolved)
            } else {
                Err(missing)
            }
        }

        /// 递归求值 `with` 中的整串 `${ ... }` 运行时表达式（#96）。
        ///
        /// 绑定白名单：`$context`（= 实例上下文，即 dispatch 的 `input`）与 `$input`。
        /// `${ $secrets.x }` / `${ $constants.x }` 是配置注入占位符（非运行时表达式）——
        /// 独立语义、支持内嵌，原样留给后续注入；同一字符串出现其他 `${...}` 或
        /// 独立表达式中混入 `$secrets`/`$constants` ⇒ fail-closed，绝不静默把字面量
        /// 发出去。错误只含 JSON 路径与绑定名，不含值。
        fn evaluate_runtime_expressions(
            &self,
            value: &Value,
            context: &Value,
        ) -> Result<Value, String> {
            self.evaluate_runtime_expressions_at(value, context, "with")
        }

        fn evaluate_runtime_expressions_at(
            &self,
            value: &Value,
            context: &Value,
            path: &str,
        ) -> Result<Value, String> {
            match value {
                Value::String(s) => {
                    let trimmed = s.trim();
                    if !trimmed.contains("${") {
                        return Ok(value.clone());
                    }
                    let standalone = trimmed.starts_with("${")
                        && trimmed.ends_with('}')
                        && trimmed.matches("${").count() == 1;
                    if !standalone {
                        // 非整串：仅当 `${...}` 全部是纯配置占位符时才放行（支持
                        // `Bearer ${ $secrets.token }` 这类既有内嵌注入）。
                        if strip_plain_config_placeholders(s).contains("${") {
                            return Err(format!(
                                "{path} 含内嵌表达式——with 仅支持整串单段表达式（无字符串插值）"
                            ));
                        }
                        return Ok(value.clone());
                    }
                    let inner = trimmed[2..trimmed.len() - 1].trim();
                    if is_plain_config_placeholder(inner) {
                        return Ok(value.clone());
                    }
                    self.evaluate_expression_string(inner, context, path)
                }
                Value::Array(arr) => arr
                    .iter()
                    .enumerate()
                    .map(|(i, v)| {
                        self.evaluate_runtime_expressions_at(v, context, &format!("{path}[{i}]"))
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map(Value::Array),
                Value::Object(obj) => obj
                    .iter()
                    .map(|(k, v)| {
                        self.evaluate_runtime_expressions_at(v, context, &format!("{path}.{k}"))
                            .map(|v| (k.clone(), v))
                    })
                    .collect::<Result<serde_json::Map<_, _>, _>>()
                    .map(Value::Object),
                other => Ok(other.clone()),
            }
        }

        /// 求值单个已剥壳的表达式字符串（`inner` 为 `${ ... }` 内层文本）。
        fn evaluate_expression_string(
            &self,
            inner: &str,
            context: &Value,
            path: &str,
        ) -> Result<Value, String> {
            if inner.is_empty() {
                return Err(format!("{path} 的运行时表达式为空"));
            }
            // 首字符闸门：求值器对无法识别的裸 token 会回退为「整个 context」，这里
            // 只放行确定是表达式的形态，避免把 `hello` 这类文本静默换成上下文。
            let first = inner.chars().next().unwrap_or('\0');
            let literal = matches!(inner, "true" | "false" | "null");
            let supported = literal
                || first == '$'
                || first == '.'
                || first == '"'
                || first == '('
                || first == '{'
                || first == '-'
                || first.is_ascii_digit();
            if !supported {
                return Err(format!(
                    "{path} 的运行时表达式不受支持（无法识别起始 token '{first}'）"
                ));
            }
            for binding in expression_bindings(inner) {
                if !matches!(binding.as_str(), "context" | "input") {
                    return Err(format!(
                        "{path} 引用了未知运行时绑定 ${binding}（仅支持 $context / $input；\
                         $secrets / $constants 仅支持整串占位符形式）"
                    ));
                }
            }
            let mut vars = HashMap::new();
            vars.insert("input".to_string(), context.clone());
            ExpressionEvaluator::new()
                .evaluate_with_vars(inner, context, &vars)
                .map_err(|e| format!("{path} 运行时表达式求值失败：{e}"))
        }
    }

    /// `${ $secrets.<key> }` / `${ $constants.<key> }` 的纯占位符判定（键为标识符形）。
    fn is_plain_config_placeholder(inner: &str) -> bool {
        ["$secrets.", "$constants."].iter().any(|prefix| {
            inner
                .strip_prefix(prefix)
                .map(|key| {
                    !key.is_empty()
                        && key
                            .chars()
                            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
                })
                .unwrap_or(false)
        })
    }

    /// 去掉字符串中的纯配置占位符片段（残留检测用）；非配置的 `${...}` 原样保留。
    fn strip_plain_config_placeholders(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        let mut rest = s;
        while let Some(pos) = rest.find("${") {
            out.push_str(&rest[..pos]);
            let after = &rest[pos + 2..];
            match after.find('}') {
                Some(end) => {
                    if !is_plain_config_placeholder(after[..end].trim()) {
                        out.push_str("${");
                        out.push_str(&after[..=end]);
                    }
                    rest = &after[end + 1..];
                }
                None => {
                    out.push_str("${");
                    rest = after;
                }
            }
        }
        out.push_str(rest);
        out
    }

    /// 提取表达式中的 `$name` 绑定（忽略字符串字面量）。
    fn expression_bindings(expr: &str) -> Vec<String> {
        let mut names = Vec::new();
        let mut chars = expr.chars().peekable();
        let mut in_string = false;
        let mut escape = false;
        while let Some(c) = chars.next() {
            if in_string {
                if escape {
                    escape = false;
                } else if c == '\\' {
                    escape = true;
                } else if c == '"' {
                    in_string = false;
                }
                continue;
            }
            match c {
                '"' => in_string = true,
                '$' => {
                    let mut name = String::new();
                    while let Some(&n) = chars.peek() {
                        if n.is_ascii_alphanumeric() || n == '_' {
                            name.push(n);
                            chars.next();
                        } else {
                            break;
                        }
                    }
                    if !name.is_empty() && !names.contains(&name) {
                        names.push(name);
                    }
                }
                _ => {}
            }
        }
        names
    }

    /// oauth2 配置缺失：非重试失败，只点名字段名（不回声任何值）。
    fn oauth2_config_failure(field: &str) -> DispatchResult {
        DispatchResult::Failure {
            error: format!("oauth2 auth missing required field 'auth.{field}'"),
            retryable: false,
        }
    }

    /// 令牌端点错误体的安全摘要：只取 OAuth2 `error` / `error_description`（截断），
    /// 并把 `client_secret` 的字面出现替换为 `[redacted]`；不回显原始响应体。
    fn oauth2_error_detail(body: &str, client_secret: &str) -> String {
        let detail = serde_json::from_str::<Value>(body)
            .ok()
            .and_then(|v| {
                let error = v.get("error").and_then(|e| e.as_str()).unwrap_or("");
                let description = v
                    .get("error_description")
                    .and_then(|d| d.as_str())
                    .unwrap_or("");
                let joined = match (error.is_empty(), description.is_empty()) {
                    (false, false) => format!("{error}: {description}"),
                    (false, true) => error.to_string(),
                    (true, false) => description.to_string(),
                    (true, true) => String::new(),
                };
                (!joined.is_empty()).then_some(joined)
            })
            .unwrap_or_else(|| "(no OAuth2 error detail)".to_string());
        redact_secret(&detail, client_secret)
            .chars()
            .take(300)
            .collect()
    }

    /// 把 `secret` 的每个非空字面出现替换为 `[redacted]`。
    fn redact_secret(text: &str, secret: &str) -> String {
        if secret.is_empty() {
            return text.to_string();
        }
        text.replace(secret, "[redacted]")
    }

    /// 占位符前缀与错误信息里的标签（两者必须一致）。
    const SECRETS_PLACEHOLDER_PREFIX: &str = "${ $secrets.";
    const CONSTANTS_PLACEHOLDER_PREFIX: &str = "${ $constants.";

    /// 收集解析后残留的 `${ $secrets.<key> }` / `${ $constants.<key> }`，
    /// 以 `secrets.<key>` / `constants.<key>` 形式去重列出（不含凭据值）。
    fn collect_unresolved_placeholders(value: &Value, out: &mut Vec<String>) {
        match value {
            Value::String(s) => {
                for (prefix, label) in [
                    (SECRETS_PLACEHOLDER_PREFIX, "secrets"),
                    (CONSTANTS_PLACEHOLDER_PREFIX, "constants"),
                ] {
                    let mut rest = s.as_str();
                    while let Some(pos) = rest.find(prefix) {
                        let after = &rest[pos + prefix.len()..];
                        // 占位符语法以 ` }` 收尾（与 resolve_placeholders 的 format 一致）
                        let end = after.find(" }").unwrap_or(after.len());
                        let key = after[..end].trim();
                        if !key.is_empty() {
                            let token = format!("{label}.{key}");
                            if !out.contains(&token) {
                                out.push(token);
                            }
                        }
                        rest = &after[end..];
                    }
                }
            }
            Value::Array(arr) => arr
                .iter()
                .for_each(|v| collect_unresolved_placeholders(v, out)),
            Value::Object(obj) => obj
                .values()
                .for_each(|v| collect_unresolved_placeholders(v, out)),
            _ => {}
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
            idempotency_key: Option<&str>,
        ) -> DispatchResult {
            // 先求值运行时表达式（$context/$input），再解析 secrets/constants 配置
            // 占位符；残留未命中 key ⇒ fail-closed，绝不把字面量 `${ $secrets.x }`
            // 当凭据发出去（远端 401/403 看不出是本地配置缺失）。
            // 错误只含 key 名/路径与绑定名，不含任何凭据值。
            let resolved_with = match with {
                Some(w) => {
                    let evaluated = match self.evaluate_runtime_expressions(w, input) {
                        Ok(v) => v,
                        Err(msg) => {
                            return DispatchResult::Failure {
                                error: format!("workflow dispatch blocked: {msg}"),
                                retryable: false,
                            }
                        }
                    };
                    match self.resolve_placeholders_checked(&evaluated) {
                        Ok(v) => Some(v),
                        Err(missing) => {
                            return DispatchResult::Failure {
                                error: format!(
                                    "workflow dispatch blocked: unresolved placeholder(s) {} — \
                                     缺失的 key 未配置于 agent 的 [services.workflow_dispatch]",
                                    missing.join(", ")
                                ),
                                retryable: false,
                            }
                        }
                    }
                }
                None => None,
            };
            match service {
                "http" => {
                    self.dispatch_http(resolved_with.as_ref(), input, idempotency_key)
                        .await
                }
                "grpc" => self.dispatch_grpc(resolved_with.as_ref(), input).await,
                _ => {
                    // function call — 从 use.functions 解析后 fallback 到 http/grpc 语义
                    DispatchResult::Failure {
                        error: format!("unknown service type: {service}"),
                        retryable: false,
                    }
                }
            }
        }
    }

    impl HttpTaskDispatcher {
        /// 应用认证配置（basic / bearer / oauth2）
        ///
        /// oauth2 走 client_credentials：向 `tokenUrl` 取短 TTL 令牌后作 Bearer 发出。
        /// 令牌按过期时间缓存并在过期前刷新；取用失败按 `DispatchResult` 的可重试
        /// 语义映射（连接/超时/5xx 可重试，4xx/缺 access_token/非 JSON 不可重试），
        /// 错误信息不含 `client_secret`。
        async fn apply_auth(
            &self,
            req: reqwest::RequestBuilder,
            auth: &Value,
        ) -> Result<reqwest::RequestBuilder, DispatchResult> {
            let scheme = auth
                .get("scheme")
                .or_else(|| auth.get("type"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            match scheme {
                "basic" => {
                    let user = auth
                        .get("username")
                        .or_else(|| auth.get("user"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let pass = auth.get("password").and_then(|v| v.as_str()).unwrap_or("");
                    Ok(req.basic_auth(user, Some(pass)))
                }
                "bearer" => {
                    let token = auth.get("token").and_then(|v| v.as_str()).unwrap_or("");
                    Ok(req.bearer_auth(token))
                }
                "oauth2" => {
                    let field = |names: &[&str]| {
                        names
                            .iter()
                            .find_map(|n| auth.get(*n).and_then(|v| v.as_str()))
                            .filter(|v| !v.is_empty())
                    };
                    let token_url = field(&["tokenUrl", "token_url"])
                        .ok_or_else(|| oauth2_config_failure("tokenUrl"))?;
                    let client_id = field(&["clientId", "client_id"])
                        .ok_or_else(|| oauth2_config_failure("clientId"))?;
                    let client_secret = field(&["clientSecret", "client_secret"])
                        .ok_or_else(|| oauth2_config_failure("clientSecret"))?;
                    let scope = field(&["scope"]);
                    let token = self
                        .oauth2_token(token_url, client_id, client_secret, scope)
                        .await?;
                    Ok(req.bearer_auth(token))
                }
                _ => Ok(req),
            }
        }

        /// 取（或复用缓存中的）OAuth2 client_credentials 令牌。
        async fn oauth2_token(
            &self,
            token_url: &str,
            client_id: &str,
            client_secret: &str,
            scope: Option<&str>,
        ) -> Result<String, DispatchResult> {
            let key = OAuth2CacheKey {
                token_url: token_url.to_string(),
                client_id: client_id.to_string(),
                scope: scope.map(String::from),
            };
            // 单飞：持锁跨令牌请求，并发派发不会重复取令牌（同一 dispatcher 实例）。
            let mut cache = self.oauth2_tokens.lock().await;
            if let Some(cached) = cache.get(&key) {
                if cached.expires_at > std::time::Instant::now() {
                    return Ok(cached.token.clone());
                }
            }

            let mut form = vec![
                ("grant_type", "client_credentials"),
                ("client_id", client_id),
                ("client_secret", client_secret),
            ];
            if let Some(s) = scope {
                form.push(("scope", s));
            }
            let resp = match self.client.post(token_url).form(&form).send().await {
                Ok(resp) => resp,
                Err(e) => {
                    return Err(DispatchResult::Failure {
                        error: format!("OAuth2 token endpoint {token_url} request failed: {e}"),
                        retryable: e.is_timeout() || e.is_connect(),
                    })
                }
            };
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            if !status.is_success() {
                return Err(DispatchResult::Failure {
                    error: format!(
                        "OAuth2 token endpoint {token_url} returned status {}: {}",
                        status.as_u16(),
                        oauth2_error_detail(&body, client_secret)
                    ),
                    retryable: status.is_server_error(),
                });
            }
            let parsed: Value = match serde_json::from_str(&body) {
                Ok(v) => v,
                Err(_) => {
                    return Err(DispatchResult::Failure {
                        error: format!(
                            "OAuth2 token endpoint {token_url} returned a non-JSON response"
                        ),
                        retryable: false,
                    })
                }
            };
            let token = match parsed.get("access_token").and_then(|v| v.as_str()) {
                Some(t) if !t.is_empty() => t.to_string(),
                _ => {
                    return Err(DispatchResult::Failure {
                        error: format!(
                            "OAuth2 token endpoint {token_url} response missing access_token"
                        ),
                        retryable: false,
                    })
                }
            };
            let ttl_secs = parsed
                .get("expires_in")
                .and_then(|v| v.as_u64().or_else(|| v.as_str()?.parse().ok()));
            if let Some(ttl) = ttl_secs.filter(|t| *t > 0) {
                // 提前刷新窗口 = min(30s, ttl/10)：短 TTL 令牌不会被固定窗口贬为永不缓存。
                let margin = std::cmp::min(
                    std::time::Duration::from_secs(30),
                    std::time::Duration::from_secs(ttl / 10),
                );
                cache.insert(
                    key,
                    CachedToken {
                        token: token.clone(),
                        expires_at: std::time::Instant::now() + std::time::Duration::from_secs(ttl)
                            - margin,
                    },
                );
            }
            Ok(token)
        }

        /// 完整 HTTP 调用契约：
        /// - `method`: GET/POST/PUT/DELETE/PATCH
        /// - `endpoint`: 字符串 URI 或 `{uri, method}` 对象
        /// - `query`: 查询参数映射
        /// - `headers`: 请求头映射
        /// - `body`: 显式请求体（缺省用工作流输入）
        /// - `auth`: 认证（basic/bearer/oauth2）
        async fn dispatch_http(
            &self,
            with: Option<&Value>,
            input: &Value,
            idempotency_key: Option<&str>,
        ) -> DispatchResult {
            let (method, endpoint, query, body, headers, auth) = match with {
                Some(w) => {
                    // endpoint 对象形式 { uri, method } 或字符串
                    let (ep_method, ep_uri) = match w.get("endpoint") {
                        Some(Value::Object(ep)) => (
                            ep.get("method")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string(),
                            ep.get("uri")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string(),
                        ),
                        Some(Value::String(s)) => (String::new(), s.clone()),
                        _ => (String::new(), String::new()),
                    };
                    let method = w
                        .get("method")
                        .and_then(|v| v.as_str())
                        .map(String::from)
                        .filter(|m| !m.is_empty())
                        .or(if ep_method.is_empty() {
                            None
                        } else {
                            Some(ep_method)
                        })
                        .unwrap_or_else(|| "GET".to_string())
                        .to_uppercase();
                    let query = w.get("query").and_then(|v| v.as_object()).cloned();
                    let body = w.get("body").cloned();
                    let headers = w.get("headers").and_then(|v| v.as_object()).cloned();
                    let auth = w.get("auth").cloned();
                    (method, ep_uri, query, body, headers, auth)
                }
                None => ("GET".to_string(), String::new(), None, None, None, None),
            };

            if endpoint.is_empty() {
                return DispatchResult::Failure {
                    error: "HTTP call missing 'endpoint' in 'with' config".into(),
                    retryable: false,
                };
            }

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

            // 查询参数
            if let Some(q) = query {
                req = req.query(&q);
            }

            // 请求头
            if let Some(hdrs) = headers {
                for (k, v) in hdrs {
                    if let Some(val) = v.as_str() {
                        req = req.header(k.as_str(), val);
                    }
                }
            }

            // 幂等键：派发前已在 step token store 登记的确定性 token。
            // 在自定义 headers 之后注入，使去重键来自运行时派生而非调用方 config。
            if let Some(key) = idempotency_key {
                req = req.header("Idempotency-Key", key);
            }

            // 请求体：显式 body 优先，其次工作流输入
            let payload = body.unwrap_or_else(|| input.clone());
            if matches!(method.as_str(), "POST" | "PUT" | "PATCH") {
                req = req.json(&payload);
            }

            // 认证
            if let Some(auth_cfg) = auth {
                req = match self.apply_auth(req, &auth_cfg).await {
                    Ok(req) => req,
                    Err(failure) => return failure,
                };
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

        /// gRPC 调用：
        /// - `endpoint`: `{uri, method}`（method 为 `package.Service/Method`）
        /// - `body`: 请求消息（JSON → raw bytes 透传；proto 编码由调用方/转码层负责）
        ///
        /// 通过 tonic 通道发起标准 gRPC unary 调用，响应以 raw bytes 返回。
        async fn dispatch_grpc(&self, with: Option<&Value>, _input: &Value) -> DispatchResult {
            let w = match with {
                Some(w) => w,
                None => {
                    return DispatchResult::Failure {
                        error: "gRPC call missing 'endpoint' config".into(),
                        retryable: false,
                    }
                }
            };
            let uri = w
                .get("endpoint")
                .and_then(|v| v.as_object())
                .and_then(|e| e.get("uri"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let method = w
                .get("endpoint")
                .and_then(|v| v.as_object())
                .and_then(|e| e.get("method"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if uri.is_empty() || method.is_empty() {
                return DispatchResult::Failure {
                    error: "gRPC call requires endpoint.uri and endpoint.method".into(),
                    retryable: false,
                };
            }
            let (service, method_name) = match method.split_once('/') {
                Some((s, m)) => (s.to_string(), m.to_string()),
                None => {
                    return DispatchResult::Failure {
                        error: format!(
                            "gRPC method must be 'package.Service/Method', got {method}"
                        ),
                        retryable: false,
                    }
                }
            };
            let path: tonic::codegen::http::uri::PathAndQuery =
                match format!("/{service}/{method_name}").parse() {
                    Ok(p) => p,
                    Err(e) => {
                        return DispatchResult::Failure {
                            error: format!("invalid gRPC path: {e}"),
                            retryable: false,
                        }
                    }
                };

            let channel = match tonic::transport::Channel::from_shared(uri.to_string()) {
                Ok(ch) => match ch.connect().await {
                    Ok(c) => c,
                    Err(e) => {
                        return DispatchResult::Failure {
                            error: format!("gRPC connect to {uri} failed: {e}"),
                            retryable: true,
                        }
                    }
                },
                Err(e) => {
                    return DispatchResult::Failure {
                        error: format!("invalid gRPC uri {uri}: {e}"),
                        retryable: false,
                    }
                }
            };

            // 请求体：with.body 或输入（JSON → raw bytes 透传）
            let body = w.get("body").cloned().unwrap_or(Value::Null);
            let payload = serde_json::to_vec(&body).unwrap_or_default();

            let mut grpc = tonic::client::Grpc::new(channel);
            if let Err(e) = grpc.ready().await {
                return DispatchResult::Failure {
                    error: format!("gRPC channel not ready: {e}"),
                    retryable: true,
                };
            }
            let request = tonic::Request::new(RawMessage {
                payload: payload.clone(),
            });
            match grpc
                .unary(
                    request,
                    path,
                    tonic_prost::ProstCodec::<RawMessage, RawMessage>::default(),
                )
                .await
            {
                Ok(resp) => {
                    let msg = resp.into_inner();
                    let data = serde_json::from_slice::<Value>(&msg.payload).unwrap_or(
                        Value::String(String::from_utf8_lossy(&msg.payload).to_string()),
                    );
                    DispatchResult::Success { data }
                }
                Err(status) => DispatchResult::Failure {
                    error: format!("gRPC {method} failed: {status}"),
                    retryable: status.code() == tonic::Code::Unavailable
                        || status.code() == tonic::Code::DeadlineExceeded,
                },
            }
        }
    }

    /// gRPC 通用调用消息（raw bytes 透传）
    #[derive(Clone, PartialEq, ::prost::Message)]
    struct RawMessage {
        #[prost(bytes = "vec", tag = "1")]
        payload: Vec<u8>,
    }

    /// 类型别名 ───
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
    /// 输入问题 ≠ 基础设施故障（error/deny 区分思路）。
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

    /// 工作流引擎错误 —— typed 错误（signal/start 错误码 typed 映射）
    ///
    /// gRPC 映射：`InvalidArgument` → `InvalidArgument`；`NotFound` → `NotFound`；
    /// `FailedPrecondition` → `FailedPrecondition`；`Internal` → `Internal`。
    /// 输入问题 ≠ 状态冲突 ≠ 基础设施故障（error/deny 区分思路）。
    #[derive(Debug, Clone, PartialEq)]
    pub enum WorkflowEngineError {
        /// 输入 / signal 名不匹配（gRPC → InvalidArgument）
        InvalidArgument(String),
        /// 实例 / 定义未找到（gRPC → NotFound）
        NotFound(String),
        /// 状态冲突：对非挂起 / 终端实例操作（gRPC → FailedPrecondition）
        FailedPrecondition(String),
        /// 存储 / 内部（gRPC → Internal）
        Internal(String),
    }

    impl std::fmt::Display for WorkflowEngineError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                WorkflowEngineError::InvalidArgument(msg) => write!(f, "invalid argument: {msg}"),
                WorkflowEngineError::NotFound(msg) => write!(f, "not found: {msg}"),
                WorkflowEngineError::FailedPrecondition(msg) => {
                    write!(f, "failed precondition: {msg}")
                }
                WorkflowEngineError::Internal(msg) => write!(f, "internal error: {msg}"),
            }
        }
    }

    impl std::error::Error for WorkflowEngineError {}

    impl From<coord_core::workflow::runtime::RuntimeError> for WorkflowEngineError {
        fn from(e: coord_core::workflow::runtime::RuntimeError) -> Self {
            use coord_core::workflow::runtime::RuntimeError;
            match e {
                RuntimeError::NotFound(id) => {
                    WorkflowEngineError::NotFound(format!("instance not found: {id}"))
                }
                RuntimeError::AlreadyCompleted(id) => WorkflowEngineError::FailedPrecondition(
                    format!("instance already in terminal state: {id}"),
                ),
                RuntimeError::StaleRevision(id) => WorkflowEngineError::FailedPrecondition(
                    format!("stale revision for instance: {id}"),
                ),
                RuntimeError::DefinitionDrift {
                    instance_id,
                    expected,
                    actual,
                } => WorkflowEngineError::FailedPrecondition(format!(
                    "definition drift: instance {instance_id} pinned {expected}, loaded {actual}"
                )),
                RuntimeError::InvalidSignal(msg) => WorkflowEngineError::InvalidArgument(msg),
                RuntimeError::DefinitionNotFound(id) => {
                    WorkflowEngineError::NotFound(format!("workflow definition not found: {id}"))
                }
                RuntimeError::StoreError(msg) => {
                    WorkflowEngineError::Internal(format!("store error: {msg}"))
                }
                RuntimeError::GotoTargetNotFound(t) => {
                    WorkflowEngineError::Internal(format!("goto target not found: {t}"))
                }
            }
        }
    }

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
            let mut parts: Vec<u64> = version.split('.').filter_map(|p| p.parse().ok()).collect();
            if let Some(last) = parts.last_mut() {
                *last += 1;
            }
            parts
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(".")
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
        /// R-AGT-09：生产 KV 持久化后端（Some = 启动时全量重建 + watch 对账）
        kv_store: Option<Arc<KvWorkflowStore>>,
        runtime: Option<Arc<EngineRuntime>>,
        expression: ExpressionEvaluator,
        clock: SystemClock,
        /// Spike 3：KV outbox/定时器存储（Some = durable 装配）
        durable_store: Option<Arc<crate::services::outbox_kv::KvOutboxStore>>,
        /// Spike 3：outbox relay（Some 且 `relay_enabled` = 启动后持续投递）
        outbox_relay: Option<Arc<coord_core::workflow::durable::OutboxRelay>>,
        /// Spike 3：relay 是否由 `start()` 拉起
        relay_enabled: bool,
        /// Spike 3：relay 轮询间隔（毫秒）
        relay_interval_ms: u64,
        /// durable 装配的 MQ 句柄 + 事件 topic（仅 durable 构造设置）。
        ///
        /// 装配期构造早于 `PluginManager::start_all()`（MQ 的 redb 尚未打开），
        /// 构造时 `create_topic` 必然失败；因此 topic 的创建推迟到 `start()`——
        /// 等 MQ 就绪后在第一个产出/订阅之前建 topic。
        durable_mq: Option<Arc<crate::services::mq::MessageQueueService>>,
        durable_topic: String,
        /// v2 审批桥（仅 durable 装配注入）。
        ///
        /// `None` ⇒ 审批命令 / 查询 fail-closed（`FailedPrecondition`），绝不退化为
        /// 内存审批：无桥就无审批实例，静默成功会比报错更坏。
        approval: Option<Arc<ApprovalBridge>>,
    }

    /// durable 装配的实例驱动租约 TTL（秒）。
    ///
    /// 续租间隔由 TTL 推导（`ttl/3`，见 `KvInstanceLease`）——30s TTL ⇒ 10s 续租，
    /// 单次 KV 抖动或 GC 停顿不会立即丢失租约，同时驱动者在崩溃后 ≤30s 内可被
    /// 其他进程接管。取值与 Task 3 的 spike2 并发判据一致。
    const DURABLE_INSTANCE_LEASE_TTL_SECS: i64 = 30;

    /// Spike 3：持久化 outbox / 定时器接线选项。
    ///
    /// 只在 [`WorkflowEngineService::new_with_kv_store_durable`] 使用；
    /// 内存路径（`new` / `new_with_dispatch` / `new_for_test`）行为逐字不变。
    pub struct DurableEngineOptions {
        /// 事件投递面（MQ 服务实例，agent 内 redb 持久化）
        pub mq: Arc<crate::services::mq::MessageQueueService>,
        /// 事件 topic
        pub topic: String,
        /// 是否由 `start()` 拉起 relay：
        /// `false` 时事件只落 outbox（崩溃恢复证据刻意拆开「写」与「投递」两段）
        pub relay_enabled: bool,
        /// relay 轮询间隔（毫秒）
        pub relay_interval_ms: u64,
        /// 审批节点 `roles` 的角色解析映射（角色名 → 审批人）。
        ///
        /// 来自 `ServicesConfig::workflow_approval_roles`；空映射时使用 `roles` 的节点
        /// 在开启时 fail-closed（未映射角色不得静默当空席位）。
        pub approval_roles: HashMap<String, Vec<String>>,
    }

    /// 待办审批席位（`ListPendingTasks` 查询投影；`kind`/`status` 为模型 JSON 口径）。
    #[derive(Debug, Clone, PartialEq)]
    pub struct PendingTaskView {
        pub instance_id: String,
        pub task_id: String,
        pub node: String,
        pub branch_id: String,
        pub round_id: String,
        pub kind: String,
        pub status: String,
        pub assignee: String,
        /// 超时阈值（Unix ms）；0 = 无超时
        pub deadline_ms: i64,
        pub created_at_ms: i64,
    }

    /// 审批实例详情（`GetInstanceDetail` 查询投影）。
    ///
    /// `status` 为大写实例状态；`branches`/`tasks`/`rounds` 为引擎快照的原始实体。
    #[derive(Debug, Clone, PartialEq)]
    pub struct InstanceDetailView {
        pub instance_id: String,
        pub status: String,
        pub initiator: String,
        pub revision: u64,
        pub definition_name: String,
        pub branches: Vec<ApprovalBranch>,
        pub tasks: Vec<ApprovalTask>,
        pub rounds: Vec<ApprovalRound>,
    }

    /// 审批实体枚举的 JSON 标签（serde snake_case，与模型持久化口径一致）。
    pub(crate) fn json_label<T: serde::Serialize>(value: &T) -> String {
        serde_json::to_value(value)
            .ok()
            .and_then(|v| v.as_str().map(str::to_string))
            .unwrap_or_default()
    }

    /// 桥命令响应 `{"instance_id", "status"}` → `(instance_id, status)` 元组。
    fn approval_response_status(response: &Value) -> Result<(String, String), BridgeError> {
        let instance_id = response
            .get("instance_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                BridgeError::Serialization("approval response missing instance_id".into())
            })?;
        let status = response
            .get("status")
            .and_then(Value::as_str)
            .ok_or_else(|| BridgeError::Serialization("approval response missing status".into()))?;
        Ok((instance_id.to_string(), status.to_string()))
    }

    /// 运行时错误 → 桥错误族（服务层审批方法的统一外部口径）。
    fn map_runtime_to_bridge(e: coord_core::workflow::runtime::RuntimeError) -> BridgeError {
        use coord_core::workflow::runtime::RuntimeError;
        match e {
            RuntimeError::NotFound(id) => BridgeError::NotFound(id),
            RuntimeError::DefinitionNotFound(id) => BridgeError::NotFound(id),
            RuntimeError::StoreError(msg) => BridgeError::Store(msg),
            RuntimeError::StaleRevision(id) => BridgeError::Stale(id),
            RuntimeError::AlreadyCompleted(id) => {
                BridgeError::InvalidTransition(format!("instance already terminal: {id}"))
            }
            RuntimeError::GotoTargetNotFound(target) => {
                BridgeError::InvalidTransition(format!("goto target not found: {target}"))
            }
            RuntimeError::InvalidSignal(msg) => BridgeError::InvalidTransition(msg),
            RuntimeError::DefinitionDrift { instance_id, .. } => {
                BridgeError::InvalidTransition(format!("definition drift: {instance_id}"))
            }
        }
    }

    impl WorkflowEngineService {
        /// 创建生产级 WorkflowEngineService（使用 MemoryWorkflowStore + HttpTaskDispatcher + MemoryEventProvider）
        pub fn new() -> Self {
            Self::new_with_dispatch(&WorkflowDispatchConfig::default())
        }

        /// 创建内存态 WorkflowEngineService，并注入派发 secrets/constants 配置
        ///
        /// 无 server 连接时的回退路径（单 agent 测试/离线场景），
        /// `dispatch.workflow_dispatch` 仍生效（与 KV 路径同口径）。
        pub fn new_with_dispatch(dispatch: &WorkflowDispatchConfig) -> Self {
            let store: Arc<dyn WorkflowStore + Send + Sync> = Arc::new(MemoryWorkflowStore::new());
            Self::with_dispatcher(
                store,
                None,
                Arc::new(HttpTaskDispatcher::from_config(dispatch)),
            )
        }

        /// 创建基于 KvWorkflowStore 的生产级 WorkflowEngineService
        ///
        /// 工作流定义和实例通过 coord-server 的 KV/Txn/Watch API 持久化，
        /// 享受 Raft 共识保证。适用于多 agent 部署场景。
        ///
        /// R-AGT-09：`start()` 时调用 `KvWorkflowStore::init()`（全量重建
        /// 本地缓存 + 启动 watch），watch 断连自动重连并对账。
        pub fn new_with_kv_store(inner: Arc<AgentInner>) -> Self {
            Self::new_with_kv_store_and_dispatch(inner, &WorkflowDispatchConfig::default())
        }

        /// 基于 KvWorkflowStore 且注入派发 secrets/constants 配置的生产构造。
        ///
        /// 工作流定义存 KV，凭据不进定义存储（见
        /// `docs/production/workflow-semantics.md` §6）——定义只写占位符，
        /// 材料在本构造处注入。
        pub fn new_with_kv_store_and_dispatch(
            inner: Arc<AgentInner>,
            dispatch: &WorkflowDispatchConfig,
        ) -> Self {
            let kv_store = Arc::new(KvWorkflowStore::new(inner));
            let store: Arc<dyn WorkflowStore + Send + Sync> =
                Arc::clone(&kv_store) as Arc<dyn WorkflowStore + Send + Sync>;
            Self::with_dispatcher(
                store,
                Some(kv_store),
                Arc::new(HttpTaskDispatcher::from_config(dispatch)),
            )
        }

        /// 创建「KV 持久化 + Outbox + 持久化定时器」的引擎（Spike 2026-10 Spike 3）。
        ///
        /// 与 [`Self::new_with_kv_store_and_dispatch`] 的差别仅在注入面：
        /// - 事件先落 KV outbox（`AtomicOutboxCommit` 时与状态同事务），
        ///   由 relay 投递到 MQ；
        /// - `wait`/`retry`/超时写持久化定时器，重启后扫描到期项；
        /// - `listen` 用 MQ 订阅 + KV 持久化游标（重启续读）；
        /// - 实例驱动租约（`KvInstanceLease`，构造传入同一 outbox store）保证
        ///   「一实例一驱动」；
        /// - step token 存储（`KvStepStore`）为 `call` 派发登记幂等键。
        pub fn new_with_kv_store_durable(
            inner: Arc<AgentInner>,
            dispatch: &WorkflowDispatchConfig,
            opts: DurableEngineOptions,
        ) -> Self {
            use crate::services::mq_event_provider::{CursorStore, MqEventProvider};
            use crate::services::outbox_kv::{KvCursorStore, KvOutboxStore};
            use crate::services::outbox_relay::MqOutboxSink;
            use crate::services::step_kv::KvStepStore;
            use crate::services::workflow_lease::KvInstanceLease;
            use coord_core::workflow::durable::{
                AtomicOutboxCommit, DurableSupport, DurableTimerStore, OutboxRelay, OutboxSink,
                OutboxStore, RelayPolicy, StepTokenStore,
            };
            use coord_core::workflow::ports::InstanceLease;

            let kv_store = Arc::new(KvWorkflowStore::new(Arc::clone(&inner)));
            let store: Arc<dyn WorkflowStore + Send + Sync> =
                Arc::clone(&kv_store) as Arc<dyn WorkflowStore + Send + Sync>;

            let durable_kv = Arc::new(KvOutboxStore::new(Arc::clone(&inner)));
            let sink: Arc<dyn OutboxSink> =
                Arc::new(MqOutboxSink::new(Arc::clone(&opts.mq), opts.topic.clone()));
            let relay = Arc::new(OutboxRelay::new(
                Arc::clone(&durable_kv) as Arc<dyn OutboxStore>,
                sink,
                RelayPolicy::default(),
            ));

            let cursors: Arc<dyn CursorStore> = Arc::new(KvCursorStore::new(Arc::clone(&inner)));
            let event_provider: Arc<dyn EventProvider + Send + Sync> =
                Arc::new(MqEventProvider::new_with_cursor_store(
                    Arc::clone(&opts.mq),
                    opts.topic.clone(),
                    cursors,
                ));

            // step token 存储：`call` 派发前的幂等键（去重外部副作用），与 outbox
            // 同在 `/_workflow/v3/` 键空间；注入后重试/恢复复用同一 token。
            let support = DurableSupport::new(
                Arc::clone(&durable_kv) as Arc<dyn DurableTimerStore>,
                Arc::clone(&durable_kv) as Arc<dyn OutboxStore>,
            )
            .with_atomic_commit(Arc::clone(&durable_kv) as Arc<dyn AtomicOutboxCommit>)
            .with_steps(Arc::new(KvStepStore::new(Arc::clone(&inner))) as Arc<dyn StepTokenStore>);

            // 实例驱动租约：一实例一驱动；fenced_commit 复用同一 outbox store 的
            // 键布局（见 `KvInstanceLease`）。
            let instance_lease: Arc<dyn InstanceLease> = Arc::new(KvInstanceLease::new(
                Arc::clone(&inner),
                DURABLE_INSTANCE_LEASE_TTL_SECS,
                Arc::clone(&durable_kv),
            ));

            // v2 审批桥：审批存储与 outbox 共库（同 `inner`），席位角色经配置映射解析。
            // 只有 durable 路径注入；非 durable / 内存路径 `approval = None` ⇒ 审批命令 fail-closed。
            let approval_store: Arc<dyn ApprovalStore> = Arc::new(KvApprovalStore::new(
                Arc::clone(&inner),
                Arc::clone(&durable_kv),
            ));
            let approval_bridge = Arc::new(ApprovalBridge::new(
                approval_store,
                Arc::new(SystemClock) as Arc<dyn Clock>,
                Some(Arc::new(MapApproverResolver::new(
                    opts.approval_roles.clone(),
                ))),
            ));

            let mut svc = Self::assemble(
                store,
                Some(kv_store),
                Arc::new(HttpTaskDispatcher::from_config(dispatch)),
                event_provider,
                Some(instance_lease),
                Some(support),
                Some(Arc::clone(&durable_kv)),
                Some(relay),
                opts.relay_enabled,
                opts.relay_interval_ms,
                Some(approval_bridge),
            );
            // topic 创建推迟到 `start()`（届时 MQ 已就绪）；此处只留句柄。
            svc.durable_mq = Some(opts.mq);
            svc.durable_topic = opts.topic;
            svc
        }

        /// 共享装配：store + dispatcher 决定 runtime，其余字段相同（内存路径）。
        fn with_dispatcher(
            store: Arc<dyn WorkflowStore + Send + Sync>,
            kv_store: Option<Arc<KvWorkflowStore>>,
            dispatcher: Arc<dyn TaskDispatcher + Send + Sync>,
        ) -> Self {
            Self::assemble(
                store,
                kv_store,
                dispatcher,
                Arc::new(MemoryEventProvider::new()) as Arc<dyn EventProvider + Send + Sync>,
                None,
                None,
                None,
                None,
                false,
                1000,
                None,
            )
        }

        /// 确保 durable 事件 topic 已创建（create-if-absent）。
        ///
        /// 只能在 MQ 就绪后调用：`create_topic` 要求 MQ 已 `start()`（redb 打开），
        /// 而 durable 引擎的构造早于 `start_all()`。因此先**有界**等 `is_started`，
        /// 超时则 fail-closed 报错（不静默不投递）。已存在时跳过——构造期若 MQ
        /// 已启动（如测试先起 MQ），当时的 `create_topic` 已建过。
        async fn ensure_durable_topic(
            mq: &crate::services::mq::MessageQueueService,
            topic: &str,
        ) -> Result<(), String> {
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
            while !mq.is_started() {
                if tokio::time::Instant::now() >= deadline {
                    return Err(format!(
                        "workflow durable: MQ service not started within 30s; cannot ensure \
                         events topic '{topic}'"
                    ));
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            if mq
                .get_topic_config(topic)
                .map_err(|e| format!("workflow durable: read topic '{topic}' failed: {e}"))?
                .is_none()
            {
                // 与 `MqOutboxSink` / `MqEventProvider` 的建 topic 参数逐字一致。
                if let Err(create_err) = mq.create_topic(
                    topic,
                    crate::services::mq::TopicConfig {
                        partitions: 1,
                        retention_secs: 7 * 24 * 3600,
                        max_message_size: 1024 * 1024,
                    },
                ) {
                    // 只在并发创建竞争（另一驱动者已建同名 topic）下容忍失败：
                    // 回读确认存在则不报错；仍不存在（或回读也失败）则 fail-closed，
                    // 否则投递会在无 topic 下静默停摆。
                    match mq.get_topic_config(topic) {
                        Ok(Some(_)) => {}
                        Ok(None) => {
                            return Err(format!(
                                "workflow durable: failed to create events topic '{topic}': \
                                 {create_err}"
                            ));
                        }
                        Err(read_err) => {
                            return Err(format!(
                                "workflow durable: create topic '{topic}' failed ({create_err}); \
                                 re-read failed: {read_err}"
                            ));
                        }
                    }
                }
            }
            Ok(())
        }

        /// 装配入口（内存路径与 durable 路径共用；不破坏既有构造签名）。
        #[allow(clippy::too_many_arguments)]
        fn assemble(
            store: Arc<dyn WorkflowStore + Send + Sync>,
            kv_store: Option<Arc<KvWorkflowStore>>,
            dispatcher: Arc<dyn TaskDispatcher + Send + Sync>,
            event_provider: Arc<dyn EventProvider + Send + Sync>,
            instance_lease: Option<Arc<dyn coord_core::workflow::ports::InstanceLease>>,
            durable: Option<coord_core::workflow::durable::DurableSupport>,
            durable_store: Option<Arc<crate::services::outbox_kv::KvOutboxStore>>,
            outbox_relay: Option<Arc<coord_core::workflow::durable::OutboxRelay>>,
            relay_enabled: bool,
            relay_interval_ms: u64,
            approval: Option<Arc<ApprovalBridge>>,
        ) -> Self {
            let expression = ExpressionEvaluator::new();
            let clock = SystemClock;
            let executor = coord_core::workflow::engine::WorkflowExecutor::new(
                expression.clone(),
                clock.clone(),
            );
            let mut runtime = WorkflowRuntime::new(
                executor,
                clock.clone(),
                Arc::clone(&store),
                dispatcher,
                event_provider,
            );
            if let Some(lease) = instance_lease {
                runtime = runtime.with_instance_lease(lease);
            }
            if let Some(durable) = durable {
                runtime = runtime.with_durable(durable);
            }
            // 审批桥双注入：runtime 用它开启/恢复审批节点，service 用它执行命令/查询。
            if let Some(bridge) = approval.as_ref() {
                runtime = runtime.with_approval_bridge(Arc::clone(bridge));
            }

            Self {
                store,
                kv_store,
                runtime: Some(Arc::new(runtime)),
                expression,
                clock,
                durable_store,
                outbox_relay,
                relay_enabled,
                relay_interval_ms,
                durable_mq: None,
                durable_topic: String::new(),
                approval,
            }
        }

        /// 创建测试用 WorkflowEngineService（使用 NoopTaskDispatcher + NoopEventProvider）
        ///
        /// 测试中不使用真实 HTTP 调用和事件总线，避免外部依赖。内存路径无审批桥
        /// （`approval = None`），审批命令/查询 fail-closed。
        #[cfg(test)]
        pub fn new_for_test() -> Self {
            Self::new_for_test_with_approval(None)
        }

        /// 内存路径 + 可选审批桥（测试用）：驱动路径需要桥时经它注入 `runtime`。
        #[cfg(test)]
        pub fn new_for_test_with_approval(approval: Option<Arc<ApprovalBridge>>) -> Self {
            let store: Arc<dyn WorkflowStore + Send + Sync> = Arc::new(MemoryWorkflowStore::new());
            let expression = ExpressionEvaluator::new();
            let clock = SystemClock;
            let executor = coord_core::workflow::engine::WorkflowExecutor::new(
                expression.clone(),
                clock.clone(),
            );
            let mut runtime = WorkflowRuntime::new(
                executor,
                clock.clone(),
                Arc::clone(&store),
                Arc::new(NoopTaskDispatcher) as Arc<dyn TaskDispatcher + Send + Sync>,
                Arc::new(NoopEventProvider) as Arc<dyn EventProvider + Send + Sync>,
            );
            if let Some(bridge) = approval.as_ref() {
                runtime = runtime.with_approval_bridge(Arc::clone(bridge));
            }

            Self {
                store,
                kv_store: None,
                runtime: Some(Arc::new(runtime)),
                expression,
                clock,
                durable_store: None,
                outbox_relay: None,
                relay_enabled: false,
                relay_interval_ms: 1000,
                durable_mq: None,
                durable_topic: String::new(),
                approval,
            }
        }

        fn runtime(&self) -> Result<&Arc<EngineRuntime>, WorkflowEngineError> {
            self.runtime
                .as_ref()
                .ok_or_else(|| WorkflowEngineError::Internal("runtime not initialized".to_string()))
        }

        /// 工作流后台 worker 的存活快照（供 agent 层指标/告警拉取）。
        ///
        /// `None` = 运行时未初始化（不启动任何后台任务 ⇒ 无故障可言）。
        pub fn worker_liveness(&self) -> Option<coord_core::workflow::runtime::WorkerLiveness> {
            self.runtime.as_ref().map(|rt| rt.worker_liveness())
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
                        errors
                            .iter()
                            .map(|e| e.message.clone())
                            .collect::<Vec<_>>()
                            .join("; "),
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
            self.store
                .save_definition_atomic(&def)
                .await
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
                        errors
                            .iter()
                            .map(|e| e.message.clone())
                            .collect::<Vec<_>>()
                            .join("; "),
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
            self.store
                .save_definition_atomic(&def)
                .await
                .map_err(|e| DeployError::Store(format!("store error: {e}")))?;

            Ok(def)
        }

        pub async fn get_definition(
            &self,
            definition_id: &str,
        ) -> Result<Option<WorkflowDefinition>, String> {
            let all_defs = self
                .store
                .list_definitions("", usize::MAX, None)
                .await
                .map_err(|e| format!("store error: {e}"))?;
            Ok(all_defs
                .into_iter()
                .find(|d| d.id.as_deref() == Some(definition_id)))
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
        ) -> Result<WorkflowInstance, WorkflowEngineError> {
            self.start_instance_as(definition_id, input, None).await
        }

        /// 启动实例并把发起人（CCT `sub`）钉到实例上。
        ///
        /// `initiator` 必须经本参数注入（而非启动后回填）：审批节点开启时桥读实例的
        /// `initiator` 做自审判定，事后回填会与异步驱动竞争并可能让审批实例记成
        /// `anonymous`。`None` = 未认证路径（旧快照兼容）。
        pub async fn start_instance_as(
            &self,
            definition_id: &str,
            input: Value,
            initiator: Option<&str>,
        ) -> Result<WorkflowInstance, WorkflowEngineError> {
            let def = self
                .get_definition(definition_id)
                .await
                .map_err(|e| WorkflowEngineError::Internal(format!("store error: {e}")))?
                .ok_or_else(|| {
                    WorkflowEngineError::NotFound(format!(
                        "workflow definition not found: {definition_id}"
                    ))
                })?;

            self.runtime()?
                .start_as(&def, input, initiator)
                .await
                .map_err(WorkflowEngineError::from)
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

        /// 当前状态名：按实例钉死的定义，把引擎任务名归一化为定义中声明的状态名
        /// （issue #101）。定义读不到时回退为原始任务名 —— 读路径不因存储抖动失败。
        pub async fn current_state_name(&self, inst: &WorkflowInstance) -> String {
            let raw = inst.current_task_name();
            match self
                .store
                .load_definition(
                    &inst.definition_ns,
                    &inst.definition_name,
                    &inst.definition_version,
                )
                .await
            {
                Ok(Some(def)) => def.declared_state_name(raw),
                _ => raw.to_string(),
            }
        }

        pub async fn signal_instance(
            &self,
            instance_id: &str,
            signal_name: &str,
            payload: Value,
        ) -> Result<WorkflowInstance, WorkflowEngineError> {
            self.resume_instance(instance_id, Some(signal_name), Some(payload), None)
                .await
        }

        pub async fn resume_instance(
            &self,
            instance_id: &str,
            signal_name: Option<&str>,
            payload: Option<Value>,
            idempotency_key: Option<&str>,
        ) -> Result<WorkflowInstance, WorkflowEngineError> {
            self.runtime()?
                .resume(instance_id, signal_name, payload, idempotency_key)
                .await
                .map_err(WorkflowEngineError::from)
        }

        // ─── 审批 v2（ADR-0013 D7）命令 / 查询 ───

        /// 审批桥（`None` ⇒ 非 durable 装配）。
        fn approval_bridge(&self) -> Result<&Arc<ApprovalBridge>, BridgeError> {
            self.approval.as_ref().ok_or_else(|| {
                BridgeError::InvalidTransition("workflow durable engine disabled".into())
            })
        }

        /// 执行审批命令并按其节点结论驱动工作流。
        ///
        /// `Duplicate`（幂等键命中）原样返回首次响应，不重复驱动；`Applied` 时按分支
        /// 结论 / 实例终态恢复或故障工作流（与截止路径共用 `apply_approval_node_outcome`）。
        ///
        /// `ReturnToNode` 例外：桥只重开目标节点分支、工作流仍挂在返回节点上，故命令
        /// 生效后额外 `resume_approval(.., Some(target_node))` 把实例重新驱动到目标节点
        /// （否则目标分支通过时会按返回节点的 `onPass` 恢复，跳过中间审批节点）。
        pub async fn approval_command(
            &self,
            workflow_id: &str,
            actor: &str,
            cmd: ApprovalCommand<'_>,
            idempotency_key: Option<&str>,
        ) -> Result<(String, String), BridgeError> {
            let bridge = self.approval_bridge()?;
            // `ReturnToNode` 的恢复目标须在命令被桥消费前取出：桥只重开**目标节点**的
            // 分支，工作流仍挂在**返回节点**上。若不把实例重新驱动到目标节点，目标分支
            // 通过时 `apply_approval_node_outcome` 会用返回节点的 `onPass` 恢复，跳过
            // 二者之间的所有审批节点。
            let return_target = match &cmd {
                ApprovalCommand::ReturnToNode { target_node, .. } => {
                    Some((*target_node).to_string())
                }
                _ => None,
            };
            match bridge
                .command(workflow_id, actor, cmd, idempotency_key)
                .await?
            {
                CommandOutcome::Duplicate(response) => approval_response_status(&response),
                CommandOutcome::Applied {
                    response,
                    node_outcome,
                    terminal,
                } => {
                    match return_target {
                        Some(target) => {
                            // 重新驱动到目标节点：其重开分支已是 `Open`，`ensure_node_open`
                            // 复用并重新挂起（attempt 已递增）。恢复失败即命令失败——绝不
                            // 静默返回成功却停在错误的节点上。
                            let runtime = self.runtime.as_ref().ok_or_else(|| {
                                BridgeError::Store("runtime not initialized".into())
                            })?;
                            runtime
                                .resume_approval(
                                    workflow_id,
                                    serde_json::json!({"status": "returned"}),
                                    Some(&target),
                                )
                                .await
                                .map_err(map_runtime_to_bridge)?;
                        }
                        None => {
                            self.drive_approval_outcome(workflow_id, node_outcome, terminal)
                                .await?;
                        }
                    }
                    approval_response_status(&response)
                }
            }
        }

        /// 审批结论 → 工作流恢复 / 路由 / 故障。
        ///
        /// 通过（分支本轮 `Passed`）优先：经共享 helper 恢复（honor 节点 `on_pass`）。
        /// 实例终态 `Rejected` 同样走 helper（`on_reject` 路由或 `approval_rejected` 故障）；
        /// `Cancelled`/`Terminated` 按既有口径置 fault。`Completed` 由工作流驱动收尾
        /// （`finish_if_complete`）达成，命令路径无额外动作。
        async fn drive_approval_outcome(
            &self,
            workflow_id: &str,
            node_outcome: Option<NodeOutcome>,
            terminal: Option<ApprovalOutcome>,
        ) -> Result<(), BridgeError> {
            let runtime = self
                .runtime
                .as_ref()
                .ok_or_else(|| BridgeError::Store("runtime not initialized".into()))?;
            if node_outcome == Some(NodeOutcome::Passed) {
                runtime
                    .apply_approval_node_outcome(workflow_id, NodeOutcome::Passed)
                    .await
                    .map_err(map_runtime_to_bridge)?;
                return Ok(());
            }
            match terminal {
                Some(ApprovalOutcome::Rejected) => {
                    runtime
                        .apply_approval_node_outcome(workflow_id, NodeOutcome::Rejected)
                        .await
                        .map_err(map_runtime_to_bridge)?;
                }
                Some(ApprovalOutcome::Cancelled) => {
                    runtime
                        .fail_approval(
                            workflow_id,
                            "approval_cancelled",
                            "approval cancelled without an on_reject target",
                        )
                        .await
                        .map_err(map_runtime_to_bridge)?;
                }
                Some(ApprovalOutcome::Terminated) => {
                    runtime
                        .fail_approval(workflow_id, "approval_terminated", "approval terminated")
                        .await
                        .map_err(map_runtime_to_bridge)?;
                }
                Some(ApprovalOutcome::Completed) | None => {}
            }
            Ok(())
        }

        /// 待办审批席位查询（`open_tasks_for(actor)` 跨实例投影 + 实例 id 分页）。
        ///
        /// `page_size == 0 ⇒ DEFAULT_LIST_PAGE_SIZE`；`page_token` = 上一页最后
        /// 扫描到的实例 id。返回 `(tasks, next_page_token)`，`None` 表示已到末页。
        pub async fn list_pending_tasks(
            &self,
            actor: &str,
            page_size: usize,
            page_token: Option<&str>,
        ) -> Result<(Vec<PendingTaskView>, Option<String>), BridgeError> {
            let bridge = self.approval_bridge()?;
            let requested = if page_size == 0 {
                coord_core::workflow::ports::DEFAULT_LIST_PAGE_SIZE
            } else {
                page_size
            };
            let limit = coord_core::workflow::approval::store::clamp_scan_limit(requested);
            let entries = bridge
                .store()
                .scan(page_token, limit)
                .await
                .map_err(BridgeError::from)?;
            let scanned = entries.len();
            let mut tasks = Vec::new();
            for entry in &entries {
                let engine = &entry.engine;
                for task in engine.open_tasks_for(actor) {
                    let node = engine
                        .branches
                        .get(&task.branch_id)
                        .map(|b| b.node.clone())
                        .unwrap_or_default();
                    tasks.push(PendingTaskView {
                        instance_id: entry.id.clone(),
                        task_id: task.id.clone(),
                        node,
                        branch_id: task.branch_id.clone(),
                        round_id: task.round_id.clone(),
                        kind: json_label(&task.kind),
                        status: json_label(&task.status),
                        assignee: task.assignee.clone().unwrap_or_default(),
                        deadline_ms: task.deadline.unwrap_or(0),
                        created_at_ms: task.created_at,
                    });
                }
            }
            let next = if scanned < limit {
                None
            } else {
                entries.last().map(|e| e.id.clone())
            };
            Ok((tasks, next))
        }

        /// 审批实例详情（引擎快照投影）。
        pub async fn get_instance_detail(
            &self,
            workflow_id: &str,
        ) -> Result<InstanceDetailView, BridgeError> {
            let bridge = self.approval_bridge()?;
            let engine = bridge
                .store()
                .load(workflow_id)
                .await
                .map_err(BridgeError::from)?
                .map(|(engine, _rev)| engine)
                .ok_or_else(|| BridgeError::NotFound(workflow_id.to_string()))?;
            Ok(InstanceDetailView {
                instance_id: engine.instance.id.clone(),
                status: json_label(&engine.instance.status).to_uppercase(),
                initiator: engine.instance.initiator.clone(),
                revision: engine.instance.revision,
                definition_name: engine.instance.definition_name.clone(),
                branches: engine.branches.values().cloned().collect(),
                tasks: engine.tasks.values().cloned().collect(),
                rounds: engine.rounds.values().cloned().collect(),
            })
        }

        /// 取消实例（控制面）：非终态 → `Cancelled` + `workflow.cancelled`（同一次提交）。
        ///
        /// 经 [`WorkflowRuntime::cancel_instance`] 的 CAS 写路径：线性一致读修订号 +
        /// 冲突重放，并发完成的终态不会被取消覆盖。终态 ⇒ `FailedPrecondition`，
        /// 实例不存在 ⇒ `NotFound`。
        pub async fn cancel_instance(&self, instance_id: &str) -> Result<(), WorkflowEngineError> {
            self.runtime()?
                .cancel_instance(instance_id)
                .await
                .map_err(WorkflowEngineError::from)
        }

        /// 列出工作流实例（过滤 + 分页）——契约见 [`WorkflowStore::list_instances`]。
        pub async fn list_instances(
            &self,
            namespace: Option<&str>,
            definition_name: Option<&str>,
            page_size: usize,
            page_token: Option<&str>,
        ) -> Result<(Vec<WorkflowInstance>, Option<String>), String> {
            self.store
                .list_instances(namespace, definition_name, page_size, page_token)
                .await
                .map_err(|e| format!("store error: {e}"))
        }

        /// 测试辅助：暴露 store 以构造确定性状态（仅测试构建可见）。
        #[cfg(test)]
        pub(crate) fn store_for_test(
            &self,
        ) -> Arc<dyn coord_core::workflow::ports::WorkflowStore + Send + Sync> {
            Arc::clone(&self.store)
        }

        /// 删除实例（保留策略，G-WF-1）。
        ///
        /// 仅终态实例（COMPLETED / FAULTED / CANCELLED）可删；运行/等待/挂起中
        /// 返回 `FailedPrecondition`（先 Cancel）。删除不可回滚（无归档副本）。
        /// 读与删之间的并发删除按幂等成功处理（终态一致：不存在）。
        pub async fn delete_instance(&self, instance_id: &str) -> Result<(), WorkflowEngineError> {
            let inst = self
                .store
                .load_instance(instance_id)
                .await
                .map_err(|e| WorkflowEngineError::Internal(format!("store error: {e}")))?
                .ok_or_else(|| {
                    WorkflowEngineError::NotFound(format!("instance not found: {instance_id}"))
                })?;

            if !inst.status.is_terminal() {
                return Err(WorkflowEngineError::FailedPrecondition(format!(
                    "instance is not in a terminal state ({:?}); cancel it first",
                    inst.status
                )));
            }

            let _ = self
                .store
                .delete_instance(instance_id)
                .await
                .map_err(|e| WorkflowEngineError::Internal(format!("store error: {e}")))?;
            Ok(())
        }

        /// 删除定义（保留策略，G-WF-1）。
        ///
        /// 守卫：**任何**实例（含终态）引用该 `(namespace, name, version)` 都
        /// 不允许删除（先删实例）。删除后 `GetDefinition` / 回滚到该版本返回
        /// `NotFound`；同名同版本可重新 Deploy（= 全新定义）。
        pub async fn delete_definition(
            &self,
            namespace: &str,
            name: &str,
            version: &str,
        ) -> Result<(), WorkflowEngineError> {
            let exists = self
                .store
                .load_definition(namespace, name, version)
                .await
                .map_err(|e| WorkflowEngineError::Internal(format!("store error: {e}")))?
                .is_some();
            if !exists {
                return Err(WorkflowEngineError::NotFound(format!(
                    "definition not found: {namespace}/{name}@{version}"
                )));
            }

            // 引用检查以存储为准（KV 后端会全量扫实例表并刷新缓存，避免缓存滞后
            // 误放行）。保留策略是低频运维操作，全量扫描代价可接受。
            let (instances, _) = self
                .store
                .list_instances(None, None, usize::MAX, None)
                .await
                .map_err(|e| WorkflowEngineError::Internal(format!("store error: {e}")))?;
            let referenced = instances.iter().any(|i| {
                i.definition_ns == namespace
                    && i.definition_name == name
                    && i.definition_version == version
            });
            if referenced {
                return Err(WorkflowEngineError::FailedPrecondition(format!(
                    "definition {namespace}/{name}@{version} is still referenced by one or more instances; delete them first"
                )));
            }

            let _ = self
                .store
                .delete_definition(namespace, name, version)
                .await
                .map_err(|e| WorkflowEngineError::Internal(format!("store error: {e}")))?;
            Ok(())
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
                kv_store: self.kv_store.clone(),
                runtime: self.runtime.clone(),
                expression: self.expression.clone(),
                clock: self.clock.clone(),
                durable_store: self.durable_store.clone(),
                outbox_relay: self.outbox_relay.clone(),
                relay_enabled: self.relay_enabled,
                relay_interval_ms: self.relay_interval_ms,
                durable_mq: self.durable_mq.clone(),
                durable_topic: self.durable_topic.clone(),
                approval: self.approval.clone(),
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
            // R-AGT-09：KV 持久化后端启动恢复——全量重建本地缓存 + watch 订阅
            if let Some(kv_store) = &self.kv_store {
                kv_store
                    .init()
                    .await
                    .map_err(|e| format!("workflow KvWorkflowStore init failed: {e}"))?;
                tracing::info!("Workflow engine started with KvWorkflowStore (raft-persisted)");
            }

            // durable 装配：此刻 MQ 插件已 start()（装配期构造早于 start_all，构造时
            // 建 topic 必失败）。在拉起 relay / 恢复驱动之前确保事件 topic 存在，
            // 否则产出与订阅都会报 topic not found。
            //
            // 顺序不变式：`PluginManager::start_all()` 按插件名 BTreeMap 序启动，
            // MQ 插件名 `"mq"` < 本引擎名 `"workflow-engine"` ⇒ MQ 先启动。此处
            // 的 30s 有界等待只是防该命名/排序变化的保险，不能替代它。
            if let Some(mq) = &self.durable_mq {
                Self::ensure_durable_topic(mq, &self.durable_topic).await?;
            }

            // Spike 3：持久化恢复 —— 到期定时器先触发，再对账挂起实例（重订阅 listen、
            // 补写 wait/retry 定时器），最后拉起周期扫描与 relay。
            // 恢复必须先于投递：relay 一启动就会把崩溃前遗留的 outbox 事件投出去，
            // 而此刻实例尚未重建驱动/重订阅 —— 先投递会让消费者看到「无人在驱动」的实例。
            if let (Some(runtime), Some(_durable)) = (&self.runtime, &self.durable_store) {
                runtime.recover_durable_state().await;
                runtime.start_durable_workers();
                tracing::info!("Workflow engine durable recovery done (outbox + persisted timers)");
            }
            if self.relay_enabled {
                if let Some(relay) = &self.outbox_relay {
                    let _handle = crate::services::outbox_relay::spawn_outbox_relay(
                        Arc::clone(relay),
                        std::time::Duration::from_millis(self.relay_interval_ms.max(1)),
                    );
                    tracing::info!(
                        interval_ms = self.relay_interval_ms,
                        "Workflow outbox relay started"
                    );
                }
            }
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
    use coord_core::workflow::model::{
        InstanceStatus, Task, TaskFrame, TaskStatus, WorkflowInstance,
    };
    use coord_core::workflow::ports::{DispatchResult, TaskDispatcher};

    use std::collections::HashMap;
    use std::sync::Arc;

    use coord_core::workflow::approval::bridge::{
        ApprovalBridge, ApprovalCommand, BridgeError, MapApproverResolver,
    };
    use coord_core::workflow::approval::model::JoinPolicy;
    use coord_core::workflow::approval::store::{ApprovalStore, MemoryApprovalStore};
    use coord_core::workflow::ports::{Clock, SystemClock};

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

        let inst = svc
            .start_instance(&def_id, serde_json::json!({}))
            .await
            .unwrap();
        assert!(!inst.id.is_empty());
        assert_eq!(inst.definition_name, "linear-wf");
        // 标准相位：start 创建即 Pending，drive 首步推进 Running；call 任务通过
        // NoopTaskDispatcher 立即完成，实例可能已 Pending/Running/Waiting/Completed
        assert!(matches!(
            inst.status,
            InstanceStatus::Pending
                | InstanceStatus::Running
                | InstanceStatus::Waiting
                | InstanceStatus::Suspended
                | InstanceStatus::Completed
        ));
    }

    #[tokio::test]
    async fn test_engine_get_instance_status() {
        let svc = WorkflowEngineService::new_for_test();
        let yaml = sample_linear_yaml();
        let def_id = svc.deploy_definition("test", &yaml).await.unwrap();
        let inst = svc
            .start_instance(&def_id, serde_json::json!({}))
            .await
            .unwrap();

        let loaded = svc.get_instance(&inst.id).await.unwrap();
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().id, inst.id);
    }

    /// issue #101：`GetStatus.current_state_name` 不得暴露编译期内部任务名。
    #[tokio::test]
    async fn test_current_state_name_normalizes_internal_route_task() {
        let svc = WorkflowEngineService::new_for_test();
        let yaml = r#"
id: onboarding
version: "1.0"
start: autostart
states:
  - name: autostart
    type: event
    onEvents:
      - eventRefs: [ { eventRef: activate } ]
        transition: human
      - eventRefs: [ { eventRef: void } ]
        transition: cancelpath
  - name: human
    type: inject
    data: { step: human }
    transition: cancelpath
  - name: cancelpath
    type: inject
    data: {}
    end: true
events:
  - name: activate
    type: icps.activation
  - name: void
    type: icps.void
"#;
        let def_id = svc.deploy_definition("test", yaml).await.unwrap();
        let def = svc.get_definition(&def_id).await.unwrap().unwrap();

        let frame = |name: &str| TaskFrame {
            task_name: name.to_string(),
            task_type: "switch".to_string(),
            status: TaskStatus::Completed,
            input: None,
            output: None,
            started_at: None,
            ended_at: None,
            retry_count: 0,
            pending_branches: None,
        };

        // 事件状态挂起在 listen 上：当前任务名即状态名，归一化后不变
        let mut inst = WorkflowInstance::new(&def, serde_json::json!({}), 0);
        inst.task_stack.push(frame("autostart"));
        inst.current_task_index = 0;
        assert_eq!(svc.current_state_name(&inst).await, "autostart");

        // route Goto 提交窗口：index 已指向下一状态，栈顶仍是 `<状态>__route` 帧。
        // 读侧必须归一化到声明状态名。
        inst.task_stack.push(frame("autostart__route"));
        inst.current_task_index = inst.task_stack.len();
        assert_eq!(svc.current_state_name(&inst).await, "autostart");
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
        let inst = svc
            .start_instance(&def_id, serde_json::json!({}))
            .await
            .unwrap();

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
    async fn test_engine_delete_instance_requires_terminal_state() {
        let svc = WorkflowEngineService::new_for_test();
        let store = svc.store_for_test();

        // 非终态（挂起/运行）不可删，必须 FailedPrecondition
        let yaml = sample_wait_yaml();
        let def_id = svc.deploy_definition("test", &yaml).await.unwrap();
        let inst = svc
            .start_instance(&def_id, serde_json::json!({}))
            .await
            .unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        let loaded = svc.get_instance(&inst.id).await.unwrap().unwrap();
        if !loaded.status.is_terminal() {
            let err = svc.delete_instance(&inst.id).await.unwrap_err();
            assert!(
                matches!(err, WorkflowEngineError::FailedPrecondition(_)),
                "expected FailedPrecondition for non-terminal instance, got {err:?}"
            );
        }

        // 终态可删；再删 → NotFound（不存在报错，不是静默成功）
        let mut terminal = loaded.clone();
        terminal.status = InstanceStatus::Cancelled;
        store.save_instance(&terminal).await.unwrap();
        svc.delete_instance(&inst.id).await.unwrap();
        assert!(svc.get_instance(&inst.id).await.unwrap().is_none());
        let err = svc.delete_instance(&inst.id).await.unwrap_err();
        assert!(
            matches!(err, WorkflowEngineError::NotFound(_)),
            "second delete should be NotFound, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_engine_delete_definition_guard_and_roundtrip() {
        let svc = WorkflowEngineService::new_for_test();
        let store = svc.store_for_test();

        let yaml = sample_linear_yaml();
        let def_id = svc.deploy_definition("test", &yaml).await.unwrap();
        let def = svc.get_definition(&def_id).await.unwrap().unwrap();
        let (ns, name, version) = (
            def.document.namespace.clone(),
            def.document.name.clone(),
            def.document.version.clone(),
        );

        // 直接构造一个引用该定义的终态实例（确定性；不依赖运行时推进节奏）
        let inst = WorkflowInstance {
            id: "inst-ref-1".into(),
            definition_ns: ns.clone(),
            definition_name: name.clone(),
            definition_version: version.clone(),
            status: InstanceStatus::Cancelled,
            context: serde_json::json!({}),
            task_stack: vec![],
            current_task_index: 0,
            created_at: 0,
            updated_at: 0,
            output: None,
            fault: None,
            suspension_meta: None,
            initiator: None,
            definition_hash: None,
            engine_version: None,
        };
        store.save_instance(&inst).await.unwrap();

        // 实例引用存在（任意状态）⇒ 拒绝删定义
        let err = svc
            .delete_definition(&ns, &name, &version)
            .await
            .unwrap_err();
        assert!(
            matches!(err, WorkflowEngineError::FailedPrecondition(_)),
            "expected FailedPrecondition while instances reference the definition, got {err:?}"
        );

        // 删除引用实例后可删定义；再删 → NotFound
        svc.delete_instance(&inst.id).await.unwrap();
        svc.delete_definition(&ns, &name, &version).await.unwrap();
        assert!(svc.get_definition(&def_id).await.unwrap().is_none());
        let err = svc
            .delete_definition(&ns, &name, &version)
            .await
            .unwrap_err();
        assert!(
            matches!(err, WorkflowEngineError::NotFound(_)),
            "second delete should be NotFound, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_engine_signal_resume_wait_workflow() {
        let svc = WorkflowEngineService::new_for_test();
        let yaml = sample_wait_yaml();
        let def_id = svc.deploy_definition("test", &yaml).await.unwrap();
        let inst = svc
            .start_instance(&def_id, serde_json::json!({}))
            .await
            .unwrap();

        // call 任务会挂起，给 drive loop 一点时间
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let loaded = svc.get_instance(&inst.id).await.unwrap().unwrap();
        assert_eq!(
            loaded.status,
            InstanceStatus::Waiting,
            "wait task should be in waiting phase (auto-resume)"
        );

        let resumed = svc
            .signal_instance(&inst.id, "approved", serde_json::json!({"result": "ok"}))
            .await
            .unwrap();
        assert!(!resumed.id.is_empty());
    }

    #[tokio::test]
    async fn test_engine_list_definitions() {
        let svc = WorkflowEngineService::new_for_test();
        svc.deploy_definition("ns-a", &sample_linear_yaml())
            .await
            .unwrap();
        svc.deploy_definition("ns-a", &sample_wait_yaml())
            .await
            .unwrap();
        svc.deploy_definition("ns-b", &sample_switch_yaml())
            .await
            .unwrap();

        let list_a = svc.list_definitions("ns-a", 10, None).await.unwrap();
        assert_eq!(list_a.len(), 2, "ns-a should have 2 definitions");

        let list_b = svc.list_definitions("ns-b", 10, None).await.unwrap();
        assert_eq!(list_b.len(), 1, "ns-b should have 1 definition");
    }

    #[tokio::test]
    async fn test_engine_list_instances() {
        let svc = WorkflowEngineService::new_for_test();
        let def_id = svc
            .deploy_definition("test", &sample_linear_yaml())
            .await
            .unwrap();

        svc.start_instance(&def_id, serde_json::json!({}))
            .await
            .unwrap();
        svc.start_instance(&def_id, serde_json::json!({}))
            .await
            .unwrap();

        let (instances, next_token) = svc
            .list_instances(None, Some("linear-wf"), 10, None)
            .await
            .unwrap();
        assert_eq!(instances.len(), 2);
        assert!(next_token.is_none(), "一页装下全部时不得给续读令牌");
    }

    #[tokio::test]
    async fn test_engine_idempotent_resume() {
        let svc = WorkflowEngineService::new_for_test();
        // 使用包含 wait 任务的工作流，确保实例会挂起
        let yaml = sample_wait_yaml();
        let def_id = svc.deploy_definition("test", &yaml).await.unwrap();
        let inst = svc
            .start_instance(&def_id, serde_json::json!({}))
            .await
            .unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // 实例应该处于 Waiting 状态（在 wait 步骤，自动恢复）
        let loaded = svc.get_instance(&inst.id).await.unwrap().unwrap();
        assert_eq!(
            loaded.status,
            InstanceStatus::Waiting,
            "instance should be waiting on wait task"
        );

        let r1 = svc
            .resume_instance(
                &inst.id,
                Some("approved"),
                Some(serde_json::json!({"ok": true})),
                Some("key-001"),
            )
            .await;
        assert!(r1.is_ok(), "first resume should succeed");

        let r2 = svc
            .resume_instance(
                &inst.id,
                Some("approved"),
                Some(serde_json::json!({"ok": true})),
                Some("key-001"),
            )
            .await;
        assert!(
            r2.is_ok(),
            "duplicate idempotent resume should succeed (no-op)"
        );
    }

    // ─── startByDefinition 真契约 + event 多 onEvents 路由 + signal typed 错误 ───

    /// CNCF SW 审批流：event 状态双 onEvents（approve → notify / reject → reject）
    fn sample_approval_sw_yaml() -> String {
        r#"
id: approval-wf
version: "1.0"
start: approve
states:
  - name: approve
    type: event
    onEvents:
      - eventRefs: [ { "eventRef": "approved" } ]
        transition: notify
      - eventRefs: [ { "eventRef": "rejected" } ]
        transition: reject
  - name: notify
    type: inject
    data: { result: "approved" }
    end: true
  - name: reject
    type: inject
    data: { result: "rejected" }
    end: true
events:
  - name: approved
    type: icps.approval.approved
  - name: rejected
    type: icps.approval.rejected
"#
        .to_string()
    }

    #[tokio::test]
    async fn test_engine_start_by_definition_id_with_input() {
        let svc = WorkflowEngineService::new_for_test();
        let def_id = svc
            .deploy_definition("test", &sample_approval_sw_yaml())
            .await
            .unwrap();

        // 真契约：按已部署定义 ID 启动（携带 input）
        let inst = svc
            .start_instance(&def_id, serde_json::json!({"orderId": "ORD-1"}))
            .await
            .unwrap();
        assert_eq!(inst.definition_name, "approval-wf");

        // 定义不存在 → NotFound（gRPC → NotFound）
        let err = svc
            .start_instance("def-missing", serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(
            matches!(err, WorkflowEngineError::NotFound(_)),
            "expected NotFound, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_engine_signal_multi_on_events_routes_and_typed_errors() {
        let svc = WorkflowEngineService::new_for_test();
        let def_id = svc
            .deploy_definition("test", &sample_approval_sw_yaml())
            .await
            .unwrap();
        let inst = svc
            .start_instance(&def_id, serde_json::json!({"orderId": "ORD-1"}))
            .await
            .unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

        // listen 挂起：Waiting + suspension_meta.reason == "listen"
        let loaded = svc.get_instance(&inst.id).await.unwrap().unwrap();
        assert_eq!(loaded.status, InstanceStatus::Waiting);
        assert_eq!(loaded.suspension_meta.as_ref().unwrap().reason, "listen");

        // ① signal 名不匹配期望事件类型集合 → InvalidArgument（gRPC → InvalidArgument）
        let err = svc
            .resume_instance(
                &inst.id,
                Some("wrong.signal"),
                Some(serde_json::json!({})),
                None,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, WorkflowEngineError::InvalidArgument(_)),
            "expected InvalidArgument, got {err:?}"
        );

        // ② 实例不存在 → NotFound（gRPC → NotFound）
        let err = svc
            .resume_instance(
                "missing-inst",
                Some("icps.approval.approved"),
                Some(serde_json::json!({})),
                None,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, WorkflowEngineError::NotFound(_)),
            "expected NotFound, got {err:?}"
        );

        // ③ reject 分支：手动 signal 命中期望事件类型 → 按 _signal.name 路由到 reject，实例完成
        let resumed = svc
            .resume_instance(
                &inst.id,
                Some("icps.approval.rejected"),
                Some(serde_json::json!({"ok": false})),
                None,
            )
            .await
            .unwrap();
        assert_eq!(resumed.status, InstanceStatus::Running);
        assert_eq!(resumed.context["_signal"]["name"], "icps.approval.rejected");
        assert_eq!(
            resumed.context["_event"]["eventType"],
            "icps.approval.rejected"
        );

        tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;
        let done = svc.get_instance(&inst.id).await.unwrap().unwrap();
        assert_eq!(
            done.status,
            InstanceStatus::Completed,
            "reject signal should route to reject state and complete"
        );
        // 路由必须落到 reject 分支（inject result=rejected），而非缺省直通 end：
        // 用裸标识符的路由条件恒假，会静默走缺省分支，使 result 缺失
        assert_eq!(
            done.context["result"], "rejected",
            "reject signal must route to the reject branch, not the default branch"
        );

        // ④ 对已完成的终端实例 signal → FailedPrecondition（gRPC → FailedPrecondition）
        let err = svc
            .resume_instance(
                &inst.id,
                Some("icps.approval.approved"),
                Some(serde_json::json!({})),
                None,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, WorkflowEngineError::FailedPrecondition(_)),
            "expected FailedPrecondition, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_engine_signal_approved_routes_to_notify_branch() {
        let svc = WorkflowEngineService::new_for_test();
        let def_id = svc
            .deploy_definition("test", &sample_approval_sw_yaml())
            .await
            .unwrap();
        let inst = svc
            .start_instance(&def_id, serde_json::json!({"orderId": "ORD-2"}))
            .await
            .unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

        let loaded = svc.get_instance(&inst.id).await.unwrap().unwrap();
        assert_eq!(loaded.status, InstanceStatus::Waiting);

        // approve 分支：手动 signal 命中期望事件类型 → 按事件类型路由到 notify
        let resumed = svc
            .resume_instance(
                &inst.id,
                Some("icps.approval.approved"),
                Some(serde_json::json!({"ok": true})),
                None,
            )
            .await
            .unwrap();
        assert_eq!(resumed.status, InstanceStatus::Running);

        tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;
        let done = svc.get_instance(&inst.id).await.unwrap().unwrap();
        assert_eq!(done.status, InstanceStatus::Completed);
        assert_eq!(
            done.context["result"], "approved",
            "approve signal must route to the notify branch, not the default branch"
        );
    }

    #[tokio::test]
    async fn test_engine_switch_workflow() {
        let svc = WorkflowEngineService::new_for_test();
        let yaml = sample_switch_yaml();
        let def_id = svc.deploy_definition("test", &yaml).await.unwrap();
        let inst = svc
            .start_instance(&def_id, serde_json::json!({"amount": 200}))
            .await
            .unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let loaded = svc.get_instance(&inst.id).await.unwrap().unwrap();
        // switch 工作流可能已完成或挂起
        assert!(
            loaded.current_task_index > 0
                || loaded.status == InstanceStatus::Suspended
                || loaded.status == InstanceStatus::Completed,
            "switch should have advanced past the decision task"
        );
    }

    // ─── CNCF Serverless Workflow 权威格式 conformance ───

    /// SW 文档（CNCF 权威格式，start + states[]）
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
        // SW 权威文档直接喂 deployDefinition（原生解析，无转换层）
        let svc = WorkflowEngineService::new_for_test();
        let def_id = svc
            .deploy_definition("icps-flow", sample_cncf_sw())
            .await
            .unwrap();
        assert!(!def_id.is_empty());

        let loaded = svc.get_definition(&def_id).await.unwrap().unwrap();
        assert_eq!(loaded.document.dsl, "cncf-serverless-workflow");
        assert_eq!(loaded.document.name, "order-approval");
        assert_eq!(loaded.document.namespace, "icps-flow");
        // 转换结构：inject → set、operation → call、switch、终端 __end
        assert!(loaded
            .do_tasks
            .iter()
            .any(|t| matches!(t.task, Task::Set(_))));
        assert!(loaded
            .do_tasks
            .iter()
            .any(|t| matches!(t.task, Task::Call(_))));
        assert!(loaded.do_tasks.iter().any(|t| t.name == "check"));
        assert!(loaded.do_tasks.iter().any(|t| t.name == "__end"));
    }

    #[tokio::test]
    async fn test_deploy_cncf_sw_invalid_returns_validation_error() {
        let svc = WorkflowEngineService::new_for_test();
        // delay 状态已支持（P3）；缺 duration 触发校验错误
        let bad = r#"{
          "id": "x", "version": "1.0", "start": "s",
          "states": [ { "name": "s", "type": "delay", "transition": "end" } ]
        }"#;
        let err = svc.deploy_definition("ns", bad).await.unwrap_err();
        match err {
            DeployError::Validation(msg) => {
                assert!(msg.contains("delay requires 'duration'"), "msg: {msg}")
            }
            other => panic!("expected Validation error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_sw_branch_executes_only_chosen_path() {
        // 分支互斥（端到端）：amount=1500 → senior-approve 分支，notify 不得执行
        let svc = WorkflowEngineService::new_for_test();
        let def_id = svc
            .deploy_definition("test", sample_branch_sw())
            .await
            .unwrap();
        let inst = svc
            .start_instance(&def_id, serde_json::json!({"amount": 1500}))
            .await
            .unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let loaded = svc.get_instance(&inst.id).await.unwrap().unwrap();
        assert_eq!(
            loaded.status,
            InstanceStatus::Completed,
            "workflow should complete, fault={:?}",
            loaded.fault
        );

        let ran: Vec<&str> = loaded
            .task_stack
            .iter()
            .map(|f| f.task_name.as_str())
            .collect();
        assert!(
            ran.contains(&"senior-approve"),
            "senior-approve branch should run, stack={ran:?}"
        );
        assert!(
            !ran.contains(&"notify"),
            "notify branch must NOT run (fall-through), stack={ran:?}"
        );
    }

    #[tokio::test]
    async fn test_sw_branch_default_path_executes() {
        // 分支互斥（default）：amount=100 → notify 分支，senior-approve 不得执行
        let svc = WorkflowEngineService::new_for_test();
        let def_id = svc
            .deploy_definition("test", sample_branch_sw())
            .await
            .unwrap();
        let inst = svc
            .start_instance(&def_id, serde_json::json!({"amount": 100}))
            .await
            .unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let loaded = svc.get_instance(&inst.id).await.unwrap().unwrap();
        assert_eq!(
            loaded.status,
            InstanceStatus::Completed,
            "workflow should complete, fault={:?}",
            loaded.fault
        );

        let ran: Vec<&str> = loaded
            .task_stack
            .iter()
            .map(|f| f.task_name.as_str())
            .collect();
        assert!(
            ran.contains(&"notify"),
            "notify branch should run, stack={ran:?}"
        );
        assert!(
            !ran.contains(&"senior-approve"),
            "senior-approve branch must NOT run, stack={ran:?}"
        );
    }

    // ═══ 补偿语义的**端到端**验收═══
    //
    // 为什么必须有这一条：`sw.rs` 的编译期用例
    // （`test_compensated_by_wraps_in_catch_all`）只证明 `compensatedBy` 会被编译成
    // TryCatch 的 catch-all 转场，**不证明失败时补偿真的跑起来**。
    // 「编译出了转场」与「失败时确实转过去并执行了补偿动作」是两件事 ——
    // 引擎侧 TryCatch 的错误路由、以及补偿状态的 dispatch，都可能静默不生效。
    //
    // 判据（三条都要满足才算证据）：
    //   ① 被补偿状态的动作**确实失败**（对端是关闭端口）；
    //   ② 补偿**动作真的发出去了**（由服务端记录到请求，而不是"状态名出现在栈上"）；
    //   ③ 正常路径（`done`）**不得**执行。

    /// 回显 + **记录请求**的本地 HTTP 服务器。
    ///
    /// 与 `spawn_echo_server()` 的区别：它把收到的请求首行留档，供断言
    /// "补偿动作真的发出去了" —— 只看实例的 task_stack 只能证明"状态跑了"，
    /// 证明不了"动作跑了"。
    async fn spawn_recording_server() -> (String, std::sync::Arc<parking_lot::Mutex<Vec<String>>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let log: std::sync::Arc<parking_lot::Mutex<Vec<String>>> =
            std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
        let log_for_task = log.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                let log = log_for_task.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let _ = sock.read(&mut buf).await;
                    let req = String::from_utf8_lossy(&buf).to_string();
                    let first_line = req.lines().next().unwrap_or("").to_string();
                    log.lock().push(first_line.clone());
                    let body = serde_json::json!({"echo": first_line}).to_string();
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                    let _ = sock.shutdown().await;
                });
            }
        });
        (addr, log)
    }

    /// 带补偿的 SW 文档：`reserve` 失败 ⇒ 必须转去 `undo` 并执行 `refund`。
    fn sample_compensation_sw(charge_url: &str, refund_url: &str) -> String {
        format!(
            r#"{{
          "id": "comp-wf",
          "version": "1.0",
          "start": "reserve",
          "functions": [
            {{ "name": "chargeCard", "operation": "{charge_url}" }},
            {{ "name": "refundCard", "operation": "{refund_url}" }}
          ],
          "states": [
            {{ "name": "reserve", "type": "operation",
              "actions": [ {{ "name": "charge",
                              "functionRef": {{ "refName": "chargeCard" }} }} ],
              "compensatedBy": "undo",
              "transition": "done" }},
            {{ "name": "undo", "type": "compensate",
              "actions": [ {{ "name": "refund",
                              "functionRef": {{ "refName": "refundCard" }} }} ],
              "end": true }},
            {{ "name": "done", "type": "inject",
              "data": {{ "ok": true }}, "end": true }}
          ]
        }}"#
        )
    }

    #[tokio::test]
    async fn test_sw_compensated_by_runs_compensation_end_to_end() {
        let (ok_addr, log) = spawn_recording_server().await;
        // 关闭端口：连接必然被拒 ⇒ 被补偿状态的动作**真的失败**
        let dead_url = "http://127.0.0.1:1/charge".to_string();
        let refund_url = format!("http://{ok_addr}/refund");
        let doc = sample_compensation_sw(&dead_url, &refund_url);

        let svc = WorkflowEngineService::new(); // 真 HttpTaskDispatcher（不是 Noop）
        let def_id = svc.deploy_definition("test", &doc).await.unwrap();
        let inst = svc
            .start_instance(&def_id, serde_json::json!({"orderId": "o-1"}))
            .await
            .unwrap();

        // 等补偿链路跑完（dispatch 失败 + 转场 + 补偿 dispatch）
        for _ in 0..40 {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            let cur = svc.get_instance(&inst.id).await.unwrap().unwrap();
            if cur.status == InstanceStatus::Completed || cur.status == InstanceStatus::Failed {
                break;
            }
        }

        let loaded = svc.get_instance(&inst.id).await.unwrap().unwrap();
        let ran: Vec<&str> = loaded
            .task_stack
            .iter()
            .map(|f| f.task_name.as_str())
            .collect();

        // ② 补偿状态跑到了
        assert!(
            ran.iter().any(|n| n.starts_with("undo")),
            "补偿状态未执行，task_stack={ran:?}，status={:?}，fault={:?}",
            loaded.status,
            loaded.fault
        );
        // ③ 正常路径不得执行
        assert!(
            !ran.iter().any(|n| n.starts_with("done")),
            "被补偿状态失败后不得继续走正常路径，task_stack={ran:?}"
        );
        // ② 补偿**动作真的发出去了**（服务端有记录），而不仅是"状态名在栈上"
        let requests = log.lock().clone();
        assert!(
            requests.iter().any(|r| r.contains("/refund")),
            "补偿动作未真正 dispatch（服务端未收到 /refund 请求）：{requests:?}"
        );
    }

    /// 三步流程 + 逆序补偿链：step1 ✓ → step2 ✓ → step3 ✗ ⇒ undo2 → undo1。
    ///
    /// `step3.compensatedBy = undo2` 把「第 3 步失败」路由进补偿链；`undo2` 自身
    /// 完成后 `transition` 到 `undo1`（补偿按**逆序**逐级回溯，由 DSL 连线显式
    /// 表达——引擎不内置自动逆序栈，见 `docs/production/workflow-semantics.md`）。
    fn sample_reverse_compensation_sw(ok_addr: &str, dead_url: &str) -> String {
        format!(
            r#"{{
          "id": "rev-comp-wf",
          "version": "1.0",
          "start": "step1",
          "functions": [
            {{ "name": "charge1", "operation": "http://{ok_addr}/charge1" }},
            {{ "name": "charge2", "operation": "http://{ok_addr}/charge2" }},
            {{ "name": "charge3", "operation": "{dead_url}" }},
            {{ "name": "refund2", "operation": "http://{ok_addr}/refund2" }},
            {{ "name": "refund1", "operation": "http://{ok_addr}/refund1" }}
          ],
          "states": [
            {{ "name": "step1", "type": "operation",
              "actions": [ {{ "name": "c1",
                              "functionRef": {{ "refName": "charge1" }} }} ],
              "compensatedBy": "undo1",
              "transition": "step2" }},
            {{ "name": "step2", "type": "operation",
              "actions": [ {{ "name": "c2",
                              "functionRef": {{ "refName": "charge2" }} }} ],
              "compensatedBy": "undo2",
              "transition": "step3" }},
            {{ "name": "step3", "type": "operation",
              "actions": [ {{ "name": "c3",
                              "functionRef": {{ "refName": "charge3" }} }} ],
              "compensatedBy": "undo2",
              "transition": "done" }},
            {{ "name": "undo2", "type": "compensate",
              "actions": [ {{ "name": "r2",
                              "functionRef": {{ "refName": "refund2" }} }} ],
              "transition": "undo1" }},
            {{ "name": "undo1", "type": "compensate",
              "actions": [ {{ "name": "r1",
                              "functionRef": {{ "refName": "refund1" }} }} ],
              "end": true }},
            {{ "name": "done", "type": "inject",
              "data": {{ "ok": true }}, "end": true }}
          ]
        }}"#
        )
    }

    /// G-WF-1 验收锚点：三步流程中途失败 → **逆序**补偿且可重放。
    ///
    /// 同一 DSL 起两个实例，逐实例断言动作序列 `c1 → c2 → r2 → r1`：
    /// ① 前两步动作真的成功（服务端有记录）；
    /// ② 第 3 步失败后，补偿链**先 r2 再 r1**（逆序，不是按声明顺序）；
    /// ③ 整个序列可重放（第二个实例重复同一序列）。
    #[tokio::test]
    async fn test_sw_three_step_failure_compensates_in_reverse_order_and_replays() {
        let (ok_addr, log) = spawn_recording_server().await;
        let dead_url = "http://127.0.0.1:1/charge3".to_string();
        let doc = sample_reverse_compensation_sw(&ok_addr, &dead_url);

        // 真 HttpTaskDispatcher（不是 Noop）：动作要真的发到记录服务器
        let svc = WorkflowEngineService::new();
        let def_id = svc.deploy_definition("test", &doc).await.unwrap();

        for run in 0..2 {
            let inst = svc
                .start_instance(&def_id, serde_json::json!({ "run": run }))
                .await
                .unwrap();

            let mut terminal = false;
            for _ in 0..60 {
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                let cur = svc.get_instance(&inst.id).await.unwrap().unwrap();
                if cur.status == InstanceStatus::Completed || cur.status == InstanceStatus::Failed {
                    terminal = true;
                    break;
                }
            }
            assert!(terminal, "run {run}: 补偿链未在限时内终止");
        }

        let requests = log.lock().clone();

        // 逐请求归类 → 断言完整动作序列（两个实例各一遍，逐实例顺序固定）
        let seq: Vec<&str> = requests
            .iter()
            .map(|r| {
                if r.contains("/charge1") {
                    "c1"
                } else if r.contains("/charge2") {
                    "c2"
                } else if r.contains("/refund2") {
                    "r2"
                } else if r.contains("/refund1") {
                    "r1"
                } else {
                    "other"
                }
            })
            .collect();
        assert_eq!(
            seq,
            vec!["c1", "c2", "r2", "r1", "c1", "c2", "r2", "r1"],
            "应为「前两步成功 → 逆序补偿（先 refund2 再 refund1）」且可重放；\
             实际请求：{requests:?}"
        );
    }

    // ═══ P2：HTTP 完整契约 + secrets/constants 注入 + 认证 ═══

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// 启动本地回显 HTTP 服务器，返回地址
    async fn spawn_echo_server() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let mut buf = [0u8; 8192];
                    let _ = sock.read(&mut buf).await;
                    let req_str = String::from_utf8_lossy(&buf);
                    // 回显请求首行 + Authorization 头（后者用于断言凭据注入）
                    let first_line = req_str.lines().next().unwrap_or("");
                    let authorization = req_str
                        .lines()
                        .find_map(|l| {
                            let (name, value) = l.split_once(':')?;
                            name.eq_ignore_ascii_case("authorization")
                                .then(|| value.trim().to_string())
                        })
                        .unwrap_or_default();
                    let body = serde_json::json!({
                        "echo": first_line,
                        "authorization": authorization,
                    })
                    .to_string();
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                    let _ = sock.shutdown().await;
                });
            }
        });
        addr
    }

    #[tokio::test]
    async fn test_http_dispatcher_full_contract() {
        use crate::services::workflow::phase4::HttpTaskDispatcher;
        use std::collections::HashMap;
        let addr = spawn_echo_server().await;

        let dispatcher = HttpTaskDispatcher::new().with_secrets(HashMap::from([(
            "apiKey".to_string(),
            "SECRET-123".to_string(),
        )]));

        let with = serde_json::json!({
            "method": "POST",
            "endpoint": {"uri": format!("http://{addr}"), "method": "POST"},
            "query": {"a": "1", "b": "2"},
            "body": {"token": "${ $secrets.apiKey }"},
            "headers": {"X-Test": "yes"},
            "auth": {"scheme": "bearer", "token": "${ $secrets.apiKey }"},
        });

        let result = dispatcher
            .dispatch("http", Some(&with), &serde_json::json!({"x": 1}), None)
            .await;
        match result {
            DispatchResult::Success { data } => {
                // 服务器回显请求首行（POST /?a=1&b=2 HTTP/1.1）
                let echo = data.get("echo").and_then(|v| v.as_str()).unwrap_or("");
                assert!(
                    echo.starts_with("POST /?a=1&b=2"),
                    "unexpected echo: {echo}"
                );
            }
            DispatchResult::Failure { error, .. } => panic!("HTTP dispatch failed: {error}"),
        }
    }

    /// 注入的 secret 真的进了 Authorization 头（配置 → dispatcher 映射验证）。
    #[tokio::test]
    async fn test_http_dispatcher_secret_injected_into_authorization_header() {
        use crate::service::WorkflowDispatchConfig;
        use crate::services::workflow::phase4::HttpTaskDispatcher;
        let addr = spawn_echo_server().await;

        let config = WorkflowDispatchConfig {
            secrets: std::collections::HashMap::from([(
                "approvalToken".to_string(),
                "TOKEN-XYZ".to_string(),
            )]),
            ..WorkflowDispatchConfig::default()
        };
        let dispatcher = HttpTaskDispatcher::from_config(&config);

        let with = serde_json::json!({
            "method": "GET",
            "endpoint": format!("http://{addr}"),
            "auth": {"scheme": "bearer", "token": "${ $secrets.approvalToken }"},
        });
        match dispatcher
            .dispatch("http", Some(&with), &serde_json::json!({}), None)
            .await
        {
            DispatchResult::Success { data } => {
                let auth = data
                    .get("authorization")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                assert_eq!(auth, "Bearer TOKEN-XYZ", "Authorization 头未带上注入凭据");
            }
            DispatchResult::Failure { error, .. } => panic!("HTTP dispatch failed: {error}"),
        }
    }

    /// constants（字符串 + 数字 JSON 值）替换生效。
    #[tokio::test]
    async fn test_http_dispatcher_constants_replacement() {
        use crate::services::workflow::phase4::HttpTaskDispatcher;
        let addr = spawn_echo_server().await;
        let dispatcher = HttpTaskDispatcher::new().with_constants(serde_json::Map::from_iter([
            ("region".to_string(), serde_json::json!("cn-north-1")),
            ("retries".to_string(), serde_json::json!(3)),
        ]));
        let with = serde_json::json!({
            "method": "GET",
            "endpoint": format!("http://{addr}"),
            "query": {"region": "${ $constants.region }", "retries": "${ $constants.retries }"},
        });
        match dispatcher
            .dispatch("http", Some(&with), &serde_json::json!({}), None)
            .await
        {
            DispatchResult::Success { data } => {
                let echo = data.get("echo").and_then(|v| v.as_str()).unwrap_or("");
                // 字符串常量按 JSON 序列化（带引号）替换，数字常量原样；两者均与
                // `with_constants` 既有行为一致（本次不改替代表达式）。
                assert!(
                    echo.contains("region=%22cn-north-1%22"),
                    "unexpected echo: {echo}"
                );
                assert!(echo.contains("retries=3"), "unexpected echo: {echo}");
            }
            DispatchResult::Failure { error, .. } => panic!("HTTP dispatch failed: {error}"),
        }
    }

    /// 未命中 key ⇒ fail-closed：retryable=false，错误含全部缺失 key 名，
    /// 不含占位符字面量（绝不把 `${ $secrets.x }` 当凭据发出）。
    #[tokio::test]
    async fn test_http_dispatcher_missing_placeholders_fail_closed() {
        use crate::services::workflow::phase4::HttpTaskDispatcher;
        let dispatcher = HttpTaskDispatcher::new();
        let with = serde_json::json!({
            "method": "GET",
            "endpoint": "http://127.0.0.1:1/never-sent",
            "body": {"token": "${ $secrets.approvalToken }"},
            "headers": {"X-Region": "${ $constants.region }"},
        });
        match dispatcher
            .dispatch("http", Some(&with), &serde_json::json!({}), None)
            .await
        {
            DispatchResult::Failure { error, retryable } => {
                assert!(!retryable, "配置缺失不得可重试：{error}");
                assert!(error.contains("secrets.approvalToken"), "error: {error}");
                assert!(error.contains("constants.region"), "error: {error}");
                assert!(!error.contains("${"), "错误不得回显占位符字面量：{error}");
            }
            DispatchResult::Success { .. } => panic!("缺失占位符不得派发成功"),
        }
    }

    /// 默认（未注入）配置下，不含占位符的请求行为不变。
    #[tokio::test]
    async fn test_http_dispatcher_default_config_unchanged_without_placeholders() {
        use crate::services::workflow::phase4::HttpTaskDispatcher;
        let addr = spawn_echo_server().await;
        let dispatcher = HttpTaskDispatcher::from_config(&Default::default());
        let with = serde_json::json!({
            "method": "GET",
            "endpoint": format!("http://{addr}"),
            "headers": {"X-Test": "yes"},
        });
        match dispatcher
            .dispatch("http", Some(&with), &serde_json::json!({}), None)
            .await
        {
            DispatchResult::Success { data } => {
                let echo = data.get("echo").and_then(|v| v.as_str()).unwrap_or("");
                assert!(echo.starts_with("GET /"), "unexpected echo: {echo}");
            }
            DispatchResult::Failure { error, .. } => panic!("HTTP dispatch failed: {error}"),
        }
    }

    // ═══ P2：`with` 运行时表达式求值（#96）═══

    /// `$context` / `$input` 整串表达式在 query / header / body / auth 中的求值。
    #[tokio::test]
    async fn test_http_dispatcher_context_expression_evaluated() {
        use crate::services::workflow::phase4::HttpTaskDispatcher;
        let (addr, log) = spawn_request_recording_server().await;
        let dispatcher = HttpTaskDispatcher::new();
        let with = serde_json::json!({
            "method": "POST",
            "endpoint": format!("http://{addr}/commands/activate"),
            "query": {"emp": "${ $context.target.id }"},
            "headers": {"X-Employee": "${ $context.target.id }"},
            "body": {"payload": {"employee_id": "${ $context.target.id }"}, "n": "${ $input.count }"},
            "auth": {"scheme": "bearer", "token": "${ $context.token }"},
        });
        let input = serde_json::json!({"target": {"id": "E-42"}, "count": 2, "token": "CTX-TOKEN"});
        match dispatcher.dispatch("http", Some(&with), &input, None).await {
            DispatchResult::Success { .. } => {}
            DispatchResult::Failure { error, .. } => panic!("HTTP dispatch failed: {error}"),
        }
        let req = log.lock().join("\n");
        let req_lower = req.to_lowercase();
        assert!(
            req.contains("POST /commands/activate?emp=E-42 "),
            "query 未求值：{req}"
        );
        assert!(
            req_lower.contains("x-employee: e-42"),
            "header 未求值：{req}"
        );
        assert!(
            req.contains(r#""employee_id":"E-42""#),
            "body 未求值：{req}"
        );
        assert!(req.contains(r#""n":2"#), "$input 未求值：{req}");
        assert!(
            req_lower.contains("authorization: bearer ctx-token"),
            "auth 未求值：{req}"
        );
        assert!(!req.contains("${"), "占位符字面量不得发出：{req}");
    }

    /// 未知绑定 / 内嵌表达式 / 非表达式文本 ⇒ fail-closed，且请求不发出。
    #[tokio::test]
    async fn test_http_dispatcher_expression_failure_fails_closed() {
        use crate::services::workflow::phase4::HttpTaskDispatcher;
        let (addr, log) = spawn_request_recording_server().await;
        let dispatcher = HttpTaskDispatcher::new();
        let input = serde_json::json!({"target": {"id": "E-42"}, "token": "CTX-TOKEN"});
        let cases = [
            serde_json::json!({"method":"GET","endpoint":format!("http://{addr}/x"),
                               "body": {"v": "${ $workflow.id }"}}),
            serde_json::json!({"method":"GET","endpoint":format!("http://{addr}/x"),
                               "query": {"v": "id=${ $context.target.id }"}}),
            serde_json::json!({"method":"GET","endpoint":format!("http://{addr}/x"),
                               "body": {"v": "${ $context.target.id } ${ $context.token }"}}),
            serde_json::json!({"method":"GET","endpoint":format!("http://{addr}/x"),
                               "body": {"v": "${ strconv }"}}),
        ];
        for with in &cases {
            match dispatcher.dispatch("http", Some(with), &input, None).await {
                DispatchResult::Failure { error, retryable } => {
                    assert!(!retryable, "fail-closed 不得可重试：{error}");
                    assert!(
                        error.contains("workflow dispatch blocked"),
                        "error: {error}"
                    );
                    assert!(!error.contains("${"), "错误不得回显占位符：{error}");
                }
                DispatchResult::Success { .. } => panic!("fail-closed 不得派发成功：{with}"),
            }
        }
        assert!(
            log.lock().is_empty(),
            "fail-closed 不得发出任何请求：{:?}",
            log.lock()
        );
    }

    // ═══ P2：`new_with_dispatch` 接线端到端（定义里的占位符 → 真实请求）═══

    /// 记录每个连接完整请求原文的服务器（body / header / query 断言用）。
    async fn spawn_request_recording_server(
    ) -> (String, std::sync::Arc<parking_lot::Mutex<Vec<String>>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
            haystack.windows(needle.len()).position(|w| w == needle)
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let log: std::sync::Arc<parking_lot::Mutex<Vec<String>>> =
            std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
        let log_for_task = log.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                let log = log_for_task.clone();
                tokio::spawn(async move {
                    // 读到 header 结束，再按 Content-Length 读完 body：小请求通常单段到达，
                    // 但分包时只读第一段会让 body 断言假阴性。
                    let mut raw = Vec::new();
                    let mut buf = [0u8; 4096];
                    while let Ok(n) = sock.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                        raw.extend_from_slice(&buf[..n]);
                        if let Some(head_end) = find_subslice(&raw, b"\r\n\r\n") {
                            let head = String::from_utf8_lossy(&raw[..head_end]);
                            let content_len = head
                                .lines()
                                .find_map(|l| {
                                    let (name, value) = l.split_once(':')?;
                                    name.eq_ignore_ascii_case("content-length")
                                        .then(|| value.trim().parse::<usize>().ok())
                                        .flatten()
                                })
                                .unwrap_or(0);
                            if raw.len() >= head_end + 4 + content_len {
                                break;
                            }
                        }
                    }
                    log.lock().push(String::from_utf8_lossy(&raw).to_string());
                    let body = serde_json::json!({"ok": true}).to_string();
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                    let _ = sock.shutdown().await;
                });
            }
        });
        (addr, log)
    }

    // ═══ P2：OAuth2 client_credentials 派发认证（#97）═══

    /// mock OAuth2 令牌端点：默认每次签发递增令牌（`MOCK-TOKEN-<n>`）并记录表单原文；
    /// 状态码 / 响应体可被测试覆盖（覆盖优先于默认 JSON）。
    struct OAuth2TokenServer {
        addr: String,
        form_bodies: std::sync::Arc<parking_lot::Mutex<Vec<String>>>,
        status: std::sync::Arc<parking_lot::Mutex<u16>>,
        body_override: std::sync::Arc<parking_lot::Mutex<Option<String>>>,
    }

    impl OAuth2TokenServer {
        async fn spawn(expires_in: Option<u64>) -> Self {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};

            fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
                haystack.windows(needle.len()).position(|w| w == needle)
            }

            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap().to_string();
            let form_bodies: std::sync::Arc<parking_lot::Mutex<Vec<String>>> =
                std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
            let status: std::sync::Arc<parking_lot::Mutex<u16>> =
                std::sync::Arc::new(parking_lot::Mutex::new(200));
            let body_override: std::sync::Arc<parking_lot::Mutex<Option<String>>> =
                std::sync::Arc::new(parking_lot::Mutex::new(None));
            let forms_for_task = form_bodies.clone();
            let status_for_task = status.clone();
            let override_for_task = body_override.clone();
            tokio::spawn(async move {
                loop {
                    let Ok((mut sock, _)) = listener.accept().await else {
                        break;
                    };
                    let forms = forms_for_task.clone();
                    let st = status_for_task.clone();
                    let bo = override_for_task.clone();
                    tokio::spawn(async move {
                        let mut raw = Vec::new();
                        let mut buf = [0u8; 4096];
                        while let Ok(n) = sock.read(&mut buf).await {
                            if n == 0 {
                                break;
                            }
                            raw.extend_from_slice(&buf[..n]);
                            if let Some(head_end) = find_subslice(&raw, b"\r\n\r\n") {
                                let head = String::from_utf8_lossy(&raw[..head_end]);
                                let len = head
                                    .lines()
                                    .find_map(|l| {
                                        let (name, value) = l.split_once(':')?;
                                        name.eq_ignore_ascii_case("content-length")
                                            .then(|| value.trim().parse::<usize>().ok())
                                            .flatten()
                                    })
                                    .unwrap_or(0);
                                if raw.len() >= head_end + 4 + len {
                                    break;
                                }
                            }
                        }
                        let head_end = find_subslice(&raw, b"\r\n\r\n").unwrap_or(raw.len());
                        let body_start = (head_end + 4).min(raw.len());
                        forms
                            .lock()
                            .push(String::from_utf8_lossy(&raw[body_start..]).to_string());
                        let code = *st.lock();
                        let resp_body = match (code, bo.lock().clone()) {
                            (200, None) => {
                                let n = forms.lock().len();
                                let mut v = serde_json::json!({
                                    "access_token": format!("MOCK-TOKEN-{n}"),
                                    "token_type": "Bearer",
                                });
                                if let Some(ttl) = expires_in {
                                    v["expires_in"] = serde_json::json!(ttl);
                                }
                                v.to_string()
                            }
                            (_, Some(b)) => b,
                            (other, None) => {
                                serde_json::json!({"error": format!("status {other}")}).to_string()
                            }
                        };
                        let resp = format!(
                            "HTTP/1.1 {code} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            resp_body.len(),
                            resp_body
                        );
                        let _ = sock.write_all(resp.as_bytes()).await;
                        let _ = sock.shutdown().await;
                    });
                }
            });
            Self {
                addr,
                form_bodies,
                status,
                body_override,
            }
        }

        fn token_url(&self) -> String {
            format!("http://{}/oauth2/token", self.addr)
        }
    }

    /// oauth2 client_credentials：取令牌并作为 Bearer 发出；表单只带 client_credentials 字段。
    #[tokio::test]
    async fn test_http_dispatcher_oauth2_gets_token_and_sends_bearer() {
        use crate::services::workflow::phase4::HttpTaskDispatcher;
        let token_server = OAuth2TokenServer::spawn(Some(300)).await;
        let (res_addr, res_log) = spawn_auth_recording_server().await;
        let dispatcher = HttpTaskDispatcher::new().with_secrets(HashMap::from([
            ("hrClientId".to_string(), "HR-CLIENT".to_string()),
            ("hrClientSecret".to_string(), "HR-SECRET".to_string()),
        ]));
        let with = serde_json::json!({
            "method": "POST",
            "endpoint": format!("http://{res_addr}/commands/activate"),
            "auth": {
                "scheme": "oauth2",
                "tokenUrl": token_server.token_url(),
                "clientId": "${ $secrets.hrClientId }",
                "clientSecret": "${ $secrets.hrClientSecret }",
                "scope": "hr.employee.activate",
            },
        });
        match dispatcher
            .dispatch("http", Some(&with), &serde_json::json!({}), None)
            .await
        {
            DispatchResult::Success { .. } => {}
            DispatchResult::Failure { error, .. } => panic!("HTTP dispatch failed: {error}"),
        }
        let forms = token_server.form_bodies.lock().clone();
        assert_eq!(forms.len(), 1, "应恰好取一次令牌：{forms:?}");
        let form = &forms[0];
        assert!(
            form.contains("grant_type=client_credentials"),
            "form: {form}"
        );
        assert!(form.contains("client_id=HR-CLIENT"), "form: {form}");
        assert!(form.contains("client_secret=HR-SECRET"), "form: {form}");
        assert!(form.contains("scope=hr.employee.activate"), "form: {form}");
        assert_eq!(
            res_log.lock().as_slice(),
            ["Bearer MOCK-TOKEN-1"],
            "资源服务器未收到按 oauth2 取得的 Bearer 令牌"
        );
    }

    /// 未过期令牌复用：两次派发只取一次令牌。
    #[tokio::test]
    async fn test_http_dispatcher_oauth2_token_cached_until_expiry() {
        use crate::services::workflow::phase4::HttpTaskDispatcher;
        let token_server = OAuth2TokenServer::spawn(Some(300)).await;
        let (res_addr, res_log) = spawn_auth_recording_server().await;
        let dispatcher = HttpTaskDispatcher::new().with_secrets(HashMap::from([
            ("hrClientId".to_string(), "HR-CLIENT".to_string()),
            ("hrClientSecret".to_string(), "HR-SECRET".to_string()),
        ]));
        let with = serde_json::json!({
            "method": "GET",
            "endpoint": format!("http://{res_addr}/ping"),
            "auth": {
                "scheme": "oauth2",
                "tokenUrl": token_server.token_url(),
                "clientId": "${ $secrets.hrClientId }",
                "clientSecret": "${ $secrets.hrClientSecret }",
            },
        });
        for _ in 0..2 {
            match dispatcher
                .dispatch("http", Some(&with), &serde_json::json!({}), None)
                .await
            {
                DispatchResult::Success { .. } => {}
                DispatchResult::Failure { error, .. } => panic!("HTTP dispatch failed: {error}"),
            }
        }
        assert_eq!(
            token_server.form_bodies.lock().len(),
            1,
            "未过期令牌应复用（不反复取令牌）"
        );
        assert_eq!(
            res_log.lock().as_slice(),
            ["Bearer MOCK-TOKEN-1", "Bearer MOCK-TOKEN-1"]
        );
    }

    /// 过期（expires_in=0）令牌刷新：两次派发各取一次令牌。
    #[tokio::test]
    async fn test_http_dispatcher_oauth2_token_refreshed_when_expired() {
        use crate::services::workflow::phase4::HttpTaskDispatcher;
        let token_server = OAuth2TokenServer::spawn(Some(0)).await;
        let (res_addr, res_log) = spawn_auth_recording_server().await;
        let dispatcher = HttpTaskDispatcher::new().with_secrets(HashMap::from([
            ("hrClientId".to_string(), "HR-CLIENT".to_string()),
            ("hrClientSecret".to_string(), "HR-SECRET".to_string()),
        ]));
        let with = serde_json::json!({
            "method": "GET",
            "endpoint": format!("http://{res_addr}/ping"),
            "auth": {
                "scheme": "oauth2",
                "tokenUrl": token_server.token_url(),
                "clientId": "${ $secrets.hrClientId }",
                "clientSecret": "${ $secrets.hrClientSecret }",
            },
        });
        for _ in 0..2 {
            match dispatcher
                .dispatch("http", Some(&with), &serde_json::json!({}), None)
                .await
            {
                DispatchResult::Success { .. } => {}
                DispatchResult::Failure { error, .. } => panic!("HTTP dispatch failed: {error}"),
            }
        }
        assert_eq!(
            token_server.form_bodies.lock().len(),
            2,
            "过期令牌应刷新（每轮重新取令牌）"
        );
        assert_eq!(
            res_log.lock().as_slice(),
            ["Bearer MOCK-TOKEN-1", "Bearer MOCK-TOKEN-2"],
            "第二轮应使用刷新后的令牌"
        );
    }

    /// 令牌端点失败口径：5xx retryable、4xx 非 retryable、缺 access_token 非 retryable；
    /// 错误信息对 client_secret 脱敏，失败不调用资源服务器。
    #[tokio::test]
    async fn test_http_dispatcher_oauth2_token_endpoint_failure_semantics() {
        use crate::services::workflow::phase4::HttpTaskDispatcher;
        let token_server = OAuth2TokenServer::spawn(Some(300)).await;
        let (res_addr, res_log) = spawn_auth_recording_server().await;
        let dispatcher = HttpTaskDispatcher::new().with_secrets(HashMap::from([
            ("hrClientId".to_string(), "HR-CLIENT".to_string()),
            ("hrClientSecret".to_string(), "HR-SECRET".to_string()),
        ]));
        let with = serde_json::json!({
            "method": "GET",
            "endpoint": format!("http://{res_addr}/ping"),
            "auth": {
                "scheme": "oauth2",
                "tokenUrl": token_server.token_url(),
                "clientId": "${ $secrets.hrClientId }",
                "clientSecret": "${ $secrets.hrClientSecret }",
            },
        });

        // 5xx → 可重试
        *token_server.status.lock() = 503;
        match dispatcher
            .dispatch("http", Some(&with), &serde_json::json!({}), None)
            .await
        {
            DispatchResult::Failure { error, retryable } => {
                assert!(retryable, "令牌端点 5xx 应可重试：{error}");
                assert!(
                    !error.contains("HR-SECRET"),
                    "错误不得含 client_secret：{error}"
                );
            }
            DispatchResult::Success { .. } => panic!("令牌端点 5xx 不得派发成功"),
        }

        // 4xx → 不可重试；响应体回显 client_secret 时必须脱敏
        *token_server.status.lock() = 400;
        *token_server.body_override.lock() = Some(
            serde_json::json!({
                "error": "invalid_client",
                "error_description": "client_secret HR-SECRET rejected",
            })
            .to_string(),
        );
        match dispatcher
            .dispatch("http", Some(&with), &serde_json::json!({}), None)
            .await
        {
            DispatchResult::Failure { error, retryable } => {
                assert!(!retryable, "令牌端点 4xx 不得可重试：{error}");
                assert!(
                    error.contains("invalid_client"),
                    "应保留 OAuth2 错误码：{error}"
                );
                assert!(
                    !error.contains("HR-SECRET"),
                    "错误必须脱敏 client_secret：{error}"
                );
            }
            DispatchResult::Success { .. } => panic!("令牌端点 4xx 不得派发成功"),
        }

        // 200 但响应缺 access_token → 不可重试
        *token_server.status.lock() = 200;
        *token_server.body_override.lock() =
            Some(serde_json::json!({"token_type": "Bearer"}).to_string());
        match dispatcher
            .dispatch("http", Some(&with), &serde_json::json!({}), None)
            .await
        {
            DispatchResult::Failure { error, retryable } => {
                assert!(!retryable, "缺 access_token 不得可重试：{error}");
            }
            DispatchResult::Success { .. } => panic!("缺 access_token 不得派发成功"),
        }

        assert!(
            res_log.lock().is_empty(),
            "令牌获取失败不得调用资源服务器：{:?}",
            res_log.lock()
        );
    }

    /// oauth2 配置缺失（tokenUrl / clientSecret）⇒ fail-closed：不可重试、点名字段、
    /// 不取令牌、不调用资源服务器。
    #[tokio::test]
    async fn test_http_dispatcher_oauth2_missing_config_fails_closed() {
        use crate::services::workflow::phase4::HttpTaskDispatcher;
        let (res_addr, res_log) = spawn_auth_recording_server().await;
        let dispatcher = HttpTaskDispatcher::new().with_secrets(HashMap::from([
            ("hrClientId".to_string(), "HR-CLIENT".to_string()),
            ("hrClientSecret".to_string(), "HR-SECRET".to_string()),
        ]));

        // 缺 tokenUrl
        let with = serde_json::json!({
            "method": "GET",
            "endpoint": format!("http://{res_addr}/ping"),
            "auth": {"scheme": "oauth2", "clientId": "${ $secrets.hrClientId }"},
        });
        match dispatcher
            .dispatch("http", Some(&with), &serde_json::json!({}), None)
            .await
        {
            DispatchResult::Failure { error, retryable } => {
                assert!(!retryable, "配置缺失不得可重试：{error}");
                assert!(error.contains("tokenUrl"), "应点名 tokenUrl：{error}");
                assert!(!error.contains("HR-SECRET"), "错误不得含凭据值：{error}");
            }
            DispatchResult::Success { .. } => panic!("配置缺失不得派发成功"),
        }

        // 缺 clientSecret：tokenUrl 指向必然拒连的端口——如果实现去取令牌会是
        // retryable 的连接失败，本用例断言必须是配置缺失的非重试失败。
        let with = serde_json::json!({
            "method": "GET",
            "endpoint": format!("http://{res_addr}/ping"),
            "auth": {
                "scheme": "oauth2",
                "tokenUrl": "http://127.0.0.1:1/oauth2/token",
                "clientId": "${ $secrets.hrClientId }",
            },
        });
        match dispatcher
            .dispatch("http", Some(&with), &serde_json::json!({}), None)
            .await
        {
            DispatchResult::Failure { error, retryable } => {
                assert!(!retryable, "配置缺失不得可重试：{error}");
                assert!(
                    error.contains("clientSecret"),
                    "应点名 clientSecret：{error}"
                );
            }
            DispatchResult::Success { .. } => panic!("配置缺失不得派发成功"),
        }

        assert!(
            res_log.lock().is_empty(),
            "配置缺失不得调用资源服务器：{:?}",
            res_log.lock()
        );
    }

    /// 记录 Authorization 头的服务器（接线验证用）。
    async fn spawn_auth_recording_server(
    ) -> (String, std::sync::Arc<parking_lot::Mutex<Vec<String>>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let log: std::sync::Arc<parking_lot::Mutex<Vec<String>>> =
            std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
        let log_for_task = log.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                let log = log_for_task.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 8192];
                    let _ = sock.read(&mut buf).await;
                    let req = String::from_utf8_lossy(&buf).to_string();
                    let auth = req
                        .lines()
                        .find_map(|l| {
                            let (name, value) = l.split_once(':')?;
                            name.eq_ignore_ascii_case("authorization")
                                .then(|| value.trim().to_string())
                        })
                        .unwrap_or_default();
                    log.lock().push(auth);
                    let body = serde_json::json!({"ok": true}).to_string();
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                    let _ = sock.shutdown().await;
                });
            }
        });
        (addr, log)
    }

    /// 单动作 SW：GET `http://<addr>/ping`，bearer token 用 secret 占位符。
    fn sample_secret_dispatch_sw(addr: &str) -> String {
        format!(
            r#"{{
          "id": "dispatch-secret-wf",
          "version": "1.0",
          "start": "call",
          "functions": [ {{ "name": "ping", "operation": "http://{addr}/ping" }} ],
          "states": [
            {{ "name": "call", "type": "operation",
              "actions": [ {{ "name": "pingAction",
                              "functionRef": {{ "refName": "ping",
                                "arguments": {{ "method": "GET",
                                  "auth": {{ "scheme": "bearer", "token": "${{ $secrets.approvalToken }}" }} }} }} }} ],
              "end": true }}
          ]
        }}"#
        )
    }

    /// 生产构造 `new_with_dispatch` 把 `[services.workflow_dispatch]` 的 secrets
    /// 真的接到了 HTTP 派发上（定义里的占位符 → 真实 Authorization 头）。
    #[tokio::test]
    async fn test_engine_new_with_dispatch_injects_secret_end_to_end() {
        let (addr, log) = spawn_auth_recording_server().await;
        let cfg = crate::service::WorkflowDispatchConfig {
            secrets: std::collections::HashMap::from([(
                "approvalToken".to_string(),
                "E2E-TOKEN".to_string(),
            )]),
            ..Default::default()
        };
        let svc = WorkflowEngineService::new_with_dispatch(&cfg);
        let def_id = svc
            .deploy_definition("test", &sample_secret_dispatch_sw(&addr))
            .await
            .unwrap();
        let inst = svc
            .start_instance(&def_id, serde_json::json!({}))
            .await
            .unwrap();

        let mut terminal = false;
        for _ in 0..40 {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            let cur = svc.get_instance(&inst.id).await.unwrap().unwrap();
            if cur.status == InstanceStatus::Completed || cur.status == InstanceStatus::Failed {
                assert_eq!(
                    cur.status,
                    InstanceStatus::Completed,
                    "工作流未成功：{:?}",
                    cur.fault
                );
                terminal = true;
                break;
            }
        }
        assert!(terminal, "工作流未在限时内终止");
        let auths = log.lock().clone();
        assert!(
            auths.iter().any(|a| a == "Bearer E2E-TOKEN"),
            "注入的 secret 未到达 Authorization 头：{auths:?}"
        );
    }

    /// 未配置 secret ⇒ 占位符 fail-closed：工作流失败，且请求根本不发出。
    #[tokio::test]
    async fn test_engine_new_with_dispatch_missing_secret_fails_closed_end_to_end() {
        let (addr, log) = spawn_auth_recording_server().await;
        let svc = WorkflowEngineService::new_with_dispatch(&Default::default());
        let def_id = svc
            .deploy_definition("test", &sample_secret_dispatch_sw(&addr))
            .await
            .unwrap();
        let inst = svc
            .start_instance(&def_id, serde_json::json!({}))
            .await
            .unwrap();

        let mut status = None;
        for _ in 0..40 {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            let cur = svc.get_instance(&inst.id).await.unwrap().unwrap();
            if cur.status == InstanceStatus::Completed || cur.status == InstanceStatus::Failed {
                status = Some(cur.status);
                break;
            }
        }
        assert_eq!(
            status,
            Some(InstanceStatus::Failed),
            "缺 secret 应 fail-closed 令工作流失败"
        );
        assert!(
            log.lock().is_empty(),
            "fail-closed 不应发出任何请求：{:?}",
            log.lock()
        );
    }

    /// 单动作 SW：POST `http://<addr>/commands/activate`，body 用 `$context` 表达式。
    fn sample_context_dispatch_sw(addr: &str) -> String {
        format!(
            r#"{{
          "id": "dispatch-context-wf",
          "version": "1.0",
          "start": "call",
          "functions": [ {{ "name": "activate", "operation": "http://{addr}/commands/activate" }} ],
          "states": [
            {{ "name": "call", "type": "operation",
              "actions": [ {{ "name": "activateAction",
                              "functionRef": {{ "refName": "activate",
                                "arguments": {{ "method": "POST",
                                  "body": {{ "employee_id": "${{ $context.employee_id }}" }} }} }} }} ],
              "end": true }}
          ]
        }}"#
        )
    }

    /// 引擎端到端：实例上下文经 `$context` 表达式进入请求体。
    #[tokio::test]
    async fn test_engine_context_expression_end_to_end() {
        let (addr, log) = spawn_request_recording_server().await;
        let svc = WorkflowEngineService::new();
        let def_id = svc
            .deploy_definition("test", &sample_context_dispatch_sw(&addr))
            .await
            .unwrap();
        let inst = svc
            .start_instance(&def_id, serde_json::json!({"employee_id": "E-99"}))
            .await
            .unwrap();

        let mut terminal = false;
        for _ in 0..40 {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            let cur = svc.get_instance(&inst.id).await.unwrap().unwrap();
            if cur.status == InstanceStatus::Completed || cur.status == InstanceStatus::Failed {
                assert_eq!(
                    cur.status,
                    InstanceStatus::Completed,
                    "工作流未成功：{:?}",
                    cur.fault
                );
                terminal = true;
                break;
            }
        }
        assert!(terminal, "工作流未在限时内终止");
        let req = log.lock().join("\n");
        assert!(
            req.contains(r#""employee_id":"E-99""#),
            "上下文未经 $context 进入请求体：{req}"
        );
    }

    #[tokio::test]
    async fn test_http_dispatcher_endpoint_string_form() {
        use crate::services::workflow::phase4::HttpTaskDispatcher;
        let addr = spawn_echo_server().await;
        let dispatcher = HttpTaskDispatcher::new();
        let with = serde_json::json!({
            "method": "GET",
            "endpoint": format!("http://{addr}"),
        });
        let result = dispatcher
            .dispatch("http", Some(&with), &serde_json::json!({}), None)
            .await;
        match result {
            DispatchResult::Success { data } => {
                let echo = data.get("echo").and_then(|v| v.as_str()).unwrap_or("");
                assert!(echo.starts_with("GET /"), "unexpected echo: {echo}");
            }
            DispatchResult::Failure { error, .. } => panic!("HTTP dispatch failed: {error}"),
        }
    }

    #[tokio::test]
    async fn test_grpc_dispatch_invalid_config() {
        use crate::services::workflow::phase4::HttpTaskDispatcher;
        let dispatcher = HttpTaskDispatcher::new();
        // 缺少 method → 配置错误（非重试）
        let with = serde_json::json!({"endpoint": {"uri": "http://127.0.0.1:1"}});
        let result = dispatcher
            .dispatch("grpc", Some(&with), &serde_json::json!({}), None)
            .await;
        assert!(matches!(
            result,
            DispatchResult::Failure {
                retryable: false,
                ..
            }
        ));
    }

    // ─── 审批 v2 服务面（命令 / 查询 / 身份） ───

    fn sample_approve_yaml(name: &str) -> String {
        format!(
            r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: {name}
  version: "1.0"
do:
  - review:
      approve:
        approvers: ["alice"]
"#
        )
    }

    fn sample_approve_role_yaml(name: &str, role: &str) -> String {
        format!(
            r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: {name}
  version: "1.0"
do:
  - review:
      approve:
        roles: ["{role}"]
"#
        )
    }

    fn sample_approve_quorum_yaml(name: &str) -> String {
        format!(
            r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: {name}
  version: "1.0"
do:
  - review:
      approve:
        approvers: ["alice", "bob"]
        mode: "quorum:2"
"#
        )
    }

    /// 内存审批存储 + 桥的测试服务（驱动路径与命令/查询路径共用同一桥）。
    fn approve_service(
        roles: HashMap<String, Vec<String>>,
    ) -> (WorkflowEngineService, Arc<MemoryApprovalStore>) {
        let store = Arc::new(MemoryApprovalStore::new());
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let bridge = Arc::new(ApprovalBridge::new(
            store.clone(),
            clock,
            Some(Arc::new(MapApproverResolver::new(roles))),
        ));
        (
            WorkflowEngineService::new_for_test_with_approval(Some(bridge)),
            store,
        )
    }

    /// 轮询直到 approval 实例出现某审批人的 open 席位，返回席位 id。
    async fn await_open_seat(
        store: &MemoryApprovalStore,
        instance_id: &str,
        assignee: &str,
    ) -> String {
        for _ in 0..500 {
            if let Some((engine, _)) = store.load(instance_id).await.unwrap() {
                if let Some(t) = engine
                    .tasks
                    .values()
                    .find(|t| t.assignee.as_deref() == Some(assignee) && t.status.is_open())
                {
                    return t.id.clone();
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("approval seat for '{assignee}' on '{instance_id}' never opened");
    }

    /// 轮询直到 approval 存储里至少有 `n` 个实例。
    async fn await_approval_entries(store: &MemoryApprovalStore, n: usize) {
        for _ in 0..500 {
            if store.scan(None, 200).await.unwrap().len() >= n {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("fewer than {n} approval instances opened");
    }

    /// 发起人经 `start_instance_as` 注入实例（`initiator` 为 `Some`）。
    #[tokio::test]
    async fn start_instance_as_records_initiator() {
        let svc = WorkflowEngineService::new_for_test();
        let def_id = svc
            .deploy_definition("test", &sample_linear_yaml())
            .await
            .unwrap();

        let with_initiator = svc
            .start_instance_as(&def_id, serde_json::json!({}), Some("zhangsan"))
            .await
            .unwrap();
        let loaded = svc.get_instance(&with_initiator.id).await.unwrap().unwrap();
        assert_eq!(loaded.initiator.as_deref(), Some("zhangsan"));

        // 无发起人：字段为 None（旧快照兼容）。
        let anonymous = svc
            .start_instance(&def_id, serde_json::json!({}))
            .await
            .unwrap();
        let loaded = svc.get_instance(&anonymous.id).await.unwrap().unwrap();
        assert_eq!(loaded.initiator, None);
    }

    /// 非 durable（无桥）⇒ 审批命令 fail-closed（`FailedPrecondition` 语义），不退化为内存审批。
    #[tokio::test]
    async fn approval_command_without_bridge_fails_closed() {
        let svc = WorkflowEngineService::new_for_test();
        let err = svc
            .approval_command(
                "whatever",
                "alice",
                ApprovalCommand::Approve {
                    task_id: "t1",
                    comment: None,
                },
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(
            err,
            BridgeError::InvalidTransition("workflow durable engine disabled".into())
        );

        // 查询面同样 fail-closed。
        let err = svc.list_pending_tasks("alice", 10, None).await.unwrap_err();
        assert!(matches!(err, BridgeError::InvalidTransition(_)), "{err:?}");
        let err = svc.get_instance_detail("whatever").await.unwrap_err();
        assert!(matches!(err, BridgeError::InvalidTransition(_)), "{err:?}");
    }

    /// 节点 `roles` 无解析映射 ⇒ fail-closed：驱动把实例置 Failed（不静默开节点）。
    #[tokio::test]
    async fn approve_with_unmapped_role_faults_closed() {
        let (svc, _store) = approve_service(HashMap::new());
        let def_id = svc
            .deploy_definition(
                "test",
                &sample_approve_role_yaml("unmapped-role-wf", "ghost"),
            )
            .await
            .unwrap();
        let inst = svc
            .start_instance_as(&def_id, serde_json::json!({}), Some("zhangsan"))
            .await
            .unwrap();

        let mut final_status = None;
        for _ in 0..500 {
            let cur = svc.get_instance(&inst.id).await.unwrap().unwrap();
            if cur.status.is_terminal() {
                final_status = Some(cur.status);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            final_status,
            Some(InstanceStatus::Failed),
            "未解析角色必须 fault 工作流，不得静默开节点"
        );
    }

    /// 幂等键重放：第二次调用命中缓存响应，状态与首次一致且不再驱动。
    #[tokio::test]
    async fn idem_key_replay_returns_same_status() {
        let (svc, store) = approve_service(HashMap::new());
        let def_id = svc
            .deploy_definition("test", &sample_approve_yaml("idem-wf"))
            .await
            .unwrap();
        let inst = svc
            .start_instance_as(&def_id, serde_json::json!({}), Some("zhangsan"))
            .await
            .unwrap();
        let task_id = await_open_seat(&store, &inst.id, "alice").await;

        let first = svc
            .approval_command(
                &inst.id,
                "alice",
                ApprovalCommand::Approve {
                    task_id: &task_id,
                    comment: None,
                },
                Some("idem-approve-1"),
            )
            .await
            .unwrap();
        let second = svc
            .approval_command(
                &inst.id,
                "alice",
                ApprovalCommand::Approve {
                    task_id: &task_id,
                    comment: None,
                },
                Some("idem-approve-1"),
            )
            .await
            .unwrap();
        assert_eq!(first, second, "幂等重放必须返回同一 (instance_id, status)");
        assert_eq!(first.0, inst.id);
    }

    /// 待办查询：按实例 id 分页，只返回请求 actor 的 open 席位。
    #[tokio::test]
    async fn list_pending_tasks_paginates_and_filters_actor() {
        let (svc, store) = approve_service(HashMap::new());
        let def_id = svc
            .deploy_definition("test", &sample_approve_yaml("pending-wf"))
            .await
            .unwrap();
        let mut ids = Vec::new();
        for _ in 0..3 {
            let inst = svc
                .start_instance_as(&def_id, serde_json::json!({}), Some("initor"))
                .await
                .unwrap();
            ids.push(inst.id);
        }
        await_approval_entries(&store, 3).await;

        // 非审批人：无待办。
        let (none, _) = svc.list_pending_tasks("mallory", 10, None).await.unwrap();
        assert!(none.is_empty(), "只返回请求 actor 的席位");

        // 分页 1：3 个实例各 1 席 ⇒ 需 3 页，且不重叠不遗漏。
        let mut seen = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let (page, next) = svc
                .list_pending_tasks("alice", 1, token.as_deref())
                .await
                .unwrap();
            for t in &page {
                assert_eq!(t.assignee, "alice");
                seen.push(t.instance_id.clone());
            }
            match next {
                Some(t) => token = Some(t),
                None => break,
            }
        }
        seen.sort();
        ids.sort();
        assert_eq!(seen, ids, "分页必须覆盖全部 3 个实例，不重叠不遗漏");
    }

    /// 实例详情：返回快照字段（大写状态、发起人、修订号、分支/席位/轮次）。
    #[tokio::test]
    async fn get_instance_detail_returns_snapshot_fields() {
        let (svc, store) = approve_service(HashMap::new());
        let def_id = svc
            .deploy_definition("test", &sample_approve_yaml("detail-wf"))
            .await
            .unwrap();
        let inst = svc
            .start_instance_as(&def_id, serde_json::json!({}), Some("zhangsan"))
            .await
            .unwrap();
        await_open_seat(&store, &inst.id, "alice").await;

        let detail = svc.get_instance_detail(&inst.id).await.unwrap();
        assert_eq!(detail.instance_id, inst.id);
        assert_eq!(detail.status, "RUNNING");
        assert_eq!(detail.initiator, "zhangsan");
        assert_eq!(detail.definition_name, "detail-wf");
        assert!(detail.revision >= 1, "修订号应每次变更 +1");
        assert_eq!(detail.branches.len(), 1);
        assert_eq!(detail.tasks.len(), 1);
        assert_eq!(detail.rounds.len(), 1);
        // 缺省 `mode` = 会签（All）。
        assert_eq!(detail.branches[0].join, JoinPolicy::All);

        // 参数化策略（quorum）必须原样穿过服务投影，不得退化成缺省。
        // 定义 id 为 `{namespace}-{unix_secs}`：同名前缀会在一秒内碰撞，用不同
        // namespace 保证两个定义 id 不同。
        let quorum_def = svc
            .deploy_definition(
                "test-quorum",
                &sample_approve_quorum_yaml("detail-quorum-wf"),
            )
            .await
            .unwrap();
        assert_ne!(quorum_def, def_id, "不同定义必须得到不同 id");
        let quorum_inst = svc
            .start_instance_as(&quorum_def, serde_json::json!({}), Some("zhangsan"))
            .await
            .unwrap();
        await_open_seat(&store, &quorum_inst.id, "alice").await;
        let quorum_detail = svc.get_instance_detail(&quorum_inst.id).await.unwrap();
        assert_eq!(quorum_detail.branches[0].join, JoinPolicy::Quorum(2));
    }
}
