// coord-core/workflow/raft_store.rs
// RaftWorkflowStore —— 基于 Raft 复制日志的持久化 WorkflowStore
//
// ⚠️ DEPRECATED: 此方案已被 KvWorkflowStore 替代。
// KvWorkflowStore (coord-agent/src/services/workflow_store.rs) 通过
// coord-server 的 KV + Txn + Watch API 实现，无需 coord-server 改造，
// 零侵入、代码更简单、模式已被 agent 其他服务充分验证。
//
// 详见 docs/kv-workflow-store-dev-plan.md。
//
// 原始设计：
// - 包装 MemoryWorkflowStore 作为热数据缓存（读取无 Raft 开销）
// - 所有写操作通过 RaftProposer trait 提交到 Raft 共识层
// - 序列化使用 bincode（紧凑二进制，快速序列化）
//
// RaftProposer trait 由 coord-server 实现，coord-core 不依赖 coord-server。
//
// 保留此文件用于 git 历史可追溯及测试中的 NoopRaftProposer。

#![allow(deprecated)]

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::model::{SuspensionMeta, WorkflowDefinition, WorkflowFault, WorkflowInstance};
use super::ports::{MemoryWorkflowStore, StoreError, WorkflowStore};

// ─── RaftProposer trait ───

/// Raft 提案接口 —— 由 coord-server 的 Raft 层实现
///
/// 所有工作流状态变更通过此 trait 提交到 Raft 共识。
/// coord-core 不依赖 coord-server，由上层组装时注入实现。
#[async_trait]
pub trait RaftProposer: Send + Sync {
    /// 向 Raft 集群提案一条命令，等待多数派确认后返回
    async fn propose(&self, cmd_bytes: Vec<u8>) -> Result<Vec<u8>, RaftProposeError>;
}

/// Raft 提案错误
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftProposeError {
    /// Raft 集群不可用（无 Leader）
    NotAvailable,
    /// 提案超时
    Timeout,
    /// 序列化错误
    Serialization(String),
    /// 内部错误
    Internal(String),
}

impl std::fmt::Display for RaftProposeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RaftProposeError::NotAvailable => write!(f, "raft cluster not available"),
            RaftProposeError::Timeout => write!(f, "raft proposal timed out"),
            RaftProposeError::Serialization(msg) => write!(f, "serialization error: {msg}"),
            RaftProposeError::Internal(msg) => write!(f, "raft internal error: {msg}"),
        }
    }
}

impl std::error::Error for RaftProposeError {}

// ─── Workflow 状态机命令 ───

/// Raft 日志中的工作流命令（所有确定性状态变更的载体）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkflowCommand {
    /// 部署/更新工作流定义
    DeployDefinition {
        definition: WorkflowDefinition,
    },
    /// 创建或更新工作流实例
    UpsertInstance {
        instance: WorkflowInstance,
    },
    /// 标记实例完成
    CompleteInstance {
        instance_id: String,
        output: serde_json::Value,
        at_ms: i64,
    },
    /// 标记实例失败
    FailInstance {
        instance_id: String,
        fault: WorkflowFault,
        at_ms: i64,
    },
    /// 挂起实例
    SuspendInstance {
        instance_id: String,
        meta: SuspensionMeta,
        at_ms: i64,
    },
    /// 恢复实例
    ResumeInstance {
        instance_id: String,
        result: serde_json::Value,
        next_task_index: usize,
        at_ms: i64,
        /// 幂等键（可选），防止网络重试导致双重恢复
        idempotency_key: Option<String>,
    },
    /// 取消实例
    CancelInstance {
        instance_id: String,
        at_ms: i64,
    },
}

/// 工作流命令的响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkflowResponse {
    Ok,
    IdempotencyKeyConsumed,
}

/// 将 WorkflowCommand 序列化为 JSON 字节
fn serialize_command(cmd: &WorkflowCommand) -> Result<Vec<u8>, StoreError> {
    serde_json::to_vec(cmd).map_err(|e| StoreError::SerializationError(e.to_string()))
}

/// 从 JSON 字节反序列化 WorkflowResponse
fn deserialize_response(bytes: &[u8]) -> Result<WorkflowResponse, StoreError> {
    serde_json::from_slice(bytes).map_err(|e| StoreError::SerializationError(e.to_string()))
}

// ─── RaftWorkflowStore ───

/// RaftWorkflowStore —— 基于 Raft 复制日志的 WorkflowStore
///
/// ⚠️ DEPRECATED: 请使用 `KvWorkflowStore` (coord-agent) 替代。
/// 详见 docs/kv-workflow-store-dev-plan.md。
///
/// # 架构
///
/// ```text
/// RaftWorkflowStore
/// ├── MemoryWorkflowStore (热数据缓存，读取直接返回)
/// ├── RaftProposer (写入走 Raft 共识)
/// └── Idempotency Keys (幂等键集合，内存维护)
/// ```
///
/// 读操作（load_*, list_*）直接访问 MemoryWorkflowStore 缓存，无需 Raft 共识。
/// 写操作（save_*, Complete/Fail/Suspend/Resume/Cancel）通过 Raft 提案 → apply 到缓存。
///
/// 恢复时，从 Raft 日志重放所有 WorkflowCommand 重建缓存状态。
#[deprecated(
    since = "0.2.0",
    note = "Use KvWorkflowStore (coord-agent/src/services/workflow_store.rs) instead. See docs/kv-workflow-store-dev-plan.md."
)]
pub struct RaftWorkflowStore<P: RaftProposer> {
    /// 内存热缓存
    inner: Arc<MemoryWorkflowStore>,
    /// Raft 提案器
    proposer: Arc<P>,
    /// 已消费的幂等键集合
    consumed_idempotency_keys: Mutex<HashSet<String>>,
}

impl<P: RaftProposer> RaftWorkflowStore<P> {
    /// 创建新的 RaftWorkflowStore
    pub fn new(proposer: P) -> Self {
        Self {
            inner: Arc::new(MemoryWorkflowStore::new()),
            proposer: Arc::new(proposer),
            consumed_idempotency_keys: Mutex::new(HashSet::new()),
        }
    }

    /// 获取内部 MemoryWorkflowStore 的引用（用于恢复重放）
    pub fn inner_store(&self) -> &Arc<MemoryWorkflowStore> {
        &self.inner
    }

    /// 直接应用一条命令到本地缓存（用于 Raft 日志重放）
    ///
    /// 此方法不经过 Raft 共识，仅更新本地缓存。
    /// 在 Raft apply 路径中调用。
    pub fn apply_command(&self, cmd: &WorkflowCommand) -> Result<WorkflowResponse, StoreError> {
        match cmd {
            WorkflowCommand::DeployDefinition { definition } => {
                // 直接写入内部存储（同步方式，因为 MemoryWorkflowStore 内部是 Mutex）
                // 注意：这里使用 block_on 或直接操作内部 map
                // MemoryWorkflowStore 的方法都是 async，但实际是同步的 Mutex 操作
                // 我们通过 inner 的公开接口来操作
                self.apply_definition_sync(definition)?;
                Ok(WorkflowResponse::Ok)
            }
            WorkflowCommand::UpsertInstance { instance } => {
                self.apply_instance_sync(instance)?;
                Ok(WorkflowResponse::Ok)
            }
            WorkflowCommand::CompleteInstance {
                instance_id,
                output,
                at_ms,
            } => {
                self.apply_complete_sync(instance_id, output, *at_ms)?;
                Ok(WorkflowResponse::Ok)
            }
            WorkflowCommand::FailInstance {
                instance_id,
                fault,
                at_ms,
            } => {
                self.apply_fail_sync(instance_id, fault, *at_ms)?;
                Ok(WorkflowResponse::Ok)
            }
            WorkflowCommand::SuspendInstance {
                instance_id,
                meta,
                at_ms,
            } => {
                self.apply_suspend_sync(instance_id, meta, *at_ms)?;
                Ok(WorkflowResponse::Ok)
            }
            WorkflowCommand::ResumeInstance {
                instance_id,
                result,
                next_task_index,
                at_ms,
                idempotency_key,
            } => {
                // 幂等性检查
                if let Some(key) = idempotency_key {
                    let combined = format!("{instance_id}:{key}");
                    let mut keys = self
                        .consumed_idempotency_keys
                        .lock()
                        .map_err(|e| StoreError::IoError(e.to_string()))?;
                    if !keys.insert(combined) {
                        return Ok(WorkflowResponse::IdempotencyKeyConsumed);
                    }
                }
                self.apply_resume_sync(instance_id, result, *next_task_index, *at_ms)?;
                Ok(WorkflowResponse::Ok)
            }
            WorkflowCommand::CancelInstance {
                instance_id,
                at_ms,
            } => {
                self.apply_cancel_sync(instance_id, *at_ms)?;
                Ok(WorkflowResponse::Ok)
            }
        }
    }

    /// 通过 Raft 提案执行命令，并应用到本地缓存
    async fn propose_and_apply(&self, cmd: WorkflowCommand) -> Result<(), StoreError> {
        let cmd_bytes = serialize_command(&cmd)?;
        let resp_bytes = self
            .proposer
            .propose(cmd_bytes)
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;
        let resp = deserialize_response(&resp_bytes)?;
        match resp {
            WorkflowResponse::Ok => {
                // apply 到本地缓存（proposer 应该已经在 apply 路径中调用了 apply_command，
                // 但为了安全这里也调用一次）
                self.apply_command(&cmd)?;
                Ok(())
            }
            WorkflowResponse::IdempotencyKeyConsumed => {
                // 幂等键已消费，不重复 apply
                Ok(())
            }
        }
    }

    // ─── 同步 apply 辅助方法（操作 MemoryWorkflowStore 内部 map） ───

    fn apply_definition_sync(&self, def: &WorkflowDefinition) -> Result<(), StoreError> {
        let key = format!(
            "{}/{}@{}",
            def.document.namespace, def.document.name, def.document.version
        );
        self.inner
            .definitions
            .lock()
            .map_err(|e: std::sync::PoisonError<_>| StoreError::IoError(e.to_string()))?
            .insert(key, def.clone());
        Ok(())
    }

    fn apply_instance_sync(&self, inst: &WorkflowInstance) -> Result<(), StoreError> {
        self.inner
            .instances
            .lock()
            .map_err(|e: std::sync::PoisonError<_>| StoreError::IoError(e.to_string()))?
            .insert(inst.id.clone(), inst.clone());
        Ok(())
    }

    fn apply_complete_sync(
        &self,
        instance_id: &str,
        output: &serde_json::Value,
        at_ms: i64,
    ) -> Result<(), StoreError> {
        let mut instances = self
            .inner
            .instances
            .lock()
            .map_err(|e: std::sync::PoisonError<_>| StoreError::IoError(e.to_string()))?;
        if let Some(inst) = instances.get_mut(instance_id) {
            inst.status = super::model::InstanceStatus::Completed;
            inst.output = Some(output.clone());
            inst.updated_at = at_ms;
        }
        Ok(())
    }

    fn apply_fail_sync(
        &self,
        instance_id: &str,
        fault: &WorkflowFault,
        at_ms: i64,
    ) -> Result<(), StoreError> {
        let mut instances = self
            .inner
            .instances
            .lock()
            .map_err(|e: std::sync::PoisonError<_>| StoreError::IoError(e.to_string()))?;
        if let Some(inst) = instances.get_mut(instance_id) {
            inst.status = super::model::InstanceStatus::Failed;
            inst.fault = Some(fault.clone());
            inst.updated_at = at_ms;
        }
        Ok(())
    }

    fn apply_suspend_sync(
        &self,
        instance_id: &str,
        meta: &SuspensionMeta,
        at_ms: i64,
    ) -> Result<(), StoreError> {
        let mut instances = self
            .inner
            .instances
            .lock()
            .map_err(|e: std::sync::PoisonError<_>| StoreError::IoError(e.to_string()))?;
        if let Some(inst) = instances.get_mut(instance_id) {
            inst.status = super::model::InstanceStatus::Suspended;
            inst.suspension_meta = Some(meta.clone());
            inst.updated_at = at_ms;
        }
        Ok(())
    }

    fn apply_resume_sync(
        &self,
        instance_id: &str,
        result: &serde_json::Value,
        next_task_index: usize,
        at_ms: i64,
    ) -> Result<(), StoreError> {
        let mut instances = self
            .inner
            .instances
            .lock()
            .map_err(|e: std::sync::PoisonError<_>| StoreError::IoError(e.to_string()))?;
        if let Some(inst) = instances.get_mut(instance_id) {
            inst.status = super::model::InstanceStatus::Running;
            inst.suspension_meta = None;
            inst.current_task_index = next_task_index;
            // 将 resume 结果合并到 context
            if !result.is_null() {
                inst.context["_resume_result"] = result.clone();
            }
            inst.updated_at = at_ms;
        }
        Ok(())
    }

    fn apply_cancel_sync(&self, instance_id: &str, at_ms: i64) -> Result<(), StoreError> {
        let mut instances = self
            .inner
            .instances
            .lock()
            .map_err(|e: std::sync::PoisonError<_>| StoreError::IoError(e.to_string()))?;
        if let Some(inst) = instances.get_mut(instance_id) {
            inst.status = super::model::InstanceStatus::Cancelled;
            inst.updated_at = at_ms;
        }
        Ok(())
    }
}

// ─── WorkflowStore trait 实现 ───

#[async_trait]
impl<P: RaftProposer + 'static> WorkflowStore for RaftWorkflowStore<P> {
    async fn save_definition(&self, def: &WorkflowDefinition) -> Result<(), StoreError> {
        let cmd = WorkflowCommand::DeployDefinition {
            definition: def.clone(),
        };
        self.propose_and_apply(cmd).await
    }

    async fn load_definition(
        &self,
        namespace: &str,
        name: &str,
        version: &str,
    ) -> Result<Option<WorkflowDefinition>, StoreError> {
        // 直接读缓存
        self.inner.load_definition(namespace, name, version).await
    }

    async fn save_instance(&self, inst: &WorkflowInstance) -> Result<(), StoreError> {
        let cmd = WorkflowCommand::UpsertInstance {
            instance: inst.clone(),
        };
        self.propose_and_apply(cmd).await
    }

    async fn load_instance(&self, id: &str) -> Result<Option<WorkflowInstance>, StoreError> {
        self.inner.load_instance(id).await
    }

    async fn list_definitions(
        &self,
        namespace: &str,
        page_size: usize,
        page_token: Option<&str>,
    ) -> Result<Vec<WorkflowDefinition>, StoreError> {
        self.inner
            .list_definitions(namespace, page_size, page_token)
            .await
    }

    async fn list_instances(
        &self,
        namespace: Option<&str>,
        definition_name: Option<&str>,
        page_size: usize,
        page_token: Option<&str>,
    ) -> Result<Vec<WorkflowInstance>, StoreError> {
        self.inner
            .list_instances(namespace, definition_name, page_size, page_token)
            .await
    }

    async fn save_resume_idempotency_key(
        &self,
        instance_id: &str,
        key: &str,
    ) -> Result<bool, StoreError> {
        // 先检查内存中的幂等键集合
        let combined = format!("{instance_id}:{key}");
        let mut keys = self
            .consumed_idempotency_keys
            .lock()
            .map_err(|e| StoreError::IoError(e.to_string()))?;
        Ok(keys.insert(combined))
    }
}

// ─── 测试用的 NoopRaftProposer ───

/// 测试用 RaftProposer：直接 apply 到本地缓存，不经过 Raft 共识
#[cfg(test)]
pub struct NoopRaftProposer {
    store: Arc<MemoryWorkflowStore>,
}

#[cfg(test)]
impl NoopRaftProposer {
    pub fn new(store: Arc<MemoryWorkflowStore>) -> Self {
        Self { store }
    }
}

#[cfg(test)]
#[async_trait]
impl RaftProposer for NoopRaftProposer {
    async fn propose(&self, cmd_bytes: Vec<u8>) -> Result<Vec<u8>, RaftProposeError> {
        let cmd: WorkflowCommand = serde_json::from_slice(&cmd_bytes)
            .map_err(|e| RaftProposeError::Serialization(e.to_string()))?;

        // 直接应用到 store（同步 Mutex 操作）
        let response = match &cmd {
            WorkflowCommand::DeployDefinition { definition } => {
                let key = format!(
                    "{}/{}@{}",
                    definition.document.namespace,
                    definition.document.name,
                    definition.document.version
                );
                self.store
                    .definitions
                    .lock()
                    .map_err(|e| RaftProposeError::Internal(e.to_string()))?
                    .insert(key, definition.clone());
                WorkflowResponse::Ok
            }
            WorkflowCommand::UpsertInstance { instance } => {
                self.store
                    .instances
                    .lock()
                    .map_err(|e| RaftProposeError::Internal(e.to_string()))?
                    .insert(instance.id.clone(), instance.clone());
                WorkflowResponse::Ok
            }
            WorkflowCommand::CompleteInstance {
                instance_id,
                output,
                at_ms,
            } => {
                let mut instances = self
                    .store
                    .instances
                    .lock()
                    .map_err(|e| RaftProposeError::Internal(e.to_string()))?;
                if let Some(inst) = instances.get_mut(instance_id) {
                    inst.status = crate::workflow::model::InstanceStatus::Completed;
                    inst.output = Some(output.clone());
                    inst.updated_at = *at_ms;
                }
                WorkflowResponse::Ok
            }
            WorkflowCommand::FailInstance {
                instance_id,
                fault,
                at_ms,
            } => {
                let mut instances = self
                    .store
                    .instances
                    .lock()
                    .map_err(|e| RaftProposeError::Internal(e.to_string()))?;
                if let Some(inst) = instances.get_mut(instance_id) {
                    inst.status = crate::workflow::model::InstanceStatus::Failed;
                    inst.fault = Some(fault.clone());
                    inst.updated_at = *at_ms;
                }
                WorkflowResponse::Ok
            }
            WorkflowCommand::SuspendInstance {
                instance_id,
                meta,
                at_ms,
            } => {
                let mut instances = self
                    .store
                    .instances
                    .lock()
                    .map_err(|e| RaftProposeError::Internal(e.to_string()))?;
                if let Some(inst) = instances.get_mut(instance_id) {
                    inst.status = crate::workflow::model::InstanceStatus::Suspended;
                    inst.suspension_meta = Some(meta.clone());
                    inst.updated_at = *at_ms;
                }
                WorkflowResponse::Ok
            }
            WorkflowCommand::ResumeInstance {
                instance_id,
                result,
                next_task_index,
                at_ms,
                idempotency_key: _,
            } => {
                let mut instances = self
                    .store
                    .instances
                    .lock()
                    .map_err(|e| RaftProposeError::Internal(e.to_string()))?;
                if let Some(inst) = instances.get_mut(instance_id) {
                    inst.status = crate::workflow::model::InstanceStatus::Running;
                    inst.suspension_meta = None;
                    inst.current_task_index = *next_task_index;
                    if !result.is_null() {
                        inst.context["_resume_result"] = result.clone();
                    }
                    inst.updated_at = *at_ms;
                }
                WorkflowResponse::Ok
            }
            WorkflowCommand::CancelInstance {
                instance_id,
                at_ms,
            } => {
                let mut instances = self
                    .store
                    .instances
                    .lock()
                    .map_err(|e| RaftProposeError::Internal(e.to_string()))?;
                if let Some(inst) = instances.get_mut(instance_id) {
                    inst.status = crate::workflow::model::InstanceStatus::Cancelled;
                    inst.updated_at = *at_ms;
                }
                WorkflowResponse::Ok
            }
        };

        serde_json::to_vec(&response)
            .map_err(|e| RaftProposeError::Serialization(e.to_string()))
    }
}

// ─── 测试 ───

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::model::{Document, InstanceStatus};
    use serde_json::json;

    fn make_test_store() -> RaftWorkflowStore<NoopRaftProposer> {
        let inner = Arc::new(MemoryWorkflowStore::new());
        let proposer = NoopRaftProposer::new(Arc::clone(&inner));
        RaftWorkflowStore {
            inner,
            proposer: Arc::new(proposer),
            consumed_idempotency_keys: Mutex::new(HashSet::new()),
        }
    }

    fn make_test_definition(name: &str) -> WorkflowDefinition {
        WorkflowDefinition {
            id: None,
            document: Document {
                dsl: "1.0.0".into(),
                namespace: "test".into(),
                name: name.into(),
                version: "1.0".into(),
                title: None,
                summary: None,
                tags: None,
            },
            do_tasks: vec![],
            input: None,
            output: None,
            timeout: None,
            use_components: None,
        raw_yaml: None,
        }
    }

    fn make_test_instance(id: &str, def_name: &str) -> WorkflowInstance {
        WorkflowInstance {
            id: id.into(),
            definition_ns: "test".into(),
            definition_name: def_name.into(),
            definition_version: "1.0".into(),
            status: InstanceStatus::Running,
            context: json!({}),
            task_stack: vec![],
            current_task_index: 0,
            created_at: 1000,
            updated_at: 1000,
            output: None,
            fault: None,
            suspension_meta: None,
        }
    }

    #[tokio::test]
    async fn test_raft_store_save_and_load_definition() {
        let store = make_test_store();
        let def = make_test_definition("test-wf");

        store.save_definition(&def).await.unwrap();
        let loaded = store
            .load_definition("test", "test-wf", "1.0")
            .await
            .unwrap()
            .unwrap();

        assert_eq!(loaded.document.name, "test-wf");
    }

    #[tokio::test]
    async fn test_raft_store_save_and_load_instance() {
        let store = make_test_store();
        let inst = make_test_instance("inst-1", "test-wf");

        store.save_instance(&inst).await.unwrap();
        let loaded = store.load_instance("inst-1").await.unwrap().unwrap();

        assert_eq!(loaded.id, "inst-1");
        assert_eq!(loaded.status, InstanceStatus::Running);
    }

    #[tokio::test]
    async fn test_raft_store_load_instance_not_found() {
        let store = make_test_store();
        let result = store.load_instance("nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_raft_store_list_definitions() {
        let store = make_test_store();
        store
            .save_definition(&make_test_definition("wf-a"))
            .await
            .unwrap();
        store
            .save_definition(&make_test_definition("wf-b"))
            .await
            .unwrap();

        let defs = store.list_definitions("test", 10, None).await.unwrap();
        assert_eq!(defs.len(), 2);
    }

    #[test]
    fn test_workflow_command_serialization_roundtrip() {
        let cmd = WorkflowCommand::DeployDefinition {
            definition: make_test_definition("test-wf"),
        };
        let bytes = serialize_command(&cmd).unwrap();
        let decoded: WorkflowCommand = serde_json::from_slice(&bytes).unwrap();
        match decoded {
            WorkflowCommand::DeployDefinition { definition } => {
                assert_eq!(definition.document.name, "test-wf");
            }
            _ => panic!("expected DeployDefinition"),
        }
    }

    #[test]
    fn test_workflow_command_resume_instance_roundtrip() {
        let cmd = WorkflowCommand::ResumeInstance {
            instance_id: "inst-1".into(),
            result: json!({"approved": true}),
            next_task_index: 3,
            at_ms: 2000,
            idempotency_key: Some("key-123".into()),
        };
        let bytes = serialize_command(&cmd).unwrap();
        let decoded: WorkflowCommand = serde_json::from_slice(&bytes).unwrap();
        match decoded {
            WorkflowCommand::ResumeInstance {
                instance_id,
                result,
                next_task_index,
                at_ms,
                idempotency_key,
            } => {
                assert_eq!(instance_id, "inst-1");
                assert_eq!(result, json!({"approved": true}));
                assert_eq!(next_task_index, 3);
                assert_eq!(at_ms, 2000);
                assert_eq!(idempotency_key, Some("key-123".into()));
            }
            _ => panic!("expected ResumeInstance"),
        }
    }

    #[tokio::test]
    async fn test_raft_store_idempotency_key() {
        let store = make_test_store();
        let r1 = store
            .save_resume_idempotency_key("inst-1", "key-1")
            .await
            .unwrap();
        assert!(r1);
        let r2 = store
            .save_resume_idempotency_key("inst-1", "key-1")
            .await
            .unwrap();
        assert!(!r2);
    }
}
