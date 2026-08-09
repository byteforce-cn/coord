// coord-core/workflow/ports.rs
// 端口 trait 抽象层 —— 执行引擎与外部世界的契约
//
// 定义工作流执行所需的所有外部依赖接口：
// - Clock:           时间抽象（可注入，便于测试）
// - ExpressionEval:  表达式求值（jq 子集）
// - TaskDispatcher:  外部服务调用派发（HTTP/gRPC/function）
// - EventProvider:   事件总线（CloudEvents 发布/订阅）
// - WorkflowStore:   持久化存储（内存 / Raft）

use async_trait::async_trait;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use super::model::{WorkflowDefinition, WorkflowInstance};
use super::expression::ExpressionError;

// ─── Clock ───

/// 时间抽象 —— 支持注入以简化测试
pub trait Clock: Send + Sync {
    /// 当前 Unix 毫秒时间戳
    fn now_ms(&self) -> i64;
}

/// 真实系统时钟
pub struct SystemClock;

impl SystemClock {
    pub fn new() -> Self {
        SystemClock
    }
}

impl Clone for SystemClock {
    fn clone(&self) -> Self {
        SystemClock
    }
}

impl Clock for SystemClock {
    fn now_ms(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    }
}

// ─── ExpressionEval ───

/// 表达式求值 —— 适配现有的 ExpressionEvaluator
pub trait ExpressionEval: Send + Sync {
    /// 求值表达式，返回 JSON Value
    fn evaluate(&self, expr: &str, context: &Value) -> Result<Value, ExpressionError>;

    /// 求值布尔表达式
    fn evaluate_bool(&self, expr: &str, context: &Value) -> Result<bool, ExpressionError> {
        let result = self.evaluate(expr, context)?;
        Ok(match result {
            Value::Bool(b) => b,
            Value::Null => false,
            Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
            Value::String(s) => !s.is_empty(),
            Value::Array(a) => !a.is_empty(),
            Value::Object(o) => !o.is_empty(),
        })
    }
}

/// 将现有的 ExpressionEvaluator 适配到 ExpressionEval trait
impl ExpressionEval for super::expression::ExpressionEvaluator {
    fn evaluate(&self, expr: &str, context: &Value) -> Result<Value, ExpressionError> {
        self.evaluate(expr, context)
    }

    fn evaluate_bool(&self, expr: &str, context: &Value) -> Result<bool, ExpressionError> {
        self.evaluate_bool(expr, context)
    }
}

// ─── TaskDispatcher ───

/// 任务派发结果
#[derive(Debug, Clone, PartialEq)]
pub enum DispatchResult {
    /// 调用成功，返回响应数据
    Success { data: Value },
    /// 调用失败
    Failure { error: String, retryable: bool },
}

/// 任务派发器 —— 负责执行 call 任务的外部调用
#[async_trait]
pub trait TaskDispatcher: Send + Sync {
    /// 派发外部调用并等待结果
    async fn dispatch(
        &self,
        service: &str,
        with: Option<&Value>,
        input: &Value,
    ) -> DispatchResult;
}

/// 为 Arc<T> 提供 TaskDispatcher 的委托实现（支持 trait object 类型擦除）
#[async_trait]
impl<T: TaskDispatcher + Send + Sync + ?Sized> TaskDispatcher for Arc<T> {
    async fn dispatch(
        &self,
        service: &str,
        with: Option<&Value>,
        input: &Value,
    ) -> DispatchResult {
        self.as_ref().dispatch(service, with, input).await
    }
}

// ─── EventProvider ───

/// 事件提供者 —— 负责事件的发布与监听
#[async_trait]
pub trait EventProvider: Send + Sync {
    /// 发布事件
    async fn emit(&self, event_type: &str, source: Option<&str>, data: &Value);

    /// 等待匹配的事件（返回 true 表示事件已到达）
    async fn wait_for_event(
        &self,
        event_type: Option<&str>,
        source: Option<&str>,
        subject: Option<&str>,
        timeout_ms: u64,
    ) -> bool;
}

/// 为 Arc<T> 提供 EventProvider 的委托实现（支持 trait object 类型擦除）
#[async_trait]
impl<T: EventProvider + Send + Sync + ?Sized> EventProvider for Arc<T> {
    async fn emit(&self, event_type: &str, source: Option<&str>, data: &Value) {
        self.as_ref().emit(event_type, source, data).await
    }

    async fn wait_for_event(
        &self,
        event_type: Option<&str>,
        source: Option<&str>,
        subject: Option<&str>,
        timeout_ms: u64,
    ) -> bool {
        self.as_ref().wait_for_event(event_type, source, subject, timeout_ms).await
    }
}

/// 空事件提供者 —— 所有操作均为 no-op
///
/// 用于测试或不需要事件能力的部署场景。
pub struct NoopEventProvider;

#[async_trait]
impl EventProvider for NoopEventProvider {
    async fn emit(&self, _event_type: &str, _source: Option<&str>, _data: &Value) {}
    async fn wait_for_event(
        &self,
        _event_type: Option<&str>,
        _source: Option<&str>,
        _subject: Option<&str>,
        _timeout_ms: u64,
    ) -> bool {
        false
    }
}

/// 内存事件提供者 —— 基于 tokio::sync::broadcast
///
/// 支持单进程内的 emit/listen，用于开发、测试和单节点部署。
/// 每个事件类型对应一个 broadcast channel，listener 通过 wait_for_event
/// 等待匹配的事件到达。
pub struct MemoryEventProvider {
    /// event_type → (broadcast sender, next_event_id)
    channels: std::sync::Mutex<
        HashMap<String, tokio::sync::broadcast::Sender<(String, Option<String>, Value)>>,
    >,
}

impl MemoryEventProvider {
    /// 创建新的内存事件提供者
    pub fn new() -> Self {
        Self {
            channels: std::sync::Mutex::new(HashMap::new()),
        }
    }
}

impl Default for MemoryEventProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl EventProvider for MemoryEventProvider {
    async fn emit(&self, event_type: &str, source: Option<&str>, data: &Value) {
        let sender = {
            let mut channels = self.channels.lock().unwrap();
            channels
                .entry(event_type.to_string())
                .or_insert_with(|| {
                    let (tx, _) = tokio::sync::broadcast::channel(256);
                    tx
                })
                .clone()
        };
        let _ = sender.send((event_type.to_string(), source.map(|s| s.to_string()), data.clone()));
    }

    async fn wait_for_event(
        &self,
        event_type: Option<&str>,
        source: Option<&str>,
        _subject: Option<&str>,
        timeout_ms: u64,
    ) -> bool {
        let et = match event_type {
            Some(et) => et.to_string(),
            None => return false, // 不支持无过滤的通配监听
        };

        let mut rx = {
            let mut channels = self.channels.lock().unwrap();
            channels
                .entry(et.clone())
                .or_insert_with(|| {
                    let (tx, _) = tokio::sync::broadcast::channel(256);
                    tx
                })
                .subscribe()
        };

        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_millis(timeout_ms);

        loop {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Ok((recv_type, recv_source, _data))) => {
                    // 检查 source 过滤器
                    if let Some(ref src_filter) = source {
                        if recv_source.as_deref() != Some(&src_filter[..]) {
                            continue; // source 不匹配，继续等待
                        }
                    }
                    // 检查 event_type（冗余但安全）
                    if recv_type == et {
                        return true;
                    }
                }
                Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(n))) => {
                    // 重新订阅以跳过积压消息
                    let _ = n; // n = 跳过的消息数
                    rx = {
                        let channels = self.channels.lock().unwrap();
                        channels.get(&et).unwrap().subscribe()
                    };
                    continue;
                }
                Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => {
                    return false;
                }
                Err(_elapsed) => {
                    // 超时
                    return false;
                }
            }
        }
    }
}

// ─── WorkflowStore ───

/// 工作流持久化存储
#[async_trait]
pub trait WorkflowStore: Send + Sync {
    /// 保存工作流定义
    async fn save_definition(&self, def: &WorkflowDefinition) -> Result<(), StoreError>;

    /// 原子保存工作流定义（对齐 policy `PutBundle` 的原子覆盖语义）
    ///
    /// 用于定义部署：同一 `(namespace, name, version)` 并发覆盖时防止丢更新
    /// （KvWorkflowStore 实现为 Txn CAS，冲突自动重试）。
    /// 默认实现回退到乐观 `save_definition()`（MemoryWorkflowStore 基于互斥锁，天然原子）。
    async fn save_definition_atomic(&self, def: &WorkflowDefinition) -> Result<(), StoreError> {
        self.save_definition(def).await
    }

    /// 列出某 `(namespace, name)` 定义的全部版本（回滚目标发现）
    ///
    /// 默认实现基于 `list_definitions` 过滤；KvWorkflowStore 可覆盖为前缀扫描。
    async fn list_definition_versions(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Vec<WorkflowDefinition>, StoreError> {
        let defs = self.list_definitions(namespace, usize::MAX, None).await?;
        Ok(defs.into_iter().filter(|d| d.document.name == name).collect())
    }

    /// 加载工作流定义
    async fn load_definition(
        &self,
        namespace: &str,
        name: &str,
        version: &str,
    ) -> Result<Option<WorkflowDefinition>, StoreError>;

    /// 保存工作流实例
    async fn save_instance(&self, inst: &WorkflowInstance) -> Result<(), StoreError>;

    /// 原子保存工作流实例（CAS 基于 mod_revision）
    ///
    /// 用于关键状态转换（Complete/Fail/Suspend/Resume），防止并发冲突。
    /// 返回 true 表示 CAS 成功写入，false 表示冲突（需调用方重试）。
    ///
    /// 默认实现回退到乐观 `save_instance()`（适用于 MemoryWorkflowStore）。
    async fn save_instance_atomic(
        &self,
        inst: &WorkflowInstance,
        _expected_mod_rev: i64,
    ) -> Result<bool, StoreError> {
        self.save_instance(inst).await?;
        Ok(true)
    }

    /// 加载工作流实例
    async fn load_instance(&self, id: &str) -> Result<Option<WorkflowInstance>, StoreError>;

    /// 列出命名空间下的工作流定义
    async fn list_definitions(
        &self,
        namespace: &str,
        page_size: usize,
        page_token: Option<&str>,
    ) -> Result<Vec<WorkflowDefinition>, StoreError>;

    /// 列出工作流实例
    async fn list_instances(
        &self,
        namespace: Option<&str>,
        definition_name: Option<&str>,
        page_size: usize,
        page_token: Option<&str>,
    ) -> Result<Vec<WorkflowInstance>, StoreError>;

    /// 保存恢复幂等键
    ///
    /// 返回 true 表示首次使用此 key（允许继续恢复），
    /// false 表示 key 已消费（重复请求，应直接返回当前实例状态）。
    async fn save_resume_idempotency_key(
        &self,
        instance_id: &str,
        key: &str,
    ) -> Result<bool, StoreError>;
}

/// 存储错误
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    NotFound(String),
    AlreadyExists(String),
    IoError(String),
    SerializationError(String),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::NotFound(msg) => write!(f, "not found: {msg}"),
            StoreError::AlreadyExists(msg) => write!(f, "already exists: {msg}"),
            StoreError::IoError(msg) => write!(f, "io error: {msg}"),
            StoreError::SerializationError(msg) => write!(f, "serialization error: {msg}"),
        }
    }
}

impl std::error::Error for StoreError {}

// ─── MemoryWorkflowStore（生产可用，RaftWorkflowStore 的热缓存） ───

/// 内存工作流存储 —— 用于测试和 RaftWorkflowStore 的本地热缓存
pub struct MemoryWorkflowStore {
    pub(crate) definitions: Mutex<HashMap<String, WorkflowDefinition>>,
    pub(crate) instances: Mutex<HashMap<String, WorkflowInstance>>,
    /// 幂等键集合: key = "{instance_id}:{idempotency_key}"
    idempotency_keys: Mutex<HashSet<String>>,
}

impl MemoryWorkflowStore {
    pub fn new() -> Self {
        Self {
            definitions: Mutex::new(HashMap::new()),
            instances: Mutex::new(HashMap::new()),
            idempotency_keys: Mutex::new(HashSet::new()),
        }
    }

    fn def_key(namespace: &str, name: &str, version: &str) -> String {
        format!("{namespace}/{name}@{version}")
    }
}

#[async_trait]
impl WorkflowStore for MemoryWorkflowStore {
    async fn save_definition(&self, def: &WorkflowDefinition) -> Result<(), StoreError> {
        let key = Self::def_key(
            &def.document.namespace,
            &def.document.name,
            &def.document.version,
        );
        self.definitions
            .lock()
            .map_err(|e| StoreError::IoError(e.to_string()))?
            .insert(key, def.clone());
        Ok(())
    }

    async fn load_definition(
        &self,
        namespace: &str,
        name: &str,
        version: &str,
    ) -> Result<Option<WorkflowDefinition>, StoreError> {
        let key = Self::def_key(namespace, name, version);
        Ok(self
            .definitions
            .lock()
            .map_err(|e| StoreError::IoError(e.to_string()))?
            .get(&key)
            .cloned())
    }

    async fn save_instance(&self, inst: &WorkflowInstance) -> Result<(), StoreError> {
        self.instances
            .lock()
            .map_err(|e| StoreError::IoError(e.to_string()))?
            .insert(inst.id.clone(), inst.clone());
        Ok(())
    }

    async fn load_instance(&self, id: &str) -> Result<Option<WorkflowInstance>, StoreError> {
        Ok(self
            .instances
            .lock()
            .map_err(|e| StoreError::IoError(e.to_string()))?
            .get(id)
            .cloned())
    }

    async fn list_definitions(
        &self,
        namespace: &str,
        _page_size: usize,
        _page_token: Option<&str>,
    ) -> Result<Vec<WorkflowDefinition>, StoreError> {
        let defs = self
            .definitions
            .lock()
            .map_err(|e| StoreError::IoError(e.to_string()))?;
        if namespace.is_empty() {
            Ok(defs.values().cloned().collect())
        } else {
            Ok(defs
                .values()
                .filter(|d| d.document.namespace == namespace)
                .cloned()
                .collect())
        }
    }

    async fn list_instances(
        &self,
        _namespace: Option<&str>,
        _definition_name: Option<&str>,
        _page_size: usize,
        _page_token: Option<&str>,
    ) -> Result<Vec<WorkflowInstance>, StoreError> {
        Ok(self
            .instances
            .lock()
            .map_err(|e| StoreError::IoError(e.to_string()))?
            .values()
            .cloned()
            .collect())
    }

    async fn save_resume_idempotency_key(
        &self,
        instance_id: &str,
        key: &str,
    ) -> Result<bool, StoreError> {
        let combined = format!("{instance_id}:{key}");
        let mut keys = self
            .idempotency_keys
            .lock()
            .map_err(|e| StoreError::IoError(e.to_string()))?;
        Ok(keys.insert(combined))
    }
}

// WorkflowStore 委托实现 for Arc<MemoryWorkflowStore>
// 允许在多个组件之间共享同一个 MemoryWorkflowStore 实例

#[async_trait]
impl WorkflowStore for Arc<MemoryWorkflowStore> {
    async fn save_definition(&self, def: &WorkflowDefinition) -> Result<(), StoreError> {
        self.as_ref().save_definition(def).await
    }

    async fn load_definition(
        &self,
        namespace: &str,
        name: &str,
        version: &str,
    ) -> Result<Option<WorkflowDefinition>, StoreError> {
        self.as_ref().load_definition(namespace, name, version).await
    }

    async fn save_instance(&self, inst: &WorkflowInstance) -> Result<(), StoreError> {
        self.as_ref().save_instance(inst).await
    }

    async fn load_instance(&self, id: &str) -> Result<Option<WorkflowInstance>, StoreError> {
        self.as_ref().load_instance(id).await
    }

    async fn list_definitions(
        &self,
        namespace: &str,
        page_size: usize,
        page_token: Option<&str>,
    ) -> Result<Vec<WorkflowDefinition>, StoreError> {
        self.as_ref().list_definitions(namespace, page_size, page_token).await
    }

    async fn list_instances(
        &self,
        namespace: Option<&str>,
        definition_name: Option<&str>,
        page_size: usize,
        page_token: Option<&str>,
    ) -> Result<Vec<WorkflowInstance>, StoreError> {
        self.as_ref().list_instances(namespace, definition_name, page_size, page_token).await
    }

    async fn save_resume_idempotency_key(
        &self,
        instance_id: &str,
        key: &str,
    ) -> Result<bool, StoreError> {
        self.as_ref().save_resume_idempotency_key(instance_id, key).await
    }
}

// WorkflowStore 委托实现 for Arc<dyn WorkflowStore + Send + Sync>
// 允许 WorkflowRuntime 使用 trait object 作为存储类型，支持运行时多态。

#[async_trait]
impl WorkflowStore for Arc<dyn WorkflowStore + Send + Sync> {
    async fn save_definition(&self, def: &WorkflowDefinition) -> Result<(), StoreError> {
        self.as_ref().save_definition(def).await
    }

    async fn load_definition(
        &self,
        namespace: &str,
        name: &str,
        version: &str,
    ) -> Result<Option<WorkflowDefinition>, StoreError> {
        self.as_ref().load_definition(namespace, name, version).await
    }

    async fn save_instance(&self, inst: &WorkflowInstance) -> Result<(), StoreError> {
        self.as_ref().save_instance(inst).await
    }

    async fn load_instance(&self, id: &str) -> Result<Option<WorkflowInstance>, StoreError> {
        self.as_ref().load_instance(id).await
    }

    async fn list_definitions(
        &self,
        namespace: &str,
        page_size: usize,
        page_token: Option<&str>,
    ) -> Result<Vec<WorkflowDefinition>, StoreError> {
        self.as_ref().list_definitions(namespace, page_size, page_token).await
    }

    async fn list_instances(
        &self,
        namespace: Option<&str>,
        definition_name: Option<&str>,
        page_size: usize,
        page_token: Option<&str>,
    ) -> Result<Vec<WorkflowInstance>, StoreError> {
        self.as_ref().list_instances(namespace, definition_name, page_size, page_token).await
    }

    async fn save_resume_idempotency_key(
        &self,
        instance_id: &str,
        key: &str,
    ) -> Result<bool, StoreError> {
        self.as_ref().save_resume_idempotency_key(instance_id, key).await
    }
}

// ─── 测试用 Mock 实现 ───

/// 可控时钟（测试用）
#[cfg(test)]
pub struct TestClock {
    time_ms: Mutex<i64>,
}

#[cfg(test)]
impl TestClock {
    pub fn new(start_ms: i64) -> Self {
        Self {
            time_ms: Mutex::new(start_ms),
        }
    }

    pub fn advance(&self, ms: i64) {
        let mut t = self.time_ms.lock().unwrap();
        *t += ms;
    }
}

#[cfg(test)]
impl Clock for TestClock {
    fn now_ms(&self) -> i64 {
        *self.time_ms.lock().unwrap()
    }
}

#[cfg(test)]
pub mod test_utils {
    use super::*;

    /// 可控时钟（测试用）—— 重新导出
    pub use super::TestClock;
    pub use super::MemoryWorkflowStore;
    pub use super::NoopEventProvider;

    /// 记录式事件提供者（测试用）—— 记录所有 emit 调用，支持断言
    pub struct RecordingEventProvider {
        pub emitted_events: Mutex<Vec<(String, Option<String>, Value)>>,
    }

    impl RecordingEventProvider {
        pub fn new() -> Self {
            Self {
                emitted_events: Mutex::new(Vec::new()),
            }
        }

        pub fn emitted_count(&self) -> usize {
            self.emitted_events.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl EventProvider for RecordingEventProvider {
        async fn emit(&self, event_type: &str, source: Option<&str>, data: &Value) {
            self.emitted_events
                .lock()
                .unwrap()
                .push((event_type.to_string(), source.map(|s| s.to_string()), data.clone()));
        }
        async fn wait_for_event(
            &self,
            _event_type: Option<&str>,
            _source: Option<&str>,
            _subject: Option<&str>,
            _timeout_ms: u64,
        ) -> bool {
            false
        }
    }

    /// 空任务派发器（测试用）
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
}

#[cfg(test)]
mod tests {
    use super::test_utils::*;
    use super::*;
    use crate::workflow::model::{Document, WorkflowDefinition};

    #[test]
    fn test_test_clock_advance() {
        let clock = TestClock::new(1000);
        assert_eq!(clock.now_ms(), 1000);
        clock.advance(500);
        assert_eq!(clock.now_ms(), 1500);
    }

    #[tokio::test]
    async fn test_memory_store_save_load_definition() {
        let store = MemoryWorkflowStore::new();
        let def = WorkflowDefinition {
            id: None,
            document: Document {
                dsl: "1.0.0".into(),
                namespace: "test".into(),
                name: "wf".into(),
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
        };

        store.save_definition(&def).await.unwrap();
        let loaded = store
            .load_definition("test", "wf", "1.0")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.document.name, "wf");
    }

    #[tokio::test]
    async fn test_memory_store_save_load_instance() {
        let store = MemoryWorkflowStore::new();
        let inst = WorkflowInstance {
            id: "inst-1".into(),
            definition_ns: "test".into(),
            definition_name: "wf".into(),
            definition_version: "1.0".into(),
            status: crate::workflow::model::InstanceStatus::Running,
            context: serde_json::json!({}),
            task_stack: vec![],
            current_task_index: 0,
            created_at: 1000,
            updated_at: 1000,
            output: None,
            fault: None,
            suspension_meta: None,
        };

        store.save_instance(&inst).await.unwrap();
        let loaded = store.load_instance("inst-1").await.unwrap().unwrap();
        assert_eq!(loaded.id, "inst-1");
        assert_eq!(loaded.status, crate::workflow::model::InstanceStatus::Running);
    }

    #[tokio::test]
    async fn test_memory_store_load_nonexistent() {
        let store = MemoryWorkflowStore::new();
        let result = store.load_instance("nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    // ─── Idempotency Key 测试 ───

    #[tokio::test]
    async fn test_idempotency_first_call_returns_true() {
        let store = MemoryWorkflowStore::new();
        let result = store
            .save_resume_idempotency_key("inst-1", "key-1")
            .await
            .unwrap();
        assert!(result, "first call with a key should return true");
    }

    #[tokio::test]
    async fn test_idempotency_duplicate_call_returns_false() {
        let store = MemoryWorkflowStore::new();
        let r1 = store
            .save_resume_idempotency_key("inst-1", "key-1")
            .await
            .unwrap();
        assert!(r1);
        let r2 = store
            .save_resume_idempotency_key("inst-1", "key-1")
            .await
            .unwrap();
        assert!(!r2, "duplicate call with same key should return false");
    }

    #[tokio::test]
    async fn test_idempotency_different_instances_same_key() {
        let store = MemoryWorkflowStore::new();
        // 相同 key 但不同 instance_id 不冲突
        let r1 = store
            .save_resume_idempotency_key("inst-1", "key-1")
            .await
            .unwrap();
        assert!(r1);
        let r2 = store
            .save_resume_idempotency_key("inst-2", "key-1")
            .await
            .unwrap();
        assert!(r2, "different instance with same key should not conflict");
    }

    // ─── MemoryEventProvider 测试 ───

    #[tokio::test]
    async fn test_memory_event_provider_emit_and_wait() {
        let provider = std::sync::Arc::new(MemoryEventProvider::new());

        // 先启动 wait（订阅），再 emit
        let p = provider.clone();
        let handle = tokio::spawn(async move {
            p.wait_for_event(Some("order.created"), Some("/coord/orders"), None, 2000)
                .await
        });

        // 短暂延迟确保订阅已建立
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        provider
            .emit("order.created", Some("/coord/orders"), &serde_json::json!({"id": "123"}))
            .await;

        let received = handle.await.unwrap();
        assert!(received, "should receive the emitted event");
    }

    #[tokio::test]
    async fn test_memory_event_provider_wait_timeout() {
        let provider = MemoryEventProvider::new();

        // 没有 emit，应该超时
        let received = provider
            .wait_for_event(Some("nonexistent.event"), None, None, 100)
            .await;
        assert!(!received, "should timeout when no event emitted");
    }

    #[tokio::test]
    async fn test_memory_event_provider_source_filter() {
        let provider = std::sync::Arc::new(MemoryEventProvider::new());

        // 启动 wait for source B（先订阅）
        let p_b = provider.clone();
        let handle_b = tokio::spawn(async move {
            p_b.wait_for_event(Some("ping"), Some("/source/B"), None, 500)
                .await
        });

        // 启动 wait for source A（先订阅）
        let p_a = provider.clone();
        let handle_a = tokio::spawn(async move {
            p_a.wait_for_event(Some("ping"), Some("/source/A"), None, 2000)
                .await
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // emit from source A
        provider
            .emit("ping", Some("/source/A"), &serde_json::json!({}))
            .await;

        // source B 的 wait 应该超时
        let r_b = handle_b.await.unwrap();
        assert!(!r_b, "should not match different source");

        // source A 的 wait 应该匹配
        let r_a = handle_a.await.unwrap();
        assert!(r_a, "should match the emitted source");
    }

    #[tokio::test]
    async fn test_memory_event_provider_multiple_subscribers() {
        let provider = std::sync::Arc::new(MemoryEventProvider::new());

        let p1 = provider.clone();
        let h1 = tokio::spawn(async move {
            p1.wait_for_event(Some("broadcast.test"), None, None, 2000)
                .await
        });

        let p2 = provider.clone();
        let h2 = tokio::spawn(async move {
            p2.wait_for_event(Some("broadcast.test"), None, None, 2000)
                .await
        });

        // 短暂延迟后 emit
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        provider
            .emit("broadcast.test", None, &serde_json::json!({"msg": "hello"}))
            .await;

        let r1 = h1.await.unwrap();
        let r2 = h2.await.unwrap();
        assert!(r1, "subscriber 1 should receive event");
        assert!(r2, "subscriber 2 should receive event");
    }
}
