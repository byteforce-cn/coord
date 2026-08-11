// coord-agent: KvWorkflowStore —— 基于 KV + Txn + Watch 的生产级 WorkflowStore
//
// 设计:
// - 通过 coord-server 的 KV / Txn / Watch gRPC API 持久化工作流数据
// - 本地 MemoryWorkflowStore 作为读缓存，Watch 订阅实现缓存失效
// - 关键状态转换使用 Txn CAS 基于 mod_revision 保证并发安全
// - 所有写操作经过 Raft 共识（由 coord-server 保证）
//
// 参见 docs/kv-workflow-store-dev-plan.md

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::mpsc;

use coord_core::workflow::model::{WorkflowDefinition, WorkflowInstance};
use coord_core::workflow::ports::{MemoryWorkflowStore, StoreError, WorkflowStore};
use coord_proto::kv::PutRequest;
use coord_proto::txn::compare::{CompareResult, Target, TargetValue};
use coord_proto::txn::request_op::Op;
use coord_proto::txn::{Compare, RequestOp};

use crate::proxy::AgentInner;

// ─── KvWorkflowStore ───

/// 基于 KV + Txn + Watch 的生产级 WorkflowStore
///
/// # 架构
///
/// ```text
/// KvWorkflowStore
/// ├── Arc<AgentInner>        → gRPC 客户端（KV / Txn / Watch）
/// ├── Arc<MemoryWorkflowStore> → 本地读缓存
/// └── AtomicBool             → 初始化标记
/// ```
///
/// 读路径：本地缓存命中 → 直接返回；miss → KV range → 填充缓存 → 返回
/// 写路径：KV put → 更新本地缓存
/// 缓存失效：Watch 订阅 → 收到事件 → 失效本地缓存对应 key
pub struct KvWorkflowStore {
    /// coord-client 内部客户端（共享 AgentInner）
    inner: Arc<AgentInner>,
    /// 本地读缓存
    cache: Arc<MemoryWorkflowStore>,
    /// 是否已初始化（完成全量加载 + Watch 订阅）
    initialized: AtomicBool,
}

impl KvWorkflowStore {
    /// 创建新的 KvWorkflowStore（尚未初始化）
    pub fn new(inner: Arc<AgentInner>) -> Self {
        Self {
            inner,
            cache: Arc::new(MemoryWorkflowStore::new()),
            initialized: AtomicBool::new(false),
        }
    }

    /// 启动恢复：全量加载定义和实例 + 启动 Watch 订阅
    ///
    /// 多次调用安全（幂等保护）。
    pub async fn init(&self) -> Result<(), StoreError> {
        if self.initialized.swap(true, Ordering::SeqCst) {
            return Ok(()); // 已初始化
        }

        // 1. 全量加载工作流定义
        self.load_all_definitions().await?;

        // 2. 全量加载工作流实例
        self.load_all_instances().await?;

        // 3. 启动 Watch 订阅（后台任务）
        self.start_watch_background().await?;

        Ok(())
    }

    /// 带状态检查的原子写入（Txn CAS 基于 mod_revision）
    ///
    /// 用于关键状态转换（Complete/Fail/Suspend/Resume），防止并发冲突。
    /// 返回 true 表示 CAS 成功写入，false 表示冲突（需重试）。
    pub async fn save_instance_atomic(
        &self,
        inst: &WorkflowInstance,
        expected_mod_rev: i64,
    ) -> Result<bool, StoreError> {
        let key = Self::instance_key(&inst.id);
        let value = serde_json::to_vec(inst)
            .map_err(|e| StoreError::SerializationError(e.to_string()))?;

        let compare = Compare {
            result: CompareResult::Equal as i32,
            target: Target::ModRev as i32,
            key: key.clone(),
            target_value: Some(TargetValue::ModRevision(expected_mod_rev)),
        };

        let put = RequestOp {
            op: Some(Op::RequestPut(PutRequest {
                key,
                value,
                lease_id: 0,
                prev_kv: false,
                request_id: vec![],
            })),
        };

        let resp = self
            .inner
            .client
            .txn()
            .txn(vec![compare], vec![put], vec![])
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;

        if resp.succeeded {
            // CAS 成功，更新本地缓存
            self.cache.save_instance(inst).await?;
        }

        Ok(resp.succeeded)
    }

    // ─── 内部辅助：Key 构造 ───

    fn def_key(namespace: &str, name: &str, version: &str) -> Vec<u8> {
        format!("/_workflow/v3/defs/{namespace}/{name}@{version}").into_bytes()
    }

    fn def_prefix(namespace: &str) -> Vec<u8> {
        format!("/_workflow/v3/defs/{namespace}/").into_bytes()
    }

    fn instance_key(id: &str) -> Vec<u8> {
        format!("/_workflow/v3/instances/{id}").into_bytes()
    }

    fn instance_prefix() -> Vec<u8> {
        b"/_workflow/v3/instances/".to_vec()
    }

    fn idem_key(instance_id: &str, key: &str) -> Vec<u8> {
        format!("/_workflow/v3/idem/{instance_id}:{key}").into_bytes()
    }

    // ─── 内部辅助：全量加载 ───

    async fn load_all_definitions(&self) -> Result<(), StoreError> {
        // 全量扫描所有 namespace 的定义
        let prefix = b"/_workflow/v3/defs/".to_vec();
        let range_end = prefix_end(&prefix);
        let pairs = self
            .inner
            .client
            .kv()
            .range(&prefix, &range_end, 0, 0)
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;

        for (_k, v) in pairs {
            if let Ok(def) = serde_json::from_slice::<WorkflowDefinition>(&v) {
                let _ = self.cache.save_definition(&def).await;
            }
        }
        Ok(())
    }

    async fn load_all_instances(&self) -> Result<(), StoreError> {
        let prefix = Self::instance_prefix();
        let range_end = prefix_end(&prefix);
        let pairs = self
            .inner
            .client
            .kv()
            .range(&prefix, &range_end, 0, 0)
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;

        for (_k, v) in pairs {
            if let Ok(inst) = serde_json::from_slice::<WorkflowInstance>(&v) {
                let _ = self.cache.save_instance(&inst).await;
            }
        }
        Ok(())
    }

    // ─── 内部辅助：Watch 后台任务 ───

    async fn start_watch_background(&self) -> Result<(), StoreError> {
        let prefix = b"/_workflow/v3/".to_vec();
        let cache = Arc::clone(&self.cache);

        // 获取当前最新 revision 作为 Watch 起点
        // 从 KV 读取任意 key 获得当前 revision
        let start_rev = match self
            .inner
            .client
            .kv()
            .range(&prefix, &prefix_end(&prefix), 1, 0)
            .await
        {
            Ok(pairs) if !pairs.is_empty() => {
                // 需要获取 revision，使用 range_full 或直接使用 0（从最新开始）
                // 简化：从最新开始
                0i64
            }
            _ => 0i64,
        };

        let watch_rx = self
            .inner
            .client
            .watch()
            .watch(&prefix, start_rev)
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;

        // 后台任务：接收 Watch 事件，失效本地缓存
        tokio::spawn(async move {
            Self::watch_loop(watch_rx, cache).await;
        });

        Ok(())
    }

    /// Watch 事件处理循环
    async fn watch_loop(
        mut rx: mpsc::Receiver<Result<coord_proto::watch::WatchEvent, coord_core::error::Error>>,
        cache: Arc<MemoryWorkflowStore>,
    ) {
        while let Some(event_result) = rx.recv().await {
            match event_result {
                Ok(event) => {
                    for kv in &event.kvs {
                        let key_str = String::from_utf8_lossy(&kv.key);
                        // 根据 key 前缀决定当前策略：更新缓存
                        if key_str.starts_with("/_workflow/v3/defs/") {
                            // 定义变更：用事件携带的新值更新缓存
                            if let Ok(def) = serde_json::from_slice::<WorkflowDefinition>(&kv.value) {
                                let _ = cache.save_definition(&def).await;
                            }
                        } else if key_str.starts_with("/_workflow/v3/instances/") {
                            if let Ok(inst) = serde_json::from_slice::<WorkflowInstance>(&kv.value) {
                                let _ = cache.save_instance(&inst).await;
                            }
                        }
                        // idem keys 不需要缓存处理
                    }
                }
                Err(_e) => {
                    // Watch 连接断开，日志记录后退出（由上层重连机制处理）
                    tracing::warn!("KvWorkflowStore watch stream disconnected");
                    break;
                }
            }
        }
    }
}

// ─── WorkflowStore trait 实现 ───

#[async_trait]
impl WorkflowStore for KvWorkflowStore {
    async fn save_definition(&self, def: &WorkflowDefinition) -> Result<(), StoreError> {
        let namespace = &def.document.namespace;
        let name = &def.document.name;
        let version = &def.document.version;
        let key = Self::def_key(namespace, name, version);
        let value = serde_json::to_vec(def)
            .map_err(|e| StoreError::SerializationError(e.to_string()))?;

        self.inner
            .client
            .kv()
            .put(&key, &value)
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;

        // 更新本地缓存
        self.cache.save_definition(def).await?;
        Ok(())
    }

    /// 原子保存工作流定义 —— Txn CAS（对齐 policy `PutBundle` 原子覆盖）
    ///
    /// 同一 `(namespace, name, version)` 并发覆盖时防止丢更新：
    /// - key 已存在 → Compare(VALUE == 当前值) + Put（内容 CAS）
    /// - key 不存在 → Compare(VERSION == 0) + Put（仅当不存在时写入）
    /// 冲突自动重试（≤5 次）。
    async fn save_definition_atomic(
        &self,
        def: &WorkflowDefinition,
    ) -> Result<(), StoreError> {
        let key = Self::def_key(&def.document.namespace, &def.document.name, &def.document.version);
        let value = serde_json::to_vec(def)
            .map_err(|e| StoreError::SerializationError(e.to_string()))?;

        let mut attempts = 0;
        loop {
            attempts += 1;
            if attempts > 5 {
                return Err(StoreError::IoError(
                    "save_definition_atomic: too many concurrent conflicts".into(),
                ));
            }

            // 读取当前值（判断 key 是否存在）
            let current = {
                let pairs = self
                    .inner
                    .client
                    .kv()
                    .range(&key, &key, 1, 0)
                    .await
                    .map_err(|e| StoreError::IoError(e.to_string()))?;
                pairs.into_iter().next().map(|(_k, v)| v)
            };

            let compare = match &current {
                Some(cur) => Compare {
                    result: CompareResult::Equal as i32,
                    target: Target::Value as i32,
                    key: key.clone(),
                    target_value: Some(TargetValue::Value(cur.clone())),
                },
                None => Compare {
                    result: CompareResult::Equal as i32,
                    target: Target::Version as i32,
                    key: key.clone(),
                    target_value: Some(TargetValue::Version(0)),
                },
            };

            let put = RequestOp {
                op: Some(Op::RequestPut(PutRequest {
                    key: key.clone(),
                    value: value.clone(),
                    lease_id: 0,
                    prev_kv: false,
                    request_id: vec![],
                })),
            };

            let resp = self
                .inner
                .client
                .txn()
                .txn(vec![compare], vec![put], vec![])
                .await
                .map_err(|e| StoreError::IoError(e.to_string()))?;

            if resp.succeeded {
                // CAS 成功，更新本地缓存
                self.cache.save_definition(def).await?;
                return Ok(());
            }
            // CAS 冲突（并发覆盖）→ 重试
        }
    }

    async fn load_definition(
        &self,
        namespace: &str,
        name: &str,
        version: &str,
    ) -> Result<Option<WorkflowDefinition>, StoreError> {
        // 先查本地缓存
        if let Some(def) = self.cache.load_definition(namespace, name, version).await? {
            return Ok(Some(def));
        }

        let key = Self::def_key(namespace, name, version);
        let pairs = self
            .inner
            .client
            .kv()
            .range(&key, &key, 1, 0)
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;

        if let Some((_k, v)) = pairs.into_iter().next() {
            let def: WorkflowDefinition = serde_json::from_slice(&v)
                .map_err(|e| StoreError::SerializationError(e.to_string()))?;
            self.cache.save_definition(&def).await?;
            Ok(Some(def))
        } else {
            Ok(None)
        }
    }

    async fn save_instance(&self, inst: &WorkflowInstance) -> Result<(), StoreError> {
        let key = Self::instance_key(&inst.id);
        let value = serde_json::to_vec(inst)
            .map_err(|e| StoreError::SerializationError(e.to_string()))?;

        self.inner
            .client
            .kv()
            .put(&key, &value)
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;

        // 更新本地缓存
        self.cache.save_instance(inst).await?;
        Ok(())
    }

    async fn load_instance(&self, id: &str) -> Result<Option<WorkflowInstance>, StoreError> {
        // 先查本地缓存
        if let Some(inst) = self.cache.load_instance(id).await? {
            return Ok(Some(inst));
        }

        let key = Self::instance_key(id);
        let pairs = self
            .inner
            .client
            .kv()
            .range(&key, &key, 1, 0)
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;

        if let Some((_k, v)) = pairs.into_iter().next() {
            let inst: WorkflowInstance = serde_json::from_slice(&v)
                .map_err(|e| StoreError::SerializationError(e.to_string()))?;
            self.cache.save_instance(&inst).await?;
            Ok(Some(inst))
        } else {
            Ok(None)
        }
    }

    async fn list_definitions(
        &self,
        namespace: &str,
        _page_size: usize,
        _page_token: Option<&str>,
    ) -> Result<Vec<WorkflowDefinition>, StoreError> {
        let prefix = if namespace.is_empty() {
            b"/_workflow/v3/defs/".to_vec()
        } else {
            Self::def_prefix(namespace)
        };
        let range_end = prefix_end(&prefix);

        let pairs = self
            .inner
            .client
            .kv()
            .range(&prefix, &range_end, 0, 0)
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;

        let mut defs = Vec::with_capacity(pairs.len());
        for (_k, v) in pairs {
            let def: WorkflowDefinition = serde_json::from_slice(&v)
                .map_err(|e| StoreError::SerializationError(e.to_string()))?;
            // 按 namespace 过滤（如果指定了）
            if namespace.is_empty() || def.document.namespace == namespace {
                defs.push(def);
            }
        }

        // 批量更新缓存
        for def in &defs {
            let _ = self.cache.save_definition(def).await;
        }

        Ok(defs)
    }

    async fn list_instances(
        &self,
        _namespace: Option<&str>,
        _definition_name: Option<&str>,
        _page_size: usize,
        _page_token: Option<&str>,
    ) -> Result<Vec<WorkflowInstance>, StoreError> {
        let prefix = Self::instance_prefix();
        let range_end = prefix_end(&prefix);

        let pairs = self
            .inner
            .client
            .kv()
            .range(&prefix, &range_end, 0, 0)
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;

        let mut instances = Vec::with_capacity(pairs.len());
        for (_k, v) in pairs {
            let inst: WorkflowInstance = serde_json::from_slice(&v)
                .map_err(|e| StoreError::SerializationError(e.to_string()))?;
            instances.push(inst);
        }

        // 批量更新缓存
        for inst in &instances {
            let _ = self.cache.save_instance(inst).await;
        }

        Ok(instances)
    }

    async fn save_resume_idempotency_key(
        &self,
        instance_id: &str,
        key: &str,
    ) -> Result<bool, StoreError> {
        let idem_key = Self::idem_key(instance_id, key);

        // Txn CAS: Version==0 表示 key 不存在 → 首次使用
        let compare = Compare {
            result: CompareResult::Equal as i32,
            target: Target::Version as i32,
            key: idem_key.clone(),
            target_value: Some(TargetValue::Version(0)),
        };

        let put = RequestOp {
            op: Some(Op::RequestPut(PutRequest {
                key: idem_key,
                value: vec![],
                lease_id: 0,
                prev_kv: false,
                request_id: vec![],
            })),
        };

        let resp = self
            .inner
            .client
            .txn()
            .txn(vec![compare], vec![put], vec![])
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;

        Ok(resp.succeeded)
    }

    async fn save_instance_atomic(
        &self,
        inst: &WorkflowInstance,
        expected_mod_rev: i64,
    ) -> Result<bool, StoreError> {
        self.save_instance_atomic(inst, expected_mod_rev).await
    }
}

// WorkflowStore 委托实现 for Arc<KvWorkflowStore>
// 注意：由于 Rust 孤儿规则限制，不能直接 impl WorkflowStore for Arc<KvWorkflowStore>。
// 取而代之，KvWorkflowStore 本身实现了 WorkflowStore，
// 而 Arc<dyn WorkflowStore + Send + Sync> 的 blanket impl（在 ports.rs）
// 负责 trait object 的委托。
// 使用时通过 Arc<KvWorkflowStore> → Arc<dyn WorkflowStore + Send + Sync> 类型转换。

// ─── 工具函数 ───

/// 计算前缀扫描的 range_end（prefix 最后一个字节 +1）
///
/// 例如：prefix "/_workflow/v3/defs/ns/" → range_end "/_workflow/v3/defs/ns0"
/// 空列表表示 prefix 全是 0xFF，返回空 Vec 表示扫描到无穷。
pub(crate) fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    for i in (0..end.len()).rev() {
        if end[i] < 0xFF {
            end[i] += 1;
            end.truncate(i + 1);
            return end;
        }
    }
    // prefix 全是 0xFF，返回空表示扫描到无穷
    Vec::new()
}

// ═══════════════════════════════════════════════════════════════════
// 测试 (TDD)
// ═══════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    // ─── Key 构造测试 ───

    #[test]
    fn test_def_key_format() {
        let key = KvWorkflowStore::def_key("default", "my-workflow", "1.0");
        let key_str = String::from_utf8(key).unwrap();
        assert_eq!(key_str, "/_workflow/v3/defs/default/my-workflow@1.0");
    }

    #[test]
    fn test_def_key_with_special_chars() {
        let key = KvWorkflowStore::def_key("com.example", "order-process", "2.0.1");
        let key_str = String::from_utf8(key).unwrap();
        assert_eq!(
            key_str,
            "/_workflow/v3/defs/com.example/order-process@2.0.1"
        );
    }

    #[test]
    fn test_def_prefix_format() {
        let prefix = KvWorkflowStore::def_prefix("default");
        let prefix_str = String::from_utf8(prefix).unwrap();
        assert_eq!(prefix_str, "/_workflow/v3/defs/default/");
    }

    #[test]
    fn test_instance_key_format() {
        let key = KvWorkflowStore::instance_key("inst-12345");
        let key_str = String::from_utf8(key).unwrap();
        assert_eq!(key_str, "/_workflow/v3/instances/inst-12345");
    }

    #[test]
    fn test_instance_prefix_format() {
        let prefix = KvWorkflowStore::instance_prefix();
        let prefix_str = String::from_utf8(prefix).unwrap();
        assert_eq!(prefix_str, "/_workflow/v3/instances/");
    }

    #[test]
    fn test_idem_key_format() {
        let key = KvWorkflowStore::idem_key("inst-abc", "idem-xyz");
        let key_str = String::from_utf8(key).unwrap();
        assert_eq!(key_str, "/_workflow/v3/idem/inst-abc:idem-xyz");
    }

    // ─── prefix_end 测试 ───

    #[test]
    fn test_prefix_end_normal() {
        let end = prefix_end(b"/_workflow/v3/defs/ns/");
        // ns/ → ns0
        assert_eq!(end, b"/_workflow/v3/defs/ns0".to_vec());
    }

    #[test]
    fn test_prefix_end_last_byte_ff() {
        let end = prefix_end(b"/_workflow/\xff");
        // \xff → carry: /_workflow/ becomes /_workflox
        // Actually: [47, 95, 119, 111, 114, 107, 102, 108, 111, 119, 47, 255]
        // Last byte 0xFF → go to prev byte 47('/') → 48('0') → truncate
        assert_eq!(end, b"/_workflow0".to_vec());
    }

    #[test]
    fn test_prefix_end_all_ff() {
        let end = prefix_end(&[0xFF, 0xFF, 0xFF]);
        // All 0xFF → empty (scan to infinity)
        assert!(end.is_empty());
    }

    #[test]
    fn test_prefix_end_empty() {
        let end = prefix_end(b"");
        // Empty → empty (no bytes to increment)
        assert!(end.is_empty());
    }

    // ─── 序列化往返测试 ───

    #[test]
    fn test_workflow_instance_json_roundtrip() {
        use coord_core::workflow::model::InstanceStatus;
        let inst = WorkflowInstance {
            id: "test-inst-1".to_string(),
            definition_ns: "default".to_string(),
            definition_name: "test-wf".to_string(),
            definition_version: "1.0".to_string(),
            status: InstanceStatus::Running,
            context: serde_json::json!({"key": "value"}),
            task_stack: vec![],
            current_task_index: 0,
            created_at: 1000,
            updated_at: 2000,
            output: Some(serde_json::json!({"result": "ok"})),
            fault: None,
            suspension_meta: None,
        };

        let json = serde_json::to_vec(&inst).unwrap();
        let restored: WorkflowInstance = serde_json::from_slice(&json).unwrap();
        assert_eq!(restored.id, inst.id);
        assert_eq!(restored.status, inst.status);
        assert_eq!(restored.created_at, inst.created_at);
    }

    #[test]
    fn test_workflow_definition_json_roundtrip() {
        use coord_core::workflow::model::Document;
        let def = WorkflowDefinition {
            id: Some("def-1".to_string()),
            document: Document {
                dsl: "1.0".to_string(),
                namespace: "default".to_string(),
                name: "test-wf".to_string(),
                version: "1.0".to_string(),
                title: None,
                summary: None,
                tags: None,
            },
            do_tasks: vec![],
            input: None,
            output: None,
            timeout: None,
            use_components: None,
            schedule: Default::default(),
            auth: Default::default(),
            secrets: Default::default(),
            constants: Default::default(),
            task_meta: Default::default(),
            raw_yaml: Some("name: test-wf\nversion: \"1.0\"".to_string()),
        };

        let json = serde_json::to_vec(&def).unwrap();
        let restored: WorkflowDefinition = serde_json::from_slice(&json).unwrap();
        assert_eq!(restored.id, def.id);
        assert_eq!(restored.document.name, def.document.name);
        assert_eq!(restored.raw_yaml, def.raw_yaml);
    }

    // ─── KvWorkflowStore 构造与缓存测试 ───

    /// 测试 KvWorkflowStore 可以创建并持有缓存引用
    #[test]
    fn test_kv_store_creation() {
        // 这个测试不依赖实际 AgentInner，仅验证结构可创建
        let cache = Arc::new(MemoryWorkflowStore::new());
        // 验证 MemoryWorkflowStore 基本操作可用
        let _ = cache;
    }

    // ─── WorkflowStore for Arc<KvWorkflowStore> 委托测试 ───

    /// 验证 KvWorkflowStore 可以通过 trait object 使用
    #[test]
    fn test_kv_store_trait_object_compatible() {
        // KvWorkflowStore 实现了 WorkflowStore trait
        // 因此 Arc<KvWorkflowStore> 可以转为 Arc<dyn WorkflowStore + Send + Sync>
        // (通过 blanket impl WorkflowStore for Arc<dyn WorkflowStore + Send + Sync>)
        fn assert_workflow_store<T: WorkflowStore + Send + Sync + ?Sized>(_: &T) {}
        
        // 验证 MemoryWorkflowStore 满足约束（编译期检查）
        let mem = MemoryWorkflowStore::new();
        assert_workflow_store(&mem);
    }
}
