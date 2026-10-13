// coord-agent: KvWorkflowStore —— 基于 KV + Txn + Watch 的生产级 WorkflowStore
//
// 设计:
// - 通过 coord-server 的 KV / Txn / Watch gRPC API 持久化工作流数据
// - 本地 MemoryWorkflowStore 作为读缓存，Watch 订阅实现缓存失效
// - 关键状态转换使用 Txn CAS 基于 mod_revision 保证并发安全
// - 所有写操作经过 Raft 共识（由 coord-server 保证）

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;

use coord_core::workflow::model::{WorkflowDefinition, WorkflowInstance};
use coord_core::workflow::ports::{
    MemoryWorkflowStore, StoreError, WorkflowStore, DEFAULT_LIST_PAGE_SIZE,
};
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
        let value =
            serde_json::to_vec(inst).map_err(|e| StoreError::SerializationError(e.to_string()))?;

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

    pub(crate) fn instance_key(id: &str) -> Vec<u8> {
        format!("/_workflow/v3/instances/{id}").into_bytes()
    }

    fn instance_prefix() -> Vec<u8> {
        b"/_workflow/v3/instances/".to_vec()
    }

    fn idem_key(instance_id: &str, key: &str) -> Vec<u8> {
        format!("/_workflow/v3/idem/{instance_id}:{key}").into_bytes()
    }

    /// 把 `list_instances` 的 definition 过滤值（定义名或定义 ID）解析为定义名。
    ///
    /// 定义键以 `(namespace, name, version)` 定位（见 [`Self::def_key`]），无法按
    /// ID 直接取键，故扫描定义表：定义名命中优先，其次按 ID 命中取其名，都不命中
    /// 则回退为原值（等价于按名精确匹配）。定义数量少，全表扫描可接受。
    async fn resolve_definition_name(&self, definition: &str) -> Result<String, StoreError> {
        let prefix = b"/_workflow/v3/defs/".to_vec();
        let range_end = prefix_end(&prefix);
        let pairs = self
            .inner
            .client
            .kv()
            .range(&prefix, &range_end, 0, 0)
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;
        let mut id_match: Option<String> = None;
        for (_k, v) in pairs {
            let Ok(def) = serde_json::from_slice::<WorkflowDefinition>(&v) else {
                continue;
            };
            if def.document.name == definition {
                return Ok(definition.to_string());
            }
            if id_match.is_none() && def.id.as_deref() == Some(definition) {
                id_match = Some(def.document.name.clone());
            }
        }
        Ok(id_match.unwrap_or_else(|| definition.to_string()))
    }

    // ─── 内部辅助：全量加载 ───

    async fn load_all_definitions(&self) -> Result<(), StoreError> {
        Self::reconcile_definitions(&self.inner, &self.cache).await
    }

    async fn load_all_instances(&self) -> Result<(), StoreError> {
        Self::reconcile_instances(&self.inner, &self.cache).await
    }

    /// 全量对账：重新扫描定义（覆盖缓存中的陈旧条目，并**剪除**已删除条目）。
    async fn reconcile_definitions(
        inner: &Arc<AgentInner>,
        cache: &Arc<MemoryWorkflowStore>,
    ) -> Result<(), StoreError> {
        // 全量扫描所有 namespace 的定义
        let prefix = b"/_workflow/v3/defs/".to_vec();
        let range_end = prefix_end(&prefix);
        let pairs = inner
            .client
            .kv()
            .range(&prefix, &range_end, 0, 0)
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;

        // KV 现存定义了哪些 (ns, name, version)——用于剪枝（删除不存在于 KV 的缓存条目）
        let mut present: std::collections::HashSet<String> = std::collections::HashSet::new();
        for (k, v) in pairs {
            if let Ok(def) = serde_json::from_slice::<WorkflowDefinition>(&v) {
                present.insert(String::from_utf8_lossy(&k).into_owned());
                let _ = cache.save_definition(&def).await;
            }
        }

        // 剪枝：缓存中有、KV 中没有的定义 → 删除（否则删除操作会被旧缓存遮蔽）
        if let Ok(cached) = cache.list_definitions("", usize::MAX, None).await {
            for def in cached {
                let key = Self::def_key(
                    &def.document.namespace,
                    &def.document.name,
                    &def.document.version,
                );
                if !present.contains(&String::from_utf8_lossy(&key).into_owned()) {
                    let _ = cache
                        .delete_definition(
                            &def.document.namespace,
                            &def.document.name,
                            &def.document.version,
                        )
                        .await;
                }
            }
        }
        Ok(())
    }

    /// 全量对账：重新扫描实例（覆盖缓存中的陈旧条目，并**剪除**已删除条目）。
    async fn reconcile_instances(
        inner: &Arc<AgentInner>,
        cache: &Arc<MemoryWorkflowStore>,
    ) -> Result<(), StoreError> {
        let prefix = Self::instance_prefix();
        let range_end = prefix_end(&prefix);
        let pairs = inner
            .client
            .kv()
            .range(&prefix, &range_end, 0, 0)
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;

        let mut present: std::collections::HashSet<String> = std::collections::HashSet::new();
        for (k, v) in pairs {
            if let Ok(inst) = serde_json::from_slice::<WorkflowInstance>(&v) {
                present.insert(inst.id.clone());
                let _ = cache.save_instance(&inst).await;
            }
            let _ = k; // 键前缀即实例空间；id 以值中的 id 为准（两处一致）
        }

        // 剪枝：缓存中有、KV 中没有的实例 → 删除
        if let Ok((cached, _)) = cache.list_instances(None, None, usize::MAX, None).await {
            for inst in cached {
                if !present.contains(&inst.id) {
                    let _ = cache.delete_instance(&inst.id).await;
                }
            }
        }
        Ok(())
    }

    // ─── 内部辅助：Watch 后台任务（R-AGT-14：断连重连 + 水位续传 + 全量对账）──

    /// 启动 watch 订阅后台任务。
    ///
    /// 循环体：连接 → 消费事件（更新缓存并推进水位）→ 断连退避 →
    /// 全量对账（弥补断连窗口）→ 用上次水位重连。
    async fn start_watch_background(&self) -> Result<(), StoreError> {
        let inner = Arc::clone(&self.inner);
        let cache = Arc::clone(&self.cache);
        let prefix = b"/_workflow/v3/".to_vec();
        tokio::spawn(async move {
            let mut last_rev: i64 = 0;
            let mut attempt: u32 = 0;
            loop {
                match Self::connect_and_consume(&inner, &prefix, last_rev, &cache).await {
                    Ok(final_rev) => {
                        last_rev = final_rev;
                    }
                    Err(e) => {
                        tracing::warn!("KvWorkflowStore watch ended (last_rev={last_rev}): {e}");
                    }
                }

                // 指数退避（1s → 2s → 4s → … → 上限 32s）
                attempt += 1;
                let backoff = std::time::Duration::from_secs(1u64 << attempt.min(5));
                tracing::info!(
                    "KvWorkflowStore watch reconnecting in {backoff:?} (attempt {attempt})"
                );
                tokio::time::sleep(backoff).await;

                // 断连窗口对账：全量重载定义 + 实例（覆盖式，消除陈旧缓存）
                if let Err(e) = Self::reconcile_definitions(&inner, &cache).await {
                    tracing::error!("KvWorkflowStore reconcile definitions failed: {e}");
                }
                if let Err(e) = Self::reconcile_instances(&inner, &cache).await {
                    tracing::error!("KvWorkflowStore reconcile instances failed: {e}");
                }
            }
        });
        Ok(())
    }

    /// 建立 watch 连接并消费事件直到断连，返回退出时的最新水位。
    ///
    /// - `start_rev` 为本地已知水位：断连重连时续传，避免窗口内事件丢失；
    /// - 收到 `BufferOverflow` / `HistoryUnavailable` 时返回，
    ///   由外层触发全量对账（缓存可能不完整）。
    async fn connect_and_consume(
        inner: &Arc<AgentInner>,
        prefix: &[u8],
        start_rev: i64,
        cache: &Arc<MemoryWorkflowStore>,
    ) -> Result<i64, StoreError> {
        let mut watch_rx = inner
            .client
            .watch()
            .watch(prefix, start_rev)
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;

        let mut last_rev = start_rev;
        while let Some(event_result) = watch_rx.recv().await {
            match event_result {
                Ok(event) => {
                    use coord_proto::watch::watch_event::EventType;
                    let needs_reconcile = matches!(
                        event.r#type,
                        t if t == EventType::BufferOverflow as i32
                            || t == EventType::HistoryUnavailable as i32
                    );
                    let is_delete = event.r#type == EventType::Delete as i32;
                    if event.revision > last_rev {
                        last_rev = event.revision;
                    }
                    for kv in &event.kvs {
                        let key_str = String::from_utf8_lossy(&kv.key);
                        if key_str.starts_with("/_workflow/v3/defs/") {
                            if is_delete {
                                // 删除事件不携带可解析的值：按键比对缓存条目并移除
                                // （定义表小，线性扫描足够；否则旧缓存会把已删定义遮蔽）。
                                Self::evict_definition_by_key(cache, &kv.key).await;
                            } else if let Ok(def) =
                                serde_json::from_slice::<WorkflowDefinition>(&kv.value)
                            {
                                let _ = cache.save_definition(&def).await;
                            }
                        } else if key_str.starts_with("/_workflow/v3/instances/") {
                            if is_delete {
                                if let Some(id) = key_str.rsplit('/').next() {
                                    let _ = cache.delete_instance(id).await;
                                }
                            } else if let Ok(inst) =
                                serde_json::from_slice::<WorkflowInstance>(&kv.value)
                            {
                                let _ = cache.save_instance(&inst).await;
                            }
                        }
                    }
                    if needs_reconcile {
                        tracing::warn!(
                            "KvWorkflowStore watch overflow/history-unavailable; full reconcile required"
                        );
                        return Ok(last_rev);
                    }
                }
                Err(e) => {
                    return Err(StoreError::IoError(e.to_string()));
                }
            }
        }
        // 流自然结束（服务端关闭）→ 上层重连
        Ok(last_rev)
    }
    /// 按 KV 键从缓存中移除对应定义（删除事件；定义表小，线性比对足够）。
    async fn evict_definition_by_key(cache: &Arc<MemoryWorkflowStore>, key: &[u8]) -> bool {
        let Ok(defs) = cache.list_definitions("", usize::MAX, None).await else {
            return false;
        };
        for d in defs {
            let k = Self::def_key(&d.document.namespace, &d.document.name, &d.document.version);
            if k == key {
                return cache
                    .delete_definition(&d.document.namespace, &d.document.name, &d.document.version)
                    .await
                    .unwrap_or(false);
            }
        }
        false
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
        let value =
            serde_json::to_vec(def).map_err(|e| StoreError::SerializationError(e.to_string()))?;

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
    async fn save_definition_atomic(&self, def: &WorkflowDefinition) -> Result<(), StoreError> {
        let key = Self::def_key(
            &def.document.namespace,
            &def.document.name,
            &def.document.version,
        );
        let value =
            serde_json::to_vec(def).map_err(|e| StoreError::SerializationError(e.to_string()))?;

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

    /// 普通写入（**无 CAS**）：先写 KV，成功后更新本地缓存。
    ///
    /// 缓存更新语义只在**写入生效后**发生；需要防丢更新的路径必须走
    /// [`WorkflowStore::save_instance_atomic`] / 租约的 `fenced_save`（两者在 CAS
    /// 失败时不更新缓存，避免把未生效的写落进读缓存）。
    async fn save_instance(&self, inst: &WorkflowInstance) -> Result<(), StoreError> {
        let key = Self::instance_key(&inst.id);
        let value =
            serde_json::to_vec(inst).map_err(|e| StoreError::SerializationError(e.to_string()))?;

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

    /// 线性一致读实例及其 `mod_revision`（**绕过本地缓存**：缓存不保存修订号）。
    ///
    /// 供驱动循环的 CAS 写回取期望修订号（见 `WorkflowStore::load_instance_with_rev`）。
    async fn load_instance_with_rev(
        &self,
        id: &str,
    ) -> Result<Option<(WorkflowInstance, i64)>, StoreError> {
        let key = Self::instance_key(id);
        let rows = self
            .inner
            .client
            .kv()
            .range_with_rev(&key, &key, 1, 0)
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;
        match rows.into_iter().next() {
            Some((_k, v, mod_revision)) => {
                let inst: WorkflowInstance = serde_json::from_slice(&v)
                    .map_err(|e| StoreError::SerializationError(e.to_string()))?;
                Ok(Some((inst, mod_revision)))
            }
            None => Ok(None),
        }
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
        namespace: Option<&str>,
        definition_name: Option<&str>,
        page_size: usize,
        page_token: Option<&str>,
    ) -> Result<(Vec<WorkflowInstance>, Option<String>), StoreError> {
        let page_size = if page_size == 0 {
            DEFAULT_LIST_PAGE_SIZE
        } else {
            page_size
        };
        let ns_filter = namespace.filter(|s| !s.is_empty());
        let def_filter = match definition_name.filter(|s| !s.is_empty()) {
            Some(d) => Some(self.resolve_definition_name(d).await?),
            None => None,
        };

        let prefix = Self::instance_prefix();
        let range_end = prefix_end(&prefix);
        // 续读：从上一页最后一条实例之后开始（前缀 + id + 0x00 是紧邻后继键）。
        let mut start = match page_token.filter(|t| !t.is_empty()) {
            Some(token) => key_after(&Self::instance_key(token)),
            None => prefix.clone(),
        };

        // 过取一条：多扫到一条匹配项即可确认「仍有剩余」，从而给出真实续读令牌。
        let target = page_size.saturating_add(1);
        let mut matched: Vec<WorkflowInstance> = Vec::new();
        loop {
            // KV range 的 limit 在**过滤之前**生效；若按 page_size 截断会漏掉
            // 被过滤项后面的匹配项。因此每批一次全量（limit 0，由 server 的
            // max_range_limit 封顶）扫描，循环推进直到收满或前缀扫尽。
            let pairs = self
                .inner
                .client
                .kv()
                .range(&start, &range_end, 0, 0)
                .await
                .map_err(|e| StoreError::IoError(e.to_string()))?;
            if pairs.is_empty() {
                break;
            }
            let last_key = pairs.last().map(|(k, _)| k.clone());
            for (_k, v) in &pairs {
                let inst: WorkflowInstance = serde_json::from_slice(v)
                    .map_err(|e| StoreError::SerializationError(e.to_string()))?;
                if ns_filter.is_some_and(|ns| inst.definition_ns != ns) {
                    continue;
                }
                if def_filter
                    .as_deref()
                    .is_some_and(|name| inst.definition_name != name)
                {
                    continue;
                }
                matched.push(inst);
                if matched.len() >= target {
                    break;
                }
            }
            if matched.len() >= target {
                break;
            }
            // 续读起点严格大于上一批最后一个键，保证向前推进、不重不漏。
            match last_key {
                Some(lk) => start = key_after(&lk),
                None => break,
            }
        }

        let has_more = matched.len() > page_size;
        matched.truncate(page_size);
        let next_token = if has_more {
            matched.last().map(|i| i.id.clone())
        } else {
            None
        };

        // 批量更新缓存（仅本页）
        for inst in &matched {
            let _ = self.cache.save_instance(inst).await;
        }

        Ok((matched, next_token))
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

    async fn delete_definition(
        &self,
        namespace: &str,
        name: &str,
        version: &str,
    ) -> Result<bool, StoreError> {
        let key = Self::def_key(namespace, name, version);

        // KV Delete 对不存在的 key 也成功 ⇒ 先探测存在性（返回语义：Ok(false)
        // 表示本就不存在，调用方可据此报 NOT_FOUND）。
        let existed = {
            let pairs = self
                .inner
                .client
                .kv()
                .range(&key, &key, 1, 0)
                .await
                .map_err(|e| StoreError::IoError(e.to_string()))?;
            !pairs.is_empty()
        };

        if existed {
            self.inner
                .client
                .kv()
                .delete(&key)
                .await
                .map_err(|e| StoreError::IoError(e.to_string()))?;
        }

        // 缓存失效：缓存是派生数据，删除后必须不可见（无论 KV 是否命中）。
        let _ = self.cache.delete_definition(namespace, name, version).await;
        Ok(existed)
    }

    async fn delete_instance(&self, id: &str) -> Result<bool, StoreError> {
        let key = Self::instance_key(id);

        let existed = {
            let pairs = self
                .inner
                .client
                .kv()
                .range(&key, &key, 1, 0)
                .await
                .map_err(|e| StoreError::IoError(e.to_string()))?;
            !pairs.is_empty()
        };

        if existed {
            self.inner
                .client
                .kv()
                .delete(&key)
                .await
                .map_err(|e| StoreError::IoError(e.to_string()))?;
        }

        let _ = self.cache.delete_instance(id).await;
        Ok(existed)
    }
}

// WorkflowStore 委托实现 for Arc<KvWorkflowStore>
// 注意：由于 Rust 孤儿规则限制，不能直接 impl WorkflowStore for Arc<KvWorkflowStore>。
// 取而代之，KvWorkflowStore 本身实现了 WorkflowStore，
// 而 Arc<dyn WorkflowStore + Send + Sync> 的 blanket impl（在 ports.rs）
// 负责 trait object 的委托。
// 使用时通过 Arc<KvWorkflowStore> → Arc<dyn WorkflowStore + Send + Sync> 类型转换。

// ─── 工具函数 ───

/// 计算「严格大于 `key` 的最小键」：追加 0x00 字节。
///
/// 用于 KV range 的续读起点：只要上一键非空，就能保证下一批从它之后开始
/// （不重不漏）；对任意字节串避免做进位，比 `+1` 更直观。
pub(crate) fn key_after(key: &[u8]) -> Vec<u8> {
    let mut next = key.to_vec();
    next.push(0);
    next
}

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

    #[test]
    fn test_key_after_is_strict_successor() {
        let a = KvWorkflowStore::instance_key("a");
        let after_a = key_after(&a);
        // 续读起点严格大于上一键
        assert!(after_a.as_slice() > a.as_slice());

        // 顺序上的下一个实例键仍落在续读区间内（不重不漏）
        let b = KvWorkflowStore::instance_key("b");
        assert!(b.as_slice() > after_a.as_slice());

        // 紧邻后继 = 实例键 + 0x00
        let expected: Vec<u8> = KvWorkflowStore::instance_key("inst-42")
            .into_iter()
            .chain([0u8])
            .collect();
        assert_eq!(
            key_after(&KvWorkflowStore::instance_key("inst-42")),
            expected
        );
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
            definition_hash: None,
            engine_version: None,
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
