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

use super::expression::ExpressionError;
use super::model::{WorkflowDefinition, WorkflowInstance};

// ─── Clock ───

/// 时间抽象 —— 支持注入以简化测试
pub trait Clock: Send + Sync {
    /// 当前 Unix 毫秒时间戳
    fn now_ms(&self) -> i64;
}

/// 真实系统时钟
#[derive(Default)]
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

    /// 求值表达式并注入标准运行时变量绑定（`$context/$input/$output/$task/$workflow/$runtime/...`）
    ///
    /// 默认实现忽略变量绑定（等价于 `evaluate`）；支持变量注入的实现应覆盖。
    fn evaluate_with_vars(
        &self,
        expr: &str,
        context: &Value,
        _vars: &HashMap<String, Value>,
    ) -> Result<Value, ExpressionError> {
        self.evaluate(expr, context)
    }

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

    /// 布尔求值（变量注入版）
    fn evaluate_bool_with_vars(
        &self,
        expr: &str,
        context: &Value,
        vars: &HashMap<String, Value>,
    ) -> Result<bool, ExpressionError> {
        let result = self.evaluate_with_vars(expr, context, vars)?;
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

    fn evaluate_with_vars(
        &self,
        expr: &str,
        context: &Value,
        vars: &HashMap<String, Value>,
    ) -> Result<Value, ExpressionError> {
        self.evaluate_with_vars(expr, context, vars)
    }

    fn evaluate_bool(&self, expr: &str, context: &Value) -> Result<bool, ExpressionError> {
        self.evaluate_bool(expr, context)
    }

    fn evaluate_bool_with_vars(
        &self,
        expr: &str,
        context: &Value,
        vars: &HashMap<String, Value>,
    ) -> Result<bool, ExpressionError> {
        self.evaluate_bool_with_vars(expr, context, vars)
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
    /// 派发外部调用并等待结果。
    ///
    /// `idempotency_key` 为 `Some` 时，实现必须把它作为下游去重键透传（HTTP 用
    /// `Idempotency-Key` 头）；`None` = 本次派发无去重语义（未注入 step token 存储）。
    async fn dispatch(
        &self,
        service: &str,
        with: Option<&Value>,
        input: &Value,
        idempotency_key: Option<&str>,
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
        idempotency_key: Option<&str>,
    ) -> DispatchResult {
        self.as_ref()
            .dispatch(service, with, input, idempotency_key)
            .await
    }
}

// ─── EventProvider ───

/// 事件提供者 —— 负责事件的发布与监听
#[async_trait]
pub trait EventProvider: Send + Sync {
    /// 发布事件
    async fn emit(&self, event_type: &str, source: Option<&str>, data: &Value);

    /// 等待任一匹配的事件（事件类型集合任一命中即返回）
    ///
    /// 返回实际到达的事件类型（`None` = 超时 / 未匹配 / 不支持）。
    async fn wait_for_event(
        &self,
        event_types: &[&str],
        source: Option<&str>,
        subject: Option<&str>,
        timeout_ms: u64,
    ) -> Option<String>;
}

/// 为 Arc<T> 提供 EventProvider 的委托实现（支持 trait object 类型擦除）
#[async_trait]
impl<T: EventProvider + Send + Sync + ?Sized> EventProvider for Arc<T> {
    async fn emit(&self, event_type: &str, source: Option<&str>, data: &Value) {
        self.as_ref().emit(event_type, source, data).await
    }

    async fn wait_for_event(
        &self,
        event_types: &[&str],
        source: Option<&str>,
        subject: Option<&str>,
        timeout_ms: u64,
    ) -> Option<String> {
        self.as_ref()
            .wait_for_event(event_types, source, subject, timeout_ms)
            .await
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
        _event_types: &[&str],
        _source: Option<&str>,
        _subject: Option<&str>,
        _timeout_ms: u64,
    ) -> Option<String> {
        None
    }
}

/// 事件元组类型别名（clippy::type_complexity）
type EventTuple = (String, Option<String>, Value);

/// 内存事件提供者 —— 基于 tokio::sync::broadcast
///
/// 支持单进程内的 emit/listen，用于开发、测试和单节点部署。
/// 每个事件类型对应一个 broadcast channel，listener 通过 wait_for_event
/// 等待匹配的事件到达。
pub struct MemoryEventProvider {
    /// event_type → (broadcast sender, next_event_id)
    channels: std::sync::Mutex<HashMap<String, tokio::sync::broadcast::Sender<EventTuple>>>,
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
            let mut channels = self.channels.lock().unwrap_or_else(|e| e.into_inner());
            channels
                .entry(event_type.to_string())
                .or_insert_with(|| {
                    let (tx, _) = tokio::sync::broadcast::channel(256);
                    tx
                })
                .clone()
        };
        let _ = sender.send((
            event_type.to_string(),
            source.map(|s| s.to_string()),
            data.clone(),
        ));
    }

    async fn wait_for_event(
        &self,
        event_types: &[&str],
        source: Option<&str>,
        _subject: Option<&str>,
        timeout_ms: u64,
    ) -> Option<String> {
        if event_types.is_empty() {
            return None; // 不支持无过滤的通配监听
        }

        // 为每个事件类型订阅独立 broadcast channel
        let mut rxs: Vec<(
            String,
            tokio::sync::broadcast::Receiver<(String, Option<String>, Value)>,
        )> = {
            let mut channels = self.channels.lock().unwrap_or_else(|e| e.into_inner());
            event_types
                .iter()
                .map(|et| {
                    let key = et.to_string();
                    let rx = channels
                        .entry(key.clone())
                        .or_insert_with(|| {
                            let (tx, _) = tokio::sync::broadcast::channel(256);
                            tx
                        })
                        .subscribe();
                    (key, rx)
                })
                .collect()
        };

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);

        // 多 channel 轮询（短超时轮流检查，避免单 channel 阻塞）
        loop {
            for (et, rx) in rxs.iter_mut() {
                match tokio::time::timeout(std::time::Duration::from_millis(10), rx.recv()).await {
                    Ok(Ok((recv_type, recv_source, _data))) => {
                        // 检查 source 过滤器
                        if let Some(src_filter) = source {
                            if recv_source.as_deref() != Some(src_filter) {
                                continue; // source 不匹配，继续等待
                            }
                        }
                        if event_types.contains(&recv_type.as_str()) {
                            return Some(recv_type);
                        }
                    }
                    Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_n))) => {
                        // 积压：重新订阅跳过旧消息
                        *rx = {
                            let channels = self.channels.lock().unwrap_or_else(|e| e.into_inner());
                            match channels.get(et) {
                                Some(ch) => ch.subscribe(),
                                None => return None,
                            }
                        };
                    }
                    Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => {
                        return None;
                    }
                    Err(_elapsed) => {
                        // 该 channel 短超时，轮询下一个
                    }
                }
                if tokio::time::Instant::now() >= deadline {
                    return None;
                }
            }
        }
    }
}

// ─── WorkflowStore ───

/// `list_instances` / `list_definitions` 的缺省页大小：调用方未指定
/// （`page_size == 0`）时存储层采用的兜底值。
pub const DEFAULT_LIST_PAGE_SIZE: usize = 50;

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
        Ok(defs
            .into_iter()
            .filter(|d| d.document.name == name)
            .collect())
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

    /// 线性一致地读取实例及其**存储修订号**（`mod_revision`）。
    ///
    /// 供驱动循环做 `Compare(ModRev)` 的乐观并发控制：拿到期望修订号后，
    /// 关键状态转换用 [`WorkflowStore::save_instance_atomic`] 写回，冲突即失去写权。
    ///
    /// 返回 `Ok(Some((inst, mod_revision)))` 表示该存储支持 CAS 前置读取；
    /// `Ok(None)` 表示该存储不提供修订号 —— 调用方**必须回退**到普通
    /// `save_instance`，且不得把它当作「实例不存在」（不存在由 `load_instance`
    /// 的 `Ok(None)` 表达）。默认实现返回 `Ok(None)`，保证内存存储行为不变。
    async fn load_instance_with_rev(
        &self,
        id: &str,
    ) -> Result<Option<(WorkflowInstance, i64)>, StoreError> {
        let _ = id;
        Ok(None)
    }

    /// 列出命名空间下的工作流定义
    async fn list_definitions(
        &self,
        namespace: &str,
        page_size: usize,
        page_token: Option<&str>,
    ) -> Result<Vec<WorkflowDefinition>, StoreError>;

    /// 列出工作流实例（过滤 + 分页）
    ///
    /// # 过滤
    /// - `namespace`：`None` 或空串 = 不过滤；否则按实例的 `definition_ns` 精确匹配；
    /// - `definition_name`：`None` 或空串 = 不过滤；否则按实例的 `definition_name`
    ///   精确匹配。取值可为定义名或定义 ID（实现负责把 ID 解析为定义名）。
    ///
    /// # 分页契约
    /// - `page_size` 限制**过滤后**的返回条数；`0` 取 [`DEFAULT_LIST_PAGE_SIZE`]；
    /// - `page_token` 为上一页返回的续读令牌（**不透明**：调用方只应原样回传，
    ///   不得解析其内容）；
    /// - 返回 `(instances, next_token)`；`next_token` 仅在过滤结果仍有剩余时为
    ///   `Some`，为 `None` 表示已到末尾。逐页续读保证不重不漏。
    async fn list_instances(
        &self,
        namespace: Option<&str>,
        definition_name: Option<&str>,
        page_size: usize,
        page_token: Option<&str>,
    ) -> Result<(Vec<WorkflowInstance>, Option<String>), StoreError>;

    /// 保存恢复幂等键
    ///
    /// 返回 true 表示首次使用此 key（允许继续恢复），
    /// false 表示 key 已消费（重复请求，应直接返回当前实例状态）。
    async fn save_resume_idempotency_key(
        &self,
        instance_id: &str,
        key: &str,
    ) -> Result<bool, StoreError>;

    /// 删除定义（保留策略入口，G-WF-1）。
    ///
    /// 存储层只做删除与缓存失效；「是否允许删」（无实例引用等）的守卫由调用方
    /// （引擎层）执行。返回 `Ok(true)` = 存在并已删除，`Ok(false)` = 本就不存在。
    /// 未实现删除原语的后端返回 [`StoreError::Unsupported`]，调用方必须按
    /// 「不支持」fail-closed 处理。删除不可回滚（无归档副本）。
    async fn delete_definition(
        &self,
        namespace: &str,
        name: &str,
        version: &str,
    ) -> Result<bool, StoreError> {
        let _ = (namespace, name, version);
        Err(StoreError::Unsupported(
            "delete_definition is not supported by this store".into(),
        ))
    }

    /// 删除实例（保留策略入口，G-WF-1）。语义与 [`WorkflowStore::delete_definition`]
    /// 相同：存储层不做状态守卫，`Ok(true)` = 存在并已删除。
    async fn delete_instance(&self, id: &str) -> Result<bool, StoreError> {
        let _ = id;
        Err(StoreError::Unsupported(
            "delete_instance is not supported by this store".into(),
        ))
    }
}

/// 存储错误
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    NotFound(String),
    AlreadyExists(String),
    IoError(String),
    SerializationError(String),
    /// 后端不支持该原语（如未实现删除的存储）——调用方必须 fail-closed 处理，
    /// 不得当作「已删除」或「不存在」。
    Unsupported(String),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::NotFound(msg) => write!(f, "not found: {msg}"),
            StoreError::AlreadyExists(msg) => write!(f, "already exists: {msg}"),
            StoreError::IoError(msg) => write!(f, "io error: {msg}"),
            StoreError::SerializationError(msg) => write!(f, "serialization error: {msg}"),
            StoreError::Unsupported(msg) => write!(f, "unsupported: {msg}"),
        }
    }
}

impl std::error::Error for StoreError {}

// ─── 实例驱动租约（单写者） ───
//
// 「一实例一驱动」的端口抽象。coord Lease **没有 fencing token**
// （见 `docs/production/consumer-contract.md` §3），因此正确性**不能**只靠租约互斥：
// 失去租约的驱动者可能迟到写入。fencing 由**实现方**用「租约 key 的 mod_revision
// 作为 epoch + 写入时三重 compare（实例 mod_revision + 租约 key mod_revision + 租约 key 值==token）」承担
// （见 [`LeaseGuard::fenced_save`]）。

/// 实例驱动租约 —— 保证同一实例同一时刻只有一个驱动者。
///
/// 对象安全，可注入 [`super::runtime::WorkflowRuntime`]；未注入时引擎不做租约，
/// 行为与现状一致。
#[async_trait]
pub trait InstanceLease: Send + Sync {
    /// 尝试获取 `instance_id` 的驱动租约。
    ///
    /// - `Ok(Some(guard))`：获得写权（guard 在释放/过期前有效，需周期续租）；
    /// - `Ok(None)`：已被其他驱动者持有 —— 调用方应**跳过**驱动（不等待、不阻塞）；
    /// - `Err(_)`：租约后端不可用 —— 调用方必须 **fail-closed** 跳过驱动，
    ///   不得退化为无租约驱动。
    async fn acquire(&self, instance_id: &str) -> Result<Option<Arc<dyn LeaseGuard>>, StoreError>;
}

/// 实例驱动租约句柄 —— 持有期间代表本驱动者拥有实例写权。
#[async_trait]
pub trait LeaseGuard: Send + Sync {
    /// 续租；返回 `false` 表示租约已丢失（写权失效）。
    async fn renew(&self) -> Result<bool, StoreError>;

    /// 主动释放租约（尽力而为：释放失败由 TTL 过期兜底回收）。
    async fn release(&self);

    /// fencing epoch：租约权威的单调标识（KV 侧 = 租约 key 的 `mod_revision`）。
    fn epoch(&self) -> i64;

    /// 续租间隔（默认 1s）；由驱动的后台续租任务使用。
    fn renew_interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs(1)
    }

    /// 在 fencing 保护下原子写实例状态。
    ///
    /// 实现必须**同时**校验：
    /// 1. 实例 key 的 `mod_revision == expected_mod_rev`（乐观并发）；
    /// 2. 租约 key 的 `mod_revision == self.epoch()`（租约未被撤销/重授）。
    ///
    /// 返回 `true` 表示写入生效；`false` 表示校验失败（本驱动已失去写权）。
    /// 默认实现返回 `false`（fail-closed）——不支持 fencing 的租约**不得**静默写入。
    async fn fenced_save(
        &self,
        _inst: &WorkflowInstance,
        _expected_mod_rev: i64,
    ) -> Result<bool, StoreError> {
        Err(StoreError::Unsupported(
            "fenced_save is not supported by this lease backend".into(),
        ))
    }
}

// ─── MemoryWorkflowStore（生产可用，RaftWorkflowStore 的热缓存） ───

/// 内存工作流存储 —— 用于测试和 RaftWorkflowStore 的本地热缓存
#[derive(Default)]
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

    /// 把 `list_instances` 的 definition 过滤值（定义名或定义 ID）解析为定义名。
    /// 定义名直接命中；否则按 ID 匹配定义取其名；都不命中则回退为原值
    /// （等价于按名精确匹配，通常结果为空）。
    fn resolve_definition_name(&self, definition: &str) -> Result<String, StoreError> {
        let defs = self
            .definitions
            .lock()
            .map_err(|e| StoreError::IoError(e.to_string()))?;
        if defs.values().any(|d| d.document.name == definition) {
            return Ok(definition.to_string());
        }
        if let Some(def) = defs.values().find(|d| d.id.as_deref() == Some(definition)) {
            return Ok(def.document.name.clone());
        }
        Ok(definition.to_string())
    }
}

/// 对已过滤的实例集合做稳定排序、分页与续读。
///
/// 排序键为实例 id（与 KV 前缀扫描的键序一致），续读令牌即上一页最后一条的
/// 实例 id；实现按 id **严格大于**令牌定位起点，避免分页边界重复或遗漏。
/// 返回的 `next_token` 仅在仍有剩余时为 `Some`。
fn paginate_instances(
    mut matched: Vec<WorkflowInstance>,
    page_size: usize,
    page_token: Option<&str>,
) -> (Vec<WorkflowInstance>, Option<String>) {
    matched.sort_by(|a, b| a.id.cmp(&b.id));
    let page_size = if page_size == 0 {
        DEFAULT_LIST_PAGE_SIZE
    } else {
        page_size
    };
    let start = match page_token.filter(|t| !t.is_empty()) {
        Some(token) => matched.partition_point(|i| i.id.as_str() <= token),
        None => 0,
    };
    let end = start.saturating_add(page_size).min(matched.len());
    let has_more = end < matched.len();
    let page = matched[start..end].to_vec();
    let next_token = if has_more {
        page.last().map(|i| i.id.clone())
    } else {
        None
    };
    (page, next_token)
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
        namespace: Option<&str>,
        definition_name: Option<&str>,
        page_size: usize,
        page_token: Option<&str>,
    ) -> Result<(Vec<WorkflowInstance>, Option<String>), StoreError> {
        let ns = namespace.filter(|s| !s.is_empty());
        let def = match definition_name.filter(|s| !s.is_empty()) {
            Some(d) => Some(self.resolve_definition_name(d)?),
            None => None,
        };

        let matched: Vec<WorkflowInstance> = self
            .instances
            .lock()
            .map_err(|e| StoreError::IoError(e.to_string()))?
            .values()
            .filter(|i| ns.is_none_or(|ns| i.definition_ns == ns))
            .filter(|i| def.as_deref().is_none_or(|name| i.definition_name == name))
            .cloned()
            .collect();

        Ok(paginate_instances(matched, page_size, page_token))
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

    async fn delete_definition(
        &self,
        namespace: &str,
        name: &str,
        version: &str,
    ) -> Result<bool, StoreError> {
        let key = Self::def_key(namespace, name, version);
        Ok(self
            .definitions
            .lock()
            .map_err(|e| StoreError::IoError(e.to_string()))?
            .remove(&key)
            .is_some())
    }

    async fn delete_instance(&self, id: &str) -> Result<bool, StoreError> {
        Ok(self
            .instances
            .lock()
            .map_err(|e| StoreError::IoError(e.to_string()))?
            .remove(id)
            .is_some())
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
        self.as_ref()
            .load_definition(namespace, name, version)
            .await
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
        self.as_ref()
            .list_definitions(namespace, page_size, page_token)
            .await
    }

    async fn list_instances(
        &self,
        namespace: Option<&str>,
        definition_name: Option<&str>,
        page_size: usize,
        page_token: Option<&str>,
    ) -> Result<(Vec<WorkflowInstance>, Option<String>), StoreError> {
        self.as_ref()
            .list_instances(namespace, definition_name, page_size, page_token)
            .await
    }

    async fn save_resume_idempotency_key(
        &self,
        instance_id: &str,
        key: &str,
    ) -> Result<bool, StoreError> {
        self.as_ref()
            .save_resume_idempotency_key(instance_id, key)
            .await
    }

    async fn delete_definition(
        &self,
        namespace: &str,
        name: &str,
        version: &str,
    ) -> Result<bool, StoreError> {
        self.as_ref()
            .delete_definition(namespace, name, version)
            .await
    }

    async fn delete_instance(&self, id: &str) -> Result<bool, StoreError> {
        self.as_ref().delete_instance(id).await
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
        self.as_ref()
            .load_definition(namespace, name, version)
            .await
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
        self.as_ref()
            .list_definitions(namespace, page_size, page_token)
            .await
    }

    async fn list_instances(
        &self,
        namespace: Option<&str>,
        definition_name: Option<&str>,
        page_size: usize,
        page_token: Option<&str>,
    ) -> Result<(Vec<WorkflowInstance>, Option<String>), StoreError> {
        self.as_ref()
            .list_instances(namespace, definition_name, page_size, page_token)
            .await
    }

    async fn save_resume_idempotency_key(
        &self,
        instance_id: &str,
        key: &str,
    ) -> Result<bool, StoreError> {
        self.as_ref()
            .save_resume_idempotency_key(instance_id, key)
            .await
    }

    async fn delete_definition(
        &self,
        namespace: &str,
        name: &str,
        version: &str,
    ) -> Result<bool, StoreError> {
        self.as_ref()
            .delete_definition(namespace, name, version)
            .await
    }

    async fn delete_instance(&self, id: &str) -> Result<bool, StoreError> {
        self.as_ref().delete_instance(id).await
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

    pub use super::MemoryWorkflowStore;
    pub use super::NoopEventProvider;
    /// 可控时钟（测试用）—— 重新导出
    pub use super::TestClock;

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
            self.emitted_events.lock().unwrap().push((
                event_type.to_string(),
                source.map(|s| s.to_string()),
                data.clone(),
            ));
        }
        async fn wait_for_event(
            &self,
            _event_types: &[&str],
            _source: Option<&str>,
            _subject: Option<&str>,
            _timeout_ms: u64,
        ) -> Option<String> {
            None
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
            _idempotency_key: Option<&str>,
        ) -> DispatchResult {
            DispatchResult::Success { data: Value::Null }
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
            schedule: Default::default(),
            auth: Default::default(),
            secrets: Default::default(),
            constants: Default::default(),
            task_meta: Default::default(),
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
        assert_eq!(
            loaded.status,
            crate::workflow::model::InstanceStatus::Running
        );
    }

    #[tokio::test]
    async fn test_memory_store_load_nonexistent() {
        let store = MemoryWorkflowStore::new();
        let result = store.load_instance("nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_memory_store_delete_definition_and_instance() {
        let store = MemoryWorkflowStore::new();

        // 定义：删除返回「是否命中」，重复删除返回 false（幂等语义在调用方）
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
            schedule: Default::default(),
            auth: Default::default(),
            secrets: Default::default(),
            constants: Default::default(),
            task_meta: Default::default(),
            raw_yaml: None,
        };
        store.save_definition(&def).await.unwrap();
        assert!(store.delete_definition("test", "wf", "1.0").await.unwrap());
        assert!(store
            .load_definition("test", "wf", "1.0")
            .await
            .unwrap()
            .is_none());
        assert!(!store.delete_definition("test", "wf", "1.0").await.unwrap());

        // 实例：同上
        let inst = WorkflowInstance {
            id: "inst-del-1".into(),
            definition_ns: "test".into(),
            definition_name: "wf".into(),
            definition_version: "1.0".into(),
            status: crate::workflow::model::InstanceStatus::Completed,
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
        assert!(store.delete_instance("inst-del-1").await.unwrap());
        assert!(store.load_instance("inst-del-1").await.unwrap().is_none());
        assert!(!store.delete_instance("inst-del-1").await.unwrap());
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
            p.wait_for_event(&["order.created"], Some("/coord/orders"), None, 2000)
                .await
        });

        // 短暂延迟确保订阅已建立
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        provider
            .emit(
                "order.created",
                Some("/coord/orders"),
                &serde_json::json!({"id": "123"}),
            )
            .await;

        let received = handle.await.unwrap();
        assert_eq!(
            received.as_deref(),
            Some("order.created"),
            "should receive the emitted event"
        );
    }

    #[tokio::test]
    async fn test_memory_event_provider_wait_timeout() {
        let provider = MemoryEventProvider::new();

        // 没有 emit，应该超时
        let received = provider
            .wait_for_event(&["nonexistent.event"], None, None, 100)
            .await;
        assert!(received.is_none(), "should timeout when no event emitted");
    }

    #[tokio::test]
    async fn test_memory_event_provider_source_filter() {
        let provider = std::sync::Arc::new(MemoryEventProvider::new());

        // 启动 wait for source B（先订阅）
        let p_b = provider.clone();
        let handle_b = tokio::spawn(async move {
            p_b.wait_for_event(&["ping"], Some("/source/B"), None, 500)
                .await
        });

        // 启动 wait for source A（先订阅）
        let p_a = provider.clone();
        let handle_a = tokio::spawn(async move {
            p_a.wait_for_event(&["ping"], Some("/source/A"), None, 2000)
                .await
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // emit from source A
        provider
            .emit("ping", Some("/source/A"), &serde_json::json!({}))
            .await;

        // source B 的 wait 应该超时
        let r_b = handle_b.await.unwrap();
        assert!(r_b.is_none(), "should not match different source");

        // source A 的 wait 应该匹配
        let r_a = handle_a.await.unwrap();
        assert_eq!(
            r_a.as_deref(),
            Some("ping"),
            "should match the emitted source"
        );
    }

    #[tokio::test]
    async fn test_memory_event_provider_multiple_subscribers() {
        let provider = std::sync::Arc::new(MemoryEventProvider::new());

        let p1 = provider.clone();
        let h1 = tokio::spawn(async move {
            p1.wait_for_event(&["broadcast.test"], None, None, 2000)
                .await
        });

        let p2 = provider.clone();
        let h2 = tokio::spawn(async move {
            p2.wait_for_event(&["broadcast.test"], None, None, 2000)
                .await
        });

        // 短暂延迟后 emit
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        provider
            .emit("broadcast.test", None, &serde_json::json!({"msg": "hello"}))
            .await;

        let r1 = h1.await.unwrap();
        let r2 = h2.await.unwrap();
        assert_eq!(
            r1.as_deref(),
            Some("broadcast.test"),
            "subscriber 1 should receive event"
        );
        assert_eq!(
            r2.as_deref(),
            Some("broadcast.test"),
            "subscriber 2 should receive event"
        );
    }

    // ─── list_instances 过滤 + 分页（issue #56 契约） ───

    fn mk_instance(id: &str, ns: &str, name: &str) -> WorkflowInstance {
        WorkflowInstance {
            id: id.into(),
            definition_ns: ns.into(),
            definition_name: name.into(),
            definition_version: "1.0".into(),
            status: crate::workflow::model::InstanceStatus::Running,
            context: serde_json::json!({}),
            task_stack: vec![],
            current_task_index: 0,
            created_at: 0,
            updated_at: 0,
            output: None,
            fault: None,
            suspension_meta: None,
        }
    }

    fn mk_definition(id: Option<&str>, ns: &str, name: &str) -> WorkflowDefinition {
        WorkflowDefinition {
            id: id.map(str::to_string),
            document: Document {
                dsl: "1.0.0".into(),
                namespace: ns.into(),
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
            schedule: Default::default(),
            auth: Default::default(),
            secrets: Default::default(),
            constants: Default::default(),
            task_meta: Default::default(),
            raw_yaml: None,
        }
    }

    #[tokio::test]
    async fn test_list_instances_filters_by_namespace_and_definition() {
        let store = MemoryWorkflowStore::new();
        store
            .save_definition(&mk_definition(Some("def-a"), "ns-a", "alpha-wf"))
            .await
            .unwrap();
        store
            .save_definition(&mk_definition(Some("def-b"), "ns-b", "beta-wf"))
            .await
            .unwrap();
        store
            .save_instance(&mk_instance("a-1", "ns-a", "alpha-wf"))
            .await
            .unwrap();
        store
            .save_instance(&mk_instance("a-2", "ns-a", "alpha-wf"))
            .await
            .unwrap();
        store
            .save_instance(&mk_instance("b-1", "ns-b", "beta-wf"))
            .await
            .unwrap();

        // namespace 过滤：ns-b 实例不得出现
        let (all_a, token) = store
            .list_instances(Some("ns-a"), None, 10, None)
            .await
            .unwrap();
        assert_eq!(all_a.len(), 2);
        assert!(all_a.iter().all(|i| i.definition_ns == "ns-a"));
        assert!(token.is_none());

        // definition 过滤：ID 解析为定义名
        let (by_id, _) = store
            .list_instances(None, Some("def-a"), 10, None)
            .await
            .unwrap();
        assert_eq!(by_id.len(), 2);
        assert!(by_id.iter().all(|i| i.definition_name == "alpha-wf"));

        // definition 过滤：直接按名
        let (by_name, _) = store
            .list_instances(None, Some("beta-wf"), 10, None)
            .await
            .unwrap();
        assert_eq!(by_name.len(), 1);
        assert_eq!(by_name[0].id, "b-1");
    }

    #[tokio::test]
    async fn test_list_instances_paginates_without_overlap_or_omission() {
        let store = MemoryWorkflowStore::new();
        for i in 0..5u32 {
            store
                .save_instance(&mk_instance(&format!("a-{i}"), "ns-a", "alpha-wf"))
                .await
                .unwrap();
        }

        let (page1, t1) = store
            .list_instances(Some("ns-a"), Some("alpha-wf"), 2, None)
            .await
            .unwrap();
        assert_eq!(page1.len(), 2);
        let t1 = t1.expect("有剩余时必须返回续读令牌");

        let (page2, t2) = store
            .list_instances(Some("ns-a"), Some("alpha-wf"), 2, Some(&t1))
            .await
            .unwrap();
        assert_eq!(page2.len(), 2);
        let t2 = t2.expect("有剩余时必须返回续读令牌");

        let (page3, t3) = store
            .list_instances(Some("ns-a"), Some("alpha-wf"), 2, Some(&t2))
            .await
            .unwrap();
        assert_eq!(page3.len(), 1);
        assert!(t3.is_none(), "末页不得返回续读令牌");

        let mut ids: Vec<String> = page1
            .iter()
            .chain(page2.iter())
            .chain(page3.iter())
            .map(|i| i.id.clone())
            .collect();
        assert_eq!(ids.len(), 5);
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 5, "分页不得重叠");
    }

    #[tokio::test]
    async fn test_list_instances_page_size_zero_uses_default() {
        let store = MemoryWorkflowStore::new();
        for i in 0..(DEFAULT_LIST_PAGE_SIZE + 1) {
            store
                .save_instance(&mk_instance(&format!("i-{i:04}"), "ns", "wf"))
                .await
                .unwrap();
        }
        let (page, token) = store.list_instances(None, None, 0, None).await.unwrap();
        assert_eq!(page.len(), DEFAULT_LIST_PAGE_SIZE);
        assert!(token.is_some(), "超过一页时必须返回续读令牌");
    }
}
