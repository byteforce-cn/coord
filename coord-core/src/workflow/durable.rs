//! 持久化 outbox 与持久化定时器（Spike 2026-10 Spike 3 决策证据）。
//!
//! 现状引擎把「状态保存」和「事件发布」拆成两次独立调用，事件经进程内
//! broadcast（重启即丢）；wait/retry/listen/超时全靠进程内 tokio 定时器
//! （进程死亡即丢）。本模块给出可注入的持久化抽象，使：
//!
//! - **状态写入与 outbox 写入同事务**（[`AtomicOutboxCommit`]），消费者按
//!   `event_id` 去重即得「不丢不重」的应用语义；
//! - **定时器写 KV**（[`DurableTimerStore`]），重启后按到期扫描重放；
//! - **Relay**（[`OutboxRelay`]）轮询 → 投递 → 标位，失败指数退避。
//!
//! 这里只定义契约与纯逻辑（记录结构、退避、relay 推进），KV/事务实现放在
//! 存储侧（`coord-agent`）；`coord-core` 不依赖任何具体后端。

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::model::WorkflowInstance;
use super::ports::StoreError;

/// 事件契约命名空间前缀（`com.china-workflow.*`）。
pub const EVENT_TYPE_NAMESPACE: &str = "com.china-workflow";

/// JSON Schema 的稳定定位前缀（dataschema）。
pub const DATASCHEMA_PREFIX: &str = "https://schemas.coord.dev/events";

/// 内部生命周期类型（标准 `io.serverlessworkflow.*`）→ **对外**事件契约类型。
///
/// 引擎内部沿用 CNCF Lifecycle Events 的类型名；对外契约（消费者看到的、以及
/// `schema/*.json` 覆盖的）统一收在 `com.china-workflow.*` 下。两者只在这一个
/// 函数里转换，避免命名空间在代码里出现第二种拼法。
pub fn external_event_type(internal: &str) -> String {
    match internal.strip_prefix("io.serverlessworkflow.") {
        Some(rest) => format!("{EVENT_TYPE_NAMESPACE}.{rest}"),
        None => internal.to_string(),
    }
}

/// 事件类型的 JSON Schema 定位。
pub fn dataschema_for(external_type: &str) -> String {
    format!("{DATASCHEMA_PREFIX}/{external_type}.json")
}

/// outbox 记录状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutboxStatus {
    /// 待投递（含重试等待）
    Pending,
    /// 已投递给下游投递面
    Delivered,
    /// 重试耗尽，停在这里等人处置（不静默丢弃）
    Dead,
}

/// outbox 记录 —— 一条「状态变更必须伴生」的待发事件。
///
/// `id` 即投递面的事件 ID（CloudEvents `id`）；消费者按它去重。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OutboxRecord {
    /// 全局唯一事件 ID（投递面去重键）
    pub id: String,
    /// 聚合根（= workflow instance_id，也是 partitionkey）
    pub aggregate_id: String,
    /// per-instance 单调序号（存储侧分配，见 `append` 契约）
    pub sequence: u64,
    /// 事件类型（`com.china-workflow.*`）
    pub event_type: String,
    /// CloudEvents specversion（固定 "1.0"）
    #[serde(default = "default_specversion")]
    pub specversion: String,
    /// 事件来源
    pub source: String,
    /// 事件主体（可选）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    /// payload 的 JSON Schema 定位（dataschema）
    pub dataschema: String,
    /// 事件时间（RFC3339 UTC）
    pub time: String,
    /// 事件负载
    pub data: Value,
    /// 入库时间（Unix ms）
    pub created_at_ms: i64,
    /// 当前状态
    pub status: OutboxStatus,
    /// 已投递尝试次数
    #[serde(default)]
    pub attempts: u32,
    /// 下一次可投递时间（Unix ms；退避用）
    #[serde(default)]
    pub next_attempt_at_ms: i64,
    /// 投递成功时间（Unix ms）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivered_at_ms: Option<i64>,
}

fn default_specversion() -> String {
    "1.0".to_string()
}

impl OutboxRecord {
    /// 生命周期事件记录（`emit_lifecycle` 的 outbox 形态）。
    ///
    /// `sequence == 0` 表示「由存储侧在同一写入里分配」。
    pub fn lifecycle(
        aggregate_id: &str,
        event_type: &str,
        data: Value,
        now_ms: i64,
        id: String,
    ) -> Self {
        let external_type = external_event_type(event_type);
        Self {
            id,
            aggregate_id: aggregate_id.to_string(),
            sequence: 0,
            dataschema: dataschema_for(&external_type),
            event_type: external_type,
            specversion: "1.0".to_string(),
            source: "coord/workflow".to_string(),
            subject: Some(aggregate_id.to_string()),
            time: rfc3339_utc(now_ms),
            data,
            created_at_ms: now_ms,
            status: OutboxStatus::Pending,
            attempts: 0,
            next_attempt_at_ms: now_ms,
            delivered_at_ms: None,
        }
    }
}

/// 指数退避：`base * 2^attempt` 封顶 `cap`。
///
/// 无抖动 —— 抖动会让「同输入同输出」的复现测试失去确定性；Spike 阶段先证明
/// 退避在不丢语义下可收敛，抖动属于部署调优。
pub fn backoff_ms(attempt: u32, base_ms: u64, cap_ms: u64) -> u64 {
    let shift = attempt.min(32);
    let factor = 1u64.checked_shl(shift).unwrap_or(u64::MAX);
    base_ms.saturating_mul(factor).min(cap_ms)
}

/// outbox 存储契约。
///
/// `append` 的分配契约：`sequence == 0` 时由实现分配该 `aggregate_id` 下的
/// 下一个单调序号；同 `id` 重复 append 是幂等空操作（重试安全）。
#[async_trait]
pub trait OutboxStore: Send + Sync {
    /// 追加一条待发事件（幂等）。
    async fn append(&self, rec: &OutboxRecord) -> Result<(), StoreError>;

    /// 列出可投递记录：`Pending` 且 `next_attempt_at_ms <= now_ms`，
    /// 按 `next_attempt_at_ms` 升序，最多 `limit` 条。
    async fn pending(&self, now_ms: i64, limit: usize) -> Result<Vec<OutboxRecord>, StoreError>;

    /// 标记投递成功。
    async fn mark_delivered(&self, id: &str, delivered_at_ms: i64) -> Result<(), StoreError>;

    /// 标记重试（更新尝试次数与下次可投递时间）。
    async fn mark_retry(
        &self,
        id: &str,
        attempts: u32,
        next_attempt_at_ms: i64,
    ) -> Result<(), StoreError>;

    /// 标记重试耗尽（进死信状态，保留记录可查）。
    async fn mark_dead(&self, id: &str, attempts: u32) -> Result<(), StoreError>;

    /// 列出全部记录（证据导出/对账）。
    async fn list_all(&self) -> Result<Vec<OutboxRecord>, StoreError>;
}

/// 状态写入 + outbox 写入的**同事务**提交契约。
///
/// 这是「事件不丢」的根：状态与事件要么都落、要么都不落；不存在
/// 「状态已改而事件缺失」的窗口。
#[async_trait]
pub trait AtomicOutboxCommit: Send + Sync {
    /// 同一事务内提交 `inst` 状态与 `rec`（`sequence == 0` 时同事务分配）。
    ///
    /// 返回 `Ok(false)` = 事务比较失败（**什么都没写**），调用方可退化为分步
    /// `append`；返回 `Err` = 基础设施错误，同样什么都没写。
    async fn commit_state_and_outbox(
        &self,
        inst: &WorkflowInstance,
        rec: &OutboxRecord,
        expected_mod_rev: Option<i64>,
    ) -> Result<bool, StoreError>;
}

/// 定时器种类 —— 决定到期后由谁处理。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum TimerKind {
    /// wait 完成 / retry 重试（同 `WorkflowRuntime::auto_resume` 的 reason）
    AutoResume { reason: String },
    /// 工作流级 / 任务级超时（`expected_index = None` 为工作流级）
    Timeout { expected_index: Option<usize> },
}

/// 持久化定时器记录。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimerRecord {
    /// 主键：`{fire_at_ms:020}:{instance_id}:{kind}`（同一实例同一到期时刻去重）
    pub id: String,
    pub instance_id: String,
    /// 到期时刻（Unix ms）
    pub fire_at_ms: i64,
    pub kind: TimerKind,
    /// 创建时刻（Unix ms）
    pub created_at_ms: i64,
}

impl TimerRecord {
    pub fn auto_resume(instance_id: &str, reason: &str, fire_at_ms: i64, now_ms: i64) -> Self {
        Self {
            id: format!("{fire_at_ms:020}:{instance_id}:resume:{reason}"),
            instance_id: instance_id.to_string(),
            fire_at_ms,
            kind: TimerKind::AutoResume {
                reason: reason.to_string(),
            },
            created_at_ms: now_ms,
        }
    }

    pub fn timeout(
        instance_id: &str,
        expected_index: Option<usize>,
        fire_at_ms: i64,
        now_ms: i64,
    ) -> Self {
        let idx = expected_index
            .map(|i| i.to_string())
            .unwrap_or_else(|| "wf".to_string());
        Self {
            id: format!("{fire_at_ms:020}:{instance_id}:timeout:{idx}"),
            instance_id: instance_id.to_string(),
            fire_at_ms,
            kind: TimerKind::Timeout { expected_index },
            created_at_ms: now_ms,
        }
    }
}

/// 持久化定时器存储契约。
///
/// 键布局必须让「到期时间可扫描」（`due` 按 `fire_at_ms` 升序取前缀范围），
/// 与实现无关；KV 实现见 `coord-agent` 的 `KvOutboxStore`。
#[async_trait]
pub trait DurableTimerStore: Send + Sync {
    /// 写入/覆盖一个定时器（幂等）。
    async fn schedule(&self, rec: &TimerRecord) -> Result<(), StoreError>;

    /// 到期（`fire_at_ms <= now_ms`）定时器，按到期时间升序，最多 `limit` 条。
    async fn due(&self, now_ms: i64, limit: usize) -> Result<Vec<TimerRecord>, StoreError>;

    /// 移除定时器（触发完成或取消）。
    async fn remove(&self, id: &str) -> Result<(), StoreError>;

    /// 列出全部定时器（证据导出/对账）。
    async fn list_all(&self) -> Result<Vec<TimerRecord>, StoreError>;
}

/// 投递面契约 —— 把一条 outbox 记录交给下游（MQ / Event）。
#[async_trait]
pub trait OutboxSink: Send + Sync {
    /// 投递一条；`Ok(())` 表示下游已收下（允许下游再重复投递）。
    async fn deliver(&self, rec: &OutboxRecord) -> Result<(), String>;
}

/// Relay 轮询/退避策略。
#[derive(Debug, Clone)]
pub struct RelayPolicy {
    /// 单次 tick 最多取多少条
    pub batch: usize,
    /// 单条记录最多投递尝试次数（超出进死信）
    pub max_attempts: u32,
    /// 退避基数
    pub base_backoff_ms: u64,
    /// 退避上限
    pub max_backoff_ms: u64,
}

impl Default for RelayPolicy {
    fn default() -> Self {
        Self {
            batch: 64,
            max_attempts: 8,
            base_backoff_ms: 200,
            max_backoff_ms: 30_000,
        }
    }
}

/// 单次 tick 的结果（证据/指标用）。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RelayReport {
    pub polled: usize,
    pub delivered: usize,
    pub retried: usize,
    pub dead: usize,
}

/// Outbox Relay —— 轮询待投递记录，投递成功后标位，失败指数退避。
///
/// 不丢的边界：投递成功但**标位前**崩溃 ⇒ 重启后重复投递（at-least-once）。
/// 「不重」由消费者按 `event_id` 去重保证，不是 relay 的职责。
pub struct OutboxRelay {
    store: Arc<dyn OutboxStore>,
    sink: Arc<dyn OutboxSink>,
    policy: RelayPolicy,
}

impl OutboxRelay {
    pub fn new(
        store: Arc<dyn OutboxStore>,
        sink: Arc<dyn OutboxSink>,
        policy: RelayPolicy,
    ) -> Self {
        Self {
            store,
            sink,
            policy,
        }
    }

    pub fn policy(&self) -> &RelayPolicy {
        &self.policy
    }

    /// 推进一轮：取待投递 → 逐条投递 → 标位/退避。
    pub async fn tick(&self, now_ms: i64) -> Result<RelayReport, StoreError> {
        let records = self.store.pending(now_ms, self.policy.batch).await?;
        let mut report = RelayReport {
            polled: records.len(),
            ..RelayReport::default()
        };
        for rec in records {
            match self.sink.deliver(&rec).await {
                Ok(()) => {
                    self.store.mark_delivered(&rec.id, now_ms).await?;
                    report.delivered += 1;
                }
                Err(_) => {
                    let attempts = rec.attempts + 1;
                    if attempts >= self.policy.max_attempts {
                        self.store.mark_dead(&rec.id, attempts).await?;
                        report.dead += 1;
                    } else {
                        let delay = backoff_ms(
                            attempts,
                            self.policy.base_backoff_ms,
                            self.policy.max_backoff_ms,
                        );
                        self.store
                            .mark_retry(&rec.id, attempts, now_ms + delay as i64)
                            .await?;
                        report.retried += 1;
                    }
                }
            }
        }
        Ok(report)
    }
}

/// 运行时注入的持久化能力集合。
pub struct DurableSupport {
    pub timers: Arc<dyn DurableTimerStore>,
    pub outbox: Arc<dyn OutboxStore>,
    /// 可用时，生命周期事件走「状态 + outbox 同事务」提交
    pub atomic_commit: Option<Arc<dyn AtomicOutboxCommit>>,
}

impl DurableSupport {
    pub fn new(timers: Arc<dyn DurableTimerStore>, outbox: Arc<dyn OutboxStore>) -> Self {
        Self {
            timers,
            outbox,
            atomic_commit: None,
        }
    }

    pub fn with_atomic_commit(mut self, commit: Arc<dyn AtomicOutboxCommit>) -> Self {
        self.atomic_commit = Some(commit);
        self
    }
}

// ─── 内存实现（coord-core 单测与无外部依赖场景） ───

/// 进程内 outbox 存储（测试/降级用；重启即失，**不**提供持久性）。
#[derive(Debug, Default)]
pub struct MemoryOutboxStore {
    inner: std::sync::Mutex<Vec<OutboxRecord>>,
    seq: std::sync::Mutex<std::collections::HashMap<String, u64>>,
}

impl MemoryOutboxStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> Vec<OutboxRecord> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

#[async_trait]
impl OutboxStore for MemoryOutboxStore {
    async fn append(&self, rec: &OutboxRecord) -> Result<(), StoreError> {
        let mut all = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if all.iter().any(|r| r.id == rec.id) {
            return Ok(());
        }
        let mut rec = rec.clone();
        if rec.sequence == 0 {
            let mut seqs = self.seq.lock().unwrap_or_else(|e| e.into_inner());
            let next = seqs.entry(rec.aggregate_id.clone()).or_insert(0);
            *next += 1;
            rec.sequence = *next;
        }
        all.push(rec);
        Ok(())
    }

    async fn pending(&self, now_ms: i64, limit: usize) -> Result<Vec<OutboxRecord>, StoreError> {
        let all = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut out: Vec<OutboxRecord> = all
            .iter()
            .filter(|r| r.status == OutboxStatus::Pending && r.next_attempt_at_ms <= now_ms)
            .cloned()
            .collect();
        out.sort_by_key(|r| r.next_attempt_at_ms);
        out.truncate(limit);
        Ok(out)
    }

    async fn mark_delivered(&self, id: &str, delivered_at_ms: i64) -> Result<(), StoreError> {
        let mut all = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(r) = all.iter_mut().find(|r| r.id == id) {
            r.status = OutboxStatus::Delivered;
            r.delivered_at_ms = Some(delivered_at_ms);
        }
        Ok(())
    }

    async fn mark_retry(
        &self,
        id: &str,
        attempts: u32,
        next_attempt_at_ms: i64,
    ) -> Result<(), StoreError> {
        let mut all = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(r) = all.iter_mut().find(|r| r.id == id) {
            r.attempts = attempts;
            r.next_attempt_at_ms = next_attempt_at_ms;
        }
        Ok(())
    }

    async fn mark_dead(&self, id: &str, attempts: u32) -> Result<(), StoreError> {
        let mut all = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(r) = all.iter_mut().find(|r| r.id == id) {
            r.status = OutboxStatus::Dead;
            r.attempts = attempts;
        }
        Ok(())
    }

    async fn list_all(&self) -> Result<Vec<OutboxRecord>, StoreError> {
        Ok(self.snapshot())
    }
}

/// 进程内定时器存储（测试/降级用；重启即失，**不**提供持久性）。
#[derive(Debug, Default)]
pub struct MemoryTimerStore {
    inner: std::sync::Mutex<std::collections::BTreeMap<String, TimerRecord>>,
}

impl MemoryTimerStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl DurableTimerStore for MemoryTimerStore {
    async fn schedule(&self, rec: &TimerRecord) -> Result<(), StoreError> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(rec.id.clone(), rec.clone());
        Ok(())
    }

    async fn due(&self, now_ms: i64, limit: usize) -> Result<Vec<TimerRecord>, StoreError> {
        let map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut out: Vec<TimerRecord> = map
            .values()
            .filter(|r| r.fire_at_ms <= now_ms)
            .cloned()
            .collect();
        out.sort_by_key(|r| (r.fire_at_ms, r.id.clone()));
        out.truncate(limit);
        Ok(out)
    }

    async fn remove(&self, id: &str) -> Result<(), StoreError> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(id);
        Ok(())
    }

    async fn list_all(&self) -> Result<Vec<TimerRecord>, StoreError> {
        let map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut out: Vec<TimerRecord> = map.values().cloned().collect();
        out.sort_by_key(|r| (r.fire_at_ms, r.id.clone()));
        Ok(out)
    }
}

/// Unix ms → RFC3339 UTC（`YYYY-MM-DDTHH:MM:SS.mmmZ`）。
///
/// 自己实现而不是引入时间库：`coord-core` 的依赖面受 `deny.toml` 约束，而
/// CloudEvents `time` 只需要一个无时区的 UTC 序列化。
pub fn rfc3339_utc(ms: i64) -> String {
    let ms = ms.max(0);
    let secs = ms / 1000;
    let millis = (ms % 1000) as u32;
    let days = secs / 86_400;
    let rem = secs % 86_400;
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let (y, mo, d) = civil_from_days(days);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}.{millis:03}Z")
}

/// Howard Hinnant 的 `civil_from_days`：把「1970-01-01 起的天数」还原为公历日期。
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn rec(id: &str, aggregate: &str, now: i64) -> OutboxRecord {
        OutboxRecord::lifecycle(
            aggregate,
            "com.china-workflow.spike3.test",
            serde_json::json!({"k": id}),
            now,
            id.to_string(),
        )
    }

    #[test]
    fn external_namespace_mapping_is_single_definition() {
        assert_eq!(
            external_event_type("io.serverlessworkflow.workflow.started.v1"),
            "com.china-workflow.workflow.started.v1"
        );
        // 已经是目标命名空间的类型原样保留（不重复加前缀）
        assert_eq!(
            external_event_type("com.china-workflow.workflow.started.v1"),
            "com.china-workflow.workflow.started.v1"
        );
        let rec = OutboxRecord::lifecycle(
            "i1",
            "io.serverlessworkflow.workflow.started.v1",
            Value::Null,
            0,
            "e".into(),
        );
        assert_eq!(rec.event_type, "com.china-workflow.workflow.started.v1");
        assert_eq!(
            rec.dataschema,
            "https://schemas.coord.dev/events/com.china-workflow.workflow.started.v1.json"
        );
    }

    #[test]
    fn backoff_is_exponential_and_capped() {
        assert_eq!(backoff_ms(0, 200, 30_000), 200);
        assert_eq!(backoff_ms(1, 200, 30_000), 400);
        assert_eq!(backoff_ms(3, 200, 30_000), 1600);
        assert_eq!(backoff_ms(9, 200, 30_000), 30_000);
        // 溢出保护：attempt 很大时不 panic、不返回 0
        assert_eq!(backoff_ms(u32::MAX, 200, 30_000), 30_000);
    }

    #[test]
    fn rfc3339_utc_matches_known_instants() {
        assert_eq!(rfc3339_utc(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(rfc3339_utc(1_700_000_000_000), "2023-11-14T22:13:20.000Z");
        assert_eq!(rfc3339_utc(1_700_000_000_123), "2023-11-14T22:13:20.123Z");
    }

    #[test]
    fn timer_ids_are_unique_per_due_and_kind() {
        let a = TimerRecord::auto_resume("i1", "wait", 1000, 0);
        let b = TimerRecord::auto_resume("i1", "retry", 1000, 0);
        assert_ne!(a.id, b.id);
        let c = TimerRecord::timeout("i1", Some(2), 1000, 0);
        assert_ne!(a.id, c.id);
    }

    #[test]
    fn memory_store_assigns_monotonic_per_aggregate_sequence() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = MemoryOutboxStore::new();
            store.append(&rec("a", "inst-1", 10)).await.unwrap();
            store.append(&rec("b", "inst-1", 20)).await.unwrap();
            store.append(&rec("c", "inst-2", 30)).await.unwrap();
            let all = store.list_all().await.unwrap();
            let seq = |id: &str| all.iter().find(|r| r.id == id).unwrap().sequence;
            assert_eq!(seq("a"), 1);
            assert_eq!(seq("b"), 2);
            assert_eq!(seq("c"), 1, "sequence is per-aggregate, not global");
        });
    }

    #[derive(Default)]
    struct FlakySink {
        calls: AtomicUsize,
        fail_first: usize,
        delivered: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait]
    impl OutboxSink for FlakySink {
        async fn deliver(&self, rec: &OutboxRecord) -> Result<(), String> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            if n < self.fail_first {
                return Err("injected downstream failure".to_string());
            }
            self.delivered
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(rec.id.clone());
            Ok(())
        }
    }

    #[tokio::test]
    async fn relay_delivers_and_retries_with_backoff() {
        let store: Arc<dyn OutboxStore> = Arc::new(MemoryOutboxStore::new());
        store.append(&rec("e1", "inst-1", 0)).await.unwrap();
        let sink = Arc::new(FlakySink {
            calls: AtomicUsize::new(0),
            fail_first: 2,
            delivered: std::sync::Mutex::new(Vec::new()),
        });
        let relay = OutboxRelay::new(
            Arc::clone(&store),
            Arc::clone(&sink) as Arc<dyn OutboxSink>,
            RelayPolicy {
                base_backoff_ms: 100,
                ..RelayPolicy::default()
            },
        );

        let r1 = relay.tick(0).await.unwrap();
        assert_eq!(r1.retried, 1);
        assert_eq!(r1.delivered, 0);
        // 退避未到期（attempts=1 ⇒ 200ms）⇒ 下一 tick 不再取
        assert_eq!(relay.tick(100).await.unwrap().polled, 0);
        // 到期 ⇒ 第二次尝试再次失败（退避 400ms）
        let r2 = relay.tick(200).await.unwrap();
        assert_eq!(r2.retried, 1);
        assert_eq!(relay.tick(500).await.unwrap().polled, 0);
        // 第三次成功（在 600ms 到期后）
        let r3 = relay.tick(600).await.unwrap();
        assert_eq!(r3.delivered, 1);
        // 投递成功后不再被轮询
        assert_eq!(relay.tick(10_000).await.unwrap().polled, 0);

        let all = store.list_all().await.unwrap();
        assert_eq!(all[0].status, OutboxStatus::Delivered);
        assert_eq!(all[0].attempts, 2, "前两次失败已记账");
        assert_eq!(sink.delivered.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn relay_dead_letters_after_max_attempts() {
        struct AlwaysFail;
        #[async_trait]
        impl OutboxSink for AlwaysFail {
            async fn deliver(&self, _rec: &OutboxRecord) -> Result<(), String> {
                Err("always fails".to_string())
            }
        }
        let store: Arc<dyn OutboxStore> = Arc::new(MemoryOutboxStore::new());
        store.append(&rec("e1", "inst-1", 0)).await.unwrap();
        let relay = OutboxRelay::new(
            Arc::clone(&store),
            Arc::new(AlwaysFail),
            RelayPolicy {
                max_attempts: 2,
                base_backoff_ms: 1,
                max_backoff_ms: 1,
                batch: 8,
            },
        );
        assert_eq!(relay.tick(0).await.unwrap().retried, 1);
        assert_eq!(relay.tick(10).await.unwrap().dead, 1);
        let all = store.list_all().await.unwrap();
        assert_eq!(all[0].status, OutboxStatus::Dead);
    }

    #[tokio::test]
    async fn append_is_idempotent_on_same_id() {
        let store: Arc<dyn OutboxStore> = Arc::new(MemoryOutboxStore::new());
        store.append(&rec("dup", "inst-1", 0)).await.unwrap();
        store.append(&rec("dup", "inst-1", 0)).await.unwrap();
        assert_eq!(store.list_all().await.unwrap().len(), 1);
    }
}
