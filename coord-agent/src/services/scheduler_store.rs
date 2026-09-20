// coord-agent: SchedulerStore —— 调度状态共享存储抽象（Memory / Kv）
//
// 目标：调度任务的定义 / 状态 / 认领记录**重启不丢、跨 Agent 可见**（计划书 P0-10 / E9）。
//
// 背景（计划书 §2.3 重核表）：`SchedulerService` 此前用三个
// `Arc<RwLock<HashMap<..>>>`（`tasks` / `claims` / `states`）持有全部状态 ——
// ⇒ 重启即丢全部调度状态；多节点各持一份 ⇒ "多节点唯一调度"不成立。
// 本模块把这三张表合并为**一条记录一条 KV**，并以 **CAS** 实现原子认领。
//
// 设计要点：
// - **单一真相**：`TaskRecord` 同时含任务定义 / 状态 / 认领，三者不再可能互相漂移
//   （此前三张表可各自不同步）。
// - **原子认领**：`SchedulerStore::cas` 是"读到的旧值 → 新值"的**逐字节** CAS
//   （`coord.txn.Compare{target: VALUE}`）。多 Agent 并发认领同一任务时，
//   只有第一个提交者成功，其余 CAS 失败后重读 → 看到 Running → 放弃。
// - **确定性序列化**：CAS 是逐字节比较，而 `HashMap` 迭代序不确定 ⇒
//   序列化前必须经 [`TaskRecordWire`]（`BTreeMap`）归一，否则同一逻辑值两次
//   序列化可能不同，CAS 会**恒失败**。这是本模块最容易被忽略的一处正确性前提。
// - **时间基准**：落盘用**墙钟毫秒**（`Instant` 不可序列化、且不跨进程可比）。
//   TTL 判定一律 "now >= claimed_at_ms + ttl_ms"。
//
// Key 空间：
//   /_scheduler/v1/task/{task_id}  → TaskRecord（JSON，metadata 已排序）

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::proxy::AgentInner;
use crate::services::scheduler::{ScheduleTask, TaskState, TaskType};
use crate::services::workflow_store::prefix_end;

/// 任务键前缀（KV 空间）
pub const SCHEDULER_PREFIX: &[u8] = b"/_scheduler/v1/task/";

/// 当前墙钟毫秒（自 UNIX_EPOCH）
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ──── 记录类型 ────

/// 认领记录（落盘形态）
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimRecord {
    /// 认领者标识
    pub worker_id: String,
    /// 认领墙钟（毫秒，自 UNIX_EPOCH）
    pub claimed_at_ms: u64,
}

impl ClaimRecord {
    /// 是否已过期（`now >= claimed_at_ms + ttl_ms`）
    pub fn is_expired(&self, now_ms: u64, ttl_ms: u64) -> bool {
        now_ms >= self.claimed_at_ms.saturating_add(ttl_ms)
    }
}

/// 任务记录 —— **调度状态的单一真相**
///
/// 任务定义 / 状态 / 认领三者同处一条记录：任一变更都是一次 CAS，
/// 因此不存在"状态说 Running 但认领表为空"这类跨表不一致。
#[derive(Debug, Clone, PartialEq)]
pub struct TaskRecord {
    /// 任务定义
    pub task: ScheduleTask,
    /// 任务状态
    pub state: TaskState,
    /// 当前认领（`None` = 无人认领）
    pub claim: Option<ClaimRecord>,
}

impl TaskRecord {
    /// 新建（Pending，无认领）
    pub fn new(task: ScheduleTask) -> Self {
        Self {
            task,
            state: TaskState::Pending,
            claim: None,
        }
    }

    /// 当前是否可被认领
    ///
    /// 判定与历史内存实现逐条对齐（不得放宽）：
    /// - `Pending` / `Failed` ⇒ 可认领；
    /// - `Running` ⇒ 仅当既有认领**已过期**，或不存在认领记录（不一致态）时可认领；
    /// - `Completed` ⇒ 不可认领（Exactly-Once 的终态）。
    pub fn is_claimable(&self, now_ms: u64, ttl_ms: u64) -> bool {
        match self.state {
            TaskState::Pending | TaskState::Failed => true,
            TaskState::Running => match &self.claim {
                Some(c) => c.is_expired(now_ms, ttl_ms),
                None => true,
            },
            TaskState::Completed => false,
        }
    }
}

// ──── 序列化（确定性）────

/// 落盘线格式：`metadata` 用 `BTreeMap` 归一 ⇒ 同一逻辑值序列化逐字节稳定。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TaskRecordWire {
    task_id: String,
    task_type: TaskType,
    description: String,
    metadata: BTreeMap<String, String>,
    state: TaskState,
    claim: Option<ClaimRecord>,
}

fn canonicalize(record: &TaskRecord) -> TaskRecordWire {
    TaskRecordWire {
        task_id: record.task.task_id.clone(),
        task_type: record.task.task_type.clone(),
        description: record.task.description.clone(),
        metadata: record
            .task
            .metadata
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        state: record.state,
        claim: record.claim.clone(),
    }
}

/// 确定性序列化（**CAS 前提**，见模块头）
pub fn serialize_task(record: &TaskRecord) -> Result<Vec<u8>, SchedulerStoreError> {
    serde_json::to_vec(&canonicalize(record))
        .map_err(|e| SchedulerStoreError::Serialization(e.to_string()))
}

/// 反序列化
pub fn deserialize_task(bytes: &[u8]) -> Result<TaskRecord, SchedulerStoreError> {
    let wire: TaskRecordWire = serde_json::from_slice(bytes)
        .map_err(|e| SchedulerStoreError::Serialization(e.to_string()))?;
    Ok(TaskRecord {
        task: ScheduleTask {
            task_id: wire.task_id,
            task_type: wire.task_type,
            description: wire.description,
            metadata: wire.metadata.into_iter().collect::<HashMap<_, _>>(),
        },
        state: wire.state,
        claim: wire.claim,
    })
}

/// `/_scheduler/v1/task/{task_id}`
pub fn task_key(task_id: &str) -> Vec<u8> {
    let mut k = SCHEDULER_PREFIX.to_vec();
    k.extend_from_slice(task_id.as_bytes());
    k
}

// ──── SchedulerStoreError ────

/// 存储层错误
#[derive(Debug)]
pub enum SchedulerStoreError {
    /// 底层 KV 错误（连接 / 重定向 / 超时）
    Kv(String),
    /// 序列化 / 反序列化错误
    Serialization(String),
}

impl std::fmt::Display for SchedulerStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Kv(msg) => write!(f, "kv error: {msg}"),
            Self::Serialization(msg) => write!(f, "serialization error: {msg}"),
        }
    }
}

impl std::error::Error for SchedulerStoreError {}

// ──── SchedulerStore trait ────

/// 调度状态存储抽象
///
/// 生产实现 [`KvSchedulerStore`]（coord-server 共享 KV：重启不丢 + 多 Agent 共享 +
/// CAS 原子认领）；开发 / 单测实现 [`MemorySchedulerStore`]。
#[async_trait]
pub trait SchedulerStore: Send + Sync {
    /// 读取单条记录
    async fn get(&self, task_id: &str) -> Result<Option<TaskRecord>, SchedulerStoreError>;

    /// 列出全部记录
    async fn list(&self) -> Result<Vec<TaskRecord>, SchedulerStoreError>;

    /// **仅当不存在时**创建；返回 `false` 表示已存在（未写入）
    async fn create(&self, task_id: &str, record: &TaskRecord) -> Result<bool, SchedulerStoreError>;

    /// 删除（不存在不算错误）
    async fn delete(&self, task_id: &str) -> Result<(), SchedulerStoreError>;

    /// 原子 CAS：`expected` = 读到的旧值（`None` = 期望不存在）。
    ///
    /// 返回 `true` 表示写入成功；`false` 表示期间被他人改动（调用方应重读重试）。
    async fn cas(
        &self,
        task_id: &str,
        expected: Option<&TaskRecord>,
        new: &TaskRecord,
    ) -> Result<bool, SchedulerStoreError>;
}

// ──── MemorySchedulerStore（开发 / 单测）────

/// 内存实现：语义与 [`KvSchedulerStore`] 完全一致（含 CAS 与确定性序列化校验），
/// 用于无 server 的骨架模式与单测。
#[derive(Default)]
pub struct MemorySchedulerStore {
    entries: RwLock<HashMap<String, TaskRecord>>,
}

impl MemorySchedulerStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// 当前条目数（测试 / 可观测性用）
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl SchedulerStore for MemorySchedulerStore {
    async fn get(&self, task_id: &str) -> Result<Option<TaskRecord>, SchedulerStoreError> {
        Ok(self.entries.read().get(task_id).cloned())
    }

    async fn list(&self) -> Result<Vec<TaskRecord>, SchedulerStoreError> {
        Ok(self.entries.read().values().cloned().collect())
    }

    async fn create(
        &self,
        task_id: &str,
        record: &TaskRecord,
    ) -> Result<bool, SchedulerStoreError> {
        let mut entries = self.entries.write();
        if entries.contains_key(task_id) {
            return Ok(false);
        }
        entries.insert(task_id.to_string(), record.clone());
        Ok(true)
    }

    async fn delete(&self, task_id: &str) -> Result<(), SchedulerStoreError> {
        self.entries.write().remove(task_id);
        Ok(())
    }

    async fn cas(
        &self,
        task_id: &str,
        expected: Option<&TaskRecord>,
        new: &TaskRecord,
    ) -> Result<bool, SchedulerStoreError> {
        let mut entries = self.entries.write();
        match (expected, entries.get(task_id)) {
            // 期望不存在：仅当确实不存在时创建
            (None, None) => {
                entries.insert(task_id.to_string(), new.clone());
                Ok(true)
            }
            (None, Some(_)) => Ok(false),
            // 期望存在：逐条比对（等价于 KV 的 value-CAS）
            (Some(exp), Some(cur)) => {
                if cur != exp {
                    return Ok(false);
                }
                entries.insert(task_id.to_string(), new.clone());
                Ok(true)
            }
            (Some(_), None) => Ok(false),
        }
    }
}

// ──── KvSchedulerStore（生产：coord-server 共享 KV）────

/// 生产实现：经 `AgentInner.client` 的 KV / Txn 能力访问 coord-server 共享存储。
///
/// - 值经 coord-server redb 持久化 + Raft 共识落库（agent 重启后仍可恢复）；
/// - 多 Agent 共享同一键空间 ⇒ 认领具备**跨节点唯一性**；
/// - `create` 用 `Compare{target: VERSION, version: 0}`（键不存在）保证"注册不覆盖"；
/// - `cas` 用 `Compare{target: VALUE}` 逐字节比较（依赖 [`serialize_task`] 的确定性）。
pub struct KvSchedulerStore {
    inner: Arc<AgentInner>,
}

impl KvSchedulerStore {
    pub fn new(inner: Arc<AgentInner>) -> Self {
        Self { inner }
    }
}

/// 键不存在比较（etcd 语义：`VERSION == 0` ⇒ 键不存在）
fn compare_key_absent(key: &[u8]) -> coord_proto::txn::Compare {
    use coord_proto::txn::compare::{CompareResult, Target, TargetValue};
    coord_proto::txn::Compare {
        result: CompareResult::Equal as i32,
        target: Target::Version as i32,
        key: key.to_vec(),
        target_value: Some(TargetValue::Version(0)),
    }
}

/// PUT 操作
fn put_op(key: &[u8], value: Vec<u8>) -> coord_proto::txn::RequestOp {
    use coord_proto::kv::PutRequest;
    use coord_proto::txn::request_op::Op;
    coord_proto::txn::RequestOp {
        op: Some(Op::RequestPut(PutRequest {
            key: key.to_vec(),
            value,
            lease_id: 0,
            prev_kv: false,
            request_id: Vec::new(),
        })),
    }
}

#[async_trait]
impl SchedulerStore for KvSchedulerStore {
    async fn get(&self, task_id: &str) -> Result<Option<TaskRecord>, SchedulerStoreError> {
        let key = task_key(task_id);
        let pairs = self
            .inner
            .client
            .kv()
            .range(&key, &[], 1, 0)
            .await
            .map_err(|e| SchedulerStoreError::Kv(e.to_string()))?;
        match pairs.first() {
            Some((_k, v)) => Ok(Some(deserialize_task(v)?)),
            None => Ok(None),
        }
    }

    async fn list(&self) -> Result<Vec<TaskRecord>, SchedulerStoreError> {
        let end = prefix_end(SCHEDULER_PREFIX);
        // limit = 0：全量返回。调度任务数量受运维约束，量级可控。
        let pairs = self
            .inner
            .client
            .kv()
            .range(SCHEDULER_PREFIX, &end, 0, 0)
            .await
            .map_err(|e| SchedulerStoreError::Kv(e.to_string()))?;

        let mut out = Vec::with_capacity(pairs.len());
        for (_k, v) in pairs {
            out.push(deserialize_task(&v)?);
        }
        Ok(out)
    }

    async fn create(
        &self,
        task_id: &str,
        record: &TaskRecord,
    ) -> Result<bool, SchedulerStoreError> {
        let key = task_key(task_id);
        let value = serialize_task(record)?;
        let resp = self
            .inner
            .client
            .txn()
            .txn(vec![compare_key_absent(&key)], vec![put_op(&key, value)], vec![])
            .await
            .map_err(|e| SchedulerStoreError::Kv(e.to_string()))?;
        Ok(resp.succeeded)
    }

    async fn delete(&self, task_id: &str) -> Result<(), SchedulerStoreError> {
        self.inner
            .client
            .kv()
            .delete(&task_key(task_id))
            .await
            .map_err(|e| SchedulerStoreError::Kv(e.to_string()))?;
        Ok(())
    }

    async fn cas(
        &self,
        task_id: &str,
        expected: Option<&TaskRecord>,
        new: &TaskRecord,
    ) -> Result<bool, SchedulerStoreError> {
        let key = task_key(task_id);
        let new_value = serialize_task(new)?;

        match expected {
            // 期望不存在 ⇒ 与 create 同口径（键不存在条件）
            None => {
                let resp = self
                    .inner
                    .client
                    .txn()
                    .txn(
                        vec![compare_key_absent(&key)],
                        vec![put_op(&key, new_value)],
                        vec![],
                    )
                    .await
                    .map_err(|e| SchedulerStoreError::Kv(e.to_string()))?;
                Ok(resp.succeeded)
            }
            // 期望存在 ⇒ value-CAS（逐字节）
            Some(exp) => {
                let old_value = serialize_task(exp)?;
                self.inner
                    .client
                    .txn()
                    .cas(&key, &old_value, &new_value)
                    .await
                    .map_err(|e| SchedulerStoreError::Kv(e.to_string()))
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════
// 测试
// ═══════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    fn task(id: &str) -> ScheduleTask {
        ScheduleTask {
            task_id: id.to_string(),
            task_type: TaskType::Cron {
                expression: "*/5 * * * *".into(),
            },
            description: "test".into(),
            metadata: HashMap::new(),
        }
    }

    fn task_with_meta(id: &str, pairs: &[(&str, &str)]) -> ScheduleTask {
        ScheduleTask {
            task_id: id.to_string(),
            task_type: TaskType::Once,
            description: "meta".into(),
            metadata: pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    #[test]
    fn test_canonical_serialization_is_deterministic_regardless_of_hashmap_order() {
        // 造两个 metadata 插入序不同、逻辑值相同的任务。
        // HashMap 迭代序随插入序/随机种子变化；若不归一，CAS 会恒失败。
        let a = task_with_meta(
            "j1",
            &[("z", "1"), ("a", "2"), ("m", "3"), ("b", "4"), ("q", "5")],
        );
        let b = task_with_meta(
            "j1",
            &[("q", "5"), ("b", "4"), ("m", "3"), ("a", "2"), ("z", "1")],
        );

        let rec_a = TaskRecord::new(a);
        let rec_b = TaskRecord::new(b);
        assert_eq!(
            serialize_task(&rec_a).unwrap(),
            serialize_task(&rec_b).unwrap(),
            "同一逻辑记录必须序列化逐字节相同（CAS 前提）"
        );
        assert_eq!(deserialize_task(&serialize_task(&rec_a).unwrap()).unwrap(), rec_a);
    }

    #[test]
    fn test_task_key_layout() {
        assert_eq!(task_key("job-1"), b"/_scheduler/v1/task/job-1".to_vec());
        assert!(task_key("job-1").starts_with(SCHEDULER_PREFIX));
    }

    #[test]
    fn test_claim_expiry_boundary() {
        let c = ClaimRecord {
            worker_id: "w".into(),
            claimed_at_ms: 1000,
        };
        assert!(!c.is_expired(1099, 100));
        assert!(c.is_expired(1100, 100), "边界含等号：now >= claimed_at + ttl");
        // 极端输入：饱和加法封顶 u64::MAX（不 panic），故 now 达到封顶即判过期。
        // 这是**安全方向**：宁可让认领被释放，也不因整数回绕而永久卡死。
        assert!(c.is_expired(u64::MAX, u64::MAX), "饱和封顶后应判过期而非回绕");
    }

    #[test]
    fn test_is_claimable_matrix() {
        let t = || TaskRecord::new(task("j"));
        let ttl = 100u64;

        // Pending / Failed ⇒ 可认领
        assert!(t().is_claimable(1000, ttl));
        let mut failed = t();
        failed.state = TaskState::Failed;
        assert!(failed.is_claimable(1000, ttl));

        // Completed ⇒ 永不可认领
        let mut done = t();
        done.state = TaskState::Completed;
        assert!(!done.is_claimable(1000, ttl));

        // Running + 未过期 ⇒ 不可认领
        let mut running = t();
        running.state = TaskState::Running;
        running.claim = Some(ClaimRecord {
            worker_id: "w1".into(),
            claimed_at_ms: 1000,
        });
        assert!(!running.is_claimable(1050, ttl));
        // Running + 已过期 ⇒ 可认领
        assert!(running.is_claimable(1100, ttl));

        // Running 却无认领记录（不一致态）⇒ 允许接管，避免任务永久卡死
        let mut orphan = t();
        orphan.state = TaskState::Running;
        orphan.claim = None;
        assert!(orphan.is_claimable(1000, ttl));
    }

    #[tokio::test]
    async fn test_memory_create_is_insert_if_absent() {
        let store = MemorySchedulerStore::new();
        let rec = TaskRecord::new(task("j"));

        assert!(store.create("j", &rec).await.unwrap());
        assert!(!store.create("j", &rec).await.unwrap(), "重复创建必须返回 false");
        assert_eq!(store.len(), 1);
    }

    #[tokio::test]
    async fn test_memory_get_list_delete_roundtrip() {
        let store = MemorySchedulerStore::new();
        assert!(store.get("missing").await.unwrap().is_none());

        store.create("a", &TaskRecord::new(task("a"))).await.unwrap();
        store.create("b", &TaskRecord::new(task("b"))).await.unwrap();
        assert_eq!(store.list().await.unwrap().len(), 2);

        let got = store.get("a").await.unwrap().expect("record");
        assert_eq!(got.task.task_id, "a");
        assert_eq!(got.state, TaskState::Pending);

        store.delete("a").await.unwrap();
        assert!(store.get("a").await.unwrap().is_none());
        // 幂等：重复删除不报错
        store.delete("a").await.unwrap();
        assert_eq!(store.list().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_memory_cas_rejects_stale_expected() {
        let store = MemorySchedulerStore::new();
        let v0 = TaskRecord::new(task("j"));
        store.create("j", &v0).await.unwrap();

        // 从 v0 → v1
        let mut v1 = v0.clone();
        v1.state = TaskState::Running;
        assert!(store.cas("j", Some(&v0), &v1).await.unwrap());

        // 再用已过期的 v0 作 expected ⇒ 必须失败（这正是"第二个认领者"的路径）
        let mut v2 = v0.clone();
        v2.state = TaskState::Failed;
        assert!(!store.cas("j", Some(&v0), &v2).await.unwrap());
        // 且未被写入
        assert_eq!(store.get("j").await.unwrap().unwrap().state, TaskState::Running);
    }

    #[tokio::test]
    async fn test_memory_cas_none_expected_creates_only_when_absent() {
        let store = MemorySchedulerStore::new();
        let rec = TaskRecord::new(task("j"));
        assert!(store.cas("j", None, &rec).await.unwrap());
        assert!(!store.cas("j", None, &rec).await.unwrap());
    }

    #[tokio::test]
    async fn test_memory_cas_rejects_when_key_missing_but_expected_present() {
        let store = MemorySchedulerStore::new();
        let rec = TaskRecord::new(task("ghost"));
        assert!(!store.cas("ghost", Some(&rec), &rec).await.unwrap());
    }

    /// CAS 竞争：N 个并发"认领者"从同一旧值出发，只能有一个成功。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_memory_cas_only_one_concurrent_writer_wins() {
        let store = Arc::new(MemorySchedulerStore::new());
        let v0 = TaskRecord::new(task("hot"));
        store.create("hot", &v0).await.unwrap();

        let mut handles = Vec::new();
        for i in 0..16 {
            let store = Arc::clone(&store);
            let expected = v0.clone();
            handles.push(tokio::spawn(async move {
                let mut next = expected.clone();
                next.state = TaskState::Running;
                next.claim = Some(ClaimRecord {
                    worker_id: format!("w{i}"),
                    claimed_at_ms: now_ms(),
                });
                store.cas("hot", Some(&expected), &next).await.unwrap()
            }));
        }

        let mut wins = 0;
        for h in handles {
            if h.await.unwrap() {
                wins += 1;
            }
        }
        assert_eq!(wins, 1, "并发 CAS 必须恰好一个成功");
    }
}
