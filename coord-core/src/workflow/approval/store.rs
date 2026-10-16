//! v2 审批存储端口与内存实现。
//!
//! 端口只有带 CAS 的提交一条写路径（ADR-0013 D3）：没有「无 CAS」形态，冲突一律
//! `Ok(false)` 且零写入。生产实现（KV Txn）在 `coord-agent`；本模块的
//! 内存实现供正确性测试使用。

use std::collections::{BTreeMap, HashSet};
use std::ops::Bound;
use std::sync::Mutex;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::workflow::durable::{OutboxRecord, OutboxStatus};
use crate::workflow::ports::StoreError;

use super::engine::Engine;

/// `commit` 的期望修订号谓词。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedRev {
    /// 要求键不存在（创建实例；恰好一次语义）。
    CreateOnly,
    /// 要求当前修订号恰为 `n`，`n >= 1`（`load` 返回的 `mod_rev`；首次写入后的修订号是 1）。
    ///
    /// 首次写入用 [`ExpectedRev::CreateOnly`]；`Exact(0)` 是保留值（缺键在 KV 里读作
    /// `mod_revision == 0`，会让「不存在」被误判成匹配），`commit` 一律拒绝。
    Exact(i64),
}

/// `scan` 的默认页大小：`limit == 0` 时取该值。
pub const APPROVAL_SCAN_DEFAULT_LIMIT: usize = 50;

/// `scan` 的硬上限：任何 `limit` 都不会一次返回超过该条数。
pub const APPROVAL_SCAN_MAX_LIMIT: usize = 200;

/// 把调用方给的 `limit` 钳成实际页大小（`0` ⇒ [`APPROVAL_SCAN_DEFAULT_LIMIT`，
/// 上限 [`APPROVAL_SCAN_MAX_LIMIT`]）。
///
/// 两个实现共用：分页上限只此一处定义，避免内存与 KV 行为分叉。
pub fn clamp_scan_limit(limit: usize) -> usize {
    match limit {
        0 => APPROVAL_SCAN_DEFAULT_LIMIT,
        n => n.min(APPROVAL_SCAN_MAX_LIMIT),
    }
}

/// 一条已提交命令的幂等记录。
///
/// 与产生它的状态、事件在同一临界区/Txn 写入（见
/// [`ApprovalStore::commit_with_idem`]），因此「响应落了库」与「命令已生效」等价。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IdemRecord {
    /// 命令幂等键（同一实例内唯一定位一次命令调用）。
    pub key: String,
    /// 首次成功提交时返回给调用方的响应；重试命中即原样返回。
    pub response: Value,
}

/// [`ApprovalStore::scan`] 返回的实例条目。
#[derive(Debug, Clone, PartialEq)]
pub struct ApprovalInstanceEntry {
    /// 实例 id（`/_workflow/v4/instances/{id}` 的 `{id}` 段）。
    pub id: String,
    /// 已提交的实例状态。
    pub engine: Engine,
    /// 存储修订号（下一次 `Exact(rev)` 提交的期望值）。
    pub mod_rev: i64,
}

/// v2 审批实例存储端口。
///
/// 写路径只有 [`ApprovalStore::commit_with_idem`]（及其 `idem = None` 形态
/// [`ApprovalStore::commit`]），且必须带 [`ExpectedRev`]：状态与一批事件在一次提交里
/// 同生共死，冲突时零写入返回 `Ok(false)`（调用方重读后重试，禁止盲写覆盖）。
#[async_trait]
pub trait ApprovalStore: Send + Sync {
    /// 线性一致地读取实例状态与其修订号；键不存在返回 `Ok(None)`。
    async fn load(&self, instance_id: &str) -> Result<Option<(Engine, i64)>, StoreError>;

    /// 读命令幂等记录（`/_workflow/v4/idempotency/{instance}/{key}`）；键不存在返回
    /// `Ok(None)`。
    ///
    /// 命中即「该命令已成功提交」：调用方直接返回 `response`，不再执行命令。
    async fn load_idem(&self, instance_id: &str, key: &str) -> Result<Option<Value>, StoreError>;

    /// CAS 提交：状态、本批事件与可选幂等记录同一次写入。
    ///
    /// `expected` 与当前修订号匹配 ⇒ 存储 `engine`、追加 `events`（存储侧按批次顺序
    /// 分配 per-instance 单调 `sequence`）、修订号 +1，返回 `Ok(true)`；不匹配 ⇒ 零写入
    /// 返回 `Ok(false)`；调用方构造记录时填的 `sequence` 由存储侧覆盖。
    ///
    /// 重放/幂等重复提交（状态逐字相同且本批事件全部已在库）也返回 `Ok(true)`，但零写入、
    /// 不推进修订号。
    ///
    /// `idem` 为 `Some` 时，幂等记录与状态、事件在同一临界区/Txn 生效：只有提交成功它
    /// 才存在，失败时 `load_idem` 读不到——重试方因此不会拿到从未落库的响应。同一
    /// `key` 只有首次成功提交能写（失败/重放都不覆盖已缓存的响应）。
    ///
    /// `Exact(0)` 是保留值：`ensure_expected_rev_allowed` 在写路径前 fail-closed 拒绝。
    ///
    /// `sequence` 的分配基准由实现决定：内存实现取已存事件的最大 `sequence` + 1；KV 实现
    /// 取持久化的 outbox 序号计数器 `/_workflow/v3/outbox/seq/{aggregate}`（见 `coord-agent`
    /// 的 `KvApprovalStore`），该计数器不随事件日志剪枝而回退，序号因此不会回退或重叠。
    async fn commit_with_idem(
        &self,
        engine: &Engine,
        events: &[OutboxRecord],
        expected: ExpectedRev,
        idem: Option<&IdemRecord>,
    ) -> Result<bool, StoreError>;

    /// 带 CAS 的提交（不带幂等记录）：取
    /// [`ApprovalStore::commit_with_idem`] 的 `idem = None` 形态。
    async fn commit(
        &self,
        engine: &Engine,
        events: &[OutboxRecord],
        expected: ExpectedRev,
    ) -> Result<bool, StoreError> {
        self.commit_with_idem(engine, events, expected, None).await
    }

    /// 按实例 id 字典序分页扫描已提交实例；`after` 独占续读，`limit` 经
    /// [`clamp_scan_limit`] 钳制。
    ///
    /// 只扫 v4 实例前缀：v1 实例扫描面（`/_workflow/v3/instances/`）与 v4 内的其他
    /// 键（如幂等记录）都不在本方法的结果里。
    async fn scan(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ApprovalInstanceEntry>, StoreError>;
}

/// 事件聚合根与实例不一致时的 fail-closed 拒绝消息。
///
/// `StoreError` 分类里没有 invalid-argument 变体，用 `IoError` 承载这条存储侧输入
/// 校验失败；消息逐字稳定，供 KV 实现（`coord-agent`）复用同一锚点。
pub const APPROVAL_COMMIT_AGGREGATE_MISMATCH: &str = "approval commit: event aggregate_id mismatch";

/// 同一批事件里出现重复 `id` 时的 fail-closed 拒绝消息。
///
/// 事件按 `id` 写 `ids/{id}` 索引；重复 id 会让后者覆盖前者，一条记录变得无法按 id 找回，
/// 而重放判定仍报告「批次在库」。消息逐字稳定，供 KV 实现（`coord-agent`）复用同一锚点。
pub const APPROVAL_COMMIT_DUPLICATE_EVENT_ID: &str = "approval commit: duplicate event id in batch";

/// `Exact(0)` 被保留时的 fail-closed 拒绝消息。
///
/// 缺键的 `mod_revision` 在不同后端语义不同（KV 读作 0，内存实现从不匹配），`Exact(0)`
/// 会让「键不存在」被误判成匹配；首次写入必须用 `CreateOnly`。消息逐字稳定，供 KV 实现
/// （`coord-agent`）复用同一锚点。
pub const APPROVAL_COMMIT_EXACT_ZERO_RESERVED: &str =
    "approval commit: Exact(0) is reserved; use CreateOnly for first writes";

/// `commit` 前置校验：`Exact(0)` 是保留值，必须拒绝。
///
/// 内存与 KV 两个实现的第一条前置校验，保证「缺键 ⇒ 匹配」的分歧在写路径前就被拦截。
pub fn ensure_expected_rev_allowed(expected: ExpectedRev) -> Result<(), StoreError> {
    match expected {
        ExpectedRev::Exact(0) => Err(StoreError::IoError(
            APPROVAL_COMMIT_EXACT_ZERO_RESERVED.to_string(),
        )),
        _ => Ok(()),
    }
}

/// `commit` 前置校验：一批事件的 `aggregate_id` 必须全部等于实例 id，且 `id` 互不重复。
///
/// 事件写进 `records/{aggregate}/` 后 relay 按聚合分区投递；聚合根不一致意味着调用方
/// 把别家事件挂到本实例上，必须在写前拒绝。批内重复 id 会让一条记录被后来的覆盖、
/// 无法按 id 找回，同样必须在写前拒绝。
pub fn ensure_event_aggregates_match(
    engine: &Engine,
    events: &[OutboxRecord],
) -> Result<(), StoreError> {
    let mut seen = HashSet::new();
    for rec in events {
        if rec.aggregate_id != engine.instance.id {
            return Err(StoreError::IoError(
                APPROVAL_COMMIT_AGGREGATE_MISMATCH.to_string(),
            ));
        }
        if !seen.insert(rec.id.as_str()) {
            return Err(StoreError::IoError(
                APPROVAL_COMMIT_DUPLICATE_EVENT_ID.to_string(),
            ));
        }
    }
    Ok(())
}

/// 内存实现里的一条实例记录。
#[derive(Debug, Clone)]
struct Entry {
    engine: Engine,
    mod_rev: i64,
    events: Vec<OutboxRecord>,
    /// 幂等记录（命令键 ⇒ 首次成功提交的响应），与状态同锁。
    idem: BTreeMap<String, Value>,
}

/// 进程内审批存储（正确性测试用）。
///
/// **原子性**：状态、修订号、事件批次与幂等记录存在同一个 `Mutex` 下的同一条记录里，
/// [`ApprovalStore::commit_with_idem`] 在一次临界区内完成更新 ⇒ 成功即全部生效，
/// `Ok(false)` 即它们与提交前逐字相同。这与 `durable.rs` 的 `MemoryAtomicCommit`
/// （先写状态、再逐条写事件，**非原子**，文档明确禁止生产使用）不同；生产路径必须用
/// `coord-agent` 的 KV Txn 实现。
#[derive(Debug, Default)]
pub struct MemoryApprovalStore {
    inner: Mutex<BTreeMap<String, Entry>>,
}

impl MemoryApprovalStore {
    /// 新建空存储。
    pub fn new() -> Self {
        Self::default()
    }

    /// 某实例已存的事件批次（对账/测试用），按分配顺序返回。
    pub async fn events(&self, instance_id: &str) -> Vec<OutboxRecord> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(instance_id)
            .map(|entry| entry.events.clone())
            .unwrap_or_default()
    }

    /// 某实例当前修订号；键不存在返回 `None`。
    pub async fn mod_rev(&self, instance_id: &str) -> Option<i64> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(instance_id)
            .map(|entry| entry.mod_rev)
    }
}

#[async_trait]
impl ApprovalStore for MemoryApprovalStore {
    async fn load(&self, instance_id: &str) -> Result<Option<(Engine, i64)>, StoreError> {
        let map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(map
            .get(instance_id)
            .map(|entry| (entry.engine.clone(), entry.mod_rev)))
    }

    async fn load_idem(&self, instance_id: &str, key: &str) -> Result<Option<Value>, StoreError> {
        let map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(map
            .get(instance_id)
            .and_then(|entry| entry.idem.get(key).cloned()))
    }

    async fn commit_with_idem(
        &self,
        engine: &Engine,
        events: &[OutboxRecord],
        expected: ExpectedRev,
        idem: Option<&IdemRecord>,
    ) -> Result<bool, StoreError> {
        ensure_expected_rev_allowed(expected)?;
        ensure_event_aggregates_match(engine, events)?;
        let mut map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let id = engine.instance.id.as_str();
        let matched = match expected {
            ExpectedRev::CreateOnly => !map.contains_key(id),
            ExpectedRev::Exact(n) => map.get(id).map(|e| e.mod_rev == n).unwrap_or(false),
        };
        if !matched {
            return Ok(false);
        }

        let start = map
            .get(id)
            .and_then(|entry| entry.events.iter().map(|r| r.sequence).max())
            .unwrap_or(0)
            + 1;
        let batch: Vec<OutboxRecord> = events
            .iter()
            .enumerate()
            .map(|(i, rec)| {
                let mut stored = rec.clone();
                stored.sequence = start + i as u64;
                stored.status = OutboxStatus::Pending;
                stored
            })
            .collect();
        match map.get_mut(id) {
            Some(entry) => {
                entry.engine = engine.clone();
                entry.events.extend(batch);
                entry.mod_rev += 1;
                if let Some(rec) = idem {
                    // 首次成功提交拥有该 key：KV 实现的 `VERSION==0` 谓词保证重复提交
                    // 写不进该键，重试方以 `load_idem` 命中的响应为准。
                    entry
                        .idem
                        .entry(rec.key.clone())
                        .or_insert(rec.response.clone());
                }
            }
            None => {
                let mut idem_map = BTreeMap::new();
                if let Some(rec) = idem {
                    idem_map.insert(rec.key.clone(), rec.response.clone());
                }
                map.insert(
                    id.to_string(),
                    Entry {
                        engine: engine.clone(),
                        mod_rev: 1,
                        events: batch,
                        idem: idem_map,
                    },
                );
            }
        }
        Ok(true)
    }

    async fn scan(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ApprovalInstanceEntry>, StoreError> {
        let map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        // `after` 独占：起点是同名实例键的严格后继。
        let start: Bound<String> = match after {
            Some(id) => Bound::Excluded(id.to_string()),
            None => Bound::Unbounded,
        };
        Ok(map
            .range((start, Bound::Unbounded))
            .take(clamp_scan_limit(limit))
            .map(|(id, entry)| ApprovalInstanceEntry {
                id: id.clone(),
                engine: entry.engine.clone(),
                mod_rev: entry.mod_rev,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::approval::engine::{Engine, StartSpec};
    use crate::workflow::approval::events::event_record;
    use crate::workflow::approval::model::{
        ApprovalPolicy, JoinPolicy, RejectPolicy, Scope, SeatSpec,
    };
    use crate::workflow::durable::OutboxStatus;
    use serde_json::json;

    const INSTANCE_ID: &str = "inst-t4";

    fn idem(key: &str, response: Value) -> IdemRecord {
        IdemRecord {
            key: key.into(),
            response,
        }
    }

    /// 一页扫描结果的实例 id（按返回顺序）。
    async fn scan_ids(
        store: &MemoryApprovalStore,
        after: Option<&str>,
        limit: usize,
    ) -> Vec<String> {
        store
            .scan(after, limit)
            .await
            .expect("scan")
            .into_iter()
            .map(|entry| entry.id)
            .collect()
    }

    fn start(id: &str) -> Engine {
        Engine::start(
            StartSpec {
                id: id.into(),
                definition_ns: "eis".into(),
                definition_name: "leave-approval".into(),
                definition_version: "1.0.0".into(),
                definition_hash: "sha256:t4".into(),
                initiator: "zhangsan".into(),
                context: Scope::new(),
            },
            1_000,
        )
        .expect("start instance")
    }

    fn evt(id: &str) -> OutboxRecord {
        event_record(
            INSTANCE_ID,
            "com.china-workflow.instance.completed.v1",
            &json!({ "instance_id": INSTANCE_ID }),
            1_000,
            id.into(),
        )
        .expect("known event type")
    }

    #[tokio::test]
    async fn memory_store_create_only_is_exactly_once() {
        let store = MemoryApprovalStore::new();
        let first = start(INSTANCE_ID);
        assert!(store
            .commit(&first, &[], ExpectedRev::CreateOnly)
            .await
            .unwrap());
        assert_eq!(store.mod_rev(INSTANCE_ID).await, Some(1));

        let mut second = start(INSTANCE_ID);
        second.instance.initiator = "lisi".into();
        assert!(!store
            .commit(&second, &[], ExpectedRev::CreateOnly)
            .await
            .unwrap());

        let (loaded, rev) = store.load(INSTANCE_ID).await.unwrap().expect("instance");
        assert_eq!(rev, 1);
        assert_eq!(loaded, first);
    }

    #[tokio::test]
    async fn memory_store_exact_cas_rejects_stale() {
        let store = MemoryApprovalStore::new();
        let first = start(INSTANCE_ID);
        assert!(store
            .commit(&first, &[], ExpectedRev::CreateOnly)
            .await
            .unwrap());
        // 首次成功提交后修订号是 1；`Exact(0)` 是保留值（见
        // `memory_store_rejects_reserved_exact_zero`），不再作为 CAS 输入。
        assert_eq!(store.mod_rev(INSTANCE_ID).await, Some(1));

        let mut second = first.clone();
        second.instance.initiator = "lisi".into();
        assert!(store
            .commit(&second, &[], ExpectedRev::Exact(1))
            .await
            .unwrap());
        assert_eq!(store.mod_rev(INSTANCE_ID).await, Some(2));
        assert!(!store
            .commit(&second, &[], ExpectedRev::Exact(1))
            .await
            .unwrap());
        assert!(store
            .commit(&second, &[], ExpectedRev::Exact(2))
            .await
            .unwrap());
        assert_eq!(store.mod_rev(INSTANCE_ID).await, Some(3));
    }

    #[tokio::test]
    async fn memory_store_rejects_reserved_exact_zero() {
        // `Exact(0)` 是保留值：即便键不存在也必须拒绝，不能与 `CreateOnly` 混同。
        // KV 实现把缺键读作 `mod_revision == 0`，若不拦，`Exact(0)` 会创建实例；内存
        // 实现则从不匹配——两个实现共用 `ensure_expected_rev_allowed` 这一锚点消歧。
        let store = MemoryApprovalStore::new();
        let engine = start(INSTANCE_ID);
        let err = store
            .commit(&engine, &[evt("evt-1")], ExpectedRev::Exact(0))
            .await
            .unwrap_err();
        assert_eq!(
            err,
            StoreError::IoError(APPROVAL_COMMIT_EXACT_ZERO_RESERVED.into())
        );
        assert!(store.load(INSTANCE_ID).await.unwrap().is_none());
        assert_eq!(store.mod_rev(INSTANCE_ID).await, None);
        assert!(store.events(INSTANCE_ID).await.is_empty());
    }

    #[tokio::test]
    async fn memory_store_cas_failure_leaves_no_state_and_no_events() {
        let store = MemoryApprovalStore::new();
        let first = start(INSTANCE_ID);
        assert!(store
            .commit(
                &first,
                &[evt("evt-1"), evt("evt-2")],
                ExpectedRev::CreateOnly
            )
            .await
            .unwrap());

        let before_state = store.load(INSTANCE_ID).await.unwrap().expect("instance");
        let before_events = store.events(INSTANCE_ID).await;
        let before_rev = store.mod_rev(INSTANCE_ID).await;
        assert_eq!(before_events.len(), 2);

        let mut intruder = first.clone();
        intruder.instance.initiator = "lisi".into();
        assert!(!store
            .commit(&intruder, &[evt("evt-3")], ExpectedRev::Exact(99))
            .await
            .unwrap());
        assert!(!store
            .commit(&intruder, &[evt("evt-3")], ExpectedRev::CreateOnly)
            .await
            .unwrap());

        assert_eq!(
            store.load(INSTANCE_ID).await.unwrap().unwrap(),
            before_state
        );
        let after_events = store.events(INSTANCE_ID).await;
        assert_eq!(after_events, before_events);
        assert!(after_events.iter().all(|r| r.id != "evt-3"));
        assert_eq!(store.mod_rev(INSTANCE_ID).await, before_rev);
    }

    #[tokio::test]
    async fn memory_store_load_roundtrip_preserves_engine() {
        let store = MemoryApprovalStore::new();
        let mut engine = start(INSTANCE_ID);
        let (b, _) = engine
            .enter_branch(
                engine.instance.revision,
                "apply",
                None,
                JoinPolicy::All,
                RejectPolicy::AnyReject,
                ApprovalPolicy::default(),
                1_001,
            )
            .unwrap();
        engine
            .open_approval(
                engine.instance.revision,
                &b,
                vec![SeatSpec::approver("alice")],
                None,
                1_002,
            )
            .unwrap();
        assert!(store
            .commit(&engine, &[], ExpectedRev::CreateOnly)
            .await
            .unwrap());

        let (loaded, rev) = store.load(INSTANCE_ID).await.unwrap().expect("instance");
        assert_eq!(rev, 1);
        assert_eq!(loaded, engine);
        assert_eq!(
            loaded.snapshot_json().unwrap(),
            engine.snapshot_json().unwrap()
        );
    }

    #[tokio::test]
    async fn memory_store_assigns_monotonic_sequences() {
        let store = MemoryApprovalStore::new();
        let engine = start(INSTANCE_ID);
        let batch1 = vec![evt("evt-1"), evt("evt-2")];
        assert!(store
            .commit(&engine, &batch1, ExpectedRev::CreateOnly)
            .await
            .unwrap());
        let batch2 = vec![evt("evt-3"), evt("evt-4")];
        assert!(store
            .commit(&engine, &batch2, ExpectedRev::Exact(1))
            .await
            .unwrap());

        let stored = store.events(INSTANCE_ID).await;
        let seqs: Vec<u64> = stored.iter().map(|r| r.sequence).collect();
        assert_eq!(seqs, vec![1, 2, 3, 4]);
        assert!(stored.iter().all(|r| r.status == OutboxStatus::Pending));
        // 存储侧分配序号时不得就地改写调用方传入的记录。
        assert!(batch1.iter().chain(batch2.iter()).all(|r| r.sequence == 0));
    }

    #[tokio::test]
    async fn memory_store_rejects_foreign_aggregate_event() {
        let store = MemoryApprovalStore::new();
        let engine = start(INSTANCE_ID);
        let foreign = event_record(
            "inst-other",
            "com.china-workflow.instance.completed.v1",
            &json!({}),
            1_000,
            "evt-x".into(),
        )
        .unwrap();
        let err = store
            .commit(&engine, &[foreign], ExpectedRev::CreateOnly)
            .await
            .unwrap_err();
        assert_eq!(
            err,
            StoreError::IoError("approval commit: event aggregate_id mismatch".into())
        );
        assert!(store.load(INSTANCE_ID).await.unwrap().is_none());
        assert!(store.events(INSTANCE_ID).await.is_empty());
    }

    #[tokio::test]
    async fn empty_event_batch_does_not_advance_seq_counter() {
        // 空批次只写状态：序号分配基准不得因此前进，下一批事件从 1 号起。
        let store = MemoryApprovalStore::new();
        let engine = start(INSTANCE_ID);
        assert!(store
            .commit(&engine, &[], ExpectedRev::CreateOnly)
            .await
            .unwrap());
        assert!(store
            .commit(&engine, &[evt("evt-1")], ExpectedRev::Exact(1))
            .await
            .unwrap());

        let stored = store.events(INSTANCE_ID).await;
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].sequence, 1);
    }

    #[tokio::test]
    async fn idem_record_roundtrips_and_is_isolated_per_instance() {
        let store = MemoryApprovalStore::new();
        let a = start("inst-t4-a");
        let b = start("inst-t4-b");
        let rec = idem("cmd-1", json!({ "status": "APPROVED" }));

        // 记录以 JSON 存 KV：键与响应必须原样往返。
        let raw = serde_json::to_string(&rec).unwrap();
        assert_eq!(serde_json::from_str::<IdemRecord>(&raw).unwrap(), rec);

        assert!(store
            .commit_with_idem(&a, &[], ExpectedRev::CreateOnly, Some(&rec))
            .await
            .unwrap());
        assert!(store
            .commit_with_idem(&b, &[], ExpectedRev::CreateOnly, Some(&rec))
            .await
            .unwrap());

        assert_eq!(
            store.load_idem("inst-t4-a", "cmd-1").await.unwrap(),
            Some(json!({ "status": "APPROVED" }))
        );
        assert_eq!(
            store.load_idem("inst-t4-b", "cmd-1").await.unwrap(),
            Some(json!({ "status": "APPROVED" }))
        );
        // 键空间按实例隔离：别的实例、别的 key 都读不到。
        assert_eq!(store.load_idem("inst-t4-c", "cmd-1").await.unwrap(), None);
        assert_eq!(store.load_idem("inst-t4-a", "cmd-2").await.unwrap(), None);
    }

    #[tokio::test]
    async fn commit_with_idem_writes_record_on_success() {
        let store = MemoryApprovalStore::new();
        let engine = start(INSTANCE_ID);
        let rec = idem("cmd-1", json!({ "status": "APPROVED", "rev": 1 }));

        assert!(store
            .commit_with_idem(
                &engine,
                &[evt("evt-1")],
                ExpectedRev::CreateOnly,
                Some(&rec)
            )
            .await
            .unwrap());
        assert_eq!(
            store.load_idem(INSTANCE_ID, "cmd-1").await.unwrap(),
            Some(json!({ "status": "APPROVED", "rev": 1 }))
        );
        assert_eq!(store.load_idem(INSTANCE_ID, "cmd-2").await.unwrap(), None);
    }

    #[tokio::test]
    async fn commit_with_idem_failure_leaves_no_idem_record() {
        // 幂等记录只随**成功**提交生效：CAS 不匹配与输入校验失败都必须零留痕。
        let store = MemoryApprovalStore::new();
        let engine = start(INSTANCE_ID);
        assert!(store
            .commit_with_idem(&engine, &[], ExpectedRev::CreateOnly, None)
            .await
            .unwrap());

        let mut other = engine.clone();
        other.instance.initiator = "lisi".into();
        assert!(!store
            .commit_with_idem(
                &other,
                &[],
                ExpectedRev::CreateOnly,
                Some(&idem("cmd-x", json!({ "status": "APPROVED" })))
            )
            .await
            .unwrap());
        assert!(!store
            .commit_with_idem(
                &other,
                &[],
                ExpectedRev::Exact(99),
                Some(&idem("cmd-y", json!({ "status": "APPROVED" })))
            )
            .await
            .unwrap());
        assert_eq!(store.load_idem(INSTANCE_ID, "cmd-x").await.unwrap(), None);
        assert_eq!(store.load_idem(INSTANCE_ID, "cmd-y").await.unwrap(), None);

        let foreign = event_record(
            "inst-other",
            "com.china-workflow.instance.completed.v1",
            &json!({ "instance_id": "inst-other" }),
            1_000,
            "evt-f".into(),
        )
        .unwrap();
        assert!(store
            .commit_with_idem(
                &engine,
                &[foreign],
                ExpectedRev::Exact(1),
                Some(&idem("cmd-z", json!({ "status": "APPROVED" })))
            )
            .await
            .is_err());
        assert_eq!(store.load_idem(INSTANCE_ID, "cmd-z").await.unwrap(), None);
    }

    #[tokio::test]
    async fn commit_with_idem_keeps_the_first_cached_response() {
        // 同一 key 再次提交（重试方漏看 `load_idem`）不覆盖首次响应：KV 实现的
        // `VERSION==0` 谓词保证该键只有首次成功提交能写，两个实现必须同观。
        let store = MemoryApprovalStore::new();
        let engine = start(INSTANCE_ID);
        assert!(store
            .commit_with_idem(
                &engine,
                &[],
                ExpectedRev::CreateOnly,
                Some(&idem("cmd-1", json!({ "status": "APPROVED" })))
            )
            .await
            .unwrap());
        assert!(store
            .commit_with_idem(
                &engine,
                &[],
                ExpectedRev::Exact(1),
                Some(&idem("cmd-1", json!({ "status": "REJECTED" })))
            )
            .await
            .unwrap());
        assert_eq!(
            store.load_idem(INSTANCE_ID, "cmd-1").await.unwrap(),
            Some(json!({ "status": "APPROVED" }))
        );
    }

    #[tokio::test]
    async fn scan_paginates_in_key_order() {
        let store = MemoryApprovalStore::new();
        // 乱序写入，扫描必须按实例 id 字典序返回。
        for id in ["inst-b", "inst-a", "inst-d", "inst-c", "inst-e"] {
            assert!(store
                .commit(&start(id), &[], ExpectedRev::CreateOnly)
                .await
                .unwrap());
        }

        assert_eq!(scan_ids(&store, None, 2).await, ["inst-a", "inst-b"]);
        // `after` 独占：等于某实例 id 时它不重复出现在下一页。
        assert_eq!(
            scan_ids(&store, Some("inst-b"), 2).await,
            ["inst-c", "inst-d"]
        );
        assert_eq!(scan_ids(&store, Some("inst-d"), 2).await, ["inst-e"]);
        assert!(scan_ids(&store, Some("inst-e"), 2).await.is_empty());

        // 条目携带状态与修订号（供查询 RPC 直接用）。
        let first = store
            .scan(None, 1)
            .await
            .unwrap()
            .pop()
            .expect("first entry");
        assert_eq!(first.engine, start("inst-a"));
        assert_eq!(first.mod_rev, 1);

        // 幂等记录不进入实例扫描面。
        let rec = idem("cmd-1", json!({ "status": "APPROVED" }));
        assert!(store
            .commit_with_idem(&start("inst-a"), &[], ExpectedRev::Exact(1), Some(&rec))
            .await
            .unwrap());
        assert_eq!(scan_ids(&store, None, 10).await.len(), 5);
    }

    #[tokio::test]
    async fn scan_limit_bounds() {
        let store = MemoryApprovalStore::new();
        for i in 0..(APPROVAL_SCAN_MAX_LIMIT + 1) {
            let id = format!("inst-cap-{i:03}");
            assert!(store
                .commit(&start(&id), &[], ExpectedRev::CreateOnly)
                .await
                .unwrap());
        }

        assert_eq!(clamp_scan_limit(0), APPROVAL_SCAN_DEFAULT_LIMIT);
        assert_eq!(clamp_scan_limit(3), 3);
        assert_eq!(clamp_scan_limit(usize::MAX), APPROVAL_SCAN_MAX_LIMIT);
        assert_eq!(store.scan(None, 3).await.unwrap().len(), 3);
        assert_eq!(
            store.scan(None, 0).await.unwrap().len(),
            APPROVAL_SCAN_DEFAULT_LIMIT
        );
        // 上限对带 `after` 的续页同样生效。
        assert_eq!(
            store.scan(None, usize::MAX).await.unwrap().len(),
            APPROVAL_SCAN_MAX_LIMIT
        );
        assert_eq!(
            store
                .scan(Some("inst-cap-000"), usize::MAX)
                .await
                .unwrap()
                .len(),
            APPROVAL_SCAN_MAX_LIMIT
        );
    }

    #[tokio::test]
    async fn memory_store_rejects_duplicate_event_ids() {
        // 批内重复 id 会让 `ids/{id}` 索引被后来的覆盖、一条记录无法按 id 找回，
        // 而重放判定仍报告「批次在库」；必须在写前 fail-closed。
        let store = MemoryApprovalStore::new();
        let engine = start(INSTANCE_ID);
        let dup = vec![evt("evt-1"), evt("evt-2"), evt("evt-1")];
        let err = store
            .commit(&engine, &dup, ExpectedRev::CreateOnly)
            .await
            .unwrap_err();
        assert_eq!(
            err,
            StoreError::IoError(APPROVAL_COMMIT_DUPLICATE_EVENT_ID.into())
        );
        assert!(store.load(INSTANCE_ID).await.unwrap().is_none());
        assert!(store.events(INSTANCE_ID).await.is_empty());
    }
}
