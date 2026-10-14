//! v2 审批存储端口与内存实现。
//!
//! 端口只有带 CAS 的提交一条写路径（ADR-0013 D3）：没有「无 CAS」形态，冲突一律
//! `Ok(false)` 且零写入。生产实现（KV Txn）在 `coord-agent`；本模块的
//! 内存实现供正确性测试使用。

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::workflow::durable::{OutboxRecord, OutboxStatus};
use crate::workflow::ports::StoreError;

use super::engine::Engine;

/// `commit` 的期望修订号谓词。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedRev {
    /// 要求键不存在（创建实例；恰好一次语义）。
    CreateOnly,
    /// 要求当前修订号恰为 `n`（读写之间的乐观 CAS）。
    Exact(i64),
}

/// v2 审批实例存储端口。
///
/// 写路径只有 [`ApprovalStore::commit`]，且必须带 [`ExpectedRev`]：状态与一批事件在
/// 一次提交里同生共死，冲突时零写入返回 `Ok(false)`（调用方重读后重试，禁止盲写覆盖）。
#[async_trait]
pub trait ApprovalStore: Send + Sync {
    /// 线性一致地读取实例状态与其修订号；键不存在返回 `Ok(None)`。
    async fn load(&self, instance_id: &str) -> Result<Option<(Engine, i64)>, StoreError>;

    /// CAS 提交：状态与事件同一次写入。
    ///
    /// `expected` 与当前修订号匹配 ⇒ 存储 `engine`、追加 `events`（存储侧按批次顺序
    /// 分配 per-instance 单调 `sequence`，首条 = 已存 max + 1）、修订号 +1，返回
    /// `Ok(true)`；不匹配 ⇒ 零写入返回 `Ok(false)`。
    async fn commit(
        &self,
        engine: &Engine,
        events: &[OutboxRecord],
        expected: ExpectedRev,
    ) -> Result<bool, StoreError>;
}

/// 事件聚合根与实例不一致时的 fail-closed 拒绝消息。
///
/// `StoreError` 分类里没有 invalid-argument 变体，用 `IoError` 承载这条存储侧输入
/// 校验失败；消息逐字稳定，供 KV 实现（`coord-agent`）复用同一锚点。
pub const APPROVAL_COMMIT_AGGREGATE_MISMATCH: &str = "approval commit: event aggregate_id mismatch";

/// `commit` 前置校验：一批事件的 `aggregate_id` 必须全部等于实例 id。
///
/// 事件写进 `records/{aggregate}/` 后 relay 按聚合分区投递；聚合根不一致意味着调用方
/// 把别家事件挂到本实例上，必须在写前拒绝。
pub fn ensure_event_aggregates_match(
    engine: &Engine,
    events: &[OutboxRecord],
) -> Result<(), StoreError> {
    for rec in events {
        if rec.aggregate_id != engine.instance.id {
            return Err(StoreError::IoError(
                APPROVAL_COMMIT_AGGREGATE_MISMATCH.to_string(),
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
}

/// 进程内审批存储（正确性测试用）。
///
/// **原子性**：状态、修订号与事件批次存在同一个 `Mutex` 下的同一条记录里，
/// [`MemoryApprovalStore::commit`] 在一次临界区内完成三者更新 ⇒ 成功即全部生效，
/// `Ok(false)` 即三者与提交前逐字相同。这与 `durable.rs` 的 `MemoryAtomicCommit`
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

    async fn commit(
        &self,
        engine: &Engine,
        events: &[OutboxRecord],
        expected: ExpectedRev,
    ) -> Result<bool, StoreError> {
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
            }
            None => {
                map.insert(
                    id.to_string(),
                    Entry {
                        engine: engine.clone(),
                        mod_rev: 1,
                        events: batch,
                    },
                );
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::approval::engine::{Engine, StartSpec};
    use crate::workflow::approval::events::event_record;
    use crate::workflow::approval::model::{JoinPolicy, RejectPolicy, Scope, SeatSpec};
    use crate::workflow::durable::OutboxStatus;
    use serde_json::json;

    const INSTANCE_ID: &str = "inst-t4";

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
            "com.china-workflow.instance.completed",
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
        // 首次成功提交后修订号是 1，而不是 0。
        assert!(!store
            .commit(&first, &[], ExpectedRev::Exact(0))
            .await
            .unwrap());

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
            "com.china-workflow.instance.completed",
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
}
