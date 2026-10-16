//! KV 实现的 v2 审批存储（coord-agent）。
//!
//! 实例状态写 v4 独立键空间 `/_workflow/v4/instances/{id}`（见
//! `coord_core::workflow::approval::keys`），伴生事件写共享的 v3 outbox 设施
//! `/_workflow/v3/outbox/**`，两者在**同一个 Txn** 提交（ADR-0013 D3/D8）。
//!
//! 唯一的写路径 [`ApprovalStore::commit_with_idem`] 带 CAS，谓词由三部分构成：outbox
//! 侧（seq 计数器值与本批每条事件 id 的 `VERSION==0`，复用 [`KvOutboxStore`] 的构造）、
//! 实例侧（`CreateOnly` ⇒ `VERSION==0`，即 create-if-absent；`Exact(n)` ⇒ `MOD_REV==n`）、
//! 以及可选的幂等键侧（`VERSION==0`）。任一谓词不成立整个 Txn 零写入，绝不盲写覆盖。
//!
//! outbox 序号取自持久化的 `/_workflow/v3/outbox/seq/{aggregate}` 计数器
//! （[`KvOutboxStore::read_sequence`]），**不**取自事件日志：日志可被 `prune` 清理，
//! 只有计数器永久保留，序号才不会回退/重叠。

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use coord_core::workflow::approval::engine::Engine;
use coord_core::workflow::approval::keys;
use coord_core::workflow::approval::store::{
    clamp_scan_limit, ensure_event_aggregates_match, ensure_expected_rev_allowed,
    ApprovalInstanceEntry, ApprovalStore, ExpectedRev, IdemRecord,
};
use coord_core::workflow::durable::OutboxRecord;
use coord_core::workflow::ports::StoreError;
use coord_proto::kv::PutRequest;
use coord_proto::txn::compare::{CompareResult, Target, TargetValue};
use coord_proto::txn::request_op::Op;
use coord_proto::txn::{Compare, RequestOp};

use crate::proxy::AgentInner;

use super::outbox_kv::KvOutboxStore;
use super::workflow_store::key_after;

/// 提交时对同一 Txn 的重试上限。
///
/// 只有 outbox 序号竞争才会重试；实例谓词失效立即 `Ok(false)`，不消耗重试预算。
const COMMIT_RETRY_LIMIT: usize = 8;

/// 基于 coord-server KV Txn 的 v2 审批存储。
///
/// 状态文档 `/_workflow/v4/instances/{id}`，事件走 v3 outbox 传输设施，幂等记录写
/// `/_workflow/v4/idempotency/{id}/{key}`；一次 [`ApprovalStore::commit_with_idem`] 把
/// 这三者放进同一个 Txn，成功即全部生效、失败即零写入。
pub struct KvApprovalStore {
    inner: Arc<AgentInner>,
    outbox: Arc<KvOutboxStore>,
}

impl KvApprovalStore {
    /// 用共享的 KV 客户端与 outbox 存储构造（两者必须指向同一个 `AgentInner`）。
    pub fn new(inner: Arc<AgentInner>, outbox: Arc<KvOutboxStore>) -> Self {
        Self { inner, outbox }
    }

    /// 线性一致读单键：返回实例状态与其 `mod_revision`；键不存在返回 `Ok(None)`。
    async fn load_instance(&self, key: &[u8]) -> Result<Option<(Engine, i64)>, StoreError> {
        let rows = self
            .inner
            .client
            .kv()
            .range_with_rev(key, key, 1, 0)
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;
        match rows.into_iter().next() {
            Some((_k, value, mod_revision)) => serde_json::from_slice::<Engine>(&value)
                .map(|engine| Some((engine, mod_revision)))
                .map_err(|e| StoreError::SerializationError(e.to_string())),
            None => Ok(None),
        }
    }

    /// 本批事件是否**全部**已按事件 id 落库（重放判定的幂等索引侧）。
    ///
    /// 任一 id 缺失即 `false`：只有整批都在，重放才等价于原子成功。
    async fn all_events_present(&self, events: &[OutboxRecord]) -> Result<bool, StoreError> {
        for rec in events {
            if self.outbox.record_by_id(&rec.id).await?.is_none() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn txn(
        &self,
        compares: Vec<Compare>,
        success: Vec<RequestOp>,
    ) -> Result<bool, StoreError> {
        let resp = self
            .inner
            .client
            .txn()
            .txn(compares, success, vec![])
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;
        Ok(resp.succeeded)
    }
}

#[async_trait]
impl ApprovalStore for KvApprovalStore {
    async fn load(&self, instance_id: &str) -> Result<Option<(Engine, i64)>, StoreError> {
        let key = keys::instance_key(instance_id);
        self.load_instance(&key).await
    }

    async fn load_idem(&self, instance_id: &str, key: &str) -> Result<Option<Value>, StoreError> {
        let idem_key = keys::idem_key(instance_id, key);
        let rows = self
            .inner
            .client
            .kv()
            .range_with_rev(&idem_key, &idem_key, 1, 0)
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;
        match rows.into_iter().next() {
            Some((_k, value, _rev)) => serde_json::from_slice::<IdemRecord>(&value)
                .map(|rec| Some(rec.response))
                .map_err(|e| StoreError::SerializationError(e.to_string())),
            None => Ok(None),
        }
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
        let key = keys::instance_key(&engine.instance.id);
        let value = serde_json::to_vec(engine)
            .map_err(|e| StoreError::SerializationError(e.to_string()))?;
        // 幂等键与内容在循环外定下：每一轮重试写的都是同一份记录。
        let idem_ops = match idem {
            Some(rec) => Some((
                keys::idem_key(&engine.instance.id, &rec.key),
                serde_json::to_vec(rec)
                    .map_err(|e| StoreError::SerializationError(e.to_string()))?,
            )),
            None => None,
        };

        for _ in 0..COMMIT_RETRY_LIMIT {
            let current = self.load_instance(&key).await?;

            // (a) 重放：状态逐字相同且本批事件全部已在库 ⇒ 幂等成功（零写入）。
            if replay_matches(current.as_ref(), engine) && self.all_events_present(events).await? {
                return Ok(true);
            }
            // (b) 实例谓词失效 ⇒ 零写入返回，调用方重读后重试。
            if !instance_predicate_holds(expected, current.as_ref()) {
                return Ok(false);
            }

            // 谓词成立 ⇒ 状态 + 全部事件（+ 幂等记录）在同一 Txn 提交；序号接在持久化
            // 计数器之后。
            let (cur, cur_bytes) = self.outbox.read_sequence(&engine.instance.id).await?;
            let mut compares =
                KvOutboxStore::outbox_compares(&engine.instance.id, cur_bytes, events);
            compares.push(instance_predicate(&key, expected));
            let mut success = vec![put(key.clone(), value.clone())];
            success.extend(KvOutboxStore::outbox_ops(events, cur + 1)?);
            if let Some((idem_key, idem_value)) = idem_ops.as_ref() {
                // 幂等键必须不存在：同一命令键只有首次成功提交能写（重放不覆盖缓存响应）。
                compares.push(idem_predicate(idem_key));
                success.push(put(idem_key.clone(), idem_value.clone()));
            }

            if self.txn(compares, success).await? {
                return Ok(true);
            }
            // Txn 失败（多半是 outbox 序号或幂等键被并发写者占住）：下一轮重读后重新判定。
        }
        Ok(false)
    }

    async fn scan(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ApprovalInstanceEntry>, StoreError> {
        let prefix = keys::instance_prefix();
        // `after` 独占续读：`key_after` 给出严格后继键，等于 `after` 的实例不重复出现。
        // 范围终点卡在实例前缀内，v4 内的其他键（幂等记录）不会进入本扫描面。
        let start = match after {
            Some(id) => key_after(&keys::instance_key(id)),
            None => prefix.to_vec(),
        };
        let end = keys::prefix_range_end(prefix);
        let rows = self
            .inner
            .client
            .kv()
            .range_with_rev(&start, &end, clamp_scan_limit(limit) as i64, 0)
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;

        let mut entries = Vec::with_capacity(rows.len());
        for (key, value, mod_revision) in rows {
            let Some(id) = key
                .strip_prefix(prefix)
                .and_then(|suffix| std::str::from_utf8(suffix).ok())
            else {
                return Err(StoreError::SerializationError(
                    "approval scan: key outside the instance prefix".to_string(),
                ));
            };
            let engine: Engine = serde_json::from_slice(&value)
                .map_err(|e| StoreError::SerializationError(e.to_string()))?;
            entries.push(ApprovalInstanceEntry {
                id: id.to_string(),
                engine,
                mod_rev: mod_revision,
            });
        }
        Ok(entries)
    }
}

/// `Put` 请求操作（状态文档写入；与 v1 存储侧的构造逐字一致）。
fn put(key: Vec<u8>, value: Vec<u8>) -> RequestOp {
    RequestOp {
        op: Some(Op::RequestPut(PutRequest {
            key,
            value,
            lease_id: 0,
            prev_kv: false,
            request_id: vec![],
        })),
    }
}

/// 实例侧 CAS 谓词：`CreateOnly` ⇒ `VERSION==0`；`Exact(n)` ⇒ `MOD_REV==n`。
fn instance_predicate(key: &[u8], expected: ExpectedRev) -> Compare {
    match expected {
        ExpectedRev::CreateOnly => Compare {
            result: CompareResult::Equal as i32,
            target: Target::Version as i32,
            key: key.to_vec(),
            target_value: Some(TargetValue::Version(0)),
        },
        ExpectedRev::Exact(n) => Compare {
            result: CompareResult::Equal as i32,
            target: Target::ModRev as i32,
            key: key.to_vec(),
            target_value: Some(TargetValue::ModRevision(n)),
        },
    }
}

/// 幂等键侧 CAS 谓词：要求键不存在（`VERSION==0`），即该命令键只被成功写入一次。
fn idem_predicate(key: &[u8]) -> Compare {
    Compare {
        result: CompareResult::Equal as i32,
        target: Target::Version as i32,
        key: key.to_vec(),
        target_value: Some(TargetValue::Version(0)),
    }
}

/// 读到的当前状态是否满足 `expected`（谓词的读侧镜像）。
fn instance_predicate_holds(expected: ExpectedRev, current: Option<&(Engine, i64)>) -> bool {
    match expected {
        ExpectedRev::CreateOnly => current.is_none(),
        ExpectedRev::Exact(n) => current.map(|(_, rev)| *rev == n).unwrap_or(false),
    }
}

/// 读到的当前状态是否与本次提交的 `engine` 逐字相同（重放判定的状态侧）。
fn replay_matches(current: Option<&(Engine, i64)>, engine: &Engine) -> bool {
    current.map(|(stored, _)| stored == engine).unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use coord_core::workflow::approval::engine::StartSpec;
    use coord_core::workflow::approval::model::Scope;

    fn start(id: &str, initiator: &str) -> Engine {
        Engine::start(
            StartSpec {
                id: id.into(),
                definition_ns: "eis".into(),
                definition_name: "leave-approval".into(),
                definition_version: "1.0.0".into(),
                definition_hash: "sha256:t5".into(),
                initiator: initiator.into(),
                context: Scope::new(),
            },
            1_000,
        )
        .expect("start instance")
    }

    #[test]
    fn approval_kv_key_layout_isolated_from_v1() {
        let key = keys::instance_key("i");
        assert_eq!(key, b"/_workflow/v4/instances/i".to_vec());
        assert!(key.starts_with(keys::instance_prefix()));

        let v1_key = super::super::workflow_store::KvWorkflowStore::instance_key("i");
        assert_eq!(
            String::from_utf8_lossy(&v1_key),
            "/_workflow/v3/instances/i"
        );
        assert!(!key.starts_with(&v1_key));
        assert!(!v1_key.starts_with(keys::instance_prefix()));
    }

    #[test]
    fn create_only_predicate_compares_version_zero() {
        let cmp = instance_predicate(b"/_workflow/v4/instances/i", ExpectedRev::CreateOnly);
        assert_eq!(cmp.result, CompareResult::Equal as i32);
        assert_eq!(cmp.target, Target::Version as i32);
        assert_eq!(cmp.target_value, Some(TargetValue::Version(0)));
    }

    #[test]
    fn exact_predicate_compares_mod_revision() {
        let cmp = instance_predicate(b"/_workflow/v4/instances/i", ExpectedRev::Exact(7));
        assert_eq!(cmp.result, CompareResult::Equal as i32);
        assert_eq!(cmp.target, Target::ModRev as i32);
        assert_eq!(cmp.target_value, Some(TargetValue::ModRevision(7)));
    }

    #[test]
    fn instance_predicate_holds_only_for_matching_revision() {
        let engine = start("i", "alice");
        let current = vec![(engine.clone(), 5)];
        assert!(instance_predicate_holds(
            ExpectedRev::Exact(5),
            current.first()
        ));
        assert!(!instance_predicate_holds(
            ExpectedRev::Exact(4),
            current.first()
        ));
        assert!(!instance_predicate_holds(
            ExpectedRev::CreateOnly,
            current.first()
        ));
        assert!(instance_predicate_holds(ExpectedRev::CreateOnly, None));
        assert!(!instance_predicate_holds(ExpectedRev::Exact(1), None));
    }

    #[test]
    fn replay_requires_identical_state() {
        let engine = start("i", "alice");
        let mut other = engine.clone();
        other.instance.initiator = "bob".into();
        let current = vec![(engine.clone(), 1)];
        assert!(replay_matches(current.first(), &engine));
        assert!(!replay_matches(current.first(), &other));
        assert!(!replay_matches(None, &engine));
    }
}
