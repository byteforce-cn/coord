// coord-agent: KV 持久化 outbox 与持久化定时器（Spike 2026-10 Spike 3）
//
// 实现 `coord-core` 的 `OutboxStore` / `DurableTimerStore` / `AtomicOutboxCommit`：
// 全部经 coord-server 的 KV + Txn（Raft 共识），因此进程被 kill -9 后记录仍在。
//
// 键布局（`/_workflow/v3/` 与 `KvWorkflowStore` 同一前缀族）：
//
// ```text
// /_workflow/v3/outbox/records/{aggregate_id}/{sequence:020}   → OutboxRecord JSON（权威归档）
// /_workflow/v3/outbox/ids/{event_id}                          → 记录键（幂等索引）
// /_workflow/v3/outbox/seq/{aggregate_id}                      → 该聚合的 seq 计数器（prune 永久保留）
// /_workflow/v3/outbox/pending/{next_attempt_at:020}:{event_id} → OutboxRecord JSON（到期扫描索引）
// /_workflow/v3/timers/{fire_at_ms:020}:{instance_id}:{kind}   → TimerRecord JSON
// ```
//
// 「同实例前缀 + 时间序/序号」两处都成立：`records/{aggregate_id}/` 是实例前缀 +
// 零填充序号，`pending/` 与 `timers/` 是零填充时间序（字典序 = 时间序 ⇒ 到期可扫描）。

use std::sync::Arc;

use async_trait::async_trait;

use coord_core::workflow::durable::{
    AtomicOutboxCommit, DurableTimerStore, OutboxRecord, OutboxStatus, OutboxStore, TimerRecord,
};
use coord_core::workflow::model::WorkflowInstance;
use coord_core::workflow::ports::StoreError;
use coord_proto::kv::{DeleteRequest, PutRequest};
use coord_proto::txn::compare::{CompareResult, Target, TargetValue};
use coord_proto::txn::request_op::Op;
use coord_proto::txn::{Compare, RequestOp};

use crate::proxy::AgentInner;

use super::workflow_store::{prefix_end, KvWorkflowStore};

/// KV 持久化 outbox + 定时器存储。
pub struct KvOutboxStore {
    inner: Arc<AgentInner>,
}

impl KvOutboxStore {
    pub fn new(inner: Arc<AgentInner>) -> Self {
        Self { inner }
    }

    // ─── 键构造 ───

    fn record_key(aggregate: &str, sequence: u64) -> Vec<u8> {
        format!("/_workflow/v3/outbox/records/{aggregate}/{sequence:020}").into_bytes()
    }

    fn records_prefix() -> Vec<u8> {
        b"/_workflow/v3/outbox/records/".to_vec()
    }

    fn id_key(id: &str) -> Vec<u8> {
        format!("/_workflow/v3/outbox/ids/{id}").into_bytes()
    }

    fn seq_key(aggregate: &str) -> Vec<u8> {
        format!("/_workflow/v3/outbox/seq/{aggregate}").into_bytes()
    }

    fn pending_key(next_attempt_at_ms: i64, id: &str) -> Vec<u8> {
        format!("/_workflow/v3/outbox/pending/{next_attempt_at_ms:020}:{id}").into_bytes()
    }

    fn pending_prefix() -> Vec<u8> {
        b"/_workflow/v3/outbox/pending/".to_vec()
    }

    fn timer_key(id: &str) -> Vec<u8> {
        format!("/_workflow/v3/timers/{id}").into_bytes()
    }

    fn timer_prefix() -> Vec<u8> {
        b"/_workflow/v3/timers/".to_vec()
    }

    // ─── 小工具 ───

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        let pairs = self
            .inner
            .client
            .kv()
            .range(key, &[], 0, 0)
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;
        Ok(pairs.into_iter().next().map(|(_, v)| v))
    }

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
    fn del(key: Vec<u8>) -> RequestOp {
        RequestOp {
            op: Some(Op::RequestDelete(DeleteRequest {
                key,
                range_end: vec![],
                prev_kv: false,
                request_id: vec![],
            })),
        }
    }

    fn compare_value_equal(key: Vec<u8>, value: Vec<u8>) -> Compare {
        Compare {
            result: CompareResult::Equal as i32,
            target: Target::Value as i32,
            key,
            target_value: Some(TargetValue::Value(value)),
        }
    }

    fn compare_version_equal(key: Vec<u8>, version: i64) -> Compare {
        Compare {
            result: CompareResult::Equal as i32,
            target: Target::Version as i32,
            key,
            target_value: Some(TargetValue::Version(version)),
        }
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

    /// 读取聚合的 seq 计数器（缺省 0）及用于 CAS 的当前字节。
    ///
    /// 键不存在时 compare 的目标必须是**空值**（而非 "0" 字符串）：coord 的
    /// Txn 对缺失键的 `VALUE` 比较取空字节（见 mvcc 的 `value.unwrap_or_default()`）。
    pub(crate) async fn read_sequence(
        &self,
        aggregate: &str,
    ) -> Result<(u64, Vec<u8>), StoreError> {
        let raw = self.get(&Self::seq_key(aggregate)).await?;
        match raw {
            None => Ok((0, Vec::new())),
            Some(bytes) => {
                let cur = std::str::from_utf8(&bytes)
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);
                Ok((cur, bytes))
            }
        }
    }

    async fn record_by_id(&self, id: &str) -> Result<Option<OutboxRecord>, StoreError> {
        let Some(record_key) = self.get(&Self::id_key(id)).await? else {
            return Ok(None);
        };
        let Some(raw) = self.get(&record_key).await? else {
            return Ok(None);
        };
        serde_json::from_slice(&raw)
            .map(Some)
            .map_err(|e| StoreError::SerializationError(e.to_string()))
    }

    /// 重写一条已存在记录：删旧 pending 索引 + 写记录 + 额外操作（同事务）。
    async fn rewrite_record(
        &self,
        rec: &OutboxRecord,
        old_pending_key: Vec<u8>,
        extra: Vec<RequestOp>,
    ) -> Result<(), StoreError> {
        let record_key = Self::record_key(&rec.aggregate_id, rec.sequence);
        let raw =
            serde_json::to_vec(rec).map_err(|e| StoreError::SerializationError(e.to_string()))?;
        let mut success = vec![Self::del(old_pending_key), Self::put(record_key, raw)];
        success.extend(extra);
        let _ = self.txn(vec![], success).await?;
        Ok(())
    }

    /// 一批 outbox 记录的同事务写操作：seq 计数器 + 每条 `ids/records/pending`。
    ///
    /// `seq_start` = 第一条分配的 sequence（= 读到的当前 seq + 1）；记录按数组顺序
    /// 占号 `seq_start..seq_start + n - 1`，seq 计数器推进到 `seq_start + n - 1`。
    /// 调用方须保证 `recs` 同属一个聚合根（seq 键取首条的 `aggregate_id`）；空切片
    /// 返回空操作（此时不写 seq，仅提交实例状态）。乐观 CAS 与租约 fenced 路径共用，
    /// 使键布局与序号分配只有一处定义。
    pub(crate) fn outbox_ops(
        recs: &[OutboxRecord],
        seq_start: u64,
    ) -> Result<Vec<RequestOp>, StoreError> {
        let Some(first) = recs.first() else {
            return Ok(Vec::new());
        };
        let aggregate = &first.aggregate_id;
        let last = seq_start + recs.len() as u64 - 1;
        let mut ops = Vec::with_capacity(1 + recs.len() * 3);
        ops.push(Self::put(
            Self::seq_key(aggregate),
            last.to_string().into_bytes(),
        ));
        for (i, rec) in recs.iter().enumerate() {
            let sequence = seq_start + i as u64;
            let mut stored = rec.clone();
            stored.sequence = sequence;
            stored.status = OutboxStatus::Pending;
            let raw = serde_json::to_vec(&stored)
                .map_err(|e| StoreError::SerializationError(e.to_string()))?;
            ops.push(Self::put(
                Self::id_key(&rec.id),
                Self::record_key(aggregate, sequence),
            ));
            ops.push(Self::put(
                Self::record_key(aggregate, sequence),
                raw.clone(),
            ));
            ops.push(Self::put(
                Self::pending_key(stored.next_attempt_at_ms, &rec.id),
                raw,
            ));
        }
        Ok(ops)
    }

    /// 一批记录的 outbox 侧 CAS 谓词：seq `VALUE == 当前字节` + 每条 id `VERSION == 0`。
    ///
    /// 实例状态侧的谓词（MOD_REV 或租约持有）由调用方追加：无论乐观 CAS 还是租约
    /// fenced 提交，outbox 侧谓词都相同，故只在此处构造。空批次返回空谓词——没有
    /// 对应写入时比较 seq 只会让一次纯状态提交被并发的 `append` 无端判失败。
    pub(crate) fn outbox_compares(
        aggregate: &str,
        cur_bytes: Vec<u8>,
        recs: &[OutboxRecord],
    ) -> Vec<Compare> {
        if recs.is_empty() {
            return Vec::new();
        }
        let mut compares = Vec::with_capacity(1 + recs.len());
        compares.push(Self::compare_value_equal(
            Self::seq_key(aggregate),
            cur_bytes,
        ));
        for rec in recs {
            compares.push(Self::compare_version_equal(Self::id_key(&rec.id), 0));
        }
        compares
    }
}

/// 保留窗口的计时基准（删除条件缺一不可的两项之一）；`None` = 不可删（未投递）。
///
/// - `Delivered`：投递成功时刻（`delivered_at_ms`）；缺字段的老记录退回入库时刻。
/// - `Dead`：末次尝试时刻，即 `max(created_at_ms, next_attempt_at_ms)`。记录不携带
///   「末次尝试」的独立时间戳，而是每次失败重试把 `next_attempt_at_ms` 写成该次
///   尝试的到期时刻且进入死信后不再更新，无重试时它就是入库时刻。仅以入库时刻为
///   基准，会在旧事件刚失败进死信时把它当成早已过期删掉。
///   边界：`max_attempts == 1` 时首次失败即进死信、基址可能是旧的到期时刻，记录
///   可在进死信的**同一 tick** 被回收（丢失死信证据）；默认策略 `max_attempts = 8`
///   下每次失败都经 `mark_retry` 重置基址，此边界不可达。
fn retention_basis_ms(rec: &OutboxRecord) -> Option<i64> {
    match rec.status {
        OutboxStatus::Pending => None,
        OutboxStatus::Delivered => Some(rec.delivered_at_ms.unwrap_or(rec.created_at_ms)),
        OutboxStatus::Dead => Some(rec.created_at_ms.max(rec.next_attempt_at_ms)),
    }
}

#[async_trait]
impl OutboxStore for KvOutboxStore {
    async fn append(&self, rec: &OutboxRecord) -> Result<(), StoreError> {
        // 幂等：同 id 已入库 ⇒ 空操作（relay 重试/恢复重放都靠它安全）
        if self.record_by_id(&rec.id).await?.is_some() {
            return Ok(());
        }
        for _ in 0..8 {
            let (cur, cur_bytes) = self.read_sequence(&rec.aggregate_id).await?;
            let success = Self::outbox_ops(std::slice::from_ref(rec), cur + 1)?;
            let compares =
                Self::outbox_compares(&rec.aggregate_id, cur_bytes, std::slice::from_ref(rec));
            if self.txn(compares, success).await? {
                return Ok(());
            }
            // 可能是并发 append 已写同 id（幂等成功），否则重试
            if self.record_by_id(&rec.id).await?.is_some() {
                return Ok(());
            }
        }
        Err(StoreError::IoError(
            "outbox append: sequence CAS did not converge after 8 attempts".to_string(),
        ))
    }

    async fn pending(&self, now_ms: i64, limit: usize) -> Result<Vec<OutboxRecord>, StoreError> {
        let prefix = Self::pending_prefix();
        let pairs = self
            .inner
            .client
            .kv()
            .range(&prefix, &prefix_end(&prefix), 0, 0)
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;
        let mut out = Vec::new();
        for (key, value) in pairs {
            // 键前缀 = 零填充 next_attempt_at（20 位）；字典序 = 时间序
            let Ok(suffix) = std::str::from_utf8(&key[prefix.len()..]) else {
                continue;
            };
            let Some((ts, _id)) = suffix.split_once(':') else {
                continue;
            };
            let Ok(next_at) = ts.parse::<i64>() else {
                continue;
            };
            // 已到期排序：后面的一定更晚，可以停
            if next_at > now_ms {
                break;
            }
            let Ok(rec) = serde_json::from_slice::<OutboxRecord>(&value) else {
                continue;
            };
            if rec.status != OutboxStatus::Pending {
                continue;
            }
            out.push(rec);
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    async fn mark_delivered(&self, id: &str, delivered_at_ms: i64) -> Result<(), StoreError> {
        let Some(mut rec) = self.record_by_id(id).await? else {
            return Ok(());
        };
        let old_pending = Self::pending_key(rec.next_attempt_at_ms, &rec.id);
        rec.status = OutboxStatus::Delivered;
        rec.delivered_at_ms = Some(delivered_at_ms);
        self.rewrite_record(&rec, old_pending, vec![]).await
    }

    async fn mark_retry(
        &self,
        id: &str,
        attempts: u32,
        next_attempt_at_ms: i64,
    ) -> Result<(), StoreError> {
        let Some(mut rec) = self.record_by_id(id).await? else {
            return Ok(());
        };
        // 旧 pending 键必须在改 `next_attempt_at_ms` 之前算出来
        let old_pending = Self::pending_key(rec.next_attempt_at_ms, &rec.id);
        let new_pending = Self::pending_key(next_attempt_at_ms, &rec.id);
        rec.attempts = attempts;
        rec.next_attempt_at_ms = next_attempt_at_ms;
        rec.status = OutboxStatus::Pending;
        let raw =
            serde_json::to_vec(&rec).map_err(|e| StoreError::SerializationError(e.to_string()))?;
        self.rewrite_record(&rec, old_pending, vec![Self::put(new_pending, raw)])
            .await
    }

    async fn mark_dead(&self, id: &str, attempts: u32) -> Result<(), StoreError> {
        let Some(mut rec) = self.record_by_id(id).await? else {
            return Ok(());
        };
        let old_pending = Self::pending_key(rec.next_attempt_at_ms, &rec.id);
        rec.status = OutboxStatus::Dead;
        rec.attempts = attempts;
        self.rewrite_record(&rec, old_pending, vec![]).await
    }

    async fn list_all(&self) -> Result<Vec<OutboxRecord>, StoreError> {
        let prefix = Self::records_prefix();
        let pairs = self
            .inner
            .client
            .kv()
            .range(&prefix, &prefix_end(&prefix), 0, 0)
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;
        let mut out = Vec::new();
        for (_k, v) in pairs {
            if let Ok(rec) = serde_json::from_slice::<OutboxRecord>(&v) {
                out.push(rec);
            }
        }
        out.sort_by(|a, b| {
            a.aggregate_id
                .cmp(&b.aggregate_id)
                .then(a.sequence.cmp(&b.sequence))
        });
        Ok(out)
    }

    /// 回收已终结且过保留窗口的记录（删除条件缺一不可，见 trait 契约）。
    ///
    /// 扫描 `records/`（权威归档，含删改）逐个判定，每条一个 Txn 删
    /// `records/` + `ids/` + 残留的 `pending/`：
    ///
    /// - compare 用「记录值未变」做 CAS：清扫期间记录被并发重写（例如死信被重新投递、
    ///   已投递记录被补写时间）时该条本轮跳过，不会把新状态删掉；
    /// - `seq/` **不删** —— 它是 per-instance 序号单调的唯一载体，删了会让清理后新事件
    ///   的序号与已投递事件的历史序号重叠；
    /// - 单次调用只扫**一个** `records/` 前缀窗口（`limit = 0` 的 range 被服务端
    ///   `max_range_limit` 截断，无截断标记）；回收因而是**每次调用尽力而为**：只有
    ///   当合格记录落在本次窗口内才会被回收，落在窗口外的要等后续 tick 重扫命中。
    ///   「全前缀扫描 + 无分页游标」的代价与有界化（aged index / 续读游标）是 M2
    ///   的跟进项，本实现不改键布局。
    async fn prune(
        &self,
        now_ms: i64,
        retention_ms: i64,
        limit: usize,
    ) -> Result<usize, StoreError> {
        if limit == 0 {
            return Ok(0);
        }
        let prefix = Self::records_prefix();
        let pairs = self
            .inner
            .client
            .kv()
            .range(&prefix, &prefix_end(&prefix), 0, 0)
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;

        let mut pruned = 0usize;
        for (record_key, value) in pairs {
            if pruned >= limit {
                break;
            }
            let Ok(rec) = serde_json::from_slice::<OutboxRecord>(&value) else {
                continue;
            };
            let Some(basis_ms) = retention_basis_ms(&rec) else {
                continue; // Pending：未投递不删
            };
            if basis_ms.saturating_add(retention_ms) >= now_ms {
                continue;
            }
            let ops = vec![
                Self::del(record_key.clone()),
                Self::del(Self::id_key(&rec.id)),
                Self::del(Self::pending_key(rec.next_attempt_at_ms, &rec.id)),
            ];
            if self
                .txn(vec![Self::compare_value_equal(record_key, value)], ops)
                .await?
            {
                pruned += 1;
            }
        }
        Ok(pruned)
    }
}

#[async_trait]
impl DurableTimerStore for KvOutboxStore {
    async fn schedule(&self, rec: &TimerRecord) -> Result<(), StoreError> {
        let raw =
            serde_json::to_vec(rec).map_err(|e| StoreError::SerializationError(e.to_string()))?;
        self.inner
            .client
            .kv()
            .put(&Self::timer_key(&rec.id), &raw)
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;
        Ok(())
    }

    async fn due(&self, now_ms: i64, limit: usize) -> Result<Vec<TimerRecord>, StoreError> {
        let prefix = Self::timer_prefix();
        let pairs = self
            .inner
            .client
            .kv()
            .range(&prefix, &prefix_end(&prefix), 0, 0)
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;
        let mut out = Vec::new();
        for (key, value) in pairs {
            let Ok(suffix) = std::str::from_utf8(&key[prefix.len()..]) else {
                continue;
            };
            let Some((ts, _rest)) = suffix.split_once(':') else {
                continue;
            };
            let Ok(fire_at) = ts.parse::<i64>() else {
                continue;
            };
            if fire_at > now_ms {
                break;
            }
            if let Ok(rec) = serde_json::from_slice::<TimerRecord>(&value) {
                out.push(rec);
            }
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    async fn remove(&self, id: &str) -> Result<(), StoreError> {
        self.inner
            .client
            .kv()
            .delete(&Self::timer_key(id))
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;
        Ok(())
    }

    async fn list_all(&self) -> Result<Vec<TimerRecord>, StoreError> {
        let prefix = Self::timer_prefix();
        let pairs = self
            .inner
            .client
            .kv()
            .range(&prefix, &prefix_end(&prefix), 0, 0)
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;
        let mut out: Vec<TimerRecord> = pairs
            .into_iter()
            .filter_map(|(_k, v)| serde_json::from_slice::<TimerRecord>(&v).ok())
            .collect();
        out.sort_by(|a, b| a.fire_at_ms.cmp(&b.fire_at_ms).then(a.id.cmp(&b.id)));
        Ok(out)
    }
}

#[async_trait]
impl AtomicOutboxCommit for KvOutboxStore {
    async fn commit_state_and_outbox_batch(
        &self,
        inst: &WorkflowInstance,
        recs: &[OutboxRecord],
        expected_mod_rev: Option<i64>,
    ) -> Result<bool, StoreError> {
        // 批次属于一个聚合根（= 实例 id）；用记录侧聚合键读 seq，保持与 append 同一计数器。
        let aggregate = recs
            .first()
            .map(|r| r.aggregate_id.as_str())
            .unwrap_or(inst.id.as_str());
        for _ in 0..8 {
            let (cur, cur_bytes) = self.read_sequence(aggregate).await?;
            let instance_raw = serde_json::to_vec(inst)
                .map_err(|e| StoreError::SerializationError(e.to_string()))?;

            let mut success = vec![Self::put(
                KvWorkflowStore::instance_key(&inst.id),
                instance_raw,
            )];
            success.extend(Self::outbox_ops(recs, cur + 1)?);

            let mut compares = Self::outbox_compares(aggregate, cur_bytes, recs);
            if let Some(rev) = expected_mod_rev {
                compares.push(Compare {
                    result: CompareResult::Equal as i32,
                    target: Target::ModRev as i32,
                    key: KvWorkflowStore::instance_key(&inst.id),
                    target_value: Some(TargetValue::ModRevision(rev)),
                });
            }

            if self.txn(compares, success).await? {
                return Ok(true);
            }
            // CAS 失败：**全部** id 已存在（前一次其实已提交）⇒ 幂等重提交；否则重读 seq 重试。
            let mut all_present = !recs.is_empty();
            for rec in recs {
                if self.record_by_id(&rec.id).await?.is_none() {
                    all_present = false;
                    break;
                }
            }
            if all_present {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

// ─── MQ 订阅游标的 KV 持久化（MqEventProvider 的 CursorStore） ───

/// KV 持久化订阅游标：`/_workflow/v3/mq-offsets/{subscription}`。
///
/// `MqEventProvider` 的重启续读依赖它；进程 kill -9 后位点仍在。
pub struct KvCursorStore {
    inner: Arc<AgentInner>,
}

impl KvCursorStore {
    pub fn new(inner: Arc<AgentInner>) -> Self {
        Self { inner }
    }

    fn key(subscription: &str) -> Vec<u8> {
        format!("/_workflow/v3/mq-offsets/{subscription}").into_bytes()
    }
}

#[async_trait]
impl super::mq_event_provider::CursorStore for KvCursorStore {
    async fn load(&self, subscription: &str) -> Result<u64, String> {
        let pairs = self
            .inner
            .client
            .kv()
            .range(&Self::key(subscription), &[], 0, 0)
            .await
            .map_err(|e| e.to_string())?;
        Ok(pairs
            .into_iter()
            .next()
            .and_then(|(_, v)| String::from_utf8(v).ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0))
    }

    async fn store(&self, subscription: &str, next_offset: u64) -> Result<(), String> {
        self.inner
            .client
            .kv()
            .put(&Self::key(subscription), next_offset.to_string().as_bytes())
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}
