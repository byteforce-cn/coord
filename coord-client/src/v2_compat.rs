//! v1 形状 → v2 wire 的数据面兼容层（Agent 内部服务迁移，#170）。
//!
//! 只在 [`crate::config::DataPlane::V2`] 下启用：调用方仍按 v1 的
//! `Kv/Txn/Watch/Lease` 形状调用，本模块把请求落到 `coord.*.v2`。v1 路径
//! 不经过这里，行为不变。
//!
//! 写路径统一走 `coord.txn.v2`（单写集 `Begin` + `Commit`）：写时间戳由
//! Sequencer 分配，调用方不需要 `data:tso:allocate`；也不经过 `coord.kv.v2`
//! 直写，因此没有「直写越过 pending CommitDecision」的次序窗口（#244）。
//!
//! v1 `Txn` 的 compare 语义用 v2 的 OCC 读集复现：在会话里读比较键（服务端
//! 记录读集）→ 本地判定 → `Commit` 校验读集；冲突说明比较键在快照之后被写过，
//! 换新 `start_ts` 重跑即可（等价于 v1 的串行执行）。

use std::collections::{BTreeMap, BTreeSet};

use coord_core::error::{Error, Result};
use coord_core::error_code::CoordErrorCode;
use coord_core::kv_range::prefix_successor;
use coord_proto::kv::v2 as kv_v2;
use coord_proto::kv::{DeleteResponse, KeyValue, PutResponse, RangeResponse};
use coord_proto::txn::compare::{CompareResult, Target, TargetValue};
use coord_proto::txn::request_op::Op as RequestOpKind;
use coord_proto::txn::response_op::Op as ResponseOpKind;
use coord_proto::txn::v2 as txn_v2;
use coord_proto::txn::v2::mutation::Op as MutationOp;
use coord_proto::txn::{Compare, RequestOp, ResponseOp, TxnResponse};
use coord_proto::watch::{watch_event::EventType, WatchEvent};
use tokio::sync::mpsc;

use crate::client::Client;
use crate::retry::RetryState;
use crate::txn_v2::{TxnCommit, TxnV2Session};

/// v1 形状的 `Put`；返回 `commit_ts`（v1 的 revision）。
pub(crate) async fn kv_put(
    client: &Client,
    key: &[u8],
    value: &[u8],
    lease_id: i64,
    request_id: &[u8],
) -> Result<u64> {
    let mutation = txn_v2::Mutation {
        op: MutationOp::Put as i32,
        key: key.to_vec(),
        range_end: Vec::new(),
        value: value.to_vec(),
        lease_id,
    };
    let commit_ts = commit_write(client, vec![mutation], request_id).await?;
    client.observe_commit_ts(commit_ts);
    Ok(commit_ts)
}

/// v1 形状的 `Range`；返回 v2 元数据完整的行。
///
/// v1 的 `revision` 与 v2 的 `read_ts` 同义（0 = 最新/safe_ts）。
pub(crate) async fn kv_range(
    client: &Client,
    key: &[u8],
    range_end: &[u8],
    limit: i64,
    revision: i64,
    keys_only: bool,
    count_only: bool,
) -> Result<(Vec<kv_v2::KeyValue>, i64, i64)> {
    let read_ts = if revision == 0 {
        client.read_floor()
    } else {
        u64::try_from(revision)
            .map_err(|_| Error::InvalidArgument("revision must be >= 0".into()))?
    };
    let (kvs, count, read_ts) = client
        .kv_v2()
        .range(key, range_end, limit, read_ts, keys_only, count_only)
        .await?;
    let read_ts = i64::try_from(read_ts)
        .map_err(|_| Error::Internal("negative read_ts in response".into()))?;
    Ok((kvs, count, read_ts))
}

/// v1 形状的 `Delete`；返回 `(deleted, commit_ts)`。
///
/// 删除计数在同一会话里先读命中集再提交：读集（服务端记录）保证提交时命中集
/// 没有被并发改写；冲突重跑，不返回伪造计数。
pub(crate) async fn kv_delete(
    client: &Client,
    key: &[u8],
    range_end: &[u8],
    request_id: &[u8],
) -> Result<(i64, u64)> {
    let session = client.txn_v2().begin(0, 0).await?;
    let range = normalize_range(key, range_end);
    let hits = match session
        .read(range.0, range.1.as_deref().unwrap_or_default(), 0, true)
        .await
    {
        Ok((hits, _)) => hits,
        Err(e) => {
            // 冲突/失败都必须放弃会话：服务端会话 TTL 默认 1h，不能靠过期回收。
            let _ = session.abort().await;
            return Err(e);
        }
    };
    let deleted = hits.len() as i64;
    let mutation = delete_mutation(&range);
    match session.commit_in_place(vec![mutation], request_id).await {
        Ok(TxnCommit::Committed { commit_ts }) => {
            client.observe_commit_ts(commit_ts);
            Ok((deleted, commit_ts))
        }
        Ok(TxnCommit::Conflict) => {
            let _ = session.abort().await;
            Err(conflict_exhausted())
        }
        Err(e) => {
            let _ = session.abort().await;
            Err(e)
        }
    }
}

/// v1 `Txn`（compare + success/failure 写集）→ v2 OCC 会话。
pub(crate) async fn txn(
    client: &Client,
    compares: Vec<Compare>,
    success_ops: Vec<RequestOp>,
    failure_ops: Vec<RequestOp>,
    request_id: Vec<u8>,
) -> Result<TxnResponse> {
    let mut retry = RetryState::new(client.retry_config());
    loop {
        match txn_once(client, &compares, &success_ops, &failure_ops, &request_id).await {
            Ok(resp) => return Ok(resp),
            Err(TxnAttempt::Conflict) => match retry.next_attempt() {
                Some(wait) => tokio::time::sleep(wait).await,
                None => return Err(conflict_exhausted()),
            },
            Err(TxnAttempt::Fatal(e)) => return Err(e),
        }
    }
}

enum TxnAttempt {
    Conflict,
    Fatal(Error),
}

impl From<Error> for TxnAttempt {
    fn from(e: Error) -> Self {
        Self::Fatal(e)
    }
}

async fn txn_once(
    client: &Client,
    compares: &[Compare],
    success_ops: &[RequestOp],
    failure_ops: &[RequestOp],
    request_id: &[u8],
) -> std::result::Result<TxnResponse, TxnAttempt> {
    let session = client.txn_v2().begin(0, 0).await?;
    let (succeeded, mutations, responses) =
        match build_txn(&session, compares, success_ops, failure_ops).await {
            Ok(v) => v,
            Err(e) => {
                // 读/构造失败时服务端会话仍然存活（TTL 默认 1h），显式收口。
                let _ = session.abort().await;
                return Err(e);
            }
        };

    let revision = match session.commit_in_place(mutations, request_id).await {
        Ok(TxnCommit::Committed { commit_ts }) => {
            client.observe_commit_ts(commit_ts);
            commit_ts
        }
        Ok(TxnCommit::Conflict) => {
            let _ = session.abort().await;
            return Err(TxnAttempt::Conflict);
        }
        Err(e) => {
            let _ = session.abort().await;
            return Err(e.into());
        }
    };

    let responses = responses
        .into_iter()
        .map(|op| stamp_response(op, revision))
        .collect();
    Ok(TxnResponse {
        succeeded,
        responses,
        revision: revision as i64,
    })
}

async fn build_txn(
    session: &TxnV2Session,
    compares: &[Compare],
    success_ops: &[RequestOp],
    failure_ops: &[RequestOp],
) -> std::result::Result<(bool, Vec<txn_v2::Mutation>, Vec<ResponseOp>), TxnAttempt> {
    // 比较键去重后一次读齐：同一 key 的多个比较共享同一快照值。
    let mut compare_keys = BTreeSet::new();
    for cmp in compares {
        compare_keys.insert(cmp.key.clone());
    }
    let mut snapshot: BTreeMap<Vec<u8>, kv_v2::KeyValue> = BTreeMap::new();
    for key in &compare_keys {
        let (kvs, _) = session.read(key, &[], 1, false).await?;
        if let Some(kv) = kvs.into_iter().next() {
            snapshot.insert(key.clone(), kv);
        }
    }

    let mut succeeded = true;
    for cmp in compares {
        if !compare_matches(cmp, snapshot.get(&cmp.key))? {
            succeeded = false;
            break;
        }
    }

    let ops = if succeeded { success_ops } else { failure_ops };
    let (mutations, responses) = apply_ops(session, ops).await?;
    Ok((succeeded, mutations, responses))
}

/// 依次执行 v1 事务内的 op：写集累积为 `Mutation`，读 op 在快照 + 已执行的
/// 写集上求值（严格按 op 顺序，后写/后删覆盖先写）。
async fn apply_ops(
    session: &TxnV2Session,
    ops: &[RequestOp],
) -> std::result::Result<(Vec<txn_v2::Mutation>, Vec<ResponseOp>), TxnAttempt> {
    let mut mutations: Vec<txn_v2::Mutation> = Vec::new();
    let mut responses = Vec::with_capacity(ops.len());

    for op in ops {
        let Some(kind) = op.op.as_ref() else {
            return Err(TxnAttempt::Fatal(Error::InvalidArgument(
                "txn op has no payload".into(),
            )));
        };
        match kind {
            RequestOpKind::RequestPut(p) => {
                let prev = if p.prev_kv {
                    let (kvs, _) = session.read(&p.key, &[], 1, false).await?;
                    let rows = replay(&mutations, kvs, &p.key, &[]);
                    rows.into_iter().find(|kv| kv.key == p.key)
                } else {
                    None
                };
                mutations.push(txn_v2::Mutation {
                    op: MutationOp::Put as i32,
                    key: p.key.clone(),
                    range_end: Vec::new(),
                    value: p.value.clone(),
                    lease_id: p.lease_id,
                });
                responses.push(ResponseOp {
                    op: Some(ResponseOpKind::ResponsePut(PutResponse {
                        prev_kv: prev,
                        revision: 0, // commit 后统一填
                    })),
                });
            }
            RequestOpKind::RequestDelete(d) => {
                let range = normalize_range(&d.key, &d.range_end);
                let (kvs, _) = session
                    .read(
                        range.0,
                        range.1.as_deref().unwrap_or_default(),
                        0,
                        !d.prev_kv,
                    )
                    .await?;
                let rows = replay(
                    &mutations,
                    kvs,
                    range.0,
                    range.1.as_deref().unwrap_or_default(),
                );
                let deleted = rows.len() as i64;
                let prev_kvs = if d.prev_kv { rows.to_vec() } else { Vec::new() };
                mutations.push(delete_mutation(&range));
                responses.push(ResponseOp {
                    op: Some(ResponseOpKind::ResponseDelete(DeleteResponse {
                        deleted,
                        prev_kvs,
                        revision: 0,
                    })),
                });
            }
            RequestOpKind::RequestRange(r) => {
                if r.revision != 0 {
                    return Err(TxnAttempt::Fatal(Error::InvalidArgument(
                        "historical revision inside a txn is not supported on the v2 data plane"
                            .into(),
                    )));
                }
                let keys_only = r.keys_only || r.count_only;
                let (kvs, _) = session.read(&r.key, &r.range_end, 0, keys_only).await?;
                let mut rows = replay(&mutations, kvs, &r.key, &r.range_end);
                rows.sort_by(|a, b| a.key.cmp(&b.key));
                if r.count_only {
                    let count = rows.len() as i64;
                    responses.push(ResponseOp {
                        op: Some(ResponseOpKind::ResponseRange(RangeResponse {
                            kvs: Vec::new(),
                            count,
                            revision: 0,
                        })),
                    });
                } else {
                    if r.limit > 0 {
                        rows.truncate(r.limit as usize);
                    }
                    let count = rows.len() as i64;
                    responses.push(ResponseOp {
                        op: Some(ResponseOpKind::ResponseRange(RangeResponse {
                            kvs: rows,
                            count,
                            revision: 0,
                        })),
                    });
                }
            }
        }
    }
    Ok((mutations, responses))
}

/// 把已执行的写集按顺序回放到快照行上，返回请求区间内的可见行（v1 KeyValue）。
///
/// 回放而不是只查 overlay 表：`DeleteRange` 必须删掉快照里存在但本事务没有
/// 单独写过的 key；「先删后写」也必须让 key 重新出现。
fn replay(
    mutations: &[txn_v2::Mutation],
    snapshot: Vec<kv_v2::KeyValue>,
    key: &[u8],
    range_end: &[u8],
) -> Vec<KeyValue> {
    let point = range_end.is_empty() || range_end == key;
    let mut rows: BTreeMap<Vec<u8>, KeyValue> = snapshot
        .into_iter()
        .map(|kv| (kv.key.clone(), kv_to_v1(&kv)))
        .collect();

    for m in mutations {
        match MutationOp::try_from(m.op).ok() {
            Some(MutationOp::Put) => {
                rows.insert(
                    m.key.clone(),
                    KeyValue {
                        key: m.key.clone(),
                        value: m.value.clone(),
                        create_revision: 0,
                        mod_revision: 0,
                        version: 0,
                        lease_id: m.lease_id,
                    },
                );
            }
            Some(MutationOp::Delete) => {
                rows.remove(&m.key);
            }
            Some(MutationOp::DeleteRange) => {
                rows.retain(|k, _| {
                    !(k.as_slice() >= m.key.as_slice() && k.as_slice() < m.range_end.as_slice())
                });
            }
            Some(MutationOp::Unspecified) | None => {}
        }
    }

    rows.into_values()
        .filter(|kv| {
            if point {
                kv.key == key
            } else {
                kv.key.as_slice() >= key && kv.key.as_slice() < range_end
            }
        })
        .collect()
}

async fn commit_write(
    client: &Client,
    mutations: Vec<txn_v2::Mutation>,
    request_id: &[u8],
) -> Result<u64> {
    let session = client.txn_v2().begin(0, 0).await?;
    match session.commit_in_place(mutations, request_id).await {
        Ok(TxnCommit::Committed { commit_ts }) => Ok(commit_ts),
        Ok(TxnCommit::Conflict) => {
            let _ = session.abort().await;
            Err(conflict_exhausted())
        }
        Err(e) => {
            let _ = session.abort().await;
            Err(e)
        }
    }
}

fn conflict_exhausted() -> Error {
    Error::V2 {
        code: CoordErrorCode::TxnConflict,
        message: "v2 transaction kept conflicting after the configured retries".into(),
    }
}

/// `(key, Some(range_end))` = 半开区间；`(key, None)` = 单键。
type RangeSpec<'a> = (&'a [u8], Option<Vec<u8>>);

fn normalize_range<'a>(key: &'a [u8], range_end: &'a [u8]) -> RangeSpec<'a> {
    if range_end.is_empty() || range_end == key {
        (key, None)
    } else {
        (key, Some(range_end.to_vec()))
    }
}

fn delete_mutation((key, range_end): &RangeSpec<'_>) -> txn_v2::Mutation {
    match range_end {
        Some(end) => txn_v2::Mutation {
            op: MutationOp::DeleteRange as i32,
            key: key.to_vec(),
            range_end: end.clone(),
            value: Vec::new(),
            lease_id: 0,
        },
        None => txn_v2::Mutation {
            op: MutationOp::Delete as i32,
            key: key.to_vec(),
            range_end: Vec::new(),
            value: Vec::new(),
            lease_id: 0,
        },
    }
}

/// commit 之后把响应里的 revision 统一填成同一个 `commit_ts`（v1 的事务批 revision）。
fn stamp_response(mut op: ResponseOp, revision: u64) -> ResponseOp {
    match op.op.as_mut() {
        Some(ResponseOpKind::ResponsePut(p)) => p.revision = revision as i64,
        Some(ResponseOpKind::ResponseDelete(d)) => d.revision = revision as i64,
        Some(ResponseOpKind::ResponseRange(r)) => r.revision = revision as i64,
        None => {}
    }
    op
}

fn kv_to_v1(kv: &kv_v2::KeyValue) -> KeyValue {
    KeyValue {
        key: kv.key.clone(),
        value: kv.value.clone(),
        create_revision: kv.create_revision,
        mod_revision: kv.mod_revision,
        version: kv.version,
        lease_id: kv.lease_id,
    }
}

fn compare_matches(cmp: &Compare, actual: Option<&kv_v2::KeyValue>) -> Result<bool> {
    let target_value = cmp
        .target_value
        .as_ref()
        .ok_or_else(|| Error::InvalidArgument("txn compare has no target value".into()))?;
    let result = CompareResult::try_from(cmp.result)
        .map_err(|_| Error::InvalidArgument(format!("invalid compare result {}", cmp.result)))?;
    let target = Target::try_from(cmp.target)
        .map_err(|_| Error::InvalidArgument(format!("invalid compare target {}", cmp.target)))?;

    let ordering = match (target, target_value) {
        (Target::Value, TargetValue::Value(expected)) => {
            let actual_value: &[u8] = actual.map(|kv| kv.value.as_slice()).unwrap_or_default();
            actual_value.cmp(expected.as_slice())
        }
        (Target::Version, TargetValue::Version(expected)) => {
            actual.map(|kv| kv.version).unwrap_or(0).cmp(expected)
        }
        (Target::ModRev, TargetValue::ModRevision(expected)) => {
            actual.map(|kv| kv.mod_revision).unwrap_or(0).cmp(expected)
        }
        _ => {
            return Err(Error::InvalidArgument(
                "txn compare target/value variant mismatch".into(),
            ))
        }
    };

    let ok = match result {
        CompareResult::Equal => ordering.is_eq(),
        CompareResult::NotEqual => ordering.is_ne(),
        CompareResult::Greater => ordering.is_gt(),
        CompareResult::Less => ordering.is_lt(),
    };
    Ok(ok)
}

/// v1 前缀订阅 → v2 半开区间。
///
/// v1 的空 `range_end` 是**前缀**匹配；v2 的空 `range_end` 是单键。空 key
/// 表示全 keyspace（v2 用 `key="" & range_end=""` 表达同一件事）；前缀没有
/// 字典序后继时 fail-closed，不退化成单键或无界扫描。
fn watch_range(key: &[u8], range_end: &[u8]) -> Result<Vec<u8>> {
    if !range_end.is_empty() {
        return Ok(range_end.to_vec());
    }
    if key.is_empty() {
        return Ok(Vec::new());
    }
    prefix_successor(key).ok_or_else(|| {
        Error::InvalidArgument(
            "watch prefix has no lexicographic successor; cannot map it onto the v2 range".into(),
        )
    })
}

/// v1 形状的 Watch → v2 订阅 + 事件转换。
pub(crate) async fn watch(
    client: &Client,
    key: &[u8],
    range_end: &[u8],
    start_revision: i64,
    prev_kv: bool,
) -> Result<mpsc::Receiver<Result<WatchEvent>>> {
    let start_revision = u64::try_from(start_revision)
        .map_err(|_| Error::InvalidArgument("start_revision must be >= 0".into()))?;
    let end = watch_range(key, range_end)?;
    let mut options = crate::watch_v2::WatchV2Options::new(key.to_vec())
        .with_range(end)
        .with_prev_kv(prev_kv);
    if start_revision > 0 {
        options = options.from_revision(start_revision);
    }
    let mut subscription = client.watch_v2().watch(options).await?;

    let (tx, rx) = mpsc::channel::<Result<WatchEvent>>(256);
    tokio::spawn(async move {
        while let Some(batch) = subscription.recv().await {
            match batch {
                Ok(batch) => {
                    for event in batch.events {
                        for v1_event in v1_events(event, prev_kv) {
                            if tx.send(Ok(v1_event)).await.is_err() {
                                return;
                            }
                        }
                    }
                }
                Err(Error::HistoryUnavailable { .. }) => {
                    let _ = tx
                        .send(Ok(WatchEvent {
                            r#type: EventType::HistoryUnavailable as i32,
                            kvs: Vec::new(),
                            prev_kv: None,
                            revision: 0,
                        }))
                        .await;
                    return;
                }
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            }
        }
        let _ = tx
            .send(Err(Error::Internal("v2 watch subscription ended".into())))
            .await;
    });
    Ok(rx)
}

fn v1_events(event: crate::watch_v2::WatchV2Event, prev_kv: bool) -> Vec<WatchEvent> {
    if prev_kv {
        // v1 事件只承载一个 prev_kv：逐 change 下发，保住每个旧值。
        return event
            .changes
            .into_iter()
            .map(|change| {
                let value = change.value.clone().unwrap_or_default();
                WatchEvent {
                    r#type: if change.value.is_some() {
                        EventType::Put as i32
                    } else {
                        EventType::Delete as i32
                    },
                    kvs: vec![KeyValue {
                        key: change.key,
                        value,
                        create_revision: 0,
                        mod_revision: event.commit_ts as i64,
                        version: 0,
                        lease_id: 0,
                    }],
                    prev_kv: change.prev.as_ref().map(kv_to_v1),
                    revision: event.commit_ts as i64,
                }
            })
            .collect();
    }

    // v1 的一个事务事件只有一个类型：混合批按 v1 既有口径记 PUT（删除项 value 为空）。
    let delete_only = event.changes.iter().all(|c| c.value.is_none());
    let kvs = event
        .changes
        .into_iter()
        .map(|change| KeyValue {
            key: change.key,
            value: change.value.unwrap_or_default(),
            create_revision: 0,
            mod_revision: event.commit_ts as i64,
            version: 0,
            lease_id: 0,
        })
        .collect();
    vec![WatchEvent {
        r#type: if delete_only {
            EventType::Delete as i32
        } else {
            EventType::Put as i32
        },
        kvs,
        prev_kv: None,
        revision: event.commit_ts as i64,
    }]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watch_range_maps_prefix_to_successor() {
        assert_eq!(
            watch_range(b"/_registry/services/", b"").unwrap(),
            b"/_registry/services0"
        );
        assert_eq!(watch_range(b"", b"").unwrap(), Vec::<u8>::new());
        assert_eq!(watch_range(b"a", b"z").unwrap(), b"z".to_vec());
        assert!(watch_range(&[0xFF, 0xFF], b"").is_err());
    }

    #[test]
    fn compare_matches_absent_key_uses_zero_metadata() {
        let cmp = |target, value| Compare {
            result: CompareResult::Equal as i32,
            target,
            key: b"k".to_vec(),
            target_value: Some(value),
        };
        assert!(
            compare_matches(&cmp(Target::Version as i32, TargetValue::Version(0)), None).unwrap()
        );
        assert!(compare_matches(
            &cmp(Target::ModRev as i32, TargetValue::ModRevision(0)),
            None
        )
        .unwrap());
        assert!(compare_matches(
            &cmp(Target::Value as i32, TargetValue::Value(Vec::new())),
            None
        )
        .unwrap());
        assert!(
            !compare_matches(&cmp(Target::Version as i32, TargetValue::Version(1)), None).unwrap()
        );
    }
}
