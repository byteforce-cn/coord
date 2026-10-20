//! `coord.<domain>.v2` 的 tonic 服务壳（ADR-0015 G0）。
//!
//! 每个 handler 先过 [`KvTState::gate`]：关闭或能力未交付 → `FAILED_PRECONDITION`。
//! 门禁通过但 handler 尚未实现 → `UNIMPLEMENTED`（这是实现进度问题，不是配置
//! 问题；把两者混成同一个码会让运维无法判断该改配置还是该升级版本）。
//!
//! 服务**始终注册**（只要对应的 tonic server 被挂到 gRPC builder 上）：关闭时
//! reflection 仍然可见，便于客户端在连接阶段就发现"这个集群没有 KV-T"，而不是
//! 拿到 `UNAVAILABLE` 后误判为网络故障。

use std::time::Duration;

use futures::{StreamExt, TryStreamExt};
use tonic::{Request, Response, Status, Streaming};

use coord_proto::kv::v2 as kv_v2;
use coord_proto::lease::v2 as lease_v2;
use coord_proto::storage::v2 as storage_v2;
use coord_proto::tso::v2 as tso_v2;
use coord_proto::txn::v2 as txn_v2;
use coord_proto::watch::v2 as watch_v2;

use coord_core::error::Error;
use coord_core::error_code::{attach, CoordErrorCode};
use coord_core::kv_range::RangeSemantics;

use super::lease as lease_rpc;
use super::txn::{ReadIntent, ReadSetLimit, SessionError};
use super::KvTCapability;
use crate::raft::tso::{normalize_batch_size, TsoAllocator, TsoBatch};
use crate::raft::type_config::{Command, Response as RaftResponse};
use crate::server::{map_err, CoordNode, KvRangeSegment};

/// 门禁 + 未实现哨兵。
fn shell(capability: KvTCapability, node: &CoordNode) -> Status {
    match node.kvt().gate(capability) {
        Err(status) => status,
        Ok(()) => Status::unimplemented(format!(
            "{} gate passed but the handler is not implemented yet",
            capability.package()
        )),
    }
}

type ShellStream<T> = tokio_stream::Empty<Result<T, Status>>;

// ──── coord.tso.v2 ────

/// TSO 分配链路的错误映射：先结构化分类（NOT_LEADER / INVALID_ARGUMENT /
/// UNAVAILABLE），再交给通用 `map_err` 脱敏。
pub(super) fn tso_status(err: Error) -> Status {
    let code = match &err {
        Error::NotLeader { .. } | Error::NotLeaderNoHint => Some(CoordErrorCode::NotLeader),
        Error::InvalidArgument(_) => Some(CoordErrorCode::InvalidArgument),
        Error::ClusterUnavailable(_) => Some(CoordErrorCode::Unavailable),
        _ => None,
    };
    let status = crate::server::map_err(err);
    match code {
        Some(code) => attach(status, code),
        None => status,
    }
}

/// 经 region 0 raft 申请批次。无 raft（如未装配的单节点进程）→ 配置/装配错误
/// （FAILED_PRECONDITION），不是网络故障。
async fn allocate_tso(node: &CoordNode, batch_size: u32) -> Result<TsoBatch, Status> {
    let Some(raft) = node.raft.as_ref() else {
        return Err(attach(
            Status::failed_precondition(
                "kv_t requires a region 0 raft to allocate TSO (multi_raft assembly missing)",
            ),
            CoordErrorCode::FailedPrecondition,
        ));
    };
    let allocator = TsoAllocator::new(raft.as_ref().clone(), node.node_id);
    allocator.allocate(batch_size).await.map_err(tso_status)
}

fn ts_to_i64(ts: u64) -> Result<i64, Status> {
    i64::try_from(ts).map_err(|_| Status::internal("TSO exceeds i64 wire range"))
}

#[tonic::async_trait]
impl tso_v2::tso_server::Tso for CoordNode {
    async fn allocate(
        &self,
        request: Request<tso_v2::AllocateRequest>,
    ) -> Result<Response<tso_v2::AllocateResponse>, Status> {
        self.kvt().gate(KvTCapability::Tso)?;
        let max = self.kvt().config().tso_batch_size.min(65_536);
        let batch_size =
            normalize_batch_size(request.into_inner().batch_size, max).map_err(tso_status)?;
        let batch = allocate_tso(self, batch_size).await?;
        Ok(Response::new(tso_v2::AllocateResponse {
            start_ts: ts_to_i64(batch.start_ts)?,
            end_ts: ts_to_i64(batch.end_ts)?,
        }))
    }

    /// `GetTs` 从本地批次缓存发放（一次 raft 申请一批）；follower 拒绝。
    async fn get_ts(
        &self,
        _request: Request<tso_v2::GetTsRequest>,
    ) -> Result<Response<tso_v2::GetTsResponse>, Status> {
        self.kvt().gate(KvTCapability::Tso)?;
        let client = self.tso_client().ok_or_else(|| {
            attach(
                Status::failed_precondition(
                    "kv_t requires a region 0 raft to allocate TSO \
                     (region 0 raft / tso client assembly missing)",
                ),
                CoordErrorCode::FailedPrecondition,
            )
        })?;
        let ts = client
            .get_ts_above(self.safe_ts_fence())
            .await
            .map_err(tso_status)?;
        Ok(Response::new(tso_v2::GetTsResponse { ts: ts_to_i64(ts)? }))
    }
}

// ──── coord.kv.v2 ────

/// 结构化错误映射：v2 调用方只按 trailer 码分支，不得解析消息文本。
fn v2_status(err: Error) -> Status {
    let code = match &err {
        Error::InvalidArgument(_) => Some(CoordErrorCode::InvalidArgument),
        Error::RevisionCompacted { .. } | Error::HistoryUnavailable { .. } => {
            Some(CoordErrorCode::OutOfRange)
        }
        Error::CommitRejected { .. } => Some(CoordErrorCode::CommitRejected),
        _ => None,
    };
    let status = crate::server::map_err(err);
    match code {
        Some(code) => attach(status, code),
        None => status,
    }
}

fn require_positive_ts(name: &str, ts: i64) -> Result<u64, Status> {
    if ts <= 0 {
        return Err(attach(
            Status::invalid_argument(format!("{name} must be > 0")),
            CoordErrorCode::InvalidArgument,
        ));
    }
    Ok(ts as u64)
}

fn to_kv_v2(
    key: &[u8],
    value: &[u8],
    meta: Option<&crate::storage::mvcc::KvMetadata>,
) -> kv_v2::KeyValue {
    match meta {
        Some(m) => kv_v2::KeyValue {
            key: key.to_vec(),
            value: value.to_vec(),
            create_revision: m.create_revision,
            mod_revision: m.mod_revision,
            version: m.version,
            lease_id: m.lease_id,
        },
        None => kv_v2::KeyValue {
            key: key.to_vec(),
            value: value.to_vec(),
            create_revision: 0,
            mod_revision: 0,
            version: 1,
            lease_id: 0,
        },
    }
}

/// v2 / v1 KeyValue 字段一一对应；幂等缓存复用 v1 容器，转换必须无信息损失。
fn kv_v2_to_v1(kv: &kv_v2::KeyValue) -> coord_proto::kv::KeyValue {
    coord_proto::kv::KeyValue {
        key: kv.key.clone(),
        value: kv.value.clone(),
        create_revision: kv.create_revision,
        mod_revision: kv.mod_revision,
        version: kv.version,
        lease_id: kv.lease_id,
    }
}

fn kv_v1_to_v2(kv: &coord_proto::kv::KeyValue) -> kv_v2::KeyValue {
    kv_v2::KeyValue {
        key: kv.key.clone(),
        value: kv.value.clone(),
        create_revision: kv.create_revision,
        mod_revision: kv.mod_revision,
        version: kv.version,
        lease_id: kv.lease_id,
    }
}

fn record_commit_rejected(node: &CoordNode) {
    if let Some(metrics) = node.metrics() {
        metrics.inc_kv_v2_commit_rejected();
    }
}

/// 单键半开区间上界：`key + 0x00` 恰好只包含 `key` 自身（长度前缀序）。
fn single_key_end(key: &[u8]) -> Vec<u8> {
    let mut end = Vec::with_capacity(key.len() + 1);
    end.extend_from_slice(key);
    end.push(0);
    end
}

/// 等待**全局** safe_ts 追到 `read_ts`；超时返回可重试的 `SAFE_TS_LAG`。
///
/// 必须等全部 Region 的 apply 水位，而不是只看目标 Region：多 Region 下
/// `commit_ts <= read_ts` 的事务可能已落到某个领先 Region，落后 Region 尚未
/// apply；只看目标会让同一快照看到部分提交。
async fn wait_for_safe_ts(node: &CoordNode, read_ts: u64, wait_ms: u64) -> Result<(), Status> {
    let started = tokio::time::Instant::now();
    let deadline = started + Duration::from_millis(wait_ms);
    loop {
        if node.safe_ts() >= read_ts {
            if let Some(metrics) = node.metrics() {
                metrics.inc_kv_v2_safe_ts_wait(started.elapsed().as_millis() as u64);
            }
            return Ok(());
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            if let Some(metrics) = node.metrics() {
                metrics.inc_kv_v2_safe_ts_wait(started.elapsed().as_millis() as u64);
                metrics.inc_kv_v2_safe_ts_lag();
            }
            return Err(attach(
                Status::unavailable(format!(
                    "read_ts {read_ts} exceeds safe_ts (global apply lag); retry later"
                )),
                CoordErrorCode::SafeTsLag,
            ));
        }
        tokio::time::sleep((deadline - now).min(Duration::from_millis(10))).await;
    }
}

/// 单次 Range 同时在飞的 Region 分段数上限。
///
/// 分段读取是网络 + ReadIndex RPC；不设上界时一个大范围会把所有 Region 同时
/// 打到饱和。`buffered` 保持分段顺序，因此合并结果仍按 key 有序。
const RANGE_FANOUT_CONCURRENCY: usize = 8;

/// 读取一个 Region 分段在 `read_ts` 上的精确快照。
///
/// 快照读不需要 ReadIndex 屏障：请求级等待已保证本节点**所有**数据 Region 的
/// applied revision >= `read_ts`（`read_ts=0` 解析为本地 safe_ts，同样 <= 最小
/// applied），本地副本足以重建该快照；压缩水位随 applied revision 前进，
/// 历史在保留窗口内可回放。分段再复核一次 applied 水位：不足即
/// `SAFE_TS_LAG`（可重试），不得把「当前态」当成未来 revision 的快照返回。
/// `read_ts == current` 时走实时读，但若读事务的 revision 已越过 `read_ts`
/// 则退回精确历史视图，保证返回行 `mod_revision <= read_ts`。
async fn read_kv_range_segment(
    seg: KvRangeSegment,
    read_ts: u64,
    scan_limit: usize,
) -> Result<Vec<kv_v2::KeyValue>, Status> {
    let target = &seg.target;
    let current_now = target.mvcc.current_revision();
    if read_ts > current_now {
        return Err(attach(
            Status::unavailable(format!(
                "read_ts {read_ts} exceeds local applied revision {current_now} of region {}; \
                 retry later",
                target.region_id
            )),
            CoordErrorCode::SafeTsLag,
        ));
    }
    let rows = if read_ts == 0 {
        // 0 是“早于任何 commit”的合法快照（safe_ts 也可能为 0）；
        // changelog 的 0 水位会被当成已压缩，不能走历史读。
        Vec::new()
    } else if read_ts == current_now {
        let (rows, live_revision) = target
            .mvcc
            .range_in_with_metadata(&seg.start, &seg.end, scan_limit)
            .map_err(map_err)?;
        if live_revision > read_ts {
            target
                .mvcc
                .range_at_revision_with_metadata(&seg.start, &seg.end, scan_limit, read_ts)
                .map_err(v2_status)?
        } else {
            rows
        }
    } else {
        target
            .mvcc
            .range_at_revision_with_metadata(&seg.start, &seg.end, scan_limit, read_ts)
            .map_err(v2_status)?
    };

    Ok(rows
        .into_iter()
        .map(|(key, value, meta)| to_kv_v2(&key, &value, Some(&meta)))
        .collect())
}

#[tonic::async_trait]
impl kv_v2::kv_server::Kv for CoordNode {
    async fn put(
        &self,
        request: Request<kv_v2::PutRequest>,
    ) -> Result<Response<kv_v2::PutResponse>, Status> {
        self.kvt().gate(KvTCapability::Kv)?;
        self.ensure_writable()?;

        let metadata = request.metadata().clone();
        let req = request.into_inner();
        let commit_ts = require_positive_ts("commit_ts", req.commit_ts)?;
        self.guard_object_write_key(&req.key)?;

        // 幂等重放必须先于租约存活校验：原请求成功后租约可能已 revoke，
        // 用同一 request_id 重试应回放首次结果，而不是变成 FAILED_PRECONDITION。
        let idem_key = (!req.request_id.is_empty())
            .then(|| crate::server::v2_idempotency_key(b"put", &metadata, &req.request_id));
        if let Some(key) = &idem_key {
            if let Some(entry) = self.check_idempotent_entry(key) {
                return Ok(Response::new(kv_v2::PutResponse {
                    prev_kv: entry.prev_kv.as_ref().map(kv_v1_to_v2),
                    commit_ts: entry.revision,
                }));
            }
        }

        // 绑定前 fail-closed：未知/已吊销/已过期租约不得写入新 key（否则
        // revoke 之后写入的数据会失去清理语义）。
        let lease_id = if req.lease_id != 0 {
            lease_rpc::require_live_lease(self, req.lease_id)?;
            Some(req.lease_id)
        } else {
            None
        };

        let target = self.kv_target_for_key(&req.key)?;
        self.guard_key_in_target(&target, &req.key)?;

        let prev_kv = if req.prev_kv {
            // prev_kv 必须来自已 apply 到 commit index 的视图；否则 leader 本地
            // 落后时会把旧值当“当前值”返回。
            self.ensure_linearizable_on(
                target.raft.as_deref(),
                target.raft_log_store.as_ref(),
                target.region_id,
            )
            .await?;
            target
                .mvcc
                .get(&req.key)
                .map_err(map_err)?
                .map(|prev_value| {
                    let meta = target.mvcc.get_kv_metadata(&req.key).map_err(map_err)?;
                    Ok::<_, Status>(to_kv_v2(&req.key, &prev_value, meta.as_ref()))
                })
                .transpose()?
        } else {
            None
        };

        if let Some(raft) = &target.raft {
            let cmd = Command::PutV2 {
                key: req.key.clone(),
                value: req.value.clone(),
                lease_id,
                commit_ts,
            };
            let resp = self
                .client_write_with_timeout(raft, cmd, target.region_id)
                .await?;
            match resp.response() {
                RaftResponse::Put { revision } => {
                    if *revision != commit_ts {
                        return Err(Status::internal(format!(
                            "raft applied commit_ts {revision}, expected {commit_ts}"
                        )));
                    }
                }
                RaftResponse::CommitRejected {
                    commit_ts,
                    current_revision,
                } => {
                    record_commit_rejected(self);
                    return Err(v2_status(Error::CommitRejected {
                        commit_ts: *commit_ts,
                        current_revision: *current_revision,
                    }));
                }
                other => {
                    return Err(Status::internal(format!(
                        "unexpected raft response for PutV2: {other:?}"
                    )))
                }
            }
        } else {
            target
                .mvcc
                .put_at_commit_ts(
                    &req.key,
                    &req.value,
                    lease_id,
                    commit_ts,
                    crate::storage::mvcc::AppliedLogId::standalone(commit_ts),
                )
                .map_err(|err| {
                    if matches!(err, Error::CommitRejected { .. }) {
                        record_commit_rejected(self);
                    }
                    v2_status(err)
                })?;
        }

        if let Some(key) = idem_key {
            self.cache_idempotent_put(key, commit_ts as i64, prev_kv.as_ref().map(kv_v2_to_v1));
        }

        Ok(Response::new(kv_v2::PutResponse {
            prev_kv,
            commit_ts: commit_ts as i64,
        }))
    }

    async fn range(
        &self,
        request: Request<kv_v2::RangeRequest>,
    ) -> Result<Response<kv_v2::RangeResponse>, Status> {
        self.kvt().gate(KvTCapability::Kv)?;
        let req = request.into_inner();
        if req.read_ts < 0 {
            return Err(attach(
                Status::invalid_argument("read_ts must be >= 0"),
                CoordErrorCode::InvalidArgument,
            ));
        }

        let max_range = self.kvt().config().max_range as usize;
        let limit = if req.limit < 0 {
            return Err(attach(
                Status::invalid_argument("limit must be >= 0"),
                CoordErrorCode::InvalidArgument,
            ));
        } else if req.limit == 0 {
            max_range
        } else {
            let requested = req.limit as usize;
            if max_range > 0 && requested > max_range {
                return Err(attach(
                    Status::invalid_argument(format!(
                        "range limit {requested} exceeds max {max_range}"
                    )),
                    CoordErrorCode::ReadSetTooLarge,
                ));
            }
            requested
        };

        let single_key = RangeSemantics::of(&req.key, &req.range_end).is_single_key();
        let end = if single_key {
            single_key_end(&req.key)
        } else {
            req.range_end.clone()
        };

        let safe_ts = self.safe_ts();
        let read_ts = if req.read_ts == 0 {
            safe_ts
        } else {
            req.read_ts as u64
        };
        if read_ts > safe_ts {
            wait_for_safe_ts(self, read_ts, self.kvt().config().read_ts_wait_ms).await?;
        }

        // 空区间（非单键且 end <= key）：不路由、不读，但仍回显已解析的 read_ts。
        if !single_key && end <= req.key {
            return Ok(Response::new(kv_v2::RangeResponse {
                kvs: Vec::new(),
                count: 0,
                read_ts: read_ts as i64,
            }));
        }

        let scan_limit = if req.count_only {
            if max_range == 0 {
                0
            } else {
                max_range.saturating_add(1)
            }
        } else {
            limit
        };

        let segments = self.kv_range_targets(&req.key, &end)?;
        let reads = segments
            .into_iter()
            .map(|seg| read_kv_range_segment(seg, read_ts, scan_limit));
        let mut parts = futures::stream::iter(reads).buffered(RANGE_FANOUT_CONCURRENCY);

        let mut kvs: Vec<kv_v2::KeyValue> = Vec::new();
        let mut total = 0usize;
        while let Some(part) = parts.try_next().await? {
            let mut part = part;
            if self.object_enabled() {
                part.retain(|kv| !crate::storage::object_store::key_in_object_space(&kv.key));
            }
            if req.count_only {
                // count_only 只需要总数：逐段累加并及时失败，不把所有行留在内存。
                total = total.saturating_add(part.len());
                if max_range > 0 && total > max_range {
                    return Err(attach(
                        Status::invalid_argument(format!(
                            "range scan exceeds max {max_range}; narrow the range"
                        )),
                        CoordErrorCode::ReadSetTooLarge,
                    ));
                }
            } else {
                kvs.extend(part);
                if limit > 0 && kvs.len() >= limit {
                    // 分段按 key 有序：已拿到全局前 limit 条，后续分段无需再读。
                    kvs.truncate(limit);
                    break;
                }
            }
        }
        drop(parts);

        let count = if req.count_only {
            total as i64
        } else {
            kvs.len() as i64
        };
        if req.keys_only {
            for kv in &mut kvs {
                kv.value.clear();
            }
        }
        if req.count_only {
            kvs.clear();
        }

        Ok(Response::new(kv_v2::RangeResponse {
            kvs,
            count,
            read_ts: read_ts as i64,
        }))
    }

    async fn delete(
        &self,
        request: Request<kv_v2::DeleteRequest>,
    ) -> Result<Response<kv_v2::DeleteResponse>, Status> {
        self.kvt().gate(KvTCapability::Kv)?;
        self.ensure_writable()?;

        let metadata = request.metadata().clone();
        let req = request.into_inner();
        let commit_ts = require_positive_ts("commit_ts", req.commit_ts)?;
        let is_range = RangeSemantics::of(&req.key, &req.range_end).is_interval();
        if is_range {
            self.guard_object_write_range(&req.key, &req.range_end)?;
        } else {
            self.guard_object_write_key(&req.key)?;
        }

        let idem_key = (!req.request_id.is_empty())
            .then(|| crate::server::v2_idempotency_key(b"delete", &metadata, &req.request_id));
        if let Some(key) = &idem_key {
            if let Some(entry) = self.check_idempotent_entry(key) {
                return Ok(Response::new(kv_v2::DeleteResponse {
                    deleted: entry.deleted,
                    prev_kvs: entry.prev_kvs.iter().map(kv_v1_to_v2).collect(),
                    commit_ts: entry.revision,
                }));
            }
        }

        let target = self.kv_target_for_key(&req.key)?;
        if is_range {
            self.guard_range_in_target(&target, &req.key, &req.range_end)?;
        } else {
            self.guard_key_in_target(&target, &req.key)?;
        }

        // deleted/prev_kvs 由本地视图计算，必须先确认本地 apply 已覆盖已提交日志。
        self.ensure_linearizable_on(
            target.raft.as_deref(),
            target.raft_log_store.as_ref(),
            target.region_id,
        )
        .await?;
        let max_range = self.kvt().config().max_range as usize;
        let live = if is_range {
            None
        } else {
            target.mvcc.get(&req.key).map_err(map_err)?
        };
        let prev_kvs: Vec<kv_v2::KeyValue> = if req.prev_kv {
            if is_range {
                target
                    .mvcc
                    .range_in(&req.key, &req.range_end, max_range)
                    .map_err(map_err)?
                    .into_iter()
                    .map(|(k, v)| {
                        let meta = target.mvcc.get_kv_metadata(&k).ok().flatten();
                        to_kv_v2(&k, &v, meta.as_ref())
                    })
                    .collect()
            } else {
                live.iter()
                    .map(|value| {
                        let meta = target.mvcc.get_kv_metadata(&req.key).ok().flatten();
                        to_kv_v2(&req.key, value, meta.as_ref())
                    })
                    .collect()
            }
        } else {
            Vec::new()
        };

        let (deleted, revision): (i64, u64) = if let Some(raft) = &target.raft {
            let cmd = if is_range {
                Command::DeleteRangeV2 {
                    key: req.key.clone(),
                    range_end: req.range_end.clone(),
                    commit_ts,
                }
            } else {
                Command::DeleteV2 {
                    key: req.key.clone(),
                    commit_ts,
                }
            };
            let resp = self
                .client_write_with_timeout(raft, cmd, target.region_id)
                .await?;
            match resp.response() {
                RaftResponse::Delete { revision } => {
                    if *revision != commit_ts {
                        return Err(Status::internal(format!(
                            "raft applied commit_ts {revision}, expected {commit_ts}"
                        )));
                    }
                    (if live.is_some() { 1 } else { 0 }, *revision)
                }
                RaftResponse::DeleteRange { revision, deleted } => {
                    if *revision != commit_ts {
                        return Err(Status::internal(format!(
                            "raft applied commit_ts {revision}, expected {commit_ts}"
                        )));
                    }
                    (*deleted as i64, *revision)
                }
                RaftResponse::CommitRejected {
                    commit_ts,
                    current_revision,
                } => {
                    record_commit_rejected(self);
                    return Err(v2_status(Error::CommitRejected {
                        commit_ts: *commit_ts,
                        current_revision: *current_revision,
                    }));
                }
                other => {
                    return Err(Status::internal(format!(
                        "unexpected raft response for DeleteV2: {other:?}"
                    )))
                }
            }
        } else if is_range {
            let outcome = target
                .mvcc
                .delete_range_at_commit_ts(
                    &req.key,
                    &req.range_end,
                    commit_ts,
                    crate::storage::mvcc::AppliedLogId::standalone(commit_ts),
                )
                .map_err(|err| {
                    if matches!(err, Error::CommitRejected { .. }) {
                        record_commit_rejected(self);
                    }
                    v2_status(err)
                })?;
            (outcome.deleted_keys.len() as i64, commit_ts)
        } else {
            target
                .mvcc
                .delete_at_commit_ts(
                    &req.key,
                    commit_ts,
                    crate::storage::mvcc::AppliedLogId::standalone(commit_ts),
                )
                .map_err(|err| {
                    if matches!(err, Error::CommitRejected { .. }) {
                        record_commit_rejected(self);
                    }
                    v2_status(err)
                })?;
            (if live.is_some() { 1 } else { 0 }, commit_ts)
        };

        if let Some(key) = idem_key {
            self.cache_idempotent_delete(
                key,
                deleted,
                prev_kvs.iter().map(kv_v2_to_v1).collect(),
                revision as i64,
            );
        }

        Ok(Response::new(kv_v2::DeleteResponse {
            deleted,
            prev_kvs,
            commit_ts: revision as i64,
        }))
    }
}

// ──── coord.txn.v2 ────

/// 会话错误的线上映射：调用方只按 trailer 码分支（D12.5）。
fn txn_session_status(err: SessionError) -> Status {
    match err {
        SessionError::NotFound => attach(
            Status::aborted("transaction session not found or expired; restart the transaction"),
            CoordErrorCode::TxnNotFound,
        ),
        SessionError::ReadSetLimit(ReadSetLimit::Entries) => attach(
            Status::invalid_argument(
                "transaction read set exceeds max_read_set; narrow the reads or split the transaction",
            ),
            CoordErrorCode::ReadSetTooLarge,
        ),
        SessionError::ReadSetLimit(ReadSetLimit::RangeKeys) => attach(
            Status::invalid_argument(
                "transaction range scan exceeds max_range keys; narrow the range or split the transaction",
            ),
            CoordErrorCode::ReadSetTooLarge,
        ),
        SessionError::Draining => attach(
            Status::unavailable("key range is draining for split; retry with backoff"),
            CoordErrorCode::Unavailable,
        ),
    }
}

#[tonic::async_trait]
impl txn_v2::txn_server::Txn for CoordNode {
    async fn begin(
        &self,
        request: Request<txn_v2::BeginRequest>,
    ) -> Result<Response<txn_v2::BeginResponse>, Status> {
        self.kvt().gate(KvTCapability::Txn)?;
        let req = request.into_inner();
        if req.start_ts < 0 {
            return Err(attach(
                Status::invalid_argument("start_ts must be >= 0"),
                CoordErrorCode::InvalidArgument,
            ));
        }

        let start_ts = if req.start_ts == 0 {
            let client = self.tso_client().ok_or_else(|| {
                attach(
                    Status::failed_precondition(
                        "coord.txn.v2 Begin with start_ts=0 requires the local TSO client \
                         (region 0 raft / tso client assembly missing)",
                    ),
                    CoordErrorCode::FailedPrecondition,
                )
            })?;
            client
                .get_ts_above(self.safe_ts_fence())
                .await
                .map_err(tso_status)?
        } else {
            req.start_ts as u64
        };

        // 事务在 start_ts 上的快照必须已经可服务：本节点所有数据 Region 都
        // apply 到 start_ts 后，读才不会漏掉 commit_ts < start_ts 的已决策事务。
        if start_ts > self.safe_ts() {
            wait_for_safe_ts(self, start_ts, self.kvt().config().read_ts_wait_ms).await?;
        }

        let default_ms = self.kvt().config().txn_ttl_secs.saturating_mul(1000);
        let ttl_ms = if req.ttl_ms == 0 {
            default_ms
        } else {
            u64::from(req.ttl_ms).min(default_ms)
        };
        let txn_id = self
            .txn_sessions()
            .begin(start_ts, Duration::from_millis(ttl_ms));
        Ok(Response::new(txn_v2::BeginResponse {
            txn_id,
            start_ts: ts_to_i64(start_ts)?,
        }))
    }

    async fn read(
        &self,
        request: Request<txn_v2::ReadRequest>,
    ) -> Result<Response<txn_v2::ReadResponse>, Status> {
        self.kvt().gate(KvTCapability::Txn)?;
        let req = request.into_inner();
        if req.limit < 0 {
            return Err(attach(
                Status::invalid_argument("limit must be >= 0"),
                CoordErrorCode::InvalidArgument,
            ));
        }

        let config = self.kvt().config();
        let max_range = config.max_range as usize;
        let max_read_set = config.max_read_set as usize;

        // 与 coord.kv.v2.Range / 鉴权层共用同一处区间语义：
        // range_end 为空或等于 key → 点读；否则半开 [key, range_end)。
        let single_key = RangeSemantics::of(&req.key, &req.range_end).is_single_key();
        let end = if single_key {
            single_key_end(&req.key)
        } else {
            req.range_end.clone()
        };
        let requested_limit = if single_key || req.limit == 0 {
            None
        } else {
            let requested = req.limit as usize;
            if max_range > 0 && requested > max_range {
                return Err(txn_session_status(SessionError::ReadSetLimit(
                    ReadSetLimit::RangeKeys,
                )));
            }
            Some(requested)
        };

        let intent = if single_key {
            ReadIntent::Point(&req.key)
        } else {
            ReadIntent::Range {
                start: &req.key,
                end: &req.range_end,
            }
        };
        // 先记录读集再读数据：并发读无法穿过上限；数据读失败时谓词保留是
        // 安全的方向（读集超集只会产生保守冲突，不会漏冲突）。
        let start_ts = self
            .txn_sessions()
            .record_read(&req.txn_id, intent, max_read_set)
            .map_err(txn_session_status)?;

        if !single_key && end <= req.key {
            return Ok(Response::new(txn_v2::ReadResponse {
                kvs: Vec::new(),
                read_ts: start_ts as i64,
            }));
        }

        let scan_limit = requested_limit.unwrap_or_else(|| max_range.saturating_add(1));
        let segments = self.kv_range_targets(&req.key, &end)?;
        let reads = segments
            .into_iter()
            .map(|seg| read_kv_range_segment(seg, start_ts, scan_limit));
        let mut parts = futures::stream::iter(reads).buffered(RANGE_FANOUT_CONCURRENCY);

        let mut kvs = Vec::new();
        while let Some(mut part) = parts.try_next().await? {
            if self.object_enabled() {
                part.retain(|kv| !crate::storage::object_store::key_in_object_space(&kv.key));
            }
            kvs.extend(part);
            if let Some(limit) = requested_limit {
                if kvs.len() >= limit {
                    kvs.truncate(limit);
                    break;
                }
            } else if kvs.len() > max_range {
                return Err(txn_session_status(SessionError::ReadSetLimit(
                    ReadSetLimit::RangeKeys,
                )));
            }
        }
        drop(parts);

        if req.keys_only {
            for kv in &mut kvs {
                kv.value.clear();
            }
        }
        Ok(Response::new(txn_v2::ReadResponse {
            kvs,
            read_ts: start_ts as i64,
        }))
    }

    /// 决策/校验归 #149/#151；未交付前门禁通过后返回 `UNIMPLEMENTED`。
    async fn commit(
        &self,
        _request: Request<txn_v2::CommitRequest>,
    ) -> Result<Response<txn_v2::CommitResponse>, Status> {
        Err(shell(KvTCapability::Txn, self))
    }

    /// 幂等删除会话：未知/已过期/已 abort 的 id 不报错。
    async fn abort(
        &self,
        request: Request<txn_v2::AbortRequest>,
    ) -> Result<Response<txn_v2::AbortResponse>, Status> {
        self.kvt().gate(KvTCapability::Txn)?;
        self.txn_sessions().abort(&request.into_inner().txn_id);
        Ok(Response::new(txn_v2::AbortResponse {}))
    }
}

// ──── coord.lease.v2 ────

#[tonic::async_trait]
impl lease_v2::lease_server::Lease for CoordNode {
    type LeaseKeepAliveStream =
        tokio_stream::wrappers::ReceiverStream<Result<lease_v2::LeaseKeepAliveResponse, Status>>;

    async fn lease_grant(
        &self,
        request: Request<lease_v2::LeaseGrantRequest>,
    ) -> Result<Response<lease_v2::LeaseGrantResponse>, Status> {
        self.kvt().gate(KvTCapability::Lease)?;
        lease_rpc::grant(self, request.into_inner()).await
    }

    async fn lease_revoke(
        &self,
        request: Request<lease_v2::LeaseRevokeRequest>,
    ) -> Result<Response<lease_v2::LeaseRevokeResponse>, Status> {
        self.kvt().gate(KvTCapability::Lease)?;
        lease_rpc::revoke(self, request.into_inner()).await
    }

    async fn lease_keep_alive(
        &self,
        request: Request<Streaming<lease_v2::LeaseKeepAliveRequest>>,
    ) -> Result<Response<Self::LeaseKeepAliveStream>, Status> {
        self.kvt().gate(KvTCapability::Lease)?;
        lease_rpc::keep_alive(self, request)
    }

    async fn lease_time_to_live(
        &self,
        request: Request<lease_v2::LeaseTimeToLiveRequest>,
    ) -> Result<Response<lease_v2::LeaseTimeToLiveResponse>, Status> {
        self.kvt().gate(KvTCapability::Lease)?;
        lease_rpc::time_to_live(self, request.into_inner()).await
    }
}

// ──── coord.watch.v2 ────

#[tonic::async_trait]
impl watch_v2::watch_server::Watch for CoordNode {
    type WatchStream = ShellStream<watch_v2::WatchResponse>;

    async fn watch(
        &self,
        _request: Request<Streaming<watch_v2::WatchRequest>>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        Err(shell(KvTCapability::Watch, self))
    }
}

// ──── coord.storage.v2 ────

#[tonic::async_trait]
impl storage_v2::storage_server::Storage for CoordNode {
    type GetStream = ShellStream<storage_v2::GetResponse>;

    async fn put(
        &self,
        _request: Request<Streaming<storage_v2::PutRequest>>,
    ) -> Result<Response<storage_v2::PutResponse>, Status> {
        Err(shell(KvTCapability::Storage, self))
    }

    async fn get(
        &self,
        _request: Request<storage_v2::GetRequest>,
    ) -> Result<Response<Self::GetStream>, Status> {
        Err(shell(KvTCapability::Storage, self))
    }

    async fn stat(
        &self,
        _request: Request<storage_v2::StatRequest>,
    ) -> Result<Response<storage_v2::StatResponse>, Status> {
        Err(shell(KvTCapability::Storage, self))
    }

    async fn delete(
        &self,
        _request: Request<storage_v2::DeleteRequest>,
    ) -> Result<Response<storage_v2::DeleteResponse>, Status> {
        Err(shell(KvTCapability::Storage, self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use coord_core::error_code::error_code_of;

    #[test]
    fn draining_session_error_is_retryable_unavailable() {
        let status = txn_session_status(SessionError::Draining);
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(error_code_of(&status).as_deref(), Some("UNAVAILABLE"));
    }
}
