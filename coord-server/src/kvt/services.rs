//! `coord.<domain>.v2` 的 tonic 服务壳（ADR-0015 G0）。
//!
//! 每个 handler 先过 [`KvTState::gate`]：关闭或能力未交付 → `FAILED_PRECONDITION`。
//! 门禁通过但 handler 尚未实现 → `UNIMPLEMENTED`（这是实现进度问题，不是配置
//! 问题；把两者混成同一个码会让运维无法判断该改配置还是该升级版本）。
//!
//! 服务**始终注册**（只要对应的 tonic server 被挂到 gRPC builder 上）：关闭时
//! reflection 仍然可见，便于客户端在连接阶段就发现"这个集群没有 KV-T"，而不是
//! 拿到 `UNAVAILABLE` 后误判为网络故障。

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use futures::{StreamExt, TryStreamExt};
use tokio::sync::mpsc;
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
use super::sequencer::CommitCandidate;
use super::txn::{ReadIntent, ReadSetLimit, SessionError};
use super::KvTCapability;
use crate::raft::tso::{normalize_batch_size, TsoAllocator, TsoBatch};
use crate::raft::type_config::{
    Command, ObjectStoreOp, Response as RaftResponse, SequencerInvalid, SequencerMutation,
    SequencerOutcome, SequencerReadRange,
};
use crate::server::{map_err, CoordNode, KvRangeSegment, ObjectTarget};
use crate::storage::object_store::{self, ChunkStore, ObjectLimits, ObjectManifest};

// ──── coord.tso.v2 ────

/// TSO 分配链路的错误映射：先结构化分类（NOT_LEADER / INVALID_ARGUMENT /
/// UNAVAILABLE），再交给通用 `map_err` 脱敏。
pub(crate) fn tso_status(err: Error) -> Status {
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

/// v2 能力装配缺失（无 region 0 raft / sequencer / TSO）→ FAILED_PRECONDITION。
pub(crate) fn kv_t_assembly_status(msg: &str) -> Status {
    attach(
        Status::failed_precondition(msg.to_string()),
        CoordErrorCode::FailedPrecondition,
    )
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
pub(crate) fn v2_status(err: Error) -> Status {
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

/// 路由版本 metadata key（ADR-0015 D12.5）：十进制 u64。
pub const ROUTING_EPOCH_METADATA_KEY: &str = "coord-routing-epoch";

/// 校验请求携带的 `coord-routing-epoch`（#161）。
///
/// 取值是**权威 Region 表的 `version`**（全局路由版本）。不能用 per-Region 的
/// `RegionEpoch{conf_ver, version}`：一个 u64 表达不了一对分量，而 Range/Txn 的
/// 一次请求在 split 后可能横跨 version 不同的两个 Region。
///
/// - 未携带：不校验（proto 头注允许；单 Region / 尚未学到版本的客户端）；
/// - 与本地已应用版本一致：放行；
/// - 不一致（客户端过旧，或本节点还没 apply 到该版本）：`ABORTED` +
///   `EPOCH_STALE`，并在 trailing metadata 回传本节点当前版本；
/// - 非法值：`INVALID_ARGUMENT`（静默忽略会让调用方以为 epoch 生效了）。
///
/// `[kv_t]` 关闭 / 无 RegionManager 时直接放行，v1 行为不变（ADR-0001）。
pub(crate) fn guard_routing_epoch(
    node: &CoordNode,
    metadata: &tonic::metadata::MetadataMap,
) -> Result<(), Status> {
    let Some(manager) = node.region_manager.as_ref() else {
        return Ok(());
    };
    let Some(raw) = metadata.get(ROUTING_EPOCH_METADATA_KEY) else {
        return Ok(());
    };
    let client = raw
        .to_str()
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .ok_or_else(|| {
            attach(
                Status::invalid_argument("coord-routing-epoch must be a decimal u64"),
                CoordErrorCode::InvalidArgument,
            )
        })?;
    // 请求路径也是表收敛点（与 `kv_target_for_key` 同一口径）：先按本地已 apply
    // 的表刷新，避免把“本节点还没刷新”误报成客户端的 epoch 过期。
    if let Err(err) = node.refresh_region_table() {
        tracing::warn!(error = %err, "region table refresh failed; checking routing epoch against the current route table");
    }
    let current = manager.table_version();
    if client == current {
        return Ok(());
    }
    let mut status = attach(
        Status::new(
            tonic::Code::Aborted,
            format!("stale routing epoch {client}; server has {current} (refresh route and retry)"),
        ),
        CoordErrorCode::EpochStale,
    );
    if let Ok(value) = tonic::metadata::MetadataValue::from_str(&current.to_string()) {
        status
            .metadata_mut()
            .insert(ROUTING_EPOCH_METADATA_KEY, value);
    }
    Err(status)
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
        guard_routing_epoch(self, request.metadata())?;
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
        guard_routing_epoch(self, request.metadata())?;
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
        guard_routing_epoch(self, request.metadata())?;
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
/// 把 wire 上的写集转成 sequencer 事务，并在进入会话前拒绝非法形状。
///
/// 用户级输入错误用 `INVALID_ARGUMENT`（不可重试）：形状校验先于会话提交占用，
/// 非法请求不会把会话卡在 `Committing`。`DELETE_RANGE` 要求 `range_end` 非空
/// 且 `key < range_end`；空区间不是「无操作」，而是形状错误（避免调用方以为
/// 删除生效）。
fn validate_commit_mutations(
    mutations: &[txn_v2::Mutation],
) -> Result<Vec<SequencerMutation>, Status> {
    use txn_v2::mutation::Op;

    let mut out = Vec::with_capacity(mutations.len());
    for mutation in mutations {
        if mutation.lease_id < 0 {
            return Err(attach(
                Status::invalid_argument("lease_id must be >= 0"),
                CoordErrorCode::InvalidArgument,
            ));
        }
        out.push(match mutation.op() {
            Op::Put if mutation.range_end.is_empty() => SequencerMutation::Put {
                key: mutation.key.clone(),
                value: mutation.value.clone(),
                lease_id: (mutation.lease_id != 0).then_some(mutation.lease_id),
            },
            Op::Delete if mutation.range_end.is_empty() => SequencerMutation::Delete {
                key: mutation.key.clone(),
            },
            Op::DeleteRange => {
                if mutation.range_end.is_empty() {
                    return Err(attach(
                        Status::invalid_argument("DELETE_RANGE requires a non-empty range_end"),
                        CoordErrorCode::InvalidArgument,
                    ));
                }
                if mutation.range_end <= mutation.key {
                    return Err(attach(
                        Status::invalid_argument("DELETE_RANGE requires key < range_end"),
                        CoordErrorCode::InvalidArgument,
                    ));
                }
                SequencerMutation::DeleteRange {
                    start: mutation.key.clone(),
                    end: mutation.range_end.clone(),
                }
            }
            Op::Put | Op::Delete | Op::Unspecified => {
                return Err(attach(
                    Status::invalid_argument(
                        "invalid mutation: PUT/DELETE must be a single key (empty range_end)",
                    ),
                    CoordErrorCode::InvalidArgument,
                ))
            }
        });
    }
    Ok(out)
}

fn txn_session_status(err: SessionError) -> Status {
    match err {
        SessionError::NotFound => attach(
            Status::aborted("transaction session not found or expired; restart the transaction"),
            CoordErrorCode::TxnNotFound,
        ),
        SessionError::Closed => attach(
            Status::aborted("transaction is committing or committed; start a new transaction"),
            CoordErrorCode::TxnNotFound,
        ),
        SessionError::Committed => attach(
            Status::failed_precondition(
                "transaction already committed with a different request_id; start a new transaction",
            ),
            CoordErrorCode::FailedPrecondition,
        ),
        SessionError::CommitInProgress => attach(
            Status::failed_precondition(
                "transaction commit already in progress with another request_id",
            ),
            CoordErrorCode::FailedPrecondition,
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
        guard_routing_epoch(self, request.metadata())?;
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
        guard_routing_epoch(self, request.metadata())?;
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

    /// 读集快照 + 写集交给 region 0 Commit Sequencer；`commit_ts` 由 sequencer
    /// 分配。冲突/非法不写决策，会话回到可 `Abort` 的状态；幂等重放直接返回
    /// 首次 `commit_ts`。不等待各 Region apply（D12.3）。
    async fn commit(
        &self,
        request: Request<txn_v2::CommitRequest>,
    ) -> Result<Response<txn_v2::CommitResponse>, Status> {
        self.kvt().gate(KvTCapability::Txn)?;
        guard_routing_epoch(self, request.metadata())?;
        let req = request.into_inner();
        if req.txn_id.is_empty() {
            return Err(attach(
                Status::invalid_argument("txn_id must not be empty"),
                CoordErrorCode::InvalidArgument,
            ));
        }
        let mutations = validate_commit_mutations(&req.mutations)?;

        let input = self
            .txn_sessions()
            .begin_commit(&req.txn_id, &req.request_id)
            .map_err(txn_session_status)?;
        if let Some(commit_ts) = input.replay {
            return Ok(Response::new(txn_v2::CommitResponse {
                commit_ts: ts_to_i64(commit_ts)?,
            }));
        }

        let release = |status: Status| {
            self.txn_sessions()
                .cancel_commit(&req.txn_id, &req.request_id);
            status
        };

        // 与 coord.kv.v2.Put 同规则：提案前拒绝死租约，避免决策落盘后无法
        // 再绑定/清理。验证失败释放提交占用，会话仍可 Abort 或改写集重试。
        for mutation in &mutations {
            if let SequencerMutation::Put {
                lease_id: Some(lease_id),
                ..
            } = mutation
            {
                if let Err(status) = lease_rpc::require_live_lease(self, *lease_id) {
                    return Err(release(status));
                }
            }
        }

        let Some(sequencer) = self.commit_sequencer() else {
            return Err(release(attach(
                Status::failed_precondition(
                    "txn Commit requires the region0 Commit Sequencer (local assembly missing)",
                ),
                CoordErrorCode::FailedPrecondition,
            )));
        };

        let (read_set, read_ranges) = input.read_set.into_parts();
        let candidate = CommitCandidate {
            txn_id: req.txn_id.clone(),
            start_ts: input.start_ts,
            request_id: req.request_id.clone(),
            read_set,
            read_ranges: read_ranges
                .into_iter()
                .map(|range| SequencerReadRange {
                    start: range.start,
                    end: range.end,
                })
                .collect(),
            mutations,
        };
        let limit = self.kvt().config().max_txn_bytes;
        if candidate.wire_bytes() as u64 > limit {
            return Err(release(attach(
                Status::invalid_argument(format!(
                    "transaction write set exceeds max_txn_bytes ({limit}); split the transaction"
                )),
                CoordErrorCode::TxnTooLarge,
            )));
        }

        let outcome = match sequencer.validate_and_commit(vec![candidate]).await {
            Ok(outcomes) => outcomes.into_iter().next(),
            // 传输失败可能已经落盘（幂等索引会回放）；释放占用让重试可继续。
            Err(err) => return Err(release(v2_status(err))),
        };
        match outcome {
            Some(SequencerOutcome::Committed { commit_ts })
            | Some(SequencerOutcome::Duplicate { commit_ts }) => {
                self.txn_sessions()
                    .finish_commit(&req.txn_id, &req.request_id, commit_ts);
                Ok(Response::new(txn_v2::CommitResponse {
                    commit_ts: ts_to_i64(commit_ts)?,
                }))
            }
            Some(SequencerOutcome::Conflict { commit_ts, .. }) => Err(release(attach(
                Status::aborted(format!(
                    "transaction conflict at commit_ts {commit_ts}; restart with a newer start_ts"
                )),
                CoordErrorCode::TxnConflict,
            ))),
            Some(SequencerOutcome::Invalid { reason, .. }) => Err(release(match reason {
                SequencerInvalid::ReadSetTooLarge | SequencerInvalid::RangeScanExceeded => attach(
                    Status::invalid_argument("transaction read set exceeds the server limit"),
                    CoordErrorCode::ReadSetTooLarge,
                ),
                SequencerInvalid::CommitTsNotAboveStart => Status::internal(
                    "sequencer allocated commit_ts not above start_ts (TSO invariant breach)",
                ),
                SequencerInvalid::MissingTxnId => attach(
                    Status::invalid_argument("txn_id must not be empty"),
                    CoordErrorCode::InvalidArgument,
                ),
            })),
            Some(SequencerOutcome::OutOfOrder {
                commit_ts,
                last_commit_ts,
            }) => Err(release(Status::internal(format!(
                "commit_ts {commit_ts} is not above the sequencer watermark {last_commit_ts}"
            )))),
            None => Err(release(Status::internal(
                "sequencer returned no outcome for the transaction",
            ))),
        }
    }

    /// 幂等删除会话：未知/已过期/已 abort 的 id 不报错。
    async fn abort(
        &self,
        request: Request<txn_v2::AbortRequest>,
    ) -> Result<Response<txn_v2::AbortResponse>, Status> {
        self.kvt().gate(KvTCapability::Txn)?;
        guard_routing_epoch(self, request.metadata())?;
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
        guard_routing_epoch(self, request.metadata())?;
        lease_rpc::grant(self, request.into_inner()).await
    }

    async fn lease_revoke(
        &self,
        request: Request<lease_v2::LeaseRevokeRequest>,
    ) -> Result<Response<lease_v2::LeaseRevokeResponse>, Status> {
        self.kvt().gate(KvTCapability::Lease)?;
        guard_routing_epoch(self, request.metadata())?;
        lease_rpc::revoke(self, request.into_inner()).await
    }

    async fn lease_keep_alive(
        &self,
        request: Request<Streaming<lease_v2::LeaseKeepAliveRequest>>,
    ) -> Result<Response<Self::LeaseKeepAliveStream>, Status> {
        self.kvt().gate(KvTCapability::Lease)?;
        guard_routing_epoch(self, request.metadata())?;
        lease_rpc::keep_alive(self, request)
    }

    async fn lease_time_to_live(
        &self,
        request: Request<lease_v2::LeaseTimeToLiveRequest>,
    ) -> Result<Response<lease_v2::LeaseTimeToLiveResponse>, Status> {
        self.kvt().gate(KvTCapability::Lease)?;
        guard_routing_epoch(self, request.metadata())?;
        lease_rpc::time_to_live(self, request.into_inner()).await
    }
}

// ──── coord.watch.v2 ────

#[tonic::async_trait]
impl watch_v2::watch_server::Watch for CoordNode {
    type WatchStream =
        tokio_stream::wrappers::ReceiverStream<Result<watch_v2::WatchResponse, Status>>;

    async fn watch(
        &self,
        request: Request<Streaming<watch_v2::WatchRequest>>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let response = super::watch_rpc::serve(self, request).await?;
        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            response.into_inner(),
        )))
    }
}

// ──── coord.storage.v2 ────

/// 对象存储运行时限制。未启用 = 配置/装配问题（FAILED_PRECONDITION），
/// 不是「未实现」：能力门禁已在前面把两者分开。
fn object_limits(node: &CoordNode) -> Result<Arc<ObjectLimits>, Status> {
    node.object_limits
        .clone()
        .ok_or_else(|| kv_t_assembly_status("object storage is disabled: [object_storage].enabled=false"))
}

fn object_chunk_store(node: &CoordNode, target: &ObjectTarget) -> Result<Arc<ChunkStore>, Status> {
    let _ = object_limits(node)?;
    target
        .chunk_store
        .clone()
        .ok_or_else(|| kv_t_assembly_status("object storage is disabled on this region"))
}

fn object_stat_v2(bucket: &str, object_id: &[u8], m: &ObjectManifest) -> storage_v2::ObjectStat {
    storage_v2::ObjectStat {
        bucket: bucket.to_string(),
        object_id: object_id.to_vec(),
        size: m.size as i64,
        chunks: m.chunks.len() as i64,
        commit_ts: m.last_revision as i64,
        create_ts: m.create_revision as i64,
        exists: true,
        committed: m.committed,
    }
}

/// 缺对象的 Stat：proto 的 `exists` 字段就是为此存在（不返回 NOT_FOUND），
/// 与 Get/Delete（流/删除语义无法表达缺席）不同。
fn missing_object_stat(bucket: &str, object_id: &[u8]) -> storage_v2::ObjectStat {
    storage_v2::ObjectStat {
        bucket: bucket.to_string(),
        object_id: object_id.to_vec(),
        size: 0,
        chunks: 0,
        commit_ts: 0,
        create_ts: 0,
        exists: false,
        committed: false,
    }
}

/// 读 manifest：`read_ts == 0` 走 ReadIndex 线性一致读，`> 0` 读历史快照。
///
/// 历史读先等 `safe_ts` 越过 `read_ts`（该时间点的历史已不再变化），再按
/// changelog 重建 manifest——不能拿当前值顶替（那会把删除后的重建读成旧对象）。
async fn read_object_manifest_at(
    node: &CoordNode,
    target: &ObjectTarget,
    bucket: &[u8],
    object_id: &[u8],
    read_ts: i64,
) -> Result<Option<ObjectManifest>, Status> {
    if read_ts < 0 {
        return Err(attach(
            Status::invalid_argument("read_ts must be >= 0"),
            CoordErrorCode::InvalidArgument,
        ));
    }
    if read_ts == 0 {
        node.object_linearizable(target).await?;
        return object_store::read_manifest(&target.mvcc, bucket, object_id).map_err(map_err);
    }
    let read_ts = read_ts as u64;
    if read_ts > node.safe_ts() {
        wait_for_safe_ts(node, read_ts, node.kvt().config().read_ts_wait_ms).await?;
    }
    let key = object_store::manifest_key(bucket, object_id);
    match target.mvcc.get_at_revision(&key, read_ts) {
        Ok(row) => row
            .map(|bytes| {
                ObjectManifest::from_bytes(&bytes)
                    .ok_or_else(|| Status::internal("corrupt object manifest row"))
            })
            .transpose(),
        Err(err) => Err(v2_status(err)),
    }
}

/// chunk 文件缺失：历史读下对象字节不保留（chunk 不进 MVCC），必须是显式的
/// `OUT_OF_RANGE` + `HISTORY_UNAVAILABLE`，不能回退成当前值。
fn history_unavailable(object_id: &[u8]) -> Status {
    attach(
        Status::out_of_range(format!(
            "object bytes for {} at read_ts are out of range: chunk files are not versioned \
             (overwritten/deleted since read_ts)",
            String::from_utf8_lossy(object_id)
        )),
        CoordErrorCode::OutOfRange,
    )
}

fn now_unix_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[tonic::async_trait]
impl storage_v2::storage_server::Storage for CoordNode {
    type GetStream = tokio_stream::wrappers::ReceiverStream<Result<storage_v2::GetResponse, Status>>;

    async fn put(
        &self,
        request: Request<Streaming<storage_v2::PutRequest>>,
    ) -> Result<Response<storage_v2::PutResponse>, Status> {
        self.kvt().gate(KvTCapability::Storage)?;
        guard_routing_epoch(self, request.metadata())?;
        self.ensure_writable()?;
        let limits = object_limits(self)?;

        let mut stream = request.into_inner();
        // 首帧必须携带 meta（bucket/object_id/total_size）
        let meta = match stream.message().await? {
            Some(storage_v2::PutRequest {
                part: Some(storage_v2::put_request::Part::Meta(meta)),
            }) => meta,
            Some(_) => {
                return Err(attach(
                    Status::invalid_argument(
                        "first PutRequest message must carry meta (bucket/object_id/total_size)",
                    ),
                    CoordErrorCode::InvalidArgument,
                ))
            }
            None => {
                return Err(attach(
                    Status::invalid_argument("empty Put stream"),
                    CoordErrorCode::InvalidArgument,
                ))
            }
        };
        let bucket = meta.bucket;
        let object_id = meta.object_id;
        object_store::validate_ref(bucket.as_bytes(), &object_id).map_err(v2_status)?;
        // `total_size`：`> 0` = 声明长度（Commit 须严格相等）；`-1` = 未知长度
        // （流末按实际字节定长，累计上限 = `max_object_size`）；其余非法。
        let declared: Option<u64> = match meta.total_size {
            -1 => None,
            n if n > 0 => Some(n as u64),
            _ => {
                return Err(attach(
                    Status::invalid_argument(
                        "total_size must be > 0 (declared) or -1 (unknown)",
                    ),
                    CoordErrorCode::InvalidArgument,
                ))
            }
        };
        if declared.is_some_and(|n| n > limits.max_object_size) {
            return Err(attach(
                Status::invalid_argument(format!(
                    "total_size must be in (0, {}]",
                    limits.max_object_size
                )),
                CoordErrorCode::InvalidArgument,
            ));
        }

        let target = self.object_target_for(bucket.as_bytes(), &object_id)?;
        let store = object_chunk_store(self, &target)?;

        // 配额 admission（尽力而为）：估算 = 已用 + 本次声明（未知长度按上限保守估）
        let estimate = declared.unwrap_or(limits.max_object_size);
        if limits.quota_bytes > 0 {
            let used = store.usage_bytes().map_err(map_err)?;
            if used.saturating_add(estimate) > limits.quota_bytes {
                return Err(Status::resource_exhausted(format!(
                    "object storage quota exceeded: used {used} + {estimate} > {}",
                    limits.quota_bytes
                )));
            }
        }

        let (_, ok) = self
            .propose_object_op_v2(
                &target,
                ObjectStoreOp::Begin {
                    bucket: bucket.as_bytes().to_vec(),
                    object_id: object_id.clone(),
                    total_size: declared.unwrap_or(0),
                    started_at_unix: now_unix_secs(),
                },
            )
            .await?;
        if !ok {
            return Err(Status::already_exists(format!(
                "object {bucket}/{} already exists (committed or upload in progress)",
                String::from_utf8_lossy(&object_id)
            )));
        }

        // 逐 chunk 经 raft 提交；manifest 行与 chunk 文件在同一条目里落盘
        let mut seq: u32 = 0;
        let mut sent_bytes: u64 = 0;
        loop {
            let msg = match stream.message().await {
                Ok(Some(m)) => m,
                Ok(None) => break,
                Err(e) => {
                    // 客户端中止：残留 Creating 由 GC 回收
                    return Err(Status::aborted(format!(
                        "put stream error (partial object left for GC): {e}"
                    )));
                }
            };
            let data = match msg.part {
                Some(storage_v2::put_request::Part::Chunk(data)) => data,
                Some(storage_v2::put_request::Part::Meta(_)) => {
                    return Err(attach(
                        Status::invalid_argument("meta must only appear as the first message"),
                        CoordErrorCode::InvalidArgument,
                    ))
                }
                None => {
                    return Err(attach(
                        Status::invalid_argument("empty PutRequest message"),
                        CoordErrorCode::InvalidArgument,
                    ))
                }
            };
            if data.is_empty() || data.len() > limits.chunk_size {
                return Err(attach(
                    Status::invalid_argument(format!(
                        "chunk size {} must be in (0, {}]",
                        data.len(),
                        limits.chunk_size
                    )),
                    CoordErrorCode::InvalidArgument,
                ));
            }
            let len = data.len() as u64;
            let bound = declared.unwrap_or(limits.max_object_size);
            if sent_bytes.saturating_add(len) > bound {
                return Err(attach(
                    Status::invalid_argument(if declared.is_some() {
                        "uploaded bytes exceed declared total_size"
                    } else {
                        "uploaded bytes exceed max object size"
                    }),
                    CoordErrorCode::InvalidArgument,
                ));
            }
            let (_, ok) = self
                .propose_object_op_v2(
                    &target,
                    ObjectStoreOp::Chunk {
                        bucket: bucket.as_bytes().to_vec(),
                        object_id: object_id.clone(),
                        seq,
                        data,
                        now_unix: now_unix_secs(),
                    },
                )
                .await?;
            if !ok {
                return Err(Status::aborted(
                    "chunk rejected by state machine (ordering/limits); abort upload \
                     (partial object left for GC)",
                ));
            }
            sent_bytes += len;
            seq += 1;
        }

        if seq == 0 {
            return Err(attach(
                Status::invalid_argument("object must contain at least one chunk"),
                CoordErrorCode::InvalidArgument,
            ));
        }
        let (commit_ts, ok) = self
            .propose_object_op_v2(
                &target,
                ObjectStoreOp::Commit {
                    bucket: bucket.as_bytes().to_vec(),
                    object_id: object_id.clone(),
                },
            )
            .await?;
        if !ok {
            return Err(attach(
                Status::invalid_argument(
                    "commit rejected: received bytes != declared total_size (partial object \
                     left for GC)",
                ),
                CoordErrorCode::InvalidArgument,
            ));
        }
        let final_size = declared.unwrap_or(sent_bytes);
        tracing::info!(
            bucket = %bucket,
            chunks = seq,
            size = final_size,
            commit_ts,
            "object committed (v2)"
        );
        Ok(Response::new(storage_v2::PutResponse {
            commit_ts: ts_to_i64(commit_ts)?,
            size: final_size as i64,
            chunks: seq as i64,
        }))
    }

    async fn get(
        &self,
        request: Request<storage_v2::GetRequest>,
    ) -> Result<Response<Self::GetStream>, Status> {
        self.kvt().gate(KvTCapability::Storage)?;
        guard_routing_epoch(self, request.metadata())?;
        let req = request.into_inner();
        let bucket = req.bucket;
        let object_id = req.object_id;
        object_store::validate_ref(bucket.as_bytes(), &object_id).map_err(v2_status)?;

        let target = self.object_target_for(bucket.as_bytes(), &object_id)?;
        let store = object_chunk_store(self, &target)?;
        let manifest = read_object_manifest_at(self, &target, bucket.as_bytes(), &object_id, req.read_ts)
            .await?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "object {bucket}/{} not found",
                    String::from_utf8_lossy(&object_id)
                ))
            })?;
        if !manifest.committed {
            return Err(Status::failed_precondition(format!(
                "object {bucket}/{} upload in progress or aborted (not committed)",
                String::from_utf8_lossy(&object_id)
            )));
        }

        let historical = req.read_ts > 0;
        let (tx, rx) = mpsc::channel::<Result<storage_v2::GetResponse, Status>>(4);
        let bucket_c = bucket.clone();
        let object_id_c = object_id.clone();
        tokio::spawn(async move {
            let first = storage_v2::GetResponse {
                part: Some(storage_v2::get_response::Part::Stat(object_stat_v2(
                    &bucket_c,
                    &object_id_c,
                    &manifest,
                ))),
            };
            if tx.send(Ok(first)).await.is_err() {
                return;
            }
            let n = manifest.chunks.len() as u32;
            for seq in 0..n {
                let store = Arc::clone(&store);
                let manifest = manifest.clone();
                let bucket = bucket_c.clone();
                let object_id = object_id_c.clone();
                let res = tokio::task::spawn_blocking(move || {
                    object_store::read_manifest_chunk(&store, &manifest, bucket.as_bytes(), &object_id, seq)
                        .map_err(|err| match err {
                            Error::NotFound { .. } if historical => history_unavailable(&object_id),
                            Error::NotFound { .. } => Status::not_found(format!(
                                "object {}/{} disappeared during Get",
                                String::from_utf8_lossy(bucket.as_bytes()),
                                String::from_utf8_lossy(&object_id)
                            )),
                            other => map_err(other),
                        })
                })
                .await;
                let data = match res {
                    Ok(Ok(d)) => d,
                    Ok(Err(e)) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                    Err(e) => {
                        let _ = tx
                            .send(Err(Status::internal(format!("chunk read task join: {e}"))))
                            .await;
                        return;
                    }
                };
                if tx
                    .send(Ok(storage_v2::GetResponse {
                        part: Some(storage_v2::get_response::Part::Chunk(data)),
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    async fn stat(
        &self,
        request: Request<storage_v2::StatRequest>,
    ) -> Result<Response<storage_v2::StatResponse>, Status> {
        self.kvt().gate(KvTCapability::Storage)?;
        guard_routing_epoch(self, request.metadata())?;
        let req = request.into_inner();
        let bucket = req.bucket;
        let object_id = req.object_id;
        object_store::validate_ref(bucket.as_bytes(), &object_id).map_err(v2_status)?;

        let target = self.object_target_for(bucket.as_bytes(), &object_id)?;
        object_chunk_store(self, &target)?;
        let manifest = read_object_manifest_at(self, &target, bucket.as_bytes(), &object_id, req.read_ts)
            .await?;
        let stat = match manifest {
            Some(m) => object_stat_v2(&bucket, &object_id, &m),
            None => missing_object_stat(&bucket, &object_id),
        };
        Ok(Response::new(storage_v2::StatResponse { stat: Some(stat) }))
    }

    async fn delete(
        &self,
        request: Request<storage_v2::DeleteRequest>,
    ) -> Result<Response<storage_v2::DeleteResponse>, Status> {
        self.kvt().gate(KvTCapability::Storage)?;
        guard_routing_epoch(self, request.metadata())?;
        self.ensure_writable()?;
        let req = request.into_inner();
        let bucket = req.bucket;
        let object_id = req.object_id;
        object_store::validate_ref(bucket.as_bytes(), &object_id).map_err(v2_status)?;

        let target = self.object_target_for(bucket.as_bytes(), &object_id)?;
        object_chunk_store(self, &target)?;
        // 线性一致读：不存在就不提案（避免无谓的 commit_ts 消耗与 tombstone）
        self.object_linearizable(&target).await?;
        if object_store::read_manifest(&target.mvcc, bucket.as_bytes(), &object_id)
            .map_err(map_err)?
            .is_none()
        {
            return Err(Status::not_found(format!(
                "object {bucket}/{} not found",
                String::from_utf8_lossy(&object_id)
            )));
        }
        let (commit_ts, ok) = self
            .propose_object_op_v2(
                &target,
                ObjectStoreOp::Delete {
                    bucket: bucket.as_bytes().to_vec(),
                    object_id: object_id.clone(),
                },
            )
            .await?;
        Ok(Response::new(storage_v2::DeleteResponse {
            deleted: ok,
            commit_ts: ts_to_i64(commit_ts)?,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use coord_core::error_code::error_code_of;

    fn mutation(op: txn_v2::mutation::Op, key: &[u8], range_end: &[u8]) -> txn_v2::Mutation {
        txn_v2::Mutation {
            op: op as i32,
            key: key.to_vec(),
            range_end: range_end.to_vec(),
            value: b"v".to_vec(),
            lease_id: 0,
        }
    }

    #[test]
    fn draining_session_error_is_retryable_unavailable() {
        let status = txn_session_status(SessionError::Draining);
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(error_code_of(&status).as_deref(), Some("UNAVAILABLE"));
    }

    #[test]
    fn commit_mutations_validate_ops_and_ranges() {
        use txn_v2::mutation::Op;

        assert!(validate_commit_mutations(&[mutation(Op::Put, b"k", b"")]).is_ok());
        assert!(validate_commit_mutations(&[mutation(Op::Delete, b"k", b"")]).is_ok());

        for bad in [
            mutation(Op::Unspecified, b"k", b""),
            mutation(Op::Put, b"k", b"z"),
            mutation(Op::Delete, b"k", b"z"),
        ] {
            let err = validate_commit_mutations(&[bad]).expect_err("invalid mutation");
            assert_eq!(err.code(), tonic::Code::InvalidArgument, "{err:?}");
            assert_eq!(error_code_of(&err).as_deref(), Some("INVALID_ARGUMENT"));
        }

        // 区间写已交付；点写的 range_end 仍必须为空。
        assert!(validate_commit_mutations(&[mutation(Op::DeleteRange, b"a", b"b")]).is_ok());

        let mut negative_lease = mutation(Op::Put, b"k", b"");
        negative_lease.lease_id = -1;
        assert_eq!(
            validate_commit_mutations(&[negative_lease])
                .expect_err("negative lease")
                .code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn test_validate_commit_mutations_accepts_delete_range() {
        use txn_v2::mutation::Op;

        assert_eq!(
            validate_commit_mutations(&[mutation(Op::DeleteRange, b"a", b"m")]).unwrap(),
            vec![SequencerMutation::DeleteRange {
                start: b"a".to_vec(),
                end: b"m".to_vec(),
            }]
        );
    }

    #[test]
    fn test_validate_commit_mutations_rejects_bad_delete_range() {
        use txn_v2::mutation::Op;

        for bad in [
            mutation(Op::DeleteRange, b"a", b""),
            mutation(Op::DeleteRange, b"b", b"a"),
            mutation(Op::DeleteRange, b"a", b"a"),
        ] {
            let err = validate_commit_mutations(&[bad]).expect_err("invalid range");
            assert_eq!(err.code(), tonic::Code::InvalidArgument, "{err:?}");
            assert_eq!(error_code_of(&err).as_deref(), Some("INVALID_ARGUMENT"));
        }
    }

    #[test]
    fn session_errors_map_to_protocol_codes() {
        let not_found = txn_session_status(SessionError::NotFound);
        assert_eq!(not_found.code(), tonic::Code::Aborted);
        assert_eq!(error_code_of(&not_found).as_deref(), Some("TXN_NOT_FOUND"));

        let closed = txn_session_status(SessionError::Closed);
        assert_eq!(closed.code(), tonic::Code::Aborted);
        assert_eq!(error_code_of(&closed).as_deref(), Some("TXN_NOT_FOUND"));

        for err in [SessionError::Committed, SessionError::CommitInProgress] {
            let status = txn_session_status(err);
            assert_eq!(status.code(), tonic::Code::FailedPrecondition, "{status:?}");
            assert_eq!(
                error_code_of(&status).as_deref(),
                Some("FAILED_PRECONDITION")
            );
        }

        let too_large = txn_session_status(SessionError::ReadSetLimit(ReadSetLimit::RangeKeys));
        assert_eq!(too_large.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            error_code_of(&too_large).as_deref(),
            Some("READ_SET_TOO_LARGE")
        );
    }
}
