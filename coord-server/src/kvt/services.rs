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

use super::KvTCapability;
use crate::raft::tso::{normalize_batch_size, TsoAllocator, TsoBatch};
use crate::raft::type_config::{Command, Response as RaftResponse};
use crate::server::{map_err, CoordNode};

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
fn tso_status(err: Error) -> Status {
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
async fn wait_for_safe_ts(
    node: &CoordNode,
    read_ts: u64,
    wait_ms: u64,
) -> Result<(), Status> {
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
        if req.lease_id != 0 {
            // v2 Lease 未交付前绑定 lease 会让 v1 的按 log-index 吊销删除
            // CommitTs 数据（不推进 revision），破坏 D11.2/7。
            return Err(attach(
                Status::failed_precondition(
                    "kv.v2 lease binding requires coord.lease.v2 (#145); \
                     retry with lease_id=0",
                ),
                CoordErrorCode::FailedPrecondition,
            ));
        }
        self.guard_object_write_key(&req.key)?;

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
                lease_id: None,
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
                    None,
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

        let target = self.kv_target_for_key(&req.key)?;
        let single_key = RangeSemantics::of(&req.key, &req.range_end).is_single_key();
        if !single_key {
            self.guard_range_in_target(&target, &req.key, &req.range_end)?;
        }

        // 读屏障确认 leader 且本地 apply 已覆盖已提交日志；否则 follower 可能
        // 用落后的 current_revision 把 read_ts=0 解析成旧快照。
        self.ensure_linearizable_on(
            target.raft.as_deref(),
            target.raft_log_store.as_ref(),
            target.region_id,
        )
        .await?;

        let safe_ts = self.safe_ts();
        let read_ts = if req.read_ts == 0 {
            safe_ts
        } else {
            req.read_ts as u64
        };
        if read_ts > safe_ts {
            wait_for_safe_ts(self, read_ts, self.kvt().config().read_ts_wait_ms).await?;
        }

        let current_now = target.mvcc.current_revision();
        let scan_limit = if req.count_only {
            if max_range == 0 {
                0
            } else {
                max_range.saturating_add(1)
            }
        } else {
            limit
        };
        let end = if single_key {
            single_key_end(&req.key)
        } else {
            req.range_end.clone()
        };

        // safe_ts 是各 Region current 的最小值，因此 `read_ts <= safe_ts` 必然
        // `<= target.current`；但并发的 fence 可能让 current 在两步之间又前进。
        let (rows, header_read_ts) = if read_ts == 0 {
            // 0 是“早于任何 commit”的合法快照（safe_ts 也可能为 0）；
            // changelog 的 0 水位会被当成已压缩，不能走历史读。
            (Vec::new(), 0)
        } else if read_ts == current_now {
            // 值、元数据、header revision 同一读事务；header 用该事务的 revision，
            // 不能用读事务之前的 current——并发提交会让 header 落后于返回的行。
            let (rows, live_revision) = target
                .mvcc
                .range_in_with_metadata(&req.key, &end, scan_limit)
                .map_err(map_err)?;
            if live_revision > read_ts {
                // 显式/全局快照读越过了目标 revision：改用精确历史视图，
                // 保证 `mod_revision <= read_ts`（read_ts=0 已在上一个分支处理）。
                let rows = target
                    .mvcc
                    .range_at_revision_with_metadata(&req.key, &end, scan_limit, read_ts)
                    .map_err(v2_status)?;
                (rows, read_ts)
            } else {
                (rows, read_ts)
            }
        } else {
            let rows = target
                .mvcc
                .range_at_revision_with_metadata(&req.key, &end, scan_limit, read_ts)
                .map_err(v2_status)?;
            (rows, read_ts)
        };

        let mut kvs: Vec<kv_v2::KeyValue> = rows
            .into_iter()
            .map(|(key, value, meta)| to_kv_v2(&key, &value, Some(&meta)))
            .collect();
        if self.object_enabled() {
            kvs.retain(|kv| !crate::storage::object_store::key_in_object_space(&kv.key));
        }
        if req.count_only && max_range > 0 && kvs.len() > max_range {
            return Err(attach(
                Status::invalid_argument(format!(
                    "range scan exceeds max {max_range}; narrow the range"
                )),
                CoordErrorCode::ReadSetTooLarge,
            ));
        }

        let count = kvs.len() as i64;
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
            read_ts: header_read_ts as i64,
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

#[tonic::async_trait]
impl txn_v2::txn_server::Txn for CoordNode {
    async fn begin(
        &self,
        _request: Request<txn_v2::BeginRequest>,
    ) -> Result<Response<txn_v2::BeginResponse>, Status> {
        Err(shell(KvTCapability::Txn, self))
    }

    async fn read(
        &self,
        _request: Request<txn_v2::ReadRequest>,
    ) -> Result<Response<txn_v2::ReadResponse>, Status> {
        Err(shell(KvTCapability::Txn, self))
    }

    async fn commit(
        &self,
        _request: Request<txn_v2::CommitRequest>,
    ) -> Result<Response<txn_v2::CommitResponse>, Status> {
        Err(shell(KvTCapability::Txn, self))
    }

    async fn abort(
        &self,
        _request: Request<txn_v2::AbortRequest>,
    ) -> Result<Response<txn_v2::AbortResponse>, Status> {
        Err(shell(KvTCapability::Txn, self))
    }
}

// ──── coord.lease.v2 ────

#[tonic::async_trait]
impl lease_v2::lease_server::Lease for CoordNode {
    type LeaseKeepAliveStream = ShellStream<lease_v2::LeaseKeepAliveResponse>;

    async fn lease_grant(
        &self,
        _request: Request<lease_v2::LeaseGrantRequest>,
    ) -> Result<Response<lease_v2::LeaseGrantResponse>, Status> {
        Err(shell(KvTCapability::Lease, self))
    }

    async fn lease_revoke(
        &self,
        _request: Request<lease_v2::LeaseRevokeRequest>,
    ) -> Result<Response<lease_v2::LeaseRevokeResponse>, Status> {
        Err(shell(KvTCapability::Lease, self))
    }

    async fn lease_keep_alive(
        &self,
        _request: Request<Streaming<lease_v2::LeaseKeepAliveRequest>>,
    ) -> Result<Response<Self::LeaseKeepAliveStream>, Status> {
        Err(shell(KvTCapability::Lease, self))
    }

    async fn lease_time_to_live(
        &self,
        _request: Request<lease_v2::LeaseTimeToLiveRequest>,
    ) -> Result<Response<lease_v2::LeaseTimeToLiveResponse>, Status> {
        Err(shell(KvTCapability::Lease, self))
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
