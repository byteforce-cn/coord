//! `coord.lease.v2` 服务实现与后台清理（#145）。
//!
//! 租约元数据只存在 region 0 system raft（`/_leasev2/`）；绑定关系由业务
//! Region 的 `KvMetadata.lease_id` 维护。Revoke/到期先写 region 0 决策
//! （`commit_ts` 来自 TSO），再由 [`CoordNode::start_kvt_lease_revoker`] 按 key
//! 所在 Region 扇出 `DeleteKeysByLeaseV2`。数据 Region 以更高 `commit_ts`
//! 已 apply 时删除会被 `COMMIT_REJECTED`，revoker 重新取时间戳并更新决策后重试。

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status, Streaming};

use coord_core::error_code::{attach, CoordErrorCode};
use coord_core::types::RegionId;
use coord_proto::lease::v2 as lease_v2;

use crate::lease::wall_clock_now_ms;
use crate::raft::type_config::{Command, LeaseV2Op, Response as RaftResponse};
use crate::server::{map_err, CoordNode};
use crate::storage::mvcc::LeaseV2Record;

use super::services::tso_status;

/// TTL 上限与 v1 一致（`LeaseManager::grant_with_id_checked`）。
pub(crate) const LEASE_V2_MAX_TTL_SECONDS: i64 = 86_400;
/// `request_id` 是表键后缀；限制长度避免无界键/内存放大。
const LEASE_V2_MAX_REQUEST_ID: usize = 256;
/// TimeToLive(keys=true) 的返回上限（契约允许服务端截断）。
const LEASE_V2_MAX_KEYS: usize = 10_000;

const REVOKER_TICK: Duration = Duration::from_millis(100);
const EXPIRY_TICK: Duration = Duration::from_millis(250);
/// 周期性重扫已 revoke 记录（快照安装跳过日志 apply 时没有广播）。
const REVOKER_RESCAN_TICKS: u32 = 600;

fn failed_precondition(message: &str) -> Status {
    attach(
        Status::failed_precondition(message),
        CoordErrorCode::FailedPrecondition,
    )
}

fn require_raft(node: &CoordNode) -> Result<&Arc<crate::raft::CoordRaft>, Status> {
    node.raft.as_ref().ok_or_else(|| {
        failed_precondition(
            "coord.lease.v2 requires a region 0 raft \
             (multi_raft/system raft assembly missing)",
        )
    })
}

fn validate_ttl(ttl_seconds: i64) -> Result<i64, Status> {
    if ttl_seconds <= 0 || ttl_seconds > LEASE_V2_MAX_TTL_SECONDS {
        return Err(attach(
            Status::invalid_argument(format!(
                "ttl_seconds must be in 1..={LEASE_V2_MAX_TTL_SECONDS}, got {ttl_seconds}"
            )),
            CoordErrorCode::InvalidArgument,
        ));
    }
    Ok(ttl_seconds)
}

fn validate_request_id(request_id: &[u8]) -> Result<(), Status> {
    if request_id.len() > LEASE_V2_MAX_REQUEST_ID {
        return Err(attach(
            Status::invalid_argument(format!(
                "request_id exceeds {LEASE_V2_MAX_REQUEST_ID} bytes"
            )),
            CoordErrorCode::InvalidArgument,
        ));
    }
    Ok(())
}

fn remaining_seconds(deadline_wall_ms: i64, now_ms: i64) -> i64 {
    if deadline_wall_ms <= now_ms {
        0
    } else {
        (deadline_wall_ms - now_ms + 999) / 1000
    }
}

/// 从本地 TSO 批次缓存取时间戳。租约 RPC 只在 region 0 leader 上执行，因此
/// 缓存必然来自本节点；未装配 TSO = 配置/装配错误（不是网络错误）。
pub(super) async fn next_ts(node: &CoordNode) -> Result<u64, Status> {
    let client = node.tso_client().ok_or_else(|| {
        failed_precondition(
            "coord.lease.v2 requires the TSO client (region 0 raft / [kv_t] assembly missing)",
        )
    })?;
    client
        .get_ts_above(node.safe_ts_fence())
        .await
        .map_err(tso_status)
}

async fn submit(node: &CoordNode, op: LeaseV2Op) -> Result<RaftResponse, Status> {
    let raft = require_raft(node)?;
    let timeout = node.lease_write_timeout();
    let resp = tokio::time::timeout(timeout, raft.client_write(Command::LeaseV2(op)))
        .await
        .map_err(|_| {
            attach(
                Status::deadline_exceeded("lease.v2 write timed out (no quorum?)"),
                CoordErrorCode::DeadlineExceeded,
            )
        })?
        .map_err(|e| Status::internal(format!("raft lease.v2 write failed: {e}")))?;
    Ok(resp.response().clone())
}

/// 绑定前校验：记录必须存在、未吊销且未过期。
pub(crate) fn require_live_lease(node: &CoordNode, lease_id: i64) -> Result<LeaseV2Record, Status> {
    let record = node
        .storage
        .get_lease_v2_record(lease_id)
        .map_err(map_err)?
        .ok_or_else(|| {
            attach(
                Status::failed_precondition(format!("lease {lease_id} does not exist")),
                CoordErrorCode::FailedPrecondition,
            )
        })?;
    if record.revoked {
        return Err(attach(
            Status::failed_precondition(format!("lease {lease_id} is revoked")),
            CoordErrorCode::FailedPrecondition,
        ));
    }
    if record.deadline_wall_ms <= wall_clock_now_ms() {
        return Err(attach(
            Status::failed_precondition(format!("lease {lease_id} has expired")),
            CoordErrorCode::FailedPrecondition,
        ));
    }
    Ok(record)
}

pub(super) async fn grant(
    node: &CoordNode,
    req: lease_v2::LeaseGrantRequest,
) -> Result<Response<lease_v2::LeaseGrantResponse>, Status> {
    if req.lease_id < 0 {
        return Err(attach(
            Status::invalid_argument("lease_id must be >= 0"),
            CoordErrorCode::InvalidArgument,
        ));
    }
    let ttl = validate_ttl(req.ttl_seconds)?;
    validate_request_id(&req.request_id)?;
    node.ensure_lease_leader().await?;

    let granted_ts = next_ts(node).await?;
    let lease_id = if req.lease_id == 0 {
        i64::try_from(granted_ts).map_err(|_| Status::internal("TSO exceeds lease id i64 range"))?
    } else {
        req.lease_id
    };
    let deadline_wall_ms = wall_clock_now_ms().saturating_add(ttl.saturating_mul(1000));

    let resp = submit(
        node,
        LeaseV2Op::Grant {
            lease_id,
            ttl_seconds: ttl,
            granted_ts,
            deadline_wall_ms,
            request_id: req.request_id,
        },
    )
    .await?;

    match resp {
        RaftResponse::LeaseV2Grant {
            lease_id,
            granted_ts,
            ttl_seconds,
            created,
        } => {
            if !created {
                return Err(attach(
                    Status::already_exists(format!(
                        "lease id {lease_id} is already in use; request a different id \
                         (or omit id to let the server allocate one)"
                    )),
                    CoordErrorCode::AlreadyExists,
                ));
            }
            Ok(Response::new(lease_v2::LeaseGrantResponse {
                lease_id,
                ttl_seconds,
                granted_ts: i64::try_from(granted_ts)
                    .map_err(|_| Status::internal("TSO exceeds i64 wire range"))?,
            }))
        }
        other => Err(Status::internal(format!(
            "unexpected raft response for LeaseV2 Grant: {other:?}"
        ))),
    }
}

pub(super) async fn revoke(
    node: &CoordNode,
    req: lease_v2::LeaseRevokeRequest,
) -> Result<Response<lease_v2::LeaseRevokeResponse>, Status> {
    if req.lease_id <= 0 {
        return Err(attach(
            Status::invalid_argument("lease_id must be > 0"),
            CoordErrorCode::InvalidArgument,
        ));
    }
    validate_request_id(&req.request_id)?;
    node.ensure_lease_leader().await?;

    // 未知/已吊销 = 幂等成功：不分配 commit_ts、不写决策、不重复删除。
    match node
        .storage
        .get_lease_v2_record(req.lease_id)
        .map_err(map_err)?
    {
        None => {
            return Ok(Response::new(lease_v2::LeaseRevokeResponse {
                commit_ts: 0,
            }));
        }
        Some(record) if record.revoked => {
            return Ok(Response::new(lease_v2::LeaseRevokeResponse {
                commit_ts: 0,
            }));
        }
        Some(_) => {}
    }

    let commit_ts = next_ts(node).await?;
    let resp = submit(
        node,
        LeaseV2Op::Revoke {
            lease_id: req.lease_id,
            commit_ts,
            expected_deadline_wall_ms: 0,
            request_id: req.request_id,
        },
    )
    .await?;
    match resp {
        RaftResponse::LeaseV2Revoke { commit_ts } => {
            Ok(Response::new(lease_v2::LeaseRevokeResponse {
                commit_ts: i64::try_from(commit_ts)
                    .map_err(|_| Status::internal("commit_ts exceeds i64 wire range"))?,
            }))
        }
        other => Err(Status::internal(format!(
            "unexpected raft response for LeaseV2 Revoke: {other:?}"
        ))),
    }
}

fn lease_keys(node: &CoordNode, lease_id: i64) -> Result<Vec<Vec<u8>>, Status> {
    let mut keys = Vec::new();
    if let Some(manager) = &node.region_manager {
        for handle in manager.list_regions() {
            let Some(rt) = manager.runtime(handle.region_id()) else {
                continue;
            };
            let limit = LEASE_V2_MAX_KEYS.saturating_sub(keys.len());
            if limit == 0 {
                break;
            }
            keys.extend(rt.mvcc.keys_by_lease(lease_id, limit).map_err(map_err)?);
        }
    } else {
        keys = node
            .storage
            .keys_by_lease(lease_id, LEASE_V2_MAX_KEYS)
            .map_err(map_err)?;
    }
    keys.sort();
    keys.dedup();
    Ok(keys)
}

pub(super) async fn time_to_live(
    node: &CoordNode,
    req: lease_v2::LeaseTimeToLiveRequest,
) -> Result<Response<lease_v2::LeaseTimeToLiveResponse>, Status> {
    if req.lease_id <= 0 {
        return Err(attach(
            Status::invalid_argument("lease_id must be > 0"),
            CoordErrorCode::InvalidArgument,
        ));
    }
    // leader-only：follower 的本地 applied 视图可能落后，会把存在的租约误报
    // NOT_FOUND（与 KeepAlive 的 fail-closed 口径一致）。
    node.ensure_lease_leader().await?;
    let record = node
        .storage
        .get_lease_v2_record(req.lease_id)
        .map_err(map_err)?
        .filter(|record| !record.revoked)
        .ok_or_else(|| {
            attach(
                Status::not_found(format!("lease {} not found", req.lease_id)),
                CoordErrorCode::NotFound,
            )
        })?;
    let remaining = remaining_seconds(record.deadline_wall_ms, wall_clock_now_ms());
    let keys = if req.keys && remaining > 0 {
        lease_keys(node, req.lease_id)?
    } else {
        Vec::new()
    };
    Ok(Response::new(lease_v2::LeaseTimeToLiveResponse {
        lease_id: req.lease_id,
        ttl_seconds: remaining,
        granted_ttl_seconds: record.ttl,
        keys,
    }))
}

pub(super) fn keep_alive(
    node: &CoordNode,
    request: tonic::Request<Streaming<lease_v2::LeaseKeepAliveRequest>>,
) -> Result<Response<ReceiverStream<Result<lease_v2::LeaseKeepAliveResponse, Status>>>, Status> {
    require_raft(node)?;
    let raft = node.raft.clone();
    let node_id = node.node_id;
    let storage = Arc::clone(&node.storage);
    let timeout = node.lease_write_timeout();
    let mut stream = request.into_inner();
    let (tx, rx) = mpsc::channel::<Result<lease_v2::LeaseKeepAliveResponse, Status>>(16);

    tokio::spawn(async move {
        while let Ok(Some(req)) = stream.message().await {
            let Some(raft) = raft.as_ref() else {
                let _ = tx
                    .send(Err(failed_precondition("region 0 raft missing")))
                    .await;
                break;
            };
            if raft.current_leader().await != Some(node_id) {
                let _ = tx
                    .send(Err(attach(
                        Status::unavailable(format!(
                            "lease keep-alive rejected: node {node_id} is not the leader"
                        )),
                        CoordErrorCode::NotLeader,
                    )))
                    .await;
                break;
            }

            let record = match storage.get_lease_v2_record(req.lease_id) {
                Ok(Some(record)) if !record.revoked => record,
                Ok(_) => {
                    let _ = tx
                        .send(Err(attach(
                            Status::not_found(format!("lease {} not found", req.lease_id)),
                            CoordErrorCode::NotFound,
                        )))
                        .await;
                    break;
                }
                Err(e) => {
                    let _ = tx.send(Err(map_err(e))).await;
                    break;
                }
            };
            if remaining_seconds(record.deadline_wall_ms, wall_clock_now_ms()) <= 0 {
                let _ = tx
                    .send(Err(attach(
                        Status::not_found(format!("lease {} expired", req.lease_id)),
                        CoordErrorCode::NotFound,
                    )))
                    .await;
                break;
            }

            let deadline_wall_ms =
                wall_clock_now_ms().saturating_add(record.ttl.saturating_mul(1000));
            let op = LeaseV2Op::KeepAlive {
                lease_id: req.lease_id,
                deadline_wall_ms,
            };
            let result =
                tokio::time::timeout(timeout, raft.client_write(Command::LeaseV2(op))).await;
            let response = match result {
                Ok(Ok(resp)) => match resp.response() {
                    RaftResponse::LeaseV2KeepAlive { ok: true } => {
                        Some(remaining_seconds(deadline_wall_ms, wall_clock_now_ms()))
                    }
                    RaftResponse::LeaseV2KeepAlive { ok: false } => None,
                    other => {
                        let _ = tx
                            .send(Err(Status::internal(format!(
                                "unexpected raft response for LeaseV2 KeepAlive: {other:?}"
                            ))))
                            .await;
                        break;
                    }
                },
                Ok(Err(e)) => {
                    let _ = tx
                        .send(Err(Status::internal(format!(
                            "raft lease.v2 keep-alive failed: {e}"
                        ))))
                        .await;
                    break;
                }
                Err(_) => {
                    let _ = tx
                        .send(Err(attach(
                            Status::deadline_exceeded("lease keep-alive timed out (no quorum?)"),
                            CoordErrorCode::DeadlineExceeded,
                        )))
                        .await;
                    break;
                }
            };

            match response {
                Some(ttl_seconds) if ttl_seconds > 0 => {
                    if tx
                        .send(Ok(lease_v2::LeaseKeepAliveResponse {
                            lease_id: req.lease_id,
                            ttl_seconds,
                        }))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                _ => {
                    let _ = tx
                        .send(Err(attach(
                            Status::not_found(format!("lease {} not found", req.lease_id)),
                            CoordErrorCode::NotFound,
                        )))
                        .await;
                    break;
                }
            }
        }
    });

    Ok(Response::new(ReceiverStream::new(rx)))
}

fn enqueue_lease(node: &CoordNode, pending: &mut BTreeSet<(RegionId, i64, u64)>, id: i64, ts: u64) {
    pending.retain(|(_, lease_id, _)| *lease_id != id);
    if let Some(manager) = &node.region_manager {
        for handle in manager.list_regions() {
            pending.insert((handle.region_id(), id, ts));
        }
    } else {
        // legacy 单 Raft：region 0 就是业务数据面。
        pending.insert((0, id, ts));
    }
}

fn enqueue_revoked_from_storage(node: &CoordNode, pending: &mut BTreeSet<(RegionId, i64, u64)>) {
    match node.storage.list_lease_v2_records() {
        Ok(records) => {
            for (id, record) in records {
                if record.revoked && record.revoked_commit_ts > 0 {
                    enqueue_lease(node, pending, id, record.revoked_commit_ts);
                }
            }
        }
        Err(e) => tracing::warn!("lease.v2 revoker startup scan failed: {e}"),
    }
}

impl CoordNode {
    /// 到期清理 worker：region 0 leader 扫描过期租约并写入 Revoke 决策。
    ///
    /// 只有 `[kv_t].enabled=true` 时由 main 启动；关闭时不存在任何 v2 后台任务。
    pub fn start_kvt_lease_expiry_worker(self: &Arc<Self>) {
        let node = Arc::clone(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(EXPIRY_TICK);
            loop {
                tick.tick().await;
                if node.raft.is_none() || node.ensure_lease_leader().await.is_err() {
                    continue;
                }
                let now = wall_clock_now_ms();
                let expired: Vec<(i64, LeaseV2Record)> = match node.storage.list_lease_v2_records()
                {
                    Ok(records) => records
                        .into_iter()
                        .filter(|(_, record)| !record.revoked && record.deadline_wall_ms <= now)
                        .collect(),
                    Err(e) => {
                        tracing::warn!("lease.v2 expiry scan failed: {e}");
                        continue;
                    }
                };
                for (lease_id, observed) in expired {
                    // 读取与提案之间可能刚被 KeepAlive 续期：重新读一次再判。
                    match node.storage.get_lease_v2_record(lease_id) {
                        Ok(Some(current))
                            if !current.revoked
                                && current.deadline_wall_ms <= wall_clock_now_ms() => {}
                        _ => continue,
                    }
                    let commit_ts = match next_ts(&node).await {
                        Ok(ts) => ts,
                        Err(e) => {
                            tracing::warn!(lease_id, "lease.v2 expiry TSO allocation failed: {e}");
                            continue;
                        }
                    };
                    let op = LeaseV2Op::Revoke {
                        lease_id,
                        commit_ts,
                        expected_deadline_wall_ms: observed.deadline_wall_ms,
                        request_id: Vec::new(),
                    };
                    match submit(&node, op).await {
                        Ok(RaftResponse::LeaseV2Revoke { commit_ts }) if commit_ts > 0 => {
                            tracing::info!(lease_id, commit_ts, "lease.v2 expired; cleanup queued");
                        }
                        Ok(other) => {
                            tracing::debug!(lease_id, response = %other, "lease.v2 expiry no-op");
                        }
                        Err(e) => {
                            tracing::warn!(lease_id, "lease.v2 expiry revoke failed: {e}");
                        }
                    }
                }
            }
        });
    }

    /// per-Region v2 租约清理 worker。
    ///
    /// 启动时扫描已 Revoke 记录（重启续跑），之后消费 region 0 apply 广播。
    /// 每个数据 Region 只由本节点担任 leader 时提交；其他 leader 节点同样收到
    /// 广播/执行启动扫描（幂等）。
    pub fn start_kvt_lease_revoker(self: &Arc<Self>, mut rx: mpsc::UnboundedReceiver<(i64, u64)>) {
        let node = Arc::clone(self);
        tokio::spawn(async move {
            let mut pending: BTreeSet<(RegionId, i64, u64)> = BTreeSet::new();
            let mut need_ts: BTreeSet<i64> = BTreeSet::new();
            let mut ticks: u32 = 0;
            enqueue_revoked_from_storage(&node, &mut pending);

            let mut tick = tokio::time::interval(REVOKER_TICK);
            loop {
                tokio::select! {
                    maybe = rx.recv() => {
                        match maybe {
                            Some((lease_id, commit_ts)) => {
                                enqueue_lease(&node, &mut pending, lease_id, commit_ts);
                            }
                            None => break,
                        }
                    }
                    _ = tick.tick() => {
                        ticks = ticks.wrapping_add(1);
                        if ticks.is_multiple_of(REVOKER_RESCAN_TICKS) {
                            enqueue_revoked_from_storage(&node, &mut pending);
                        }
                    }
                }

                let timeout = node.lease_write_timeout();
                let mut done: Vec<(RegionId, i64, u64)> = Vec::new();
                for (region_id, lease_id, commit_ts) in pending.iter().copied() {
                    let Some(manager) = &node.region_manager else {
                        // legacy：region 0 由本节点 raft 直接服务。
                        let Some(raft) = node.raft.as_ref() else {
                            continue;
                        };
                        if raft.current_leader().await != Some(node.node_id) {
                            continue;
                        }
                        let cmd = Command::DeleteKeysByLeaseV2 {
                            lease_id,
                            commit_ts,
                        };
                        match tokio::time::timeout(timeout, raft.client_write(cmd)).await {
                            Ok(Ok(resp)) => match resp.response() {
                                RaftResponse::DeleteRange { .. } => {
                                    done.push((region_id, lease_id, commit_ts))
                                }
                                RaftResponse::CommitRejected { .. } => {
                                    done.push((region_id, lease_id, commit_ts));
                                    need_ts.insert(lease_id);
                                }
                                other => {
                                    tracing::warn!(lease_id, response = %other, "unexpected lease.v2 cleanup response");
                                }
                            },
                            Ok(Err(e)) => {
                                tracing::warn!(lease_id, "lease.v2 cleanup propose failed: {e}");
                            }
                            Err(_) => {}
                        }
                        continue;
                    };

                    let Some(rt) = manager.runtime(region_id) else {
                        continue;
                    };
                    match rt.raft.current_leader().await {
                        Some(leader) if leader == node.node_id => {
                            let cmd = Command::DeleteKeysByLeaseV2 {
                                lease_id,
                                commit_ts,
                            };
                            match tokio::time::timeout(timeout, rt.raft.client_write(cmd)).await {
                                Ok(Ok(resp)) => match resp.response() {
                                    RaftResponse::DeleteRange { .. } => {
                                        done.push((region_id, lease_id, commit_ts))
                                    }
                                    RaftResponse::CommitRejected { .. } => {
                                        done.push((region_id, lease_id, commit_ts));
                                        need_ts.insert(lease_id);
                                    }
                                    other => {
                                        tracing::warn!(region_id, lease_id, response = %other, "unexpected lease.v2 cleanup response");
                                    }
                                },
                                Ok(Err(e)) => {
                                    tracing::warn!(
                                        region_id,
                                        lease_id,
                                        "lease.v2 cleanup propose failed: {e}"
                                    );
                                }
                                Err(_) => {}
                            }
                        }
                        Some(_) => done.push((region_id, lease_id, commit_ts)),
                        None => {}
                    }
                }
                for key in done {
                    pending.remove(&key);
                }

                if need_ts.is_empty() {
                    continue;
                }
                if node.ensure_lease_leader().await.is_err() {
                    continue;
                }
                for lease_id in std::mem::take(&mut need_ts) {
                    let record = match node.storage.get_lease_v2_record(lease_id) {
                        Ok(Some(record)) => record,
                        Ok(None) => continue,
                        Err(e) => {
                            tracing::warn!(lease_id, "lease.v2 cleanup record read failed: {e}");
                            need_ts.insert(lease_id);
                            continue;
                        }
                    };
                    if !record.revoked {
                        continue;
                    }
                    let commit_ts = match next_ts(&node).await {
                        Ok(ts) => ts,
                        Err(e) => {
                            tracing::warn!(lease_id, "lease.v2 cleanup TSO retry failed: {e}");
                            need_ts.insert(lease_id);
                            continue;
                        }
                    };
                    let op = LeaseV2Op::Revoke {
                        lease_id,
                        commit_ts,
                        expected_deadline_wall_ms: 0,
                        request_id: Vec::new(),
                    };
                    match submit(&node, op).await {
                        Ok(RaftResponse::LeaseV2Revoke { commit_ts }) if commit_ts > 0 => {
                            // apply 广播会重新入队；这里也直接入队，避免依赖
                            // 通道是否在 apply 前被消费。
                            enqueue_lease(&node, &mut pending, lease_id, commit_ts);
                        }
                        Ok(_) => {}
                        Err(e) => {
                            tracing::warn!(lease_id, "lease.v2 cleanup revoke retry failed: {e}");
                            need_ts.insert(lease_id);
                        }
                    }
                }
            }
        });
    }
}
