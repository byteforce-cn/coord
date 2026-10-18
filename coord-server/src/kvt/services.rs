//! `coord.<domain>.v2` 的 tonic 服务壳（ADR-0015 G0）。
//!
//! 每个 handler 先过 [`KvTState::gate`]：关闭或能力未交付 → `FAILED_PRECONDITION`。
//! 门禁通过但 handler 尚未实现 → `UNIMPLEMENTED`（这是实现进度问题，不是配置
//! 问题；把两者混成同一个码会让运维无法判断该改配置还是该升级版本）。
//!
//! 服务**始终注册**（只要对应的 tonic server 被挂到 gRPC builder 上）：关闭时
//! reflection 仍然可见，便于客户端在连接阶段就发现"这个集群没有 KV-T"，而不是
//! 拿到 `UNAVAILABLE` 后误判为网络故障。

use tonic::{Request, Response, Status, Streaming};

use coord_proto::kv::v2 as kv_v2;
use coord_proto::lease::v2 as lease_v2;
use coord_proto::storage::v2 as storage_v2;
use coord_proto::tso::v2 as tso_v2;
use coord_proto::txn::v2 as txn_v2;
use coord_proto::watch::v2 as watch_v2;

use coord_core::error::Error;
use coord_core::error_code::{attach, CoordErrorCode};

use super::KvTCapability;
use crate::raft::tso::{normalize_batch_size, TsoAllocator, TsoBatch};
use crate::server::CoordNode;

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
        let ts = client.get_ts().await.map_err(tso_status)?;
        Ok(Response::new(tso_v2::GetTsResponse { ts: ts_to_i64(ts)? }))
    }
}

// ──── coord.kv.v2 ────

#[tonic::async_trait]
impl kv_v2::kv_server::Kv for CoordNode {
    async fn put(
        &self,
        _request: Request<kv_v2::PutRequest>,
    ) -> Result<Response<kv_v2::PutResponse>, Status> {
        Err(shell(KvTCapability::Kv, self))
    }

    async fn range(
        &self,
        _request: Request<kv_v2::RangeRequest>,
    ) -> Result<Response<kv_v2::RangeResponse>, Status> {
        Err(shell(KvTCapability::Kv, self))
    }

    async fn delete(
        &self,
        _request: Request<kv_v2::DeleteRequest>,
    ) -> Result<Response<kv_v2::DeleteResponse>, Status> {
        Err(shell(KvTCapability::Kv, self))
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
