//! v2 集群上的 v1 数据面冻结代理（#170/#173）。
//!
//! `[kv_t].enabled=true` 时 v1 KV/Txn/Watch/Lease/Storage 在服务端被策略拒绝
//! （`FAILED_PRECONDITION`）。Agent 的内存路由在 v2 模式下会把 v1 形状的调用
//! 适配到 `coord.*.v2`（供内部服务使用），所以对外暴露的 v1 代理**不能**继续
//! 走同一个客户端——否则 Agent 就成了 D7/D13 明确禁止的 v1 兼容 facade。
//!
//! 这里保留 v1 服务注册（reflection 可见），handler 在本地直接返回与服务端
//! 逐字一致的诊断锚点，先于任何后端调用。

use coord_core::error_code::{attach, CoordErrorCode};
use coord_proto::kv::{
    kv_server::Kv, DeleteRequest, DeleteResponse, PutRequest, PutResponse, RangeRequest,
    RangeResponse,
};
use coord_proto::lease::{
    lease_server::Lease, LeaseGrantRequest, LeaseGrantResponse, LeaseKeepAliveRequest,
    LeaseKeepAliveResponse, LeaseRevokeRequest, LeaseRevokeResponse,
};
use coord_proto::storage::{
    storage_server::Storage, DeleteRequest as StorageDeleteRequest,
    DeleteResponse as StorageDeleteResponse, GetRequest as StorageGetRequest,
    GetResponse as StorageGetResponse, PutRequest as StoragePutRequest,
    PutResponse as StoragePutResponse, StatRequest as StorageStatRequest,
    StatResponse as StorageStatResponse,
};
use coord_proto::txn::{txn_server::Txn, TxnRequest, TxnResponse};
use coord_proto::watch::{watch_server::Watch, WatchRequest, WatchResponse};
use tokio_stream::wrappers::ReceiverStream;

/// 与 `coord-server::kvt::KvTState::v1_gate` 逐字一致（稳定诊断锚点）。
const FROZEN: &str = "v1 data plane is frozen on a v2 cluster: use coord.<domain>.v2 \
                      (ADR-0015 D7/D13)";

fn frozen() -> tonic::Status {
    attach(
        tonic::Status::failed_precondition(FROZEN),
        CoordErrorCode::FailedPrecondition,
    )
}

// ──── KV ────

pub struct FrozenKv;

#[tonic::async_trait]
impl Kv for FrozenKv {
    async fn put(
        &self,
        _request: tonic::Request<PutRequest>,
    ) -> Result<tonic::Response<PutResponse>, tonic::Status> {
        Err(frozen())
    }

    async fn range(
        &self,
        _request: tonic::Request<RangeRequest>,
    ) -> Result<tonic::Response<RangeResponse>, tonic::Status> {
        Err(frozen())
    }

    async fn delete(
        &self,
        _request: tonic::Request<DeleteRequest>,
    ) -> Result<tonic::Response<DeleteResponse>, tonic::Status> {
        Err(frozen())
    }
}

// ──── Txn ────

pub struct FrozenTxn;

#[tonic::async_trait]
impl Txn for FrozenTxn {
    async fn txn(
        &self,
        _request: tonic::Request<TxnRequest>,
    ) -> Result<tonic::Response<TxnResponse>, tonic::Status> {
        Err(frozen())
    }
}

// ──── Lease ────

pub struct FrozenLease;

#[tonic::async_trait]
impl Lease for FrozenLease {
    async fn lease_grant(
        &self,
        _request: tonic::Request<LeaseGrantRequest>,
    ) -> Result<tonic::Response<LeaseGrantResponse>, tonic::Status> {
        Err(frozen())
    }

    async fn lease_revoke(
        &self,
        _request: tonic::Request<LeaseRevokeRequest>,
    ) -> Result<tonic::Response<LeaseRevokeResponse>, tonic::Status> {
        Err(frozen())
    }

    type LeaseKeepAliveStream = ReceiverStream<Result<LeaseKeepAliveResponse, tonic::Status>>;

    async fn lease_keep_alive(
        &self,
        _request: tonic::Request<tonic::Streaming<LeaseKeepAliveRequest>>,
    ) -> Result<tonic::Response<Self::LeaseKeepAliveStream>, tonic::Status> {
        Err(frozen())
    }
}

// ──── Watch ────

pub struct FrozenWatch;

#[tonic::async_trait]
impl Watch for FrozenWatch {
    type WatchStream = ReceiverStream<Result<WatchResponse, tonic::Status>>;

    async fn watch(
        &self,
        _request: tonic::Request<tonic::Streaming<WatchRequest>>,
    ) -> Result<tonic::Response<Self::WatchStream>, tonic::Status> {
        Err(frozen())
    }
}

// ──── Storage ────

pub struct FrozenStorage;

#[tonic::async_trait]
impl Storage for FrozenStorage {
    type GetStream = ReceiverStream<Result<StorageGetResponse, tonic::Status>>;

    async fn put(
        &self,
        _request: tonic::Request<tonic::Streaming<StoragePutRequest>>,
    ) -> Result<tonic::Response<StoragePutResponse>, tonic::Status> {
        Err(frozen())
    }

    async fn get(
        &self,
        _request: tonic::Request<StorageGetRequest>,
    ) -> Result<tonic::Response<Self::GetStream>, tonic::Status> {
        Err(frozen())
    }

    async fn stat(
        &self,
        _request: tonic::Request<StorageStatRequest>,
    ) -> Result<tonic::Response<StorageStatResponse>, tonic::Status> {
        Err(frozen())
    }

    async fn delete(
        &self,
        _request: tonic::Request<StorageDeleteRequest>,
    ) -> Result<tonic::Response<StorageDeleteResponse>, tonic::Status> {
        Err(frozen())
    }
}
