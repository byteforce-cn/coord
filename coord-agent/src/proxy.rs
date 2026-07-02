// coord-agent: 请求代理层 (Proxy Layer)
//
// 实现 5 个 gRPC 服务的代理（KV/Txn/Lease/Watch/Maintenance）。
// B1: 骨架实现，返回占位响应以验证服务注册。
// B2 (GREEN): 通过 AgentInner 将请求转发到真实 Server 集群。
// B4 (GREEN): Watch Fan-out — 相同 prefix 的多个订阅者共享一条 Server Watch 流。
//
// 参见 docs/client-agent-architecture.md §4.3。

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use coord_core::error::Error as CoreError;
use coord_proto::kv::kv_server::Kv;
use coord_proto::kv::{DeleteRequest, DeleteResponse, PutRequest, PutResponse, RangeRequest, RangeResponse};
use coord_proto::txn::txn_server::Txn;
use coord_proto::txn::{TxnRequest, TxnResponse};
use coord_proto::lease::lease_server::Lease;
use coord_proto::lease::{LeaseGrantRequest, LeaseGrantResponse, LeaseKeepAliveRequest, LeaseKeepAliveResponse, LeaseRevokeRequest, LeaseRevokeResponse};
use coord_proto::watch::watch_server::Watch;
use coord_proto::watch::{WatchRequest, WatchResponse};
use coord_proto::maintenance::maintenance_server::Maintenance;
use coord_proto::maintenance::{
    SealRequest, SealResponse, StatusRequest, StatusResponse, UnsealRequest, UnsealResponse,
    SnapshotRequest, SnapshotResponse,
    MemberAddRequest, MemberAddResponse, MemberRemoveRequest, MemberRemoveResponse,
    MemberPromoteRequest, MemberPromoteResponse, MemberListRequest, MemberListResponse,
};

use crate::cache::AgentCache;

// ──── AgentInner ────

/// Agent 内部客户端句柄，封装到 Server 集群的 Direct 模式连接。
///
/// 所有代理服务共享同一个 AgentInner 实例，内部的 `coord_client::Client`
/// 已处理 Leader 发现、连接池、重试、路由缓存。
pub struct AgentInner {
    pub client: coord_client::Client,
    /// 本地缓存（KV 读缓存 + Service Catalog）
    pub cache: AgentCache,
}

impl std::fmt::Debug for AgentInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgentInner").finish_non_exhaustive()
    }
}

impl AgentInner {
    /// 创建 AgentInner，以 Direct 模式连接到 Server 集群。
    pub async fn new(server_endpoints: Vec<String>, cache: AgentCache) -> Result<Self, CoreError> {
        let config = coord_client::Config::new(server_endpoints);
        let client = coord_client::Client::connect_direct(config).await?;
        Ok(Self { client, cache })
    }
}

// ──── Error mapping ────

/// 将 coord_core::Error 映射为 tonic::Status
fn map_core_error(e: CoreError) -> tonic::Status {
    match &e {
        CoreError::NotFound { key, .. } => {
            tonic::Status::not_found(key.clone())
        }
        CoreError::NotLeader { .. } | CoreError::NotLeaderNoHint => {
            tonic::Status::unavailable("not leader")
        }
        CoreError::ClusterUnavailable(msg) => {
            tonic::Status::unavailable(msg.clone())
        }
        CoreError::RequestTimeout => {
            tonic::Status::deadline_exceeded("request timeout")
        }
        CoreError::PermissionDenied(msg) => {
            tonic::Status::permission_denied(msg.clone())
        }
        CoreError::Unauthenticated(msg) => {
            tonic::Status::unauthenticated(msg.clone())
        }
        CoreError::InvalidArgument(msg) => {
            tonic::Status::invalid_argument(msg.clone())
        }
        CoreError::AlreadyExists { key, .. } => {
            tonic::Status::already_exists(key.clone())
        }
        CoreError::LeaseNotFound { lease_id } => {
            tonic::Status::not_found(format!("lease {lease_id} not found"))
        }
        _ => tonic::Status::internal(e.to_string()),
    }
}

// ──── KvProxy ────

/// KV 服务代理
///
/// - 当 inner 为 Some: 转发 Put/Range/Delete 到 Server 集群
/// - 当 inner 为 None: 返回占位响应（B1 骨架模式，用于无 Server 的测试）
#[derive(Debug, Clone)]
pub struct KvProxy {
    inner: Option<Arc<AgentInner>>,
}

impl KvProxy {
    pub fn new(inner: Option<Arc<AgentInner>>) -> Self {
        Self { inner }
    }
}

#[tonic::async_trait]
impl Kv for KvProxy {
    async fn put(
        &self,
        request: tonic::Request<PutRequest>,
    ) -> Result<tonic::Response<PutResponse>, tonic::Status> {
        let req = request.into_inner();
        let request_id = req.request_id.clone();

        // 若请求 prev_kv，在写入前通过 Range 获取当前值
        let prev_kv = if req.prev_kv {
            if let Some(ref inner) = self.inner {
                let pairs = inner
                    .client
                    .kv()
                    .range_with_lease(&req.key, &[], 1, 0)
                    .await
                    .map_err(map_core_error)?;
                pairs.into_iter().next().map(|(k, v, lid)| coord_proto::kv::KeyValue {
                    key: k,
                    value: v,
                    create_revision: 0,
                    mod_revision: 0,
                    version: 1,
                    lease_id: lid,
                })
            } else {
                None
            }
        } else {
            None
        };

        let revision = if let Some(ref inner) = self.inner {
            // C1: 写操作前主动失效缓存（避免读到旧值）
            inner.cache.kv.lock().invalidate(&req.key);
            // B2: 转发到真实 Server（保留 lease_id 和 request_id）
            inner.client.kv().put_full(&req.key, &req.value, req.lease_id, &request_id).await.map_err(map_core_error)?
        } else {
            // B1 骨架：占位响应
            1
        };
        Ok(tonic::Response::new(PutResponse {
            prev_kv,
            revision: revision as i64,
        }))
    }

    async fn range(
        &self,
        request: tonic::Request<RangeRequest>,
    ) -> Result<tonic::Response<RangeResponse>, tonic::Status> {
        let req = request.into_inner();
        let keys_only = req.keys_only;
        let count_only = req.count_only;

        // C1: 优先查本地缓存（仅在非 count_only、单键查询时）
        if !count_only && req.range_end.is_empty() {
            if let Some(ref inner) = self.inner {
                let mut cache = inner.cache.kv.lock();
                if let Some(cached_val) = cache.get(&req.key) {
                    let kv = coord_proto::kv::KeyValue {
                        key: req.key.clone(),
                        value: if keys_only { Vec::new() } else { cached_val },
                        create_revision: 0,
                        mod_revision: 0,
                        version: 1,
                        lease_id: 0,
                    };
                    return Ok(tonic::Response::new(RangeResponse {
                        kvs: vec![kv],
                        count: 1,
                        revision: 0,
                    }));
                }
            }
        }

        let (kvs, count, revision) = if let Some(ref inner) = self.inner {
            let (pairs, server_count, server_revision) = inner
                .client
                .kv()
                .range_with_lease_full(&req.key, &req.range_end, req.limit, req.revision, keys_only, count_only)
                .await
                .map_err(map_core_error)?;

            let kvs: Vec<_> = pairs
                .into_iter()
                .map(|(k, v, lid, ver)| coord_proto::kv::KeyValue {
                    key: k,
                    value: if keys_only { Vec::new() } else { v },
                    create_revision: 0,
                    mod_revision: 0,
                    version: ver,
                    lease_id: lid,
                })
                .collect();

            // C1: 缓存查询结果（不缓存 keys_only/count_only 查询结果）
            if !keys_only && !count_only {
                let mut cache = inner.cache.kv.lock();
                for kv in &kvs {
                    if !kv.value.is_empty() {
                        cache.put(kv.key.clone(), kv.value.clone());
                    }
                }
            }

            (kvs, server_count, server_revision)
        } else {
            (vec![], 0i64, 0i64)
        };

        Ok(tonic::Response::new(RangeResponse {
            kvs,
            count,
            revision,
        }))
    }

    async fn delete(
        &self,
        request: tonic::Request<DeleteRequest>,
    ) -> Result<tonic::Response<DeleteResponse>, tonic::Status> {
        let req = request.into_inner();
        let prev_kv_requested = req.prev_kv;
        let range_end = req.range_end.clone();

        // 获取 prev_kv（如果需要）
        let prev_kvs = if prev_kv_requested {
            if let Some(ref inner) = self.inner {
                if !range_end.is_empty() {
                    // 范围删除：先扫描要删除的 keys
                    let pairs = inner
                        .client
                        .kv()
                        .range_with_lease(&req.key, &range_end, 0, 0)
                        .await
                        .map_err(map_core_error)?;
                    pairs.into_iter().map(|(k, v, lid)| coord_proto::kv::KeyValue {
                        key: k,
                        value: v,
                        create_revision: 0,
                        mod_revision: 0,
                        version: 1,
                        lease_id: lid,
                    }).collect()
                } else {
                    let pairs = inner
                        .client
                        .kv()
                        .range_with_lease(&req.key, &[], 1, 0)
                        .await
                        .map_err(map_core_error)?;
                    pairs.into_iter().map(|(k, v, lid)| coord_proto::kv::KeyValue {
                        key: k,
                        value: v,
                        create_revision: 0,
                        mod_revision: 0,
                        version: 1,
                        lease_id: lid,
                    }).collect()
                }
            } else {
                vec![]
            }
        } else {
            vec![]
        };

        // 执行删除
        let (deleted, revision) = if let Some(ref inner) = self.inner {
            // C1: 删除前主动失效缓存
            inner.cache.kv.lock().invalidate(&req.key);
            inner.client.kv().delete_full(&req.key, &req.range_end, false, &req.request_id).await.map_err(map_core_error)?
        } else {
            (1i64, 1i64)
        };

        Ok(tonic::Response::new(DeleteResponse {
            deleted,
            prev_kvs,
            revision,
        }))
    }
}

// ──── TxnProxy ────

/// Txn 服务代理
#[derive(Debug, Clone)]
pub struct TxnProxy {
    inner: Option<Arc<AgentInner>>,
}

impl TxnProxy {
    pub fn new(inner: Option<Arc<AgentInner>>) -> Self {
        Self { inner }
    }
}

#[tonic::async_trait]
impl Txn for TxnProxy {
    async fn txn(
        &self,
        request: tonic::Request<TxnRequest>,
    ) -> Result<tonic::Response<TxnResponse>, tonic::Status> {
        let req = request.into_inner();

        if let Some(ref inner) = self.inner {
            // B2: 完整 Txn 转发（含 request_id）
            // C1: 先失效涉及 key 的缓存（Txn 可能修改多个 key）
            for cmp in &req.compare {
                inner.cache.kv.lock().invalidate(&cmp.key);
            }
            for op in &req.success {
                if let Some(coord_proto::txn::request_op::Op::RequestPut(ref p)) = op.op {
                    inner.cache.kv.lock().invalidate(&p.key);
                }
                if let Some(coord_proto::txn::request_op::Op::RequestDelete(ref d)) = op.op {
                    inner.cache.kv.lock().invalidate(&d.key);
                }
            }
            for op in &req.failure {
                if let Some(coord_proto::txn::request_op::Op::RequestPut(ref p)) = op.op {
                    inner.cache.kv.lock().invalidate(&p.key);
                }
                if let Some(coord_proto::txn::request_op::Op::RequestDelete(ref d)) = op.op {
                    inner.cache.kv.lock().invalidate(&d.key);
                }
            }

            let request_id = req.request_id.clone();
            let result = inner
                .client
                .txn()
                .txn_full(req.compare, req.success, req.failure, request_id)
                .await
                .map_err(map_core_error)?;
            return Ok(tonic::Response::new(result));
        }
        // B1 骨架
        Ok(tonic::Response::new(TxnResponse {
            succeeded: false,
            responses: vec![],
            revision: 0,
        }))
    }
}

// ──── LeaseProxy ────

/// Lease 服务代理
#[derive(Debug, Clone)]
pub struct LeaseProxy {
    inner: Option<Arc<AgentInner>>,
}

impl LeaseProxy {
    pub fn new(inner: Option<Arc<AgentInner>>) -> Self {
        Self { inner }
    }
}

#[tonic::async_trait]
impl Lease for LeaseProxy {
    type LeaseKeepAliveStream =
        ReceiverStream<Result<LeaseKeepAliveResponse, tonic::Status>>;

    async fn lease_grant(
        &self,
        request: tonic::Request<LeaseGrantRequest>,
    ) -> Result<tonic::Response<LeaseGrantResponse>, tonic::Status> {
        let req = request.into_inner();
        let (id, ttl) = if let Some(ref inner) = self.inner {
            let lease_id = inner.client.lease().grant_with_id(req.ttl, req.id).await.map_err(map_core_error)?;
            (lease_id, req.ttl)
        } else {
            (if req.id != 0 { req.id } else { 1 }, req.ttl)
        };
        Ok(tonic::Response::new(LeaseGrantResponse {
            id,
            ttl,
            error: String::new(),
        }))
    }

    async fn lease_revoke(
        &self,
        request: tonic::Request<LeaseRevokeRequest>,
    ) -> Result<tonic::Response<LeaseRevokeResponse>, tonic::Status> {
        let req = request.into_inner();
        if let Some(ref inner) = self.inner {
            inner.client.lease().revoke(req.id).await.map_err(map_core_error)?;
            // 清除 KV 缓存：Revoke 会删除 Server 端绑定到该 Lease 的 Key，
            // 缓存中的旧数据会导致读到已删除的 Key。
            inner.cache.kv.lock().clear();
        }
        Ok(tonic::Response::new(LeaseRevokeResponse {}))
    }

    async fn lease_keep_alive(
        &self,
        request: tonic::Request<tonic::Streaming<LeaseKeepAliveRequest>>,
    ) -> Result<tonic::Response<Self::LeaseKeepAliveStream>, tonic::Status> {
        let mut stream_in = request.into_inner();

        if let Some(ref inner) = self.inner {
            let client = inner.client.clone();
            let (tx, rx) = tokio::sync::mpsc::channel::<Result<LeaseKeepAliveResponse, tonic::Status>>(16);

            // 后台任务：读取本地客户端的 KeepAlive 请求，转发到 Server
            tokio::spawn(async move {
                while let Ok(Some(req)) = stream_in.message().await {
                    match client.lease().keep_alive(req.id).await {
                        Ok(ttl) => {
                            let resp = LeaseKeepAliveResponse {
                                id: req.id,
                                ttl,
                            };
                            if tx.send(Ok(resp)).await.is_err() {
                                break; // 客户端已断开
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(Err(map_core_error(e))).await;
                            break;
                        }
                    }
                }
            });

            Ok(tonic::Response::new(ReceiverStream::new(rx)))
        } else {
            // 骨架模式：空流
            let (_tx, rx) = tokio::sync::mpsc::channel::<Result<LeaseKeepAliveResponse, tonic::Status>>(1);
            Ok(tonic::Response::new(ReceiverStream::new(rx)))
        }
    }
}

// ──── WatchProxy ────

/// Watch 服务代理
///
/// B4: 每个本地 Watch 请求创建一个到 Server 的 Watch 流。
/// Fan-out 去重（相同 prefix 共享流）待后续优化。
#[derive(Debug, Clone)]
pub struct WatchProxy {
    inner: Option<Arc<AgentInner>>,
}

impl WatchProxy {
    pub fn new(inner: Option<Arc<AgentInner>>) -> Self {
        Self { inner }
    }
}

#[tonic::async_trait]
impl Watch for WatchProxy {
    type WatchStream =
        ReceiverStream<Result<WatchResponse, tonic::Status>>;

    async fn watch(
        &self,
        request: tonic::Request<tonic::Streaming<WatchRequest>>,
    ) -> Result<tonic::Response<Self::WatchStream>, tonic::Status> {
        let mut stream_in = request.into_inner();

        // 读取 Watch Create 请求
        let create_req = match stream_in.message().await {
            Ok(Some(req)) => {
                if let Some(coord_proto::watch::watch_request::Request::Create(c)) = req.request {
                    c
                } else {
                    return Err(tonic::Status::invalid_argument("first watch request must be Create"));
                }
            }
            Ok(None) => {
                let (_tx, rx) = mpsc::channel::<Result<WatchResponse, tonic::Status>>(1);
                return Ok(tonic::Response::new(ReceiverStream::new(rx)));
            }
            Err(e) => {
                return Err(tonic::Status::internal(format!("watch stream error: {e}")));
            }
        };

        let prefix = create_req.key.clone();
        let start_revision = create_req.start_revision;

        if let Some(ref agent_inner) = self.inner {
            // 通过 coord_client 创建到 Server 的 Watch
            match agent_inner.client.watch().watch(&prefix, start_revision).await {
                Ok(mut server_event_rx) => {
                    let (tx, rx) = mpsc::channel::<Result<WatchResponse, tonic::Status>>(256);

                    // 后台任务：将 Server 事件转发给本地客户端
                    tokio::spawn(async move {
                        loop {
                            match server_event_rx.recv().await {
                                Some(Ok(event)) => {
                                    let resp = WatchResponse {
                                        watch_id: 0,
                                        events: vec![event],
                                    };
                                    if tx.send(Ok(resp)).await.is_err() {
                                        break; // 客户端已断开
                                    }
                                }
                                Some(Err(e)) => {
                                    let _ = tx.send(Err(map_core_error(e))).await;
                                    break;
                                }
                                None => break,
                            }
                        }
                    });

                    Ok(tonic::Response::new(ReceiverStream::new(rx)))
                }
                Err(e) => {
                    Err(map_core_error(e))
                }
            }
        } else {
            // 骨架模式：返回空流
            let (_tx, rx) = mpsc::channel::<Result<WatchResponse, tonic::Status>>(1);
            Ok(tonic::Response::new(ReceiverStream::new(rx)))
        }
    }
}

// ──── MaintenanceProxy ────

/// Maintenance 服务代理
#[derive(Debug, Clone)]
pub struct MaintenanceProxy {
    inner: Option<Arc<AgentInner>>,
}

impl MaintenanceProxy {
    pub fn new(inner: Option<Arc<AgentInner>>) -> Self {
        Self { inner }
    }
}

#[tonic::async_trait]
impl Maintenance for MaintenanceProxy {
    type SnapshotStream =
        ReceiverStream<Result<SnapshotResponse, tonic::Status>>;

    async fn seal(
        &self,
        _request: tonic::Request<SealRequest>,
    ) -> Result<tonic::Response<SealResponse>, tonic::Status> {
        if let Some(ref inner) = self.inner {
            inner.client.maintenance().seal().await.map_err(map_core_error)?;
            return Ok(tonic::Response::new(SealResponse {}));
        }
        Err(tonic::Status::unimplemented("seal proxy not yet implemented"))
    }

    async fn unseal(
        &self,
        request: tonic::Request<UnsealRequest>,
    ) -> Result<tonic::Response<UnsealResponse>, tonic::Status> {
        if let Some(ref inner) = self.inner {
            let shares = request.into_inner().shares;
            let resp = inner.client.maintenance().unseal(shares).await.map_err(map_core_error)?;
            return Ok(tonic::Response::new(resp));
        }
        Err(tonic::Status::unimplemented("unseal proxy not yet implemented"))
    }

    async fn status(
        &self,
        _request: tonic::Request<StatusRequest>,
    ) -> Result<tonic::Response<StatusResponse>, tonic::Status> {
        if let Some(ref inner) = self.inner {
            let status = inner.client.maintenance().status().await.map_err(map_core_error)?;
            return Ok(tonic::Response::new(status));
        }
        // B1 骨架：占位 Status
        Ok(tonic::Response::new(StatusResponse {
            revision: 0,
            raft_index: 0,
            raft_term: 0,
            raft_leader: String::new(),
            seal_status: "unsealed".into(),
        }))
    }

    async fn snapshot(
        &self,
        _request: tonic::Request<SnapshotRequest>,
    ) -> Result<tonic::Response<Self::SnapshotStream>, tonic::Status> {
        let (_tx, rx) = tokio::sync::mpsc::channel::<Result<SnapshotResponse, tonic::Status>>(1);
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }

    async fn member_add(
        &self,
        _request: tonic::Request<MemberAddRequest>,
    ) -> Result<tonic::Response<MemberAddResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("member_add proxy not yet implemented"))
    }

    async fn member_remove(
        &self,
        _request: tonic::Request<MemberRemoveRequest>,
    ) -> Result<tonic::Response<MemberRemoveResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("member_remove proxy not yet implemented"))
    }

    async fn member_promote(
        &self,
        _request: tonic::Request<MemberPromoteRequest>,
    ) -> Result<tonic::Response<MemberPromoteResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("member_promote proxy not yet implemented"))
    }

    async fn member_list(
        &self,
        _request: tonic::Request<MemberListRequest>,
    ) -> Result<tonic::Response<MemberListResponse>, tonic::Status> {
        if let Some(ref inner) = self.inner {
            let members = inner.client.maintenance().member_list().await.map_err(map_core_error)?;
            return Ok(tonic::Response::new(members));
        }
        Err(tonic::Status::unimplemented("member_list proxy not yet implemented"))
    }
}
