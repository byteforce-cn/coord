// coord-agent: 请求代理层 (Proxy Layer)
//
// 实现 6 个 gRPC 服务的代理（KV/Txn/Lease/Watch/Maintenance/Storage）。
// B1: 骨架实现，返回占位响应以验证服务注册。
// B2 (GREEN): 通过 AgentInner 将请求转发到真实 Server 集群。
// B4 (GREEN): Watch Fan-out — 相同 prefix 的多个订阅者共享一条 Server Watch 流。
//
// Storage（coord.storage 对象存储）代理 v1：经 coord-client SDK 转发，agent
// 侧缓冲整对象（≤256MiB）后上传/回放——语义与流式协议透传，简化代理实现。
//
// 参见。

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use coord_core::error::Error as CoreError;
use coord_proto::kv::kv_server::Kv;
use coord_proto::kv::{
    DeleteRequest, DeleteResponse, PutRequest, PutResponse, RangeRequest, RangeResponse,
};
use coord_proto::lease::lease_server::Lease;
use coord_proto::lease::{
    LeaseGrantRequest, LeaseGrantResponse, LeaseKeepAliveRequest, LeaseKeepAliveResponse,
    LeaseRevokeRequest, LeaseRevokeResponse,
};
use coord_proto::maintenance::maintenance_server::Maintenance;
use coord_proto::maintenance::{
    CompactRequest, CompactResponse, JoinRequest, JoinResponse, MemberAddRequest,
    MemberAddResponse, MemberListRequest, MemberListResponse, MemberPromoteRequest,
    MemberPromoteResponse, MemberRemoveRequest, MemberRemoveResponse, SealRequest, SealResponse,
    SnapshotRequest, SnapshotResponse, StatusRequest, StatusResponse, UnsealRequest,
    UnsealResponse,
};
use coord_proto::storage::storage_server::Storage;
use coord_proto::storage::{
    get_response, put_request, DeleteRequest as StorageDeleteRequest,
    DeleteResponse as StorageDeleteResponse, GetRequest as StorageGetRequest,
    GetResponse as StorageGetResponse, PutMeta, PutRequest as StoragePutRequest,
    PutResponse as StoragePutResponse, StatRequest as StorageStatRequest,
    StatResponse as StorageStatResponse,
};
use coord_proto::txn::txn_server::Txn;
use coord_proto::txn::{TxnRequest, TxnResponse};
use coord_proto::watch::watch_server::Watch;
use coord_proto::watch::{WatchRequest, WatchResponse};

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
    ///
    /// `tls` 为 Some 时经 TLS/mTLS 通道连接（PEM 字节，来自 `AgentTlsConfig`）；
    /// Server 集群启用 TLS 时必需，否则连接会失败并退化为 skeleton 模式。
    ///
    /// `self_identity` 为 **agent 自身身份**的凭据句柄（F-50，2026-09-19）：
    /// 它只作为**回退**使用 —— 调用方凭据在场时逐字不变地优先调用方。
    pub async fn new(
        server_endpoints: Vec<String>,
        cache: AgentCache,
        tls: Option<coord_client::config::TlsConfig>,
        self_identity: std::sync::Arc<coord_client::credential::CachedTokenProvider>,
    ) -> Result<Self, CoreError> {
        let mut config = coord_client::Config::new(server_endpoints);
        if let Some(t) = tls {
            config = config.with_tls(t);
        }
        // 第四轮 §3.2：agent 出站必须能携带**调用方凭据**。此前 `token_provider`
        // 为 `None` → 生产默认配置（auth_enabled = true）下经 agent 的每一个数据面
        // 请求都被服务端以 `missing CCT token` 拒绝。
        //
        // 这里装的是**按请求**提供者：凭据由 agent 鉴权中间件在放行入站请求时
        // 写入任务局部量（`coord_client::credential::scoped_request_token`）。
        //
        // F-50（2026-09-19）：仅装按请求提供者会让 agent **自己发起**的后台流量
        // 永远无凭据（锁自动续期 / registry 目录加载与订阅 / idgen nodeid 注册
        // 全部 `missing CCT token`）。故改为两级：**调用方凭据优先**
        // （`RequestScopedTokenProvider`），缺失时回退 **agent 自身身份**
        // （`self_identity`，由 `PluginIdentityManager::bootstrap_self_identity`
        // 开通，能力集限定在内部键空间 `/_<domain>/*`）。
        //
        // 顺序本身是安全属性：回退凭据**不可能**顶替调用方身份执行请求；
        // 而入站请求在 agent 鉴权中间件里已被 fail-closed 校验过
        //（无凭据/无能力一律拒绝，不会走到这里）。
        config = config.with_token_provider(std::sync::Arc::new(
            coord_client::credential::FallbackTokenProvider::new(
                std::sync::Arc::new(coord_client::credential::RequestScopedTokenProvider),
                self_identity as std::sync::Arc<dyn coord_client::TokenProvider>,
            ),
        ));
        let client = coord_client::Client::connect_direct(config).await?;
        Ok(Self { client, cache })
    }
}

// ──── Error mapping ────

/// 将 coord_core::Error 映射为 tonic::Status
///
/// 第四轮 §3.14.2：**每个** `Status` 都附上结构化错误码 trailer
/// （`x-coord-error-code`）。此前 Rust 侧零处写入该 trailer，Java `ErrorMapper` 的
/// 首选分支因而是死代码，只有一张 6→12 的有损状态码映射生效：`NOT_FOUND` 被报成
/// "注册中心服务不存在"、not-leader 被报成"agent 挂了"——而 **SDK 的重试矩阵正是
/// 按这些码决策的**（not-leader 应重定向重试；agent 挂了应等待）。
///
/// 映射以 `coord_core::error_code::CoordErrorCode` 为唯一定义；本函数是 agent
/// 数据面（Java 经 agent 接入的主路径）上所有 `CoreError` 的**唯一**出口。
fn map_core_error(e: CoreError) -> tonic::Status {
    use coord_core::error_code::{attach, CoordErrorCode as EC};

    let (status, code) = match &e {
        CoreError::NotFound { key, .. } => (tonic::Status::not_found(key.clone()), EC::NotFound),
        // not-leader 与"集群不可用"必须区分：前者 SDK 应重定向到 leader 后重试，
        // 后者 SDK 应退避等待。修复前两者都是裸 `unavailable`。
        CoreError::NotLeader { .. } | CoreError::NotLeaderNoHint => {
            (tonic::Status::unavailable("not leader"), EC::NotLeader)
        }
        CoreError::ClusterUnavailable(msg) => {
            (tonic::Status::unavailable(msg.clone()), EC::Unavailable)
        }
        CoreError::RequestTimeout => (
            tonic::Status::deadline_exceeded("request timeout"),
            EC::DeadlineExceeded,
        ),
        CoreError::PermissionDenied(msg) => (
            tonic::Status::permission_denied(msg.clone()),
            EC::PermissionDenied,
        ),
        CoreError::Unauthenticated(msg) => (
            tonic::Status::unauthenticated(msg.clone()),
            EC::Unauthenticated,
        ),
        CoreError::InvalidArgument(msg) => (
            tonic::Status::invalid_argument(msg.clone()),
            EC::InvalidArgument,
        ),
        CoreError::AlreadyExists { key, .. } => (
            tonic::Status::already_exists(key.clone()),
            EC::AlreadyExists,
        ),
        CoreError::LeaseNotFound { lease_id } => (
            tonic::Status::not_found(format!("lease {lease_id} not found")),
            EC::NotFound,
        ),
        // Txn CAS 失败是**业务判定**（不是故障、更不是 INTERNAL）：
        // 调用方应读 compare 结果分支，而不是当成不可重试的内部错误。
        CoreError::TxnCompareFailed => (
            tonic::Status::aborted("txn compare failed"),
            EC::TxnCasFailed,
        ),
        CoreError::RevisionCompacted { .. } => (
            tonic::Status::failed_precondition(e.to_string()),
            EC::FailedPrecondition,
        ),
        CoreError::ClusterSealed | CoreError::ClusterUnsealing => (
            tonic::Status::failed_precondition(e.to_string()),
            EC::FailedPrecondition,
        ),
        CoreError::LeaseTTLOutOfRange { .. } => {
            (tonic::Status::out_of_range(e.to_string()), EC::OutOfRange)
        }
        CoreError::WatchTooManyConnections { .. } | CoreError::Backpressure(_) => (
            tonic::Status::resource_exhausted(e.to_string()),
            EC::ResourceExhausted,
        ),
        _ => (tonic::Status::internal(e.to_string()), EC::Internal),
    };
    attach(status, code)
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
                pairs
                    .into_iter()
                    .next()
                    .map(|(k, v, lid)| coord_proto::kv::KeyValue {
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
            // B1：**先写 Server，再失效缓存**。此前是写前失效，存在竞态窗口：
            // 失效与写入之间的并发读会把旧值重新回填进缓存。
            let revision = inner
                .client
                .kv()
                .put_full(&req.key, &req.value, req.lease_id, &request_id)
                .await
                .map_err(map_core_error)?;
            inner.cache.kv.lock().invalidate(&req.key);
            revision
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

        // B1：本地读缓存仅在已显式开启 + 单键 + **非历史读**（revision==0）时参与。
        // 历史读（revision!=0）必须直连 Server，否则会命中最新缓存而返回错误版本。
        if !count_only && req.range_end.is_empty() && req.revision == 0 {
            if let Some(ref inner) = self.inner {
                let mut cache = inner.cache.kv.lock();
                if cache.is_enabled() {
                    if let Some(cached_val) = cache.get(&req.key) {
                        // 缓存只保存 value，不含任何 MVCC 元数据 → 全部置 0 表示
                        // “未知”。**绝不伪造** create_revision/mod_revision/version/
                        // lease_id/response.revision（B1）。
                        let kv = coord_proto::kv::KeyValue {
                            key: req.key.clone(),
                            value: if keys_only { Vec::new() } else { cached_val },
                            create_revision: 0,
                            mod_revision: 0,
                            version: 0,
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
        }

        let (kvs, count, revision) = if let Some(ref inner) = self.inner {
            let (pairs, server_count, server_revision) = inner
                .client
                .kv()
                .range_with_lease_full(
                    &req.key,
                    &req.range_end,
                    req.limit,
                    req.revision,
                    keys_only,
                    count_only,
                )
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
                    pairs
                        .into_iter()
                        .map(|(k, v, lid)| coord_proto::kv::KeyValue {
                            key: k,
                            value: v,
                            create_revision: 0,
                            mod_revision: 0,
                            version: 1,
                            lease_id: lid,
                        })
                        .collect()
                } else {
                    let pairs = inner
                        .client
                        .kv()
                        .range_with_lease(&req.key, &[], 1, 0)
                        .await
                        .map_err(map_core_error)?;
                    pairs
                        .into_iter()
                        .map(|(k, v, lid)| coord_proto::kv::KeyValue {
                            key: k,
                            value: v,
                            create_revision: 0,
                            mod_revision: 0,
                            version: 1,
                            lease_id: lid,
                        })
                        .collect()
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
            inner
                .client
                .kv()
                .delete_full(&req.key, &req.range_end, false, &req.request_id)
                .await
                .map_err(map_core_error)?
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
    type LeaseKeepAliveStream = ReceiverStream<Result<LeaseKeepAliveResponse, tonic::Status>>;

    async fn lease_grant(
        &self,
        request: tonic::Request<LeaseGrantRequest>,
    ) -> Result<tonic::Response<LeaseGrantResponse>, tonic::Status> {
        let req = request.into_inner();
        let (id, ttl) = if let Some(ref inner) = self.inner {
            let lease_id = inner
                .client
                .lease()
                .grant_with_id(req.ttl, req.id)
                .await
                .map_err(map_core_error)?;
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
            inner
                .client
                .lease()
                .revoke(req.id)
                .await
                .map_err(map_core_error)?;
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
            let (tx, rx) =
                tokio::sync::mpsc::channel::<Result<LeaseKeepAliveResponse, tonic::Status>>(16);

            // 后台任务：读取本地客户端的 KeepAlive 请求，转发到 Server
            tokio::spawn(async move {
                while let Ok(Some(req)) = stream_in.message().await {
                    match client.lease().keep_alive(req.id).await {
                        Ok(ttl) => {
                            let resp = LeaseKeepAliveResponse { id: req.id, ttl };
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
            let (_tx, rx) =
                tokio::sync::mpsc::channel::<Result<LeaseKeepAliveResponse, tonic::Status>>(1);
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
    /// R-AGT-20：指标（watch 订阅数实时回写；None = 不采集）
    metrics: Option<crate::metrics::AgentMetrics>,
}

impl WatchProxy {
    pub fn new(inner: Option<Arc<AgentInner>>) -> Self {
        Self {
            inner,
            metrics: None,
        }
    }

    /// R-AGT-20：挂载指标（watch 订阅数实时回写）。
    pub fn with_metrics(mut self, metrics: Option<crate::metrics::AgentMetrics>) -> Self {
        self.metrics = metrics;
        self
    }
}

#[tonic::async_trait]
impl Watch for WatchProxy {
    type WatchStream = ReceiverStream<Result<WatchResponse, tonic::Status>>;

    async fn watch(
        &self,
        request: tonic::Request<tonic::Streaming<WatchRequest>>,
    ) -> Result<tonic::Response<Self::WatchStream>, tonic::Status> {
        // W1-6：Watch 的 scope 判定在**这里**做，不在鉴权层 —— 订阅的 key/前缀在首帧
        // `WatchCreateRequest` 里，而首帧在**流式 body** 中；鉴权层要拿到它就得缓存整个
        // body，而流的 body 在客户端 half-close 前不结束（缓存 ⇒ 请求永久挂起，第四轮
        // P0 的形态）。所以鉴权层在放行时把授权快照塞进请求扩展，handler 拿到首帧后判定。
        //
        // `None` 的语义见 `DeferredScopeGrants`：鉴权关闭 / root / 该请求未经鉴权层。
        // **必须在 `into_inner()` 之前取** —— 扩展随请求一起被消费。
        let grants = request
            .extensions()
            .get::<coord_core::grpc_auth::DeferredScopeGrants>()
            .cloned();
        let mut stream_in = request.into_inner();

        // 读取 Watch Create 请求
        let create_req = match stream_in.message().await {
            Ok(Some(req)) => {
                if let Some(coord_proto::watch::watch_request::Request::Create(c)) = req.request {
                    c
                } else {
                    return Err(tonic::Status::invalid_argument(
                        "first watch request must be Create",
                    ));
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

        // scope 判定（fail-closed）：订阅区间必须被授权 scope **整体覆盖**。
        //
        // 区间由 `watch_create_access` 计算 —— 与投递侧 `key_matches` 同源、与鉴权层的
        // body 提取**同一实现**（`extract_scope_access` 的 Watch 分支调的就是它），
        // 所以"handler 判定的区间"与"实际会投递的 key 集合"不可能漂移。
        let access =
            coord_core::grpc_auth::watch_create_access(&create_req.key, &create_req.range_end);
        if let Err(reason) = coord_core::grpc_auth::check_deferred_scope(grants.as_ref(), &[access])
        {
            // 身份有效但权限不足 ⇒ PERMISSION_DENIED（不是 UNAUTHENTICATED）
            tracing::warn!(
                prefix = %String::from_utf8_lossy(&prefix),
                reason = %reason,
                "watch proxy: subscription denied by deferred scope check"
            );
            return Err(coord_core::error_code::attach(
                tonic::Status::permission_denied(reason),
                coord_core::error_code::CoordErrorCode::PermissionDenied,
            ));
        }

        // R-AGT-20：订阅 +1（退订在转发任务结束时 -1）
        if let Some(ref metrics) = self.metrics {
            metrics.inc_watch_subscribers();
        }

        if let Some(ref agent_inner) = self.inner {
            // 通过 coord_client 创建到 Server 的 Watch
            tracing::debug!(
                prefix = %String::from_utf8_lossy(&prefix),
                start_revision,
                "watch proxy: subscribing upstream"
            );
            match agent_inner
                .client
                .watch()
                .watch(&prefix, start_revision)
                .await
            {
                Ok(mut server_event_rx) => {
                    let (tx, rx) = mpsc::channel::<Result<WatchResponse, tonic::Status>>(256);

                    // 后台任务：将 Server 事件转发给本地客户端
                    let metrics_for_task = self.metrics.clone();
                    // B1：Watch 驱动缓存失效 —— 此前只在 put 侧失效，导致 A 写入后
                    // B 端在本地 TTL 到期前仍读到旧值。
                    let cache_for_task = Arc::clone(agent_inner);
                    tokio::spawn(async move {
                        // R-AGT-20：退订（任务结束/客户端断开时）
                        let _decrement = DecrementGuard {
                            metrics: metrics_for_task,
                        };
                        loop {
                            match server_event_rx.recv().await {
                                Some(Ok(event)) => {
                                    // 任何事件都失效该 watch 前缀下的读缓存（保守失效：
                                    // 宁可多清，不可残留陈旧值）。
                                    cache_for_task.cache.kv.lock().invalidate_prefix(&prefix);
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
                Err(e) => Err(map_core_error(e)),
            }
        } else {
            // 骨架模式：返回空流
            //
            // ⚠️ 这一支**不发出任何事件**，且调用方拿到的是一条看起来正常的空流：
            // 客户端会一直等（`message().await` 永久挂起 / Java 侧超时）。因此这里必须
            // 留下 WARN —— 「watch 静默无事件」是本仓库已经发生过的事故形态
            // （第四轮：流式请求 body 被鉴权层缓存，请求永不转发）。
            tracing::warn!(
                "watch proxy has no upstream client (skeleton mode): returning an empty stream \
                 that will never yield events"
            );
            let (_tx, rx) = mpsc::channel::<Result<WatchResponse, tonic::Status>>(1);
            Ok(tonic::Response::new(ReceiverStream::new(rx)))
        }
    }
}

// ──── MaintenanceProxy ────

/// R-AGT-20：Watch 转发任务退订守卫（任务结束时 metrics -1）。
struct DecrementGuard {
    metrics: Option<crate::metrics::AgentMetrics>,
}

impl Drop for DecrementGuard {
    fn drop(&mut self) {
        if let Some(ref metrics) = self.metrics {
            metrics.dec_watch_subscribers();
        }
    }
}

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
    type SnapshotStream = ReceiverStream<Result<SnapshotResponse, tonic::Status>>;

    async fn seal(
        &self,
        _request: tonic::Request<SealRequest>,
    ) -> Result<tonic::Response<SealResponse>, tonic::Status> {
        if let Some(ref inner) = self.inner {
            inner
                .client
                .maintenance()
                .seal()
                .await
                .map_err(map_core_error)?;
            return Ok(tonic::Response::new(SealResponse {}));
        }
        Err(tonic::Status::unimplemented(
            "seal proxy not yet implemented",
        ))
    }

    async fn unseal(
        &self,
        request: tonic::Request<UnsealRequest>,
    ) -> Result<tonic::Response<UnsealResponse>, tonic::Status> {
        if let Some(ref inner) = self.inner {
            let shares = request.into_inner().shares;
            let resp = inner
                .client
                .maintenance()
                .unseal(shares)
                .await
                .map_err(map_core_error)?;
            return Ok(tonic::Response::new(resp));
        }
        Err(tonic::Status::unimplemented(
            "unseal proxy not yet implemented",
        ))
    }

    async fn status(
        &self,
        _request: tonic::Request<StatusRequest>,
    ) -> Result<tonic::Response<StatusResponse>, tonic::Status> {
        if let Some(ref inner) = self.inner {
            let status = inner
                .client
                .maintenance()
                .status()
                .await
                .map_err(map_core_error)?;
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

    async fn compact(
        &self,
        _request: tonic::Request<CompactRequest>,
    ) -> Result<tonic::Response<CompactResponse>, tonic::Status> {
        // 压缩由运维经 server 直连的 Maintenance::Compact 执行；
        // agent 代理层不转发（集群管理不属 agent 面）
        Err(tonic::Status::unimplemented(
            "compact not available via agent proxy",
        ))
    }

    async fn member_add(
        &self,
        _request: tonic::Request<MemberAddRequest>,
    ) -> Result<tonic::Response<MemberAddResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "member_add proxy not yet implemented",
        ))
    }

    async fn join(
        &self,
        _request: tonic::Request<JoinRequest>,
    ) -> Result<tonic::Response<JoinResponse>, tonic::Status> {
        // agent 代理层不提供 Join（集群管理由 server 直连）
        Err(tonic::Status::unimplemented(
            "join not available via agent proxy",
        ))
    }

    async fn member_remove(
        &self,
        _request: tonic::Request<MemberRemoveRequest>,
    ) -> Result<tonic::Response<MemberRemoveResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "member_remove proxy not yet implemented",
        ))
    }

    async fn member_promote(
        &self,
        _request: tonic::Request<MemberPromoteRequest>,
    ) -> Result<tonic::Response<MemberPromoteResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "member_promote proxy not yet implemented",
        ))
    }

    async fn member_list(
        &self,
        _request: tonic::Request<MemberListRequest>,
    ) -> Result<tonic::Response<MemberListResponse>, tonic::Status> {
        if let Some(ref inner) = self.inner {
            let members = inner
                .client
                .maintenance()
                .member_list()
                .await
                .map_err(map_core_error)?;
            return Ok(tonic::Response::new(members));
        }
        Err(tonic::Status::unimplemented(
            "member_list proxy not yet implemented",
        ))
    }
}

// ──── StorageProxy（coord.storage 对象存储代理） ────

/// 对象存储服务代理：经 coord-client（coord.storage SDK）转发到 Server 集群。
///
/// v1 透传语义（agent 侧缓冲整对象，简化代理；对象 ≤ 256MiB）：
/// - Put：收集客户端流 → `Client::storage().put_chunked`（服务端语义原样透传：
///   已存在 → ALREADY_EXISTS、字节不符 → INVALID_ARGUMENT、leader 变更中断由
///   SDK 重试/错误透传）；
/// - Get：SDK 全量下载后按 ≤4MiB 分片回放 server-stream（首条 stat）；
/// - Stat/Delete：unary 透传（缺失 → NOT_FOUND，与 server 一致）。
const STORAGE_PROXY_MAX_BUFFER: usize = 256 * 1024 * 1024; // 对齐 max_object_size 默认
const STORAGE_PROXY_CHUNK: usize = 4 * 1024 * 1024; // 回放分片（对齐 chunk_size 默认）

#[derive(Debug, Clone)]
pub struct StorageProxy {
    inner: Option<Arc<AgentInner>>,
}

impl StorageProxy {
    pub fn new(inner: Option<Arc<AgentInner>>) -> Self {
        Self { inner }
    }
}

#[tonic::async_trait]
impl Storage for StorageProxy {
    async fn put(
        &self,
        request: tonic::Request<tonic::Streaming<StoragePutRequest>>,
    ) -> Result<tonic::Response<StoragePutResponse>, tonic::Status> {
        let Some(inner) = &self.inner else {
            return Err(tonic::Status::failed_precondition(
                "object storage proxy unavailable: agent not connected to a cluster",
            ));
        };
        let mut stream = request.into_inner();
        // 首条必须为 meta
        let meta: PutMeta = match stream.message().await? {
            Some(m) => match m.part {
                Some(put_request::Part::Meta(meta)) => meta,
                _ => {
                    return Err(tonic::Status::invalid_argument(
                        "first PutRequest message must carry meta",
                    ))
                }
            },
            None => return Err(tonic::Status::invalid_argument("empty Put stream")),
        };
        if meta.bucket.is_empty() {
            return Err(tonic::Status::invalid_argument("bucket must not be empty"));
        }
        // 收集数据（上限防护：声明 total_size 或 256MiB）。
        // `total_size = -1`（未知长度）→ cap 取 0；代理会按实际收到字节经
        // `put_chunked` 重新声明长度，因此未知长度透传同样成立。
        let cap = (meta.total_size.max(0) as usize).min(STORAGE_PROXY_MAX_BUFFER);
        let mut data: Vec<u8> = Vec::with_capacity(cap);
        while let Some(m) = stream.message().await? {
            match m.part {
                Some(put_request::Part::Chunk(c)) => {
                    if data.len().saturating_add(c.len()) > STORAGE_PROXY_MAX_BUFFER {
                        return Err(tonic::Status::resource_exhausted(
                            "object exceeds agent proxy buffer limit (256MiB)",
                        ));
                    }
                    data.extend_from_slice(&c);
                }
                Some(put_request::Part::Meta(_)) => {
                    return Err(tonic::Status::invalid_argument(
                        "meta must only appear as the first message",
                    ))
                }
                None => return Err(tonic::Status::invalid_argument("empty PutRequest message")),
            }
        }
        if data.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "object must contain at least one chunk",
            ));
        }
        let res = inner
            .client
            .storage()
            .put_chunked(&meta.bucket, &meta.object_id, &data, STORAGE_PROXY_CHUNK)
            .await
            .map_err(map_core_error)?;
        Ok(tonic::Response::new(StoragePutResponse {
            revision: res.revision as i64,
            size: res.size as i64,
            chunks: res.chunks as i64,
        }))
    }

    type GetStream = ReceiverStream<Result<StorageGetResponse, tonic::Status>>;

    async fn get(
        &self,
        request: tonic::Request<StorageGetRequest>,
    ) -> Result<tonic::Response<Self::GetStream>, tonic::Status> {
        let Some(inner) = &self.inner else {
            return Err(tonic::Status::failed_precondition(
                "object storage proxy unavailable: agent not connected to a cluster",
            ));
        };
        let req = request.into_inner();
        let od = inner
            .client
            .storage()
            .get(&req.bucket, &req.object_id)
            .await
            .map_err(map_core_error)?;
        let (tx, rx) = mpsc::channel::<Result<StorageGetResponse, tonic::Status>>(4);
        tokio::spawn(async move {
            // 首条 stat
            let stat = StorageGetResponse {
                part: Some(get_response::Part::Stat(od.stat)),
            };
            if tx.send(Ok(stat)).await.is_err() {
                return;
            }
            for c in od.data.chunks(STORAGE_PROXY_CHUNK) {
                let chunk = StorageGetResponse {
                    part: Some(get_response::Part::Chunk(c.to_vec())),
                };
                if tx.send(Ok(chunk)).await.is_err() {
                    return;
                }
            }
        });
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }

    async fn stat(
        &self,
        request: tonic::Request<StorageStatRequest>,
    ) -> Result<tonic::Response<StorageStatResponse>, tonic::Status> {
        let Some(inner) = &self.inner else {
            return Err(tonic::Status::failed_precondition(
                "object storage proxy unavailable: agent not connected to a cluster",
            ));
        };
        let req = request.into_inner();
        let stat = inner
            .client
            .storage()
            .stat(&req.bucket, &req.object_id)
            .await
            .map_err(map_core_error)?
            .ok_or_else(|| {
                tonic::Status::not_found(format!(
                    "object {}/{} not found",
                    req.bucket,
                    String::from_utf8_lossy(&req.object_id)
                ))
            })?;
        Ok(tonic::Response::new(StorageStatResponse {
            stat: Some(stat),
        }))
    }

    async fn delete(
        &self,
        request: tonic::Request<StorageDeleteRequest>,
    ) -> Result<tonic::Response<StorageDeleteResponse>, tonic::Status> {
        let Some(inner) = &self.inner else {
            return Err(tonic::Status::failed_precondition(
                "object storage proxy unavailable: agent not connected to a cluster",
            ));
        };
        let req = request.into_inner();
        let (deleted, revision) = inner
            .client
            .storage()
            .delete_full(&req.bucket, &req.object_id)
            .await
            .map_err(map_core_error)?;
        Ok(tonic::Response::new(StorageDeleteResponse {
            deleted,
            revision: revision as i64,
        }))
    }
}
