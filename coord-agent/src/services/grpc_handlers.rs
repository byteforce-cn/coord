// coord-agent: gRPC trait implementations for pluggable services
//
// This module implements gRPC service traits (from agent_api.proto) for
// each pluggable service. It bridges between proto request/response types
// and the service's internal public API.
//
// Registry and Config gRPC handlers remain inline in their respective
// service files (registry.rs, config_center.rs).

use std::sync::Arc;

use base64::Engine as _;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use crate::feature_flags::{FeatureFlagService, FlagEvalContext};
use crate::services::replication::{
    ReplicatedStore, ReplicationEntry, ReplicationError, ReplicationManager,
};
use crate::services::{
    cache::CacheService,
    circuit_breaker::CircuitBreakerService,
    event_notification::{CloudEvent, Event, EventNotificationService},
    idgen::IdGenService,
    leader_election::{LeaderElectionService, LeaderRole},
    lock::{LockService, ReleaseOutcome},
    mq::{MessageQueueService, TopicConfig},
    policy::{AccessRequest, PolicyService},
    rate_limiter::RateLimiterService,
    scheduler::SchedulerService,
    transit::TransitService,
    workflow::{WorkflowInstance, WorkflowService, WorkflowState},
};

use coord_proto::agent::{
    cache_server::Cache, circuit_breaker_server::CircuitBreaker, event_server::Event as EventSvc,
    feature_flags_server::FeatureFlags, id_gen_server::IdGen,
    leader_election_server::LeaderElection, lock_server::Lock, mq_server::Mq,
    policy_server::Policy, rate_limiter_server::RateLimiter, replica_server::Replica,
    scheduler_server::Scheduler, transit_server::Transit, workflow_server::Workflow,
    CacheDeleteRequest, CacheDeleteResponse, CacheGetRequest, CacheGetResponse,
    CacheHGetAllRequest, CacheHGetAllResponse, CacheHGetRequest, CacheHGetResponse,
    CacheHSetRequest, CacheHSetResponse, CacheLLenRequest, CacheLLenResponse, CacheLPushRequest,
    CacheLPushResponse, CacheLRangeRequest, CacheLRangeResponse, CacheRPopRequest,
    CacheRPopResponse, CacheSAddRequest, CacheSAddResponse, CacheSMembersRequest,
    CacheSMembersResponse, CacheSetRequest, CacheSetResponse, CircuitBreakerGetStateRequest,
    CircuitBreakerGetStateResponse, CircuitBreakerReportFailureRequest,
    CircuitBreakerReportFailureResponse, CircuitBreakerReportSuccessRequest,
    CircuitBreakerReportSuccessResponse, CircuitBreakerResetRequest, CircuitBreakerResetResponse,
    CloudEventMessage, EventPublishRequest, EventPublishResponse, EventSubscribeRequest,
    EventUnsubscribeRequest, EventUnsubscribeResponse, FeatureFlagEvaluateRequest,
    FeatureFlagEvaluateResponse, FeatureFlagIsEnabledRequest, FeatureFlagIsEnabledResponse,
    IdGenNextBatchRequest, IdGenNextBatchResponse, IdGenNextIdRequest, IdGenNextIdResponse,
    LeaderCampaignRequest, LeaderCampaignResponse, LeaderGetLeaderRequest, LeaderGetLeaderResponse,
    LeaderResignRequest, LeaderResignResponse, LeaderWatchEvent, LeaderWatchRequest,
    LockAcquireRequest, LockAcquireResponse, LockGetInfoRequest, LockGetInfoResponse,
    LockReleaseRequest, LockReleaseResponse, LockRenewRequest, LockRenewResponse, MqAckRequest,
    MqAckResponse, MqCreateTopicRequest, MqCreateTopicResponse, MqDeleteTopicRequest,
    MqDeleteTopicResponse, MqGetTopicLeaderRequest, MqGetTopicLeaderResponse, MqMessage,
    MqMoveToDlqRequest, MqMoveToDlqResponse, MqPollDlqRequest, MqPollDlqResponse, MqPollRequest,
    MqPollResponse, MqPublishRequest, MqPublishResponse, MqSubscribeRequest, PolicyBundleInfo,
    PolicyBundleVersionInfo, PolicyCheckPermissionRequest,
    PolicyCheckPermissionResponse, PolicyDeleteBundleRequest, PolicyDeleteBundleResponse,
    PolicyEvaluateRequest, PolicyEvaluateResponse, PolicyExplainRequest, PolicyExplainResponse,
    PolicyListBundleVersionsRequest, PolicyListBundleVersionsResponse, PolicyListBundlesRequest,
    PolicyListBundlesResponse, PolicyPutBundleRequest, PolicyPutBundleResponse,
    PolicyRollbackBundleRequest, PolicyRollbackBundleResponse, PolicySetBundleEnabledRequest,
    PolicySetBundleEnabledResponse, RateLimiterAllowRequest, RateLimiterAllowResponse,
    ReplicaApplyRequest, ReplicaApplyResponse, ReplicaEntry as ReplicaEntryProto,
    ReplicaHeartbeatRequest, ReplicaHeartbeatResponse, ReplicaReconcileRequest,
    ReplicaShardProgress, SchedulerClaimJobRequest, SchedulerClaimJobResponse,
    SchedulerCompleteJobRequest, SchedulerCompleteJobResponse, SchedulerHeartbeatRequest,
    SchedulerHeartbeatResponse, SchedulerRegisterJobRequest, SchedulerRegisterJobResponse,
    TransitDecryptRequest, TransitDecryptResponse, TransitEncryptRequest, TransitEncryptResponse,
    TransitHmacSignRequest, TransitHmacSignResponse, TransitHmacVerifyRequest,
    TransitHmacVerifyResponse, TransitRewrapRequest, TransitRewrapResponse, WorkflowCancelRequest,
    WorkflowCancelResponse,
    WorkflowDefinitionSummary, WorkflowDefinitionVersion, WorkflowDeployRequest,
    WorkflowDeployResponse, WorkflowGetDefinitionRequest, WorkflowGetDefinitionResponse,
    WorkflowGetStatusRequest, WorkflowGetStatusResponse, WorkflowInstanceSummary,
    WorkflowListDefinitionVersionsRequest, WorkflowListDefinitionVersionsResponse,
    WorkflowListDefinitionsRequest, WorkflowListDefinitionsResponse, WorkflowListInstancesRequest,
    WorkflowListInstancesResponse, WorkflowRollbackDefinitionRequest,
    WorkflowRollbackDefinitionResponse, WorkflowSignalRequest, WorkflowSignalResponse,
    WorkflowStartRequest, WorkflowStartResponse,
};

use tonic::{Request, Response, Status};
/// 内部错误脱敏（与 coord-server 同口径）：详情只进 agent 日志，
/// gRPC 客户端仅收到通用 `internal error`，不泄露存储/引擎内部细节。
fn sanitized_internal<E: std::fmt::Display>(e: E) -> Status {
    tracing::error!(error = %e, "internal error returned to client (sanitized)");
    Status::internal("internal error")
}

/// 数据面复制/存储错误映射：安全可回传的语义错误保留（not leader 为显式
/// 契约），其余脱敏。与 coord-server `map_err` 的字符串模式识别同口径。
///
/// 错误码面（与 `coord_core::error_code` 对齐）：not leader → `NOT_LEADER`
/// （Java SDK 按码决策“重定向到 leader 重试”）；ISR 降级 → `UNAVAILABLE`
/// （可退避重试）；资源不存在 → `NOT_FOUND`；配额拒绝 → `RESOURCE_EXHAUSTED`。
fn map_service_error(e: impl std::fmt::Display) -> Status {
    let msg = e.to_string();
    let lower = msg.to_ascii_lowercase();
    if lower.contains("not leader") {
        return coord_core::error_code::attach(
            Status::failed_precondition(msg),
            coord_core::error_code::CoordErrorCode::NotLeader,
        );
    }
    if lower.contains("isr degraded") {
        // 副本不足 = 可用性条件（可重试等待），不是数据错误
        return coord_core::error_code::attach(
            Status::unavailable(msg),
            coord_core::error_code::CoordErrorCode::Unavailable,
        );
    }
    if lower.contains("not found") {
        return Status::not_found(msg);
    }
    if msg.contains("max_size_bytes") {
        // 容量上界拒绝（B-PL-3 / B-PL-4）：可诊断的语义错误，不脱敏。
        // 锚点由 CacheService::ensure_entry_fits 与 MQ 配额检查
        // （MessageQueueService::ensure_quota_tx）生成。
        return Status::resource_exhausted(msg);
    }
    sanitized_internal(msg)
}

/// MQ 非 Leader 的统一错误：`FAILED_PRECONDITION` + 结构化错误码 `NOT_LEADER`
/// + `coord-leader-hint` trailer（leader 地址）。消费者据此可编程路由，
/// **无需解析错误文案**（G-MQ-1）。
pub const MQ_LEADER_HINT_TRAILER: &str = "coord-leader-hint";

fn mq_not_leader_status(shard: &str, leader: &str) -> Status {
    let status = Status::failed_precondition(format!(
        "not leader for shard '{shard}' (leader is {leader})"
    ));
    let mut status = coord_core::error_code::attach(
        status,
        coord_core::error_code::CoordErrorCode::NotLeader,
    );
    if let Ok(v) = tonic::metadata::MetadataValue::try_from(leader) {
        status.metadata_mut().insert(MQ_LEADER_HINT_TRAILER, v);
    }
    status
}

// ════════════════════════════════════════════════════════════
// Lock Service
// ════════════════════════════════════════════════════════════

#[tonic::async_trait]
impl Lock for LockService {
    async fn acquire(
        &self,
        request: Request<LockAcquireRequest>,
    ) -> Result<Response<LockAcquireResponse>, Status> {
        let req = request.into_inner();
        match LockService::acquire(self, &req.name, &req.holder_id, req.ttl_seconds as u64).await {
            Ok(Some(info)) => Ok(Response::new(LockAcquireResponse {
                acquired: true,
                lease_id: info.lease_id,
                holder_id: info.holder_id,
            })),
            Ok(None) => Ok(Response::new(LockAcquireResponse {
                acquired: false,
                lease_id: 0,
                holder_id: String::new(),
            })),
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    async fn release(
        &self,
        request: Request<LockReleaseRequest>,
    ) -> Result<Response<LockReleaseResponse>, Status> {
        let req = request.into_inner();
        match LockService::release(self, &req.name, &req.holder_id, req.lease_id).await {
            // 契约：成功 / 幂等（锁不存在或已过期）都是 released=true
            Ok(ReleaseOutcome::Released | ReleaseOutcome::Gone) => {
                Ok(Response::new(LockReleaseResponse { released: true }))
            }
            // 契约（`lock.proto` 的 `LockReleaseRequest` 语义承诺逐字）：
            // 「(holder_id, lease_id) 与当前持有者不匹配 → PERMISSION_DENIED」。
            //
            // 不回 `released=false`（那会被读成"锁本来就不在"）也不静默成功：
            // 这是 fencing 的正面判据，必须让调用方看见"你的凭据不对"。
            Ok(ReleaseOutcome::Forbidden) => Err(Status::permission_denied(
                "lock is held by another (holder_id, lease_id)",
            )),
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    async fn renew(
        &self,
        request: Request<LockRenewRequest>,
    ) -> Result<Response<LockRenewResponse>, Status> {
        let req = request.into_inner();
        match LockService::renew(self, &req.name, &req.holder_id, req.lease_id).await {
            // 契约：`new_ttl` = 续约后的 TTL（秒）；**0 = 租约已失效、锁已释放**。
            // 成功路径**不得**回 0 —— 那个值在契约里恰是"锁已经没了"，按契约读返回值
            // 的客户端会在**续期成功的那一刻**认为锁丢了。
            Ok(Some(new_ttl)) => Ok(Response::new(LockRenewResponse {
                new_ttl: new_ttl as i64,
            })),
            // 租约不是本调用方的 / 已失效 ⇒ 契约规定的信号就是 `new_ttl = 0`，
            // 而不是错误码：调用方要的是"这个租约不再是你的"这一可行动事实，
            // 用 NOT_FOUND 表达它反而会被误读成传输故障。
            Ok(None) => Ok(Response::new(LockRenewResponse { new_ttl: 0 })),
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    async fn get_lock_info(
        &self,
        request: Request<LockGetInfoRequest>,
    ) -> Result<Response<LockGetInfoResponse>, Status> {
        let req = request.into_inner();
        match LockService::query(self, &req.name).await {
            Ok(Some(info)) => Ok(Response::new(LockGetInfoResponse {
                name: info.name,
                holder_id: info.holder_id,
                lease_id: info.lease_id,
                acquired_at: info.acquired_at as i64,
                ttl_seconds: info.ttl_secs as i64,
                exists: true,
            })),
            Ok(None) => Ok(Response::new(LockGetInfoResponse::default())),
            Err(e) => Err(sanitized_internal(e)),
        }
    }
}

// ════════════════════════════════════════════════════════════
// IdGen Service
// ════════════════════════════════════════════════════════════

#[tonic::async_trait]
impl IdGen for IdGenService {
    async fn next_id(
        &self,
        request: Request<IdGenNextIdRequest>,
    ) -> Result<Response<IdGenNextIdResponse>, Status> {
        let req = request.into_inner();
        match IdGenService::next_id(self, &req.name).await {
            Ok(id) => Ok(Response::new(IdGenNextIdResponse { id: id as i64 })),
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    async fn next_batch(
        &self,
        request: Request<IdGenNextBatchRequest>,
    ) -> Result<Response<IdGenNextBatchResponse>, Status> {
        let req = request.into_inner();
        let count = if req.count > 0 { req.count as u64 } else { 1 };
        match IdGenService::next_ids(self, &req.name, count).await {
            Ok(ids) => Ok(Response::new(IdGenNextBatchResponse {
                ids: ids.into_iter().map(|id| id as i64).collect(),
            })),
            Err(e) => Err(sanitized_internal(e)),
        }
    }
}

// ════════════════════════════════════════════════════════════
// LeaderElection Service
// ════════════════════════════════════════════════════════════

#[tonic::async_trait]
impl LeaderElection for LeaderElectionService {
    async fn campaign(
        &self,
        request: Request<LeaderCampaignRequest>,
    ) -> Result<Response<LeaderCampaignResponse>, Status> {
        let req = request.into_inner();
        match LeaderElectionService::campaign(
            self,
            &req.group_name,
            &req.candidate_id,
            req.ttl_seconds as u64,
        )
        .await
        {
            Ok(LeaderRole::Leader) => Ok(Response::new(LeaderCampaignResponse {
                elected: true,
                lease_id: 0,
                leader_id: req.candidate_id,
            })),
            Ok(_) => Ok(Response::new(LeaderCampaignResponse {
                elected: false,
                lease_id: 0,
                leader_id: String::new(),
            })),
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    async fn resign(
        &self,
        request: Request<LeaderResignRequest>,
    ) -> Result<Response<LeaderResignResponse>, Status> {
        let req = request.into_inner();
        match LeaderElectionService::resign(self, &req.group_name, &req.candidate_id).await {
            Ok(()) => Ok(Response::new(LeaderResignResponse { resigned: true })),
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    async fn get_leader(
        &self,
        request: Request<LeaderGetLeaderRequest>,
    ) -> Result<Response<LeaderGetLeaderResponse>, Status> {
        let req = request.into_inner();
        match LeaderElectionService::get_group_info(self, &req.group_name) {
            Some(info) => Ok(Response::new(LeaderGetLeaderResponse {
                leader_id: info.leader_id,
                lease_id: info.lease_id,
                elected_at: info.elected_at as i64,
                exists: true,
            })),
            None => Ok(Response::new(LeaderGetLeaderResponse::default())),
        }
    }

    type WatchStream = ReceiverStream<Result<LeaderWatchEvent, Status>>;

    async fn watch(
        &self,
        request: Request<LeaderWatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let req = request.into_inner();
        let group_name = req.group_name.clone();
        let mut rx = LeaderElectionService::subscribe_role_changes(self);
        let (tx, out_rx) = tokio::sync::mpsc::channel(32);

        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok((group, role, _info)) => {
                        if group == group_name || group_name.is_empty() {
                            let event_type = match role {
                                LeaderRole::Leader => 1i32,
                                _ => 2i32,
                            };
                            if tx
                                .send(Ok(LeaderWatchEvent {
                                    r#type: event_type,
                                    group_name: group,
                                    leader_id: String::new(),
                                }))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(out_rx)))
    }
}

// ════════════════════════════════════════════════════════════
// Event Service
// ════════════════════════════════════════════════════════════

/// 事件 → 契约消息（CloudEvents 1.0 简化形态 + 全局序号 seq；G-EV-1）
fn event_to_message(event: &crate::services::event_notification::Event) -> CloudEventMessage {
    let ce = CloudEvent::from_event(event);
    CloudEventMessage {
        id: ce.id,
        specversion: ce.specversion,
        r#type: ce.event_type,
        source: ce.source,
        data: ce.data.unwrap_or_default(),
        data_content_type: ce.datacontenttype.unwrap_or_default(),
        subject: ce.subject.unwrap_or_default(),
        time: ce.time.unwrap_or_default(),
        seq: event.seq,
    }
}

#[tonic::async_trait]
impl EventSvc for EventNotificationService {
    async fn publish(
        &self,
        request: Request<EventPublishRequest>,
    ) -> Result<Response<EventPublishResponse>, Status> {
        let req = request.into_inner();
        let event = Event::new(&req.event_type, &req.source, req.data);
        let event_id = event.id.clone();
        match EventNotificationService::publish(self, event).await {
            Ok(()) => Ok(Response::new(EventPublishResponse { event_id })),
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    type SubscribeStream = ReceiverStream<Result<CloudEventMessage, Status>>;

    async fn subscribe(
        &self,
        request: Request<EventSubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let req = request.into_inner();
        let filter_type = req.event_type.clone();
        let cursor: Option<u64> = if req.cursor.trim().is_empty() {
            None
        } else {
            Some(req.cursor.trim().parse::<u64>().map_err(|_| {
                Status::invalid_argument("event cursor must be a decimal seq")
            })?)
        };

        // 先订阅 live（在补投扫描之前建立 ⇒ 扫描期间的新事件不漏、经 seq 去重）
        let mut rx = EventNotificationService::subscribe(self);
        let (tx, out_rx) = tokio::sync::mpsc::channel(64);

        // 默认路径：实时推送（既有语义不变，不补投）
        if cursor.is_none() {
            tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(event) => {
                            if !filter_type.is_empty() && event.event_type != filter_type {
                                continue;
                            }
                            if tx.send(Ok(event_to_message(&event))).await.is_err() {
                                break;
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            });
            return Ok(Response::new(ReceiverStream::new(out_rx)));
        }

        // 持久化游标路径（G-EV-1）：先按位点补投（保留窗口 = KV 中仍在的事件），
        // 再转入实时；重放与实时之间不丢事件（可能重复——消费方按 seq 幂等）。
        let inner = Arc::clone(self.inner());
        let filter = filter_type.clone();
        let start = cursor.unwrap();
        tokio::spawn(async move {
            let mut last = start;
            'catchup: loop {
                // 水位：补投扫描前读计数器（此后发布的事件全部走 live）
                let watermark =
                    match crate::services::event_notification::read_seq_counter(&inner).await {
                        Ok(w) => w,
                        Err(e) => {
                            let _ = tx.send(Err(sanitized_internal(e))).await;
                            return;
                        }
                    };

                // 分页补投 (last, watermark]
                loop {
                    let page = match crate::services::event_notification::fetch_events_page(
                        &inner, last, watermark, 256,
                    )
                    .await
                    {
                        Ok(p) => p,
                        Err(e) => {
                            let _ = tx.send(Err(sanitized_internal(e))).await;
                            return;
                        }
                    };
                    if page.is_empty() {
                        break;
                    }
                    let mut max_seq = last;
                    for event in &page {
                        max_seq = max_seq.max(event.seq);
                        if !filter.is_empty() && event.event_type != filter {
                            continue;
                        }
                        if tx.send(Ok(event_to_message(event))).await.is_err() {
                            return;
                        }
                    }
                    if max_seq <= last {
                        break;
                    }
                    last = max_seq;
                }
                last = last.max(watermark);

                // 实时阶段：seq ≤ last（= 补投水位）的事件已覆盖 ⇒ 跳过
                loop {
                    match rx.recv().await {
                        Ok(event) => {
                            if event.seq <= last {
                                continue;
                            }
                            let seq = event.seq;
                            if filter.is_empty() || event.event_type == filter {
                                if tx.send(Ok(event_to_message(&event))).await.is_err() {
                                    return;
                                }
                            }
                            last = last.max(seq);
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            // 广播追不上：回到补投（从 last 续扫，KV 中仍在的事件不丢）
                            continue 'catchup;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(out_rx)))
    }

    async fn unsubscribe(
        &self,
        _request: Request<EventUnsubscribeRequest>,
    ) -> Result<Response<EventUnsubscribeResponse>, Status> {
        Ok(Response::new(EventUnsubscribeResponse {}))
    }
}

// ════════════════════════════════════════════════════════════
// Cache Service
// ════════════════════════════════════════════════════════════

#[tonic::async_trait]
impl Cache for CacheService {
    /// 全部数据面操作（含 redb 读）经 `run_blocking` 在阻塞线程池执行，
    /// 避免同步 redb 事务阻塞 agent 的 tokio worker。
    async fn get(
        &self,
        request: Request<CacheGetRequest>,
    ) -> Result<Response<CacheGetResponse>, Status> {
        let req = request.into_inner();
        let key = req.key.clone();
        match self.run_blocking(move |me| me.string_get(&key)).await {
            Ok(Some(value)) => Ok(Response::new(CacheGetResponse { value, found: true })),
            Ok(None) => Ok(Response::new(CacheGetResponse {
                value: vec![],
                found: false,
            })),
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    async fn set(
        &self,
        request: Request<CacheSetRequest>,
    ) -> Result<Response<CacheSetResponse>, Status> {
        let req = request.into_inner();
        let ttl = if req.ttl_seconds > 0 {
            Some(req.ttl_seconds as u64)
        } else {
            None
        };
        if self.replication_enabled() {
            self.string_put_replicated(&req.key, req.value, ttl)
                .await
                .map_err(map_service_error)?;
        } else {
            let key = req.key.clone();
            self.run_blocking(move |me| me.string_put(&key, req.value, ttl))
                .await
                .map_err(map_service_error)?;
        }
        Ok(Response::new(CacheSetResponse {}))
    }

    async fn delete(
        &self,
        request: Request<CacheDeleteRequest>,
    ) -> Result<Response<CacheDeleteResponse>, Status> {
        let req = request.into_inner();
        let deleted = if self.replication_enabled() {
            self.string_delete_replicated(&req.key)
                .await
                .map_err(map_service_error)?
        } else {
            let key = req.key.clone();
            self.run_blocking(move |me| me.string_delete(&key))
                .await
                .map_err(sanitized_internal)?
        };
        Ok(Response::new(CacheDeleteResponse { deleted }))
    }

    async fn h_get(
        &self,
        request: Request<CacheHGetRequest>,
    ) -> Result<Response<CacheHGetResponse>, Status> {
        let req = request.into_inner();
        let key = req.key.clone();
        let field = req.field.clone();
        match self
            .run_blocking(move |me| me.hash_field_get(&key, &field))
            .await
        {
            Ok(Some(value)) => Ok(Response::new(CacheHGetResponse { value, found: true })),
            Ok(None) => Ok(Response::new(CacheHGetResponse {
                value: vec![],
                found: false,
            })),
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    async fn h_set(
        &self,
        request: Request<CacheHSetRequest>,
    ) -> Result<Response<CacheHSetResponse>, Status> {
        let req = request.into_inner();
        if self.replication_enabled() {
            self.hash_field_put_replicated(&req.key, &req.field, req.value, None)
                .await
                .map_err(map_service_error)?;
        } else {
            let key = req.key.clone();
            let field = req.field.clone();
            self.run_blocking(move |me| me.hash_field_put(&key, &field, req.value, None))
                .await
                .map_err(map_service_error)?;
        }
        Ok(Response::new(CacheHSetResponse {}))
    }

    async fn h_get_all(
        &self,
        request: Request<CacheHGetAllRequest>,
    ) -> Result<Response<CacheHGetAllResponse>, Status> {
        let req = request.into_inner();
        let key = req.key.clone();
        match self.run_blocking(move |me| me.hash_get_all(&key)).await {
            Ok(fields) => {
                let map: std::collections::HashMap<String, Vec<u8>> = fields.into_iter().collect();
                Ok(Response::new(CacheHGetAllResponse { fields: map }))
            }
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    async fn l_push(
        &self,
        request: Request<CacheLPushRequest>,
    ) -> Result<Response<CacheLPushResponse>, Status> {
        let req = request.into_inner();
        if self.replication_enabled() {
            self.list_push_left_replicated(&req.key, req.value, None)
                .await
                .map_err(map_service_error)?;
        } else {
            let key = req.key.clone();
            self.run_blocking(move |me| me.list_push_left(&key, req.value, None))
                .await
                .map_err(map_service_error)?;
        }
        let key = req.key.clone();
        match self.run_blocking(move |me| me.list_length(&key)).await {
            Ok(len) => Ok(Response::new(CacheLPushResponse { length: len as i64 })),
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    async fn l_range(
        &self,
        request: Request<CacheLRangeRequest>,
    ) -> Result<Response<CacheLRangeResponse>, Status> {
        let req = request.into_inner();
        let key = req.key.clone();
        match self
            .run_blocking(move |me| me.list_range(&key, req.start, req.stop))
            .await
        {
            Ok(values) => Ok(Response::new(CacheLRangeResponse { values })),
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    async fn r_pop(
        &self,
        request: Request<CacheRPopRequest>,
    ) -> Result<Response<CacheRPopResponse>, Status> {
        let req = request.into_inner();
        if self.replication_enabled() {
            match self.list_pop_replicated(&req.key, true).await {
                Ok(Some(value)) => Ok(Response::new(CacheRPopResponse { value, found: true })),
                Ok(None) => Ok(Response::new(CacheRPopResponse {
                    value: vec![],
                    found: false,
                })),
                Err(e) => Err(map_service_error(e)),
            }
        } else {
            let key = req.key.clone();
            match self.run_blocking(move |me| me.list_pop_right(&key)).await {
                Ok(Some(value)) => Ok(Response::new(CacheRPopResponse { value, found: true })),
                Ok(None) => Ok(Response::new(CacheRPopResponse {
                    value: vec![],
                    found: false,
                })),
                Err(e) => Err(sanitized_internal(e)),
            }
        }
    }

    async fn l_len(
        &self,
        request: Request<CacheLLenRequest>,
    ) -> Result<Response<CacheLLenResponse>, Status> {
        let req = request.into_inner();
        let key = req.key.clone();
        match self.run_blocking(move |me| me.list_length(&key)).await {
            Ok(len) => Ok(Response::new(CacheLLenResponse { length: len as i64 })),
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    async fn s_add(
        &self,
        request: Request<CacheSAddRequest>,
    ) -> Result<Response<CacheSAddResponse>, Status> {
        let req = request.into_inner();
        if self.replication_enabled() {
            self.set_add_replicated(&req.key, req.member, None)
                .await
                .map_err(map_service_error)?;
        } else {
            let key = req.key.clone();
            self.run_blocking(move |me| me.set_add(&key, req.member, None))
                .await
                .map_err(map_service_error)?;
        }
        Ok(Response::new(CacheSAddResponse {}))
    }

    async fn s_members(
        &self,
        request: Request<CacheSMembersRequest>,
    ) -> Result<Response<CacheSMembersResponse>, Status> {
        let req = request.into_inner();
        let key = req.key.clone();
        match self.run_blocking(move |me| me.set_members(&key)).await {
            Ok(members) => Ok(Response::new(CacheSMembersResponse { members })),
            Err(e) => Err(sanitized_internal(e)),
        }
    }
}

// ════════════════════════════════════════════════════════════
// MQ Service
// ════════════════════════════════════════════════════════════

#[tonic::async_trait]
impl Mq for MessageQueueService {
    async fn create_topic(
        &self,
        request: Request<MqCreateTopicRequest>,
    ) -> Result<Response<MqCreateTopicResponse>, Status> {
        let req = request.into_inner();
        let config = TopicConfig {
            partitions: req.partitions as u32,
            retention_secs: 86400,
            max_message_size: 1024 * 1024,
        };
        // redb 写事务移到阻塞线程池
        self.run_blocking(move |me| me.create_topic(&req.topic, config))
            .await
            .map_err(sanitized_internal)?;
        Ok(Response::new(MqCreateTopicResponse {}))
    }

    async fn publish(
        &self,
        request: Request<MqPublishRequest>,
    ) -> Result<Response<MqPublishResponse>, Status> {
        let req = request.into_inner();
        let partition = if req.partition >= 0 {
            req.partition as u32
        } else {
            0
        };

        // `key` 与 `idempotency_key` **不得**被静默丢弃（两条分支都传 `None` 即等于丢弃）：
        // - `key`：引擎未建模独立列 ⇒ 随消息 headers 持久化（hex 编码，键可为任意二进制）；
        // - `idempotency_key`：走引擎的生产者级去重索引
        //   （同键重复 publish 不产生第二个 offset）。
        let mut headers = std::collections::BTreeMap::new();
        if !req.key.is_empty() {
            headers.insert("key".to_string(), hex::encode(&req.key));
        }
        let idempotency_key = if req.idempotency_key.is_empty() {
            None
        } else {
            Some(req.idempotency_key.clone())
        };

        if self.replication_enabled() {
            // 非 Leader 在写入前拒绝（结构化：错误码 NOT_LEADER + leader trailer）
            if let Some(rm) = self.replication_manager() {
                let shard = format!("mq:{}", req.topic);
                if !rm.is_leader(&shard) {
                    return Err(mq_not_leader_status(&shard, &rm.shard_leader(&shard)));
                }
            }
            match self
                .produce_replicated(
                    &req.topic,
                    partition,
                    req.payload,
                    Some(headers),
                    idempotency_key.as_deref(),
                )
                .await
            {
                Ok(offset) => Ok(Response::new(MqPublishResponse {
                    offset: offset as i64,
                })),
                Err(e) => Err(map_service_error(e)),
            }
        } else {
            let topic = req.topic.clone();
            match self
                .run_blocking(move |me| {
                    me.produce_idempotent(
                        &topic,
                        partition,
                        req.payload,
                        Some(headers),
                        idempotency_key.as_deref(),
                    )
                })
                .await
            {
                Ok(offset) => Ok(Response::new(MqPublishResponse {
                    offset: offset as i64,
                })),
                Err(e) => Err(map_service_error(e)),
            }
        }
    }

    type SubscribeStream =
        std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<MqMessage, Status>> + Send>>;

    async fn subscribe(
        &self,
        request: Request<MqSubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let req = request.into_inner();
        // 复制启用时仅分区 Leader 推送；Follower 返回明确「非 Leader」错误
        if let Some(rm) = self.replication_manager() {
            let shard = format!("mq:{}", req.topic);
            if !rm.is_leader(&shard) {
                return Err(mq_not_leader_status(&shard, &rm.shard_leader(&shard)));
            }
        }
        let (tx, out_rx) = tokio::sync::mpsc::channel(64);
        // 基于消费组 offset 的长轮询推送。注册订阅者并回放已提交
        // 偏移之后的消息；此后 produce 直接向该 channel 推送（按偏移过滤）。
        self.subscribe(&req.topic, &req.consumer_group, tx)
            .await
            .map_err(sanitized_internal)?;

        let topic = req.topic.clone();
        let stream = ReceiverStream::new(out_rx).map(move |(partition, record)| {
            Ok(MqMessage {
                topic: topic.clone(),
                partition: partition as i32,
                offset: record.offset as i64,
                key: Vec::new(),
                payload: record.payload,
                timestamp: record.timestamp as i64,
                dlq_reason: String::new(),
                dlq_detail: String::new(),
            })
        });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn ack(&self, request: Request<MqAckRequest>) -> Result<Response<MqAckResponse>, Status> {
        let req = request.into_inner();
        // 消费组偏移为 Leader 本地状态；复制启用时仅 Leader 提交偏移
        if let Some(rm) = self.replication_manager() {
            let shard = format!("mq:{}", req.topic);
            if !rm.is_leader(&shard) {
                return Err(mq_not_leader_status(&shard, &rm.shard_leader(&shard)));
            }
        }
        let partition = if req.partition >= 0 {
            req.partition as u32
        } else {
            0
        };
        let topic = req.topic.clone();
        let group = req.consumer_group.clone();
        // redb 写事务移到阻塞线程池
        self.run_blocking(move |me| me.commit_offset(&group, &topic, partition, req.offset as u64))
            .await
            .map_err(sanitized_internal)?;
        Ok(Response::new(MqAckResponse {}))
    }

    /// 按 offset 批量拉取（poll + ack 即得 at-least-once + 增量游标）。
    /// 复用引擎 `consume()`（从 start_offset 读最多 max_count 条）。
    async fn poll(
        &self,
        request: Request<MqPollRequest>,
    ) -> Result<Response<MqPollResponse>, Status> {
        let req = request.into_inner();
        let partition = if req.partition >= 0 {
            req.partition as u32
        } else {
            0
        };
        let max_count = if req.max_count <= 0 {
            100
        } else {
            req.max_count as u64
        };
        let topic = req.topic.clone();
        let topic_check = req.topic.clone();
        let start_offset = req.start_offset.max(0) as u64;

        // 删除后的 topic 必须返回明确错误（G-MQ-4），不得退化为「空结果」
        let exists = self
            .run_blocking(move |me| me.get_topic_config(&topic_check))
            .await
            .map_err(map_service_error)?
            .is_some();
        if !exists {
            return Err(Status::not_found(format!("topic '{}' not found", req.topic)));
        }

        match self
            .run_blocking(move |me| me.consume(&topic, partition, start_offset, max_count))
            .await
        {
            Ok(records) => {
                let messages = records
                    .into_iter()
                    .map(|r| MqMessage {
                        topic: req.topic.clone(),
                        partition: partition as i32,
                        offset: r.offset as i64,
                        key: Vec::new(), // 引擎当前不持久化 key，见 mq.rs MessageRecord
                        payload: r.payload,
                        timestamp: r.timestamp as i64,
                        dlq_reason: String::new(),
                        dlq_detail: String::new(),
                    })
                    .collect();
                Ok(Response::new(MqPollResponse { messages }))
            }
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    /// 读取死信队列（DLQ 可观测）
    async fn poll_dlq(
        &self,
        request: Request<MqPollDlqRequest>,
    ) -> Result<Response<MqPollDlqResponse>, Status> {
        let req = request.into_inner();
        let partition = if req.partition >= 0 {
            req.partition as u32
        } else {
            0
        };
        let max_count = if req.max_count <= 0 {
            100
        } else {
            req.max_count as u64
        };
        let topic = req.topic.clone();

        match self
            .run_blocking(move |me| me.consume_dlq(&topic, partition, max_count))
            .await
        {
            Ok(records) => {
                let messages = records
                    .into_iter()
                    .map(|r| MqMessage {
                        topic: req.topic.clone(),
                        partition: partition as i32,
                        offset: r.offset as i64,
                        key: Vec::new(),
                        payload: r.payload,
                        timestamp: r.timestamp as i64,
                        // G-MQ-2：DLQ 内容含原因，消费者可读
                        dlq_reason: r.error_reason.unwrap_or_default(),
                        dlq_detail: r.error_detail.unwrap_or_default(),
                    })
                    .collect();
                Ok(Response::new(MqPollDlqResponse { messages }))
            }
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    /// Leader/ISR 拓扑查询（G-MQ-1）：任意 agent 可答；单 agent 时
    /// `replication_enabled=false`、`leader_agent` 为空（调用方视本 agent 为 Leader）。
    async fn get_topic_leader(
        &self,
        request: Request<MqGetTopicLeaderRequest>,
    ) -> Result<Response<MqGetTopicLeaderResponse>, Status> {
        let req = request.into_inner();
        let topic_check = req.topic.clone();
        let cfg = self
            .run_blocking(move |me| me.get_topic_config(&topic_check))
            .await
            .map_err(map_service_error)?
            .ok_or_else(|| Status::not_found(format!("topic '{}' not found", req.topic)))?;

        // ISR 成员枚举结果含自身；单 agent 自动降级 min_isr=1（effective_min_isr）
        let (leader_agent, isr_members, replication_enabled, degraded, min_isr) =
            match self.replication_manager() {
                Some(rm) => {
                    let shard = format!("mq:{}", req.topic);
                    (
                        rm.shard_leader(&shard),
                        rm.isr_members(),
                        true,
                        rm.is_degraded(),
                        rm.effective_min_isr() as u64,
                    )
                }
                None => (String::new(), Vec::new(), false, false, 0),
            };

        Ok(Response::new(MqGetTopicLeaderResponse {
            topic: req.topic,
            leader_agent,
            isr_members,
            replication_enabled,
            degraded,
            partitions: cfg.partitions as i32,
            min_isr,
        }))
    }

    /// 显式移入 DLQ（G-MQ-2，管理路径）：主日志 → DLQ 净额记账，分区不被阻塞；
    /// 消息或 topic 不存在返回 `NOT_FOUND`。ISR 启用时仅 Leader 可写（Follower
    /// 返回 `FAILED_PRECONDITION` + leader 提示），且迁移经复制通道全域一致。
    async fn move_to_dlq(
        &self,
        request: Request<MqMoveToDlqRequest>,
    ) -> Result<Response<MqMoveToDlqResponse>, Status> {
        let req = request.into_inner();
        let partition = if req.partition >= 0 {
            req.partition as u32
        } else {
            0
        };
        let offset = req.offset.max(0) as u64;

        if self.replication_enabled() {
            if let Some(rm) = self.replication_manager() {
                let shard = format!("mq:{}", req.topic);
                if !rm.is_leader(&shard) {
                    return Err(mq_not_leader_status(&shard, &rm.shard_leader(&shard)));
                }
            }
            self.move_to_dlq_replicated(&req.topic, partition, offset, &req.reason, &req.detail)
                .await
                .map_err(map_service_error)?;
        } else {
            let topic = req.topic.clone();
            let reason = req.reason.clone();
            let detail = req.detail.clone();
            self.run_blocking(move |me| me.move_to_dlq(&topic, partition, offset, &reason, &detail))
                .await
                .map_err(map_service_error)?;
        }
        Ok(Response::new(MqMoveToDlqResponse {}))
    }

    /// 删除 topic 并回收全部存量（G-MQ-4，管理路径）。
    ///
    /// ISR 启用时必须向 Leader 调用（Follower 返回 `FAILED_PRECONDITION` +
    /// leader 提示）；删除决定经复制通道全域下发，Follower 幂等应用。
    async fn delete_topic(
        &self,
        request: Request<MqDeleteTopicRequest>,
    ) -> Result<Response<MqDeleteTopicResponse>, Status> {
        let req = request.into_inner();
        let topic = req.topic.clone();

        let stats = if self.replication_enabled() {
            if let Some(rm) = self.replication_manager() {
                let shard = format!("mq:{topic}");
                if !rm.is_leader(&shard) {
                    return Err(mq_not_leader_status(&shard, &rm.shard_leader(&shard)));
                }
            }
            self.delete_topic_replicated(&topic)
                .await
                .map_err(map_service_error)?
        } else {
            self.run_blocking(move |me| me.delete_topic_full(&topic))
                .await
                .map_err(map_service_error)?
        };

        Ok(Response::new(MqDeleteTopicResponse {
            messages_removed: stats.messages_removed,
            dlq_removed: stats.dlq_removed,
            offsets_removed: stats.offsets_removed,
            idempotency_removed: stats.idempotency_removed,
            bytes_reclaimed: stats.bytes_reclaimed,
        }))
    }
}

// ════════════════════════════════════════════════════════════
// Replica Service — Agent↔Agent ISR 数据复制（v2.1 已落地）
// ════════════════════════════════════════════════════════════

/// Replica 服务路由：把复制条目分发到本 agent 的 MQ / Cache 数据面服务。
///
/// 实现 `ReplicatedStore`（供 ReplicationManager 心跳 / Reconcile 调用）
/// 与 `Replica` gRPC trait（供对端 agent 推送 / 拉取 / 心跳）。
pub struct ReplicaRouter {
    manager: Arc<ReplicationManager>,
    mq: Option<Arc<MessageQueueService>>,
    cache: Option<Arc<CacheService>>,
    /// 自身弱引用：心跳任务的签名是 `start_heartbeat(self: &Arc<Self>, ...)`，
    /// 而 `BaseService::start(&self)` 只有 `&self` —— 与 Cache/MQ 数据面服务
    /// 同一模式（装配后 `bind_self_weak` 一次）。
    self_arc: parking_lot::RwLock<Option<std::sync::Weak<ReplicaRouter>>>,
}

impl ReplicaRouter {
    pub fn new(
        manager: Arc<ReplicationManager>,
        mq: Option<Arc<MessageQueueService>>,
        cache: Option<Arc<CacheService>>,
    ) -> Self {
        Self {
            manager,
            mq,
            cache,
            self_arc: parking_lot::RwLock::new(None),
        }
    }

    /// 绑定自身弱引用（装配后调用一次），使插件生命周期能驱动 ISR 心跳。
    pub fn bind_self_weak(&self, me: &Arc<Self>) {
        *self.self_arc.write() = Some(Arc::downgrade(me));
    }

    /// 升级自身强引用（未绑定 / 已释放 → None）。
    pub fn self_arc(&self) -> Option<Arc<Self>> {
        self.self_arc.read().as_ref().and_then(|w| w.upgrade())
    }

    pub fn manager(&self) -> Arc<ReplicationManager> {
        self.manager.clone()
    }
}

// ──── BaseService：插件生命周期（心跳任务归插件所有）────
//
// 迁移前：心跳在 serve 装配期直接 `manager.start_heartbeat(router.clone())`，
// 与任何生命周期无关（服务停止后心跳仍在跑）。
// 迁移后：`start()` 启动心跳、`stop()` 停止心跳，由 `PluginManager` 统一驱动。

#[async_trait::async_trait]
impl crate::service::BaseService for ReplicaRouter {
    fn name(&self) -> &'static str {
        "replication"
    }

    async fn start(&self) -> crate::service::ServiceResult<()> {
        match self.self_arc() {
            Some(me) => {
                me.manager.start_heartbeat(me.clone());
                tracing::info!("ISR replication heartbeat started (plugin lifecycle)");
                Ok(())
            }
            None => Err("replication router: self reference not bound \
                         (bind_self_weak was not called during assembly)"
                .into()),
        }
    }

    async fn stop(&self) -> crate::service::ServiceResult<()> {
        self.manager.stop_heartbeat();
        tracing::info!("ISR replication heartbeat stopped (plugin lifecycle)");
        Ok(())
    }

    fn health_check(&self) -> bool {
        // 复制管理器始终可用（对端可达性由心跳/Reconcile 自行收敛，属数据面
        // 质量而非服务健康）；就绪与否由插件的启动状态表达。
        true
    }
}

impl ReplicatedStore for ReplicaRouter {
    fn shards(&self) -> Vec<String> {
        let mut shards = Vec::new();
        if let Some(c) = &self.cache {
            shards.extend(c.shards());
        }
        if let Some(m) = &self.mq {
            shards.extend(m.shards());
        }
        shards
    }

    fn last_local_sequence(&self, shard: &str) -> u64 {
        if shard.starts_with("mq:") {
            self.mq
                .as_ref()
                .map(|m| m.last_local_sequence(shard))
                .unwrap_or(0)
        } else {
            self.cache
                .as_ref()
                .map(|c| c.last_local_sequence(shard))
                .unwrap_or(0)
        }
    }

    fn apply_entry(&self, entry: &ReplicationEntry) -> Result<(), ReplicationError> {
        if entry.shard_id.starts_with("mq:") {
            self.mq
                .as_ref()
                .ok_or_else(|| ReplicationError::Store("mq service not enabled".to_string()))?
                .apply_entry(entry)
        } else {
            self.cache
                .as_ref()
                .ok_or_else(|| ReplicationError::Store("cache service not enabled".to_string()))?
                .apply_entry(entry)
        }
    }

    fn read_entries(&self, shard: &str, from_seq: u64, limit: u64) -> Vec<ReplicationEntry> {
        if shard.starts_with("mq:") {
            self.mq
                .as_ref()
                .map(|m| m.read_entries(shard, from_seq, limit))
                .unwrap_or_default()
        } else {
            self.cache
                .as_ref()
                .map(|c| c.read_entries(shard, from_seq, limit))
                .unwrap_or_default()
        }
    }
}

#[tonic::async_trait]
impl Replica for ReplicaRouter {
    /// Leader → Follower：应用一条复制条目（幂等；重复条目 applied=true）。
    /// redb 写事务移到阻塞线程池。
    async fn apply(
        &self,
        request: Request<ReplicaApplyRequest>,
    ) -> Result<Response<ReplicaApplyResponse>, Status> {
        let req = request.into_inner();
        let proto = req
            .entry
            .ok_or_else(|| Status::invalid_argument("missing entry"))?;
        let entry = ReplicationEntry::from_proto(&proto)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let mq = self.mq.clone();
        let cache = self.cache.clone();
        let shard_id = entry.shard_id.clone();
        let entry_for_apply = entry.clone();
        // 与 ReplicatedStore::apply_entry 同一路由逻辑（mq: 前缀 → MQ 引擎）。
        // 应用成功即 applied=true（幂等；重复条目 applied=true）。
        let applied = tokio::task::spawn_blocking(move || {
            let r = if shard_id.starts_with("mq:") {
                mq.as_ref()
                    .ok_or_else(|| ReplicationError::Store("mq service not enabled".to_string()))?
                    .apply_entry(&entry_for_apply)
            } else {
                cache
                    .as_ref()
                    .ok_or_else(|| {
                        ReplicationError::Store("cache service not enabled".to_string())
                    })?
                    .apply_entry(&entry_for_apply)
            };
            r.map(|_| true)
        })
        .await
        .map_err(sanitized_internal)?
        .map_err(sanitized_internal)?;
        let last = {
            let mq = self.mq.clone();
            let cache = self.cache.clone();
            let shard_id = entry.shard_id.clone();
            tokio::task::spawn_blocking(move || {
                if shard_id.starts_with("mq:") {
                    mq.as_ref()
                        .map(|m| m.last_local_sequence(&shard_id))
                        .unwrap_or(0)
                } else {
                    cache
                        .as_ref()
                        .map(|c| c.last_local_sequence(&shard_id))
                        .unwrap_or(0)
                }
            })
            .await
            .map_err(sanitized_internal)?
        };
        Ok(Response::new(ReplicaApplyResponse {
            applied,
            last_sequence: last,
        }))
    }

    type ReconcileStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<ReplicaEntryProto, Status>> + Send>,
    >;

    /// Follower → Leader：拉取缺失序列号区间的复制条目（stream 回放）。
    /// redb range 读移到阻塞线程池。
    async fn reconcile(
        &self,
        request: Request<ReplicaReconcileRequest>,
    ) -> Result<Response<Self::ReconcileStream>, Status> {
        let req = request.into_inner();
        let limit = if req.limit == 0 { 1000 } else { req.limit };
        let mq = self.mq.clone();
        let cache = self.cache.clone();
        let shard_id = req.shard_id.clone();
        let entries = tokio::task::spawn_blocking(move || {
            if shard_id.starts_with("mq:") {
                mq.as_ref()
                    .map(|m| m.read_entries(&shard_id, req.start_sequence, limit))
                    .unwrap_or_default()
            } else {
                cache
                    .as_ref()
                    .map(|c| c.read_entries(&shard_id, req.start_sequence, limit))
                    .unwrap_or_default()
            }
        })
        .await
        .map_err(sanitized_internal)?;
        let stream = tokio_stream::iter(entries.into_iter().map(|e| Ok(e.to_proto())));
        Ok(Response::new(Box::pin(stream)))
    }

    /// 双向心跳：维护 ISR 成员 + 交换各 shard 最后序列号（落后检测）。
    /// 各 shard last_seq 的 redb 读移到阻塞线程池。
    async fn isr_heartbeat(
        &self,
        request: Request<ReplicaHeartbeatRequest>,
    ) -> Result<Response<ReplicaHeartbeatResponse>, Status> {
        let req = request.into_inner();
        // 记录对端心跳（ISR 成员维护；对端失败后由 manager 心跳任务移除）
        if !req.agent_addr.is_empty() && req.agent_addr != self.manager.agent_addr() {
            self.manager.add_peer(req.agent_addr.clone());
        }
        // 返回本 agent 各 shard 最后序列号（供对端落后检测触发 Reconcile）
        let mq = self.mq.clone();
        let cache = self.cache.clone();
        let leader_progress = tokio::task::spawn_blocking(move || {
            let mut shards = Vec::new();
            if let Some(c) = &cache {
                shards.extend(c.shards());
            }
            if let Some(m) = &mq {
                shards.extend(m.shards());
            }
            shards
                .into_iter()
                .map(|s| {
                    let last_seq = if s.starts_with("mq:") {
                        mq.as_ref().map(|m| m.last_local_sequence(&s)).unwrap_or(0)
                    } else {
                        cache
                            .as_ref()
                            .map(|c| c.last_local_sequence(&s))
                            .unwrap_or(0)
                    };
                    ReplicaShardProgress {
                        shard_id: s,
                        last_sequence: last_seq,
                    }
                })
                .collect::<Vec<ReplicaShardProgress>>()
        })
        .await
        .map_err(sanitized_internal)?;
        Ok(Response::new(ReplicaHeartbeatResponse {
            in_isr: true,
            leader_progress,
        }))
    }
}

// ════════════════════════════════════════════════════════════
// Scheduler Service
// ════════════════════════════════════════════════════════════

#[tonic::async_trait]
impl Scheduler for SchedulerService {
    async fn register_job(
        &self,
        request: Request<SchedulerRegisterJobRequest>,
    ) -> Result<Response<SchedulerRegisterJobResponse>, Status> {
        let req = request.into_inner();
        let task = crate::services::scheduler::ScheduleTask {
            // task_id 必须与 ClaimJob 查询的键一致（wire 上只有 `name`）：
            // 若用 `helper_uuid()` 作 task_id 注册、却按 `req.name` 认领，
            // ClaimJob 永远找不到任务（"注册成功但永远领不到"）。
            task_id: req.name.clone(),
            task_type: crate::services::scheduler::TaskType::Cron {
                expression: req.cron_expression,
            },
            description: req.name.clone(),
            // payload 必须以**二进制安全**的形式存起来：契约里它是 `bytes`，
            // 而 metadata 是 `String` 表。不得用 `from_utf8_lossy` 直接塞进去
            // ⇒ 非 UTF-8 负载会被**静默替换**成 U+FFFD。故存 base64（原始字节），
            // 同时保留 `payload` 文本键以兼容既有记录（读侧两者都认）。
            metadata: [
                (
                    "payload".to_string(),
                    String::from_utf8_lossy(&req.payload).to_string(),
                ),
                (
                    "payload_b64".to_string(),
                    base64::engine::general_purpose::STANDARD.encode(&req.payload),
                ),
            ]
            .into_iter()
            .collect(),
        };
        self.register_task(task).await.map_err(sanitized_internal)?;
        Ok(Response::new(SchedulerRegisterJobResponse {
            job_id: req.name,
        }))
    }

    async fn claim_job(
        &self,
        request: Request<SchedulerClaimJobRequest>,
    ) -> Result<Response<SchedulerClaimJobResponse>, Status> {
        let req = request.into_inner();
        let worker_id = helper_uuid();
        match self.try_claim(&req.name, &worker_id).await {
            Ok(Some(claim)) => Ok(Response::new(SchedulerClaimJobResponse {
                job_id: claim.task_id,
                // `payload` **不得**硬编码为 `vec![]` —— 注册时携带的 payload
                // 必须原样返回，契约里这个字段是承诺字段。
                payload: claim.payload,
                found: true,
            })),
            Ok(None) => Ok(Response::new(SchedulerClaimJobResponse::default())),
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    async fn heartbeat(
        &self,
        request: Request<SchedulerHeartbeatRequest>,
    ) -> Result<Response<SchedulerHeartbeatResponse>, Status> {
        let req = request.into_inner();
        // wire 无 worker 身份（`SchedulerHeartbeatRequest` 只有 `job_id`）⇒ 以
        // `job_id` 作 claim 句柄。不得硬编码传 `"worker"`，它与认领时的随机
        // uuid 永不相等 ⇒ 续期静默失效。
        //
        // 已修的第二个静默失效：`renew_claim_any` 返回 `bool`，而 handler 把结果
        // **直接丢掉**（`let _ = ...?`）—— 于是“句柄已失效”与“续期成功”在 wire 上
        // 完全同形（都回空 OK），调用方会一直以为自己还持有任务。现改为 fail-loud：
        // 不成立即 `FAILED_PRECONDITION`（与 `MqAck` 的“非 Leader 回
        // FAILED_PRECONDITION”同口径：用错误码表达“前置条件不成立”，不新增字段）。
        let renewed = self
            .renew_claim_any(&req.job_id)
            .await
            .map_err(sanitized_internal)?;
        if !renewed {
            return Err(Status::failed_precondition(format!(
                "scheduler: claim for '{}' is no longer valid \
                 (unknown job, not claimed, or worker mismatch)",
                req.job_id
            )));
        }
        Ok(Response::new(SchedulerHeartbeatResponse {}))
    }

    async fn complete_job(
        &self,
        request: Request<SchedulerCompleteJobRequest>,
    ) -> Result<Response<SchedulerCompleteJobResponse>, Status> {
        let req = request.into_inner();
        // 同上：`job_id` 即凭据；若传 `"worker"` ⇒ CompleteJob 必报错。
        //
        // 第三个静默失效：`mark_completed_impl` 对**不存在的任务**直接 `Ok(())`
        // （它对 `release` 是合理的 no-op，但对 `complete` 不是）—— 句柄写错的
        // worker 会得到“完成成功”，从而不再重试、也不再上报失败。现按
        // “凭据必须先算数”fail-loud；已完成的任务仍幂等（多次 complete 均 OK），
        // 由`is_completed` 分支保持不变。
        if !self
            .has_live_claim(&req.job_id)
            .await
            .map_err(sanitized_internal)?
        {
            let already_done = matches!(
                self.get_task_state(&req.job_id)
                    .await
                    .map_err(sanitized_internal)?,
                Some(crate::services::scheduler::TaskState::Completed)
            );
            if !already_done {
                return Err(Status::failed_precondition(format!(
                    "scheduler: no live claim for '{}' \
                     (unknown job, already released/expired, or never claimed)",
                    req.job_id
                )));
            }
        }
        self.mark_completed_any_with_result(
            &req.job_id,
            if req.result.is_empty() {
                None
            } else {
                Some(req.result.clone())
            },
        )
        .await
        .map_err(sanitized_internal)?;
        Ok(Response::new(SchedulerCompleteJobResponse {}))
    }
}

// ════════════════════════════════════════════════════════════
// Workflow Service
// ════════════════════════════════════════════════════════════

#[tonic::async_trait]
impl Workflow for WorkflowService {
    async fn start(
        &self,
        request: Request<WorkflowStartRequest>,
    ) -> Result<Response<WorkflowStartResponse>, Status> {
        let req = request.into_inner();
        let instance_id = helper_uuid();
        // 从 definition_dsl 中提取工作流名称（支持 YAML/JSON）
        let wf_name = extract_workflow_name(&req.definition_dsl);
        let inst = WorkflowInstance::new(&instance_id, &wf_name, req.input);
        self.start_instance(inst)
            .await
            .map_err(sanitized_internal)?;
        Ok(Response::new(WorkflowStartResponse {
            workflow_id: instance_id,
        }))
    }

    async fn get_status(
        &self,
        request: Request<WorkflowGetStatusRequest>,
    ) -> Result<Response<WorkflowGetStatusResponse>, Status> {
        let req = request.into_inner();
        match self.get_instance(&req.workflow_id).await {
            Ok(Some(inst)) => Ok(Response::new(WorkflowGetStatusResponse {
                workflow_id: inst.instance_id,
                status: helper_workflow_state_str(&inst.state),
                output: inst.output,
                error_message: inst.error_message,
                definition_name: inst.workflow_name,
                input: inst.input,
                created_at: inst.created_at as i64,
                updated_at: inst.updated_at as i64,
                task_stack: vec![],
                current_state_name: String::new(),
                suspension: None,
            })),
            Ok(None) => Err(Status::not_found("workflow not found")),
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    async fn signal(
        &self,
        request: Request<WorkflowSignalRequest>,
    ) -> Result<Response<WorkflowSignalResponse>, Status> {
        let req = request.into_inner();
        self.signal_instance(&req.workflow_id, &req.signal_name, &req.payload)
            .await
            .map_err(sanitized_internal)?;
        tracing::info!(
            "Workflow signal: id={}, signal={}",
            req.workflow_id,
            req.signal_name
        );
        Ok(Response::new(WorkflowSignalResponse {}))
    }

    async fn cancel(
        &self,
        request: Request<WorkflowCancelRequest>,
    ) -> Result<Response<WorkflowCancelResponse>, Status> {
        let req = request.into_inner();
        self.transition_state(
            &req.workflow_id,
            WorkflowState::Running,
            WorkflowState::Cancelled,
        )
        .await
        .map_err(sanitized_internal)?;
        Ok(Response::new(WorkflowCancelResponse {}))
    }

    async fn deploy(
        &self,
        request: Request<WorkflowDeployRequest>,
    ) -> Result<Response<WorkflowDeployResponse>, Status> {
        let req = request.into_inner();
        match self
            .deploy_definition(&req.namespace, &req.definition_yaml)
            .await
        {
            Ok((workflow_id, version, name)) => Ok(Response::new(WorkflowDeployResponse {
                workflow_id,
                version,
                namespace: req.namespace,
                name,
            })),
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    async fn list_definitions(
        &self,
        request: Request<WorkflowListDefinitionsRequest>,
    ) -> Result<Response<WorkflowListDefinitionsResponse>, Status> {
        let req = request.into_inner();
        match self
            .list_definitions(&req.namespace, req.page_size, &req.page_token)
            .await
        {
            Ok((definitions, next_token)) => Ok(Response::new(WorkflowListDefinitionsResponse {
                definitions: definitions
                    .into_iter()
                    .map(|d| WorkflowDefinitionSummary {
                        workflow_id: d.id,
                        name: d.name,
                        version: d.version,
                        status: d.status,
                        created_at: d.created_at,
                    })
                    .collect(),
                next_page_token: next_token,
            })),
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    async fn get_definition(
        &self,
        request: Request<WorkflowGetDefinitionRequest>,
    ) -> Result<Response<WorkflowGetDefinitionResponse>, Status> {
        let req = request.into_inner();
        match self.get_definition_by_id(&req.workflow_id).await {
            Ok(def) => Ok(Response::new(WorkflowGetDefinitionResponse {
                workflow_id: def.id,
                name: def.name,
                definition_yaml: def.yaml,
                version: def.version,
                status: def.status,
                created_at: def.created_at,
            })),
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    // 遗留 WorkflowService（未注册，已被 WorkflowEngineService 取代）：
    // 版本化/回滚能力由 WorkflowEngineService 提供，此处显式返回 Unimplemented。
    async fn list_definition_versions(
        &self,
        _request: Request<WorkflowListDefinitionVersionsRequest>,
    ) -> Result<Response<WorkflowListDefinitionVersionsResponse>, Status> {
        Err(Status::unimplemented(
            "workflow definition versioning is provided by WorkflowEngineService (phase4); legacy WorkflowService is deprecated",
        ))
    }

    async fn rollback_definition(
        &self,
        _request: Request<WorkflowRollbackDefinitionRequest>,
    ) -> Result<Response<WorkflowRollbackDefinitionResponse>, Status> {
        Err(Status::unimplemented(
            "workflow definition rollback is provided by WorkflowEngineService (phase4); legacy WorkflowService is deprecated",
        ))
    }

    async fn list_instances(
        &self,
        request: Request<WorkflowListInstancesRequest>,
    ) -> Result<Response<WorkflowListInstancesResponse>, Status> {
        let req = request.into_inner();
        match self
            .list_instances(
                &req.workflow_id,
                &req.namespace,
                req.page_size,
                &req.page_token,
            )
            .await
        {
            Ok((instances, next_token)) => Ok(Response::new(WorkflowListInstancesResponse {
                instances: instances
                    .into_iter()
                    .map(|i| WorkflowInstanceSummary {
                        instance_id: i.id,
                        workflow_id: i.workflow_id,
                        state: i.state,
                        started_at: i.started_at,
                        updated_at: i.updated_at,
                        definition_name: i.definition_name,
                        namespace: String::new(),
                        output_json: vec![],
                        context_json: vec![],
                    })
                    .collect(),
                next_page_token: next_token,
            })),
            Err(e) => Err(sanitized_internal(e)),
        }
    }
}

// ════════════════════════════════════════════════════════════
// Workflow Service (Engine) — 对接 coord-core 工作流引擎
// ════════════════════════════════════════════════════════════

use crate::services::workflow::phase4::{DeployError, WorkflowEngineError, WorkflowEngineService};

// 将部署错误映射为 gRPC 状态码：输入/校验问题 → InvalidArgument，存储问题 → Internal（脱敏）
fn map_deploy_error(e: DeployError) -> Status {
    match e {
        DeployError::Validation(msg) => Status::invalid_argument(format!("deploy error: {msg}")),
        DeployError::Store(msg) => sanitized_internal(msg),
    }
}

// 引擎错误 typed 映射：
// InvalidArgument → InvalidArgument；NotFound → NotFound；FailedPrecondition → FailedPrecondition；其余 → Internal（脱敏）
fn map_engine_error(e: WorkflowEngineError) -> Status {
    match e {
        WorkflowEngineError::InvalidArgument(msg) => Status::invalid_argument(msg),
        WorkflowEngineError::NotFound(msg) => Status::not_found(msg),
        WorkflowEngineError::FailedPrecondition(msg) => Status::failed_precondition(msg),
        WorkflowEngineError::Internal(msg) => sanitized_internal(msg),
    }
}

#[tonic::async_trait]
impl Workflow for WorkflowEngineService {
    async fn start(
        &self,
        request: Request<WorkflowStartRequest>,
    ) -> Result<Response<WorkflowStartResponse>, Status> {
        let req = request.into_inner();

        // definition_id 与 definition_dsl 互斥（startByDefinition 真契约）
        if !req.definition_id.is_empty() && !req.definition_dsl.is_empty() {
            return Err(Status::invalid_argument(
                "definition_id and definition_dsl are mutually exclusive",
            ));
        }

        let input: serde_json::Value = if req.input.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&req.input).unwrap_or(serde_json::Value::Null)
        };

        let namespace = "default";
        let def_id = if !req.definition_id.is_empty() {
            // 按已部署定义启动（跳过内联 deploy）
            req.definition_id.clone()
        } else {
            self.deploy_definition(namespace, &req.definition_dsl)
                .await
                .map_err(map_deploy_error)?
        };

        let inst = self
            .start_instance(&def_id, input)
            .await
            .map_err(map_engine_error)?;

        Ok(Response::new(WorkflowStartResponse {
            workflow_id: inst.id,
        }))
    }

    async fn get_status(
        &self,
        request: Request<WorkflowGetStatusRequest>,
    ) -> Result<Response<WorkflowGetStatusResponse>, Status> {
        let req = request.into_inner();
        match self.get_instance(&req.workflow_id).await {
            Ok(Some(inst)) => {
                let status_str = match inst.status {
                    coord_core::workflow::model::InstanceStatus::Pending => "PENDING",
                    coord_core::workflow::model::InstanceStatus::Running => "RUNNING",
                    coord_core::workflow::model::InstanceStatus::Waiting => "WAITING",
                    coord_core::workflow::model::InstanceStatus::Suspended => "SUSPENDED",
                    coord_core::workflow::model::InstanceStatus::Completed => "COMPLETED",
                    coord_core::workflow::model::InstanceStatus::Failed => "FAULTED",
                    coord_core::workflow::model::InstanceStatus::Cancelled => "CANCELLED",
                };

                let output_bytes = inst
                    .output
                    .as_ref()
                    .map(|v| serde_json::to_vec(v).unwrap_or_default())
                    .unwrap_or_default();

                let input_bytes = serde_json::to_vec(&inst.context).unwrap_or_default();

                // 填充 task_stack
                let task_stack: Vec<coord_proto::agent::TaskFrame> = inst
                    .task_stack
                    .iter()
                    .map(|tf| coord_proto::agent::TaskFrame {
                        task_name: tf.task_name.clone(),
                        task_type: tf.task_type.clone(),
                        status: format!("{:?}", tf.status).to_uppercase(),
                        input: serde_json::to_vec(&tf.input).unwrap_or_default(),
                        output: serde_json::to_vec(&tf.output).unwrap_or_default(),
                        started_at: tf.started_at.unwrap_or(0),
                        ended_at: tf.ended_at.unwrap_or(0),
                        retry_count: tf.retry_count as i32,
                    })
                    .collect();

                // 当前状态名 = 当前任务帧任务名（SW 状态名 = 当前任务名，含驳回/分支后目标）
                let current_state_name = inst
                    .task_stack
                    .get(inst.current_task_index)
                    .map(|tf| tf.task_name.clone())
                    .or_else(|| inst.task_stack.last().map(|tf| tf.task_name.clone()))
                    .unwrap_or_default();

                // 挂起元信息（SUSPENDED/WAITING 时返回）
                let suspension =
                    inst.suspension_meta
                        .as_ref()
                        .map(|m| coord_proto::agent::SuspensionMeta {
                            reason: m.reason.clone(),
                            until_ms: m.until_ms.unwrap_or(0),
                            expected_signal: m.expected_signal.clone().unwrap_or_default(),
                            event_type: m
                                .event_filter
                                .as_ref()
                                .and_then(|f| f.event_type.clone())
                                .unwrap_or_default(),
                            service: m.service.clone().unwrap_or_default(),
                        });

                Ok(Response::new(WorkflowGetStatusResponse {
                    workflow_id: inst.id,
                    status: status_str.to_string(),
                    output: output_bytes,
                    error_message: inst.fault.map(|f| f.title).unwrap_or_default(),
                    definition_name: inst.definition_name,
                    input: input_bytes,
                    created_at: inst.created_at,
                    updated_at: inst.updated_at,
                    task_stack,
                    current_state_name,
                    suspension,
                }))
            }
            Ok(None) => Err(Status::not_found("workflow instance not found")),
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    async fn signal(
        &self,
        request: Request<WorkflowSignalRequest>,
    ) -> Result<Response<WorkflowSignalResponse>, Status> {
        let req = request.into_inner();
        let payload: serde_json::Value = if req.payload.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&req.payload).unwrap_or(serde_json::Value::Null)
        };

        let idempotency_key = if req.idempotency_key.is_empty() {
            None
        } else {
            Some(req.idempotency_key.as_str())
        };

        self.resume_instance(
            &req.workflow_id,
            Some(&req.signal_name),
            Some(payload),
            idempotency_key,
        )
        .await
        .map_err(map_engine_error)?;

        Ok(Response::new(WorkflowSignalResponse {}))
    }

    async fn cancel(
        &self,
        request: Request<WorkflowCancelRequest>,
    ) -> Result<Response<WorkflowCancelResponse>, Status> {
        let req = request.into_inner();
        self.cancel_instance(&req.workflow_id)
            .await
            .map_err(map_engine_error)?;
        Ok(Response::new(WorkflowCancelResponse {}))
    }

    async fn deploy(
        &self,
        request: Request<WorkflowDeployRequest>,
    ) -> Result<Response<WorkflowDeployResponse>, Status> {
        let req = request.into_inner();
        let workflow_id = self
            .deploy_definition(&req.namespace, &req.definition_yaml)
            .await
            .map_err(map_deploy_error)?;

        match self.get_definition(&workflow_id).await {
            Ok(Some(def)) => Ok(Response::new(WorkflowDeployResponse {
                workflow_id,
                version: def.document.version,
                namespace: def.document.namespace,
                name: def.document.name,
            })),
            _ => Ok(Response::new(WorkflowDeployResponse {
                workflow_id,
                version: "1.0".into(),
                namespace: req.namespace,
                name: String::new(),
            })),
        }
    }

    async fn list_definitions(
        &self,
        request: Request<WorkflowListDefinitionsRequest>,
    ) -> Result<Response<WorkflowListDefinitionsResponse>, Status> {
        let req = request.into_inner();
        let page_size = if req.page_size > 0 {
            req.page_size as usize
        } else {
            50
        };

        match self
            .list_definitions(&req.namespace, page_size, Some(&req.page_token))
            .await
        {
            Ok(defs) => {
                let summaries: Vec<WorkflowDefinitionSummary> = defs
                    .into_iter()
                    .map(|d| WorkflowDefinitionSummary {
                        workflow_id: d.id.unwrap_or_default(),
                        name: d.document.name,
                        version: d.document.version,
                        status: "active".into(),
                        created_at: 0,
                    })
                    .collect();
                Ok(Response::new(WorkflowListDefinitionsResponse {
                    definitions: summaries,
                    next_page_token: String::new(),
                }))
            }
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    async fn get_definition(
        &self,
        request: Request<WorkflowGetDefinitionRequest>,
    ) -> Result<Response<WorkflowGetDefinitionResponse>, Status> {
        let req = request.into_inner();
        match self.get_definition(&req.workflow_id).await {
            Ok(Some(def)) => Ok(Response::new(WorkflowGetDefinitionResponse {
                workflow_id: def.id.unwrap_or_default(),
                name: def.document.name,
                definition_yaml: def.raw_yaml.unwrap_or_default(),
                version: def.document.version,
                status: "active".into(),
                created_at: 0,
            })),
            Ok(None) => Err(Status::not_found("definition not found")),
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    async fn list_definition_versions(
        &self,
        request: Request<WorkflowListDefinitionVersionsRequest>,
    ) -> Result<Response<WorkflowListDefinitionVersionsResponse>, Status> {
        let req = request.into_inner();
        match self
            .list_definition_versions(&req.namespace, &req.name)
            .await
        {
            Ok(defs) => {
                let mut versions: Vec<WorkflowDefinitionVersion> = defs
                    .into_iter()
                    .map(|d| WorkflowDefinitionVersion {
                        version: d.document.version,
                        workflow_id: d.id.unwrap_or_default(),
                        status: "active".into(),
                        created_at: 0,
                    })
                    .collect();
                versions.sort_by(|a, b| a.version.cmp(&b.version));
                Ok(Response::new(WorkflowListDefinitionVersionsResponse {
                    versions,
                }))
            }
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    async fn rollback_definition(
        &self,
        request: Request<WorkflowRollbackDefinitionRequest>,
    ) -> Result<Response<WorkflowRollbackDefinitionResponse>, Status> {
        let req = request.into_inner();
        match self
            .rollback_definition(&req.namespace, &req.name, &req.version)
            .await
        {
            Ok(def) => Ok(Response::new(WorkflowRollbackDefinitionResponse {
                workflow_id: def.id.unwrap_or_default(),
                version: def.document.version,
                namespace: def.document.namespace,
                name: def.document.name,
            })),
            Err(e) => Err(map_deploy_error(e)),
        }
    }

    async fn list_instances(
        &self,
        request: Request<WorkflowListInstancesRequest>,
    ) -> Result<Response<WorkflowListInstancesResponse>, Status> {
        let req = request.into_inner();
        let page_size = if req.page_size > 0 {
            req.page_size as usize
        } else {
            50
        };

        match self
            .list_instances(Some(&req.namespace), None, page_size, Some(&req.page_token))
            .await
        {
            Ok(instances) => {
                let summaries: Vec<WorkflowInstanceSummary> = instances
                    .into_iter()
                    .map(|i| {
                        let state_str = match i.status {
                            coord_core::workflow::model::InstanceStatus::Running => "RUNNING",
                            coord_core::workflow::model::InstanceStatus::Suspended => "SUSPENDED",
                            coord_core::workflow::model::InstanceStatus::Completed => "COMPLETED",
                            coord_core::workflow::model::InstanceStatus::Failed => "FAILED",
                            coord_core::workflow::model::InstanceStatus::Cancelled => "CANCELLED",
                            _ => "UNKNOWN",
                        };
                        WorkflowInstanceSummary {
                            instance_id: i.id,
                            workflow_id: i.definition_name.clone(),
                            state: state_str.to_string(),
                            started_at: i.created_at,
                            updated_at: i.updated_at,
                            definition_name: i.definition_name,
                            namespace: i.definition_ns,
                            output_json: i
                                .output
                                .map(|v| serde_json::to_vec(&v).unwrap_or_default())
                                .unwrap_or_default(),
                            context_json: serde_json::to_vec(&i.context).unwrap_or_default(),
                        }
                    })
                    .collect();
                Ok(Response::new(WorkflowListInstancesResponse {
                    instances: summaries,
                    next_page_token: String::new(),
                }))
            }
            Err(e) => Err(sanitized_internal(e)),
        }
    }
}

// ════════════════════════════════════════════════════════════
// Policy Service
// ════════════════════════════════════════════════════════════

#[tonic::async_trait]
impl Policy for PolicyService {
    async fn check_permission(
        &self,
        request: Request<PolicyCheckPermissionRequest>,
    ) -> Result<Response<PolicyCheckPermissionResponse>, Status> {
        let req = request.into_inner();
        let mut context = std::collections::HashMap::new();
        if !req.context.is_empty() {
            if let Ok(map) =
                serde_json::from_slice::<std::collections::HashMap<String, String>>(&req.context)
            {
                context = map;
            }
        }
        let access_req = AccessRequest {
            subject: req.principal,
            action: req.action,
            resource: req.resource,
            context,
        };
        match self.evaluate(&access_req) {
            Ok(decision) => {
                let allowed = matches!(
                    decision.effect,
                    crate::services::policy::PolicyEffect::Allow
                );
                Ok(Response::new(PolicyCheckPermissionResponse {
                    allowed,
                    reason: decision.reason,
                }))
            }
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    async fn evaluate(
        &self,
        request: Request<PolicyEvaluateRequest>,
    ) -> Result<Response<PolicyEvaluateResponse>, Status> {
        let req = request.into_inner();
        if req.query.is_empty() {
            return Err(Status::invalid_argument("query must not be empty"));
        }
        let input_json = String::from_utf8_lossy(&req.input).to_string();
        let opa = self.opa_engine().clone();

        // 同步 Rego 求值放到阻塞线程池，避免阻塞 agent 异步执行器。
        // error/deny 区分：求值错误（语法/输入）→ gRPC InvalidArgument；
        // deny/无匹配 → 成功响应且 result 为 false / null。
        let value = tokio::task::spawn_blocking(move || opa.eval_query(&req.query, &input_json))
            .await
            .map_err(sanitized_internal)?
            .map_err(Status::invalid_argument)?;

        let result = serde_json::to_vec(&value).map_err(sanitized_internal)?;
        Ok(Response::new(PolicyEvaluateResponse { result }))
    }

    async fn explain(
        &self,
        request: Request<PolicyExplainRequest>,
    ) -> Result<Response<PolicyExplainResponse>, Status> {
        let req = request.into_inner();
        let input_json = String::from_utf8_lossy(&req.input).to_string();
        let opa = self.opa_engine().clone();

        // 同步 Rego explain（trace 求值）放到阻塞线程池，避免阻塞 agent 异步执行器。
        // 错误语义与原 `PolicyService::explain` 一致：统一 sanitized internal。
        let trace = tokio::task::spawn_blocking(move || opa.explain(&req.query, &input_json))
            .await
            .map_err(sanitized_internal)?
            .map_err(sanitized_internal)?;

        Ok(Response::new(PolicyExplainResponse {
            trace: trace.into_bytes(),
        }))
    }

    async fn put_bundle(
        &self,
        request: Request<PolicyPutBundleRequest>,
    ) -> Result<Response<PolicyPutBundleResponse>, Status> {
        let req = request.into_inner();
        match self
            .put_bundle(&req.tenant_id, &req.namespace, &req.name, &req.rego_content)
            .await
        {
            Ok(info) => Ok(Response::new(PolicyPutBundleResponse {
                bundle_id: info.bundle_id,
                name: info.name,
                namespace: info.namespace,
                created_at: info.created_at,
                updated_at: info.updated_at,
                enabled: info.enabled,
                version: info.version,
            })),
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    async fn delete_bundle(
        &self,
        request: Request<PolicyDeleteBundleRequest>,
    ) -> Result<Response<PolicyDeleteBundleResponse>, Status> {
        let req = request.into_inner();
        match self.delete_bundle(&req.bundle_id).await {
            Ok(deleted) => Ok(Response::new(PolicyDeleteBundleResponse { deleted })),
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    async fn list_bundles(
        &self,
        request: Request<PolicyListBundlesRequest>,
    ) -> Result<Response<PolicyListBundlesResponse>, Status> {
        let req = request.into_inner();
        let tenant_id = if req.tenant_id.is_empty() {
            None
        } else {
            Some(req.tenant_id.as_str())
        };
        match self.list_bundles(tenant_id).await {
            Ok(bundles) => {
                let proto_bundles: Vec<PolicyBundleInfo> = bundles
                    .into_iter()
                    .map(|b| PolicyBundleInfo {
                        bundle_id: b.bundle_id,
                        name: b.name,
                        namespace: b.namespace,
                        tenant_id: b.tenant_id,
                        enabled: b.enabled,
                        created_at: b.created_at,
                        updated_at: b.updated_at,
                        version: b.version,
                    })
                    .collect();
                Ok(Response::new(PolicyListBundlesResponse {
                    bundles: proto_bundles,
                }))
            }
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    async fn set_bundle_enabled(
        &self,
        request: Request<PolicySetBundleEnabledRequest>,
    ) -> Result<Response<PolicySetBundleEnabledResponse>, Status> {
        let req = request.into_inner();
        match self.set_bundle_enabled(&req.bundle_id, req.enabled).await {
            Ok(success) => Ok(Response::new(PolicySetBundleEnabledResponse { success })),
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    async fn rollback_bundle(
        &self,
        request: Request<PolicyRollbackBundleRequest>,
    ) -> Result<Response<PolicyRollbackBundleResponse>, Status> {
        let req = request.into_inner();
        match self.rollback_bundle(&req.bundle_id, req.version).await {
            Ok(info) => Ok(Response::new(PolicyRollbackBundleResponse {
                success: true,
                version: info.version,
                restored_version: req.version,
                bundle: Some(PolicyBundleInfo {
                    bundle_id: info.bundle_id,
                    name: info.name,
                    namespace: info.namespace,
                    tenant_id: info.tenant_id,
                    enabled: info.enabled,
                    created_at: info.created_at,
                    updated_at: info.updated_at,
                    version: info.version,
                }),
            })),
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    async fn list_bundle_versions(
        &self,
        request: Request<PolicyListBundleVersionsRequest>,
    ) -> Result<Response<PolicyListBundleVersionsResponse>, Status> {
        let req = request.into_inner();
        match self.list_bundle_versions(&req.bundle_id).await {
            Ok(versions) => {
                let proto_versions: Vec<PolicyBundleVersionInfo> = versions
                    .into_iter()
                    .map(|v| PolicyBundleVersionInfo {
                        version: v.version,
                        created_at: v.created_at,
                        is_current: v.is_current,
                    })
                    .collect();
                Ok(Response::new(PolicyListBundleVersionsResponse {
                    versions: proto_versions,
                }))
            }
            Err(e) => Err(sanitized_internal(e)),
        }
    }
}

// ════════════════════════════════════════════════════════════
// Transit Service
// ════════════════════════════════════════════════════════════

#[tonic::async_trait]
impl Transit for TransitService {
    async fn encrypt(
        &self,
        request: Request<TransitEncryptRequest>,
    ) -> Result<Response<TransitEncryptResponse>, Status> {
        let req = request.into_inner();
        // 持久化路径：DEK 落 coord-server KV ⇒ 重启后仍可解密；
        // KV 写失败即报错（fail-closed），不回退到"只在本进程有效"的密文。
        match self.encrypt_persisted(&req.plaintext).await {
            Ok((ciphertext, _dek_id)) => Ok(Response::new(TransitEncryptResponse { ciphertext })),
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    async fn decrypt(
        &self,
        request: Request<TransitDecryptRequest>,
    ) -> Result<Response<TransitDecryptResponse>, Status> {
        let req = request.into_inner();
        // DEK ID 现在嵌入在 ciphertext 包头中（自描述格式），不再需要外部传入；
        // 内存未命中时从共享 KV 回取（重启恢复），消费后删除 KV 记录（用后即焚）。
        match self.decrypt_persisted(&req.ciphertext, "").await {
            Ok(plaintext) => Ok(Response::new(TransitDecryptResponse { plaintext })),
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    async fn hmac_sign(
        &self,
        request: Request<TransitHmacSignRequest>,
    ) -> Result<Response<TransitHmacSignResponse>, Status> {
        let req = request.into_inner();
        match self.hmac_sign(&req.data, &req.algorithm) {
            Ok(signature) => Ok(Response::new(TransitHmacSignResponse {
                signature,
                algorithm: req.algorithm,
            })),
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    async fn hmac_verify(
        &self,
        request: Request<TransitHmacVerifyRequest>,
    ) -> Result<Response<TransitHmacVerifyResponse>, Status> {
        let req = request.into_inner();
        match self.hmac_verify(&req.data, &req.signature, &req.algorithm) {
            Ok(valid) => Ok(Response::new(TransitHmacVerifyResponse { valid })),
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    /// KEK 材料迁移（管理路径，G-TR-1）：旧材料解出 DEK → 主材料重包。
    /// 旧材料密文经此迁移后，旧材料方可从注入集合下线。
    async fn rewrap(
        &self,
        request: Request<TransitRewrapRequest>,
    ) -> Result<Response<TransitRewrapResponse>, Status> {
        let req = request.into_inner();
        if req.dek_id.is_empty() {
            return Err(Status::invalid_argument("dek_id must not be empty"));
        }
        match self.rewrap_persisted(&req.dek_id).await {
            Ok(new_dek_id) => Ok(Response::new(TransitRewrapResponse {
                new_dek_id,
                kek_id: self.primary_kek_id().to_string(),
            })),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("not found") || msg.contains("expired") {
                    // 不存在 / 已过期：明确 NOT_FOUND（过期拒绝轮换）
                    Err(Status::not_found(msg))
                } else if msg.contains("is not injected") {
                    // 包裹材料未注入（旧材料已下线但仍有存量）：前置条件不满足
                    Err(Status::failed_precondition(msg))
                } else {
                    Err(sanitized_internal(msg))
                }
            }
        }
    }
}

// ════════════════════════════════════════════════════════════
// CircuitBreaker Service
// ════════════════════════════════════════════════════════════

#[tonic::async_trait]
impl CircuitBreaker for CircuitBreakerService {
    async fn get_state(
        &self,
        _request: Request<CircuitBreakerGetStateRequest>,
    ) -> Result<Response<CircuitBreakerGetStateResponse>, Status> {
        let state = self.state();
        Ok(Response::new(CircuitBreakerGetStateResponse {
            state: format!("{:?}", state),
            last_failure_time: 0,
        }))
    }

    async fn report_success(
        &self,
        _request: Request<CircuitBreakerReportSuccessRequest>,
    ) -> Result<Response<CircuitBreakerReportSuccessResponse>, Status> {
        self.record_success();
        Ok(Response::new(CircuitBreakerReportSuccessResponse {}))
    }

    async fn report_failure(
        &self,
        _request: Request<CircuitBreakerReportFailureRequest>,
    ) -> Result<Response<CircuitBreakerReportFailureResponse>, Status> {
        self.record_failure();
        Ok(Response::new(CircuitBreakerReportFailureResponse {}))
    }

    async fn reset(
        &self,
        _request: Request<CircuitBreakerResetRequest>,
    ) -> Result<Response<CircuitBreakerResetResponse>, Status> {
        self.reset();
        Ok(Response::new(CircuitBreakerResetResponse {}))
    }
}

// ════════════════════════════════════════════════════════════
// RateLimiter Service
// ════════════════════════════════════════════════════════════

#[tonic::async_trait]
impl RateLimiter for RateLimiterService {
    async fn allow(
        &self,
        request: Request<RateLimiterAllowRequest>,
    ) -> Result<Response<RateLimiterAllowResponse>, Status> {
        let req = request.into_inner();
        for _ in 0..req.permits.max(1) {
            if self.try_acquire().is_err() {
                return Ok(Response::new(RateLimiterAllowResponse {
                    allowed: false,
                    remaining: 0,
                    reset_time: 0,
                }));
            }
        }
        let remaining = self.available_tokens() as i64;
        Ok(Response::new(RateLimiterAllowResponse {
            allowed: true,
            remaining,
            reset_time: 0,
        }))
    }
}

// ════════════════════════════════════════════════════════════
// FeatureFlags Service
// ════════════════════════════════════════════════════════════

#[tonic::async_trait]
impl FeatureFlags for FeatureFlagService {
    async fn is_enabled(
        &self,
        request: Request<FeatureFlagIsEnabledRequest>,
    ) -> Result<Response<FeatureFlagIsEnabledResponse>, Status> {
        let req = request.into_inner();
        match self.is_enabled(&req.flag_name).await {
            Ok(enabled) => Ok(Response::new(FeatureFlagIsEnabledResponse {
                enabled,
                variant: String::new(),
            })),
            Err(e) => Err(sanitized_internal(e)),
        }
    }

    async fn evaluate(
        &self,
        request: Request<FeatureFlagEvaluateRequest>,
    ) -> Result<Response<FeatureFlagEvaluateResponse>, Status> {
        let req = request.into_inner();
        let ctx = FlagEvalContext::default();
        match FeatureFlagService::evaluate(self, &req.flag_name, &ctx).await {
            Ok(result) => {
                let json = serde_json::to_vec(&result).unwrap_or_default();
                Ok(Response::new(FeatureFlagEvaluateResponse { result: json }))
            }
            Err(e) => Err(sanitized_internal(e)),
        }
    }
}

// ════════════════════════════════════════════════════════════
// 错误脱敏测试（与 coord-server 同口径：详情只进日志）
// ════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    /// internal 错误脱敏：客户端仅收到通用 "internal error"，不含内部细节
    #[test]
    fn test_sanitized_internal_hides_details() {
        let status =
            sanitized_internal("sensitive detail: /var/lib/coord-agent/store.db table corrupted");
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), "internal error");
        assert!(!status.message().contains("store.db"));
    }

    /// 数据面错误映射：not leader 保留为显式契约（含 NOT_LEADER 错误码），
    /// ISR 降级 → UNAVAILABLE，not found → NOT_FOUND，其余脱敏
    #[test]
    fn test_map_service_error_preserves_not_leader() {
        let not_leader = map_service_error("not leader for shard 'mq:t' (leader is other-agent)");
        assert_eq!(not_leader.code(), tonic::Code::FailedPrecondition);
        assert!(not_leader.message().contains("not leader"));
        assert_eq!(
            coord_core::error_code::error_code_of(&not_leader).as_deref(),
            Some("NOT_LEADER"),
            "结构化错误码必须是 NOT_LEADER（SDK 按码决策重定向）"
        );

        // ISR 降级 = 可用性条件（可退避重试）⇒ UNAVAILABLE
        let degraded = map_service_error("ISR degraded: need 2 replicas, have 1");
        assert_eq!(degraded.code(), tonic::Code::Unavailable);
        assert_eq!(
            coord_core::error_code::error_code_of(&degraded).as_deref(),
            Some("UNAVAILABLE")
        );

        // 资源不存在 ⇒ NOT_FOUND（删除后的 topic 读写、move_to_dlq 等）
        let missing = map_service_error("topic 'orders' not found");
        assert_eq!(missing.code(), tonic::Code::NotFound);

        // 容量上界（B-PL-3 / B-PL-4）：可诊断的语义错误 ⇒ RESOURCE_EXHAUSTED
        let quota = map_service_error(
            "mq publish rejected: ... max_size_bytes=80 — quota is enforced at publish \
             (see boundaries.md B-PL-4)",
        );
        assert_eq!(quota.code(), tonic::Code::ResourceExhausted);
        assert!(quota.message().contains("max_size_bytes"));

        let other = map_service_error("sensitive store detail");
        assert_eq!(other.code(), tonic::Code::Internal);
        assert_eq!(other.message(), "internal error");
    }

    /// 非 Leader 统一错误：FAILED_PRECONDITION + NOT_LEADER + leader 提示 trailer
    /// （可编程路由，不解析文案；G-MQ-1）
    #[test]
    fn test_mq_not_leader_status_carries_structured_hint() {
        let status = mq_not_leader_status("mq:orders", "127.0.0.1:19201");
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            coord_core::error_code::error_code_of(&status).as_deref(),
            Some("NOT_LEADER")
        );
        let hint = status
            .metadata()
            .get(MQ_LEADER_HINT_TRAILER)
            .and_then(|v| v.to_str().ok());
        assert_eq!(hint, Some("127.0.0.1:19201"));
    }

    /// 部署错误映射：校验错误保留细节（InvalidArgument），存储错误脱敏
    #[test]
    fn test_map_deploy_error_sanitizes_store() {
        let status = map_deploy_error(DeployError::Store("sensitive store detail".into()));
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), "internal error");
        assert!(!status.message().contains("sensitive"));

        let validation = map_deploy_error(DeployError::Validation("bad yaml".into()));
        assert_eq!(validation.code(), tonic::Code::InvalidArgument);
        assert!(validation.message().contains("bad yaml"));
    }

    /// 引擎错误映射：Internal 脱敏，InvalidArgument/NotFound 保留细节
    #[test]
    fn test_map_engine_error_sanitizes_internal() {
        let status = map_engine_error(WorkflowEngineError::Internal(
            "sensitive engine detail".into(),
        ));
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), "internal error");
        assert!(!status.message().contains("sensitive"));

        let invalid = map_engine_error(WorkflowEngineError::InvalidArgument("bad input".into()));
        assert_eq!(invalid.code(), tonic::Code::InvalidArgument);
        assert!(invalid.message().contains("bad input"));
    }
}

// ──── Private helpers ────

fn helper_uuid() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!(
        "{:016x}-{:04x}-4{:03x}-{:04x}-{:012x}",
        ts & 0xFFFFFFFFFFFFFFFF,
        ((ts >> 64) as u16),
        (ts >> 80) as u16 & 0xFFF,
        0x8000 | ((ts >> 96) as u16 & 0x3FFF),
        ts & 0xFFFFFFFFFFFF,
    )
}

fn _helper_unix_ts() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn helper_workflow_state_str(state: &WorkflowState) -> String {
    match state {
        WorkflowState::Pending => "PENDING",
        WorkflowState::Running => "RUNNING",
        WorkflowState::Completed => "COMPLETED",
        WorkflowState::Failed => "FAILED",
        WorkflowState::Cancelled => "CANCELLED",
        _ => "UNKNOWN",
    }
    .to_string()
}

/// 从 YAML/JSON 定义中提取工作流名称
fn extract_workflow_name(definition_dsl: &str) -> String {
    // 尝试 JSON 解析（CNCF Serverless Workflow 使用 "id" 字段）
    if let Ok(dsl) = serde_json::from_str::<serde_json::Value>(definition_dsl) {
        if let Some(name) = dsl.get("name").and_then(|v| v.as_str()) {
            return name.to_string();
        }
        // CNCF Serverless Workflow DSL 使用 "id" 作为工作流标识符
        if let Some(id) = dsl.get("id").and_then(|v| v.as_str()) {
            return id.to_string();
        }
    }
    // 回退到 YAML 行解析：查找 "name:" 或 "id:" 行（支持缩进、引号）
    definition_dsl
        .lines()
        .find(|l| {
            let trimmed = l.trim_start();
            trimmed.starts_with("name:") || trimmed.starts_with("id:")
        })
        .and_then(|l| l.split_once(':').map(|x| x.1))
        .map(|s| s.trim().trim_matches('"').trim_matches('\'').to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "workflow".to_string())
}
