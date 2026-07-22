// coord-agent: gRPC trait implementations for pluggable services
//
// This module implements gRPC service traits (from agent_api.proto) for
// each pluggable service. It bridges between proto request/response types
// and the service's internal public API.
//
// Registry and Config gRPC handlers remain inline in their respective
// service files (registry.rs, config_center.rs).

use std::collections::BTreeMap;
use tokio_stream::wrappers::ReceiverStream;

use crate::services::{
    lock::{LockService, LockInfo},
    idgen::IdGenService,
    leader_election::{LeaderElectionService, LeaderRole, ElectionGroup},
    event_notification::{EventNotificationService, Event, CloudEvent},
    cache::CacheService,
    mq::{MessageQueueService, TopicConfig},
    scheduler::SchedulerService,
    workflow::{WorkflowService, WorkflowInstance, WorkflowState},
    policy::{PolicyService, AccessRequest},
    transit::TransitService,
    circuit_breaker::CircuitBreakerService,
    rate_limiter::RateLimiterService,
};
use crate::feature_flags::{FeatureFlagService, FlagEvalContext};

use coord_proto::agent::{
    lock_server::Lock,
    LockAcquireRequest, LockAcquireResponse,
    LockReleaseRequest, LockReleaseResponse,
    LockRenewRequest, LockRenewResponse,
    LockGetInfoRequest, LockGetInfoResponse,
    id_gen_server::IdGen,
    IdGenNextIdRequest, IdGenNextIdResponse,
    IdGenNextBatchRequest, IdGenNextBatchResponse,
    leader_election_server::LeaderElection,
    LeaderCampaignRequest, LeaderCampaignResponse,
    LeaderResignRequest, LeaderResignResponse,
    LeaderGetLeaderRequest, LeaderGetLeaderResponse,
    LeaderWatchRequest, LeaderWatchEvent,
    event_server::Event as EventSvc,
    EventPublishRequest, EventPublishResponse,
    EventSubscribeRequest, CloudEventMessage,
    EventUnsubscribeRequest, EventUnsubscribeResponse,
    cache_server::Cache,
    CacheGetRequest, CacheGetResponse,
    CacheSetRequest, CacheSetResponse,
    CacheDeleteRequest, CacheDeleteResponse,
    CacheHGetRequest, CacheHGetResponse,
    CacheHSetRequest, CacheHSetResponse,
    CacheHGetAllRequest, CacheHGetAllResponse,
    CacheLPushRequest, CacheLPushResponse,
    CacheLRangeRequest, CacheLRangeResponse,
    CacheSAddRequest, CacheSAddResponse,
    CacheSMembersRequest, CacheSMembersResponse,
    mq_server::Mq,
    MqCreateTopicRequest, MqCreateTopicResponse,
    MqPublishRequest, MqPublishResponse,
    MqSubscribeRequest, MqMessage,
    MqAckRequest, MqAckResponse,
    scheduler_server::Scheduler,
    SchedulerRegisterJobRequest, SchedulerRegisterJobResponse,
    SchedulerClaimJobRequest, SchedulerClaimJobResponse,
    SchedulerHeartbeatRequest, SchedulerHeartbeatResponse,
    SchedulerCompleteJobRequest, SchedulerCompleteJobResponse,
    workflow_server::Workflow,
    WorkflowStartRequest, WorkflowStartResponse,
    WorkflowGetStatusRequest, WorkflowGetStatusResponse,
    WorkflowSignalRequest, WorkflowSignalResponse,
    WorkflowCancelRequest, WorkflowCancelResponse,
    WorkflowDeployRequest, WorkflowDeployResponse,
    WorkflowListDefinitionsRequest, WorkflowListDefinitionsResponse,
    WorkflowDefinitionSummary,
    WorkflowGetDefinitionRequest, WorkflowGetDefinitionResponse,
    WorkflowListInstancesRequest, WorkflowListInstancesResponse,
    WorkflowInstanceSummary,
    policy_server::Policy,
    PolicyCheckPermissionRequest, PolicyCheckPermissionResponse,
    PolicyEvaluateRequest, PolicyEvaluateResponse,
    PolicyExplainRequest, PolicyExplainResponse,
    PolicyPutBundleRequest, PolicyPutBundleResponse,
    PolicyDeleteBundleRequest, PolicyDeleteBundleResponse,
    PolicyListBundlesRequest, PolicyListBundlesResponse,
    PolicySetBundleEnabledRequest, PolicySetBundleEnabledResponse,
    PolicyBundleInfo,
    transit_server::Transit,
    TransitEncryptRequest, TransitEncryptResponse,
    TransitDecryptRequest, TransitDecryptResponse,
    TransitHmacSignRequest, TransitHmacSignResponse,
    TransitHmacVerifyRequest, TransitHmacVerifyResponse,
    circuit_breaker_server::CircuitBreaker,
    CircuitBreakerGetStateRequest, CircuitBreakerGetStateResponse,
    CircuitBreakerReportSuccessRequest, CircuitBreakerReportSuccessResponse,
    CircuitBreakerReportFailureRequest, CircuitBreakerReportFailureResponse,
    CircuitBreakerResetRequest, CircuitBreakerResetResponse,
    rate_limiter_server::RateLimiter,
    RateLimiterAllowRequest, RateLimiterAllowResponse,
    feature_flags_server::FeatureFlags,
    FeatureFlagIsEnabledRequest, FeatureFlagIsEnabledResponse,
    FeatureFlagEvaluateRequest, FeatureFlagEvaluateResponse,
};

use tonic::{Request, Response, Status};

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
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn release(
        &self,
        request: Request<LockReleaseRequest>,
    ) -> Result<Response<LockReleaseResponse>, Status> {
        let req = request.into_inner();
        match LockService::release(self, &req.name, &req.holder_id).await {
            Ok(released) => Ok(Response::new(LockReleaseResponse { released })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn renew(
        &self,
        request: Request<LockRenewRequest>,
    ) -> Result<Response<LockRenewResponse>, Status> {
        let req = request.into_inner();
        match LockService::renew(self, &req.name, &req.holder_id).await {
            Ok(true) => Ok(Response::new(LockRenewResponse { new_ttl: 0 })),
            Ok(false) => Err(Status::not_found("lock not held or expired")),
            Err(e) => Err(Status::internal(e.to_string())),
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
            Err(e) => Err(Status::internal(e.to_string())),
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
            Err(e) => Err(Status::internal(e.to_string())),
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
            Err(e) => Err(Status::internal(e.to_string())),
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
        match LeaderElectionService::campaign(self, &req.group_name, &req.candidate_id, req.ttl_seconds as u64).await {
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
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn resign(
        &self,
        request: Request<LeaderResignRequest>,
    ) -> Result<Response<LeaderResignResponse>, Status> {
        let req = request.into_inner();
        match LeaderElectionService::resign(self, &req.group_name, &req.candidate_id).await {
            Ok(()) => Ok(Response::new(LeaderResignResponse { resigned: true })),
            Err(e) => Err(Status::internal(e.to_string())),
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
                            if tx.send(Ok(LeaderWatchEvent {
                                r#type: event_type,
                                group_name: group,
                                leader_id: String::new(),
                            })).await.is_err() {
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
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    type SubscribeStream = ReceiverStream<Result<CloudEventMessage, Status>>;

    async fn subscribe(
        &self,
        request: Request<EventSubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let req = request.into_inner();
        let filter_type = req.event_type.clone();
        let mut rx = EventNotificationService::subscribe(self);
        let (tx, out_rx) = tokio::sync::mpsc::channel(64);

        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        if !filter_type.is_empty() && event.event_type != filter_type {
                            continue;
                        }
                        let ce = CloudEvent::from_event(&event);
                        let msg = CloudEventMessage {
                            id: ce.id,
                            specversion: ce.specversion,
                            r#type: ce.event_type,
                            source: ce.source,
                            data: ce.data.unwrap_or_default(),
                            data_content_type: ce.datacontenttype.unwrap_or_default(),
                            subject: ce.subject.unwrap_or_default(),
                            time: ce.time.unwrap_or_default(),
                        };
                        if tx.send(Ok(msg)).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
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
    async fn get(&self, request: Request<CacheGetRequest>) -> Result<Response<CacheGetResponse>, Status> {
        let req = request.into_inner();
        match self.string_get(&req.key) {
            Ok(Some(value)) => Ok(Response::new(CacheGetResponse { value, found: true })),
            Ok(None) => Ok(Response::new(CacheGetResponse { value: vec![], found: false })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn set(&self, request: Request<CacheSetRequest>) -> Result<Response<CacheSetResponse>, Status> {
        let req = request.into_inner();
        let ttl = if req.ttl_seconds > 0 { Some(req.ttl_seconds as u64) } else { None };
        self.string_put(&req.key, req.value, ttl)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(CacheSetResponse {}))
    }

    async fn delete(&self, request: Request<CacheDeleteRequest>) -> Result<Response<CacheDeleteResponse>, Status> {
        let req = request.into_inner();
        match self.string_delete(&req.key) {
            Ok(deleted) => Ok(Response::new(CacheDeleteResponse { deleted })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn h_get(&self, request: Request<CacheHGetRequest>) -> Result<Response<CacheHGetResponse>, Status> {
        let req = request.into_inner();
        match self.hash_field_get(&req.key, &req.field) {
            Ok(Some(value)) => Ok(Response::new(CacheHGetResponse { value, found: true })),
            Ok(None) => Ok(Response::new(CacheHGetResponse { value: vec![], found: false })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn h_set(&self, request: Request<CacheHSetRequest>) -> Result<Response<CacheHSetResponse>, Status> {
        let req = request.into_inner();
        self.hash_field_put(&req.key, &req.field, req.value, None)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(CacheHSetResponse {}))
    }

    async fn h_get_all(&self, request: Request<CacheHGetAllRequest>) -> Result<Response<CacheHGetAllResponse>, Status> {
        let req = request.into_inner();
        match self.hash_get_all(&req.key) {
            Ok(fields) => {
                let map: std::collections::HashMap<String, Vec<u8>> = fields.into_iter().collect();
                Ok(Response::new(CacheHGetAllResponse { fields: map }))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn l_push(&self, request: Request<CacheLPushRequest>) -> Result<Response<CacheLPushResponse>, Status> {
        let req = request.into_inner();
        self.list_push_left(&req.key, req.value, None)
            .map_err(|e| Status::internal(e.to_string()))?;
        match self.list_length(&req.key) {
            Ok(len) => Ok(Response::new(CacheLPushResponse { length: len as i64 })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn l_range(&self, request: Request<CacheLRangeRequest>) -> Result<Response<CacheLRangeResponse>, Status> {
        let req = request.into_inner();
        match self.list_range(&req.key, req.start, req.stop) {
            Ok(values) => Ok(Response::new(CacheLRangeResponse { values })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn s_add(&self, request: Request<CacheSAddRequest>) -> Result<Response<CacheSAddResponse>, Status> {
        let req = request.into_inner();
        self.set_add(&req.key, req.member, None)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(CacheSAddResponse {}))
    }

    async fn s_members(&self, request: Request<CacheSMembersRequest>) -> Result<Response<CacheSMembersResponse>, Status> {
        let req = request.into_inner();
        match self.set_members(&req.key) {
            Ok(members) => Ok(Response::new(CacheSMembersResponse { members })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }
}

// ════════════════════════════════════════════════════════════
// MQ Service
// ════════════════════════════════════════════════════════════

#[tonic::async_trait]
impl Mq for MessageQueueService {
    async fn create_topic(&self, request: Request<MqCreateTopicRequest>) -> Result<Response<MqCreateTopicResponse>, Status> {
        let req = request.into_inner();
        let config = TopicConfig {
            partitions: req.partitions as u32,
            retention_secs: 86400,
            max_message_size: 1024 * 1024,
        };
        self.create_topic(&req.topic, config)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(MqCreateTopicResponse {}))
    }

    async fn publish(&self, request: Request<MqPublishRequest>) -> Result<Response<MqPublishResponse>, Status> {
        let req = request.into_inner();
        let partition = if req.partition >= 0 { req.partition as u32 } else { 0 };
        match self.produce(&req.topic, partition, req.payload, None) {
            Ok(offset) => Ok(Response::new(MqPublishResponse { offset: offset as i64 })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    type SubscribeStream = ReceiverStream<Result<MqMessage, Status>>;

    async fn subscribe(&self, _request: Request<MqSubscribeRequest>) -> Result<Response<Self::SubscribeStream>, Status> {
        let (tx, out_rx) = tokio::sync::mpsc::channel(64);
        // MQ subscribe requires long-lived state (consumer offsets + polling).
        // For initial implementation, return a placeholder stream.
        tokio::spawn(async move {
            let _ = tx; // keep channel alive until client disconnects
            tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
        });
        Ok(Response::new(ReceiverStream::new(out_rx)))
    }

    async fn ack(&self, request: Request<MqAckRequest>) -> Result<Response<MqAckResponse>, Status> {
        let req = request.into_inner();
        let partition = if req.partition >= 0 { req.partition as u32 } else { 0 };
        self.commit_offset(&req.consumer_group, &req.topic, partition, req.offset as u64)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(MqAckResponse {}))
    }
}

// ════════════════════════════════════════════════════════════
// Scheduler Service
// ════════════════════════════════════════════════════════════

#[tonic::async_trait]
impl Scheduler for SchedulerService {
    async fn register_job(&self, request: Request<SchedulerRegisterJobRequest>) -> Result<Response<SchedulerRegisterJobResponse>, Status> {
        let req = request.into_inner();
        let task = crate::services::scheduler::ScheduleTask {
            task_id: helper_uuid(),
            task_type: crate::services::scheduler::TaskType::Cron { expression: req.cron_expression },
            description: req.name.clone(),
            metadata: [
                ("payload".to_string(), String::from_utf8_lossy(&req.payload).to_string()),
            ].into_iter().collect(),
        };
        self.register_task(task)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(SchedulerRegisterJobResponse { job_id: req.name }))
    }

    async fn claim_job(&self, request: Request<SchedulerClaimJobRequest>) -> Result<Response<SchedulerClaimJobResponse>, Status> {
        let req = request.into_inner();
        let worker_id = helper_uuid();
        match self.try_claim(&req.name, &worker_id) {
            Ok(Some(claim)) => Ok(Response::new(SchedulerClaimJobResponse {
                job_id: claim.task_id,
                payload: vec![],
                found: true,
            })),
            Ok(None) => Ok(Response::new(SchedulerClaimJobResponse::default())),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn heartbeat(&self, request: Request<SchedulerHeartbeatRequest>) -> Result<Response<SchedulerHeartbeatResponse>, Status> {
        let req = request.into_inner();
        self.renew_claim(&req.job_id, "worker")
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(SchedulerHeartbeatResponse {}))
    }

    async fn complete_job(&self, request: Request<SchedulerCompleteJobRequest>) -> Result<Response<SchedulerCompleteJobResponse>, Status> {
        let req = request.into_inner();
        self.mark_completed(&req.job_id, "worker")
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(SchedulerCompleteJobResponse {}))
    }
}

// ════════════════════════════════════════════════════════════
// Workflow Service
// ════════════════════════════════════════════════════════════

#[tonic::async_trait]
impl Workflow for WorkflowService {
    async fn start(&self, request: Request<WorkflowStartRequest>) -> Result<Response<WorkflowStartResponse>, Status> {
        let req = request.into_inner();
        let instance_id = helper_uuid();
        // 从 definition_dsl 中提取工作流名称（支持 YAML/JSON）
        let wf_name = extract_workflow_name(&req.definition_dsl);
        let inst = WorkflowInstance::new(&instance_id, &wf_name, req.input);
        self.start_instance(inst).await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(WorkflowStartResponse { workflow_id: instance_id }))
    }

    async fn get_status(&self, request: Request<WorkflowGetStatusRequest>) -> Result<Response<WorkflowGetStatusResponse>, Status> {
        let req = request.into_inner();
        match self.get_instance(&req.workflow_id).await {
            Ok(Some(inst)) => Ok(Response::new(WorkflowGetStatusResponse {
                workflow_id: inst.instance_id,
                status: helper_workflow_state_str(&inst.state),
                output: inst.output,
                error_message: inst.error_message,
                definition_name: inst.workflow_name,
                input: inst.input,
            })),
            Ok(None) => Err(Status::not_found("workflow not found")),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn signal(&self, request: Request<WorkflowSignalRequest>) -> Result<Response<WorkflowSignalResponse>, Status> {
        let req = request.into_inner();
        self.signal_instance(&req.workflow_id, &req.signal_name, &req.payload).await
            .map_err(|e| Status::internal(e.to_string()))?;
        tracing::info!(
            "Workflow signal: id={}, signal={}",
            req.workflow_id, req.signal_name
        );
        Ok(Response::new(WorkflowSignalResponse {}))
    }

    async fn cancel(&self, request: Request<WorkflowCancelRequest>) -> Result<Response<WorkflowCancelResponse>, Status> {
        let req = request.into_inner();
        self.transition_state(&req.workflow_id, WorkflowState::Running, WorkflowState::Cancelled).await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(WorkflowCancelResponse {}))
    }

    async fn deploy(&self, request: Request<WorkflowDeployRequest>) -> Result<Response<WorkflowDeployResponse>, Status> {
        let req = request.into_inner();
        match self.deploy_definition(&req.namespace, &req.definition_yaml).await {
            Ok((workflow_id, version, name)) => Ok(Response::new(WorkflowDeployResponse {
                workflow_id,
                version,
                namespace: req.namespace,
                name,
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn list_definitions(&self, request: Request<WorkflowListDefinitionsRequest>) -> Result<Response<WorkflowListDefinitionsResponse>, Status> {
        let req = request.into_inner();
        match self.list_definitions(&req.namespace, req.page_size, &req.page_token).await {
            Ok((definitions, next_token)) => Ok(Response::new(WorkflowListDefinitionsResponse {
                definitions: definitions.into_iter().map(|d| WorkflowDefinitionSummary {
                    workflow_id: d.id,
                    name: d.name,
                    version: d.version,
                    status: d.status,
                    created_at: d.created_at,
                }).collect(),
                next_page_token: next_token,
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn get_definition(&self, request: Request<WorkflowGetDefinitionRequest>) -> Result<Response<WorkflowGetDefinitionResponse>, Status> {
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
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn list_instances(&self, request: Request<WorkflowListInstancesRequest>) -> Result<Response<WorkflowListInstancesResponse>, Status> {
        let req = request.into_inner();
        match self.list_instances(&req.workflow_id, &req.namespace, req.page_size, &req.page_token).await {
            Ok((instances, next_token)) => Ok(Response::new(WorkflowListInstancesResponse {
                instances: instances.into_iter().map(|i| WorkflowInstanceSummary {
                    instance_id: i.id,
                    workflow_id: i.workflow_id,
                    state: i.state,
                    started_at: i.started_at,
                    updated_at: i.updated_at,
                    definition_name: i.definition_name,
                }).collect(),
                next_page_token: next_token,
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }
}

// ════════════════════════════════════════════════════════════
// Policy Service
// ════════════════════════════════════════════════════════════

#[tonic::async_trait]
impl Policy for PolicyService {
    async fn check_permission(&self, request: Request<PolicyCheckPermissionRequest>) -> Result<Response<PolicyCheckPermissionResponse>, Status> {
        let req = request.into_inner();
        let mut context = std::collections::HashMap::new();
        if !req.context.is_empty() {
            if let Ok(map) = serde_json::from_slice::<std::collections::HashMap<String, String>>(&req.context) {
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
                let allowed = matches!(decision.effect, crate::services::policy::PolicyEffect::Allow);
                Ok(Response::new(PolicyCheckPermissionResponse {
                    allowed,
                    reason: decision.reason,
                }))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn evaluate(&self, _request: Request<PolicyEvaluateRequest>) -> Result<Response<PolicyEvaluateResponse>, Status> {
        Err(Status::unimplemented("raw rego evaluate not yet implemented; use check_permission"))
    }

    async fn explain(&self, request: Request<PolicyExplainRequest>) -> Result<Response<PolicyExplainResponse>, Status> {
        let req = request.into_inner();
        let input_json = String::from_utf8_lossy(&req.input).to_string();
        match PolicyService::explain(self, &req.query, &input_json) {
            Ok(trace) => Ok(Response::new(PolicyExplainResponse {
                trace: trace.into_bytes(),
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn put_bundle(&self, request: Request<PolicyPutBundleRequest>) -> Result<Response<PolicyPutBundleResponse>, Status> {
        let req = request.into_inner();
        match self.put_bundle(&req.tenant_id, &req.namespace, &req.name, &req.rego_content).await {
            Ok(info) => Ok(Response::new(PolicyPutBundleResponse {
                bundle_id: info.bundle_id,
                name: info.name,
                namespace: info.namespace,
                created_at: info.created_at,
                updated_at: info.updated_at,
                enabled: info.enabled,
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn delete_bundle(&self, request: Request<PolicyDeleteBundleRequest>) -> Result<Response<PolicyDeleteBundleResponse>, Status> {
        let req = request.into_inner();
        match self.delete_bundle(&req.bundle_id).await {
            Ok(deleted) => Ok(Response::new(PolicyDeleteBundleResponse { deleted })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn list_bundles(&self, request: Request<PolicyListBundlesRequest>) -> Result<Response<PolicyListBundlesResponse>, Status> {
        let req = request.into_inner();
        let tenant_id = if req.tenant_id.is_empty() { None } else { Some(req.tenant_id.as_str()) };
        match self.list_bundles(tenant_id).await {
            Ok(bundles) => {
                let proto_bundles: Vec<PolicyBundleInfo> = bundles.into_iter().map(|b| PolicyBundleInfo {
                    bundle_id: b.bundle_id,
                    name: b.name,
                    namespace: b.namespace,
                    tenant_id: b.tenant_id,
                    enabled: b.enabled,
                    created_at: b.created_at,
                    updated_at: b.updated_at,
                }).collect();
                Ok(Response::new(PolicyListBundlesResponse {
                    bundles: proto_bundles,
                }))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn set_bundle_enabled(&self, request: Request<PolicySetBundleEnabledRequest>) -> Result<Response<PolicySetBundleEnabledResponse>, Status> {
        let req = request.into_inner();
        match self.set_bundle_enabled(&req.bundle_id, req.enabled).await {
            Ok(success) => Ok(Response::new(PolicySetBundleEnabledResponse { success })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }
}

// ════════════════════════════════════════════════════════════
// Transit Service
// ════════════════════════════════════════════════════════════

#[tonic::async_trait]
impl Transit for TransitService {
    async fn encrypt(&self, request: Request<TransitEncryptRequest>) -> Result<Response<TransitEncryptResponse>, Status> {
        let req = request.into_inner();
        match self.encrypt(&req.plaintext) {
            Ok((ciphertext, _dek_id)) => Ok(Response::new(TransitEncryptResponse { ciphertext })),
            Err(e) => Err(Status::internal(e)),
        }
    }

    async fn decrypt(&self, request: Request<TransitDecryptRequest>) -> Result<Response<TransitDecryptResponse>, Status> {
        let req = request.into_inner();
        // DEK ID 现在嵌入在 ciphertext 包头中（自描述格式），不再需要外部传入
        match self.decrypt(&req.ciphertext, "") {
            Ok(plaintext) => Ok(Response::new(TransitDecryptResponse { plaintext })),
            Err(e) => Err(Status::internal(e)),
        }
    }

    async fn hmac_sign(&self, request: Request<TransitHmacSignRequest>) -> Result<Response<TransitHmacSignResponse>, Status> {
        let req = request.into_inner();
        match self.hmac_sign(&req.data, &req.algorithm) {
            Ok(signature) => Ok(Response::new(TransitHmacSignResponse {
                signature,
                algorithm: req.algorithm,
            })),
            Err(e) => Err(Status::internal(e)),
        }
    }

    async fn hmac_verify(&self, request: Request<TransitHmacVerifyRequest>) -> Result<Response<TransitHmacVerifyResponse>, Status> {
        let req = request.into_inner();
        match self.hmac_verify(&req.data, &req.signature, &req.algorithm) {
            Ok(valid) => Ok(Response::new(TransitHmacVerifyResponse { valid })),
            Err(e) => Err(Status::internal(e)),
        }
    }
}

// ════════════════════════════════════════════════════════════
// CircuitBreaker Service
// ════════════════════════════════════════════════════════════

#[tonic::async_trait]
impl CircuitBreaker for CircuitBreakerService {
    async fn get_state(&self, _request: Request<CircuitBreakerGetStateRequest>) -> Result<Response<CircuitBreakerGetStateResponse>, Status> {
        let state = self.state();
        Ok(Response::new(CircuitBreakerGetStateResponse {
            state: format!("{:?}", state),
            last_failure_time: 0,
        }))
    }

    async fn report_success(&self, _request: Request<CircuitBreakerReportSuccessRequest>) -> Result<Response<CircuitBreakerReportSuccessResponse>, Status> {
        self.record_success();
        Ok(Response::new(CircuitBreakerReportSuccessResponse {}))
    }

    async fn report_failure(&self, _request: Request<CircuitBreakerReportFailureRequest>) -> Result<Response<CircuitBreakerReportFailureResponse>, Status> {
        self.record_failure();
        Ok(Response::new(CircuitBreakerReportFailureResponse {}))
    }

    async fn reset(&self, _request: Request<CircuitBreakerResetRequest>) -> Result<Response<CircuitBreakerResetResponse>, Status> {
        self.reset();
        Ok(Response::new(CircuitBreakerResetResponse {}))
    }
}

// ════════════════════════════════════════════════════════════
// RateLimiter Service
// ════════════════════════════════════════════════════════════

#[tonic::async_trait]
impl RateLimiter for RateLimiterService {
    async fn allow(&self, request: Request<RateLimiterAllowRequest>) -> Result<Response<RateLimiterAllowResponse>, Status> {
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
    async fn is_enabled(&self, request: Request<FeatureFlagIsEnabledRequest>) -> Result<Response<FeatureFlagIsEnabledResponse>, Status> {
        let req = request.into_inner();
        match self.is_enabled(&req.flag_name) {
            Ok(enabled) => Ok(Response::new(FeatureFlagIsEnabledResponse {
                enabled,
                variant: String::new(),
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn evaluate(&self, request: Request<FeatureFlagEvaluateRequest>) -> Result<Response<FeatureFlagEvaluateResponse>, Status> {
        let req = request.into_inner();
        let ctx = FlagEvalContext::default();
        match FeatureFlagService::evaluate(self, &req.flag_name, &ctx) {
            Ok(result) => {
                let json = serde_json::to_vec(&result).unwrap_or_default();
                Ok(Response::new(FeatureFlagEvaluateResponse { result: json }))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }
}

// ──── Private helpers ────

fn helper_uuid() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:016x}-{:04x}-4{:03x}-{:04x}-{:012x}",
        ts & 0xFFFFFFFFFFFFFFFF,
        (ts >> 64) as u16 & 0xFFFF,
        (ts >> 80) as u16 & 0xFFF,
        0x8000 | ((ts >> 96) as u16 & 0x3FFF),
        ts & 0xFFFFFFFFFFFF,
    )
}

fn helper_unix_ts() -> u64 {
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
    }.to_string()
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
        .and_then(|l| l.splitn(2, ':').nth(1))
        .map(|s| s.trim().trim_matches('"').trim_matches('\'').to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "workflow".to_string())
}
