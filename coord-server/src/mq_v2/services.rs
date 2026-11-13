//! `coord.mq.v2` 的 tonic 服务壳（ADR-0016 M0-4）。
//!
//! 每个 handler 先过 [`MqV2State::gate`]：关闭或能力未交付 → `FAILED_PRECONDITION`。
//! 门禁通过但 handler 尚未实现 → `UNIMPLEMENTED`（实现进度问题，不是配置问题；
//! 混成同一个码会让运维无法判断该改配置还是该升级版本）——与 KV-T v2 同一口径。
//!
//! 服务**始终注册**：关闭时 reflection 仍然可见，客户端在连接阶段就能发现"这个
//! 集群没有 MQ v2"，而不是拿到 `UNAVAILABLE` 后误判为网络故障。

use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use coord_core::error_code::{attach, CoordErrorCode};
use coord_proto::mq::v2 as mq_v2;
use coord_proto::mq::v2::mq_server::Mq;

use super::publish::{publish_response, validate_batch_entries};
use super::topics::{create_topic_response, topic_not_found};
use super::MqV2Capability;
use crate::raft::type_config::Response as RaftResponse;
use crate::server::CoordNode;
use crate::storage::mq_log::MqAppendEntry;

/// 门禁通过但 handler 尚未接线时的响应（M0 壳阶段不会到达：能力集合为空）。
fn unimplemented(capability: MqV2Capability) -> Status {
    attach(
        Status::unimplemented(format!(
            "coord.mq.v2 {} handler is not wired yet (gate pending; see \
             docs/production/ops/mq-v2-gate.md)",
            capability.label()
        )),
        CoordErrorCode::Internal,
    )
}

/// 先过门禁，再按"未实现"返回（M0 的每个 handler 都是这两步）。
macro_rules! gated {
    ($self:ident, $cap:expr) => {{
        $self.mq_v2().gate($cap)?;
        Err(unimplemented($cap))
    }};
}

#[tonic::async_trait]
impl Mq for CoordNode {
    async fn create_topic(
        &self,
        request: Request<mq_v2::CreateTopicRequest>,
    ) -> Result<Response<mq_v2::CreateTopicResponse>, Status> {
        self.mq_v2().gate(MqV2Capability::Topic)?;
        let request = request.into_inner();
        let command = self.mq_topic_create_command(&request)?;
        // topic 表是 region 0 控制面元数据：提案走 system raft（单条命令）。
        let raft = self.raft.as_ref().ok_or_else(|| {
            attach(
                Status::failed_precondition(
                    "mq_v2 CreateTopic requires a region 0 raft (multi_raft assembly \
                     missing)",
                ),
                CoordErrorCode::FailedPrecondition,
            )
        })?;
        let response = self.client_write_with_timeout(raft, command, 0).await?;
        match response.response() {
            RaftResponse::MqTopic { outcome } => Ok(Response::new(create_topic_response(
                &request.topic,
                outcome,
            )?)),
            other => Err(Status::internal(format!(
                "unexpected raft response for CreateTopic: {other:?}"
            ))),
        }
    }

    async fn describe_topic(
        &self,
        request: Request<mq_v2::DescribeTopicRequest>,
    ) -> Result<Response<mq_v2::DescribeTopicResponse>, Status> {
        self.mq_v2().gate(MqV2Capability::Topic)?;
        Ok(Response::new(
            self.mq_describe_topic(&request.into_inner().topic).await?,
        ))
    }

    async fn list_topics(
        &self,
        request: Request<mq_v2::ListTopicsRequest>,
    ) -> Result<Response<mq_v2::ListTopicsResponse>, Status> {
        self.mq_v2().gate(MqV2Capability::Topic)?;
        let request = request.into_inner();
        Ok(Response::new(self.mq_list_topics(
            &request.prefix,
            request.page_size,
            &request.page_token,
        )?))
    }

    async fn update_topic(
        &self,
        request: Request<mq_v2::UpdateTopicRequest>,
    ) -> Result<Response<mq_v2::UpdateTopicResponse>, Status> {
        self.mq_v2().gate(MqV2Capability::Topic)?;
        Ok(Response::new(
            self.mq_update_topic(request.into_inner()).await?,
        ))
    }

    async fn delete_topic(
        &self,
        _request: Request<mq_v2::DeleteTopicRequest>,
    ) -> Result<Response<mq_v2::DeleteTopicResponse>, Status> {
        gated!(self, MqV2Capability::Topic)
    }

    async fn get_topic_deletion(
        &self,
        _request: Request<mq_v2::GetTopicDeletionRequest>,
    ) -> Result<Response<mq_v2::GetTopicDeletionResponse>, Status> {
        gated!(self, MqV2Capability::Topic)
    }

    async fn publish(
        &self,
        request: Request<mq_v2::PublishRequest>,
    ) -> Result<Response<mq_v2::PublishResponse>, Status> {
        self.mq_v2().gate(MqV2Capability::Publish)?;
        let request = request.into_inner();
        let target =
            self.mq_partition_target(&request.topic, request.partition, request.topic_epoch)?;
        let appended = self
            .mq_append(
                &target,
                vec![MqAppendEntry {
                    key: request.key,
                    payload: request.payload,
                    idempotency_key: request.idempotency_key,
                    headers: request.headers.into_iter().collect(),
                    producer: request.producer,
                }],
            )
            .await?;
        let entry = appended
            .entries
            .first()
            .ok_or_else(|| Status::internal("mq append applied no entry for a single publish"))?;
        Ok(Response::new(publish_response(entry)?))
    }

    async fn publish_batch(
        &self,
        request: Request<mq_v2::PublishBatchRequest>,
    ) -> Result<Response<mq_v2::PublishBatchResponse>, Status> {
        self.mq_v2().gate(MqV2Capability::Publish)?;
        let request = request.into_inner();
        // 条数上限先于 topic 查找校验：超限/空批与 topic 是否存在无关。
        validate_batch_entries(request.entries.len())?;
        let target =
            self.mq_partition_target(&request.topic, request.partition, request.topic_epoch)?;
        let entries = request
            .entries
            .into_iter()
            .map(|entry| MqAppendEntry {
                key: entry.key,
                payload: entry.payload,
                idempotency_key: entry.idempotency_key,
                headers: entry.headers.into_iter().collect(),
                producer: entry.producer,
            })
            .collect();
        // 整批是**一个**分区请求：apply 内任一条超限/超配额 ⇒ 一条都不追加。
        let appended = self.mq_append(&target, entries).await?;
        let mut results = Vec::with_capacity(appended.entries.len());
        for entry in &appended.entries {
            results.push(publish_response(entry)?);
        }
        Ok(Response::new(mq_v2::PublishBatchResponse { results }))
    }

    async fn poll(
        &self,
        request: Request<mq_v2::PollRequest>,
    ) -> Result<Response<mq_v2::PollResponse>, Status> {
        self.mq_v2().gate(MqV2Capability::Consume)?;
        // `Poll` 读已提交状态、不提案（长轮询的等待在 `mq_poll` 内）。
        Ok(Response::new(self.mq_poll(request.into_inner()).await?))
    }

    async fn ack(
        &self,
        request: Request<mq_v2::AckRequest>,
    ) -> Result<Response<mq_v2::AckResponse>, Status> {
        self.mq_v2().gate(MqV2Capability::Consume)?;
        Ok(Response::new(self.mq_ack(request.into_inner()).await?))
    }

    async fn ack_batch(
        &self,
        request: Request<mq_v2::AckBatchRequest>,
    ) -> Result<Response<mq_v2::AckBatchResponse>, Status> {
        self.mq_v2().gate(MqV2Capability::Consume)?;
        Ok(Response::new(
            self.mq_ack_batch(request.into_inner()).await?,
        ))
    }

    async fn join_group(
        &self,
        request: Request<mq_v2::JoinGroupRequest>,
    ) -> Result<Response<mq_v2::JoinGroupResponse>, Status> {
        self.mq_v2().gate(MqV2Capability::Consume)?;
        Ok(Response::new(
            self.mq_join_group(request.into_inner()).await?,
        ))
    }

    async fn heartbeat(
        &self,
        request: Request<mq_v2::HeartbeatRequest>,
    ) -> Result<Response<mq_v2::HeartbeatResponse>, Status> {
        self.mq_v2().gate(MqV2Capability::Consume)?;
        Ok(Response::new(
            self.mq_heartbeat(request.into_inner()).await?,
        ))
    }

    async fn leave_group(
        &self,
        request: Request<mq_v2::LeaveGroupRequest>,
    ) -> Result<Response<mq_v2::LeaveGroupResponse>, Status> {
        self.mq_v2().gate(MqV2Capability::Consume)?;
        Ok(Response::new(
            self.mq_leave_group(request.into_inner()).await?,
        ))
    }

    async fn get_committed_offsets(
        &self,
        request: Request<mq_v2::GetCommittedOffsetsRequest>,
    ) -> Result<Response<mq_v2::GetCommittedOffsetsResponse>, Status> {
        self.mq_v2().gate(MqV2Capability::Consume)?;
        Ok(Response::new(
            self.mq_get_committed_offsets(request.into_inner()).await?,
        ))
    }

    async fn describe_group(
        &self,
        request: Request<mq_v2::DescribeGroupRequest>,
    ) -> Result<Response<mq_v2::DescribeGroupResponse>, Status> {
        self.mq_v2().gate(MqV2Capability::Consume)?;
        Ok(Response::new(
            self.mq_describe_group(request.into_inner()).await?,
        ))
    }

    async fn list_groups(
        &self,
        request: Request<mq_v2::ListGroupsRequest>,
    ) -> Result<Response<mq_v2::ListGroupsResponse>, Status> {
        self.mq_v2().gate(MqV2Capability::Consume)?;
        Ok(Response::new(
            self.mq_list_groups(request.into_inner()).await?,
        ))
    }

    type SubscribeStream = ReceiverStream<Result<mq_v2::Message, Status>>;

    async fn subscribe(
        &self,
        request: Request<mq_v2::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        self.mq_v2().gate(MqV2Capability::Subscribe)?;
        // best-effort 推送：背压丢弃、不推进位点（至少一次路径是 Poll + Ack）。
        Ok(Response::new(
            self.mq_subscribe(request.into_inner()).await?,
        ))
    }

    async fn move_to_dlq(
        &self,
        request: Request<mq_v2::MoveToDlqRequest>,
    ) -> Result<Response<mq_v2::MoveToDlqResponse>, Status> {
        self.mq_v2().gate(MqV2Capability::Dlq)?;
        Ok(Response::new(
            self.mq_move_to_dlq(request.into_inner()).await?,
        ))
    }

    async fn poll_dlq(
        &self,
        request: Request<mq_v2::PollDlqRequest>,
    ) -> Result<Response<mq_v2::PollDlqResponse>, Status> {
        self.mq_v2().gate(MqV2Capability::Dlq)?;
        Ok(Response::new(self.mq_poll_dlq(request.into_inner()).await?))
    }

    async fn get_topic_leader(
        &self,
        request: Request<mq_v2::GetTopicLeaderRequest>,
    ) -> Result<Response<mq_v2::GetTopicLeaderResponse>, Status> {
        self.mq_v2().gate(MqV2Capability::Topic)?;
        let request = request.into_inner();
        let table = self.mq_topic_table()?;
        let Some(meta) = table.topic(&request.topic) else {
            return Err(topic_not_found(&request.topic));
        };
        Ok(Response::new(mq_v2::GetTopicLeaderResponse {
            topic: meta.name.clone(),
            topic_id: meta.topic_id,
            topic_epoch: meta.epoch,
            partitions: self.mq_partition_leaders(meta).await,
        }))
    }
}
