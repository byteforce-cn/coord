use coord_proto::coord::v1::raft_internal_service_server::RaftInternalService;
use coord_proto::coord::v1::{
    RaftAppendEntriesRequest, RaftAppendEntriesResponse, RaftPreVoteRequest, RaftPreVoteResponse,
    RaftRequestVoteRequest, RaftRequestVoteResponse,
};
use tonic::{Request, Response, Status};

use crate::raft_runtime::RaftRuntime;

#[derive(Clone)]
pub struct RaftInternalGrpc {
    raft: RaftRuntime,
}

impl RaftInternalGrpc {
    pub fn new(raft: RaftRuntime) -> Self {
        Self { raft }
    }
}

#[tonic::async_trait]
impl RaftInternalService for RaftInternalGrpc {
    async fn append_entries(
        &self,
        request: Request<RaftAppendEntriesRequest>,
    ) -> Result<Response<RaftAppendEntriesResponse>, Status> {
        let response = self
            .raft
            .handle_append_entries(request.into_inner())
            .await
            .map_err(Status::internal)?;
        Ok(Response::new(response))
    }

    async fn request_vote(
        &self,
        request: Request<RaftRequestVoteRequest>,
    ) -> Result<Response<RaftRequestVoteResponse>, Status> {
        let response = self
            .raft
            .handle_request_vote(request.into_inner())
            .await
            .map_err(Status::internal)?;
        Ok(Response::new(response))
    }

    async fn pre_vote(
        &self,
        request: Request<RaftPreVoteRequest>,
    ) -> Result<Response<RaftPreVoteResponse>, Status> {
        let response = self
            .raft
            .handle_pre_vote(request.into_inner())
            .await
            .map_err(Status::internal)?;
        Ok(Response::new(response))
    }
}
