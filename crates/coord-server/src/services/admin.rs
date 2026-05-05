//! gRPC service impl: `admin`.
//!
//! Split out from the monolithic `services.rs` in Batch 4a of the
//! April-2026 code review. Shared imports live in [`super`].

use super::*;
use crate::wire::error::coord_status;
use coord_core::error::CoordError;

#[derive(Clone)]
pub struct AdminGrpc {
    members: Arc<RwLock<HashMap<String, String>>>,
    locks: Arc<LockManager>,
    runtime: RuntimeConfig,
    raft: RaftRuntime,
}

impl AdminGrpc {
    pub fn new(
        members: Arc<RwLock<HashMap<String, String>>>,
        locks: Arc<LockManager>,
        runtime: RuntimeConfig,
        _metrics: Arc<CoordMetrics>,
        raft: RaftRuntime,
    ) -> Self {
        Self {
            members,
            locks,
            runtime,
            raft,
        }
    }
}

#[tonic::async_trait]
impl AdminService for AdminGrpc {
    async fn cluster_status(
        &self,
        _request: Request<ClusterStatusRequest>,
    ) -> Result<Response<ClusterStatusResponse>, Status> {
        let mut members: Vec<String> = self.members.read().await.keys().cloned().collect();
        members.sort();

        Ok(Response::new(ClusterStatusResponse {
            node_id: self.runtime.node_id.clone(),
            state: self.raft.role_label().await,
            members,
            dev_mode: self.runtime.dev_mode,
        }))
    }

    async fn list_locks(
        &self,
        _request: Request<()>,
    ) -> Result<Response<LockListResponse>, Status> {
        let locks = self
            .locks
            .list_holders()
            .await
            .into_iter()
            .map(|lock| LockInfo {
                lock_name: lock.lock_name,
                owner: lock.owner,
                expires_unix_ms: lock.expires_unix_ms,
            })
            .collect();

        Ok(Response::new(LockListResponse { locks }))
    }

    async fn member_add(
        &self,
        request: Request<MemberAddRequest>,
    ) -> Result<Response<MemberAddResponse>, Status> {
        let req = request.into_inner();
        if req.node_id.trim().is_empty() || req.address.trim().is_empty() {
            return Err(coord_status(CoordError::InvalidArgument(
                "node_id and address are required".to_string(),
            )));
        }

        let (added, members) = self
            .raft
            .propose_member_add(req.node_id, req.address)
            .await
            .map_err(|e| coord_status(CoordError::Unavailable(e)))?;

        Ok(Response::new(MemberAddResponse { added, members }))
    }

    async fn member_remove(
        &self,
        request: Request<MemberRemoveRequest>,
    ) -> Result<Response<MemberRemoveResponse>, Status> {
        let req = request.into_inner();
        if req.node_id.trim().is_empty() {
            return Err(coord_status(CoordError::InvalidArgument(
                "node_id is required".to_string(),
            )));
        }
        let (removed, members) = self
            .raft
            .propose_member_remove(req.node_id, req.force_unreachable)
            .await
            .map_err(|e| coord_status(CoordError::Unavailable(e)))?;

        Ok(Response::new(MemberRemoveResponse { removed, members }))
    }

    #[tracing::instrument(skip(self, _request))]
    async fn create_backup(
        &self,
        _request: Request<BackupCreateRequest>,
    ) -> Result<Response<BackupCreateResponse>, Status> {
        let payload = self
            .raft
            .snapshot_backup_payload()
            .await
            .map_err(|e| coord_status(CoordError::Unavailable(e)))?;
        let created_unix_ms = payload.created_unix_ms;

        let payload_json = persistence::payload_to_json_v5(&payload).map_err(|err| {
            coord_status(CoordError::Internal(format!(
                "failed to serialize backup payload: {err}"
            )))
        })?;

        Ok(Response::new(BackupCreateResponse {
            payload_json,
            created_unix_ms,
        }))
    }

    #[tracing::instrument(skip(self, request))]
    async fn restore_backup(
        &self,
        request: Request<BackupRestoreRequest>,
    ) -> Result<Response<BackupRestoreResponse>, Status> {
        let req = request.into_inner();
        if req.payload_json.trim().is_empty() {
            return Err(coord_status(CoordError::InvalidArgument(
                "payload_json cannot be empty".to_string(),
            )));
        }

        let message = self
            .raft
            .propose_backup_restore(req.payload_json)
            .await
            .map_err(|e| coord_status(CoordError::Unavailable(e)))?;

        Ok(Response::new(BackupRestoreResponse {
            restored: true,
            message,
        }))
    }
}
