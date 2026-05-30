//! gRPC service impl: `lock`.
//!
//! Split out from the monolithic `services.rs` in Batch 4a of the
//! April-2026 code review. Shared imports live in [`super`].
//!
//! Business logic (token generation, TTL math, encoding, decoding) lives in
//! [`crate::application::lock_app::LockApp`]; this file is a thin transport
//! adapter that converts proto types and manages the streaming keep-alive loop.

use super::*;
use crate::application::lock_app::{LockAcquireResult, LockApp};
use coord_core::error::CoordError;

#[derive(Clone)]
pub struct LockGrpc {
    lock_app: LockApp,
}

impl LockGrpc {
    pub fn new(lock_app: LockApp) -> Self {
        Self { lock_app }
    }
}

#[tonic::async_trait]
impl LockService for LockGrpc {
    type KeepAliveStream = ReceiverStream<Result<LockKeepAliveResponse, Status>>;

    #[tracing::instrument(skip(self, request), fields(lock_name, owner))]
    async fn acquire(
        &self,
        request: Request<LockAcquireRequest>,
    ) -> Result<Response<LockAcquireResponse>, Status> {
        let req = request.into_inner();
        validate_key(&req.lock_name).map_err(coord_status)?;
        validate_key(&req.owner).map_err(coord_status)?;

        let result = self
            .lock_app
            .acquire(&req.lock_name, &req.owner, req.ttl_seconds, req.wait)
            .await
            .map_err(|e| coord_status(CoordError::Unavailable(e)))?;

        let response = match result {
            LockAcquireResult::Acquired {
                token,
                expires_unix_ms,
                fencing_token,
            } => LockAcquireResponse {
                acquired: true,
                token,
                message: "acquired".to_string(),
                expires_unix_ms,
                fencing_token,
            },
            LockAcquireResult::Queued => LockAcquireResponse {
                acquired: false,
                token: String::new(),
                message: "queued".to_string(),
                expires_unix_ms: 0,
                fencing_token: 0,
            },
            LockAcquireResult::Busy => LockAcquireResponse {
                acquired: false,
                token: String::new(),
                message: "busy".to_string(),
                expires_unix_ms: 0,
                fencing_token: 0,
            },
        };

        Ok(Response::new(response))
    }

    #[tracing::instrument(skip(self, request), fields(lock_name))]
    async fn release(
        &self,
        request: Request<LockReleaseRequest>,
    ) -> Result<Response<LockReleaseResponse>, Status> {
        let req = request.into_inner();
        validate_key(&req.lock_name).map_err(coord_status)?;

        self.lock_app
            .release(&req.lock_name, &req.token)
            .await
            .map_err(|e| coord_status(CoordError::Unavailable(e)))?;

        Ok(Response::new(LockReleaseResponse { released: true }))
    }

    async fn keep_alive(
        &self,
        request: Request<tonic::Streaming<LockKeepAliveRequest>>,
    ) -> Result<Response<Self::KeepAliveStream>, Status> {
        let mut stream = request.into_inner();
        let lock_app = self.lock_app.clone();
        let (tx, rx) = mpsc::channel(64);

        tokio::spawn(async move {
            while let Some(next) = stream.next().await {
                let Ok(req) = next else {
                    break;
                };

                let ttl = req.ttl_seconds.max(1);

                let (ok_flag, expires_unix_ms) =
                    match lock_app.keep_alive(&req.lock_name, &req.token, ttl).await {
                        Ok(Some(exp)) => (true, exp),
                        Ok(None) => (true, 0),
                        Err(_) => (false, 0),
                    };

                if tx
                    .send(Ok(LockKeepAliveResponse {
                        ok: ok_flag,
                        expires_unix_ms,
                    }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
