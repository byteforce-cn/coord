//! gRPC service impl: `seal`.
//!
//! Split out from the monolithic `services.rs` in Batch 4a of the
//! April-2026 code review. Shared imports live in [`super`].

use super::*;
use crate::application::security_app::SecurityApp;

#[derive(Clone)]
pub struct SealGrpc {
    security_app: SecurityApp,
}

impl SealGrpc {
    pub fn new(security_app: SecurityApp) -> Self {
        Self { security_app }
    }
}

#[tonic::async_trait]
impl SealService for SealGrpc {
    #[tracing::instrument(skip(self, request))]
    async fn init(
        &self,
        request: Request<InitSecurityRequest>,
    ) -> Result<Response<InitSecurityResponse>, Status> {
        let req = request.into_inner();
        let shares_total = req.secret_shares.max(req.shares_total).max(1);
        let threshold = req.secret_threshold.max(req.threshold).max(1);

        let (shares, root_token) = self
            .security_app
            .init_domain(shares_total, threshold)
            .await
            .map_err(coord_status)?;

        Ok(Response::new(InitSecurityResponse {
            initialized: true,
            sealed: true,
            shares_total,
            threshold,
            unseal_shares: shares.clone(),
            key_shares: shares,
            root_token,
        }))
    }

    #[tracing::instrument(skip(self, request))]
    async fn init_seal(
        &self,
        request: Request<InitSecurityRequest>,
    ) -> Result<Response<InitSecurityResponse>, Status> {
        // `init_seal` is an alias for `init`.
        self.init(request).await
    }

    #[tracing::instrument(skip(self, _request))]
    async fn get_seal_status(
        &self,
        _request: Request<GetSealStatusRequest>,
    ) -> Result<Response<GetSealStatusResponse>, Status> {
        let status = self.security_app.seal_status().await;

        Ok(Response::new(GetSealStatusResponse {
            initialized: status.initialized,
            sealed: status.sealed,
            shares_total: status.shares_total,
            threshold: status.threshold,
            progress: status.progress,
        }))
    }

    #[tracing::instrument(skip(self, request))]
    async fn unseal(
        &self,
        request: Request<UnsealRequest>,
    ) -> Result<Response<UnsealResponse>, Status> {
        let req = request.into_inner();
        let share = if req.share.is_empty() {
            req.key_share
        } else {
            req.share
        };
        let status = self
            .security_app
            .unseal(&share)
            .await
            .map_err(coord_status)?;

        Ok(Response::new(UnsealResponse {
            sealed: status.sealed,
            progress: status.progress,
            threshold: status.threshold,
        }))
    }

    #[tracing::instrument(skip(self, _request))]
    async fn seal(&self, _request: Request<SealRequest>) -> Result<Response<SealResponse>, Status> {
        let status = self.security_app.seal().await.map_err(coord_status)?;

        Ok(Response::new(SealResponse {
            sealed: status.sealed,
        }))
    }

    async fn rotate_root_key(
        &self,
        request: Request<RotateRootKeyRequest>,
    ) -> Result<Response<RotateRootKeyResponse>, Status> {
        let req = request.into_inner();
        let shares_total = req.shares_total.max(1);
        let threshold = req.threshold.max(1);

        let shares = self
            .security_app
            .rotate_root_key(shares_total, threshold)
            .await
            .map_err(coord_status)?;

        Ok(Response::new(RotateRootKeyResponse {
            rotated: true,
            shares_total,
            threshold,
            unseal_shares: shares,
        }))
    }
}
