//! gRPC service impl: `seal`.
//!
//! Split out from the monolithic `services.rs` in Batch 4a of the
//! April-2026 code review. Shared imports live in [`super`].

use super::*;

#[derive(Clone)]
pub struct SealGrpc {
    security: Arc<SecurityController>,
    domain_lifecycle: Arc<DomainLifecycleManager>,
    metrics: Arc<CoordMetrics>,
}

impl SealGrpc {
    pub fn new(
        security: Arc<SecurityController>,
        domain_lifecycle: Arc<DomainLifecycleManager>,
        metrics: Arc<CoordMetrics>,
    ) -> Self {
        Self {
            security,
            domain_lifecycle,
            metrics,
        }
    }

    async fn do_init(
        &self,
        request: Request<InitSecurityRequest>,
    ) -> Result<Response<InitSecurityResponse>, Status> {
        let req = request.into_inner();
        let shares_total = req.secret_shares.max(req.shares_total).max(1);
        let threshold = req.secret_threshold.max(req.threshold).max(1);
        let mut domain = self
            .domain_lifecycle
            .capture(self.security.export_auth_state_snapshot().await)
            .await;

        // Create a real root access token and embed it in the domain so it
        // survives seal/unseal cycles.  A "root" role is needed because
        // restore_auth_from_snapshot skips tokens whose role_id is missing.
        let root_token = generate_root_token();
        let root_snapshot = create_root_token_snapshot(&root_token, 86400);
        domain.auth.roles.push(SecurityRoleSnapshot {
            role_id: "root".to_string(),
            role_name: "root".to_string(),
            policies: vec!["*".to_string()],
            token_ttl_seconds: 86400,
            secret_id_ttl_seconds: 86400,
            secret_id_num_uses: 0,
        });
        domain.auth.access_tokens.push(root_snapshot);

        let shares = self
            .security
            .init_security_with_domain(shares_total, threshold, domain)
            .await
            .map_err(coord_status)?;

        self.domain_lifecycle.clear().await.map_err(coord_status)?;
        self.metrics.coord_security_sealed.set(1);

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
}

#[tonic::async_trait]
impl SealService for SealGrpc {
    #[tracing::instrument(skip(self, request))]
    async fn init(
        &self,
        request: Request<InitSecurityRequest>,
    ) -> Result<Response<InitSecurityResponse>, Status> {
        self.do_init(request).await
    }

    #[tracing::instrument(skip(self, request))]
    async fn init_seal(
        &self,
        request: Request<InitSecurityRequest>,
    ) -> Result<Response<InitSecurityResponse>, Status> {
        self.do_init(request).await
    }

    #[tracing::instrument(skip(self, _request))]
    async fn get_seal_status(
        &self,
        _request: Request<GetSealStatusRequest>,
    ) -> Result<Response<GetSealStatusResponse>, Status> {
        let status = self.security.seal_status().await;
        self.metrics
            .coord_security_sealed
            .set(if status.initialized && status.sealed {
                1
            } else {
                0
            });

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
        self.metrics.coord_security_unseal_attempts_total.inc();

        let req = request.into_inner();
        let share = if req.share.is_empty() {
            req.key_share
        } else {
            req.share
        };
        let status = self.security.unseal(&share).await.map_err(coord_status)?;

        if !status.sealed
            && let Some(domain) = self.security.take_unsealed_domain_snapshot().await
        {
            self.domain_lifecycle
                .restore_domain(domain)
                .await
                .map_err(coord_status)?;
        }

        self.metrics
            .coord_security_sealed
            .set(if status.sealed { 1 } else { 0 });

        Ok(Response::new(UnsealResponse {
            sealed: status.sealed,
            progress: status.progress,
            threshold: status.threshold,
        }))
    }

    #[tracing::instrument(skip(self, _request))]
    async fn seal(&self, _request: Request<SealRequest>) -> Result<Response<SealResponse>, Status> {
        let pre_status = self.security.seal_status().await;
        let status = if pre_status.initialized && !pre_status.sealed {
            let domain = self
                .domain_lifecycle
                .capture(self.security.export_auth_state_snapshot().await)
                .await;
            self.security
                .seal_with_domain(domain)
                .await
                .map_err(coord_status)?
        } else {
            self.security.seal().await.map_err(coord_status)?
        };

        if status.sealed {
            self.domain_lifecycle.clear().await.map_err(coord_status)?;
        }

        self.metrics
            .coord_security_sealed
            .set(if status.sealed { 1 } else { 0 });

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
            .security
            .rotate_unseal_shares(shares_total, threshold)
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
