//! gRPC service impl: `auth`.
//!
//! Split out from the monolithic `services.rs` in Batch 4a of the
//! April-2026 code review. Shared imports live in [`super`].

use super::*;
use crate::application::security_app::SecurityApp;
use crate::wire::error::coord_status;
use coord_core::error::CoordError;

#[derive(Clone)]
pub struct AuthGrpc {
    security_app: SecurityApp,
}

impl AuthGrpc {
    pub fn new(security_app: SecurityApp) -> Self {
        Self { security_app }
    }
}

#[tonic::async_trait]
impl AuthService for AuthGrpc {
    async fn create_app_role(
        &self,
        request: Request<CreateAppRoleRequest>,
    ) -> Result<Response<CreateAppRoleResponse>, Status> {
        let req = request.into_inner();
        let role = self
            .security_app
            .create_approle(
                &req.role_name,
                req.policies,
                req.token_ttl_seconds,
                req.secret_id_ttl_seconds,
                req.secret_id_num_uses,
            )
            .await
            .map_err(coord_status)?;

        Ok(Response::new(CreateAppRoleResponse {
            role_id: role.role_id,
            role_name: role.role_name,
            policies: role.policies,
            token_ttl_seconds: role.token_ttl_seconds,
            secret_id_ttl_seconds: role.secret_id_ttl_seconds,
            secret_id_num_uses: role.secret_id_num_uses,
        }))
    }

    async fn generate_secret_id(
        &self,
        request: Request<GenerateSecretIdRequest>,
    ) -> Result<Response<GenerateSecretIdResponse>, Status> {
        let req = request.into_inner();
        let role_id = if req.role_id.is_empty() {
            // look up role_id by role_name
            let snapshot = self.security_app.export_auth_state_snapshot().await;
            snapshot
                .roles
                .into_iter()
                .find(|r| r.role_name == req.role_name)
                .map(|r| r.role_id)
                .ok_or_else(|| {
                    coord_status(CoordError::NotFound {
                        resource: "approle",
                        id: req.role_name.clone(),
                    })
                })?
        } else {
            req.role_id
        };
        let secret = self
            .security_app
            .generate_secret_id(&role_id)
            .await
            .map_err(coord_status)?;

        Ok(Response::new(GenerateSecretIdResponse {
            role_id: secret.role_id,
            secret_id: secret.secret_id,
            expires_unix_seconds: secret.expires_unix_seconds,
        }))
    }

    #[tracing::instrument(skip(self, request), fields(role_id))]
    async fn login_app_role(
        &self,
        request: Request<LoginAppRoleRequest>,
    ) -> Result<Response<LoginAppRoleResponse>, Status> {
        let req = request.into_inner();
        let token = self
            .security_app
            .login_approle(&req.role_id, &req.secret_id)
            .await
            .map_err(coord_status)?;

        Ok(Response::new(LoginAppRoleResponse {
            access_token: token.access_token.clone(),
            token: token.access_token,
            role_id: token.role_id,
            policies: token.policies,
            expires_unix_seconds: token.expires_unix_seconds,
        }))
    }

    #[tracing::instrument(skip(self, request))]
    async fn lookup_token(
        &self,
        request: Request<LookupTokenRequest>,
    ) -> Result<Response<LookupTokenResponse>, Status> {
        let req = request.into_inner();
        let token_str = if req.access_token.is_empty() {
            req.token
        } else {
            req.access_token
        };
        let token = self.security_app.lookup_token(&token_str).await;

        Ok(Response::new(LookupTokenResponse {
            valid: token.valid,
            role_id: token.role_id,
            policies: token.policies,
            expires_unix_seconds: token.expires_unix_seconds,
        }))
    }

    async fn revoke_token(
        &self,
        request: Request<RevokeTokenRequest>,
    ) -> Result<Response<RevokeTokenResponse>, Status> {
        let req = request.into_inner();
        let token_str = if req.access_token.is_empty() {
            req.token
        } else {
            req.access_token
        };
        let revoked = self
            .security_app
            .revoke_token(&token_str)
            .await
            .map_err(coord_status)?;

        Ok(Response::new(RevokeTokenResponse { revoked }))
    }

    async fn get_app_role_id(
        &self,
        request: Request<GetAppRoleIdRequest>,
    ) -> Result<Response<GetAppRoleIdResponse>, Status> {
        let req = request.into_inner();
        let snapshot = self.security_app.export_auth_state_snapshot().await;
        let role = snapshot
            .roles
            .into_iter()
            .find(|r| r.role_name == req.role_name)
            .ok_or_else(|| {
                coord_status(CoordError::NotFound {
                    resource: "approle",
                    id: req.role_name.clone(),
                })
            })?;

        Ok(Response::new(GetAppRoleIdResponse {
            role_id: role.role_id,
            role_name: role.role_name,
        }))
    }
}
