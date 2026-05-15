//! gRPC service impl: `policy` (PDP — Policy Decision Point).
//!
//! Thin transport adapter: proto ↔ application types.
//! Business logic lives in [`crate::application::policy_app::PolicyApp`].

use super::*;
use crate::application::policy_app::{PolicyApp, PolicyAppError};
use coord_core::policy::model::PolicyError;

/// Convert a [`PolicyAppError`] into a [`tonic::Status`].
fn policy_app_status(err: PolicyAppError) -> Status {
    match &err {
        PolicyAppError::InvalidInput(msg) => Status::invalid_argument(msg.clone()),
        PolicyAppError::Raft(msg) => Status::unavailable(msg.clone()),
        PolicyAppError::Codec(msg) => Status::internal(msg.clone()),
        PolicyAppError::Policy(policy_err) => policy_error_status(policy_err),
    }
}

fn policy_error_status(err: &PolicyError) -> Status {
    match err {
        PolicyError::BundleNotFound { id } => {
            Status::not_found(format!("policy bundle not found: {id}"))
        }
        PolicyError::BundleDisabled { id } => {
            Status::failed_precondition(format!("policy bundle is disabled: {id}"))
        }
        PolicyError::Evaluation { message } => {
            Status::internal(format!("policy evaluation error: {message}"))
        }
        PolicyError::InputTooLarge { size, limit } => Status::invalid_argument(format!(
            "input too large: {size} bytes (limit {limit})"
        )),
        PolicyError::Store { message } => Status::internal(format!("policy store error: {message}")),
    }
}

#[derive(Clone)]
pub struct PolicyGrpc {
    app: PolicyApp,
}

impl PolicyGrpc {
    pub fn new(app: PolicyApp) -> Self {
        Self { app }
    }
}

#[tonic::async_trait]
impl PolicyService for PolicyGrpc {
    async fn put_policy_bundle(
        &self,
        request: Request<PutPolicyBundleRequest>,
    ) -> Result<Response<PutPolicyBundleResponse>, Status> {
        let req = request.into_inner();
        let bundle = self
            .app
            .put_bundle(req.tenant_id, req.namespace, req.name, req.rego_source)
            .await
            .map_err(policy_app_status)?;

        Ok(Response::new(PutPolicyBundleResponse {
            id: bundle.id,
            tenant_id: bundle.tenant_id,
            namespace: bundle.namespace,
            name: bundle.name,
            version: bundle.version,
            enabled: bundle.enabled,
            created_at_ms: bundle.created_at_ms,
            updated_at_ms: bundle.updated_at_ms,
        }))
    }

    async fn set_bundle_enabled(
        &self,
        request: Request<SetBundleEnabledRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        self.app
            .set_enabled(&req.id, req.enabled)
            .await
            .map_err(policy_app_status)?;
        Ok(Response::new(()))
    }

    async fn delete_policy_bundle(
        &self,
        request: Request<DeletePolicyBundleRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        self.app
            .delete_bundle(&req.id)
            .await
            .map_err(policy_app_status)?;
        Ok(Response::new(()))
    }

    async fn list_policy_bundles(
        &self,
        request: Request<ListPolicyBundlesRequest>,
    ) -> Result<Response<ListPolicyBundlesResponse>, Status> {
        let req = request.into_inner();
        let tenant_filter = if req.tenant_id.is_empty() {
            None
        } else {
            Some(req.tenant_id.as_str())
        };
        let bundles = self.app.list_bundles(tenant_filter).await;

        let infos = bundles
            .into_iter()
            .map(|b| PolicyBundleInfo {
                id: b.id,
                tenant_id: b.tenant_id,
                namespace: b.namespace,
                name: b.name,
                version: b.version,
                enabled: b.enabled,
                created_at_ms: b.created_at_ms,
                updated_at_ms: b.updated_at_ms,
            })
            .collect();

        Ok(Response::new(ListPolicyBundlesResponse { bundles: infos }))
    }

    async fn evaluate(
        &self,
        request: Request<EvaluateRequest>,
    ) -> Result<Response<EvaluateResponse>, Status> {
        let req = request.into_inner();
        let input: serde_json::Value = serde_json::from_str(&req.input_json)
            .map_err(|e| Status::invalid_argument(format!("invalid input_json: {e}")))?;

        let result = self
            .app
            .evaluate(&req.bundle_id, &req.query, &input)
            .await
            .map_err(policy_app_status)?;

        let result_json = serde_json::to_string(&result.value)
            .map_err(|e| Status::internal(format!("failed to serialize result: {e}")))?;

        Ok(Response::new(EvaluateResponse {
            result_json,
            allowed: result.allowed,
        }))
    }

    async fn explain(
        &self,
        request: Request<EvaluateRequest>,
    ) -> Result<Response<ExplainResponse>, Status> {
        let req = request.into_inner();
        let input: serde_json::Value = serde_json::from_str(&req.input_json)
            .map_err(|e| Status::invalid_argument(format!("invalid input_json: {e}")))?;

        let lines = self
            .app
            .explain(&req.bundle_id, &req.query, &input)
            .await
            .map_err(policy_app_status)?;

        Ok(Response::new(ExplainResponse { lines }))
    }
}
