//! gRPC service impl: `registry`.
//!
//! Split out from the monolithic `services.rs` in Batch 4a of the
//! April-2026 code review. Shared imports live in [`super`].
//!
//! Business logic (lease generation, TTL math, Raft proposal) lives in
//! [`crate::application::registry_app::RegistryApp`]; this file is a thin
//! transport adapter that converts proto types.

use super::*;
use crate::application::registry_app::RegistryApp;
use crate::wire::error::coord_status;
use coord_core::error::CoordError;

#[derive(Clone)]
pub struct RegistryGrpc {
    registry_app: RegistryApp,
}

impl RegistryGrpc {
    pub fn new(registry_app: RegistryApp) -> Self {
        Self { registry_app }
    }
}

#[tonic::async_trait]
impl RegistryService for RegistryGrpc {
    type DiscoverStream =
        Pin<Box<dyn Stream<Item = Result<ServiceInstance, Status>> + Send + 'static>>;

    async fn register(&self, request: Request<RegisterRequest>) -> Result<Response<Lease>, Status> {
        let req = request.into_inner();
        let instance = req.instance.ok_or_else(|| {
            coord_status(CoordError::InvalidArgument(
                "instance is required".to_string(),
            ))
        })?;

        let core_instance = to_core_instance(instance)?;

        let lease = self
            .registry_app
            .register(core_instance, req.ttl_seconds)
            .await
            .map_err(|err| coord_status(CoordError::Unavailable(err)))?;

        Ok(Response::new(Lease {
            lease_id: lease.lease_id,
            ttl_seconds: lease.ttl_seconds,
            expires_unix_ms: lease.expires_unix_ms,
        }))
    }

    async fn deregister(&self, request: Request<ServiceInstance>) -> Result<Response<()>, Status> {
        let instance = request.into_inner();
        validate_key(&instance.service_name).map_err(coord_status)?;
        validate_key(&instance.instance_id).map_err(coord_status)?;

        self.registry_app
            .deregister(&instance.service_name, &instance.instance_id)
            .await
            .map_err(|err| coord_status(CoordError::Unavailable(err)))?;

        Ok(Response::new(()))
    }

    async fn discover(
        &self,
        request: Request<ServiceQuery>,
    ) -> Result<Response<Self::DiscoverStream>, Status> {
        let query = request.into_inner();
        validate_key(&query.service_name).map_err(coord_status)?;
        let instances = self
            .registry_app
            .registry()
            .discover(&query.service_name)
            .await;

        let output = tokio_stream::iter(
            instances
                .into_iter()
                .map(|instance| Ok(to_proto_instance(instance))),
        );
        Ok(Response::new(Box::pin(output)))
    }

    async fn heartbeat(&self, request: Request<Lease>) -> Result<Response<Lease>, Status> {
        let lease = request.into_inner();

        let renewed = self
            .registry_app
            .heartbeat(&lease.lease_id)
            .await
            .map_err(|_e| {
                coord_status(CoordError::NotFound {
                    resource: "lease",
                    id: lease.lease_id.clone(),
                })
            })?;

        Ok(Response::new(Lease {
            lease_id: renewed.lease_id,
            ttl_seconds: renewed.ttl_seconds,
            expires_unix_ms: renewed.expires_unix_ms,
        }))
    }
}
