//! gRPC service impl: `registry`.
//!
//! Split out from the monolithic `services.rs` in Batch 4a of the
//! April-2026 code review. Shared imports live in [`super`].

use super::*;
use crate::wire::error::coord_status;
use coord_core::error::CoordError;
use coord_core::registry::LeaseRecord;
use uuid::Uuid;

#[derive(Clone)]
pub struct RegistryGrpc {
    registry: Arc<ServiceRegistry>,
    metrics: Arc<CoordMetrics>,
    raft: RaftRuntime,
}

impl RegistryGrpc {
    pub fn new(
        registry: Arc<ServiceRegistry>,
        metrics: Arc<CoordMetrics>,
        raft: RaftRuntime,
    ) -> Self {
        Self {
            registry,
            metrics,
            raft,
        }
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
        let ttl = req.ttl_seconds.max(1);

        // Pre-generate lease so it's deterministic across all Raft nodes.
        let lease = LeaseRecord {
            lease_id: Uuid::new_v4().to_string(),
            ttl_seconds: ttl,
            expires_unix_ms: SystemClock.now_ms() + ttl * 1000,
        };

        let payload = ServiceRegistry::encode_register_bytes(&core_instance, &lease);
        self.raft
            .propose_business_command("registry", payload)
            .await
            .map_err(|err| {
                coord_status(CoordError::Unavailable(format!(
                    "raft propose failed: {err}"
                )))
            })?;

        self.metrics
            .coord_services_registered_total
            .set(self.registry.service_count().await as i64);

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

        let payload =
            ServiceRegistry::encode_deregister_bytes(&instance.service_name, &instance.instance_id);
        self.raft
            .propose_business_command("registry", payload)
            .await
            .map_err(|err| {
                coord_status(CoordError::Unavailable(format!(
                    "raft propose failed: {err}"
                )))
            })?;

        self.metrics
            .coord_services_registered_total
            .set(self.registry.service_count().await as i64);

        Ok(Response::new(()))
    }

    async fn discover(
        &self,
        request: Request<ServiceQuery>,
    ) -> Result<Response<Self::DiscoverStream>, Status> {
        let query = request.into_inner();
        validate_key(&query.service_name).map_err(coord_status)?;
        let instances = self.registry.discover(&query.service_name).await;

        let output = tokio_stream::iter(
            instances
                .into_iter()
                .map(|instance| Ok(to_proto_instance(instance))),
        );
        Ok(Response::new(Box::pin(output)))
    }

    async fn heartbeat(&self, request: Request<Lease>) -> Result<Response<Lease>, Status> {
        self.metrics.coord_service_instances_heartbeat_total.inc();

        let lease = request.into_inner();
        // P4A fix：仅读取当前租约信息（只读操作），不直接写状态；
        // 状态更新通过 Raft propose → apply_heartbeat 确定性地应用，
        // 避免 leader 直接写 + Raft apply 造成双写。
        let current = self
            .registry
            .get_lease(&lease.lease_id)
            .await
            .ok_or_else(|| {
                coord_status(CoordError::NotFound {
                    resource: "lease",
                    id: lease.lease_id.clone(),
                })
            })?;

        let new_expires_unix_ms = SystemClock.now_ms() + current.ttl_seconds * 1000;
        let payload = ServiceRegistry::encode_heartbeat_bytes(&lease.lease_id, new_expires_unix_ms);
        self.raft
            .propose_business_command("registry", payload)
            .await
            .map_err(|err| {
                coord_status(CoordError::Unavailable(format!(
                    "raft propose failed: {err}"
                )))
            })?;

        Ok(Response::new(Lease {
            lease_id: current.lease_id,
            ttl_seconds: current.ttl_seconds,
            expires_unix_ms: new_expires_unix_ms,
        }))
    }
}
