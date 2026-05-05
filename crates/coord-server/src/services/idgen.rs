//! gRPC service impl: `idgen`.
//!
//! Split out from the monolithic `services.rs` in Batch 4a of the
//! April-2026 code review. Shared imports live in [`super`].

use super::*;

#[derive(Clone)]
pub struct IdGenGrpc {
    idgen: Arc<Snowflake>,
    metrics: Arc<CoordMetrics>,
}

impl IdGenGrpc {
    pub fn new(idgen: Arc<Snowflake>, metrics: Arc<CoordMetrics>) -> Self {
        Self { idgen, metrics }
    }
}

#[tonic::async_trait]
impl IdGenService for IdGenGrpc {
    async fn generate_snowflake(
        &self,
        request: Request<SnowflakeRequest>,
    ) -> Result<Response<SnowflakeResponse>, Status> {
        self.metrics.coord_id_generate_total.inc();

        let req = request.into_inner();
        let ids = self.idgen.batch(req.batch.max(1));
        Ok(Response::new(SnowflakeResponse { ids }))
    }
}
