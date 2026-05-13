//! gRPC service impl: `idgen`.
//!
//! Split out from the monolithic `services.rs` in Batch 4a of the
//! April-2026 code review. Shared imports live in [`super`].

use super::*;
use crate::application::idgen_app::IdGenApp;

#[derive(Clone)]
pub struct IdGenGrpc {
    idgen_app: IdGenApp,
}

impl IdGenGrpc {
    pub fn new(idgen_app: IdGenApp) -> Self {
        Self { idgen_app }
    }
}

#[tonic::async_trait]
impl IdGenService for IdGenGrpc {
    async fn generate_snowflake(
        &self,
        request: Request<SnowflakeRequest>,
    ) -> Result<Response<SnowflakeResponse>, Status> {
        let req = request.into_inner();
        let ids = self.idgen_app.generate(req.batch.max(1) as u32);
        Ok(Response::new(SnowflakeResponse { ids }))
    }
}
