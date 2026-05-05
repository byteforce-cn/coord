//! gRPC service impl: `config`.
//!
//! Split out from the monolithic `services.rs` in Batch 4a of the
//! April-2026 code review. Shared imports live in [`super`].
//!
//! Business logic (key validation, Raft proposal, subscription) lives in
//! [`crate::application::config_app::ConfigApp`]; this file is a thin
//! transport adapter that converts proto types and manages the streaming loop.

use super::*;
use crate::application::config_app::ConfigApp;
use coord_core::error::CoordError;

#[derive(Clone)]
pub struct ConfigGrpc {
    config_app: ConfigApp,
}

impl ConfigGrpc {
    pub fn new(config_app: ConfigApp) -> Self {
        Self { config_app }
    }
}

#[tonic::async_trait]
impl ConfigService for ConfigGrpc {
    type WatchConfigStream = ReceiverStream<Result<ConfigResponse, Status>>;

    async fn get_config(
        &self,
        request: Request<ConfigRequest>,
    ) -> Result<Response<ConfigResponse>, Status> {
        let req = request.into_inner();
        validate_key(&req.key).map_err(coord_status)?;
        let entry = self.config_app.get(&req.key).await.ok_or_else(|| {
            coord_status(CoordError::NotFound {
                resource: "config",
                id: req.key.clone(),
            })
        })?;

        Ok(Response::new(to_proto_config(entry)))
    }

    async fn put_config(
        &self,
        request: Request<PutConfigRequest>,
    ) -> Result<Response<ConfigResponse>, Status> {
        let req = request.into_inner();
        validate_key(&req.key).map_err(coord_status)?;

        let entry = self
            .config_app
            .put(req.key, req.value)
            .await
            .map_err(|e| coord_status(CoordError::Internal(e)))?;
        Ok(Response::new(to_proto_config(entry)))
    }

    async fn watch_config(
        &self,
        request: Request<ConfigRequest>,
    ) -> Result<Response<Self::WatchConfigStream>, Status> {
        let req = request.into_inner();
        validate_key(&req.key).map_err(coord_status)?;

        let sub = self.config_app.subscribe(&req.key).await;
        let mut rx = sub.receiver;
        let current = sub.current;

        let (tx, out_rx) = mpsc::channel(64);
        let gauge = self
            .config_app
            .metrics()
            .coord_config_watches_active
            .clone();
        gauge.inc();

        tokio::spawn(async move {
            if let Some(existing) = current {
                let _ = tx.send(Ok(to_proto_config(existing))).await;
            }

            loop {
                match rx.recv().await {
                    Ok(entry) => {
                        if tx.send(Ok(to_proto_config(entry))).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }

            gauge.dec();
        });

        Ok(Response::new(ReceiverStream::new(out_rx)))
    }
}
