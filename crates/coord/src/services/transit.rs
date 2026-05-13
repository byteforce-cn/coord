//! gRPC service impl: `transit`.
//!
//! Split out from the monolithic `services.rs` in Batch 4a of the
//! April-2026 code review. Shared imports live in [`super`].
//!
//! Write operations (create_key, rotate_key) are delegated to
//! [`crate::application::transit_app::TransitApp`]; this file is a thin
//! transport adapter that converts proto types.

use super::*;
use crate::application::transit_app::TransitApp;
use coord_core::error::CoordError;

#[derive(Clone)]
pub struct TransitGrpc {
    transit_app: TransitApp,
}

impl TransitGrpc {
    pub fn new(transit_app: TransitApp) -> Self {
        Self { transit_app }
    }
}

#[tonic::async_trait]
impl TransitService for TransitGrpc {
    async fn create_key(
        &self,
        request: Request<CreateKeyRequest>,
    ) -> Result<Response<CreateKeyResponse>, Status> {
        let req = request.into_inner();
        let info = self
            .transit_app
            .create_key(&req.key_name)
            .await
            .map_err(|e| coord_status(CoordError::Unavailable(e)))?;

        Ok(Response::new(CreateKeyResponse {
            key_name: info.key_name,
            primary_version: info.primary_version,
            algorithm: String::new(),
        }))
    }

    async fn encrypt(
        &self,
        request: Request<EncryptRequest>,
    ) -> Result<Response<EncryptResponse>, Status> {
        let req = request.into_inner();
        let (ciphertext, version) = self
            .transit_app
            .engine()
            .encrypt(&req.key_name, req.plaintext.as_bytes())
            .await
            .map_err(coord_status)?;

        self.transit_app
            .metrics()
            .coord_transit_encryption_requests_total
            .inc();

        Ok(Response::new(EncryptResponse {
            ciphertext,
            version,
        }))
    }

    async fn decrypt(
        &self,
        request: Request<DecryptRequest>,
    ) -> Result<Response<DecryptResponse>, Status> {
        let req = request.into_inner();
        let (plaintext_bytes, version) = self
            .transit_app
            .engine()
            .decrypt(&req.key_name, &req.ciphertext)
            .await
            .map_err(coord_status)?;

        let plaintext = String::from_utf8_lossy(&plaintext_bytes).into_owned();
        Ok(Response::new(DecryptResponse { plaintext, version }))
    }

    async fn rotate_key(
        &self,
        request: Request<RotateKeyRequest>,
    ) -> Result<Response<RotateKeyResponse>, Status> {
        let req = request.into_inner();
        let info = self
            .transit_app
            .rotate_key(&req.key_name)
            .await
            .map_err(|e| coord_status(CoordError::Unavailable(e)))?;

        Ok(Response::new(RotateKeyResponse {
            key_name: info.key_name,
            primary_version: info.primary_version,
        }))
    }

    async fn hmac_sign(
        &self,
        request: Request<HmacSignRequest>,
    ) -> Result<Response<HmacSignResponse>, Status> {
        let req = request.into_inner();
        let (hmac, version) = self
            .transit_app
            .engine()
            .hmac_sign(&req.key_name, req.input.as_bytes())
            .await
            .map_err(coord_status)?;

        Ok(Response::new(HmacSignResponse { hmac, version }))
    }

    async fn hmac_verify(
        &self,
        request: Request<HmacVerifyRequest>,
    ) -> Result<Response<HmacVerifyResponse>, Status> {
        let req = request.into_inner();
        let valid = self
            .transit_app
            .engine()
            .hmac_verify(&req.key_name, req.input.as_bytes(), &req.hmac)
            .await
            .map_err(coord_status)?;

        Ok(Response::new(HmacVerifyResponse { valid }))
    }

    async fn get_transit_key(
        &self,
        request: Request<GetTransitKeyRequest>,
    ) -> Result<Response<GetTransitKeyResponse>, Status> {
        let req = request.into_inner();
        let info = self
            .transit_app
            .engine()
            .get_key_info(&req.key_name)
            .await
            .map_err(coord_status)?;
        Ok(Response::new(GetTransitKeyResponse {
            key_name: info.key_name,
            algorithm: "AES256-GCM96".to_string(),
            primary_version: info.primary_version,
        }))
    }
}
