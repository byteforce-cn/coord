//! Application service: transit encryption key management.
//!
//! [`TransitApp`] encapsulates all write-path business logic for the transit
//! namespace: key creation and rotation, both replicated via Raft.
//! Read-only operations (encrypt, decrypt, HMAC, snapshot) remain on the
//! [`TransitEngine`] directly.

use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use rand::RngCore;
use rand::rngs::OsRng;

use coord_core::metrics::CoordMetrics;
use coord_core::transit::{TransitEngine, TransitKeyInfo};

use crate::raft_runtime::RaftRuntime;

/// Application service for transit encryption key operations.
///
/// Centralises Raft proposal logic and metrics so that both gRPC and HTTP
/// transport handlers are thin translation layers.
#[derive(Clone)]
pub struct TransitApp {
    transit: Arc<TransitEngine>,
    metrics: Arc<CoordMetrics>,
    raft: RaftRuntime,
}

impl TransitApp {
    pub fn new(transit: Arc<TransitEngine>, metrics: Arc<CoordMetrics>, raft: RaftRuntime) -> Self {
        Self {
            transit,
            metrics,
            raft,
        }
    }

    /// Reference to the underlying engine for read-only operations.
    pub fn engine(&self) -> &Arc<TransitEngine> {
        &self.transit
    }

    /// Reference to the metrics collector.
    pub fn metrics(&self) -> &Arc<CoordMetrics> {
        &self.metrics
    }

    /// Create a new transit encryption key (Raft-replicated).
    pub async fn create_key(&self, key_name: &str) -> Result<TransitKeyInfo, String> {
        let material_b64 = generate_key_material_b64();
        let payload = TransitEngine::encode_create_key_bytes(key_name, &material_b64);
        self.raft
            .propose_business_command("transit", payload)
            .await
            .map_err(|e| format!("raft propose failed: {e}"))?;

        self.transit
            .get_key_info(key_name)
            .await
            .map_err(|e| e.to_string())
    }

    /// Rotate an existing transit encryption key (Raft-replicated).
    pub async fn rotate_key(&self, key_name: &str) -> Result<TransitKeyInfo, String> {
        let current_info = self
            .transit
            .get_key_info(key_name)
            .await
            .map_err(|e| e.to_string())?;

        let new_version = current_info.primary_version + 1;
        let material_b64 = generate_key_material_b64();
        let payload = TransitEngine::encode_rotate_key_bytes(key_name, new_version, &material_b64);
        self.raft
            .propose_business_command("transit", payload)
            .await
            .map_err(|e| format!("raft propose failed: {e}"))?;

        self.metrics.coord_transit_key_rotation_total.inc();

        self.transit
            .get_key_info(key_name)
            .await
            .map_err(|e| e.to_string())
    }
}

fn generate_key_material_b64() -> String {
    let mut material = [0_u8; 32];
    OsRng.fill_bytes(&mut material);
    BASE64.encode(material)
}
