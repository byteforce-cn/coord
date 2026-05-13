//! Cross-cutting helpers shared by multiple handler families.
//!
//! - `capture_security_domain_snapshot`: snapshot transit + pki + auth state
//!   into the `SecurityDomainSnapshot` that the seal/unseal path wraps under
//!   the barrier key.
//! - `clear_runtime_security_domain`: reset transit + pki to empty /
//!   freshly-initialized state, used as part of seal and on first init.
//! - `fresh_pki_snapshot`: emit a baseline snapshot of a new `PkiEngine` so
//!   callers can restore to a known-empty state.

use std::sync::Arc;

use axum::http::StatusCode;

use coord_core::clock::{Clock, SystemClock};
use coord_core::pki::PkiEngine;
use coord_core::security::SecurityDomainSnapshot;
use coord_core::state::CoordinatorState;

use super::error::ApiError;

pub(super) async fn capture_security_domain_snapshot(
    state: &CoordinatorState,
) -> SecurityDomainSnapshot {
    SecurityDomainSnapshot {
        transit_keys: state.transit().snapshot().await,
        pki: Some(state.pki().snapshot().await),
        auth: state.security().export_auth_state_snapshot().await,
    }
}

pub(super) async fn clear_runtime_security_domain(
    state: &CoordinatorState,
) -> Result<(), ApiError> {
    state
        .transit()
        .restore(Vec::new())
        .await
        .map_err(|err| ApiError::new(StatusCode::PRECONDITION_FAILED, err))?;

    state
        .pki()
        .restore(fresh_pki_snapshot().await?)
        .await
        .map_err(|err| ApiError::new(StatusCode::PRECONDITION_FAILED, err.to_string()))?;

    Ok(())
}

pub(super) async fn fresh_pki_snapshot() -> Result<coord_core::pki::PkiStateSnapshot, ApiError> {
    let clock = Arc::new(SystemClock) as Arc<dyn Clock>;
    let pki = PkiEngine::new(clock)
        .map_err(|err| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    Ok(pki.snapshot().await)
}
