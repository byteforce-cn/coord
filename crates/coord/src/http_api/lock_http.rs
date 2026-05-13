//! HTTP handlers: distributed locks.

use axum::Json;
use axum::extract::State;
use axum::http::HeaderMap;
use serde::Serialize;

use super::HttpApiState;
use super::auth::require_console_capability;
use super::error::ApiError;

#[derive(Serialize)]
pub(super) struct LockItemResponse {
    lock_name: String,
    owner: String,
    expires_unix_ms: i64,
}

#[derive(Serialize)]
pub(super) struct LocksResponse {
    locks: Vec<LockItemResponse>,
}

pub(super) async fn locks(
    State(app): State<HttpApiState>,
    headers: HeaderMap,
) -> Result<Json<LocksResponse>, ApiError> {
    require_console_capability(&app, &headers, "lock.read").await?;

    let mut holders = app.state.locks().list_holders().await;
    holders.sort_by(|left, right| left.lock_name.cmp(&right.lock_name));

    let locks = holders
        .into_iter()
        .map(|entry| LockItemResponse {
            lock_name: entry.lock_name,
            owner: entry.owner,
            expires_unix_ms: entry.expires_unix_ms,
        })
        .collect();

    Ok(Json(LocksResponse { locks }))
}
