//! HTTP handlers: transit encryption keys.

use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use serde::{Deserialize, Serialize};

use coord_core::validation::validate_key;

use super::HttpApiState;
use super::audit::{record_operation_audit, record_risk_audit, require_risk_operation_capability};
use super::auth::require_console_capability;
use super::error::ApiError;

#[derive(Serialize)]
pub(super) struct TransitKeyResponse {
    key_name: String,
    primary_version: u32,
    total_versions: usize,
}

#[derive(Serialize)]
pub(super) struct TransitKeysResponse {
    keys: Vec<TransitKeyResponse>,
}

#[derive(Deserialize)]
pub(super) struct TransitKeyWriteRequest {
    key_name: String,
}

#[derive(Serialize)]
pub(super) struct TransitKeyWriteResponse {
    key_name: String,
    primary_version: u32,
    total_versions: u32,
}

pub(super) async fn transit_keys(
    State(app): State<HttpApiState>,
    headers: HeaderMap,
) -> Result<Json<TransitKeysResponse>, ApiError> {
    require_console_capability(&app, &headers, "transit.read").await?;

    let snapshot = app.state.transit().snapshot().await;
    let mut keys = snapshot
        .into_iter()
        .map(|item| TransitKeyResponse {
            key_name: item.key_name,
            primary_version: item.primary_version,
            total_versions: item.versions.len(),
        })
        .collect::<Vec<_>>();
    keys.sort_by(|left, right| left.key_name.cmp(&right.key_name));

    Ok(Json(TransitKeysResponse { keys }))
}

pub(super) async fn transit_key_create(
    State(app): State<HttpApiState>,
    headers: HeaderMap,
    Json(body): Json<TransitKeyWriteRequest>,
) -> Result<Json<TransitKeyWriteResponse>, ApiError> {
    require_console_capability(&app, &headers, "transit.admin").await?;

    if let Err(e) = validate_key(&body.key_name) {
        record_operation_audit(
            &app,
            "transit.key.create",
            &body.key_name,
            "failed",
            format!("key_name: {e}"),
        );
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            format!("key_name: {e}"),
        ));
    }

    let result = match app.transit_app.create_key(&body.key_name).await {
        Ok(r) => r,
        Err(err) => {
            record_operation_audit(&app, "transit.key.create", &body.key_name, "failed", &err);
            return Err(ApiError::new(StatusCode::BAD_REQUEST, err));
        }
    };
    record_operation_audit(
        &app,
        "transit.key.create",
        &result.key_name,
        "succeeded",
        format!("primary_version={}", result.primary_version),
    );

    Ok(Json(TransitKeyWriteResponse {
        key_name: result.key_name,
        primary_version: result.primary_version,
        total_versions: result.total_versions,
    }))
}

pub(super) async fn transit_key_rotate(
    State(app): State<HttpApiState>,
    headers: HeaderMap,
    Json(body): Json<TransitKeyWriteRequest>,
) -> Result<Json<TransitKeyWriteResponse>, ApiError> {
    let target = body.key_name.trim().to_string();
    let audit = require_risk_operation_capability(
        &app,
        &headers,
        "transit.admin",
        "transit.key.rotate",
        &target,
    )
    .await?;

    let result = app
        .transit_app
        .rotate_key(&body.key_name)
        .await
        .map_err(|err| {
            record_risk_audit(&app, &audit, "failed", &err);
            ApiError::new(StatusCode::BAD_REQUEST, err)
        })?;

    record_risk_audit(
        &app,
        &audit,
        "succeeded",
        format!(
            "key {} rotated to primary version {}",
            result.key_name, result.primary_version
        ),
    );

    Ok(Json(TransitKeyWriteResponse {
        key_name: result.key_name,
        primary_version: result.primary_version,
        total_versions: result.total_versions,
    }))
}
