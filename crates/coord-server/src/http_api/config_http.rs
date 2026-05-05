//! HTTP handlers: config key-value store.

use axum::Json;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use serde::{Deserialize, Serialize};

use coord_core::validation::validate_key;

use super::HttpApiState;
use super::audit::record_operation_audit;
use super::auth::require_console_capability;
use super::error::ApiError;

#[derive(Deserialize)]
pub(super) struct ConfigQuery {
    prefix: Option<String>,
}

#[derive(Serialize)]
pub(super) struct ConfigItemResponse {
    key: String,
    value: String,
    version: i64,
}

#[derive(Serialize)]
pub(super) struct ConfigsResponse {
    configs: Vec<ConfigItemResponse>,
}

#[derive(Deserialize)]
pub(super) struct ConfigPutHttpRequest {
    key: String,
    value: String,
}

#[derive(Serialize)]
pub(super) struct ConfigPutHttpResponse {
    key: String,
    value: String,
    version: i64,
}

pub(super) async fn configs(
    State(app): State<HttpApiState>,
    Query(query): Query<ConfigQuery>,
    headers: HeaderMap,
) -> Result<Json<ConfigsResponse>, ApiError> {
    require_console_capability(&app, &headers, "config.read").await?;

    let mut snapshot = app.state.config().snapshot().await;
    if let Some(prefix) = query.prefix {
        snapshot.retain(|entry| entry.key.starts_with(&prefix));
    }

    let configs = snapshot
        .into_iter()
        .map(|entry| ConfigItemResponse {
            key: entry.key,
            value: entry.value,
            version: entry.version,
        })
        .collect();

    Ok(Json(ConfigsResponse { configs }))
}

pub(super) async fn configs_put(
    State(app): State<HttpApiState>,
    headers: HeaderMap,
    Json(body): Json<ConfigPutHttpRequest>,
) -> Result<Json<ConfigPutHttpResponse>, ApiError> {
    require_console_capability(&app, &headers, "config.put").await?;

    if let Err(e) = validate_key(&body.key) {
        record_operation_audit(&app, "config.put", &body.key, "failed", e.to_string());
        return Err(ApiError::new(StatusCode::BAD_REQUEST, e.to_string()));
    }

    let entry = app
        .config_app
        .put(body.key, body.value)
        .await
        .map_err(|e| {
            record_operation_audit(&app, "config.put", "", "failed", &e);
            ApiError::new(StatusCode::BAD_REQUEST, e)
        })?;
    record_operation_audit(
        &app,
        "config.put",
        &entry.key,
        "succeeded",
        format!("version={}", entry.version),
    );
    Ok(Json(ConfigPutHttpResponse {
        key: entry.key,
        value: entry.value,
        version: entry.version,
    }))
}
