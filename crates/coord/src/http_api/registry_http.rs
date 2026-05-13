//! HTTP handlers: service registry.

use std::collections::BTreeMap;
use std::collections::HashMap;

use axum::Json;
use axum::extract::State;
use axum::http::HeaderMap;
use serde::Serialize;

use super::HttpApiState;
use super::auth::require_console_capability;
use super::error::ApiError;

#[derive(Serialize)]
pub(super) struct ServiceInstanceResponse {
    instance_id: String,
    host: String,
    port: u32,
    lease_id: String,
    expires_unix_ms: i64,
    metadata: HashMap<String, String>,
}

#[derive(Serialize)]
pub(super) struct ServiceGroupResponse {
    service_name: String,
    instances: Vec<ServiceInstanceResponse>,
}

#[derive(Serialize)]
pub(super) struct ServicesResponse {
    services: Vec<ServiceGroupResponse>,
}

pub(super) async fn services(
    State(app): State<HttpApiState>,
    headers: HeaderMap,
) -> Result<Json<ServicesResponse>, ApiError> {
    require_console_capability(&app, &headers, "registry.read").await?;

    let snapshot = app.state.registry().snapshot().await;
    let mut grouped: BTreeMap<String, Vec<ServiceInstanceResponse>> = BTreeMap::new();

    for item in snapshot {
        grouped
            .entry(item.instance.service_name)
            .or_default()
            .push(ServiceInstanceResponse {
                instance_id: item.instance.instance_id,
                host: item.instance.host,
                port: item.instance.port,
                lease_id: item.lease.lease_id,
                expires_unix_ms: item.lease.expires_unix_ms,
                metadata: item.instance.metadata,
            });
    }

    let services = grouped
        .into_iter()
        .map(|(service_name, mut instances)| {
            instances.sort_by(|left, right| left.instance_id.cmp(&right.instance_id));
            ServiceGroupResponse {
                service_name,
                instances,
            }
        })
        .collect();

    Ok(Json(ServicesResponse { services }))
}
