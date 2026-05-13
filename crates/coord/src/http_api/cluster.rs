//! HTTP handlers: cluster status, membership, backup, overview.

use std::collections::HashSet;

use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use serde::{Deserialize, Serialize};

use super::HttpApiState;
use super::audit::{record_risk_audit, require_risk_operation_capability};
use super::auth::{enforce_high_risk_rate_limit, require_console_capability};
use super::error::ApiError;
use crate::persistence;

// ── DTOs ─────────────────────────────────────────────────────────────────────

#[derive(Serialize)]
pub(super) struct ClusterMemberResponse {
    node_id: String,
    address: String,
}

#[derive(Serialize)]
pub(super) struct ClusterStatusResponse {
    node_id: String,
    role: String,
    dev_mode: bool,
    members: Vec<ClusterMemberResponse>,
}

#[derive(Deserialize)]
pub(super) struct ClusterMemberAddRequest {
    node_id: String,
    address: String,
}

#[derive(Deserialize)]
pub(super) struct ClusterMemberRemoveRequest {
    node_id: String,
    #[serde(default)]
    force_unreachable: bool,
}

#[derive(Serialize)]
pub(super) struct ClusterMemberChangeResponse {
    changed: bool,
    members: Vec<String>,
    message: String,
}

#[derive(Serialize)]
pub(super) struct BackupCreateHttpResponse {
    payload_json: String,
    created_unix_ms: i64,
}

#[derive(Deserialize)]
pub(super) struct BackupRestoreHttpRequest {
    payload_json: String,
}

#[derive(Serialize)]
pub(super) struct BackupRestoreHttpResponse {
    restored: bool,
    message: String,
}

#[derive(Serialize)]
pub(super) struct OverviewResponse {
    node_id: String,
    role: String,
    dev_mode: bool,
    member_count: usize,
    service_count: usize,
    instance_count: usize,
    config_count: usize,
    lock_count: usize,
    transit_key_count: usize,
    pki_issued_count: usize,
    pki_revoked_count: usize,
    security_initialized: bool,
    security_sealed: bool,
}

// ── Handlers ─────────────────────────────────────────────────────────────────

pub(super) async fn cluster_status(
    State(app): State<HttpApiState>,
    headers: HeaderMap,
) -> Result<Json<ClusterStatusResponse>, ApiError> {
    require_console_capability(&app, &headers, "cluster.read").await?;

    let mut members = app
        .state
        .members()
        .read()
        .await
        .iter()
        .map(|(node_id, address)| ClusterMemberResponse {
            node_id: node_id.clone(),
            address: address.clone(),
        })
        .collect::<Vec<_>>();
    members.sort_by(|left, right| left.node_id.cmp(&right.node_id));

    Ok(Json(ClusterStatusResponse {
        node_id: app.state.runtime().node_id.clone(),
        role: app.raft.role_label().await,
        dev_mode: app.state.runtime().dev_mode,
        members,
    }))
}

pub(super) async fn cluster_member_add(
    State(app): State<HttpApiState>,
    headers: HeaderMap,
    Json(body): Json<ClusterMemberAddRequest>,
) -> Result<Json<ClusterMemberChangeResponse>, ApiError> {
    require_console_capability(&app, &headers, "cluster.member_add").await?;

    coord_core::validation::validate_key(&body.node_id)
        .map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, format!("node_id: {e}")))?;
    if body.address.trim().is_empty() || body.address.trim().len() != body.address.len() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "address must be non-empty and trimmed",
        ));
    }

    let (changed, members) = app
        .raft
        .propose_member_add(body.node_id.clone(), body.address.clone())
        .await
        .map_err(|err| ApiError::new(StatusCode::PRECONDITION_FAILED, err))?;

    Ok(Json(ClusterMemberChangeResponse {
        changed,
        members,
        message: if changed {
            format!("member {} added", body.node_id)
        } else {
            format!("member {} already exists", body.node_id)
        },
    }))
}

pub(super) async fn cluster_member_remove(
    State(app): State<HttpApiState>,
    headers: HeaderMap,
    Json(body): Json<ClusterMemberRemoveRequest>,
) -> Result<Json<ClusterMemberChangeResponse>, ApiError> {
    let target = body.node_id.trim().to_string();
    let audit = require_risk_operation_capability(
        &app,
        &headers,
        "cluster.member_remove",
        "cluster.member_remove",
        &target,
    )
    .await?;

    if target.is_empty() {
        record_risk_audit(&app, &audit, "failed", "node_id is required");
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "node_id is required",
        ));
    }

    let (changed, members) = app
        .raft
        .propose_member_remove(body.node_id.clone(), body.force_unreachable)
        .await
        .map_err(|err| {
            record_risk_audit(&app, &audit, "failed", &err);
            ApiError::new(StatusCode::PRECONDITION_FAILED, err)
        })?;

    let message = if changed {
        format!("member {} removed", body.node_id)
    } else {
        format!("member {} not found", body.node_id)
    };
    record_risk_audit(&app, &audit, "succeeded", &message);

    Ok(Json(ClusterMemberChangeResponse {
        changed,
        members,
        message,
    }))
}

pub(super) async fn backup_create(
    State(app): State<HttpApiState>,
    headers: HeaderMap,
) -> Result<Json<BackupCreateHttpResponse>, ApiError> {
    enforce_high_risk_rate_limit(&app, &headers, "admin.backup.create")?;
    require_console_capability(&app, &headers, "admin.backup").await?;

    let payload = app
        .raft
        .snapshot_backup_payload()
        .await
        .map_err(|err| ApiError::new(StatusCode::PRECONDITION_FAILED, err))?;

    let payload_json = persistence::payload_to_json_v5(&payload)
        .map_err(|err| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

    Ok(Json(BackupCreateHttpResponse {
        payload_json,
        created_unix_ms: payload.created_unix_ms,
    }))
}

pub(super) async fn backup_restore(
    State(app): State<HttpApiState>,
    headers: HeaderMap,
    Json(body): Json<BackupRestoreHttpRequest>,
) -> Result<Json<BackupRestoreHttpResponse>, ApiError> {
    enforce_high_risk_rate_limit(&app, &headers, "admin.backup.restore")?;
    let audit = require_risk_operation_capability(
        &app,
        &headers,
        "admin.backup",
        "admin.backup.restore",
        "runtime-snapshot",
    )
    .await?;

    if body.payload_json.trim().is_empty() {
        record_risk_audit(&app, &audit, "failed", "payload_json cannot be empty");
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "payload_json cannot be empty",
        ));
    }

    let message = app
        .raft
        .propose_backup_restore(body.payload_json)
        .await
        .map_err(|err| {
            record_risk_audit(&app, &audit, "failed", &err);
            ApiError::new(StatusCode::PRECONDITION_FAILED, err)
        })?;

    record_risk_audit(&app, &audit, "succeeded", &message);

    Ok(Json(BackupRestoreHttpResponse {
        restored: true,
        message,
    }))
}

pub(super) async fn overview(
    State(app): State<HttpApiState>,
    headers: HeaderMap,
) -> Result<Json<OverviewResponse>, ApiError> {
    require_console_capability(&app, &headers, "overview.read").await?;

    let services = app.state.registry().snapshot().await;
    let configs = app.state.config().snapshot().await;
    let locks = app.state.locks().list_holders().await;
    let transit = app.state.transit().snapshot().await;
    let pki = app.state.pki().snapshot().await;
    let members = app.state.members().read().await.len();
    let seal_status = app.state.security().seal_status().await;

    Ok(Json(OverviewResponse {
        node_id: app.state.runtime().node_id.clone(),
        role: app.raft.role_label().await,
        dev_mode: app.state.runtime().dev_mode,
        member_count: members,
        service_count: services
            .iter()
            .map(|snapshot| snapshot.instance.service_name.clone())
            .collect::<HashSet<_>>()
            .len(),
        instance_count: services.len(),
        config_count: configs.len(),
        lock_count: locks.len(),
        transit_key_count: transit.len(),
        pki_issued_count: pki.issued.len(),
        pki_revoked_count: pki.revocations.len(),
        security_initialized: seal_status.initialized,
        security_sealed: seal_status.sealed,
    }))
}
