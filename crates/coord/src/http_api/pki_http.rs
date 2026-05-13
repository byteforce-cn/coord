//! HTTP handlers: PKI certificate management.

use std::collections::HashMap;

use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use serde::{Deserialize, Serialize};

use coord_core::pki::CertificateIssueOptions;
use coord_core::validation::validate_key;

use super::HttpApiState;
use super::audit::{record_operation_audit, record_risk_audit, require_risk_operation_capability};
use super::auth::require_console_capability;
use super::error::ApiError;

// ── DTOs ─────────────────────────────────────────────────────────────────────

#[derive(Serialize)]
pub(super) struct CertificateItemResponse {
    serial_number: String,
    common_name: String,
    sans: Vec<String>,
    not_after_unix_seconds: i64,
    revoked: bool,
    revoked_reason: String,
    revoked_at_unix_seconds: i64,
    auto_renew_enabled: bool,
    renew_before_seconds: i64,
    managed_by_acme: bool,
}

#[derive(Serialize)]
pub(super) struct CertificatesResponse {
    certificates: Vec<CertificateItemResponse>,
}

#[derive(Deserialize)]
pub(super) struct PkiIssueHttpRequest {
    common_name: String,
    #[serde(default)]
    sans: Vec<String>,
    #[serde(default)]
    ttl_seconds: i64,
    #[serde(default)]
    auto_renew: bool,
    #[serde(default)]
    renew_before_seconds: i64,
}

#[derive(Serialize)]
pub(super) struct PkiIssueHttpResponse {
    serial_number: String,
    common_name: String,
    sans: Vec<String>,
    not_after_unix_seconds: i64,
    auto_renew: bool,
    renew_before_seconds: i64,
}

#[derive(Deserialize)]
pub(super) struct PkiRenewHttpRequest {
    serial_number: String,
    #[serde(default)]
    ttl_seconds: i64,
}

#[derive(Serialize)]
pub(super) struct PkiRenewHttpResponse {
    old_serial_number: String,
    new_serial_number: String,
    common_name: String,
    sans: Vec<String>,
    not_after_unix_seconds: i64,
    auto_renew: bool,
    renew_before_seconds: i64,
}

#[derive(Deserialize)]
pub(super) struct PkiStatusHttpRequest {
    serial_number: String,
}

#[derive(Serialize)]
pub(super) struct PkiStatusHttpResponse {
    status: String,
    reason: String,
    revoked_at_unix_seconds: i64,
    not_after_unix_seconds: i64,
    auto_renew: bool,
    renew_before_seconds: i64,
}

#[derive(Deserialize)]
pub(super) struct PkiRevokeHttpRequest {
    serial_number: String,
    #[serde(default)]
    reason: String,
}

#[derive(Serialize)]
pub(super) struct PkiRevokeHttpResponse {
    revoked: bool,
    serial_number: String,
    reason: String,
    message: String,
}

#[derive(Deserialize)]
pub(super) struct PkiAutoRenewPolicyUpdateHttpRequest {
    serial_number: String,
    enabled: bool,
    #[serde(default)]
    renew_before_seconds: i64,
}

#[derive(Serialize)]
pub(super) struct PkiAutoRenewPolicyUpdateHttpResponse {
    updated: bool,
    auto_renew: bool,
    renew_before_seconds: i64,
    not_after_unix_seconds: i64,
}

#[derive(Serialize)]
pub(super) struct PkiAutoRenewedCertificateHttpResponse {
    old_serial_number: String,
    new_serial_number: String,
    common_name: String,
    not_after_unix_seconds: i64,
}

#[derive(Serialize)]
pub(super) struct PkiAutoRenewRunHttpResponse {
    renewed_count: u32,
    renewed: Vec<PkiAutoRenewedCertificateHttpResponse>,
    errors: Vec<String>,
}

#[derive(Serialize)]
pub(super) struct PkiAcmeChallengeHttpResponse {
    domain: String,
    challenge_type: String,
    token: String,
    validated: bool,
}

#[derive(Deserialize)]
pub(super) struct PkiAcmeOrderCreateHttpRequest {
    #[serde(default)]
    domains: Vec<String>,
    #[serde(default)]
    ttl_seconds: i64,
    #[serde(default)]
    challenge_type: String,
    #[serde(default)]
    auto_renew: bool,
    #[serde(default)]
    renew_before_seconds: i64,
}

#[derive(Serialize)]
pub(super) struct PkiAcmeOrderCreateHttpResponse {
    order_id: String,
    status: String,
    challenges: Vec<PkiAcmeChallengeHttpResponse>,
    expires_unix_seconds: i64,
}

#[derive(Deserialize)]
pub(super) struct PkiAcmeChallengeCompleteHttpRequest {
    order_id: String,
    domain: String,
    token: String,
}

#[derive(Serialize)]
pub(super) struct PkiAcmeChallengeCompleteHttpResponse {
    order_id: String,
    status: String,
    challenges: Vec<PkiAcmeChallengeHttpResponse>,
}

#[derive(Deserialize)]
pub(super) struct PkiAcmeFinalizeHttpRequest {
    order_id: String,
    #[serde(default)]
    common_name: String,
}

#[derive(Serialize)]
pub(super) struct PkiAcmeFinalizeHttpResponse {
    order_id: String,
    status: String,
    serial_number: String,
    common_name: String,
    sans: Vec<String>,
    not_after_unix_seconds: i64,
    auto_renew: bool,
    renew_before_seconds: i64,
}

// ── Handlers ─────────────────────────────────────────────────────────────────

pub(super) async fn pki_certificates(
    State(app): State<HttpApiState>,
    headers: HeaderMap,
) -> Result<Json<CertificatesResponse>, ApiError> {
    require_console_capability(&app, &headers, "pki.read").await?;

    let snapshot = app.state.pki().snapshot().await;
    let revocations = snapshot
        .revocations
        .into_iter()
        .map(|record| (record.serial_number.clone(), record))
        .collect::<HashMap<_, _>>();

    let mut certificates = snapshot
        .issued
        .into_iter()
        .map(|item| {
            let revoked = revocations.get(&item.serial_number);
            CertificateItemResponse {
                serial_number: item.serial_number,
                common_name: item.common_name,
                sans: item.sans,
                not_after_unix_seconds: item.not_after_unix_seconds,
                revoked: revoked.is_some(),
                revoked_reason: revoked
                    .map(|value| value.reason.clone())
                    .unwrap_or_default(),
                revoked_at_unix_seconds: revoked
                    .map(|value| value.revoked_at_unix_seconds)
                    .unwrap_or_default(),
                auto_renew_enabled: item.auto_renew_enabled,
                renew_before_seconds: item.renew_before_seconds,
                managed_by_acme: item.managed_by_acme,
            }
        })
        .collect::<Vec<_>>();
    certificates.sort_by(|left, right| left.serial_number.cmp(&right.serial_number));

    Ok(Json(CertificatesResponse { certificates }))
}

pub(super) async fn pki_issue(
    State(app): State<HttpApiState>,
    headers: HeaderMap,
    Json(body): Json<PkiIssueHttpRequest>,
) -> Result<Json<PkiIssueHttpResponse>, ApiError> {
    require_console_capability(&app, &headers, "pki.issue").await?;

    if let Err(e) = validate_key(&body.common_name) {
        record_operation_audit(
            &app,
            "pki.issue",
            &body.common_name,
            "failed",
            format!("common_name: {e}"),
        );
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            format!("common_name: {e}"),
        ));
    }
    for (idx, san) in body.sans.iter().enumerate() {
        if let Err(e) = validate_key(san) {
            record_operation_audit(
                &app,
                "pki.issue",
                &body.common_name,
                "failed",
                format!("sans[{idx}]: {e}"),
            );
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                format!("sans[{idx}]: {e}"),
            ));
        }
    }

    let cn_for_audit = body.common_name.clone();
    let issued = match app
        .pki_app
        .issue_certificate(
            &body.common_name,
            body.sans,
            CertificateIssueOptions {
                ttl_seconds: body.ttl_seconds.max(60),
                auto_renew_enabled: body.auto_renew,
                renew_before_seconds: body.renew_before_seconds,
                managed_by_acme: false,
            },
        )
        .await
    {
        Ok(cert) => cert,
        Err(err) => {
            record_operation_audit(&app, "pki.issue", &cn_for_audit, "failed", &err);
            return Err(ApiError::new(StatusCode::BAD_REQUEST, err));
        }
    };

    record_operation_audit(
        &app,
        "pki.issue",
        &issued.common_name,
        "succeeded",
        format!("serial={}", issued.serial_number),
    );

    Ok(Json(PkiIssueHttpResponse {
        serial_number: issued.serial_number,
        common_name: issued.common_name,
        sans: issued.sans,
        not_after_unix_seconds: issued.not_after_unix_seconds,
        auto_renew: issued.auto_renew_enabled,
        renew_before_seconds: issued.renew_before_seconds,
    }))
}

pub(super) async fn pki_renew(
    State(app): State<HttpApiState>,
    headers: HeaderMap,
    Json(body): Json<PkiRenewHttpRequest>,
) -> Result<Json<PkiRenewHttpResponse>, ApiError> {
    require_console_capability(&app, &headers, "pki.renew").await?;

    let serial_number = body.serial_number.trim().to_string();
    if serial_number.is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "serial_number cannot be empty",
        ));
    }

    let renewed = app
        .pki_app
        .renew_certificate(&serial_number, body.ttl_seconds.max(60))
        .await
        .map_err(|err| ApiError::new(StatusCode::BAD_REQUEST, err))?;

    Ok(Json(PkiRenewHttpResponse {
        old_serial_number: serial_number,
        new_serial_number: renewed.serial_number,
        common_name: renewed.common_name,
        sans: renewed.sans,
        not_after_unix_seconds: renewed.not_after_unix_seconds,
        auto_renew: renewed.auto_renew_enabled,
        renew_before_seconds: renewed.renew_before_seconds,
    }))
}

pub(super) async fn pki_status(
    State(app): State<HttpApiState>,
    headers: HeaderMap,
    Json(body): Json<PkiStatusHttpRequest>,
) -> Result<Json<PkiStatusHttpResponse>, ApiError> {
    require_console_capability(&app, &headers, "pki.read").await?;

    if body.serial_number.trim().is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "serial_number cannot be empty",
        ));
    }

    app.state.metrics().coord_pki_ocsp_queries_total.inc();

    let report = app
        .state
        .pki()
        .check_certificate_status(&body.serial_number)
        .await;

    Ok(Json(PkiStatusHttpResponse {
        status: report.status.as_str().to_string(),
        reason: report.reason,
        revoked_at_unix_seconds: report.revoked_at_unix_seconds,
        not_after_unix_seconds: report.not_after_unix_seconds,
        auto_renew: report.auto_renew_enabled,
        renew_before_seconds: report.renew_before_seconds,
    }))
}

pub(super) async fn pki_revoke(
    State(app): State<HttpApiState>,
    headers: HeaderMap,
    Json(body): Json<PkiRevokeHttpRequest>,
) -> Result<Json<PkiRevokeHttpResponse>, ApiError> {
    let target = body.serial_number.trim().to_string();
    let audit = require_risk_operation_capability(
        &app,
        &headers,
        "pki.revoke",
        "pki.certificate.revoke",
        &target,
    )
    .await?;

    let serial_number = body.serial_number.trim().to_string();
    if serial_number.is_empty() {
        record_risk_audit(&app, &audit, "failed", "serial_number cannot be empty");
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "serial_number cannot be empty",
        ));
    }

    let reason = if body.reason.trim().is_empty() {
        "unspecified".to_string()
    } else {
        body.reason.trim().to_string()
    };

    let revoked = app
        .pki_app
        .revoke_certificate(&serial_number, &reason)
        .await
        .map_err(|err| {
            record_risk_audit(&app, &audit, "failed", &err);
            ApiError::new(StatusCode::BAD_REQUEST, err)
        })?;

    record_risk_audit(
        &app,
        &audit,
        if revoked { "succeeded" } else { "noop" },
        if revoked {
            format!("certificate {serial_number} revoked")
        } else {
            format!("certificate {serial_number} not found")
        },
    );

    Ok(Json(PkiRevokeHttpResponse {
        revoked,
        serial_number: serial_number.clone(),
        reason,
        message: if revoked {
            format!("certificate {serial_number} revoked")
        } else {
            format!("certificate {serial_number} not found")
        },
    }))
}

pub(super) async fn pki_auto_renew_policy_update(
    State(app): State<HttpApiState>,
    headers: HeaderMap,
    Json(body): Json<PkiAutoRenewPolicyUpdateHttpRequest>,
) -> Result<Json<PkiAutoRenewPolicyUpdateHttpResponse>, ApiError> {
    require_console_capability(&app, &headers, "pki.admin").await?;

    if body.serial_number.trim().is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "serial_number cannot be empty",
        ));
    }

    let updated = app
        .pki_app
        .update_auto_renew_policy(&body.serial_number, body.enabled, body.renew_before_seconds)
        .await
        .map_err(|err| ApiError::new(StatusCode::BAD_REQUEST, err))?;

    Ok(Json(PkiAutoRenewPolicyUpdateHttpResponse {
        updated: updated.updated,
        auto_renew: updated.auto_renew_enabled,
        renew_before_seconds: updated.renew_before_seconds,
        not_after_unix_seconds: updated.not_after_unix_seconds,
    }))
}

pub(super) async fn pki_auto_renew_run(
    State(app): State<HttpApiState>,
    headers: HeaderMap,
) -> Result<Json<PkiAutoRenewRunHttpResponse>, ApiError> {
    require_console_capability(&app, &headers, "pki.admin").await?;

    let execution = app.pki_app.run_auto_renew().await;

    Ok(Json(PkiAutoRenewRunHttpResponse {
        renewed_count: execution.renewed.len() as u32,
        renewed: execution
            .renewed
            .into_iter()
            .map(|item| PkiAutoRenewedCertificateHttpResponse {
                old_serial_number: item.old_serial_number,
                new_serial_number: item.new_serial_number,
                common_name: item.common_name,
                not_after_unix_seconds: item.not_after_unix_seconds,
            })
            .collect(),
        errors: execution.errors,
    }))
}

pub(super) async fn pki_acme_order_create(
    State(app): State<HttpApiState>,
    headers: HeaderMap,
    Json(body): Json<PkiAcmeOrderCreateHttpRequest>,
) -> Result<Json<PkiAcmeOrderCreateHttpResponse>, ApiError> {
    require_console_capability(&app, &headers, "pki.issue").await?;

    let order = app
        .pki_app
        .create_acme_order(
            body.domains,
            body.ttl_seconds.max(60),
            &body.challenge_type,
            body.auto_renew,
            body.renew_before_seconds,
        )
        .await
        .map_err(|err| ApiError::new(StatusCode::BAD_REQUEST, err))?;

    Ok(Json(PkiAcmeOrderCreateHttpResponse {
        order_id: order.order_id,
        status: order.status,
        challenges: to_pki_acme_challenges(order.challenges),
        expires_unix_seconds: order.expires_unix_seconds,
    }))
}

pub(super) async fn pki_acme_challenge_complete(
    State(app): State<HttpApiState>,
    headers: HeaderMap,
    Json(body): Json<PkiAcmeChallengeCompleteHttpRequest>,
) -> Result<Json<PkiAcmeChallengeCompleteHttpResponse>, ApiError> {
    require_console_capability(&app, &headers, "pki.issue").await?;

    let order = app
        .pki_app
        .complete_acme_challenge(&body.order_id, &body.domain, &body.token)
        .await
        .map_err(|err| ApiError::new(StatusCode::BAD_REQUEST, err))?;

    Ok(Json(PkiAcmeChallengeCompleteHttpResponse {
        order_id: order.order_id,
        status: order.status,
        challenges: to_pki_acme_challenges(order.challenges),
    }))
}

pub(super) async fn pki_acme_finalize(
    State(app): State<HttpApiState>,
    headers: HeaderMap,
    Json(body): Json<PkiAcmeFinalizeHttpRequest>,
) -> Result<Json<PkiAcmeFinalizeHttpResponse>, ApiError> {
    require_console_capability(&app, &headers, "pki.issue").await?;

    let finalized = app
        .pki_app
        .finalize_acme_order(&body.order_id, &body.common_name)
        .await
        .map_err(|err| ApiError::new(StatusCode::PRECONDITION_FAILED, err))?;

    let certificate = finalized.certificate;

    Ok(Json(PkiAcmeFinalizeHttpResponse {
        order_id: finalized.order_id,
        status: finalized.status,
        serial_number: certificate.serial_number,
        common_name: certificate.common_name,
        sans: certificate.sans,
        not_after_unix_seconds: certificate.not_after_unix_seconds,
        auto_renew: certificate.auto_renew_enabled,
        renew_before_seconds: certificate.renew_before_seconds,
    }))
}

fn to_pki_acme_challenges(
    challenges: Vec<coord_core::pki::AcmeChallenge>,
) -> Vec<PkiAcmeChallengeHttpResponse> {
    challenges
        .into_iter()
        .map(|item| PkiAcmeChallengeHttpResponse {
            domain: item.domain,
            challenge_type: item.challenge_type,
            token: item.token,
            validated: item.validated,
        })
        .collect()
}
