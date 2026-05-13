//! HTTP handlers: security (auth, seal, roles).

use std::collections::HashMap;

use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use serde::{Deserialize, Serialize};

use super::HttpApiState;
use super::audit::{record_operation_audit, record_risk_audit, require_risk_operation_capability};
use super::auth::{enforce_high_risk_rate_limit, extract_bearer_token, require_console_capability};
use super::console_roles::console_role_templates;
use super::error::ApiError;
use super::helpers::{capture_security_domain_snapshot, clear_runtime_security_domain};

// ── DTOs ─────────────────────────────────────────────────────────────────────

#[derive(Serialize)]
pub(super) struct SecurityStatusResponse {
    initialized: bool,
    sealed: bool,
    shares_total: u32,
    threshold: u32,
    progress: u32,
    token_valid: bool,
    token_role_id: String,
    token_policies: Vec<String>,
    token_expires_unix_seconds: i64,
}

#[derive(Deserialize)]
pub(super) struct SecurityLoginRequest {
    role_id: String,
    secret_id: String,
}

#[derive(Serialize)]
pub(super) struct SecurityLoginResponse {
    access_token: String,
    role_id: String,
    policies: Vec<String>,
    expires_unix_seconds: i64,
}

#[derive(Serialize)]
pub(super) struct SecuritySealResponse {
    sealed: bool,
    message: String,
}

#[derive(Serialize)]
pub(super) struct SecurityTokenResponse {
    valid: bool,
    role_id: String,
    policies: Vec<String>,
    expires_unix_seconds: i64,
}

#[derive(Serialize)]
pub(super) struct SecurityRoleItemResponse {
    role_id: String,
    role_name: String,
    policies: Vec<String>,
    token_ttl_seconds: i64,
    secret_id_ttl_seconds: i64,
    secret_id_num_uses: u32,
}

#[derive(Serialize)]
pub(super) struct SecurityRolesResponse {
    roles: Vec<SecurityRoleItemResponse>,
}

#[derive(Serialize)]
pub(super) struct SecurityBootstrapRoleResponse {
    role_id: String,
    role_name: String,
    policies: Vec<String>,
    token_ttl_seconds: i64,
    secret_id_ttl_seconds: i64,
    secret_id_num_uses: u32,
    created: bool,
}

#[derive(Serialize)]
pub(super) struct SecurityBootstrapConsoleRolesResponse {
    roles: Vec<SecurityBootstrapRoleResponse>,
    message: String,
}

// ── Handlers ─────────────────────────────────────────────────────────────────

pub(super) async fn security_status(
    State(app): State<HttpApiState>,
    headers: HeaderMap,
) -> Json<SecurityStatusResponse> {
    let status = app.state.security().seal_status().await;
    let token_lookup = if status.initialized && !status.sealed {
        if let Some(token) = extract_bearer_token(&headers) {
            app.state.security().lookup_token(&token).await
        } else {
            coord_core::security::TokenLookupResult {
                valid: false,
                role_id: String::new(),
                policies: Vec::new(),
                expires_unix_seconds: 0,
            }
        }
    } else {
        coord_core::security::TokenLookupResult {
            valid: false,
            role_id: String::new(),
            policies: Vec::new(),
            expires_unix_seconds: 0,
        }
    };

    Json(SecurityStatusResponse {
        initialized: status.initialized,
        sealed: status.sealed,
        shares_total: status.shares_total,
        threshold: status.threshold,
        progress: status.progress,
        token_valid: token_lookup.valid,
        token_role_id: token_lookup.role_id,
        token_policies: token_lookup.policies,
        token_expires_unix_seconds: token_lookup.expires_unix_seconds,
    })
}

pub(super) async fn security_login(
    State(app): State<HttpApiState>,
    headers: HeaderMap,
    Json(body): Json<SecurityLoginRequest>,
) -> Result<Json<SecurityLoginResponse>, ApiError> {
    enforce_high_risk_rate_limit(&app, &headers, "security.login")?;

    let token = match app
        .state
        .security()
        .login_approle(&body.role_id, &body.secret_id)
        .await
    {
        Ok(t) => t,
        Err(err) => {
            record_operation_audit(
                &app,
                "security.login",
                &body.role_id,
                "failed",
                err.to_string(),
            );
            return Err(ApiError::new(StatusCode::UNAUTHORIZED, err));
        }
    };

    app.state.metrics().coord_auth_approle_login_total.inc();
    record_operation_audit(&app, "security.login", &token.role_id, "succeeded", "");

    Ok(Json(SecurityLoginResponse {
        access_token: token.access_token,
        role_id: token.role_id,
        policies: token.policies,
        expires_unix_seconds: token.expires_unix_seconds,
    }))
}

pub(super) async fn security_seal(
    State(app): State<HttpApiState>,
    headers: HeaderMap,
) -> Result<Json<SecuritySealResponse>, ApiError> {
    enforce_high_risk_rate_limit(&app, &headers, "security.seal")?;
    let audit = require_risk_operation_capability(
        &app,
        &headers,
        "security.seal",
        "security.seal",
        "security-domain",
    )
    .await?;

    let pre_status = app.state.security().seal_status().await;
    let status = if pre_status.initialized && !pre_status.sealed {
        let domain = capture_security_domain_snapshot(&app.state).await;
        app.state
            .security()
            .seal_with_domain(domain)
            .await
            .map_err(|err| {
                record_risk_audit(&app, &audit, "failed", err.to_string());
                ApiError::new(StatusCode::PRECONDITION_FAILED, err)
            })?
    } else {
        app.state.security().seal().await.map_err(|err| {
            record_risk_audit(&app, &audit, "failed", err.to_string());
            ApiError::new(StatusCode::PRECONDITION_FAILED, err)
        })?
    };

    if status.sealed {
        clear_runtime_security_domain(&app.state)
            .await
            .inspect_err(|err| {
                record_risk_audit(&app, &audit, "failed", &err.message);
            })?;
    }

    app.state
        .metrics()
        .coord_security_sealed
        .set(if status.sealed { 1 } else { 0 });

    let message = if pre_status.sealed {
        "security domain already sealed".to_string()
    } else {
        "security domain sealed; runtime security state cleared".to_string()
    };

    record_risk_audit(&app, &audit, "succeeded", &message);

    Ok(Json(SecuritySealResponse {
        sealed: status.sealed,
        message,
    }))
}

pub(super) async fn security_token(
    State(app): State<HttpApiState>,
    headers: HeaderMap,
) -> Result<Json<SecurityTokenResponse>, ApiError> {
    require_console_capability(&app, &headers, "security.read").await?;

    let token = extract_bearer_token(&headers)
        .ok_or_else(|| ApiError::new(StatusCode::UNAUTHORIZED, "missing bearer token"))?;

    let lookup = app.state.security().lookup_token(&token).await;
    if !lookup.valid {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "invalid or expired token",
        ));
    }

    Ok(Json(SecurityTokenResponse {
        valid: lookup.valid,
        role_id: lookup.role_id,
        policies: lookup.policies,
        expires_unix_seconds: lookup.expires_unix_seconds,
    }))
}

pub(super) async fn security_roles(
    State(app): State<HttpApiState>,
    headers: HeaderMap,
) -> Result<Json<SecurityRolesResponse>, ApiError> {
    require_console_capability(&app, &headers, "security.read").await?;

    let auth = app.state.security().export_auth_state_snapshot().await;
    let mut roles = auth
        .roles
        .into_iter()
        .map(|role| SecurityRoleItemResponse {
            role_id: role.role_id,
            role_name: role.role_name,
            policies: role.policies,
            token_ttl_seconds: role.token_ttl_seconds,
            secret_id_ttl_seconds: role.secret_id_ttl_seconds,
            secret_id_num_uses: role.secret_id_num_uses,
        })
        .collect::<Vec<_>>();
    roles.sort_by(|left, right| left.role_id.cmp(&right.role_id));

    Ok(Json(SecurityRolesResponse { roles }))
}

pub(super) async fn security_bootstrap_console_roles(
    State(app): State<HttpApiState>,
    headers: HeaderMap,
) -> Result<Json<SecurityBootstrapConsoleRolesResponse>, ApiError> {
    require_console_capability(&app, &headers, "security.admin").await?;

    let existing_snapshot = app.state.security().export_auth_state_snapshot().await;
    let mut existing_by_name: HashMap<String, SecurityRoleItemResponse> = HashMap::new();
    for role in existing_snapshot.roles {
        if role.role_name.trim().is_empty() {
            continue;
        }
        existing_by_name.insert(
            role.role_name.clone(),
            SecurityRoleItemResponse {
                role_id: role.role_id,
                role_name: role.role_name,
                policies: role.policies,
                token_ttl_seconds: role.token_ttl_seconds,
                secret_id_ttl_seconds: role.secret_id_ttl_seconds,
                secret_id_num_uses: role.secret_id_num_uses,
            },
        );
    }

    let mut roles = Vec::new();
    let mut created = 0_u32;
    for template in console_role_templates() {
        if let Some(existing) = existing_by_name.get(template.role_name) {
            roles.push(SecurityBootstrapRoleResponse {
                role_id: existing.role_id.clone(),
                role_name: existing.role_name.clone(),
                policies: existing.policies.clone(),
                token_ttl_seconds: existing.token_ttl_seconds,
                secret_id_ttl_seconds: existing.secret_id_ttl_seconds,
                secret_id_num_uses: existing.secret_id_num_uses,
                created: false,
            });
            continue;
        }

        let created_role = app
            .state
            .security()
            .create_approle(
                template.role_name,
                template.policies.clone(),
                template.token_ttl_seconds,
                template.secret_id_ttl_seconds,
                template.secret_id_num_uses,
            )
            .await
            .map_err(|err| ApiError::new(StatusCode::PRECONDITION_FAILED, err))?;
        created = created.saturating_add(1);

        roles.push(SecurityBootstrapRoleResponse {
            role_id: created_role.role_id,
            role_name: created_role.role_name,
            policies: created_role.policies,
            token_ttl_seconds: created_role.token_ttl_seconds,
            secret_id_ttl_seconds: created_role.secret_id_ttl_seconds,
            secret_id_num_uses: created_role.secret_id_num_uses,
            created: true,
        });
    }

    let message = if created == 0 {
        "console role templates already exist".to_string()
    } else {
        format!("created {} console role template(s)", created)
    };

    Ok(Json(SecurityBootstrapConsoleRolesResponse {
        roles,
        message,
    }))
}
