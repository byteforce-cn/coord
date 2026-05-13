//! Risk-operation and general-operation audit sinks.
//!
//! Both audit kinds are intentionally best-effort: failure to persist a line
//! is logged via `tracing::warn` but never propagated to the handler caller,
//! so audit instrumentation can never turn a successful request into a 5xx.
//!
//! High-risk operations (seal/unseal/backup/restore/membership changes) are
//! gated by [`require_risk_operation_capability`] and logged into
//! `<data_dir>/audit/risk-ops.jsonl`. General mutations (config put, pki
//! issue, workflow start, …) are logged via [`record_operation_audit`] into
//! `<data_dir>/audit/ops.jsonl`.

use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use sha2::{Digest, Sha256};

use axum::http::{HeaderMap, StatusCode};
use serde::Serialize;
use tracing::warn;

use coord_core::clock::{Clock, SystemClock};

use super::HttpApiState;
use super::auth::{extract_bearer_token, require_console_session};
use super::error::ApiError;

pub(super) struct RiskAuditContext {
    pub(super) role_id: String,
    pub(super) capability: String,
    pub(super) operation: String,
    pub(super) target: String,
}

#[derive(Serialize)]
pub(super) struct RiskAuditArchiveEntry {
    pub(super) timestamp_unix_seconds: i64,
    pub(super) node_id: String,
    pub(super) role_id: String,
    pub(super) capability: String,
    pub(super) operation: String,
    pub(super) target: String,
    pub(super) outcome: String,
    pub(super) detail: String,
    /// SHA-256 (base64) of the immediately preceding JSONL line, or `null` for
    /// the first entry.  Forms a tamper-evident hash chain across the log.
    pub(super) prev_hash: Option<String>,
}

pub(super) async fn require_risk_operation_capability(
    app: &HttpApiState,
    headers: &HeaderMap,
    capability: &str,
    operation: &str,
    target: &str,
) -> Result<RiskAuditContext, ApiError> {
    require_console_session(app, headers).await?;

    let status = app.state.security().seal_status().await;
    if !status.initialized {
        return Ok(RiskAuditContext {
            role_id: "bootstrap".to_string(),
            capability: capability.to_string(),
            operation: operation.to_string(),
            target: target.to_string(),
        });
    }

    let token = extract_bearer_token(headers)
        .ok_or_else(|| ApiError::new(StatusCode::UNAUTHORIZED, "missing bearer token"))?;
    let lookup = app.state.security().lookup_token(&token).await;
    let role_id = if lookup.valid {
        lookup.role_id
    } else {
        "unknown".to_string()
    };

    let context = RiskAuditContext {
        role_id,
        capability: capability.to_string(),
        operation: operation.to_string(),
        target: target.to_string(),
    };

    match app
        .state
        .security()
        .authorize_token(&token, capability)
        .await
    {
        Ok(()) => Ok(context),
        Err(message) => {
            app.state.metrics().coord_authz_denied_total.inc();
            record_risk_audit(app, &context, "denied", message.to_string());
            Err(ApiError::new(StatusCode::FORBIDDEN, message))
        }
    }
}

pub(super) fn record_risk_audit(
    app: &HttpApiState,
    context: &RiskAuditContext,
    outcome: &str,
    detail: impl AsRef<str>,
) {
    let entry = RiskAuditArchiveEntry {
        timestamp_unix_seconds: SystemClock.now_seconds(),
        node_id: app.state.runtime().node_id.clone(),
        role_id: context.role_id.clone(),
        capability: context.capability.clone(),
        operation: context.operation.clone(),
        target: context.target.clone(),
        outcome: outcome.to_string(),
        detail: detail.as_ref().to_string(),
        prev_hash: None, // filled in by write_risk_audit_archive
    };

    if let Err(err) = write_risk_audit_archive(app, &entry) {
        warn!(
            node_id = %entry.node_id,
            operation = %entry.operation,
            target = %entry.target,
            outcome = %entry.outcome,
            error = %err,
            "failed to write risk audit archive"
        );
    }
}

fn write_risk_audit_archive(
    app: &HttpApiState,
    entry: &RiskAuditArchiveEntry,
) -> Result<(), String> {
    let path = app
        .state
        .runtime()
        .data_dir
        .join("audit")
        .join("risk-ops.jsonl");

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| {
            format!(
                "failed to create risk audit dir {}: {err}",
                parent.display()
            )
        })?;
    }

    // Compute the hash of the last line for the chain.
    let prev_hash = read_last_line_hash(&path);

    // Build a new entry struct with the chain hash filled in.
    let chained = RiskAuditArchiveEntry {
        prev_hash,
        timestamp_unix_seconds: entry.timestamp_unix_seconds,
        node_id: entry.node_id.clone(),
        role_id: entry.role_id.clone(),
        capability: entry.capability.clone(),
        operation: entry.operation.clone(),
        target: entry.target.clone(),
        outcome: entry.outcome.clone(),
        detail: entry.detail.clone(),
    };

    let line = serde_json::to_string(&chained)
        .map_err(|err| format!("failed to serialize risk audit entry: {err}"))?;

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|err| {
            format!(
                "failed to open risk audit archive {}: {err}",
                path.display()
            )
        })?;

    writeln!(file, "{line}").map_err(|err| {
        format!(
            "failed to append risk audit archive {}: {err}",
            path.display()
        )
    })?;

    Ok(())
}

/// Read the last non-empty line of `path` and return its SHA-256 (base64).
/// Returns `None` if the file does not exist, is empty, or cannot be read.
fn read_last_line_hash(path: &Path) -> Option<String> {
    let file = std::fs::File::open(path).ok()?;
    let reader = BufReader::new(file);
    let last_line = reader
        .lines()
        .map_while(Result::ok)
        .filter(|l| !l.trim().is_empty())
        .last()?;
    let digest = Sha256::digest(last_line.as_bytes());
    Some(BASE64.encode(digest))
}

#[derive(Debug, Serialize)]
pub(super) struct OperationAuditEntry {
    pub(super) timestamp_unix_seconds: i64,
    pub(super) node_id: String,
    pub(super) operation: String,
    pub(super) target: String,
    pub(super) outcome: String,
    pub(super) detail: String,
}

/// General-purpose operation audit for non-high-risk mutations (config put,
/// workflow start, transit key create, pki issue, login, …). Complements the
/// high-risk `risk-ops.jsonl` archive. Writes to `<data_dir>/audit/ops.jsonl`.
///
/// Failure to write is logged but never propagated to the caller — audit must
/// not block or fail a user operation. Callers invoke this at both success and
/// failure branches so denial events are recorded.
pub(super) fn record_operation_audit(
    app: &HttpApiState,
    operation: &str,
    target: &str,
    outcome: &str,
    detail: impl AsRef<str>,
) {
    let entry = OperationAuditEntry {
        timestamp_unix_seconds: SystemClock.now_seconds(),
        node_id: app.state.runtime().node_id.clone(),
        operation: operation.to_string(),
        target: target.to_string(),
        outcome: outcome.to_string(),
        detail: detail.as_ref().to_string(),
    };
    if let Err(err) = write_operation_audit(app, &entry) {
        warn!(
            operation = %entry.operation,
            target = %entry.target,
            outcome = %entry.outcome,
            error = %err,
            "failed to write operation audit archive"
        );
    }
}

fn write_operation_audit(app: &HttpApiState, entry: &OperationAuditEntry) -> Result<(), String> {
    append_operation_audit_line(&app.state.runtime().data_dir, entry)
}

/// File-backed sink for [`OperationAuditEntry`], factored out so unit tests
/// can exercise it without constructing a full `HttpApiState`.
pub(super) fn append_operation_audit_line(
    data_dir: &Path,
    entry: &OperationAuditEntry,
) -> Result<(), String> {
    let path = data_dir.join("audit").join("ops.jsonl");
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("failed to create ops audit dir {}: {err}", parent.display()))?;
    }
    let line = serde_json::to_string(entry)
        .map_err(|err| format!("failed to serialize ops audit entry: {err}"))?;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|err| format!("failed to open ops audit archive {}: {err}", path.display()))?;
    writeln!(file, "{line}").map_err(|err| {
        format!(
            "failed to append ops audit archive {}: {err}",
            path.display()
        )
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_audit(op: &str, target: &str, outcome: &str) -> OperationAuditEntry {
        OperationAuditEntry {
            timestamp_unix_seconds: 1_700_000_000,
            node_id: "node-test".into(),
            operation: op.into(),
            target: target.into(),
            outcome: outcome.into(),
            detail: "detail".into(),
        }
    }

    #[test]
    fn operation_audit_creates_jsonl_and_appends() {
        let dir = std::env::temp_dir().join(format!(
            "coord-audit-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        append_operation_audit_line(&dir, &sample_audit("config.put", "/foo", "succeeded"))
            .expect("first write");
        append_operation_audit_line(&dir, &sample_audit("pki.issue", "cn=test", "failed"))
            .expect("second write");

        let path = dir.join("audit").join("ops.jsonl");
        let content = std::fs::read_to_string(&path).expect("read jsonl");
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2, "each call appends one JSON line");

        let first: serde_json::Value = serde_json::from_str(lines[0]).expect("line 0 valid json");
        assert_eq!(first["operation"], "config.put");
        assert_eq!(first["target"], "/foo");
        assert_eq!(first["outcome"], "succeeded");
        assert_eq!(first["node_id"], "node-test");

        let second: serde_json::Value = serde_json::from_str(lines[1]).expect("line 1 valid json");
        assert_eq!(second["operation"], "pki.issue");
        assert_eq!(second["outcome"], "failed");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn operation_audit_tolerates_preexisting_directory() {
        let dir = std::env::temp_dir().join(format!(
            "coord-audit-pre-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(dir.join("audit")).unwrap();
        append_operation_audit_line(&dir, &sample_audit("security.login", "role-1", "succeeded"))
            .expect("write with pre-existing dir");
        let path = dir.join("audit").join("ops.jsonl");
        assert!(path.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
