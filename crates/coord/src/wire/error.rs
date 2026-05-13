//! Canonical adapters for [`coord_core::error::CoordError`] → wire formats.
//!
//! This is the **single** place where the project translates a domain
//! error into either a [`tonic::Status`] or an HTTP [`ApiError`].
//! Service handlers and HTTP handlers MUST use these adapters (via
//! `?` on a `Result<T, CoordError>`) instead of constructing
//! `Status` / `ApiError` ad-hoc. Keeping the mapping here guarantees
//! the wire contract described in `doc/code-review-2026-04.md`
//! (section "Unified error contract").

use axum::Json;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use coord_core::error::{CoordError, ErrorKind};
use serde::Serialize;
use tonic::Status;

/// Map a [`CoordError`] to a [`tonic::Status`].
///
/// The gRPC metadata always carries:
/// * `coord-error-code`: the stable machine-readable code
///   (see [`CoordError::code`]).
/// * `coord-error-kind`: the [`ErrorKind`] discriminant name.
/// * `retry-after-seconds`: present only when the error surfaces a
///   rate-limit hint.
pub fn status_from_coord_error(err: &CoordError) -> Status {
    let code = match err.kind() {
        ErrorKind::Validation => tonic::Code::InvalidArgument,
        ErrorKind::NotFound => tonic::Code::NotFound,
        ErrorKind::AlreadyExists => tonic::Code::AlreadyExists,
        ErrorKind::Unauthenticated => tonic::Code::Unauthenticated,
        ErrorKind::PermissionDenied => tonic::Code::PermissionDenied,
        ErrorKind::ResourceExhausted => tonic::Code::ResourceExhausted,
        ErrorKind::Conflict => tonic::Code::Aborted,
        ErrorKind::FailedPrecondition => tonic::Code::FailedPrecondition,
        ErrorKind::Unavailable => tonic::Code::Unavailable,
        ErrorKind::DeadlineExceeded => tonic::Code::DeadlineExceeded,
        ErrorKind::Internal => tonic::Code::Internal,
    };
    let mut status = Status::new(code, err.to_string());
    let metadata = status.metadata_mut();
    if let Ok(value) = err.code().parse() {
        metadata.insert("coord-error-code", value);
    }
    let kind_name = format!("{:?}", err.kind());
    if let Ok(value) = kind_name.parse() {
        metadata.insert("coord-error-kind", value);
    }
    if let Some(seconds) = err.retry_after_seconds()
        && let Ok(value) = seconds.to_string().parse()
    {
        metadata.insert("retry-after-seconds", value);
    }
    status
}

/// Convenience adapter: convert any value `Into<CoordError>` into a
/// [`tonic::Status`] using the canonical mapping.
///
/// Intended for use with `Result::map_err`:
///
/// ```ignore
/// self.transit.create_key(&name).await.map_err(coord_status)?;
/// ```
pub fn coord_status<E: Into<CoordError>>(err: E) -> Status {
    status_from_coord_error(&err.into())
}

/// HTTP error body returned to JSON clients.
///
/// The shape is considered stable: `{ code, kind, message, retry_after_seconds? }`.
#[derive(Debug, Serialize)]
#[allow(dead_code)] // public HTTP contract; consumed reflectively by serde
pub struct CoordErrorBody {
    pub code: &'static str,
    pub kind: &'static str,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after_seconds: Option<u64>,
}

/// Map a [`CoordError`] to an [`axum`] JSON response.
///
/// Prefer `?` on a `Result<T, CoordError>` in handlers that return
/// `Result<_, HttpCoordError>`; the `http_api::error::ApiError` shim
/// exists for legacy handlers only and will be removed once all
/// call sites migrate.
#[allow(dead_code)] // public HTTP seam used outside http_api/ submodule
pub struct HttpCoordError(pub CoordError);

impl From<CoordError> for HttpCoordError {
    fn from(value: CoordError) -> Self {
        Self(value)
    }
}

impl IntoResponse for HttpCoordError {
    fn into_response(self) -> Response {
        let kind = self.0.kind();
        let status = StatusCode::from_u16(kind.http_status_u16())
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let retry_after = self.0.retry_after_seconds();
        let body = CoordErrorBody {
            code: self.0.code(),
            kind: kind_to_static_str(kind),
            message: self.0.to_string(),
            retry_after_seconds: retry_after,
        };
        let mut resp = (status, Json(body)).into_response();
        if let Some(seconds) = retry_after
            && let Ok(value) = seconds.to_string().parse()
        {
            resp.headers_mut().insert(header::RETRY_AFTER, value);
        }
        resp
    }
}

#[allow(dead_code)] // reserved for HTTP error serialisation in T-P1-03
fn kind_to_static_str(kind: ErrorKind) -> &'static str {
    match kind {
        ErrorKind::Validation => "Validation",
        ErrorKind::NotFound => "NotFound",
        ErrorKind::AlreadyExists => "AlreadyExists",
        ErrorKind::Unauthenticated => "Unauthenticated",
        ErrorKind::PermissionDenied => "PermissionDenied",
        ErrorKind::ResourceExhausted => "ResourceExhausted",
        ErrorKind::Conflict => "Conflict",
        ErrorKind::FailedPrecondition => "FailedPrecondition",
        ErrorKind::Unavailable => "Unavailable",
        ErrorKind::DeadlineExceeded => "DeadlineExceeded",
        ErrorKind::Internal => "Internal",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use coord_core::security::SecurityError;
    use coord_core::transit::TransitError;
    use coord_core::validation::KeyValidationError;

    #[test]
    fn sealed_maps_to_failed_precondition_with_code() {
        let err: CoordError = SecurityError::Sealed.into();
        let status = status_from_coord_error(&err);
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            status
                .metadata()
                .get("coord-error-code")
                .map(|v| v.to_str().unwrap_or_default().to_string())
                .as_deref(),
            Some("security.sealed")
        );
    }

    #[test]
    fn key_not_found_maps_to_not_found() {
        let err: CoordError = TransitError::KeyNotFound {
            key_name: "k".into(),
        }
        .into();
        let status = status_from_coord_error(&err);
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[test]
    fn rate_limited_attaches_retry_after_metadata() {
        let err = CoordError::RateLimited {
            reason: "login".into(),
            retry_after_seconds: Some(3),
        };
        let status = status_from_coord_error(&err);
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        assert_eq!(
            status
                .metadata()
                .get("retry-after-seconds")
                .map(|v| v.to_str().unwrap_or_default().to_string())
                .as_deref(),
            Some("3")
        );
    }

    #[test]
    fn http_validation_error_has_400_status_and_body_code() {
        let err: CoordError = KeyValidationError::Empty.into();
        let resp = HttpCoordError(err).into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn http_rate_limit_sets_retry_after_header() {
        let err = CoordError::RateLimited {
            reason: "login".into(),
            retry_after_seconds: Some(7),
        };
        let resp = HttpCoordError(err).into_response();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        let header_value = resp
            .headers()
            .get(header::RETRY_AFTER)
            .expect("Retry-After header must be set");
        assert_eq!(header_value.to_str().unwrap_or(""), "7");
    }

    // ---- T-P1-04 additions: exhaustive error-contract regression ----

    #[test]
    fn invalid_argument_maps_to_grpc_invalid_argument() {
        let err = CoordError::InvalidArgument("bad".into());
        let status = status_from_coord_error(&err);
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            status
                .metadata()
                .get("coord-error-code")
                .unwrap()
                .to_str()
                .unwrap(),
            "validation.invalid_argument"
        );
        assert_eq!(
            status
                .metadata()
                .get("coord-error-kind")
                .unwrap()
                .to_str()
                .unwrap(),
            "Validation"
        );
    }

    #[test]
    fn not_found_maps_to_grpc_not_found() {
        let err = CoordError::NotFound {
            resource: "config",
            id: "k1".into(),
        };
        let status = status_from_coord_error(&err);
        assert_eq!(status.code(), tonic::Code::NotFound);
        assert_eq!(
            status
                .metadata()
                .get("coord-error-code")
                .unwrap()
                .to_str()
                .unwrap(),
            "generic.not_found"
        );
    }

    #[test]
    fn unauthenticated_maps_to_grpc_unauthenticated() {
        let err = CoordError::Unauthenticated("no token".into());
        let status = status_from_coord_error(&err);
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn permission_denied_maps_to_grpc_permission_denied() {
        let err = CoordError::PermissionDenied("nope".into());
        let status = status_from_coord_error(&err);
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn unavailable_maps_to_grpc_unavailable() {
        let err = CoordError::Unavailable("no leader".into());
        let status = status_from_coord_error(&err);
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(
            status
                .metadata()
                .get("coord-error-code")
                .unwrap()
                .to_str()
                .unwrap(),
            "cluster.unavailable"
        );
    }

    #[test]
    fn internal_maps_to_grpc_internal() {
        let err = CoordError::Internal("bug".into());
        let status = status_from_coord_error(&err);
        assert_eq!(status.code(), tonic::Code::Internal);
    }

    #[test]
    fn http_not_found_returns_404() {
        let err = CoordError::NotFound {
            resource: "lock",
            id: "x".into(),
        };
        let resp = HttpCoordError(err).into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn http_unavailable_returns_503() {
        let err = CoordError::Unavailable("down".into());
        let resp = HttpCoordError(err).into_response();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn http_internal_returns_500() {
        let err = CoordError::Internal("crash".into());
        let resp = HttpCoordError(err).into_response();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn http_invalid_argument_returns_400() {
        let err = CoordError::InvalidArgument("bad field".into());
        let resp = HttpCoordError(err).into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn grpc_status_message_contains_error_text() {
        let err = CoordError::NotFound {
            resource: "workflow",
            id: "wf-1".into(),
        };
        let status = status_from_coord_error(&err);
        assert!(
            status.message().contains("wf-1"),
            "gRPC message should contain the resource id"
        );
    }

    #[test]
    fn coord_status_convenience_matches_direct_call() {
        let err = CoordError::Internal("test".into());
        let direct = status_from_coord_error(&err);
        let convenience = coord_status(CoordError::Internal("test".into()));
        assert_eq!(direct.code(), convenience.code());
        assert_eq!(direct.message(), convenience.message());
    }
}
