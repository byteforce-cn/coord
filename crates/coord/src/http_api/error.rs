//! JSON-shaped API errors with optional `Retry-After` support.
//!
//! Extracted from the monolithic `http_api.rs` to isolate the error contract
//! so it can be depended on by sibling submodules (`auth`, `audit`, `ui`,
//! `helpers`) without pulling in the rest of the handler surface.
//!
//! As of Batch 5 round 2, `ApiError` carries the stable
//! [`coord_core::error`] code + kind so the JSON body matches the
//! gRPC metadata contract. Handlers that still construct `ApiError`
//! directly continue to work unchanged; new code should prefer
//! `?` on a `Result<T, CoordError>` via the
//! [`From<CoordError> for ApiError`] impl below.

use axum::Json;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use coord_core::error::{CoordError, ErrorKind};
use serde::Serialize;

#[derive(Debug)]
pub(super) struct ApiError {
    pub(super) status: StatusCode,
    pub(super) message: String,
    pub(super) retry_after_seconds: Option<u64>,
    pub(super) code: Option<&'static str>,
    pub(super) kind: Option<&'static str>,
}

impl ApiError {
    pub(super) fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
            retry_after_seconds: None,
            code: None,
            kind: None,
        }
    }
}

impl From<CoordError> for ApiError {
    fn from(err: CoordError) -> Self {
        let kind = err.kind();
        let status = StatusCode::from_u16(kind.http_status_u16())
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        Self {
            status,
            message: err.to_string(),
            retry_after_seconds: err.retry_after_seconds(),
            code: Some(err.code()),
            kind: Some(kind_static(kind)),
        }
    }
}

const fn kind_static(kind: ErrorKind) -> &'static str {
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

#[derive(Serialize)]
struct ErrorBody {
    error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    kind: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_after_seconds: Option<u64>,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let retry_after = self.retry_after_seconds;
        let body = ErrorBody {
            error: self.message,
            code: self.code,
            kind: self.kind,
            retry_after_seconds: retry_after,
        };
        let mut resp = (self.status, Json(body)).into_response();
        if let Some(secs) = retry_after
            && let Ok(value) = secs.to_string().parse()
        {
            resp.headers_mut().insert(header::RETRY_AFTER, value);
        }
        resp
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use coord_core::security::SecurityError;
    use coord_core::validation::KeyValidationError;

    #[test]
    fn api_error_retry_after_header_propagates() {
        let err = ApiError {
            status: StatusCode::TOO_MANY_REQUESTS,
            message: "slow down".into(),
            retry_after_seconds: Some(7),
            code: None,
            kind: None,
        };
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        let header_value = resp
            .headers()
            .get(header::RETRY_AFTER)
            .expect("Retry-After header must be set");
        assert_eq!(header_value.to_str().unwrap(), "7");
    }

    #[test]
    fn api_error_without_retry_after_has_no_header() {
        let err = ApiError::new(StatusCode::BAD_REQUEST, "bad");
        let resp = err.into_response();
        assert!(resp.headers().get(header::RETRY_AFTER).is_none());
    }

    #[test]
    fn coord_error_conversion_preserves_code_and_status() {
        let api: ApiError = CoordError::from(SecurityError::Sealed).into();
        assert_eq!(api.status, StatusCode::PRECONDITION_FAILED);
        assert_eq!(api.code, Some("security.sealed"));
        assert_eq!(api.kind, Some("FailedPrecondition"));
    }

    #[test]
    fn coord_error_validation_becomes_400() {
        let api: ApiError = CoordError::from(KeyValidationError::Empty).into();
        assert_eq!(api.status, StatusCode::BAD_REQUEST);
        assert_eq!(api.code, Some("validation.invalid_key"));
    }

    #[test]
    fn coord_error_rate_limited_carries_retry_after() {
        let api: ApiError = CoordError::RateLimited {
            reason: "login".into(),
            retry_after_seconds: Some(3),
        }
        .into();
        assert_eq!(api.status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(api.retry_after_seconds, Some(3));
    }
}
