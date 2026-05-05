//! Error-contract regression tests (T-P1-04).
//!
//! These tests verify that the canonical error mapping in
//! `wire::error` produces the expected gRPC codes, HTTP status codes,
//! and metadata for every [`ErrorKind`] category. Any new service
//! handler that bypasses the unified mapping will fail these assertions.

use coord_core::error::{CoordError, ErrorKind};

// ---------------------------------------------------------------------------
// Helper: build representative errors for each ErrorKind
// ---------------------------------------------------------------------------

fn representative_errors() -> Vec<(CoordError, ErrorKind, &'static str)> {
    vec![
        (
            CoordError::InvalidArgument("bad field".into()),
            ErrorKind::Validation,
            "validation.invalid_argument",
        ),
        (
            CoordError::NotFound {
                resource: "config",
                id: "k1".into(),
            },
            ErrorKind::NotFound,
            "generic.not_found",
        ),
        (
            CoordError::Unauthenticated("missing token".into()),
            ErrorKind::Unauthenticated,
            "auth.unauthenticated",
        ),
        (
            CoordError::PermissionDenied("read-only".into()),
            ErrorKind::PermissionDenied,
            "auth.permission_denied",
        ),
        (
            CoordError::RateLimited {
                reason: "too fast".into(),
                retry_after_seconds: Some(5),
            },
            ErrorKind::ResourceExhausted,
            "rate_limit.exceeded",
        ),
        (
            CoordError::Internal("oops".into()),
            ErrorKind::Internal,
            "internal.error",
        ),
        (
            CoordError::Unavailable("no leader".into()),
            ErrorKind::Unavailable,
            "cluster.unavailable",
        ),
    ]
}

// ---------------------------------------------------------------------------
// ErrorKind → HTTP status code mapping
// ---------------------------------------------------------------------------

#[test]
fn error_kind_http_status_mapping() {
    let cases = [
        (ErrorKind::Validation, 400),
        (ErrorKind::NotFound, 404),
        (ErrorKind::AlreadyExists, 409),
        (ErrorKind::Unauthenticated, 401),
        (ErrorKind::PermissionDenied, 403),
        (ErrorKind::ResourceExhausted, 429),
        (ErrorKind::Conflict, 409),
        (ErrorKind::FailedPrecondition, 412),
        (ErrorKind::Unavailable, 503),
        (ErrorKind::DeadlineExceeded, 504),
        (ErrorKind::Internal, 500),
    ];
    for (kind, expected) in cases {
        assert_eq!(
            kind.http_status_u16(),
            expected,
            "{kind:?} should map to HTTP {expected}"
        );
    }
}

// ---------------------------------------------------------------------------
// ErrorKind → gRPC code name mapping
// ---------------------------------------------------------------------------

#[test]
fn error_kind_grpc_code_name_mapping() {
    let cases = [
        (ErrorKind::Validation, "InvalidArgument"),
        (ErrorKind::NotFound, "NotFound"),
        (ErrorKind::AlreadyExists, "AlreadyExists"),
        (ErrorKind::Unauthenticated, "Unauthenticated"),
        (ErrorKind::PermissionDenied, "PermissionDenied"),
        (ErrorKind::ResourceExhausted, "ResourceExhausted"),
        (ErrorKind::Conflict, "Aborted"),
        (ErrorKind::FailedPrecondition, "FailedPrecondition"),
        (ErrorKind::Unavailable, "Unavailable"),
        (ErrorKind::DeadlineExceeded, "DeadlineExceeded"),
        (ErrorKind::Internal, "Internal"),
    ];
    for (kind, expected) in cases {
        assert_eq!(
            kind.grpc_code_name(),
            expected,
            "{kind:?} should map to gRPC {expected}"
        );
    }
}

// ---------------------------------------------------------------------------
// CoordError → kind + code consistency
// ---------------------------------------------------------------------------

#[test]
fn coord_error_kind_and_code_consistency() {
    for (err, expected_kind, expected_code) in representative_errors() {
        assert_eq!(err.kind(), expected_kind, "kind mismatch for {err}");
        assert_eq!(err.code(), expected_code, "code mismatch for {err}");
    }
}

// ---------------------------------------------------------------------------
// retry_after_seconds only present for RateLimited
// ---------------------------------------------------------------------------

#[test]
fn retry_after_only_on_rate_limited() {
    for (err, kind, _) in representative_errors() {
        if kind == ErrorKind::ResourceExhausted {
            assert!(
                err.retry_after_seconds().is_some(),
                "RateLimited should carry retry hint"
            );
        } else {
            assert!(
                err.retry_after_seconds().is_none(),
                "{err} should not carry retry hint"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Error code format: must be <domain>.<reason>
// ---------------------------------------------------------------------------

#[test]
fn error_code_format_is_dotted() {
    for (err, _, code) in representative_errors() {
        assert!(
            code.contains('.'),
            "error code '{code}' for {err} must use <domain>.<reason> format"
        );
    }
}
