// Integration tests for coord-macros
//
// Integration tests run as a separate crate that CAN use proc macros
// (unlike unit tests inside a proc-macro crate).

use coord_macros::{Builder, ValidateRevision};

// ──── ValidateRevision ────

#[derive(ValidateRevision)]
struct PutResponse {
    revision: u64,
    key: Vec<u8>,
}

#[derive(ValidateRevision)]
struct DeleteResponse {
    revision: u64,
}

#[test]
fn test_validate_revision_valid() {
    let resp = PutResponse {
        revision: 5,
        key: b"hello".to_vec(),
    };
    assert!(resp.validate_revision().is_ok());
}

#[test]
fn test_validate_revision_zero_fails() {
    let resp = PutResponse {
        revision: 0,
        key: b"test".to_vec(),
    };
    let err = resp.validate_revision().unwrap_err();
    assert!(matches!(err, coord_core::error::Error::InvalidArgument(_)));
}

#[test]
fn test_validate_revision_delete_response() {
    let resp = DeleteResponse { revision: 42 };
    assert!(resp.validate_revision().is_ok());

    let resp = DeleteResponse { revision: 0 };
    assert!(resp.validate_revision().is_err());
}

// ──── Builder ────

#[derive(Builder, Debug, PartialEq)]
struct Config {
    timeout_ms: u64,
    max_retries: u32,
    name: String,
}

#[test]
fn test_builder_all_fields() {
    let config = Config {
        timeout_ms: 5000,
        max_retries: 3,
        name: "test".into(),
    }
    .with_timeout_ms(10000)
    .with_max_retries(5)
    .with_name("production".into());

    assert_eq!(config.timeout_ms, 10000);
    assert_eq!(config.max_retries, 5);
    assert_eq!(config.name, "production");
}

#[test]
fn test_builder_single_field() {
    let config = Config {
        timeout_ms: 1000,
        max_retries: 1,
        name: "default".into(),
    }
    .with_timeout_ms(3000);

    assert_eq!(config.timeout_ms, 3000);
    assert_eq!(config.max_retries, 1); // unchanged
    assert_eq!(config.name, "default"); // unchanged
}
