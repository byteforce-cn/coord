// CCT v3 — Capability Credential Token
//
// JWT-like structured token format (see docs/capability-auth-implementation.md §3):
//   CCT = base64url( header ) || "." || base64url( payload ) || "." || base64url( signature )
//
// Payload contains only role IDs (not full capability lists). Agent resolves
// capabilities from locally cached Role→Capability map.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use sha2::Sha256;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

type HmacSha256 = Hmac<Sha256>;

// ──── CCT Header ────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CctHeader {
    pub alg: String,
    pub typ: String,
    pub kid: String,
}

impl Default for CctHeader {
    fn default() -> Self {
        Self {
            alg: "HMAC-SHA256".to_string(),
            typ: "CCT".to_string(),
            kid: "token-signing-key-v1".to_string(),
        }
    }
}

// ──── CCT Payload ────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CctPayload {
    /// JWT ID — unique token identifier (for revocation)
    pub jti: String,
    /// Issuer — cluster identifier
    pub iss: String,
    /// Subject — AppRole name
    pub sub: String,
    /// Audience
    #[serde(default)]
    pub aud: Vec<String>,
    /// Issued at (Unix seconds)
    pub iat: i64,
    /// Expiration (Unix seconds)
    pub exp: i64,
    /// Role IDs embedded in token (resolved locally by Agent)
    pub roles: Vec<String>,
    /// Per-capability scope overrides (empty = use role default)
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub scope_overrides: HashMap<String, String>,
}

// ──── CCT Token ────

/// A fully decoded CCT token
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CctToken {
    pub header: CctHeader,
    pub payload: CctPayload,
    /// Raw signature bytes (for verification)
    pub signature: Vec<u8>,
}

// ──── CCT Encode/Decode ────

/// Encode header + payload into a CCT string, signed with the given key.
pub fn encode_cct(header: &CctHeader, payload: &CctPayload, signing_key: &[u8]) -> Result<String> {
    let header_json = serde_json::to_string(header)
        .map_err(|e| Error::Internal(format!("CCT header serialization: {e}")))?;
    let payload_json = serde_json::to_string(payload)
        .map_err(|e| Error::Internal(format!("CCT payload serialization: {e}")))?;

    let header_b64 = base64_url_encode(header_json.as_bytes());
    let payload_b64 = base64_url_encode(payload_json.as_bytes());

    let signing_input = format!("{header_b64}.{payload_b64}");
    let signature = sign_data(signing_input.as_bytes(), signing_key)?;
    let sig_b64 = base64_url_encode(&signature);

    Ok(format!("{signing_input}.{sig_b64}"))
}

/// Decode and verify a CCT string.
pub fn decode_cct(token: &str, signing_key: &[u8]) -> Result<CctToken> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(Error::InvalidToken("CCT must have 3 parts (header.payload.signature)".to_string()));
    }

    let header_json = base64_url_decode(parts[0])
        .map_err(|e| Error::InvalidToken(format!("header decode: {e}")))?;
    let payload_json = base64_url_decode(parts[1])
        .map_err(|e| Error::InvalidToken(format!("payload decode: {e}")))?;
    let signature = base64_url_decode(parts[2])
        .map_err(|e| Error::InvalidToken(format!("signature decode: {e}")))?;

    // Verify signature
    let signing_input = format!("{}.{}", parts[0], parts[1]);
    verify_signature(signing_input.as_bytes(), &signature, signing_key)?;

    let header: CctHeader = serde_json::from_slice(&header_json)
        .map_err(|e| Error::InvalidToken(format!("header JSON: {e}")))?;
    let payload: CctPayload = serde_json::from_slice(&payload_json)
        .map_err(|e| Error::InvalidToken(format!("payload JSON: {e}")))?;

    Ok(CctToken { header, payload, signature })
}

/// Check if a CCT payload has expired, with optional clock drift tolerance.
pub fn is_expired(payload: &CctPayload, clock_drift_secs: i64) -> bool {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    payload.exp < now - clock_drift_secs
}

// ──── Internal helpers ────

fn base64_url_encode(data: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(data)
}

fn base64_url_decode(encoded: &str) -> std::result::Result<Vec<u8>, String> {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|e| e.to_string())
}

fn sign_data(data: &[u8], key: &[u8]) -> Result<Vec<u8>> {
    let mut mac = HmacSha256::new_from_slice(key)
        .map_err(|e| Error::Crypto(format!("HMAC key invalid: {e}")))?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn verify_signature(data: &[u8], signature: &[u8], key: &[u8]) -> Result<()> {
    let mut mac = HmacSha256::new_from_slice(key)
        .map_err(|e| Error::Crypto(format!("HMAC key invalid: {e}")))?;
    mac.update(data);
    mac.verify_slice(signature)
        .map_err(|_| Error::InvalidToken("signature verification failed".to_string()))
}

// ──── Tests ────

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_KEY: &[u8] = b"test-signing-key-32-bytes-long!!";

    // ──── Phase 0.1: CCT encode/decode round-trip (RED) ────

    #[test]
    fn test_cct_roundtrip_basic() {
        let header = CctHeader::default();
        let payload = CctPayload {
            jti: "tok_test_001".to_string(),
            iss: "coord-cluster-01".to_string(),
            sub: "approle-order-service".to_string(),
            aud: vec!["coord-agent".to_string()],
            iat: 1719990000,
            exp: 1719993600,
            roles: vec!["service-writer".to_string()],
            scope_overrides: HashMap::new(),
        };

        let token = encode_cct(&header, &payload, TEST_KEY)
            .expect("encode should succeed");

        // Token should be 3-part base64url
        assert!(token.starts_with("eyJ"), "Token should start with base64url JSON header");
        assert_eq!(token.matches('.').count(), 2, "Token should have exactly 2 dots");

        let decoded = decode_cct(&token, TEST_KEY)
            .expect("decode should succeed");

        assert_eq!(decoded.header, header);
        assert_eq!(decoded.payload, payload);
    }

    #[test]
    fn test_cct_decode_wrong_key_fails() {
        let header = CctHeader::default();
        let payload = CctPayload {
            jti: "tok_test_002".to_string(),
            iss: "coord-cluster-01".to_string(),
            sub: "test".to_string(),
            aud: vec!["coord-agent".to_string()],
            iat: 1719990000,
            exp: 1719993600,
            roles: vec!["reader".to_string()],
            scope_overrides: HashMap::new(),
        };

        let token = encode_cct(&header, &payload, TEST_KEY).unwrap();
        let wrong_key = b"wrong-key-32-bytes-long-here!!!";

        let result = decode_cct(&token, wrong_key);
        assert!(result.is_err(), "decode with wrong key should fail");
        match result {
            Err(Error::InvalidToken(msg)) => {
                assert!(msg.contains("signature"), "error should mention signature");
            }
            _ => panic!("expected InvalidToken error"),
        }
    }

    #[test]
    fn test_cct_expiry_check() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let expired_payload = CctPayload {
            jti: "tok_expired".to_string(),
            iss: "test".to_string(),
            sub: "test".to_string(),
            aud: vec![],
            iat: now - 7200,
            exp: now - 3600, // expired 1 hour ago
            roles: vec![],
            scope_overrides: HashMap::new(),
        };

        assert!(is_expired(&expired_payload, 300));
        assert!(!is_expired(&expired_payload, 7200)); // wide drift tolerance

        let valid_payload = CctPayload {
            jti: "tok_valid".to_string(),
            iss: "test".to_string(),
            sub: "test".to_string(),
            aud: vec![],
            iat: now - 60,
            exp: now + 3600, // valid for 1 more hour
            roles: vec![],
            scope_overrides: HashMap::new(),
        };

        assert!(!is_expired(&valid_payload, 300));
    }

    #[test]
    fn test_cct_multiple_roles() {
        let header = CctHeader::default();
        let payload = CctPayload {
            jti: "tok_multi".to_string(),
            iss: "test".to_string(),
            sub: "test".to_string(),
            aud: vec!["coord-agent".to_string()],
            iat: 1719990000,
            exp: 1719993600,
            roles: vec!["service-reader".to_string(), "config-manager".to_string()],
            scope_overrides: HashMap::new(),
        };

        let token = encode_cct(&header, &payload, TEST_KEY).unwrap();
        let decoded = decode_cct(&token, TEST_KEY).unwrap();
        assert_eq!(decoded.payload.roles.len(), 2);
        assert!(decoded.payload.roles.contains(&"service-reader".to_string()));
        assert!(decoded.payload.roles.contains(&"config-manager".to_string()));
    }

    #[test]
    fn test_cct_with_scope_overrides() {
        let header = CctHeader::default();
        let mut overrides = HashMap::new();
        overrides.insert("data:kv:read".to_string(), "/app/order-service/".to_string());
        overrides.insert("data:kv:write".to_string(), "/app/order-service/".to_string());

        let payload = CctPayload {
            jti: "tok_override".to_string(),
            iss: "test".to_string(),
            sub: "test".to_string(),
            aud: vec!["coord-agent".to_string()],
            iat: 1719990000,
            exp: 1719993600,
            roles: vec!["service-writer".to_string()],
            scope_overrides: overrides,
        };

        let token = encode_cct(&header, &payload, TEST_KEY).unwrap();
        let decoded = decode_cct(&token, TEST_KEY).unwrap();
        assert_eq!(decoded.payload.scope_overrides.len(), 2);
        assert_eq!(
            decoded.payload.scope_overrides.get("data:kv:read"),
            Some(&"/app/order-service/".to_string())
        );
    }

    #[test]
    fn test_cct_tampered_payload_fails() {
        let header = CctHeader::default();
        let payload = CctPayload {
            jti: "tok_tamper".to_string(),
            iss: "test".to_string(),
            sub: "test".to_string(),
            aud: vec![],
            iat: 1719990000,
            exp: 1719993600,
            roles: vec!["reader".to_string()],
            scope_overrides: HashMap::new(),
        };

        let token = encode_cct(&header, &payload, TEST_KEY).unwrap();

        // Tamper with the payload part (replace roles)
        let parts: Vec<&str> = token.split('.').collect();
        let tampered_payload = base64_url_encode(
            br#"{"jti":"tok_tamper","iss":"test","sub":"test","aud":[],"iat":1719990000,"exp":1719993600,"roles":["admin"]}"#
        );
        let tampered_token = format!("{}.{}.{}", parts[0], tampered_payload, parts[2]);

        let result = decode_cct(&tampered_token, TEST_KEY);
        assert!(result.is_err(), "tampered token should fail verification");
    }

    #[test]
    fn test_cct_invalid_format() {
        // Too few parts
        let result = decode_cct("header.payload", TEST_KEY);
        assert!(result.is_err());

        // Empty string
        let result = decode_cct("", TEST_KEY);
        assert!(result.is_err());
    }
}
