// CCT v3 — Capability Credential Token
//
// JWT-like structured token format:
//   CCT = base64url( header ) || "." || base64url( payload ) || "." || base64url( signature )
//
// Payload contains only role IDs (not full capability lists). Agent resolves
// capabilities from locally cached Role→Capability map.
//
// 签名算法双轨制——
// - `HMAC-SHA256`：历史对称方案（server/agent 同钥），保留用于兼容验证；
// - `Ed25519`：非对称方案（server 持私钥签发，agent 仅持公钥验证），
//   任一 agent 被控无法伪造 token。生产签发改用 Ed25519，验证双算法并行
//   （宽限期至存量 HMAC token TTL 结束）。

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::error::{Error, Result};

type HmacSha256 = Hmac<Sha256>;

/// CCT 签名算法标识
pub const CCT_ALG_HMAC_SHA256: &str = "HMAC-SHA256";
pub const CCT_ALG_ED25519: &str = "Ed25519";

/// HMAC 密钥最小长度（字节）。A3 fail-closed：
///
/// 空密钥是**真实漏洞**——`hex::decode("")` 得到 `Ok(vec![])`，而 `hmac` 对空 key
/// 返回 `Ok`，于是"知道密钥为空"的任何人都能自签 `roles:["root"]` 的 CCT。
/// 因此 HMAC 分支**拒绝**空密钥与弱于 256 bit 的密钥，而不是把它当作可用密钥。
pub const MIN_HMAC_KEY_LEN: usize = 32;

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
            alg: CCT_ALG_HMAC_SHA256.to_string(),
            typ: "CCT".to_string(),
            kid: "token-signing-key-v1".to_string(),
        }
    }
}

impl CctHeader {
    /// Ed25519 签发头（kid 标记非对称签名密钥）
    pub fn ed25519() -> Self {
        Self {
            alg: CCT_ALG_ED25519.to_string(),
            typ: "CCT".to_string(),
            kid: "cct-ed25519-key-v1".to_string(),
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

/// Encode header + payload into a CCT string, HMAC-signed with the given key.
pub fn encode_cct(header: &CctHeader, payload: &CctPayload, signing_key: &[u8]) -> Result<String> {
    let (signing_input, header_b64, payload_b64) = frame(header, payload)?;
    let signature = sign_hmac(signing_input.as_bytes(), signing_key)?;
    let sig_b64 = base64_url_encode(&signature);
    Ok(format!("{header_b64}.{payload_b64}.{sig_b64}"))
}

/// 用 Ed25519 私钥签发 CCT（header.alg 须为 `Ed25519`）。
pub fn encode_cct_ed25519(
    header: &CctHeader,
    payload: &CctPayload,
    signing_key: &SigningKey,
) -> Result<String> {
    if header.alg != CCT_ALG_ED25519 {
        return Err(Error::Internal(format!(
            "encode_cct_ed25519 requires header.alg = {CCT_ALG_ED25519} (got {})",
            header.alg
        )));
    }
    let (signing_input, header_b64, payload_b64) = frame(header, payload)?;
    let signature = signing_key.sign(signing_input.as_bytes());
    let sig_b64 = base64_url_encode(&signature.to_bytes());
    Ok(format!("{header_b64}.{payload_b64}.{sig_b64}"))
}

/// Decode and verify a CCT string（HMAC-SHA256 签名）。
pub fn decode_cct(token: &str, signing_key: &[u8]) -> Result<CctToken> {
    decode_cct_any(token, &[signing_key], None)
}

/// 按 header.alg 分发的验证入口。
///
/// - `HMAC-SHA256` → 依次尝试 `hmac_keys`（宽限期：多版本历史密钥）；
/// - `Ed25519` → 用 `ed25519_pub`（32 字节公钥）验证；未配置公钥 → 拒绝。
pub fn decode_cct_any(
    token: &str,
    hmac_keys: &[&[u8]],
    ed25519_pub: Option<&[u8]>,
) -> Result<CctToken> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(Error::InvalidToken(
            "CCT must have 3 parts (header.payload.signature)".to_string(),
        ));
    }

    let header_json = base64_url_decode(parts[0])
        .map_err(|e| Error::InvalidToken(format!("header decode: {e}")))?;
    let payload_json = base64_url_decode(parts[1])
        .map_err(|e| Error::InvalidToken(format!("payload decode: {e}")))?;
    let signature = base64_url_decode(parts[2])
        .map_err(|e| Error::InvalidToken(format!("signature decode: {e}")))?;

    // 先解 header 以按 alg 分发验签
    let header: CctHeader = serde_json::from_slice(&header_json)
        .map_err(|e| Error::InvalidToken(format!("header JSON: {e}")))?;
    let signing_input = format!("{}.{}", parts[0], parts[1]);

    match header.alg.as_str() {
        CCT_ALG_HMAC_SHA256 => {
            let mut verified = false;
            let mut usable_key_seen = false;
            for key in hmac_keys {
                // A3：空/过短密钥**不构成**可用密钥材料。若全部候选都不可用，
                // 必须报错而不是"验签通过"（空 key 的 HMAC 是任何人都能算的）。
                if !is_usable_hmac_key(key) {
                    continue;
                }
                usable_key_seen = true;
                if verify_hmac(signing_input.as_bytes(), &signature, key).is_ok() {
                    verified = true;
                    break;
                }
            }
            if !usable_key_seen {
                return Err(Error::InvalidToken(format!(
                    "HMAC-SHA256 CCT presented but no usable HMAC key configured \
                     (need >= {MIN_HMAC_KEY_LEN} bytes; empty/short keys are refused)"
                )));
            }
            if !verified {
                return Err(Error::InvalidToken(
                    "signature verification failed".to_string(),
                ));
            }
        }
        CCT_ALG_ED25519 => {
            let pub_bytes: [u8; 32] = ed25519_pub
                .ok_or_else(|| {
                    Error::InvalidToken(
                        "Ed25519 CCT presented but no public key configured".to_string(),
                    )
                })?
                .try_into()
                .map_err(|_| {
                    Error::InvalidToken("invalid Ed25519 public key length".to_string())
                })?;
            let vk = VerifyingKey::from_bytes(&pub_bytes)
                .map_err(|e| Error::Crypto(format!("invalid Ed25519 public key: {e}")))?;
            let sig = Signature::from_slice(&signature)
                .map_err(|e| Error::InvalidToken(format!("invalid Ed25519 signature: {e}")))?;
            vk.verify_strict(signing_input.as_bytes(), &sig)
                .map_err(|_| Error::InvalidToken("signature verification failed".to_string()))?;
        }
        other => {
            return Err(Error::InvalidToken(format!("unsupported CCT alg: {other}")));
        }
    }

    let payload: CctPayload = serde_json::from_slice(&payload_json)
        .map_err(|e| Error::InvalidToken(format!("payload JSON: {e}")))?;

    Ok(CctToken {
        header,
        payload,
        signature,
    })
}

/// 仅用 Ed25519 公钥验证（agent 侧无 HMAC 密钥时使用）。
pub fn decode_cct_ed25519(token: &str, ed25519_pub: &[u8]) -> Result<CctToken> {
    decode_cct_any(token, &[], Some(ed25519_pub))
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

/// 序列化并 base64 帧化 header/payload，返回 (signing_input, header_b64, payload_b64)
fn frame(header: &CctHeader, payload: &CctPayload) -> Result<(String, String, String)> {
    let header_json = serde_json::to_string(header)
        .map_err(|e| Error::Internal(format!("CCT header serialization: {e}")))?;
    let payload_json = serde_json::to_string(payload)
        .map_err(|e| Error::Internal(format!("CCT payload serialization: {e}")))?;

    let header_b64 = base64_url_encode(header_json.as_bytes());
    let payload_b64 = base64_url_encode(payload_json.as_bytes());
    let signing_input = format!("{header_b64}.{payload_b64}");
    Ok((signing_input, header_b64, payload_b64))
}

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

fn sign_hmac(data: &[u8], key: &[u8]) -> Result<Vec<u8>> {
    ensure_usable_hmac_key(key)?;
    let mut mac = HmacSha256::new_from_slice(key)
        .map_err(|e| Error::Crypto(format!("HMAC key invalid: {e}")))?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn verify_hmac(data: &[u8], signature: &[u8], key: &[u8]) -> Result<()> {
    ensure_usable_hmac_key(key)?;
    let mut mac = HmacSha256::new_from_slice(key)
        .map_err(|e| Error::Crypto(format!("HMAC key invalid: {e}")))?;
    mac.update(data);
    mac.verify_slice(signature)
        .map_err(|_| Error::InvalidToken("signature verification failed".to_string()))
}

/// A3 / 第三轮 §3.6：密钥材料是否可用于 HMAC。
///
/// 要求：长度 >= [`MIN_HMAC_KEY_LEN`]，**且不是"单字节重复"的占位串**（全零 / 全 `0xFF` 等）。
///
/// 为何把熵要求放进原语层：此前"长度"检查只在 agent 侧、"全零"检查只在服务端配置层
/// （见 `coord/src/config.rs`），**两侧不对称**——运维若照抄一个 32 字节全零占位串，
/// agent 会接受，而服务端对同一模式是显式拒绝的。A3 的教训就是：安全下限必须设在
/// 密码学原语层，而不是散落在调用点。
pub fn is_usable_hmac_key(key: &[u8]) -> bool {
    key.len() >= MIN_HMAC_KEY_LEN
        && key
            .first()
            .is_some_and(|first| key.iter().any(|b| b != first))
}

/// A3：拒绝空/过短/**占位** HMAC 密钥（fail-closed，绝不"当作可用密钥"继续）。
fn ensure_usable_hmac_key(key: &[u8]) -> Result<()> {
    if is_usable_hmac_key(key) {
        return Ok(());
    }
    Err(Error::Crypto(format!(
        "HMAC signing/verification key must be >= {MIN_HMAC_KEY_LEN} bytes and not a \
         single-byte-repeated placeholder (got {} bytes); such keys are refused (A3)",
        key.len()
    )))
}

// ──── Tests ────

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_KEY: &[u8] = b"test-signing-key-32-bytes-long!!";

    /// 第三轮 §3.6：占位密钥（全零 / 单字节重复）必须在**原语层**被拒。
    ///
    /// 此前长度检查只在 agent 侧、全零检查只在服务端配置层——两侧不对称，
    /// 运维照抄 32 字节全零占位串时 agent 会接受。
    #[test]
    fn placeholder_hmac_keys_are_not_usable() {
        assert!(!is_usable_hmac_key(&[]), "空密钥");
        assert!(!is_usable_hmac_key(&[0u8; 31]), "过短");
        assert!(!is_usable_hmac_key(&[0u8; 32]), "32B 全零占位");
        assert!(!is_usable_hmac_key(&[0xABu8; 32]), "32B 单字节重复占位");
        assert!(is_usable_hmac_key(TEST_KEY));
        // sign/verify 同样 fail-closed
        assert!(sign_hmac(b"x", &[0u8; 32]).is_err());
        assert!(verify_hmac(b"x", &[0u8; 32], &[0u8; 32]).is_err());
    }

    // ──── CCT encode/decode round-trip (RED) ────

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

        let token = encode_cct(&header, &payload, TEST_KEY).expect("encode should succeed");

        // Token should be 3-part base64url
        assert!(
            token.starts_with("eyJ"),
            "Token should start with base64url JSON header"
        );
        assert_eq!(
            token.matches('.').count(),
            2,
            "Token should have exactly 2 dots"
        );

        let decoded = decode_cct(&token, TEST_KEY).expect("decode should succeed");

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
        // 注意：必须是 >= MIN_HMAC_KEY_LEN 的"错误密钥"，否则会先撞上
        // A3 的"密钥材料不可用"分支（见 test_cct_hmac_empty_key_is_refused_*）。
        let wrong_key = b"wrong-key-32-bytes-long-here!!!!";

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
        assert!(decoded
            .payload
            .roles
            .contains(&"service-reader".to_string()));
        assert!(decoded
            .payload
            .roles
            .contains(&"config-manager".to_string()));
    }

    #[test]
    fn test_cct_with_scope_overrides() {
        let header = CctHeader::default();
        let mut overrides = HashMap::new();
        overrides.insert(
            "data:kv:read".to_string(),
            "/app/order-service/".to_string(),
        );
        overrides.insert(
            "data:kv:write".to_string(),
            "/app/order-service/".to_string(),
        );

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

    // ──── Ed25519 非对称签发/验证 ────

    fn ed_test_payload() -> CctPayload {
        CctPayload {
            jti: "ed-token-1".to_string(),
            iss: "coord-cluster-01".to_string(),
            sub: "approle-order-service".to_string(),
            aud: vec!["coord-agent".to_string()],
            iat: 1719990000,
            exp: 1719993600,
            roles: vec!["root".to_string()],
            scope_overrides: HashMap::new(),
        }
    }

    #[test]
    fn test_cct_ed25519_roundtrip() {
        let signing_key = SigningKey::from_bytes(&[7u8; 32]);
        let header = CctHeader::ed25519();
        let token = encode_cct_ed25519(&header, &ed_test_payload(), &signing_key).unwrap();
        assert!(token.starts_with("eyJ"));

        let pub_key = signing_key.verifying_key().to_bytes();
        let decoded = decode_cct_ed25519(&token, &pub_key).unwrap();
        assert_eq!(decoded.header.alg, CCT_ALG_ED25519);
        assert_eq!(decoded.payload.roles, vec!["root".to_string()]);

        // decode_cct_any 双算法入口同样可用
        let decoded2 = decode_cct_any(&token, &[], Some(&pub_key)).unwrap();
        assert_eq!(decoded2.payload.jti, "ed-token-1");
    }

    #[test]
    fn test_cct_ed25519_rejects_wrong_public_key() {
        let signing_key = SigningKey::from_bytes(&[7u8; 32]);
        let header = CctHeader::ed25519();
        let token = encode_cct_ed25519(&header, &ed_test_payload(), &signing_key).unwrap();

        // 另一把公钥无法验证
        let other = SigningKey::from_bytes(&[8u8; 32])
            .verifying_key()
            .to_bytes();
        assert!(decode_cct_ed25519(&token, &other).is_err());
    }

    #[test]
    fn test_cct_ed25519_rejects_tampered_payload() {
        let signing_key = SigningKey::from_bytes(&[7u8; 32]);
        let header = CctHeader::ed25519();
        let token = encode_cct_ed25519(&header, &ed_test_payload(), &signing_key).unwrap();
        let parts: Vec<&str> = token.split('.').collect();
        let tampered_payload = base64_url_encode(
            br#"{"jti":"x","iss":"x","sub":"x","aud":[],"iat":1719990000,"exp":1719993600,"roles":["admin"]}"#,
        );
        let tampered = format!("{}.{}.{}", parts[0], tampered_payload, parts[2]);
        let pub_key = signing_key.verifying_key().to_bytes();
        assert!(decode_cct_ed25519(&tampered, &pub_key).is_err());
    }

    #[test]
    fn test_cct_alg_cross_rejection() {
        // HMAC token 不能被 Ed25519 公钥验证（alg 分发）
        let header = CctHeader::default();
        let hmac_token = encode_cct(&header, &ed_test_payload(), TEST_KEY).unwrap();
        let pub_key = SigningKey::from_bytes(&[7u8; 32])
            .verifying_key()
            .to_bytes();
        assert!(decode_cct_ed25519(&hmac_token, &pub_key).is_err());

        // Ed25519 token 不能被 decode_cct（仅 HMAC key）验证
        let signing_key = SigningKey::from_bytes(&[7u8; 32]);
        let ed_token =
            encode_cct_ed25519(&CctHeader::ed25519(), &ed_test_payload(), &signing_key).unwrap();
        assert!(decode_cct(&ed_token, TEST_KEY).is_err());

        // 未配置公钥时 Ed25519 token 拒绝
        assert!(decode_cct_any(&ed_token, &[], None).is_err());

        // 未知 alg 拒绝
        let mut bad_header = CctHeader::ed25519();
        bad_header.alg = "RS256".to_string();
        let bad_token = encode_cct_ed25519(&bad_header, &ed_test_payload(), &signing_key);
        assert!(bad_token.is_err());
    }

    // ──── A3：空 / 过短 HMAC 密钥必须被拒（P0 回归固化） ────

    #[test]
    fn test_cct_hmac_empty_key_is_refused_not_accepted() {
        // 攻击者视角：知道"密钥为空"即可自签 root token——历史上这**会成功**。
        let header = CctHeader::default();
        let forged = encode_cct(&header, &ed_test_payload(), TEST_KEY).unwrap();
        // 空 key 验签必须失败（且原因必须是"无可用密钥材料"，不是签名不匹配）
        let err = decode_cct_any(&forged, &[&[]], None).expect_err("empty key must not verify");
        assert!(
            format!("{err}").contains("no usable HMAC key"),
            "expected no-usable-key error, got: {err}"
        );
        // 传空 key 切片（agent 只配公钥时的真实形态）同样拒绝
        assert!(decode_cct_any(&forged, &[], None).is_err());
        // 过短密钥（<32B）同样不构成可用材料
        assert!(decode_cct_any(&forged, &[b"short"], None).is_err());
        // 正确的 32B 密钥仍然可用（不误伤正常路径）
        assert!(decode_cct_any(&forged, &[TEST_KEY], None).is_ok());
    }

    #[test]
    fn test_cct_encode_with_empty_key_is_refused() {
        let header = CctHeader::default();
        let err =
            encode_cct(&header, &ed_test_payload(), &[]).expect_err("empty key must not sign");
        assert!(format!("{err}").contains("A3"), "err: {err}");
        assert!(encode_cct(&header, &ed_test_payload(), b"short").is_err());
        assert!(encode_cct(&header, &ed_test_payload(), TEST_KEY).is_ok());
    }
}
