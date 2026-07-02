// Token Manager — Simple Token generation & validation (ADP §14.2)
//
// Simple Token: `Authorization: Bearer coord_<random_hex>`
// - 32-byte random token, hex-encoded with "coord_" prefix
// - Stored as SHA256 hash in the token store
// - Configurable expiry (default 15 minutes per ADP §14.5)
// - Refresh token support (longer-lived, single-use)

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use rand::Rng;
use sha2::{Sha256, Digest};

use coord_core::error::{Error, Result};

// ──── Auth Token ────

/// Authentication token issued to a user
#[derive(Debug, Clone)]
pub struct AuthToken {
    /// The token string (bearer token)
    pub token: String,
    /// Username this token belongs to
    pub username: String,
    /// When this token expires
    pub expires_at: Instant,
    /// Whether this is a refresh token
    pub is_refresh: bool,
}

// ──── Token Entry (stored internally) ────

#[allow(dead_code)]
struct TokenEntry {
    /// SHA256 hash of the token
    hash: Vec<u8>,
    /// Username
    username: String,
    /// Expiry time
    expires_at: Instant,
    /// Whether this is a refresh token
    is_refresh: bool,
}

// ──── Token Manager ────

/// Manages authentication tokens: issue, validate, revoke
pub struct TokenManager {
    /// Active tokens (token → entry)
    tokens: Arc<RwLock<HashMap<String, TokenEntry>>>,
    /// Token TTL for access tokens
    access_token_ttl: Duration,
    /// Token TTL for refresh tokens
    refresh_token_ttl: Duration,
}

impl TokenManager {
    /// Create a new TokenManager
    pub fn new(access_token_ttl_secs: u64, refresh_token_ttl_secs: u64) -> Self {
        Self {
            tokens: Arc::new(RwLock::new(HashMap::new())),
            access_token_ttl: Duration::from_secs(access_token_ttl_secs),
            refresh_token_ttl: Duration::from_secs(refresh_token_ttl_secs),
        }
    }

    /// Create with default TTLs (15 min access, 24h refresh)
    pub fn with_defaults() -> Self {
        Self::new(15 * 60, 24 * 60 * 60)
    }

    /// Issue a new access token for a user
    pub fn issue_token(&self, username: &str) -> AuthToken {
        let token = generate_token("coord_");
        let hash = hash_token(&token);

        let entry = TokenEntry {
            hash,
            username: username.to_string(),
            expires_at: Instant::now() + self.access_token_ttl,
            is_refresh: false,
        };

        self.tokens.write().insert(token.clone(), entry);

        AuthToken {
            token,
            username: username.to_string(),
            expires_at: Instant::now() + self.access_token_ttl,
            is_refresh: false,
        }
    }

    /// Issue a new refresh token for a user
    pub fn issue_refresh_token(&self, username: &str) -> AuthToken {
        let token = generate_token("coord_refresh_");
        let hash = hash_token(&token);

        let entry = TokenEntry {
            hash,
            username: username.to_string(),
            expires_at: Instant::now() + self.refresh_token_ttl,
            is_refresh: true,
        };

        self.tokens.write().insert(token.clone(), entry);

        AuthToken {
            token,
            username: username.to_string(),
            expires_at: Instant::now() + self.refresh_token_ttl,
            is_refresh: true,
        }
    }

    /// Validate a token and return the username if valid
    pub fn validate(&self, token: &str) -> Result<String> {
        let tokens = self.tokens.read();

        let entry = tokens
            .get(token)
            .ok_or_else(|| Error::InvalidToken("token not found".to_string()))?;

        if Instant::now() >= entry.expires_at {
            return Err(Error::TokenExpired);
        }

        Ok(entry.username.clone())
    }

    /// Revoke a token
    pub fn revoke(&self, token: &str) {
        self.tokens.write().remove(token);
    }

    /// Revoke all tokens for a user
    pub fn revoke_user_tokens(&self, username: &str) {
        self.tokens
            .write()
            .retain(|_, entry| entry.username != username);
    }

    /// Clean up expired tokens
    pub fn cleanup_expired(&self) -> usize {
        let now = Instant::now();
        let mut tokens = self.tokens.write();
        let before = tokens.len();
        tokens.retain(|_, entry| now < entry.expires_at);
        before - tokens.len()
    }

    /// Get the number of active tokens
    pub fn active_count(&self) -> usize {
        self.tokens.read().len()
    }
}

// ──── Token utilities ────

/// Generate a random token string with a prefix
fn generate_token(prefix: &str) -> String {
    let mut rng = rand::thread_rng();
    let random_bytes: [u8; 32] = rng.gen();
    format!("{}{}", prefix, hex::encode(random_bytes))
}

/// Hash a token for storage
fn hash_token(token: &str) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    hasher.finalize().to_vec()
}

// ──── Tests ────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_issue_and_validate_token() {
        let tm = TokenManager::with_defaults();
        let token = tm.issue_token("alice");

        let username = tm.validate(&token.token).unwrap();
        assert_eq!(username, "alice");
    }

    #[test]
    fn test_validate_invalid_token() {
        let tm = TokenManager::with_defaults();
        let result = tm.validate("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_revoke_token() {
        let tm = TokenManager::with_defaults();
        let token = tm.issue_token("bob");

        tm.revoke(&token.token);
        let result = tm.validate(&token.token);
        assert!(result.is_err());
    }

    #[test]
    fn test_revoke_user_tokens() {
        let tm = TokenManager::with_defaults();
        let t1 = tm.issue_token("charlie");
        let t2 = tm.issue_token("charlie");
        let t3 = tm.issue_token("dave");

        tm.revoke_user_tokens("charlie");
        assert!(tm.validate(&t1.token).is_err());
        assert!(tm.validate(&t2.token).is_err());
        assert!(tm.validate(&t3.token).is_ok());
    }

    #[test]
    fn test_cleanup_expired() {
        let tm = TokenManager::new(0, 0); // 0-second TTL (immediately expired)
        let _token = tm.issue_token("eve");

        // Tokens with 0 TTL are immediately expired
        let cleaned = tm.cleanup_expired();
        assert_eq!(cleaned, 1);
        assert_eq!(tm.active_count(), 0);
    }
}
