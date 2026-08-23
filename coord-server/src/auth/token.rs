// Token Manager — Simple Token generation & validation (ADP §14.2)
//
// Simple Token: `Authorization: Bearer coord_<random_hex>`
// - 32-byte random token, hex-encoded with "coord_" prefix
// - Stored as SHA256 hash in the token store
// - Configurable expiry (default 15 minutes per ADP §14.5)
// - Refresh token support (longer-lived, single-use)

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use rand::Rng;
use sha2::{Digest, Sha256};

use coord_core::error::{Error, Result};

/// P2-07：当前墙钟 unix 秒（会话过期判定统一口径）
pub fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ──── Auth Token ────

/// Authentication token issued to a user
#[derive(Debug, Clone)]
pub struct AuthToken {
    /// The token string (bearer token)
    pub token: String,
    /// SHA256 hex of the token（持久化会话键，P2-07）
    pub hash_hex: String,
    /// Username this token belongs to
    pub username: String,
    /// When this token expires（墙钟 unix 秒，P2-07：重启后仍有效）
    pub expires_at_unix: u64,
    /// When this token expires（进程内单调钟，仅展示/测试用）
    pub expires_at: Instant,
    /// Whether this is a refresh token
    pub is_refresh: bool,
}

// ──── Token Entry (stored internally) ────

#[allow(dead_code)]
struct TokenEntry {
    /// Username
    username: String,
    /// Expiry time（墙钟 unix 秒）
    expires_at_unix: u64,
    /// Whether this is a refresh token
    is_refresh: bool,
}

// ──── Token Manager ────

/// Manages authentication tokens: issue, validate, revoke
///
/// P2-07：内部表以 token 的 SHA256 hex 为键（明文 token 不落内存表键），
/// 过期判定用墙钟时间——经 `register_session`/`load_sessions` 可从
/// `/_sys/auth/sessions/` 持久化状态重建，重启不失效。
pub struct TokenManager {
    /// Active tokens (hash_hex → entry)
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
        self.issue(username, false)
    }

    /// Issue a new refresh token for a user
    pub fn issue_refresh_token(&self, username: &str) -> AuthToken {
        self.issue(username, true)
    }

    fn issue(&self, username: &str, is_refresh: bool) -> AuthToken {
        let token = generate_token(if is_refresh {
            "coord_refresh_"
        } else {
            "coord_"
        });
        let hash = hash_token(&token);
        let hash_hex = hex::encode(&hash);
        let ttl = if is_refresh {
            self.refresh_token_ttl
        } else {
            self.access_token_ttl
        };
        let expires_at_unix = now_unix() + ttl.as_secs();

        self.tokens.write().insert(
            hash_hex.clone(),
            TokenEntry {
                username: username.to_string(),
                expires_at_unix,
                is_refresh,
            },
        );

        AuthToken {
            token,
            hash_hex,
            username: username.to_string(),
            expires_at_unix,
            expires_at: Instant::now() + ttl,
            is_refresh,
        }
    }

    /// Validate a token and return the username if valid
    pub fn validate(&self, token: &str) -> Result<String> {
        let hash_hex = hex::encode(hash_token(token));
        let tokens = self.tokens.read();

        let entry = tokens
            .get(&hash_hex)
            .ok_or_else(|| Error::InvalidToken("token not found".to_string()))?;

        if now_unix() >= entry.expires_at_unix {
            return Err(Error::TokenExpired);
        }

        Ok(entry.username.clone())
    }

    /// Revoke a token
    pub fn revoke(&self, token: &str) {
        let hash_hex = hex::encode(hash_token(token));
        self.tokens.write().remove(&hash_hex);
    }

    /// Revoke all tokens for a user
    pub fn revoke_user_tokens(&self, username: &str) {
        self.tokens
            .write()
            .retain(|_, entry| entry.username != username);
    }

    /// Clean up expired tokens
    pub fn cleanup_expired(&self) -> usize {
        let now = now_unix();
        let mut tokens = self.tokens.write();
        let before = tokens.len();
        tokens.retain(|_, entry| now < entry.expires_at_unix);
        before - tokens.len()
    }

    /// Get the number of active tokens
    pub fn active_count(&self) -> usize {
        self.tokens.read().len()
    }

    // ──── P2-07：会话持久化接口 ────

    /// 注册持久化会话（raft apply 钩子与启动装载共用）。
    pub fn register_session(
        &self,
        hash_hex: &str,
        username: &str,
        expires_at_unix: u64,
        is_refresh: bool,
    ) {
        self.tokens.write().insert(
            hash_hex.to_string(),
            TokenEntry {
                username: username.to_string(),
                expires_at_unix,
                is_refresh,
            },
        );
    }

    /// 删除会话（refresh 消费/登出/吊销，raft apply 钩子共用）。
    pub fn remove_session(&self, hash_hex: &str) {
        self.tokens.write().remove(hash_hex);
    }

    /// 查询 refresh token 的会话信息（校验存在、未过期、确为 refresh token）。
    pub fn session_of_refresh(&self, token: &str) -> Result<(String, String, u64)> {
        let hash_hex = hex::encode(hash_token(token));
        let tokens = self.tokens.read();
        let entry = tokens
            .get(&hash_hex)
            .ok_or_else(|| Error::InvalidToken("refresh token not found".to_string()))?;
        if !entry.is_refresh {
            return Err(Error::InvalidToken("not a refresh token".to_string()));
        }
        if now_unix() >= entry.expires_at_unix {
            return Err(Error::TokenExpired);
        }
        Ok((hash_hex, entry.username.clone(), entry.expires_at_unix))
    }

    /// 启动装载：从 `/_sys/auth/sessions/` 前缀条目重建会话表（重启不失效）。
    pub fn load_sessions(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) {
        use crate::auth::manager::{AuthSessionRecord, AUTH_SESSION_PREFIX};
        let mut tokens = self.tokens.write();
        for (key, value) in entries {
            if !key.starts_with(AUTH_SESSION_PREFIX) {
                continue;
            }
            if let Some(rec) = AuthSessionRecord::from_bytes(&value) {
                let hash_hex = String::from_utf8_lossy(&key[AUTH_SESSION_PREFIX.len()..]);
                tokens.insert(
                    hash_hex.to_string(),
                    TokenEntry {
                        username: rec.username,
                        expires_at_unix: rec.expires_at_unix,
                        is_refresh: rec.is_refresh,
                    },
                );
            }
        }
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

    // ──── P2-07：会话持久化 ────

    #[test]
    fn test_session_rebuild_across_restart() {
        use crate::auth::manager::{AuthSessionRecord, AUTH_SESSION_PREFIX};
        let tm = TokenManager::with_defaults();
        let token = tm.issue_token("bob");

        // 模拟 raft apply 持久化形态，重启后新实例装载
        let key = [AUTH_SESSION_PREFIX, token.hash_hex.as_bytes()].concat();
        let rec = AuthSessionRecord {
            username: "bob".to_string(),
            expires_at_unix: token.expires_at_unix,
            is_refresh: false,
        };
        let tm2 = TokenManager::with_defaults();
        tm2.load_sessions(vec![(key, rec.to_bytes().unwrap())]);
        assert_eq!(tm2.validate(&token.token).unwrap(), "bob");
    }

    #[test]
    fn test_refresh_single_use_consume() {
        let tm = TokenManager::with_defaults();
        let refresh = tm.issue_refresh_token("alice");
        let (hash_hex, username, _exp) = tm.session_of_refresh(&refresh.token).unwrap();
        assert_eq!(username, "alice");
        assert_eq!(hash_hex, refresh.hash_hex);

        // 单次使用：apply 钩子删除后不可再查
        tm.remove_session(&hash_hex);
        assert!(tm.session_of_refresh(&refresh.token).is_err());
    }

    #[test]
    fn test_access_token_rejected_as_refresh() {
        let tm = TokenManager::with_defaults();
        let access = tm.issue_token("alice");
        assert!(tm.session_of_refresh(&access.token).is_err());
    }

    #[test]
    fn test_register_session_makes_token_valid() {
        let tm = TokenManager::with_defaults();
        let refresh = tm.issue_refresh_token("carol");
        // 新节点视图（apply 钩子）：按持久化信息注册
        let tm2 = TokenManager::with_defaults();
        tm2.register_session(&refresh.hash_hex, "carol", refresh.expires_at_unix, true);
        assert_eq!(tm2.validate(&refresh.token).unwrap(), "carol");
        assert!(tm2.session_of_refresh(&refresh.token).is_ok());
    }
}
