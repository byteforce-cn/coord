// Auth Endpoint Rate Limiter (Phase 3.6)
//
// IP-based rate limiting for the Agent's Authenticate (login) endpoint.
// Prevents brute-force attacks by limiting to 10 req/s per client IP.
// Exceeded requests return HTTP 429 Too Many Requests (gRPC RESOURCE_EXHAUSTED).
//
// See docs/capability-auth-implementation.md §3.6, §8.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

// ──── Configuration ────

/// Rate limit configuration for the login endpoint.
#[derive(Debug, Clone)]
pub struct LoginRateLimitConfig {
    /// Maximum requests per second per client IP
    pub max_requests_per_sec: u32,
    /// Burst size (maximum tokens in bucket)
    pub burst_size: u32,
    /// How long to retain inactive IP entries (cleanup interval)
    pub entry_ttl_secs: u64,
}

impl Default for LoginRateLimitConfig {
    fn default() -> Self {
        Self {
            max_requests_per_sec: 10,
            burst_size: 10,
            entry_ttl_secs: 300, // 5 minutes
        }
    }
}

// ──── Per-IP Token Bucket ────

/// Token bucket state for a single client IP.
#[derive(Debug)]
struct TokenBucket {
    /// Available tokens (fractional for smooth refill)
    tokens: f64,
    /// Last token refill timestamp
    last_refill: Instant,
}

impl TokenBucket {
    fn new(burst_size: u32) -> Self {
        Self {
            tokens: burst_size as f64,
            last_refill: Instant::now(),
        }
    }

    /// Refill tokens based on elapsed time.
    fn refill(&mut self, refill_rate: f64, burst_size: u32) {
        let elapsed = self.last_refill.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            self.tokens = (self.tokens + elapsed * refill_rate).min(burst_size as f64);
            self.last_refill = Instant::now();
        }
    }

    /// Try to consume one token. Returns true if successful.
    fn try_consume(&mut self, refill_rate: f64, burst_size: u32) -> bool {
        self.refill(refill_rate, burst_size);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

// ──── Login Rate Limiter ────

/// IP-based rate limiter for the Agent's Authenticate (login) endpoint.
///
/// Thread-safe. Uses per-IP token buckets with automatic cleanup of stale entries.
pub struct LoginRateLimiter {
    /// Per-IP token buckets
    buckets: Arc<Mutex<HashMap<String, TokenBucket>>>,
    /// Configuration
    config: LoginRateLimitConfig,
    /// Total allowed requests
    allowed_total: AtomicU64,
    /// Total denied requests
    denied_total: AtomicU64,
    /// Last cleanup timestamp
    last_cleanup: Arc<Mutex<Instant>>,
}

impl LoginRateLimiter {
    /// Create a new login rate limiter with default config (10 req/s).
    pub fn new() -> Self {
        Self::with_config(LoginRateLimitConfig::default())
    }

    /// Create a new login rate limiter with custom config.
    pub fn with_config(config: LoginRateLimitConfig) -> Self {
        Self {
            buckets: Arc::new(Mutex::new(HashMap::new())),
            config,
            allowed_total: AtomicU64::new(0),
            denied_total: AtomicU64::new(0),
            last_cleanup: Arc::new(Mutex::new(Instant::now())),
        }
    }

    /// Check if a request from the given client IP should be allowed.
    ///
    /// Returns `Ok(())` if within rate limit, or `Err(RateLimitExceeded)` if exceeded.
    pub fn check(&self, client_ip: &str) -> Result<(), RateLimitExceeded> {
        let mut buckets = self.buckets.lock();

        // Periodic cleanup of stale entries
        self.maybe_cleanup(&mut buckets);

        let bucket = buckets
            .entry(client_ip.to_string())
            .or_insert_with(|| TokenBucket::new(self.config.burst_size));

        let allowed = bucket.try_consume(
            self.config.max_requests_per_sec as f64,
            self.config.burst_size,
        );

        if allowed {
            self.allowed_total.fetch_add(1, Ordering::Relaxed);
            Ok(())
        } else {
            self.denied_total.fetch_add(1, Ordering::Relaxed);
            Err(RateLimitExceeded {
                client_ip: client_ip.to_string(),
                max_requests_per_sec: self.config.max_requests_per_sec,
                retry_after_secs: 1,
            })
        }
    }

    /// Get the number of active client IPs being tracked.
    pub fn active_clients(&self) -> usize {
        self.buckets.lock().len()
    }

    /// Get total allowed request count.
    pub fn allowed_count(&self) -> u64 {
        self.allowed_total.load(Ordering::Relaxed)
    }

    /// Get total denied request count.
    pub fn denied_count(&self) -> u64 {
        self.denied_total.load(Ordering::Relaxed)
    }

    /// Remove all tracked IP entries (for testing).
    pub fn reset(&self) {
        self.buckets.lock().clear();
        self.allowed_total.store(0, Ordering::Relaxed);
        self.denied_total.store(0, Ordering::Relaxed);
    }

    // ──── Internal ────

    /// Periodically remove entries from IPs that haven't been seen recently.
    fn maybe_cleanup(&self, buckets: &mut HashMap<String, TokenBucket>) {
        let mut last = self.last_cleanup.lock();
        if last.elapsed().as_secs() < self.config.entry_ttl_secs {
            return;
        }

        // Remove stale entries (buckets with full tokens that haven't been touched)
        let ttl = Duration::from_secs(self.config.entry_ttl_secs);
        let now = Instant::now();
        buckets.retain(|_, bucket| {
            // Only retain if bucket has been used recently or has less than full tokens
            now.duration_since(bucket.last_refill) < ttl
                || bucket.tokens < self.config.burst_size as f64
        });

        *last = Instant::now();
    }
}

impl Default for LoginRateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

// ──── Rate Limit Error ────

/// Rate limit exceeded error, returned to the client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateLimitExceeded {
    /// The client IP that exceeded the limit
    pub client_ip: String,
    /// The configured rate limit
    pub max_requests_per_sec: u32,
    /// Suggested retry-after duration in seconds
    pub retry_after_secs: u32,
}

impl std::fmt::Display for RateLimitExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "rate limit exceeded: {} req/s for IP {}, retry after {}s",
            self.max_requests_per_sec, self.client_ip, self.retry_after_secs
        )
    }
}

impl std::error::Error for RateLimitExceeded {}

// ──── Tests ────

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    // ──── Phase 3.6 TDD Tests ────

    #[test]
    fn test_rate_limiter_allows_requests_within_limit() {
        let limiter = LoginRateLimiter::new();

        // 10 requests from same IP should all be allowed (within burst)
        for i in 0..10 {
            assert!(
                limiter.check("192.168.1.1").is_ok(),
                "request {} should be allowed (within burst)",
                i + 1
            );
        }

        assert_eq!(limiter.allowed_count(), 10);
        assert_eq!(limiter.denied_count(), 0);
    }

    #[test]
    fn test_rate_limiter_denies_requests_exceeding_limit() {
        let limiter = LoginRateLimiter::with_config(LoginRateLimitConfig {
            max_requests_per_sec: 10,
            burst_size: 10,
            entry_ttl_secs: 300,
        });

        // Exhaust burst
        for _ in 0..10 {
            assert!(limiter.check("192.168.1.1").is_ok());
        }

        // 11th request should be denied
        let result = limiter.check("192.168.1.1");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.client_ip, "192.168.1.1");
        assert_eq!(err.max_requests_per_sec, 10);
        assert_eq!(err.retry_after_secs, 1);
        assert_eq!(limiter.denied_count(), 1);
    }

    #[test]
    fn test_rate_limiter_independent_per_ip() {
        let limiter = LoginRateLimiter::with_config(LoginRateLimitConfig {
            max_requests_per_sec: 10,
            burst_size: 10,
            entry_ttl_secs: 300,
        });

        // Exhaust IP 1
        for _ in 0..10 {
            assert!(limiter.check("192.168.1.1").is_ok());
        }
        assert!(limiter.check("192.168.1.1").is_err());

        // IP 2 should still be allowed
        assert!(limiter.check("192.168.1.2").is_ok());
        assert!(limiter.check("192.168.1.2").is_ok());

        assert_eq!(limiter.active_clients(), 2);
    }

    #[test]
    fn test_rate_limiter_refills_over_time() {
        let limiter = LoginRateLimiter::with_config(LoginRateLimitConfig {
            max_requests_per_sec: 100, // 100 tokens/sec = fast refill
            burst_size: 10,
            entry_ttl_secs: 300,
        });

        // Exhaust burst
        for _ in 0..10 {
            assert!(limiter.check("192.168.1.1").is_ok());
        }
        assert!(limiter.check("192.168.1.1").is_err());

        // Wait for refill (100 tokens/sec, need 1 token = ~10ms)
        thread::sleep(Duration::from_millis(50));

        // Should now have ~5 tokens, enough for at least 1 request
        assert!(
            limiter.check("192.168.1.1").is_ok(),
            "should allow request after refill"
        );
    }

    #[test]
    fn test_rate_limiter_single_ip_high_burst() {
        let limiter = LoginRateLimiter::with_config(LoginRateLimitConfig {
            max_requests_per_sec: 10,
            burst_size: 3, // small burst
            entry_ttl_secs: 300,
        });

        // 3 requests allowed (burst size)
        for _ in 0..3 {
            assert!(limiter.check("10.0.0.1").is_ok());
        }
        // 4th denied
        assert!(limiter.check("10.0.0.1").is_err());
    }

    #[test]
    fn test_rate_limiter_multiple_ips_exhausted() {
        let limiter = LoginRateLimiter::with_config(LoginRateLimitConfig {
            max_requests_per_sec: 5,
            burst_size: 5,
            entry_ttl_secs: 300,
        });

        let ips = ["10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4", "10.0.0.5"];

        // Exhaust all IPs
        for ip in &ips {
            for _ in 0..5 {
                assert!(limiter.check(ip).is_ok(), "IP {} within limit", ip);
            }
            assert!(limiter.check(ip).is_err(), "IP {} exceeded", ip);
        }

        assert_eq!(limiter.active_clients(), 5);
        assert_eq!(limiter.allowed_count(), 25);
        assert_eq!(limiter.denied_count(), 5);
    }

    #[test]
    fn test_rate_limiter_reset_clears_state() {
        let limiter = LoginRateLimiter::new();

        // Exhaust
        for _ in 0..10 {
            let _ = limiter.check("192.168.1.1");
        }
        let _ = limiter.check("192.168.1.1"); // denied
        assert!(limiter.denied_count() > 0);

        // Reset
        limiter.reset();

        assert_eq!(limiter.active_clients(), 0);
        assert_eq!(limiter.allowed_count(), 0);
        assert_eq!(limiter.denied_count(), 0);

        // Should work again after reset
        assert!(limiter.check("192.168.1.1").is_ok());
    }

    #[test]
    fn test_rate_limiter_default_config_matches_spec() {
        let limiter = LoginRateLimiter::new();
        // 10 req/s per IP as specified in the design doc §3.6 & §8
        let config = LoginRateLimitConfig::default();
        assert_eq!(config.max_requests_per_sec, 10);
        assert_eq!(config.burst_size, 10);

        // Verify we can make exactly 10 and get denied on 11th
        for _ in 0..10 {
            assert!(limiter.check("192.168.1.100").is_ok());
        }
        assert!(limiter.check("192.168.1.100").is_err());
    }

    #[test]
    fn test_rate_limiter_error_display() {
        let err = RateLimitExceeded {
            client_ip: "10.0.0.1".to_string(),
            max_requests_per_sec: 10,
            retry_after_secs: 1,
        };
        let msg = format!("{err}");
        assert!(msg.contains("10.0.0.1"));
        assert!(msg.contains("10 req/s"));
        assert!(msg.contains("retry after 1s"));
    }

    #[test]
    fn test_rate_limiter_ipv6_addresses() {
        let limiter = LoginRateLimiter::new();

        // IPv6 should work the same
        for _ in 0..10 {
            assert!(limiter.check("::1").is_ok());
        }
        assert!(limiter.check("::1").is_err());

        // Another IPv6
        assert!(limiter.check("2001:db8::1").is_ok());
    }
}
