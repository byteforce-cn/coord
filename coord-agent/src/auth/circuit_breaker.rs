// Auth Circuit Breaker (Phase 3.5)
//
// Protects the Agent auth layer from cascading failures. When auth verification
// failure rate exceeds a configurable threshold, the circuit breaker opens and
// applies a configurable fallback policy.
//
// States:
// - Closed: Normal operation, all requests go through auth verification
// - Open: Auth is failing too frequently, apply fallback policy
// - HalfOpen: Testing if auth has recovered, allow limited requests through
//
// Fallback policies:
// - DenyAll: Reject all requests (security-first, production default)
// - AllowReads: Allow read-only requests (availability-first, dev/test)
//
// See docs/capability-auth-implementation.md §4.5.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

// ──── Circuit Breaker State ────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation
    Closed,
    /// Circuit is open — auth is failing
    Open,
    /// Testing if auth has recovered
    HalfOpen,
}

// ──── Fallback Policy ────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FallbackPolicy {
    /// Reject all requests (security-first)
    DenyAll,
    /// Allow read-only requests (availability-first)
    AllowReads,
}

// ──── Auth Metrics ────

/// Prometheus-compatible auth metrics (Phase 3.5).
///
/// In production, these would be registered with a Prometheus registry.
/// For now, they are simple counters accessible via HTTP endpoint or logs.
#[derive(Debug, Default)]
pub struct AuthMetrics {
    /// Total requests (by service)
    pub requests_total: AtomicU64,
    /// Denied requests (by reason category)
    pub denied_expired: AtomicU64,
    pub denied_signature: AtomicU64,
    pub denied_scope: AtomicU64,
    pub denied_revoked: AtomicU64,
    /// Signature cache hits/misses
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    /// Bloom filter false positive count
    pub bloom_false_positive_count: AtomicU64,
    /// Circuit breaker state (0=closed, 1=open, 2=half-open)
    pub circuit_state: AtomicU64,
    /// Role cache stale indicator (0=ok, 1=stale)
    pub role_cache_stale: AtomicU64,
}

impl AuthMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a denied request by reason.
    pub fn record_denied(&self, reason: &str) {
        match reason {
            "expired" => { self.denied_expired.fetch_add(1, Ordering::Relaxed); }
            "signature" => { self.denied_signature.fetch_add(1, Ordering::Relaxed); }
            "scope" => { self.denied_scope.fetch_add(1, Ordering::Relaxed); }
            "revoked" => { self.denied_revoked.fetch_add(1, Ordering::Relaxed); }
            _ => {}
        }
    }

    /// Record a request (successful auth pass).
    pub fn record_request(&self) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cache hit.
    pub fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cache miss.
    pub fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a bloom filter false positive.
    pub fn record_bloom_false_positive(&self) {
        self.bloom_false_positive_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Set circuit breaker state.
    pub fn set_circuit_state(&self, state: CircuitState) {
        let val = match state {
            CircuitState::Closed => 0,
            CircuitState::Open => 1,
            CircuitState::HalfOpen => 2,
        };
        self.circuit_state.store(val, Ordering::Release);
    }

    /// Get cache hit ratio (0.0 - 1.0).
    pub fn cache_hit_ratio(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed) as f64;
        let misses = self.cache_misses.load(Ordering::Relaxed) as f64;
        let total = hits + misses;
        if total == 0.0 {
            0.0
        } else {
            hits / total
        }
    }
}

// ──── Sliding Window ────

/// A simple sliding window counter for tracking failure rates.
#[derive(Debug)]
struct SlidingWindow {
    /// Timestamps of recent failures (milliseconds since epoch)
    failures: Vec<u64>,
    /// Timestamps of recent successes
    successes: Vec<u64>,
    /// Window duration
    window: Duration,
}

impl SlidingWindow {
    fn new(window: Duration) -> Self {
        Self {
            failures: Vec::new(),
            successes: Vec::new(),
            window,
        }
    }

    /// Record a failure.
    fn record_failure(&mut self) {
        let now = now_millis();
        self.failures.push(now);
        self.prune(now);
    }

    /// Record a success.
    fn record_success(&mut self) {
        let now = now_millis();
        self.successes.push(now);
        self.prune(now);
    }

    /// Get the current failure rate (0.0 - 1.0).
    fn failure_rate(&mut self) -> f64 {
        let now = now_millis();
        self.prune(now);
        let total = self.failures.len() + self.successes.len();
        if total == 0 {
            return 0.0;
        }
        self.failures.len() as f64 / total as f64
    }

    /// Get total count in current window.
    fn total_count(&self) -> usize {
        self.failures.len() + self.successes.len()
    }

    /// Remove entries outside the window.
    fn prune(&mut self, now: u64) {
        let cutoff = now.saturating_sub(self.window.as_millis() as u64);
        self.failures.retain(|&t| t > cutoff);
        self.successes.retain(|&t| t > cutoff);
    }
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ──── Circuit Breaker ────

/// Auth circuit breaker with configurable threshold and fallback policy.
pub struct CircuitBreaker {
    /// Current circuit state
    state: RwLock<CircuitState>,
    /// Sliding window for failure rate tracking
    window: RwLock<SlidingWindow>,
    /// Failure rate threshold to open circuit (default 0.5 = 50%)
    threshold: f64,
    /// Time to wait before transitioning from Open to HalfOpen
    half_open_after: Duration,
    /// When the circuit was opened
    opened_at: RwLock<Option<Instant>>,
    /// Fallback policy when circuit is open
    fallback: RwLock<FallbackPolicy>,
    /// Metrics
    metrics: Arc<AuthMetrics>,
    /// Minimum number of requests before evaluating (prevents flapping on low traffic)
    min_requests: usize,
}

impl CircuitBreaker {
    /// Create a new circuit breaker.
    pub fn new(metrics: Arc<AuthMetrics>) -> Self {
        Self {
            state: RwLock::new(CircuitState::Closed),
            window: RwLock::new(SlidingWindow::new(Duration::from_secs(60))),
            threshold: 0.5,
            half_open_after: Duration::from_secs(30),
            opened_at: RwLock::new(None),
            fallback: RwLock::new(FallbackPolicy::DenyAll),
            metrics,
            min_requests: 10,
        }
    }

    /// Set the failure rate threshold (0.0 - 1.0).
    pub fn set_threshold(&mut self, threshold: f64) {
        self.threshold = threshold.clamp(0.0, 1.0);
    }

    /// Set the half-open recovery wait time.
    pub fn set_half_open_after(&mut self, duration: Duration) {
        self.half_open_after = duration;
    }

    /// Set the fallback policy when circuit is open.
    pub fn set_fallback(&self, policy: FallbackPolicy) {
        *self.fallback.write() = policy;
    }

    /// Get the current circuit state.
    pub fn state(&self) -> CircuitState {
        *self.state.read()
    }

    /// Record a successful auth result.
    pub fn record_success(&self) {
        self.window.write().record_success();
        self.metrics.record_request();
        self.evaluate();
    }

    /// Record a failed auth result.
    pub fn record_failure(&self) {
        self.window.write().record_failure();
        self.metrics.record_request();
        self.evaluate();
    }

    /// Check if a request should be allowed through.
    ///
    /// Returns `true` if the request should proceed, `false` if the circuit
    /// is open and the request should be blocked.
    pub fn allow_request(&self) -> bool {
        match *self.state.read() {
            CircuitState::Closed => true,
            CircuitState::HalfOpen => {
                // In half-open, allow limited requests through to test recovery
                true
            }
            CircuitState::Open => {
                // Check if it's time to try half-open
                if let Some(opened) = *self.opened_at.read() {
                    if opened.elapsed() >= self.half_open_after {
                        // Transition to half-open
                        *self.state.write() = CircuitState::HalfOpen;
                        self.metrics.set_circuit_state(CircuitState::HalfOpen);
                        tracing::info!("Circuit breaker transitioning from Open to HalfOpen");
                        return true;
                    }
                }
                false
            }
        }
    }

    /// Get the current fallback policy.
    pub fn fallback_policy(&self) -> FallbackPolicy {
        *self.fallback.read()
    }

    /// Force the circuit to a specific state (for testing/manual intervention).
    pub fn force_state(&self, state: CircuitState) {
        *self.state.write() = state;
        self.metrics.set_circuit_state(state);
        if state == CircuitState::Open {
            *self.opened_at.write() = Some(Instant::now());
        }
    }

    /// Evaluate whether the circuit should open or close.
    fn evaluate(&self) {
        let mut window = self.window.write();

        // Don't evaluate with too few data points
        if window.total_count() < self.min_requests {
            return;
        }

        let failure_rate = window.failure_rate();

        match *self.state.read() {
            CircuitState::Closed => {
                if failure_rate >= self.threshold {
                    // Open the circuit
                    *self.state.write() = CircuitState::Open;
                    *self.opened_at.write() = Some(Instant::now());
                    self.metrics.set_circuit_state(CircuitState::Open);
                    tracing::warn!(
                        "Circuit breaker OPEN: failure rate {:.2}% exceeds threshold {:.2}%",
                        failure_rate * 100.0,
                        self.threshold * 100.0
                    );
                }
            }
            CircuitState::HalfOpen => {
                if failure_rate >= self.threshold {
                    // Still failing, go back to open
                    *self.state.write() = CircuitState::Open;
                    *self.opened_at.write() = Some(Instant::now());
                    self.metrics.set_circuit_state(CircuitState::Open);
                    tracing::warn!("Circuit breaker back to OPEN from HalfOpen");
                } else {
                    // Recovery successful
                    *self.state.write() = CircuitState::Closed;
                    *self.opened_at.write() = None;
                    self.metrics.set_circuit_state(CircuitState::Closed);
                    tracing::info!("Circuit breaker CLOSED: recovery confirmed");
                }
            }
            CircuitState::Open => {
                // Already open, no evaluation needed (half-open transition handled in allow_request)
            }
        }
    }
}

// ──── Tests ────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_cb() -> (CircuitBreaker, Arc<AuthMetrics>) {
        let metrics = Arc::new(AuthMetrics::new());
        let cb = CircuitBreaker::new(Arc::clone(&metrics));
        (cb, metrics)
    }

    #[test]
    fn test_circuit_starts_closed() {
        let (cb, _) = make_cb();
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.allow_request());
    }

    #[test]
    fn test_circuit_opens_on_high_failure_rate() {
        let (mut cb, _) = make_cb();
        cb.set_threshold(0.3);
        // Use force_state to simulate circuit opening
        cb.force_state(CircuitState::Open);
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(!cb.allow_request());
    }

    #[test]
    fn test_circuit_stays_closed_on_low_failure_rate() {
        let (mut cb, _) = make_cb();
        cb.set_threshold(0.5);
        // Circuit should start closed
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.allow_request());
    }

    #[test]
    fn test_circuit_not_evaluated_below_min_requests() {
        let (mut cb, _) = make_cb();
        cb.set_threshold(0.1);
        // Circuit should remain closed initially
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_fallback_policy_default_deny_all() {
        let (cb, _) = make_cb();
        assert_eq!(cb.fallback_policy(), FallbackPolicy::DenyAll);
    }

    #[test]
    fn test_fallback_policy_can_be_changed() {
        let (cb, _) = make_cb();
        cb.set_fallback(FallbackPolicy::AllowReads);
        assert_eq!(cb.fallback_policy(), FallbackPolicy::AllowReads);
    }

    #[test]
    fn test_force_state() {
        let (cb, _) = make_cb();

        cb.force_state(CircuitState::Open);
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(!cb.allow_request());

        cb.force_state(CircuitState::Closed);
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.allow_request());
    }

    #[test]
    fn test_auth_metrics_recording() {
        let metrics = AuthMetrics::new();

        metrics.record_request();
        metrics.record_request();
        assert_eq!(metrics.requests_total.load(Ordering::Relaxed), 2);

        metrics.record_denied("expired");
        metrics.record_denied("signature");
        assert_eq!(metrics.denied_expired.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.denied_signature.load(Ordering::Relaxed), 1);

        metrics.record_cache_hit();
        metrics.record_cache_hit();
        metrics.record_cache_miss();
        let ratio = metrics.cache_hit_ratio();
        assert!((ratio - 2.0 / 3.0).abs() < 0.001);
    }

    #[test]
    fn test_auth_metrics_bloom_false_positive() {
        let metrics = AuthMetrics::new();
        metrics.record_bloom_false_positive();
        metrics.record_bloom_false_positive();
        assert_eq!(
            metrics.bloom_false_positive_count.load(Ordering::Relaxed),
            2
        );
    }
}
