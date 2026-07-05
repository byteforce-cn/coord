// Auth Sync Scheduler (Phase 3.3)
//
// Orchestrates periodic synchronization between Agent and Server:
// - Role mapping: full sync every 5 minutes (configurable)
// - Revocation delta: incremental sync every 10 seconds
// - High-sensitivity role detection: forces server lookup each request
//
// See docs/capability-auth-implementation.md §3.2, §3.4, §6.3.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use super::role_cache::{RoleCache, RoleEntry};

// ──── Sync Configuration ────

/// Configuration for the auth sync scheduler.
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Full role mapping sync interval (default: 5 minutes)
    pub role_sync_interval_secs: u64,
    /// Revocation delta sync interval (default: 10 seconds)
    pub revocation_sync_interval_secs: u64,
    /// Whether auto-sync is enabled
    pub auto_sync_enabled: bool,
    /// Maximum retries before logging a warning
    pub max_retries: u32,
    /// Retry backoff base duration
    pub retry_backoff_base_secs: u64,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            role_sync_interval_secs: 300, // 5 minutes
            revocation_sync_interval_secs: 10,
            auto_sync_enabled: true,
            max_retries: 3,
            retry_backoff_base_secs: 1,
        }
    }
}

// ──── Sync Stats ────

/// Statistics about synchronization operations.
#[derive(Debug, Clone, Default)]
pub struct SyncStats {
    /// Number of successful role syncs
    pub role_syncs_succeeded: u64,
    /// Number of failed role syncs
    pub role_syncs_failed: u64,
    /// Number of successful revocation delta syncs
    pub revocation_syncs_succeeded: u64,
    /// Number of failed revocation delta syncs
    pub revocation_syncs_failed: u64,
    /// Time of last successful role sync (Unix seconds)
    pub last_role_sync: i64,
    /// Time of last successful revocation sync (Unix seconds)
    pub last_revocation_sync: i64,
    /// Whether the sync loop is currently running
    pub is_running: bool,
}

// ──── Sync Scheduler ────

/// Manages periodic synchronization of role mappings and revocation lists.
///
/// This is designed to be run as a background task (tokio or dedicated thread).
/// It coordinates the sync intervals and retry logic.
pub struct SyncScheduler {
    /// Shared role cache to update
    role_cache: Arc<RoleCache>,
    /// Configuration
    config: SyncConfig,
    /// Sync statistics
    stats: Arc<Mutex<SyncStats>>,
    /// Stop signal for the sync loop
    stop_flag: Arc<AtomicBool>,
}

impl SyncScheduler {
    /// Create a new sync scheduler.
    pub fn new(role_cache: Arc<RoleCache>, config: SyncConfig) -> Self {
        Self {
            role_cache,
            config,
            stats: Arc::new(Mutex::new(SyncStats::default())),
            stop_flag: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Create with default configuration.
    pub fn with_defaults(role_cache: Arc<RoleCache>) -> Self {
        Self::new(role_cache, SyncConfig::default())
    }

    /// Get the role sync interval.
    pub fn role_sync_interval(&self) -> Duration {
        Duration::from_secs(self.config.role_sync_interval_secs)
    }

    /// Get the revocation sync interval.
    pub fn revocation_sync_interval(&self) -> Duration {
        Duration::from_secs(self.config.revocation_sync_interval_secs)
    }

    /// Get current sync statistics.
    pub fn stats(&self) -> SyncStats {
        self.stats.lock().clone()
    }

    /// Signal the sync loop to stop.
    pub fn stop(&self) {
        self.stop_flag.store(true, Ordering::Release);
    }

    /// Check if the sync loop is running.
    pub fn is_running(&self) -> bool {
        !self.stop_flag.load(Ordering::Acquire)
    }

    /// Determine the next sync action and its scheduled time.
    ///
    /// This helps the caller decide when to run the next sync. Returns
    /// `(action, delay_until)` where `action` describes what to sync.
    pub fn next_sync(&self) -> (SyncAction, Duration) {
        let now = Instant::now();
        let role_sync_interval = self.role_sync_interval();
        let rev_sync_interval = self.revocation_sync_interval();

        let last_role = self.role_cache.last_sync_time();

        // Calculate time since last role sync
        let time_since_role = if last_role > 0 {
            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            Duration::from_secs((now_secs - last_role).max(0) as u64)
        } else {
            Duration::MAX // Never synced — should sync immediately
        };

        let role_needed = time_since_role >= role_sync_interval;

        if role_needed {
            (SyncAction::RoleFullSync, Duration::ZERO)
        } else {
            let until_role = role_sync_interval.saturating_sub(time_since_role);
            let next = until_role.min(rev_sync_interval);
            (SyncAction::RevocationDelta, next)
        }
    }

    /// Record a successful role sync.
    pub fn record_role_sync_success(&self) {
        let mut stats = self.stats.lock();
        stats.role_syncs_succeeded += 1;
        stats.last_role_sync = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
    }

    /// Record a failed role sync.
    pub fn record_role_sync_failure(&self) {
        let mut stats = self.stats.lock();
        stats.role_syncs_failed += 1;
    }

    /// Record a successful revocation delta sync.
    pub fn record_revocation_sync_success(&self) {
        let mut stats = self.stats.lock();
        stats.revocation_syncs_succeeded += 1;
        stats.last_revocation_sync = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
    }

    /// Record a failed revocation delta sync.
    pub fn record_revocation_sync_failure(&self) {
        let mut stats = self.stats.lock();
        stats.revocation_syncs_failed += 1;
    }

    /// Calculate retry backoff for a given attempt number.
    pub fn retry_backoff(&self, attempt: u32) -> Duration {
        let base = Duration::from_secs(self.config.retry_backoff_base_secs);
        base * 2u32.pow(attempt.min(10))
    }
}

// ──── Sync Action ────

/// Describes what type of sync should be performed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncAction {
    /// Full role→capability mapping sync
    RoleFullSync,
    /// Incremental revocation delta sync
    RevocationDelta,
    /// No sync needed yet
    Idle,
}

// ──── Tests ────

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    // ──── Phase 3.3 TDD Tests ────

    #[test]
    fn test_sync_scheduler_default_config() {
        let cache = Arc::new(RoleCache::new());
        let scheduler = SyncScheduler::with_defaults(cache);

        assert_eq!(scheduler.role_sync_interval(), Duration::from_secs(300));
        assert_eq!(scheduler.revocation_sync_interval(), Duration::from_secs(10));
        assert!(scheduler.is_running());
    }

    #[test]
    fn test_sync_scheduler_custom_config() {
        let cache = Arc::new(RoleCache::new());
        let config = SyncConfig {
            role_sync_interval_secs: 60,
            revocation_sync_interval_secs: 5,
            auto_sync_enabled: true,
            max_retries: 5,
            retry_backoff_base_secs: 2,
        };
        let scheduler = SyncScheduler::new(cache, config);

        assert_eq!(scheduler.role_sync_interval(), Duration::from_secs(60));
        assert_eq!(scheduler.revocation_sync_interval(), Duration::from_secs(5));
    }

    #[test]
    fn test_sync_scheduler_initial_next_sync_is_role() {
        let cache = Arc::new(RoleCache::new());
        let scheduler = SyncScheduler::with_defaults(cache);

        // Cache is not initialized, so first sync should be role full sync
        let (action, delay) = scheduler.next_sync();
        assert_eq!(action, SyncAction::RoleFullSync);
        assert_eq!(delay, Duration::ZERO);
    }

    #[test]
    fn test_sync_scheduler_after_role_sync_next_is_revocation() {
        let cache = Arc::new(RoleCache::new());

        // Simulate having completed a role sync
        cache.sync_full(vec![]);

        let scheduler = SyncScheduler::with_defaults(cache);

        let (action, _) = scheduler.next_sync();
        // After role sync, next should be revocation delta (since role was just synced)
        assert_eq!(action, SyncAction::RevocationDelta);
    }

    #[test]
    fn test_sync_scheduler_stop_and_running() {
        let cache = Arc::new(RoleCache::new());
        let scheduler = SyncScheduler::with_defaults(cache);

        assert!(scheduler.is_running());
        scheduler.stop();
        assert!(!scheduler.is_running());
    }

    #[test]
    fn test_sync_stats_initial_state() {
        let cache = Arc::new(RoleCache::new());
        let scheduler = SyncScheduler::with_defaults(cache);

        let stats = scheduler.stats();
        assert_eq!(stats.role_syncs_succeeded, 0);
        assert_eq!(stats.role_syncs_failed, 0);
        assert_eq!(stats.revocation_syncs_succeeded, 0);
        assert_eq!(stats.revocation_syncs_failed, 0);
    }

    #[test]
    fn test_sync_stats_record_role_sync_success() {
        let cache = Arc::new(RoleCache::new());
        let scheduler = SyncScheduler::with_defaults(cache);

        scheduler.record_role_sync_success();
        scheduler.record_role_sync_success();

        let stats = scheduler.stats();
        assert_eq!(stats.role_syncs_succeeded, 2);
        assert!(stats.last_role_sync > 0);
    }

    #[test]
    fn test_sync_stats_record_role_sync_failure() {
        let cache = Arc::new(RoleCache::new());
        let scheduler = SyncScheduler::with_defaults(cache);

        scheduler.record_role_sync_failure();
        scheduler.record_role_sync_failure();
        scheduler.record_role_sync_failure();

        let stats = scheduler.stats();
        assert_eq!(stats.role_syncs_failed, 3);
    }

    #[test]
    fn test_sync_stats_record_revocation_events() {
        let cache = Arc::new(RoleCache::new());
        let scheduler = SyncScheduler::with_defaults(cache);

        scheduler.record_revocation_sync_success();
        scheduler.record_revocation_sync_failure();

        let stats = scheduler.stats();
        assert_eq!(stats.revocation_syncs_succeeded, 1);
        assert_eq!(stats.revocation_syncs_failed, 1);
        assert!(stats.last_revocation_sync > 0);
    }

    #[test]
    fn test_retry_backoff_grows_exponentially() {
        let cache = Arc::new(RoleCache::new());
        let scheduler = SyncScheduler::with_defaults(cache);

        let b0 = scheduler.retry_backoff(0);
        let b1 = scheduler.retry_backoff(1);
        let b2 = scheduler.retry_backoff(2);
        let b3 = scheduler.retry_backoff(3);

        assert_eq!(b0, Duration::from_secs(1)); // base * 2^0
        assert_eq!(b1, Duration::from_secs(2)); // base * 2^1
        assert_eq!(b2, Duration::from_secs(4)); // base * 2^2
        assert_eq!(b3, Duration::from_secs(8)); // base * 2^3
    }

    #[test]
    fn test_retry_backoff_capped() {
        let cache = Arc::new(RoleCache::new());
        let config = SyncConfig {
            retry_backoff_base_secs: 2,
            max_retries: 10,
            ..Default::default()
        };
        let scheduler = SyncScheduler::new(cache, config);

        // At attempt 10, backoff = 2 * 2^10 = 2048 seconds
        let b10 = scheduler.retry_backoff(10);
        assert_eq!(b10, Duration::from_secs(2048)); // 2 * 2^10

        // Cap: the min(attempt, 10) limits us
        let b20 = scheduler.retry_backoff(20);
        assert_eq!(b20, Duration::from_secs(2048)); // same as attempt 10
    }

    #[test]
    fn test_role_cache_sync_full_updates_sync_time() {
        let cache = RoleCache::new();

        let before = cache.last_sync_time();
        assert_eq!(before, 0); // never synced

        cache.sync_full(vec![RoleEntry {
            name: "test".to_string(),
            grants: vec![],
            high_sensitive: false,
        }]);

        let after = cache.last_sync_time();
        assert!(after > 0); // should be updated
    }
}
