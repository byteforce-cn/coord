//! Application service: distributed lock management.
//!
//! [`LockApp`] owns all business logic for lock operations:
//! token generation, TTL calculation, command encoding, result decoding,
//! and the `coord_locks_held` gauge.  The gRPC transport handler delegates
//! all domain work here and only converts proto ↔ application types.

use std::sync::Arc;

use coord_core::clock::{Clock, SystemClock};
use coord_core::lock::{AcquireOutcome, LockManager};
use coord_core::metrics::CoordMetrics;
use uuid::Uuid;

use crate::raft_runtime::RaftRuntime;

/// Result of a lock-acquire operation at the application layer.
///
/// The transport handler converts this into the proto `LockAcquireResponse`.
pub enum LockAcquireResult {
    /// The lock was granted; `token` must be presented for subsequent
    /// `release` / `keep_alive` calls.
    Acquired { token: String, expires_unix_ms: i64 },
    /// The lock is held but the caller set `wait = true`; the request has
    /// been enqueued for deferred wakeup.
    Queued,
    /// The lock is held and the caller did not request waiting.
    Busy,
}

/// Application service for distributed lock operations.
///
/// Centralises business logic (token generation, TTL math, command encoding,
/// result decoding) so that the transport handler is a thin translation layer.
#[derive(Clone)]
pub struct LockApp {
    locks: Arc<LockManager>,
    metrics: Arc<CoordMetrics>,
    raft: RaftRuntime,
}

impl LockApp {
    pub fn new(locks: Arc<LockManager>, metrics: Arc<CoordMetrics>, raft: RaftRuntime) -> Self {
        Self {
            locks,
            metrics,
            raft,
        }
    }

    /// Attempt to acquire a distributed lock on behalf of `owner`.
    ///
    /// Generates a random `token`, computes the expiry from `ttl_secs`,
    /// proposes the command via Raft, and decodes the committed result.
    pub async fn acquire(
        &self,
        key: &str,
        owner: &str,
        ttl_secs: i64,
        wait: bool,
    ) -> Result<LockAcquireResult, String> {
        self.metrics.coord_locks_acquire_total.inc();

        let ttl = ttl_secs.max(1);
        let token = Uuid::new_v4().to_string();
        let expires_unix_ms = SystemClock.now_ms() + ttl * 1_000;

        let payload = LockManager::encode_acquire_bytes(key, owner, wait, &token, expires_unix_ms);
        let result_bytes = self
            .raft
            .propose_business_command_for_result("lock", payload)
            .await
            .map_err(|e| format!("raft propose failed: {e}"))?;

        let outcome: AcquireOutcome = serde_json::from_slice(&result_bytes)
            .map_err(|e| format!("failed to decode lock acquire result: {e}"))?;

        let result = match outcome {
            AcquireOutcome::Acquired {
                token: t,
                expires_unix_ms: exp,
            } => LockAcquireResult::Acquired {
                token: t,
                expires_unix_ms: exp,
            },
            AcquireOutcome::Queued => LockAcquireResult::Queued,
            AcquireOutcome::Busy => LockAcquireResult::Busy,
        };

        self.sync_held_gauge().await;
        Ok(result)
    }

    /// Release a held lock identified by `key` and `token`.
    pub async fn release(&self, key: &str, token: &str) -> Result<(), String> {
        let payload = LockManager::encode_release_bytes(key, token);
        self.raft
            .propose_business_command("lock", payload)
            .await
            .map_err(|e| format!("raft propose failed: {e}"))?;

        self.sync_held_gauge().await;
        Ok(())
    }

    /// Renew the TTL of a held lock.
    ///
    /// Returns the new expiry timestamp on success, or `None` if the lock
    /// is no longer held under the given token.
    pub async fn keep_alive(
        &self,
        key: &str,
        token: &str,
        ttl_secs: i64,
    ) -> Result<Option<i64>, String> {
        let ttl = ttl_secs.max(1);
        let new_expires_unix_ms = SystemClock.now_ms() + ttl * 1_000;

        let payload = LockManager::encode_keep_alive_bytes(key, token, new_expires_unix_ms);
        let result_bytes = self
            .raft
            .propose_business_command_for_result("lock", payload)
            .await
            .map_err(|e| format!("raft propose failed: {e}"))?;

        let new_expiry: Option<i64> = serde_json::from_slice(&result_bytes).unwrap_or(None);
        Ok(new_expiry)
    }

    /// Synchronise the `coord_locks_held` gauge with the current lock state.
    async fn sync_held_gauge(&self) {
        self.metrics
            .coord_locks_held
            .set(self.locks.list_holders().await.len() as i64);
    }
}

#[cfg(test)]
mod tests {
    // Unit tests for LockApp are exercised via integration tests.
    // Add focused unit tests here if the logic grows more complex.
}
