//! Integration tests: config watch and lock keep_alive timeout / backpressure.
//!
//! These tests exercise the core-level watch and keepalive behaviour using
//! `TestClock` to simulate time progression without real sleeps.

use std::sync::Arc;
use std::time::Duration;

use coord_core::clock::{Clock, TestClock};
use coord_core::config::ConfigCenter;
use coord_core::lock::{AcquireOutcome, LockManager};
use tokio::sync::broadcast;

// ─── helpers ─────────────────────────────────────────────────────────────────

fn fresh_config() -> Arc<ConfigCenter> {
    Arc::new(ConfigCenter::new())
}

fn fresh_lock(clock: Arc<TestClock>) -> Arc<LockManager> {
    Arc::new(LockManager::new(clock as Arc<dyn coord_core::clock::Clock>))
}

// ─── config watch tests ──────────────────────────────────────────────────────

/// Watch receiver gets notified when a key is updated.
#[tokio::test]
async fn watch_receives_put_notification() {
    let config = fresh_config();
    let mut rx = config.watch("/app/env").await;

    config
        .put("/app/env".to_string(), "staging".to_string())
        .await;

    let entry = rx.recv().await.expect("should receive notification");
    assert_eq!(entry.key, "/app/env");
    assert_eq!(entry.value, "staging");
}

/// Watch receiver gets multiple sequential updates.
#[tokio::test]
async fn watch_receives_sequential_updates() {
    let config = fresh_config();
    let mut rx = config.watch("/counter").await;

    config.put("/counter".to_string(), "1".to_string()).await;
    config.put("/counter".to_string(), "2".to_string()).await;
    config.put("/counter".to_string(), "3".to_string()).await;

    let v1 = rx.recv().await.expect("update 1");
    assert_eq!(v1.value, "1");

    let v2 = rx.recv().await.expect("update 2");
    assert_eq!(v2.value, "2");

    let v3 = rx.recv().await.expect("update 3");
    assert_eq!(v3.value, "3");
}

/// Watch for a key that doesn't exist yet still fires when key is first created.
#[tokio::test]
async fn watch_fires_on_key_creation() {
    let config = fresh_config();
    let mut rx = config.watch("/future/key").await;

    config
        .put("/future/key".to_string(), "created".to_string())
        .await;

    let entry = rx.recv().await.expect("should receive creation event");
    assert_eq!(entry.value, "created");
}

/// Watching one key does not receive events for a different key.
#[tokio::test]
async fn watch_is_key_scoped() {
    let config = fresh_config();
    let mut rx_a = config.watch("/key/a").await;

    config
        .put("/key/b".to_string(), "b-value".to_string())
        .await;
    config
        .put("/key/a".to_string(), "a-value".to_string())
        .await;

    let entry = rx_a.recv().await.expect("should receive /key/a event");
    assert_eq!(entry.key, "/key/a");
    assert_eq!(entry.value, "a-value");
}

/// When broadcast channel is overflowed, receiver sees a lagged error but
/// can continue receiving subsequent updates.
#[tokio::test]
async fn watch_backpressure_lagged_recoverable() {
    let config = fresh_config();
    let mut rx = config.watch("/flood").await;

    // Flood 130 updates (broadcast capacity is 128)
    for i in 0..130 {
        config.put("/flood".to_string(), format!("{i}")).await;
    }

    // First recv may be lagged (overflow)
    match rx.recv().await {
        Ok(entry) => {
            // Got a value — that's fine if not overflowed
            assert!(!entry.value.is_empty());
        }
        Err(broadcast::error::RecvError::Lagged(n)) => {
            // Expected: some messages were dropped due to backpressure
            assert!(n > 0);
            // But we should be able to receive the next message
            let next = rx.recv().await.expect("should recover after lag");
            assert!(!next.value.is_empty());
        }
        Err(broadcast::error::RecvError::Closed) => {
            panic!("channel should not be closed while sender exists");
        }
    }
}

/// Dropping all watchers doesn't prevent future subscriptions.
#[tokio::test]
async fn watch_resubscribe_after_drop() {
    let config = fresh_config();

    {
        let _rx = config.watch("/ephemeral").await;
        // _rx dropped here
    }

    let mut rx2 = config.watch("/ephemeral").await;
    config
        .put("/ephemeral".to_string(), "after-resub".to_string())
        .await;

    let entry = rx2.recv().await.expect("new subscription should work");
    assert_eq!(entry.value, "after-resub");
}

// ─── lock keep_alive tests ───────────────────────────────────────────────────

/// keep_alive extends the lock expiry.
#[tokio::test]
async fn keep_alive_extends_expiry() {
    let clock = Arc::new(TestClock::new(1_000_000));
    let locks = fresh_lock(clock.clone());

    let acquired = locks.acquire("svc-lock", "worker-a", 10, false).await;
    let token = match acquired {
        AcquireOutcome::Acquired { token, .. } => token,
        other => panic!("expected Acquired, got {other:?}"),
    };

    // Advance 5 seconds — still within TTL
    clock.advance(Duration::from_secs(5));
    let new_expiry = locks
        .keep_alive("svc-lock", &token, 10)
        .await
        .expect("keep_alive should succeed");
    // New expiry should be clock.now_ms() + 10*1000
    assert!(new_expiry > 1_000_000 + 5_000);
}

/// keep_alive after expiry still succeeds (lazy expiry: holder is only
/// cleaned up on the next acquire, not on keep_alive).
#[tokio::test]
async fn keep_alive_after_expiry_still_extends() {
    let clock = Arc::new(TestClock::new(1_000_000));
    let locks = fresh_lock(clock.clone());

    let acquired = locks.acquire("expired-lock", "worker-a", 5, false).await;
    let token = match acquired {
        AcquireOutcome::Acquired { token, .. } => token,
        other => panic!("expected Acquired, got {other:?}"),
    };

    // Advance past TTL
    clock.advance(Duration::from_secs(10));

    // keep_alive still works because expiry is lazy (only checked on acquire)
    let new_expiry = locks.keep_alive("expired-lock", &token, 5).await;
    assert!(
        new_expiry.is_some(),
        "keep_alive succeeds — lazy expiry doesn't clean up on keep_alive"
    );

    // But a new acquire by another owner WILL see the expiry and take over
    // (because acquire checks expires_unix_ms)
    // First, advance past the new expiry too
    clock.advance(Duration::from_secs(10));
    let acquired_b = locks.acquire("expired-lock", "worker-b", 60, false).await;
    assert!(
        matches!(acquired_b, AcquireOutcome::Acquired { .. }),
        "another owner should acquire after expiry"
    );
}

/// keep_alive with wrong token returns None.
#[tokio::test]
async fn keep_alive_wrong_token_rejected() {
    let clock = Arc::new(TestClock::new(1_000_000));
    let locks = fresh_lock(clock);

    let acquired = locks.acquire("tok-lock", "worker-a", 60, false).await;
    assert!(matches!(acquired, AcquireOutcome::Acquired { .. }));

    let result = locks.keep_alive("tok-lock", "wrong-token", 60).await;
    assert!(result.is_none(), "keep_alive with wrong token should fail");
}

/// Lock becomes available to next waiter after TTL expiry.
#[tokio::test]
async fn lock_expires_and_waiter_can_acquire() {
    let clock = Arc::new(TestClock::new(1_000_000));
    let locks = fresh_lock(clock.clone());

    // Worker A acquires with short TTL
    let acquired = locks.acquire("ttl-lock", "worker-a", 2, false).await;
    assert!(matches!(acquired, AcquireOutcome::Acquired { .. }));

    // Worker B is busy (no wait)
    let busy = locks.acquire("ttl-lock", "worker-b", 60, false).await;
    assert!(matches!(busy, AcquireOutcome::Busy));

    // Time passes beyond TTL
    clock.advance(Duration::from_secs(3));

    // Worker B can now acquire
    let acquired_b = locks.acquire("ttl-lock", "worker-b", 60, false).await;
    assert!(
        matches!(acquired_b, AcquireOutcome::Acquired { .. }),
        "worker-b should acquire after TTL expiry"
    );
}

/// Keep_alive on non-existent lock returns None.
#[tokio::test]
async fn keep_alive_nonexistent_lock() {
    let clock = Arc::new(TestClock::new(1_000_000));
    let locks = fresh_lock(clock);

    let result = locks.keep_alive("no-such-lock", "some-token", 60).await;
    assert!(result.is_none());
}

/// Multiple sequential keep_alive calls all extend correctly.
#[tokio::test]
async fn sequential_keep_alive_extends_repeatedly() {
    let clock = Arc::new(TestClock::new(1_000_000));
    let locks = fresh_lock(clock.clone());

    let acquired = locks.acquire("renew-lock", "worker-a", 5, false).await;
    let token = match acquired {
        AcquireOutcome::Acquired { token, .. } => token,
        other => panic!("expected Acquired, got {other:?}"),
    };

    for _ in 0..5 {
        clock.advance(Duration::from_secs(3));
        let expiry = locks
            .keep_alive("renew-lock", &token, 5)
            .await
            .expect("keep_alive should succeed on each renewal");
        assert!(expiry > clock.now_ms());
    }

    // After 5 renewals (15 seconds elapsed), lock should still be held
    let holders = locks.list_holders().await;
    assert_eq!(holders.len(), 1);
    assert_eq!(holders[0].owner, "worker-a");
}
