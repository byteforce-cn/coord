//! Integration tests: config-centre backup and restore roundtrip.
//!
//! These tests use `coord_core::config::ConfigCenter` directly to verify that
//! the snapshot / restore contract is upheld end-to-end.  In production the
//! same snapshot/restore path is exercised by `propose_backup_restore` in the
//! Raft runtime (which is unit-tested in `src/raft_runtime.rs`).

use std::sync::Arc;

use coord_core::clock::{Clock, SystemClock};
use coord_core::config::ConfigCenter;
use coord_core::lock::{AcquireOutcome, LockManager};
use coord_core::pki::PkiEngine;
use coord_core::transit::TransitEngine;

// ─── helpers ─────────────────────────────────────────────────────────────────

fn fresh_config_center() -> Arc<ConfigCenter> {
    Arc::new(ConfigCenter::new())
}

fn fresh_lock_manager() -> Arc<LockManager> {
    let clock = Arc::new(SystemClock) as Arc<dyn Clock>;
    Arc::new(LockManager::new(clock))
}

// ─── tests ───────────────────────────────────────────────────────────────────

/// A round-trip `snapshot → restore` preserves all written entries.
#[tokio::test]
async fn snapshot_restore_preserves_all_entries() {
    let source = fresh_config_center();
    source
        .put("/app/env".to_string(), "production".to_string())
        .await;
    source
        .put("/app/version".to_string(), "2.0".to_string())
        .await;
    source
        .put("/feature/dark-mode".to_string(), "true".to_string())
        .await;

    let snapshot = source.snapshot().await;
    assert_eq!(snapshot.len(), 3);

    let target = fresh_config_center();
    target.restore(snapshot).await;

    let env = target.get("/app/env").await.expect("env key must exist");
    assert_eq!(env.value, "production");

    let ver = target
        .get("/app/version")
        .await
        .expect("version key must exist");
    assert_eq!(ver.value, "2.0");

    let dark = target
        .get("/feature/dark-mode")
        .await
        .expect("feature flag must exist");
    assert_eq!(dark.value, "true");
}

/// Restoring into a non-empty center completely replaces the previous state.
#[tokio::test]
async fn restore_replaces_existing_entries() {
    let source = fresh_config_center();
    source.put("/key".to_string(), "original".to_string()).await;
    let snapshot = source.snapshot().await;

    let target = fresh_config_center();
    target.put("/key".to_string(), "stale".to_string()).await;
    target
        .put("/extra".to_string(), "extra-value".to_string())
        .await;

    target.restore(snapshot).await;

    let key = target
        .get("/key")
        .await
        .expect("key must exist after restore");
    assert_eq!(
        key.value, "original",
        "restored value must override stale value"
    );

    assert!(
        target.get("/extra").await.is_none(),
        "/extra should be gone after restore"
    );
}

/// Restoring from an empty snapshot wipes all entries.
#[tokio::test]
async fn restore_from_empty_snapshot_clears_all() {
    let center = fresh_config_center();
    center
        .put("/some/key".to_string(), "value".to_string())
        .await;
    center.restore(Vec::new()).await;
    assert!(center.snapshot().await.is_empty());
}

/// `put` increments the version on successive writes to the same key.
#[tokio::test]
async fn put_increments_version() {
    let center = fresh_config_center();
    let v1 = center.put("/k".to_string(), "first".to_string()).await;
    let v2 = center.put("/k".to_string(), "second".to_string()).await;
    assert!(
        v2.version > v1.version,
        "version must increase on overwrite"
    );
}

/// Snapshot entries are internally consistent (key matches content).
#[tokio::test]
async fn snapshot_entries_are_consistent() {
    let center = fresh_config_center();
    center.put("/a".to_string(), "alpha".to_string()).await;
    center.put("/b".to_string(), "beta".to_string()).await;

    let snapshot = center.snapshot().await;
    for entry in &snapshot {
        let live = center.get(&entry.key).await.expect("entry must exist");
        assert_eq!(live.value, entry.value);
        assert_eq!(live.version, entry.version);
    }
}

/// Lock snapshot/restore must preserve holder and waiter queue ordering.
#[tokio::test]
async fn lock_snapshot_restore_preserves_holder_and_wait_queue() {
    let source = fresh_lock_manager();

    let acquired = source.acquire("deploy", "worker-a", 60, false).await;
    let holder_token = match acquired {
        AcquireOutcome::Acquired { token, .. } => token,
        other => panic!("worker-a should acquire lock, got {other:?}"),
    };

    let queued = source.acquire("deploy", "worker-b", 60, true).await;
    assert_eq!(queued, AcquireOutcome::Queued);

    let snapshot = source.snapshot().await;
    let restored = fresh_lock_manager();
    restored.restore(snapshot).await;

    assert!(restored.release("deploy", &holder_token).await);

    let next = restored.acquire("deploy", "worker-b", 60, true).await;
    assert!(matches!(next, AcquireOutcome::Acquired { .. }));
}

/// Transit snapshot/restore must preserve key material and allow post-restore verification.
#[tokio::test]
async fn transit_snapshot_restore_preserves_key_material() {
    let source = TransitEngine::new();

    source.create_key("hmac-key").await.expect("create key");
    let (signature, _version) = source
        .hmac_sign("hmac-key", b"critical-data")
        .await
        .expect("sign data");

    let snapshot = source.snapshot().await;

    let restored = TransitEngine::new();
    restored
        .restore(snapshot)
        .await
        .expect("restore transit snapshot");

    let verified = restored
        .hmac_verify("hmac-key", b"critical-data", &signature)
        .await
        .expect("verify with restored key");
    assert!(verified, "HMAC signature must verify after restore");

    let tampered = restored
        .hmac_verify("hmac-key", b"tampered-data", &signature)
        .await
        .expect("tampered verify should return false, not error");
    assert!(!tampered, "tampered data must not verify");
}

/// Transit encrypt→snapshot→restore→decrypt roundtrip.
#[tokio::test]
async fn transit_snapshot_restore_decrypt_roundtrip() {
    let source = TransitEngine::new();
    source.create_key("enc-key").await.expect("create key");
    let (ciphertext, _) = source
        .encrypt("enc-key", b"secret-payload")
        .await
        .expect("encrypt");

    let snapshot = source.snapshot().await;
    let restored = TransitEngine::new();
    restored.restore(snapshot).await.expect("restore");

    let (plaintext, _) = restored
        .decrypt("enc-key", &ciphertext)
        .await
        .expect("decrypt with restored key");
    assert_eq!(plaintext, b"secret-payload");
}

/// PKI snapshot/restore must preserve issued certificate records for renewal.
#[tokio::test]
async fn pki_snapshot_restore_preserves_certificate_for_renewal() {
    let clock = Arc::new(SystemClock) as Arc<dyn Clock>;
    let source = PkiEngine::new(clock).expect("create pki");
    let issued = source
        .issue_certificate("svc.internal", vec!["svc.internal".to_string()], 3600)
        .await
        .expect("issue certificate");

    let snapshot = source.snapshot().await;

    let clock2 = Arc::new(SystemClock) as Arc<dyn Clock>;
    let restored = PkiEngine::new(clock2).expect("create pki");
    restored
        .restore(snapshot)
        .await
        .expect("restore PKI snapshot");

    let renewed = restored
        .renew_certificate(&issued.serial_number, 7200)
        .await
        .expect("renew restored certificate");
    assert_eq!(renewed.common_name, "svc.internal");
}

/// PKI snapshot/restore must preserve revocation state.
#[tokio::test]
async fn pki_snapshot_restore_preserves_revocation() {
    let clock = Arc::new(SystemClock) as Arc<dyn Clock>;
    let source = PkiEngine::new(clock).expect("create pki");
    let issued = source
        .issue_certificate("revoked.internal", vec![], 3600)
        .await
        .expect("issue");
    source
        .revoke_certificate(&issued.serial_number)
        .await
        .expect("revoke");

    let snapshot = source.snapshot().await;
    let clock2 = Arc::new(SystemClock) as Arc<dyn Clock>;
    let restored = PkiEngine::new(clock2).expect("create pki");
    restored.restore(snapshot).await.expect("restore");

    let renew_err = restored
        .renew_certificate(&issued.serial_number, 3600)
        .await;
    assert!(
        renew_err.is_err(),
        "renewing a revoked certificate after restore must fail"
    );
}
