//! Integration tests: seal / unseal lifecycle with Shamir secret sharing.
//!
//! `SecurityController` holds the barrier key protecting the security domain.
//! These tests walk through the full `init → seal → unseal` cycle that
//! production nodes execute during bootstrapping, restart, and operator-
//! initiated seals.

use std::sync::Arc;

use coord_core::clock::SystemClock;
use coord_core::pki::PkiEngine;
use coord_core::security::{DomainLifecycleManager, SecurityController};
use coord_core::transit::TransitEngine;

// ─── helpers ─────────────────────────────────────────────────────────────────

fn fresh_security() -> Arc<SecurityController> {
    Arc::new(SecurityController::new())
}

// ─── tests ───────────────────────────────────────────────────────────────────

/// Fresh instance is neither initialized nor unsealed (sealed=false because not initialized).
#[tokio::test]
async fn new_security_is_uninitialized() {
    let sec = fresh_security();
    let status = sec.seal_status().await;
    assert!(!status.initialized);
    assert!(
        !status.sealed,
        "uninitialized instance has no domain to protect, sealed=false"
    );
}

/// `init_security` returns exactly `shares_total` shares and leaves the
/// instance in a sealed state.
#[tokio::test]
async fn init_returns_correct_share_count_and_seals() {
    let sec = fresh_security();
    let shares = sec.init_security(5, 3).await.expect("init should succeed");

    assert_eq!(shares.len(), 5, "should return 5 shares");

    let status = sec.seal_status().await;
    assert!(status.initialized);
    assert!(status.sealed, "must remain sealed immediately after init");
    assert_eq!(status.shares_total, 5);
    assert_eq!(status.threshold, 3);
}

/// Submitting exactly `threshold` shares unseals the barrier.
#[tokio::test]
async fn exact_threshold_unseals() {
    let sec = fresh_security();
    let shares = sec.init_security(3, 2).await.expect("init");

    // First share: still sealed, progress = 1
    let s1 = sec.unseal(&shares[0]).await.expect("unseal share 1");
    assert!(s1.sealed, "sealed after 1/2 shares");
    assert_eq!(s1.progress, 1);

    // Second share: should unseal
    let s2 = sec.unseal(&shares[1]).await.expect("unseal share 2");
    assert!(
        !s2.sealed,
        "must be unsealed after threshold shares submitted"
    );
    assert_eq!(s2.progress, 0, "progress resets to 0 once unsealed");
}

/// Submitting only `threshold - 1` shares is not enough to unseal.
#[tokio::test]
async fn below_threshold_does_not_unseal() {
    let sec = fresh_security();
    let shares = sec.init_security(5, 3).await.expect("init");

    sec.unseal(&shares[0]).await.expect("share 1");
    let status = sec.unseal(&shares[1]).await.expect("share 2");
    assert!(status.sealed, "2/3 shares should NOT unseal");
    assert_eq!(status.progress, 2);
}

/// After unsealing, calling `seal` returns the instance to sealed state.
#[tokio::test]
async fn seal_re_seals_an_unsealed_instance() {
    let sec = fresh_security();
    let shares = sec.init_security(2, 2).await.expect("init");
    sec.unseal(&shares[0]).await.expect("share 1");
    sec.unseal(&shares[1]).await.expect("share 2");

    let before = sec.seal_status().await;
    assert!(!before.sealed, "precondition: must be unsealed");

    let after = sec.seal().await.expect("seal should succeed");
    assert!(after.sealed, "must be sealed again");
}

/// Initialising an already-initialised instance is rejected.
#[tokio::test]
async fn double_init_is_rejected() {
    let sec = fresh_security();
    sec.init_security(3, 2).await.expect("first init");
    let err = sec.init_security(3, 2).await;
    assert!(
        err.is_err(),
        "second init_security call should return an error"
    );
}

/// Threshold of 0 is rejected by the share config validator.
#[tokio::test]
async fn zero_threshold_is_rejected() {
    let sec = fresh_security();
    let err = sec.init_security(3, 0).await;
    assert!(err.is_err(), "threshold of 0 must be rejected");
}

/// Threshold greater than total is rejected.
#[tokio::test]
async fn threshold_greater_than_total_is_rejected() {
    let sec = fresh_security();
    let err = sec.init_security(2, 5).await;
    assert!(err.is_err(), "threshold > total must be rejected");
}

// ─── T-P2-05: seal / unseal + transit / pki combo tests ─────────────────────

fn fresh_transit() -> Arc<TransitEngine> {
    Arc::new(TransitEngine::new())
}

fn fresh_pki() -> Arc<PkiEngine> {
    let clock = Arc::new(SystemClock) as Arc<dyn coord_core::clock::Clock>;
    Arc::new(PkiEngine::new(clock).expect("create pki engine"))
}

fn fresh_domain_lifecycle(
    transit: Arc<TransitEngine>,
    pki: Arc<PkiEngine>,
) -> Arc<DomainLifecycleManager> {
    let clock = Arc::new(SystemClock) as Arc<dyn coord_core::clock::Clock>;
    Arc::new(DomainLifecycleManager::new(transit, pki, clock))
}

/// Helper: unseal with all shares.
async fn unseal_all(sec: &SecurityController, shares: &[String]) {
    for share in shares {
        sec.unseal(share).await.expect("unseal share");
    }
    assert!(!sec.seal_status().await.sealed);
}

/// Full cycle: init → unseal → create transit key → seal with domain → unseal → verify key.
#[tokio::test]
async fn transit_key_survives_seal_unseal_cycle() {
    let sec = Arc::new(SecurityController::new());
    let transit = fresh_transit();
    let pki = fresh_pki();
    let dlm = fresh_domain_lifecycle(transit.clone(), pki.clone());

    let shares = sec.init_security(3, 2).await.expect("init");
    unseal_all(&sec, &shares[..2]).await;

    // Restore empty domain after unseal (no keys yet)
    if let Some(domain) = sec.take_unsealed_domain_snapshot().await {
        dlm.restore_domain(domain)
            .await
            .expect("restore empty domain");
    }

    // Create transit key while unsealed
    transit
        .create_key("seal-test-key")
        .await
        .expect("create transit key");
    let (sig, _) = transit
        .hmac_sign("seal-test-key", b"payload")
        .await
        .expect("sign");

    // Seal with captured domain
    let auth = sec.export_auth_state_snapshot().await;
    let domain = dlm.capture(auth).await;
    sec.seal_with_domain(domain)
        .await
        .expect("seal with domain");
    assert!(sec.seal_status().await.sealed);

    // Transit key should be wiped after seal+clear
    dlm.clear().await.expect("clear after seal");
    assert!(
        transit.hmac_sign("seal-test-key", b"x").await.is_err(),
        "transit key must be inaccessible while sealed"
    );

    // Unseal again
    unseal_all(&sec, &shares[..2]).await;
    let restored_domain = sec
        .take_unsealed_domain_snapshot()
        .await
        .expect("domain snapshot after unseal");
    dlm.restore_domain(restored_domain)
        .await
        .expect("restore domain after unseal");

    // Verify transit key is back
    let verified = transit
        .hmac_verify("seal-test-key", b"payload", &sig)
        .await
        .expect("verify with restored key");
    assert!(verified, "transit HMAC must verify after unseal");
}

/// PKI certificate survives seal → unseal cycle.
#[tokio::test]
async fn pki_certificate_survives_seal_unseal_cycle() {
    let sec = Arc::new(SecurityController::new());
    let transit = fresh_transit();
    let pki = fresh_pki();
    let dlm = fresh_domain_lifecycle(transit.clone(), pki.clone());

    let shares = sec.init_security(2, 2).await.expect("init");
    unseal_all(&sec, &shares).await;
    if let Some(domain) = sec.take_unsealed_domain_snapshot().await {
        dlm.restore_domain(domain).await.expect("restore");
    }

    // Issue certificate while unsealed
    let issued = pki
        .issue_certificate("app.internal", vec!["app.internal".to_string()], 3600)
        .await
        .expect("issue cert");

    // Seal
    let auth = sec.export_auth_state_snapshot().await;
    let domain = dlm.capture(auth).await;
    sec.seal_with_domain(domain).await.expect("seal");
    dlm.clear().await.expect("clear");

    // Unseal
    unseal_all(&sec, &shares).await;
    let restored = sec
        .take_unsealed_domain_snapshot()
        .await
        .expect("domain after unseal");
    dlm.restore_domain(restored).await.expect("restore domain");

    // Renew should work with restored PKI state
    let renewed = pki
        .renew_certificate(&issued.serial_number, 7200)
        .await
        .expect("renew restored cert");
    assert_eq!(renewed.common_name, "app.internal");
}

/// Transit encrypt/decrypt roundtrip survives seal → unseal.
#[tokio::test]
async fn transit_encrypt_decrypt_survives_seal_unseal() {
    let sec = Arc::new(SecurityController::new());
    let transit = fresh_transit();
    let pki = fresh_pki();
    let dlm = fresh_domain_lifecycle(transit.clone(), pki.clone());

    let shares = sec.init_security(2, 2).await.expect("init");
    unseal_all(&sec, &shares).await;
    if let Some(d) = sec.take_unsealed_domain_snapshot().await {
        dlm.restore_domain(d).await.expect("restore");
    }

    transit.create_key("enc-seal").await.expect("create key");
    let (ciphertext, _) = transit
        .encrypt("enc-seal", b"secret")
        .await
        .expect("encrypt");

    // Seal → unseal
    let auth = sec.export_auth_state_snapshot().await;
    let domain = dlm.capture(auth).await;
    sec.seal_with_domain(domain).await.expect("seal");
    dlm.clear().await.expect("clear");
    unseal_all(&sec, &shares).await;
    let rd = sec.take_unsealed_domain_snapshot().await.expect("domain");
    dlm.restore_domain(rd).await.expect("restore");

    let (plaintext, _) = transit
        .decrypt("enc-seal", &ciphertext)
        .await
        .expect("decrypt after unseal");
    assert_eq!(plaintext, b"secret");
}

/// Sealing without domain capture loses transit keys (expected behavior).
#[tokio::test]
async fn seal_without_domain_loses_transit_keys() {
    let sec = Arc::new(SecurityController::new());
    let transit = fresh_transit();
    let pki = fresh_pki();
    let dlm = fresh_domain_lifecycle(transit.clone(), pki.clone());

    let shares = sec.init_security(2, 2).await.expect("init");
    unseal_all(&sec, &shares).await;
    if let Some(d) = sec.take_unsealed_domain_snapshot().await {
        dlm.restore_domain(d).await.expect("restore");
    }

    transit.create_key("ephemeral").await.expect("create");

    // Seal WITHOUT capturing domain (uses default empty domain)
    sec.seal().await.expect("seal");
    dlm.clear().await.expect("clear");

    // Unseal — domain is empty, so transit key is gone
    unseal_all(&sec, &shares).await;
    if let Some(d) = sec.take_unsealed_domain_snapshot().await {
        dlm.restore_domain(d).await.expect("restore empty");
    }

    assert!(
        transit.encrypt("ephemeral", b"x").await.is_err(),
        "transit key should be lost when sealed without domain capture"
    );
}
