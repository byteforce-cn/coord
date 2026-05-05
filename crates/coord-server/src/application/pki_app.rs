//! Application service: PKI certificate lifecycle management.
//!
//! [`PkiApp`] encapsulates all write-path business logic for the PKI
//! namespace: certificate issuance, renewal, revocation, auto-renew, and
//! ACME operations — all replicated via Raft.
//!
//! Read-only operations (status check, CA chain, CRL, snapshot) remain on
//! the [`PkiEngine`] directly.

use std::sync::Arc;

use coord_core::clock::{Clock, SystemClock};
use coord_core::metrics::CoordMetrics;
use coord_core::pki::{
    AcmeChallenge, AcmeFinalizedOrder, AcmeOrder, AutoRenewExecution, AutoRenewPolicyState,
    AutoRenewedCertificate, CertificateIssueOptions, IssuedCertificate, PkiEngine,
};

use crate::raft_runtime::RaftRuntime;

/// Application service for PKI certificate operations.
///
/// Centralises the generate-then-replicate pattern and metrics recording
/// so that gRPC, HTTP, and background tasks share a single write path.
#[derive(Clone)]
pub struct PkiApp {
    pki: Arc<PkiEngine>,
    metrics: Arc<CoordMetrics>,
    raft: RaftRuntime,
}

impl PkiApp {
    pub fn new(pki: Arc<PkiEngine>, metrics: Arc<CoordMetrics>, raft: RaftRuntime) -> Self {
        Self { pki, metrics, raft }
    }

    /// Reference to the underlying engine for read-only operations.
    pub fn engine(&self) -> &Arc<PkiEngine> {
        &self.pki
    }

    /// Reference to the metrics collector.
    pub fn metrics(&self) -> &Arc<CoordMetrics> {
        &self.metrics
    }

    /// Issue a new certificate (Raft-replicated).
    ///
    /// Uses the two-step generate-then-replicate pattern:
    /// 1. Generate cert + key material locally (leader only).
    /// 2. Replicate the metadata record via Raft so all nodes converge.
    pub async fn issue_certificate(
        &self,
        common_name: &str,
        sans: Vec<String>,
        options: CertificateIssueOptions,
    ) -> Result<IssuedCertificate, String> {
        let issued = self
            .pki
            .generate_cert_only(common_name, sans, options.clone())
            .await
            .map_err(|e| e.to_string())?;

        let payload = PkiEngine::encode_store_issued_bytes(
            &issued.serial_number,
            &issued.common_name,
            &issued.sans,
            issued.not_after_unix_seconds,
            options.ttl_seconds,
            issued.auto_renew_enabled,
            issued.renew_before_seconds,
            options.managed_by_acme,
        );
        self.raft
            .propose_business_command("pki", payload)
            .await
            .map_err(|e| format!("raft propose failed: {e}"))?;

        self.record_issue_metrics(&issued);
        Ok(issued)
    }

    /// Renew an existing certificate (Raft-replicated).
    pub async fn renew_certificate(
        &self,
        serial_number: &str,
        ttl_seconds: i64,
    ) -> Result<IssuedCertificate, String> {
        let renewed = self
            .pki
            .generate_renewed_cert_only(serial_number, Some(ttl_seconds))
            .await
            .map_err(|e| e.to_string())?;

        let payload = PkiEngine::encode_store_renewed_bytes(
            serial_number,
            &renewed.serial_number,
            &renewed.common_name,
            &renewed.sans,
            renewed.not_after_unix_seconds,
            ttl_seconds,
            renewed.auto_renew_enabled,
            renewed.renew_before_seconds,
            false,
            true, // revoke_previous
            SystemClock.now_seconds(),
        );
        self.raft
            .propose_business_command("pki", payload)
            .await
            .map_err(|e| format!("raft propose failed: {e}"))?;

        self.record_issue_metrics(&renewed);
        Ok(renewed)
    }

    /// Revoke a certificate (Raft-replicated).
    pub async fn revoke_certificate(
        &self,
        serial_number: &str,
        reason: &str,
    ) -> Result<bool, String> {
        let payload =
            PkiEngine::encode_revoke_bytes(serial_number, reason, SystemClock.now_seconds());
        self.raft
            .propose_business_command("pki", payload)
            .await
            .map_err(|e| format!("raft propose failed: {e}"))?;

        self.metrics.coord_pki_certificates_revoked_total.inc();
        Ok(true)
    }

    /// Update auto-renew policy for a certificate (Raft-replicated).
    pub async fn update_auto_renew_policy(
        &self,
        serial_number: &str,
        enabled: bool,
        renew_before_seconds: i64,
    ) -> Result<AutoRenewPolicyState, String> {
        let payload =
            PkiEngine::encode_update_auto_renew_bytes(serial_number, enabled, renew_before_seconds);
        self.raft
            .propose_business_command("pki", payload)
            .await
            .map_err(|e| format!("raft propose failed: {e}"))?;

        // Read back the applied state (the Raft apply has already executed
        // on the leader by the time propose returns).
        self.pki
            .get_auto_renew_policy(serial_number)
            .await
            .map_err(|e| e.to_string())
    }

    /// Run auto-renewal cycle (Raft-replicated per certificate).
    ///
    /// Finds certificates eligible for renewal and individually renews each
    /// via the Raft-replicated `renew_certificate` path, ensuring multi-node
    /// consistency.
    pub async fn run_auto_renew(&self) -> AutoRenewExecution {
        let snapshot = self.pki.snapshot().await;
        let now = SystemClock.now_seconds();

        let revoked_serials: std::collections::HashSet<String> = snapshot
            .revocations
            .iter()
            .map(|r| r.serial_number.clone())
            .collect();

        let mut execution = AutoRenewExecution::default();

        for cert in &snapshot.issued {
            if !cert.auto_renew_enabled {
                continue;
            }
            if revoked_serials.contains(&cert.serial_number) {
                continue;
            }
            let remaining = cert.not_after_unix_seconds - now;
            if remaining > cert.renew_before_seconds {
                continue;
            }

            match self
                .renew_certificate(&cert.serial_number, cert.ttl_seconds)
                .await
            {
                Ok(renewed) => {
                    self.metrics.coord_pki_auto_renew_total.inc();
                    execution.renewed.push(AutoRenewedCertificate {
                        old_serial_number: cert.serial_number.clone(),
                        new_serial_number: renewed.serial_number,
                        common_name: renewed.common_name,
                        not_after_unix_seconds: renewed.not_after_unix_seconds,
                    });
                }
                Err(err) => {
                    execution
                        .errors
                        .push(format!("failed to renew {}: {err}", cert.serial_number));
                }
            }
        }

        execution
    }

    /// Create an ACME order (Raft-replicated).
    pub async fn create_acme_order(
        &self,
        domains: Vec<String>,
        ttl_seconds: i64,
        challenge_type: &str,
        auto_renew_enabled: bool,
        renew_before_seconds: i64,
    ) -> Result<AcmeOrder, String> {
        let prepared = self
            .pki
            .prepare_acme_order(
                domains,
                ttl_seconds,
                challenge_type,
                auto_renew_enabled,
                renew_before_seconds,
            )
            .await
            .map_err(|e| e.to_string())?;

        let payload = PkiEngine::encode_create_acme_order_bytes(&prepared);
        self.raft
            .propose_business_command("pki", payload)
            .await
            .map_err(|e| format!("raft propose failed: {e}"))?;

        self.metrics.coord_pki_acme_orders_total.inc();

        // Construct response from prepared data (Raft apply has already
        // executed on the leader by the time propose returns).
        Ok(AcmeOrder {
            order_id: prepared.order_id,
            status: "PENDING".to_string(),
            challenges: prepared
                .challenges
                .into_iter()
                .map(|c| AcmeChallenge {
                    domain: c.domain,
                    challenge_type: c.challenge_type,
                    token: c.token,
                    validated: false,
                })
                .collect(),
            expires_unix_seconds: prepared.expires_unix_seconds,
            finalized_serial_number: None,
        })
    }

    /// Complete an ACME challenge (Raft-replicated).
    pub async fn complete_acme_challenge(
        &self,
        order_id: &str,
        domain: &str,
        token: &str,
    ) -> Result<AcmeOrder, String> {
        let normalized_domain = self
            .pki
            .validate_acme_challenge(order_id, domain, token)
            .await
            .map_err(|e| e.to_string())?;

        let payload = PkiEngine::encode_complete_acme_challenge_bytes(order_id, &normalized_domain);
        self.raft
            .propose_business_command("pki", payload)
            .await
            .map_err(|e| format!("raft propose failed: {e}"))?;

        // Read back the applied state.
        self.pki
            .get_acme_order(order_id)
            .await
            .ok_or_else(|| "order not found after apply".to_string())
    }

    /// Finalize an ACME order and issue the certificate (Raft-replicated).
    ///
    /// Uses an atomic `FinalizeAcmeOrder` Raft command that stores the
    /// issued certificate metadata and marks the order as Valid in a
    /// single apply step.
    pub async fn finalize_acme_order(
        &self,
        order_id: &str,
        common_name: &str,
    ) -> Result<AcmeFinalizedOrder, String> {
        let (certificate, ttl_seconds, resolved_common_name) = self
            .pki
            .prepare_acme_finalize(order_id, common_name)
            .await
            .map_err(|e| e.to_string())?;

        let payload = PkiEngine::encode_finalize_acme_order_bytes(
            order_id,
            &resolved_common_name,
            &certificate.serial_number,
            &certificate.sans,
            certificate.not_after_unix_seconds,
            ttl_seconds,
            certificate.auto_renew_enabled,
            certificate.renew_before_seconds,
        );
        self.raft
            .propose_business_command("pki", payload)
            .await
            .map_err(|e| format!("raft propose failed: {e}"))?;

        self.record_issue_metrics(&certificate);
        Ok(AcmeFinalizedOrder {
            order_id: order_id.to_string(),
            status: "VALID".to_string(),
            certificate,
            ttl_seconds,
        })
    }

    fn record_issue_metrics(&self, cert: &IssuedCertificate) {
        self.metrics.coord_pki_certificates_issued_total.inc();
        let expiry_seconds =
            (cert.not_after_unix_seconds - SystemClock.now_seconds()).max(0) as f64;
        self.metrics
            .coord_pki_certificates_expiry_seconds
            .observe(expiry_seconds);
    }
}
