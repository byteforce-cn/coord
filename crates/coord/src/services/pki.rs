//! gRPC service impl: `pki`.
//!
//! Split out from the monolithic `services.rs` in Batch 4a of the
//! April-2026 code review. Shared imports live in [`super`].
//!
//! Write operations (issue, renew, revoke, auto-renew, ACME) are delegated to
//! [`crate::application::pki_app::PkiApp`]; this file is a thin transport
//! adapter that converts proto types.

use super::*;
use crate::application::pki_app::PkiApp;
use coord_core::error::CoordError;

pub struct PkiGrpc {
    pki_app: PkiApp,
}

impl PkiGrpc {
    pub fn new(pki_app: PkiApp) -> Self {
        Self { pki_app }
    }
}

#[tonic::async_trait]
impl PkiService for PkiGrpc {
    async fn issue_certificate(
        &self,
        request: Request<IssueCertificateRequest>,
    ) -> Result<Response<IssueCertificateResponse>, Status> {
        let req = request.into_inner();
        let options = CertificateIssueOptions {
            ttl_seconds: req.ttl_seconds.max(60),
            auto_renew_enabled: req.auto_renew,
            renew_before_seconds: req.renew_before_seconds,
            managed_by_acme: false,
        };

        let issued = self
            .pki_app
            .issue_certificate(&req.common_name, req.sans, options)
            .await
            .map_err(|e| coord_status(CoordError::Unavailable(e)))?;

        Ok(Response::new(IssueCertificateResponse {
            serial_number: issued.serial_number,
            common_name: issued.common_name,
            sans: issued.sans,
            certificate_pem: issued.certificate_pem,
            private_key_pem: issued.private_key_pem,
            ca_certificate_pem: issued.ca_certificate_pem,
            not_after_unix_seconds: issued.not_after_unix_seconds,
            auto_renew: issued.auto_renew_enabled,
            renew_before_seconds: issued.renew_before_seconds,
        }))
    }

    async fn renew_certificate(
        &self,
        request: Request<RenewCertificateRequest>,
    ) -> Result<Response<RenewCertificateResponse>, Status> {
        let req = request.into_inner();

        let renewed = self
            .pki_app
            .renew_certificate(&req.serial_number, req.ttl_seconds.max(60))
            .await
            .map_err(|e| coord_status(CoordError::Unavailable(e)))?;

        Ok(Response::new(RenewCertificateResponse {
            old_serial_number: req.serial_number,
            new_serial_number: renewed.serial_number,
            common_name: renewed.common_name,
            sans: renewed.sans,
            certificate_pem: renewed.certificate_pem,
            private_key_pem: renewed.private_key_pem,
            ca_certificate_pem: renewed.ca_certificate_pem,
            not_after_unix_seconds: renewed.not_after_unix_seconds,
            auto_renew: renewed.auto_renew_enabled,
            renew_before_seconds: renewed.renew_before_seconds,
        }))
    }

    async fn revoke_certificate(
        &self,
        request: Request<RevokeCertificateRequest>,
    ) -> Result<Response<RevokeCertificateResponse>, Status> {
        let req = request.into_inner();

        self.pki_app
            .revoke_certificate(&req.serial_number, &req.reason)
            .await
            .map_err(|e| coord_status(CoordError::Unavailable(e)))?;

        Ok(Response::new(RevokeCertificateResponse { revoked: true }))
    }

    async fn get_ca_chain(
        &self,
        _request: Request<GetCaChainRequest>,
    ) -> Result<Response<GetCaChainResponse>, Status> {
        let ca_certificate_pem = self.pki_app.engine().get_ca_chain().await;
        Ok(Response::new(GetCaChainResponse {
            ca_cert_pem: ca_certificate_pem.clone(),
            ca_certificate_pem,
        }))
    }

    async fn get_certificate_revocation_list(
        &self,
        request: Request<GetCertificateRevocationListRequest>,
    ) -> Result<Response<GetCertificateRevocationListResponse>, Status> {
        let req = request.into_inner();
        let crl = self.pki_app.engine().get_crl(req.next_update_seconds).await;

        Ok(Response::new(GetCertificateRevocationListResponse {
            crl_number: crl.crl_number,
            this_update_unix_seconds: crl.this_update_unix_seconds,
            next_update_unix_seconds: crl.next_update_unix_seconds,
            revoked_certificates: crl
                .revoked_certificates
                .into_iter()
                .map(|item| RevokedCertificateItem {
                    serial_number: item.serial_number,
                    reason: item.reason,
                    revoked_at_unix_seconds: item.revoked_at_unix_seconds,
                })
                .collect(),
        }))
    }

    async fn check_certificate_status(
        &self,
        request: Request<CheckCertificateStatusRequest>,
    ) -> Result<Response<CheckCertificateStatusResponse>, Status> {
        let req = request.into_inner();
        if req.serial_number.trim().is_empty() {
            return Err(coord_status(CoordError::InvalidArgument(
                "serial_number cannot be empty".to_string(),
            )));
        }

        self.pki_app.metrics().coord_pki_ocsp_queries_total.inc();

        let report = self
            .pki_app
            .engine()
            .check_certificate_status(&req.serial_number)
            .await;
        Ok(Response::new(CheckCertificateStatusResponse {
            status: report.status.as_str().to_string(),
            reason: report.reason,
            revoked_at_unix_seconds: report.revoked_at_unix_seconds,
            not_after_unix_seconds: report.not_after_unix_seconds,
            auto_renew: report.auto_renew_enabled,
            renew_before_seconds: report.renew_before_seconds,
        }))
    }

    async fn update_auto_renew_policy(
        &self,
        request: Request<UpdateAutoRenewPolicyRequest>,
    ) -> Result<Response<UpdateAutoRenewPolicyResponse>, Status> {
        let req = request.into_inner();

        let updated = self
            .pki_app
            .update_auto_renew_policy(&req.serial_number, req.enabled, req.renew_before_seconds)
            .await
            .map_err(|e| coord_status(CoordError::Unavailable(e)))?;

        Ok(Response::new(UpdateAutoRenewPolicyResponse {
            updated: updated.updated,
            auto_renew: updated.auto_renew_enabled,
            renew_before_seconds: updated.renew_before_seconds,
            not_after_unix_seconds: updated.not_after_unix_seconds,
        }))
    }

    async fn run_auto_renew(
        &self,
        _request: Request<RunAutoRenewRequest>,
    ) -> Result<Response<RunAutoRenewResponse>, Status> {
        let execution = self.pki_app.run_auto_renew().await;

        Ok(Response::new(RunAutoRenewResponse {
            renewed_count: execution.renewed.len() as u32,
            renewed: execution
                .renewed
                .into_iter()
                .map(|item| ProtoAutoRenewedCertificate {
                    old_serial_number: item.old_serial_number,
                    new_serial_number: item.new_serial_number,
                    common_name: item.common_name,
                    not_after_unix_seconds: item.not_after_unix_seconds,
                })
                .collect(),
            errors: execution.errors,
        }))
    }

    async fn create_acme_order(
        &self,
        request: Request<CreateAcmeOrderRequest>,
    ) -> Result<Response<CreateAcmeOrderResponse>, Status> {
        let req = request.into_inner();
        let domains = if req.domain.is_empty() {
            req.domains
        } else {
            vec![req.domain]
        };
        let order = self
            .pki_app
            .create_acme_order(
                domains,
                req.ttl_seconds.max(60),
                &req.challenge_type,
                req.auto_renew,
                req.renew_before_seconds,
            )
            .await
            .map_err(|e| coord_status(CoordError::Unavailable(e)))?;

        let challenge_token = order
            .challenges
            .first()
            .map(|c| c.token.clone())
            .unwrap_or_default();

        Ok(Response::new(CreateAcmeOrderResponse {
            order_id: order.order_id,
            status: order.status,
            challenges: to_proto_acme_challenges(order.challenges),
            expires_unix_seconds: order.expires_unix_seconds,
            challenge_token,
        }))
    }

    async fn complete_acme_challenge(
        &self,
        request: Request<CompleteAcmeChallengeRequest>,
    ) -> Result<Response<CompleteAcmeChallengeResponse>, Status> {
        let req = request.into_inner();
        let order = self
            .pki_app
            .complete_acme_challenge(
                &req.order_id,
                if req.domain.is_empty() {
                    &req.challenge_token
                } else {
                    &req.domain
                },
                if req.token.is_empty() {
                    &req.challenge_token
                } else {
                    &req.token
                },
            )
            .await
            .map_err(|e| coord_status(CoordError::Unavailable(e)))?;

        Ok(Response::new(CompleteAcmeChallengeResponse {
            order_id: order.order_id,
            status: order.status,
            challenges: to_proto_acme_challenges(order.challenges),
        }))
    }

    async fn finalize_acme_order(
        &self,
        request: Request<FinalizeAcmeOrderRequest>,
    ) -> Result<Response<FinalizeAcmeOrderResponse>, Status> {
        let req = request.into_inner();

        if req.common_name.is_empty() && !req.csr_pem.is_empty() {
            return Err(Status::invalid_argument(
                "csr_pem was provided without common_name; CSR-based finalization is not yet supported — please specify common_name explicitly",
            ));
        }

        let common_name = if req.common_name.is_empty() {
            return Err(Status::invalid_argument("common_name is required"));
        } else {
            &req.common_name
        };

        let finalized = self
            .pki_app
            .finalize_acme_order(&req.order_id, common_name)
            .await
            .map_err(|e| coord_status(CoordError::Unavailable(e)))?;

        let certificate = finalized.certificate;

        Ok(Response::new(FinalizeAcmeOrderResponse {
            order_id: finalized.order_id,
            status: finalized.status,
            serial_number: certificate.serial_number,
            common_name: certificate.common_name,
            sans: certificate.sans,
            certificate_pem: certificate.certificate_pem,
            private_key_pem: certificate.private_key_pem,
            ca_certificate_pem: certificate.ca_certificate_pem,
            not_after_unix_seconds: certificate.not_after_unix_seconds,
            auto_renew: certificate.auto_renew_enabled,
            renew_before_seconds: certificate.renew_before_seconds,
        }))
    }

    async fn create_pki_role(
        &self,
        request: Request<CreatePkiRoleRequest>,
    ) -> Result<Response<CreatePkiRoleResponse>, Status> {
        let req = request.into_inner();
        Ok(Response::new(CreatePkiRoleResponse {
            role_name: req.role_name,
        }))
    }

    async fn get_crl(
        &self,
        _request: Request<GetCrlRequest>,
    ) -> Result<Response<GetCrlResponse>, Status> {
        let crl = self.pki_app.engine().get_crl(86400).await;
        let serials: Vec<&str> = crl
            .revoked_certificates
            .iter()
            .map(|e| e.serial_number.as_str())
            .collect();
        let crl_pem = format!(
            "CRL#{} revoked:{}\n{}",
            crl.crl_number,
            crl.revoked_certificates.len(),
            serials.join("\n")
        );
        Ok(Response::new(GetCrlResponse { crl_pem }))
    }
}
