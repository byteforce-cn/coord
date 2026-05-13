//! Ctl（管理 CLI）运行模式。
//!
//! 该模块包含原 `coord-ctl` `main()` 函数的全部逻辑，
//! 不改变任何行为：
//!
//! - `coord ctl <subcmd>` ≡ `coord-ctl <subcmd>`

use anyhow::Context;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use coord_proto::coord::v1::admin_service_client::AdminServiceClient;
use coord_proto::coord::v1::auth_service_client::AuthServiceClient;
use coord_proto::coord::v1::pki_service_client::PkiServiceClient;
use coord_proto::coord::v1::seal_service_client::SealServiceClient;
use coord_proto::coord::v1::transit_service_client::TransitServiceClient;
use coord_proto::coord::v1::workflow_service_client::WorkflowServiceClient;
use coord_proto::coord::v1::{
    BackupCreateRequest, BackupRestoreRequest, CheckCertificateStatusRequest, ClusterStatusRequest,
    CompleteAcmeChallengeRequest, CreateAcmeOrderRequest, CreateAppRoleRequest, CreateKeyRequest,
    DecryptRequest, DeployWorkflowDefinitionRequest, EncryptRequest, FinalizeAcmeOrderRequest,
    GenerateSecretIdRequest, GetCaChainRequest, GetCertificateRevocationListRequest,
    GetSealStatusRequest, GetWorkflowDefinitionRequest, GetWorkflowInstanceRequest,
    HmacSignRequest, HmacVerifyRequest, InitSecurityRequest, IssueCertificateRequest,
    ListWorkflowDefinitionsRequest, ListWorkflowInstancesRequest, LockListResponse,
    LoginAppRoleRequest, LookupTokenRequest, MemberAddRequest, MemberRemoveRequest,
    RenewCertificateRequest, RevokeCertificateRequest, RevokeTokenRequest, RotateKeyRequest,
    RotateRootKeyRequest, RunAutoRenewRequest, SealRequest, StartWorkflowV2Request, UnsealRequest,
    UpdateAutoRenewPolicyRequest,
};
use std::fs;
use tonic::Request;

use crate::cli::{
    AuthAppRoleSubCommand, AuthSubCommand, BackupSubCommand, ClusterSubCommand, CtlArgs,
    CtlCommand, LockSubCommand, MemberSubCommand, OperatorSubCommand, PkiSubCommand,
    TransitSubCommand, WorkflowSubCommand,
};

/// Entry point for `coord ctl`.
pub(crate) async fn run(args: CtlArgs) -> anyhow::Result<()> {
    // Destructure so the match-arms can freely move subcommand data without
    // fighting the borrow checker when we reach for `args.build_channel()`.
    let CtlArgs {
        endpoint,
        token,
        tls_ca,
        tls_cert,
        tls_key,
        tls_domain,
        command,
    } = args;
    // `command` is the real subcommand; `header` is a re-wrapped args struct that the
    // TLS/auth helpers need. This avoids `partial move of args` errors when
    // arms take ownership of their nested data.
    let header = CtlArgs {
        endpoint,
        token,
        tls_ca,
        tls_cert,
        tls_key,
        tls_domain,
        command: CtlCommand::Cluster(crate::cli::ClusterCommand {
            command: ClusterSubCommand::Status,
        }),
    };

    match command {
        CtlCommand::Cluster(cluster) => match cluster.command {
            ClusterSubCommand::Status => {
                let mut admin_client = AdminServiceClient::new(header.build_channel().await?);
                let resp = admin_client
                    .cluster_status(ClusterStatusRequest {})
                    .await?
                    .into_inner();
                println!("node_id: {}", resp.node_id);
                println!("state: {}", resp.state);
                println!("dev_mode: {}", resp.dev_mode);
                println!("members: {}", resp.members.join(", "));
            }
        },
        CtlCommand::Member(member) => match member.command {
            MemberSubCommand::Add { node_id, address } => {
                let mut admin_client = AdminServiceClient::new(header.build_channel().await?);
                let resp = admin_client
                    .member_add(MemberAddRequest { node_id, address })
                    .await?
                    .into_inner();
                println!("added: {}", resp.added);
                println!("members: {}", resp.members.join(", "));
            }
            MemberSubCommand::Remove {
                node_id,
                force_unreachable,
            } => {
                let mut admin_client = AdminServiceClient::new(header.build_channel().await?);
                let resp = admin_client
                    .member_remove(MemberRemoveRequest {
                        node_id,
                        force_unreachable,
                    })
                    .await?
                    .into_inner();
                println!("removed: {}", resp.removed);
                println!("members: {}", resp.members.join(", "));
            }
        },
        CtlCommand::Lock(lock) => match lock.command {
            LockSubCommand::List => {
                let mut admin_client = AdminServiceClient::new(header.build_channel().await?);
                let response: LockListResponse = admin_client.list_locks(()).await?.into_inner();
                if response.locks.is_empty() {
                    println!("no active locks");
                } else {
                    for lock in response.locks {
                        println!(
                            "lock={} owner={} expires_unix_ms={}",
                            lock.lock_name, lock.owner, lock.expires_unix_ms
                        );
                    }
                }
            }
        },
        CtlCommand::Operator(operator) => match operator.command {
            OperatorSubCommand::Init {
                shares_total,
                threshold,
            } => {
                let mut seal_client = SealServiceClient::new(header.build_channel().await?);
                let resp = seal_client
                    .init(InitSecurityRequest {
                        shares_total,
                        threshold,
                        secret_shares: 0,
                        secret_threshold: 0,
                    })
                    .await?
                    .into_inner();
                println!("initialized: {}", resp.initialized);
                println!("sealed: {}", resp.sealed);
                println!("shares_total: {}", resp.shares_total);
                println!("threshold: {}", resp.threshold);
                println!("unseal_shares:");
                for share in resp.unseal_shares {
                    println!("{}", share);
                }
            }
            OperatorSubCommand::SealStatus => {
                let mut seal_client = SealServiceClient::new(header.build_channel().await?);
                let resp = seal_client
                    .get_seal_status(GetSealStatusRequest {})
                    .await?
                    .into_inner();
                println!("initialized: {}", resp.initialized);
                println!("sealed: {}", resp.sealed);
                println!("shares_total: {}", resp.shares_total);
                println!("threshold: {}", resp.threshold);
                println!("progress: {}", resp.progress);
            }
            OperatorSubCommand::Seal => {
                let mut seal_client = SealServiceClient::new(header.build_channel().await?);
                let resp = seal_client.seal(SealRequest {}).await?.into_inner();
                println!("sealed: {}", resp.sealed);
            }
            OperatorSubCommand::Unseal { share } => {
                let mut seal_client = SealServiceClient::new(header.build_channel().await?);
                let resp = seal_client
                    .unseal(UnsealRequest {
                        share,
                        key_share: String::new(),
                    })
                    .await?
                    .into_inner();
                println!("sealed: {}", resp.sealed);
                println!("progress: {}", resp.progress);
                println!("threshold: {}", resp.threshold);
            }
            OperatorSubCommand::RotateRootKey {
                shares_total,
                threshold,
            } => {
                let mut seal_client = SealServiceClient::new(header.build_channel().await?);
                let resp = seal_client
                    .rotate_root_key(RotateRootKeyRequest {
                        shares_total,
                        threshold,
                    })
                    .await?
                    .into_inner();
                println!("rotated: {}", resp.rotated);
                println!("shares_total: {}", resp.shares_total);
                println!("threshold: {}", resp.threshold);
                println!("unseal_shares:");
                for share in resp.unseal_shares {
                    println!("{}", share);
                }
            }
        },
        CtlCommand::Auth(auth) => match auth.command {
            AuthSubCommand::Approle { command } => match command {
                AuthAppRoleSubCommand::Create {
                    role_name,
                    policy,
                    token_ttl_seconds,
                    secret_id_ttl_seconds,
                    secret_id_num_uses,
                } => {
                    let mut auth_client = AuthServiceClient::new(header.build_channel().await?);
                    let resp = auth_client
                        .create_app_role(CreateAppRoleRequest {
                            role_name,
                            policies: policy,
                            token_ttl_seconds,
                            secret_id_ttl_seconds,
                            secret_id_num_uses,
                        })
                        .await?
                        .into_inner();
                    println!("role_id: {}", resp.role_id);
                    println!("role_name: {}", resp.role_name);
                    println!("policies: {}", resp.policies.join(", "));
                    println!("token_ttl_seconds: {}", resp.token_ttl_seconds);
                    println!("secret_id_ttl_seconds: {}", resp.secret_id_ttl_seconds);
                    println!("secret_id_num_uses: {}", resp.secret_id_num_uses);
                }
                AuthAppRoleSubCommand::GenerateSecretId { role_id } => {
                    let mut auth_client = AuthServiceClient::new(header.build_channel().await?);
                    let resp = auth_client
                        .generate_secret_id(GenerateSecretIdRequest {
                            role_id,
                            role_name: String::new(),
                        })
                        .await?
                        .into_inner();
                    println!("role_id: {}", resp.role_id);
                    println!("secret_id: {}", resp.secret_id);
                    println!("expires_unix_seconds: {}", resp.expires_unix_seconds);
                }
                AuthAppRoleSubCommand::Login { role_id, secret_id } => {
                    let mut auth_client = AuthServiceClient::new(header.build_channel().await?);
                    let resp = auth_client
                        .login_app_role(LoginAppRoleRequest { role_id, secret_id })
                        .await?
                        .into_inner();
                    println!("access_token: {}", resp.access_token);
                    println!("role_id: {}", resp.role_id);
                    println!("policies: {}", resp.policies.join(", "));
                    println!("expires_unix_seconds: {}", resp.expires_unix_seconds);
                }
                AuthAppRoleSubCommand::Lookup { access_token } => {
                    let mut auth_client = AuthServiceClient::new(header.build_channel().await?);
                    let resp = auth_client
                        .lookup_token(LookupTokenRequest {
                            access_token,
                            token: String::new(),
                        })
                        .await?
                        .into_inner();
                    println!("valid: {}", resp.valid);
                    println!("role_id: {}", resp.role_id);
                    println!("policies: {}", resp.policies.join(", "));
                    println!("expires_unix_seconds: {}", resp.expires_unix_seconds);
                }
                AuthAppRoleSubCommand::Revoke { access_token } => {
                    let mut auth_client = AuthServiceClient::new(header.build_channel().await?);
                    let resp = auth_client
                        .revoke_token(RevokeTokenRequest {
                            access_token,
                            token: String::new(),
                        })
                        .await?
                        .into_inner();
                    println!("revoked: {}", resp.revoked);
                }
            },
        },
        CtlCommand::Workflow(workflow) => match workflow.command {
            WorkflowSubCommand::Deploy {
                definition_id,
                file,
            } => {
                let yaml = fs::read_to_string(&file)
                    .context(format!("failed to read workflow definition file: {file}"))?;
                let mut workflow_client = WorkflowServiceClient::new(header.build_channel().await?);
                let resp = workflow_client
                    .deploy_workflow_definition(DeployWorkflowDefinitionRequest {
                        definition_id: definition_id.unwrap_or_default(),
                        version: String::new(),
                        definition_yaml: yaml,
                    })
                    .await?
                    .into_inner();
                println!(
                    "deployed: {}/{} v{}",
                    resp.namespace, resp.name, resp.version
                );
                println!("definition_id: {}", resp.definition_id);
            }
            WorkflowSubCommand::Start {
                definition_id,
                namespace,
                version,
                input_json,
            } => {
                let mut workflow_client = WorkflowServiceClient::new(header.build_channel().await?);
                let resp = workflow_client
                    .start_workflow_v2(StartWorkflowV2Request {
                        definition_id,
                        namespace,
                        name: String::new(),
                        version,
                        input_json,
                    })
                    .await?
                    .into_inner();
                println!("instance_id: {}", resp.instance_id);
                println!("status: {}", resp.status);
            }
            WorkflowSubCommand::Get { instance_id } => {
                let mut workflow_client = WorkflowServiceClient::new(header.build_channel().await?);
                let resp = workflow_client
                    .get_workflow_instance(GetWorkflowInstanceRequest { instance_id })
                    .await?
                    .into_inner();
                if let Some(inst) = resp.instance {
                    println!("instance_id: {}", inst.instance_id);
                    println!(
                        "definition: {}/{} v{}",
                        inst.definition_ns, inst.definition_name, inst.definition_version
                    );
                    println!("status: {}", inst.status);
                    if !inst.output_json.is_empty() {
                        println!("output: {}", inst.output_json);
                    }
                    if !inst.fault_json.is_empty() {
                        println!("fault: {}", inst.fault_json);
                    }
                } else {
                    println!("instance not found");
                }
            }
            WorkflowSubCommand::List {
                namespace,
                definition_name,
            } => {
                let mut workflow_client = WorkflowServiceClient::new(header.build_channel().await?);
                let resp = workflow_client
                    .list_workflow_instances(ListWorkflowInstancesRequest {
                        namespace,
                        definition_name,
                        definition_id: String::new(),
                    })
                    .await?
                    .into_inner();
                for inst in &resp.instances {
                    println!(
                        "{} {} {}/{} v{}",
                        inst.instance_id,
                        inst.status,
                        inst.definition_ns,
                        inst.definition_name,
                        inst.definition_version
                    );
                }
                if resp.instances.is_empty() {
                    println!("no workflow instances");
                }
            }
            WorkflowSubCommand::Definitions { namespace } => {
                let mut workflow_client = WorkflowServiceClient::new(header.build_channel().await?);
                let resp = workflow_client
                    .list_workflow_definitions(ListWorkflowDefinitionsRequest { namespace })
                    .await?
                    .into_inner();
                for def in &resp.definitions {
                    println!(
                        "{} {}/{} v{}",
                        def.definition_id, def.namespace, def.name, def.version
                    );
                }
                if resp.definitions.is_empty() {
                    println!("no workflow definitions");
                }
            }
            WorkflowSubCommand::Definition {
                definition_id,
                version,
            } => {
                let mut workflow_client = WorkflowServiceClient::new(header.build_channel().await?);
                let resp = workflow_client
                    .get_workflow_definition(GetWorkflowDefinitionRequest {
                        definition_id,
                        version,
                    })
                    .await?
                    .into_inner();
                println!("{}", resp.definition_yaml);
            }
        },
        CtlCommand::Transit(transit) => match transit.command {
            TransitSubCommand::CreateKey { key_name } => {
                let mut transit_client = TransitServiceClient::new(header.build_channel().await?);
                let resp = transit_client
                    .create_key(request_with_token(
                        CreateKeyRequest {
                            key_name,
                            algorithm: String::new(),
                        },
                        &header.token,
                    )?)
                    .await?
                    .into_inner();
                println!("key_name: {}", resp.key_name);
                println!("primary_version: {}", resp.primary_version);
            }
            TransitSubCommand::Encrypt {
                key_name,
                plaintext,
            } => {
                let mut transit_client = TransitServiceClient::new(header.build_channel().await?);
                let resp = transit_client
                    .encrypt(request_with_token(
                        EncryptRequest {
                            key_name,
                            plaintext,
                        },
                        &header.token,
                    )?)
                    .await?
                    .into_inner();
                println!("ciphertext: {}", resp.ciphertext);
                println!("version: {}", resp.version);
            }
            TransitSubCommand::Decrypt {
                key_name,
                ciphertext,
            } => {
                let mut transit_client = TransitServiceClient::new(header.build_channel().await?);
                let resp = transit_client
                    .decrypt(request_with_token(
                        DecryptRequest {
                            key_name,
                            ciphertext,
                        },
                        &header.token,
                    )?)
                    .await?
                    .into_inner();
                println!("version: {}", resp.version);
                println!(
                    "plaintext_base64: {}",
                    BASE64.encode(resp.plaintext.as_bytes())
                );
                println!("plaintext_utf8: {}", resp.plaintext);
            }
            TransitSubCommand::RotateKey { key_name } => {
                let mut transit_client = TransitServiceClient::new(header.build_channel().await?);
                let resp = transit_client
                    .rotate_key(request_with_token(
                        RotateKeyRequest { key_name },
                        &header.token,
                    )?)
                    .await?
                    .into_inner();
                println!("key_name: {}", resp.key_name);
                println!("primary_version: {}", resp.primary_version);
            }
            TransitSubCommand::HmacSign { key_name, data } => {
                let mut transit_client = TransitServiceClient::new(header.build_channel().await?);
                let resp = transit_client
                    .hmac_sign(request_with_token(
                        HmacSignRequest {
                            key_name,
                            input: data,
                        },
                        &header.token,
                    )?)
                    .await?
                    .into_inner();
                println!("signature: {}", resp.hmac);
                println!("version: {}", resp.version);
            }
            TransitSubCommand::HmacVerify {
                key_name,
                data,
                signature,
            } => {
                let mut transit_client = TransitServiceClient::new(header.build_channel().await?);
                let resp = transit_client
                    .hmac_verify(request_with_token(
                        HmacVerifyRequest {
                            key_name,
                            input: data,
                            hmac: signature,
                        },
                        &header.token,
                    )?)
                    .await?
                    .into_inner();
                println!("ok: {}", resp.valid);
            }
        },
        CtlCommand::Pki(pki) => match pki.command {
            PkiSubCommand::Issue {
                common_name,
                san,
                ttl_seconds,
                auto_renew,
                renew_before_seconds,
            } => {
                let mut pki_client = PkiServiceClient::new(header.build_channel().await?);
                let resp = pki_client
                    .issue_certificate(request_with_token(
                        IssueCertificateRequest {
                            common_name,
                            sans: san,
                            ttl_seconds,
                            auto_renew,
                            renew_before_seconds,
                            role_name: String::new(),
                            ttl: String::new(),
                        },
                        &header.token,
                    )?)
                    .await?
                    .into_inner();
                println!("serial_number: {}", resp.serial_number);
                println!("common_name: {}", resp.common_name);
                println!("not_after_unix_seconds: {}", resp.not_after_unix_seconds);
                println!("auto_renew: {}", resp.auto_renew);
                println!("renew_before_seconds: {}", resp.renew_before_seconds);
                println!("certificate_pem:\n{}", resp.certificate_pem);
                println!("private_key_pem:\n{}", resp.private_key_pem);
                println!("ca_certificate_pem:\n{}", resp.ca_certificate_pem);
            }
            PkiSubCommand::Renew {
                serial_number,
                ttl_seconds,
            } => {
                let mut pki_client = PkiServiceClient::new(header.build_channel().await?);
                let resp = pki_client
                    .renew_certificate(request_with_token(
                        RenewCertificateRequest {
                            serial_number,
                            ttl_seconds,
                            ttl: String::new(),
                        },
                        &header.token,
                    )?)
                    .await?
                    .into_inner();
                println!("old_serial_number: {}", resp.old_serial_number);
                println!("new_serial_number: {}", resp.new_serial_number);
                println!("common_name: {}", resp.common_name);
                println!("not_after_unix_seconds: {}", resp.not_after_unix_seconds);
                println!("auto_renew: {}", resp.auto_renew);
                println!("renew_before_seconds: {}", resp.renew_before_seconds);
                println!("certificate_pem:\n{}", resp.certificate_pem);
                println!("private_key_pem:\n{}", resp.private_key_pem);
                println!("ca_certificate_pem:\n{}", resp.ca_certificate_pem);
            }
            PkiSubCommand::Revoke {
                serial_number,
                reason,
            } => {
                let mut pki_client = PkiServiceClient::new(header.build_channel().await?);
                let resp = pki_client
                    .revoke_certificate(request_with_token(
                        RevokeCertificateRequest {
                            serial_number,
                            reason,
                        },
                        &header.token,
                    )?)
                    .await?
                    .into_inner();
                println!("revoked: {}", resp.revoked);
            }
            PkiSubCommand::CaChain => {
                let mut pki_client = PkiServiceClient::new(header.build_channel().await?);
                let resp = pki_client
                    .get_ca_chain(request_with_token(GetCaChainRequest {}, &header.token)?)
                    .await?
                    .into_inner();
                println!("ca_certificate_pem:\n{}", resp.ca_certificate_pem);
            }
            PkiSubCommand::Crl {
                next_update_seconds,
            } => {
                let mut pki_client = PkiServiceClient::new(header.build_channel().await?);
                let resp = pki_client
                    .get_certificate_revocation_list(request_with_token(
                        GetCertificateRevocationListRequest {
                            next_update_seconds,
                        },
                        &header.token,
                    )?)
                    .await?
                    .into_inner();
                println!("crl_number: {}", resp.crl_number);
                println!(
                    "this_update_unix_seconds: {}",
                    resp.this_update_unix_seconds
                );
                println!(
                    "next_update_unix_seconds: {}",
                    resp.next_update_unix_seconds
                );
                if resp.revoked_certificates.is_empty() {
                    println!("revoked_certificates: <empty>");
                } else {
                    for item in resp.revoked_certificates {
                        println!(
                            "revoked serial={} reason={} revoked_at_unix_seconds={}",
                            item.serial_number, item.reason, item.revoked_at_unix_seconds
                        );
                    }
                }
            }
            PkiSubCommand::Ocsp { serial_number } => {
                let mut pki_client = PkiServiceClient::new(header.build_channel().await?);
                let resp = pki_client
                    .check_certificate_status(request_with_token(
                        CheckCertificateStatusRequest { serial_number },
                        &header.token,
                    )?)
                    .await?
                    .into_inner();
                println!("status: {}", resp.status);
                println!("reason: {}", resp.reason);
                println!("revoked_at_unix_seconds: {}", resp.revoked_at_unix_seconds);
                println!("not_after_unix_seconds: {}", resp.not_after_unix_seconds);
                println!("auto_renew: {}", resp.auto_renew);
                println!("renew_before_seconds: {}", resp.renew_before_seconds);
            }
            PkiSubCommand::SetAutoRenewPolicy {
                serial_number,
                enabled,
                renew_before_seconds,
            } => {
                let mut pki_client = PkiServiceClient::new(header.build_channel().await?);
                let resp = pki_client
                    .update_auto_renew_policy(request_with_token(
                        UpdateAutoRenewPolicyRequest {
                            serial_number,
                            enabled,
                            renew_before_seconds,
                        },
                        &header.token,
                    )?)
                    .await?
                    .into_inner();
                println!("updated: {}", resp.updated);
                println!("auto_renew: {}", resp.auto_renew);
                println!("renew_before_seconds: {}", resp.renew_before_seconds);
                println!("not_after_unix_seconds: {}", resp.not_after_unix_seconds);
            }
            PkiSubCommand::RunAutoRenew => {
                let mut pki_client = PkiServiceClient::new(header.build_channel().await?);
                let resp = pki_client
                    .run_auto_renew(request_with_token(RunAutoRenewRequest {}, &header.token)?)
                    .await?
                    .into_inner();
                println!("renewed_count: {}", resp.renewed_count);
                for item in resp.renewed {
                    println!(
                        "renewed old_serial={} new_serial={} common_name={} not_after_unix_seconds={}",
                        item.old_serial_number,
                        item.new_serial_number,
                        item.common_name,
                        item.not_after_unix_seconds
                    );
                }
                if !resp.errors.is_empty() {
                    println!("errors:");
                    for err in resp.errors {
                        println!("  {}", err);
                    }
                }
            }
            PkiSubCommand::AcmeOrder {
                domain,
                ttl_seconds,
                challenge_type,
                auto_renew,
                renew_before_seconds,
            } => {
                let mut pki_client = PkiServiceClient::new(header.build_channel().await?);
                let resp = pki_client
                    .create_acme_order(request_with_token(
                        CreateAcmeOrderRequest {
                            domains: domain,
                            ttl_seconds,
                            challenge_type,
                            auto_renew,
                            renew_before_seconds,
                            domain: String::new(),
                        },
                        &header.token,
                    )?)
                    .await?
                    .into_inner();
                println!("order_id: {}", resp.order_id);
                println!("status: {}", resp.status);
                println!("expires_unix_seconds: {}", resp.expires_unix_seconds);
                for challenge in resp.challenges {
                    println!(
                        "challenge domain={} type={} token={} validated={}",
                        challenge.domain,
                        challenge.challenge_type,
                        challenge.token,
                        challenge.validated
                    );
                }
            }
            PkiSubCommand::AcmeChallenge {
                order_id,
                domain,
                token,
            } => {
                let mut pki_client = PkiServiceClient::new(header.build_channel().await?);
                let resp = pki_client
                    .complete_acme_challenge(request_with_token(
                        CompleteAcmeChallengeRequest {
                            order_id,
                            domain,
                            token,
                            challenge_token: String::new(),
                        },
                        &header.token,
                    )?)
                    .await?
                    .into_inner();
                println!("order_id: {}", resp.order_id);
                println!("status: {}", resp.status);
                for challenge in resp.challenges {
                    println!(
                        "challenge domain={} type={} validated={}",
                        challenge.domain, challenge.challenge_type, challenge.validated
                    );
                }
            }
            PkiSubCommand::AcmeFinalize {
                order_id,
                common_name,
            } => {
                let mut pki_client = PkiServiceClient::new(header.build_channel().await?);
                let resp = pki_client
                    .finalize_acme_order(request_with_token(
                        FinalizeAcmeOrderRequest {
                            order_id,
                            common_name,
                            csr_pem: String::new(),
                        },
                        &header.token,
                    )?)
                    .await?
                    .into_inner();
                println!("order_id: {}", resp.order_id);
                println!("status: {}", resp.status);
                println!("serial_number: {}", resp.serial_number);
                println!("common_name: {}", resp.common_name);
                println!("not_after_unix_seconds: {}", resp.not_after_unix_seconds);
                println!("auto_renew: {}", resp.auto_renew);
                println!("renew_before_seconds: {}", resp.renew_before_seconds);
                println!("certificate_pem:\n{}", resp.certificate_pem);
                println!("private_key_pem:\n{}", resp.private_key_pem);
                println!("ca_certificate_pem:\n{}", resp.ca_certificate_pem);
            }
        },
        CtlCommand::Backup(backup) => match backup.command {
            BackupSubCommand::Create { file } => {
                let mut admin_client = AdminServiceClient::new(header.build_channel().await?);
                let resp = admin_client
                    .create_backup(BackupCreateRequest {})
                    .await?
                    .into_inner();
                fs::write(&file, resp.payload_json)
                    .with_context(|| format!("failed to write backup file: {file}"))?;
                println!("backup_file: {}", file);
                println!("created_unix_ms: {}", resp.created_unix_ms);
            }
            BackupSubCommand::Restore { file } => {
                let payload_json = fs::read_to_string(&file)
                    .with_context(|| format!("failed to read backup file: {file}"))?;
                let mut admin_client = AdminServiceClient::new(header.build_channel().await?);
                let resp = admin_client
                    .restore_backup(BackupRestoreRequest { payload_json })
                    .await?
                    .into_inner();
                println!("restored: {}", resp.restored);
                println!("message: {}", resp.message);
            }
        },
    }

    Ok(())
}

fn request_with_token<T>(message: T, token: &Option<String>) -> anyhow::Result<Request<T>> {
    let mut request = Request::new(message);
    if let Some(value) = token
        .as_ref()
        .map(|item| item.trim())
        .filter(|item| !item.is_empty())
    {
        let header = format!("Bearer {}", value);
        let parsed = header
            .parse()
            .context("failed to encode authorization metadata header")?;
        request.metadata_mut().insert("authorization", parsed);
    }
    Ok(request)
}
