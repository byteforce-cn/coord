use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use coord_core::config::ConfigEntry;
use coord_core::lock::LockManager;
use coord_core::metrics::CoordMetrics;
use coord_core::pki::CertificateIssueOptions;
use coord_core::registry::ServiceInstance as CoreServiceInstance;
use coord_core::state::RuntimeConfig;
use coord_core::validation::validate_key;
use coord_core::workflow::engine::InstanceStatus;
use coord_core::workflow::parser::parse_yaml;
use coord_proto::coord::v1::admin_service_server::AdminService;
use coord_proto::coord::v1::auth_service_server::AuthService;
use coord_proto::coord::v1::config_service_server::ConfigService;
use coord_proto::coord::v1::id_gen_service_server::IdGenService;
use coord_proto::coord::v1::lock_service_server::LockService;
use coord_proto::coord::v1::pki_service_server::PkiService;
use coord_proto::coord::v1::registry_service_server::RegistryService;
use coord_proto::coord::v1::seal_service_server::SealService;
use coord_proto::coord::v1::transit_service_server::TransitService;
use coord_proto::coord::v1::workflow_service_server::WorkflowService;
use coord_proto::coord::v1::{
    AcmeChallenge as ProtoAcmeChallenge, AutoRenewedCertificate as ProtoAutoRenewedCertificate,
    BackupCreateRequest, BackupCreateResponse, BackupRestoreRequest, BackupRestoreResponse,
    CheckCertificateStatusRequest, CheckCertificateStatusResponse, ClusterStatusRequest,
    ClusterStatusResponse, CompleteAcmeChallengeRequest, CompleteAcmeChallengeResponse,
    ConfigRequest, ConfigResponse, CreateAcmeOrderRequest, CreateAcmeOrderResponse,
    CreateAppRoleRequest, CreateAppRoleResponse, CreateKeyRequest, CreateKeyResponse,
    CreatePkiRoleRequest, CreatePkiRoleResponse, DecryptRequest, DecryptResponse, EncryptRequest,
    EncryptResponse, FinalizeAcmeOrderRequest, FinalizeAcmeOrderResponse, GenerateSecretIdRequest,
    GenerateSecretIdResponse, GetAppRoleIdRequest, GetAppRoleIdResponse, GetCaChainRequest,
    GetCaChainResponse, GetCertificateRevocationListRequest, GetCertificateRevocationListResponse,
    GetCrlRequest, GetCrlResponse, GetSealStatusRequest, GetSealStatusResponse,
    GetTransitKeyRequest, GetTransitKeyResponse, HmacSignRequest, HmacSignResponse,
    HmacVerifyRequest, HmacVerifyResponse, InitSecurityRequest, InitSecurityResponse,
    IssueCertificateRequest, IssueCertificateResponse, Lease, LockAcquireRequest,
    LockAcquireResponse, LockInfo, LockKeepAliveRequest, LockKeepAliveResponse, LockListResponse,
    LockReleaseRequest, LockReleaseResponse, LoginAppRoleRequest, LoginAppRoleResponse,
    LookupTokenRequest, LookupTokenResponse, MemberAddRequest, MemberAddResponse,
    MemberRemoveRequest, MemberRemoveResponse, PutConfigRequest, RegisterRequest,
    RenewCertificateRequest, RenewCertificateResponse, RevokeCertificateRequest,
    RevokeCertificateResponse, RevokeTokenRequest, RevokeTokenResponse, RevokedCertificateItem,
    RotateKeyRequest, RotateKeyResponse, RotateRootKeyRequest, RotateRootKeyResponse,
    RunAutoRenewRequest, RunAutoRenewResponse, SealRequest, SealResponse, ServiceInstance,
    ServiceQuery, SnowflakeRequest, SnowflakeResponse, UnsealRequest, UnsealResponse,
    UpdateAutoRenewPolicyRequest, UpdateAutoRenewPolicyResponse,
};
use coord_proto::coord::v1::{
    DeployWorkflowDefinitionRequest, DeployWorkflowDefinitionResponse,
    GetWorkflowDefinitionRequest, GetWorkflowDefinitionResponse, GetWorkflowInstanceRequest,
    GetWorkflowInstanceResponse, ListWorkflowDefinitionsRequest, ListWorkflowDefinitionsResponse,
    ListWorkflowInstancesRequest, ListWorkflowInstancesResponse, ResumeWorkflowRequest,
    ResumeWorkflowResponse, StartWorkflowV2Request, StartWorkflowV2Response,
    WorkflowDefinitionInfo, WorkflowInstanceInfo,
};
use coord_proto::coord::v1::policy_service_server::PolicyService;
use coord_proto::coord::v1::{
    DeletePolicyBundleRequest, EvaluateRequest, EvaluateResponse, ExplainResponse,
    ListPolicyBundlesRequest, ListPolicyBundlesResponse, PolicyBundleInfo,
    PutPolicyBundleRequest, PutPolicyBundleResponse, SetBundleEnabledRequest,
};
use futures::Stream;
use futures::StreamExt;
use tokio::sync::{RwLock, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::persistence;
use crate::raft_runtime::RaftRuntime;
use crate::wire::error::coord_status;

mod helpers;
pub(crate) use helpers::*;

mod admin;
mod auth;
mod config;
mod idgen;
mod lock;
mod pki;
mod policy;
mod registry;
mod seal;
mod transit;
mod workflow;

pub use admin::AdminGrpc;
pub use auth::AuthGrpc;
pub use config::ConfigGrpc;
pub use idgen::IdGenGrpc;
pub use lock::LockGrpc;
pub use pki::PkiGrpc;
pub use policy::PolicyGrpc;
pub use registry::RegistryGrpc;
pub use seal::SealGrpc;
pub use transit::TransitGrpc;
pub use workflow::WorkflowGrpc;
