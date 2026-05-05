use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

#[cfg(feature = "workflow-preview")]
use crate::workflow_adapters::{CoordWorkflowRuntime, new_coord_workflow_runtime};
use coord_core::clock::{Clock, SystemClock};
use coord_core::config::ConfigEntry;
use coord_core::idgen::Snowflake;
use coord_core::lock::LockManager;
use coord_core::metrics::CoordMetrics;
use coord_core::pki::CertificateIssueOptions;
use coord_core::registry::{ServiceInstance as CoreServiceInstance, ServiceRegistry};
use coord_core::security::{
    DomainLifecycleManager, SecurityController, SecurityRoleSnapshot, create_root_token_snapshot,
    generate_root_token,
};
use coord_core::state::RuntimeConfig;
use coord_core::validation::validate_key;
#[cfg(feature = "workflow-preview")]
use coord_core::workflow::engine::InstanceStatus;
#[cfg(feature = "workflow-preview")]
use coord_core::workflow::parser::parse_yaml;
#[cfg(feature = "workflow-preview")]
use coord_core::workflow::ports::WorkflowStore;
use coord_proto::coord::v1::admin_service_server::AdminService;
use coord_proto::coord::v1::auth_service_server::AuthService;
use coord_proto::coord::v1::config_service_server::ConfigService;
use coord_proto::coord::v1::id_gen_service_server::IdGenService;
use coord_proto::coord::v1::lock_service_server::LockService;
use coord_proto::coord::v1::pki_service_server::PkiService;
use coord_proto::coord::v1::registry_service_server::RegistryService;
use coord_proto::coord::v1::seal_service_server::SealService;
use coord_proto::coord::v1::transit_service_server::TransitService;
#[cfg(feature = "workflow-preview")]
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
#[cfg(feature = "workflow-preview")]
use coord_proto::coord::v1::{
    DeployWorkflowDefinitionRequest, DeployWorkflowDefinitionResponse,
    GetWorkflowDefinitionRequest, GetWorkflowDefinitionResponse, GetWorkflowInstanceRequest,
    GetWorkflowInstanceResponse, ListWorkflowDefinitionsRequest, ListWorkflowDefinitionsResponse,
    ListWorkflowInstancesRequest, ListWorkflowInstancesResponse, ResumeWorkflowRequest,
    ResumeWorkflowResponse, StartWorkflowV2Request, StartWorkflowV2Response,
    WorkflowDefinitionInfo, WorkflowInstanceInfo,
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
mod registry;
mod seal;
mod transit;
#[cfg(feature = "workflow-preview")]
mod workflow;

pub use admin::AdminGrpc;
pub use auth::AuthGrpc;
pub use config::ConfigGrpc;
pub use idgen::IdGenGrpc;
pub use lock::LockGrpc;
pub use pki::PkiGrpc;
pub use registry::RegistryGrpc;
pub use seal::SealGrpc;
pub use transit::TransitGrpc;
#[cfg(feature = "workflow-preview")]
pub use workflow::WorkflowGrpc;
