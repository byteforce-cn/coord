//! Shared conversion helpers used by multiple gRPC service impls.
//!
//! Extracted during the Batch 4a split. Crate-internal visibility so they
//! can be forwarded to every sibling module via `use super::*;`.

use super::*;
use coord_core::error::CoordError;

// tonic::Status is a large type; callers box Err variants themselves at call sites.
#[allow(clippy::result_large_err)]
pub(crate) fn to_core_instance(instance: ServiceInstance) -> Result<CoreServiceInstance, Status> {
    if instance.service_name.is_empty() {
        return Err(coord_status(CoordError::InvalidArgument(
            "service_name cannot be empty".to_string(),
        )));
    }
    if instance.instance_id.is_empty() {
        return Err(coord_status(CoordError::InvalidArgument(
            "instance_id cannot be empty".to_string(),
        )));
    }
    if instance.host.is_empty() {
        return Err(coord_status(CoordError::InvalidArgument(
            "host cannot be empty".to_string(),
        )));
    }

    Ok(CoreServiceInstance {
        service_name: instance.service_name,
        instance_id: instance.instance_id,
        host: instance.host,
        port: instance.port,
        metadata: instance.metadata,
    })
}

pub(crate) fn to_proto_instance(instance: CoreServiceInstance) -> ServiceInstance {
    ServiceInstance {
        service_name: instance.service_name,
        instance_id: instance.instance_id,
        host: instance.host,
        port: instance.port,
        metadata: instance.metadata,
    }
}

pub(crate) fn to_proto_config(entry: ConfigEntry) -> ConfigResponse {
    ConfigResponse {
        key: entry.key,
        value: entry.value,
        version: entry.version,
    }
}

pub(crate) fn to_proto_acme_challenges(
    challenges: Vec<coord_core::pki::AcmeChallenge>,
) -> Vec<ProtoAcmeChallenge> {
    challenges
        .into_iter()
        .map(|challenge| ProtoAcmeChallenge {
            domain: challenge.domain,
            challenge_type: challenge.challenge_type,
            token: challenge.token,
            validated: challenge.validated,
        })
        .collect()
}
