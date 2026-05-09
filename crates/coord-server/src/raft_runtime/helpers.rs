//! RaftRuntime helper re-exports from coord-core.

#[cfg(test)]
pub(super) use coord_core::raft_runtime::{ELECTION_TIMEOUT_BASE, ELECTION_TIMEOUT_JITTER_MAX};
pub(super) use coord_core::raft_runtime::{
    majority, members_to_nodes, normalize_endpoint, random_election_timeout,
};
