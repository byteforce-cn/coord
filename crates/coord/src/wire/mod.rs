//! Wire-format adapter boundary for `coord-server`.
//!
//! This module is the single canonical boundary between the gRPC protobuf
//! surface (`coord_proto::coord::v1::*`) and the internal state machine /
//! domain types used by the rest of the server.
//!
//! # Rationale
//!
//! `coord-core` has **zero** dependency on `coord-proto`; all protobuf
//! types live on the server side. Keeping the adapter code inside a
//! dedicated `wire/` module has three concrete benefits:
//!
//! 1. Search-friendly: every `proto → domain` / `domain → proto` call
//!    can be found by scoping a search to `crates/coord-server/src/wire`.
//! 2. Test-friendly: adapter unit tests exercise conversion fidelity
//!    without spinning up a full gRPC server.
//! 3. Migration-friendly: when the wire format evolves (new proto
//!    version, gRPC-web, etc.) the blast radius is confined to this
//!    module.
//!
//! # Contents
//!
//! Currently the concentrated adapter pair is for Raft log entries:
//! protobuf types stay in `coord-server`, while the domain types live in
//! `coord_core::raft_runtime`.

pub mod error;
pub mod raft;
