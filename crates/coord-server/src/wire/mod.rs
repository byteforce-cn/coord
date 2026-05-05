//! Wire-format adapter boundary for `coord-server`.
//!
//! This module is the single canonical seam between the gRPC protobuf
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
//! Currently the only concentrated adapter pair is for the Raft log
//! entry (`raft_store::PersistedLogEntry::{to_proto,from_proto}`). That
//! adapter pair already lives in `raft_store.rs` for historic reasons;
//! the re-exports below mark the canonical wire surface so that new
//! adapters are discoverable from one place and so that follow-up work
//! can relocate the conversion code without touching call sites.
//!
//! Batch 4d round 1 intentionally keeps this module additive — no
//! behaviour moves here in this round. Subsequent refactors are
//! expected to pull conversion helpers out of `services.rs`,
//! `raft_runtime.rs` and `http_api.rs` into typed submodules.

pub mod error;
pub mod raft;
