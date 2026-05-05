//! Application service layer.
//!
//! This module contains use-case façades that sit between the transport
//! handlers (gRPC / HTTP) and the coord-core domain modules. Each façade:
//!
//! - Owns the business-combination logic (token generation, TTL math,
//!   command encoding / result decoding).
//! - Centralises cross-cutting concerns (metrics, audit).
//! - Returns `coord_core` types – no protobuf types leak into this layer.
//!
//! Transport handlers translate proto types to/from application types and
//! delegate all domain work to the façade.

pub mod config_app;
pub mod lock_app;
pub mod pki_app;
pub mod transit_app;
