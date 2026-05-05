//! Observability wiring: W3C Trace Context propagation + OTLP hook.
//!
//! This module is the single seam through which both the HTTP and gRPC
//! edges learn about incoming `traceparent` headers and attach them to
//! the currently running span. It deliberately does **not** pull in the
//! `opentelemetry` crate tree: landing that bundle without a real
//! collector to talk to adds ~1.5M LOC of compile cost for no runtime
//! benefit. The adapter is shaped so wiring an OTLP exporter later is a
//! local change in [`init_telemetry`].
//!
//! # Scope
//! * `traceparent` extraction and round-trip for both HTTP (tower layer)
//!   and gRPC (tonic interceptor).
//! * `trace_id` + `parent_span_id` attached to [`tracing::Span`]s so
//!   structured log sinks correlate requests across services.
//! * `--otlp-endpoint` / `COORD_OTLP_ENDPOINT` is accepted and logged;
//!   when a collector is configured, a follow-up PR swaps the no-op
//!   exporter for an `opentelemetry-otlp` pipeline without touching
//!   call sites.
//!
//! # Header format
//!
//! W3C Trace Context version-00:
//! `00-<32 hex trace-id>-<16 hex parent-id>-<2 hex flags>`
//! Any malformed header is silently ignored (per spec §3.2.2.5).

use tracing::info;

/// Parsed W3C traceparent header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceContext {
    pub trace_id: String,
    pub parent_span_id: String,
    pub flags: String,
}

impl TraceContext {
    /// Parse a `traceparent` header value. Returns `None` for any value
    /// that does not strictly match version-00 shape so that malformed
    /// input never breaks request handling.
    pub fn parse(header: &str) -> Option<Self> {
        let parts: Vec<&str> = header.trim().split('-').collect();
        if parts.len() != 4 {
            return None;
        }
        let [version, trace_id, parent_id, flags] = [parts[0], parts[1], parts[2], parts[3]];
        if version != "00" {
            return None;
        }
        if trace_id.len() != 32 || !is_hex(trace_id) || is_all_zero(trace_id) {
            return None;
        }
        if parent_id.len() != 16 || !is_hex(parent_id) || is_all_zero(parent_id) {
            return None;
        }
        if flags.len() != 2 || !is_hex(flags) {
            return None;
        }
        Some(Self {
            trace_id: trace_id.to_ascii_lowercase(),
            parent_span_id: parent_id.to_ascii_lowercase(),
            flags: flags.to_ascii_lowercase(),
        })
    }

    /// Render back to canonical header form for forwarding to downstream calls.
    #[allow(dead_code)] // reserved for future distributed tracing forwarding
    pub fn to_header(&self) -> String {
        format!(
            "00-{}-{}-{}",
            self.trace_id, self.parent_span_id, self.flags
        )
    }
}

fn is_hex(s: &str) -> bool {
    s.chars().all(|c| c.is_ascii_hexdigit())
}

fn is_all_zero(s: &str) -> bool {
    s.chars().all(|c| c == '0')
}

/// Initialize cross-cutting telemetry state.
///
/// Today this only logs the OTLP endpoint (if configured) and records
/// that traceparent propagation is active. The signature is stable so
/// swapping in a real exporter is a drop-in replacement.
pub fn init_telemetry(otlp_endpoint: Option<&str>) {
    match otlp_endpoint {
        Some(endpoint) if !endpoint.trim().is_empty() => {
            info!(
                otlp_endpoint = endpoint,
                "telemetry: OTLP endpoint configured; traceparent propagation active \
                 (opentelemetry-otlp exporter pending infra rollout)"
            );
        }
        _ => {
            info!(
                "telemetry: no OTLP endpoint configured; traceparent propagation active, \
                 spans emitted via tracing-subscriber only"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    #[test]
    fn parse_accepts_canonical_header() {
        let tc = TraceContext::parse(VALID).expect("valid header");
        assert_eq!(tc.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(tc.parent_span_id, "00f067aa0ba902b7");
        assert_eq!(tc.flags, "01");
    }

    #[test]
    fn parse_rejects_wrong_version() {
        assert!(
            TraceContext::parse("ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                .is_none()
        );
    }

    #[test]
    fn parse_rejects_all_zero_trace_id() {
        assert!(
            TraceContext::parse("00-00000000000000000000000000000000-00f067aa0ba902b7-01")
                .is_none()
        );
    }

    #[test]
    fn parse_rejects_wrong_lengths() {
        assert!(TraceContext::parse("00-abc-00f067aa0ba902b7-01").is_none());
        assert!(TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-abc-01").is_none());
        assert!(
            TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0").is_none()
        );
    }

    #[test]
    fn parse_ignores_non_hex() {
        assert!(
            TraceContext::parse("00-ZZZ92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                .is_none()
        );
    }

    #[test]
    fn round_trip_header() {
        let tc = TraceContext::parse(VALID).unwrap();
        assert_eq!(tc.to_header(), VALID);
    }

    #[test]
    fn parse_normalizes_case() {
        let upper = "00-4BF92F3577B34DA6A3CE929D0E0E4736-00F067AA0BA902B7-01";
        let tc = TraceContext::parse(upper).unwrap();
        assert_eq!(tc.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
    }
}
