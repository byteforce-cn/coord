// HMAC-SHA256 Performance Benchmark (Phase 0.2)
//
// Validates that CCT token signing + verification meets the < 1ms/req
// performance target specified in the design doc.
//
// See docs/capability-auth-implementation.md §0.2, §4.4.

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Instant;

    use coord_core::auth::cct::{decode_cct, encode_cct, CctHeader, CctPayload};

    const BENCH_SIGNING_KEY: &[u8] = b"bench-signing-key--32-bytes-long!!";

    fn make_payload() -> CctPayload {
        CctPayload {
            jti: "bench-jti-fixed".to_string(),
            iss: "bench-cluster".to_string(),
            sub: "bench-app".to_string(),
            aud: vec!["coord-agent".to_string()],
            iat: 1719990000,
            exp: 2000000000,
            roles: vec!["service-reader".to_string(), "service-writer".to_string()],
            scope_overrides: HashMap::new(),
        }
    }

    // ──── Phase 0.2: Encode benchmark ────

    #[test]
    fn test_cct_encode_performance_target() {
        let header = CctHeader::default();
        let payload = make_payload();
        let iterations = 1000;

        // Warmup
        for _ in 0..100 {
            let _ = encode_cct(&header, &payload, BENCH_SIGNING_KEY).unwrap();
        }

        // Benchmark
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = encode_cct(&header, &payload, BENCH_SIGNING_KEY).unwrap();
        }
        let elapsed = start.elapsed();
        let avg_us = elapsed.as_micros() as f64 / iterations as f64;

        println!(
            "CCT encode: {iterations} iterations in {elapsed:?}, avg = {avg_us:.1}µs/req"
        );

        // Target: < 1ms = 1000µs per request
        assert!(
            avg_us < 1000.0,
            "CCT encode too slow: {avg_us:.1}µs > 1000µs target"
        );
    }

    // ──── Phase 0.2: Decode + verify benchmark ────

    #[test]
    fn test_cct_decode_performance_target() {
        let header = CctHeader::default();
        let payload = make_payload();
        let cct = encode_cct(&header, &payload, BENCH_SIGNING_KEY).unwrap();
        let iterations = 1000;

        // Warmup
        for _ in 0..100 {
            let _ = decode_cct(&cct, BENCH_SIGNING_KEY).unwrap();
        }

        // Benchmark
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = decode_cct(&cct, BENCH_SIGNING_KEY).unwrap();
        }
        let elapsed = start.elapsed();
        let avg_us = elapsed.as_micros() as f64 / iterations as f64;

        println!(
            "CCT decode+verify: {iterations} iterations in {elapsed:?}, avg = {avg_us:.1}µs/req"
        );

        // Target: < 1ms = 1000µs per request
        assert!(
            avg_us < 1000.0,
            "CCT decode too slow: {avg_us:.1}µs > 1000µs target"
        );
    }

    // ──── Phase 0.2: Combined encode-decode roundtrip ────

    #[test]
    fn test_cct_roundtrip_performance_target() {
        let header = CctHeader::default();
        let payload = make_payload();
        let iterations = 1000;

        let start = Instant::now();
        for _ in 0..iterations {
            let cct = encode_cct(&header, &payload, BENCH_SIGNING_KEY).unwrap();
            let _ = decode_cct(&cct, BENCH_SIGNING_KEY).unwrap();
        }
        let elapsed = start.elapsed();
        let avg_us = elapsed.as_micros() as f64 / iterations as f64;

        println!(
            "CCT roundtrip (encode+decode): {iterations} iterations in {elapsed:?}, avg = {avg_us:.1}µs/req"
        );

        // Target: < 2ms = 2000µs for full roundtrip
        assert!(
            avg_us < 2000.0,
            "CCT roundtrip too slow: {avg_us:.1}µs > 2000µs target"
        );
    }

    // ──── Phase 0.2: Token with multiple roles ────

    #[test]
    fn test_cct_with_many_roles_performance() {
        let header = CctHeader::default();
        let payload = CctPayload {
            jti: "bench-many-roles".to_string(),
            iss: "bench-cluster".to_string(),
            sub: "bench-app".to_string(),
            aud: vec!["coord-agent".to_string()],
            iat: 1719990000,
            exp: 2000000000,
            roles: (0..20).map(|i| format!("role-{i}")).collect(),
            scope_overrides: HashMap::new(),
        };
        let iterations = 500;

        let start = Instant::now();
        for _ in 0..iterations {
            let cct = encode_cct(&header, &payload, BENCH_SIGNING_KEY).unwrap();
            let _ = decode_cct(&cct, BENCH_SIGNING_KEY).unwrap();
        }
        let elapsed = start.elapsed();
        let avg_us = elapsed.as_micros() as f64 / iterations as f64;

        println!(
            "CCT roundtrip (20 roles): {iterations} iterations in {elapsed:?}, avg = {avg_us:.1}µs/req"
        );

        // Even with many roles, should be under 2ms
        assert!(
            avg_us < 2000.0,
            "CCT roundtrip (20 roles) too slow: {avg_us:.1}µs > 2000µs target"
        );
    }

    // ──── Phase 0.2: Scope override performance ────

    #[test]
    fn test_cct_with_scope_overrides_performance() {
        let header = CctHeader::default();
        let mut scope_overrides = HashMap::new();
        scope_overrides.insert("data:kv:read".to_string(), "/app/order-service/".to_string());
        scope_overrides.insert("data:kv:write".to_string(), "/app/order-service/".to_string());
        scope_overrides.insert("coord:registry:discover".to_string(), "payment-service".to_string());

        let payload = CctPayload {
            jti: "bench-scope-overrides".to_string(),
            iss: "bench-cluster".to_string(),
            sub: "bench-app".to_string(),
            aud: vec!["coord-agent".to_string()],
            iat: 1719990000,
            exp: 2000000000,
            roles: vec!["service-writer".to_string()],
            scope_overrides,
        };
        let iterations = 500;

        let start = Instant::now();
        for _ in 0..iterations {
            let cct = encode_cct(&header, &payload, BENCH_SIGNING_KEY).unwrap();
            let _ = decode_cct(&cct, BENCH_SIGNING_KEY).unwrap();
        }
        let elapsed = start.elapsed();
        let avg_us = elapsed.as_micros() as f64 / iterations as f64;

        println!(
            "CCT roundtrip (with scope overrides): {iterations} iterations in {elapsed:?}, avg = {avg_us:.1}µs/req"
        );

        assert!(
            avg_us < 2000.0,
            "CCT roundtrip (scope overrides) too slow: {avg_us:.1}µs > 2000µs target"
        );
    }
}
