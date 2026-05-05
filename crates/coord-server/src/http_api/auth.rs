//! Console session / capability / rate-limit gating helpers.
//!
//! All helpers take `&HttpApiState` + `&HeaderMap` by reference. They do not
//! own the request extraction; handlers call them after destructuring.
//!
//! Semantics:
//! - **`require_console_session`** — when the security domain is initialized and
//!   unsealed, demands a valid bearer token; when not yet initialized (bootstrap),
//!   returns `SERVICE_UNAVAILABLE` because the console cannot function until the
//!   security domain is bootstrapped via gRPC `SealService/Init`.
//! - **`require_console_capability`** — extends session with capability-level
//!   authorization via the security domain's policy matcher.
//! - **`enforce_high_risk_rate_limit`** — token-bucket rate-limit keyed by
//!   `{route}:{client_ip}`, returning a 429 with a sane `Retry-After`.

use axum::http::{HeaderMap, StatusCode, header};

use super::HttpApiState;
use super::error::ApiError;

pub(super) async fn require_console_session(
    app: &HttpApiState,
    headers: &HeaderMap,
) -> Result<(), ApiError> {
    let status = app.state.security().seal_status().await;
    app.state
        .metrics()
        .coord_security_sealed
        .set(if status.sealed { 1 } else { 0 });
    if !status.initialized {
        return Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "security domain not initialised; call SealService/Init first",
        ));
    }

    if status.sealed {
        return Err(ApiError::new(
            StatusCode::LOCKED,
            "security domain is sealed",
        ));
    }

    let token = extract_bearer_token(headers)
        .ok_or_else(|| ApiError::new(StatusCode::UNAUTHORIZED, "missing bearer token"))?;

    let lookup = app.state.security().lookup_token(&token).await;
    if !lookup.valid {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "invalid or expired token",
        ));
    }

    Ok(())
}

pub(super) async fn require_console_capability(
    app: &HttpApiState,
    headers: &HeaderMap,
    capability: &str,
) -> Result<(), ApiError> {
    require_console_session(app, headers).await?;

    let status = app.state.security().seal_status().await;
    if !status.initialized {
        return Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "security domain not initialised; call SealService/Init first",
        ));
    }

    let token = extract_bearer_token(headers)
        .ok_or_else(|| ApiError::new(StatusCode::UNAUTHORIZED, "missing bearer token"))?;

    match app
        .state
        .security()
        .authorize_token(&token, capability)
        .await
    {
        Ok(()) => Ok(()),
        Err(message) => {
            app.state.metrics().coord_authz_denied_total.inc();
            Err(ApiError::new(StatusCode::FORBIDDEN, message))
        }
    }
}

pub(super) fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    let header_value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let trimmed = header_value.trim();
    let (scheme, token) = trimmed.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("Bearer") {
        return None;
    }
    let token = token.trim();
    if token.is_empty() {
        None
    } else {
        Some(token.to_string())
    }
}

/// Best-effort client identity for rate-limit bucketing. Prefers
/// `X-Forwarded-For` (first hop) when behind a trusted reverse proxy, then
/// `X-Real-IP`, else a fixed "anonymous" bucket. Caller composes this with a
/// route prefix so login, seal, backup, etc. each have their own bucket.
pub(super) fn client_rate_limit_key(headers: &HeaderMap, route: &str) -> String {
    let ident = headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split(',').next())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .or_else(|| {
            headers
                .get("x-real-ip")
                .and_then(|v| v.to_str().ok())
                .map(str::trim)
                .filter(|s| !s.is_empty())
        })
        .unwrap_or("anonymous");
    format!("{route}:{ident}")
}

pub(super) fn enforce_high_risk_rate_limit(
    app: &HttpApiState,
    headers: &HeaderMap,
    route: &str,
) -> Result<(), ApiError> {
    let key = client_rate_limit_key(headers, route);
    match app.high_risk_limiter.check(&key) {
        Ok(()) => Ok(()),
        Err(retry_after_seconds) => {
            let retry_after = retry_after_seconds.ceil().max(1.0) as u64;
            // Route through CoordError so the JSON body carries the stable
            // `rate_limit.exceeded` code + kind alongside the Retry-After header.
            let err: ApiError = coord_core::error::CoordError::RateLimited {
                reason: route.to_string(),
                retry_after_seconds: Some(retry_after),
            }
            .into();
            Err(err)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn headers_with(key: &str, value: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            axum::http::HeaderName::from_bytes(key.as_bytes()).unwrap(),
            HeaderValue::from_str(value).unwrap(),
        );
        h
    }

    #[test]
    fn rate_limit_key_uses_xff_first_hop() {
        let h = headers_with("x-forwarded-for", "10.0.0.1, 10.0.0.2");
        assert_eq!(
            client_rate_limit_key(&h, "security.login"),
            "security.login:10.0.0.1"
        );
    }

    #[test]
    fn rate_limit_key_falls_back_to_real_ip() {
        let h = headers_with("x-real-ip", "203.0.113.7");
        assert_eq!(
            client_rate_limit_key(&h, "admin.backup.create"),
            "admin.backup.create:203.0.113.7"
        );
    }

    #[test]
    fn rate_limit_key_defaults_to_anonymous() {
        let h = HeaderMap::new();
        assert_eq!(
            client_rate_limit_key(&h, "security.seal"),
            "security.seal:anonymous"
        );
    }

    #[test]
    fn rate_limit_key_isolates_routes() {
        let h = headers_with("x-real-ip", "10.0.0.1");
        assert_ne!(
            client_rate_limit_key(&h, "security.login"),
            client_rate_limit_key(&h, "security.seal")
        );
    }

    #[test]
    fn extract_bearer_token_various_shapes() {
        let mut h = HeaderMap::new();
        h.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str("Bearer abc123").unwrap(),
        );
        assert_eq!(extract_bearer_token(&h), Some("abc123".into()));

        h.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str("bearer x").unwrap(),
        );
        assert_eq!(extract_bearer_token(&h), Some("x".into()));

        h.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str("Basic u:p").unwrap(),
        );
        assert_eq!(extract_bearer_token(&h), None);

        h.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str("Bearer   ").unwrap(),
        );
        assert_eq!(extract_bearer_token(&h), None);
    }
}
