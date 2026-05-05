//! Static UI serving for the coord console SPA.
//!
//! Responsible for:
//! - Resolving the `ui/console/dist` directory relative to common layouts.
//! - Serving `index.html` on SPA route fallback (any path without a file extension).
//! - Serving asset files (js/css/fonts/images) with appropriate `Content-Type`.
//! - Rejecting path traversal via `..` segments.

use std::fs;
use std::path::{Path, PathBuf};

use axum::body::Body;
use axum::extract::{Path as AxumPath, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};

use super::HttpApiState;
use super::error::ApiError;

/// Resolve the compiled UI dist directory relative to cwd.
///
/// Tries a couple of common layouts (repo-root `ui/console/dist`, or the
/// crate-relative `../../ui/console/dist`). Falls back to the first candidate
/// if none exist so the caller still gets a predictable path.
pub fn resolve_ui_dist_dir() -> PathBuf {
    let candidates = [
        PathBuf::from("ui/console/dist"),
        PathBuf::from("../../ui/console/dist"),
    ];

    for path in candidates {
        if path.exists() {
            return path;
        }
    }

    PathBuf::from("ui/console/dist")
}

pub(super) async fn ui_index(State(app): State<HttpApiState>) -> Response {
    serve_ui_asset(&app, "").await
}

pub(super) async fn ui_path(
    State(app): State<HttpApiState>,
    AxumPath(path): AxumPath<String>,
) -> Response {
    serve_ui_asset(&app, &path).await
}

async fn serve_ui_asset(app: &HttpApiState, requested: &str) -> Response {
    if !app.ui_dist_dir.exists() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "coord ui dist not found. build with: cd ui/console && npm run build",
        )
            .into_response();
    }

    if requested.split('/').any(|segment| segment == "..") {
        return ApiError::new(StatusCode::BAD_REQUEST, "invalid path").into_response();
    }

    let normalized = requested.trim_matches('/');
    let wants_index = normalized.is_empty();
    let has_extension = Path::new(normalized).extension().is_some();

    let candidate = if wants_index {
        app.ui_dist_dir.join("index.html")
    } else {
        app.ui_dist_dir.join(normalized)
    };

    if candidate.is_file() {
        match fs::read(&candidate) {
            Ok(bytes) => {
                return binary_response(StatusCode::OK, content_type_for(&candidate), bytes);
            }
            Err(_) => {
                return ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "failed to read ui asset")
                    .into_response();
            }
        }
    }

    if has_extension {
        return ApiError::new(StatusCode::NOT_FOUND, "ui asset not found").into_response();
    }

    let fallback = app.ui_dist_dir.join("index.html");
    match fs::read(&fallback) {
        Ok(bytes) => binary_response(StatusCode::OK, "text/html; charset=utf-8", bytes),
        Err(_) => ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "coord ui entrypoint not found. build with: cd ui/console && npm run build",
        )
        .into_response(),
    }
}

fn binary_response(status: StatusCode, content_type: &'static str, bytes: Vec<u8>) -> Response {
    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static(content_type),
    );
    response
}

fn content_type_for(path: &Path) -> &'static str {
    match path
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or_default()
    {
        "html" => "text/html; charset=utf-8",
        "js" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" => "application/json; charset=utf-8",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn content_type_matrix() {
        assert_eq!(
            content_type_for(Path::new("a.html")),
            "text/html; charset=utf-8"
        );
        assert_eq!(
            content_type_for(Path::new("a.js")),
            "text/javascript; charset=utf-8"
        );
        assert_eq!(
            content_type_for(Path::new("a.css")),
            "text/css; charset=utf-8"
        );
        assert_eq!(content_type_for(Path::new("a.svg")), "image/svg+xml");
        assert_eq!(content_type_for(Path::new("a.woff2")), "font/woff2");
        assert_eq!(
            content_type_for(Path::new("nope")),
            "application/octet-stream"
        );
    }

    #[test]
    fn resolve_ui_dist_dir_is_stable_fallback() {
        // Without the dist tree present in the workspace, the fallback wins.
        let p = resolve_ui_dist_dir();
        assert!(p.ends_with("ui/console/dist"));
    }
}
