// BFF 静态资源服务
//
// 使用 rust-embed 在编译时将前端 dist/ 嵌入二进制。
// 职责（ADP/UI 开发文档 §3.2/§7.2）：
// 1. 静态资源服务 — 嵌入的 JS/CSS/图片，含缓存头
// 2. SPA Fallback — 对非 /api 路径返回 index.html

use axum::{
    body::Body,
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use rust_embed::RustEmbed;

/// 嵌入 coord-ui/dist/ 构建产物
///
/// 编译时嵌入，确保前后端版本强制一致。
/// 构建前需确保已运行 `cd coord-ui && pnpm build`。
#[derive(RustEmbed)]
#[folder = "../coord-ui/dist/"]
pub struct UiAssets;

/// 根据路径提供静态资源
///
/// 对带 hash 的资源文件（如 assets/index-abc123.js）设置 1 年强缓存，
/// 对 index.html 不缓存，确保前端更新即时生效。
pub fn serve_static(path: &str) -> Option<Response> {
    let path = path.trim_start_matches('/');
    // 根路径或目录路径 → index.html
    let path = if path.is_empty() || path.ends_with('/') {
        "index.html"
    } else {
        path
    };

    UiAssets::get(path).map(|asset| {
        let mime = mime_from_path(path);
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, mime.parse().unwrap());

        // 缓存策略：带 hash 的资源文件名包含 8 位十六进制 hash（如 index-BxK1vLwz.js）
        if is_hashed_asset(path) {
            headers.insert(
                header::CACHE_CONTROL,
                "public, max-age=31536000, immutable".parse().unwrap(),
            );
        } else {
            headers.insert(
                header::CACHE_CONTROL,
                "no-cache".parse().unwrap(),
            );
        }

        (StatusCode::OK, headers, Body::from(asset.data.to_vec())).into_response()
    })
}

/// SPA fallback：返回 index.html，由前端路由接管
pub fn serve_index_html() -> Response {
    if let Some(asset) = UiAssets::get("index.html") {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            "text/html; charset=utf-8".parse().unwrap(),
        );
        headers.insert(
            header::CACHE_CONTROL,
            "no-cache".parse().unwrap(),
        );
        (StatusCode::OK, headers, Body::from(asset.data.to_vec())).into_response()
    } else {
        // 前端未构建时的占位页
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
            SPA_PLACEHOLDER,
        )
            .into_response()
    }
}

/// 判断是否为带 hash 的资源文件（可长期缓存）
fn is_hashed_asset(path: &str) -> bool {
    // 匹配模式: (字母数字)-8位以上hexhash.扩展名
    // 例如: index-BxK1vLwz.js, Inter-Regular-Cy7hIqVF.woff2
    let name = path.rsplit('/').next().unwrap_or(path);
    let stem = name.rsplit('.').next().unwrap_or(name);
    // 检查 stem 中是否包含 -XXXXXXXX 格式的 hash
    if let Some(last_seg) = stem.rsplit('-').next() {
        last_seg.len() >= 8 && last_seg.chars().all(|c| c.is_ascii_hexdigit())
    } else {
        false
    }
}

/// 根据文件扩展名返回 MIME 类型
fn mime_from_path(path: &str) -> &'static str {
    let ext = path.rsplit('.').next().unwrap_or("");
    match ext {
        "html" => "text/html; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "js" | "mjs" => "application/javascript; charset=utf-8",
        "json" => "application/json; charset=utf-8",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "svg" => "image/svg+xml",
        "ico" => "image/x-icon",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "ttf" => "font/ttf",
        "eot" => "application/vnd.ms-fontobject",
        "txt" => "text/plain; charset=utf-8",
        "xml" => "application/xml",
        "webp" => "image/webp",
        "avif" => "image/avif",
        "map" => "application/json",
        _ => "application/octet-stream",
    }
}

/// 前端未构建时的占位 HTML
const SPA_PLACEHOLDER: &str = r#"<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Coord 控制台</title>
    <style>
        body { font-family: system-ui, -apple-system, sans-serif; display: flex; justify-content: center; align-items: center; min-height: 100vh; margin: 0; background: #f1f5f9; color: #334155; }
        .container { text-align: center; padding: 2rem; }
        h1 { font-size: 2rem; margin-bottom: 0.5rem; color: #0f172a; }
        p { color: #64748b; margin: 0.5rem 0; }
        code { background: #e2e8f0; padding: 0.2rem 0.5rem; border-radius: 0.25rem; font-size: 0.9rem; }
    </style>
</head>
<body>
    <div class="container">
        <h1>&#9878; Coord 控制台</h1>
        <p>前端应用未构建。请运行 <code>cd coord-ui &amp;&amp; pnpm build</code> 构建前端资源。</p>
        <p style="margin-top:1rem;font-size:0.85rem">或设置 <code>ui_enabled = false</code> 仅使用 API 代理模式。</p>
    </div>
</body>
</html>"#;
