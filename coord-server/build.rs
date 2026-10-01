// coord-server build script
//
// 确保 coord-ui/dist/ 在编译时存在，避免 rust-embed 编译失败。
// - 如果 dist/ 已构建（含 index.html），不做任何事。
// - 如果 dist/ 缺失或为空，生成占位 index.html 告知用户构建前端。

use std::path::Path;

fn main() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dist_dir = manifest_dir.join("../coord-ui/dist");
    let index_html = dist_dir.join("index.html");

    if index_html.exists() {
        println!("cargo:rerun-if-changed=../coord-ui/dist/index.html");
        return;
    }

    // 前端未构建：生成占位 dist/，确保 rust-embed 编译通过
    std::fs::create_dir_all(&dist_dir).ok();

    let placeholder = r#"<!DOCTYPE html>
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
        <p>前端应用未构建。请运行 <code>cd coord-ui && pnpm install && pnpm build</code> 后重新编译。</p>
    </div>
</body>
</html>"#;

    std::fs::write(&index_html, placeholder).ok();
    println!("cargo:warning=coord-ui/dist/ 未构建，已生成占位页面。运行 `cd coord-ui && pnpm install && pnpm build` 构建完整前端。");
}
