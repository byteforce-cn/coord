#!/usr/bin/env bash
# 构建组件模型 wasm 插件夹具（`coord-agent/tests/fixtures/plugin-guest/`）。
#
# 产物：coord-agent/tests/fixtures/coord-plugin-guest.wasm
#   —— 已提交到仓库，集成测试直接加载；CI 若安装了工具链则重建并校验一致性。
#
# 工具链前置（见 README / 计划 §19）：
#   rustup target add wasm32-unknown-unknown
#   cargo install wasm-tools   # 或下载 prebuilt 二进制
#
# 为什么走 `wasm32-unknown-unknown` + `component embed/new` 而不是 `wasm32-wasip2`：
# 后者会让 rustc 的 std 链入整套 `wasi:cli/*` + `wasi:io/*` import，而本项目的
# 沙箱策略是**无 WASI**（能力面 = `coord:plugin/host` 一个接口）。unknown-unknown
# 产出的是无任何 import 的 core module，`component new` 不需要 preview1 adapter。
set -euo pipefail

cd "$(dirname "$0")/.."
ROOT="$PWD"
GUEST_DIR="$ROOT/coord-agent/tests/fixtures/plugin-guest"
WIT_DIR="$ROOT/coord-agent/wit"
OUT="$ROOT/coord-agent/tests/fixtures/coord-plugin-guest.wasm"
TARGET="wasm32-unknown-unknown"
WORLD="plugin"

command -v wasm-tools >/dev/null 2>&1 || {
    echo "error: wasm-tools not found on PATH" >&2
    echo "  install: cargo install wasm-tools  (or grab a prebuilt release binary)" >&2
    exit 1
}

if ! rustup target list --installed | grep -qx "$TARGET"; then
    echo "error: rust target '$TARGET' not installed" >&2
    echo "  install: rustup target add $TARGET" >&2
    exit 1
fi

echo "::group::cargo build ($TARGET, release)"
cargo build --manifest-path "$GUEST_DIR/Cargo.toml" --release --target "$TARGET"
echo "::endgroup::"

CORE_WASM="$GUEST_DIR/target/$TARGET/release/coord_plugin_guest.wasm"
[ -f "$CORE_WASM" ] || {
    echo "error: expected core module not found: $CORE_WASM" >&2
    exit 1
}

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# 1) 把 WIT 元数据嵌进 core module（ABI 描述）
wasm-tools component embed "$WIT_DIR" -w "$WORLD" "$CORE_WASM" -o "$TMP/embedded.wasm"

# 2) core module → component（无 preview1 import，故无需 adapter）
wasm-tools component new "$TMP/embedded.wasm" -o "$TMP/component.wasm"

# 3) 去掉 name/debug 等自定义段（夹具体积 + 可重复构建）
wasm-tools strip "$TMP/component.wasm" -o "$TMP/stripped.wasm"

mkdir -p "$(dirname "$OUT")"
cp "$TMP/stripped.wasm" "$OUT"

echo "built: $OUT ($(wc -c < "$OUT") bytes)"
echo "--- imports (must be exactly the coord host interface, no WASI) ---"
wasm-tools component wit "$OUT" | sed -n '1,12p'
