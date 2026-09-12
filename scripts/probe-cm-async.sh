#!/usr/bin/env bash
# component-model-async（真 async WIT / `stream<T>`）guest 工具链探针
#
# 计划 §19「ABI 收敛 / component-model-async」的**结论依据**。本脚本不进 CI：
# 它是一个可复跑的探针，用来在工具链演进后重新核对「现在能不能切到 async WIT」。
#
# 本轮结论（2026-09-12；wit-bindgen 0.41 / wasm-tools 1.259.0 / wasmtime 48.0.2）：
#   1. WIT 里写 `ping: async func() -> u32;` 能被 wit-parser 接受（语法面已就绪）；
#   2. guest 侧要拿到 async 绑定，必须开 wit-bindgen 的**非默认 `async` feature**
#      且 `generate!({ async: true })`——否则该 import **完全不生成**（`host::ping`
#      不存在）；
#   3. 开了之后 async import 变成带前缀的名字（`ping` → `async_ping`），并依赖
#      `wit_bindgen::rt::async_support`（task / waitable-set 运行时）；
#   4. 宿主侧还需 wasmtime 的 `component-model-async` feature（48.0.2 有，我们未开）
#      + `bindgen!` 的 async 函数级 flag（`func_wrap_concurrent` 路径）。
#
# ⇒ 切到 async WIT **不是**「同一份 WIT 换实现」：guest 调用面变更 + 新运行时 +
#    新宿主 feature 三者同时动，且 wit-bindgen 的 async 支持仍是 draft 级 API。
#    故本期保持「同步签名 WIT + 宿主 async 实现（wasmtime fiber 桥）」的等价方案；
#    工具链成熟（async API 稳定、guest 侧命名不再变）后可直接切。
#
# ── 批次 12 复核（2026-09-12，本机有网 → 工具链可升级）：**仍未打通**，但病因已定死 ──
#   a. **命名不稳定已消失**：wit-bindgen **0.44.0** 下 guest 侧函数名回到 `ping`
#      （0.41 的 `async_ping` 前缀取消），`cargo build --target wasm32-unknown-unknown`
#      直接通过 → 说明上游正在收敛，但**编码层仍不兼容**；
#   b. **core 侧命名约定**（`wasm-tools print` 实测）：async 宿主导入被写成
#      `(import "probe:cm/host@0.1.0" "[async-lower][async]ping" ...)` ——即
#      `[async-lower]` 前缀套在**已被 `[async]` 修饰的名字**上；
#   c. **失败点 1（wit-bindgen 自带 custom section）**：`wasm-tools component new`
#      解码 `component-type:wit-bindgen:…:encoded world` 时报
#        `export name `[async]ping` is not a valid extern name`（非 kebab-case）
#      —— wasmparser 的 kebab 校验（`validator/component.rs` 的 `to_kebab_string`）
#      对 `[async]` 前缀**无豁免**；
#   d. **失败点 2（剥离 custom section 后的自证）**：先把 guest 产物
#      `wasm-tools strip --all` 再 `component embed`（改用 wit-component 自己的编码），
#      错误变成
#        `failed to resolve import "probe:cm/host@0.1.0::[async-lower][async]ping"`
#      → 说明**两侧对「async 导入的 core 名」约定不一致**（wit-component 期望
#      `[async-lower]ping`，wit-bindgen 发的是 `[async-lower][async]ping`）。
#  ⇒ 结论：async WIT 的阻塞点是 **wit-bindgen ↔ wit-component/wasm-tools 的编码约定**
#    （不是 API 稳定性、也不是我们侧实现），两侧都升到自洽版本前不可用；
#    `[async]`/`[async-lower]` 名约定一旦对齐，本脚本第 4 步会直接给出 component。
#    何时可切：`wasm-tools component new` 不再拒绝 `[async]` 前缀名（或 wit-bindgen
#    不再产出该前缀）。届时再动宿主（wasmtime `component-model-async` feature +
#    `bindgen!` async flag）与 guest（`generate!({ async: true })`）。
set -uo pipefail

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
TARGET="wasm32-unknown-unknown"

mkdir -p "$TMP/wit" "$TMP/src"

cat > "$TMP/wit/probe.wit" <<'WIT'
package probe:cm@0.1.0;

interface host {
  /// 真 async 宿主导入（component-model-async 提案）
  ping: async func() -> u32;
}

interface guest {
  run: func() -> u32;
}

world probe {
  import host;
  export guest;
}
WIT

cat > "$TMP/Cargo.toml" <<'TOML'
[package]
name = "cm-async-probe"
version = "0.0.0"
edition = "2021"
publish = false

[workspace]

[lib]
crate-type = ["cdylib"]

[dependencies]
wit-bindgen = { version = "0.41", features = ["async"] }
TOML

cat > "$TMP/src/lib.rs" <<'RUST'
// async import 开启后名字带 `async_` 前缀（探针结论 3）
wit_bindgen::generate!({
    path: "wit",
    world: "probe",
    async: true,
});

use exports::probe::cm::guest::Guest;

struct Component;

impl Guest for Component {
    async fn run() -> u32 {
        probe::cm::host::async_ping().await
    }
}

export!(Component);
RUST

echo "── 1. 前置检查 ──"
if ! rustup target list --installed | grep -qx "$TARGET"; then
    echo "SKIP: rust target '$TARGET' 未安装（rustup target add $TARGET）"
    exit 0
fi
if ! command -v wasm-tools >/dev/null 2>&1; then
    echo "SKIP: wasm-tools 不在 PATH（见 README 的工具链安装步骤）"
    exit 0
fi

echo "── 2. guest 构建（wit-bindgen 0.41 + async feature + async: true）──"
if ! cargo build --manifest-path "$TMP/Cargo.toml" --release --target "$TARGET" 2>&1 | tail -12; then
    echo "结论：guest 侧构建失败 → async WIT 仍不可用（见上方错误）"
    exit 0
fi

CORE="$TMP/target/$TARGET/release/cm_async_probe.wasm"
[ -f "$CORE" ] || { echo "结论：未产出 core module：$CORE"; exit 0; }
echo "core module: $(wc -c < "$CORE") bytes"

echo "── 3. wasm-tools: component embed ──"
if ! wasm-tools component embed "$TMP/wit" -w probe "$CORE" -o "$TMP/embedded.wasm"; then
    echo "结论：component embed 失败 → async ABI 编码未就绪"
    exit 0
fi
echo "embed: ok"

echo "── 4. wasm-tools: component new（async lift/lower 编码）──"
if ! wasm-tools component new "$TMP/embedded.wasm" -o "$TMP/component.wasm"; then
    echo
    echo "结论：component new 失败 → 本期 async WIT 链路断在这一步。"
    echo "本轮实测错误（wit-bindgen 0.41 的 async 元数据编码 + wasm-tools 1.259 解码）："
    echo "  export name '[async]ping' is not a valid extern name"
    echo "即 wit-bindgen 0.41 用 '[async]' 前缀标记 async 导出，而 wasm-tools 1.259"
    echo "的解码器要求 kebab-case 外部名 → 两侧编码约定不一致。"
    echo "（升级 guest 侧 wit-bindgen 或许能对齐，但本机无网络、新版本依赖未缓存，"
    echo "  未验证；属结论的不确定部分。）"
    echo
echo "── 结论 ──"
echo "guest 代码生成本身可用（core module 产出成功），但 async ABI 的组件编码"
echo "在当前固定工具链下不可用；且即使打通，guest 调用面名字会带 async_ 前缀、"
echo "需要 wit_bindgen::rt::async_support 运行时，宿主还需 wasmtime 的"
echo "component-model-async feature → 三条路径同时改，等 API 稳定后再做。"
    exit 0
fi
echo "component: $(wc -c < "$TMP/component.wasm") bytes"
wasm-tools component wit "$TMP/component.wasm" | sed -n '1,20p'

echo "── 结论 ──"
echo "async 链路全通（embed + new 均成功）→ 可以启动宿主侧 async ABI 移植。"
