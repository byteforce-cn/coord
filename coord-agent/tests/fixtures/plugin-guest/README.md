# 组件模型 wasm 插件夹具（参考 guest）

本目录是**组件模型 ABI**（`coord-agent/wit/coord-plugin.wit`）的参考 guest，
用于 `coord-agent/tests/agent_plugin_component_test.rs` 的端到端验证。

| 文件 | 说明 |
|---|---|
| `src/lib.rs` | guest 实现（`wit-bindgen` 生成绑定；覆盖宿主 import 的每条路径） |
| `Cargo.toml` | 独立 cargo 工程（`[workspace]` 自声明，**不是** workspace 成员） |
| `Cargo.lock` | 锁定依赖（连同提交的 `.wasm` 一起保证夹具可重现） |
| `../../coord-plugin-guest.wasm` | **已提交**的产物（测试直接加载，无需工具链） |

## 重新构建

```sh
scripts/build-plugin-guest.sh
```

脚本会：`cargo build --release --target wasm32-unknown-unknown`
→ `wasm-tools component embed <wit> -w plugin`
→ `wasm-tools component new`（无需 WASI adapter）
→ `wasm-tools strip` → 覆盖 `coord-agent/tests/fixtures/coord-plugin-guest.wasm`。

## 工具链前置

```sh
rustup target add wasm32-unknown-unknown
cargo install wasm-tools          # 或下载 prebuilt 二进制
```

## 为什么不是 `wasm32-wasip2`

`wasm32-wasip2` 会让 rustc 的 `std` 链入整套 `wasi:cli/*` + `wasi:io/*` import
（实测 14 个 wasi 接口），而本项目的沙箱策略是**无 WASI**——能力面必须等于
`coord:plugin/host` 一个接口。`wasm32-unknown-unknown` 产出无 import 的 core
module，`wasm-tools component new` 不需要 preview1 adapter，产物 import 面只有
`coord:plugin/host`（`fixture_is_a_wasi_free_component` 断言这一点）。

## 夹具方法（`handleInvoke(method, payload)`）

| method | payload | 返回 | 验证点 |
|---|---|---|---|
| `echo` | 任意字节 | 原样 | ABI 往返 |
| `kv-put` | `key=value` | `rev:<revision>` | 宿主 import + typed record |
| `kv-get` | `key` | 值（空 = 不存在） | `kv-range` 单键路径 |
| `kv-create` | `key=value` | `ok:<rev>` / `conflict` | CAS（`version == 0`）+ typed variant |
| `txn-create` | `key=value` | `succeeded` / `failed` | txn + Compare |
| `lease-grant` | ttl 秒数 | `lease:<id>` | grant + keepAlive + revoke |
| `watch-first` | `key` | `<类型>:<值>` | **阻塞式 `next` 的 fiber 异步桥** |
| `storage-roundtrip` | `bucket/object=内容` | `size:<n>` | put/get/stat + 对象字节面 |
| `env` | 配置键 | 值 / `ErrNotFound` | `[plugins].env` 注入 |
| `log` | 文本 | `logged` | 宿主日志 |
| `spin` | — | 不返回 | fuel/epoch 沙箱（trap） |
| `alloc` | 字节数 | 长度 | `StoreLimits` 内存上限（trap） |
| `forbidden` | 越界 key | `forbidden` / `allowed:<rev>` | 作用域守卫 fail-closed |
| 其它 | — | `Err` | 插件级错误（≠ trap，实例存活） |
