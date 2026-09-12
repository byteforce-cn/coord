// coord-agent: 三条宿主 import 路径的 **ABI 账本**（计划 §19「ABI 收敛」）
//
// 单一事实来源 = `coord-agent/wit/coord-plugin.wit`（组件 ABI）。本模块把宿主面
// 编码成可在 CI 中**机械比对**的表，保证「新增原语必须同时落到每条路径」——
// 漏改任何一条都会在 `cargo test -p coord-agent` 里失败，而不是等某个插件在
// 运行时才暴露。
//
// ──── 三条路径 ────
// 1. **组件 ABI**（`wit/coord-plugin.wit` + `component_engine.rs`）：完整面；
// 2. **core wasm ABI**（`wasm_engine.rs`，手写 ABI）：声明的**最小子集**
//    （保留给无组件工具链 / 极小产物场景）；
// 3. **JS SDK**（`sdk/bind.rs`，rquickjs 宿主）：数据面全量 + JS 专属糖
//    （`util.*`、`stdlib.js` 的组合原语）。
//
// ──── 校验方式（见本文件 `mod tests`）────
// - WIT：解析 `wit/coord-plugin.wit`，逐条比对 `interface host` 的函数名、
//   `resource subscription` 的方法名、`variant sdk-error` 的变体名；
// - core ABI：扫描 `wasm_engine.rs` 里 `func_wrap(HOST_MODULE, "<name>")` 的
//   实际注册名（源文本扫描，防止「账本写了、代码没注册」）；
// - JS：扫描 `sdk/bind.rs` 的 `set("<name>")` 调用。命名约定：账本里
//   `ns.fn` = 命名空间成员（接收者变量与命名空间同名，如 `kv.set("put")`），
//   无前缀 = `coord.<fn>`（`coord.set("txn")` / `coord.set("limits")`），
//   `#` = 句柄方法（如 `watch#next` → 订阅句柄对象的 `set("next")`）；
// - 错误：`SdkErrorCode::ALL` 的异常名 / 稳定名 / core ABI 码三者必须与本表一致。

use crate::plugin::sdk::backend::SdkErrorCode;

// ──── core ABI 错误码（宿主 import 返回值 / `handle_invoke` 低 32 位）────
//
// 这三条路径共用同一套**语义**错误码；数字本身是 core ABI 的线格式，
// 因此在这里（唯一一处）定义，`wasm_engine.rs` 直接复用。

pub const ERR_FORBIDDEN: i32 = -1;
pub const ERR_NOT_FOUND: i32 = -2;
pub const ERR_INVALID: i32 = -3;
pub const ERR_UNAVAILABLE: i32 = -4;
pub const ERR_EXHAUSTED: i32 = -5;
pub const ERR_INTERNAL: i32 = -6;
/// 输出缓冲区不足（guest 给的 `out_cap` 太小）—— core ABI 传输层错误，
/// 不对应任何 WIT 变体。
pub const ERR_BUFFER_TOO_SMALL: i32 = -7;
/// 访问 guest 内存失败（越界 / 无导出 `memory` / `alloc` 失败）—— 同上。
pub const ERR_MEMORY: i32 = -8;
/// `kv_create` CAS 未命中（键已存在）。
pub const ERR_CONFLICT: i32 = -9;

// ──── 账本 ────

/// 一条宿主原语在三条路径上的落点。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostOp {
    /// 组件 ABI（WIT `interface host`）的函数名。
    pub wit: &'static str,
    /// core ABI 的 import 名；`None` = 不在 core 最小子集内。
    pub core: Option<&'static str>,
    /// JS SDK 落点（约定见模块头注释）；`None` = 该路径没有独立入口。
    pub js: Option<&'static str>,
}

/// 宿主面账本（与 `wit/coord-plugin.wit` 逐条对应，顺序同 WIT 声明）。
pub const HOST_OPS: &[HostOp] = &[
    HostOp {
        wit: "kv-put",
        core: Some("kv_put"),
        js: Some("kv.put"),
    },
    HostOp {
        wit: "kv-get",
        core: Some("kv_get"),
        js: Some("kv.get"),
    },
    HostOp {
        wit: "kv-range",
        core: None,
        js: Some("kv.range"),
    },
    HostOp {
        wit: "kv-delete",
        core: Some("kv_delete"),
        js: Some("kv.delete"),
    },
    HostOp {
        wit: "kv-create",
        core: Some("kv_create"),
        js: Some("kv.create"),
    },
    HostOp {
        wit: "txn",
        core: None,
        js: Some("txn"),
    },
    HostOp {
        wit: "lease-grant",
        core: Some("lease_grant"),
        js: Some("lease.grant"),
    },
    HostOp {
        wit: "lease-revoke",
        core: Some("lease_revoke"),
        js: Some("lease.revoke"),
    },
    HostOp {
        wit: "lease-keep-alive",
        core: None,
        js: Some("lease.keepAlive"),
    },
    HostOp {
        wit: "watch-subscribe",
        core: None,
        js: Some("watch.subscribe"),
    },
    HostOp {
        wit: "subscription.next",
        core: None,
        js: Some("watch#next"),
    },
    HostOp {
        wit: "subscription.close",
        core: None,
        js: Some("watch#close"),
    },
    HostOp {
        wit: "storage-put",
        core: None,
        js: Some("storage.put"),
    },
    HostOp {
        wit: "storage-get",
        core: None,
        js: Some("storage.get"),
    },
    HostOp {
        wit: "storage-stat",
        core: None,
        js: Some("storage.stat"),
    },
    HostOp {
        wit: "storage-delete",
        core: None,
        js: Some("storage.delete"),
    },
    HostOp {
        wit: "storage-open-write",
        core: None,
        js: Some("storage.openWrite"),
    },
    HostOp {
        wit: "uploader.write",
        core: None,
        js: Some("storage#write"),
    },
    HostOp {
        wit: "uploader.commit",
        core: None,
        js: Some("storage#commit"),
    },
    HostOp {
        wit: "uploader.abort",
        core: None,
        js: Some("storage#abort"),
    },
    HostOp {
        wit: "storage-open-read",
        core: None,
        js: Some("storage.openRead"),
    },
    HostOp {
        wit: "downloader.stat",
        core: None,
        js: Some("storage#stat"),
    },
    HostOp {
        wit: "downloader.read",
        core: None,
        js: Some("storage#read"),
    },
    HostOp {
        wit: "downloader.close",
        core: None,
        js: Some("storage#close"),
    },
    HostOp {
        wit: "env",
        core: Some("env"),
        js: Some("env"),
    },
    HostOp {
        wit: "log",
        core: Some("log"),
        js: Some("log"),
    },
    HostOp {
        wit: "limits",
        core: None,
        js: Some("limits"),
    },
];

/// 一条**语义**错误在三条路径上的名字。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SdkErrorSpec {
    /// 组件 ABI（WIT `variant sdk-error`）的变体名。
    pub wit: &'static str,
    /// JS 异常名 = core ABI 错误名（两条路径共用 `Err*` 命名）。
    pub name: &'static str,
    /// core ABI 线格式错误码。
    pub core: i32,
}

/// 语义错误账本。
///
/// 7 条与 [`SdkErrorCode::ALL`] 一一对应（`conflict` = `kv-create` CAS 未命中）。
pub const SDK_ERRORS: &[SdkErrorSpec] = &[
    SdkErrorSpec {
        wit: "forbidden",
        name: "ErrForbidden",
        core: ERR_FORBIDDEN,
    },
    SdkErrorSpec {
        wit: "not-found",
        name: "ErrNotFound",
        core: ERR_NOT_FOUND,
    },
    SdkErrorSpec {
        wit: "invalid-argument",
        name: "ErrInvalidArgument",
        core: ERR_INVALID,
    },
    SdkErrorSpec {
        wit: "unavailable",
        name: "ErrUnavailable",
        core: ERR_UNAVAILABLE,
    },
    SdkErrorSpec {
        wit: "resource-exhausted",
        name: "ErrResourceExhausted",
        core: ERR_EXHAUSTED,
    },
    SdkErrorSpec {
        wit: "conflict",
        name: "ErrConflict",
        core: ERR_CONFLICT,
    },
    SdkErrorSpec {
        wit: "internal",
        name: "ErrInternal",
        core: ERR_INTERNAL,
    },
];

/// core ABI 的传输层错误码（不对应 WIT 变体，只在 core ABI 内部出现）。
pub const CORE_TRANSPORT_ERRORS: &[(&str, i32)] = &[
    ("ErrBufferTooSmall", ERR_BUFFER_TOO_SMALL),
    ("ErrMemoryAccess", ERR_MEMORY),
];

/// core ABI **独有**的 import（WIT / JS 路径没有对应入口）。
///
/// **批次 8 起为空**：`kv_get` 已收敛为 WIT `kv-get` + JS `coord.kv.get`
/// （三条路径共用 `PluginSdk::kv_get`）。保留该常量作为「core ABI 允许的
/// 额外糖入口」白名单——若未来再出现 core-only 原语，在此登记，账本测试
/// 会照旧断言它不得与 WIT 原语重名。
pub const CORE_ONLY_OPS: &[&str] = &[];

/// core ABI 错误码 → 稳定名（含传输层码；未知一律 `ErrInternal`）。
pub fn core_err_name(code: i32) -> &'static str {
    if let Some(spec) = SDK_ERRORS.iter().find(|s| s.core == code) {
        return spec.name;
    }
    if let Some((name, _)) = CORE_TRANSPORT_ERRORS.iter().find(|(_, c)| *c == code) {
        return name;
    }
    "ErrInternal"
}

/// [`SdkErrorCode`] → core ABI 错误码。
pub fn core_err_code(code: SdkErrorCode) -> i32 {
    let name = code.exception_name();
    SDK_ERRORS
        .iter()
        .find(|s| s.name == name)
        .map(|s| s.core)
        .unwrap_or(ERR_INTERNAL)
}

/// 账本里所有 core ABI import 名（含 `None` 过滤），供 `wasm_engine.rs` 之外
/// 的工具/测试枚举。
pub fn core_import_names() -> impl Iterator<Item = &'static str> {
    HOST_OPS.iter().filter_map(|op| op.core)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    /// WIT 源（唯一事实来源）。
    const WIT: &str = include_str!("../../wit/coord-plugin.wit");
    /// core ABI 实现（扫描实际注册的 import 名）。
    const CORE_ABI_SRC: &str = include_str!("wasm_engine.rs");
    /// JS 绑定（扫描 `set("<name>")` 调用）。
    const JS_BIND_SRC: &str = include_str!("sdk/bind.rs");

    /// 解析 WIT：返回（`interface host` 函数名, **资源方法**全名
    /// `<resource>.<method>`, `variant sdk-error` 变体名）。
    /// 只做行级状态机（WIT 文本结构规则）。
    fn wit_surface(src: &str) -> (BTreeSet<String>, BTreeSet<String>, BTreeSet<String>) {
        let mut host_funcs = BTreeSet::new();
        let mut resource_methods = BTreeSet::new();
        let mut error_variants = BTreeSet::new();
        let (mut in_host, mut in_variant) = (false, false);
        // 当前所在 `resource <name> {` 的前缀（`None` = 不在 resource 内）
        let mut resource: Option<String> = None;

        for raw in src.lines() {
            let line = raw.split("//").next().unwrap_or("").trim_end();
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            // 列 0 的 `}` 结束当前接口；其它接口（`guest`）直接跳过。
            if line.starts_with('}') {
                in_host = false;
                resource = None;
                in_variant = false;
                continue;
            }
            if line.starts_with("interface ") {
                in_host = line.starts_with("interface host {");
                continue;
            }
            if trimmed == "}" {
                // 缩进的 `}`：结束内层块（resource / variant / record）
                resource = None;
                in_variant = false;
                continue;
            }
            if !in_host {
                continue;
            }
            // `resource <name> {` —— 资源方法一律以 `<resource>.<method>` 记账，
            // 避免不同资源同名方法（如多个 `close`）在集合里互相吞掉。
            if let Some(rest) = trimmed.strip_prefix("resource ") {
                if let Some((name, _)) = rest.split_once('{') {
                    resource = Some(format!("{}.", name.trim()));
                }
                continue;
            }
            if trimmed.starts_with("variant sdk-error {") {
                in_variant = true;
                continue;
            }
            if let Some((name, rest)) = trimmed.split_once(':') {
                if rest.trim_start().starts_with("func") {
                    match &resource {
                        Some(prefix) => {
                            resource_methods.insert(format!("{prefix}{}", name.trim()));
                        }
                        None => {
                            host_funcs.insert(name.trim().to_string());
                        }
                    }
                    continue;
                }
            }
            if in_variant {
                if let Some((name, _)) = trimmed.split_once('(') {
                    error_variants.insert(name.trim().to_string());
                }
            }
        }
        (host_funcs, resource_methods, error_variants)
    }

    /// 扫描 core ABI 源码里实际注册的 `func_wrap(HOST_MODULE, "<name>")`。
    fn core_abi_imports(src: &str) -> BTreeSet<String> {
        let mut names = BTreeSet::new();
        let mut lines = src.lines().peekable();
        while let Some(line) = lines.next() {
            if !line.trim_end().ends_with("func_wrap(") {
                continue;
            }
            // 形如：\n            HOST_MODULE,\n            "kv_get",
            let Some(module) = lines.next() else { break };
            if module.trim().trim_end_matches(',') != "HOST_MODULE" {
                continue;
            }
            let Some(name_line) = lines.next() else { break };
            let name = name_line.trim().trim_end_matches(',').trim_matches('"');
            names.insert(name.to_string());
        }
        names
    }

    /// 扫描 JS 绑定源码里的 `set("<name>")`，返回（接收者, 名字）集合。
    ///
    /// 字符串字面量可能换行（`handle.set(\n "next", ...)`），因此按位置扫描
    /// 而不是逐行扫描。
    fn js_set_calls(src: &str) -> BTreeSet<(String, String)> {
        const MARK: &str = ".set(";
        let mut calls = BTreeSet::new();
        let mut from = 0usize;
        while let Some(pos) = src[from..].find(MARK) {
            let abs = from + pos;
            let receiver = src[..abs]
                .rsplit(|c: char| !(c.is_alphanumeric() || c == '_'))
                .next()
                .unwrap_or("")
                .to_string();
            let tail = &src[abs + MARK.len()..];
            if let Some(inner) = tail.trim_start().strip_prefix('"') {
                if let Some(end) = inner.find('"') {
                    calls.insert((receiver, inner[..end].to_string()));
                }
            }
            from = abs + MARK.len();
        }
        calls
    }

    /// JS 账本路径 → 期望的（接收者, 名字）。
    fn js_expectation(path: &str) -> (String, String) {
        if let Some((ns, method)) = path.split_once('.') {
            return (ns.to_string(), method.to_string());
        }
        if let Some((_watch, method)) = path.split_once('#') {
            return ("handle".to_string(), method.to_string());
        }
        ("coord".to_string(), path.to_string())
    }

    #[test]
    fn wit_host_surface_matches_ledger() {
        let (host_funcs, resource_methods, error_variants) = wit_surface(WIT);

        let declared: BTreeSet<String> = HOST_OPS
            .iter()
            .filter(|op| !op.wit.contains('.'))
            .map(|op| op.wit.to_string())
            .collect();
        assert_eq!(
            host_funcs, declared,
            "WIT `interface host` 的函数集合与账本不一致（新增原语必须同时改两处）"
        );

        // 资源方法以 `<resource>.<method>` 全名比对（避免跨资源同名方法互相吞掉）。
        let declared_methods: BTreeSet<String> = HOST_OPS
            .iter()
            .filter(|op| op.wit.contains('.'))
            .map(|op| op.wit.to_string())
            .collect();
        assert_eq!(
            resource_methods, declared_methods,
            "WIT `resource` 的方法集合与账本不一致"
        );
        assert!(
            resource_methods
                .iter()
                .any(|m| m.starts_with("subscription.")),
            "watch 订阅的 RAII 方法必须仍在账本内"
        );
        assert!(
            resource_methods.iter().any(|m| m.starts_with("uploader."))
                && resource_methods
                    .iter()
                    .any(|m| m.starts_with("downloader.")),
            "对象存储流式会话（uploader/downloader）的 RAII 方法必须在账本内"
        );

        let declared_errors: BTreeSet<String> =
            SDK_ERRORS.iter().map(|e| e.wit.to_string()).collect();
        assert_eq!(
            error_variants, declared_errors,
            "WIT `variant sdk-error` 的变体集合与账本不一致"
        );
    }

    #[test]
    fn core_abi_surface_matches_ledger() {
        let actual = core_abi_imports(CORE_ABI_SRC);
        let mut declared: BTreeSet<String> = core_import_names().map(str::to_string).collect();
        declared.extend(CORE_ONLY_OPS.iter().map(|s| s.to_string()));
        assert_eq!(
            actual, declared,
            "core ABI（wasm_engine.rs）注册的 import 集合与账本声明不一致"
        );
        // core ABI 是**最小子集**：至少有一个 WIT 原语没有 core 落点
        assert!(
            HOST_OPS.iter().any(|op| op.core.is_none()),
            "core ABI 声明为最小子集，不应覆盖全部 WIT 原语"
        );
        // 独占入口不得与 WIT 原语重名（WIT 新增原语时不会与糖入口撞车）
        let (wit_funcs, _, _) = wit_surface(WIT);
        for only in CORE_ONLY_OPS {
            assert!(
                !wit_funcs.contains(*only) && !HOST_OPS.iter().any(|op| op.wit == *only),
                "core ABI 独占入口 {only} 与 WIT 原语重名"
            );
        }
    }

    #[test]
    fn js_surface_covers_ledger() {
        let calls = js_set_calls(JS_BIND_SRC);
        let mut missing = Vec::new();
        for op in HOST_OPS {
            let Some(path) = op.js else { continue };
            let expected = js_expectation(path);
            if !calls.contains(&expected) {
                missing.push(format!("{} -> {path} (期望 {expected:?})", op.wit));
            }
        }
        assert!(
            missing.is_empty(),
            "JS SDK 缺少账本声明的落点：{}",
            missing.join(", ")
        );
    }

    #[test]
    fn error_names_are_consistent_across_paths() {
        // ① SdkErrorCode → JS 异常名必须在账本里
        for code in SdkErrorCode::ALL {
            let name = code.exception_name();
            let spec = SDK_ERRORS
                .iter()
                .find(|s| s.name == name)
                .unwrap_or_else(|| panic!("SdkErrorCode::{code:?} 的异常名 {name} 不在账本里"));
            // ② WIT 变体名 = 稳定短名（下划线 → 短横线）
            assert_eq!(
                spec.wit,
                code.as_str().replace('_', "-"),
                "SdkErrorCode::{code:?} 的 WIT 变体名与稳定短名不一致"
            );
            // ③ core ABI 码双向一致
            assert_eq!(core_err_code(code), spec.core);
            assert_eq!(core_err_name(spec.core), name);
            assert_eq!(name, core_err_name(core_err_code(code)));
        }

        // ④ core ABI 的传输层码不得与语义码重号
        for (name, code) in CORE_TRANSPORT_ERRORS {
            assert!(
                !SDK_ERRORS.iter().any(|s| s.core == *code),
                "传输层错误码 {name}({code}) 与语义错误码重号"
            );
            assert_eq!(core_err_name(*code), *name);
        }

        // ⑤ 语义码唯一
        let codes: BTreeSet<i32> = SDK_ERRORS.iter().map(|s| s.core).collect();
        assert_eq!(codes.len(), SDK_ERRORS.len(), "语义错误码必须唯一");
    }
}
