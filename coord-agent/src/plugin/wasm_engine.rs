// coord-agent: wasm 插件宿主（Phase 4 / 计划 §11 Phase 4、§8.3 D8）
//
// 第三方计算模块的**强沙箱**：wasmtime + （v1）core wasm ABI。
//
// ──── 与计划的偏差（须在评审时确认）────
// §6 D3 选定的 ABI 是「组件模型 + WIT（宿主 import 类型安全）」。本次先落地
// **D3 明确保留的降级路径：core wasm 手写 ABI**，原因与取舍：
// - 本环境无组件 guest 工具链（`wasm-tools component new` / `cargo component`
//   与 wasm32 目标不可用），组件 guest 夹具无法在 CI 内可重复构建；
// - core ABI 已能覆盖 Phase 4 的验收面（沙箱隔离、资源上限、能力边界），
//   且不引入无法验证的生成代码；
// - 组件模型路径（`wasmtime::component` + WIT world）列为 4.1 spike 的下一步：
//   依赖特性已锁 wasmtime 48.0.1，切换 ABI 不改沙箱策略与宿主 SDK 门面。
//
// ──── 沙箱手段（第三方强制，D8）────
// - **无 WASI**：不链任何 wasi import，guest 只能看见 `coord` 模块（能力白名单
//   即宿主 import 边界本身）；
// - **fuel**：`consume_fuel` + 按 `max_exec_ms` 派生的指令预算 → 耗尽即 trap；
// - **epoch**：`epoch_interruption` + 全局 tick 线程 → 墙钟超时 trap（防「不消耗
//   fuel 的忙等」与长时间轮询）；
// - **内存上限**：`StoreLimits` → `memory.grow` 超限失败（不是 trap）；
// - **能力边界**：宿主 import 统一走 [`PluginSdk`]（作用域守卫 + 调用面钩子），
//   guest 无法绕过（与 JS 路径同一门面）；
// - **崩溃隔离**：任何 trap（fuel/epoch/越界/除零）只终结该插件的 store 并把插件
//   标记 Failed，agent 进程与其它插件不受影响。
//
// ──── ABI v1（core wasm）────
// guest 导出（`alloc` 必需）：
// - `memory`：线性内存；
// - `alloc(len: i32) -> i32`：返回可写区域指针（0 = 失败）；
// - `handle_invoke(method_ptr: i32, method_len: i32, payload_ptr: i32,
//    payload_len: i32) -> i64`：
//     成功 = `(ptr << 32) | len`（`len` 为应答字节数，`ptr` 为 guest 内存偏移）；
//     失败 = 高 32 位为 0 且低 32 位为负 int32（错误码见 `ERR_*`），
//     即 `(i64.extend_i32_u err)`；
// - `init() -> i32`（可选）：非 0 = 初始化失败；
// - `stop()`（可选）。
//
// 宿主导入（module `"coord"`，全部同步、返回 i32：`>= 0` = 写入字节数，
// `< 0` = 错误码）：
// - `kv_get(key_ptr, key_len, out_ptr, out_cap)`
// - `kv_put(key_ptr, key_len, val_ptr, val_len, out_ptr, out_cap)`
// - `kv_create(key_ptr, key_len, val_ptr, val_len, lease_id: i64, out_ptr, out_cap)`
//   （create-if-absent CAS：键存在 → `ERR_CONFLICT`）
// - `kv_delete(key_ptr, key_len, out_ptr, out_cap)`
// - `lease_grant(ttl_secs: i64, out_ptr, out_cap)`
// - `lease_revoke(lease_id: i64)`
// - `env(key_ptr, key_len, out_ptr, out_cap)`
// - `log(level_ptr, level_len, msg_ptr, msg_len)`
//
// 多字节整数一律小端 8 字节（i64）。
//
// 宿主 import 是**同步**的（guest 视角无异步 wasm）：宿主函数内部用
// `Handle::block_on` 驱动 coord-client 调用。插件调用跑在自己的专属线程
// （非 async 上下文），因此不会嵌套 runtime（R1 桥的另一端）。

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use wasmtime::{
    Caller, Config, Engine, Extern, Instance, Linker, Memory, Module, Store, StoreLimits,
    StoreLimitsBuilder, TypedFunc,
};

use crate::plugin::identity::PluginIdentityManager;
use crate::plugin::manifest::{PluginLimits, PluginManifest, PluginRuntime};
use crate::plugin::sdk::backend::{KvDelete, KvPut};
use crate::plugin::sdk::{PluginSdk, SdkError, SdkErrorCode};
use crate::plugin::{Plugin, PluginLoader, PluginStatus};
use crate::service::ServiceResult;

// ──── ABI 常量 ────

/// 宿主导入模块名。
pub const HOST_MODULE: &str = "coord";

/// guest 内存中单次读写上限（对齐 gRPC 4MiB 解码上限）。
const MAX_IO_BYTES: usize = 4 * 1024 * 1024;

/// 插件命令队列深度上限（第四轮 §3.10 i）。
///
/// 队列此前是**无界**的，而每条命令持有整个 payload（上限 [`MAX_IO_BYTES`]）。
/// 取 16：最坏驻留 ≈ 16 × 4 MiB = 64 MiB/插件，同时远大于正常并发（插件线程串行
/// 执行，且调用方在 `max_exec_ms + 1s` 的 watchdog 内就会放弃）。
///
/// 队列满不是故障而是**背压**：返回显式错误让调用方退避，而不是继续吞内存。
const MAX_PLUGIN_QUEUE_DEPTH: usize = 16;

/// epoch 滴答间隔（毫秒）：`max_exec_ms` 换算成 epoch 数。
///
/// 组件模型路径（`component_engine.rs`）复用同一节奏，保证两条 ABI 的
/// 超时语义一致。
pub(crate) const EPOCH_TICK_MS: u64 = 10;

/// 每毫秒允许的燃料（指令预算派生系数）。
///
/// fuel 是**确定性**上限（指令计数），epoch 是墙钟上限；两者互补：
/// fuel 挡住「计算爆炸」，epoch 挡住「不推进指令的等待」。
pub(crate) const FUEL_PER_MS: u64 = 200_000;

// 错误码（宿主 import 返回值 / `handle_invoke` 低 32 位）
//
// 定义在 `plugin::abi`（三条 ABI 路径共用的**单一来源**，见账本模块文档）；
// 本文件只做别名导入 + 值→名/名→值的查表，数值与命名规则不在此处维护。
use super::abi::{self, ERR_BUFFER_TOO_SMALL, ERR_INVALID, ERR_MEMORY, ERR_NOT_FOUND};

/// ABI 错误码 → 稳定名（与 JS 路径的 `Err*` 命名一致，便于观测与插件作者排障）。
fn err_name(code: i32) -> &'static str {
    abi::core_err_name(code)
}

/// [`SdkErrorCode`] → ABI 错误码。
fn err_code(code: SdkErrorCode) -> i32 {
    abi::core_err_code(code)
}

fn sdk_err_code(e: &SdkError) -> i32 {
    err_code(e.code)
}

/// wasm trap 分类（指标 `reason` 标签；未知一律 `"other"`）。
///
/// 与 [`Trap`] 的语义对应：`OutOfFuel` = 指令预算耗尽（确定性上限），
/// `Interrupt` = epoch 墙钟超时；内存/栈越界同样计入沙箱拒绝。
///
/// 组件模型路径复用本函数 → core ABI 与组件 ABI 的 trap 指标口径一致。
pub(crate) fn classify_trap(err: &wasmtime::Error) -> &'static str {
    match err.downcast_ref::<wasmtime::Trap>() {
        Some(wasmtime::Trap::OutOfFuel) => "fuel",
        Some(wasmtime::Trap::Interrupt) => "epoch",
        Some(wasmtime::Trap::MemoryOutOfBounds) | Some(wasmtime::Trap::StackOverflow) => "memory",
        _ => "other",
    }
}

// ──── 共享引擎（编译一次 + 全局 epoch 滴答）────

struct SharedEngine {
    engine: Engine,
    stop: AtomicBool,
}

impl Drop for SharedEngine {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

impl SharedEngine {
    fn new() -> Result<Arc<Self>, String> {
        let mut config = Config::new();
        // 确定性指令预算 + 墙钟中断（D8 第三方强制手段）
        config.consume_fuel(true);
        config.epoch_interruption(true);
        // 无组件模型 / 无线程：能力面即宿主 import 边界
        config.wasm_component_model(false);
        let engine = Engine::new(&config).map_err(|e| format!("wasmtime engine init: {e}"))?;
        let shared = Arc::new(Self {
            engine,
            stop: AtomicBool::new(false),
        });

        // epoch 滴答线程：进程内所有 wasm store 共享一个 tick 源。
        let weak = Arc::downgrade(&shared);
        std::thread::Builder::new()
            .name("coord-plugin-epoch".into())
            .spawn(move || loop {
                let Some(shared) = weak.upgrade() else {
                    return; // 引擎已释放
                };
                if shared.stop.load(Ordering::Relaxed) {
                    return;
                }
                drop(shared);
                std::thread::sleep(Duration::from_millis(EPOCH_TICK_MS));
                if let Some(shared) = weak.upgrade() {
                    shared.engine.increment_epoch();
                }
            })
            .map_err(|e| format!("failed to spawn epoch ticker: {e}"))?;

        Ok(shared)
    }
}

// ──── store 数据 + 宿主函数上下文 ────

/// 单插件 store 的宿主数据。
struct HostState {
    sdk: Arc<PluginSdk>,
    env: BTreeMap<String, String>,
    limits: StoreLimits,
    /// agent 主运行时句柄（宿主函数在其上驱动 coord-client 调用）。
    rt: tokio::runtime::Handle,
    /// 单次调用的墙钟预算（毫秒），用于 epoch deadline。
    max_exec_ms: u64,
}

impl HostState {
    fn new(
        sdk: Arc<PluginSdk>,
        env: BTreeMap<String, String>,
        limits: &PluginLimits,
        rt: tokio::runtime::Handle,
    ) -> Self {
        let bytes = (limits.max_memory_mb as usize).saturating_mul(1024 * 1024);
        let store_limits = StoreLimitsBuilder::new()
            .memory_size(bytes)
            .instances(1)
            .tables(4)
            .memories(1)
            .build();
        Self {
            sdk,
            env,
            limits: store_limits,
            rt,
            max_exec_ms: limits.max_exec_ms,
        }
    }

    /// guest 侧 epoch deadline（滴答数）。
    fn epoch_deadline(&self) -> u64 {
        (self.max_exec_ms / EPOCH_TICK_MS).max(1)
    }

    /// 单次调用的燃料预算。
    fn fuel_budget(&self) -> u64 {
        self.max_exec_ms.saturating_mul(FUEL_PER_MS).max(1_000_000)
    }
}

// ──── guest 内存读写 ────

fn guest_memory(caller: &mut Caller<'_, HostState>) -> Result<Memory, i32> {
    match caller.get_export("memory") {
        Some(Extern::Memory(m)) => Ok(m),
        _ => Err(ERR_MEMORY),
    }
}

/// 读 guest 内存区间（越界 / 超上限 → 错误码）。
fn read_guest(caller: &mut Caller<'_, HostState>, ptr: i32, len: i32) -> Result<Vec<u8>, i32> {
    if len < 0 || ptr < 0 {
        return Err(ERR_INVALID);
    }
    let len = len as usize;
    if len > MAX_IO_BYTES {
        return Err(ERR_INVALID);
    }
    let mem = guest_memory(caller)?;
    let data = mem.data_mut(caller);
    let start = ptr as usize;
    let end = start.checked_add(len).ok_or(ERR_MEMORY)?;
    if end > data.len() {
        return Err(ERR_MEMORY);
    }
    Ok(data[start..end].to_vec())
}

/// 写 guest 内存区间，返回写入字节数（缓冲区不足 / 越界 → 错误码）。
fn write_guest(caller: &mut Caller<'_, HostState>, ptr: i32, cap: i32, bytes: &[u8]) -> i32 {
    if ptr < 0 || cap < 0 {
        return ERR_INVALID;
    }
    if bytes.len() > cap as usize {
        return ERR_BUFFER_TOO_SMALL;
    }
    if bytes.len() > MAX_IO_BYTES {
        return ERR_INVALID;
    }
    let mem = match guest_memory(caller) {
        Ok(m) => m,
        Err(c) => return c,
    };
    let start = ptr as usize;
    let end = match start.checked_add(bytes.len()) {
        Some(e) => e,
        None => return ERR_MEMORY,
    };
    let data = mem.data_mut(caller);
    if end > data.len() {
        return ERR_MEMORY;
    }
    data[start..end].copy_from_slice(bytes);
    bytes.len() as i32
}

/// i64 小端编码（ABI 统一多字节表示）。
fn i64_le(v: i64) -> [u8; 8] {
    v.to_le_bytes()
}

/// 在 agent 运行时上驱动一次 SDK 调用（插件线程不是 async 上下文）。
fn block_on<T>(caller: &Caller<'_, HostState>, fut: impl std::future::Future<Output = T>) -> T {
    let rt = caller.data().rt.clone();
    rt.block_on(fut)
}

/// 提取 guest 传入的字节参数。
macro_rules! guest_bytes {
    ($caller:expr, $ptr:expr, $len:expr) => {
        match read_guest(&mut $caller, $ptr, $len) {
            Ok(v) => v,
            Err(code) => return code,
        }
    };
}

// ──── 宿主函数 ────

fn define_host_functions(linker: &mut Linker<HostState>) -> Result<(), String> {
    let wrap_err =
        |e: wasmtime::Error| -> String { format!("wasmtime host import definition failed: {e}") };

    // coord.kv_get（与组件 ABI 的 `kv-get`、JS 的 `coord.kv.get` 共用门面实现）
    linker
        .func_wrap(
            HOST_MODULE,
            "kv_get",
            |mut caller: Caller<'_, HostState>, kp: i32, kl: i32, op: i32, oc: i32| -> i32 {
                let key = guest_bytes!(caller, kp, kl);
                let sdk = Arc::clone(&caller.data().sdk);
                let res = block_on(&caller, sdk.kv_get(key));
                match res {
                    Ok(value) => write_guest(&mut caller, op, oc, &value),
                    Err(e) => sdk_err_code(&e),
                }
            },
        )
        .map_err(wrap_err)?;

    // coord.kv_put
    linker
        .func_wrap(
            HOST_MODULE,
            "kv_put",
            |mut caller: Caller<'_, HostState>,
             kp: i32,
             kl: i32,
             vp: i32,
             vl: i32,
             op: i32,
             oc: i32|
             -> i32 {
                let key = guest_bytes!(caller, kp, kl);
                let value = guest_bytes!(caller, vp, vl);
                let sdk = Arc::clone(&caller.data().sdk);
                let res = block_on(
                    &caller,
                    sdk.kv_put(KvPut {
                        key,
                        value,
                        lease_id: 0,
                        prev_kv: false,
                        request_id: Vec::new(),
                    }),
                );
                match res {
                    Ok(out) => write_guest(&mut caller, op, oc, &i64_le(out.revision)),
                    Err(e) => sdk_err_code(&e),
                }
            },
        )
        .map_err(wrap_err)?;

    // coord.kv_create（create-if-absent CAS；与组件 / JS 路径共用门面实现）
    linker
        .func_wrap(
            HOST_MODULE,
            "kv_create",
            |mut caller: Caller<'_, HostState>,
             kp: i32,
             kl: i32,
             vp: i32,
             vl: i32,
             lease_id: i64,
             op: i32,
             oc: i32|
             -> i32 {
                let key = guest_bytes!(caller, kp, kl);
                let value = guest_bytes!(caller, vp, vl);
                let sdk = Arc::clone(&caller.data().sdk);
                let res = block_on(&caller, sdk.kv_create(key, value, lease_id));
                match res {
                    Ok(revision) => write_guest(&mut caller, op, oc, &i64_le(revision)),
                    Err(e) => sdk_err_code(&e),
                }
            },
        )
        .map_err(wrap_err)?;

    // coord.kv_delete
    linker
        .func_wrap(
            HOST_MODULE,
            "kv_delete",
            |mut caller: Caller<'_, HostState>, kp: i32, kl: i32, op: i32, oc: i32| -> i32 {
                let key = guest_bytes!(caller, kp, kl);
                let sdk = Arc::clone(&caller.data().sdk);
                let res = block_on(
                    &caller,
                    sdk.kv_delete(KvDelete {
                        key,
                        range_end: Vec::new(),
                        prev_kv: false,
                        request_id: Vec::new(),
                    }),
                );
                match res {
                    Ok(out) => write_guest(&mut caller, op, oc, &i64_le(out.deleted)),
                    Err(e) => sdk_err_code(&e),
                }
            },
        )
        .map_err(wrap_err)?;

    // coord.lease_grant
    linker
        .func_wrap(
            HOST_MODULE,
            "lease_grant",
            |mut caller: Caller<'_, HostState>, ttl_secs: i64, op: i32, oc: i32| -> i32 {
                let sdk = Arc::clone(&caller.data().sdk);
                let res = block_on(&caller, sdk.lease_grant(ttl_secs, 0));
                match res {
                    Ok(id) => write_guest(&mut caller, op, oc, &i64_le(id)),
                    Err(e) => sdk_err_code(&e),
                }
            },
        )
        .map_err(wrap_err)?;

    // coord.lease_revoke
    linker
        .func_wrap(
            HOST_MODULE,
            "lease_revoke",
            |caller: Caller<'_, HostState>, lease_id: i64| -> i32 {
                let sdk = Arc::clone(&caller.data().sdk);
                match block_on(&caller, sdk.lease_revoke(lease_id)) {
                    Ok(()) => 0,
                    Err(e) => sdk_err_code(&e),
                }
            },
        )
        .map_err(wrap_err)?;

    // coord.env
    linker
        .func_wrap(
            HOST_MODULE,
            "env",
            |mut caller: Caller<'_, HostState>, kp: i32, kl: i32, op: i32, oc: i32| -> i32 {
                let key = guest_bytes!(caller, kp, kl);
                let key = match String::from_utf8(key) {
                    Ok(k) => k,
                    Err(_) => return ERR_INVALID,
                };
                let value = caller.data().env.get(&key).cloned();
                match value {
                    Some(v) => write_guest(&mut caller, op, oc, v.as_bytes()),
                    None => ERR_NOT_FOUND,
                }
            },
        )
        .map_err(wrap_err)?;

    // coord.log
    linker
        .func_wrap(
            HOST_MODULE,
            "log",
            |mut caller: Caller<'_, HostState>, lp: i32, ll: i32, mp: i32, ml: i32| -> i32 {
                let level = guest_bytes!(caller, lp, ll);
                let msg = guest_bytes!(caller, mp, ml);
                let level = String::from_utf8_lossy(&level).into_owned();
                let msg = String::from_utf8_lossy(&msg).into_owned();
                let plugin = caller.data().sdk.plugin().to_string();
                match level.as_str() {
                    "error" => tracing::error!(target: "coord_agent::plugin", "[{plugin}] {msg}"),
                    "warn" | "warning" => {
                        tracing::warn!(target: "coord_agent::plugin", "[{plugin}] {msg}")
                    }
                    "debug" => tracing::debug!(target: "coord_agent::plugin", "[{plugin}] {msg}"),
                    "trace" => tracing::trace!(target: "coord_agent::plugin", "[{plugin}] {msg}"),
                    _ => tracing::info!(target: "coord_agent::plugin", "[{plugin}] {msg}"),
                }
                0
            },
        )
        .map_err(wrap_err)?;

    Ok(())
}

// ──── 插件线程内的实例 ────

/// 武装一次执行的资源预算（fuel 指令数 + epoch 墙钟增量）。
///
/// `set_epoch_deadline` 是**相对当前 epoch 的增量**，因此每次 wasm 执行
/// （实例化 / `init` / `invoke`）前都要重新武装。
fn arm_budget(store: &mut Store<HostState>) -> Result<(), String> {
    let budget = store.data().fuel_budget();
    store
        .set_fuel(budget)
        .map_err(|e| format!("failed to reset fuel: {e}"))?;
    let deadline = store.data().epoch_deadline();
    store.set_epoch_deadline(deadline);
    Ok(())
}

/// 已实例化的 wasm 插件（store + 导出函数句柄）。
struct WasmInstance {
    store: Store<HostState>,
    instance: Instance,
    memory: Memory,
    alloc: TypedFunc<i32, i32>,
    handle_invoke: TypedFunc<(i32, i32, i32, i32), i64>,
}

/// 调用返回：应答字节 或 错误码（i32 负数）。
enum CallOutcome {
    Response(Vec<u8>),
    /// `handle_invoke` 以负码返回
    PluginError(i32),
}

/// 单次 wasm 调用的失败。
///
/// `trap = Some(reason)` 表示失败由 wasmtime trap（fuel/epoch/越界…）引起，
/// 携带指标标签；`None` 表示宿主侧拒绝（请求过大、alloc 失败等）。
///
/// 两条 ABI（core / 组件模型）共用本类型 → 指标与看门狗语义一致。
#[derive(Debug)]
pub(crate) struct WasmInvokeError {
    pub(crate) message: String,
    pub(crate) trap: Option<&'static str>,
}

impl WasmInvokeError {
    pub(crate) fn host(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            trap: None,
        }
    }

    /// 由 wasmtime trap 构造（`reason` 来自 [`classify_trap`]）。
    pub(crate) fn trapped(reason: &'static str, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            trap: Some(reason),
        }
    }
}

impl From<String> for WasmInvokeError {
    fn from(message: String) -> Self {
        Self::host(message)
    }
}

impl WasmInstance {
    fn new(
        shared: &SharedEngine,
        module: &Module,
        sdk: Arc<PluginSdk>,
        env: BTreeMap<String, String>,
        limits: &PluginLimits,
        rt: tokio::runtime::Handle,
    ) -> Result<Self, String> {
        let mut store = Store::new(&shared.engine, HostState::new(sdk, env, limits, rt));
        store.limiter(|state| &mut state.limits);
        // 武装预算后再实例化（与组件模型路径同一做法）：`set_epoch_deadline`
        // 是相对当前 epoch 的增量，给 1 个 tick 会让「实例化期间执行 guest
        // start 代码」越过 10ms 滴答后以 `wasm trap: interrupt` 失败。
        arm_budget(&mut store)?;

        let mut linker = Linker::new(&shared.engine);
        define_host_functions(&mut linker)?;
        let instance: Instance = linker
            .instantiate(&mut store, module)
            .map_err(|e| format!("instantiate failed: {e}"))?;

        let memory = match instance.get_export(&mut store, "memory") {
            Some(Extern::Memory(m)) => m,
            _ => return Err("module must export its linear memory as 'memory'".into()),
        };
        let alloc = instance
            .get_typed_func::<i32, i32>(&mut store, "alloc")
            .map_err(|e| format!("module must export 'alloc(i32) -> i32': {e}"))?;
        let handle_invoke = instance
            .get_typed_func::<(i32, i32, i32, i32), i64>(&mut store, "handle_invoke")
            .map_err(|e| {
                format!("module must export 'handle_invoke(i32,i32,i32,i32) -> i64': {e}")
            })?;

        // 可选 init()：非 0 = 初始化失败（独立预算）
        if let Ok(init) = instance.get_typed_func::<(), i32>(&mut store, "init") {
            arm_budget(&mut store)?;
            match init.call(&mut store, ()) {
                Ok(0) => {}
                Ok(code) => return Err(format!("plugin init() returned failure code {code}")),
                Err(e) => return Err(format!("plugin init() trapped: {e}")),
            }
        }

        Ok(Self {
            store,
            instance,
            memory,
            alloc,
            handle_invoke,
        })
    }

    /// 调一次 `handle_invoke`（含资源预算重设与 trap → 错误映射）。
    fn invoke(&mut self, method: &str, payload: &[u8]) -> Result<CallOutcome, WasmInvokeError> {
        // 每次调用重设预算：fuel（确定性指令）+ epoch（墙钟）
        arm_budget(&mut self.store).map_err(WasmInvokeError::host)?;

        let (method_ptr, payload_ptr) = {
            let total = method.len() + payload.len();
            if total > MAX_IO_BYTES {
                return Err(WasmInvokeError::host(format!(
                    "request too large ({total} bytes > {MAX_IO_BYTES})"
                )));
            }
            let base = self
                .alloc
                .call(&mut self.store, total as i32)
                .map_err(|e| WasmInvokeError {
                    trap: Some(classify_trap(&e)),
                    message: format!("guest alloc() trapped: {e}"),
                })?;
            if base == 0 {
                return Err(WasmInvokeError::host(
                    "guest alloc() failed (out of memory)",
                ));
            }
            let base = base as usize;
            let size = self.memory.data(&self.store).len();
            if base + total > size {
                return Err(WasmInvokeError::host(
                    "guest alloc() returned an out-of-bounds pointer",
                ));
            }
            {
                let data = self.memory.data_mut(&mut self.store);
                data[base..base + method.len()].copy_from_slice(method.as_bytes());
                data[base + method.len()..base + total].copy_from_slice(payload);
            }
            (base as i32, (base + method.len()) as i32)
        };

        let packed = self
            .handle_invoke
            .call(
                &mut self.store,
                (
                    method_ptr,
                    method.len() as i32,
                    payload_ptr,
                    payload.len() as i32,
                ),
            )
            .map_err(|e| WasmInvokeError {
                trap: Some(classify_trap(&e)),
                message: format!("plugin trapped: {e}"),
            })?;

        let hi = (packed >> 32) as u32;
        let lo = packed as u32;
        if hi == 0 && (lo as i32) < 0 {
            return Ok(CallOutcome::PluginError(lo as i32));
        }
        let len = lo as usize;
        if len > MAX_IO_BYTES {
            return Err(WasmInvokeError::host(format!(
                "response too large ({len} bytes > {MAX_IO_BYTES})"
            )));
        }
        if len == 0 {
            return Ok(CallOutcome::Response(Vec::new()));
        }
        let ptr = hi as usize;
        let data = self.memory.data(&self.store);
        let end = ptr
            .checked_add(len)
            .ok_or_else(|| WasmInvokeError::host("response pointer overflow"))?;
        if end > data.len() {
            return Err(WasmInvokeError::host("response pointer out of bounds"));
        }
        Ok(CallOutcome::Response(data[ptr..end].to_vec()))
    }

    /// 可选 `stop()`（插件卸载/停止钩子）。
    fn stop(&mut self) {
        if let Ok(stop) = self
            .instance
            .get_typed_func::<(), ()>(&mut self.store, "stop")
        {
            let _ = stop.call(&mut self.store, ());
        }
    }
}

// ──── 插件加载器 ────

/// wasm 插件加载器（`[plugins].dir` 下的 `.wasm` 模块）。
pub struct WasmPluginLoader {
    dir: PathBuf,
    backend: Arc<dyn crate::plugin::sdk::PluginSdkBackend>,
    env: BTreeMap<String, String>,
    hooks: Option<Arc<crate::plugin::hooks::HookRegistry>>,
    identity: Option<Arc<PluginIdentityManager>>,
    metrics: Option<crate::metrics::AgentMetrics>,
    rc: tokio::runtime::Handle,
    shared: Arc<SharedEngine>,
    /// 组件模型引擎（**惰性**创建：只有真的加载到组件时才初始化第二个 Engine
    /// + epoch 滴答线程；core ABI 部署零额外开销）。
    component:
        std::sync::OnceLock<Result<Arc<crate::plugin::component_engine::ComponentLoader>, String>>,
}

impl WasmPluginLoader {
    /// 由插件目录 + SDK 后端 + 环境注入构建（编译引擎 + 启动 epoch 滴答线程）。
    pub fn new(
        dir: impl Into<PathBuf>,
        backend: Arc<dyn crate::plugin::sdk::PluginSdkBackend>,
        env: BTreeMap<String, String>,
        rc: tokio::runtime::Handle,
    ) -> Result<Self, String> {
        Ok(Self {
            dir: dir.into(),
            backend,
            env,
            hooks: None,
            identity: None,
            metrics: None,
            rc,
            shared: SharedEngine::new()?,
            component: std::sync::OnceLock::new(),
        })
    }

    /// 惰性初始化组件模型引擎；初始化失败会在后续调用中稳定复现（不重试）。
    fn component_loader(
        &self,
    ) -> Result<Arc<crate::plugin::component_engine::ComponentLoader>, String> {
        let cell = self.component.get_or_init(|| {
            crate::plugin::component_engine::ComponentLoader::new(
                self.dir.clone(),
                Arc::clone(&self.backend),
                self.env.clone(),
                self.hooks.clone(),
                self.identity.clone(),
                self.metrics.clone(),
            )
            .map(Arc::new)
        });
        match cell {
            Ok(loader) => Ok(Arc::clone(loader)),
            Err(e) => Err(e.clone()),
        }
    }

    /// 挂载调用面钩子注册表（仅作用于插件发起的调用）。
    pub fn with_hooks(mut self, hooks: Arc<crate::plugin::hooks::HookRegistry>) -> Self {
        self.hooks = Some(hooks);
        self
    }

    /// 挂载插件身份管理器（每插件独立服务账户 + 受限 CCT 自动续期，D5/§10.1）。
    pub fn with_identity(mut self, identity: Arc<PluginIdentityManager>) -> Self {
        self.identity = Some(identity);
        self
    }

    /// 挂载插件指标（调用结果 + sandbox trap 计数）。
    pub fn with_metrics(mut self, metrics: crate::metrics::AgentMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// 解析插件入口的绝对路径（限制在插件目录内，与 JS 加载器同规则）。
    pub(crate) fn entry_path(&self, manifest: &PluginManifest) -> Result<PathBuf, String> {
        let rel = manifest.resolved_entry();
        let path = crate::plugin::normalize_plugin_path(&self.dir.join(rel));
        if !path.starts_with(&self.dir) {
            return Err(format!(
                "plugin '{}' entry '{}' escapes the plugin dir",
                manifest.name, rel
            ));
        }
        Ok(path)
    }
}

#[async_trait]
impl PluginLoader for WasmPluginLoader {
    async fn load(&self, manifest: &PluginManifest) -> ServiceResult<Arc<dyn Plugin>> {
        manifest
            .validate()
            .map_err(crate::service::ServiceError::from)?;
        if manifest.runtime != PluginRuntime::Wasm {
            return Err(format!(
                "WasmPluginLoader cannot load runtime '{}' (plugin '{}')",
                manifest.runtime.as_str(),
                manifest.name
            )
            .into());
        }
        let entry = self
            .entry_path(manifest)
            .map_err(crate::service::ServiceError::from)?;
        let bytes = tokio::fs::read(&entry)
            .await
            .map_err(|e| -> crate::service::ServiceError {
                format!(
                    "failed to read plugin '{}' module {}: {e}",
                    manifest.name,
                    entry.display()
                )
                .into()
            })?;

        // ABI 自动判别（Phase 4.1）：组件二进制 → 组件模型宿主（WIT 类型安全 +
        // 异步宿主 import）；core module → 本文件的手写 ABI。两条路径共用
        // 同一 `PluginSdk` 门面 / 沙箱手段 / 指标口径，manifest 语义一致。
        if crate::plugin::component_engine::is_component_binary(&bytes) {
            let loader = self
                .component_loader()
                .map_err(crate::service::ServiceError::from)?;
            return loader.load_bytes(manifest, entry, &bytes).await;
        }

        let module = Module::new(&self.shared.engine, &bytes).map_err(
            |e| -> crate::service::ServiceError {
                format!(
                    "plugin '{}' module {} failed to compile: {e}",
                    manifest.name,
                    entry.display()
                )
                .into()
            },
        )?;
        let mut sdk = PluginSdk::new(
            manifest.name.clone(),
            &manifest.capabilities,
            Arc::clone(&self.backend),
        );
        if let Some(hooks) = &self.hooks {
            sdk = sdk.with_hooks(Arc::clone(hooks));
        }

        // Phase 4.x：开通插件服务账户（失败 → 警告并回退共享未鉴权客户端）。
        // 与 JS 引擎同一 `PluginIdentityManager`，出站隔离语义一致（D5）。
        if let Some(identity) = &self.identity {
            if let Err(e) = identity
                .ensure(&manifest.name, &manifest.capabilities)
                .await
            {
                tracing::warn!(
                    "wasm plugin '{}': identity provisioning failed ({e}); outbound calls fall \
                     back to the shared agent client",
                    manifest.name
                );
            }
        }

        Ok(Arc::new(WasmPlugin {
            manifest: manifest.clone(),
            entry_path: entry,
            fingerprint: crate::plugin::content_fingerprint_bytes(&bytes),
            module,
            sdk: Arc::new(sdk),
            env: self.env.clone(),
            limits: manifest.limits.clone(),
            shared: Arc::clone(&self.shared),
            rt: self.rc.clone(),
            identity: self.identity.clone(),
            metrics: self.metrics.clone(),
            trapped: Arc::new(AtomicBool::new(false)),
            state: Mutex::new(WasmState::new()),
        }))
    }

    /// 模块文件当前内容指纹（不编译）：与 `WasmPlugin::content_fingerprint` 比对。
    fn fingerprint(&self, manifest: &PluginManifest) -> Option<String> {
        let path = self.entry_path(manifest).ok()?;
        crate::plugin::content_fingerprint(&path)
    }
}

// ──── 插件实例 ────

struct WasmState {
    status: PluginStatus,
    /// 有界命令队列的发送端（见 [`MAX_PLUGIN_QUEUE_DEPTH`]）：`SyncSender` 才能
    /// 表达"满了就背压"，`Sender` 是无界的。
    tx: Option<std::sync::mpsc::SyncSender<WasmCommand>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl WasmState {
    fn new() -> Self {
        Self {
            status: PluginStatus::Loaded,
            tx: None,
            thread: None,
        }
    }
}

enum WasmCommand {
    Invoke {
        method: String,
        payload: Vec<u8>,
        resp: tokio::sync::oneshot::Sender<Result<Vec<u8>, String>>,
    },
    Stop {
        resp: std::sync::mpsc::Sender<()>,
    },
}

/// 单个 wasm 插件实例（一个专属线程 + 一个 wasmtime store）。
pub struct WasmPlugin {
    manifest: PluginManifest,
    entry_path: PathBuf,
    /// 加载时的模块内容指纹（`sha256:<hex>`；Phase 5 版本化重载检测）
    fingerprint: String,
    module: Module,
    sdk: Arc<PluginSdk>,
    env: BTreeMap<String, String>,
    limits: PluginLimits,
    shared: Arc<SharedEngine>,
    rt: tokio::runtime::Handle,
    /// 插件身份管理器（Some = 停止时注销该插件的受限 CCT 账户）
    identity: Option<Arc<PluginIdentityManager>>,
    /// 插件指标（fuel/epoch trap 计数 + 调用结果计数）
    metrics: Option<crate::metrics::AgentMetrics>,
    /// 插件线程是否因 trap 丢弃过实例（fuel/epoch/OOB…）。
    /// store 一旦 trap 不可继续使用：置位后由调用方把插件标记 Failed。
    trapped: Arc<AtomicBool>,
    state: Mutex<WasmState>,
}

impl std::fmt::Debug for WasmPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmPlugin")
            .field("name", &self.manifest.name)
            .field("entry", &self.entry_path)
            .finish_non_exhaustive()
    }
}

impl WasmPlugin {
    fn fail(&self, reason: impl Into<String>) {
        let reason = reason.into();
        tracing::error!("wasm plugin '{}' failed: {reason}", self.manifest.name);
        let mut st = self.state.lock();
        st.status = PluginStatus::Failed(reason);
        st.tx = None;
    }

    /// 单次调用（含外层看门狗：epoch 失效时兜底，避免插件线程卡死拖住调用方）。
    async fn invoke_inner(&self, method: &str, payload: &[u8]) -> Result<Vec<u8>, String> {
        let tx = {
            let st = self.state.lock();
            st.tx.clone()
        };
        let Some(tx) = tx else {
            return Err(format!("plugin '{}' is not running", self.manifest.name));
        };
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        let cmd = WasmCommand::Invoke {
            method: method.to_string(),
            payload: payload.to_vec(),
            resp: resp_tx,
        };
        // 有界队列：满 = 背压（fail-closed），不得阻塞 async 执行器。
        match tx.try_send(cmd) {
            Ok(()) => {}
            Err(std::sync::mpsc::TrySendError::Full(_)) => {
                return Err(format!(
                    "plugin '{}' invocation queue is full ({MAX_PLUGIN_QUEUE_DEPTH} pending) — 
                     backpressure, retry after the in-flight calls drain",
                    self.manifest.name
                ));
            }
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                self.fail("plugin thread is gone");
                return Err(format!("plugin '{}' thread is gone", self.manifest.name));
            }
        }
        // 外层看门狗：epoch 预算 + 1s 余量（wasm 内部应先 trap）
        let wait = Duration::from_millis(self.limits.max_exec_ms.saturating_add(1_000));
        let result = match tokio::time::timeout(wait, resp_rx).await {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => {
                self.fail("plugin thread dropped the response channel");
                return Err(format!("plugin '{}' thread is gone", self.manifest.name));
            }
            Err(_) => {
                self.fail(format!("invoke watchdog timed out after {wait:?}"));
                return Err(format!(
                    "plugin '{}' invoke exceeded the watchdog ({wait:?})",
                    self.manifest.name
                ));
            }
        };
        if result.is_err() && self.trapped.load(Ordering::Acquire) {
            // trap（fuel/epoch/越界/除零）→ store 已丢弃，插件不再可用
            self.fail(format!(
                "wasm trap discarded the instance: {}",
                result.as_ref().err().cloned().unwrap_or_default()
            ));
        }
        result
    }
}

#[async_trait]
impl Plugin for WasmPlugin {
    fn name(&self) -> &str {
        &self.manifest.name
    }

    /// 内容指纹 = 加载时的模块字节 SHA256（Phase 5 版本化）。
    fn content_fingerprint(&self) -> Option<String> {
        Some(self.fingerprint.clone())
    }

    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    async fn init(&self) -> ServiceResult<()> {
        // 第四轮 §3.10 i：**有界**命令队列。
        //
        // 此前是 `std::sync::mpsc::channel()`（无界）：调用方每次 `invoke` 都把
        // 方法名 + 整个 payload（上限 `MAX_IO_BYTES` = 4 MiB）推进队列，而插件线程
        // 单条串行执行。并发调用一多，队列就能无界增长（调用方在 watchdog 超时前
        // 不会退，但每一条已经排进队列的命令都占着内存）。
        //
        // 现在改为有界 + `try_send` 背压：满了直接返回错误（fail-closed），
        // **不**用阻塞式 `send` —— 在 async 上下文里阻塞发送会挂死执行器。
        let (tx, rx) = std::sync::mpsc::sync_channel::<WasmCommand>(MAX_PLUGIN_QUEUE_DEPTH);
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<Result<(), String>>();

        let thread = {
            let name = self.manifest.name.clone();
            let module = self.module.clone();
            let sdk = Arc::clone(&self.sdk);
            let env = self.env.clone();
            let limits = self.limits.clone();
            let shared = Arc::clone(&self.shared);
            let rt = self.rt.clone();
            let trapped = Arc::clone(&self.trapped);
            let metrics = self.metrics.clone();

            std::thread::Builder::new()
                .name(format!("coord-plugin-{name}"))
                .spawn(move || {
                    let mut instance = match WasmInstance::new(
                        &shared, &module, sdk, env, &limits, rt,
                    ) {                        Ok(i) => i,
                        Err(e) => {
                            let _ = ready_tx.send(Err(e));
                            return;
                        }
                    };
                    if ready_tx.send(Ok(())).is_err() {
                        return;
                    }
                    // 命令循环（同步 recv：宿主函数内部阻塞驱动 coord-client）
                    while let Ok(cmd) = rx.recv() {
                        match cmd {
                            WasmCommand::Invoke {
                                method,
                                payload,
                                resp,
                            } => {
                                let out = instance.invoke(&method, &payload);
                                let fatal = out.is_err();
                                let (mapped, trap) = match out {
                                    Ok(CallOutcome::Response(bytes)) => (Ok(bytes), None),
                                    Ok(CallOutcome::PluginError(code)) => (
                                        Err(format!(
                                            "plugin '{name}' returned {} (abi code {code})",
                                            err_name(code)
                                        )),
                                        None,
                                    ),
                                    Err(e) => (Err(e.message), e.trap),
                                };
                                // 关键顺序：`trapped` 必须在**回包之前**置位。
                                //
                                // 调用方（`invoke_inner`）收到错误响应后会立即读 `trapped`
                                // 来决定是否把插件标记为 `Failed`。此前是先 `resp.send()`、
                                // 再 `trapped.store(..)`，于是调用方几乎总是读到 `false`
                                // ⇒ **trap 掉的插件仍被标记 `Started`**：继续被路由、
                                // 健康检查也仍报好，而它对应的实例/存储已被丢弃。
                                // （该竞态由 `component_plugin_spin_traps_and_is_isolated`
                                // 在 CI 上暴露；两个引擎同构，一并修正。）
                                // `Release`/`Acquire` 与下面的回包共同建立 happens-before。
                                if fatal {
                                    if let (Some(metrics), Some(reason)) = (&metrics, trap) {
                                        metrics.record_plugin_trap(&name, reason);
                                    }
                                    trapped.store(true, Ordering::Release);
                                }
                                let _ = resp.send(mapped);
                                if fatal {
                                    // trap（fuel/epoch/OOB…）→ store 不可继续使用 → 丢弃实例
                                    tracing::error!(
                                        "wasm plugin '{name}' trapped (reason={}); discarding instance",
                                        trap.unwrap_or("other")
                                    );
                                    break;
                                }
                            }
                            WasmCommand::Stop { resp } => {
                                instance.stop();
                                let _ = resp.send(());
                                break;
                            }
                        }
                    }
                    tracing::debug!("wasm plugin '{name}' thread exited");
                })
                .map_err(|e| -> crate::service::ServiceError {
                    format!("failed to spawn wasm plugin thread: {e}").into()
                })?
        };

        {
            let mut st = self.state.lock();
            st.tx = Some(tx);
            st.thread = Some(thread);
        }

        match ready_rx.await {
            Ok(Ok(())) => {
                tracing::info!(
                    "wasm plugin '{}' initialised (entry={}, max_memory={}MiB, max_exec={}ms, \
                     fuel={}, epoch_ticks={})",
                    self.manifest.name,
                    self.entry_path.display(),
                    self.limits.max_memory_mb,
                    self.limits.max_exec_ms,
                    self.limits.max_exec_ms.saturating_mul(FUEL_PER_MS),
                    (self.limits.max_exec_ms / EPOCH_TICK_MS).max(1),
                );
                Ok(())
            }
            Ok(Err(e)) => {
                self.fail(e);
                Err("wasm plugin initialisation failed (see logs)".into())
            }
            Err(_) => {
                self.fail("wasm plugin thread exited during init");
                Err("wasm plugin initialisation failed".into())
            }
        }
    }

    async fn start(&self) -> ServiceResult<()> {
        let mut st = self.state.lock();
        if st.tx.is_none() {
            return Err(format!("plugin '{}' is not initialised", self.manifest.name).into());
        }
        st.status = PluginStatus::Started;
        Ok(())
    }

    async fn stop(&self) -> ServiceResult<()> {
        let (tx, thread) = {
            let mut st = self.state.lock();
            st.status = PluginStatus::Stopped;
            (st.tx.take(), st.thread.take())
        };
        self.sdk.release().await;
        if let Some(tx) = tx {
            let (resp_tx, resp_rx) = std::sync::mpsc::channel::<()>();
            // 队列可能已被在途 Invoke 占满：`try_send` 失败**不**致命——
            // `tx` 出作用域后通道关闭，插件线程的 `rx.recv()` 会返回 Err 并退出
            // （下面的 `thread.join()` 保持正确）。
            match tx.try_send(WasmCommand::Stop { resp: resp_tx }) {
                Ok(()) => {
                    let _ = tokio::time::timeout(Duration::from_secs(5), async move {
                        let _ = resp_rx.recv();
                    })
                    .await;
                }
                Err(std::sync::mpsc::TrySendError::Full(_)) => tracing::warn!(
                    plugin = %self.manifest.name,
                    "plugin queue full; relying on channel close to stop the plugin thread"
                ),
                Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {}
            }
        }
        if let Some(thread) = thread {
            let _ = tokio::task::spawn_blocking(move || thread.join()).await;
        }
        // 注销插件身份（停止 CCT 续期任务 + 移除 authed client）
        if let Some(identity) = &self.identity {
            identity.forget(&self.manifest.name);
        }
        Ok(())
    }

    fn health_check(&self) -> bool {
        matches!(self.state.lock().status, PluginStatus::Started)
    }

    fn status(&self) -> PluginStatus {
        self.state.lock().status.clone()
    }

    async fn invoke(&self, method: &str, payload: &[u8]) -> ServiceResult<Vec<u8>> {
        let result = self.invoke_checked(method, payload).await;
        if let Some(metrics) = &self.metrics {
            metrics.record_plugin_invocation(&self.manifest.name, "wasm", result.is_ok());
        }
        result.map_err(|e| -> crate::service::ServiceError { e.into() })
    }
}

impl WasmPlugin {
    /// 调用前置检查 + 实际调用（`Plugin::invoke` 负责计数）。
    async fn invoke_checked(&self, method: &str, payload: &[u8]) -> Result<Vec<u8>, String> {
        {
            let st = self.state.lock();
            if !matches!(st.status, PluginStatus::Started) {
                return Err(format!(
                    "plugin '{}' is not started (status={})",
                    self.manifest.name,
                    st.status.as_str()
                ));
            }
        }
        self.invoke_inner(method, payload).await
    }
}

// ──── 测试（WAT 夹具：core ABI 全链路 + 沙箱手段）────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::manifest::{PluginCapability, PluginSource, PluginTrust};
    use crate::plugin::sdk::backend::{
        CompareTarget, KvDeleteOut, KvRange, KvRangeOut, KvRecord, ObjectGetOut, ObjectPutOut,
        ObjectStatDto, PluginSdkBackend, SdkResult, TxnOp, TxnOpOut, TxnOut, TxnReq, WatchEventDto,
        WatchSubscribe, CAP_KV_DELETE, CAP_KV_READ, CAP_KV_WRITE, CAP_LEASE_GRANT,
        CAP_LEASE_REVOKE, CAP_TXN_EXECUTE,
    };
    use parking_lot::Mutex as TestMutex;

    // ──── stub 后端（内存 CAS + 租约）────

    /// stub KV 条目：`(值, 版本)`。
    type StubEntry = (Vec<u8>, i64);

    #[derive(Default)]
    struct StubBackend {
        kvs: TestMutex<std::collections::BTreeMap<Vec<u8>, StubEntry>>,
        revision: TestMutex<i64>,
        lease_seq: TestMutex<i64>,
        puts: TestMutex<Vec<(Vec<u8>, Vec<u8>)>>,
    }

    impl StubBackend {
        fn next_revision(&self) -> i64 {
            let mut r = self.revision.lock();
            *r += 1;
            *r
        }
    }

    #[async_trait]
    impl PluginSdkBackend for StubBackend {
        async fn kv_put(
            &self,
            _plugin: &str,
            req: KvPut,
        ) -> SdkResult<crate::plugin::sdk::KvPutOut> {
            self.puts.lock().push((req.key.clone(), req.value.clone()));
            let revision = self.next_revision();
            let mut kvs = self.kvs.lock();
            match kvs.get_mut(&req.key) {
                Some(entry) => {
                    entry.0 = req.value;
                    entry.1 += 1;
                }
                None => {
                    kvs.insert(req.key, (req.value, 1));
                }
            }
            Ok(crate::plugin::sdk::KvPutOut {
                prev_kv: None,
                revision,
            })
        }

        async fn kv_range(&self, _plugin: &str, req: KvRange) -> SdkResult<KvRangeOut> {
            let kvs = self.kvs.lock();
            let records = kvs
                .iter()
                .filter(|(k, _)| k.starts_with(&req.key))
                .map(|(k, (v, ver))| KvRecord {
                    key: k.clone(),
                    value: v.clone(),
                    lease_id: 0,
                    version: *ver,
                })
                .collect::<Vec<_>>();
            Ok(KvRangeOut {
                count: records.len() as i64,
                kvs: records,
                revision: *self.revision.lock(),
            })
        }

        async fn kv_delete(&self, _plugin: &str, req: KvDelete) -> SdkResult<KvDeleteOut> {
            let removed = self.kvs.lock().remove(&req.key);
            Ok(KvDeleteOut {
                deleted: i64::from(removed.is_some()),
                prev_kvs: Vec::new(),
                revision: self.next_revision(),
            })
        }

        async fn txn(&self, _plugin: &str, req: TxnReq) -> SdkResult<TxnOut> {
            let mut succeeded = true;
            {
                let kvs = self.kvs.lock();
                for c in &req.compares {
                    let version = kvs.get(&c.key).map(|(_, v)| *v).unwrap_or(0);
                    if c.target == CompareTarget::Version && version != c.int_value {
                        succeeded = false;
                        break;
                    }
                }
            }
            let revision = self.next_revision();
            let ops = if succeeded {
                &req.success
            } else {
                &req.failure
            };
            let mut responses = Vec::new();
            for op in ops {
                match op {
                    TxnOp::Put(p) => {
                        let mut kvs = self.kvs.lock();
                        match kvs.get_mut(&p.key) {
                            Some(e) => {
                                e.0 = p.value.clone();
                                e.1 += 1;
                            }
                            None => {
                                kvs.insert(p.key.clone(), (p.value.clone(), 1));
                            }
                        }
                        responses.push(TxnOpOut::Put(crate::plugin::sdk::KvPutOut {
                            prev_kv: None,
                            revision,
                        }));
                    }
                    TxnOp::Delete(d) => {
                        let removed = self.kvs.lock().remove(&d.key);
                        responses.push(TxnOpOut::Delete(KvDeleteOut {
                            deleted: i64::from(removed.is_some()),
                            prev_kvs: Vec::new(),
                            revision,
                        }));
                    }
                    TxnOp::Range(_) => {
                        responses.push(TxnOpOut::Range(KvRangeOut {
                            kvs: Vec::new(),
                            count: 0,
                            revision,
                        }));
                    }
                }
            }
            Ok(TxnOut {
                succeeded,
                revision,
                responses,
            })
        }

        async fn lease_grant(&self, _plugin: &str, _ttl: i64, _id: i64) -> SdkResult<i64> {
            let mut seq = self.lease_seq.lock();
            *seq += 1;
            Ok(*seq)
        }

        async fn lease_revoke(&self, _plugin: &str, _id: i64) -> SdkResult<()> {
            Ok(())
        }

        async fn lease_keep_alive(&self, _plugin: &str, _id: i64) -> SdkResult<()> {
            Ok(())
        }

        async fn lease_stop_keep_alive(&self, _plugin: &str, _id: i64) -> SdkResult<()> {
            Ok(())
        }

        async fn watch_subscribe(&self, _plugin: &str, _req: WatchSubscribe) -> SdkResult<u64> {
            Ok(1)
        }

        async fn watch_next(&self, _plugin: &str, _id: u64) -> SdkResult<Option<WatchEventDto>> {
            Ok(None)
        }

        async fn watch_close(&self, _plugin: &str, _id: u64) -> SdkResult<()> {
            Ok(())
        }

        async fn storage_put(
            &self,
            _plugin: &str,
            _bucket: &str,
            _object_id: &[u8],
            data: &[u8],
        ) -> SdkResult<ObjectPutOut> {
            Ok(ObjectPutOut {
                revision: 1,
                size: data.len() as i64,
                chunks: 1,
            })
        }

        async fn storage_get(
            &self,
            _plugin: &str,
            bucket: &str,
            object_id: &[u8],
        ) -> SdkResult<ObjectGetOut> {
            Ok(ObjectGetOut {
                stat: ObjectStatDto {
                    bucket: bucket.to_string(),
                    object_id: object_id.to_vec(),
                    size: 0,
                    chunks: 0,
                    revision: 1,
                    exists: true,
                    committed: true,
                },
                data: Vec::new(),
            })
        }

        async fn storage_stat(
            &self,
            _plugin: &str,
            _bucket: &str,
            _object_id: &[u8],
        ) -> SdkResult<Option<ObjectStatDto>> {
            Ok(None)
        }

        async fn storage_delete(
            &self,
            _plugin: &str,
            _bucket: &str,
            _object_id: &[u8],
        ) -> SdkResult<bool> {
            Ok(true)
        }
    }

    // ──── WAT 夹具 ────

    /// 回显：应答指向 payload 区（不拷贝）。
    const WAT_ECHO: &str = r#"
(module
  (memory (export "memory") 4)
  (global $next (mut i32) (i32.const 4096))
  (func (export "alloc") (param $len i32) (result i32)
    (local $p i32)
    (local.set $p (global.get $next))
    (global.set $next (i32.add (global.get $next) (local.get $len)))
    (local.get $p))
  (func (export "handle_invoke") (param $mp i32) (param $ml i32) (param $pp i32) (param $pl i32) (result i64)
    (i64.or
      (i64.shl (i64.extend_i32_u (local.get $pp)) (i64.const 32))
      (i64.extend_i32_u (local.get $pl)))))
"#;

    /// KV：把 payload 当作 key，写入常量 value，返回（宿主写入的）revision。
    /// 错误码原样透传（`i64.extend_i32_u` → 高 32 位为 0、低 32 位为负）。
    const WAT_KV: &str = r#"
(module
  (import "coord" "kv_put" (func $kv_put (param i32 i32 i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 4)
  (global $next (mut i32) (i32.const 4096))
  (data (i32.const 1024) "v")
  (func (export "alloc") (param $len i32) (result i32)
    (local $p i32)
    (local.set $p (global.get $next))
    (global.set $next (i32.add (global.get $next) (local.get $len)))
    (local.get $p))
  (func (export "handle_invoke") (param $mp i32) (param $ml i32) (param $pp i32) (param $pl i32) (result i64)
    (local $status i32)
    (local.set $status
      (call $kv_put
        (local.get $pp) (local.get $pl)
        (i32.const 1024) (i32.const 1)
        (i32.const 2048) (i32.const 8)))
    (if (i32.lt_s (local.get $status) (i32.const 0))
      (then (return (i64.extend_i32_u (local.get $status)))))
    (i64.or
      (i64.shl (i64.extend_i32_u (i32.const 2048)) (i64.const 32))
      (i64.const 8))))
"#;

    /// 忙等：不消耗可观测状态，直到 fuel/epoch 触发 trap。
    const WAT_SPIN: &str = r#"
(module
  (memory (export "memory") 1)
  (func (export "alloc") (param i32) (result i32) (i32.const 1024))
  (func (export "handle_invoke") (param i32 i32 i32 i32) (result i64)
    (loop $l (br $l))
    (i64.const 0)))
"#;

    /// 内存增长：尝试增长到 64 页；被 StoreLimits 挡住则返回 "0"，否则 "1"。
    const WAT_MEMHOG: &str = r#"
(module
  (memory (export "memory") 4)
  (func (export "alloc") (param i32) (result i32) (i32.const 1024))
  (func (export "handle_invoke") (param i32 i32 i32 i32) (result i64)
    (local $ok i32)
    (local.set $ok (i32.const 1))
    (block $done
      (loop $grow
        (br_if $done (i32.ge_u (memory.size) (i32.const 64)))
        (if (i32.eq (memory.grow (i32.const 1)) (i32.const -1))
          (then
            (local.set $ok (i32.const 0))
            (br $done)))
        (br $grow)))
    (i32.store8 (i32.const 2048) (local.get $ok))
    (i64.or
      (i64.shl (i64.extend_i32_u (i32.const 2048)) (i64.const 32))
      (i64.const 1))))
"#;

    /// 缺 `alloc`：加载期必须拒绝（ABI 契约）。
    const WAT_NO_ALLOC: &str = r#"
(module
  (memory (export "memory") 1)
  (func (export "handle_invoke") (param i32 i32 i32 i32) (result i64) (i64.const 0)))
"#;

    // ──── 夹具装配 ────

    struct Harness {
        dir: tempfile::TempDir,
        backend: Arc<StubBackend>,
    }

    impl Harness {
        fn new(source: &str) -> Self {
            Self::with_files(&[("index.wasm", source)])
        }

        fn with_files(files: &[(&str, &str)]) -> Self {
            let dir = tempfile::tempdir().expect("tempdir");
            for (name, source) in files {
                std::fs::write(dir.path().join(name), source).expect("write fixture");
            }
            Self {
                dir,
                backend: Arc::new(StubBackend::default()),
            }
        }

        fn loader(&self) -> WasmPluginLoader {
            WasmPluginLoader::new(
                self.dir.path(),
                Arc::clone(&self.backend) as Arc<dyn PluginSdkBackend>,
                BTreeMap::new(),
                tokio::runtime::Handle::current(),
            )
            .expect("loader")
        }

        async fn load_named(&self, name: &str, limits: PluginLimits) -> Arc<dyn Plugin> {
            let m = manifest(name, limits);
            self.loader().load(&m).await.expect("load")
        }

        /// 指定入口文件的 manifest（同目录多夹具用）。
        fn manifest_with_entry(name: &str, entry: &str, limits: PluginLimits) -> PluginManifest {
            PluginManifest {
                entry: entry.into(),
                ..manifest(name, limits)
            }
        }
    }

    fn manifest(name: &str, limits: PluginLimits) -> PluginManifest {
        PluginManifest {
            name: name.into(),
            version: "1.0.0".into(),
            runtime: PluginRuntime::Wasm,
            trust: PluginTrust::ThirdParty,
            entry: "index.wasm".into(),
            capabilities: vec![
                PluginCapability {
                    id: CAP_KV_READ.into(),
                    scope: "/app/kv/".into(),
                },
                PluginCapability {
                    id: CAP_KV_WRITE.into(),
                    scope: "/app/kv/".into(),
                },
                PluginCapability {
                    id: CAP_KV_DELETE.into(),
                    scope: "/app/kv/".into(),
                },
                PluginCapability {
                    id: CAP_TXN_EXECUTE.into(),
                    scope: String::new(),
                },
                PluginCapability {
                    id: CAP_LEASE_GRANT.into(),
                    scope: String::new(),
                },
                PluginCapability {
                    id: CAP_LEASE_REVOKE.into(),
                    scope: String::new(),
                },
            ],
            limits,
            hooks: false,
            source: PluginSource::default(),
        }
    }

    fn fast_limits() -> PluginLimits {
        PluginLimits {
            max_memory_mb: 4,
            max_exec_ms: 2_000,
            max_objects: 16,
        }
    }

    // ──── 测试 ────

    /// ABI 基线：payload 往返 + 状态机（Loaded → Started → Stopped）。
    #[tokio::test(flavor = "multi_thread")]
    async fn wasm_echo_round_trips_payload() {
        let h = Harness::new(WAT_ECHO);
        let p = h.load_named("echo", fast_limits()).await;
        assert_eq!(p.status(), PluginStatus::Loaded);
        p.init().await.expect("init");
        p.start().await.expect("start");
        assert_eq!(p.status(), PluginStatus::Started);
        assert_eq!(
            p.invoke("any", b"hello wasm").await.expect("invoke"),
            b"hello wasm"
        );
        // 空应答
        assert_eq!(p.invoke("any", b"").await.expect("invoke"), b"");
        p.stop().await.expect("stop");
        assert_eq!(p.status(), PluginStatus::Stopped);
    }

    /// 宿主 import 边界：`coord.kv_put` 经门面（作用域守卫）落到后端；
    /// 越界 key 在 agent 侧被拒（不触达后端）。
    #[tokio::test(flavor = "multi_thread")]
    async fn wasm_kv_put_goes_through_scope_guard() {
        let h = Harness::new(WAT_KV);
        let p = h.load_named("kv", fast_limits()).await;
        p.init().await.expect("init");
        p.start().await.expect("start");

        let out = p.invoke("put", b"/app/kv/a").await.expect("invoke");
        assert_eq!(out.len(), 8, "revision must be an 8-byte LE integer");
        let revision = i64::from_le_bytes(out.try_into().expect("8 bytes"));
        assert!(revision > 0);
        assert_eq!(
            h.backend.puts.lock().as_slice(),
            [(b"/app/kv/a".to_vec(), b"v".to_vec())]
        );

        // 越界：错误码经 handle_invoke 透传 → 宿主映射回 ErrForbidden
        let err = p.invoke("put", b"/outside/x").await.unwrap_err();
        assert!(
            format!("{err}").contains("ErrForbidden"),
            "unexpected error: {err}"
        );
        assert_eq!(h.backend.puts.lock().len(), 1, "越界写不得触达后端");

        p.stop().await.expect("stop");
    }

    /// 沙箱：无限循环被 fuel/epoch 打断 → 该插件失败并被丢弃，
    /// 但进程与**其它插件**（同一引擎的新 store）不受影响（崩溃隔离）。
    #[tokio::test(flavor = "multi_thread")]
    async fn wasm_infinite_loop_is_interrupted_and_isolated() {
        let h = Harness::with_files(&[("index.wasm", WAT_SPIN), ("echo.wasm", WAT_ECHO)]);
        let loader = h.loader();

        let mut limits = fast_limits();
        limits.max_exec_ms = 200;
        let spin = loader.load(&manifest("spin", limits)).await.expect("load");
        spin.init().await.expect("init");
        spin.start().await.expect("start");

        let err = spin.invoke("go", b"").await.unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("trap") || msg.contains("epoch") || msg.contains("fuel"),
            "unexpected error: {msg}"
        );
        assert!(matches!(spin.status(), PluginStatus::Failed(_)));

        // 同引擎的另一个插件照常工作（trap 不污染引擎/进程）
        let echo = loader
            .load(&Harness::manifest_with_entry(
                "echo",
                "echo.wasm",
                fast_limits(),
            ))
            .await
            .expect("load echo");
        echo.init().await.expect("init");
        echo.start().await.expect("start");
        assert_eq!(
            echo.invoke("m", b"still-alive").await.expect("echo"),
            b"still-alive"
        );
        echo.stop().await.expect("stop");
    }

    /// 沙箱：内存上限必须由 `StoreLimits` 强制（同一夹具在不同上限下行为不同）。
    #[tokio::test(flavor = "multi_thread")]
    async fn wasm_memory_limit_is_enforced() {
        let h = Harness::new(WAT_MEMHOG);

        // 1MiB 上限 → 增长到 16 页即失败 → 夹具返回 "0"
        let tight = h
            .load_named(
                "hog-tight",
                PluginLimits {
                    max_memory_mb: 1,
                    ..fast_limits()
                },
            )
            .await;
        tight.init().await.expect("init");
        tight.start().await.expect("start");
        assert_eq!(tight.invoke("grow", b"").await.expect("invoke"), vec![0u8]);
        // 未 trap：插件仍可服务
        assert!(matches!(tight.status(), PluginStatus::Started));
        assert_eq!(tight.invoke("grow", b"").await.expect("invoke"), vec![0u8]);
        tight.stop().await.expect("stop");

        // 8MiB 上限 → 同一夹具可增长到 64 页 → 返回 "1"
        let loose = h
            .load_named(
                "hog-loose",
                PluginLimits {
                    max_memory_mb: 8,
                    ..fast_limits()
                },
            )
            .await;
        loose.init().await.expect("init");
        loose.start().await.expect("start");
        assert_eq!(loose.invoke("grow", b"").await.expect("invoke"), vec![1u8]);
        loose.stop().await.expect("stop");
    }

    /// ABI 契约：缺 `alloc` 导出 → 初始化失败（不进入服务状态）。
    #[tokio::test(flavor = "multi_thread")]
    async fn wasm_module_without_alloc_is_rejected() {
        let h = Harness::new(WAT_NO_ALLOC);
        let p = h.load_named("bad", fast_limits()).await;
        let err = p.init().await.unwrap_err();
        assert!(
            format!("{err}").contains("initialisation failed"),
            "unexpected error: {err}"
        );
        assert!(matches!(p.status(), PluginStatus::Failed(_)));
    }

    /// 加载器按 runtime 拒绝：JS manifest 不能交给 wasm 引擎。
    #[tokio::test(flavor = "multi_thread")]
    async fn wasm_loader_rejects_js_runtime() {
        let h = Harness::new(WAT_ECHO);
        let mut m = manifest("mismatch", fast_limits());
        m.runtime = PluginRuntime::Js;
        assert!(h.loader().load(&m).await.is_err());
    }

    /// 入口逃逸插件目录 → 加载期拒绝。
    #[tokio::test(flavor = "multi_thread")]
    async fn wasm_entry_outside_plugin_dir_is_rejected() {
        let h = Harness::new(WAT_ECHO);
        let mut m = manifest("escapee", fast_limits());
        m.entry = "../evil.wasm".into();
        assert!(h.loader().load(&m).await.is_err());
    }

    /// D6/D8：调用面钩子对 wasm 插件的写请求同样可见、可拒（同一门面）。
    #[tokio::test(flavor = "multi_thread")]
    async fn wasm_call_face_hook_can_deny_write() {
        use crate::plugin::hooks::{CallCtx, CallDecision, CallHook, CallOp, HookRegistry};

        struct DenyWrites {
            seen: Arc<TestMutex<Vec<String>>>,
        }

        impl CallHook for DenyWrites {
            fn name(&self) -> &str {
                "deny-writes"
            }
            fn before(&self, ctx: &CallCtx) -> CallDecision {
                self.seen.lock().push(ctx.op.as_str().to_string());
                if ctx.op == CallOp::KvPut {
                    CallDecision::Deny("writes frozen by operator".into())
                } else {
                    CallDecision::Allow
                }
            }
        }

        let h = Harness::new(WAT_KV);
        let seen = Arc::new(TestMutex::new(Vec::new()));
        let registry = Arc::new(HookRegistry::new());
        registry.register(
            "*",
            Arc::new(DenyWrites {
                seen: Arc::clone(&seen),
            }),
        );

        let p = h
            .loader()
            .with_hooks(Arc::clone(&registry))
            .load(&manifest("kv", fast_limits()))
            .await
            .expect("load");
        p.init().await.expect("init");
        p.start().await.expect("start");

        let err = p.invoke("put", b"/app/kv/a").await.unwrap_err();
        assert!(
            format!("{err}").contains("ErrForbidden"),
            "unexpected error: {err}"
        );
        assert!(h.backend.puts.lock().is_empty(), "拒绝后不得触达后端");
        assert_eq!(seen.lock().as_slice(), ["kv_put"]);
        assert_eq!(registry.stats().denied_total, 1);
    }

    /// 离线 fallback 客户端（不拨号；身份管理器只需构造 Client）。
    async fn offline_client() -> coord_client::client::Client {
        let config = coord_client::Config::new(vec!["http://127.0.0.1:1".to_string()]);
        coord_client::client::Client::connect_direct(config)
            .await
            .expect("client construction should not dial")
    }

    /// Phase 4.x：wasm 插件身份与 JS 引擎一致——加载期开通受限 CCT 账户
    /// （用户 + 角色 + 逐能力 grant），停止时注销。
    #[tokio::test(flavor = "multi_thread")]
    async fn wasm_plugin_identity_is_provisioned_and_released() {
        use crate::plugin::identity::{
            IssuedToken, PluginAuthGateway, PluginClients, PluginIdentityManager,
        };

        #[derive(Default)]
        struct StubGateway {
            grants: TestMutex<Vec<(String, String, String)>>,
        }

        #[async_trait]
        impl PluginAuthGateway for StubGateway {
            async fn user_add(&self, _user: &str, _password: &str) -> Result<(), String> {
                Ok(())
            }
            async fn role_add(&self, _role: &str) -> Result<(), String> {
                Ok(())
            }
            async fn role_grant_capability(
                &self,
                role: &str,
                capability_id: &str,
                scope: &str,
            ) -> Result<(), String> {
                self.grants.lock().push((
                    role.to_string(),
                    capability_id.to_string(),
                    scope.to_string(),
                ));
                Ok(())
            }
            async fn user_grant_role(&self, _user: &str, _role: &str) -> Result<(), String> {
                Ok(())
            }
            async fn authenticate(
                &self,
                _user: &str,
                _password: &str,
            ) -> Result<IssuedToken, String> {
                Ok(IssuedToken {
                    cct: "cct-1".into(),
                    refresh_token: Some("rt-1".into()),
                    expires_at: crate::plugin::identity::now_secs() + 900,
                })
            }
            async fn refresh(&self, _refresh_token: &str) -> Result<IssuedToken, String> {
                Ok(IssuedToken {
                    cct: "cct-2".into(),
                    refresh_token: Some("rt-2".into()),
                    expires_at: crate::plugin::identity::now_secs() + 900,
                })
            }
        }

        let h = Harness::new(WAT_ECHO);
        let gateway = Arc::new(StubGateway::default());
        let clients = Arc::new(PluginClients::new(offline_client().await));
        let mgr = Arc::new(
            PluginIdentityManager::new(
                Arc::clone(&gateway) as Arc<dyn PluginAuthGateway>,
                vec!["http://127.0.0.1:1".to_string()],
                None,
                h.dir.path(),
                Arc::clone(&clients),
            )
            .expect("identity manager"),
        );

        let m = manifest("echo", fast_limits());
        let p = h
            .loader()
            .with_identity(Arc::clone(&mgr))
            .load(&m)
            .await
            .expect("load");

        assert_eq!(clients.len(), 1, "wasm plugin must get a per-plugin client");
        assert_eq!(
            gateway.grants.lock().len(),
            m.capabilities.len(),
            "every declared capability must be granted"
        );

        p.init().await.expect("init");
        p.start().await.expect("start");
        p.stop().await.expect("stop");
        assert_eq!(clients.len(), 0, "stop must release the plugin identity");
    }

    /// Phase 5：wasm sandbox trap（fuel/epoch）与调用结果进入 AgentMetrics。
    #[tokio::test(flavor = "multi_thread")]
    async fn wasm_trap_is_counted_in_metrics() {
        let h = Harness::new(WAT_SPIN);
        let metrics = crate::metrics::AgentMetrics::new();
        let mut limits = fast_limits();
        limits.max_exec_ms = 200;

        let p = h
            .loader()
            .with_metrics(metrics.clone())
            .load(&manifest("spin", limits))
            .await
            .expect("load");
        p.init().await.expect("init");
        p.start().await.expect("start");
        let _ = p.invoke("go", b"").await.unwrap_err();

        let text = metrics.render_prometheus_text();
        assert!(
            text.contains(
                "coord_agent_plugin_invocations_total{plugin=\"spin\",engine=\"wasm\",outcome=\"error\"} 1"
            ),
            "invocation must be counted: {text}"
        );
        assert!(
            text.contains("coord_agent_plugin_traps_total{plugin=\"spin\",reason=\"fuel\"} 1")
                || text
                    .contains("coord_agent_plugin_traps_total{plugin=\"spin\",reason=\"epoch\"} 1"),
            "fuel/epoch trap must be counted: {text}"
        );
    }
}
