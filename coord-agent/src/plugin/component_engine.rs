// coord-agent: wasm 插件宿主 —— **组件模型** 路径（计划 §6 D3 / §11 Phase 4.1）
//
// 与 [`super::wasm_engine`]（core wasm 手写 ABI，默认路径）并列：
// 同一个 `runtime = "wasm"` manifest，按模块二进制**自动判别 ABI**
// —— 组件（layer 1，`\0asm\x0d\0\x01\0`）= 本文件；core module = core ABI。
// 判定见 [`is_component_binary`]；两条路径共用同一个 `PluginSdk` 门面、
// 同一套沙箱手段、同一份指标口径（D6/D8 不分 ABI）。
//
// ──── 组件模型相对 core ABI 的收益（4.1 spike 的结论）────
// 1. **类型安全**：宿主 import 与 guest 导出由 WIT（`coord-agent/wit/coord-plugin.wit`）
//    描述，两侧绑定由生成器产出（宿主 `wasmtime::component::bindgen!`，guest
//    `wit-bindgen`）；手写 ABI 的指针/长度/打包 i64 约定整体消失；
// 2. **异步宿主 import**：`bindgen!({ imports: { default: async } })` 让每个宿主
//    函数成为 `async fn`，wasmtime 用 **fiber** 在 await 点挂起 guest 栈
//    （`LinkerInstance::func_wrap_async`）→ 宿主函数直接在 tokio 上 await
//    coord-client，**不需要** core ABI 路径里 `Handle::block_on` 的阻塞桥，
//    也不需要为「不阻塞 agent 工作线程」而依赖 `spawn_blocking`；
// 3. **流式能力有类型表达**：watch 订阅是 WIT `resource subscription`
//    句柄 + 阻塞式 `next()`（guest 视角同步，宿主侧 fiber 挂起）；
// 4. **无 WASI**：world 不 import 任何 `wasi:*`，能力面 = `coord:plugin/host`
//    一个接口（与 core ABI 的「module 名白名单」等价，但由工具链强制）。
//
// ──── 执行模型 ────
// 每个组件插件 = 专属线程 + 专属 current-thread tokio 运行时 + 专属
// `Store`（状态跨调用保留）。命令经 `tokio::sync::mpsc` 投递，线程内
// `rt.block_on(async { recv → invoke })`，因此**宿主侧后台任务**
// （如 lease 保活）在空闲期仍能推进。
// 沙箱：`consume_fuel`（按 `max_exec_ms` 派生指令预算）+ `epoch_interruption`
// （全局滴答线程，墙钟超时）+ `StoreLimits`（内存上限）+ 无 WASI +
// 能力边界在宿主 import 边界（统一走 `PluginSdk`）。
// trap（fuel/epoch/越界/除零）→ 丢弃 store + 插件标记 Failed，agent 与其它
// 插件不受影响；外层另有 `max_exec_ms + 1s` 看门狗兜底。
//
// ──── ABI 判别与兼容 ────
// 组件 ABI 与 core ABI 是**并存**关系（不是替换）：core ABI 保留给
// 「无组件工具链」或需要极小产物的场景；两者对 manifest / 能力声明 / 指标
// 的语义完全一致，切换只影响 guest 侧构建方式。

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use wasmtime::component::{Component, Linker};
use wasmtime::{Config, Engine, Store, StoreLimits, StoreLimitsBuilder};

use super::wasm_engine::{classify_trap, EPOCH_TICK_MS, FUEL_PER_MS};
use super::{Plugin, PluginStatus};
use crate::plugin::hooks::HookRegistry;
use crate::plugin::identity::PluginIdentityManager;
use crate::plugin::manifest::{PluginLimits, PluginManifest};
use crate::plugin::sdk::backend::{
    Compare, CompareOp, CompareTarget, KvDelete, KvPut, KvRange, KvRecord, ObjectStatDto,
    SdkError as SdkErr, SdkErrorCode, TxnOp as SdkTxnOp, TxnOpOut, TxnReq, WatchEventDto,
    WatchEventKind, WatchSubscribe,
};
use crate::plugin::sdk::PluginSdk;
use crate::service::ServiceResult;

// ──── 生成的绑定（WIT → Rust）────
//
// `imports: { default: async }`：全部宿主 import 生成 **async 宿主函数**
// （wasmtime fiber：guest 侧仍是同步调用，宿主侧可以 await）；
// `exports: { default: async }`：guest 导出按 async 方式调用
// （`call_async`）—— 当宿主 import 是 async 时这是**必需**的，否则运行时
// 会在同步调用路径上直接报错。
mod bindings {
    wasmtime::component::bindgen!({
        path: "wit",
        world: "plugin",
        imports: { default: async },
        exports: { default: async },
        // watch 订阅句柄 = **宿主定义的 WIT resource**（RAII）。
        //
        // 只有把资源映射到宿主自己的 Rust 类型（`with` 的「item 投影」形式：
        // `"<interface>.<resource>"`），宿主才能**创建**句柄（`ResourceTable::push`
        // 需要具体类型）；不映射时 `bindgen!` 生成的资源标记类型是不可实例化的
        // 空 enum，宿主无法构造。
        //
        // 映射粒度是**单个资源**（不是整个 interface），因此本接口其余 DTO
        // （kv/txn/lease/storage/... record 与 variant）仍由 `bindgen!` 生成——
        // 不需要手写整套 `Host*` trait 与全部 record/variant 类型。
        //
        // 路径写 `super::WatchSubscription`：`bindgen!` 生成的 `pub use` 相对
        // 本模块（`mod bindings` 的父模块）解析。
        with: {
            "coord:plugin/host.subscription": super::WatchSubscription,
            "coord:plugin/host.uploader": super::StorageUploader,
            "coord:plugin/host.downloader": super::StorageDownloader,
        },
    });
}

use bindings::coord::plugin::host as wit;
use wasmtime::component::{Resource, ResourceTable};

/// 组件二进制魔数（layer 1 = component；core module 为 layer 0）。
const COMPONENT_LAYER: [u8; 8] = [0x00, 0x61, 0x73, 0x6d, 0x0d, 0x00, 0x01, 0x00];

/// 判别组件与 core module（不依赖错误字符串，只看二进制头）。
pub fn is_component_binary(bytes: &[u8]) -> bool {
    bytes.len() >= 8 && bytes[..8] == COMPONENT_LAYER
}

// ──── 共享引擎（组件模型 + async；编译一次 + 全局 epoch 滴答）────

struct SharedComponentEngine {
    engine: Engine,
    stop: AtomicBool,
}

impl Drop for SharedComponentEngine {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

impl SharedComponentEngine {
    fn new() -> Result<Arc<Self>, String> {
        let mut config = Config::new();
        // 沙箱手段与 core ABI 路径逐项一致（D8）
        config.consume_fuel(true);
        config.epoch_interruption(true);
        config.wasm_component_model(true);
        // 明确的负向开关：组件插件无系统接口（无 WASI / 无 GC）
        config.wasm_gc(false);
        let engine = Engine::new(&config).map_err(|e| format!("wasmtime engine init: {e}"))?;
        let shared = Arc::new(Self {
            engine,
            stop: AtomicBool::new(false),
        });

        // epoch 滴答线程（与 core 引擎同节奏；每个 Engine 独立计数）
        let weak = Arc::downgrade(&shared);
        std::thread::Builder::new()
            .name("coord-plugin-epoch-cm".into())
            .spawn(move || loop {
                let Some(shared) = weak.upgrade() else {
                    return;
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

// ──── store 数据 + 宿主 import 实现 ────

/// 组件 ABI 的 watch 订阅句柄（WIT `resource subscription`）。
///
/// 这是 `bindgen!` 的 `with` 映射目标：guest 侧句柄被 drop 时组件模型回调宿主
/// `drop`，我们在那里释放底层订阅 —— **不需要** guest 显式 `close`，
/// 也不会拖到插件停止才由 `PluginSdk::release()` 兜底回收。
pub struct WatchSubscription {
    /// 底层 SDK 订阅 id（`PluginSdk::watch_subscribe` 分配）。
    id: u64,
    /// 是否已被显式 `close()`（`drop` 时避免重复释放）。
    closed: bool,
}

/// 组件 ABI 的对象**上传会话**句柄（WIT `resource uploader`）。
///
/// 与 [`WatchSubscription`] 同一 RAII 模式：guest 侧句柄 drop → 宿主 `drop`
/// 中止未提交的上传（等价 `abort`），不会拖到插件停止才回收。
pub struct StorageUploader {
    /// 底层 SDK 上传会话 id（`PluginSdk::storage_open_write` 分配）。
    id: u64,
    /// 打开时声明的对象坐标（`commit` 构造 `object-stat` 需要）。
    bucket: String,
    object_id: Vec<u8>,
    /// 是否已 `commit` / `abort`（`drop` 时避免重复释放）。
    finished: bool,
}

/// 组件 ABI 的对象**下载会话**句柄（WIT `resource downloader`）。
pub struct StorageDownloader {
    /// 底层 SDK 下载会话 id（`PluginSdk::storage_open_read` 分配）。
    id: u64,
    /// 是否已被显式 `close()`（`drop` 时避免重复释放）。
    closed: bool,
}

/// 组件插件 store 的宿主数据 —— 同时是 WIT `host` 接口的实现者。
struct ComponentHostState {
    sdk: Arc<PluginSdk>,
    env: BTreeMap<String, String>,
    limits: StoreLimits,
    /// 宿主资源句柄表（当前只有 watch 订阅；容量 = `max_objects`）。
    ///
    /// 用 `Arc<Mutex<..>>` 而非直接字段：宿主 import 是 async 且实现必须返回
    /// **不借用 `&mut self`** 的 future（wasmtime fiber 桥要求），而
    /// `watch_subscribe` 需要**在 await 之后**才拿得到底层 id、才有东西可登记。
    /// 共享句柄表是「await 之后仍能登记资源」的最简形式；表只在插件自己的
    /// store 内使用（单线程），无跨线程争用。
    table: Arc<Mutex<ResourceTable>>,
    max_exec_ms: u64,
    max_memory_mb: u64,
    max_objects: u64,
}

impl ComponentHostState {
    fn new(sdk: Arc<PluginSdk>, env: BTreeMap<String, String>, limits: &PluginLimits) -> Self {
        let bytes = (limits.max_memory_mb as usize).saturating_mul(1024 * 1024);
        // 注意：组件实例化会展开成**多个** core instance（guest + 适配层），
        // `instances` 不能像 core ABI 路径那样取 1；此处按上限给足但仍有界。
        let store_limits = StoreLimitsBuilder::new()
            .memory_size(bytes)
            .instances(8)
            .tables(8)
            .memories(4)
            .build();
        // 句柄表容量 = `max_objects`（「单插件可并发的协调调用句柄上限」）：
        // 句柄泄漏/滥用会以 `resource-exhausted` 提前失败，而不是无界增长。
        let mut table = ResourceTable::new();
        table.set_max_capacity(limits.max_objects as usize);
        Self {
            sdk,
            env,
            limits: store_limits,
            table: Arc::new(Mutex::new(table)),
            max_exec_ms: limits.max_exec_ms,
            max_memory_mb: limits.max_memory_mb as u64,
            max_objects: limits.max_objects as u64,
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

/// 宿主 `sdk-error` ← SDK 错误（与 JS / core ABI 路径同名同语义）。
fn wit_err(e: &SdkErr) -> wit::SdkError {
    let msg = e.to_string();
    match e.code {
        SdkErrorCode::Forbidden => wit::SdkError::Forbidden(msg),
        SdkErrorCode::NotFound => wit::SdkError::NotFound(msg),
        SdkErrorCode::InvalidArgument => wit::SdkError::InvalidArgument(msg),
        SdkErrorCode::Unavailable => wit::SdkError::Unavailable(msg),
        SdkErrorCode::ResourceExhausted => wit::SdkError::ResourceExhausted(msg),
        SdkErrorCode::Conflict => wit::SdkError::Conflict(msg),
        SdkErrorCode::Internal => wit::SdkError::Internal(msg),
    }
}

fn wit_kv(rec: &KvRecord) -> wit::Kv {
    wit::Kv {
        key: rec.key.clone(),
        value: rec.value.clone(),
        lease_id: rec.lease_id,
        version: rec.version,
    }
}

fn wit_put_out(out: &crate::plugin::sdk::KvPutOut) -> wit::KvPutOut {
    wit::KvPutOut {
        revision: out.revision,
        prev: out.prev_kv.as_ref().map(wit_kv),
    }
}

fn wit_range_out(out: &crate::plugin::sdk::KvRangeOut) -> wit::KvRangeOut {
    wit::KvRangeOut {
        kvs: out.kvs.iter().map(wit_kv).collect(),
        count: out.count,
        revision: out.revision,
    }
}

fn wit_delete_out(out: &crate::plugin::sdk::KvDeleteOut) -> wit::KvDeleteOut {
    wit::KvDeleteOut {
        deleted: out.deleted,
        revision: out.revision,
    }
}

fn wit_txn_op_out(out: &TxnOpOut) -> wit::TxnOpOut {
    match out {
        TxnOpOut::Put(p) => wit::TxnOpOut::Put(wit_put_out(p)),
        TxnOpOut::Range(r) => wit::TxnOpOut::Range(wit_range_out(r)),
        TxnOpOut::Delete(d) => wit::TxnOpOut::Delete(wit_delete_out(d)),
    }
}

fn wit_object_stat(stat: &ObjectStatDto) -> wit::ObjectStat {
    wit::ObjectStat {
        bucket: stat.bucket.clone(),
        object_id: stat.object_id.clone(),
        size: stat.size,
        chunks: stat.chunks,
        revision: stat.revision,
        exists: stat.exists,
        committed: stat.committed,
    }
}

fn wit_watch_event(ev: &WatchEventDto) -> wit::WatchEvent {
    wit::WatchEvent {
        kind: match ev.kind {
            WatchEventKind::Put => wit::WatchEventKind::Put,
            WatchEventKind::Delete => wit::WatchEventKind::Delete,
            WatchEventKind::BufferOverflow => wit::WatchEventKind::BufferOverflow,
            WatchEventKind::HistoryUnavailable => wit::WatchEventKind::HistoryUnavailable,
        },
        kvs: ev.kvs.iter().map(wit_kv).collect(),
        prev: ev.prev_kv.as_ref().map(wit_kv),
        revision: ev.revision,
    }
}

/// 组件 ABI 的宿主 import 实现（全部 async：内层 await coord-client）。
///
/// 注意实现形态：方法体只 `clone()` 出 `Arc<PluginSdk>` 与标量，随后
/// `async move { ... }`，**不把 `&mut self` 带进 future** —— 这是
/// wasmtime fiber 桥能安全挂起 guest 栈的前提。
impl wit::Host for ComponentHostState {
    fn kv_put(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        lease_id: i64,
        prev_kv: bool,
        request_id: Option<String>,
    ) -> impl std::future::Future<Output = Result<wit::KvPutOut, wit::SdkError>> + Send {
        let sdk = Arc::clone(&self.sdk);
        async move {
            sdk.kv_put(KvPut {
                key,
                value,
                lease_id,
                prev_kv,
                request_id: request_id.map(String::into_bytes).unwrap_or_default(),
            })
            .await
            .map(|out| wit_put_out(&out))
            .map_err(|e| wit_err(&e))
        }
    }

    fn kv_range(
        &mut self,
        key: Vec<u8>,
        range_end: Vec<u8>,
        limit: i64,
        revision: i64,
        keys_only: bool,
        count_only: bool,
    ) -> impl std::future::Future<Output = Result<wit::KvRangeOut, wit::SdkError>> + Send {
        let sdk = Arc::clone(&self.sdk);
        async move {
            sdk.kv_range(KvRange {
                key,
                range_end,
                limit,
                revision,
                keys_only,
                count_only,
            })
            .await
            .map(|out| wit_range_out(&out))
            .map_err(|e| wit_err(&e))
        }
    }

    fn kv_delete(
        &mut self,
        key: Vec<u8>,
        range_end: Vec<u8>,
        prev_kv: bool,
    ) -> impl std::future::Future<Output = Result<wit::KvDeleteOut, wit::SdkError>> + Send {
        let sdk = Arc::clone(&self.sdk);
        async move {
            sdk.kv_delete(KvDelete {
                key,
                range_end,
                prev_kv,
                request_id: Vec::new(),
            })
            .await
            .map(|out| wit_delete_out(&out))
            .map_err(|e| wit_err(&e))
        }
    }

    fn kv_get(
        &mut self,
        key: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, wit::SdkError>> + Send {
        let sdk = Arc::clone(&self.sdk);
        async move { sdk.kv_get(key).await.map_err(|e| wit_err(&e)) }
    }

    fn kv_create(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        lease_id: i64,
    ) -> impl std::future::Future<Output = Result<i64, wit::SdkError>> + Send {
        // 三条 ABI 路径共用 `PluginSdk::kv_create`（compare version == 0 的 CAS），
        // 冲突语义（`sdk-error.conflict`）也由门面统一给出。
        let sdk = Arc::clone(&self.sdk);
        async move {
            sdk.kv_create(key, value, lease_id)
                .await
                .map_err(|e| wit_err(&e))
        }
    }

    fn txn(
        &mut self,
        request: wit::TxnRequest,
    ) -> impl std::future::Future<Output = Result<wit::TxnResponse, wit::SdkError>> + Send {
        let sdk = Arc::clone(&self.sdk);
        async move {
            let req = TxnReq {
                compares: request
                    .compares
                    .iter()
                    .map(|c| Compare {
                        op: op_from_wit(c.op),
                        target: target_from_wit(c.target),
                        key: c.key.clone(),
                        int_value: c.int_value,
                        bytes_value: c.bytes_value.clone(),
                    })
                    .collect(),
                success: request.success.iter().map(txn_op_from_wit).collect(),
                failure: request.failure.iter().map(txn_op_from_wit).collect(),
                request_id: request
                    .request_id
                    .map(String::into_bytes)
                    .unwrap_or_default(),
            };
            sdk.txn(req)
                .await
                .map(|out| wit::TxnResponse {
                    succeeded: out.succeeded,
                    revision: out.revision,
                    responses: out.responses.iter().map(wit_txn_op_out).collect(),
                })
                .map_err(|e| wit_err(&e))
        }
    }

    fn lease_grant(
        &mut self,
        ttl_secs: i64,
    ) -> impl std::future::Future<Output = Result<wit::Lease, wit::SdkError>> + Send {
        let sdk = Arc::clone(&self.sdk);
        async move {
            sdk.lease_grant(ttl_secs, 0)
                .await
                .map(|id| wit::Lease { id, ttl: ttl_secs })
                .map_err(|e| wit_err(&e))
        }
    }

    fn lease_revoke(
        &mut self,
        id: i64,
    ) -> impl std::future::Future<Output = Result<(), wit::SdkError>> + Send {
        let sdk = Arc::clone(&self.sdk);
        async move { sdk.lease_revoke(id).await.map_err(|e| wit_err(&e)) }
    }

    fn lease_keep_alive(
        &mut self,
        id: i64,
    ) -> impl std::future::Future<Output = Result<(), wit::SdkError>> + Send {
        let sdk = Arc::clone(&self.sdk);
        async move { sdk.lease_keep_alive(id).await.map_err(|e| wit_err(&e)) }
    }

    fn watch_subscribe(
        &mut self,
        key: Vec<u8>,
        range_end: Vec<u8>,
        start_revision: i64,
        prev_kv: bool,
    ) -> impl std::future::Future<Output = Result<Resource<WatchSubscription>, wit::SdkError>> + Send
    {
        let sdk = Arc::clone(&self.sdk);
        let table = Arc::clone(&self.table);
        async move {
            let id = sdk
                .watch_subscribe(WatchSubscribe {
                    key,
                    range_end,
                    start_revision,
                    prev_kv,
                })
                .await
                .map_err(|e| wit_err(&e))?;
            // 底层订阅已建立 → 登记句柄（容量 = `max_objects`）。
            table
                .lock()
                .push(WatchSubscription { id, closed: false })
                .map_err(|e| {
                    wit::SdkError::ResourceExhausted(format!(
                        "watch handle table is full (max_objects): {e}"
                    ))
                })
        }
    }

    fn storage_put(
        &mut self,
        bucket: String,
        object_id: Vec<u8>,
        data: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<wit::ObjectStat, wit::SdkError>> + Send {
        let sdk = Arc::clone(&self.sdk);
        async move {
            sdk.storage_put(&bucket, &object_id, &data)
                .await
                .map(|out| wit::ObjectStat {
                    bucket: bucket.clone(),
                    object_id: object_id.clone(),
                    size: out.size,
                    chunks: out.chunks,
                    revision: out.revision,
                    exists: true,
                    committed: true,
                })
                .map_err(|e| wit_err(&e))
        }
    }

    fn storage_get(
        &mut self,
        bucket: String,
        object_id: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, wit::SdkError>> + Send {
        let sdk = Arc::clone(&self.sdk);
        async move {
            sdk.storage_get(&bucket, &object_id)
                .await
                .map(|out| out.data)
                .map_err(|e| wit_err(&e))
        }
    }

    fn storage_stat(
        &mut self,
        bucket: String,
        object_id: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<Option<wit::ObjectStat>, wit::SdkError>> + Send
    {
        let sdk = Arc::clone(&self.sdk);
        async move {
            sdk.storage_stat(&bucket, &object_id)
                .await
                .map(|opt| {
                    opt.filter(|s| s.exists && s.committed)
                        .map(|s| wit_object_stat(&s))
                })
                .map_err(|e| wit_err(&e))
        }
    }

    fn storage_delete(
        &mut self,
        bucket: String,
        object_id: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<bool, wit::SdkError>> + Send {
        let sdk = Arc::clone(&self.sdk);
        async move {
            sdk.storage_delete(&bucket, &object_id)
                .await
                .map_err(|e| wit_err(&e))
        }
    }

    fn storage_open_write(
        &mut self,
        bucket: String,
        object_id: Vec<u8>,
        total_size: u64,
    ) -> impl std::future::Future<Output = Result<Resource<StorageUploader>, wit::SdkError>> + Send
    {
        let sdk = Arc::clone(&self.sdk);
        let table = Arc::clone(&self.table);
        async move {
            let id = sdk
                .storage_open_write(&bucket, &object_id, total_size)
                .await
                .map_err(|e| wit_err(&e))?;
            table
                .lock()
                .push(StorageUploader {
                    id,
                    bucket,
                    object_id,
                    finished: false,
                })
                .map_err(|e| {
                    wit::SdkError::ResourceExhausted(format!(
                        "storage handle table is full (max_objects): {e}"
                    ))
                })
        }
    }

    fn storage_open_read(
        &mut self,
        bucket: String,
        object_id: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<Resource<StorageDownloader>, wit::SdkError>> + Send
    {
        let sdk = Arc::clone(&self.sdk);
        let table = Arc::clone(&self.table);
        async move {
            let id = sdk
                .storage_open_read(&bucket, &object_id)
                .await
                .map_err(|e| wit_err(&e))?;
            table
                .lock()
                .push(StorageDownloader { id, closed: false })
                .map_err(|e| {
                    wit::SdkError::ResourceExhausted(format!(
                        "storage handle table is full (max_objects): {e}"
                    ))
                })
        }
    }

    fn env(
        &mut self,
        key: String,
    ) -> impl std::future::Future<Output = Result<String, wit::SdkError>> + Send {
        let value = self.env.get(&key).cloned();
        async move { value.ok_or_else(|| wit::SdkError::NotFound(format!("env '{key}' is not set"))) }
    }

    fn log(
        &mut self,
        level: String,
        message: String,
    ) -> impl std::future::Future<Output = ()> + Send {
        let plugin = self.sdk.plugin().to_string();
        log_line(&plugin, &level, &message);
        async move {}
    }

    fn limits(&mut self) -> impl std::future::Future<Output = wit::ResourceLimits> + Send {
        let limits = wit::ResourceLimits {
            max_exec_ms: self.max_exec_ms,
            max_memory_mb: self.max_memory_mb,
            max_objects: self.max_objects,
        };
        async move { limits }
    }
}

// ──── RAII 宿主资源：watch 订阅句柄 ────

/// `close()` 的动作（在同步段判定，异步段执行 —— 避免把 `&mut self` 带进 future）。
enum CloseAction {
    Release(u64),
    AlreadyClosed,
    Invalid(String),
}

/// WIT `resource subscription` 的宿主实现。
///
/// 三条路径都会释放底层订阅：`next`（读取）、`close`（显式提前释放，**幂等**）、
/// `drop`（guest 侧句柄生命周期结束时的兜底 —— 组件模型自动调用，
/// 因此忘记 `close` 不会泄漏）。
impl wit::HostSubscription for ComponentHostState {
    fn next(
        &mut self,
        self_: Resource<WatchSubscription>,
    ) -> impl std::future::Future<Output = Result<Option<wit::WatchEvent>, wit::SdkError>> + Send
    {
        let sdk = Arc::clone(&self.sdk);
        // 同步段取状态（句柄表只在插件自己的 store 内使用）；异步段不再借用 self。
        let state = self.table.lock().get(&self_).map(|s| (s.id, s.closed));
        async move {
            match state {
                Ok((id, false)) => sdk
                    .watch_next(id)
                    .await
                    .map(|opt| opt.as_ref().map(wit_watch_event))
                    .map_err(|e| wit_err(&e)),
                Ok((_, true)) => Err(wit::SdkError::InvalidArgument(
                    "watch subscription is already closed".to_string(),
                )),
                Err(e) => Err(wit::SdkError::InvalidArgument(format!(
                    "watch subscription handle is not valid: {e}"
                ))),
            }
        }
    }

    fn close(
        &mut self,
        self_: Resource<WatchSubscription>,
    ) -> impl std::future::Future<Output = Result<(), wit::SdkError>> + Send {
        let sdk = Arc::clone(&self.sdk);
        let action = {
            let mut table = self.table.lock();
            match table.get_mut(&self_) {
                Ok(sub) if !sub.closed => {
                    sub.closed = true;
                    CloseAction::Release(sub.id)
                }
                Ok(_) => CloseAction::AlreadyClosed,
                Err(e) => CloseAction::Invalid(e.to_string()),
            }
        };
        async move {
            match action {
                CloseAction::Release(id) => sdk.watch_close(id).await.map_err(|e| wit_err(&e)),
                CloseAction::AlreadyClosed => Ok(()),
                CloseAction::Invalid(msg) => Err(wit::SdkError::InvalidArgument(format!(
                    "watch subscription handle is not valid: {msg}"
                ))),
            }
        }
    }

    fn drop(
        &mut self,
        rep: Resource<WatchSubscription>,
    ) -> impl std::future::Future<Output = wasmtime::Result<()>> + Send {
        let sdk = Arc::clone(&self.sdk);
        let removed = self.table.lock().delete(rep);
        async move {
            match removed {
                // guest 没有显式 close 就 drop：这里立刻释放（RAII 的核心收益）
                Ok(sub) if !sub.closed => {
                    if let Err(e) = sdk.watch_close(sub.id).await {
                        // 句柄已从表中删除，无法重试；插件停止时
                        // `PluginSdk::release()` 仍会幂等回收。
                        tracing::warn!(
                            "watch subscription release failed on drop (id={}): {e}",
                            sub.id
                        );
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    // 重复 drop / 表已被清空：组件模型在实例销毁时可能再次回调
                    tracing::debug!("watch subscription handle already released: {e}");
                }
            }
            Ok(())
        }
    }
}

// ──── RAII 宿主资源：对象存储流式会话（批次 10）────

/// 对象存储流式会话的同步段动作（异步段不再借用 `&mut self`）。
enum IdAction {
    /// 执行（会话 id）
    Id(u64),
    /// 已提交 / 已中止 / 已关闭（幂等路径）
    Done,
    /// 句柄非法（表项缺失等）
    Invalid(String),
}

/// `commit` 的同步段动作（需要对象坐标来构造 `object-stat`）。
enum CommitAction {
    Go {
        id: u64,
        bucket: String,
        object_id: Vec<u8>,
    },
    Done,
    Invalid(String),
}

/// WIT `resource uploader` 的宿主实现（对象分块上传）。
///
/// 与 `subscription` 同一 RAII 语义：`commit` 结束会话；`abort` 幂等；
/// `drop` 兜底中止**未提交**的上传（guest 忘记 commit/abort 既不泄漏、
/// 也不留悬挂流）。
impl wit::HostUploader for ComponentHostState {
    fn write(
        &mut self,
        self_: Resource<StorageUploader>,
        chunk: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<u64, wit::SdkError>> + Send {
        let sdk = Arc::clone(&self.sdk);
        let action = {
            let mut table = self.table.lock();
            match table.get_mut(&self_) {
                Ok(up) if !up.finished => IdAction::Id(up.id),
                Ok(_) => IdAction::Done,
                Err(e) => IdAction::Invalid(e.to_string()),
            }
        };
        async move {
            match action {
                IdAction::Id(id) => sdk
                    .storage_write_chunk(id, &chunk)
                    .await
                    .map_err(|e| wit_err(&e)),
                IdAction::Done => Err(wit::SdkError::NotFound(
                    "upload session is already finished".to_string(),
                )),
                IdAction::Invalid(msg) => Err(wit::SdkError::InvalidArgument(format!(
                    "upload handle is not valid: {msg}"
                ))),
            }
        }
    }

    fn commit(
        &mut self,
        self_: Resource<StorageUploader>,
    ) -> impl std::future::Future<Output = Result<wit::ObjectStat, wit::SdkError>> + Send {
        let sdk = Arc::clone(&self.sdk);
        let action = {
            let mut table = self.table.lock();
            match table.get_mut(&self_) {
                Ok(up) if !up.finished => {
                    up.finished = true;
                    CommitAction::Go {
                        id: up.id,
                        bucket: up.bucket.clone(),
                        object_id: up.object_id.clone(),
                    }
                }
                Ok(_) => CommitAction::Done,
                Err(e) => CommitAction::Invalid(e.to_string()),
            }
        };
        async move {
            match action {
                CommitAction::Go {
                    id,
                    bucket,
                    object_id,
                } => sdk
                    .storage_commit_write(id)
                    .await
                    .map(|out| wit::ObjectStat {
                        bucket,
                        object_id,
                        size: out.size,
                        chunks: out.chunks,
                        revision: out.revision,
                        exists: true,
                        committed: true,
                    })
                    .map_err(|e| wit_err(&e)),
                CommitAction::Done => Err(wit::SdkError::NotFound(
                    "upload session is already finished".to_string(),
                )),
                CommitAction::Invalid(msg) => Err(wit::SdkError::InvalidArgument(format!(
                    "upload handle is not valid: {msg}"
                ))),
            }
        }
    }

    fn abort(
        &mut self,
        self_: Resource<StorageUploader>,
    ) -> impl std::future::Future<Output = Result<(), wit::SdkError>> + Send {
        let sdk = Arc::clone(&self.sdk);
        let action = {
            let mut table = self.table.lock();
            match table.get_mut(&self_) {
                Ok(up) if !up.finished => {
                    up.finished = true;
                    IdAction::Id(up.id)
                }
                Ok(_) => IdAction::Done,
                Err(e) => IdAction::Invalid(e.to_string()),
            }
        };
        async move {
            match action {
                IdAction::Id(id) => sdk.storage_abort_write(id).await.map_err(|e| wit_err(&e)),
                // 幂等：已提交/已中止再次 abort 仍是 ok
                IdAction::Done => Ok(()),
                IdAction::Invalid(msg) => Err(wit::SdkError::InvalidArgument(format!(
                    "upload handle is not valid: {msg}"
                ))),
            }
        }
    }

    fn drop(
        &mut self,
        rep: Resource<StorageUploader>,
    ) -> impl std::future::Future<Output = wasmtime::Result<()>> + Send {
        let sdk = Arc::clone(&self.sdk);
        let removed = self.table.lock().delete(rep);
        async move {
            match removed {
                // guest 未 commit/abort 就 drop：立刻中止上传（RAII 的核心收益）
                Ok(up) if !up.finished => {
                    if let Err(e) = sdk.storage_abort_write(up.id).await {
                        tracing::warn!("storage upload abort failed on drop (id={}): {e}", up.id);
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::debug!("storage upload handle already released: {e}");
                }
            }
            Ok(())
        }
    }
}

/// WIT `resource downloader` 的宿主实现（对象分块下载）。
impl wit::HostDownloader for ComponentHostState {
    fn stat(
        &mut self,
        self_: Resource<StorageDownloader>,
    ) -> impl std::future::Future<Output = Result<wit::ObjectStat, wit::SdkError>> + Send {
        let sdk = Arc::clone(&self.sdk);
        let action = {
            let mut table = self.table.lock();
            match table.get_mut(&self_) {
                Ok(d) if !d.closed => IdAction::Id(d.id),
                Ok(_) => IdAction::Done,
                Err(e) => IdAction::Invalid(e.to_string()),
            }
        };
        async move {
            match action {
                IdAction::Id(id) => sdk
                    .storage_reader_stat(id)
                    .await
                    .map(|s| wit_object_stat(&s))
                    .map_err(|e| wit_err(&e)),
                IdAction::Done => Err(wit::SdkError::InvalidArgument(
                    "download session is already closed".to_string(),
                )),
                IdAction::Invalid(msg) => Err(wit::SdkError::InvalidArgument(format!(
                    "download handle is not valid: {msg}"
                ))),
            }
        }
    }

    fn read(
        &mut self,
        self_: Resource<StorageDownloader>,
        max_len: u64,
    ) -> impl std::future::Future<Output = Result<Option<Vec<u8>>, wit::SdkError>> + Send {
        let sdk = Arc::clone(&self.sdk);
        let action = {
            let mut table = self.table.lock();
            match table.get_mut(&self_) {
                Ok(d) if !d.closed => IdAction::Id(d.id),
                Ok(_) => IdAction::Done,
                Err(e) => IdAction::Invalid(e.to_string()),
            }
        };
        async move {
            match action {
                IdAction::Id(id) => sdk
                    .storage_read_chunk(id, max_len)
                    .await
                    .map_err(|e| wit_err(&e)),
                IdAction::Done => Err(wit::SdkError::InvalidArgument(
                    "download session is already closed".to_string(),
                )),
                IdAction::Invalid(msg) => Err(wit::SdkError::InvalidArgument(format!(
                    "download handle is not valid: {msg}"
                ))),
            }
        }
    }

    fn close(
        &mut self,
        self_: Resource<StorageDownloader>,
    ) -> impl std::future::Future<Output = Result<(), wit::SdkError>> + Send {
        let sdk = Arc::clone(&self.sdk);
        let action = {
            let mut table = self.table.lock();
            match table.get_mut(&self_) {
                Ok(d) if !d.closed => {
                    d.closed = true;
                    IdAction::Id(d.id)
                }
                Ok(_) => IdAction::Done,
                Err(e) => IdAction::Invalid(e.to_string()),
            }
        };
        async move {
            match action {
                IdAction::Id(id) => sdk.storage_close_read(id).await.map_err(|e| wit_err(&e)),
                // 幂等：已关闭再次 close 仍是 ok
                IdAction::Done => Ok(()),
                IdAction::Invalid(msg) => Err(wit::SdkError::InvalidArgument(format!(
                    "download handle is not valid: {msg}"
                ))),
            }
        }
    }

    fn drop(
        &mut self,
        rep: Resource<StorageDownloader>,
    ) -> impl std::future::Future<Output = wasmtime::Result<()>> + Send {
        let sdk = Arc::clone(&self.sdk);
        let removed = self.table.lock().delete(rep);
        async move {
            match removed {
                Ok(d) if !d.closed => {
                    if let Err(e) = sdk.storage_close_read(d.id).await {
                        tracing::warn!("storage download close failed on drop (id={}): {e}", d.id);
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::debug!("storage download handle already released: {e}");
                }
            }
            Ok(())
        }
    }
}

/// 收集句柄表里仍存活的句柄，并逐条标记为已释放（避免随后 `drop` 重复释放）。
///
/// 返回 `(订阅 id, 上传会话 id, 下载会话 id)`。用于实例销毁前的兜底
/// （trap / 插件停止）：正常路径下句柄由 guest 的 drop 逐个释放，
/// 只有「guest 未 drop 就被销毁」才走到这里。
fn live_handle_ids(table: &mut ResourceTable) -> (Vec<u64>, Vec<u64>, Vec<u64>) {
    let (mut subs, mut uploads, mut downloads) = (Vec::new(), Vec::new(), Vec::new());
    for entry in table.iter_mut() {
        if let Some(sub) = entry.downcast_mut::<WatchSubscription>() {
            if !sub.closed {
                sub.closed = true;
                subs.push(sub.id);
            }
        } else if let Some(up) = entry.downcast_mut::<StorageUploader>() {
            if !up.finished {
                up.finished = true;
                uploads.push(up.id);
            }
        } else if let Some(d) = entry.downcast_mut::<StorageDownloader>() {
            if !d.closed {
                d.closed = true;
                downloads.push(d.id);
            }
        }
    }
    (subs, uploads, downloads)
}

/// `int-value` 等标量在 WIT 侧无借用问题；这里处理 `Compare` 的双向映射。
fn op_from_wit(op: wit::CompareOp) -> CompareOp {
    match op {
        wit::CompareOp::Equal => CompareOp::Equal,
        wit::CompareOp::NotEqual => CompareOp::NotEqual,
        wit::CompareOp::Greater => CompareOp::Greater,
        wit::CompareOp::Less => CompareOp::Less,
    }
}

fn target_from_wit(t: wit::CompareTarget) -> CompareTarget {
    match t {
        wit::CompareTarget::Version => CompareTarget::Version,
        wit::CompareTarget::Value => CompareTarget::Value,
        wit::CompareTarget::ModRevision => CompareTarget::ModRevision,
    }
}

fn txn_op_from_wit(op: &wit::TxnOp) -> SdkTxnOp {
    match op {
        wit::TxnOp::Put(p) => SdkTxnOp::Put(KvPut {
            key: p.key.clone(),
            value: p.value.clone(),
            lease_id: p.lease_id,
            prev_kv: p.prev_kv,
            request_id: Vec::new(),
        }),
        wit::TxnOp::Delete(d) => SdkTxnOp::Delete(KvDelete {
            key: d.key.clone(),
            range_end: d.range_end.clone(),
            prev_kv: d.prev_kv,
            request_id: Vec::new(),
        }),
        wit::TxnOp::Range(r) => SdkTxnOp::Range(KvRange {
            key: r.key.clone(),
            range_end: r.range_end.clone(),
            limit: r.limit,
            revision: r.revision,
            keys_only: r.keys_only,
            count_only: r.count_only,
        }),
    }
}

fn log_line(plugin: &str, level: &str, message: &str) {
    match level {
        "error" => tracing::error!(target: "coord_agent::plugin", "[{plugin}] {message}"),
        "warn" | "warning" => tracing::warn!(target: "coord_agent::plugin", "[{plugin}] {message}"),
        "debug" => tracing::debug!(target: "coord_agent::plugin", "[{plugin}] {message}"),
        "trace" => tracing::trace!(target: "coord_agent::plugin", "[{plugin}] {message}"),
        _ => tracing::info!(target: "coord_agent::plugin", "[{plugin}] {message}"),
    }
}

/// 武装一次执行的资源预算（fuel 指令数 + epoch 墙钟增量）。
///
/// `set_epoch_deadline` 是**相对当前 epoch 的增量**，因此每次 wasm 执行
/// （实例化 / `init` / `invoke`）前都要重新武装，否则预算会被上一次执行耗尽。
fn arm_budget(store: &mut Store<ComponentHostState>) -> Result<(), String> {
    let budget = store.data().fuel_budget();
    store
        .set_fuel(budget)
        .map_err(|e| format!("failed to reset fuel: {e}"))?;
    let deadline = store.data().epoch_deadline();
    store.set_epoch_deadline(deadline);
    Ok(())
}

// ──── 插件线程内的实例 ────

/// 已实例化的组件插件（store + 生成的 world 绑定）。
struct ComponentInstance {
    store: Store<ComponentHostState>,
    plugin: bindings::Plugin,
}

/// 调用返回：应答字节 或 插件级错误（guest 显式 `err`，与 trap 可区分）。
enum ComponentCallOutcome {
    Response(Vec<u8>),
    PluginError(String),
}

impl ComponentInstance {
    async fn new(
        shared: &SharedComponentEngine,
        component: &Component,
        sdk: Arc<PluginSdk>,
        env: BTreeMap<String, String>,
        limits: &PluginLimits,
    ) -> Result<Self, String> {
        let mut store = Store::new(&shared.engine, ComponentHostState::new(sdk, env, limits));
        store.limiter(|state| &mut state.limits);
        // 先武装预算再实例化：`set_epoch_deadline` 是**相对当前 epoch 的增量**，
        // 若在此处只给 1 个 tick，组件实例化（会执行 guest 的 start/适配层代码）
        // 一旦跨过 10ms 滴答就会以 `wasm trap: interrupt` 失败 —— 组件实例化
        // 比 core module 慢得多，并行测试下这是可复现的真实故障。
        arm_budget(&mut store)?;

        let mut linker: Linker<ComponentHostState> = Linker::new(&shared.engine);
        // 宿主类型 = store 数据本身（`HasSelf`）：`with` 只映射了 watch 句柄这一个
        // 资源（见文件顶部 `bindgen!` 配置），其余接口类型仍是生成类型，因此
        // 实例化直接把 `&mut ComponentHostState` 作为 trait 实现者传入。
        bindings::Plugin::add_to_linker::<
            ComponentHostState,
            wasmtime::component::HasSelf<ComponentHostState>,
        >(&mut linker, |state| state)
        .map_err(|e| format!("failed to add host imports to linker: {e}"))?;

        let plugin = bindings::Plugin::instantiate_async(&mut store, component, &linker)
            .await
            .map_err(|e| format!("instantiate failed: {e}"))?;

        // guest `init()`：独立预算（与单次 invoke 同等对待）
        arm_budget(&mut store)?;
        match plugin.coord_plugin_guest().call_init(&mut store).await {
            Ok(Ok(())) => {}
            Ok(Err(msg)) => return Err(format!("plugin init() failed: {msg}")),
            Err(e) => return Err(format!("plugin init() trapped: {e}")),
        }

        Ok(Self { store, plugin })
    }

    /// 调一次 guest `handle-invoke`（含资源预算重设与 trap → 错误映射）。
    async fn invoke(
        &mut self,
        method: &str,
        payload: &[u8],
    ) -> Result<ComponentCallOutcome, super::wasm_engine::WasmInvokeError> {
        arm_budget(&mut self.store).map_err(super::wasm_engine::WasmInvokeError::host)?;

        match self
            .plugin
            .coord_plugin_guest()
            .call_handle_invoke(&mut self.store, method, payload)
            .await
        {
            Ok(Ok(bytes)) => Ok(ComponentCallOutcome::Response(bytes)),
            Ok(Err(msg)) => Ok(ComponentCallOutcome::PluginError(msg)),
            Err(e) => Err(super::wasm_engine::WasmInvokeError::trapped(
                classify_trap(&e),
                format!("plugin trapped: {e}"),
            )),
        }
    }

    /// guest `stop()`（可选导出；失败只记录）。
    async fn stop(&mut self) {
        if let Err(e) = self
            .plugin
            .coord_plugin_guest()
            .call_stop(&mut self.store)
            .await
        {
            tracing::debug!("component plugin stop() failed: {e}");
        }
    }

    /// 释放句柄表里仍存活的句柄，并清空句柄表（实例销毁前的兜底）。
    ///
    /// 正常路径下 guest 的句柄 drop 已逐个释放；这里覆盖两类残局：
    /// ① guest 未 drop 就被销毁（trap / 实例丢弃）；
    /// ② 插件停止时仍有存活句柄。覆盖 watch 订阅 + 上传/下载会话。
    async fn release_handles(&mut self) {
        let sdk = Arc::clone(&self.store.data().sdk);
        let (subs, uploads, downloads) = {
            let mut table = self.store.data().table.lock();
            let ids = live_handle_ids(&mut table);
            // 句柄表整体换成空表：残留表项随旧表一起丢弃（id 已收走）
            let cap = table.max_capacity();
            let mut fresh = ResourceTable::new();
            fresh.set_max_capacity(cap);
            let _drained = std::mem::replace(&mut *table, fresh);
            ids
        };
        for id in subs {
            if let Err(e) = sdk.watch_close(id).await {
                tracing::debug!("watch subscription release failed (id={id}): {e}");
            }
        }
        for id in uploads {
            if let Err(e) = sdk.storage_abort_write(id).await {
                tracing::debug!("storage upload release failed (id={id}): {e}");
            }
        }
        for id in downloads {
            if let Err(e) = sdk.storage_close_read(id).await {
                tracing::debug!("storage download release failed (id={id}): {e}");
            }
        }
    }
}

// ──── 插件加载器 ────

/// 组件模型加载器（由 [`super::wasm_engine::WasmPluginLoader`] 按 ABI 派发）。
pub(crate) struct ComponentLoader {
    dir: PathBuf,
    backend: Arc<dyn crate::plugin::sdk::PluginSdkBackend>,
    env: BTreeMap<String, String>,
    hooks: Option<Arc<HookRegistry>>,
    identity: Option<Arc<PluginIdentityManager>>,
    metrics: Option<crate::metrics::AgentMetrics>,
    shared: Arc<SharedComponentEngine>,
}

impl ComponentLoader {
    pub(crate) fn new(
        dir: PathBuf,
        backend: Arc<dyn crate::plugin::sdk::PluginSdkBackend>,
        env: BTreeMap<String, String>,
        hooks: Option<Arc<HookRegistry>>,
        identity: Option<Arc<PluginIdentityManager>>,
        metrics: Option<crate::metrics::AgentMetrics>,
    ) -> Result<Self, String> {
        Ok(Self {
            dir,
            backend,
            env,
            hooks,
            identity,
            metrics,
            shared: SharedComponentEngine::new()?,
        })
    }

    /// 加载一个组件插件（调用方已读完字节并判定为组件）。
    pub(crate) async fn load_bytes(
        &self,
        manifest: &PluginManifest,
        entry: PathBuf,
        bytes: &[u8],
    ) -> ServiceResult<Arc<dyn Plugin>> {
        let component = Component::new(&self.shared.engine, bytes).map_err(
            |e| -> crate::service::ServiceError {
                format!(
                    "plugin '{}' component {} failed to compile: {e}",
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

        // 与 core ABI / JS 路径同一身份管理器（每插件受限 CCT，D5）
        // 与 `js_engine` 一致：用有界重试，失败后在 ERROR 级别明确说出后果。
        if let Some(identity) = &self.identity {
            if let Err(e) = identity
                .ensure_with_retry(
                    &manifest.name,
                    &manifest.capabilities,
                    crate::plugin::identity::ENSURE_RETRY_POLICY,
                )
                .await
            {
                tracing::error!(
                    "component plugin '{}': identity provisioning failed after retries ({e}); \
                     outbound calls fall back to the SHARED UNAUTHENTICATED client, so every \
                     constraint check will be denied by the server until the plugin is reloaded \
                     (SIGHUP) or the agent restarts",
                    manifest.name
                );
            }
        }

        let dir_ok = entry.starts_with(&self.dir);
        if !dir_ok {
            return Err(format!(
                "plugin '{}' entry '{}' escapes the plugin dir",
                manifest.name,
                entry.display()
            )
            .into());
        }

        Ok(Arc::new(ComponentPlugin {
            manifest: manifest.clone(),
            entry_path: entry,
            fingerprint: crate::plugin::content_fingerprint_bytes(bytes),
            component,
            sdk: Arc::new(sdk),
            env: self.env.clone(),
            limits: manifest.limits.clone(),
            shared: Arc::clone(&self.shared),
            identity: self.identity.clone(),
            metrics: self.metrics.clone(),
            trapped: Arc::new(AtomicBool::new(false)),
            state: Mutex::new(ComponentState::new()),
        }))
    }
}

// ──── 插件实例 ────

struct ComponentState {
    status: PluginStatus,
    tx: Option<tokio::sync::mpsc::Sender<ComponentCommand>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl ComponentState {
    fn new() -> Self {
        Self {
            status: PluginStatus::Loaded,
            tx: None,
            thread: None,
        }
    }
}

enum ComponentCommand {
    Invoke {
        method: String,
        payload: Vec<u8>,
        resp: tokio::sync::oneshot::Sender<Result<Vec<u8>, String>>,
    },
    Stop {
        resp: tokio::sync::oneshot::Sender<()>,
    },
}

/// 单组件插件实例（专属线程 + 专属 current-thread 运行时 + 专属 store）。
pub struct ComponentPlugin {
    manifest: PluginManifest,
    entry_path: PathBuf,
    /// 加载时的组件字节内容指纹（`sha256:<hex>`；Phase 5 版本化重载检测）
    fingerprint: String,
    component: Component,
    sdk: Arc<PluginSdk>,
    env: BTreeMap<String, String>,
    limits: PluginLimits,
    shared: Arc<SharedComponentEngine>,
    identity: Option<Arc<PluginIdentityManager>>,
    metrics: Option<crate::metrics::AgentMetrics>,
    trapped: Arc<AtomicBool>,
    state: Mutex<ComponentState>,
}

impl std::fmt::Debug for ComponentPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComponentPlugin")
            .field("name", &self.manifest.name)
            .field("entry", &self.entry_path)
            .finish_non_exhaustive()
    }
}

impl ComponentPlugin {
    fn fail(&self, reason: impl Into<String>) {
        let reason = reason.into();
        tracing::error!("component plugin '{}' failed: {reason}", self.manifest.name);
        let mut st = self.state.lock();
        st.status = PluginStatus::Failed(reason);
        st.tx = None;
    }

    /// 单次调用（含外层看门狗）。
    async fn invoke_inner(&self, method: &str, payload: &[u8]) -> Result<Vec<u8>, String> {
        let tx = {
            let st = self.state.lock();
            st.tx.clone()
        };
        let Some(tx) = tx else {
            return Err(format!("plugin '{}' is not running", self.manifest.name));
        };
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        let cmd = ComponentCommand::Invoke {
            method: method.to_string(),
            payload: payload.to_vec(),
            resp: resp_tx,
        };
        if tx.send(cmd).await.is_err() {
            self.fail("plugin thread is gone");
            return Err(format!("plugin '{}' thread is gone", self.manifest.name));
        }
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
        if result.is_err() && self.trapped.load(Ordering::Relaxed) {
            self.fail(format!(
                "wasm trap discarded the instance: {}",
                result.as_ref().err().cloned().unwrap_or_default()
            ));
        }
        result
    }
}

#[async_trait]
impl Plugin for ComponentPlugin {
    fn name(&self) -> &str {
        &self.manifest.name
    }

    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    /// 内容指纹 = 加载时的组件字节 SHA256（Phase 5 版本化）。
    fn content_fingerprint(&self) -> Option<String> {
        Some(self.fingerprint.clone())
    }

    async fn init(&self) -> ServiceResult<()> {
        // 有界命令通道：容量 16，背压交给调用方（网关层已有并发上限）
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ComponentCommand>(16);
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<Result<(), String>>();
        let failed = Arc::new(Mutex::new(None::<String>));

        let thread = {
            let name = self.manifest.name.clone();
            let component = self.component.clone();
            let sdk = Arc::clone(&self.sdk);
            let env = self.env.clone();
            let limits = self.limits.clone();
            let shared = Arc::clone(&self.shared);
            let trapped = Arc::clone(&self.trapped);
            let metrics = self.metrics.clone();
            let failed = Arc::clone(&failed);

            std::thread::Builder::new()
                .name(format!("coord-plugin-cm-{name}"))
                .spawn(move || {
                    // 专属 current-thread 运行时：宿主 import 的 async 在
                    // 此推进；空闲期仍能跑后台任务（lease 保活等）。
                    let rt = match tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(rt) => rt,
                        Err(e) => {
                            let msg = format!("failed to build plugin runtime: {e}");
                            *failed.lock() = Some(msg.clone());
                            let _ = ready_tx.send(Err(msg));
                            return;
                        }
                    };

                    rt.block_on(async move {
                        let mut instance =
                            match ComponentInstance::new(&shared, &component, sdk, env, &limits)
                                .await
                            {
                                Ok(i) => i,
                                Err(e) => {
                                    *failed.lock() = Some(e.clone());
                                    let _ = ready_tx.send(Err(e));
                                    return;
                                }
                            };
                        if ready_tx.send(Ok(())).is_err() {
                            return;
                        }
                        while let Some(cmd) = rx.recv().await {
                            match cmd {
                                ComponentCommand::Invoke {
                                    method,
                                    payload,
                                    resp,
                                } => {
                                    let out = instance.invoke(&method, &payload).await;
                                    let fatal = out.is_err();
                                    let (mapped, trap) = match out {
                                        Ok(ComponentCallOutcome::Response(bytes)) => {
                                            (Ok(bytes), None)
                                        }
                                        Ok(ComponentCallOutcome::PluginError(msg)) => (
                                            Err(format!("plugin '{name}' returned error: {msg}")),
                                            None,
                                        ),
                                        Err(e) => (Err(e.message), e.trap),
                                    };
                                    let _ = resp.send(mapped);
                                    if fatal {
                                        if let (Some(metrics), Some(reason)) = (&metrics, trap) {
                                            metrics.record_plugin_trap(&name, reason);
                                        }
                                        // trap → store 不可继续使用 → 丢弃实例；
                                        // 先把句柄表里仍存活的订阅释放掉（guest 已无法 drop）
                                        instance.release_handles().await;
                                        trapped.store(true, Ordering::Relaxed);
                                        tracing::error!(
                                            "component plugin '{name}' trapped (reason={}); \
                                             discarding instance",
                                            trap.unwrap_or("other")
                                        );
                                        break;
                                    }
                                }
                                ComponentCommand::Stop { resp } => {
                                    instance.stop().await;
                                    instance.release_handles().await;
                                    let _ = resp.send(());
                                    break;
                                }
                            }
                        }
                        tracing::debug!("component plugin '{name}' thread exited");
                    });
                })
                .map_err(|e| -> crate::service::ServiceError {
                    format!("failed to spawn component plugin thread: {e}").into()
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
                    "component plugin '{}' initialised (entry={}, max_memory={}MiB, \
                     max_exec={}ms, fuel={}, epoch_ticks={})",
                    self.manifest.name,
                    self.entry_path.display(),
                    self.limits.max_memory_mb,
                    self.limits.max_exec_ms,
                    self.limits.max_exec_ms.saturating_mul(FUEL_PER_MS),
                    (self.limits.max_exec_ms / EPOCH_TICK_MS).max(1),
                );
                Ok(())
            }
            Ok(Err(_)) | Err(_) => {
                let reason = failed
                    .lock()
                    .clone()
                    .unwrap_or_else(|| "component plugin thread exited during init".into());
                self.fail(reason.clone());
                Err(format!("component plugin initialisation failed: {reason}").into())
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
        if let Some(tx) = tx {
            let (resp_tx, resp_rx) = tokio::sync::oneshot::channel::<()>();
            if tx
                .send(ComponentCommand::Stop { resp: resp_tx })
                .await
                .is_ok()
            {
                let _ = tokio::time::timeout(Duration::from_secs(5), resp_rx).await;
            }
        }
        if let Some(thread) = thread {
            let _ = tokio::task::spawn_blocking(move || thread.join()).await;
        }
        // 插件线程已退出（guest drop / 宿主兜底释放都已完成）→ 再清后端侧残留
        // 句柄（lease 保活等）。顺序刻意如此：先让 RAII 逐句柄释放，`release()`
        // 只作兜底。
        self.sdk.release().await;
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

impl ComponentPlugin {
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
