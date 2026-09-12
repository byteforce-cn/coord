// coord-agent: 宿主导入 SDK 的 rquickjs 绑定（计划 §7 / §7.4）
//
// 注入到插件 isolate 的全局 `coord` 对象：
//
// ```js
// coord.plugin                      // 插件名
// coord.kv.put(key, value, opts?)   // → {prevKv?, revision}
// coord.kv.range(key, opts?)        // → {kvs, count, revision}
// coord.kv.delete(key, opts?)       // → {deleted, prevKvs, revision}
// coord.txn(compares, success, failure, opts?) // → {succeeded, responses, revision}
// coord.lease.grant(ttl, id?)       // → {id, ttl}
// coord.lease.revoke(id)            // → undefined
// coord.lease.keepAlive(id)         // → {id, stop()}
// coord.log(level, msg)             // 宿主日志
// coord.env(key)                    // 配置注入（string | undefined）
// coord.util.encode(str)            // → Uint8Array（UTF-8）
// coord.util.decode(bytes)          // → string
// coord.util.isForbidden(err)       // 判定 SDK 拒绝类错误
// coord.util.sleep(ms)              // 异步睡眠（受执行预算约束，见内置 SDK）
// coord.limits.maxExecMs            // 单次调用执行预算（毫秒）
// coord.limits.maxMemoryMb          // 内存上限（MiB）
// ```
//
// 约定：
// - **二进制**：值以 `Uint8Array` 出入；键与值也接受 `string`（按 UTF-8 编码）；
// - **数字**：`revision` / `leaseId` / `version` 等以 JS number 暴露；
// - **错误**：宿主函数 reject 一个 `Error`，其 `name` 为 `ErrNotFound` /
//   `ErrUnavailable` / `ErrForbidden` / `ErrResourceExhausted` /
//   `ErrInvalidArgument` / `ErrInternal`，`code` 为稳定短名（§7.3）。
//
// 实现要点（R1 桥）：
// - 每个宿主函数都是 `Async`（rquickjs `futures` 特性）包装的 **具名 async fn**：
//   参数按值持有（`Value<'js>` / `Ctx<'js>`），不跨 await 借用 JS 栈上数据；
// - 真正的工作（scope 守卫 + coord-client 调用）在 [`PluginSdk`] 里完成，
//   本模块只做 JS ↔ Rust 的编解码。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use rquickjs::function::{Async, Opt};
use rquickjs::{Array, Ctx, Exception, FromJs, Function, IntoJs, Object, TypedArray, Value};

use crate::plugin::manifest::PluginLimits;
use crate::plugin::sdk::backend::{
    Compare, CompareOp, CompareTarget, KvDelete, KvPut, KvRange, KvRecord, SdkError, TxnOp,
    TxnOpOut, TxnReq, WatchSubscribe,
};
use crate::plugin::sdk::PluginSdk;

// ──── 安装入口 ────

/// 把 `coord` 全局对象装进上下文。
pub fn install(
    ctx: &Ctx<'_>,
    sdk: Arc<PluginSdk>,
    env: &BTreeMap<String, String>,
    limits: &PluginLimits,
) -> rquickjs::Result<()> {
    let coord = Object::new(ctx.clone())?;
    coord.set("plugin", sdk.plugin().to_string())?;

    // ── limits：插件可见的资源预算（内置 JS SDK 用它做提前失败）──
    let limits_obj = Object::new(ctx.clone())?;
    limits_obj.set("maxExecMs", limits.max_exec_ms)?;
    limits_obj.set("maxMemoryMb", limits.max_memory_mb)?;
    limits_obj.set("maxObjects", limits.max_objects)?;
    coord.set("limits", limits_obj)?;

    // ── kv ──
    let kv = Object::new(ctx.clone())?;
    kv.set("put", kv_put(ctx, Arc::clone(&sdk))?)?;
    kv.set("get", kv_get(ctx, Arc::clone(&sdk))?)?;
    kv.set("range", kv_range(ctx, Arc::clone(&sdk))?)?;
    kv.set("delete", kv_delete(ctx, Arc::clone(&sdk))?)?;
    kv.set("create", kv_create(ctx, Arc::clone(&sdk))?)?;
    coord.set("kv", kv)?;

    // ── txn ──
    coord.set("txn", txn(ctx, Arc::clone(&sdk))?)?;

    // ── lease ──
    let lease = Object::new(ctx.clone())?;
    lease.set("grant", lease_grant(ctx, Arc::clone(&sdk))?)?;
    lease.set("revoke", lease_revoke(ctx, Arc::clone(&sdk))?)?;
    lease.set("keepAlive", lease_keep_alive(ctx, Arc::clone(&sdk))?)?;
    coord.set("lease", lease)?;

    // ── watch ──
    let watch = Object::new(ctx.clone())?;
    watch.set("subscribe", watch_subscribe(ctx, Arc::clone(&sdk))?)?;
    coord.set("watch", watch)?;

    // ── storage ──
    let storage = Object::new(ctx.clone())?;
    storage.set("put", storage_put(ctx, Arc::clone(&sdk))?)?;
    storage.set("get", storage_get(ctx, Arc::clone(&sdk))?)?;
    storage.set("stat", storage_stat(ctx, Arc::clone(&sdk))?)?;
    storage.set("delete", storage_delete(ctx, Arc::clone(&sdk))?)?;
    storage.set("openWrite", storage_open_write(ctx, Arc::clone(&sdk))?)?;
    storage.set("openRead", storage_open_read(ctx, Arc::clone(&sdk))?)?;
    coord.set("storage", storage)?;

    // ── log / env ──
    coord.set("log", log_fn(ctx)?)?;
    let env = env.clone();
    coord.set(
        "env",
        Function::new(ctx.clone(), move |key: String| -> Option<String> {
            env.get(&key).cloned()
        })?,
    )?;

    // ── util ──
    let util = Object::new(ctx.clone())?;
    util.set("encode", Function::new(ctx.clone(), util_encode)?)?;
    util.set("decode", Function::new(ctx.clone(), util_decode)?)?;
    util.set(
        "isForbidden",
        Function::new(ctx.clone(), util_is_forbidden)?,
    )?;
    util.set("sleep", util_sleep(ctx)?)?;
    coord.set("util", util)?;

    ctx.globals().set("coord", coord)?;

    // 内置 JS SDK（Phase 3.3 部分）：在 `coord` 之上合成的组合原语
    // （idgen 等；只使用 §7 宿主导入，不新增宿主面）。
    ctx.eval::<(), _>(STDLIB_JS)?;
    Ok(())
}

/// 内置 JS SDK 源（与 `sdk/stdlib.js` 同源，编译期嵌入）。
const STDLIB_JS: &str = include_str!("stdlib.js");

// ──── JS ↔ Rust 基本转换 ────

/// JS 值 → 字节：`string`（UTF-8）/ `Uint8Array` / `null|undefined`（空）。
pub fn js_bytes<'js>(ctx: &Ctx<'js>, value: &Value<'js>) -> rquickjs::Result<Vec<u8>> {
    if value.is_null() || value.is_undefined() {
        return Ok(Vec::new());
    }
    if let Some(s) = value.as_string() {
        return Ok(s.to_string()?.into_bytes());
    }
    if value.is_object() {
        if let Ok(ta) = TypedArray::<u8>::from_js(ctx, value.clone()) {
            return Ok(ta.as_bytes().map(|b| b.to_vec()).unwrap_or_default());
        }
    }
    Err(Exception::throw_type(
        ctx,
        "expected a string, Uint8Array or null/undefined value",
    ))
}

/// 字节 → JS `Uint8Array`。
pub fn bytes_to_js<'js>(ctx: &Ctx<'js>, bytes: &[u8]) -> rquickjs::Result<Value<'js>> {
    TypedArray::<u8>::new_copy(ctx.clone(), bytes)?.into_js(ctx)
}

/// `coord.util.encode(str)` → `Uint8Array`（UTF-8）。
fn util_encode<'js>(ctx: Ctx<'js>, s: String) -> rquickjs::Result<Value<'js>> {
    bytes_to_js(&ctx, s.as_bytes())
}

/// `coord.util.decode(bytes)` → `string`（非法 UTF-8 用替换字符）。
fn util_decode<'js>(ctx: Ctx<'js>, v: Value<'js>) -> rquickjs::Result<String> {
    let bytes = js_bytes(&ctx, &v)?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

/// `coord.util.sleep(ms)`：宿主侧异步等待（组合原语的重试退避 / 观测超时需要）。
///
/// - 非负整数毫秒，上限 1 小时（防御性夹紧：真正约束是插件的 `max_exec_ms`，
///   超预算的等待会被中断处理器打断并丢弃 isolate —— 内置 SDK 在调用前检查）；
/// - 不占用 JS 线程：等待期间宿主事件循环仍在驱动其它 job。
fn util_sleep<'js>(ctx: &Ctx<'js>) -> rquickjs::Result<Function<'js>> {
    Function::new(ctx.clone(), Async(util_sleep_call))
}

async fn util_sleep_call<'js>(ms: Value<'js>) -> rquickjs::Result<()> {
    let ms = js_i64(&ms, 0).clamp(0, 3_600_000) as u64;
    tokio::time::sleep(Duration::from_millis(ms)).await;
    Ok(())
}

/// `coord.util.isForbidden(err)` → bool。
fn util_is_forbidden<'js>(_ctx: Ctx<'js>, v: Value<'js>) -> bool {
    v.as_object()
        .and_then(|o| o.get::<_, Option<String>>("name").ok().flatten())
        .map(|n| n == "ErrForbidden")
        .unwrap_or(false)
}

/// JS 数字 → i64（非数字 → `default`）。
fn js_i64(value: &Value<'_>, default: i64) -> i64 {
    value
        .as_int()
        .map(i64::from)
        .or_else(|| value.as_float().map(|f| f as i64))
        .unwrap_or(default)
}

/// JS 布尔 → bool（非布尔 → `default`）。
fn js_bool(value: &Value<'_>, default: bool) -> bool {
    value.as_bool().unwrap_or(default)
}

/// 取对象的布尔字段。
fn obj_bool<'js>(obj: &Object<'js>, key: &str, default: bool) -> rquickjs::Result<bool> {
    let v: Value<'js> = obj.get(key)?;
    Ok(js_bool(&v, default))
}

/// 取对象的 i64 字段。
fn obj_i64<'js>(obj: &Object<'js>, key: &str, default: i64) -> rquickjs::Result<i64> {
    let v: Value<'js> = obj.get(key)?;
    Ok(js_i64(&v, default))
}

/// 取对象的字节字段。
fn obj_bytes<'js>(ctx: &Ctx<'js>, obj: &Object<'js>, key: &str) -> rquickjs::Result<Vec<u8>> {
    let v: Value<'js> = obj.get(key)?;
    js_bytes(ctx, &v)
}

/// 可选 options 对象 → 惰性解析（`undefined` / `null` → `None`）。
fn opt_object<'js>(ctx: &Ctx<'js>, v: &Value<'js>) -> rquickjs::Result<Option<Object<'js>>> {
    if v.is_null() || v.is_undefined() {
        return Ok(None);
    }
    match v.as_object() {
        Some(o) => Ok(Some(o.clone())),
        None => Err(Exception::throw_type(ctx, "options must be an object")),
    }
}

/// `Opt<Value>` 参数 → options 对象。
fn opt_opts<'js>(ctx: &Ctx<'js>, opts: &Opt<Value<'js>>) -> rquickjs::Result<Option<Object<'js>>> {
    match &opts.0 {
        Some(v) => opt_object(ctx, v),
        None => Ok(None),
    }
}

// ──── 错误抛出 ────

/// [`SdkError`] → JS 异常（`name` = `ErrXxx`，`code` = 稳定短名）。
pub fn throw_sdk_error<'js>(ctx: &Ctx<'js>, e: SdkError) -> rquickjs::Error {
    let name = e.code.exception_name();
    let msg = format!("{name}: {}", e.message);
    match Exception::from_message(ctx.clone(), &msg) {
        Ok(exc) => {
            let obj = exc.as_object();
            let _ = obj.set("name", name);
            let _ = obj.set("code", e.code.as_str());
            exc.throw()
        }
        Err(err) => err,
    }
}

// ──── 结果 → JS ────

/// KV 记录 → JS 对象 `{key, value, leaseId, version}`。
fn record_to_js<'js>(ctx: &Ctx<'js>, rec: &KvRecord) -> rquickjs::Result<Object<'js>> {
    let o = Object::new(ctx.clone())?;
    o.set("key", bytes_to_js(ctx, &rec.key)?)?;
    o.set("value", bytes_to_js(ctx, &rec.value)?)?;
    o.set("leaseId", rec.lease_id)?;
    o.set("version", rec.version)?;
    Ok(o)
}

/// KV 记录数组 → JS 数组。
fn records_to_js<'js>(ctx: &Ctx<'js>, recs: &[KvRecord]) -> rquickjs::Result<Array<'js>> {
    let arr = Array::new(ctx.clone())?;
    for (i, rec) in recs.iter().enumerate() {
        arr.set(i, record_to_js(ctx, rec)?)?;
    }
    Ok(arr)
}

// ──── kv ────

fn kv_put<'js>(ctx: &Ctx<'js>, sdk: Arc<PluginSdk>) -> rquickjs::Result<Function<'js>> {
    Function::new(
        ctx.clone(),
        Async(
            move |ctx: Ctx<'js>, key: Value<'js>, value: Value<'js>, opts: Opt<Value<'js>>| {
                let sdk = Arc::clone(&sdk);
                async move { run_kv_put(ctx, sdk, key, value, opts).await }
            },
        ),
    )
}

async fn run_kv_put<'js>(
    ctx: Ctx<'js>,
    sdk: Arc<PluginSdk>,
    key: Value<'js>,
    value: Value<'js>,
    opts: Opt<Value<'js>>,
) -> rquickjs::Result<Object<'js>> {
    let mut req = KvPut {
        key: js_bytes(&ctx, &key)?,
        value: js_bytes(&ctx, &value)?,
        lease_id: 0,
        prev_kv: false,
        request_id: Vec::new(),
    };
    if let Some(o) = opt_opts(&ctx, &opts)? {
        req.lease_id = obj_i64(&o, "leaseId", 0)?;
        req.prev_kv = obj_bool(&o, "prevKv", false)?;
        req.request_id = obj_bytes(&ctx, &o, "requestId")?;
    }
    let out = sdk
        .kv_put(req)
        .await
        .map_err(|e| throw_sdk_error(&ctx, e))?;
    let obj = Object::new(ctx.clone())?;
    obj.set("revision", out.revision)?;
    if let Some(prev) = &out.prev_kv {
        obj.set("prevKv", record_to_js(&ctx, prev)?)?;
    }
    Ok(obj)
}

fn kv_get<'js>(ctx: &Ctx<'js>, sdk: Arc<PluginSdk>) -> rquickjs::Result<Function<'js>> {
    Function::new(
        ctx.clone(),
        Async(move |ctx: Ctx<'js>, key: Value<'js>| {
            let sdk = Arc::clone(&sdk);
            async move { run_kv_get(ctx, sdk, key).await }
        }),
    )
}

async fn run_kv_get<'js>(
    ctx: Ctx<'js>,
    sdk: Arc<PluginSdk>,
    key: Value<'js>,
) -> rquickjs::Result<Value<'js>> {
    let value = sdk
        .kv_get(js_bytes(&ctx, &key)?)
        .await
        .map_err(|e| throw_sdk_error(&ctx, e))?;
    bytes_to_js(&ctx, &value)
}

fn kv_create<'js>(ctx: &Ctx<'js>, sdk: Arc<PluginSdk>) -> rquickjs::Result<Function<'js>> {
    Function::new(
        ctx.clone(),
        Async(
            move |ctx: Ctx<'js>, key: Value<'js>, value: Value<'js>, opts: Opt<Value<'js>>| {
                let sdk = Arc::clone(&sdk);
                async move { run_kv_create(ctx, sdk, key, value, opts).await }
            },
        ),
    )
}

async fn run_kv_create<'js>(
    ctx: Ctx<'js>,
    sdk: Arc<PluginSdk>,
    key: Value<'js>,
    value: Value<'js>,
    opts: Opt<Value<'js>>,
) -> rquickjs::Result<Object<'js>> {
    let mut lease_id = 0i64;
    if let Some(o) = opt_opts(&ctx, &opts)? {
        lease_id = obj_i64(&o, "leaseId", 0)?;
    }
    let revision = sdk
        .kv_create(js_bytes(&ctx, &key)?, js_bytes(&ctx, &value)?, lease_id)
        .await
        .map_err(|e| throw_sdk_error(&ctx, e))?;
    let obj = Object::new(ctx.clone())?;
    obj.set("revision", revision)?;
    Ok(obj)
}

fn kv_range<'js>(ctx: &Ctx<'js>, sdk: Arc<PluginSdk>) -> rquickjs::Result<Function<'js>> {
    Function::new(
        ctx.clone(),
        Async(
            move |ctx: Ctx<'js>, key: Value<'js>, opts: Opt<Value<'js>>| {
                let sdk = Arc::clone(&sdk);
                async move { run_kv_range(ctx, sdk, key, opts).await }
            },
        ),
    )
}

async fn run_kv_range<'js>(
    ctx: Ctx<'js>,
    sdk: Arc<PluginSdk>,
    key: Value<'js>,
    opts: Opt<Value<'js>>,
) -> rquickjs::Result<Object<'js>> {
    let mut req = KvRange {
        key: js_bytes(&ctx, &key)?,
        range_end: Vec::new(),
        limit: 0,
        revision: 0,
        keys_only: false,
        count_only: false,
    };
    if let Some(o) = opt_opts(&ctx, &opts)? {
        req.range_end = obj_bytes(&ctx, &o, "rangeEnd")?;
        req.limit = obj_i64(&o, "limit", 0)?;
        req.revision = obj_i64(&o, "revision", 0)?;
        req.keys_only = obj_bool(&o, "keysOnly", false)?;
        req.count_only = obj_bool(&o, "countOnly", false)?;
    }
    let out = sdk
        .kv_range(req)
        .await
        .map_err(|e| throw_sdk_error(&ctx, e))?;
    let obj = Object::new(ctx.clone())?;
    obj.set("kvs", records_to_js(&ctx, &out.kvs)?)?;
    obj.set("count", out.count)?;
    obj.set("revision", out.revision)?;
    Ok(obj)
}

fn kv_delete<'js>(ctx: &Ctx<'js>, sdk: Arc<PluginSdk>) -> rquickjs::Result<Function<'js>> {
    Function::new(
        ctx.clone(),
        Async(
            move |ctx: Ctx<'js>, key: Value<'js>, opts: Opt<Value<'js>>| {
                let sdk = Arc::clone(&sdk);
                async move { run_kv_delete(ctx, sdk, key, opts).await }
            },
        ),
    )
}

async fn run_kv_delete<'js>(
    ctx: Ctx<'js>,
    sdk: Arc<PluginSdk>,
    key: Value<'js>,
    opts: Opt<Value<'js>>,
) -> rquickjs::Result<Object<'js>> {
    let mut req = KvDelete {
        key: js_bytes(&ctx, &key)?,
        range_end: Vec::new(),
        prev_kv: false,
        request_id: Vec::new(),
    };
    if let Some(o) = opt_opts(&ctx, &opts)? {
        req.range_end = obj_bytes(&ctx, &o, "rangeEnd")?;
        req.prev_kv = obj_bool(&o, "prevKv", false)?;
        req.request_id = obj_bytes(&ctx, &o, "requestId")?;
    }
    let out = sdk
        .kv_delete(req)
        .await
        .map_err(|e| throw_sdk_error(&ctx, e))?;
    let obj = Object::new(ctx.clone())?;
    obj.set("deleted", out.deleted)?;
    obj.set("prevKvs", records_to_js(&ctx, &out.prev_kvs)?)?;
    obj.set("revision", out.revision)?;
    Ok(obj)
}

// ──── txn ────

/// 解析 JS compare：`{key, target, op, value?|version?|modRevision?}`。
fn parse_compare<'js>(ctx: &Ctx<'js>, v: &Value<'js>) -> rquickjs::Result<Compare> {
    let o = v
        .as_object()
        .ok_or_else(|| Exception::throw_type(ctx, "txn compare must be an object"))?;
    let target_s: String = o.get("target")?;
    let op_s: String = o.get("op")?;
    let target = match target_s.as_str() {
        "value" => CompareTarget::Value,
        "version" => CompareTarget::Version,
        "modRevision" | "mod_revision" => CompareTarget::ModRevision,
        other => {
            return Err(Exception::throw_type(
                ctx,
                &format!("unknown compare target '{other}'"),
            ))
        }
    };
    let op = match op_s.as_str() {
        "equal" | "eq" | "==" => CompareOp::Equal,
        "notEqual" | "ne" | "!=" => CompareOp::NotEqual,
        "greater" | "gt" | ">" => CompareOp::Greater,
        "less" | "lt" | "<" => CompareOp::Less,
        other => {
            return Err(Exception::throw_type(
                ctx,
                &format!("unknown compare op '{other}'"),
            ))
        }
    };
    let mut c = Compare {
        op,
        target,
        key: obj_bytes(ctx, o, "key")?,
        int_value: 0,
        bytes_value: Vec::new(),
    };
    match target {
        CompareTarget::Value => c.bytes_value = obj_bytes(ctx, o, "value")?,
        CompareTarget::Version => c.int_value = obj_i64(o, "version", 0)?,
        CompareTarget::ModRevision => c.int_value = obj_i64(o, "modRevision", 0)?,
    }
    Ok(c)
}

/// 解析 JS compare 数组。
fn parse_compares<'js>(ctx: &Ctx<'js>, v: &Value<'js>) -> rquickjs::Result<Vec<Compare>> {
    if v.is_null() || v.is_undefined() {
        return Ok(Vec::new());
    }
    let arr = v
        .as_array()
        .ok_or_else(|| Exception::throw_type(ctx, "txn compares must be an array"))?;
    let mut out = Vec::new();
    for item in arr.iter::<Value<'js>>() {
        out.push(parse_compare(ctx, &item?)?);
    }
    Ok(out)
}

/// 解析 JS txn 操作：`{type: "put"|"range"|"delete", ...}`。
fn parse_txn_op<'js>(ctx: &Ctx<'js>, v: &Value<'js>) -> rquickjs::Result<TxnOp> {
    let o = v
        .as_object()
        .ok_or_else(|| Exception::throw_type(ctx, "txn op must be an object"))?;
    let kind: String = o.get("type")?;
    match kind.as_str() {
        "put" => Ok(TxnOp::Put(KvPut {
            key: obj_bytes(ctx, o, "key")?,
            value: obj_bytes(ctx, o, "value")?,
            lease_id: obj_i64(o, "leaseId", 0)?,
            prev_kv: obj_bool(o, "prevKv", false)?,
            request_id: obj_bytes(ctx, o, "requestId")?,
        })),
        "range" => Ok(TxnOp::Range(KvRange {
            key: obj_bytes(ctx, o, "key")?,
            range_end: obj_bytes(ctx, o, "rangeEnd")?,
            limit: obj_i64(o, "limit", 0)?,
            revision: obj_i64(o, "revision", 0)?,
            keys_only: obj_bool(o, "keysOnly", false)?,
            count_only: obj_bool(o, "countOnly", false)?,
        })),
        "delete" => Ok(TxnOp::Delete(KvDelete {
            key: obj_bytes(ctx, o, "key")?,
            range_end: obj_bytes(ctx, o, "rangeEnd")?,
            prev_kv: obj_bool(o, "prevKv", false)?,
            request_id: obj_bytes(ctx, o, "requestId")?,
        })),
        other => Err(Exception::throw_type(
            ctx,
            &format!("unknown txn op type '{other}'"),
        )),
    }
}

/// 解析操作数组。
fn parse_op_array<'js>(
    ctx: &Ctx<'js>,
    v: &Value<'js>,
    field: &str,
) -> rquickjs::Result<Vec<TxnOp>> {
    if v.is_null() || v.is_undefined() {
        return Ok(Vec::new());
    }
    let arr = v
        .as_array()
        .ok_or_else(|| Exception::throw_type(ctx, &format!("txn '{field}' must be an array")))?;
    let mut ops = Vec::new();
    for item in arr.iter::<Value<'js>>() {
        ops.push(parse_txn_op(ctx, &item?)?);
    }
    Ok(ops)
}

/// Txn 分支结果 → JS 对象（带 `type` 判别字段）。
fn txn_op_out_to_js<'js>(ctx: &Ctx<'js>, out: &TxnOpOut) -> rquickjs::Result<Object<'js>> {
    let obj = Object::new(ctx.clone())?;
    match out {
        TxnOpOut::Put(p) => {
            obj.set("type", "put")?;
            obj.set("revision", p.revision)?;
            if let Some(prev) = &p.prev_kv {
                obj.set("prevKv", record_to_js(ctx, prev)?)?;
            }
        }
        TxnOpOut::Range(r) => {
            obj.set("type", "range")?;
            obj.set("kvs", records_to_js(ctx, &r.kvs)?)?;
            obj.set("count", r.count)?;
            obj.set("revision", r.revision)?;
        }
        TxnOpOut::Delete(d) => {
            obj.set("type", "delete")?;
            obj.set("deleted", d.deleted)?;
            obj.set("prevKvs", records_to_js(ctx, &d.prev_kvs)?)?;
            obj.set("revision", d.revision)?;
        }
    }
    Ok(obj)
}

fn txn<'js>(ctx: &Ctx<'js>, sdk: Arc<PluginSdk>) -> rquickjs::Result<Function<'js>> {
    Function::new(
        ctx.clone(),
        Async(
            move |ctx: Ctx<'js>,
                  compares: Value<'js>,
                  success: Value<'js>,
                  failure: Value<'js>,
                  opts: Opt<Value<'js>>| {
                let sdk = Arc::clone(&sdk);
                async move { run_txn(ctx, sdk, compares, success, failure, opts).await }
            },
        ),
    )
}

async fn run_txn<'js>(
    ctx: Ctx<'js>,
    sdk: Arc<PluginSdk>,
    compares: Value<'js>,
    success: Value<'js>,
    failure: Value<'js>,
    opts: Opt<Value<'js>>,
) -> rquickjs::Result<Object<'js>> {
    let mut req = TxnReq {
        compares: parse_compares(&ctx, &compares)?,
        success: parse_op_array(&ctx, &success, "success")?,
        failure: parse_op_array(&ctx, &failure, "failure")?,
        request_id: Vec::new(),
    };
    if let Some(o) = opt_opts(&ctx, &opts)? {
        req.request_id = obj_bytes(&ctx, &o, "requestId")?;
    }
    let out = sdk.txn(req).await.map_err(|e| throw_sdk_error(&ctx, e))?;
    let obj = Object::new(ctx.clone())?;
    obj.set("succeeded", out.succeeded)?;
    obj.set("revision", out.revision)?;
    let responses = Array::new(ctx.clone())?;
    for (i, r) in out.responses.iter().enumerate() {
        responses.set(i, txn_op_out_to_js(&ctx, r)?)?;
    }
    obj.set("responses", responses)?;
    Ok(obj)
}

// ──── lease ────

fn lease_grant<'js>(ctx: &Ctx<'js>, sdk: Arc<PluginSdk>) -> rquickjs::Result<Function<'js>> {
    Function::new(
        ctx.clone(),
        Async(move |ctx: Ctx<'js>, ttl: Value<'js>, id: Opt<Value<'js>>| {
            let sdk = Arc::clone(&sdk);
            async move { run_lease_grant(ctx, sdk, ttl, id).await }
        }),
    )
}

async fn run_lease_grant<'js>(
    ctx: Ctx<'js>,
    sdk: Arc<PluginSdk>,
    ttl: Value<'js>,
    id: Opt<Value<'js>>,
) -> rquickjs::Result<Object<'js>> {
    let ttl = js_i64(&ttl, 0);
    let id = id.0.as_ref().map(|v| js_i64(v, 0)).unwrap_or(0);
    let lease_id = sdk
        .lease_grant(ttl, id)
        .await
        .map_err(|e| throw_sdk_error(&ctx, e))?;
    let obj = Object::new(ctx.clone())?;
    obj.set("id", lease_id)?;
    obj.set("ttl", ttl)?;
    Ok(obj)
}

fn lease_revoke<'js>(ctx: &Ctx<'js>, sdk: Arc<PluginSdk>) -> rquickjs::Result<Function<'js>> {
    Function::new(
        ctx.clone(),
        Async(move |ctx: Ctx<'js>, id: Value<'js>| {
            let sdk = Arc::clone(&sdk);
            async move { run_lease_revoke(ctx, sdk, id).await }
        }),
    )
}

async fn run_lease_revoke<'js>(
    ctx: Ctx<'js>,
    sdk: Arc<PluginSdk>,
    id: Value<'js>,
) -> rquickjs::Result<()> {
    sdk.lease_revoke(js_i64(&id, 0))
        .await
        .map_err(|e| throw_sdk_error(&ctx, e))
}

fn lease_keep_alive<'js>(ctx: &Ctx<'js>, sdk: Arc<PluginSdk>) -> rquickjs::Result<Function<'js>> {
    Function::new(
        ctx.clone(),
        Async(move |ctx: Ctx<'js>, id: Value<'js>| {
            let sdk = Arc::clone(&sdk);
            async move { run_lease_keep_alive(ctx, sdk, id).await }
        }),
    )
}

async fn run_lease_keep_alive<'js>(
    ctx: Ctx<'js>,
    sdk: Arc<PluginSdk>,
    id: Value<'js>,
) -> rquickjs::Result<Object<'js>> {
    let id = js_i64(&id, 0);
    sdk.lease_keep_alive(id)
        .await
        .map_err(|e| throw_sdk_error(&ctx, e))?;
    let handle = Object::new(ctx.clone())?;
    handle.set("id", id)?;
    let stop_sdk = Arc::clone(&sdk);
    handle.set(
        "stop",
        Function::new(
            ctx.clone(),
            Async(move |ctx: Ctx<'js>| {
                let sdk = Arc::clone(&stop_sdk);
                async move { run_lease_keep_alive_stop(ctx, sdk, id).await }
            }),
        )?,
    )?;
    Ok(handle)
}

async fn run_lease_keep_alive_stop<'js>(
    ctx: Ctx<'js>,
    sdk: Arc<PluginSdk>,
    id: i64,
) -> rquickjs::Result<()> {
    sdk.lease_stop_keep_alive(id)
        .await
        .map_err(|e| throw_sdk_error(&ctx, e))
}

// ──── watch ────

fn watch_subscribe<'js>(ctx: &Ctx<'js>, sdk: Arc<PluginSdk>) -> rquickjs::Result<Function<'js>> {
    Function::new(
        ctx.clone(),
        Async(
            move |ctx: Ctx<'js>, key: Value<'js>, opts: Opt<Value<'js>>| {
                let sdk = Arc::clone(&sdk);
                async move { run_watch_subscribe(ctx, sdk, key, opts).await }
            },
        ),
    )
}

async fn run_watch_subscribe<'js>(
    ctx: Ctx<'js>,
    sdk: Arc<PluginSdk>,
    key: Value<'js>,
    opts: Opt<Value<'js>>,
) -> rquickjs::Result<Object<'js>> {
    let mut req = WatchSubscribe {
        key: js_bytes(&ctx, &key)?,
        range_end: Vec::new(),
        start_revision: 0,
        prev_kv: false,
    };
    if let Some(o) = opt_opts(&ctx, &opts)? {
        req.range_end = obj_bytes(&ctx, &o, "rangeEnd")?;
        req.start_revision = obj_i64(&o, "startRevision", 0)?;
        req.prev_kv = obj_bool(&o, "prevKv", false)?;
    }
    let id = sdk
        .watch_subscribe(req)
        .await
        .map_err(|e| throw_sdk_error(&ctx, e))?;

    let handle = Object::new(ctx.clone())?;
    handle.set("id", id)?;

    // next()：取下一条事件（null = 流结束）
    let next_sdk = Arc::clone(&sdk);
    handle.set(
        "next",
        Function::new(
            ctx.clone(),
            Async(move |ctx: Ctx<'js>| {
                let sdk = Arc::clone(&next_sdk);
                async move { run_watch_next(ctx, sdk, id).await }
            }),
        )?,
    )?;

    // close()：关闭订阅（幂等）
    let close_sdk = Arc::clone(&sdk);
    handle.set(
        "close",
        Function::new(
            ctx.clone(),
            Async(move |ctx: Ctx<'js>| {
                let sdk = Arc::clone(&close_sdk);
                async move { run_watch_close(ctx, sdk, id).await }
            }),
        )?,
    )?;
    Ok(handle)
}

async fn run_watch_close<'js>(ctx: Ctx<'js>, sdk: Arc<PluginSdk>, id: u64) -> rquickjs::Result<()> {
    sdk.watch_close(id)
        .await
        .map_err(|e| throw_sdk_error(&ctx, e))
}

// ──── storage ────

/// 对象元数据 → JS 对象。
fn object_stat_to_js<'js>(
    ctx: &Ctx<'js>,
    stat: &crate::plugin::sdk::ObjectStatDto,
) -> rquickjs::Result<Object<'js>> {
    let o = Object::new(ctx.clone())?;
    o.set("bucket", stat.bucket.clone())?;
    o.set("objectId", bytes_to_js(ctx, &stat.object_id)?)?;
    o.set("size", stat.size)?;
    o.set("chunks", stat.chunks)?;
    o.set("revision", stat.revision)?;
    o.set("exists", stat.exists)?;
    o.set("committed", stat.committed)?;
    Ok(o)
}

fn storage_put<'js>(ctx: &Ctx<'js>, sdk: Arc<PluginSdk>) -> rquickjs::Result<Function<'js>> {
    Function::new(
        ctx.clone(),
        Async(
            move |ctx: Ctx<'js>, bucket: Value<'js>, object_id: Value<'js>, data: Value<'js>| {
                let sdk = Arc::clone(&sdk);
                async move { run_storage_put(ctx, sdk, bucket, object_id, data).await }
            },
        ),
    )
}

async fn run_storage_put<'js>(
    ctx: Ctx<'js>,
    sdk: Arc<PluginSdk>,
    bucket: Value<'js>,
    object_id: Value<'js>,
    data: Value<'js>,
) -> rquickjs::Result<Object<'js>> {
    let bucket = js_string(&ctx, &bucket, "bucket")?;
    let object_id = js_bytes(&ctx, &object_id)?;
    let data = js_bytes(&ctx, &data)?;
    let out = sdk
        .storage_put(&bucket, &object_id, &data)
        .await
        .map_err(|e| throw_sdk_error(&ctx, e))?;
    let o = Object::new(ctx.clone())?;
    o.set("revision", out.revision)?;
    o.set("size", out.size)?;
    o.set("chunks", out.chunks)?;
    Ok(o)
}

fn storage_get<'js>(ctx: &Ctx<'js>, sdk: Arc<PluginSdk>) -> rquickjs::Result<Function<'js>> {
    Function::new(
        ctx.clone(),
        Async(
            move |ctx: Ctx<'js>, bucket: Value<'js>, object_id: Value<'js>| {
                let sdk = Arc::clone(&sdk);
                async move { run_storage_get(ctx, sdk, bucket, object_id).await }
            },
        ),
    )
}

async fn run_storage_get<'js>(
    ctx: Ctx<'js>,
    sdk: Arc<PluginSdk>,
    bucket: Value<'js>,
    object_id: Value<'js>,
) -> rquickjs::Result<Object<'js>> {
    let bucket = js_string(&ctx, &bucket, "bucket")?;
    let object_id = js_bytes(&ctx, &object_id)?;
    let out = sdk
        .storage_get(&bucket, &object_id)
        .await
        .map_err(|e| throw_sdk_error(&ctx, e))?;
    let o = Object::new(ctx.clone())?;
    o.set("stat", object_stat_to_js(&ctx, &out.stat)?)?;
    o.set("data", bytes_to_js(&ctx, &out.data)?)?;
    Ok(o)
}

fn storage_stat<'js>(ctx: &Ctx<'js>, sdk: Arc<PluginSdk>) -> rquickjs::Result<Function<'js>> {
    Function::new(
        ctx.clone(),
        Async(
            move |ctx: Ctx<'js>, bucket: Value<'js>, object_id: Value<'js>| {
                let sdk = Arc::clone(&sdk);
                async move { run_storage_stat(ctx, sdk, bucket, object_id).await }
            },
        ),
    )
}

/// `storage.stat`：不存在 → `null`（而非抛错）。
async fn run_storage_stat<'js>(
    ctx: Ctx<'js>,
    sdk: Arc<PluginSdk>,
    bucket: Value<'js>,
    object_id: Value<'js>,
) -> rquickjs::Result<Value<'js>> {
    let bucket = js_string(&ctx, &bucket, "bucket")?;
    let object_id = js_bytes(&ctx, &object_id)?;
    let stat = sdk
        .storage_stat(&bucket, &object_id)
        .await
        .map_err(|e| throw_sdk_error(&ctx, e))?;
    match stat {
        Some(stat) => object_stat_to_js(&ctx, &stat).and_then(|o| o.into_js(&ctx)),
        None => Ok(Value::new_null(ctx.clone())),
    }
}

fn storage_delete<'js>(ctx: &Ctx<'js>, sdk: Arc<PluginSdk>) -> rquickjs::Result<Function<'js>> {
    Function::new(
        ctx.clone(),
        Async(
            move |ctx: Ctx<'js>, bucket: Value<'js>, object_id: Value<'js>| {
                let sdk = Arc::clone(&sdk);
                async move { run_storage_delete(ctx, sdk, bucket, object_id).await }
            },
        ),
    )
}

async fn run_storage_delete<'js>(
    ctx: Ctx<'js>,
    sdk: Arc<PluginSdk>,
    bucket: Value<'js>,
    object_id: Value<'js>,
) -> rquickjs::Result<Object<'js>> {
    let bucket = js_string(&ctx, &bucket, "bucket")?;
    let object_id = js_bytes(&ctx, &object_id)?;
    let deleted = sdk
        .storage_delete(&bucket, &object_id)
        .await
        .map_err(|e| throw_sdk_error(&ctx, e))?;
    let o = Object::new(ctx.clone())?;
    o.set("deleted", deleted)?;
    Ok(o)
}

// ──── storage 流式会话（批次 10：分块读写，不整块驻留 guest 内存）────
//
// JS 没有 RAII：句柄对象持有会话 id，并提供显式 `abort` / `close`。
// 忘记显式收尾时，插件停止的 `PluginSdk::release()` 仍会按插件兜底回收
// （`CoordSdkBackend::release_plugin`）。

fn storage_open_write<'js>(ctx: &Ctx<'js>, sdk: Arc<PluginSdk>) -> rquickjs::Result<Function<'js>> {
    Function::new(
        ctx.clone(),
        Async(
            move |ctx: Ctx<'js>,
                  bucket: Value<'js>,
                  object_id: Value<'js>,
                  total_size: Opt<i64>| {
                let sdk = Arc::clone(&sdk);
                async move { run_storage_open_write(ctx, sdk, bucket, object_id, total_size).await }
            },
        ),
    )
}

async fn run_storage_open_write<'js>(
    ctx: Ctx<'js>,
    sdk: Arc<PluginSdk>,
    bucket: Value<'js>,
    object_id: Value<'js>,
    total_size: Opt<i64>,
) -> rquickjs::Result<Object<'js>> {
    let bucket = js_string(&ctx, &bucket, "bucket")?;
    let object_id = js_bytes(&ctx, &object_id)?;
    // 第三参可省略/为 0 → **未知长度**（commit 时定长）；负数非法。
    let total = total_size.0.unwrap_or(0);
    if total < 0 {
        return Err(Exception::throw_type(
            &ctx,
            "'totalSize' must be >= 0 (0 or omitted = unknown length)",
        ));
    }
    let total = total as u64;
    let id = sdk
        .storage_open_write(&bucket, &object_id, total)
        .await
        .map_err(|e| throw_sdk_error(&ctx, e))?;

    let handle = Object::new(ctx.clone())?;
    handle.set("id", id)?;
    if total == 0 {
        handle.set("totalSize", rquickjs::Null)?;
    } else {
        handle.set("totalSize", total)?;
    }

    // write(chunk)：随写随发；返回累计字节数
    let write_sdk = Arc::clone(&sdk);
    handle.set(
        "write",
        Function::new(
            ctx.clone(),
            Async(move |ctx: Ctx<'js>, chunk: Value<'js>| {
                let sdk = Arc::clone(&write_sdk);
                async move {
                    let chunk = js_bytes(&ctx, &chunk)?;
                    sdk.storage_write_chunk(id, &chunk)
                        .await
                        .map_err(|e| throw_sdk_error(&ctx, e))
                }
            }),
        )?,
    )?;

    let commit_sdk = Arc::clone(&sdk);
    handle.set(
        "commit",
        Function::new(
            ctx.clone(),
            Async(move |ctx: Ctx<'js>| {
                let sdk = Arc::clone(&commit_sdk);
                async move { run_storage_commit(ctx, sdk, id).await }
            }),
        )?,
    )?;

    let abort_sdk = Arc::clone(&sdk);
    handle.set(
        "abort",
        Function::new(
            ctx.clone(),
            Async(move |ctx: Ctx<'js>| {
                let sdk = Arc::clone(&abort_sdk);
                async move { run_storage_abort(ctx, sdk, id).await }
            }),
        )?,
    )?;
    Ok(handle)
}

async fn run_storage_commit<'js>(
    ctx: Ctx<'js>,
    sdk: Arc<PluginSdk>,
    id: u64,
) -> rquickjs::Result<Object<'js>> {
    let out = sdk
        .storage_commit_write(id)
        .await
        .map_err(|e| throw_sdk_error(&ctx, e))?;
    let o = Object::new(ctx.clone())?;
    o.set("revision", out.revision)?;
    o.set("size", out.size)?;
    o.set("chunks", out.chunks)?;
    Ok(o)
}

async fn run_storage_abort<'js>(
    ctx: Ctx<'js>,
    sdk: Arc<PluginSdk>,
    id: u64,
) -> rquickjs::Result<()> {
    sdk.storage_abort_write(id)
        .await
        .map_err(|e| throw_sdk_error(&ctx, e))
}

fn storage_open_read<'js>(ctx: &Ctx<'js>, sdk: Arc<PluginSdk>) -> rquickjs::Result<Function<'js>> {
    Function::new(
        ctx.clone(),
        Async(
            move |ctx: Ctx<'js>, bucket: Value<'js>, object_id: Value<'js>| {
                let sdk = Arc::clone(&sdk);
                async move { run_storage_open_read(ctx, sdk, bucket, object_id).await }
            },
        ),
    )
}

async fn run_storage_open_read<'js>(
    ctx: Ctx<'js>,
    sdk: Arc<PluginSdk>,
    bucket: Value<'js>,
    object_id: Value<'js>,
) -> rquickjs::Result<Object<'js>> {
    let bucket = js_string(&ctx, &bucket, "bucket")?;
    let object_id = js_bytes(&ctx, &object_id)?;
    let id = sdk
        .storage_open_read(&bucket, &object_id)
        .await
        .map_err(|e| throw_sdk_error(&ctx, e))?;

    let handle = Object::new(ctx.clone())?;
    handle.set("id", id)?;

    // stat()：打开时已取得的元数据
    let stat_sdk = Arc::clone(&sdk);
    handle.set(
        "stat",
        Function::new(
            ctx.clone(),
            Async(move |ctx: Ctx<'js>| {
                let sdk = Arc::clone(&stat_sdk);
                async move { run_storage_reader_stat(ctx, sdk, id).await }
            }),
        )?,
    )?;

    // read(maxLen)：返回 ≤ maxLen 字节；null = 读完
    let read_sdk = Arc::clone(&sdk);
    handle.set(
        "read",
        Function::new(
            ctx.clone(),
            Async(move |ctx: Ctx<'js>, max_len: Value<'js>| {
                let sdk = Arc::clone(&read_sdk);
                async move { run_storage_read(ctx, sdk, id, max_len).await }
            }),
        )?,
    )?;

    let close_sdk = Arc::clone(&sdk);
    handle.set(
        "close",
        Function::new(
            ctx.clone(),
            Async(move |ctx: Ctx<'js>| {
                let sdk = Arc::clone(&close_sdk);
                async move { run_storage_close_read(ctx, sdk, id).await }
            }),
        )?,
    )?;
    Ok(handle)
}

async fn run_storage_reader_stat<'js>(
    ctx: Ctx<'js>,
    sdk: Arc<PluginSdk>,
    id: u64,
) -> rquickjs::Result<Object<'js>> {
    let stat = sdk
        .storage_reader_stat(id)
        .await
        .map_err(|e| throw_sdk_error(&ctx, e))?;
    object_stat_to_js(&ctx, &stat)
}

async fn run_storage_read<'js>(
    ctx: Ctx<'js>,
    sdk: Arc<PluginSdk>,
    id: u64,
    max_len: Value<'js>,
) -> rquickjs::Result<Value<'js>> {
    let max = js_i64(&max_len, 0);
    if max <= 0 {
        return Err(Exception::throw_type(
            &ctx,
            "'maxLen' must be a positive integer",
        ));
    }
    match sdk
        .storage_read_chunk(id, max as u64)
        .await
        .map_err(|e| throw_sdk_error(&ctx, e))?
    {
        Some(chunk) => bytes_to_js(&ctx, &chunk),
        None => Ok(Value::new_null(ctx.clone())),
    }
}

async fn run_storage_close_read<'js>(
    ctx: Ctx<'js>,
    sdk: Arc<PluginSdk>,
    id: u64,
) -> rquickjs::Result<()> {
    sdk.storage_close_read(id)
        .await
        .map_err(|e| throw_sdk_error(&ctx, e))
}

/// JS 值 → 必填字符串（非字符串 → 类型错误）。
fn js_string<'js>(ctx: &Ctx<'js>, v: &Value<'js>, field: &str) -> rquickjs::Result<String> {
    match v.as_string() {
        Some(s) => s.to_string(),
        None => Err(Exception::throw_type(
            ctx,
            &format!("'{field}' must be a string"),
        )),
    }
}

async fn run_watch_next<'js>(
    ctx: Ctx<'js>,
    sdk: Arc<PluginSdk>,
    id: u64,
) -> rquickjs::Result<Value<'js>> {
    let event = sdk
        .watch_next(id)
        .await
        .map_err(|e| throw_sdk_error(&ctx, e))?;
    let Some(event) = event else {
        return Ok(Value::new_null(ctx.clone()));
    };
    let obj = Object::new(ctx.clone())?;
    obj.set("type", event.kind.as_str())?;
    obj.set("revision", event.revision)?;
    obj.set("kvs", records_to_js(&ctx, &event.kvs)?)?;
    if let Some(prev) = &event.prev_kv {
        obj.set("prevKv", record_to_js(&ctx, prev)?)?;
    }
    obj.into_js(&ctx)
}

// ──── log ────

fn log_fn<'js>(ctx: &Ctx<'js>) -> rquickjs::Result<Function<'js>> {
    Function::new(ctx.clone(), |level: Opt<String>, msg: Opt<Value<'_>>| {
        let level = level.0.unwrap_or_else(|| "info".to_string());
        let msg = msg
            .0
            .and_then(|v| {
                v.as_string()
                    .and_then(|s| s.to_string().ok())
                    .or_else(|| Some(format!("{v:?}")))
            })
            .unwrap_or_default();
        match level.as_str() {
            "error" => tracing::error!(target: "coord_agent::plugin", "{msg}"),
            "warn" | "warning" => tracing::warn!(target: "coord_agent::plugin", "{msg}"),
            "debug" => tracing::debug!(target: "coord_agent::plugin", "{msg}"),
            "trace" => tracing::trace!(target: "coord_agent::plugin", "{msg}"),
            _ => tracing::info!(target: "coord_agent::plugin", "{msg}"),
        }
    })
}
