//! 参考 guest：coord-agent **组件模型** wasm 插件的 ABI 夹具（测试用）。
//!
//! 与 `coord-agent/src/plugin/component_engine.rs` 的宿主实现成对存在：
//! 夹具覆盖宿主 import 的每一条路径（含 watch 阻塞式 `next` 的 fiber 异步桥），
//! 生成的 `.wasm` 提交到 `tests/fixtures/coord-plugin-guest.wasm`，由集成测试加载。
//!
//! 重新构建：
//! ```sh
//! scripts/build-plugin-guest.sh
//! ```
//!
//! 注意：本 crate **不是** workspace 成员（`[workspace]` 自声明），只按需构建。

wit_bindgen::generate!({
    path: "../../../wit",
    world: "plugin",
});

use exports::coord::plugin::guest::Guest;
use coord::plugin::host as host_api;
use std::cell::RefCell;

struct Component;

thread_local! {
    /// 跨调用**存活**的订阅句柄（验证宿主在实例销毁 / 插件停止时的兜底回收）。
    static HELD: RefCell<Vec<host_api::Subscription>> = const { RefCell::new(Vec::new()) };
}

/// 方法名 → 行为；返回原始字节（错误 → `Err(消息)`，宿主映射为插件级错误，
/// 与 trap 可区分）。
impl Guest for Component {
    fn init() -> Result<(), String> {
        host_api::log("info", "coord-plugin-guest: init");
        // 只读预算：确认 `limits()` 可用（插件据此提前失败）
        let limits = host_api::limits();
        host_api::log(
            "debug",
            &format!(
                "limits: max_exec_ms={} max_memory_mb={} max_objects={}",
                limits.max_exec_ms, limits.max_memory_mb, limits.max_objects
            ),
        );
        Ok(())
    }

    fn handle_invoke(method: String, payload: Vec<u8>) -> Result<Vec<u8>, String> {
        let text = String::from_utf8(payload.clone()).unwrap_or_default();
        match method.as_str() {
            // 纯计算（不触碰宿主能力）：ABI 往返验证
            "echo" => Ok(payload),

            // ── KV ──

            // payload = `key=value`；返回 `rev:<revision>`。
            "kv-put" => {
                let (key, value) = split_pair(&text)?;
                let out = host_api::kv_put(key.as_bytes(), value.as_bytes(), 0, false, None)
                    .map_err(fmt_err)?;
                Ok(format!("rev:{}", out.revision).into_bytes())
            }

            // payload = `key`；返回值（不存在 → `missing`）。
            "kv-get" => match host_api::kv_get(text.as_bytes()) {
                Ok(value) => Ok(value),
                Err(host_api::SdkError::NotFound(_)) => Ok(b"missing".to_vec()),
                Err(e) => Err(fmt_err(e)),
            },

            // payload = `key=value`；create-if-absent（CAS）。
            // 返回 `ok:<revision>` 或 `conflict`。
            "kv-create" => {
                let (key, value) = split_pair(&text)?;
                match host_api::kv_create(key.as_bytes(), value.as_bytes(), 0) {
                    Ok(rev) => Ok(format!("ok:{rev}").into_bytes()),
                    Err(host_api::SdkError::Conflict(_)) => Ok(b"conflict".to_vec()),
                    Err(e) => Err(fmt_err(e)),
                }
            }

            // payload = `key=value`；txn：compare `version == 0` → put。
            // 返回 `succeeded` / `failed`。
            "txn-create" => {
                let (key, value) = split_pair(&text)?;
                let out = host_api::txn(&host_api::TxnRequest {
                    compares: vec![host_api::Compare {
                        op: host_api::CompareOp::Equal,
                        target: host_api::CompareTarget::Version,
                        key: key.as_bytes().to_vec(),
                        int_value: 0,
                        bytes_value: Vec::new(),
                    }],
                    success: vec![host_api::TxnOp::Put(host_api::KvPutOp {
                        key: key.as_bytes().to_vec(),
                        value: value.as_bytes().to_vec(),
                        lease_id: 0,
                        prev_kv: false,
                    })],
                    failure: Vec::new(),
                    request_id: None,
                })
                .map_err(fmt_err)?;
                Ok(if out.succeeded {
                    b"succeeded".to_vec()
                } else {
                    b"failed".to_vec()
                })
            }

            // ── 租约 ──

            // payload = ttl 秒数；返回 `lease:<id>`。
            "lease-grant" => {
                let ttl: i64 = text.trim().parse().map_err(|_| "ttl must be an integer")?;
                let lease = host_api::lease_grant(ttl).map_err(fmt_err)?;
                // 保活 + 立即释放：验证两条路径都通
                host_api::lease_keep_alive(lease.id).map_err(fmt_err)?;
                host_api::lease_revoke(lease.id).map_err(fmt_err)?;
                Ok(format!("lease:{}", lease.id).into_bytes())
            }

            // ── Watch（RAII 句柄 + 阻塞式 next：宿主侧是 fiber 挂起的异步 await）──

            // payload = `key`；订阅并取**第一条**事件，返回 `type:value`。
            "watch-first" => {
                let sub = host_api::watch_subscribe(text.as_bytes(), &[], 0, true)
                    .map_err(fmt_err)?;
                let event = sub.next().map_err(fmt_err)?;
                let out = match event {
                    Some(ev) => {
                        let value = ev
                            .kvs
                            .first()
                            .map(|kv| String::from_utf8_lossy(&kv.value).into_owned())
                            .unwrap_or_default();
                        format!("{}:{value}", kind_str(ev.kind))
                    }
                    None => "closed".to_string(),
                };
                sub.close().map_err(fmt_err)?;
                Ok(out.into_bytes())
            }

            // payload = `key`；**不 close 直接 drop** 句柄 → 宿主侧的 `drop`
            // 必须立刻释放订阅（RAII 的核心收益：忘记 close 不泄漏）。
            "watch-drop" => {
                let sub = host_api::watch_subscribe(text.as_bytes(), &[], 0, true)
                    .map_err(fmt_err)?;
                drop(sub);
                Ok(b"dropped".to_vec())
            }

            // payload = 次数 N；连续订阅 N 次并**保持存活**（Vec 持句柄），
            // 返回实际成功条数。验证两件事：
            // ① 句柄表容量 = manifest 的 `max_objects`（越界 → resource-exhausted）；
            // ② 调用返回时局部句柄离开作用域 → 宿主 `drop` 把订阅全部释放。
            "watch-many" => {
                let n: usize = text.trim().parse().map_err(|_| "count must be an integer")?;
                let mut held = Vec::new();
                for _ in 0..n {
                    match host_api::watch_subscribe(b"/app/kv/watch-many", &[], 0, true) {
                        Ok(sub) => held.push(sub),
                        Err(host_api::SdkError::ResourceExhausted(_)) => break,
                        Err(e) => return Err(fmt_err(e)),
                    }
                }
                Ok(format!("{}", held.len()).into_bytes())
            }

            // payload = `key`；订阅并**跨调用持有**句柄（不 close、不 drop），
            // 返回 `held:<n>`。用于验证插件停止时宿主对残留句柄的兜底回收。
            "watch-hold" => {
                let sub = host_api::watch_subscribe(text.as_bytes(), &[], 0, true)
                    .map_err(fmt_err)?;
                let n = HELD.with(|held| {
                    let mut held = held.borrow_mut();
                    held.push(sub);
                    held.len()
                });
                Ok(format!("held:{n}").into_bytes())
            }

            // 丢掉全部跨调用持有的句柄（离开作用域 → 宿主 drop）→ `released`。
            "watch-release" => {
                let n = HELD.with(|held| held.borrow_mut().len());
                HELD.with(|held| held.borrow_mut().clear());
                Ok(format!("released:{n}").into_bytes())
            }

            // payload = `key`；显式 close 两次（幂等）→ `closed`。
            "watch-close-twice" => {
                let sub = host_api::watch_subscribe(text.as_bytes(), &[], 0, true)
                    .map_err(fmt_err)?;
                sub.close().map_err(fmt_err)?;
                sub.close().map_err(fmt_err)?;
                Ok(b"closed".to_vec())
            }

            // payload = `key`；close 之后 `next` → 宿主应返回 `invalid-argument`
            // （句柄仍然合法，但已释放）。
            "watch-next-after-close" => {
                let sub = host_api::watch_subscribe(text.as_bytes(), &[], 0, true)
                    .map_err(fmt_err)?;
                sub.close().map_err(fmt_err)?;
                match sub.next() {
                    Err(host_api::SdkError::InvalidArgument(_)) => Ok(b"invalid".to_vec()),
                    Err(e) => Err(fmt_err(e)),
                    Ok(_) => Ok(b"unexpected".to_vec()),
                }
            }

            // ── 对象存储 ──

            // payload = `bucket/object=内容`；put → get → stat 往返。
            "storage-roundtrip" => {
                let (path, data) = split_pair(&text)?;
                let (bucket, object) = path
                    .split_once('/')
                    .ok_or("path must be `bucket/object`")?;
                let put = host_api::storage_put(bucket, object.as_bytes(), data.as_bytes())
                    .map_err(fmt_err)?;
                let got = host_api::storage_get(bucket, object.as_bytes()).map_err(fmt_err)?;
                if got != data.as_bytes() {
                    return Err("storage roundtrip mismatch".into());
                }
                let stat = host_api::storage_stat(bucket, object.as_bytes())
                    .map_err(fmt_err)?
                    .ok_or("stat returned none after put")?;
                if !stat.exists || !stat.committed {
                    return Err("stat says object is not committed".into());
                }
                Ok(format!("size:{}", put.size).into_bytes())
            }

            // payload = `bucket/object=内容`；**分块**上传（3B/块）→**分块**下载
            // （每次 ≤4B）。覆盖 RAII 会话：`uploader.write/commit`、
            // `downloader.stat/read/close` 与「单次读取 ≤ max-len」的截断/暂存语义。
            "storage-stream-roundtrip" => {
                let (path, data) = split_pair(&text)?;
                let (bucket, object) =
                    path.split_once('/').ok_or("path must be `bucket/object`")?;
                let total = data.len() as u64;

                let writer = host_api::storage_open_write(bucket, object.as_bytes(), total)
                    .map_err(fmt_err)?;
                let mut written = 0u64;
                for chunk in data.as_bytes().chunks(3) {
                    written = writer.write(chunk).map_err(fmt_err)?;
                }
                if written != total {
                    return Err(format!("cumulative write count {written} != {total}"));
                }
                let put = writer.commit().map_err(fmt_err)?;

                let reader =
                    host_api::storage_open_read(bucket, object.as_bytes()).map_err(fmt_err)?;
                let stat = reader.stat().map_err(fmt_err)?;
                if stat.size != total as i64 || !stat.committed {
                    return Err(format!("bad stat after streaming put: size={}", stat.size));
                }
                let mut got = Vec::new();
                loop {
                    match reader.read(4).map_err(fmt_err)? {
                        Some(chunk) => got.extend_from_slice(&chunk),
                        None => break,
                    }
                }
                reader.close().map_err(fmt_err)?;
                if got != data.as_bytes() {
                    return Err(format!(
                        "stream roundtrip mismatch (got {} bytes, want {})",
                        got.len(),
                        data.len()
                    ));
                }
                Ok(format!("size:{}", put.size).into_bytes())
            }

            // payload = `bucket/object`；打开下载会话但对象不存在 → `missing`。
            "storage-stream-read-missing" => {
                let (bucket, object) = text
                    .split_once('/')
                    .ok_or("payload must be `bucket/object`")?;
                match host_api::storage_open_read(bucket, object.as_bytes()) {
                    Ok(_) => Ok(b"unexpected".to_vec()),
                    Err(host_api::SdkError::NotFound(_)) => Ok(b"missing".to_vec()),
                    Err(e) => Err(fmt_err(e)),
                }
            }

            // payload = `bucket/object`；声明 total-size=4 却写 5 字节 → `invalid`
            // （宿主 / 门面在越界那一刻拒绝；句柄随后 drop → 宿主兜底中止上传）。
            "storage-stream-overflow" => {
                let (bucket, object) = text
                    .split_once('/')
                    .ok_or("payload must be `bucket/object`")?;
                let writer =
                    host_api::storage_open_write(bucket, object.as_bytes(), 4).map_err(fmt_err)?;
                writer.write(b"abc").map_err(fmt_err)?;
                match writer.write(b"de") {
                    Err(host_api::SdkError::InvalidArgument(_)) => Ok(b"invalid".to_vec()),
                    Err(e) => Err(fmt_err(e)),
                    Ok(_) => Ok(b"unexpected".to_vec()),
                }
            }

            // payload = `bucket/object`；**未知长度**（total-size=0）：
            // 逐块写 8B × 5 = 40B，commit 时按实际字节定长 → `size:40`。
            "storage-stream-unknown" => {
                let (bucket, object) = text
                    .split_once('/')
                    .ok_or("payload must be `bucket/object`")?;
                let writer =
                    host_api::storage_open_write(bucket, object.as_bytes(), 0).map_err(fmt_err)?;
                let mut written = 0u64;
                for _ in 0..5 {
                    written = writer.write(b"12345678").map_err(fmt_err)?;
                }
                if written != 40 {
                    return Err(format!("cumulative write count {written} != 40"));
                }
                let put = writer.commit().map_err(fmt_err)?;
                // 注意：此夹具跑在测试桩后端上，`chunks` 由桩固定为 1；
                // 这里只断言「未知长度在提交时被定长」（真实 chunk 计数由
                // 进程级用例覆盖）。
                if put.size != 40 {
                    return Err(format!(
                        "bad commit for unknown-length put: size={} chunks={}",
                        put.size, put.chunks
                    ));
                }
                // 读回：stat.size 必须在 commit 时定长（而非 0）
                let reader =
                    host_api::storage_open_read(bucket, object.as_bytes()).map_err(fmt_err)?;
                let stat = reader.stat().map_err(fmt_err)?;
                if stat.size != 40 || !stat.committed {
                    return Err(format!(
                        "bad stat after unknown-length put: size={}",
                        stat.size
                    ));
                }
                let mut got = Vec::new();
                while let Some(chunk) = reader.read(7).map_err(fmt_err)? {
                    got.extend_from_slice(&chunk);
                }
                reader.close().map_err(fmt_err)?;
                if got != b"12345678".repeat(5) {
                    return Err("unknown-length roundtrip mismatch".into());
                }
                Ok(format!("size:{}", put.size).into_bytes())
            }

            // ── env / log ──

            // payload = env key；返回值（缺失 → 插件级错误）。
            "env" => host_api::env(&text).map(String::into_bytes).map_err(fmt_err),

            "log" => {
                host_api::log("info", &text);
                Ok(b"logged".to_vec())
            }

            // ── 沙箱验证钩子 ──

            // 自旋到被 fuel / epoch 打断（宿主应计 trap，且不拖垮进程）。
            "spin" => loop {
                std::hint::spin_loop();
            },

            // 申请超大内存（宿主 `StoreLimits` 应拒绝增长 →
            // `Vec` 分配 panic → trap）。
            "alloc" => {
                let n: usize = text.trim().parse().unwrap_or(4 * 1024 * 1024);
                let v = vec![7u8; n];
                Ok(vec![v.len() as u8])
            }

            // 越界的能力调用（宿主守卫应拒 `forbidden`）。
            "forbidden" => match host_api::kv_put(
                text.as_bytes(),
                b"nope",
                0,
                false,
                None,
            ) {
                Ok(out) => Ok(format!("allowed:{}", out.revision).into_bytes()),
                Err(host_api::SdkError::Forbidden(_)) => Ok(b"forbidden".to_vec()),
                Err(e) => Err(fmt_err(e)),
            },

            other => Err(format!("unknown method '{other}'")),
        }
    }

    fn stop() {
        host_api::log("info", "coord-plugin-guest: stop");
    }
}

fn split_pair(text: &str) -> Result<(&str, &str), String> {
    text.split_once('=')
        .ok_or_else(|| format!("payload must be `key=value`, got '{text}'"))
}

fn kind_str(kind: host_api::WatchEventKind) -> &'static str {
    match kind {
        host_api::WatchEventKind::Put => "PUT",
        host_api::WatchEventKind::Delete => "DELETE",
        host_api::WatchEventKind::BufferOverflow => "BUFFER_OVERFLOW",
        host_api::WatchEventKind::HistoryUnavailable => "HISTORY_UNAVAILABLE",
    }
}

/// `sdk-error` → 人类可读消息（与 JS / core ABI 路径的错误名一致）。
fn fmt_err(e: host_api::SdkError) -> String {
    match e {
        host_api::SdkError::Forbidden(m) => format!("ErrForbidden: {m}"),
        host_api::SdkError::NotFound(m) => format!("ErrNotFound: {m}"),
        host_api::SdkError::InvalidArgument(m) => format!("ErrInvalidArgument: {m}"),
        host_api::SdkError::Unavailable(m) => format!("ErrUnavailable: {m}"),
        host_api::SdkError::ResourceExhausted(m) => format!("ErrResourceExhausted: {m}"),
        host_api::SdkError::Conflict(m) => format!("ErrConflict: {m}"),
        host_api::SdkError::Internal(m) => format!("ErrInternal: {m}"),
    }
}

export!(Component);
