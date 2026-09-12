// 组件模型 wasm 插件端到端测试（Phase 4.1 / 计划 §11 Phase 4）
//
// 与 `wasm_engine.rs` 的单测（core ABI + 内联 WAT 夹具）互补：
// 本文件加载 **真实组件**（`tests/fixtures/coord-plugin-guest.wasm`，由
// `scripts/build-plugin-guest.sh` 用 `wasm32-unknown-unknown` +
// `wasm-tools component embed/new` 产出，已提交仓库）走**完整公开面**：
// `WasmPluginLoader`（ABI 自动判别）→ `PluginSdk` 门面 → stub 后端。
//
// 覆盖的 4.1 验收点：
// 1. ABI 判别：组件二进制走组件宿主（而非 core ABI）；
// 2. **无 WASI**：夹具 import 面只有 `coord:plugin/host`；
// 3. **异步宿主 import**：宿主函数在 tokio 上 await（watch 的阻塞式 `next`
//    由宿主侧 sleep 后回填 → 证明 fiber 挂起/恢复成立，而非 fake 同步返回）；
// 4. 类型安全 ABI：typed record / variant 往返（kv / txn / storage / watch）；
// 5. 能力边界：`PluginSdk` 作用域守卫在组件路径同样 fail-closed；
// 6. 沙箱：fuel/epoch 超时、内存上限 trap 隔离；插件级错误不杀实例。
//
// 不使用 `plugin-wasm` feature 时整个测试 crate 为空（CI feature 矩阵覆盖）。
#![cfg(feature = "plugin-wasm")]

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;

use coord_agent::plugin::sdk::backend::{
    CompareTarget, KvDelete, KvDeleteOut, KvPut, KvPutOut, KvRange, KvRangeOut, KvRecord,
    ObjectGetOut, ObjectPutOut, ObjectStatDto, PluginSdkBackend, SdkError, SdkResult, TxnOp,
    TxnOpOut, TxnOut, TxnReq, WatchEventDto, WatchEventKind, WatchSubscribe, CAP_KV_DELETE,
    CAP_KV_READ, CAP_KV_WRITE, CAP_LEASE_GRANT, CAP_LEASE_KEEPALIVE, CAP_LEASE_REVOKE,
    CAP_STORAGE_READ, CAP_STORAGE_WRITE, CAP_TXN_EXECUTE, CAP_WATCH_SUBSCRIBE,
};
use coord_agent::plugin::{
    Plugin, PluginCapability, PluginLimits, PluginLoader, PluginManifest, PluginRuntime,
    PluginStatus, PluginTrust, WasmPluginLoader,
};

/// 提交在仓库里的组件夹具（`scripts/build-plugin-guest.sh` 产出）。
const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/coord-plugin-guest.wasm"
);
const FIXTURE_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");
const FIXTURE_NAME: &str = "coord-plugin-guest.wasm";

// ──── stub 后端：内存 KV（version CAS）+ 租约 + 可投递 watch + 对象 ────

/// `key → (值, 版本)`（version 用于 create-if-absent CAS）
type KvMap = BTreeMap<Vec<u8>, (Vec<u8>, i64)>;
/// `(bucket, object_id) → 对象字节`
type ObjectMap = BTreeMap<(String, Vec<u8>), Vec<u8>>;

#[derive(Default)]
struct StubBackend {
    kvs: parking_lot::Mutex<KvMap>,
    objects: parking_lot::Mutex<ObjectMap>,
    /// 待投递的 watch 事件（`watch_next` 每次取一条）
    events: parking_lot::Mutex<std::collections::VecDeque<WatchEventDto>>,
    revision: parking_lot::Mutex<i64>,
    lease_seq: parking_lot::Mutex<i64>,
    /// stub 侧记录到的写入（断言「宿主调用真的发生了」）
    puts: parking_lot::Mutex<Vec<(Vec<u8>, Vec<u8>)>>,
    /// 下次分配的订阅 id（每次 subscribe 递增，便于断言句柄一一对应）
    next_subscription: parking_lot::Mutex<u64>,
    /// 已释放的订阅 id（RAII 断言：drop / close 都会到这里）
    closes: parking_lot::Mutex<Vec<u64>>,
    /// 活跃的**上传会话**（分块累积到内存）
    uploads: parking_lot::Mutex<BTreeMap<u64, StubUpload>>,
    /// 活跃的**下载会话**（游标）
    downloads: parking_lot::Mutex<BTreeMap<u64, StubDownload>>,
    /// 会话 id 分配器（上传 / 下载共用一个空间）
    next_session: parking_lot::Mutex<u64>,
    /// 已中止的上传 id（RAII 断言：显式 abort / drop 都会到这里）
    aborted_uploads: parking_lot::Mutex<Vec<u64>>,
    /// 已关闭的下载 id（同上）
    closed_downloads: parking_lot::Mutex<Vec<u64>>,
}

/// stub 上传会话：累积字节，`commit` 时才落进 `objects`。
struct StubUpload {
    bucket: String,
    object_id: Vec<u8>,
    total: u64,
    buf: Vec<u8>,
}

/// stub 下载会话：对已落盘对象开一个游标。
struct StubDownload {
    bucket: String,
    object_id: Vec<u8>,
    data: Vec<u8>,
    pos: usize,
}

impl StubBackend {
    fn next_revision(&self) -> i64 {
        let mut r = self.revision.lock();
        *r += 1;
        *r
    }

    fn push_event(&self, kind: WatchEventKind, key: &[u8], value: &[u8]) {
        let revision = self.next_revision();
        self.events.lock().push_back(WatchEventDto {
            kind,
            kvs: vec![KvRecord {
                key: key.to_vec(),
                value: value.to_vec(),
                lease_id: 0,
                version: 1,
            }],
            prev_kv: None,
            revision,
        });
    }
}

#[async_trait]
impl PluginSdkBackend for StubBackend {
    async fn kv_put(&self, _plugin: &str, req: KvPut) -> SdkResult<KvPutOut> {
        self.puts.lock().push((req.key.clone(), req.value.clone()));
        let revision = self.next_revision();
        let mut kvs = self.kvs.lock();
        match kvs.get_mut(&req.key) {
            Some(e) => {
                e.0 = req.value;
                e.1 += 1;
            }
            None => {
                kvs.insert(req.key, (req.value, 1));
            }
        }
        Ok(KvPutOut {
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
                    responses.push(TxnOpOut::Put(KvPutOut {
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
                TxnOp::Range(_) => responses.push(TxnOpOut::Range(KvRangeOut {
                    kvs: Vec::new(),
                    count: 0,
                    revision,
                })),
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
        let mut next = self.next_subscription.lock();
        *next += 1;
        Ok(*next)
    }

    /// **关键**：宿主侧真的 await（sleep 之后才回填）—— guest 的阻塞式 `next`
    /// 必须经过 wasmtime fiber 挂起/恢复才能拿到这个值。若宿主是假同步返回，
    /// 本测试无法通过（时间断言）。
    async fn watch_next(&self, _plugin: &str, _id: u64) -> SdkResult<Option<WatchEventDto>> {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        Ok(self.events.lock().pop_front())
    }

    async fn watch_close(&self, _plugin: &str, id: u64) -> SdkResult<()> {
        self.closes.lock().push(id);
        Ok(())
    }

    async fn storage_put(
        &self,
        _plugin: &str,
        bucket: &str,
        object_id: &[u8],
        data: &[u8],
    ) -> SdkResult<ObjectPutOut> {
        self.objects
            .lock()
            .insert((bucket.to_string(), object_id.to_vec()), data.to_vec());
        Ok(ObjectPutOut {
            revision: self.next_revision(),
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
        let key = (bucket.to_string(), object_id.to_vec());
        let objects = self.objects.lock();
        let data = objects
            .get(&key)
            .cloned()
            .ok_or_else(|| SdkError::not_found("object not found"))?;
        Ok(ObjectGetOut {
            stat: ObjectStatDto {
                bucket: bucket.to_string(),
                object_id: object_id.to_vec(),
                size: data.len() as i64,
                chunks: 1,
                revision: 1,
                exists: true,
                committed: true,
            },
            data,
        })
    }

    async fn storage_stat(
        &self,
        _plugin: &str,
        bucket: &str,
        object_id: &[u8],
    ) -> SdkResult<Option<ObjectStatDto>> {
        let key = (bucket.to_string(), object_id.to_vec());
        Ok(self.objects.lock().get(&key).map(|data| ObjectStatDto {
            bucket: bucket.to_string(),
            object_id: object_id.to_vec(),
            size: data.len() as i64,
            chunks: 1,
            revision: 1,
            exists: true,
            committed: true,
        }))
    }

    async fn storage_delete(
        &self,
        _plugin: &str,
        bucket: &str,
        object_id: &[u8],
    ) -> SdkResult<bool> {
        Ok(self
            .objects
            .lock()
            .remove(&(bucket.to_string(), object_id.to_vec()))
            .is_some())
    }

    // ──── 流式会话（批次 10）────

    async fn storage_open_write(
        &self,
        _plugin: &str,
        bucket: &str,
        object_id: &[u8],
        total_size: u64,
    ) -> SdkResult<u64> {
        let id = {
            let mut n = self.next_session.lock();
            *n += 1;
            *n
        };
        self.uploads.lock().insert(
            id,
            StubUpload {
                bucket: bucket.to_string(),
                object_id: object_id.to_vec(),
                total: total_size,
                buf: Vec::new(),
            },
        );
        Ok(id)
    }

    async fn storage_write_chunk(&self, _plugin: &str, id: u64, data: &[u8]) -> SdkResult<u64> {
        let mut uploads = self.uploads.lock();
        let up = uploads
            .get_mut(&id)
            .ok_or_else(|| SdkError::not_found("upload session not found"))?;
        if up.total != 0 && up.buf.len() as u64 + data.len() as u64 > up.total {
            return Err(SdkError::invalid_argument(
                "chunk overflows declared total_size",
            ));
        }
        up.buf.extend_from_slice(data);
        Ok(up.buf.len() as u64)
    }

    async fn storage_commit_write(&self, _plugin: &str, id: u64) -> SdkResult<ObjectPutOut> {
        let up = self
            .uploads
            .lock()
            .remove(&id)
            .ok_or_else(|| SdkError::not_found("upload session not found"))?;
        if up.total == 0 {
            if up.buf.is_empty() {
                return Err(SdkError::invalid_argument(
                    "unknown-length upload must write at least one chunk",
                ));
            }
        } else if up.buf.len() as u64 != up.total {
            return Err(SdkError::invalid_argument(
                "written bytes != declared total_size",
            ));
        }
        self.objects
            .lock()
            .insert((up.bucket.clone(), up.object_id.clone()), up.buf.clone());
        Ok(ObjectPutOut {
            revision: self.next_revision(),
            size: up.buf.len() as i64,
            chunks: 1,
        })
    }

    async fn storage_abort_write(&self, _plugin: &str, id: u64) -> SdkResult<()> {
        self.uploads.lock().remove(&id);
        self.aborted_uploads.lock().push(id);
        Ok(())
    }

    async fn storage_open_read(
        &self,
        _plugin: &str,
        bucket: &str,
        object_id: &[u8],
    ) -> SdkResult<u64> {
        let data = self
            .objects
            .lock()
            .get(&(bucket.to_string(), object_id.to_vec()))
            .cloned()
            .ok_or_else(|| SdkError::not_found("object not found"))?;
        let id = {
            let mut n = self.next_session.lock();
            *n += 1;
            *n
        };
        self.downloads.lock().insert(
            id,
            StubDownload {
                bucket: bucket.to_string(),
                object_id: object_id.to_vec(),
                data,
                pos: 0,
            },
        );
        Ok(id)
    }

    async fn storage_read_chunk(
        &self,
        _plugin: &str,
        id: u64,
        max_len: u64,
    ) -> SdkResult<Option<Vec<u8>>> {
        let mut downloads = self.downloads.lock();
        let d = downloads
            .get_mut(&id)
            .ok_or_else(|| SdkError::not_found("download session not found"))?;
        if d.pos >= d.data.len() {
            return Ok(None);
        }
        let end = (d.pos + max_len as usize).min(d.data.len());
        let chunk = d.data[d.pos..end].to_vec();
        d.pos = end;
        Ok(Some(chunk))
    }

    async fn storage_reader_stat(&self, _plugin: &str, id: u64) -> SdkResult<ObjectStatDto> {
        let downloads = self.downloads.lock();
        let d = downloads
            .get(&id)
            .ok_or_else(|| SdkError::not_found("download session not found"))?;
        Ok(ObjectStatDto {
            bucket: d.bucket.clone(),
            object_id: d.object_id.clone(),
            size: d.data.len() as i64,
            chunks: 1,
            revision: 1,
            exists: true,
            committed: true,
        })
    }

    async fn storage_close_read(&self, _plugin: &str, id: u64) -> SdkResult<()> {
        self.downloads.lock().remove(&id);
        self.closed_downloads.lock().push(id);
        Ok(())
    }
}

// ──── 夹具装配 ────

fn manifest(
    name: &str,
    capabilities: Vec<PluginCapability>,
    limits: PluginLimits,
) -> PluginManifest {
    PluginManifest {
        name: name.into(),
        version: "0.1.0".into(),
        runtime: PluginRuntime::Wasm,
        trust: PluginTrust::ThirdParty,
        entry: FIXTURE_NAME.into(),
        capabilities,
        limits,
        hooks: false,
        source: Default::default(),
    }
}

fn cap(id: &str, scope: &str) -> PluginCapability {
    PluginCapability {
        id: id.into(),
        scope: scope.into(),
    }
}

/// 覆盖夹具全部方法所需的能力（scope 限定在 `/app/` 前缀）。
fn full_capabilities() -> Vec<PluginCapability> {
    vec![
        cap(CAP_KV_READ, "/app/"),
        cap(CAP_KV_WRITE, "/app/"),
        cap(CAP_KV_DELETE, "/app/"),
        cap(CAP_TXN_EXECUTE, ""),
        cap(CAP_LEASE_GRANT, ""),
        cap(CAP_LEASE_REVOKE, ""),
        cap(CAP_LEASE_KEEPALIVE, ""),
        cap(CAP_WATCH_SUBSCRIBE, ""),
        // 对象存储 / watch 不带资源键 → 必须空 scope（server 侧不提取 scope key）
        cap(CAP_STORAGE_READ, ""),
        cap(CAP_STORAGE_WRITE, ""),
    ]
}

fn limits(max_exec_ms: u64, max_memory_mb: u32) -> PluginLimits {
    PluginLimits {
        max_exec_ms,
        max_memory_mb,
        max_objects: 64,
    }
}

async fn load(backend: Arc<StubBackend>, manifest: &PluginManifest) -> Arc<dyn Plugin> {
    let loader = WasmPluginLoader::new(
        FIXTURE_DIR,
        backend as Arc<dyn PluginSdkBackend>,
        BTreeMap::new(),
        tokio::runtime::Handle::current(),
    )
    .expect("loader");
    let plugin = loader.load(manifest).await.expect("component plugin load");
    plugin.init().await.expect("init");
    plugin.start().await.expect("start");
    plugin
}

// ──── 测试 ────

/// 夹具确实是**无 WASI** 的组件二进制（4.1 的核心约束）。
#[test]
fn fixture_is_a_wasi_free_component() {
    let bytes =
        std::fs::read(FIXTURE).expect("fixture present (run scripts/build-plugin-guest.sh)");
    assert!(
        coord_agent::plugin::component_engine::is_component_binary(&bytes),
        "fixture must be a component (layer 1), not a core module"
    );
    // 组件把 import 名以字符串存在自定义段里；`wasi:` 前缀不存在 ⇒ 无 WASI import。
    let text = String::from_utf8_lossy(&bytes);
    assert!(
        !text.contains("wasi:"),
        "component fixture must not import any wasi interface"
    );
    assert!(
        text.contains("coord:plugin/host"),
        "component fixture must import the coord host interface"
    );
}

/// 类型安全 ABI 往返 + guest `init()` 成功（`init` 内部调用 `limits()` / `log()`，
/// 两者都是 async 宿主 import）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn component_plugin_init_and_echo() {
    let backend = Arc::new(StubBackend::default());
    let plugin = load(
        Arc::clone(&backend),
        &manifest("cm-echo", full_capabilities(), limits(2_000, 16)),
    )
    .await;

    assert!(plugin.health_check());
    let out = plugin
        .invoke("echo", b"hello-component")
        .await
        .expect("echo");
    assert_eq!(out, b"hello-component");

    plugin.stop().await.expect("stop");
    assert!(!plugin.health_check());
}

/// KV 写入经 `PluginSdk` 门面落到后端（typed record 往返 + revision 回传）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn component_plugin_kv_put_and_get() {
    let backend = Arc::new(StubBackend::default());
    let plugin = load(
        Arc::clone(&backend),
        &manifest("cm-kv", full_capabilities(), limits(2_000, 16)),
    )
    .await;

    let out = plugin
        .invoke("kv-put", b"/app/kv/cm=value-1")
        .await
        .expect("kv-put");
    assert_eq!(
        String::from_utf8_lossy(&out),
        "rev:1",
        "kv-put must return the host-assigned revision"
    );

    let out = plugin
        .invoke("kv-get", b"/app/kv/cm")
        .await
        .expect("kv-get");
    assert_eq!(out, b"value-1");

    // 缺失键 → typed `sdk-error.not-found`（不是 trap / 空返回）
    let out = plugin
        .invoke("kv-get", b"/app/kv/absent")
        .await
        .expect("kv-get (missing)");
    assert_eq!(out, b"missing");

    assert_eq!(
        backend.puts.lock().len(),
        1,
        "host call must reach the SDK backend"
    );

    plugin.stop().await.expect("stop");
}

/// create-if-absent CAS：首次 `ok:<rev>`，重复 `conflict`（typed variant 往返）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn component_plugin_kv_create_is_create_if_absent() {
    let backend = Arc::new(StubBackend::default());
    let plugin = load(
        Arc::clone(&backend),
        &manifest("cm-cas", full_capabilities(), limits(2_000, 16)),
    )
    .await;

    let first = plugin
        .invoke("kv-create", b"/app/kv/lock=own")
        .await
        .unwrap();
    assert!(
        String::from_utf8_lossy(&first).starts_with("ok:"),
        "{first:?}"
    );

    let second = plugin
        .invoke("kv-create", b"/app/kv/lock=other")
        .await
        .unwrap();
    assert_eq!(second, b"conflict");

    plugin.stop().await.expect("stop");
}

/// 作用域守卫在组件路径同样 fail-closed：越界写被 agent 拒绝（不进后端）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn component_plugin_scope_guard_denies_out_of_scope_write() {
    let backend = Arc::new(StubBackend::default());
    // 声明了 kv:write，但 scope 只覆盖 `/app/`
    let plugin = load(
        Arc::clone(&backend),
        &manifest("cm-scope", full_capabilities(), limits(2_000, 16)),
    )
    .await;

    let out = plugin
        .invoke("forbidden", b"/etc/other")
        .await
        .expect("plugin returns a typed error, not a trap");
    assert_eq!(out, b"forbidden");
    assert!(
        backend.puts.lock().is_empty(),
        "denied call must never reach the backend"
    );

    plugin.stop().await.expect("stop");
}

/// **异步宿主 import** 的实证：guest 阻塞式 `watch-next` 对应宿主侧 50ms await，
/// 耗时下界即宿主 await 的时长（fiber 挂起/恢复成立）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn component_plugin_watch_next_awaits_on_the_host() {
    let backend = Arc::new(StubBackend::default());
    backend.push_event(WatchEventKind::Put, b"/app/kv/watch", b"payload");

    let plugin = load(
        Arc::clone(&backend),
        &manifest("cm-watch", full_capabilities(), limits(5_000, 16)),
    )
    .await;

    let started = std::time::Instant::now();
    let out = plugin
        .invoke("watch-first", b"/app/kv/watch")
        .await
        .expect("watch-first");
    let elapsed = started.elapsed();

    assert_eq!(String::from_utf8_lossy(&out), "PUT:payload");
    assert!(
        elapsed >= std::time::Duration::from_millis(50),
        "host import must have been awaited (elapsed={elapsed:?})"
    );

    plugin.stop().await.expect("stop");
}

// ──── RAII 句柄（resource subscription）────

/// **RAII 的核心断言**：guest 不 `close`、直接 `drop` 句柄 → 宿主 `drop`
/// 立刻释放底层订阅（不是等到插件停止，也不是靠 `PluginSdk::release()` 兜底）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn component_plugin_watch_handle_drop_releases_subscription() {
    let backend = Arc::new(StubBackend::default());
    let plugin = load(
        Arc::clone(&backend),
        &manifest("cm-watch-raii", full_capabilities(), limits(5_000, 16)),
    )
    .await;

    let out = plugin
        .invoke("watch-drop", b"/app/kv/raii")
        .await
        .expect("watch-drop");
    assert_eq!(out, b"dropped");
    assert_eq!(
        backend.closes.lock().as_slice(),
        &[1],
        "dropping the guest handle must release the subscription immediately"
    );

    plugin.stop().await.expect("stop");
}

/// `close()` 幂等：第二次调用不重复释放（宿主不重复打后端）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn component_plugin_watch_close_is_idempotent() {
    let backend = Arc::new(StubBackend::default());
    let plugin = load(
        Arc::clone(&backend),
        &manifest("cm-watch-idem", full_capabilities(), limits(5_000, 16)),
    )
    .await;

    let out = plugin
        .invoke("watch-close-twice", b"/app/kv/idem")
        .await
        .expect("watch-close-twice");
    assert_eq!(out, b"closed");
    assert_eq!(
        backend.closes.lock().as_slice(),
        &[1],
        "close() must release exactly once"
    );

    plugin.stop().await.expect("stop");
}

/// `close()` 之后句柄仍合法但已释放：`next` → `invalid-argument`（typed 错误，
/// 不是 trap，实例继续可用）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn component_plugin_watch_next_after_close_is_invalid() {
    let backend = Arc::new(StubBackend::default());
    let plugin = load(
        Arc::clone(&backend),
        &manifest("cm-watch-closed", full_capabilities(), limits(5_000, 16)),
    )
    .await;

    let out = plugin
        .invoke("watch-next-after-close", b"/app/kv/closed")
        .await
        .expect("watch-next-after-close");
    assert_eq!(out, b"invalid");
    assert!(
        plugin.health_check(),
        "typed error must not kill the instance"
    );
    assert!(plugin.invoke("echo", b"alive").await.is_ok());

    plugin.stop().await.expect("stop");
}

/// 句柄表容量 = manifest 的 `max_objects`：超过上限的订阅以
/// `resource-exhausted` 提前失败；**调用返回时局部句柄离开作用域**，宿主把
/// 这批订阅全部释放（同时验证 RAII 与容量两条语义）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn component_plugin_watch_handles_are_bounded_by_max_objects() {
    let backend = Arc::new(StubBackend::default());
    let mut manifest = manifest("cm-watch-cap", full_capabilities(), limits(5_000, 16));
    manifest.limits.max_objects = 2;
    let plugin = load(Arc::clone(&backend), &manifest).await;

    let out = plugin.invoke("watch-many", b"5").await.expect("watch-many");
    assert_eq!(
        String::from_utf8_lossy(&out),
        "2",
        "handle table must be capped at max_objects"
    );
    let mut closes = backend.closes.lock().clone();
    closes.sort_unstable();
    assert_eq!(
        closes,
        vec![1, 2],
        "leaving the scope must release both held handles"
    );

    // 表已清空 → 下一次调用仍可分配（容量上限不是一次性配额）
    let out = plugin
        .invoke("watch-many", b"2")
        .await
        .expect("watch-many #2");
    assert_eq!(String::from_utf8_lossy(&out), "2");

    plugin.stop().await.expect("stop");
}

/// 实例销毁前的兜底：guest **跨调用持有**句柄且不 drop 就停止插件
/// （`release_handles` 在 stop 路径扫表释放）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn component_plugin_stop_releases_surviving_watch_handles() {
    let backend = Arc::new(StubBackend::default());
    let plugin = load(
        Arc::clone(&backend),
        &manifest("cm-watch-stop", full_capabilities(), limits(5_000, 16)),
    )
    .await;

    let out = plugin
        .invoke("watch-hold", b"/app/kv/held")
        .await
        .expect("watch-hold");
    assert_eq!(String::from_utf8_lossy(&out), "held:1");
    assert!(
        backend.closes.lock().is_empty(),
        "a live handle must not be released while the plugin runs"
    );

    plugin.stop().await.expect("stop");
    assert_eq!(
        backend.closes.lock().as_slice(),
        &[1],
        "stop must sweep handles the guest never dropped"
    );
}

/// 对象存储往返（typed `object-stat` + 大 payload 字节面）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn component_plugin_storage_roundtrip() {
    let backend = Arc::new(StubBackend::default());
    let plugin = load(
        Arc::clone(&backend),
        &manifest("cm-storage", full_capabilities(), limits(2_000, 16)),
    )
    .await;

    let out = plugin
        .invoke("storage-roundtrip", b"journal/cm=chunk-payload")
        .await
        .expect("storage-roundtrip");
    assert_eq!(String::from_utf8_lossy(&out), "size:13");

    plugin.stop().await.expect("stop");
}

// ──── 对象存储流式会话（resource uploader / downloader，批次 10）────

/// 分块上传（3B/块）→ 分块下载（每次 ≤4B）往返；`commit` / `close` 收尾，
/// 断言既不触发 `abort`（已提交），也不残留下载会话。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn component_plugin_storage_stream_roundtrip() {
    let backend = Arc::new(StubBackend::default());
    let plugin = load(
        Arc::clone(&backend),
        &manifest("cm-stream", full_capabilities(), limits(2_000, 16)),
    )
    .await;

    // 10 字节：3+3+3+1 上传；4+4+2 下载
    let out = plugin
        .invoke("storage-stream-roundtrip", b"inbox/obj-1=abcdefghij")
        .await
        .expect("storage-stream-roundtrip");
    assert_eq!(String::from_utf8_lossy(&out), "size:10");

    assert!(
        backend.aborted_uploads.lock().is_empty(),
        "a committed upload must not be aborted"
    );
    assert_eq!(
        backend.closed_downloads.lock().as_slice(),
        &[2],
        "the download session (id 2, after the upload's id 1) must be closed"
    );
    assert!(
        backend.downloads.lock().is_empty(),
        "no download session may survive close()"
    );

    plugin.stop().await.expect("stop");
}

/// 未知长度分块上传（`total-size = 0`，批次 11）：逐块写、`commit` 时定长；
/// 提交后 `stat.size` 必须等于实际字节数（而非 0）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn component_plugin_storage_stream_unknown_length() {
    let backend = Arc::new(StubBackend::default());
    let plugin = load(
        Arc::clone(&backend),
        &manifest("cm-stream-unknown", full_capabilities(), limits(2_000, 16)),
    )
    .await;

    let out = plugin
        .invoke("storage-stream-unknown", b"inbox/obj-u")
        .await
        .expect("storage-stream-unknown");
    assert_eq!(String::from_utf8_lossy(&out), "size:40");

    assert!(
        backend.aborted_uploads.lock().is_empty(),
        "a committed unknown-length upload must not be aborted"
    );
    assert_eq!(
        backend
            .objects
            .lock()
            .get(&("inbox".to_string(), b"obj-u".to_vec()))
            .map(Vec::len),
        Some(40),
        "the unknown-length object must be stored with the actual byte count"
    );

    plugin.stop().await.expect("stop");
}

/// 越界写入在**那一刻**被拒（typed `invalid-argument`，不是 trap）；句柄随后
/// drop → 宿主兜底中止上传（断言后端确实收到 abort，且没有提交任何对象）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn component_plugin_storage_stream_overflow_is_rejected() {
    let backend = Arc::new(StubBackend::default());
    let plugin = load(
        Arc::clone(&backend),
        &manifest("cm-stream-over", full_capabilities(), limits(2_000, 16)),
    )
    .await;

    let out = plugin
        .invoke("storage-stream-overflow", b"inbox/obj-2")
        .await
        .expect("overflow is a plugin-level error, not a trap");
    assert_eq!(out, b"invalid");

    assert_eq!(
        backend.aborted_uploads.lock().as_slice(),
        &[1],
        "dropping an unfinished upload handle must abort it on the host"
    );
    assert!(
        backend.objects.lock().is_empty(),
        "an aborted upload must not commit an object"
    );

    plugin.stop().await.expect("stop");
}

/// 打开不存在的对象 → typed `not-found`（与 `storage.stat` 的 `null` 语义不同：
/// 会话必须打开成功才有意义）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn component_plugin_storage_stream_read_missing() {
    let backend = Arc::new(StubBackend::default());
    let plugin = load(
        Arc::clone(&backend),
        &manifest("cm-stream-miss", full_capabilities(), limits(2_000, 16)),
    )
    .await;

    let out = plugin
        .invoke("storage-stream-read-missing", b"inbox/nope")
        .await
        .expect("missing object is a typed error");
    assert_eq!(out, b"missing");

    plugin.stop().await.expect("stop");
}

/// 租约三连（grant → keepAlive → revoke）在组件路径可用。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn component_plugin_lease_paths() {
    let backend = Arc::new(StubBackend::default());
    let plugin = load(
        Arc::clone(&backend),
        &manifest("cm-lease", full_capabilities(), limits(2_000, 16)),
    )
    .await;

    let out = plugin
        .invoke("lease-grant", b"30")
        .await
        .expect("lease-grant");
    assert_eq!(String::from_utf8_lossy(&out), "lease:1");

    plugin.stop().await.expect("stop");
}

/// 插件级错误（guest 显式 `err`）与 trap 可区分：实例保持可用。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn component_plugin_error_does_not_kill_the_instance() {
    let backend = Arc::new(StubBackend::default());
    let plugin = load(
        Arc::clone(&backend),
        &manifest("cm-err", full_capabilities(), limits(2_000, 16)),
    )
    .await;

    let err = plugin
        .invoke("no-such-method", b"")
        .await
        .expect_err("unknown method");
    assert!(
        err.to_string().contains("unknown method"),
        "unexpected error: {err}"
    );
    assert!(plugin.health_check(), "plugin-level error must not kill it");
    assert!(plugin.invoke("echo", b"still-alive").await.is_ok());

    plugin.stop().await.expect("stop");
}

/// fuel/epoch：忙等插件被沙箱打断、实例丢弃、插件标记 Failed，
/// 且**同引擎的另一个插件不受影响**。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn component_plugin_spin_traps_and_is_isolated() {
    let backend = Arc::new(StubBackend::default());
    let metrics = coord_agent::metrics::AgentMetrics::new();

    let loader = WasmPluginLoader::new(
        FIXTURE_DIR,
        Arc::clone(&backend) as Arc<dyn PluginSdkBackend>,
        BTreeMap::new(),
        tokio::runtime::Handle::current(),
    )
    .expect("loader")
    .with_metrics(metrics.clone());

    let spinning = loader
        .load(&manifest("cm-spin", full_capabilities(), limits(200, 16)))
        .await
        .expect("load spin");
    spinning.init().await.expect("init spin");
    spinning.start().await.expect("start spin");

    let healthy = loader
        .load(&manifest(
            "cm-healthy",
            full_capabilities(),
            limits(2_000, 16),
        ))
        .await
        .expect("load healthy");
    healthy.init().await.expect("init healthy");
    healthy.start().await.expect("start healthy");

    let err = spinning
        .invoke("spin", b"")
        .await
        .expect_err("spin must be interrupted");
    assert!(
        err.to_string().contains("watchdog") || err.to_string().contains("trapped"),
        "unexpected error: {err}"
    );
    assert!(
        matches!(spinning.status(), PluginStatus::Failed(_)),
        "trapped plugin must be marked Failed, got {:?}",
        spinning.status()
    );

    // 隔离：另一个插件照常服务
    let out = healthy
        .invoke("echo", b"isolated")
        .await
        .expect("healthy echo");
    assert_eq!(out, b"isolated");

    // 观测：trap 计数进 AgentMetrics（reason = fuel | epoch）
    let rendered = metrics.render_prometheus_text();
    assert!(
        rendered.contains("coord_agent_plugin_traps_total"),
        "trap metric must be exported"
    );
    assert!(
        rendered.contains("plugin=\"cm-spin\""),
        "trap metric must be labelled with the plugin\n{rendered}"
    );

    let _ = spinning.stop().await;
    let _ = healthy.stop().await;
}

/// 内存上限：申请远超预算的内存 → 失败于沙箱，而不是拖垮宿主。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn component_plugin_memory_limit_is_enforced() {
    let backend = Arc::new(StubBackend::default());
    let plugin = load(
        Arc::clone(&backend),
        // 2MiB 上限（模块初始内存可容纳）；夹具申请 8MiB
        &manifest("cm-mem", full_capabilities(), limits(2_000, 2)),
    )
    .await;

    let err = plugin
        .invoke("alloc", b"8388608")
        .await
        .expect_err("allocation beyond StoreLimits must fail");
    assert!(!err.to_string().is_empty());

    plugin.stop().await.expect("stop");
}
