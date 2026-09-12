// coord-agent: rquickjs 插件宿主（Phase 1.1 / §7.4 异步桥）
//
// 架构（每插件一个隔离单元）：
//
// ```text
//  Agent 异步运行时            插件专属 std::thread
//  ─────────────────           ────────────────────────────────
//  JsPlugin::invoke ─┐          current-thread tokio runtime + LocalSet
//                    ├─ mpsc ─►  AsyncRuntime (QuickJS)
//  oneshot 回填     ◄┘          ├─ drive()（JS job queue + Rust future）
//                               ├─ AsyncContext（isolate）
//                               └─ 宿主 SDK 出站 → coord-client
// ```
//
// 关键约束与手段：
// - **单线程 per isolate**（R2）：QuickJS 非线程安全，插件内不共享可变状态；
// - **内存上限**：`AsyncRuntime::set_memory_limit`（`limits.max_memory_mb`）；
// - **执行超时**：中断处理器按调用 arm/disarm deadline（`limits.max_exec_ms`），
//   超时由 QuickJS 抛出不可捕获错误 → **isolate 丢弃**（线程退出 + 插件 Failed）；
// - **崩溃隔离**（R7）：JS 异常只让单次调用失败；线程级失败由外层看门狗兜底；
// - **模块加载**：ESM（`Module::declare` + `eval`）；相对导入经 `PluginResolver`
//   限制在插件目录内（拒绝 `..` 逃逸）。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};

use crate::plugin::identity::PluginIdentityManager;
use crate::plugin::manifest::{PluginLimits, PluginManifest, PluginRuntime};
use crate::plugin::sdk::{PluginSdk, PluginSdkBackend};
use crate::plugin::{Plugin, PluginLoader, PluginStatus};
use crate::service::{ServiceError, ServiceResult};

// ──── 时钟 ────

/// 当前 epoch 毫秒（时钟异常 → 0 = 永不超时，避免误杀）。
fn epoch_ms() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => u64::try_from(d.as_millis()).unwrap_or(0),
        Err(_) => 0,
    }
}

// ──── 模块解析 / 加载（沙箱边界）────

/// 逻辑路径归一化（不触盘；消除 `.` / `..`）。
use crate::plugin::normalize_plugin_path as normalize;

/// 模块解析器：导入名 → 插件目录内的绝对路径（越界 → 加载错误）。
struct PluginResolver {
    root: PathBuf,
}

impl rquickjs::loader::Resolver for PluginResolver {
    fn resolve<'js>(
        &mut self,
        _ctx: &rquickjs::Ctx<'js>,
        base: &str,
        name: &str,
        _attributes: Option<rquickjs::loader::ImportAttributes<'js>>,
    ) -> rquickjs::Result<String> {
        let candidate = if name.starts_with("./") || name.starts_with("../") {
            let base_dir = Path::new(base).parent().unwrap_or(&self.root);
            base_dir.join(name)
        } else if name.starts_with('/') {
            PathBuf::from(name)
        } else {
            // 裸名 → 插件根目录
            self.root.join(name)
        };
        let normalized = normalize(&candidate);
        if !normalized.starts_with(&self.root) {
            return Err(rquickjs::Error::new_loading(name));
        }
        Ok(normalized.to_string_lossy().into_owned())
    }
}

/// 模块加载器：读取已被解析器限制在插件目录内的文件。
struct PluginFileLoader;

impl rquickjs::loader::Loader for PluginFileLoader {
    fn load<'js>(
        &mut self,
        ctx: &rquickjs::Ctx<'js>,
        name: &str,
        _attributes: Option<rquickjs::loader::ImportAttributes<'js>>,
    ) -> rquickjs::Result<rquickjs::Module<'js>> {
        let source =
            std::fs::read_to_string(name).map_err(|_| rquickjs::Error::new_loading(name))?;
        rquickjs::Module::declare(ctx.clone(), name.to_string(), source)
    }
}

// ──── 宿主命令 ────

/// invoke 结果：`Ok(bytes)` 或 `Err(已格式化的异常消息)`。
type InvokeResult = Result<Vec<u8>, String>;

enum JsCommand {
    /// 调用插件导出的 `handleInvoke(method, payload)`
    Invoke {
        method: String,
        payload: Vec<u8>,
        resp: oneshot::Sender<InvokeResult>,
    },
    /// 停止：调用可选 `stop()` 后退出线程
    Stop { resp: oneshot::Sender<()> },
}

// ──── 共享运行时控制 ────

/// 跨线程共享的中断控制（epoch ms；0 = 关闭）。
#[derive(Default)]
struct InterruptCtl {
    deadline_ms: AtomicU64,
    tripped: AtomicBool,
}

impl InterruptCtl {
    /// 为一次调用武装 deadline。
    fn arm(&self, max_exec_ms: u64) {
        self.tripped.store(false, Ordering::Relaxed);
        let now = epoch_ms();
        let dl = if now == 0 { 0 } else { now + max_exec_ms };
        self.deadline_ms.store(dl, Ordering::Relaxed);
    }

    /// 调用结束，关闭 deadline。
    fn disarm(&self) {
        self.deadline_ms.store(0, Ordering::Relaxed);
    }

    /// 中断处理器：已超时则返回 true（QuickJS 抛不可捕获错误）。
    fn expired(&self) -> bool {
        let dl = self.deadline_ms.load(Ordering::Relaxed);
        if dl == 0 {
            return false;
        }
        let now = epoch_ms();
        if now != 0 && now >= dl {
            self.tripped.store(true, Ordering::Relaxed);
            return true;
        }
        false
    }

    /// 本插件是否被中断过（isolate 不可继续使用）。
    fn was_tripped(&self) -> bool {
        self.tripped.load(Ordering::Relaxed)
    }
}

// ──── 插件加载器 ────

/// JS 插件加载器（`[plugins].dir` 下的 ESM 入口）。
pub struct JsPluginLoader {
    dir: PathBuf,
    backend: Arc<dyn PluginSdkBackend>,
    env: BTreeMap<String, String>,
    /// 插件身份管理器（Phase 1.3；Some = 为每个插件开通受限 CCT 账户）
    identity: Option<Arc<PluginIdentityManager>>,
    /// 调用面 typed 钩子（Phase 2.2）
    hooks: Option<Arc<crate::plugin::hooks::HookRegistry>>,
    /// 插件指标（调用结果 + 中断/超时计数）
    metrics: Option<crate::metrics::AgentMetrics>,
}

impl JsPluginLoader {
    /// 由插件目录 + SDK 后端 + 环境注入构建。
    pub fn new(
        dir: impl Into<PathBuf>,
        backend: Arc<dyn PluginSdkBackend>,
        env: BTreeMap<String, String>,
    ) -> Self {
        Self {
            dir: dir.into(),
            backend,
            env,
            identity: None,
            hooks: None,
            metrics: None,
        }
    }

    /// 挂载插件身份管理器（每个插件独立服务账户 + 受限 CCT）。
    pub fn with_identity(mut self, identity: Arc<PluginIdentityManager>) -> Self {
        self.identity = Some(identity);
        self
    }

    /// 挂载插件指标（调用结果 + 中断/超时计数）。
    pub fn with_metrics(mut self, metrics: crate::metrics::AgentMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// 挂载调用面钩子注册表（仅作用于插件发起的调用）。
    pub fn with_hooks(mut self, hooks: Arc<crate::plugin::hooks::HookRegistry>) -> Self {
        self.hooks = Some(hooks);
        self
    }

    /// 解析插件入口的绝对路径（限制在插件目录内）。
    pub(crate) fn entry_path(&self, manifest: &PluginManifest) -> Result<PathBuf, String> {
        let rel = manifest.resolved_entry();
        let path = normalize(&self.dir.join(rel));
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
impl PluginLoader for JsPluginLoader {
    async fn load(&self, manifest: &PluginManifest) -> ServiceResult<Arc<dyn Plugin>> {
        manifest.validate().map_err(ServiceError::from)?;
        if manifest.runtime != PluginRuntime::Js {
            return Err(format!(
                "JsPluginLoader cannot load runtime '{}' (plugin '{}')",
                manifest.runtime.as_str(),
                manifest.name
            )
            .into());
        }
        let entry = self.entry_path(manifest).map_err(ServiceError::from)?;
        let source = tokio::fs::read_to_string(&entry)
            .await
            .map_err(|e| -> ServiceError {
                format!(
                    "failed to read plugin '{}' entry {}: {e}",
                    manifest.name,
                    entry.display()
                )
                .into()
            })?;

        // Phase 1.3：开通插件服务账户（失败 → 警告并回退共享未鉴权客户端）
        if let Some(identity) = &self.identity {
            if let Err(e) = identity
                .ensure(&manifest.name, &manifest.capabilities)
                .await
            {
                tracing::warn!(
                    "plugin '{}': identity provisioning failed ({e}); outbound calls fall back to \
                     the shared agent client",
                    manifest.name
                );
            }
        }

        // 先构造门面（可能挂载调用面钩子），再包成 Arc
        let mut sdk = PluginSdk::new(
            manifest.name.clone(),
            &manifest.capabilities,
            Arc::clone(&self.backend),
        );
        if let Some(hooks) = &self.hooks {
            sdk = sdk.with_hooks(Arc::clone(hooks));
        }
        let sdk = Arc::new(sdk);

        Ok(Arc::new(JsPlugin::new(
            manifest.clone(),
            entry,
            source,
            sdk,
            self,
        )))
    }

    /// 入口脚本当前内容指纹（不加载）：与 `JsPlugin::content_fingerprint` 比对。
    fn fingerprint(&self, manifest: &PluginManifest) -> Option<String> {
        let path = self.entry_path(manifest).ok()?;
        crate::plugin::content_fingerprint(&path)
    }
}

// ──── JS 插件实例 ────

struct JsState {
    status: PluginStatus,
    tx: Option<mpsc::Sender<JsCommand>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl JsState {
    fn new() -> Self {
        Self {
            status: PluginStatus::Loaded,
            tx: None,
            thread: None,
        }
    }
}

/// 单个 JS 插件实例（一个专属线程 + 一个 QuickJS isolate）。
pub struct JsPlugin {
    manifest: PluginManifest,
    entry_path: PathBuf,
    source: String,
    plugin_dir: PathBuf,
    sdk: Arc<PluginSdk>,
    env: BTreeMap<String, String>,
    limits: PluginLimits,
    ctl: Arc<InterruptCtl>,
    identity: Option<Arc<PluginIdentityManager>>,
    metrics: Option<crate::metrics::AgentMetrics>,
    state: Mutex<JsState>,
}

impl std::fmt::Debug for JsPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsPlugin")
            .field("name", &self.manifest.name)
            .field("entry", &self.entry_path)
            .finish_non_exhaustive()
    }
}

impl JsPlugin {
    fn new(
        manifest: PluginManifest,
        entry_path: PathBuf,
        source: String,
        sdk: Arc<PluginSdk>,
        loader: &JsPluginLoader,
    ) -> Self {
        let limits = manifest.limits.clone();
        Self {
            manifest,
            entry_path,
            source,
            plugin_dir: loader.dir.clone(),
            sdk,
            env: loader.env.clone(),
            limits,
            ctl: Arc::new(InterruptCtl::default()),
            identity: loader.identity.clone(),
            metrics: loader.metrics.clone(),
            state: Mutex::new(JsState::new()),
        }
    }

    /// 标记失败并切断命令通道（插件线程随之退出，isolate 丢弃）。
    fn fail(&self, reason: impl Into<String>) {
        let reason = reason.into();
        tracing::error!("plugin '{}' failed: {reason}", self.manifest.name);
        let mut st = self.state.lock();
        st.status = PluginStatus::Failed(reason);
        st.tx = None;
    }

    /// 发起一次调用并等待结果（含外层看门狗）。
    async fn invoke_inner(&self, method: &str, payload: &[u8]) -> InvokeResult {
        let tx = {
            let st = self.state.lock();
            st.tx.clone()
        };
        let Some(tx) = tx else {
            return Err(format!("plugin '{}' is not running", self.manifest.name));
        };
        let (resp_tx, resp_rx) = oneshot::channel::<InvokeResult>();
        let cmd = JsCommand::Invoke {
            method: method.to_string(),
            payload: payload.to_vec(),
            resp: resp_tx,
        };
        if tx.send(cmd).await.is_err() {
            self.fail("plugin thread is gone");
            return Err(format!("plugin '{}' thread is gone", self.manifest.name));
        }
        // 外层看门狗：给 JS 中断留 1s 余量（中断优先，此处为硬兜底）
        let wait = Duration::from_millis(self.limits.max_exec_ms.saturating_add(1_000));
        let result = match tokio::time::timeout(wait, resp_rx).await {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => {
                self.fail("plugin thread dropped the response channel");
                Err(format!("plugin '{}' thread is gone", self.manifest.name))
            }
            Err(_) => {
                self.fail(format!("invoke watchdog timed out after {wait:?}"));
                Err(format!(
                    "plugin '{}' invoke exceeded the watchdog ({wait:?})",
                    self.manifest.name
                ))
            }
        };
        if result.is_err() && self.ctl.was_tripped() {
            self.fail(format!(
                "execution interrupted after {}ms (max_exec_ms); isolate discarded",
                self.limits.max_exec_ms
            ));
        }
        result
    }
}

#[async_trait]
impl Plugin for JsPlugin {
    fn name(&self) -> &str {
        &self.manifest.name
    }

    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    /// 内容指纹 = 加载时读入的入口脚本字节的 SHA256（Phase 5 版本化）。
    fn content_fingerprint(&self) -> Option<String> {
        Some(crate::plugin::content_fingerprint_bytes(
            self.source.as_bytes(),
        ))
    }

    async fn init(&self) -> ServiceResult<()> {
        let (tx, rx) = mpsc::channel::<JsCommand>(16);
        let (ready_tx, ready_rx) = oneshot::channel::<Result<(), String>>();

        let thread = {
            let name = self.manifest.name.clone();
            let entry_name = self.entry_path.to_string_lossy().into_owned();
            let source = self.source.clone();
            let dir = self.plugin_dir.clone();
            let sdk = Arc::clone(&self.sdk);
            let env = self.env.clone();
            let limits = self.limits.clone();
            let ctl = Arc::clone(&self.ctl);

            std::thread::Builder::new()
                .name(format!("coord-plugin-{name}"))
                .spawn(move || {
                    let rt = match tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(rt) => rt,
                        Err(e) => {
                            let _ = ready_tx.send(Err(format!("tokio runtime: {e}")));
                            return;
                        }
                    };
                    let local = tokio::task::LocalSet::new();
                    rt.block_on(local.run_until(host_loop(
                        name, limits, sdk, env, ctl, entry_name, source, dir, rx, ready_tx,
                    )));
                })
                .map_err(|e| -> ServiceError {
                    format!("failed to spawn plugin thread: {e}").into()
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
                    "JS plugin '{}' initialised (entry={}, max_memory={}MiB, max_exec={}ms)",
                    self.manifest.name,
                    self.entry_path.display(),
                    self.limits.max_memory_mb,
                    self.limits.max_exec_ms
                );
                Ok(())
            }
            Ok(Err(e)) => {
                self.fail(e);
                Err("plugin initialisation failed (see logs)".into())
            }
            Err(_) => {
                self.fail("plugin thread exited during init");
                Err("plugin thread exited during init".into())
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
        // 释放插件持有的后台句柄（Lease 保活等）
        self.sdk.release().await;
        // Phase 1.3：注销插件身份（停止 CCT 续期任务 + 移除 authed client）
        if let Some(identity) = &self.identity {
            identity.forget(&self.manifest.name);
        }

        if let Some(tx) = tx {
            let (resp_tx, resp_rx) = oneshot::channel::<()>();
            if tx.send(JsCommand::Stop { resp: resp_tx }).await.is_ok() {
                let _ = tokio::time::timeout(Duration::from_secs(5), resp_rx).await;
            }
        }
        // 线程退出后再 join，避免阻塞 async 运行时
        if let Some(thread) = thread {
            let _ = tokio::task::spawn_blocking(move || thread.join()).await;
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
            metrics.record_plugin_invocation(&self.manifest.name, "js", result.is_ok());
            if result.is_err() && self.ctl.was_tripped() {
                metrics.record_plugin_trap(&self.manifest.name, "timeout");
            }
        }
        result.map_err(|e| -> ServiceError { e.into() })
    }
}

impl JsPlugin {
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

// ──── 宿主事件循环（插件专属线程内）────

/// 插件线程主循环：建运行时 → 装 SDK → 求值模块 → 服务命令。
#[allow(clippy::too_many_arguments)]
async fn host_loop(
    name: String,
    limits: PluginLimits,
    sdk: Arc<PluginSdk>,
    env: BTreeMap<String, String>,
    ctl: Arc<InterruptCtl>,
    entry_name: String,
    source: String,
    plugin_dir: PathBuf,
    mut rx: mpsc::Receiver<JsCommand>,
    ready: oneshot::Sender<Result<(), String>>,
) {
    let js_rt = match rquickjs::AsyncRuntime::new() {
        Ok(rt) => rt,
        Err(e) => {
            let _ = ready.send(Err(format!("failed to create QuickJS runtime: {e}")));
            return;
        }
    };

    // ── 资源上限（D8 第一方轻沙箱）──
    js_rt
        .set_memory_limit(limits.max_memory_mb as usize * 1024 * 1024)
        .await;
    js_rt.set_max_stack_size(1024 * 1024).await;
    js_rt
        .set_loader(PluginResolver { root: plugin_dir }, PluginFileLoader)
        .await;
    {
        let handler_ctl = Arc::clone(&ctl);
        js_rt
            .set_interrupt_handler(Some(Box::new(move || handler_ctl.expired())))
            .await;
    }

    // ── 事件循环驱动（Rust future + JS job queue）──
    let driver = js_rt.clone();
    tokio::task::spawn_local(async move {
        driver.drive().await;
    });

    let ctx = match rquickjs::AsyncContext::full(&js_rt).await {
        Ok(ctx) => ctx,
        Err(e) => {
            let _ = ready.send(Err(format!("failed to create QuickJS context: {e}")));
            return;
        }
    };

    // ── 初始化：装 SDK + 求值入口模块 ──
    {
        let init_sdk = Arc::clone(&sdk);
        let init_limits = limits.clone();
        let init = ctx
            .async_with(async move |ctx| -> rquickjs::Result<()> {
                crate::plugin::sdk::bind::install(&ctx, init_sdk, &env, &init_limits)?;
                evaluate_entry(&ctx, &entry_name, &source).await
            })
            .await;

        if let Err(e) = init {
            let msg = format!("plugin '{name}' init failed: {e}");
            let _ = ready.send(Err(msg));
            return;
        }
    }
    if ready.send(Ok(())).is_err() {
        return;
    }

    // ── 命令循环 ──
    while let Some(cmd) = rx.recv().await {
        match cmd {
            JsCommand::Invoke {
                method,
                payload,
                resp,
            } => {
                ctl.arm(limits.max_exec_ms);
                let result = ctx
                    .async_with(async move |ctx| -> rquickjs::Result<Vec<u8>> {
                        call_handler(&ctx, &method, payload).await
                    })
                    .await;
                ctl.disarm();
                let result = match result {
                    Ok(bytes) => Ok(bytes),
                    Err(e) => Err(format_js_error(&ctx, e).await),
                };
                let _ = resp.send(result);
                if ctl.was_tripped() {
                    // isolate 已被中断污染 → 丢弃，插件标记 Failed
                    tracing::error!(
                        "plugin '{name}' execution interrupted after {}ms; discarding isolate",
                        limits.max_exec_ms
                    );
                    break;
                }
            }
            JsCommand::Stop { resp } => {
                let _ = ctx
                    .async_with(async move |ctx| -> rquickjs::Result<()> {
                        call_optional_hook(&ctx, "stop").await
                    })
                    .await;
                let _ = resp.send(());
                break;
            }
        }
    }
    sdk.release().await;
    drop(ctx);
    drop(js_rt);
    tracing::debug!("plugin '{name}' thread exited");
}

/// 求值入口 ESM 模块，并把导出钩子挂到全局（isolate 生命周期内可反复取用）。
async fn evaluate_entry<'js>(
    ctx: &rquickjs::Ctx<'js>,
    entry_name: &str,
    source: &str,
) -> rquickjs::Result<()> {
    let module =
        rquickjs::Module::declare(ctx.clone(), entry_name.to_string(), source.to_string())?;
    let (module, promise) = module.eval()?;
    let _: rquickjs::Value<'js> = promise.into_future::<rquickjs::Value<'js>>().await?;
    let namespace = module.namespace()?;
    stash_hooks(ctx, &namespace)
}

/// 钩子搬运：模块导出优先，其次全局（脚本风格）。
fn stash_hooks<'js>(
    ctx: &rquickjs::Ctx<'js>,
    namespace: &rquickjs::Object<'js>,
) -> rquickjs::Result<()> {
    let globals = ctx.globals();
    for hook in ["handleInvoke", "init", "stop"] {
        let exported: Option<rquickjs::Function<'js>> = namespace.get(hook)?;
        let fallback: Option<rquickjs::Function<'js>> = match exported {
            Some(_) => None,
            None => globals.get(hook)?,
        };
        if let Some(f) = exported.or(fallback) {
            globals.set(format!("__coord_hook_{hook}"), f)?;
        }
    }
    Ok(())
}

/// 调用插件导出的 `handleInvoke(method, payload)`（缺失 → 类型错误）。
async fn call_handler<'js>(
    ctx: &rquickjs::Ctx<'js>,
    method: &str,
    payload: Vec<u8>,
) -> rquickjs::Result<Vec<u8>> {
    let globals = ctx.globals();
    let hook: Option<rquickjs::Function<'js>> = globals.get("__coord_hook_handleInvoke")?;
    let Some(hook) = hook else {
        return Err(rquickjs::Exception::throw_type(
            ctx,
            "plugin does not export 'handleInvoke'",
        ));
    };
    let payload = crate::plugin::sdk::bind::bytes_to_js(ctx, &payload)?;
    let promise: rquickjs::Promise<'js> = hook.call((method.to_string(), payload))?;
    let result: rquickjs::Value<'js> = promise.into_future::<rquickjs::Value<'js>>().await?;
    crate::plugin::sdk::bind::js_bytes(ctx, &result)
}

/// 调用可选钩子（未导出 → no-op；返回 Promise 时等待其结算）。
async fn call_optional_hook<'js>(ctx: &rquickjs::Ctx<'js>, hook: &str) -> rquickjs::Result<()> {
    let globals = ctx.globals();
    let f: Option<rquickjs::Function<'js>> = globals.get(format!("__coord_hook_{hook}"))?;
    let Some(f) = f else {
        return Ok(());
    };
    let result: rquickjs::Value<'js> = f.call(())?;
    if result.is_promise() {
        let promise: rquickjs::Promise<'js> =
            result.into_promise().ok_or(rquickjs::Error::Unknown)?;
        let _: rquickjs::Value<'js> = promise.into_future::<rquickjs::Value<'js>>().await?;
    }
    Ok(())
}

/// 把 rquickjs 错误变成可读消息（JS 异常 → 读取 pending exception）。
async fn format_js_error(ctx: &rquickjs::AsyncContext, e: rquickjs::Error) -> String {
    match e {
        rquickjs::Error::Exception => {
            ctx.with(|ctx| {
                let caught = ctx.catch();
                format_caught(&ctx, &caught)
            })
            .await
        }
        other => other.to_string(),
    }
}

/// 异常值 → 可读消息（优先 `Error.message`，其次字符串，最后 Debug）。
fn format_caught<'js>(ctx: &rquickjs::Ctx<'js>, v: &rquickjs::Value<'js>) -> String {
    if let Some(exc) = v.as_exception() {
        if let Some(msg) = exc.message() {
            let name = exc
                .as_object()
                .get::<_, Option<String>>("name")
                .ok()
                .flatten()
                .unwrap_or_else(|| "Error".to_string());
            return format!("{name}: {msg}");
        }
    }
    if let Some(s) = v.as_string() {
        if let Ok(s) = s.to_string() {
            return s;
        }
    }
    let _ = ctx;
    format!("{v:?}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::manifest::{PluginCapability, PluginSource, PluginTrust};
    use crate::plugin::sdk::backend::{
        CompareOp, CompareTarget, KvDelete, KvDeleteOut, KvPut, KvPutOut, KvRange, KvRangeOut,
        KvRecord, ObjectGetOut, ObjectPutOut, ObjectStatDto, PluginSdkBackend, SdkError, SdkResult,
        TxnOp, TxnOpOut, TxnOut, TxnReq, WatchEventDto, WatchEventKind, WatchSubscribe,
        CAP_KV_DELETE, CAP_KV_READ, CAP_KV_WRITE, CAP_LEASE_GRANT, CAP_LEASE_KEEPALIVE,
        CAP_LEASE_REVOKE, CAP_STORAGE_READ, CAP_STORAGE_WRITE, CAP_TXN_EXECUTE,
        CAP_WATCH_SUBSCRIBE,
    };
    use parking_lot::Mutex as TestMutex;

    /// 已提交对象表（流式会话 `commit` 的落点）。
    type StubObjects = BTreeMap<(String, Vec<u8>), Vec<u8>>;

    /// 内存 KV stub：验证「JS → 宿主 SDK → 协调原语」全链路（R1 桥 spike）。
    ///
    /// txn 实现了真实的 version/mod_revision/value CAS 语义与单调 revision，
    /// 因此 `coord.lock` / `coord.election` 的互斥逻辑可在此确定性验证。
    /// 简化点（与真实 server 的差异，测试时需注意）：
    /// - `range` 在 `rangeEnd` 为空时按**前缀**匹配（真实 server 是精确键）；
    /// - 租约是常量句柄，`lease.revoke` 不删除绑定键（server 端会删）。
    #[derive(Default)]
    struct StubBackend {
        store: TestMutex<StubState>,
        calls: TestMutex<Vec<String>>,
        /// Watch 订阅：`(plugin, id) → 脚本`
        watches: TestMutex<BTreeMap<(String, u64), StubWatch>>,
        /// 已提交对象：`(bucket, object_id) → 字节`（流式会话 `commit` 落这里）
        objects: TestMutex<StubObjects>,
        /// 上传会话：`id → 草稿`
        uploads: TestMutex<BTreeMap<u64, StubUpload>>,
        /// 下载会话：`id → 游标`
        downloads: TestMutex<BTreeMap<u64, StubDownload>>,
        /// 会话 id 分配器（上传 / 下载共用）
        next_session: TestMutex<u64>,
    }

    /// stub 上传会话（分块累积；`commit` 时校验并落盘）。
    struct StubUpload {
        bucket: String,
        object_id: Vec<u8>,
        total: u64,
        buf: Vec<u8>,
    }

    /// stub 下载会话（对已落盘对象的游标）。
    struct StubDownload {
        bucket: String,
        object_id: Vec<u8>,
        data: Vec<u8>,
        pos: usize,
    }

    /// stub KV 状态（revision 从 2 起，首次写 = 3，与历史断言对齐）。
    #[derive(Default)]
    struct StubState {
        kvs: BTreeMap<Vec<u8>, StubEntry>,
        revision: i64,
        /// 租约自增序列（与 server 的 NEXT_LEASE_ID 同序）
        lease_seq: i64,
    }

    #[derive(Clone)]
    struct StubEntry {
        value: Vec<u8>,
        lease_id: i64,
        version: i64,
        mod_revision: i64,
    }

    impl StubState {
        fn next_revision(&mut self) -> i64 {
            if self.revision < 2 {
                self.revision = 2;
            }
            self.revision += 1;
            self.revision
        }

        fn record(&self, key: &[u8], entry: &StubEntry) -> KvRecord {
            KvRecord {
                key: key.to_vec(),
                value: entry.value.clone(),
                lease_id: entry.lease_id,
                version: entry.version,
            }
        }

        fn get(&self, key: &[u8]) -> Option<(&Vec<u8>, &StubEntry)> {
            self.kvs.get_key_value(key)
        }

        /// 写入并返回旧记录。
        fn put(
            &mut self,
            key: &[u8],
            value: &[u8],
            lease_id: i64,
            revision: i64,
        ) -> Option<KvRecord> {
            let prev = self.kvs.get(key).map(|e| self.record(key, e));
            match self.kvs.get_mut(key) {
                Some(entry) => {
                    entry.value = value.to_vec();
                    entry.lease_id = lease_id;
                    entry.version += 1;
                    entry.mod_revision = revision;
                }
                None => {
                    self.kvs.insert(
                        key.to_vec(),
                        StubEntry {
                            value: value.to_vec(),
                            lease_id,
                            version: 1,
                            mod_revision: revision,
                        },
                    );
                }
            }
            prev
        }

        fn delete(&mut self, key: &[u8]) -> Option<KvRecord> {
            let prev = self.kvs.get(key).map(|e| self.record(key, e));
            self.kvs.remove(key);
            prev
        }

        /// 按 `key` 前缀（或 `rangeEnd` 左闭右开）收集记录。
        fn range(&self, key: &[u8], range_end: &[u8]) -> Vec<KvRecord> {
            self.kvs
                .iter()
                .filter(|(k, _)| {
                    if range_end.is_empty() {
                        k.starts_with(key)
                    } else {
                        k.as_slice() >= key && k.as_slice() < range_end
                    }
                })
                .map(|(k, e)| self.record(k, e))
                .collect()
        }
    }

    /// stub watch 脚本：预置事件队列；`pending` = 永不产生事件（测超时路径）。
    struct StubWatch {
        events: Vec<WatchEventDto>,
        pending: bool,
    }

    #[async_trait]
    impl PluginSdkBackend for StubBackend {
        async fn kv_put(&self, plugin: &str, req: KvPut) -> SdkResult<KvPutOut> {
            self.calls.lock().push(format!("{plugin}:put"));
            let mut store = self.store.lock();
            let revision = store.next_revision();
            let prev = store.put(&req.key, &req.value, req.lease_id, revision);
            Ok(KvPutOut {
                prev_kv: prev,
                revision,
            })
        }

        async fn kv_range(&self, _plugin: &str, req: KvRange) -> SdkResult<KvRangeOut> {
            let store = self.store.lock();
            let kvs = store.range(&req.key, &req.range_end);
            Ok(KvRangeOut {
                count: kvs.len() as i64,
                kvs,
                revision: store.revision,
            })
        }

        async fn kv_delete(&self, _plugin: &str, req: KvDelete) -> SdkResult<KvDeleteOut> {
            let mut store = self.store.lock();
            let revision = store.next_revision();
            let prev = store.delete(&req.key);
            Ok(KvDeleteOut {
                deleted: i64::from(prev.is_some()),
                prev_kvs: prev.into_iter().collect(),
                revision,
            })
        }

        /// 真实 CAS 语义：比较全部满足走 success 分支，否则走 failure 分支。
        async fn txn(&self, plugin: &str, req: TxnReq) -> SdkResult<TxnOut> {
            self.calls.lock().push(format!("{plugin}:txn"));
            let mut store = self.store.lock();
            let revision = store.next_revision();

            let mut succeeded = true;
            for c in &req.compares {
                let entry = store.get(&c.key).map(|(_, e)| e.clone());
                let (version, mod_revision, value) = match &entry {
                    Some(e) => (e.version, e.mod_revision, e.value.clone()),
                    None => (0, 0, Vec::new()),
                };
                let cmp = match c.target {
                    CompareTarget::Version => version.cmp(&c.int_value),
                    CompareTarget::ModRevision => mod_revision.cmp(&c.int_value),
                    CompareTarget::Value => value.cmp(&c.bytes_value),
                };
                let ok = match c.op {
                    CompareOp::Equal => cmp == std::cmp::Ordering::Equal,
                    CompareOp::NotEqual => cmp != std::cmp::Ordering::Equal,
                    CompareOp::Greater => cmp == std::cmp::Ordering::Greater,
                    CompareOp::Less => cmp == std::cmp::Ordering::Less,
                };
                if !ok {
                    succeeded = false;
                    break;
                }
            }

            let ops = if succeeded {
                &req.success
            } else {
                &req.failure
            };
            let mut responses = Vec::new();
            for op in ops {
                match op {
                    TxnOp::Put(p) => {
                        let prev = store.put(&p.key, &p.value, p.lease_id, revision);
                        responses.push(TxnOpOut::Put(KvPutOut {
                            prev_kv: if p.prev_kv { prev } else { None },
                            revision,
                        }));
                    }
                    TxnOp::Range(r) => {
                        let kvs = store.range(&r.key, &r.range_end);
                        responses.push(TxnOpOut::Range(KvRangeOut {
                            count: kvs.len() as i64,
                            kvs,
                            revision,
                        }));
                    }
                    TxnOp::Delete(d) => {
                        let prev = store.delete(&d.key);
                        responses.push(TxnOpOut::Delete(KvDeleteOut {
                            deleted: i64::from(prev.is_some()),
                            prev_kvs: prev.into_iter().collect(),
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
            // 单调分配（与 server 的 NEXT_LEASE_ID 同序）：保证不同竞争者的租约互不
            // 干扰 —— 锁的失败清理只能删除自己租约绑定的键。
            let mut store = self.store.lock();
            store.lease_seq += 1;
            Ok(store.lease_seq)
        }

        async fn lease_revoke(&self, _plugin: &str, id: i64) -> SdkResult<()> {
            // 与 server 的 `LeaseOp::Revoke { delete_keys: true }` 对齐：
            // 撤销租约连带删除绑定到该租约的键（锁释放依赖此语义）。
            let mut store = self.store.lock();
            if id != 0 {
                let bound: Vec<Vec<u8>> = store
                    .kvs
                    .iter()
                    .filter(|(_, e)| e.lease_id == id)
                    .map(|(k, _)| k.clone())
                    .collect();
                for key in bound {
                    let revision = store.next_revision();
                    store.put(&key, &[], 0, revision);
                    store.delete(&key);
                }
            }
            Ok(())
        }

        async fn lease_keep_alive(&self, _plugin: &str, _id: i64) -> SdkResult<()> {
            Ok(())
        }

        async fn lease_stop_keep_alive(&self, _plugin: &str, _id: i64) -> SdkResult<()> {
            Ok(())
        }

        async fn watch_subscribe(&self, plugin: &str, req: WatchSubscribe) -> SdkResult<u64> {
            self.calls.lock().push(format!(
                "{plugin}:watch:{}",
                String::from_utf8_lossy(&req.key)
            ));
            let id = self.watches.lock().len() as u64 + 1;
            let key = req.key.clone();
            // `/never/` 前缀 → 永不产生事件（用于验证有界观测的超时路径）
            let pending = key.starts_with(b"/never/");
            // 预置两条事件：PUT 后 DELETE（供 JS 侧 next() 消费）
            let events = if pending {
                Vec::new()
            } else {
                vec![
                    WatchEventDto {
                        kind: WatchEventKind::Put,
                        kvs: vec![KvRecord {
                            key: key.clone(),
                            value: b"v1".to_vec(),
                            lease_id: 0,
                            version: 1,
                        }],
                        prev_kv: None,
                        revision: 5,
                    },
                    WatchEventDto {
                        kind: WatchEventKind::Delete,
                        kvs: vec![KvRecord {
                            key,
                            value: Vec::new(),
                            lease_id: 0,
                            version: 2,
                        }],
                        prev_kv: Some(KvRecord {
                            key: Vec::new(),
                            value: b"v1".to_vec(),
                            lease_id: 0,
                            version: 1,
                        }),
                        revision: 6,
                    },
                ]
            };
            self.watches
                .lock()
                .insert((plugin.to_string(), id), StubWatch { events, pending });
            Ok(id)
        }

        async fn watch_next(&self, plugin: &str, id: u64) -> SdkResult<Option<WatchEventDto>> {
            // 先取出结果再 await：`PluginSdkBackend` 的 future 必须是 Send，
            // 不能把 `parking_lot` 守卫带过 await 边界。
            enum Next {
                Pending,
                Event(WatchEventDto),
                Closed,
            }
            let next = {
                let mut watches = self.watches.lock();
                match watches.get_mut(&(plugin.to_string(), id)) {
                    Some(w) if w.pending => Next::Pending,
                    Some(w) if !w.events.is_empty() => Next::Event(w.events.remove(0)),
                    Some(_) => Next::Closed,
                    None => return Err(SdkError::not_found("no such subscription")),
                }
            };
            match next {
                Next::Pending => {
                    // 永不就绪：调用方必须自行加超时（election.observe 的 sleep 竞速）
                    std::future::pending::<()>().await;
                    Ok(None)
                }
                Next::Event(e) => Ok(Some(e)),
                Next::Closed => Ok(None),
            }
        }

        async fn watch_close(&self, plugin: &str, id: u64) -> SdkResult<()> {
            self.watches.lock().remove(&(plugin.to_string(), id));
            Ok(())
        }

        async fn storage_put(
            &self,
            plugin: &str,
            bucket: &str,
            object_id: &[u8],
            data: &[u8],
        ) -> SdkResult<ObjectPutOut> {
            self.calls
                .lock()
                .push(format!("{plugin}:storage_put:{bucket}/{}", object_id.len()));
            Ok(ObjectPutOut {
                revision: 11,
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
                    size: 2,
                    chunks: 1,
                    revision: 11,
                    exists: true,
                    committed: true,
                },
                data: b"hi".to_vec(),
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

        // ──── 流式会话（批次 10）────

        async fn storage_open_write(
            &self,
            plugin: &str,
            bucket: &str,
            object_id: &[u8],
            total_size: u64,
        ) -> SdkResult<u64> {
            self.calls.lock().push(format!(
                "{plugin}:storage_open_write:{bucket}/{}",
                object_id.len()
            ));
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
            // `total == 0` = 未知长度：不设上限（server 侧另按 max_object_size 封顶）。
            if up.total != 0 && up.buf.len() as u64 + data.len() as u64 > up.total {
                return Err(SdkError::invalid_argument(
                    "chunk overflows declared total_size",
                ));
            }
            up.buf.extend_from_slice(data);
            Ok(up.buf.len() as u64)
        }

        async fn storage_commit_write(&self, plugin: &str, id: u64) -> SdkResult<ObjectPutOut> {
            self.calls.lock().push(format!("{plugin}:storage_commit"));
            let up = self
                .uploads
                .lock()
                .remove(&id)
                .ok_or_else(|| SdkError::not_found("upload session not found"))?;
            // `total == 0` = 未知长度：仅要求非空，按实际字节定长。
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
            let size = up.buf.len() as i64;
            self.objects
                .lock()
                .insert((up.bucket.clone(), up.object_id.clone()), up.buf);
            Ok(ObjectPutOut {
                revision: 11,
                size,
                chunks: 1,
            })
        }

        async fn storage_abort_write(&self, plugin: &str, id: u64) -> SdkResult<()> {
            self.calls
                .lock()
                .push(format!("{plugin}:storage_abort:{id}"));
            self.uploads.lock().remove(&id);
            Ok(())
        }

        async fn storage_open_read(
            &self,
            plugin: &str,
            bucket: &str,
            object_id: &[u8],
        ) -> SdkResult<u64> {
            self.calls
                .lock()
                .push(format!("{plugin}:storage_open_read:{bucket}"));
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
                revision: 11,
                exists: true,
                committed: true,
            })
        }

        async fn storage_close_read(&self, plugin: &str, id: u64) -> SdkResult<()> {
            self.calls
                .lock()
                .push(format!("{plugin}:storage_close_read:{id}"));
            self.downloads.lock().remove(&id);
            Ok(())
        }
    }

    fn manifest(name: &str, caps: Vec<PluginCapability>) -> PluginManifest {
        PluginManifest {
            name: name.into(),
            version: "1.0.0".into(),
            runtime: PluginRuntime::Js,
            trust: PluginTrust::FirstParty,
            entry: "index.js".into(),
            capabilities: caps,
            limits: PluginLimits::default(),
            hooks: false,
            source: PluginSource::default(),
        }
    }

    fn cap(id: &str, scope: &str) -> PluginCapability {
        PluginCapability {
            id: id.into(),
            scope: scope.into(),
        }
    }

    struct Harness {
        dir: tempfile::TempDir,
        backend: Arc<StubBackend>,
    }

    impl Harness {
        fn new(source: &str) -> Self {
            let dir = tempfile::tempdir().expect("tempdir");
            std::fs::write(dir.path().join("index.js"), source).expect("write plugin");
            Self {
                dir,
                backend: Arc::new(StubBackend::default()),
            }
        }

        fn loader(&self) -> JsPluginLoader {
            JsPluginLoader::new(
                self.dir.path(),
                Arc::clone(&self.backend) as Arc<dyn PluginSdkBackend>,
                BTreeMap::new(),
            )
        }

        async fn load(&self, m: &PluginManifest) -> Arc<dyn Plugin> {
            self.loader().load(m).await.expect("load")
        }
    }

    const PLUGIN_OK: &str = r#"
export async function handleInvoke(method, payload) {
  if (method === "echo") return payload;
  if (method === "put") {
    const r = await coord.kv.put("/app/counter/a", coord.util.encode("7"));
    return coord.util.encode(String(r.revision));
  }
  if (method === "range") {
    const r = await coord.kv.range("/app/counter/");
    return r.kvs[0].value;
  }
  if (method === "get") {
    return await coord.kv.get("/app/counter/a");
  }
  if (method === "get-missing") {
    try {
      await coord.kv.get("/app/counter/none");
      return coord.util.encode("ALLOWED");
    } catch (e) {
      return coord.util.encode(e.name);
    }
  }
  if (method === "create") {
    try {
      const r = await coord.kv.create("/app/counter/b", coord.util.encode("1"));
      return coord.util.encode("ok:" + String(r.revision));
    } catch (e) {
      return coord.util.encode(e.name);
    }
  }
  if (method === "cap") {
    try {
      await coord.kv.put("/outside/x", "1");
      return coord.util.encode("ALLOWED");
    } catch (e) {
      return coord.util.encode(e.name + "|" + coord.util.isForbidden(e));
    }
  }
  if (method === "lease") {
    const l = await coord.lease.grant(30);
    const h = await coord.lease.keepAlive(l.id);
    await h.stop();
    return coord.util.encode(String(l.id));
  }
  if (method === "watch") {
    const sub = await coord.watch.subscribe("/app/counter/");
    const e1 = await sub.next();
    const e2 = await sub.next();
    const e3 = await sub.next();
    await sub.close();
    const parts = [e1.type, coord.util.decode(e1.kvs[0].value), e2.type, String(e3)];
    return coord.util.encode(parts.join(","));
  }
  if (method === "storage") {
    const put = await coord.storage.put("inbox", "obj-1", coord.util.encode("payload"));
    const got = await coord.storage.get("inbox", "obj-1");
    const stat = await coord.storage.stat("inbox", "obj-1");
    const del = await coord.storage.delete("inbox", "obj-1");
    const parts = [String(put.size), coord.util.decode(got.data), String(stat), String(del.deleted)];
    return coord.util.encode(parts.join(","));
  }
  if (method === "stream") {
    const payload = coord.util.encode("abcdefghij"); // 10 字节
    const w = await coord.storage.openWrite("inbox", "obj-2", payload.length);
    const a = await w.write(payload.subarray(0, 3));
    const b = await w.write(payload.subarray(3, 7));
    const c = await w.write(payload.subarray(7));
    const committed = await w.commit();

    const r = await coord.storage.openRead("inbox", "obj-2");
    const st = await r.stat();
    let got = new Uint8Array(0);
    for (;;) {
      const chunk = await r.read(4);
      if (chunk === null) break;
      const merged = new Uint8Array(got.length + chunk.length);
      merged.set(got);
      merged.set(chunk, got.length);
      got = merged;
    }
    await r.close();
    const parts = [
      "w=" + a + "/" + b + "/" + c,
      "size=" + committed.size,
      "statSize=" + st.size,
      "echo=" + coord.util.decode(got),
    ];
    return coord.util.encode(parts.join(","));
  }
  if (method === "stream-unknown") {
    // 省略 totalSize = 未知长度：commit 时按实际字节定长
    const w = await coord.storage.openWrite("inbox", "obj-u");
    const a = await w.write(coord.util.encode("12345"));
    const b = await w.write(coord.util.encode("678"));
    const committed = await w.commit();

    const r = await coord.storage.openRead("inbox", "obj-u");
    const st = await r.stat();
    let got = new Uint8Array(0);
    for (;;) {
      const chunk = await r.read(4);
      if (chunk === null) break;
      const merged = new Uint8Array(got.length + chunk.length);
      merged.set(got);
      merged.set(chunk, got.length);
      got = merged;
    }
    await r.close();
    const parts = [
      "totalSize=" + String(w.totalSize),
      "w=" + a + "/" + b,
      "size=" + committed.size,
      "statSize=" + st.size,
      "echo=" + coord.util.decode(got),
    ];
    return coord.util.encode(parts.join(","));
  }
  if (method === "boom") throw new Error("intentional failure");
  if (method === "spin") { while (true) { /* 忙等，触发中断超时 */ } }
  throw new Error("unknown method " + method);
}
"#;

    fn caps() -> Vec<PluginCapability> {
        vec![
            cap(CAP_KV_READ, "/app/counter/"),
            cap(CAP_KV_WRITE, "/app/counter/"),
            cap(CAP_KV_DELETE, "/app/counter/"),
            cap(CAP_TXN_EXECUTE, ""),
            cap(CAP_LEASE_GRANT, ""),
            cap(CAP_LEASE_KEEPALIVE, ""),
            cap(CAP_WATCH_SUBSCRIBE, ""),
            cap(CAP_STORAGE_READ, ""),
            cap(CAP_STORAGE_WRITE, ""),
        ]
    }

    /// Phase 3.3 锁/选举插件：租约锁所需的全部能力（lease 类必须空 scope）。
    fn lock_caps() -> Vec<PluginCapability> {
        vec![
            cap(CAP_KV_READ, "/app/lock/"),
            cap(CAP_KV_WRITE, "/app/lock/"),
            cap(CAP_TXN_EXECUTE, ""),
            cap(CAP_LEASE_GRANT, ""),
            cap(CAP_LEASE_REVOKE, ""),
            cap(CAP_LEASE_KEEPALIVE, ""),
            cap(CAP_WATCH_SUBSCRIBE, ""),
        ]
    }

    /// 解析 `k=v,k=v` 形式的结果串。
    fn parse_kv(text: &str) -> BTreeMap<String, String> {
        text.split(',')
            .filter_map(|part| part.split_once('='))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// Phase 3.3 锁/选举测试插件（组合原语 `coord.lock` / `coord.election`）。
    const PLUGIN_LOCKS: &str = r#"
export async function handleInvoke(method, payload) {
  if (method === "exclusive") {
    const first = await coord.lock.acquire("/app/lock/orders", { ttlMs: 5000 });
    if (first === null) return coord.util.encode("NULL");
    const view = await coord.lock.get("/app/lock/orders");
    const second = await coord.lock.acquire("/app/lock/orders", { ttlMs: 5000 });
    const heldBefore = await first.held();
    await first.release();
    const heldAfter = await first.held();
    const third = await coord.lock.acquire("/app/lock/orders", { ttlMs: 5000 });
    const parts = [
      "f1=" + first.fencing,
      "f2=" + (second === null ? "null" : String(second.fencing)),
      "owner=" + view.owner,
      "held=" + heldBefore + "/" + heldAfter,
      "f3=" + (third === null ? "null" : String(third.fencing)),
      "monotonic=" + (third !== null && third.fencing > first.fencing),
    ];
    if (third !== null) await third.release();
    return coord.util.encode(parts.join(","));
  }
  if (method === "campaign") {
    const h = await coord.election.campaign("/app/lock/leader", { ttlMs: 5000 });
    if (h === null) return coord.util.encode("NULL");
    const isLeader = await h.leader();
    const view = await coord.election.leader("/app/lock/leader");
    const rival = await coord.election.campaign("/app/lock/leader", { ttlMs: 5000 });
    const parts = [
      "leader=" + isLeader,
      "view=" + (view === null ? "null" : view.owner),
      "rival=" + (rival === null ? "null" : "GOT"),
      "fencing=" + h.fencing,
    ];
    await h.resign();
    return coord.util.encode(parts.join(","));
  }
  if (method === "observe-event") {
    const e = await coord.election.observe("/app/lock/watch", { timeoutMs: 500 });
    return coord.util.encode(e === null ? "null" : e.type + ":" + e.revision);
  }
  if (method === "observe-timeout") {
    const e = await coord.election.observe("/never/lock/watch", { timeoutMs: 120 });
    return coord.util.encode(e === null ? "TIMEOUT" : e.type);
  }
  if (method === "wait-budget") {
    try {
      await coord.lock.acquire("/app/lock/orders", { waitMs: 60000 });
      return coord.util.encode("ALLOWED");
    } catch (e) {
      return coord.util.encode(e.name);
    }
  }
  if (method === "scope") {
    try {
      await coord.lock.acquire("/outside/lock", { ttlMs: 5000 });
      return coord.util.encode("ALLOWED");
    } catch (e) {
      return coord.util.encode(e.name + "|" + coord.util.isForbidden(e));
    }
  }
  throw new Error("unknown method " + method);
}
"#;

    /// Phase 3.2 验收：`coord.storage.*` 往返（put/get/stat/delete）。
    #[tokio::test(flavor = "multi_thread")]
    async fn js_plugin_round_trips_storage() {
        let h = Harness::new(PLUGIN_OK);
        let p = h.load(&manifest("counter", caps())).await;
        p.init().await.expect("init");
        p.start().await.expect("start");

        let out = p.invoke("storage", b"").await.expect("storage");
        // put.size=7 (payload) / get.data=hi / stat=null（stub 返回 None）/ delete=true
        assert_eq!(out, b"7,hi,null,true");
        let calls = h.backend.calls.lock().clone();
        assert!(
            calls.iter().any(|c| c == "counter:storage_put:inbox/5"),
            "{calls:?}"
        );
        p.stop().await.expect("stop");
    }

    /// 批次 10：`coord.storage.openWrite/openRead` 分块会话（不整块驻留内存）。
    ///
    /// 覆盖：累计写计数（3→7→10）、`commit` 落盘、`reader.stat()`、
    /// 「单次读 ≤ maxLen」的截断（10 字节按 4+4+2 取回）与 `close` 收尾。
    #[tokio::test(flavor = "multi_thread")]
    async fn js_plugin_round_trips_storage_streaming() {
        let h = Harness::new(PLUGIN_OK);
        let p = h.load(&manifest("counter", caps())).await;
        p.init().await.expect("init");
        p.start().await.expect("start");

        let out = p.invoke("stream", b"").await.expect("stream");
        assert_eq!(
            String::from_utf8_lossy(&out),
            "w=3/7/10,size=10,statSize=10,echo=abcdefghij"
        );

        let calls = h.backend.calls.lock().clone();
        assert!(
            calls
                .iter()
                .any(|c| c == "counter:storage_open_write:inbox/5"),
            "{calls:?}"
        );
        assert!(
            calls.iter().any(|c| c == "counter:storage_open_read:inbox"),
            "{calls:?}"
        );
        assert!(
            calls.iter().any(|c| c == "counter:storage_commit"),
            "{calls:?}"
        );
        assert!(
            calls.iter().any(|c| c == "counter:storage_close_read:2"),
            "{calls:?}"
        );
        assert!(
            h.backend.downloads.lock().is_empty(),
            "close() must release the download session"
        );
        assert!(
            h.backend.uploads.lock().is_empty(),
            "commit() must release the upload session"
        );

        p.stop().await.expect("stop");
    }

    /// 批次 11：未知长度分块上传（`openWrite` 省略 `totalSize`）——
    /// 句柄 `totalSize === null`，`commit` 按实际字节定长后 `stat.size` 一致。
    #[tokio::test(flavor = "multi_thread")]
    async fn js_plugin_round_trips_unknown_length_storage_stream() {
        let h = Harness::new(PLUGIN_OK);
        let p = h.load(&manifest("counter", caps())).await;
        p.init().await.expect("init");
        p.start().await.expect("start");

        let out = p
            .invoke("stream-unknown", b"")
            .await
            .expect("stream-unknown");
        assert_eq!(
            String::from_utf8_lossy(&out),
            "totalSize=null,w=5/8,size=8,statSize=8,echo=12345678"
        );

        assert!(
            h.backend.uploads.lock().is_empty(),
            "commit() must release the unknown-length upload session"
        );
        assert!(
            h.backend.downloads.lock().is_empty(),
            "close() must release the download session"
        );

        p.stop().await.expect("stop");
    }

    /// Phase 3.1 验收：`coord.watch.subscribe` → `next()` 事件流 → `close()`。
    #[tokio::test(flavor = "multi_thread")]
    async fn js_plugin_consumes_watch_events() {
        let h = Harness::new(PLUGIN_OK);
        let p = h.load(&manifest("counter", caps())).await;
        p.init().await.expect("init");
        p.start().await.expect("start");

        let out = p.invoke("watch", b"").await.expect("watch");
        assert_eq!(out, b"PUT,v1,DELETE,null");
        // 订阅已在插件内 close → 后端句柄释放
        assert!(h.backend.watches.lock().is_empty());

        p.stop().await.expect("stop");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn js_plugin_round_trips_payload_and_sdk() {
        let h = Harness::new(PLUGIN_OK);
        let p = h.load(&manifest("counter", caps())).await;
        p.init().await.expect("init");
        p.start().await.expect("start");
        assert_eq!(p.status(), PluginStatus::Started);

        // 纯字节往返
        assert_eq!(p.invoke("echo", b"hello").await.expect("echo"), b"hello");

        // SDK：JS 发起 kv.put → 宿主后端
        assert_eq!(p.invoke("put", b"").await.expect("put"), b"3");
        assert_eq!(h.backend.calls.lock().as_slice(), ["counter:put"]);

        // SDK：读回写入的值
        assert_eq!(p.invoke("range", b"").await.expect("range"), b"7");

        // scope 越界 → ErrForbidden
        assert_eq!(
            p.invoke("cap", b"").await.expect("cap"),
            b"ErrForbidden|true"
        );

        // lease + 保活句柄
        assert_eq!(p.invoke("lease", b"").await.expect("lease"), b"1");

        p.stop().await.expect("stop");
        assert_eq!(p.status(), PluginStatus::Stopped);
    }

    /// 批次 8：`coord.kv.get` / `coord.kv.create` 与 WIT / core ABI 共用同一门面
    /// 实现（缺失 → `ErrNotFound`；已存在 → `ErrConflict`），并走调用面钩子/指标。
    #[tokio::test(flavor = "multi_thread")]
    async fn js_plugin_kv_get_and_create_share_host_semantics() {
        let h = Harness::new(PLUGIN_OK);
        let p = h.load(&manifest("counter", caps())).await;
        p.init().await.expect("init");
        p.start().await.expect("start");

        // put 写入 `/app/counter/a` = "7"，get 读回同一值。
        p.invoke("put", b"").await.expect("put");
        assert_eq!(p.invoke("get", b"").await.expect("get"), b"7");

        // 缺失键 → typed `ErrNotFound`（不是返回 undefined / 空值）。
        assert_eq!(
            p.invoke("get-missing", b"").await.expect("get-missing"),
            b"ErrNotFound"
        );

        // create-if-absent：首次成功（返回 revision），重复 → `ErrConflict`。
        let first = p.invoke("create", b"").await.expect("create");
        assert!(
            String::from_utf8_lossy(&first).starts_with("ok:"),
            "first create must succeed: {first:?}"
        );
        assert_eq!(
            p.invoke("create", b"").await.expect("create twice"),
            b"ErrConflict"
        );

        p.stop().await.expect("stop");
    }

    /// Phase 5：JS 插件的调用结果计数进入 AgentMetrics（engine="js"）。
    #[tokio::test(flavor = "multi_thread")]
    async fn js_invocations_are_counted_in_metrics() {
        let h = Harness::new(PLUGIN_OK);
        let metrics = crate::metrics::AgentMetrics::new();
        let p = h
            .loader()
            .with_metrics(metrics.clone())
            .load(&manifest("counter", caps()))
            .await
            .expect("load");
        p.init().await.expect("init");
        p.start().await.expect("start");

        p.invoke("echo", b"a").await.expect("echo");
        let _ = p.invoke("boom", b"").await.unwrap_err();

        let text = metrics.render_prometheus_text();
        assert!(
            text.contains(
                "coord_agent_plugin_invocations_total{plugin=\"counter\",engine=\"js\",outcome=\"ok\"} 1"
            ),
            "{text}"
        );
        assert!(
            text.contains(
                "coord_agent_plugin_invocations_total{plugin=\"counter\",engine=\"js\",outcome=\"error\"} 1"
            ),
            "{text}"
        );
        p.stop().await.expect("stop");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn js_exception_is_isolated_to_the_call() {
        let h = Harness::new(PLUGIN_OK);
        let p = h.load(&manifest("counter", caps())).await;
        p.init().await.expect("init");
        p.start().await.expect("start");

        let err = p.invoke("boom", b"").await.unwrap_err();
        assert!(err.to_string().contains("intentional failure"), "{err}");

        // 插件仍可继续服务（崩溃隔离）
        assert_eq!(
            p.invoke("echo", b"still-alive").await.expect("echo"),
            b"still-alive"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn missing_handler_is_reported() {
        let h = Harness::new("export const nothing = 1;\n");
        let p = h.load(&manifest("empty", caps())).await;
        p.init().await.expect("init");
        p.start().await.expect("start");
        let err = p.invoke("echo", b"").await.unwrap_err();
        assert!(err.to_string().contains("handleInvoke"), "{err}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn module_evaluation_error_fails_init() {
        let h = Harness::new("throw new Error('bad module');\n");
        let p = h.load(&manifest("broken", caps())).await;
        let err = p.init().await.unwrap_err();
        assert!(format!("{err}").contains("initialisation failed"), "{err}");
        assert!(matches!(p.status(), PluginStatus::Failed(_)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn entry_outside_plugin_dir_is_rejected() {
        let h = Harness::new("export function handleInvoke() {}");
        let mut m = manifest("escapee", caps());
        m.entry = "../evil.js".into();
        assert!(h.loader().load(&m).await.is_err());

        m.entry = "sub/../../evil.js".into();
        assert!(h.loader().load(&m).await.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn relative_import_stays_inside_plugin_dir() {
        let h = Harness::new(
            "import { bump } from './lib.js';\n\
             export async function handleInvoke() { return bump(); }\n",
        );
        std::fs::write(
            h.dir.path().join("lib.js"),
            "export function bump() { return 'from-lib'; }\n",
        )
        .expect("write lib");
        let p = h.load(&manifest("multi", caps())).await;
        p.init().await.expect("init");
        p.start().await.expect("start");
        assert_eq!(p.invoke("any", b"").await.expect("invoke"), b"from-lib");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn infinite_loop_is_interrupted_and_isolated() {
        let h = Harness::new(PLUGIN_OK);
        let mut m = manifest("spinner", caps());
        m.limits.max_exec_ms = 300;
        let p = h.load(&m).await;
        p.init().await.expect("init");
        p.start().await.expect("start");

        let err = p.invoke("spin", b"").await.unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("interrupt") || msg.contains("watchdog") || msg.contains("Exception"),
            "unexpected error: {msg}"
        );
        // 超时后 isolate 被丢弃 → 插件 Failed，不再对外服务
        assert!(matches!(p.status(), PluginStatus::Failed(_)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn memory_limit_is_enforced() {
        let h = Harness::new(
            "export async function handleInvoke() {\n\
               const chunks = [];\n\
               for (let i = 0; i < 512; i++) { chunks.push(new Uint8Array(1024 * 1024)); }\n\
               return 'ok';\n\
             }\n",
        );
        let mut m = manifest("hog", caps());
        m.limits.max_memory_mb = 8;
        let p = h.load(&m).await;
        p.init().await.expect("init");
        p.start().await.expect("start");
        let err = p.invoke("any", b"").await.unwrap_err();
        let msg = format!("{err}").to_lowercase();
        assert!(
            msg.contains("memory") || msg.contains("exception") || msg.contains("alloc"),
            "unexpected error: {msg}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn loader_rejects_non_js_runtime() {
        let h = Harness::new("");
        let mut m = manifest("wasmish", caps());
        m.runtime = PluginRuntime::Wasm;
        assert!(h.loader().load(&m).await.is_err());
    }

    /// Phase 3.3 验收：`coord.lock` 互斥 + 隔离令牌单调 + 释放后可再获取。
    ///
    /// stub 后端实现了真实 CAS 语义，因此这里验证的是「租约 + version-CAS」内核；
    /// 真实 server 上的跨插件竞争见 `tests/agent_plugin_js_test.rs`。
    #[tokio::test(flavor = "multi_thread")]
    async fn js_lock_is_mutually_exclusive_with_monotonic_fencing() {
        let h = Harness::new(PLUGIN_LOCKS);
        let p = h.load(&manifest("locks", lock_caps())).await;
        p.init().await.expect("init");
        p.start().await.expect("start");

        let out = p.invoke("exclusive", b"").await.expect("exclusive");
        let text = String::from_utf8(out).expect("utf8");
        let fields = parse_kv(&text);

        assert_eq!(fields.get("owner"), Some(&"locks".to_string()), "{text}");
        assert_eq!(
            fields.get("held"),
            Some(&"true/false".to_string()),
            "{text}"
        );
        assert_eq!(
            fields.get("f2"),
            Some(&"null".to_string()),
            "第二次抢同一把锁必须失败（CAS 互斥）：{text}"
        );
        let f1: i64 = fields.get("f1").expect("f1").parse().expect("f1 int");
        let f3: i64 = fields.get("f3").expect("f3").parse().expect("f3 int");
        assert!(f1 > 0 && f3 > 0, "{text}");
        assert!(f3 > f1, "隔离令牌必须严格单调：{text}");
        assert_eq!(fields.get("monotonic"), Some(&"true".to_string()), "{text}");

        p.stop().await.expect("stop");
    }

    /// Phase 3.3 验收：`coord.election.campaign/resign/leader` + 有界观测。
    #[tokio::test(flavor = "multi_thread")]
    async fn js_election_campaigns_and_observes_leadership() {
        let h = Harness::new(PLUGIN_LOCKS);
        let p = h.load(&manifest("locks", lock_caps())).await;
        p.init().await.expect("init");
        p.start().await.expect("start");

        let out = p.invoke("campaign", b"").await.expect("campaign");
        let text = String::from_utf8(out).expect("utf8");
        let fields = parse_kv(&text);
        assert_eq!(fields.get("leader"), Some(&"true".to_string()), "{text}");
        assert_eq!(fields.get("view"), Some(&"locks".to_string()), "{text}");
        assert_eq!(
            fields.get("rival"),
            Some(&"null".to_string()),
            "同一选举键上同时只能有一位领导：{text}"
        );
        assert!(fields.get("fencing").expect("fencing") != "0", "{text}");

        // 事件路径：订阅到 PUT 事件
        let out = p.invoke("observe-event", b"").await.expect("observe");
        assert_eq!(out, b"PUT:5");
        // 超时路径：无事件的订阅在有界等待后返回 null（不挂死、不触发中断）
        let out = p
            .invoke("observe-timeout", b"")
            .await
            .expect("observe-timeout");
        assert_eq!(out, b"TIMEOUT");

        p.stop().await.expect("stop");
    }

    /// Phase 3.3：等待预算必须小于单次调用执行预算（否则会中断并丢弃 isolate）。
    #[tokio::test(flavor = "multi_thread")]
    async fn js_lock_wait_budget_is_bounded_by_exec_budget() {
        let h = Harness::new(PLUGIN_LOCKS);
        let p = h.load(&manifest("locks", lock_caps())).await;
        p.init().await.expect("init");
        p.start().await.expect("start");

        let out = p.invoke("wait-budget", b"").await.expect("wait-budget");
        assert_eq!(out, b"RangeError");
        // 插件未被中断丢弃 → 状态仍为 Started（isolate 未被销毁）
        assert!(matches!(p.status(), PluginStatus::Started));

        p.stop().await.expect("stop");
    }

    /// Phase 3.3：锁键同样受插件作用域约束（越界在门面层拒绝）。
    #[tokio::test(flavor = "multi_thread")]
    async fn js_lock_key_respects_plugin_scope() {
        let h = Harness::new(PLUGIN_LOCKS);
        let p = h.load(&manifest("locks", lock_caps())).await;
        p.init().await.expect("init");
        p.start().await.expect("start");

        let out = p.invoke("scope", b"").await.expect("scope");
        assert_eq!(out, b"ErrForbidden|true");

        p.stop().await.expect("stop");
    }

    /// Phase 2.2 验收：调用面 `before` 钩子对插件 kv 写请求**可见且可拒绝**。
    #[tokio::test(flavor = "multi_thread")]
    async fn call_face_hook_can_deny_plugin_write() {
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

        let h = Harness::new(PLUGIN_OK);
        let seen = Arc::new(TestMutex::new(Vec::new()));
        let registry = Arc::new(HookRegistry::new());
        registry.register(
            "*",
            Arc::new(DenyWrites {
                seen: Arc::clone(&seen),
            }),
        );

        let loader = h.loader().with_hooks(Arc::clone(&registry));
        let p = loader
            .load(&manifest("counter", caps()))
            .await
            .expect("load");
        p.init().await.expect("init");
        p.start().await.expect("start");

        // 写请求被调用面钩子拒绝 → 插件侧看到 ErrForbidden，后端未被调用
        let err = p.invoke("put", b"").await.unwrap_err();
        assert!(err.to_string().contains("ErrForbidden"), "{err}");
        assert!(h.backend.calls.lock().is_empty());
        assert_eq!(seen.lock().as_slice(), ["kv_put"]);

        // 读请求放行
        assert_eq!(p.invoke("echo", b"ok").await.expect("echo"), b"ok");
        assert_eq!(registry.stats().denied_total, 1);
    }
}
