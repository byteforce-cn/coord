// coord-agent: 插件 SPI 与生命周期管理
//
// 模块结构：
// - manifest:  插件 manifest + 引擎配置规格
// - native:    `NativePluginAdapter`（把既有 BaseService 统一到插件 SPI，零行为变更）
// - sdk:       宿主导入 SDK（插件 → 协调原语；作用域守卫 + 后端抽象）
// - js_engine: rquickjs 宿主（Phase 1，feature `plugin-js`）
//
// 设计要点：
// - `Plugin` 是对象安全的异步 SPI（load/init/start/stop/health）；
// - `PluginManager` 统一管理生命周期，**单个插件失败不阻塞其余**（崩溃隔离）；
// - `reload` 仅应用插件集 diff（新增/移除/版本替换），结构性变更需重启。

pub mod grpc;
pub mod manifest;
pub mod native;
pub mod sdk;
pub mod server;

pub use grpc::AgentGrpcService;

/// 三条宿主 import 路径的 ABI 账本（单一事实来源 = `wit/coord-plugin.wit`）
pub mod abi;

pub mod identity;

/// 网关层拦截点（Phase 2.1）
pub mod gateway;

/// 调用面 typed 钩子（Phase 2.2）
pub mod hooks;

#[cfg(feature = "plugin-js")]
pub mod js_engine;

#[cfg(feature = "plugin-wasm")]
pub mod wasm_engine;

#[cfg(feature = "plugin-wasm")]
pub mod component_engine;

#[cfg(feature = "plugin-js")]
pub use js_engine::JsPluginLoader;

#[cfg(feature = "plugin-wasm")]
pub use wasm_engine::WasmPluginLoader;

#[cfg(any(feature = "plugin-js", feature = "plugin-wasm"))]
pub mod dispatch;

#[cfg(any(feature = "plugin-js", feature = "plugin-wasm"))]
pub use dispatch::EnginePluginLoader;

pub use gateway::{
    GatewayDecision, GatewayHook, GatewayIdentity, GatewayRequestCtx, PluginGateway,
    PluginGatewayLayer,
};
pub use hooks::{CallCtx, CallDecision, CallHook, CallOp, CallOutcome, HookRegistry, HookStats};

pub use manifest::{
    PluginCapability, PluginEngineConfig, PluginLimits, PluginManifest, PluginRuntime,
    PluginSource, PluginTrust,
};
pub use native::NativePluginAdapter;
pub use server::PluginService;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;

use crate::service::ServiceResult;

// ──── 插件状态 ────

/// 插件生命周期状态。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PluginStatus {
    /// 已加载（未启动）
    Loaded,
    /// 运行中
    Started,
    /// 已停止
    Stopped,
    /// 失败（原因；需人工介入或下次重载重试）
    Failed(String),
}

impl PluginStatus {
    /// 稳定的字符串表示（观测 / gRPC List 面）。
    pub fn as_str(&self) -> &str {
        match self {
            PluginStatus::Loaded => "loaded",
            PluginStatus::Started => "started",
            PluginStatus::Stopped => "stopped",
            PluginStatus::Failed(_) => "failed",
        }
    }
}

// ──── 插件 SPI ────

/// 插件统一接口（对象安全）。
#[async_trait]
pub trait Plugin: Send + Sync {
    /// 插件名（manifest.name）
    fn name(&self) -> &str;

    /// manifest 引用
    fn manifest(&self) -> &PluginManifest;

    /// 初始化：注入 SDK / 建立资源（此时不对外服务）
    async fn init(&self) -> ServiceResult<()>;

    /// 启动：开始对外服务
    async fn start(&self) -> ServiceResult<()>;

    /// 停止：释放资源（幂等）
    async fn stop(&self) -> ServiceResult<()>;

    /// 健康检查
    fn health_check(&self) -> bool;

    /// 当前状态
    fn status(&self) -> PluginStatus;

    /// 调用插件方法（请求/响应为不透明 bytes）。
    ///
    /// 默认 UNIMPLEMENTED：不可被调用的插件（如纯观测型原生适配器）无需实现。
    async fn invoke(&self, _method: &str, _payload: &[u8]) -> ServiceResult<Vec<u8>> {
        Err(format!("plugin '{}' does not support invoke", self.name()).into())
    }

    /// 已加载产物的内容指纹（`sha256:<hex>`）。
    ///
    /// `None` = 该插件不参与内容级重载检测（如原生适配器），此时
    /// `PluginManager::reload` 仅按 manifest `version` 判定是否替换。
    fn content_fingerprint(&self) -> Option<String> {
        None
    }

    /// 本插件对外暴露的 gRPC 服务面（无 gRPC 接口 → `None`）。
    ///
    /// 由 [`PluginManager::build_grpc_router`] 在服务链组装期调用：
    /// **已启动**的插件才注册其 gRPC 服务（未启动 = 不对外服务，fail-closed），
    /// 因此服务的暴露与生命周期状态天然一致。
    fn grpc_service(&self) -> Option<AgentGrpcService> {
        None
    }

    /// 是否为内建插件（宿主自有服务，如原生服务适配器）。
    ///
    /// 内建插件**不参与**配置驱动的 `reload` diff：配置里写不出它们，
    /// 它们也不会被「期望集」里缺席而移除。
    fn is_builtin(&self) -> bool {
        false
    }
}

/// 插件加载器：把 manifest 变成可运行插件（JS/wasm 引擎在后续阶段实现）。
#[async_trait]
pub trait PluginLoader: Send + Sync {
    /// 按 manifest 加载插件。
    async fn load(&self, manifest: &PluginManifest) -> ServiceResult<Arc<dyn Plugin>>;

    /// 计算 manifest 指向的产物**当前**内容指纹（不加载）。
    ///
    /// 与 [`Plugin::content_fingerprint`] 比较即可发现「同版本内容变更」
    /// （就地替换插件文件是运维常见做法）。默认 `None` = 不做内容级检测。
    fn fingerprint(&self, _manifest: &PluginManifest) -> Option<String> {
        None
    }
}

/// 归一化插件入口路径（消除 `.` / `..`），用于「不得逃出插件目录」检查。
///
/// JS / wasm 两个引擎共用：目录逃逸是加载期安全边界（§8.1）。
#[cfg(any(feature = "plugin-js", feature = "plugin-wasm"))]
pub(crate) fn normalize_plugin_path(path: &std::path::Path) -> std::path::PathBuf {
    use std::path::Component;
    let mut out = std::path::PathBuf::new();
    for comp in path.components() {
        match comp {
            Component::ParentDir => {
                out.pop();
            }
            Component::CurDir => {}
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// 插件产物内容指纹：`sha256:<hex>`（Phase 5「版本化」——同版本内容变更也触发重载）。
///
/// 指纹只覆盖**入口文件字节**：manifest 的结构性变更（能力/限制/hooks）由
/// `version` 变更表达；两者任一变化都会导致重载。
#[cfg(any(feature = "plugin-js", feature = "plugin-wasm"))]
pub(crate) fn content_fingerprint_bytes(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

/// 读取入口文件并计算内容指纹（读不到 → `None`，此时仅按 `version` 判定）。
#[cfg(any(feature = "plugin-js", feature = "plugin-wasm"))]
pub(crate) fn content_fingerprint(path: &std::path::Path) -> Option<String> {
    std::fs::read(path)
        .ok()
        .map(|bytes| content_fingerprint_bytes(&bytes))
}

// ──── 重载报告 ────

/// 插件集 diff 应用结果。
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PluginReloadReport {
    /// 新增并启动
    pub added: Vec<String>,
    /// 移除并停止
    pub removed: Vec<String>,
    /// 版本替换（移除旧 + 加载新）
    pub replaced: Vec<String>,
    /// 因缺少可用加载器而延后（结构性变更/引擎未启用）
    pub deferred: Vec<String>,
    /// 失败（插件名，原因）
    pub failed: Vec<(String, String)>,
}

impl PluginReloadReport {
    /// 是否有任何变更生效。
    pub fn is_noop(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.replaced.is_empty()
    }
}

// ──── 插件管理器 ────

/// 插件管理器：注册表 + 生命周期 + 崩溃隔离。
pub struct PluginManager {
    plugins: RwLock<BTreeMap<String, Arc<dyn Plugin>>>,
    config: PluginEngineConfig,
    /// 插件指标（加载/启动失败计数；观测面 Phase 5）
    metrics: Option<crate::metrics::AgentMetrics>,
}

impl PluginManager {
    /// 由引擎配置构建（空注册表）。
    pub fn new(config: PluginEngineConfig) -> Self {
        Self {
            plugins: RwLock::new(BTreeMap::new()),
            config,
            metrics: None,
        }
    }

    /// 挂载插件指标（加载/启动失败计数）。
    pub fn with_metrics(mut self, metrics: crate::metrics::AgentMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// 记录一次插件加载/启动失败（指标）。
    fn note_failure(&self, name: &str) {
        if let Some(metrics) = &self.metrics {
            metrics.record_plugin_load_failure(name);
        }
    }

    /// 引擎配置引用。
    pub fn config(&self) -> &PluginEngineConfig {
        &self.config
    }

    /// 引擎是否启用。
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// 注册一个已构建的插件（同名拒绝）。
    pub async fn register(&self, plugin: Arc<dyn Plugin>) -> ServiceResult<()> {
        let name = plugin.name().to_string();
        let mut plugins = self.plugins.write();
        if plugins.contains_key(&name) {
            return Err(format!("plugin '{name}' is already registered").into());
        }
        plugins.insert(name, plugin);
        Ok(())
    }

    /// 注册一个**内建**插件（宿主自有服务：原生服务适配器、插件调用面）。
    ///
    /// 与 [`Self::register`] 的区别只有一点：内建插件不参与 `reload` diff
    /// （既不会被移除，也不会因配置里没有同名条目而被判定缺席）。
    pub async fn register_builtin(&self, plugin: Arc<dyn Plugin>) -> ServiceResult<()> {
        let name = plugin.name().to_string();
        if !plugin.is_builtin() {
            return Err(format!("plugin '{name}' is not marked as builtin").into());
        }
        self.register(plugin).await
    }

    /// 内建插件名（有序）。
    pub fn builtin_names(&self) -> Vec<String> {
        self.plugins
            .read()
            .iter()
            .filter(|(_, p)| p.is_builtin())
            .map(|(n, _)| n.clone())
            .collect()
    }

    /// 动态组装 gRPC 服务链：遍历**已启动**插件，逐个挂其服务面。
    ///
    /// 取代历史上 `lib.rs` 中硬编码的 `add_optional_service` 长链：
    /// 服务的「存在与否」由插件注册表决定，「是否对外」由插件状态决定
    /// （启动失败的插件不会暴露半死的 gRPC 面）。
    pub fn build_grpc_router<L>(
        &self,
        mut router: tonic::transport::server::Router<L>,
    ) -> tonic::transport::server::Router<L> {
        let plugins = self.plugins.read();
        for (name, plugin) in plugins.iter() {
            if plugin.status() != PluginStatus::Started {
                continue;
            }
            if let Some(svc) = plugin.grpc_service() {
                tracing::debug!("plugin '{name}': mounting gRPC service '{}'", svc.name());
                router = svc.add_to(router);
            }
        }
        router
    }

    /// 已注册插件名（有序）。
    pub fn names(&self) -> Vec<String> {
        self.plugins.read().keys().cloned().collect()
    }

    /// 已注册插件数。
    pub fn len(&self) -> usize {
        self.plugins.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 取插件句柄（供调用面使用）。
    pub fn get(&self, name: &str) -> Option<Arc<dyn Plugin>> {
        self.plugins.read().get(name).cloned()
    }

    /// 查询单个插件状态。
    pub fn status_of(&self, name: &str) -> Option<PluginStatus> {
        self.plugins.read().get(name).map(|p| p.status())
    }

    /// 启动全部插件。
    ///
    /// **崩溃隔离**：单个插件 init/start 失败只标记该插件 `Failed`，
    /// 记录错误并继续启动其余插件；返回失败列表（空 = 全部成功）。
    pub async fn start_all(&self) -> Vec<(String, String)> {
        // 先 clone 出 (name, plugin) 列表，避免持锁跨 await
        let items: Vec<(String, Arc<dyn Plugin>)> = self
            .plugins
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), Arc::clone(v)))
            .collect();

        let mut failures = Vec::new();
        for (name, plugin) in items {
            if let Err(e) = plugin.init().await {
                let msg = format!("init failed: {e}");
                tracing::error!("plugin '{name}' {msg}; isolating");
                self.note_failure(&name);
                failures.push((name, msg));
                continue;
            }
            if let Err(e) = plugin.start().await {
                let msg = format!("start failed: {e}");
                tracing::error!("plugin '{name}' {msg}; isolating");
                self.note_failure(&name);
                failures.push((name, msg));
                continue;
            }
            tracing::info!("plugin '{name}' started");
        }
        failures
    }

    /// 停止全部插件（逆序，幂等；单个失败不阻塞其余）。
    pub async fn stop_all(&self) {
        let items: Vec<(String, Arc<dyn Plugin>)> = self
            .plugins
            .read()
            .iter()
            .rev()
            .map(|(k, v)| (k.clone(), Arc::clone(v)))
            .collect();
        for (name, plugin) in items {
            if let Err(e) = plugin.stop().await {
                tracing::error!("plugin '{name}' stop failed: {e}");
            }
        }
    }

    /// 健康检查：(healthy, total, unhealthy_names)。
    pub fn health_check_all(&self) -> (usize, usize, Vec<String>) {
        let plugins = self.plugins.read();
        let total = plugins.len();
        let mut healthy = 0;
        let mut unhealthy = Vec::new();
        for (name, p) in plugins.iter() {
            if p.health_check() {
                healthy += 1;
            } else {
                unhealthy.push(name.clone());
            }
        }
        (healthy, total, unhealthy)
    }

    /// 应用插件集 diff（新增 / 移除 / 版本替换）。
    ///
    /// - `desired`：期望的插件集（来自重载后的配置）；
    /// - `loader`：可选的加载器（Phase 0 骨架无 JS/wasm 加载器，此时新增条目
    ///   记入 `deferred`）；
    /// - 结构性变更（引擎开关 / 默认限制 / 引擎参数）不在本方法职责内，需重启。
    pub async fn reload(
        &self,
        desired: &[PluginManifest],
        loader: Option<&dyn PluginLoader>,
    ) -> PluginReloadReport {
        let mut report = PluginReloadReport::default();

        let mut wanted: BTreeMap<String, PluginManifest> = BTreeMap::new();
        for m in desired {
            if let Err(e) = m.validate() {
                self.note_failure(&m.name);
                report.failed.push((m.name.clone(), e));
                continue;
            }
            wanted.insert(m.name.clone(), m.clone());
        }

        // 内建插件（原生服务 / 插件调用面）不参与配置 diff：配置写不出它们，
        // 也不会因为它们不在 desired 里而被摘掉。
        let builtins: BTreeSet<String> = self
            .plugins
            .read()
            .iter()
            .filter(|(_, p)| p.is_builtin())
            .map(|(n, _)| n.clone())
            .collect();
        for name in &builtins {
            if wanted.contains_key(name) {
                self.note_failure(name);
                report.failed.push((
                    name.clone(),
                    "name collides with a builtin plugin (native service); rename the entry".into(),
                ));
                wanted.remove(name);
            }
        }

        let current: BTreeSet<String> = self
            .plugins
            .read()
            .keys()
            .filter(|k| !builtins.contains(*k))
            .cloned()
            .collect();
        let wanted_keys: BTreeSet<String> = wanted.keys().cloned().collect();

        // 1. 移除不再期望的插件
        for name in current.difference(&wanted_keys) {
            let existing = self.plugins.write().remove(name);
            if let Some(p) = existing {
                if let Err(e) = p.stop().await {
                    self.note_failure(name);
                    report
                        .failed
                        .push((name.clone(), format!("stop failed: {e}")));
                }
                report.removed.push(name.clone());
            }
        }

        // 2. 新增 / 版本替换 / 内容变更替换
        for (name, m) in wanted {
            let existing = self.plugins.read().get(&name).cloned();
            let existing_version = existing.as_ref().map(|p| p.manifest().version.clone());

            // 内容指纹（Phase 5 版本化）：同版本但入口文件字节变化 → 也要重载。
            // loader 缺席（skeleton）时无指纹信息 → 退化为纯 version 判定。
            let desired_fingerprint = loader.and_then(|l| l.fingerprint(&m));
            let content_changed = match (&desired_fingerprint, existing.as_ref()) {
                (Some(desired), Some(current)) => match current.content_fingerprint() {
                    Some(loaded) => *desired != loaded,
                    None => false,
                },
                _ => false,
            };

            let needs_load = match &existing_version {
                None => true,
                Some(v) if *v != m.version || content_changed => {
                    // 版本替换 / 内容变更：先停旧
                    let old = self.plugins.write().remove(&name);
                    if let Some(p) = old {
                        let _ = p.stop().await;
                    }
                    true
                }
                Some(_) => false,
            };
            if !needs_load {
                continue;
            }

            let replaced = existing_version.is_some();
            match loader {
                Some(l) => match l.load(&m).await {
                    Ok(plugin) => {
                        if let Err(e) = self.register(plugin).await {
                            self.note_failure(&name);
                            report.failed.push((name.clone(), e.to_string()));
                            continue;
                        }
                        let loaded = self.plugins.read().get(&name).cloned();
                        if let Some(p) = loaded {
                            if let Err(e) = p.init().await {
                                self.note_failure(&name);
                                report
                                    .failed
                                    .push((name.clone(), format!("init failed: {e}")));
                                continue;
                            }
                            // 重载后直接进入服务（与 start_all 语义对齐）
                            if let Err(e) = p.start().await {
                                self.note_failure(&name);
                                report
                                    .failed
                                    .push((name.clone(), format!("start failed: {e}")));
                                continue;
                            }
                        }
                        if replaced {
                            report.replaced.push(name.clone());
                        } else {
                            report.added.push(name.clone());
                        }
                    }
                    Err(e) => {
                        self.note_failure(&name);
                        report.failed.push((name.clone(), e.to_string()));
                    }
                },
                None => {
                    report.deferred.push(name.clone());
                    tracing::info!(
                        "plugin '{name}' deferred (no loader available yet; restart required)"
                    );
                }
            }
        }

        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::manifest::{PluginLimits, PluginRuntime, PluginSource, PluginTrust};

    struct StubPlugin {
        manifest: PluginManifest,
        started: RwLock<bool>,
        fail_start: bool,
        /// 供内容指纹测试使用（None = 不参与内容级重载）
        fingerprint: Option<String>,
        /// 内建插件（原生服务形态）
        builtin: bool,
    }

    impl StubPlugin {
        fn new(name: &str, fail_start: bool) -> Self {
            Self {
                manifest: PluginManifest {
                    name: name.into(),
                    version: "1.0.0".into(),
                    runtime: PluginRuntime::Js,
                    trust: PluginTrust::FirstParty,
                    entry: "index.js".into(),
                    capabilities: vec![],
                    limits: PluginLimits::default(),
                    hooks: false,
                    source: PluginSource::default(),
                },
                started: RwLock::new(false),
                fail_start,
                fingerprint: None,
                builtin: false,
            }
        }

        fn with_builtin(mut self) -> Self {
            self.builtin = true;
            self
        }

        fn with_fingerprint(mut self, fp: Option<&str>) -> Self {
            self.fingerprint = fp.map(|s| s.to_string());
            self
        }
    }

    #[async_trait]
    impl Plugin for StubPlugin {
        fn name(&self) -> &str {
            &self.manifest.name
        }
        fn manifest(&self) -> &PluginManifest {
            &self.manifest
        }
        async fn init(&self) -> ServiceResult<()> {
            Ok(())
        }
        async fn start(&self) -> ServiceResult<()> {
            if self.fail_start {
                return Err("boom".into());
            }
            *self.started.write() = true;
            Ok(())
        }
        async fn stop(&self) -> ServiceResult<()> {
            *self.started.write() = false;
            Ok(())
        }
        fn health_check(&self) -> bool {
            *self.started.read()
        }
        fn status(&self) -> PluginStatus {
            if *self.started.read() {
                PluginStatus::Started
            } else {
                PluginStatus::Stopped
            }
        }
        fn content_fingerprint(&self) -> Option<String> {
            self.fingerprint.clone()
        }

        fn is_builtin(&self) -> bool {
            self.builtin
        }
    }

    /// 内容指纹可配置的加载器（模拟 JS/wasm 引擎的文件哈希）。
    struct FingerprintLoader {
        fingerprints: BTreeMap<String, String>,
    }

    #[async_trait]
    impl PluginLoader for FingerprintLoader {
        async fn load(&self, manifest: &PluginManifest) -> ServiceResult<Arc<dyn Plugin>> {
            let mut plugin = StubPlugin::new(&manifest.name, false)
                .with_fingerprint(self.fingerprints.get(&manifest.name).map(|s| s.as_str()));
            plugin.manifest = manifest.clone();
            Ok(Arc::new(plugin))
        }

        fn fingerprint(&self, manifest: &PluginManifest) -> Option<String> {
            self.fingerprints.get(&manifest.name).cloned()
        }
    }

    /// 同名注册被拒绝（与已删除的 `ServiceManager::register` 同语义）。
    #[tokio::test]
    async fn duplicate_registration_is_rejected() {
        let manager = PluginManager::new(PluginEngineConfig::default());
        manager
            .register(Arc::new(StubPlugin::new("dup", false)))
            .await
            .unwrap();
        let err = manager
            .register(Arc::new(StubPlugin::new("dup", false)))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("already registered"), "{err}");
        assert_eq!(manager.len(), 1);
    }

    /// 生命周期与健康汇总覆盖**全部**插件（内建原生服务与脚本插件一视同仁）。
    #[tokio::test]
    async fn lifecycle_and_health_cover_every_registered_plugin() {
        let manager = PluginManager::new(PluginEngineConfig::default());
        manager
            .register_builtin(Arc::new(
                StubPlugin::new("native-like", false).with_builtin(),
            ))
            .await
            .unwrap();
        manager
            .register(Arc::new(StubPlugin::new("script", false)))
            .await
            .unwrap();

        let (healthy, total, unhealthy) = manager.health_check_all();
        assert_eq!((healthy, total), (0, 2));
        assert_eq!(unhealthy.len(), 2, "nothing started yet");

        let failures = manager.start_all().await;
        assert!(failures.is_empty(), "{failures:?}");
        let (healthy, total, unhealthy) = manager.health_check_all();
        assert_eq!((healthy, total), (2, 2));
        assert!(unhealthy.is_empty());

        manager.stop_all().await;
        assert_eq!(manager.health_check_all().0, 0, "stop_all must stop all");
    }

    /// `register_builtin` 只接受带内建标记的插件（防止把普通插件错登记成宿主自有服务）。
    #[tokio::test]
    async fn register_builtin_requires_the_marker() {
        let manager = PluginManager::new(PluginEngineConfig::default());
        let err = manager
            .register_builtin(Arc::new(StubPlugin::new("p", false)))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not marked as builtin"), "{err}");
        assert!(manager.is_empty());
    }

    /// 内建插件不参与配置 diff：`reload` 不会因为「期望集里没有它」而摘掉原生服务。
    #[tokio::test]
    async fn reload_preserves_builtin_plugins() {
        let manager = PluginManager::new(PluginEngineConfig::default());
        manager
            .register_builtin(Arc::new(StubPlugin::new("registry", false).with_builtin()))
            .await
            .unwrap();
        manager
            .register(Arc::new(StubPlugin::new("user-plugin", false)))
            .await
            .unwrap();
        manager.start_all().await;

        let report = manager.reload(&[], None).await;
        assert_eq!(report.removed, vec!["user-plugin".to_string()]);
        assert!(
            manager.get("registry").is_some(),
            "builtin service plugin must survive a config reload"
        );
        assert_eq!(manager.builtin_names(), vec!["registry".to_string()]);
    }

    /// 配置条目与内建服务撞名 → 明确失败（不得悄悄替换宿主自有服务）。
    #[tokio::test]
    async fn reload_rejects_entry_colliding_with_builtin() {
        let manager = PluginManager::new(PluginEngineConfig::default());
        manager
            .register_builtin(Arc::new(StubPlugin::new("cache", false).with_builtin()))
            .await
            .unwrap();
        manager.start_all().await;

        let manifest = StubPlugin::new("cache", false).manifest;
        let report = manager.reload(&[manifest], None).await;
        assert_eq!(report.failed.len(), 1, "{report:?}");
        assert!(report.failed[0].1.contains("builtin"), "{report:?}");
        assert!(
            manager
                .get("cache")
                .map(|p| p.is_builtin())
                .unwrap_or(false),
            "the builtin must be untouched by the colliding entry"
        );
    }

    #[tokio::test]
    async fn start_all_isolates_failures() {
        let manager = PluginManager::new(PluginEngineConfig::default());
        manager
            .register(Arc::new(StubPlugin::new("good", false)))
            .await
            .unwrap();
        manager
            .register(Arc::new(StubPlugin::new("bad", true)))
            .await
            .unwrap();

        let failures = manager.start_all().await;
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].0, "bad");
        // good 仍健康（崩溃隔离）
        let (healthy, total, unhealthy) = manager.health_check_all();
        assert_eq!(total, 2);
        assert_eq!(healthy, 1);
        assert_eq!(unhealthy, vec!["bad".to_string()]);

        manager.stop_all().await;
        assert_eq!(manager.health_check_all().0, 0);
    }

    #[tokio::test]
    async fn reload_removes_and_defers_without_loader() {
        let manager = PluginManager::new(PluginEngineConfig::default());
        manager
            .register(Arc::new(StubPlugin::new("old", false)))
            .await
            .unwrap();
        manager.start_all().await;

        let mut desired = StubPlugin::new("new", false).manifest;
        desired.name = "new".into();
        let report = manager.reload(&[desired], None).await;
        assert_eq!(report.removed, vec!["old".to_string()]);
        assert_eq!(report.deferred, vec!["new".to_string()]);
        assert!(report.failed.is_empty());
        assert_eq!(manager.names(), Vec::<String>::new());
    }

    /// Phase 5 版本化：**同版本但内容变更**（就地替换插件文件）也要重载；
    /// 内容与版本都不变 → no-op（避免 SIGHUP 抖动导致插件重启）。
    #[tokio::test]
    async fn reload_replaces_on_content_change_without_version_bump() {
        let manager = PluginManager::new(PluginEngineConfig::default());
        let manifest = StubPlugin::new("p", false).manifest;
        let v_a = FingerprintLoader {
            fingerprints: [("p".to_string(), "sha256:a".to_string())].into(),
        };

        manager
            .register(Arc::new(
                StubPlugin::new("p", false).with_fingerprint(Some("sha256:a")),
            ))
            .await
            .unwrap();
        manager.start_all().await;

        // 同版本 + 同内容 → no-op（插件不被重启）
        let report = manager
            .reload(std::slice::from_ref(&manifest), Some(&v_a))
            .await;
        assert!(
            report.is_noop(),
            "identical content must not reload: {report:?}"
        );
        assert_eq!(manager.status_of("p"), Some(PluginStatus::Started));

        // 同版本 + 内容变更 → replaced
        let v_b = FingerprintLoader {
            fingerprints: [("p".to_string(), "sha256:b".to_string())].into(),
        };
        let report = manager
            .reload(std::slice::from_ref(&manifest), Some(&v_b))
            .await;
        assert_eq!(report.replaced, vec!["p".to_string()]);
        assert!(report.added.is_empty() && report.failed.is_empty());
        assert_eq!(
            manager.get("p").unwrap().content_fingerprint().as_deref(),
            Some("sha256:b"),
            "reloaded plugin must expose the new fingerprint"
        );
        assert_eq!(manager.status_of("p"), Some(PluginStatus::Started));
    }

    /// 插件不暴露指纹（原生适配器 / 无 loader）→ 仅按 version 判定，不会误重载。
    #[tokio::test]
    async fn reload_ignores_content_when_no_fingerprint_available() {
        let manager = PluginManager::new(PluginEngineConfig::default());
        let manifest = StubPlugin::new("p", false).manifest;
        manager
            .register(Arc::new(StubPlugin::new("p", false)))
            .await
            .unwrap();
        manager.start_all().await;

        // loader 有指纹，但已加载插件不暴露指纹 → 保守 no-op
        let loader = FingerprintLoader {
            fingerprints: [("p".to_string(), "sha256:b".to_string())].into(),
        };
        let report = manager
            .reload(std::slice::from_ref(&manifest), Some(&loader))
            .await;
        assert!(report.is_noop(), "no loaded fingerprint → must not reload");

        // loader 不支持指纹（默认实现）→ 同样 no-op
        struct NoFingerprintLoader;
        #[async_trait]
        impl PluginLoader for NoFingerprintLoader {
            async fn load(&self, _m: &PluginManifest) -> ServiceResult<Arc<dyn Plugin>> {
                Err("unused".into())
            }
        }
        let report = manager
            .reload(std::slice::from_ref(&manifest), Some(&NoFingerprintLoader))
            .await;
        assert!(report.is_noop());
    }

    /// 内容指纹随入口字节变化（sha256 前缀 + 确定性）。
    #[cfg(any(feature = "plugin-js", feature = "plugin-wasm"))]
    #[test]
    fn content_fingerprint_is_deterministic_sha256() {
        let a = content_fingerprint_bytes(b"export default 1;");
        let b = content_fingerprint_bytes(b"export default 1;");
        let c = content_fingerprint_bytes(b"export default 2;");
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert!(a.starts_with("sha256:"), "fingerprint must be tagged: {a}");
        assert_eq!(a.len(), "sha256:".len() + 64);
    }
}
