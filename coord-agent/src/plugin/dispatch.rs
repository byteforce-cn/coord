// coord-agent: 引擎派发加载器
//
// 一个 `PluginLoader` 按 `manifest.runtime` 把插件路由到对应引擎：
// - `js`   → [`JsPluginLoader`]（rquickjs 轻沙箱，第一方编排）
// - `wasm` → [`WasmPluginLoader`]（wasmtime 强沙箱，第三方计算）
// - `native` → 拒绝：原生服务经 `NativePluginAdapter` 注册（不经磁盘加载）。
//
// 两个引擎共用：同一个 `PluginSdkBackend`（出站客户端来源）、同一个
// `HookRegistry`（调用面钩子）、同一份 `[plugins].env` 注入，
// 因此作用域守卫、调用面钩子、能力边界在两条路径上语义一致（D6/D8）。

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;

use crate::plugin::hooks::HookRegistry;
use crate::plugin::manifest::{PluginManifest, PluginRuntime};
use crate::plugin::sdk::PluginSdkBackend;
use crate::plugin::{Plugin, PluginLoader};
use crate::service::ServiceResult;

#[cfg(any(feature = "plugin-js", feature = "plugin-wasm"))]
use crate::plugin::identity::PluginIdentityManager;
#[cfg(feature = "plugin-js")]
use crate::plugin::JsPluginLoader;

#[cfg(feature = "plugin-wasm")]
use crate::plugin::WasmPluginLoader;

/// 按 runtime 派发的插件加载器。
pub struct EnginePluginLoader {
    #[cfg(feature = "plugin-js")]
    js: JsPluginLoader,
    #[cfg(feature = "plugin-wasm")]
    wasm: WasmPluginLoader,
}

impl EnginePluginLoader {
    /// 构建派发加载器。
    ///
    /// `rc` 仅 wasm 引擎需要（宿主函数在 agent 运行时上驱动 coord-client 调用）。
    #[allow(unused_variables)]
    pub fn new(
        dir: impl Into<PathBuf>,
        backend: Arc<dyn PluginSdkBackend>,
        env: BTreeMap<String, String>,
        rc: tokio::runtime::Handle,
    ) -> Result<Self, String> {
        let dir = dir.into();
        Ok(Self {
            #[cfg(feature = "plugin-js")]
            js: JsPluginLoader::new(dir.clone(), Arc::clone(&backend), env.clone()),
            #[cfg(feature = "plugin-wasm")]
            wasm: WasmPluginLoader::new(dir, backend, env, rc)?,
        })
    }

    /// 挂载插件身份管理器（两个引擎一致：每插件独立服务账户 + 受限 CCT）。
    #[cfg(any(feature = "plugin-js", feature = "plugin-wasm"))]
    pub fn with_identity(mut self, identity: Arc<PluginIdentityManager>) -> Self {
        #[cfg(feature = "plugin-js")]
        {
            self.js = self.js.with_identity(Arc::clone(&identity));
        }
        #[cfg(feature = "plugin-wasm")]
        {
            self.wasm = self.wasm.with_identity(identity);
        }
        self
    }

    /// 挂载插件指标（两个引擎一致：调用结果 + sandbox trap 计数）。
    pub fn with_metrics(mut self, metrics: crate::metrics::AgentMetrics) -> Self {
        #[cfg(feature = "plugin-js")]
        {
            self.js = self.js.with_metrics(metrics.clone());
        }
        #[cfg(feature = "plugin-wasm")]
        {
            self.wasm = self.wasm.with_metrics(metrics);
        }
        self
    }

    /// 挂载调用面钩子注册表（两个引擎同时生效）。
    pub fn with_hooks(mut self, hooks: Arc<HookRegistry>) -> Self {
        #[cfg(feature = "plugin-js")]
        {
            self.js = self.js.with_hooks(Arc::clone(&hooks));
        }
        #[cfg(feature = "plugin-wasm")]
        {
            self.wasm = self.wasm.with_hooks(hooks);
        }
        self
    }
}

#[async_trait]
impl PluginLoader for EnginePluginLoader {
    async fn load(&self, manifest: &PluginManifest) -> ServiceResult<Arc<dyn Plugin>> {
        match manifest.runtime {
            #[cfg(feature = "plugin-js")]
            PluginRuntime::Js => self.js.load(manifest).await,
            #[cfg(feature = "plugin-wasm")]
            PluginRuntime::Wasm => self.wasm.load(manifest).await,
            other => Err(format!(
                "no engine enabled for runtime '{}' (plugin '{}'): build with `plugin-js` \
                 and/or `plugin-wasm`",
                other.as_str(),
                manifest.name
            )
            .into()),
        }
    }

    /// 内容指纹按 runtime 派发到对应引擎（Phase 5 版本化重载检测）。
    fn fingerprint(&self, manifest: &PluginManifest) -> Option<String> {
        match manifest.runtime {
            #[cfg(feature = "plugin-js")]
            PluginRuntime::Js => self.js.fingerprint(manifest),
            #[cfg(feature = "plugin-wasm")]
            PluginRuntime::Wasm => self.wasm.fingerprint(manifest),
            _ => None,
        }
    }
}

impl std::fmt::Debug for EnginePluginLoader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnginePluginLoader").finish_non_exhaustive()
    }
}
