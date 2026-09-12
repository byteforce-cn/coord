// coord-agent: 原生服务 → 插件 SPI（**真插件**：生命周期与 gRPC 面均由插件拥有）
//
// 历史（已废弃）：`NativePluginAdapter::adopted` 曾是「登记性适配」——生命周期
// 归 `ServiceManager`，gRPC 注册归 `lib.rs` 的硬编码 `add_optional_service` 链，
// 适配器只转发健康检查。结果同一批服务存在**三份清单**（ServiceManager 注册表、
// 硬编码路由链、插件注册表），插件只是壳。
//
// 现在：`PluginManager` 是**唯一**服务宿主——
// - 生命周期：`Plugin::init/start/stop` 直接驱动 `BaseService`；
// - gRPC 面：`Plugin::grpc_service()` 返回 [`AgentGrpcService`]，
//   由 `PluginManager::build_grpc_router` 动态组装（不再有第二份清单）；
// - 健康：`Plugin::health_check` 委托 `BaseService::health_check`，
//   与脚本插件共用同一条健康/观测面。
//
// `is_builtin()` = true：内建插件不参与配置驱动的 reload diff（见 `plugin/mod.rs`）。

use std::sync::Arc;

use async_trait::async_trait;

use crate::plugin::grpc::AgentGrpcService;
use crate::plugin::manifest::{
    PluginEngineConfig, PluginLimits, PluginManifest, PluginRuntime, PluginSource, PluginTrust,
};
use crate::plugin::{Plugin, PluginStatus};
use crate::service::{BaseService, ServiceResult};

/// 原生服务的插件形态：宿主自有服务（内建插件），生命周期与 gRPC 面都由插件拥有。
pub struct NativePluginAdapter {
    manifest: PluginManifest,
    service: Arc<dyn BaseService>,
    /// 本插件对外暴露的 gRPC 服务（无 gRPC 接口的服务为 `None`）
    grpc: Option<AgentGrpcService>,
    started: std::sync::atomic::AtomicBool,
}

impl NativePluginAdapter {
    /// 以默认元数据包装一个原生服务（插件拥有其生命周期）。
    pub fn new(service: Arc<dyn BaseService>) -> Self {
        Self::with_version(service, env!("CARGO_PKG_VERSION"))
    }

    /// 指定版本号包装（版本参与重载 diff）。
    pub fn with_version(service: Arc<dyn BaseService>, version: &str) -> Self {
        let name = service.name().to_string();
        let manifest = PluginManifest {
            name: name.clone(),
            version: version.to_string(),
            runtime: PluginRuntime::Native,
            trust: PluginTrust::FirstParty,
            entry: format!("native:{name}"),
            capabilities: Vec::new(),
            limits: PluginLimits::default(),
            hooks: false,
            source: PluginSource::default(),
        };
        Self {
            manifest,
            service,
            grpc: None,
            started: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// 挂载本插件对外暴露的 gRPC 服务面。
    pub fn with_grpc(mut self, grpc: AgentGrpcService) -> Self {
        self.grpc = Some(grpc);
        self
    }

    /// 覆盖 manifest 字段（如能力声明 / hooks / 限制）。
    pub fn with_manifest(mut self, manifest: PluginManifest) -> Self {
        self.manifest = manifest;
        self
    }

    /// 内部服务句柄（测试 / 装配排查用）。
    pub fn service(&self) -> &Arc<dyn BaseService> {
        &self.service
    }
}

#[async_trait]
impl Plugin for NativePluginAdapter {
    fn name(&self) -> &str {
        &self.manifest.name
    }

    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    async fn init(&self) -> ServiceResult<()> {
        // 原生服务的初始化在其 `start()` 内完成（含 PKI CA get-or-create 之类
        // 需要异步动作的初始化），此处保持 no-op。
        Ok(())
    }

    async fn start(&self) -> ServiceResult<()> {
        self.service.start().await?;
        self.started
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    async fn stop(&self) -> ServiceResult<()> {
        let r = self.service.stop().await;
        self.started
            .store(false, std::sync::atomic::Ordering::Release);
        r
    }

    fn health_check(&self) -> bool {
        self.started.load(std::sync::atomic::Ordering::Acquire) && self.service.health_check()
    }

    fn status(&self) -> PluginStatus {
        if self.started.load(std::sync::atomic::Ordering::Acquire) {
            PluginStatus::Started
        } else {
            PluginStatus::Stopped
        }
    }

    fn grpc_service(&self) -> Option<AgentGrpcService> {
        self.grpc.clone()
    }

    fn is_builtin(&self) -> bool {
        true
    }
}

/// 内置插件集配置（原生服务插件化默认值；`[plugins]` 未启用时为空的零行为路径）。
pub fn default_native_engine_config() -> PluginEngineConfig {
    PluginEngineConfig {
        enabled: false,
        ..PluginEngineConfig::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::ServiceResult;
    use parking_lot::RwLock;

    struct Stub {
        started: RwLock<bool>,
    }

    #[async_trait]
    impl BaseService for Stub {
        fn name(&self) -> &'static str {
            "stub"
        }
        async fn start(&self) -> ServiceResult<()> {
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
    }

    #[tokio::test]
    async fn adapter_forwards_lifecycle_and_keeps_name() {
        let adapter = NativePluginAdapter::new(Arc::new(Stub {
            started: RwLock::new(false),
        }));
        assert_eq!(adapter.name(), "stub");
        assert_eq!(adapter.manifest().runtime, PluginRuntime::Native);
        assert!(adapter.is_builtin(), "native adapters are builtin plugins");
        assert_eq!(adapter.status(), PluginStatus::Stopped);

        adapter.init().await.unwrap();
        adapter.start().await.unwrap();
        assert!(adapter.health_check());
        assert_eq!(adapter.status(), PluginStatus::Started);

        adapter.stop().await.unwrap();
        assert!(!adapter.health_check());
        assert_eq!(adapter.status(), PluginStatus::Stopped);
    }

    /// **真插件**语义：适配器拥有底层服务生命周期（不再是「adopted」登记模式）。
    #[tokio::test]
    async fn adapter_owns_the_underlying_service_lifecycle() {
        let svc = Arc::new(Stub {
            started: RwLock::new(false),
        });
        let adapter = NativePluginAdapter::new(svc.clone());
        adapter.start().await.unwrap();
        assert!(*svc.started.read(), "adapter.start must start the service");
        adapter.stop().await.unwrap();
        assert!(
            !*svc.started.read(),
            "adapter.stop must stop the service (owned lifecycle)"
        );
    }

    /// gRPC 面只在插件**启动后**才随插件暴露（未启动 = 不健康 = 不挂载）。
    #[tokio::test]
    async fn grpc_face_is_exposed_and_health_tracks_lifecycle() {
        let cb = Arc::new(
            crate::services::circuit_breaker::CircuitBreakerService::new(
                5,
                std::time::Duration::from_secs(30),
            ),
        );
        let adapter =
            NativePluginAdapter::new(cb.clone()).with_grpc(AgentGrpcService::CircuitBreaker(cb));
        assert_eq!(adapter.name(), "circuit_breaker");
        assert!(!adapter.health_check(), "not started → not healthy");
        assert!(
            adapter.grpc_service().is_some(),
            "service face must be declared"
        );
        assert_eq!(
            adapter.grpc_service().map(|s| s.name()),
            Some("circuit_breaker")
        );

        adapter.start().await.unwrap();
        assert!(adapter.health_check());
        assert_eq!(adapter.status(), PluginStatus::Started);

        adapter.stop().await.unwrap();
        assert!(!adapter.health_check());
        assert_eq!(adapter.status(), PluginStatus::Stopped);
    }
}
