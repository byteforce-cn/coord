// coord-agent: 通用插件调用面（coord.plugin.Plugin）
//
// v1 通用调用：`(plugin_id, method, payload bytes) → payload bytes`。
// 载荷语义由插件自行定义（内部约定），宿主只做路由与错误映射。
// 作用域：agent 本地服务（不经 server 代理）。

use std::sync::Arc;

use tonic::{Request, Response, Status};

use coord_proto::plugin::plugin_server::Plugin as PluginTrait;
use coord_proto::plugin::{
    InvokeRequest, InvokeResponse, ListPluginsRequest, ListPluginsResponse, PluginInfo,
};

use crate::plugin::PluginManager;

/// 插件调用 gRPC 服务。
pub struct PluginService {
    manager: Arc<PluginManager>,
}

impl PluginService {
    pub fn new(manager: Arc<PluginManager>) -> Self {
        Self { manager }
    }
}

#[tonic::async_trait]
impl PluginTrait for PluginService {
    async fn invoke(
        &self,
        request: Request<InvokeRequest>,
    ) -> Result<Response<InvokeResponse>, Status> {
        let req = request.into_inner();
        if req.plugin_id.trim().is_empty() {
            return Err(Status::invalid_argument("plugin_id must not be empty"));
        }
        let plugin = self
            .manager
            .get(&req.plugin_id)
            .ok_or_else(|| Status::not_found(format!("plugin '{}' not loaded", req.plugin_id)))?;

        // 未启动的插件不对外服务（与 BaseService 健康语义一致）
        if plugin.status() != crate::plugin::PluginStatus::Started {
            return Err(Status::unavailable(format!(
                "plugin '{}' is not running (status={})",
                req.plugin_id,
                plugin.status().as_str()
            )));
        }

        match plugin.invoke(&req.method, &req.payload).await {
            Ok(payload) => Ok(Response::new(InvokeResponse { payload })),
            Err(e) => Err(Status::internal(format!(
                "plugin '{}' invoke '{}' failed: {e}",
                req.plugin_id, req.method
            ))),
        }
    }

    async fn list(
        &self,
        _request: Request<ListPluginsRequest>,
    ) -> Result<Response<ListPluginsResponse>, Status> {
        let mut plugins = Vec::new();
        for name in self.manager.names() {
            if let Some(p) = self.manager.get(&name) {
                let m = p.manifest();
                plugins.push(PluginInfo {
                    name: m.name.clone(),
                    version: m.version.clone(),
                    runtime: m.runtime.as_str().to_string(),
                    trust: m.trust.as_str().to_string(),
                    status: p.status().as_str().to_string(),
                    // 统一健康面：原生服务（内建插件）与脚本插件一致上报，
                    // 取代历史上「服务健康只进日志」的状况。
                    healthy: p.health_check(),
                    builtin: p.is_builtin(),
                });
            }
        }
        Ok(Response::new(ListPluginsResponse { plugins }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::manifest::{
        PluginEngineConfig, PluginLimits, PluginManifest, PluginRuntime, PluginSource, PluginTrust,
    };
    use crate::plugin::{Plugin, PluginStatus};
    use crate::service::ServiceResult;
    use async_trait::async_trait;

    /// 参考插件：回显 `method:payload`。
    struct EchoPlugin {
        manifest: PluginManifest,
        started: std::sync::atomic::AtomicBool,
    }

    impl EchoPlugin {
        fn new() -> Self {
            Self {
                manifest: PluginManifest {
                    name: "echo".into(),
                    version: "1.0.0".into(),
                    runtime: PluginRuntime::Native,
                    trust: PluginTrust::FirstParty,
                    entry: "native:echo".into(),
                    capabilities: vec![],
                    limits: PluginLimits::default(),
                    hooks: false,
                    source: PluginSource::default(),
                },
                started: std::sync::atomic::AtomicBool::new(false),
            }
        }
    }

    #[async_trait]
    impl Plugin for EchoPlugin {
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
            self.started
                .store(true, std::sync::atomic::Ordering::Release);
            Ok(())
        }
        async fn stop(&self) -> ServiceResult<()> {
            self.started
                .store(false, std::sync::atomic::Ordering::Release);
            Ok(())
        }
        fn health_check(&self) -> bool {
            self.started.load(std::sync::atomic::Ordering::Acquire)
        }
        fn status(&self) -> PluginStatus {
            if self.health_check() {
                PluginStatus::Started
            } else {
                PluginStatus::Stopped
            }
        }
        async fn invoke(&self, method: &str, payload: &[u8]) -> ServiceResult<Vec<u8>> {
            if method == "echo" {
                Ok(payload.to_vec())
            } else {
                Err(format!("unknown method '{method}'").into())
            }
        }
    }

    async fn service_with_echo(started: bool) -> PluginService {
        let manager = Arc::new(PluginManager::new(PluginEngineConfig::default()));
        manager.register(Arc::new(EchoPlugin::new())).await.unwrap();
        if started {
            manager.start_all().await;
        }
        PluginService::new(manager)
    }

    #[tokio::test]
    async fn invoke_routes_to_plugin_and_echoes_payload() {
        let svc = service_with_echo(true).await;
        let resp = svc
            .invoke(Request::new(InvokeRequest {
                plugin_id: "echo".into(),
                method: "echo".into(),
                payload: b"hello".to_vec(),
            }))
            .await
            .unwrap();
        assert_eq!(resp.into_inner().payload, b"hello");
    }

    #[tokio::test]
    async fn invoke_unknown_plugin_is_not_found() {
        let svc = service_with_echo(true).await;
        let err = svc
            .invoke(Request::new(InvokeRequest {
                plugin_id: "missing".into(),
                method: "echo".into(),
                payload: vec![],
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn invoke_stopped_plugin_is_unavailable() {
        let svc = service_with_echo(false).await;
        let err = svc
            .invoke(Request::new(InvokeRequest {
                plugin_id: "echo".into(),
                method: "echo".into(),
                payload: vec![],
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn invoke_unknown_method_is_internal_error() {
        let svc = service_with_echo(true).await;
        let err = svc
            .invoke(Request::new(InvokeRequest {
                plugin_id: "echo".into(),
                method: "nope".into(),
                payload: vec![],
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
    }

    #[tokio::test]
    async fn list_reports_loaded_plugins() {
        let svc = service_with_echo(true).await;
        let resp = svc
            .list(Request::new(ListPluginsRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.plugins.len(), 1);
        assert_eq!(resp.plugins[0].name, "echo");
        assert_eq!(resp.plugins[0].runtime, "native");
        assert_eq!(resp.plugins[0].status, "started");
        assert!(
            resp.plugins[0].healthy,
            "started echo plugin must be healthy"
        );
        assert!(
            !resp.plugins[0].builtin,
            "a hand-registered stub is not a builtin service plugin"
        );
    }

    /// 统一健康面：原生服务（内建插件）与脚本插件出现在同一份清单里，
    /// 各自上报 `healthy` / `builtin`，且健康是**实时**读取的。
    #[tokio::test]
    async fn list_reports_builtin_native_services_with_live_health() {
        struct Flaky {
            healthy: std::sync::atomic::AtomicBool,
        }

        impl Flaky {
            fn set(&self, v: bool) {
                self.healthy.store(v, std::sync::atomic::Ordering::Release);
            }
        }

        #[async_trait]
        impl crate::service::BaseService for Flaky {
            fn name(&self) -> &'static str {
                "flaky"
            }
            async fn start(&self) -> ServiceResult<()> {
                Ok(())
            }
            async fn stop(&self) -> ServiceResult<()> {
                Ok(())
            }
            fn health_check(&self) -> bool {
                self.healthy.load(std::sync::atomic::Ordering::Acquire)
            }
        }

        let flaky = Arc::new(Flaky {
            healthy: std::sync::atomic::AtomicBool::new(false),
        });
        let manager = Arc::new(PluginManager::new(PluginEngineConfig::default()));
        manager
            .register_builtin(Arc::new(crate::plugin::NativePluginAdapter::new(
                flaky.clone(),
            )))
            .await
            .unwrap();
        // 启动成功（进程活着），但服务自报不健康 —— 历史上这种状态只进日志，
        // 现在直接体现在统一清单里。
        manager.start_all().await;

        let svc = PluginService::new(Arc::clone(&manager));
        let resp = svc
            .list(Request::new(ListPluginsRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.plugins.len(), 1);
        assert_eq!(resp.plugins[0].name, "flaky");
        assert!(resp.plugins[0].builtin, "native adapter must be builtin");
        assert_eq!(resp.plugins[0].status, "started");
        assert!(
            !resp.plugins[0].healthy,
            "unhealthy builtin service must be reported as unhealthy"
        );

        // 健康是每次查询实时读取的（不缓存启动期快照）
        flaky.set(true);
        let resp = svc
            .list(Request::new(ListPluginsRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.plugins[0].healthy, "health must be read live");
    }
}
