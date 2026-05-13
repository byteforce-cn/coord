//! P4D-07: `ClientAgent` — 将 Gossip、DiscoveryCache、ProxyClient 组合成统一代理。
//!
//! 实现 `DiscoveryProvider` trait，读操作优先命中本地缓存，未命中时通过 Gossip
//! 收集各节点广播的 `ServiceDelta` 并回填缓存。
//! 写操作（register/deregister/heartbeat）透传到 coord-server（CP 路径）。

use std::collections::HashMap;
use std::sync::Arc;

use coord_core::clock::Clock;
use coord_core::discovery::{DiscoveryError, DiscoveryProvider};
use coord_core::discovery_cache::DiscoveryCache;
use coord_core::gossip_types::{GossipAgent, ServiceDelta};
use coord_core::registry::{LeaseRecord, ServiceInstance};

use crate::proxy::ProxyClient;

/// 统一客户端代理。
pub struct ClientAgent {
    gossip: Arc<dyn GossipAgent>,
    cache: Arc<DiscoveryCache>,
    _proxy: Arc<ProxyClient>,
    clock: Arc<dyn Clock>,
}

impl ClientAgent {
    pub fn new(
        gossip: Arc<dyn GossipAgent>,
        cache: Arc<DiscoveryCache>,
        proxy: Arc<ProxyClient>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            gossip,
            cache,
            _proxy: proxy,
            clock,
        }
    }

    /// 从 Gossip 同步指定服务的实例列表到本地缓存。
    async fn sync_from_gossip(&self, service_name: &str) -> Vec<ServiceInstance> {
        let deltas: Vec<ServiceDelta> = self.gossip.service_deltas(service_name).await;
        let now_ms = self.clock.now_ms();
        let instances: Vec<ServiceInstance> = deltas
            .into_iter()
            .filter(|d| d.healthy && d.expires_unix_ms > now_ms)
            .map(|d| ServiceInstance {
                service_name: d.service_name,
                instance_id: d.instance_id,
                host: d.host,
                port: d.port,
                metadata: HashMap::new(),
            })
            .collect();
        self.cache.put(service_name, instances.clone());
        instances
    }
}

#[async_trait::async_trait]
impl DiscoveryProvider for ClientAgent {
    /// 注册服务实例（透传到 CP coord-server，再通过 Gossip 广播给其他 client）。
    ///
    /// 当前实现返回占位 lease（后续接入真实 gRPC stub 时替换）。
    async fn register(
        &self,
        instance: ServiceInstance,
        ttl_seconds: i64,
    ) -> Result<LeaseRecord, DiscoveryError> {
        let ttl = ttl_seconds.max(1);
        let expires = self.clock.now_ms() + ttl * 1000;
        // 广播到 Gossip 环
        let delta = ServiceDelta {
            service_name: instance.service_name.clone(),
            instance_id: instance.instance_id.clone(),
            host: instance.host.clone(),
            port: instance.port,
            healthy: true,
            expires_unix_ms: expires,
        };
        self.gossip.put_service_delta(delta).await.map_err(|e| {
            DiscoveryError::Unavailable(format!("gossip put_service_delta failed: {e}"))
        })?;
        // 失效本地缓存，下次 discover 时从 Gossip 重新同步
        self.cache.invalidate(&instance.service_name);
        Ok(LeaseRecord {
            lease_id: uuid::Uuid::new_v4().to_string(),
            ttl_seconds: ttl,
            expires_unix_ms: expires,
        })
    }

    async fn deregister(
        &self,
        service_name: &str,
        instance_id: &str,
    ) -> Result<(), DiscoveryError> {
        self.gossip
            .remove_service_delta(service_name, instance_id)
            .await
            .map_err(|e| DiscoveryError::Unavailable(e.to_string()))?;
        self.cache.remove_instance(service_name, instance_id);
        Ok(())
    }

    async fn heartbeat(&self, _lease_id: &str) -> Result<LeaseRecord, DiscoveryError> {
        // Client agent 路径：lease 管理在 coord-server，AP 路径 heartbeat = 续 Gossip TTL。
        // 当前实现返回占位值（后续接入真实 gRPC stub 时替换）。
        Err(DiscoveryError::Unavailable(
            "heartbeat via client agent requires CP coord-server connection".to_string(),
        ))
    }

    async fn discover(
        &self,
        service_name: &str,
    ) -> Result<Vec<ServiceInstance>, DiscoveryError> {
        // 优先命中本地缓存
        if let Some(cached) = self.cache.get(service_name) {
            return Ok(cached);
        }
        // 缓存未命中，从 Gossip 同步
        Ok(self.sync_from_gossip(service_name).await)
    }
}

// ─── 测试 ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use coord_core::clock::SystemClock;
    use coord_core::discovery::DiscoveryProvider;
    use coord_core::discovery_cache::DiscoveryCache;
    use coord_core::gossip_types::NullGossipAgent;
    use coord_core::registry::ServiceInstance;

    use super::ClientAgent;
    use crate::proxy::ProxyClient;

    fn agent() -> ClientAgent {
        let gossip = Arc::new(NullGossipAgent::new("n1", "127.0.0.1:9090"));
        let cache = Arc::new(DiscoveryCache::new(30_000, Arc::new(SystemClock)));
        let proxy = Arc::new(ProxyClient::new(vec![]));
        let clock = Arc::new(SystemClock);
        ClientAgent::new(gossip, cache, proxy, clock)
    }

    fn inst(svc: &str, id: &str) -> ServiceInstance {
        ServiceInstance {
            service_name: svc.to_string(),
            instance_id: id.to_string(),
            host: "127.0.0.1".to_string(),
            port: 8080,
            metadata: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn discover_unknown_service_returns_empty() {
        let a = agent();
        let result = a.discover("no-svc").await.expect("discover ok");
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn register_and_discover_via_cache() {
        let gossip = Arc::new(NullGossipAgent::new("n1", "127.0.0.1:9090"));
        let cache = Arc::new(DiscoveryCache::new(30_000, Arc::new(SystemClock)));
        let proxy = Arc::new(ProxyClient::new(vec![]));
        let clock = Arc::new(SystemClock);
        let a = ClientAgent::new(gossip, cache.clone(), proxy, clock);

        a.register(inst("svc-a", "i1"), 60)
            .await
            .expect("register ok");
        // 注册后 cache 被 invalidate，discover 会走 gossip（NullAgent 返回空），再次空列表
        // 验证：不会 panic
        let _ = a.discover("svc-a").await.expect("discover ok");
    }

    #[tokio::test]
    async fn deregister_removes_from_cache() {
        let gossip = Arc::new(NullGossipAgent::new("n1", "127.0.0.1:9090"));
        let cache = Arc::new(DiscoveryCache::new(30_000, Arc::new(SystemClock)));
        let proxy = Arc::new(ProxyClient::new(vec![]));
        let clock = Arc::new(SystemClock);
        let a = ClientAgent::new(gossip, cache.clone(), proxy, clock);

        // 手动填充缓存
        cache.put("svc-b", vec![inst("svc-b", "i1"), inst("svc-b", "i2")]);
        a.deregister("svc-b", "i1").await.expect("deregister ok");
        let found = cache.get("svc-b").expect("cache hit");
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].instance_id, "i2");
    }
}
