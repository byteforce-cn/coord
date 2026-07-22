// coord-agent: 服务注册与发现 (Registry Service)
//
// 实现 BaseService trait，提供微服务注册、发现、健康检查能力。
// 基于 Coord 核心原语（KV + Lease + Watch）构建。
//
// 架构（v3.0）:
// - 本地缓存全量注册表（延迟 <1ms），Watch Fan-out 维护更新
// - 与 Server 断连时保留最后已知实例快照（自我保护）
// - 通过 Lease 绑定实现实例自动过期
//
// 参见 docs/client-agent-architecture-v3.md §5.1。

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use lru::LruCache;
use parking_lot::RwLock as ParkingRwLock;
use tokio::sync::watch;

use crate::proxy::AgentInner;
use crate::service::{BaseService, ServiceError, ServiceResult};

// ──── 类型定义 ────

/// 服务实例元数据
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ServiceInstance {
    /// 服务名称（如 "order-service"）
    pub service_name: String,
    /// 实例唯一标识（如 "node1:8080"）
    pub instance_id: String,
    /// 实例地址（host:port）
    pub address: String,
    /// 实例元数据（JSON 格式）
    pub metadata: Vec<u8>,
    /// 绑定的 Lease ID（0 表示未绑定）
    pub lease_id: i64,
    /// 注册时间（Unix 时间戳，秒）
    pub registered_at: u64,
}

impl ServiceInstance {
    /// 创建服务实例
    pub fn new(
        service_name: impl Into<String>,
        instance_id: impl Into<String>,
        address: impl Into<String>,
        metadata: Vec<u8>,
        lease_id: i64,
    ) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Self {
            service_name: service_name.into(),
            instance_id: instance_id.into(),
            address: address.into(),
            metadata,
            lease_id,
            registered_at: now,
        }
    }
}

/// 服务发现查询结果
#[derive(Debug, Clone)]
pub struct DiscoveryResult {
    /// 服务名称
    pub service_name: String,
    /// 可用实例列表
    pub instances: Vec<ServiceInstance>,
}

// ──── RegistryCache（替代旧 cache.rs 中的 RegistryCache） ────

/// Registry 服务本地缓存
///
/// 全量缓存注册表，Watch 驱动增量更新。
/// 与旧 `cache::RegistryCache` 的区别：
/// - 存储类型化 `ServiceInstance` 而非原始 bytes
/// - 支持按服务名查询
/// - 内建自我保护模式（断连时保留快照）
pub struct RegistryCache {
    /// 实例缓存：key = "/_registry/services/{svc}/instances/{id}" → ServiceInstance
    instances: LruCache<String, ServiceInstance>,
    /// 自我保护模式：Server 断连时保留最后快照
    self_protection: bool,
    /// 最后成功同步时间
    last_sync: Instant,
}

impl RegistryCache {
    /// 创建 Registry 缓存
    pub fn new(max_entries: usize) -> Self {
        let cap = NonZeroUsize::new(max_entries.max(1)).unwrap();
        Self {
            instances: LruCache::new(cap),
            self_protection: false,
            last_sync: Instant::now(),
        }
    }

    /// 全量加载实例列表
    pub fn load_full(&mut self, instances: Vec<ServiceInstance>) {
        for inst in instances {
            let key = Self::make_key(&inst.service_name, &inst.instance_id);
            self.instances.put(key, inst);
        }
        self.last_sync = Instant::now();
        self.self_protection = false;
    }

    /// 应用 Watch 事件（Put: 新增/更新，Delete: 移除）
    pub fn apply_event(&mut self, key: &[u8], value: Option<&[u8]>) {
        let key_str = String::from_utf8_lossy(key).to_string();

        match value {
            Some(data) => {
                // 尝试从 value 反序列化 ServiceInstance（JSON）
                if let Ok(inst) = serde_json::from_slice::<ServiceInstance>(data) {
                    let cache_key = Self::make_key(&inst.service_name, &inst.instance_id);
                    self.instances.put(cache_key, inst);
                } else {
                    // 兼容旧格式：存储原始数据，保留 key 作为索引
                    tracing::debug!("RegistryCache: non-JSON value for key {key_str}, storing as raw");
                }
            }
            None => {
                // Delete 事件：按 key 移除
                // key 格式: /_registry/services/{svc}/instances/{id}
                self.instances.pop(&key_str);
            }
        }
        self.last_sync = Instant::now();
    }

    /// 查询指定服务的所有实例
    pub fn discover(&self, service_name: &str) -> Vec<ServiceInstance> {
        let prefix = format!("/_registry/services/{service_name}/instances/");
        self.instances
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(_, v)| v.clone())
            .collect()
    }

    /// 查询所有服务及其实例
    pub fn discover_all(&self) -> BTreeMap<String, Vec<ServiceInstance>> {
        let mut result: BTreeMap<String, Vec<ServiceInstance>> = BTreeMap::new();
        for (_, inst) in self.instances.iter() {
            result
                .entry(inst.service_name.clone())
                .or_default()
                .push(inst.clone());
        }
        result
    }

    /// 获取指定实例
    pub fn get(&self, service_name: &str, instance_id: &str) -> Option<ServiceInstance> {
        let key = Self::make_key(service_name, instance_id);
        self.instances.peek(&key).cloned()
    }

    /// 进入自我保护模式
    pub fn enter_self_protection(&mut self) {
        self.self_protection = true;
        tracing::warn!("RegistryCache: entering self-protection mode (server unreachable)");
    }

    /// 退出自我保护模式
    pub fn exit_self_protection(&mut self) {
        self.self_protection = false;
        tracing::info!("RegistryCache: exiting self-protection mode");
    }

    /// 是否处于自我保护模式
    pub fn is_self_protection(&self) -> bool {
        self.self_protection
    }

    /// 当前缓存条目数
    pub fn len(&self) -> usize {
        self.instances.len()
    }

    /// 缓存是否为空
    pub fn is_empty(&self) -> bool {
        self.instances.is_empty()
    }

    /// 距离上次成功同步的时间
    pub fn time_since_last_sync(&self) -> Duration {
        Instant::now().duration_since(self.last_sync)
    }

    // 构造缓存 key
    fn make_key(service_name: &str, instance_id: &str) -> String {
        format!("/_registry/services/{service_name}/instances/{instance_id}")
    }

    /// 构造 Server 存储 key（与缓存 key 相同）
    pub fn storage_key(service_name: &str, instance_id: &str) -> Vec<u8> {
        Self::make_key(service_name, instance_id).into_bytes()
    }
}

// ──── RegistryService ────

/// 服务注册与发现服务
///
/// 实现 `BaseService` trait，为 Java 应用提供服务注册、发现、心跳能力。
/// 内部使用共享的 AgentInner 连接 Server 集群。
pub struct RegistryService {
    /// 到 Server 集群的内部客户端（共享）
    inner: Arc<AgentInner>,
    /// 本地注册表缓存
    cache: Arc<ParkingRwLock<RegistryCache>>,
    /// 健康状态
    healthy: ParkingRwLock<bool>,
    /// 关闭信号发送端
    shutdown_tx: ParkingRwLock<Option<watch::Sender<()>>>,
    /// Watch 事件广播（用于 gRPC Watch 流）
    watch_tx: tokio::sync::broadcast::Sender<WatchEvent>,
}

impl RegistryService {
    /// 服务名称常量
    pub const NAME: &'static str = "registry";

    /// 创建 RegistryService
    pub fn new(inner: Arc<AgentInner>, cache_max_entries: usize) -> Self {
        let (watch_tx, _) = tokio::sync::broadcast::channel(256);
        Self {
            inner,
            cache: Arc::new(ParkingRwLock::new(RegistryCache::new(cache_max_entries))),
            healthy: ParkingRwLock::new(false),
            shutdown_tx: ParkingRwLock::new(None),
            watch_tx,
        }
    }

    /// 注册服务实例
    ///
    /// 在 Server 中写入服务实例数据并绑定 Lease。
    /// 同时更新本地缓存并广播 Watch 事件。
    pub async fn register(&self, instance: ServiceInstance) -> ServiceResult<()> {
        let key = RegistryCache::storage_key(&instance.service_name, &instance.instance_id);
        let value = serde_json::to_vec(&instance)
            .map_err(|e| format!("failed to serialize instance: {e}"))?;

        // 通过 KV 写入（若绑定 Lease，使用 Lease-bound Put）
        if instance.lease_id > 0 {
            self.inner
                .client
                .kv()
                .put_lease(&key, &value, instance.lease_id)
                .await
                .map_err(|e| format!("failed to register instance: {e}"))?;
        } else {
            self.inner
                .client
                .kv()
                .put(&key, &value)
                .await
                .map_err(|e| format!("failed to register instance: {e}"))?;
        }

        // 更新本地缓存
        self.cache.write().apply_event(&key, Some(&value));

        // 广播 Watch 事件（全量：该服务的全部实例）
        let all_instances = self.cache.read().discover(&instance.service_name);
        let proto_instances: Vec<coord_proto::agent::ServiceInstance> = all_instances
            .iter()
            .map(|inst| coord_proto::agent::ServiceInstance {
                instance_id: inst.instance_id.clone(),
                service_name: inst.service_name.clone(),
                metadata: String::from_utf8_lossy(&inst.metadata).to_string(),
            })
            .collect();
        let _ = self.watch_tx.send(WatchEvent {
            r#type: 1, // INSTANCES_ADDED
            instances: proto_instances,
            revision: 0,
        });

        tracing::info!(
            "RegistryService: registered {}/{} at {}",
            instance.service_name,
            instance.instance_id,
            instance.address
        );
        Ok(())
    }

    /// 注销服务实例
    ///
    /// 从 Server 中删除服务实例数据。
    /// 同时从本地缓存移除并广播 Watch 事件。
    pub async fn deregister(
        &self,
        service_name: &str,
        instance_id: &str,
    ) -> ServiceResult<()> {
        let key = RegistryCache::storage_key(service_name, instance_id);

        self.inner
            .client
            .kv()
            .delete(&key)
            .await
            .map_err(|e| format!("failed to deregister instance: {e}"))?;

        // 从本地缓存移除
        self.cache.write().apply_event(&key, None);

        // 广播 Watch 事件（全量：该服务的剩余实例）
        let remaining = self.cache.read().discover(service_name);
        let proto_instances: Vec<coord_proto::agent::ServiceInstance> = remaining
            .iter()
            .map(|inst| coord_proto::agent::ServiceInstance {
                instance_id: inst.instance_id.clone(),
                service_name: inst.service_name.clone(),
                metadata: String::from_utf8_lossy(&inst.metadata).to_string(),
            })
            .collect();
        let _ = self.watch_tx.send(WatchEvent {
            r#type: 2, // INSTANCES_REMOVED
            instances: proto_instances,
            revision: 0,
        });

        tracing::info!(
            "RegistryService: deregistered {}/{}",
            service_name,
            instance_id
        );
        Ok(())
    }

    /// 发现服务实例
    ///
    /// 从本地缓存读取（<1ms），不访问 Server。
    pub fn discover(&self, service_name: &str) -> DiscoveryResult {
        let instances = self.cache.read().discover(service_name);
        DiscoveryResult {
            service_name: service_name.to_string(),
            instances,
        }
    }

    /// 发现所有服务
    pub fn discover_all(&self) -> BTreeMap<String, Vec<ServiceInstance>> {
        self.cache.read().discover_all()
    }

    /// 获取指定实例
    pub fn get_instance(
        &self,
        service_name: &str,
        instance_id: &str,
    ) -> Option<ServiceInstance> {
        self.cache.read().get(service_name, instance_id)
    }

    /// 本地缓存条目数
    pub fn cache_len(&self) -> usize {
        self.cache.read().len()
    }

    /// 是否处于自我保护模式
    pub fn is_self_protection(&self) -> bool {
        self.cache.read().is_self_protection()
    }
}

#[async_trait]
impl BaseService for RegistryService {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    async fn start(&self) -> ServiceResult<()> {
        tracing::info!("RegistryService: starting");

        // 从 Server 全量拉取注册表
        match self.load_full_catalog().await {
            Ok(count) => {
                tracing::info!("RegistryService: loaded {count} instances from server");
                *self.healthy.write() = true;
            }
            Err(e) => {
                tracing::warn!("RegistryService: failed to load initial catalog: {e}; starting with empty cache");
                // 不阻塞启动：空缓存启动，Watch 会逐步填充
                *self.healthy.write() = true;
            }
        }

        // 启动 Watch 后台任务，维护缓存更新
        let (_tx, mut rx) = watch::channel::<()>(());
        *self.shutdown_tx.write() = Some(_tx);

        let inner = self.inner.clone();
        let cache = self.cache.clone();
        tokio::spawn(async move {
            tracing::info!("RegistryService: Watch background task started");
            let prefix = b"/_registry/services/";

            // 首次订阅 Watch
            let mut event_rx = match inner.client.watch().watch(prefix, 0).await {
                Ok(rx) => rx,
                Err(e) => {
                    tracing::warn!("RegistryService: failed to subscribe Watch: {e}; entering self-protection");
                    cache.write().enter_self_protection();
                    return;
                }
            };

            loop {
                tokio::select! {
                    _ = rx.changed() => {
                        tracing::info!("RegistryService: Watch background task shutting down");
                        break;
                    }
                    event = event_rx.recv() => {
                        match event {
                            Some(Ok(we)) => {
                                use coord_proto::watch::watch_event::EventType;
                                for kv in &we.kvs {
                                    let value = if we.r#type == EventType::Delete as i32 {
                                        None
                                    } else {
                                        Some(kv.value.as_slice())
                                    };
                                    cache.write().apply_event(&kv.key, value);
                                }
                                // 收到事件 = Server 可达，退出自我保护
                                if cache.read().is_self_protection() {
                                    cache.write().exit_self_protection();
                                }
                            }
                            Some(Err(e)) => {
                                tracing::warn!("RegistryService: Watch stream error: {e}; reconnecting...");
                                cache.write().enter_self_protection();
                                // 重连
                                match inner.client.watch().watch(prefix, 0).await {
                                    Ok(new_rx) => {
                                        event_rx = new_rx;
                                        tracing::info!("RegistryService: Watch reconnected");
                                    }
                                    Err(e2) => {
                                        tracing::error!("RegistryService: Watch reconnect failed: {e2}");
                                        break;
                                    }
                                }
                            }
                            None => {
                                tracing::warn!("RegistryService: Watch stream ended; reconnecting...");
                                cache.write().enter_self_protection();
                                match inner.client.watch().watch(prefix, 0).await {
                                    Ok(new_rx) => {
                                        event_rx = new_rx;
                                        tracing::info!("RegistryService: Watch reconnected");
                                    }
                                    Err(e) => {
                                        tracing::error!("RegistryService: Watch reconnect failed: {e}");
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }

    async fn stop(&self) -> ServiceResult<()> {
        tracing::info!("RegistryService: stopping");
        // 通知后台任务关闭
        if let Some(tx) = self.shutdown_tx.write().take() {
            let _ = tx.send(());
        }
        *self.healthy.write() = false;
        Ok(())
    }

    fn health_check(&self) -> bool {
        *self.healthy.read()
    }
}

impl RegistryService {
    /// 从 Server 全量加载注册表
    async fn load_full_catalog(&self) -> Result<usize, ServiceError> {
        let prefix = b"/_registry/services/";
        // 使用 Range 扫描全量注册表
        let end = prefix.to_vec();
        // range_end = prefix with last byte incremented for prefix scan
        let mut range_end = end.clone();
        if let Some(last) = range_end.last_mut() {
            *last = last.wrapping_add(1);
        }

        let pairs = self
            .inner
            .client
            .kv()
            .range(prefix, &range_end, 0, 0)
            .await
            .map_err(|e| format!("failed to load registry catalog: {e}"))?;

        let mut instances = Vec::new();
        for (key, value) in pairs {
            match serde_json::from_slice::<ServiceInstance>(&value) {
                Ok(inst) => instances.push(inst),
                Err(_) => {
                    // 旧格式：从 key 推断 service_name 和 instance_id
                    let key_str = String::from_utf8_lossy(&key);
                    if let Some((svc, id)) = parse_legacy_key(&key_str) {
                        instances.push(ServiceInstance {
                            service_name: svc,
                            instance_id: id,
                            address: String::new(),
                            metadata: value,
                            lease_id: 0,
                            registered_at: 0,
                        });
                    }
                }
            }
        }

        let count = instances.len();
        self.cache.write().load_full(instances);
        Ok(count)
    }
}

/// 从旧格式 key 解析 service_name 和 instance_id
///
/// key 格式: `/_registry/services/{svc}/instances/{id}`
fn parse_legacy_key(key: &str) -> Option<(String, String)> {
    let parts: Vec<&str> = key.split('/').collect();
    if parts.len() >= 5 && parts[1] == "_registry" && parts[2] == "services" {
        let service_name = parts[3].to_string();
        let instance_id = parts.get(5).map(|s| s.to_string()).unwrap_or_default();
        Some((service_name, instance_id))
    } else {
        None
    }
}

impl std::fmt::Debug for RegistryService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistryService")
            .field("cache_len", &self.cache_len())
            .field("healthy", &self.health_check())
            .field("self_protection", &self.is_self_protection())
            .finish()
    }
}

// ──── gRPC Registry trait 实现 ────

use coord_proto::agent::{
    RegisterRequest, RegisterResponse,
    DeregisterRequest, DeregisterResponse,
    HeartbeatRequest, HeartbeatResponse,
    DiscoverRequest, DiscoverResponse,
    WatchRequest, WatchEvent,
};
use coord_proto::agent::registry_server::Registry;

#[tonic::async_trait]
impl Registry for RegistryService {
    async fn register(
        &self,
        request: tonic::Request<RegisterRequest>,
    ) -> Result<tonic::Response<RegisterResponse>, tonic::Status> {
        let req = request.into_inner();
        // Create a lease for the TTL
        let lease_id = if req.ttl_seconds > 0 {
            self.inner
                .client
                .lease()
                .grant(req.ttl_seconds as i64)
                .await
                .map_err(|e| tonic::Status::internal(format!("lease grant failed: {e}")))?
        } else {
            0
        };

        let instance = ServiceInstance {
            service_name: req.service_name.clone(),
            instance_id: req.instance_id.clone(),
            address: req.metadata.clone(), // metadata field carries address info for agent_api
            metadata: req.metadata.into_bytes(),
            lease_id,
            registered_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };

        self.register(instance).await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        Ok(tonic::Response::new(RegisterResponse { lease_id }))
    }

    async fn deregister(
        &self,
        request: tonic::Request<DeregisterRequest>,
    ) -> Result<tonic::Response<DeregisterResponse>, tonic::Status> {
        let req = request.into_inner();
        self.deregister(&req.service_name, &req.instance_id).await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(tonic::Response::new(DeregisterResponse {}))
    }

    async fn heartbeat(
        &self,
        request: tonic::Request<HeartbeatRequest>,
    ) -> Result<tonic::Response<HeartbeatResponse>, tonic::Status> {
        let req = request.into_inner();
        let ttl = self.inner
            .client
            .lease()
            .keep_alive(req.lease_id)
            .await
            .map_err(|e| tonic::Status::internal(format!("lease keep-alive failed: {e}")))?;
        Ok(tonic::Response::new(HeartbeatResponse { ttl }))
    }

    async fn discover(
        &self,
        request: tonic::Request<DiscoverRequest>,
    ) -> Result<tonic::Response<DiscoverResponse>, tonic::Status> {
        let req = request.into_inner();
        let filter_mode = req.filter_mode();

        let instances: Vec<coord_proto::agent::ServiceInstance> = match filter_mode {
            // ALL: return every registered instance across all services
            coord_proto::agent::FilterMode::All => {
                let all = self.discover_all();
                all.values()
                    .flatten()
                    .map(|inst| coord_proto::agent::ServiceInstance {
                        instance_id: inst.instance_id.clone(),
                        service_name: inst.service_name.clone(),
                        metadata: String::from_utf8_lossy(&inst.metadata).to_string(),
                    })
                    .collect()
            }
            // PREFIX: match services whose name starts with the given prefix
            coord_proto::agent::FilterMode::Prefix => {
                let all = self.discover_all();
                let prefix = &req.service_name;
                all.iter()
                    .filter(|(svc, _)| svc.starts_with(prefix.as_str()))
                    .flat_map(|(_, instances)| instances)
                    .map(|inst| coord_proto::agent::ServiceInstance {
                        instance_id: inst.instance_id.clone(),
                        service_name: inst.service_name.clone(),
                        metadata: String::from_utf8_lossy(&inst.metadata).to_string(),
                    })
                    .collect()
            }
            // EXACT / UNSPECIFIED: exact service name match (backward compatible)
            _ => {
                let result = self.discover(&req.service_name);
                result
                    .instances
                    .iter()
                    .map(|inst| coord_proto::agent::ServiceInstance {
                        instance_id: inst.instance_id.clone(),
                        service_name: inst.service_name.clone(),
                        metadata: String::from_utf8_lossy(&inst.metadata).to_string(),
                    })
                    .collect()
            }
        };

        Ok(tonic::Response::new(DiscoverResponse {
            instances,
            revision: 0,
        }))
    }

    /// Server streaming response type for the Watch method.
    type WatchStream = tokio_stream::wrappers::ReceiverStream<
        Result<WatchEvent, tonic::Status>,
    >;

    async fn watch(
        &self,
        request: tonic::Request<WatchRequest>,
    ) -> Result<tonic::Response<Self::WatchStream>, tonic::Status> {
        let req = request.into_inner();
        let service_name = req.service_name.clone();
        let mut rx = self.watch_tx.subscribe();
        let (tx, out_rx) = tokio::sync::mpsc::channel(32);

        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        // 过滤：只推送匹配 service_name 的事件
                        let has_match = event.instances.iter().any(|inst| inst.service_name == service_name);
                        if has_match {
                            if tx.send(Ok(event)).await.is_err() {
                                break; // client disconnected
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("RegistryService watch lagged by {n} events");
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        });

        Ok(tonic::Response::new(
            tokio_stream::wrappers::ReceiverStream::new(out_rx),
        ))
    }
}

// ──── tests ────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_cache_basic() {
        let mut cache = RegistryCache::new(500);

        let inst = ServiceInstance::new(
            "order-service",
            "node1:8080",
            "10.0.0.1:8080",
            br#"{"zone":"us-east-1"}"#.to_vec(),
            1001,
        );
        let key = RegistryCache::storage_key("order-service", "node1:8080");
        let value = serde_json::to_vec(&inst).unwrap();

        cache.apply_event(&key, Some(&value));
        assert_eq!(cache.len(), 1);
        assert!(!cache.is_empty());

        let found = cache.discover("order-service");
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].instance_id, "node1:8080");

        // 删除
        cache.apply_event(&key, None);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_registry_cache_discover_all() {
        let mut cache = RegistryCache::new(500);

        let inst1 = ServiceInstance::new("svc-a", "i1", "addr1", vec![], 0);
        let inst2 = ServiceInstance::new("svc-a", "i2", "addr2", vec![], 0);
        let inst3 = ServiceInstance::new("svc-b", "i1", "addr3", vec![], 0);

        for inst in [&inst1, &inst2, &inst3] {
            let key = RegistryCache::storage_key(&inst.service_name, &inst.instance_id);
            let value = serde_json::to_vec(inst).unwrap();
            cache.apply_event(&key, Some(&value));
        }

        let all = cache.discover_all();
        assert_eq!(all.len(), 2); // svc-a, svc-b
        assert_eq!(all.get("svc-a").unwrap().len(), 2);
        assert_eq!(all.get("svc-b").unwrap().len(), 1);
    }

    #[test]
    fn test_registry_cache_self_protection() {
        let mut cache = RegistryCache::new(10);
        assert!(!cache.is_self_protection());

        cache.enter_self_protection();
        assert!(cache.is_self_protection());

        cache.exit_self_protection();
        assert!(!cache.is_self_protection());
    }

    #[test]
    fn test_registry_cache_load_full_clears_protection() {
        let mut cache = RegistryCache::new(10);
        cache.enter_self_protection();

        let inst = ServiceInstance::new("test", "i1", "addr", vec![], 0);
        cache.load_full(vec![inst]);
        assert!(!cache.is_self_protection());
    }

    #[test]
    fn test_parse_legacy_key() {
        let result = parse_legacy_key("/_registry/services/order-service/instances/node1");
        assert_eq!(
            result,
            Some(("order-service".to_string(), "node1".to_string()))
        );

        let result = parse_legacy_key("/other/prefix");
        assert_eq!(result, None);
    }
}
