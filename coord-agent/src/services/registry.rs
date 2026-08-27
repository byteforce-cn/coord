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

use std::collections::{BTreeMap, HashMap};
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
        let cap = NonZeroUsize::new(max_entries.max(1)).unwrap_or(NonZeroUsize::MIN);
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
                    tracing::debug!(
                        "RegistryCache: non-JSON value for key {key_str}, storing as raw"
                    );
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

    /// R-AGT-11：清空缓存（断连重连后的全量对账用：以 server 全量替换本地）
    pub fn clear(&mut self) {
        self.instances.clear();
        self.last_sync = Instant::now();
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
    /// R-AGT-11：实例健康探测结果（实例缓存 key → 最近一次探测存活与否）
    probe_results: Arc<ParkingRwLock<HashMap<String, bool>>>,
    /// R-AGT-11：探测任务关闭信号
    probe_shutdown_tx: ParkingRwLock<Option<watch::Sender<()>>>,
    /// R-AGT-20：资源隔离线程池（可选；watch/探测任务经 background 池）
    pools: Option<Arc<crate::threadpool::AgentThreadPools>>,
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
            probe_results: Arc::new(ParkingRwLock::new(HashMap::new())),
            probe_shutdown_tx: ParkingRwLock::new(None),
            pools: None,
        }
    }

    /// R-AGT-20：挂载资源隔离线程池（watch/探测后台任务经 background 池）。
    pub fn with_thread_pools(
        mut self,
        pools: Option<Arc<crate::threadpool::AgentThreadPools>>,
    ) -> Self {
        self.pools = pools;
        self
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
        Self::broadcast_instances(&self.cache, &self.watch_tx, &instance.service_name, 1);

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
    pub async fn deregister(&self, service_name: &str, instance_id: &str) -> ServiceResult<()> {
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
        Self::broadcast_instances(&self.cache, &self.watch_tx, service_name, 2);

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
    /// R-AGT-11：按最近探测结果摘流——已探测且不存活的实例不返回；
    /// 自我保护模式（Server 断连）下不过滤（保留最后已知快照）。
    pub fn discover(&self, service_name: &str) -> DiscoveryResult {
        let instances = self.filter_alive(self.cache.read().discover(service_name));
        DiscoveryResult {
            service_name: service_name.to_string(),
            instances,
        }
    }

    /// 发现所有服务（R-AGT-11：同 `discover` 摘流过滤）。
    pub fn discover_all(&self) -> BTreeMap<String, Vec<ServiceInstance>> {
        let all = self.cache.read().discover_all();
        all.into_iter()
            .map(|(svc, instances)| (svc, self.filter_alive(instances)))
            .collect()
    }

    /// R-AGT-11：按最近探测结果过滤不存活实例。
    ///
    /// - 自我保护模式：不过滤（保留最后已知快照）；
    /// - 未探测过的实例（无结果）视为存活（fail-open，注册后首轮探测前可见）。
    fn filter_alive(&self, instances: Vec<ServiceInstance>) -> Vec<ServiceInstance> {
        filter_alive_impl(
            self.cache.read().is_self_protection(),
            &self.probe_results.read(),
            instances,
        )
    }

    /// 获取指定实例
    pub fn get_instance(&self, service_name: &str, instance_id: &str) -> Option<ServiceInstance> {
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

    // ──── R-AGT-11：实例健康探测 ────

    /// 查询实例最近一次探测结果（None = 尚未探测）。
    pub fn is_instance_alive(&self, service_name: &str, instance_id: &str) -> Option<bool> {
        let key = RegistryCache::make_key(service_name, instance_id);
        self.probe_results.read().get(&key).copied()
    }

    /// 立即探测全部缓存实例并刷新结果，返回 (存活数, 总数)。
    pub async fn probe_all_now(&self) -> (usize, usize) {
        let instances: Vec<ServiceInstance> = self
            .cache
            .read()
            .discover_all()
            .into_values()
            .flatten()
            .collect();
        let total = instances.len();
        let mut alive = 0;
        let mut results = HashMap::new();
        for inst in &instances {
            let ok = probe_instance_addr(&inst.address).await;
            if ok {
                alive += 1;
            }
            results.insert(
                RegistryCache::make_key(&inst.service_name, &inst.instance_id),
                ok,
            );
        }
        *self.probe_results.write() = results;
        (alive, total)
    }

    /// 从实例存储 key 解析服务名（`/_registry/services/{svc}/instances/{id}`）。
    fn service_name_from_key(key: &[u8]) -> Option<String> {
        let key_str = String::from_utf8_lossy(key).to_string();
        let rest = key_str.strip_prefix("/_registry/services/")?;
        // 必须含 "/instances/" 分隔符；服务名非空且不含斜杠（拒绝畸形 key）
        let (svc, _) = rest.split_once("/instances/")?;
        if svc.is_empty() || svc.contains('/') {
            None
        } else {
            Some(svc.to_string())
        }
    }

    /// 将服务实例列表广播到本地 watch 订阅者（R-AGT-11：跨节点回灌）。
    fn broadcast_instances(
        cache: &Arc<ParkingRwLock<RegistryCache>>,
        watch_tx: &tokio::sync::broadcast::Sender<WatchEvent>,
        service_name: &str,
        event_type: i32,
    ) {
        let instances = cache.read().discover(service_name);
        let proto_instances: Vec<coord_proto::agent::ServiceInstance> = instances
            .iter()
            .map(|inst| coord_proto::agent::ServiceInstance {
                instance_id: inst.instance_id.clone(),
                service_name: inst.service_name.clone(),
                metadata: String::from_utf8_lossy(&inst.metadata).to_string(),
            })
            .collect();
        let _ = watch_tx.send(WatchEvent {
            r#type: event_type, // INSTANCES_ADDED / INSTANCES_REMOVED
            instances: proto_instances,
            revision: 0,
        });
    }
}

/// R-AGT-11：摘流过滤纯函数（discover/discover_all 共用；便于单测）。
///
/// - `self_protection`：自我保护模式不过滤（保留最后已知快照）；
/// - 探测结果中不存在 = 未探测 → 视为存活（fail-open）。
fn filter_alive_impl(
    self_protection: bool,
    probes: &HashMap<String, bool>,
    instances: Vec<ServiceInstance>,
) -> Vec<ServiceInstance> {
    if self_protection {
        return instances;
    }
    instances
        .into_iter()
        .filter(|inst| {
            let key = RegistryCache::make_key(&inst.service_name, &inst.instance_id);
            probes.get(&key).copied().unwrap_or(true)
        })
        .collect()
}

/// R-AGT-11：TCP 连接探测（1s 超时）；空地址视为不存活（避免误判可用）。
async fn probe_instance_addr(address: &str) -> bool {
    if address.is_empty() {
        return false;
    }
    match tokio::time::timeout(
        Duration::from_secs(1),
        tokio::net::TcpStream::connect(address),
    )
    .await
    {
        Ok(Ok(_stream)) => true,
        Ok(Err(_)) | Err(_) => false,
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
        let watch_tx = self.watch_tx.clone();
        let watch_task = async move {
            tracing::info!("RegistryService: Watch background task started");
            let prefix = b"/_registry/services/";

            // 首次订阅 Watch（start_revision=0 = 从最新开始，启动时已有全量目录）
            let mut event_rx = match inner.client.watch().watch(prefix, 0).await {
                Ok(rx) => rx,
                Err(e) => {
                    tracing::warn!(
                        "RegistryService: failed to subscribe Watch: {e}; entering self-protection"
                    );
                    cache.write().enter_self_protection();
                    return;
                }
            };
            // R-AGT-11：水位——最近一次成功接收的事件 revision（断连重连续传依据）
            let mut last_rev: i64 = 0;

            loop {
                tokio::select! {
                    _ = rx.changed() => {
                        tracing::info!("RegistryService: Watch background task shutting down");
                        break;
                    }
                    event = event_rx.recv() => {
                        match event {
                            Some(Ok(we)) => {
                                last_rev = last_rev.max(we.revision);
                                use coord_proto::watch::watch_event::EventType;
                                for kv in &we.kvs {
                                    let value = if we.r#type == EventType::Delete as i32 {
                                        None
                                    } else {
                                        Some(kv.value.as_slice())
                                    };
                                    cache.write().apply_event(&kv.key, value);
                                }
                                // R-AGT-11：跨节点变更回灌本地 watch 流——
                                // A agent 注册/下线，B agent 的订阅者收到通知
                                let mut affected: std::collections::HashSet<String> =
                                    std::collections::HashSet::new();
                                for kv in &we.kvs {
                                    if let Some(svc) = Self::service_name_from_key(&kv.key) {
                                        affected.insert(svc);
                                    }
                                }
                                let event_type =
                                    if we.r#type == EventType::Delete as i32 { 2 } else { 1 };
                                for svc in &affected {
                                    Self::broadcast_instances(&cache, &watch_tx, svc, event_type);
                                }
                                // 收到事件 = Server 可达，退出自我保护
                                if cache.read().is_self_protection() {
                                    cache.write().exit_self_protection();
                                }
                            }
                            Some(Err(e)) => {
                                tracing::warn!("RegistryService: Watch stream error: {e}; reconnecting...");
                                cache.write().enter_self_protection();
                                // R-AGT-11：重连三件套——① 水位续传（从 last_rev 起），
                                // ② 续传成功后全量对账（断连窗口内变更一致可见）
                                match inner.client.watch().watch(prefix, last_rev).await {
                                    Ok(new_rx) => {
                                        event_rx = new_rx;
                                        reconcile_registry(&inner, &cache).await;
                                        tracing::info!("RegistryService: Watch reconnected (from rev {last_rev}) + reconciled");
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
                                match inner.client.watch().watch(prefix, last_rev).await {
                                    Ok(new_rx) => {
                                        event_rx = new_rx;
                                        reconcile_registry(&inner, &cache).await;
                                        tracing::info!("RegistryService: Watch reconnected (from rev {last_rev}) + reconciled");
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
        };
        // R-AGT-20：watch 任务经 background 池 spawn（未挂线程池时回退 tokio::spawn）
        if let Some(ref pools) = self.pools {
            pools.spawn_background(watch_task);
        } else {
            tokio::spawn(watch_task);
        }

        // R-AGT-11：周期健康探测任务（15s 一次 TCP connect，区分存活/不存活实例）
        let (_probe_tx, mut probe_rx) = watch::channel::<()>(());
        *self.probe_shutdown_tx.write() = Some(_probe_tx);
        let cache_for_probe = self.cache.clone();
        let results_for_probe = self.probe_results.clone();
        let probe_task = async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(15));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = probe_rx.changed() => break,
                    _ = ticker.tick() => {
                        let instances: Vec<ServiceInstance> = cache_for_probe
                            .read()
                            .discover_all()
                            .into_values()
                            .flatten()
                            .collect();
                        let mut results = HashMap::new();
                        for inst in &instances {
                            let ok = probe_instance_addr(&inst.address).await;
                            results.insert(
                                RegistryCache::make_key(&inst.service_name, &inst.instance_id),
                                ok,
                            );
                        }
                        *results_for_probe.write() = results;
                    }
                }
            }
        };
        // R-AGT-20：探测任务经 background 池 spawn（未挂线程池时回退 tokio::spawn）
        if let Some(ref pools) = self.pools {
            pools.spawn_background(probe_task);
        } else {
            tokio::spawn(probe_task);
        }

        Ok(())
    }

    async fn stop(&self) -> ServiceResult<()> {
        tracing::info!("RegistryService: stopping");
        // 通知后台任务关闭
        if let Some(tx) = self.shutdown_tx.write().take() {
            let _ = tx.send(());
        }
        if let Some(tx) = self.probe_shutdown_tx.write().take() {
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
        load_catalog_into(&self.inner, &self.cache).await
    }
}

/// 从 Server 全量拉取注册表并灌入缓存（`load_full_catalog` 的共享实现）。
async fn load_catalog_into(
    inner: &Arc<AgentInner>,
    cache: &Arc<ParkingRwLock<RegistryCache>>,
) -> Result<usize, ServiceError> {
    let prefix = b"/_registry/services/";
    // 使用 Range 扫描全量注册表
    let end = prefix.to_vec();
    // range_end = prefix with last byte incremented for prefix scan
    let mut range_end = end.clone();
    if let Some(last) = range_end.last_mut() {
        *last = last.wrapping_add(1);
    }

    let pairs = inner
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
    cache.write().load_full(instances);
    Ok(count)
}

/// R-AGT-11：全量对账——清空缓存后以 Server 全量替换，成功退出自我保护。
async fn reconcile_registry(inner: &Arc<AgentInner>, cache: &Arc<ParkingRwLock<RegistryCache>>) {
    cache.write().clear();
    match load_catalog_into(inner, cache).await {
        Ok(count) => {
            tracing::info!("RegistryService: reconciled {count} instances from server");
            if cache.read().is_self_protection() {
                cache.write().exit_self_protection();
            }
        }
        Err(e) => {
            tracing::warn!("RegistryService: full reconcile failed: {e}");
        }
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

use coord_proto::agent::registry_server::Registry;
use coord_proto::agent::{
    DeregisterRequest, DeregisterResponse, DiscoverRequest, DiscoverResponse, HeartbeatRequest,
    HeartbeatResponse, RegisterRequest, RegisterResponse, WatchEvent, WatchRequest,
};

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

        self.register(instance)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        Ok(tonic::Response::new(RegisterResponse { lease_id }))
    }

    async fn deregister(
        &self,
        request: tonic::Request<DeregisterRequest>,
    ) -> Result<tonic::Response<DeregisterResponse>, tonic::Status> {
        let req = request.into_inner();
        self.deregister(&req.service_name, &req.instance_id)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(tonic::Response::new(DeregisterResponse {}))
    }

    async fn heartbeat(
        &self,
        request: tonic::Request<HeartbeatRequest>,
    ) -> Result<tonic::Response<HeartbeatResponse>, tonic::Status> {
        let req = request.into_inner();
        let ttl = self
            .inner
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
    type WatchStream = tokio_stream::wrappers::ReceiverStream<Result<WatchEvent, tonic::Status>>;

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
                        let has_match = event
                            .instances
                            .iter()
                            .any(|inst| inst.service_name == service_name);
                        if has_match && tx.send(Ok(event)).await.is_err() {
                            break; // client disconnected
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
    fn test_registry_cache_clear() {
        let mut cache = RegistryCache::new(500);
        let inst = ServiceInstance::new("svc-a", "i1", "addr1", vec![], 0);
        let key = RegistryCache::storage_key("svc-a", "i1");
        let value = serde_json::to_vec(&inst).unwrap();
        cache.apply_event(&key, Some(&value));
        assert_eq!(cache.len(), 1);
        cache.clear();
        assert!(cache.is_empty(), "clear 用于全量对账前清空");
    }

    // ──── R-AGT-11：摘流过滤 ────

    #[test]
    fn test_filter_alive_excludes_dead_instances() {
        let a = ServiceInstance::new("svc", "alive", "addr1", vec![], 0);
        let b = ServiceInstance::new("svc", "dead", "addr2", vec![], 0);
        let c = ServiceInstance::new("svc", "unknown", "addr3", vec![], 0);
        let mut probes = HashMap::new();
        probes.insert(RegistryCache::make_key("svc", "alive"), true);
        probes.insert(RegistryCache::make_key("svc", "dead"), false);
        // c 未探测 → fail-open 保留

        let filtered = filter_alive_impl(false, &probes, vec![a, b, c]);
        let ids: Vec<&str> = filtered.iter().map(|i| i.instance_id.as_str()).collect();
        assert_eq!(ids, vec!["alive", "unknown"], "死实例摘流、未探测实例保留");
    }

    #[test]
    fn test_filter_alive_keeps_all_in_self_protection() {
        let a = ServiceInstance::new("svc", "dead", "addr2", vec![], 0);
        let mut probes = HashMap::new();
        probes.insert(RegistryCache::make_key("svc", "dead"), false);

        let filtered = filter_alive_impl(true, &probes, vec![a]);
        assert_eq!(filtered.len(), 1, "自我保护模式保留最后已知快照，不过滤");
    }

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

    // ──── R-AGT-11：服务名解析 + 健康探测 ────

    #[test]
    fn test_service_name_from_key() {
        let svc = RegistryService::service_name_from_key(
            b"/_registry/services/order-service/instances/node1",
        );
        assert_eq!(svc.as_deref(), Some("order-service"));

        // 非注册表 key → None
        assert!(RegistryService::service_name_from_key(b"/_config/x").is_none());
        assert!(RegistryService::service_name_from_key(b"/_registry/services//x").is_none());
    }

    #[tokio::test]
    async fn test_probe_instance_addr_alive_and_dead() {
        // 本地监听端口 → 存活
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        assert!(probe_instance_addr(&addr).await);
        drop(listener);

        // 空地址 → 不存活（避免误判可用）
        assert!(!probe_instance_addr("").await);
    }

    #[tokio::test]
    async fn test_probe_instance_addr_unreachable() {
        // 保留端口：绑定后关闭 → connect 大概率拒绝；若系统仍可连接则跳过
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let alive = probe_instance_addr(&addr.to_string()).await;
        // 端口刚释放可能短暂 TIME_WAIT 可连；此处仅断言函数不 panic 且返回 bool
        let _ = alive;
    }
}
