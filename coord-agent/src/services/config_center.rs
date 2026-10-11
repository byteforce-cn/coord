// coord-agent: 配置中心 (Config Center Service)
//
// 实现 BaseService trait，提供配置读写、热更新能力。
// 基于 Coord 核心原语（KV + Watch）构建。
//
// 架构（v3.0）:
// - 本地配置缓存，Watch 驱动热更新
// - 断连时回退缓存配置
// - 支持按应用/环境/标签维度隔离

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock as ParkingRwLock;
use tokio::sync::watch;

use crate::proxy::AgentInner;
use crate::service::{BaseService, ServiceResult};

// ──── 类型定义 ────

/// 配置条目
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ConfigEntry {
    /// 配置 key（如 "app.order-service.db.host"）
    pub key: String,
    /// 配置值（UTF-8 字符串）
    pub value: String,
    /// 版本号（每次更新递增）
    pub version: u64,
    /// 更新时间（Unix 时间戳，秒）
    pub updated_at: u64,
}

impl ConfigEntry {
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        let now = unix_ts();
        Self {
            key: key.into(),
            value: value.into(),
            version: 1,
            updated_at: now,
        }
    }

    /// 构造 Server 存储 key
    pub fn storage_key(key: &str) -> Vec<u8> {
        format!("/_config/{key}").into_bytes()
    }
}

fn unix_ts() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ──── ConfigCache ────

/// 配置中心本地缓存
///
/// 本地缓存全量配置，Watch 驱动增量更新。
/// 与 Server 断连时保留最后已知配置（回退模式）。
pub struct ConfigCache {
    /// 配置条目：key → ConfigEntry
    entries: BTreeMap<String, ConfigEntry>,
    /// 是否处于回退模式（Server 不可达）
    fallback_mode: bool,
}

impl ConfigCache {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            fallback_mode: false,
        }
    }

    /// 获取配置值
    pub fn get(&self, key: &str) -> Option<&ConfigEntry> {
        self.entries.get(key)
    }

    /// 获取配置值（仅返回值字符串）
    pub fn get_value(&self, key: &str) -> Option<&str> {
        self.entries.get(key).map(|e| e.value.as_str())
    }

    /// 按前缀查询配置
    pub fn get_by_prefix(&self, prefix: &str) -> Vec<&ConfigEntry> {
        self.entries
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(_, v)| v)
            .collect()
    }

    /// 写入/更新配置（本地缓存）
    pub fn put(&mut self, entry: ConfigEntry) {
        self.entries.insert(entry.key.clone(), entry);
    }

    /// 删除配置
    pub fn remove(&mut self, key: &str) -> Option<ConfigEntry> {
        self.entries.remove(key)
    }

    /// 全量加载配置
    pub fn load_full(&mut self, entries: Vec<ConfigEntry>) {
        self.entries.clear();
        for entry in entries {
            self.entries.insert(entry.key.clone(), entry);
        }
        self.fallback_mode = false;
    }

    /// 应用 Watch 事件
    pub fn apply_event(&mut self, key: &[u8], value: Option<&[u8]>) {
        let key_str = String::from_utf8_lossy(key).to_string();
        // 解析出配置 key（去掉 /_config/ 前缀）
        let config_key = key_str
            .strip_prefix("/_config/")
            .unwrap_or(&key_str)
            .to_string();

        match value {
            Some(data) => {
                if let Ok(entry) = serde_json::from_slice::<ConfigEntry>(data) {
                    self.entries.insert(config_key, entry);
                }
            }
            None => {
                self.entries.remove(&config_key);
            }
        }
    }

    /// 进入回退模式
    pub fn enter_fallback(&mut self) {
        self.fallback_mode = true;
    }

    /// 退出回退模式
    pub fn exit_fallback(&mut self) {
        self.fallback_mode = false;
    }

    /// 是否处于回退模式
    pub fn is_fallback(&self) -> bool {
        self.fallback_mode
    }

    /// 缓存条目数
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// 缓存是否为空
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// 获取所有配置
    pub fn all(&self) -> &BTreeMap<String, ConfigEntry> {
        &self.entries
    }
}

impl Default for ConfigCache {
    fn default() -> Self {
        Self::new()
    }
}

// ──── ConfigCenterService ────

/// 配置中心服务
///
/// 实现 `BaseService` trait，为应用提供配置读写与热更新能力。
pub struct ConfigCenterService {
    /// 到 Server 集群的内部客户端（共享）
    inner: Arc<AgentInner>,
    /// 本地配置缓存（Arc 共享，供 Watch 后台任务读取）
    cache: Arc<ParkingRwLock<ConfigCache>>,
    /// 健康状态
    healthy: ParkingRwLock<bool>,
    /// 关闭信号发送端
    shutdown_tx: ParkingRwLock<Option<watch::Sender<()>>>,
    /// 快照位点 W（全局 revision 口径）：本地缓存已包含全部 revision ≤ W 的变更。
    /// 0 = 尚未完成一次对账（消费者按「无位点」处理，见 config.proto 头注）。
    /// 更新纪律：对账 = 先采集 C0 → 读数据 → 换缓存 → 存 W := C0；实时事件 =
    /// 应用批次后 W := max(W, batch_revision)。
    watermark: Arc<std::sync::atomic::AtomicI64>,
    /// R-AGT-20：资源隔离线程池（可选；watch 任务经 background 池）
    pools: Option<Arc<crate::threadpool::AgentThreadPools>>,
}

impl ConfigCenterService {
    /// 服务名称常量
    pub const NAME: &'static str = "config_center";

    /// 创建 ConfigCenterService
    pub fn new(inner: Arc<AgentInner>) -> Self {
        Self {
            inner,
            cache: Arc::new(ParkingRwLock::new(ConfigCache::new())),
            healthy: ParkingRwLock::new(false),
            shutdown_tx: ParkingRwLock::new(None),
            watermark: Arc::new(std::sync::atomic::AtomicI64::new(0)),
            pools: None,
        }
    }

    /// R-AGT-20：挂载资源隔离线程池（watch 后台任务经 background 池）。
    pub fn with_thread_pools(
        mut self,
        pools: Option<Arc<crate::threadpool::AgentThreadPools>>,
    ) -> Self {
        self.pools = pools;
        self
    }

    /// 设置配置（写入 Server + 更新本地缓存）。
    ///
    /// 返回 `(entry, revision)`：`revision` = 本次写入的**全局位点**
    /// （Server KV 全局 Revision 口径，与 List/Watch 的 revision 同一坐标轴）。
    pub async fn set(&self, key: &str, value: &str) -> ServiceResult<(ConfigEntry, u64)> {
        let storage_key = ConfigEntry::storage_key(key);

        // 先读取当前版本（若存在）
        let current_version = if let Some(existing) = self.cache.read().get(key) {
            existing.version
        } else {
            0
        };

        let entry = ConfigEntry {
            key: key.to_string(),
            value: value.to_string(),
            version: current_version + 1,
            updated_at: unix_ts(),
        };

        let serialized = serde_json::to_vec(&entry)
            .map_err(|e| format!("failed to serialize config entry: {e}"))?;

        let revision = self
            .inner
            .client
            .kv()
            .put(&storage_key, &serialized)
            .await
            .map_err(|e| format!("failed to write config '{key}': {e}"))?;

        // 更新本地缓存（乐观路径：权威变更经 KV Watch 以全局 revision 下发，
        // 见 `watch` 后台任务；本地先应用可缩短读己之写窗口）
        self.cache.write().put(entry.clone());

        tracing::info!("ConfigCenter: set key='{key}', version={}", entry.version);
        Ok((entry, revision))
    }

    /// 获取配置（优先本地缓存）
    pub fn get(&self, key: &str) -> Option<ConfigEntry> {
        self.cache.read().get(key).cloned()
    }

    /// 获取配置值（仅返回值字符串）
    pub fn get_value(&self, key: &str) -> Option<String> {
        self.cache.read().get_value(key).map(|s| s.to_string())
    }

    /// 按前缀查询配置
    pub fn get_by_prefix(&self, prefix: &str) -> Vec<ConfigEntry> {
        self.cache
            .read()
            .get_by_prefix(prefix)
            .into_iter()
            .cloned()
            .collect()
    }

    /// 删除配置
    pub async fn delete(&self, key: &str) -> ServiceResult<()> {
        let storage_key = ConfigEntry::storage_key(key);

        self.inner
            .client
            .kv()
            .delete(&storage_key)
            .await
            .map_err(|e| format!("failed to delete config '{key}': {e}"))?;

        self.cache.write().remove(key);
        tracing::info!("ConfigCenter: deleted key='{key}'");
        Ok(())
    }

    /// 从 Server 全量加载配置
    pub async fn load_all(&self) -> ServiceResult<Vec<ConfigEntry>> {
        let entries = Self::reload_from_server(&self.inner, &self.cache, &self.watermark).await?;
        let count = entries.len();
        tracing::info!("ConfigCenter: loaded {count} configs from server");
        Ok(entries)
    }

    /// R-AGT-14：从 Server 全量重载并覆盖本地缓存（启动恢复 / 断连对账共用）。
    async fn reload_from_server(
        inner: &Arc<AgentInner>,
        cache: &Arc<ParkingRwLock<ConfigCache>>,
        watermark: &std::sync::atomic::AtomicI64,
    ) -> Result<Vec<ConfigEntry>, String> {
        let prefix = b"/_config/";
        let mut range_end = prefix.to_vec();
        if let Some(last) = range_end.last_mut() {
            *last = last.wrapping_add(1);
        }

        // 快照位点 C0 必须**先于**数据读取采集：revision 随时间单调递增，故
        // C0 ≤ 数据读取实际反映的 revision。这是 `ConfigListResponse.revision`
        // 的安全方向——回放 [C0, ∞) 必然覆盖快照之外的一切变更（可重复、不可丢）。
        let c0 = Self::read_current_revision(inner, prefix, &range_end).await?;

        let pairs = inner
            .client
            .kv()
            .range(prefix, &range_end, 0, 0)
            .await
            .map_err(|e| format!("failed to load configs: {e}"))?;

        let entries: Vec<ConfigEntry> = pairs
            .into_iter()
            .filter_map(|(_k, v)| serde_json::from_slice(&v).ok())
            .collect();

        cache.write().load_full(entries.clone());
        // 换缓存**之后**才推进位点：在位点推进前 List 仍返回旧位点
        // （≤ 当前缓存状态对应的 revision），方向依旧安全。
        watermark.store(c0, std::sync::atomic::Ordering::SeqCst);
        Ok(entries)
    }

    /// 采集 Server 当前全局 revision（只读）；用作快照位点 C0。
    async fn read_current_revision(
        inner: &Arc<AgentInner>,
        key: &[u8],
        range_end: &[u8],
    ) -> Result<i64, String> {
        // limit=1 + count_only：不拉取数据行，仅取响应 revision（响应体恒空）。
        let (_kvs, _count, revision) = inner
            .client
            .kv()
            .range_full(key, range_end, 1, 0, true, true)
            .await
            .map_err(|e| format!("failed to read current revision: {e}"))?;
        Ok(revision)
    }

    /// KV 变更 → `ConfigWatchEvent`。
    ///
    /// - 删除 = `new_value` 缺省（presence 语义的唯一锚点）；
    /// - 值 = `ConfigEntry` JSON 的 `value` 字段（畸形 JSON 跳过并告警——与
    ///   缓存 `apply_event` 的对账口径一致，避免两处各自发散）；
    /// - `revision` = KV 全局位点（非每键版本号）。
    fn watch_event_from_kv(
        kv_key: &[u8],
        value: &[u8],
        event_type: i32,
        revision: i64,
    ) -> Option<ConfigWatchEvent> {
        use coord_proto::watch::watch_event::EventType;
        let key_str = String::from_utf8_lossy(kv_key);
        let key = key_str.strip_prefix("/_config/")?.to_string();
        if event_type == EventType::Delete as i32 {
            return Some(ConfigWatchEvent {
                key,
                new_value: None,
                revision,
            });
        }
        match serde_json::from_slice::<ConfigEntry>(value) {
            Ok(entry) => Some(ConfigWatchEvent {
                key,
                new_value: Some(entry.value),
                revision,
            }),
            Err(e) => {
                tracing::warn!("ConfigCenterService: skip malformed config entry '{key}': {e}");
                None
            }
        }
    }

    /// 缓存条目数
    pub fn cache_len(&self) -> usize {
        self.cache.read().len()
    }

    /// 是否处于回退模式
    pub fn is_fallback(&self) -> bool {
        self.cache.read().is_fallback()
    }
}

#[async_trait]
impl BaseService for ConfigCenterService {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    async fn start(&self) -> ServiceResult<()> {
        tracing::info!("ConfigCenterService: starting");

        // 从 Server 全量拉取配置
        match self.load_all().await {
            Ok(_) => {
                *self.healthy.write() = true;
            }
            Err(e) => {
                tracing::warn!("ConfigCenterService: failed to load initial configs: {e}; starting with empty cache");
                *self.healthy.write() = true;
            }
        }

        // 启动 Watch 后台任务
        let (_tx, mut rx) = watch::channel::<()>(());
        *self.shutdown_tx.write() = Some(_tx);

        let inner = self.inner.clone();
        let cache = self.cache.clone();
        let watermark = self.watermark.clone();
        let watch_task = async move {
            tracing::info!("ConfigCenterService: Watch background task started");
            let prefix: &[u8] = b"/_config/";
            // R-AGT-14：水位续传。水位定义收敛到 `watermark`（快照位点 W）：
            // 服务端回放 revision ≥ W 的变更后转实时，断连窗口由回放补齐
            // （含 W 本身，重复应用幂等）。
            let mut attempt: u32 = 0;

            'outer: loop {
                // 位点 0 = 尚未完成一次对账：先对账（采集 C0 + 全量换缓存），
                // 否则订阅「从最新开始」会漏掉启动/断连窗口内的全部变更。
                if watermark.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                    match Self::reload_from_server(&inner, &cache, &watermark).await {
                        Ok(entries) => {
                            tracing::info!(
                                "ConfigCenterService: initial reconcile loaded {} configs",
                                entries.len()
                            );
                            attempt = 0;
                        }
                        Err(e) => {
                            tracing::warn!(
                                "ConfigCenterService: initial reconcile failed: {e}; retrying"
                            );
                            attempt += 1;
                            let backoff = std::time::Duration::from_secs(1u64 << attempt.min(5));
                            tokio::time::sleep(backoff).await;
                            continue 'outer;
                        }
                    }
                }

                let start_rev = watermark.load(std::sync::atomic::Ordering::SeqCst);
                // 订阅 Watch（自位点续传；>0 时服务端先回放再转实时）
                let event_rx = match inner.client.watch().watch(prefix, start_rev).await {
                    Ok(rx) => rx,
                    Err(e) => {
                        tracing::warn!(
                            "ConfigCenterService: failed to subscribe Watch: {e}; fallback + reconcile"
                        );
                        cache.write().enter_fallback();
                        attempt += 1;
                        let backoff = std::time::Duration::from_secs(1u64 << attempt.min(5));
                        tokio::time::sleep(backoff).await;
                        if let Err(e2) = Self::reload_from_server(&inner, &cache, &watermark).await
                        {
                            tracing::error!(
                                "ConfigCenterService: reconcile after subscribe failure failed: {e2}"
                            );
                        }
                        continue 'outer;
                    }
                };
                let mut event_rx = event_rx;

                // 消费事件，断连/溢出 → 退到外层对账重连
                loop {
                    tokio::select! {
                        _ = rx.changed() => {
                            tracing::info!("ConfigCenterService: Watch background task shutting down");
                            break 'outer;
                        }
                        event = event_rx.recv() => {
                            match event {
                                Some(Ok(we)) => {
                                    use coord_proto::watch::watch_event::EventType;
                                    // 溢出/历史不可用 → 缓存可能不完整，须全量对账
                                    let needs_reconcile = matches!(
                                        we.r#type,
                                        t if t == EventType::BufferOverflow as i32
                                            || t == EventType::HistoryUnavailable as i32
                                    );
                                    for kv in &we.kvs {
                                        let value = if we.r#type == EventType::Delete as i32 {
                                            None
                                        } else {
                                            Some(kv.value.as_slice())
                                        };
                                        cache.write().apply_event(&kv.key, value);
                                    }
                                    if cache.read().is_fallback() {
                                        cache.write().exit_fallback();
                                    }
                                    // 位点推进：本批次已全部应用（缓存包含全部 ≤ revision 的变更）
                                    if we.revision > 0 {
                                        watermark.fetch_max(
                                            we.revision,
                                            std::sync::atomic::Ordering::SeqCst,
                                        );
                                    }
                                    if needs_reconcile {
                                        tracing::warn!(
                                            "ConfigCenterService: Watch overflow/history-unavailable; full reconcile"
                                        );
                                        cache.write().enter_fallback();
                                        break;
                                    }
                                }
                                Some(Err(e)) => {
                                    tracing::warn!(
                                        "ConfigCenterService: Watch stream error: {e}; reconcile + reconnect"
                                    );
                                    cache.write().enter_fallback();
                                    break;
                                }
                                None => {
                                    tracing::warn!(
                                        "ConfigCenterService: Watch stream ended; reconcile + reconnect"
                                    );
                                    cache.write().enter_fallback();
                                    break;
                                }
                            }
                        }
                    }
                }

                // R-AGT-14：断连/溢出 → 指数退避 + 全量对账（覆盖式消除断连窗口差异）。
                // 对账把位点推进到新的 C0；下一次订阅从该位点续订，对账读取与下次
                // 订阅之间的缝隙由服务端回放补齐（不得把位点重置为 0，否则缝隙会静默丢事件）。
                attempt += 1;
                let backoff = std::time::Duration::from_secs(1u64 << attempt.min(5));
                tracing::info!(
                    "ConfigCenterService: reconnecting in {backoff:?} (attempt {attempt}, watermark={})",
                    watermark.load(std::sync::atomic::Ordering::SeqCst)
                );
                tokio::time::sleep(backoff).await;
                match Self::reload_from_server(&inner, &cache, &watermark).await {
                    Ok(entries) => {
                        tracing::info!(
                            "ConfigCenterService: reconciled {} configs after disconnect",
                            entries.len()
                        );
                        attempt = 0;
                    }
                    Err(e) => {
                        tracing::error!(
                            "ConfigCenterService: reconcile after disconnect failed: {e}; retrying"
                        );
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

        Ok(())
    }

    async fn stop(&self) -> ServiceResult<()> {
        tracing::info!("ConfigCenterService: stopping");
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

impl std::fmt::Debug for ConfigCenterService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConfigCenterService")
            .field("cache_len", &self.cache_len())
            .field("healthy", &self.health_check())
            .finish()
    }
}

// ──── gRPC Config trait 实现 ────

use coord_proto::agent::config_server::Config;
use coord_proto::agent::{
    ConfigGetRequest, ConfigGetResponse, ConfigListRequest, ConfigListResponse, ConfigPutRequest,
    ConfigPutResponse, ConfigWatchEvent, ConfigWatchRequest,
};

#[tonic::async_trait]
impl Config for ConfigCenterService {
    async fn get(
        &self,
        request: tonic::Request<ConfigGetRequest>,
    ) -> Result<tonic::Response<ConfigGetResponse>, tonic::Status> {
        let req = request.into_inner();
        let entry = self.get(&req.key);
        match entry {
            Some(e) => Ok(tonic::Response::new(ConfigGetResponse {
                value: e.value.clone(),
                found: true,
            })),
            None => Ok(tonic::Response::new(ConfigGetResponse {
                value: String::new(),
                found: false,
            })),
        }
    }

    async fn put(
        &self,
        request: tonic::Request<ConfigPutRequest>,
    ) -> Result<tonic::Response<ConfigPutResponse>, tonic::Status> {
        let req = request.into_inner();
        let (_entry, revision) = self
            .set(&req.key, &req.value)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(tonic::Response::new(ConfigPutResponse {
            revision: revision as i64,
        }))
    }

    async fn list(
        &self,
        request: tonic::Request<ConfigListRequest>,
    ) -> Result<tonic::Response<ConfigListResponse>, tonic::Status> {
        let req = request.into_inner();
        let entries: std::collections::HashMap<String, String> = self
            .get_by_prefix(&req.prefix)
            .into_iter()
            .map(|e| (e.key, e.value))
            .collect();
        // 快照位点 W（≤ 缓存状态对应的 revision）：消费者以它为起订位点即可
        // 闭合「快照 → watch 建立」之间的 resync 窗口（见 config.proto 头注）。
        Ok(tonic::Response::new(ConfigListResponse {
            entries,
            revision: self.watermark.load(std::sync::atomic::Ordering::SeqCst),
        }))
    }

    /// Server streaming response type for the Watch method.
    type WatchStream =
        tokio_stream::wrappers::ReceiverStream<Result<ConfigWatchEvent, tonic::Status>>;

    /// Config Watch：**逐订阅者直连 Server KV Watch**。
    ///
    /// 修复旧实现的两处语义缺口：
    /// ① 旧实现只广播本进程 `set()` 的变更——其他 agent / 直连 KV 的变更与
    ///   **删除**不会下发；② `start_revision` 被忽略——断线续订静默丢窗口。
    ///
    /// 事件约定（与 config.proto 头注一致）：`revision` = KV 全局位点；
    /// `new_value` 缺省 = 删除。溢出 / 历史不可用 ⇒ 以错误状态终止流，由消费者
    /// 按位点重连（重放补齐）或重读 `List` 重建。
    async fn watch(
        &self,
        request: tonic::Request<ConfigWatchRequest>,
    ) -> Result<tonic::Response<Self::WatchStream>, tonic::Status> {
        let req = request.into_inner();
        let kv_prefix = {
            let mut k = b"/_config/".to_vec();
            k.extend_from_slice(req.prefix.as_bytes());
            k
        };
        let start_revision = req.start_revision;
        let inner = self.inner.clone();
        let (tx, out_rx) = tokio::sync::mpsc::channel(64);

        tokio::spawn(async move {
            let mut event_rx = match inner.client.watch().watch(&kv_prefix, start_revision).await {
                Ok(rx) => rx,
                Err(e) => {
                    let _ = tx
                        .send(Err(tonic::Status::unavailable(format!(
                            "config watch subscribe failed: {e}"
                        ))))
                        .await;
                    return;
                }
            };

            loop {
                match event_rx.recv().await {
                    Some(Ok(we)) => {
                        use coord_proto::watch::watch_event::EventType;
                        match EventType::try_from(we.r#type) {
                            Ok(EventType::BufferOverflow) => {
                                // 本订阅者缓冲溢出 = 已有事件被丢：终止流，客户端按
                                // last_revision + 1 重连（服务端回放补齐窗口）。
                                let _ = tx
                                    .send(Err(tonic::Status::unavailable(
                                        "config watch buffer overflow; \
                                         reconnect from last revision to replay",
                                    )))
                                    .await;
                                return;
                            }
                            Ok(EventType::HistoryUnavailable) => {
                                // 请求位点已被压缩：无法保证不丢。终止流；消费者
                                // 必须重读 List 取新位点后重订（不得静默续跑）。
                                let _ = tx
                                    .send(Err(tonic::Status::failed_precondition(
                                        "requested config history is no longer available; \
                                         re-read List for a fresh revision and resubscribe",
                                    )))
                                    .await;
                                return;
                            }
                            _ => {
                                for kv in &we.kvs {
                                    let Some(event) = Self::watch_event_from_kv(
                                        &kv.key,
                                        kv.value.as_slice(),
                                        we.r#type,
                                        we.revision,
                                    ) else {
                                        continue;
                                    };
                                    if tx.send(Ok(event)).await.is_err() {
                                        return; // client disconnected
                                    }
                                }
                            }
                        }
                    }
                    Some(Err(e)) => {
                        let _ = tx
                            .send(Err(tonic::Status::unavailable(format!(
                                "config watch stream error: {e}; reconnect to resume"
                            ))))
                            .await;
                        return;
                    }
                    None => {
                        let _ = tx
                            .send(Err(tonic::Status::internal("config watch stream closed")))
                            .await;
                        return;
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

    // ──── ConfigEntry 测试 ────

    #[test]
    fn test_config_entry_creation() {
        let entry = ConfigEntry::new("app.order-service.db.host", "localhost");
        assert_eq!(entry.key, "app.order-service.db.host");
        assert_eq!(entry.value, "localhost");
        assert_eq!(entry.version, 1);
        assert!(entry.updated_at > 0);
    }

    #[test]
    fn test_config_entry_storage_key() {
        let key = ConfigEntry::storage_key("app.db.host");
        assert_eq!(String::from_utf8_lossy(&key), "/_config/app.db.host");
    }

    #[test]
    fn test_config_entry_serialization_roundtrip() {
        let entry = ConfigEntry {
            key: "test.key".into(),
            value: "test-value".into(),
            version: 3,
            updated_at: 1700000000,
        };
        let json = serde_json::to_vec(&entry).unwrap();
        let restored: ConfigEntry = serde_json::from_slice(&json).unwrap();
        assert_eq!(restored, entry);
    }

    // ──── ConfigCache 测试 ────

    #[test]
    fn test_config_cache_put_and_get() {
        let mut cache = ConfigCache::new();
        let entry = ConfigEntry::new("app.host", "10.0.0.1");
        cache.put(entry.clone());

        assert_eq!(cache.len(), 1);
        assert!(!cache.is_empty());

        let found = cache.get("app.host").unwrap();
        assert_eq!(found.value, "10.0.0.1");
        assert_eq!(found.version, 1);
    }

    #[test]
    fn test_config_cache_get_value() {
        let mut cache = ConfigCache::new();
        cache.put(ConfigEntry::new("app.port", "8080"));

        assert_eq!(cache.get_value("app.port"), Some("8080"));
        assert_eq!(cache.get_value("nonexistent"), None);
    }

    #[test]
    fn test_config_cache_overwrite() {
        let mut cache = ConfigCache::new();
        cache.put(ConfigEntry::new("app.host", "10.0.0.1"));

        let entry_v2 = ConfigEntry {
            key: "app.host".into(),
            value: "10.0.0.2".into(),
            version: 2,
            updated_at: unix_ts(),
        };
        cache.put(entry_v2);

        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get_value("app.host"), Some("10.0.0.2"));
    }

    #[test]
    fn test_config_cache_remove() {
        let mut cache = ConfigCache::new();
        cache.put(ConfigEntry::new("app.host", "10.0.0.1"));
        cache.put(ConfigEntry::new("app.port", "8080"));

        let removed = cache.remove("app.host").unwrap();
        assert_eq!(removed.value, "10.0.0.1");
        assert_eq!(cache.len(), 1);
        assert!(cache.get("app.host").is_none());
        assert!(cache.get("app.port").is_some());
    }

    #[test]
    fn test_config_cache_prefix_query() {
        let mut cache = ConfigCache::new();
        cache.put(ConfigEntry::new("app.order.db.host", "db1"));
        cache.put(ConfigEntry::new("app.order.db.port", "5432"));
        cache.put(ConfigEntry::new("app.payment.db.host", "db2"));
        cache.put(ConfigEntry::new("other.service.url", "http://x"));

        let app_order = cache.get_by_prefix("app.order");
        assert_eq!(app_order.len(), 2);

        let app = cache.get_by_prefix("app");
        assert_eq!(app.len(), 3);

        let none = cache.get_by_prefix("nonexistent");
        assert!(none.is_empty());
    }

    #[test]
    fn test_config_cache_load_full() {
        let mut cache = ConfigCache::new();
        cache.put(ConfigEntry::new("old.key", "old-value"));
        assert_eq!(cache.len(), 1);

        let new_entries = vec![
            ConfigEntry::new("new.a", "a"),
            ConfigEntry::new("new.b", "b"),
        ];
        cache.load_full(new_entries);
        assert_eq!(cache.len(), 2);
        assert!(cache.get("old.key").is_none());
        assert!(cache.get("new.a").is_some());
    }

    #[test]
    fn test_config_cache_fallback_mode() {
        let mut cache = ConfigCache::new();
        assert!(!cache.is_fallback());

        cache.enter_fallback();
        assert!(cache.is_fallback());

        // load_full 应退出回退模式
        cache.load_full(vec![]);
        assert!(!cache.is_fallback());

        cache.enter_fallback();
        cache.exit_fallback();
        assert!(!cache.is_fallback());
    }

    #[test]
    fn test_config_cache_apply_event_put_and_delete() {
        let mut cache = ConfigCache::new();

        let entry = ConfigEntry::new("test.key", "test-value");
        let storage_key = ConfigEntry::storage_key("test.key");
        let value = serde_json::to_vec(&entry).unwrap();

        // Watch Put 事件
        cache.apply_event(&storage_key, Some(&value));
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get_value("test.key"), Some("test-value"));

        // Watch Delete 事件
        cache.apply_event(&storage_key, None);
        assert_eq!(cache.len(), 0);
        assert!(cache.get("test.key").is_none());
    }

    #[test]
    fn test_config_cache_apply_event_invalid_json() {
        let mut cache = ConfigCache::new();
        let storage_key = ConfigEntry::storage_key("test.key");

        // 无效 JSON 不 panic，仅忽略
        cache.apply_event(&storage_key, Some(b"not-valid-json"));
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_config_cache_default() {
        let cache = ConfigCache::default();
        assert!(cache.is_empty());
        assert!(!cache.is_fallback());
    }

    // ──── ConfigCenterService 名称常量测试 ────

    #[test]
    fn test_config_center_name_constant() {
        assert_eq!(ConfigCenterService::NAME, "config_center");
    }

    // ──── KV 变更 → Watch 事件翻译（presence 语义的单一出口）────

    #[test]
    fn test_watch_event_from_kv_put_delete_and_malformed() {
        use coord_proto::watch::watch_event::EventType;

        let entry = ConfigEntry::new("app.db.host", "db1");
        let json = serde_json::to_vec(&entry).unwrap();

        // PUT → new_value 在场（可为空串）
        let ev = ConfigCenterService::watch_event_from_kv(
            b"/_config/app.db.host",
            &json,
            EventType::Put as i32,
            42,
        )
        .expect("put event");
        assert_eq!(ev.key, "app.db.host");
        assert_eq!(ev.new_value.as_deref(), Some("db1"));
        assert_eq!(ev.revision, 42);

        // 空值 PUT → new_value 在场且为空串（≠ 删除）
        let empty_entry = ConfigEntry::new("app.empty", "");
        let empty_json = serde_json::to_vec(&empty_entry).unwrap();
        let ev = ConfigCenterService::watch_event_from_kv(
            b"/_config/app.empty",
            &empty_json,
            EventType::Put as i32,
            43,
        )
        .expect("empty-value put event");
        assert_eq!(ev.new_value.as_deref(), Some(""));

        // DELETE → new_value 缺省（presence = 删除语义的锚点）
        let ev = ConfigCenterService::watch_event_from_kv(
            b"/_config/app.db.host",
            b"",
            EventType::Delete as i32,
            44,
        )
        .expect("delete event");
        assert!(ev.new_value.is_none());
        assert_eq!(ev.revision, 44);

        // 畸形 JSON → 跳过（与缓存 apply_event 的对账口径一致）
        assert!(ConfigCenterService::watch_event_from_kv(
            b"/_config/broken",
            b"not-json",
            EventType::Put as i32,
            45,
        )
        .is_none());

        // 非 /_config/ 前缀 → 防御性跳过
        assert!(ConfigCenterService::watch_event_from_kv(
            b"/other/key",
            &json,
            EventType::Put as i32,
            46,
        )
        .is_none());
    }
}
