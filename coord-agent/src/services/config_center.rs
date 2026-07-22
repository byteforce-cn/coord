// coord-agent: 配置中心 (Config Center Service)
//
// 实现 BaseService trait，提供配置读写、热更新能力。
// 基于 Coord 核心原语（KV + Watch）构建。
//
// 架构（v3.0）:
// - 本地配置缓存，Watch 驱动热更新
// - 断连时回退缓存配置
// - 支持按应用/环境/标签维度隔离
//
// 参见 docs/client-agent-architecture-v3.md §5.2。

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
    /// Watch 事件广播（用于 gRPC Watch 流）
    watch_tx: tokio::sync::broadcast::Sender<ConfigWatchEvent>,
}

impl ConfigCenterService {
    /// 服务名称常量
    pub const NAME: &'static str = "config_center";

    /// 创建 ConfigCenterService
    pub fn new(inner: Arc<AgentInner>) -> Self {
        let (watch_tx, _) = tokio::sync::broadcast::channel(256);
        Self {
            inner,
            cache: Arc::new(ParkingRwLock::new(ConfigCache::new())),
            healthy: ParkingRwLock::new(false),
            shutdown_tx: ParkingRwLock::new(None),
            watch_tx,
        }
    }

    /// 设置配置（写入 Server + 更新本地缓存）

    /// 设置配置（写入 Server + 更新本地缓存）
    pub async fn set(&self, key: &str, value: &str) -> ServiceResult<ConfigEntry> {
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

        self.inner
            .client
            .kv()
            .put(&storage_key, &serialized)
            .await
            .map_err(|e| format!("failed to write config '{key}': {e}"))?;

        // 更新本地缓存
        self.cache.write().put(entry.clone());

        // 广播 Watch 事件
        let _ = self.watch_tx.send(ConfigWatchEvent {
            key: key.to_string(),
            new_value: Some(value.to_string()),
            revision: entry.version as i64,
        });

        tracing::info!("ConfigCenter: set key='{key}', version={}", entry.version);
        Ok(entry)
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
        let prefix = b"/_config/";
        let mut range_end = prefix.to_vec();
        if let Some(last) = range_end.last_mut() {
            *last = last.wrapping_add(1);
        }

        let pairs = self
            .inner
            .client
            .kv()
            .range(prefix, &range_end, 0, 0)
            .await
            .map_err(|e| format!("failed to load configs: {e}"))?;

        let entries: Vec<ConfigEntry> = pairs
            .into_iter()
            .filter_map(|(_k, v)| serde_json::from_slice(&v).ok())
            .collect();

        let count = entries.len();
        self.cache.write().load_full(entries.clone());
        tracing::info!("ConfigCenter: loaded {count} configs from server");
        Ok(entries)
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
        tokio::spawn(async move {
            tracing::info!("ConfigCenterService: Watch background task started");
            let prefix = b"/_config/";

            // 首次订阅 Watch
            let mut event_rx = match inner.client.watch().watch(prefix, 0).await {
                Ok(rx) => rx,
                Err(e) => {
                    tracing::warn!("ConfigCenterService: failed to subscribe Watch: {e}; entering fallback");
                    cache.write().enter_fallback();
                    return;
                }
            };

            loop {
                tokio::select! {
                    _ = rx.changed() => {
                        tracing::info!("ConfigCenterService: Watch background task shutting down");
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
                                if cache.read().is_fallback() {
                                    cache.write().exit_fallback();
                                }
                            }
                            Some(Err(e)) => {
                                tracing::warn!("ConfigCenterService: Watch stream error: {e}; reconnecting...");
                                cache.write().enter_fallback();
                                match inner.client.watch().watch(prefix, 0).await {
                                    Ok(new_rx) => {
                                        event_rx = new_rx;
                                        tracing::info!("ConfigCenterService: Watch reconnected");
                                    }
                                    Err(e2) => {
                                        tracing::error!("ConfigCenterService: Watch reconnect failed: {e2}");
                                        break;
                                    }
                                }
                            }
                            None => {
                                tracing::warn!("ConfigCenterService: Watch stream ended; reconnecting...");
                                cache.write().enter_fallback();
                                match inner.client.watch().watch(prefix, 0).await {
                                    Ok(new_rx) => {
                                        event_rx = new_rx;
                                        tracing::info!("ConfigCenterService: Watch reconnected");
                                    }
                                    Err(e) => {
                                        tracing::error!("ConfigCenterService: Watch reconnect failed: {e}");
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

use coord_proto::agent::{
    ConfigGetRequest, ConfigGetResponse,
    ConfigPutRequest, ConfigPutResponse,
    ConfigListRequest, ConfigListResponse,
    ConfigWatchRequest, ConfigWatchEvent,
};
use coord_proto::agent::config_server::Config;

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
        let entry = self.set(&req.key, &req.value).await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(tonic::Response::new(ConfigPutResponse {
            revision: entry.version as i64,
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
        Ok(tonic::Response::new(ConfigListResponse { entries }))
    }

    /// Server streaming response type for the Watch method.
    type WatchStream = tokio_stream::wrappers::ReceiverStream<
        Result<ConfigWatchEvent, tonic::Status>,
    >;

    async fn watch(
        &self,
        request: tonic::Request<ConfigWatchRequest>,
    ) -> Result<tonic::Response<Self::WatchStream>, tonic::Status> {
        let req = request.into_inner();
        let prefix = req.prefix.clone();
        let mut rx = self.watch_tx.subscribe();
        let (tx, out_rx) = tokio::sync::mpsc::channel(32);

        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        if event.key.starts_with(&prefix) {
                            if tx.send(Ok(event)).await.is_err() {
                                break; // client disconnected
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("ConfigCenter watch lagged by {n} events");
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
}
