// coord-agent: 线程池资源隔离模块 (Phase A)
//
// v8.2 §3.2: 资源隔离策略
// - proxy_core: 核心代理路径（高优先级），默认 8 线程
// - dataplane: 数据面读写、复制流（中优先级），默认 4 线程
// - background: Watch 同步、淘汰、心跳（低优先级），默认 2 线程
//
// 使用 tokio::runtime::Handle 在当前 runtime 上 spawn 任务，
// 通过独立 tokio::task::JoinSet 跟踪各池任务生命周期。

use std::future::Future;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::task::JoinSet;

// ──── ThreadPoolConfig ────

/// 线程池大小配置
///
/// 支持从 TOML 配置文件 `[agent.threadpools]` 段反序列化。
/// 零值表示使用 tokio 默认（不限制并发）。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ThreadPoolConfig {
    /// 核心代理路径线程数（默认 8）
    #[serde(default = "default_proxy_core_size")]
    pub proxy_core_size: usize,

    /// 数据面读写、复制流线程数（默认 4）
    #[serde(default = "default_dataplane_size")]
    pub dataplane_size: usize,

    /// Watch 同步、淘汰、心跳线程数（默认 2）
    #[serde(default = "default_background_size")]
    pub background_size: usize,
}

fn default_proxy_core_size() -> usize { 8 }
fn default_dataplane_size() -> usize { 4 }
fn default_background_size() -> usize { 2 }

impl Default for ThreadPoolConfig {
    fn default() -> Self {
        Self {
            proxy_core_size: 8,
            dataplane_size: 4,
            background_size: 2,
        }
    }
}

// ──── AgentThreadPools ────

/// Agent 线程池管理器
///
/// 提供按优先级分池的 spawn 方法。
/// 当前实现基于 tokio 多线程 runtime，通过 JoinSet 跟踪各池任务。
/// 未来可扩展为独立 tokio Runtime 实例以实现真正的线程隔离。
pub struct AgentThreadPools {
    config: ThreadPoolConfig,
    proxy_core_tasks: Arc<Mutex<JoinSet<()>>>,
    dataplane_tasks: Arc<Mutex<JoinSet<()>>>,
    background_tasks: Arc<Mutex<JoinSet<()>>>,
}

impl AgentThreadPools {
    /// 使用给定配置创建线程池管理器
    pub fn new(config: ThreadPoolConfig) -> Self {
        Self {
            config,
            proxy_core_tasks: Arc::new(Mutex::new(JoinSet::new())),
            dataplane_tasks: Arc::new(Mutex::new(JoinSet::new())),
            background_tasks: Arc::new(Mutex::new(JoinSet::new())),
        }
    }

    /// 在 proxy_core 池中 spawn 任务
    pub fn spawn_proxy_core<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.proxy_core_tasks.lock().spawn(future);
    }

    /// 在 dataplane 池中 spawn 任务
    pub fn spawn_dataplane<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.dataplane_tasks.lock().spawn(future);
    }

    /// 在 background 池中 spawn 任务
    pub fn spawn_background<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.background_tasks.lock().spawn(future);
    }

    /// 获取配置引用
    pub fn config(&self) -> &ThreadPoolConfig {
        &self.config
    }

    /// 优雅关闭：等待所有池中任务完成
    pub async fn shutdown(&self) {
        // 按优先级逆序等待：background → dataplane → proxy_core
        let mut bg = self.background_tasks.lock();
        while bg.join_next().await.is_some() {}
        let mut dp = self.dataplane_tasks.lock();
        while dp.join_next().await.is_some() {}
        let mut pc = self.proxy_core_tasks.lock();
        while pc.join_next().await.is_some() {}
    }
}

impl std::fmt::Debug for AgentThreadPools {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgentThreadPools")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

// ──── tests ────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_thread_pool_config_defaults() {
        let config = ThreadPoolConfig::default();
        assert_eq!(config.proxy_core_size, 8);
        assert_eq!(config.dataplane_size, 4);
        assert_eq!(config.background_size, 2);
    }

    #[test]
    fn test_thread_pool_config_serialization() {
        let config = ThreadPoolConfig {
            proxy_core_size: 16,
            dataplane_size: 8,
            background_size: 4,
        };
        let toml_str = toml::to_string(&config).expect("序列化失败");
        assert!(toml_str.contains("proxy_core_size = 16"));
        assert!(toml_str.contains("dataplane_size = 8"));
        assert!(toml_str.contains("background_size = 4"));
    }

    #[test]
    fn test_thread_pool_config_deserialization() {
        let toml_str = r#"
proxy_core_size = 12
dataplane_size = 6
background_size = 3
"#;
        let config: ThreadPoolConfig = toml::from_str(toml_str).expect("反序列化失败");
        assert_eq!(config.proxy_core_size, 12);
        assert_eq!(config.dataplane_size, 6);
        assert_eq!(config.background_size, 3);
    }
}
