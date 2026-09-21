// Connection Pool — gRPC channel pool management
//
// Features:
// - Per-endpoint connection pool (default 2 connections per endpoint)
// - Watch uses independent connections
// - Idle connections closed after 5 minutes, recreated on demand
// - Thread-safe (Arc<RwLock<>>)

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

use coord_core::error::{Error, Result};

use crate::config::{Config, TlsConfig};
use crate::credential::{AuthedChannel, CredentialInterceptor, NoopTokenProvider, TokenProvider};

// ──── Pool Entry ────

/// A single channel in the pool with idle tracking
#[derive(Clone)]
struct PoolChannel {
    channel: AuthedChannel,
    /// When this channel was last used
    last_used: Instant,
}

/// Pool of channels for a single endpoint
struct EndpointPool {
    /// Available channels for this endpoint
    channels: Vec<PoolChannel>,
    /// Max channels per endpoint
    max_connections: usize,
}

impl EndpointPool {
    fn new(max_connections: usize) -> Self {
        Self {
            channels: Vec::with_capacity(max_connections),
            max_connections,
        }
    }

    /// Return a channel to the pool
    fn put(&mut self, channel: AuthedChannel) {
        if self.channels.len() < self.max_connections {
            self.channels.push(PoolChannel {
                channel,
                last_used: Instant::now(),
            });
        }
        // If pool is full, drop the channel (it will be closed)
    }

    /// Remove idle channels that exceed the timeout
    fn cleanup_idle(&mut self, idle_timeout: Duration) -> usize {
        let now = Instant::now();
        let before = self.channels.len();
        self.channels
            .retain(|pc| now.duration_since(pc.last_used) < idle_timeout);
        before - self.channels.len()
    }
}

// ──── Connection Pool ────

/// gRPC connection pool for a Coord client
///
/// Maintains separate pools per endpoint. Watch clients get dedicated
/// connections from a separate pool to avoid contention with short-lived RPCs.
pub struct ConnectionPool {
    /// Regular connection pools per endpoint
    pools: Arc<RwLock<HashMap<String, EndpointPool>>>,
    /// Watch-specific connection pools per endpoint
    watch_pools: Arc<RwLock<HashMap<String, EndpointPool>>>,
    /// 上次空闲连接清理的时间戳。
    ///
    /// W1-5：`cleanup_idle()` 此前**全仓零生产调用** —— 文件头注释承诺的
    /// “闲置连接 5 分钟后关闭”从未发生，端点 key 与最多 2 条 HTTP/2 连接
    /// 永不回收。这里做成「访问时机会式清理」（见 [`Self::maybe_sweep`]），
    /// 而不是新增一个后台任务：`coord-client` 可能在**没有 tokio runtime**
    /// 的上下文里构造（例如同步的单测与 CLI 路径），`tokio::spawn` 会 panic。
    last_sweep: Arc<parking_lot::Mutex<Instant>>,
    /// Pool configuration
    max_connections_per_endpoint: usize,
    connect_timeout: Duration,
    idle_timeout: Duration,
    /// TLS/mTLS 通道配置（None = 明文 http）
    tls: Option<Arc<TlsConfig>>,
    /// 出站凭据提供者（None = 不附加鉴权头）
    token_provider: Arc<dyn TokenProvider>,
}

impl ConnectionPool {
    /// Create a new connection pool from client config
    pub fn new(config: &Config) -> Self {
        Self {
            pools: Arc::new(RwLock::new(HashMap::new())),
            watch_pools: Arc::new(RwLock::new(HashMap::new())),
            last_sweep: Arc::new(parking_lot::Mutex::new(Instant::now())),
            max_connections_per_endpoint: config.connections_per_endpoint,
            connect_timeout: config.connect_timeout,
            idle_timeout: config.connection_idle_timeout,
            tls: config.tls.clone().map(Arc::new),
            token_provider: config
                .token_provider
                .clone()
                .unwrap_or_else(|| Arc::new(NoopTokenProvider)),
        }
    }

    /// 把一个裸 Channel 包装为携带凭据的通道。
    pub(crate) fn wrap(&self, channel: Channel) -> AuthedChannel {
        InterceptedService::new(
            channel,
            CredentialInterceptor::new(Arc::clone(&self.token_provider)),
        )
    }

    /// Get a regular channel for the given endpoint
    pub async fn get(&self, endpoint: &str) -> Result<AuthedChannel> {
        self.get_from(&self.pools, endpoint).await
    }

    /// Get a watch-dedicated channel for the given endpoint
    pub async fn get_watch(&self, endpoint: &str) -> Result<AuthedChannel> {
        self.get_from(&self.watch_pools, endpoint).await
    }

    /// Return a regular channel to the pool
    pub fn put(&self, endpoint: &str, channel: AuthedChannel) {
        let mut pools = self.pools.write();
        let pool = pools
            .entry(endpoint.to_string())
            .or_insert_with(|| EndpointPool::new(self.max_connections_per_endpoint));
        pool.put(channel);
    }

    /// Return a watch channel to the pool
    pub fn put_watch(&self, endpoint: &str, channel: AuthedChannel) {
        let mut pools = self.watch_pools.write();
        let pool = pools
            .entry(endpoint.to_string())
            .or_insert_with(|| EndpointPool::new(self.max_connections_per_endpoint));
        pool.put(channel);
    }

    /// Clean up idle connections across all pools
    pub fn cleanup_idle(&self) -> usize {
        let mut cleaned = 0;
        for pools in [&self.pools, &self.watch_pools] {
            let mut pools_guard = pools.write();
            pools_guard.retain(|_, pool| {
                let removed = pool.cleanup_idle(self.idle_timeout);
                cleaned += removed;
                !pool.channels.is_empty()
            });
        }
        if cleaned > 0 {
            tracing::debug!(
                cleaned,
                idle_timeout_secs = self.idle_timeout.as_secs(),
                "connection pool: reclaimed idle channels"
            );
        }
        cleaned
    }

    /// 机会式空闲连接清理（W1-5）：距上次清理超过 `idle_timeout` 时顺带清一遍。
    ///
    /// 放在取连接的热路径上，但**至多每 `idle_timeout` 触发一次**，代价是
    /// O(端点数 × 每端点连接数)；不新增后台任务（见 `last_sweep` 字段注释）。
    /// `idle_timeout` 为 0（配置成“不回收”）时用 60s 作为最小节流窗口，
    /// 避免每次取连接都做一次全表扫描。
    fn maybe_sweep(&self) {
        let min_interval = if self.idle_timeout.is_zero() {
            Duration::from_secs(60)
        } else {
            self.idle_timeout
        };
        {
            let last = self.last_sweep.lock();
            if last.elapsed() < min_interval {
                return;
            }
        }
        // 双检（持锁）：并发取连接时只有一个线程真正执行清理。
        {
            let mut last = self.last_sweep.lock();
            if last.elapsed() < min_interval {
                return;
            }
            *last = Instant::now();
        }
        self.cleanup_idle();
    }

    // ──── Internal ────

    async fn get_from(
        &self,
        pools: &Arc<RwLock<HashMap<String, EndpointPool>>>,
        endpoint: &str,
    ) -> Result<AuthedChannel> {
        // Try to get an existing channel from the pool (under lock)
        {
            let mut pools_guard = pools.write();
            if let Some(pool) = pools_guard.get_mut(endpoint) {
                if let Some(channel) = pool.channels.pop() {
                    return Ok(channel.channel);
                }
            }
        }
        // No existing channel, create a new connection (lock released)
        let channel =
            crate::tls::connect(endpoint, Some(self.connect_timeout), self.tls.as_deref())
                .await
                .map_err(|e| Error::ClusterUnavailable(format!("connect failed: {e}")))?;
        // W1-5：借用这次取连接的时机回收其它端点上已闲置的连接
        // （含 `watch_pools`）。清理不会影响刚建立的这条连接。
        self.maybe_sweep();
        Ok(self.wrap(channel))
    }
}

impl Clone for ConnectionPool {
    fn clone(&self) -> Self {
        Self {
            pools: Arc::clone(&self.pools),
            watch_pools: Arc::clone(&self.watch_pools),
            last_sweep: Arc::clone(&self.last_sweep),
            max_connections_per_endpoint: self.max_connections_per_endpoint,
            connect_timeout: self.connect_timeout,
            idle_timeout: self.idle_timeout,
            tls: self.tls.clone(),
            token_provider: Arc::clone(&self.token_provider),
        }
    }
}

// ──── Tests ────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_creation() {
        let config = Config::new(vec!["127.0.0.1:50051".to_string()]);
        let pool = ConnectionPool::new(&config);
        assert_eq!(pool.max_connections_per_endpoint, 2);
        assert_eq!(pool.connect_timeout, Duration::from_secs(3));
        assert_eq!(pool.idle_timeout, Duration::from_secs(300));
    }

    #[test]
    fn test_endpoint_pool_new() {
        let pool = EndpointPool::new(2);
        assert!(pool.channels.is_empty());
    }

    /// W1-5 的负控制：`maybe_sweep` 必须**节流**（窗口内不清理、不推进时间戳），
    /// 且窗口外必须触发一次清理并推进时间戳。没有这条，把 `cleanup_idle()` 接上
    /// 也可能退化成「每次取连接都全表扫描」。
    #[test]
    fn test_maybe_sweep_throttles_then_fires_when_due() {
        let config = Config::new(vec!["127.0.0.1:50051".to_string()]);
        let pool = ConnectionPool::new(&config);

        let t0 = *pool.last_sweep.lock();
        pool.maybe_sweep();
        let t1 = *pool.last_sweep.lock();
        assert_eq!(t0, t1, "清理窗口内不应触发（也不应推进时间戳）");

        // 把「上次清理」拨到远超窗口之前 → 这一次必须触发并推进。
        *pool.last_sweep.lock() = Instant::now()
            .checked_sub(Duration::from_secs(600))
            .expect("600s 在 Instant 范围内");
        pool.maybe_sweep();
        let t2 = *pool.last_sweep.lock();
        assert!(t2 > t1, "超出窗口后应触发清理并推进时间戳");
    }

    /// `idle_timeout = 0`（配置成“不回收”）时用 60s 作为最小节流窗口，
    /// 不能退化成每次取连接都扫一遍。
    #[test]
    fn test_maybe_sweep_zero_idle_timeout_uses_min_interval() {
        let mut config = Config::new(vec!["127.0.0.1:50051".to_string()]);
        config.connection_idle_timeout = Duration::ZERO;
        let pool = ConnectionPool::new(&config);

        let t0 = *pool.last_sweep.lock();
        pool.maybe_sweep();
        assert_eq!(*pool.last_sweep.lock(), t0, "0 超时仍应节流，不应每取必扫");
    }
}
