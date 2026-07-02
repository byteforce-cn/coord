// Connection Pool — gRPC channel pool management (ADP §10.3)
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
use tonic::transport::Channel;

use coord_core::error::{Error, Result};

use crate::config::Config;

// ──── Pool Entry ────

/// A single channel in the pool with idle tracking
#[derive(Clone)]
struct PoolChannel {
    channel: Channel,
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
    fn put(&mut self, channel: Channel) {
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
    /// Pool configuration
    max_connections_per_endpoint: usize,
    connect_timeout: Duration,
    idle_timeout: Duration,
}

impl ConnectionPool {
    /// Create a new connection pool from client config
    pub fn new(config: &Config) -> Self {
        Self {
            pools: Arc::new(RwLock::new(HashMap::new())),
            watch_pools: Arc::new(RwLock::new(HashMap::new())),
            max_connections_per_endpoint: config.connections_per_endpoint,
            connect_timeout: config.connect_timeout,
            idle_timeout: config.connection_idle_timeout,
        }
    }

    /// Get a regular channel for the given endpoint
    pub async fn get(&self, endpoint: &str) -> Result<Channel> {
        self.get_from(&self.pools, endpoint).await
    }

    /// Get a watch-dedicated channel for the given endpoint
    pub async fn get_watch(&self, endpoint: &str) -> Result<Channel> {
        self.get_from(&self.watch_pools, endpoint).await
    }

    /// Return a regular channel to the pool
    pub fn put(&self, endpoint: &str, channel: Channel) {
        let mut pools = self.pools.write();
        let pool = pools
            .entry(endpoint.to_string())
            .or_insert_with(|| EndpointPool::new(self.max_connections_per_endpoint));
        pool.put(channel);
    }

    /// Return a watch channel to the pool
    pub fn put_watch(&self, endpoint: &str, channel: Channel) {
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
        cleaned
    }

    // ──── Internal ────

    async fn get_from(
        &self,
        pools: &Arc<RwLock<HashMap<String, EndpointPool>>>,
        endpoint: &str,
    ) -> Result<Channel> {
        // Phase 1: Try to get an existing channel from the pool (under lock)
        {
            let mut pools_guard = pools.write();
            if let Some(pool) = pools_guard.get_mut(endpoint) {
                if let Some(channel) = pool.channels.pop() {
                    return Ok(channel.channel);
                }
            }
        }
        // Phase 2: No existing channel, create a new connection (lock released)
        let url = format!("http://{endpoint}");
        Channel::from_shared(url)
            .map_err(|e| Error::Internal(format!("invalid endpoint: {e}")))?
            .connect_timeout(self.connect_timeout)
            .connect()
            .await
            .map_err(|e| Error::ClusterUnavailable(format!("connect failed: {e}")))
    }
}

impl Clone for ConnectionPool {
    fn clone(&self) -> Self {
        Self {
            pools: Arc::clone(&self.pools),
            watch_pools: Arc::clone(&self.watch_pools),
            max_connections_per_endpoint: self.max_connections_per_endpoint,
            connect_timeout: self.connect_timeout,
            idle_timeout: self.idle_timeout,
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
        let mut pool = EndpointPool::new(2);
        assert!(pool.channels.is_empty());
    }
}
