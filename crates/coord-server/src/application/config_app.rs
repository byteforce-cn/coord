//! Application service: configuration management.
//!
//! [`ConfigApp`] encapsulates all business logic for the config namespace:
//! read, write (Raft-replicated), and live-change subscriptions.
//! It returns `coord_core` types only – no protobuf types cross this boundary.

use std::sync::Arc;

use coord_core::config::{ConfigCenter, ConfigEntry};
use coord_core::metrics::CoordMetrics;
use tokio::sync::broadcast;

use crate::raft_runtime::RaftRuntime;

/// A current-value + live-update subscription handle.
///
/// Returned by [`ConfigApp::subscribe`].  The caller owns the streaming
/// mechanics; this struct only carries the initial state and the channel.
pub struct ConfigSubscription {
    /// The current value at the time of subscription (if any).
    pub current: Option<ConfigEntry>,
    /// Receiver for all subsequent committed updates to this key.
    pub receiver: broadcast::Receiver<ConfigEntry>,
}

/// Application service for configuration management.
///
/// Centralises validation, metrics, and Raft proposal so that the gRPC
/// transport handler remains a thin translation layer.
#[derive(Clone)]
pub struct ConfigApp {
    config: Arc<ConfigCenter>,
    metrics: Arc<CoordMetrics>,
    raft: RaftRuntime,
}

impl ConfigApp {
    pub fn new(config: Arc<ConfigCenter>, metrics: Arc<CoordMetrics>, raft: RaftRuntime) -> Self {
        Self {
            config,
            metrics,
            raft,
        }
    }

    /// Expose the metrics for gauge management in the transport layer.
    pub fn metrics(&self) -> &Arc<CoordMetrics> {
        &self.metrics
    }

    /// Look up a configuration value by key.
    ///
    /// Increments the `coord_config_gets_total` counter.
    pub async fn get(&self, key: &str) -> Option<ConfigEntry> {
        self.metrics.coord_config_gets_total.inc();
        self.config.get(key).await
    }

    /// Write a configuration value (replicated via Raft).
    ///
    /// Returns the committed entry on success or a string error on failure.
    pub async fn put(&self, key: String, value: String) -> Result<ConfigEntry, String> {
        self.raft
            .propose_put_config(key, value)
            .await
            .map_err(|e| e.to_string())
    }

    /// Subscribe to live changes for a key.
    ///
    /// Returns the current value (if any) together with a broadcast receiver
    /// that will yield each future committed value.  The caller is responsible
    /// for managing the `coord_config_watches_active` gauge via
    /// `self.metrics.coord_config_watches_active`.
    pub async fn subscribe(&self, key: &str) -> ConfigSubscription {
        let receiver = self.config.watch(key).await;
        let current = self.config.get(key).await;
        ConfigSubscription { current, receiver }
    }
}

#[cfg(test)]
mod tests {
    // Unit tests for ConfigApp are exercised via integration tests.
    // Add focused unit tests here if the logic grows more complex.
}
