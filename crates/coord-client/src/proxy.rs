#![allow(dead_code)] // P4D-05 代理模块尚未接入主流程，待后续 sprint 完成后移除
//! P4D-05: `ProxyClient` — 透传 CP 操作到 coord-server。
//!
//! 在 gossip 环中发现 coord-server gRPC 端点，随机轮询（或后续实现 leader sticky）。

use anyhow::Context;
use tonic::transport::Channel;

/// gRPC 透传客户端，使用 `tonic::transport::Channel` 连接 coord-server。
pub struct ProxyClient {
    endpoints: Vec<String>,
}

impl ProxyClient {
    /// 创建代理客户端。`endpoints` 为 `http://host:port` 格式的 gRPC 端点列表。
    pub fn new(endpoints: Vec<String>) -> Self {
        Self { endpoints }
    }

    /// 获取可用的 gRPC Channel（轮询 endpoints）。
    ///
    /// 目前选取第一个可连接的端点；后续可扩展为 leader-sticky 路由。
    pub async fn connect(&self) -> anyhow::Result<Channel> {
        for ep in &self.endpoints {
            match tonic::transport::Endpoint::from_shared(ep.clone())
                .context("invalid endpoint")?
                .connect()
                .await
            {
                Ok(ch) => return Ok(ch),
                Err(e) => {
                    tracing::warn!(endpoint = %ep, error = %e, "failed to connect");
                }
            }
        }
        anyhow::bail!("no reachable coord-server endpoints: {:?}", self.endpoints)
    }
}

// ─── 测试 ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::ProxyClient;

    #[test]
    fn new_stores_endpoints() {
        let client = ProxyClient::new(vec![
            "http://127.0.0.1:8080".to_string(),
            "http://127.0.0.1:8081".to_string(),
        ]);
        assert_eq!(client.endpoints.len(), 2);
    }

    #[tokio::test]
    async fn connect_fails_if_no_endpoint() {
        let client = ProxyClient::new(vec![]);
        let result = client.connect().await;
        assert!(result.is_err());
    }
}
