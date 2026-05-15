//! P4D-05: `ChitchatGossipAgent` — 基于 chitchat 0.10.1 的 Gossip 成员发现实现。
//!
//! 使用 Scuttlebutt 协议同步节点状态，key 约定：
//! - `coord/role`              → "server" | "client"
//! - `coord/grpc_addr`         → "host:port"
//! - `coord/api_version`       → "1"
//! - `coord/svc/{svc}/{id}`    → JSON `ServiceDelta`
//! - `coord/health/{svc}/{id}` → JSON `HealthPayload`

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Context;
use chitchat::transport::UdpTransport;
use chitchat::{ChitchatConfig, ChitchatId, FailureDetectorConfig, spawn_chitchat};
use coord_core::gossip_types::{GossipAgent, GossipMember, GossipNodeRole, ServiceDelta};

/// 基于 chitchat 0.10.1 的 Gossip 代理实现。
pub struct ChitchatGossipAgent {
    handle: chitchat::ChitchatHandle,
    local: GossipMember,
}

impl ChitchatGossipAgent {
    /// 启动 Gossip 代理。
    ///
    /// - `local`：本节点元数据（`gossip_addr` 必须为合法 `host:port`）。
    /// - `cluster_id`：所有节点必须相同。
    /// - `seeds`：种子节点 UDP 地址列表。
    pub async fn start(
        local: GossipMember,
        cluster_id: impl Into<String>,
        seeds: Vec<String>,
    ) -> anyhow::Result<Self> {
        let listen_addr: SocketAddr = local
            .gossip_addr
            .parse()
            .with_context(|| format!("invalid gossip_addr: {}", local.gossip_addr))?;
        let advertise_addr: SocketAddr = listen_addr; // 同地址广播

        let chitchat_id = ChitchatId::new(local.node_id.clone(), local.generation, advertise_addr);

        let seed_nodes: Vec<String> = seeds;

        let config = ChitchatConfig {
            chitchat_id,
            cluster_id: cluster_id.into(),
            gossip_interval: Duration::from_millis(500),
            listen_addr,
            seed_nodes,
            failure_detector_config: FailureDetectorConfig::default(),
            marked_for_deletion_grace_period: Duration::from_secs(60),
            catchup_callback: None,
            extra_liveness_predicate: None,
        };

        let role_str = match local.role {
            GossipNodeRole::Server => "server",
            GossipNodeRole::Client => "client",
        };
        let initial_kvs: Vec<(String, String)> = vec![
            ("coord/role".to_string(), role_str.to_string()),
            ("coord/grpc_addr".to_string(), local.grpc_addr.clone()),
            (
                "coord/api_version".to_string(),
                local.api_version.to_string(),
            ),
        ];

        let transport = UdpTransport;
        let handle = spawn_chitchat(config, initial_kvs, &transport)
            .await
            .context("failed to spawn chitchat")?;

        Ok(Self { handle, local })
    }

    // ─── 内部辅助 ─────────────────────────────────────────────────────────────

    /// 将 chitchat `NodeState` 中的节点元数据解析为 `GossipMember`。
    fn node_state_to_member(id: &ChitchatId, state: &chitchat::NodeState) -> Option<GossipMember> {
        let role = match state.get("coord/role")? {
            "server" => GossipNodeRole::Server,
            "client" => GossipNodeRole::Client,
            _ => GossipNodeRole::Client,
        };
        let grpc_addr = state.get("coord/grpc_addr").unwrap_or("").to_string();
        let api_version: u32 = state
            .get("coord/api_version")
            .and_then(|s| s.parse().ok())
            .unwrap_or(1);
        Some(GossipMember {
            node_id: id.node_id.clone(),
            gossip_addr: id.gossip_advertise_addr.to_string(),
            grpc_addr,
            role,
            api_version,
            generation: id.generation_id,
        })
    }
}

#[async_trait::async_trait]
impl GossipAgent for ChitchatGossipAgent {
    async fn join(&self, seeds: &[SocketAddr]) -> anyhow::Result<()> {
        for &addr in seeds {
            self.handle
                .gossip(addr)
                .with_context(|| format!("gossip handshake with {addr} failed"))?;
        }
        Ok(())
    }

    async fn leave(&self) -> anyhow::Result<()> {
        // chitchat 没有显式 leave；停止 handle 即可（超时后被其他节点标记 dead）
        Ok(())
    }

    fn local_member(&self) -> GossipMember {
        self.local.clone()
    }

    async fn members(&self) -> Vec<GossipMember> {
        self.handle
            .with_chitchat(|cc| {
                cc.live_nodes()
                    .filter_map(|id| {
                        cc.node_state(id)
                            .and_then(|s| Self::node_state_to_member(id, s))
                    })
                    .collect()
            })
            .await
    }

    async fn server_members(&self) -> Vec<GossipMember> {
        self.members()
            .await
            .into_iter()
            .filter(|m| m.role == GossipNodeRole::Server)
            .collect()
    }

    async fn put_service_delta(&self, delta: ServiceDelta) -> anyhow::Result<()> {
        let key = delta.chitchat_key();
        let value = serde_json::to_string(&delta)?;
        self.handle
            .with_chitchat(|cc| {
                cc.self_node_state().set(key.clone(), value.clone());
            })
            .await;
        Ok(())
    }

    async fn remove_service_delta(
        &self,
        service_name: &str,
        instance_id: &str,
    ) -> anyhow::Result<()> {
        let key = format!("coord/svc/{service_name}/{instance_id}");
        self.handle
            .with_chitchat(|cc| {
                cc.self_node_state().delete(&key);
            })
            .await;
        Ok(())
    }

    async fn service_deltas(&self, service_name: &str) -> Vec<ServiceDelta> {
        let prefix = format!("coord/svc/{service_name}/");
        self.handle
            .with_chitchat(|cc| {
                let mut result = Vec::new();
                for id in cc.live_nodes() {
                    if let Some(state) = cc.node_state(id) {
                        for (_, versioned) in state.iter_prefix(&prefix) {
                            if let Ok(delta) =
                                serde_json::from_str::<ServiceDelta>(&versioned.value)
                            {
                                result.push(delta);
                            }
                        }
                    }
                }
                result
            })
            .await
    }
}
