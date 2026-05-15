//! Client（gossip sidecar 代理）运行模式。
//!
//! 提供三项能力：
//! 1. AP 服务发现缓存（dashmap + TTL）
//! 2. Gossip 成员管理（chitchat UDP 协议）
//! 3. CP 操作透传（gRPC → coord-server leader）

use std::sync::Arc;

use coord_core::clock::SystemClock;
use coord_core::discovery_cache::DiscoveryCache;
use coord_core::gossip_types::{GossipAgent, GossipMember, GossipNodeRole};
use tracing::info;
use uuid::Uuid;

use crate::cli::ClientArgs;
use crate::client::agent::ClientAgent;
use crate::client::gossip::ChitchatGossipAgent;
use crate::client::proxy::ProxyClient;

/// Entry point for `coord client`.
pub(crate) async fn run(args: ClientArgs) -> anyhow::Result<()> {
    let node_id = args
        .node_id
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    info!(node_id = %node_id, gossip_addr = %args.gossip_addr, "starting coord client proxy");

    let gossip_advertise_addr = args
        .gossip_advertise_addr
        .as_deref()
        .unwrap_or(&args.gossip_addr)
        .to_string();

    let local_member = GossipMember {
        node_id: node_id.clone(),
        gossip_addr: gossip_advertise_addr,
        grpc_addr: args.local_grpc_addr.clone(),
        role: GossipNodeRole::Client,
        api_version: 1,
        generation: current_generation(),
    };

    // 启动 Gossip 代理
    let gossip_agent = ChitchatGossipAgent::start(
        local_member,
        args.cluster_id.clone(),
        args.gossip_seeds.clone(),
    )
    .await?;
    let gossip: Arc<dyn GossipAgent> = Arc::new(gossip_agent);

    // 发现缓存
    let cache_ttl_ms = (args.cache_ttl_seconds as i64) * 1000;
    let cache = Arc::new(DiscoveryCache::new(cache_ttl_ms, Arc::new(SystemClock)));

    // 透传客户端
    let proxy = Arc::new(ProxyClient::new(
        args.server_endpoints
            .iter()
            .map(|e| {
                if e.starts_with("http") {
                    e.clone()
                } else {
                    format!("http://{e}")
                }
            })
            .collect(),
    ));

    // 组合代理（当前仅持有引用；后续 gRPC 监听器接入时注入）
    let _agent = Arc::new(ClientAgent::new(
        gossip.clone(),
        cache.clone(),
        proxy,
        Arc::new(SystemClock),
    ));

    // 加入 Gossip 环
    let seed_addrs: Vec<std::net::SocketAddr> = args
        .gossip_seeds
        .iter()
        .filter_map(|s| s.parse().ok())
        .collect();
    gossip.join(&seed_addrs).await?;

    info!("coord client proxy running; press Ctrl-C to stop");
    tokio::signal::ctrl_c().await?;
    info!("shutting down");
    gossip.leave().await?;
    Ok(())
}

/// 返回以秒为单位的当前 UNIX 时间戳作为代际号（重启后递增）。
fn current_generation() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
