//! coord-client 主入口（P4D-07）。

mod agent;
mod cli;
mod gossip;
mod health;
mod proxy;

use std::sync::Arc;

use clap::Parser;
use cli::ClientArgs;
use coord_core::clock::SystemClock;
use coord_core::discovery_cache::DiscoveryCache;
use coord_core::gossip_types::{GossipAgent, GossipMember, GossipNodeRole};
use gossip::ChitchatGossipAgent;
use proxy::ProxyClient;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 初始化结构化日志
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("coord_client=info".parse()?),
        )
        .init();

    let args = ClientArgs::parse();
    info!(node_id = %args.node_id, gossip_addr = %args.gossip_addr, "starting coord-client");

    // Gossip 广播地址（未指定时取监听地址）
    let gossip_addr = args
        .gossip_advertise_addr
        .as_deref()
        .unwrap_or(&args.gossip_addr)
        .to_string();

    let local_member = GossipMember {
        node_id: args.node_id.clone(),
        gossip_addr,
        grpc_addr: args.grpc_addr.clone(),
        role: GossipNodeRole::Client,
        api_version: 1,
        generation: current_generation(),
    };

    // 启动 Gossip 代理
    let gossip_agent =
        ChitchatGossipAgent::start(local_member, args.cluster_id.clone(), args.seeds.clone())
            .await?;
    let gossip = Arc::new(gossip_agent);

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

    // 组合代理
    let _agent = Arc::new(agent::ClientAgent::new(
        gossip.clone(),
        cache.clone(),
        proxy,
        Arc::new(SystemClock),
    ));

    // 加入 Gossip 环
    let seed_addrs: Vec<std::net::SocketAddr> =
        args.seeds.iter().filter_map(|s| s.parse().ok()).collect();
    gossip.join(&seed_addrs).await?;

    info!("coord-client running; press Ctrl-C to stop");
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
