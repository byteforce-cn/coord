//! `coord all` 运行模式 — 单进程同时启动 CP 服务端 + AP Gossip 代理。
//!
//! 适用于开发环境和单机部署：
//! - 服务端行为与 `coord dev` 完全一致（自动 init、单节点、固定 root token）
//! - Gossip 代理自动对接本地服务端的 gRPC 地址
//!
//! 关闭信号（Ctrl-C / SIGTERM）：
//!
//! - 服务端通过自身的 `shutdown_signal` future 接收信号并持久化 snapshot
//! - Gossip 代理通过 `tokio::signal::ctrl_c()` 接收信号并执行 gossip leave
//!
//! 两者独立注册信号处理，并发等待，互不阻塞。

use std::sync::Arc;
use std::time::Duration;

use coord_core::clock::SystemClock;
use coord_core::discovery_cache::DiscoveryCache;
use coord_core::gossip_types::{GossipAgent, GossipMember, GossipNodeRole};
use tracing::info;
use uuid::Uuid;

use crate::cli::AllArgs;
use crate::client::agent::ClientAgent;
use crate::client::gossip::ChitchatGossipAgent;
use crate::client::proxy::ProxyClient;

/// Entry point for `coord all`.
pub(crate) async fn run(args: AllArgs) -> anyhow::Result<()> {
    // ── 1. 启动 CP 服务端（dev 模式，后台 task）────────────────────────────────
    let server_grpc_addr = args.server.grpc_addr.clone();
    let server_args = args.server.clone();
    let server_task = tokio::spawn(async move {
        if let Err(e) = crate::modes::server::run(server_args, true).await {
            tracing::error!(error = %e, "coord server exited with error");
        }
    });

    // 等待服务端完成端口绑定后再启动 Gossip（避免连接被拒绝）
    tokio::time::sleep(Duration::from_millis(600)).await;

    // ── 2. 启动 AP Gossip 代理─────────────────────────────────────────────────
    let node_id = Uuid::new_v4().to_string();
    info!(node_id = %node_id, gossip_port = args.gossip_port, "starting embedded gossip agent");

    let gossip_addr = format!("0.0.0.0:{}", args.gossip_port);
    let local_member = GossipMember {
        node_id: node_id.clone(),
        gossip_addr: gossip_addr.clone(),
        grpc_addr: server_grpc_addr.clone(),
        role: GossipNodeRole::Client,
        api_version: 1,
        generation: current_generation(),
    };

    let gossip_agent =
        ChitchatGossipAgent::start(local_member, args.cluster_id.clone(), vec![]).await?;
    let gossip: Arc<dyn GossipAgent> = Arc::new(gossip_agent);

    let cache_ttl_ms = (args.cache_ttl_seconds as i64) * 1000;
    let cache = Arc::new(DiscoveryCache::new(cache_ttl_ms, Arc::new(SystemClock)));

    // server_endpoints: 本地服务端 gRPC 地址（无 scheme 时补 http://）
    let endpoint = if server_grpc_addr.starts_with("http") {
        server_grpc_addr.clone()
    } else {
        format!("http://{server_grpc_addr}")
    };
    let proxy = Arc::new(ProxyClient::new(vec![endpoint]));

    let _agent = Arc::new(ClientAgent::new(
        gossip.clone(),
        cache,
        proxy,
        Arc::new(SystemClock),
    ));

    // 无种子节点（单机模式），Gossip 环只有自身
    gossip.join(&[]).await?;

    info!(
        grpc_addr = %server_grpc_addr,
        gossip_addr = %gossip_addr,
        "coord all: CP server + AP gossip agent running; press Ctrl-C to stop"
    );

    // ── 3. 等待关闭信号 ────────────────────────────────────────────────────────
    tokio::signal::ctrl_c().await?;
    info!("shutdown signal received");

    gossip.leave().await?;
    info!("gossip agent stopped");

    // 等待服务端任务结束（服务端自行处理 Ctrl-C，最多等 10 s）
    tokio::time::timeout(Duration::from_secs(10), server_task)
        .await
        .ok();

    Ok(())
}

/// 返回当前 UNIX 时间戳（秒）作为代际号。
fn current_generation() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
