//! CLI 参数定义（P4D-03）。

use clap::Parser;

/// Coord 客户端代理。
///
/// 在业务节点本地运行，提供三项能力：
/// 1. AP 服务发现缓存（dashmap + TTL）
/// 2. Gossip 成员管理（chitchat UDP 协议）
/// 3. CP 操作透传（gRPC → coord-server leader）
#[derive(Debug, Parser)]
#[command(name = "coord-client", version)]
pub struct ClientArgs {
    /// 本节点唯一 ID（建议使用 hostname 或 UUID）。
    #[arg(long, env = "COORD_NODE_ID")]
    pub node_id: String,

    /// 本节点 Gossip UDP 监听地址（host:port）。
    #[arg(long, env = "COORD_GOSSIP_ADDR", default_value = "0.0.0.0:7947")]
    pub gossip_addr: String,

    /// 本节点对外广播的 Gossip 地址（通常为公网/主机 IP:port）。
    /// 不填时取 gossip_addr。
    #[arg(long, env = "COORD_GOSSIP_ADVERTISE_ADDR")]
    pub gossip_advertise_addr: Option<String>,

    /// gRPC 代理监听地址（供本机业务进程调用）。
    #[arg(long, env = "COORD_GRPC_ADDR", default_value = "0.0.0.0:9090")]
    pub grpc_addr: String,

    /// 初始 Gossip seed 节点列表（逗号分隔，host:port）。
    #[arg(long, env = "COORD_GOSSIP_SEEDS", value_delimiter = ',')]
    pub seeds: Vec<String>,

    /// CP coord-server gRPC 端点列表（逗号分隔）；用于透传注册/锁等 CP 操作。
    #[arg(long, env = "COORD_SERVER_ENDPOINTS", value_delimiter = ',')]
    pub server_endpoints: Vec<String>,

    /// 发现缓存 TTL（秒）。
    #[arg(long, env = "COORD_CACHE_TTL_SECONDS", default_value = "30")]
    pub cache_ttl_seconds: u64,

    /// 健康检查间隔（秒）。
    #[arg(long, env = "COORD_HEALTH_INTERVAL_SECONDS", default_value = "10")]
    pub health_interval_seconds: u64,

    /// Gossip 集群 ID（同一集群所有节点必须一致）。
    #[arg(long, env = "COORD_CLUSTER_ID", default_value = "coord-cluster")]
    pub cluster_id: String,
}
