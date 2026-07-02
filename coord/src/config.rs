// Coord Server 配置解析
//
// 支持三种配置源，优先级从高到低（ADP §15.1）：
// 1. CLI 参数（--id, --addr 等）
// 2. 配置文件（TOML 格式，coord.toml）
// 3. 默认值

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Coord Server 完整配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// 节点信息
    #[serde(default)]
    pub node: NodeConfig,

    /// 网络配置
    #[serde(default)]
    pub network: NetworkConfig,

    /// 集群配置
    #[serde(default)]
    pub cluster: ClusterConfig,

    /// 存储配置
    #[serde(default)]
    pub storage: StorageConfig,

    /// 安全配置
    #[serde(default)]
    pub security: SecurityConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            node: NodeConfig::default(),
            network: NetworkConfig::default(),
            cluster: ClusterConfig::default(),
            storage: StorageConfig::default(),
            security: SecurityConfig::default(),
        }
    }
}

impl Config {
    /// 从 TOML 文件加载配置
    pub fn from_file(path: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&content)?;
        Ok(config)
    }

    /// 将 CLI 参数覆盖到配置（CLI 优先级高于配置文件）
    pub fn apply_cli_overrides(
        &mut self,
        id: Option<u64>,
        grpc_addr: Option<&str>,
        raft_addr: Option<&str>,
        data_dir: Option<&PathBuf>,
        cluster_name: Option<&str>,
        join: Option<&str>,
    ) {
        if let Some(id) = id {
            self.node.id = id;
        }
        if let Some(addr) = grpc_addr {
            self.network.grpc_addr = addr.to_string();
        }
        if let Some(addr) = raft_addr {
            self.network.raft_addr = addr.to_string();
        }
        if let Some(dir) = data_dir {
            self.storage.data_dir = dir.clone();
        }
        if let Some(name) = cluster_name {
            self.cluster.cluster_name = name.to_string();
        }
        if let Some(join_addr) = join {
            self.cluster.join_addr = Some(join_addr.to_string());
        }
    }

    /// 解析数据目录（CLI > Config > 默认值）
    pub fn resolve_data_dir(&self) -> PathBuf {
        self.storage.data_dir.clone()
    }

    /// 解析 gRPC 地址
    pub fn resolve_grpc_addr(&self) -> String {
        if self.network.grpc_addr.is_empty() {
            "127.0.0.1:50051".to_string()
        } else {
            self.network.grpc_addr.clone()
        }
    }

    /// 解析 Raft 内部通信地址（默认与 gRPC 端口 +1）
    pub fn resolve_raft_addr(&self) -> String {
        if self.network.raft_addr.is_empty() {
            let grpc = self.resolve_grpc_addr();
            let base = grpc.rsplit(':').next().unwrap_or("50051");
            let port: u16 = base.parse().unwrap_or(50051);
            grpc.replace(&port.to_string(), &(port + 1).to_string())
        } else {
            self.network.raft_addr.clone()
        }
    }
}

// ──── 子配置 ────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// 节点 ID
    #[serde(default = "default_node_id")]
    pub id: u64,

    /// 节点名称
    #[serde(default = "default_node_name")]
    pub name: String,
}

fn default_node_id() -> u64 {
    1
}

fn default_node_name() -> String {
    "coord-01".to_string()
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            id: default_node_id(),
            name: default_node_name(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// gRPC 监听地址
    #[serde(default)]
    pub grpc_addr: String,

    /// Raft 内部通信地址（默认与 gRPC 端口 +1）
    #[serde(default)]
    pub raft_addr: String,

    /// HTTP 健康检查/BFF 监听地址（默认与 gRPC 端口 +10）
    #[serde(default)]
    pub http_addr: String,

    /// 是否启用 UI 控制台（BFF + 静态资源）
    #[serde(default)]
    pub ui_enabled: bool,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            grpc_addr: "127.0.0.1:50051".to_string(),
            raft_addr: String::new(),
            http_addr: String::new(),
            ui_enabled: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// 集群名称
    #[serde(default = "default_cluster_name")]
    pub cluster_name: String,

    /// 加入已有集群的 Leader 地址
    #[serde(default)]
    pub join_addr: Option<String>,

    /// 初始集群节点列表（bootstrap 时使用）
    #[serde(default)]
    pub initial_nodes: Vec<ClusterNode>,

    /// Bootstrap 模式
    #[serde(default)]
    pub bootstrap: bool,
}

fn default_cluster_name() -> String {
    "coord-cluster".to_string()
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            cluster_name: default_cluster_name(),
            join_addr: None,
            initial_nodes: Vec::new(),
            bootstrap: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNode {
    pub id: u64,
    pub grpc: String,
    pub raft: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// 数据目录路径
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("/var/lib/coord")
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: default_data_dir(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    /// TLS 服务端证书路径
    #[serde(default)]
    pub tls_cert: Option<PathBuf>,

    /// TLS 服务端私钥路径
    #[serde(default)]
    pub tls_key: Option<PathBuf>,

    /// TLS CA 证书路径（mTLS 双向验证）
    #[serde(default)]
    pub tls_ca: Option<PathBuf>,

    /// Auth 是否启用
    #[serde(default)]
    pub auth_enabled: bool,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            tls_cert: None,
            tls_key: None,
            tls_ca: None,
            auth_enabled: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.node.id, 1);
        assert_eq!(config.node.name, "coord-01");
        assert_eq!(config.network.grpc_addr, "127.0.0.1:50051");
        assert_eq!(config.cluster.cluster_name, "coord-cluster");
        assert_eq!(config.storage.data_dir, PathBuf::from("/var/lib/coord"));
        assert!(!config.cluster.bootstrap);
        assert!(config.security.tls_cert.is_none());
    }

    #[test]
    fn test_apply_cli_overrides() {
        let mut config = Config::default();
        config.apply_cli_overrides(
            Some(42),
            Some("0.0.0.0:9999"),
            Some("0.0.0.0:10000"),
            Some(&PathBuf::from("/tmp/coord")),
            Some("test-cluster"),
            Some("192.168.1.1:50051"),
        );
        assert_eq!(config.node.id, 42);
        assert_eq!(config.network.grpc_addr, "0.0.0.0:9999");
        assert_eq!(config.network.raft_addr, "0.0.0.0:10000");
        assert_eq!(config.storage.data_dir, PathBuf::from("/tmp/coord"));
        assert_eq!(config.cluster.cluster_name, "test-cluster");
        assert_eq!(config.cluster.join_addr, Some("192.168.1.1:50051".to_string()));
    }

    #[test]
    fn test_parse_toml_config() {
        let toml_str = r#"
[node]
id = 3
name = "coord-03"

[network]
grpc_addr = "0.0.0.0:50071"
raft_addr = "0.0.0.0:50072"

[cluster]
cluster_name = "prod-cluster"
bootstrap = true

[storage]
data_dir = "/data/coord"

[security]
tls_cert = "/etc/coord/server.crt"
tls_key = "/etc/coord/server.key"
tls_ca = "/etc/coord/ca.crt"
auth_enabled = true
"#;

        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.node.id, 3);
        assert_eq!(config.node.name, "coord-03");
        assert_eq!(config.network.grpc_addr, "0.0.0.0:50071");
        assert_eq!(config.network.raft_addr, "0.0.0.0:50072");
        assert_eq!(config.cluster.cluster_name, "prod-cluster");
        assert!(config.cluster.bootstrap);
        assert_eq!(config.storage.data_dir, PathBuf::from("/data/coord"));
        assert!(config.security.auth_enabled);
    }

    #[test]
    fn test_resolve_raft_addr_default() {
        let config = Config::default();
        let raft = config.resolve_raft_addr();
        assert_eq!(raft, "127.0.0.1:50052");
    }

    #[test]
    fn test_resolve_raft_addr_explicit() {
        let mut config = Config::default();
        config.network.raft_addr = "0.0.0.0:9999".to_string();
        assert_eq!(config.resolve_raft_addr(), "0.0.0.0:9999");
    }
}
