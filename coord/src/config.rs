// Coord Server 配置解析
//
// 支持三种配置源，优先级从高到低（ADP §15.1）：
// 1. CLI 参数（--id, --addr 等）
// 2. 配置文件（TOML 格式，coord.toml）
// 3. 默认值

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Coord Server 完整配置
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
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

    /// P2-02：启动前配置校验（收集全部错误，一次报清）。
    ///
    /// 校验项：节点 ID；gRPC/Raft/HTTP 地址可解析且端口合法、互不冲突；
    /// TLS 证书/私钥成对配置且文件存在（缺配不再静默降级明文，规格 C.4.6 fail-closed）；
    /// 数据目录非空；watch_buffer/并发流上限合法；bootstrap 与 join 互斥；
    /// initial_nodes 成员合法；auth_root_key 为 64 位 hex；磁盘水位比例合法。
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errs: Vec<String> = Vec::new();

        // 1. 节点 ID
        if self.node.id == 0 {
            errs.push("node.id must be > 0 (0 保留给单节点/未初始化)".to_string());
        }

        // 2. gRPC 地址
        let grpc = self.resolve_grpc_addr();
        let grpc_port = validate_addr(&grpc, "network.grpc_addr", &mut errs);

        // 3. Raft 地址（与 gRPC 必须不同端口）
        let raft = self.resolve_raft_addr();
        let raft_port = validate_addr(&raft, "network.raft_addr", &mut errs);
        if grpc_port.is_some() && raft_port.is_some() && raft == grpc {
            errs.push(format!(
                "network.raft_addr ({raft}) must differ from network.grpc_addr ({grpc})"
            ));
        }

        // 4. HTTP 地址（配置非空时校验；服务端实际端口 = gRPC + 10）
        if !self.network.http_addr.is_empty() {
            let http_port = validate_addr(&self.network.http_addr, "network.http_addr", &mut errs);
            if let (Some(h), Some(g)) = (http_port, grpc_port) {
                if h == g {
                    errs.push("network.http_addr port must differ from grpc port".to_string());
                }
            }
            if let (Some(h), Some(r)) = (http_port, raft_port) {
                if h == r {
                    errs.push("network.http_addr port must differ from raft port".to_string());
                }
            }
        }

        // 5. TLS 成对 + 文件存在（fail-closed：不再静默降级明文）
        match (&self.security.tls_cert, &self.security.tls_key) {
            (Some(cert), Some(key)) => {
                for (label, path) in [("security.tls_cert", cert), ("security.tls_key", key)] {
                    if !path.exists() {
                        errs.push(format!("{label} file does not exist: {}", path.display()));
                    } else if !path.is_file() {
                        errs.push(format!("{label} is not a regular file: {}", path.display()));
                    }
                }
            }
            (Some(_), None) => errs.push(
                "security.tls_key missing: tls_cert and tls_key must be configured together \
                 (no silent plaintext downgrade)"
                    .to_string(),
            ),
            (None, Some(_)) => errs.push(
                "security.tls_cert missing: tls_cert and tls_key must be configured together \
                 (no silent plaintext downgrade)"
                    .to_string(),
            ),
            (None, None) => {}
        }
        if self.security.tls_ca.is_some() && self.security.tls_cert.is_none() {
            errs.push(
                "security.tls_ca configured without tls_cert/tls_key (mTLS requires server TLS)"
                    .to_string(),
            );
        }
        if let Some(ca) = &self.security.tls_ca {
            if !ca.exists() {
                errs.push(format!(
                    "security.tls_ca file does not exist: {}",
                    ca.display()
                ));
            }
        }

        // 6. 数据目录
        if self.storage.data_dir.as_os_str().is_empty() {
            errs.push("storage.data_dir must not be empty".to_string());
        }

        // 7. 网络防护参数
        if self.network.watch_buffer < 16 {
            errs.push(format!(
                "network.watch_buffer must be >= 16 (got {})",
                self.network.watch_buffer
            ));
        }
        if self.network.max_concurrent_streams == 0 {
            errs.push("network.max_concurrent_streams must be >= 1".to_string());
        }

        // 8. 集群模式互斥与 initial_nodes 成员合法性
        if self.cluster.bootstrap && self.cluster.join_addr.is_some() {
            errs.push(
                "cluster.bootstrap and cluster.join_addr are mutually exclusive \
                 (joining node must not bootstrap)"
                    .to_string(),
            );
        }
        let mut seen_ids = std::collections::HashSet::new();
        for (i, node) in self.cluster.initial_nodes.iter().enumerate() {
            if node.id == 0 {
                errs.push(format!("cluster.initial_nodes[{i}].id must be > 0"));
            }
            if !seen_ids.insert(node.id) {
                errs.push(format!(
                    "cluster.initial_nodes[{i}].id = {} duplicated",
                    node.id
                ));
            }
            if node.grpc.parse::<std::net::SocketAddr>().is_err() {
                errs.push(format!(
                    "cluster.initial_nodes[{i}].grpc ({}) is not a valid SocketAddr",
                    node.grpc
                ));
            }
            if node.raft.parse::<std::net::SocketAddr>().is_err() {
                errs.push(format!(
                    "cluster.initial_nodes[{i}].raft ({}) is not a valid SocketAddr",
                    node.raft
                ));
            }
        }

        // 9. auth_root_key 必须为 64 位 hex（32 字节，规格 C.4.2）
        if let Some(key) = &self.security.auth_root_key {
            if key.len() != 64 || !key.chars().all(|c| c.is_ascii_hexdigit()) {
                errs.push("security.auth_root_key must be 64 hex chars (32 bytes)".to_string());
            }
        }

        // 10. 磁盘水位比例（0 < readonly < warn < 1）
        let warn = self.storage.disk_warn_ratio;
        let readonly = self.storage.disk_readonly_ratio;
        if !(0.0..1.0).contains(&readonly) {
            errs.push(format!(
                "storage.disk_readonly_ratio must be in (0, 1) (got {readonly})"
            ));
        }
        if !(0.0..1.0).contains(&warn) {
            errs.push(format!(
                "storage.disk_warn_ratio must be in (0, 1) (got {warn})"
            ));
        }
        if readonly >= warn {
            errs.push(format!(
                "storage.disk_warn_ratio ({warn}) must be greater than \
                 storage.disk_readonly_ratio ({readonly})"
            ));
        }

        if errs.is_empty() {
            Ok(())
        } else {
            Err(errs)
        }
    }

    /// P2-02：SIGHUP 热更新安全子集快照（仅含运行时安全生效的字段）。
    pub fn reloadable(&self) -> ReloadableConfig {
        ReloadableConfig {
            watch_buffer: self.network.watch_buffer,
            disk_warn_ratio: self.storage.disk_warn_ratio,
            disk_readonly_ratio: self.storage.disk_readonly_ratio,
        }
    }
}

/// P2-02：SIGHUP 热更新安全子集。
///
/// 仅包含运行时安全生效的字段：磁盘水位阈值（下一监控周期生效）、
/// watch 缓冲（新订阅生效）。监听地址/TLS/集群拓扑等需重启才生效。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ReloadableConfig {
    pub watch_buffer: usize,
    pub disk_warn_ratio: f64,
    pub disk_readonly_ratio: f64,
}

/// 校验 `host:port` 地址：可解析为 SocketAddr 且端口合法。
/// 返回端口号（合法时）。
fn validate_addr(addr: &str, label: &str, errs: &mut Vec<String>) -> Option<u16> {
    match addr.parse::<std::net::SocketAddr>() {
        Ok(sa) => {
            if sa.port() == 0 {
                errs.push(format!("{label} port must not be 0 ({addr})"));
                None
            } else {
                Some(sa.port())
            }
        }
        Err(_) => {
            errs.push(format!("{label} ({addr}) is not a valid host:port address"));
            None
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

    /// 每 watcher 事件队列长度（P1-02：可配，默认 1024）
    #[serde(default = "default_watch_buffer")]
    pub watch_buffer: usize,

    /// gRPC 单连接并发流上限（P1-02：默认 512）
    #[serde(default = "default_max_concurrent_streams")]
    pub max_concurrent_streams: u32,
}

fn default_watch_buffer() -> usize {
    1024
}

fn default_max_concurrent_streams() -> u32 {
    512
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            grpc_addr: "127.0.0.1:50051".to_string(),
            raft_addr: String::new(),
            http_addr: String::new(),
            ui_enabled: false,
            watch_buffer: default_watch_buffer(),
            max_concurrent_streams: default_max_concurrent_streams(),
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

    /// P2-02：磁盘水位告警阈值（可用比例 < 该值 WARN，默认 0.15）
    #[serde(default = "default_disk_warn_ratio")]
    pub disk_warn_ratio: f64,

    /// P2-02：磁盘水位只读阈值（可用比例 < 该值置只读闸，默认 0.05）
    #[serde(default = "default_disk_readonly_ratio")]
    pub disk_readonly_ratio: f64,
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("/var/lib/coord")
}

fn default_disk_warn_ratio() -> f64 {
    0.15
}

fn default_disk_readonly_ratio() -> f64 {
    0.05
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: default_data_dir(),
            disk_warn_ratio: default_disk_warn_ratio(),
            disk_readonly_ratio: default_disk_readonly_ratio(),
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

    /// Auth 是否启用（默认 **true**：生产默认鉴权开启；
    /// `false` 仅限 `dev` 子命令或显式测试配置，P0-C.1 唯一开关）
    #[serde(default = "default_auth_enabled")]
    pub auth_enabled: bool,

    /// root 密码（server 模式强制；缺省时从 `COORD_ROOT_PASSWORD` 环境变量
    /// 或随机生成并仅打印一次，规格 C.4.8）
    #[serde(default)]
    pub root_password: Option<String>,

    /// Auth 根密钥（hex 编码 32 字节）。缺省时从 `<data_dir>/auth-root-key.bin`
    /// 加载或首次生成（HKDF 派生 CCT 签名密钥，规格 C.4.2）。
    /// 多节点集群必须共享同一根密钥。
    #[serde(default)]
    pub auth_root_key: Option<String>,

    /// gRPC reflection 开关（默认 **false**，生产关闭，规格 C.4.1）
    #[serde(default)]
    pub reflection_enabled: bool,
}

fn default_auth_enabled() -> bool {
    true
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            tls_cert: None,
            tls_key: None,
            tls_ca: None,
            auth_enabled: default_auth_enabled(),
            root_password: None,
            auth_root_key: None,
            reflection_enabled: false,
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
        // P0-C.1：默认配置启动即鉴权开启
        assert!(config.security.auth_enabled);
        assert!(!config.security.reflection_enabled);
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
        assert_eq!(
            config.cluster.join_addr,
            Some("192.168.1.1:50051".to_string())
        );
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

    // ──── P2-02 启动校验与热更新 ────

    #[test]
    fn test_validate_default_config_ok() {
        let config = Config::default();
        assert!(config.validate().is_ok(), "default config must be valid");
    }

    #[test]
    fn test_validate_rejects_cert_without_key() {
        let mut config = Config::default();
        config.security.tls_cert = Some(PathBuf::from("/etc/coord/server.crt"));
        let errs = config.validate().unwrap_err();
        assert!(
            errs.iter().any(|e| e.contains("tls")),
            "cert-only must be rejected: {errs:?}"
        );
    }

    #[test]
    fn test_validate_rejects_missing_tls_files() {
        let mut config = Config::default();
        config.security.tls_cert = Some(PathBuf::from("/nonexistent/cert.crt"));
        config.security.tls_key = Some(PathBuf::from("/nonexistent/key.pem"));
        let errs = config.validate().unwrap_err();
        assert!(
            errs.iter()
                .any(|e| e.contains("not exist") || e.contains("not found")),
            "missing TLS files must be rejected: {errs:?}"
        );
    }

    #[test]
    fn test_validate_rejects_invalid_grpc_port() {
        let mut config = Config::default();
        config.network.grpc_addr = "127.0.0.1:99999".to_string();
        let errs = config.validate().unwrap_err();
        assert!(
            errs.iter().any(|e| e.contains("grpc_addr")),
            "out-of-range port must be rejected: {errs:?}"
        );
    }

    #[test]
    fn test_validate_rejects_grpc_raft_same_addr() {
        let mut config = Config::default();
        config.network.grpc_addr = "0.0.0.0:50051".to_string();
        config.network.raft_addr = "0.0.0.0:50051".to_string();
        let errs = config.validate().unwrap_err();
        assert!(
            errs.iter().any(|e| e.contains("raft_addr")),
            "grpc/raft same addr must be rejected: {errs:?}"
        );
    }

    #[test]
    fn test_validate_rejects_bootstrap_with_join() {
        let mut config = Config::default();
        config.cluster.bootstrap = true;
        config.cluster.join_addr = Some("127.0.0.1:50051".to_string());
        let errs = config.validate().unwrap_err();
        assert!(
            errs.iter()
                .any(|e| e.contains("bootstrap") && e.contains("join")),
            "bootstrap+join conflict must be rejected: {errs:?}"
        );
    }

    #[test]
    fn test_validate_rejects_bad_auth_root_key() {
        let mut config = Config::default();
        config.security.auth_root_key = Some("xyz".to_string());
        let errs = config.validate().unwrap_err();
        assert!(
            errs.iter().any(|e| e.contains("auth_root_key")),
            "non-hex-64 auth_root_key must be rejected: {errs:?}"
        );
    }

    #[test]
    fn test_validate_rejects_bad_disk_ratios() {
        let mut config = Config::default();
        config.storage.disk_warn_ratio = 0.04;
        config.storage.disk_readonly_ratio = 0.10; // warn < readonly 非法
        let errs = config.validate().unwrap_err();
        assert!(
            errs.iter().any(|e| e.contains("disk_warn_ratio")),
            "warn<readonly must be rejected: {errs:?}"
        );
    }

    #[test]
    fn test_reloadable_snapshot() {
        let mut config = Config::default();
        config.storage.disk_warn_ratio = 0.25;
        config.storage.disk_readonly_ratio = 0.08;
        let r = config.reloadable();
        assert_eq!(r.watch_buffer, 1024);
        assert_eq!(r.disk_warn_ratio, 0.25);
        assert_eq!(r.disk_readonly_ratio, 0.08);
    }
}
