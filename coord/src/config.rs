// Coord Server 配置解析
//
// 支持三种配置源，优先级从高到低：
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

    /// R-RFT-19：Raft 运行时调优（心跳/选举/快照策略；空字段 = openraft 默认）
    #[serde(default)]
    pub raft: RaftTuningConfig,

    /// R-SVC-18：运行时资源限制（per-RPC 超时、规模上限、幂等缓存参数）
    #[serde(default)]
    pub limits: LimitsConfig,

    /// Multi-Raft 配置（`[multi_raft]` 段；兼容开关 + 初始 Region 表）
    #[serde(default)]
    pub multi_raft: MultiRaftConfig,

    /// 对象存储配置（`[object_storage]` 段；默认关闭，与 `[multi_raft]` 正交）
    #[serde(default)]
    pub object_storage: ObjectStorageConfig,
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

    /// R-TST-16：Raft 实际监听地址（raft_bind_addr 非空时使用，否则 = raft_addr）。
    pub fn resolve_raft_bind_addr(&self) -> String {
        if self.network.raft_bind_addr.is_empty() {
            self.resolve_raft_addr()
        } else {
            self.network.raft_bind_addr.clone()
        }
    }

    /// 启动前配置校验（收集全部错误，一次报清）。
    ///
    /// 校验项：节点 ID；gRPC/Raft/HTTP 地址可解析且端口合法、互不冲突；
    /// TLS 证书/私钥成对配置且文件存在（缺配不再静默降级明文，fail-closed）；
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

        // 3b. R-TST-16：raft_bind_addr 非空时须可解析，且不与 grpc 冲突
        if !self.network.raft_bind_addr.is_empty() {
            let bind_port = validate_addr(
                &self.network.raft_bind_addr,
                "network.raft_bind_addr",
                &mut errs,
            );
            if let (Some(b), Some(g)) = (bind_port, grpc_port) {
                if b == g {
                    errs.push("network.raft_bind_addr port must differ from grpc port".to_string());
                }
            }
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

        // 9. auth_root_key 必须为 64 位 hex（32 字节）
        if let Some(key) = &self.security.auth_root_key {
            if key.len() != 64 || !key.chars().all(|c| c.is_ascii_hexdigit()) {
                errs.push("security.auth_root_key must be 64 hex chars (32 bytes)".to_string());
            }
        }

        // 9b. raft 共享密钥最短长度（HMAC 密钥强度下限）
        if let Some(secret) = &self.security.raft_shared_secret {
            if secret.len() < 16 {
                errs.push("security.raft_shared_secret must be at least 16 characters".to_string());
            }
        }

        // 9c. 拒绝已知占位密钥/密码（示例配置默认值不得通过校验，
        //     防止照抄示例上线：全零 root key 等价于公开密钥）。
        for (label, key) in [
            (
                "security.auth_root_key",
                self.security.auth_root_key.as_deref(),
            ),
            (
                "security.encryption_root_key",
                self.security.encryption_root_key.as_deref(),
            ),
        ] {
            if let Some(key) = key {
                if key.len() == 64 && key.bytes().all(|b| b == b'0') {
                    errs.push(format!(
                        "{label} is the all-zeros placeholder; generate a real key \
                         (e.g. `openssl rand -hex 32`)"
                    ));
                }
            }
        }
        if let Some(pw) = &self.security.root_password {
            if is_placeholder_secret(pw) {
                errs.push(
                    "security.root_password must not be a placeholder value \
                     (e.g. CHANGE_ME_STRONG_PW)"
                        .to_string(),
                );
            }
        }
        if let Some(secret) = &self.security.raft_shared_secret {
            if is_placeholder_secret(secret) {
                errs.push(
                    "security.raft_shared_secret must not be a placeholder value \
                     (e.g. change-me-raft-secret-16chars)"
                        .to_string(),
                );
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

        // 11. R-SVC-18：limits 段合法性（超时/规模/幂等参数）
        for (label, ms) in [
            ("limits.read_timeout_ms", self.limits.read_timeout_ms),
            ("limits.write_timeout_ms", self.limits.write_timeout_ms),
            ("limits.lease_timeout_ms", self.limits.lease_timeout_ms),
            ("limits.compact_timeout_ms", self.limits.compact_timeout_ms),
        ] {
            if ms == 0 {
                errs.push(format!("{label} must be > 0"));
            }
        }
        if self.limits.idempotency_ttl_secs == 0 {
            errs.push("limits.idempotency_ttl_secs must be > 0".to_string());
        }
        if self.limits.idempotency_max_entries < 16 {
            errs.push(format!(
                "limits.idempotency_max_entries must be >= 16 (got {})",
                self.limits.idempotency_max_entries
            ));
        }

        // 12. multi_raft 段合法性（enabled=true 时 initial_regions 须
        //    平铺整个 keyspace：首 region start_key 为空、相邻首尾相接无空洞/重叠、
        //    末 region end_key 为空无上界；region id 唯一且 > 0，0 保留给单
        //    Raft/system raft；v1 仅支持静态成员——本节点必须在 cluster.initial_nodes
        //    且不得走 join 流程）。
        if self.multi_raft.enabled {
            let regions = &self.multi_raft.initial_regions;
            if regions.is_empty() {
                errs.push(
                    "multi_raft.enabled = true requires initial_regions (at least one region)"
                        .to_string(),
                );
            }
            let mut seen_region_ids = std::collections::HashSet::new();
            for (i, r) in regions.iter().enumerate() {
                if r.id == 0 {
                    errs.push(format!(
                        "multi_raft.initial_regions[{i}].id must be > 0 \
                         (0 is reserved for the single-raft/system raft)"
                    ));
                }
                if !seen_region_ids.insert(r.id) {
                    errs.push(format!(
                        "multi_raft.initial_regions[{i}].id = {} duplicated",
                        r.id
                    ));
                }
            }
            // key range 平铺：按 start_key 排序后首尾相接；无空洞/重叠
            let mut sorted = regions.clone();
            sorted.sort_by(|a, b| a.start_key.cmp(&b.start_key));
            if let Some(first) = sorted.first() {
                if !first.start_key.is_empty() {
                    errs.push(format!(
                        "multi_raft.initial_regions must tile the whole keyspace: \
                         first region (id={}) start_key must be empty",
                        first.id
                    ));
                }
            }
            for w in sorted.windows(2) {
                if w[0].end_key != w[1].start_key {
                    errs.push(format!(
                        "multi_raft.initial_regions key ranges must be contiguous \
                         with no gap/overlap: region {} end_key ({:?}) != region {} start_key ({:?})",
                        w[0].id, w[0].end_key, w[1].id, w[1].start_key
                    ));
                }
            }
            if let Some(last) = sorted.last() {
                if !last.end_key.is_empty() {
                    errs.push(format!(
                        "multi_raft.initial_regions must tile the whole keyspace: \
                         last region (id={}) end_key must be empty (unbounded)",
                        last.id
                    ));
                }
            }
            // v1 静态装配约束：region 副本置于 cluster.initial_nodes；join 模式不支持
            if self.cluster.join_addr.is_some() {
                errs.push(
                    "multi_raft.enabled = true with cluster.join_addr is not supported in v1 \
                     (multi-region requires static membership in cluster.initial_nodes)"
                        .to_string(),
                );
            }
            let member_ids: std::collections::HashSet<u64> =
                self.cluster.initial_nodes.iter().map(|n| n.id).collect();
            if !member_ids.contains(&self.node.id) {
                errs.push(format!(
                    "multi_raft.enabled = true requires this node (node.id = {}) to be listed \
                     in cluster.initial_nodes (region replicas are placed on cluster members)",
                    self.node.id
                ));
            }
        }

        // 12b. multi_raft.pd 段合法性（PD 内嵌模式 v1）。
        //     - pd.enabled=true 要求 multi_raft.enabled=true（PD 附着于多 Region 装配）；
        //     - 各间隔/超时 > 0；max_concurrent_operators > 0；
        //     - target_replicas ∈ [1, cluster 成员数]（v1 静态成员下不能多于可用节点）。
        let pd = &self.multi_raft.pd;
        if pd.enabled && !self.multi_raft.enabled {
            errs.push(
                "multi_raft.pd.enabled = true requires multi_raft.enabled = true \
                 (PD schedules the regions assembled by [multi_raft])"
                    .to_string(),
            );
        }
        if pd.enabled {
            if pd.heartbeat_interval_ms == 0 {
                errs.push("multi_raft.pd.heartbeat_interval_ms must be > 0".to_string());
            }
            for (label, v) in [
                ("split_check_interval", pd.split_check_interval),
                ("merge_check_interval", pd.merge_check_interval),
                ("balance_interval", pd.balance_interval),
                ("node_heartbeat_timeout", pd.node_heartbeat_timeout),
                ("operator_running_timeout", pd.operator_running_timeout),
            ] {
                if v == 0 {
                    errs.push(format!("multi_raft.pd.{label} must be > 0"));
                }
            }
            if pd.max_concurrent_operators == 0 {
                errs.push(
                    "multi_raft.pd.max_concurrent_operators must be > 0".to_string(),
                );
            }
            if pd.region_split_size_mb == 0 {
                errs.push("multi_raft.pd.region_split_size_mb must be > 0".to_string());
            }
            if pd.region_split_keys == 0 {
                errs.push("multi_raft.pd.region_split_keys must be > 0".to_string());
            }
            if pd.region_merge_size_mb == 0 {
                errs.push("multi_raft.pd.region_merge_size_mb must be > 0".to_string());
            }
            if pd.target_replicas == 0 {
                errs.push("multi_raft.pd.target_replicas must be > 0".to_string());
            }
            if pd.target_replicas > self.cluster.initial_nodes.len() {
                errs.push(format!(
                    "multi_raft.pd.target_replicas ({}) cannot exceed the cluster member \
                     count ({}) in v1 (static membership; replicas are placed on \
                     cluster.initial_nodes)",
                    pd.target_replicas,
                    self.cluster.initial_nodes.len()
                ));
            }
        }

        // 12c. object_storage 段合法性。
        //     - chunk_size ∈ (0, 4MiB]（对齐 gRPC MAX_DECODING_MSG）；单对象 ≥ chunk；
        //     - 超时/间隔 > 0；
        //     - encryption_enabled=true 必须提供 hex64 根密钥（配置或环境变量），
        //       encryption_root_key 非空但开关关闭 → 拒绝（防止静默明文落盘）；
        //     - 与 multi_raft.legacy_migration 互斥（迁移会把 /obj/ manifest 导入
        //       Region DB，而 chunk 文件仍在根对象目录，两者失配）。
        let os = &self.object_storage;
        if os.enabled {
            const MAX_CHUNK: usize = 4 * 1024 * 1024;
            if os.chunk_size_bytes == 0 || os.chunk_size_bytes > MAX_CHUNK {
                errs.push(format!(
                    "object_storage.chunk_size_bytes must be in (0, {}] (got {})",
                    MAX_CHUNK, os.chunk_size_bytes
                ));
            }
            if os.max_object_size_bytes == 0
                || os.max_object_size_bytes < os.chunk_size_bytes as u64
            {
                errs.push(format!(
                    "object_storage.max_object_size_bytes must be >= chunk_size_bytes \
                     (got {})",
                    os.max_object_size_bytes
                ));
            }
            if os.upload_timeout_secs == 0 {
                errs.push("object_storage.upload_timeout_secs must be > 0".to_string());
            }
            if os.gc_interval_secs == 0 {
                errs.push("object_storage.gc_interval_secs must be > 0".to_string());
            }
            if os.encryption_enabled && os.encryption_root_key.trim().is_empty() {
                errs.push(
                    "object_storage.encryption_enabled = true requires \
                     object_storage.encryption_root_key (hex64) or \
                     COORD_OBJECT_STORAGE_ENCRYPTION_ROOT_KEY"
                        .to_string(),
                );
            }
            if !os.encryption_enabled && !os.encryption_root_key.trim().is_empty() {
                errs.push(
                    "object_storage.encryption_root_key set but \
                     encryption_enabled = false (misconfigured; refusing plaintext "
                        .to_string()
                        + "surprise)",
                );
            }
            if os.encryption_enabled && os.encryption_root_key.trim().len() != 64 {
                errs.push(
                    "object_storage.encryption_root_key must be hex64 (32 bytes)"
                        .to_string(),
                );
            }
            if self.multi_raft.legacy_migration {
                errs.push(
                    "object_storage.enabled with multi_raft.legacy_migration is not \
                     supported: migration would import /obj/ manifests into region DBs "
                        .to_string()
                        + "while chunk files remain under the root data dir",
                );
            }
        }

        // 13. R-RFT-19：raft 调优段合法性（选举窗口 min ≤ max；时间参数 > 0）
        for (label, ms) in [
            (
                "raft.heartbeat_interval_ms",
                self.raft.heartbeat_interval_ms,
            ),
            (
                "raft.election_timeout_min_ms",
                self.raft.election_timeout_min_ms,
            ),
            (
                "raft.election_timeout_max_ms",
                self.raft.election_timeout_max_ms,
            ),
            (
                "raft.install_snapshot_timeout_ms",
                self.raft.install_snapshot_timeout_ms,
            ),
        ] {
            if ms == Some(0) {
                errs.push(format!("{label} must be > 0"));
            }
        }
        match (
            self.raft.election_timeout_min_ms,
            self.raft.election_timeout_max_ms,
        ) {
            (Some(min), Some(max)) if min > max => errs.push(format!(
                "raft.election_timeout_min_ms ({min}) must be <= raft.election_timeout_max_ms ({max})"
            )),
            _ => {}
        }

        if errs.is_empty() {
            Ok(())
        } else {
            Err(errs)
        }
    }

    /// SIGHUP 热更新安全子集快照（仅含运行时安全生效的字段）。
    pub fn reloadable(&self) -> ReloadableConfig {
        ReloadableConfig {
            watch_buffer: self.network.watch_buffer,
            disk_warn_ratio: self.storage.disk_warn_ratio,
            disk_readonly_ratio: self.storage.disk_readonly_ratio,
        }
    }
}

/// SIGHUP 热更新安全子集。
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
/// 是否为占位密钥（示例模板 change-me / __FILL__ 类默认值）。
fn is_placeholder_secret(s: &str) -> bool {
    let s = s.trim().to_ascii_lowercase();
    s.contains("change-me") || s.contains("change_me") || s == "changeme" || s.contains("__fill")
}

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

    /// Raft 监听地址（可选；默认 = raft_addr）。
    ///
    /// R-TST-16 支撑：bind/advertise 分离（如经 TCP 代理注入分区时，
    /// 监听真实端口、对外通告代理端口）；生产一般无需配置。
    #[serde(default)]
    pub raft_bind_addr: String,

    /// HTTP 健康检查/BFF 监听地址（默认与 gRPC 端口 +10）
    #[serde(default)]
    pub http_addr: String,

    /// 是否启用 UI 控制台（BFF + 静态资源）
    #[serde(default)]
    pub ui_enabled: bool,

    /// 每 watcher 事件队列长度（可配，默认 1024）
    #[serde(default = "default_watch_buffer")]
    pub watch_buffer: usize,

    /// gRPC 单连接并发流上限（默认 512）
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
            raft_bind_addr: String::new(),
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

    /// 磁盘水位告警阈值（可用比例 < 该值 WARN，默认 0.15）
    #[serde(default = "default_disk_warn_ratio")]
    pub disk_warn_ratio: f64,

    /// 磁盘水位只读阈值（可用比例 < 该值置只读闸，默认 0.05）
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
    /// `false` 仅限 `dev` 子命令或显式测试配置，唯一开关）
    #[serde(default = "default_auth_enabled")]
    pub auth_enabled: bool,

    /// root 密码（server 模式强制；缺省时从 `COORD_ROOT_PASSWORD` 环境变量
    /// 或随机生成并仅打印一次）
    #[serde(default)]
    pub root_password: Option<String>,

    /// Auth 根密钥（hex 编码 32 字节）。缺省时从 `<data_dir>/auth-root-key.bin`
    /// 加载或首次生成（HKDF 派生 CCT 签名密钥）。
    /// 多节点集群必须共享同一根密钥。
    #[serde(default)]
    pub auth_root_key: Option<String>,

    /// gRPC reflection 开关（默认 **false**，生产关闭）
    #[serde(default)]
    pub reflection_enabled: bool,

    /// 静态加密开关（默认 **false**）。开启后 `/kv/` 用户数据
    /// 经 AES-256-GCM Barrier 加密落盘，Seal/Unseal/DEK 自动轮换生效。
    /// 缺省 root 密钥首次启动时生成并写入 `<data_dir>/encryption-root-key.bin`
    /// （0600）；升级窗口内旧明文数据需经一次性迁移工具。
    #[serde(default)]
    pub encryption_enabled: bool,

    /// 静态加密 root 密钥（hex 编码 32 字节）。
    /// 优先级：本配置 > `COORD_ENCRYPTION_ROOT_KEY` 环境变量 >
    /// `<data_dir>/encryption-root-key.bin`。多节点各存各的本地 DEK，
    /// 但每个节点启动都需同一 root 密钥以解密本地密文 DEK。
    #[serde(default)]
    pub encryption_root_key: Option<String>,

    /// Raft 节点间共享密钥（默认 None）。
    ///
    /// raft 端口认证策略（三选一，fail-closed）：
    /// 1. `tls_cert/tls_key + tls_ca` 已配置 → 强制 mTLS（缺 CA 拒绝启动）；
    /// 2. 本字段配置 → 节点间 Raft RPC 消息 HMAC-SHA256 认证；
    /// 3. 两者均无且 raft 绑定非 loopback → 拒绝启动。
    #[serde(default)]
    pub raft_shared_secret: Option<String>,
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
            encryption_enabled: false,
            encryption_root_key: None,
            raft_shared_secret: None,
        }
    }
}

/// 对象存储配置（`[object_storage]` 段；默认关闭）。
///
/// 数据面闭环设计（docs/volume-object-storage.md 决策记录）：对象 = (bucket,
/// object_id)；manifest 走 raft（/kv/ 语义强一致），chunk 数据随 raft 日志复制
/// 后落各节点本地 append-only 文件（不进 MVCC、不入快照）。chunk 上限 ≤ 4MiB
/// （对齐 gRPC MAX_DECODING_MSG），单对象默认 256MiB。关闭时磁盘布局字节级不变。
///
/// 全集群各节点配置必须一致（对齐 multi_raft 配置一致性约定）；切换 enabled 需
/// 全集群同启同停、不可在存量对象存在时关停（关闭即不再提供服务，数据保留）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectStorageConfig {
    /// 总开关（false = 不注册服务、不建任何目录，磁盘布局不变）
    #[serde(default)]
    pub enabled: bool,
    /// 单 chunk 字节上限（≤ 4MiB = 4194304；流式单消息不得越过此值）
    #[serde(default = "default_object_chunk_size")]
    pub chunk_size_bytes: usize,
    /// 单对象字节上限
    #[serde(default = "default_object_max_size")]
    pub max_object_size_bytes: u64,
    /// 配额：全节点对象合计字节上限（0 = 不限；admission 侧尽力而为，非硬限制）
    #[serde(default)]
    pub max_total_storage_bytes: u64,
    /// Creating 对象（上传中断）视为过期的秒数，后台 GC 将删除
    #[serde(default = "default_object_upload_timeout")]
    pub upload_timeout_secs: u64,
    /// 对象 GC/孤儿回收扫描间隔（秒）
    #[serde(default = "default_object_gc_interval")]
    pub gc_interval_secs: u64,
    /// chunk 文件静态加密（独立开关；不依赖 /kv/ 的 encryption_enabled）
    #[serde(default)]
    pub encryption_enabled: bool,
    /// chunk 加密根密钥（hex 64 = 32 字节；encryption_enabled=true 时必须）
    /// 或经环境变量 COORD_OBJECT_STORAGE_ENCRYPTION_ROOT_KEY 注入
    #[serde(default)]
    pub encryption_root_key: String,
    /// chunk DEK 自动轮换间隔（天；0 = 关闭自动轮换）。
    /// 根密钥经 HKDF 派生 KEK 包裹随机 DEK（key_id 版本化），到期轮换仅
    /// 影响新写入（旧 DEK 保留解密历史 chunk，对齐 /kv/ key_management 语义）。
    #[serde(default = "default_object_encryption_rotation_days")]
    pub encryption_rotation_days: u64,
}

impl Default for ObjectStorageConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            chunk_size_bytes: 4 * 1024 * 1024,
            max_object_size_bytes: 256 * 1024 * 1024,
            max_total_storage_bytes: 0,
            upload_timeout_secs: 300,
            gc_interval_secs: 60,
            encryption_enabled: false,
            encryption_root_key: String::new(),
            encryption_rotation_days: 90,
        }
    }
}

fn default_object_chunk_size() -> usize {
    4 * 1024 * 1024
}
fn default_object_max_size() -> u64 {
    256 * 1024 * 1024
}
fn default_object_upload_timeout() -> u64 {
    300
}
fn default_object_gc_interval() -> u64 {
    60
}
fn default_object_encryption_rotation_days() -> u64 {
    90
}

/// R-SVC-18：运行时资源限制配置（`[limits]` 段）。
///
/// 覆盖 per-RPC 超时（读/写/lease/compact）、Range/Txn 规模上限、
/// 幂等缓存参数（TTL/容量）。全部字段带生产保守默认值。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LimitsConfig {
    /// 读路径超时（毫秒）：线性一致性读 + 本地扫描
    #[serde(default = "default_read_timeout_ms")]
    pub read_timeout_ms: u64,

    /// 写路径 raft 提交超时（毫秒）
    #[serde(default = "default_write_timeout_ms")]
    pub write_timeout_ms: u64,

    /// Lease 写路径超时（毫秒）
    #[serde(default = "default_lease_timeout_ms")]
    pub lease_timeout_ms: u64,

    /// Compact 提案超时（毫秒）
    #[serde(default = "default_compact_timeout_ms")]
    pub compact_timeout_ms: u64,

    /// Range 单次扫描上限（0 = 不限制）
    #[serde(default = "default_max_range_limit")]
    pub max_range_limit: usize,

    /// Txn compare + success + failure 操作数上限（0 = 不限制）
    #[serde(default = "default_max_txn_ops")]
    pub max_txn_ops: usize,

    /// 幂等缓存条目 TTL（秒）
    #[serde(default = "default_idempotency_ttl_secs")]
    pub idempotency_ttl_secs: u64,

    /// 幂等缓存容量上限（FIFO 淘汰最旧）
    #[serde(default = "default_idempotency_max_entries")]
    pub idempotency_max_entries: usize,
}

fn default_read_timeout_ms() -> u64 {
    5000
}
fn default_write_timeout_ms() -> u64 {
    5000
}
fn default_lease_timeout_ms() -> u64 {
    5000
}
fn default_compact_timeout_ms() -> u64 {
    10_000
}
fn default_max_range_limit() -> usize {
    10_000
}
fn default_max_txn_ops() -> usize {
    128
}
fn default_idempotency_ttl_secs() -> u64 {
    60
}
fn default_idempotency_max_entries() -> usize {
    4096
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            read_timeout_ms: default_read_timeout_ms(),
            write_timeout_ms: default_write_timeout_ms(),
            lease_timeout_ms: default_lease_timeout_ms(),
            compact_timeout_ms: default_compact_timeout_ms(),
            max_range_limit: default_max_range_limit(),
            max_txn_ops: default_max_txn_ops(),
            idempotency_ttl_secs: default_idempotency_ttl_secs(),
            idempotency_max_entries: default_idempotency_max_entries(),
        }
    }
}

/// R-RFT-19：Raft 运行时调优配置（`[raft]` 段）。
///
/// 所有时间参数为毫秒，`None`/缺省 = 保持 openraft 默认值。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RaftTuningConfig {
    /// 心跳间隔（毫秒）
    #[serde(default)]
    pub heartbeat_interval_ms: Option<u64>,

    /// 选举超时下限（毫秒）
    #[serde(default)]
    pub election_timeout_min_ms: Option<u64>,

    /// 选举超时上限（毫秒）
    #[serde(default)]
    pub election_timeout_max_ms: Option<u64>,

    /// 安装快照超时（毫秒）
    #[serde(default)]
    pub install_snapshot_timeout_ms: Option<u64>,

    /// 快照策略：距上次快照累积日志条数（0 = Never 禁用自动快照）
    #[serde(default)]
    pub snapshot_logs_since_last: Option<u64>,

    /// 快照传输限速（字节/秒；0 = 不限速）
    #[serde(default)]
    pub snapshot_rate_limit_bytes_per_sec: u64,
}

/// Multi-Raft 配置（`[multi_raft]` 段）。
///
/// - `enabled=false`（默认）：单 Raft 模式，本段其余字段被忽略——磁盘布局、
///   备份、回滚均保持 legacy 字节级不变（退化路径）。
/// - `enabled=true`：启用多 Region 模式——节点在 `cluster.initial_nodes` 静态
///   成员上按 `initial_regions` 装配 per-region Raft 组（region ≥1，目录级存储
///   隔离于 `<data_dir>/regions/region-{id:016x}/`，见 raft/region_runtime.rs）；
///   region 0 仍作为 system raft（鉴权/会话等 `/_sys/*` 系统数据）保留在根目录。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiRaftConfig {
    /// 是否启用多 Region 模式（默认 false = 单 Raft legacy，字节级退化）
    #[serde(default)]
    pub enabled: bool,

    /// 初始 Region 表（静态装配；仅 `enabled=true` 时生效）。
    ///
    /// 每个 Region 复制到 `cluster.initial_nodes` 全部成员（v1 静态复制，无
    /// 按 Region 差异化分布）。key range 必须平铺整个 keyspace（无空洞/重叠）。
    #[serde(default)]
    pub initial_regions: Vec<InitialRegionConfig>,

    /// PD 子配置（`[multi_raft.pd]` 段；内嵌 PD）。
    ///
    /// 仅 `enabled=true`（多 Region 装配）时生效：节点内嵌运行 PlacementDriver
    /// （区域/节点心跳上报 + 调度循环 + operator 执行循环，元数据落盘
    /// `<data_dir>/pd/pd-meta.db`）。默认关闭 = 多 Region 装配但不调度。
    #[serde(default)]
    pub pd: MultiRaftPdConfig,

    /// 本次启动执行 Legacy → Multi-Raft 迁移
    /// （`[multi_raft].legacy_migration = true`，一次性）。
    ///
    /// 存量单 Raft（用户 KV 在 region 0 根 store）升级到 multi_raft 时，boot 期
    /// 各节点把根 store 的活用户 KV 经 raft `Put` 导入所属 Region（迁移经 raft
    /// 日志复制——日志==状态机，follower/快照/压缩一致），完成后经 region 0
    /// raft 写迁移标记 `/_sys/migration/legacy-v1`。迁移**只读源、不删源数据**
    /// （回滚 = 关闭 multi_raft 用 region 0 原数据字节级恢复）。
    ///
    /// 语义/边界见 `coord-server/src/migration.rs` 模块文档与
    /// ``；fail-closed 闸：根 store 有未迁移
    /// 用户 KV 且未开本开关/`allow_unmigrated` 时拒绝启动。
    #[serde(default)]
    pub legacy_migration: bool,

    /// 救援开关——根 store 有未迁移 legacy 用户 KV 时仍强制放行启动
    /// （跳过闸与迁移；数据 Region 为空，用户数据面由运维负责，通常仅用于
    /// 误开 multi_raft 后回滚排查）。正常升级路径请用 `legacy_migration`。
    #[serde(default)]
    pub allow_unmigrated: bool,
}

impl Default for MultiRaftConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            initial_regions: Vec::new(),
            pd: MultiRaftPdConfig::default(),
            legacy_migration: false,
            allow_unmigrated: false,
        }
    }
}

/// 初始 Region 配置（`[[multi_raft.initial_regions]]`）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitialRegionConfig {
    /// Region ID（必须 > 0 且唯一；0 保留给单 Raft/system raft）
    pub id: u64,

    /// Key range 起始（包含）；空 = keyspace 起点（仅允许第一个 region）
    #[serde(default)]
    pub start_key: String,

    /// Key range 结束（不包含）；空 = 无上界（仅允许最后一个 region）
    #[serde(default)]
    pub end_key: String,
}

/// PD 子配置（`[multi_raft.pd]` 段）。
///
/// v1 仅支持**内嵌模式**（PD 作为每个 Coord 进程的一部分运行，直接调度本进程
/// 装配的 Region raft；独立 PD 进程为）。字段默认值与
/// `coord_server::pd::PdConfig::default()` 对齐；`enabled=false`（默认）时本段
/// 全部字段被忽略（不装配 PD、不落盘 pd-meta.db、无调度/心跳循环）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MultiRaftPdConfig {
    /// 是否在节点内嵌运行 PD（Embedded 模式）
    pub enabled: bool,

    /// Region/节点心跳上报与成员对账间隔（毫秒；PD 调度输入的数据面节拍）
    pub heartbeat_interval_ms: u64,

    /// Split Checker 检查间隔（秒）
    pub split_check_interval: u64,

    /// Merge Checker 检查间隔（秒）
    pub merge_check_interval: u64,

    /// Leader 均衡调度间隔（秒）
    pub balance_interval: u64,

    /// 最大并发调度 Operator 数
    pub max_concurrent_operators: usize,

    /// Region 分裂大小阈值（MB）
    pub region_split_size_mb: u64,

    /// Region 分裂 Key 数阈值
    pub region_split_keys: u64,

    /// Region 合并大小阈值（MB）
    pub region_merge_size_mb: u64,

    /// 目标副本数（每 Region voter 数；v1 ≤ cluster.initial_nodes 成员数）
    pub target_replicas: usize,

    /// 节点心跳超时（秒；超时标记离线，调度不基于离线节点）
    pub node_heartbeat_timeout: u64,

    /// Running operator 认领超时（秒）——region 0 leader 周期扫描全局
    /// 队列，认领超过该时长（认领者失联/Complete 丢失 → 卡死）的 operator 放回
    /// Pending 由存活 Region leader 重认领（failover）。须大于单次 operator 正常
    /// 执行时长（默认 300s）。
    pub operator_running_timeout: u64,

    /// 调度暂停开关（初始值；true = 启动即不调度，仅 executor drain 已
    /// 排队 operator）。用于维护窗口/演练冻结调度；运行时切换见
    /// `PlacementDriver::set_scheduler_paused`（未来管理面 RPC 接入点）。
    #[serde(default)]
    pub scheduler_paused: bool,
}

impl Default for MultiRaftPdConfig {
    fn default() -> Self {
        // 与 coord_server::pd::PdConfig::default() 的对应字段保持一致
        Self {
            enabled: false,
            heartbeat_interval_ms: 5000,
            split_check_interval: 30,
            merge_check_interval: 60,
            balance_interval: 120,
            max_concurrent_operators: 10,
            region_split_size_mb: 256,
            region_split_keys: 1_000_000,
            region_merge_size_mb: 16,
            target_replicas: 3,
            node_heartbeat_timeout: 30,
            operator_running_timeout: 300,
            scheduler_paused: false,
        }
    }
}

impl MultiRaftPdConfig {
    /// 转换为 PD 运行时配置（Embedded 模式；main.rs 装配用）
    pub fn to_pd_config(&self) -> coord_server::pd::PdConfig {
        coord_server::pd::PdConfig {
            mode: coord_server::pd::PdMode::Embedded,
            external_addrs: Vec::new(),
            split_check_interval: self.split_check_interval,
            merge_check_interval: self.merge_check_interval,
            balance_interval: self.balance_interval,
            max_concurrent_operators: self.max_concurrent_operators,
            region_split_size_mb: self.region_split_size_mb,
            region_split_keys: self.region_split_keys,
            region_merge_size_mb: self.region_merge_size_mb,
            target_replicas: self.target_replicas,
            node_heartbeat_timeout: self.node_heartbeat_timeout,
            operator_running_timeout: self.operator_running_timeout,
            placement: Default::default(),
            maintenance: Default::default(),
            scheduler_paused: self.scheduler_paused,
        }
    }
}

impl LimitsConfig {
    /// 转换为 coord-server 运行时限制（R-SVC-18）。
    pub fn to_runtime_limits(&self) -> coord_server::server::RuntimeLimits {
        coord_server::server::RuntimeLimits {
            read_timeout: std::time::Duration::from_millis(self.read_timeout_ms),
            write_timeout: std::time::Duration::from_millis(self.write_timeout_ms),
            lease_timeout: std::time::Duration::from_millis(self.lease_timeout_ms),
            compact_timeout: std::time::Duration::from_millis(self.compact_timeout_ms),
            max_range_limit: self.max_range_limit,
            max_txn_ops: self.max_txn_ops,
            idempotency_ttl: std::time::Duration::from_secs(self.idempotency_ttl_secs),
            idempotency_max_entries: self.idempotency_max_entries,
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
        // 默认配置启动即鉴权开启
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

    // ──── 启动校验与热更新 ────

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

    // ──── 占位密钥/密码拒绝 ────

    #[test]
    fn test_validate_rejects_all_zero_auth_root_key() {
        let mut config = Config::default();
        config.security.auth_root_key = Some("0".repeat(64));
        let errs = config.validate().unwrap_err();
        assert!(
            errs.iter().any(|e| e.contains("all-zeros")),
            "all-zeros auth_root_key must be rejected: {errs:?}"
        );
    }

    #[test]
    fn test_validate_rejects_all_zero_encryption_root_key() {
        let mut config = Config::default();
        config.security.encryption_root_key = Some("0".repeat(64));
        let errs = config.validate().unwrap_err();
        assert!(
            errs.iter().any(|e| e.contains("all-zeros")),
            "all-zeros encryption_root_key must be rejected: {errs:?}"
        );
    }

    #[test]
    fn test_validate_rejects_placeholder_root_password() {
        let mut config = Config::default();
        config.security.root_password = Some("CHANGE_ME_STRONG_PW".to_string());
        let errs = config.validate().unwrap_err();
        assert!(
            errs.iter().any(|e| e.contains("root_password")),
            "placeholder root_password must be rejected: {errs:?}"
        );
    }

    #[test]
    fn test_validate_rejects_placeholder_raft_secret() {
        let mut config = Config::default();
        config.security.raft_shared_secret = Some("change-me-raft-secret-16chars".to_string());
        let errs = config.validate().unwrap_err();
        assert!(
            errs.iter().any(|e| e.contains("raft_shared_secret")),
            "placeholder raft_shared_secret must be rejected: {errs:?}"
        );
    }

    #[test]
    fn test_validate_rejects_fill_placeholders() {
        let mut config = Config::default();
        config.security.root_password = Some("__FILL_ROOT_PASSWORD__".to_string());
        config.security.raft_shared_secret = Some("__FILL_RAFT_SHARED_SECRET__".to_string());
        let errs = config.validate().unwrap_err();
        assert!(
            errs.iter().any(|e| e.contains("root_password"))
                && errs.iter().any(|e| e.contains("raft_shared_secret")),
            "fill placeholders must be rejected: {errs:?}"
        );
    }

    #[test]
    fn test_validate_accepts_real_secrets() {
        let mut config = Config::default();
        config.security.auth_root_key = Some("42".repeat(32));
        config.security.root_password = Some("s3cure-P@ssw0rd-2026".to_string());
        config.security.raft_shared_secret = Some("s3cure-raft-shared-secret".to_string());
        assert!(config.validate().is_ok());
    }

    // ──── raft 共享密钥校验 ────

    #[test]
    fn test_validate_rejects_short_raft_shared_secret() {
        let mut config = Config::default();
        config.security.raft_shared_secret = Some("short".to_string());
        let errs = config.validate().unwrap_err();
        assert!(
            errs.iter().any(|e| e.contains("raft_shared_secret")),
            "short raft_shared_secret must be rejected: {errs:?}"
        );
    }

    #[test]
    fn test_validate_accepts_long_raft_shared_secret() {
        let mut config = Config::default();
        config.security.raft_shared_secret = Some("this-secret-is-long-enough".to_string());
        assert!(config.validate().is_ok());
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

    // ──── Multi-Raft（[multi_raft] / /）────

    /// 构造 enabled=true + 3 region 平铺（["","b") / ["b","n") / ["n",""))
    /// 且本节点（node_id）在 initial_nodes 中的合法配置。
    fn multi_raft_member_config(node_id: u64) -> Config {
        let mut config = Config::default();
        config.node.id = node_id;
        config.cluster.initial_nodes = vec![
            ClusterNode {
                id: 1,
                grpc: "127.0.0.1:50071".to_string(),
                raft: "127.0.0.1:50072".to_string(),
            },
            ClusterNode {
                id: 2,
                grpc: "127.0.0.1:50081".to_string(),
                raft: "127.0.0.1:50082".to_string(),
            },
            ClusterNode {
                id: 3,
                grpc: "127.0.0.1:50091".to_string(),
                raft: "127.0.0.1:50092".to_string(),
            },
        ];
        config.security.auth_root_key = Some("ab".repeat(32));
        config.multi_raft.enabled = true;
        config.multi_raft.initial_regions = vec![
            InitialRegionConfig {
                id: 1,
                start_key: "".to_string(),
                end_key: "b".to_string(),
            },
            InitialRegionConfig {
                id: 2,
                start_key: "b".to_string(),
                end_key: "n".to_string(),
            },
            InitialRegionConfig {
                id: 3,
                start_key: "n".to_string(),
                end_key: "".to_string(),
            },
        ];
        config
    }

    #[test]
    fn test_multi_raft_default_disabled() {
        // 默认关闭 = 单 Raft 退化路径（initial_regions 不生效）
        let config = Config::default();
        assert!(!config.multi_raft.enabled);
        assert!(config.multi_raft.initial_regions.is_empty());
        // 迁移/救援开关默认关闭（不改变既有行为）
        assert!(!config.multi_raft.legacy_migration);
        assert!(!config.multi_raft.allow_unmigrated);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_multi_raft_migration_flags_parse() {
        let toml_str = r#"
[multi_raft]
enabled = true
legacy_migration = true
allow_unmigrated = true

[[multi_raft.initial_regions]]
id = 1
start_key = ""
end_key = ""
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert!(config.multi_raft.enabled);
        assert!(config.multi_raft.legacy_migration);
        assert!(config.multi_raft.allow_unmigrated);
    }

    #[test]
    fn test_multi_raft_parse_toml() {
        let toml_str = r#"
[multi_raft]
enabled = true

[[multi_raft.initial_regions]]
id = 1
start_key = ""
end_key = "b"

[[multi_raft.initial_regions]]
id = 2
start_key = "b"
end_key = ""
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert!(config.multi_raft.enabled);
        assert_eq!(config.multi_raft.initial_regions.len(), 2);
        assert_eq!(config.multi_raft.initial_regions[0].id, 1);
        assert_eq!(config.multi_raft.initial_regions[0].start_key, "");
        assert_eq!(config.multi_raft.initial_regions[0].end_key, "b");
        assert_eq!(config.multi_raft.initial_regions[1].id, 2);
        assert_eq!(config.multi_raft.initial_regions[1].end_key, "");
    }

    #[test]
    fn test_multi_raft_validate_ok() {
        let config = multi_raft_member_config(1);
        let errs = config.validate();
        assert!(errs.is_ok(), "valid multi_raft config: {errs:?}");
    }

    #[test]
    fn test_multi_raft_validate_empty_regions_when_enabled() {
        let mut config = multi_raft_member_config(1);
        config.multi_raft.initial_regions.clear();
        let errs = config.validate().unwrap_err();
        assert!(
            errs.iter().any(|e| e.contains("initial_regions")),
            "{errs:?}"
        );
    }

    #[test]
    fn test_multi_raft_validate_region_zero_reserved() {
        let mut config = multi_raft_member_config(1);
        config.multi_raft.initial_regions[0].id = 0;
        let errs = config.validate().unwrap_err();
        assert!(
            errs.iter().any(|e| e.contains("must be > 0")),
            "{errs:?}"
        );
    }

    #[test]
    fn test_multi_raft_validate_duplicate_region_id() {
        let mut config = multi_raft_member_config(1);
        config
            .multi_raft
            .initial_regions
            .push(InitialRegionConfig {
                id: 2,
                start_key: "z".to_string(),
                end_key: "".to_string(),
            });
        let errs = config.validate().unwrap_err();
        assert!(
            errs.iter().any(|e| e.contains("duplicated")),
            "{errs:?}"
        );
    }

    #[test]
    fn test_multi_raft_validate_gap_between_ranges() {
        // region1 结束于 "b"，region2 从 "c" 开始 → 空洞（["b","c") 无归属）
        let mut config = multi_raft_member_config(1);
        config.multi_raft.initial_regions = vec![
            InitialRegionConfig {
                id: 1,
                start_key: "".to_string(),
                end_key: "b".to_string(),
            },
            InitialRegionConfig {
                id: 2,
                start_key: "c".to_string(),
                end_key: "".to_string(),
            },
        ];
        let errs = config.validate().unwrap_err();
        assert!(
            errs.iter().any(|e| e.contains("contiguous")),
            "{errs:?}"
        );
    }

    #[test]
    fn test_multi_raft_validate_first_start_not_empty() {
        let mut config = multi_raft_member_config(1);
        config.multi_raft.initial_regions = vec![InitialRegionConfig {
            id: 1,
            start_key: "a".to_string(),
            end_key: "".to_string(),
        }];
        let errs = config.validate().unwrap_err();
        assert!(
            errs.iter().any(|e| e.contains("start_key")),
            "{errs:?}"
        );
    }

    #[test]
    fn test_multi_raft_validate_last_end_not_unbounded() {
        let mut config = multi_raft_member_config(1);
        config.multi_raft.initial_regions = vec![InitialRegionConfig {
            id: 1,
            start_key: "".to_string(),
            end_key: "z".to_string(),
        }];
        let errs = config.validate().unwrap_err();
        assert!(
            errs.iter().any(|e| e.contains("end_key")),
            "{errs:?}"
        );
    }

    #[test]
    fn test_multi_raft_validate_node_not_member() {
        // node 5 不在 initial_nodes 中（region 副本不会放在该节点上）
        let config = multi_raft_member_config(5);
        let errs = config.validate().unwrap_err();
        assert!(
            errs.iter().any(|e| e.contains("initial_nodes")),
            "{errs:?}"
        );
    }

    #[test]
    fn test_multi_raft_validate_join_rejected() {
        // v1：multi-region 仅支持静态成员（join 节点不承载 region 副本）
        let mut config = multi_raft_member_config(1);
        config.cluster.join_addr = Some("127.0.0.1:50071".to_string());
        let errs = config.validate().unwrap_err();
        assert!(
            errs.iter().any(|e| e.contains("join")),
            "{errs:?}"
        );
    }

    // ──── Multi-Raft PD（[multi_raft.pd] /）────

    #[test]
    fn test_multi_raft_pd_default_disabled() {
        // pd.enabled 默认 false = 多 Region 装配但不调度
        let config = Config::default();
        assert!(!config.multi_raft.pd.enabled);
        // 默认 pd 段与 PdConfig::default() 对齐（内嵌模式转换后）
        let pd = config.multi_raft.pd.to_pd_config();
        assert!(matches!(pd.mode, coord_server::pd::PdMode::Embedded));
        assert_eq!(pd.balance_interval, 120);
        assert_eq!(pd.target_replicas, 3);
        // Running 超时重认领默认 300s（对齐 PdConfig::default()）
        assert_eq!(pd.operator_running_timeout, 300);
        assert_eq!(config.multi_raft.pd.operator_running_timeout, 300);
        // 调度暂停默认 false（启动即正常调度）
        assert!(!config.multi_raft.pd.scheduler_paused);
        assert!(!pd.scheduler_paused);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_multi_raft_pd_parse_toml() {
        let toml_str = r#"
[multi_raft]
enabled = true

[[multi_raft.initial_regions]]
id = 1
start_key = ""
end_key = ""

[multi_raft.pd]
enabled = true
heartbeat_interval_ms = 1000
balance_interval = 10
node_heartbeat_timeout = 15
target_replicas = 2
max_concurrent_operators = 4
region_split_keys = 500000
scheduler_paused = true
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        let pd_cfg = &config.multi_raft.pd;
        assert!(pd_cfg.enabled);
        assert_eq!(pd_cfg.heartbeat_interval_ms, 1000);
        assert_eq!(pd_cfg.balance_interval, 10);
        assert_eq!(pd_cfg.node_heartbeat_timeout, 15);
        assert_eq!(pd_cfg.target_replicas, 2);
        assert_eq!(pd_cfg.max_concurrent_operators, 4);
        assert_eq!(pd_cfg.region_split_keys, 500_000);
        // 未出现的字段走默认值
        assert_eq!(pd_cfg.split_check_interval, 30);
        assert_eq!(pd_cfg.region_split_size_mb, 256);
        assert_eq!(pd_cfg.operator_running_timeout, 300);
        // 调度暂停开关透传
        assert!(pd_cfg.scheduler_paused, "toml scheduler_paused=true 应被解析");

        // to_pd_config 映射
        let pd = pd_cfg.to_pd_config();
        assert_eq!(pd.balance_interval, 10);
        assert_eq!(pd.node_heartbeat_timeout, 15);
        assert_eq!(pd.target_replicas, 2);
        assert!(pd.scheduler_paused, "scheduler_paused 应透传到 PdConfig");
        assert_eq!(pd.operator_running_timeout, 300, "默认值应透传到 PdConfig");
        assert!(pd.external_addrs.is_empty());
    }

    #[test]
    fn test_multi_raft_pd_validate_ok() {
        let mut config = multi_raft_member_config(1);
        config.multi_raft.pd.enabled = true;
        config.multi_raft.pd.target_replicas = 3; // == 成员数
        assert!(config.validate().is_ok(), "{:?}", config.validate().err());
    }

    #[test]
    fn test_multi_raft_pd_requires_multi_raft_enabled() {
        // pd.enabled=true 但 multi_raft.enabled=false → 拒绝
        let mut config = Config::default();
        config.multi_raft.pd.enabled = true;
        let errs = config.validate().unwrap_err();
        assert!(
            errs.iter().any(|e| e.contains("requires multi_raft.enabled")),
            "{errs:?}"
        );
    }

    #[test]
    fn test_multi_raft_pd_validate_zero_interval() {
        let mut config = multi_raft_member_config(1);
        config.multi_raft.pd.enabled = true;
        config.multi_raft.pd.heartbeat_interval_ms = 0;
        config.multi_raft.pd.balance_interval = 0;
        config.multi_raft.pd.operator_running_timeout = 0;
        let errs = config.validate().unwrap_err();
        assert!(
            errs.iter().any(|e| e.contains("heartbeat_interval_ms")),
            "{errs:?}"
        );
        assert!(
            errs.iter().any(|e| e.contains("balance_interval")),
            "{errs:?}"
        );
        assert!(
            errs.iter().any(|e| e.contains("operator_running_timeout")),
            "{errs:?}"
        );
    }

    #[test]
    fn test_multi_raft_pd_validate_target_replicas_exceeds_members() {
        let mut config = multi_raft_member_config(1);
        config.multi_raft.pd.enabled = true;
        config.multi_raft.pd.target_replicas = 4; // > 3 成员
        let errs = config.validate().unwrap_err();
        assert!(
            errs.iter().any(|e| e.contains("target_replicas")),
            "{errs:?}"
        );
    }
}
