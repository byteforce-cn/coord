//! Command-line interface 与 `main()` 之外的纯辅助函数。
//!
//! 从 `main.rs` 抽出的无副作用组件：CLI 结构体、peer/bootstrap 解析、
//! node_id 解析与持久化、unseal shares 读取、tracing 初始化。这些
//! 内容本就无需访问 raft/state 运行时对象，放在单独模块便于单元测试。

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(name = "coord-server", version, about = "Coordination service server")]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    Dev(ServeArgs),
    Serve(ServeArgs),
}

#[derive(Debug, Clone, Args)]
pub(crate) struct ServeArgs {
    #[arg(long, env = "COORD_GRPC_ADDR", default_value = "0.0.0.0:9090")]
    pub grpc_addr: String,
    #[arg(
        long,
        visible_alias = "metrics-addr",
        env = "COORD_HTTP_ADDR",
        default_value = "0.0.0.0:9091"
    )]
    pub http_addr: String,
    #[arg(long, env = "COORD_DATA_DIR", default_value = "/tmp/coord-dev")]
    pub data_dir: String,
    #[arg(long, env = "COORD_NODE_ID")]
    pub node_id: Option<String>,
    #[arg(long)]
    pub auto_unseal_shares_file: Option<PathBuf>,
    /// Comma-separated list of peer gRPC addresses (host:port) for cluster auto-join.
    /// Example: COORD_CLUSTER_PEERS="coord-2:9090,coord-3:9090".
    /// When set, this node will probe the peers' node ids and try to form a Raft cluster.
    /// The node with the smallest id (alphabetic order across self + peers) becomes the
    /// bootstrap leader and proposes membership additions for every peer.
    #[arg(long, env = "COORD_CLUSTER_PEERS", default_value = "")]
    pub peers: String,
    /// Whether this node should bootstrap the Raft cluster as a single-node leader and
    /// then add the configured peers via auto-join. Defaults to true if no peers are set
    /// (single-node mode); in a multi-node cluster only one node should be bootstrap=true.
    #[arg(long, env = "COORD_BOOTSTRAP", default_value = "")]
    pub bootstrap: String,
    /// PEM-encoded TLS server certificate chain. When set together with `--tls-key`,
    /// both gRPC and HTTP control-plane listeners serve over TLS. Must be PEM (not DER).
    #[arg(long, env = "COORD_TLS_CERT")]
    pub tls_cert: Option<PathBuf>,
    /// PEM-encoded TLS server private key matching `--tls-cert`.
    #[arg(long, env = "COORD_TLS_KEY")]
    pub tls_key: Option<PathBuf>,
    /// PEM-encoded CA bundle used to verify client certificates. When set, the server
    /// requires mTLS on both gRPC and HTTP (clients without a CA-signed cert are rejected).
    #[arg(long, env = "COORD_TLS_CLIENT_CA")]
    pub tls_client_ca: Option<PathBuf>,
    /// OTLP collector endpoint (e.g. `http://otel-collector:4317`). When set,
    /// the server marks OTLP as configured; trace-context propagation via W3C
    /// `traceparent` is always active regardless of this flag. The concrete
    /// OTLP exporter wiring is applied when the infra rollout stabilizes.
    #[arg(long, env = "COORD_OTLP_ENDPOINT")]
    pub otlp_endpoint: Option<String>,
    /// Dev mode only: fix the root token returned by the first `init` call.
    /// Allows unit / integration tests to hard-code a stable token value
    /// (`COORD_DEV_ROOT_TOKEN=s.test`) instead of parsing it from logs.
    /// This flag is **silently ignored** in `serve` (production) mode.
    #[arg(long, env = "COORD_DEV_ROOT_TOKEN")]
    pub dev_root_token: Option<String>,
    /// 可选：Gossip UDP 监听地址（host:port）。设置后节点会加入 Gossip 环，
    /// 将自身服务信息广播给 coord-client 代理节点。
    /// 示例：`COORD_GOSSIP_ADDR=0.0.0.0:7946`
    #[arg(long, env = "COORD_GOSSIP_ADDR")]
    pub gossip_addr: Option<String>,
}

/// Initialize the global tracing subscriber with a sensible default filter.
pub(crate) fn init_tracing(dev_mode: bool) {
    let default_filter = if dev_mode { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| default_filter.into()),
        )
        .compact()
        .init();
}

/// Parse a comma-separated peer list, trimming and dropping empty entries.
pub(crate) fn parse_peers(s: &str) -> Vec<String> {
    s.split(',')
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
        .collect()
}

/// Resolve the `--bootstrap` flag.
///
/// Empty → bootstrap when there are no peers (single-node mode); otherwise defer.
/// Non-empty → strict boolean interpretation of `1/true/yes/on`.
pub(crate) fn resolve_bootstrap_flag(raw: &str, no_peers: bool) -> bool {
    let trimmed = raw.trim().to_ascii_lowercase();
    if trimmed.is_empty() {
        return no_peers;
    }
    matches!(trimmed.as_str(), "1" | "true" | "yes" | "on")
}

/// Resolve this node's stable identity.
///
/// Priority: explicit `--node-id` flag → `<data_dir>/node_id` file → newly generated UUID.
/// In all cases the resolved value is written back to disk so the identity is stable
/// across restarts.
pub(crate) fn resolve_node_id(
    requested_node_id: Option<String>,
    data_dir: &Path,
) -> anyhow::Result<String> {
    let node_id_file = data_dir.join("node_id");

    if let Some(node_id) = requested_node_id
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        std::fs::write(&node_id_file, &node_id).with_context(|| {
            format!(
                "failed to persist provided node_id to {}",
                node_id_file.display()
            )
        })?;
        return Ok(node_id);
    }

    if node_id_file.exists() {
        let existing = std::fs::read_to_string(&node_id_file)
            .with_context(|| format!("failed to read node id file: {}", node_id_file.display()))?;
        let existing = existing.trim();
        if !existing.is_empty() {
            return Ok(existing.to_string());
        }
    }

    let generated = format!("node-{}", Uuid::new_v4().simple());
    std::fs::write(&node_id_file, &generated).with_context(|| {
        format!(
            "failed to persist generated node_id to {}",
            node_id_file.display()
        )
    })?;
    Ok(generated)
}

/// Load unseal shares from a file.
///
/// - Lines beginning with `#` (or content after `#` on any line) are treated as comments.
/// - Each line may contain multiple shares separated by `,`.
/// - Duplicates are silently deduped.
/// - An empty result is an error (an empty shares file is almost certainly operator mistake).
pub(crate) fn load_unseal_shares_from_file(shares_file: &Path) -> anyhow::Result<Vec<String>> {
    let content = std::fs::read_to_string(shares_file).with_context(|| {
        format!(
            "failed to read auto-unseal shares file: {}",
            shares_file.display()
        )
    })?;

    let mut seen = HashSet::new();
    let mut shares = Vec::new();

    for line in content.lines() {
        let line = line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }

        for token in line.split(',') {
            let share = token.trim();
            if share.is_empty() {
                continue;
            }
            if seen.insert(share.to_string()) {
                shares.push(share.to_string());
            }
        }
    }

    if shares.is_empty() {
        return Err(anyhow::anyhow!(
            "no valid unseal shares found in {}",
            shares_file.display()
        ));
    }

    Ok(shares)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Minimal unique temp dir under `std::env::temp_dir()`; cleans up on drop.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "coord-server-cli-test-{}-{}-{}",
                tag,
                std::process::id(),
                uuid::Uuid::new_v4().simple()
            ));
            std::fs::create_dir_all(&dir).expect("create temp dir");
            TempDir(dir)
        }
        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn parse_peers_trims_and_filters_empty() {
        assert_eq!(
            parse_peers(" a:9090 , b:9091 ,, c:9092 "),
            vec!["a:9090", "b:9091", "c:9092"]
        );
        assert!(parse_peers("").is_empty());
        assert!(parse_peers(" , , ").is_empty());
    }

    #[test]
    fn resolve_bootstrap_flag_defaults_to_no_peers() {
        assert!(
            resolve_bootstrap_flag("", true),
            "empty + no peers → bootstrap"
        );
        assert!(!resolve_bootstrap_flag("", false), "empty + peers → defer");
    }

    #[test]
    fn resolve_bootstrap_flag_accepts_truthy_tokens() {
        for yes in ["1", "true", "TRUE", "Yes", "on"] {
            assert!(resolve_bootstrap_flag(yes, false), "`{yes}` must be truthy");
        }
        for no in ["0", "false", "no", "off", "anything"] {
            assert!(!resolve_bootstrap_flag(no, false), "`{no}` must be falsy");
        }
    }

    #[test]
    fn resolve_node_id_prefers_explicit() {
        let tmp = TempDir::new("explicit");
        let id = resolve_node_id(Some("explicit-id".into()), tmp.path()).unwrap();
        assert_eq!(id, "explicit-id");
        let on_disk = std::fs::read_to_string(tmp.path().join("node_id")).unwrap();
        assert_eq!(on_disk, "explicit-id");
    }

    #[test]
    fn resolve_node_id_reuses_existing_file() {
        let tmp = TempDir::new("reuse");
        std::fs::write(tmp.path().join("node_id"), "persisted-id").unwrap();
        let id = resolve_node_id(None, tmp.path()).unwrap();
        assert_eq!(id, "persisted-id");
    }

    #[test]
    fn resolve_node_id_generates_when_absent() {
        let tmp = TempDir::new("gen");
        let id = resolve_node_id(None, tmp.path()).unwrap();
        assert!(id.starts_with("node-"));
        assert!(tmp.path().join("node_id").exists());
    }

    #[test]
    fn resolve_node_id_ignores_whitespace_explicit() {
        let tmp = TempDir::new("ws");
        let id = resolve_node_id(Some("   ".into()), tmp.path()).unwrap();
        assert!(id.starts_with("node-"));
    }

    #[test]
    fn load_unseal_shares_parses_and_dedupes() {
        let tmp = TempDir::new("shares-ok");
        let file = tmp.path().join("shares.txt");
        let mut f = std::fs::File::create(&file).unwrap();
        writeln!(
            f,
            "# this is a comment\nshare-1\nshare-2,share-3\nshare-1 # duplicate\n"
        )
        .unwrap();

        let shares = load_unseal_shares_from_file(&file).unwrap();
        assert_eq!(shares, vec!["share-1", "share-2", "share-3"]);
    }

    #[test]
    fn load_unseal_shares_errors_on_empty() {
        let tmp = TempDir::new("shares-empty");
        let file = tmp.path().join("shares.txt");
        std::fs::write(&file, "# only comments\n\n").unwrap();

        let err = load_unseal_shares_from_file(&file).unwrap_err();
        assert!(err.to_string().contains("no valid unseal shares"));
    }

    #[test]
    fn load_unseal_shares_errors_on_missing_file() {
        let tmp = TempDir::new("shares-missing");
        let file = tmp.path().join("does-not-exist.txt");
        let err = load_unseal_shares_from_file(&file).unwrap_err();
        assert!(err.to_string().contains("failed to read"));
    }
}
