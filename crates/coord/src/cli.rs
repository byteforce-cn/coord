//! Command-line interface 与 `main()` 之外的纯辅助函数。
//!
//! 顶层结构：`coord <server|dev|client|ctl> [args...]`
//!
//! - `server` / `dev`：对应原 `coord-server serve` / `coord-server dev`
//! - `client`：Phase 4D gossip 代理模式（当前为 stub）
//! - `ctl`：对应原 `coord-ctl`，提供所有管理子命令

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

// ─── Top-level CLI ────────────────────────────────────────────────────────────

#[derive(Debug, Parser)]
#[command(name = "coord", version, about = "Coordination service")]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Run as Raft server (production mode).
    Server(ServeArgs),
    /// Run as Raft server in development mode (auto-init, single-node, fixed root token).
    Dev(ServeArgs),
    /// Run as gossip client proxy (AP discovery cache + Gossip membership + CP passthrough).
    Client(ClientArgs),
    /// Start CP server + AP gossip agent in a single process (development / single-machine).
    All(AllArgs),
    /// Admin CLI — connect to a running coord server.
    Ctl(CtlArgs),
}

// ─── Server / Dev args ────────────────────────────────────────────────────────

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
}

// ─── Client (gossip proxy) args ───────────────────────────────────────────────

/// Arguments for `coord client` (gossip sidecar proxy mode).
#[derive(Debug, Clone, Args)]
pub(crate) struct ClientArgs {
    /// Stable node identity for this client proxy. Auto-generated UUID when omitted.
    #[arg(long, env = "COORD_CLIENT_NODE_ID")]
    pub node_id: Option<String>,
    /// UDP address for the Gossip (Scuttlebutt) membership protocol.
    #[arg(long, env = "COORD_CLIENT_GOSSIP_ADDR", default_value = "0.0.0.0:7947")]
    pub gossip_addr: String,
    /// Externally advertised Gossip address (public IP:port). Defaults to gossip_addr.
    #[arg(long, env = "COORD_CLIENT_GOSSIP_ADVERTISE_ADDR")]
    pub gossip_advertise_addr: Option<String>,
    /// gRPC address this proxy listens on for local services.
    #[arg(long, env = "COORD_CLIENT_GRPC_ADDR", default_value = "127.0.0.1:9090")]
    pub local_grpc_addr: String,
    /// HTTP/metrics address this proxy listens on.
    #[arg(long, env = "COORD_CLIENT_HTTP_ADDR", default_value = "127.0.0.1:9091")]
    pub local_http_addr: String,
    /// Gossip seed addresses (host:port), comma-separated.
    #[arg(long, env = "COORD_CLIENT_GOSSIP_SEEDS", value_delimiter = ',')]
    pub gossip_seeds: Vec<String>,
    /// Gossip cluster ID — all nodes in the same cluster must use the same value.
    #[arg(long, env = "COORD_CLIENT_CLUSTER_ID", default_value = "coord-cluster")]
    pub cluster_id: String,
    /// gRPC endpoints of coord-server nodes for CP fallback, comma-separated.
    #[arg(long, env = "COORD_CLIENT_SERVER_ENDPOINTS", value_delimiter = ',')]
    pub server_endpoints: Vec<String>,
    /// How long (seconds) to cache service-endpoint mappings from the gossip ring.
    #[arg(long, env = "COORD_CLIENT_CACHE_TTL_SECONDS", default_value_t = 30)]
    pub cache_ttl_seconds: u64,
    /// Health-check interval in seconds.
    #[arg(
        long,
        env = "COORD_CLIENT_HEALTH_INTERVAL_SECONDS",
        default_value_t = 10
    )]
    pub health_interval_seconds: u64,
    /// PEM-encoded CA for verifying server TLS certificates.
    #[arg(long, env = "COORD_CLIENT_TLS_CA")]
    pub tls_ca: Option<PathBuf>,
    /// Client certificate (PEM) for mTLS.
    #[arg(long, env = "COORD_CLIENT_TLS_CERT")]
    pub tls_cert: Option<PathBuf>,
    /// Client private key (PEM) for mTLS.
    #[arg(long, env = "COORD_CLIENT_TLS_KEY")]
    pub tls_key: Option<PathBuf>,
}

// ─── All (server + client) args ──────────────────────────────────────────────

/// Arguments for `coord all` — starts CP server and AP gossip agent in one process.
///
/// Server behaviour is identical to `coord dev` (auto-init, single-node, fixed root token).
/// The embedded gossip agent connects to the same gRPC endpoint the server binds to.
#[derive(Debug, Args)]
pub(crate) struct AllArgs {
    /// All server arguments (same as `coord dev`).
    #[command(flatten)]
    pub server: ServeArgs,
    /// UDP port for the embedded gossip agent.
    #[arg(long, env = "COORD_CLIENT_GOSSIP_PORT", default_value = "7947")]
    pub gossip_port: u16,
    /// Gossip cluster ID — must match all other nodes in the cluster.
    #[arg(long, env = "COORD_CLIENT_CLUSTER_ID", default_value = "coord-cluster")]
    pub cluster_id: String,
    /// Service-endpoint cache TTL in seconds.
    #[arg(long, env = "COORD_CLIENT_CACHE_TTL_SECONDS", default_value_t = 30)]
    pub cache_ttl_seconds: u64,
}

// ─── Ctl args ─────────────────────────────────────────────────────────────────

/// Arguments for `coord ctl` (admin CLI).
#[derive(Debug, Args)]
pub(crate) struct CtlArgs {
    #[arg(long, default_value = "http://127.0.0.1:9090")]
    pub endpoint: String,
    #[arg(long)]
    pub token: Option<String>,
    /// PEM-encoded CA bundle used to verify the server certificate. Required
    /// when the server uses a non-public CA (self-signed dev certs).
    #[arg(long, env = "COORD_TLS_CA")]
    pub tls_ca: Option<PathBuf>,
    /// Client certificate (PEM) for mTLS. Must be paired with `--tls-key`.
    #[arg(long, env = "COORD_TLS_CERT")]
    pub tls_cert: Option<PathBuf>,
    /// Client private key (PEM) for mTLS.
    #[arg(long, env = "COORD_TLS_KEY")]
    pub tls_key: Option<PathBuf>,
    /// SNI / certificate verification domain override (default: endpoint host).
    #[arg(long, env = "COORD_TLS_DOMAIN")]
    pub tls_domain: Option<String>,
    #[command(subcommand)]
    pub command: CtlCommand,
}

impl CtlArgs {
    /// Build a tonic [`Channel`] honouring `--tls-*` flags. Auto-detects TLS
    /// when the endpoint uses `https://`; clients providing `--tls-*` without
    /// `https://` receive a config error instead of a silent downgrade.
    pub(crate) async fn build_channel(&self) -> anyhow::Result<Channel> {
        let uses_tls = self.endpoint.starts_with("https://")
            || self.tls_ca.is_some()
            || self.tls_cert.is_some()
            || self.tls_key.is_some();

        let mut endpoint = Endpoint::from_shared(self.endpoint.clone())
            .with_context(|| format!("invalid endpoint: {}", self.endpoint))?;

        if uses_tls {
            if !self.endpoint.starts_with("https://") {
                anyhow::bail!(
                    "TLS flags supplied but endpoint scheme is not https://: {}",
                    self.endpoint
                );
            }
            if self.tls_cert.is_some() ^ self.tls_key.is_some() {
                anyhow::bail!("--tls-cert and --tls-key must be provided together");
            }

            let mut tls = ClientTlsConfig::new();
            if let Some(ca_path) = &self.tls_ca {
                let ca_pem = fs::read(ca_path)
                    .with_context(|| format!("read tls CA: {}", ca_path.display()))?;
                tls = tls.ca_certificate(tonic::transport::Certificate::from_pem(ca_pem));
            }
            if let (Some(cert_path), Some(key_path)) = (&self.tls_cert, &self.tls_key) {
                let cert_pem = fs::read(cert_path)
                    .with_context(|| format!("read tls cert: {}", cert_path.display()))?;
                let key_pem = fs::read(key_path)
                    .with_context(|| format!("read tls key: {}", key_path.display()))?;
                tls = tls.identity(tonic::transport::Identity::from_pem(cert_pem, key_pem));
            }
            if let Some(domain) = &self.tls_domain {
                tls = tls.domain_name(domain);
            }
            endpoint = endpoint
                .tls_config(tls)
                .context("invalid client TLS config")?;
        }

        endpoint
            .connect()
            .await
            .with_context(|| format!("failed to connect to {}", self.endpoint))
    }
}

#[derive(Debug, Subcommand)]
pub(crate) enum CtlCommand {
    Cluster(ClusterCommand),
    Member(MemberCommand),
    Lock(LockCommand),
    Operator(OperatorCommand),
    Auth(AuthCommand),
    Workflow(WorkflowCommand),
    Transit(TransitCommand),
    Pki(PkiCommand),
    Backup(BackupCommand),
}

// ─── Ctl sub-commands ─────────────────────────────────────────────────────────

#[derive(Debug, Args)]
pub(crate) struct OperatorCommand {
    #[command(subcommand)]
    pub command: OperatorSubCommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum OperatorSubCommand {
    Init {
        #[arg(long, default_value_t = 5)]
        shares_total: u32,
        #[arg(long, default_value_t = 3)]
        threshold: u32,
    },
    SealStatus,
    Seal,
    Unseal {
        share: String,
    },
    RotateRootKey {
        #[arg(long, default_value_t = 5)]
        shares_total: u32,
        #[arg(long, default_value_t = 3)]
        threshold: u32,
    },
}

#[derive(Debug, Args)]
pub(crate) struct AuthCommand {
    #[command(subcommand)]
    pub command: AuthSubCommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum AuthSubCommand {
    Approle {
        #[command(subcommand)]
        command: AuthAppRoleSubCommand,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum AuthAppRoleSubCommand {
    Create {
        role_name: String,
        #[arg(long, required = true)]
        policy: Vec<String>,
        #[arg(long, default_value_t = 3600)]
        token_ttl_seconds: i64,
        #[arg(long, default_value_t = 86400)]
        secret_id_ttl_seconds: i64,
        #[arg(long, default_value_t = 10)]
        secret_id_num_uses: u32,
    },
    GenerateSecretId {
        role_id: String,
    },
    Login {
        role_id: String,
        secret_id: String,
    },
    Lookup {
        access_token: String,
    },
    Revoke {
        access_token: String,
    },
}

#[derive(Debug, Args)]
pub(crate) struct ClusterCommand {
    #[command(subcommand)]
    pub command: ClusterSubCommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum ClusterSubCommand {
    Status,
}

#[derive(Debug, Args)]
pub(crate) struct MemberCommand {
    #[command(subcommand)]
    pub command: MemberSubCommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum MemberSubCommand {
    Add {
        node_id: String,
        address: String,
    },
    Remove {
        node_id: String,
        #[arg(long, default_value_t = false)]
        force_unreachable: bool,
    },
}

#[derive(Debug, Args)]
pub(crate) struct LockCommand {
    #[command(subcommand)]
    pub command: LockSubCommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum LockSubCommand {
    List,
}

#[derive(Debug, Args)]
pub(crate) struct WorkflowCommand {
    #[command(subcommand)]
    pub command: WorkflowSubCommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum WorkflowSubCommand {
    Deploy {
        #[arg(long)]
        definition_id: Option<String>,
        /// Path to YAML definition file
        file: String,
    },
    Start {
        #[arg(long)]
        definition_id: String,
        #[arg(long, default_value = "")]
        namespace: String,
        #[arg(long, default_value = "")]
        version: String,
        #[arg(long, default_value = "{}")]
        input_json: String,
    },
    Get {
        instance_id: String,
    },
    List {
        #[arg(long, default_value = "")]
        namespace: String,
        #[arg(long, default_value = "")]
        definition_name: String,
    },
    Definitions {
        #[arg(long, default_value = "")]
        namespace: String,
    },
    Definition {
        definition_id: String,
        #[arg(long, default_value = "")]
        version: String,
    },
}

#[derive(Debug, Args)]
pub(crate) struct TransitCommand {
    #[command(subcommand)]
    pub command: TransitSubCommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum TransitSubCommand {
    CreateKey {
        key_name: String,
    },
    Encrypt {
        key_name: String,
        plaintext: String,
    },
    Decrypt {
        key_name: String,
        ciphertext: String,
    },
    RotateKey {
        key_name: String,
    },
    HmacSign {
        key_name: String,
        data: String,
    },
    HmacVerify {
        key_name: String,
        data: String,
        signature: String,
    },
}

#[derive(Debug, Args)]
pub(crate) struct PkiCommand {
    #[command(subcommand)]
    pub command: PkiSubCommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum PkiSubCommand {
    Issue {
        common_name: String,
        #[arg(long)]
        san: Vec<String>,
        #[arg(long, default_value_t = 86400)]
        ttl_seconds: i64,
        #[arg(long, default_value_t = false)]
        auto_renew: bool,
        #[arg(long, default_value_t = 3600)]
        renew_before_seconds: i64,
    },
    Renew {
        serial_number: String,
        #[arg(long, default_value_t = 86400)]
        ttl_seconds: i64,
    },
    Revoke {
        serial_number: String,
        #[arg(long, default_value = "unspecified")]
        reason: String,
    },
    CaChain,
    Crl {
        #[arg(long, default_value_t = 600)]
        next_update_seconds: i64,
    },
    Ocsp {
        serial_number: String,
    },
    SetAutoRenewPolicy {
        serial_number: String,
        #[arg(long, default_value_t = true)]
        enabled: bool,
        #[arg(long, default_value_t = 3600)]
        renew_before_seconds: i64,
    },
    RunAutoRenew,
    AcmeOrder {
        #[arg(long, required = true)]
        domain: Vec<String>,
        #[arg(long, default_value_t = 86400)]
        ttl_seconds: i64,
        #[arg(long, default_value = "http-01")]
        challenge_type: String,
        #[arg(long, default_value_t = true)]
        auto_renew: bool,
        #[arg(long, default_value_t = 3600)]
        renew_before_seconds: i64,
    },
    AcmeChallenge {
        order_id: String,
        domain: String,
        token: String,
    },
    AcmeFinalize {
        order_id: String,
        #[arg(long, default_value = "")]
        common_name: String,
    },
}

#[derive(Debug, Args)]
pub(crate) struct BackupCommand {
    #[command(subcommand)]
    pub command: BackupSubCommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum BackupSubCommand {
    Create {
        #[arg(long, default_value = "coord-backup.json")]
        file: String,
    },
    Restore {
        file: String,
    },
}

// ─── Shared helpers (from coord-server/cli.rs) ────────────────────────────────

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
