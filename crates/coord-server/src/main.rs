#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context;
use coord_core::security::{
    SecurityDomainSnapshot, SecurityRoleSnapshot, create_root_token_snapshot, generate_root_token,
};
use coord_core::state::{CoordinatorState, RuntimeConfig};
use coord_core::workflow::engine::InstanceStatus;
use coord_core::workflow::ports::WorkflowStore;
use coord_core::workflow::store::MemoryWorkflowStore;
use coord_proto::coord::v1::admin_service_server::AdminServiceServer;
use coord_proto::coord::v1::auth_service_server::AuthServiceServer;
use coord_proto::coord::v1::config_service_server::ConfigServiceServer;
use coord_proto::coord::v1::id_gen_service_server::IdGenServiceServer;
use coord_proto::coord::v1::lock_service_server::LockServiceServer;
use coord_proto::coord::v1::pki_service_server::PkiServiceServer;
use coord_proto::coord::v1::raft_internal_service_server::RaftInternalServiceServer;
use coord_proto::coord::v1::registry_service_server::RegistryServiceServer;
use coord_proto::coord::v1::seal_service_server::SealServiceServer;
use coord_proto::coord::v1::transit_service_server::TransitServiceServer;
use coord_proto::coord::v1::workflow_service_server::WorkflowServiceServer;
use tokio::time::{Duration, sleep};
use tonic::transport::Server;
use tracing::{error, info, warn};

mod application;
mod cli;
mod http_api;
mod interceptors;
mod persistence;
mod raft_internal;
mod raft_runtime;
mod raft_store;
mod services;
mod telemetry;
mod wire;
mod workflow_adapters;

use clap::Parser;
use cli::{
    Cli, Command, ServeArgs, init_tracing, load_unseal_shares_from_file, parse_peers,
    resolve_bootstrap_flag, resolve_node_id,
};
use interceptors::{CapabilityLayer, GrpcRateLimitLayer, GrpcRedMetricsLayer, SecurityGateway};
use raft_internal::RaftInternalGrpc;
use raft_runtime::{RAFT_TICK_INTERVAL, RaftRuntime};
use raft_store::RaftStore;

use application::config_app::ConfigApp;
use application::lock_app::LockApp;
use application::pki_app::PkiApp;
use application::transit_app::TransitApp;
use services::WorkflowGrpc;
use services::{
    AdminGrpc, AuthGrpc, ConfigGrpc, IdGenGrpc, LockGrpc, PkiGrpc, RegistryGrpc, SealGrpc,
    TransitGrpc,
};
use workflow_adapters::new_coord_workflow_runtime;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let (args, dev_mode) = match cli.command.unwrap_or(Command::Dev(ServeArgs {
        grpc_addr: "0.0.0.0:9090".to_string(),
        http_addr: "0.0.0.0:9091".to_string(),
        data_dir: "/tmp/coord-dev".to_string(),
        node_id: None,
        auto_unseal_shares_file: None,
        peers: String::new(),
        bootstrap: String::new(),
        tls_cert: None,
        tls_key: None,
        tls_client_ca: None,
        otlp_endpoint: None,
        dev_root_token: None,
    })) {
        Command::Dev(args) => (args, true),
        Command::Serve(args) => (args, false),
    };

    init_tracing(dev_mode);
    telemetry::init_telemetry(args.otlp_endpoint.as_deref());

    // Install rustls default crypto provider before any TLS backend (axum-server
    // or tonic) constructs a ServerConfig. rustls 0.23 deliberately removed
    // automatic provider selection when multiple crates pull it in transitively.
    // Safe to call unconditionally — idempotent, returns Err if already set.
    if rustls::crypto::ring::default_provider()
        .install_default()
        .is_err()
    {
        // Another call site already set a provider; that's fine.
    }

    let grpc_addr = SocketAddr::from_str(&args.grpc_addr)
        .with_context(|| format!("invalid grpc_addr: {}", args.grpc_addr))?;
    let http_addr = SocketAddr::from_str(&args.http_addr)
        .with_context(|| format!("invalid http_addr: {}", args.http_addr))?;

    let data_dir = PathBuf::from(args.data_dir);
    std::fs::create_dir_all(&data_dir)
        .with_context(|| format!("failed to create data dir: {}", data_dir.display()))?;
    let node_id = resolve_node_id(args.node_id, &data_dir)?;

    let state = CoordinatorState::new(RuntimeConfig {
        node_id: node_id.clone(),
        data_dir,
        dev_mode,
    })
    .context("failed to initialize coordinator state")?;

    let raft_store = RaftStore::open(
        &state.runtime().data_dir,
        &state.runtime().node_id,
        &args.grpc_addr,
    )
    .context("failed to initialize OpenRaft+Redb store")?;
    let raft_runtime = RaftRuntime::new(state.clone(), raft_store.clone(), args.grpc_addr.clone());
    let workflow_store = Arc::new(MemoryWorkflowStore::new());
    let workflow_runtime = Arc::new(new_coord_workflow_runtime(workflow_store.clone()));

    // Register all ReplicatedModules before snapshot restore and committed-log replay.
    // BusinessCommand entries cannot be replayed safely until their namespace is present.
    raft_runtime.register_module(state.config().clone()).await;
    raft_runtime.register_module(state.locks().clone()).await;
    raft_runtime.register_module(state.registry().clone()).await;
    raft_runtime.register_module(state.transit().clone()).await;
    raft_runtime.register_module(state.pki().clone()).await;
    raft_runtime.register_module(workflow_store.clone()).await;

    // ── Cluster auto-join: decide bootstrap role and probe peer node ids ──
    let peer_addrs = parse_peers(&args.peers);
    let is_bootstrap = resolve_bootstrap_flag(&args.bootstrap, peer_addrs.is_empty());
    if !peer_addrs.is_empty() {
        info!(peers = ?peer_addrs, bootstrap = is_bootstrap, "cluster peers configured");
    }

    let raft_metadata = raft_store
        .load_metadata()
        .context("failed to load raft metadata from redb")?;
    let raft_bootstrap = raft_store
        .load_bootstrap()
        .context("failed to load raft bootstrap metadata from redb")?;
    state
        .metrics()
        .raft_log_commit_index
        .set(raft_metadata.commit_index as i64);

    info!(
        path = %raft_store.db_path().display(),
        cluster_name = %raft_store.raft_config.cluster_name,
        commit_index = raft_metadata.commit_index,
        "initialized OpenRaft + Redb metadata store"
    );
    if let Some(bootstrap) = raft_bootstrap {
        info!(
            bootstrap_node_id = %bootstrap.node_id,
            bootstrap_addr = %bootstrap.node.addr,
            "loaded raft bootstrap metadata"
        );
    }

    let snapshot_file = state.runtime().data_dir.join("state_snapshot.json");
    restore_runtime_state(&state, &snapshot_file, &raft_store, &raft_runtime).await;
    if is_bootstrap {
        raft_runtime.initialize_local_member().await;
    } else {
        info!("non-bootstrap node: deferring local member init until leader contact");
    }
    if let Err(err) = raft_runtime.replay_committed_entries_on_startup().await {
        error!(error = %err, "failed to replay committed raft log entries on startup");
    }

    // Dev auto-init: initialise the security domain with a (optionally fixed)
    // root token and immediately unseal on first startup.  Idempotent across
    // restarts because the share is persisted next to the data directory.
    if dev_mode {
        maybe_dev_auto_init_and_unseal(&state, args.dev_root_token.as_deref())
            .await
            .context("dev auto-init-and-unseal failed")?;
    }

    // A'4: warn when auto-unseal is enabled in production (non-dev) mode.
    // K8s node-drift auto-unseal is a valid use case, but operators must
    // acknowledge the security implications.
    if !dev_mode && args.auto_unseal_shares_file.is_some() {
        warn!(
            path = ?args.auto_unseal_shares_file,
            "auto-unseal shares file is configured in PRODUCTION mode. \
             Shamir shares will be read from disk on every startup. \
             Ensure the file has strict permissions (0400) and is only \
             accessible by the coord-server process."
        );
    }

    maybe_auto_unseal_from_file(&state, args.auto_unseal_shares_file.as_deref())
        .await
        .context("failed to auto-unseal from shares file")?;

    let tls_paths = coord_core::tls::TlsPaths {
        cert: args.tls_cert.clone(),
        key: args.tls_key.clone(),
        client_ca: args.tls_client_ca.clone(),
    };
    let tls_material =
        coord_core::tls::load_tls_material(&tls_paths).context("failed to load TLS material")?;
    let tls_enabled = tls_material.is_some();
    let mtls_required = tls_material
        .as_ref()
        .map(|m| m.mtls_required())
        .unwrap_or(false);

    info!(node_id = %node_id, dev_mode, "starting coord-server");
    info!(
        grpc_addr = %grpc_addr,
        http_addr = %http_addr,
        tls = tls_enabled,
        mtls = mtls_required,
        "listening endpoints"
    );

    spawn_housekeeping(state.clone());
    spawn_raft_tick_loop(raft_runtime.clone());
    spawn_snapshot_persist(state.clone(), raft_store.clone(), raft_runtime.clone());
    spawn_workflow_timer_driver(raft_runtime.clone(), workflow_runtime.clone());

    // ── Build application facades ───────────────────────────────────────────
    let config_app = ConfigApp::new(
        state.config().clone(),
        state.metrics().clone(),
        raft_runtime.clone(),
    );
    let transit_app = TransitApp::new(
        state.transit().clone(),
        state.metrics().clone(),
        raft_runtime.clone(),
    );
    let pki_app = PkiApp::new(
        state.pki().clone(),
        state.metrics().clone(),
        raft_runtime.clone(),
    );

    spawn_pki_auto_renew_loop(state.clone(), pki_app.clone(), raft_runtime.clone());

    if is_bootstrap && !peer_addrs.is_empty() {
        spawn_cluster_auto_join(raft_runtime.clone(), peer_addrs.clone());
    }
    let ui_dist_dir = http_api::resolve_ui_dist_dir();
    spawn_http_control_plane(
        state.clone(),
        raft_runtime.clone(),
        config_app,
        transit_app.clone(),
        pki_app.clone(),
        http_addr,
        ui_dist_dir,
        tls_material.clone(),
    )
    .await?;

    let shutdown_state = state.clone();
    let shutdown_raft_store = raft_store.clone();
    let shutdown_raft_runtime = raft_runtime.clone();

    let security_gw = SecurityGateway::new(state.security().clone(), state.metrics().clone());

    let mut builder = Server::builder();
    if let Some(material) = tls_material.as_ref() {
        let mut tls = tonic::transport::ServerTlsConfig::new().identity(
            tonic::transport::Identity::from_pem(material.cert_pem(), material.key_pem()),
        );
        if let Some(ca) = material.client_ca_pem() {
            tls = tls.client_ca_root(tonic::transport::Certificate::from_pem(ca));
        }
        builder = builder
            .tls_config(tls)
            .context("invalid tonic TLS config")?;
    }

    let grpc_router = builder
        .layer(GrpcRedMetricsLayer::new(state.metrics().clone()))
        .layer(GrpcRateLimitLayer::new())
        .layer(CapabilityLayer::new(security_gw))
        .add_service(RegistryServiceServer::new(RegistryGrpc::new(
            state.registry().clone(),
            state.metrics().clone(),
            raft_runtime.clone(),
        )))
        .add_service(ConfigServiceServer::new(ConfigGrpc::new(ConfigApp::new(
            state.config().clone(),
            state.metrics().clone(),
            raft_runtime.clone(),
        ))))
        .add_service(LockServiceServer::new(LockGrpc::new(LockApp::new(
            state.locks().clone(),
            state.metrics().clone(),
            raft_runtime.clone(),
        ))))
        .add_service(SealServiceServer::new(SealGrpc::new(
            state.security().clone(),
            state.domain_lifecycle().clone(),
            state.metrics().clone(),
        )))
        .add_service(AuthServiceServer::new(AuthGrpc::new(
            state.security().clone(),
            state.metrics().clone(),
        )))
        .add_service(AdminServiceServer::new(AdminGrpc::new(
            state.members().clone(),
            state.locks().clone(),
            state.runtime().clone(),
            state.metrics().clone(),
            raft_runtime.clone(),
        )));

    let grpc_router = grpc_router.add_service(WorkflowServiceServer::new(WorkflowGrpc::new(
        state.metrics().clone(),
        raft_runtime.clone(),
        workflow_runtime.clone(),
    )));

    grpc_router
        .add_service(TransitServiceServer::new(TransitGrpc::new(transit_app)))
        .add_service(PkiServiceServer::new(PkiGrpc::new(pki_app.clone())))
        .add_service(RaftInternalServiceServer::new(RaftInternalGrpc::new(
            raft_runtime,
        )))
        .add_service(IdGenServiceServer::new(IdGenGrpc::new(
            state.idgen().clone(),
            state.metrics().clone(),
        )))
        .serve_with_shutdown(
            grpc_addr,
            shutdown_signal(shutdown_state, shutdown_raft_store, shutdown_raft_runtime),
        )
        .await
        .context("gRPC server failed")
}

async fn shutdown_signal(state: CoordinatorState, raft_store: RaftStore, raft: RaftRuntime) {
    #[cfg(unix)]
    {
        let mut terminate = match tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::terminate(),
        ) {
            Ok(signal) => signal,
            Err(err) => {
                error!(error = %err, "failed to register SIGTERM handler, falling back to ctrl-c only");
                let _ = tokio::signal::ctrl_c().await;
                info!("shutdown signal received, flushing runtime snapshot");
                persist_runtime_snapshot(&state, &raft_store, &raft, "shutdown").await;
                return;
            }
        };

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate.recv() => {},
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }

    info!("shutdown signal received, flushing runtime snapshot");
    persist_runtime_snapshot(&state, &raft_store, &raft, "shutdown").await;
}

fn spawn_housekeeping(state: CoordinatorState) {
    tokio::spawn(async move {
        loop {
            state.registry().cleanup_expired().await;
            state.locks().cleanup_expired().await;

            let service_count = state.registry().service_count().await as i64;
            let lock_count = state.locks().list_holders().await.len() as i64;
            state
                .metrics()
                .coord_services_registered_total
                .set(service_count);
            state.metrics().coord_locks_held.set(lock_count);

            sleep(Duration::from_secs(1)).await;
        }
    });
}

fn spawn_raft_tick_loop(raft: RaftRuntime) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(RAFT_TICK_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            ticker.tick().await;
            raft.tick().await;
        }
    });
}

fn spawn_pki_auto_renew_loop(state: CoordinatorState, pki_app: PkiApp, raft: RaftRuntime) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(15));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            ticker.tick().await;

            if raft.role_label().await != "Leader" {
                continue;
            }

            let security_status = state.security().seal_status().await;
            if security_status.initialized && security_status.sealed {
                state.metrics().coord_security_sealed.set(1);
                continue;
            }
            state.metrics().coord_security_sealed.set(0);

            let execution = pki_app.run_auto_renew().await;
            if !execution.errors.is_empty() {
                warn!(errors = ?execution.errors, "pki auto-renew completed with errors");
            }

            for renewed in execution.renewed {
                info!(
                    old_serial = %renewed.old_serial_number,
                    new_serial = %renewed.new_serial_number,
                    common_name = %renewed.common_name,
                    "auto-renewed pki certificate"
                );
            }
        }
    });
}

fn current_unix_ms() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_millis().min(i64::MAX as u128) as i64,
        Err(_) => 0,
    }
}

fn spawn_workflow_timer_driver(
    raft: RaftRuntime,
    runtime: Arc<workflow_adapters::CoordWorkflowRuntime>,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            ticker.tick().await;

            if raft.role_label().await != "Leader" {
                continue;
            }

            let now_ms = current_unix_ms();
            let store = runtime.store();
            let instances = match store.list_instances().await {
                Ok(instances) => instances,
                Err(err) => {
                    warn!(error = %err, "workflow timer driver failed to list instances");
                    continue;
                }
            };

            for instance in instances {
                if !matches!(instance.status, InstanceStatus::Suspended) {
                    continue;
                }
                let Some(meta) = instance.suspension_meta.as_ref() else {
                    continue;
                };
                if meta.reason != "wait" {
                    continue;
                }
                let Some(until_ms) = meta.until_ms else {
                    continue;
                };
                if until_ms > now_ms {
                    continue;
                }

                let planned = match runtime
                    .resume_detached(&instance.id, serde_json::json!({}))
                    .await
                {
                    Ok(instance) => instance,
                    Err(err) => {
                        warn!(instance_id = %instance.id, error = %err, "workflow timer resume failed");
                        continue;
                    }
                };

                if let Err(err) = raft
                    .propose_business_command(
                        "workflow",
                        MemoryWorkflowStore::encode_upsert_instance_bytes(&planned),
                    )
                    .await
                {
                    warn!(instance_id = %instance.id, error = %err, "workflow timer resume proposal failed");
                }
            }
        }
    });
}

fn spawn_snapshot_persist(state: CoordinatorState, raft_store: RaftStore, raft: RaftRuntime) {
    tokio::spawn(async move {
        loop {
            persist_runtime_snapshot(&state, &raft_store, &raft, "periodic_snapshot").await;
            sleep(Duration::from_secs(5)).await;
        }
    });
}

/// Background task: when this node is the cluster bootstrap leader, repeatedly probe
/// each peer's `AdminService/ClusterStatus` to learn its `node_id`, and then propose a
/// Raft membership add for each peer. The task exits once every peer is part of the
/// committed membership.
fn spawn_cluster_auto_join(raft: RaftRuntime, peer_addrs: Vec<String>) {
    use coord_proto::coord::v1::ClusterStatusRequest;
    use coord_proto::coord::v1::admin_service_client::AdminServiceClient;

    tokio::spawn(async move {
        let mut joined: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        loop {
            let known_members = raft.snapshot_members().await;
            let mut still_pending = false;

            for addr in &peer_addrs {
                if joined.values().any(|a| a == addr) {
                    continue;
                }
                let url = if addr.starts_with("http://") || addr.starts_with("https://") {
                    addr.clone()
                } else {
                    format!("http://{}", addr)
                };

                let client_result = tokio::time::timeout(
                    Duration::from_secs(3),
                    AdminServiceClient::connect(url.clone()),
                )
                .await;

                let mut client = match client_result {
                    Ok(Ok(c)) => c,
                    Ok(Err(err)) => {
                        still_pending = true;
                        warn!(peer = %addr, error = %err, "auto-join: peer unreachable, will retry");
                        continue;
                    }
                    Err(_) => {
                        still_pending = true;
                        warn!(peer = %addr, "auto-join: peer connect timed out, will retry");
                        continue;
                    }
                };

                let status_result = tokio::time::timeout(
                    Duration::from_secs(3),
                    client.cluster_status(ClusterStatusRequest {}),
                )
                .await;

                let peer_node_id = match status_result {
                    Ok(Ok(resp)) => resp.into_inner().node_id,
                    _ => {
                        still_pending = true;
                        warn!(peer = %addr, "auto-join: cluster_status probe failed, will retry");
                        continue;
                    }
                };

                if peer_node_id.trim().is_empty() {
                    still_pending = true;
                    warn!(peer = %addr, "auto-join: peer reported empty node_id, will retry");
                    continue;
                }

                if known_members.contains_key(&peer_node_id) {
                    joined.insert(peer_node_id.clone(), addr.clone());
                    continue;
                }

                match raft
                    .propose_member_add(peer_node_id.clone(), addr.clone())
                    .await
                {
                    Ok((added, members)) => {
                        info!(
                            peer = %addr,
                            peer_node_id = %peer_node_id,
                            added,
                            members = ?members,
                            "auto-join: proposed member add"
                        );
                        joined.insert(peer_node_id.clone(), addr.clone());
                    }
                    Err(err) => {
                        still_pending = true;
                        warn!(peer = %addr, peer_node_id = %peer_node_id, error = %err, "auto-join: propose_member_add failed, will retry");
                    }
                }
            }

            if !still_pending {
                info!(joined = ?joined, "cluster auto-join completed");
                return;
            }

            sleep(Duration::from_secs(2)).await;
        }
    });
}

async fn persist_runtime_snapshot(
    state: &CoordinatorState,
    raft_store: &RaftStore,
    raft: &RaftRuntime,
    reason: &str,
) {
    let mut payload = match persistence::snapshot_payload_v5(state).await {
        Ok(p) => p,
        Err(err) => {
            error!(
                error = %err,
                reason = %reason,
                "failed to collect v5 runtime snapshot"
            );
            return;
        }
    };
    match raft.snapshot_extra_modules().await {
        Ok(extra_modules) => {
            for (namespace, bytes) in extra_modules {
                payload.modules.insert(namespace, bytes);
            }
        }
        Err(err) => {
            error!(
                error = %err,
                reason = %reason,
                "failed to collect extra replicated module snapshots"
            );
            return;
        }
    }
    match raft_store.load_metadata() {
        Ok(metadata) => {
            persistence::annotate_payload_v5_with_raft_metadata(
                &mut payload,
                metadata.commit_index,
                metadata.last_applied_index,
            );
        }
        Err(err) => {
            error!(
                error = %err,
                reason = %reason,
                "failed to load raft metadata for snapshot annotation"
            );
        }
    }

    match raft_store.save_runtime_snapshot(&payload) {
        Ok(()) => match raft_store.load_metadata() {
            Ok(metadata) => {
                state
                    .metrics()
                    .raft_log_commit_index
                    .set(metadata.commit_index as i64);
                info!(
                    reason = %reason,
                    commit_index = metadata.commit_index,
                    "persisted runtime snapshot to redb"
                );
            }
            Err(err) => {
                error!(
                    error = %err,
                    reason = %reason,
                    "failed to reload raft metadata after snapshot persistence"
                );
            }
        },
        Err(err) => {
            error!(
                error = %err,
                reason = %reason,
                "failed to persist runtime snapshot to redb"
            );
        }
    }
}

async fn restore_runtime_state(
    state: &CoordinatorState,
    snapshot_file: &Path,
    raft_store: &RaftStore,
    raft: &RaftRuntime,
) {
    match raft_store.load_runtime_snapshot() {
        Ok(Some(payload)) => {
            let modules = payload.modules.clone();
            match persistence::restore_payload_v5(state, payload).await {
                Ok(()) => {
                    if let Err(err) = raft.restore_extra_modules(&modules).await {
                        error!(error = %err, "failed to restore extra replicated module snapshots from redb");
                    }
                    info!(path = %raft_store.db_path().display(), "restored runtime snapshot from redb");
                    return;
                }
                Err(err) => {
                    error!(
                        error = %err,
                        path = %raft_store.db_path().display(),
                        "failed to restore runtime snapshot from redb, falling back to file"
                    );
                }
            }
        }
        Ok(None) => {
            info!(path = %raft_store.db_path().display(), "no runtime snapshot found in redb");
        }
        Err(err) => {
            error!(
                error = %err,
                path = %raft_store.db_path().display(),
                "failed to load runtime snapshot from redb, falling back to file"
            );
        }
    }

    match std::fs::read_to_string(snapshot_file) {
        Ok(payload_json) => match persistence::payload_v5_from_json(&payload_json) {
            Ok(payload) => {
                let modules = payload.modules.clone();
                match persistence::restore_payload_v5(state, payload).await {
                    Ok(()) => {
                        if let Err(err) = raft.restore_extra_modules(&modules).await {
                            error!(error = %err, "failed to restore extra replicated module snapshots from disk");
                        }
                        info!(path = %snapshot_file.display(), "restored runtime snapshot from disk");

                        match persistence::snapshot_payload_v5(state).await {
                            Ok(mut payload) => {
                                match raft.snapshot_extra_modules().await {
                                    Ok(extra_modules) => {
                                        for (namespace, bytes) in extra_modules {
                                            payload.modules.insert(namespace, bytes);
                                        }
                                    }
                                    Err(err) => {
                                        error!(error = %err, path = %raft_store.db_path().display(), "failed to collect extra module snapshots for redb mirror");
                                        return;
                                    }
                                }
                                match raft_store.load_metadata() {
                                    Ok(metadata) => {
                                        persistence::annotate_payload_v5_with_raft_metadata(
                                            &mut payload,
                                            metadata.commit_index,
                                            metadata.last_applied_index,
                                        );
                                    }
                                    Err(err) => {
                                        error!(
                                            error = %err,
                                            path = %raft_store.db_path().display(),
                                            "failed to load raft metadata while mirroring file snapshot"
                                        );
                                    }
                                }
                                if let Err(err) = raft_store.save_runtime_snapshot(&payload) {
                                    error!(
                                        error = %err,
                                        path = %raft_store.db_path().display(),
                                        "failed to mirror file snapshot into redb"
                                    );
                                }
                            }
                            Err(err) => {
                                error!(
                                    error = %err,
                                    path = %raft_store.db_path().display(),
                                    "failed to collect v5 snapshot for redb mirror"
                                );
                            }
                        }
                    }
                    Err(err) => {
                        error!(
                            error = %err,
                            path = %snapshot_file.display(),
                            "failed to restore runtime snapshot from disk"
                        );
                    }
                }
            }
            Err(err) => {
                error!(
                    error = %err,
                    path = %snapshot_file.display(),
                    "failed to parse runtime snapshot from disk"
                );
            }
        },
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            info!(path = %snapshot_file.display(), "no runtime snapshot found on disk");
        }
        Err(err) => {
            error!(
                error = %err,
                path = %snapshot_file.display(),
                "failed to read runtime snapshot from disk"
            );
        }
    }
}

async fn maybe_auto_unseal_from_file(
    state: &CoordinatorState,
    shares_file: Option<&Path>,
) -> anyhow::Result<()> {
    let Some(shares_file) = shares_file else {
        return Ok(());
    };

    let status = state.security().seal_status().await;
    state
        .metrics()
        .coord_security_sealed
        .set(if status.sealed { 1 } else { 0 });

    if !status.initialized {
        info!(
            path = %shares_file.display(),
            "auto-unseal shares file is configured but security domain is not initialized; skipping"
        );
        return Ok(());
    }

    if !status.sealed {
        info!(
            path = %shares_file.display(),
            "security domain already unsealed; skipping auto-unseal"
        );
        return Ok(());
    }

    let shares = load_unseal_shares_from_file(shares_file)?;
    let share_count = shares.len();

    info!(
        path = %shares_file.display(),
        shares = share_count,
        threshold = status.threshold,
        "attempting startup auto-unseal from shares file"
    );

    for share in shares {
        state.metrics().coord_security_unseal_attempts_total.inc();

        match state.security().unseal(&share).await {
            Ok(unseal_status) => {
                info!(
                    progress = unseal_status.progress,
                    threshold = unseal_status.threshold,
                    sealed = unseal_status.sealed,
                    "auto-unseal share accepted"
                );

                if !unseal_status.sealed {
                    if let Some(domain) = state.security().take_unsealed_domain_snapshot().await {
                        restore_runtime_security_domain(state, domain).await?;
                    }
                    state.metrics().coord_security_sealed.set(0);
                    info!("startup auto-unseal completed");
                    return Ok(());
                }
            }
            Err(err) => {
                warn!(error = %err, "auto-unseal share rejected");
            }
        }
    }

    let final_status = state.security().seal_status().await;
    state
        .metrics()
        .coord_security_sealed
        .set(if final_status.sealed { 1 } else { 0 });

    if final_status.sealed {
        return Err(anyhow::anyhow!(
            "auto-unseal did not reach threshold with configured shares file {} (shares: {}, threshold: {})",
            shares_file.display(),
            share_count,
            final_status.threshold
        ));
    }

    Ok(())
}

/// Dev-mode auto-init + auto-unseal.
///
/// On the **first** startup the security domain is not yet initialised.  This
/// function initialises it with a 1-of-1 Shamir split so that no manual
/// `unseal` call is required, and embeds a root token that callers can
/// hard-code in tests.
///
/// The single unseal share is written to `<data_dir>/dev-unseal.share`
/// (mode 0600) and the root token to `<data_dir>/dev-root-token.txt`
/// (mode 0600).  On subsequent restarts the function reads the share from
/// disk and unseals automatically, so the data directory must persist across
/// container/process restarts if token stability is required.
///
/// `requested_root_token` — when `Some`, the token value is used as-is;
/// when `None`, a cryptographically random token is generated instead and
/// printed to the INFO log so operators can copy it out.
async fn maybe_dev_auto_init_and_unseal(
    state: &CoordinatorState,
    requested_root_token: Option<&str>,
) -> anyhow::Result<()> {
    let data_dir = state.runtime().data_dir.clone();
    let share_file = data_dir.join("dev-unseal.share");
    let token_file = data_dir.join("dev-root-token.txt");

    let status = state.security().seal_status().await;

    if !status.initialized {
        // First startup — initialise the domain.
        let root_token = match requested_root_token {
            Some(t) if !t.trim().is_empty() => t.trim().to_string(),
            _ => generate_root_token(),
        };

        // Build a domain snapshot that includes the root token so it
        // survives seal / unseal cycles (mirrors SealGrpc::do_init).
        let root_token_snapshot = create_root_token_snapshot(&root_token, 86400 * 365);
        let mut domain = state
            .domain_lifecycle()
            .capture(state.security().export_auth_state_snapshot().await)
            .await;
        domain.auth.roles.push(SecurityRoleSnapshot {
            role_id: "root".to_string(),
            role_name: "root".to_string(),
            policies: vec!["*".to_string()],
            token_ttl_seconds: 86400 * 365,
            secret_id_ttl_seconds: 86400 * 365,
            secret_id_num_uses: 0,
        });
        domain.auth.access_tokens.push(root_token_snapshot);

        let shares = state
            .security()
            .init_security_with_domain(1, 1, domain)
            .await
            .map_err(anyhow::Error::msg)
            .context("dev auto-init: init_security_with_domain failed")?;

        // Clear runtime-protected modules right after init (same as SealGrpc::do_init).
        state
            .domain_lifecycle()
            .clear()
            .await
            .map_err(anyhow::Error::msg)
            .context("dev auto-init: domain clear failed")?;
        state.metrics().coord_security_sealed.set(1);

        let share = shares
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("dev auto-init: no shares returned"))?;

        // Persist share and root token (0600).
        write_secret_file(&share_file, &share)
            .context("dev auto-init: failed to write dev-unseal.share")?;
        write_secret_file(&token_file, &root_token)
            .context("dev auto-init: failed to write dev-root-token.txt")?;

        info!(
            root_token = %root_token,
            share_file = %share_file.display(),
            token_file = %token_file.display(),
            "dev security domain initialised (1-of-1 Shamir)"
        );
    }

    // (Re-)unseal using the persisted share — covers both first boot after
    // init above and subsequent restarts.
    let status = state.security().seal_status().await;
    if !status.sealed {
        info!("dev security domain already unsealed; skipping auto-unseal");
        return Ok(());
    }

    let share =
        std::fs::read_to_string(&share_file).with_context(|| {
            format!(
                "dev auto-unseal: cannot read share file {}; \
                 wipe the data directory to reinitialise",
                share_file.display()
            )
        })?;
    let share = share.trim().to_string();

    state.metrics().coord_security_unseal_attempts_total.inc();
    let unseal_status = state
        .security()
        .unseal(&share)
        .await
        .map_err(anyhow::Error::msg)
        .context("dev auto-unseal: unseal call failed")?;

    if unseal_status.sealed {
        return Err(anyhow::anyhow!(
            "dev auto-unseal: domain still sealed after submitting the share"
        ));
    }

    if let Some(domain) = state.security().take_unsealed_domain_snapshot().await {
        restore_runtime_security_domain(state, domain).await?;
    }
    state.metrics().coord_security_sealed.set(0);
    info!(
        token_file = %token_file.display(),
        "dev security domain unsealed"
    );
    Ok(())
}

/// Write `content` to `path` with permissions 0600, creating or truncating.
fn write_secret_file(path: &Path, content: &str) -> anyhow::Result<()> {
    use std::fs::OpenOptions;
    use std::io::Write;
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)
            .with_context(|| format!("open {}", path.display()))?;
        f.write_all(content.as_bytes())
            .with_context(|| format!("write {}", path.display()))?;
    }
    #[cfg(not(unix))]
    {
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .with_context(|| format!("open {}", path.display()))?;
        f.write_all(content.as_bytes())
            .with_context(|| format!("write {}", path.display()))?;
    }
    Ok(())
}

async fn restore_runtime_security_domain(
    state: &CoordinatorState,
    snapshot: SecurityDomainSnapshot,
) -> anyhow::Result<()> {
    state
        .domain_lifecycle()
        .restore_domain(snapshot)
        .await
        .map_err(anyhow::Error::msg)
}

#[allow(clippy::too_many_arguments)]
async fn spawn_http_control_plane(
    state: CoordinatorState,
    raft: RaftRuntime,
    config_app: ConfigApp,
    transit_app: TransitApp,
    pki_app: PkiApp,
    http_addr: SocketAddr,
    ui_dist_dir: PathBuf,
    tls: Option<coord_core::tls::TlsMaterial>,
) -> anyhow::Result<()> {
    let app =
        http_api::build_http_router(state, raft, config_app, transit_app, pki_app, ui_dist_dir);

    match tls {
        None => {
            let listener = tokio::net::TcpListener::bind(http_addr)
                .await
                .with_context(|| {
                    format!("failed to bind http control plane endpoint: {http_addr}")
                })?;
            tokio::spawn(async move {
                if let Err(err) = axum::serve(listener, app).await {
                    error!(error = %err, "http control plane exited");
                }
            });
        }
        Some(material) => {
            let rustls_cfg = axum_server::tls_rustls::RustlsConfig::from_pem(
                material.cert_pem().to_vec(),
                material.key_pem().to_vec(),
            )
            .await
            .context("failed to build axum rustls config")?;
            // NOTE: Client-CA enforcement for axum mTLS requires a custom
            // rustls ServerConfig; axum-server's high-level API today supports
            // server cert only. When --tls-client-ca is set for HTTP, mTLS is
            // enforced at the gRPC (tonic) listener — HTTP currently logs the
            // intent. This is tracked as Batch 3c follow-up.
            if material.client_ca_pem().is_some() {
                warn!(
                    "HTTP listener received mTLS CA bundle but axum-server high-level API only \
                     enforces server cert; mTLS is active on gRPC only. Clients hitting the HTTP \
                     plane must still present a valid operator token."
                );
            }
            tokio::spawn(async move {
                if let Err(err) = axum_server::bind_rustls(http_addr, rustls_cfg)
                    .serve(app.into_make_service())
                    .await
                {
                    error!(error = %err, "https control plane exited");
                }
            });
        }
    }

    Ok(())
}
