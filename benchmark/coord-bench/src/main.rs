#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use clap::{ArgAction, Parser};
use coord_proto::coord::v1::admin_service_client::AdminServiceClient;
use coord_proto::coord::v1::config_service_client::ConfigServiceClient;
use coord_proto::coord::v1::id_gen_service_client::IdGenServiceClient;
use coord_proto::coord::v1::lock_service_client::LockServiceClient;
use coord_proto::coord::v1::pki_service_client::PkiServiceClient;
use coord_proto::coord::v1::registry_service_client::RegistryServiceClient;
use coord_proto::coord::v1::transit_service_client::TransitServiceClient;
use coord_proto::coord::v1::{
    BackupCreateRequest, BackupRestoreRequest, CheckCertificateStatusRequest, ConfigRequest,
    DecryptRequest, EncryptRequest, IssueCertificateRequest, LockAcquireRequest,
    LockReleaseRequest, PutConfigRequest, RegisterRequest, RevokeCertificateRequest,
    ServiceInstance, SnowflakeRequest,
};
use serde::Serialize;
use tonic::transport::{Channel, Endpoint};

const ALL_SCENARIOS: [&str; 7] = [
    "registry_register",
    "config_put_get",
    "lock_acquire_release",
    "idgen_generate",
    "transit_encrypt_decrypt",
    "pki_issue_revoke",
    "pki_ocsp_status",
];

#[derive(Debug, Parser)]
#[command(
    name = "coord-bench",
    version,
    about = "Coord service benchmark runner"
)]
struct Cli {
    #[arg(long, default_value = "http://127.0.0.1:9090")]
    endpoint: String,
    #[arg(long, default_value_t = 20)]
    duration_seconds: u64,
    #[arg(long, default_value_t = 16)]
    concurrency: u32,
    #[arg(long, default_value_t = 0)]
    max_ops_per_worker: u64,
    #[arg(long, default_value_t = 128)]
    config_key_space: u32,
    #[arg(long, default_value_t = true, action = ArgAction::Set)]
    restore_after_run: bool,
    #[arg(long, default_value = "all")]
    scenarios: String,
    #[arg(long, default_value = "benchmark/reports")]
    output_dir: PathBuf,
    #[arg(long, default_value_t = 5)]
    connect_timeout_seconds: u64,
}

#[derive(Debug, Default)]
struct WorkerResult {
    success: u64,
    errors: u64,
    latencies_us: Vec<u64>,
    first_error: Option<String>,
}

#[derive(Debug, Serialize)]
struct BenchmarkReport {
    generated_at_unix_seconds: i64,
    endpoint: String,
    requested_duration_seconds: u64,
    concurrency: u32,
    max_ops_per_worker: u64,
    scenarios: Vec<String>,
    results: Vec<ScenarioReport>,
    summary: SuiteSummary,
}

#[derive(Debug, Serialize)]
struct SuiteSummary {
    total_success: u64,
    total_errors: u64,
    total_measured_seconds: f64,
    aggregate_success_rps: f64,
    slowest_p95_scenario: String,
}

#[derive(Debug, Serialize)]
struct ScenarioReport {
    name: String,
    duration_seconds: f64,
    concurrency: u32,
    success: u64,
    errors: u64,
    success_rps: f64,
    latency_ms: LatencyStats,
    sample_errors: Vec<String>,
}

#[derive(Debug, Serialize)]
struct LatencyStats {
    min: f64,
    avg: f64,
    p50: f64,
    p90: f64,
    p95: f64,
    p99: f64,
    max: f64,
}

impl LatencyStats {
    fn from_samples(latencies_us: &mut [u64]) -> Self {
        if latencies_us.is_empty() {
            return Self {
                min: 0.0,
                avg: 0.0,
                p50: 0.0,
                p90: 0.0,
                p95: 0.0,
                p99: 0.0,
                max: 0.0,
            };
        }

        latencies_us.sort_unstable();
        let total_us: u128 = latencies_us.iter().map(|value| *value as u128).sum();
        let len = latencies_us.len() as f64;

        Self {
            min: us_to_ms(latencies_us[0]),
            avg: (total_us as f64 / len) / 1000.0,
            p50: percentile_ms(latencies_us, 50.0),
            p90: percentile_ms(latencies_us, 90.0),
            p95: percentile_ms(latencies_us, 95.0),
            p99: percentile_ms(latencies_us, 99.0),
            max: us_to_ms(*latencies_us.last().unwrap_or(&0)),
        }
    }
}

fn percentile_ms(sorted_samples_us: &[u64], percentile: f64) -> f64 {
    if sorted_samples_us.is_empty() {
        return 0.0;
    }

    let rank = ((percentile / 100.0) * sorted_samples_us.len() as f64).ceil() as usize;
    let idx = rank.saturating_sub(1).min(sorted_samples_us.len() - 1);
    us_to_ms(sorted_samples_us[idx])
}

fn us_to_ms(value_us: u64) -> f64 {
    value_us as f64 / 1000.0
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    if cli.duration_seconds == 0 {
        bail!("duration_seconds must be greater than 0");
    }
    if cli.concurrency == 0 {
        bail!("concurrency must be greater than 0");
    }
    if cli.config_key_space == 0 {
        bail!("config_key_space must be greater than 0");
    }

    let selected_scenarios = parse_scenarios(&cli.scenarios)?;
    let channel = connect_channel(&cli).await?;

    let baseline_payload_json = if cli.restore_after_run {
        println!("capturing baseline backup before benchmark run");
        Some(capture_backup_payload_json(channel.clone()).await?)
    } else {
        None
    };

    let run_result: Result<BenchmarkReport> = async {
        let mut results = Vec::new();
        for scenario in &selected_scenarios {
            let report = run_scenario(scenario, channel.clone(), &cli).await?;
            println!(
                "scenario={} success={} errors={} rps={:.2} p95={:.2}ms",
                report.name,
                report.success,
                report.errors,
                report.success_rps,
                report.latency_ms.p95
            );
            results.push(report);
        }

        let suite_summary = summarize_suite(&results);
        let generated_at_unix_seconds = now_unix_seconds();

        Ok(BenchmarkReport {
            generated_at_unix_seconds,
            endpoint: cli.endpoint.clone(),
            requested_duration_seconds: cli.duration_seconds,
            concurrency: cli.concurrency,
            max_ops_per_worker: cli.max_ops_per_worker,
            scenarios: selected_scenarios.clone(),
            results,
            summary: suite_summary,
        })
    }
    .await;

    if let Some(payload_json) = baseline_payload_json {
        match restore_backup_payload_json(channel.clone(), payload_json).await {
            Ok(message) => println!("baseline restore applied: {message}"),
            Err(err) => {
                eprintln!("failed to restore baseline backup: {err}");
                if run_result.is_ok() {
                    return Err(err);
                }
            }
        }
    }

    let bench_report = run_result?;

    let output = write_report_files(&bench_report, &cli.output_dir)?;
    println!("json_report={}", output.json_path.display());
    println!("markdown_report={}", output.markdown_path.display());

    Ok(())
}

async fn capture_backup_payload_json(channel: Channel) -> Result<String> {
    let mut client = AdminServiceClient::new(channel);
    let response = client
        .create_backup(BackupCreateRequest {})
        .await
        .context("create_backup request failed")?
        .into_inner();

    if response.payload_json.trim().is_empty() {
        bail!("create_backup returned empty payload_json");
    }

    Ok(response.payload_json)
}

async fn restore_backup_payload_json(channel: Channel, payload_json: String) -> Result<String> {
    let mut client = AdminServiceClient::new(channel);
    let response = client
        .restore_backup(BackupRestoreRequest { payload_json })
        .await
        .context("restore_backup request failed")?
        .into_inner();

    if !response.restored {
        bail!("restore_backup returned restored=false");
    }

    Ok(response.message)
}

async fn connect_channel(cli: &Cli) -> Result<Channel> {
    let endpoint = Endpoint::from_shared(cli.endpoint.clone())
        .with_context(|| format!("invalid endpoint: {}", cli.endpoint))?
        .connect_timeout(Duration::from_secs(cli.connect_timeout_seconds));

    endpoint
        .connect()
        .await
        .with_context(|| format!("failed to connect to {}", cli.endpoint))
}

fn parse_scenarios(raw: &str) -> Result<Vec<String>> {
    let tokens: Vec<String> = raw
        .split(',')
        .map(|item| item.trim().to_ascii_lowercase())
        .filter(|item| !item.is_empty())
        .collect();

    if tokens.is_empty() {
        bail!("scenarios cannot be empty");
    }

    if tokens.iter().any(|item| item == "all") {
        return Ok(ALL_SCENARIOS.iter().map(|item| item.to_string()).collect());
    }

    let mut unique = HashSet::new();
    let mut scenarios = Vec::new();
    for token in tokens {
        if !ALL_SCENARIOS.contains(&token.as_str()) {
            bail!(
                "unsupported scenario: {}. valid scenarios: {}",
                token,
                ALL_SCENARIOS.join(",")
            );
        }
        if unique.insert(token.clone()) {
            scenarios.push(token);
        }
    }

    Ok(scenarios)
}

async fn run_scenario(name: &str, channel: Channel, cli: &Cli) -> Result<ScenarioReport> {
    match name {
        "registry_register" => run_registry_register(channel, cli).await,
        "config_put_get" => run_config_put_get(channel, cli).await,
        "lock_acquire_release" => run_lock_acquire_release(channel, cli).await,
        "idgen_generate" => run_idgen_generate(channel, cli).await,
        "transit_encrypt_decrypt" => run_transit_encrypt_decrypt(channel, cli).await,
        "pki_issue_revoke" => run_pki_issue_revoke(channel, cli).await,
        "pki_ocsp_status" => run_pki_ocsp_status(channel, cli).await,
        _ => bail!("unsupported scenario: {name}"),
    }
}

async fn run_registry_register(channel: Channel, cli: &Cli) -> Result<ScenarioReport> {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(cli.duration_seconds);
    let max_ops_per_worker = normalize_max_ops(cli.max_ops_per_worker);

    let mut handles = Vec::new();
    for worker_id in 0..cli.concurrency {
        let worker_deadline = deadline;
        let worker_channel = channel.clone();
        handles.push(tokio::spawn(async move {
            let mut client = RegistryServiceClient::new(worker_channel);
            let mut worker = WorkerResult::default();
            let mut op_idx = 0_u64;

            loop {
                if should_stop(worker_deadline, op_idx, max_ops_per_worker) {
                    break;
                }
                op_idx += 1;

                let service_name = format!("bench-registry-{worker_id}");
                let instance_id = format!("inst-{worker_id}-{op_idx}");
                let instance = ServiceInstance {
                    service_name,
                    instance_id,
                    host: "127.0.0.1".to_string(),
                    port: 10_000 + worker_id,
                    metadata: Default::default(),
                };

                let op_started = Instant::now();
                let result = async {
                    client
                        .register(RegisterRequest {
                            instance: Some(instance.clone()),
                            ttl_seconds: 30,
                        })
                        .await?;
                    client.deregister(instance).await?;
                    Ok::<(), tonic::Status>(())
                }
                .await;

                worker
                    .latencies_us
                    .push(op_started.elapsed().as_micros() as u64);
                match result {
                    Ok(()) => worker.success += 1,
                    Err(err) => record_error(&mut worker, err.to_string()),
                }
            }

            worker
        }));
    }

    finalize_scenario("registry_register", started, cli.concurrency, handles).await
}

async fn run_config_put_get(channel: Channel, cli: &Cli) -> Result<ScenarioReport> {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(cli.duration_seconds);
    let max_ops_per_worker = normalize_max_ops(cli.max_ops_per_worker);
    let config_key_space = cli.config_key_space.max(1) as u64;

    let mut handles = Vec::new();
    for worker_id in 0..cli.concurrency {
        let worker_deadline = deadline;
        let worker_channel = channel.clone();
        handles.push(tokio::spawn(async move {
            let mut client = ConfigServiceClient::new(worker_channel);
            let mut worker = WorkerResult::default();
            let mut op_idx = 0_u64;

            loop {
                if should_stop(worker_deadline, op_idx, max_ops_per_worker) {
                    break;
                }
                op_idx += 1;

                let slot = op_idx % config_key_space;
                let key = format!("/bench/config/{worker_id}/{slot}");
                let value = format!("value-{worker_id}-{op_idx}");

                let op_started = Instant::now();
                let result = async {
                    client
                        .put_config(PutConfigRequest {
                            key: key.clone(),
                            value,
                        })
                        .await?;
                    client.get_config(ConfigRequest { key }).await?;
                    Ok::<(), tonic::Status>(())
                }
                .await;

                worker
                    .latencies_us
                    .push(op_started.elapsed().as_micros() as u64);
                match result {
                    Ok(()) => worker.success += 1,
                    Err(err) => record_error(&mut worker, err.to_string()),
                }
            }

            worker
        }));
    }

    finalize_scenario("config_put_get", started, cli.concurrency, handles).await
}

async fn run_lock_acquire_release(channel: Channel, cli: &Cli) -> Result<ScenarioReport> {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(cli.duration_seconds);
    let max_ops_per_worker = normalize_max_ops(cli.max_ops_per_worker);

    let mut handles = Vec::new();
    for worker_id in 0..cli.concurrency {
        let worker_deadline = deadline;
        let worker_channel = channel.clone();
        handles.push(tokio::spawn(async move {
            let mut client = LockServiceClient::new(worker_channel);
            let mut worker = WorkerResult::default();
            let mut op_idx = 0_u64;

            loop {
                if should_stop(worker_deadline, op_idx, max_ops_per_worker) {
                    break;
                }
                op_idx += 1;

                let lock_name = format!("bench-lock-{worker_id}-{op_idx}");
                let owner = format!("worker-{worker_id}");

                let op_started = Instant::now();
                let result = async {
                    let acquire = client
                        .acquire(LockAcquireRequest {
                            lock_name: lock_name.clone(),
                            owner,
                            ttl_seconds: 30,
                            wait: false,
                        })
                        .await?
                        .into_inner();

                    if !acquire.acquired {
                        return Err(tonic::Status::aborted(format!(
                            "lock not acquired: {}",
                            acquire.message
                        )));
                    }

                    client
                        .release(LockReleaseRequest {
                            lock_name,
                            token: acquire.token,
                        })
                        .await?;
                    Ok::<(), tonic::Status>(())
                }
                .await;

                worker
                    .latencies_us
                    .push(op_started.elapsed().as_micros() as u64);
                match result {
                    Ok(()) => worker.success += 1,
                    Err(err) => record_error(&mut worker, err.to_string()),
                }
            }

            worker
        }));
    }

    finalize_scenario("lock_acquire_release", started, cli.concurrency, handles).await
}

async fn run_idgen_generate(channel: Channel, cli: &Cli) -> Result<ScenarioReport> {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(cli.duration_seconds);
    let max_ops_per_worker = normalize_max_ops(cli.max_ops_per_worker);

    let mut handles = Vec::new();
    for _ in 0..cli.concurrency {
        let worker_deadline = deadline;
        let worker_channel = channel.clone();
        handles.push(tokio::spawn(async move {
            let mut client = IdGenServiceClient::new(worker_channel);
            let mut worker = WorkerResult::default();
            let mut op_idx = 0_u64;

            loop {
                if should_stop(worker_deadline, op_idx, max_ops_per_worker) {
                    break;
                }
                op_idx += 1;

                let op_started = Instant::now();
                let result = async {
                    let resp = client
                        .generate_snowflake(SnowflakeRequest { batch: 64 })
                        .await?
                        .into_inner();
                    if resp.ids.is_empty() {
                        return Err(tonic::Status::internal(
                            "id generator returned an empty batch",
                        ));
                    }
                    Ok::<(), tonic::Status>(())
                }
                .await;

                worker
                    .latencies_us
                    .push(op_started.elapsed().as_micros() as u64);
                match result {
                    Ok(()) => worker.success += 1,
                    Err(err) => record_error(&mut worker, err.to_string()),
                }
            }

            worker
        }));
    }

    finalize_scenario("idgen_generate", started, cli.concurrency, handles).await
}

async fn run_transit_encrypt_decrypt(channel: Channel, cli: &Cli) -> Result<ScenarioReport> {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(cli.duration_seconds);
    let max_ops_per_worker = normalize_max_ops(cli.max_ops_per_worker);

    let mut handles = Vec::new();
    for worker_id in 0..cli.concurrency {
        let worker_deadline = deadline;
        let worker_channel = channel.clone();
        handles.push(tokio::spawn(async move {
            let mut client = TransitServiceClient::new(worker_channel);
            let key_name = format!("bench-transit-key-{worker_id}");
            let _ = client
                .create_key(coord_proto::coord::v1::CreateKeyRequest {
                    key_name: key_name.clone(),
                    algorithm: String::new(),
                })
                .await;

            let mut worker = WorkerResult::default();
            let mut op_idx = 0_u64;

            loop {
                if should_stop(worker_deadline, op_idx, max_ops_per_worker) {
                    break;
                }
                op_idx += 1;

                let plain = format!("payload-{worker_id}-{op_idx}");
                let op_started = Instant::now();

                let result: Result<()> = async {
                    let encrypted = client
                        .encrypt(EncryptRequest {
                            key_name: key_name.clone(),
                            plaintext: plain.clone(),
                        })
                        .await
                        .context("transit encrypt request failed")?
                        .into_inner();

                    let decrypted = client
                        .decrypt(DecryptRequest {
                            key_name: key_name.clone(),
                            ciphertext: encrypted.ciphertext,
                        })
                        .await
                        .context("transit decrypt request failed")?
                        .into_inner();

                    if decrypted.plaintext != plain {
                        bail!("decrypted payload mismatch");
                    }

                    Ok(())
                }
                .await;

                worker
                    .latencies_us
                    .push(op_started.elapsed().as_micros() as u64);
                match result {
                    Ok(()) => worker.success += 1,
                    Err(err) => record_error(&mut worker, err.to_string()),
                }
            }

            worker
        }));
    }

    finalize_scenario("transit_encrypt_decrypt", started, cli.concurrency, handles).await
}

async fn run_pki_issue_revoke(channel: Channel, cli: &Cli) -> Result<ScenarioReport> {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(cli.duration_seconds);
    let max_ops_per_worker = normalize_max_ops(cli.max_ops_per_worker);

    let mut handles = Vec::new();
    for worker_id in 0..cli.concurrency {
        let worker_deadline = deadline;
        let worker_channel = channel.clone();
        handles.push(tokio::spawn(async move {
            let mut client = PkiServiceClient::new(worker_channel);
            let mut worker = WorkerResult::default();
            let mut op_idx = 0_u64;

            loop {
                if should_stop(worker_deadline, op_idx, max_ops_per_worker) {
                    break;
                }
                op_idx += 1;

                let common_name = format!("bench-pki-{worker_id}-{op_idx}.internal");

                let op_started = Instant::now();
                let result = async {
                    let issued = client
                        .issue_certificate(IssueCertificateRequest {
                            common_name,
                            sans: Vec::new(),
                            ttl_seconds: 3600,
                            auto_renew: false,
                            renew_before_seconds: 3600,
                            role_name: String::new(),
                            ttl: String::new(),
                        })
                        .await?
                        .into_inner();

                    client
                        .revoke_certificate(RevokeCertificateRequest {
                            serial_number: issued.serial_number,
                            reason: "benchmark_cleanup".to_string(),
                        })
                        .await?;

                    Ok::<(), tonic::Status>(())
                }
                .await;

                worker
                    .latencies_us
                    .push(op_started.elapsed().as_micros() as u64);
                match result {
                    Ok(()) => worker.success += 1,
                    Err(err) => record_error(&mut worker, err.to_string()),
                }
            }

            worker
        }));
    }

    finalize_scenario("pki_issue_revoke", started, cli.concurrency, handles).await
}

async fn run_pki_ocsp_status(channel: Channel, cli: &Cli) -> Result<ScenarioReport> {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(cli.duration_seconds);
    let max_ops_per_worker = normalize_max_ops(cli.max_ops_per_worker);

    let mut handles = Vec::new();
    for worker_id in 0..cli.concurrency {
        let worker_deadline = deadline;
        let worker_channel = channel.clone();
        handles.push(tokio::spawn(async move {
            let mut client = PkiServiceClient::new(worker_channel);
            let issued = client
                .issue_certificate(IssueCertificateRequest {
                    common_name: format!("bench-ocsp-{worker_id}.internal"),
                    sans: Vec::new(),
                    ttl_seconds: 3600,
                    auto_renew: false,
                    renew_before_seconds: 3600,
                    role_name: String::new(),
                    ttl: String::new(),
                })
                .await
                .map(|resp| resp.into_inner());

            let serial_number = match issued {
                Ok(item) => item.serial_number,
                Err(err) => {
                    return WorkerResult {
                        success: 0,
                        errors: 1,
                        latencies_us: Vec::new(),
                        first_error: Some(format!("prepare issue certificate failed: {err}")),
                    };
                }
            };

            let mut worker = WorkerResult::default();
            let mut op_idx = 0_u64;

            loop {
                if should_stop(worker_deadline, op_idx, max_ops_per_worker) {
                    break;
                }
                op_idx += 1;

                let op_started = Instant::now();
                let result = async {
                    let status = client
                        .check_certificate_status(CheckCertificateStatusRequest {
                            serial_number: serial_number.clone(),
                        })
                        .await?
                        .into_inner();
                    if status.status != "GOOD" {
                        return Err(tonic::Status::aborted(format!(
                            "unexpected status: {}",
                            status.status
                        )));
                    }
                    Ok::<(), tonic::Status>(())
                }
                .await;

                worker
                    .latencies_us
                    .push(op_started.elapsed().as_micros() as u64);
                match result {
                    Ok(()) => worker.success += 1,
                    Err(err) => record_error(&mut worker, err.to_string()),
                }
            }

            worker
        }));
    }

    finalize_scenario("pki_ocsp_status", started, cli.concurrency, handles).await
}

async fn finalize_scenario(
    name: &str,
    started: Instant,
    concurrency: u32,
    handles: Vec<tokio::task::JoinHandle<WorkerResult>>,
) -> Result<ScenarioReport> {
    let mut success = 0_u64;
    let mut errors = 0_u64;
    let mut latencies = Vec::new();
    let mut sample_errors = Vec::new();

    for handle in handles {
        let worker = handle
            .await
            .map_err(|join_err| anyhow!("worker task join error: {join_err}"))?;
        success += worker.success;
        errors += worker.errors;
        latencies.extend(worker.latencies_us);
        if let Some(err) = worker.first_error
            && sample_errors.len() < 5
        {
            sample_errors.push(err);
        }
    }

    let elapsed = started.elapsed().as_secs_f64().max(0.000_001);
    let success_rps = success as f64 / elapsed;
    let latency = LatencyStats::from_samples(&mut latencies);

    Ok(ScenarioReport {
        name: name.to_string(),
        duration_seconds: elapsed,
        concurrency,
        success,
        errors,
        success_rps,
        latency_ms: latency,
        sample_errors,
    })
}

fn normalize_max_ops(raw: u64) -> Option<u64> {
    if raw == 0 { None } else { Some(raw) }
}

fn should_stop(deadline: Instant, completed_ops: u64, max_ops: Option<u64>) -> bool {
    if Instant::now() >= deadline {
        return true;
    }

    if let Some(limit) = max_ops {
        completed_ops >= limit
    } else {
        false
    }
}

fn record_error(worker: &mut WorkerResult, error: String) {
    worker.errors += 1;
    if worker.first_error.is_none() {
        worker.first_error = Some(error);
    }
}

fn summarize_suite(results: &[ScenarioReport]) -> SuiteSummary {
    let total_success: u64 = results.iter().map(|item| item.success).sum();
    let total_errors: u64 = results.iter().map(|item| item.errors).sum();
    let total_measured_seconds: f64 = results.iter().map(|item| item.duration_seconds).sum();
    let aggregate_success_rps = if total_measured_seconds > 0.0 {
        total_success as f64 / total_measured_seconds
    } else {
        0.0
    };

    let slowest = results
        .iter()
        .max_by(|a, b| {
            a.latency_ms
                .p95
                .partial_cmp(&b.latency_ms.p95)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|item| format!("{} ({:.2} ms)", item.name, item.latency_ms.p95))
        .unwrap_or_else(|| "n/a".to_string());

    SuiteSummary {
        total_success,
        total_errors,
        total_measured_seconds,
        aggregate_success_rps,
        slowest_p95_scenario: slowest,
    }
}

struct WrittenReport {
    json_path: PathBuf,
    markdown_path: PathBuf,
}

fn write_report_files(report: &BenchmarkReport, output_dir: &Path) -> Result<WrittenReport> {
    fs::create_dir_all(output_dir)
        .with_context(|| format!("failed to create output dir: {}", output_dir.display()))?;

    let timestamp = report.generated_at_unix_seconds;
    let json_path = output_dir.join(format!("report_{timestamp}.json"));
    let markdown_path = output_dir.join(format!("report_{timestamp}.md"));

    let json = serde_json::to_string_pretty(report).context("failed to serialize report json")?;
    fs::write(&json_path, json)
        .with_context(|| format!("failed to write report: {}", json_path.display()))?;

    let markdown = render_markdown(report);
    fs::write(&markdown_path, markdown)
        .with_context(|| format!("failed to write report: {}", markdown_path.display()))?;

    Ok(WrittenReport {
        json_path,
        markdown_path,
    })
}

fn render_markdown(report: &BenchmarkReport) -> String {
    let mut out = String::new();
    out.push_str("# Coord Benchmark Report\n\n");
    out.push_str(&format!(
        "- generated_at_unix_seconds: {}\n",
        report.generated_at_unix_seconds
    ));
    out.push_str(&format!("- endpoint: {}\n", report.endpoint));
    out.push_str(&format!(
        "- requested_duration_seconds: {}\n",
        report.requested_duration_seconds
    ));
    out.push_str(&format!("- concurrency: {}\n", report.concurrency));
    out.push_str(&format!(
        "- max_ops_per_worker: {}\n",
        report.max_ops_per_worker
    ));
    out.push_str(&format!("- scenarios: {}\n\n", report.scenarios.join(", ")));

    out.push_str("## Scenario Results\n\n");
    out.push_str(
        "| scenario | success | errors | rps | avg(ms) | p50(ms) | p95(ms) | p99(ms) | max(ms) |\n",
    );
    out.push_str("| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for item in &report.results {
        out.push_str(&format!(
            "| {} | {} | {} | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} |\n",
            item.name,
            item.success,
            item.errors,
            item.success_rps,
            item.latency_ms.avg,
            item.latency_ms.p50,
            item.latency_ms.p95,
            item.latency_ms.p99,
            item.latency_ms.max,
        ));
    }

    out.push_str("\n## Suite Summary\n\n");
    out.push_str(&format!(
        "- total_success: {}\n",
        report.summary.total_success
    ));
    out.push_str(&format!(
        "- total_errors: {}\n",
        report.summary.total_errors
    ));
    out.push_str(&format!(
        "- total_measured_seconds: {:.2}\n",
        report.summary.total_measured_seconds
    ));
    out.push_str(&format!(
        "- aggregate_success_rps: {:.2}\n",
        report.summary.aggregate_success_rps
    ));
    out.push_str(&format!(
        "- slowest_p95_scenario: {}\n",
        report.summary.slowest_p95_scenario
    ));

    let scenario_errors: Vec<&ScenarioReport> = report
        .results
        .iter()
        .filter(|item| !item.sample_errors.is_empty())
        .collect();
    if !scenario_errors.is_empty() {
        out.push_str("\n## Sample Errors\n\n");
        for scenario in scenario_errors {
            out.push_str(&format!("### {}\n", scenario.name));
            for err in &scenario.sample_errors {
                out.push_str(&format!("- {}\n", err));
            }
            out.push('\n');
        }
    }

    out
}

fn now_unix_seconds() -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0));
    now.as_secs() as i64
}
