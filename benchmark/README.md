# Benchmark Guide

This directory contains pressure testing assets for the coord service.

## Contents

- benchmark/coord-bench: Rust load runner for multiple service scenarios
- benchmark/run.sh: helper script to run the benchmark in release mode
- benchmark/reports: output folder for generated JSON and Markdown reports

## Covered Scenarios

- registry_register: Register then deregister service instances
- config_put_get: Put and then get configuration keys
- lock_acquire_release: Acquire and release distributed locks
- idgen_generate: Request snowflake ID batches
- transit_encrypt_decrypt: Encrypt then decrypt payloads
- pki_issue_revoke: Issue and revoke certificates
- pki_ocsp_status: Query certificate status repeatedly

## Quick Start

1. Start server

cargo run -p coord-server -- dev

2. Run all scenarios

./benchmark/run.sh --endpoint http://127.0.0.1:9090 --duration-seconds 20 --concurrency 16 --scenarios all

3. Run selected scenarios

./benchmark/run.sh --endpoint http://127.0.0.1:9090 --duration-seconds 30 --concurrency 32 --scenarios config_put_get,transit_encrypt_decrypt,idgen_generate

## Report Output

The runner automatically writes two files under benchmark/reports:

- report_<unix_timestamp>.json
- report_<unix_timestamp>.md

The Markdown report includes per-scenario throughput and latency percentiles plus sample errors.

## Useful Parameters

- --duration-seconds: test duration per scenario
- --concurrency: number of concurrent workers
- --max-ops-per-worker: cap operations per worker; 0 means unlimited by op count
- --config-key-space: max key slots per worker for config_put_get (prevents unbounded key growth)
- --restore-after-run: capture backup before benchmark and restore state after run (default true)
- --scenarios: comma-separated scenario list or all
- --output-dir: report output folder

## Data Safety Notes

- Benchmark writes use `/bench/*` namespace where applicable.
- To avoid polluting long-running environments, the runner now restores baseline state after run by default.
- If you intentionally want to keep benchmark side effects, pass `--restore-after-run false`.

## Cleanup Existing Benchmark Data

If benchmark artifacts already exist in config/PKI/workflow views, run:

./benchmark/cleanup-benchmark-data.sh http://127.0.0.1:9091

The script performs `backup create -> remove bench namespace artifacts -> backup restore`.
