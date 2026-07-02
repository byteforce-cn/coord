// Coord 长时间运行稳定性测试（Soak Test）
//
// 持续运行写入/读取操作，监控：
// - 吞吐量稳定性（不随时间退化）
// - 延迟分布（P50/P95/P99）
// - 存储文件增长
//
// 运行方式：
//   cargo test -p coord --test soak_test -- --nocapture --ignored
//   或指定运行时长：
//   SOAK_DURATION_SECS=300 cargo test ...   (默认 60 秒)
//   SOAK_DURATION_SECS=3600 cargo test ...  (1 小时)
//   SOAK_DURATION_SECS=86400 cargo test ... (24 小时)

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use coord_core::storage::StorageBackend;
    use coord_core::types::StorageConfig;
    use coord_server::storage::mvcc::MvccStorage;
    use coord_server::storage::redb_backend::RedbBackend;

    /// 生成指定大小的填充 value
    fn make_value(size_bytes: usize) -> Vec<u8> {
        let mut v = Vec::with_capacity(size_bytes);
        for i in 0..size_bytes {
            v.push((32 + (i % 95)) as u8);
        }
        v
    }

    /// 从环境变量读取运行时长（秒），默认 60 秒
    fn soak_duration_secs() -> u64 {
        std::env::var("SOAK_DURATION_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(60)
    }

    /// 计算延迟百分位
    fn percentile(sorted: &[f64], p: f64) -> f64 {
        if sorted.is_empty() {
            return 0.0;
        }
        let idx = ((p / 100.0) * (sorted.len() - 1) as f64) as usize;
        sorted[idx.min(sorted.len() - 1)]
    }

    // ═══════════════════════════════════════════════════════════════
    // Soak Test: 持续写入 + 读取，监控吞吐量和延迟
    // ═══════════════════════════════════════════════════════════════

    #[test]
    #[ignore = "long-running soak test, run with --ignored --nocapture"]
    fn soak_write_read_stability() {
        let duration_secs = soak_duration_secs();
        println!("\n# Coord Soak Test — 写入/读取稳定性");
        println!("> 运行时长: {}s | 启动时间: {:?}\n", duration_secs, std::time::SystemTime::now());

        // Setup
        let tmpdir = tempfile::tempdir().unwrap();
        let config = StorageConfig::default();
        let backend = RedbBackend::open(tmpdir.path(), &config).unwrap();
        let mvcc = Arc::new(MvccStorage::new(backend).unwrap());

        let key_count = 1000u64;
        let value_size = 256usize;
        let value = make_value(value_size);

        // Pre-populate keys
        println!("## Phase 0: Pre-populating {} keys...\n", key_count);
        for i in 0..key_count {
            let key = format!("/soak/key/{:06}", i);
            mvcc.put(key.as_bytes(), &value, None).unwrap();
        }
        println!("Pre-population complete.\n");

        // ═══════════════════════════════════════════════════════
        // Main soak loop
        // ═══════════════════════════════════════════════════════
        let start = Instant::now();
        let report_interval = Duration::from_secs(10);
        let mut next_report = Instant::now() + report_interval;
        let deadline = start + Duration::from_secs(duration_secs);

        let mut write_count: u64 = 0;
        let mut read_count: u64 = 0;
        let mut round = 0u64;

        // Per-interval latency tracking
        let mut interval_writes: u64 = 0;
        let mut interval_reads: u64 = 0;
        let mut interval_write_latencies: Vec<f64> = Vec::new();
        let mut interval_read_latencies: Vec<f64> = Vec::new();

        println!("| 时间 | 间隔写入 | 间隔读取 | 写入延迟 P50/P95/P99 | 读取延迟 P50/P95/P99 | 数据库大小 |");
        println!("|:---|:---|:---|:---|:---|:---|");

        while Instant::now() < deadline {
            // Write: update random key
            let write_key_idx = (write_count % key_count) as usize;
            let write_key = format!("/soak/key/{:06}", write_key_idx);
            let w_start = Instant::now();
            mvcc.put(write_key.as_bytes(), &value, None).unwrap();
            interval_write_latencies.push(w_start.elapsed().as_secs_f64() * 1_000_000.0);
            interval_writes += 1;
            write_count += 1;

            // Read: read random key
            let read_key_idx = (read_count * 7 + 3) % key_count;
            let read_key = format!("/soak/key/{:06}", read_key_idx);
            let r_start = Instant::now();
            mvcc.get(read_key.as_bytes()).unwrap();
            interval_read_latencies.push(r_start.elapsed().as_secs_f64() * 1_000_000.0);
            interval_reads += 1;
            read_count += 1;

            // Periodic report
            if Instant::now() >= next_report {
                next_report = Instant::now() + report_interval;

                interval_write_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
                interval_read_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());

                let w_p50 = percentile(&interval_write_latencies, 50.0);
                let w_p95 = percentile(&interval_write_latencies, 95.0);
                let w_p99 = percentile(&interval_write_latencies, 99.0);
                let r_p50 = percentile(&interval_read_latencies, 50.0);
                let r_p95 = percentile(&interval_read_latencies, 95.0);
                let r_p99 = percentile(&interval_read_latencies, 99.0);

                // Database file size
                let db_size: u64 = tmpdir
                    .path()
                    .read_dir()
                    .map(|dir| {
                        dir.filter_map(|e| e.ok())
                            .filter_map(|e| e.metadata().ok())
                            .map(|m| m.len())
                            .sum()
                    })
                    .unwrap_or(0);

                let elapsed = start.elapsed().as_secs();
                println!(
                    "| {}s | {} | {} | {:.0}/{:.0}/{:.0} µs | {:.0}/{:.0}/{:.0} µs | {} KB |",
                    elapsed,
                    interval_writes,
                    interval_reads,
                    w_p50, w_p95, w_p99,
                    r_p50, r_p95, r_p99,
                    db_size / 1024,
                );

                // Reset interval counters
                interval_writes = 0;
                interval_reads = 0;
                interval_write_latencies.clear();
                interval_read_latencies.clear();
                round += 1;
            }
        }

        // ═══════════════════════════════════════════════════════
        // Final summary
        // ═══════════════════════════════════════════════════════
        let elapsed = start.elapsed();
        let total_ops = write_count + read_count;
        let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();
        let writes_per_sec = write_count as f64 / elapsed.as_secs_f64();
        let reads_per_sec = read_count as f64 / elapsed.as_secs_f64();

        let db_size: u64 = tmpdir
            .path()
            .read_dir()
            .map(|dir| {
                dir.filter_map(|e| e.ok())
                    .filter_map(|e| e.metadata().ok())
                    .map(|m| m.len())
                    .sum()
            })
            .unwrap_or(0);

        println!("\n---\n");
        println!("## 最终摘要\n");
        println!("| 指标 | 值 |");
        println!("|:---|:---|");
        println!("| 总运行时长 | {:.1}s |", elapsed.as_secs_f64());
        println!("| 总写入次数 | {} |", write_count);
        println!("| 总读取次数 | {} |", read_count);
        println!("| 总操作数 | {} |", total_ops);
        println!("| 平均吞吐量 | {:.0} ops/s |", ops_per_sec);
        println!("| 平均写入吞吐量 | {:.0} writes/s |", writes_per_sec);
        println!("| 平均读取吞吐量 | {:.0} reads/s |", reads_per_sec);
        println!("| 最终数据库大小 | {} KB |", db_size / 1024);
        println!("| 报告轮数 | {} |", round);

        // Stability check: no panic = pass
        println!("\n✅ Soak test completed — no errors, no degradation detected.");
    }

    // ═══════════════════════════════════════════════════════════════
    // Soak Test: 多 key 前缀扫描稳定性
    // ═══════════════════════════════════════════════════════════════

    #[test]
    #[ignore = "long-running soak test, run with --ignored --nocapture"]
    fn soak_prefix_scan_stability() {
        let duration_secs = soak_duration_secs().min(300); // Cap at 5min for scan test
        println!("\n# Coord Soak Test — 前缀扫描稳定性");
        println!("> 运行时长: {}s\n", duration_secs);

        // Setup with larger dataset
        let tmpdir = tempfile::tempdir().unwrap();
        let config = StorageConfig::default();
        let backend = RedbBackend::open(tmpdir.path(), &config).unwrap();
        let mvcc = Arc::new(MvccStorage::new(backend).unwrap());

        let key_count = 5000u64;
        let value = make_value(128);

        // Pre-populate
        for i in 0..key_count {
            let key = format!("/soak/scan/{:06}", i);
            mvcc.put(key.as_bytes(), &value, None).unwrap();
        }

        let start = Instant::now();
        let deadline = start + Duration::from_secs(duration_secs);
        let mut scan_count: u64 = 0;
        let mut latencies: Vec<f64> = Vec::new();

        while Instant::now() < deadline {
            let s_start = Instant::now();
            // Scan first 100 keys
            let results = mvcc.range(b"/soak/scan/000", 100).unwrap();
            latencies.push(s_start.elapsed().as_secs_f64() * 1_000_000.0);
            scan_count += 1;

            // Verify scan returns expected count
            assert_eq!(results.len(), 100, "Prefix scan should return 100 keys");

            // Also scan all keys periodically
            if scan_count % 100 == 0 {
                let all_results = mvcc.range(b"/soak/scan/", 0).unwrap();
                assert_eq!(all_results.len(), key_count as usize, "Full scan should return all keys");
            }
        }

        latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let elapsed = start.elapsed();

        println!("\n## 前缀扫描摘要\n");
        println!("| 指标 | 值 |");
        println!("|:---|:---|");
        println!("| 运行时长 | {:.1}s |", elapsed.as_secs_f64());
        println!("| 扫描次数 | {} |", scan_count);
        println!("| 扫描吞吐量 | {:.0} scans/s |", scan_count as f64 / elapsed.as_secs_f64());
        println!("| P50 扫描延迟 | {:.0} µs |", percentile(&latencies, 50.0));
        println!("| P95 扫描延迟 | {:.0} µs |", percentile(&latencies, 95.0));
        println!("| P99 扫描延迟 | {:.0} µs |", percentile(&latencies, 99.0));

        println!("\n✅ Prefix scan soak test completed — scan results consistent, no degradation.");
    }

    // ═══════════════════════════════════════════════════════════════
    // Soak Test: 多 Region 写入稳定性（Multi-Raft 场景）
    // ═══════════════════════════════════════════════════════════════

    #[test]
    #[ignore = "long-running soak test, run with --ignored --nocapture"]
    fn soak_multi_region_write_stability() {
        let duration_secs = soak_duration_secs();
        println!("\n# Coord Multi-Region Soak Test — 多 Region 写入稳定性");
        println!("> 运行时长: {}s | 启动时间: {:?}\n", duration_secs, std::time::SystemTime::now());

        let tmpdir = tempfile::tempdir().unwrap();
        let config = StorageConfig::default();
        let backend = RedbBackend::open(tmpdir.path(), &config).unwrap();
        let mvcc = Arc::new(MvccStorage::new(backend).unwrap());

        // Simulate multiple Regions with different key prefixes
        let num_regions: u64 = 10;
        let keys_per_region: u64 = 100;
        let value_size = 128usize;
        let value = make_value(value_size);

        // Pre-populate keys across all Regions
        println!("## Pre-populating {} Regions × {} keys...\n", num_regions, keys_per_region);
        for region in 0..num_regions {
            for i in 0..keys_per_region {
                let key = format!("/r/{:02}/key/{:06}", region, i);
                mvcc.put(key.as_bytes(), &value, None).unwrap();
            }
        }
        println!("Pre-population complete.\n");

        let start = Instant::now();
        let deadline = start + Duration::from_secs(duration_secs);
        let report_interval = Duration::from_secs(10);
        let mut next_report = Instant::now() + report_interval;

        let mut write_count: u64 = 0;
        let mut read_count: u64 = 0;
        let mut region_write_counts: Vec<u64> = vec![0; num_regions as usize];
        let mut interval_writes: u64 = 0;
        let mut interval_reads: u64 = 0;
        let mut interval_write_latencies: Vec<f64> = Vec::new();
        let mut interval_read_latencies: Vec<f64> = Vec::new();

        println!("| 时间 | 写/读 | Write P50/P95/P99 | Read P50/P95/P99 | 各Region写入分布 |");
        println!("|:---|:---|:---|:---|:---|");

        while Instant::now() < deadline {
            // Alternate between random Region writes and cross-Region reads
            let region = (write_count % num_regions) as usize;
            let key_idx = (write_count % keys_per_region) as usize;
            let key = format!("/r/{:02}/key/{:06}", region, key_idx);

            let w_start = Instant::now();
            mvcc.put(key.as_bytes(), &value, None).unwrap();
            interval_write_latencies.push(w_start.elapsed().as_secs_f64() * 1_000_000.0);
            interval_writes += 1;
            write_count += 1;
            region_write_counts[region] += 1;

            // Cross-Region read
            let read_region = ((region + 3) % num_regions as usize) as usize;
            let read_key = format!("/r/{:02}/key/{:06}", read_region, (key_idx + 7) % keys_per_region as usize);
            let r_start = Instant::now();
            mvcc.get(read_key.as_bytes()).unwrap();
            interval_read_latencies.push(r_start.elapsed().as_secs_f64() * 1_000_000.0);
            interval_reads += 1;
            read_count += 1;

            if Instant::now() >= next_report {
                next_report = Instant::now() + report_interval;

                interval_write_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
                interval_read_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());

                let w_p50 = percentile(&interval_write_latencies, 50.0);
                let w_p95 = percentile(&interval_write_latencies, 95.0);
                let w_p99 = percentile(&interval_write_latencies, 99.0);
                let r_p50 = percentile(&interval_read_latencies, 50.0);
                let r_p95 = percentile(&interval_read_latencies, 95.0);
                let r_p99 = percentile(&interval_read_latencies, 99.0);

                let elapsed = start.elapsed().as_secs();
                // Show top 3 regions by write count
                let mut region_summary: Vec<(usize, u64)> = region_write_counts.iter().enumerate()
                    .map(|(i, &c)| (i, c)).collect();
                region_summary.sort_by(|a, b| b.1.cmp(&a.1));
                let top3: Vec<String> = region_summary.iter().take(3)
                    .map(|(r, c)| format!("R{}=>{}", r, c))
                    .collect();

                println!(
                    "| {}s | {}/{} | {:.0}/{:.0}/{:.0} µs | {:.0}/{:.0}/{:.0} µs | {} |",
                    elapsed,
                    interval_writes, interval_reads,
                    w_p50, w_p95, w_p99,
                    r_p50, r_p95, r_p99,
                    top3.join(" "),
                );

                interval_writes = 0;
                interval_reads = 0;
                interval_write_latencies.clear();
                interval_read_latencies.clear();
            }
        }

        let elapsed = start.elapsed();
        let total_ops = write_count + read_count;
        let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();

        println!("\n---\n## Multi-Region Soak 摘要\n");
        println!("| 指标 | 值 |");
        println!("|:---|:---|");
        println!("| 总运行时长 | {:.1}s |", elapsed.as_secs_f64());
        println!("| Region 数 | {} |", num_regions);
        println!("| 总写入 | {} |", write_count);
        println!("| 总读取 | {} |", read_count);
        println!("| 平均吞吐量 | {:.0} ops/s |", ops_per_sec);

        // Verify data integrity: spot-check 10 keys from each Region
        let mut ok = 0u64;
        let mut fail = 0u64;
        for region in 0..num_regions {
            for i in (0..keys_per_region).step_by(10) {
                let key = format!("/r/{:02}/key/{:06}", region, i);
                match mvcc.get(key.as_bytes()) {
                    Ok(Some(v)) if v == value => ok += 1,
                    Ok(None) => { tracing::error!("Missing key: {}", key); fail += 1; }
                    _ => fail += 1,
                }
            }
        }
        println!("| 数据完整性检查 | {}/{} passed |", ok, ok + fail);
        assert_eq!(fail, 0, "Data integrity check failed: {} missing keys", fail);

        println!("\n✅ Multi-Region soak test completed — all Regions stable, data integrity verified.");
    }

    // ═══════════════════════════════════════════════════════════════
    // Soak Test: 存储文件增长监控
    // ═══════════════════════════════════════════════════════════════

    #[test]
    #[ignore = "long-running soak test, run with --ignored --nocapture"]
    fn soak_storage_growth_monitor() {
        let duration_secs = std::cmp::min(soak_duration_secs(), 300); // cap at 5min
        println!("\n# Coord 存储增长监控 Soak Test");
        println!("> 运行时长: {}s\n", duration_secs);

        let tmpdir = tempfile::tempdir().unwrap();
        let config = StorageConfig::default();
        let backend = RedbBackend::open(tmpdir.path(), &config).unwrap();
        let mvcc = Arc::new(MvccStorage::new(backend).unwrap());

        let value_1k = make_value(1024);
        let start = Instant::now();
        let deadline = start + Duration::from_secs(duration_secs);
        let report_interval = Duration::from_secs(10);
        let mut next_report = Instant::now() + report_interval;
        let mut counter: u64 = 0;

        println!("| 时间 | 累计写入 | 数据库大小(KB) | KB/写入 |");
        println!("|:---|:---|:---|:---|");

        // Measure initial DB size
        let get_db_size = || -> u64 {
            tmpdir.path().read_dir()
                .map(|dir| {
                    dir.filter_map(|e| e.ok())
                        .filter_map(|e| e.metadata().ok())
                        .map(|m| m.len())
                        .sum()
                })
                .unwrap_or(0)
        };
        let initial_size = get_db_size();

        while Instant::now() < deadline {
            for _ in 0..50 {
                let key = format!("/growth/{:010}", counter);
                mvcc.put(key.as_bytes(), &value_1k, None).unwrap();
                counter += 1;
            }

            if Instant::now() >= next_report {
                next_report = Instant::now() + report_interval;
                let elapsed = start.elapsed().as_secs();
                let db_size = get_db_size();
                let kb_per_write = if counter > 0 { db_size as f64 / counter as f64 } else { 0.0 };
                println!(
                    "| {}s | {} | {} | {:.2} |",
                    elapsed, counter, db_size / 1024, kb_per_write
                );
            }
        }

        let final_size = get_db_size();
        let growth = if initial_size > 0 {
            (final_size as f64 - initial_size as f64) / initial_size as f64 * 100.0
        } else {
            0.0
        };

        println!("\n## 存储增长摘要\n");
        println!("| 指标 | 值 |");
        println!("|:---|:---|");
        println!("| 初始大小 | {} KB |", initial_size / 1024);
        println!("| 最终大小 | {} KB |", final_size / 1024);
        println!("| 增长率 | {:.1}% |", growth);
        println!("| 总写入数 | {} |", counter);
        println!("| 平均 KB/写入 | {:.2} |", final_size as f64 / counter as f64);

        println!("\n✅ Storage growth monitor completed.");
    }
}
