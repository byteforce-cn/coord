// Coord 性能基准测试（轻量版）
//
// 使用 std::time 计时，直接输出 Markdown 格式的性能报告。
// 无需 criterion 依赖，可立即运行。
//
// 运行方式：
//   cargo test -p coord --test perf_bench -- --nocapture --ignored

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

    /// 运行 benchmark 并返回 (elapsed, iterations, ops_per_sec)
    fn run_bench<F>(name: &str, iterations: u64, mut f: F) -> (Duration, u64, f64)
    where
        F: FnMut(),
    {
        // Warmup: 10% of iterations (min 10, max 100)
        let warmup = (iterations / 10).min(100).max(10);
        for _ in 0..warmup {
            f();
        }

        let start = Instant::now();
        for _ in 0..iterations {
            f();
        }
        let elapsed = start.elapsed();
        let ops_per_sec = iterations as f64 / elapsed.as_secs_f64();

        println!(
            "| {} | {} | {:?} | {:.0} ops/s |",
            name, iterations, elapsed, ops_per_sec
        );

        (elapsed, iterations, ops_per_sec)
    }

    /// 计算延迟百分位（简化版：从排序数组中取）
    fn percentile(sorted: &[f64], p: f64) -> f64 {
        if sorted.is_empty() {
            return 0.0;
        }
        let idx = ((p / 100.0) * (sorted.len() - 1) as f64) as usize;
        sorted[idx.min(sorted.len() - 1)]
    }

    /// 运行延迟分布 benchmark
    fn run_latency_bench<F>(name: &str, iterations: u64, mut f: F) -> (f64, f64, f64, f64)
    where
        F: FnMut(),
    {
        // Warmup
        let warmup = (iterations / 10).min(100).max(10);
        for _ in 0..warmup {
            f();
        }

        let mut latencies: Vec<f64> = Vec::with_capacity(iterations as usize);
        for _ in 0..iterations {
            let start = Instant::now();
            f();
            latencies.push(start.elapsed().as_secs_f64() * 1_000_000.0); // microseconds
        }

        latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let avg = latencies.iter().sum::<f64>() / latencies.len() as f64;
        let p50 = percentile(&latencies, 50.0);
        let p95 = percentile(&latencies, 95.0);
        let p99 = percentile(&latencies, 99.0);

        println!(
            "| {} | {:.1} µs | {:.1} µs | {:.1} µs | {:.1} µs |",
            name, avg, p50, p95, p99
        );

        (avg, p50, p95, p99)
    }

    // ═══════════════════════════════════════════════════════════════
    // Benchmark 1: 裸 Redb 写入吞吐量（存储引擎基线）
    // ═══════════════════════════════════════════════════════════════

    #[test]
    #[ignore = "performance benchmark, run with --ignored --nocapture"]
    fn bench_raw_redb_write_throughput() {
        println!("\n## 1. 裸 Redb 写入吞吐量\n");
        println!("| Value Size | 迭代次数 | 耗时 | 吞吐量 |");
        println!("|:---|:---|:---|:---|");

        let value_sizes = [64, 256, 1024, 4096];
        for &size in &value_sizes {
            let tmpdir = tempfile::tempdir().unwrap();
            let config = StorageConfig::default();
            let backend = RedbBackend::open(tmpdir.path(), &config).unwrap();
            let value = make_value(size);
            let iterations: u64 = if size <= 256 { 2000 } else { 1000 };

            run_bench(
                &format!("Redb write {}B", size),
                iterations,
                || {
                    backend
                        .write(|tx| {
                            tx.insert("kv", b"bench-key", &value)?;
                            Ok(())
                        })
                        .unwrap();
                },
            );
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // Benchmark 2: MvccStorage 写入吞吐量（含加密 + Changelog）
    // ═══════════════════════════════════════════════════════════════

    #[test]
    #[ignore = "performance benchmark, run with --ignored --nocapture"]
    fn bench_mvcc_write_throughput() {
        println!("\n## 2. MvccStorage 写入吞吐量（含加密 + Changelog）\n");
        println!("| Value Size | 迭代次数 | 耗时 | 吞吐量 |");
        println!("|:---|:---|:---|:---|");

        let value_sizes = [64, 256, 1024, 4096];
        for &size in &value_sizes {
            let tmpdir = tempfile::tempdir().unwrap();
            let config = StorageConfig::default();
            let backend = RedbBackend::open(tmpdir.path(), &config).unwrap();
            let mvcc = Arc::new(MvccStorage::new(backend).unwrap());
            let value = make_value(size);
            let iterations: u64 = if size <= 256 { 2000 } else { 1000 };
            let mut counter: u64 = 0;

            run_bench(
                &format!("MvccStorage write {}B", size),
                iterations,
                || {
                    let key = format!("bench-{:08}", counter);
                    counter += 1;
                    mvcc.put(key.as_bytes(), &value, None).unwrap();
                },
            );
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // Benchmark 3: MvccStorage 读取吞吐量
    // ═══════════════════════════════════════════════════════════════

    #[test]
    #[ignore = "performance benchmark, run with --ignored --nocapture"]
    fn bench_mvcc_read_throughput() {
        println!("\n## 3. MvccStorage 读取吞吐量\n");
        println!("| 场景 | 迭代次数 | 耗时 | 吞吐量 |");
        println!("|:---|:---|:---|:---|");

        // Setup: pre-populate 1000 keys
        let tmpdir = tempfile::tempdir().unwrap();
        let config = StorageConfig::default();
        let backend = RedbBackend::open(tmpdir.path(), &config).unwrap();
        let mvcc = Arc::new(MvccStorage::new(backend).unwrap());

        let n_keys: usize = 1000;
        for i in 0..n_keys {
            let key = format!("/app/item/{:06}", i);
            mvcc.put(key.as_bytes(), format!("value-{:06}", i).as_bytes(), None)
                .unwrap();
        }

        // Single-key read
        run_bench(
            "Point read (single key)",
            20000,
            || {
                mvcc.get(b"/app/item/000500").unwrap();
            },
        );

        // Prefix scan (100 keys)
        run_bench(
            "Prefix scan (100 keys)",
            2000,
            || {
                mvcc.range(b"/app/item/000", 100).unwrap();
            },
        );

        // Prefix scan (1000 keys - all)
        run_bench(
            "Prefix scan (1000 keys)",
            500,
            || {
                mvcc.range(b"/app/item/", 0).unwrap();
            },
        );
    }

    // ═══════════════════════════════════════════════════════════════
    // Benchmark 4: MvccStorage 写入延迟分布
    // ═══════════════════════════════════════════════════════════════

    #[test]
    #[ignore = "performance benchmark, run with --ignored --nocapture"]
    fn bench_mvcc_write_latency() {
        println!("\n## 4. MvccStorage 写入延迟分布（256B value）\n");
        println!("| 指标 | 平均 | P50 | P95 | P99 |");
        println!("|:---|:---|:---|:---|:---|");

        let tmpdir = tempfile::tempdir().unwrap();
        let config = StorageConfig::default();
        let backend = RedbBackend::open(tmpdir.path(), &config).unwrap();
        let mvcc = Arc::new(MvccStorage::new(backend).unwrap());
        let value = make_value(256);
        let mut counter: u64 = 0;

        run_latency_bench("MvccStorage write latency", 1000, || {
            let key = format!("lat-{:06}", counter);
            counter += 1;
            mvcc.put(key.as_bytes(), &value, None).unwrap();
        });
    }

    // ═══════════════════════════════════════════════════════════════
    // Benchmark 5: Raft Log 持久化开销（对比裸 Redb）
    // ═══════════════════════════════════════════════════════════════

    #[test]
    #[ignore = "performance benchmark, run with --ignored --nocapture"]
    fn bench_raft_log_overhead() {
        println!("\n## 5. Raft Log 持久化开销\n");
        println!("| 操作 | 迭代次数 | 耗时 | 吞吐量 |");
        println!("|:---|:---|:---|:---|");

        // 裸 Redb 写入（基线）
        {
            let tmpdir = tempfile::tempdir().unwrap();
            let config = StorageConfig::default();
            let backend = RedbBackend::open(tmpdir.path(), &config).unwrap();
            let value = make_value(256);
            run_bench("Raw Redb (baseline)", 2000, || {
                backend
                    .write(|tx| {
                        tx.insert("kv", b"key", &value)?;
                        Ok(())
                    })
                    .unwrap();
            });
        }

        // MvccStorage 写入（含 Changelog + 加密 + 元数据）
        {
            let tmpdir = tempfile::tempdir().unwrap();
            let config = StorageConfig::default();
            let backend = RedbBackend::open(tmpdir.path(), &config).unwrap();
            let mvcc = Arc::new(MvccStorage::new(backend).unwrap());
            let value = make_value(256);
            let mut counter: u64 = 0;
            run_bench("MvccStorage (encrypted + changelog)", 1000, || {
                let key = format!("key-{:06}", counter);
                counter += 1;
                mvcc.put(key.as_bytes(), &value, None).unwrap();
            });
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // 主报告生成器
    // ═══════════════════════════════════════════════════════════════

    #[test]
    #[ignore = "performance benchmark, run with --ignored --nocapture"]
    fn bench_all() {
        println!("# Coord 性能基准测试报告\n");
        println!("> 测试环境：macOS, Rust 1.93.0, Redb 4.1.0\n");

        bench_raw_redb_write_throughput();
        bench_mvcc_write_throughput();
        bench_mvcc_read_throughput();
        bench_mvcc_write_latency();
        bench_raft_log_overhead();
        bench_multi_region_write_throughput();
        bench_value_size_impact();

        println!("\n---\n");
        println!("*报告由 `cargo test -p coord --test perf_bench -- --ignored --nocapture` 生成*");
    }

    // ═══════════════════════════════════════════════════════════════
    // Benchmark 6: 多 Region 写入吞吐量（Multi-Raft 场景模拟）
    // ═══════════════════════════════════════════════════════════════

    #[test]
    #[ignore = "performance benchmark, run with --ignored --nocapture"]
    fn bench_multi_region_write_throughput() {
        println!("\n## 6. 多 Region 写入吞吐量（共享存储引擎）\n");
        println!("| Region 数 | Keys/Region | 迭代次数 | 耗时 | 吞吐量 |");
        println!("|:---|:---|:---|:---|:---|");

        let region_counts = [1u64, 5, 10, 25];
        let value = make_value(256);

        for &num_regions in &region_counts {
            let tmpdir = tempfile::tempdir().unwrap();
            let config = StorageConfig::default();
            let backend = RedbBackend::open(tmpdir.path(), &config).unwrap();
            let mvcc = Arc::new(MvccStorage::new(backend).unwrap());
            let iterations: u64 = num_regions * 200;
            let mut counter: u64 = 0;

            run_bench(
                &format!("{} Region(s) write 256B", num_regions),
                iterations,
                || {
                    let region = counter % num_regions;
                    let key = format!("/r/{:02}/k/{:08}", region, counter);
                    counter += 1;
                    mvcc.put(key.as_bytes(), &value, None).unwrap();
                },
            );
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // Benchmark 7: 不同 Value 大小下的写入吞吐量
    // ═══════════════════════════════════════════════════════════════

    #[test]
    #[ignore = "performance benchmark, run with --ignored --nocapture"]
    fn bench_value_size_impact() {
        println!("\n## 7. Value 大小对写入吞吐量的影响（单 Region）\n");
        println!("| Value Size | 迭代次数 | 耗时 | 吞吐量 | MB/s |");
        println!("|:---|:---|:---|:---|:---|");

        let value_sizes = [64, 256, 1024, 4096, 16384, 65536];
        for &size in &value_sizes {
            let tmpdir = tempfile::tempdir().unwrap();
            let config = StorageConfig::default();
            let backend = RedbBackend::open(tmpdir.path(), &config).unwrap();
            let mvcc = Arc::new(MvccStorage::new(backend).unwrap());
            let value = make_value(size);
            let iterations: u64 = match size {
                s if s <= 256 => 2000,
                s if s <= 4096 => 1000,
                s if s <= 16384 => 500,
                _ => 200,
            };
            let mut counter: u64 = 0;

            let name = if size >= 1024 {
                format!("{}KB value", size / 1024)
            } else {
                format!("{}B value", size)
            };

            let (elapsed, iters, ops) = run_bench(
                &name,
                iterations,
                || {
                    let key = format!("/valsize/k/{:08}", counter);
                    counter += 1;
                    mvcc.put(key.as_bytes(), &value, None).unwrap();
                },
            );

            let mb_per_sec = (iters as f64 * size as f64) / elapsed.as_secs_f64() / 1_048_576.0;
            println!(
                "| {} | {} | {:?} | {:.0} ops/s | {:.1} MB/s |",
                name, iters, elapsed, ops, mb_per_sec,
            );
        }
    }
}
