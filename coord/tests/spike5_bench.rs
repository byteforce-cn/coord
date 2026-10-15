// Spike 5 基准测试：单机单节点真实 Raft（`common::start_test_server`）采集**原始数据**。
//
// 这是决策证据，不是生产代码：只采原始样本、不做达标/不达标判定（阈值与目标硬件未定稿）。
//
// 运行（必须串行，并行测试会争抢 CPU/磁盘、污染延迟口径）：
//   CARGO_BUILD_JOBS=3 cargo test -p coord --test spike5_bench -- \
//     --ignored --nocapture --test-threads=1
//
// 原始数据落盘目录由 `SPIKE5_EVIDENCE_DIR` 指定；缺省为仓库
// `spike/2026-10/spike-5-bench/evidence`（相对本包 `CARGO_MANIFEST_DIR`）。
//
// 口径约束（为什么这样测）：
//   - 单节点单进程 Raft + gRPC，全部服务与压测客户端同机 —— 结果**不代表目标硬件**；
//   - KV 压测为**闭环**：每个 worker 串行发请求并逐请求计时（含排队延迟），
//     不含 coordinated-omission 修正；吞吐 = 同一测量窗口内 ops / 窗口秒数；
//   - 每档并发独立起 server，每 worker 一条独立连接（避免 HTTP/2
//     `max_concurrent_streams` 把并发请求串行化）；
//   - 每条样本带相对时钟 `t_us`（相对本测试首个 `Instant`），报告可据此独立复算吞吐。

mod common;

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::{BufWriter, Write};
    use std::path::{Path, PathBuf};
    use std::sync::OnceLock;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use coord_proto::kv::kv_client::KvClient;
    use coord_proto::kv::{PutRequest, RangeRequest};
    use coord_proto::txn::txn_client::TxnClient;
    use coord_proto::txn::{compare, request_op, Compare, RequestOp, TxnRequest};
    use coord_proto::watch::watch_client::WatchClient;
    use coord_proto::watch::{watch_request, WatchCreateRequest, WatchRequest};
    use serde_json::{json, Value};
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::transport::{Channel, Endpoint};

    use crate::common::start_test_server;

    // ── 串行闸（为什么） ────────────────────────────────────────────
    // 5 个基准各自起真实 server 并写盘；并行会互抢 CPU / fsync 路径、
    // 污染延迟与吞吐口径（仓库既有 perf_bench 同样要求 `--test-threads=1`）。
    // 进程内闸把这一前提内置：即使调用方未加 `--test-threads=1`，也串行执行。
    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn serialize_bench() -> std::sync::MutexGuard<'static, ()> {
        SERIAL.lock().unwrap_or_else(|e| e.into_inner())
    }

    // ── 证据落盘（原始数据逐条，不汇总） ──────────────────────────────

    fn evidence_root() -> PathBuf {
        std::env::var("SPIKE5_EVIDENCE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("../spike/2026-10/spike-5-bench/evidence")
            })
    }

    fn raw_path(name: &str) -> PathBuf {
        let dir = evidence_root().join("raw");
        std::fs::create_dir_all(&dir).expect("create evidence/raw");
        dir.join(name)
    }

    fn open_jsonl(path: &Path) -> BufWriter<File> {
        let file = File::create(path).unwrap_or_else(|e| panic!("create {}: {e}", path.display()));
        BufWriter::new(file)
    }

    fn write_line(w: &mut impl Write, v: &Value) {
        serde_json::to_writer(&mut *w, v).expect("serialize sample");
        w.write_all(b"\n").expect("write newline");
    }

    fn env_u64(key: &str, default: u64) -> u64 {
        std::env::var(key)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    fn write_json_pretty(path: &Path, v: &Value) {
        let file = File::create(path).unwrap_or_else(|e| panic!("create {}: {e}", path.display()));
        serde_json::to_writer_pretty(BufWriter::new(file), v).expect("serialize meta");
    }

    // ── 共享相对时钟 ────────────────────────────────────────────────

    static T_REF: OnceLock<Instant> = OnceLock::new();

    fn t_ref() -> Instant {
        *T_REF.get_or_init(Instant::now)
    }

    /// 相对本测试首个 `Instant` 的微秒数（跨任务共享，单调）。
    fn rel_us() -> u64 {
        t_ref().elapsed().as_micros() as u64
    }

    fn now_epoch_ns() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }

    /// 当前 1/5/15 分钟负载（作为共租户污染的上下文；测试机与其他 spike 共享）。
    fn loadavg() -> String {
        std::fs::read_to_string("/proc/loadavg")
            .unwrap_or_default()
            .trim()
            .to_string()
    }

    // ── 统计与进程资源 ──────────────────────────────────────────────

    /// 最近秩百分位（对已排序样本）；样本为空返回 0。
    fn percentile(sorted: &[u64], p: f64) -> u64 {
        if sorted.is_empty() {
            return 0;
        }
        let idx =
            (((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize).min(sorted.len() - 1);
        sorted[idx]
    }

    fn stats_us(mut v: Vec<u64>) -> (u64, u64, u64, u64) {
        v.sort_unstable();
        (
            percentile(&v, 50.0),
            percentile(&v, 95.0),
            percentile(&v, 99.0),
            percentile(&v, 100.0),
        )
    }

    /// 进程常驻内存（KiB）。服务端与压测端同进程，故该值覆盖两者之和。
    /// 4 KiB 页假设：Linux 默认页大小。
    fn rss_kib() -> u64 {
        let s = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
        s.split_whitespace()
            .nth(1)
            .and_then(|x| x.parse::<u64>().ok())
            .unwrap_or(0)
            * 4
    }

    /// 进程 CPU 累计 tick（utime, stime）。字段经 `)` 之后按空白切分，
    /// utime 为第 14 字段（索引 11）。
    fn cpu_ticks() -> (u64, u64) {
        let s = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
        let after = s.rsplit(')').next().unwrap_or("");
        let f: Vec<&str> = after.split_whitespace().collect();
        (
            f.get(11).and_then(|x| x.parse().ok()).unwrap_or(0),
            f.get(12).and_then(|x| x.parse().ok()).unwrap_or(0),
        )
    }

    fn dir_size(p: &Path) -> u64 {
        let mut total = 0u64;
        if let Ok(rd) = std::fs::read_dir(p) {
            for e in rd.flatten() {
                if let Ok(m) = e.metadata() {
                    if m.is_dir() {
                        total += dir_size(&e.path());
                    } else {
                        total += m.len();
                    }
                }
            }
        }
        total
    }

    async fn connect(addr: &str) -> Result<Channel, tonic::transport::Error> {
        Endpoint::from_shared(format!("http://{addr}"))
            .expect("valid endpoint uri")
            .connect()
            .await
    }

    fn status_text(e: &tonic::Status) -> String {
        format!("{:?}: {}", e.code(), e.message())
    }

    // ═══════════════════════════════════════════════════════════════
    // 1. KV 写延迟 / 吞吐：并发 100 / 500 / 1000
    // ═══════════════════════════════════════════════════════════════

    struct KvSample {
        t_us: u64,
        lat_us: u64,
        ok: bool,
    }

    async fn kv_worker(
        wid: u64,
        ch: Channel,
        conc: u64,
        value: Vec<u8>,
        ready_at: Instant,
        warm_end: Instant,
        meas_end: Instant,
    ) -> (u64, Vec<KvSample>, Option<String>) {
        let mut client = KvClient::new(ch);
        let mut out = Vec::new();
        let mut first_err: Option<String> = None;

        // 就绪宽限：等所有连接建好后再统一开窗。
        tokio::time::sleep_until(tokio::time::Instant::from_std(ready_at)).await;

        // 预热（不计入样本）。
        let mut wseq = 0u64;
        while Instant::now() < warm_end {
            let key = format!("spike5/kv/{conc}/{wid}/w{wseq}");
            let val = value.clone();
            if let Err(e) = client
                .put(PutRequest {
                    key: key.into_bytes(),
                    value: val,
                    ..Default::default()
                })
                .await
            {
                if first_err.is_none() {
                    first_err = Some(format!("warmup: {}", status_text(&e)));
                }
            }
            wseq += 1;
        }

        // 测量窗口：所有 worker 共用同一绝对窗口。
        let mut mseq = 0u64;
        while Instant::now() < meas_end {
            let key = format!("spike5/kv/{conc}/{wid}/m{mseq}");
            let val = value.clone();
            let t = rel_us();
            let start = Instant::now();
            let r = client
                .put(PutRequest {
                    key: key.into_bytes(),
                    value: val,
                    ..Default::default()
                })
                .await;
            let lat = start.elapsed().as_micros() as u64;
            if let Err(e) = &r {
                if first_err.is_none() {
                    first_err = Some(format!("measure: {}", status_text(e)));
                }
            }
            out.push(KvSample {
                t_us: t,
                lat_us: lat,
                ok: r.is_ok(),
            });
            mseq += 1;
        }
        (wid, out, first_err)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore = "spike5 benchmark; run spike/2026-10/spike-5-bench/scripts/repro.sh"]
    async fn bench_kv_write_throughput() {
        let _serial = serialize_bench();
        let _ = t_ref();
        let levels: Vec<u64> = std::env::var("SPIKE5_KV_LEVELS")
            .ok()
            .map(|s| s.split(',').filter_map(|x| x.trim().parse().ok()).collect())
            .unwrap_or_else(|| vec![100, 500, 1000]);
        let warmup = Duration::from_millis(env_u64("SPIKE5_KV_WARMUP_MS", 2_000));
        let measure = Duration::from_millis(env_u64("SPIKE5_KV_MEASURE_MS", 10_000));
        let value_bytes = env_u64("SPIKE5_KV_VALUE_BYTES", 256) as usize;
        let value = vec![b'x'; value_bytes];

        let out_path = raw_path("kv-write-latency.jsonl");
        let mut w = open_jsonl(&out_path);
        let mut metas: Vec<Value> = Vec::new();

        println!("\n# Spike 5 · KV 写延迟/吞吐（闭环，单节点 Raft）");
        println!("| 并发 | 窗口(s) | 提交 ops | 成功 | 失败 | 成功吞吐(ops/s) | P50(µs) | P95(µs) | P99(µs) | max(µs) |");
        println!("|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|");

        for &conc in &levels {
            let (addr, _sd, _g, _r, _tmp) = start_test_server().await;

            // 并发建立 conc 条独立连接。
            let mut cset = tokio::task::JoinSet::new();
            for _ in 0..conc {
                let a = addr.clone();
                cset.spawn(async move { connect(&a).await });
            }
            let mut chans = Vec::new();
            let mut conn_err = 0u64;
            while let Some(res) = cset.join_next().await {
                match res.expect("join connect task") {
                    Ok(c) => chans.push(c),
                    Err(_) => conn_err += 1,
                }
            }
            let connected = chans.len() as u64;

            let ready_at = Instant::now() + Duration::from_millis(300);
            let warm_end = ready_at + warmup;
            let meas_end = warm_end + measure;
            let t0_epoch_ns = now_epoch_ns();
            let started_at = now_epoch_ns();
            let loadavg_start = loadavg();

            let mut set = tokio::task::JoinSet::new();
            for (wid, ch) in chans.into_iter().enumerate() {
                let value = value.clone();
                set.spawn(async move {
                    kv_worker(wid as u64, ch, conc, value, ready_at, warm_end, meas_end).await
                });
            }

            let mut latencies: Vec<u64> = Vec::new();
            let mut errors: u64 = 0;
            let mut first_err: Option<String> = None;
            while let Some(res) = set.join_next().await {
                let (wid, samples, err) = res.expect("join kv worker");
                if let Some(e) = err {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
                for s in samples {
                    let rec = json!({
                        "conc": conc,
                        "worker": wid,
                        "t_us": s.t_us,
                        "lat_us": s.lat_us,
                        "ok": s.ok,
                        "value_bytes": value_bytes,
                    });
                    write_line(&mut w, &rec);
                    if s.ok {
                        latencies.push(s.lat_us);
                    } else {
                        errors += 1;
                    }
                }
            }
            w.flush().expect("flush kv jsonl");

            let ops = latencies.len() as u64 + errors;
            let ok = ops - errors;
            let measured_secs = measure.as_secs_f64();
            let throughput_ok = ok as f64 / measured_secs;
            let (p50, p95, p99, pmax) = stats_us(latencies.clone());
            println!(
                "| {} | {:.1} | {} | {} | {} | {:.0} | {} | {} | {} | {} |",
                conc, measured_secs, ops, ok, errors, throughput_ok, p50, p95, p99, pmax
            );
            if let Some(e) = &first_err {
                println!("  首个错误（并发 {conc}）：{e}");
            }

            metas.push(json!({
                "conc": conc,
                "connected": connected,
                "connection_errors": conn_err,
                "warmup_ms": warmup.as_millis(),
                "measure_ms": measure.as_millis(),
                "value_bytes": value_bytes,
                "total_ops": ops,
                "ok_ops": ok,
                "error_ops": errors,
                "throughput_ops_per_sec": ops as f64 / measured_secs,
                "throughput_ok_ops_per_sec": throughput_ok,
                "p50_us": p50, "p95_us": p95, "p99_us": p99, "max_us": pmax,
                "first_error": first_err,
                "t0_epoch_ns": t0_epoch_ns,
                "started_at_ns": started_at,
                "loadavg_start": loadavg_start,
                "loadavg_end": loadavg(),
                "finished_at_ns": now_epoch_ns(),
            }));
        }

        write_json_pretty(
            &raw_path("kv-write-meta.json"),
            &json!({
                "scenario": "kv_write_latency_throughput",
                "notes": "闭环压测；每样本 lat_us 含排队延迟；吞吐 = 窗口内 ops / measure_ms；每 worker 一条独立连接；每档独立 server",
                "levels": metas,
            }),
        );
        println!("\n原始样本：{}", out_path.display());
    }

    // ═══════════════════════════════════════════════════════════════
    // 2. Txn 操作数上限：阶梯递增，记录成功/失败点与错误文本
    // ═══════════════════════════════════════════════════════════════

    fn put_op(key: String, value_bytes: usize) -> RequestOp {
        RequestOp {
            op: Some(request_op::Op::RequestPut(PutRequest {
                key: key.into_bytes(),
                value: vec![b'v'; value_bytes],
                ..Default::default()
            })),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "spike5 benchmark; run spike/2026-10/spike-5-bench/scripts/repro.sh"]
    async fn bench_txn_size_limit() {
        let _serial = serialize_bench();
        let _ = t_ref();
        let (addr, _sd, _g, _r, _tmp) = start_test_server().await;
        let mut txn = TxnClient::new(connect(&addr).await.expect("connect"));

        let ladder: Vec<u64> = std::env::var("SPIKE5_TXN_LADDER")
            .ok()
            .map(|s| s.split(',').filter_map(|x| x.trim().parse().ok()).collect())
            .unwrap_or_else(|| vec![1, 2, 10, 50, 100, 127, 128, 129, 200, 500, 1000, 2000]);

        let out_path = raw_path("txn-size-ladder.jsonl");
        let mut w = open_jsonl(&out_path);

        println!("\n# Spike 5 · Txn 操作数阶梯（单节点 Raft，默认 max_txn_ops）");
        println!("| 场景 | 总操作数 | compare | puts | RPC ok | succeeded | 延迟(µs) | 错误 |");
        println!("|:--|--:|--:|--:|:--|:--|--:|:--|");

        // 2a. 纯 Put 阶梯：总操作数 = puts。
        for &n in &ladder {
            let success: Vec<RequestOp> = (0..n)
                .map(|i| put_op(format!("spike5/txn/puts/{n}/{i}"), 64))
                .collect();
            let req = TxnRequest {
                compare: vec![],
                success,
                failure: vec![],
                request_id: vec![],
            };
            let start = Instant::now();
            let r = txn.txn(req).await;
            let lat = start.elapsed().as_micros() as u64;
            let (rpc_ok, succeeded, revision, error) = match &r {
                Ok(resp) => {
                    let x = resp.get_ref();
                    (true, x.succeeded, x.revision, None)
                }
                Err(e) => (false, false, 0, Some(status_text(e))),
            };
            write_line(
                &mut w,
                &json!({
                    "scenario": "puts_only",
                    "total_ops": n,
                    "compares": 0,
                    "puts": n,
                    "rpc_ok": rpc_ok,
                    "succeeded": succeeded,
                    "revision": revision,
                    "latency_us": lat,
                    "error": error,
                }),
            );
            println!(
                "| puts_only | {} | 0 | {} | {} | {} | {} | {} |",
                n,
                n,
                rpc_ok,
                succeeded,
                lat,
                error.as_deref().unwrap_or("-")
            );
        }

        // 2b. CAS 语义：compare 也计入上限 —— 总操作数 = compares + puts。
        for &puts in &[127u64, 128u64] {
            let cmp = Compare {
                result: compare::CompareResult::Equal as i32,
                target: compare::Target::Version as i32,
                key: b"spike5/txn/cas/base".to_vec(),
                target_value: Some(compare::TargetValue::Version(0)),
            };
            let success: Vec<RequestOp> = (0..puts)
                .map(|i| put_op(format!("spike5/txn/cas/{puts}/{i}"), 64))
                .collect();
            let req = TxnRequest {
                compare: vec![cmp],
                success,
                failure: vec![],
                request_id: vec![],
            };
            let start = Instant::now();
            let r = txn.txn(req).await;
            let lat = start.elapsed().as_micros() as u64;
            let (rpc_ok, succeeded, revision, error) = match &r {
                Ok(resp) => {
                    let x = resp.get_ref();
                    (true, x.succeeded, x.revision, None)
                }
                Err(e) => (false, false, 0, Some(status_text(e))),
            };
            write_line(
                &mut w,
                &json!({
                    "scenario": "cas_compare_plus_puts",
                    "total_ops": 1 + puts,
                    "compares": 1,
                    "puts": puts,
                    "rpc_ok": rpc_ok,
                    "succeeded": succeeded,
                    "revision": revision,
                    "latency_us": lat,
                    "error": error,
                }),
            );
            println!(
                "| cas_compare_plus_puts | {} | 1 | {} | {} | {} | {} | {} |",
                1 + puts,
                puts,
                rpc_ok,
                succeeded,
                lat,
                error.as_deref().unwrap_or("-")
            );
        }

        w.flush().expect("flush txn jsonl");
        println!("\n原始样本：{}", out_path.display());
    }

    // ═══════════════════════════════════════════════════════════════
    // 3. 单值大小上限：阶梯递增，记录成功/失败点与错误文本
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "spike5 benchmark; run spike/2026-10/spike-5-bench/scripts/repro.sh"]
    async fn bench_value_size_limit() {
        let _serial = serialize_bench();
        let _ = t_ref();
        let (addr, _sd, _g, _r, _tmp) = start_test_server().await;
        let mut kv = KvClient::new(connect(&addr).await.expect("connect"));

        let sizes: Vec<u64> = std::env::var("SPIKE5_VALUE_LADDER")
            .ok()
            .map(|s| s.split(',').filter_map(|x| x.trim().parse().ok()).collect())
            .unwrap_or_else(|| {
                vec![
                    1024, 65_536, 1_048_576, 2_097_152, 4_194_276, 4_194_304, 8_388_608,
                    16_777_216, 33_554_432,
                ]
            });

        let out_path = raw_path("value-size-ladder.jsonl");
        let mut w = open_jsonl(&out_path);

        println!("\n# Spike 5 · 单值大小阶梯（单节点 Raft；服务端解码上限 4 MiB）");
        println!("| 值字节 | 写 RPC ok | 写 succeeded | 写延迟(µs) | 回读 ok | 回读字节 | 错误 |");
        println!("|--:|:--|:--|--:|:--|--:|:--|");

        for &sz in &sizes {
            let key = format!("spike5/value/{sz}");
            let value = vec![b'a'; sz as usize];
            let start = Instant::now();
            let r = kv
                .put(PutRequest {
                    key: key.clone().into_bytes(),
                    value,
                    ..Default::default()
                })
                .await;
            let lat = start.elapsed().as_micros() as u64;
            let (write_ok, revision, error) = match &r {
                Ok(resp) => (true, resp.get_ref().revision, None),
                Err(e) => (false, 0, Some(status_text(e))),
            };

            let (read_ok, read_len, read_error) = if write_ok {
                match kv
                    .range(RangeRequest {
                        key: key.clone().into_bytes(),
                        range_end: vec![],
                        limit: 0,
                        revision: 0,
                        keys_only: false,
                        count_only: false,
                    })
                    .await
                {
                    Ok(resp) => {
                        let n = resp
                            .get_ref()
                            .kvs
                            .first()
                            .map(|k| k.value.len())
                            .unwrap_or(0);
                        (true, n as u64, None)
                    }
                    Err(e) => (false, 0, Some(status_text(&e))),
                }
            } else {
                (false, 0, None)
            };

            write_line(
                &mut w,
                &json!({
                    "value_bytes": sz,
                    "write_rpc_ok": write_ok,
                    "write_revision": revision,
                    "write_latency_us": lat,
                    "read_ok": read_ok,
                    "read_bytes": read_len,
                    "write_error": error,
                    "read_error": read_error,
                }),
            );
            println!(
                "| {} | {} | {} | {} | {} | {} | {} |",
                sz,
                write_ok,
                write_ok,
                lat,
                read_ok,
                read_len,
                error.as_deref().or(read_error.as_deref()).unwrap_or("-")
            );
        }

        w.flush().expect("flush value jsonl");
        println!("\n原始样本：{}", out_path.display());
    }

    // ═══════════════════════════════════════════════════════════════
    // 4. Watch：1000 连接 + 1000 路扇出投递端到端延迟
    // ═══════════════════════════════════════════════════════════════

    #[allow(clippy::too_many_arguments)]
    async fn watch_task(
        id: u64,
        addr: String,
        key: Vec<u8>,
        ready_tx: mpsc::UnboundedSender<(u64, bool, String)>,
        ev_tx: mpsc::UnboundedSender<(u64, i64, u64)>,
    ) {
        let ch = match connect(&addr).await {
            Ok(c) => c,
            Err(e) => {
                let _ = ready_tx.send((id, false, format!("connect: {e}")));
                return;
            }
        };
        let mut client = WatchClient::new(ch);
        let (req_tx, req_rx) = mpsc::channel::<WatchRequest>(4);
        let create = WatchRequest {
            request: Some(watch_request::Request::Create(WatchCreateRequest {
                key,
                range_end: vec![],
                start_revision: 0,
                prev_kv: false,
            })),
        };
        if req_tx.send(create).await.is_err() {
            let _ = ready_tx.send((id, false, "request stream closed".to_string()));
            return;
        }
        let resp = match client
            .watch(tonic::Request::new(ReceiverStream::new(req_rx)))
            .await
        {
            Ok(r) => r,
            Err(e) => {
                let _ = ready_tx.send((id, false, status_text(&e)));
                return;
            }
        };
        // 保持请求半流打开，否则服务端可能视作客户端取消。
        let _keep_req_tx = req_tx;
        let mut stream = resp.into_inner();
        let _ = ready_tx.send((id, true, String::new()));

        while let Ok(Some(msg)) = stream.message().await {
            for ev in msg.events {
                let _ = ev_tx.send((id, ev.revision, rel_us()));
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore = "spike5 benchmark; run spike/2026-10/spike-5-bench/scripts/repro.sh"]
    async fn bench_watch_fanout() {
        let _serial = serialize_bench();
        let _ = t_ref();
        let (addr, _sd, _g, _r, _tmp) = start_test_server().await;

        let n: u64 = env_u64("SPIKE5_WATCH_WATCHERS", 1000);
        let rounds: u64 = env_u64("SPIKE5_WATCH_ROUNDS", 20);
        let round_timeout = Duration::from_millis(env_u64("SPIKE5_WATCH_ROUND_TIMEOUT_MS", 5_000));
        // 建连错峰：客户端与服务端同进程，1000 条连接瞬时并发会互相挤占
        // runtime 的 accept/握手路径（实测有 ~40% 建连失败）。错峰只影响建连速率，
        // 不影响「已建连后的投递延迟」口径。
        let stagger = Duration::from_micros(env_u64("SPIKE5_WATCH_CONNECT_STAGGER_US", 2_000));
        let key = b"spike5/watch/all".to_vec();

        let rss_before = rss_kib();
        let (cpu_u0, cpu_s0) = cpu_ticks();

        let (ready_tx, mut ready_rx) = mpsc::unbounded_channel::<(u64, bool, String)>();
        let (ev_tx, mut ev_rx) = mpsc::unbounded_channel::<(u64, i64, u64)>();
        let mut handles = Vec::new();
        for id in 0..n {
            let delay = stagger * id as u32;
            let (a, k, r, e) = (addr.clone(), key.clone(), ready_tx.clone(), ev_tx.clone());
            handles.push(tokio::spawn(async move {
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                watch_task(id, a, k, r, e).await;
            }));
        }
        drop(ready_tx);
        drop(ev_tx);

        // 汇总连接建立结果。
        let mut connected = 0u64;
        let mut conn_errors: Vec<Value> = Vec::new();
        for _ in 0..n {
            match ready_rx.recv().await {
                Some((_id, true, _)) => connected += 1,
                Some((id, false, e)) => conn_errors.push(json!({"watcher": id, "error": e})),
                None => break,
            }
        }
        let rss_after_connect = rss_kib();

        let out_path = raw_path("watch-fanout.jsonl");
        let mut w = open_jsonl(&out_path);
        let kv_addr = addr.clone();
        let mut kv = KvClient::new(connect(&kv_addr).await.expect("connect writer"));

        println!(
            "\n# Spike 5 · Watch 扇出（{} 连接，同一 key，{} 轮）",
            n, rounds
        );
        println!("| 轮次 | 期望投递 | 实收 | 丢失 | 写完成(µs) | P50(µs) | P95(µs) | P99(µs) | max(µs) |");
        println!("|--:|--:|--:|--:|--:|--:|--:|--:|--:|");

        let mut primed = 0u64;
        for round in 0..rounds {
            let value = format!("round-{round}").into_bytes();
            let t0 = rel_us();
            let put = kv
                .put(PutRequest {
                    key: key.clone(),
                    value,
                    ..Default::default()
                })
                .await;
            let t_put = rel_us();
            let revision = match &put {
                Ok(r) => r.get_ref().revision,
                Err(e) => {
                    write_line(
                        &mut w,
                        &json!({
                            "round": round,
                            "put_rpc_ok": false,
                            "error": status_text(e),
                        }),
                    );
                    continue;
                }
            };

            let deadline = Instant::now() + round_timeout;
            let mut latencies: Vec<u64> = Vec::new();
            let mut delivered_ids: std::collections::HashSet<u64> =
                std::collections::HashSet::new();
            while (latencies.len() as u64) < n {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                match tokio::time::timeout(remaining, ev_rx.recv()).await {
                    Ok(Some((id, ev_rev, t))) => {
                        if ev_rev >= revision {
                            delivered_ids.insert(id);
                            let lat = t.saturating_sub(t0);
                            latencies.push(lat);
                            write_line(
                                &mut w,
                                &json!({
                                    "round": round,
                                    "watcher": id,
                                    "revision": ev_rev,
                                    "expected_revision": revision,
                                    "recv_t_us": t,
                                    "latency_us": lat,
                                }),
                            );
                        }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
            let lost = n - latencies.len() as u64;
            let (p50, p95, p99, pmax) = stats_us(latencies.clone());
            println!(
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} |",
                round,
                n,
                latencies.len(),
                lost,
                t_put - t0,
                p50,
                p95,
                p99,
                pmax
            );
            write_line(
                &mut w,
                &json!({
                    "round": round,
                    "summary": true,
                    "expected": n,
                    "delivered": latencies.len(),
                    "lost": lost,
                    "write_done_us": t_put - t0,
                    "write_revision": revision,
                    "distinct_watchers": delivered_ids.len(),
                    "p50_us": p50, "p95_us": p95, "p99_us": p99, "max_us": pmax,
                }),
            );
            if round == 0 && lost == 0 {
                primed = latencies.len() as u64;
            }
        }
        w.flush().expect("flush watch jsonl");

        let rss_after = rss_kib();
        let (cpu_u1, cpu_s1) = cpu_ticks();
        let cpu_total_ticks = (cpu_u1 - cpu_u0) + (cpu_s1 - cpu_s0);
        let hz = 100u64; // Linux 默认 USER_HZ
        write_json_pretty(
            &raw_path("watch-meta.json"),
            &json!({
                "scenario": "watch_fanout_1000",
                "notes": "所有 watcher 订阅同一 key；每轮一次 Put 后等待各订阅端事件；延迟=写发出到订阅端收到；每 watcher 一条独立连接",
                "watchers": n,
                "rounds": rounds,
                "connect_stagger_us": stagger.as_micros(),
                "round_timeout_ms": round_timeout.as_millis(),
                "loadavg": loadavg(),
                "connected": connected,
                "connection_errors": conn_errors,
                "rss_kib_before": rss_before,
                "rss_kib_after_connect": rss_after_connect,
                "rss_kib_after_rounds": rss_after,
                "rss_kib_delta": rss_after.saturating_sub(rss_before),
                "cpu_ticks_total": cpu_total_ticks,
                "cpu_seconds_total": cpu_total_ticks as f64 / hz as f64,
                "primed_first_round_delivered": primed,
            }),
        );

        for h in handles {
            h.abort();
        }
        println!("\n原始样本：{}", out_path.display());
        println!(
            "连接成功 {}/{}；RSS {}→{} KiB；CPU 累计 {:.1}s",
            connected,
            n,
            rss_before,
            rss_after,
            cpu_total_ticks as f64 / hz as f64
        );
    }

    // ═══════════════════════════════════════════════════════════════
    // 5. 存储增长：写入量与 data-dir 增量
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "spike5 benchmark; run spike/2026-10/spike-5-bench/scripts/repro.sh"]
    async fn bench_storage_growth() {
        let _serial = serialize_bench();
        let _ = t_ref();
        let (addr, _sd, _g, _r, tmpdir) = start_test_server().await;
        let mut kv = KvClient::new(connect(&addr).await.expect("connect"));

        let total: u64 = env_u64("SPIKE5_STORAGE_WRITES", 20_000);
        let sample_every: u64 = env_u64("SPIKE5_STORAGE_SAMPLE_EVERY", 1_000);
        let value_bytes: usize = env_u64("SPIKE5_STORAGE_VALUE_BYTES", 256) as usize;
        let value = vec![b'd'; value_bytes];

        let out_path = raw_path("storage-growth.jsonl");
        let mut w = open_jsonl(&out_path);

        let base = dir_size(tmpdir.path());
        write_line(
            &mut w,
            &json!({
                "ops": 0,
                "dir_bytes": base,
                "per_file": file_sizes(tmpdir.path()),
            }),
        );

        println!("\n# Spike 5 · 存储增长（单节点 Raft，{total} 次 × {value_bytes}B Put）");
        println!("| 写入数 | data-dir 字节 | 增量 | 字节/写 | 累计耗时(ms) |");
        println!("|--:|--:|--:|--:|--:|");

        let t0 = Instant::now();
        let mut errors = 0u64;
        for i in 1..=total {
            let key = format!("spike5/storage/{i:08}");
            if kv
                .put(PutRequest {
                    key: key.into_bytes(),
                    value: value.clone(),
                    ..Default::default()
                })
                .await
                .is_err()
            {
                errors += 1;
            }
            if i % sample_every == 0 || i == total {
                let dir = dir_size(tmpdir.path());
                let elapsed_ms = t0.elapsed().as_millis() as u64;
                write_line(
                    &mut w,
                    &json!({
                        "ops": i,
                        "dir_bytes": dir,
                        "per_file": file_sizes(tmpdir.path()),
                        "elapsed_ms": elapsed_ms,
                    }),
                );
                let delta = dir.saturating_sub(base);
                println!(
                    "| {} | {} | {} | {:.1} | {} |",
                    i,
                    dir,
                    delta,
                    delta as f64 / i as f64,
                    elapsed_ms
                );
            }
        }
        w.flush().expect("flush storage jsonl");

        let final_dir = dir_size(tmpdir.path());
        let delta = final_dir.saturating_sub(base);
        let bytes_per_write = delta as f64 / total as f64;
        let elapsed = t0.elapsed();
        write_json_pretty(
            &raw_path("storage-meta.json"),
            &json!({
                "scenario": "storage_growth",
                "notes": "data-dir 增量按递归文件大小求和；含 raft log + redb + changelog 等全部文件",
                "writes": total,
                "value_bytes": value_bytes,
                "sample_every": sample_every,
                "errors": errors,
                "dir_bytes_base": base,
                "dir_bytes_final": final_dir,
                "dir_bytes_delta": delta,
                "bytes_per_write": bytes_per_write,
                "bytes_per_million_writes": bytes_per_write * 1_000_000.0,
                "elapsed_secs": elapsed.as_secs_f64(),
                "ops_per_sec": total as f64 / elapsed.as_secs_f64(),
                "loadavg": loadavg(),
                "final_per_file": file_sizes(tmpdir.path()),
            }),
        );
        println!(
            "\n{total} 写，值 {value_bytes}B：data-dir {} → {}（Δ{} B，{:.1} B/写）",
            base, final_dir, delta, bytes_per_write
        );
        println!("原始样本：{}", out_path.display());
    }

    fn file_sizes(dir: &Path) -> Vec<Value> {
        let mut out = Vec::new();
        if let Ok(rd) = std::fs::read_dir(dir) {
            for e in rd.flatten() {
                if let Ok(m) = e.metadata() {
                    let name = e.file_name().to_string_lossy().to_string();
                    if m.is_dir() {
                        let sub = dir_size(&e.path());
                        out.push(json!({"name": name, "dir": true, "bytes": sub}));
                    } else {
                        out.push(json!({"name": name, "dir": false, "bytes": m.len()}));
                    }
                }
            }
        }
        out
    }
}
