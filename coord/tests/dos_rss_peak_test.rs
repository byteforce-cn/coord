// Gate 0 / A2 收口（W4-5 第 2 项）：**DoS 的 RSS 峰值实测口径**（进程级）
//
// 验收口径（第四轮评审修订，见
// `docs/coord-review-verification-and-remediation-2026-09-12.md:93` 与 `:305`）：
//   「100 个 8 MiB 超大请求（累计 800 MiB）→ 全部被拒（RESOURCE_EXHAUSTED）
//     且**服务端进程 RSS 峰值有界**；阈值必须在测试里**写死**，不得预设结论」。
//
// 为什么必须进程级、且只量**服务端进程**：
//   in-process 套件里客户端与服务器同进程，客户端自身的 8 MiB 级缓冲会污染读数；
//   本套件 spawn 真实 `coord server`，只读**子进程** `/proc/<pid>/status` 的
//   `VmHWM`（自该进程启动以来的峰值 RSS ⇒ 天然即"峰值实测口径"）。
//
// 可证伪性（阈值怎么来的，而不是"当前值 + ε"）：
//   · 修前形态是鉴权层对请求体做**无界** `collect()`（A2）；
//     若读路径无界，累计 800 MiB 的请求体可全部驻留服务端 ⇒ RSS 增长量级 = 800 MiB；
//   · 阈值 `RSS_PEAK_LIMIT_KIB = 256 MiB` 落在"有界"与"无界"之间；
//     实测（2026-09-23 本机）：总增长 116 MiB（2.2 倍余量），且**逐波递减**
//     （+69.5 / +29 / +14 / +6.5 MiB）——增量与 body 字节数无关，只与流数/请求数有关。
//   · 因此另设 `WAVE_DRIFT_LIMIT_KIB = 128 MiB`：首波之后峰值不得再按字节线性增长
//     （无界读会在后 3 波再叠加 600 MiB）。
//
// 两条拦截层各有判据（本测试打第 ① 条；第 ② 条由 `coord-server` 单测覆盖）：
//   ① content-length 预检：**读 body 前**拒绝（`coord-server/src/auth/interceptor.rs:724`）；
//   ② 带硬上限的读取：content-length 缺失/说谎时由 `buffer_request_body` 截断
//     （同文件 `:739`；单测在 `:1809`/`:1827`）。
//
// 参数（与评审口径对齐）：body = 8 MiB（> `MAX_SCOPE_BODY_BYTES` = 1 MiB）；
//   请求总数 = 100（累计 800 MiB）；**在飞并发 = 25**（分 4 波）。
//   并发不取 100 的原因写在这里而不是含糊带过：100 个 8 MiB 同时在飞会让**客户端**
//   （本测试进程）吃掉约 2 GiB，而本测试量的是**服务端**；25 并发单波即 200 MiB，
//   已远超"每请求 1 MiB 上限"的量级，足以让无界读暴露。
//
// 标记 `#[ignore]`（对齐 `chaos_real` / `object_storage_process_test` 等进程级套件约定）：
//   cargo test -p coord --test dos_rss_peak_test -- --ignored --nocapture

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use coord_proto::kv::kv_client::KvClient;
use coord_proto::kv::RangeRequest;
use tonic::transport::Channel;
use tonic::Code;

const MIB: usize = 1024 * 1024;
/// 单请求体：8 MiB（> 预检上限 1 MiB）
const BODY_BYTES: usize = 8 * MIB;
/// 超大请求总数（累计 800 MiB）
const TOTAL_REQUESTS: usize = 100;
/// 在飞并发（4 波 × 25）
const CONCURRENCY: usize = 25;
/// 服务端进程 RSS 峰值（VmHWM）相对基线的允许增长：256 MiB。
/// 依据见文件头"可证伪性"：无界读 ⇒ ≥800 MiB；有界 ⇒ 预期仅几 MiB。
const RSS_PEAK_LIMIT_KIB: u64 = 256 * 1024;
/// 后续波次相对**首波**允许的额外峰值增长：128 MiB。
///
/// 这条是比"绝对阈值"更锋利的判据：首波已把 25 条流的 h2 接收缓冲建满，
/// 此后每波复用同样的流数 ⇒ 峰值**不应**再按字节线性增长。
/// 若读路径按字节累积（无界 `collect()`），后 3 波会再叠加 600 MiB ⇒ 必然触线。
///
/// 实测（2026-09-23 本机，逐波 VmHWM 增量）：+69.5 / +29 / +14 / +6.5 MiB——
/// **递减**（服务端自身随请求数的审计/指标分配为主，与 body 字节数无关）。
/// 阈值取 128 MiB ≈ 观测值 2.6 倍，同时只有"无界读"保守增量的 1/5。
const WAVE_DRIFT_LIMIT_KIB: u64 = 128 * 1024;

/// 找一个空闲端口（bind:0 后释放）
fn find_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// spawn 真实 `coord server`（默认 `auth_enabled = true`，不传 `--auth-enabled`）。
/// A2 的两条拦截都在**鉴权层**（tower）里 —— 鉴权关闭时该层不安装，
/// 因此本测试必须开鉴权才有意义。
fn spawn_server(data_dir: &std::path::Path, grpc_port: u16, raft_port: u16) -> Child {
    let bin = env!("CARGO_BIN_EXE_coord");
    let log_file = std::fs::File::create(data_dir.join("server.log")).unwrap();
    Command::new(bin)
        .arg("server")
        .arg("--id")
        .arg("1")
        .arg("--bootstrap")
        .arg("--addr")
        .arg(format!("127.0.0.1:{grpc_port}"))
        .arg("--raft-addr")
        .arg(format!("127.0.0.1:{raft_port}"))
        .arg("--data-dir")
        .arg(data_dir)
        .env("COORD_ROOT_PASSWORD", "test-root-password-123")
        .env("RUST_LOG", "coord=warn")
        .stdout(Stdio::from(log_file.try_clone().unwrap()))
        .stderr(Stdio::from(log_file))
        .spawn()
        .expect("spawn coord server")
}

/// 就绪探测：HTTP `/healthz`（BFF 匿名端点，端口 = grpc + 10）
async fn wait_ready(grpc_port: u16, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(mut stream) = tokio::net::TcpStream::connect(("127.0.0.1", grpc_port + 10)).await
        {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let req = b"GET /healthz HTTP/1.0\r\nHost: localhost\r\n\r\n";
            if stream.write_all(req).await.is_ok() {
                let mut buf = [0u8; 256];
                if let Ok(Ok(n)) =
                    tokio::time::timeout(Duration::from_millis(500), stream.read(&mut buf)).await
                {
                    if n > 0 {
                        return;
                    }
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "server did not become ready within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// 读取 `/proc/<pid>/status` 的 `(VmRSS, VmHWM)`（单位 KiB）。
///
/// `VmHWM` 是**峰值**（high water mark）—— 这正是"RSS 峰值实测"的字面口径。
fn proc_rss_kib(pid: u32) -> (u64, u64) {
    let path = format!("/proc/{pid}/status");
    let status = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read {path}: {e}（本套件是 Linux-only：RSS 口径依赖 /proc）"));
    let parse = |s: &str| -> u64 {
        // 取该行里**第一个能解析成 u64 的 token**（"VmRSS:\t 9484 kB" → 9484）。
        // 不写成 `nth(1)`：那是"整个行"的位置假设；一旦调用方传进来的是去掉
        // 前缀后的剩余串，nth(1) 会拿到单位 "kB" ⇒ 解析失败 ⇒ 0 ⇒ **断言静默失效**。
        s.split_whitespace()
            .find_map(|t| t.parse::<u64>().ok())
            .unwrap_or(0)
    };
    let mut rss = 0;
    let mut hwm = 0;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            rss = parse(rest);
        } else if let Some(rest) = line.strip_prefix("VmHWM:") {
            hwm = parse(rest);
        }
    }
    (rss, hwm)
}

async fn channel(port: u16) -> Channel {
    let addr = format!("http://127.0.0.1:{port}");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match Channel::from_shared(addr.clone())
            .unwrap()
            .connect_timeout(Duration::from_secs(3))
            .connect()
            .await
        {
            Ok(ch) => return ch,
            Err(e) if Instant::now() < deadline => {
                eprintln!("gRPC channel 未就绪（重试）：{e}");
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            Err(e) => panic!("connect 127.0.0.1:{port}: {e}"),
        }
    }
}

/// 控制组：小请求 + 无凭据 ⇒ 必须是 `UNAUTHENTICATED`（就绪敏感，窗口内重试瞬时错误）。
/// 断言"鉴权层在线"：否则"超大请求被拒"可能只是被鉴权顺带拒绝的假象。
async fn assert_auth_layer_online(kv: &mut KvClient<Channel>) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let req = RangeRequest {
            key: b"/dos/control".to_vec(),
            ..Default::default()
        };
        match kv.range(req).await {
            Err(s) if s.code() == Code::Unauthenticated => return,
            Err(s)
                if matches!(
                    s.code(),
                    Code::Unavailable | Code::DeadlineExceeded | Code::Unknown
                ) && Instant::now() < deadline =>
            {
                // 瞬时传输错误：等 gRPC 端点真正可服务
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            other => panic!("控制组期望 UNAUTHENTICATED（鉴权层在线），实得 {other:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "process-level DoS/RSS drill; run explicitly（见文件头）"]
async fn dos_oversized_bodies_are_rejected_and_rss_peak_is_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let grpc_port = find_free_port();
    let raft_port = find_free_port();
    let mut child = spawn_server(dir.path(), grpc_port, raft_port);
    wait_ready(grpc_port, Duration::from_secs(60)).await;

    let pid = child.id();
    let (rss0, hwm0) = proc_rss_kib(pid);
    // 守卫：读数为 0 说明**解析失败**（而不是"内存占用为 0"）。
    // 没有这条守卫时，解析 bug 会让下面的阈值断言永远成立 —— 测量门禁变成 no-op。
    assert!(
        hwm0 > 0 && rss0 > 0,
        "无法从 /proc/{pid}/status 读到 VmRSS/VmHWM（基线 rss={rss0} hwm={hwm0}）——\
         测量口径失效比门禁失败更危险：先修测量，再谈阈值"
    );

    let mut kv = KvClient::new(channel(grpc_port).await);
    assert_auth_layer_online(&mut kv).await;

    // 载荷：TOTAL_REQUESTS 个 8 MiB 请求（4 波 × 25 并发），每波后记录 VmHWM。
    // 大体积来自 `key` 字段：服务端对 scope 承载 RPC 先看 content-length ⇒ 读 body 前拒绝。
    let mut rejected = 0usize;
    let mut unexpected: Vec<String> = Vec::new();
    let mut wave_peaks_kib: Vec<u64> = Vec::new();
    let started = Instant::now();
    for wave in 0..(TOTAL_REQUESTS / CONCURRENCY) {
        let mut handles = Vec::with_capacity(CONCURRENCY);
        for slot in 0..CONCURRENCY {
            let mut client = kv.clone();
            let idx = wave * CONCURRENCY + slot;
            handles.push(tokio::spawn(async move {
                let req = RangeRequest {
                    key: vec![b'K'; BODY_BYTES],
                    ..Default::default()
                };
                (idx, client.range(req).await)
            }));
        }
        for handle in handles {
            let (idx, result) = handle.await.expect("join 超大请求任务");
            match result {
                Err(status) if status.code() == Code::ResourceExhausted => rejected += 1,
                other => unexpected.push(format!("#{idx}: {other:?}")),
            }
        }
        // 本波结束后的峰值（VmHWM 单调不减 ⇒ 这一串读数就是"峰值随波次的变化"）
        tokio::time::sleep(Duration::from_millis(200)).await;
        wave_peaks_kib.push(proc_rss_kib(pid).1);
    }
    let elapsed = started.elapsed();

    // 给服务端收尾时间（RST_STREAM 已发、连接缓冲回收），再读峰值。
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (rss1, hwm1) = proc_rss_kib(pid);
    assert!(
        hwm1 > 0,
        "结束时无法读到 VmHWM（rss={rss1} hwm={hwm1}）——测量口径失效"
    );
    let growth_kib = hwm1.saturating_sub(hwm0);
    // 首波增长 / 末波相对首波漂移：见 WAVE_DRIFT_LIMIT_KIB 的理由
    let first_wave_kib = wave_peaks_kib[0].saturating_sub(hwm0);
    let drift_kib = hwm1.saturating_sub(wave_peaks_kib[0]);

    println!("== A2 DoS RSS 峰值实测 ==");
    println!(
        "请求：{TOTAL_REQUESTS} × {} MiB（累计 {} MiB），在飞并发 {CONCURRENCY}",
        BODY_BYTES / MIB,
        TOTAL_REQUESTS * BODY_BYTES / MIB
    );
    println!("全部被拒（RESOURCE_EXHAUSTED）：{rejected}/{TOTAL_REQUESTS}，耗时 {elapsed:?}");
    println!(
        "服务端 RSS：基线 {} KiB（VmHWM {} KiB）→ 结束时 {} KiB（VmHWM {} KiB）",
        rss0, hwm0, rss1, hwm1
    );
    println!("逐波 VmHWM（KiB）：{wave_peaks_kib:?}  ← 增量应只在首波出现");
    println!(
        "VmHWM 增长：{} KiB（{} MiB）／绝对阈值 {} KiB（{} MiB）",
        growth_kib,
        growth_kib / 1024,
        RSS_PEAK_LIMIT_KIB,
        RSS_PEAK_LIMIT_KIB / 1024
    );
    println!(
        "首波增长：{} KiB；末波相对首波的漂移：{} KiB／阈值 {} KiB",
        first_wave_kib, drift_kib, WAVE_DRIFT_LIMIT_KIB
    );

    let _ = child.kill();
    let _ = child.wait();

    assert!(
        unexpected.is_empty(),
        "存在非 RESOURCE_EXHAUSTED 的响应（{} 条）：{:?}",
        unexpected.len(),
        &unexpected[..unexpected.len().min(5)]
    );
    assert_eq!(
        rejected, TOTAL_REQUESTS,
        "被拒数不符：{rejected}/{TOTAL_REQUESTS}"
    );
    assert!(
        growth_kib < RSS_PEAK_LIMIT_KIB,
        "服务端 RSS 峰值增长 {growth_kib} KiB 超过阈值 {RSS_PEAK_LIMIT_KIB} KiB：\
         若读路径无界，累计 {} MiB 的请求体可全部驻留（本阈值即为此设）",
        TOTAL_REQUESTS * BODY_BYTES / MIB
    );
    assert!(
        drift_kib <= WAVE_DRIFT_LIMIT_KIB,
        "峰值随累计字节增长：首波增长 {first_wave_kib} KiB，末波 {wave_peaks_kib:?} \
         ⇒ 漂移 {drift_kib} KiB > {WAVE_DRIFT_LIMIT_KIB} KiB。\
         该形态与「每请求驻留 body」一致（无界读的特征），而不是有界读（应只有首波建缓冲）"
    );
}
