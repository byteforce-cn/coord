// 对象存储（coord.storage）真实进程级 gRPC e2e（Phase A 收口）
//
// 与 in-process 套件（coord-server/tests/object_store_raft_test.rs，真实 raft
// 但无 gRPC 网络层）不同，本套件 spawn **真实 `coord server` 进程**
// （`[object_storage].enabled=true` 的 legacy/单 Region 配置），走 main.rs 的
// 配置驱动装配路径 + StorageServer 注册/健康/GC 接线，用 tonic 生成的
// `StorageClient` 验证：
//   - 流式分帧：Put（client-streaming：meta 首条 + 逐 chunk）/
//     Get（server-streaming：首条 stat + 逐 chunk），跨 4MiB chunk 边界；
//   - 4MiB RPC 边界语义：超大 chunk / 空流 / 首条非 meta / 重复 meta /
//     超声明 total_size / 字节不符 Commit → 对应 gRPC 错误码；
//   - 流式会话（`CoordStorageClient::open_put/open_put_unknown/open_get`）：
//     客户端自选块边界；**未知长度**（`total_size = -1`）在 Commit 时定长；
//   - 无覆盖写（已提交对象 Put → ALREADY_EXISTS）+ tombstone Delete +
//     NOT_FOUND 语义；
//   - 配额闸（max_total_storage_bytes 超限 → RESOURCE_EXHAUSTED，删除释放后可
//     再写）；
//   - 后台 GC：中断上传残留的 Creating 对象按 upload_timeout 回收（磁盘文件
//     同步清理），回收后同对象可重建；
//   - chunk 静态加密：启用后落盘 chunk 文件带加密头（明文不可见），读写闭环。
//
// 磁盘只读水位（ReadOnly → RESOURCE_EXHAUSTED）与 KV 写共用同一闸，由
// `server/mod.rs` 单测 test_disk_read_only_gate_rejects_writes 覆盖（进程级
// 无法低成本填满真实磁盘触发）；本套件以配额闸验证同一 Put 入口的写拒绝路径。
//
// 标记 `#[ignore]`：显式运行（本地），对齐 chaos_real / multi_raft_process 约定：
//   OBJECT_STORAGE_REAL=1 cargo test -p coord --test object_storage_process_test \
//       -- --ignored --nocapture

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use coord_proto::maintenance::maintenance_client::MaintenanceClient;
use coord_proto::maintenance::StatusRequest;
use coord_proto::storage::storage_client::StorageClient;
use coord_proto::storage::{
    get_response, put_request, DeleteRequest, DeleteResponse, GetRequest, ObjectStat, PutMeta,
    PutRequest, PutResponse, StatRequest,
};
use tonic::transport::Channel;

/// 本文件各真实进程用例共用同一把锁（对齐 multi_raft_process_test 约定）——
/// 每用例 spawn 真实 `coord server` 进程 + raft fsync，必须串行防伪超时。
static PROCESS_SUITE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn find_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

const MIB: usize = 1024 * 1024;

/// 真实进程节点（`[object_storage]` 由调用方提供整段 TOML）。
///
/// - `spawn`：单节点 legacy（现有 e2e 用例用）；
/// - `spawn_cluster`：3 节点 legacy 集群（chaos 矩阵用）——仅 id==1 bootstrap
///   并携带全部初始成员，其余节点等待 leader 复制（openraft 动态成员语义）；
///   raft 流量可经 TCP 代理（advertise=代理口、bind=真实口）注入网络分区。
struct RealNode {
    id: u64,
    grpc_port: u16,
    raft_port: u16,
    data_dir: PathBuf,
    child: Child,
}

/// 进程必杀：drop 不杀子进程会泄漏常驻 `coord server`（对齐 multi_raft 约定）。
impl Drop for RealNode {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl RealNode {
    fn spawn_common(
        id: u64,
        grpc_port: u16,
        raft_port: u16,
        data_dir: &Path,
        cfg_path: &Path,
    ) -> Self {
        let bin = env!("CARGO_BIN_EXE_coord");
        let mut cmd = Command::new(bin);
        cmd.arg("server")
            .arg("--id")
            .arg(id.to_string())
            .arg("--addr")
            .arg(format!("127.0.0.1:{grpc_port}"))
            .arg("--raft-addr")
            .arg(format!("127.0.0.1:{raft_port}"))
            .arg("--data-dir")
            .arg(data_dir)
            .arg("--config")
            .arg(cfg_path);
        // 诊断支持：OBJ_DEBUG_LOG=1 时把子进程 stdout/stderr 落盘 coord.log
        let debug_log = std::env::var("OBJ_DEBUG_LOG").is_ok();
        let rust_log = std::env::var("OBJ_RUST_LOG").unwrap_or_else(|_| "coord=warn".to_string());
        cmd.env("RUST_LOG", &rust_log);
        if debug_log {
            let log_file = std::fs::File::create(data_dir.join("coord.log")).unwrap();
            cmd.stdout(Stdio::from(log_file.try_clone().unwrap()))
                .stderr(Stdio::from(log_file));
        } else {
            cmd.stdout(Stdio::null()).stderr(Stdio::null());
        }
        let child = cmd.spawn().expect("spawn coord server");
        Self {
            id,
            grpc_port,
            raft_port,
            data_dir: data_dir.to_path_buf(),
            child,
        }
    }

    /// 单节点 legacy（cluster.bootstrap=true + 自身为唯一成员）。
    fn spawn(data_dir: &Path, grpc_port: u16, raft_port: u16, object_section: &str) -> Self {
        let cfg_dir = data_dir.join("conf");
        std::fs::create_dir_all(&cfg_dir).unwrap();
        let cfg_path = cfg_dir.join("node.toml");
        let toml = format!(
            "[cluster]\ncluster_name = \"obj-real-test\"\nbootstrap = true\n\
             [[cluster.initial_nodes]]\nid = 1\ngrpc = \"127.0.0.1:{grpc_port}\"\n\
             raft = \"127.0.0.1:{raft_port}\"\n\
             {object_section}\n\
             [security]\nauth_enabled = false\nauth_root_key = \"{}\"\n",
            "ab".repeat(32)
        );
        std::fs::write(&cfg_path, &toml).unwrap();
        Self::spawn_common(1, grpc_port, raft_port, data_dir, &cfg_path)
    }

    /// 多节点 legacy 集群节点（chaos 矩阵）：advertise 走代理口 `raft_port`、
    /// 真实监听 `raft_bind_port`（分区注入），仅 id==1 bootstrap。
    #[allow(clippy::too_many_arguments)]
    fn spawn_cluster(
        id: u64,
        grpc_port: u16,
        raft_port: u16,
        raft_bind_port: u16,
        data_dir: &Path,
        initial_nodes: &[(u64, String, String)], // (id, grpc, raft)
        object_section: &str,
    ) -> Self {
        let cfg_dir = data_dir.join("conf");
        std::fs::create_dir_all(&cfg_dir).unwrap();
        let cfg_path = cfg_dir.join("node.toml");
        let mut nodes_toml = String::new();
        for (nid, grpc, raft) in initial_nodes {
            nodes_toml.push_str(&format!(
                "[[cluster.initial_nodes]]\nid = {nid}\ngrpc = \"{grpc}\"\nraft = \"{raft}\"\n"
            ));
        }
        let bootstrap_flag = if id == 1 { "true" } else { "false" };
        let toml = format!(
            "[cluster]\ncluster_name = \"obj-chaos-test\"\nbootstrap = {bootstrap_flag}\n\
             {nodes_toml}\n\
             [network]\nraft_addr = \"127.0.0.1:{raft_port}\"\n\
             raft_bind_addr = \"127.0.0.1:{raft_bind_port}\"\n\
             {object_section}\n\
             [security]\nauth_enabled = false\nauth_root_key = \"{}\"\n",
            "ab".repeat(32)
        );
        std::fs::write(&cfg_path, toml).unwrap();
        Self::spawn_common(id, grpc_port, raft_port, data_dir, &cfg_path)
    }

    async fn wait_ready(&self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        loop {
            if let Ok(ch) = Channel::from_shared(format!("http://127.0.0.1:{}", self.grpc_port))
                .unwrap()
                .connect_timeout(Duration::from_secs(2))
                .connect()
                .await
            {
                let mut client = MaintenanceClient::new(ch);
                if client.status(StatusRequest {}).await.is_ok() {
                    return;
                }
            }
            assert!(
                Instant::now() < deadline,
                "object storage node {} not ready on :{}",
                self.id,
                self.grpc_port
            );
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }

    /// 建立 StorageClient（成功即已连接）。返回 None = 节点不可达（chaos
    /// 矩阵中节点可能正被 kill/重启，由调用方重试，不允许 panic）。
    /// 解码上限 8MiB：单 chunk 可达 4MiB，protobuf 编码消息略超 4MiB。
    async fn try_storage(&self) -> Option<StorageClient<Channel>> {
        let ch = Channel::from_shared(format!("http://127.0.0.1:{}", self.grpc_port)).ok()?;
        let ch = ch
            .connect_timeout(Duration::from_secs(3))
            .connect()
            .await
            .ok()?;
        Some(StorageClient::new(ch).max_decoding_message_size(8 * 1024 * 1024))
    }

    fn kill9(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    /// SIGSTOP 冻结（模拟停顿/时钟漂移窗口；等价于隔离一个少数派节点）
    fn pause(&mut self) {
        let pid = self.child.id();
        let _ = Command::new("kill")
            .args(["-STOP", &pid.to_string()])
            .status();
    }

    /// SIGCONT 恢复
    fn resume(&mut self) {
        let pid = self.child.id();
        let _ = Command::new("kill")
            .args(["-CONT", &pid.to_string()])
            .status();
    }

    /// 重启（同一数据目录/端口/配置，沿用已写入的 node.toml）
    fn restart(&mut self) {
        let cfg_path = self.data_dir.join("conf").join("node.toml");
        let bin = env!("CARGO_BIN_EXE_coord");
        let child = Command::new(bin)
            .arg("server")
            .arg("--id")
            .arg(self.id.to_string())
            .arg("--addr")
            .arg(format!("127.0.0.1:{}", self.grpc_port))
            .arg("--raft-addr")
            .arg(format!("127.0.0.1:{}", self.raft_port))
            .arg("--data-dir")
            .arg(&self.data_dir)
            .arg("--config")
            .arg(&cfg_path)
            .env("RUST_LOG", "coord=warn")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("respawn coord server");
        self.child = child;
    }
}

/// 节点不可达/连接失败 → 统一转为 UNAVAILABLE Status（调用方重试）
async fn stub_of(node: &RealNode) -> Result<StorageClient<Channel>, tonic::Status> {
    node.try_storage().await.ok_or_else(|| {
        tonic::Status::unavailable(format!("cannot reach node on :{}", node.grpc_port))
    })
}

/// TCP 代理分区注入器（对齐 chaos_real）：raft 流量经代理转发，`partition()`
/// 断开全部活动连接并拒绝新连接，`heal()` 恢复。
struct PartitionProxy {
    partitioned: Arc<std::sync::atomic::AtomicBool>,
    conns: Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    _accept: tokio::task::JoinHandle<()>,
}

impl PartitionProxy {
    fn start(public_port: u16, target_port: u16) -> Self {
        use std::sync::atomic::Ordering;
        let partitioned = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let conns: Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let partitioned_for_accept = Arc::clone(&partitioned);
        let conns_for_accept = Arc::clone(&conns);
        let accept = tokio::spawn(async move {
            let listener = match tokio::net::TcpListener::bind(("127.0.0.1", public_port)).await {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("PartitionProxy: bind {public_port} failed: {e}");
                    return;
                }
            };
            loop {
                let (socket, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                if partitioned_for_accept.load(Ordering::Relaxed) {
                    drop(socket);
                    continue;
                }
                let conns = Arc::clone(&conns_for_accept);
                let handle = tokio::spawn(async move {
                    let Ok(upstream) =
                        tokio::net::TcpStream::connect(("127.0.0.1", target_port)).await
                    else {
                        return;
                    };
                    let (mut rs, mut ws) = socket.into_split();
                    let (mut ut, mut uw) = upstream.into_split();
                    let a = tokio::io::copy(&mut rs, &mut uw);
                    let b = tokio::io::copy(&mut ut, &mut ws);
                    tokio::select! {
                        _ = a => {}
                        _ = b => {}
                    }
                });
                conns.lock().unwrap().push(handle);
            }
        });
        Self {
            partitioned,
            conns,
            _accept: accept,
        }
    }

    fn partition(&self) {
        use std::sync::atomic::Ordering;
        self.partitioned.store(true, Ordering::Relaxed);
        for handle in self.conns.lock().unwrap().drain(..) {
            handle.abort();
        }
    }

    fn heal(&self) {
        use std::sync::atomic::Ordering;
        self.partitioned.store(false, Ordering::Relaxed);
    }
}

// ──── Put/Get 流式辅助 ────

fn put_messages(bucket: &str, object_id: &[u8], data: &[u8], chunk: usize) -> Vec<PutRequest> {
    let mut msgs = vec![PutRequest {
        part: Some(put_request::Part::Meta(PutMeta {
            bucket: bucket.to_string(),
            object_id: object_id.to_vec(),
            total_size: data.len() as i64,
        })),
    }];
    for c in data.chunks(chunk) {
        msgs.push(PutRequest {
            part: Some(put_request::Part::Chunk(c.to_vec())),
        });
    }
    msgs
}

/// 整对象上传（默认 chunk_size 4MiB 或调用方给定）；成功返回 PutResponse。
async fn put_full(
    node: &RealNode,
    bucket: &str,
    object_id: &[u8],
    data: &[u8],
    chunk: usize,
) -> Result<PutResponse, tonic::Status> {
    let mut stub = stub_of(node).await?;
    let msgs = put_messages(bucket, object_id, data, chunk);
    let resp = stub
        .put(tonic::Request::new(tokio_stream::iter(msgs)))
        .await?;
    Ok(resp.into_inner())
}

/// 原始 Put 调用（用于错误路径/部分上传构造）。
async fn put_raw(node: &RealNode, msgs: Vec<PutRequest>) -> Result<PutResponse, tonic::Status> {
    let mut stub = stub_of(node).await?;
    let resp = stub
        .put(tonic::Request::new(tokio_stream::iter(msgs)))
        .await?;
    Ok(resp.into_inner())
}

/// Get 全量下载：断言首条为 stat，返回 (stat, 数据字节)。
async fn get_full(
    node: &RealNode,
    bucket: &str,
    object_id: &[u8],
) -> Result<(ObjectStat, Vec<u8>), tonic::Status> {
    let mut stub = stub_of(node).await?;
    let resp = stub
        .get(GetRequest {
            bucket: bucket.to_string(),
            object_id: object_id.to_vec(),
        })
        .await?;
    let mut stream = resp.into_inner();
    let mut stat: Option<ObjectStat> = None;
    let mut out = Vec::new();
    while let Some(msg) = stream.message().await? {
        match msg.part {
            Some(get_response::Part::Stat(s)) => {
                assert!(stat.is_none(), "Get must emit stat exactly once");
                stat = Some(s);
            }
            Some(get_response::Part::Chunk(c)) => out.extend_from_slice(&c),
            None => {}
        }
    }
    let stat = stat.expect("Get must emit a stat as first message");
    Ok((stat, out))
}

async fn stat_obj(
    node: &RealNode,
    bucket: &str,
    object_id: &[u8],
) -> Result<Option<ObjectStat>, tonic::Status> {
    let mut stub = stub_of(node).await?;
    let resp = stub
        .stat(StatRequest {
            bucket: bucket.to_string(),
            object_id: object_id.to_vec(),
        })
        .await;
    match resp {
        Ok(r) => Ok(r.into_inner().stat),
        Err(s) if s.code() == tonic::Code::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

async fn delete_obj(
    node: &RealNode,
    bucket: &str,
    object_id: &[u8],
) -> Result<DeleteResponse, tonic::Status> {
    let mut stub = stub_of(node).await?;
    let resp = stub
        .delete(DeleteRequest {
            bucket: bucket.to_string(),
            object_id: object_id.to_vec(),
        })
        .await?;
    Ok(resp.into_inner())
}

/// 数据模式化填充（便于加密场景在磁盘上比对明文不可见）。
fn pattern_data(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (seed as u64 + i as u64 * 131) as u8)
        .collect()
}

/// 递归收集 `<data_dir>/objects/` 下全部 chunk 文件（跳过非 chunk- 前缀）。
fn chunk_files(data_dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let root = data_dir.join("objects");
    let mut stack: Vec<PathBuf> = vec![root.clone()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p
                .file_name()
                .map(|n| n.to_string_lossy().starts_with("chunk-"))
                .unwrap_or(false)
            {
                out.push(p);
            }
        }
    }
    out
}

// ──── 用例 1：流式分帧 + 4MiB 边界 + 无覆盖写 + Delete + GC ────

#[tokio::test]
#[ignore = "real-process object storage suite; run explicitly: OBJECT_STORAGE_REAL=1"]
async fn object_storage_real_roundtrip_boundary_and_gc() {
    // E1：拒绝把「未跑」伪装成「通过」——门控变量缺失即失败。
    assert!(
        std::env::var("OBJECT_STORAGE_REAL").map(|v| !v.is_empty()).unwrap_or(false),
        "OBJECT_STORAGE_REAL must be set to run this real-process suite (E1)"
    );
    let _suite_guard = PROCESS_SUITE_LOCK.lock().await;
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();
    let node = RealNode::spawn(
        &data_dir,
        find_port(),
        find_port(),
        "[object_storage]\nenabled = true\nupload_timeout_secs = 2\ngc_interval_secs = 1\n",
    );
    node.wait_ready(Duration::from_secs(60)).await;

    let bucket = "test";
    // ── 单 chunk 小对象 ──
    let small = pattern_data(64 * 1024, 1);
    let resp = put_full(&node, bucket, b"small", &small, 4 * MIB)
        .await
        .expect("put small");
    assert_eq!(resp.size as usize, small.len());
    assert_eq!(resp.chunks, 1);

    let (stat, got) = get_full(&node, bucket, b"small").await.expect("get small");
    assert!(stat.committed && stat.exists);
    assert_eq!(stat.size as usize, small.len());
    assert_eq!(stat.chunks, 1);
    assert_eq!(got, small);

    // ── 跨 4MiB chunk 边界：总 4MiB+64KiB → 2 chunk ──
    let big_len = 4 * MIB + 64 * 1024;
    let big = pattern_data(big_len, 7);
    let resp = put_full(&node, bucket, b"big", &big, 4 * MIB)
        .await
        .expect("put big");
    assert_eq!(resp.size as usize, big_len);
    assert_eq!(resp.chunks, 2, "5MiB object must span 2 chunks");

    let (stat, got) = get_full(&node, bucket, b"big").await.expect("get big");
    assert_eq!(stat.size as usize, big_len);
    assert_eq!(stat.chunks, 2);
    assert_eq!(got, big);

    // 恰好 = chunk_size 的单 chunk 消息也应允许（≤ 4MiB）
    let exact = pattern_data(4 * MIB, 9);
    let resp = put_full(&node, bucket, b"exact", &exact, 4 * MIB)
        .await
        .expect("put exact-4MiB object");
    assert_eq!(resp.chunks, 1);
    let (_, got) = get_full(&node, bucket, b"exact").await.expect("get exact");
    assert_eq!(got, exact);

    // ── 无覆盖写：已提交对象再 Put → ALREADY_EXISTS ──
    let err = put_full(&node, bucket, b"small", &pattern_data(1024, 2), 4 * MIB)
        .await
        .expect_err("overwrite must fail");
    assert_eq!(err.code(), tonic::Code::AlreadyExists);

    // ── 4MiB 边界错误路径 ──
    // (a) 空流
    let err = put_raw(&node, vec![]).await.expect_err("empty stream");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    // (b) 首条非 meta（直接 chunk）
    let err = put_raw(
        &node,
        vec![PutRequest {
            part: Some(put_request::Part::Chunk(vec![0u8; 1024])),
        }],
    )
    .await
    .expect_err("first message must be meta");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    // (c) chunk 大于 chunk_size（4MiB+1）
    let mut msgs = put_messages(bucket, b"oversize", &pattern_data(1024, 3), 4 * MIB);
    msgs.push(PutRequest {
        part: Some(put_request::Part::Chunk(vec![0u8; 4 * MIB + 1])),
    });
    let err = put_raw(&node, msgs)
        .await
        .expect_err("chunk > chunk_size must be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    // (d) 重复 meta（meta 只能首条）
    let mut msgs = put_messages(bucket, b"dupmeta", &pattern_data(1024, 4), 4 * MIB);
    msgs.push(PutRequest {
        part: Some(put_request::Part::Meta(PutMeta {
            bucket: bucket.to_string(),
            object_id: b"dupmeta".to_vec(),
            total_size: 1024,
        })),
    });
    let err = put_raw(&node, msgs)
        .await
        .expect_err("meta must appear once");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    // (e) total_size 非法（0）
    let err = put_raw(
        &node,
        vec![PutRequest {
            part: Some(put_request::Part::Meta(PutMeta {
                bucket: bucket.to_string(),
                object_id: b"zero".to_vec(),
                total_size: 0,
            })),
        }],
    )
    .await
    .expect_err("total_size=0");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    // ── 部分上传（字节数不足声明）→ Commit 拒绝 + 残留 Creating 由 GC 回收 ──
    // 声明 total_size=4MiB，只发 1KiB 后正常结束流 → 服务端 Commit 时
    // size(1KiB) != total(4MiB) → INVALID_ARGUMENT
    let mut msgs = vec![PutRequest {
        part: Some(put_request::Part::Meta(PutMeta {
            bucket: bucket.to_string(),
            object_id: b"partial".to_vec(),
            total_size: 4 * MIB as i64,
        })),
    }];
    msgs.push(PutRequest {
        part: Some(put_request::Part::Chunk(pattern_data(1024, 5))),
    });
    let err = put_raw(&node, msgs)
        .await
        .expect_err("short upload must fail at commit");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    // 上传中断瞬间：manifest 存在（Creating，committed=false）
    let st = stat_obj(&node, bucket, b"partial")
        .await
        .expect("stat partial")
        .expect("partial manifest must exist before GC");
    assert!(!st.committed, "partial object must be Creating until GC");
    // GC（upload_timeout=2s，tick=1s）回收 → Stat NOT_FOUND
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if stat_obj(&node, bucket, b"partial")
            .await
            .expect("stat partial")
            .is_none()
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "GC did not reclaim partial Creating object within 30s"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    // 回收后同对象可重建
    let resp = put_full(&node, bucket, b"partial", &pattern_data(2048, 6), 4 * MIB)
        .await
        .expect("re-put after GC");
    assert_eq!(resp.size, 2048);

    // ── Delete → tombstone + chunk 文件删除；Get/Stat NOT_FOUND；再 Delete NOT_FOUND ──
    let d = delete_obj(&node, bucket, b"big").await.expect("delete big");
    assert!(d.deleted);
    let err = get_full(&node, bucket, b"big")
        .await
        .expect_err("get deleted");
    assert_eq!(err.code(), tonic::Code::NotFound);
    assert!(stat_obj(&node, bucket, b"big")
        .await
        .expect("stat")
        .is_none());
    // 对不存在对象 Delete → NOT_FOUND（v1 语义，见 proto 注释）
    let err = delete_obj(&node, bucket, b"big")
        .await
        .expect_err("delete missing must be NOT_FOUND");
    assert_eq!(err.code(), tonic::Code::NotFound);
}

// ──── 用例 2：配额闸（超限 RESOURCE_EXHAUSTED + 删除释放后可再写）────

#[tokio::test]
#[ignore = "real-process object storage suite; run explicitly: OBJECT_STORAGE_REAL=1"]
async fn object_storage_real_quota_resource_exhausted() {
    // E1：拒绝把「未跑」伪装成「通过」——门控变量缺失即失败。
    assert!(
        std::env::var("OBJECT_STORAGE_REAL").map(|v| !v.is_empty()).unwrap_or(false),
        "OBJECT_STORAGE_REAL must be set to run this real-process suite (E1)"
    );
    let _suite_guard = PROCESS_SUITE_LOCK.lock().await;
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();
    // 配额 2MiB；chunk 1MiB（含加密/水位共用 Put 入口的写拒绝路径）
    let node = RealNode::spawn(
        &data_dir,
        find_port(),
        find_port(),
        "[object_storage]\nenabled = true\nchunk_size_bytes = 1048576\n\
         max_object_size_bytes = 8388608\nmax_total_storage_bytes = 2097152\n\
         upload_timeout_secs = 2\ngc_interval_secs = 1\n",
    );
    node.wait_ready(Duration::from_secs(60)).await;

    let bucket = "quota";
    let one_mib = pattern_data(1 * MIB, 11);
    put_full(&node, bucket, b"a", &one_mib, MIB)
        .await
        .expect("put a (1MiB)");
    put_full(&node, bucket, b"b", &one_mib, MIB)
        .await
        .expect("put b (1MiB)");
    // used=2MiB，新增 1MiB 超配额 → Begin 即 RESOURCE_EXHAUSTED
    let err = put_full(&node, bucket, b"c", &one_mib, MIB)
        .await
        .expect_err("quota must reject 3rd MiB");
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);

    // 删除 a → chunk 文件同步删除 → 配额释放 → c 可写入
    let d = delete_obj(&node, bucket, b"a").await.expect("delete a");
    assert!(d.deleted);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match put_full(&node, bucket, b"c", &one_mib, MIB).await {
            Ok(resp) => {
                assert_eq!(resp.size, 1 * MIB as i64);
                break;
            }
            Err(e) if e.code() == tonic::Code::ResourceExhausted => {
                assert!(
                    Instant::now() < deadline,
                    "quota did not release after delete within 30s"
                );
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            Err(e) => panic!("unexpected put error after delete: {e}"),
        }
    }
    // 三个已提交对象共存且可读
    for id in [b"b", b"c"] {
        let (st, got) = get_full(&node, bucket, id).await.expect("get committed");
        assert!(st.committed);
        assert_eq!(got, one_mib);
    }
}

// ──── 用例 3：chunk 静态加密闭环（落盘文件加密，明文不可见）────

#[tokio::test]
#[ignore = "real-process object storage suite; run explicitly: OBJECT_STORAGE_REAL=1"]
async fn object_storage_real_encryption_roundtrip() {
    // E1：拒绝把「未跑」伪装成「通过」——门控变量缺失即失败。
    assert!(
        std::env::var("OBJECT_STORAGE_REAL").map(|v| !v.is_empty()).unwrap_or(false),
        "OBJECT_STORAGE_REAL must be set to run this real-process suite (E1)"
    );
    let _suite_guard = PROCESS_SUITE_LOCK.lock().await;
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();
    let node = RealNode::spawn(
        &data_dir,
        find_port(),
        find_port(),
        &format!(
            "[object_storage]\nenabled = true\nchunk_size_bytes = 1048576\n\
             encryption_enabled = true\nencryption_root_key = \"{}\"\n",
            "cd".repeat(32)
        ),
    );
    node.wait_ready(Duration::from_secs(60)).await;

    // 1MiB 单 chunk + 3MiB 三 chunk，均走加密
    let bucket = "enc";
    let d1 = pattern_data(1 * MIB, 21);
    let d2 = pattern_data(3 * MIB, 22);
    put_full(&node, bucket, b"o1", &d1, MIB)
        .await
        .expect("put o1");
    put_full(&node, bucket, b"o2", &d2, MIB)
        .await
        .expect("put o2");

    for (id, expect) in [
        (b"o1".as_slice(), d1.as_slice()),
        (b"o2".as_slice(), d2.as_slice()),
    ] {
        let (st, got) = get_full(&node, bucket, id).await.expect("get encrypted");
        assert!(st.committed);
        assert_eq!(got.as_slice(), expect);
    }

    // 落盘 chunk 文件必须带加密头且不含明文（1MiB chunk 加密文件 > 1MiB）
    let files = chunk_files(&data_dir);
    assert_eq!(files.len(), 4, "expect 1+3 chunk files");
    let mut saw_header = false;
    for f in &files {
        let raw = std::fs::read(f).expect("read chunk file");
        // 加密文件头：magic(5) + key_id(4，v2 起) + nonce(12) → > 明文长度
        assert!(
            raw.len() > 1 * MIB,
            "encrypted chunk file {} must exceed plaintext size",
            f.display()
        );
        let magic = &raw[..5];
        // v1 格式 "COBJ1"；v2 格式 "COBJ2"（Phase C DEK 化后）；均须 >= 5+4+12
        if magic == b"COBJ1" {
            saw_header = true;
        } else if magic == b"COBJ2" {
            saw_header = true;
        }
        // 明文数据不得以原文形式出现在文件中
        for pat in [
            &d1[..min(64 * 1024, d1.len())],
            &d2[..min(64 * 1024, d2.len())],
        ] {
            assert!(
                !find_subslice(&raw, pat),
                "plaintext leaked into chunk file {}",
                f.display()
            );
        }
    }
    assert!(saw_header, "chunk files must carry encryption header magic");

    // 删除后文件清除
    delete_obj(&node, bucket, b"o1").await.expect("delete o1");
    delete_obj(&node, bucket, b"o2").await.expect("delete o2");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if chunk_files(&data_dir).is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "chunk files not removed after delete"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn min(a: usize, b: usize) -> usize {
    if a < b {
        a
    } else {
        b
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() || needle.len() > haystack.len() {
        return false;
    }
    haystack.windows(needle.len()).any(|w| w == needle)
}

// ════════════════════════════════════════════════════════════════════
// 混沌矩阵（kill / SIGSTOP pause / 网络 partition）——对象存储数据面等价
// 验收，对齐 scripts/jepsen-check.sh 的 chaos_real 约定（Rust 进程级；
// 外部 Clojure Jepsen blob 工作负载在 jepsen/lab 维护，见 README）。
// ════════════════════════════════════════════════════════════════════

/// 任一存活节点上传整对象（Put 可在 follower 发起——raft client_write 自动
/// 转发 leader）；`AlreadyExists`（中断上传残留 Creating）时先尽力 Delete。
async fn put_obj_any(
    nodes: &[&RealNode],
    bucket: &str,
    object_id: &[u8],
    data: &[u8],
    chunk: usize,
    deadline: Instant,
) -> bool {
    while Instant::now() < deadline {
        for node in nodes {
            match put_full(node, bucket, object_id, data, chunk).await {
                Ok(_) => return true,
                Err(e) if e.code() == tonic::Code::AlreadyExists => {
                    // 中断上传残留：删掉后整体重试
                    for n2 in nodes {
                        let _ = delete_obj(n2, bucket, object_id).await;
                    }
                }
                Err(_) => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    false
}

/// 从任一存活节点整对象下载（Get 需 leader——ReadIndex 路径；逐节点轮询）。
async fn get_obj_any(
    nodes: &[&RealNode],
    bucket: &str,
    object_id: &[u8],
    deadline: Instant,
) -> Option<Vec<u8>> {
    while Instant::now() < deadline {
        for node in nodes {
            if let Ok((_stat, data)) = get_full(node, bucket, object_id).await {
                return Some(data);
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    None
}

/// 拉取节点 HTTP `/metrics`（默认 http_addr = grpc_port + 10）。
async fn fetch_metrics(port: u16) -> Option<String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let addr = format!("127.0.0.1:{port}");
    let mut stream = tokio::net::TcpStream::connect(&addr).await.ok()?;
    let req = format!("GET /metrics HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.ok()?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.ok()?;
    String::from_utf8(buf).ok()
}

fn metric_value(text: &str, name: &str) -> Option<i64> {
    for line in text.lines() {
        let mut parts = line.split_whitespace();
        if parts.next() == Some(name) {
            return parts.next().and_then(|v| v.parse().ok());
        }
    }
    None
}

/// 进程外判定集群当前 raft leader：各节点 HTTP `/metrics` 的 `raft_leader_id`
/// 自报 == 自身 = leader 本人；选举窗口未知 = None。
async fn current_leader(nodes: &[&RealNode]) -> Option<u64> {
    let mut reported: Vec<(u64, u64)> = Vec::new();
    for n in nodes {
        if let Some(text) = fetch_metrics(n.grpc_port + 10).await {
            if let Some(l) = metric_value(&text, "raft_leader_id") {
                if l > 0 {
                    reported.push((n.id, l as u64));
                }
            }
        }
    }
    for (node, leader) in &reported {
        if node == leader {
            return Some(*leader);
        }
    }
    reported.first().map(|(_, l)| *l)
}

/// 混沌矩阵：3 节点真实集群开启 `[object_storage]`——kill -9 / SIGSTOP /
/// 网络分区注入循环下对象存储 put/get/delete 收敛一致；末尾 kill 当前 leader
/// 验证数据面在其余节点存活（新 leader 正确服务全部已提交对象）。
#[tokio::test]
#[ignore = "real-process object storage chaos; run explicitly: OBJECT_STORAGE_REAL=1"]
async fn object_storage_real_chaos_kill_pause_partition() {
    // E1：拒绝把「未跑」伪装成「通过」——门控变量缺失即失败。
    assert!(
        std::env::var("OBJECT_STORAGE_REAL").map(|v| !v.is_empty()).unwrap_or(false),
        "OBJECT_STORAGE_REAL must be set to run this real-process suite (E1)"
    );
    let _suite_guard = PROCESS_SUITE_LOCK.lock().await;
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path();

    let grpc_ports: Vec<u16> = (0..3).map(|_| find_port()).collect();
    let real_raft_ports: Vec<u16> = (0..3).map(|_| find_port()).collect();
    let raft_ports: Vec<u16> = (0..3).map(|_| find_port()).collect();
    let proxies: Vec<PartitionProxy> = (0..3)
        .map(|i| PartitionProxy::start(raft_ports[i], real_raft_ports[i]))
        .collect();
    let initial_nodes: Vec<(u64, String, String)> = (0..3)
        .map(|i| {
            (
                (i + 1) as u64,
                format!("127.0.0.1:{}", grpc_ports[i]),
                format!("127.0.0.1:{}", raft_ports[i]),
            )
        })
        .collect();

    let object_section =
        "[object_storage]\nenabled = true\nupload_timeout_secs = 2\ngc_interval_secs = 1\n";
    let mut nodes: Vec<RealNode> = (0..3)
        .map(|i| {
            RealNode::spawn_cluster(
                (i + 1) as u64,
                grpc_ports[i],
                raft_ports[i],
                real_raft_ports[i],
                &base.join(format!("node{}", i + 1)),
                &initial_nodes,
                object_section,
            )
        })
        .collect();

    for n in &nodes {
        n.wait_ready(Duration::from_secs(60)).await;
    }

    let bucket = "chaos";
    let mut committed: Vec<(Vec<u8>, Vec<u8>)> = Vec::new(); // (object_id, data)
    let mut round: u32 = 0;

    for phase in 0..9u32 {
        round += 1;
        let object_id = format!("obj-{round:04}");
        let data = pattern_data(64 * 1024, (round % 250) as u8);

        // 1) 写入并确认（健康窗口——上一轮注入已恢复）
        assert!(
            put_obj_any(
                &nodes.iter().collect::<Vec<_>>(),
                bucket,
                object_id.as_bytes(),
                &data,
                4 * MIB,
                Instant::now() + Duration::from_secs(15)
            )
            .await,
            "round {round}: object put failed"
        );
        // 2) 读回校验（写入确认后对象必须立即可读）
        let got = get_obj_any(
            &nodes.iter().collect::<Vec<_>>(),
            bucket,
            object_id.as_bytes(),
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap_or_else(|| panic!("round {round}: committed object unreadable"));
        assert_eq!(got, data, "round {round}: readback mismatch");
        committed.push((object_id.into_bytes(), data));

        // 3) 注入（kill / pause / partition 轮换），随后等待恢复
        let victim = (phase as usize) % nodes.len();
        match phase % 3 {
            0 => {
                eprintln!("chaos: kill -9 node {} and restart", nodes[victim].id);
                nodes[victim].kill9();
                tokio::time::sleep(Duration::from_millis(400)).await;
                nodes[victim].restart();
                nodes[victim].wait_ready(Duration::from_secs(30)).await;
                tokio::time::sleep(Duration::from_secs(2)).await; // 追平复制
            }
            1 => {
                eprintln!("chaos: SIGSTOP node {} for 3s", nodes[victim].id);
                nodes[victim].pause();
                tokio::time::sleep(Duration::from_secs(3)).await;
                nodes[victim].resume();
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            _ => {
                eprintln!("chaos: partition node {} for 3s", nodes[victim].id);
                proxies[victim].partition();
                tokio::time::sleep(Duration::from_secs(3)).await;
                proxies[victim].heal();
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }

    // ── 收敛：解除全部注入，全部已提交对象可从集群读出且内容一致 ──
    for p in &proxies {
        p.heal();
    }
    // 确保有 leader（kill/partition 恢复期可能短暂无 leader）
    let conv_deadline = Instant::now() + Duration::from_secs(30);
    while current_leader(&nodes.iter().collect::<Vec<_>>())
        .await
        .is_none()
        && Instant::now() < conv_deadline
    {
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    for (object_id, data) in &committed {
        let got = get_obj_any(
            &nodes.iter().collect::<Vec<_>>(),
            bucket,
            object_id,
            Instant::now() + Duration::from_secs(20),
        )
        .await
        .unwrap_or_else(|| {
            panic!(
                "converged cluster cannot read {}",
                String::from_utf8_lossy(object_id)
            )
        });
        assert_eq!(
            &got,
            data,
            "convergence mismatch on {}",
            String::from_utf8_lossy(object_id)
        );
    }

    // 末尾再写一个跨 4MiB chunk 边界的大对象，作为数据面完整性最终校验
    let big_id = b"final-big";
    let big = pattern_data(4 * MIB + 64 * 1024, 77);
    assert!(
        put_obj_any(
            &nodes.iter().collect::<Vec<_>>(),
            bucket,
            big_id,
            &big,
            4 * MIB,
            Instant::now() + Duration::from_secs(20)
        )
        .await,
        "final multi-chunk put failed"
    );

    // ── kill 当前 leader：新 leader（其余节点）必须完整服务全部对象 ──
    let leader_id = current_leader(&nodes.iter().collect::<Vec<_>>())
        .await
        .expect("leader must exist before failover kill");
    let victim_idx = nodes.iter().position(|n| n.id == leader_id).unwrap();
    eprintln!("chaos: kill current leader node {leader_id} for failover");
    nodes[victim_idx].kill9();
    // 等待其余节点选出新 leader
    let fail_deadline = Instant::now() + Duration::from_secs(30);
    let new_leader: u64;
    loop {
        if let Some(l) = current_leader(&nodes.iter().collect::<Vec<_>>()).await {
            if l != leader_id {
                new_leader = l;
                break;
            }
        }
        assert!(
            Instant::now() < fail_deadline,
            "failover did not elect new leader"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    eprintln!("chaos: new leader = node {new_leader}");

    // 新 leader 服务全部已提交对象（含多 chunk 大对象）+ 大对象读回
    for (object_id, data) in &committed {
        let got = get_obj_any(
            &nodes.iter().collect::<Vec<_>>(),
            bucket,
            object_id,
            Instant::now() + Duration::from_secs(15),
        )
        .await
        .unwrap_or_else(|| {
            panic!(
                "after failover cannot read {}",
                String::from_utf8_lossy(object_id)
            )
        });
        assert_eq!(
            &got,
            data,
            "after-failover mismatch on {}",
            String::from_utf8_lossy(object_id)
        );
    }
    let got_big = get_obj_any(
        &nodes.iter().collect::<Vec<_>>(),
        bucket,
        big_id,
        Instant::now() + Duration::from_secs(15),
    )
    .await
    .expect("after failover cannot read final-big");
    assert_eq!(got_big, big, "after-failover multi-chunk mismatch");

    for n in &mut nodes {
        n.kill9();
    }
    for p in &proxies {
        p._accept.abort();
    }
    eprintln!(
        "object_storage chaos completed: {round} rounds (kill/pause/partition), \
         {} objects + final-big verified post-failover",
        committed.len()
    );
}

// ════════════════════════════════════════════════════════════════════
// Rust SDK（coord-client）对象存储方法 —— 真实服务器闭环验收
// ════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore = "real-process object storage suite; run explicitly: OBJECT_STORAGE_REAL=1"]
async fn object_storage_real_sdk_roundtrip() {
    // E1：拒绝把「未跑」伪装成「通过」——门控变量缺失即失败。
    assert!(
        std::env::var("OBJECT_STORAGE_REAL").map(|v| !v.is_empty()).unwrap_or(false),
        "OBJECT_STORAGE_REAL must be set to run this real-process suite (E1)"
    );
    let _suite_guard = PROCESS_SUITE_LOCK.lock().await;
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();
    let grpc_port = find_port();
    let node = RealNode::spawn(
        &data_dir,
        grpc_port,
        find_port(),
        "[object_storage]\nenabled = true\n",
    );
    node.wait_ready(Duration::from_secs(60)).await;

    // coord-client 直连（leader 发现 → 连接池 → storage 子客户端）
    let client = coord_client::Client::connect_direct(coord_client::Config::new(vec![format!(
        "127.0.0.1:{grpc_port}"
    )]))
    .await
    .expect("connect coord-client");
    let storage = client.storage();

    // 单 chunk 对象
    let small = pattern_data(64 * 1024, 31);
    let res = storage.put("sdk", b"small", &small).await.expect("sdk put");
    assert_eq!(res.size as usize, small.len());
    assert_eq!(res.chunks, 1);

    // 跨 4MiB chunk 边界对象（默认 chunk 4MiB；SDK 客户端解码上限 8MiB）
    let big = pattern_data(4 * MIB + 128 * 1024, 32);
    let res = storage.put("sdk", b"big", &big).await.expect("sdk put big");
    assert_eq!(res.size as usize, big.len());
    assert_eq!(res.chunks, 2);

    let got = storage.get("sdk", b"big").await.expect("sdk get");
    assert!(got.stat.committed);
    assert_eq!(got.data, big);

    // stat / 无覆盖写 / delete
    let st = storage
        .stat("sdk", b"small")
        .await
        .expect("sdk stat")
        .expect("small exists");
    assert_eq!(st.size as usize, small.len());
    let err = storage
        .put("sdk", b"small", &pattern_data(10, 33))
        .await
        .expect_err("overwrite must fail via SDK");
    assert!(matches!(
        err,
        coord_core::error::Error::AlreadyExists { .. }
    ));

    assert!(storage.delete("sdk", b"small").await.expect("sdk delete"));
    assert!(storage
        .stat("sdk", b"small")
        .await
        .expect("sdk stat")
        .is_none());
    let err = storage
        .get("sdk", b"small")
        .await
        .expect_err("get deleted via SDK");
    assert!(matches!(err, coord_core::error::Error::NotFound { .. }));
}

// ──── 批次 10：coord-client 流式会话（ObjectWriter / ObjectReader）────

/// `StorageClient::open_put` / `open_get` 走真实进程 server：客户端**自选块边界**
/// 逐块发送（与服务端 4MiB 默认分帧不同）、服务端流逐块取回，跨 4MiB 边界往返一致；
/// 并覆盖本地越界拒绝 / `abort` / 缺失对象 `open_get`。
#[tokio::test]
#[ignore = "real-process object storage suite; run explicitly: OBJECT_STORAGE_REAL=1"]
async fn object_storage_real_streaming_sessions() {
    // E1：拒绝把「未跑」伪装成「通过」——门控变量缺失即失败。
    assert!(
        std::env::var("OBJECT_STORAGE_REAL").map(|v| !v.is_empty()).unwrap_or(false),
        "OBJECT_STORAGE_REAL must be set to run this real-process suite (E1)"
    );
    let _suite_guard = PROCESS_SUITE_LOCK.lock().await;
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();
    let grpc_port = find_port();
    let node = RealNode::spawn(
        &data_dir,
        grpc_port,
        find_port(),
        "[object_storage]\nenabled = true\n",
    );
    node.wait_ready(Duration::from_secs(60)).await;

    let client = coord_client::Client::connect_direct(coord_client::Config::new(vec![format!(
        "127.0.0.1:{grpc_port}"
    )]))
    .await
    .expect("connect coord-client");
    let storage = client.storage();

    // 4MiB + 64KiB，按 **1MiB 客户端块**发送：块边界由调用方决定（5 个 chunk），
    // 与 `put_chunked` 固定 4MiB 分帧不同 —— 这正是流式会话的意义。
    let total = 4 * MIB + 64 * 1024;
    let payload = pattern_data(total, 41);
    let mut writer = storage
        .open_put("stream", b"big", total as u64)
        .await
        .expect("open_put");
    assert_eq!(writer.total_size(), Some(total as u64));
    assert_eq!(writer.bytes_written(), 0);

    let mut written = 0u64;
    for chunk in payload.chunks(MIB) {
        written = writer.write_chunk(chunk).await.expect("write_chunk");
    }
    assert_eq!(written, total as u64);
    assert_eq!(writer.bytes_written(), total as u64);

    let out = writer.finish().await.expect("finish");
    assert_eq!(out.size as usize, total);
    assert_eq!(out.chunks, 5, "client framing must be preserved 1:1");

    // 分块下载：服务端按 chunk 记录逐条返回（5 条）
    let mut reader = storage.open_get("stream", b"big").await.expect("open_get");
    assert_eq!(reader.stat().size as usize, total);
    assert!(reader.stat().committed);
    let mut got = Vec::with_capacity(total);
    let mut sizes = Vec::new();
    while let Some(chunk) = reader.read_chunk().await.expect("read_chunk") {
        sizes.push(chunk.len());
        got.extend_from_slice(&chunk);
    }
    reader.close();
    assert_eq!(got, payload);
    assert_eq!(sizes, vec![MIB, MIB, MIB, MIB, 64 * 1024]);

    // 本地越界：声明 total < 实际写入 → 立刻拒绝（不等服务端）
    let mut w = storage
        .open_put("stream", b"oops", 1024)
        .await
        .expect("open_put small");
    w.write_chunk(&vec![7u8; 1024])
        .await
        .expect("first chunk fits");
    let err = w.write_chunk(&vec![7u8; 1]).await.expect_err("overflow");
    assert!(matches!(err, coord_core::error::Error::InvalidArgument(_)));
    w.abort();

    // 缺失对象：open_get 必须报 NotFound（而不是打开一个空会话）
    let err = storage
        .open_get("stream", b"nope")
        .await
        .expect_err("missing object");
    assert!(matches!(err, coord_core::error::Error::NotFound { .. }));

    // ── 未知长度流式上传（批次 11）：不预先声明 total_size ──
    let mut w = storage
        .open_put_unknown("stream", b"unknown")
        .await
        .expect("open_put_unknown");
    assert_eq!(
        w.total_size(),
        None,
        "unknown-length writer has no declared total"
    );
    let payload2 = pattern_data(3 * MIB + 7, 97);
    let block = 700 * 1024;
    for chunk in payload2.chunks(block) {
        w.write_chunk(chunk).await.expect("write_chunk unknown");
    }
    let out = w.finish().await.expect("finish unknown");
    assert_eq!(out.size as usize, payload2.len());
    assert_eq!(out.chunks as usize, payload2.len().div_ceil(block));

    // 读回逐块一致（stat.size 必须在 Commit 时定长）
    let mut reader = storage
        .open_get("stream", b"unknown")
        .await
        .expect("open_get unknown");
    assert_eq!(reader.stat().size as usize, payload2.len());
    assert!(reader.stat().committed);
    let mut got = Vec::with_capacity(payload2.len());
    while let Some(c) = reader.read_chunk().await.expect("read_chunk unknown") {
        got.extend_from_slice(&c);
    }
    reader.close();
    assert_eq!(got, payload2);

    // 未知长度 + 零 chunk → 服务端拒绝（至少一个 chunk）
    let w = storage
        .open_put_unknown("stream", b"empty")
        .await
        .expect("open_put_unknown empty");
    let err = w.finish().await.expect_err("empty unknown upload");
    assert!(matches!(err, coord_core::error::Error::InvalidArgument(_)));
}
