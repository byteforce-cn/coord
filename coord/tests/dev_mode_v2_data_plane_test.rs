// `coord dev` 的数据面判据（ADR-0018）
//
// dev 拓扑 = v2 数据面（单节点 multi-raft + 内嵌 PD + `[kv_t]`），判据不是"配置写对了"，
// 而是真实 RPC 行为：
//   1. `coord.tso.v2.GetTs` 可用且单调（v1 没有 TSO —— 亚秒租约/超时判定的地基）；
//   2. `coord.txn.v2` 的 OCC 生效：并发写同一 key 的会话在后提交者上返回冲突；
//   3. v1 数据面（`coord.kv`）在该拓扑上 fail-closed（冻结语义与 B-KVT-2 一致，不静默成功）；
//   4. v1 布局的 dev 数据目录不被静默复用（ADR-0018 D6：要求 `--fresh`）。

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};

use coord_proto::kv::kv_client::KvClient as KvV1Client;
use coord_proto::kv::v2::kv_client::KvClient as KvV2Client;
use coord_proto::kv::v2::{PutRequest as PutV2, RangeRequest as RangeV2};
use coord_proto::tso::v2::tso_client::TsoClient;
use coord_proto::tso::v2::GetTsRequest;
use coord_proto::txn::v2::mutation::Op as MutationOp;
use coord_proto::txn::v2::txn_client::TxnClient;
use coord_proto::txn::v2::{BeginRequest, CommitRequest, Mutation, ReadRequest};

fn bindable(port: u16) -> bool {
    std::net::TcpListener::bind(("127.0.0.1", port)).is_ok()
}

fn find_free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// grpc、grpc+1（Raft）、grpc+10（BFF/UI）、agent、agent+1（Agent HTTP）全空闲。
fn pick_dev_ports() -> (u16, u16) {
    for _ in 0..50 {
        let grpc = find_free_port();
        let agent = find_free_port();
        let ports = [grpc, grpc + 1, grpc + 10, agent, agent + 1];
        let distinct: BTreeSet<u16> = ports.iter().copied().collect();
        if distinct.len() == ports.len() && ports.iter().all(|&p| bindable(p)) {
            return (grpc, agent);
        }
    }
    panic!("no free port set found for dev mode");
}

struct DevInstance {
    child: Child,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    _data_dir: tempfile::TempDir,
    _log_dir: tempfile::TempDir,
}

impl DevInstance {
    fn logs(&self) -> String {
        format!(
            "--- stdout ---\n{}\n--- stderr ---\n{}",
            std::fs::read_to_string(&self.stdout_path).unwrap_or_default(),
            std::fs::read_to_string(&self.stderr_path).unwrap_or_default()
        )
    }
}

impl Drop for DevInstance {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn_dev(grpc_port: u16, agent_port: u16) -> DevInstance {
    let data_dir = tempfile::tempdir().unwrap();
    let log_dir = tempfile::tempdir().unwrap();
    let stdout_path = log_dir.path().join("stdout.log");
    let stderr_path = log_dir.path().join("stderr.log");

    let child = Command::new(env!("CARGO_BIN_EXE_coord"))
        .arg("dev")
        .arg("--bind-addr")
        .arg("127.0.0.1")
        .arg("--grpc-port")
        .arg(grpc_port.to_string())
        .arg("--agent-port")
        .arg(agent_port.to_string())
        .arg("--data-dir")
        .arg(data_dir.path())
        .arg("--fresh")
        .stdout(Stdio::from(std::fs::File::create(&stdout_path).unwrap()))
        .stderr(Stdio::from(std::fs::File::create(&stderr_path).unwrap()))
        .spawn()
        .expect("spawn coord dev");

    DevInstance {
        child,
        stdout_path,
        stderr_path,
        _data_dir: data_dir,
        _log_dir: log_dir,
    }
}

async fn wait_agent_ready(dev: &mut DevInstance, agent_addr: &str) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        if tokio::net::TcpStream::connect(agent_addr).await.is_ok() {
            return;
        }
        if let Ok(Some(status)) = dev.child.try_wait() {
            panic!("coord dev exited early ({status}); logs:\n{}", dev.logs());
        }
        if tokio::time::Instant::now() > deadline {
            panic!(
                "agent gRPC {agent_addr} not ready within 60s; logs:\n{}",
                dev.logs()
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    }
}

fn put(key: &[u8], value: &[u8]) -> Mutation {
    Mutation {
        op: MutationOp::Put as i32,
        key: key.to_vec(),
        range_end: vec![],
        value: value.to_vec(),
        lease_id: 0,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn dev_runs_the_v2_data_plane() {
    let (grpc_port, agent_port) = pick_dev_ports();
    let agent_addr = format!("127.0.0.1:{agent_port}");
    let mut dev = spawn_dev(grpc_port, agent_port);
    wait_agent_ready(&mut dev, &agent_addr).await;

    let url = format!("http://{agent_addr}");

    // 1. TSO 可用且单调（v1 没有 TSO：这是亚秒租约/超时判定的地基）。
    let mut tso = TsoClient::connect(url.clone()).await.unwrap();
    let ts1 = tso.get_ts(GetTsRequest {}).await.unwrap().into_inner().ts;
    let ts2 = tso.get_ts(GetTsRequest {}).await.unwrap().into_inner().ts;
    assert!(
        ts1 > 0 && ts2 > ts1,
        "TSO must be usable and monotonic: {ts1} -> {ts2}"
    );

    // 2. OCC：并发写同一 key，后提交的会话必须冲突（不是"最后写入者静默胜出"）。
    let mut txn = TxnClient::connect(url.clone()).await.unwrap();
    let key = b"dev/v2/cas".to_vec();

    let a = txn
        .begin(BeginRequest {
            start_ts: 0,
            ttl_ms: 0,
        })
        .await
        .unwrap()
        .into_inner();
    // A 读该 key（空）⇒ 读集记入 A 会话。
    txn.read(ReadRequest {
        txn_id: a.txn_id.clone(),
        key: key.clone(),
        range_end: vec![],
        limit: 0,
        keys_only: false,
    })
    .await
    .unwrap();

    let mut b = TxnClient::connect(url.clone()).await.unwrap();
    let b_sess = b
        .begin(BeginRequest {
            start_ts: 0,
            ttl_ms: 0,
        })
        .await
        .unwrap()
        .into_inner();
    let b_commit = b
        .commit(CommitRequest {
            txn_id: b_sess.txn_id,
            mutations: vec![put(&key, b"from-b")],
            request_id: vec![],
        })
        .await
        .expect("B must commit")
        .into_inner();

    let conflict = txn
        .commit(CommitRequest {
            txn_id: a.txn_id,
            mutations: vec![put(&key, b"from-a")],
            request_id: vec![],
        })
        .await;
    let status = conflict.expect_err("A committed after B touched its read set — must conflict");
    assert_eq!(
        status.code(),
        tonic::Code::Aborted,
        "OCC conflict is ABORTED: {status}"
    );
    // 稳定错误锚点走 `x-coord-error-code`（`coord_core::error_code::error_code_of`
    // 是读取方），不重复在 message 里拼锚点（message 是给人看的诊断文案）。
    assert_eq!(
        coord_core::error_code::error_code_of(&status).as_deref(),
        Some("TXN_CONFLICT"),
        "conflict carries the stable anchor TXN_CONFLICT: {status}"
    );

    // 3. 落地的值来自 B（A 的写没有生效）。
    let mut kv2 = KvV2Client::connect(url.clone()).await.unwrap();
    // 读自己刚写的 commit_ts：`read_ts = 0` 只是服务端本地 safe_ts，不承诺 read-your-writes。
    let ranged = kv2
        .range(RangeV2 {
            key: key.clone(),
            range_end: vec![],
            limit: 0,
            read_ts: b_commit.commit_ts,
            keys_only: false,
            count_only: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ranged.kvs.len(), 1);
    assert_eq!(ranged.kvs[0].value, b"from-b");

    // 4. 显式 commit_ts 直写路径同样可用（v2 KV 数据面不是只有事务一条路）。
    let ts = tso.get_ts(GetTsRequest {}).await.unwrap().into_inner().ts;
    kv2.put(PutV2 {
        key: b"dev/v2/direct".to_vec(),
        value: b"direct".to_vec(),
        commit_ts: ts,
        lease_id: 0,
        prev_kv: false,
        request_id: vec![],
    })
    .await
    .expect("v2 direct put with TSO commit_ts must work");

    // 5. v1 数据面冻结：不得静默成功（B-KVT-2 的冻结语义）。
    let mut kv1 = KvV1Client::connect(url).await.unwrap();
    let frozen = kv1
        .put(coord_proto::kv::PutRequest {
            key: b"dev/v1/frozen".to_vec(),
            value: b"x".to_vec(),
            lease_id: 0,
            prev_kv: false,
            request_id: vec![],
        })
        .await;
    let status = frozen.expect_err("v1 data plane must be frozen on a v2 cluster/dev topology");
    assert_eq!(
        status.code(),
        tonic::Code::FailedPrecondition,
        "v1 freeze is FAILED_PRECONDITION (stable message): {status}"
    );
    assert!(
        status.message().contains("v1 data plane is frozen"),
        "v1 freeze keeps its stable diagnostic anchor: {status}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn dev_refuses_a_v1_data_dir() {
    let (grpc_port, agent_port) = pick_dev_ports();
    let data_dir = tempfile::tempdir().unwrap();
    // v1 布局：单 Raft 的 store（v2 拓扑下仍有 store.db，但没有 regions/ 目录）。
    std::fs::write(data_dir.path().join("store.db"), b"v1").unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_coord"))
        .arg("dev")
        .arg("--bind-addr")
        .arg("127.0.0.1")
        .arg("--grpc-port")
        .arg(grpc_port.to_string())
        .arg("--agent-port")
        .arg(agent_port.to_string())
        .arg("--data-dir")
        .arg(data_dir.path())
        .output()
        .expect("run coord dev");

    let stderr = String::from_utf8_lossy(&out.stderr);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        !out.status.success(),
        "dev must refuse a v1 data dir instead of silently starting empty; stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stderr.contains("v1 (single-Raft) data plane")
            || stdout.contains("v1 (single-Raft) data plane"),
        "refusal must name the reason and the --fresh path; stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stderr.contains("--fresh") || stdout.contains("--fresh"),
        "refusal must tell the operator to re-run with --fresh; stdout:\n{stdout}\nstderr:\n{stderr}"
    );
}
