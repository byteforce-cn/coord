// F-69 验收测试（L1 进程内，3 节点真实 raft + 真实 gRPC loopback 网络）：
//
// 现场形态（2026-09-25 2h soak）：kill 一个 follower ⇒ 集群仍有 quorum，写继续 ⇒
// leader 的日志被 purge 越过该 follower 位置 ⇒ 复制路径选择「发送快照」⇒
// `get_snapshot()` 走主状态机。修复前主状态机槽位恒为 None（快照构建在
// builder 克隆上）⇒ openraft 把「无快照可送」升级为存储错误 ⇒ RaftCore fatal
// ⇒ 全集群写永久失败且不可自愈。
//
// 本测试用网络阻断模拟"该 follower 不可达"（比 kill 更可控，且可恢复）：
//   1. 隔离前记录 node3 的日志位置 D；
//   2. 隔离 node3，leader 继续写直到 **purge 水位 > D**（快照发送条件成立）；
//   3. 断言 leader 保持 `running_state == Ok` 且 `client_write` 持续成功
//      （修复前：快照发送读取失败 ⇒ fatal ⇒ 本步红）；
//   4. 恢复 node3 网络，断言它经 install_snapshot **跳过已 purge 的区间**追上
//      leader（last_applied ≥ purge 水位）——端到端证明快照真的送出去了。
//
// 参数压小以加速：快照策略 LogsSinceLast(20)、max_in_snapshot_log_to_keep=5、
// purge_batch_size=1（否则默认 keep=1000 需要上千条日志才推进 purge）。

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use coord_core::storage::StorageBackend;
use coord_core::types::StorageConfig;
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcServer, RaftRpcService};
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::type_config::{Command, Response};
use coord_server::raft::CoordRaft;
use coord_server::raft::{new_basic_node, new_raft, RaftConfig, WatchReceiver};
use coord_server::storage::mvcc::MvccStorage;
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::snapshot::SnapshotTracker;

fn find_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct NodeHandle {
    node_id: u64,
    raft: Arc<CoordRaft>,
    factory: RaftNetworkFactoryImpl,
    _tmp: tempfile::TempDir,
    _server: tokio::task::JoinHandle<()>,
}

fn metrics_state(n: &Arc<CoordRaft>) -> (Option<u64>, Option<u64>, bool) {
    let m = n.metrics();
    let m = m.borrow_watched();
    (
        m.last_log_index,
        m.last_applied.map(|l| l.index),
        m.running_state.is_ok(),
    )
}

fn purged_index(n: &Arc<CoordRaft>) -> u64 {
    let m = n.metrics();
    let m = m.borrow_watched();
    m.purged.as_ref().map(|l| l.index).unwrap_or(0)
}

async fn put(raft: &Arc<CoordRaft>, key: &str, value: &str) -> Result<(), String> {
    let cmd = Command::Put {
        key: key.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
        lease_id: None,
    };
    raft.client_write(cmd)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// 以「能否提交一笔写」为准找 leader（比看 metrics 稳：不会把刚降级的旧 leader
/// 或振荡期的候选者当 leader）。每次成功找到一个 leader 就消耗一个 probe 序号。
async fn writable_leader(
    nodes: &[NodeHandle],
    probe: &mut u64,
    timeout: Duration,
) -> Arc<CoordRaft> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        for n in nodes {
            let key = format!("/f69/probe/{probe}");
            if put(&n.raft, &key, "1").await.is_ok() {
                *probe += 1;
                return Arc::clone(&n.raft);
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no writable leader within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn start_node(
    node_id: u64,
    raft_addrs: &BTreeMap<u64, String>,
    initialize: bool,
) -> NodeHandle {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();

    let backend =
        RedbBackend::open(&data_dir, &StorageConfig::default()).expect("open redb backend");
    let mvcc = Arc::new(MvccStorage::new(backend).expect("create mvcc"));
    let tracker = Arc::new(SnapshotTracker::default());

    let log_store = LogStore::new(&data_dir)
        .await
        .expect("create log store")
        .with_snapshot_tracker(Arc::clone(&tracker));
    let sm_store = StateMachineStore::new(
        Arc::clone(&mvcc),
        data_dir.join("snapshots"),
        Arc::clone(&tracker),
    );

    let factory = RaftNetworkFactoryImpl::new(node_id);
    for (id, addr) in raft_addrs {
        factory.register_node(*id, addr.clone());
    }

    let mut raft_config = RaftConfig {
        heartbeat_interval: 200,
        election_timeout_min: 800,
        election_timeout_max: 1500,
        ..Default::default()
    };
    raft_config.snapshot_policy = openraft::SnapshotPolicy::LogsSinceLast(20);
    raft_config.max_in_snapshot_log_to_keep = 5;
    raft_config.purge_batch_size = 1;

    let rpc_service = RaftRpcService::new();
    let raft = new_raft(
        node_id,
        Arc::new(raft_config),
        factory.clone(),
        log_store,
        sm_store,
    )
    .await
    .expect("create raft instance");
    rpc_service.set_raft(raft.clone());

    if initialize {
        let members: BTreeMap<u64, coord_server::raft::RaftNode> = raft_addrs
            .iter()
            .map(|(id, addr)| (*id, new_basic_node(addr)))
            .collect();
        raft.initialize(members).await.expect("initialize cluster");
    }

    let raft_addr: std::net::SocketAddr = raft_addrs[&node_id].parse().expect("parse raft addr");
    let svc = RaftRpcServer::new(rpc_service);
    let server = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(svc)
            .serve(raft_addr)
            .await;
    });

    NodeHandle {
        node_id,
        raft: Arc::new(raft),
        factory,
        _tmp: tmp,
        _server: server,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_snapshot_send_to_lagging_follower_does_not_fatal_and_catches_up() {
    let raft_addrs: BTreeMap<u64, String> = (1..=3u64)
        .map(|id| (id, format!("127.0.0.1:{}", find_port())))
        .collect();

    let mut nodes = Vec::new();
    for node_id in 1..=3u64 {
        nodes.push(start_node(node_id, &raft_addrs, node_id == 1).await);
    }

    // ── 阶段 1：选出 leader，写入 30 条，等三节点追平 ──
    let mut probe = 0u64;
    let mut leader = writable_leader(&nodes, &mut probe, Duration::from_secs(30)).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    for i in 0..30u64 {
        let key = format!("/f69/init/{i:03}");
        loop {
            match put(&leader, &key, "v").await {
                Ok(()) => break,
                Err(e) => {
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "initial writes stalled: {e}"
                    );
                    leader = writable_leader(&nodes, &mut probe, Duration::from_secs(20)).await;
                }
            }
        }
    }
    loop {
        let converged = nodes.iter().all(|n| {
            let (lli, la, _) = metrics_state(&n.raft);
            lli.is_some() && lli == la
        });
        if converged {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "nodes did not converge before isolation"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // 记录 node3 隔离时的日志位置 D
    let node3 = nodes.iter().find(|n| n.node_id == 3).unwrap();
    let (d3_last, _, _) = metrics_state(&node3.raft);
    let d3 = d3_last.expect("node3 has logs");
    assert!(
        d3 >= 30,
        "node3 should have received the initial writes, got {d3}"
    );

    // ── 阶段 2：隔离 node3（双向阻断），继续写直到 purge 水位越过 D ──
    for n in &nodes {
        if n.node_id != 3 {
            n.factory.block_node(3);
        }
    }
    node3.factory.block_node(1);
    node3.factory.block_node(2);

    let mut wrote = 0u64;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    let purged_at_cross = loop {
        let key = format!("/f69/adv/{wrote:04}");
        if put(&leader, &key, "v").await.is_err() {
            // 旧 leader 可能已降级/失能（修复前的 fatal 就会走这里）——
            // 用更强的 F-69 判据报错，避免误判为"找不到 leader"
            let (_, _, ok) = metrics_state(&leader);
            assert!(
                ok,
                "leader went fatal while replication to a lagging follower was pending (F-69)"
            );
            leader = writable_leader(&nodes, &mut probe, Duration::from_secs(20)).await;
            put(&leader, &key, "v")
                .await
                .expect("write on refreshed leader");
        }
        wrote += 1;
        let p = purged_index(&leader);
        if p > d3 {
            break p;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "leader purge never advanced past node3's position (d3={d3}); \
             snapshot-send condition never materialized"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    // ── 阶段 3（F-69 判据）：快照发送条件成立后 leader 不得失能 ──
    // 轮询 5s：修复前 fatal 会在快照发送读取失败的那一刻置下，必被采到；
    // 修复后所有采样都必须是 Ok。
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let (_, _, ok) = metrics_state(&leader);
        assert!(
            ok,
            "leader must not go fatal when a snapshot must be sent to a lagging follower (F-69)"
        );
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    put(&leader, "/f69/after-attempt", "ok")
        .await
        .expect("write after snapshot attempt");
    let snap = leader.get_snapshot().await.expect("get_snapshot");
    assert!(
        snap.is_some(),
        "leader must serve a snapshot from its state machine"
    );

    // ── 阶段 4：恢复 node3，断言它经 install_snapshot 跳过已 purge 区间追上 ──
    node3.factory.unblock_node(1);
    node3.factory.unblock_node(2);
    for n in &nodes {
        if n.node_id != 3 {
            n.factory.unblock_node(3);
        }
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let (_, applied3, ok3) = metrics_state(&node3.raft);
        assert!(ok3, "node3 must stay healthy while catching up");
        if applied3.unwrap_or(0) >= purged_at_cross {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "node3 never caught up past the purge watermark ({purged_at_cross}) \
             via install_snapshot; got {applied3:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // ── 阶段 5：再次收敛（node3 追到 leader 当前水位）且写入可用 ──
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let leader = writable_leader(&nodes, &mut probe, Duration::from_secs(20)).await;
        let (_, leader_applied, _) = metrics_state(&leader);
        let (_, applied3, _) = metrics_state(&node3.raft);
        if applied3.is_some() && applied3 == leader_applied {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "cluster did not re-converge after healing: \
             leader={leader_applied:?} node3={applied3:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let leader = writable_leader(&nodes, &mut probe, Duration::from_secs(20)).await;
    let resp = leader
        .client_write(Command::Put {
            key: b"/f69/final".to_vec(),
            value: b"ok".to_vec(),
            lease_id: None,
        })
        .await
        .expect("final write");
    assert!(matches!(resp.response(), Response::Put { .. }));
}
