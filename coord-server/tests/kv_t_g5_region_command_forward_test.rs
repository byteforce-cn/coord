//! KV-T G5：split 目标组 leader 不在执行节点时的导入命令转发。
//!
//! split 目标 Region 预创建时成员是完整副本集，leader 可能落在任意节点；而
//! `client_write` 只在 leader 本地可提案。搬迁器装配 forwarder 后，`reset_range`
//! / `import_range` 必须经 `SubmitRegionCommand` 转发到目标组 leader 提出，而不是
//! 在被拒的 follower 上以 `ForwardToLeader` 失败。

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use coord_core::storage::StorageBackend;
use coord_core::types::{NodeID, RegionId};
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcServer, RaftRpcService};
use coord_server::raft::region_runtime::CoordRangeImportTarget;
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::type_config::{Command, Response};
use coord_server::raft::{new_basic_node, new_raft, RaftConfig, RaftNode, WatchReceiver};
use coord_server::split_merge::RangeImportTarget;
use coord_server::storage::mvcc::{KvMetadata, MvccStorage, RevisionMode};
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::snapshot::SnapshotTracker;

const REGION: RegionId = 7;

fn find_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct NodeHost {
    node_id: NodeID,
    raft: coord_server::raft::CoordRaft,
    mvcc: Arc<MvccStorage<RedbBackend>>,
    factory: RaftNetworkFactoryImpl,
    _dir: tempfile::TempDir,
    _serve: tokio::task::JoinHandle<()>,
}

async fn start_two_node_region() -> Vec<NodeHost> {
    let raft_addrs: BTreeMap<NodeID, String> = (1..=2u64)
        .map(|id| (id, format!("127.0.0.1:{}", find_port())))
        .collect();

    let mut hosts = Vec::new();
    for node_id in 1..=2u64 {
        let tmpdir = tempfile::tempdir().unwrap();
        let base = tmpdir.path().to_path_buf();
        let factory = RaftNetworkFactoryImpl::new(node_id);
        for (id, addr) in &raft_addrs {
            factory.register_node(*id, addr.clone());
        }
        let service = RaftRpcService::new();

        let backend = RedbBackend::open(&base, &Default::default()).expect("open backend");
        let mvcc = Arc::new(
            MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs)
                .expect("create mvcc"),
        );
        let tracker = Arc::new(SnapshotTracker::default());
        let log_store = LogStore::new(&base)
            .await
            .expect("create log store")
            .with_snapshot_tracker(Arc::clone(&tracker));
        let sm_store = StateMachineStore::new(
            Arc::clone(&mvcc),
            base.join("snapshots"),
            Arc::clone(&tracker),
        );

        let region_factory =
            coord_server::raft::network::RegionRaftNetworkFactory::new(factory.clone(), REGION);
        let raft = new_raft(
            node_id,
            Arc::new(RaftConfig {
                heartbeat_interval: 200,
                election_timeout_min: 800,
                election_timeout_max: 1500,
                ..Default::default()
            }),
            region_factory,
            log_store,
            sm_store,
        )
        .await
        .expect("create region raft");
        service.set_region_raft(REGION, raft.clone());

        if node_id == 1 {
            let members: BTreeMap<u64, RaftNode> = raft_addrs
                .iter()
                .map(|(id, addr)| (*id, new_basic_node(addr)))
                .collect();
            raft.initialize(members).await.expect("initialize region");
        }

        let addr: std::net::SocketAddr = raft_addrs[&node_id].parse().unwrap();
        let serve = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(RaftRpcServer::new(service))
                .serve(addr)
                .await;
        });

        hosts.push(NodeHost {
            node_id,
            raft,
            mvcc,
            factory,
            _dir: tmpdir,
            _serve: serve,
        });
    }
    hosts
}

/// 已确认的 leader（`last_quorum_acked` 非空才算：刚当选的节点会有一段
/// `client_write` 仍返回 `ForwardToLeader(None, None)` 的窗口）。
fn confirmed_leader(host: &NodeHost) -> Option<NodeID> {
    let metrics = host.raft.metrics();
    let metrics = metrics.borrow_watched();
    if metrics.last_quorum_acked.is_some() {
        metrics.current_leader
    } else {
        None
    }
}

async fn wait_leader(hosts: &[NodeHost]) -> NodeID {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "region {REGION} leader not elected"
        );
        for host in hosts {
            if confirmed_leader(host) == Some(host.node_id) {
                return host.node_id;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn live(key: &[u8], value: &[u8], revision: i64) -> (Vec<u8>, Vec<u8>, KvMetadata) {
    (
        key.to_vec(),
        value.to_vec(),
        KvMetadata {
            version: 1,
            create_revision: revision,
            mod_revision: revision,
            lease_id: 0,
            deleted: false,
        },
    )
}

/// follower 上的导入端口必须把命令转发到 leader 提出（旧实现直接本地
/// `client_write` ⇒ `ForwardToLeader`，split 只在执行节点恰好是目标 leader 时成功）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_import_range_forwards_to_the_target_leader() {
    let hosts = start_two_node_region().await;
    let leader_id = wait_leader(&hosts).await;
    let leader = hosts
        .iter()
        .find(|h| h.node_id == leader_id)
        .expect("leader host");
    let follower = hosts
        .iter()
        .find(|h| h.node_id != leader.node_id)
        .expect("follower host");
    assert_ne!(
        follower.raft.current_leader().await,
        Some(follower.node_id),
        "用例前提：导入端口建在 follower 上"
    );

    let port = CoordRangeImportTarget::from_raft(follower.raft.clone())
        .with_region_id(REGION)
        .with_forwarder(follower.node_id, follower.factory.clone())
        .with_leader_wait(Duration::from_secs(5));

    port.reset_range(b"m", b"")
        .await
        .expect("ClearImportRange must be forwarded to the leader");
    let imported = port
        .import_range(b"m", b"", vec![live(b"m", b"1", 10)], 10, true, true)
        .await
        .expect("ImportRange must be forwarded to the leader");
    assert_eq!(imported, 1);

    // leader 状态机拿到行；follower 副本经 raft 复制后同样可见。
    for host in [leader, follower] {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if host.mvcc.get(b"m").unwrap().as_deref() == Some(&b"1"[..]) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "node {} did not apply the forwarded import",
                host.node_id
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// 未装配转发（单节点/测试装配）时保持原行为：本地提案。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_import_range_without_forwarder_proposes_locally() {
    let hosts = start_two_node_region().await;
    let leader_id = wait_leader(&hosts).await;
    let leader = hosts
        .iter()
        .find(|h| h.node_id == leader_id)
        .expect("leader host");

    let port = CoordRangeImportTarget::from_raft(leader.raft.clone())
        .with_region_id(REGION)
        .with_leader_wait(Duration::from_secs(5));
    let imported = port
        .import_range(b"n", b"", vec![live(b"n", b"2", 11)], 11, true, true)
        .await
        .expect("local import on the leader");
    assert_eq!(imported, 1);
    assert_eq!(leader.mvcc.get(b"n").unwrap().as_deref(), Some(&b"2"[..]));

    // 未知 Region 的转发请求必须被拒（不得把命令投给别的组）。
    let err = followerless_forward(&leader, REGION + 100)
        .await
        .expect_err("unknown region must be rejected");
    assert!(err.contains("raft not initialized for region"), "{err}");
}

async fn followerless_forward(host: &NodeHost, region_id: RegionId) -> Result<Response, String> {
    host.factory
        .submit_region_command(
            host.node_id,
            region_id,
            Command::ClearImportRange {
                start_key: b"m".to_vec(),
                end_key: b"".to_vec(),
            },
        )
        .await
}
