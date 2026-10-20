//! KV-T G4（#154）Commit Log shard raft 组多节点验收：
//!
//! - 3 节点、region 0 + 1 个数据 Region；shard 组由 region 0 leader 首建，
//!   其余节点以 follower 身份装配同一组；
//! - 只有 shard leader 搬运记录，三个副本经 raft 收敛（多节点复制）；
//! - shard leader 移交后，新 leader 从 shipped 水位继续搬运，不丢不重；
//! - 元数据操作只由 region 0 leader 提出（follower 永不 open/seal）。
//!
//! 进程级 kill/partition 归 #175（G6 混沌套件）；本文件用 in-process raft
//! 网络覆盖同一复制/移交不变量。

mod common;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use common::bind_listener;

use coord_core::storage::StorageBackend;
use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionMeta, StorageConfig};
use coord_server::kvt::commit_log_shards::{CommitLogShardManager, CommitLogShardTick};
use coord_server::kvt::KvTRuntimeConfig;
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcServer, RaftRpcService};
use coord_server::raft::region::RegionManager;
use coord_server::raft::region_runtime::{region_data_dir, RegionRuntime, RegionRuntimeSpec};
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::type_config::CommitLogShardOp;
use coord_server::raft::{new_basic_node, new_raft, CoordRaft, RaftConfig, RaftNode};
use coord_server::storage::commit_log::{CommitChange, CommitRecord};
use coord_server::storage::mvcc::{MvccStorage, RevisionMode};
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::snapshot::SnapshotTracker;
use tempfile::TempDir;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

const NODE_IDS: [u64; 3] = [1, 2, 3];
const DATA_REGION: u64 = 1;
const DEADLINE: Duration = Duration::from_secs(30);

fn raft_config() -> RaftConfig {
    RaftConfig {
        heartbeat_interval: 100,
        election_timeout_min: 400,
        election_timeout_max: 800,
        ..Default::default()
    }
}

struct ClusterNode {
    id: u64,
    region0_raft: Arc<CoordRaft>,
    data: Arc<RegionRuntime>,
    shards: Arc<CommitLogShardManager>,
    _server: tokio::task::JoinHandle<()>,
}

impl ClusterNode {
    async fn boot(
        id: u64,
        dir: PathBuf,
        listener: tokio::net::TcpListener,
        rendezvous: &BTreeMap<u64, String>,
    ) -> Self {
        let factory = RaftNetworkFactoryImpl::new(id);
        for (peer, peer_addr) in rendezvous {
            factory.register_node(*peer, peer_addr.clone());
        }
        let service = RaftRpcService::new();

        // region 0 system raft
        let backend = RedbBackend::open(&dir, &StorageConfig::default()).expect("open region 0");
        let region0_mvcc =
            Arc::new(MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs).unwrap());
        let log = LogStore::new(&dir).await.expect("open region 0 log");
        let sm = StateMachineStore::new(
            Arc::clone(&region0_mvcc),
            dir.join("snapshots"),
            Arc::new(SnapshotTracker::default()),
        );
        let region0_raft = Arc::new(
            new_raft(id, Arc::new(raft_config()), factory.clone(), log, sm)
                .await
                .expect("create region 0 raft"),
        );
        service.set_raft(region0_raft.as_ref().clone());

        // 数据 Region（3 副本；只有 bootstrap 节点初始化成员）
        let region_manager = Arc::new(RegionManager::new(id));
        let data_meta = RegionMeta {
            region_id: DATA_REGION,
            start_key: b"".to_vec(),
            end_key: b"".to_vec(),
            epoch: RegionEpoch::initial(),
            peers: rendezvous
                .iter()
                .map(|(peer, addr)| Peer {
                    node_id: *peer,
                    raft_addr: addr.clone(),
                    role: PeerRole::Voter,
                })
                .collect(),
            approximate_size: 0,
            approximate_keys: 0,
        };
        let data = region_manager
            .spawn_region(
                &factory,
                &service,
                RegionRuntimeSpec {
                    meta: data_meta,
                    data_dir: region_data_dir(&dir, DATA_REGION),
                    raft_config: Arc::new(raft_config()),
                    object_store: None,
                    revision_mode: RevisionMode::CommitTs,
                },
                id == NODE_IDS[0],
            )
            .await
            .expect("spawn data region");

        let shards = Arc::new(CommitLogShardManager::new(
            id,
            region0_raft.as_ref().clone(),
            Arc::clone(&region0_mvcc),
            Arc::clone(&region_manager),
            factory.clone(),
            service.clone(),
            dir.clone(),
            Arc::new(raft_config()),
            KvTRuntimeConfig::default(),
        ));

        let service_for_server = service.clone();
        let server = tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(RaftRpcServer::new(service_for_server))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await;
        });

        Self {
            id,
            region0_raft,
            data,
            shards,
            _server: server,
        }
    }

    /// 每个副本在 apply 时都会写自己的 outbox；这里直接模拟该产物。
    fn seed_outbox(&self, commit_ts: u64, key: &[u8], value: &[u8]) {
        let record = CommitRecord {
            shard_id: coord_server::storage::commit_log::COMMIT_LOG_STAGING_SHARD_ID,
            commit_ts,
            txn_id: format!("txn-{commit_ts}").into_bytes(),
            start_ts: commit_ts.saturating_sub(1),
            region_id: DATA_REGION,
            txn_bytes: (key.len() + value.len() + 16) as u32,
            changes: vec![CommitChange {
                key: key.to_vec(),
                value: Some(value.to_vec()),
                prev: None,
            }],
        };
        self.data
            .mvcc
            .append_commit_record(&record, 4 * 1024 * 1024)
            .expect("append outbox");
    }

    fn record_count(&self, shard_id: u64) -> u64 {
        self.shards
            .runtime(shard_id)
            .expect("shard runtime")
            .mvcc
            .commit_log_record_count()
            .expect("read shard count")
    }

    async fn rows_at(&self, shard_id: u64, commit_ts: u64) -> usize {
        self.shards
            .records_at(shard_id, commit_ts)
            .await
            .expect("read shard rows")
            .len()
    }
}

struct ShardCluster {
    _dir: TempDir,
    nodes: Vec<ClusterNode>,
}

impl ShardCluster {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut addrs = BTreeMap::new();
        let mut listeners = Vec::new();
        for id in NODE_IDS {
            let (listener, addr) = bind_listener().await;
            addrs.insert(id, addr);
            listeners.push((id, listener));
        }

        let mut nodes = Vec::new();
        for (id, listener) in listeners {
            let node_dir = dir.path().join(format!("node-{id}"));
            std::fs::create_dir_all(&node_dir).unwrap();
            nodes.push(ClusterNode::boot(id, node_dir, listener, &addrs).await);
        }

        let members: BTreeMap<u64, RaftNode> = addrs
            .iter()
            .map(|(id, addr)| (*id, new_basic_node(addr)))
            .collect();
        nodes[0]
            .region0_raft
            .initialize(members)
            .await
            .expect("initialize region 0 cluster");

        let cluster = Self { _dir: dir, nodes };
        cluster.wait_region0_leader().await;
        cluster
    }

    fn node(&self, id: u64) -> &ClusterNode {
        self.nodes.iter().find(|node| node.id == id).expect("node")
    }

    async fn wait_region0_leader(&self) -> u64 {
        let stop = tokio::time::Instant::now() + DEADLINE;
        loop {
            let mut views = Vec::new();
            for node in &self.nodes {
                views.push(node.region0_raft.current_leader().await);
            }
            if let Some(leader) = views[0] {
                if NODE_IDS.contains(&leader) && views.iter().all(|view| *view == Some(leader)) {
                    return leader;
                }
            }
            assert!(
                tokio::time::Instant::now() < stop,
                "region 0 leader not agreed: {views:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn tick_all(&self) -> Vec<CommitLogShardTick> {
        let mut ticks = Vec::new();
        for node in &self.nodes {
            match node.shards.tick().await {
                Ok(tick) => ticks.push(tick),
                // leader 切换与 tick 并发：region 0 或 shard 组的 leader 可能刚在
                // proposal 前易主。这是可重试的竞态，不是测试失败；下一轮 tick 再试。
                Err(err)
                    if err.contains("not leader") || err.contains("has to forward request") => {}
                Err(err) => panic!("shard tick: {err}"),
            }
        }
        ticks
    }

    /// tick 到所有节点都装配了 `shards` 个 shard 组。
    async fn wait_runtimes(&self, shards: usize) {
        let stop = tokio::time::Instant::now() + DEADLINE;
        loop {
            let ok = self
                .nodes
                .iter()
                .all(|node| node.shards.runtimes().len() >= shards);
            if ok {
                return;
            }
            assert!(
                tokio::time::Instant::now() < stop,
                "shard runtimes not assembled on all nodes"
            );
            self.tick_all().await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn wait_rows(&self, shard_id: u64, commit_ts: u64, expected: usize) {
        let stop = tokio::time::Instant::now() + DEADLINE;
        loop {
            let mut ok = true;
            for node in &self.nodes {
                if node.rows_at(shard_id, commit_ts).await != expected {
                    ok = false;
                }
            }
            if ok {
                return;
            }
            assert!(
                tokio::time::Instant::now() < stop,
                "shard rows not replicated to all nodes (ts {commit_ts}, expected {expected})"
            );
            self.tick_all().await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn shard_leader(&self) -> u64 {
        let stop = tokio::time::Instant::now() + DEADLINE;
        loop {
            let mut leader = None;
            let mut agreed = true;
            for node in &self.nodes {
                let Some(rt) = node.shards.runtime(1) else {
                    agreed = false;
                    break;
                };
                let view = rt.raft.current_leader().await;
                match leader {
                    None => leader = view,
                    Some(current) if view != Some(current) => agreed = false,
                    _ => {}
                }
            }
            if agreed {
                if let Some(leader) = leader {
                    return leader;
                }
            }
            assert!(
                tokio::time::Instant::now() < stop,
                "shard leader not agreed"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_shard_group_replicates_records_on_all_nodes() {
    let cluster = ShardCluster::start().await;
    cluster.tick_all().await;
    cluster.wait_runtimes(1).await;

    let table = cluster.node(1).shards.table().unwrap();
    assert_eq!(table.shards.len(), 1, "one shard opened by region 0 leader");
    assert_eq!(table.shards[0].start_ts, 1);

    // region 0 follower 永不提出元数据变更。
    let region0_leader = cluster.wait_region0_leader().await;
    for node in &cluster.nodes {
        if node.id != region0_leader {
            let tick = node.shards.tick().await.unwrap();
            assert_eq!(tick.opened, 0, "follower node {}", node.id);
            assert_eq!(tick.sealed, 0, "follower node {}", node.id);
        }
    }

    for node in &cluster.nodes {
        node.seed_outbox(10, b"k", b"v");
    }

    // 只有 shard leader 搬运；三个副本最终都读到同一记录。
    cluster.wait_rows(1, 10, 1).await;
    let ticks = cluster.tick_all().await;
    let shippers = ticks.iter().filter(|tick| tick.shipped > 0).count();
    assert!(
        shippers <= 1,
        "at most one shard leader may ship, got {shippers}"
    );
    for node in &cluster.nodes {
        assert_eq!(
            node.record_count(1),
            1,
            "node {} has exactly one row",
            node.id
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_shard_leader_transfer_continues_without_duplicates() {
    let cluster = ShardCluster::start().await;
    cluster.tick_all().await;
    cluster.wait_runtimes(1).await;
    for node in &cluster.nodes {
        node.seed_outbox(10, b"a", b"1");
    }
    cluster.wait_rows(1, 10, 1).await;

    let old_leader = cluster.shard_leader().await;
    let target = NODE_IDS
        .into_iter()
        .find(|id| *id != old_leader)
        .expect("transfer target");
    let leader_node = cluster.node(old_leader);
    let rt = leader_node
        .shards
        .runtime(1)
        .expect("old leader shard runtime");
    rt.raft
        .trigger()
        .transfer_leader(target)
        .await
        .expect("transfer shard leadership");

    let stop = tokio::time::Instant::now() + DEADLINE;
    loop {
        let mut views = Vec::new();
        for node in &cluster.nodes {
            views.push(node.shards.runtime(1).unwrap().raft.current_leader().await);
        }
        if views.iter().all(|view| *view == Some(target)) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < stop,
            "shard leader did not move to {target}: {views:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    for node in &cluster.nodes {
        node.seed_outbox(20, b"b", b"2");
    }
    cluster.wait_rows(1, 20, 1).await;

    // 新 leader 续传，且两个 ts 各只有一条（重复搬运被幂等吸收）。
    for node in &cluster.nodes {
        assert_eq!(node.rows_at(1, 10).await, 1, "node {}", node.id);
        assert_eq!(node.rows_at(1, 20).await, 1, "node {}", node.id);
        assert_eq!(
            node.record_count(1),
            2,
            "node {} has no duplicates",
            node.id
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_metadata_follows_region0_leader_and_survives_leader_change() {
    let cluster = ShardCluster::start().await;
    cluster.tick_all().await;
    cluster.wait_runtimes(1).await;
    assert_eq!(cluster.node(1).shards.table().unwrap().shards.len(), 1);

    // 元数据提案在 follower 上被拒绝（NotLeader），不会写第二份表。
    let region0_leader = cluster.wait_region0_leader().await;
    let follower = cluster
        .nodes
        .iter()
        .find(|node| node.id != region0_leader)
        .expect("follower");
    let err = follower
        .shards
        .propose(CommitLogShardOp::Seal {
            shard_id: 1,
            now_ms: 1,
        })
        .await
        .expect_err("follower must not propose metadata");
    let message = format!("{err}");
    assert!(
        message.contains("not leader") || message.contains("not leader, leader hint unavailable"),
        "unexpected follower propose error: {message}"
    );

    // region 0 leader 移交后，新 leader 继续推进（预创建由新 leader 完成）。
    let target = NODE_IDS
        .into_iter()
        .find(|id| *id != region0_leader)
        .expect("transfer target");
    cluster
        .node(region0_leader)
        .region0_raft
        .trigger()
        .transfer_leader(target)
        .await
        .expect("transfer region 0 leadership");
    let stop = tokio::time::Instant::now() + DEADLINE;
    loop {
        let leader = cluster.wait_region0_leader().await;
        if leader == target {
            break;
        }
        let _ = leader;
        assert!(
            tokio::time::Instant::now() < stop,
            "region 0 leadership did not move to {target}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let before = cluster.node(target).shards.table().unwrap();
    assert_eq!(before.shards.len(), 1);
    let tick = cluster.node(target).shards.tick().await.unwrap();
    assert_eq!(tick.opened, 0, "an open shard already exists");

    // 关闭第一轮记录，再确认新 leader 之后仍只有一张表、边界不变。
    for node in &cluster.nodes {
        node.seed_outbox(30, b"c", b"3");
    }
    cluster.wait_rows(1, 30, 1).await;
    let after = cluster.node(target).shards.table().unwrap();
    assert_eq!(after, before, "metadata must converge without drift");
}
