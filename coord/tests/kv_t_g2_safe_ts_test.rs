//! KV-T G2（#142）全局 SafeTs 验收：
//!
//! - `safe_ts = min(各数据 Region 已 apply 的 commit_ts 水位)`（legacy = region 0）；
//! - region 0 发布 TSO 高水位 fence，各 Region leader 以 raft 命令抬升水位；
//! - v2 Range 的 `read_ts=0`/显式 `read_ts` 以全局 safe_ts 为准；
//! - `GetTs` 不再发放 `<= published fence` 的值；
//! - 指标暴露 safe_ts 与每 Region apply lag。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use coord_core::storage::StorageBackend;
use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionMeta, StorageConfig};
use coord_server::kvt::{KvTCapability, KvTRuntimeConfig, KvTState};
use coord_server::metrics::Metrics;
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcService};
use coord_server::raft::region::RegionManager;
use coord_server::raft::region_runtime::{region_data_dir, RegionRuntimeSpec};
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::type_config::{Command, Response};
use coord_server::raft::{new_basic_node, new_raft, RaftConfig, WatchReceiver};
use coord_server::server::CoordNode;
use coord_server::storage::mvcc::{AppliedLogId, MvccStorage, RevisionMode};
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::snapshot::SnapshotTracker;
use tempfile::TempDir;

fn find_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn raft_config() -> RaftConfig {
    RaftConfig {
        heartbeat_interval: 200,
        election_timeout_min: 800,
        election_timeout_max: 1_500,
        ..Default::default()
    }
}

#[allow(dead_code)]
fn applied(index: u64) -> AppliedLogId {
    AppliedLogId {
        term: 1,
        node_id: 1,
        index,
    }
}

/// 单节点、region 0 system raft + 2 个数据 Region 的 G2 夹具。
///
/// 数据 Region 平铺 `["", "m")` / `["m", "")`；每个 Region 单 voter，因此
/// 进程内即可选主/提交，不需要 gRPC raft RPC server。
struct G2Node {
    node: Arc<CoordNode>,
    manager: Arc<RegionManager>,
    metrics: Arc<Metrics>,
    _dir: TempDir,
    _factory: RaftNetworkFactoryImpl,
    _rpc: RaftRpcService,
}

impl G2Node {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().to_path_buf();
        let addr = format!("127.0.0.1:{}", find_port());

        // region 0 system raft（TSO/fence 的权威日志）
        let root_dir = base.join("region-0");
        std::fs::create_dir_all(&root_dir).unwrap();
        let backend = RedbBackend::open(&root_dir, &StorageConfig::default()).unwrap();
        let mvcc = Arc::new(
            MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs).unwrap(),
        );
        let root_factory = RaftNetworkFactoryImpl::new(1);
        root_factory.register_node(1, addr.clone());
        let root_tracker = Arc::new(SnapshotTracker::default());
        let root_log = LogStore::new(&root_dir).await.unwrap();
        let root_sm = StateMachineStore::new(
            Arc::clone(&mvcc),
            root_dir.join("snapshots"),
            root_tracker,
        );
        let root_raft = Arc::new(
            new_raft(
                1,
                Arc::new(raft_config()),
                root_factory,
                root_log,
                root_sm,
            )
            .await
            .expect("create region 0 raft"),
        );
        root_raft
            .initialize(BTreeMap::from([(1u64, new_basic_node(&addr))]))
            .await
            .expect("initialize region 0 raft");

        // 数据 Region 1/2（共享节点网络工厂；单 voter 不需要真实 RPC server）
        let factory = RaftNetworkFactoryImpl::new(1);
        factory.register_node(1, addr.clone());
        let rpc = RaftRpcService::new();
        let manager = Arc::new(RegionManager::new(1));
        for (region_id, start, end) in [
            (1u64, b"".to_vec(), b"m".to_vec()),
            (2u64, b"m".to_vec(), b"".to_vec()),
        ] {
            let meta = RegionMeta {
                region_id,
                start_key: start,
                end_key: end,
                epoch: RegionEpoch::initial(),
                peers: vec![Peer {
                    node_id: 1,
                    raft_addr: addr.clone(),
                    role: PeerRole::Voter,
                }],
                approximate_size: 0,
                approximate_keys: 0,
            };
            manager
                .spawn_region(
                    &factory,
                    &rpc,
                    RegionRuntimeSpec {
                        meta,
                        data_dir: region_data_dir(&base, region_id),
                        raft_config: Arc::new(raft_config()),
                        object_store: None,
                        revision_mode: RevisionMode::CommitTs,
                    },
                    true,
                )
                .await
                .expect("spawn data region");
        }

        let mut node = CoordNode::new(Arc::clone(&mvcc));
        node.node_id = 1;
        node.raft = Some(root_raft);
        node.region_manager = Some(Arc::clone(&manager));
        node.set_kvt_state(
            KvTState::new(true, KvTRuntimeConfig::default())
                .with_implemented([KvTCapability::Kv, KvTCapability::Tso]),
        );
        let metrics = Arc::new(Metrics::new());
        node.set_metrics(Arc::clone(&metrics));
        let node = Arc::new(node);

        wait_for_leaders(&node, &manager).await;

        Self {
            node,
            manager,
            metrics,
            _dir: dir,
            _factory: factory,
            _rpc: rpc,
        }
    }

    #[allow(dead_code)]
    fn runtime(&self, region_id: u64) -> Arc<coord_server::raft::region_runtime::RegionRuntime> {
        self.manager.runtime(region_id).expect("region runtime")
    }

    async fn put_v2(&self, key: &[u8], value: &[u8], commit_ts: u64) -> Response {
        let rt = self.manager.route_runtime(key).expect("route key");
        let resp = rt
            .raft
            .client_write(Command::PutV2 {
                key: key.to_vec(),
                value: value.to_vec(),
                lease_id: None,
                commit_ts,
            })
            .await
            .expect("put v2");
        resp.response().clone()
    }

    fn metrics_text(&self) -> String {
        self.metrics.render_prometheus_text()
    }
}

async fn wait_for_leaders(node: &CoordNode, manager: &RegionManager) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let root_leader = node
            .raft
            .as_ref()
            .expect("region 0 raft")
            .current_leader()
            .await
            == Some(1);
        let data_ready = (1..=2).all(|id| {
            manager.runtime(id).is_some_and(|rt| {
                let m = rt.raft.metrics();
                let m = m.borrow_watched();
                m.current_leader == Some(1) && m.last_quorum_acked.is_some()
            })
        });
        if root_leader && data_ready {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "raft leaders not elected in time"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_safe_ts_is_min_region_revision_and_metrics() {
    let g2 = G2Node::start().await;

    assert!(matches!(
        g2.put_v2(b"apple", b"v", 100).await,
        Response::Put { revision: 100 }
    ));
    assert!(matches!(
        g2.put_v2(b"melon", b"v", 60).await,
        Response::Put { revision: 60 }
    ));

    assert_eq!(
        g2.node.safe_ts(),
        60,
        "safe_ts must be the minimum applied commit_ts watermark across regions"
    );
    assert_eq!(g2.node.safe_ts_fence(), 0, "no fence published yet");

    let text = g2.metrics_text();
    assert!(text.contains("coord_kv_t_safe_ts 60"), "{text}");
    assert!(
        text.contains("coord_region_applied_revision{region_id=\"1\"} 100"),
        "{text}"
    );
    assert!(
        text.contains("coord_region_applied_revision{region_id=\"2\"} 60"),
        "{text}"
    );
    assert!(
        text.contains("coord_region_apply_lag{region_id=\"1\"} 0"),
        "{text}"
    );
    assert!(
        text.contains("coord_region_apply_lag{region_id=\"2\"} 40"),
        "{text}"
    );

    // 直接通过 trait 读 Range 的测试在 Task 5 加入；这里只验证观测面。
    let _ = Arc::clone(&g2.manager);
}
