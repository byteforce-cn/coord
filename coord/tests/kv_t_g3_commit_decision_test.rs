//! KV-T G3（#148）CommitDecision 跨 Region 应用验收：
//!
//! - 一个决策的多 key 在各自 Region 原子可见；
//! - decision 是提交点：apply 前 `read_ts >= commit_ts` 的读等待（safe_ts 未到）；
//! - apply 后同一 `read_ts` 同时看到两个 Region 的 key；
//! - 重放幂等；safe_ts 不会越过未应用决策。

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use coord_core::error::Result as CoordResult;
use coord_core::error_code::error_code_of;
use coord_core::storage::StorageBackend;
use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionMeta, StorageConfig};
use coord_proto::kv::v2::kv_server::Kv as KvV2;
use coord_proto::kv::v2::RangeRequest as RangeRequestV2;
use coord_server::kvt::decision_apply::apply_pending_decisions;
use coord_server::kvt::sequencer::{CommitCandidate, CommitSequencer};
use coord_server::kvt::{KvTCapability, KvTRuntimeConfig, KvTState};
use coord_server::metrics::Metrics;
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcService};
use coord_server::raft::region::RegionManager;
use coord_server::raft::region_runtime::{region_data_dir, RegionRuntimeSpec};
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::tso::{TsoBatch, TsoClient, TsoSource};
use coord_server::raft::type_config::{SequencerMutation, SequencerOutcome};
use coord_server::raft::{new_basic_node, new_raft, RaftConfig, WatchReceiver};
use coord_server::server::CoordNode;
use coord_server::storage::mvcc::{AppliedLogId, MvccStorage, RevisionMode};
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::snapshot::SnapshotTracker;
use tempfile::TempDir;
use tonic::Code;

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

/// 进程内 TSO 源：只验证 sequencer 拿号与决策落盘，不启动真实 allocator。
struct FakeTso {
    high_water: AtomicU64,
}

impl FakeTso {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            high_water: AtomicU64::new(1),
        })
    }
}

#[async_trait]
impl TsoSource for FakeTso {
    async fn is_leader(&self) -> bool {
        true
    }

    async fn allocate(&self, batch_size: u32) -> CoordResult<TsoBatch> {
        let start = self
            .high_water
            .fetch_add(u64::from(batch_size), Ordering::SeqCst);
        Ok(TsoBatch {
            start_ts: start,
            end_ts: start + u64::from(batch_size),
        })
    }
}

/// 单节点、region 0 system raft + 2 个数据 Region 的 G3 夹具。
struct G3Node {
    node: Arc<CoordNode>,
    manager: Arc<RegionManager>,
    _metrics: Arc<Metrics>,
    _dir: TempDir,
    _factory: RaftNetworkFactoryImpl,
    _rpc: RaftRpcService,
}

impl G3Node {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().to_path_buf();
        let addr = format!("127.0.0.1:{}", find_port());

        // region 0 system raft（决策日志的权威日志）
        let root_dir = base.join("region-0");
        std::fs::create_dir_all(&root_dir).unwrap();
        let backend = RedbBackend::open(&root_dir, &StorageConfig::default()).unwrap();
        let root_mvcc =
            Arc::new(MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs).unwrap());
        let root_factory = RaftNetworkFactoryImpl::new(1);
        root_factory.register_node(1, addr.clone());
        let root_tracker = Arc::new(SnapshotTracker::default());
        let root_log = LogStore::new(&root_dir).await.unwrap();
        let root_sm = StateMachineStore::new(
            Arc::clone(&root_mvcc),
            root_dir.join("snapshots"),
            root_tracker,
        );
        let root_raft = Arc::new(
            new_raft(1, Arc::new(raft_config()), root_factory, root_log, root_sm)
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

        let metrics = Arc::new(Metrics::new());
        let mut node = CoordNode::new(Arc::clone(&root_mvcc));
        node.node_id = 1;
        node.raft = Some(Arc::clone(&root_raft));
        node.region_manager = Some(Arc::clone(&manager));
        node.set_kvt_state(
            KvTState::new(
                true,
                KvTRuntimeConfig {
                    read_ts_wait_ms: 50,
                    ..KvTRuntimeConfig::default()
                },
            )
            .with_implemented([
                KvTCapability::Kv,
                KvTCapability::Tso,
                KvTCapability::Txn,
            ]),
        );
        node.set_metrics(Arc::clone(&metrics));
        let tso = TsoClient::new(FakeTso::new(), 1024, None);
        node.set_tso_client(Arc::clone(&tso));
        node.set_commit_sequencer(Arc::new(CommitSequencer::new(
            root_raft.as_ref().clone(),
            1,
            tso,
            Arc::clone(&root_mvcc),
            256,
            4 * 1024 * 1024,
            Duration::from_secs(5),
        )));
        let node = Arc::new(node);

        wait_for_leaders(&node, &manager).await;

        Self {
            node,
            manager,
            _metrics: metrics,
            _dir: dir,
            _factory: factory,
            _rpc: rpc,
        }
    }

    async fn commit(&self, writes: &[(&[u8], &[u8])]) -> u64 {
        let sequencer = self.node.commit_sequencer().expect("commit sequencer");
        let outcomes = sequencer
            .validate_and_commit(vec![CommitCandidate {
                txn_id: b"txn-1".to_vec(),
                start_ts: 0,
                request_id: b"req-1".to_vec(),
                read_set: BTreeSet::new(),
                mutations: writes
                    .iter()
                    .map(|(key, value)| SequencerMutation::Put {
                        key: key.to_vec(),
                        value: value.to_vec(),
                        lease_id: None,
                    })
                    .collect(),
            }])
            .await
            .expect("validate and commit");
        match outcomes.into_iter().next() {
            Some(SequencerOutcome::Committed { commit_ts }) => commit_ts,
            other => panic!("expected Committed, got {other:?}"),
        }
    }

    fn runtime(&self, key: &[u8]) -> Arc<coord_server::raft::region_runtime::RegionRuntime> {
        self.manager.route_runtime(key).expect("route key")
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

fn range_req(key: &[u8], range_end: &[u8], read_ts: i64) -> RangeRequestV2 {
    RangeRequestV2 {
        key: key.to_vec(),
        range_end: range_end.to_vec(),
        limit: 0,
        read_ts,
        keys_only: false,
        count_only: false,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_commit_decision_applies_two_regions_atomically() {
    let g3 = G3Node::start().await;
    let commit_ts = g3.commit(&[(b"apple", b"a"), (b"melon", b"m")]).await;
    assert!(
        g3.node
            .storage
            .get_sequencer_decision(commit_ts)
            .unwrap()
            .is_some(),
        "the decision is durable before any region apply"
    );

    // apply 前：两个 Region 在 commit_ts 都读不到；显式 read_ts 必须等待 safe_ts。
    for key in [b"apple".as_slice(), b"melon".as_slice()] {
        assert_eq!(
            g3.runtime(key)
                .mvcc
                .get_at_revision(key, commit_ts)
                .unwrap(),
            None,
            "an unapplied decision must not be visible in either region"
        );
    }
    let err = g3
        .node
        .range(tonic::Request::new(range_req(
            b"apple",
            b"",
            commit_ts as i64,
        )))
        .await
        .expect_err("read_ts above safe_ts must wait");
    assert_eq!(err.code(), Code::Unavailable, "{err:?}");
    assert_eq!(
        error_code_of(&err).as_deref(),
        Some("SAFE_TS_LAG"),
        "{err:?}"
    );

    let applied = apply_pending_decisions(&g3.node, 16)
        .await
        .expect("apply decisions");
    assert_eq!(applied, 2, "one command per data region");
    assert_eq!(g3.node.safe_ts(), commit_ts);

    // 同一 read_ts 同时看到两个 Region 的 key（不会部分可见）。
    assert_eq!(
        g3.runtime(b"apple")
            .mvcc
            .get_at_revision(b"apple", commit_ts)
            .unwrap(),
        Some(b"a".to_vec())
    );
    assert_eq!(
        g3.runtime(b"apple").mvcc.range_in(b"", b"\xff", 0).unwrap(),
        vec![(b"apple".to_vec(), b"a".to_vec())]
    );
    let resp = g3
        .node
        .range(tonic::Request::new(range_req(
            b"",
            b"\xff",
            commit_ts as i64,
        )))
        .await
        .expect("range after apply")
        .into_inner();
    assert_eq!(resp.read_ts, commit_ts as i64);
    assert_eq!(resp.kvs.len(), 2, "{:?}", resp.kvs);
    assert_eq!(resp.kvs[0].key, b"apple");
    assert_eq!(resp.kvs[1].key, b"melon");

    for (region_id, key) in [(1u64, b"apple".as_slice()), (2u64, b"melon".as_slice())] {
        let rt = g3.runtime(key);
        assert_eq!(rt.mvcc.last_applied_decision_ts().unwrap(), commit_ts);
        let events = rt.mvcc.read_changelog_entries(commit_ts).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].revision, commit_ts);
        assert_eq!(events[0].changes.len(), 1, "only this region's key");
        assert_eq!(events[0].changes[0].key, key);
        assert_eq!(
            rt.mvcc.get_kv_metadata(key).unwrap().unwrap().version,
            1,
            "region {region_id} must not duplicate the write"
        );
    }

    // 重放：游标已到 commit_ts，不重复写 changelog/version。
    assert_eq!(
        apply_pending_decisions(&g3.node, 16).await.expect("replay"),
        0
    );
    assert_eq!(
        g3.runtime(b"apple")
            .mvcc
            .get_kv_metadata(b"apple")
            .unwrap()
            .unwrap()
            .version,
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_safe_ts_stops_before_unapplied_decision() {
    let g3 = G3Node::start().await;
    let commit_ts = g3.commit(&[(b"apple", b"a")]).await;
    g3.node
        .storage
        .apply_tso_allocate(1024, 1, AppliedLogId::standalone(0))
        .expect("raise TSO high water");
    assert_eq!(g3.node.storage.tso_high_water().unwrap(), 1025);

    // 决策未 apply：fence 可以发布，但每个 Region 的水位只能到 commit_ts-1；
    // 否则该决策随后会被 CommitTs 策略拒绝。
    let advanced = coord_server::kvt::safe_ts::advance_once(&g3.node)
        .await
        .expect("publish fence");
    assert_eq!(
        advanced, 0,
        "pending decision must pin the region watermark"
    );
    assert_eq!(g3.node.safe_ts_fence(), 1025);
    assert_eq!(g3.node.safe_ts(), commit_ts - 1);
    for key in [b"apple".as_slice(), b"melon".as_slice()] {
        assert_eq!(
            g3.runtime(key).mvcc.current_revision(),
            commit_ts - 1,
            "region watermark must not pass an unapplied decision"
        );
    }

    apply_pending_decisions(&g3.node, 16)
        .await
        .expect("apply decision");
    assert_eq!(g3.node.safe_ts(), commit_ts);

    let advanced = coord_server::kvt::safe_ts::advance_once(&g3.node)
        .await
        .expect("advance after apply");
    assert_eq!(advanced, 2);
    assert_eq!(g3.node.safe_ts(), 1025);
}
