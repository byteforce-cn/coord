//! KV-T G5（#165）固定 `/obj/` Region 验收：
//!
//! - 对象存储启用时，`[/obj/, /obj0)` 从 `initial_regions` 切成独立固定 Region
//!   （`storage::region_table::carve_object_region`，种子期纯计算）；
//! - 对象 manifest key 路由进该 Region，读写闭环；普通 KV key 不落进去；
//! - 边界归属：`/obj/` 属对象 Region，`/obj0` 属右段（半开区间）；
//! - 固定区间不参与 split/merge：`plan_split`/`plan_merge` 拒绝，PD 调度器
//!   不产生 operator（ADR-0015 D6/D11.9）。

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use common::bind_listener;

use coord_core::storage::StorageBackend;
use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionMeta};
use coord_server::pd::scheduler::{ScheduleContext, Scheduler, SplitChecker};
use coord_server::pd::PdConfig;
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcService};
use coord_server::raft::region::{spawn_configured_regions, RegionManager, RegionSeed};
use coord_server::raft::region_lifecycle::{
    RegionLifecycle, RegionLifecycleSpec, REGION_RECONCILE_INTERVAL,
};
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::type_config::{Command, RegionMetaOp, RegionMetaOutcome, Response};
use coord_server::raft::{
    new_basic_node, new_raft, CoordRaft, RaftConfig, RaftNode, RegionRuntime,
};
use coord_server::storage::compaction::CompactionConfig;
use coord_server::storage::mvcc::{MvccStorage, RevisionMode};
use coord_server::storage::object_store::manifest_key;
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::region_table::{carve_object_region, is_object_region, RegionTable};
use coord_server::storage::snapshot::SnapshotTracker;
use tempfile::TempDir;

const NODE_ID: u64 = 1;
const DEADLINE: Duration = Duration::from_secs(30);
/// 种子只有 Region 1（全 keyspace）；切分后左段沿用 1，对象 Region = 2，右段 = 3。
const LEFT: u64 = 1;
const OBJECT: u64 = 2;
const RIGHT: u64 = 3;

fn raft_config() -> RaftConfig {
    RaftConfig {
        heartbeat_interval: 100,
        election_timeout_min: 400,
        election_timeout_max: 800,
        ..Default::default()
    }
}

struct Harness {
    _dir: TempDir,
    raft0: Arc<CoordRaft>,
    mvcc0: Arc<MvccStorage<RedbBackend>>,
    manager: Arc<RegionManager>,
    lifecycle: Arc<RegionLifecycle>,
}

impl Harness {
    /// 单节点：region 0 = system raft（权威表）；启动种子 = 从 `[b"", b"")` 切出的
    /// 三段（与 `main.rs` 的装配路径一致）。
    async fn boot() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();

        let (_listener, addr) = bind_listener().await;
        let peers = vec![Peer {
            node_id: NODE_ID,
            raft_addr: addr.clone(),
            role: PeerRole::Voter,
        }];
        let factory = RaftNetworkFactoryImpl::new(NODE_ID);
        factory.register_node(NODE_ID, addr.clone());
        let rpc = RaftRpcService::new();
        let raft_config = Arc::new(raft_config());

        // region 0：权威 Region 表所在（system raft）
        let backend = RedbBackend::open(&root, &Default::default()).expect("open region 0");
        let mvcc0 =
            Arc::new(MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs).unwrap());
        let log = LogStore::new(&root).await.expect("open region 0 log");
        let sm = StateMachineStore::new(
            Arc::clone(&mvcc0),
            root.join("snapshots"),
            Arc::new(SnapshotTracker::default()),
        );
        let raft0 = Arc::new(
            new_raft(NODE_ID, Arc::clone(&raft_config), factory.clone(), log, sm)
                .await
                .expect("create region 0 raft"),
        );
        rpc.set_raft(raft0.as_ref().clone());
        let members: BTreeMap<u64, RaftNode> = BTreeMap::from([(NODE_ID, new_basic_node(&addr))]);
        raft0
            .initialize(members)
            .await
            .expect("initialize region 0");

        // 启动种子：单个全 keyspace Region → 切出固定对象区间
        let mut regions = vec![RegionMeta {
            region_id: LEFT,
            start_key: vec![],
            end_key: vec![],
            epoch: RegionEpoch::initial(),
            peers: peers.clone(),
            approximate_size: 0,
            approximate_keys: 0,
        }];
        assert_eq!(
            carve_object_region(&mut regions, LEFT + 1).expect("carve object region"),
            OBJECT
        );
        let seeds: Vec<RegionSeed> = regions
            .iter()
            .map(|m| RegionSeed {
                region_id: m.region_id,
                start_key: m.start_key.clone(),
                end_key: m.end_key.clone(),
            })
            .collect();
        let manager = spawn_configured_regions(
            NODE_ID,
            &root,
            &factory,
            &rpc,
            Arc::clone(&raft_config),
            None,
            RevisionMode::CommitTs,
            &seeds,
            &peers,
            true,
        )
        .await
        .expect("assemble carved regions");

        // 生命周期接管启动期装配的 Region（main.rs 的启动步骤）
        let lifecycle = Arc::new(RegionLifecycle::new(RegionLifecycleSpec {
            node_id: NODE_ID,
            manager: Arc::clone(&manager),
            factory: factory.clone(),
            rpc: rpc.clone(),
            base_dir: root.clone(),
            raft_config: Arc::clone(&raft_config),
            object_store: None,
            revision_mode: RevisionMode::CommitTs,
            table: Arc::clone(&mvcc0),
            region0_raft: raft0.as_ref().clone(),
            metrics: None,
            safe_ts: None,
            compaction: CompactionConfig {
                changelog_retention_revisions: 1,
                interval: REGION_RECONCILE_INTERVAL * 3600,
                auto_compact: true,
            },
        }));
        for handle in manager.list_regions() {
            lifecycle
                .ensure_region(&handle.meta.read().clone())
                .await
                .expect("attach carved region");
        }

        let harness = Self {
            _dir: dir,
            raft0,
            mvcc0,
            manager,
            lifecycle,
        };
        harness.wait_region0_leader().await;
        harness.seed_table(&regions).await;
        harness
    }

    async fn wait_region0_leader(&self) {
        let stop = tokio::time::Instant::now() + DEADLINE;
        loop {
            if self.raft0.current_leader().await == Some(NODE_ID) {
                return;
            }
            assert!(
                tokio::time::Instant::now() < stop,
                "region 0 leader not elected"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn seed_table(&self, regions: &[RegionMeta]) {
        let response = self
            .raft0
            .client_write(Command::RegionMeta(RegionMetaOp::Init {
                regions: regions.to_vec(),
            }))
            .await
            .expect("seed region table");
        match response.response() {
            Response::RegionMeta {
                outcome: RegionMetaOutcome::Applied { version },
            } => assert_eq!(*version, 1, "carved table seeds as version 1"),
            other => panic!("unexpected seed response: {other}"),
        }
    }

    async fn table(&self) -> RegionTable {
        let outcome = self.lifecycle.reconcile().await.expect("reconcile");
        assert!(outcome.applied, "carved table applied");
        self.mvcc0.region_table().expect("read authoritative table")
    }

    async fn wait_region_leader(&self, region_id: u64) -> Arc<RegionRuntime> {
        let stop = tokio::time::Instant::now() + DEADLINE;
        loop {
            if let Some(runtime) = self.manager.runtime(region_id) {
                if runtime.raft.current_leader().await == Some(NODE_ID) {
                    return runtime;
                }
            }
            assert!(
                tokio::time::Instant::now() < stop,
                "region {region_id} leader not elected"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn put_v2(&self, runtime: &RegionRuntime, key: &[u8], value: &[u8], commit_ts: u64) {
        let stop = tokio::time::Instant::now() + DEADLINE;
        loop {
            let result = runtime
                .raft
                .client_write(Command::PutV2 {
                    key: key.to_vec(),
                    value: value.to_vec(),
                    lease_id: None,
                    commit_ts,
                })
                .await;
            match result {
                Ok(_) => return,
                Err(err) => {
                    assert!(
                        tokio::time::Instant::now() < stop,
                        "region {} put_v2(commit_ts={commit_ts}) failed: {err}",
                        runtime.region_id()
                    );
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }
    }

    fn route_region(&self, key: &[u8]) -> u64 {
        self.manager
            .route(key)
            .unwrap_or_else(|err| panic!("no region routes {key:?}: {err}"))
            .region_id()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_obj_region_not_split() {
    let harness = Harness::boot().await;
    let table = harness.table().await;

    // 固定区间是表里的独立 Region（切分产生了三段，区间平铺仍成立）
    assert_eq!(table.regions.len(), 3, "carve yields left / object / right");
    assert!(
        is_object_region(table.get(OBJECT).expect("object region in table")),
        "region {OBJECT} owns [/obj/, /obj0)"
    );
    assert_eq!(
        table.get(LEFT).unwrap().end_key,
        b"/obj/",
        "left neighbour ends at the reserved prefix"
    );
    assert_eq!(
        table.get(RIGHT).unwrap().start_key,
        b"/obj0",
        "right neighbour starts after the reserved prefix"
    );

    // 边界归属：半开区间 —— /obj/ 属对象 Region，/obj0 属右段
    assert_eq!(harness.route_region(b"/obj/"), OBJECT);
    assert_eq!(harness.route_region(b"/obj0"), RIGHT);

    // 对象 manifest 路由进固定 Region，读写闭环；普通 KV 不落进去
    let manifest = manifest_key(b"bucket", b"object");
    assert_eq!(harness.route_region(&manifest), OBJECT);
    assert_ne!(harness.route_region(b"/kv/user-key"), OBJECT);
    let runtime = harness.wait_region_leader(OBJECT).await;
    harness
        .put_v2(&runtime, &manifest, b"manifest-body", 10)
        .await;
    assert_eq!(
        runtime.mvcc.get(&manifest).expect("read manifest"),
        Some(b"manifest-body".to_vec())
    );

    // 固定区间不参与 split/merge
    assert!(
        coord_server::storage::region_table::plan_split(&table, OBJECT, b"/obj/m", 9).is_err(),
        "object region never splits"
    );
    assert!(
        coord_server::storage::region_table::plan_merge(&table, LEFT, OBJECT).is_err(),
        "object region is never absorbed by its left neighbour"
    );
    assert!(
        coord_server::storage::region_table::plan_merge(&table, OBJECT, RIGHT).is_err(),
        "object region never absorbs its right neighbour"
    );

    // PD 调度器不产生 operator（executor 的拒绝只是边界兜底）
    let checker = SplitChecker::new(&PdConfig::default());
    let mut object = table.get(OBJECT).unwrap().clone();
    object.approximate_size = u64::MAX / 2;
    object.approximate_keys = u64::MAX;
    assert!(checker.check(&object, b"/obj/m".to_vec()).is_none());
    let ctx = ScheduleContext::new(vec![object], vec![]);
    assert!(checker.schedule(&ctx).is_empty());
}
