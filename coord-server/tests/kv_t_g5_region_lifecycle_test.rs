//! KV-T G5（#160）运行时 Region 生命周期验收：
//!
//! - 权威 Region 表新增区间 → `RegionLifecycle::reconcile` 装配运行时（raft 组 +
//!   存储 + CompactionManager），数据面按新边界路由并可读写；
//! - 新建 Region 一定有 CompactionManager，且提案走的是该 Region 自己的 raft
//!   （B-RG-1：新建 Region 永不压缩；compaction 水位真实推进）；
//! - 表删除区间 → 运行时停机并摘除，路由回到相邻 Region，已停机组的写入被拒；
//! - `apply_region_table` 不直接丢运行时（同步入口无法 await 停机），停机只由
//!   lifecycle 完成——顺序契约：先收敛路由，再装配/停机；
//! - 表未初始化（`version == 0`）与非副本 Region 不触发任何装配（v1 静态装配
//!   行为不变；非副本的 key 得到可重试的 `RouteNotReady`，不是「region 不存在」）。

mod common;

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use common::bind_listener;

use coord_core::error::Error;
use coord_core::storage::StorageBackend;
use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionMeta};
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
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::region_table::RegionChange;
use coord_server::storage::snapshot::SnapshotTracker;
use tempfile::TempDir;

const NODE_ID: u64 = 1;
const DEADLINE: Duration = Duration::from_secs(30);

fn raft_config() -> RaftConfig {
    RaftConfig {
        heartbeat_interval: 100,
        election_timeout_min: 400,
        election_timeout_max: 800,
        ..Default::default()
    }
}

fn region(id: u64, start: &[u8], end: &[u8], conf_ver: u64, version: u64) -> RegionMeta {
    RegionMeta {
        region_id: id,
        start_key: start.to_vec(),
        end_key: end.to_vec(),
        epoch: RegionEpoch { conf_ver, version },
        peers: vec![Peer {
            node_id: NODE_ID,
            raft_addr: "127.0.0.1:1".to_string(),
            role: PeerRole::Voter,
        }],
        approximate_size: 0,
        approximate_keys: 0,
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
    /// 单节点：region 0 = system raft（权威表），region 1 = 全 keyspace 静态 Region。
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

        // region 1：启动期静态装配（与 main.rs 同路径）
        let seeds = vec![RegionSeed {
            region_id: 1,
            start_key: vec![],
            end_key: vec![],
        }];
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
        .expect("assemble static region 1");

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
            // retention = 1 让测试用少量写入就能观察到水位推进；间隔拉长，
            // 只由 trigger_compaction 驱动，避免后台 tick 干扰断言。
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
                .expect("attach static region");
        }

        let harness = Self {
            _dir: dir,
            raft0,
            mvcc0,
            manager,
            lifecycle,
        };
        harness.wait_region0_leader().await;
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

    async fn propose(&self, op: RegionMetaOp) -> RegionMetaOutcome {
        let response = self
            .raft0
            .client_write(Command::RegionMeta(op))
            .await
            .expect("propose region meta op");
        match response.response() {
            Response::RegionMeta { outcome } => outcome.clone(),
            other => panic!("unexpected response: {other}"),
        }
    }

    /// 等 Region 组的单节点 leader 就绪（动态装配后首次写入前）。
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_lifecycle_spawns_compacts_and_retires_regions() {
    let harness = Harness::boot().await;

    // 启动期静态 Region 已由 lifecycle 接管（B-RG-1）
    assert_eq!(harness.lifecycle.member_count(), 1);
    assert!(harness.lifecycle.has_compaction(1));

    // 表未初始化：reconcile 是 no-op（v1 静态装配不变）
    let outcome = harness.lifecycle.reconcile().await.expect("reconcile");
    assert_eq!(outcome.spawned, 0);
    assert_eq!(outcome.retired, 0);

    assert_eq!(
        harness
            .propose(RegionMetaOp::Init {
                regions: vec![region(1, b"", b"", 1, 1)],
            })
            .await,
        RegionMetaOutcome::Applied { version: 1 }
    );
    let outcome = harness.lifecycle.reconcile().await.expect("reconcile");
    assert!(outcome.applied, "table v1 applied");
    assert_eq!((outcome.spawned, outcome.retired), (0, 0));

    // split：region 1 缩到 [.., m) + 新增 region 2 [m, ..)
    assert_eq!(
        harness
            .propose(RegionMetaOp::Apply {
                changes: vec![
                    RegionChange::Update {
                        meta: region(1, b"", b"m", 1, 2),
                    },
                    RegionChange::Add {
                        meta: region(2, b"m", b"", 1, 1),
                    },
                ],
            })
            .await,
        RegionMetaOutcome::Applied { version: 2 }
    );
    let outcome = harness.lifecycle.reconcile().await.expect("reconcile");
    assert_eq!((outcome.spawned, outcome.retired), (1, 0));
    assert_eq!(harness.lifecycle.member_count(), 2);

    // 路由按新边界生效，新区间可读写（raft 组 + 独立存储真的起来了）
    assert_eq!(harness.manager.route(b"a").unwrap().region_id(), 1);
    assert_eq!(harness.manager.route(b"z").unwrap().region_id(), 2);
    let runtime2 = harness.wait_region_leader(2).await;
    harness.put_v2(&runtime2, b"z", b"v1", 1).await;
    assert_eq!(
        runtime2.mvcc.get(b"z").expect("read region 2 key"),
        Some(b"v1".to_vec())
    );

    // B-RG-1：新 Region 的 compaction 提案走它自己的 raft，水位真实推进
    harness.put_v2(&runtime2, b"zz", b"v2", 2).await;
    assert!(harness.lifecycle.has_compaction(2));
    harness.lifecycle.trigger_compaction(2);
    let stop = tokio::time::Instant::now() + DEADLINE;
    loop {
        let watermark = runtime2.handle.compaction_watermark.load(Ordering::Relaxed);
        if watermark > 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < stop,
            "region 2 compaction watermark never advanced"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // 幂等：同一个表重复 reconcile 不再装配
    let outcome = harness.lifecycle.reconcile().await.expect("reconcile");
    assert_eq!((outcome.spawned, outcome.retired), (0, 0));

    // merge：region 1 扩到无上界 + 删除 region 2。
    // 先只收敛路由（同步入口）——运行时不得因此被悄悄丢弃（丢 Arc = 泄漏 raft 任务）。
    assert_eq!(
        harness
            .propose(RegionMetaOp::Apply {
                changes: vec![
                    RegionChange::Update {
                        meta: region(1, b"", b"", 1, 3),
                    },
                    RegionChange::Remove {
                        region_id: 2,
                        expected_epoch: RegionEpoch {
                            conf_ver: 1,
                            version: 1,
                        },
                    },
                ],
            })
            .await,
        RegionMetaOutcome::Applied { version: 3 }
    );
    let table = harness.mvcc0.region_table().expect("read region table");
    assert!(
        harness
            .manager
            .apply_region_table(&table)
            .expect("apply region table"),
        "table v3 applied"
    );
    assert_eq!(harness.manager.route(b"z").unwrap().region_id(), 1);
    assert!(
        harness.manager.runtime(2).is_some(),
        "apply_region_table must not drop the runtime handle (shutdown is async)"
    );

    // reconcile 完成停机与摘除；数据目录保留
    let outcome = harness.lifecycle.reconcile().await.expect("reconcile");
    assert_eq!((outcome.spawned, outcome.retired), (0, 1));
    assert!(harness.manager.runtime(2).is_none());
    assert!(!harness.lifecycle.has_compaction(2));
    assert_eq!(harness.lifecycle.member_count(), 1);
    assert!(runtime2.data_dir.exists(), "retire keeps the data dir");

    // 已停机的 raft 组不得再接受写入
    assert!(
        runtime2
            .raft
            .client_write(Command::PutV2 {
                key: b"z".to_vec(),
                value: b"after-retire".to_vec(),
                lease_id: None,
                commit_ts: 3,
            })
            .await
            .is_err(),
        "retired region raft must reject writes"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_lifecycle_skips_regions_this_node_does_not_replicate() {
    let harness = Harness::boot().await;

    // region 2 的副本只在 node 2：本节点不装配它，但路由表里有这个区间。
    let mut foreign = region(2, b"m", b"", 1, 1);
    foreign.peers = vec![Peer {
        node_id: 2,
        raft_addr: "127.0.0.1:2".to_string(),
        role: PeerRole::Voter,
    }];
    assert_eq!(
        harness
            .propose(RegionMetaOp::Init {
                regions: vec![region(1, b"", b"m", 1, 1), foreign],
            })
            .await,
        RegionMetaOutcome::Applied { version: 1 }
    );

    let outcome = harness.lifecycle.reconcile().await.expect("reconcile");
    assert!(outcome.applied);
    assert_eq!((outcome.spawned, outcome.retired), (0, 0));
    assert_eq!(harness.lifecycle.member_count(), 1);
    assert!(harness.manager.runtime(2).is_none());

    // 表已含区间但运行时不在本节点：可重试错误，不能报成 region 不存在。
    match harness.manager.route_runtime(b"z") {
        Err(Error::RouteNotReady) => {}
        Err(other) => panic!("expected RouteNotReady, got {other}"),
        Ok(runtime) => panic!("expected RouteNotReady, routed to {}", runtime.region_id()),
    }
}
