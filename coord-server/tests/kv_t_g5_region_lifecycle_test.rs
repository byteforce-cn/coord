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

use async_trait::async_trait;
use common::bind_listener;

use coord_core::error::Error;
use coord_core::storage::StorageBackend;
use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionMeta};
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcService};
use coord_server::raft::region::{spawn_configured_regions, RegionManager, RegionSeed};
use coord_server::raft::region_lifecycle::{
    RegionLifecycle, RegionLifecycleSpec, PENDING_TARGET_MARKER, REGION_RECONCILE_INTERVAL,
};
use coord_server::raft::region_runtime::{region_data_dir, CoordRangeImportTarget};
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::type_config::{
    Command, IntentPhase, PdIntent, PdOp, RegionMetaOp, RegionMetaOutcome, Response,
};
use coord_server::raft::{
    new_basic_node, new_raft, CoordRaft, RaftConfig, RaftNode, RegionRuntime,
};
use coord_server::split_merge::{RangeImportTarget, SplitDrain};
use coord_server::storage::compaction::CompactionConfig;
use coord_server::storage::mvcc::{KvMetadata, MvccStorage, RevisionMode};
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::region_table::{plan_split, RegionChange};
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

/// 测试用 compaction 配置：retention = 1 让少量写入就能观察到水位推进；间隔拉长，
/// 只由 trigger_compaction 驱动，避免后台 tick 干扰断言。
fn harness_compaction() -> CompactionConfig {
    CompactionConfig {
        changelog_retention_revisions: 1,
        interval: REGION_RECONCILE_INTERVAL * 3600,
        auto_compact: true,
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
    factory: RaftNetworkFactoryImpl,
    rpc: RaftRpcService,
    raft_config: Arc<RaftConfig>,
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
            compaction: harness_compaction(),
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
            factory,
            rpc,
            raft_config,
        };
        harness.wait_region0_leader().await;
        harness
    }

    /// 模拟进程重启：内存态生命周期（pending / members / reclaim）全空，权威表与
    /// 磁盘目录沿用同一份持久化状态。后台 tick 不启动，由测试逐拍驱动。
    fn restarted_lifecycle(&self) -> Arc<RegionLifecycle> {
        Arc::new(RegionLifecycle::new(RegionLifecycleSpec {
            node_id: NODE_ID,
            manager: Arc::clone(&self.manager),
            factory: self.factory.clone(),
            rpc: self.rpc.clone(),
            base_dir: self._dir.path().to_path_buf(),
            raft_config: Arc::clone(&self.raft_config),
            object_store: None,
            revision_mode: RevisionMode::CommitTs,
            table: Arc::clone(&self.mvcc0),
            region0_raft: self.raft0.as_ref().clone(),
            metrics: None,
            safe_ts: None,
            compaction: harness_compaction(),
        }))
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

    /// 提出一条 PD 队列 / split-merge journal 命令（region 0 raft）。
    async fn propose_pd(&self, op: PdOp) {
        self.raft0
            .client_write(Command::Pd(op))
            .await
            .expect("propose pd op");
    }

    fn split_intent(&self, op_id: u64, parent: u64, split_key: &[u8], new_id: u64) -> PdIntent {
        let table = self.mvcc0.region_table().expect("read region table");
        PdIntent {
            op_id,
            owner: NODE_ID,
            plan: plan_split(&table, parent, split_key, new_id).expect("plan split"),
            phase: IntentPhase::Prepared,
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
async fn test_lifecycle_precreates_split_target_and_promotes_after_cutover() {
    let harness = Harness::boot().await;
    assert_eq!(
        harness
            .propose(RegionMetaOp::Init {
                regions: vec![region(1, b"", b"", 1, 1)],
            })
            .await,
        RegionMetaOutcome::Applied { version: 1 }
    );
    harness
        .lifecycle
        .reconcile()
        .await
        .expect("reconcile table v1");

    // split 目标在 cutover 前就要有 raft 组：导入的数据必须经目标日志 apply，
    // 而目标组需要覆盖多数派副本。
    harness
        .propose_pd(PdOp::RecordIntent {
            intent: harness.split_intent(10, 1, b"m", 2),
        })
        .await;

    let outcome = harness
        .lifecycle
        .reconcile()
        .await
        .expect("pre-create target");
    assert_eq!(outcome.pending_spawned, 1);
    assert_eq!(harness.lifecycle.pending_count(), 1);
    assert!(harness.lifecycle.migration_target(2).is_some());
    let target_dir = region_data_dir(harness._dir.path(), 2);
    assert!(
        target_dir.join(PENDING_TARGET_MARKER).is_file(),
        "pre-created target carries the persisted reclaim marker"
    );
    assert!(
        harness.manager.runtime(2).is_none(),
        "pending target must not join data-plane runtimes"
    );
    assert!(
        harness
            .manager
            .list_runtimes()
            .iter()
            .all(|runtime| runtime.region_id() != 2),
        "pending target must stay out of safe_ts / watermark aggregation"
    );
    assert_eq!(
        harness.manager.route(b"z").unwrap().region_id(),
        1,
        "cutover 前目标区间仍归父 Region"
    );
    assert_eq!(harness.lifecycle.member_count(), 1);

    // 重复 tick 幂等：不重复装配、不重复计数。
    let outcome = harness
        .lifecycle
        .reconcile()
        .await
        .expect("reconcile again");
    assert_eq!(outcome.pending_spawned, 0);
    assert_eq!(harness.lifecycle.pending_count(), 1);

    let target = harness
        .lifecycle
        .migration_target(2)
        .expect("pending target runtime");
    let stop = tokio::time::Instant::now() + DEADLINE;
    while target.raft.current_leader().await != Some(NODE_ID) {
        assert!(
            tokio::time::Instant::now() < stop,
            "pending target leader not elected"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let port = CoordRangeImportTarget::from_runtime(&target);
    let rows = vec![(
        b"m".to_vec(),
        b"v".to_vec(),
        KvMetadata {
            version: 1,
            create_revision: 10,
            mod_revision: 10,
            lease_id: 0,
            deleted: false,
        },
    )];
    assert_eq!(
        port.import_range(b"m", b"", rows, 10, true, true)
            .await
            .expect("import through the pending target raft"),
        1
    );
    assert_eq!(target.mvcc.get(b"m").unwrap().as_deref(), Some(&b"v"[..]));

    // cutover：提升为正式 Region（路由复用同一个 handle + compaction 接管）。
    harness.propose_pd(PdOp::Cutover { op_id: 10 }).await;
    let outcome = harness.lifecycle.reconcile().await.expect("promote target");
    assert_eq!(outcome.pending_promoted, 1);
    assert_eq!(outcome.pending_spawned, 0);
    assert!(
        !target_dir.join(PENDING_TARGET_MARKER).exists(),
        "promotion drops the reclaim marker (a later merge must keep the dir)"
    );
    assert_eq!(harness.lifecycle.pending_count(), 0);
    assert_eq!(harness.lifecycle.member_count(), 2);
    assert!(harness.lifecycle.has_compaction(2));
    assert_eq!(harness.manager.route(b"z").unwrap().region_id(), 2);

    let runtime2 = harness.wait_region_leader(2).await;
    assert!(
        Arc::ptr_eq(&runtime2, &target),
        "promotion must reuse the pending runtime"
    );
    let route_handle = harness
        .manager
        .get_region(2)
        .expect("region 2 route handle");
    assert!(
        Arc::ptr_eq(&route_handle, &runtime2.handle),
        "route table and runtime must share one RegionHandle"
    );
    assert_eq!(
        runtime2.mvcc.get(b"m").unwrap().as_deref(),
        Some(&b"v"[..]),
        "imported rows survive promotion"
    );
    harness.put_v2(&runtime2, b"n", b"v2", 11).await;
    assert_eq!(
        runtime2.mvcc.get(b"n").unwrap().as_deref(),
        Some(&b"v2"[..])
    );

    // intent 清理后仍是正式 Region，不会被当成 pending 回收。
    harness.propose_pd(PdOp::ClearIntent { op_id: 10 }).await;
    let outcome = harness.lifecycle.reconcile().await.expect("clear intent");
    assert_eq!(outcome.pending_retired, 0);
    assert_eq!(harness.lifecycle.member_count(), 2);
    assert!(harness.manager.runtime(2).is_some());
    assert!(
        region_data_dir(harness._dir.path(), 2).exists(),
        "promoted region dir must never be reclaimed"
    );
}

struct NoopDrain;

#[async_trait]
impl SplitDrain for NoopDrain {
    async fn drain(&self, _start: &[u8], _end: &[u8], _timeout: Duration) -> Result<(), String> {
        Ok(())
    }
}

/// 生产接线路径（`main.rs` 调用的工厂）：源 Region 的 MVCC 从 `RegionManager`
/// 解析，目标 Region 从 `RegionLifecycle` 的 pending 运行时解析——导入经目标
/// raft，cutover 前目标仍不路由。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_production_migrator_wires_manager_source_and_pending_target() {
    let harness = Harness::boot().await;
    assert_eq!(
        harness
            .propose(RegionMetaOp::Init {
                regions: vec![region(1, b"", b"", 1, 1)],
            })
            .await,
        RegionMetaOutcome::Applied { version: 1 }
    );
    harness
        .lifecycle
        .reconcile()
        .await
        .expect("reconcile table v1");

    let parent = harness.wait_region_leader(1).await;
    harness.put_v2(&parent, b"a", b"1", 10).await;
    harness.put_v2(&parent, b"m", b"1", 11).await;
    harness.put_v2(&parent, b"z", b"3", 12).await;

    let intent = harness.split_intent(11, 1, b"m", 2);
    harness
        .propose_pd(PdOp::RecordIntent {
            intent: intent.clone(),
        })
        .await;
    let outcome = harness
        .lifecycle
        .reconcile()
        .await
        .expect("pre-create target");
    assert_eq!(outcome.pending_spawned, 1);

    // 不手工等 leader：生产接线（factory 的目标端口）必须自己等过选举窗口。
    let target = harness
        .lifecycle
        .migration_target(2)
        .expect("pending target runtime");

    let migrator = coord_server::split_merge::range_import_migrator(
        &harness.manager,
        &harness.lifecycle,
        Arc::new(NoopDrain),
    );
    migrator
        .migrate(&intent.plan)
        .await
        .expect("migration through production resolvers");

    assert_eq!(target.mvcc.get(b"m").unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(target.mvcc.get(b"z").unwrap().as_deref(), Some(&b"3"[..]));
    assert!(target.mvcc.get(b"a").unwrap().is_none(), "父区间左半不搬迁");
    assert!(harness.manager.runtime(2).is_none(), "导入期间目标仍不路由");
    assert_eq!(harness.manager.route(b"z").unwrap().region_id(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_lifecycle_retires_split_target_that_never_cut_over() {
    let harness = Harness::boot().await;
    assert_eq!(
        harness
            .propose(RegionMetaOp::Init {
                regions: vec![region(1, b"", b"", 1, 1)],
            })
            .await,
        RegionMetaOutcome::Applied { version: 1 }
    );
    harness
        .lifecycle
        .reconcile()
        .await
        .expect("reconcile table v1");

    harness
        .propose_pd(PdOp::RecordIntent {
            intent: harness.split_intent(11, 1, b"m", 3),
        })
        .await;
    let outcome = harness
        .lifecycle
        .reconcile()
        .await
        .expect("pre-create target");
    assert_eq!(outcome.pending_spawned, 1);
    assert_eq!(harness.lifecycle.pending_count(), 1);
    let target_dir = region_data_dir(harness._dir.path(), 3);
    assert!(target_dir.exists(), "pending target wrote its data dir");

    // 执行失败/被裁掉：intent 清除，表未改 → 预创建的目标必须停机回收。
    harness.propose_pd(PdOp::ClearIntent { op_id: 11 }).await;
    let outcome = harness.lifecycle.reconcile().await.expect("retire target");
    assert_eq!(outcome.pending_retired, 1);
    assert_eq!(harness.lifecycle.pending_count(), 0);
    assert!(harness.manager.runtime(3).is_none());
    assert_eq!(harness.manager.route(b"z").unwrap().region_id(), 1);
    assert_eq!(
        harness.mvcc0.region_table().unwrap().version,
        1,
        "表未改：回滚只停机"
    );
    assert!(
        !target_dir.exists(),
        "aborted split target dir is reclaimed"
    );

    let outcome = harness
        .lifecycle
        .reconcile()
        .await
        .expect("reconcile again");
    assert_eq!(outcome.pending_retired, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_lifecycle_skips_pending_target_without_local_replica() {
    let harness = Harness::boot().await;
    assert_eq!(
        harness
            .propose(RegionMetaOp::Init {
                regions: vec![region(1, b"", b"", 1, 1)],
            })
            .await,
        RegionMetaOutcome::Applied { version: 1 }
    );
    harness
        .lifecycle
        .reconcile()
        .await
        .expect("reconcile table v1");

    let mut intent = harness.split_intent(12, 1, b"m", 4);
    for change in &mut intent.plan.changes {
        if let RegionChange::Add { meta } = change {
            meta.peers = vec![Peer {
                node_id: 2,
                raft_addr: "127.0.0.1:2".to_string(),
                role: PeerRole::Voter,
            }];
        }
    }
    harness.propose_pd(PdOp::RecordIntent { intent }).await;

    let outcome = harness.lifecycle.reconcile().await.expect("reconcile");
    assert_eq!(outcome.pending_spawned, 0);
    assert_eq!(harness.lifecycle.pending_count(), 0);
    assert!(harness.lifecycle.migration_target(4).is_none());
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

// ─── 崩溃遗留的 split 目标目录（重启后按持久化标记清扫，#163）───

/// 目标 Region 预创建后进程崩溃、intent 在停机期间被清掉：重启后内存里没有任何
/// reclaim 记录，只剩目录上的持久化标记——第一拍拿到权威表就要把目录扫掉。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_lifecycle_sweeps_orphan_split_target_dir_after_restart() {
    let harness = Harness::boot().await;
    assert_eq!(
        harness
            .propose(RegionMetaOp::Init {
                regions: vec![region(1, b"", b"", 1, 1)],
            })
            .await,
        RegionMetaOutcome::Applied { version: 1 }
    );
    harness
        .lifecycle
        .reconcile()
        .await
        .expect("reconcile table v1");
    harness
        .propose_pd(PdOp::RecordIntent {
            intent: harness.split_intent(21, 1, b"m", 9),
        })
        .await;
    let outcome = harness
        .lifecycle
        .reconcile()
        .await
        .expect("pre-create target");
    assert_eq!(outcome.pending_spawned, 1);
    let target_dir = region_data_dir(harness._dir.path(), 9);
    assert!(target_dir.join(PENDING_TARGET_MARKER).is_file());

    // 崩溃：运行时停在原地，intent 在「停机期间」被清掉（执行失败/被裁掉）。
    harness
        .lifecycle
        .pending_runtime(9)
        .expect("pending target runtime")
        .raft
        .shutdown()
        .await
        .expect("shutdown pending target");
    harness.propose_pd(PdOp::ClearIntent { op_id: 21 }).await;

    let restarted = harness.restarted_lifecycle();
    assert_eq!(restarted.pending_count(), 0, "重启后内存态从零开始");
    let outcome = restarted
        .reconcile()
        .await
        .expect("reconcile after restart");
    assert_eq!(outcome.pending_dirs_swept, 1);
    assert!(
        !target_dir.exists(),
        "orphan split target dir must be reclaimed after restart"
    );
    assert_eq!(
        harness.mvcc0.region_table().unwrap().version,
        1,
        "清扫只删目录，不动权威表"
    );
    assert_eq!(harness.manager.route(b"z").unwrap().region_id(), 1);

    // 幂等：下一拍没有可扫的目录。
    let outcome = restarted.reconcile().await.expect("reconcile again");
    assert_eq!(outcome.pending_dirs_swept, 0);
}

/// 重启时 intent 仍在（预创建完还没 cutover）：标记目录是活的搬迁目标，不能扫。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_lifecycle_keeps_planned_split_target_dir_after_restart() {
    let harness = Harness::boot().await;
    assert_eq!(
        harness
            .propose(RegionMetaOp::Init {
                regions: vec![region(1, b"", b"", 1, 1)],
            })
            .await,
        RegionMetaOutcome::Applied { version: 1 }
    );
    harness
        .lifecycle
        .reconcile()
        .await
        .expect("reconcile table v1");
    harness
        .propose_pd(PdOp::RecordIntent {
            intent: harness.split_intent(22, 1, b"m", 10),
        })
        .await;
    let outcome = harness
        .lifecycle
        .reconcile()
        .await
        .expect("pre-create target");
    assert_eq!(outcome.pending_spawned, 1);
    let target_dir = region_data_dir(harness._dir.path(), 10);

    let restarted = harness.restarted_lifecycle();
    assert_eq!(
        restarted.sweep_orphan_split_dirs().await.expect("sweep"),
        0,
        "planned target is still claimed by the intent journal"
    );
    assert!(
        target_dir.join(PENDING_TARGET_MARKER).is_file(),
        "still-planned target dir survives the sweep"
    );
}

/// 清扫只认 pending 标记 + 无主张：已提升后从表里移除的 Region 目录（救援路径）
/// 与表里仍有的 Region 目录都不删。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_lifecycle_sweep_keeps_live_and_unmarked_region_dirs() {
    let harness = Harness::boot().await;
    assert_eq!(
        harness
            .propose(RegionMetaOp::Init {
                regions: vec![region(1, b"", b"", 1, 1)],
            })
            .await,
        RegionMetaOutcome::Applied { version: 1 }
    );
    harness
        .lifecycle
        .reconcile()
        .await
        .expect("reconcile table v1");

    // 曾经是正式 Region、被 merge/split 从表里移除后留下的目录：没有 pending 标记。
    let rescued = region_data_dir(harness._dir.path(), 11);
    std::fs::create_dir_all(&rescued).expect("create rescued dir");
    std::fs::write(rescued.join("store.db"), b"rescued").expect("write rescued data");

    // 带标记但表里仍有该 id（提升窗口里崩溃/重启）：不能删。
    let in_table = region_data_dir(harness._dir.path(), 1);
    std::fs::write(in_table.join(PENDING_TARGET_MARKER), b"pending\n").expect("write marker");

    assert_eq!(
        harness
            .lifecycle
            .sweep_orphan_split_dirs()
            .await
            .expect("sweep"),
        0
    );
    assert!(
        rescued.exists(),
        "rescued region dir is not a pending target"
    );
    assert!(
        in_table.join(PENDING_TARGET_MARKER).is_file(),
        "a marked dir whose id is in the table is live, not orphaned"
    );
}

/// `version == 0`（v1 静态装配）没有权威判据：不扫目录，也不改 v1 行为。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_lifecycle_does_not_sweep_without_an_authoritative_region_table() {
    let harness = Harness::boot().await;
    let orphan = region_data_dir(harness._dir.path(), 12);
    std::fs::create_dir_all(&orphan).expect("create marked dir");
    std::fs::write(orphan.join(PENDING_TARGET_MARKER), b"pending\n").expect("write marker");

    let outcome = harness.lifecycle.reconcile().await.expect("reconcile");
    assert_eq!(outcome.pending_dirs_swept, 0);
    assert!(
        orphan.exists(),
        "no authoritative table = no reclaim decision"
    );
    assert_eq!(
        harness
            .lifecycle
            .sweep_orphan_split_dirs()
            .await
            .expect("sweep"),
        0
    );
    assert!(orphan.exists());
}
