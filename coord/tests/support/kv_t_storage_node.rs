//! KV-T G5（#166）e2e 共享夹具：单节点 v2 集群 + 固定 `/obj/` Region + 对象 chunk 存储。
//!
//! 装配路径与 `main.rs` 的 `[kv_t].enabled && [object_storage].enabled` 一致：
//! region 0 是权威表所在（system raft），数据 Region 由 `[/obj/, /obj0)` 从全
//! keyspace 切出（左段 / 对象 Region / 右段）。TSO 用进程内 `FakeTso`（只验证
//! 分配序与落盘），不启动真实 allocator。
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::oneshot;
use tonic::transport::Server;

use coord_core::error::Result as CoordResult;
use coord_core::storage::StorageBackend;
use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionMeta, StorageConfig};
use coord_proto::storage::v2::storage_server::StorageServer as StorageV2Server;
use coord_server::kvt::{KvTCapability, KvTRuntimeConfig, KvTState};
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcService};
use coord_server::raft::region::{spawn_configured_regions, RegionManager, RegionSeed};
use coord_server::raft::region_lifecycle::{
    RegionLifecycle, RegionLifecycleSpec, REGION_RECONCILE_INTERVAL,
};
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::tso::{TsoBatch, TsoClient, TsoSource};
use coord_server::raft::type_config::{Command, RegionMetaOp, RegionMetaOutcome, Response};
use coord_server::raft::{new_basic_node, new_raft, CoordRaft, RaftConfig, RegionRuntime};
use coord_server::server::CoordNode;
use coord_server::storage::compaction::CompactionConfig;
use coord_server::storage::mvcc::{MvccStorage, RevisionMode};
use coord_server::storage::object_store::{ObjectLimits, ObjectStoreCtx};
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::region_table::carve_object_region;
use coord_server::storage::snapshot::SnapshotTracker;
use tempfile::TempDir;

pub const NODE_ID: u64 = 1;
/// 切分后：左段沿用 1，对象 Region = 2，右段 = 3。
pub const LEFT: u64 = 1;
pub const OBJECT: u64 = 2;
pub const RIGHT: u64 = 3;
const DEADLINE: Duration = Duration::from_secs(30);

pub fn raft_config() -> RaftConfig {
    RaftConfig {
        heartbeat_interval: 100,
        election_timeout_min: 400,
        election_timeout_max: 800,
        ..Default::default()
    }
}

/// 进程内 TSO 源：从 1000 起按批发放，足够区分 e2e 里的多次写。
pub struct FakeTso {
    high_water: AtomicU64,
}

impl FakeTso {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            high_water: AtomicU64::new(1_000),
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

/// 可重启的 gRPC 句柄（storage.v2 是被测面）。
pub struct StorageServerHandle {
    pub addr: String,
    shutdown: Option<oneshot::Sender<()>>,
    handle: tokio::task::JoinHandle<()>,
}

impl StorageServerHandle {
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        let _ = self.handle.await;
    }
}

pub struct StorageNode {
    pub node: Arc<CoordNode>,
    pub manager: Arc<RegionManager>,
    pub limits: Arc<ObjectLimits>,
    _lifecycle: Arc<RegionLifecycle>,
    _dir: TempDir,
    _factory: RaftNetworkFactoryImpl,
    _rpc: RaftRpcService,
}

impl StorageNode {
    pub async fn start() -> Self {
        Self::start_with(ObjectLimits {
            chunk_size: 1024,
            ..ObjectLimits::default()
        })
        .await
    }

    pub async fn start_with(limits: ObjectLimits) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let addr = format!("127.0.0.1:{}", find_port());

        // region 0：权威 Region 表（system raft）
        let region0_dir = root.join("region-0");
        std::fs::create_dir_all(&region0_dir).unwrap();
        let backend = RedbBackend::open(&region0_dir, &StorageConfig::default()).unwrap();
        let mvcc0 =
            Arc::new(MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs).unwrap());
        let factory0 = RaftNetworkFactoryImpl::new(NODE_ID);
        factory0.register_node(NODE_ID, addr.clone());
        let log0 = LogStore::new(&region0_dir).await.expect("region 0 log");
        let sm0 = StateMachineStore::new(
            Arc::clone(&mvcc0),
            region0_dir.join("snapshots"),
            Arc::new(SnapshotTracker::default()),
        );
        let raft_config = Arc::new(raft_config());
        let raft0 = Arc::new(
            new_raft(NODE_ID, Arc::clone(&raft_config), factory0, log0, sm0)
                .await
                .expect("create region 0 raft"),
        );
        raft0
            .initialize(BTreeMap::from([(NODE_ID, new_basic_node(&addr))]))
            .await
            .expect("initialize region 0");
        wait_leader(raft0.as_ref(), "region 0").await;

        let factory = RaftNetworkFactoryImpl::new(NODE_ID);
        factory.register_node(NODE_ID, addr.clone());
        let rpc = RaftRpcService::new();

        // 启动种子：全 keyspace → 切出固定 `/obj/` 区间
        let peers = vec![Peer {
            node_id: NODE_ID,
            raft_addr: addr.clone(),
            role: PeerRole::Voter,
        }];
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

        let limits = Arc::new(limits);
        let object_store = Some(Arc::new(ObjectStoreCtx {
            limits: Arc::clone(&limits),
            encryption_root_key_hex: None,
        }));
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
            object_store.clone(),
            RevisionMode::CommitTs,
            &seeds,
            &peers,
            true,
        )
        .await
        .expect("assemble carved regions");

        // 生命周期接管启动期装配的 Region，并用 region 0 的权威表收敛路由
        let lifecycle = Arc::new(RegionLifecycle::new(RegionLifecycleSpec {
            node_id: NODE_ID,
            manager: Arc::clone(&manager),
            factory: factory.clone(),
            rpc: rpc.clone(),
            base_dir: root.clone(),
            raft_config: Arc::clone(&raft_config),
            object_store,
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
        let response = raft0
            .client_write(Command::RegionMeta(RegionMetaOp::Init {
                regions: regions.clone(),
            }))
            .await
            .expect("seed region table");
        match response.response() {
            Response::RegionMeta {
                outcome: RegionMetaOutcome::Applied { version },
            } => assert_eq!(*version, 1),
            other => panic!("unexpected seed response: {other:?}"),
        }
        assert!(lifecycle.reconcile().await.expect("reconcile").applied);

        let mut node = CoordNode::new(Arc::clone(&mvcc0));
        node.node_id = NODE_ID;
        node.raft = Some(Arc::clone(&raft0));
        node.region_manager = Some(Arc::clone(&manager));
        node.object_limits = Some(Arc::clone(&limits));
        node.set_kvt_state(
            KvTState::new(
                true,
                KvTRuntimeConfig {
                    read_ts_wait_ms: 50,
                    ..KvTRuntimeConfig::default()
                },
            )
            .with_implemented([
                KvTCapability::Tso,
                KvTCapability::Kv,
                KvTCapability::Storage,
            ]),
        );
        node.set_tso_client(TsoClient::new(FakeTso::new(), 1024, None));

        let node = Arc::new(node);
        for region_id in [LEFT, OBJECT, RIGHT] {
            let runtime = manager.runtime(region_id).expect("region runtime");
            wait_leader(&runtime.raft, "data region").await;
        }

        Self {
            node,
            manager,
            limits,
            _lifecycle: lifecycle,
            _dir: dir,
            _factory: factory,
            _rpc: rpc,
        }
    }

    /// 只挂载 `coord.storage.v2`（被测面）。
    pub async fn serve(&self) -> StorageServerHandle {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let (tx, rx) = oneshot::channel();
        let node = Arc::clone(&self.node);
        let decode_limit = self.limits.chunk_size + 64 * 1024;
        let handle = tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(
                    StorageV2Server::from_arc(node).max_decoding_message_size(decode_limit),
                )
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = rx.await;
                })
                .await;
        });
        StorageServerHandle {
            addr,
            shutdown: Some(tx),
            handle,
        }
    }

    pub fn object_runtime(&self) -> Arc<RegionRuntime> {
        self.manager.runtime(OBJECT).expect("object region runtime")
    }

    /// 对象 Region 里的 manifest 行（None = 不存在/已删除）。
    pub fn manifest_row(&self, bucket: &[u8], object_id: &[u8]) -> Option<Vec<u8>> {
        let key = coord_server::storage::object_store::manifest_key(bucket, object_id);
        self.object_runtime().mvcc.get(&key).expect("read manifest")
    }

    /// 左/右段（非对象 Region）里的同名行：必须始终为空。
    pub fn non_object_regions_have(&self, bucket: &[u8], object_id: &[u8]) -> bool {
        let key = coord_server::storage::object_store::manifest_key(bucket, object_id);
        [LEFT, RIGHT].iter().any(|region_id| {
            self.manager
                .runtime(*region_id)
                .expect("runtime")
                .mvcc
                .get(&key)
                .expect("read")
                .is_some()
        })
    }
}

async fn wait_leader(raft: &CoordRaft, what: &str) {
    let stop = tokio::time::Instant::now() + DEADLINE;
    loop {
        if raft.current_leader().await == Some(NODE_ID) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < stop,
            "{what} leader not elected"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

pub fn find_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
