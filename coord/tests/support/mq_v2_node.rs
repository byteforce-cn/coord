//! MQ v2 单节点多 Region 夹具（M2 集成验收与基准共用）。
//!
//! 装配路径与 `main.rs` 一致：region 0 = system raft（topic 表 + 权威 Region 表），
//! 从单个全 keyspace 种子切出 `/_mq/log/<id>/` 与 `/_mq/group/<id>/` 固定 Region，
//! 再由 `RegionLifecycle` 接管启动期装配。夹具不自动注册任何业务回调：MQ 的写入
//! 语义全部来自 `coord-server` 的实现，测试只发 RPC / 读状态。
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use coord_core::storage::StorageBackend;
use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionMeta, StorageConfig};
use coord_proto::mq::v2 as mq_v2;
use coord_proto::mq::v2::mq_client::MqClient;
use coord_proto::mq::v2::mq_server::MqServer;
use coord_server::mq_v2::{MqV2Capability, MqV2RuntimeConfig, MqV2State};
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcService};
use coord_server::raft::region::{spawn_configured_regions, RegionManager, RegionSeed};
use coord_server::raft::region_lifecycle::{
    RegionLifecycle, RegionLifecycleSpec, REGION_RECONCILE_INTERVAL,
};
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::type_config::{
    Command, RegionMetaOp, RegionMetaOutcome, Response as RaftResponse,
};
use coord_server::raft::{new_basic_node, new_raft, CoordRaft, RaftConfig, RaftNode};
use coord_server::server::CoordNode;
use coord_server::storage::compaction::CompactionConfig;
use coord_server::storage::mq_log::MqTruncate;
use coord_server::storage::mq_topic::{MqTopicMeta, MqTopicOp, MqTopicOutcome};
use coord_server::storage::mvcc::{MvccStorage, RevisionMode};
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::region_table::{carve_mq_regions, mq_log_shard_prefix, RegionTable};
use coord_server::storage::snapshot::SnapshotTracker;
use tempfile::TempDir;
use tonic::transport::{Channel, Server};

/// 单节点集群的节点 ID。
pub const NODE_ID: u64 = 1;
/// 装配/选主的等待上限。
pub const DEADLINE: Duration = Duration::from_secs(30);

fn raft_config() -> RaftConfig {
    RaftConfig {
        heartbeat_interval: 100,
        election_timeout_min: 400,
        election_timeout_max: 800,
        ..Default::default()
    }
}

pub struct MqV2Node {
    pub addr: std::net::SocketAddr,
    pub raft0: Arc<CoordRaft>,
    pub mvcc0: Arc<MvccStorage<RedbBackend>>,
    pub manager: Arc<RegionManager>,
    pub node: Arc<CoordNode>,
    /// 切出的日志/消费组分片 Region id（按分片 id 升序）。
    pub log_regions: Vec<u64>,
    pub group_regions: Vec<u64>,
    lifecycle: Arc<RegionLifecycle>,
    serving: Option<tokio::task::JoinHandle<()>>,
    _dir: TempDir,
}

impl Drop for MqV2Node {
    fn drop(&mut self) {
        if let Some(handle) = self.serving.take() {
            handle.abort();
        }
    }
}

impl MqV2Node {
    /// 装配单节点多 Region 并挂载 `coord.mq.v2` 服务（`Topic` + `Publish` 已交付）。
    pub async fn boot(log_shards: u32, group_shards: u32) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().to_path_buf();

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("probe raft port");
        let raft_addr = listener.local_addr().expect("raft addr").to_string();
        drop(listener);

        let peers = vec![Peer {
            node_id: NODE_ID,
            raft_addr: raft_addr.clone(),
            role: PeerRole::Voter,
        }];
        let factory = RaftNetworkFactoryImpl::new(NODE_ID);
        factory.register_node(NODE_ID, raft_addr.clone());
        let rpc = RaftRpcService::new();
        let raft_config = Arc::new(raft_config());

        // region 0：权威 Region 表与 topic 表所在的 system raft。
        let backend =
            RedbBackend::open(&root, &StorageConfig::default()).expect("open region 0 storage");
        let mvcc0 = Arc::new(
            MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs)
                .expect("region 0 mvcc"),
        );
        let log = LogStore::new(&root).await.expect("open region 0 raft log");
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
        let members: BTreeMap<u64, RaftNode> =
            BTreeMap::from([(NODE_ID, new_basic_node(&raft_addr))]);
        raft0
            .initialize(members)
            .await
            .expect("initialize region 0");

        let mut regions = vec![RegionMeta {
            region_id: 1,
            start_key: Vec::new(),
            end_key: Vec::new(),
            epoch: RegionEpoch::initial(),
            peers: peers.clone(),
            approximate_size: 0,
            approximate_keys: 0,
        }];
        let (log_regions, group_regions) =
            carve_mq_regions(&mut regions, 2, log_shards, group_shards)
                .expect("carve /_mq/ shard regions");
        let seeds: Vec<RegionSeed> = regions
            .iter()
            .map(|meta| RegionSeed {
                region_id: meta.region_id,
                start_key: meta.start_key.clone(),
                end_key: meta.end_key.clone(),
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
        wait_region0_leader(&raft0).await;
        let response = raft0
            .client_write(Command::RegionMeta(RegionMetaOp::Init {
                regions: regions.clone(),
            }))
            .await
            .expect("seed authoritative region table");
        match response.response() {
            RaftResponse::RegionMeta {
                outcome: RegionMetaOutcome::Applied { version },
            } => assert_eq!(*version, 1, "carved table seeds as version 1"),
            other => panic!("unexpected region table seed response: {other:?}"),
        }
        let outcome = lifecycle.reconcile().await.expect("reconcile region table");
        assert!(outcome.applied, "carved table applied");
        let table = mvcc0
            .region_table()
            .expect("read authoritative region table");
        assert_table_tiles(&table, log_shards + group_shards);

        let mut node = CoordNode::new(Arc::clone(&mvcc0));
        node.node_id = NODE_ID;
        node.raft = Some(Arc::clone(&raft0));
        node.region_manager = Some(Arc::clone(&manager));
        node.set_metrics(Arc::new(coord_server::metrics::Metrics::new()));
        node.set_mq_v2_state(
            MqV2State::new(
                true,
                MqV2RuntimeConfig {
                    log_shards,
                    group_shards,
                    ..MqV2RuntimeConfig::default()
                },
            )
            .with_implemented([MqV2Capability::Topic, MqV2Capability::Publish]),
        );
        let node = Arc::new(node);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind grpc listener");
        let addr = listener.local_addr().expect("grpc addr");
        let serving_node = Arc::clone(&node);
        let serving = tokio::spawn(async move {
            Server::builder()
                .add_service(MqServer::from_arc(serving_node))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .expect("serve coord.mq.v2");
        });
        tokio::time::sleep(Duration::from_millis(100)).await;

        Self {
            addr,
            raft0,
            mvcc0,
            manager,
            node,
            log_regions,
            group_regions,
            lifecycle,
            serving: Some(serving),
            _dir: dir,
        }
    }

    pub async fn client(&self) -> MqClient<Channel> {
        MqClient::connect(format!("http://{}", self.addr))
            .await
            .expect("connect coord.mq.v2")
    }

    pub fn topic_row(&self, name: &str) -> MqTopicMeta {
        self.mvcc0
            .mq_topic_table()
            .expect("read topic table")
            .topic(name)
            .unwrap_or_else(|| panic!("topic {name} must exist"))
            .clone()
    }

    /// 经 region 0 raft 提交一条 topic 命令（测试用：绕过 RPC 的配置边界，
    /// 例如小段大小；生产路径只有 `CreateTopic`/`UpdateTopic`）。
    pub async fn propose_topic(&self, op: MqTopicOp) -> MqTopicOutcome {
        let response = self
            .raft0
            .client_write(Command::MqTopic(op))
            .await
            .expect("propose topic op");
        match response.response() {
            RaftResponse::MqTopic { outcome } => outcome.clone(),
            other => panic!("unexpected topic response: {other:?}"),
        }
    }

    /// 截断一个分区的段（`Command::MqTruncate`），返回回收结果。
    pub async fn truncate(
        &self,
        meta: &MqTopicMeta,
        partition: u32,
        up_to_offset: u64,
    ) -> coord_server::storage::mq_log::MqTruncateResult {
        let shard_id = meta.shard_of_partition[partition as usize];
        let runtime = self
            .manager
            .route_runtime(&mq_log_shard_prefix(shard_id))
            .unwrap_or_else(|err| panic!("no runtime for shard {shard_id}: {err}"));
        let response = runtime
            .raft
            .client_write(Command::MqTruncate(MqTruncate {
                shard_id,
                topic_id: meta.topic_id,
                partition,
                up_to_offset,
            }))
            .await
            .expect("propose truncate");
        match response.response() {
            RaftResponse::MqTruncate { result } => *result,
            other => panic!("unexpected truncate response: {other:?}"),
        }
    }

    /// 分区的已 apply 边界（按分区所属分片的本地副本读）。
    pub fn bounds(
        &self,
        meta: &MqTopicMeta,
        partition: u32,
    ) -> Option<coord_server::storage::mq_log::MqPartitionBounds> {
        let shard_id = meta.shard_of_partition[partition as usize];
        let runtime = self
            .manager
            .route_runtime(&mq_log_shard_prefix(shard_id))
            .unwrap_or_else(|err| panic!("no runtime for shard {shard_id}: {err}"));
        runtime
            .mvcc
            .mq_partition_bounds(meta.topic_id, partition)
            .expect("read partition bounds")
    }

    /// `(append_batches, append_requests)`：批量摊薄 fsync 的证据。
    pub fn append_stats(&self) -> (u64, u64) {
        let metrics = self.node.metrics().expect("fixture injects metrics");
        let text = metrics.render_prometheus_text();
        let read = |name: &str| {
            text.lines()
                .find_map(|line| {
                    line.strip_prefix(name)?
                        .strip_prefix(' ')
                        .and_then(|value| value.trim().parse::<u64>().ok())
                })
                .unwrap_or(0)
        };
        (
            read("coord_mq_v2_append_batches_total"),
            read("coord_mq_v2_append_requests_total"),
        )
    }
}

fn assert_table_tiles(table: &RegionTable, expected_shard_regions: u32) {
    let mut ordered = table.regions.clone();
    ordered.sort_by(|left, right| left.start_key.cmp(&right.start_key));
    assert!(ordered
        .first()
        .expect("table non-empty")
        .start_key
        .is_empty());
    assert!(ordered.last().expect("table non-empty").end_key.is_empty());
    for pair in ordered.windows(2) {
        assert_eq!(pair[0].end_key, pair[1].start_key, "表必须平铺无空洞");
    }
    let shards = ordered
        .iter()
        .filter(|meta| coord_server::storage::region_table::is_mq_region(meta))
        .count();
    assert_eq!(shards as u32, expected_shard_regions);
}

async fn wait_region0_leader(raft: &CoordRaft) {
    let stop = tokio::time::Instant::now() + DEADLINE;
    loop {
        if raft.current_leader().await == Some(NODE_ID) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < stop,
            "region 0 leader not elected"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// 便捷发布：`topic_epoch = 0` 表示不校验代际。
pub async fn publish(
    client: &mut MqClient<Channel>,
    topic: &str,
    partition: i32,
    payload: &[u8],
    idempotency_key: &str,
) -> Result<mq_v2::PublishResponse, tonic::Status> {
    client
        .publish(mq_v2::PublishRequest {
            topic: topic.to_string(),
            partition,
            topic_epoch: 0,
            key: Vec::new(),
            payload: payload.to_vec(),
            idempotency_key: idempotency_key.to_string(),
            headers: Default::default(),
            producer: "m2-test".to_string(),
        })
        .await
        .map(|response| response.into_inner())
}

/// 便捷建 topic：`config = None` 走服务端默认值。
pub async fn create_topic(
    client: &mut MqClient<Channel>,
    topic: &str,
    partitions: i32,
    config: Option<mq_v2::TopicConfig>,
) -> mq_v2::CreateTopicResponse {
    client
        .create_topic(mq_v2::CreateTopicRequest {
            topic: topic.to_string(),
            partitions,
            request_id: String::new(),
            config,
        })
        .await
        .expect("CreateTopic must succeed")
        .into_inner()
}
