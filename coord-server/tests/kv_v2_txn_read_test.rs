//! KV-T G2（#144）`coord.txn.v2` 读阶段：
//!
//! - `Begin` 固定 `start_ts`（显式或本地 TSO），所有 `Read` 在同一快照上；
//! - `Read` 跨 Region 复用 #143/#188 的本地快照读，并记录服务端 OCC 读集；
//! - 读集去重/合并、条目上限与单次 range 扫描上限（不静默截断）；
//! - 会话 TTL/丢失 → `ABORTED + TXN_NOT_FOUND`；`Abort` 幂等。
//!
//! 夹具为单节点 × 3 个数据 Region（`["", "d")` / `["d", "n")` / `["n", "")`），
//! 单 voter、`RevisionMode::CommitTs`，与 `kv_v2_range_fanout_test.rs` 同路径。

use std::sync::Arc;
use std::time::Duration;

use coord_core::error_code::error_code_of;
use coord_core::storage::StorageBackend;
use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionId, RegionMeta, StorageConfig};
use coord_proto::txn::v2 as txn_v2;
use coord_proto::txn::v2::txn_server::Txn as TxnV2;
use coord_server::kvt::{KvTCapability, KvTRuntimeConfig, KvTState};
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcService};
use coord_server::raft::region::RegionManager;
use coord_server::raft::region_runtime::{region_data_dir, RegionRuntimeSpec};
use coord_server::raft::tso::{TsoBatch, TsoClient, TsoSource};
use coord_server::raft::type_config::{Command, Response};
use coord_server::raft::{RaftConfig, WatchReceiver};
use coord_server::server::CoordNode;
use coord_server::storage::mvcc::{MvccStorage, RevisionMode};
use coord_server::storage::redb_backend::RedbBackend;
use tempfile::TempDir;
use tonic::Code;

const REGIONS: [(RegionId, &[u8], &[u8]); 3] = [(1, b"", b"d"), (2, b"d", b"n"), (3, b"n", b"")];

fn find_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// 可控 TSO：单批从 1 开始，`is_leader=true`（读阶段不关心批耗尽后的转发）。
struct FakeTso;

#[async_trait::async_trait]
impl TsoSource for FakeTso {
    async fn is_leader(&self) -> bool {
        true
    }

    async fn allocate(&self, batch_size: u32) -> coord_core::error::Result<TsoBatch> {
        Ok(TsoBatch {
            start_ts: 1,
            end_ts: 1 + u64::from(batch_size.max(1)),
        })
    }
}

struct TxnNode {
    node: Arc<CoordNode>,
    manager: Arc<RegionManager>,
    _dir: TempDir,
}

impl TxnNode {
    async fn start(max_range: u32, max_read_set: u32, tso: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().to_path_buf();
        let addr = format!("127.0.0.1:{}", find_port());

        let factory = RaftNetworkFactoryImpl::new(1);
        factory.register_node(1, addr.clone());
        let rpc = RaftRpcService::new();
        let manager = Arc::new(RegionManager::new(1));
        for (region_id, start, end) in REGIONS {
            let meta = RegionMeta {
                region_id,
                start_key: start.to_vec(),
                end_key: end.to_vec(),
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
                        raft_config: Arc::new(RaftConfig {
                            heartbeat_interval: 50,
                            election_timeout_min: 200,
                            election_timeout_max: 400,
                            ..Default::default()
                        }),
                        object_store: None,
                        revision_mode: RevisionMode::CommitTs,
                    },
                    true,
                )
                .await
                .expect("spawn data region");
        }

        // region 模式下 KV 全量走 manager 路由；scratch 只满足构造签名。
        let backend = RedbBackend::open(&base, &StorageConfig::default()).unwrap();
        let scratch = Arc::new(MvccStorage::new(backend).unwrap());
        let mut node = CoordNode::new(scratch);
        node.node_id = 1;
        node.region_manager = Some(Arc::clone(&manager));
        node.set_kvt_state(
            KvTState::new(
                true,
                KvTRuntimeConfig {
                    max_range,
                    max_read_set,
                    read_ts_wait_ms: 50,
                    ..KvTRuntimeConfig::default()
                },
            )
            .with_implemented([KvTCapability::Kv, KvTCapability::Txn]),
        );
        if tso {
            node.set_tso_client(TsoClient::new(Arc::new(FakeTso), 8, None));
        }
        let node = Arc::new(node);

        wait_for_leaders(&manager).await;
        Self {
            node,
            manager,
            _dir: dir,
        }
    }

    /// 8 个 key 跨 3 个 Region（r1: a,a2,a3,b；r2: d,e,e2；r3: n,z），
    /// 三区 current 均 >= 400（safe_ts = 400；e2@420 对 400 不可见）。
    async fn standard(max_range: u32, max_read_set: u32, tso: bool) -> Self {
        let fx = Self::start(max_range, max_read_set, tso).await;
        fx.put(b"a", b"a100", 100).await;
        fx.put(b"b", b"b200", 200).await;
        fx.put(b"a2", b"a260", 260).await;
        fx.put(b"a3", b"a400", 400).await;
        fx.put(b"d", b"d100", 100).await;
        fx.put(b"e", b"e250", 250).await;
        fx.put(b"e2", b"e420", 420).await;
        fx.put(b"n", b"n100", 100).await;
        fx.put(b"z", b"z400", 400).await;
        assert_eq!(fx.node.safe_ts(), 400);
        fx
    }

    /// 直接向目标 Region 的 raft 提交 `PutV2`（绕过 txn 层，模拟并发提交）。
    async fn put(&self, key: &[u8], value: &[u8], commit_ts: u64) {
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
        assert!(
            matches!(resp.response(), Response::Put { revision } if *revision == commit_ts),
            "put must apply at its commit_ts"
        );
    }

    async fn begin(
        &self,
        start_ts: i64,
        ttl_ms: u32,
    ) -> Result<txn_v2::BeginResponse, tonic::Status> {
        TxnV2::begin(
            &*self.node,
            tonic::Request::new(txn_v2::BeginRequest { start_ts, ttl_ms }),
        )
        .await
        .map(|resp| resp.into_inner())
    }

    async fn read(
        &self,
        txn_id: &[u8],
        key: &[u8],
        end: &[u8],
        limit: i64,
        keys_only: bool,
    ) -> Result<txn_v2::ReadResponse, tonic::Status> {
        TxnV2::read(
            &*self.node,
            tonic::Request::new(txn_v2::ReadRequest {
                txn_id: txn_id.to_vec(),
                key: key.to_vec(),
                range_end: end.to_vec(),
                limit,
                keys_only,
            }),
        )
        .await
        .map(|resp| resp.into_inner())
    }

    async fn abort(&self, txn_id: &[u8]) -> Result<(), tonic::Status> {
        TxnV2::abort(
            &*self.node,
            tonic::Request::new(txn_v2::AbortRequest {
                txn_id: txn_id.to_vec(),
            }),
        )
        .await
        .map(|_| ())
    }
}

async fn wait_for_leaders(manager: &RegionManager) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let ready = REGIONS.iter().all(|(id, _, _)| {
            manager.runtime(*id).is_some_and(|rt| {
                let m = rt.raft.metrics();
                let m = m.borrow_watched();
                m.current_leader == Some(1) && m.last_quorum_acked.is_some()
            })
        });
        if ready {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "region leaders not elected in time"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn keys(resp: &txn_v2::ReadResponse) -> Vec<Vec<u8>> {
    resp.kvs.iter().map(|kv| kv.key.clone()).collect()
}

fn expected_keys() -> Vec<Vec<u8>> {
    [b"a".as_slice(), b"a2", b"a3", b"b", b"d", b"e", b"n", b"z"]
        .iter()
        .map(|k| k.to_vec())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_txn_reads_one_snapshot_across_regions() {
    let fx = TxnNode::standard(100, 16, false).await;
    let begin = fx.begin(400, 0).await.unwrap();
    assert_eq!(begin.start_ts, 400);
    assert!(!begin.txn_id.is_empty());

    let a = fx.read(&begin.txn_id, b"a", b"", 0, false).await.unwrap();
    assert_eq!(a.read_ts, 400);
    assert_eq!(a.kvs.len(), 1);
    assert_eq!(a.kvs[0].value, b"a100");
    assert_eq!(a.kvs[0].mod_revision, 100);

    // 快照固定：Begin 之后的写入（commit_ts=500）对本会话不可见。
    fx.put(b"a", b"a500", 500).await;
    let a = fx.read(&begin.txn_id, b"a", b"", 0, false).await.unwrap();
    assert_eq!(a.read_ts, 400);
    assert_eq!(a.kvs[0].value, b"a100");
    assert_eq!(a.kvs[0].mod_revision, 100);

    let all = fx
        .read(&begin.txn_id, b"a", b"zzz", 0, false)
        .await
        .unwrap();
    assert_eq!(all.read_ts, 400);
    assert_eq!(keys(&all), expected_keys());
    assert!(all.kvs.iter().all(|kv| kv.mod_revision <= 400));

    let keys_only = fx.read(&begin.txn_id, b"d", b"n", 0, true).await.unwrap();
    assert_eq!(keys(&keys_only), vec![b"d".to_vec(), b"e".to_vec()]);
    assert!(keys_only.kvs.iter().all(|kv| kv.value.is_empty()));

    // 空谓词（end < key）返回空，不路由、不回退成全扫描。
    let empty = fx.read(&begin.txn_id, b"n", b"a", 0, false).await.unwrap();
    assert!(empty.kvs.is_empty());
    assert_eq!(empty.read_ts, 400);

    fx.abort(&begin.txn_id).await.unwrap();
    let err = fx
        .read(&begin.txn_id, b"a", b"", 0, false)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Aborted);
    assert_eq!(error_code_of(&err).as_deref(), Some("TXN_NOT_FOUND"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_txn_begin_allocates_start_ts_from_local_tso() {
    let fx = TxnNode::standard(100, 16, true).await;
    let begin = fx.begin(0, 0).await.unwrap();
    assert_eq!(begin.start_ts, 1, "fake TSO batch starts at 1");

    let read = fx.read(&begin.txn_id, b"a", b"", 0, false).await.unwrap();
    assert_eq!(read.read_ts, 1);
    assert!(read.kvs.is_empty(), "a@100 is after start_ts=1");

    // 没有 TSO 装配时 fail-closed（不是静默给 0 / 本地时钟）。
    let no_tso = TxnNode::standard(100, 16, false).await;
    let err = no_tso.begin(0, 0).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_txn_read_set_limit_fails_closed() {
    let fx = TxnNode::standard(100, 2, false).await;
    let begin = fx.begin(400, 0).await.unwrap();
    fx.read(&begin.txn_id, b"a", b"", 0, false).await.unwrap();
    fx.read(&begin.txn_id, b"d", b"", 0, false).await.unwrap();

    let err = fx
        .read(&begin.txn_id, b"n", b"", 0, false)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert_eq!(error_code_of(&err).as_deref(), Some("READ_SET_TOO_LARGE"));

    // 已覆盖的 point 不增长读集：满额会话里重复读仍成功。
    fx.read(&begin.txn_id, b"a", b"", 0, false).await.unwrap();

    // 新会话：一个 range 谓词只占一个条目。
    let fresh = fx.begin(400, 0).await.unwrap();
    fx.read(&fresh.txn_id, b"a", b"zzz", 0, false)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_txn_range_scan_over_max_range_fails_closed() {
    let fx = TxnNode::standard(3, 16, false).await;
    let begin = fx.begin(400, 0).await.unwrap();

    // r1 有 4 个命中 key（a/a2/a3/b）→ 超过 max_range=3，必须拒绝而不是截断。
    let err = fx
        .read(&begin.txn_id, b"a", b"zzz", 0, false)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert_eq!(error_code_of(&err).as_deref(), Some("READ_SET_TOO_LARGE"));

    // 显式 limit 是调用方要求的前缀：limit=2 返回前两条。
    let limited = fx
        .read(&begin.txn_id, b"a", b"zzz", 2, false)
        .await
        .unwrap();
    assert_eq!(keys(&limited), vec![b"a".to_vec(), b"a2".to_vec()]);

    // point read 不受 max_range 约束。
    let point = fx.read(&begin.txn_id, b"a3", b"", 0, false).await.unwrap();
    assert_eq!(point.kvs.len(), 1);

    // 小 range 正常返回；显式 limit 超过 max_range 仍拒绝。
    let small = fx.read(&begin.txn_id, b"n", b"o", 0, false).await.unwrap();
    assert_eq!(keys(&small), vec![b"n".to_vec()]);
    let err = fx
        .read(&begin.txn_id, b"n", b"o", 4, false)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert_eq!(error_code_of(&err).as_deref(), Some("READ_SET_TOO_LARGE"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_txn_session_ttl_and_unknown_id() {
    let fx = TxnNode::standard(100, 16, false).await;
    let begin = fx.begin(400, 1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let err = fx
        .read(&begin.txn_id, b"a", b"", 0, false)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Aborted);
    assert_eq!(error_code_of(&err).as_deref(), Some("TXN_NOT_FOUND"));

    let err = fx.read(b"missing", b"a", b"", 0, false).await.unwrap_err();
    assert_eq!(error_code_of(&err).as_deref(), Some("TXN_NOT_FOUND"));

    // Abort 幂等：重复调用不改变结果。
    let begin = fx.begin(400, 0).await.unwrap();
    fx.abort(&begin.txn_id).await.unwrap();
    fx.abort(&begin.txn_id).await.unwrap();
    let err = fx
        .read(&begin.txn_id, b"a", b"", 0, false)
        .await
        .unwrap_err();
    assert_eq!(error_code_of(&err).as_deref(), Some("TXN_NOT_FOUND"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_txn_begin_rejects_future_or_invalid_start_ts() {
    let fx = TxnNode::standard(100, 16, false).await;

    // start_ts 超过 safe_ts：有界等待后 SAFE_TS_LAG（可重试），不建会话。
    let err = fx.begin(10_000, 0).await.unwrap_err();
    assert_eq!(err.code(), Code::Unavailable);
    assert_eq!(error_code_of(&err).as_deref(), Some("SAFE_TS_LAG"));

    let err = fx.begin(-1, 0).await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
}
