//! KV-T G5（#167/#161）split 源 Region 的直写 fence：
//!
//! - fence 进源 Region 自己的 raft 日志，apply 序即真相：落在搬迁区间的 v2 直写
//!   （`coord.kv.v2` Put/Delete/DeleteRange）返回 `ABORTED` + `EPOCH_STALE`；
//! - 父 Region 左半段（fence 区间外）仍然可写；
//! - fence 记录随 Region 存储保存，clear 后区间内恢复可写。

#[path = "support/kv_t_watch_node.rs"]
mod watch_node;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use coord_core::error_code::error_code_of;
use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionMeta};
use coord_proto::kv::v2::kv_client::KvClient;
use coord_proto::kv::v2::kv_server::KvServer;
use coord_proto::kv::v2::PutRequest;
use coord_server::pd::executor::SplitMergeMigrator;
use coord_server::raft::type_config::{Command, Response};
use coord_server::split_merge::{
    RangeImportMigrator, RegionImportTargetResolver, RegionMvccResolver, RegionRaftResolver,
    RegionRangeResolver, SplitDrain,
};
use coord_server::storage::region_table::{plan_merge, RegionTable};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Code, Status};
use watch_node::{WatchNode, SEED_TS};

const FENCE_OP_ID: u64 = 41;
const MERGE_OP_ID: u64 = 42;

/// 只挂 `coord.kv.v2`：fence 走 Region raft，入口走 gRPC handler。
async fn serve_kv(
    node: Arc<coord_server::server::CoordNode>,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
    let addr = listener.local_addr().expect("addr").to_string();
    let handle = tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(KvServer::from_arc(node))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    (addr, handle)
}

fn assert_fenced(status: Status) {
    assert_eq!(
        status.code(),
        Code::Aborted,
        "unexpected status: {}",
        status.message()
    );
    assert_eq!(
        error_code_of(&status).as_deref(),
        Some("EPOCH_STALE"),
        "迁移窗口与 cutover 后旧 epoch 同码（可重试）"
    );
}

fn put(key: &[u8], value: &[u8], commit_ts: u64) -> PutRequest {
    PutRequest {
        key: key.to_vec(),
        value: value.to_vec(),
        commit_ts: commit_ts as i64,
        ..Default::default()
    }
}

fn region(id: u64, start: &[u8], end: &[u8]) -> RegionMeta {
    RegionMeta {
        region_id: id,
        start_key: start.to_vec(),
        end_key: end.to_vec(),
        epoch: RegionEpoch::initial(),
        peers: vec![Peer {
            node_id: 1,
            raft_addr: "127.0.0.1:1".to_string(),
            role: PeerRole::Voter,
        }],
        approximate_size: 0,
        approximate_keys: 0,
    }
}

fn two_region_table() -> RegionTable {
    let mut table = RegionTable::default();
    table
        .init(vec![region(1, b"", b"m"), region(2, b"m", b"")])
        .expect("init region table");
    table
}

struct NoopDrain;

#[async_trait]
impl SplitDrain for NoopDrain {
    async fn drain(&self, _start: &[u8], _end: &[u8], _timeout: Duration) -> Result<(), String> {
        Ok(())
    }
}

fn merge_migrator(fx: &WatchNode) -> RangeImportMigrator {
    merge_migrator_with_node(fx, 1)
}

fn merge_migrator_with_node(fx: &WatchNode, node_id: u64) -> RangeImportMigrator {
    let source: Arc<RegionMvccResolver> = {
        let manager = Arc::clone(&fx.manager);
        Arc::new(move |region_id| {
            manager
                .runtime(region_id)
                .map(|runtime| Arc::clone(&runtime.mvcc))
        })
    };
    let ranges: Arc<RegionRangeResolver> = {
        let manager = Arc::clone(&fx.manager);
        Arc::new(move |region_id| {
            manager.get_region(region_id).map(|handle| {
                let meta = handle.meta.read();
                (meta.start_key.clone(), meta.end_key.clone())
            })
        })
    };
    let source_raft: Arc<RegionRaftResolver> = {
        let manager = Arc::clone(&fx.manager);
        Arc::new(move |region_id| manager.runtime(region_id).map(|rt| rt.raft.clone()))
    };
    let targets: Arc<RegionImportTargetResolver> = Arc::new(|_| None);
    RangeImportMigrator::new(
        source,
        targets,
        ranges,
        source_raft,
        Arc::new(NoopDrain),
        Arc::new(|_| Ok(None)),
    )
    .with_node_id(node_id)
    .with_donor_fence_wait(Duration::from_secs(1))
}

/// merge 的 donor 区间也在 cutover 前被 fence：窗口内的直写不会落进被吸收 Region
/// 后消失；吸收方（本节点同时是 donor leader）在 `arm_write_fence` 里直接提案并
/// 等本地副本看到它。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_merge_donor_fence_rejects_direct_writes_until_released() {
    let fx = WatchNode::start().await;
    let (addr, _handle) = serve_kv(Arc::clone(&fx.node)).await;
    let mut kv = KvClient::connect(format!("http://{addr}"))
        .await
        .expect("connect kv v2");

    let plan = plan_merge(&two_region_table(), 1, 2).expect("merge plan");
    let migrator = merge_migrator(&fx);
    migrator
        .arm_write_fence(MERGE_OP_ID, &plan)
        .await
        .expect("arm merge donor fence");

    let donor = fx.manager.runtime(2).expect("donor runtime");
    assert_eq!(
        donor
            .mvcc
            .direct_write_fence()
            .expect("read fence")
            .map(|fence| fence.op_id),
        Some(MERGE_OP_ID),
        "the donor region must carry this merge's fence"
    );
    assert_fenced(
        kv.put(put(b"m2", b"v", SEED_TS + 20))
            .await
            .expect_err("direct writes inside the donor range must be fenced"),
    );

    // 吸收方区间不受影响。
    kv.put(put(b"aa2", b"left", SEED_TS + 21))
        .await
        .expect("the absorbing region stays writable");

    migrator
        .release_write_fence(MERGE_OP_ID, &plan)
        .await
        .expect("release merge donor fence");
    assert!(
        donor
            .mvcc
            .direct_write_fence()
            .expect("read fence")
            .is_none(),
        "releasing the merge must drop the donor fence"
    );
    kv.put(put(b"m2", b"v", SEED_TS + 22))
        .await
        .expect("the donor range is writable again after release");
}

/// 吸收方不是 donor leader 时，`arm_write_fence` 必须等 donor leader 把 fence
/// apply 到本地副本；等不到就 fail-closed，看到后才允许导出。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_merge_arm_waits_for_the_donor_leaders_fence() {
    let fx = WatchNode::start().await;
    let plan = plan_merge(&two_region_table(), 1, 2).expect("merge plan");
    // 本节点（node 2 视角）不是 donor leader：只能等。
    // 用 trait object 走 executor 的同一条方法（inherent 方法不会被 dyn 调用）。
    let migrator: Arc<dyn SplitMergeMigrator> = Arc::new(merge_migrator_with_node(&fx, 2));
    assert!(
        !migrator
            .arm_donor_write_fence(MERGE_OP_ID + 1, &plan)
            .await
            .expect("not being the donor leader is not an error"),
        "a non-leader must not arm the donor fence through the trait"
    );
    assert!(
        !migrator
            .release_donor_write_fence(MERGE_OP_ID + 1, &plan)
            .await
            .expect("not owning the fence is not an error"),
        "a non-leader must not claim the fence is released through the trait"
    );
    let err = migrator
        .arm_write_fence(MERGE_OP_ID + 1, &plan)
        .await
        .expect_err("exporting without the donor fence must fail closed");
    assert!(
        err.contains("did not apply fence op 43"),
        "unexpected error: {err}"
    );

    // donor leader 完成提案（真实节点上是 donor leader 的 executor 扫描 intent）。
    let donor = fx.manager.runtime(2).expect("donor runtime");
    donor
        .raft
        .client_write(Command::FenceDirectWrites {
            start_key: b"m".to_vec(),
            end_key: Vec::new(),
            op_id: MERGE_OP_ID + 1,
        })
        .await
        .expect("donor leader arms the fence");

    migrator
        .arm_write_fence(MERGE_OP_ID + 1, &plan)
        .await
        .expect("the local replica now has the barrier");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_split_source_fence_rejects_direct_writes_inside_the_migrating_range() {
    let fx = WatchNode::start().await;
    let (addr, _handle) = serve_kv(Arc::clone(&fx.node)).await;
    let mut kv = KvClient::connect(format!("http://{addr}"))
        .await
        .expect("connect kv v2");

    // 搬迁区间 = 右半 Region 的全部 key（split_key = "m"）。
    let region = fx.manager.route_runtime(b"m").expect("route right half");
    let armed = region
        .raft
        .client_write(Command::FenceDirectWrites {
            start_key: b"m".to_vec(),
            end_key: Vec::new(),
            op_id: FENCE_OP_ID,
        })
        .await
        .expect("propose fence")
        .response()
        .clone();
    assert!(
        matches!(armed, Response::DirectWritesFenced { op_id } if op_id == FENCE_OP_ID),
        "unexpected fence response: {armed:?}"
    );

    // 区间内直写被拒（Put / Delete / DeleteRange 共用同一判据）。
    assert_fenced(
        kv.put(put(b"m2", b"v", SEED_TS + 10))
            .await
            .expect_err("fenced put must fail"),
    );
    assert_fenced(
        kv.delete(coord_proto::kv::v2::DeleteRequest {
            key: b"m2".to_vec(),
            commit_ts: (SEED_TS + 11) as i64,
            ..Default::default()
        })
        .await
        .expect_err("fenced delete must fail"),
    );
    assert_fenced(
        kv.delete(coord_proto::kv::v2::DeleteRequest {
            key: b"m".to_vec(),
            range_end: b"z".to_vec(),
            commit_ts: (SEED_TS + 12) as i64,
            ..Default::default()
        })
        .await
        .expect_err("delete range crossing the fence must fail"),
    );

    // 父 Region 左半段照常可写。
    let ok = kv
        .put(put(b"aa2", b"left", SEED_TS + 13))
        .await
        .expect("writes outside the migrating range stay writable");
    assert_eq!(ok.into_inner().commit_ts, (SEED_TS + 13) as i64);

    // 旧 op_id 的 clear 不解开本轮 fence。
    let stale = region
        .raft
        .client_write(Command::ClearDirectWriteFence {
            op_id: FENCE_OP_ID - 1,
        })
        .await
        .expect("propose stale clear")
        .response()
        .clone();
    assert!(matches!(stale, Response::DirectWriteFenceCleared { .. }));
    assert_fenced(
        kv.put(put(b"m2", b"v", SEED_TS + 14))
            .await
            .expect_err("stale clear must not unfence"),
    );

    // `op_id = 0` 无条件清除（merge 扩回区间前摘掉上一次 split 泄漏的 fence）。
    region
        .raft
        .client_write(Command::ClearDirectWriteFence { op_id: 0 })
        .await
        .expect("propose unconditional clear");
    assert!(
        region.mvcc.direct_write_fence().unwrap().is_none(),
        "unconditional clear must drop the fence regardless of op_id"
    );

    // 重新 arm 后按同一 op_id 解除：区间内恢复可写且可读。
    region
        .raft
        .client_write(Command::FenceDirectWrites {
            start_key: b"m".to_vec(),
            end_key: Vec::new(),
            op_id: FENCE_OP_ID,
        })
        .await
        .expect("re-arm fence");
    assert_fenced(
        kv.put(put(b"m2", b"v", SEED_TS + 15))
            .await
            .expect_err("re-armed fence must reject"),
    );
    let cleared = region
        .raft
        .client_write(Command::ClearDirectWriteFence { op_id: FENCE_OP_ID })
        .await
        .expect("propose clear")
        .response()
        .clone();
    assert!(
        matches!(cleared, Response::DirectWriteFenceCleared { op_id } if op_id == FENCE_OP_ID),
        "unexpected clear response: {cleared:?}"
    );
    assert!(region.mvcc.direct_write_fence().unwrap().is_none());

    kv.put(put(b"m2", b"v", SEED_TS + 16))
        .await
        .expect("write inside the released range");
    // 直接看源 Region 存储：gRPC 读受 safe_ts（另一 Region 的已 apply 水位）
    // 限制，与本用例的 fence 判据无关。
    assert_eq!(
        region.mvcc.get(b"m2").unwrap().as_deref(),
        Some(&b"v"[..]),
        "the write must be visible after unfence"
    );
}
