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

use coord_core::error_code::error_code_of;
use coord_proto::kv::v2::kv_client::KvClient;
use coord_proto::kv::v2::kv_server::KvServer;
use coord_proto::kv::v2::PutRequest;
use coord_server::raft::type_config::{Command, Response};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Code, Status};
use watch_node::{WatchNode, SEED_TS};

const FENCE_OP_ID: u64 = 41;

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
