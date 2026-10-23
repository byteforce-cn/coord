//! KV-T G5（#167）split/merge 与 Watch/事务连续性验收：
//!
//! - Watch 只依赖 Commit Log，不依赖 Region 拓扑；split/merge 只改变路由；
//! - cutover 窗口内同一决策先在父 Region apply、再在子 Region 重放时，
//!   `commit_ts` 相同的同 key 变更只产出一个事件；
//! - `WatchCursor` 跨拓扑变化连续，从末游标重订不重放。

#[path = "support/kv_t_watch_node.rs"]
mod watch_node;

use std::str::FromStr;
use std::sync::Arc;

use coord_core::error_code::error_code_of;
use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionMeta};
use coord_proto::kv::v2::kv_server::Kv as KvV2;
use coord_proto::kv::v2::{PutRequest, RangeRequest};
use coord_server::kvt::decision_apply::apply_pending_decisions;
use coord_server::kvt::watch::{WatchCoordinator, WatchFilter, WatchStart};
use coord_server::storage::region_table::RegionTable;
use tonic::metadata::MetadataValue;
use tonic::{Code, Request};
use watch_node::WatchNode;

/// v2 数据面入口的 Region 表版本 metadata（ADR-0015 D12.5）。
const ROUTING_EPOCH: &str = "coord-routing-epoch";

fn region(region_id: u64, start: &[u8], end: &[u8]) -> RegionMeta {
    RegionMeta {
        region_id,
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

/// 初态 `[.., m)` / `[m, ..)`；split 在 `n` 切出 Region 3，merge 再并回 2。
fn split_table(version: u64) -> RegionTable {
    RegionTable {
        version,
        regions: vec![
            region(1, b"", b"m"),
            region(2, b"m", b"n"),
            region(3, b"n", b""),
        ],
    }
}

fn merged_table(version: u64) -> RegionTable {
    RegionTable {
        version,
        regions: vec![region(1, b"", b"m"), region(2, b"m", b"")],
    }
}

fn initial_table(version: u64) -> RegionTable {
    RegionTable {
        version,
        regions: vec![region(1, b"", b"m"), region(2, b"m", b"")],
    }
}

fn with_epoch<T>(message: T, epoch: u64) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        ROUTING_EPOCH,
        MetadataValue::from_str(&epoch.to_string()).expect("ascii epoch"),
    );
    request
}

fn assert_epoch_stale<T: std::fmt::Debug>(result: Result<T, tonic::Status>, current: u64) {
    let status = result.expect_err("expected EPOCH_STALE");
    assert_eq!(status.code(), Code::Aborted, "{}", status.message());
    assert_eq!(
        error_code_of(&status).as_deref(),
        Some("EPOCH_STALE"),
        "structured code"
    );
    let epoch = status
        .metadata()
        .get(ROUTING_EPOCH)
        .expect("trailer carries the current epoch")
        .to_str()
        .expect("ascii");
    assert_eq!(epoch, current.to_string(), "trailer epoch");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_watch_survives_split_merge() {
    let fx = WatchNode::start().await;
    fx.open_first_shard().await;

    let coordinator = WatchCoordinator::new(Arc::clone(&fx.shards));
    let cursor = coordinator
        .start(WatchStart {
            start_revision: watch_node::SEED_TS + 1,
            start_cursor: None,
        })
        .await
        .expect("start watch");
    let before_split = cursor;

    let ts1 = fx.commit_pairs(&[(b"aa", b"pre")]).await;

    fx.apply_region_table_and_spawn(split_table(8)).await;
    // 新 Region 从 0 起步：先 apply 已有决策，把全局 safe_ts 追上，后续事务
    // 才能用低于水位的 start_ts 开始。
    apply_pending_decisions(&fx.node, 16)
        .await
        .expect("bring the new region to the decision watermark");

    // 先在子 Region 决策，再把同一条决策“迟到”地 apply 到父 Region：模拟
    // cutover 前父 Region 先落日志、子 Region 从决策日志重放。
    let txn = fx.begin(watch_node::SEED_TS as i64).await;
    let ts2 = fx
        .commit_puts(&txn.txn_id, &[(b"nn", b"child")], &txn.txn_id)
        .await;
    fx.replay_decision_on_region(2, b"nn", b"child", ts2).await;
    apply_pending_decisions(&fx.node, 16)
        .await
        .expect("apply decisions after split");
    fx.settle(ts2).await;

    fx.apply_region_table_and_spawn(merged_table(9)).await;
    fx.retire_region(3).await;

    let ts3 = fx.commit_pairs(&[(b"zz", b"post")]).await;
    assert!(ts1 < ts2 && ts2 < ts3, "commit_ts must be strictly ordered");

    let (events, cursor) = fx
        .poll_until(&coordinator, before_split, WatchFilter::all(), 3)
        .await;
    let timestamps: Vec<u64> = events.iter().map(|event| event.commit_ts).collect();
    assert_eq!(
        timestamps,
        vec![ts1, ts2, ts3],
        "split/merge must not drop or duplicate commit_ts"
    );
    assert_eq!(cursor.commit_ts, ts3, "cursor stays continuous");

    let replayed = events
        .iter()
        .find(|event| event.commit_ts == ts2)
        .expect("replayed decision event");
    assert_eq!(
        replayed.changes.len(),
        1,
        "the same key replayed on parent and child must yield one change"
    );
    assert_eq!(replayed.changes[0].key, b"nn");

    let (again, _) = fx
        .poll_until(&coordinator, cursor, WatchFilter::all(), 0)
        .await;
    assert!(
        again.is_empty(),
        "a cursor at the watermark must not repeat events"
    );

    // 从 split 前位置重订只拿到一次重放事件。
    let cursor_at_replay = coordinator
        .start(WatchStart {
            start_revision: ts2,
            start_cursor: None,
        })
        .await
        .expect("resubscribe at the replayed revision");
    let (resumed, _) = fx
        .poll_until(&coordinator, cursor_at_replay, WatchFilter::all(), 1)
        .await;
    let resumed_event = resumed
        .iter()
        .find(|event| event.commit_ts == ts2)
        .expect("resubscribe starts at the replayed revision");
    assert_eq!(resumed_event.changes.len(), 1);
}

/// cutover 之后同一 key 只能由一个表版本服务：旧 epoch 的读/写都 `EPOCH_STALE`，
/// 当前 epoch 的写只落新 Region——旧 Region 保留 cutover 前那行，不接收新写。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_split_merge_no_double_serve() {
    let fx = WatchNode::start().await;
    fx.apply_region_table_and_spawn(initial_table(1)).await;
    let tso = fx.node.tso_client().expect("tso client").clone();

    let before = tso.get_ts().await.expect("tso");
    KvV2::put(
        &*fx.node,
        with_epoch(
            PutRequest {
                key: b"nn".to_vec(),
                value: b"old".to_vec(),
                commit_ts: before as i64,
                ..Default::default()
            },
            1,
        ),
    )
    .await
    .expect("the pre-cutover write is served by the old table");

    // cutover：Region 2 在 `n` 处 split，Region 3 接管 [n, ..)。
    fx.apply_region_table_and_spawn(split_table(2)).await;

    assert_epoch_stale(
        KvV2::put(
            &*fx.node,
            with_epoch(
                PutRequest {
                    key: b"nn".to_vec(),
                    value: b"stale".to_vec(),
                    commit_ts: tso.get_ts().await.expect("tso") as i64,
                    ..Default::default()
                },
                1,
            ),
        )
        .await,
        2,
    );
    assert_epoch_stale(
        KvV2::range(
            &*fx.node,
            with_epoch(
                RangeRequest {
                    key: b"nn".to_vec(),
                    ..Default::default()
                },
                1,
            ),
        )
        .await,
        2,
    );

    let after = tso.get_ts().await.expect("tso");
    KvV2::put(
        &*fx.node,
        with_epoch(
            PutRequest {
                key: b"nn".to_vec(),
                value: b"new".to_vec(),
                commit_ts: after as i64,
                ..Default::default()
            },
            2,
        ),
    )
    .await
    .expect("the current table version serves the key");

    assert_eq!(
        fx.manager
            .runtime(3)
            .expect("new region")
            .mvcc
            .get(b"nn")
            .expect("new region read")
            .as_deref(),
        Some(&b"new"[..])
    );
    assert_eq!(
        fx.manager
            .runtime(2)
            .expect("old region")
            .mvcc
            .get(b"nn")
            .expect("old region read")
            .as_deref(),
        Some(&b"old"[..]),
        "the old region keeps the pre-cutover row and must not receive the new write"
    );
}
