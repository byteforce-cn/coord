//! KV-T G5（#167）split/merge 与 Watch/事务连续性验收：
//!
//! - Watch 只依赖 Commit Log，不依赖 Region 拓扑；split/merge 只改变路由；
//! - cutover 窗口内同一决策先在父 Region apply、再在子 Region 重放时，
//!   `commit_ts` 相同的同 key 变更只产出一个事件；
//! - `WatchCursor` 跨拓扑变化连续，从末游标重订不重放。

#[path = "support/kv_t_watch_node.rs"]
mod watch_node;

use std::sync::Arc;

use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionMeta};
use coord_server::kvt::decision_apply::apply_pending_decisions;
use coord_server::kvt::watch::{WatchCoordinator, WatchFilter, WatchStart};
use coord_server::storage::region_table::RegionTable;
use watch_node::WatchNode;

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
