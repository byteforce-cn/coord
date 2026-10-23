//! KV-T G6（#168）greenfield v2 全链路验收：
//!
//! 一个进程内 v2 集群（multi-raft + `[kv_t]`：region 0 + 两个数据 Region）走通
//! TSO → 跨 Region 事务 → 严格 Watch → split → merge：
//!
//! - `commit_ts` 落成 key 的 `mod_revision`（ADR-0015 D3）；
//! - 跨 Region 同一事务在 Watch 里是一批、按 `commit_ts` 全局有序（D5）；
//! - split/merge 只改路由，Watch 游标连续、cutover 重放不重复（D6）；
//! - 拓扑变化前后 `current_revision` 不倒退（D11.2）。
//!
//! 锚点映射见 `docs/adr/0015-kv-t.md`「验收锚点」。

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
async fn test_full_chain_tso_txn_watch_split_merge() {
    let fx = WatchNode::start().await;
    fx.open_first_shard().await;
    let tso = fx.node.tso_client().expect("tso client").clone();

    // TSO → revision：显式 commit_ts 必须成为 key 的 mod_revision。
    let ts1 = tso.get_ts().await.expect("first tso");
    let ts2 = tso.get_ts().await.expect("second tso");
    assert!(ts2 > ts1, "TSO allocations must be strictly increasing");
    fx.put_v2(b"aa", b"tso", ts1).await;
    let left = fx.manager.route_runtime(b"aa").expect("route aa");
    let meta = left
        .mvcc
        .get_kv_metadata(b"aa")
        .expect("aa metadata")
        .expect("aa row");
    assert_eq!(
        meta.mod_revision, ts1 as i64,
        "commit_ts is the data revision (ADR-0015 D3)"
    );

    // 跨 Region 事务：两个 Region 各一键，落在同一个 commit_ts。
    let coordinator = WatchCoordinator::new(Arc::clone(&fx.shards));
    let start = coordinator
        .start(WatchStart {
            start_revision: watch_node::SEED_TS + 1,
            start_cursor: None,
        })
        .await
        .expect("start watch");
    let txn_ts = fx
        .commit_pairs(&[(b"bb", b"left"), (b"zz", b"right")])
        .await;
    assert!(txn_ts > ts2, "sequencer allocates above the client batch");
    let (events, cursor) = fx
        .poll_until(&coordinator, start, WatchFilter::all(), 1)
        .await;
    assert_eq!(events.len(), 1, "one transaction is one Watch event");
    assert_eq!(events[0].commit_ts, txn_ts);
    let keys: Vec<Vec<u8>> = events[0]
        .changes
        .iter()
        .map(|change| change.key.clone())
        .collect();
    assert_eq!(
        keys,
        vec![b"bb".to_vec(), b"zz".to_vec()],
        "the cross-region transaction stays one atomic batch"
    );
    assert_eq!(events[0].changes[0].value.as_deref(), Some(&b"left"[..]));
    assert_eq!(events[0].changes[1].value.as_deref(), Some(&b"right"[..]));

    let rev_left_before = left.mvcc.current_revision();
    let right = fx.manager.route_runtime(b"zz").expect("route zz");
    let rev_right_before = right.mvcc.current_revision();

    // split：Region 2 在 `n` 处裂成 [m,n) + [n,..)；子 Region 先决策、父 Region
    // 迟到重放同一 commit_ts。
    fx.apply_region_table_and_spawn(split_table(2)).await;
    apply_pending_decisions(&fx.node, 16)
        .await
        .expect("bring the new region to the decision watermark");
    let txn = fx.begin(watch_node::SEED_TS as i64).await;
    let split_ts = fx
        .commit_puts(&txn.txn_id, &[(b"nn", b"child")], &txn.txn_id)
        .await;
    assert!(split_ts > txn_ts);
    fx.replay_decision_on_region(2, b"nn", b"child", split_ts)
        .await;
    apply_pending_decisions(&fx.node, 16)
        .await
        .expect("apply the decision after split");
    fx.settle(split_ts).await;

    // merge：并回两个 Region；再提交一个事务验证拓扑变化后的写入仍可用。
    fx.apply_region_table_and_spawn(merged_table(3)).await;
    fx.retire_region(3).await;
    let txn = fx.begin(watch_node::SEED_TS as i64).await;
    let merge_ts = fx
        .commit_puts(&txn.txn_id, &[(b"zz", b"merged")], &txn.txn_id)
        .await;
    assert!(merge_ts > split_ts);
    apply_pending_decisions(&fx.node, 16)
        .await
        .expect("apply the decision after merge");
    fx.settle(merge_ts).await;

    // Watch 跨两次拓扑变化连续；cutover 重放只出现一次。
    let (events, end) = fx
        .poll_until(&coordinator, cursor, WatchFilter::all(), 2)
        .await;
    assert_eq!(
        events
            .iter()
            .map(|event| event.commit_ts)
            .collect::<Vec<_>>(),
        vec![split_ts, merge_ts],
        "split/merge must neither drop nor duplicate commit_ts"
    );
    assert_eq!(end.commit_ts, merge_ts, "cursor stays continuous");

    // revision 单调：拓扑变化不让任何 Region 的 current_revision 倒退。
    let left_after = fx.manager.runtime(1).expect("left runtime");
    let right_after = fx.manager.runtime(2).expect("right runtime");
    assert!(
        left_after.mvcc.current_revision() >= rev_left_before,
        "left region revision regressed across split/merge"
    );
    assert!(
        right_after.mvcc.current_revision() >= rev_right_before.max(split_ts),
        "right region revision regressed across split/merge"
    );
    assert_eq!(
        right_after.mvcc.get(b"zz").unwrap().as_deref(),
        Some(&b"merged"[..]),
        "post-merge writes are served by the merged region"
    );
}
