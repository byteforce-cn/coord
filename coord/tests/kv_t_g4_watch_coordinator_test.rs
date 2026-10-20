//! KV-T G4 Watch Coordinator 端到端验收：
//!
//! - 写入 → 决策 apply → outbox 搬入 shard 组 → Watch 按 `commit_ts` 全局有序读出；
//! - 同一事务跨两个数据 Region 的分片合并为一个原子事件；
//! - cursor 在读取端重建后无丢失、无重复；
//! - key/range 过滤不改变 cursor 推进；
//! - 保留窗口到点后整 shard 回收，旧起点返回 `HISTORY_UNAVAILABLE`。
//!
//! v2 Watch RPC 见 `kv_t_g4_watch_rpc_test`（同一夹具模块）。

#[path = "support/kv_t_watch_node.rs"]
mod watch_node;

use std::sync::Arc;

use coord_server::kvt::watch::{
    WatchCoordinator, WatchCursor, WatchError, WatchFilter, WatchStart,
};
use watch_node::WatchNode;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_watch_global_order_by_commit_ts() {
    let fx = WatchNode::start().await;
    fx.open_first_shard().await;
    let ts1 = fx.commit_pairs(&[(b"aa", b"v1")]).await;
    let ts2 = fx.commit_pairs(&[(b"bb", b"v2")]).await;
    assert!(ts2 > ts1);

    let coordinator = WatchCoordinator::new(Arc::clone(&fx.shards));
    let cursor = coordinator
        .start(WatchStart {
            start_revision: ts1,
            start_cursor: None,
        })
        .await
        .unwrap();
    let (events, cursor) = fx
        .poll_until(&coordinator, cursor, WatchFilter::all(), 2)
        .await;
    let timestamps: Vec<u64> = events.iter().map(|event| event.commit_ts).collect();
    assert_eq!(timestamps, vec![ts1, ts2], "strict global order");
    assert_eq!(cursor.commit_ts, ts2);
    let table = fx.shards.table().unwrap();
    assert!(
        table.shards.len() >= 2 && table.shards[0].sealed_at_ms > 0,
        "the events must span a sealed shard boundary: {table:?}"
    );

    let (again, _) = fx
        .poll_until(&coordinator, cursor, WatchFilter::all(), 0)
        .await;
    assert!(
        again.is_empty(),
        "cursor at the watermark must not repeat events"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_watch_atomic_multi_key_txn() {
    let fx = WatchNode::start().await;
    fx.open_first_shard().await;
    let ts = fx.commit_pairs(&[(b"aa", b"v1"), (b"zz", b"v2")]).await;

    let coordinator = WatchCoordinator::new(Arc::clone(&fx.shards));
    let start = WatchStart {
        start_revision: ts,
        start_cursor: None,
    };
    let cursor = coordinator.start(start.clone()).await.unwrap();
    let (events, _) = fx
        .poll_until(&coordinator, cursor, WatchFilter::all(), 1)
        .await;
    assert_eq!(events.len(), 1, "one txn = one event");
    assert_eq!(events[0].commit_ts, ts);
    let mut keys: Vec<Vec<u8>> = events[0]
        .changes
        .iter()
        .map(|change| change.key.clone())
        .collect();
    keys.sort();
    assert_eq!(keys, vec![b"aa".to_vec(), b"zz".to_vec()]);

    let only_aa = WatchFilter {
        key: b"aa".to_vec(),
        ..WatchFilter::all()
    };
    let cursor = coordinator.start(start).await.unwrap();
    let (filtered, _) = fx.poll_until(&coordinator, cursor, only_aa, 1).await;
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].changes.len(), 1);
    assert_eq!(filtered[0].changes[0].key, b"aa");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_watch_cursor_resumes_without_gap_or_duplicate() {
    let fx = WatchNode::start().await;
    fx.open_first_shard().await;
    let ts1 = fx.commit_pairs(&[(b"aa", b"v1")]).await;
    let ts2 = fx.commit_pairs(&[(b"bb", b"v2")]).await;

    let reader = WatchCoordinator::new(Arc::clone(&fx.shards)).with_limits(1, 1024 * 1024);
    let cursor = reader
        .start(WatchStart {
            start_revision: ts1,
            start_cursor: None,
        })
        .await
        .unwrap();
    let first = reader.poll(cursor, &WatchFilter::all()).await.unwrap();
    assert_eq!(
        first
            .events
            .iter()
            .map(|event| event.commit_ts)
            .collect::<Vec<_>>(),
        vec![ts1]
    );
    assert!(first.has_more, "the batch cap stops before the next event");

    // 新的协调器 = 读取端重建；cursor 只依赖持久化的 shard 日志。
    let resumed = WatchCoordinator::new(Arc::clone(&fx.shards)).with_limits(1, 1024 * 1024);
    let (rest, _) = fx
        .poll_until(&resumed, first.cursor, WatchFilter::all(), 1)
        .await;
    assert_eq!(
        rest.iter().map(|event| event.commit_ts).collect::<Vec<_>>(),
        vec![ts2],
        "no duplicate of the first event and no skipped commit_ts"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_watch_history_unavailable() {
    let fx = WatchNode::start().await;
    fx.open_first_shard().await;
    let first_ts = fx.commit_pairs(&[(b"aa", b"v1")]).await;
    // 第二个事务把 watermark 推过 shard 1 的 end_ts，使其 seal；它自己落进 shard 2。
    let second_ts = fx.commit_pairs(&[(b"bb", b"v2")]).await;
    assert!(second_ts > first_ts);
    fx.wait_until_sealed(1).await;

    let table = fx.shards.table().unwrap();
    let gc_before = table.shards[0].end_ts;
    assert_eq!(
        gc_before, second_ts,
        "shard 2 starts at the first retained ts"
    );
    let sealed_at = table.shards[0].sealed_at_ms;
    assert!(sealed_at != 0);

    // 窗口最后 1ms：不回收，历史仍可读。
    fx.advance_clock_ms(59_999);
    fx.shards.tick().await.unwrap();
    assert_eq!(fx.shards.table().unwrap().gc_before_ts, 0);
    let reader = WatchCoordinator::new(Arc::clone(&fx.shards));
    let cursor = reader
        .start(WatchStart {
            start_revision: 1,
            start_cursor: None,
        })
        .await
        .expect("history is still inside the window");
    let (events, cursor) = fx.poll_until(&reader, cursor, WatchFilter::all(), 1).await;
    assert_eq!(events[0].commit_ts, first_ts);
    let batch = reader.poll(cursor, &WatchFilter::all()).await.unwrap();
    assert_eq!(
        batch.oldest_available,
        WatchCursor::beginning(),
        "nothing has been reclaimed yet"
    );

    // 窗口到点：回收 shard 1；seal 状态保留，本地组与数据目录删除。
    fx.advance_clock_ms(1);
    fx.wait_until_gc_before(gc_before).await;
    let table = fx.shards.table().unwrap();
    assert_eq!(table.gc_before_ts, gc_before);
    assert_ne!(
        table.shards[0].sealed_at_ms, 0,
        "gc must not disturb the seal state"
    );
    assert!(fx.shards.runtime(1).is_none(), "the shard group is gone");

    // 旧起点 → HISTORY_UNAVAILABLE，且带上最老可用位点。
    let err = reader
        .start(WatchStart {
            start_revision: 1,
            start_cursor: None,
        })
        .await
        .unwrap_err();
    let oldest = match err {
        WatchError::HistoryUnavailable {
            requested,
            oldest_available,
        } => {
            assert_eq!(requested, 1);
            oldest_available
        }
        other => panic!("unexpected error: {other}"),
    };
    assert!(oldest.commit_ts + 1 >= gc_before);

    // 最老可用 cursor 仍可续订到第一个保留事件。
    let resumed = reader
        .start(WatchStart {
            start_revision: 0,
            start_cursor: Some(oldest),
        })
        .await
        .expect("the oldest available cursor must still resume");
    let (events, _) = fx.poll_until(&reader, resumed, WatchFilter::all(), 1).await;
    assert_eq!(
        events[0].commit_ts, second_ts,
        "the first retained event is exactly the shard boundary"
    );

    // poll 跨入 shard 2 后返回的 cursor 是 `(2, 0)`（该 shard 起点之前）：
    // 已追平的订阅不得被当成「从 ts 1 开始」而误报 HISTORY_UNAVAILABLE。
    let at_shard_start = reader
        .start(WatchStart {
            start_revision: 0,
            start_cursor: Some(WatchCursor {
                shard_id: 2,
                commit_ts: 0,
            }),
        })
        .await
        .expect("a cursor parked at shard 2's start is still inside the window");
    let batch = reader
        .poll(at_shard_start, &WatchFilter::all())
        .await
        .unwrap();
    assert_eq!(batch.events[0].commit_ts, second_ts);

    // 重启读取端：状态来自 region 0 表，不会复活已回收 shard。
    let restarted = WatchCoordinator::new(Arc::clone(&fx.shards));
    assert!(matches!(
        restarted
            .start(WatchStart {
                start_revision: 1,
                start_cursor: None,
            })
            .await,
        Err(WatchError::HistoryUnavailable { .. })
    ));
    fx.shards.tick().await.unwrap();
    assert!(
        fx.shards.runtime(1).is_none(),
        "tick must not respawn a reclaimed shard"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_gc_keeps_unsealed_shard_and_its_history() {
    let fx = WatchNode::start().await;
    fx.open_first_shard().await;
    let first_ts = fx.commit_pairs(&[(b"aa", b"v1")]).await;

    // shard 1 已定界但 watermark 还没越过 end_ts（没有后续事务）；窗口再久也不回收。
    fx.advance_clock_ms(120_000);
    fx.shards.tick().await.unwrap();
    let table = fx.shards.table().unwrap();
    assert_eq!(table.gc_before_ts, 0);
    assert!(fx.shards.runtime(1).is_some());
    let reader = WatchCoordinator::new(Arc::clone(&fx.shards));
    let cursor = reader
        .start(WatchStart {
            start_revision: 1,
            start_cursor: None,
        })
        .await
        .expect("an unsealed shard keeps its history");
    let (events, cursor) = fx.poll_until(&reader, cursor, WatchFilter::all(), 1).await;
    assert_eq!(events[0].commit_ts, first_ts);
    let batch = reader.poll(cursor, &WatchFilter::all()).await.unwrap();
    assert_eq!(
        batch.oldest_available,
        WatchCursor::beginning(),
        "nothing has been reclaimed yet"
    );

    // 后续事务让 watermark 越过 end_ts：seal 后窗口到点才回收。
    let second_ts = fx.commit_pairs(&[(b"bb", b"v2")]).await;
    fx.wait_until_sealed(1).await;
    fx.advance_clock_ms(60_000);
    fx.wait_until_gc_before(second_ts).await;
    let table = fx.shards.table().unwrap();
    assert_eq!(table.gc_before_ts, second_ts);
    assert!(fx.shards.runtime(1).is_none());

    let err = reader
        .start(WatchStart {
            start_revision: 1,
            start_cursor: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(err, WatchError::HistoryUnavailable { .. }));
    // 最老保留事件仍在 shard 2：从边界 revision 起读能看到它。
    let oldest = WatchCoordinator::new(Arc::clone(&fx.shards))
        .start(WatchStart {
            start_revision: second_ts,
            start_cursor: None,
        })
        .await
        .unwrap();
    let batch = reader.poll(oldest, &WatchFilter::all()).await.unwrap();
    assert_eq!(
        batch
            .events
            .iter()
            .map(|event| event.commit_ts)
            .collect::<Vec<_>>(),
        vec![second_ts],
        "the boundary event itself is retained"
    );
}
