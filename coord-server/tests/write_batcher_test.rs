// WriteBatcher（共享写入批处理器 / Group Commit）测试
//
// 第三轮复核 §4.4：本文件此前是一个**自建 mock**——291 行里零 `use coord*`，
// 9 个用例测的是文件内自己定义的 `MockStorage`/`MockEntry`，与真实实现
// `coord_server::storage::write_batcher::WriteBatcher` **无任何代码路径关联**，
// 却又出现在默认 `cargo test` 的统计里，让"测试数量"虚高（评估方指出它是
// "伪装成真测试"的三个文件之一）。
//
// 现改为**直接测试真实类型**：队列语义、group commit 合并、并发提交不丢不重、
// 停机前最终 flush。真实实现的公共契约见 `coord-server/src/storage/write_batcher.rs`。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use coord_server::storage::write_batcher::{WriteBatchRequest, WriteBatcher, WriteEntry};

fn entries(count: usize, base_index: u64) -> Vec<WriteEntry> {
    (0..count)
        .map(|i| WriteEntry {
            region_id: 1,
            index: base_index + i as u64,
            data: vec![i as u8],
        })
        .collect()
}

/// 记录每次 `write_fn` 收到多少条请求/条目，并对每条请求回执。
#[derive(Default)]
struct Recorder {
    calls: AtomicUsize,
    requests: AtomicUsize,
    entries: AtomicUsize,
    first_indices: Mutex<Vec<u64>>,
}

impl Recorder {
    fn record(&self, batch: Vec<WriteBatchRequest>) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        for req in batch {
            self.requests.fetch_add(1, Ordering::SeqCst);
            self.entries.fetch_add(req.entries.len(), Ordering::SeqCst);
            if let Some(first) = req.entries.first() {
                self.first_indices.lock().unwrap().push(first.index);
            }
            // 真实循环**不**代发回执：`write_fn` 负责通知调用方（见实现注释）
            let _ = req.respond_to.send(Ok(()));
        }
    }
}

// ──── 队列语义 ────

#[test]
fn submit_enqueues_in_fifo_order_and_reports_counts() {
    let batcher = WriteBatcher::new();
    assert_eq!(batcher.pending_count(), 0);
    assert_eq!(batcher.pending_entry_count(), 0);

    let _r1 = batcher.submit(1, entries(3, 100));
    let _r2 = batcher.submit(2, entries(2, 200));

    assert_eq!(batcher.pending_count(), 2);
    assert_eq!(batcher.pending_entry_count(), 5);

    let drained = batcher.drain_pending();
    assert_eq!(drained.len(), 2, "drain 必须取走全部待处理请求");
    assert_eq!(drained[0].region_id, 1, "FIFO 顺序");
    assert_eq!(drained[1].region_id, 2);
    assert_eq!(drained[0].entries.len(), 3);
    assert_eq!(drained[1].entries.first().map(|e| e.index), Some(200));

    assert_eq!(batcher.pending_count(), 0, "drain 后队列必须为空");
    assert_eq!(batcher.pending_entry_count(), 0);
}

#[test]
fn submit_after_drain_starts_a_fresh_batch() {
    let batcher = WriteBatcher::new();
    let _r = batcher.submit(1, entries(1, 1));
    assert_eq!(batcher.drain_pending().len(), 1);

    let _r2 = batcher.submit(1, entries(1, 2));
    let drained = batcher.drain_pending();
    assert_eq!(drained.len(), 1, "新提交不得与已 drain 的批次混淆");
    assert_eq!(drained[0].entries.first().map(|e| e.index), Some(2));
}

#[test]
fn drain_pending_on_empty_queue_returns_nothing() {
    let batcher = WriteBatcher::new();
    assert!(batcher.drain_pending().is_empty());
}

// ──── Group commit 语义 ────

/// 确定性验证"合并写入"：一次 flush 覆盖全部待处理请求，
/// 而不是每条请求各触发一次存储写入。
#[test]
fn run_blocking_group_commits_all_pending_into_one_call() {
    let batcher = WriteBatcher::new();
    for i in 0..5u64 {
        let _r = batcher.submit(1, entries(2, i * 2));
    }

    let rec = Recorder::default();
    batcher
        .run_blocking(
            |batch| {
                rec.record(batch);
                Ok(())
            },
            5,
        )
        .expect("run_blocking must succeed");

    assert_eq!(
        rec.calls.load(Ordering::SeqCst),
        1,
        "group commit：一次 flush 必须合并全部待处理请求"
    );
    assert_eq!(rec.requests.load(Ordering::SeqCst), 5);
    assert_eq!(rec.entries.load(Ordering::SeqCst), 10);
    assert_eq!(batcher.pending_count(), 0);
}

#[test]
fn run_blocking_with_empty_queue_does_not_invoke_write_fn() {
    let batcher = WriteBatcher::new();
    let rec = Recorder::default();
    batcher
        .run_blocking(
            |batch| {
                rec.record(batch);
                Ok(())
            },
            5,
        )
        .unwrap();
    assert_eq!(rec.calls.load(Ordering::SeqCst), 0, "空队列不得触发写入");
}

// ──── 后台循环：不丢、不重、可停机 ────

/// 并发提交 50 条请求：每一条都必须恰好被处理一次且回执送达（不丢、不重）。
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_submits_are_all_delivered_exactly_once() {
    let batcher = Arc::new(WriteBatcher::new());
    let rec: Arc<Recorder> = Arc::new(Recorder::default());

    let rec_for_fn = Arc::clone(&rec);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let handle = batcher.run(
        move |batch| {
            rec_for_fn.record(batch);
            Ok(())
        },
        5,
        shutdown_rx,
    );

    let mut receivers = Vec::new();
    for i in 0..50u64 {
        receivers.push(batcher.submit(1, entries(1, i)));
    }

    for (i, rx) in receivers.into_iter().enumerate() {
        let ack = tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .unwrap_or_else(|_| panic!("request {i} was never acknowledged (write lost)"))
            .expect("sender dropped without responding (write lost)");
        ack.expect("write_fn reported failure");
    }

    assert_eq!(
        rec.requests.load(Ordering::SeqCst),
        50,
        "恰好 50 条请求（不丢不重）"
    );
    assert_eq!(rec.entries.load(Ordering::SeqCst), 50);
    assert_eq!(batcher.pending_count(), 0);

    let _ = shutdown_tx.send(true);
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("batcher loop must stop on shutdown signal")
        .expect("batcher task must not panic");
}

/// 停机时仍在队列里的请求必须被**最终 flush**，不能随循环退出而丢失。
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_flushes_pending_requests() {
    let batcher = Arc::new(WriteBatcher::new());
    let rec: Arc<Recorder> = Arc::new(Recorder::default());

    let rec_for_fn = Arc::clone(&rec);
    // 拉长批间隔，确保这些请求会停留在队列里直到停机信号到来
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let handle = batcher.run(
        move |batch| {
            rec_for_fn.record(batch);
            Ok(())
        },
        60_000,
        shutdown_rx,
    );

    let mut receivers = Vec::new();
    for i in 0..3u64 {
        receivers.push(batcher.submit(1, entries(1, i)));
    }

    let _ = shutdown_tx.send(true);

    for (i, rx) in receivers.into_iter().enumerate() {
        tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .unwrap_or_else(|_| panic!("pending request {i} was lost on shutdown"))
            .expect("pending request sender dropped on shutdown")
            .expect("final flush reported failure");
    }
    assert_eq!(
        rec.requests.load(Ordering::SeqCst),
        3,
        "停机前必须 flush 全部待处理"
    );

    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("batcher loop must exit after shutdown")
        .expect("batcher task must not panic");
}

/// 写入失败时**不得**假装成功：`write_fn` 返回 `Err` 的请求不应收到回执
/// （由上层重试），而成功的请求必须收到回执。
#[tokio::test(flavor = "multi_thread")]
async fn failed_batch_does_not_acknowledge_its_requests() {
    let batcher = Arc::new(WriteBatcher::new());
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let handle = batcher.run(
        |batch: Vec<WriteBatchRequest>| {
            // 模拟存储写失败：不发送任何回执
            drop(batch);
            Err("simulated storage failure".to_string())
        },
        5,
        shutdown_rx,
    );

    let rx = batcher.submit(1, entries(1, 1));
    // 回执不应到达（接收端会收到 RecvError，因为 sender 被丢弃）
    let outcome = tokio::time::timeout(Duration::from_millis(300), rx).await;
    assert!(
        outcome.is_err() || outcome.unwrap().is_err(),
        "写失败的请求绝不能被回执为成功"
    );

    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
}
