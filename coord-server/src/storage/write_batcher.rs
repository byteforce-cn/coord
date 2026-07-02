// 共享写入批处理器（WriteBatcher / Group Commit）
//
// 由于 redb 使用单写者模型，多个 Region 的 Raft Log append 必须串行化。
// WriteBatcher 将多个 Region 的写入请求批量收集后一次性提交到共享存储，
// 减少写锁竞争和 fsync 开销。
//
// 设计要点（ADP §5.4）：
// - 定时刷新（默认 5ms 间隔）或达到批量大小阈值时触发
// - 单次写事务处理所有待处理的 Region 写入，减少 fsync 次数
// - 通知机制：每个写入请求携带 oneshot::Sender，批量完成后通知等待者

use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::Notify;

// ============================================================================
// WriteRequest / WriteBatch
// ============================================================================

/// 单条写入请求
#[derive(Debug)]
pub struct WriteEntry {
    /// 目标 Region
    pub region_id: u64,
    /// Raft Log Index
    pub index: u64,
    /// 序列化后的 Raft Entry 数据
    pub data: Vec<u8>,
}

/// 批量写入请求（一个 Region 的多条 Entry）
#[derive(Debug)]
pub struct WriteBatchRequest {
    /// 目标 Region
    pub region_id: u64,
    /// 待写入的 Raft Log Entry 列表
    pub entries: Vec<WriteEntry>,
    /// 完成通知通道
    pub respond_to: tokio::sync::oneshot::Sender<Result<(), String>>,
}

// ============================================================================
// WriteBatcher
// ============================================================================

/// 共享写入批处理器
///
/// 收集多个 Region 的 Raft Log append 请求，批量提交到共享存储。
/// 线程安全（内部使用 Mutex + Notify）。
pub struct WriteBatcher {
    /// 待处理写入队列
    pending: Mutex<VecDeque<WriteBatchRequest>>,
    /// 新请求到达通知
    notify: Arc<Notify>,
}

impl WriteBatcher {
    /// 创建新的 WriteBatcher
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(VecDeque::new()),
            notify: Arc::new(Notify::new()),
        }
    }

    /// 提交一个 Region 的批量写入请求
    ///
    /// 返回一个 oneshot::Receiver，调用者可以通过它等待写入完成。
    pub fn submit(
        &self,
        region_id: u64,
        entries: Vec<WriteEntry>,
    ) -> tokio::sync::oneshot::Receiver<Result<(), String>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let request = WriteBatchRequest {
            region_id,
            entries,
            respond_to: tx,
        };

        {
            let mut pending = self.pending.lock();
            pending.push_back(request);
        }

        // 通知批处理循环有新请求到达
        self.notify.notify_one();

        rx
    }

    /// 收集当前所有待处理请求（用于批量提交）
    pub fn drain_pending(&self) -> Vec<WriteBatchRequest> {
        let mut pending = self.pending.lock();
        let batch: Vec<WriteBatchRequest> = pending.drain(..).collect();
        batch
    }

    /// 获取待处理请求的 Notify 引用
    pub fn notifier(&self) -> Arc<Notify> {
        Arc::clone(&self.notify)
    }

    /// 获取当前待处理请求数量
    pub fn pending_count(&self) -> usize {
        self.pending.lock().len()
    }

    /// 获取当前待处理的总 Entry 数量（所有请求的 entries 之和）
    pub fn pending_entry_count(&self) -> usize {
        self.pending.lock().iter().map(|r| r.entries.len()).sum()
    }
}

impl Default for WriteBatcher {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// 后台批处理循环
// ============================================================================

impl WriteBatcher {
    /// 启动后台批处理循环
    ///
    /// 此方法启动一个 tokio task，持续从 pending 队列中取出写入请求并批量处理。
    ///
    /// # Arguments
    /// * `write_fn` - 写入回调：接收所有待处理的 WriteBatchRequest，执行实际存储写入
    /// * `batch_interval_ms` - 批处理间隔（毫秒），默认 5ms
    /// * `shutdown_rx` - 优雅关闭信号
    pub fn run<F>(
        self: &Arc<Self>,
        write_fn: F,
        batch_interval_ms: u64,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()>
    where
        F: Fn(Vec<WriteBatchRequest>) -> Result<(), String> + Send + Sync + 'static,
    {
        let batcher = Arc::clone(self);
        let write_fn = Arc::new(write_fn);

        tokio::spawn(async move {
            let interval = tokio::time::Duration::from_millis(batch_interval_ms);
            let notifier = batcher.notifier();

            loop {
                // 等待新请求到达或超时
                tokio::select! {
                    _ = notifier.notified() => {}
                    _ = tokio::time::sleep(interval) => {}
                    _ = shutdown_rx.changed() => {
                        // 收到关闭信号，处理最后一波 pending 后退出
                        let final_batch = batcher.drain_pending();
                        if !final_batch.is_empty() {
                            if let Err(e) = write_fn(final_batch) {
                                tracing::error!("WriteBatcher: final flush failed: {}", e);
                            }
                        }
                        tracing::info!("WriteBatcher: shutdown complete");
                        break;
                    }
                }

                // 排出所有待处理请求
                let batch = batcher.drain_pending();
                if batch.is_empty() {
                    continue;
                }

                let batch_size: usize = batch.iter().map(|r| r.entries.len()).sum();
                tracing::trace!(
                    "WriteBatcher: flushing {} requests ({} entries)",
                    batch.len(),
                    batch_size
                );

                // 批量写入
                match write_fn(batch) {
                    Ok(()) => {
                        // 成功：所有 oneshot sender 在 write_fn 内部已通知
                    }
                    Err(e) => {
                        tracing::error!("WriteBatcher: batch write failed: {}", e);
                        // 失败时不通知 sender（由上层重试）
                    }
                }
            }
        })
    }

    /// 同步版本的 run（阻塞当前线程直到 shutdown）
    ///
    /// 用于测试环境或非 tokio 上下文。
    #[doc(hidden)]
    pub fn run_blocking<F>(
        &self,
        write_fn: F,
        batch_interval_ms: u64,
    ) -> Result<(), String>
    where
        F: Fn(Vec<WriteBatchRequest>) -> Result<(), String>,
    {
        // 简化版：等待后排出并写入（单次）
        std::thread::sleep(std::time::Duration::from_millis(batch_interval_ms));
        let batch = self.drain_pending();
        if !batch.is_empty() {
            write_fn(batch)?;
        }
        Ok(())
    }
}

// ============================================================================
// 测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batcher_create() {
        let batcher = WriteBatcher::new();
        assert_eq!(batcher.pending_count(), 0);
        assert_eq!(batcher.pending_entry_count(), 0);
    }

    #[test]
    fn test_batcher_submit() {
        let batcher = WriteBatcher::new();

        let entries = vec![WriteEntry {
            region_id: 1,
            index: 1,
            data: b"hello".to_vec(),
        }];

        let _rx = batcher.submit(1, entries);
        assert_eq!(batcher.pending_count(), 1);
        assert_eq!(batcher.pending_entry_count(), 1);
    }

    #[test]
    fn test_batcher_drain_pending() {
        let batcher = WriteBatcher::new();

        batcher.submit(
            1,
            vec![WriteEntry {
                region_id: 1,
                index: 1,
                data: b"a".to_vec(),
            }],
        );
        batcher.submit(
            2,
            vec![WriteEntry {
                region_id: 2,
                index: 1,
                data: b"b".to_vec(),
            }],
        );

        assert_eq!(batcher.pending_count(), 2);

        let batch = batcher.drain_pending();
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].region_id, 1);
        assert_eq!(batch[1].region_id, 2);

        // drain 后队列为空
        assert_eq!(batcher.pending_count(), 0);
    }

    #[test]
    fn test_batcher_multiple_regions() {
        let batcher = WriteBatcher::new();

        for rid in 0..10u64 {
            batcher.submit(
                rid,
                vec![WriteEntry {
                    region_id: rid,
                    index: 1,
                    data: vec![rid as u8],
                }],
            );
        }

        assert_eq!(batcher.pending_count(), 10);
        assert_eq!(batcher.pending_entry_count(), 10);

        let batch = batcher.drain_pending();
        assert_eq!(batch.len(), 10);

        // 验证每个 Region 的数据正确
        let mut found_regions: Vec<u64> = batch.iter().map(|r| r.region_id).collect();
        found_regions.sort();
        assert_eq!(found_regions, (0..10).collect::<Vec<u64>>());
    }

    #[tokio::test]
    async fn test_batcher_respond_to_sender() {
        let batcher = WriteBatcher::new();

        let rx = batcher.submit(
            1,
            vec![WriteEntry {
                region_id: 1,
                index: 1,
                data: b"test".to_vec(),
            }],
        );

        // 模拟批处理完成
        let batch = batcher.drain_pending();
        for req in batch {
            let _ = req.respond_to.send(Ok(()));
        }

        // 等待通知
        let result = rx.await.unwrap();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_batcher_run_background() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use tokio::sync::watch;

        let batcher = Arc::new(WriteBatcher::new());
        let flush_count = Arc::new(AtomicUsize::new(0));
        let total_entries = Arc::new(AtomicUsize::new(0));

        let fc = Arc::clone(&flush_count);
        let te = Arc::clone(&total_entries);

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // 启动后台批处理循环
        let handle = batcher.run(
            move |batch| {
                fc.fetch_add(1, Ordering::SeqCst);
                let count: usize = batch.iter().map(|r| r.entries.len()).sum();
                te.fetch_add(count, Ordering::SeqCst);
                // 通知等待者
                for req in batch {
                    let _ = req.respond_to.send(Ok(()));
                }
                Ok(())
            },
            5, // 5ms interval
            shutdown_rx,
        );

        // 提交一些写入
        let mut receivers = Vec::new();
        for i in 0..5 {
            let rx = batcher.submit(
                i as u64,
                vec![WriteEntry {
                    region_id: i as u64,
                    index: 1,
                    data: vec![i as u8],
                }],
            );
            receivers.push(rx);
        }

        // 等待所有写入完成
        for rx in receivers {
            let result = rx.await.unwrap();
            assert!(result.is_ok());
        }

        // 发送关闭信号
        let _ = shutdown_tx.send(true);
        handle.await.unwrap();

        // 至少有一次 flush
        assert!(flush_count.load(Ordering::SeqCst) >= 1);
        assert_eq!(total_entries.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn test_batcher_run_empty_queue() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use tokio::sync::watch;

        let batcher = Arc::new(WriteBatcher::new());
        let flush_count = Arc::new(AtomicUsize::new(0));
        let fc = Arc::clone(&flush_count);

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let handle = batcher.run(
            move |_batch| {
                fc.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
            5,
            shutdown_rx,
        );

        // 不提交任何写入，直接关闭
        let _ = shutdown_tx.send(true);
        handle.await.unwrap();

        // 没有需要 flush 的请求
        assert_eq!(flush_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_batcher_run_final_flush_on_shutdown() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use tokio::sync::watch;

        let batcher = Arc::new(WriteBatcher::new());
        let flush_count = Arc::new(AtomicUsize::new(0));
        let fc = Arc::clone(&flush_count);

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let handle = batcher.run(
            move |batch| {
                fc.fetch_add(1, Ordering::SeqCst);
                for req in batch {
                    let _ = req.respond_to.send(Ok(()));
                }
                Ok(())
            },
            5,
            shutdown_rx,
        );

        // 提交写入
        let rx = batcher.submit(
            1,
            vec![WriteEntry {
                region_id: 1,
                index: 1,
                data: b"final".to_vec(),
            }],
        );

        // 立即关闭
        let _ = shutdown_tx.send(true);
        handle.await.unwrap();
        let _ = rx.await;

        // 关闭时应 flush 最后一批
        assert!(flush_count.load(Ordering::SeqCst) >= 1, "should flush on shutdown");
    }
}
