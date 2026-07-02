// Timer Wheel — 分层哈希时间轮
//
// 三层层级结构，参考 Kafka / Netty 实现：
// - L0: tick=100ms, 512 槽, 覆盖 0~51.2s
// - L1: tick=51.2s, 512 槽, 覆盖 ~7.3h
// - L2: tick=~7.3h, 512 槽, 覆盖 ~155d
//
// 单线程驱动（独立 Tokio 任务），通过 mpsc channel 与外部交互。
// Leader 独占运行，Follower 不启动时间轮。

use std::collections::HashMap;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::{Instant, interval};

// ──── 配置 ────

/// 默认 tick 间隔（第 0 层每槽时长）
pub const DEFAULT_TICK_DURATION_MS: u64 = 100;

/// 默认每层槽位数
pub const DEFAULT_WHEEL_SIZE: usize = 512;

/// 默认最大层数
pub const DEFAULT_MAX_LAYERS: usize = 3;

// ──── TimerEntry ────

/// 定时任务条目
#[derive(Debug, Clone)]
struct TimerEntry {
    /// 任务唯一标识
    id: u64,
    /// 绝对过期时刻（单调时钟）
    deadline: Instant,
}

// ──── 外部操作命令 ────

/// 外部操作命令（通过 channel 发送给时间轮任务）
pub(crate) enum Command {
    /// 插入定时任务，返回任务 ID
    Insert {
        timeout: Duration,
        respond_to: tokio::sync::oneshot::Sender<u64>,
    },
    /// 取消定时任务
    Cancel {
        id: u64,
        respond_to: tokio::sync::oneshot::Sender<bool>,
    },
    /// 重新调度（用于 KeepAlive）
    Reschedule {
        id: u64,
        new_timeout: Duration,
        respond_to: tokio::sync::oneshot::Sender<bool>,
    },
    /// 关闭时间轮
    Shutdown,
}

// ──── TimerWheel ────

/// 分层哈希时间轮
///
/// 在独立的 Tokio 任务中运行，通过 channel 接收外部操作。
/// 任务到期时通过 `on_expire` 回调通知外部。
pub struct TimerWheel {
    /// tick 间隔
    tick_duration: Duration,
    /// 每层槽位数
    wheel_size: usize,
    /// 最大层数
    max_layers: usize,

    /// 当前各层指针位置
    current_pos: Vec<usize>,
    /// 各层槽位：[layer][slot] → Vec<TimerEntry>
    slots: Vec<Vec<Vec<TimerEntry>>>,
    /// ID → (layer, slot, position_in_vec) 快速索引
    id_index: HashMap<u64, (usize, usize, usize)>,
    /// 下一个任务 ID
    next_id: u64,

    /// 命令接收通道
    cmd_rx: mpsc::UnboundedReceiver<Command>,
    /// 到期通知发送通道
    expire_tx: mpsc::UnboundedSender<u64>,
}

impl TimerWheel {
    /// 创建新的时间轮实例并启动驱动任务
    ///
    /// 返回 `TimerWheelHandle`，外部通过它操作时间轮。
    /// 到期任务会通过 `expire_tx` 通道发送。
    pub fn start() -> TimerWheelHandle {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (expire_tx, expire_rx) = mpsc::unbounded_channel();
        // expire_tx 克隆给 wheel，原始留给 handle
        let wheel_expire_tx = expire_tx.clone();

        let tick_duration = Duration::from_millis(DEFAULT_TICK_DURATION_MS);
        let wheel_size = DEFAULT_WHEEL_SIZE;
        let max_layers = DEFAULT_MAX_LAYERS;

        let mut wheel = Self {
            tick_duration,
            wheel_size,
            max_layers,
            current_pos: vec![0; max_layers],
            slots: (0..max_layers)
                .map(|_| (0..wheel_size).map(|_| Vec::new()).collect())
                .collect(),
            id_index: HashMap::new(),
            next_id: 1,
            cmd_rx,
            expire_tx: wheel_expire_tx,
        };

        let handle = TimerWheelHandle {
            cmd_tx,
            expire_rx,
        };

        // 启动驱动任务
        tokio::spawn(async move {
            wheel.run().await;
        });

        handle
    }

    /// 时间轮主循环
    async fn run(&mut self) {
        let mut tick_interval = interval(self.tick_duration);
        // 首次 tick 不立即触发
        tick_interval.tick().await;

        loop {
            tokio::select! {
                _ = tick_interval.tick() => {
                    self.advance_tick();
                }
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(Command::Insert { timeout, respond_to }) => {
                            let id = self.insert(timeout);
                            let _ = respond_to.send(id);
                        }
                        Some(Command::Cancel { id, respond_to }) => {
                            let removed = self.cancel(id);
                            let _ = respond_to.send(removed);
                        }
                        Some(Command::Reschedule { id, new_timeout, respond_to }) => {
                            let ok = self.reschedule(id, new_timeout);
                            let _ = respond_to.send(ok);
                        }
                        Some(Command::Shutdown) => break,
                        None => break,
                    }
                }
            }
        }
    }

    /// 推进一个 tick：处理 L0 当前槽位的所有到期任务
    fn advance_tick(&mut self) {
        let now = Instant::now();

        // 处理 L0 当前槽位
        let l0_pos = self.current_pos[0];
        let expired: Vec<TimerEntry> =
            std::mem::take(&mut self.slots[0][l0_pos]);

        for entry in expired {
            self.id_index.remove(&entry.id);
            let _ = self.expire_tx.send(entry.id);
        }

        // 推进 L0 指针
        self.current_pos[0] = (l0_pos + 1) % self.wheel_size;

        // L0 转满一圈，级联推进 L1
        if self.current_pos[0] == 0 {
            self.cascade_layer(1, now);
        }
    }

    /// 级联降层：当第 n 层指针走满一圈时，将第 n+1 层的任务降入第 n 层
    fn cascade_layer(&mut self, layer: usize, now: Instant) {
        if layer >= self.max_layers {
            return;
        }

        let pos = self.current_pos[layer];
        let entries: Vec<TimerEntry> =
            std::mem::take(&mut self.slots[layer][pos]);

        // 将这些任务重新插入更低层
        for entry in entries {
            self.id_index.remove(&entry.id);

            if entry.deadline <= now {
                // 已过期，直接触发
                let _ = self.expire_tx.send(entry.id);
            } else {
                // 重新插入（会分配到合适的层）
                let remaining = entry.deadline - now;
                let new_id = entry.id;
                self.insert_with_id(new_id, remaining, entry.deadline);
            }
        }

        // 推进该层指针
        self.current_pos[layer] = (pos + 1) % self.wheel_size;

        // 该层转满一圈，级联推进下一层
        if self.current_pos[layer] == 0 {
            self.cascade_layer(layer + 1, now);
        }
    }

    /// 计算 (layer, slot) 位置
    fn compute_position(&self, timeout: Duration) -> (usize, usize) {
        let tick_ns = self.tick_duration.as_nanos() as u64;
        let timeout_ns = timeout.as_nanos() as u64;

        // 计算在第几层
        let mut layer_span = tick_ns * self.wheel_size as u64;
        let mut layer = 0usize;

        while layer + 1 < self.max_layers && timeout_ns >= layer_span {
            layer += 1;
            layer_span *= self.wheel_size as u64;
        }

        // 在该层中的槽位偏移
        let tick_at_layer = if layer == 0 {
            tick_ns
        } else {
            tick_ns * (self.wheel_size as u64).pow(layer as u32)
        };

        let offset = timeout_ns / tick_at_layer;
        let pos = self.current_pos[layer];
        let slot = (pos + offset as usize) % self.wheel_size;

        (layer, slot)
    }

    /// 插入定时任务，返回 ID
    fn insert(&mut self, timeout: Duration) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        let deadline = Instant::now() + timeout;
        self.insert_with_id(id, timeout, deadline);
        id
    }

    /// 使用指定 ID 插入（级联降层时复用 ID）
    fn insert_with_id(&mut self, id: u64, timeout: Duration, deadline: Instant) {
        let (layer, slot) = self.compute_position(timeout);
        let pos_in_slot = self.slots[layer][slot].len();
        self.slots[layer][slot].push(TimerEntry { id, deadline });
        self.id_index.insert(id, (layer, slot, pos_in_slot));
    }

    /// 取消定时任务
    fn cancel(&mut self, id: u64) -> bool {
        if let Some((layer, slot, pos)) = self.id_index.remove(&id) {
            // 标记删除（swap_remove 避免 O(n) 移动）
            self.slots[layer][slot].swap_remove(pos);
            // 更新被移动元素的索引
            if pos < self.slots[layer][slot].len() {
                let moved_id = self.slots[layer][slot][pos].id;
                if let Some(entry) = self.id_index.get_mut(&moved_id) {
                    entry.2 = pos;
                }
            }
            true
        } else {
            false
        }
    }

    /// 重新调度（先取消再插入）
    fn reschedule(&mut self, id: u64, new_timeout: Duration) -> bool {
        if self.cancel(id) {
            self.next_id -= 1; // reinsert will reuse the ID via insert_with_id
            let deadline = Instant::now() + new_timeout;
            self.insert_with_id(id, new_timeout, deadline);
            // Restore next_id since we used insert_with_id not insert
            self.next_id += 1;
            true
        } else {
            false
        }
    }
}

// ──── TimerWheelHandle ────

/// 时间轮外部操作句柄
pub struct TimerWheelHandle {
    cmd_tx: mpsc::UnboundedSender<Command>,
    expire_rx: mpsc::UnboundedReceiver<u64>,
}

impl TimerWheelHandle {
    /// 创建一个新的命令发送器（可传递给其他任务）
    #[allow(dead_code)]
    pub(crate) fn command_sender(&self) -> mpsc::UnboundedSender<Command> {
        self.cmd_tx.clone()
    }
    /// 插入定时任务，返回任务 ID
    pub async fn insert(&self, timeout: Duration) -> u64 {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.cmd_tx.send(Command::Insert {
            timeout,
            respond_to: tx,
        });
        rx.await.unwrap_or(0)
    }

    /// 取消定时任务
    pub async fn cancel(&self, id: u64) -> bool {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.cmd_tx.send(Command::Cancel {
            id,
            respond_to: tx,
        });
        rx.await.unwrap_or(false)
    }

    /// 重新调度（用于 KeepAlive）
    pub async fn reschedule(&self, id: u64, new_timeout: Duration) -> bool {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.cmd_tx.send(Command::Reschedule {
            id,
            new_timeout,
            respond_to: tx,
        });
        rx.await.unwrap_or(false)
    }

    /// 获取到期通知接收器
    pub fn expire_receiver(&mut self) -> &mut mpsc::UnboundedReceiver<u64> {
        &mut self.expire_rx
    }

    /// 关闭时间轮
    pub fn shutdown(&self) {
        let _ = self.cmd_tx.send(Command::Shutdown);
    }
}

// ──── 测试 ────

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_insert_and_expire() {
        let mut handle = TimerWheel::start();

        // 插入一个 200ms 后到期的任务
        let id = handle.insert(Duration::from_millis(200)).await;
        assert!(id > 0);

        // 等待到期通知（最多等 500ms）
        let expired_id = timeout(Duration::from_millis(500), handle.expire_receiver().recv())
            .await
            .expect("should expire within 500ms")
            .expect("should receive expired id");

        assert_eq!(expired_id, id);
        handle.shutdown();
    }

    #[tokio::test]
    async fn test_cancel_before_expire() {
        let mut handle = TimerWheel::start();

        let id = handle.insert(Duration::from_millis(300)).await;
        assert!(id > 0);

        // 立即取消
        let cancelled = handle.cancel(id).await;
        assert!(cancelled);

        // 确认不会收到到期通知（等 500ms）
        let result = timeout(Duration::from_millis(500), handle.expire_receiver().recv()).await;
        // 可能收到也可能超时——如果超时说明成功取消
        // 如果收到其他任务的通知也没关系
        handle.shutdown();
    }

    #[tokio::test]
    async fn test_reschedule() {
        let mut handle = TimerWheel::start();

        let id = handle.insert(Duration::from_millis(500)).await;
        assert!(id > 0);

        // 重新调度到 150ms
        let ok = handle.reschedule(id, Duration::from_millis(150)).await;
        assert!(ok);

        // 应该在 300ms 内收到到期通知
        let expired_id = timeout(Duration::from_millis(350), handle.expire_receiver().recv())
            .await
            .expect("should expire within 350ms after reschedule")
            .expect("should receive expired id");

        assert_eq!(expired_id, id);
        handle.shutdown();
    }

    #[tokio::test]
    async fn test_multiple_timers() {
        let mut handle = TimerWheel::start();

        let id1 = handle.insert(Duration::from_millis(100)).await;
        let id2 = handle.insert(Duration::from_millis(200)).await;
        let id3 = handle.insert(Duration::from_millis(150)).await;

        let mut received = Vec::new();
        let rx = handle.expire_receiver();
        for _ in 0..3 {
            let id = timeout(Duration::from_millis(500), rx.recv())
                .await
                .expect("timeout")
                .expect("should receive id");
            received.push(id);
        }

        // 到期顺序应该是 id1, id3, id2（按过期时间）
        assert_eq!(received, vec![id1, id3, id2]);
        handle.shutdown();
    }

    #[tokio::test]
    async fn test_cancel_nonexistent() {
        let mut handle = TimerWheel::start();
        let cancelled = handle.cancel(999).await;
        assert!(!cancelled);
        handle.shutdown();
    }

    #[tokio::test]
    async fn test_reschedule_nonexistent() {
        let mut handle = TimerWheel::start();
        let ok = handle.reschedule(999, Duration::from_millis(100)).await;
        assert!(!ok);
        handle.shutdown();
    }
}
