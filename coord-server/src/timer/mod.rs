// Timer Wheel — 分层哈希时间轮
//
// 三层层级结构，参考 Kafka / Netty 实现：
// - L0: tick=100ms, 512 槽, 覆盖 0~51.2s
// - L1: tick=51.2s, 512 槽, 覆盖 ~7.3h
// - L2: tick=~7.3h, 512 槽, 覆盖 ~155d
//
// 单线程驱动（独立 Tokio 任务），通过 mpsc channel 与外部交互。
// Leader 独占运行，Follower 不启动时间轮。
//
// **本模块不是租约过期的触发源。** 曾经它对外暴露一个 `expire_tx`/`expire_rx` 通道
// 作为"到期通知"接口（第三方报告第四节 §3.7 把它当成主输出接口），但该通道在**生产
// 路径上没有任何消费者**：`LeaseManager` 持有唯一的 `TimerWheelHandle` 却从不读它的
// receiver，全部 4 个 `expire_receiver()` 调用点都在 `#[cfg(test)]` 内；而租约过期实际
// 由 `LeaseManager::check_expired()`（单调/墙钟到期比较）在 `server/mod.rs` 的 200ms
// 轮询里驱动，经 raft 下发 `LeaseOp::Revoke{delete_keys:true}`。
//
// 后果是：每有一条租约**自然过期**（提前 revoke 走 `cancel`，不发消息），就会往一个
// **无界** mpsc 里追加一条 id，永不消费 —— 约 140 MB/天（100 次过期/s 量级）。
// 本轮取"删掉"而非"接上"：再接一条独立触发器会与墙钟轮询形成**第二条 revoke 路径**
// （重复 propose、双触发竞争），而轮询路径是唯一被测试覆盖、且在时间轮目标死亡时
// 仍然存活的路径。保留双路径的收益（省掉一次 O(N) 扫描）远小于它的风险。
//
// 因此本模块现在的职责只有一个：**维护定时器索引本身**，即 `insert`/`cancel`/
// `reschedule` 与 tick 驱动的索引自清理（到期条目必须从 `id_index`/`slots` 中移除，
// 否则时间轮自身会无界增长）。

use std::collections::HashMap;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::{interval, Instant};

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
///
/// 注意：它**不**对外通知到期事件（见模块头注释——旧的 `expire_tx` 通道在生产
/// 路径上无消费者，本轮已删除）。租约到期由 `LeaseManager::check_expired()` 驱动。
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
}

impl TimerWheel {
    /// 创建新的时间轮实例并启动驱动任务
    ///
    /// 返回 `TimerWheelHandle`，外部通过它操作时间轮。
    pub fn start() -> TimerWheelHandle {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

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
        };

        let handle = TimerWheelHandle { cmd_tx };

        // 启动驱动任务
        //
        // 第三轮 §4.2①：时间轮**必须**受监督——租约的到期时刻登记在它里面，
        // 朴素 `tokio::spawn` 丢弃 handle 后，它 panic 掉没有任何人知道。
        crate::supervisor::spawn_supervised("timer_wheel", async move {
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

    /// 推进一个 tick：驱逐 L0 当前槽位的所有到期条目
    ///
    /// 驱逐 = 从 `id_index` / `slots` 移除。**不发射任何事件**（见模块头注释）：
    /// 租约过期由 `LeaseManager::check_expired()` 的墙钟轮询检测，与这里无关。
    /// 本函数存在的意义是让时间轮**自身**不无界增长。
    fn advance_tick(&mut self) {
        let now = Instant::now();

        // 处理 L0 当前槽位
        let l0_pos = self.current_pos[0];
        let expired: Vec<TimerEntry> = std::mem::take(&mut self.slots[0][l0_pos]);

        for entry in expired {
            self.id_index.remove(&entry.id);
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
        let entries: Vec<TimerEntry> = std::mem::take(&mut self.slots[layer][pos]);

        // 将这些任务重新插入更低层
        for entry in entries {
            self.id_index.remove(&entry.id);

            if entry.deadline > now {
                // 重新插入（会分配到合适的层）
                let remaining = entry.deadline - now;
                let new_id = entry.id;
                self.insert_with_id(new_id, remaining, entry.deadline);
            }
            // deadline <= now：已过期，驱逐即可（`id_index` 已在上面移除）
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
}

impl TimerWheelHandle {
    /// 创建一个新的命令发送器（可传递给其他任务）
    #[allow(dead_code)]
    pub(crate) fn command_sender(&self) -> mpsc::UnboundedSender<Command> {
        self.cmd_tx.clone()
    }
    /// 插入定时任务，返回任务 ID。
    ///
    /// 第三轮 §4.2①：时间轮驱动任务死亡（panic / 通道关闭）时返回 `None`，
    /// **不得**退化为 `0`。
    ///
    /// `0` 从来不是合法 ID（`next_id` 从 1 起），但它在旧实现里被当作"失败值"
    /// 一路传播：`lease/mod.rs` 把 0 存进租约记录，而 `cancel(0)` 是空操作
    /// （`id_index` 中无 0）→ **该租约永不触发到期** → 绑定 Key 静默无界泄漏。
    /// 这是"后台任务静默死亡"最难诊断的形态：没有报错、没有日志、只是能力消失。
    ///
    /// 调用方必须显式处理 `None`（fail-closed：宁可分配失败，也不制造永不过期的租约）。
    pub async fn insert(&self, timeout: Duration) -> Option<u64> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self
            .cmd_tx
            .send(Command::Insert {
                timeout,
                respond_to: tx,
            })
            .is_err()
        {
            tracing::error!("timer wheel is gone: refusing to allocate a timer id");
            return None;
        }
        match rx.await {
            Ok(id) => Some(id),
            Err(_) => {
                tracing::error!(
                    "timer wheel dropped the insert request: refusing to fabricate a timer id"
                );
                None
            }
        }
    }

    /// 取消定时任务。
    ///
    /// 通道关闭（时间轮已死）时返回 `false`——即"**未能**取消"，属 fail-closed
    /// 方向：调用方不得把它误读成"已取消"。
    pub async fn cancel(&self, id: u64) -> bool {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.cmd_tx.send(Command::Cancel { id, respond_to: tx });
        rx.await.unwrap_or(false)
    }

    /// 重新调度（用于 KeepAlive）。
    ///
    /// 通道关闭 / 目标 ID 不存在时返回 `false`（fail-closed：调用方按续期失败处理）。
    pub async fn reschedule(&self, id: u64, new_timeout: Duration) -> bool {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.cmd_tx.send(Command::Reschedule {
            id,
            new_timeout,
            respond_to: tx,
        });
        rx.await.unwrap_or(false)
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

    /// 驱逐判据（可观察）：条目到期并被 tick 驱逐后，`cancel(id)` 必须返回 `false`
    /// （`id_index` 中已无此 id），而在到期前必须返回 `true`。
    ///
    /// 这是删除 `expire_tx` 通道后能保留的**唯一**对外可观察证据：旧测试靠读
    /// 那个通道断言"到期了"，但该通道在生产中无人消费（§3.7），所以那些断言在
    /// 验证一个不存在的接口。
    async fn is_still_registered(handle: &TimerWheelHandle, id: u64) -> bool {
        handle.cancel(id).await
    }

    #[tokio::test]
    async fn test_insert_and_evict() {
        let handle = TimerWheel::start();

        // 插入一个 200ms 后到期的任务
        let id = handle
            .insert(Duration::from_millis(200))
            .await
            .expect("timer wheel alive");
        assert!(id > 0);

        // 尚未到期：仍在索引中（可取消）
        assert!(
            is_still_registered(&handle, id).await,
            "未到期条目必须仍在索引中"
        );

        // 重新插入一个，等它自然到期并被 tick 驱逐
        let id2 = handle
            .insert(Duration::from_millis(150))
            .await
            .expect("timer wheel alive");
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(
            !is_still_registered(&handle, id2).await,
            "到期条目必须已被驱逐出索引（否则时间轮自身无界增长）"
        );

        handle.shutdown();
    }

    #[tokio::test]
    async fn test_cancel_before_expire() {
        let handle = TimerWheel::start();

        let id = handle
            .insert(Duration::from_millis(300))
            .await
            .expect("timer wheel alive");
        assert!(id > 0);

        // 立即取消
        let cancelled = handle.cancel(id).await;
        assert!(cancelled);

        // 取消后不得再次出现在索引中（重复取消返回 false）
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(!handle.cancel(id).await, "已取消的 id 不得重新出现");

        handle.shutdown();
    }

    #[tokio::test]
    async fn test_reschedule() {
        let handle = TimerWheel::start();

        let id = handle
            .insert(Duration::from_millis(500))
            .await
            .expect("timer wheel alive");
        assert!(id > 0);

        // 重新调度到 2s（足够长，避免与下面的立即断言竞争）
        let ok = handle.reschedule(id, Duration::from_secs(2)).await;
        assert!(ok);

        // 重排后仍是同一个 id 且仍在索引中
        assert!(handle.cancel(id).await, "重排必须复用原 id 并保留在索引中");

        handle.shutdown();
    }

    #[tokio::test]
    async fn test_multiple_timers_all_evicted() {
        let handle = TimerWheel::start();

        let id1 = handle
            .insert(Duration::from_millis(100))
            .await
            .expect("alive");
        let id2 = handle
            .insert(Duration::from_millis(200))
            .await
            .expect("alive");
        let id3 = handle
            .insert(Duration::from_millis(150))
            .await
            .expect("alive");

        tokio::time::sleep(Duration::from_millis(500)).await;
        for id in [id1, id2, id3] {
            assert!(
                !handle.cancel(id).await,
                "id {id} 到期后必须已被驱逐（索引不得无界增长）"
            );
        }

        // 驱逐之后时间轮必须仍然可用（指针/索引未被破坏）
        let id4 = handle
            .insert(Duration::from_millis(50))
            .await
            .expect("wheel still usable after eviction");
        assert!(id4 > 0);
        assert!(id4 != id1 && id4 != id2 && id4 != id3, "id 必须单调不复用");

        handle.shutdown();
    }

    #[tokio::test]
    async fn test_cancel_nonexistent() {
        let handle = TimerWheel::start();
        let cancelled = handle.cancel(999).await;
        assert!(!cancelled);
        handle.shutdown();
    }

    #[tokio::test]
    async fn test_reschedule_nonexistent() {
        let handle = TimerWheel::start();
        let ok = handle.reschedule(999, Duration::from_millis(100)).await;
        assert!(!ok);
        handle.shutdown();
    }

    /// §3.7 回归：时间轮**不得**再对外暴露到期通知通道。
    ///
    /// 该通道此前在生产路径上无人消费，每条自然过期的租约往无界 mpsc 追加一条
    /// id（≈140 MB/天）。此测试是"通道已被删除"的编译期/运行期卡口：若有人重新
    /// 引入一个只会写、无人读的到期通道，本测试无法编译——正是我们想要的信号。
    /// 到期检测的唯一位置是 `LeaseManager::check_expired()`。
    #[tokio::test]
    async fn no_unconsumed_expire_channel() {
        let handle = TimerWheel::start();
        let id = handle
            .insert(Duration::from_millis(50))
            .await
            .expect("alive");
        tokio::time::sleep(Duration::from_millis(250)).await;
        // 到期后索引被驱逐即为全部可观察行为；不存在"到期事件"这回事。
        assert!(!handle.cancel(id).await);
        handle.shutdown();
    }

    /// 第三轮 §4.2① 回归：时间轮死亡后 `insert` 必须返回 `None`，
    /// **不得**退化为 `0`（0 不是合法 ID，会让租约永不触发到期）。
    #[tokio::test]
    async fn insert_fails_closed_when_wheel_is_dead() {
        let handle = TimerWheel::start();
        handle.shutdown();
        // 等驱动任务退出并释放 cmd_rx
        tokio::time::sleep(Duration::from_millis(50)).await;

        let id = handle.insert(Duration::from_millis(100)).await;
        assert_eq!(id, None, "时间轮已死时必须返回 None（绝不是 0）");
    }
}
