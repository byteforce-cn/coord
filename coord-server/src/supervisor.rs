//! 后台任务监督原语（第三轮复核 §4.2①）。
//!
//! 问题：仓库 `src/` 下 82 处 `tokio::spawn`，绝大多数**丢弃 `JoinHandle`**，
//! 全仓无 supervisor、无 restart、无 `JoinError` 检查、无 health 信号
//! （`catch_unwind` 用量为 0）。后台循环一旦 panic 或意外 return，对应能力
//! **静默永久失效**，且最难诊断——没有报错、没有日志、只是"某个功能不再发生"：
//!
//! - timer wheel 死亡 → `TimerWheelHandle::insert` 失败 → 租约永不触发到期
//!   （现已 fail-closed 成显式错误，见 `timer/mod.rs`）；
//! - 快照调度死亡 → 再无自动快照，直到磁盘写满才发现；
//! - object GC 死亡 → 孤儿 chunk 只增不减；
//! - write batcher 死亡 → 所有 `submit()` 永久挂起。
//!
//! 本模块提供最小可用的监督原语：
//!
//! - [`spawn_supervised`]：spawn 任务；任务**意外结束**（正常 return / panic /
//!   cancel）时输出 **ERROR** 级日志，并登记到进程级"已死任务"清单；
//! - [`dead_tasks`] / [`has_dead_tasks`]：供 `/health?verbose=true` 与运维查询
//!   "哪些后台能力已经死了"。
//!
//! # 为什么**不**自动重启
//!
//! 这些循环大多持有不可重建的本地状态（PD 调度令牌、写入批游标、快照去重时间戳）。
//! 盲目重启可能造成"双跑"（旧任务其实只是卡住而非死亡）或状态错乱。本轮的取舍是
//! 把**静默死亡变成显式可观测死亡**：ERROR 日志可被现有告警规则捕获，`dead_tasks()`
//! 可在健康检查里暴露。重启/降级策略应由上层按各自语义决定。

use std::future::Future;
use std::sync::Mutex;

/// 已意外结束的后台任务名（进程级；只增不减）。
static DEAD_TASKS: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());

/// spawn 一个**受监督**的后台任务。
///
/// 与裸 `tokio::spawn` 的区别：任务结束时一定留下 ERROR 日志与可查询记录，
/// 而不是无声无息地消失。
///
/// 返回的 `JoinHandle` 是**监督任务**的句柄（不是被监督任务本身）；
/// 用它 `abort()` 只会中止监督，不会中止被监督任务——需要主动停止时请用任务
/// 自身的 shutdown 通道（各循环均已支持）。
pub fn spawn_supervised<F>(name: &'static str, fut: F) -> tokio::task::JoinHandle<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let inner = tokio::spawn(fut);
    tokio::spawn(async move {
        match inner.await {
            Ok(()) => tracing::error!(
                task = name,
                "background task EXITED: its capability is now DEAD (no auto-restart)"
            ),
            Err(e) if e.is_panic() => tracing::error!(
                task = name,
                "background task PANICKED: its capability is now DEAD (no auto-restart): {e}"
            ),
            Err(e) => tracing::error!(
                task = name,
                "background task CANCELLED: its capability is now DEAD (no auto-restart): {e}"
            ),
        }
        DEAD_TASKS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(name);
    })
}

/// 与 [`spawn_supervised`] 相同，但该任务**预期内**会随 `shutdown` 信号退出。
///
/// `shutdown` 变为 `true` 之后任务才结束 → 视为优雅停机，**不**记录为死亡
/// （否则每次正常停机都会污染死亡清单，把信号变成噪声）。
/// 任务**先于**关闭信号结束 → 仍是意外，照常告警并登记。
pub fn spawn_supervised_with_shutdown<F>(
    name: &'static str,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    fut: F,
) -> tokio::task::JoinHandle<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let inner = tokio::spawn(fut);
    tokio::spawn(async move {
        tokio::select! {
            res = inner => {
                match res {
                    Ok(()) => tracing::error!(
                        task = name,
                        "background task EXITED before shutdown: its capability is now DEAD"
                    ),
                    Err(e) if e.is_panic() => tracing::error!(
                        task = name,
                        "background task PANICKED: its capability is now DEAD: {e}"
                    ),
                    Err(e) => tracing::error!(
                        task = name,
                        "background task CANCELLED: its capability is now DEAD: {e}"
                    ),
                }
                DEAD_TASKS
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push(name);
            }
            _ = shutdown.wait_for(|v| *v) => {
                tracing::info!(
                    task = name,
                    "background task stopped on shutdown signal (graceful)"
                );
            }
        }
    })
}

/// 已意外结束的后台任务名列表。
pub fn dead_tasks() -> Vec<&'static str> {
    DEAD_TASKS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

/// 是否存在已死亡的后台任务（供 readiness 检查使用）。
pub fn has_dead_tasks() -> bool {
    !DEAD_TASKS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supervised_task_records_exit() {
        // 该测试不依赖全局清单的初始状态（其它测试可能已登记过）
        let before = dead_tasks().len();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let handle = spawn_supervised("unit-test-task", async {});
            // 等监督任务登记完成
            for _ in 0..100 {
                if dead_tasks().len() > before {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            handle.abort();
        });
        let dead = dead_tasks();
        assert!(
            dead.contains(&"unit-test-task"),
            "exited task must be recorded, got {dead:?}"
        );
        assert!(has_dead_tasks());
    }
}
