//! SafeTs fence 推进器（ADR-0015 D3/D12.3，#142）。
//!
//! safe_ts = 各数据 Region 已 apply 水位的**最小值**。只靠数据写入推进会让
//! 空闲 Region 永久钉住 safe_ts，因此需要一个与数据无关的推进源：
//!
//! 1. region 0 leader 周期性把 **TSO 分配高水位**（下一未发放值）作为 fence
//!    经 region 0 raft 发布（`Command::PublishSafeTs`，节点收敛）；
//! 2. 每个节点对本地为 leader 的数据 Region 提案 `Command::AdvanceSafeTs{fence}`，
//!    把 `META_CURRENT_REVISION` 抬到 fence（不写数据/changelog）。
//!
//! fence 一旦被所有 Region apply，任何 `commit_ts <= fence` 的新写入都会被
//! Region 存储层拒绝，因此 `safe_ts` 达到 fence 后该时间点的历史不再变化。
//! CommitDecision 的可见性（#148）：fence 发布与 sequencer 提案共用一把锁，
//! 发布瞬间没有 in-flight 决策；phase 2 再把每个 Region 的目标水位封顶到
//! 「下一条未应用决策 - 1」，避免把 current_revision 抬过尚未 apply 的决策
//! （那会让该决策随后被 `CommitTs` 策略拒绝）。
//!
//! 只在 Region leader 上提案：follower 的 current revision 由 raft apply 收敛；
//! 无 leader 的 Region 会短暂钉住 safe_ts（保守，不静默跳过）。

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::raft::type_config::{Command, Response};
use crate::raft::WatchReceiver;
use crate::server::CoordNode;

/// 默认 fence 推进周期。
///
/// 1s 让默认 100k revision 的 compaction retention 覆盖约 27.8h（> Watch 24h
/// 默认）；真正的 time-based retention 归 #157。测试直接调用 [`advance_once`]，
/// 不依赖这个周期。
pub const SAFE_TS_ADVANCE_INTERVAL: Duration = Duration::from_secs(1);

/// 执行一轮：发布 fence（若是 region 0 leader）+ fence 本节点领导的数据 Region。
///
/// 返回本轮实际抬升水位的数据 Region 数。单个 Region 失败不阻止其他 Region；
/// 全部尝试完后返回第一个错误（调用方记录 warning 后下轮重试）。
pub async fn advance_once(node: &CoordNode) -> Result<usize, String> {
    let Some(manager) = node.region_manager.as_ref() else {
        // legacy 单 Region：safe_ts 就是 storage.current_revision，无需 fence。
        return Ok(0);
    };

    // Phase 1：region 0 leader 发布 TSO 分配高水位作为 fence。
    // 高水位是「下一未发放值」：fence 之后 GetTs 只会发放更大的值。
    if let Some(raft) = node.raft.as_ref() {
        if raft.current_leader().await == Some(node.node_id) {
            // 锁必须覆盖「读高水位 → 提案」：sequencer 在锁内取 TSO，因此
            // fence 发布时不可能有已分配但未落盘的 commit_ts。
            let _fence_gate = match node.commit_sequencer() {
                Some(sequencer) => Some(sequencer.lock_fence_publication().await),
                None => None,
            };
            let high_water = node
                .storage
                .tso_high_water()
                .map_err(|e| format!("read TSO high-water: {e}"))?;
            let published = node
                .storage
                .safe_ts_fence()
                .map_err(|e| format!("read safe-ts fence: {e}"))?;
            if high_water > published {
                let resp = raft
                    .client_write(Command::PublishSafeTs {
                        fence_ts: high_water,
                    })
                    .await
                    .map_err(|e| format!("publish safe-ts fence {high_water}: {e}"))?;
                match resp.response() {
                    Response::SafeTsPublished { fence_ts } if *fence_ts >= high_water => {}
                    other => {
                        return Err(format!(
                            "unexpected PublishSafeTs response for {high_water}: {other:?}"
                        ))
                    }
                }
            }
        }
    }

    // Phase 2：把已发布的 fence 应用到本节点领导的每个数据 Region。
    let fence = node
        .storage
        .safe_ts_fence()
        .map_err(|e| format!("read safe-ts fence: {e}"))?;
    if fence == 0 {
        return Ok(0);
    }

    let mut advanced = 0usize;
    let mut first_error: Option<String> = None;
    for rt in manager.list_runtimes() {
        let is_leader = {
            let metrics = rt.raft.metrics();
            let metrics = metrics.borrow_watched();
            metrics.current_leader == Some(node.node_id)
        };
        if !is_leader {
            continue;
        }
        let current = rt.mvcc.current_revision();
        let cursor = match rt.mvcc.last_applied_decision_ts() {
            Ok(cursor) => cursor,
            Err(e) => {
                first_error.get_or_insert_with(|| {
                    format!("region {} read decision cursor: {e}", rt.region_id())
                });
                continue;
            }
        };
        let target = match node.storage.next_sequencer_decision_ts(cursor) {
            Ok(Some(next)) if next <= current => {
                first_error.get_or_insert_with(|| {
                    format!(
                        "region {} current_revision {current} passed unapplied decision {next}",
                        rt.region_id()
                    )
                });
                continue;
            }
            Ok(Some(next)) => fence.min(next.saturating_sub(1)),
            Ok(None) => fence,
            Err(e) => {
                first_error.get_or_insert_with(|| format!("region 0 scan pending decisions: {e}"));
                continue;
            }
        };
        if current >= target {
            continue;
        }

        match rt
            .raft
            .client_write(Command::AdvanceSafeTs { commit_ts: target })
            .await
        {
            Ok(resp) => match resp.response() {
                Response::SafeTsAdvanced { revision } if *revision >= target => {
                    advanced += 1;
                }
                other => {
                    first_error.get_or_insert_with(|| {
                        format!(
                            "region {} unexpected AdvanceSafeTs response for {target}: {other:?}",
                            rt.region_id()
                        )
                    });
                }
            },
            Err(e) => {
                first_error.get_or_insert_with(|| {
                    format!("region {} advance safe-ts to {target}: {e}", rt.region_id())
                });
            }
        }
    }

    // 刷新 gauge / 每 Region apply lag（失败不影响水位）。
    node.safe_ts();

    match first_error {
        Some(err) => Err(err),
        None => Ok(advanced),
    }
}

/// 启动后台推进任务（进程生命周期；shutdown 时随进程退出）。
pub fn start_advancer(node: Arc<CoordNode>, interval: Duration) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            if let Err(err) = advance_once(&node).await {
                tracing::warn!(error = %err, "safe-ts advance failed; retrying next tick");
            }
        }
    })
}
