//! CommitDecision 应用器（ADR-0015 D4/D12.3，#148）。
//!
//! region 0 的 sequencer 只负责「校验 + 写决策」；本模块把决策日志按
//! `commit_ts` 顺序送到**每个数据 Region 自己的 raft**，由 apply 在一个 redb
//! 事务里写数据/changelog/游标。跨 Region 的原子可见性不靠分布式事务：
//! `safe_ts = min(各 Region 已 apply 水位)`，读在 `read_ts > safe_ts` 时等待。
//!
//! 每个 Region 独立续跑：游标在 Region 存储的 `/_sys/kv_t/decision_applied`，
//! 崩溃/换 leader 后从游标重放，apply 的 changelog 守卫保证幂等。没有本 Region
//! key 的决策以空切片推进游标，否则空闲 Region 会永久钉住 safe_ts。

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use coord_core::types::RegionMeta;

use crate::raft::type_config::{Command, CommitDecisionApply, Response, SequencerMutation};
use crate::raft::region_runtime::RegionRuntime;
use crate::raft::WatchReceiver;
use crate::server::CoordNode;

/// 默认 apply 周期：Commit 只等决策落盘，apply 允许最多一个周期的可见性延迟。
pub const DECISION_APPLY_INTERVAL: Duration = Duration::from_millis(50);

fn is_leader(rt: &RegionRuntime, node_id: u64) -> bool {
    let metrics = rt.raft.metrics();
    let metrics = metrics.borrow_watched();
    metrics.current_leader == Some(node_id)
}

/// `end_key` 为空 = 无上界；Region 区间语义与 `range_in` 一致（`[start,end)`）。
fn key_in_region(meta: &RegionMeta, key: &[u8]) -> bool {
    meta.start_key.as_slice() <= key
        && (meta.end_key.is_empty() || key < meta.end_key.as_slice())
}

/// 扫描 region 0 决策日志，把新决策应用到本节点领导的数据 Region。
///
/// 返回本轮推进的决策条数（跨 Region 合计）。单个 Region 失败不阻止其他
/// Region；全部尝试完后返回首个错误，调用方记录 warning 后下轮重试。
pub async fn apply_pending_decisions(node: &CoordNode, batch: usize) -> Result<u64, String> {
    if !node.kvt().is_enabled() {
        return Ok(0);
    }
    let Some(manager) = &node.region_manager else {
        return Ok(0);
    };
    let batch = batch.max(1);
    let mut applied_total = 0u64;
    let mut first_error: Option<String> = None;

    for rt in manager.list_runtimes() {
        if !is_leader(&rt, node.node_id) {
            continue;
        }
        let meta = rt.meta();
        loop {
            let cursor = match rt.mvcc.last_applied_decision_ts() {
                Ok(cursor) => cursor,
                Err(err) => {
                    first_error.get_or_insert_with(|| {
                        format!("region {} read decision cursor: {err}", rt.region_id())
                    });
                    break;
                }
            };
            let batch_decisions = match node.storage.sequencer_decisions_after(cursor, batch) {
                Ok(decisions) => decisions,
                Err(err) => {
                    return Err(format!("region 0 scan commit decisions: {err}"));
                }
            };
            if batch_decisions.is_empty() {
                break;
            }
            let Some(last_ts) = batch_decisions.last().map(|decision| decision.commit_ts) else {
                break;
            };

            let payload: Vec<CommitDecisionApply> = batch_decisions
                .iter()
                .map(|decision| CommitDecisionApply {
                    commit_ts: decision.commit_ts,
                    mutations: decision
                        .mutations
                        .iter()
                        .filter(|mutation| key_in_region(&meta, mutation.key()))
                        .cloned()
                        .collect::<Vec<SequencerMutation>>(),
                })
                .collect();
            let more = batch_decisions.len() >= batch;

            match rt
                .raft
                .client_write(Command::ApplyCommitDecisions { decisions: payload })
                .await
            {
                Ok(resp) => match resp.response() {
                    Response::CommitDecisionsApplied {
                        through_ts,
                        decisions,
                    } if *through_ts >= last_ts => {
                        applied_total += *decisions;
                        if !more {
                            break;
                        }
                    }
                    Response::CommitDecisionsApplied { through_ts, .. } => {
                        first_error.get_or_insert_with(|| {
                            format!(
                                "region {} decision apply stopped at {through_ts}, expected {last_ts}",
                                rt.region_id()
                            )
                        });
                        break;
                    }
                    other => {
                        first_error.get_or_insert_with(|| {
                            format!(
                                "region {} unexpected ApplyCommitDecisions response: {other:?}",
                                rt.region_id()
                            )
                        });
                        break;
                    }
                },
                Err(err) => {
                    first_error.get_or_insert_with(|| {
                        format!("region {} propose commit decisions: {err}", rt.region_id())
                    });
                    break;
                }
            }
        }
    }

    match first_error {
        Some(err) => Err(err),
        None => Ok(applied_total),
    }
}

/// 启动后台应用任务（进程生命周期；shutdown 时随进程退出）。
pub fn start_applier(node: Arc<CoordNode>, interval: Duration) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let batch = node.kvt().config().commit_sequencer_batch as usize;
            match apply_pending_decisions(&node, batch).await {
                Ok(0) => {}
                Ok(decisions) => {
                    tracing::debug!(
                        decisions,
                        "applied commit decisions to local region leaders"
                    );
                }
                Err(err) => {
                    tracing::warn!(error = %err, "commit decision apply failed; retrying next tick");
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use coord_core::types::{Peer, PeerRole, RegionEpoch};

    fn meta(start: &[u8], end: &[u8]) -> RegionMeta {
        RegionMeta {
            region_id: 1,
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

    #[test]
    fn key_in_region_respects_half_open_bounds() {
        let bounded = meta(b"m", b"t");
        assert!(!key_in_region(&bounded, b"a"));
        assert!(key_in_region(&bounded, b"m"));
        assert!(key_in_region(&bounded, b"s"));
        assert!(!key_in_region(&bounded, b"t"));

        let unbounded = meta(b"m", b"");
        assert!(key_in_region(&unbounded, b"m"));
        assert!(key_in_region(&unbounded, b"zzz"));
        assert!(!key_in_region(&unbounded, b"l"));
    }
}
