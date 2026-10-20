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

use crate::raft::region_runtime::RegionRuntime;
use crate::raft::type_config::{Command, CommitDecisionApplyLog, Response, SequencerMutation};
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
    meta.start_key.as_slice() <= key && (meta.end_key.is_empty() || key < meta.end_key.as_slice())
}

fn mutation_bytes(mutation: &SequencerMutation) -> usize {
    match mutation {
        SequencerMutation::Put { key, value, .. } => key.len() + value.len() + 24,
        SequencerMutation::Delete { key } => key.len() + 16,
        SequencerMutation::DeleteRange { start, end } => start.len() + end.len() + 32,
    }
}

/// 把区间写裁剪到 Region 的 `[start, end)`；完全不相交返回 `None`。
///
/// Region/写区间的空 `end` 都表示 +∞；`DeleteRange.end` 在 RPC 层非空，这里
/// 仍按通用语义处理，避免直接调用方（测试/回放）越界。
fn clip_range(meta: &RegionMeta, start: &[u8], end: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
    let clip_start = start.max(meta.start_key.as_slice());
    let clip_end = match (meta.end_key.is_empty(), end.is_empty()) {
        (_, true) => meta.end_key.clone(),
        (true, false) => end.to_vec(),
        (false, false) => end.min(meta.end_key.as_slice()).to_vec(),
    };
    if clip_end.is_empty() || clip_start < clip_end.as_slice() {
        Some((clip_start.to_vec(), clip_end))
    } else {
        None
    }
}

/// 按 Region 裁剪一条 mutation；点写按 key 归属，区间写按区间相交。
fn clip_mutation(meta: &RegionMeta, mutation: &SequencerMutation) -> Option<SequencerMutation> {
    match mutation {
        SequencerMutation::Put { .. } | SequencerMutation::Delete { .. } => mutation
            .point_key()
            .filter(|key| key_in_region(meta, key))
            .map(|_| mutation.clone()),
        SequencerMutation::DeleteRange { start, end } => clip_range(meta, start, end)
            .map(|(start, end)| SequencerMutation::DeleteRange { start, end }),
    }
}

/// 按条数与字节双上限构造一个 Region 的 apply 载荷。
///
/// 决策的最大单条是 `max_txn_bytes`（4MiB）；只按条数切批会让 256 条大事务
/// 变成 1GiB 的 raft 负载。首条无论多大都保留（绝不丢决策）。
fn build_apply_batch(
    decisions: &[crate::storage::mvcc::SequencerDecision],
    meta: &RegionMeta,
    batch: usize,
    max_bytes: usize,
) -> Vec<CommitDecisionApplyLog> {
    let mut payload = Vec::new();
    let mut bytes = 0usize;
    for decision in decisions.iter().take(batch) {
        let mutations: Vec<SequencerMutation> = decision
            .mutations
            .iter()
            .filter_map(|mutation| clip_mutation(meta, mutation))
            .collect();
        let size = 24 + mutations.iter().map(mutation_bytes).sum::<usize>();
        if !payload.is_empty() && bytes.saturating_add(size) > max_bytes.max(1) {
            break;
        }
        bytes = bytes.saturating_add(size);
        payload.push(CommitDecisionApplyLog {
            shard_id: crate::storage::commit_log::COMMIT_LOG_STAGING_SHARD_ID,
            region_id: meta.region_id,
            commit_ts: decision.commit_ts,
            txn_id: decision.txn_id.clone(),
            start_ts: decision.start_ts,
            mutations,
        });
    }
    payload
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
            let payload = build_apply_batch(
                &batch_decisions,
                &meta,
                batch,
                node.kvt().config().max_txn_bytes as usize,
            );
            let Some(last_ts) = payload.last().map(|decision| decision.commit_ts) else {
                break;
            };
            let more = payload.len() >= batch || payload.len() < batch_decisions.len();

            match rt
                .raft
                .client_write(Command::ApplyCommitDecisionsWithLog {
                    max_txn_bytes: node.kvt().config().max_txn_bytes,
                    decisions: payload,
                })
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

    fn decision(
        commit_ts: u64,
        mutations: Vec<SequencerMutation>,
    ) -> crate::storage::mvcc::SequencerDecision {
        crate::storage::mvcc::SequencerDecision {
            txn_id: format!("t{commit_ts}").into_bytes(),
            request_id: Vec::new(),
            start_ts: commit_ts.saturating_sub(1),
            commit_ts,
            mutations,
        }
    }

    fn put(key: &[u8], value: &[u8]) -> SequencerMutation {
        SequencerMutation::Put {
            key: key.to_vec(),
            value: value.to_vec(),
            lease_id: None,
        }
    }

    #[test]
    fn build_apply_batch_caps_bytes_and_keeps_order() {
        let all = meta(b"", b"");
        let decisions = vec![
            decision(10, vec![put(b"a", &[0u8; 100])]),
            decision(20, vec![put(b"b", &[0u8; 100])]),
            decision(30, Vec::new()),
        ];
        let payload = build_apply_batch(&decisions, &all, 10, 150);
        assert_eq!(
            payload.len(),
            1,
            "second 100B txn must wait for the next batch"
        );
        assert_eq!(payload[0].commit_ts, 10);

        // 空决策几乎不占字节：不因前面的数据决策被挤掉顺序。
        let payload = build_apply_batch(&decisions, &all, 10, usize::MAX);
        assert_eq!(payload.len(), 3);
        assert_eq!(payload[2].commit_ts, 30);

        // 首条超预算也保留，绝不丢决策。
        let payload = build_apply_batch(&decisions, &all, 10, 1);
        assert_eq!(payload.len(), 1);
        assert_eq!(payload[0].commit_ts, 10);
    }

    #[test]
    fn build_apply_batch_filters_by_region() {
        let region = meta(b"m", b"t");
        let decisions = vec![decision(10, vec![put(b"a", b"1"), put(b"n", b"2")])];
        let payload = build_apply_batch(&decisions, &region, 10, usize::MAX);
        assert_eq!(payload.len(), 1);
        assert_eq!(payload[0].mutations.len(), 1);
        assert_eq!(payload[0].mutations[0].point_key(), Some(b"n".as_slice()));
        assert_eq!(
            payload[0].shard_id,
            crate::storage::commit_log::COMMIT_LOG_STAGING_SHARD_ID
        );
        assert_eq!(payload[0].region_id, region.region_id);
        assert_eq!(payload[0].txn_id, b"t10");
        assert_eq!(payload[0].start_ts, 9);
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

    fn delete_range(start: &[u8], end: &[u8]) -> SequencerMutation {
        SequencerMutation::DeleteRange {
            start: start.to_vec(),
            end: end.to_vec(),
        }
    }

    #[test]
    fn clip_mutation_clips_delete_range_per_region() {
        let left = meta(b"", b"m");
        let right = meta(b"m", b"");

        assert_eq!(
            clip_mutation(&left, &delete_range(b"a", b"z")),
            Some(delete_range(b"a", b"m"))
        );
        assert_eq!(
            clip_mutation(&right, &delete_range(b"a", b"z")),
            Some(delete_range(b"m", b"z"))
        );
        assert_eq!(
            clip_mutation(&right, &delete_range(b"a", b"b")),
            None,
            "a range that ends before the region start has no mutations"
        );
        assert_eq!(
            clip_mutation(&left, &put(b"apple", b"1")),
            Some(put(b"apple", b"1"))
        );
        assert_eq!(clip_mutation(&right, &put(b"apple", b"1")), None);
        assert_eq!(
            clip_mutation(
                &right,
                &SequencerMutation::Delete {
                    key: b"melon".to_vec()
                }
            ),
            Some(SequencerMutation::Delete {
                key: b"melon".to_vec()
            })
        );
    }

    #[test]
    fn build_apply_batch_keeps_decision_without_region_mutations() {
        let region = meta(b"m", b"t");
        let payload = build_apply_batch(
            &[decision(10, vec![delete_range(b"a", b"b")])],
            &region,
            10,
            usize::MAX,
        );
        assert_eq!(payload.len(), 1, "the cursor must still advance");
        assert_eq!(payload[0].commit_ts, 10);
        assert!(payload[0].mutations.is_empty());
    }

    #[test]
    fn build_apply_batch_accounts_delete_range_bytes() {
        let all = meta(b"", b"");
        let decisions = vec![
            decision(10, vec![delete_range(b"a", &[0xffu8; 100])]),
            decision(20, vec![delete_range(b"b", &[0xffu8; 100])]),
        ];
        let payload = build_apply_batch(&decisions, &all, 10, 150);
        assert_eq!(payload.len(), 1);
        assert_eq!(payload[0].commit_ts, 10);
    }
}
