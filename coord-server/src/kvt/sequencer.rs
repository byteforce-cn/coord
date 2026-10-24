//! leader 侧 Commit Sequencer（ADR-0015 D4/D9/D11，#146）。
//!
//! 校验与决策由 region 0 system raft 的 `Command::Sequencer` apply 完成；本模块
//! 只负责：按条数/字节切批、从 TSO 取 `commit_ts`、串行化提案，保证提案顺序 =
//! `commit_ts` 顺序。`coord.txn.v2.Commit` 的 RPC 接线归 #151，本模块先提供可独立
//! 测试的内部接口。`propose` 锁也被 safe_ts 的 fence 发布复用：两者串行化后，
//! 已发布的 fence 之后不会再有 `commit_ts <= fence` 的新决策。

use std::collections::BTreeSet;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use coord_core::error::{Error, Result};
use coord_core::types::NodeID;

use crate::raft::tso::TsoClient;
use crate::raft::type_config::{
    Command, Response, SequencerMutation, SequencerOp, SequencerReadRange, SequencerTxn,
    SequencerTxnWithRanges, SequencerValidationLimits,
};
use crate::raft::{CoordRaft, WatchReceiver};
use crate::storage::mvcc::MvccStorage;
use crate::storage::redb_backend::RedbBackend;

/// 单条事务在批大小估算里的固定开销（txn_id/request_id 长度、枚举标签、长度前缀）。
/// 只用于切批，不参与 wire 编码或上限判定。
pub const SEQUENCER_TXN_OVERHEAD_BYTES: usize = 64;

/// 单次 raft 提案的超时上限（无 quorum 时返回可重试错误，不无限挂起）。
/// 超时后命令仍可能稍后 commit；调用方用同一 txn_id/request_id 重试会命中
/// 幂等索引，不会产生第二个决策。
pub const SEQUENCER_PROPOSE_TIMEOUT: Duration = Duration::from_secs(10);

/// 一条待校验事务（leader 侧输入；`commit_ts` 由 sequencer 分配）。
#[derive(Clone, Debug)]
pub struct CommitCandidate {
    pub txn_id: Vec<u8>,
    pub start_ts: u64,
    pub request_id: Vec<u8>,
    /// point read set（`BTreeSet` 去重、顺序稳定）。
    pub read_set: BTreeSet<Vec<u8>>,
    /// range 读谓词（`[start, end)`；`end` 空 = 无上界）。
    pub read_ranges: Vec<SequencerReadRange>,
    pub mutations: Vec<SequencerMutation>,
}

impl CommitCandidate {
    /// 粗估 wire 字节数（key/value 实际长度 + 固定开销），用于字节上限切批。
    pub fn wire_bytes(&self) -> usize {
        let reads: usize = self
            .read_set
            .iter()
            .map(Vec::len)
            .sum::<usize>()
            .saturating_add(
                self.read_ranges
                    .iter()
                    .map(|range| {
                        range
                            .start
                            .len()
                            .saturating_add(range.end.len())
                            .saturating_add(16)
                    })
                    .sum(),
            );
        let writes: usize = self
            .mutations
            .iter()
            .map(|mutation| match mutation {
                SequencerMutation::Put { key, value, .. } => key.len() + value.len() + 16,
                SequencerMutation::Delete { key } => key.len() + 8,
                SequencerMutation::DeleteRange { start, end } => {
                    start.len().saturating_add(end.len()).saturating_add(32)
                }
            })
            .sum();
        SEQUENCER_TXN_OVERHEAD_BYTES
            .saturating_add(self.txn_id.len())
            .saturating_add(self.request_id.len())
            .saturating_add(reads)
            .saturating_add(writes)
    }
}

/// 按条数与字节双上限切批，保持输入顺序。
///
/// 单条事务无论多大都独占一批（绝不拆事务）；`max_count` / `max_bytes` 为 0
/// 时按 1 处理，避免产生空批或除零。
pub fn batch_ranges(sizes: &[usize], max_count: usize, max_bytes: usize) -> Vec<Range<usize>> {
    let max_count = max_count.max(1);
    let max_bytes = max_bytes.max(1);
    let mut ranges = Vec::new();
    let mut start = 0usize;
    let mut bytes = 0usize;
    for (index, size) in sizes.iter().enumerate() {
        let count = index - start;
        let would_exceed =
            count >= max_count || (count > 0 && bytes.saturating_add(*size) > max_bytes);
        if would_exceed {
            ranges.push(start..index);
            start = index;
            bytes = 0;
        }
        bytes = bytes.saturating_add(*size);
    }
    if start < sizes.len() {
        ranges.push(start..sizes.len());
    }
    ranges
}

/// region 0 leader 上的 Commit Sequencer。
pub struct CommitSequencer {
    raft: CoordRaft,
    node_id: NodeID,
    tso: Arc<TsoClient>,
    /// region 0 存储：读 safe-point fence（`commit_ts` 必须高于已发布 fence）。
    storage: Arc<MvccStorage<RedbBackend>>,
    max_batch_count: usize,
    max_batch_bytes: usize,
    /// OCC 校验上限；随命令写入 raft 日志，apply 不读本地配置。
    limits: SequencerValidationLimits,
    propose_timeout: Duration,
    /// 串行化「取 TSO → 提案」：否则并发提案可能让 `commit_ts` 顺序与日志顺序
    /// 不一致，OCC 校验会漏掉本应冲突的事务。等待锁即背压：并发调用串行排队。
    propose: tokio::sync::Mutex<()>,
    /// 最近一次在本任期确认「region 0 本地 apply 已追平提交位」的任期（0 = 未确认）。
    ///
    /// 换 leader 后 applied 状态可能落后于旧 leader 已发布的 fence；直接取号会
    /// 分配出 `commit_ts <= 已发布 fence` 的决策，破坏 [`Self::publish_fence`]
    /// 的「决策恒大于已发布 fence」不变量。
    linearized_term: AtomicU64,
}

impl CommitSequencer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        raft: CoordRaft,
        node_id: NodeID,
        tso: Arc<TsoClient>,
        storage: Arc<MvccStorage<RedbBackend>>,
        max_batch_count: usize,
        max_batch_bytes: usize,
        limits: SequencerValidationLimits,
        propose_timeout: Duration,
    ) -> Self {
        Self {
            raft,
            node_id,
            tso,
            storage,
            max_batch_count: max_batch_count.max(1),
            max_batch_bytes: max_batch_bytes.max(1),
            limits,
            propose_timeout,
            propose: tokio::sync::Mutex::new(()),
            linearized_term: AtomicU64::new(0),
        }
    }

    /// 在 propose 锁内发布 safe-point fence（region 0 raft），返回有效 fence。
    ///
    /// 调用方保证 `fence_ts` 的所有 `commit_ts` 都已发放（例如直写自己的
    /// `commit_ts - 1`）。发布与取号共用 [`Self::propose`]，因此：
    /// - 发布瞬间没有 in-flight 决策；
    /// - 之后的决策 `commit_ts` 必然大于该 fence。
    ///
    /// 两件事合起来给出直写次序守卫需要的判据：本地 apply 到 `F` 的节点看得到
    /// 所有 `commit_ts <= F` 的决策，且之后不会再有 `commit_ts <= F` 的决策。
    pub async fn publish_fence(&self, fence_ts: u64) -> Result<u64> {
        let _guard = self.propose.lock().await;
        self.publish_fence_locked(fence_ts).await
    }

    /// 在 propose 锁内把 **TSO 分配高水位**发布为 fence（safe_ts 推进器使用）。
    ///
    /// 高水位必须在锁内读取：锁外读到的高水位可能低于随后（锁内）取号得到的
    /// `commit_ts`，那会让已发布 fence 越过一条刚落盘的决策。
    pub async fn publish_high_water_fence(&self) -> Result<u64> {
        let _guard = self.propose.lock().await;
        let high_water = self.storage.tso_high_water()?;
        self.publish_fence_locked(high_water).await
    }

    async fn publish_fence_locked(&self, fence_ts: u64) -> Result<u64> {
        let published = self.storage.safe_ts_fence()?;
        // fence 只前进：已经发布到更高值时无需再写一条 raft 日志。
        if fence_ts <= published {
            return Ok(published);
        }
        let response = self
            .raft
            .client_write(Command::PublishSafeTs { fence_ts })
            .await
            .map_err(|err| Error::ClusterUnavailable(format!("publish safe-ts fence: {err}")))?;
        match response.response() {
            Response::SafeTsPublished {
                fence_ts: effective,
            } if *effective >= fence_ts => Ok(*effective),
            other => Err(Error::Internal(format!(
                "unexpected PublishSafeTs response for {fence_ts}: {other:?}"
            ))),
        }
    }

    /// 换任期后先确认本节点 region 0 apply 追平提交位，再读 fence 取号。
    ///
    /// `client_write` 返回时本地 apply 已包含该条目（响应来自状态机），因此同一
    /// 任期内的 fence 读取是新鲜的；唯一可能滞后的是「刚换 leader、尚未写过任何
    /// 条目」的窗口。ReadIndex + `applied_index_at_least` 正好覆盖这一段。
    async fn ensure_region0_applied(&self) -> Result<()> {
        let term = self.raft.metrics().borrow_watched().current_term;
        if term > 0 && self.linearized_term.load(Ordering::Acquire) == term {
            return Ok(());
        }
        if let Err(status) = crate::server::ensure_linearizable_barrier(
            self.node_id,
            self.propose_timeout,
            Some(&self.raft),
            None,
            0,
        )
        .await
        {
            return Err(match self.raft.current_leader().await {
                Some(leader) if leader != self.node_id => Error::NotLeader { leader_addr: None },
                _ => Error::ClusterUnavailable(format!("region0 apply barrier failed: {status}")),
            });
        }
        self.linearized_term.store(term, Ordering::Release);
        Ok(())
    }

    /// 校验并提交一批事务；返回与输入一一对应、顺序一致的结果。
    ///
    /// 超过条数/字节上限时按 [`batch_ranges`] 拆成多次 raft 提案；每次提案
    /// 独立取 TSO。失败提案留下的时间戳空洞符合 D2（允许空洞、绝不复用）。
    pub async fn validate_and_commit(
        &self,
        candidates: Vec<CommitCandidate>,
    ) -> Result<Vec<crate::raft::type_config::SequencerOutcome>> {
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let sizes: Vec<usize> = candidates.iter().map(CommitCandidate::wire_bytes).collect();
        let ranges = batch_ranges(&sizes, self.max_batch_count, self.max_batch_bytes);
        let mut outcomes = Vec::with_capacity(candidates.len());
        for range in ranges {
            outcomes.extend(self.propose_batch(&candidates[range]).await?);
        }
        Ok(outcomes)
    }

    async fn propose_batch(
        &self,
        candidates: &[CommitCandidate],
    ) -> Result<Vec<crate::raft::type_config::SequencerOutcome>> {
        let _guard = self.propose.lock().await;
        // fence 读取必须晚于「apply 追平提交位」：否则新 leader 可能用旧 fence
        // 取号，发出 `commit_ts <= 已发布 fence` 的决策。
        self.ensure_region0_applied().await?;
        let fence = self.storage.safe_ts_fence()?;
        let mut txns = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let commit_ts = self.tso.get_ts_above(fence).await?;
            txns.push(SequencerTxnWithRanges {
                txn: SequencerTxn {
                    txn_id: candidate.txn_id.clone(),
                    start_ts: candidate.start_ts,
                    commit_ts,
                    request_id: candidate.request_id.clone(),
                    read_set: candidate.read_set.iter().cloned().collect(),
                    mutations: candidate.mutations.clone(),
                },
                read_ranges: candidate.read_ranges.clone(),
            });
        }

        let command = Command::Sequencer(SequencerOp::ValidateAndCommitWithRanges {
            txns,
            limits: self.limits,
        });
        let response =
            match tokio::time::timeout(self.propose_timeout, self.raft.client_write(command)).await
            {
                Ok(Ok(response)) => response,
                Ok(Err(err)) => return Err(self.classify_propose_error(err).await),
                Err(_) => {
                    return Err(Error::ClusterUnavailable(format!(
                        "region0 sequencer propose timed out after {:?}",
                        self.propose_timeout
                    )))
                }
            };
        match response.response() {
            Response::Sequencer { outcomes } if outcomes.len() == candidates.len() => {
                Ok(outcomes.clone())
            }
            other => Err(Error::Internal(format!(
                "unexpected region0 sequencer response: {other:?}"
            ))),
        }
    }

    /// 提案失败可能是 leadership 迁移：先刷新 leader 视图再分类，避免把迁移
    /// 误报成存储错误。
    async fn classify_propose_error(&self, err: impl std::fmt::Display) -> Error {
        match self.raft.current_leader().await {
            Some(leader) if leader != self.node_id => Error::NotLeader { leader_addr: None },
            _ => Error::ClusterUnavailable(format!("region0 sequencer propose failed: {err}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_ranges_empty_and_single() {
        assert!(batch_ranges(&[], 256, 1_000).is_empty());
        assert_eq!(batch_ranges(&[10], 256, 1_000), vec![0..1]);
        // 单条超过字节上限：独占一批，不拆分、不丢弃。
        assert_eq!(batch_ranges(&[10_000, 1], 256, 1_000), vec![0..1, 1..2]);
    }

    #[test]
    fn test_batch_ranges_count_boundary() {
        let sizes = vec![1usize; 300];
        assert_eq!(
            batch_ranges(&sizes, 256, usize::MAX),
            vec![0..256, 256..300]
        );
        assert_eq!(batch_ranges(&sizes, 1, usize::MAX).len(), 300);
        assert_eq!(
            batch_ranges(&sizes, 0, usize::MAX),
            batch_ranges(&sizes, 1, usize::MAX),
            "0 按 1 处理，不产生空批"
        );
    }

    #[test]
    fn test_batch_ranges_byte_boundary_keeps_order() {
        // [60, 40] 正好 100 → 同批；再加 1 字节 → 新批。
        assert_eq!(batch_ranges(&[60, 40, 1], 256, 100), vec![0..2, 2..3]);
        // 正好等于上限仍同批（上限是闭边界）。
        assert_eq!(batch_ranges(&[60, 40], 256, 100), vec![0..2]);
        // 0 字节上限按 1 处理；0 字节条目本身不占额度，不会触发切批。
        assert_eq!(batch_ranges(&[0, 0], 256, 0), vec![0..2]);
        assert_eq!(batch_ranges(&[1, 1], 256, 0), vec![0..1, 1..2]);
    }

    #[test]
    fn test_wire_bytes_counts_delete_range() {
        use crate::raft::type_config::SequencerMutation;

        let base = CommitCandidate {
            txn_id: b"t".to_vec(),
            start_ts: 1,
            request_id: b"r".to_vec(),
            read_set: BTreeSet::new(),
            read_ranges: Vec::new(),
            mutations: Vec::new(),
        };
        let mut ranged = base.clone();
        ranged.mutations = vec![SequencerMutation::DeleteRange {
            start: b"a".to_vec(),
            end: vec![0u8; 4_096],
        }];
        assert_eq!(ranged.wire_bytes() - base.wire_bytes(), 1 + 4_096 + 32);
    }
}
