//! 全局 TSO：region 0 状态机分配器 + 进程内批次缓存（ADR-0015 D2/D12.4，#136/#137）。
//!
//! 只有 region 0 leader 能在本地 `client_write`；follower 返回 `NOT_LEADER`
//! （跨节点转发是后续工作项）。[`TsoClient`] 缓存 `[next, end)` 区间，低于阈值
//! 预取新批，失去 leadership 时丢弃未用完的批（ADR-0015 D2）。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::Mutex;

use coord_core::error::{Error, Result};
use coord_core::types::NodeID;

use crate::metrics::Metrics;

use super::type_config::{Command, Response, TsoOp};
use super::CoordRaft;

/// 单个 TSO 批次（左闭右开：`start_ts` 可用，`end_ts` 不可用）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TsoBatch {
    pub start_ts: u64,
    pub end_ts: u64,
}

/// 批次请求归一：`0` 视为 1；超过 `max`（`[kv_t].tso_batch_size`，
/// D9 上限 65536）显式拒绝，不静默截断。
pub fn normalize_batch_size(requested: u32, max: u32) -> Result<u32> {
    let size = requested.max(1);
    if size > max {
        return Err(Error::InvalidArgument(format!(
            "tso batch_size {size} exceeds max {max}"
        )));
    }
    Ok(size)
}

/// region 0 leader 的 TSO 分配器。
///
/// `CoordRaft` 的 `Clone` 是 Arc bump；分配器可随请求临时构造。高水位不在
/// 内存：每次分配都经 raft apply 读/写 `/_sys/tso/high_water`，重启/换 leader
/// 自然接续。
#[derive(Clone)]
pub struct TsoAllocator {
    raft: CoordRaft,
    node_id: NodeID,
}

impl TsoAllocator {
    pub fn new(raft: CoordRaft, node_id: NodeID) -> Self {
        Self { raft, node_id }
    }

    /// 申请一个 `[start_ts, end_ts)` 批次。
    ///
    /// 非 leader：`NotLeader`（调用方应刷新 leader 后重试）；选举窗口：
    /// `ClusterUnavailable`。提交流失败后复核 leader，避免把 leadership 迁移
    /// 误报成存储错误。
    pub async fn allocate(&self, batch_size: u32) -> Result<TsoBatch> {
        let batch_size = batch_size.max(1);
        match self.raft.current_leader().await {
            Some(leader) if leader == self.node_id => {}
            Some(_) => return Err(Error::NotLeader { leader_addr: None }),
            None => {
                return Err(Error::ClusterUnavailable(
                    "region 0 leader unknown (election window)".to_string(),
                ))
            }
        }

        let resp = match self
            .raft
            .client_write(Command::Tso(TsoOp::Allocate { batch_size }))
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                // 提交流失败可能是 leadership 迁移：先刷新 leader 视图再分类。
                return match self.raft.current_leader().await {
                    Some(leader) if leader != self.node_id => {
                        Err(Error::NotLeader { leader_addr: None })
                    }
                    _ => Err(Error::ClusterUnavailable(format!(
                        "region0 raft tso propose failed: {e}"
                    ))),
                };
            }
        };

        match resp.response() {
            Response::Tso { start_ts, end_ts } => Ok(TsoBatch {
                start_ts: *start_ts,
                end_ts: *end_ts,
            }),
            other => Err(Error::Internal(format!(
                "unexpected region0 tso propose response: {other:?}"
            ))),
        }
    }
}

/// 批次缓存剩余量低于 `batch_size / TSO_REFILL_THRESHOLD_DIVISOR` 时预取新批。
///
/// 预取会丢弃当前批剩余部分——ADR-0015 D2 只要求唯一/单调、允许空洞。
pub const TSO_REFILL_THRESHOLD_DIVISOR: u64 = 8;

/// 选举窗口（`ClusterUnavailable`）下 refill 重试的截止时间；到期后把错误
/// 上抛，避免请求无限挂起。
pub const TSO_REFILL_DEADLINE: Duration = Duration::from_secs(5);

const REFILL_BACKOFF_START: Duration = Duration::from_millis(10);
const REFILL_BACKOFF_MAX: Duration = Duration::from_millis(200);

/// TSO 来源（生产实现是 [`TsoAllocator`]；测试用可控 fake）。
#[async_trait]
pub trait TsoSource: Send + Sync + 'static {
    /// 本节点当前是否为 region 0 leader（follower 不得从本地缓存发放）。
    async fn is_leader(&self) -> bool;
    /// 申请 `[start_ts, end_ts)` 批；语义同 [`TsoAllocator::allocate`]。
    async fn allocate(&self, batch_size: u32) -> Result<TsoBatch>;
}

#[async_trait]
impl TsoSource for TsoAllocator {
    async fn is_leader(&self) -> bool {
        self.raft.current_leader().await == Some(self.node_id)
    }

    async fn allocate(&self, batch_size: u32) -> Result<TsoBatch> {
        TsoAllocator::allocate(self, batch_size).await
    }
}

/// 进程内 TSO 发放器：一次 raft 申请一批，本地逐值发放。
///
/// 不变量：同一进程发出的值互不重复；新批 `start_ts` 不得低于本地 `next`
/// （低于则 fail-closed，而不是发放可能重复的值）；follower 拒绝发放。
pub struct TsoClient {
    source: Arc<dyn TsoSource>,
    batch_size: u32,
    metrics: Option<Arc<Metrics>>,
    cache: Mutex<CachedBatch>,
    /// 串行化 refill 的 raft round-trip；与 `cache` 分开，因为这里要跨 `await`。
    refill: tokio::sync::Mutex<()>,
    was_leader: AtomicBool,
}

#[derive(Debug, Clone, Copy, Default)]
struct CachedBatch {
    next: u64,
    end: u64,
}

impl TsoClient {
    pub fn new(
        source: Arc<dyn TsoSource>,
        batch_size: u32,
        metrics: Option<Arc<Metrics>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            source,
            batch_size: batch_size.max(1),
            metrics,
            cache: Mutex::new(CachedBatch::default()),
            refill: tokio::sync::Mutex::new(()),
            was_leader: AtomicBool::new(false),
        })
    }

    /// 只读快照 `(next, end)`（诊断/测试用）。
    pub fn cached_range(&self) -> (u64, u64) {
        let cache = self.cache.lock();
        (cache.next, cache.end)
    }

    /// 取一个全局唯一、严格递增的 TSO。
    ///
    /// 非 leader → `NotLeader`（并丢弃未用完的批，换 leader 后不再发放旧值）；
    /// 选举窗口 → 有界重试后返回 `ClusterUnavailable`。
    pub async fn get_ts(&self) -> Result<u64> {
        self.get_ts_above(0).await
    }

    /// 取一个 **大于 `floor`** 的 TSO。
    ///
    /// `floor` 是已发布的 safe-point fence：`<= floor` 的 `commit_ts` 写入必被
    /// 数据 Region 拒绝，继续发放只会让调用方重试。跳过这些值只产生空洞
    /// （D2/D12.4 允许），不会重复发放。
    pub async fn get_ts_above(&self, floor: u64) -> Result<u64> {
        if !self.source.is_leader().await {
            if self.was_leader.swap(false, Ordering::AcqRel) {
                self.discard_batch();
                if let Some(metrics) = &self.metrics {
                    metrics.inc_tso_leader_change();
                }
            }
            return Err(Error::NotLeader { leader_addr: None });
        }
        self.was_leader.store(true, Ordering::Release);

        loop {
            self.skip_to_above(floor);
            if self.needs_refill() {
                self.refill().await?;
                continue;
            }
            if let Some(ts) = self.claim() {
                if ts > floor {
                    return Ok(ts);
                }
            }
        }
    }

    /// 丢弃缓存中 `<= floor` 的值；跳过批尾时清空，由 loop 的 refill 接续。
    fn skip_to_above(&self, floor: u64) {
        if floor == 0 {
            return;
        }
        let remaining = {
            let mut cache = self.cache.lock();
            if cache.next <= floor {
                cache.next = floor.saturating_add(1).min(cache.end);
            }
            cache.end.saturating_sub(cache.next)
        };
        if let Some(metrics) = &self.metrics {
            metrics.set_tso_batch_remaining(remaining);
        }
    }

    fn refill_threshold(&self) -> u64 {
        u64::from(self.batch_size)
            .div_ceil(TSO_REFILL_THRESHOLD_DIVISOR)
            .max(1)
    }

    fn needs_refill(&self) -> bool {
        let cache = self.cache.lock();
        cache.end.saturating_sub(cache.next) <= self.refill_threshold()
    }

    /// 从缓存领取一个值；锁内不跨 `await`。
    fn claim(&self) -> Option<u64> {
        let (ts, remaining) = {
            let mut cache = self.cache.lock();
            if cache.next >= cache.end {
                return None;
            }
            let ts = cache.next;
            cache.next += 1;
            (ts, cache.end - cache.next)
        };
        if let Some(metrics) = &self.metrics {
            metrics.set_tso_current(ts);
            metrics.set_tso_batch_remaining(remaining);
        }
        Some(ts)
    }

    /// 丢弃当前批剩余值（换 leader 后按 D2 直接丢弃）。
    fn discard_batch(&self) {
        let mut cache = self.cache.lock();
        cache.next = cache.end;
        drop(cache);
        if let Some(metrics) = &self.metrics {
            metrics.set_tso_batch_remaining(0);
        }
    }

    async fn refill(&self) -> Result<()> {
        let _guard = self.refill.lock().await;
        // double-check：等待期间其他任务可能已经换过批。
        if !self.needs_refill() {
            return Ok(());
        }

        let deadline = Instant::now() + TSO_REFILL_DEADLINE;
        let mut backoff = REFILL_BACKOFF_START;
        loop {
            match self.source.allocate(self.batch_size).await {
                Ok(batch) => return self.install_batch(batch),
                Err(err) => {
                    if let Some(metrics) = &self.metrics {
                        metrics.inc_tso_refill_failure();
                    }
                    if matches!(err, Error::NotLeader { .. } | Error::NotLeaderNoHint) {
                        self.discard_batch();
                        self.was_leader.store(false, Ordering::Release);
                        if let Some(metrics) = &self.metrics {
                            metrics.inc_tso_leader_change();
                        }
                        return Err(err);
                    }
                    if !matches!(err, Error::ClusterUnavailable(_)) || Instant::now() >= deadline {
                        return Err(err);
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(REFILL_BACKOFF_MAX);
                }
            }
        }
    }

    fn install_batch(&self, batch: TsoBatch) -> Result<()> {
        let remaining = {
            let mut cache = self.cache.lock();
            if batch.end_ts <= batch.start_ts {
                return Err(Error::Internal(format!(
                    "tso batch is empty: [{}, {}) (would spin without issuing a TSO)",
                    batch.start_ts, batch.end_ts
                )));
            }
            if batch.start_ts < cache.next {
                return Err(Error::Internal(format!(
                    "tso batch regressed: start_ts {} below next {} (refusing to reuse a TSO)",
                    batch.start_ts, cache.next
                )));
            }
            cache.next = batch.start_ts;
            cache.end = batch.end_ts;
            cache.end - cache.next
        };
        if let Some(metrics) = &self.metrics {
            metrics.set_tso_batch_remaining(remaining);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Arc;

    use async_trait::async_trait;

    use super::*;
    use crate::metrics::Metrics;

    /// 可控 TSO 源：高水位用原子计数模拟（真实实现是 region 0 状态机）。
    /// refill 由 `TsoClient` 串行化，因此这里无需并发保护。
    #[derive(Clone)]
    struct FakeSource {
        inner: Arc<FakeInner>,
    }

    struct FakeInner {
        leader: AtomicBool,
        high_water: AtomicU64,
        allocate_calls: AtomicU64,
        failures_left: AtomicU64,
        regressing: AtomicBool,
        empty: AtomicBool,
        first_allocation_done: AtomicBool,
    }

    impl FakeSource {
        fn new() -> Self {
            Self {
                inner: Arc::new(FakeInner {
                    leader: AtomicBool::new(true),
                    high_water: AtomicU64::new(1),
                    allocate_calls: AtomicU64::new(0),
                    failures_left: AtomicU64::new(0),
                    regressing: AtomicBool::new(false),
                    empty: AtomicBool::new(false),
                    first_allocation_done: AtomicBool::new(false),
                }),
            }
        }

        fn regressing() -> Self {
            let source = Self::new();
            source.inner.regressing.store(true, Ordering::SeqCst);
            source
        }

        /// 永远返回空批（模拟实现错误）：客户端必须报错，不得死循环。
        fn empty() -> Self {
            let source = Self::new();
            source.inner.empty.store(true, Ordering::SeqCst);
            source
        }

        fn set_leader(&self, leader: bool) {
            self.inner.leader.store(leader, Ordering::SeqCst);
        }

        fn fail_next_allocations(&self, n: u64) {
            self.inner.failures_left.store(n, Ordering::SeqCst);
        }

        fn allocate_calls(&self) -> u64 {
            self.inner.allocate_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl TsoSource for FakeSource {
        async fn is_leader(&self) -> bool {
            self.inner.leader.load(Ordering::SeqCst)
        }

        async fn allocate(&self, batch_size: u32) -> Result<TsoBatch> {
            self.inner.allocate_calls.fetch_add(1, Ordering::SeqCst);
            let failures = self.inner.failures_left.load(Ordering::SeqCst);
            if failures > 0 {
                self.inner
                    .failures_left
                    .store(failures - 1, Ordering::SeqCst);
                return Err(Error::ClusterUnavailable("fake election window".into()));
            }
            if self.inner.empty.load(Ordering::SeqCst) {
                return Ok(TsoBatch {
                    start_ts: 1,
                    end_ts: 1,
                });
            }
            if self.inner.regressing.load(Ordering::SeqCst)
                && self
                    .inner
                    .first_allocation_done
                    .swap(true, Ordering::SeqCst)
            {
                return Ok(TsoBatch {
                    start_ts: 1,
                    end_ts: 2,
                });
            }
            let start = self.inner.high_water.load(Ordering::SeqCst);
            let end = start + u64::from(batch_size);
            self.inner.high_water.store(end, Ordering::SeqCst);
            Ok(TsoBatch {
                start_ts: start,
                end_ts: end,
            })
        }
    }

    #[test]
    fn test_normalize_batch_size_bounds() {
        assert_eq!(normalize_batch_size(0, 1024).unwrap(), 1);
        assert_eq!(normalize_batch_size(1024, 1024).unwrap(), 1024);
        assert!(matches!(
            normalize_batch_size(1025, 1024),
            Err(coord_core::error::Error::InvalidArgument(_))
        ));
    }

    #[tokio::test]
    async fn test_tso_client_serves_from_local_batch() {
        let source = FakeSource::new();
        let client = TsoClient::new(Arc::new(source.clone()), 64, None);
        let mut issued = Vec::new();
        for _ in 0..10 {
            issued.push(client.get_ts().await.unwrap());
        }
        assert_eq!(issued, (1..=10).collect::<Vec<_>>());
        assert_eq!(source.allocate_calls(), 1, "一批 64 个应只过一次 raft");
    }

    #[tokio::test]
    async fn test_get_ts_above_skips_values_at_or_below_floor() {
        let source = FakeSource::new();
        let client = TsoClient::new(Arc::new(source.clone()), 8, None);

        assert_eq!(client.get_ts_above(5).await.unwrap(), 6);
        assert_eq!(client.cached_range(), (7, 9));
        assert_eq!(source.allocate_calls(), 1, "同一批内跳值不应重新申请");

        assert_eq!(client.get_ts_above(20).await.unwrap(), 21);
        assert!(client.cached_range().0 > 20);
    }

    #[tokio::test]
    async fn test_tso_client_concurrent_allocations_are_unique() {
        let source = FakeSource::new();
        let client = TsoClient::new(Arc::new(source.clone()), 64, None);

        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..1024 {
            let client = Arc::clone(&client);
            tasks.spawn(async move { client.get_ts().await.unwrap() });
        }
        let mut issued = Vec::new();
        while let Some(result) = tasks.join_next().await {
            issued.push(result.unwrap());
        }
        issued.sort_unstable();
        let before = issued.len();
        issued.dedup();
        assert_eq!(before, 1024, "并发发放必须发放全部 1024 个 TSO");
        assert_eq!(issued.len(), 1024, "并发发放出现重复 TSO");
        assert!(
            source.allocate_calls() <= 32,
            "本地缓存未生效：1024 次发放申请了 {} 个批次",
            source.allocate_calls()
        );
    }

    #[tokio::test]
    async fn test_tso_client_discards_batch_after_leader_loss() {
        let source = FakeSource::new();
        let metrics = Arc::new(Metrics::new());
        let client = TsoClient::new(Arc::new(source.clone()), 8, Some(Arc::clone(&metrics)));

        let first = client.get_ts().await.unwrap();
        let (_, old_end) = client.cached_range();
        // batch=8 → 阈值 1；消费到只剩 1 个但未耗尽
        for _ in 0..6 {
            client.get_ts().await.unwrap();
        }
        assert_eq!(client.cached_range().1 - client.cached_range().0, 1);

        source.set_leader(false);
        let err = client
            .get_ts()
            .await
            .expect_err("follower must not serve cached TSO");
        assert!(matches!(err, Error::NotLeader { .. }));
        assert!(
            metrics
                .render_prometheus_text()
                .contains("coord_tso_leader_changes_total 1"),
            "leader 丢失必须记一次 leader change"
        );

        source.set_leader(true);
        let next = client.get_ts().await.unwrap();
        assert!(
            next > first && next >= old_end,
            "旧批剩余值不得再发放: first={first} next={next} old_end={old_end}"
        );
    }

    #[tokio::test]
    async fn test_tso_client_retries_transient_refill_failures() {
        let source = FakeSource::new();
        source.fail_next_allocations(3);
        let metrics = Arc::new(Metrics::new());
        let client = TsoClient::new(Arc::new(source.clone()), 8, Some(Arc::clone(&metrics)));

        let ts = client.get_ts().await.expect("选举窗口重试后必须成功");
        assert!(ts > 0);
        assert!(
            metrics
                .render_prometheus_text()
                .contains("coord_tso_refill_failures_total 3"),
            "每次失败尝试都要计入 refill failures"
        );
    }

    #[tokio::test]
    async fn test_tso_client_rejects_regressing_batch() {
        let source = FakeSource::regressing();
        let client = TsoClient::new(Arc::new(source.clone()), 8, None);
        for _ in 0..7 {
            client.get_ts().await.unwrap();
        }
        let err = client
            .get_ts()
            .await
            .expect_err("regressing batch must fail closed");
        assert!(
            matches!(err, Error::Internal(ref message) if message.contains("regressed")),
            "expected Internal(tso batch regressed), got {err}"
        );
    }

    #[tokio::test]
    async fn test_tso_client_rejects_empty_batch() {
        let source = FakeSource::empty();
        let client = TsoClient::new(Arc::new(source), 8, None);
        let err = client
            .get_ts()
            .await
            .expect_err("empty batch must fail closed, not spin");
        assert!(
            matches!(err, Error::Internal(ref message) if message.contains("empty")),
            "expected Internal(tso batch is empty), got {err}"
        );
    }
}
