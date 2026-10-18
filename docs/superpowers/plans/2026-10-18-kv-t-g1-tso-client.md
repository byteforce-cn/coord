# KV-T G1 TSO 客户端批量发放器 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 落地 issue #137——在 server 进程内提供低开销、并发安全的 TSO 发放器：本地缓存 `[next, end)`，低于阈值预取，耗尽前阻塞，失去 leadership 时丢弃未用完的批，并把 `coord.tso.v2/GetTs` 从「每次过 raft」切到本地缓存。

**Architecture:** `TsoClient` 包一个 `TsoSource`（生产实现是 `TsoAllocator`，测试用可控 fake）。每个 `get_ts` 先确认本节点仍是 region 0 leader（follower 不得从本地缓存发放），再从 `parking_lot::Mutex<Cache>` 里领一个号；剩余量低于 `batch_size/8` 时用 `tokio::sync::Mutex` 串行化一次 refill（`TsoAllocator::allocate`），把整批换成 `[start, end)`——旧批未用完的部分直接丢弃（ADR-0015 D2 允许空洞）。`CoordNode` 只在 `[kv_t].enabled=true` 且 region 0 raft 已装配时由 `main.rs` 注入客户端；关闭时不构造、不起后台任务。

**Tech Stack:** Rust、tokio、parking_lot、async-trait、openraft。

**Spec:** `docs/adr/0015-kv-t.md` D2/D9 + issue #137；已有接口见 `coord-server/src/raft/tso.rs`（#136）。

## Global Constraints

- `[kv_t].enabled=false`（默认）不构造 `TsoClient`、不新增后台任务、不改变启动与 v1 行为（ADR-0001）。
- TSO 只增不减、允许空洞、**绝不复用**；换 leader/重启后未用完的批直接丢弃，新批 `start_ts` 必须大于所有已发放值。
- follower 的 `GetTs` 返回 `NOT_LEADER`；不得从本地缓存发放导致双份高水位。
- 客户端必须 fail-closed：新批 `start_ts` 低于本地 `next` 视为实现错误（`Internal`），不得静默发放。
- 指标名固定：`coord_tso_current`、`coord_tso_batch_remaining`、`coord_tso_refill_failures_total`、`coord_tso_leader_changes_total`。
- 注释只写当前「为什么」；不新增 TODO/FIXME；临时文件只在 `/tmp`；文档引用 0 悬空。
- 提交前本地跑：`cargo fmt --all -- --check`、`cargo clippy --workspace --lib --bins -- -D warnings`、`bash scripts/check-panics.sh`、`cargo test`、`cargo audit`、`cargo deny check`、`bash scripts/check-doc-refs.sh`。

## Review Focus

1. **并发重复**：1000+ 并发 `get_ts` 不得出现重复/倒退；领取路径不能跨 `await` 持锁。
2. **leader 切换**：失去 leadership 后旧批剩余值不得再发放；`tso_leader_changes_total` 只计一次跃迁。
3. **refill 竞争**：多任务同时触发 refill 时只允许一次 raft round-trip；等待者醒来后不得重复申请。
4. **失败语义**：选举窗口（`ClusterUnavailable`）有界重试后返回错误，不无限挂起；`NotLeader` 立即上抛。
5. **fail-closed**：回退批次必须报错而不是发放重复值。

---

### Task 1: 指标槽位与 `render_prometheus_text`

**Files:**
- Modify: `coord-server/src/metrics.rs`（`MetricsInner` + setter + render + tests）

**Interfaces:**
- Produces:
  - `pub fn set_tso_current(&self, ts: u64)`
  - `pub fn set_tso_batch_remaining(&self, remaining: u64)`
  - `pub fn inc_tso_refill_failure(&self)`
  - `pub fn inc_tso_leader_change(&self)`

- [ ] **Step 1: 写失败测试**（`metrics.rs` tests）

```rust
#[test]
fn test_metrics_kv_t_tso_gauges_and_counters() {
    let m = Metrics::new();
    m.set_tso_current(4242);
    m.set_tso_batch_remaining(17);
    m.inc_tso_refill_failure();
    m.inc_tso_leader_change();
    let output = m.render_prometheus_text();
    assert!(output.contains("coord_tso_current 4242"));
    assert!(output.contains("coord_tso_batch_remaining 17"));
    assert!(output.contains("coord_tso_refill_failures_total 1"));
    assert!(output.contains("coord_tso_leader_changes_total 1"));
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server --lib metrics::tests::test_metrics_kv_t_tso`
Expected: FAIL（方法不存在）

- [ ] **Step 3: 实现**

- `MetricsInner` 增加 4 个 `AtomicU64`（默认 0）。
- setter/inc 用 `Ordering::Relaxed`（与既有 KV-T gauge 一致）。
- `render_prometheus_text` 在 `coord_kv_t_enabled` 之后输出四个指标（gauge/counter 各带 `# HELP`/`# TYPE`）。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server --lib metrics::tests::test_metrics_kv_t_tso`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add coord-server/src/metrics.rs
git commit -m "feat(kv-t): add TSO client metrics gauges and counters (#137)"
```

### Task 2: `TsoClient` 本地批次缓存

**Files:**
- Modify: `coord-server/src/raft/tso.rs`（`TsoSource` + `TsoClient` + 单测）

**Interfaces:**
- Produces:
  - `pub const TSO_REFILL_THRESHOLD_DIVISOR: u64 = 8;`（阈值 = `max(1, batch_size/8)`）
  - `pub const TSO_REFILL_DEADLINE: Duration = Duration::from_secs(5);`
  - `#[async_trait] pub trait TsoSource: Send + Sync + 'static { async fn is_leader(&self) -> bool; async fn allocate(&self, batch_size: u32) -> Result<TsoBatch>; }`
  - `impl TsoSource for TsoAllocator`
  - `pub struct TsoClient`；`TsoClient::new(Arc<dyn TsoSource>, batch_size: u32, Option<Arc<Metrics>>) -> Arc<Self>`
  - `pub async fn get_ts(&self) -> Result<u64>`
  - `pub fn cached_range(&self) -> (u64, u64)`（诊断/测试只读快照）

- [ ] **Step 1: 写失败测试**（`raft/tso.rs` tests，fake source 用 `AtomicU64` + `AtomicBool`）

```rust
#[tokio::test]
async fn test_tso_client_serves_from_local_batch() {
    let src = FakeSource::new(64);
    let client = TsoClient::new(Arc::new(src.clone()), 64, None);
    for _ in 0..10 { client.get_ts().await.unwrap(); }
    assert_eq!(src.allocate_calls(), 1, "一批 64 个应只过一次 raft");
}

#[tokio::test]
async fn test_tso_client_concurrent_allocations_are_unique() {
    let src = FakeSource::new(64);
    let client = TsoClient::new(Arc::new(src.clone()), 64, None);
    let mut handles = Vec::new();
    for _ in 0..1024 {
        let c = Arc::clone(&client);
        handles.push(tokio::spawn(async move { c.get_ts().await.unwrap() }));
    }
    let mut ts = Vec::new();
    for h in handles { ts.push(h.await.unwrap()); }
    ts.sort_unstable();
    ts.dedup();
    assert_eq!(ts.len(), 1024, "并发发放出现重复 TSO");
    assert!(src.allocate_calls() <= 1024 / 8 + 1);
}

#[tokio::test]
async fn test_tso_client_discards_batch_after_leader_loss() {
    let src = FakeSource::new(8);
    let metrics = Arc::new(Metrics::new());
    let client = TsoClient::new(Arc::new(src.clone()), 8, Some(Arc::clone(&metrics)));
    let first = client.get_ts().await.unwrap();
    let (_, old_end) = client.cached_range();
    // 消费到阈值以下但未耗尽（batch=8 → 阈值 1）
    for _ in 0..6 { client.get_ts().await.unwrap(); }
    src.set_leader(false);
    let err = client.get_ts().await.expect_err("follower must not serve cached TSO");
    assert!(matches!(err, Error::NotLeader { .. }));
    assert!(metrics.render_prometheus_text().contains("coord_tso_leader_changes_total 1"));
    src.set_leader(true);
    let next = client.get_ts().await.unwrap();
    assert!(next > first && next >= old_end, "旧批剩余值不得再发放: next={next} old_end={old_end}");
}

#[tokio::test]
async fn test_tso_client_retries_transient_refill_failures() {
    let src = FakeSource::new(8);
    src.fail_next_allocations(3); // ClusterUnavailable
    let metrics = Arc::new(Metrics::new());
    let client = TsoClient::new(Arc::new(src.clone()), 8, Some(Arc::clone(&metrics)));
    assert!(client.get_ts().await.unwrap() > 0);
    assert!(metrics.render_prometheus_text().contains("coord_tso_refill_failures_total 3"));
}

#[tokio::test]
async fn test_tso_client_rejects_regressing_batch() {
    let src = FakeSource::new(8).with_regressing_batches(); // 第二批 start 小于已发放值
    let client = TsoClient::new(Arc::new(src.clone()), 8, None);
    for _ in 0..8 { client.get_ts().await.unwrap(); }
    let err = client.get_ts().await.expect_err("regressing batch must fail closed");
    assert!(matches!(err, Error::Internal(_)));
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server --lib raft::tso`
Expected: FAIL（`TsoClient`/`TsoSource` 不存在）

- [ ] **Step 3: 实现**

- `TsoClient::get_ts`：先 `source.is_leader().await`；非 leader → `was_leader.swap(false)` 为真时 `discard_cache()` + `inc_tso_leader_change()`，返回 `Error::NotLeader { leader_addr: None }`；leader 则 `was_leader.store(true)`。
- 领取：`parking_lot::Mutex<Cache{next,end}>`；`next < end` 时取 `next`、`next += 1`，更新 `set_tso_current` / `set_tso_batch_remaining`；锁内不跨 `await`。
- 预取：剩余量 `<= max(1, batch_size/8)` 时进入 refill；`tokio::sync::Mutex` 串行化，拿到锁后 double-check `needs_refill`（别人已换批则直接返回）。
- refill：`source.allocate(batch_size)`；成功时若 `batch.start_ts < cache.next` → `Error::Internal("tso batch regressed")`（fail-closed）；否则整体替换 `next/end`（旧批剩余丢弃）。
- 失败：`ClusterUnavailable` 记 `inc_tso_refill_failure()`，退避 10ms→200ms，直到 `TSO_REFILL_DEADLINE`（5s）后返回最后一个错误；`NotLeader` 记一次失败后立即上抛并丢弃缓存。
- `impl TsoSource for TsoAllocator`：`is_leader` = `raft.current_leader().await == Some(node_id)`。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server --lib raft::tso`
Expected: PASS（含 #136 的 `test_normalize_batch_size_bounds`）

- [ ] **Step 5: Commit**

```bash
git add coord-server/src/raft/tso.rs
git commit -m "feat(kv-t): cache TSO batches in-process with leader-loss discard (#137)"
```

### Task 3: `GetTs` 走本地缓存 + 启动装配

**Files:**
- Modify: `coord-server/src/server/mod.rs`（`tso_client` 字段 + setter/getter）
- Modify: `coord-server/src/kvt/services.rs`（`GetTs`）
- Modify: `coord/src/main.rs`（enabled 且 raft 存在时注入）
- Modify: `coord/tests/kv_t_g1_tso_rpc_test.rs`（fake source 注入的 RPC 测试）

**Interfaces:**
- Produces: `CoordNode::set_tso_client(&mut self, Arc<TsoClient>)`；`CoordNode::tso_client(&self) -> Option<&Arc<TsoClient>>`
- Consumes: Task 2 的 `TsoClient::get_ts`

- [ ] **Step 1: 写失败测试**（`coord/tests/kv_t_g1_tso_rpc_test.rs`）

```rust
#[tokio::test]
async fn test_get_ts_served_from_local_batch() {
    let server = start_server_with_tso_client(true).await;
    let mut client = client(server.addr).await;
    let a = client.get_ts(GetTsRequest {}).await.unwrap().into_inner().ts;
    let b = client.get_ts(GetTsRequest {}).await.unwrap().into_inner().ts;
    assert!(b > a);
    assert_eq!(server.allocate_calls(), 1, "GetTs 不应每次过 raft");
}

#[tokio::test]
async fn test_get_ts_on_follower_is_not_leader() {
    let server = start_server_with_tso_client(false).await; // fake source 非 leader
    let err = client(server.addr).await.get_ts(GetTsRequest {}).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition); // 或 NotLeader，按 attach 后的映射断言
}
```

  - 第二个用例的真实码：`tso_status` 把 `NotLeader` 映射为 `NOT_LEADER` 结构化码；断言 `error_code_of(&err) == Some("NOT_LEADER")`，不要断言文案。
  - `start_server_with_tso_client(enabled_leader: bool)` 复用现有 `start_server` 的装配，注入 `TsoClient::new(Arc::new(FakeSource), 8, None)`；fake source 定义放在测试文件内（`TsoSource` 已是公开接口）。

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord --test kv_t_g1_tso_rpc_test`
Expected: FAIL（`set_tso_client` 不存在 / GetTs 未走缓存）

- [ ] **Step 3: 实现**

- `CoordNode` 增加 `tso_client: Option<Arc<TsoClient>>`（`new()` 置 `None`）、setter（`&mut self`，`Arc::new(node)` 之前）与 getter。
- `GetTs`：gate 后取 `self.tso_client()`；缺失 → `FAILED_PRECONDITION` 且消息含 `region 0 raft`（沿用 #136 的装配缺失口径）；`client.get_ts().await` 经 `tso_status` 映射。
- `main.rs`：`node.set_kvt_state(...)` 之后、`Arc::new(node)` 之前：
  ```rust
  if cfg.kv_t.enabled {
      if let Some(raft) = node.raft.as_ref() {
          node.set_tso_client(TsoClient::new(
              Arc::new(TsoAllocator::new(raft.as_ref().clone(), node.node_id)),
              cfg.kv_t.tso_batch_size,
              Some(Arc::clone(&metrics)),
          ));
      }
  }
  ```
- `Allocate` RPC 保持直连状态机（显式批请求不读缓存）。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord --test kv_t_g1_tso_rpc_test && cargo test -p coord-server --lib kvt`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add coord-server/src/server/mod.rs coord-server/src/kvt/services.rs coord/src/main.rs coord/tests/kv_t_g1_tso_rpc_test.rs
git commit -m "feat(kv-t): serve GetTs from the local TSO batch cache (#137)"
```

### Task 4: 多节点验收 + 全量门禁

**Files:**
- Modify: `coord/tests/cluster_test.rs`

- [ ] **Step 1: 写失败测试**

```rust
#[tokio::test(flavor = "multi_thread")]
async fn test_tso_client_batches_and_never_reuses_across_failover() {
    // 1. 3 节点集群，找 leader；客户端 batch_size=64 注入 leader 的 raft
    // 2. 200 个并发 get_ts：全部唯一；本地缓存生效（高水位只前进少量批次）
    // 3. kill leader；新 leader 上新建客户端；首个 ts > 旧客户端发出的最大值
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord --test cluster_test test_tso_client_batches -- --nocapture`
Expected: FAIL（`TsoClient` 尚未在测试中可用）

- [ ] **Step 3: 实现测试装配**

- `TsoClient::new(Arc::new(TsoAllocator::new(node.raft.as_ref().clone(), node.node_id)), 64, None)`。
- 新高水位从 `node.node.storage.tso_high_water()` 读；沿用 `wait_for_any_leader` 与 release-raft 句柄（`drop` 客户端/分配器）后 `kill()` 的模式，避免杀人后心跳保活。
- 并发数量取 200（CI 时长约束），单测已覆盖 1024。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord --test cluster_test test_tso_client_batches -- --nocapture`
Expected: PASS

- [ ] **Step 5: 全量门禁 + Commit**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-panics.sh
cargo test -p coord-server --lib
cargo test -p coord --test cluster_test
bash scripts/check-doc-refs.sh
git add coord/tests/cluster_test.rs
git commit -m "test(kv-t): assert TSO client batches and survives leader failover (#137)"
```

## Execution

- 分支：`feat/kv-t-g1-tso-client`；PR 关闭 #137，正文按验收项逐条附证据（测试名/命令/指标名）。
- `cargo test` 全量（含集群测试）在 PR 前本地跑通；CI 全绿后 `gh pr merge --merge --delete-branch`。
- 非范围（留给后续工作项）：`Allocate` RPC 的转发与跨地域 TSO（#129 口径）、`commit_ts` 写路径（#138）、`GetTs` 的 batch>1 发放（协议不变）。
