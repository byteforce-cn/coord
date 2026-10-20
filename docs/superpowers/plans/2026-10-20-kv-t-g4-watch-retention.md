# KV-T G4 Watch Retention & GC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Commit Log 按 `watch_retention`（默认 24h，可配 1h–7d）整 shard 回收；超出窗口的 Watch 起点返回 `HISTORY_UNAVAILABLE` 并给出最老可用位点，不静默丢事件。

**Architecture:** region 0 的 shard 表新增单调 `gc_before_ts`（`commit_ts < gc_before_ts` 已不可读），由 region 0 leader 在 `tick` 里只在「shard 已 seal、watermark 越过 `end_ts`、`sealed_at_ms + retention` 已到」时通过新 raft 操作 `CommitLogShardOp::Gc` 推进。每个节点按本地已 apply 的表回收本地 shard 组（shutdown → 移除运行时 → 删除数据目录）；读取端在解析起点/续订 cursor 时用同一水位判定 `HISTORY_UNAVAILABLE`。

**Tech Stack:** Rust, tokio, openraft（region 0 单组），复用 `CommitLogShardManager` / `CommitLogShardTable` / `WatchCoordinator`。

**Spec:** `docs/adr/0015-kv-t.md` D5/D9/D11.7；issue #157（前置 #154、#155、#156）。

## Global Constraints

- `[kv_t].enabled=false`（默认）时不得改变 v1 行为；新代码只在 v2 装配路径可达（ADR-0001）。
- 保留窗口读 `[kv_t].watch_retention`（秒；默认 86400，配置校验 3600–604800），不新增第二份配置。
- 回收粒度是**整个 shard**：只回收 `end_ts != 0 && sealed_at_ms != 0 && watermark >= end_ts && sealed_at_ms + retention <= now` 的 shard；未 seal / 未定界 / watermark 未越过的 shard 一律不回收。
- `gc_before_ts` 单调递增、只能等于某个 shard 的 `end_ts`；`commit_ts < gc_before_ts` 的事件不可再读出。
- 同一 `commit_ts` 的全部 Region 分片仍是一个原子事件；回收只影响起点可用性，不改变已返回事件的语义。
- 注释只写当前「为什么」（不变量/陷阱/契约），不写日期、变更史或一次性任务号；不新增 TODO/FIXME。
- 临时文件只放 `/tmp`。
- 提交前本机必须跑通 `cargo fmt --all -- --check`、`cargo clippy --workspace --lib --bins -- -D warnings`、`bash scripts/check-panics.sh`、`cargo audit`、`cargo deny check`；涉及文档跑 `bash scripts/check-doc-refs.sh`。

## Ledger of decisions

- **Ruling:** 回收后保留 shard 元数据行，只推进表上的 `gc_before_ts` — shard id 保持连续、`shard_for_ts` 与 cursor 解析仍能按 commit_ts 定位；代价是元数据行永久累积（每 shard 至少 1 小时一个，可忽略）。
- **Ruling:** 物理回收 = shutdown 该 shard 的 raft 组、从本节点运行时表移除、删除其数据目录；`tick` / `local_watermark` / `ensure_runtime` 都跳过已回收 shard，避免下一次 tick 把它空组重新拉起来 — 代价是回收后该 shard 在本节点不可再读（这正是语义），且依赖 openraft `shutdown()` 能及时停止任务。
- **Ruling:** 可用性判定只看「第一个未读事件」是否还在：普通 cursor 是 `commit_ts + 1`；`commit_ts < shard.start_ts`（含跨 shard 后返回的 `{shard_id, 0}`）取 `shard.start_ts`。只看 `commit_ts + 1` 会把已追平、停在 shard 起点的订阅误判成越界。被过滤掉的事件同样算「可读」，因此过滤条件不参与判定。
- **Ruling:** 不新增 proto 字段；`WatchError::HistoryUnavailable` 携带最老可用 `WatchCursor`，`WatchBatch` 同样回显 — #158 的 RPC 层决定如何把它放到 `OUT_OF_RANGE` + `HISTORY_UNAVAILABLE` 上。
- **Ruling:** 时间锚点用 `sealed_at_ms`（seal 完成时刻），并要求回收时 `watermark >= end_ts` — 超时 seal 可能发生在 watermark 越过之前，仅凭 `sealed_at_ms` 不能证明「不会再有迟到记录」。

## Review Focus

- **窗口边界**：`now == sealed_at_ms + retention` 时必须已经可回收（含端点），差 1ms 不回收；两个方向都要有断言。
- **GC 不破坏未完成 shard**：超时 seal 但 `watermark < end_ts` 的 shard 不得回收；未定界 shard 永不回收；回收后其它 shard 的 seal/搬运不受影响。
- **重启后不复活**：GC 后重启（新的读取端/新的协调器视图）不得重新拉起已回收 shard 的空 raft 组，也不得把旧 cursor 当成新历史。
- **cursor 边界**：`commit_ts = gc_before_ts - 1` 的 cursor 必须仍可续订（第一个未读事件正好是最老保留事件）；`gc_before_ts - 2` 必须报 `HISTORY_UNAVAILABLE`。
- **同事务原子性**：回收只发生在 shard 边界，不可能切开一个 `commit_ts`；已回收 shard 的末尾事件不会因跨 shard 推进被跳过（sealed + watermark 是跨 shard 前提）。

---

### Task 1: shard 表的 GC 水位与回收判定

**Files:**
- Modify: `coord-server/src/storage/commit_log_shard.rs`
- Modify: `coord-server/src/raft/type_config.rs`（`CommitLogShardOp::Gc`）
- Modify: `coord-server/src/storage/mvcc.rs`（`apply_commit_log_shard_op` 新分支）

**Interfaces:**
- Produces:
  - `CommitLogShardTable { shards: Vec<CommitLogShardMeta>, pub gc_before_ts: u64 }`（`Default` = 0）
  - `CommitLogShardTable::advance_gc(&mut self, through_ts: u64) -> Result<bool>`
  - `CommitLogShardTable::is_reclaimed(&self, meta: &CommitLogShardMeta) -> bool`
  - `pub fn gc_due(meta: &CommitLogShardMeta, watermark: u64, now_ms: u64, retention_secs: u64) -> bool`
  - `pub fn gc_boundary(table: &CommitLogShardTable, watermark: u64, now_ms: u64, retention_secs: u64) -> Option<u64>`
  - `CommitLogShardOp::Gc { through_ts: u64 }`
- 语义：
  - `advance_gc` 只接受「某个已 seal shard 的 `end_ts`」，且 `through_ts > gc_before_ts`；同值/更小值是 no-op（幂等），非边界/有未 seal 的 shard 落在边界内 → `Err`（apply 内降级为 no-op）；`validate()` 要求 `gc_before_ts` 为 0 或等于某个 shard 的 `end_ts`。
  - `gc_due` = 已定界 + 已 seal + `watermark >= end_ts` + `now_ms >= sealed_at_ms + retention_secs * 1000`。
  - `gc_boundary` 从 `gc_before_ts` 之后第一个 shard 起连续回收，遇到第一个不满足的 shard 即停（不允许跨洞回收）；无候选返回 `None`。

- [ ] **Step 1: 写失败测试（纯函数 + apply 分支）**

在 `commit_log_shard.rs` 的 `#[cfg(test)] mod tests`：

```rust
#[test]
fn gc_requires_seal_completeness_and_the_retention_window() {
    let sealed = CommitLogShardMeta {
        shard_id: 1,
        start_ts: 1,
        end_ts: 101,
        opened_at_ms: 0,
        ended_at_ms: 1_000,
        sealed_at_ms: 10_000,
    };
    assert!(!gc_due(&sealed, 100, 10_060_000, 60), "watermark must pass end_ts");
    assert!(!gc_due(&sealed, 101, 10_059_999, 60), "window must elapse");
    assert!(gc_due(&sealed, 101, 10_060_000, 60), "boundary is inclusive");
    let unsealed = CommitLogShardMeta { sealed_at_ms: 0, ..sealed.clone() };
    assert!(!gc_due(&unsealed, 1_000, 10_000_000, 60));
    let unbounded = CommitLogShardMeta { end_ts: 0, ..sealed.clone() };
    assert!(!gc_due(&unbounded, 1_000, 10_000_000, 60));
}
```

在 `mvcc.rs` 的 shard 元数据测试旁新增：

```rust
#[test]
fn test_apply_commit_log_gc_is_monotonic_and_structural() {
    // open 1 + set_end + open 2 + seal 1，然后 apply Gc(101)。
    // 断言：表 gc_before_ts == 101；再 apply Gc(101) 不报错也不变；
    // apply Gc(150)（非边界）被降级为 no-op；未 seal 的 shard 2 被 Gc(201) 拒绝。
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server --lib commit_log_shard::tests`
Expected: FAIL（`gc_due` 不存在）

- [ ] **Step 3: 实现表字段、判定函数与 apply 分支**

`CommitLogShardTable` 加 `pub gc_before_ts: u64`（`Default` 0；`validate` 校验边界）。`advance_gc` 的 `through_ts <= self.gc_before_ts` 返回 `Ok(false)`；找不到 `end_ts == through_ts` 或该边界内有未 seal 的 shard 返回 `Err(Error::InvalidArgument(...))`。`is_reclaimed` 只认 `end_ts != 0 && end_ts <= gc_before_ts`。`gc_boundary` 用 `is_reclaimed` 跳过已回收 shard，遇到 `end_ts == 0` 或 `!gc_due` 立即停止。

`CommitLogShardOp` 增加 `Gc { through_ts: u64 }`；`mvcc::apply_commit_log_shard_op` 的 match 增加 `Gc => table.advance_gc(*through_ts)`（沿用现有的「Err 只告警、表不变」逻辑）。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server --lib commit_log_shard::tests && cargo test -p coord-server --lib storage::mvcc::tests::test_apply_commit_log_gc`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add coord-server/src/storage/commit_log_shard.rs coord-server/src/raft/type_config.rs coord-server/src/storage/mvcc.rs
git commit -m "feat(kv-t): add commit log gc watermark to the shard table"
```

---

### Task 2: Watch 起点/续订的 HISTORY_UNAVAILABLE 与最老可用位点

**Files:**
- Modify: `coord-server/src/kvt/watch.rs`

**Interfaces:**
- Produces:
  - `WatchCursor` 继续 `(shard_id, commit_ts)`；新增 `impl Display`
  - `WatchError::HistoryUnavailable { requested: u64, oldest_available: WatchCursor }`
  - `WatchBatch.oldest_available: WatchCursor`
  - `pub fn oldest_available(table: &CommitLogShardTable) -> WatchCursor`
  - 私有 `fn ensure_available(table: &CommitLogShardTable, cursor: WatchCursor) -> Result<(), WatchError>`
- 语义：
  - 可用性：第一个未读事件（`commit_ts + 1`；`commit_ts` 在 shard 起点之前时取 `start_ts`）`>= table.gc_before_ts`；`gc_before_ts == 0` 恒可用。
  - `oldest_available` = `position_before(table, gc_before_ts - 1)`；`gc_before_ts == 0` = `WatchCursor::beginning()`。
  - `start()` 解析出 cursor 后、`poll()` 读取前都要 `ensure_available`；`poll` 返回的批携带 `oldest_available`。
  - 回归：跨 shard 后 poll 返回的 `{shard_id, 0}` 必须仍可续订（第一个保留事件就是该 shard 起点）。
  - `poll` 读 shard 期间若 GC 刚好推进（`ensure_runtime` 拒绝），必须重读表并转成 `HistoryUnavailable`，不得把内部错误泄露给调用方。

- [ ] **Step 1: 写失败测试**

在 `watch.rs` 的 `#[cfg(test)] mod tests`（`two_shards()` 需补 `gc_before_ts: 0`）：

```rust
#[test]
fn history_before_the_gc_boundary_is_unavailable() {
    let table = CommitLogShardTable {
        shards: vec![shard(1, 1, 101), shard(2, 101, 0)],
        gc_before_ts: 101,
    };
    assert!(matches!(
        ensure_available(&table, WatchCursor::beginning()),
        Err(WatchError::HistoryUnavailable { requested: 1, .. })
    ));
    let err = ensure_available(&table, WatchCursor { shard_id: 1, commit_ts: 99 }).unwrap_err();
    match err {
        WatchError::HistoryUnavailable { requested, oldest_available } => {
            assert_eq!(requested, 100);
            assert_eq!(oldest_available, WatchCursor { shard_id: 1, commit_ts: 100 });
        }
        other => panic!("unexpected error: {other}"),
    }
    assert!(ensure_available(&table, WatchCursor { shard_id: 1, commit_ts: 100 }).is_ok());
    assert!(ensure_available(&table, WatchCursor { shard_id: 2, commit_ts: 100 }).is_ok());
}

#[test]
fn oldest_available_points_at_the_last_reclaimed_ts() {
    assert_eq!(oldest_available(&CommitLogShardTable::default()), WatchCursor::beginning());
    let table = CommitLogShardTable {
        shards: vec![shard(1, 1, 101), shard(2, 101, 0)],
        gc_before_ts: 101,
    };
    assert_eq!(oldest_available(&table), WatchCursor { shard_id: 1, commit_ts: 100 });
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server --lib kvt::watch::tests`
Expected: FAIL（`ensure_available`/`oldest_available` 不存在）

- [ ] **Step 3: 实现错误、判定与调用点**

`start()`：`validate_start` → 解析 cursor（`start_cursor` 或 `position_before`）→ `ensure_available`。`poll()`：读表后先 `ensure_available`，批结果填 `oldest_available`。`scan_shard` 的读取错误在 `poll` 里包裹：出错后重读 `self.shards.table()`，若 `ensure_available` 现在失败则返回 `HistoryUnavailable`，否则保留原错误。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server --lib kvt::watch::tests`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add coord-server/src/kvt/watch.rs
git commit -m "feat(kv-t): report HISTORY_UNAVAILABLE for reclaimed watch history"
```

---

### Task 3: 协调器的 GC 提案与本地 shard 组回收

**Files:**
- Modify: `coord-server/src/kvt/commit_log_shards.rs`
- Modify: `coord-server/src/metrics.rs`

**Interfaces:**
- Consumes: Task 1 的 `gc_due`/`gc_boundary`/`is_reclaimed`/`CommitLogShardOp::Gc`；Task 2 的 watch 可用性（无需改动）。
- Produces:
  - `CommitLogShardManager::gc_local(&self, table: &CommitLogShardTable) -> Result<u64, String>`：返回本轮本地实际回收的 shard 数
  - `CommitLogShardManager::advance_gc_metadata(&self, watermark: u64) -> Result<Option<u64>, String>`
  - `CommitLogShardTick { gc_before_ts: u64, gc_shards: u64 }`
  - `Metrics::set_kv_t_commit_log_gc_before(ts)`、`Metrics::add_kv_t_commit_log_shards_gc(shards)`
  - 指标：`coord_kv_t_commit_log_gc_before`（gauge）、`coord_kv_t_commit_log_shards_gc_total`（counter）

行为：
- `tick()` 顺序：读表 → `gc_local`（清理上一轮已标记的）→ 只为未回收 shard `ensure_runtime`/`ship_shard` → `local_watermark` → region 0 leader：发布 watermark、`advance_metadata`、`advance_gc_metadata` → 重读表 → `gc_local`（让本轮提案立即生效）→ 填 `gc_before_ts`/`gc_shards`。
- `local_watermark()` 与 `ship_shard` 跳过已回收 shard；`ensure_runtime` 对已回收 shard 返回明确错误（不重新拉起空组）。
- `gc_local`：取出运行时 → `raft.shutdown().await` → 从 `runtimes` 移除 → `tokio::fs::remove_dir_all(rt.data_dir)`（目录不存在视为成功）。
- leader 侧 `advance_gc_metadata`：`gc_boundary(...)` 有候选才 `propose(CommitLogShardOp::Gc { through_ts })`。

- [ ] **Step 1: 写失败测试（协调器单测）**

在 `commit_log_shards.rs` 的测试模块新增（复用现有 `ShardManagerFixture`）：

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_reclaims_only_a_sealed_completed_shard_after_retention() {
    // 时钟可注入：open(1) → put/ship 一条 → tick 直到 shard 1 sealed。
    // 1) 窗口未到（sealed_at + retention - 1）tick：gc_before_ts == 0、runtime(1) 仍在。
    // 2) 窗口到点 tick：gc_before_ts == shard1.end_ts、runtime(1) 为 None、数据目录不存在。
    // 3) 再次 tick：不复活（runtime(1) 仍 None），watermark 不回退。
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_never_reclaims_an_unsealed_or_incomplete_shard() {
    // 构造 sealed_at_ms != 0 但 watermark 被一个未搬运记录钉住的 shard：
    // tick 后 gc_before_ts == 0；清掉阻塞后 tick 才推进。
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server --lib kvt::commit_log_shards::tests::gc_`
Expected: FAIL（`gc_before_ts`/`gc_shards` 字段或行为不存在）

- [ ] **Step 3: 实现 tick 集成、本地回收与指标**

按上面的行为实现；`ensure_runtime` 的表检查放在运行时缓存未命中之后、peers 解析之前。`start_shard_manager` 里把 `tick.gc_before_ts`/`tick.gc_shards` 写进指标。`Metrics` 加 gauge/counter 字段、setter/add 方法与 Prometheus 文本输出。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server --lib kvt::commit_log_shards && cargo test -p coord-server --lib metrics::tests::test_metrics_commit_log_gc`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add coord-server/src/kvt/commit_log_shards.rs coord-server/src/metrics.rs
git commit -m "feat(kv-t): reclaim sealed commit log shards past the retention window"
```

---

### Task 4: 端到端验收（窗口边界、HISTORY_UNAVAILABLE、重启、seal 不受影响）

**Files:**
- Modify: `coord/tests/kv_t_g4_watch_coordinator_test.rs`（夹具加可注入时钟 + 保留窗口用例）

**Interfaces:**
- Consumes: `CommitLogShardManager::with_clock`、Task 2 的 `WatchError::HistoryUnavailable`/`oldest_available`、Task 3 的 `gc_before_ts`/`runtime`。
- 夹具改动：`WatchNode::start()` 用 `Arc<AtomicU64>` 注入时钟（初值 1_000_000），测试可 `advance_clock(secs)`；`KvTRuntimeConfig.watch_retention = 60`（测试不需要等 24h）。新增 `wait_until_sealed(shard_id)`。

- [ ] **Step 1: 写失败测试**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_watch_history_unavailable() {
    let fx = WatchNode::start().await;
    fx.open_first_shard().await;
    let first_ts = fx.commit_pairs(&[(b"aa", b"v1")]).await;
    fx.wait_until_sealed(1).await;
    let gc_before = fx.shards.table().unwrap().shards[0].end_ts;
    assert!(gc_before > first_ts);

    // 窗口前 1ms：不回收，历史仍可读。
    fx.advance_clock_ms(59_999).await;
    fx.shards.tick().await.unwrap();
    assert_eq!(fx.shards.table().unwrap().gc_before_ts, 0);
    let reader = WatchCoordinator::new(Arc::clone(&fx.shards));
    let cursor = reader
        .start(WatchStart { start_revision: 1, start_cursor: None })
        .await
        .expect("history is still inside the window");
    let (events, _) = fx.poll_until(&reader, cursor, WatchFilter::all(), 1).await;
    assert_eq!(events[0].commit_ts, first_ts);

    // 窗口到点前提交第二个事务（落在后续 shard 内）。
    let second_ts = fx.commit_pairs(&[(b"bb", b"v2")]).await;

    // 窗口到点：回收 shard 1；seal 状态保留，本地组删除。
    fx.advance_clock_ms(1).await;
    fx.shards.tick().await.unwrap();
    let table = fx.shards.table().unwrap();
    assert_eq!(table.gc_before_ts, gc_before);
    assert_ne!(table.shards[0].sealed_at_ms, 0, "gc must not disturb seal state");
    assert!(fx.shards.runtime(1).is_none(), "reclaimed shard runtime is gone");

    // 旧起点 → HISTORY_UNAVAILABLE；最老可用位点 → 可以续订。
    let err = reader
        .start(WatchStart { start_revision: 1, start_cursor: None })
        .await
        .unwrap_err();
    let oldest = match err {
        WatchError::HistoryUnavailable { oldest_available, .. } => oldest_available,
        other => panic!("unexpected error: {other}"),
    };
    assert!(oldest.commit_ts + 1 >= gc_before);
    let resumed = reader
        .start(WatchStart { start_revision: 0, start_cursor: Some(oldest) })
        .await
        .expect("the oldest available cursor must still resume");
    let (events, _) = fx.poll_until(&reader, resumed, WatchFilter::all(), 1).await;
    assert_eq!(events[0].commit_ts, second_ts, "the oldest cursor must resume at the first retained event");

    // 重启读取端：状态来自 region 0 表，不会复活已回收 shard。
    let restarted = WatchCoordinator::new(Arc::clone(&fx.shards));
    assert!(matches!(
        restarted.start(WatchStart { start_revision: 1, start_cursor: None }).await,
        Err(WatchError::HistoryUnavailable { .. })
    ));
    fx.shards.tick().await.unwrap();
    assert!(fx.shards.runtime(1).is_none(), "tick must not respawn a reclaimed shard");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_gc_keeps_unsealed_shard_and_its_history() {
    // 只 commit 一条、但不让 shard 1 seal（watermark 被未搬运记录钉住）：
    // 时钟越过 retention 后 tick，gc_before_ts == 0，shard 1 的历史仍可读；
    // 放开搬运并 seal 后，下一次 tick 才回收。
}
```



- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord --test kv_t_g4_watch_coordinator_test`
Expected: FAIL（`advance_clock_ms`/`wait_until_sealed`/`HistoryUnavailable` 不存在）

- [ ] **Step 3: 实现夹具与用例**

时钟用 `Arc<AtomicU64>`；`advance_clock_ms` 用 `fetch_add`。`wait_until_sealed(shard_id)` 循环 tick 直到 `table.shards` 中该 shard `sealed_at_ms != 0`（20s 上限）。注意 region 0 leader 的 GC 提案在 seal 之后的下一次 tick 才会发出，测试循环里容忍 `gc_before_ts` 暂时为 0。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord --test kv_t_g4_watch_coordinator_test`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add coord/tests/kv_t_g4_watch_coordinator_test.rs
git commit -m "test(kv-t): cover watch retention window and HISTORY_UNAVAILABLE"
```

---

### Task 5: 文档（保留窗口/GC 的当前口径）

**Files:**
- Modify: `docs/production/consumer-contract.md`（Watch / Commit Log 段）
- Modify: `docs/production/ops/boundaries.md`（B-KVT-4）

内容：
- 保留窗口由 `[kv_t].watch_retention` 决定（1h–7d，默认 24h）；按整 shard 回收，条件是 sealed + `watermark >= end_ts` + `sealed_at_ms + retention` 到点。
- `gc_before_ts` 单调、持久在 region 0；`commit_ts < gc_before_ts` 的事件不可读，起点/续订落到该区间返回 `OUT_OF_RANGE` + `HISTORY_UNAVAILABLE`（客户端全量重同步）；最老可用 cursor 用于诊断/续订。
- 已回收 shard 的元数据行保留（id 连续、按 commit_ts 路由）；本地 raft 组与数据目录删除，重启不复活。
- 容量：MVCC 与 Commit Log 各存一份完整 value，保留窗口直接决定磁盘上界；24h 默认下按写入速率 × 2 × 24h 估算，`gc_before_ts` 与 `coord_kv_t_commit_log_shards_gc_total` 可观测回收进度。
- `docs/production/ops/boundaries.md` 的 B-KVT-4 事实锚点补 `coord-server/src/kvt/commit_log_shards.rs`、`coord-server/src/storage/commit_log_shard.rs` 与指标名。

- [ ] **Step 1: 更新两份文档**

- [ ] **Step 2: 跑引用检查**

Run: `bash scripts/check-doc-refs.sh`
Expected: 悬空路径 0

- [ ] **Step 3: 提交**

```bash
git add docs/production/consumer-contract.md docs/production/ops/boundaries.md
git commit -m "docs(kv-t): document watch retention and gc semantics"
```

---

### Task 6: 门禁与 PR

- [ ] **Step 1: 全量门禁**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-panics.sh
cargo audit
cargo deny check advisories licenses bans sources
cargo test -p coord-server --lib kvt::
cargo test -p coord --test kv_t_g4_watch_coordinator_test
cargo test -p coord --test kv_t_g4_commit_log_shards_test
```

- [ ] **Step 2: 推分支并开 PR**

```bash
git push -u origin feat/kv-t-g4-watch-retention
gh pr create --base main --title "feat(kv-t): Watch 保留窗口与 HISTORY_UNAVAILABLE（#157）" --body-file /tmp/coord-157/pr.md
```

PR 正文覆盖：整 shard GC 与 `gc_before_ts`、不可用判定与最老位点、重启不复活、指标、非范围（RPC/授权归 #158）；Merge Danger 标 one-way（新增 raft 日志变体）。

- [ ] **Step 3: 等 CI 全绿并合并**

```bash
gh pr checks --watch
gh pr merge --merge --delete-branch
```

> 本仓历史用 `--merge`（AGENTS.md 规定 coord 用 `--merge`）。

- [ ] **Step 4: 收尾**

在 issue #157 评论实现口径 + 验收证据（测试名/命令/结果），关闭 issue。
