# KV-T G4 Watch Coordinator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 Commit Log shard 组之上提供按 `commit_ts` 全局有序、同一事务原子批的跨 shard 读取器：cursor 续订、key/range 过滤、`prev_kv`、单批上限与有界背压。

**Architecture:** 新增 `kvt::watch`，只读 region 0 的 shard 表与各 shard 组的 CommitRecord；不新增 RPC（归 #158），不改变 v1 Watch。读取上界取本节点 `local_watermark`（与已发布 watermark 同一 D9 口径，但在副本上只反映本地已 apply 的事实），因此不会因为本地 shard 副本滞后而跳过记录。shard 内按 `commit_ts` 整组读取（`records_after(1)` + `records_at(ts)`），跨 shard 只在「shard 已 seal 且 watermark >= end_ts」后推进；cursor 是 `(shard_id, commit_ts)`，`shard_id=0` 表示首个 shard 之前。

**Tech Stack:** Rust, tokio，复用 `CommitLogShardManager` / `CommitLogShardTable` / `CommitRecord`。

**Spec:** `docs/adr/0015-kv-t.md` D5/D9/D11.7/D12；issue #156（前置 #154、#155）。

## Global Constraints

- `[kv_t].enabled=false`（默认）时不得改变 v1 行为；新代码只在 v2 装配路径可达（ADR-0001）。
- 事件按 `commit_ts` 严格递增；同一 `commit_ts` 的所有 Region 分片必须合并为一个批次，绝不拆成多个事件。
- 单批 ≤1000 条 change 或 1MB；单个事务本身超过上限时不拆批（事务在 Commit 阶段受 4MiB 限制）。
- cursor `(shard_id, commit_ts)`：`commit_ts` 是「已消费到的位置」，续订从 `commit_ts` 之后继续；`shard_id=0` 表示首个 shard 之前。
- 历史窗口/GC/HISTORY_UNAVAILABLE 归 #157；RPC/授权/指标归 #158。本计划不实现它们。
- 注释只写当前「为什么」（不变量/陷阱/契约），不写日期、变更史或一次性任务号；不新增 TODO/FIXME。
- 临时文件只放 `/tmp`。
- 提交前本机必须跑通 `cargo fmt --all -- --check`、`cargo clippy --workspace --lib --bins -- -D warnings`、`bash scripts/check-panics.sh`、`cargo audit`、`cargo deny check`；涉及文档跑 `bash scripts/check-doc-refs.sh`。

## Ledger

- **Ruling:** open shard 也读到 `local_watermark`，跨 shard 才要求 sealed + watermark 越过 end_ts — D9 的 watermark 已承诺「不会再有更小 ts」，只按 sealed 读会让 Watch 最长等 1 小时；代价是 watermark 停滞时 Watch 也停滞（可观测，#155 已有 stall 指标）。
- **Ruling:** 读取上界用本节点 `local_watermark`，不用 region 0 已发布 watermark — 副本的 shard 状态机可能尚未 apply 某条 append，直接用发布值会跳过记录；代价是非数据面节点的 Watch 时延变高，#158 负责选择服务节点/路由。
- **Ruling:** `poll` 是拉模型，不建后台订阅任务 — 背压天然有界（每次最多一批），单批上限由调用方可见；#158 的 RPC 流再套有界通道。

## Review Focus

- **只读已确定的区间**：shard 内上界 = `min(shard.end_ts - 1, local_watermark)`。watermark 承诺「不会再有更小的 commit_ts 记录」，因此 open shard 也能安全读到 watermark；跨 shard 仍只在 `sealed && watermark >= end_ts` 后发生（D9）。
- **副本滞后不能导致丢事件**：不能直接用 region 0 已发布 watermark 作为本地 shard 组的读取上界——副本可能尚未 apply 某条 append。`local_watermark` 由本地 outbox + 本地 shard 已 ship 水位计算，单调但可能更低，天然是本地安全界。
- **同事务原子**：`commit_ts` 的 Region 分片可能跨越 `records_after` 的 limit；必须以 `records_at(ts)` 取整组后再判断批上限，不能按行切批。
- **过滤不改变 cursor 推进**：被过滤掉的事务仍要推进 cursor，否则续订会反复扫描同一段；但过滤绝不能把不匹配的 key 放进事件。
- **cursor 边界**：`start_revision=0` = 当前 watermark+1（即 cursor 落在 watermark）；`start_revision=n` 含 n（cursor 落在 n-1）；两者与 `start_cursor` 互斥。
- **背压有界**：读取是拉模型，单次调用最多返回一批；不维护无界订阅缓冲。`has_more` 只是提示，调用方随时可以停止轮询。

---

### Task 1: Cursor、过滤器与批上限模型

**Files:**
- Create: `coord-server/src/kvt/watch.rs`
- Modify: `coord-server/src/kvt/mod.rs`（`pub mod watch;`）

**Interfaces:**
- Produces:
  - `pub struct WatchCursor { pub shard_id: u64, pub commit_ts: u64 }`：`beginning()`、`is_before_first_shard()`
  - `pub enum EventKindFilter { All, PutsOnly, DeletesOnly }`（Default = All）
  - `pub struct WatchFilter { pub key: Vec<u8>, pub range_end: Vec<u8>, pub kind: EventKindFilter, pub prev_kv: bool }`：`all()`、`validate()`、`matches(&CommitChange)`
  - `pub struct WatchEvent { pub commit_ts: u64, pub changes: Vec<CommitChange> }`
  - `pub struct WatchBatch { pub events, pub cursor, pub watermark, pub sealed, pub has_more }`
  - `pub struct WatchStart { pub start_revision: u64, pub start_cursor: Option<WatchCursor> }`
  - `pub enum WatchError { InvalidRequest(String), CursorUnavailable { shard_id: u64 }, Unavailable(String), Storage(coord_core::error::Error) }`
  - `pub const WATCH_MAX_CHANGES_PER_BATCH: usize = 1000;`
  - `pub const WATCH_MAX_BYTES_PER_BATCH: usize = 1024 * 1024;`
- 过滤语义（与 `coord.watch.v2` 契约一致）：`key` 与 `range_end` 都空 = 全 keyspace；`range_end` 空 = 单 key；否则 `[key, range_end)`。
- `WatchEvent::from_records(commit_ts, records, filter)`：合并同 ts 的 Region 分片、过滤 change、`prev_kv=false` 时清空 prev；返回 `Option<Self>`（过滤后为空 = None）。
- 批上限工具 `BatchBuilder`（`pub(crate)` 或私有 + 单测）：`try_push(event) -> bool`；空批的第一条事件无论多大都接受。

- [x] **Step 1: 写失败测试**

在 `watch.rs` 的 `#[cfg(test)] mod tests` 中：

```rust
#[test]
fn filter_matches_exact_key_range_and_event_kinds() {
    let put = change(b"a", Some(b"v"));
    let del = change(b"b", None);

    assert!(WatchFilter::all().matches(&put));
    assert!(WatchFilter { key: b"a".to_vec(), ..WatchFilter::all() }.matches(&put));
    assert!(!WatchFilter { key: b"a".to_vec(), ..WatchFilter::all() }.matches(&del));

    let range = WatchFilter {
        key: b"a".to_vec(),
        range_end: b"c".to_vec(),
        ..WatchFilter::all()
    };
    assert!(range.matches(&put));
    assert!(range.matches(&del));
    assert!(!range.matches(&change(b"c", Some(b"v"))), "end is exclusive");

    let puts = WatchFilter { kind: EventKindFilter::PutsOnly, ..WatchFilter::all() };
    assert!(puts.matches(&put));
    assert!(!puts.matches(&del));
    let deletes = WatchFilter { kind: EventKindFilter::DeletesOnly, ..WatchFilter::all() };
    assert!(!deletes.matches(&put));
    assert!(deletes.matches(&del));
}

#[test]
fn filter_rejects_reversed_range() {
    let filter = WatchFilter {
        key: b"z".to_vec(),
        range_end: b"a".to_vec(),
        ..WatchFilter::all()
    };
    assert!(filter.validate().is_err());
}

#[test]
fn event_merges_region_fragments_and_strips_prev_when_disabled() {
    let mut filter = WatchFilter::all();
    let event = WatchEvent::from_records(7, vec![record(7, 1, b"a"), record(7, 2, b"b")], &filter)
        .expect("matching changes");
    assert_eq!(event.commit_ts, 7);
    let keys: Vec<_> = event.changes.iter().map(|c| c.key.clone()).collect();
    assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);
    assert!(event.changes.iter().all(|c| c.prev.is_none()));

    filter.prev_kv = true;
    let with_prev = WatchEvent::from_records(7, vec![record(7, 1, b"a")], &filter).unwrap();
    assert!(with_prev.changes[0].prev.is_some());
}

#[test]
fn batch_keeps_an_oversized_atomic_event_and_stops_before_the_next() {
    let mut builder = BatchBuilder::new(2, 1024);
    let big = event_with_changes(3);
    assert!(builder.try_push(big), "an oversized single txn is never split");
    assert!(!builder.try_push(event_with_changes(1)), "next event exceeds the cap");
    assert_eq!(builder.events().len(), 1);
    assert_eq!(builder.events()[0].changes.len(), 3);
}

#[test]
fn batch_respects_the_byte_budget() {
    let mut builder = BatchBuilder::new(1000, 64);
    assert!(builder.try_push(event_with_bytes(32)));
    assert!(!builder.try_push(event_with_bytes(64)));
}
```

- [x] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server --lib kvt::watch::tests`
Expected: FAIL（模块/类型不存在）

- [x] **Step 3: 实现模型与过滤**

实现上述类型；`validate()` 只拒绝 `range_end` 非空且 `range_end <= key`（空 `range_end` = 单 key）。`WatchEvent::from_records` 按 records 顺序拼接 changes，过滤后为空返回 `None`。

约 180 行，含注释。

- [x] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server --lib kvt::watch::tests`
Expected: PASS

- [x] **Step 5: 提交**

```bash
git add coord-server/src/kvt/watch.rs coord-server/src/kvt/mod.rs
git commit -m "feat(kv-t): model watch cursor, filters and batch limits"
```

---

### Task 2: 起始位点解析

**Files:**
- Modify: `coord-server/src/kvt/watch.rs`

**Interfaces:**
- Produces:
  - `pub struct WatchCoordinator { shards: Arc<CommitLogShardManager>, max_changes, max_bytes }`
  - `WatchCoordinator::new(shards)`、`with_limits(max_changes, max_bytes)`
  - `pub async fn start(&self, start: WatchStart) -> Result<WatchCursor, WatchError>`
  - 私有 `fn position_before(table: &CommitLogShardTable, ts: u64) -> WatchCursor`
  - 私有 `fn resolve_position(table, cursor) -> Result<(usize, u64), WatchError>`
- Consumes: `CommitLogShardManager::{table, local_watermark}`。

语义：
- `start_revision == 0`：取 `local_watermark`，cursor 落在 watermark（读 `> watermark`）。
- `start_revision > 0`：cursor 落在 `start_revision - 1`；若该位置恰好是 shard 起点之前，规整为 `(shard_id, 0)`。
- `start_cursor` 与 `start_revision` 同时设置 → `InvalidRequest`；cursor 的 shard 不在表中 → `CursorUnavailable`。
- 空表（尚未 open 第一个 shard）时允许 `beginning()`，等 shard 出现后从 1 开始；`start_revision > 0` 时用 `(0, start_revision - 1)` 表达「首个 shard、跳过更早的 ts」。

- [x] **Step 1: 写失败测试**

```rust
fn shard(id: u64, start: u64, end: u64) -> CommitLogShardMeta { ... }

#[test]
fn start_revision_resolves_to_the_previous_position() {
    let table = CommitLogShardTable { shards: vec![shard(1, 1, 101), shard(2, 101, 0)] };
    assert_eq!(position_before(&table, 0), WatchCursor::beginning());
    assert_eq!(position_before(&table, 1), WatchCursor { shard_id: 1, commit_ts: 0 });
    assert_eq!(position_before(&table, 50), WatchCursor { shard_id: 1, commit_ts: 49 });
    assert_eq!(position_before(&table, 101), WatchCursor { shard_id: 1, commit_ts: 100 });
    assert_eq!(position_before(&table, 102), WatchCursor { shard_id: 2, commit_ts: 101 });
}

#[test]
fn resolve_position_crosses_a_consumed_shard_and_rejects_unknown_shards() {
    let table = CommitLogShardTable { shards: vec![shard(1, 1, 101), shard(2, 101, 0)] };
    assert_eq!(resolve_position(&table, WatchCursor { shard_id: 1, commit_ts: 100 }).unwrap(), (1, 100));
    assert_eq!(resolve_position(&table, WatchCursor { shard_id: 1, commit_ts: 101 }).unwrap(), (1, 101));
    assert!(resolve_position(&table, WatchCursor { shard_id: 9, commit_ts: 0 }).is_err());
}

#[test]
fn start_rejects_dual_positioning() {
    // 纯参数校验，不触 raft：把互斥检查抽成 validate_start(start)。
    let start = WatchStart { start_revision: 5, start_cursor: Some(WatchCursor::beginning()) };
    assert!(validate_start(&start).is_err());
}
```

- [x] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server --lib kvt::watch::tests`
Expected: FAIL（函数不存在）

- [x] **Step 3: 实现解析**

实现 `validate_start`、`position_before`、`resolve_position` 与 `start()`。`start()` 在 `start_revision == 0` 时调用 `shards.local_watermark().await` 并映射错误到 `Unavailable`。

- [x] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server --lib kvt::watch::tests`
Expected: PASS

- [x] **Step 5: 提交**

```bash
git add coord-server/src/kvt/watch.rs
git commit -m "feat(kv-t): resolve watch start revision and cursor"
```

---

### Task 3: 跨 shard 顺序读取与批组装

**Files:**
- Modify: `coord-server/src/kvt/watch.rs`

**Interfaces:**
- Produces: `pub async fn poll(&self, cursor: WatchCursor, filter: &WatchFilter) -> Result<WatchBatch, WatchError>`
- 读取算法：
  1. `table = shards.table()`；`watermark = shards.local_watermark().await`。
  2. `resolve_position` 得到 `(idx, after_ts)`；空表返回空批。
  3. 对每个 shard：`bound = min(end_ts - 1 (若已定界), watermark)`。
  4. `records_after(shard, after, 1)` 取下一个 ts；`records_at(shard, ts)` 取整组；经 filter 合并；空事件只推进 cursor。
  5. 批满时停在该事件之前（cursor 不越过未返回的事件）。
  6. 当前 shard 已消费到 bound，且 `sealed && watermark >= end_ts` → 推进到下一 shard（`after = next.start_ts - 1`）。
  7. 返回 cursor：`after < shard.start_ts` 时规整为 `commit_ts = 0`；`sealed` = cursor 所在 shard 已 seal；`has_more` = 因批上限停止。
- `poll` 先 `filter.validate()`；未知 cursor shard → `CursorUnavailable`；shard 读失败 → `Storage`/`Unavailable`。

- [x] **Step 1: 写失败测试（纯辅助函数）**

把「批剩余空间 + 是否可跨 shard」抽成可单测的小函数：

```rust
#[test]
fn shard_read_bound_is_the_watermark_or_the_shard_end() {
    let open = shard(1, 1, 0);
    assert_eq!(read_bound(&open, 50), 50);
    let bounded = shard(2, 101, 201);
    assert_eq!(read_bound(&bounded, 500), 200);
    assert_eq!(read_bound(&bounded, 150), 150);
}

#[test]
fn crossing_requires_a_sealed_shard_below_the_watermark() {
    let bounded = shard(1, 1, 101);
    assert!(!may_cross(&bounded, 100), "not sealed yet");
    let sealed = CommitLogShardMeta { sealed_at_ms: 1, ..bounded };
    assert!(!may_cross(&sealed, 100), "watermark must reach end_ts");
    assert!(may_cross(&sealed, 101));
    assert!(!may_cross(&shard(2, 101, 0), 10_000), "the latest shard is unbounded");
}
```

- [x] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server --lib kvt::watch::tests`
Expected: FAIL（`read_bound`/`may_cross` 不存在）

- [x] **Step 3: 实现 `poll` 与辅助函数**

实现 `read_bound`、`may_cross`、`scan_shard`、`poll`。`scan_shard` 的批满返回 `(stopped_before_ts, full=true)`，确保 cursor 不越过未进批的事件。

- [x] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server --lib kvt::watch::tests`
Expected: PASS

- [x] **Step 5: 提交**

```bash
git add coord-server/src/kvt/watch.rs
git commit -m "feat(kv-t): read commit log shards in global watch order"
```

---

### Task 4: 端到端验收（跨 shard、原子事务、cursor 恢复、过滤）

**Files:**
- Create: `coord/tests/kv_t_g4_watch_coordinator_test.rs`

复用 `coord/tests/kv_t_g4_commit_log_shards_test.rs` 的双 Region 装配（region 0 raft + 两个数据 Region + `CommitLogShardManager` + TSO/Txn），配置 `commit_log_shard_records: 2` 让每个事务（每 Region 一条记录）触发定界/预创建，从而产生两个 shard。

**Fixtures:**
- `WatchNode::start()`：同 #154 测试的进程内装配；`wait_for_leaders`。
- `commit_pairs(pairs) -> commit_ts`：`TxnV2::begin` + `Commit`；`apply_pending_decisions(&node, 16)`；`shards.tick()`。
- `open_first_shard()`：`shards.tick()`。
- `poll_all(coordinator, cursor, filter) -> (Vec<WatchEvent>, WatchCursor)`：循环 `poll` 直到 `has_more == false` 且不再有事件（加超时上限，避免空转）。

- [x] **Step 1: 写失败测试**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_watch_global_order_by_commit_ts() {
    let fx = WatchNode::start().await;
    fx.open_first_shard().await;
    let ts1 = fx.commit_pairs(&[(b"aa", b"v1")]).await;
    let ts2 = fx.commit_pairs(&[(b"bb", b"v2")]).await;
    assert!(ts2 > ts1);

    let coordinator = WatchCoordinator::new(Arc::clone(&fx.shards));
    let mut cursor = coordinator
        .start(WatchStart { start_revision: 1, start_cursor: None })
        .await
        .unwrap();
    let (events, cursor) = fx.poll_until(&coordinator, cursor, WatchFilter::all(), 2).await;
    assert_eq!(events.iter().map(|e| e.commit_ts).collect::<Vec<_>>(), vec![ts1, ts2]);
    assert_eq!(cursor.commit_ts, ts2);
    assert!(fx.poll_until(&coordinator, cursor, WatchFilter::all(), 0).await.0.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_watch_atomic_multi_key_txn() {
    let fx = WatchNode::start().await;
    fx.open_first_shard().await;
    let ts = fx.commit_pairs(&[(b"aa", b"v1"), (b"zz", b"v2")]).await;

    let coordinator = WatchCoordinator::new(Arc::clone(&fx.shards));
    let cursor = WatchCursor::beginning();
    let (events, _) = fx.poll_until(&coordinator, cursor, WatchFilter::all(), 1).await;
    assert_eq!(events.len(), 1, "one txn = one event");
    assert_eq!(events[0].commit_ts, ts);
    let mut keys: Vec<_> = events[0].changes.iter().map(|c| c.key.clone()).collect();
    keys.sort();
    assert_eq!(keys, vec![b"aa".to_vec(), b"zz".to_vec()]);

    let only_aa = WatchFilter { key: b"aa".to_vec(), ..WatchFilter::all() };
    let (events, _) = fx.poll_until(&coordinator, WatchCursor::beginning(), only_aa, 1).await;
    assert_eq!(events[0].changes.len(), 1);
    assert_eq!(events[0].changes[0].key, b"aa");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_watch_cursor_resumes_without_gap_or_duplicate() {
    let fx = WatchNode::start().await;
    fx.open_first_shard().await;
    let ts1 = fx.commit_pairs(&[(b"aa", b"v1")]).await;
    let ts2 = fx.commit_pairs(&[(b"bb", b"v2")]).await;

    let reader = WatchCoordinator::new(Arc::clone(&fx.shards)).with_limits(1, 1024 * 1024);
    let cursor = reader.start(WatchStart { start_revision: 1, start_cursor: None }).await.unwrap();
    let first = reader.poll(cursor, &WatchFilter::all()).await.unwrap();
    assert_eq!(first.events.iter().map(|e| e.commit_ts).collect::<Vec<_>>(), vec![ts1]);
    assert!(first.has_more);

    // 新的协调器 = 进程重启后的读取端；cursor 只依赖持久化的 shard 日志。
    let resumed = WatchCoordinator::new(Arc::clone(&fx.shards)).with_limits(1, 1024 * 1024);
    let (rest, _) = fx.poll_until(&resumed, first.cursor, WatchFilter::all(), 1).await;
    assert_eq!(rest.iter().map(|e| e.commit_ts).collect::<Vec<_>>(), vec![ts2]);
}
```

- [x] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord --test kv_t_g4_watch_coordinator_test`
Expected: FAIL（文件/类型不存在）

- [x] **Step 3: 实现测试夹具并让测试通过**

夹具照抄 #154 测试的 region 0/数据 Region 装配；`commit_pairs` 与 `apply_pending_decisions` 同 #154；`poll_until` 在 20s 超时前循环 `poll`，每次推进 cursor。

- [x] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord --test kv_t_g4_watch_coordinator_test`
Expected: PASS（3 tests）

- [x] **Step 5: 提交**

```bash
git add coord/tests/kv_t_g4_watch_coordinator_test.rs
git commit -m "test(kv-t): cover global order, atomic txn and cursor resume"
```

---

### Task 5: 文档

**Files:**
- Modify: `docs/production/consumer-contract.md`（Watch v2 / Commit Log 段）

在 Commit Log 分片段后补当前读取语义（不写任务号）：

- Watch Coordinator 按 shard 顺序读，shard 内按 `commit_ts` 整组读取；读取上界是本地 watermark（不会越过未完成的 commit_ts）。
- 跨 shard 只在 shard sealed 且 watermark 越过后推进；cursor 为 `(shard_id, commit_ts)`，`shard_id=0` 表示首个 shard 之前。
- 单批 ≤1000 change 或 1MB，单事务原子批不拆；`prev_kv` 可选，过滤 key/range/put/delete 后 cursor 仍推进。
- 仍不做：保留窗口/GC（#157）、RPC/授权/指标（#158）。

- [x] **Step 1: 更新文档**

- [x] **Step 2: 跑引用检查**

Run: `bash scripts/check-doc-refs.sh`
Expected: 悬空路径 0

- [x] **Step 3: 提交**

```bash
git add docs/production/consumer-contract.md
git commit -m "docs(kv-t): document watch coordinator read and cursor semantics"
```

---

### Task 6: 门禁与 PR

- [x] **Step 1: 全量门禁**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-panics.sh
cargo audit
cargo deny check advisories licenses bans sources
cargo test -p coord-server --lib kvt::watch
cargo test -p coord --test kv_t_g4_watch_coordinator_test
```

- [ ] **Step 2: 推分支并开 PR**

```bash
git push -u origin feat/kv-t-g4-watch-coordinator
gh pr create --base main --title "feat(kv-t): Watch Coordinator across commit log shards (#156)" --body-file /tmp/coord-156-pr.md
```

PR 正文覆盖：顺序/原子/cursor/过滤/批上限/本地 watermark 安全界；明确非范围 #157/#158。

- [ ] **Step 3: 等 CI 全绿并合并**

```bash
gh pr checks --watch
gh pr merge --merge --delete-branch
```

> 本仓历史用 `--merge`（AGENTS.md 规定 coord 用 `--merge`）。
