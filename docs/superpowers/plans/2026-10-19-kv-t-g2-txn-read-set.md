# KV-T G2（#144）v2 事务读阶段与 OCC 读集 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 `coord.txn.v2.Txn.Begin/Read/Abort` 的读阶段：服务端会话固定 `start_ts`，所有读在同一跨 Region 快照上执行，并把 point key / range 谓词记入服务端 OCC 读集（含上限与去重/合并）；`Commit` 仍留给 #151。

**Architecture:** 会话与读集是**协调节点内存态**（ADR-0015 D12.2，不复制）：`TxnSessions` 持有 `txn_id → {start_ts, expiry, ReadSet}`；`Read` 先原子地记录谓词（同时校验 `max_read_set`），再用 #143/#188 的本地快照读（`kv_range_targets` + `read_kv_range_segment`）在 `start_ts` 上取数。`Begin` 在 `start_ts` 快照可服务（`safe_ts >= start_ts`）之前按 `read_ts_wait_ms` 有界等待，超时 `UNAVAILABLE + SAFE_TS_LAG`（可重试）。

**Tech Stack:** Rust、tonic/prost、coord-proto v2、parking_lot、uuid、futures。

**Spec:** `docs/adr/0015-kv-t.md` D4 / D12.1 / D12.2 / D12.3 / D12.5 + issue #144；线缆语义以 `coord-proto/src/proto/coord/txn/v2/txn.proto` 头注为准。

## Global Constraints

- `[kv_t].enabled=false`（默认）时 v1 行为完全不变；`coord.txn.v2` 仍注册但一律 `FAILED_PRECONDITION`。
- 读集由**服务端**保存；客户端不能通过省略读集条目规避冲突（D12.2）。会话不复制：丢失/过期 → `ABORTED` + `TXN_NOT_FOUND`。
- 一个会话内所有 `Read` 使用同一个 `start_ts`；`read_ts` 回显 `start_ts`。
- 读集条目上限 `max_read_set`、单次 range 扫描上限 `max_range`：超限 `INVALID_ARGUMENT` + `READ_SET_TOO_LARGE`，**不得静默截断**。
- v2 线缆没有”事务内暂存写“（写集随 `Commit` 提交），因此本项不存在 read-your-writes；未提交写对任何读都不可见。
- 不新增配置项、不新增 RPC、不新增 `TODO`/`FIXME`；临时文件只放 `/tmp`；注释只写当前「为什么」。
- `Commit` 不在本项实现（#149/#151）；不得为了“看起来完整”写一个不校验的 Commit。
- 提交前本机跑：`cargo fmt --all -- --check`、`cargo clippy --workspace --lib --bins -- -D warnings`、`bash scripts/check-panics.sh`、`cargo audit --no-fetch`、`cargo deny check`、`bash scripts/check-doc-refs.sh`、`cargo test --workspace --no-fail-fast`。

## Rulings

- **读集在数据读之前记录**：先 `record_read`（原子校验上限），再读数据。读失败（`SAFE_TS_LAG`/`HISTORY`）时谓词保留——读集是超集只会造成保守冲突，不会漏冲突；重试同一条读是幂等去重。这样也避免”先读后判“在两次并发读之间穿过上限。
- **上限判定用精确增量**：新增 point/range 若被已有谓词覆盖或会与已有 range 合并，条目数不增，允许在上限处继续；否则条目数 +1，达到上限即拒绝。被 range 覆盖的 point 在合并时吸收，保证同一读集不因读序不同而忽大忽小。
- **`range_end == key` 按单键处理**：复用 `RangeSemantics::of`（与 `coord.kv.v2.Range`、鉴权层同一处定义），不引入第二套区间语义；`range_end < key` 是空谓词，记录后直接返回空结果。
- **`limit` 的语义**：`limit > 0` 是调用方要求的前缀条数（≤ `max_range`），超过 `max_range` 拒绝；`limit == 0` 表示不限制，扫描超过 `max_range` 即拒绝（不截断）。point read 忽略 `limit`（最多 1 条）。
- **`read_ts=0` 的类比**：`Read` 没有 `read_ts` 字段，快照时间恒为会话 `start_ts`；`Begin(start_ts=0)` 走本地 `TsoClient`（与 `GetTs` 同一来源），无 TSO 装配时 `FAILED_PRECONDITION`。**当前限制**：`TsoClient` 只在 region 0 leader 发放，因此 `start_ts=0` 的 `Begin` 目前必须落在 region 0 leader；显式 `start_ts` 不受此限（客户端可先从 `GetTs` 取号）。该限制随 #170/G6 的 TSO 转发收敛，本项不改 `TsoClient`。
- **`Abort` 属于会话生命周期，随本项交付**：幂等删除；未知/已过期/已 abort 的 id 返回 OK（不引入新错误语义）。`Commit` 保持门禁后 `UNIMPLEMENTED`（#151）。
- **对象空间过滤与 `coord.kv.v2.Range` 一致**：对象存储开启时过滤 `/obj/` 命中行，避免事务读看到内部 chunk/manifest；`/obj/` 的完整例外归 #165。

## Review Focus

1. **同一快照**：Begin 后即便有 `commit_ts > start_ts` 的新写入 apply，后续 point/range 读仍只看到 `start_ts` 版本（先写后读、跨 Region 都要验证）。
2. **读集增量正确**：重复 point、range 覆盖 point、重叠/相邻 range 合并不增加条目；读序不同得到同一最小读集。
3. **上限 fail-closed**：读集满后新的独立谓词拒绝；range 超 `max_range` 拒绝；两者都不得返回部分行/静默截断。
4. **会话边界**：过期/未知/abort 后统一 `ABORTED + TXN_NOT_FOUND`；`Abort` 重复调用 OK；TTL 被 `txn_ttl_secs` 钳制。
5. **默认关闭零变化**：`[kv_t].enabled=false` 时 `Begin/Read/Abort` 仍 `FAILED_PRECONDITION`，v1 路径字节级不变。

---

### Task 1: 失败的端到端读阶段测试

**Files:**
- Create: `coord-server/tests/kv_v2_txn_read_test.rs`

**Interfaces:**
- Consumes: `RegionManager::spawn_region`、`CoordNode::set_kvt_state` / `set_tso_client`、`txn_v2::txn_server::Txn`、`Command::PutV2`、`TsoClient`/`TsoSource`/`TsoBatch`。
- Produces: `Begin/Read/Abort` 的 RED 证据（当前 `FAILED_PRECONDITION` 或 `UNIMPLEMENTED`）。

- [ ] **Step 1: 写测试夹具 + 7 个用例**

夹具复制 `coord-server/tests/kv_v2_range_fanout_test.rs` 的 `FanoutNode`（单节点 × 3 Region
`["", "d")` / `["d", "n")` / `["n", "")`，单 voter，`RevisionMode::CommitTs`），并做三处改动：

1. 结构体改名 `TxnNode`，`start(max_range, max_read_set, tso)`；
2. `KvTState ... .with_implemented([KvTCapability::Kv, KvTCapability::Txn])`，配置
   `KvTRuntimeConfig { max_range, max_read_set, read_ts_wait_ms: 50, txn_ttl_secs: 30, ..Default::default() }`；
3. `tso=true` 时在 `Arc::new(node)` 前注入：

```rust
struct FakeTso;
#[async_trait::async_trait]
impl coord_server::raft::tso::TsoSource for FakeTso {
    async fn is_leader(&self) -> bool {
        true
    }
    async fn allocate(
        &self,
        batch_size: u32,
    ) -> coord_core::error::Result<coord_server::raft::tso::TsoBatch> {
        Ok(coord_server::raft::tso::TsoBatch {
            start_ts: 1,
            end_ts: 1 + u64::from(batch_size),
        })
    }
}
// …
node.set_tso_client(coord_server::raft::tso::TsoClient::new(
    Arc::new(FakeTso),
    8,
    None,
));
```

数据播种与 `standard_data()` 相同（r1: a@100/b@200/a2@260/a3@400，r2: d@100/e@250/e2@420，
r3: n@100/z@400，`safe_ts == 400`）。用例：

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_txn_reads_one_snapshot_across_regions() {
    let fx = TxnNode::standard(100, 16, false).await;
    let begin = fx.begin(400, 0).await.unwrap();
    assert_eq!(begin.start_ts, 400);
    assert_eq!(fx.read(&begin.txn_id, b"a", b"", 0, false).await.unwrap().kvs[0].value, b"a100");

    // 快照固定：begin 之后的写入（commit_ts=500）不可见。
    fx.put(b"a", b"a500", 500).await;
    let a = fx.read(&begin.txn_id, b"a", b"", 0, false).await.unwrap();
    assert_eq!(a.read_ts, 400);
    assert_eq!(a.kvs[0].value, b"a100");
    assert_eq!(a.kvs[0].mod_revision, 100);

    let all = fx.read(&begin.txn_id, b"a", b"zzz", 0, false).await.unwrap();
    assert_eq!(all.read_ts, 400);
    assert_eq!(
        keys(&all),
        [b"a".as_slice(), b"a2", b"a3", b"b", b"d", b"e", b"n", b"z"]
    );

    let keys_only = fx.read(&begin.txn_id, b"d", b"n", 0, true).await.unwrap();
    assert!(keys_only.kvs.iter().all(|kv| kv.value.is_empty()));

    // 空谓词（end < start）返回空，不路由、不回退成全扫描。
    let empty = fx.read(&begin.txn_id, b"n", b"a", 0, false).await.unwrap();
    assert!(empty.kvs.is_empty());
    assert_eq!(empty.read_ts, 400);

    fx.abort(&begin.txn_id).await.unwrap();
    let err = fx.read(&begin.txn_id, b"a", b"", 0, false).await.unwrap_err();
    assert_eq!(err.code(), Code::Aborted);
    assert_eq!(error_code_of(&err).as_deref(), Some("TXN_NOT_FOUND"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_txn_begin_allocates_start_ts_from_local_tso() {
    let fx = TxnNode::standard(100, 16, true).await;
    let begin = fx.begin(0, 0).await.unwrap();
    assert_eq!(begin.start_ts, 1, "fake TSO batch starts at 1");
    let read = fx.read(&begin.txn_id, b"a", b"", 0, false).await.unwrap();
    assert_eq!(read.read_ts, 1);
    assert!(read.kvs.is_empty(), "a@100 is after start_ts=1");

    // 没有 TSO 装配时 fail-closed（不是静默给 0）。
    let no_tso = TxnNode::standard(100, 16, false).await;
    let err = no_tso.begin(0, 0).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_txn_read_set_limit_fails_closed() {
    let fx = TxnNode::standard(100, 2, false).await;
    let begin = fx.begin(400, 0).await.unwrap();
    fx.read(&begin.txn_id, b"a", b"", 0, false).await.unwrap();
    fx.read(&begin.txn_id, b"d", b"", 0, false).await.unwrap();

    let err = fx.read(&begin.txn_id, b"n", b"", 0, false).await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert_eq!(error_code_of(&err).as_deref(), Some("READ_SET_TOO_LARGE"));

    // 已覆盖的 point 不增长读集：仍可在满额会话里重复读。
    fx.read(&begin.txn_id, b"a", b"", 0, false).await.unwrap();

    // 新会话：range 谓词作为一个条目可容纳。
    let fresh = fx.begin(400, 0).await.unwrap();
    fx.read(&fresh.txn_id, b"a", b"zzz", 0, false).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_txn_range_scan_over_max_range_fails_closed() {
    let fx = TxnNode::standard(3, 16, false).await;
    let begin = fx.begin(400, 0).await.unwrap();

    // r1 有 4 个命中 key（a/a2/a3/b）→ 超过 max_range=3，必须拒绝而不是截断。
    let err = fx.read(&begin.txn_id, b"a", b"zzz", 0, false).await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert_eq!(error_code_of(&err).as_deref(), Some("READ_SET_TOO_LARGE"));

    let limited = fx.read(&begin.txn_id, b"a", b"zzz", 2, false).await.unwrap();
    assert_eq!(keys(&limited), [b"a".as_slice(), b"a2"]);

    // point read 不受 max_range 约束。
    fx.read(&begin.txn_id, b"a3", b"", 0, false).await.unwrap();

    // 小 range 正常返回；显式 limit 超过 max_range 仍拒绝。
    let small = fx.read(&begin.txn_id, b"n", b"zzz", 0, false).await.unwrap();
    assert_eq!(keys(&small), [b"n".as_slice()]);
    let err = fx.read(&begin.txn_id, b"n", b"zzz", 4, false).await.unwrap_err();
    assert_eq!(error_code_of(&err).as_deref(), Some("READ_SET_TOO_LARGE"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_txn_session_ttl_and_unknown_id() {
    let fx = TxnNode::standard(100, 16, false).await;
    let begin = fx.begin(400, 1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let err = fx.read(&begin.txn_id, b"a", b"", 0, false).await.unwrap_err();
    assert_eq!(err.code(), Code::Aborted);
    assert_eq!(error_code_of(&err).as_deref(), Some("TXN_NOT_FOUND"));

    let err = fx.read(b"missing", b"a", b"", 0, false).await.unwrap_err();
    assert_eq!(error_code_of(&err).as_deref(), Some("TXN_NOT_FOUND"));

    let begin = fx.begin(400, 0).await.unwrap();
    fx.abort(&begin.txn_id).await.unwrap();
    fx.abort(&begin.txn_id).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_txn_begin_rejects_future_or_invalid_start_ts() {
    let fx = TxnNode::standard(100, 16, false).await;
    let err = fx.begin(10_000, 0).await.unwrap_err();
    assert_eq!(err.code(), Code::Unavailable);
    assert_eq!(error_code_of(&err).as_deref(), Some("SAFE_TS_LAG"));

    let err = fx.begin(-1, 0).await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
}
```

- [ ] **Step 2: 跑测试确认 RED**

Run: `cargo test -p coord-server --test kv_v2_txn_read_test`
Expected: FAIL —— `Begin` 命中能力门禁 `FAILED_PRECONDITION`（`coord.txn.v2` 未实现），
或 `UNIMPLEMENTED`；HTTP 层不连通等环境错误不算 RED。

- [ ] **Step 3: 提交测试**

```bash
git add coord-server/tests/kv_v2_txn_read_test.rs
git commit -m "test(kv-t): pin cross-region txn read contract (#144)"
```

### Task 2: 读集与会话模块（单元测试先行）

**Files:**
- Create: `coord-server/src/kvt/txn.rs`
- Modify: `coord-server/src/kvt/mod.rs`（`pub(crate) mod txn;`）

**Interfaces:**
- Produces:
  - `ReadSet { points, ranges, empty_ranges }`，方法 `len()`、`try_add_point(&[u8], usize) -> Result<(), ReadSetLimit>`、`try_add_range(&[u8], &[u8], usize) -> Result<(), ReadSetLimit>`；
  - `ReadSetLimit::{Entries, RangeKeys}`（`Copy`）；
  - `TxnSessions::new()`、`begin(start_ts: u64, ttl: Duration) -> Vec<u8>`、`record_read(&[u8], ReadIntent<'_>, usize) -> Result<u64, SessionError>`、`abort(&[u8])`、`purge_expired()`；
  - `ReadIntent::{Point(&[u8]), Range { start, end }}`；`SessionError::{NotFound, ReadSetLimit(ReadSetLimit)}`。

- [ ] **Step 1: 写单元测试**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn read_set_merges_overlapping_and_adjacent_ranges() {
        let mut set = ReadSet::default();
        set.try_add_range(b"a", b"c", 10).unwrap();
        set.try_add_range(b"b", b"d", 10).unwrap();
        set.try_add_range(b"d", b"f", 10).unwrap(); // 相邻也合并
        assert_eq!(set.len(), 1);
        assert_eq!(set.ranges, [ReadRange::new(b"a", b"f")]);
    }

    #[test]
    fn read_set_absorbs_points_covered_by_range() {
        let mut set = ReadSet::default();
        set.try_add_point(b"b", 10).unwrap();
        set.try_add_range(b"a", b"c", 10).unwrap();
        assert_eq!(set.len(), 1);
        assert!(set.points.is_empty());
        // 先 range 后 point：已覆盖，不增长。
        set.try_add_point(b"b", 10).unwrap();
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn read_set_dedupes_and_rejects_over_limit() {
        let mut set = ReadSet::default();
        set.try_add_point(b"a", 2).unwrap();
        set.try_add_point(b"a", 2).unwrap();
        set.try_add_point(b"b", 2).unwrap();
        assert_eq!(set.try_add_point(b"c", 2), Err(ReadSetLimit::Entries));
        assert_eq!(set.len(), 2, "被拒绝的谓词不得改动读集");
    }

    #[test]
    fn read_set_at_limit_can_still_merge() {
        let mut set = ReadSet::default();
        set.try_add_range(b"a", b"c", 2).unwrap();
        set.try_add_point(b"z", 2).unwrap();
        // 与已有 range 重叠 → 条目不增，满额也允许。
        set.try_add_range(b"b", b"d", 2).unwrap();
        assert_eq!(set.len(), 2);
        // 完全独立的谓词 → 拒绝。
        assert_eq!(set.try_add_range(b"x", b"y", 2), Err(ReadSetLimit::Entries));
        assert_eq!(set.try_add_range(b"q", b"q", 2), Err(ReadSetLimit::Entries));
    }

    #[test]
    fn empty_range_is_recorded_and_deduped() {
        let mut set = ReadSet::default();
        set.try_add_range(b"z", b"a", 2).unwrap();
        set.try_add_range(b"z", b"a", 2).unwrap();
        assert_eq!(set.len(), 1);
        assert_eq!(set.empty_ranges.len(), 1);
    }

    #[test]
    fn sessions_expire_and_abort_is_idempotent() {
        let sessions = TxnSessions::new();
        let id = sessions.begin(7, Duration::from_millis(1));
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(
            sessions.record_read(&id, ReadIntent::Point(b"a"), 10),
            Err(SessionError::NotFound)
        );

        let id = sessions.begin(7, Duration::from_secs(30));
        assert_eq!(
            sessions.record_read(&id, ReadIntent::Point(b"a"), 10),
            Ok(7)
        );
        sessions.abort(&id);
        sessions.abort(&id);
        assert_eq!(sessions.read_set(&id), None);
    }

    #[test]
    fn session_records_range_and_point_in_one_read_set() {
        let sessions = TxnSessions::new();
        let id = sessions.begin(9, Duration::from_secs(30));
        sessions
            .record_read(&id, ReadIntent::Range { start: b"a", end: b"c" }, 10)
            .unwrap();
        sessions
            .record_read(&id, ReadIntent::Point(b"b"), 10)
            .unwrap();
        let set = sessions.read_set(&id).unwrap();
        assert_eq!(set.len(), 1);
        assert_eq!(set.ranges, [ReadRange::new(b"a", b"c")]);
    }
}
```

- [ ] **Step 2: 跑测试确认 RED**

Run: `cargo test -p coord-server kvt::txn`
Expected: 编译失败（模块/类型不存在）。

- [ ] **Step 3: 实现模块**

要点（实现自由，但必须满足上面的断言与接口）：

- `ReadSet`：`points: BTreeSet<Vec<u8>>`、`ranges: Vec<ReadRange>`（非空、排序、互不重叠/不相邻）、
  `empty_ranges: BTreeSet<ReadRange>`（`end <= start` 的谓词；去重且计入 `len()`）。
- `try_add_point`：重复或已被某个非空 range 覆盖 → `Ok(())`；否则 `len() >= max` → `Err(Entries)`；
  插入后返回。
- `try_add_range`（非空）：被单个已有 range 完全覆盖 → `Ok(())`；与某个 range 重叠/相邻 →
  允许插入（条目数不会增加）；完全独立且 `len() >= max` → `Err(Entries)`；插入后
  `merge_ranges()`（排序 + 合并 + 吸收被覆盖的 points）。
- `try_add_range`（空）：重复 → `Ok(())`；否则按条目上限检查后插入 `empty_ranges`。
- `TxnSessions`：`Mutex<BTreeMap<Vec<u8>, TxnSession>>`；`begin` 先 `purge_expired()` 再
  `Uuid::new_v4().as_bytes().to_vec()`；`record_read` 在锁内先判断过期（过期即删除并返回
  `NotFound`），再更新读集；`abort` 直接 `remove`；`read_set(&id)` 仅测试可见（`#[cfg(test)]`）。
- 不在锁内做 I/O 或 `await`；所有公开项 `pub(crate)`，模块 `pub(crate) mod txn;`。

- [ ] **Step 4: 跑测试确认 GREEN**

Run: `cargo test -p coord-server kvt::txn`
Expected: 6 passed。

- [ ] **Step 5: 提交**

```bash
git add coord-server/src/kvt/txn.rs coord-server/src/kvt/mod.rs
git commit -m "feat(kv-t): add txn read-set session primitives (#144)"
```

### Task 3: 接线 Begin/Read/Abort 与会话生命周期

**Files:**
- Modify: `coord-server/src/server/mod.rs`（`CoordNode` 新增 `txn_sessions` 字段 + `new()` 初始化 + 访问器）
- Modify: `coord-server/src/kvt/services.rs`（实现 `Txn::begin/read/abort`，`Commit` 保持 `shell`）
- Modify: `coord/src/main.rs`（`implemented` 增加 `Txn`，更新启动日志/注释）
- Modify: `coord-server/src/kvt/mod.rs`（模块文档写明 Txn 读阶段已交付）

**Interfaces:**
- Consumes: Task 2 的 `TxnSessions` / `ReadIntent` / `SessionError`；`tso_client()`、`safe_ts()`、`safe_ts_fence()`、`tso_status()`、`wait_for_safe_ts()`、`kv_range_targets()`、`read_kv_range_segment()`、`single_key_end()`、`RANGE_FANOUT_CONCURRENCY`。
- Produces: `CoordNode::txn_sessions() -> &TxnSessions`（`pub(crate)`）。

- [ ] **Step 1: 跑 Task 1 的 e2e，确认仍然 RED（门禁未放行）**

Run: `cargo test -p coord-server --test kv_v2_txn_read_test`
Expected: FAIL（`FAILED_PRECONDITION`），证明测试确实锁在能力门禁上。

- [ ] **Step 2: `CoordNode` 增加会话字段**

```rust
/// `coord.txn.v2` 服务端会话（ADR-0015 D12.2）：只在协调节点内存，不复制。
txn_sessions: crate::kvt::txn::TxnSessions,
// new():
txn_sessions: crate::kvt::txn::TxnSessions::new(),
// 访问器：
pub(crate) fn txn_sessions(&self) -> &crate::kvt::txn::TxnSessions {
    &self.txn_sessions
}
```

- [ ] **Step 3: 实现 `Txn::begin`**

```rust
self.kvt().gate(KvTCapability::Txn)?;
let req = request.into_inner();
if req.start_ts < 0 { /* INVALID_ARGUMENT */ }
let start_ts = if req.start_ts == 0 {
    let client = self.tso_client().ok_or_else(|| /* FAILED_PRECONDITION */)?;
    client.get_ts_above(self.safe_ts_fence()).await.map_err(tso_status)?
} else {
    req.start_ts as u64
};
if start_ts > self.safe_ts() {
    wait_for_safe_ts(self, start_ts, self.kvt().config().read_ts_wait_ms).await?;
}
let default_ms = self.kvt().config().txn_ttl_secs.saturating_mul(1000);
let ttl_ms = if req.ttl_ms == 0 { default_ms } else { u64::from(req.ttl_ms).min(default_ms) };
let txn_id = self.txn_sessions().begin(start_ts, Duration::from_millis(ttl_ms));
Ok(Response::new(txn_v2::BeginResponse { txn_id, start_ts: ts_to_i64(start_ts)? }))
```

- [ ] **Step 4: 实现 `Txn::read`**

```rust
self.kvt().gate(KvTCapability::Txn)?;
let req = request.into_inner();
// limit < 0 → INVALID_ARGUMENT
let config = self.kvt().config();
let max_range = config.max_range as usize;
let max_read_set = config.max_read_set as usize;

let single_key = RangeSemantics::of(&req.key, &req.range_end).is_single_key();
let end = if single_key { single_key_end(&req.key) } else { req.range_end.clone() };
let intent = if single_key {
    ReadIntent::Point(&req.key)
} else {
    ReadIntent::Range { start: &req.key, end: &req.range_end }
};
let start_ts = self.txn_sessions()
    .record_read(&req.txn_id, intent, max_read_set)
    .map_err(txn_session_status)?;

if !single_key && end <= req.key {
    return Ok(Response::new(txn_v2::ReadResponse { kvs: Vec::new(), read_ts: start_ts as i64 }));
}
let requested_limit = if single_key { None } else if req.limit == 0 { None } else {
    let n = req.limit as usize;
    if n > max_range { /* INVALID_ARGUMENT + READ_SET_TOO_LARGE */ }
    Some(n)
};
let scan_limit = requested_limit.unwrap_or(max_range.saturating_add(1));
let segments = self.kv_range_targets(&req.key, &end)?;
let reads = segments.into_iter().map(|seg| read_kv_range_segment(seg, start_ts, scan_limit));
let mut parts = futures::stream::iter(reads).buffered(RANGE_FANOUT_CONCURRENCY);
let mut kvs = Vec::new();
while let Some(mut part) = parts.try_next().await? {
    if self.object_enabled() {
        part.retain(|kv| !crate::storage::object_store::key_in_object_space(&kv.key));
    }
    kvs.extend(part);
    if let Some(limit) = requested_limit {
        if kvs.len() >= limit { kvs.truncate(limit); break; }
    } else if kvs.len() > max_range {
        return Err(attach(Status::invalid_argument(format!(
            "transaction range scan exceeds max {max_range} keys; narrow the range or split the transaction"
        )), CoordErrorCode::ReadSetTooLarge));
    }
}
drop(parts);
if req.keys_only { for kv in &mut kvs { kv.value.clear(); } }
Ok(Response::new(txn_v2::ReadResponse { kvs, read_ts: start_ts as i64 }))
```

- [ ] **Step 5: 实现 `Txn::abort`，`Commit` 保持 shell**

```rust
self.kvt().gate(KvTCapability::Txn)?;
self.txn_sessions().abort(&request.into_inner().txn_id);
Ok(Response::new(txn_v2::AbortResponse {}))
```

`Commit` 继续返回 `Err(shell(KvTCapability::Txn, self))`（门禁通过后为 `UNIMPLEMENTED`；
决策/校验归 #149/#151）。新增 `txn_session_status`：`NotFound → ABORTED + TXN_NOT_FOUND`，
`ReadSetLimit → INVALID_ARGUMENT + READ_SET_TOO_LARGE`。

- [ ] **Step 6: 放行能力并更新装配日志**

- `coord/src/main.rs`：`.with_implemented([Tso, Kv, Txn])`；日志改为
  “`coord.txn.v2` Begin/Read/Abort（G2 读阶段）可用，Commit 待 #151”。
- `coord-server/src/kvt/mod.rs` 模块文档：已交付能力改为 “G1 的 Tso/Kv + G2 的 Txn 读阶段”。

- [ ] **Step 7: 跑 e2e 确认 GREEN**

Run: `cargo test -p coord-server --test kv_v2_txn_read_test`
Expected: 6 passed。

- [ ] **Step 8: 提交**

```bash
git add coord-server/src/server/mod.rs coord-server/src/kvt/services.rs coord-server/src/kvt/mod.rs coord/src/main.rs
git commit -m "feat(kv-t): serve txn read phase with server-side read set (#144)"
```

### Task 4: 回归与门禁

**Files:**
- 无新增（验证 + 必要时修正）。

- [ ] **Step 1: 相关回归**

```bash
cargo test -p coord-server --test kv_v2_range_fanout_test
cargo test -p coord-server --test kv_v2_multi_node_range_test
cargo test -p coord-server --test kv_v2_single_region_test
cargo test -p coord --test kv_t_g1_kv_rpc_test
cargo test -p coord --test kv_t_g2_safe_ts_test
cargo test -p coord --test kv_t_g0_test
```

Expected: 全绿；G0 测试确认关闭态仍 `FAILED_PRECONDITION`。

- [ ] **Step 2: 全量门禁（AGENTS.md）**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-panics.sh
cargo audit --no-fetch
cargo deny check
bash scripts/check-doc-refs.sh
cargo test --workspace --no-fail-fast
```

Expected: 全部通过；任何一项缺失都不算完成。

- [ ] **Step 3: 提交修正（如有）**

```bash
git add -A
git commit -m "chore(kv-t): satisfy txn read-phase gates (#144)"
```

- [ ] **Step 4: 开 PR**

先跑 `gh pr checks --watch` 前的本地门禁（本任务 Step 2）；PR 正文写清：
- `Closes #144`；
- RED 证据（`FAILED_PRECONDITION`）与 GREEN 证据（7 passed）；
- 读集/上限/会话边界的取舍（Rulings）；
- 已知限制：`Begin(start_ts=0)` 目前依赖 region 0 leader 的 TSO（`TsoClient` 只在 leader 发放），
  显式 `start_ts` 不受限；`Commit` 归 #151。
