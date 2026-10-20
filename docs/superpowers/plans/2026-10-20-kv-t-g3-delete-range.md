# KV-T G3（#198）事务区间写 DELETE_RANGE 与区间写索引 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 `coord.txn.v2.Commit` 接受 `DELETE_RANGE`，并让 OCC 对区间写与并发
point/range 读集正确判冲突；区间删除跨 Region 原子可见、apply/重放幂等。

**Architecture:** `SequencerMutation` 末尾追加 `DeleteRange { start, end }`
（postcard 追加变体，旧决策日志可解码）。region 0 的 sequencer 在同一个
redb 写事务里把区间写记入**独立的区间索引**（哈希键 + 序列化条目，全量扫描、
受 `max_range` 预算约束），OCC 校验把 point 读与 range 读同时对照点索引与区间
索引。决策 apply 时由 `decision_apply` 把区间按 Region 裁剪，数据 Region 只在
自己的 `TABLE_KV` 内标记 tombstone，并与 changelog/游标同事务落盘。

**Tech Stack:** Rust、openraft/redb、postcard、tokio、tonic（无新增依赖/RPC/配置项）。

**Spec:** issue #198（https://github.com/byteforce-cn/coord/issues/198）+
`docs/adr/0015-kv-t.md` D4/D9/D11/D12；前置 #144（读集）、#146（sequencer）、
#147/#149（校验索引）、#148（决策 apply）、#151（Commit 面）；总表 #127。

## Global Constraints

- `[kv_t].enabled=false`（默认）时 v1 行为字节级不变（ADR-0001）；本项只动
  v2 txn/sequencer/decision apply 与文档。
- `commit_ts` 全局唯一、严格单调、绝不复用（D11.1/D11.2）；冲突/非法不写决策，
  且不得让 raft apply 返回 `Err`（D11.5）。
- 同一 `txn_id` 永不产生第二个 `CommitDecision`；同一 `txn_id` + `request_id`
  的重试返回首次 `commit_ts`（D12.2）。
- `DELETE_RANGE` 的 `range_end` 必须非空且 `key < range_end`（proto 已定）；
  `end` 为空在 sequencer 内部只按防御性代码处理，不是合法输入。
- 区间写只在**裁剪后的 Region 区间**内落盘；Region 存储不得出现跨区删除。
- 索引是派生数据：丢索引只能由 `catch_up_sequencer_index` /
  `rebuild_sequencer_validation_index` 从决策日志重建，重建结果必须与原索引
  等价。
- 不新增 `TODO`/`FIXME`；注释只写当前「为什么」；临时文件只放 `/tmp`。

## Rulings

- **区间索引用「哈希键 + 序列化条目」并全量扫描，不引入有序区间树。**
  区间写远少于点写；扫描受 `max_range` 预算约束，超预算 fail-closed
  （`RangeScanExceeded`）。代价：区间写很多时 point 读变慢/被拒；正确性不受
  影响，容量优化归 G6（#174）。若错误：性能回归，不是语义回归。
- **`SequencerMutation::key()` 改为 `point_key() -> Option<&[u8]>`。**
  区间写没有单一 key；返回 `Option` 让所有调用点显式处理，避免把 `start`
  误当整条 mutation 的归属键。代价：3 个调用点与 1 个断言改名。
- **区间删除的 changelog 对账（`event_matches_mutations`）直接返回 false。**
  区间删除产生的 change 条数取决于 apply 时存活的 key，无法从 mutation 反推；
  正常重放由「游标与 changelog 同事务」保证在 `commit_ts <= 游标` 处跳过。
  该函数只会在「同 `commit_ts` 已被非决策写占用」时被触达，返回 false 让
  apply 停在该决策前（fail-closed），不冒「误判已应用」的风险。代价：真出现
  这种漂移时 safe_ts 会停住并由 warning 暴露，而不是静默跳过。
- **裁剪放在 `decision_apply`，`build_apply_batch` 产出的每条 mutation 都已
  落在目标 Region 内。** MVCC apply 不再判断 Region 边界，只按载荷执行。
  代价：裁剪逻辑需要覆盖空 `end`（+∞）与空 Region 上界。
- **apply 内联复用区间删除的语义，不调用 `delete_range_at_commit_ts`。**
  后者自己开 `backend.write`，无法嵌进 `apply_commit_decisions` 已有的写事务；
  内联实现 tombstone + changelog + 游标同事务，语义等价于既有区间删除路径。
  代价：两处 tombstone 代码需要保持一致的 metadata 语义。
- **不做区间索引压缩。** 目前没有可用的「最老活跃 `start_ts`」水位（会话只在
  协调节点内存里），压缩阈值无从定义；条目随决策单调增长。容量与 GC 归 G6
  （#174）。代价：长期大量区间删除会让索引变大、扫描变慢，最终按预算
  fail-closed，但不影响正确性。
- **不新增 RPC 字段、配置项或依赖。** `DELETE_RANGE` 已在 proto 中；
  `max_txn_bytes` 按 `start`/`end` 字节计（不枚举 key）。
- **读集/写集上限语义不变**：区间写只占一条写集条目；区间索引扫描超
  `max_range` 映射为 `READ_SET_TOO_LARGE`（与 #149 一致）。

## Review Focus

1. **跨 Region 原子可见**：一个覆盖两个 Region 的区间删除在 apply 后必须
   同时消失（`safe_ts` 收敛到 `commit_ts`），不得出现一半删一半留。
2. **谓词冲突完整性**：point 读落在区间写内、range 读与区间写相交都必须
   `TXN_CONFLICT`；相邻区间（`[a,c)` 与 `[c,e)`）与不相交区间不得误报。
3. **扫描预算 fail-closed**：区间索引项超过 `max_range` 时必须返回
   `READ_SET_TOO_LARGE`，绝不能被当作 `Clear`。
4. **重放幂等**：同一决策重复 apply、同一 `txn_id`/`request_id` 重试都只产生
   一个决策、一次删除；changelog 已占用同一 `commit_ts` 时 apply 停住而非跳过。
5. **裁剪边界**：`[a,z)` 在 `[..m)`/`[m..)` 上分别变成 `[a,m)`/`[m,z)`；
   完全落在 Region 外的区间不产生 mutation，但决策仍推进游标。
6. **关闭态/回归**：`[kv_t].enabled=false` 与 v1 路径不受影响；点写/点删的
   既有冲突与索引重建测试保持通过。

## Setup（执行前）

```bash
git fetch origin
git switch -c feat/kv-t-g3-delete-range origin/main
```

沿用仓库既有做法：在主 checkout 的工作分支上执行，不新建 worktree（共享
`target/` 构建缓存）；进度写入 `.superpowers/sdd/<plan-basename>/progress.md`。

---

### Task 1: 决策类型、区间索引、Region 内 apply 与裁剪

**Files:**
- Modify: `coord-server/src/raft/type_config.rs`（`SequencerMutation`、
  `point_key`）
- Modify: `coord-server/src/kvt/sequencer.rs`（`CommitCandidate::wire_bytes`）
- Modify: `coord-server/src/storage/mvcc.rs`（区间索引、OCC 校验、apply、
  changelog 对账）
- Modify: `coord-server/src/kvt/decision_apply.rs`（Region 裁剪）
- Test: 上述文件的 `#[cfg(test)] mod`

**Interfaces:**
- Consumes: `apply_sequencer_op`、`apply_commit_decisions`、
  `sequencer_point_query`、`sequencer_range_query`、
  `sequence_index_writes_in_tx`、`catch_up_sequencer_index`、
  `rebuild_sequencer_validation_index`、`build_apply_batch`。
- Produces:
  - `SequencerMutation::DeleteRange { start: Vec<u8>, end: Vec<u8> }`（追加变体）
  - `SequencerMutation::point_key(&self) -> Option<&[u8]>`
  - `const SEQUENCER_INTERVAL_INDEX_PREFIX: &[u8] = b"/_sys/seq/iidx/";`
  - `struct SequencerIntervalWrite { start: Vec<u8>, end: Vec<u8>, commit_ts: Revision }`
  - `fn clip_mutation(meta: &RegionMeta, mutation: &SequencerMutation) -> Option<SequencerMutation>`
  - `fn sequencer_interval_point_in(tx, key, start_ts, max_entries) -> Result<SequencerRangeQuery>`
  - `fn sequencer_interval_range_in(tx, start, end, start_ts, max_entries) -> Result<SequencerRangeQuery>`

- [x] **Step 1: 写失败测试（RED）**

在 `coord-server/src/storage/mvcc.rs` 的测试模块追加（复用既有
`create_commit_ts_storage`、`applied_for`、`seq_op`、`decision`、`put`、
`delete` 辅助）：

- `test_sequencer_range_delete_conflicts_with_point_and_range_read`
  - 提交 `DeleteRange { start: b"b", end: b"f" }`，`start_ts=1, commit_ts=10`；
  - `sequencer_point_query(b"c", 5)` → `Some(Hit { key: b"b", commit_ts: 10 })`；
  - `sequencer_point_query(b"c", 10)` → `None`（窗口是 `(start_ts, ·]`）；
  - `sequencer_point_query(b"a", 5)` / `b"f"` → `None`（左闭右开）；
  - `sequencer_range_query(b"a", b"c", 5, 0)` → `Hit(b"b", 10)`；
  - `sequencer_range_query(b"a", b"b", 5, 0)` / `(b"f", b"z")` → `Clear`
    （相邻不算相交）；
  - `sequencer_range_query(b"a", b"", 5, 0)` → `Hit(b"b", 10)`（空 end = +∞）。
- `test_sequencer_range_delete_same_batch_conflicts`
  - 同一批两条事务：`DeleteRange [b"b", b"f")`（`commit_ts=10`）在前，
    区间读 `[b"a", b"z")`（`start_ts=5, commit_ts=20`）在后；
  - outcome 依次为 `Committed { 10 }` 与
    `Conflict { commit_ts: 20, conflict_key: b"b", conflict_ts: 10 }`；
  - `get_sequencer_decision(20)` 为 `None`。
- `test_sequencer_interval_index_rebuild_is_equivalent`
  - 提交两条不同区间写后调用 `rebuild_sequencer_validation_index(16)`；
  - `sequencer_point_query` / `sequencer_range_query` 仍命中，且
    `sequencer_index_last_ts()` 等于最后决策 ts；
  - 旧的点写索引在同一重建后仍可用（复用既有 `seq_index_seed` 断言）。
- `test_sequencer_interval_index_survives_snapshot`
  - 提交区间写后 `export_snapshot_data` → `import_snapshot_data` 到新存储；
  - 恢复后的 `sequencer_point_query` / `sequencer_range_query` 与原来一致
    （区间索引在 `/_sys/` 域内随快照携带）。
- `test_sequencer_interval_scan_budget_is_fail_closed`
  - 提交 5 条互不相同的区间写；构造读集为 point key 的事务，`limits.max_range=3`；
  - outcome 为 `Invalid { reason: RangeScanExceeded }`，且不写决策。
- `test_apply_commit_decisions_delete_range_is_atomic_and_replays`
  - 先写 `a/b/c`（`put_at_commit_ts`），再 apply
    `decision(10, [DeleteRange{a,d}, Put{d,"v"}])`；
  - `a/b/c` 均不可见、`d == "v"`、`current_revision()==10`、
    `last_applied_decision_ts()==10`；
  - 同一决策再 apply 一次：`through_ts==10`、`decisions==0`，数据不变；
  - changelog 10 是 `EventType::Txn`，changes 包含 4 条（3 删 1 写）。
- `test_apply_commit_decisions_delete_range_stops_at_occupied_ts`
  - 先用 `delete_range_at_commit_ts(b"a", b"d", 10, applied_for(1))` 占用 10；
  - apply `decision(10, [DeleteRange{b"x", b"z"}])`；
  - 结果 `through_ts==0`、`decisions==0`，且 `x` 的元数据未被改动
    （fail-closed，不把不同写当成已应用）。

在 `coord-server/src/kvt/decision_apply.rs` 测试模块追加：

- `clip_mutation_clips_delete_range_per_region`
  - Region `[b"", b"m")` 与 `[b"m", b"")`；
  - `DeleteRange{a,z}` → `[a,m)` 与 `[m,z)`；
  - `DeleteRange{a,b}` 对 `[m..)` → `None`；
  - 点写 `Put{apple}` 只落在第一个 Region。
- `build_apply_batch_keeps_decision_without_region_mutations`
  - 一条只有区间外 mutation 的决策仍出现在载荷里（`mutations` 为空），
    保证游标推进。
- `build_apply_batch_accounts_delete_range_bytes`
  - `max_bytes` 只够第一条时，第二条大区间决策留到下一批。

在 `coord-server/src/kvt/sequencer.rs` 测试模块追加：

- `test_wire_bytes_counts_delete_range`
  - `DeleteRange{a, 4KiB end}` 的 `wire_bytes()` 比空读集候选多
    `start.len() + end.len() + 32`。

- [x] **Step 2: 运行测试确认失败**

Run: `cargo test -p coord-server --lib kvt::sequencer::tests::test_wire_bytes_counts_delete_range`
Expected: 编译失败（`SequencerMutation::DeleteRange` 不存在）。

- [x] **Step 3: 实现类型与线缆估算**

`type_config.rs`：

```rust
pub enum SequencerMutation {
    Put { key: Vec<u8>, value: Vec<u8>, lease_id: Option<i64> },
    Delete { key: Vec<u8> },
    /// 区间写 `[start, end)`；`end` 非空（RPC 层校验），作为最后一个变体追加，
    /// 旧 postcard 决策日志无需重写。
    DeleteRange { start: Vec<u8>, end: Vec<u8> },
}

impl SequencerMutation {
    /// 点写的 key；区间写返回 `None`（没有单一归属 key）。
    pub fn point_key(&self) -> Option<&[u8]>
}
```

`sequencer.rs` 的 `CommitCandidate::wire_bytes` 增加
`DeleteRange { start, end } => start.len() + end.len() + 32`。

- [x] **Step 4: 实现区间索引与 OCC 校验**

`mvcc.rs`：

- 新增前缀常量与条目类型 `SequencerIntervalWrite`（postcard/envelope 编码）。
- `sequencer_index_writes_in_tx`：
  - 点写沿用「哈希索引 + 有序 point 索引」；
  - `DeleteRange` 写一条
    `prefixed_key(SEQUENCER_INTERVAL_INDEX_PREFIX, &sequencer_hash(b"interval", &[start, end]))`
    的条目，值保留最大 `commit_ts`（重放/重建只前进不回退）。
- 新增 `sequencer_interval_point_in`：扫描全部区间项，命中条件
  `start <= key < end && commit_ts > start_ts`；`max_entries` 超预算返回
  `Exceeded { scanned: max_entries }`（0 = 不限）。
- 新增 `sequencer_interval_range_in`：命中条件
  `write.start < read_end（空=+∞）&& read_start < write.end && write.commit_ts > start_ts`；
  空读区间（`end` 非空且 `start >= end`）直接 `Clear`。
- `sequencer_point_query`：点索引与区间索引都查，返回 `commit_ts` 更大的命中。
- `sequencer_range_query_in`：先查有序点索引，再查区间索引，取 `commit_ts`
  更大的命中；同 ts 取字典序更小的 `key`。
- `sequencer_validate_in_tx`：
  - 把 `max_range` 的计算提前到读集循环之前；
  - point 读在哈希索引未命中后，用 `sequencer_interval_point_in(..., max_range)`；
  - range 读的 `sequencer_range_query_in` 自动包含区间索引；
  - `Exceeded` → `SequencerOutcome::Invalid { reason: RangeScanExceeded }`；
  - 冲突结果的 `conflict_key`：point 读返回被读 key，range 读返回区间起点。
- `rebuild_sequencer_validation_index` 的清空前缀列表加入
  `SEQUENCER_INTERVAL_INDEX_PREFIX`。

- [x] **Step 5: 实现决策 apply 与 changelog 对账**

`mvcc.rs` 的 `apply_commit_decisions`：

- 新增 `DeleteRange { start, end }` 分支：在 `[encode_kv_key(start),
  encode_kv_key(end))` 内扫描 `TABLE_KV`，对每个未删除 key 读
  `TABLE_KV_META` 并 `mark_deleted(commit_ts)`，按 key 追加
  `KeyValueChange { key, value: None, prev_value: None }`。
- `event_matches_mutations`：若 `mutations` 含任一 `DeleteRange`，返回 false
  （理由见 Rulings；正常重放靠游标跳过）。

`decision_apply.rs`：

- 新增 `clip_range(meta, start, end) -> Option<(Vec<u8>, Vec<u8>)>`：
  `start' = max(start, meta.start_key)`；
  `end' = min(end, meta.end_key)`（Region `end` 为空 = +∞；`end` 为空 = +∞）；
  `start' < end'`（或 `end'` 为 +∞）才保留。
- 新增 `clip_mutation(meta, mutation) -> Option<SequencerMutation>`：
  点写按 `point_key()` 判 `key_in_region`；区间写走 `clip_range`。
- `build_apply_batch` 改用 `filter_map(clip_mutation)`；`mutation_bytes` 增加
  `DeleteRange => start.len() + end.len() + 32`。

- [x] **Step 6: 运行测试确认通过**

Run:
```bash
cargo test -p coord-server --lib storage::mvcc::tests::test_sequencer_
cargo test -p coord-server --lib storage::mvcc::tests::test_apply_commit_decisions_
cargo test -p coord-server --lib kvt::decision_apply::tests::
cargo test -p coord-server --lib kvt::sequencer::tests::
```
Expected: PASS；新增 10 个测试全绿，既有 sequencer/索引/apply 测试不回归。

- [x] **Step 7: 提交**

```bash
git add coord-server/src/raft/type_config.rs coord-server/src/kvt/sequencer.rs \
  coord-server/src/storage/mvcc.rs coord-server/src/kvt/decision_apply.rs
git commit -m "feat(kv-t): support DELETE_RANGE decisions with interval validation index (#198)"
```

---

### Task 2: Commit RPC 接线与写集校验

**Files:**
- Modify: `coord-server/src/kvt/services.rs`（`validate_commit_mutations`）
- Test: `coord-server/src/kvt/services.rs` 的 `#[cfg(test)] mod`

**Interfaces:**
- Consumes: Task 1 的 `SequencerMutation::DeleteRange`。
- Produces: `Commit` 接受 `Op::DeleteRange`，非法形状返回
  `INVALID_ARGUMENT`（不占用会话提交）。

- [x] **Step 1: 写失败测试（RED）**

`services.rs` 测试模块：

- `test_validate_commit_mutations_accepts_delete_range`
  - `Op::DeleteRange, key=b"a", range_end=b"m"` →
    `Ok([DeleteRange { start: b"a", end: b"m" }])`；
- `test_validate_commit_mutations_rejects_bad_delete_range`
  - `range_end` 为空 → `INVALID_ARGUMENT`；
  - `range_end <= key` → `INVALID_ARGUMENT`；
  - `PUT`/`DELETE` 带非空 `range_end` 仍被拒（保留既有断言）。

Run: `cargo test -p coord-server --lib kvt::services::tests::test_validate_commit_mutations_accepts_delete_range`
Expected: FAIL（当前返回 `UNIMPLEMENTED`）。

- [x] **Step 2: 实现**

`validate_commit_mutations`：

```rust
Op::DeleteRange => {
    if mutation.range_end.is_empty() {
        return Err(attach(
            Status::invalid_argument("DELETE_RANGE requires a non-empty range_end"),
            CoordErrorCode::InvalidArgument,
        ));
    }
    if mutation.range_end <= mutation.key {
        return Err(attach(
            Status::invalid_argument("DELETE_RANGE requires key < range_end"),
            CoordErrorCode::InvalidArgument,
        ));
    }
    SequencerMutation::DeleteRange {
        start: mutation.key.clone(),
        end: mutation.range_end.clone(),
    }
}
```

同时更新该函数与 `commit` 的文档注释：不再声称区间写未交付。

- [x] **Step 3: 运行测试确认通过**

Run: `cargo test -p coord-server --lib kvt::services::tests::test_validate_commit_mutations_`
Expected: PASS（含既有 point 形状断言）。

- [x] **Step 4: 提交**

```bash
git add coord-server/src/kvt/services.rs
git commit -m "feat(kv-t): accept txn DELETE_RANGE in Commit (#198)"
```

---

### Task 3: e2e 跨 Region 原子可见与重放幂等

**Files:**
- Create: `coord/tests/kv_t_g3_delete_range_test.rs`（复制
  `kv_t_g3_txn_commit_test.rs` 的夹具并改名 `DeleteRangeNode`）
- Modify: 无需

**Interfaces:**
- Consumes: Task 1/2；`apply_pending_decisions`、`safe_ts`、
  `TxnV2::{begin,read,commit,abort}`、`KvV2::range`。
- Produces: 测试名供 #198 证据引用：
  `test_txn_delete_range_is_atomic_across_regions`、
  `test_txn_delete_range_replays_same_request_id_and_apply_is_idempotent`。

- [x] **Step 1: 建夹具与失败测试**

夹具同 `TxnCommitNode`：单节点、region 0 + 数据 Region 1 `[..m)`、2 `[m..)`，
seed `apple`（Region 1）与 `melon`（Region 2）于 `SEED_TS`、`zebra`（Region 2）
于 `SEED_TS - 1`（区间外对照；同一 Region 的 `commit_ts` 必须严格递增）。
`node.safe_ts()` 仍为 `SEED_TS`。新增辅助：

```rust
async fn commit_delete_range(
    &self, txn_id: &[u8], start: &[u8], end: &[u8], request_id: &[u8],
) -> Result<i64, tonic::Status>;
```

测试：

- `test_txn_delete_range_is_atomic_across_regions`
  - `begin(SEED_TS)` → `read(b"", b"z")` 返回 `apple`/`melon`（不含 `zebra`）；
  - `commit_delete_range(b"", b"z")` → `commit_ts == SEED_TS + 1`；
  - `apply_pending_decisions(&node, 16)` 后 `safe_ts == commit_ts`，
    且 Region 1/2 的 `last_applied_decision_ts()` 都等于 `commit_ts`；
  - `range(b"", b"z", commit_ts)` 为空；`range(b"zebra", b"", commit_ts)`
    仍为 `seed`。
- `test_txn_delete_range_replays_same_request_id_and_apply_is_idempotent`
  - 同一 `txn_id` + `request_id` 连续两次提交返回同一 `commit_ts`；
  - `sequencer_decisions_after(SEED_TS, 16)` 只有 1 条；
  - `apply_pending_decisions` 连续两次：第二次不报错、`safe_ts` 不变、
    被删 key 仍不可见。

Run: `cargo test -p coord --test kv_t_g3_delete_range_test test_txn_delete_range_`
Expected: 编译失败（测试文件/夹具未完成）→ 写完后若失败，按
superpowers:systematic-debugging 定位，不改断言迁就实现。

- [x] **Step 2: 运行确认通过**

Run: `cargo test -p coord --test kv_t_g3_delete_range_test test_txn_delete_range_`
Expected: 2 passed。

- [x] **Step 3: 提交**

```bash
git add coord/tests/kv_t_g3_delete_range_test.rs
git commit -m "test(kv-t): cover cross-region DELETE_RANGE visibility and replay (#198)"
```

---

### Task 4: e2e 冲突判定与输入校验

**Files:**
- Modify: `coord/tests/kv_t_g3_delete_range_test.rs`

**Interfaces:**
- Consumes: Task 3 的夹具与辅助。
- Produces: 测试名
  `test_txn_delete_range_conflicts_with_point_and_range_read_sets`、
  `test_txn_delete_range_rejects_invalid_shapes`。

- [x] **Step 1: 写失败测试（RED）**

- `test_txn_delete_range_conflicts_with_point_and_range_read_sets`
  - point 冲突：A `begin(SEED_TS)` + `read("apple")`；B 提交
    `DeleteRange [a, m)`；A 提交 → `ABORTED` + `TXN_CONFLICT`，且 A 的
    `commit_ts` 无决策；
  - range 冲突：C `read([a, f))`；D 提交 `DeleteRange [d, k)`；C 提交冲突；
  - 不相交：E `read([a, c))`；F 提交 `DeleteRange [m, z)`；E 提交成功
    （空写集，`Committed`）；
  - 相邻边界：G `read([a, m))`；H 提交 `DeleteRange [m, z)`；G 提交成功。
- `test_txn_delete_range_rejects_invalid_shapes`
  - `range_end` 为空 → `INVALID_ARGUMENT`；
  - `range_end <= key` → `INVALID_ARGUMENT`；
  - `OP_UNSPECIFIED` → `INVALID_ARGUMENT`；
  - 三次失败后同一会话仍可 `read` 并成功提交单 key 写。

Run: `cargo test -p coord --test kv_t_g3_delete_range_test test_txn_delete_range_`
Expected: 新增两条测试先失败（实现缺口）或通过（若 Task 1 已覆盖）；
无论哪种，逐条核对断言确实覆盖了上列语义，不允许空跑。

- [x] **Step 2: 修到通过**

Run: `cargo test -p coord --test kv_t_g3_delete_range_test`
Expected: 4 passed。

- [x] **Step 3: 提交**

```bash
git add coord/tests/kv_t_g3_delete_range_test.rs
git commit -m "test(kv-t): pin DELETE_RANGE OCC conflicts and input validation (#198)"
```

---

### Task 5: 文档、门禁、PR 与 issue 收口

**Files:**
- Modify: `docs/production/consumer-contract.md`
- Modify: `coord/src/main.rs`（启动 warning）
- Modify: `docs/superpowers/plans/2026-10-20-kv-t-g3-delete-range.md`（勾选/记录）

- [x] **Step 1: 文档与计划勾选**

`docs/production/consumer-contract.md` 的事务 bullet：把
「`DELETE_RANGE` 尚未交付（`UNIMPLEMENTED`，按单 key 删除）」替换为当前语义：

- `DELETE_RANGE` 删除 `[key, range_end)`（`range_end` 必须非空且大于 `key`）；
- 与 point/range 读集按区间相交判冲突，冲突返回 `ABORTED` + `TXN_CONFLICT`；
- 区间写与同一事务的其它 mutation 共享同一 `commit_ts`，跨 Region 由 `safe_ts`
  收敛后原子可见；
- 校验索引扫描受 `max_range` 预算约束，超限 `READ_SET_TOO_LARGE`。

`coord/src/main.rs` 的 KV-T 启动 warning：去掉
「and txn DELETE_RANGE stays UNIMPLEMENTED」，其余能力声明不变。

把本计划的复选框、`## 验证记录` 与验收矩阵按实际执行结果填写完整
（门禁通过后再勾选，不预留「应该通过」）。

- [x] **Step 2: 文档引用检查**

Run: `bash scripts/check-doc-refs.sh`
Expected: 悬空引用 = 0。

- [x] **Step 3: 本地门禁（AGENTS.md）**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-panics.sh
cargo audit
cargo deny check
cargo test -p coord-server --lib mvcc::tests::test_sequencer_
cargo test -p coord-server --lib kvt::decision_apply::tests::
cargo test -p coord-server --lib kvt::services::tests::test_validate_commit_mutations_
cargo test -p coord --test kv_t_g3_delete_range_test
```

Expected: 全部 PASS；`cargo audit` / `cargo deny check` 无新增 advisory/许可证问题。

- [x] **Step 4: 分支、PR、CI**

先确认当前改动都在工作分支上（`git switch -c feat/kv-t-g3-delete-range`），
再推送并开 PR：

```bash
git push -u origin feat/kv-t-g3-delete-range
gh pr create --base main \
  --title "feat(kv-t): txn DELETE_RANGE with interval validation index (#198)" \
  --body "Closes #198. 见 docs/superpowers/plans/2026-10-20-kv-t-g3-delete-range.md"
gh pr checks --watch
gh pr merge --merge --delete-branch
```

- [x] **Step 5: 证据评论与里程碑核对**

PR 合并后在 #198 评论列出：测试名（Task 3/4 的 4 个）、门禁命令与结果、
`safe_ts` 收敛断言、区间索引重建断言；核对 Milestone「KV-T G3 Serializable」
open_issues = 0（8/8 closed），并在 #127 无新增工作项。

- [x] **Step 6: 同步本地**

```bash
git switch main && git pull
```

---

## 验证记录

- 分支：`feat/kv-t-g3-delete-range`（自 `origin/main` = 50751ea）。
- 单元/集成：
  - `cargo test -p coord-server --lib storage::mvcc::tests::` → 117 passed；
  - `cargo test -p coord-server --lib kvt::` → 32 passed；
  - `cargo test -p coord --test kv_t_g3_delete_range_test` → 4 passed
    （跨 Region 原子可见、request-id/replay 幂等、point/range 冲突与
    非重叠/相邻、非法形状）；
  - `cargo test -p coord --test kv_t_g3_txn_commit_test` → 4 passed；
  - `cargo test -p coord --test kv_t_g3_commit_decision_test` → 2 passed。
- 门禁：
  - `cargo fmt --all -- --check` → OK；
  - `cargo clippy --workspace --lib --bins -- -D warnings` → OK；
  - `bash scripts/check-panics.sh` → 0 violations；
  - `cargo audit` → 仅既有 `RUSTSEC-2024-0449` allowed warning；
  - `cargo deny check` → advisories/bans/licenses/sources 全部 ok；
  - `bash scripts/check-doc-refs.sh` → 清除本机 gitignored 目录
    （`docs/` 下被 `.gitignore` 排除的本地评估目录、`.superpowers/`）后为
    OK（CI 状态）。

## 执行中的裁定

- **区间索引条目存完整边界**：哈希键去重、值存 `(start,end,commit_ts)`，
  扫描全量并受 `max_range` 约束；未实现有序区间树/压缩（容量归 #174）。
- **含 `DeleteRange` 的 changelog 对账返回 false**：游标与 changelog 同事务，
  正常重放靠游标跳过；能走到对账的同 ts 场景一律 fail-closed。
- **e2e 测试夹具的 `zebra` 写在 `SEED_TS - 1`**：同一 Region 的 `commit_ts`
  严格递增，不能与 `melon` 同为 `SEED_TS`；`safe_ts = min(current_revision)`
  仍为 `SEED_TS`。
- **Task 3/4 合并为一个提交**：两条验收追加同一新测试文件，四个测试名仍
  分别对应证据；不为了拆提交而复制夹具。
- **字节上限测试改用 `0xff` 上界**：最初用全 `0x00` 作 `end`，实际是
  `end < start` 的非法区间，被裁剪丢弃；测试数据修正，非实现放宽。
- **本地 doc-refs 的悬空引用来自 gitignored 文件**（`.gitignore` 的
  `docs/*`），CI 不包含；已在本机将本地评估目录与 `.superpowers/` 临时移出
  后验证为 OK。

## 验收矩阵

| #198 验收项 | 证据（测试/命令） | 状态 |
|:--|:--|:--|
| 区间删除跨 Region 原子可见（safe_ts 收敛） | `test_txn_delete_range_is_atomic_across_regions` | ☑ |
| apply/重放幂等、无部分可见 | `test_apply_commit_decisions_delete_range_is_atomic_and_replays`、`test_txn_delete_range_replays_same_request_id_and_apply_is_idempotent` | ☑ |
| point/range 读集与区间写冲突；不重叠不冲突 | `test_sequencer_range_delete_conflicts_with_point_and_range_read`、`test_txn_delete_range_conflicts_with_point_and_range_read_sets` | ☑ |
| `[kv_t].enabled=false` 时 v1 行为不变 | 既有 `kv_t_g0_test` / v1 套件 | ☑ |
| 门禁与文档 | Task 5 命令输出 + `check-doc-refs.sh` | ☑ |
