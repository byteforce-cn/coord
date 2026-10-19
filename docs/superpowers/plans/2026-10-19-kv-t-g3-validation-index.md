# KV-T G3（#147）ValidationIndex 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 #146 的 point 哈希索引旁补齐可区间查询的写索引：sequencer 能对任意
`[start, end)` 判断 `(start_ts, commit_ts]` 内是否有写命中，并提供快照携带、
启动追赶与显式重建。

**Architecture:** 写决策时在同一 redb 事务里同时推进两个派生索引：既有的
`/_sys/seq/vidx/{sha256(key)}`（point）与新的有序索引 `/_sys/seq/ridx/{key}`（range）。
range 查询对有序索引做**有界**区间扫描，预算耗尽返回 `Exceeded` 由 #149 fail-closed。
`/_sys/seq/vidx_last_ts` 记录索引已覆盖的决策水位；正常提交同事务推进，落后时按批追赶，
显式重建先清空索引再追赶。两者都随 `/_sys/*` 快照导出。

**Tech Stack:** Rust、redb、serde、sha2（无新增依赖/RPC）。

**Spec:** `docs/adr/0015-kv-t.md` D4/D9 + issue #147；前置 #146；消费方 #149；总表 #127。

## Global Constraints

- `[kv_t].enabled=false`（默认）时不做任何索引维护/追赶，v1 行为字节级不变（ADR-0001）。
- 索引是**派生数据**：`/_sys/seq/decision/` 才是事实源；任何索引内容都必须能由决策日志重建。
- apply 不因用户级结果返回 `Err`（#146 约束不变）；本项不改变任何 `SequencerOutcome` 的判定。
- 本项不新增公开 RPC，不改 `coord.txn.v2` 契约；range 谓词接入校验归 #149。
- 查询只回答「索引已覆盖的决策」；调用方必须在 `commit_ts > last_commit_ts` 的校验点调用
  （#146 已保证），本项不引入上界参数，也不承诺查询未来的写。
- 区间语义与 `range_in` 一致：`[start, end)`，`end` 为空 = 无上界；`start >= end` = 空区间。
- 不新增 `TODO`/`FIXME`；临时文件只放 `/tmp`；注释只写当前「为什么」。

## Rulings

- **有序索引落盘，不做内存区间树**：redb 的 B-tree 本身按 key 有序，`/_sys/seq/ridx/{key}`
  的区间扫描即区间索引；快照已按 `/_sys/*` 整域导出，无需新格式。内存上界是
  `O(scan limit)`（查询）与 `O(batch)`（追赶），不随索引总量增长。
- **保留 point 哈希索引**：D9 明确两者并存；point 查询 O(1)，range 查询走有序索引。
- **预算按“访问的索引项”计**：`Exceeded { scanned }` 表示区间内还有未检查的 key，调用方必须
  当作「无法证明无冲突」拒绝（#149），绝不能当 `Clear`。`limit = 0` 表示不限（与
  `range_in` 的既有约定一致）。
- **`start_ts` 是唯一时间下界**：sequencer apply 先检查 `commit_ts > last_commit_ts` 再校验，
  所以索引里的写都早于正在校验的 `commit_ts`；只比较 `ts > start_ts` 即精确。若未来允许
  乱序查询，必须改成多版本索引，不能偷偷复用本接口。
- **水位与追赶**：`/_sys/seq/vidx_last_ts` 与索引在同一 redb 事务里推进，因此正常路径不落后；
  水位让「索引丢失/旧快照/人工清理」可被检测并按批补。启动只做追赶，不做全量重建
  （全量重建是显式维护操作）。
- **索引保留窗口**：与决策日志/Commit Log 同寿；本项不做 GC，GC 归 #153/#157，容量归 #169/#174。
- **被写入的 key 包含 Delete**：删除也是写，必须让并发读谓词冲突（否则幻读）。
- **索引覆盖全局决策流，不按 Commit Log shard 拆分**：#146 的 sequencer 是 region 0 单组，
  OCC 校验必须全局串行；D9 的「每 shard」在 G3 就是这一个全局索引。Commit Log 分片（#153+）
  只影响日志存储/读取，不得把校验拆成互相看不见的分片。
- **追赶/重建要求写入静默**：索引落后的窗口会把冲突看成无冲突（假阴性），所以启动路径在
  对外服务前 fail-closed 追赶；`rebuild_*` 是离线维护 API，不能在服务中调用。
- **索引 key 明文**：`/_sys/` 域不加密（与 `/_changelog/` 已存的完整 key 同口径）；本项不改变
  Barrier 的加密边界，索引不引入新的敏感数据类别。

## Review Focus

1. **两个索引同步**：每条 mutation 同时推进哈希与有序索引；重放/同批重复写只前进不回退。
2. **区间边界**：`[start,end)`；`end` 空 = 无上界且不能扫出 `/_sys/seq/` 前缀；`start >= end` 为空。
3. **预算 fail-closed**：`Exceeded` 不得退化成 `Clear`；预算按访问项计，`limit=0` 不限。
4. **水位正确**：追赶从 `marker+1` 开始，不重复、不跳过；快照/重开后 marker 与索引一致。
5. **重建幂等**：清空后重建与原始索引对拍一致；重复重建结果不变。
6. **内存上界**：查询最多持有 `limit+1` 行，追赶最多持有 `batch` 条决策；索引不在内存常驻。
7. **关闭态**：`kv_t=false` 时启动路径不调用追赶，v1 行为不变。

---

### Task 1: 失败的索引语义测试

**Files:**
- Modify: `coord-server/src/storage/mvcc.rs`（`#[cfg(test)] mod`）

**Interfaces:**
- Consumes: `create_commit_ts_storage()`、`seq_op()`、`seq_txn()`、`applied_for()`、`SnapshotData`
  导出/导入辅助（既有测试）。
- Produces: 以下待实现接口的 RED 断言：
  `sequencer_point_query(&self, key, start_ts) -> Result<Option<SequencerIndexHit>>`、
  `sequencer_range_query(&self, start, end, start_ts, max_entries) -> Result<SequencerRangeQuery>`、
  `catch_up_sequencer_index(&self, batch) -> Result<SequencerIndexRebuild>`、
  `rebuild_sequencer_validation_index(&self, batch) -> Result<SequencerIndexRebuild>`。

- [x] **Step 1: 写 4 个失败测试**

```rust
#[test] fn test_sequencer_index_point_query() {
    // txn@10 put a/b/d；txn@12 overwrite a + delete c；txn@15 put e
    // point a start_ts=9 => Hit{a,12}; start_ts=12 => None; z => None
}
#[test] fn test_sequencer_range_query_boundaries_and_deletes() {
    // [a,b) => a@12; [b,d) start_ts=11 => c@12（Delete 也算写）;
    // [a,e) => a@12（同 ts 取字典序最小）; [a,"") => e@15; [c,c) => Clear; [e,b) => Clear
}
#[test] fn test_sequencer_range_query_scan_limit_is_fail_closed() {
    // k0..k4：max_entries=3 => Exceeded{scanned:3}；=5 => Hit k4；=0 => Hit k4
}
#[test] fn test_sequencer_index_matches_brute_force() {
    // 200 条递增 commit_ts、随机 key（k00..k19）与随机 start_ts；
    // 100 个随机 [start,end) 查询与决策日志暴力扫描对拍（含同 ts 取字典序最小）
}
```

- [x] **Step 2: 跑 RED**

Run: `cargo test -p coord-server --lib sequencer_index -- --nocapture`
Expected: 编译失败（`SequencerIndexHit`/`SequencerRangeQuery`/查询与追赶方法不存在）。

### Task 2: 有序索引记录与区间查询

**Files:**
- Modify: `coord-server/src/storage/mvcc.rs`

**Interfaces:**
- Produces（后续任务/ #149 依赖的精确签名）：

```rust
pub struct SequencerIndexHit { pub key: Vec<u8>, pub commit_ts: Revision }

pub enum SequencerRangeQuery {
    Hit(SequencerIndexHit),
    Clear,
    Exceeded { scanned: usize },
}

pub fn sequencer_point_query(&self, key: &[u8], start_ts: Revision)
    -> Result<Option<SequencerIndexHit>>;
pub fn sequencer_range_query(&self, start: &[u8], end: &[u8], start_ts: Revision,
    max_entries: usize) -> Result<SequencerRangeQuery>;
```

- [x] **Step 1: 常量与查询**
  - `SEQUENCER_RANGE_INDEX_PREFIX = b"/_sys/seq/ridx/"`、`SEQUENCER_INDEX_LAST_TS =
    b"/_sys/seq/vidx_last_ts"`；`prefix_upper_bound(prefix) -> Vec<u8>`（末字节 +1，必有界，
    因为前缀以 `/` 结尾）。
  - point：读哈希索引，`ts > start_ts` 返回 `Hit{key.clone(), ts}`，否则 `None`。
  - range：`start >= end`（end 非空）直接 `Clear`；`iter_range_limited(
    TABLE_KV, ridx+start, end 为空 ? ridx 上界 : ridx+end, limit==0 ? 0 : limit+1)`；
    取 `ts > start_ts` 的最大 ts（同 ts 保留先扫到的、字典序更小的 key）；行数 > limit 返回
    `Exceeded { scanned: limit }`。
- [x] **Step 2: 提交路径同步推进两个索引**
  - 抽出 `sequencer_index_writes_in_tx(tx, decision) -> Result<u64>`：遍历 mutations，哈希索引
    与有序索引都只前进；commit 路径在写决策后调用，并把 `SEQUENCER_INDEX_LAST_TS` 设为
    `commit_ts`（同一事务）。
- [x] **Step 3: 跑 GREEN（Task 1 的 point/range/limit/brute-force）**

Run: `cargo test -p coord-server --lib sequencer_index -- --nocapture`
Expected: 4 个测试 PASS。

### Task 3: 有界区间扫描原语

**Files:**
- Modify: `coord-core/src/storage.rs`（`ReadTx` 默认方法）
- Modify: `coord-server/src/storage/redb_backend.rs`（`RedbReadTx`/`RedbWriteTx` 覆盖）

**Interfaces:**
- Produces: `ReadTx::iter_range_limited(&self, table, start, end, limit) -> Result<Vec<(Vec<u8>, Vec<u8>)>>`；
  `limit=0` 不限，否则最多返回 `limit` 行，语义同 `iter_range`（`[start,end)`、有序）。

- [x] **Step 1: trait 默认实现 + redb 流式覆盖**
  - 默认实现：调用 `iter_range` 后 `truncate(limit)`（其他后端语义正确）。
  - redb 两个 impl 在 `table.range(start..)` 迭代中达到 `limit` 立即 `break`，不物化整个区间。
- [x] **Step 2: 跑 GREEN**

Run: `cargo test -p coord-server --lib sequencer_range_query_scan_limit_is_fail_closed`
Expected: PASS（行为不变；此任务把上界从「先全量物化再截断」变为真正的流式上界）。

### Task 4: 追赶、重建与启动接线

**Files:**
- Modify: `coord-server/src/storage/mvcc.rs`
- Modify: `coord/src/main.rs`

**Interfaces:**
- Produces:

```rust
pub struct SequencerIndexRebuild {
    pub from_ts: u64,
    pub to_ts: u64,
    pub decisions: u64,
    pub keys: u64,
}

pub fn sequencer_index_last_ts(&self) -> Result<u64>;
pub fn catch_up_sequencer_index(&self, batch: usize) -> Result<SequencerIndexRebuild>;
pub fn rebuild_sequencer_validation_index(&self, batch: usize)
    -> Result<SequencerIndexRebuild>;
```

- [x] **Step 1: 写 2 个失败测试**

```rust
#[test] fn test_sequencer_index_catch_up_after_lag() {
    // 删掉 ridx 与 marker 模拟落后 => 查询 Clear；catch_up(batch=2) 后与暴力扫描一致，
    // decisions=3、第二次调用 decisions=0；rebuild(batch=2) 结果与原始一致
}
#[test] fn test_sequencer_index_survives_reopen_and_snapshot() {
    // reopen 后 range/point 仍命中；export_snapshot_data -> import_snapshot_data 到新库后同样命中
}
```

Run: `cargo test -p coord-server --lib sequencer_index_catch_up -- --nocapture`
Expected: 编译失败（追赶/重建方法不存在）。

- [x] **Step 2: 按批追赶**
  - 读事务取 `marker` 与 `decision/{marker+1}` 起最多 `batch` 条决策；写事务逐条重放索引并推进
    marker（处理完最后一条才推进）；循环到无新决策或追上 `sequencer_last_commit_ts`。
  - `batch = 0` 按 1 处理；返回 `from_ts`（首次 marker）、`to_ts`（末条决策 ts）、`decisions`、
    `keys`（处理的 mutation 数）。
- [x] **Step 3: 显式重建**
  - 一个写事务内清空 `/_sys/seq/vidx/`、`/_sys/seq/ridx/` 与 marker，再调用追赶。
- [x] **Step 4: 启动接线（仅 kv_t 开启）**
  - `coord/src/main.rs` 在 `cfg.kv_t.enabled` 的 v2 初始化段调用
    `mvcc.catch_up_sequencer_index(cfg.kv_t.commit_sequencer_batch as usize)`；错误即启动失败
    （fail-closed），有追赶量时 `tracing::info!` 记录 `from/to/decisions/keys`；关闭态不调用。
- [x] **Step 5: 跑 GREEN**

Run: `cargo test -p coord-server --lib sequencer_index -- --nocapture`
Expected: 6 个索引测试全 PASS。

### Task 5: 文档、门禁与 PR

- [x] **Step 1: 勾选计划与验证记录**（测试名、命令、缺口）
- [x] **Step 2: 本机跑 CI 基础门禁**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-panics.sh
cargo audit
cargo deny check
cargo test --workspace --no-fail-fast
bash scripts/check-doc-refs.sh
```

- [ ] **Step 3: PR**
  - 分支 `feat/kv-t-g3-validation-index`，PR 正文写 `Closes #147`、两条边界
    （「#149 才接线谓词校验」「索引只覆盖 sequencer 决策，直写不经索引」）。
  - `gh pr checks --watch` 全绿后 `gh pr merge --merge --delete-branch`。
- [ ] **Step 4: 关闭 #147 的证据评论**（测试名/命令/对拍结论/内存上界说明）

## 验收矩阵

| 用例 | 断言 |
|:--|:--|
| point 命中 | `ts > start_ts` 返回 `Hit`；`<= start_ts` 返回 `None` |
| range 命中 | `[start,end)` 内最大 `commit_ts`；end 排除；Delete 计入写 |
| 空区间 | `start == end`、`start > end` 均 `Clear` |
| 无上界 | `end=""` 扫到有序索引前缀末尾，不越界到其他 `/_sys/` 记录 |
| 预算 | 超过 `max_entries` 返回 `Exceeded`，不返回 `Clear`；`limit=0` 不限 |
| 对拍 | 200 条决策 × 100 个随机区间与暴力扫描完全一致（含同 ts 取字典序最小） |
| 落后追赶 | 删除索引+marker 后按 batch=2 追平，决策数一致且不重复 |
| 重建 | 清空后重建与原始查询逐项一致；第二次重建 decisions=0 |
| 重开/快照 | reopen 与 import_snapshot_data 后 point/range 仍命中，marker 随快照恢复 |
| 关闭态 | `kv_t=false` 不调用追赶；v1 测试全绿 |

## 验证记录

- `cargo test -p coord-server --lib sequencer`：17 passed（含 6 个 #147 索引测试：
  point、range 边界/Delete、预算 fail-closed、200 条决策 × 170 次查询暴力对拍、
  落后追赶/重建、reopen + 快照）。
- `cargo test -p coord-server --test kv_t_g3_sequencer_test`：3 passed（#146 回归）。
- `cargo test --workspace --no-fail-fast -- --test-threads=4`：EXIT=0。
- `cargo fmt --all -- --check`、`cargo clippy --workspace --lib --bins -- -D warnings`、
  `bash scripts/check-panics.sh`：通过。
- `cargo audit`：1 条既有 allowed warning（unsafe-libyaml unmaintained）；
  `cargo deny check`：advisories/bans/licenses/sources 全 ok。
- `bash scripts/check-doc-refs.sh`：在仅含 Git 跟踪文件的 detached worktree 中 OK
  （本地工作区的悬空引用全部来自 gitignored `docs/pki-assessment/`）。
