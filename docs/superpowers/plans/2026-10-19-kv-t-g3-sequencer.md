# KV-T G3（#146）Commit Sequencer 与 ValidateAndCommit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 region 0 system raft 上交付全局 Commit Sequencer：按日志顺序校验事务读集、分配
`commit_ts`、写 `CommitDecision`；冲突/重复/乱序都是结构化响应，不写决策、不让 apply 返回 `Err`。

**Architecture:** 新命令 `Command::Sequencer(SequencerOp::ValidateAndCommit)` 只经 region 0
raft 提交。leader 侧的 `CommitSequencer` 把候选事务按 `commit_sequencer_batch`（默认 256）与
`max_txn_bytes` 切批，持进程内互斥锁从 TSO 取 `commit_ts` 后提案，保证提案顺序 = 时间戳
顺序。apply 在 `/_sys/seq/` 下原子写决策、幂等索引与 point-key 校验索引；`/_sys/*` 随 region 0
快照导出/恢复。

**Tech Stack:** Rust、openraft/redb、serde、sha2、tonic/prost（无新增 RPC）。

**Spec:** `docs/adr/0015-kv-t.md` D2/D4/D9/D11/D12 + issue #146；总表 #127。

## Global Constraints

- `[kv_t].enabled=false`（默认）时不装配 sequencer、不提案任何 `Command::Sequencer`，v1 行为
  字节级不变（ADR-0001）。
- `commit_ts` 必须来自 TSO；apply 不分配时间戳、不读配置/墙钟（确定性）。
- apply 不因用户级结果返回 `Err`：冲突、重复、乱序、非法时间戳都返回 `SequencerOutcome`
  （D11.5）。只有存储/编码失败才是 `Err`。
- `commit_ts` 全局唯一、单调；同一事务所有 key 共享同一 `commit_ts`（D11.1/D11.2）。
- 冲突只写响应、不写决策；重复请求返回首次 `commit_ts`，不重复分配。
- 不新增公开 RPC：`coord.txn.v2.Commit` 仍由 #151 接线；本项交付内部 sequencer 与命令。
- Range 谓词/区间写不在本项：内部命令类型只表达 point read set 与 point mutation，
  不会静默丢弃 range（#147/#149 扩展时必须显式改类型）。
- 不新增 `TODO`/`FIXME`；临时文件只放 `/tmp`；注释只写当前「为什么」。

## Rulings

- **单 Raft 组 = region 0 system raft**：D4 要求单组串行化点；首个交付阶段复用 TSO 所在的
  region 0，不新建第二个 Raft group。独立 sequencer group 属于后续扩容项。
- **时间戳顺序 = 提案顺序**：`CommitSequencer` 用 `tokio::sync::Mutex` 串行化「取 TSO →
  `client_write`」。后到的 apply 若发现 `commit_ts <= last_commit_ts`，返回 `OutOfOrder`
  且不写决策（D11.1 的最后防线；leader 切换/重复决策的完整处理归 #152）。
- **校验索引本项只做 point key**：`/_sys/seq/vidx/{sha256(key)}` → 最大 `commit_ts`。
  point read set 的 key 只要 `max_write_ts > start_ts` 即冲突。range 谓词与区间写由
  #147（区间索引）/#149（幻读）交付；命令类型不提供 range 字段，避免「收下但忽略」。
- **幂等键**：`txn_id` 单次提交（同一 `txn_id` 永不产生第二个决策） + 非空 `request_id`
  去重。`request_id` 被另一个 `txn_id` 命中时返回 `Invalid(RequestIdReused)`，不返回别人的
  决策。空 `txn_id` → `Invalid(MissingTxnId)`。
- **决策日志**：`/_sys/seq/decision/{commit_ts BE}` 存完整 `SequencerDecision`（txn_id、
  request_id、start_ts、commit_ts、mutations）。不写 changelog、不产生 Watch 事件；
  Commit Log/Watch 归 G4（#153+）。
- **快照**：决策/索引/幂等/`last_commit_ts` 全部在 `/_sys/seq/`，随
  `SnapshotData::sys_entries` 导出恢复（与 `/_sys/tso/` 同一域）。
- **批边界**：条数上限 `commit_sequencer_batch`；字节上限 `max_txn_bytes`（单事务可能超过
  上限，此时独占一批，绝不拆分事务）。批内顺序 = 最终 `commit_ts` 顺序；失败提案留下的
  TSO 空洞按 D2 允许。
- **直写 API 的边界**：`coord.kv.v2` 直写仍按 #141 使用调用方 `commit_ts`，不经 sequencer，
  因此**不被本项的校验索引记录**。普通业务写入必须走 #151 的 `Txn.Commit`；直写与 OCC 的
  交互在 #149/#168 收口（写入 PR 的 Review Focus，不静默承诺）。
- **指标**：apply 侧记录 `batches`/`txns`/`conflicts`/`validation duration`，用于批大小、
  冲突率与校验耗时观测。
- **决策日志保留**：本项不做 GC（Commit Log 分片/保留窗口归 #153/#157）；
  `/_sys/seq/decision/` 随 region 0 状态增长，G6 备份/容量项（#169/#174）收口。

## Review Focus

1. **apply 不 wedge**：冲突/重复/乱序/非法时间戳都返回响应变体；任何用户输入都不能让
   Raft apply 返回 `Err`。
2. **单调且不复用**：重复请求先命中幂等索引（返回首次 `commit_ts`，即使新提案带了更大的
   时间戳）；非重复的旧时间戳命中 `OutOfOrder`，不写决策。
3. **冲突窗口正确**：只在 `(start_ts, commit_ts]` 内命中写才冲突；`start_ts` 之后、本事务
   `commit_ts` 之前的写可见并冲突；本事务之后的写不影响本次决策。
4. **恢复**：决策、索引、幂等键、`last_commit_ts` 在同一 redb 事务里；重开/装快照后重放
   同一事务不产生第二个决策。
5. **批边界**：260 条候选按 256 切两批；跨批 `commit_ts` 仍严格递增；超大事务不被拆分。
6. **关闭态零变化**：未装配 sequencer 时没有提案路径；`Txn.Commit` 仍是门禁后的未实现。

---

### Task 1: 失败的存储语义测试

**Files:**
- Modify: `coord-server/src/storage/mvcc.rs`（`#[cfg(test)] mod`）

**Interfaces:**
- Consumes: `AppliedLogId::standalone`、`create_storage()`、`RevisionMode::CommitTs`。
- Produces: 以下待实现接口的 RED 断言：
  `apply_sequencer_op(&SequencerOp, revision, applied) -> Result<Vec<SequencerOutcome>>`、
  `get_sequencer_decision(commit_ts) -> Result<Option<SequencerDecision>>`、
  `sequencer_last_commit_ts() -> Result<u64>`。

- [x] **Step 1: 写 5 个失败测试**

```rust
#[test] fn test_sequencer_orders_conflicting_txns() {
    // batch [txn1(read k, put k=v1, start=1, commit=10),
    //        txn2(read k, put k=v2, start=2, commit=11)]
    // => [Committed{10}, Conflict{commit_ts:11, conflict_key:k, conflict_ts:10}]
    // txn2 无决策；last_commit_ts == 10
}
#[test] fn test_sequencer_conflict_writes_no_decision() { /* 只提交冲突 txn */ }
#[test] fn test_sequencer_retry_is_idempotent() {
    // 同 txn_id + request_id、更大 commit_ts => Duplicate{首次 ts}；决策数仍 1
}
#[test] fn test_sequencer_rejects_out_of_order_commit_ts() {
    // 先 commit=20，再不同 txn commit=19 => OutOfOrder{19,20}，无第二个决策
}
#[test] fn test_sequencer_decision_and_index_survive_reopen() {
    // drop → reopen；决策/索引/last_ts 仍在；重放同一命令仍是 Duplicate
}
```

- [x] **Step 2: 跑 RED**

Run: `cargo test -p coord-server --lib sequencer -- --nocapture`
Expected: 编译失败（`SequencerOp`/`apply_sequencer_op` 不存在）。

### Task 2: 命令、决策记录与 apply

**Files:**
- Modify: `coord-server/src/raft/type_config.rs`
- Modify: `coord-server/src/storage/mvcc.rs`

- [x] `SequencerMutation`（Put/Delete point）、`SequencerTxn`（txn_id/start_ts/commit_ts/
  request_id/read_set/mutations）、`SequencerOutcome`、`SequencerInvalid`、
  `SequencerOp::ValidateAndCommit { txns }`；`Command::Sequencer` / `Response::Sequencer`
  **末尾追加**。
- [x] `SequencerDecision` + `/_sys/seq/*` 编码；`apply_sequencer_op` 单事务内按顺序：
  幂等 → 时间戳合法 → 乱序 → 读集冲突 → 写决策/索引/`last_commit_ts` → `META_LAST_APPLIED`。
- [x] 公开 `get_sequencer_decision` / `sequencer_last_commit_ts`。
- [x] `state_machine.rs` apply 分支 + `record_sequencer_batch` 指标（Task 4）。
- [x] Run: `cargo test -p coord-server --lib sequencer` → GREEN。

### Task 3: leader 侧切批与 TSO 取号

**Files:**
- Create: `coord-server/src/kvt/sequencer.rs`
- Modify: `coord-server/src/kvt/mod.rs`

- [x] `CommitCandidate { txn_id, start_ts, request_id, read_set: BTreeSet<Vec<u8>>, mutations }`。
- [x] `CommitSequencer::new(raft, node_id, tso, storage, batch, max_batch_bytes, propose_timeout)`；
      `validate_and_commit(Vec<CommitCandidate>) -> Result<Vec<SequencerOutcome>>`。
- [x] `batch_ranges(sizes, max_count, max_bytes)` 纯函数：条数/字节双上限、单事务独占、
      保持顺序（单测：0/1/256/257/oversize）。
- [x] 持 `propose` 互斥锁（并发调用排队 = 背压）：逐条
      `tso.get_ts_above(storage.safe_ts_fence()?)` 赋 `commit_ts` → `client_write`；
      提案带 `[limits].write_timeout_ms` 超时；非 leader / 失败 / 超时按 `NotLeader` /
      `ClusterUnavailable` 分类。
- [x] 单测 `batch_ranges`（0/1/256/257/超大事务）；集成测试用真实 TSO 验证 260 条切两批、
      `commit_ts` 严格递增、响应长度一致。

### Task 4: 指标与节点装配

**Files:**
- Modify: `coord-server/src/metrics.rs`
- Modify: `coord-server/src/server/mod.rs`
- Modify: `coord/src/main.rs`

- [x] `Metrics::{record_sequencer_batch, kvt_sequencer_*}` + Prometheus HELP/TYPE + 指标单测。
- [x] `CoordNode` 新增 `sequencer: Option<Arc<CommitSequencer>>` 与
      `set_commit_sequencer` / `commit_sequencer()`；默认 `None`。
- [x] `main.rs`：仅在 `[kv_t].enabled` 且有 region 0 raft 时构造（与 TSO client 同一
      段），传入 `commit_sequencer_batch` 与 `max_txn_bytes`。
- [x] `[kv_t].enabled=false` 时无 sequencer 装配；启动日志不改承诺（Commit 仍归 #151）。

### Task 5: 端到端与多节点验收

**Files:**
- Create: `coord-server/tests/kv_t_g3_sequencer_test.rs`

- [x] 单节点 region 0 raft + 真实 `TsoAllocator`/`TsoClient`：
      冲突顺序可复现、幂等重试、260 条两批、指标计数。
- [x] 两节点 region 0 raft：leader 提交决策；follower 的 sequencer 返回 `NotLeader`；
      follower 副本最终看到同一决策（raft 复制）。
- [x] 快照恢复：`export_snapshot_data` → `import_snapshot_data` 到新库，
      决策/索引/`last_commit_ts` 恢复，重放同一事务仍幂等。
- [x] Run: `cargo test -p coord-server --test kv_t_g3_sequencer_test` → GREEN。

### Task 6: 文档与门禁

- [x] 本计划勾选 + 验证记录；PR 描述写明「Txn.Commit 仍归 #151」「直写不经过校验索引」
      两条边界。
- [x] `cargo fmt --all -- --check`
- [x] `cargo clippy --workspace --lib --bins -- -D warnings`
- [x] `bash scripts/check-panics.sh`
- [x] `cargo audit --no-fetch` / `cargo deny check`
- [x] `cargo test --workspace --no-fail-fast`
- [ ] 关闭 #146 前逐条勾选验收并在评论写证据（测试名/命令）。

## 验证记录

- `cargo test -p coord-server --lib sequencer`：11 passed（决策顺序/冲突不写决策/幂等/
  乱序/重开恢复/快照恢复/batch_ranges/指标）。
- `cargo test -p coord-server --test kv_t_g3_sequencer_test`：3 passed（真实 TSO 冲突顺序、
  260 条两批与指标、两节点 follower NotLeader + 决策复制）。
- `cargo test --workspace --no-fail-fast`：全绿。
- fmt / clippy（`--lib --bins -D warnings`）/ check-panics / audit / deny 通过。
- `bash scripts/check-doc-refs.sh`：本计划与代码引用可达（CI 通过）。本地工作区另有 3 条
  来自 gitignored 历史文档的既有悬空引用，不在 git 树内。

## 验收矩阵

| 用例 | 断言 |
|:--|:--|
| 冲突批 `[txn1, txn2]` | 前一条 Committed；后一条 Conflict 且 `conflict_ts` = 前一条 ts |
| 冲突不写决策 | `get_sequencer_decision(conflict_ts)` 为 None；`last_commit_ts` 不变 |
| 幂等重试 | 同 txn/request 返回 Duplicate(首次 ts)；决策数 1 |
| 乱序提案 | OutOfOrder，不写决策 |
| 260 条 / batch=256 | 两批；260 个 Committed；ts 严格递增；指标 batches=2 |
| 重启 | 决策/索引/last_ts 恢复；重放不重复 |
| 快照 | 装快照后同样恢复 |
| 2 节点 | follower 提交 NotLeader；follower 副本收敛到 leader 的决策 |
| 关闭态 | 无 sequencer 装配；`Txn.Commit` 仍 FAILED_PRECONDITION/UNIMPLEMENTED |
