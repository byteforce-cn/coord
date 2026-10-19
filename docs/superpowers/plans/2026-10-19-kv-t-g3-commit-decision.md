# KV-T G3（#148）CommitDecision 跨区应用与读可见性 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 region 0 已持久化的 `CommitDecision` 按 `commit_ts` 顺序应用到各数据 Region 的
MVCC；同一 Region 内的多 key 原子可见，跨 Region 由 safe_ts 收敛；崩溃后从每 Region 的
持久化游标续跑，重放不重复写 changelog。

**Architecture:** 每个数据 Region 维护自己的已应用决策游标
（`/_sys/kv_t/decision_applied`，随 Region 快照恢复）。节点上的 applier 扫描 region 0
决策日志，只把属于本 Region 的 mutations 装进 `Command::ApplyCommitDecisions`，经该 Region
raft 一次事务 apply；非本 Region 的决策以空切片推进游标。safe_ts 推进时以「该 Region 下一条
未应用决策 - 1」封顶，sequencer 提案与 fence 发布共用同一把锁，保证 fence 发布后不会再有
`commit_ts <= fence` 的新决策。

**Tech Stack:** Rust、openraft/redb、serde、tokio（无新增依赖/RPC）。

**Spec:** `docs/adr/0015-kv-t.md` D4/D12.3/D11 + issue #148；前置 #138/#142/#146/#147；
消费方 #149/#151；总表 #127。

## Global Constraints

- `[kv_t].enabled=false`（默认）时不启动 applier、不提案任何 v2 命令，v1 行为字节级不变
  （ADR-0001）。
- decision 是提交点：`Commit` 不等 apply；read_ts 覆盖 commit_ts 但 Region 未 apply 时，
  读按 #142 的 safe_ts 等待/拒绝，不得绕过 decision 直接读 Region。
- 同一 Region 内一个决策的所有 mutation 在**一个 redb 写事务**里完成；多 key 不得部分可见。
- 不同 Region 之间不做分布式事务：原子可见性由 `safe_ts = min(已 apply 水位)` 保证。
- 漂移应用（`commit_ts <= current_revision` 且无 changelog）不得静默丢数据：停止在坏决策
  之前、保留游标、告警，由 fence 封顶逻辑保证正常路径不会发生。
- 用户级冲突/拒绝不得让 Raft apply 返回 `Err`（D11.5）；apply 只返回进度响应。
- 幂等：同一决策重放不重复写 changelog、不重复推进 version；游标只前进。
- 不新增 `TODO`/`FIXME`；临时文件只放 `/tmp`；注释只写当前「为什么」。

## Rulings

- **applier 按 Region 独立推进，不做全局 applied 游标**：每个 Region 的 leader 只 apply
  自己负责的 key 切片；空切片也推进游标，否则空闲 Region 会永远钉住 safe_ts。Region
  快照已按 `/_sys/*` 整域导出，游标放 `/_sys/kv_t/decision_applied` 即随快照恢复。
- **fence 发布与 sequencer 提案共用锁**：`CommitSequencer::propose` 已是「取 TSO → 提案」
  的串行点；safe_ts 发布 fence 时持有同一把锁，因此发布瞬间没有 in-flight 决策，且
  `get_ts_above(fence)` 保证之后分配的 `commit_ts > fence`。这替代了「维护 in-flight
  最小 commit_ts」的独立结构（#142 注释预留的计算点），语义等价且无新竞态。
- **safe_ts 逐 Region 以 pending decision 封顶**：`AdvanceSafeTs` 是纯水位推进；
  若 Region 尚未 apply 决策 D，就把 current_revision 抬过 D，D 再 apply 会被
  `CommitTs` 策略拒绝。因此每 Region 的目标 = `min(fence, next_pending - 1)`；
  `next_pending` 从该 Region 游标起扫本地 region 0 决策日志。
- **空决策也推进 current_revision**：Region 对该决策没有 key，说明它已「应用」到该
  `commit_ts`；推进水位不会伪造数据，反而让 safe_ts 不依赖 fence 周期。
- **apply 不产生 Watch 事件**：严格透明 Watch 归 G4（#153+）；本项只写本地 changelog。
- **直写 `coord.kv.v2` 与决策的交错不在本项收口**：直写绕过 sequencer，可能把 Region
  水位抬到未应用决策之上；#149/#168 负责把普通业务写入全部收敛到 Txn。本项在检测到
  漂移时 fail-closed（停止 + 告警），不静默覆盖。

## Review Focus

1. **单 Region 原子性**：一个决策的 Put/Delete 混合在一个 redb 事务；崩溃后要么全有要么全无。
2. **跨 Region 可见性**：apply 前 `read_ts >= commit_ts` 的读必须等 safe_ts，不能读到一半；
   apply 后 `read_ts < commit_ts` 仍看不到。
3. **重放幂等**：同一批决策重复提案、或崩溃后从旧游标重跑，changelog/version 不重复。
4. **游标与快照**：reopen、装快照后游标不回退；旧快照/丢失游标最多重放（幂等），不跳过。
5. **fence 不越过未应用决策**：pending decision 存在时 `advance_once` 只能抬到 `D-1`；
   apply 后下一轮才到 fence。
6. **关闭态**：`kv_t=false` 时没有 applier 任务/提案路径，v1 测试全绿。

---

### Task 1: 存储层失败的 apply 语义测试

**Files:**
- Modify: `coord-server/src/storage/mvcc.rs`（`#[cfg(test)] mod`）

**Interfaces:**
- Consumes: `create_commit_ts_storage()`、`applied_for()`、`export_snapshot_data` /
  `import_snapshot_data`（既有测试辅助）。
- Produces 待实现接口的 RED 断言：
  `apply_commit_decisions(&self, decisions: &[CommitDecisionApply], revision, applied)
  -> Result<CommitDecisionApplyResult>`、
  `last_applied_decision_ts(&self) -> Result<Revision>`、
  `sequencer_decisions_after(&self, after_ts, limit) -> Result<Vec<SequencerDecision>>`。

- [x] **Step 1: 写 5 个失败测试**

```rust
#[test] fn test_apply_commit_decisions_multi_key_is_atomic() {
    // 一个 commit_ts=10 的决策：Put a、Put b、Delete c
    // => a/b 可见、c 删除、current_revision=10、游标=10
    // changelog@10 只有一个 Txn 事件、3 条 changes；b 的 version=1
}
#[test] fn test_apply_commit_decisions_replay_is_idempotent() {
    // apply [10,20] → 再 apply 同一批 → version/changelog 不变、through=20
    // 再 apply [20,30]：20 跳过、30 生效
}
#[test] fn test_apply_commit_decisions_empty_advances_watermark() {
    // 空 mutations 的决策：current_revision/游标前进，但不写 changelog
}
#[test] fn test_apply_commit_decisions_cursor_survives_reopen_and_snapshot() {
    // reopen 后游标=20；export -> 新库 import 后游标=20、重复 apply 为 no-op
}
#[test] fn test_apply_commit_decisions_stops_before_fence_overtake() {
    // 先把 current_revision 抬到 100（advance_commit_ts_watermark），
    // 再 apply commit_ts=50 且无 changelog 的决策 => through 停在 0/前一条，不写数据
}
```

- [x] **Step 2: 跑 RED**

Run: `cargo test -p coord-server --lib apply_commit_decisions -- --nocapture`
Expected: 编译失败（`CommitDecisionApply`/方法不存在）。

### Task 2: 批量 apply、游标与决策扫描

**Files:**
- Modify: `coord-server/src/storage/mvcc.rs`
- Modify: `coord-server/src/raft/type_config.rs`（payload 类型）

**Interfaces:**
- Produces（后续任务/测试依赖的精确签名）：

```rust
pub struct CommitDecisionApply { pub commit_ts: u64, pub mutations: Vec<SequencerMutation> }
pub struct CommitDecisionApplyResult { pub through_ts: Revision, pub decisions: u64 }

pub fn apply_commit_decisions(
    &self,
    decisions: &[crate::raft::type_config::CommitDecisionApply],
    revision: Revision,
    applied: AppliedLogId,
) -> Result<CommitDecisionApplyResult>;
pub fn last_applied_decision_ts(&self) -> Result<Revision>;
pub fn sequencer_decisions_after(
    &self, after_ts: Revision, limit: usize,
) -> Result<Vec<SequencerDecision>>;
pub fn next_sequencer_decision_ts(&self, after_ts: Revision) -> Result<Option<Revision>>;
```

- [x] **Step 1: 常量与游标**
  - `DECISION_APPLIED_TS = b"/_sys/kv_t/decision_applied"`（u64 BE；放 `/_sys/` 以随
    Region 快照整域恢复）。
  - `last_applied_decision_ts` 读缺失 = 0。
- [x] **Step 2: 单事务批量 apply**
  - 先读游标；对每条 `commit_ts > through` 的决策：
    - changelog 已存在且 changes 与 mutations 一致 → 视为已应用，只前进 through；
    - changelog 存在但不一致 → 停在该决策之前；
    - 不存在但 `commit_ts <= current_revision` → 停在该决策之前（fence 漂移，fail-closed）；
    - 否则按 mutation 顺序写 KV/元数据（Put 加密、Delete 标 tombstone、lease_id 照抄），
      最后写一个 `EventType::Txn` 的 `ChangeEvent`（changes 含全部 mutation，空 mutations
      不写 changelog）。
  - 结束后写 `META_CURRENT_REVISION = max(current, through)`、游标、`META_LAST_APPLIED`；
    `decisions` = 本次真正推进的条数。
- [x] **Step 3: 决策日志有界扫描**
  - `sequencer_decisions_after`：从 `decision/{after_ts+1}` 起扫前缀，`limit=0` 按 1；
    校验 key 的 ts 与记录 `commit_ts` 一致。
  - `next_sequencer_decision_ts` = 上述 `limit=1` 的首条。
- [x] **Step 4: 跑 GREEN**

Run: `cargo test -p coord-server --lib apply_commit_decisions -- --nocapture`
Expected: 5 个测试 PASS。

### Task 3: 命令、响应与状态机分支

**Files:**
- Modify: `coord-server/src/raft/type_config.rs`
- Modify: `coord-server/src/raft/state_machine.rs`

- [x] **Step 1: 末尾追加命令/响应**
  - `Command::ApplyCommitDecisions { decisions: Vec<CommitDecisionApply> }`（只由数据 Region
    raft 提出；**末尾追加**）。
  - `Response::CommitDecisionsApplied { through_ts: u64, decisions: u64 }`。
  - 补 `Display` 分支；`CommitDecisionApply` derive
    `Clone, Debug, PartialEq, Eq, Serialize, Deserialize`。
- [x] **Step 2: state machine 分支**
  - `execute_command` 调 `sm.apply_commit_decisions(decisions, revision, applied)`；
    `through_ts < 最后一条 commit_ts` 时 `tracing::error!` 记录卡住的位置；
    返回 `(Response::CommitDecisionsApplied { .. }, None)`（不产生 Watch 事件）。
- [x] **Step 3: 跑存储/状态机测试**

Run: `cargo test -p coord-server --lib apply_commit_decisions -- --nocapture`
Expected: PASS（编译覆盖新分支）。

### Task 4: 每 Region applier 与启动接线

**Files:**
- Create: `coord-server/src/kvt/decision_apply.rs`
- Modify: `coord-server/src/kvt/mod.rs`（`pub mod decision_apply;`）
- Modify: `coord/src/main.rs`

**Interfaces:**
- Produces:
  `apply_pending_decisions(node: &CoordNode, batch: usize) -> Result<u64, String>`、
  `start_applier(node: Arc<CoordNode>, interval: Duration) -> JoinHandle<()>`、
  `DECISION_APPLY_INTERVAL: Duration`（50ms）。

- [x] **Step 1: applier**
  - `kv_t` 关闭 / 无 `region_manager` → 0（不提案）。
  - 对每个本地 leader 的 data runtime：循环 `last_applied_decision_ts` →
    `node.storage.sequencer_decisions_after(cursor, batch)`；按 Region 的
    `start_key`/`end_key` 过滤 mutations（end 为空 = 无上界）；`client_write`
    `ApplyCommitDecisions`；响应 `through_ts >= 本批最后一条` 才继续，否则返回错误。
  - 单 Region 失败不影响其他 Region；返回首条错误（调用方 warning 后下轮重试）。
- [x] **Step 2: 启动（仅 kv_t + multi-raft + region 0 raft）**
  - `main.rs` 在 `Arc::new(node)` 之后、safe_ts advancer 附近调用 `start_applier`；
    关闭态不启动。
- [x] **Step 3: 编译 + 现有多 Region 测试**

Run: `cargo test -p coord --test kv_t_g2_safe_ts_test`
Expected: PASS（关闭 applier 时原有行为不变）。

### Task 5: safe_ts 与 fence 的决策封顶

**Files:**
- Modify: `coord-server/src/kvt/sequencer.rs`（暴露 fence 锁）
- Modify: `coord-server/src/kvt/safe_ts.rs`

**Interfaces:**
- Produces: `CommitSequencer::lock_fence_publication(&self) -> MutexGuard<'_, ()>`
  （复用 `propose` 锁；持锁期间没有新的 TSO 分配/提案）。

- [x] **Step 1: fence 发布持锁**
  - `advance_once` phase 1：若 `node.commit_sequencer()` 存在，先取锁；锁内读
    TSO 高水位并 `client_write(PublishSafeTs)`。
- [x] **Step 2: 逐 Region pending 封顶**
  - phase 2 计算 `target = match node.storage.next_sequencer_decision_ts(cursor) {
    Some(next) if next <= current => 返回不变式错误,
    Some(next) => fence.min(next - 1),
    None => fence }`；`current >= target` 跳过；否则 `AdvanceSafeTs { commit_ts: target }`。
- [x] **Step 3: 跑 G2 回归**

Run: `cargo test -p coord --test kv_t_g2_safe_ts_test`
Expected: PASS（无决策时 fence 行为不变）。

### Task 6: 跨 Region 验收测试

**Files:**
- Create: `coord/tests/kv_t_g3_commit_decision_test.rs`

**Fixture:** 单节点 region 0 raft + 两个数据 Region（`["","m")` / `["m","")`），
`FakeTso` + 真实 `CommitSequencer`，`node.storage` 是 region 0 MVCC。

- [x] **Step 1: 写失败测试**

```rust
async fn test_commit_decision_applies_two_regions_atomically() {
    // CommitCandidate: Put apple（Region 1）+ Put melon（Region 2），start_ts=0
    // 决策后、apply 前：两个 Region 在 commit_ts 都读不到；decision 存在
    // apply_pending_decisions：两个 Region 都可见，safe_ts == commit_ts
    // 再次 apply_pending_decisions：version/changelog 不变
}
async fn test_safe_ts_stops_before_unapplied_decision() {
    // 决策后先 apply_tso_allocate 高水位，advance_once
    // => 两 Region current == commit_ts-1，safe_ts == commit_ts-1
    // apply 后再 advance_once => safe_ts == fence
}
```

- [x] **Step 2: 跑 RED**

Run: `cargo test -p coord --test kv_t_g3_commit_decision_test -- --nocapture`
Expected: FAIL（`apply_pending_decisions` 不存在/断言失败）。

- [x] **Step 3: 跑到 GREEN**

Run: `cargo test -p coord --test kv_t_g3_commit_decision_test -- --nocapture`
Expected: 2 个测试 PASS。

### Task 7: 文档、门禁与 PR

- [ ] **Step 1: 勾选计划与验证记录**（测试名/命令/边界）
- [ ] **Step 2: 本机跑 CI 基础门禁**

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
  - 分支 `feat/kv-t-g3-commit-decision`，正文 `Closes #148`，写两条边界
    （「Watch 仍归 #158」「直写与 decision 交错由 #149/#168 收口」）。
  - CI 全绿后 `gh pr merge --merge --delete-branch`。
- [ ] **Step 4: 关闭 #148 的证据评论**（测试名/命令/崩溃重放结论/safe_ts 封顶说明）

## 验收矩阵

| 用例 | 断言 |
|:--|:--|
| 单 Region 多 key | 一个 changelog 事件、所有 key 同一 redb 事务可见 |
| 跨 Region | apply 前 `read_ts>=commit_ts` 不可得（safe_ts 未到）；apply 后两 Region 同时可见 |
| 决策即提交点 | decision 存在但未 apply 时，`read_ts<commit_ts` 看不到 |
| 重放 | 同批重复 apply 不重复 changelog/version；游标只前进 |
| 崩溃恢复 | reopen/装快照后游标不回退；从旧游标重跑幂等 |
| fence 封顶 | pending decision D 时 safe_ts <= D-1；apply 后才到 fence |
| 漂移 | `commit_ts <= current` 且无 changelog 时停在坏决策前，不静默写 |
| 关闭态 | kv_t=false 不启动 applier；G2 与全量 v1 测试不变 |

## 验证记录

- `cargo test -p coord-server --lib apply_commit_decisions`：5 passed
  （多 key 单事件、重放幂等、空决策水位/游标丢失、reopen+快照、fence 漂移 fail-closed）。
- `cargo test -p coord-server --lib sequencer_decisions_after`：1 passed（有界、有序扫描）。
- `cargo test -p coord-server --lib kvt::decision_apply`：1 passed（Region 半开区间过滤）。
- `cargo test -p coord --test kv_t_g3_commit_decision_test`：2 passed
  （两 Region 原子可见 + 重放；未应用决策把 safe_ts 钉在 `commit_ts-1`）。
- `cargo test -p coord --test kv_t_g2_safe_ts_test`：7 passed（fence/等待回归不变）。
- 门禁与全量测试结果见 PR 正文。
