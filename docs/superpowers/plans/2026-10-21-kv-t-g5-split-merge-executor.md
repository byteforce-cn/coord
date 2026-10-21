# KV-T G5 (#162) Split/Merge Executor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 PD 的 `SplitRegion` / `MergeRegion` operator 从 unsupported 改为真实的、可重启恢复的执行：intent/journal 先落 region 0，数据搬迁（可注入钩子，缺省 fail-closed）完成后经一次 CAS 原子改写权威 Region 表，再单向对账 `PdMetaStore`。

**Architecture:** intent 与 cutover 都走 region 0 raft（`PdOp`，复用既有 leader 转发）；cutover 在 `apply_pd_op` 的同一个 redb 写事务里读 intent、校验 `expected_table_version`、原子应用 `RegionChange` 批并回写 intent 阶段——表与日志不可能分叉。执行器（数据 Region leader 节点）负责计划、迁移钩子、轮询本地 region 0 视图、对账与清理。

**Tech Stack:** Rust / openraft / redb / postcard / tokio。

**Spec:** `docs/adr/0015-kv-t.md` D6（任意 key 在线 Split/Merge）、D11.2/8/9；issue #162（`gh issue view 162`）；边界 `docs/production/ops/boundaries.md` B-RG-1 / B-KVT-3。

## Global Constraints

- `[kv_t].enabled=false`（默认）时不得改变 v1 行为（ADR-0001）：表未初始化（`version == 0`）时 split/merge 一律 fail-closed，`PdMetaStore` 对账 no-op。
- 表级非法变更降级为 `Rejected`，**不得**让 raft apply 返回 `Err`（D11.5）。
- 用户级冲突/拒绝不静默成功：执行失败必须 `Complete{success=false, error}`，且不得重复副作用。
- 注释只写当前「为什么」；不写日期、变更史；不新增 `TODO`/`FIXME`。
- 临时文件只放 `/tmp/coord-*`；提交前跑 AGENTS.md 门禁（fmt / clippy / check-panics / audit / deny / check-doc-refs）。

## Rulings（本计划对 spec 的落地裁定）

1. **intent 落 region 0 而不是数据 Region**：cutover 与 intent 阶段必须在同一写事务里（否则表变了、日志没变），region 0 是表所在组；`PdOp` 已有 leader 转发，不需要新 wire 形态。
2. **cutover 用 `expected_table_version` 做 CAS**：计划按本地表视图计算，视图过期就整批拒绝，不做「尽量应用」。
3. **数据搬迁是注入钩子 `SplitMergeMigrator`，缺省失败**：`#163`/`#164` 才拥有目录/MVCC/lease 搬迁细节；本切片绝不在未搬迁数据时切表（否则 split 后 key 不可见）。
4. **只有队列认领路径能执行 split/merge**（需要 `op_id` 做 journal key）；`OperatorExecutor::execute()` 直接调用返回明确错误。
5. **`/obj/` 保守守卫**：split_key 落在 `[b"/obj/", b"/obj0")` 拒绝；merge 的并集区间触及对象空间一律拒绝（`#165` 再收口固定区间）。
6. **终态垃圾回收**：op 已 Success/Failed 但 intent 仍在 → `Applied` 的补对账后清理，`Prepared` 的标记 `Rejected` 后清理（迁移已复制的数据由 #163 的目录回收负责）。

## File Structure

- Modify: `coord-server/src/storage/region_table.rs` — 纯计算：`SplitMergeKind` / `SplitMergePlan` / `plan_split` / `plan_merge`（与表不变量同处一地）。
- Modify: `coord-server/src/raft/type_config.rs` — `SplitMergePlan` 的 journal 信封 `PdIntent` / `IntentPhase`；`PdOp::{RecordIntent,Cutover,ClearIntent}`。
- Modify: `coord-server/src/storage/mvcc.rs` — `PD_INTENT_PREFIX`、apply（intent 写入 / 原子 cutover / 清理）与读取；同事务复用 `RegionTable`。
- Modify: `coord-server/src/raft/system_raft.rs` — `SystemRaftHandle::{region_table,pd_intents,pd_intent}` 及实现。
- Modify: `coord-server/src/pd/executor.rs` — 迁移钩子、split/merge 执行、恢复/GC、`PdMetaStore` 单向对账。
- Test: `coord-server/tests/kv_t_g5_split_merge_test.rs` — 真实 region 0 raft 的端到端验收。
- Modify: `docs/production/ops/boundaries.md` — B-RG-1 / B-KVT-3 现状。

## 非本切片（#162 保持 open）

- `[multi_raft.pd] max_concurrent_operators` 与「调度暂停」：调度器已按 Pending 上限节流，执行器每拍最多一个 operator；本切片不改调度阈值/暂停语义。
- 数据搬迁实现（MVCC 行/lease/对象 manifest、drain）：`#163`/`#164`/`#165`；本切片只定义并调用钩子。
- 多节点 leader 分布与 Region 差异化副本：仍按 v1「每 Region 复制到全部成员」。

## 评审后追加（执行中）

- 对账只同步表拥有的字段（路由 + split version），保留 PD-only 的 `peers`/`conf_ver` 与心跳统计；否则 AddPeer 每拍被回滚。
- merge 的合并 version 取 `max(left, right) + 1`，避免吸收更高 version 时回退。
- 补 cutover 超时与 intent 跨重启持久化用例。

## Review Focus

- split_key 等于 Region 边界 / 落在 `/obj/` / 新 region id 已存在时，必须拒绝且表版本不变。
- 表版本在计划后前进（并发 split/merge、其它元数据变更）时，cutover 必须整批拒绝，不能部分生效。
- 迁移钩子缺失或失败时，**不得**写 intent 之外的任何状态、不得切表。
- 进程在 Cutover 已 apply、Complete 未提出之间崩溃：重启后必须补对账并清 intent，不能重复迁移副作用。
- merge 后旧 Region 的运行时退役由 lifecycle 完成（表删除 → `retire_region`），执行器不得自行停机。

---

### Task 1: `SplitMergePlan` 与 split/merge 计划校验

**Files:**
- Modify: `coord-server/src/storage/region_table.rs`
- Test: `coord-server/src/storage/region_table.rs`（`#[cfg(test)] mod tests` 追加）

**Interfaces:**
- Consumes: `RegionTable::{version,get,route}`, `RegionChange::{Add,Update,Remove}`, `coord_core::types::{RegionId,RegionMeta,RegionEpoch}`。
- Produces:
  - `pub enum SplitMergeKind { Split, Merge }`
  - `pub struct SplitMergePlan { pub kind: SplitMergeKind, pub source_region: RegionId, pub target_region: Option<RegionId>, pub changes: Vec<RegionChange>, pub expected_table_version: u64 }`
  - `pub fn plan_split(table: &RegionTable, region_id: RegionId, split_key: &[u8], new_region_id: RegionId) -> Result<SplitMergePlan>`
  - `pub fn plan_merge(table: &RegionTable, left: RegionId, right: RegionId) -> Result<SplitMergePlan>`
  - 用 `coord_core::error::Result`，错误消息以 `Error::InvalidArgument` 携带可读原因。

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn test_plan_split_computes_contiguous_child_range() {
    let table = table_with(vec![meta(1, b"", b"")]);
    let plan = plan_split(&table, 1, b"m", 2).expect("split plan");
    assert_eq!(plan.kind, SplitMergeKind::Split);
    assert_eq!(plan.source_region, 1);
    assert_eq!(plan.target_region, Some(2));
    assert_eq!(plan.expected_table_version, table.version);
    let mut next = table.clone();
    next.apply(&plan.changes).expect("plan applies");
    let parent = next.get(1).unwrap();
    let child = next.get(2).unwrap();
    assert_eq!(parent.end_key, b"m");
    assert_eq!(child.start_key, b"m");
    assert_eq!(child.end_key, b"");
    assert_eq!(parent.epoch.version, table.get(1).unwrap().epoch.version + 1);
    assert_eq!(child.epoch.version, parent.epoch.version);
    assert_eq!(child.peers, parent.peers);
}

#[test]
fn test_plan_split_rejects_boundaries_and_object_keys() {
    let table = table_with(vec![meta(1, b"a", b"z")]);
    for key in [b"a".as_slice(), b"z".as_slice()] {
        assert!(plan_split(&table, 1, key, 2).is_err(), "boundary {key:?}");
    }
    for key in [b"/obj/".as_slice(), b"/obj/m/x".as_slice()] {
        assert!(plan_split(&table, 1, key, 2).is_err(), "object {key:?}");
    }
    assert!(plan_split(&table, 1, b"m", 1).is_err(), "id reuse");
    assert!(plan_split(&RegionTable::default(), 1, b"m", 2).is_err(), "uninitialized");
}

#[test]
fn test_plan_merge_requires_adjacency_and_no_object_space() {
    let table = table_with(vec![meta(1, b"", b"m"), meta(2, b"m", b"")]);
    let plan = plan_merge(&table, 1, 2).expect("merge plan");
    let mut next = table.clone();
    next.apply(&plan.changes).expect("plan applies");
    assert_eq!(next.get(1).unwrap().end_key, b"");
    assert!(next.get(2).is_none());
    assert_eq!(plan.target_region, None);
    assert!(plan_merge(&table, 2, 1).is_err(), "order/adjacency");

    let obj = table_with(vec![
        meta(1, b"", b"/obj/"),
        meta(2, b"/obj/", b"/obj0"),
        meta(3, b"/obj0", b""),
    ]);
    assert!(plan_merge(&obj, 1, 2).is_err(), "object region merged away");
    assert!(plan_merge(&obj, 2, 3).is_err(), "object region merged away");
    assert!(plan_merge(&obj, 3, 1).is_err(), "non-adjacent");
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server storage::region_table::tests::test_plan_ -- --nocapture`
Expected: FAIL（`plan_split` / `plan_merge` / `SplitMergeKind` 未定义；`table_with` 若缺则一并补为测试辅助）

- [ ] **Step 3: 实现计划函数**

规则（严格按此，勿放宽）：
- 两函数都先要求 `table.version > 0`，否则 `InvalidArgument("region table is not initialized")`。
- split：`split_key` 必须严格落在源 Region 的半开区间内（`end_key` 空 = 无上界）；`new_region_id != 0`、不等于源、且表内不存在；`key_in_object_space(split_key)` 为真时拒绝。
- split 的 `changes`：`Update{ parent.end_key = split_key, epoch.version += 1 }`；`Add{ child: start = split_key, end = parent.end_key, epoch = { conf_ver: parent.conf_ver, version: parent.epoch.version + 1 }, peers = parent.peers.clone(), approximate_* = 0 }`。
- merge：两 Region 都存在且 `left.end_key == right.start_key`（非空）；`range_touches_object_space(left.start_key, right.end_key)` 为真时拒绝。
- merge 的 `changes`：`Update{ left.end_key = right.end_key, epoch.version += 1 }` + `Remove{ region_id: right, expected_epoch: right.epoch }`。
- `expected_table_version = table.version`。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server storage::region_table::tests`
Expected: PASS（含既有 epoch/区间用例）

- [ ] **Step 5: 提交**

```bash
git add coord-server/src/storage/region_table.rs
git commit -m "feat(kv-t): plan split and merge as atomic region-table batches"
```

---

### Task 2: intent journal 类型与 `PdOp` 变体

**Files:**
- Modify: `coord-server/src/raft/type_config.rs`

**Interfaces:**
- Consumes: `storage::region_table::SplitMergePlan`、`coord_core::types::NodeID`。
- Produces:
  - `pub enum IntentPhase { Prepared, Applied { table_version: u64 }, Rejected { reason: String } }`
  - `pub struct PdIntent { pub op_id: u64, pub owner: NodeID, pub plan: SplitMergePlan, pub phase: IntentPhase }`，带 `to_bytes`/`from_bytes`（postcard，同 `PdQueueEntry` 风格）。
  - `PdOp` 末尾追加：`RecordIntent { intent: PdIntent }`、`Cutover { op_id: u64 }`、`ClearIntent { op_id: u64 }`。

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn test_pd_intent_roundtrip_and_phase() {
    let intent = PdIntent {
        op_id: 7,
        owner: 1,
        plan: /* 用 Task 1 的 plan_split 构造 */,
        phase: IntentPhase::Prepared,
    };
    let bytes = intent.to_bytes().unwrap();
    assert_eq!(PdIntent::from_bytes(&bytes).unwrap(), intent);
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server raft::type_config::tests::test_pd_intent`
Expected: FAIL（类型未定义）

- [ ] **Step 3: 定义类型与变体**

`PdOp` 只追加到末尾（变体索引兼容），注释写清：`RecordIntent` 首次写入生效、已存在则保留首个（同一 `op_id` 的计划只算一次）；`Cutover` 只对 `Prepared` 生效；`ClearIntent` 幂等删除。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server raft::type_config::tests`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add coord-server/src/raft/type_config.rs
git commit -m "feat(kv-t): add pd intent journal types for split/merge"
```

---

### Task 3: region 0 apply —— intent 写入、原子 cutover、读取与清理

**Files:**
- Modify: `coord-server/src/storage/mvcc.rs`
- Test: `coord-server/src/storage/mvcc.rs`（`#[cfg(test)] mod tests` 追加）

**Interfaces:**
- Consumes: Task 1 的 `SplitMergePlan`、Task 2 的 `PdIntent`/`IntentPhase`、既有 `PD_QUEUE_PREFIX`/`encode_pd_op_key` 与 `REGION_TABLE_KEY`。
- Produces:
  - `pub const PD_INTENT_PREFIX: &[u8] = b"/_pd/intents/";`
  - `fn encode_pd_intent_key(op_id: u64) -> Vec<u8>`（`pub(crate)`）
  - `pub fn pd_intents(&self) -> Result<Vec<PdIntent>>`
  - `pub fn pd_intent(&self, op_id: u64) -> Result<Option<PdIntent>>`

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn test_pd_cutover_applies_table_and_marks_intent_once() {
    // 表 version=1，单 Region 1 ["","")；plan = split(1, "m", 2)
    // apply RecordIntent @ rev1 → pd_intent(7) = Prepared
    // apply Cutover{op_id:7} @ rev2 → table.version=2、region 2 存在、intent=Applied{2}
    // 再 apply Cutover{op_id:7} @ rev3 → 表版本/内容不变（幂等），intent 仍 Applied{2}
}

#[test]
fn test_pd_cutover_rejects_stale_table_version() {
    // Seed intent 计划 expected_version=1；先把表推进到 2（另一批合法变更）
    // Cutover → intent.phase = Rejected{..}，表版本仍 2，内容不变
}

#[test]
fn test_pd_clear_intent_is_idempotent() {
    // Record → Clear → pd_intent = None；再 Clear 不报错
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server storage::mvcc::tests::test_pd_cutover -- --nocapture`
Expected: FAIL（`pd_intents` 等未定义 / `PdOp::Cutover` 未处理）

- [ ] **Step 3: 在 `apply_pd_op` 内实现三个分支**

- `RecordIntent`：`tx.get(intent_key)` 已存在 → 不覆盖；否则写入。不写 changelog 事件（沿用该函数末尾的空事件）。
- `Cutover`：读 intent；`None` / 非 `Prepared` → no-op；`Prepared` → 读表，`table.version != expected_table_version` → `Rejected{reason}`；否则 `table.apply(&plan.changes)`：`Ok` 写回表 + `Applied{table_version}`，`Err` → `Rejected`。**阶段写回与表写入必须在同一 `tx`**。
- `ClearIntent`：`tx.remove`，缺失不报错。
- 读取：`pd_intents` 按 `iter_prefix(TABLE_KV, PD_INTENT_PREFIX)` 解码，坏记录跳过并 `tracing::warn!`（内部记录损坏不应拖垮读路径）；`pd_intent` 单键读取。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server storage::mvcc::tests`
Expected: PASS（含既有 PD 队列/表用例）

- [ ] **Step 5: 提交**

```bash
git add coord-server/src/storage/mvcc.rs
git commit -m "feat(kv-t): journal split/merge intents and cut over the table atomically"
```

---

### Task 4: `SystemRaftHandle` 暴露表与 intent 视图

**Files:**
- Modify: `coord-server/src/raft/system_raft.rs`
- Modify: `coord-server/src/pd/mod.rs`（测试替身 `FakeSystemRaft`）
- Modify: `coord-server/src/pd/executor.rs`（测试替身，如有）

**Interfaces:**
- Consumes: `MvccStorage::{region_table,pd_intents,pd_intent}`。
- Produces:
  - `fn region_table(&self) -> Result<RegionTable>`
  - `fn pd_intents(&self) -> Result<Vec<PdIntent>>`
  - `fn pd_intent(&self, op_id: u64) -> Result<Option<PdIntent>>`
  - 实现于 `CoordSystemRaftHandle`（`mvcc` 读取，错误映射 `Error::Internal`）与测试替身（默认空表/空 intent；需要驱动 split 的用例在 executor 测试内自带可编程替身）。

- [ ] **Step 1: 扩展 trait 与实现，补测试替身**

`FakeSystemRaft`（`coord-server/src/pd/mod.rs` 的 `mod tests`）增加字段 `table: Mutex<RegionTable>`、`intents: Mutex<Vec<PdIntent>>`，`propose_pd` 对三个新变体做与 `apply_pd_op` 同构的内存模拟（Record 首次写入、Cutover CAS + 写表、Clear 删除）。把该替身、`proposed_ops`、`queue_entry`、`seed_pending`、`seed_running` 提升为 `pub(crate)`，供 `pd::executor::tests` 复用（同一 crate 的 `cfg(test)` 编译单元）。

- [ ] **Step 2: 编译 + 既有测试**

Run: `cargo test -p coord-server pd:: --no-run && cargo test -p coord-server pd::tests`
Expected: PASS（无新行为断言，只有编译与既有用例回归）

- [ ] **Step 3: 提交**

```bash
git add coord-server/src/raft/system_raft.rs coord-server/src/pd/mod.rs coord-server/src/pd/executor.rs
git commit -m "feat(kv-t): expose region table and pd intents on the system raft handle"
```

---

### Task 5: 执行器 split/merge 路径（钩子 + cutover + 对账）

**Files:**
- Modify: `coord-server/src/pd/executor.rs`
- Test: `coord-server/src/pd/executor.rs`（模块内单测）

**Interfaces:**
- Consumes: Task 1 `plan_split`/`plan_merge`、Task 2 `PdOp::{RecordIntent,Cutover,ClearIntent}`/`PdIntent`/`IntentPhase`、Task 4 `SystemRaftHandle::{region_table,pd_intents,pd_intent,propose_pd}`、`PdMetaStore::{create_region,update_region,delete_region,list_regions,get_region}`。
- Produces:
  - `#[async_trait::async_trait] pub trait SplitMergeMigrator: Send + Sync { async fn migrate(&self, plan: &SplitMergePlan) -> std::result::Result<(), String>; }`
  - `pub fn with_migrator(mut self, migrator: Arc<dyn SplitMergeMigrator>) -> Self`
  - `pub fn with_cutover_timeout(mut self, timeout: Duration) -> Self`（默认 5s，测试注入短值）
  - `async fn execute_split_merge(&self, op_id: u64, op: &Operator) -> std::result::Result<(), String>`
  - `fn reconcile_meta_store(&self, table: &RegionTable) -> std::result::Result<(), String>`

- [ ] **Step 1: 写失败测试（模块内，用 Task 4 的可编程 `FakeSystemRaft` + `FakeRaft` leader）**

```rust
#[test] // tokio::test
async fn test_split_operator_plans_intent_migrates_and_cuts_over() { /* 见断言清单 */ }
#[tokio::test]
async fn test_split_without_migrator_fails_closed_before_side_effects() {}
#[tokio::test]
async fn test_merge_operator_cuts_over_and_reconciles_meta_store() {}
#[tokio::test]
async fn test_split_cutover_rejected_when_table_version_moves() {}
```

断言清单：
- split 成功：`migrator.calls == 1`（plan.expected_table_version = 旧版本、changes 两条）、队列表 `Success`、`pd_intents` 为空、`meta_store` 中 region 1 end_key 收缩 + region 2 存在、替身表 version +1。
- 无 migrator：`Err` 包含 `data migration not wired`，`pd_intents` 为空、替身表版本不变、`migrator` 无调用。
- merge 成功：region 2 从表与 `meta_store` 删除，region 1 end_key 为空。
- 表版本前进：`Err` 包含 `stale`/`rejected`，表内容不变、intent 已清理、`meta_store` 与表一致。

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server pd::executor::tests::test_split_operator -- --nocapture`
Expected: FAIL（split 分支仍返回 unsupported）

- [ ] **Step 3: 实现执行路径**

`execute_one_raft` 认领成功后：`Operator::SplitRegion{..} | Operator::MergeRegion{..}` → `execute_split_merge(op_id, &op)`，其余走既有 `execute`。`execute()` 对这两种 operator 返回 `Err("split/merge requires a claimed queue operator (journal op id)")`。

`execute_split_merge` 顺序（失败即 `Err`，由调用方 `Complete{success=false}`）：
1. `self.pd.system_raft()` 缺失 → `Err`。
2. 读 `table`；`pd_intent(op_id)`。
3. 无 intent：`self.migrator` 缺失 → `Err("split/merge data migration not wired (#163)")`（**在任何 propose 之前**）；`plan_split`/`plan_merge`；`propose_pd(RecordIntent{owner: self.node_id, phase: Prepared})`。
4. intent 阶段：`Applied` → 对账 + `ClearIntent` + `Ok`；`Rejected{reason}` → `ClearIntent` + `Err(reason)`；`Prepared` → `migrate(&plan)` → `propose_pd(Cutover{op_id})` → 轮询 `pd_intent` 直到非 `Prepared`（`cutover_timeout`，25ms 间隔）→ 回到 4。
5. 轮询超时 → `Err("cutover outcome not visible ...; intent retained")`（不清理 intent，交给恢复/GC）。

`reconcile_meta_store`：`table.version == 0` → no-op；表内每项与 `meta_store` 对比，缺失 `create_region`、不同 `update_region`；`meta_store` 中表里没有的 `delete_region`（`RegionNotFound` 忽略）。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server pd::executor::tests`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add coord-server/src/pd/executor.rs
git commit -m "feat(kv-t): execute split and merge through the intent journal"
```

---

### Task 6: 重启恢复、终态 GC 与逐步对账

**Files:**
- Modify: `coord-server/src/pd/executor.rs`
- Test: `coord-server/src/pd/executor.rs`（模块内单测）

**Interfaces:**
- Consumes: Task 5 的 `execute_split_merge` / `reconcile_meta_store`；`PdQueueEntry::{is_running,is_terminal,claimed_by}`。
- Produces: `async fn recover_intents(&self, system: &dyn SystemRaftHandle, queue: &[PdQueueEntry])`（在 `execute_one_raft` 认领新 operator 之前调用）。

- [ ] **Step 1: 写失败测试**

```rust
#[tokio::test]
async fn test_executor_resumes_prepared_intent_for_running_operator() {
    // 预置：表 version=1；intent(op_id=5, owner=1, Prepared)；队列条目 Running/claimed_by=1
    // 期望：execute_one 先恢复 → migrate 1 次 → cutover → Success → intent 清理
}

#[tokio::test]
async fn test_executor_finalizes_applied_intent_then_cleans_it() {
    // 预置：intent Applied（表已 split）、op Running/claimed_by=1
    // 期望：不再调用 migrate；meta_store 对账到表；intent 清理；Complete 成功
}

#[tokio::test]
async fn test_executor_gc_rejects_prepared_intent_of_terminal_operator() {
    // 预置：op Failed、intent Prepared；表未变
    // 期望：intent → Rejected → 清理；表版本不变；不调用 migrate
}

#[tokio::test]
async fn test_executor_reconciles_meta_store_every_tick() {
    // 表已 split，meta_store 仍是旧形态；无 operator
    // 期望：execute_one（或循环一拍）后 meta_store 与表一致
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server pd::executor::tests::test_executor_ -- --nocapture`
Expected: FAIL（恢复逻辑不存在）

- [ ] **Step 3: 实现恢复与 GC**

`execute_one_raft` 开头（读到 queue 后）：
1. `try_reconcile`：`system.region_table()` 成功且 `version > 0` → `reconcile_meta_store`（失败只 `warn`，不阻塞执行）。
2. `recover_intents(system, &queue)`：
   - `pd_intents()`；对每个 intent：
     - 队列中对应 `op_id` **不存在**或 `is_terminal()`：`Applied` → `reconcile_meta_store` + `ClearIntent`；`Prepared` → `RecordIntent{phase: Rejected{reason: "operator ended before cutover"}}` + `ClearIntent`（不用 Cutover：迁移可能未完成）。
     - `is_running() && claimed_by == self.node_id`：构造 `Operator` 后调 `execute_split_merge`，成功/失败都 `Complete`（与认领路径同口径，含审计事件）。
   - 恢复期间不得再认领新 operator（处理完即返回 `Some(op)`，或本拍回收过 intent 则返回 `None` 让下一拍认领——两种都可，测试以行为断言为准：**本拍至多一个迁移副作用**）。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server pd::executor::tests`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add coord-server/src/pd/executor.rs
git commit -m "feat(kv-t): resume and garbage-collect split/merge intents"
```

---

### Task 7: 端到端验收（真实 region 0 raft）、文档与门禁

**Files:**
- Create: `coord-server/tests/kv_t_g5_split_merge_test.rs`
- Modify: `docs/production/ops/boundaries.md`

**Interfaces:**
- Consumes: Task 5/6 的 `OperatorExecutor::{new,with_migrator,execute_one}`、`CoordSystemRaftHandle::new`、`common::bind_listener`。
- Produces: 验收证据（测试名）写进 issue #162 评论与 PR 描述。

- [ ] **Step 1: 写端到端测试**

复用 `kv_t_g5_region_lifecycle_test.rs` 的 boot 骨架（单节点 region 0 + 静态 region 1 + 真实表）：
- 种子表（region 1 = `["", "")`）；`CoordSystemRaftHandle::new(raft0, mvcc0)` → `pd.attach_system_raft` → `OperatorExecutor::new(pd, NODE_ID).with_migrator(fake)`。
- `propose_pd(PdOp::Enqueue{ op: SplitRegion{region_id:1, split_key:b"m", new_region_id:2}, requester:1, ... })`。
- `execute_one(&resolver)`（resolver 返回一个 `current_leader() == NODE_ID` 的 `RegionRaftHandle` 替身）。
- 断言：表 version=2、region 1 end=`m`、region 2 start=`m`、`meta_store` 与表一致、`mvcc0.pd_intents()` 为空、队列条目 `Success`、迁移钩子恰好 1 次。
- 再跑 `execute_one` → `None`，迁移钩子次数不变（幂等/无重复副作用）。

- [ ] **Step 2: 跑测试**

Run: `cargo test -p coord-server --test kv_t_g5_split_merge_test -- --nocapture`
Expected: PASS

- [ ] **Step 3: 更新边界文档**

`docs/production/ops/boundaries.md`：
- B-RG-1 现状行补：Split/Merge executor 已按 intent/journal 真实执行表 cutover；数据搬迁未接线时 fail-closed（`#163`/`#164`）；`PdMetaStore` 每拍从权威表单向对账。
- B-KVT-3 现状行补：split_key 落在 `/obj/` 或 merge 并集触及 `/obj/` 一律拒绝（`#165` 收口固定区间）。
- 顺手跑 `bash scripts/check-doc-refs.sh`（悬空 = 0）。

- [ ] **Step 4: 全量门禁**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-panics.sh
cargo audit --no-fetch
cargo deny --offline check
cargo test -p coord-server
bash scripts/check-doc-refs.sh
```

Expected: 全部 PASS（audit/deny 本机无外网时走缓存 DB，PR CI 复核）。

- [ ] **Step 5: 提交并开 PR（分支 → PR → CI 全绿 → squash merge → 删分支）**

```bash
git add -A
git commit -m "test(kv-t): prove split/merge cutover against a real region 0 raft"
git push -u origin fix/kv-t-g5-split-merge-executor
gh pr create --base main --fill
gh pr checks --watch
gh pr merge --merge --delete-branch
```
