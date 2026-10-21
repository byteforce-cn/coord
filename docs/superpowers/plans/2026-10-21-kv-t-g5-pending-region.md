# KV-T G5 (#163) Split Target Pending Region Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 split 的 cutover **之前**把目标 Region 的 raft 组装配到每个副本节点（不注册路由、不服务读写），使 `RangeImportMigrator` 能经目标 Region raft 导入数据；cutover 后把预创建运行时原样提升为正式 Region（同一 `RegionHandle`、补 `CompactionManager`），intent 消失且未 cutover 时停机回收。

**Architecture:** `RegionLifecycle::reconcile` 增加一条「pending 目标」支线：从 region 0 的 intent journal（`MvccStorage::pd_intents`）读出 `Prepared`/`Applied` 的 split 计划，对本地副本的 `RegionChange::Add` 目标用 `spawn_region_runtime` 装配**未路由**运行时，存进 `RegionLifecycle::pending`（不进 `RegionManager::runtimes`，避免 pending Region 参与 safe_ts / watermark / decision-apply 聚合）。cutover 生效后，先把 pending 运行时装入 `RegionManager::runtimes`（仍无路由），由 `apply_region_table` 复用它的 `RegionHandle` 注册路由，再由既有 `ensure_region` 补 compaction 并接管。

**Tech Stack:** Rust / openraft / redb / tokio。

**Spec:** issue #163（剩余项 1）、ADR-0015 D6；上一刀 `docs/superpowers/plans/2026-10-21-kv-t-g5-split-migrator.md` 的裁定 6（目标 Region 必须已装配）；#159/#160 的权威表与生命周期。

## Global Constraints

- `[kv_t].enabled=false`（默认）不启动 reconcile loop，也不存在 intent：v1 行为不变（ADR-0001）。
- pending Region **不得注册路由**：cutover 前它的 key 区间仍归父 Region，任何路由/读写命中都必须是父 Region。
- pending Region **不得进入 `RegionManager::runtimes`**（提升前）：`safe_ts`、CommitWatermark、decision apply 都按 `list_runtimes` 聚合，未 cutover 的空 Region 会拖住水位或提前应用决策。
- 目标 raft 组只在 region 0 leader 节点 `initialize=true`（与数据 Region / Commit Log shard 同约定）；其余副本空成员集启动。
- pending 运行时提升后必须与路由表共享**同一个** `RegionHandle`（角色/成员更新不能分叉）。
- 数据目录保留：失败回滚只停机不删目录（回收仍归 #163 后续项）。
- 注释只写当前「为什么」；不写日期/变更史；不新增 `TODO`/`FIXME`。

## Rulings

1. **pending 存 `RegionLifecycle::pending`，不进 `manager.runtimes`**：pending 是生命周期内部状态；数据面可见性是 cutover 驱动的。代价：生产搬迁器 resolver 必须经 `RegionLifecycle::migration_target` 取目标（接线随生产装配切片）。
2. **提升时先 attach 再 `apply_region_table`**：让路由表复用 pending 的 `RegionHandle`；`apply_region_table` 对「表里新增但已有 runtime」的 Region 复用运行时 handle，而不是新建第二个。
3. **pending 只由 split 的 `RegionChange::Add` 产生**：merge 没有新 Region，`target_region=None`；`Rejected` intent 不产生 pending。
4. **回滚 = 停机不删目录**：intent 被清且表里没有该 Region → `raft.shutdown()` 后从 pending 摘除；目录留待回收切片，重复失败最多留孤儿目录，不会让新 Region 读到旧数据（页级幂等守卫按 `mod_revision`）。
5. **pending 不启动 `CompactionManager`**：没有读路径，压缩只会在提升后按 B-RG-1 启动。

## File Structure

- Modify: `coord-server/src/raft/region.rs` — `attach_runtime`（装入未路由运行时）、`apply_region_table` 复用已有运行时 handle。
- Modify: `coord-server/src/raft/region_lifecycle.rs` — `pending` 集合、`ensure_pending`/`retire_pending`/`desired_pending`、reconcile 四段顺序、`ReconcileOutcome` 计数、`migration_target` 访问器。
- Modify: `coord-server/tests/kv_t_g5_region_lifecycle_test.rs` — 预创建 → 导入 → cutover 提升 → 回滚回收。
- Modify: `docs/production/ops/boundaries.md` — B-RG-1 现状行。

## Review Focus

- cutover 前目标 key 是否仍 100% 路由到父 Region？pending 是否绝不出现在 `list_runtimes`？
- cutover 后路由 handle 与运行时 handle 是否同一个 `Arc`？角色/成员更新是否只有一个来源？
- 同一 intent 的重复 reconcile 是否幂等（不重复 spawn、不重复提升、不重复停机）？
- intent 被清（失败回滚）后 pending raft 是否真的停机，且不误停已提升的 Region？
- 非副本节点是否完全不装配 pending？region 0 leader 是否仍是唯一初始化者？

---

### Task 1: pending 运行时装配与访问器

**Files:**
- Modify: `coord-server/src/raft/region_lifecycle.rs`
- Test: `coord-server/tests/kv_t_g5_region_lifecycle_test.rs`

**Interfaces:**
- `RegionLifecycle::pending_count(&self) -> usize`
- `RegionLifecycle::pending_runtime(&self, region_id: RegionId) -> Option<Arc<RegionRuntime>>`
- `RegionLifecycle::migration_target(&self, region_id: RegionId) -> Option<Arc<RegionRuntime>>`（members ∪ pending）
- `ReconcileOutcome { pending_spawned, pending_promoted, pending_retired: u64 }`

- [ ] **Step 1: 写失败测试**

在 `kv_t_g5_region_lifecycle_test.rs` 增加 `propose_pd` / `record_split_intent` 辅助与首条断言：表 v1（region 1 全区间）后提出 `PdOp::RecordIntent`（`plan_split` 1 → 2 at `m`），`reconcile()` 后：

```rust
assert_eq!(outcome.pending_spawned, 1);
assert_eq!(harness.lifecycle.pending_count(), 1);
assert!(harness.lifecycle.migration_target(2).is_some());
assert!(harness.manager.runtime(2).is_none(), "pending must not join data-plane runtimes");
assert_eq!(harness.manager.route(b"z").unwrap().region_id(), 1, "no early routing");
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p coord-server --test kv_t_g5_region_lifecycle_test precreates_split_target`
Expected: FAIL（`pending_count` / `pending_spawned` 不存在）

- [ ] **Step 3: 实现 pending 装配**

`desired_pending(table)`：读 `self.table.pd_intents()`，跳过 `IntentPhase::Rejected`，取 `RegionChange::Add` 且 `table.get(id).is_none()` 且 `is_local_replica` 的目标。`ensure_pending(meta)`：transition 锁内双检（pending / manager.runtime），`Arc::new(RegionHandle::new(meta))` + `spawn_region_runtime(..., initialize = region0 leader)`（initialize 失败降级重试 false），插入 `pending`。pending 不启动 compaction。

- [ ] **Step 4: 运行确认通过**

Run: `cargo test -p coord-server --test kv_t_g5_region_lifecycle_test precreates_split_target`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add coord-server/src/raft/region_lifecycle.rs coord-server/tests/kv_t_g5_region_lifecycle_test.rs
git commit -m "feat(kv-t): pre-create split target regions as unrouted runtimes"
```

### Task 2: 目标 raft 导入与 cutover 提升

**Files:**
- Modify: `coord-server/src/raft/region.rs`
- Modify: `coord-server/src/raft/region_lifecycle.rs`
- Test: `coord-server/tests/kv_t_g5_region_lifecycle_test.rs`

**Interfaces:**
- `RegionManager::attach_runtime(&self, runtime: Arc<RegionRuntime>) -> bool`（只写 runtimes，不写路由）
- `apply_region_table` 新建 handle 前先查同 id runtime，复用其 `RegionHandle` 并 `replace_meta`
- reconcile 顺序：读表/intents → 提升（attach + 移出 pending）→ `apply_region_table` → 表内 Region `ensure_region` → 未 cutover 目标 `ensure_pending` → 表外运行时 `retire_region`

- [ ] **Step 1: 写失败测试**

同一测试继续：等待 pending 组选主后 `CoordRangeImportTarget::from_runtime` 导入一行（`watermark=10`），断言 pending 存储可读；提出 `PdOp::Cutover` 后 `reconcile()`：

```rust
assert_eq!(outcome.pending_promoted, 1);
assert_eq!(harness.lifecycle.pending_count(), 0);
assert!(harness.lifecycle.has_compaction(2));
assert_eq!(harness.manager.route(b"z").unwrap().region_id(), 2);
let runtime2 = harness.wait_region_leader(2).await;
assert_eq!(runtime2.mvcc.get(b"m").unwrap().as_deref(), Some(&b"v"[..]));
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p coord-server --test kv_t_g5_region_lifecycle_test precreates_split_target`
Expected: FAIL（prepared 目标在表里出现后仍留在 pending / 路由未复用同一 handle）

- [ ] **Step 3: 实现提升与 handle 复用**

`attach_runtime` 幂等插入 runtimes；提升循环只处理「表里已有该 Region 且本地副本」的 pending：attach → 移出 pending；非本地副本或 intent 已消失且表中没有 → `retire_pending`。`apply_region_table` 的 `None` 分支查 `self.runtimes` 复用 handle。提升后由既有 `ensure_region` 补 compaction/members。

- [ ] **Step 4: 运行确认通过**

Run: `cargo test -p coord-server --test kv_t_g5_region_lifecycle_test`
Expected: PASS（新旧用例全绿）

- [ ] **Step 5: 提交**

```bash
git add coord-server/src/raft/region.rs coord-server/src/raft/region_lifecycle.rs coord-server/tests/kv_t_g5_region_lifecycle_test.rs
git commit -m "feat(kv-t): promote pre-created split targets on cutover"
```

### Task 3: 未 cutover 的回滚回收

**Files:**
- Modify: `coord-server/src/raft/region_lifecycle.rs`
- Test: `coord-server/tests/kv_t_g5_region_lifecycle_test.rs`

- [ ] **Step 1: 写失败测试**

新用例：表 v1 → RecordIntent（region 1 → 3 at `m`）→ reconcile（pending=1）→ `PdOp::ClearIntent` → reconcile：

```rust
assert_eq!(outcome.pending_retired, 1);
assert_eq!(harness.lifecycle.pending_count(), 0);
assert!(harness.manager.runtime(3).is_none());
assert_eq!(harness.manager.route(b"z").unwrap().region_id(), 1);
assert_eq!(harness.mvcc0.region_table().unwrap().version, 1, "table untouched");
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p coord-server --test kv_t_g5_region_lifecycle_test retires_pending`
Expected: FAIL（`pending_retired` 不存在 / 运行时未停机）

- [ ] **Step 3: 实现回收**

desired 集中不含、且表里没有 → transition 锁内 `raft.shutdown()` 成功后从 pending 摘除；shutdown 失败保留待下轮重试。目录不清。

- [ ] **Step 4: 运行确认通过**

Run: `cargo test -p coord-server --test kv_t_g5_region_lifecycle_test retires_pending`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add coord-server/src/raft/region_lifecycle.rs coord-server/tests/kv_t_g5_region_lifecycle_test.rs
git commit -m "feat(kv-t): retire split targets that never cut over"
```

### Task 4: 文档与门禁

**Files:**
- Modify: `docs/production/ops/boundaries.md`（B-RG-1 现状行）

- [ ] **Step 1: 更新 B-RG-1**

补：split 目标在 cutover 前由 `RegionLifecycle` 按 intent journal 在各副本预创建（未路由、不进 `list_runtimes`、无 compaction），cutover 后复用同一 handle 提升并补 compaction；intent 清除且未 cutover → 停机保留目录。**生产装配（`EmbeddedPd` 注入搬迁器 + drain 句柄）与目录回收仍未交付（#163）**。

- [ ] **Step 2: 全门禁**

```bash
bash scripts/check-doc-refs.sh
cargo test -p coord-server
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-panics.sh
cargo audit
cargo deny check
```

- [ ] **Step 3: 提交、开 PR、issue 进度评论**

```bash
git add docs/production/ops/boundaries.md
git commit -m "docs(kv-t): record split target pre-creation (#163)"
git push -u origin feat/kv-t-g5-pending-region
gh pr create --base main --fill
gh issue comment 163 --body "目标 pending Region 预创建已交付；剩余：生产装配、目录回收。"
```
