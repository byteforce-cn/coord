# KV-T G5 (#163) Aborted Split Target Reclamation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** split 未 cutover 就被放弃时，除了停机预创建的目标 Region，还要回收它的数据目录（已导入的页/raft 日志），并在删除失败时下轮重试；已提升的 Region 目录绝不能被误删。

**Architecture:** `RegionLifecycle` 增加待回收集合 `reclaim: RwLock<BTreeSet<RegionId>>`。`retire_pending` 停机并移出 `pending` 后把 id 放进集合；每次 `reconcile` 末尾按**当前**权威表再确认「不在表里」后删除 `region_data_dir(base_dir, id)`。删除成功出集合，失败保留待下轮；进程重启后集合为空，遗留目录留给后续 GC 切片。

**Tech Stack:** Rust / redb / openraft / tokio。

**Spec:** issue #163 剩余项「目录回收与失败回滚（导入已复制的数据）」；ADR-0015 D6/D11.2；上一刀 `docs/superpowers/plans/2026-10-21-kv-t-g5-pending-region.md`。

## Global Constraints

- 只删除**从未进入权威表**的目标目录；删除前按当前表再确认一次，表里有的 id 只出集合、不删目录。
- 删除失败不得让 reconcile 失败或丢掉待回收 id（下一 tick 重试），也不得影响其它 Region。
- `[kv_t].enabled=false` 不启动 reconcile loop，无 pending/reclaim 状态，v1 行为不变。
- 提升后的 Region 目录、retire 的表内 Region 目录（搬迁/救援用）都不属于本切片。

## Rulings

1. **删除放在 reconcile 的末尾、按表复核**：`retire_pending` 只登记；真正删除前再看一次表，防止「刚放弃的 id 又被提升」的竞态。
2. **进程重启的孤儿目录不在本切片**：intent 已清的 pending 目录没有持久化记录，重启后无法与其它历史目录区分；留给后续目录 GC（需要 region 0 的回收标记）。
3. **删除 NotFound 视为成功**：重复 tick / 人工清理后不应永远告警。

## File Structure

- Modify: `coord-server/src/raft/region_lifecycle.rs` — `reclaim` 集合、登记与 sweep。
- Modify: `coord-server/tests/kv_t_g5_region_lifecycle_test.rs` — 回滚删目录、提升不删目录。
- Modify: `docs/production/ops/boundaries.md` — B-RG-1 现状行。

## Review Focus

- 未 cutover 的目标目录是否真的被删，且删除发生在 raft 停机之后？
- 表里存在的 Region 目录是否绝不进删除路径？
- 删除失败是否保留重试而不是静默丢 id？
- 提升/正常 retire 的目录是否保持不变？

---

### Task 1: 回滚删除目标目录

- [ ] **Step 1: 写失败测试**

在 `test_lifecycle_retires_split_target_that_never_cut_over` 中：

```rust
let target_dir = region_data_dir(harness._dir.path(), 3);
assert!(target_dir.exists(), "pending target wrote its data dir");
// ... ClearIntent + reconcile ...
assert!(!target_dir.exists(), "aborted split target dir is reclaimed");
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p coord-server --test kv_t_g5_region_lifecycle_test retires_split_target`
Expected: FAIL（目录仍存在）

- [ ] **Step 3: 实现登记 + 按表复核删除**

`retire_pending` 成功后 `reclaim.insert(region_id)`；reconcile 末尾 `reclaim_aborted_dirs(&table)`：表里有该 id → 出集合不删；否则 `std::fs::remove_dir_all(region_data_dir(...))`，`NotFound` 或成功 → 出集合，其它错误 warn 并保留。

- [ ] **Step 4: 运行确认通过**

Run: `cargo test -p coord-server --test kv_t_g5_region_lifecycle_test`
Expected: PASS

### Task 2: 提升不删目录 + 文档门禁

- [ ] **Step 5: 断言提升目录保留**

在提升用例清 intent 后：`assert!(region_data_dir(harness._dir.path(), 2).exists())`。

- [ ] **Step 6: 更新 B-RG-1**

补：未 cutover 的目标停机后删除数据目录（失败下轮重试；重启遗留孤儿目录与生产装配仍未交付，#163）。

- [ ] **Step 7: 全门禁 + 提交 + PR**

```bash
cargo test -p coord-server
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-panics.sh
cargo audit && cargo deny check
bash scripts/check-doc-refs.sh   # 本地 ignored 会话产物先移到 /tmp
```
