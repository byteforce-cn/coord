# KV-T G5 (#167) Split/Merge Continuity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** 让 split/merge 的 cutover 不把已决策事务丢在旧 Region：split 目标从导入水位继续 apply 后续决策；merge 只在两侧决策游标/水位一致时搬迁；Watch 对 cutover 重放的同一 `commit_ts` 同 key 记录只产出一个事件，cursor 跨拓扑变化连续。

**Architecture:** 在 `MvccStorage::apply_range_import` 的 split 分支把目标决策游标钉到 `watermark`（导入快照已包含 <= watermark 的全部决策）；merge 搬迁器在 drain 后、导出前要求两侧 `current_revision` 与 `last_applied_decision_ts` 完全一致，避免接收方游标跳过被吸收区间尚未 apply 的决策；`WatchEvent::from_records` 按 key 去重同一 `commit_ts` 的跨 Region 分片（cutover 窗口内同一决策可能在父子两侧各写一条 CommitRecord）。不改变 wire 形状，不新增 raft 命令变体。

**Tech Stack:** Rust / openraft / redb / tokio。

**Spec:** issue #167、ADR-0015 D5/D6/D11.7/D11.8、边界 `docs/production/ops/boundaries.md` B-KVT-9/B-KVT-10；上一刀 `docs/superpowers/plans/2026-10-22-kv-t-g5-merge-migrator.md`。

## Global Constraints

- `[kv_t].enabled=false`（默认）时不得改变 v1 行为（ADR-0001）：导入/搬迁/Watch 改动只在 v2 commit_ts 模式与 `coord.watch.v2` 路径生效。
- 已 commit 的 key 不丢；同一 key 不得因 cutover 重放产生重复 Watch 事件；`mod_revision` 不倒退。
- 用户级拒绝不得让 raft apply 返回 `Err`（D11.5）；搬迁前置条件不满足时返回 `Err` 给 executor，intent 保留重试。
- 注释只写当前「为什么」；不写日期/变更史；不新增 `TODO`/`FIXME`。
- 临时文件只放 `/tmp/coord-*`；提交前跑 AGENTS.md 门禁（fmt / clippy / check-panics / audit / deny / check-doc-refs）。

## Rulings（本计划对 spec 的落地裁定）

1. **split 目标游标 = 导入水位**：导出快照的 `watermark` 是源 Region 已 apply 的 `current_revision`；导入完成后目标必须跳过 <= watermark 的决策，否则 `apply_commit_decisions` 会在第一个旧决策上遇到「`commit_ts <= current_revision` 且无 changelog」而永久停住（safe_ts 被封顶）。因此 split 导入在 `done` 页同时写 `DECISION_APPLIED_TS = watermark`。
2. **split 源必须先把决策 apply 到当前水位**：`last_applied_decision_ts == current_revision`；否则快照里的 `current_revision` 越过了尚未 apply 的决策，目标游标会跳过它们。不满足 → 整次搬迁失败（executor 重试），不做猜测。空 Region（revision 0）仍可搬迁：导入水位允许 0，目标游标保持 0，第一条 `commit_ts=1` 的决策不会被跳过。
3. **merge 两侧必须收敛到同一 revision**：merge 搬迁前要求 donor 与 receiver 的 `current_revision` 相等，且各自 `last_applied_decision_ts == current_revision`。接收方领先 donor 时，接收方游标已越过 donor 尚未 apply 的决策；直接合并会让这些决策（可能命中被吸收区间）永远被跳过。接收方落后 donor 仍沿用既有 fail-closed。
4. **Watch 同 ts 同 key 去重只跨 record**：cutover 窗口内同一决策可能先在父 Region apply、再在子 Region 重放；两条 CommitRecord 的 `region_id` 不同。`WatchEvent::from_records` 对后续 record 中已出现过的 key 只保留首个 change；同一 record 内的多次变更（例如同事务 put+delete）不去重。
5. **跨协调节点会话 drain 与迁移窗口内的直接 KV 写仍不在本切片**：本次只保证决策/Watch 连续性；并发直写冻结仍归 #167 的后续切片（`EPOCH_STALE` 只覆盖 cutover 后的旧 epoch 请求）。

## File Structure

- Modify: `coord-server/src/storage/mvcc.rs` — split 导入写决策游标；单测。
- Modify: `coord-server/src/split_merge.rs` — split 源/merge 两侧决策水位前置条件。
- Modify: `coord-server/tests/kv_t_g5_split_migrator_test.rs` — 源数据改由决策 apply 播种；断言目标游标；后续决策可 apply。
- Modify: `coord-server/tests/kv_t_g5_merge_migrator_test.rs` — donor/left 决策水位收敛；新增 donor 落后 fail-closed；调整既有用水位差的用例。
- Modify: `coord-server/src/kvt/watch.rs` — 跨 record 同 key 去重；单测。
- Create: `coord/tests/kv_t_g5_continuity_test.rs` — `test_watch_survives_split_merge`（真实 shard/Region 拓扑变化 + cutover 重放）。
- Modify: `coord/tests/support/kv_t_watch_node.rs` — 表变更 + 新 Region 运行时装配 + 直写指定 Region 的测试入口。
- Modify: `coord/tests/kv_t_g5_routing_epoch_test.rs` — `coord.storage.v2` 旧 epoch 覆盖（#161 收口证据）。
- Modify: `docs/production/ops/boundaries.md` — B-KVT-9/B-KVT-10 现状。
- Modify: `docs/adr/0015-kv-t.md` — D6 落地说明（游标/收敛/去重）。

## Review Focus

- split 目标导入后 `last_applied_decision_ts` 是否等于 watermark，而不是 0？
- split 源有未 apply 决策（cursor < current）时是否 fail-closed，而不是让目标跳过？
- merge 接收方领先 donor 时是否拒绝，而不是丢掉 donor 尚未 apply 的决策？
- Watch 去重是否只合并跨 record 的重复 key，同一事务内同 key 的多次变更是否保留？
- cutover 前后 `WatchCursor` 是否单调连续，重连是否不重放？
- v1/`[kv_t]` 关闭时是否有任何行为变化？

---

### Task 1: split 目标从导入水位续跑决策

**Files:**
- Modify: `coord-server/src/storage/mvcc.rs`
- Test: `coord-server/src/storage/mvcc.rs`

**Interfaces:**
- `MvccStorage::apply_range_import(..., seal_prior_history: bool, done: bool, applied)` 行为：`seal_prior_history && done` 时除现有 revision/压缩水位外，写 `DECISION_APPLIED_TS = max(existing, watermark)`。

- [x] **Step 1: 写失败测试**

在 range import 测试旁新增：导入 watermark=11 后 `last_applied_decision_ts() == 11`；随后应用 `commit_ts=5` 的旧决策返回 `through_ts == 11`（跳过），再应用 `commit_ts=12` 的新决策成功且行可见。

- [x] **Step 2: 运行测试确认失败**

Run: `cargo test -p coord-server --lib storage::mvcc::tests::test_apply_range_import_seeds_target_decision_cursor`
Expected: FAIL（实际游标 0，旧决策把 apply 卡住）。

- [x] **Step 3: 实现**

在 `apply_range_import` 的 `if done { if seal_prior_history { ... } }` 分支写入决策游标（只前进不后退）。

- [x] **Step 4: 运行测试确认通过**

Run: 同一命令；Expected: PASS。

### Task 2: 搬迁器校验决策水位

**Files:**
- Modify: `coord-server/src/split_merge.rs`
- Test: `coord-server/tests/kv_t_g5_split_migrator_test.rs`
- Test: `coord-server/tests/kv_t_g5_merge_migrator_test.rs`

**Interfaces:**
- `RangeImportMigrator::migrate_split`：导出前要求源 `last_applied_decision_ts() == current_revision()`。
- `RangeImportMigrator::migrate_merge`：drain 后要求 donor 与 receiver 的 `current_revision()` 相等，且各自 `last_applied_decision_ts() == current_revision()`。

- [x] **Step 1: 改写测试播种，先暴露缺口**

split 源/merge donor/left 的测试数据改由 `apply_commit_decisions` 播种（保留 lease 与 tombstone）；断言 split 目标游标=源水位、后续决策可 apply。新增 merge「donor 落后 receiver」失败用例。

- [x] **Step 2: 运行测试确认现状**

Run: `cargo test -p coord-server --test kv_t_g5_split_migrator_test --test kv_t_g5_merge_migrator_test`
Expected: 新断言/前置条件用例 FAIL。

- [x] **Step 3: 实现前置条件**

在 drain 之后、导出之前检查；错误消息写明哪一侧的决策游标落后于水位（executor 会保留 intent 重试）。

- [x] **Step 4: 全量跑两个测试文件**

Run: 同上；Expected: PASS。

### Task 3: Watch 跨 Region 重放去重

**Files:**
- Modify: `coord-server/src/kvt/watch.rs`
- Test: `coord-server/src/kvt/watch.rs`
- Create: `coord/tests/kv_t_g5_continuity_test.rs`
- Modify: `coord/tests/support/kv_t_watch_node.rs`

- [x] **Step 1: 写失败单测**

`WatchEvent::from_records` 输入两条 `region_id` 不同、key 相同的记录 → 只产出一个 change；同 record 内同 key 两次变更 → 两个都保留；不相交 key 的 records → 全部保留。

- [x] **Step 2: 跑单测确认失败**

Run: `cargo test -p coord-server --lib kvt::watch::tests::event_dedups_cross_region_replays`
Expected: FAIL。

- [x] **Step 3: 实现去重并跑单测**

按 record 顺序维护已见 key 集合；跨 record 只保留首个 change。Run: 同上；Expected: PASS。

- [x] **Step 4: 写 `test_watch_survives_split_merge`**

夹具新增「应用表并装配新 Region」与「向指定 Region 直写」入口。测试：写 `aa` → split 表 + 装配 Region 3 → 对同一 key `nn`/同一 `commit_ts` 同时写父/子 Region（模拟 cutover 重放）→ merge 表 → 写 `zz`；从 split 前游标读，断言事件按 ts 单调、`nn` 只出现一次、cursor 连续、从末游标重订无事件。

- [x] **Step 5: 跑 continuity 测试**

Run: `cargo test -p coord --test kv_t_g5_continuity_test`
Expected: PASS。

### Task 4: #161 storage 入口覆盖与文档

**Files:**
- Modify: `coord/tests/kv_t_g5_routing_epoch_test.rs`
- Modify: `docs/production/ops/boundaries.md`
- Modify: `docs/adr/0015-kv-t.md`

- [x] **Step 1: 在路由 epoch 测试中挂载 `coord.storage.v2`**

给测试节点登记 `Storage` 能力与 object limits；用过期 epoch 调 `Stat`/`Delete`，断言 `ABORTED + EPOCH_STALE` 且 trailer 回传服务端版本；补 `LeaseRevoke`。

- [x] **Step 2: 更新边界与 ADR**

B-KVT-9/B-KVT-10 写明决策游标与 merge 收敛前置；D6 落地行写明 split 目标游标、merge 收敛、Watch 去重与仍 open 的直写冻结。

- [x] **Step 3: 文档引用检查**

Run: `bash scripts/check-doc-refs.sh`；Expected: 悬空路径 0。

### Task 5: 门禁与交付

- [x] `cargo fmt --all -- --check`
- [x] `cargo clippy --workspace --lib --bins -- -D warnings`
- [x] `bash scripts/check-panics.sh`
- [x] `cargo audit` / `cargo deny check`（advisories / licenses / bans / sources）
- [x] `cargo test -p coord-server --lib`、`cargo test -p coord-server --test kv_t_g5_split_migrator_test --test kv_t_g5_merge_migrator_test`、`cargo test -p coord --test kv_t_g5_routing_epoch_test --test kv_t_g5_continuity_test`
- [ ] 分支 → PR → CI 全绿 → merge → 删分支；关闭 #161，在 #167 留进展与残余项。
