# KV-T G5 Merge Donor Write Fence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** merge 的 export→cutover 窗口内，被吸收方（donor Region）也不再接受落在搬迁区间的 v2 直写；吸收方从**已 apply 该 fence 的本地副本**导出，因此窗口内不会丢失写。

**Architecture:** fence 仍然只进 Region 自己的 raft 日志（`Command::FenceDirectWrites` / `ClearDirectWriteFence`）。split 由执行者（源 leader）直接提案；merge 的 donor leader 可能不在执行节点上，所以由**每个节点的 PD executor 在执行中的 merge intent 上自检**：本节点是 donor leader 时向 donor raft 提案 fence。吸收方在 `migrate` 前等待本地 donor 副本 apply 同一 `op_id` 的 fence——这一步同时是导出屏障：fence 之前的写一定在本地状态里，之后的直写会被 donor leader 拒绝。operator 进入终态后 donor leader 清除 fence；执行失败时吸收方尽力清除，donor leader 在条目离开 `Running` 后兜底清除。

**Tech Stack:** Rust / openraft / redb / tonic。

**Spec:** issue #167「merge donor 的 fence」、issue #161「cutover 双写/双读守卫」；ADR-0015 D6/D11.5/D12.5；边界 `docs/production/ops/boundaries.md` B-KVT-10；上一刀 `docs/superpowers/plans/2026-10-22-kv-t-g5-split-write-fence.md`。

## Global Constraints

- `[kv_t].enabled=false`（默认）时不得改变 v1 行为（ADR-0001）：没有 intent、没有 migrator、没有 fence 提案。
- 用户级拒绝不得让 raft apply 返回 `Err`（D11.5）：被 fence 的直写仍走既有 `ABORTED` + `EPOCH_STALE`。
- wire 契约不变：不新增错误码、不改 proto、不新增 region 0 命令；本切片只复用 `FenceDirectWrites` / `ClearDirectWriteFence` 与现有 intent/queue。
- fence 生命周期必须由 `op_id` 标识；不同 `op_id` 的 clear 是 no-op（已有语义）。
- 注释只写当前「为什么」；不写日期/变更史；不新增 `TODO`/`FIXME`。
- 临时文件只放 `/tmp/coord-*`；提交前跑 AGENTS.md 门禁（fmt / clippy / check-panics / audit / deny / check-doc-refs）。

## Rulings

1. **donor fence 由 donor leader 自己提案。** region raft 的 `client_write` 只在本节点是 leader 时可用；从吸收方节点向 donor 提案会得到 `ForwardToLeader`。每个节点的 PD executor 本来就会扫 region 0 intent（`recover_intents`），在这里加一条：intent 处于 `Prepared` 且队列条目 `Running` 时，本节点若是 donor leader 则提案 fence（同一 `op_id` 幂等）。
2. **吸收方等待本地 fence 而不是等待另一个信号。** `arm_write_fence`（merge 分支）先清吸收方的陈旧 fence，再尝试直接 arm（本节点同时是 donor leader 时），否则轮询本地 donor 副本的 `direct_write_fence()` 直到 `op_id` 匹配。本地副本 apply 了 fence，就说明它已经包含 fence 之前的所有写；之后的直写会被 donor leader 拒绝。等待有界（默认 30s，见 `with_donor_fence_wait`），超时整次失败、intent 保留重试。
3. **donor 必须本地可导出。** merge 的导出仍需要吸收方节点持有 donor 副本（现状）；没有本地副本时 arm 直接失败，不把 merge 停在半搬状态。
4. **失败与终态都要清，且 intent 以 fence 为收口条件。** 只有拥有 fence 的 Region leader（split 源 / merge donor）能清；其它节点返回 `Ok(false)`，`recover_intents` 的 orphan/terminal 分支保留 intent，下一拍由 owner 清完再收口——否则 absorbing 节点先看到终态就会把 fence 的最后一个句柄丢掉，区间永久只读。cutover 成功后 donor 组被移除，clear 是 no-op（`Applied` 不需等 owner）。
5. **不 fence 决策、safe_ts、导入。** 与本切片无关；行为不变。
6. **租约 revoke 扇出不在本切片**：`DeleteKeysByLeaseV2` 的窗口内冻结仍是 #167 残余，边界文档保留。

## File Structure

- Modify: `coord-server/src/pd/executor.rs` — `SplitMergeMigrator` 增加 donor arm/release 默认方法；`recover_intents` 在 Running merge intent 上 arm、在 terminal 上 release；单测。
- Modify: `coord-server/src/split_merge.rs` — `RangeImportMigrator` 增加 `node_id`、donor 区间解析、donor arm/release、merge arm 的本地 fence 等待；单测。
- Modify: `coord/src/main.rs` — 生产接线传入 `manager.node_id()`（若通过 `range_import_migrator` 内部取则无需改这里）。
- Modify: `coord/tests/kv_t_g5_write_fence_test.rs` — 真实 Region raft + gRPC：merge arm 后 donor 区间直写 `EPOCH_STALE`、吸收区间可写、release 后恢复。
- Modify: `docs/adr/0015-kv-t.md`、`docs/production/ops/boundaries.md` — 当前态与残余。
- Modify: `coord-server/tests/kv_t_g5_*_test.rs` — 夹具实现新增 trait 默认方法无需改；若编译需要再补。

## Review Focus

- donor leader 不在执行节点时，吸收方是否真的等到本地 fence 才导出（而不是只看 intent/leader 视图）？
- 等待超时是否让 operator 失败且保留 intent，而不是继续导出？
- 失败/终态后 donor fence 是否会被清掉（不留永久只读区间）？
- 同一 `op_id` 的 arm 是否幂等（donor leader 每拍扫描不会每拍写一条 raft 日志）？
- `[kv_t].enabled=false` / 未装 migrator 时是否完全 no-op？

---

### Task 1: donor 区间解析与 arm/release 端口

**Files:**
- Modify: `coord-server/src/split_merge.rs`
- Modify: `coord-server/src/pd/executor.rs`

**Interfaces:**
- `SplitMergeMigrator` 新增默认方法：
  - `async fn arm_donor_write_fence(&self, op_id: u64, plan: &SplitMergePlan) -> Result<bool, String>`：`true` = 本节点是 donor leader 且 fence 已生效；`false` = 不是 donor leader（或 plan 不是 merge）。
  - `async fn release_donor_write_fence(&self, op_id: u64, plan: &SplitMergePlan) -> Result<bool, String>`：`true` = 本地是 donor leader 且已清（或 donor 已不在本地表）；`false` = 本节点不负责（intent 留给 owner）。
- `RangeImportMigrator::merge_donor_range(plan) -> Result<(RegionId, Vec<u8>, Vec<u8>), String>`：从 plan 的 `RegionChange::Remove` 找到 donor，再用本地表解析区间。
- `RangeImportMigrator` 增加 `node_id`（`with_node_id`），生产由 `range_import_migrator` 从 `RegionManager::node_id()` 注入。

- [x] **Step 1: 写失败测试**：`test_merge_donor_range_matches_the_removed_region`（plan_merge 的 Remove region 与表区间一致）。
- [x] **Step 2: 运行确认失败**。
- [x] **Step 3: 实现**：trait 默认方法 + donor 区间解析 + `node_id` 字段。
- [x] **Step 4: 通过**。

### Task 2: donor arm/release 与 merge 导出屏障

**Files:**
- Modify: `coord-server/src/split_merge.rs`

- [x] **Step 1: 写失败测试（可单测的部分）**：本地 fence `op_id` 匹配时等待立即返回；不匹配/缺失时在 `with_fence_wait` 超时后返回 Err（用真实 `MvccStorage`，不启 raft）。
- [x] **Step 2: 运行确认失败**。
- [x] **Step 3: 实现**：
  - `arm_donor_write_fence`：先读本地 donor storage；已匹配 → `true`；不是本地 leader → `false`；否则提案 `FenceDirectWrites` 并校验回显。
  - `release_donor_write_fence`：donor 已不在本地表 → `Ok(true)`；不是本地 leader / 没有本地副本 → `Ok(false)`；否则单次提案 `ClearDirectWriteFence { op_id }`，成功 `Ok(true)`（下一拍兜底，不阻塞 executor）。
  - `arm_write_fence` merge 分支：清吸收方 fence → 尝试直接 arm → `wait_for_donor_fence(op_id, donor, storage)`。
  - `release_write_fence` 返回 `Result<bool, String>`：split 快速判 leader 后单次清除；merge 转发 `release_donor_write_fence`。
- [x] **Step 4: 通过**。

### Task 3: executor 的 donor leader 自检

**Files:**
- Modify: `coord-server/src/pd/executor.rs`

- [x] **Step 1: 写失败测试**：`FakeMigrator` 记录 `donor-arm` / `release`；merge intent + Running 条目 → `recover_intents` 调用 donor-arm；merge intent + 终态条目 → `release` 成功后清 intent，`release` 失败（非 owner）时保留 intent。
- [x] **Step 2: 运行确认失败**。
- [x] **Step 3: 实现**：`recover_intents` 的 Running（他人认领）分支调用 `arm_donor_write_fence`（仅 `Prepared`）；terminal/orphan 分支对非 `Applied` intent 先调 `release_write_fence`，只有 `Ok(true)` 才 `ClearIntent`，`Ok(false)`/Err 保留 intent 等 fence owner。
- [x] **Step 4: 通过**：`cargo test -p coord-server --lib pd::executor`。

### Task 4: 真实节点集成测试

**Files:**
- Modify: `coord/tests/kv_t_g5_write_fence_test.rs`

- [x] **Step 1: 写测试**：用 `WatchNode` 的两 Region 表构造 `plan_merge(1, 2)`，手工装配 `RangeImportMigrator`（node_id = 1）；`arm_write_fence` 后：
  - donor 区间 `"m2"` 的 v2 Put → `ABORTED` + `EPOCH_STALE`；
  - 吸收方区间 `"aa2"` 可写；
  - `release_write_fence` 后 `"m2"` 可写。
- [x] **Step 2: 运行确认失败 → 实现对齐 → 通过**：`cargo test -p coord --test kv_t_g5_write_fence_test`。

### Task 5: 文档与门禁

- [x] **Step 1: ADR/边界现状**：ADR-0015 D6 落地行写 donor-leader 自检、本地 fence 屏障与残留（跨协调节点 drain、租约扇出）；boundaries B-KVT-10 同步。
- [x] **Step 2: 文档引用检查**：`bash scripts/check-doc-refs.sh`（悬空 0）。
- [x] **Step 3: 门禁**：fmt / clippy `-D warnings` / check-panics / audit / deny。
- [x] **Step 4: 测试**：`cargo test -p coord-server --lib`、`cargo test -p coord --test kv_t_g5_write_fence_test --test kv_t_g5_routing_epoch_test`、`cargo test -p coord-server --test kv_t_g5_split_merge_test`。
- [x] **Step 5: 交付**：分支 → PR → CI 全绿 → merge → 删分支；#167/#161 留进展与残余。
