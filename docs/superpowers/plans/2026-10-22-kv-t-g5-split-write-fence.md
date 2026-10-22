# KV-T G5 (#167/#161) Split Source Write Fence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** split 的 export→cutover 窗口内不再接受落在搬迁区间的 `coord.kv.v2` 直写：fence 作为源 Region **自己 raft 日志**里的一条命令落地，因此与它要挡的直写共享同一个 apply 序——apply 在 fence 之前被写的行一定进了导出快照，之后到达的直写一律 `ABORTED` + `EPOCH_STALE`（客户端刷新路由后重试）。

**Architecture:** 新增 `Command::FenceDirectWrites { start_key, end_key, op_id }` / `Command::ClearDirectWriteFence { op_id }` 与对应 `Response`；fence 记录写源 Region MVCC 的 `/_sys/kv_t/direct_write_fence`（随 `/_sys/*` 快照域恢复，换 leader/重启不回退）。`PutV2`/`DeleteV2`/`DeleteRangeV2` 的 apply 在写之前读该记录，命中即返回 `Response::DirectWritesFenced`（不返回 `Err`，D11.5）。执行器在 `migrate` 之前向源 Region 提案 fence、在 intent 离开 `Prepared` 或本次执行失败时清除。**决策 apply（`ApplyCommitDecisions`）、safe_ts、导入命令不 fence**：前者由子 Region 从决策日志续跑，后两者不是搬迁区间的直写。

**Tech Stack:** Rust / openraft / redb / tonic。

**Spec:** issue #167「迁移窗口内的直接 KV 写冻结」、issue #161「cutover 双写/双读守卫」；ADR-0015 D6/D11.5/D12.5；边界 `docs/production/ops/boundaries.md` B-KVT-9/B-KVT-10；上一刀 `docs/superpowers/plans/2026-10-22-kv-t-g5-continuity.md`。

## Global Constraints

- `[kv_t].enabled=false`（默认）时不得改变 v1 行为（ADR-0001）：fence 只由 v2 split 执行器提案，v1 集群永远没有 fence 记录；未启用 `[kv_t]` 的 Region 收到 fence 命令按无害记录 apply（与 `ApplyCommitDecisions` 同一口径）。
- 用户级拒绝不得让 raft apply 返回 `Err`（D11.5）：被 fence 的直写返回既有 `EPOCH_STALE` 语义的响应，不返回存储错误。
- wire 契约不变：不新增错误码、不改 proto。`EPOCH_STALE` 已定义为「刷新路由后重试」（D12.5），迁移窗口复用同一码。
- fence 记录必须随快照携带（`/_sys/*` 域），否则换 leader/装快照后旧区间重新可写。
- 注释只写当前「为什么」；不写日期/变更史；不新增 `TODO`/`FIXME`。
- 临时文件只放 `/tmp/coord-*`；提交前跑 AGENTS.md 门禁（fmt / clippy / check-panics / audit / deny / check-doc-refs）。

## Rulings（本计划对 spec 的落地裁定）

1. **fence 进源 Region 的 raft 日志，不做节点本地开关**：直写只能由 Region leader 提案（follower 返回 `RegionNotLeader`，`map_region_client_write_error` 不转发）。fence 也在同一日志里，apply 序即真相：写进 fence 之前的直写一定已 apply 并进入之后的导出水位；fence 之后到达的直写被 apply 拒绝。换 leader 不丢 fence（状态机状态随日志复制），节点本地标记会。
2. **fence 只覆盖被搬迁的区间，不冻结整个 Region**：split 的父 Region 左半段仍然在线可写。区间取计划里的子 Region `[start_key, end_key)`；`end_key` 为空表示 +∞。
3. **fence 只挡三类直写**：`PutV2` / `DeleteV2` / `DeleteRangeV2`（客户端自带 `commit_ts`，不经过决策日志，是「落父 Region 后丢失」的唯一路径）。`DeleteRangeV2` 只要与 fence 区间相交就整条拒绝（部分应用会留下半搬状态）。
4. **fence 不挡决策与水位**：`ApplyCommitDecisions` / `AdvanceSafeTs` / `PublishCommitWatermark` / `ImportRange*` 继续 apply。决策 > 导出水位的行由子 Region 从决策日志续跑（连续性切片已保证游标），父 Region 侧的同 `commit_ts` 行在 cutover 后成为孤儿。
5. **clear 必须匹配 op_id**：同 op_id 的 arm/clear 幂等；不同 op_id 的 clear 是 no-op（迟到的旧 intent 不能解开新一次迁移的 fence）。arm 带不同 op_id 表示新一轮迁移，直接替换。
6. **执行器持有 fence 生命周期**：arm 在 `migrate` 之前；clear 在 intent 进入 `Applied`/`Rejected` 之后，以及本次执行返回 `Err` 时。失败时清掉是正确且必要——写入会落在尚未 cutover 的父 Region，下一次重试重新导出即包含它们；持续持有会把卡住的 split 变成「区间永久只读」。终态（`Applied`/`Rejected`）的 clear 是 **best-effort**：提不出去（换届后本节点不再是源 Region leader）不能把 intent 永久钉在 `Applied`；此时 fence 对 cutover 后的父 Region 区间是惰性的（新区间不再覆盖它）。
7. **merge donor 不在本切片**：merge 的执行器认领条件是**吸收方（左）**的 leader，donor 的 leader 可能在别的节点；从本节点向 donor raft 提案只会拿到 `ForwardToLeader`（会变成永久失败）。donor 的 fence 需要「donor leader 自己认领 fence」的机制（或跨节点提案通道），留作 #167 的后续切片，边界文档写明残余。
8. **merge 的 arm = 无条件 clear**：`ClearDirectWriteFence { op_id: 0 }` 表示无条件清除，merge 在吸收方扩回被吸收区间前提一条（吸收方就是本节点 leader）。这是上面 best-effort 释放的兜底：一条陈旧的 split fence 否则会在 merge 扩回区间后把那段变成永久拒写。
9. **租约清理扇出不在本切片**：`DeleteKeysByLeaseV2` 由各节点 revoker 在**本节点担任 leader 的 Region** 上提案，且失败会留在 pending 里重扫/换新 `commit_ts` 重试；窗口内它仍可能落父 Region，但不会丢数据（子 Region 的 revoker 会再次清理），因此不与直写同批处理，边界文档记一行。

## File Structure

- Modify: `coord-server/src/raft/type_config.rs` — 两个 `Command` 变体 + 一个 `Response` 变体（末尾追加，旧日志继续可解码）；`Display` 分支。
- Modify: `coord-server/src/storage/mvcc.rs` — fence 记录读写（`/_sys/kv_t/direct_write_fence`）+ 区间相交判据 + 单测。
- Modify: `coord-server/src/raft/state_machine.rs` — fence/clear 的 apply 分支；三类直写的 apply 前检查；单测。
- Modify: `coord-server/src/kvt/services.rs` — `DirectWritesFenced` → `ABORTED` + `EPOCH_STALE`（put/delete 两处）。
- Modify: `coord-server/src/split_merge.rs` — `RangeImportMigrator` 增加源 Region raft 解析器；`arm_write_fence` / `release_write_fence` 实现（split = 冻结子区间；merge = 吸收方无条件清旧 fence）。
- Modify: `coord-server/src/pd/executor.rs` — `SplitMergeMigrator` 增加 arm/release 两个方法；`execute_split_merge` 的 fence 生命周期；执行器单测（顺序 + 失败即清）。
- Modify: `coord-server/tests/kv_t_g5_split_merge_test.rs` — `RecordingMigrator` 补齐 arm/release。
- Modify: `coord-server/tests/kv_t_g5_split_migrator_test.rs`、`coord-server/tests/kv_t_g5_merge_migrator_test.rs` — `RangeImportMigrator::new` 的新参数（这些夹具不测 fence，传空解析器）。
- Modify: `coord-server/src/storage/snapshot.rs` — fence 记录随 `/_sys/*` 域进快照的单测。
- Create: `coord/tests/kv_t_g5_write_fence_test.rs` — 真实节点：fence 生效期间区间内直写 `EPOCH_STALE`、区间外照常、清除后区间内可写。
- 生产接线无需改 `coord/src/main.rs`：`range_import_migrator` 已拿到 `RegionManager`，源 raft 解析器在内部构造。
- Modify: `docs/adr/0015-kv-t.md` — D6 落地行 + D12.5 一句（迁移窗口也返回 `EPOCH_STALE`）。
- Modify: `docs/production/ops/boundaries.md` — B-KVT-9/B-KVT-10 现状与残余（merge donor、租约扇出）。

## Review Focus

- fence 应用后，落在 fence 区间内、`commit_ts` 更大的直写是否一律拒绝，而不是「拒绝一次后又被接受」？
- fence 区间外的键（父 Region 左半段）是否仍然可写，v1/`[kv_t]` 关闭时是否完全无变化？
- fence 记录是否随 `/_sys/*` 快照恢复（装快照/换 leader 后仍然拒绝）？
- `DeleteRangeV2` 与 fence 区间相交时是否整条拒绝，而不是只挡相交部分？
- 执行器是否在 `migrate` 之前 arm、在 `Applied`/`Rejected`/失败之后 clear，且 clear 带同一 op_id（merge 侧是无条件 clear）？
- 被 fence 的直写是否返回 `ABORTED` + `EPOCH_STALE`（可重试）而不是 `INTERNAL`？

---

### Task 1: MVCC 侧 fence 记录与相交判据

**Files:**
- Modify: `coord-server/src/storage/mvcc.rs`

**Interfaces:**
- `MvccStorage::fence_direct_writes(&self, start: &[u8], end: &[u8], op_id: u64, applied: AppliedLogId) -> Result<u64>`：写入/替换记录，返回生效的 op_id。
- `MvccStorage::clear_direct_write_fence(&self, op_id: u64, applied: AppliedLogId) -> Result<()>`：仅当记录 op_id 匹配时删除。
- `MvccStorage::direct_write_fence(&self) -> Result<Option<(Vec<u8>, Vec<u8>, u64)>>`。
- `MvccStorage::direct_write_fence_hit(&self, key: &[u8], end: Option<&[u8]>) -> Result<Option<u64>>`：点写（`end=None`）/区间写命中返回 fence op_id。

- [x] **Step 1: 写失败测试**：fence 后点写命中、区间相交命中（含空 `end` = +∞）、区间之外不命中；不同 op_id 的 clear 是 no-op、同 op_id 清除后不再命中。
- [x] **Step 2: 运行确认失败**：`cargo test -p coord-server --lib storage::mvcc::tests::test_direct_write_fence`。
- [x] **Step 3: 实现**：记录编码 `op_id(u64 BE) || start_len(u32 BE) || start || end`，写在 `TABLE_KV`；按 `applied_contains` 幂等；同时推进 `META_LAST_APPLIED`。
- [x] **Step 4: 通过**：同一命令 PASS。

### Task 2: raft 命令与 apply 拒绝

**Files:**
- Modify: `coord-server/src/raft/type_config.rs`
- Modify: `coord-server/src/raft/state_machine.rs`

- [x] **Step 1: 写失败测试**（`state_machine.rs` 既有 apply 测试旁）：CommitTs 模式下 arm 后 `PutV2`（区间内）返回 `DirectWritesFenced`、区间外返回 `Put`；`DeleteRangeV2` 相交返回 `DirectWritesFenced`；`ApplyCommitDecisions` 仍然推进游标；clear 后区间内 `PutV2` 成功。
- [x] **Step 2: 运行确认失败**。
- [x] **Step 3: 实现**：`Command::FenceDirectWrites` / `Command::ClearDirectWriteFence` 分支调用 Task 1；`PutV2`/`DeleteV2`/`DeleteRangeV2` 在调用存储前查 `direct_write_fence_hit`，命中时 `set_last_applied` + `Response::DirectWritesFenced { op_id }`（无 Watch 事件、不推进 revision）。
- [x] **Step 4: 通过**。

### Task 3: gRPC 侧映射

**Files:**
- Modify: `coord-server/src/kvt/services.rs`
- Test: 复用 Task 5 的集成测试

- [x] **Step 1: 实现**：put/delete 的 response match 增加 `DirectWritesFenced` → `attach(Status::aborted(...), CoordErrorCode::EpochStale)`；消息说明「region migrating; refresh route and retry」，与 `guard_routing_epoch` 同码不同消息（调用方只按码分支，D12.5）。
- [x] **Step 2: 本地编译**：`cargo build -p coord-server`。

### Task 4: 搬迁器与执行器接线

**Files:**
- Modify: `coord-server/src/split_merge.rs`
- Modify: `coord-server/src/pd/executor.rs`
- Modify: `coord-server/tests/kv_t_g5_split_merge_test.rs`
- Modify: `coord/src/main.rs`

**Interfaces:**
- `SplitMergeMigrator` 新增 `arm_write_fence(&self, op_id: u64, plan: &SplitMergePlan) -> Result<(), String>` 与 `release_write_fence(&self, op_id: u64, plan: &SplitMergePlan)`（同步方法；默认无实现者只有测试替身）。
- `range_import_migrator(manager, lifecycle, drain)` 内部用 `manager.runtime(id).map(|rt| rt.raft.clone())` 解析源 Region raft。

- [x] **Step 1: 写失败测试**（executor 单测）：`FakeMigrator` 记录 `arm`/`release`；split 成功路径断言 `arm` 在 `migrate` 之前、`release` 在 cutover 之后且 op_id 一致；`migrate` 失败路径断言 `release` 仍被调用。
- [x] **Step 2: 运行确认失败**。
- [x] **Step 3: 实现**：`RangeImportMigrator::arm_write_fence` 解析 split 计划里的子区间与源 raft，等 leader 后提案 `FenceDirectWrites`；`release_write_fence` 提案 `ClearDirectWriteFence`（失败只 warn）。merge 计划 arm/release 都是 no-op（裁定 7）。执行器把 `execute_split_merge` 拆成「arm → 驱动 intent → finally release」。
- [x] **Step 4: 通过**：`cargo test -p coord-server --lib pd::executor`。

### Task 5: 真实节点集成测试

**Files:**
- Create: `coord/tests/kv_t_g5_write_fence_test.rs`

- [x] **Step 1: 写测试**：装配 v2 单 Region 节点（沿用 `coord/tests/kv_t_g5_*` 夹具），向 Region raft 提案 `FenceDirectWrites { "m".."" }`，断言：`Put("m2")` → `ABORTED` + `EPOCH_STALE`；`Put("a")` 成功；`DeleteRangeV2(["a","z"))` → `EPOCH_STALE`；clear 后 `Put("m2")` 成功。
- [x] **Step 2: 运行确认失败 → 实现对齐 → 通过**：`cargo test -p coord --test kv_t_g5_write_fence_test`。

### Task 6: 文档与门禁

- [x] **Step 1: ADR/边界现状**：ADR-0015 D6 落地行写 fence 的日志序与残余（merge donor、租约扇出）；D12.5 一句「迁移窗口内的直写与 cutover 后旧 epoch 同码」；boundaries B-KVT-9/B-KVT-10 同步。
- [x] **Step 2: 文档引用检查**：`bash scripts/check-doc-refs.sh`（悬空 0）。
- [x] **Step 3: 门禁**：fmt / clippy `-D warnings` / check-panics / audit / deny。
- [x] **Step 4: 测试**：`cargo test -p coord-server --lib`、`cargo test -p coord --test kv_t_g5_write_fence_test --test kv_t_g5_routing_epoch_test`、`cargo test -p coord-server --test kv_t_g5_split_merge_test`。
- [x] **Step 5: 交付**：分支 → PR → CI 全绿 → merge → 删分支；#167/#161 留进展与残余项。
