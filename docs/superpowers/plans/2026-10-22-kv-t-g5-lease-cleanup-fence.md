# KV-T G5 (#167) 迁移窗口内的租约清理 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** split/merge 的迁移窗口内不再把 `DeleteKeysByLeaseV2` 落进正在搬迁的 Region（父/donor 导出后的删除会让目标复活、目标侧空跑会把 `commit_ts` 烧成 replay 让导入行永久删不掉），窗口结束后按当时的 Region 表把清理补上。

**Architecture:** 复用已有的直写 fence（`FenceDirectWrites`）作为「本 Region 正在搬迁」的区域级标记：apply 期只要 fence 生效就整条拒绝租约清理（`CommitRejected`，不写数据/不写 changelog/不推 revision），revoker 在本地副本看到 fence 时把该租约清理**挂起**（不重取 `commit_ts`，避免窗口内 10Hz 换时间戳），等挂起时记录的 Region 都不再 fence 后按当时的 Region 表重新扇出一次。空清理（没有任何匹配行）不再消费 `commit_ts`：它不写 changelog/revision，否则同一 ts 的后续清理会被当成 replay，搬迁后才进来的旧 revision 行永远删不掉。

**Tech Stack:** Rust / openraft / redb / tokio。

**Spec:** issue #167「lease revoke 扇出」残余、issue #161「cutover 禁止双写/双读」、ADR-0015 D6/D11.4/D11.6、边界 `docs/production/ops/boundaries.md` B-KVT-10；上一刀 `docs/superpowers/plans/2026-10-22-kv-t-g5-merge-donor-fence.md`（裁定 6 明确本项为残余）。

## Global Constraints

- `[kv_t].enabled=false`（默认）时不得改变 v1 行为（ADR-0001）：v1 的 `Command::DeleteKeysByLease`（log-index 路径）不读 fence、不改语义；fence 只可能在 v2 装配里出现。
- 用户级拒绝不得让 raft apply 返回 `Err`（D11.5）：被 fence 挡住的清理仍走 `CommitRejected`，由 revoker 换 `commit_ts` 重试。
- wire 契约不变：不新增错误码、不改 proto、不新增 raft 命令变体；只复用 `FenceDirectWrites` 与既有 `DeleteKeysByLeaseV2` 响应。
- `commit_ts` 全局唯一、绝不复用（D11.1）；空清理不落 changelog 只是「不占 revision」，不是把 ts 再发给别人。
- 注释只写当前「为什么」；不写日期/变更史；不新增 `TODO`/`FIXME`。
- 临时文件只放 `/tmp/coord-*`；提交前跑 AGENTS.md 门禁（fmt / clippy / check-panics / audit / deny / check-doc-refs）。

## Rulings

1. **fence 是区域级判据，不按 key 判。** 窗口内整块 Region 的 key 归属都在变：split 源把 `[start, end)` 交给目标，merge donor 把整段交给吸收方。清理只在「删哪些行」上看 key 会把「没有匹配行」当成可应用——而 merge 吸收方此刻正是没有任何匹配行（行还没搬进来），一旦落一条空 Lease 事件，`commit_ts` 变成 replay，导入进来的旧 revision 行就再也删不掉。因此 `delete_keys_by_lease_at_commit_ts` 见到 fence 一律 `CommitRejected`，不区分区间。
2. **空清理不消费 `commit_ts`。** 没有任何行匹配时不写 changelog、不推 `META_CURRENT_REVISION`（只推进 applied），返回 `applied + deleted=[]`；state machine 对空删除不产出 Watch 事件。搬迁把带同一 lease 的旧 revision 行搬进来后，同一 ts 的清理仍然可应用（或在 safe-ts 已越过它时按既有 `CommitRejected` → revoker 换 ts 路径重试）。
3. **revoker 挂起而不是换时间戳。** 本地 Region 副本有 fence 时，把 `(lease → commit_ts, 挡住它的 Region)` 从 pending 挪到挂起表：窗口内既不提案也不重取 TSO（否则一次迁移窗口会打出几十到几百条 region 0 revoke 决策）。挂起时记录的 Region 都消失（merge donor 组被移除）或 fence 已清（cutover 完成）后，用**当时的** Region 表重新扇出同一 `commit_ts`。
4. **不做吸收方 fence、不做跨协调节点 drain。** 吸收方在窗口内没有搬进来的行，空清理由裁定 2 兜底（不再永久损坏）；跨协调节点会话 drain 仍是 #167 的独立残余，边界文档保留。
5. **重扇出只解决「新 Region 没收到」，不保证 ts 一定可用。** cutover 后 safe-ts 推进器可能已把 Region 水位抬过挂起的 `commit_ts`；那次 `CommitRejected` 仍走既有路径（region 0 leader 换 ts 并广播），与今天 rescan 的收敛口径一致。

## File Structure

- Modify: `coord-server/src/storage/mvcc.rs` — `delete_keys_by_lease_at_commit_ts` 的 fence 拒绝与空清理语义；单测。
- Modify: `coord-server/src/raft/state_machine.rs` — 空删除不产出 Lease Watch 事件。
- Modify: `coord-server/src/kvt/lease.rs` — revoker 的挂起表与窗口后重扇出；`region_fenced` 判定。
- Modify: `coord/tests/kv_t_g2_lease_rpc_test.rs` — 真实 Region raft + gRPC：fence 的 Region 延后清理、fence 清除后补清。
- Modify: `docs/adr/0015-kv-t.md` — D6 落地行与 D11.6 口径。
- Modify: `docs/production/ops/boundaries.md` — B-KVT-10 现状与残余。

## Review Focus

- fence 生效时，`DeleteKeysByLeaseV2` 是否**任何**匹配行数都被拒（含 0 行），且不写数据/changelog/revision？
- 空清理之后同一 `commit_ts` 的新增匹配行是否还能被删（不是 replay），而不是永久残留？
- revoker 在窗口内是否真的不提案、不换 `commit_ts`（而不是 10Hz 打 region 0）？
- 窗口结束（fence 清 / Region 组消失）后是否按**当时**的 Region 表重扇出，cutover 后的新 Region 不漏清？
- `[kv_t].enabled=false`、legacy 单 raft 路径、以及 v1 `DeleteKeysByLease` 是否完全不变？

---

### Task 1: 存储层的 fence 拒绝与空清理语义

**Files:**
- Modify: `coord-server/src/storage/mvcc.rs`
- Modify: `coord-server/src/raft/state_machine.rs`

**Interfaces:**
- `MvccStorage::delete_keys_by_lease_at_commit_ts(lease_id, commit_ts, applied) -> Result<(ApplyOutcome, Vec<Vec<u8>>)>`：
  - 有 fence → `Err(Error::CommitRejected { commit_ts, current_revision })`，无副作用；
  - 无匹配行 → `Ok((ApplyOutcome::applied(), vec![]))`，不写 changelog、不推 revision、只写 `META_LAST_APPLIED`；
  - 有匹配行 → 既有行为（`accept_commit_ts` + changelog + applied）。
- `Command::DeleteKeysByLeaseV2` apply：`outcome.replayed || deleted.is_empty()` 时不产出 `ChangeEvent`。

- [x] **Step 1: 写失败测试**（mvcc 单测）
  - `test_lease_v2_cleanup_defers_while_a_migration_fence_is_active`：fence 后清理返回 `CommitRejected`，两个绑定 key 仍在，`current_revision` 不动；fence 清除后同一 ts 删除成功。
  - `test_lease_v2_empty_cleanup_does_not_consume_the_commit_ts`：无匹配行 → 无 changelog、`current_revision` 不动；随后写入更旧 revision 的绑定行（模拟 merge 导入），同一 ts 的清理删除它。
- [x] **Step 2: 运行确认失败**：`cargo test -p coord-server --lib storage::mvcc::tests::test_lease_v2_cleanup`、`...::test_lease_v2_empty_cleanup`。
- [x] **Step 3: 实现**：fence 判定放写事务最前（`direct_write_fence_in_tx`），匹配为空时提前返回；`accept_commit_ts` 移到确认有删除之后。state machine 补空删除不产事件的判据。
- [x] **Step 4: 通过**：`cargo test -p coord-server --lib storage::mvcc`、`cargo test -p coord-server --lib raft::state_machine`。

### Task 2: revoker 的挂起与窗口后重扇出

**Files:**
- Modify: `coord-server/src/kvt/lease.rs`

**Interfaces:**
- 私有 `fn region_fenced(node: &CoordNode, region_id: RegionId) -> bool`：本节点有该 Region 副本时读 `rt.mvcc.direct_write_fence()`；读失败按「仍 fence」处理（fail-closed）；无副本返回 `false`。
- `start_kvt_lease_revoker` 新增挂起表 `BTreeMap<i64 /*lease*/, (u64 /*commit_ts*/, BTreeSet<RegionId>)>`：
  - 提案前 `region_fenced` 为真 → 记入挂起表并从 `pending` 移除，不提案；
  - 每个 tick 末：挂起表里记录的 Region 全部不再 fence（副本消失也算结束）→ `enqueue_lease` 用同一 `commit_ts` 重扇出。

- [x] **Step 1: 写失败测试**：见 Task 3 的集成用例（先红）。
- [x] **Step 2: 实现**挂起表 + `region_fenced` + tick 末重扇出；legacy（无 `region_manager`）路径不变。
- [x] **Step 3: 通过**：`cargo test -p coord-server --lib kvt::lease`（若有单测）+ Task 3。

### Task 3: 真实节点集成测试

**Files:**
- Modify: `coord/tests/kv_t_g2_lease_rpc_test.rs`

- [x] **Step 1: 写测试** `test_lease_v2_cleanup_defers_on_fenced_region_until_cutover`：
  - 授予租约，在两个 Region 各写一个绑定 key；
  - 对 Region 2 提案 `FenceDirectWrites`（split 源/donor 的窗口标记）；
  - `lease_revoke` 后：Region 1 的 key 被清（`wait_gone`）；500ms 后 Region 2 的 key 仍在（窗口内不落删除）；
  - 清除 Region 2 的 fence（cutover 完成）后 `wait_gone`：key 被补清。
- [x] **Step 2: 运行确认失败 → 实现 → 通过**：`cargo test -p coord --test kv_t_g2_lease_rpc_test`。

### Task 4: 文档与门禁

- [x] **Step 1: ADR/边界现状**：ADR-0015 D6 直写冻结行写入租约清理延期与「空清理不消费 ts」（D11.6）；boundaries B-KVT-10 去掉「租约 revoke 扇出」残余、只留跨协调节点 drain，并补判据测试名。
- [x] **Step 2: 文档引用检查**：`bash scripts/check-doc-refs.sh`（悬空 0）。
- [x] **Step 3: 门禁**：fmt / clippy `-D warnings` / check-panics / audit / deny。
- [x] **Step 4: 测试**：`cargo test -p coord-server --lib storage::mvcc`、`... raft::state_machine`、`cargo test -p coord --test kv_t_g2_lease_rpc_test --test kv_t_g5_write_fence_test`、`cargo test -p coord-server --test kv_t_g5_split_merge_test`。
- [x] **Step 5: 交付**：分支 → PR → CI 全绿 → merge → 删分支；#167 留进展与残余（跨协调节点 drain）。
