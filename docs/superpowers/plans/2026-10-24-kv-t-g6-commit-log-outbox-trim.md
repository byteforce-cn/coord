# KV-T G6 (#174) Commit Log outbox 裁剪 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 数据 Region 的 Commit Log staging outbox（`/_sys/commit_log/1/{ts}/{region}`）随 shard 回收水位一起裁剪，磁盘上界回到「保留窗口 × 写入速率」，不再只由 shard 组的回收决定。

**Architecture:** 裁剪是**数据 Region 自己的 raft 命令**（`Command::TrimCommitLogOutbox`），由 region 0 的 `gc_before_ts` 驱动：该水位以下的 shard 已整体回收，outbox 记录不再被 ship/Watch 读取。shard 协调器在每个 tick 里对**本节点领导的数据 Region** 提案，命令带 `through_ts` 与 `max_rows`（确定性上限），apply 只删 staging shard 中 `commit_ts < through_ts` 的行并推进本地 `META_COMMIT_LOG_TRIMMED_BEFORE`。装 Raft 快照时该水位随数据一起作废（行已被整体替换），由下一次 tick 重建。

**Tech Stack:** Rust / openraft / redb；coord-server 状态机 + kvt shard 协调器。

**Spec:** issue #174 的容量/写放大残余（#157 shard GC 后 outbox 无裁剪路径）、ADR-0015 D5/D9（保留窗口与回收粒度）、`docs/production/ops/boundaries.md` B-KVT-4。

## Global Constraints

- `[kv_t].enabled=false` 时没有任何新命令被提出，v1 行为/磁盘布局不变（ADR-0001）。
- apply 不得返回 `Err`（D11.5）：`through_ts` 过小/重放/空区间一律幂等 no-op。
- 水位只由 raft apply 写：禁止 leader 在本地直接删（会破坏副本确定性/快照重放）。
- 只删 `commit_ts < through_ts` 的 **staging shard** 行；`through_ts` 来自 region 0 的 `gc_before_ts`（整 shard 边界），不自行推导。
- 单条命令最多删 `T` 行（`max_rows` 在命令里，replay 不读本地配置）；未删完由下一 tick 继续。
- 不新增 wire/proto 字段；不改变 Watch 可读窗口与 `HISTORY_UNAVAILABLE` 边界。
- 注释只写当前「为什么」；不新增 `TODO`/`FIXME`；临时文件只在 `/tmp/coord-*`。
- 合入前跑 AGENTS.md 门禁（fmt / clippy / check-panics / audit / deny / check-doc-refs）。

## Rulings

1. **裁剪挂在 shard 协调器上，不挂在写入路径。** 写入路径不知道 `gc_before_ts`；在每次 Put/Commit 上读 region 0 表会把跨状态机读带进 apply 序。tick 是唯一同时看得到两边的位置。
2. **按行数切片，不按字节。** 删除不搬运 value，命令只带 `through_ts/max_rows`；apply 自己按 key 序扫（确定性）。读到下一行只为算出新的 `trimmed_before`，与 ship 的 256 行读取同量级。
3. **`trimmed_before` 的语义是「所有 `< 它的行已删除」。** 被 `max_rows` 截断时取剩余最小 `commit_ts`；这保证下一轮从正确位置继续，且不会把同 `commit_ts` 的其它 Region 分片留在界内。
4. **回收优先于裁剪。** 同一 tick 先 `advance_gc_metadata` 再裁剪；裁剪失败只告警，不影响 seal/ship/watermark（容量优化不能变成可用性故障）。
5. **快照导入清空该水位。** 快照整体替换 `TABLE_COMMIT_LOG`，旧水位不再描述新状态；保留它会让被快照带回来的低位行永久不删。

## File Structure

| 文件 | 责任 |
|:--|:--|
| `coord-server/src/storage/commit_log.rs` | key → `commit_ts` 解码 helper |
| `coord-server/src/storage/mvcc.rs` | `trim_commit_log_outbox` / `commit_log_trimmed_before` + 单测 |
| `coord-server/src/raft/type_config.rs` | `Command::TrimCommitLogOutbox`、`Response::CommitLogOutboxTrimmed` |
| `coord-server/src/raft/state_machine.rs` | apply 分支 + 单测 |
| `coord-server/src/kvt/commit_log_shards.rs` | tick 触发、批量循环、`trimmed` 计数 + 集成测试 |
| `coord-server/src/metrics.rs` | `coord_kv_t_commit_log_outbox_rows_trimmed_total` |
| `coord-server/src/kvt/commit_log_shards.rs`（`start_shard_manager`） | tick 指标接线 |
| `docs/production/ops/boundaries.md` | B-KVT-4 判据补裁剪与证据 |
| `docs/adr/0015-kv-t.md` | 验收锚点补一条（不新增语义） |

## Tasks

### Task 1: MVCC 裁剪原语

- [x] Step 1: 写失败测试 `storage::mvcc::tests::test_trim_commit_log_outbox_is_monotonic_and_idempotent`
      与 `..._stops_at_max_rows_and_reports_the_next_ts`。
- [x] Step 2: 跑测试确认失败（方法不存在）。
- [x] Step 3: 实现 `trim_commit_log_outbox` / `commit_log_trimmed_before`、
      `META_COMMIT_LOG_TRIMMED_BEFORE`、key 解码 helper；快照导入清空水位。
- [x] Step 4: 测试绿；补快照 roundtrip（裁剪后导出/导入不复活行）。

### Task 2: raft 命令与 apply

- [x] Step 1: 写失败测试：`apply` 返回 `Response::CommitLogOutboxTrimmed`，
      重放同一 applied 不重复删除；`through_ts=0`/更小值是 no-op。
- [x] Step 2: 加 `Command`/`Response` 变体（末尾追加）与 apply 分支。
- [x] Step 3: 测试绿。

### Task 3: tick 触发与指标

- [x] Step 1: 写失败测试 `gc_trims_outbox_rows_below_the_reclaimed_boundary_once`：
      shard 回收后 tick 删除 `< gc_before_ts` 的 outbox 行，shard 记录仍可读；
      再 tick 不重复删除。
- [x] Step 2: `CommitLogShardManager::trim_outboxes`：只对本节点 leader 的数据 Region
      提案，每 tick 有界循环；失败只 warn。
- [x] Step 3: `CommitLogShardTick.trimmed` + metrics 计数器；`coord/src/main.rs` 接线。
- [x] Step 4: 测试绿。

### Task 4: 文档与证据

- [x] `boundaries.md` B-KVT-4 补裁剪判据/测试名；`0015-kv-t.md` 验收锚点补一行。
- [x] `bash scripts/check-doc-refs.sh` 0 悬空（干净 worktree）。
- [ ] PR 描述与 #174 评论写证据（测试名/命令）；`[kv_t].enabled=false` 回归由全量 workspace 测试覆盖。
