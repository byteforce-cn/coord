# MQ v2 M1-5/M1-6 — 保留策略与耐久性证据 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan
> task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 M1-5 的保留/配额口径做成可复跑的存储层策略与证据（按段回收、earliest/latest、
配额随回收释放、truncate 重放幂等），并补齐 M1-6 的耐久性测试（进程重启、副本一致性、
快照/恢复、组位点跨重启）。

**Architecture:** 不新增命令：保留回收仍由 `Command::MqTruncate` 承载，Leader 侧用纯函数
`mq_retention_cutoff` 从段年龄算出 `up_to_offset`；分片提供 `mq_partitions()` 枚举与
`mq_partition_bounds()` 供 reaper/Poll 使用。周期提案的运行时接线随 M2-1（分片 Leader
循环），本任务只交付确定性策略 + apply + 测试。

**Spec:** `docs/superpowers/specs/2026-10-29-mq-v2-design.md` §5.2/§5.6/§6 M1-5/M1-6；
`docs/adr/0016-mq-v2.md` D3/D7/D13。

## Global Constraints

- 保留只删**整段前缀**，至少保留最新段；`earliest_offset` 只前进、不回退。
- 时间由命令携带（`MqTruncate` 不带时间；`up_to_offset` 由提案方算好，apply 不读时钟）。
- 记账随回收在同一事务内扣减：`log_bytes`/`idem_bytes` 同步减少，`dlq_bytes` 不因主段回收变化。
- 配额拒绝仍是整批拒绝（既有 `MqAppendRejection`），不新增部分写路径。
- 非测试代码不得出现 `unwrap/expect/panic`。
- 不推进快照格式版本：组/段元数据仍在 `mq_meta`。

## File Structure

| 文件 | 责任 |
|:--|:--|
| `coord-server/src/storage/mq_log.rs` | `mq_partition_bounds`、`mq_partitions`、`mq_retention_cutoff` + 测试 |
| `coord-server/src/storage/mq_group.rs` | 组状态/位点跨重启测试 |
| `docs/production/ops/mq-v2-gate.md` | M1 行补 M1-5/M1-6 证据 |

## Review Focus

- **earliest 只前进**：多次 truncate（含重放）不得让 `earliest_offset` 回退。
- **配额释放**：回收后同一 topic 的 append 能再次通过；记账与段字节逐项一致。
- **幂等索引同水位回收**：低于 earliest 的幂等条目一并删除，重复键不会命中已回收 offset。
- **至少保留最新段**：即使 `up_to_offset` 覆盖全部段，也不能把最后一个段删掉。
- **重启不倒退**：重新打开同一目录后 offset/段/DLQ/组位点与关闭前一致。
- **副本一致性**：同一命令序列在两个 storage 上 apply 得到逐字相同的状态（换届后继续）。

---

### Task 1: 保留/配额策略（M1-5）

**Files:** `coord-server/src/storage/mq_log.rs`（实现 + 模块内测试）

**Interfaces:**
- `pub struct MqPartitionBounds { pub earliest_offset: u64, pub next_offset: u64 }`
- `MvccStorage::mq_partition_bounds(topic_id, partition) -> Result<Option<MqPartitionBounds>>`
- `MvccStorage::mq_partitions() -> Result<Vec<(u64, u32)>>`（本分片全部分区，按 key 升序）
- `pub fn mq_retention_cutoff(segments: &[MqSegmentMeta], now_ms: i64, retention_secs: u64) -> Option<u64>`
  （只对 `segments[..len-1]` 判断 `now_ms - opened_at_ms >= retention_secs*1000`，
  返回最后一个过期段的 `end_offset`；0 保留期/无过期段/单段 ⇒ `None`）

- [ ] **Step 1: 写失败测试**
  - `retention_cutoff_keeps_the_latest_segment`
  - `retention_cutoff_is_none_without_expired_segments`
  - `truncate_frees_quota_and_prunes_idempotency_entries`
  - `truncate_replay_is_idempotent`
  - `partition_bounds_track_earliest_and_latest`
- [ ] **Step 2: 跑测试确认失败**
  Run: `cargo test -p coord-server --lib storage::mq_log::tests::retention` Expected: FAIL（编译错）
- [ ] **Step 3: 实现**
- [ ] **Step 4: 跑测试确认通过**
  Run: `cargo test -p coord-server --lib storage::mq_log` Expected: PASS
- [ ] **Step 5: 提交** `feat(mq-v2): segment retention policy and partition bounds (M1-5)`

### Task 2: 耐久性（M1-6）

**Files:** `coord-server/src/storage/mq_log.rs`、`coord-server/src/storage/mq_group.rs`

- [ ] **Step 1: 写失败测试**
  - `reopening_the_storage_preserves_offsets_segments_and_dlq`（drop 后同目录重开）
  - `replica_applies_the_same_log_to_the_same_state`（两 storage 同命令序列 → 状态逐字相同；
    在“新 Leader”上继续追加 offset 接着走）
  - `group_state_and_offsets_survive_reopen`（组状态 + 位点跨重开）
- [ ] **Step 2: 跑测试确认失败**（重开测试若因锁/存储层问题失败，先修测试再修实现）
- [ ] **Step 3: 实现（如有）**
- [ ] **Step 4: 跑全部 MQ 存储测试**
  Run: `cargo test -p coord-server --lib storage::mq_log storage::mq_group`
- [ ] **Step 5: 提交** `test(mq-v2): durability across restart and replica replay (M1-6)`

### Task 3: Gate 证据与本地 CI

- [ ] 更新 `docs/production/ops/mq-v2-gate.md` M1 行：M1-5/M1-6 证据命令。
- [ ] `cargo fmt --all -- --check`、`cargo clippy --workspace --lib --bins -- -D warnings`、
      `bash scripts/check-panics.sh`、`bash scripts/check-doc-refs.sh`、`cargo audit`、`cargo deny check`。
- [ ] PR + `gh pr checks --watch` 全绿后 `gh pr merge --merge --delete-branch`。
