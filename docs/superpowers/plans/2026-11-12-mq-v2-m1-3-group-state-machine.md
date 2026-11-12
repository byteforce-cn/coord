# MQ v2 M1-3 — 消费组状态机实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在消费组分片的 `MvccStorage` 上落地确定性的消费组状态机
（`MqGroupJoin/Heartbeat/Leave/Reap/Ack`），让成员、代际、分区分配与已提交位点在
快照/重放/换届后不倒退，且一个代际内一个分区至多分配给一个成员。

**Architecture:** 新增 `coord-server/src/storage/mq_group.rs`，与 M1-2 的段日志同构：
命令载荷携带全部非确定性输入（`now_ms`、订阅的 topic_id/分区数），apply 只读
`TABLE_MQ_META` 并按 applied index 幂等（缓存最近一次结果），位点按
`(group, topic_id, partition)` 单行存储（不在热路径重写整张组表）。组行与位点行落在
`TABLE_MQ_META`，因此复用既有快照通道（`mq_meta_entries`），不推进快照格式版本。

**Tech Stack:** Rust 2021、redb、postcard + 既有 `envelope` 信封、openraft 状态机
（`Command`/`Response` 末尾追加变体）。

**Spec:** `docs/superpowers/specs/2026-10-29-mq-v2-design.md` §5.5/§6 M1-3；
`docs/adr/0016-mq-v2.md` D2/D5/D6/D13。

## Global Constraints

- `[mq_v2].enabled=false` 时零副作用：本任务只新增状态机代码与命令变体，不注册服务、不切区间。
- apply 必须确定性：不读本地时钟/配置/随机数；新成员 `member_id` 由 apply 内的
  `next_member_seq` 生成（幂等键与命令重放一致）。
- 用户级拒绝（组不存在/代际过期/非成员/未分配）只能是 outcome，不能让 apply 失败。
- 非测试代码不得出现 `unwrap/expect/panic/unimplemented/todo`（`scripts/check-panics.sh`）。
- offset 是 `u64`（proto 用 `int64` 承载）：committed = 下一条待消费，单调不减；
  重复/更旧的 Ack 幂等成功、不回退。
- 分配策略固定 range（Kafka 同口径）：按 topic 排序、成员按 `member_id` 排序，
  `per = ceil(partitions / subscribers)`。
- `Command`/`Response` 变体**只允许末尾追加**（postcard 兼容既有日志）。

## File Structure

| 文件 | 责任 |
|:--|:--|
| `coord-server/src/storage/mq_group.rs`（新建） | 组命令载荷、组/成员/分配/位点表、apply、只读查询、单测 |
| `coord-server/src/storage/mod.rs` | 注册 `pub mod mq_group;` |
| `coord-server/src/raft/type_config.rs` | 末尾追加 `Command::MqGroup`、`Response::MqGroup` 与 Display |
| `coord-server/src/raft/state_machine.rs` | `execute_command` 分派 + 状态机级测试 |
| `docs/production/ops/mq-v2-gate.md` | M1 行证据（M1-3 可复跑命令） |

## Review Focus

- **代际与分配的唯一性**：成员变化/过期必须 `generation++` 并整体重算分配；同一代际内
  同一分区不得出现在两个成员的分配里。
- **Ack 原子性**：`AckBatch` 任一条未分配/代际过期 ⇒ 整批拒绝、一条位点都不写
  （契约明文要求，不得部分提交）。
- **位点不回退**：重复/更旧 Ack 返回当前 committed，不写更小值；`offset == u64::MAX`
  时 `+1` 不得回绕到 0。
- **重放幂等**：同一条 Raft 条目重复 apply 不得产生第二个成员或第二次代际推进。
- **空组与位点保留**：成员全部离开/过期后组进入 Empty，位点仍在（TTL 回收属 M3-6）。
- **快照不丢**：组行 + 位点行走既有 `TABLE_MQ_META` 通道，恢复后 generation/位点一致。

---

### Task 1: 组状态机存储模块（`mq_group.rs`）

**Files:**
- Create: `coord-server/src/storage/mq_group.rs`
- Modify: `coord-server/src/storage/mod.rs:17-18`（加 `pub mod mq_group;`）
- Test: `coord-server/src/storage/mq_group.rs`（模块内 `#[cfg(test)]`）

**Interfaces:**
- Produces（后续任务/ M3 handler 使用）:
  - `MqGroupOp` 枚举：`Join(MqGroupJoin)`、`Heartbeat(MqGroupHeartbeat)`、
    `Leave(MqGroupLeave)`、`Reap(MqGroupReap)`、`Ack(MqGroupAck)`。
  - `MqGroupResult` 枚举：`Joined(MqGroupJoined)`、`Heartbeat(MqGroupHeartbeatReply)`、
    `Left(MqGroupLeft)`、`Reaped(MqGroupReaped)`、`Acked(MqGroupAcked)`、
    `Rejected(MqGroupRejection)`。
  - `MqGroupRejection`：`GroupNotFound`、`StaleGeneration { current_generation }`、
    `NotMember { current_generation }`、`NotAssigned { topic_id, partition }`、
    `Invalid { reason }`。
  - `MvccStorage::<B>::apply_mq_group_op(&self, op: &MqGroupOp, applied: AppliedLogId) -> Result<MqGroupResult>`
  - `MvccStorage::<B>::mq_group(&self, group: &str) -> Result<Option<MqGroupState>>`
  - `MvccStorage::<B>::mq_group_offsets(&self, group: &str) -> Result<Vec<MqGroupOffset>>`
  - `MvccStorage::<B>::mq_group_names(&self, prefix: &str) -> Result<Vec<String>>`
- 命令载荷字段（全部确定性输入）：
  - `MqGroupJoin { group, member_id, topics: Vec<MqGroupTopic>, session_timeout_ms: u32, now_ms: i64 }`
  - `MqGroupTopic { topic: String, topic_id: u64, partitions: u32 }`
  - `MqGroupHeartbeat { group, member_id, generation, now_ms }`
  - `MqGroupLeave { group, member_id, generation, now_ms }`
  - `MqGroupReap { shard_id: u32, now_ms: i64 }`
  - `MqGroupAck { group, member_id, generation, entries: Vec<MqGroupAckEntry>, now_ms }`
  - `MqGroupAckEntry { topic_id: u64, partition: u32, offset: u64 }`

**行为固定（实现时逐条对齐）：**

1. 键编码：组行 `b'G' ++ group`；位点行 `b'O' ++ len(group) as u32 ++ group ++ topic_id(8) ++ partition(4)`。
   分片 apply 水位与最近结果：`b'S' ++ shard_id(8)`（与 `mq_log` 同布局，两者不共享 storage）。
2. `Join`：组不存在则创建；`member_id` 为空 ⇒ 用 `next_member_seq` 生成 `m{seq:016x}`
   （冲突则继续递增）；非空但不存在 ⇒ 以该 id 新建成员。订阅未变且是既有成员 ⇒ 只续期，
   不推进代际；新建/订阅变化 ⇒ `generation++` 并整体重算 range 分配。返回该成员的分配 +
   每条分配的已提交位点（无记录 0）、`session_timeout_ms`、`heartbeat_interval_ms =
   max(session_timeout_ms / 3, 1)`。校验：组名非空且 ≤255 字节、topics 非空、topic 名
   非空、`partitions > 0`、无重名 topic、`session_timeout_ms > 0`；否则 `Invalid`。
3. `Heartbeat`：组不存在 ⇒ `GroupNotFound`；代际不等 ⇒ `StaleGeneration`；
   成员不存在 ⇒ `NotMember`；否则刷新 `last_heartbeat_ms = now_ms`，返回
   `{ generation, rebalance_required: false }`。
4. `Leave`：组不存在 ⇒ `GroupNotFound`；成员不存在 ⇒ `Left { removed: false, generation }`
   （重试幂等）；代际不等 ⇒ `StaleGeneration`；移除成员后 `generation++` 并重算剩余成员分配。
5. `Reap`：扫描本分片全部组；`now_ms >= last_heartbeat_ms + session_timeout_ms` 的成员移除，
   有成员被移除的组 `generation++` 并重算分配（组保留，位点保留，Empty 态）。
   返回 `{ groups_scanned, members_expired, groups_changed }`。
6. `Ack`：组不存在 ⇒ `GroupNotFound`；代际不等 ⇒ `StaleGeneration`；成员不存在 ⇒
   `NotMember`；任一条 `(topic_id, partition)` 不在该成员分配里 ⇒ `NotAssigned` 且整批不写；
   全部通过才写：`committed = max(existing, offset.saturating_add(1))`，只在推进时更新
   `updated_at_ms = now_ms`；按请求顺序逐条返回新 committed（重复条目按顺序累积）。
7. 幂等：`applied.index <= shard.applied_index` ⇒ 返回缓存的 `last_result`（缺失时返回
   `Rejected(Invalid { reason: "replayed group command has no cached result" })`），不写任何状态。
8. 所有成功的写路径在**同一 redb 事务**里写组行/位点行、分片水位与
   `TABLE_META/META_LAST_APPLIED`。

- [ ] **Step 1: 写失败测试**（模块内 `#[cfg(test)]`，用 `RedbBackend` + `TempDir`）
  - `join_assigns_every_partition_to_exactly_one_member`
  - `rejoin_with_the_same_subscription_keeps_the_generation`
  - `subscription_change_bumps_the_generation_and_reassigns`
  - `join_without_member_id_generates_unique_member_ids`
  - `heartbeat_rejects_stale_generation_and_unknown_member`
  - `ack_is_monotonic_and_rejects_unassigned_partitions`
  - `ack_batch_rejects_without_partial_writes`
  - `leave_is_idempotent_and_rebalances`
  - `reap_expires_members_and_transfers_their_partitions`
  - `replay_of_the_same_applied_index_does_not_duplicate_members`
  - `group_state_and_offsets_survive_snapshot_roundtrip`
  - `group_state_is_deterministic_across_two_stores`
- [ ] **Step 2: 跑测试确认失败**
  Run: `cargo test -p coord-server --lib storage::mq_group`
  Expected: FAIL（`mq_group` 模块/类型不存在）
- [ ] **Step 3: 实现模块 + 注册 `mod`**
- [ ] **Step 4: 跑测试确认通过**
  Run: `cargo test -p coord-server --lib storage::mq_group`
  Expected: PASS（12 例）
- [ ] **Step 5: 提交**
  `git commit -m "feat(mq-v2): consumer group state machine (M1-3)"`

### Task 2: Raft 命令/响应接线

**Files:**
- Modify: `coord-server/src/raft/type_config.rs`（`Command`/`Response` 末尾追加 + Display）
- Modify: `coord-server/src/raft/state_machine.rs`（`execute_command` 分派）
- Test: `coord-server/src/raft/state_machine.rs`（模块内测试）

**Interfaces:**
- Consumes: Task 1 的 `MqGroupOp` / `MqGroupResult` / `apply_mq_group_op`。
- Produces: `Command::MqGroup(MqGroupOp)`、`Response::MqGroup { result: MqGroupResult }`。

- [ ] **Step 1: 写失败测试** `test_apply_mq_group_commands_join_ack_and_replay`
  （join → ack → 重复 apply 同 index 返回缓存结果；`event.is_none()`；水位落到
  `get_applied_log_id`）
- [ ] **Step 2: 跑测试确认失败**
  Run: `cargo test -p coord-server --lib test_apply_mq_group_commands`
  Expected: FAIL（变体不存在）
- [ ] **Step 3: 实现变体、Display、分派**（MQ 组命令不写 changelog、不上 Watch，注释说明）
- [ ] **Step 4: 跑测试确认通过**
  Run: `cargo test -p coord-server --lib test_apply_mq_group_commands`
  Expected: PASS
- [ ] **Step 5: 提交** `git commit -m "feat(mq-v2): route group commands through the raft state machine (M1-3)"`

### Task 3: Gate 证据

**Files:**
- Modify: `docs/production/ops/mq-v2-gate.md` §1（M1 行证据列）

- [ ] **Step 1: 更新 M1 行**为「M1-1/M1-2/M1-3/M1-4 已落地」并列出可复跑命令：
  `cargo test -p coord-server --lib storage::mq_group`、
  `cargo test -p coord-server --lib test_apply_mq_group_commands`、
  `cargo test -p coord-server --lib carve_mq`、`cargo test -p coord --bins derive_`、
  `check-panics` 0。
- [ ] **Step 2: 文档引用检查** Run: `bash scripts/check-doc-refs.sh` Expected: 0 悬空
- [ ] **Step 3: 提交** `git commit -m "docs(mq-v2): record M1-3 gate evidence"`

### Task 4: 本地 CI 门禁（提 PR 前必须全绿）

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --workspace --lib --bins -- -D warnings`
- [ ] `bash scripts/check-panics.sh`
- [ ] `bash scripts/check-doc-refs.sh`
- [ ] `cargo audit`、`cargo deny check`
- [ ] `cargo test -p coord-server --lib storage::mq_group` 与 `--lib raft::state_machine`
- [ ] 推送分支 + `gh pr create --base main --fill`，`gh pr checks --watch` 全绿后
      `gh pr merge --delete-branch --merge`
