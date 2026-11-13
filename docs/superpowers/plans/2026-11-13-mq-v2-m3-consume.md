# MQ v2 M3 — Poll/Ack 与消费组实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** 交付 `coord.mq.v2` 的读路径与消费组协调：`Poll`（长轮询/边界/不静默 clamp）、
`Ack`/`AckBatch`（代际 fencing、位点单调、整批原子）、Join/Heartbeat/Leave/
GetCommittedOffsets/DescribeGroup/ListGroups（range 分配 + 会话过期回收），以及
Rust consumer helper、best-effort `Subscribe` 与 lag/组 TTL 观测。

**Architecture:** 复用 M1-3 的组状态机（`storage::mq_group`，命令载荷已携带全部非确定性
输入）与 M1-2 的段日志读面（`mq_records`/`mq_partition_bounds`）。新增
`coord-server/src/mq_v2/consume.rs`（Poll 读面 + offset 边界 trailer）、
`coord-server/src/mq_v2/groups.rs`（组命令 → group shard Raft 提案 + 只读面）；
批次唤醒复用写路径的 `ShardBatcher`（append 提交后 `Notify` 唤醒长轮询）；
组回收并入已有的 `mq_v2_reaper_pass` 周期任务。

**Tech Stack:** Rust 2021、tonic 0.14、openraft（`Command::MqGroup` 已存在）、redb、
tokio（`Notify` + `select!`）。

**Spec:** `docs/superpowers/specs/2026-10-29-mq-v2-design.md` §5.4/§5.5/§6 M3；
`docs/adr/0016-mq-v2.md` D5/D6/D13。

## Global Constraints

- `[mq_v2].enabled=false` 时零副作用：所有 handler 先过 `gate(Consume)`；关闭态不建
  Notify/任务、不读状态。
- `Poll`/`Ack` 只承诺**已提交**数据：`Poll` 读分片 Leader 的已 apply 状态；`Ack`
  必须经 group shard 的 Raft 提交后返回（`client_write` 返回即满足）。
- **不静默 clamp**：显式 `start_offset < earliest` ⇒ `OFFSET_OUT_OF_RANGE` +
  trailer `coord-earliest-offset` / `coord-latest-offset`；`Poll` 不推进已提交位点。
- **代际 fencing 在 apply 内**：handler 不得先在内存判断 member/generation/assignment。
- **整批原子**：`AckBatch`（1–256）任一条未分配/代际过期 ⇒ 整批不写（M1-3 已实现，
  handler 只要不拆批）。
- **一个分区一代际一个成员**：由 M1-3 的 range 分配保证；handler 只做路由与映射。
- 组 shard = `fnv1a64(group) % [mq_v2].group_shards`（**跨版本稳定**；不得换成
  `DefaultHasher`，否则重启后组状态会漂到另一个分片）。
- 非测试代码不得出现 `unwrap/expect/panic/unimplemented/todo`；`Command`/`Response`
  变体只在末尾追加（本计划只可能给 `MqGroupOp` 追加 `Gc`）。

## 语义决定（实现前先读）

1. **`Poll.start_offset` 的三种取值**（proto `optional int64`）：
   - 未设置（`None`）⇒ 从**当前最早可用** offset 开始（不报错、不 clamp：调用方没有
     指定任何东西；SDK 总是显式传 committed/reset 后的值）；
   - 显式 `Some(x)` 且 `x < earliest` ⇒ `OFFSET_OUT_OF_RANGE`（trailer 带
     earliest/latest）；
   - 显式 `Some(x)` 且 `x >= latest` ⇒ 空批；有 `wait_ms` 时等到超时。
2. **长轮询窗口**：`wait_ms` 上限 = `[mq_v2].long_poll_max_ms`（契约 30s）；超过上限
   按上限截断（不是错误）。等待用分片 `Notify`（append 提交后唤醒）+ 200ms 兜底轮询
   （避免丢唤醒导致整段等待）。超时返回空批，`next_offset` = 请求起点。
3. **`max_bytes` 与单条消息**：按整条截断；若**第一条可见消息**就超过 `max_bytes`，
   仍然返回它（否则该消息永远无法被消费）；其余消息在超限处停止。
4. **tombstone**：`Poll` 跳过 `moved_to_dlq.is_some()` 的记录（DLQ 消息由 `PollDlq`
   读；M4-1），但 `next_offset` 仍跨过它们。
5. **`DescribeGroup.state`**：v2.0 只有 `EMPTY`（无成员，位点保留）与 `STABLE`；
   `PREPARING_REBALANCE`/`DEAD` 不产生（apply 内直接重算分配、组 TTL 才回收）。
6. **组 TTL**：无成员且位点 `updated_at_ms` 早于 `now_ms - GROUP_OFFSET_TTL_SECS`
   （常量 7d）的组在 reaper 的 `MqGroupGc` 中回收；回收是幂等的。
7. **`Subscribe` 是 best-effort**：有界队列（64）满时丢弃并计数，不推进位点、不重投；
   显式 `start_offset < earliest` ⇒ 流以 `OFFSET_OUT_OF_RANGE` 结束。

## Review Focus

- **未提交即可见**：`Poll` 是否可能读到一个尚未 apply 的 offset？（只读 `mvcc`，禁止读
  Leader 内存缓冲）
- **Ack 越权**：Ack 一个未分配给该成员的分区、或旧代际，是否在 apply 内被拒（不是
  handler 先查后写）？
- **offset 回退**：Ack 更小 offset 是否保持 committed 不回退？`u64::MAX` 是否回绕？
- **长轮询唤醒丢失**：append 与 Poll 竞争时，Poll 是否可能睡满等待窗口却漏掉新消息？
- **组分片迁移**：改名/重启后同一组是否解析到同一 group shard（哈希稳定性）？
- **空批语义**：`next_offset` 在空批时是否等于请求起点（不是 0，也不是 latest）？

---

### Task 1: Poll 读面与长轮询（M3-1）

**Files:**
- Create: `coord-server/src/mq_v2/consume.rs`
- Modify: `coord-server/src/mq_v2/publish.rs`（`ShardBatcher` 增加 `Notify`；flush 成功后
  `notify_waiters`；`MqPublishTarget` 重命名为 `MqPartitionTarget`，供读路径复用）
- Modify: `coord-server/src/mq_v2/services.rs`（`poll` handler）
- Modify: `coord-server/src/mq_v2/mod.rs`（`pub mod consume;`、trailer 常量）
- Modify: `coord-server/src/metrics.rs`（poll 计数器）
- Test: `coord-server/src/mq_v2/consume.rs`、`coord/tests/mq_v2_m3_consume_test.rs`

**Interfaces:**
- Produces:
  - `pub const EARLIEST_OFFSET_METADATA_KEY: &str = "coord-earliest-offset";`
  - `pub const LATEST_OFFSET_METADATA_KEY: &str = "coord-latest-offset";`
  - `CoordNode::mq_poll(&self, request: mq_v2::PollRequest) -> Result<mq_v2::PollResponse, Status>`
  - `ShardBatcher::append_notify(&self) -> tokio::sync::futures::Notified<'_>`（或
    `async fn wait_append(&self, timeout: Duration)`）
- Consumes: `MqV2RuntimeConfig::{poll_max_messages, poll_max_bytes, long_poll_max_ms}`、
  `MvccStorage::{mq_records, mq_partition_bounds}`、`MqStoredRecord::is_visible`。

- [ ] **Step 1: 写失败测试**
  - `poll_returns_committed_messages_and_next_offset`（3 条 → 前 2 条 + `next_offset=2`）；
  - `poll_skips_dlq_tombstones_but_advances_next_offset`；
  - `poll_rejects_an_explicit_offset_below_earliest`（trailer 带 earliest/latest）；
  - `poll_without_start_offset_starts_at_the_earliest_available`（保留回收后仍可读）；
  - `poll_waits_for_an_append_when_the_log_is_empty`（wait_ms=500，另一任务 50ms 后
    publish → Poll 返回该消息，且总耗时 < 400ms）；
  - `poll_times_out_with_an_empty_batch`（wait_ms=100，无写入 → 空批 +
    `next_offset == start`）；
  - `poll_caps_messages_and_bytes`（max_messages=2；max_bytes 只够 1 条 → 1 条）。
- [ ] **Step 2: 跑测试确认失败**
  Run: `cargo test -p coord-server --lib mq_v2::consume`
- [ ] **Step 3: 实现 Poll + Notify 唤醒 + 指标**
- [ ] **Step 4: 跑测试确认通过**（同一命令）
- [ ] **Step 5: 提交** `git commit -m "feat(mq-v2): poll with offsets, long-poll and boundaries (M3-1)"`

### Task 2: Ack / AckBatch 与组分片路由（M3-2）

**Files:**
- Create: `coord-server/src/mq_v2/groups.rs`（组 shard 路由 + 命令提案 + 结果映射）
- Modify: `coord-server/src/mq_v2/services.rs`（`ack`/`ack_batch` handler）
- Test: `coord-server/src/mq_v2/groups.rs`、`coord/tests/mq_v2_m3_group_test.rs`

**Interfaces:**
- Produces:
  - `pub fn mq_group_shard(group: &str, group_shards: u32) -> u32`（FNV-1a 64 取模）
  - `CoordNode::mq_group_runtime(&self, group: &str) -> Result<(u32, Arc<RegionRuntime>), Status>`
  - `CoordNode::mq_group_ensure_leader(&self, group) -> Result<Arc<RegionRuntime>, Status>`
  - `CoordNode::mq_propose_group(&self, op: MqGroupOp) -> Result<MqGroupResult, Status>`
  - `fn group_rejection_status(&MqGroupRejection) -> Status`
  - `CoordNode::mq_resolve_ack_entry(&self, table, group, entry) -> Result<MqGroupAckEntry, Status>`
    （topic/epoch/partition → topic_id）
- Consumes: `Command::MqGroup(MqGroupOp::Ack)`、`MqGroupResult::Acked`、`MqGroupRejection`。

- [ ] **Step 1: 写失败测试**
  - `group_shard_is_stable_across_restarts`（同一名字多次/不同进程常量值一致）；
  - `ack_advances_committed_and_is_monotonic`（ack 0 → 1；ack 0 再次 → 仍 1）；
  - `ack_batch_is_atomic_when_one_partition_is_unassigned`（整批拒绝、
    `NOT_GROUP_MEMBER`，位点不变）；
  - `ack_rejects_a_stale_generation`（`GROUP_EPOCH_STALE`）；
  - `ack_maps_unknown_topic_and_partition_to_stable_codes`。
- [ ] **Step 2: 跑测试确认失败**
  Run: `cargo test -p coord-server --lib mq_v2::groups`
- [ ] **Step 3: 实现路由/提案/映射**
- [ ] **Step 4: 跑测试确认通过**
- [ ] **Step 5: 提交** `git commit -m "feat(mq-v2): ack/ack_batch through the group shard (M3-2)"`

### Task 3: 组协调（Join/Heartbeat/Leave/GetCommittedOffsets/Describe/List）+ reaper（M3-3）

**Files:**
- Modify: `coord-server/src/mq_v2/groups.rs`（其余 handler 逻辑）
- Modify: `coord-server/src/mq_v2/services.rs`（6 个 handler）
- Modify: `coord-server/src/mq_v2/reaper.rs`（组会话过期 `MqGroupOp::Reap` + 组 TTL
  `MqGroupOp::Gc`）
- Modify: `coord-server/src/storage/mq_group.rs`（追加 `MqGroupOp::Gc` + apply）
- Modify: `coord-server/src/metrics.rs`（组/恢复计数器）
- Test: 上述文件 + `coord/tests/mq_v2_m3_group_test.rs`

**Interfaces:**
- Produces:
  - `MqGroupOpGc { shard_id: u32, now_ms: i64, offset_ttl_ms: u64 }` →
    `MqGroupResult::Gced(MqGroupGcResult { groups_removed, offsets_removed })`
  - handler：`join_group`/`heartbeat`/`leave_group`/`get_committed_offsets`/
    `describe_group`/`list_groups`
- 语义：Join 的 `topics` 必须已存在（fail-closed，`TOPIC_NOT_FOUND`）；`session_timeout_ms=0`
  ⇒ `[mq_v2].group_session_timeout_ms`；范围 1000–300000 ms；`ListGroups` 逐 group shard
  合并排序，`page_size` 默认 100（上限 1000），`page_token` = 上一页最后一个组名。

- [ ] **Step 1: 写失败测试**
  - `join_assigns_partitions_and_returns_committed_offsets`；
  - `rejoin_with_the_same_subscription_keeps_the_generation`（端到端）；
  - `join_rejects_unknown_topics`（`TOPIC_NOT_FOUND`）；
  - `heartbeat_rejects_stale_generation`、`leave_is_idempotent`；
  - `expired_members_are_reaped_and_partitions_transfer`（reaper pass 后另一成员拿到分区）；
  - `group_ttl_gc_removes_empty_groups_and_offsets`；
  - `describe_group_reports_lag_and_members`、`list_groups_paginates`；
  - `get_committed_offsets_returns_uncommitted_as_zero`。
- [ ] **Step 2: 跑测试确认失败**
- [ ] **Step 3: 实现 + `main.rs` 组 reaper 周期（5s ticker，与保留回收同一任务）**
- [ ] **Step 4: 跑测试确认通过**
- [ ] **Step 5: 提交** `git commit -m "feat(mq-v2): consumer group coordination and reapers (M3-3)"`

### Task 4: Rust consumer helper（M3-4）

**Files:**
- Create: `coord-client/src/mq_consumer.rs`
- Modify: `coord-client/src/mq_v2.rs`（`join_group`/`heartbeat`/`leave_group`/`ack`/
  `ack_batch`/`get_committed_offsets`/`poll`；组级 Leader 缓存 `group → addr`）
- Test: `coord-client/src/mq_consumer.rs`（纯逻辑）+ `coord/tests/mq_v2_m3_cluster_test.rs`

- [ ] **Step 1: 写失败测试**（reset policy 选择、再平衡触发、位点缓存）
- [ ] **Step 2: 实现**：`MqConsumer::{join, poll, ack, close}`；后台 heartbeat（间隔取
  JoinGroup 的 `heartbeat_interval_ms`）；`GROUP_EPOCH_STALE`/`REBALANCE_IN_PROGRESS`
  ⇒ 重新 Join；`OFFSET_OUT_OF_RANGE` ⇒ `ResetPolicy::{Earliest,Latest,Fail}`。
- [ ] **Step 3: 三节点 e2e**：两个消费者分片消费、kill 一个成员后分区转移且不重复提交。
- [ ] **Step 4: 提交** `git commit -m "feat(mq-v2): rust consumer helper with rebalance handling (M3-4)"`

### Task 5: Subscribe（best-effort）与 lag/组 TTL 观测（M3-5/M3-6）

**Files:**
- Modify: `coord-server/src/mq_v2/consume.rs`（订阅流）+ `services.rs`
- Modify: `coord-server/src/metrics.rs`
- Test: `coord/tests/mq_v2_m3_consume_test.rs`

- [ ] **Step 1: 写失败测试**（背压丢消息计数、不推进位点、`OFFSET_OUT_OF_RANGE` 结束流）
- [ ] **Step 2: 实现**（有界 64 队列；`start_offset=0` ⇒ latest）
- [ ] **Step 3: lag/组指标断言**
- [ ] **Step 4: gate 文档 §2c（M3 分项）+ 提交** `docs(mq-v2): record M3 gate evidence`

### Task 6: 本地 CI 门禁与 PR

- [ ] fmt/clippy/check-panics/check-doc-refs/audit/deny + 相关测试套件
- [ ] PR：C = Task 1–3（服务端读路径与组），D = Task 4–5（客户端/订阅/证据）
