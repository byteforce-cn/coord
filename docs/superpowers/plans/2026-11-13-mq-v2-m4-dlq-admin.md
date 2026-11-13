# MQ v2 M4 — DLQ 与管理面实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** 交付 `coord.mq.v2` 的死信与 topic 管理面：`MoveToDlq`/`PollDlq`、
`DescribeTopic`/`ListTopics`/`UpdateTopic`、`DeleteTopic`/`GetTopicDeletion`（异步回收 +
组位点清理），并补齐能力/审计证据。

**Architecture:** 复用 M1-2 的 DLQ 表（`apply_mq_move_to_dlq`/`mq_dlq_records`）与 M1-4 的
topic 表（`MqTopicOp`）。新增 `coord-server/src/mq_v2/admin.rs`（管理面读/更新/删除）与
`coord-server/src/mq_v2/dlq.rs`（死信读写）；删除回收并入 `mq_v2_reaper_pass`，
需要两个**末尾追加**的命令变体：`Command::MqDropPartition`（物理回收分区，含段/幂等/
DLQ）与 `MqTopicOp::DeletionProgress`（推进删除进度），以及 `MqGroupOp::DropTopic`
（清理被删除 topic 的组位点）。

**Tech Stack:** Rust 2021、tonic 0.14、openraft（append-only 变体）、redb。

**Spec:** `docs/superpowers/specs/2026-10-29-mq-v2-design.md` §5.6/§6 M4；
`docs/adr/0016-mq-v2.md` D7/D9/D10/D11。

## Global Constraints

- 关闭态零副作用：所有 handler 先 `gate(...)`（Manage = `Topic`，DLQ = `Dlq`），
  reaper 只在 `[mq_v2].enabled=true` 时存在（既有约束）。
- `DeleteTopic` 返回后旧 topic_id 的读写**立即** `TOPIC_NOT_FOUND`（不等回收）。
- `MoveToDlq` 幂等：同 `(topic_id, partition, offset)` 重复调用返回首次 `dlq_offset`。
- `Poll` 跳过 tombstone（M3-1 已实现）；`PollDlq` 只读 DLQ，带 `reason`/`detail`。
- 命令变体**只允许末尾追加**；`MqTopicOp`/`MqGroupOp` 的新变体同样追加。
- 用户级拒绝只按码分支（`TOPIC_NOT_FOUND`/`TOPIC_EXISTS`/`TOPIC_EPOCH_STALE`/
  `OFFSET_OUT_OF_RANGE`/`INVALID_ARGUMENT`）。
- 回收失败不得让删除卡死：失败的 partition 留在 `pending_partitions`，下一周期重试；
  `GetTopicDeletion` 必须诚实报告 `state=Failed/Pending`，不得提前写 Done。

## 语义决定

1. **`UpdateTopic` 的 0 值**：配置字段 `0` = 保持当前值（proto 明文）；`retention_secs`
   与 `dlq_retention_secs` 的非 0 值必须在 1h–30d；`max_message_bytes` ≤ 8MiB；
   `segment_bytes` 1MiB–1GiB。`expected_topic_epoch` 非 0 时做 CAS。
2. **`DescribeTopic` 的一致性**：分区的 `earliest/latest/log_bytes` 读**本节点**分片副本
   （可滞后，观测用途）；`leader_addr` 来自 raft metrics + `grpc_addr_of`。
3. **`DeleteTopic` 的 `delete_id`**：用请求的 `now_ms`（i64 → u64）作为幂等键；同
   `delete_id` 重放返回首次结果（M1-4 已实现）。
4. **物理回收**：`MqDropPartition` 删除该分区的全部记录/段/幂等索引/DLQ 行与
   `MqPartitionMeta`，并从 `MqTopicUsage` 扣除；apply 幂等（重复 apply 返回 0 计数）。
5. **组位点清理**：`MqGroupOp::DropTopic { shard_id, topic_id }` 删除该分片内所有组里
   `topic_id` 的位点行，返回删除条数；空的组保留（成员可能还在，只是 topic 被删）。
6. **删除进度**：`MqTopicOp::DeletionProgress { delete_id, partition }` 从
   `pending_partitions` 移除一个分区；清空时 `state=Done`。回收器对每个 pending 分区
   依次做 `MqDropPartition`（日志分片 Leader）→ 所有 group shard `DropTopic`（每个
   shard 一次，代价可接受）→ `DeletionProgress`；任一步失败则整轮跳过该分区。
7. **`ListTopics` 分页**：`page_size` 默认 100（上限 1000），`page_token` = 上一页最后
   一个 topic 名（与 `ListGroups` 同口径）。

## Review Focus

- **删除后复活**：同名重建得到新 `topic_id`；回收必须按旧 `topic_id` 隔离，不能删掉
  新 topic 的数据。
- **幂等**：`MoveToDlq` 重复、`DeleteTopic` 重复、`MqDropPartition` 重放。
- **配额记账**：DLQ 复制后 `dlq_bytes` 增加、主日志保留期内双份计费；回收后归零。
- **未启用时**：`UpdateTopic`/`DeleteTopic` 在 `enabled=false` 时必须
  `FAILED_PRECONDITION`（不是 UNIMPLEMENTED）。
- **审计/能力**：缺能力 → `PERMISSION_DENIED`（interceptor 既有行为）；`Dlq` 能力未
  交付时 DLQ RPC 仍是 `FAILED_PRECONDITION`。

---

### Task 1: DLQ（M4-1）

**Files:**
- Create: `coord-server/src/mq_v2/dlq.rs`
- Modify: `coord-server/src/mq_v2/services.rs`（`move_to_dlq`/`poll_dlq`）
- Modify: `coord/src/main.rs`（`MqV2Capability::Dlq`）
- Test: `coord-server/src/mq_v2/dlq.rs`、`coord/tests/mq_v2_m4_dlq_test.rs`

**Interfaces:**
- `CoordNode::mq_move_to_dlq(&self, request) -> Result<mq_v2::MoveToDlqResponse, Status>`
- `CoordNode::mq_poll_dlq(&self, request) -> Result<mq_v2::PollDlqResponse, Status>`

- [ ] **Step 1: 失败测试**：moved / already_moved / missing→`OFFSET_OUT_OF_RANGE`；
  `PollDlq` 返回原 offset + reason/detail、`next_dlq_offset` 单调；`Poll` 不再返回该条。
- [ ] **Step 2: 实现**（复用 `mq_partition_target` + `mq_ensure_shard_leader`；DLQ 读
  本地分片副本）。
- [ ] **Step 3: gate 证据**（M4-1 行）。

### Task 2: 管理读面与更新（M4-3）

**Files:**
- Create: `coord-server/src/mq_v2/admin.rs`
- Modify: `coord-server/src/mq_v2/services.rs`（`describe_topic`/`list_topics`/`update_topic`）
- Test: `coord-server/src/mq_v2/admin.rs`、`coord/tests/mq_v2_m4_admin_test.rs`

- [ ] **Step 1: 失败测试**：Describe 的 config/epoch/partitions（leader/earliest/latest/
  log_bytes）；List 分页/前缀；Update 的 0=保持、CAS 过期、非法值、未知 topic。
- [ ] **Step 2: 实现** + `UpdateTopic` 展开（0 = 当前值）与 `MqTopicOp::Update` 映射。
- [ ] **Step 3: gate 证据**（M4-3 行）。

### Task 3: DeleteTopic / GetTopicDeletion + 异步回收（M4-2）

**Files:**
- Modify: `coord-server/src/storage/mq_log.rs`（`Command::MqDropPartition` apply）
- Modify: `coord-server/src/raft/type_config.rs`（变体 + Display + label）
- Modify: `coord-server/src/storage/mq_topic.rs`（`MqTopicOp::DeletionProgress` apply）
- Modify: `coord-server/src/storage/mq_group.rs`（`MqGroupOp::DropTopic` apply）
- Modify: `coord-server/src/mq_v2/admin.rs`/`reaper.rs`/`services.rs`
- Test: 同上 + `coord/tests/mq_v2_m4_delete_test.rs`

- [ ] **Step 1: 失败测试**：删除后立即 NOT_FOUND；回收推进到 Done 且段/DLQ/幂等/组位点
  全部清空；同名重建不复活旧消息、旧 topic_id 读写 NOT_FOUND；replay 幂等。
- [ ] **Step 2: 实现**（三个变体 + 回收循环 + 进度）。
- [ ] **Step 3: gate 证据**（M4-2 行）。

### Task 4: 能力/审计与 gate（M4-4）

- [ ] `cargo test -p coord --test mq_v2_g0_test`（关闭/未交付/已交付三态仍成立）。
- [ ] 审计：管理面 RPC 走既有 audit 事件（若缺则补事件类型 + 断言）。
- [ ] gate 文档 §2d（M4 分项）+ 设计文档头注 M0–M4。
- [ ] 本地 CI 门禁 + PR。
