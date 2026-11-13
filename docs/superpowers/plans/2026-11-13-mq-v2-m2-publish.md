# MQ v2 M2 — Publish 写路径实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** 落地 `coord.mq.v2` 的写路径：Publish/PublishBatch 经分片 Leader 批量提交 Raft，
apply 内分配 offset / 幂等去重 / 配额与大小整批拒绝，并提供 Rust 客户端、Agent v2 代理、
指标与基准；M0–M2 gate 证据可复跑。

**Architecture:** 新增 `coord-server/src/mq_v2/publish.rs`（写路径：topic 元数据 →
partition → 日志分片 Region → 分片 Leader 判定 → 每分片 FIFO batcher → 一条
`Command::MqAppendBatch` → apply 结果按请求回填）与 `coord-server/src/mq_v2/topics.rs`
（M2 需要的 `CreateTopic`/`GetTopicLeader` 接线 + topic 配置展开）。M1 已有的
`Storage::apply_mq_append_batch` 是唯一的写入语义实现（offset/幂等/配额/段封口），
本计划**不改**它的语义，只补 handler 侧的路由、批量、错误码与观测面。

**Tech Stack:** Rust 2021、tonic 0.14、openraft（`Command`/`Response` 末尾已有变体）、
redb + postcard、tokio（`oneshot` + `Mutex` 串行化）。

**Spec:** `docs/superpowers/specs/2026-10-29-mq-v2-design.md` §5.3/§6 M2；
`docs/adr/0016-mq-v2.md` D4/D7/D8/D10/D13；gate 证据单一归属
`docs/production/ops/mq-v2-gate.md`。

## Global Constraints

- `[mq_v2].enabled=false` 时零副作用：不建 batcher、不读 topic 表、不提案；服务壳仍
  一律 `FAILED_PRECONDITION`（ADR-0016 D11）。
- **响应即已提交**：`Publish` 返回前必须完成 Raft 多数派提交 + apply（`client_write`
  返回即满足）；不得读 Leader 本地未提交缓冲。
- **单分区原子**：`PublishBatch` 的一个分区对应**一个** `MqAppendRequest`；apply 内
  任一条超限/超配额 ⇒ 整个请求不写（无部分提交）。
- offset 由 apply 分配（`u64`，proto 用 `int64` 承载）；handler 不得自己算 offset，
  不得 clamp/复用。
- 用户级拒绝只按错误码分支：`TOPIC_NOT_FOUND` / `PARTITION_NOT_FOUND` /
  `TOPIC_EPOCH_STALE` / `MESSAGE_TOO_LARGE` / `QUOTA_EXCEEDED` / `NOT_LEADER`
  （trailer `x-coord-error-code`，非 Leader 另带 `coord-leader-hint`）。
- 非测试代码不得出现 `unwrap/expect/panic/unimplemented/todo`（`scripts/check-panics.sh`）。
- `Command`/`Response` 变体**只在末尾追加**；本计划预期无需新增变体。
- v1 MQ 行为字节级不变；v2 集群不服务 v1 MQ，Agent 在 `data_plane="v2"` 下只代理
  `coord.mq.v2`。
- 临时文件一律放 `/tmp`（本仓 AGENTS.md）。

## 语义决定（实现前先读，均需逐字落进代码注释）

1. **批量窗口**：D4 的「最多 64 条或 2 ms，先到先发」按「先到先发」落地——**不引入
   人为 linger**。同分片写请求经一个 `tokio::sync::Mutex`（flush 锁）串行化：持锁者
   立即取走队列里已有的请求（≤64 条）合成一条 `MqAppendBatch`；Raft 往返期间到达的
   请求自然积累成下一批。上限 64 条 = 契约 `PublishBatch` 上限；批深度由指标
   `coord_mq_v2_append_batches_total` / `coord_mq_v2_append_requests_total` 取证。
2. **CreateTopic 的 0 值口径**（契约 + `config.example.toml` 的合解读法）：
   `retention_secs=0` → `[mq_v2].default_retention_secs`；`max_message_bytes=0` →
   `[mq_v2].max_message_bytes`；`segment_bytes=0` → `[mq_v2].segment_bytes`；
   `dlq_retention_secs=0` → 展开后的 `retention_secs`；**`max_bytes`**：请求**未带**
   `config` 时 → `[mq_v2].max_topic_bytes`（config.example：「topic 默认容量配额」），
   带了 `config` 且 `max_bytes=0` → 0 = 不限（proto：「0 = 不限」）。
3. **能力面**：M2 交付 `MqV2Capability::Topic` 的 `CreateTopic`/`GetTopicLeader` 与
   `MqV2Capability::Publish` 的 `Publish`/`PublishBatch`；其余 manage RPC 通过门禁后
   落 `UNIMPLEMENTED`（M4-2/M4-3 接线）。`GetTopicLeader` 门禁用 `Topic`（它是
   topic 级发现面，与 `Publish` 客户端配套）。
4. **分片路由**：partition → `topic.shard_of_partition[partition]` → key
   `mq_log_shard_prefix(shard_id)`（`/_mq/log/<id>/`）→ `RegionManager::route_runtime`
   → 该 Region 的 `CoordRaft`。无 `region_manager`（未装配 multi-raft）⇒
   `FAILED_PRECONDITION`（装配问题，不是网络故障）。
5. **Leader 判定**：先 `runtime.raft.current_leader().await == Some(self.node_id)` 快速
   返回 `NOT_LEADER` + hint；与真实提案之间的领导权变化由 `client_write` 的
   `ForwardToLeader` 兜底（同一错误码 + hint）。
6. **幂等窗口** = topic 保留期：M1 的 `MqTruncate` 回收段时一并剪掉指纹 < earliest 的
   幂等索引，窗口外同键重新使用。M2 只写证据，不改实现。

## Review Focus

- **未提交即可见**：handler 是否在 `client_write` 返回前就去读了本地分片状态？（禁止）
- **部分提交**：`PublishBatch` 的一条超限/超配额，是否在 `next_offset` 上留下痕迹？
- **跨分区/跨分片串行化**：batcher 必须按 `shard_id` 分桶；不同分片不得互相阻塞。
- **Follower 上的写入**：必须返回 `UNAVAILABLE + NOT_LEADER + hint`，不能排队等待或
  静默重试到别的分片。
- **关闭态副作用**：`enabled=false` 时不得因本计划的装配（batcher/metrics/topic 读取）
  产生任何状态。
- **offset 溢出/回绕**：`u64` → proto `int64` 转换失败要显式报错，不得回绕成负数。

---

### Task 1: `CreateTopic` / `GetTopicLeader` 接线（M2 前置）

**Files:**
- Create: `coord-server/src/mq_v2/topics.rs`
- Modify: `coord-server/src/mq_v2/mod.rs`（`pub mod topics;` + `pub(crate) fn now_ms()`）
- Modify: `coord-server/src/mq_v2/services.rs`（两个 handler 从 `gated!` 改为真实接线）
- Modify: `coord/src/main.rs`（`with_implemented([Topic, Publish])`）
- Test: `coord-server/src/mq_v2/topics.rs`（模块内 `#[cfg(test)]`）

**Interfaces:**
- Produces（后续任务/客户端使用）:
  - `CoordNode::mq_topic_table(&self) -> Result<MqTopicTable, tonic::Status>`
  - `CoordNode::mq_expand_topic_config(&self, requested: Option<mq_v2::TopicConfig>) -> Result<MqTopicConfig, tonic::Status>`
  - `CoordNode::mq_topic_create_command(&self, req: &mq_v2::CreateTopicRequest) -> Result<crate::raft::type_config::Command, tonic::Status>`
  - `CoordNode::mq_partition_leaders(&self, meta: &MqTopicMeta) -> Vec<mq_v2::PartitionLeader>`
  - `fn create_topic_response(outcome: MqTopicOutcome) -> Result<mq_v2::CreateTopicResponse, tonic::Status>`（模块内 `pub(crate)`）
- Consumes: `MvccStorage::mq_topic_table`、`MqTopicOp::Create`、`mq_log_shard_prefix`、
  `CoordRaft`/`client_write_with_timeout`。

- [ ] **Step 1: 写失败测试**（`topics.rs` 模块内；用 `RedbBackend` + `TempDir` 建
  `CoordNode`，topic 表用 `apply_mq_topic_op` 直接种入，不需要 raft）
  - `expand_topic_config_applies_the_documented_defaults`：`None` →
    `max_bytes == 默认 max_topic_bytes`、`retention_secs == default_retention_secs`、
    `max_message_bytes == max_message_bytes`、`segment_bytes == segment_bytes`、
    `dlq_retention_secs == retention_secs`。
  - `expand_topic_config_keeps_an_explicit_zero_max_bytes`：`Some(TopicConfig{max_bytes: 0, ..})`
    → `max_bytes == 0`（不限），其余字段仍展开。
  - `expand_topic_config_rejects_out_of_range_values`：`retention_secs=1`、
    `max_message_bytes=9<<20`、`segment_bytes=0` 之外的每项都映射
    `INVALID_ARGUMENT`（`0001` 断言 `error_code_of` == `INVALID_ARGUMENT`）。
  - `create_topic_response_maps_outcomes_to_stable_codes`：`Created` → 响应字段逐字；
    `Exists` → `ALREADY_EXISTS` + `TOPIC_EXISTS`；`Invalid` → `INVALID_ARGUMENT`。
  - `partition_leaders_report_the_shard_of_each_partition`：用
    `RegionManager::new` + `register_region`（**不**装配 runtime）登记切出的分片区间，
    `mq_partition_leaders` 返回的 `shard_id` 与 `shard_of_partition` 一致、
    `leader_addr` 为空串（选举中）。
- [ ] **Step 2: 跑测试确认失败**
  Run: `cargo test -p coord-server --lib mq_v2::topics`
  Expected: FAIL（模块/类型不存在）
- [ ] **Step 3: 实现模块 + 接线**
  - `mq_topic_create_command`：校验 topic 名非空、`partitions > 0`（其余交给 state
    machine 的 `Invalid`），展开配置，`now_ms()`，`log_shards`/`max_partitions_per_shard`
    取 `self.mq_v2().config()`。
  - handler：`gate(Topic)` → `mq_topic_create_command` → `self.raft`（None ⇒
    `FAILED_PRECONDITION`，消息点名 multi-raft 装配）→
    `client_write_with_timeout(raft, cmd, 0)` → 取 `Response::MqTopic { outcome }` →
    `create_topic_response`。
  - `get_topic_leader` handler：`gate(Topic)` → `mq_topic_table().topic(name)`（无 ⇒
    `NOT_FOUND` + `TOPIC_NOT_FOUND`）→ `mq_partition_leaders`。分区边界用分片本地副本
    `MvccStorage::mq_partition_bounds`（可滞后，字段是观测用途）；无 runtime ⇒ 0/空。
- [ ] **Step 4: 跑测试确认通过**
  Run: `cargo test -p coord-server --lib mq_v2::topics`
  Expected: PASS（5 例）
- [ ] **Step 5: 提交**
  `git commit -m "feat(mq-v2): wire CreateTopic and GetTopicLeader (M2-1)"`

### Task 2: 写路径核心（batcher + Publish/PublishBatch）

**Files:**
- Create: `coord-server/src/mq_v2/publish.rs`
- Modify: `coord-server/src/mq_v2/mod.rs`（`pub mod publish;`）
- Modify: `coord-server/src/server/mod.rs`（`CoordNode` 新增
  `mq_v2_batchers: Arc<crate::mq_v2::publish::PublishBatchers>`，`new()` 初始化空表；
  `pub(crate) fn mq_v2_batchers(&self) -> &Arc<...>`）
- Modify: `coord-server/src/mq_v2/services.rs`（`publish`/`publish_batch` 接线）
- Modify: `coord-server/src/metrics.rs`（`mq_v2_append_batches_total`、
  `mq_v2_append_requests_total` 两个计数器 + `render` + 记录函数）
- Test: `coord-server/src/mq_v2/publish.rs`、`coord-server/src/metrics.rs`

**Interfaces:**
- Produces:
  - `pub(crate) struct MqPublishTarget { topic_id: u64, partition: u32, shard_id: u32, region_id: RegionId, runtime: Arc<RegionRuntime>, max_message_bytes: u32, max_bytes: u64, segment_bytes: u64 }`
  - `CoordNode::mq_publish_target(&self, topic: &str, partition: i32, topic_epoch: u64) -> Result<MqPublishTarget, Status>`
  - `CoordNode::mq_append(&self, target: &MqPublishTarget, entries: Vec<MqAppendEntry>) -> Result<MqAppended, Status>`
  - `pub(crate) struct PublishBatchers`：`shard(&self, shard_id: u32) -> Arc<ShardBatcher>`
  - `pub(crate) struct ShardBatcher`：`push(&self, PendingAppend)`、`take_batch(&self) -> Vec<PendingAppend>`（FIFO，≤ `MAX_APPEND_REQUESTS = 64`）、`flush(&self) -> tokio::sync::MutexGuard<'_, ()>`
  - `fn append_rejection_status(rejection: &MqAppendRejection) -> Status`
  - `fn validate_batch_entries(len: usize) -> Result<(), Status>`（1..=64）
- Consumes: Task 1 的 `mq_topic_table`；M1 的 `apply_mq_append_batch` 语义与
  `MqAppendRequest/MqAppendBatch/MqAppendOutcome`。

- [ ] **Step 1: 写失败测试**
  - `publish.rs`：
    `publish_target_rejects_unknown_topic`（`TOPIC_NOT_FOUND`）、
    `publish_target_rejects_a_stale_epoch`（`TOPIC_EPOCH_STALE`）、
    `publish_target_rejects_an_out_of_range_partition`（`PARTITION_NOT_FOUND`）、
    `publish_target_requires_multi_raft_assembly`（`FAILED_PRECONDITION`）、
    `take_batch_is_fifo_and_capped_at_64`、
    `rejection_statuses_keep_their_wire_codes`。
  - `metrics.rs`：`render` 含两个新计数器的 HELP/TYPE 行。
- [ ] **Step 2: 跑测试确认失败**
  Run: `cargo test -p coord-server --lib mq_v2::publish && cargo test -p coord-server --lib metrics`
  Expected: FAIL
- [ ] **Step 3: 实现**
  - `mq_publish_target`：读表 → topic/epoch/partition 校验 → 展开
    `max_message_bytes`/`max_bytes`/`segment_bytes`（直接用已展开的 `topic.config`）→
    `route_runtime(mq_log_shard_prefix(shard))` → 组装 target。
  - `mq_append`：
    1. leader 预检（`current_leader`）→ 非 Leader：
       `attach(map_err(Error::RegionNotLeader{region_id, leader_addr: self.grpc_addr_of(leader_id)}), NOT_LEADER)`；
    2. `batcher.push(PendingAppend{request, reply})`；
    3. `let _guard = batcher.flush().await;` → `loop { let batch = take_batch(); if empty break;
       let (requests, replies) = batch.into_iter().map(|p| (p.request, p.reply)).unzip();
       let cmd = Command::MqAppendBatch(MqAppendBatch{shard_id, now_ms(), requests});
       let result = self.client_write_with_timeout(&target.runtime.raft, cmd, target.region_id).await;
       deliver(result, replies) }`；计数 `append_batches_total += 1`、
       `append_requests_total += n`；
    4. `drop(_guard)` 后 `rx.await`（sender 被取消 ⇒ `UNAVAILABLE`「batch cancelled before
       commit」）。
  - 错误回填：`Err(status)` 时 `status.clone()` 发给所有 reply；响应请求数不匹配 ⇒
    `INTERNAL` 发给所有 reply。
  - handler `publish`：`gate(Publish)` → `mq_publish_target` → `mq_append(entries: [entry])`
    → `PublishResponse`（offset `i64::try_from` 失败 ⇒ `INTERNAL`）。
  - handler `publish_batch`：`gate(Publish)` → `validate_batch_entries` →
    `mq_publish_target` → `mq_append(entries: all)` → 逐条 `PublishResponse`。
- [ ] **Step 4: 跑测试确认通过**
  Run: `cargo test -p coord-server --lib mq_v2::publish && cargo test -p coord-server --lib metrics`
  Expected: PASS
- [ ] **Step 5: 提交**
  `git commit -m "feat(mq-v2): per-shard publish batcher and handlers (M2-1)"`

### Task 3: 写路径集成验收（M2-1/M2-2/M2-3）

**Files:**
- Create: `coord/tests/support/mq_v2_node.rs`（单节点多 Region 夹具）
- Create: `coord/tests/mq_v2_m2_publish_test.rs`
- Test: 同上

**Interfaces:**
- Consumes: `carve_mq_regions`、`spawn_configured_regions`、`RegionLifecycle`、
  `Command::RegionMeta(RegionMetaOp::Init)`（照 `coord-server/tests/kv_t_g5_obj_region_test.rs`
  的 boot 路径）。
- Produces: `MqV2Node::boot(log_shards, group_shards) -> MqV2Node`，暴露
  `addr`、`mvcc0`、`manager`、`runtime(shard_id)`、`shutdown()`。

- [ ] **Step 1: 夹具 + 失败测试**（每个用例断言错误码走 `error_code_of`，不断言文案）
  - `create_topic_then_publish_returns_a_committed_offset`：CreateTopic(4 分区) →
    Publish → offset 0；Publish 两次 → 1；分片 `mq_partition_bounds().next_offset == 2`。
  - `publish_offsets_are_unique_and_monotonic_under_concurrency`：并发 64 次 →
    offset 集合恰为 `0..64`。
  - `concurrent_publishes_are_coalesced_into_fewer_raft_entries`：同一 64 次并发后
    `append_batches_total < append_requests_total`（指标从节点读取）。
  - `duplicate_idempotency_key_returns_the_first_offset`：两次同键 → 同 offset +
    `deduplicated=true`（第二次）；同批内重复键同理。
  - `publish_batch_is_atomic_when_one_entry_is_oversized`：topic
    `max_message_bytes=64`，批内含一条 200B → `MESSAGE_TOO_LARGE`，`next_offset` 不变。
  - `publish_batch_over_the_quota_writes_nothing`：topic `max_bytes=2048`，批超配额 →
    `QUOTA_EXCEEDED`，`next_offset` 不变。
  - `publish_after_retention_truncation_reuses_the_idempotency_key`：publish(k) →
    `Command::MqTruncate(up_to=1)` → publish(k) → **新** offset（窗口边界）。
  - `topic_epoch_is_enforced_on_the_write_path`：`MqTopicOp::Update` 推进 epoch 后旧
    epoch 写入 → `TOPIC_EPOCH_STALE`。
  - `publish_batch_rejects_empty_and_over_64_entries`：`INVALID_ARGUMENT`。
  - `publish_is_gated_by_the_publish_capability`：能力集合只开 `Topic` →
    `FAILED_PRECONDITION`。
- [ ] **Step 2: 跑测试确认失败**
  Run: `cargo test -p coord --test mq_v2_m2_publish_test`
  Expected: FAIL（夹具/接口不存在）
- [ ] **Step 3: 实现夹具（不实现业务逻辑；业务已在 Task 2 完成）**
- [ ] **Step 4: 跑测试确认通过**
  Run: `cargo test -p coord --test mq_v2_m2_publish_test`
  Expected: PASS（10 例）
- [ ] **Step 5: 提交**
  `git commit -m "test(mq-v2): publish path acceptance (M2-1/M2-2/M2-3)"`

### Task 4: Rust 客户端 `MqV2Client`

**Files:**
- Create: `coord-client/src/mq_v2.rs`
- Modify: `coord-client/src/lib.rs`（`pub mod mq_v2;` + re-export）
- Modify: `coord-client/src/client.rs`（`pub fn mq_v2()`、`pub fn endpoints()`、
  `pub(crate) async fn channel_for(endpoint) -> Result<AuthedChannel>`）
- Test: `coord-client/src/mq_v2.rs`（纯函数/hint 解析）+ Task 6 e2e

**Interfaces:**
- Produces:
  - `MqV2Client::{create_topic(&MqCreateTopic) -> Result<MqTopicCreated>}`、
    `{get_topic_leader(&str) -> Result<Vec<MqPartitionLeader>>}`、
    `{publish(&MqPublish) -> Result<MqPublishResult>}`、
    `{publish_batch(&MqPublishBatch) -> Result<Vec<MqPublishResult>>}`
  - `struct MqPublish { topic: String, partition: i32, topic_epoch: u64, key: Vec<u8>, payload: Vec<u8>, idempotency_key: String, headers: BTreeMap<String, String>, producer: String }`
  - `struct MqPublishBatch { topic, partition, topic_epoch, entries: Vec<MqBatchEntry> }`
  - `struct MqPublishResult { offset: u64, timestamp_ms: i64, deduplicated: bool }`
- 路由：`leaders: Arc<RwLock<HashMap<(String, i32), String>>>`；每次 publish 先查缓存，
  未命中 `GetTopicLeader`；`NOT_LEADER` 的 `coord-leader-hint` 更新**该分区**的缓存并
  立即重试一次；其余可重试错误走 `RetryState`；`CreateTopic` 用
  `execute_write_with_retry`（region 0 的 leader hint 是节点级，不是分区级）。

- [ ] **Step 1: 写失败测试**
  `hint_from_a_not_leader_status_updates_the_partition_cache`（伪造 Status + metadata）、
  `endpoint_without_a_hint_is_not_cached`。
- [ ] **Step 2: 跑测试确认失败**
  Run: `cargo test -p coord-client --lib mq_v2`
- [ ] **Step 3: 实现（含 `Client` 的 3 个访问器；`MqV2Client` 必须 `Clone`）**
- [ ] **Step 4: 跑测试确认通过**
  Run: `cargo test -p coord-client --lib mq_v2`
- [ ] **Step 5: 提交** `git commit -m "feat(mq-v2): Rust client publish path with per-partition leader hints (M2-4)"`

### Task 5: Agent v2 代理与启动门禁

**Files:**
- Modify: `coord-agent/src/proxy_v2.rs`（`MqV2Proxy`）
- Modify: `coord-agent/src/lib.rs`（v2 启动门禁：`services.mq` 允许；v1 MQ 服务只在
  `data_plane=V1` 装配；router 注册 `MqServer(MqV2Proxy)`；`AgentGrpcSvcs` 增 `mq: bool`）
- Test: Task 6 e2e（进程级）

**Interfaces:**
- `MqV2Proxy::new(inner: Option<Arc<AgentInner>>)`；`Publish`/`PublishBatch`/
  `CreateTopic`/`GetTopicLeader` 转发 `inner.client.mq_v2()`；其余 15 个 RPC 返回
  `UNIMPLEMENTED`（消息指向 gate 文档），不使用 `panic!`。
- `coord.mq.v2` 只在 `data_plane == V2 && services.mq == true` 时注册；
  `workflow_durable` 在 v2 仍 fail-closed（M5）。

- [ ] **Step 1: 写失败测试**（进程级，放 Task 6 文件；此步只写断言骨架，
  `cargo test --no-run -p coord --test mq_v2_m2_cluster_test` 失败于代理缺失）
- [ ] **Step 2: 实现代理与门禁**
- [ ] **Step 3: `cargo check -p coord-agent`**（无 panic 宏）
- [ ] **Step 4: 提交** `git commit -m "feat(mq-v2): agent v2 MQ proxy and startup gate (M2-4)"`

### Task 6: 三节点集群 e2e（M2-4）

**Files:**
- Create: `coord/tests/mq_v2_m2_cluster_test.rs`
- Test: 同上（复用 `coord/tests/common/v2_cluster.rs`；`V2ClusterOptions.toml` 追加
  `[mq_v2] enabled = true / log_shards = 2 / group_shards = 1`）

- [ ] **Step 1: 写失败测试**
  - `follower_returns_not_leader_with_a_leader_hint`：对非该分区 Leader 的节点发原始
    `Publish` → `UNAVAILABLE`、`NOT_LEADER`、metadata 有 `coord-leader-hint`。
  - `client_publish_reroutes_across_the_cluster`：`coord_client::Client`（3 端点）→
    `mq_v2().publish` 成功且 offset 递增；不再需要手工重试。
  - `agent_with_services_mq_on_v2_publishes_through_the_proxy`：起 agent
    （`data_plane="v2"`、`services.mq=true`、`static_peers=[node1]`）→ 经 agent 的
    `coord.mq.v2` `CreateTopic` + `Publish` 成功；agent 进程不因 `services.mq` 退出。
- [ ] **Step 2: 跑测试确认失败**
  Run: `cargo test -p coord --test mq_v2_m2_cluster_test -- --test-threads=1`
- [ ] **Step 3: 实现**（只修装配/客户端，不改服务端语义）
- [ ] **Step 4: 跑测试确认通过**（同一命令）
- [ ] **Step 5: 提交** `git commit -m "test(mq-v2): three-node publish e2e and agent proxy (M2-4)"`

### Task 7: 指标、基准与 gate 证据（M2-5）

**Files:**
- Modify: `coord-server/src/metrics.rs`（publish 侧计数器 + render + 单测）
- Modify: `coord-server/src/mq_v2/publish.rs` / `services.rs`（生产写入点）
- Create: `coord/tests/mq_v2_bench.rs`（`#[ignore]`，复用 `support/mq_v2_node.rs`）
- Modify: `docs/production/ops/mq-v2-gate.md`（§1 M2 行 → ✅ + 可复跑命令）
- Modify: `docs/superpowers/specs/2026-10-29-mq-v2-design.md`（头注状态：M0–M2 完成）

指标（全部 `coord_mq_v2_` 前缀）：
`publish_requests_total`、`publish_entries_total`、`publish_bytes_total`、
`publish_not_leader_total`、`publish_rejected_total`、`publish_duplicates_total`、
`publish_latency_us_total`、`append_batches_total`、`append_requests_total`。

- [ ] **Step 1: 写失败测试**：metrics 渲染断言 + bench 的输出行断言（Markdown 行含
  `ops/s` 与 `p50/p99`）。
- [ ] **Step 2: 跑测试确认失败**
  Run: `cargo test -p coord-server --lib metrics -q`；`cargo test -p coord --test mq_v2_bench --no-run`
- [ ] **Step 3: 实现指标写入点与 bench**
  （bench：固定 payload 256B，串行 1 万次 + 并发 64 × 1 万次，打印吞吐/p50/p99 与平均
  批深度；release 运行）
- [ ] **Step 4: 验证**
  Run: `cargo test -p coord-server --lib metrics`；
  `cargo test --release -p coord --test mq_v2_bench -- --ignored --nocapture --test-threads=1`
  Expected: PASS 且报告数字落 stdout（数字只作记录，M6-3 出正式报告）
- [ ] **Step 5: 更新 gate 文档 + 提交**
  `git commit -m "docs(mq-v2): record M2 gate evidence"`

### Task 8: 本地 CI 门禁与 PR

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --workspace --lib --bins -- -D warnings`
- [ ] `bash scripts/check-panics.sh`
- [ ] `bash scripts/check-doc-refs.sh`
- [ ] `cargo audit`、`cargo deny check`
- [ ] `cargo test -p coord-server --lib mq_v2` + `cargo test -p coord --test mq_v2_m2_publish_test`
- [ ] 推送分支 + `gh pr create --base main --fill`；`gh pr checks --watch` 全绿后
      `gh pr merge --delete-branch --squash`（M2 分两个 PR：A = Task 1–3，B = Task 4–7）

## Self-Review

- **Spec 覆盖**：M2-1（路由/批量/提交后响应）→ Task 2/3；M2-2（offset/幂等/窗口）→
  Task 3；M2-3（上限/错误/原子性）→ Task 2/3；M2-4（Agent 代理 + Rust 客户端）→
  Task 4/5/6；M2-5（指标/基准）→ Task 7。设计 §5.3 五步写路径全部有对应实现步骤。
- **类型一致性**：`mq_publish_target`/`mq_append`/`MqPublishTarget` 在 Task 2 定义，
  Task 3/4/6 只消费；`MqPublishResult` 字段与 proto `PublishResponse` 一一对应。
- **Review Focus 测试归属**：未提交可见 → Task 3 的 `next_offset` 断言（提交后才有）；
  部分提交 → 两条 atomic 用例；跨分片串行化 → 夹具 `log_shards=2` + 并发用例；
  Follower 写入 → Task 6 第一条；关闭态副作用 → Task 2 的 gate 单测（现有 M0 用例）；
  offset 溢出 → Task 2 的 `i64::try_from` 分支（handler 单测覆盖失败路径需构造
  `offset > i64::MAX`，以 `u64::MAX` 直接调用转换函数断言 `INTERNAL`）。
