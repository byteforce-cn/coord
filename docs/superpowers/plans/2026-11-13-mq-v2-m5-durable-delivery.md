# MQ v2 M5（第一切片）— v2 投递面与 durable 工作流实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 `services.workflow_durable = true` 在 `data_plane = "v2"` 上真正可用：outbox
relay 投递到 `coord.mq.v2`（`event_id` 作幂等键）、`listen` 从 v2 事件 topic 续读、
durable/审批在 v2 上装配并通过 kill -9 证据。

**Architecture:** 状态与 outbox 的「同事务」由既有 `AtomicOutboxCommit` + `coord-client`
的 v2 适配层（`v2_compat::txn` → 单次 `coord.txn.v2` 提交）承担，本切片不重写存储层。
只替换**投递面**：新增 `coord-agent/src/services/mq_v2_delivery.rs`（`MqV2OutboxSink`
实现 `OutboxSink`、`MqV2EventProvider` 实现 `EventProvider`），由
`WorkflowEngineService::new_with_kv_store_durable_v2` 装配；v1 装配路径（`DurableEngineOptions`
/ `new_with_kv_store_durable`）逐字不变。

**Tech Stack:** Rust 2021、tonic 0.14、`coord-client`（`MqV2Client` / `Client::mq_v2()`）、
`coord.txn.v2` / `coord.kv.v2`、tokio。

**Spec:** `docs/superpowers/specs/2026-10-29-mq-v2-design.md` §6 M5（M5-1/M5-2/M5-5 的 v2
投递面部分）；`docs/adr/0016-mq-v2.md` D12；`docs/adr/0015-kv-t.md` D13（B-KVT-6）。

## Global Constraints

- `data_plane = "v1"` 行为**字节级不变**：不新增/不改 `DurableEngineOptions` 字段，v1 装配
  与 topic 引导仍走 `MessageQueueService`（ADR-0001）。
- v2 durable 要求 `services.mq = true`（`coord.mq.v2` 代理开关）与 Server 连接；缺失在
  启动期拒绝并点名前置（不静默降级到非 durable）。
- 服务器 `[mq_v2].enabled = false` 时 `CreateTopic` 返回 `FAILED_PRECONDITION`：Agent 启动
  必须失败并点名 MQ v2 不可用——不得静默不发事件。
- 事件 topic 固定 `partitions = 1` / `retention_secs = 7d` / `max_message_bytes = 1MiB`，
  与 v1 durable topic 参数一致；已存在时**不改配置**（不自动 `UpdateTopic`）。
- 幂等键 = `OutboxRecord::id`（`event_id`），去重窗口 = topic 保留期（ADR-0016 D8）；
  重复投递不得产生第二个 offset。
- 用户级错误只按码分支（`TOPIC_EPOCH_STALE`/`TOPIC_NOT_FOUND`/`TOPIC_EXISTS`/
  `OFFSET_OUT_OF_RANGE`）。
- 非测试代码不得 `unwrap/expect/panic/unimplemented/todo`；`scripts/check-panics.sh` 0。
- 不改 `coord.mq.v2` 契约、不改 `coord.workflow.v1` wire（Start 幂等键属 M5-3 切片）。

## Review Focus

- **已存在的事件 topic 配置不同**（别人建了多分区/别的保留期）：启动不失败、不静默
  `UpdateTopic`；所有事件仍投递到 partition 0（per-instance 有序）。
- **`[mq_v2].enabled=false`**：Agent 启动失败且错误文案点名 MQ v2（不是 UNAVAILABLE 噪声）。
- **relay 重投**（崩溃/租约回收/退避重试同一记录）：MQ 幂等索引去重，topic 上只有一条。
- **`listen` 游标低于 `earliest`**（保留删段）：重置到最早可用并继续，只 WARN，不丢后续事件。
- **kill -9 后重启**：已提交实例状态与 outbox 不丢；重启后不重复投递已完成事件。
- **v1 集群**：`services.workflow_durable` 仍要求 `services.mq`；v1 装配测试全绿。

---

### Task 1: v2 outbox 投递面（`MqV2OutboxSink` + topic 引导）

**Files:**
- Create: `coord-agent/src/services/mq_v2_delivery.rs`
- Modify: `coord-agent/src/services/mod.rs`（按字母序加 `pub mod mq_v2_delivery;`）

**Interfaces:**
- Produces（后续任务依赖）：
  - `pub const DURABLE_TOPIC_PARTITIONS: i32 = 1;`
  - `pub struct MqV2OutboxSink`，`pub fn new(inner: Arc<AgentInner>, topic: impl Into<String>) -> Self`
  - `pub async fn ensure_topic(&self) -> Result<u64, String>`（返回 `topic_epoch`；单次尝试）
  - `pub async fn wait_until_ready(&self) -> Result<u64, String>`（有界 30s 重试瞬时错误）
  - `pub(crate) fn publish_request(&self, rec: &OutboxRecord) -> MqPublish`
  - `impl OutboxSink for MqV2OutboxSink`

语义决定（实现前先读）：
1. `CreateTopic` 的 `request_id = format!("workflow-durable:{topic}")`：同键重放返回首次
   `topic_id/topic_epoch`；返回 `TOPIC_EXISTS`（别人建的）时退回 `GetTopicLeader` 取
   `topic_epoch` 并接受既有分区数（只投 partition 0）。
2. `publish_request` 的 `payload = serde_json::to_vec(rec)`；headers 与 v1
   `MqOutboxSink` 逐字一致（`ce-id`/`ce-type`/`ce-source`/`ce-specversion`/
   `ce-dataschema`/`partitionkey`/`sequence`）；`idempotency_key = rec.id`；
   `producer = "coord-agent/workflow-durable"`；`partition = 0`。
3. `deliver`：epoch 缓存为 0 时先 `ensure_topic()`；`Error::V2 { code: TopicEpochStale |
   TopicNotFound }` 时刷新 epoch 后**重试一次**；其余错误转 `String` 交给 relay 退避。
4. `ensure_topic` 的失败文案必须带 topic 名与 MQ v2 门禁提示：
   `with_context` 风格：`format!("workflow durable: ensure events topic '{topic}' failed: {err}")`。

- [x] **Step 1: 写失败单测（纯映射，不需要服务器）**

```rust
#[test]
fn publish_request_carries_the_event_id_as_the_idempotency_key() {
    let sink = MqV2OutboxSink::for_test("com.china-workflow.events");
    let rec = outbox_record("evt-1");
    let req = sink.publish_request(&rec);
    assert_eq!(req.idempotency_key, "evt-1");
    assert_eq!(req.partition, 0);
    assert_eq!(req.topic, "com.china-workflow.events");
    let decoded: OutboxRecord = serde_json::from_slice(&req.payload).expect("payload is the record");
    assert_eq!(decoded.id, "evt-1");
    assert_eq!(req.headers.get("ce-id").map(String::as_str), Some("evt-1"));
    assert_eq!(req.headers.get("partitionkey").map(String::as_str), Some("inst-1"));
}
```

（`for_test` = 只填 topic、`inner` 不可用；`outbox_record` 是测试内构造函数。）

- [x] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-agent --lib services::mq_v2_delivery`
Expected: 编译失败（`MqV2OutboxSink` 不存在）。

- [x] **Step 3: 实现 `MqV2OutboxSink`**

`ensure_topic` 用 `inner.client.mq_v2().create_topic(&MqCreateTopic { topic, partitions: DURABLE_TOPIC_PARTITIONS, request_id, config: Some(MqTopicConfig { retention_secs: 7d, max_message_bytes: 1MiB, ..Default::default() }) })`；
`TOPIC_EXISTS` 时 `get_topic_leader(topic)`。`wait_until_ready` 循环 100ms 重试直到 30s：
`Error::V2 { code: TopicEpochStale | TopicNotFound }` 之外的 `Unavailable/DeadlineExceeded/
ClusterUnavailable` 都算瞬时；`FailedPrecondition`（MQ v2 关闭）立即失败。

- [x] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-agent --lib services::mq_v2_delivery`
Expected: PASS。

- [x] **Step 5: Commit**

```bash
git add coord-agent/src/services/mq_v2_delivery.rs coord-agent/src/services/mod.rs
git commit -m "feat(mq-v2): v2 outbox sink and events topic bootstrap (M5-1)"
```

---

### Task 2: v2 listen 投递面（`MqV2EventProvider`）

**Files:**
- Modify: `coord-agent/src/services/mq_v2_delivery.rs`
- Modify: `coord-agent/src/services/mq_event_provider.rs`（把 `encode_event`/`decode_event`/
  匹配逻辑提成 `pub(crate)` 自由函数；v1 provider 改调用点，行为不变）

**Interfaces:**
- Consumes: `CursorStore`（`KvCursorStore`，键空间 `/_workflow/v3/mq-offsets/`）。
- Produces:
  - `pub struct MqV2EventProvider`，`pub fn new_with_cursor_store(inner: Arc<AgentInner>, topic: impl Into<String>, cursors: Arc<dyn CursorStore>) -> Self`
  - `impl EventProvider for MqV2EventProvider`
  - `pub(crate) fn encode_event(event_type: &str, source: Option<&str>, data: &Value) -> Vec<u8>`
  - `pub(crate) fn decode_event(payload: &[u8]) -> Option<(String, Option<String>, Option<String>, Value)>`
  - `pub(crate) fn event_matches(types: &[&str], source: Option<&str>, subject: Option<&str>, got: (&str, Option<&str>, Option<&str>)) -> bool`

语义决定：
1. 轮询 `poll(topic, partition=0, start_offset=Some(cursor), max_messages=64)`；非空批把本地
   游标推进到 `result.next_offset`；与非匹配记录一样**不持久化**游标——命中后才能由
   `ack_event` 提交 `offset + 1`。
2. `Error::V2 { code: OffsetOutOfRange }` ⇒ 本轮到 `start_offset = None`（服务端从最早
   可用开始）并 `tracing::warn!` 一次；不返回错误、不静默跳过后续事件。
3. `emit` 为 best-effort（与 v1 一致，忽略错误）：`publish(topic, 0, encode_event(...))`。
4. 超时/空批按 20ms 轮询到截止，返回 `None`（与 v1 一致）。

- [x] **Step 1: 写失败单测**

```rust
#[test]
fn event_provider_decodes_a_cloud_event_and_matches_the_filter() {
    let payload = encode_event("com.example.created", Some("coord/workflow"), &json!({"n": 1}));
    let (event_type, source, subject, data) = decode_event(&payload).expect("valid cloud event");
    assert_eq!(event_type, "com.example.created");
    assert_eq!(source.as_deref(), Some("coord/workflow"));
    assert!(subject.is_none());
    assert_eq!(data, json!({"n": 1}));
    assert!(event_matches(&["com.example.created"], None, None, (&event_type, source.as_deref(), subject.as_deref())));
    assert!(!event_matches(&["com.example.other"], None, None, (&event_type, source.as_deref(), subject.as_deref())));
}

#[test]
fn event_provider_reuses_the_subscription_key_of_the_v1_provider() {
    let key = subscription_key("t", &["b", "a"], None, None);
    assert_eq!(key, subscription_key("t", &["a", "b"], None, None));
}
```

- [x] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-agent --lib services::mq_v2_delivery`
Expected: 编译失败（`decode_event`/`event_matches` 不存在）。

- [x] **Step 3: 实现 provider 与共享函数**

v1 `MqEventProvider` 的 `Self::encode_event`/`Self::decode_event` 与内联匹配改为调用共享
自由函数（`super::mq_event_provider::*`）；`subscription_key` 改为 `pub(crate)`。

- [x] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-agent --lib services::mq_v2_delivery services::mq_event_provider`
Expected: PASS。

- [x] **Step 5: Commit**

```bash
git add coord-agent/src/services/mq_v2_delivery.rs coord-agent/src/services/mq_event_provider.rs
git commit -m "feat(mq-v2): v2 event provider for durable listen (M5-1)"
```

---

### Task 3: durable 引擎的 v2 装配与启动门禁

**Files:**
- Modify: `coord-agent/src/services/workflow.rs`（新增 `DurableV2EngineOptions`、
  `new_with_kv_store_durable_v2`、`durable_v2_sink` 字段、`start()` 里的 v2 topic 引导；
  抽取共用装配 helper，v1 构造器行为不变）
- Modify: `coord-agent/src/lib.rs`（删除 v2 拒绝块，v2 分支选择 v2 构造器）
- Test: `coord-agent/tests/agent_workflow_durable_gate_test.rs`（改 `test_v2_data_plane_rejects_workflow_durable`）

**Interfaces:**
- Produces:
  - `pub struct DurableV2EngineOptions { pub topic: String, pub relay_enabled: bool, pub relay_interval_ms: u64, pub approval_roles: HashMap<String, Vec<String>>, pub on_behalf_services: Vec<String> }`
  - `pub fn new_with_kv_store_durable_v2(inner: Arc<AgentInner>, dispatch: &WorkflowDispatchConfig, opts: DurableV2EngineOptions) -> Self`

语义决定：
1. 共用装配 helper 接收已构造的 `Arc<dyn OutboxSink>`、`Arc<dyn EventProvider + Send + Sync>`；
   v1/v2 只差 sink/provider 与 topic 引导句柄。`durable_mq`（v1）与
   `durable_v2_sink: Option<Arc<MqV2OutboxSink>>` 互斥。
2. `start()`：先 v1 `ensure_durable_topic`（不变），再对 `durable_v2_sink` 调
   `wait_until_ready()`；失败 ⇒ `start()` 返回错误（Agent 启动失败）。
3. `coord-agent/src/lib.rs`：`data_plane == V2 && workflow_durable` 时——
   `services.mq != true` ⇒ 错误点名 `services.mq`；无 Server 连接 ⇒ 错误点名
   `server connection`；否则 `new_with_kv_store_durable_v2`，不再构造 v1 `mq_handle`。
4. 审批桥、租约、step store、`KvOutboxStore`、`KvWorkflowStore` 原样复用（同一
   `AgentInner` ⇒ v2 适配层）。

- [x] **Step 1: 改失败测试**

`test_v2_data_plane_rejects_workflow_durable` 改为两个用例：

```rust
#[tokio::test]
async fn test_v2_durable_requires_services_mq() {
    // data_plane = V2, workflow_durable = true, mq = false
    // 期望 Err 且 message.contains("services.mq")
}

#[tokio::test]
async fn test_v2_durable_requires_a_server_connection() {
    // data_plane = V2, workflow_durable = true, mq = true, static_peers 空
    // 期望 Err 且 message.contains("server connection")
}
```

- [x] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-agent --test agent_workflow_durable_gate_test`
Expected: 第一个用例失败（v2 分支仍报 "v2 data plane"）。

- [x] **Step 3: 实现装配 helper、v2 构造器与启动接线**

- [x] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-agent --test agent_workflow_durable_gate_test`
Expected: PASS（4 个用例）。

- [x] **Step 5: Commit**

```bash
git add coord-agent/src/services/workflow.rs coord-agent/src/lib.rs \
  coord-agent/tests/agent_workflow_durable_gate_test.rs
git commit -m "feat(mq-v2): assemble durable workflow on the v2 data plane (M5-2)"
```

---

### Task 4: v2 durable 证据（单事务提交 + kill -9 重启）

**Files:**
- Create: `coord/tests/mq_v2_m5_durable_test.rs`（`#[path = "common/mod.rs"]`，真实 3 节点
  v2 集群；`--test-threads=1`）

**Interfaces:**
- Consumes: `common::v2_cluster::{runtime, V2Cluster, V2ClusterOptions}`、
  `WorkflowEngineService::new_with_kv_store_durable_v2`、`KvOutboxStore`。

测试 1（in-process engine，`relay_enabled = false`）：
`durable_state_and_outbox_commit_in_one_v2_transaction`
- 用 `AgentInner::new_with_data_plane(..., DataPlane::V2)` 连集群；部署含
  `inject → end` 状态的 CNCF SW 定义并 `start_instance_as`；等实例 `Completed`。
- `OutboxStore::list_all` 取该实例 `com.china-workflow.instance.completed.v1` 记录；用
  `client.kv_v2().range` 读 `WorkflowInstance::storage_key(id)` 与
  `/_workflow/v3/outbox/records/{aggregate}/{sequence:020}`，断言两者 `mod_revision` 相等
  （= 同一次 `coord.txn.v2` 提交；outbox 的 `mark_delivered` 未运行，因为 relay 关闭）。

测试 2（真实 agent 进程）：
`durable_workflow_survives_agent_kill_and_never_duplicates_events`
- 起 `V2Cluster`（`[mq_v2] enabled = true, log_shards = 2, group_shards = 1`）与 Agent
  （`data_plane = "v2"`, `services = { workflow = true, workflow_durable = true, mq = true }`,
  `workflow_events_topic = "com.china-workflow.events"`）；断言 agent 日志含
  `"Workflow outbox relay started"`。
- 经 Agent 的 `Workflow.Start`（inline DSL，同 `kv_t_g6_agent_services_v2_test`）启动，等
  `COMPLETED`；`ListPendingTasks` 返回 `Ok`（审批桥可用，不是 `FAILED_PRECONDITION`）。
- 用 `MqV2Client::poll` 从服务器读 `com.china-workflow.events`：恰好 1 条
  `ce-type = com.china-workflow.instance.completed.v1`；用该消息 `ce-id` 作为
  `idempotency_key` 再 `publish` 一次 ⇒ `deduplicated = true`（证明投递键就是 event_id）。
- `kill -9` Agent，重启同一配置/数据目录；等 relay 日志；`GetStatus` 仍 `COMPLETED`；
  再 poll topic ⇒ `completed` 事件仍恰好 1 条（无重复、无丢失）。

- [x] **Step 1: 写测试（两个用例）**
- [x] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord --test mq_v2_m5_durable_test -- --test-threads=1`
Expected: 编译失败（`new_with_kv_store_durable_v2` 在 Task 3 已实现 ⇒ 第一个用例应通过；
第二个用例在 Task 3 完成前无法启动 Agent，先记录失败）。

- [x] **Step 3: 修到绿（按失败点最小改动，不改语义）**
- [x] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord --test mq_v2_m5_durable_test -- --test-threads=1`
Expected: PASS（2 个用例）。

- [x] **Step 5: Commit**

```bash
git add coord/tests/mq_v2_m5_durable_test.rs
git commit -m "test(mq-v2): durable state+outbox and kill -9 evidence on v2 (M5-1/M5-2)"
```

---

### Task 5: 文档与 gate 证据

**Files:**
- Modify: `docs/production/ops/mq-v2-gate.md`（M5 行：M5-1/M5-2 ✅ + 证据；M5-3/M5-4 仍 ⏳）
- Modify: `docs/production/ops/boundaries.md`（B-KVT-6：解冻条件已到一半——v2 durable/审批
  在 `[mq_v2].enabled=true` + `services.mq=true` 时可用；MQ v2 仍 PROPOSED）
- Modify: `docs/production/workflow-semantics.md` §8（可用/不可用清单）
- Modify: `docs/superpowers/specs/2026-10-29-mq-v2-design.md` 头注（M5 进行中）

- [x] **Step 1: 更新 gate/buildings 文档，跑引用检查**

Run: `bash scripts/check-doc-refs.sh`
Expected: `dangling refs: 0`。

- [x] **Step 2: 跑本机全量门禁（提 PR 前）**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-panics.sh
cargo audit
cargo deny check
```

- [x] **Step 3: Commit**

```bash
git add docs/
git commit -m "docs(mq-v2): record M5 durable delivery evidence and lift B-KVT-6 (M5-5)"
```

---

## Self-review notes

- 覆盖率：M5-1（outbox 同事务 + MQ v2 投递 + event_id 幂等）→ Task 1/4；
  M5-2（durable/审批上 v2 + 启动门禁 + 纯 KV 不回归）→ Task 3/4；
  M5-5 与 v2 投递面相关部分 → Task 5。M5-3（trigger 定义/分发器）与 M5-4 的
  trigger e2e 不在本切片，另起计划。
- 类型一致性：`OutboxSink`/`EventProvider`/`CursorStore` 沿用 `coord-core` 既有 trait；
  `MqV2OutboxSink`/`MqV2EventProvider` 的名字在 Task 1/2/3/4 一致。
