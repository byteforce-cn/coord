# Spike 3 · Outbox 方案（KV 同事务写状态 + 事件）

> Owner: Spike 执行（AI 协作者）｜ Last verified: 2026-10-13
> 状态：初版完成（决策证据，不是生产契约）
> 回答手册 §4 Spike 3 的问题 1、2（同事务原子性 / Relay 不丢）
> 关键实现：`coord-core/src/workflow/durable.rs`、`coord-agent/src/services/outbox_kv.rs`、
> `coord-agent/src/services/outbox_relay.rs`

## 0. 结论（先给判定）

| 问题 | 判定 | 依据 |
|:--|:--|:--|
| 状态写入 + outbox 写入能否在同一 KV Txn 原子完成？ | **能**，且已在真实 Raft KV 上验证「要么都落、要么都不落」 | `coord/tests/spike3_durable_kv.rs::state_and_outbox_commit_is_all_or_nothing` |
| Relay 投递 + 标记 + 失败退避能否保证不丢（允许重复，靠 event_id 去重）？ | **能**（at-least-once）；重复由消费者按 `event_id` 去重吸收；broker 侧还有 `produce_idempotent(event_id)` 第二道闸 | 同上文件的 `relay_delivers_all_records_and_consumer_dedups_by_event_id`、`outbox_events_pending_at_crash_are_delivered_after_restart`；进程级证据 `evidence/kill9-*-120314.log` + `evidence/*-120314/{outbox,events}.tsv` |

「验证通过」不是证据：上面每一行的可执行命令见 §6。

## 1. KV key 布局

全部落在既有的 `/_workflow/v3/` 前缀族下（与 `KvWorkflowStore` 同族，便于按域治理）：

| key | value | 作用 |
|:--|:--|:--|
| `/_workflow/v3/outbox/records/{aggregate_id}/{sequence:020}` | `OutboxRecord` JSON | **权威归档**：`list_all` 只读它 |
| `/_workflow/v3/outbox/ids/{event_id}` | 记录键 | 幂等索引（同 `event_id` 重复 append = 空操作） |
| `/_workflow/v3/outbox/seq/{aggregate_id}` | 十进制序号 | per-instance 单调序号计数器（CAS 目标） |
| `/_workflow/v3/outbox/pending/{next_attempt_at_ms:020}:{event_id}` | `OutboxRecord` JSON | **到期扫描索引**（字典序 = 时间序） |
| `/_workflow/v3/mq-offsets/{subscription}` | 十进制 offset | `MqEventProvider` 的持久化订阅游标（见 RECOVERY.md） |

两处「同实例前缀 + 时间序/序号」：

- `records/{aggregate_id}/` 是**实例前缀**，第二段是**零填充 20 位序号** ⇒ 同一实例的记录按键即按发生顺序；
- `pending/` 与 `timers/` 的第一段是**零填充 20 位时间戳** ⇒ 字节字典序 = 到期时间序 ⇒ 「扫描到期项」就是一次前缀 range。

`aggregate_id` = `WorkflowInstance.id`，同时是 MQ 的 `partitionkey`（同一实例的事件落同一分区 ⇒ 分区内有序 ⇒ per-instance 有序）。

## 2. Txn 形状（状态 put + outbox put 同事务）

`AtomicOutboxCommit::commit_state_and_outbox(inst, rec, expected_mod_rev)` 在**一个**
`Txn/Txn` 里提交：

```text
compares (AND):
  outbox/seq/{agg}   VALUE == <读到的计数器字节>      # 缺失键的目标值是空字节
  outbox/ids/{evt}   VERSION == 0                     # 事件 ID 未用过（幂等）
  [可选] instances/{id} MOD_REV == expected_mod_rev   # 关键状态转换的 CAS

success ops:
  put instances/{id}                         = 状态快照
  put outbox/seq/{agg}                       = seq + 1
  put outbox/ids/{evt}                       = records 键
  put outbox/records/{agg}/{seq:020}         = OutboxRecord
  put outbox/pending/{next_attempt:020}:{evt} = OutboxRecord

failure ops: 空
```

三个性质：

1. **原子**：compare 任一不满足 ⇒ 整个 Txn 不生效（failure 分支为空），
   观察不到「状态已改而事件缺失」或反之。实测见 §6 的第 1 条命令：
   故意用不匹配的 `mod_rev` ⇒ 状态键与 outbox 记录**都**读不到；
   去掉约束 ⇒ 两者**同时**可见。
2. **幂等**：`ids/{event_id}` 的 `VERSION == 0` 让同一事件重复提交成为空操作
   （relay 重投 / 恢复重放安全）。
3. **per-instance 单调**：序号在同一事务里自增，不依赖全局 revision；同一实例的
   事件序号连续 +1（证据 `evidence/wait-*/outbox.tsv` 的 `sequence` 列 = 1..6）。

`coord-agent/src/services/outbox_kv.rs:420` 是唯一实现；`coord-core` 只定义契约，
不依赖具体后端。

### 2.1 当前装配下的边界（必须说清楚）

`WorkflowRuntime::emit_lifecycle`（`coord-core/src/workflow/runtime.rs:1025`）在生命周期
事件处走同事务提交：**事件与"该实例的当前状态快照"同一个 Txn**。`drive()` 内部的其余
状态保存仍是普通 Put（不在同一 Txn）——把 `drive` 的每一处保存都折叠进 Txn 需要重构
3885 行的驱动循环，超出 Spike 范围。

这不影响不丢：事件只要没和状态一起提交，就压根没发布；状态照旧由既有的保存点保证。
它影响的是"状态与事件严格同版本"这一更强性质，留给阶段二（见 §7 缺口 G1）。

在「Spike 2 CAS/fencing 写路径 + 本 spike durable 能力」的当前装配下，这条边界
没有收窄、反而多了一层：`drive` 的状态写回走 `persist_instance`（CAS / 租约
`fenced_save`），随后 `emit_lifecycle` 才用 `commit_state_and_outbox` 提交
「同一份快照 + 事件」。于是外部观测者可能先看到终态、后看到事件记录（两次写
之间的窗口）。编排者在集成分支复跑 kill -9 retry 场景捕获到该窗口：终态已是
`failed`、单次 `pending==0` 观测之后，1.8s 时 summary 仍见
`workflow.faulted.v1` 为 `Pending`（证据 `evidence/verdict-20261013-122925.tsv`
与 `evidence/kill9-retry-20261013-122925.log`）。

这不构成事件丢失（relay 随后投递、消费者按 `event_id` 去重），但说明：阶段二的
正确集成点是让 outbox 记录搭上 `persist_instance`/`fenced_save` 的同一次 CAS
Txn，而不是在状态写之后单独提交；否则「状态与事件严格同版本」在 CAS/fencing
路径上不成立。harness 的排空判定已改为「连续多轮为 0」的稳定性观测，避免把这个
窗口误报成事件丢失（窗口本身仍保留在证据里）。

## 3. 记录结构

```rust
OutboxRecord {
  id, aggregate_id, sequence, event_type, specversion, source, subject,
  dataschema, time, data, created_at_ms, status, attempts, next_attempt_at_ms, delivered_at_ms
}
```

- `id`：全局唯一事件 ID（CloudEvents `id`），**消费者去重键**；
- `event_type`：`com.china-workflow.*`（见 `EVENT-CONTRACT.md`）；
- `dataschema`：`https://schemas.coord.dev/events/{event_type}.json`；
- `time`：RFC3339 UTC（自实现序列化，避免为 coord-core 引入时间库）；
- `status`：`pending | delivered | dead`；`dead` 是**重试耗尽**的显式终态，不静默丢弃。

## 4. Relay：轮询 → 投递 → 标位 → 退避

`OutboxRelay::tick(now)`（`coord-core/src/workflow/durable.rs:314`）：

1. `pending(now, batch)` 取 `status = pending && next_attempt_at <= now`，按到期时间升序；
2. 逐条 `sink.deliver()`：
   - 成功 → `mark_delivered`（删 pending 索引 + 记录置 `delivered`，同事务）；
   - 失败 → `attempts + 1`；未到上限则 `mark_retry(attempts, now + backoff)`（移动 pending
     索引键）；到上限则 `mark_dead`；
3. 退避 `backoff_ms(attempt) = min(base * 2^attempt, cap)`，默认 base 200ms / cap 30s。
   **不加抖动**：抖动会让「同输入同输出」的复现测试失去确定性。

不丢的边界，逐字写清：

- 投递成功但**标位前**进程死 ⇒ 重启后重复投递（at-least-once 的正常形态）；
- 「不重」由两处吸收：消费者按 `id` 去重（契约，见 §5），以及 broker 的
  `produce_idempotent(idempotency_key = event_id)`（relay 重投不产生第二个 offset）。
  实测见 §6 第 2 条命令。

## 5. 投递面选择：MQ（Poll+Ack），不选 `coord.event.v1` 的理由

选 **MQ（`MessageQueueService`）**：

- 引擎（`coord-agent` 进程内）**本进程就有** `produce/consume/commit_offset`，
  `coord.event.v1` 的 `EventPublish` 只在 agent gRPC 面暴露，引擎侧没有直接客户端入口
  （`coord-client::Client` 无 event 客户端）；
- `docs/production/consumer-contract.md` §5 明确 `Poll + Ack` 是至少一次消费的**唯一**
  正确性路径（`Subscribe` 推送在背压时丢弃、不做不丢承诺）；
- `produce_idempotent` 免费提供按事件 ID 的 broker 侧去重。

`coord.event.v1` 仍是**可行的替代投递面**（全局单调 `seq` + cursor 补投），若要改用它，
替换点只有一个：`OutboxSink` 的实现（`coord-agent/src/services/outbox_relay.rs`），
relay/存储/契约都不动。

投递面载荷 = `OutboxRecord` 的 JSON（**不是**裸 `data`），并同时写 MQ headers：
`ce-id / ce-type / ce-source / ce-specversion / ce-dataschema / partitionkey / sequence`。

## 5.1 TTL / 保留口径

- 本 Spike **不实现 outbox TTL 清理**。理由（也回答手册「验证 Outbox 的 TTL 机制是否
  能保证事件不丢」）：TTL 是**删除**语义，任何删除都只可能在「已投递且已过保留窗口」
  之后发生才有意义；在 `status = delivered` 未确认前删记录 = 丢事件。
- 现状：记录只增不删；`/tmp` 里 3 个场景各 4–6 条，无清理压力。
- 保留策略归属：历史归档是**外部消费者职责**（手册 §4 原文），coord 侧只需保证
  「未投递不删」。因此**不做 TTL 自动删除**，只提供 `status` 供外部归档器判断。
- 若阶段二要做 TTL：必须满足 `status ∈ {delivered, dead}` 且 `delivered_at_ms + TTL < now`，
  且删除本身写审计（避免"清理由此丢事件"无法复盘）。

## 5.2 消费者按 event_id 去重契约

1. 消费者以**至少一次**为前提写代码：同一 `id` 可能到达多次；
2. 业务副作用以 `id` 为幂等键（去重表 / 唯一索引 / 状态守卫任选，但必须落库）；
3. `(aggregate_id, sequence)` 可用于**乱序检测**（同一实例内序号连续 +1）；
4. 不得假设 `id` 单调：`sequence` 才单调，且只保证 per-instance。

Spike 的验证消费者实现即此契约的最小版：`drain_topic` 拉全量 → 按 `id` 去重 →
`events.tsv` 标 `duplicate` 列（证据里全为 `false`，见 §6）。

## 6. 复现命令

```bash
# 1) 机制级（真实进程内单节点 Raft + KV/Txn）
CARGO_BUILD_JOBS=3 cargo test -p coord --test spike3_durable_kv -- --test-threads=1
#    其中 state_and_outbox_commit_is_all_or_nothing  = 问题 1
#         relay_delivers_all_records_and_consumer_dedups_by_event_id = 问题 2

# 2) 进程级 kill -9（真实二进制 + SIGKILL，含 outbox 补投证据）
SPIKE3_SKIP_BUILD=1 bash spike/2026-10/spike-3-recovery/scripts/repro.sh

# 3) 契约 schema 校验（对证据里的每条事件跑 JSON Schema）
python3 spike/2026-10/spike-3-recovery/scripts/schema_check.py
```

## 7. 缺口（阶段二输入）

- **G1（状态/事件同版本）**：`drive()` 的状态保存点尚未全部折叠进同事务提交（§2.1），
  且当前装配下状态写回走 CAS/fencing、事件提交是随后的独立 Txn。要拿到
  「任意时刻状态与事件严格同版本」，需要把 outbox 记录并入 `persist_instance`
  （KV 侧 `fenced_save` 的 Txn 增加 outbox ops），或统一成
  `commit_state_and_outbox` 单写路径；工作量集中在
  `coord-core/src/workflow/runtime.rs` 的 `drive` 与
  `coord-agent/src/services/workflow_lease.rs` 的 `fenced_save`。
- **G2（outbox 无清理）**：记录只增不删（§5.1）。需要保留策略 + 审计删除。
- **G3（relay 单点）**：relay 在 agent 进程内轮询；多 agent 部署下多副本会重复投递
  （被 broker 幂等键与消费者去重吸收，但会放大流量）。阶段二需要 relay 选主或
  按 outbox 分区归属。
- **G4（背压）**：`RelayPolicy.batch = 64`、扫描在每次 tick 全前缀 range；事件量大时
  需要 range 分页与并发投递上限。
