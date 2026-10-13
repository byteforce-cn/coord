# Spike 3 · 事件契约草案（`com.china-workflow.*`）

> Owner: Spike 执行（AI 协作者）｜ Last verified: 2026-10-13
> 状态：草案，**不是**对外承诺契约（不新增 RPC/字段，不改 `coord.workflow.v1`）
> 回答手册 §4 Spike 3 的问题 3（type 命名空间 / dataschema / per-instance sequence / partitionkey）
> 生成器：`schema/generate.py`（唯一事实源）｜ 校验器：`spike/2026-10/spike-3-recovery/scripts/schema_check.py`

## 0. 结论

| 契约要素 | 落地情况 | 证据 |
|:--|:--|:--|
| type 命名空间 `com.china-workflow.*` | 已落地（内部生命周期常量 → 对外类型只在 `durable::external_event_type` 一处转换） | `coord-core/src/workflow/durable.rs:35`；`evidence/*/events.tsv` 的 `event_type` 列 |
| dataschema：每个事件类型一个 JSON Schema | 已落地（11 个类型 = 11 个文件，生成 + 校验脚本可复跑） | `schema/*.json`；`python3 spike/2026-10/spike-3-recovery/scripts/schema_check.py` |
| per-instance sequence 单调 | 已落地（存储侧在同一 Txn 分配；同实例 +1） | `evidence/*/outbox.tsv` 的 `sequence` 列（wait 场景 1..6） |
| partitionkey = instance_id | 已落地（= `aggregate_id`，MQ 投递按它选分区） | `coord-agent/src/services/outbox_relay.rs` 的 `partition_for` |

## 1. 命名空间映射（单一转换点）

引擎内部沿用 CNCF Lifecycle Events 的类型名（`io.serverlessworkflow.*`，定义在
`coord-core/src/workflow/runtime.rs` 的 `lifecycle` 模块）。**对外**契约统一收在
`com.china-workflow.*` 下，转换只有一处：

```rust
external_event_type("io.serverlessworkflow.workflow.started.v1")
  == "com.china-workflow.workflow.started.v1"
```

转换发生在 `OutboxRecord::lifecycle`（`coord-core/src/workflow/durable.rs`）——
即 outbox 是**唯一**的对外事件出口。已在目标命名空间的类型原样保留（不重复加前缀）。

| 内部常量 | 对外类型 |
|:--|:--|
| `io.serverlessworkflow.workflow.started.v1` | `com.china-workflow.workflow.started.v1` |
| `io.serverlessworkflow.workflow.waiting.v1` | `com.china-workflow.workflow.waiting.v1` |
| `io.serverlessworkflow.workflow.suspended.v1` | `com.china-workflow.workflow.suspended.v1` |
| `io.serverlessworkflow.workflow.resumed.v1` | `com.china-workflow.workflow.resumed.v1` |
| `io.serverlessworkflow.workflow.completed.v1` | `com.china-workflow.workflow.completed.v1` |
| `io.serverlessworkflow.workflow.faulted.v1` | `com.china-workflow.workflow.faulted.v1` |
| `io.serverlessworkflow.workflow.cancelled.v1` | `com.china-workflow.workflow.cancelled.v1` |
| `io.serverlessworkflow.task.started.v1` | `com.china-workflow.task.started.v1` |
| `io.serverlessworkflow.task.completed.v1` | `com.china-workflow.task.completed.v1` |
| `io.serverlessworkflow.task.faulted.v1` | `com.china-workflow.task.faulted.v1` |
| `io.serverlessworkflow.task.retried.v1` | `com.china-workflow.task.retried.v1` |

## 2. 事件形态：Outbox 记录（不是裸 data）

投递面的载荷就是 `OutboxRecord` 的 JSON（`OUTBOX.md` §3），CloudEvents 属性直接落在
记录字段上，并同时写 MQ headers 便于无解析路由：

| 字段 | CloudEvents 对应 | 约束 |
|:--|:--|:--|
| `id` | `id` | 全局唯一，**消费者去重键** |
| `specversion` | `specversion` | 固定 `"1.0"` |
| `event_type` | `type` | `com.china-workflow.*`，每类型一个 schema |
| `source` | `source` | 当前固定 `coord/workflow` |
| `subject` | `subject` | = `aggregate_id` |
| `time` | `time` | RFC3339 UTC（毫秒精度） |
| `data` | `data` | 实例状态快照（见 §3） |
| `dataschema` | `dataschema` | `https://schemas.coord.dev/events/{event_type}.json` |
| `aggregate_id` | —（扩展） | = `instance_id` = **partitionkey** |
| `sequence` | —（扩展） | **per-instance 单调**，从 1 起，同实例 +1 |
| `status` / `attempts` / `next_attempt_at_ms` / `delivered_at_ms` | —（扩展） | outbox 投递状态（内部可观测） |

MQ headers（`coord-agent/src/services/outbox_relay.rs`）：
`ce-id`、`ce-type`、`ce-source`、`ce-specversion`、`ce-dataschema`、
`partitionkey`、`sequence`。

## 3. `data` 形状

所有生命周期事件的 `data` 都是同一份实例状态快照（`WorkflowRuntime::emit_lifecycle`）：

```json
{
  "workflowId": "<instance_id>",
  "workflowName": "<definition name>",
  "workflowVersion": "<semver>",
  "namespace": "<namespace>",
  "status": "running|waiting|suspended|completed|failed|cancelled|pending",
  "output": <any|null>,
  "fault": <object|null>
}
```

每个类型的 schema 额外固定 `event_type`/`dataschema` 两个常量，并对**运行时能保证**的
事件收紧 `data.status`（例如 `workflow.completed.v1` 只允许 `"completed"`）；
`task.*` 事件发生在实例可能处于 `running`/`waiting` 的多种相位，故只约束状态集合
（不做过紧约束 —— 过紧会让契约在合法场景下误报）。

## 4. 契约不变量（消费者可依赖）

1. **至少一次**：同一 `id` 可能到达多次；业务副作用必须以 `id` 为幂等键。
2. **per-instance 有序**：同一 `aggregate_id` 内 `sequence` 连续 +1；分区选择由
   `partitionkey = aggregate_id` 保证同实例事件落同一分区。
3. **重复不空洞**：允许重复，**不允许**缺席 —— 只要 outbox 里标了 `delivered`，消费者侧
   必然能看到该 `id`（本次 spike 的 `missing_event_ids = 0` 即此判据）。
4. **不得假设全局顺序**：`sequence` 只在实例内单调；跨实例无先后语义。
5. **`dead` 不是投递**：`status = dead` 表示重试耗尽、由人处置；消费者不应等到它。

## 5. schema 清单与校验

```bash
# 重新生成（唯一事实源：schema/generate.py 的清单）
python3 spike/2026-10/spike-3-recovery/schema/generate.py
python3 spike/2026-10/spike-3-recovery/schema/generate.py --check   # CI 用：与清单一致

# 对证据里每条 com.china-workflow.* 事件跑 JSON Schema 校验
python3 spike/2026-10/spike-3-recovery/scripts/schema_check.py
```

`schema/` 下 11 个文件（每个事件类型一个）：

```text
com.china-workflow.workflow.started.v1.json      com.china-workflow.task.started.v1.json
com.china-workflow.workflow.waiting.v1.json      com.china-workflow.task.completed.v1.json
com.china-workflow.workflow.suspended.v1.json    com.china-workflow.task.faulted.v1.json
com.china-workflow.workflow.resumed.v1.json      com.china-workflow.task.retried.v1.json
com.china-workflow.workflow.completed.v1.json
com.china-workflow.workflow.faulted.v1.json
com.china-workflow.workflow.cancelled.v1.json
```

`spike/2026-10/spike-3-recovery/scripts/schema_check.py` 是**fail-closed** 的：缺 schema、schema 与记录不符都以非 0 退出。

## 6. 与既有契约的关系（边界）

- 本草案**不**修改 `apis/contracts/proto/coord/` 的任何 proto，也不新增 RPC/字段；
- `docs/production/consumer-contract.md` §5 的 `Poll + Ack` 消费纪律继续适用；本草案只
  补充「事件长什么样」；
- `docs/adr/0013-workflow-approval-v2.md`（proposed）是后续形态决策的入口；本草案是它的
  输入之一（事件契约部分），不是替代。
- 命名空间 `com.china-workflow.*` 与既有 `io.serverlessworkflow.*` **并存**：前者是对外
  出口，后者是引擎内部常量。若阶段二决定只保留一个，应改
  `durable::external_event_type` 一处并把本文档与 schema 生成器一起改。

## 7. 缺口

- **E1（`data` 过粗）**：当前 `data` 是实例状态快照，审批场景需要的字段（轮次、任务实例、
  加签人、票型）尚未建模 —— 依赖 Spike 1 的状态模型 v2。本草案的 schema 会随之扩展
  （新增类型 + 字段），结构不变。
- **E2（无 `cancelled` 产出）**：`workflow.cancelled.v1` 的 schema 已就位，但当前
  `cancel_instance` 不发生命周期事件（不是本 Spike 的范围，属于既有行为），故证据里
  没有该类型的事件。
- **E3（CloudEvents 完整属性）**：记录是 CloudEvents 的**简化形态**（与
  `apis/contracts/proto/coord/event/v1/event.proto` 头注同口径）。换成完整属性封装
  属于 v1.2 增量，不在 Spike 内。
