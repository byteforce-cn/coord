# v2 审批事件 JSON Schema

本目录是 v2 审批引擎事件契约的**单一归属**：每个已固化的事件类型一个 `draft 2020-12`
JSON Schema 文件，文件名 == 事件类型 + `.json`。

## 契约来源

- 事件类型清单：`coord-core/src/workflow/approval/events.rs` 的 `APPROVAL_EVENT_TYPES`
  （仅含引擎 `emit` 调用点会发出的 7 个类型）。
- 记录字段与 `dataschema`：`coord-core/src/workflow/durable.rs` 的 `OutboxRecord::lifecycle`
  与 `dataschema_for`。
- 命名、`partitionkey`/`sequence` 去重口径见 `docs/adr/0013-workflow-approval-v2.md`（D4）。

## 约定

- `$id` == `https://schemas.coord.dev/events/<type>.json`，即 `dataschema_for(type)` 的产物。
- `required` 只钉「记录投递时必带」的字段；outbox 记录的内部字段（`aggregate_id`、
  `sequence`、`created_at_ms`、投递状态等）由 `additionalProperties: true` 放行——消费者
  前向兼容：新增内部字段不破坏本契约，也不因缺失而误拒。
- 记录字段名为 `event_type`（对内序列化名）；其 CloudEvents 属性名是 `type`，两者一一对应。
- 事件类型带 `.v1` 版本后缀：一旦流出即对外契约，只能新增 `.v2`，不得原地改名。

## 门禁

`scripts/check-event-schemas.sh`（CI lint job）fail-closed 校验：从 `events.rs` 提取类型
清单（提取为空即失败），断言每个类型恰有一个 schema、`$id` 与 `dataschema_for` 一致、JSON
可解析，且目录内没有 catalog 之外的孤儿 schema。新增事件类型时必须同时补 schema。
