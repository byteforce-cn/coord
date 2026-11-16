# coord-java-sdk CHANGELOG

格式参照 [Keep a Changelog](https://keepachangelog.com/)。版本号与 `pom.xml` 同步；未发布的
改动记在 `Unreleased`，发版时整体归入对应版本。

## [Unreleased]

### Added — workflow 任务读面与任务级命令（issue #401）

- `WorkflowClient.listTasks(TaskQueryFilter, int, String)` → `TaskPage`：暴露 `ListTasks` 的
  七个过滤维度（`instanceId` / `assignee` / `completedBy` / `candidateGroup` / `node` /
  `status` / `dueBeforeMs`），并返回 `nextPageToken`（既有 `listPendingTasks` 丢弃了该
  token，新方法不再丢弃）。
- `WorkflowClient.getTask(String)` → `TaskDetail`：席位 + votes + `assignments` + 实例状态；
  `task_id` 服务端全局唯一，不要求 instance id。
- `WorkflowClient.history(String)` → `List<ApprovalHistoryEvent>`：`attributesJson` 原样透传
  （SDK 不解析、不重排）。
- `WorkflowClient.urge` / `cc` / `withdrawTask` 及其 `*OnBehalfOf` 变体，与既有审批命令族
  （`approve` / `reject` / `terminate` …）同构：`idempotencyKey` 原样透传，重放返回首次响应。
- `ApprovalTask` 增加读面字段（`instanceId` / `node` / `origin` / `createdAtMs` /
  `predecessorTaskId` / `successorTaskId` / `spawnedByActionId` / `assignments`）；既有 8/9 参
  构造器保持兼容，`GetInstanceDetail` 投影下这些字段为空（该 wire 消息不携带它们）。

### Notes

- `listTasks` 是 `coord:workflow:read` 门禁的**管理面读**：空过滤会跨实例扫描，`assignee`
  只是过滤条件而非授权收窄；分页单位是实例，单页任务数可能大于 `pageSize`；带 `instanceId`
  时不分页、返回该实例全部匹配。
- 未改 proto、未改 wire：`coord.workflow.v1` 的两个 proto 副本保持逐字一致。

### Added — workflow 动态动作与实例读面（issue #404）

- `WorkflowClient.resumeWorkflow` / `resumeWorkflowOnBehalfOf`：`WAITING` 超时策略下按新的相对
  截止重建席位（`new_due_in_ms`）；只有发起人 / sign admin 可恢复。
- `WorkflowClient.delegate(..., Long delegateForMs, ...)` 与 `delegateOnBehalfOf` 重载：发送
  `delegate_for_ms`（`null` / `0` = 不自动到期）；既有 5 参方法语义不变。
- `WorkflowClient.addSign(..., AddAssigneePosition, taskId, actionId)` 与对应 `addSignOnBehalfOf`
  重载：前后加签的 `position` / 锚定 `task_id` / 客户端 `action_id`（派生任务的
  `spawnedByActionId`）全部上 wire；`CURRENT` 等价于既有方法。
- `WorkflowClient.removeSign(..., List<String> assignees, String taskId)` 与对应
  `removeSignOnBehalfOf` 重载：按人减签（`assignees`，与 `task_ids` 互斥）+ 定位轮次的锚定
  `task_id`。
- `WorkflowClient.listInstances(namespace, definition, status, startedAfterMs, startedBeforeMs,
  pageSize, pageToken)` → `InstancePage`：启动时间窗（含边界，`0` = 不限）与 `nextPageToken`
  回传；既有 `List` 返回签名的两个重载保持不变。
- `WorkflowClient.getDefinition(namespace, name, version)`：按定义三元组直取（`WorkflowGetDefinitionRequest`
  的 ns/name/version 字段）；按 `workflowId` 的旧方法保留。

### Notes

- 命令类方法保持「一次调用一个实例状态」：`resumeWorkflow` 的新截止 / 重建席位、`delegate`
  的到期时间、`addSign` 的派生席位、`removeSign` 的重算轮次都不在返回值里；需要这些细节时
  走读面（`getInstanceDetail` / `listTasks` / `history`）。
- `addSign` 的 `BEFORE` / `AFTER` 必须给 `taskId`；缺锚点由服务端以 `INVALID_ARGUMENT` 拒绝
  （SDK 不做本地校验，保持与服务端口径一致）。
