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
