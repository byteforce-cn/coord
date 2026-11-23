# seedj WorkflowProvider → coord workflow v2 映射

Owner: coord workflow ｜ Last verified: 2026-11-22

本文件是 `seedj-workflow-coord` 适配器的字段映射口径。coord 的承诺面以
`workflow-semantics.md` 与 `coord-proto/src/proto/workflow.proto` 为准；**本文件未列出的
seedj 字段一律视为不支持**——适配器必须 fail-closed（拒绝注册/动作），不得静默忽略。

## 1. 定义（`register` / `findDefinition`）

| seedj | coord | 说明 |
|:--|:--|:--|
| `StepKind.ACTIVITY` | `do[]` 中的 `call`（HTTP/gRPC/服务派发）或 `set` | 纯计算/变量赋值用 `set`；外部副作用用 `call`，幂等由调用方负责。 |
| `StepKind.HUMAN_TASK` | `approve` | 字段映射见 §2。 |
| `StepKind.WAIT_SIGNAL` | `listen`（事件）或 `wait`（定时） | `Signal` RPC 按 `signal_name` 恢复；`listen` 的 `event_type` 与 signal 名不是同一字段，适配器需在实例启动后按 `expected_signal` 对齐。 |
| `StepKind.COMPENSATION` | `compensatedBy` + 逆序状态链 | 引擎**不内置**自动逆序栈，逆序由 DSL 连线表达（见 `workflow-semantics.md` §1）。 |
| `dependsOn` DAG | `do[]` 顺序 + `switch.transition`；并行用 `fork` / `join` | coord 没有通用 DAG 调度器。菱形/跨层依赖必须展开为 fork/join + transition；无法保持语义时**拒绝注册**。 |

- 步骤级 `RetryPolicy(maxAttempts, initialBackoff, maxBackoff, multiplier)` → 任务级
  `retry{limit, delay, backoff: exponential, maxDelay, multiplier}`：四字段 1:1
  （`maxAttempts→limit`、`initialBackoff→delay`，两者都含首次；`maxBackoff→maxDelay` 封顶；
  `multiplier` 为指数底数，缺省 2.0）。`maxDelay` / `multiplier` 为 additive DSL 字段。
- `register` → `Deploy`：接受 JSON 或 YAML；同 `(namespace, name, version)` 同内容幂等、
  不同内容 `ALREADY_EXISTS`（定义不可变；变更请提升 version）。
- `findDefinition(id, version)` → `GetDefinition(namespace, name, version)`；响应带
  `definition_hash`（与实例钉死哈希同口径）与 `definition_json`（结构化模型）。
- 定义版本是字符串精确匹配；seedj 的 `major.minor` 可直接作为 version（coord 不强制格式，
  但排序/比较是字符串语义，适配器负责规范化为零填充或用 `ListDefinitionVersions`）。

## 2. `HumanTaskSpec` → `approve` 节点

| seedj | coord `approve` 键 | 备注 |
|:--|:--|:--|
| `assignees` | `approvers` | 单个主体列表；表达式条目支持 `${ ... }`。 |
| 候选组 / 部门 | `roles`（经 `ApproverResolver` 展开）或 `pool` | coord 无 GROUP/DEPARTMENT 类型；适配器把组解析为 `roles` 或候选主体列表。 |
| `signCompletion.AllApprove` | `mode: all` | 缺省。 |
| `signCompletion.AnyApprove` | `mode: any` | |
| `signCompletion.AtLeastCount(n)` | `mode: "quorum:<n>"` | n > 0。 |
| `signCompletion.AtLeastPercentage(p)` | `mode: "ratio:<num>/<den>"` | 通过票数按**向上取整**；`p` 需转为最简分数。 |
| `rejectCompletion.AnyRejectTerminates` | `reject: anyReject`（或 `veto: true`） | |
| `rejectCompletion.AtLeastRejects(n)` | `reject: "atLeastRejects:<n>"` | |
| `rejectCompletion.RecordAndContinue` | `reject: recordAndContinue` | 剩余席位不可能满足通过条件时立即回退（`SIGN_COMPLETION_UNSATISFIABLE`）。 |
| `rejectTargets` | 已进入节点白名单 | coord 的驳回目标必须是**已到达**的节点；更宽的 per-task 白名单未建模，适配器需自行校验后映射。 |
| `dueIn` | `deadline`（ISO8601 duration） | 缺省只置 `Expired` 并从计票分母剔除，**不自动通过/驳回**（B-WF-5）；seedj 的默认「超时失败」映射为 `timeoutPolicy: fail`，`metadata.timeoutPolicy=WAITING` 映射为 `timeoutPolicy: waiting`（ADR-0013 D13）。 |
| `allowCc` | `allowCc` | 缺省 `false`；仅任务级抄送读取。 |
| `allowAddAssignee` / `allowRemove` / `allowTransfer` / `allowDelegate` / `allowWithdraw` / `allowUrge` | 未建模 | 适配器按能力矩阵声明；不得默认放行。`allowAddAssignee` 的 `position` 由 `AddSign.position` 表达。 |
| `formKey` / `formSchema` / `formData` | **不支持（不扩承诺）** | ADR-0013 D9 明确不做业务表单/业务数据存储；`Approve/Reject` 只带 `comment`。表单数据由 seedj 侧存储并在读视图合并，coord 不在 wire 上新增 `formData`。 |

## 3. 动作（`act`）

| seedj `WorkflowAction` | coord 命令 | 支持状态 |
|:--|:--|:--|
| `Approve` | `Approve` | ✅（`formData` 按 D9 不支持，由 seedj 侧存储；时间戳取 coord 服务端时钟） |
| `Reject(targetStep)` | `Reject` + `target_node` | ✅（原子投票 + 回退） |
| `Transfer` | `Transfer` | ✅ 受让方仅支持**单一 USER principal**（原责任人进 `transferred_from`，不携带投票记录）。`ParticipantSpec(ROLE/GROUP/DEPARTMENT/EXPRESSION)` 由适配器 fail-closed——coord 不在动态动作上展开参与者组。 |
| `Delegate(delegateFor)` | `Delegate` + `delegate_for_ms` | ✅ 被委派人仅支持单一 USER principal；组/部门/角色由适配器 fail-closed。到期由审批 deadline 定时器撤销代理权（`DELEGATION_EXPIRED`）；终态撤销代理 assignment。 |
| `Withdraw(任务级)` | `WithdrawTask` + `target_node` | ✅（仅原审批人；保留通过记录；带否决票的席位拒绝撤回） |
| `WithdrawInstance` | `Withdraw` + `target_node` | ✅（仅发起人；级联收口实例）。`target_node` 非空时随 `Cancellation` 持久化并投影 `INSTANCE_ROLLED_BACK{from_node,to_node}`；活动席位落 `withdrawn`、assignment reason `WITHDRAWN`。目标为空 = 不投影回退事件。 |
| `Urge` | `Urge` | ✅（只写审计/通知；`History` 投影 `TASK_URGED`） |
| `Cc` | `Cc` | ✅（任务级要求节点 `allowCc: true`；实例级限发起人/`sign_admins`） |
| `AddAssignee(position)` | `AddSign` + `position`/`task_id`/`action_id` | ✅ `CURRENT` / `BEFORE` / `AFTER`；响应 `derived_task_ids`；BEFORE 挂起原席位、完成后恢复；AFTER 原轮次通过后执行、否决/回退时丢弃（`TASK_AFTER_SPAWN_DISCARDED`）。 |
| `RemoveAssignee` | `RemoveSign` + `assignees`/`task_id` | ✅ 按 assignee 移除未决席位（候选池移除单个候选），响应 `removed_task_ids` + 重算后的 `round`。 |
| `Resume(newDueIn)` | `ResumeWorkflow` | ✅ 仅 `WAITING` 有效；按上一轮席位重建任务并写新截止（`timeoutPolicy: waiting` 超时后）。时间预算取步骤级 `task_meta.timeout`（seedj `StepDefinition.timeout`），未声明时回退 `approve.deadline`。coord 工作流仍为 `SUSPENDED`（审批挂起），审批聚合与 seedj `RUNNING` 对齐。 |
| `Signal(name)` | `Signal` | ✅（`signal_name` 必须匹配挂起等待） |
| `Cancel` / `Terminate` | `Cancel` / `Terminate` | ✅（终态不可再变更） |

## 4. 查询 / 历史

| seedj | coord |
|:--|:--|
| `describe(instanceId)` / `status` | `GetStatus` + `GetInstanceDetail`（`revision` 为 CAS 守卫） |
| `describeTask(taskId)` | `GetTask`（席位 + votes + 实例状态 + `origin`/`predecessor`/`successor`/`spawned_by`/`assignments` + 时间戳）。task 带 `updated_at_ms` / `completed_at_ms`，round 带 `opened_at_ms` / `closed_at_ms`，assignment 带 `assigned_at_ms` / `resolved_at_ms`（未终态为 0）——适配器 1:1 填 seedj `HumanTask` / `TaskAssignment` |
| `listTasks(TaskQuery)` | `ListTasks`：`instance_id` / `assignee` / `completed_by` / `candidate_group` / `node` / `status` / `due_before_ms` |
| `pendingFor` | `ListTasks{assignee, status=created|claimed}`（或 `ListPendingTasks`） |
| `search(WorkflowSearchQuery)` | `ListInstances`：`definition` / `status_filter` / `status_in[]` / `started_after_ms` / `started_before_ms`；seedj `RUNNING` 传 `["RUNNING","SUSPENDED"]`（审批挂起在 seedj 仍是 RUNNING），过滤与分页由服务端完成 |
| `history(instanceId)` | `History`：`STEP_ENTERED` / `TASK_*` / `ROUND_*` / `INSTANCE_*` + `TASK_URGED` / `TASK_CC` / `TASK_WITHDRAWN` / `TASK_ASSIGNEE_REMOVED` / `TASK_ASSIGNMENT_SUSPENDED|RESUMED` / `TASK_ADD_ASSIGNEE_BEFORE|AFTER` / `TASK_AFTER_SPAWN_DISCARDED` / `DELEGATION_EXPIRED`；`actionId` 当前为空（派生任务用 `spawned_by_action_id`） |
| `provider.now()` | 无独立时钟 RPC；响应时间戳均为 Unix ms，使用本地时钟即可 |
| `capabilities` | 由适配器按本表声明；未实现项必须报 `false`（或回退本地 provider） |

**SDK 门面**：`coord-java-sdk`（main，尚未发版）提供 `listTasks(TaskQueryFilter, pageSize, pageToken)`
/ `getTask` / `history` 与 `urge` / `cc` / `withdrawTask`（含 `*OnBehalfOf` 变体）。适配器走
SDK 而不是自行发 gRPC。`listTasks` 是管理面读（`coord:workflow:read`，空过滤跨实例扫描），
`assignee` 不构成授权收窄；分页必须回传 `TaskPage.nextPageToken`，否则只能读到第一页。

## 5. 已知缺口（跟踪）

- **引擎/契约面无未落地项**：#381 的剩余项（前后加签、assignment 读面、按 assignee 减签、
  委派到期、`WAITING` + `Resume`）已随 ADR-0013 D13 落地。
- **SDK 发版**：任务读面、任务级命令，以及 `status_in[]`、`withdraw(targetNode)` 与
task/round/assignment 时间戳随 `coord-java-sdk` 0.4.5 源码发布。适配器使用前按实际 SDK
版本核对。
- `formData`：按 ADR-0013 D9 **不实现**，由 seedj 侧存储并在读视图合并（见 §2）。
- `History.actionId` 只有命令携带 `idempotency_key` 时非空；系统动作（超时/到期）与
  未提供键的命令为空。
- 动态 `Transfer` / `Delegate` 不接受参与者组：seedj 的 `ParticipantSpec(ROLE/GROUP/
  DEPARTMENT/EXPRESSION)` 由适配器 fail-closed。组/部门的展开只存在于 `approve` 节点的
  `roles` / `pool`（§2）。
- 无审批聚合的实例（纯 ACTIVITY / WAIT / LISTEN）：`Terminate` 回退到工作流 runtime，
  `GetStatus.status=FAULTED` 且 `approval_status` 为空，`fault.title="terminated"`；
  `History` 投影 `INSTANCE_STARTED` + `INSTANCE_TERMINATED`（不读审批聚合）。
