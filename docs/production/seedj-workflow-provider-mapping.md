# seedj WorkflowProvider → coord workflow v2 映射

Owner: coord workflow ｜ Last verified: 2026-11-15

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
| `dueIn` | `deadline`（ISO8601 duration） | 到期只置 `Expired` 并从计票分母剔除，**不自动通过/驳回**（B-WF-5）；`WAITING` 恢复策略未实现。 |
| `allowCc` | `allowCc` | 缺省 `false`；仅任务级抄送读取。 |
| `allowAddAssignee` / `allowRemove` / `allowTransfer` / `allowDelegate` / `allowWithdraw` / `allowUrge` | 未建模 | 适配器按能力矩阵声明；不得默认放行。 |
| `formKey` / `formSchema` / `formData` | 未建模 | `Approve/Reject` 只带 `comment`；表单数据由 seedj 侧存储并在读视图合并。 |

## 3. 动作（`act`）

| seedj `WorkflowAction` | coord 命令 | 支持状态 |
|:--|:--|:--|
| `Approve` | `Approve` | ✅（`formData` / `occurredAt` 未建模） |
| `Reject(targetStep)` | `Reject` + `target_node` | ✅（原子投票 + 回退） |
| `Transfer` | `Transfer` | ✅（原责任人进 `transferred_from`，不携带投票记录） |
| `Delegate(delegateFor)` | `Delegate` | ⚠️ 无到期自动恢复；`delegateFor` 须由适配器用定时任务调用撤回复原 |
| `Withdraw(任务级)` | `WithdrawTask` + `target_node` | ✅（仅原审批人；保留通过记录；带否决票的席位拒绝撤回） |
| `WithdrawInstance` | `Withdraw` | ✅（仅发起人；级联取消实例） |
| `Urge` | `Urge` | ✅（只写审计/通知，不写历史） |
| `Cc` | `Cc` | ✅（任务级要求节点 `allowCc: true`；实例级限发起人/`sign_admins`） |
| `AddAssignee(position)` | `AddSign` | ⚠️ 只支持“向当前轮次加席位”；`BEFORE/AFTER` 派生任务、assignment 挂起/恢复未建模（#381） |
| `RemoveAssignee` | `RemoveSign` | ⚠️ 按席位 id；适配器先用 `GetTask`/`ListTasks` 把 assignee 映射到 task id；重算结果通过 `GetInstanceDetail` 读回 |
| `Resume(newDueIn)` | — | ❌ 未实现（无 `WAITING` 状态策略与 `Resume` RPC） |
| `Signal(name)` | `Signal` | ✅（`signal_name` 必须匹配挂起等待） |
| `Cancel` / `Terminate` | `Cancel` / `Terminate` | ✅（终态不可再变更） |

## 4. 查询 / 历史

| seedj | coord |
|:--|:--|
| `describe(instanceId)` / `status` | `GetStatus` + `GetInstanceDetail`（`revision` 为 CAS 守卫） |
| `describeTask(taskId)` | `GetTask`（席位 + votes + 实例状态） |
| `listTasks(TaskQuery)` | `ListTasks`：`instance_id` / `assignee` / `completed_by` / `candidate_group` / `node` / `status` / `due_before_ms` |
| `pendingFor` | `ListTasks{assignee, status=created|claimed}`（或 `ListPendingTasks`） |
| `search(WorkflowSearchQuery)` | `ListInstances`：`definition` / `status_filter` / `started_after_ms` / `started_before_ms` |
| `history(instanceId)` | `History`：`STEP_ENTERED` / `TASK_*` / `ROUND_*` / `INSTANCE_*`；`actionId` 当前为空 |
| `provider.now()` | 无独立时钟 RPC；响应时间戳均为 Unix ms，使用本地时钟即可 |
| `capabilities` | 由适配器按本表声明；未实现项必须报 `false`（或回退本地 provider） |

## 5. 已知缺口（跟踪）

- 前后加签（`AddAssigneePosition.BEFORE/AFTER`）、减签重算的 assignment 级视图、委派到期、
  `WAITING` + `Resume` → issue #381 剩余项；任务级撤回/催办/抄送已落地（见 §3）。
- seedj `HumanTask` 的 `predecessorTaskId` / `successorTaskId` / `spawnedByActionId` /
  assignment 级 `SUSPENDED|TRANSFERRED|DELEGATED` 状态：依赖 #381 的派生任务与
  assignment 状态机；当前只能由 task 的 `transferred_from` / `delegation` / votes
  近似映射。
- `History` 不含 `TASK_ASSIGNMENT_SUSPENDED/RESUMED`、`TASK_ADD_ASSIGNEE_BEFORE`、
  `TASK_CC`（对应能力未落地）。
