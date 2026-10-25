# 工作流语义（补偿 / 重试 / 信号 / 保留）

> Owner: maintainers ｜ Last verified: 2026-10-16

面向接入方的语义单一归属页（G-WF-1）。事实源：`coord.workflow.v1` 的 proto 声明
（wire 字段）、`coord-core/src/workflow/`（sw 编译器 + runtime）与
`ops/boundaries.md`（不承诺清单）；消费范式见 `consumer-contract.md` §7。

## 1. 补偿（`compensatedBy`）

- **编译**：状态上的 `compensatedBy: <state>` 编译为该状态的 TryCatch
  **catch-all** 转场（`<state>` 通常是 `type: compensate` 的单动作状态，也可是
  普通操作状态）。`onErrors` 与 `compensatedBy` 同用时按 catch 子句顺序路由，
  catch-all 兜底。
- **执行**：被包裹状态的动作失败（含 `call` 派发失败）⇒ 转入补偿状态并执行其
  动作；正常路径**不得**执行补偿。
- **逆序补偿**：引擎**不内置**自动逆序栈；逆序由 DSL 连线表达——把补偿状态
  串起来（`undo2` 的 `transition: undo1`），失败状态的 `compensatedBy` 指向
  链头。三步链路判据：
  `test_sw_three_step_failure_compensates_in_reverse_order_and_replays`
  （真 dispatcher；断言动作序列 `c1 → c2 → r2 → r1`；同 DSL 两个实例验证可
  重放）。
- **幂等是使用方的责任**：补偿动作可能与正常动作一样被重试/恢复再次触发，
  引擎不提供跨动作去重（与 at-least-once 消费同理——补偿动作需自身幂等）。
- **前置副作用会真的发生**：失败状态的 try 体动作在失败前已经执行（如 `c1`、
  `c2` 已成功）；补偿链负责撤销它们。这是 saga 的本义。

## 2. 重试与超时

- 重试由 `onErrors[].retry` / 任务级 retry 字段下线（编译器展开为 catch +
  转场/重试接线）；**副作用幂等责任同样在使用方**（`call` 派发不去重）。
- 超时（`timeout`，def 级 / 状态级）触发即走错误路径——可被 `onErrors` /
  `compensatedBy` 接住。
- 挂起（wait / call / listen）**不计入**解释器步数保护（步数只计状态推进）。

## 3. 信号 / 恢复 / 取消

- `Signal`：`signal_name` 必须匹配当前挂起等待（否则 `INVALID_ARGUMENT`）；
  携带 `idempotency_key` 时**重复 signal 不重复执行**（KV 幂等键）。
- `Cancel` / 恢复：对运行/等待/挂起实例有效；**终态实例再操作返回
  `FAILED_PRECONDITION`**（状态守卫）。取消**不自动触发补偿**——是否补偿由
  DSL 连线决定。
- `resume`（按序列恢复）：仅挂起/等待实例；运行中实例返回状态冲突。

## 4. 保留策略（G-WF-1）

实例与定义**不再是 append-only**：提供显式删除 API（管理路径，capability 见
`grpc_auth.rs`）；**没有**自动 TTL/归档（保留节奏由运维显式执行）。

| RPC | 守卫 | 违反时 | 删除后 |
|:--|:--|:--|:--|
| `DeleteInstance(workflow_id)` | 仅**终态**（COMPLETED / FAULTED / CANCELLED） | `FAILED_PRECONDITION`（先 Cancel） | `GetStatus` → `NOT_FOUND`；读-删竞态按幂等成功（终态一致：不存在） |
| `DeleteDefinition(ns, name, version)` | **无任何实例引用**该版本（含终态） | `FAILED_PRECONDITION`（先删实例） | `GetDefinition` / 指向该版本的回滚 → `NOT_FOUND`；同名同版本重新 `Deploy` = 全新定义 |

- 删除**不可回滚**（无归档副本）——先确认无恢复需求，再执行。
- **存储差异**：生产 `KvWorkflowStore` = KV `delete` + watch 删除事件驱动
  缓存失效 + 断连重连的对账**剪枝**（缓存条目不在 KV ⇒ 移除）；测试/热缓存
  `MemoryWorkflowStore` = 内存移除；已退役的 `RaftWorkflowStore` **不支持**
  删除（fail-closed `Unsupported`，不会被误认为已删除）。
- 判据：`ports.rs::test_memory_store_delete_definition_and_instance`、
  `workflow.rs::test_engine_delete_instance_requires_terminal_state`、
  `test_engine_delete_definition_guard_and_roundtrip`。

## 5. 实例状态速查

`PENDING → RUNNING ⇄ WAITING/SUSPENDED → COMPLETED | FAILED(FAULTED) | CANCELLED`；
`GetStatus.status` 字符串映射见 `grpc_handlers.rs`（`FAULTED` 即 Failed）。
子流程恢复的轮询边界见 `boundaries.md` B-WF-2 / B-WF-3。

**业务阶段（`GetStatus.current_state_name`）**：只回报 CNCF SW 定义里声明的稳定状态名。
引擎为编译期展开生成的内部任务名（`<状态>__route` / `<状态>__transition` /
`<状态>__action_<i>` 等）在读取时按实例钉死的定义归一化回声明状态名（issue #101），
消费方无需做前缀剥离。

- 读侧对**历史实例**同样生效（声明状态从 `do_tasks` 推导，无需数据迁移）；
  外部若已把 `<状态>__route` 落库，升级后重读 `GetStatus` 即得声明状态名，
  或按同规则去掉 `__<内部后缀>`。
- 遗留 `document/do` DSL 无「声明状态」概念，仍返回当前任务名（行为不变）。
- 原始引擎任务名（含内部名）保留在 `task_stack[].task_name`，供诊断，不做归一化。
- 判据：`sw.rs::test_declared_state_name_normalizes_internal_task_names`、
  `model.rs::current_task_name_reads_index_then_falls_back_to_last`、
  `workflow.rs::test_current_state_name_normalizes_internal_route_task`、
  `grpc_handlers.rs::test_get_status_reports_declared_state_name_not_internal_task`。

## 6. HTTP 任务派发：运行时表达式与凭据注入（`$context` / `$secrets` / `$constants`）

`call` / operation 任务的 `with` 支持 HTTP 调用（`endpoint` / `method` / `headers` /
`body` / `auth`）。凭据**不写进定义**（定义随 KV / 快照 / 备份流转，`GetDefinition`
可原样读出），而是在 agent 侧配置注入、只在进程内解析：

```yaml
- name: notify
  type: operation
  with:
    method: POST
    endpoint: "http://ims-approval:8083/api/internal/activate"
    auth: { scheme: bearer, token: "${ $secrets.approvalToken }" }
    body: { requestId: "${ $context.requestId }" }
```

- 占位符映射由 agent 配置 `[services.workflow_dispatch]` 注入：

```toml
[services.workflow_dispatch.secrets]
approvalToken = "..."       # key → 字符串

[services.workflow_dispatch.constants]
region = "cn-north-1"       # key → 任意 JSON 值（字符串/数字/布尔/数组/嵌套表）
retries = 3
```

- 缺省空映射 = 不注入（与无本配置段的既有行为一致）。
- **fail-closed**：解析后仍残留 `${ $secrets.<key> }` / `${ $constants.<key> }`
  ⇒ 派发前拒绝（`retryable: false`），错误信息点名缺失的 key（不含凭据值）；
  不会把占位符字面量当请求头发出去（否则远端 401/403 会被误判为对端问题）。
- 字符串常量按 JSON 序列化（带引号）替换，数字/布尔原样——与 `with_constants`
  既有语义一致。
- **运行时表达式**：`with` 的任一字符串位置（`body` / `headers` / `auth` / `query` /
  `endpoint`）支持**整串单段** `${ ... }`，在派发前求值；绑定 `$context`
  （= 实例上下文）与 `$input`（同值）——其余标准绑定（如 `$workflow` / `$runtime`）
  在 agent 派发层不可得，出现即 fail-closed。表达式结果替换整个字符串（可为
  对象/数字/布尔，不限于字符串）；`with` **无字符串插值**：内嵌 `${...}` 或一段
  字符串里含多段表达式一律拒绝，不会把占位符字面量发给对端。
- **与 `$secrets` / `$constants` 的先后**：含 `$secrets` / `$constants` 的字符串
  按配置注入处理（支持内嵌，如 `Bearer ${ $secrets.token }`），不参与运行时求值；
  两种形态混在同一字符串（如 `${ $context.a + $constants.b }`）⇒ fail-closed。

OAuth2 client_credentials（`auth.scheme: oauth2`）由 agent 取短 TTL 令牌后再
以 Bearer 发出：

```yaml
auth:
  scheme: oauth2
  tokenUrl: "http://ims-iam:8081/oauth2/token"
  clientId: "${ $secrets.hrClientId }"
  clientSecret: "${ $secrets.hrClientSecret }"
  scope: "hr.employee.activate"
```

- `clientId` / `clientSecret` **必须来自 `$secrets`**（不落定义存储，不写进日志 /
  错误信息）；令牌按 (tokenUrl, clientId, scope) 在进程内缓存，提前
  `min(30s, TTL/10)` 刷新（`expires_in` 缺失 / 非正 = 不缓存，每次取）。
- 令牌端点 5xx / 连接失败 / 超时 ⇒ `retryable: true`；4xx / 响应缺
  `access_token` / 非 JSON ⇒ `retryable: false`（与 `DispatchResult::Failure`
  语义一致）。错误信息只含字段名与 OAuth2 `error`/`error_description`（截断、
  `client_secret` 脱敏），不含凭据值。
- 判据：`test_http_dispatcher_secret_injected_into_authorization_header`、
  `test_http_dispatcher_missing_placeholders_fail_closed`、
  `test_http_dispatcher_context_expression_evaluated`、
  `test_http_dispatcher_expression_failure_fails_closed`、
  `test_http_dispatcher_oauth2_gets_token_and_sends_bearer`、
  `test_http_dispatcher_oauth2_token_cached_until_expiry`、
  `test_http_dispatcher_oauth2_token_refreshed_when_expired`、
  `test_http_dispatcher_oauth2_token_endpoint_failure_semantics`、
  `test_http_dispatcher_oauth2_missing_config_fails_closed`、
  `test_engine_context_expression_end_to_end`、
  `test_engine_new_with_dispatch_injects_secret_end_to_end`、
  `test_engine_new_with_dispatch_missing_secret_fails_closed_end_to_end`。

## 7. v2 人工审批（原生 `approve` 节点）

面向接入方的当前态口径。定案（决策、DSL 语法、授权矩阵、边界）单一归属
`docs/adr/0013-workflow-approval-v2.md`（D2/D7/D8/D9/D11），本页不复述定案，只给
接入所需的可消费面。

- **RPC（12 个，`coord.workflow.v1`）**：
  - 命令 10 个——`Approve` / `Reject` / `Acknowledge` / `AddSign` / `RemoveSign` /
    `ReturnToNode` / `Transfer` / `Delegate` / `Withdraw` / `Terminate`，能力
    `coord:workflow:execute`；
  - 查询 2 个——`ListPendingTasks` / `GetInstanceDetail`，能力 `coord:workflow:read`。
  - 命令请求末字段 `idempotency_key`；主体身份取自认证上下文（CCT），请求体不携带 actor。
    错误码映射：`StaleRevision → ABORTED`、`NotAuthorized → PERMISSION_DENIED`、
    `InvalidTransition → FAILED_PRECONDITION`。能力与错误码事实源见
    `coord-core/src/grpc_auth.rs` 与 `consumer-contract.md` §0.2 / §7。
- **DSL**：定义里用原生 `approve` 节点声明席位与策略（camelCase 键：`approvers / pool / readers /
  roles / mode / veto / deadline / onPass / onReject / signAdmins / allowSelfApprove`），未知键、
  非法 `mode`、不可换算 `deadline`、四席位来源全空均 fail-closed；语法逐字见 ADR-0013 D2。
- **动态席位（ADR-0014 D1）**：四个席位来源的单个条目可写 `${ ... }` 表达式，在节点开启时
  按实例上下文求值（`$input` / `$context` 同值；返回字符串或字符串数组）。空结果 / 非字符串
  元素 / 空主体 fail-closed 并点名来源键；`roles` 的表达式结果再经 `ApproverResolver` 展开：

  ```yaml
  - hr_review:
      approve:
        approvers: ["${ $input.hrApprovers }"]   # 启动 input 提供审批人集合
        readers: ["${ $context.auditor }"]
        roles: ["${ $input.approverRole }"]
  ```
- **挂起 / 恢复**：走到 `approve` 节点 ⇒ 实例 `SUSPENDED`，`SuspensionMeta` 给出
  `reason="approval"`、`expected_signal="approval.completed"`、`approval_instance_id` 与节点名
  （审批实例 id = workflow 实例 id）。分支本轮通过 ⇒ 按节点 `onPass` 恢复（缺省顺序推进下一
  任务）；实例级否决 ⇒ 有 `onReject` 则路由，否则以 `approval_rejected` 置 fault；取消/终止
  **不**按 `onReject` 路由，分别以 `approval_cancelled`/`approval_terminated` 置 fault
  （映射事实源 ADR-0013 D11）。重启对账**不**重驱动挂起实例，但重建 deadline 定时器。
- **授权**：引擎 `actor_can_act`（席位责任人/候选/被委派人）叠加桥的职责分离——默认发起人
  不可自批（`allowSelfApprove` 可开，匿名发起豁免）、加签/减签 = `signAdmins` ∪ 发起人、
  驳回至节点 = 未决席位持有人 ∪ `signAdmins` ∪ 发起人、终止 = `signAdmins`（内部
  `SYSTEM_ACTOR="system"` 恒可，用于运行时收口）。矩阵逐行见 ADR-0013 D7。
- **服务代终端用户（ADR-0014 D2）**：业务服务可在 10 个命令请求里填 `on_behalf_of`、
  在 `ListPendingTasks` 里填 `assignee`。调用方服务必须列在 agent 配置
  `services.workflow_on_behalf_services`（缺省空 = 不授权）；生效主体是被代表用户，
  席位归属判定不变。`VoteRecord.actor` / `AuditEntry.by` 记被代表用户，新增
  `acting_service` 记执行服务（经 `GetInstanceDetail` 的 votes/audit 可见）。
  保留主体 `system` 两侧都拒绝；未授权代操作在任何副作用前 `PERMISSION_DENIED`。
- **幂等**：命令按 `idempotency_key` 去重，记录键
  `/_workflow/v4/idempotency/{instance}/{key}`，与状态、本批事件**同一次 CAS 提交**写入；
  命中即原样返回首次响应，不重复执行（重放/重启后仍成立）。
- **事件（已固化）**：7 个类型，带 `.v1` 版本后缀——实例级
  `com.china-workflow.instance.returned|cancelled|terminated|completed|rejected.v1`、
  轮次级 `com.china-workflow.approval.round.passed|rejected.v1`。每个类型在
  `apis/contracts/schemas/events/<type>.json` 有 JSON Schema（`$id` =
  `https://schemas.coord.dev/events/<type>.json`），由 `scripts/check-event-schemas.sh`
  门禁（catalog ↔ schema 双向一致）。事件仍写 v3 outbox 传输设施；消费者按
  `(instance_id, sequence)` 去重（`sequence` 每实例单调）。
- **deadline**：节点 `deadline` 换算为席位的绝对截止，由**持久化定时器**驱动过期；到期只把
  席位置 `Expired` 并从计票分母剔除，**绝不自动通过/驳回**——全员过期或被减签的轮次保持
  `Open`，处置由人（`AddSign` 或 `Terminate`）决定。**混合轮次**（部分席位已批、部分过期）里
  过期席位从 `total` 剔除；剔除后剩余席位全部通过 ⇒ 本轮**判通过**（与「全员过期保持
  `Open`」区分）。重开（`ReturnToNode`）的席位不设 deadline。
- **启用条件**：审批桥与审批聚合仅在 durable 工作流引擎装配时注入。`coord dev` 是
  Server + Agent 一体化拓扑，ADR-0014 D3 起 `workflow_durable` 在 dev 预设中默认开启
  （审批面开箱可用）；生产默认仍关闭。未开 `workflow_durable` 时审批 RPC 返回
  `FAILED_PRECONDITION`（例外：保留 actor `system` 的 `Terminate` 先被授权拒绝为
  `PERMISSION_DENIED`），且 `approve` 节点在驱动时 fail-closed（实例 `Failed` +
  `workflow.faulted`）——绝不静默降级为无审批继续。生产启用示例：

  ```toml
  [services]
  workflow = true
  workflow_durable = true
  mq = true
  # 服务代终端用户白名单（CCT subject；空 = 不授权）
  workflow_on_behalf_services = ["ims-approval"]
  ```
- **存储与写路径**：状态单文档 `/_workflow/v4/instances/{instance_id}`，与 v1 的
  `/_workflow/v3/instances/` 扫描面互不可见；唯一写路径带 CAS
  （`ExpectedRev::{CreateOnly, Exact(mod_rev)}`，无无守卫写入形态），状态与本批事件同一
  KV Txn 提交。布局与谓词见 ADR-0013 D3/D8。
- **查询与分页**：`ListPendingTasks` 默认查调用方自己；服务代查时填 `assignee`（须在白名单内），
  按实例 id 字典序跨实例扫描 + 分页（`page_size` 0 ⇒ 默认 50；`page_token` = 上一页最后实例
  id；硬上限 200）；`GetInstanceDetail` 返回实例快照（branches / tasks(含 votes) / rounds /
  audit + `revision`，`revision` 为 CAS 守卫值）。

**已知未落地（当前口径）**：

- 未实现 D8 的 watch 驱动按实例有界缓存：KV 实现按实例做线性一致单键读，无缓存/淘汰；
- 无 UI 审批面、无 REST facade（`coord-ui` 仅影响评估）；
- `roles` 的角色名可来自运行时表达式，但角色→审批人仍是启动/命令路径注入的静态
  `ApproverResolver`；外部解析服务未接线；
- `ListPendingTasks` 为全 v4 前缀扫描（无按年龄/actor 的索引），规模上界见 `ops/boundaries.md`
  的 `B-WF-*`。

## 8. v2 集群（KV-T）语义

- **公共语义不变**：接入方仍使用 `coord.workflow.v1`；Agent 在 `data_plane = "v2"` 时
  把内部 KV 持久化面（工作流存储、审批聚合、outbox）经 `coord-client` 适配层写到
  `coord.txn.v2` / 读 `coord.kv.v2`，业务状态机与审批语义不因集群形态改变。
- **可用**：纯 KV 路径（`workflow = true`、`workflow_durable = false`）——真实 v2
  集群判据 `KvTV2ClusterIntegrationTest#workflowPureKvPathCompletesOnV2`（inject → end
  在无 MQ 的 v2 集群上完成）。
- **不可用（fail-closed）**：MQ v1 在 v2 集群冻结（`boundaries.md` B-KVT-6）；`data_plane =
  "v2"` 时 `services.mq = true` 或 `services.workflow_durable = true` **启动期拒绝**，
  不静默降级到非 durable/无投递面。判据：
  `agent_workflow_durable_gate_test::test_v2_data_plane_rejects_mq`、
  `test_v2_data_plane_rejects_workflow_durable`。
- **审批面**：审批桥与聚合只在 durable 引擎装配时注入（见上一节）；v2 上 durable 被拒，
  因此审批 RPC 保持非 durable 集群的 `FAILED_PRECONDITION` 口径，**不**承诺 v2 审批可用。
- 能力矩阵见 ADR-0015 D13；MQ v2 属 G7（#178），KV-T GA 前不提供。
