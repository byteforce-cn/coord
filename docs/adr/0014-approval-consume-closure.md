# ADR-0014: 审批面消费闭环 —— 动态席位来源 / 服务代终端用户 / 一体化启用

- 状态：accepted
- 日期：2026-10-17
- 关联：ADR-0001（默认关闭）、ADR-0009（dev 预设）、ADR-0013 D2/D7/D11（审批 v2）

## 背景

ADR-0013 交付了原生 `approve` 节点与 12 个审批 RPC，但三个消费侧缺口使下游
（BFF → `ims-approval` → coord）无法按「实例、按人」消费审批面：

1. 席位来源只能是定义字面量或启动配置里的静态角色映射；业务服务的审批人是运行期
   生成的 UUID / 组织维度解析结果，写不进定义或 coord 配置；
2. 命令与待办查询的主体只能取调用方 CCT。业务服务持有自己的服务身份，既不能按终端
   用户查待办，也不能代其批准；
3. `workflow_durable` 在一体化 `coord dev`（Server + Agent 同进程）里默认关闭，
   `approve` 节点与审批 RPC 在单容器部署里不可用。

本 ADR 固化这三条能力面的当前口径；细节语义仍以 ADR-0013 为单一归属，本文只记录增量。

## 决定

### D1 席位来源支持运行时表达式（`$input` / `$context`）

- `approve` 节点的四个席位来源（`approvers` / `pool` / `readers` / `roles`）中，
  **单个条目**可以写成 `${ ... }` 运行时表达式，在**节点开启时**按实例上下文求值。
  `$input` 与 `$context` 绑定同一实例上下文（与 `with` 的求值口径一致）。
- 表达式可返回主体字符串或主体数组；数组元素必须全部是字符串。空数组、`null`、
  非字符串元素与空主体一律 fail-closed，错误信息点名来源键（`approve.approvers` 等）。
- `roles` 的表达式结果先展开为角色名，再经 `ApproverResolver` 解析为审批人；角色
  无映射仍按既有口径 `approve.role.<name>` 拒绝。外部解析服务不在本决定内。
- 席位来源展开为空 / 没有任何可计票席位时拒绝开节点：绝不静默少派席位，也绝不让
  空轮次自动通过。

### D2 服务代终端用户（on-behalf-of）

- 协议增量（`coord.workflow.v1`，Minor 兼容）：
  - 10 个命令请求新增 `string on_behalf_of`（空 = 直接以 CCT 主体执行）；
  - `WorkflowListPendingTasksRequest` 新增 `string assignee`（空 = 调用方自身）；
  - `ApprovalTask` 新增 `repeated ApprovalVote votes`；
    `WorkflowGetInstanceDetailResponse` 新增 `repeated ApprovalAudit audit`。
- 授权两层：
  1. 调用方服务必须列在 agent 配置 `services.workflow_on_behalf_services`
     （CCT 主体白名单；缺省空 = 不授权任何代操作）；
  2. 生效主体是**被代表用户**，引擎的席位归属判定（责任人 / 候选 / 被委派人）
     原样生效。服务身份只解锁「代谁」，不改变「谁能批」。
- 直接操作（`on_behalf_of` 为空或等于调用方主体）不要求白名单。
- 保留内部主体 `system` 在调用方与被代表主体两侧都拒绝（`PERMISSION_DENIED`）；
  未获授权的代操作在任何副作用之前拒绝。
- 审计：`VoteRecord.actor` / `AuditEntry.by` = 被代表用户；
  新增 `acting_service` = 执行服务。二者经 `GetInstanceDetail` 的 votes/audit 暴露。
- 幂等键语义不变：同 `idempotency_key` 重放返回首次响应，不重复执行/计数。

### D3 一体化部署默认启用 durable 审批面

- `coord dev` 是单命令拉起 Server + Agent 的一体化拓扑，KV 与 MQ 前置天然满足；
  `ServiceConfig::dev_mode()` 将 `workflow_durable` 置为 `true`，审批面开箱可用。
- **生产默认仍关闭**（ADR-0001）：显式 `workflow_durable = true` 仍需 Server 连接 +
  `services.mq = true`，缺一拒绝启动。
- 验收证据：`coord/tests/dev_mode_services_test.rs` 的
  `dev_mode_approval_flow_is_durable_end_to_end`（部署含 `approve` 节点的定义 →
  Start → `SUSPENDED` → ListPendingTasks → Approve → `COMPLETED`），以及
  `coord/tests/approval_rpc_test.rs` 的真 Raft durable 套件。

## 后果

- 同一份定义可由启动输入把席位派给不同主体，不需要为每个实例改定义或重启 coord。
- 业务服务可按终端用户查待办并代其批准；越权/未授权路径 fail-closed 且零副作用。
- `coord dev` 与生产显式开启共享同一 durable 装配；生产默认关闭的治理口径不变。
- 未实现（明确不在承诺内）：外部审批人目录的 HTTP 解析服务；按组织的自动席位推导；
  watch 驱动的实例缓存（仍以 ADR-0013 D8/D9 为准）。

## 参考

- 语义单一归属：`docs/adr/0013-workflow-approval-v2.md`（D2/D7/D8/D11）
- 接入面：`docs/production/workflow-semantics.md` §7、
  `docs/production/consumer-contract.md` §7
- 配置：`services.workflow_on_behalf_services`、`services.workflow_approval_roles`
- 判据：`coord-core/src/workflow/approval/bridge.rs`（动态席位）、
  `coord-agent/src/services/grpc_handlers.rs`（代理授权/审计）、
  `coord/tests/dev_mode_services_test.rs`（一体化 e2e）
