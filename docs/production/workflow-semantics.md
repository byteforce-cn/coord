# 工作流语义（补偿 / 重试 / 信号 / 保留）

> Owner: maintainers ｜ Last verified: 2026-10-14

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

## 6. HTTP 任务派发与凭据注入（`$secrets` / `$constants`）

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
- `$context` / `$input` 等运行时绑定由表达式求值器处理；本配置只影响 `with` 内的
  `$secrets` / `$constants` 占位符。
- 判据：`test_http_dispatcher_secret_injected_into_authorization_header`、
  `test_http_dispatcher_missing_placeholders_fail_closed`、
  `test_engine_new_with_dispatch_injects_secret_end_to_end`、
  `test_engine_new_with_dispatch_missing_secret_fails_closed_end_to_end`。

## 7. v2 审批模型与存储（未接线，默认关闭）

> 状态：**未接线**——尚无 proto/RPC、无 DSL `approve` 节点构造、无授权判定，也没有生产装配点，
> 新代码当前只被测试驱动。**契约（RPC、事件 `.v1` 后缀与 JSON Schema）与授权模型归 M2-C**，
> 经 EIS/ICPS 确认后固化；本节只描述已落库的模型与存储事实，不承诺 M2-C 行为。

- **模型（ADR-0013 D2）**：五层实体 = `Instance`（实例）/ `Branch`（节点实例）/ `Task`（席位）/
  `Round`（轮次）/ `VoteRecord`（内嵌于席位），加 task > branch > instance 三级变量作用域；
  分支通过不自动收口实例（驱动方在定义末端调用收口），否决是终局。判定顺序与状态转换的冻结
  证据在 `spike/2026-10/spike-1-state-model/docs/TRANSITIONS.md`（只读，仅链接不复制），当前
  口径见 `coord-core/src/workflow/approval/` 的模块 rustdoc。
- **存储（ADR-0013 D3/D8）**：状态单文档 `/_workflow/v4/instances/{instance_id}`，与 v1 的
  `/_workflow/v3/instances/` 扫描面互不可见；伴生事件仍走 `/_workflow/v3/outbox/**`。唯一写
  路径带 CAS（`ExpectedRev::{CreateOnly, Exact(mod_rev)}`），且状态与本批事件在同一 KV Txn
  提交——谓词不成立即零写入，调用方重读后重试。
- **事件类型（草案）**：`APPROVAL_EVENT_TYPES` 当前 7 个——实例级
  `com.china-workflow.instance.returned` / `.cancelled` / `.terminated` / `.completed` /
  `.rejected`，轮次级 `com.china-workflow.approval.round.passed` / `.rejected`。**这些名称尚
  无 `.v1` 后缀、无 JSON Schema，是草案**；M2-C 经 EIS/ICPS 确认后固化正式契约，本里程碑不
  接线，不会有生产事件流出。
- **已知未落地（当前口径）**：
  - 未实现 D8 的 watch 驱动按实例有界缓存：KV 实现按实例做线性一致单键读，无缓存/淘汰；
  - 被作废轮次的阅签（`read_ack`）席位保持 open（与 Spike 1 原型一致，阅签不计票）；
  - `open_tasks_for` 是单实例内存投影，没有跨实例待办索引（待办查询 RPC 归 M2-C）。
