# 工作流语义（补偿 / 重试 / 信号 / 保留）

> Owner: maintainers ｜ Last verified: 2026-10-09

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
