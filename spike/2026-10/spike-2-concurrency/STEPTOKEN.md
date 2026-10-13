# Spike 2 · StepToken 方案（CAS key vs `request_id`）

> Last verified: 2026-10-13 ｜ 关联：`DESIGN.md`（单写者/租约/fencing）、手册 G2/G4/G6
> 现状锚点：`apis/contracts/proto/coord/kv/kv.proto`（`request_id` 语义）、
> `coord-core/src/workflow/ports.rs`（`TaskDispatcher::dispatch` 无幂等键）、
> `coord-core/src/workflow/runtime.rs`（`StepResult::Suspend`/`ExternalCall` 派发）。

## 1. 问题

外部副作用（`call` 节点）必须在**并发重试 / 崩溃恢复 / 多驱动者**下不重复执行。
本 spike 的租约 + fencing 保证「一实例一驱动」与「拒绝迟到状态写回」，但
**无法撤回已发出的外部调用**（`DESIGN.md` §5.2）。因此副作用去重必须有独立机制：StepToken。

## 2. 结论：用 CAS key，**不可**用 `request_id`

| 候选 | 结论 | 依据 |
|:--|:--|:--|
| KV `request_id` | **不可用** | kv.proto 明示：**单节点、进程内内存、TTL 60s、容量 4096 FIFO、不随 Raft 复制、不持久化**；键 = 客户端身份 + request_id。换节点/重启/跨客户端重试**会重复生效**。不具备跨节点、跨重启的幂等基础。 |
| CAS key（create-if-absent） | **采用** | `Txn(Compare(Version==0) → Put)` 原子创建唯一键；经 Raft 复制、持久化；首个写者胜出，天然跨节点/跨重启。 |

## 3. 推荐 key 布局

```text
/_workflow/v3/steps/{instance_id}/{step_id}
```

- `step_id`：**逻辑步骤的稳定标识**，由 `task_name`（+ 分支/轮次）派生，**不得**每次尝试换新值
  —— 否则去重失效。建议 `{task_name}`，若同一任务可多次执行（循环/驳回）再加轮次/索引。
- value：`{"token": "<stable>", "state": "pending"|"done", "at_ms": <i64>}`
  —— 两态记录（不是布尔），支持崩溃后判断「已登记但未完成」。
- 去重判定：`Txn(Compare(steps_key Version==0) → Put(pending))` 成功 ⇒ 首次；
  失败 ⇒ 已登记，按已记录 `state` 决定「已完成 → 跳过」或「pending → 重放（外部按 token 去重）」。

## 4. 去重时序：先登记后副作用

两种顺序的崩溃窗口：

| 顺序 | 崩溃窗口 | 后果 |
|:--|:--|:--|
| **先副作用后登记** | 副作用已发出、登记未落盘 | 重试重复副作用（除非外部按 token 去重） |
| **先登记后副作用**（推荐） | 登记 pending 已落盘、副作用未发出 | 重试看到 pending，用**同一 token** 重发；外部按 token 去重 ⇒ 不重复；若外部未收到过则正常执行 |

推荐流程（单写者租约下，见 `DESIGN.md`）：

1. `Txn(Compare(steps_key Version==0) → Put(pending))`，**与**提交该步骤的状态转换（drive 的 CAS/fencing 写）
   放在**同一 Txn**（否则两者之间崩溃会丢失 token 记录）—— 这需要把 StepToken 写纳入
   Spike 3 的「状态 + outbox 同事务」形状。
2. 派发外部调用，**携带 token 作为幂等键**。
3. 成功后将 steps_key 更新为 `done`。

## 5. 残余窗口与所需改动

| 残余风险 | 说明 | 所需改动 |
|:--|:--|:--|
| 外部系统不认幂等键 | CAS key 只能去重 **coord 侧**；外部重复调用仍可能发生 | 外部副作用接收方必须支持幂等键；否则只能「至少一次 + 下游去重」 |
| 派发签名无幂等键 | 现状 `TaskDispatcher::dispatch(service, with, input)` **没有**幂等键参数（手册 G6） | 扩展 dispatch 签名（或经 `with` 透传 `idempotencyKey`），由引擎从 StepToken 注入 |
| 步骤记录与状态写非同一 Txn | 两步写之间存在崩溃窗口，token 记录可能丢失 | 与 Spike 3 对齐：状态 + StepToken(outbox) 同事务 |
| token 稳定性 | 每次重试生成新 token 会让去重失效 | token 由 `(instance_id, step_id)` 确定性派生，跨重试不变 |
| `pending` 永久悬挂 | 副作用成功但 `done` 写失败 | `pending` + token 可重放（外部去重）；需超时/对账兜底 |

**未实现（本 spike 范围外）**：StepToken 的执行路径（登记/去重/状态两态）未接线；
本 spike 只提供结论与所需改动。这与手册 G6（派发无幂等键）一致。
