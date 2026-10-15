# Spike 1 · 状态模型 v2 字段清单（WorkflowInstance v2）

> Owner: Spike 执行（AI 协作者）｜ Last verified: 2026-10-13
> 输入：**PROVISIONAL** — 手册 §3.2 语义表（EIS/ICPS 正式清单 10-18 到位后复核）
> 实现：`crate/src/model.rs`（唯一实现）｜ 转换规则：`TRANSITIONS.md`｜ 缺口：`GAPS.md`

## 0. 设计口径

- 实体：Instance（实例）/ Branch（分支=节点实例）/ Task（席位的审批任务）/
  Round（轮次）/ VoteRecord（投票记录，内嵌于 Task）。
- 纯内存、事件驱动、确定性：ID 内部生成、时间戳与操作者由调用方注入。
- 每条命令带 `expected_revision` 乐观并发守卫（对应 G2 的 CAS 写路径，
  Spike 2 接 KV Txn 时复用同一语义）。
- 与手册 §4 清单的偏离见 §3；全部为显式判断，不是遗漏。

## 1. 实体字段

### 1.1 Instance（流程实例）

| 字段 | 类型 | 说明 / 不变量 |
|:--|:--|:--|
| `id` | string | 实例唯一标识 |
| `definition_ns` / `definition_name` / `definition_version` | string | 定义坐标；版本漂移无哈希保护（见 GAPS Q10） |
| `initiator` | string | 发起人；撤回权的唯一依据（手册清单未列，见 §3 R5） |
| `status` | enum | `pending / running / completed / rejected / terminated / cancelled / faulted` |
| `context` | map | 实例级变量（作用域第三级） |
| `root_branch_id` | string? | 首个分支 |
| `revision` | u64 | 每次变更 +1；命令以它做 CAS 守卫 |
| `jump_count` | u32 | 驳回至节点累计次数；≥ MAX_JUMPS(20) 拒绝再次驳回 |
| `audit` | list | 管理动作审计（enter/add_sign/remove_sign/return/withdraw/terminate） |
| `returns` | list | 驳回记录（from_node / to_node / by / comment / at） |
| `created_at` / `updated_at` | i64 | Unix ms，外部注入 |

终态：`completed | rejected | terminated | cancelled | faulted`；终态实例拒绝一切变更命令。

### 1.2 Branch（分支 = 节点的运行实例）

| 字段 | 类型 | 说明 / 不变量 |
|:--|:--|:--|
| `id` | string | 分支标识 |
| `instance_id` | string | 所属实例 |
| `parent_branch_id` | string? | 嵌套分支预留；父分支对子分支结果的记账未实现（GAPS Q9） |
| `node` | string | 定义中的节点名；驳回白名单以「已进入节点」为键 |
| `attempt` | u32 | 第几次进入该节点（驳回重开递增，历史分支保留） |
| `status` | enum | `open / completed / rejected / returned / superseded / cancelled / terminated` |
| `join` | enum | `all / any / quorum(n) / ratio{num,den}` |
| `reject` | enum | `any_reject`（一票否决）/ `on_exhausted`（全部投完再判） |
| `scope` | map | 分支级变量（作用域第二级） |
| `round_ids` | list | 该分支的轮次 |
| `created_at` / `closed_at` | i64 | 关闭时间只写一次（作废不回填） |

不变量：同一实例同时最多一个 `open` 分支（线性原型；流程级并行见 GAPS Q8）。

### 1.3 Task（审批任务 = 一个审批席位）

| 字段 | 类型 | 说明 / 不变量 |
|:--|:--|:--|
| `id` | string | 任务标识 |
| `branch_id` / `round_id` | string | 所属分支与当前轮次 |
| `kind` | enum | `approval`（计票）/ `read_ack`（阅签，不计票） |
| `status` | enum | `created / claimed / completed / rejected / cancelled / expired` |
| `assignee` | string? | 唯一责任人；候选池任务认领前为 None |
| `candidates` | list | 候选池；`assignee=None` 时池内人可认领/直接投票（隐式认领） |
| `deadline` | i64? | 超时阈值；过期由外部调度器调用 `expire` 驱动（无进程内定时器） |
| `delegation` | struct? | `{from, to, granted_at}`；被委派人与原审批人均可批（原审批人权限保留） |
| `transferred_from` | list | 转办来源审计链；转办后原审批人**不再**可批（临时口径，GAPS Q1） |
| `votes` | list | `VoteRecord{round_id, decision, actor, on_behalf_of, comment, at}` |
| `scope` | map | 任务级变量（作用域第一级） |
| `created_at` / `updated_at` | i64 | |

不变量：`completed/rejected/cancelled/expired` 为终态；投票仅限 `created/claimed`；
转办/委派仅限未决任务；委派只允许当前 assignee 发起。

### 1.4 Round（轮次 = 节点实例的一次投票周期）

| 字段 | 类型 | 说明 / 不变量 |
|:--|:--|:--|
| `id` | string | 轮次标识 |
| `branch_id` | string | **作用域为分支而非单个 task**（见 §3 R1） |
| `number` | u32 | 分支内轮次序号，从 1 起 |
| `status` | enum | `open / passed / rejected / superseded / cancelled` |
| `total_count` | u32 | 可计票审批席位数（不含阅签、已取消、已过期） |
| `completed_count` / `rejected_count` | u32 | 通过/否决票数 |
| `opened_at` / `closed_at` | i64 | |

不变量：一个分支同时最多一个 `open` 轮次；`total_count=0`（只有阅签）即视为通过。

### 1.5 变量作用域（三级）

| 级别 | 存储 | 求值优先级 |
|:--|:--|:--|
| task | `Task.scope` | 1（最高） |
| branch | `Branch.scope` | 2 |
| instance | `Instance.context` | 3（最低） |

`Engine::resolve_var(task_id, key)` 按此顺序求值。写入命令尚未提供（GAPS Q13）。

## 2. 命令与效果（API 速览）

| 命令 | 目的 | 关键前置 |
|:--|:--|:--|
| `start` | 建实例（pending） | — |
| `enter_branch` | 进入节点 | 无其他 open 分支 |
| `open_approval` | 开轮次+建席位 | 分支 open、无 open 轮次 |
| `add_sign` | 加签 | 分支 open、轮次 open |
| `remove_sign` | 减签 | 目标未表态（GAPS Q2） |
| `claim` | 认领池席位 | 未指派、在候选池 |
| `approve` / `reject` | 表决 | 未决、有资格（assignee/候选人/被委派人） |
| `acknowledge` | 阅签 | ReadAck、未决 |
| `transfer` | 转办 | 未决、by=当前责任人（或候选人） |
| `delegate` | 委派 | 未决、by=assignee |
| `return_to_node` | 驳回至节点 | 分支 open、目标在白名单、jump 未超限 |
| `withdraw` | 撤回 | by=发起人 |
| `terminate` | 终止 | 非终态（权限判定不在本原型） |
| `expire` | 置过期 | 已过 deadline |
| `finish` | 定义走到末端时收口实例 | 无 open 分支、最近根分支为 completed/rejected |

效果流 `Effect`：实例/分支/任务/轮次状态变化、席位创建与指派、以及事件草案
（`com.china-workflow.*`，供 Spike 3 对齐）。所有命令返回 `Result<Vec<Effect>, Error>`；
`Error`：`not_found / invalid_transition / not_authorized / invalid_argument / stale_revision`。

## 3. 与手册 §4 字段清单的偏离（Rulings）

| # | 手册口径 | 本模型 | 理由与代价 |
|:--|:--|:--|:--|
| R1 | `Round.task_id` | `Round.branch_id` | 会签/或签是「一个节点 N 个席位」的投票，轮次挂单个 task 无法同时统计多席位；`Task` 的 assignee/candidates 也只能覆盖单审批人。代价：与手册字面不一致，需 EIS/ICPS 确认（Q3 同源） |
| R2 | 未列投票记录 | 内嵌 `Task.votes` | 一票否决/票签/委派/审计都需要 actor、on_behalf_of、时间与意见；轮次只保留计数 |
| R3 | `Task.type` | `Task.kind` | 语义同；值域固定为 `approval/read_ack` |
| R4 | 状态未穷举 | Instance +`rejected`；Branch +`returned/superseded`；Round +`superseded/cancelled` | 驳回重开与作废必须可与业务结果区分，否则审计不可读 |
| R5 | Instance 无发起人 | +`initiator` | 撤回权需要明确归属 |
| R6 | 未定义收口语义 | 通过=`finish` 显式收口；否决立即 `rejected` | 状态机内部无法得知定义是否还有后继节点；否决是终局 |
| R7 | 未定义超时处置 | `expired` 不计票；处置策略待定 | 自动通过/驳回/升级是业务决策（Q4） |
| R8 | 未定义权限模型 | 仅 `by` 字段 + 审计 | 谁可加签/驳回/终止/代批需要授权模型（Q6） |

## 4. 可表达性结论（对 G1）

手册 §4 Spike 1 的全部 P0 与 P1 场景在模型层均可表达并已通过测试
（16/16，见 `../README.md`）。G1 的「需要 v2 状态模型」成立；但重构范围是
**新增分支/任务/轮次/作用域实体层**，不需要推翻实例级状态机。剩余风险集中在
授权语义、并发分支与定义层治理（GAPS Q1–Q13）。
