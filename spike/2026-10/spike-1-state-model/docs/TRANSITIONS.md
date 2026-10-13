# Spike 1 · 状态转换表

> Owner: Spike 执行（AI 协作者）｜ Last verified: 2026-10-13
> 实现：`crate/src/engine.rs`｜ 字段：`MODEL.md`｜ 用例：`crate/tests/scenarios.rs`

## 1. 实体状态机

### Instance

| 当前 | 事件 | 下一状态 | 条件 |
|:--|:--|:--|:--|
| pending | `enter_branch` | running | 首个分支创建 |
| running | 分支 `rejected`（任一机制） | rejected | 一票否决 / 耗尽不通过 |
| running | `finish` | completed | 无 open 分支且最近根分支 completed |
| running | `finish` | rejected | 最近根分支 rejected（通常已由分支判定先置位） |
| running | `withdraw` | cancelled | by = initiator |
| running | `terminate` | terminated | 管理路径 |
| completed / rejected / terminated / cancelled / faulted | 任意变更命令 | 拒绝（`invalid_transition`） | 终态 seal |

### Branch

| 当前 | 事件 | 下一状态 | 条件 |
|:--|:--|:--|:--|
| open | 轮次 `passed` | completed | 判定见 §3 |
| open | 轮次 `rejected` | rejected | 一票否决 / 耗尽不通过 |
| open | `return_to_node`（自身） | returned | 驳回动作主动关闭 |
| open | 被驳回目标重开 / 管理取消 | superseded | 作废，不改变实例 |
| open | `withdraw` / `terminate` | cancelled / terminated | 级联 |
| 终态 | 任意 | 拒绝 | 仅 superseded 可由 completed 再置（保留历史分支） |

### Task（席位）

| 当前 | 事件 | 下一状态 | 条件 |
|:--|:--|:--|:--|
| created | `claim` | claimed | 在候选池 |
| created | `approve`/`reject`（池席位） | completed / rejected | 隐式认领 |
| created/claimed | `approve` | completed | 有资格（assignee / 候选人 / 被委派人） |
| created/claimed | `reject` | rejected | 同上（一票否决立即生效） |
| created/claimed | `acknowledge` | completed | kind=read_ack |
| created/claimed | `transfer` | （状态不变，责任人变更） | by = 原责任人 |
| created/claimed | `delegate` | （状态不变，增加代办人） | by = assignee |
| created/claimed | `remove_sign` / 判定取消 / 管理取消 | cancelled | 未表态 |
| created/claimed | `expire` | expired | now ≥ deadline |
| 终态 | 任意 | 拒绝 | — |

### Round

| 当前 | 事件 | 下一状态 | 条件 |
|:--|:--|:--|:--|
| open | 判定通过 | passed | §3 |
| open | 判定不通过 | rejected | §3 |
| open | 驳回 / 管理取消 | superseded / cancelled | — |
| passed/rejected/… | 任意 | 拒绝 | 同一分支轮次不重开（驳回重开的是新分支新轮次） |

## 2. 命令 × 前置条件 × 效果

| 命令 | 前置条件 | 状态变化 | 效果（Effect） |
|:--|:--|:--|:--|
| `start` | — | 建实例 pending | — |
| `enter_branch` | revision 匹配；实例非终态；无 open 分支 | pending→running（首次）；新分支 open；attempt+1 | `BranchCreated` |
| `open_approval` | 分支 open；无 open 轮次 | 新轮次 open；建席位 created | `RoundOpened`、`TaskCreated`、`RoundCounted` |
| `claim` | 席位未决、未指派、在候选池 | created→claimed；assignee=principal | `TaskAssigned(Claim)`、`TaskStatus` |
| `approve` / `reject` | 席位未决、kind=approval、有资格 | →completed / rejected；写 votes；可能触发 §3 判定 | `TaskStatus`、`RoundCounted`、可能 `RoundStatus`/`BranchStatus`/`InstanceStatus`/`Event` |
| `acknowledge` | 席位未决、kind=read_ack、有资格 | →completed | `TaskStatus` |
| `add_sign` | 分支 open、轮次 open | 追加 approval 席位（created） | `TaskCreated`、`RoundCounted`；审计 `add_sign` |
| `remove_sign` | 分支 open、轮次 open、目标未决 | →cancelled | `TaskStatus`、`RoundCounted`；审计 `remove_sign` |
| `transfer` | 席位未决、kind=approval、by=责任人/候选人 | assignee=to；transferred_from.push(原)；清委派 | `TaskAssigned(Transfer)`（池席位补 `TaskStatus`）； |
| `delegate` | 席位未决、by=assignee | delegation={from,to} | `TaskAssigned(Delegate)` |
| `return_to_node` | 分支 open；目标∈已进入节点；jump<MAX | 当前分支 returned；当前轮次 superseded；目标旧分支 superseded；新建目标分支 open；jump_count+1；记 returns | `BranchStatus`×2、`RoundStatus`、`BranchCreated`、`Event`；审计 |
| `withdraw` | by=initiator | open 分支/轮次/席位全部取消；实例 cancelled | `TaskStatus`×n、`RoundStatus`、`BranchStatus`、`InstanceStatus`、`Event` |
| `terminate` | 非终态 | 同上（terminated） | 同上 |
| `expire` | 席位未决、有 deadline、now≥deadline | →expired；approval 席位触发重计 | `TaskStatus`、可能 `RoundCounted`/判定 |
| `finish` | 无 open 分支；最近根分支为 completed/rejected | 实例 completed/rejected | `InstanceStatus`、`Event` |

所有命令先过 `expected_revision` 守卫：不匹配 → `stale_revision`，状态零变化。

## 3. 判定规则（recount 收口）

记 `total` = 未取消未过期的 approval 席位数，`completed` / `rejected` = 通过/否决数：

| 顺序 | 条件 | 判定 |
|:--|:--|:--|
| 1 | reject=any_reject 且 rejected>0 | **rejected**（一票否决；余席取消） |
| 2 | total=0 或 completed=total | **passed**（含只有阅签的节点） |
| 3 | join=any 且 completed≥1 | **passed**（余席取消） |
| 4 | join=quorum(n) 且 completed≥n | **passed**（余席取消） |
| 5 | join=ratio{num,den} 且 completed·den ≥ total·num | **passed**（余席取消） |
| 6 | completed+rejected=total（全部投完）且未通过 | **rejected**（耗尽判定） |
| 7 | 其余 | 保持 open |

通过/否决后：轮次关闭 → 分支 completed/rejected → 否决立即实例 rejected；
通过不自动收口实例，由 `finish` 显式收口（R6）。

## 4. 并发与幂等（模型层）

- `revision` 单调递增；命令必须带正确 revision（乐观并发，不阻塞）。
- 同一 (instance, branch, round) 的判定在命令内原子完成；模型层无中间态泄漏。
- 幂等由调用方用幂等键在命令层去重（本原型未内置；Spike 2 用 CAS key 验证）。

## 5. 测试覆盖映射

| 场景 | 用例 | 结果 |
|:--|:--|:--|
| 会签 + 加签 | `p0_huiqian_with_add_sign` | ✅ |
| 驳回至节点 | `p0_return_to_node_reopens_history` | ✅ |
| 一票否决 | `p0_one_vote_veto` | ✅ |
| 或签 | `p0_any_sign_cancels_rest` | ✅ |
| 转办 | `p0_transfer_moves_authority` | ✅ |
| 委派 | `delegate_allows_proxy_and_keeps_principal` | ✅ |
| 票签（通过/耗尽） | `ratio_pass_then_exhausted_reject` | ✅ |
| 阅签 | `read_ack_does_not_gate_and_reader_only_branch_passes` | ✅ |
| 撤回 / 终止 | `withdraw_only_by_initiator` / `terminate_cascades_and_seals_instance` | ✅ |
| CAS 陈旧写入 | `stale_revision_is_rejected` | ✅ |
| 候选人池 | `pool_claim_rules` | ✅ |
| 超时过期 | `deadline_expire_excludes_seat_from_quorum` | ✅ |
| 白名单 / jump 上限 | `return_target_whitelist_and_jump_limit` | ✅ |
| 三级作用域 | `variable_scope_precedence` | ✅ |
| JSON 快照 | `evidence_snapshot_serializable` | ✅ |
