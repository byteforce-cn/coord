# ADR-0013: 工作流引擎 v2 —— 中国式审批能力落地（状态模型 / 并发 / 事件 / 恢复 / 契约）

- 状态：accepted（2026-10-15 用户方裁定：EIS/ICPS 无疑问、集成后反馈；Spike 1–5 证据已齐备，惟 H3 性能判据待 10-18 阈值定稿与目标硬件复测后复核）
- 日期：2026-10-13
- 决策者：Spike Lead（张三）+ 契约 owner；评审：CTO、平台架构组、EIS/ICPS 接口人

## 背景

### 目标

`workflow` 能力增强：让 coord 原生承载中国式审批场景（会签/加签/减签/驳回至节点/
转办/委派/一票否决/或签/票签/阅签/撤回/终止），消费方（EIS/ICPS）不再在外围
用 signal/listen/fork 拼装。

### 现状缺口（单一归属：`spike/2026-10/` 评估与验证产出）

| 缺口 | 现状 | 证据 |
|:--|:--|:--|
| G1 状态模型 | 扁平 `WorkflowInstance + task_stack`，无分支/任务/轮次/作用域 | `coord-core/src/workflow/model.rs`；Spike 1 |
| G2 实例级并发 | `save_instance_atomic` 无调用点；无租约/fencing；`request_id` 不可作幂等基础 | `workflow_store.rs:84,736`；`kv.proto` |
| G3 事件/Outbox | 无 outbox；生产装配硬编码 `MemoryEventProvider`；`MqEventProvider` 无装配点 | `workflow.rs:2060-2065` |
| G4 崩溃恢复 | wait/retry/listen/超时为进程内定时器；启动只对账子流程 | `runtime.rs:583,815,927,1220,1344,398` |
| G5 定义不可变 | 只固定 version，无 `definition_hash`/`engine_version` | `workflow_store.rs:390-468` |
| G6 幂等传递 | `dispatch(service, with, input)` 无幂等键 | `workflow.rs:1464` |
| G7 审批语义/RPC | `coord.workflow.v1` 无审批命令/实体 | `workflow.proto` |
| G8 动态分支/扩展 | 无运行时增删分支；官方 `use.extensions` 静默丢弃 | Spike 4 §3.3 |
| G9 读模型/保留 | 实例全量常驻 agent 内存；无自动 TTL/归档 | `workflow_store.rs:581-612`；`boundaries.md` B-WF-1 |

可复用地基：KV/线性一致读、`Txn` CAS、`Watch` 续订/重同步、`Lease`、`coord.event.v1`、
MQ `Poll+Ack`、RBAC/能力表、TLS——本 ADR 不重建任何一项。

### 先行验证（判据来源）

- Spike 1（已完成）：P0 审批语义在 `Instance/Branch/Task/Round/三级作用域` 上
  全部可表达（16/16）；裁定 Round 作用域=分支、分支通过≠实例收口。证据
  `spike/2026-10/spike-1-state-model/`。
- Spike 4（已完成）：官方 SW 1.0.3 语料 24 份，语义可用 13/22；`run:*` 与
  `use.extensions` 静默误读。证据 `spike/2026-10/spike-4-conformance/REPORT.md`。
- Spike 2（已完成）：CAS + 租约 + fencing 在真实单节点 Raft 上验证（7/7）：
  `save_instance_atomic`（`Compare(ModRev)`）并发恰一胜；`LeaseGrant` + Txn
  create-if-absent 得互斥租约；Lease 无 fencing token 的处置为「租约 key 的
  `mod_revision` 作 epoch + 写回多重 compare」——旧驱动迟到写被拒。缺口：fencing
  只覆盖状态写回，外部副作用与部分保存点（resume/timeout/fail 等）未接线；
  StepToken 必须用 CAS key（`request_id` 不可用）。证据
  `spike/2026-10/spike-2-concurrency/`。
- Spike 3（已完成）：状态 + outbox 同 Txn、relay at-least-once + `event_id` 去重、
  持久化定时器 + 重启扫描；进程级 kill -9 的 wait/listen/retry 三场景均恢复，
  事件不丢不重；`MqEventProvider` 两处缺陷已修。缺口：`drive` 的状态保存点与 outbox
  提交仍是两次写（外部可先见终态后见伴生事件）、listen 游标与实例状态不同事务、
  outbox 无 TTL/清理。证据 `spike/2026-10/spike-3-recovery/`。
- Spike 5（已完成，阈值未定稿）：安静机原始基准 —— KV 写 636–724 ops/s、
  P50 134–1331 ms；Watch 1000 路 0 丢失、P99 16.6 ms；Txn ≤128 操作；单值 ≤4 MiB
  （gRPC）；存储 2819.5 B/写。阈值（§3.3）与目标硬件缺失 ⇒ 不做达标判定。
  证据 `spike/2026-10/spike-5-bench/REPORT.md`。

### 约束

- ADR-0001：未经生产验收的能力面默认关闭；ADR-0009：dev 预设不等于生产装配。
- ADR-0010：G-WF-1 目标 2027-03-31，本文是其审批维度的增量，不改变其验收锚点。
- `apis/contracts/WHITEPAPER.md` §4 兼容性铁律与 §11 变更流程；`STATUS.md`
  目标日期校验会因 COMMITTED 未落地而 CI 失败。
- 边界三步（`boundaries.md` + `STATUS.md` + 负控制证据）；`grpc_auth.rs`
  能力表漏登记 = 鉴权开启时全拒。

## 决定

### D1 形态：扩展现有 `coord-core/workflow`，不新建独立引擎

- 审批域以新模块落地（`coord-core/src/workflow/approval/` 承载 v2 模型/状态机/命令），
  复用现有 runtime/ports/store 抽象；**不**另建 crate、**不**另起服务端引擎。
- 实例以 `engine_version` 区分语义：v1 实例按现有引擎只读兼容，不做原地迁移；
  新实例走 v2。同一实例不允许跨版本驱动。
- 退出条件（触发新 ADR 重议，不静默改向）：① Spike 2/3 证明 KV CAS/Lease/恢复
  在该路径不可行；② Spike 5 证明扩展路径不达标而重构可达标（手册 §4 Spike 6
  判据表）；③ 审批与通用 DSL 的演进节奏不可调和。
- 依据：Spike 1 证明实例状态机不需要推翻；避免双引擎/双口径；ADR-0010 单线交付。

### D2 状态模型 v2（五层）

- 实体：`Instance`（在现有实例文档上扩展 `engine_version/definition_hash/root_branch`
  等字段）、`Branch`、`Task`（席位）、`Round`、`VoteRecord`（内嵌）、三级变量作用域
  （task > branch > instance）。
- 固定口径（Spike 1 裁定，EIS/ICPS 可改但需走变更）：
  1. **Round 作用域=分支**（会签是「一个节点 N 席位」的投票，挂单 task 无法统计）；
  2. **分支通过不自动收口实例**，驱动方在定义末端调用收口；否决是终局；
  3. 转办=原审批人失去审批权、保留审计链；委派=原审批人保留权限；
     减签不允许撤回已投票席位；超时仅置 `expired` 且不计票（处置策略待 EIS 确认）；
  4. 阅签不计票；`cancelled/expired` 席位不计入分母；无弃权票型（待确认）。
- 不变量：同一实例同时最多一个 Open 分支（v2.0 线性模型）；一个分支同时最多一个
  Open 轮次；命令带 `expected_revision` CAS 守卫。
- 定义表面：coord 原生方言新增**审批节点构造**（`approve` 任务 + 会签/或签/票签/
  一票否决/审批人来源/deadline 等策略字段）；**不**通过官方 SW `extensions` 接入
  （其现状为静默丢弃，且不在承诺面）。语法见下「原生 `approve` 节点 DSL（已固化）」。

#### 原生 `approve` 节点 DSL（已固化）

节点体为单键对象 `approve:`，键集合恰为下列 camelCase 字段名；出现未知键即 fail-closed
（`UnsupportedConstruct("approve.<key>")`，不静默丢弃——把 `allowSelfApprove` 写成 snake_case 的
`allow_self_approve` 会被拒绝，而不是静默改变审批权）：

```yaml
- finance:
    approve:
      approvers: [alice, bob]   # 静态名单（四来源至少一项非空）
      pool: [carol]             # 候选池（整体合成为一个席位）
      readers: [dave]           # 阅签（不计票）
      roles: [finance]          # 角色来源，经 ApproverResolver 解析
      mode: all                 # all|any|quorum:<n>|ratio:<num>/<den>，默认 all
      veto: true                # 默认 true（一票否决）；false = 投尽未达才判不通过
      deadline: PT24H           # 可选 ISO8601/官方 duration；相对挂起时刻换算
      onPass: notify            # 可选；缺省 = 顺序推进下一任务
      onReject: rework          # 可选；缺省 = 工作流 fault（approval_rejected）
      signAdmins: [admin]       # 可选；加签/减签/驳回重开/终止的授权名单
      allowSelfApprove: false   # 默认 false（发起人不可自批）
```

- 席位来源 `approvers/pool/readers/roles` 全空 ⇒ `MissingRequiredField`；`mode` 非法形状
  （含 `quorum:0`、`ratio:0/<den>`、`ratio:<num>/0` 等零阈值——恒真会退化成「零审批自动通过」）
  在解析期 fail-closed；`deadline` 必须在解析期可换算为具体时刻，否则拒绝（不得把无期限当默认）。
- 仅阅签席的轮次没有可计票席位，立即判通过（阅签不计票）；而 `total == 0`（全员过期或被减签）
  的轮次**保持 Open、绝不自动通过**。
- `roles` 在启动/命令路径注入 `ApproverResolver` 后展开为具体审批人；未注入或无对应映射时，
  节点开启即 fail-closed（`approve.role.<name>`），不静默少派席位。

### D3 实例级并发控制（单写者）

- **KV Txn `mod_revision` CAS 是唯一权威写路径**：引擎驱动的每次实例写回改用
  `save_instance_atomic`；`save_instance` 在 v2 路径删除（不留后门）。
  **落地要求（Spike 2 证据）**：v2 的全部实例写路径（含 resume/timeout/fail 等
  非 drive 保存点）必须纳入同一 CAS/fenced 提交；只改 `drive` 会留下迟到写窗口。
- 实例租约（`coord.lease`）只作驱动权优化（减少无效竞争），**不作为正确性基础**；
  fencing token 缺失的处置（已验证方案）：实例状态写用 Txn 同时比较实例
  `mod_revision`、租约 key `mod_revision`（epoch）与租约 key 值（holder token），
  旧驱动迟到写被拒。**残余风险**：fencing 只拒绝状态写回，**无法撤回已发出的
  外部副作用**——必须由 StepToken 兜底（外部调用幂等键）。
- **StepToken = (instance_id, step_id)** 存**独立 CAS 键**
  `/_workflow/v3/steps/{instance_id}/{step_id}`，**不**与实例实体同键：`register` 是
  create-if-absent（`Txn(Compare(key VERSION==0) → Put)`），`call` **派发前**登记、
  成功后标 `Done`。外部副作用幂等键 = 该 token，经 `TaskDispatcher` 扩展签名透传
  （HTTP header）。为何独立键：token 随 step 数量增长，挂在实例实体会让每次实例写都
  携带并变胖（实例 bloom）；而 register-before-dispatch 把去重边界前移到「副作用发出
  之前」——`register` 未成功即 fail-closed 不派发，重试/崩溃重建复用同一确定性 token
  （at-least-once + 下游按幂等键去重）。KV `request_id`（单节点/进程内/60s）**不得**
  作为幂等或 StepToken 基础。
- CAS 冲突语义：立即停止本轮驱动，重读实例后由驱动循环重试；禁止盲写覆盖。

### D4 事件契约与 Outbox

- 状态变更与 outbox 记录**同一 KV Txn 原子写入**（v2 写路径落地要求；Spike 已证
  机制可行，drive 集成见下条）；Relay 轮询 outbox → 投递 →
  标位，至少一次；失败指数退避；DLQ 复用 MQ 路径。
- 事件命名空间固定 `com.china-workflow.*`（CTO 手册口径）；每条事件含
  `id/source/specversion/type/dataschema/subject(instance_id)/partitionkey(instance_id)/
  sequence(每实例单调)`；消费者按 `(instance_id, sequence)` 去重；`event_schema_version`
  固定到实例。
- outbox 保留 24h（TTL 由 Relay 清理；**Spike 未实现 TTL，属落地要求**，见
  `spike-3-recovery/OUTBOX.md` §5.1/§7）；历史归档是外部消费者职责，coord 不承诺。
- 生产装配必须走 MQ/Event；`MemoryEventProvider` 仅 dev。接线前先修
  `MqEventProvider` 的两处缺陷（offset 重启归零、共享单游标）——**Spike 3 已修并
  带回归测试**。
- **落地要求（Spike 3 集成发现）**：outbox 记录必须搭上状态写同一次 Txn
  （v2 写路径为 `commit_instance` 的 CAS Txn，注入租约时经 `fenced_commit` 做同一 Txn
  的三重比较），不得在状态写之后另行 append；否则外部观测者可先见终态、后见伴生事件
  （证据与缺口见 `spike-3-recovery/OUTBOX.md` §2.1 / G1）。

### D5 崩溃恢复

- 定时器持久化：wait/retry/listen/任务超时的到期时间落 KV（含索引），启动扫描
  到期项重新驱动；不再依赖进程内 tokio 定时器。
- 启动对账覆盖**所有非终态实例**（不止子流程父子对）；listen 订阅从 KV 状态
  重建，事件消费位点持久化。
- 「有 StepStarted 无 StepCompleted」由 StepToken 识别并重放；正确性依赖 D3 的
  幂等键，不承诺 exactly-once。
- 验收判据见 `spike/2026-10/NEXT-SPIKES-NOTES.md` 的 Spike 3（杀进程 + 时间线日志）。

### D6 定义不可变与 SW 1.0.3 符合性

- **定义钉死**：实例记录 `definition_hash`（内容哈希）与 `engine_version`；同
  `(ns,name,version)` 内容变化 = 新 revision/hash，不覆盖；resume/重驱动校验 hash，
  漂移 fail-closed。
- **官方 SW 1.0.3 不承诺完整符合**：定义「支持子集 + fail-closed 拒绝清单」，
  未实现构造必须显式拒绝（`run.*` 非 workflow、`use.extensions`、非 switch 的
  `then` 等），**禁止静默误读**（Spike 4 的核心发现）。
- 先以内置适配层处理流程形状差异（官方 `switch.then`、fork/for 形状等）；完整
  1.0.3 解析器/执行语义另立里程碑，不与本次审批交付绑定。
- Spike 4 的 corpus + 脚本纳入回归（`spike-4-conformance/harness`）。

### D7 审批契约与授权

- 契约面：`coord.workflow.v1` **增量新增 12 个 RPC**（WHITEPAPER §4.2 允许的 Minor）：
  命令 10 个——`Approve / Reject / Acknowledge / AddSign / RemoveSign / ReturnToNode /
  Transfer / Delegate / Withdraw / Terminate`；查询 2 个——`ListPendingTasks /
  GetInstanceDetail`。命令请求带 `idempotency_key`（复用 Signal 模式），主体身份取自
  认证上下文（CCT），**不信任请求体里的 actor 字段**。
- 能力登记：命令 10 个 → `coord:workflow:execute`，查询 2 个 → `coord:workflow:read`，
  定义新增策略 → `coord:workflow:define`；全部登记 `grpc_auth.rs::rpc_capability`。错误码映射：
  `StaleRevision → ABORTED`、`NotAuthorized → PERMISSION_DENIED`、
  `InvalidTransition → FAILED_PRECONDITION`。
- **授权矩阵（两层，缺一不可）**：引擎 `actor_can_act` 判席位归属，桥在其上补职责分离
  （见下）；申请体不携带 actor，一律取 CCT。

  | 操作 | 授权主体 |
  |:--|:--|
  | `Approve / Reject / Acknowledge / Transfer / Delegate` | 席位责任人 / 候选池成员 / 被委派人（引擎 `actor_can_act`）；当节点 `allowSelfApprove=false` 时附加 `actor != initiator`（`initiator == "anonymous"` 豁免——匿名发起无自审约束） |
  | `AddSign / RemoveSign` | 分支 `signAdmins` ∪ 实例发起人 |
  | `ReturnToNode` | 分支未决席位持有人（`assignee`/`candidate`/被委派人）∪ `signAdmins` ∪ 发起人 |
  | `Withdraw` | 仅发起人（引擎自身强制） |
  | `Terminate` | `signAdmins`（任一分支声明者）；保留内部 `SYSTEM_ACTOR="system"` 生命周期权威——恒可终止，用于 `cancel_instance` 运行时收口，不对用户暴露 |
  | `expire` | 仅桥（持久化定时器），无 RPC |

- 契约交付顺序：proto 先行 → 实现 → 验收项；COMMITTED 状态提升在生产验收之后
  （ADR-0001），期间按能力默认关闭。

### D8 存储布局与读模型

- v2 实体使用**独立 KV 前缀**（不与现有 `/_workflow/v3/instances/` 全量扫描
  前缀混用）；具体 key 布局在 Spike 2/3 落地时固定并配键空间测试。
- v2 路径**不做启动全量加载**：按实例作用域加载 + watch 驱动的有界缓存
  （淘汰策略在实现中固定），避免 G9 的内存/扫描代价。
- 保留策略维持 B-WF-1（显式删除、无自动 TTL/归档）；outbox TTL 仅清 outbox。
- **键布局（已固化）**：状态为单文档 `/_workflow/v4/instances/{instance_id}`；伴生事件仍走
  共享的 `/_workflow/v3/outbox/**` 传输设施（relay 已认该前缀），仅实例状态换到独立键空间。
  v4 前缀与 v1 的 `/_workflow/v3/instances/` 全量扫描面互不可见（配双向键空间测试）。
- **写路径（已固化）**：唯一写路径带 CAS 谓词
  `ExpectedRev::{CreateOnly, Exact(mod_rev)}`——`CreateOnly` 比较 `VERSION==0`
  （create-if-absent）、`Exact(n)` 比较 `MOD_REV==n`；**没有无守卫写入形态**。谓词不成立
  整个 Txn 零写入返回 `Ok(false)`，调用方重读后重试；状态与本批事件在同一 Txn 提交
  （见 D3/D4）。
- **实现归属**：`MemoryApprovalStore` 仅测试用（内存同锁临界区原子，禁止生产）；生产走
  `coord-agent` 的 `KvApprovalStore`（outbox 序号取自持久化计数器
  `/_workflow/v3/outbox/seq/{aggregate}`，不随事件日志剪枝回退）。
- **幂等记录（已固化）**：命令幂等记录键
  `/_workflow/v4/idempotency/{instance}/{key}`——与实例前缀同属 v4 命名空间但**不同前缀**，
  实例扫描面不会把幂等记录当实例；`{instance}` 为独立分段（`inst-1` 前缀不吞 `inst-10` 的记录）。
  幂等记录与状态、本批事件**同一次 CAS 提交**写入：只有提交成功它才存在，失败时读不到——
  重试方不会拿到从未落库的响应；同一 `key` 只有首次成功提交能写。命中即原样返回首次响应，
  不再执行命令。
- **前缀扫描与分页（已固化）**：`scan(after, limit)` 按实例 id 字典序升序，`after` 独占续读；
  `limit` 经同一处钳制（`0 ⇒ 默认 50`，硬上限 `200`），内存与 KV 实现共用。`ListPendingTasks`
  的 `page_token` = 上一页最后一条实例 id；`next_page_token` 仅在扫描满页时给出。

### D9 非目标与边界（永不承诺区不变）

- 不做：业务表单/业务数据存储、跨服务 XA/distributed transaction、exactly-once、
  自动归档/TTL、跨分区强一致读（WHITEPAPER §9.3 红线不变）。
- v2.0 不承诺：流程级并行分支（多 Open 分支）与嵌套子流程的结果记账；
  如需要，另立 ADR 扩展（Spike 1 GAPS Q8/Q9）。
- 查询外部化（评估报告 §11）不在本期承诺；Spike 5 容量评估超阈值时另议。
- deadline 到期**无自动处置**（Q4）：过期只把席位置 `Expired` 并从计票分母剔除，既不自动
  通过也不自动驳回；全员过期/被减签的轮次保持 `Open`，实例不因超时收口。处置由人决定
  （`AddSign` 补席或 `Terminate` 终止是唯一出口）。
- 命令路径**不产生审批实例 `Faulted`**：RPC 命令只收口为 `Completed / Rejected / Cancelled /
  Terminated`；`Faulted` 保留给版本级故障——定义漂移时 `resume_approval` 在写前 fail-closed，
  工作流实例置 `Failed`（`FAULTED`），审批命令不返回 `Faulted`。`cancel_instance` 经内部
  `SYSTEM_ACTOR` 先终止未决审批，再按既有取消路径收口。
- carry（本期不接线、不在承诺内）：无 UI 审批面、无 REST facade（`coord-ui` 只做影响评估）；
  `roles` 的外部解析服务未接线（仅启动/命令路径注入的静态映射，见 D2）；审批实例**未实现**
  D8 的 watch 驱动按实例有界缓存（KV 实现按实例做线性一致单键读，无缓存/淘汰）。
- **冻结证据只读**：本里程碑对 `spike/2026-10/**` 的改动仅为 harness 穷尽性补齐
  （`Task::Approve` 分支与 `approval_roles` 配置字段），不改任何评估产出/证据。

### D10 验收门与治理动作

- 验收门（手册 §4 Spike 6 判据）：D1 形态判据 = Spike 1（满足）+ Spike 2/3/5；
  Spike 5 阈值未达标 → 按判据表重议 P2/No-Go，**不静默顺延**。
- 落地动作清单（缺一不可）：
  1. `apis/contracts/proto/coord/workflow/v1/workflow.proto` 增量 + `buf breaking` 全绿；
  2. `WHITEPAPER.md` §11.2 PR Checklist + `CHANGELOG.md` + 消费者告知（EIS/ICPS）；
  3. `STATUS.md` 新增验收项与目标日期（禁止晚于 ADR-0010 G-WF-1 的 2027-03-31）；
  4. `boundaries.md` B-WF-1/2/3 对应更新（默认定时器/恢复/保留口径）；
  5. `grpc_auth.rs` 能力表登记 + 负控制测试；
  6. `docs/production/workflow-semantics.md` 单一归属页更新；
  7. Spike 4 语料回归 + Spike 2/3 注入用例纳入 CI；
  8. SDK/UI 影响评估（coord-client / coord-java-sdk / coord-ui）。

### D10.1 形态判据对照（手册 §4 Spike 6，证据截至 2026-10-13）

| 手册判据 | 证据 | 结论 |
|:--|:--|:--|
| 现有状态模型可通过扩展满足五层状态要求，且改动不影响现有契约 | Spike 1（P0 语义 16/16，`spike-1-state-model/`）；Spike 2/3 均未新增/修改对外 RPC 或 proto 字段 | ✅ 满足（扩展路径） |
| 现有状态模型无法扩展 / 扩展破坏契约兼容 | 无证据（Spike 1/2/3 均为增量） | ✅ 不触发 |
| 现有引擎性能不达标（按 §3.3 阈值） | Spike 5 安静机 dev profile：KV 636–724 ops/s、P50 134–1331 ms；**阈值与目标硬件未定稿** | ⏸ 无法判定（待 10-18 阈值 + 目标硬件/release 复测） |
| 性能不达标但新建可达标 / 性能达标但状态模型不达标 | 同上前提未定 | ⏸ 不适用 |
| 两者均不达标 ⇒ No-Go | 无证据 | ✅ 不触发 |
| **当前形态选择（可随时按上表重议）** | — | **扩展现有 `coord-core/workflow`（D1）维持；H3 判定前不新建独立 crate，触发条件与退出条件见 D1 / 「待验证假设」** |

**Go / No-Go（当前判定）**：**Go（有条件）** —— 状态模型（Spike 1）、并发（Spike 2）、
恢复（Spike 3）三条判据有可复现证据，D1 形态维持；**条件**是 H3 在 10-18 阈值定稿 +
目标硬件/release 复测后复核：若性能判据不达标，按上表重议（新建独立 crate 或 No-Go，
新 ADR），不静默顺延。前置输入缺失（`CHARTER-INPUTS.md` 3.2/3.3）为手册允许的
启动口径，不是本文档可自决项。

### D11 运行时接线：挂起 / 恢复 / 节点结论（已固化）

- **挂起**：工作流走到 `approve` 节点时，运行时经审批桥幂等开启审批实例（**审批实例 id =
  workflow 实例 id**，跨同一实例的多个 `approve` 节点复用），并把 workflow 置 `Suspended`
  （人工挂起）：`SuspensionMeta { reason: "approval", expected_signal: "approval.completed",
  approval_instance_id, node }`。对账（`reconcile_resumable_instances`）**不**重驱动挂起实例
  （否则会把人工审批直接跑过去），但重建其 deadline 持久化定时器，弥合「先提交 `Suspended`、
  再异步落定时器」窗口里死亡导致的截止丢失。
  未注入审批桥或未启用 durable 装配时，`approve` 节点 fail-closed（实例 `Failed` +
  `workflow.faulted`），**不**静默降级为无审批继续。
- **`NodeOutcome{Passed,Rejected}` 与实例终态 `ApprovalOutcome` 分离**：`NodeOutcome` 是
  **分支/轮次级**的本轮结论，用于驱动 workflow 的 `onPass`/`onReject` 路由；`ApprovalOutcome`
  （`Completed / Rejected / Cancelled / Terminated`）是**实例级**终态。命令路径与截止路径共享同一
  映射（`apply_approval_node_outcome`）。实例 `Completed` 由工作流驱动收尾
  （`finish_if_complete`）达成，命令路径无额外动作。
- **resume-on-pass**：分支本轮 `Passed` ⇒ 清挂起并按节点 `onPass` 恢复（缺省顺序推进下一
  任务）；实例级 `Rejected` 走同一映射（有 `onReject` 则路由，否则 fault `approval_rejected`）；
  `Cancelled`/`Terminated` 按既有口径 fault。
- **`return_to_node` 重开目标节点**：目标席位取该节点**最近一次 attempt** 的席位逐字还原
  （`kind`/`assignee`/`candidates`）；重开**不设 deadline**（旧的绝对截止早已过期，沿用会立即
  过期），兜底出口是 `signAdmins` 的 `Terminate`/`AddSign`；目标节点无既有席位可复用 ⇒
  **fail-closed**（拒绝，绝不留下「Open 但零席位」的不可恢复分支）。重开必须与开轮**同批**提交。

## 后果

- 收益：审批语义原生、单一口径；复用 KV/Txn/Watch/Lease/Event/MQ，不重造持久层；
  v1 实例与既有消费方（EIS `document/do` 方言）不受影响；契约演进走既有治理。
- 代价：`coord.workflow.v1` 增 RPC 与验收项（Minor 流程 + 消费方告知）；
  v2 实体层与 CAS/Outbox/持久化定时器带来实现与运维复杂度；`engine_version`
  引入长期双语义维护窗口。
- 钉住的不变量：CAS 是唯一写路径；未实现构造 fail-closed；事件与状态同 Txn；
  COMMITTED 之前默认关闭；边界不因本 ADR 扩大（B-WF-1 除外，另行更新）。

## 待验证假设与触发重议

| # | 假设 | 验证（Spike） | 结论（2026-10-13） |
|:--|:--|:--|:--|
| H1 | KV Txn CAS + Lease 可实现「一实例一驱动」且无重复副作用 | Spike 2 | **成立**（真实 Raft 7/7；带内对照：无租约时副作用重复 2 次）。附带缺口：fencing 仅覆盖状态写回；StepToken 执行路径未实现，落地面 D3 |
| H2 | 状态+Outbox 同 Txn + 持久定时器可实现崩溃恢复且事件不丢不重 | Spike 3 | **成立**（kill -9 三场景 PASS，`missing/duplicate = 0`）。附带缺口：drive 保存点与 outbox 提交仍两次写、listen 游标窗口；落地面 D4/D5 |
| H3 | 扩展路径的写延迟/吞吐/Txn 上限满足 10-18 定稿阈值 | Spike 5 | **未判定**（阈值/目标硬件缺失；安静机 dev profile 原始数据已备，重议触发：阈值定稿 + 目标硬件/release 复测） |
| H4 | 审批规模下 v2 作用域缓存不触发内存/扫描问题 | Spike 5 | **部分**：1000 路 watch 0 丢失、RSS +60.6 MB；v2 作用域缓存未单独验证（Spike 1 为纯内存原型）→ 作为阶段二验收项 |
| H5 | EIS/ICPS 对 run:script 等执行语义非 P0 | 10-18 需求清单 | 未变（清单未到位，临时口径见 `CHARTER-INPUTS.md`） |

## 参考

- 验证产出：`spike/2026-10/spike-1-state-model/`、`spike/2026-10/spike-2-concurrency/`、
  `spike/2026-10/spike-3-recovery/`、`spike/2026-10/spike-4-conformance/`、
  `spike/2026-10/spike-5-bench/REPORT.md`、`spike/2026-10/NEXT-SPIKES-NOTES.md`、
  `spike/2026-10/spike-6-adr/`。
- 契约与治理：`apis/contracts/WHITEPAPER.md`、`apis/contracts/STATUS.md`、
  `apis/contracts/proto/coord/workflow/v1/workflow.proto`、
  `docs/production/ops/boundaries.md`、`docs/production/workflow-semantics.md`、
  `docs/production/consumer-contract.md`。
- 代码锚点：`coord-core/src/workflow/`、`coord-agent/src/services/workflow_store.rs`、
  `coord-agent/src/services/workflow.rs`、`coord-core/src/grpc_auth.rs`。
- 相关 ADR：ADR-0001（默认关闭）、ADR-0009（dev 预设）、ADR-0010（EIS 承接）。
