# ADR-0013: 工作流引擎 v2 —— 中国式审批能力落地（状态模型 / 并发 / 事件 / 恢复 / 契约）

- 状态：proposed（待 Spike 2/3/5 证据齐备并经 CTO/契约 owner 评审后转 accepted）
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
- Spike 4（已完成）：官方 SW 1.0.3 语料 24 份，语义可用 13/23；`run:*` 与
  `use.extensions` 静默误读。证据 `spike/2026-10/spike-4-conformance/REPORT.md`。
- Spike 2（CAS/租约/StepToken）、Spike 3（Outbox/恢复）、Spike 5（基准）：
  **未完成，是本 ADR 的验收门**（见「待验证假设与触发重议」）。

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
  （其现状为静默丢弃，且不在承诺面）。语法细节随 D7 的契约设计一并评审。

### D3 实例级并发控制（单写者）

- **KV Txn `mod_revision` CAS 是唯一权威写路径**：引擎驱动的每次实例写回改用
  `save_instance_atomic`；`save_instance` 在 v2 路径删除（不留后门）。
- 实例租约（`coord.lease`）只作驱动权优化（减少无效竞争），**不作为正确性基础**；
  fencing token 缺失是已知边界（`consumer-contract.md` §3），先记录、后按需扩展。
- **StepToken = (instance_id, step_seq)** 持久化在实例实体上，与状态同 Txn 写入；
  外部副作用幂等键 = StepToken，经 `TaskDispatcher` 扩展签名透传（HTTP header）。
  KV `request_id`（单节点/进程内/60s）**不得**作为幂等或 StepToken 基础。
- CAS 冲突语义：立即停止本轮驱动，重读实例后由驱动循环重试；禁止盲写覆盖。

### D4 事件契约与 Outbox

- 状态变更与 outbox 记录**同一 KV Txn 原子写入**；Relay 轮询 outbox → 投递 →
  标位，至少一次；失败指数退避；DLQ 复用 MQ 路径。
- 事件命名空间固定 `com.china-workflow.*`（CTO 手册口径）；每条事件含
  `id/source/specversion/type/dataschema/subject(instance_id)/partitionkey(instance_id)/
  sequence(每实例单调)`；消费者按 `(instance_id, sequence)` 去重；`event_schema_version`
  固定到实例。
- outbox 保留 24h（TTL 由 Relay 清理）；历史归档是外部消费者职责，coord 不承诺。
- 生产装配必须走 MQ/Event；`MemoryEventProvider` 仅 dev。接线前先修
  `MqEventProvider` 的两处缺陷（offset 重启归零、共享单游标）。

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

- 契约面：`coord.workflow.v1` **增量新增 RPC**（WHITEPAPER §4.2 允许的 Minor）：
  任务操作 `Approve / Reject / Acknowledge / AddSign / RemoveSign / ReturnToNode /
  Transfer / Delegate`，实例操作 `Withdraw / Terminate`，查询
  `ListPendingTasks / GetInstanceDetail`；请求带 `idempotency_key`（复用 Signal 模式），
  主体身份取自认证上下文（CCT），**不信任请求体里的 actor 字段**。
- 能力登记：命令类 → `coord:workflow:execute`，查询类 → `coord:workflow:read`，
  定义新增策略 → `coord:workflow:define`；全部登记 `grpc_auth.rs` 能力表。
- 授权模型（引擎级，不依赖 RPC 能力粒度）：节点声明审批人来源（静态名单/角色/
  外部解析服务）；操作权限矩阵覆盖「谁可加签/减签/驳回/转办/终止」；默认职责
  分离（发起人不可自批，可配置）；转办/委派/代批全程审计。
- 契约交付顺序：proto 先行 → 实现 → 验收项；COMMITTED 状态提升在生产验收之后
  （ADR-0001），期间按能力默认关闭。

### D8 存储布局与读模型

- v2 实体使用**独立 KV 前缀**（不与现有 `/_workflow/v3/instances/` 全量扫描
  前缀混用）；具体 key 布局在 Spike 2/3 落地时固定并配键空间测试。
- v2 路径**不做启动全量加载**：按实例作用域加载 + watch 驱动的有界缓存
  （淘汰策略在实现中固定），避免 G9 的内存/扫描代价。
- 保留策略维持 B-WF-1（显式删除、无自动 TTL/归档）；outbox TTL 仅清 outbox。

### D9 非目标与边界（永不承诺区不变）

- 不做：业务表单/业务数据存储、跨服务 XA/distributed transaction、exactly-once、
  自动归档/TTL、跨分区强一致读（WHITEPAPER §9.3 红线不变）。
- v2.0 不承诺：流程级并行分支（多 Open 分支）与嵌套子流程的结果记账；
  如需要，另立 ADR 扩展（Spike 1 GAPS Q8/Q9）。
- 查询外部化（评估报告 §11）不在本期承诺；Spike 5 容量评估超阈值时另议。

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

## 后果

- 收益：审批语义原生、单一口径；复用 KV/Txn/Watch/Lease/Event/MQ，不重造持久层；
  v1 实例与既有消费方（EIS `document/do` 方言）不受影响；契约演进走既有治理。
- 代价：`coord.workflow.v1` 增 RPC 与验收项（Minor 流程 + 消费方告知）；
  v2 实体层与 CAS/Outbox/持久化定时器带来实现与运维复杂度；`engine_version`
  引入长期双语义维护窗口。
- 钉住的不变量：CAS 是唯一写路径；未实现构造 fail-closed；事件与状态同 Txn；
  COMMITTED 之前默认关闭；边界不因本 ADR 扩大（B-WF-1 除外，另行更新）。

## 待验证假设与触发重议

| # | 假设 | 验证（Spike） | 不成立的后果 |
|:--|:--|:--|:--|
| H1 | KV Txn CAS + Lease 可实现「一实例一驱动」且无重复副作用 | Spike 2 | 重议 D1/D3（形态或新协调原语，新 ADR） |
| H2 | 状态+Outbox 同 Txn + 持久定时器可实现崩溃恢复且事件不丢不重 | Spike 3 | D4/D5 降级或重议；No-Go 判据 |
| H3 | 扩展路径的写延迟/吞吐/Txn 上限满足 10-18 定稿阈值 | Spike 5 | 按手册判据表选择新 crate 或 No-Go |
| H4 | 审批规模下 v2 作用域缓存不触发内存/扫描问题 | Spike 5 | D8 引入外部读模型（新 ADR） |
| H5 | EIS/ICPS 对 run:script 等执行语义非 P0 | 10-18 需求清单 | D6 范围上调，排期顺延需走契约流程 |

## 参考

- 验证产出：`spike/2026-10/spike-1-state-model/`、`spike/2026-10/spike-4-conformance/`、
  `spike/2026-10/NEXT-SPIKES-NOTES.md`、`spike/2026-10/spike-6-adr/`。
- 契约与治理：`apis/contracts/WHITEPAPER.md`、`apis/contracts/STATUS.md`、
  `apis/contracts/proto/coord/workflow/v1/workflow.proto`、
  `docs/production/ops/boundaries.md`、`docs/production/workflow-semantics.md`、
  `docs/production/consumer-contract.md`。
- 代码锚点：`coord-core/src/workflow/`、`coord-agent/src/services/workflow_store.rs`、
  `coord-agent/src/services/workflow.rs`、`coord-core/src/grpc_auth.rs`。
- 相关 ADR：ADR-0001（默认关闭）、ADR-0009（dev 预设）、ADR-0010（EIS 承接）。
