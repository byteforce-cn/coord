# ADR 草案（Spike 6 输入）：审批工作流引擎的形态与边界

> 状态：**draft**（待 Spike 2/3/5 结果补全判据；正式化时按 `docs/adr/` 编号递增）
> Owner: Spike Lead（张三）+ 契约 owner｜起草：AI 协作者｜ Last verified: 2026-10-13
> 事实源：`docs/adr/0010-eis-consumer-gaps-full-landing.md`（G-WF-1）、
> `docs/production/workflow-semantics.md`、本目录 Spike 1/4 产出

## 背景

手册要求在本目录做出 Go/No-Go 与形态选择。截至 2026-10-13，Spike 1/4 已完成初版：

- Spike 1：P0 审批语义在「Instance + Branch + Task + Round + 三级作用域」模型上
  全部可表达（16/16 用例，`spike-1-state-model/`）；
- Spike 4：现解析器与官方 1.0.3 语义可用率 13/23；`run:*` 与 `extensions` 存在
  静默误读（`spike-4-conformance/REPORT.md`）；
- Spike 2/3/5 未执行（10-31/11-10 前完成）。

## 决策判据（手册 §4 Spike 6 表 + 证据）

| # | 判据 | 证据 | 当前判断 |
|:--|:--|:--|:--|
| D1 | 现有状态模型可通过扩展满足五层要求且不破坏契约 | Spike 1 模型 + 现有 `workflow.proto` | **倾向满足**：扩展现有引擎模型层，不推翻实例状态机；契约增量（新实体/RPC）待设计 |
| D2 | 扩展会破坏现有契约兼容性 | Spike 4 §3（`run`/`switch`/`extensions` 形状冲突） | 待定：native `document/do` 方言可保持；官方形状兼容需适配层或新版本号 |
| D3 | 现有引擎性能达标（按 10-18 阈值） | Spike 5 | **未测** |
| D4 | 现有引擎性能不达标但新建可达标 | Spike 5 | **未测** |
| D5 | 并发/恢复根本障碍 | Spike 2/3 | **未测**；现状（无 CAS 写路径、无 Outbox、进程内定时器）已知为缺口 |

## 候选路径

| 路径 | 说明 | 前置条件 |
|:--|:--|:--|
| P1 扩展现有 `coord-core/workflow`（v2 模型） | 新增 Branch/Task/Round/Scope 实体 + 审批命令；复用 KV/Watch/Lease/Event | D1 成立 + Spike 2/3 证明 CAS/Outbox/恢复可补齐 |
| P2 新建独立审批引擎 crate（复用 coord KV/Event/MQ） | 审批语义与通用工作流解耦 | D1 不成立或契约兼容代价过高 |
| P3 No-Go | 重新设计 | D3+D4 均不成立，或 D5 发现根本障碍 |

## 当前倾向（待 Spike 2/3/5 定案）

P1 + 分期，理由：

1. 模型层可扩展（Spike 1 的直接证据）；
2. 审批语义与通用 DSL 共用状态/事件/恢复基础设施，分叉两套的成本更高；
3. Spike 4 表明「完全符合官方 1.0.3」是独立的大工程，与审批引擎形态解耦，
   不应绑进本次决策。

## 迁移与边界草案

1. **阶段 A（立即）**：对未实现构造 fail-closed（`run.*` 非 workflow、
   `use.extensions`、非 switch 的 `then`），消除静默误读；
2. **阶段 B**：状态模型 v2 + CAS 写路径 + 实例租约 + Outbox + 持久化定时器
   （与 Spike 2/3 结论合流），先内部接口，后对外契约；
3. **阶段 C**：`coord.workflow.v1` 契约增量（审批命令/查询）走
   `apis/contracts/WHITEPAPER.md`，能力表登记 `grpc_auth.rs`，验收项同步
   `STATUS.md`，边界更新 `boundaries.md`；
4. 与 ADR-0010 G-WF-1（2027-03-31）对齐，不改变其验收锚点；新增承诺另行编号。

## 开放问题（Spike 6 必须回答）

- 授权模型（谁可加签/驳回/终止/代批）与职责分离（Spike 1 GAPS Q6）；
- 流程级并行分支/子流程记账（Q8/Q9）；
- 定义不可变性（definition_hash/engine_version，评估报告 G5）；
- 审批事件的对外契约与 `coord.event.v1` 的关系（G3）；
- 消费方（EIS/ICPS）对 run:script 的真实需求是否为 P0。

## 参考

- `../spike-1-state-model/docs/GAPS.md`（14 项未决语义）
- `../spike-4-conformance/REPORT.md`（差距量化与工作量）
- `../NEXT-SPIKES-NOTES.md`（Spike 2/3/5 接续）
- `docs/adr/0010-eis-consumer-gaps-full-landing.md`、`docs/production/workflow-semantics.md`
