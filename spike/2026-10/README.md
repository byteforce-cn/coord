# coord 工作流引擎 Spike — 2026-10

> Owner: Spike 执行（AI 协作者）｜ Last verified: 2026-10-13
> 状态：进行中｜分支：`spike/2026-10-workflow-v2`（不并入 main）

本目录是 CTO 批准《coord 工作流引擎 Spike 落地执行手册 v1.1》的产出物集合。
所有内容为**决策证据**，不是生产代码，不承诺任何对外契约。

## 目录

| 目录 | 对应 Spike | 状态 |
|:--|:--|:--|
| `CHARTER-INPUTS.md` | 前置条件 3.1/3.2/3.3 的台账（含临时替代输入声明） | 已建立（3.2/3.3 未到位） |
| `NEXT-SPIKES-NOTES.md` | Spike 2/3/5 接续手册（第一步 + 锚点 + 红线） | 已建立 |
| `spike-1-state-model/` | Spike 1 状态模型 v2 内存验证 | ✅ 初版完成（10-13，16/16，可复现） |
| `spike-4-conformance/` | Spike 4 规范符合性 | ✅ 初版完成（10-13，24 语料，报告+原始数据） |
| `spike-2-concurrency/` | Spike 2 单写者并发演练 | ✅ 初版完成（10-13，7/7，CAS+租约+fencing，可复现） |
| `spike-3-recovery/` | Spike 3 Outbox + 恢复 | ✅ 初版完成（10-13；kill -9 三场景 PASS，机制测试 6/6，事件 schema 校验通过） |
| `spike-5-bench/` | Spike 5 基准测试 | ✅ 初版完成（10-13，5/5，报告+逐条原始数据；共享测试机、非目标硬件） |
| `spike-6-adr/` | Spike 6 形态决策 | ✅ 详细 ADR 已落 `docs/adr/0013-*.md`（proposed）；本目录为证据索引 |

## 事实源与边界

- 架构决策：`docs/adr/0013-workflow-approval-v2.md`（proposed，本分支落档）。
- 现状事实源：`apis/contracts/proto/coord/`、`coord-core/src/workflow/`、
  `docs/production/workflow-semantics.md`、`docs/production/ops/boundaries.md`、`docs/adr/`。
- 本目录只记录 Spike 的假设、验证与结论；与现状冲突处以现状事实源为准。
- 不修改 `main`；不新增对外 RPC/字段；Spike 工装只在 spike 分支登记
  （根工作区成员：`spike/2026-10/spike-4-conformance/harness`、
  `spike/2026-10/spike-3-recovery/harness`，main 上均不存在）。

## 复现方法（Spike 1）

```bash
cd spike/2026-10/spike-1-state-model/crate
cargo test 2>&1 | tee ../evidence/test-run-$(date +%Y%m%d-%H%M%S).log
```

每个 Spike 的 `README.md` 给出该项的最小复现步骤。
