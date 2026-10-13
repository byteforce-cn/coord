# coord 工作流引擎 Spike — 2026-10

> Owner: Spike 执行（AI 协作者）｜ Last verified: 2026-10-13
> 状态：进行中｜分支：`spike/2026-10-workflow-v2`（不并入 main）

本目录是 CTO 批准《coord 工作流引擎 Spike 落地执行手册 v1.1》的产出物集合。
所有内容为**决策证据**，不是生产代码，不承诺任何对外契约。

## 目录

| 目录 | 对应 Spike | 状态 |
|:--|:--|:--|
| `CHARTER-INPUTS.md` | 前置条件 3.1/3.2/3.3 的台账（含临时替代输入声明） | 已建立 |
| `spike-1-state-model/` | Spike 1 状态模型 v2 内存验证 | 进行中 |
| `spike-4-conformance/` | Spike 4 规范符合性 | 待启动 |
| `spike-2-concurrency/` | Spike 2 单写者并发演练 | 待启动（10-31 截止） |
| `spike-3-recovery/` | Spike 3 Outbox + 恢复 | 待启动（10-31 截止） |
| `spike-5-bench/` | Spike 5 基准测试 | 待启动（平台架构组主责） |
| `spike-6-adr/` | Spike 6 形态决策 ADR 草案 | 待启动（11-01 起） |

## 事实源与边界

- 现状事实源：`apis/contracts/proto/coord/`、`coord-core/src/workflow/`、
  `docs/production/workflow-semantics.md`、`docs/production/ops/boundaries.md`、`docs/adr/`。
- 本目录只记录 Spike 的假设、验证与结论；与现状冲突处以现状事实源为准。
- 不修改 `main`；不新增对外 RPC/字段；不把 Spike crate 登记进根工作区。

## 复现方法（Spike 1）

```bash
cd spike/2026-10/spike-1-state-model/crate
cargo test 2>&1 | tee ../evidence/test-run-$(date +%Y%m%d-%H%M%S).log
```

每个 Spike 的 `README.md` 给出该项的最小复现步骤。
