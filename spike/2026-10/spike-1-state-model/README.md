# Spike 1 · 状态模型 v2 内存验证

> Owner: Spike 执行（AI 协作者）｜ Last verified: 2026-10-13
> 手册对应：§4 Spike 1（10-24 检查点初版）

## 输入（PROVISIONAL）

| 输入 | 状态 | 说明 |
|:--|:--|:--|
| EIS/ICPS 审批语义需求清单（10-18） | **未到位** | 以手册 §3.2 语义表为临时输入，见 `../CHARTER-INPUTS.md` |
| coord v1 现状模型 | 已读 | `coord-core/src/workflow/model.rs`（扁平 instance+task_stack） |

## 产出

| 产出 | 路径 |
|:--|:--|
| 字段清单 | `docs/MODEL.md` |
| 状态转换表 | `docs/TRANSITIONS.md` |
| 缺口清单 / 未决语义 | `docs/GAPS.md` |
| 纯内存状态机原型（Rust） | `crate/`（16 个集成用例） |
| 运行日志 | `evidence/test-run-*.log` |
| 场景 JSON 快照 | `evidence/scenario-snapshot-*.json` |

## 复现

```bash
cd crate
cargo test 2>&1 | tee ../evidence/test-run-$(date +%Y%m%d-%H%M%S).log
cargo run --example dump_scenario > ../evidence/scenario-snapshot-$(date +%Y%m%d-%H%M%S).json
```

## 结果（2026-10-13）

- `cargo test`：**16 passed / 0 failed**（日志见 `evidence/`）。
- P0 场景：会签+加签、驳回至节点、一票否决、或签、转办 —— 全部通过。
- 附加：委派、票签（通过/耗尽）、阅签、撤回、终止、CAS 陈旧写入、候选人池、
  超时过期、白名单/jump 守卫、三级变量作用域。
- 「无法表达」P0 项：**0**；未决语义 14 项，其中 6 项须 EIS/ICPS 确认（`docs/GAPS.md`）。

## 与手册验收标准对照

| 标准 | 结果 |
|:--|:--|
| P0 场景全部跑通 | ✅ 5/5 场景用例通过 |
| 状态转换表完整 | ✅ `docs/TRANSITIONS.md`（4 个实体状态机 + 命令表 + 判定表） |
| 无法表达项有明确清单 | ✅ `docs/GAPS.md`（0 项 P0；14 项未决/未覆盖，Q1–Q14） |
| 测试用例可执行 + 日志 | ✅ 16 例 + `evidence/*.log` |
| 不接 KV / 不接网络 | ✅ 独立 crate，无 coord 依赖 |

## 关键建模判断（详见 `docs/MODEL.md` §3）

1. **Round 作用域是分支不是任务**：会签/或签需要「一个节点 N 席位」的统计，
   手册字段草稿 `Round.task_id` 与 `Task.assignee` 在多审批人语义下互斥。
2. **分支通过 ≠ 实例收口**：状态机无法得知定义是否还有后继节点，故通过后由
   驱动方 `finish` 显式收口；否决是终局（立即 rejected）。
3. **授权不在本原型**：加签/驳回/终止只记 `by` 与审计，权限判定必须单独设计
   （`GAPS.md` Q6）。
