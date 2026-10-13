# Spike 6 · 形态决策 —— 证据索引与判据对照

> Owner: Spike 执行（AI 协作者）｜ Last verified: 2026-10-13

**决策与判据对照已单一定义在 [ADR-0013](../../../docs/adr/0013-workflow-approval-v2.md)（proposed）**：
形态选择（D1）、判据对照表（D10.1）、H1–H5 结论（「待验证假设与触发重议」）、
边界与迁移策略均在该 ADR，本目录不再复制内容。

| 验证项 | 状态 | 证据 |
|:--|:--|:--|
| Spike 1 状态模型 v2 | ✅ 完成（P0 语义 16/16） | `../spike-1-state-model/README.md` |
| Spike 2 单写者并发 | ✅ 完成（真实 Raft 7/7；fencing 方案 + 残余风险） | `../spike-2-concurrency/README.md`、`DESIGN.md`、`STEPTOKEN.md` |
| Spike 3 Outbox + 恢复 | ✅ 完成（kill -9 wait/listen/retry 三场景 PASS） | `../spike-3-recovery/README.md`、`OUTBOX.md`、`RECOVERY.md` |
| Spike 4 规范符合性 | ✅ 完成（24 语料；语义可用 13/22） | `../spike-4-conformance/REPORT.md` |
| Spike 5 基准 | ✅ 完成（安静机 canonical；非目标硬件/阈值未定稿） | `../spike-5-bench/REPORT.md` |
| 前置输入台账 | 3.2/3.3 未到位（临时口径已标注） | `../CHARTER-INPUTS.md` |

**当前决策**：扩展现有 `coord-core/workflow`（ADR-0013 D1）。
H1/H2 成立且各带落地缺口（写入 ADR D3/D4/D5）；**H3（性能）在 10-18 阈值定稿 +
目标硬件/release 复测前无法判定**，ADR 保持 proposed，不新建 crate 但保留触发重议。
ADR 转 accepted 的前提：阈值判定完成 + CTO 与契约 owner 评审；若 H3 不成立，
按 ADR-0013 判据表重议（新 ADR），不改写本决策而不留痕。
