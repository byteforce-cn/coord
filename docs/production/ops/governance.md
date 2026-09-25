# 治理：缺陷 SLA / 门禁责任人 / 承诺变更流程（W9）

- **日期**：2026-09-21
- **锚点**：`docs/production/production-readiness-plan-2026-09-21.md` §5 W9、P-Gate 7/9
- **判据**：本文件存在 + 每条规则给出**触发条件、时限、责任人角色、留档位置**。
  责任人填**角色**而非个人（组织事实无法由仓内产物证明，按 §1.3 的口径也**不**假装能）。

---

## §1 缺陷分级与 SLA（沿用既有定义，补"谁盯"与"留档在哪"）

分级定义沿用 `jepsen/docs/dev.md` §6（P0/P1/P2），**不重新发明**。
下表只补治理要素：

| 级 | 触发 | SLA | 责任人（角色） | 留档位置 | 升级条件 |
|:--|:--|:--|:--|:--|:--|
| **P0** | 数据丢失/错误、线性违反、锁双持、越权放大、写错 raft 组 | **48h 内**出最小复现；修复或按双签豁免前不得收口 | 值班 owner（当周的主程） | `jepsen/docs/coord-findings.md` 对应 `F-xx` + 证据目录 | 48h 未复现 ⇒ 上报决策方，进入 §5 的 Go/No-Go 复查 |
| **P1** | 可用性/RTO 不达标、行为未定义但无数据错误、agent 缓存 stale 有界可绕过 | 里程碑前修或**书面接受**（写进 `boundaries.md`） | 模块 owner | `coord-findings.md` + `remaining-known-gaps.md` | 连续两个里程碑未处置 ⇒ 视为 P0 |
| **P2** | 报错不友好、指标缺失、文档不符 | 记录即可 | 模块 owner | `remaining-known-gaps.md` | 同类重复 3 次 ⇒ 升 P1 |

**已有实例（本轮）**：
- F-27（lease 过期 revoke 丢失）= P0，已按此流程走：`F-27` 条目 + `store/coord/<ts>/` 复跑归档 +
  `docs/production/production-readiness-plan-2026-09-21.md` §11 台账。
- F-68（checker 缺 `start_offset`）= P1，已修 + 两份 lab 归档 + 负控制 fixture。

---

## §2 门禁责任人

| 门禁 | 文件 | 责任人（角色） | 何时跑 | 红了怎么办 |
|:--|:--|:--|:--|:--|
| 契约 wire 同步 | `apis/contracts/scripts/check-wire-sync.sh` | 契约 owner | 每次 PR + contract-check workflow | 补契约或补实现，**不得**放宽脚本 |
| descriptor 漂移 | `apis/contracts/scripts/check-wire-descriptor.sh` | 契约 owner | 同上 | 同上 |
| SDK 同步 | `apis/contracts/scripts/check-sdk-sync.sh` | SDK owner | 同上 | 重新生成 SDK |
| panic 0 违规 | `scripts/check-panics.sh` | 平台 owner | 每次 PR | 改 `match`/`if let`，**不得**加 `expect`（见既有教训） |
| 错误码契约 | `scripts/check-error-code-contract.sh` | 契约 owner | 每次 PR | 补 `x-coord-error-code` trailer |
| 告警↔runbook 绑定 | `scripts/check-gate-drills.sh`（**本轮新增**） | 运维 owner | 每次 PR | 补 `runbook_url` 或补 runbook 小节 |
| buf lint / breaking | `.github/workflows/contract-check.yml` | 契约 owner | 每次 PR | 破坏性变更走 §3 |

> **纪律**：门禁**红了不许绕过**。本仓库已明确记录过「红色的门禁不是严格的门禁，
> 是教人忽略的门禁」（`remaining-known-gaps.md:249`）。放行只有一条合法路径：
> 走 §3 的承诺变更流程并**公开记录**。

---

## §3 承诺变更流程（演练一次）

沿用 `apis/contracts/WHITEPAPER.md` §11.2 的**协议变更 PR Checklist**（强制），
本项目在它之上补三条**治理级**要求：

1. **谁批**：承诺面（`STATUS.md` 的 COMMITTED/STABLE 集合）变更必须由
   **契约 owner + 决策方**双签；`boundaries.md` 里「不承诺 → 承诺」的移动只需契约 owner 签。
2. **留档**：裁定记录写进 `docs/production/production-readiness-plan-2026-09-21.md` §8.3
   （格式：裁定内容 / 裁定人 / 日期 / **落地证据（可重跑）**）。
   §8.3 已有的 U-01/U-02/U-03 是本流程的样例。
3. **证据**：任何承诺升级都必须附**负控制**（§7 证据规范第 3 条）。
   只写"已支持"而不给负控制的变更**退回**。

### 3.1 本轮的流程演练（真实发生）

| 步 | 实例 |
|:--|:--|
| 触发 | U-01（免责声明与承诺互斥，`README.md:17`） |
| 双签 | 决策方（2026-09-21）+ 契约 owner |
| 留档 | §8.3 U-01 行，含「取②限定」与落地证据（两份 README diff、`grep` 归零） |
| 证据 | 文本 diff（可 `git diff` 复核） |
| 结论 | 流程**可执行**；唯一未闭合处是「裁定人」在仓库里只能记成角色/会话，属组织事实 |

---

## §4 未决项台账的维护

- 未决项（U-04…U-10）在 `production-readiness-plan-2026-09-21.md` §8.2，
  每项必须有：**需谁定 / 截止 / 本计划中的位置**。
- **到期未裁定的未决项**：自动升级到 §5 的 Go/No-Go 复查（因为未裁定会让某道门无法判定）。
- 已裁定的**不得**留在 §8.2 假装未决；必须移到 §8.3 并给落地证据。

---

## §5 Go/No-Go 复查节奏

- 每次「对外声明」之前跑一遍 §9 的一票否决清单（8 条）。
- 一票否决清单里**可自证**的（P-Gate、F-27、常驻红门禁、免责声明、证据签回）由 CI + 文档自证；
- **不可自证**的（人力、引入方签字）必须在 Go/No-Go 会议**当场出示**，
  不得以"应该没问题"代替；第三方审计报告已按 U-14（2026-09-25）移出门槛 ⇒
  反向义务：**任何声明不得暗示"已审计"**（计划书 §9-4）。

---

## §6 本轮治理面的交付

| 项 | 产物 | 判据 |
|:--|:--|:--|
| 缺陷 SLA | 本文 §1（沿用既有定义 + 补治理要素） | 本文存在；分级定义与 `jepsen/docs/dev.md` §6 一致 |
| 门禁责任人 | 本文 §2（含本轮新增的 `check-gate-drills.sh`） | 上表门禁文件**都存在**（可 `ls`） |
| 承诺变更流程 | 本文 §3 + §8.3 的 U-01/U-02/U-03 实例 | 实例有裁定人 + 日期 + 可重跑证据 |
| 流程演练 | §3.1（U-01 全流程） | 该次裁定可在 git diff 中复核 |
