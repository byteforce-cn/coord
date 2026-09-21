# 依赖治理与 bincode 退场计划（W4-4）

- **日期**：2026-09-21
- **锚点**：`docs/production/production-readiness-plan-2026-09-21.md` §5 W4-4、P-Gate 6
  （「`cargo deny` 零未闭合豁免（当前 bincode 豁免须有替代路径）」）
- **现状**：`deny.toml` 的 `ignore` 只有 **1** 条：`RUSTSEC-2025-0141`（bincode 1.3.3
  被标记为 **unmaintained**，**不是漏洞**，且 `Solution: No safe upgrade is available!`）。
- **本轮变化**：把「替换序列化格式」从一句技术债，变成**分阶段、每步带判据**的退场计划。
  在计划完成之前，P-Gate 6 的该项**保持红**（豁免仍在）。

---

## §1 为什么不能"直接换掉"

bincode 在本仓库不是工具库，而是**持久化格式**，直接承载六类**长期存活**的数据：

| # | 承载物 | 位置 | 是否有版本化 |
|:--|:--|:--|:--|
| 1 | 快照 | `coord-server/src/storage/snapshot.rs` | ✅ `SnapshotDataV1/V2/V3` 三代兼容解码 |
| 2 | Raft 日志条目 | `coord-core/src/workflow/raft_store.rs` | ❌ 无显式版本信封 |
| 3 | auth 用户/角色/会话/吊销 | `coord-server/src/auth/manager.rs`（`/_sys/auth/`） | 🟡 变体索引即变体号（`test_auth_op_bincode_variant_indices_appended`） |
| 4 | PD Region 元数据 | `coord-server/src/pd/meta_store.rs` | ❌ |
| 5 | 对象存储 manifest | `coord-server/src/storage/object_store.rs` | ❌ |
| 6 | `AppliedLogId` 旧编码回退 | `coord-server/src/storage/mvcc.rs` | 🟡 专为兼容旧 bincode 编码而保留 |

⇒ 一次「换库」= **六处数据迁移 + 双向兼容窗口**。在没有生产部署之前做（现在是窗口），
成本最低；但**不能**在没有迁移期与回滚路径的情况下做。

---

## §2 退场计划（分阶段，每步可独立验收）

| 阶段 | 动作 | 判据（可执行） | 回滚 |
|:--|:--|:--|:--|
| **P0 先立"格式可辨识"** | 给第 2/4/5 项（当前无版本信封的）加**统一的格式魔数 + 版本字节前缀** | 新增单测：旧数据（无前缀）仍能解码；新数据带前缀；篡改前缀 ⇒ 显式报错（不是"解码成垃圾"） | 前缀写入可降级（解码兼容） |
| **P1 选型 + 双写（阴影）** | 选定替代（候选：`postcard` / `rkyv` / `wincode`），实现 `Codec` trait，**读路径双解**、写路径仍写 bincode | 全量 workspace 测试绿；新增对照测试：同一结构两种编码**逐字节可往返** | 删除新 codec |
| **P2 迁移窗口** | 写路径切到新格式（打新前缀）；读路径同时支持两种 | 快照/日志/auth/PD/manifest 五类各有「旧数据读 + 新数据读 + 混读」测试 | 切回旧写路径（旧数据未动） |
| **P3 关闭豁免** | 从 `deny.toml` 删除 `ignore` 条目，bincode 从依赖图消失（或仅测试用） | `cargo deny check advisories` 绿且 `grep -rn bincode Cargo.toml */Cargo.toml` 归零 | 恢复依赖 + 旧解码路径保留一个 minor |

**验收**：P3 完成 + `cargo deny` 无豁免 ⇒ W4-4 闭环、P-Gate 6 该项转绿。

---

## §3 与本轮其它改动的关系

- 阶段 **P0 的测试**（旧数据仍可解码）与 §7 证据规范的「负控制」要求一致：
  必须**正反两向**验证（能读旧数据 **且** 篡改前缀会显式失败）。
- 阶段 **P2 的混读测试**是 `check-wire-descriptor` 之外的**数据面**对应物：
  契约面已有 descriptor 卡口，数据面此前没有。
- **不得**以「bincode 有豁免所以可以再加一条豁免」的方式推进：`deny.toml` 的维护约定
  写明「`ignore` 只允许按『一个通告 + 一段理由 + 一条关闭路径』增长」。
