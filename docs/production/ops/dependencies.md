# 依赖治理与 bincode 退场计划

> Owner: maintainers ｜ Last verified: 2026-10-02

- **现状**：`deny.toml` 的 `ignore` 只有 **1** 条：`RUSTSEC-2025-0141`（bincode 1.3.3
  被标记为 **unmaintained**，**不是漏洞**，且 `Solution: No safe upgrade is available!`）。
- **目标**：以**分阶段、每步带判据**的退场计划替换序列化格式，最终关闭该豁免。

---

## 1. 为什么不能"直接换掉"

bincode 在本仓库不是工具库，而是**持久化格式**，直接承载六类**长期存活**的数据：

| # | 承载物 | 位置 | 是否有版本化 |
|:--|:--|:--|:--|
| 1 | 快照 | `coord-server/src/storage/snapshot.rs` | ✅ `SnapshotDataV1/V2/V3` 三代兼容解码 |
| 2 | Raft 日志条目 | `coord-server/src/raft/log_store.rs`（Entry/Vote/Committed/LastPurged；region 模式复用同一实现） | ✅ 统一前缀信封（P0） |
| 3 | auth 用户/角色/会话/吊销 | `coord-server/src/auth/manager.rs`（`/_sys/auth/`） | 🟡 变体索引即变体号（`test_auth_op_bincode_variant_indices_appended`） |
| 4 | PD Region 元数据 | `coord-server/src/pd/meta_store.rs` | ✅ 统一前缀信封（P0） |
| 5 | 对象存储 manifest | `coord-server/src/storage/object_store.rs` | ✅ 统一前缀信封（P0） |
| 6 | `AppliedLogId` 旧编码回退 | `coord-server/src/storage/mvcc.rs` | 🟡 专为兼容旧 bincode 编码而保留 |

> 注：`coord-core` 声明的 `bincode` 依赖无源码使用点，随 P3 从依赖图删除。

⇒ 一次「换库」= **六处数据迁移 + 双向兼容窗口**。在没有生产部署之前做（现在是窗口），
成本最低；但**不能**在没有迁移期与回滚路径的情况下做。

---

## 2. 退场计划（分阶段，每步可独立验收）

| 阶段 | 动作 | 判据（可执行） | 回滚 |
|:--|:--|:--|:--|
| **P0 先立"格式可辨识"** | 给第 2/4/5 项（当前无版本信封的）加**统一的格式魔数 + 版本字节前缀** | 新增单测：旧数据（无前缀）仍能解码；新数据带前缀；篡改前缀 ⇒ 显式报错（不是"解码成垃圾"） | 前缀写入可降级（解码兼容） |
| **P1 选型 + 双解** | 选定替代（候选：`postcard` / `wincode` / `bitcode` / `rkyv`；**选型结论见 ADR-0005**（accepted：`postcard`）），信封层**读路径三路双解**（V1 / V2 / 无前缀）、写路径仍写 bincode | 全量 workspace 测试绿；新增对照测试：同一结构两种编码**逐字节可往返** | 删除新 codec |
| **P2 迁移窗口** | 写路径切到新格式（打新前缀）；读路径同时支持两种；剩余直写面逐个信封化（设计：ADR-0006；分 P2a 读先行 / P2b 写切换两步；RPC 载荷标记单独评审：ADR-0007） | 快照/日志/auth/PD/manifest 五类各有「旧数据读 + 新数据读 + 混读」测试 | 切回旧写路径（旧数据未动） |
| **P3 关闭豁免** | 从 `deny.toml` 删除 `ignore` 条目，bincode 从依赖图消失（或仅测试用） | `cargo deny check advisories` 绿且 `grep -rn bincode Cargo.toml */Cargo.toml` 归零 | 恢复依赖 + 旧解码路径保留一个 minor |

> **进度**：P0「格式可辨识」已落地（2026-10-02）——第 2/4/5 项写路径统一为
> `MAGIC(4B) + VERSION(1B) + bincode` 信封，读路径兼容无前缀旧行（实现：
> `coord-server/src/storage/envelope.rs`）；P1 选型已定（ADR-0005：postcard，
> `VERSION=2`），读路径三路双解（V1 / V2 / 无前缀）+ 精确消费（拒绝尾随字节）
> 已落地（2026-10-02）。
> **P2 设计评审已通过**（ADR-0006：持久化写路径 V2 迁移 + 剩余直写面信封化；
> ADR-0007：RPC 载荷编码标记，单独评审），实现拆分 P2a（读先行）/ P2b（写
> 切换）两步。
> **P2a 读先行已落地**（2026-10-02）：快照 / auth 记录×5 / SM 元数据 /
> PD 队列条目四个直写面接入三路读，旧格式读收窄为精确消费（快照的 bincode
> 迁移阶梯保留）；写路径保持现状——P2b（写切换）进行中。

**完成判据**：P3 完成且 `cargo deny` 无豁免。

---

## 3. 约定

- 阶段 **P0 的测试**必须**正反两向**验证（能读旧数据 **且** 篡改前缀会显式失败）。
- 阶段 **P2 的混读测试**是数据面的对应物：契约面已有 descriptor 校验，数据面此前没有。
- **不得**以「bincode 有豁免所以可以再加一条豁免」的方式推进：`deny.toml` 的维护约定
  写明「`ignore` 只允许按『一个通告 + 一段理由 + 一条关闭路径』增长」。
