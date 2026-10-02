# 依赖治理与 bincode 退场记录（已完成）

> Owner: maintainers ｜ Last verified: 2026-10-02

- **现状（完成态）**：bincode 已完全退场——依赖图无 bincode（`cargo tree -i bincode`
  无匹配）、`deny.toml` 无豁免条目；持久化与 RPC 载荷统一为信封 V2（postcard，
  `MAGIC(4B) | VERSION_V2(1B)`），无前缀 / V1 / RPC codec=0 等旧格式一律**显式拒绝**
  （不静默错读、不试错解码）。
- **目标（已达成）**：以分阶段、每步带判据的退场计划替换序列化格式并关闭豁免；
  后续依赖纪律 = 不得重新引入 bincode（`unmaintained = "all"` 且无豁免，重新引入直接
  fail-closed）。

---

## 1. 退场为什么分阶段（记录）

bincode 在本仓库不是工具库，而是**持久化格式**，直接承载多类**长期存活**的数据：

| # | 承载物 | 位置 | 退场后格式 |
|:--|:--|:--|:--|
| 1 | 快照 | `coord-server/src/storage/snapshot.rs` | 信封 V2（postcard；内部 `version=5`） |
| 2 | Raft 日志条目 | `coord-server/src/raft/log_store.rs`（Entry/Vote/Committed/LastPurged；region 模式复用同一实现） | 信封 V2 |
| 3 | auth 用户/角色/会话/吊销/bootstrap | `coord-server/src/auth/manager.rs`（`/_sys/auth/`） | 信封 V2（变体索引 = wire 的一部分，只允许末尾追加，见 `test_auth_op_variant_indices_appended`） |
| 4 | PD Region 元数据 | `coord-server/src/pd/meta_store.rs` | 信封 V2 |
| 5 | 对象存储 manifest | `coord-server/src/storage/object_store.rs` | 信封 V2 |
| 6 | `AppliedLogId` | `coord-server/src/storage/mvcc.rs` | `ALI1` 手写定长（无失败路径；旧 bincode 回退已删除） |
| 7 | raft RPC 载荷（跨节点） | `coord-server/src/raft/network.rs` | postcard（`payload_codec=1`；ADR-0007） |

⇒ 一次「换库」= **多处数据迁移 + 双向兼容窗口**；本仓当时无生产部署，按分级判据
逐阶段推进（见下一节表）完成。

---

## 2. 退场阶段（全部完成）

| 阶段 | 动作 | 判据（可执行） | 状态 |
|:--|:--|:--|:--|
| **P0 先立"格式可辨识"** | 给第 2/4/5 项加统一魔数 + 版本字节前缀 | 旧数据仍能解码；新数据带前缀；篡改前缀 ⇒ 显式报错 | ✅ |
| **P1 选型 + 双解** | 选型（ADR-0005：postcard，`VERSION_V2`）；读路径三路双解 + 精确消费 | 两种编码逐字节对照可往返；篡改矩阵全部显式失败 | ✅ |
| **P2 迁移窗口** | 写路径切 V2 + 四个直写面信封化（P2a 读先行 / P2b 写切换，ADR-0006）；RPC 载荷标记先读后写（R1/R2，ADR-0007） | 每面「旧行 / V1 / V2 / 混读 / 篡改」矩阵 + 写前缀白盒断言 + 负控制 | ✅ |
| **P3 关闭豁免** | 删除旧读腿与迁移阶梯；bincode 从依赖图消失；删 `deny.toml` 豁免 | `cargo deny check advisories` 无豁免；`grep bincode Cargo.toml */Cargo.toml` 与 `bincode::`（`.rs`）归零；全量测试绿 | ✅ |

> **完成记录**（2026-10-02）：P0–P3 落地，判据均以测试钉住——信封实现：
> `coord-server/src/storage/envelope.rs`（唯一 V2；旧格式显式拒绝）；各面拒绝用例
> （快照 / auth / SM 元数据 / PD 队列 / raft 四表 / PD Region / manifest / RPC）与
> 写路径白盒断言分布在对应模块测试中；负控制逐项实跑后还原。

**完成判据（已满足）**：`cargo deny check advisories` 无豁免；bincode 不在依赖图；
旧格式读写路径已删除（出现即为显式错误）。

---

## 3. 约定

- 格式演进只经**信封版本字节**（`storage/envelope.rs`）：新增字段/改结构属 Breaking，
  不得依赖 `#[serde(default)]` 之类的宽松解码行为。
- 篡改防护要求：每条读路径必须**精确消费**（拒绝尾随字节）且对未知版本/损坏前缀
  **显式失败**；新增持久化面必须复用统一信封。
- **不得**重新引入 bincode：`deny.toml` 无豁免（`unmaintained = "all"`），重新引入
  将直接 fail-closed。
