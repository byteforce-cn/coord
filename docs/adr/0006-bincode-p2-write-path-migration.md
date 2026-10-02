# ADR-0006: bincode 退场 P2——写路径 V2 迁移与剩余直写面信封化

- 状态：proposed
- 日期：2026-10-02
- 决策者：维护团队（本文档即 ADR-0005 决定 4 要求的 P2 单独设计评审材料；评审通过后转 accepted）
- 承接：ADR-0005（决定 4）；P0（统一信封）与 P1（读路径三路）已落地

## 背景

ADR-0005 选定 postcard 为替代编码（信封 `VERSION=2`），并给出迁移机制边界：
P1 只做读路径三路双解；P2 切写路径，并给其余直写面加信封/等价格式识别；P3
收尾删除。P2 要求**单独立项设计评审**（ADR-0005 决定 4），本 ADR 覆盖**持久化
面**；raft RPC 载荷编码标记按同条要求**单独评审**（ADR-0007），不在本文档范围。

决策相关现状：

| 面 | 写路径 | 读路径 | 备注 |
|:--|:--|:--|:--|
| raft 日志四表（Entry/Vote/Committed/LastPurged） | 信封 V1（bincode） | 无前缀 / V1 / V2 | P0 信封 + P1 三路 |
| PD Region 行（`pd/meta_store.rs`） | 信封 V1（bincode） | 无前缀 / V1 / V2 | 同上 |
| 对象 manifest 行（`storage/object_store.rs`） | 信封 V1（bincode） | 无前缀 / V1 / V2 | 同上 |
| 快照（`SnapshotData` 自版本化 v5 + v2–v4 迁移阶梯） | 无前缀 bincode | 无前缀 bincode + 阶梯 | 本轮信封化对象 |
| auth 记录 ×5（user/role/session/revoked/bootstrap） | 无前缀 bincode | 无前缀 bincode | 行值经快照原样透传 |
| SM 元数据（`META_SNAPSHOT` / `META_MEMBERSHIP`） | 无前缀 bincode | 无前缀 bincode | `PersistedSnapshotMeta` / `StoredMembership` |
| PD 队列条目（`PdQueueEntry`） | 无前缀 bincode | 无前缀 bincode | 行值经快照原样透传 |
| `AppliedLogId`（`META_LAST_APPLIED`） | `ALI1` 手写定长 | `ALI1` + bincode 回退 | 本 ADR 维持不动 |

其余持久化编码（`KvMetadata` 33B、`LeaseRecord` 24B、`ChangeEvent` v2 手写
定长、compacted 水位 8B 大端）不依赖 bincode、自带格式识别，P2/P3 均无需改动
——列出以核对 ADR-0005「载体与格式面清单（P1–P3 的完整覆盖面）」闭环。

## 决定

### D1 写路径统一 V2

`envelope::encode` 改为 `MAGIC(4B) | VERSION_V2(1B) | postcard(payload)`；
`VERSION=1` 降级为**只读兼容版本**。读路径保持三路（无前缀 bincode / V1
bincode / V2 postcard），三条均精确消费（拒绝尾随字节，与 P1 同口径）。

不引入运行期写版本开关：同一二进制只有一种写格式，避免配置漂移产生两种活跃
写格式——迁移中的"两种格式"只存在于读侧窗口。

### D2 四个直写面统一信封化

| 面 | 写 | 读 |
|:--|:--|:--|
| 快照 | `SnapshotData::to_bytes` ⇒ V2-postcard（内部 `version` 字段保持 5） | `from_bytes_migrating`：V2 ⇒ postcard 且 `version==5`（否则显式错）；V1 / 无前缀 ⇒ 现行为 bincode + v5→v4→v3→v2 迁移阶梯（语义不变） |
| auth 记录（5 类） | `to_bytes` ⇒ V2 | 三路；保持"损坏 ⇒ `None`，调用方跳过"现有语义 |
| SM 元数据 | `PersistedSnapshotMeta` 与 membership 写 ⇒ V2 | 三路；旧行兼容 |
| PD 队列条目 | `PdQueueEntry::to_bytes` ⇒ V2 | 三路；保持"损坏 ⇒ `None`"现有语义 |

要点：

- **不新增第二套信封**：复用 `MAGIC | VERSION` 布局与精确消费；实现上把
  "前缀判别"从泛型解码中抽为共享辅助，供快照的阶梯读与各面使用。
- 快照 V2 腿**只承载 v5**：postcard payload 只会由本轮写路径产生；bincode 腿的
  迁移阶梯保持现状，直到 P3 删除（见 D6）。
- 快照的 `SnapshotRawEntry.value` 原样透传：auth/PD 等内层行值各自带信封，
  外层快照不做二次解析。
- 四个面的旧格式读统一**收窄为精确消费**（与 P1 对三载体的收紧一致）；
  未发现依赖尾随容忍的写入方。

### D3 `AppliedLogId` 保持 `ALI1` 手写定长

不改为信封/postcard：该编码位于 apply 写事务热路径，设计约束是**无失败路径**
（见 `coord-server/src/storage/mvcc.rs` 注释）。P3 仅删除其 bincode 回退分支。

### D4 实施拆分与升级顺序（先读后写）

- 前置事实：三载体的"读路径先行"已在 P1 完成；四个新面尚无任何信封读。
- **P2a（读先行）**：四个新面接入三路读；所有写路径保持现状（三载体 V1、
  四表面无前缀 bincode）。
- **P2b（写切换）**：`encode` 与四个新面的 `to_bytes` 统一切 V2。
- 部署规则（约束多节点升级与回滚判定）：任一节点组合中，**全部节点先运行含
  P2a 的版本**，才允许 P2b 版本开始写新格式；P2b 之后回滚必须退到含三路读的
  版本，不得退到更早版本。
- 本仓当前无生产部署（`docs/production/ops/dependencies.md` 口径）：上述顺序
  以 PR/评审边界固化，在首个真实部署出现前把机制逐面验证完毕。

### D5 测试与负控制（P2a / P2b 各自可独立验收）

- 每面五类用例：旧行读 / V1 行读 / V2 行读 / 同库混读 / 篡改（魔数、版本
  字节、尾随、截断）⇒ **全部显式失败**（对齐 ADR-0005 的「V2 魔数破坏 ⇒ 全部
  显式失败」锚点）。
- 快照额外回归：v2–v4 阶梯迁移用例继续通过（bincode 腿）；V2 腿校验
  `version != 5` 显式错、尾随拒绝。
- 负控制逐项实跑（红后还原）：移除读判别 ⇒ 对应旧行用例红；写路径回退 V1 ⇒
  新前缀断言红；移除 V2 remainder 断言 ⇒ 尾随用例红。
- 全 workspace 测试绿；fmt / clippy（`-D warnings`）；`scripts/check-doc-refs.sh`。

### D6 P3 衔接

P2b 与 ADR-0007 的 RPC 写切换落地后，P3 删除：bincode 依赖（含 coord-core
未用声明）、envelope 的 V1/无前缀分支、快照 v2–v4 阶梯、auth/SM/PD 旧读、
`ALI1` bincode 回退、RPC codec=0 读，以及 `deny.toml` 豁免。判据与时间窗见
`docs/production/ops/dependencies.md` 与收尾追踪 issue。

## 后果

- 写侧收敛为单格式（V2）；读侧三路窗口在 P2a–P3 之间显式存在并被测试钉住，
  P3 后收敛为单路。新旧行并存是被验证的状态，而不是偶然行为。
- 尺寸收益（ADR-0005 实测 postcard 普遍小于 bincode）自 P2b 起覆盖全部持久化
  面；写编码约 2× 慢为百 ns/行量级，相对落盘/复制开销可忽略。
- 代价：P2a 期间四个面同时存在多种读分支，测试矩阵是主要成本，由负控制保真。
- 回滚：P2a 可独立回滚（删新读路径）；P2b 回滚需保留 P2a 读路径（不得退到
  P1 之前），与部署规则一致。

## 参考

- `docs/adr/0005-bincode-replacement-codec-selection.md`（选型与阶段边界；本 ADR
  承接决定 4）
- `docs/production/ops/dependencies.md`（P0–P3 判据与窗口口径）
- 追踪：issue #18（退场）、#24（P2）；RPC 标记设计 = ADR-0007
- 代码锚点：`coord-server/src/storage/envelope.rs`、`coord-server/src/storage/snapshot.rs`
  （`to_bytes` / `from_bytes_migrating`）、`coord-server/src/auth/manager.rs`
  （5 类记录）、`coord-server/src/raft/state_machine.rs`（`PersistedSnapshotMeta` /
  membership）、`coord-server/src/raft/type_config.rs`（`PdQueueEntry`）、
  `coord-server/src/storage/mvcc.rs`（`AppliedLogId`）、`coord-server/src/pd/meta_store.rs`、
  `coord-server/src/raft/log_store.rs`、`coord-server/src/storage/object_store.rs`
