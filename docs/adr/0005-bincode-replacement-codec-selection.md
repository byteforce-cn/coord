# ADR-0005: bincode 退场 P1——替代序列化格式选型（postcard）

- 状态：proposed
- 日期：2026-10-02
- 决策者：维护团队（待评审）

## 背景

`bincode` 1.3.3 已被标记 unmaintained（RUSTSEC-2025-0141；`deny.toml` 豁免），
退场计划 P0–P3 见 `docs/production/ops/dependencies.md`。P0「格式可辨识」已给
raft 日志行 / PD Region 行 / 对象 manifest 行加统一信封
`MAGIC(4B) + VERSION(1B) + bincode`（实现锚点 `coord-server/src/storage/envelope.rs`）。
本 ADR 解决 P1 的**选型**：信封 `VERSION=2` 承载哪种替代编码，以及由此确定的
迁移机制边界。

### 硬约束：serde 数据模型

持久化载荷大量由 **openraft 的外部类型**构成（`Entry`/`Vote`/`LogId`/`Membership`/
快照元数据、raft RPC 请求/响应）；coord 不能给上游类型追加派生。替代方案必须
直接消费现有 `serde::Serialize/Deserialize` 面——否则需要为上游类型手写序列化
实现，新增 unsafe 面并持续跟随上游版本变更（本仓库 unsafe 仅 6 处针脚级原语）。

### 载体与格式面清单（P1–P3 的完整覆盖面）

| 面 | 位置 | 现状 |
|:--|:--|:--|
| raft 日志行（Entry/Vote/Committed/LastPurged） | `coord-server/src/raft/log_store.rs` | P0 信封 |
| PD Region 行 | `coord-server/src/pd/meta_store.rs` | P0 信封 |
| 对象 manifest 行 | `coord-server/src/storage/object_store.rs` | P0 信封 |
| 快照（v2–v5 自版本化阶梯） | `coord-server/src/storage/snapshot.rs` | 直写 bincode |
| auth 记录（bootstrap/session/user/role/revocation 等） | `coord-server/src/auth/manager.rs` | 直写 bincode |
| SM 元数据行（`PersistedSnapshotMeta` / membership） | `coord-server/src/raft/state_machine.rs` | 直写 bincode |
| PD 队列行（`PdQueueEntry`） | `coord-server/src/raft/type_config.rs` | 直写 bincode |
| raft RPC 载荷（跨节点传输） | `coord-server/src/raft/network.rs` | 直写 bincode |
| `AppliedLogId` 旧编码回退 | `coord-server/src/storage/mvcc.rs` | 手写编码 + bincode 回退 |

### 候选域与判据

候选域 = issue #18 通告给出的 alternatives：`postcard` / `wincode` / `bitcode` /
`rkyv`（bincode 家族因通告判定「无可用升级」整体排除）。判据：

1. **serde 兼容**（硬约束，见上）；
2. 线格式稳定性承诺（数据长期存活，需有规格而非仅「兼容 bincode」）；
3. 严格解码 / 显式失败（对齐 P0 判据）；
4. 维护活跃度、许可证（`deny.toml` 白名单）、MSRV、依赖面；
5. 尺寸与吞吐（非决定性，但需实测）；
6. 不新增 unsafe 面。

## 评估（实测，2026-10-02）

> 方法：载体形状镜像（openraft `Entry`/`Vote`、`RegionMeta`、`ObjectManifest`、
> `AuthRole`、AuthOp 风格枚举、`SnapshotData`、`PersistedSnapshotMeta`），
> 不可压缩伪随机负载，release（Rust 1.98.1）。实验台不入库；本 ADR 记录关键数字。

### 尺寸 / 吞吐（代表性截面）

| 形状（字节） | bincode | postcard | bitcode |
|:--|--:|--:|--:|
| Entry 行（120B 载荷） | 157 | 126 | 130 |
| RegionMeta 行 | 140 | 70 | 81 |
| AuthRole 行 | 122 | 59 | 61 |
| 对象 manifest 行 | 265 | 174 | 185 |
| 快照 ~8KB / ~530KB | 5610 / 608810 | 4580 / 582481 | 4489 / 577238 |

吞吐（Entry 行编码/解码）：bincode 83/172 ns；postcard 161/122 ns；bitcode
689/691 ns。快照 ~530KB 解码同为 ~0.53–0.57 ms 量级。rkyv 尺寸 ≈ bincode
（168B/144B/5688B），零拷贝模型见下。

### 解码严格性（实测）

- **尾随字节**：bincode 与 postcard 的顶层便捷函数**均静默忽略**（`decode(bytes+[0xDE,0xAD])`
  返回 `Ok`）；bitcode、rkyv 拒绝；wincode 默认忽略、`deserialize_exact` 拒绝。
  ⇒ 无论选谁，**精确消费必须在信封层显式强制**（postcard 用 `take_from_bytes` 取
  remainder，实测可行；bincode 亦有 `reject_trailing_bytes` 选项，但其问题主体是
  可维护性而非能力）。
- 截断（全部前缀长）：各候选全部拒绝，无静默通过；Option 非法标签（0x02/0xFF）
  与枚举未知变体：postcard / bincode / bitcode 全部显式 `Err`。
- **魔数破坏回退路径（P0 已知窗口）实测**：V1 行（bincode payload）魔数单字节破坏
  后走旧格式整行解码——`EntryLike` 5 个取值中 0x01 一例静默解出差值；`VoteLike`
  **5/5 全部静默解出差值**（无结构约束可兜底）；`RegionMetaLike` / 快照元数据行
  全部显式 `Err`（行 key 不变量兜底）。**同一实验对 V2 行（postcard payload）：
  3/3 类型、5/5 取值全部显式 `Err`**——postcard 字节几乎不可能按 bincode 结构
  成立，该窗口在 V2 形状下关闭（P1 测试须钉住此性质）。

### 候选结论

- **postcard 1.1.3（选定）**：纯 serde 插件，直接依赖仅 `serde` + `cobs`
  （+ 默认关闭的可选 features）；v1.0 起「documented and stable wire format」
  （线格式规格随仓发布，README「Format Stability」）；MIT OR Apache-2.0；
  无 unsafe 面新增大项；尺寸普遍优于 bincode（小结构约减半），解码持平或略优，
  小结构编码约 2× 慢（百 ns 量级，相对 raft/落盘开销可忽略）。
- **wincode 0.6.2（不选，记录理由）**：唯一「零数据迁移」路径——实测 9/9 形状
  （含 8KB 快照、Option、枚举、元组、嵌套、字符串）与 `bincode::serialize`
  **逐字节相同**（其 README 承诺「Produces the same bytes as bincode for the
  covered shapes when using bincode's default configuration, provided your
  SchemaWrite/SchemaRead schemas and containers match the layout implied by your
  serde types.」），并有 `deserialize_exact`。但其 `SchemaWrite`/`SchemaRead`
  是 **unsafe trait** 且**没有 serde 桥**：全部类型（含 openraft 外部类型与
  raft RPC 类型，十余个、部分为泛型）需手写 `unsafe impl` 逐字节复刻 bincode
  布局并持续跟随上游 alpha 变更——与 unsafe 立场冲突、错误代价为静默数据损坏；
  集合类型需 containers/适配器（源码核查），递归类型派生实测触发 E0391；
  线格式承诺依附 bincode 兼容而非独立规格（0.x 版本）。此路径在有 serde 桥或
  上游官方适配前不具备工程可行性。
- **bitcode 0.6.9（不选）**：serde 模式可用（`bitcode::serialize/deserialize`）、
  尾随拒绝、体积最小档；但编解码慢 5–7×（Entry 行 690ns vs 96–161ns），整数为
  自研变长编码（`-1i64` 占 9B），线格式无独立稳定性规格（0.x），收益不足。
- **rkyv 0.8.18（不选）**：零拷贝归档模型与「整行读写」的行存储不匹配；同样
  无 serde 桥、需全量派生（外部类型不可行）；尺寸/严格性相对 postcard 无优势。

## 决定

1. **payload 编码选定 `postcard`（1.1.3）**：信封 `VERSION=2 ⇒ postcard(payload)`；
   `VERSION=1` 仍为 bincode；无前缀行仍按历史 bincode 解码。引入方式：
   `default-features = false` + 仅启用 `alloc`/`use-std`。
2. **精确消费在信封层强制**：V2 解码必须 `take_from_bytes` 并断言 remainder 为空；
   不得使用静默忽略尾随字节的便捷函数（bincode 现状同理，P1 一并收紧）。
3. **P1（实现另 PR）**：读路径三路（V1 / V2 / 无前缀）；写路径仍 V1；对每个已
   信封化载体补「旧行读 / 新行读 / 混读 / 前缀与尾随篡改显式失败」正反用例与负
   控制；「V2 魔数破坏 ⇒ 全部显式失败」进入测试锚点。
4. **P2（另设计评审）**：写路径切 V2；其余直写面（快照 / auth / SM 元数据 /
   PD 队列）逐个信封化或加等价格式识别；滚动升级遵循「先全集群升级读路径、再切
   写路径」；raft RPC 载荷的编码标记单独评审（涉及 coord-proto，不用试错解码）。
5. **P3**：删除 bincode 依赖、V1 读路径、无前缀回退与 deny 豁免（时间窗按依赖
   治理约定判定；coord-core 的未用 bincode 依赖一并删除）。
6. **不做**：不为上游类型手写 unsafe 序列化（排除 wincode 路径）；不采用
   bitcode / rkyv；信封层校验和（如 CRC）不在本 ADR 范围，P2 按新依赖流程单独评审。

## 后果

- 尺寸普遍缩小（小结构 20–50%），写编码约 2× 慢（百 ns/行，占比可忽略），
  解码持平或略优。
- 严格性提升：V2 行在魔数破坏实验中全部显式失败（V1 的 `Vote` 类静默窗口不再
  出现于 V2 形状）；遗留 V1 行的窗口在 P3 移除回退路径后关闭。
- 迁移成本集中在 P1/P2 读路径与测试；仓库尚无生产部署（dependencies.md 口径），
  窗口成本最低。
- 依赖面：`+postcard(serde, cobs)`；`−bincode` 在 P3 完成。
- 回滚：P1 不动旧数据、不切写路径，删除新 codec 即回滚；P2 切写后回滚需保留
  窗口内 V2 行的读路径。

## 参考

- 计划与豁免：`docs/production/ops/dependencies.md`（P0–P3 与判据）；`deny.toml`
  （RUSTSEC-2025-0141 豁免与关闭路径）。
- 追踪：issue #18（退场）；#19 / PR #20（P0 信封）；本 ADR 对应 P1 选型。
- 代码锚点：`coord-server/src/storage/envelope.rs`、`raft/log_store.rs`、
  `pd/meta_store.rs`、`storage/object_store.rs`、`storage/snapshot.rs`、
  `auth/manager.rs`、`raft/state_machine.rs`、`raft/type_config.rs`、
  `storage/mvcc.rs`、`raft/network.rs`。
- 实验（不入库）：形状镜像 + 尺寸/吞吐/严格性/篡改矩阵；候选版本 postcard 1.1.3 /
  wincode 0.6.2 / bitcode 0.6.9 / rkyv 0.8.18，2026-10-02，Rust 1.98.1（release）。
