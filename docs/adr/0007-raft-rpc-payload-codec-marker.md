# ADR-0007: Raft RPC 载荷编码标记（bincode 退场 P2-RPC）

- 状态：proposed
- 日期：2026-10-02
- 决策者：维护团队（本文档即 ADR-0005 决定 4 末句要求的**单独**设计评审材料；评审通过后转 accepted）
- 承接：ADR-0005（决定 4）；与 ADR-0006（持久化面 P2 设计）并列、独立评审

## 背景

raft 节点间通信（`coord-server/src/raft/network.rs`）把 openraft RPC 实体
（`AppendEntries` / `Vote` / `TransferLeader` / `InstallSnapshot` 请求与响应、
流式快照帧 `SnapshotStreamMessage`、`SubmitPdOp` 请求/响应）用 **bincode**
序列化后装入 `coord-proto` 的 `RaftMessage.payload`（proto `bytes` 字段）。
bincode 退场 P3 要求删除该依赖，因此这些载荷必须切换编码；ADR-0005 决定 4
将其定为**单独评审**项，并给出两个约束：**涉及 coord-proto**、**不用试错解码**。

与持久化行的差异（决定设计的三个事实）：

1. **瞬时字节、无持久化**：切换不涉及历史数据迁移，只有跨版本节点间互操作；
2. **不得试错解码**：持久化行可以"按结构探测"兜底（历史包袱），RPC 载荷不可以
   ——把 postcard 字节交给 bincode 解码存在**静默解出差值**的理论窗口（同
   ADR-0005 魔数实验一类的宽松解码风险），必须以显式标记分派；
3. **可选认证**：共享密钥路径对 payload 计算 HMAC（`auth_tag`）；标记若不在
   认证覆盖内，被篡改后可能把一次合法签名的载荷重定向到另一种解码器。

现状要点：

- `RaftMessage { payload: 1, region_id: 2, trace_context: 3, auth_tag: 4 }`
  （proto3）；
- 服务与消息定义在 `coord-proto/src/proto/raft.proto`；contract 检查（buf lint +
  breaking）是 CI 门，字段**新增**属于兼容演进；
- 另一候选形态是在 payload 内加魔数前缀（复用存储信封）。

## 决定

### D1 显式标记字段（proto 层）

`RaftMessage` 新增 `uint32 payload_codec = 5;`：

- `0` = bincode（**兼容默认**：历史发送方不带该字段，proto3 读作 0）；
- `1` = postcard；
- 其他值 ⇒ **fail-closed** 拒绝（不猜测、不回落）。

所有经 `RaftMessage.payload` 的请求与响应（含流式快照帧、`SubmitPdOp`）统一
适用；两个方向各自设置自己的标记。

### D2 读取按标记分派，禁止试错

接收侧先读 `payload_codec`，按标记选择唯一解码器；**不得**先试一种再回退另一种。
未知标记在认证/解码前显式拒绝（错误信息用稳定措辞）。

### D3 认证覆盖标记（共享密钥路径）

- `payload_codec = 0`：**保持** `auth_tag = HMAC(secret, payload)`（与既有节点
  字节级互操作，混布窗口内不改）；
- `payload_codec = 1`：定义为 `auth_tag = HMAC(secret, DOMAIN || codec || payload)`，
  `DOMAIN` 为固定域分离标签（如 `b"coord-raft-payload-v2"`）。

不对称是刻意设计：既保证旧节点互操作（codec=0 输入不变），又保证新编码的标记
被认证覆盖——篡改标记 `0↔1` 会使两条校验路径都失败，不会把合法签名的载荷重定向
到另一种解码器。未配置共享密钥（mTLS 或无认证）时沿用现有"载荷不认证"语义。
`region_id` / `trace_context` 不在 `auth_tag` 覆盖范围（既有现状），不在本 ADR 范围。

### D4 升级顺序（先读后写，两步）

- **R1（读双分派）**：识别并正确处理标记 0/1；写仍为 0（字节与 MAC 不变）。
  全集群升级到含 R1 的版本。
- **R2（写切换）**：发送端写 1（含新 MAC 输入）。
- 规则：**在全部接收方运行 R1 之前，任何节点不得发送 1**——旧接收方忽略未知
  proto 字段后会按 bincode 解码 postcard 字节，多数形状显式失败，但存在理论上的
  静默窗口，必须靠顺序消除。R2 后回滚需退到含 R1 的版本。
- P3（删除 bincode）在 R2 全量部署后执行：删 `payload_codec = 0` 读取，发送端
  恒为 1；`payload_codec` 字段保留（值非 1 ⇒ 显式拒绝）。

### D5 判据与测试

- 单测：标记 0/1 各自双向往返；未知标记（如 2）显式拒绝；标记缺失（=0）按
  bincode 处理；
- 认证：codec=0 的 MAC 与既有实现逐字节一致（回归向量）；codec=1 的 MAC 覆盖
  标记（篡改 `payload_codec` ⇒ 校验失败）；`DOMAIN || codec || payload` 的编码
  规范写入测试（防两侧实现漂移）；
- 篡改矩阵：标记置换 / 截断 / 尾随 ⇒ 显式失败，不得静默解出值；
- 负控制（实跑红后还原）：去掉标记分派（改试错回落）⇒ 对应用例红；去掉 MAC
  域分离 ⇒ 标记篡改用例红。

### D6 考察过的替代（不选）

- **payload 内魔数前缀（复用存储信封）**：标记在协议层不可见（descriptor / buf
  检查看不到），且"RPC 载荷带格式前缀"的认证设计需另行规定；两个问题都指向
  协议层显式字段，故不选。
- **试错解码（先 postcard 再 bincode，或反向）**：静默错解风险，明确禁止
  （ADR-0005 决定 4 已给同条约束）。
- **同一版本同时读双写新**：违反"先读后写"，在混布集群产生静默窗口；本仓无
  生产部署也保留该纪律，为首个真实部署固化语义。

## 后果

- coord-proto 以字段新增方式兼容演进，contract 检查可覆盖；
- 与持久化面相同，读窗口（0/1 双分派）在 R1–P3 之间显式存在，P3 收敛为单路；
- 认证新增域分离输入，需要 R1/R2 两步跨版本协同；具体升级 runbook 另行补充
  （不在本 ADR 展开）。

## 参考

- ADR-0005（决定 4：RPC 载荷编码标记单独评审）、ADR-0006（持久化面 P2 设计）
- `coord-proto/src/proto/raft.proto`（`RaftMessage`）；`coord-server/src/raft/network.rs`
  （`serialize_payload` / `deserialize_payload` / `compute_raft_auth_tag` /
  `verify_raft_auth`）
- 追踪：issue #18、#24（P2）
