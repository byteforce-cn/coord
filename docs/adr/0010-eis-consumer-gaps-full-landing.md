# ADR-0010: 全量承接 EIS 消费方能力缺口（G-*）：方向、批次与治理

- 状态：accepted
- 日期：2026-10-09
- 决策者：维护团队

## 背景

EIS 平台（IMS/IAM 的承载方，coord 的首个重消费域）对 coord v0.2.1（`2c61f23`）
完成逐服务复核，提出 15 项能力缺口（编号 G-*），覆盖 policy / mq / pki /
workflow / transit / event / scheduler 与消费方接入文档。需求原文、现状证据与
验收口径见「参考」中的 EIS 侧工件。

缺口分两类：

- **已指定采纳路径的生产化阻塞**：policy / mq / pki / transit，均落在
  2026-12-31 契约期；
- **组织级复用与演进的阻塞**：workflow / scheduler / event 及接入成本，
  2027-03-31 及后续。

维护团队按同版本代码逐条复核：全部缺口成立；其中 G-TR-1 的现状以「`rewrap`
已实现且有测试、但未暴露为 API；KEK 无多材料并存」为准。维护团队决定
**全量承接**（无驳回项），并以本 ADR 固定 coord 侧的落地口径、批次与治理要求，
作为排期、验收与对消费方回执的总锚点。

单一归属：缺口的问题陈述与原始证据归 EIS 侧文档；本仓只维护「coord 承诺
什么」——本 ADR、`apis/contracts/STATUS.md` 验收项、`docs/production/ops/boundaries.md`
边界。

## 决定

### 1. 批次与优先序

全量承接，按批次推进；批内按 P1 → P2 → P3 顺序，P3 资源冲突时可滑入后批，
但不滑出本 ADR 承接范围：

| 批次 | 缺口 | 目标时点 |
|:--|:--|:--|
| 1（立即） | G-PKI-1（补丁级）、G-X-1（文档） | G-PKI-1 尽早；G-X-1 2026-11 前 |
| 2（契约期） | G-POL-1、G-POL-2、G-MQ-1、G-MQ-2、G-MQ-3、G-MQ-4、G-PKI-2、G-PKI-3（设计先行）、G-TR-1 | 2026-12-31 |
| 3（契约期） | G-WF-1、G-SC-1 | 2027-03-31 |
| 随批（增量 P3） | G-EV-1（可随批 2 提前） | 不晚于 2027-03-31 |
| 产品决策 | G-TR-2（本 ADR 定谳：维持边界 + 触发条件） | — |

### 2. G-PKI-1：到期语义修正（补丁级）

- **决定**：到期 = 换新触发。`IssueCert` / `RotateCert` 对过期的当前记录走
  替换路径重签（新 serial、新密钥），**不再返回既有过期记录**；
  `RenewCert(serial)` 保留为按序列号恢复入口；三者与 `GetCertByCN`（仅未过期
  返回当前证书，否则 `NOT_FOUND`）语义一致。替换走并发安全的版本化 CAS；
  并发与重启一致性纳入用例。
- 选换新路径（而非「返回明确错误 + 文档化必须 `RenewCert`」）：消费者恢复
  路径最短，与 KMS 语义一致。
- **验收锚点**：短 TTL 用例——过期前 rotate 正常；过期后 `IssueCert` /
  `RotateCert` 得到新 serial/新密钥；`RenewCert(serial)` 可恢复；不等待大版本。

### 3. G-X-1：消费方契约文档

- **决定**：在 `docs/production/` 新增消费方契约页（按服务分节）：
  - 每服务一致性 / 路由 / 失败语义与推荐消费范式（如 MQ 正确性路径必须
    `Poll+Ack`，`Subscribe` 仅实时场景）；
  - 每 RPC 所需能力（capability）名，指向 `coord-core/src/grpc_auth.rs`
    能力表；
  - 已接受的既有边界清单，指向 `docs/production/ops/boundaries.md` 与
    proto 声明。
- Java SDK 补齐 README 并与契约页一致；每服务提供一条规范示例
  （落 SDK 既有示例面）。
- **单一归属**：契约页只做聚合与范式指引，事实源仍是 proto 声明 /
  boundaries / STATUS，禁止第三份口径。
- **验收锚点**：新消费者按文档 1 天内完成接入、对边界无歧义（以 IMS 二期
  接入为试点回执）。

### 4. G-POL-1：bundle 分发与启动加载

- **决定**：补齐分发语义，四件套：
  1. agent 启动时从 Server KV 加载全部 enabled bundles；
  2. 变更传播（Watch 或等效）并**文档化收敛时限**（目标 ≤10s 量级）；
  3. 版本一致性语义：并发 Put / 回滚后的最终状态一致、版本单调、无丢更新，
     错误语义明确；
  4. proto 边界声明补齐「分发 / 加载 / 收敛」行为，与
     `apis/contracts/STATUS.md` 验收项同步。
- 存储面（版本化 / Txn CAS / rollback）已完备，不改；改动聚焦分发与加载。
- **验收锚点**：A 上 Put → 重启 A → 判定不变；集群内 B（未处理写入）在收敛
  时限内同判；Rollback / SetEnabled 全 agent 收敛一致；并发 Put 版本单调。

### 5. G-POL-2：RBAC 管理面（明确边界）

- **决定**：不提供 RBAC 跨 agent 管理面。`CheckPermission` 明确为 **agent
  本地 / 嵌入式**用途；生产接入统一走 OPA bundle + `Evaluate`（以 G-POL-1
  为前置）。proto 边界声明与 SDK 文档标注；提供「装载 → 判定 → 重启仍生效」
  的替代路径 demo。
- 理由：避免 RBAC 结构化策略与 Rego 双策略面跨 agent 分发；与 proto 既有
  声明（RBAC 为 Agent 本地内存）一致。

### 6. G-MQ-1：消费路由官方方案

- **决定**：提供「Leader 可发现 + 失败可恢复」的官方方案：
  1. 非 Leader 错误必须**可编程判别**（错误码 / 结构字段或等效载体，禁止
     要求解析错误文案）并携带 leader 提示；
  2. 提供 Leader 查询 / 路由能力：RPC + SDK 自动路由，或查询接口 + 明确的
     重试范式；SDK 侧优先 Java；
  3. 文档化分区 / Leader / ISR 拓扑与消费者部署的对应关系、Leader 不可达 /
     切换时的错误码与恢复步骤（与 proto 语义一致）。
- **验收锚点**：双 agent（ISR 启用）——A 上 `Publish`、B 上 `Poll+Ack` 全量
  收到且分区内有序；kill Leader → 明确错误码；恢复后继续消费；**无静默
  丢失**。

### 7. G-MQ-2：DLQ 闭环（显式路径）

- **决定**：提供公开的显式管理 RPC（移入 DLQ，能力门禁；复用既有净额记账
  与 `reason/detail` 语义），并文档化毒消息处置流程。自动投毒判定（按重试
  次数 / 策略）不在本轮范围（该二选一方向的另一支）。
- **验收锚点**：构造毒消息 → 显式调用进入 DLQ；分区不被阻塞；DLQ 内容含
  原因可读。

### 8. G-MQ-3：消费组可观测（lag / offset）

- **决定**：新增每 (topic, group, partition) 的 committed offset / lag 指标
  （沿用既有 metrics 面与命名约定），并提供告警口径（`monitoring/` 示例与
  文档）。
- **验收锚点**：Prometheus 可查询 lag；滞后场景可观测、可告警。

### 9. G-MQ-4：topic 生命周期（删除与回收）

- **决定**：提供 topic 删除（能力门禁 RPC）：删除配置并回收该 topic 全部
  存量（消息 / DLQ / 消费位点 / 幂等索引），配额归还；删除后该 topic 读写在
  各 agent 返回明确错误，同名重建 = 空 topic。
- ISR 下删除语义必须**全域一致**（机制随实施设计：经复制通道下发删除决定
  或等效保障）；「先停读写再删除」的前置条件与并发失败语义文档化；
  `docs/production/ops/boundaries.md` B-PL-4(d) 对应更新。
- **验收锚点**：删除后配额归还可验证；前置条件外的行为有明确错误。

### 10. G-PKI-2：到期观测面

- **决定**：暴露到期观测面 = 指标（距到期窗口内的证书计数，窗口可配；基于
  既有 `is_expiring_soon` 语义）+ 告警 / 查询示例。不加 RPC（指标面足够）。
- **验收锚点**：Prometheus 查询与告警示例可复现并文档化。

### 11. G-PKI-3：KMS 语义与访问控制（设计先行）

- **决定**：交付并评审三件设计物：
  1. 访问控制模型：按 CN / 用途 scope 化，或明示「read = 私钥读取权」的
     定级口径；
  2. 吊销 / 泄露应急 playbook：CN 轮换流程（revocation 实现不在本轮）；
  3. HSM 路线：保持「非 HSM」显式边界，注明重新评估触发（组织合规要求
     HSM 时另行 ADR）。
- 落点：`docs/production/ops/security.md`（访问控制与应急口径）与
  `docs/production/ops/runbook.md`（演练 / 轮换操作）；涉跨模块取舍时新增
  ADR。
- 若设计评审采纳最小实现（如按用途 CN + scope 细化），另行验收。

### 12. G-TR-1：KEK 多材料与 rewrap

- **决定**：
  1. 多材料并存解密窗口：主材料 + 历史材料集；密文 / DEK 记录携带材料
     标识，解密按标识选材料（旧材料密文在新材料实例可读）；
  2. rewrap 挂为管理路径（API 或工具）：旧材料解出 DEK → 主材料重包；
     复用既有 `rewrap` 实现并扩展到跨材料；
  3. 演练文档（含回退路径）。
- 边界更新：`docs/production/ops/boundaries.md` B-SE-6 对应更新；模块头与
  实现对齐。
- **验收锚点**：旧材料密文在新材料实例可解密；rewrap 后仅新材料可解；演练
  含回退。

### 13. G-WF-1：workflow 补偿执行器与保留策略

- **决定**：书面承诺在 2027-03-31 契约期内交付：
  1. DSL 解释器 + Saga 补偿执行器 + 端到端验收；
  2. 实例 / 定义保留与归档策略（append-only 边界对应更新）；
  3. 语义文档：补偿幂等、重试 / 超时、信号 / 取消（与 proto 既有字段
     对齐）；
  4. 模块头与 STATUS 的承诺措辞收敛为同一口径。
- **验收锚点**：三步流程中途失败 → 逆序补偿且可重放；重启恢复；实例生命
  周期管理可用；文档齐备。

### 14. G-EV-1：event 持久化游标与补投

- **决定**：实现持久化游标与补投（增量能力）：订阅位点持久化，在保留窗口
  内按位点补投；为显式使用路径，既有默认推送语义不变。「断线不补投」边界
  更新为「默认不补投；显式使用持久化游标时可在保留窗口内补投」；契约与
  边界文档同步。
- **验收锚点**：断线重连后可按位点补投；投递窗口与语义在契约中明示。

### 15. G-SC-1：scheduler KV 存储与 result 持久化

- **决定**：交付 KV 存储实现（替代内存 HashMap）与 result 持久化，过既有
  验收项（2027-03-31 契约期）；边界文档同步。

### 16. G-TR-2：远程非对称签名（产品决策）

- **决定**：本轮不新增远程非对称签名能力（transit 维持对称加密 + HMAC；
  PKI 维持「密钥交付持有方」模型）。触发条件：组织 KMS 方向确定要求
  「私钥不出域」签名（token / 审计锚点等）时，另行立项评估（候选：
  transit 增 ES256 Sign，或独立 KMS 服务；先设计后实施）。

### 17. 边界稳定性与不扩承诺

- EIS 已接受的既有边界（cache 跨节点提交非原子、MQ `Subscribe`
  best-effort 与容量逐写严格拒绝、ratelimiter / circuitbreaker 本地态、
  idgen、lock 等）**保持语义稳定**；除 G-EV-1 的明示变更外不动。这些边界
  随 G-X-1 在消费方契约页显式展示。
- EIS 明确不要求 coord 承担的范围（业务数据存储、XA / 分布式事务、授权
  判定语义、全局有序 / exactly-once 等）继续不在承接范围。

### 18. 治理

- 落地三步不变：①更新 `docs/production/ops/boundaries.md`；②同步
  `apis/contracts/STATUS.md` 验收项；③负控制证据入测试与 PR。
- 新增 RPC 必须登记 `coord-core/src/grpc_auth.rs` 能力表（漏登记 = 鉴权
  开启时全部拒绝）。
- 契约增量（RPC / 字段、验收项增补）走契约变更流程
  （`apis/contracts/WHITEPAPER.md`），目标日期不得静默顺延；验收项增补先行
  覆盖 G-POL-1 / G-MQ-1 / G-PKI-1 / G-PKI-2。
- 实施中出现的跨模块设计细则（如 bundle 分发一致性、ISR 删除、KEK 多材料）
  如需独立决策：新增 ADR（编号递增）并回链本 ADR。
- 验收协作：EIS 侧以 seedj `coord-it` 消费侧用例交叉验证；coord 侧对应
  单测 / 集成测试，双方结果互为对应缺口的关闭判据。

## 后果

- 收益：EIS 的「生产多实例化」与「组织级复用」阻断被逐项解除；
  policy / mq / pki / transit 契约期内的验收面与文档面补齐；消费方接入
  成本由 G-X-1 显式收敛；G-X-1 的契约页成为后续所有消费方接入的单一入口
  （新服务上线须同步补页）。
- 代价：多条边界从「不承诺」转为承诺（B-PL-4(d)、B-SE-6、B-WF-1 等）；
  新增 RPC / 字段带来契约增量，需走契约流程；G-EV-1 语义从「断线不补投」
  变更为「可在保留窗口内补投」；批次 2 工作量集中。
- 不变量：ADR-0001 的默认关闭口径与 fail-closed 姿态不受影响；本 ADR 不
  扩大 EIS 非请求项范围；未列入本表的边界不获得新承诺。

## 参考

- 需求来源（外部工件，单一归属在 EIS 仓库）：《Coord 业务缺口需求
  （IAM/EIS 落地视角）》v1.0（2026-10-09），含 G-POL / G-MQ / G-PKI /
  G-WF / G-TR / G-X / G-EV / G-SC 编号与逐条验收口径。
- 契约与状态：`apis/contracts/STATUS.md`、`apis/contracts/WHITEPAPER.md`、
  `apis/contracts/proto/coord/` 下各服务契约。
- 边界：`docs/production/ops/boundaries.md`（B-PL-4、B-SE-6、B-WF-1 等）。
- 代码锚点：`coord-agent/src/services/policy.rs`、
  `coord-agent/src/services/mq.rs`、`coord-agent/src/pki.rs`、
  `coord-agent/src/pki_store.rs`、`coord-agent/src/services/transit.rs`、
  `coord-agent/src/services/workflow.rs`、`coord-agent/src/metrics.rs`、
  `coord-core/src/grpc_auth.rs`、
  `coord-java-sdk/src/main/java/cn/byteforce/coord/sdk/internal/rpc/MqClientImpl.java`。
- 相关 ADR：ADR-0001（默认关闭）、ADR-0002（cache 原子性边界）、
  ADR-0009（dev 预设）。
