# Coord 平台对外协议兼容性白皮书（v1.0.0）

> 版本：contracts/v1.0.0 ｜ 冻结日期：2026-08-27 ｜ 基线代码：`82eb398`（main）
>
> **文档地位**：本白皮书是 Coord 平台对上游业务方（100+ 微服务，Java/Go）的**协议稳定性承诺**。
> 它与内部实现（Raft / openraft / redb）彻底解耦：内部重构只受 CI 门禁约束，不影响本承诺。
>
> **重要声明**：本白皮书是**协议兼容性承诺**，不是生产就绪声明。平台整体生产就绪验收
> 以 `docs/production-readiness-remediation-2026-08-27.md` §6 的 9 道验收门为准（当前未全部通过，
> 见本文 §11 已知限制）。协议承诺自冻结日起先行生效。

---

## 1. v1 承诺范围

### 1.1 承诺的服务（绿区）

以下服务通过 Coord Server **客户端 gRPC 端口**（默认 `50051`，`config.example.toml:15`）提供，
纳入 v1 稳定承诺，受 `buf breaking` 门禁保护：

| 包 | 服务 | RPC | 承诺级别 |
|:---|:---|:---|:---:|
| `coord.kv` | KV | `Put` / `Range` / `Delete` | **稳定** |
| `coord.txn` | Txn | `Txn` | **稳定** |
| `coord.lease` | Lease | `LeaseGrant` / `LeaseRevoke` / `LeaseKeepAlive`（双向流） | **稳定** |
| `coord.watch` | Watch | `Watch`（双向流） | **稳定** |
| `coord.maintenance` | Maintenance | **仅** `Status` | **稳定（裁剪）** |
| `grpc.health.v1` | Health | `Check` / `Watch` | **稳定（标准协议）** |

契约文件：`apis/contracts/proto/`。**判定依据**：KV/Txn/Watch/Lease 为 P0/P1 完成域，
全量测试 1656 passed / 0 failed（2026-08-27 实测，见整改文档 §9.1.1），多节点 Raft 集群
测试（Leader 故障切换 / 网络分区 / 成员变更）全绿。

### 1.2 明确不承诺的接口（黄区与红区，见 §2 矩阵）

- **Maintenance 的其余 RPC**（`Seal`/`Unseal`/`Snapshot`/`Compact`/`MemberAdd`/`MemberRemove`/
  `MemberPromote`/`MemberList`/`Join`）：线端存在但仅供**平台运维侧**使用，
  业务方禁止依赖；`StatusResponse` 中编号 2–5 字段（raft_index/raft_term/raft_leader/
  seal_status）为内部运维字段，已在契约中 reserved，不属于承诺语义。
- **Auth 服务**：线端已提供服务，但凭证体系处于 CCT v3 迁移中期
  （`auth.proto:172` 旧 `token` 字段已标记 deprecated），为避免把过渡形态固化为承诺，
  **不纳入 v1**，列为 v1.1 候选（见 §12）。已启用 Auth 的集群中，业务方经
  `authorization: Bearer <token>` metadata 认证（见 §6），该 metadata 约定本身纳入承诺。
- **Agent 侧全部服务**（`coord.agent` 包：Registry/Config/Lock/IdGen/LeaderElection/Event/
  Cache/MQ/Scheduler/Workflow/Policy/Handshake/Replica）：见 §2 黄区/红区与 §8 红线。

---

## 2. 能力成熟度矩阵（源码考古结论）

> 考古日期 2026-08-27。每项结论附**代码证据**（文件:行号 @ `82eb398`）。
> 图例：🟢 稳定可承诺 ｜ 🟡 存在但暂不承诺（候选/运维专用）｜ 🔴 实验/残缺，严禁对外承诺

| 能力域 | 级别 | 结论与证据 |
|:---|:---:|:---|
| KV（Put/Range/Delete） | 🟢 | P0 完成；服务挂载 `coord/src/main.rs:2693`；契约 `coord/kv/kv.proto` |
| Txn（CAS 原子事务） | 🟢 | P1 完成；单 Raft 日志条目原子提交；`coord/src/main.rs:2694` |
| Watch（双向流） | 🟢 | P1 完成；背压/溢出协议见 §7.3；`coord/src/main.rs:2696` |
| Lease（含 KeepAlive 流） | 🟢 | P2 完成；时间轮 Leader 独占 + 单调时钟（ADP §24）；`coord/src/main.rs:2695` |
| Maintenance.Status | 🟢（裁剪） | 仅承诺 `revision` 字段；其余字段/方法见下 |
| Health（grpc.health.v1） | 🟢 | `tonic_health::server::health_reporter()`，`coord/src/main.rs:2176,2692` |
| Auth（认证/RBAC） | 🟡 | 功能完成（14/14 测试）但凭证模型迁移中（`auth.proto:172` deprecated token）；v1.1 候选 |
| Maintenance 运维 RPC（Seal/Unseal/Snapshot/Compact/Member*/Join） | 🟡 | 线端存在（`maintenance.proto:103-114`），运维专用，不承诺 |
| Registry（服务注册） | 🟡 | Agent 侧服务，数据面落 Server KV（`coord-agent/src/services/registry.rs:88`）；但定义在内部包 `coord.agent`（`agent_api.proto:38`），与实验性能力同包，需包迁移后纳入（§12） |
| 静态加密 / Seal / Unseal | 🔴 | **不承诺**。R-SEC-01 已接线（`coord-server/src/server/mod.rs:1785-1798`、`coord/src/main.rs:2018`），但默认关闭（`security.encryption_enabled=false`，`main.rs:2045`），且未经生产验证。对外协议**不出现**任何加密/封存语义 |
| Multi-Raft / PD | 🔴 | **experimental，零生产路径引用**（ADP 实现进度表 P2-01 行：`pd/`、`raft/region.rs` 已标注 experimental）；生产形态 = 单 Raft 组 + 定期快照 |
| Cache / MQ（ISR 复制） | 🔴 | **experimental**（R-AGT-13，ADP §25.1）：ISR 提交非原子、分区 Leader 静态无故障转移、push 背压丢消息；生产语义 = 单 Agent at-least-once（poll+ack） |
| Replica 服务 | 🔴 | **仅内部 Agent 间通信**（`agent_api.proto:579`，仅由 coord-agent 挂载 `coord-agent/src/lib.rs:319`），永不对外 |
| Workflow（Saga） | 🔴 | 生产路径为内存态（ADP 口径修正，2026-08-23），不承诺任何持久化/补偿语义 |
| Config/Lock/IdGen/LeaderElection/Event/Scheduler/Policy | 🔴 | Agent 侧服务，未纳入对外承诺；其中 Lock/LeaderElection 如未来对外，将基于 KV/Txn/Lease 重新承诺语义而非直接暴露 Agent 接口 |

---

## 3. 兼容性铁律（N-1 承诺）

### 3.1 禁止项（任何版本、任何时候）

1. **禁止删除字段**（含 message、enum 值、rpc）。
2. **禁止修改字段编号**（field number 即线上协议，编号即契约）。
3. **禁止修改字段类型**（含 `repeated` ↔ 单值的切换）。
4. **禁止修改 rpc 的流式属性**（unary ↔ stream）。
5. **禁止重命名包 / 服务 / 方法 / 字段**——proto3 下重命名虽 wire 兼容，
   但会破坏生成代码与文档引用，视为 Breaking。
6. **禁止将 optional 语义收紧**（如新增服务端强制校验导致旧客户端请求被拒）。

### 3.2 允许项（Minor / Patch）

| 变更 | 版本 | 约束 |
|:---|:---:|:---|
| 新增 optional 字段 | Minor | 新字段必须对旧客户端透明（缺省值语义不变） |
| 新增 rpc 方法 | Minor | 不影响既有方法 |
| 新增 enum 值 | Minor | 仅追加；客户端必须容忍未知枚举值 |
| 新增 message | Minor | — |
| 注释/文档修订 | Patch | 不得改变语义承诺 |
| 服务端行为收紧（如新增上限） | Minor | 必须提前一个 Minor 版本公告（见 §3.4） |

### 3.3 废弃策略

- 废弃字段/rpc 必须先在 proto 中标记 `[deprecated = true]` 并在 CHANGELOG 公示
  **sunsetDate**；从标记到移除的存活期 **≥ 12 个月**（严于内部 ADP §20.3 的
  "一个 Minor 版本"——对外承诺以本白皮书为准）。
- 移除只发生在 Major 版本，且移除后必须 `reserved` 编号，永久防复用。

### 3.4 行为兼容

wire 兼容不等于行为兼容。以下服务端行为变化同样视为 Breaking，需 Major 流程：
错误码变更（§5 映射表即契约）、默认值变更、时序语义变更（如 Watch 投递顺序保证减弱）、
请求大小/数量上限的新增强制（可豁免：安全漏洞修复所需的紧急收紧，按 §10.4 紧急流程）。

### 3.5 包名与版本段

v1 承诺沿用线端既有包名（`coord.kv` 等，**不含版本段**）——这是与运行中服务端
wire 兼容的必要条件（gRPC 方法路径含包名）。未来 Major 升级引入新包
（如 `coord.kv.v2`），新旧包**双跑 ≥ 12 个月**。`buf.yaml` 中的 lint 例外
（`PACKAGE_VERSION_SUFFIX` 等）是为此刻意保留，禁止为通过 lint 而"修正"。

---

## 4. 版本规则

**协议版本与代码版本解耦**，独立演进：

```
contracts/v{MAJOR}.{MINOR}.{PATCH}   （git tag，如 contracts/v1.0.0）

PATCH：注释/文档修订，无线端变更
MINOR：新增 optional 字段 / rpc / enum 值（向后兼容）
MAJOR：破坏性变更（非必要不使用；须提前 ≥ 3 个月发布废弃警告，
       且新旧包双跑 ≥ 12 个月）
```

- 每次 MINOR 及以上发版，proto 产物推送至企业 Artifact 仓库，并更新《协议变更公示板》。
- 客户端依赖规则：锁定到 MINOR（如 `contracts/v1.x`），自动吸收 PATCH。
- 服务端兼容性承诺：服务端同时支持 **N 与 N-1** 两个 MINOR 版本生成的客户端。

---

## 5. 错误码契约

### 5.1 承诺的映射表（以代码为准）

以下映射来自 `coord-server/src/server/mod.rs:595-669`（`map_core_error`），
是**契约的一部分**。变更任一映射 = Breaking Change。

| 场景 | gRPC Status | 附加信息 |
|:---|:---|:---|
| 参数非法（含 Lease TTL 越界、事务过大、Shamir 分片不足） | `INVALID_ARGUMENT` | — |
| 资源不存在（Key/Lease/Region） | `NOT_FOUND` | — |
| 资源已存在（用户/角色） | `ALREADY_EXISTS` | — |
| **非 Leader（写请求被拒绝）** | **`UNAVAILABLE`** | metadata `coord-leader-hint: <leader grpc addr>`（可能缺失） |
| 集群不可用（无 Leader/选举中） | `UNAVAILABLE` | — |
| 集群 Sealed / Unsealing（运维态） | `UNAVAILABLE` | — |
| 请求超时 | `DEADLINE_EXCEEDED` | — |
| 指定 Revision 已被压缩清理 | `OUT_OF_RANGE` | 消息含最老可用 revision（仅运维参考，禁止程序解析） |
| Watch 连接数超限 / 背压 | `RESOURCE_EXHAUSTED` | — |
| 权限不足 | `PERMISSION_DENIED` | — |
| 未认证 / Token 过期或无效 / Auth 未启用 | `UNAUTHENTICATED` | — |
| Txn 条件不满足 | **非错误** | 正常返回 `OK`，由 `TxnResponse.succeeded=false` 表达 |
| 存储/加密/共识内部错误 | `INTERNAL` | 详情仅入服务端日志 |

> **文档漂移修正**：内部文档 ADP §23.2 称 NotLeader 映射为 `FailedPrecondition`，
> 与实际代码（`UNAVAILABLE`，`server/mod.rs:608`）不符。**以代码与本表为准**，
> ADP 侧修正已列入文档治理 backlog。

### 5.2 客户端重试纪律（承诺的语义基础）

| Status | 客户端动作 |
|:---|:---|
| `UNAVAILABLE`（含 leader-hint） | **可安全重试**：优先按 `coord-leader-hint` 重定向；无 hint 时退避后重试集群任意节点。Leader 切换窗口期出现本错误是**正常行为**，不得上报为业务故障 |
| `DEADLINE_EXCEEDED` / `RESOURCE_EXCEEDED` | 指数退避重试 |
| `INTERNAL` / `OUT_OF_RANGE` / 其余 | **不可盲目重试**：按业务语义处理（如 OUT_OF_RANGE 需全量重同步） |
| `UNAUTHENTICATED` | 重新认证后重试，禁止高频重试 |

### 5.3 客户端编程铁律：只依赖 Status Code

- **禁止解析错误消息文本**做任何分支判断。消息文本是给人看的，可随时调整
  （不视为 Breaking）。
- **已知债**：当前实现存在 28 处内部错误串直出（整改项 R5，
  `production-readiness-remediation-2026-08-27.md` P1-01）。这正是"只依赖 Status Code"
  纪律必须强制执行的原因——R5 整改完成后消息文本将进一步收敛。
- 响应消息中遗留的错误文本字段（如 `LeaseGrantResponse.error`）同样禁止解析。

---

## 6. Metadata 约定（承诺项）

| Key | 方向 | 语义 |
|:---|:---|:---|
| `authorization: Bearer <token>` | 请求 | 认证凭证（Auth 启用时必需；Auth 未启用时忽略）。同时作为幂等去重的客户端身份维度 |
| `coord-leader-hint: <host:port>` | 响应 | 非 Leader 拒绝时携带的 Leader 地址提示（`server/mod.rs:178`）。可能缺失，缺失时重试其他节点 |
| `grpc-timeout` | 请求 | 标准 gRPC 超时头；客户端**必须**为每次调用设置 deadline（建议写 5s / 读 3s / 流式不设 deadline 但设 keepalive） |

**承诺**：以上三个 key 的名称与语义稳定；服务端永不依赖其他自定义 metadata 返回关键控制信息。

---

## 7. 核心语义承诺与边界（防腐层）

### 7.1 Revision

- 全局 **单调递增的 64 位标识**，每次成功写入（Put/Delete/Txn）分配新值。
- 客户端只允许：相等性判断、大小比较、作为 `RangeRequest.revision` /
  `WatchCreateRequest.start_revision` 参数回传。
- **绝不承诺**其与 Raft Log Index / Term 的任何对齐关系；不承诺连续无空洞
  （空洞允许存在）。底层替换存储/共识引擎时本承诺不变。

### 7.2 一致性

- **写**：经 Leader 共识、多数派提交后返回——线性一致写。
- **读**：默认线性一致读（ReadIndex 确认后读取状态机）。
- **Leader 切换窗口**：写与线性一致读可能短暂返回 `UNAVAILABLE`，
  重试即可恢复；**不承诺**切换期间零错误。
- **不承诺**"跨分区强一致读"与"事务实时回滚"（红线 R1）：平台生产形态为
  单 Raft 组，无跨分区语义；已提交事务不可回滚，业务补偿由调用方实现。
- Watch 在 Follower 上可用，事件可能滞后于 Leader（ADP §5.4）。

### 7.3 Watch 重同步协议（客户端必须实现）

1. 正常事件：按 `revision` 去重（至少一次投递，可能重复）。
2. 收到 `BUFFER_OVERFLOW`：事件流出现空洞 → 以当前已确认 revision 做 `Range`
   全量拉取重建本地视图 → 继续 Watch。
3. 收到 `HISTORY_UNAVAILABLE`：请求的 `start_revision` 历史已清理 → `Range`
   全量同步 → 以最新 revision 重建 Watch。
4. 断线重连：`start_revision = 已确认最大 revision + 1` 重建流。

### 7.4 Lease

- TTL 以秒计；服务端可按配置上下限调整，以 `LeaseGrantResponse.ttl` 为准。
- 过期/Revoke → 绑定 Key 级联删除，删除事件正常投递 Watch。
- **不承诺精确过期时刻**：回收时刻可能略晚于 TTL（Leader 侧定时调度，
  Leader 切换后由新 Leader 重建定时器，ADP §24）。
- KeepAlive 断流后应在 TTL 内重建流续期；`ttl=0` 响应表示 Lease 已失效。

### 7.5 幂等

- `request_id`（Put/Delete/Txn 可选）：同一客户端身份（authorization 维度）下，
  重复提交相同 `request_id` 不重复生效，返回首次执行结果
  （`server/mod.rs:155-173`）。网络重试/超时重发场景建议始终携带。
- 不带 `request_id` 的写重试**不保证幂等**。

---

## 8. 红线声明（永不承诺，除非另立版本公告）

- **R1 时序依赖**：不承诺事务实时回滚、不承诺跨分区强一致读、不承诺
  Leader 切换期间零错误。
- **R2 安全细节**：对外协议不出现"加密存储"/"Seal"/"Unseal"语义，直到该能力
  默认开启、经生产验证并单独立项公告（当前默认关闭，`main.rs:2045`）。
- **R3 实验性能力隔离**：Cache/MQ 等实验性能力如对外开放，必须：
  1) 使用独立包名 `coord.experimental.<domain>.v1`；
  2) 随包附《降级逃生说明书》（含 ISR 非原子提交、静态 Leader 无故障转移、
     push 背压丢消息等风险描述与逃生路径）；
  3) 不受本白皮书兼容性铁律保护，可在任一 MINOR 版本变更或下线。
- **R4 内部通道**：`coord.raft.*`（Raft 节点间 RPC，独立端口 50052）、
  `coord.agent.Replica`（ISR 复制）永不对外；`raft_addr` 不应对业务网络开放。

---

## 9. 字段描述规范（Description 标准）

契约文件中的注释即承诺文本，书写规则：

1. 每个对外字段必须说明：语义、单位、零值含义、是否为服务端分配。
2. **不得**描述内部实现（如"该值等于 Raft 日志索引"）；只允许描述可观测语义。
3. 服务端分配字段必须注明"客户端不得自行构造"。
4. 不确定/保留的语义必须显式写"不承诺……"，留白即默认不承诺。

---

## 10. 变更流程与质量门禁

### 10.1 CI 门禁（`.github/workflows/contract-check.yml`，本仓库已落地）

| 卡口 | 工具 | 红线 |
|:---|:---|:---|
| 规范检查 | `buf lint` | 失败即阻断 |
| 破坏性检查 | `buf breaking --against` 基线（冻结后 = `contracts/v1.0.0` tag） | **检出 Breaking 即置红；分支保护设为 required check，合并按钮自动置灰** |
| wire 一致性 | `scripts/check-wire-sync.sh` | 契约承诺的 rpc/字段编号必须与 `coord-proto` 实现一致，防"空头承诺"与"静默漂移" |

### 10.2 协议变更 PR Checklist（强制）

- [ ] 变更类型（Patch/Minor/Major）已在 PR 标题标注；
- [ ] `buf breaking` 全绿（Major 除外，Major 走 §4 双跑流程）；
- [ ] 新增字段为 optional 语义，缺省值不改变既有行为；
- [ ] 字段注释符合 §9 规范，无内部实现泄漏；
- [ ] 错误码映射无变更，或已按 Breaking 流程处理；
- [ ] CHANGELOG 已更新；废弃字段含 `sunsetDate`（存活期 ≥ 12 个月）；
- [ ] 已填写《协议变更消费者告知模板》：**影响范围（消费者数量/名单）**、
      **回滚预案**、灰度计划。

### 10.3 合约测试（Consumer-Driven Contract）

- 维护一组模拟业务方的 Mock Client（Rust `coord-client` + Java `coord-java-sdk`
  各一），覆盖 KV/Txn/Lease/Watch 四域各 ≥1 正向用例 + 1 错误码用例
  （验证客户端只依赖 Status Code 行为正确）；
- 每次服务端发布前自动运行，响应结构必须严格匹配契约 proto。

### 10.4 紧急变更

安全漏洞所需的紧急收紧可豁免"提前一个 Minor 公告"，但必须在 48 小时内
补发公示并更新 CHANGELOG，且不得变更字段编号/类型（只允许服务端行为收紧）。

---

## 11. 已知限制（2026-08-27 基线）

以下不影响协议承诺的稳定性，但影响生产使用决策，必须如实告知消费者：

1. **生产就绪验收未全部通过**（整改文档 §6 九门）：独立 Jepsen、72h 浸泡、
   第三方安全审计、kind 部署验证证据缺失（P0-03/R10）。
2. 共识依赖 `openraft 0.10.0-alpha.25`（alpha 版本，P0-04；上游无稳定版，
   按风险接受处理）。
3. 28 处内部错误串直出客户端（P1-01/R5 未整改）——§5.3 纪律必须执行。
4. 监控/部署物/runbook 存在缺口（P1-02/03/04）。
5. gRPC reflection 由 `security.reflection_enabled` 控制（`main.rs:2549`），
   生产默认关闭时 grpcurl 类工具不可用——消费方应以本契约文件生成客户端，
   不依赖服务端反射。

---

## 12. Roadmap（候选，非承诺）

| 版本 | 候选内容 | 纳入前置条件 |
|:---:|:---|:---|
| v1.1 | Auth 客户端面（`Authenticate`/`RefreshToken`/`AuthStatus`） | CCT v3 迁移完成、deprecated `token` 字段下线路径明确 |
| v1.1 | Registry（服务注册/发现） | 包迁移至 `coord.registry.v1`（脱离 `coord.agent` 内部包）、Agent 双跑新旧服务路径 |
| v1.2 | Maintenance 运维面扩展（`Snapshot`/`Compact` 只读化） | 运维接口鉴权与审计硬化 |
| v2.0 | （如需）任何 Breaking 变更 | 提前 ≥3 个月废弃警告 + 新包双跑 ≥12 个月 |

---

*本白皮书随 `apis/contracts/` 一同版本化。修订本文件 = 修订承诺本身，一律走 §10 变更流程。*
