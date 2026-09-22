# Coord 平台对外协议白皮书（v1.2.0）

> 版本：contracts/v1.2.0 ｜ 修订日期：2026-09-20 ｜ 基线代码：`fdbeef3`（main）
>
> **本版变更（v1.1.0 → v1.2.0，Minor）**：GA 范围由「5 项服务协调能力」扩为
> **coord-agent 全部 17 项能力 + `coord.storage`**（`STATUS.md` COMMITTED 段）；
> `EXPERIMENTAL` 区**清空**（原 4 个 `coord.experimental.*` 包从未有 proto、
> 从未有消费者，直接建为稳定包，**不以实验包形态开放任何能力**）。
> 逐包对照与消费者告知见 `CHANGELOG.md` 的 `contracts/v1.2.0` 条目（§11.2 流程）。
>
> **契约地位**：本白皮书是 Coord 平台对上游业务方（100+ 微服务，Java/Go）的
> **服务协调能力承诺**：注册发现、分布式锁、Leader 选举、分布式 ID、事件通知，
> 以及本版新增的配置中心、权限策略、熔断、限流、信封加密、PKI、缓存、消息队列、
> 特性开关、工作流（Saga）、调度、对象存储。
> 契约以承诺**倒逼 coord 项目将这些能力做实**——承诺即交付义务，GA 期限逾期即
> CI 红牌（§13 倒逼机制）。
>
> **一个能力的 GA（G1–G6）**＝ ① 契约文件存在且进 wire 卡口；② 实现已挂在契约包名下；
> ③ descriptor 级 wire 相等；④ 语义边界（不承诺什么）已写入契约注释；⑤ 有可复现验收；
> ⑥ 默认开关口径已裁定。**边界声明与能力声明同等重要**（§10 规则 4）。
>
> **原语定位**：KV/Txn/Lease/Watch 是平台实现底座，保留稳定承诺供 SDK 与数据面对齐，
> **不构成对业务消费者的编排建议**——业务方不需要也不应该用原语自行拼装协调逻辑。
>
> **重要声明**：本白皮书是协议承诺，不是生产就绪声明。生产就绪的验收判据以
> [`docs/production/production-readiness-plan-2026-09-21.md`](../production/production-readiness-plan-2026-09-21.md)
> §4 的 9 道生产门（P-Gate 1–9）为准（当前未全部通过，见 §12 已知限制）。
> 该文件取代了此前引用的 `docs/production-readiness-remediation-2026-08-27.md`
> （该文件在磁盘与 git 历史中均不存在）。

---

## 1. 承诺哲学与分层

### 1.1 契约承诺什么

本契约向业务消费者承诺**服务协调能力**：服务注册发现、分布式锁、Leader 选举、
分布式 ID、事件通知，以及配置、策略、熔断、限流、加密、PKI、缓存、消息、
特性开关、工作流、调度、对象存储（语义契约见 §3 与各 proto 注释）。
KV/Txn/Lease/Watch 是平台实现底座，
保留稳定承诺（§1.4）供 SDK 与数据面对齐，**不构成对业务方的编排建议**——
业务方不需要也不应该用原语自行拼装协调逻辑。

**承诺的两半必须同样明确**：每一项能力不仅声明「承诺什么」，也声明
「**不承诺什么**」（§10 规则 4）。只写前者会让消费者把未声明的留白读成承诺。

承诺 = 交付义务：COMMITTED 服务带 GA 硬截止（§13），期限逾期即 CI 红牌。
契约先行冻结，实现必须追上——这就是倒逼机制。

### 1.2 三态分层

| 状态 | 含义 | 门禁 |
|:---|:---|:---|
| `STABLE` | 稳定承诺，wire 冻结，受兼容性铁律保护 | buf breaking + wire-sync 硬卡口 |
| `COMMITTED` | 承诺中：proto 已冻结进契约，GA 期限硬截止 | wire-sync 期限卡口（逾期未迁移 → CI 红） |
| `EXPERIMENTAL` | 整改承诺：缺陷清单 + 整改期限，独立实验包 | 期限公示治理（§9），不得以稳定面消费 |

单一事实来源：`apis/contracts/STATUS.md`（承诺台账，脚本解析）。

> **当前 EXPERIMENTAL 区为空**（自 `contracts/v1.2.0` 起）。§9.1 保留历史整改台账，
> §9.2 的规则仍然有效，适用于**将来**要以实验包形态开放的新能力。

### 1.3 能力承诺面（v1.2，COMMITTED，共 17 项 + 对象存储）

| 能力 | 契约包 | RPC | GA 期限 |
|:---|:---|:---|:---:|
| 服务注册发现 | `coord.registry.v1` | `Register` / `Deregister` / `Heartbeat` / `Discover` / `Watch` | 2026-10-31 |
| 分布式锁 | `coord.lock.v1` | `Acquire` / `Release` / `Renew` / `GetLockInfo` | 2026-11-30 |
| Leader 选举 | `coord.election.v1` | `Campaign` / `Resign` / `GetLeader` / `Watch` | 2026-11-30 |
| 分布式 ID | `coord.idgen.v1` | `NextId` / `NextBatch` | 2026-10-31 |
| 事件通知 | `coord.event.v1` | `Publish` / `Subscribe` / `Unsubscribe` | 2026-12-31 |
| 配置中心 | `coord.config.v1` | `Get` / `Put` / `List` / `Watch` | 2026-12-31 |
| 权限策略引擎 | `coord.policy.v1` | `CheckPermission` / `Evaluate` / `Explain` / `PutBundle` / `DeleteBundle` / `ListBundles` / `SetBundleEnabled` / `RollbackBundle` / `ListBundleVersions` | 2026-12-31 |
| 熔断器 | `coord.circuitbreaker.v1` | `GetState` / `ReportSuccess` / `ReportFailure` / `Reset` | 2026-12-31 |
| 限流器 | `coord.ratelimiter.v1` | `Allow` | 2026-12-31 |
| 安全传输（信封加密） | `coord.transit.v1` | `Encrypt` / `Decrypt` / `HmacSign` / `HmacVerify` | 2026-12-31 |
| PKI 证书签发 | `coord.pki.v1` | `InitCa` / `IssueCert` / `RenewCert` / `VerifyCert` / `GetCaCert` / `RotateCert` / `ListCerts` / `GetCertByCN` | 2026-12-31 |
| 缓存 | `coord.cache.v1` | `Get` / `Set` / `Delete` / `HGet` / `HSet` / `HGetAll` / `LPush` / `LRange` / `RPop` / `LLen` / `SAdd` / `SMembers` | 2026-12-31 |
| 消息队列 | `coord.mq.v1` | `CreateTopic` / `Publish` / `Subscribe` / `Poll` / `Ack` / `PollDlq` | 2026-12-31 |
| 特性开关 | `coord.featureflags.v1` | `IsEnabled` / `Evaluate` | 2026-12-31 |
| 工作流（Saga） | `coord.workflow.v1` | `Start` / `GetStatus` / `Signal` / `Cancel` / `Deploy` / `ListDefinitions` / `GetDefinition` / `ListDefinitionVersions` / `RollbackDefinition` / `ListInstances` | 2027-03-31 |
| 调度 | `coord.scheduler.v1` | `RegisterJob` / `ClaimJob` / `Heartbeat` / `CompleteJob` | 2027-03-31 |
| 对象存储（Server 侧，agent 代理） | `coord.storage` | 见 `storage.proto`（**包名无 `.v1` 后缀，本版不迁**） | 2026-12-31 |

语义契约全文 = 各 proto 文件注释（§10 规范）。
wire 与迁移前的 `coord.agent.*` 实现逐字段一致（含 service 名与方法名），
迁移为机械式重挂载，禁止趁机改 wire；两侧一致性由**第 4 道卡口**
（`check-wire-descriptor.sh`，descriptor 级逐字段比对）守住。

**边界声明（本版新增，§10 规则 4 的正式落地）**：`policy` 的 RBAC 策略为
**Agent 本地**（OPA bundle 在 server KV），`circuitbreaker` / `ratelimiter` 为
**Agent 本地内存，不跨 Agent 共享**，`cache` 的跨节点复制为**best-effort 且
分区 Leader 无自动故障转移**，`mq` 的 `Subscribe` 为 **best-effort 推送**
（at-least-once 的唯一承诺路径是 `Poll` + `Ack`）。以上均写在对应 proto 的服务级
注释中，并以原型**不承诺清单**的形式复述（见 `STATUS.md` 与各 proto）。

### 1.4 底座原语（STABLE，实现底座）

| 包 | 服务 | 定位 |
|:---|:---|:---|
| `coord.kv` / `coord.txn` / `coord.lease` / `coord.watch` | KV / Txn / Lease / Watch | 协调能力的实现底座、SDK 与数据面对齐面；业务方不应直接以其拼装协调逻辑 |
| `coord.maintenance` | `Status`（裁剪） | 探活 |
| `grpc.health.v1` | Health | 标准健康检查 |

判定依据：KV/Txn/Watch/Lease 为 P0/P1 完成域，全量测试 1656 passed / 0 failed
（2026-08-27 实测，见整改文档 §9.1.1），多节点 Raft 集群测试
（Leader 故障切换 / 网络分区 / 成员变更）全绿。

---

## 2. 能力现状与证据（源码考古 2026-08-27；迁移后状态回填 2026-09-20）

每项结论附代码证据（文件:行号 @ `82eb398`）。“距 GA 的缺口”一栏保留**当时的**
判定以便追溯；已在 `contracts/v1.2.0`（2026-09-19）闭合的条目以 ~~删除线~~ + 现状注明。
GA 验收以 `docs/production/readiness` 与 `WHITEPAPER.md` §13 的 G1–G6 为准。

| 能力 | 现状 | 证据 | 距 GA 的缺口 |
|:---|:---|:---|:---|
| Registry | 实现存在：KV+Lease 绑定 + 本地全量缓存 + Watch fan-out | `coord-agent/src/services/registry.rs:88` | ~~迁移至 `coord.registry.v1` 包~~ ✅ 已迁移（wire 逐字段未变）；错误码对齐、重复注册幂等验证已入库 |
| Lock | 实现存在：Lease + Txn(IfNotExists) 互斥 | `coord-agent/src/services/lock.rs` | ~~包迁移~~ ✅；~~非持有者释放 `PERMISSION_DENIED` 对齐~~ ✅（`release` 现校验 `(holder_id, lease_id)`，不匹配即 `PERMISSION_DENIED`） |
| LeaderElection | 实现存在 | `coord-agent/src/services/leader_election.rs` | ~~包迁移~~ ✅；续约（重新 Campaign）语义验证已入库 |
| IdGen | 雪花 1+41+10+12，离线号段可用 | `coord-agent/src/services/idgen.rs` | ~~包迁移~~ ✅；时钟回拨防护**未做** ⇒ 已改为显式不承诺边界（§3.4） |
| Event | 简化 CloudEvents 形态（完整封装为蓝图） | `coord-agent/src/services/event_notification.rs` | ~~包迁移~~ ✅；~~`subscription_id` 由 Subscribe 显式下发~~ ⚠️ **仍未做** ⇒ 已按 §10 规则 6 在 `event.proto` 声明（`Unsubscribe` 不携带状态、id 为本地派生）；断线窗口不补投已声明 |
| KV / Txn / Watch / Lease | STABLE（P0/P1 完成域） | `coord/src/main.rs:2693-2696` | — |
| Status / Health | STABLE | `coord/src/main.rs:2176,2692` | — |
| Cache / MQ | 已整改：Cache 跨节点提交语义 + 静态 Leader 判定为**设计边界并写入契约**；MQ `Poll`+`Ack` 为 at-least-once 承诺路径、`Subscribe` 显式声明 best-effort、`Publish` 幂等键落地 | `cache.proto` / `mq.proto` 边界声明；`services/mq.rs` 的 `IDEMPOTENCY_TABLE` | 见 §9.1 台账 |
| Workflow / Scheduler | 已整改：Workflow 走 `KvWorkflowStore` 持久化 + 补偿语义端到端验收；Scheduler 三张内存 HashMap 改 `KvSchedulerStore`（跨节点 CAS 唯一认领） | `services/workflow_store.rs`；`services/scheduler_store.rs` | 见 §9.1 台账 |
| Multi-Raft/PD、静态加密 | 不对外承诺（红线 §9.3） | Multi-Raft/PD：内部实现已接线生产路径（region 目录级隔离 + 内嵌 PD，Phase 2–3 T2.1–T3.4 已合入 main，默认关闭 opt-in）；红线 §9.3 维持 | — |

---

## 3. 能力承诺面语义契约（v1.2）

全文以各 proto 注释为准（`apis/contracts/proto/coord/<domain>/v1/`）；本节为判定摘要。
错误码遵循 §6 契约。

### 3.1 Registry（`coord.registry.v1`）

- 注册绑定租约：TTL 内无 `Heartbeat` → 实例自动过期并广播移除事件（至少一次投递）。
- 同 `(service_name, instance_id)` 重复 `Register` = 覆盖续约（幂等），返回新 `lease_id`。
- `Discover` = 线性一致读；`revision` 全局单调递增，客户端可作版本比较。
- `Watch` 增量有序投递；断线重连 `start_revision = 已确认 revision + 1`；
  历史被清理 → `OUT_OF_RANGE` → 全量 `Discover` 重建。
- `metadata` 为不透明字节（建议 JSON，承载 address 等），服务端不解析。
- 错误码：TTL 越界 → `INVALID_ARGUMENT`；注销幂等（实例不存在也返回 OK）。

### 3.2 Lock（`coord.lock.v1`）

- 线性一致互斥：同一时刻同一锁名至多一个持有者；`Acquire` 失败 →
  `acquired=false`（正常返回，非错误）。
- TTL 内未 `Renew` → 自动释放；`Renew` 返回新 TTL，租约已失效 → `new_ttl=0`。
- 仅持有者可 `Release`（`holder_id` + `lease_id` 校验）；非持有者 → `PERMISSION_DENIED`。
- `GetLockInfo` 线性一致读。不承诺公平性与可重入。

### 3.3 LeaderElection（`coord.election.v1`）

- `Campaign` 原子竞选：无 leader → 获胜者 `elected=true`；有 leader 且非本人 →
  `elected=false`（正常返回）。
- 任期续约 = 定期重新 `Campaign`（同 `candidate_id`）；TTL 内未续约 → 广播 `LEADER_EXPIRED`。
- `Resign` 仅当前 leader 本人有效，否则 `resigned=false`。
- `Watch`：`LEADER_ELECTED` / `LEADER_RESIGNED` / `LEADER_EXPIRED` 至少一次投递；
  重连后先 `GetLeader` 重建状态再续订。

### 3.4 IdGen（`coord.idgen.v1`）

- 同名发号器内 ID 全局唯一、趋势递增；不承诺连续（并发跳号允许）。
- 默认雪花（1 符号 + 41 毫秒 + 10 worker + 12 序列）；号段模式经 `step` opt-in。
- 与 Server 断连时，号段缓存内可继续发号（不承诺无限离线）。
- 时钟回拨：正常单调时钟假设下唯一；跨回拨唯一性防护落地前不承诺（STATUS 整改项）。

### 3.5 Event（`coord.event.v1`）

- 连接存续期间 at-least-once、单订阅内按发布顺序投递；断线窗口不承诺补投
  （持久化游标 = v1.2 候选）。
- `event_id` 服务端分配、全局唯一；`specversion` 固定 `"1.0"`。
- 订阅匹配：`event_type` 精确匹配（前缀匹配 v1.2 候选）。
- `subscription_id` 当前由 `event_type` 派生；迁移时须由 `Subscribe` 显式下发（STATUS 整改项）。

### 3.6 本版新增能力族（v1.2，`coord.<domain>.v1`）

本节不重复各 RPC 的逐字段语义（以 proto 注释为准），只归纳**共有的判定口径**
与**必须知道的边界**。边界全文见 §9.1.1。

| 族 | 包 | 必须知道的边界 |
|:---|:---|:---|
| 配置 | `coord.config.v1` | `Watch` 为增量变更通知；断线重连须先 `List` 重建全量（与 §8.3 同纪律） |
| 策略 | `coord.policy.v1` | **RBAC 求值在 Agent 本地**；bundle 存 server KV，`RollbackBundle` 按版本回滚 |
| 韧度 | `coord.circuitbreaker.v1` / `coord.ratelimiter.v1` | **均为 Agent 本地内存状态，不跨 Agent 共享**（N 个 Agent 的聚合放行率可达单 Agent 限额的 N 倍） |
| 加密 | `coord.transit.v1` | 信封加密；DEK 持久化于 server KV 且有 TTL，**单次使用**（解密即焚）；KEK 供给未托管，见 §12.7 |
| 证书 | `coord.pki.v1` | `InitCa` 幂等（已初始化则返回现有 CA）；证书签发/轮换均走 server KV + Barrier 加密存储 |
| 缓存 | `coord.cache.v1` | 跨节点复制 **best-effort**；分区 Leader **不承诺自动故障转移**；**不承诺容量上限**；`min_isr` 失败时**本地写入已生效**（错误不可读作"未写入"） |
| 消息 | `coord.mq.v1` | `Subscribe` **best-effort 推送**；at-least-once 仅由 `Poll` + `Ack` 承诺；`Publish` 的 `idempotency_key` 去重（同键不追加、重启后仍去重）；`PollDlq` 读死信 |
| 开关 | `coord.featureflags.v1` | 只读 RPC；开关变更**带外发生**，故服务端不做本地缓存（避免静默陈旧）。无 Watch 面 |
| 工作流 | `coord.workflow.v1` | 定义版本化（`ListDefinitionVersions` / `RollbackDefinition`）；实例持久化；`compensatedBy` / `onErrors` 的补偿动作**真的会发出**（§9.1 台账） |
| 调度 | `coord.scheduler.v1` | 认领为**跨节点原子 CAS**，多节点下同一任务不重复触发；`Heartbeat` / `CompleteJob` 以**认领句柄**（`job_id`）为凭据 |
| 对象存储 | `coord.storage` | 数据面在 **server**，agent 仅代理；**包名无 `.v1` 后缀（本版不迁，改名即 Breaking）**；已知边界见 `STATUS.md` 该行 |

> 未列入本表的语义（错误码、零值、字段分配）一律以 proto 注释为唯一事实来源；
> 本节与 proto 冲突时以 proto 为准。

---

## 4. 兼容性铁律（N-1 承诺）

### 4.1 禁止项（任何版本、任何时候）

1. **禁止删除字段**（含 message、enum 值、rpc）。
2. **禁止修改字段编号**（field number 即线上协议，编号即契约）。
3. **禁止修改字段类型**（含 `repeated` ↔ 单值的切换）。
4. **禁止修改 rpc 的流式属性**（unary ↔ stream）。
5. **禁止重命名包 / 服务 / 方法 / 字段**——proto3 下重命名虽 wire 兼容，
   但会破坏生成代码与文档引用，视为 Breaking。
6. **禁止将 optional 语义收紧**（如新增服务端强制校验导致旧客户端请求被拒）。

### 4.2 允许项（Minor / Patch）

| 变更 | 版本 | 约束 |
|:---|:---:|:---|
| 新增 optional 字段 | Minor | 新字段必须对旧客户端透明（缺省值语义不变） |
| 新增 rpc 方法 | Minor | 不影响既有方法 |
| 新增 enum 值 | Minor | 仅追加；客户端必须容忍未知枚举值 |
| 新增 message | Minor | — |
| 注释/文档修订 | Patch | 不得改变语义承诺 |
| 服务端行为收紧（如新增上限） | Minor | 必须提前一个 Minor 版本公告（见 §4.4） |

### 4.3 废弃策略

- 废弃字段/rpc 必须先在 proto 中标记 `[deprecated = true]` 并在 CHANGELOG 公示
  **sunsetDate**；从标记到移除的存活期 **≥ 12 个月**（严于内部 ADP §20.3 的
  "一个 Minor 版本"——对外承诺以本白皮书为准）。
- 移除只发生在 Major 版本，且移除后必须 `reserved` 编号，永久防复用。

### 4.4 行为兼容

wire 兼容不等于行为兼容。以下服务端行为变化同样视为 Breaking，需 Major 流程：
错误码变更（§6 映射表即契约）、默认值变更、时序语义变更（如 Watch 投递顺序保证减弱）、
请求大小/数量上限的新增强制（可豁免：安全漏洞修复所需的紧急收紧，按 §11.4 紧急流程）。

### 4.5 包名与版本段

- 底座原语沿用线端既有包名（`coord.kv` 等，**不含版本段**）——与运行中服务端
  wire 兼容的必要条件（gRPC 方法路径含包名）；未来 Major 升级引入新包
  （如 `coord.kv.v2`），新旧包**双跑 ≥ 12 个月**。
- 能力承诺面（v1.1 起）一律使用带版本段包名 `coord.<domain>.v1`——这是
  能力承诺独立演进的基础，Major 升级按同规则引入 `.v2` 双跑。
- `buf.yaml` 中的 lint 例外（`PACKAGE_VERSION_SUFFIX` 等）仅为底座原语
  线端兼容而保留，禁止为通过 lint 而"修正"。

---

## 5. 版本规则

**协议版本与代码版本解耦**，独立演进：

```
contracts/v{MAJOR}.{MINOR}.{PATCH}   （git tag，如 contracts/v1.1.0）

PATCH：注释/文档修订，无线端变更
MINOR：新增 optional 字段 / rpc / enum 值（向后兼容）
MAJOR：破坏性变更（非必要不使用；须提前 ≥ 3 个月发布废弃警告，
       且新旧包双跑 ≥ 12 个月）
```

- 每次 MINOR 及以上发版，proto 产物推送至企业 Artifact 仓库，并更新《协议变更公示板》。
- 客户端依赖规则：锁定到 MINOR（如 `contracts/v1.x`），自动吸收 PATCH。
- 服务端兼容性承诺：服务端同时支持 **N 与 N-1** 两个 MINOR 版本生成的客户端。

---

## 6. 错误码契约

### 6.1 承诺的映射表（以代码为准）

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

### 6.2 客户端重试纪律（承诺的语义基础）

| Status | 客户端动作 |
|:---|:---|
| `UNAVAILABLE`（含 leader-hint） | **可安全重试**：优先按 `coord-leader-hint` 重定向；无 hint 时退避后重试集群任意节点。Leader 切换窗口期出现本错误是**正常行为**，不得上报为业务故障 |
| `DEADLINE_EXCEEDED` / `RESOURCE_EXCEEDED` | 指数退避重试 |
| `INTERNAL` / `OUT_OF_RANGE` / 其余 | **不可盲目重试**：按业务语义处理（如 OUT_OF_RANGE 需全量重同步） |
| `UNAUTHENTICATED` | 重新认证后重试，禁止高频重试 |

### 6.3 客户端编程铁律：只依赖 Status Code

- **禁止解析错误消息文本**做任何分支判断。消息文本是给人看的，可随时调整
  （不视为 Breaking）。
- **已知债**：当前实现存在 28 处内部错误串直出（整改项 R5；在
  [`docs/production/production-readiness-plan-2026-09-21.md`](../production/production-readiness-plan-2026-09-21.md)
  §4 的 P-Gate P1 下跟踪）。这正是"只依赖 Status Code"
  纪律必须强制执行的原因——R5 整改完成后消息文本将进一步收敛。
- 响应消息中遗留的错误文本字段（如 `LeaseGrantResponse.error`）同样禁止解析。

---

## 7. Metadata 约定（承诺项）

| Key | 方向 | 语义 |
|:---|:---|:---|
| `authorization: Bearer <token>` | 请求 | 认证凭证（Auth 启用时必需；Auth 未启用时忽略）。同时作为幂等去重的客户端身份维度 |
| `coord-leader-hint: <host:port>` | 响应 | 非 Leader 拒绝时携带的 Leader 地址提示（`server/mod.rs:178`）。可能缺失，缺失时重试其他节点 |
| `grpc-timeout` | 请求 | 标准 gRPC 超时头；客户端**必须**为每次调用设置 deadline（建议写 5s / 读 3s / 流式不设 deadline 但设 keepalive） |

**承诺**：以上三个 key 的名称与语义稳定；服务端永不依赖其他自定义 metadata 返回关键控制信息。

---

## 8. 核心语义承诺与边界（底座原语）

### 8.1 Revision

- 全局 **单调递增的 64 位标识**，每次成功写入（Put/Delete/Txn）分配新值。
- 客户端只允许：相等性判断、大小比较、作为 `RangeRequest.revision` /
  `WatchCreateRequest.start_revision` 参数回传。
- **绝不承诺**其与 Raft Log Index / Term 的任何对齐关系；不承诺连续无空洞
  （空洞允许存在）。底层替换存储/共识引擎时本承诺不变。

### 8.2 一致性

- **写**：经 Leader 共识、多数派提交后返回——线性一致写。
- **读**：默认线性一致读（ReadIndex 确认后读取状态机）。
- **Leader 切换窗口**：写与线性一致读可能短暂返回 `UNAVAILABLE`，
  重试即可恢复；**不承诺**切换期间零错误。
- **不承诺**"跨分区强一致读"与"事务实时回滚"（红线 R1，见 §9）：平台生产形态为
  单 Raft 组，无跨分区语义；已提交事务不可回滚，业务补偿由调用方实现。
- Watch 在 Follower 上可用，事件可能滞后于 Leader（ADP §5.4）。

### 8.3 Watch 重同步协议（客户端必须实现）

1. 正常事件：按 `revision` 去重（至少一次投递，可能重复）。
2. 收到 `BUFFER_OVERFLOW`：事件流出现空洞 → 以当前已确认 revision 做 `Range`
   全量拉取重建本地视图 → 继续 Watch。
3. 收到 `HISTORY_UNAVAILABLE`：请求的 `start_revision` 历史已清理 → `Range`
   全量同步 → 以最新 revision 重建 Watch。
4. 断线重连：`start_revision = 已确认最大 revision + 1` 重建流。

### 8.4 Lease

- TTL 以秒计；服务端可按配置上下限调整，以 `LeaseGrantResponse.ttl` 为准。
- 过期/Revoke → 绑定 Key 级联删除，删除事件正常投递 Watch。
- **不承诺精确过期时刻**：回收时刻可能略晚于 TTL（Leader 侧定时调度，
  Leader 切换后由新 Leader 重建定时器，ADP §24）。
- KeepAlive 断流后应在 TTL 内重建流续期；`ttl=0` 响应表示 Lease 已失效。

### 8.5 幂等

- `request_id`（Put/Delete/Txn 可选）：同一客户端身份（authorization 维度）下，
  重复提交相同 `request_id` 不重复生效，返回首次执行结果
  （`server/mod.rs:155-173`）。网络重试/超时重发场景建议始终携带。
- 不带 `request_id` 的写重试**不保证幂等**。

---

## 9. 承诺修复区与实验包规则

### 9.1 承诺修复区（EXPERIMENTAL：缺陷清单 + 整改承诺 + 期限）

> **本区自 `contracts/v1.2.0`（2026-09-19）起为空。**
> 下表为**历史台账**，保留以便追溯「什么缺陷、以什么形态闭合、判据在哪」。
> 台账不删行的理由：删掉就等于把「曾经承诺过整改」这件事从文本里抹去。
> 逐条的闭合形态见下表末列。

| 能力 | 当时缺陷（源码考古） | 当时的整改承诺 | 期限 | 闭合形态（2026-09-19） |
|:---|:---|:---|:---:|:---|
| Cache | ISR 提交非原子、分区 Leader 静态无故障转移 | ISR 原子提交 + 分区 Leader 故障转移，验收后申请晋升 | 2026-12-31 | **改判为设计边界并写入契约**：跨节点复制为 best-effort（`min_isr` 不满足时 RPC 返错，但**本地写入已生效** ⇒ 错误不可读作"未写入"）；分区 Leader 为确定性静态判定（显式覆盖优先，否则 ISR 成员中地址最小者），**不产生脑裂**，代价是 fail-closed。原文"Follower **永久**落后"经逐行核验**过强**，实为"暂时不一致 + 心跳 Reconcile 可补齐" |
| MQ | push 背压丢消息（ISR 同上） | 生产语义收敛为单 Agent at-least-once（poll+ack），背压不丢消息并文档化 | 2026-12-31 | **承诺面收缩 + 显式声明**：`Subscribe` 服务级注释声明为 **best-effort 推送**（channel 打满即丢弃、不报错不重投）、**不是 at-least-once 通道**；at-least-once 的唯一承诺路径 = `Poll` + `Ack`。`Publish` 的 `idempotency_key` 已真正生效（同键不追加、重启后仍去重） |
| Workflow（Saga） | 生产路径为内存态，无持久化/补偿 | 持久化 + 补偿语义落地后方允许对外 | 2027-03-31 | **两项均落地**：`KvWorkflowStore`（持久化）+ 补偿端到端验收（真 dispatcher + 真失败 + 记录型 HTTP 服务端证明动作**真的发出**）。验收过程另查出两个 P0：try 块丢弃 `Suspend`（静默丢副作用）、`functionRef` 从未解析到 `functions[].operation`（任何 CNCF `operation` 状态必然失败），均已修 |
| Scheduler | 头部声称 KV CAS + Lease，实现为内存 HashMap | 改为 KV 真实现，多节点唯一调度 | 2027-03-31 | **已改**：三张 HashMap 合并为 `TaskRecord` 一条记录，生产走 `KvSchedulerStore`（`coord.txn` value-CAS 跨节点原子认领）；模块头自述与实现现已一致。另修两处使 gRPC 面实质不可用的缺陷（`task_id` 注册/认领键不一致、Heartbeat/Complete 硬编码 `"worker"`） |

整改期限逾期 = 台账（STATUS.md）与本文档同步红牌公示。
验收口径：整改文档 §6 验收门对应子集。

#### 9.1.1 能力边界声明（v1.2.0 正式确认，§10 规则 4 的落地）

「不承诺什么」与「承诺什么」同等重要，且**必须写在契约里**（而非只写在本文档）。
本版对以下五项作出**正式确认**，作为 `policy` / `circuitbreaker` / `ratelimiter`
（原 B-13 / B-14）等项的闭合形态：

| # | 能力 | 正式声明的边界 | 落点 |
|:---|:---|:---|:---|
| 1 | `coord.policy.v1` | **RBAC 策略为 Agent 本地**：OPA bundle 存于 server KV，但策略求值在本 Agent 进程内完成，**不承诺跨 Agent 的策略一致性窗口** | `policy.proto` 服务级注释 |
| 2 | `coord.circuitbreaker.v1` | **本地内存，不跨 Agent 共享**：熔断状态是单 Agent 观测的结果，多 Agent 下同一依赖可呈现不同状态 | `circuitbreaker.proto` 服务级注释 |
| 3 | `coord.ratelimiter.v1` | **本地令牌桶，不跨 Agent 共享**：N 个 Agent 的聚合放行率可达单 Agent 限额的 N 倍 | `ratelimiter.proto` 服务级注释 |
| 4 | `coord.cache.v1` | 跨节点复制 best-effort；分区 Leader **不承诺自动故障转移**；**不承诺容量上限**（`max_size` 不执行，仅可观测） | `cache.proto` 服务级 + 文件级注释 |
| 5 | `coord.mq.v1` | `Subscribe` **不承诺不丢**（best-effort）；仅 `Poll` + `Ack` 承诺 at-least-once；`Publish` 只承诺"落本地日志" | `mq.proto` 服务级注释 |

> **为什么这条要单列**：这五项在 v1.1 白皮书里要么被写成"缺陷待整改"，要么
> 完全没写。**留白会被消费者读成承诺**（§10 规则 4）。把它们从"缺陷"改为
> "显式边界"不是降低标准，而是**让契约与实现说同一句话** —— 且这句话是可核验的
> （边界文本就在 proto 里，`check-wire-descriptor.sh` 逐包比对时会一并读到）。

### 9.2 实验包规则（EXPERIMENTAL 对外开放的前置）

> **当前无实验包**（`STATUS.md` 的 EXPERIMENTAL 区为空）。以下规则对**将来**
> 要以实验包形态开放的新能力仍然有效。

1) 独立包名 `coord.experimental.<domain>.v1`；
2) 随包附《降级逃生说明书》（含 ISR 非原子提交、静态 Leader 无故障转移、
   push 背压丢消息等风险描述与逃生路径）；
3) 不受兼容性铁律保护，可在任一 MINOR 版本变更或下线；
4) §9.1 整改承诺期限未兑现，不得以任何形态对外开放。

### 9.3 红线（永不承诺，除非另立版本公告）

- **R1 时序依赖**：不承诺事务实时回滚、跨分区强一致读、Leader 切换期间零错误。
- **R2 安全细节**：对外协议不出现加密存储 / Seal / Unseal 语义，直到该能力
  默认开启、经生产验证并单独立项公告（当前默认关闭，`main.rs:2045`）。
- **R3 内部通道**：`coord.raft.*`（Raft 节点间 RPC，独立端口 50052）、
  `coord.agent.Replica`（ISR 复制）永不对外；`raft_addr` 不应对业务网络开放。

---

## 10. 字段描述规范（Description 标准）

契约文件中的注释即承诺文本，书写规则：

1. 每个对外字段必须说明：语义、单位、零值含义、是否为服务端分配。
2. **不得**描述内部实现（如"该值等于 Raft 日志索引"）；只允许描述可观测语义。
3. 服务端分配字段必须注明"客户端不得自行构造"。
4. 不确定/保留的语义必须显式写"不承诺……"，留白即默认不承诺。
5. **局部性/单机性能力必须显式声明作用域**（v1.2 新增）：凡状态落在单个 Agent
   进程内、不跨 Agent 共享的能力（熔断、限流、RBAC 策略求值等），必须在服务级
   注释里写明"**为 Agent 本地**"，并说明多 Agent 下的可观测后果
   （例：N 个 Agent 的聚合放行率可达单 Agent 限额的 N 倍）。
   适用情形的逐项清单与正式确认见 §9.1.1。
6. 被移出承诺面的能力**不得删除**，必须以"显式声明为 best-effort / 不承诺"的
   形式留在契约里（例：`cache` 的容量上限、`mq` 的 `Subscribe`）。
   删除会让已有消费者静默失去一个（曾经默认存在的）保证；声明则会让他们看见边界。

---

## 11. 变更流程与质量门禁

### 11.1 CI 门禁（`.github/workflows/contract-check.yml`，本仓库已落地）

| 卡口 | 工具 | 红线 |
|:---|:---|:---|
| 规范检查 | `buf lint` | 失败即阻断 |
| 破坏性检查 | `buf breaking --against` 基线（冻结后 = `contracts/v1.1.0` tag） | **检出 Breaking 即置红；分支保护设为 required check，合并按钮自动置灰** |
| wire 一致性 | `scripts/check-wire-sync.sh` | 契约承诺的 rpc/字段编号必须与 `coord-proto` 实现一致，防"空头承诺"与"静默漂移" |
| 期限卡口 | `scripts/check-wire-sync.sh`（解析 STATUS.md） | COMMITTED 服务 GA 期限逾期且未迁移至契约包 → 置红（倒逼机制，§13） |

### 11.2 协议变更 PR Checklist（强制）

- [ ] 变更类型（Patch/Minor/Major）已在 PR 标题标注；
- [ ] `buf breaking` 全绿（Major 除外，Major 走 §4 双跑流程）；
- [ ] 新增字段为 optional 语义，缺省值不改变既有行为；
- [ ] 字段注释符合 §10 规范，无内部实现泄漏；
- [ ] 错误码映射无变更，或已按 Breaking 流程处理；
- [ ] CHANGELOG 已更新；废弃字段含 `sunsetDate`（存活期 ≥ 12 个月）；
- [ ] 已填写《协议变更消费者告知模板》：**影响范围（消费者数量/名单）**、
      **回滚预案**、灰度计划。

#### 11.2.1 `contracts/v1.2.0` 的消费者告知记录（已执行）

本版是本白皮书**第一次动用 §11 流程做「对已公示承诺的变更」**（其余变更都是新增
包/新增字段），故把模板逐栏落成可核验的事实，而不是勾选：

| 模板栏位 | 本次内容 |
|:---|:---|
| **变更类型** | **Minor**（新增 11 个包 + 5 项状态位提升）。**无 Breaking**：未改动任何既有包的字段编号与类型；`buf breaking` 基线 = `contracts/v1.1.2`，CI 强制 |
| **影响范围（消费者数量/名单）** | 受影响的对外标识只有状态位与包**新增**。被提前提升的 4 个 `coord.experimental.*` 包在**本仓零引用**（从来只有 `STATUS.md` 的台账行，**没有 proto 文件、没有生成产物、没有调用方**）⇒ 已知消费者数 **0**。`coord.storage` 状态位提升**不动包名/服务名/字段**，既有消费者无需改代码。<br>**不知情消费者风险**：本仓不可证明仓外无人按 `coord.experimental.*` 自行生成过客户端 ⇒ 计入未决项 **U5**，按"公告优先于假设"处理（见下"告知动作"） |
| **回滚预案** | 状态位提升不回滚（回滚 = 重新降级为 EXPERIMENTAL，属另一次 §11 变更）。`coord.storage` 保持原名不迁，故**不存在需要回滚的 wire 变更**。若需回滚代码面：契约面与代码面**可独立交付**（D4：契约先行、代码顺延），回滚代码 tag 不影响已发布的 `contracts/v1.2.0` |
| **灰度计划** | 本版**无 wire 变更**，故不设按比例灰度。交付顺序上的等价物：契约面先发（本版），代码面 tag `v0.2.0` 待整改基线 §6 的判据全绿后再打；期间消费方可先按新契约生成客户端（**契约消费预览不受限**，见 §12.1） |
| **告知动作** | ① `apis/contracts/CHANGELOG.md` 的 `contracts/v1.2.0` 条目含逐包对照表与"不得静默提前"声明；② `STATUS.md` 的 EXPERIMENTAL 区清空并写明理由；③ 本白皮书 §1.3 / §9.1 / §9.1.1 / §12 / §13 同步修订；④ CI `.github/workflows/contract-check.yml` 的 5 道卡口作为**机器化**的持续告知（任何后续漂移立即置红） |

### 11.3 合约测试（Consumer-Driven Contract）

- 维护一组模拟业务方的 Mock Client（Rust `coord-client` + Java `coord-java-sdk`
  各一），覆盖 KV/Txn/Lease/Watch 四域各 ≥1 正向用例 + 1 错误码用例
  （验证客户端只依赖 Status Code 行为正确）；
- 每次服务端发布前自动运行，响应结构必须严格匹配契约 proto。

### 11.4 紧急变更

安全漏洞所需的紧急收紧可豁免"提前一个 Minor 公告"，但必须在 48 小时内
补发公示并更新 CHANGELOG，且不得变更字段编号/类型（只允许服务端行为收紧）。

---

## 12. 已知限制（2026-09-20 基线）

以下不影响协议承诺的稳定性，但影响生产使用决策，必须如实告知消费者：

1. **承诺面服务已在契约包名下**：§1.3 全部 17 项能力的实现已从
   `coord.agent.*` 内部包迁至 `coord.<domain>.v1` 契约包（wire 逐字段未变），
   两侧一致性由 wire-sync + descriptor 两道卡口持续守住。
   GA 期限见 §13；期限前请勿将 COMMITTED 契约当作生产可用面验收
   （契约消费预览不受限）。
2. **生产就绪验收未全部通过**（整改文档 §6 九门）：独立 Jepsen、72h 浸泡、
   第三方安全审计、kind 部署验证证据缺失（P0-03/R10）。
3. 共识依赖 `openraft 0.10.0-alpha.25`（alpha 版本，P0-04；上游无稳定版，
   按风险接受处理）。
4. 部分内部错误串直出客户端（P1-01/R5 未整改）——§6.3 纪律必须执行。
5. 监控/部署物/runbook 存在缺口（P1-02/03/04）。
6. gRPC reflection 由 `security.reflection_enabled` 控制（`main.rs:2549`），
   生产默认关闭时 grpcurl 类工具不可用——消费方应以本契约文件生成客户端，
   不依赖服务端反射。
7. **`coord.transit.v1` 的 KEK 供给为「启动注入」，不是外部 KMS**（U9 / U-04，
   2026-09-22 落地）：KEK **不再**由 `kek_id` 配置串确定性派生。启用本服务时必须
   注入 32 字节密钥材料（环境变量 `COORD_TRANSIT_KEK`（hex64），或
   `<agent data_dir>/transit-kek.bin`（32 字节原始材料）），
   KEK = `HKDF-SHA256(材料, info="coord-transit-kek-v1:" || kek_id)`；
   材料缺失/长度不符 ⇒ agent **拒绝启动**（fail-closed，无回落路径）。
   因此「拿到配置即可推导 KEK」**已不成立**（`kek_id` 降级为域分隔/审计标签）。
   仍未闭合的边界：**本方案不是外部 KMS**——材料以文件/环境形态落在 agent 主机上，
   主机被控即泄露；落盘 DEK 的静态保护另有一层 coord-server redb + Barrier 加密。
   消费方不应把它当作外部 KMS 的替代品。
8. **`replication`（`coord.agent.Replica`）永不对外**（红线 §9.3 R3）：
   它是 ISR 内部复制通道，受支持、有测试、有文档，但**不产生对外契约包**。

---

## 13. GA 期限与倒逼机制

下表与 `STATUS.md` 的 COMMITTED 段**逐行对应**，由 `check-wire-sync.sh` 解析：
COMMITTED 服务的 GA 期限逾期且未迁移至契约包 → CI 置红。

| 能力 | 契约包 | GA 期限 | 逾期行为 |
|:---|:---|:---:|:---|
| 服务注册发现 | `coord.registry.v1` | 2026-10-31 | check-wire-sync 置红 |
| 分布式 ID | `coord.idgen.v1` | 2026-10-31 | 同上 |
| 分布式锁 | `coord.lock.v1` | 2026-11-30 | 同上 |
| Leader 选举 | `coord.election.v1` | 2026-11-30 | 同上 |
| 事件通知 | `coord.event.v1` | 2026-12-31 | 同上 |
| 配置中心 | `coord.config.v1` | 2026-12-31 | 同上 |
| 权限策略引擎 | `coord.policy.v1` | 2026-12-31 | 同上 |
| 熔断器 | `coord.circuitbreaker.v1` | 2026-12-31 | 同上 |
| 限流器 | `coord.ratelimiter.v1` | 2026-12-31 | 同上 |
| 安全传输（信封加密） | `coord.transit.v1` | 2026-12-31 | 同上 |
| PKI 证书签发 | `coord.pki.v1` | 2026-12-31 | 同上 |
| 缓存 | `coord.cache.v1` | 2026-12-31 | 同上 |
| 消息队列 | `coord.mq.v1` | 2026-12-31 | 同上 |
| 特性开关 | `coord.featureflags.v1` | 2026-12-31 | 同上 |
| 对象存储（Server） | `coord.storage` | 2026-12-31 | 同上 |
| 工作流（Saga） | `coord.workflow.v1` | 2027-03-31 | 同上 |
| 调度 | `coord.scheduler.v1` | 2027-03-31 | 同上 |

- **GA 验收定义（G1–G6）**：① 契约文件存在并进 wire 卡口；② 实现已挂在契约包名下；
  ③ descriptor 级 wire 相等；④ **语义边界（不承诺什么）已写入契约注释**（§10 规则 4/5）；
  ⑤ 有可复现验收（测试或 lab 产物）；⑥ 默认开关口径已裁定。
- **期限调整 = 契约变更**：走 §11 变更流程并公示，不得静默顺延。
- **状态位提前提升也是契约变更**：`contracts/v1.2.0` 把原 EXPERIMENTAL 的四项
  提前提升为 COMMITTED，已验证 §11.2 Checklist（含消费者告知）并记入 CHANGELOG。
  **不得静默提前。**
  承诺不是摆设——逾期即红牌，直到兑现。
- **v1.2 候选**：Auth 客户端面（前置：CCT v3 迁移完成）、Config 中心
  （前置：包迁移）、Event 持久化游标、Maintenance 运维面只读化。

---

*本白皮书随 `apis/contracts/` 一同版本化。修订本文件 = 修订承诺本身，一律走 §11 变更流程。*
