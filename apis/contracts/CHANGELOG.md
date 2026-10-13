# 契约变更记录（CHANGELOG）

版本规则见 WHITEPAPER.md。契约版本独立于代码版本。

## [contracts/v1.5.0] — 2026-10-12（Minor：Transit HMAC 用途化密钥与轮换窗口，G-TR-3 / ADR-0012）

**发布口径**：本条目为 **Minor**（新增字段 + 注释级语义声明 + 行为收紧；
不改动任何既有字段编号与类型；`buf breaking` 基线 = `contracts/v1.4.0`，CI 强制）。

**新增字段（3 个消息，向后兼容）**

| 消息 | 字段 | 语义 |
|:--|:--|:--|
| `coord.transit.v1.TransitHmacSignRequest` | `string purpose = 3` | 必填用途标签（非空、不含 NUL、≤128 字节）；按用途域分隔派生 HMAC 密钥，不同用途互不可验 |
| 同上 | `string key_id = 4` | 可选材料标识：空 = 主材料（新写入）；显式 = 指定材料（轮换期散列变体复算） |
| `coord.transit.v1.TransitHmacSignResponse` | `string key_id = 3` | 服务端分配：实际签名材料标识；调用方必须与签名一起持久化 |
| `coord.transit.v1.TransitHmacVerifyRequest` | `string purpose = 4` | 必填：签名时的用途 |
| 同上 | `string key_id = 5` | 必填：签名时的材料；空 ⇒ `INVALID_ARGUMENT`，未注入 ⇒ `FAILED_PRECONDITION`（fail-loud） |

**行为收紧（消费者告知）**

- `HmacSign` / `HmacVerify` 的 `purpose` 与 `HmacVerify` 的 `key_id` 由缺省变为必填；
  未提供（含 coord-java-sdk 0.2.1 二进制）⇒ `INVALID_ARGUMENT`。
- 不支持算法由 `INTERNAL` 改为 `INVALID_ARGUMENT`（与参数非法同口径；无已知消费者依赖）。
- Java SDK：无用途/无 `key_id` 的旧 HMAC 方法被用途化 API 取代（0.3.x 未发布窗口内）。
- **存量签名不可迁移**：本改动前产出的 HMAC（无 key_id、旧派生公式）一律不可复算；升级后须按新主材料重散列回写（流程见 runbook §4.4）。

**影响范围（消费者数量/名单）**：本仓已知 HMAC 调用方 **0**（`coord-client` / `coord` /
`java-example` 无调用；`coord-java-sdk` 0.2.1 已发布二进制中的 `hmacSign` / `hmacVerify`
调用将收到 `INVALID_ARGUMENT`）。EIS 尚未切换到 transit（ADR-0012；`eis/docs/IMPLEMENTATION.md`
将其列为可选映射）。仓外不知情消费者无法在仓内证否，按「零已知消费者 + 本条目公告」处理。

**回滚预案**：回滚本分支即可；无服务端状态迁移（材料注入集合与落盘格式均未变），
回滚后旧客户端行为恢复。

**灰度计划**：不支持滚动升级（见 `docs/production/ops/upgrade.md`）——全集群同版本切换；
切换前完成客户端 SDK 升级与 `key_id` 持久化改造。

**语义声明（wire 不变）**

- HMAC 密钥 = `HKDF-SHA256(注入材料, info = "coord-transit-hmac-v1:" || u32be(len(kek_id)) || kek_id || u32be(len(purpose)) || purpose)`；
  历史材料**参与** HMAC 派生（作废「仅派生解密 KEK」注释）。
- 轮换窗口：新签名用主材料；旧签名按其 `key_id` 在历史材料注入期可复算；材料下线后显式
  `key_id` ⇒ `FAILED_PRECONDITION`。HMAC 不参与 `Rewrap`（迁移 = 重散列回写）。
- 主材料标识经 `COORD_TRANSIT_KEK_ID` 注入（缺省 `default-kek`）；**轮换 = 新标识 + 新材料**，
  旧材料按旧标识注入历史集合；同一标识不得对应两份材料。
- 跨 agent 一致性：同集群所有 agent 必须注入相同材料与标识；同一
  `(purpose, data, key_id, algorithm)` 在任一 agent 结果一致。

**校验（本地已跑）**

- `apis/contracts/scripts/check-wire-sync.sh` exit 0；`check-wire-descriptor.sh` exit 0；
  `check-sdk-sync.sh` exit 0；`cargo fmt --all -- --check` / `cargo clippy --all-targets --all-features`
  / `cargo test` 全绿；`scripts/check-doc-refs.sh` / `scripts/check-panics.sh` /
  `scripts/check-consumer-doc-labels.sh` exit 0。
- `coord-java-sdk` `mvn -B -q verify` 全绿 —— **JDK 21.0.12.1（Temurin）**（Tests run 186 /
  failures 0 / errors 0）；`java-example` `mvn -B -q verify -DskipTests` exit 0。

## [contracts/v1.4.0] — 2026-10-11（Minor：消费方审计第二批增量——配置位点/缓存键空间/调度结果回读/开关管理面）

**发布口径**：本条目为 **Minor**（新增 RPC / 新增字段 / 注释级语义声明；
不改动任何既有字段编号与类型；`buf breaking` 基线 = 上一合同的既有包，CI 强制）。
新增 RPC 7 个（Cache ×3、Scheduler ×1、FeatureFlags ×3）、新增字段 1 个
（`ConfigListResponse.revision`）与 1 个新消息组（`CacheSetIfAbsent*` /
`CacheKeys*` / `CacheClear*` / `SchedulerGetJob*` / `FeatureFlagPutFlag*` /
`FeatureFlagDeleteFlag*` / `FeatureFlagListFlags` / `FeatureFlagEntry`，全部新增）。

**新增 RPC（7 个）**

| 契约包 | RPC | 能力 | 说明 |
|:--|:--|:--|:--|
| `coord.cache.v1` | `SetIfAbsent` | `coord:cache:write` | 原子条件写（SETNX）：仅当键不存在/已过期时写入；返回 `written`；单 agent 同事务判定，ISR 在 Leader 本地事务判定 |
| `coord.cache.v1` | `Keys` | `coord:cache:read` | 键枚举：有界、游标分页、四类逻辑键去重升序；本节点视角 best-effort |
| `coord.cache.v1` | `Clear` | `coord:cache:write` | 按逻辑键前缀清理全部数据类型（批式；ISR 经复制通道全域下发） |
| `coord.scheduler.v1` | `GetJob` | `coord:scheduler:read` | 任务详情 / 结果回读（G-SC-1 读取面）：只读幂等；未注册 ⇒ `found=false` |
| `coord.featureflags.v1` | `PutFlag` | `coord:flags:manage` | 写入 / 覆盖开关（全量或 `percentage` 灰度，`[0,100]` 外拒绝） |
| `coord.featureflags.v1` | `DeleteFlag` | `coord:flags:manage` | 删除开关（幂等） |
| `coord.featureflags.v1` | `ListFlags` | `coord:flags:read` | 列出全部开关（按键升序） |

**新增字段（1 个消息，向后兼容）**

- `coord.config.v1.ConfigListResponse` 新增 `int64 revision = 2`（快照位点 W；
  0 = 位点未知）。

**语义声明变更（wire 不变；含一处「坐标轴变更」告知）**

- `coord.config.v1`：**快照 + 位点续订（resync 纪律）**——`List` 返回快照位点；
  `Watch(start_revision)` 按位点回放后转实时；`WatchEvent.new_value` 的
  presence 缺省 = **键已删除**（空串 = 存储的空值）；事件 `revision` 为**全局位点**。
  ⚠️ **Consumer 告知**：`ConfigPutResponse.revision` 由「每键版本号」改为
  **全局位点**（Server KV 全局 Revision 口径）——此前该字段无文档承诺，
  若曾按每键版本号解析需改用全局位点语义（与 List/Watch 同一坐标轴）。
  位点不可达（历史压缩）时 Watch 流以 `FAILED_PRECONDITION` 终止（重读 List 重订）。
- `coord.cache.v1`：SetNX / TTL 交互（仅写入成功时生效）、ISR 错误语义同 `Set`
  （本地可能已生效；重试 = 再次尝试获取）；Keys/Clear 的边界（有界扫描 /
  批式 / ISR 全域下发 / NUL 限制）见 proto 与 `boundaries.md` B-PL-5。
- `coord.scheduler.v1`：`CompleteJob.result` 的读取面已补（`GetJob`）——
  「留档用于重启不丢、无读取面」的旧注释作废；结果首个为准、重启/换 agent 可读。
- `coord.featureflags.v1`：管理面 RPC 落地 ⇒「开关更新是带外发生的」的旧口径
  改为「RPC 管理面 + 带外 KV 路径并存」；读路径直读存储无本地缓存 ⇒ 写入立即生效。

**校验**（本地已跑）

- `apis/contracts/scripts/check-wire-sync.sh` exit 0（包名/rpc/字段编号双副本一致）
- `apis/contracts/scripts/check-wire-descriptor.sh` exit 0（descriptor 级 wire 相等）
- `apis/contracts/scripts/check-sdk-sync.sh` exit 0（契约面 ↔ Java SDK 命名空间）

## [contracts/v1.3.0] — 2026-10-09（Minor：ADR-0010 全量承接批次的契约增量）

**发布口径**：本条目为 **Minor**（新增 RPC / 新增字段 / 注释级语义声明；
不改动任何既有字段编号与类型；`buf breaking` 基线 = `contracts/v1.2.0`，CI 强制）。
新增 RPC 6 个（MQ ×3、Transit ×1、Workflow ×2）、新增字段 4 个（MqMessage ×2、
event ×2，全部向后兼容），以及 MQ 路由/删除、Policy 本地定位、Scheduler result
持久化、Transit 多材料、Event 持久化游标、Workflow 保留策略等边界声明。

**新增 RPC（6 个）**

| 契约包 | RPC | 能力 | 说明 |
|:--|:--|:--|:--|
| `coord.mq.v1` | `GetTopicLeader` | `coord:mq:consume` | topic 的 Leader/ISR 拓扑查询（任意 agent 可答；单 agent 语义显式） |
| `coord.mq.v1` | `MoveToDlq` | `coord:mq:manage` | 显式移入 DLQ（管理路径；ISR 启用时经复制通道全域一致） |
| `coord.mq.v1` | `DeleteTopic` | `coord:mq:manage` | 删除 topic 并全量回收存量（配额归还；ISR 全域一致；同名重建 = 空 topic） |
| `coord.transit.v1` | `Rewrap` | `coord:transit:crypto` | KEK 材料迁移管理路径（旧材料解出 → 主材料重包） |
| `coord.workflow.v1` | `DeleteDefinition` | `coord:workflow:define` | 保留策略：删定义（前置：无实例引用；拒绝→`FAILED_PRECONDITION`） |
| `coord.workflow.v1` | `DeleteInstance` | `coord:workflow:execute` | 保留策略：删实例（仅终态；非终态→`FAILED_PRECONDITION`；不可回滚） |

**新增字段（2 个消息，向后兼容）**

- `coord.mq.v1.MqMessage` 新增 `dlq_reason = 7` / `dlq_detail = 8`
  （仅 `PollDlq` 填充；普通消息为空）——DLQ 内容含原因可读（G-MQ-2）。
- `coord.event.v1`：`EventSubscribeRequest.cursor = 2`（持久化游标，十进制 seq；
  空 = 默认实时不补投）与 `CloudEventMessage.seq = 9`（全局单调序号）——
  断线重连后可在保留窗口内按位点补投（G-EV-1；“保留窗口 = Server KV 中
  仍保留的事件”，投递窗口与语义见 proto 头注）。

**注释级语义声明变更（wire 不变）**

- `coord.mq.v1`：Leader 与消费路由（非 Leader 写路径 = `FAILED_PRECONDITION` +
  `x-coord-error-code: NOT_LEADER` + `coord-leader-hint` trailer，可编程判别）、
  删除语义（前置条件 / 全域一致 / 删除后 `NOT_FOUND`）。
- `coord.policy.v1`：`CheckPermission` 明确为 **agent 本地 / 嵌入式**用途；
  生产接入走 OPA bundle（分发/加载/收敛口径一并声明）+ `Evaluate`。
- `coord.scheduler.v1`：`CompleteJob.result` 由「接受但不留档」变更为
  **随任务记录持久化**（首个结果为准；契约面仍无读取 RPC）——能力增强，
  不构成 Breaking。
- `coord.transit.v1`：多材料解密窗口与 `Rewrap` 的错误语义声明。
- `coord.workflow.v1`：删除语义（终态守卫 / 引用守卫 / 删除后 `NOT_FOUND` /
  不可回滚）；补偿语义口径（`compensatedBy` 为 catch-all 转场，**无自动逆序
  栈**——逆序由 DSL 连线表达）。

**消费者告知**

- 全量承接口径与批次计划见 `docs/adr/0010-eis-consumer-gaps-full-landing.md`；
  缺口编号（G-*）与验收锚点同上。
- 非 Leader 错误新增结构化 trailer：既有按文案解析的消费者可不变，新代码应
  优先读 `x-coord-error-code` / `coord-leader-hint`。
- `DeleteTopic` 为破坏性管理路径：调用方须先停该 topic 读写（前置条件见 proto 头注）。
- `DeleteInstance` / `DeleteDefinition` 为保留策略管理路径：删除**不可回滚**
  （无归档副本），保留节奏由调用方显式管理（语义见
  `docs/production/workflow-semantics.md`）。

**校验**（本地已跑）

- `apis/contracts/scripts/check-wire-sync.sh` exit 0（包名/rpc/字段编号双副本一致）
- `apis/contracts/scripts/check-wire-descriptor.sh` exit 0（descriptor 级 wire 相等）
- `bash scripts/check-gate-drills.sh` exit 0（告警↔runbook 一一对应）

## [contracts/v1.2.0] — 2026-09-19（Minor：coord-agent 全量契约面补齐 + EXPERIMENTAL 区清空）

**发布口径**：本条目为 **Minor**（新增包 + 状态位提升），**不改动任何既有包的字段编号与类型**
（`buf breaking` 基线 = `contracts/v1.1.2`，CI `.github/workflows/contract-check.yml` 强制）。

**新增契约包（11 个，全部直接建为稳定包 `coord.<domain>.v1`）**

`coord.config.v1` / `coord.pki.v1` / `coord.policy.v1` / `coord.circuitbreaker.v1` /
`coord.ratelimiter.v1` / `coord.transit.v1` / `coord.cache.v1` / `coord.mq.v1` /
`coord.workflow.v1` / `coord.scheduler.v1` / `coord.featureflags.v1`。

- 每个包的 wire（service 名 / rpc 名 / 字段编号与类型）**逐字取自**迁移前的实现
  `coord-proto/src/proto/agent_api.proto` 对应 service 块，**未趁机改动任何 wire**。
- **不以 `coord.experimental.*` 形态开放**：`STATUS.md:35-39` 曾声明的 4 个
  `coord.experimental.*` 包**从未有 proto 文件、从未有任何消费者**（本仓零引用），
  故直接建为稳定包**不构成 Breaking**，亦不违反 WHITEPAPER.md 的实验包规则
  （该规则约束"以实验包形态对外开放"，本版不以实验包形态开放任何东西）。

**状态位变更（对已公示承诺的变更 —— 须按 WHITEPAPER.md 的变更流程重新公示，含消费者告知）**

| 契约包 | 原状态 | 新状态 | 原期限 | 说明 |
|:---|:---|:---|:---|:---|
| `coord.experimental.cache.v1` | EXPERIMENTAL | → `coord.cache.v1` **COMMITTED** | 2026-12-31 | 提前提升；验收项见 STATUS.md |
| `coord.experimental.mq.v1` | EXPERIMENTAL | → `coord.mq.v1` **COMMITTED** | 2026-12-31 | 提前提升 |
| `coord.experimental.workflow.v1` | EXPERIMENTAL | → `coord.workflow.v1` **COMMITTED** | 2027-03-31 | 提前提升 |
| `coord.experimental.scheduler.v1` | EXPERIMENTAL | → `coord.scheduler.v1` **COMMITTED** | 2027-03-31 | 提前提升 |
| `coord.storage` | EXPERIMENTAL | **COMMITTED** | 2026-12-31 | 状态位提升；**包名不迁**（无 `.v1` 后缀），改名即 Breaking，本版不改 |

> **这不是"到期自然转正"，而是对已公示承诺的提前变更**，属 WHITEPAPER.md 变更流程的适用情形，
> 故在本条目中显式公示（含上表逐包对照），不得静默提前。

**状态表对齐**

- `STATUS.md` 的 EXPERIMENTAL 区**清空**：上述 5 项全部转入 COMMITTED 段。
- `WHITEPAPER.md` 的 4 项实验能力清单与 `STATUS.md` 的 5 行口径分歧（多出 `coord.storage`）
  以 **`STATUS.md` 为准**（它已含 `coord.storage`，且该包 proto 已存在、Java SDK 已有
  `objectStore()` 访问器）。
- `WHITEPAPER.md` 中两条缺陷描述经代码实测**已过期**，按变更流程同步修订
  （Cache「ISR 提交非原子」→ 跨节点提交非原子；Workflow「无持久化/补偿」→ 已有
  `KvWorkflowStore` + 补偿语义，改为验收项）。

**边界声明（同上批，2026-09-20 补齐；WHITEPAPER.md 边界声明规则的落点）**

- `coord.event.v1` 的 `Unsubscribe`：实测服务端**忽略请求、恒回成功** —— 订阅的生存期
  就是那条 gRPC 流，服务端没有按 `subscription_id` 索引的注册表。**保留该 RPC 不删**
  （删除会让按契约生成的客户端编译不过），改为在 proto 里显式声明「不携带状态」，
  并指明真正的取消方式是关闭 `Subscribe` 流。`subscription_id` 当前亦非服务端分配。
- `coord.scheduler.v1` 的 `ClaimJob` / `Heartbeat` / `CompleteJob`：`job_id` 即**认领
  句柄与凭据**；`payload` 回传注册时携带的**原字节**（二进制安全）；句柄失效时
  `Heartbeat` / `CompleteJob` 返回 `FAILED_PRECONDITION`（不再是空 OK）；已 `Completed`
  的任务重复完成仍**幂等**；`result` **被接受但不留档**（契约里没有读取结果的 RPC）。
- 以上均为**注释/语义声明级**变更：`buf breaking` 与 descriptor 级 wire 比对全绿
  （`check-wire-descriptor.sh` exit 0），两份 proto 副本逐字相同。

**SDK 面（同批补齐，2026-09-20）**

- `coord-java-sdk` 新增 `event` 与 `scheduler` 两个客户端面（`EventClient` /
  `SchedulerClient` + `CoordClient.events()` / `.scheduler()`）。此前这两个 **COMMITTED**
  包在 SDK 里连接口都不存在 —— 属「契约已承诺、客户端面缺失」，由新增的
  `check-sdk-sync.sh` **反向覆盖**校验抓出（现 `17/17` 契约包均有 SDK impl）。

**不迁移（保持内部，不建对外契约）**

`coord.agent.Handshake` / `coord.agent.Health` / `coord.agent.Replica` —— 前者为协议协商、
中者为探活、后者为 ISR 复制通道（WHITEPAPER.md 红线 R3「永不对外」）。
Replication 的定位为**内部能力**（受支持、有测试、有文档），**不产生对外契约包**。

---

## [contracts/v1.1.2] — 2026-09-06（Multi-Raft PD 治理闭环 + 演练收口）

**不改变任何对外承诺**：Multi-Raft/PD 维持红线（永不对外承诺，除非另立版本公告）。

**口径修正（非契约承诺）**

- Multi-Raft 内部实现进度更新：PD operator 全局队列（region 0 raft 承载）补齐
  **跨节点 propose 转发**——执行器可在「目标 Region leader」节点认领并
  经节点间 `SubmitPdOp` RPC 把 Claim/Complete 转发到 region 0 leader 提出
  （修复 transfer-leader 真实进程演练暴露的「执行器仅能在 region 0 leader ==
  Region leader 时执行」缺陷）。
- 真实进程演练收口（`multi_raft_process_test`，本地 6/6 PASS）：
  transfer-leader 均衡、add-peer（target bump 滚动重启）、关→开→关升级/回滚
  （含 fail-closed 闸真实进程验证）、chaos region 模式（kill/partition × PD +
  线性一致）。
- 验证与收尾（Jepsen multi-register 矩阵归档、性能 80% 阈值、摘牌发布）如需对外承诺，按
  红线约定「另立版本公告」另行立项（本条目不含任何对外承诺）。

---

## [contracts/v1.1.1] — 2026-09-05（Multi-Raft 实现进度更新）

**不改变任何对外承诺**：Multi-Raft/PD 维持红线（永不对外承诺，除非另立版本公告）。

**口径修正（非契约承诺）**

- Multi-Raft/PD 内部实现进度更新：Phase 2–3 已完成并合入 main
  （region 目录级存储隔离 + 内嵌 PD 已接线生产路径，`[multi_raft]` 默认关闭 opt-in），
  不再处于「零生产路径引用」状态；WHITEPAPER.md 证据列已同步。
- 历史条目（v1.0.0「Multi-Raft/PD（experimental，零生产路径引用）」）保留不改，
  系当时状态的真实记录（历史条目不改原则）。
- 功能闭环与验证（per-Region Watch/Lease/快照/压缩、Jepsen multi-register 矩阵、72h soak、
  性能基线）如需对外承诺，按红线约定「另立版本公告」另行立项（本条目不含任何对外承诺）。

---

## [contracts/v1.1.0] — 2026-08-27（承诺面扩展：协调能力入契约）

**哲学变更（WHITEPAPER 重写为 v1.1.0）**

- 契约定位从「底座原语兼容性承诺」改为「服务协调能力承诺」：
  KV/Txn/Lease/Watch 降级为实现底座，业务消费面 = 协调能力。
- 新增三态分层 STABLE / COMMITTED / EXPERIMENTAL（WHITEPAPER.md 三态分层）。
- 新增 `STATUS.md` 契约状态表（目标日期，机器可读）；
  `check-wire-sync.sh` 增加目标日期校验：COMMITTED 服务逾期未迁移至契约包 → CI 失败。

**纳入（COMMITTED，承诺中 + 目标日期）**

| 能力 | 契约包 | 目标日期 |
|:---|:---|:---:|
| 服务注册发现 | `coord.registry.v1` | 2026-10-31 |
| 分布式锁 | `coord.lock.v1` | 2026-11-30 |
| Leader 选举 | `coord.election.v1` | 2026-11-30 |
| 分布式 ID | `coord.idgen.v1` | 2026-10-31 |
| 事件通知 | `coord.event.v1` | 2026-12-31 |

wire 与 `coord.agent.*` 现行实现逐字段一致，迁移为机械式重挂载（禁止趁机改 wire）。

**底座原语（STABLE，保留）**：KV / Txn / Lease / Watch / Maintenance.Status / Health。

**实验承诺区（EXPERIMENTAL，缺陷清单 + 目标日期）**：Cache / MQ / Workflow / Scheduler
（缺陷清单与目标日期见 WHITEPAPER.md）。

**接入路径修正**：业务入口 = 本机 Agent `127.0.0.1:19527`；
Server 端口（50051/50052）仅 Agent 可达，不向业务网络开放。

## [contracts/v1.0.0] — 2026-08-27（首次冻结）

基线代码：`82eb398`（main）。

**纳入（稳定承诺）**

- `coord.kv.KV`：`Put` / `Range` / `Delete`
- `coord.txn.Txn`：`Txn`
- `coord.lease.Lease`：`LeaseGrant` / `LeaseRevoke` / `LeaseKeepAlive`（双向流）
- `coord.watch.Watch`：`Watch`（双向流）
- `coord.maintenance.Maintenance`：仅 `Status`（`StatusResponse` 仅承诺 `revision` 字段，
  编号 2–5 reserved）
- `grpc.health.v1.Health`：`Check` / `Watch`

**随附承诺文本**

- 兼容性铁律与废弃策略（WHITEPAPER.md）
- 错误码契约（WHITEPAPER.md，以 `map_core_error` 代码为准；
  修正内部文档 NotLeader 映射漂移：实际为 `UNAVAILABLE` + `coord-leader-hint`）
- Metadata 约定（WHITEPAPER.md）
- 语义边界：Revision 单调递增但不承诺与 Raft Log Index 对齐（WHITEPAPER.md）

**明确排除（红区，不承诺）**

- Multi-Raft/PD（experimental，零生产路径引用）
- Cache/MQ ISR 复制（experimental，R-AGT-13）
- Seal/Unseal/静态加密（默认关闭，未生产验证）
- Workflow（生产路径内存态）、Agent 侧其余服务、`coord.agent.Replica`（内部）

**质量校验**

- 新增 `.github/workflows/contract-check.yml`：buf lint + buf breaking + wire-sync 三项校验。

---

*后续条目模板：版本号 / 日期 / 变更类型（Patch|Minor|Major）/ 变更内容 / 影响范围（消费者数量）/ 回滚预案 / sunsetDate（如涉及废弃）*
