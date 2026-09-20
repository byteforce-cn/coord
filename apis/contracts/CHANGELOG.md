# 契约变更记录（CHANGELOG）

版本规则见 WHITEPAPER.md §5。契约版本独立于代码版本。

## [contracts/v1.2.0] — 2026-09-19（Minor：coord-agent 全量 GA 契约面补齐 + EXPERIMENTAL 区清空）

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
  故直接建为稳定包**不构成 Breaking**，亦不违反 WHITEPAPER §9.2 实验包规则
  （该规则约束"以实验包形态对外开放"，本版不以实验包形态开放任何东西）。

**状态位变更（对已公示承诺的变更 —— 须按 WHITEPAPER §11 重新公示，含消费者告知）**

| 契约包 | 原状态 | 新状态 | 原期限 | 说明 |
|:---|:---|:---|:---|:---|
| `coord.experimental.cache.v1` | EXPERIMENTAL | → `coord.cache.v1` **COMMITTED** | 2026-12-31 | 提前提升；整改项见 STATUS.md |
| `coord.experimental.mq.v1` | EXPERIMENTAL | → `coord.mq.v1` **COMMITTED** | 2026-12-31 | 提前提升 |
| `coord.experimental.workflow.v1` | EXPERIMENTAL | → `coord.workflow.v1` **COMMITTED** | 2027-03-31 | 提前提升 |
| `coord.experimental.scheduler.v1` | EXPERIMENTAL | → `coord.scheduler.v1` **COMMITTED** | 2027-03-31 | 提前提升 |
| `coord.storage` | EXPERIMENTAL | **COMMITTED** | 2026-12-31 | 状态位提升；**包名不迁**（无 `.v1` 后缀），改名即 Breaking，本版不改 |

> **这不是"到期自然转正"，而是对已公示承诺的提前变更**，属 WHITEPAPER §11 变更流程的适用情形，
> 故在本条目中显式公示（含上表逐包对照），不得静默提前。

**台账/上位文本对齐**

- `STATUS.md` 的 EXPERIMENTAL 区**清空**：上述 5 项全部转入 COMMITTED 段。
- `WHITEPAPER.md` §9.1 的 4 项实验能力清单与 `STATUS.md` 的 5 行口径分歧（多出 `coord.storage`）
  以 **`STATUS.md` 为准**（它已含 `coord.storage`，且该包 proto 已存在、Java SDK 已有
  `objectStore()` 访问器）。
- `WHITEPAPER.md` §9.1 的两条缺陷描述经代码实测**已过期**，按 §11 流程同步修订
  （Cache「ISR 提交非原子」→ 跨节点提交非原子；Workflow「无持久化/补偿」→ 已有
  `KvWorkflowStore` + 补偿语义，改为验收项）。修订稿见
  `docs/production/agent-ga-remediation-baseline.md`。

**边界声明（同上批，2026-09-20 补齐；`WHITEPAPER.md` §10 规则 4/5/6 落点）**

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
  `check-sdk-sync.sh` **反向覆盖**卡口抓出（现 `17/17` GA 契约包均有 SDK impl）。

**不迁移（保持内部，不建对外契约）**

`coord.agent.Handshake` / `coord.agent.Health` / `coord.agent.Replica` —— 前者为协议协商、
中者为探活、后者为 ISR 复制通道（WHITEPAPER §9.3 红线 R3「永不对外」）。
Replication 的 GA 口径为**内部能力**（受支持、有测试、有文档），**不产生对外契约包**。

---

## [contracts/v1.1.2] — 2026-09-06（内部口径修正：Multi-Raft PD 治理闭环 + 演练收口）

**不改变任何对外承诺**：Multi-Raft/PD 维持红线 §9.3（永不对外承诺，除非另立版本公告）；
本条目仅记录内部成熟度口径更新（决策 D4 选项 a）。

**口径修正（内部成熟度，非契约承诺）**

- Multi-Raft 内部实现进度更新：PD operator 全局队列（region 0 raft 承载）补齐
  **跨节点 propose 转发（D1-a P5）**——执行器可在「目标 Region leader」节点认领并
  经节点间 `SubmitPdOp` RPC 把 Claim/Complete 转发到 region 0 leader 提出
  （修复 T5.14 transfer-leader 真实进程演练暴露的「执行器仅能在 region 0 leader ==
  Region leader 时执行」缺陷）。
- 阶段 E 前置真实进程演练收口（`multi_raft_process_test`，本地 6/6 PASS）：
  transfer-leader 均衡、add-peer（target bump 滚动重启）、关→开→关升级/回滚
  （含 fail-closed 闸真实进程验证）、chaos region 模式（kill/partition × PD +
  线性一致）。
- 验证与收尾（Jepsen multi-register 矩阵归档、性能 80% 阈值、摘牌发布）见
  `docs/coord-multi-raft-production-plan-2026-09-05.md`（M8–M9）；如需对外承诺按
  红线 §9.3「另立版本公告」另行立项（本条目不含任何对外承诺）。

---

## [contracts/v1.1.1] — 2026-09-05（内部口径修正：Multi-Raft 实现进度更新）

**不改变任何对外承诺**：Multi-Raft/PD 维持红线 §9.3（永不对外承诺，除非另立版本公告）；
本条目仅记录内部成熟度口径更新（决策 D4 选项 a）。

**口径修正（内部成熟度，非契约承诺）**

- Multi-Raft/PD 内部实现进度更新：Phase 2–3（T2.1–T3.4）已完成并合入 main
  （region 目录级存储隔离 + 内嵌 PD 已接线生产路径，`[multi_raft]` 默认关闭 opt-in），
  不再处于「零生产路径引用」状态；WHITEPAPER §2 证据列已同步。
- 历史条目（v1.0.0「Multi-Raft/PD（experimental，零生产路径引用）」）保留不改，
  系当时状态的真实记录（历史条目不改原则）。
- 功能闭环与验证（per-Region Watch/Lease/快照/压缩、进程级测试入 nightly、
  Jepsen multi-register 矩阵、72h soak、性能基线）见
  `docs/coord-multi-raft-production-plan-2026-09-05.md`（M5–M9）；完成后如需对外
  承诺按红线 §9.3「另立版本公告」另行立项（本条目不含任何对外承诺）。

---

## [contracts/v1.1.0] — 2026-08-27（承诺面扩展：协调能力入契约）

**哲学变更（WHITEPAPER 重写为 v1.1.0）**

- 契约定位从「底座原语兼容性承诺」改为「服务协调能力承诺，以承诺倒逼实现」：
  KV/Txn/Lease/Watch 降级为实现底座，业务消费面 = 协调能力。
- 新增三态分层 STABLE / COMMITTED / EXPERIMENTAL（WHITEPAPER §1.2）。
- 新增 `STATUS.md` 承诺台账（GA/整改期限，机器可读）；
  `check-wire-sync.sh` 增加期限卡口：COMMITTED 服务期限逾期且未迁移至契约包 → CI 红。

**纳入（COMMITTED，承诺中 + GA 硬截止）**

| 能力 | 契约包 | GA 期限 |
|:---|:---|:---:|
| 服务注册发现 | `coord.registry.v1` | 2026-10-31 |
| 分布式锁 | `coord.lock.v1` | 2026-11-30 |
| Leader 选举 | `coord.election.v1` | 2026-11-30 |
| 分布式 ID | `coord.idgen.v1` | 2026-10-31 |
| 事件通知 | `coord.event.v1` | 2026-12-31 |

wire 与 `coord.agent.*` 现行实现逐字段一致，迁移为机械式重挂载（禁止趁机改 wire）。

**底座原语（STABLE，保留）**：KV / Txn / Lease / Watch / Maintenance.Status / Health。

**承诺修复区（EXPERIMENTAL，整改承诺 + 期限）**：Cache / MQ / Workflow / Scheduler
（缺陷清单与期限见 WHITEPAPER §9.1）。

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

- 兼容性铁律与废弃策略（WHITEPAPER §3）
- 错误码契约（WHITEPAPER §5，以 `map_core_error` 代码为准；
  修正内部文档 NotLeader 映射漂移：实际为 `UNAVAILABLE` + `coord-leader-hint`）
- Metadata 约定（WHITEPAPER §6）
- 语义边界：Revision 单调递增但不承诺与 Raft Log Index 对齐（WHITEPAPER §7）

**明确排除（红区，不承诺）**

- Multi-Raft/PD（experimental，零生产路径引用）
- Cache/MQ ISR 复制（experimental，R-AGT-13）
- Seal/Unseal/静态加密（默认关闭，未生产验证）
- Workflow（生产路径内存态）、Agent 侧其余服务、`coord.agent.Replica`（内部）

**质量门禁**

- 新增 `.github/workflows/contract-check.yml`：buf lint + buf breaking + wire-sync 三卡口。

---

*后续条目模板：版本号 / 日期 / 变更类型（Patch|Minor|Major）/ 变更内容 / 影响范围（消费者数量）/ 回滚预案 / sunsetDate（如涉及废弃）*
