# 契约变更记录（CHANGELOG）

版本规则见 WHITEPAPER.md §5。契约版本独立于代码版本。

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
