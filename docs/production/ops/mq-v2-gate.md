# MQ v2 GA Gate（M0–M7 证据与签字）

> Owner: maintainers ｜ Last verified: 2026-10-29（设计/拆分入库，M0 未启动）
>
> **状态：未启动（NO-GO）**。M0 契约冻结尚未开始；M7 GA 不早于 KV-T GA
> （`docs/adr/0015-kv-t.md` D8）。本文是 MQ v2 gate 状态的**单一事实源**；语义以
> `docs/adr/0016-mq-v2.md` 为准，工作项与依赖以
> `docs/superpowers/specs/2026-10-29-mq-v2-design.md` 为准。
>
> 划线规则：**没有可复跑证据的项不写“通过”**；“未跑”必须留在未达标列。

## 1. Gate 总览

| Gate | 出口口径 | 状态 | 证据（可复跑） |
|:--|:--|:--|:--|
| M0 契约冻结 | ADR/契约/配置/服务壳；默认拒绝 | ⏳ 未开始 | 见 §2 |
| M1 日志/元数据/容量 | 分片/段日志/组状态机/topic 元数据/保留 | ⏳ 未开始 | — |
| M2 Publish | 路由/批量/offset/幂等/配额 | ⏳ 未开始 | — |
| M3 Poll/Ack/组 | 至少一次/长轮询/代际 fencing/再平衡 | ⏳ 未开始 | — |
| M4 DLQ/管理面 | 死信/删除回收/更新/能力 | ⏳ 未开始 | — |
| M5 工作流 | v2 outbox/durable/审批/trigger | ⏳ 未开始 | — |
| M6 SDK/Ops/证据 | Rust/Java/Ops/性能/混沌/Jepsen | ⏳ 未开始 | — |
| M7 GA | 全绿 + KV-T GA + 签字 | ⛔ 未达标 | 见 §3 |

## 2. M0 分项（启动 gate）

| 项 | 状态 | 证据 / 未达标原因 |
|:--|:--|:--|
| M0-1 ADR/设计/任务树 | 🟡 本次 PR（合入后生效） | ADR-0016 accepted；本文件；设计文档；#178 链接 8 个 phase issue |
| M0-2 `coord.mq.v2` 契约冻结 | ⏳ 未开始 | 需要 proto 两侧副本、descriptor/wire/error-code 校验 |
| M0-3 `[mq_v2]` 配置/启动校验 | ⏳ 未开始 | 默认关闭；requires `[kv_t]`；区间/分片校验 |
| M0-4 服务壳/能力/代理骨架 | ⏳ 未开始 | 关闭时 `FAILED_PRECONDITION`；v1 字节级不变 |

## 3. 关闭 M7 的硬条件

1. M0–M6 每个 gate 的出口判据全绿，证据可在仓库用命令复跑。
2. **KV-T GA 已签字**（`docs/production/ops/kv-t-ga-gate.md` §4）；MQ v2 GA 不早于它。
3. v1 MQ 在 v2 集群仍 fail-closed；`[mq_v2].enabled=false` 时 v1 行为字节级不变。
4. v2 工作流 durable/审批/MQ trigger 的端到端证据（M5-4）通过。
5. 性能/容量/长稳/混沌/Jepsen 证据（M6-3/M6-4）通过；未达标项按 D13 停止，不静默降级。
6. 契约 `coord.mq.v2` 从 PROPOSED 翻 COMMITTED，且 `STATUS.md`/WHITEPAPER/
   consumer-contract/boundaries 同步。
7. §4 签字记录填写；未填即“未签”。

## 4. 签字记录

| 日期 | 结论 | 依据 commit / CI run | 未关闭项 | 签字人 |
|:--|:--|:--|:--|:--|
| — | **未签** | — | §3 全部 | — |

## 5. 回退/降级口径

- `[mq_v2].enabled=false` 回退：停 MQ v2 服务/后台任务；v2 KV/事务面不受影响；
  已写入的 MQ 数据留在 Region 状态机中，重新开启后可继续读。
- **不能**回退到 MQ v1：v2 集群不服务 v1 MQ，也没有 v1→v2 数据迁移（ADR-0016 D1）。
- 任一 gate 未达标 ⇒ 停在 gate，不静默降级语义；需要降级时新增 ADR 并记录风险。
- MQ v2 与 KV-T 的 gate 相互独立：KV-T 已 GA 后，MQ v2 出问题不回退 KV-T 语义。
