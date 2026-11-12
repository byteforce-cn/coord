# MQ v2 GA Gate（M0–M7 证据与签字）

> Owner: maintainers ｜ Last verified: 2026-11-12（M0 四项全部落地）
>
> **状态：M0 完成（GO）**。M0-1（#178 链接 #332–#339）、M0-2（`coord.mq.v2`
> 冻结为 PROPOSED）、M0-3（`[mq_v2]` 配置/启动校验）、M0-4（服务壳/能力门禁）
> 均已合入。M7 GA 不早于 KV-T GA（`docs/adr/0015-kv-t.md` D8），该前置已于
> 2026-11-11 签字。本文是 MQ v2 gate 状态的**单一事实源**；语义以
> `docs/adr/0016-mq-v2.md` 为准，工作项与依赖以
> `docs/superpowers/specs/2026-10-29-mq-v2-design.md` 为准。
>
> 划线规则：**没有可复跑证据的项不写“通过”**；“未跑”必须留在未达标列。

## 1. Gate 总览

| Gate | 出口口径 | 状态 | 证据（可复跑） |
|:--|:--|:--|:--|
| M0 契约冻结 | ADR/契约/配置/服务壳；默认拒绝 | ✅ | 见 §2 |
| M1 日志/元数据/容量 | 分片/段日志/组状态机/topic 元数据/保留 | 🟡 进行中 | M1-1/M1-2/M1-3/M1-4 已落地：`cargo test -p coord-server --lib storage::mq_group`（12 例）、`cargo test -p coord-server --lib test_apply_mq_group_commands`、`cargo test -p coord-server --lib storage::mq_log`、`cargo test -p coord-server --lib storage::mq_topic`、`cargo test -p coord-server --lib carve_mq`、`cargo test -p coord --bins derive_`；`check-panics` 0 |
| M2 Publish | 路由/批量/offset/幂等/配额 | ⏳ 未开始 | — |
| M3 Poll/Ack/组 | 至少一次/长轮询/代际 fencing/再平衡 | ⏳ 未开始 | — |
| M4 DLQ/管理面 | 死信/删除回收/更新/能力 | ⏳ 未开始 | — |
| M5 工作流 | v2 outbox/durable/审批/trigger | ⏳ 未开始 | — |
| M6 SDK/Ops/证据 | Rust/Java/Ops/性能/混沌/Jepsen | ⏳ 未开始 | — |
| M7 GA | 全绿 + KV-T GA + 签字 | ⛔ 未达标 | 见 §3 |

## 2. M0 分项（启动 gate）

| 项 | 状态 | 证据 / 未达标原因 |
|:--|:--|:--|
| M0-1 ADR/设计/任务树 | ✅ | ADR-0016 accepted；设计文档 + 本文件入库；#178 评论链接 8 个 phase issue（#332–#339） |
| M0-2 `coord.mq.v2` 契约冻结 | ✅ | 两侧 proto 副本 + build 接线；`check-wire-sync.sh` / `check-wire-descriptor.sh`（strict）/ `buf lint` / `check-error-code-contract.sh` / `check-sdk-sync.sh`（PROPOSED 豁免）绿；11 个码 Rust↔Java 逐字一致；19 个 RPC 能力映射进 `rpc_capability`（capability registry 门禁强制） |
| M0-3 `[mq_v2]` 配置/启动校验 | ✅ | `cargo test -p coord config::tests::test_mq_v2`（5 例）；默认关闭；非法值点名 `mq_v2.*`；`enabled=true` 缺 `[kv_t]` 启动拒绝并点名两项 |
| M0-4 服务壳/能力门禁 | ✅ | `cargo test -p coord --test mq_v2_g0_test`（4 例：关闭/未交付/已交付三态 + reflection 可见）；`coord_mq_v2_enabled` gauge；v1 数据面不受影响。**Agent `MqV2Proxy` 转发随 M2-4**（转发需要 Rust client；接线前 `services.mq=true` 在 v2 上仍启动期 fail-closed，避免暴露恒失败的服务） |

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
