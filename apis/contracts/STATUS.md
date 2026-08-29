# 承诺台账（STATUS）

> 单一事实来源：契约状态、GA/整改期限、实现落点。`scripts/check-wire-sync.sh` 解析本文件。
>
> **列结构是机器契约，勿改动**：`| 能力 | 契约包 | 状态 | 期限 | 实现落点/整改要点 |`
>
> 状态：`STABLE` 稳定承诺 ｜ `COMMITTED` 承诺中（期限 = GA 硬截止，逾期即 CI 红）｜
> `EXPERIMENTAL` 整改承诺（期限 = 整改验收，治理跟踪，不得以稳定面消费）

## 能力承诺面（COMMITTED）

| 能力 | 契约包 | 状态 | 期限 | 实现落点/整改要点 |
|:---|:---|:---:|:---|:---|
| 服务注册发现 | coord.registry.v1 | COMMITTED | 2026-10-31 | coord-agent/src/services/registry.rs：迁移至契约包 + 错误码对齐 + 重复注册幂等验证 |
| 分布式锁 | coord.lock.v1 | COMMITTED | 2026-11-30 | coord-agent/src/services/lock.rs：迁移至契约包 + 非持有者释放 PERMISSION_DENIED 对齐 |
| Leader 选举 | coord.election.v1 | COMMITTED | 2026-11-30 | coord-agent/src/services/leader_election.rs：迁移至契约包 + 续约（重新 Campaign）语义验证 |
| 分布式 ID | coord.idgen.v1 | COMMITTED | 2026-10-31 | coord-agent/src/services/idgen.rs：迁移至契约包 + 时钟回拨防护落地或明确不承诺边界 |
| 事件通知 | coord.event.v1 | COMMITTED | 2026-12-31 | coord-agent/src/services/event_notification.rs：迁移至契约包 + Subscribe 显式下发 subscription_id |

## 底座原语（STABLE）

| 能力 | 契约包 | 状态 | 期限 | 实现落点/整改要点 |
|:---|:---|:---:|:---|:---|
| KV | coord.kv | STABLE | - | coord/src/main.rs:2693；wire-sync 硬卡口 |
| 事务（CAS） | coord.txn | STABLE | - | coord/src/main.rs:2694 |
| 租约 | coord.lease | STABLE | - | coord/src/main.rs:2695 |
| 变更监听 | coord.watch | STABLE | - | coord/src/main.rs:2696 |
| 探活 | coord.maintenance | STABLE | - | 仅 Status（裁剪承诺） |
| 健康检查 | grpc.health.v1 | STABLE | - | 标准协议 |

## 承诺修复区（EXPERIMENTAL）

| 能力 | 契约包 | 状态 | 期限 | 实现落点/整改要点 |
|:---|:---|:---:|:---|:---|
| 缓存 | coord.experimental.cache.v1 | EXPERIMENTAL | 2026-12-31 | ISR 原子提交 + 分区 Leader 故障转移 |
| 消息队列 | coord.experimental.mq.v1 | EXPERIMENTAL | 2026-12-31 | 背压不丢消息 + 单 Agent at-least-once（poll+ack）文档化 |
| 工作流（Saga） | coord.experimental.workflow.v1 | EXPERIMENTAL | 2027-03-31 | 持久化 + 补偿语义落地后方可对外 |
| 调度 | coord.experimental.scheduler.v1 | EXPERIMENTAL | 2027-03-31 | KV 真实现（替代内存 HashMap） |
