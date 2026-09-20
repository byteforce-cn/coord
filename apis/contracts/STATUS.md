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
| 服务注册发现 | coord.registry.v1 | COMMITTED | 2026-10-31 | coord-agent/src/services/registry.rs：已迁移至契约包；重复注册幂等验证 |
| 分布式锁 | coord.lock.v1 | COMMITTED | 2026-11-30 | coord-agent/src/services/lock.rs：已迁移至契约包；**F-28 fencing（release 校验 lease_id）须闭环** |
| Leader 选举 | coord.election.v1 | COMMITTED | 2026-11-30 | coord-agent/src/services/leader_election.rs：已迁移至契约包；续约（重新 Campaign）语义验证 |
| 分布式 ID | coord.idgen.v1 | COMMITTED | 2026-10-31 | coord-agent/src/services/idgen.rs：已迁移至契约包；时钟回拨防护落地或明确不承诺边界 |
| 事件通知 | coord.event.v1 | COMMITTED | 2026-12-31 | coord-agent/src/services/event_notification.rs：已迁移至契约包；Subscribe 显式下发 subscription_id |
| 配置中心 | coord.config.v1 | COMMITTED | 2026-12-31 | coord-agent/src/services/config_center.rs：迁至契约包（KV 驱动 + 本地 cache + watch） |
| 权限策略引擎 | coord.policy.v1 | COMMITTED | 2026-12-31 | coord-agent/src/services/policy.rs：迁至契约包；**边界声明：OPA bundle 在 server KV，RBAC 策略为 Agent 本地** |
| 熔断器 | coord.circuitbreaker.v1 | COMMITTED | 2026-12-31 | coord-agent/src/services/circuit_breaker.rs：迁至契约包；**边界声明：本地内存，不跨 Agent 共享** |
| 限流器 | coord.ratelimiter.v1 | COMMITTED | 2026-12-31 | coord-agent/src/services/rate_limiter.rs：迁至契约包；**边界声明：本地令牌桶，不跨 Agent 共享** |
| 安全传输（信封加密） | coord.transit.v1 | COMMITTED | 2026-12-31 | coord-agent/src/services/transit.rs：迁至契约包；**DEK 持久化整改（P0-5）** |
| PKI 证书签发 | coord.pki.v1 | COMMITTED | 2026-12-31 | coord-agent/src/pki.rs：迁至契约包（coord-server KV + Barrier 加密） |
| 缓存 | coord.cache.v1 | COMMITTED | 2026-12-31 | coord-agent/src/services/cache.rs：迁至契约包；**跨节点提交原子性（P0-8）+ 分区 Leader 故障转移** |
| 消息队列 | coord.mq.v1 | COMMITTED | 2026-12-31 | coord-agent/src/services/mq.rs：迁至契约包；**poll+ack 全链路（F-67 59/59 失败）+ 幂等键（F-57）+ 背压丢消息语义文档化** |
| 特性开关 | coord.featureflags.v1 | COMMITTED | 2026-12-31 | coord-agent/src/services/feature_flags.rs：迁至契约包；**KV 化整改（重启不丢）** |
| 工作流（Saga） | coord.workflow.v1 | COMMITTED | 2027-03-31 | coord-agent/src/services/workflow_store.rs（KvWorkflowStore）：迁至契约包；持久化 + 补偿语义端到端验收 |
| 调度 | coord.scheduler.v1 | COMMITTED | 2027-03-31 | coord-agent/src/services/scheduler.rs：迁至契约包；**KV 真实现（替代内存 HashMap，P0-10）** |
| 对象存储（Server） | coord.storage | COMMITTED | 2026-12-31 | 数据面闭环 v1（docs/production/volume-object-storage.md）：manifest raft 强一致 + chunk 文件旁路 MVCC/快照 + 流式 Put/Get 绕 4MiB + 配额/磁盘水位/GC；2026-09-06 批次收口：chunk 加密 DEK 化（HKDF KEK 包裹随机 DEK + key_id 版本 + encryption_rotation_days 轮换，v1 兼容读）+ PD Region 心跳「存储字节」维度/split 阈值纳入/存储重 Region 均衡门控 + Rust/Java SDK 对象方法 + Agent 存储代理 + 进程级 e2e 与 kill/pause/partition 混沌矩阵；2026-09-12 增量：流式上传支持**未知长度**（`PutMeta.total_size = -1`，以 max_object_size 封顶、Commit 时定长；`>0` 声明长度语义不变）；已知边界：快照恢复落后节点本地 chunk 清空 rebuild（Get UNAVAILABLE）、全集群配置须一致、创建对象在保留前缀 /obj/ |

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

**本区已于 `contracts/v1.2.0`（2026-09-19）清空。**

原 4 个 `coord.experimental.*` 包（cache / mq / workflow / scheduler）**从未有 proto 文件、
从未有任何消费者**，故直接建为稳定包 `coord.<domain>.v1` 并转入上方 COMMITTED 段，
**不以实验包形态开放任何能力**（不构成 Breaking，亦不违反 WHITEPAPER §9.2 实验包规则）。

| 能力 | 契约包 | 状态 | 期限 | 实现落点/整改要点 |
|:---|:---|:---:|:---|:---|

