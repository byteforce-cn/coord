# 契约状态表（STATUS）

> 单一事实来源：契约状态、目标日期、实现落点。`scripts/check-wire-sync.sh` 解析本文件。
>
> **列结构是机器契约，勿改动**：`| 能力 | 契约包 | 状态 | 期限 | 实现落点/边界说明 |`
>
> 状态：`STABLE` 稳定承诺 ｜ `COMMITTED` 承诺中（期限 = 目标日期，到期未落地契约包即 CI 失败）｜
> `EXPERIMENTAL` 实验期承诺（期限 = 转正评估，不得以稳定面消费）

## 能力承诺面（COMMITTED）

| 能力 | 契约包 | 状态 | 期限 | 实现落点/边界说明 |
|:---|:---|:---:|:---|:---|
| 服务注册发现 | coord.registry.v1 | COMMITTED | 2026-10-31 | coord-agent/src/services/registry.rs：已迁移至契约包；重复注册幂等验证 |
| 分布式锁 | coord.lock.v1 | COMMITTED | 2026-11-30 | coord-agent/src/services/lock.rs：已迁移至契约包；release 校验 `lease_id`（fencing）为验收项 |
| Leader 选举 | coord.election.v1 | COMMITTED | 2026-11-30 | coord-agent/src/services/leader_election.rs：已迁移至契约包；续约（重新 Campaign）语义验证 |
| 分布式 ID | coord.idgen.v1 | COMMITTED | 2026-10-31 | coord-agent/src/services/idgen.rs：已迁移至契约包；时钟回拨防护（或显式声明不承诺边界）为验收项 |
| 事件通知 | coord.event.v1 | COMMITTED | 2026-12-31 | coord-agent/src/services/event_notification.rs：已迁移至契约包；Subscribe 显式下发 subscription_id 为验收项；**持久化游标矩阵与保留窗口内补投（G-EV-1）已落地**（默认实时不补投；显式 cursor 位点补投；seq 全局单调） |
| 配置中心 | coord.config.v1 | COMMITTED | 2026-12-31 | coord-agent/src/services/config_center.rs：已迁移至契约包（KV 驱动 + 本地 cache + watch）；**位点化读路径已落地**：List 返回快照 revision、Watch 按位点回放/续订（含删除事件，presence 语义）、Put 透出全局位点——「快照 + watchFrom」resync 纪律见 proto 头注；e2e 判据 `coord/tests/config_resync_test.rs` |
| 权限策略引擎 | coord.policy.v1 | COMMITTED | 2026-12-31 | coord-agent/src/services/policy.rs：已迁移至契约包；**边界声明：OPA bundle 在 server KV，RBAC 策略为 Agent 本地（CheckPermission 仅本地/嵌入式用途）**；bundle 分发已落地：启动全量加载 + Watch 收敛（≤10s 量级）+ per-key CAS 版本单调（G-POL-1） |
| 熔断器 | coord.circuitbreaker.v1 | COMMITTED | 2026-12-31 | coord-agent/src/services/circuit_breaker.rs：已迁移至契约包；**边界声明：本地内存，不跨 Agent 共享** |
| 限流器 | coord.ratelimiter.v1 | COMMITTED | 2026-12-31 | coord-agent/src/services/rate_limiter.rs：已迁移至契约包；**边界声明：本地令牌桶，不跨 Agent 共享** |
| 安全传输（信封加密） | coord.transit.v1 | COMMITTED | 2026-12-31 | coord-agent/src/services/transit.rs：已迁移至契约包；DEK 持久化（KV + 用后即焚）已落地；**KEK 多材料解密窗口 + `Rewrap` 材料迁移已落地（G-TR-1）**；**HMAC 用途化密钥 + key_id 轮换窗口（G-TR-3）已落地**：purpose 必填、互不可验；签名携带材料标识，历史材料窗口内可复算、下线后 FAILED_PRECONDITION（ADR-0012） |
| PKI 证书签发 | coord.pki.v1 | COMMITTED | 2026-12-31 | coord-agent/src/pki.rs：已迁移至契约包（coord-server KV + Barrier 加密）；**到期 = 换新触发（Issue/Rotate 对过期记录版本化 CAS 替换，G-PKI-1）已落地**；**SAN 签发（CN 派生默认开）与「参数一致才幂等」已落地**；到期观测指标（G-PKI-2）已落地 |
| 缓存 | coord.cache.v1 | COMMITTED | 2026-12-31 | coord-agent/src/services/cache.rs：已迁移至契约包；**跨节点提交原子性 + 分区 Leader 故障转移为验收项**；容量上界（B-PL-3）已强制：默认 1GB、周期 reaper 收敛（不含 ISR 复制日志，边界见 boundaries.md）；**SetIfAbsent（原子条件写）/ Keys（有界枚举）/ Clear（逻辑键前缀清理，ISR 经复制通道全域下发）已落地**（多键空间边界见 B-PL-5；并发判据：N 并发 SetNX 恰一成功） |
| 消息队列 | coord.mq.v1 | COMMITTED | 2026-12-31 | coord-agent/src/services/mq.rs：已迁移至契约包；poll+ack 全链路与幂等键为验收项；Subscribe 背压丢消息语义见 proto 声明；容量上界（B-PL-4）已在 publish 入口强制：默认 1GB、同事务记账 ⇒ 逐写严格上界（超界拒绝 `RESOURCE_EXHAUSTED`）+ 保留窗口周期回收（不含 ISR 复制日志，边界见 boundaries.md）；**Leader 查询/结构化 not-leader（G-MQ-1）、显式 DLQ 管理（G-MQ-2）、消费组 lag/offset 指标（G-MQ-3）、topic 删除与全量回收（G-MQ-4）已落地** |
| 特性开关 | coord.featureflags.v1 | COMMITTED | 2026-12-31 | coord-agent/src/services/feature_flags.rs：已迁移至契约包；**KV 化（重启不丢）已落地**；**管理面（PutFlag / DeleteFlag / ListFlags）已落地**——读路径直读存储无本地缓存 ⇒ 写入立即生效（跨 agent 一致） |
| 工作流（Saga） | coord.workflow.v1 | COMMITTED | 2027-03-31 | coord-agent/src/services/workflow_store.rs（KvWorkflowStore）：已迁移至契约包；补偿语义端到端已落地（sw 编译器 `compensatedBy`；三步**逆序**补偿 e2e `test_sw_three_step_failure_compensates_in_reverse_order_and_replays`——该用例暴露并修掉两处静默丢副作用缺陷：try 体内层索引未归零、包装器改名导致 goto 失配）；**保留策略（G-WF-1）已落地**：`DeleteInstance`/`DeleteDefinition`（终态/引用守卫；KV 删除 + watch 缓存失效 + 对账剪枝）；**人工审批 v2（ADR-0013 / ADR-0014）已落地**：原生 `approve` 节点 DSL（席位来源支持 `${ ... }` 运行时表达式）+ 12 个审批 RPC（命令可带 `on_behalf_of`、查询可按 `assignee`，审计区分 `actor`/`acting_service`）（10 命令 `coord:workflow:execute`：Approve/Reject/Acknowledge/AddSign/RemoveSign/ReturnToNode/Transfer/Delegate/Withdraw/Terminate；2 查询 `coord:workflow:read`：ListPendingTasks/GetInstanceDetail）、挂起/恢复（`expected_signal="approval.completed"`）、引擎级授权矩阵、命令幂等（`/_workflow/v4/idempotency/{instance}/{key}` 同 Txn）、事件 `.v1` + JSON Schema（`apis/contracts/schemas/events/`）；语义见 docs/production/workflow-semantics.md §7 |
| 调度 | coord.scheduler.v1 | COMMITTED | 2027-03-31 | coord-agent/src/services/scheduler.rs：已迁移至契约包；KV 存储（KvSchedulerStore + CAS 认领，重启不丢/跨 Agent 唯一）已落地；**complete 的 result 随任务记录持久化（G-SC-1） + `GetJob` 结果回读（只读/幂等；重启/换 agent 可读）已落地** |
| 对象存储（Server） | coord.storage | COMMITTED | 2026-12-31 | 数据面闭环 v1（docs/production/volume-object-storage.md）：manifest raft 强一致 + chunk 文件旁路 MVCC/快照 + 流式 Put/Get 绕 4MiB + 配额/磁盘水位/GC；chunk 加密 DEK 化（HKDF KEK 包裹随机 DEK + key_id 版本 + encryption_rotation_days 轮换，v1 兼容读）+ PD Region 心跳「存储字节」维度/split 阈值纳入/存储 Region 均衡 + Rust/Java SDK 对象方法 + Agent 存储代理 + 进程级 e2e 与 kill/pause/partition 混沌矩阵；流式上传支持**未知长度**（`PutMeta.total_size = -1`，以 max_object_size 封顶、Commit 时定长；`>0` 声明长度语义不变）；已知边界：快照恢复落后节点本地 chunk 清空 rebuild（Get UNAVAILABLE）、全集群配置须一致、创建对象在保留前缀 /obj/ |

## 底座原语（STABLE）

| 能力 | 契约包 | 状态 | 期限 | 实现落点/边界说明 |
|:---|:---|:---:|:---|:---|
| KV | coord.kv | STABLE | - | coord/src/main.rs:2693；由 wire-sync 校验 |
| 事务（CAS） | coord.txn | STABLE | - | coord/src/main.rs:2694 |
| 租约 | coord.lease | STABLE | - | coord/src/main.rs:2695 |
| 变更监听 | coord.watch | STABLE | - | coord/src/main.rs:2696 |
| 探活 | coord.maintenance | STABLE | - | 仅 Status（裁剪承诺） |
| 健康检查 | grpc.health.v1 | STABLE | - | 标准协议 |

## KV-T v2（ADR-0015；默认关闭）

`[kv_t].enabled=false`（默认）时 v2 服务仍注册（reflection 可见），但每个 RPC 返回
`FAILED_PRECONDITION`；v1 行为、磁盘布局与服务注册不变。契约包已随 G0 落地；
实现按 G1–G6 gate 分批交付（ADR-0015 D10），未交付能力同样 `FAILED_PRECONDITION`。
v2 集群不服务 v1 数据面、不做 v1→v2 迁移（ADR-0015 D13）。

| 能力 | 契约包 | 状态 | 期限 | 实现落点/边界说明 |
|:---|:---|:---:|:---|:---|
| 全局时间戳（TSO） | coord.tso.v2 | COMMITTED | 2027-12-31 | 契约包：`proto/coord/tso/v2/`；G1（TSO 状态机/客户端）；能力 `data:tso:allocate`；批只保证唯一/单调，允许空洞、绝不复用 |
| KV v2 | coord.kv.v2 | COMMITTED | 2027-12-31 | 契约包：`proto/coord/kv/v2/`；G1（单 Region 读写）/G2（跨区 Range）；`commit_ts`/`read_ts` 语义见 proto 头注；`CommitRejected`/`SAFE_TS_LAG` |
| Txn v2 | coord.txn.v2 | COMMITTED | 2027-12-31 | 契约包：`proto/coord/txn/v2/`；G3（OCC + Commit Sequencer）；服务端读集会话，冲突 `TXN_CONFLICT`；上限 4MiB/4096/10000 |
| Watch v2 | coord.watch.v2 | COMMITTED | 2027-12-31 | 契约包：`proto/coord/watch/v2/`；G4（Commit Log/Coordinator）；按 `commit_ts` 全局有序、事务原子批、窗口不足 `HISTORY_UNAVAILABLE` |
| Lease v2 | coord.lease.v2 | COMMITTED | 2027-12-31 | 契约包：`proto/coord/lease/v2/`；G2（跨区 revoke/region 0 租约表）；`LeaseTimeToLive` 能力 `data:lease:read` |
| Storage v2 | coord.storage.v2 | COMMITTED | 2027-12-31 | 契约包：`proto/coord/storage/v2/`；G5（manifest commit_ts + `/obj/` 固定区间）；`/obj/` 不参与任意 KV split |

## 实验承诺区（EXPERIMENTAL）

**本区已于 `contracts/v1.2.0`（2026-09-19）清空。**

原 4 个 `coord.experimental.*` 包（cache / mq / workflow / scheduler）**从未有 proto 文件、
从未有任何消费者**，故直接建为稳定包 `coord.<domain>.v1` 并转入上方 COMMITTED 段，
**不以实验包形态开放任何能力**（不构成 Breaking）。

| 能力 | 契约包 | 状态 | 期限 | 实现落点/边界说明 |
|:---|:---|:---:|:---|:---|

