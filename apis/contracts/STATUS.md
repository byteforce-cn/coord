# 契约状态表（STATUS）

> 单一事实来源：契约状态、目标日期、实现落点。`scripts/check-wire-sync.sh` 解析本文件。
>
> **列结构是机器契约，勿改动**：`| 能力 | 契约包 | 状态 | 期限 | 实现落点/边界说明 |`
>
> 状态：`STABLE` 稳定承诺 ｜ `COMMITTED` 承诺中（期限 = 目标日期，到期未落地契约包即 CI 失败）｜
> `EXPERIMENTAL` 实验期承诺（期限 = 转正评估，不得以稳定面消费）｜
> `PROPOSED` 已冻结待落地（wire 已冻结但**不构成承诺面**，期限列写 `-`；不要求 SDK impl，
> 翻成 `COMMITTED` 后回到期限/SDK 双向强制）。PROPOSED 不是 EXPERIMENTAL：前者是
> “对外形状定了、实现与 GA 未到”，后者是“形状未定、只做试验”。

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
| 安全传输 / KMS 密钥面 | coord.transit.v1 | COMMITTED | 2026-12-31 | coord-agent/src/services/transit_key.rs + transit.rs：**ADR-0019 KMS 面已落地**——命名密钥档案 + 版本三边界（latest/min_decryption/min_encryption）与 CAS 生命周期（Create/Read/List/Rotate/UpdateConfig/Delete/Trim/Import/Export）；CCT3 按名信封加密 + Rewrap（密文进/密文出）+ GenerateDataKey + batch；非对称 Sign/Verify（Ed25519/ECDSA P-256/384/521/RSA PKCS#1 v1.5+PSS，prehashed fail-loud，openssl 独立验签）；HMAC 用途化 + key_id 窗口（ADR-0012）保持。KEK 多材料窗口按版本材料的 `kek_id` 生效。**边界**：软件 KEK（非 HSM）；无 convergent/hash/random/离线缓存；导出需 `coord:transit:key_export` 且 CA key 恒不可导出；KMS 写操作 actor 审计为 v2 backlog |
| PKI 证书签发 | coord.pki.v1 | COMMITTED | 2026-12-31 | coord-agent/src/pki.rs：已迁移至契约包（coord-server KV + Barrier 加密）；**ADR-0019 D6 私钥留库已落地**——CA/leaf key 在 transit 命名密钥档案（KEK 包裹），Issue/Renew/Rotate/GetCertByCN 只返回 `key_material` 引用、不再返回 `key_pem`；`GetCertBySerial` 按 serial 只读查询（含 retired/expired，未知 ⇒ NOT_FOUND，不含材料），状态域 `active|retired|expired`（expired 服务端按 not_after 判定）；**`SignCsr` 已落地**——只签 CSR 公钥（PoP 必检，私钥不入 coord，`key_material=external`），`usage` 精确写 KU/EKU（SERVER/CLIENT/BOTH），外部证书的 Renew/Rotate 显式 `FAILED_PRECONDITION`；leaf key `pki/leaf/{cn}` 版本随轮换递增，CA key `pki/ca` never_exportable；远程签名走 `Transit/Sign`（签名可被证书公钥验证）。**到期 = 换新触发（Issue/Rotate 对过期记录版本化 CAS 替换，G-PKI-1）**、**SAN 签发与「参数一致才幂等」**、到期观测（G-PKI-2）保持；**吊销面（ADR-0020）已落地**——`RevokeCert` / `GetCertStatus` / `GetCrl` / `RotateCrl` / `Tidy`，吊销账本 `/_pki/v1/revoked/{cert_serial}` 与 CA 同存储（跨实例/重启、永久保留，重复吊销幂等），`VerifyCert` 查账本，`IssueCert` / `SignCsr` / `RenewCert` 对已吊销身份返回 `FAILED_PRECONDITION`（`RotateCert` 是显式换新身份路径），CRL 编号持久化在 `/_pki/v1/crl`（内容变化或显式轮换才递增，RPC 分发、无 HTTP 端点），`Tidy` 按 `not_after + retention` 回收历史但保留吊销记录；记录 serial = 证书内嵌 X.509 serial；**serial 入参按 X.509 INTEGER 数值口径匹配**（`GetCertBySerial`/`RevokeCert`/`GetCertStatus`/`RenewCert` 忽略大小写与前导零，回显仍定长 32 hex，非 hex ⇒ INVALID_ARGUMENT）；能力 `pki:cert:revoke`（Revoke/RotateCrl/Tidy） |
| 缓存 | coord.cache.v1 | COMMITTED | 2026-12-31 | coord-agent/src/services/cache.rs：已迁移至契约包；**跨节点提交原子性 + 分区 Leader 故障转移为验收项**；容量上界（B-PL-3）已强制：默认 1GB、周期 reaper 收敛（不含 ISR 复制日志，边界见 boundaries.md）；**SetIfAbsent（原子条件写）/ Keys（有界枚举）/ Clear（逻辑键前缀清理，ISR 经复制通道全域下发）已落地**（多键空间边界见 B-PL-5；并发判据：N 并发 SetNX 恰一成功） |
| 消息队列 | coord.mq.v1 | COMMITTED | 2026-12-31 | coord-agent/src/services/mq.rs：已迁移至契约包；poll+ack 全链路与幂等键为验收项；Subscribe 背压丢消息语义见 proto 声明；容量上界（B-PL-4）已在 publish 入口强制：默认 1GB、同事务记账 ⇒ 逐写严格上界（超界拒绝 `RESOURCE_EXHAUSTED`）+ 保留窗口周期回收（不含 ISR 复制日志，边界见 boundaries.md）；**Leader 查询/结构化 not-leader（G-MQ-1）、显式 DLQ 管理（G-MQ-2）、消费组 lag/offset 指标（G-MQ-3）、topic 删除与全量回收（G-MQ-4）已落地**；**DLQ 归因（`group` / `failure_type` / `attempts`）与按组过滤已落地**（分区级、多组共享 ⇒ 归因随记录落盘，`PollDlq.group` 服务端过滤） |
| 特性开关 | coord.featureflags.v1 | COMMITTED | 2026-12-31 | coord-agent/src/services/feature_flags.rs：已迁移至契约包；**KV 化（重启不丢）已落地**；**管理面（PutFlag / DeleteFlag / ListFlags）已落地**——读路径直读存储无本地缓存 ⇒ 写入立即生效（跨 agent 一致） |
| 工作流（Saga） | coord.workflow.v1 | COMMITTED | 2027-03-31 | coord-agent/src/services/workflow_store.rs（KvWorkflowStore）：已迁移至契约包；补偿语义端到端已落地（sw 编译器 `compensatedBy`；三步**逆序**补偿 e2e `test_sw_three_step_failure_compensates_in_reverse_order_and_replays`——该用例暴露并修掉两处静默丢副作用缺陷：try 体内层索引未归零、包装器改名导致 goto 失配）；**保留策略（G-WF-1）已落地**：`DeleteInstance`/`DeleteDefinition`（终态/引用守卫；KV 删除 + watch 缓存失效 + 对账剪枝）；**人工审批 v2（ADR-0013 / ADR-0014，含 D13 assignment 状态机）已落地**：原生 `approve` 节点 DSL（席位来源支持 `${ ... }` 运行时表达式；`timeoutPolicy: none|fail|waiting`）+ 19 个审批 RPC（命令可带 `on_behalf_of`、查询可按 `assignee`，审计区分 `actor`/`acting_service`）（14 命令 `coord:workflow:execute`：Approve/Reject/Acknowledge/AddSign/RemoveSign/ReturnToNode/Transfer/Delegate/Withdraw/Terminate/Urge/Cc/WithdrawTask/ResumeWorkflow；5 查询 `coord:workflow:read`：ListPendingTasks/GetInstanceDetail/ListTasks/GetTask/History）、挂起/恢复（`expected_signal="approval.completed"`）、加签位置（BEFORE/AFTER）与 assignment 读面、按 assignee 减签、委派到期、任务/轮次/assignment 读面时间戳、实例撤回 `targetStep`（`INSTANCE_ROLLED_BACK`）、`ListInstances.status_in` 多值过滤、无审批聚合实例的 `Terminate` 回退、命令幂等（`/_workflow/v4/idempotency/{instance}/{key}` 同 Txn）、事件 `.v1` + JSON Schema（`apis/contracts/schemas/events/`）；语义见 docs/production/workflow-semantics.md §7 |
| 调度 | coord.scheduler.v1 | COMMITTED | 2027-03-31 | coord-agent/src/services/scheduler.rs：已迁移至契约包；KV 存储（KvSchedulerStore + CAS 认领，重启不丢/跨 Agent 唯一）已落地；**complete 的 result 随任务记录持久化（G-SC-1） + `GetJob` 结果回读（只读/幂等；重启/换 agent 可读）已落地** |

## 底座原语（STABLE）

| 能力 | 契约包 | 状态 | 期限 | 实现落点/边界说明 |
|:---|:---|:---:|:---|:---|
| 探活 | coord.maintenance | STABLE | - | 仅 Status（裁剪承诺） |
| 健康检查 | grpc.health.v1 | STABLE | - | 标准协议 |

> **v1 数据面已移出承诺面（ADR-0018 D3）**：`coord.kv` / `coord.txn` / `coord.lease` / `coord.watch` / `coord.storage`(v1) 不再列为承诺项——既有 v1 集群继续按其冻结语义运行（B-KVT-2），但不再演进、不再要求 SDK 覆盖；新装与服务面一律用 `coord.*.v2`。wire 级删除需要单独 ADR（涉及既有 v1 集群与 Jepsen v1 夹具的退役）。

## KV-T v2（ADR-0015、ADR-0017；GA 2026-11-11）

`[kv_t].enabled` 三态（ADR-0017）：不写 = 多 Region + PD 集群自动启用 v2，单 Raft
集群仍是 v1；显式 `false` 时 v2 服务仍注册（reflection 可见）但每个 RPC 返回
`FAILED_PRECONDITION`。v1 行为、磁盘布局与服务注册在 v1 下不变。G0–G6 已全部交付
（ADR-0015 D10）；v2 集群不服务 v1 数据面、不做 v1→v2 迁移（ADR-0015 D13）。

| 能力 | 契约包 | 状态 | 期限 | 实现落点/边界说明 |
|:---|:---|:---:|:---|:---|
| 全局时间戳（TSO） | coord.tso.v2 | COMMITTED | 2027-12-31 | 契约包：`proto/coord/tso/v2/`；G1（TSO 状态机/客户端）；能力 `data:tso:allocate`；批只保证唯一/单调，允许空洞、绝不复用 |
| KV v2 | coord.kv.v2 | COMMITTED | 2027-12-31 | 契约包：`proto/coord/kv/v2/`；G1（单 Region 读写）/G2（跨区 Range）；`commit_ts`/`read_ts` 语义见 proto 头注；`CommitRejected`/`SAFE_TS_LAG` |
| Txn v2 | coord.txn.v2 | COMMITTED | 2027-12-31 | 契约包：`proto/coord/txn/v2/`；G3（OCC + Commit Sequencer）；服务端读集会话，冲突 `TXN_CONFLICT`；上限 4MiB/4096/10000 |
| Watch v2 | coord.watch.v2 | COMMITTED | 2027-12-31 | 契约包：`proto/coord/watch/v2/`；G4（Commit Log/Coordinator/RPC）；按 `commit_ts` 全局有序、事务原子批、窗口不足 `HISTORY_UNAVAILABLE`；`Watch` 双向流（Create/Cancel/ack/cursor 续订）与 `coord-client` 的 `watch_v2()` 断线重连已落地，订阅指标见 consumer-contract |
| Lease v2 | coord.lease.v2 | COMMITTED | 2027-12-31 | 契约包：`proto/coord/lease/v2/`；G2（region 0 租约表 + 跨区 revoke 最终一致清理）；`LeaseTimeToLive` 能力 `data:lease:read` |
| Storage v2 | coord.storage.v2 | COMMITTED | 2027-12-31 | 契约包：`proto/coord/storage/v2/`；G5（manifest commit_ts + `/obj/` 固定区间）；`/obj/` 不参与任意 KV split；Put/Get/Stat/Delete 已在 v2 集群可用（`read_ts` 快照读 manifest，chunk 字节不版本化 ⇒ 历史 chunk 不可用时 `OUT_OF_RANGE`）；对象存储的其余承诺面事实（chunk DEK 加密 + key_id 轮换、流式 Put/Get 绕 4MiB、未知长度上传 `PutMeta.total_size = -1`、配额/磁盘水位/GC、快照恢复落后节点 chunk rebuild ⇒ Get UNAVAILABLE、全集群配置一致、对象前缀 /obj/）由 v1 实现承载，v2 面沿用同一语义（docs/production/volume-object-storage.md） |

## MQ v2（COMMITTED；ADR-0016；GA 2026-11-14）

MQ v2 是 G7 的 greenfield 面：只服务 v2 集群，不双跑 v1、不迁移 v1 数据（ADR-0015 D13 /
ADR-0016 D1）。契约在 M0 冻结（wire 不再变），M0–M7 已交付并于 2026-11-14 签字 GA；
GA gate 证据、未关闭项与 post-GA backlog 见
`docs/production/ops/mq-v2-gate.md`。

| 能力 | 契约包 | 状态 | 期限 | 实现落点/边界说明 |
|:---|:---|:---:|:---|:---|
| 消息队列 v2（服务端分区日志/消费组） | coord.mq.v2 | COMMITTED | 2027-12-31 | 契约包：`proto/coord/mq/v2/`；M0 冻结（19 个 RPC + 11 个结构化错误码），GA 未改 wire；`[mq_v2].enabled=false` 时服务注册但一律 `FAILED_PRECONDITION`；分区内有序 + 至少一次（`Poll`+`Ack`），`Subscribe` 仅 best-effort；无 exactly-once / Kafka 兼容 / 自动分区再平衡。Java SDK：`client.mqV2()` 18/19 RPC + `MqV2Consumer`（`Subscribe` 无客户端）；GA 证据见 `docs/production/ops/mq-v2-gate.md` |

## 实验承诺区（EXPERIMENTAL）

**本区已于 `contracts/v1.2.0`（2026-09-19）清空。**

原 4 个 `coord.experimental.*` 包（cache / mq / workflow / scheduler）**从未有 proto 文件、
从未有任何消费者**，故直接建为稳定包 `coord.<domain>.v1` 并转入上方 COMMITTED 段，
**不以实验包形态开放任何能力**（不构成 Breaking）。

| 能力 | 契约包 | 状态 | 期限 | 实现落点/边界说明 |
|:---|:---|:---:|:---|:---|

