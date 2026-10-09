# 消费方契约页（Consumer Contract）

> Owner: maintainers ｜ Last verified: 2026-10-09

面向**接入方**的聚合页：按服务给出「一致性 / 路由 / 失败语义 + 推荐消费范式」，
并把每 RPC 的能力名与已接受的边界汇总为可检索的表。

**单一归属（禁止第三份口径）**：事实源是 —— proto 声明
（`apis/contracts/proto/`，与 `coord-proto/src/proto/` 字节一致）、边界清单
（`ops/boundaries.md`）、能力状态（`apis/contracts/STATUS.md`）、能力表
（`coord-core/src/grpc_auth.rs::rpc_capability`）。本页只做**聚合与范式指引**；
若本页与上述文件冲突，以它们为准。**本页过期即缺陷。**

---

## 0. 通用约定

### 0.1 鉴权与能力（capability）

- agent 面**所有** RPC 在开启鉴权时按能力名 fail-closed：未登记的 RPC 直接拒绝
  （未知 RPC ⇒ 403），与鉴权开关无关的例外见下。
- 例外（自身无能力要求，服务内自证/一次性令牌）：`Auth/Authenticate`、
  `Auth/RefreshToken`、`Auth/Bootstrap`、`Auth/GetRevocationDelta`（仅需合法
  CCT）。
- 能力名唯一维护在 `coord-core/src/grpc_auth.rs::rpc_capability`；本页 §1–§11
  列出的是**消费方视角的对照**，变更以该文件为准。
- 客户端侧：能力由服务端 CCT 决定，SDK 不本地判断；`PERMISSION_DENIED`
  表示 CCT 无对应能力（不是重试目标）。

### 0.2 错误契约（结构化判别，禁止解析文案）

- 服务端在 gRPC **trailer** 下发 `x-coord-error-code`（如 `NOT_LEADER` /
  `UNAVAILABLE` / `NOT_FOUND` / `FAILED_PRECONDITION` /
  `RESOURCE_EXHAUSTED`）。Rust 侧 `ErrorCode`、Java 侧
  `ErrorMapper`/`ErrorCode` 读取同一 trailer。
- 数据面路由错误（非 Leader 写路径）额外携带 `coord-leader-hint` trailer
  （Leader 地址）。Java SDK：`CoordException#getLeaderHint()`。
- 重试纪律（与两语言 SDK 的重试矩阵一致）：
  | 错误码 | 处理 |
  |:--|:--|
  | `UNAVAILABLE` / `DEADLINE_EXCEEDED` | 可重试（幂等语义自负；SDK 有界重试） |
  | `NOT_LEADER`（带 hint） | **按 hint 重路由**到新地址，不得解析文案 |
  | `FAILED_PRECONDITION`（ISR 降级/状态不符） | 不重路由；退避重试或等待恢复 |
  | `RESOURCE_EXHAUSTED` | **不重试**（配额/背压信号）；按 §5/§6 容量口径处置 |
  | `PERMISSION_DENIED` | 不重试（能力缺失，走授权变更） |

### 0.3 读路径与 watch 纪律（所有 watch 类服务通用）

- watch/subscribe 是**收敛通知**：断线期间的变化不保证逐条补投（例外见 §4
  事件游标）。重连后**重读权威状态**（KV `Range` / `GetStatus` / `Discover`），
  不要试图从 watch 流重建全量。
- watch 的背压语义：服务端按订阅者窗口丢弃时可观测（见各服务指标）。

---

## 1. KV / Txn / Lease / Watch（Server 数据面）

| RPC（组） | 能力 |
|:--|:--|
| `KV/Range` | `data:kv:read` |
| `KV/Put`、`KV/Delete` | `data:kv:write` / `data:kv:delete` |
| `Txn/Txn` | `data:txn:execute` |
| `Lease/LeaseGrant` / `LeaseRevoke` / `LeaseKeepAlive` | `data:lease:grant` / `data:lease:revoke` / `data:lease:keepalive` |
| `Watch/Watch` | `data:watch:subscribe` |

- 一致性：写路径走 Raft leader（非 leader 返回 `NOT_LEADER` + hint，SDK 自动
  重定向）；读路径收敛口径见 `WHITEPAPER.md`。
- Watch 断线重连的 resync 纪律见 §0.3（WHITEPAPER「watch resync rules」）。
- Region 模式下的 watch 前缀限制与跨 region 拒绝见 `boundaries.md` B-RG-2。

## 2. Registry / Config（场景 ①）

| RPC（组） | 能力 |
|:--|:--|
| `Registry/Register` / `Deregister` / `Heartbeat` / `Discover` / `Watch` | `coord:registry:register` / `:deregister` / `:heartbeat` / `:discover` / `:watch` |
| `Config/Get` / `List` / `Watch` | `coord:config:read` / `:list` / `:watch` |
| `Config/Put` | `coord:config:write` |

- 注册发现：实例心跳过期由服务端判定下线；消费者侧以 `Discover` 结果为准，
  `Watch` 只作变更信号。
- 配置中心：读取「KV + 本地 cache + watch」；变更传播是收敛语义（§0.3）。

## 3. Lock / LeaderElection / IdGen（场景 ②）

| RPC（组） | 能力 |
|:--|:--|
| `Lock/Acquire` / `Release` / `Renew` / `GetLockInfo` | `coord:lock:acquire` / `:release` / `:renew` / `:info` |
| `LeaderElection/Campaign` / `Resign` / `GetLeader` / `Watch` | `coord:election:campaign` / `:resign` / `:read` / `:watch` |
| `IdGen/NextId` / `NextBatch` | `coord:idgen:next` |

- 锁：租约化——持有期内必须 `Renew`；租约过期即释放（下游必须以 fencing 或
  幂等吸收「过期持有者仍以为自己持锁」）。
- 选举：`Watch` 推送领导者变更；`GetLeader` 为快照查询。
- IdGen：全局单调 ID；批量为吞吐优化。

## 4. Event（事件通知，G-EV-1）

| RPC | 能力 |
|:--|:--|
| `Event/Publish` | `coord:event:publish` |
| `Event/Subscribe` | `coord:event:subscribe` |
| `Event/Unsubscribe` | `coord:event:unsubscribe` |

**投递语义（契约逐字见 `event.proto` 头注）**：

- **默认实时**：`Subscribe` 不携带 `cursor` 时，断线窗口**不补投**；
- **位点补投**：携带 `cursor`（上次收到的 `seq`，十进制字符串）时，服务端在
  **保留窗口**（= Server KV 中仍保留的事件）内按 `seq` 升序补投
  `seq > cursor` 的事件，再转入实时；**重放与实时之间不丢，但可能重复**
  （保持消费幂等）；
- `seq` 全局单调（跨 topic 无关事件类型），随每条 `CloudEventMessage.seq`
  下发；**持久化它**作为下次重连的游标；
- `Unsubscribe` 为兼容空操作（订阅 = gRPC 流本身；真正取消 = 关闭流）。
- 保留窗口 = 存储中仍存在的事件；早于窗口的缺口无法从 cursor 感知——把补投
  当 best-effort catch-up，配上权威状态重读。

**判据**：`coord/tests/event_cursor_replay_test.rs`（默认不补投 / cursor 补投
后无缝转实时 / 全量补投保序 / 类型过滤在补投路径生效）。

## 5. MQ（消息队列，G-MQ-1..4）

| RPC（组） | 能力 |
|:--|:--|
| `MQ/CreateTopic` / `PollDlq` / `MoveToDlq` / `DeleteTopic` | `coord:mq:manage` |
| `MQ/Publish` | `coord:mq:publish` |
| `MQ/Subscribe` | `coord:mq:subscribe` |
| `MQ/Poll` / `Ack` / `GetTopicLeader` | `coord:mq:consume` |

### 5.1 正确性路径（必须）

- **`Poll` + `Ack`** 是至少一次消费的**唯一**正确性路径：`Poll` 从
  `start_offset` 增量拉取；`Ack` 提交消费组位点；崩溃重启后从已提交位点续读。
- **`Subscribe` 仅实时场景**：服务端推送在订阅者背压时**丢弃**（best-effort，
  契约注释逐字见 `mq.proto`），不做「不丢」承诺——需要可靠性就用 `Poll+Ack`。
- 位点只在 `Ack` 成功后前进；**重复投递是 at-least-once 的正常形态**，消费必须
  幂等（同 `(topic, partition, offset)` 去重）。

### 5.2 路由（ISR 启用时）

- 分区 Leader 由确定性静态指派（`mq:{topic}` 分片的 min-addr）；**写路径
  （Publish/Ack）必须到达分区 Leader**。
- 查询：`GetTopicLeader`（任意 agent 可答；返回 leader 地址、ISR 成员、
  `replication_enabled` / `degraded` / `min_isr`）。
- 非 Leader 写：返回 `FAILED_PRECONDITION`（trailer `x-coord-error-code:
  NOT_LEADER` + `coord-leader-hint`）——**按 hint 重路由**；ISR 降级
  （`degraded=true`）时写被拒（`UNAVAILABLE` 语义），**退避等待 ISR 恢复**，不
  重路由。
- Java SDK：`MqClient#getTopicLeader` + `CoordException#getLeaderHint()`。

### 5.3 DLQ（毒消息闭环）

- 显式管理路径 `MoveToDlq`（净额记账、分区不被阻塞）；`reason` / `detail`
  随消息存入，`PollDlq` 可读（`MqMessage.dlqReason/dlqDetail`）。
- **无自动投毒判定**（按重试次数/策略的自动判定不在承诺内）——移入决定属于
  消费者。
- 毒消息处置范式：`Poll` → 处理失败 N 次 → `MoveToDlq`（带 reason）→ `Ack`
  原 offset → 继续消费；运维侧 `PollDlq` 复盘。

### 5.4 topic 生命周期（删除）

- `DeleteTopic`（管理路径）：删除配置并回收**全部**存量（消息 / DLQ / 消费
  位点 / 幂等索引），配额归还；ISR 下删除决定经复制通道**全域一致**。
- **前置条件：先停止该 topic 的所有读写**。并发读者/写者会看到明确错误
  （未知 topic / `NOT_FOUND`），没有静默部分状态。
- 同名重建 = 空 topic。回收计数随响应返回（`MqDeleteTopicResponse`）。
- 容量边界（逐写严格上界、记账口径）见 `boundaries.md` B-PL-4。

### 5.5 可观测（消费组 lag）

- `coord_agent_mq_consumer_lag{topic,group,partition}`（= next_offset −
  committed，下限 0）、`coord_agent_mq_consumer_offset`、
  `coord_agent_mq_next_offset`；告警 `CoordMqConsumerLag`（处置见 runbook §6）。
- 滞后持续增长 = 生产快于消费（扩消费者）；停滞且 > 0 = 消费者停摆。

## 6. Cache（缓存，EXPERIMENTAL）

| RPC（组） | 能力 |
|:--|:--|
| `Cache/Get` / `HGet` / `HGetAll` / `LRange` / `LLen` / `SMembers` | `coord:cache:read` |
| `Cache/Set` / `HSet` / `LPush` / `RPop` / `SAdd` / `Delete` | `coord:cache:write` |

- 容量：默认 1GB，超界淘汰 + 单条超限写拒绝（`RESOURCE_EXHAUSTED`）；
  周期收敛、非逐写严格上界的口径与 ISR 本地回收行为见 `boundaries.md` B-PL-3。
- 缓存**不是**权威存储；跨节点提交非原子（ADR-0002 边界）——以数据库/权威源
  为准。

## 7. Workflow / Scheduler（EXPERIMENTAL）

| RPC（组） | 能力 |
|:--|:--|
| `Scheduler/RegisterJob` | `coord:scheduler:manage` |
| `Scheduler/ClaimJob` / `Heartbeat` / `CompleteJob` | `coord:scheduler:execute` |
| `Workflow/Start` / `Signal` / `Cancel` | `coord:workflow:execute` |
| `Workflow/GetStatus` / `ListDefinitions` / `GetDefinition` / `ListDefinitionVersions` / `ListInstances` | `coord:workflow:read` |
| `Workflow/Deploy` / `RollbackDefinition` | `coord:workflow:define` |

- 调度：认领式（`ClaimJob` 带租约，`Heartbeat` 续约），worker 崩溃由租约过期
  重派；**任务完成结果随完成持久化**（G-SC-1）——`CompleteJob` 携带 `result`
  字节（可选），任务详情可回读 `result` / `completed_at`；**首个结果生效**
  （重复完成幂等，不覆盖）。
- 工作流：实例与定义 **append-only**（无删除/保留 API，`boundaries.md`
  B-WF-1）；子流程恢复为周期扫描（B-WF-2/B-WF-3）。
- **补偿（saga）已落地**：`sw.rs` 编译 DSL 的 `CompensatedBy`，实例失败/取消
  时按序执行补偿步骤（端到端判据
  `coord-agent/src/services/workflow.rs::test_sw_compensated_by_runs_compensation_end_to_end`）。

## 8. Policy（策略引擎，G-POL-1 / G-POL-2）

| RPC（组） | 能力 |
|:--|:--|
| `Policy/CheckPermission` / `Evaluate` / `Explain` | `coord:policy:evaluate` |
| `Policy/PutBundle` / `DeleteBundle` / `SetBundleEnabled` / `RollbackBundle` / `ListBundles` / `ListBundleVersions` | `coord:policy:manage` |

- **`CheckPermission` 仅本 agent 内存 RBAC（本地 / 嵌入式用途）**：不跨 agent
  分发 RBAC 结构化策略，**不提供 RBAC 跨 agent 管理面**（G-POL-2，契约注释
  逐字见 `policy.proto`）。
- 生产接入统一走 **OPA bundle + `Evaluate`**：bundle 存 Server KV，agent
  启动全量装载 enabled bundle + `Watch` 收敛（**≤10s 量级**）；`PutBundle` /
  `RollbackBundle` 走 per-key CAS（版本单调、无丢更新）。

### 8.1 demo recipe：装载 → 判定 → 重启仍生效（G-POL-2 替代路径）

1. **装载**：`PutBundle`（enabled=true，Rego 内容）到当前批次的 bundle key；
2. **判定**：`Evaluate`（输入 + bundle 引用）返回决策；等待 ≤10s 收敛后，
   在**任意**已启用 policy 的 agent 上判定一致；
3. **重启仍生效**：重启 agent 进程 —— 启动时自动从 KV 全量装载 enabled
   bundle，**无需**重新 `PutBundle`，判定结果不变；
4. **变更/回滚**：`PutBundle` 新版本或 `SetBundleEnabled(false)` /
   `RollbackBundle` 后 ≤10s 收敛。

自动化判据：`coord/tests/policy_bundle_distribution_test.rs::test_bundle_distribution_load_convergence_and_versioning`
（双 agent：A `PutBundle` 本地生效、B ≤10s 收敛；重启 A 后重加载；版本单调、
并发 Put 无丢更新）。

## 9. PKI（证书，G-PKI-1 / G-PKI-2 / G-PKI-3）

| RPC（组） | 能力 |
|:--|:--|
| `Pki/InitCa` | `pki:ca:init` |
| `Pki/IssueCert` / `RenewCert` | `pki:cert:issue` |
| `Pki/RotateCert` | `pki:cert:rotate` |
| `Pki/ListCerts` / `GetCertByCN` / `GetCaCert` / `VerifyCert` | `pki:cert:read` |

- **权限定级 `GetCertByCN` = 私钥读取权**（返回含 PEM 私钥；`ListCerts` 才是
  无私钥摘要）——授权矩阵按此口径出授（`security.md` §3.1）。
- **到期 = 换新触发**（G-PKI-1）：`IssueCert` / `RotateCert` 对过期记录走替换
  路径重签（新 serial / 新密钥）；`RenewCert(serial)` 为按序列号恢复入口；
  `GetCertByCN` 仅未过期返回，否则 `NOT_FOUND`。
- 观测：`coord_agent_pki_certs_active` / `..._certs_expiring_soon`（窗口
  `expiry_warn_hours`，默认 6h），告警 `CoordPkiCertExpiringSoon`。
- **无吊销（CRL/OCSP 不在本轮）**：泄露应急 = 消费侧剔除 + CN 轮换
  （playbook：`security.md` §3.2 / runbook §4.5）；非 HSM 边界与重估触发见
  `boundaries.md` B-PKI-1。

## 10. Transit（信封加密与 HMAC，G-TR-1 / G-TR-2）

| RPC（组） | 能力 |
|:--|:--|
| `Transit/Encrypt` / `Decrypt` / `HmacSign` / `HmacVerify` / `Rewrap` | `coord:transit:crypto` |

- 信封加密：DEK 一次性、KEK 留在 agent；**KEK 必须外部注入**（未注入 ⇒ 启用
  transit 的 agent **拒绝启动**，B-SE-2）。
- **多材料解密窗口（G-TR-1）**：主材料 + 历史材料（`COORD_TRANSIT_KEK_OLD` /
  `transit-kek-old.txt`）；密文/DEK 记录携带材料标识；`Rewrap` 为管理路径
  （旧材料解出 DEK → 主材料重包，返回 `new_dek_id` / `kek_id`）。**迁移完成前
  不得下线旧材料**（流程见 runbook §4.4）。
- **G-TR-2（产品决策定谳）**：**不提供远程非对称签名**——transit 维持对称
  加密 + HMAC，PKI 维持「密钥交付持有方」模型。「私钥不出域」的签名场景
  （token / 审计锚点等）不在承诺内；触发条件（组织 KMS 方向确定）时另行立项
  评估。边界条目：`boundaries.md` B-TR-1。

## 11. 本地态服务（CircuitBreaker / RateLimiter / FeatureFlags）

| RPC（组） | 能力 |
|:--|:--|
| `CircuitBreaker/GetState` | `coord:breaker:read` |
| `CircuitBreaker/ReportSuccess` / `ReportFailure` | `coord:breaker:report` |
| `CircuitBreaker/Reset` | `coord:breaker:manage` |
| `RateLimiter/Allow` | `coord:ratelimit:check` |
| `FeatureFlags/IsEnabled` / `Evaluate` | `coord:flags:read` |

- 熔断器与限流器是**本 agent 本地内存态**：不跨 agent 共享（每个 agent 一份
  计数/令牌桶；多 agent 部署 = 各节点独立配额）。契约声明见 `STATUS.md` 与
  `circuitbreaker.proto` / `ratelimiter.proto`。
- 方法应为「就近调用」（同进程/同主机 agent），不要当作全局协调器。

## 12. 对象存储（EXPERIMENTAL 数据面）

- RPC：`Storage/Get` / `Stat`（`data:storage:read`）、`Put` / `Delete`
  （`data:storage:write`）。
- 语义、配额、流式上传与边界（快照恢复期间 `Get` 返回 `UNAVAILABLE`、保留
  前缀 `/obj/`）见 `volume-object-storage.md`。

## 13. 已接受边界总表（不承诺清单，索引）

事实源：`ops/boundaries.md`（逐条带实现锚点与「若要变成承诺」路径）。与消费方
最相关的条目：

| 边界 | 一句话 | 消费影响 |
|:--|:--|:--|
| B-WF-1 | 工作流实例与定义 append-only（无删除/保留 API） | 生命周期由业务侧自己的保留策略兜 |
| B-WF-2 / B-WF-3 | 子流程恢复为周期轮询；父定义暂读不到时暂停接管 | 恢复有秒级延迟，不是事件驱动 |
| B-RG-1 / B-RG-2 | 不支持动态增删 region；region 模式 watch 拒绝跨 region | 静态拓扑规划；watch 前缀受限 |
| B-CX-1 / B-CX-2 | 连接数上限但无寿命回收；客户端连接池机会式回收 | 容量规划考虑长连接占坑 |
| B-SE-1..B-SE-8 | TLS fail-closed / KEK 注入 / dev 例外等安全边界 | 生产接入按 TLS+注入材料配置 |
| B-PKI-1 | 非 HSM + 无吊销；`GetCertByCN`=私钥读取权 | 授权口径 + 轮换应急（§9） |
| B-TR-1 | 不提供远程非对称签名（G-TR-2） | 需「私钥不出域」签名时另行立项 |
| B-PL-3 | 缓存容量周期收敛、非真 LRU、ISR 本地回收 | 缓存不作为配额硬边界依赖 |
| B-PL-4 | MQ 逐写严格上界；ISR 下配额/回收各节点本地；删除仅经 `DeleteTopic` | §5.4 前置条件；不得用内部分段方法代餐 |
| B-ST-1 / B-ST-3 | 日志回收快照锚定；对象存储恢复期间 `UNAVAILABLE` | 备份/恢复演练按 runbook §3 |

未经 `boundaries.md`「边界变更」三步（更新清单 + STATUS + 负控制证据）流程，
任何「不承诺」不得被写成承诺。

## 14. 接入检查清单（新消费者「1 天接入」判据）

1. 选定服务包（§1–§12），按表确认自己的 CCT 具备所需能力名；缺失走授权变更。
2. 错误处理：只判别 `x-coord-error-code` / SDK 结构化码；确认未解析文案。
3. 重试策略：按 §0.2 矩阵落地（幂等前提、NOT_LEADER 重路由、RSE 不重试）。
4. 选对消费范式：MQ 用 `Poll+Ack`；事件要补投就持久化 `seq`；watch 断线
   重读权威状态。
5. 核对所依赖服务在 §13 中的边界，确认不在承诺上与文档冲突。
6. 按 SDK（Rust / Java）接口 javadoc 的规范示例跑通一条最小链路。
