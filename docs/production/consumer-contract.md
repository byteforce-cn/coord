# 消费方契约页（Consumer Contract）

> Owner: maintainers ｜ Last verified: 2026-10-18

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
- **执行面（唯一事实源）**：能力名唯一维护在
  `coord-core/src/grpc_auth.rs::rpc_capability`（方法 → 能力）；只有这里返回的
  能力名会被鉴权层判定。本页 §1–§11 列出的是**消费方视角的对照**，变更以该文件为准。
- **发现面**：`CapabilityRegistry/List`（`coord capability list`）列出的 live 能力
  集合与执行面**双向恒等**——这是机器判据，不是承诺：
  `scripts/check-capability-registry.sh` 从 proto 描述符集枚举全部方法，反向推出
  执行面集合并与 registry 比对（新增 RPC 漏登记、只补一侧、废弃标记不全均会置红）。
- **`deprecated=true` 语义**：已废弃的名字**永远不会被执行**（`rpc_capability`
  不返回它们），保留只为让存量授权与拼写错误在 `List`/`Get` 里可发现；`description`
  写明替代能力，或明确「无替代（功能未实现）」。
- **授权写入校验**：`RoleGrantCapability` 对未知或已废弃的能力名返回
  `INVALID_ARGUMENT` 并点名该 ID（废弃项同时给出替代能力）；`RoleRevokeCapability`
  **不校验**能力名（放行任意 ID），否则校验上线前写入的拼写错误将无法清理。
  存量授权不需要数据迁移——被拒的只是新写入。
- 运行时 `CapabilityRegistry/Register` 注册的自定义能力**不在执行面**
  （`rpc_capability` 无对应映射 ⇒ 永远不会被判定），因此也不参与授权校验；
  可执行能力只能通过编译期目录扩展。
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
- **v1 数据面在 v2 集群不服务**（ADR-0015 D13）：v1 包冻结，不做 facade/迁移；
  每个 v1 数据面 RPC 返回 `FAILED_PRECONDITION`（文案 `v1 data plane is frozen
  on a v2 cluster`，先于参数/leader/写闸判定），health 报 `NOT_SERVING`，
  v1 `coord.storage` 不注册；v2 见 §1.1。

### 1.1 KV-T v2（`coord.<domain>.v2`，默认关闭）

默认 `[kv_t].enabled=false`：v2 服务注册（reflection 可见）但每个 RPC 返回
`FAILED_PRECONDITION`；开启后按 gate 分批可用（G0–G6，ADR-0015 D10），未交付
能力同样 `FAILED_PRECONDITION`。语义单一归属：ADR-0015 D2–D13 + v2 proto 头注。

| RPC（组） | 能力 |
|:--|:--|
| `coord.tso.v2.TSO/Allocate`、`GetTs` | `data:tso:allocate` |
| `coord.kv.v2.KV/Range` | `data:kv:read` |
| `coord.kv.v2.KV/Put` / `Delete` | `data:kv:write` / `data:kv:delete` |
| `coord.txn.v2.Txn/Begin` / `Read` / `Commit` / `Abort` | `data:txn:execute` |
| `coord.lease.v2.Lease/LeaseGrant` / `LeaseRevoke` / `LeaseKeepAlive` / `LeaseTimeToLive` | `data:lease:grant` / `:revoke` / `:keepalive` / `data:lease:read` |
| `coord.watch.v2.Watch/Watch` | `data:watch:subscribe` |
| `coord.storage.v2.Storage/Put` / `Get` / `Stat` / `Delete` | `data:storage:write` / `data:storage:read` |

- **一致性**：serializable（非 strict）；`Range.read_ts=0` 取 `safe_ts`，
  `read_ts > safe_ts` 等待至多 `read_ts_wait_ms` 后 `SAFE_TS_LAG`（可重试）。
- **跨 Region Range**：由请求节点的本地副本服务（v1 静态复制下每节点持有全部
  Region），不要求 leader；`read_ts <= 本地 applied` 由 safe_ts 等待 + 分段复核
  保证，不足 `UNAVAILABLE + SAFE_TS_LAG`（可重试）；整请求失败、不返回部分结果。
  `read_ts=0` 取**本节点** `safe_ts`，可能落后于集群最新提交、跨节点不保证同一
  数值/单调；跨节点同一快照或 read-your-writes 请用显式 `read_ts`。
- **事务**：`Begin` 建服务端读集会话；`Commit` 在 CommitDecision 落盘后返回
  `commit_ts`（不等待各 Region apply，读用 `read_ts >= commit_ts`）；冲突
  `ABORTED` + `TXN_CONFLICT`（换 `start_ts` 重跑）；会话丢失/已提交再读
  `TXN_NOT_FOUND`（重跑）；同一 `txn_id` + `request_id` 在 TTL 内幂等回放首次
  `commit_ts`（协调节点存活、region 0 leader 切换时同样成立；协调节点崩溃会
  丢会话，需 `TXN_NOT_FOUND` 后重跑整个事务）。写集 4MiB / 读集 4096 / 单 range
  10000 key 上限（写集超限
  `TXN_TOO_LARGE`，读集超限 `READ_SET_TOO_LARGE`，均 `INVALID_ARGUMENT`）。
  `DELETE_RANGE` 删除 `[key, range_end)`（`range_end` 必须非空且大于 `key`），
  与同一事务的其它 mutation 共享 `commit_ts`，跨 Region 由 `safe_ts` 收敛后
  原子可见；point/range 读集与区间写按相交判冲突（冲突返回 `TXN_CONFLICT`；
  校验索引扫描超 `max_range` 返回 `READ_SET_TOO_LARGE`）。`max_range` 按区间
  索引条目总量计（生命周期累计，超过后所有带读集的事务都会被拒），不是单次
  谓词的 key 数；区间删除的 apply 成本与区间内存活 key 数成正比（不按
  `max_txn_bytes` 计）。容量、GC 与分块 apply 见 #174。TTL 从 `Begin`
  起算、`Read` 不续期；split drain 期间命中被 drain 区间的 `Read` 返回
  `UNAVAILABLE`（退避后重试同一会话，或重跑）。
- **Lease v2**：Grant/Revoke/KeepAlive/TimeToLive 由 region 0 leader 服务；租约
  元数据在 region 0，绑定关系由各数据 Region 的 `KvMetadata.lease_id` 维护。
  `Revoke` 返回清理决策的 `commit_ts`（未知/已吊销 lease 幂等返回 0）；跨 Region
  删除最终一致（数据 Region 已 apply 更高 `commit_ts` 时服务端重取 TSO 重试）。
  `Put(lease_id>0)` 对未知/已吊销/已过期 lease 返回 `FAILED_PRECONDITION`。
  `LeaseTimeToLive(keys=true)` 从请求节点的数据 Region 副本收集绑定 key（有上限）。
- **Watch**：按 `commit_ts` 全局有序、同一事务原子批；断线用
  `WatchCursor(shard_id, commit_ts)` 续订；窗口不足 `OUT_OF_RANGE` +
  `HISTORY_UNAVAILABLE`（须全量重同步）。Commit Log 记录按 Region 分片、按
  `commit_ts` 合并为一个原子事件；记录含完整 key/value 与 `prev_kv`（不依赖
  Region 历史），zstd 压缩，写集 4MiB 上限在 Commit 阶段强制；记录值与 MVCC
  同信封（保留 `key_id`），DEK 轮换窗口须 ≥ `watch_retention`（默认 24h，最大
  7d），否则超出窗口的旧事件不可解密。Commit Log 随快照/备份恢复归 #169。
- **Commit Log 分片**：shard 是 `commit_ts` 的半开区间 `[start_ts,end_ts)`
  （`end_ts=0` 表示未定界，仅最后一个 shard），由 region 0 权威元数据唯一
  定义、按前一 shard 的 `end_ts` 首尾相接（无重叠/空洞）；第一个 shard 从
  `start_ts=1` 起，覆盖建表前的历史。每个 shard 一个独立 Raft 组：记录按
  `commit_ts` 存完整 change record（含 `prev_kv`），读取以 shard 组为准。
  阈值按 D9：1,000,000 条或 1 小时先到先 seal，达到 80% 预创建下一个 shard，
  定界后 seal 超时 5 分钟。数据 Region 的本地 outbox 是写前日志（与数据 apply
  同事务），由 shard leader 幂等搬入 shard 组；换 leader/重启从 per-Region
  shipped 水位续传。CommitWatermark 是「不再出现更小 `commit_ts` 记录」的单调
  水位：无 in-flight 时等于最后决策的 `commit_ts`，否则为最早未 apply/未搬运的
  `commit_ts - 1`；shard 只在 `watermark >= end_ts` 时 seal，停滞超过 5 分钟按
  超时 seal 并计入 `coord_kv_t_commit_watermark_stalls_total`，sealed shard 仍
  接受区间内迟到记录。**Watch 读取**：Coordinator 按 shard 顺序读，shard 内按
  `commit_ts` 整组读取；上界取本节点 watermark，跨 shard 只在 shard sealed 且
  watermark 越过 `end_ts` 后推进。同一事务的 Region 分片合并为一个事件，单批
  ≤1000 条 change 或 1MB（事务自身超限不拆批）。cursor 为
  `WatchCursor(shard_id, commit_ts)`，`shard_id=0` 表示首个 shard 之前；
  key/range 与 put/delete 过滤只裁剪事件内容，cursor 仍推进。**保留窗口**：按
  `[kv_t].watch_retention`（1h–7d，默认 24h）整 shard 回收；回收条件是 shard
  已 seal、`watermark >= end_ts`、且 `sealed_at_ms + watch_retention` 已到。
  回收水位 `gc_before_ts` 持久在 region 0 表、只前进且只等于某个 shard 的
  `end_ts`；`commit_ts < gc_before_ts` 的事件不可读，起点/续订落入该区间返回
  `OUT_OF_RANGE` + `HISTORY_UNAVAILABLE`（客户端全量重同步），最老可用
  `WatchCursor` 随错误与批返回。shard 元数据行保留（id 连续、按 `commit_ts`
  路由），本地 shard 组与数据目录删除，重启不复活。shard 组副本随窗口回收；
  **数据 Region 的 staging outbox 尚未裁剪**，因此磁盘上界暂不严格等于
  窗口 × 写入速率（裁剪归 #174 的容量工作）；回收进度看
  `coord_kv_t_commit_log_gc_before` 与 `coord_kv_t_commit_log_shards_gc_total`。
  **RPC 面**（`coord.watch.v2.Watch`，G4）：双向流，首帧必须是 `create`；同一流可
  建多个订阅，`cancel` 按 `watch_id` 生效（未知 id 幂等）。服务端建立订阅后先回一条
  空 `events` 批（ack），回显解析后的 `cursor` 与当前 `watermark`/`sealed`；
  `start_revision` 与 `start_cursor` 互斥（同时给出 `INVALID_ARGUMENT`），
  `start_revision=0` 表示从当前 watermark 之后开始。`range_end` 空 = 单 key，
  key/range 都空 = 全 keyspace；`filter`/`prev_kv` 只裁剪事件内容，cursor 照常推进。
  历史窗口不足时 RPC 以 `OUT_OF_RANGE` + `HISTORY_UNAVAILABLE` 终止（消息带最老可用
  位置），**不是**可重试错误。单条流的响应通道有界（32 批）：慢消费者承受背压，
  服务端不丢事件。`coord-client` 的 `watch_v2()` 包装隐藏 `watch_id`，断线后用最后
  一批 `cursor` 自动重开流；投递语义是**至少一次**（重放按 `commit_ts` 幂等）。
  订阅指标：`coord_kv_t_watch_streams`、`coord_kv_t_watch_batches_total`、
  `coord_kv_t_watch_events_total`、`coord_kv_t_watch_lag_commit_ts`、
  `coord_kv_t_watch_history_unavailable_total`、`coord_kv_t_watch_backpressure_total`。
  **授权边界**：能力 `data:watch:subscribe`；非空 scope 的授权按订阅首帧的
  key/range 判定（鉴权层放行并下发 `DeferredScopeGrants`，handler 在首帧
  `check_deferred_scope`）；越界订阅返回 `PERMISSION_DENIED`，v1/v2 同口径；
  保留命名空间（`/_pki/` 等）与 `/obj/` 的订阅一律拒绝。
- **路由**：可带 metadata `coord-routing-epoch`（值为 Region 表 `version`）；
  与服务端已应用版本不一致返回 `ABORTED` + `EPOCH_STALE`（trailing metadata
  回传当前版本），刷新路由后重试；不带该 metadata 不校验。
- **对象存储**：manifest 用 `commit_ts`；`/obj/` 固定 Region 区间，不参与任意
  KV split；chunk 不随 KV split 迁移（`boundaries.md` B-KVT-3）。
- **不承诺**：v1 数据迁移 / v1 facade；MQ v1 在 v2 集群不可用；MQ v2 未 GA；
  严格外部实时序（`boundaries.md` B-KVT-1..8）。

## 2. Registry / Config（场景 ①）

| RPC（组） | 能力 |
|:--|:--|
| `Registry/Register` / `Deregister` / `Heartbeat` / `Discover` / `Watch` | `coord:registry:register` / `:deregister` / `:heartbeat` / `:discover` / `:watch` |
| `Config/Get` / `List` / `Watch` | `coord:config:read` / `:list` / `:watch` |
| `Config/Put` | `coord:config:write` |

- 注册发现：实例心跳过期由服务端判定下线；消费者侧以 `Discover` 结果为准，
  `Watch` 只作变更信号。
- **实例 metadata 是不透明元数据**（proto 约定建议 JSON），实例地址放 `address`
  键（`{"address":"host:port",...}`）；agent 的存活探测只读该键，读不到地址的
  实例**不探测**（保留在 `Discover` 结果里，不因元数据形态被摘流）。
- 配置中心：读取「KV + 本地 cache + watch」；变更传播是收敛语义（§0.3）。
  - **快照 + 位点续订（resync 纪律）**：`List` 返回 `entries + revision`（位点 W）；
    以 W 为起订位点订阅，闭合「快照 → 订阅」窗口——**快照后被删除的 key
    也能从位点事件可靠收敛**（可能重复，应用须幂等）。Java SDK：
    `listWithRevision` + `watchFrom`（SDK 断线重连自动按 `last + 1` 续订）。
  - **删除语义（presence）**：`ConfigWatchEvent.new_value` 缺省 = 键已删除；
    空串 = 存储的空值。键存在性以 `ConfigGetResponse.found` 为准，不以空串推断。
  - `revision` 为**全局位点**（Server KV 全局 Revision 口径）：`Put` 响应、
    `List` 快照、`Watch` 事件三者同一坐标轴；持久化事件的 `revision` 作为续订游标。
  - 位点已不可达（历史压缩）时 Watch 流以 `FAILED_PRECONDITION` 终止：须重读
    `List` 取新位点后重订（不得静默续跑；overflow 终止为 `UNAVAILABLE`，按位点重连即可重放）。

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
| `MQ/CreateTopic` / `DeleteTopic` | `coord:mq:manage` |
| `MQ/MoveToDlq` / `PollDlq` | `coord:mq:dlq` |
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
- **最小权限**：毒消息处置只需 `coord:mq:consume + coord:mq:dlq`；
  `coord:mq:manage` 只覆盖 topic 生命周期（`CreateTopic` / `DeleteTopic`），
  不得为处置毒消息而授予。
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

## 6. Cache（缓存）

| RPC（组） | 能力 |
|:--|:--|
| `Cache/Get` / `HGet` / `HGetAll` / `LRange` / `LLen` / `SMembers` / `Keys` | `coord:cache:read` |
| `Cache/Set` / `SetIfAbsent` / `HSet` / `LPush` / `RPop` / `SAdd` / `Delete` / `Clear` | `coord:cache:write` |

- 容量：默认 1GB，超界淘汰 + 单条超限写拒绝（`RESOURCE_EXHAUSTED`）；
  周期收敛、非逐写严格上界的口径与 ISR 本地回收行为见 `boundaries.md` B-PL-3。
- 缓存**不是**权威存储；跨节点提交非原子（ADR-0002 边界）——以数据库/权威源
  为准。
- **原子条件写 `SetIfAbsent`（SETNX）**：仅当 key 不存在（或已过期）时写入并返回
  `written=true`；已存在时不改动原值与 TTL，TTL 仅在写入成功时生效。单 agent 下
  判定与写入同一写事务（N 并发仅 1 个成功）；ISR 下判定在 Leader 本地事务完成、
  成功经复制通道下发。错误语义与 `Set` 相同（本地可能已生效）；重试 = 再次尝试
  获取（首次已写则返回 false，与 Redis SETNX 同口径）。
- **键空间操作**：`Keys(prefix, limit, cursor)` 有界枚举（string/hash/list/set
  逻辑键去重、升序、游标分页；本节点视角 best-effort）；`Clear(prefix)` 按逻辑键
  前缀清理**全部数据类型**（批式执行、记账同步扣减；ISR 下清理决定经复制通道
  全域下发）。键不得含 NUL（`\x00`）字节。边界见 `boundaries.md` B-PL-5。

## 7. Workflow / Scheduler

| RPC（组） | 能力 |
|:--|:--|
| `Scheduler/RegisterJob` | `coord:scheduler:manage` |
| `Scheduler/ClaimJob` / `Heartbeat` / `CompleteJob` | `coord:scheduler:execute` |
| `Scheduler/GetJob` | `coord:scheduler:read` |
| `Workflow/Start` / `Signal` / `Cancel` | `coord:workflow:execute` |
| `Workflow/GetStatus` / `ListDefinitions` / `GetDefinition` / `ListDefinitionVersions` / `ListInstances` | `coord:workflow:read` |
| `Workflow/Deploy` / `RollbackDefinition` | `coord:workflow:define` |
| `Workflow/Approve` / `Reject` / `Acknowledge` / `AddSign` / `RemoveSign` / `ReturnToNode` / `Transfer` / `Delegate` / `Withdraw` / `Terminate` | `coord:workflow:execute` |
| `Workflow/ListPendingTasks` / `GetInstanceDetail` | `coord:workflow:read` |

- 调度：认领式（`ClaimJob` 带租约，`Heartbeat` 续约），worker 崩溃由租约过期
  重派；**任务完成结果随完成持久化**（G-SC-1）——`CompleteJob` 携带 `result`
  字节（可选），**首个结果为准**（重复完成幂等，不覆盖）；结果与完成时刻经
  **`GetJob` 回读**（只读、幂等；未注册 ⇒ `found=false`，不是错误码；重启 /
  换 agent 后仍可读，状态在 Server KV）。
- 工作流：**保留策略已落地（G-WF-1）**——`DeleteInstance`（仅终态）/`DeleteDefinition`（无实例引用才可删）；删除**不可回滚**、**无自动 TTL/归档**（`boundaries.md` B-WF-1；完整语义见 `workflow-semantics.md`）；子流程恢复为周期扫描（B-WF-2/B-WF-3）。
- **人工审批 v2（原生 `approve` 节点，ADR-0013 / ADR-0014）**：走到该节点的工作流实例进入
  `SUSPENDED`（`expected_signal = "approval.completed"`，审批实例 id = workflow 实例 id）；
  外部经 10 个命令 RPC（`Approve/Reject/Acknowledge/AddSign/RemoveSign/ReturnToNode/Transfer/
  Delegate/Withdraw/Terminate`）操作席位、经 2 个查询 RPC（`ListPendingTasks`/
  `GetInstanceDetail`）读待办与实例详情。命令携带 `idempotency_key`，重放返回首次响应
  （不重复执行）；默认主体身份取自 CCT（请求体无 actor）。
  业务服务代终端用户时：命令填 `on_behalf_of`、`ListPendingTasks` 填 `assignee`；调用方
  服务须在 agent 配置 `services.workflow_on_behalf_services` 白名单内，生效主体是被代表
  用户（席位归属判定不变）。`GetInstanceDetail` 的 votes/audit 同时给出 `actor`/`by`
  与 `acting_service`，可区分「谁批的」与「哪个服务代执行」。
  席位来源支持 `${ ... }` 运行时表达式（`$input`/`$context`），空结果 fail-closed。
  审批事件写 `com.china-workflow.*` 命名空间、带 `.v1` 后缀：实例级 `instance.returned|
  cancelled|terminated|completed|rejected.v1`、轮次级
  `approval.round.passed|rejected.v1`；**消费者按 `(instance_id, sequence)` 去重**，每个类型
  的 JSON Schema 在 `apis/contracts/schemas/events/`。
  审批面依赖 durable 装配：`coord dev` 一体化拓扑默认开启；生产默认关闭（ADR-0001），
  未启用时审批 RPC 返回 `FAILED_PRECONDITION`，且 `approve` 节点在驱动时 fault
  （实例 `FAILED`）。授权矩阵、deadline 无自动处置、幂等与分页口径见
  `workflow-semantics.md` §7 / ADR-0013 / ADR-0014。
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
- **证书 profile**：密钥 ECDSA P-256；KU = `digitalSignature` + `keyEncipherment`；
  EKU = `clientAuth` + `serverAuth`；有效期由请求 TTL（缺省 24h）决定。
  **身份以 SAN 为准**，CN 仅作 subject（OpenSSL 的 CN 回退不是契约）：`IssueCert`
  未传 `dns_names` / `ip_addresses` 时默认把 CN 派生为 DNS SAN；CN 不是合法
  DNS 名（非 ASCII 等）⇒ `INVALID_ARGUMENT`（显式传 `dns_names` 可保留任意 CN）。
  `RotateCert` 未传 SAN ⇒ 继承当前证书；`RenewCert` 同样继承。
- **参数一致才幂等**：同 CN 的 `IssueCert` 仅在 SAN 与 TTL 都与当前 active 记录一致时
  返回既有证书（同 serial / 同私钥）；SAN 或 TTL 不同 ⇒ 等价 `RotateCert`
  （新 serial + 新密钥，旧记录 retired 保留至 `not_after`，按 serial 仍可取回）。
- 观测：`coord_agent_pki_certs_active` / `..._certs_expiring_soon`（窗口
  `expiry_warn_hours`，默认 6h），告警 `CoordPkiCertExpiringSoon`。dev 下
  `agent_port+1/metrics` 可见这些 gauge（≤1 个 30s 采样周期），但**不等于**告警会
  触发——dev 默认没有 Prometheus 抓取；告警判据需要真实抓取链路。
- 开发态取证书（不依赖 grpcurl）：`docs/pki-quickstart.md`（`coord pki ca/issue/list`）。
- **无吊销（CRL/OCSP 不在本轮）**：泄露应急 = 消费侧剔除 + CN 轮换
  （playbook：`security.md` §3.2 / runbook §4.5）；非 HSM 边界与重估触发见
  `boundaries.md` B-PKI-1。

## 10. Transit（信封加密与 HMAC，G-TR-1 / G-TR-2 / G-TR-3）

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
- **HMAC 用途化与轮换窗口（G-TR-3，ADR-0012）**：`HmacSign` / `HmacVerify` 的 `purpose`
  必填（非空、不含 NUL、≤128 字节；空/非法 ⇒ `INVALID_ARGUMENT`），按用途域分隔派生、
  互不可验。签名的 `key_id` 是材料标识（Vault Transit 的 key version 等价物），**必须与
  签名一起持久化**；验签 `key_id` 必填（空 ⇒ `INVALID_ARGUMENT`），只按该材料复算：材料
  已注入 ⇒ 正常校验；未注入 ⇒ `FAILED_PRECONDITION`（fail-loud，不是 `valid=false`）。
  新写入不传 `key_id`（主材料）；显式 `key_id` 用于轮换期按旧材料复算散列变体（DRV-005
  防重）。轮换/下线/回退见 runbook §4.4。**跨 agent 一致性**：同集群所有 agent 必须注入
  相同材料与标识；同一 `(purpose, data, key_id, algorithm)` 在任一 agent 结果一致。
  主材料标识由部署注入（`COORD_TRANSIT_KEK_ID`，缺省 `default-kek`），**轮换时随材料
  换代**（同一标识不得对应两份材料）；旧材料按旧标识注入历史集合，旧签名 / 旧记录
  才可按 `key_id` 复算。
  用途是**域分隔标签，不是授权边界**——持有 `coord:transit:crypto` 的调用方可签/验任意合法 `purpose`；按服务隔离用途不属本契约承诺。

## 11. 本地态服务（CircuitBreaker / RateLimiter / FeatureFlags）

| RPC（组） | 能力 |
|:--|:--|
| `CircuitBreaker/GetState` | `coord:breaker:read` |
| `CircuitBreaker/ReportSuccess` / `ReportFailure` | `coord:breaker:report` |
| `CircuitBreaker/Reset` | `coord:breaker:manage` |
| `RateLimiter/Allow` | `coord:ratelimit:check` |
| `FeatureFlags/IsEnabled` / `Evaluate` / `ListFlags` | `coord:flags:read` |
| `FeatureFlags/PutFlag` / `DeleteFlag` | `coord:flags:manage` |

- 熔断器与限流器是**本 agent 本地内存态**：不跨 agent 共享（每个 agent 一份
  计数/令牌桶；多 agent 部署 = 各节点独立配额）。契约声明见 `STATUS.md` 与
  `circuitbreaker.proto` / `ratelimiter.proto`。
- 方法应为「就近调用」（同进程/同主机 agent），不要当作全局协调器。
- 特性开关：状态存 **Server KV**（重启不丢、跨 agent 共享）；读路径直读存储、
  无本地缓存 ⇒ `PutFlag`（全量；`percentage` 给出时为灰度，`[0,100]` 外拒绝
  `INVALID_ARGUMENT`）与 `DeleteFlag`（幂等）**写入立即生效**，从任一已连接
  agent 求值一致；`ListFlags` 按键升序返回全部开关。带外 KV 写入路径仍可用：
  键 `/_featureflags/v1/flag/{flag_name}`，值为 `{"enabled":bool,"percentage":0-100|null}`
  （JSON）。

## 12. 对象存储（数据面）

- RPC：`Storage/Get` / `Stat`（`data:storage:read`）、`Put` / `Delete`
  （`data:storage:write`）。
- 语义、配额、流式上传与边界（快照恢复期间 `Get` 返回 `UNAVAILABLE`、保留
  前缀 `/obj/`）见 `volume-object-storage.md`。

## 13. 已接受边界总表（不承诺清单，索引）

事实源：`ops/boundaries.md`（逐条带实现锚点与「若要变成承诺」路径）。与消费方
最相关的条目：

| 边界 | 一句话 | 消费影响 |
|:--|:--|:--|
| B-WF-1 | 工作流有显式保留 API（`DeleteInstance` 仅终态 / `DeleteDefinition` 无实例引用才可删），但**无自动 TTL/归档/删除审计** | 保留节奏由调用方显式管理；删除不可回滚（无归档副本） |
| B-WF-2 / B-WF-3 | 子流程恢复为周期轮询；父定义暂读不到时暂停接管 | 恢复有秒级延迟，不是事件驱动 |
| B-WF-5 | 审批 deadline 到期只置席位置过期，**不自动通过/驳回**；全员过期/被减签保持 `Open` | 超时处置由人（`AddSign` 补席 / `Terminate` 终止） |
| B-WF-6 | `ListPendingTasks` 为全 v4 实例前缀扫描 + 分页（无按 actor/年龄的索引） | 待办查询随实例规模线性增长；自行分页 |
| B-WF-8 | 无 UI 审批面、无 REST facade（`coord-ui` 仅影响评估） | 审批面仅 gRPC；Web 接入需另行立项建 facade |
| B-WF-9 | `ReturnToNode` 重开席位不设 deadline | 重开后无自动提醒；靠 `AddSign`/`Terminate` 兜底 |
| B-WF-10 | 审批命令不产生审批实例 `Faulted`；只收口 `Completed/Rejected/Cancelled/Terminated` | 版本/定义漂移故障落在工作流实例 `FAULTED`，不从命令返回 |
| B-RG-1 / B-RG-2 | v1 静态 Region 拓扑（`[kv_t]` 的表驱动生命周期已接线，但 split/merge 数据搬迁未交付）；region 模式 watch 拒绝跨 region | 静态拓扑规划；watch 前缀受限 |
| B-CX-1 / B-CX-2 | 连接数上限但无寿命回收；客户端连接池机会式回收 | 容量规划考虑长连接占坑 |
| B-SE-1..B-SE-8 | TLS fail-closed / KEK 注入 / dev 例外等安全边界 | 生产接入按 TLS+注入材料配置 |
| B-PKI-1 | 非 HSM + 无吊销；`GetCertByCN`=私钥读取权 | 授权口径 + 轮换应急（§9） |
| B-TR-1 | 不提供远程非对称签名（G-TR-2） | 需「私钥不出域」签名时另行立项 |
| B-PL-3 | 缓存容量周期收敛、非真 LRU、ISR 本地回收 | 缓存不作为配额硬边界依赖 |
| B-PL-5 | 缓存 `Keys` 为有界/本节点视角枚举；`Clear` 批式、与并发写按到达顺序 | 运维清理窗口内暂停并发写；键不得含 NUL |
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
