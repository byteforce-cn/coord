# coord-agent Jepsen 覆盖方案（v0.2 —— **已落地**）

> 日期：2026-09-18 ｜ 基线：coord @ `4a5e3ee`
>
> **v0.2 状态（2026-09-18，同日落地）**：本文的建议已**落成代码**并在 docker lab
> 真跑，产出两条 coord 侧实质缺陷（**F-28** fencing 缺失 / **F-32** agent 无 root
> 能力旁路，均为 P0 且已复现）与一条未闭环的 P0 候选（**F-34** 互斥重叠，已附
> 决定性实验设计）。逐项交付状态见文末 §11；缺陷单见 `coord-findings.md` §14。
>
> 定位：这份文档回答一个问题 —— **dev.md 的 M5 把 coord-agent 当成"最后一个面上的
> 一个里程碑"，但 coord-agent 已经长成"第二个被测系统"。** 本文按 coord-agent 自己的
> 愿景（`docs/coord-agent-plugin-engine-plan-2026-09-09.md`）与承诺台账
> （`apis/contracts/STATUS.md`）反推：jepsen 该覆盖什么、不该覆盖什么、怎么落地、
> 需要对方书面确认什么。
>
> 配套：`dev.md`（v3.5，M5 现状）、`coverage-gaps-and-implementation-plan.md`
> （G-01…G-18）、`coord-findings.md`（F-01…F-34）、`soak-closure-report.md`。
> 本文**是建议稿**：M5 的正式修订落在 `dev.md`（合并清单见 §9），在双方确认前不改
> 动现有排期与验收口径。

---

## 0. 摘要（给决策层）

1. **coord-agent 的进程内测试已经很厚，jepsen 的补位不是"再测一遍"。**
   插件引擎侧已有 `agent_plugin_js_test` / `agent_plugin_component_test`（16 项，
   含无 WASI 断言、fuel/epoch 隔离、内存上限、作用域守卫 fail-closed）、
   `plugin/abi.rs` 三条 ABI 路径账本、`coord/tests/plugin_auth_multinode_process_test.rs`
   （双节点 raft 重启后授权存续）。这些**必须继续留在进程内**（沙箱逃逸、ABI 一致性、
   typed 错误语义在黑盒下不可判）。jepsen 唯一不可替代的价值是四条：
   **多进程/多实例拓扑、故障与竞态时序、可回放的失败历史、以及"直连 vs 经 agent"的
   差分归因**。

2. **现有 M5（T5.1–T5.7，6.5 人日）在三处与 agent 现状脱节**（§2/§3）：
   - **没有"多 agent 拓扑"**。而 lock / election / registry 的互斥、唯一性、幽灵实例
     语义**在单 agent 下结构性不可判** —— 单 agent 下只能测到本地缓存
     （`lock.rs` 的 `LockCache`、`registry.rs` 的"本地缓存全量注册表"），测不到授权
     权威（server 侧 Txn CAS / Lease）。现有 T5.3/T5.4 没有说明要起几个 agent。
   - **缺两条已入台账的能力**：`idgen`（COMMITTED，GA **2026-10-31**，是 agent 面里
     最早的硬截止）与 `event`（2026-12-31）在 dev.md 覆盖矩阵里**没有行**；
     `coverage-gaps` 里 idgen 只是 P2。**承诺期限与覆盖排期倒挂**（idgen 最早截止、
     排得最晚）。
   - **没有安全/信任面的对抗性验证**：断连时的降级语义（fail-open 还是 fail-closed）、
     插件 CCT 失效后是否回退到共享客户端（= 权限放大）、网关 `Deny` 是否**真的**阻断
     后端、SIGHUP 热重载窗口期拦截是否失守。这是 agent 引入路径上唯一的高危抽象
     泄漏点，且都属于"只有故障注入 + 黑盒才能证伪"的类别。

3. **两条可立刻执行的结构性建议**（§4/§8）：
   - **`--via-agent` 做成已有 workload 的复用开关，而不是新 workload**：agent 只是
     传输路径，`mapck`/`txnck`/`scanck`/`watchck`/`leaseck` **一行不改**就能判定；
     同时必须新增"这次 run 真的经过 agent 了吗"的**路由证明**判据（否则客户端连错
     地址 = 直连假绿，属 F-13 同类）。
   - **M5 前移（与 M4 并行），并拆成 M5a/M5b**：T6.1 的 72h 组合浸泡卡在
     lock/election/registry 三个面上（`--soak-mix` 里一出现就构造期硬失败），
     而 M5 现在排在关键路径尾部 ⇒ 72h 无法在承诺窗口内起跑。M5 前移是**唯一**能让
     72h 按 W7 起跑的办法。

---

## 1. 分工边界：jepsen 在 coord-agent 上该做什么、不该做什么

| 验证对象 | 判定手段 | 为什么 |
|:--|:--|:--|
| 沙箱（fuel/epoch/内存上限/无 WASI）、trap 隔离 | **进程内**（已有 16 项） | 黑盒断言不出发动机内部状态；进程内可直接断言"逃逸确实失败" |
| ABI 一致性（WIT ⟷ core ⟷ JS）、typed 错误名/码 | **进程内 + CI 账本**（`plugin/abi.rs`） | 结构一致性，不是运行时属性 |
| 单进程生命周期（load/init/start/stop/unload）、SIGHUP 应用 diff | **进程内** | 无故障叠加时黑盒无增益 |
| **插件/服务的"本地真相 vs 服务端真相"分裂** | **jepsen** | 需要 agent 进程与 server 集群**真实断连**、真实时钟、真实 leader 切割 |
| **多 agent 实例的互斥/唯一性**（lock/election/idgen/registry） | **jepsen** | 单进程测试里不存在"第二个实例"，语义不可判 |
| **经 agent 的读写/流式一致性**（差分） | **jepsen** | 需要外部客户端时序 + 可回放历史 + 直连对照 |
| **进程被 kill 后的资源泄漏**（插件持有的 lease / 订阅 / 句柄） | **jepsen**（独占） | 进程内测试无法杀死自己的宿主；必须从 server 侧可观测状态反证 |
| **安全边界的 fail-closed**（scope、CCT 过期、网关拒绝、降级回退） | **jepsen** | 需要在"判据不可用"（server 不可达/授权未同步）时观察是否**拒绝**而不是放行 |

> 关键取舍：**不重复已有进程内断言**。重复建设的代价不是工时，而是**新增假绿面** ——
> 同一承诺被两处"覆盖"，报告里看起来更绿，实际两边都只测了一半。

---

## 2. 现状覆盖矩阵（agent 面）

来源：`dev.md` §1（A1–G5 缺口表）、`apis/contracts/STATUS.md`、以及本轮对
`coord-agent/src/services/*`、`plugin/*` 的源码阅读。

| 侧 | 生产承诺（锚点） | 我读到的源码事实 | jepsen 现状 | 缺口 |
|:--|:--|:--|:--|:--|
| **代理传输面**（KV/Txn/Range/Delete/Watch/Lease 经 agent） | 契约 STABLE 面经 agent 后语义不变 | 6 个代理服务；Watch/KeepAlive/StorageGet 为 mpsc 流桥（`proxy.rs:465/551/898`） | `--via-agent` **不存在**（`coord.clj` 里只有注释提到 M5） | AG-01 |
| **lock** `coord.lock.v1` | 同一锁名同一时刻至多一个持有者；TTL 未 Renew 自动释放；仅 (holder_id, lease_id) 匹配者可 Release | `acquire` = server 侧 `Txn Compare{Version==0}` + `Put(lease_id)`（**权威在 server**），本地 `LockCache` 是镜像；`lock.rs:187-198` 记录了历史 P0：曾以**本地墙钟** `is_expired()` 清理 → 假丢锁 → **临界区重入**，已删除该路径 | 零 | AG-02 / AG-03 |
| **election** `coord.election.v1` | 同 group 同一时刻至多一个 leader；TTL 无续约自动过期并广播 | Lease + Watch + Txn；实例身份随机；`leader_election.rs:111` 记录了第四轮 P0（重复 campaign 的例外分支判据） | 零 | AG-02 / AG-10 |
| **registry** `coord.registry.v1` | 注册/心跳/发现；重复注册幂等 | `registry.rs:7-9` 明写"本地缓存全量注册表（延迟 <1ms）…**与 Server 断连时保留最后已知实例快照（自我保护）**" | 零 | AG-07 |
| **idgen** `coord.idgen.v1` | 同名发号器 ID 全局唯一；台账要求"时钟回拨防护落地或明确不承诺边界" | 默认 snowflake（10bit nodeid，**离线可用**；nodeid = `COORD_NODE_ID` > 主机名稳定哈希；启动期在 `/_idgen/nodes/{nodeid}` CAS 注册、冲突顺延、重启保持）；segment 模式 opt-in（本地号段 + server Txn CAS） | 零；`coverage-gaps` 记 P2 | AG-08 |
| **cache / mq / event**（EXPERIMENTAL，GA 2026-12-31） | cache：ISR 原子提交 + 分区 Leader 故障转移；mq：背压不丢 + at-least-once；event：订阅 ID 显式下发 | cache = 本地 redb + `expires_at` 内嵌值 + 复制表/幂等键/本地序列号同事务 | 零 | AG-09 / AG-11 |
| **storage 经 agent** | 分块流式 put/get，`total_size=-1` 未知长度；GC/水位 | agent 侧流桥 + 服务端 choreography（`docs/production/volume-object-storage.md`） | 零（`object_storage_process_test.rs` 为进程内） | AG-12 |
| **插件引擎 / 网关 / 身份** | 分层信任；网关可拒绝；插件 = `plugin/{id}` 受限 CCT + capability/scope | `PluginGatewayLayer`（Deny → status，观测模式，计数）；`identity.rs` 头注释有**降级语义**（开通失败 → 回退） | 零（全在进程内） | AG-04 / AG-05 / AG-06 / AG-13 |
| **可观测面**（metrics/health） | 运维判据 | `coord_agent_plugin_*` 等指标 + `/metrics` | 未用作判据 | AG-14 |

---

## 3. 缺口清单

> 严重度：**P0** = agent 引入路径上的承诺且无对抗性验证，阻塞引入；**P1** = 未验证的
> 契约条款；**P2** = 故障模型/拓扑缺口。

### AG-01 [P0] `--via-agent` 差分基线不存在（且没有"确实经过 agent"的判据）

- **锚点**：dev.md §4 T5.2 计划"T5.1 完成立即 2h `--via-agent` 冒烟"，但 `coord.clj`
  的 `cli-opts` 与 `client-gen` 里**没有任何 agent 路径**；`client.clj` 只连 server。
- **缺陷假设**：无法区分"agent 引入的语义变化"与"coord 自身缺陷"。
- **判据**：同一 workload、同一种子跑两路（`--direct` 对照 / `--via-agent`），
  **归因规则**：direct 红 ⇒ 先修 server 面，该 run **不得**用于给 agent 定罪；
  direct 绿 + via-agent 红 ⇒ 红必属 agent 合成层。另加**路由证明**：
  run 前后 agent 侧 `coord_agent*_total` 增量 > 0，且该判据进 `:valid?`
  （不然连错地址 = 直连假绿）。

### AG-02 [P0] 多 agent 拓扑缺失 ⇒ 互斥/唯一性结构性不可判

- **锚点**：`lock.rs` 本地 `LockCache` + `/ _lock/{name}` 服务端 key；`leader_election.rs`
  实例身份仅"每进程唯一"。
- **缺陷假设**：两个 agent 各自的本地缓存各持一份真相；分区/重连窗口内"双方都认为
  自己持有"（临界区重入）；heal 后没有唯一的收敛判据。
- **判据**：≥2 个 agent 同时竞争同一锁名/同一选举组；**重叠持有 > 时钟容差 = 0**；
  非持有者 Release/Renew 必须 `PERMISSION_DENIED`；heal 后 T 秒内收敛到唯一持有者。
- **依赖**：AG-01 的差分骨架。

### AG-03 [P0] 断连降级语义（fail-open / fail-closed）零验证

- **锚点**：`registry.rs` 的"自我保护快照"；`lock.rs` 的 C4 `renew_action`
  （`Err(_)` → **Keep**，fail-safe）；`identity.rs` 的降级回退。
- **缺陷假设**：agent 与 server 断连时，本地服务**继续服务**（可用性优先），于是
  出现"分区期间陈旧读 / 幽灵实例 / 本地认为持锁 / 越界放行"。
- **判据（成对，缺一不可）**：①**可用性**——按书面定义判定（可以降级，但降级范围
  必须写下来）；②**不得放大**——分区期间 scope 越界、CCT 过期、未知 RPC 必须仍然
  被拒（fail-closed）。只测①会得到"分区期间一切正常"的假绿（F-13 同类，见 §6）。

### AG-04 [P0] 网关拒绝未被证明"真阻断"，热重载窗口未验证

- **锚点**：`gateway.rs` 的 `GatewayDecision::Deny` → HTTP status；`PluginGateway`
  有 `requests_total` / `denied_total` / `path_count`。
- **缺陷假设**：拒绝发生在 router 之后（后端已执行）、拒绝只影响响应而不影响副作用；
  SIGHUP 应用插件集 diff 期间出现"窗口期无拦截"。
- **判据**：`Deny` 后**后端状态必须不变**（用写请求 + server 侧 revision/值断言，
  不能只看返回码）；重载前后各注入一次必须拒绝的请求，**两次都必须红**（证明窗口期
  没有失守）；`denied_total` 与业务侧观测计数一致。

### AG-05 [P0] CCT 失效 / 续期失败的回退路径不得降级为共享客户端或匿名

- **锚点**：`identity.rs`（bootstrap CCT → 插件账户 → 15min CCT + 24h refresh 单次
  使用；失败回退密码重认证）；插件计划 §19-M1 注"默认回退共享客户端"。
- **缺陷假设**：server 重启/授权未同步/refresh 失效时，agent 用**共享客户端**继续调
  server ⇒ 插件越权（权限放大）；或调用静默失败被当成"业务无锁"。
- **判据**：①制造 CCT 失效（过期/伪造 refresh/服务端撤销）后，插件调用必须**被拒**
  且可观测（不得成功）；②不得出现"以更高身份的凭据成功"（用 server 侧审计/角色断言）；
  ③失败必须显式（插件侧 typed 错误，不得返回伪成功）。

### AG-06 [P0] 插件持有的 lease / 锁在 agent 被 kill 后必须回收（跨进程泄漏）

- **锚点**：`stdlib.js` 的 `_acquireLeaseKey` / `_lockHandle` / keepAlive 句柄；
  `component_engine.rs` 的 `resource subscription`（drop 即释放）；agent kill = 不执行
  drop。
- **缺陷假设**：agent 进程被 `kill -9` 后，插件申请的 lease 需等 TTL 到期（可接受，
  须有上界）还是**永久残留**（锁 key 泄漏 ⇒ 死锁）；订阅/句柄表在 server 侧残留。
- **判据**：`kill-agent` 后，**从 server 侧**断言 lease 在 `ttl + grace` 内消失、
  锁 key 被级联删除、订阅被清理；`restart-agent-keep-data` 后不得出现"复活但状态
  混乱"（本地持久化与 server 真相不一致）。
- **为什么必须 jepsen**：进程内测试无法杀死自己的宿主。

### AG-07 [P1] registry 的"自我保护快照"= **刻意陈旧**，须书面定义上界

- **锚点**：`registry.rs:8`「与 Server 断连时保留最后已知实例快照（自我保护）」。
- **缺陷假设**：幽灵实例（lease 已过期但仍在其他 agent 的缓存里被返回）持续到
  heal 之后；或 TTL 到期实例在健康的 agent 上仍被发现。
- **判据（先按"分区期允许陈旧、heal 后必须收敛"设计，上界进 §7 待确认）**：
  ①健康状态下：LEASE 过期实例在发现接口中消失，误差 ≤ 书面超时；
  ②分区期间：允许返回最后快照（不得视为违约），但**必须**在 API 语义里可区分（或
  在契约里写清），否则上层会把陈旧当成真相；
  ③heal 后 X 秒内幽灵实例 = 0。

### AG-08 [P0 若引入发号 / P1 否则] idgen 唯一性与时钟回拨

- **锚点**：`idgen.rs:5-11`（snowflake 默认、离线可用、nodeid 来源优先级、启动期
  `/_idgen/nodes/{nodeid}` CAS 注册与冲突顺延）；`STATUS.md` idgen GA **2026-10-31**，
  整改要点"时钟回拨防护落地或明确不承诺边界"。
- **缺陷假设**（按可证伪性排序）：
  ①**nodeid 冲突** —— 默认 nodeid 回退到"主机名稳定哈希"，两台同名主机 / 容器主机名
  重复 / 迁移后主机名变化 ⇒ 同一 nodeid、同毫秒同 seq ⇒ **重复 ID**。这是生产最易
  触发的形态，而**单进程测试结构性测不到**；
  ②**时钟回拨** —— snowflake 依赖墙钟；回拨窗口内发重号（台账明确要求"落地或明确
  不承诺边界"）；
  ③segment 模式 leader 切换后号段重叠（本地缓存 + server CAS）。
- **判据**：跨 2 个 agent + 跨重启的**全局唯一性**（含刻意制造 nodeid 冲突）；
  单调性/趋势递增按契约措辞判定；时钟 nemesis 下要么唯一性不破，要么**契约明确
  声明不承诺**并把边界写成可复现的判据。

### AG-09 [P1] cache 本地持久化 + 复制的 TTL / 重连 / 一致性

- **锚点**：`cache.rs`（redb 四表 + `encode_ttl` 绝对到期时间戳 + 复制表/幂等键/本地
  序列号同事务）；`STATUS.md`：「ISR 原子提交 + 分区 Leader 故障转移」。
- **判据**：TTL 不得**提前**消失（契约只承诺"不晚于/不早于"中的一侧，须确认）；
  agent kill + 重启后本地序列号/幂等键与 server 真相一致（不得重复应用）；
  分区期间是否降级为不一致读——若承诺线性一致则 P0。

### AG-10 [P1] election：唯一 leader + 续约语义 + resign 后不可见

- **锚点**：`leader_election.rs:111`（第四轮 P0 的例外分支判据）；`STATUS.md` 整改
  要点"续约（重新 Campaign）语义验证"。
- **判据**：同 group **同时双 leader = 0**；TTL 无续约 ⇒ 自动过期；resign 后
  `GetLeader` 不得再返回旧 leader；agent pause/kill 后旧 leader 必须过期（不得因本地
  缓存存活而永久在位）。

### AG-11 [P2] event / mq 的 at-least-once

- **判据**：订阅端**不得丢**（缺口出现在消息序号上且无显式错误 = 红）；dup 允许但
  必须幂等（重复消息的业务影响 = 0）；背压下不得静默丢弃。

### AG-12 [P1] storage 经 agent 的流式代理

- **缺陷假设**：agent 在流中途被 kill ⇒ 半成品对象**可见**（读到不完整对象 = P0）；
  未知长度（`total_size=-1`）路径在 agent 重试后重复 commit；4MiB 绕行边界；
  快照恢复落后节点的 chunk rebuild 期间 get 的可观测行为（STATUS 已列为已知边界）。
- **判据**：半成品不可见（get 必须 not-found / 显式错误）；中断后重试的对象大小与
  内容 sha 必须等于写入内容；rebuild 窗口内的行为要么成功要么显式 UNAVAILABLE。

### AG-13 [P1] 凭据与身份的持久化 / 轮换语义

- **锚点**：`coord/src/credentials.rs`（落盘 0600、自动续期、logout 幂等）；
  `coord-agent/src/plugin/identity.rs`（provisioner 持久开通凭据）。
- **判据**：`restart-agent-keep-data` 后凭据可复用且**不越权**；refresh 单次使用
  （重放必须失败）；伪造 refresh fail-closed；轮换期间业务调用不中断也不放大。

### AG-14 [P2] 可观测面作为判据

- 把 agent metrics（`coord_agent_plugin_*`、网关 `denied_total`、代理计数）纳入
  checker 可见字段，作为 AG-01 路由证明与 AG-04 计数一致性判据的来源。
  **注意**：指标只能做"证明某事发生过"，不得作为"证明某事没发生"的判据。

---

## 4. lab 与客户端改造

### 4.1 成本比预期低：agent 就是同一个 `coord` 二进制

源码锚点 `coord/src/main.rs:1656`（`Commands::Agent` 分支：`--agent-addr` /
`--http-addr` / `--discovery` / `--server-addrs` / `--agent-config`），配置加载
`coord_agent::AgentConfig::from_file`（fail-closed）。

```bash
# lab 侧（每台 agent 节点）
coord agent --agent-config /root/coord-test/agent-a1.toml \
            --agent-addr 0.0.0.0:19527 \
            --server-addrs n1:8080,n2:8080,n3:8080
```

⇒ **不需要新产物、不需要新镜像**：lab 只是多一个进程 + 一份 TOML + 一个 pidfile，
`db.clj` 的 `Kill` 协议原样复用（新 pidfile 命名）。这比 dev.md 里 T5.1 的 1.5 人日
估算要便宜。

### 4.2 凭据接线（已有 CLI，lab setup 里自动做一次）

server 侧 `[security].agent_bootstrap_tokens`（≥16 字符一次性）+
`coord security bootstrap-role`（幂等，授予 agent 引导角色最小能力集）；
agent 侧 `[auth].bootstrap_token`。缺 `bootstrap_token` 时 agent 会**fail-closed 拒绝**
（`lib.rs` 的 `validate_auth_key_material`）—— 这条本身要有一条落绿 fixture，
不然 lab 里会出现"agent 全被拒但报告看着像业务失败"。

### 4.3 拓扑

| 拓扑 | 节点 | 用途 |
|:--|:--|:--|
| T-A | n1..n3 + a1 | 差分基线（AG-01）、代理传输面、storage 代理 |
| T-B | n1..n3 + a1,a2 | 互斥/唯一性/选举（AG-02/AG-08/AG-10）、多 agent 差分 |
| T-C | n1..n3 + a1,a2，agent 与 server 同机（不同端口/data_dir） | 生产 daemonset 形态；同机故障的耦合（kill 同机 = agent+server 一起死） |

### 4.4 nemesis 扩展（复用 `nemesis.clj` 的结构）

| nemesis | 语义 | 服务的缺口 |
|:--|:--|:--|
| `:kill-agent` | `kill -9` 单个 agent（不重启） | AG-06、AG-09 |
| `:kill-agent-all` | 全部 agent | AG-03（降级边界） |
| `:restart-agent-keep-data` | 保留 `data_dir` 重启 | AG-06、AG-13 |
| `:pause-agent` | SIGSTOP agent | AG-02（本地不推进但客户端仍连着） |
| `:partition-agent-server` | agent ↔ 全部 server 断（客户端 → agent 仍通） | **AG-03 核心**：降级语义 |
| `:partition-agent-split` | a1 只连 n1、a2 只连 n2/n3（制造两个"真相源"） | AG-02、AG-10（双持有/双 leader） |
| `:reload-agent` | SIGHUP（插件集 diff） | AG-04 窗口期 |
| `:clock-agent` | 墙钟 ±N 分钟（仅 agent 侧） | AG-08（snowflake/CCT）；**对 server lease 无效**（F-08 已判单调钟） |

### 4.5 客户端与差分运行

- `--via-agent` = 把 endpoint 换成 agent 地址；**workload 生成器不动**。
- `--via-agent-multi a1,a2` = 按 op 轮转/哈希到不同 agent（互斥类 workload 必需）。
- 差分矩阵（**不跑全矩阵**，只跑核心 4 面 × 2 nemesis，控制机器时间）：

| workload | direct | via-agent(a1) | via-agent-multi | nemesis |
|:--|:--:|:--:|:--:|:--|
| map | ✅ | ✅ | — | none, kill-server |
| txn | ✅ | ✅ | — | none, kill-server |
| watch | ✅ | ✅ | — | none, kill-agent |
| lease | ✅ | ✅ | — | none, partition-agent-server |

---

## 5. checker 设计

### 5.1 复用优先（不新造判据）

agent 只是传输路径 ⇒ `mapck` / `txnck` / `scanck` / `watchck` / `leaseck` **零改动**
即可判定 via-agent 路径（这正是 §6 里"不得为通过而改断言"的反面：也不得为新路径造
第二套判据）。

### 5.2 新 checker（4 个，各配 ≥2 个负控制 fixture + 漏检边界声明）

| checker | 判据 | 漏检边界（R2 声明） |
|:--|:--|:--|
| `jepsen.coord.lockck` | 跨 agent 重叠持有 > 容差 = 0；非持有者 Release/Renew 必须拒；TTL+grace 内活锁消失；fencing 值（若存在）单调 | 不判公平性/可重入（契约不承诺）；不判服务器内部锁状态机 |
| `jepsen.coord.electck` | 同 group 双 leader = 0；resign 后不可见；TTL 无续约必过期 | 不判公平选举顺序；不判 callback 投递次数 |
| `jepsen.coord.regck` | 健康态幽灵实例 = 0；heal 后 X 秒收敛；重复注册幂等 | 不判发现延迟 P99（只判上界是否存在） |
| `jepsen.coord.idgenck` | 跨 agent/跨重启**全局唯一**；单调/趋势按契约措辞；回拨边界与书面口径一致 | 不判 ID 的分布均匀性；不判 snowflake 内部位布局 |

### 5.3 fixture 与门禁

- 全部 fixture 进 `make checkers`（现 17 套 99 个 → 预计 +4 套 / +10 个）；
- 每 checker 一次**变异校验**（真实绿历史手工注入该 checker 负责的缺陷必须抓红）；
- 门槛参数一律用 `or` 解析（F-09），禁 destructuring `:or`。

---

## 6. 判据与假绿防线

沿用 `dev.md` §5.5 全部 10 条（尤其 5「这个 workload 真的跑了吗」、9「新接一个面的
第一跑：先看计数，再看 `:valid?`」）。**新增 4 条 agent 专属**：

11. **必须证明"这次 run 真的经过了 agent"**（路由证明）。判据进 `:valid?`：
    agent 侧代理计数增量 > 0。反例形态：客户端 endpoint 写错 ⇒ 直连 ⇒ 报告与 direct
    完全一致、全绿，而 agent 一行代码没执行。
12. **差分归因不得混淆**：direct 红的 run 不能用来给 agent 定罪；两个 run 必须同种子；
    证据里必须同时归档 direct 与 via-agent 的 MANIFEST 供交叉核对。
13. **降级路径必须成对判据**：agent 的每一处"自我保护/降级"（registry 保留快照、
    锁续期 `Err → Keep`、开通失败回退、缓存兜底）都同时需要①可用性判据
    ②不得放大权限 / 不得静默陈旧的判据。只写①会产出"分区期间一切正常"的假绿。
14. **"进程 kill 才能测的泄漏"必须从 server 侧反证**：断言对象是 server 可观测状态
    （lease 是否仍在、锁 key 是否仍存在、订阅是否被清理），而不是 agent 自己的日志。
15. **agent 自述的状态必须有一条绕开 agent 的地面真值通道**（2026-09-18 第二轮，
    由 F-34 的分诊倒逼）。原文第 14 条只要求 kill 类泄漏从 server 侧反证，F-34 说明
    这个要求要**一般化**：agent 汇报的任何「我持有了 / 我注册了 / 我发号了」都可能
    与 server 真相不一致，而**只在 agent 侧取样是分诊不了的** —— 同一批自述在
    「真违约」「agent 汇报层不一致」「度量本身有偏差」三种解释下长得一样。
    落地：`lock` 面常驻 `:f :lock-probe`（server 端点直接读 `/_lock/{name}`），且
    **探针缺失 ⇒ 判未执行**（不是绿）。每条 agent 自述类判据交付时都要能回答
    「这条判据的证据，除了 agent 自己，还有谁说过话？」
16. **区间/时序类判据必须自带口径对照与独立估计**（F-34 三层根因）。细节见
    `dev.md` §5.5 第 13/16 条。要点：锚点用 op 自读的 `System/nanoTime`；闭合判据
    带所有者身份；交付时给出「去掉本判据的锚/闭合逻辑会得到什么数」。

---

## 7. 待 coord-agent 团队确认（书面，链接进 MANIFEST）

> 规则同 `dev.md` §5.4：**默认值仅限开发迭代；验收级 run 必须使用书面确认的取值**。
> 这 10 条里有 5 条是"当前实现事实上已经选了一边，但没人写下来"——这是本方案认为
> 最有价值的倒逼产出（比多跑几个红 run 更值）。

| # | 待确认语义 | 现状（源码事实） | 为什么必须书面 |
|:--|:--|:--|:--|
| ① | agent 与 server 断连时，**每个本地服务**的承诺：fail-stop / 本地继续（陈旧上界？） | 未在任何文档中统一声明；`registry` 明确"自我保护"、lock 续期明确 fail-safe Keep | 决定 AG-03/AG-07 的判据方向；不写就会"测试自定义严格度" |
| ② | registry 快照允许的 **staleness 上界** 与幽灵实例容忍窗口 X | 源码只有"保留最后已知快照"，无数字 | 判据需要一个数；未确认则 AG-07 无法验收 |
| ③ | lock 的**权威**与 fencing：到期判定以 server 单调钟还是 agent 墙钟？是否存在 fencing token？跨 agent 可比吗？ | acquire 权威在 server（Txn CAS + Lease）；`is_expired`（墙钟）已从清理路径删除，仅作续期调度提示 | `dev.md` §5.4-①（500ms 容差）需要这条才能定；否则跨机时钟差直接导致双持有 |
| ④ | lock / election 的 TTL、grace、续期节拍 | `--lease-tolerance-ms` 等参数尚未定 | 与 §5.4-⑤（lease grace = 2×ttl）对齐 |
| ⑤ | idgen：nodeid 冲突的**承诺**（同名主机/容器主机名重复时会发生什么）与时钟回拨边界 | 默认 snowflake 离线可用；nodeid = 显式 > 主机名哈希 | AG-08 的 P0/P1 定级取决于此；台账已把"时钟回拨防护"列为整改要点 |
| ⑥ | 插件 CCT 失效/续期失败时的**降级**语义（是否允许回退共享客户端？若允许，权限如何不放大？） | `identity.rs` 头注释有降级语义；插件计划 M1 提"默认回退共享客户端" | AG-05 是 P0 安全项，必须 fail-closed 或书面豁免 |
| ⑦ | SIGHUP 热重载期间的 in-flight 请求与拦截点语义（原子性？窗口期？） | 文档只说"仅应用插件集 diff" | AG-04 的窗口期判据 |
| ⑧ | cache 的一致性承诺（线性一致 / 会话 / 最终）与 ISR 复制因子、分区期读写语义 | `STATUS.md`「ISR 原子提交 + 分区 Leader 故障转移」 | AG-09 的判据强度 |
| ⑨ | storage 经 agent 的边界：半成品可见性、未知长度重试的 commit 幂等、快照恢复期行为 | `STATUS.md` 已列"已知边界"但非判据口径 | AG-12 |
| ⑩ | agent 侧可依赖的**判据来源**（metrics 字段名与语义、health 定义） | 已有 `coord_agent_*` / `coord_agent_plugin_*` | AG-01 路由证明、AG-14 |

---

## 8. 排期与机器时间建议

### 8.1 建议拆成 M5a / M5b

| 里程碑 | 任务 | 人日 |
|:--|:--|:--|
| **M5a 传输面 + 安全面**（阻塞生产引入） | TA1 lab agent 接线（0.5）· TA2 差分运行 + 路由证明（1）· TA3 lock 双 agent（1.5）· TA4 election 双 agent（1）· TA5 安全面四判据（AG-03/04/05/06）（2）· TA6 idgen 唯一性（1）· TA7 收口 8h（0.5） | **7.5** |
| **M5b 本地语义面 + 流式面** | TB1 registry（1）· TB2 cache（1）· TB3 event/mq（0.5）· TB4 storage 经 agent（1.5）· TB5 凭据/身份（0.5）· TB6 可观测面纳入判据（0.5）· TB7 收口 24h（0.5） | **5.5** |
| 合计 | （原 M5 = 6.5d；增量 6.5d 来自：idgen、event、安全面、storage 代理、双 agent 拓扑 —— 都是原计划缺项） | **13** |

### 8.2 关键路径修正（重要）

现状（`dev.md` §2）：`... T5.1 → T5.2 → T5.6 → T6.0 → T6.1`，而 T6.1 的
`--soak-mix` 一旦包含 lock/election/registry 就**构造期硬失败** ⇒ **M5 延误 = 72h 顺延**。
建议：**M5a 前移到 W5（与 M4 并行）**，M5b 放 W6。这样 W7 的 72h 起跑不再依赖
"M5 恰好按时完成"，而是依赖一个已经跑过 8h 的 M5a。

### 8.3 机器时间

| 类别 | 明细 | 时长 |
|:--|:--|:--|
| 短矩阵（差分对） | 4 workload × 2 路 × 2 nemesis，45–90s/组合 | 不计入主表（§7.2 额度内） |
| M5a 收口 | 8h（多 agent 组合，T-A + T-B） | 8h |
| M5b 冒烟 / 收口 | 2h agent 冒烟 + 24h（原 T5.6） | 26h |
| **计划内增量** | 相对原 M5 的 2h + 24h | **+8h** |

---

## 9. 与 `dev.md` 的合并清单（本文档获批后执行）

1. **§1 覆盖矩阵**：新增 D1 拆分（传输面差分）；新增两行 **IdGen**（AG-08）与
   **Event/MQ**（AG-11）；D2 补注"需 ≥2 个 agent"；新增 **Agent 安全面**
   （AG-03/04/05/06）一行。
2. **§2 依赖图**：`T5.1 → T5.2` 之后拆 `T5a.*`/`T5b.*`；标注
   `T5a 前移与 M4 并行`；`T6.1 ⇐ T5b` 的硬前置关系写显式。
3. **§3.1 工时**：M5 由 6.5 → 13 人日；总量按 §0 的口径重算（对外承诺区间需重签）。
4. **§4 M5 章**：用本文 §3/§5 重写；T5.3/T5.4 必须写明"≥2 agent 拓扑"。
5. **§5.1 短矩阵表**：补 `lock`/`election`/`registry`/`idgen` 四行的门槛数值
   （现表已有 lock/election/registry 三行的判据，但**没有 idgen 行**，补上）。
6. **§5.4 参数表**：并入本文 §7 的 ①③④⑦（其余归 M5b）。
7. **§7.1 排期表**：W5 加入 M5a，W6 由 M5b 取代原 M5；W7 的 2h agent 冒烟取消
   （已在 M5b 内）。
8. **§8 延期项**：目前没有 agent 项；若 §7 的某些语义双方决定"不承诺"，则按
   P0 双签 + ≤90 天有效期登记到 `docs/production/remaining-known-gaps.md`。
9. **§9 倒逼清单**：追加本文 §7 的 10 条（每条带"已答/待答"状态位，与现有第 1–8 条
   同格式）。

---

## 10. 风险与"不做"清单

| 风险 | 缓解 |
|:--|:--|
| 差分运行把机器时间翻倍 | 只对 4 个核心 workload 做差分对（§4.5），不做全矩阵 |
| agent 侧"降级"是**刻意设计**，贸然判红会产出假红 | §7 的 10 条先书面确认；未确认前按"记录 + 存疑"处理，不写入验收门禁 |
| 多 agent 拓扑让 lab 复杂度上升（端口/凭据/data_dir 隔离） | 用 T-C（同机不同端口）做日常开发，T-B 只在矩阵/长跑用 |
| 与进程内测试重复建设 | §1 的分工表作为评审卡口：新 case 必须先回答"为什么进程内测不到" |
| idgen P0/P1 定级影响承诺 | 由引入用途决定（是否用发号做业务主键）；§7-⑤ 未答前按 P1 排期，但**不晚于 2026-10-31** |
| **不做**：沙箱逃逸、ABI 一致性、SDK typed 错误、单进程生命周期、SIGHUP diff 逻辑本身 | 留在进程内/CI；jepsen 只在它们**与故障叠加**时介入（如 reload × 走量） |

---

## 11. 落地状态（v0.2，2026-09-18）

| 项 | 内容 | 状态 | 证据 |
|:--|:--|:--|:--|
| TA1 agent 部署 | `jepsen/src/jepsen/coord/agent.clj`：同一 `coord` 二进制（`coord agent`）、独立路径、loopback 绑定 + 控制机 SSH 隧道、run 级随机隧道端口、功能验证（经隧道 GET /metrics） | ✅ | `make test ... AGENTS=2` 真跑：agent 起在 n4/n5，隧道端到端验证通过 |
| TA1b 鉴权接线 | bootstrap token → `agent-bootstrap` 角色；**Ed25519 公钥**（`scripts/derive-cct-pubkey.py` 派生 + lab 常量 + `--agent-verifying-key` 覆盖）；21 个 capability 显式授给 root（F-32 的 lab 处置） | ✅ | 首轮卡住的三次失败就是这个顺序上的三个坑（见 findings §14） |
| TA2 wire 层 | `CoordRpc.java` 新增 `coord.agent.{Lock,IdGen,LeaderElection,Registry}` 共 13 个方法；`proto.clj` 请求构造/响应读取 | ✅ | `scripts/check-agent-wire.clj`（方法名/字段号与 `agent_api.proto` **原文**对比） |
| TA2b 路由证明 | `routing-proof!` + gates 的 `:agent-route-not-proven` 门槛（计数为 0 必红）；本地面走 `:agent-local-surface?` 分支 | ✅ | `gates-agent-fixtures{,-ok}` 两个方向 |
| TA3–TA6 四个面 | `--workload lock / election / idgen / registry` + `lockck / electck / idgenck / regck` | ✅ 代码 + 离线 fixture | 20 个 fixture 全绿；lab 四个面见 §11b |
| TA8 **服务端地面真值探针** | `:f :lock-probe`（`client.clj` 的 `try-probe` 走 **server** 端点，直接 `KV Range` `/_lock/{name}`）+ checker 判据 5（`lock-server-truth-contradiction`，边界容差 100ms，**探针缺失判未执行**）+ 生成器 1/5 槽位 | ✅ | `lock-probe-fixtures` 4 个 fixture（正/负/边界/缺失）；真跑 28 样本 0 读失败 0 硬违约 |
| TA9 分诊工具 | `scripts/lock-diag.clj`：同一份历史跑四种区间口径（`:f34` / `:legacy` / `:new` / `:true-end`），把「度量缺陷」与「真违约」分开 | ✅ | F-34：老历史 `:f34=:new=99`、`:true-end=0`；修后 `43 → 3 → 0` |
| TA10 四个面的判据口径统一 | **每个面都必须做一遍**的区间动作（F-35 是漏做 election 的代价）：op 记 `:t0-ns`、checker 用 `abs-ms`、闭合用「已不在我名下」的证据 | ✅ lock（`:gone-at-ms`）/ election（`:gone-at-ms` + `abs-ms`）；registry/idgen 无区间判据 | `lock-fixtures`(10) / `elect-fixtures`(6) / `lock-probe-fixtures`(4) 全绿 |
| TA11 agent nemesis 契约 | `completion` 包装：`invoke!` 返回 op map（原来返回向量 ⇒ 一轮 86 条 `invalid-completion`，F-36） | ✅ | 见 `coord-findings.md` §15 F-36 |
| TA12 路由证明的口径 | run 期间**多次采样**：`:up?`（曾经）/`:up-now?`（现在）/`:total`（各次最大值）；本地面门槛改为「至少一个 agent 曾经在」（F-37） | ✅ | `gates-agent-local-fixtures{,-ok}` 两向 fixture |
| TA13 **AG-06 崩溃持有者的回收** | `jepsen.coord.faultwin`（nemesis 时间窗）+ `:f :lock-abandon`（弃锁）+ `lockck` 判据 6/7（回收 / 不得假丢锁）+ 门槛「判过 ≠ 违反」 | ✅ | `lock-agent-fixtures` 6 个；lab：`lock:partition` `:orphans 0`、`lock:none(ttl=30)` `:phantom-loss 9` |
| TA14 **election 的服务端地面真值** | `:f :election-probe`（读 `/_election/{group}`）+ `electck` 判据 4（矛盾必红、边界只记录、探针缺失判未执行）—— F-35「残留」段的补丁 | ✅ | `elect-probe-fixtures` 4 个；lab `elect:none` 绿（`probes 74 / contradictions 0`） |
| TA15 **F-50：agent 自发流量无凭据** | 由 TA13 首跑交出：`missing CCT token` 让锁续期 / registry 目录与订阅 / idgen 节点注册全部失效 | ❗ **coord 侧待修（P0 候选）** | `coord-findings.md` §16；agent 日志四路同因 |
| TA7 agent nemesis | `:kill-agent / :kill-agent-all / :pause-agent / :partition-agent-server / :agent-all` | ✅ | 矩阵入口 `make matrix-m5`（含 `lock:partition-agent-server`） |
| 门禁 | `make checkers` **26 套 133 个 fixture**（退出码 0，0 FAIL）；`matrix-m5` / `matrix-m5-diff`（新增 `KEEP_GOING=1` 分面归因档） | ✅ | `make checkers` 退出码 0；矩阵结论见 §15 |
| 文档 | 本文 + `coord-findings.md` §14（F-34 闭环）+ `dev.md` §5.5 第 13–16 条 | ✅ | — |

### 待办（下一轮，按优先级）

> **第二轮收尾（2026-09-18 晚）**：待办 1–5 全部完成（证据见 `coord-findings.md` §15
> 的「本轮矩阵的实测结论」）。过程中矩阵又交出十条**测试自身**缺陷（F-35…F-44）
> 与一条 coord-agent 侧观察（F-46）；`election` / `idgen` / `registry` 六个 cell
> 全绿，`lock` 三个 cell **只剩 F-28**，`map` 的差分对双绿。

1. ~~**F-34 分诊（P0 候选）**~~ ✅ **已完成**（探针常驻为 checker 判据 5；F-34 判定
   为测试自身的三层区间度量缺陷，99 → 0）。
2. ~~**把 lock 以外的三个面在 lab 跑通**~~ ✅ **已完成**（`election` / `idgen` /
   `registry` 各 none|kill-agent 两档全绿）。
3. **idgen 的 nodeid 冲突形态** —— ✅ **已闭环（第七轮）**：`--agent-idgen-node-ids 7,7`
   + `--rate 200 --concurrency 8n`（把「同毫秒同 nodeid」的概率拉到每秒 ~10 次量级）。
   同时，注册路径「从未成功」的**根因已被证实不是时序**，而是 **F-50**：agent 自发的
   流量没有凭据通道 ⇒ `/_idgen/nodes/{nodeid}` 的 CAS 注册永远 `missing CCT token`
   ⇒「显式撞车 ⇒ 顺延」这条路径在生产配置下**结构性不可达**（见
   `coord-findings.md` §16）。**这一条已从「测试侧的实验不足」升级为 coord 侧缺陷。**
4. ~~**还原 soakfull 的 agent 面**~~ ✅ **已完成**（soak 日志里直接看到
   `:lock-contend` / `:elect-campaign` 的 invoke/completion）。注意：`make soakfull`
   的 `SOAK_TIME_LIMIT` 只覆盖 workload 的 `--time-limit`，**不**覆盖
   `--soak-quiet`（默认 1800s）——短冒烟要显式传 `SOAK_EXTRA="--soak-quiet 60"`，
   否则会挂住 lab 半小时。
5. **差分矩阵** —— `map:none` 的 direct/via-agent 双绿（F-43/44/45 都是在这条线上
   抓到的）；第七轮把 `matrix-m5-diff` 的归因提示改成**按本格 direct 结果分叉**
   （direct 绿 ⇒ 「红只可能来自 agent 合成层」；direct 也红 ⇒ 「先修 server 面，
   这一格不得给 agent 定罪」）。剩下三个 workload（txn / watch / lease）的差分对
   **仍是下一轮的第一件事**（F-44 已修，可跑）。
6. **§7 的 10 条书面确认**：其中 3 条本轮已被实测间接回答（①断连降级、⑤ idgen 边界、
   ⑥ 部署期密钥同步），另有 1 条变成了**带证据的具体问题**（F-46：显式 nodeid 撞车
   时的预期行为），需要与 coord 团队把它固化成文字。

> **已知阻塞**：F-28（fencing）与 F-32（agent 无 root 旁路）未修之前，`lock` 与
> `--via-agent` 的 cell **注定红**（这是正确行为：`make matrix-m5` 会在第一个 lock
> cell 停下）。要在这段时间里给其它面做分面归因，用
> `make matrix-m5 KEEP_GOING=1 SKIP_CHECKERS=1` —— 它跑完所有 cell 再汇总失败列表，
> **退出码仍是 1**，只用于归因，不得用于验收（见 lab Makefile 里该变量的注释）。

---

## 附：本文档引用的源码锚点

| 主题 | 位置 |
|:--|:--|
| agent 命令与配置加载 | `coord/src/main.rs:1656`（`Commands::Agent`）、`coord-agent/src/lib.rs:419`（`from_file`） |
| agent 认证 fail-closed 校验 | `coord-agent/src/lib.rs:232-287`（`validate_auth_key_material`） |
| 锁权威与本地缓存 | `coord-agent/src/services/lock.rs:337`（acquire Txn CAS）、`187-215`（历史 P0：本地墙钟判定 → 假丢锁） |
| 锁续期 fail-safe | `coord-agent/src/services/lock.rs:77-100`（`RenewAction`） |
| 选举身份与例外分支 | `coord-agent/src/services/leader_election.rs:100-113` |
| registry 自我保护快照 | `coord-agent/src/services/registry.rs:6-9` |
| idgen nodeid / 号段 | `coord-agent/src/services/idgen.rs:5-11`、`152-160` |
| cache TTL 与复制 | `coord-agent/src/services/cache.rs:153-195` |
| 网关拦截与计数 | `coord-agent/src/plugin/gateway.rs:125-215`、`:277-460` |
| 插件身份 / CCT 续期 / 降级 | `coord-agent/src/plugin/identity.rs:10-17`、`:101-120` |
| 代理流桥 | `coord-agent/src/proxy.rs:465/551/898` |
| 承诺台账（GA 期限） | `apis/contracts/STATUS.md` |
| agent 愿景 | `docs/coord-agent-plugin-engine-plan-2026-09-09.md`（§5 架构 / §7 SDK / §9 拦截点 / §12 里程碑） |
| 既有进程内覆盖（不重复） | `coord-agent/tests/agent_plugin_{js,component}_test.rs`、`coord-agent/src/plugin/abi.rs`、`coord/tests/plugin_*_process_test.rs` |

---

## 12. 落地状态（v0.3，2026-09-19）：M5b 起步 —— agent 本地数据面

第八轮把 M5b 里**不需要插件引擎**的两个面做成了可跑的 workload + checker +
fixture，并把两条「结构性不可测」的边界显式写下来（拒绝假红，而不是让判据去猜）。

| 项 | 内容 | 状态 | 证据 |
|:--|:--|:--|:--|
| TB2 cache（AG-09） | `--workload cache` + `jepsen.coord.cacheck`：fabricated / 持久写丢失 / TTL 提前 / TTL 幽灵 / list 丢推入 / set 丢成员 / **重启后持久写消失**（redb 承诺）+ 4 项样本门槛 | ✅ 代码 + fixture（10 + 1）；lab `matrix-m5b` 的 cache 三档见 PROGRESS §1.8 | `scripts/cache-fixtures/`（含 3 条负控制 + 3 条守门员） |
| TB3 mq（AG-11） | `--workload mq` + `jepsen.coord.mqck`：静默丢失 / payload 不一致 / 发布 offset 重复 / 响应内重复 / 乱序 / **已确认又被投递** + idempotency_key 观察 + 2 项门槛 | ✅ 代码 + fixture（10 + 1）；lab 见 PROGRESS §1.8 | `scripts/mq-fixtures/`（含 4 条负控制 + 3 条守门员） |
| 多 agent 前提 | `local-consistency-workloads = #{:cache :mq}`：`--agents > 1` 时**构造期拒绝**（数据在 agent 进程本地，「读不到」在多 agent 下是合法的） | ✅ | `coord.clj` 的构造期校验 + 两个 checker 的「漏检边界」段 |
| 能力引导 | `client-capabilities` 补 `coord:cache:read/write`、`coord:mq:manage/publish/consume`（取自 `coord-core/src/auth` 的权威表） | ✅ | `agent.clj`；wire 自检 `scripts/check-agent-wire.clj` |
| 门禁 | `make checkers` 32 套 152 个；新增 `matrix-m5b`（cache/mq × none\|kill-agent\|partition-agent-server，`--agents 1`） | ✅ 离线全绿 | `make checkers` 退出码 0 |
| 新发现 | **F-57**（coord-agent P1：`idempotency_key` 声明未使用）、**F-58**（lab 拓扑：跨 agent 的复制面不可测）、**F-59…F-62**（测试自身 4 条，全部被 fixture 拦下） | 记录在案 | `coord-findings.md` §17 |

### 仍未落地的 M5b / M5a 项（下一轮，按优先级）

1. **存储代理面（AG-12）** —— `Storage` 经 agent 的流式代理：半成品对象不可见、
   中断重试后的对象大小/sha、`total_size=-1` 路径、快照恢复期的 `get` 行为。
   需要 `Replica`/Storage 的流式读取器（与 MQ 的 Poll/Ack 不同，这一面**必须**
   用流：断流是核心故障形态）。
2. **凭据/身份面（AG-13）** —— `restart-agent-keep-data` 后凭据可复用且不越权、
   refresh **单次使用**（重放必须失败）、伪造 refresh fail-closed、轮换期间不中断。
   现有 `:kill-agent` 的 `:stop` 半场已经是「保留 data_dir 重启」，骨架已在。
3. **registry 的「分区期陈旧上界」（AG-07 第②③条）** —— 现 checker 只判健康态幽灵
   与自身观测链；**F-51**（auth 下以空缓存启动 + 订阅失败 ⇒ 跨 agent 发现结构性
   不可用）需要一条「缺失」判据（当前只覆盖「陈旧/幽灵」）。
4. **可观测面纳入判据（AG-14）** —— 新面已把 `:by-op` 完成数打进 summary（F-41 的
   纪律），但 agent metrics（`denied_total` / 代理计数 / 插件计数）还没有被当作
   判据来源用起来。
5. **安全面 AG-03/04/05**（TA5，2 人日）—— 断连降级成对判据、网关拒绝的「真阻断」
   与热重载窗口、CCT 失效回退不得降级。**前置**：lab 需要起**插件引擎**
   （`plugins` 配置默认关闭）并部署一个测试插件；网关面还依赖 `:reload-agent`
   （SIGHUP）nemesis。建议单独一轮，并在动手前先确认插件在 lab 的部署形态。
6. **差分对的 lab 实跑**（M5a 收口）—— `MATRIX_M5_DIFF` 已含 `map/txn/watch/lease`
   四格，代码就绪，只差真跑 + 证据入库（第八轮的机器时间用在 M5b 上）。
7. **soak 面并入 cache/mq** —— 这里有一个**拓扑冲突**（不是代码量问题）：T6.1 的比例
   里 lock/election/registry 要求 `--agents ≥ 2`，而 cache/mq 的一致性判据要求
   **恰好 1 个 agent**（数据在进程本地，见 F-58）。因此「把 cache/mq 并进同一个
   soak」在解决 ISR 拓扑问题之前**结构性做不到**。可选路径：② 单开一个 `--agents 1`
   的 cache/mq 浸泡（`--soak-mix cache=...,mq=...`）；③ 先回答 §5.4-⑩ 的拓扑问题，
   再让 cache/mq 与多 agent 控制面共存。已实现的面在 `--soak-mix` 里出现但未支持时，
   `soakfull-mix` 会**构造期硬失败**（「拒绝静默丢弃」）。
