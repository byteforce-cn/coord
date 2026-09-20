# coord-agent 全量 GA 整改基线（W0 产出）

- **日期**：2026-09-19
- **基线 commit**：`4a5e3ee`（工作树含本轮 v0.2.0 改动）
- **对应计划**：`docs/coord-agent-ga-v0.2.0-plan-2026-09-19.md` §5 工作流 W0
- **体例**：遵循 `docs/production/remaining-known-gaps.md:10-11` —— 每条必须是**可核验事实**
  （`file:line` / 命令 / 测试）。本文**不采信** `WHITEPAPER.md` §9.1 与计划书的自述，
  逐条以代码实测为准。凡与两文档不符处，以本文为准。

> **为什么需要本文**：D1 由「分层 GA」翻转为「全量 GA」后，批次 7（cache / mq / workflow /
> scheduler）成为关键路径，而其整改内容原定照抄 `WHITEPAPER.md:311-318` §9.1。
> 实测表明该清单**至少 2 条已过期**。若照抄，会去做已经做完的事、并漏掉真正的 blocker。

---

## 1. §9.1 四项断言的重核（W0-1）

| # | §9.1 原断言 | 实测 | 判定 |
|:--|:--|:--|:--|
| 1 | Cache「ISR 提交**非原子**」 | `coord-agent/src/services/cache.rs:1161` `replicated_write`：先**单事务**本地提交，再推送 ISR，最后 `ensure_isr` 校验（`:1160` 注释自述）。本地提交是原子的（幂等键 + 数据 + 簿记同事务）；**跨节点无原子提交** —— leader 本地提交后、follower ack 前崩溃 ⇒ follower 永久落后 | ⚠️ **成立但表述错误**：不是「本地非原子」，是「**跨节点提交非原子**」。须重述 |
| 2 | Cache「分区 Leader **静态无故障转移**」 | `coord-agent/src/services/replication.rs:5` / `:322` / `:832` / `:842` —— 自述「分区 Leader **静态分配**（min-addr）」；`shard_leader` / `is_leader` 按成员集合确定性计算，**无重新选举路径** | ✅ **成立**（计划书 §2.3 曾标「须重核」，重核结果为**原判正确**） |
| 3 | Workflow「生产路径为**内存态**，无持久化/补偿」 | `coord-agent/src/services/workflow_store.rs:39` `struct KvWorkflowStore`（KV + Txn + Watch 持久化）；`coord-agent/src/lib.rs:1169` `// R-AGT-09：有 server 连接时走 KvWorkflowStore（raft 持久化）`；补偿语义在 `coord-core/src/workflow/sw.rs:134` `compensated_by` / `:137` `used_for_compensation` | ❌ **§9.1 已过期**：持久化与补偿均在。应改为「**端到端验收**」项 |
| 4 | Scheduler「内存 HashMap **假实现**」 | `coord-agent/src/services/scheduler.rs` —— 整改前 `tasks` / `claims` / `states` 三个 `Arc<RwLock<HashMap<..>>>`（原 `:86/:88/:90`），而同文件头部自述「任务认领: **KV CAS + Lease** 防止重复执行」 | ✅ **成立**，且属于**第三类缺陷：头部宣称与实现不符**（见 §3）。**已于 2026-09-19 修复**（B-05，E9）：三表合一 + `KvSchedulerStore`，头部自述与实现已一致 |

**结论**：4 项中 2 项成立（Cache 分区、Scheduler）、1 项成立但须重述（Cache ISR）、
1 项已过期（Workflow）。

---

## 2. 其余服务面的状态后端（W0-1 续）

实测命令（均可复现）：

```bash
grep -n 'dek_store\|TransitDekStore\|_persisted' coord-agent/src/services/transit.rs
grep -n 'TransitDekStore\|/_transit/v1/dek'        coord-agent/src/services/transit_store.rs
grep -n 'RwLock<HashMap'           coord-agent/src/feature_flags.rs
grep -n 'RBAC 策略'                 coord-agent/src/services/policy.rs
grep -n 'RwLock<'                  coord-agent/src/services/circuit_breaker.rs
grep -n 'Mutex<f64>'               coord-agent/src/services/rate_limiter.rs
sed -n '160,182p' coord-agent/src/service.rs
```

| 服务 | 状态后端 | 实测锚点 |
|:--|:--|:--|
| `transit` | **DEK 落 coord-server KV**（内存注册表降为缓存）**✅ 已整改 2026-09-19** | `services/transit_store.rs`（`KvTransitDekStore`，键 `/_transit/v1/dek/{dek_id}`）；服务侧 `services/transit.rs` 的 `with_store` / `encrypt_persisted` / `decrypt_persisted` / `rewrap_persisted`（原纯内存锚点已不在） |
| `feature_flags` | **开关状态落 coord-server KV**（生产）／内存仅作无 server 的降级 **✅ 已整改 2026-09-19（E2）** | `feature_flags_store.rs`（`KvFeatureFlagStore`，键 `/_featureflags/v1/flag/{key}`）；`feature_flags.rs` `with_store`；`lib.rs` 有 server 连接则注入 KV store，否则内存降级 + 告警 |
| `policy` | OPA bundle 在 KV；**RBAC 策略为本地内存** | `services/policy.rs:186` `/// RBAC 策略（本地内存）`；`:3` 头部却称「基于规则的授权决策引擎（RBAC/ABAC）」 |
| `circuit_breaker` | **全内存** | `services/circuit_breaker.rs:38` `state: Arc<RwLock<CircuitState>>`、`:42` `last_failure_time` |
| `rate_limiter` | **全内存**令牌桶 | `services/rate_limiter.rs:39` `tokens: Arc<Mutex<f64>>` |
| `cache` | redb 本地 + ISR 复制 | `services/cache.rs:206` … **自述「声明的容量上限（字节）。当前未被强制执行」** |
| `mq` | redb + ISR（**生产者幂等索引已落地**） | `services/mq.rs` 自述「打满（背压）时丢弃消息（`try_send`），可靠消费请使用 poll + ack」（B-09 仍未变）；`IDEMPOTENCY_TABLE` + `produce_idempotent` **✅ 已整改 2026-09-19（F-57 / V9）** |
| `scheduler` | **任务/状态/认领落 coord-server KV**（生产）／内存仅作降级 **✅ 已整改 2026-09-19（E9）** | `services/scheduler_store.rs`（`KvSchedulerStore` + `SchedulerStore` 抽象，键 `/_scheduler/v1/task/{task_id}`）；`services/scheduler.rs` 三张 HashMap 已合并为一条记录，认领经 CAS |

**默认开关**（`service.rs:160-182` 实测）：

| 服务 | 默认值 | 行 |
|:--|:--|:--|
| `cache` | **`true`** | `:171` |
| `workflow` | **`true`** | `:173` |
| `transit` | **`true`** | `:179` |
| `pki` / `registry` / `config_center` / `lock` / `idgen` / `policy` | `true` | `:162-166`, `:174`, `:180` |
| `leader_election` / `event_notification` | **`false`** | `:169`, `:170` |
| `mq` / `scheduler` / `circuit_breaker` / `rate_limiter` / `feature_flags` / `replication` | `false` | `:172`, `:175-178`, `:181` |

> **G6 口径（默认开关）必须显式裁定**：`cache` / `workflow` / `transit` 三项
> **默认开启且未整改完毕**。全量 GA 后它们成为承诺面 ⇒ 「默认开启」必须**以整改完成为前提**，
> 否则等于默认配置即暴露未整改面。
>
> **`transit` 已闭合（2026-09-19，D5）**：DEK 已落 coord-server KV（"默认开启 + 重启不丢密钥"），
> 故 G6 对 transit 取「**启用即可用**」，无需改默认值。**`cache` / `workflow` 仍待裁定**（E5b）。

---

## 3. 合并后的 blocker 清单（W0-2）

设计缺陷（§9.1 族）+ 运行缺陷（jepsen 族）+ **自述式缺陷**（本文新增第三类）去重合并。
严重度为**两轴**：`(是否阻断 GA, 是否阻断发布)`。

### P0 —— 阻断 GA 语义成立

| # | 缺陷 | 类别 | 证据 | 复现/验收 |
|:--|:--|:--|:--|:--|
| **B-01** | 鉴权开启时 agent **自发流量无凭据**（锁续期 / registry 目录加载与订阅 / idgen nodeid 注册） | 运行 | `jepsen/docs/coord-findings.md:1577`（`confirmed-by-run`）、专节 `:1591`；`coord-agent/src/auth/interceptor.rs:556`、`coord-agent/src/lib.rs:629` | F-50 复现命令；V6 |
| **B-02** | `LockService::release` **不校验 `lease_id`**（fencing 缺失） | 运行 | **✅ 已修复 2026-09-19（F-28）**：`release` 改为按**当前真相**判凭据（先本地快照，不符则回查 Server 的原始 `/_lock/{name}`），`(holder_id, lease_id)` **两者都匹配**才撤销租约；不匹配 ⇒ 契约规定的 `PERMISSION_DENIED`（不再是静默 `released=false`）。**顺带修同一契约面的 F-33**：`LockRenewResponse.new_ttl` 此前**成功也回 0**（契约里 0 = "租约已失效"）⇒ 按契约读返回值的客户端会在**续期成功那一刻**认为锁丢了；现回真实 TTL，失效时回 0。`coord-findings.md:966`（104/104 仍命中）、`:966` 的 fencing 探针形态即 `lease_id + 1` | **验收**：`cargo test -p coord-agent --lib services::lock`（21 ✓，含 3 条新增：错 lease_id / 错 holder / `ReleaseOutcome` 契约语义）；`--test agent_lock_test`；jepsen `lockck` 的 `:lock-fencing-missing` 判据（V6 族）|
| **B-03** | agent 侧**无 root 全能力旁路**，鉴权开启时 root 经 agent 全路径被拒 | 运行 | **✅ 已修复 2026-09-19（F-32）**：`ROOT_ROLE` 上移到 `coord-core/src/auth/mod.rs`（+ `is_root()`），**server 与 agent 共用同一个常量**；agent 拦截器在**路由登记之后、能力/scope 判定之前**放行 root（跳过能力与 scope，但**不**跳过"未登记 RPC 即拒绝"那条路由层 fail-closed 防线）。**未采用**"把 root 全能力物化进角色记录"：那会引入新的漂移面（以后新增能力不会自动进那条记录，root 会缺能力且只在运行期可见）。`coord-findings.md:970` | **验收**：`cargo test -p coord-agent --lib auth::interceptor`（**23 ✓**，含 2 条新增：`test_interceptor_root_bypasses_capability_without_explicit_grants`（root 在"角色记录里无逐项能力"的真实形态下走通，且未登记 RPC 仍拒）/ `test_interceptor_non_root_still_denied_without_grant`（反向对照））+ `cargo test -p coord-core auth`（root 常量逐字相等语义）|
| **B-04** | MQ `poll + ack` 路径**全失败**（`Ack` 59/59） | 运行 | **✅ 2026-09-19 已分诊，根因是「测试侧」而非 coord 缺陷**：jepsen 手写 descriptor `jepsen/src/java/jepsen/coord/CoordRpc.java` 的 `mqAckRequest()` 把**字段 2/3 写反**（写成 `partition=2, consumer_group=3`；真 proto 是 `consumer_group=2, partition=3`）⇒ 客户端把 **int32（varint）** 发在服务端的 **string（length-delimited）** 字段上 ⇒ **wire type 不匹配**、protobuf 解码失败 ⇒ `Ack` 100% 失败。与 F-63/F-64/F-65/F-66 **同族**：测试侧编码缺陷伪装成"被测系统全灭"。已修 descriptor 并**加固卡口**（见下） | `coord-findings.md:1768`（F-67，原文标「[待分诊, P1]」）。**验收**：`jepsen/scripts/check-agent-wire.clj` 现为**全量**比对（遍历 `AGENT_FILES` 每个 message 的每个字段：字段名/字段号/**wire type**/repeated + 服务名/方法名），**正负对照均验**：还原 2/3 写反 ⇒ 红（字段号不符 ×2）；把 `max_count` 由 i32 改 str（号不变）⇒ 红（wire type 不符）。**这同时消掉了 F-67 不成立的那半** —— 它此前只查手工枚举的 22 个 message，MQ 不在其中 |
| **B-05** | Scheduler 头部宣称「KV CAS + Lease」，实现为**三个内存 HashMap** | 设计 + **自述式** | **✅ 已修复 2026-09-19（E9）**：三张 HashMap 合并为 `TaskRecord` 一条记录，生产走 `KvSchedulerStore`（coord-server KV + Txn CAS）；模块头自述已与实现一致。**附带修复两处**：① `task_id` 用随机 uuid 注册、却按 `req.name` 认领 ⇒ ClaimJob **永远找不到任务**；② handler 给 Heartbeat/Complete 硬编码 `"worker"`（与认领时的 uuid 永不相等）⇒ 续期静默失效、CompleteJob 必报错；现改 claim 句柄语义（`*_any`） | **验收**：`cargo test -p coord-agent --lib services::scheduler`（18 ✓）/ `--lib services::scheduler_store`（9 ✓）/ `--test agent_scheduler_test`（18 ✓，含并发 CAS 唯一性、共享 store 重启存续） |
| ~~**B-06**~~ | ~~`transit` DEK **不落盘**且**默认开启**~~ **✅ 已修复（2026-09-19，取「持久化」案）** | 设计 | 修复前：`services/transit.rs:58`（内存 `HashMap`）；修复后：`services/transit_store.rs`（`KvTransitDekStore`）、`services/transit.rs`（`*_persisted`）、`lib.rs`（有 server 连接则注入 KV store，否则内存降级 + 告警） | **验收**：`cargo test -p coord-agent --lib services::transit::tests`（22 ✓）/ `--lib services::transit_store`（6 ✓）/ `--test agent_transit_test`（8 ✓，含重启恢复 2 例）；全量 `-p coord-agent --lib` 424 ✓（无回归）。**附带修复**：轮换后 DEK 可二次解密（按包头 id 删除）、`dek_ttl_secs` 声明未用。**残留**：KEK 由 `kek_id` 确定性派生 ⇒ KEK 不保密，静态保护依赖 server Barrier（记为 U9，非本项） |
| **B-15** | **`runtime.rs` 的 try 块丢弃 `Suspend`** ⇒ 被 `compensatedBy` / `onErrors` 包裹的状态，其 `call` 动作**一次都不会发出**（静默丢副作用） | 设计（运行期） | **✅ 已修复 2026-09-19（E10 验收中发现）**：`call` 任务执行器不同步做 I/O，它返回 `Suspend{ExternalCall}` 由 Runtime 派发；而 TryBlock 的 match 只有 `NextTask\|Completed\|Failed\|_`，`Suspend` 落进 `_ => {}` 被静默丢弃 ⇒ 被补偿状态的动作**既不成功也不失败**，工作流继续沿正常路径前进。已改为在 try 块内与主循环同口径派发（Success → 应用到 try 上下文；Failure → communication fault 交给 catch 路由） | **验收**：`cargo test -p coord-agent --lib test_sw_compensated_by_runs_compensation_end_to_end`（修前红：`task_stack=["reserve__try","reserve__transition","done","done__transition"]`；修后绿） |
| **B-16** | **`functionRef` 从未解析到 `functions[].operation`** ⇒ 任何 CNCF `operation` 状态在真 dispatcher 下**必然失败**（`unknown service type: <函数名>`） | 设计（编译期） | **✅ 已修复 2026-09-19（E10 验收中发现）**：dispatcher 签名 `dispatch(service, with, input)` **看不到文档的 `functions[]`** ⇒ 解析只能在编译期做。新增 `sw.rs::resolve_function_call()`：`operation` 为 `http(s)://` ⇒ 发 `CallType::Http` + `endpoint.uri`（方法取动作参数 `method`，缺省 POST）；其它保持函数名并显式失败。**测试盲区**：现有 SW 用例普遍用 `NoopTaskDispatcher`（对一切返回成功），恰好掩盖"派发根本没发生" | **验收**：同上 E10 用例（由记录型 HTTP 服务端收到 `/refund` 请求证得）；`sw.rs` 的旧断言 `CallType::Function("approveOrder")` 已改为 `endpoint.uri == "http://icps/approve"` |
| **B-07** | Cache **跨节点提交非原子** | 设计（重述） | **✅ 2026-09-19 按「显式声明边界」闭合**（计划书 E7 给出的两条路之一；另一条是"实现可恢复语义"，属 M4）。**准确的失效形态**（逐行核验后重述）：① **本地**提交是原子的（幂等键+数据+序列号同事务），**跨节点**不是；顺序固定为「本地提交 → 推送 ISR → `ensure_isr`」；② 因此推送/`min_isr` 失败时 RPC 返错但**本地写入已生效** —— 调用方**不能**把错误读作"未写入"；③ 落后副本**不是永久**的：心跳检测到落后即触发 `pull_and_catch_up` → Reconcile，且复制日志无上限保留 ⇒ 窗口是"Leader 提交后、Follower 补齐前"的**暂时**不一致（原稿"Follower 永久落后"**过强**，已更正）。已写入 `cache.rs` 模块头 + `replicated_write` 文档 + **对外契约** `cache.proto` 的边界声明（两份副本逐字相同） | **卡口**：`bash apis/contracts/scripts/check-wire-descriptor.sh`（exit 0，注释变更不动 wire）；`cargo check --workspace --all-targets` |

### P1 —— 阻断契约语义完整

| # | 缺陷 | 类别 | 证据 |
|:--|:--|:--|:--|
| **B-08** | Cache 分区 Leader **静态无故障转移** | 设计 | **✅ 2026-09-19 判定为「设计边界」并写入契约**：`shard_leader` = 显式覆盖优先、否则 ISR 成员中地址最小者，所有 agent 基于同一成员集合算出同一结果 ⇒ **不产生脑裂**，代价是 Leader 失联时该分区**不可写**（fail-closed），恢复靠运维介入而非自动选举。已写入 `cache.proto` 的边界声明（"不承诺自动故障转移"）。`services/replication.rs:840-841` 自述"静态分配"与实现一致 |
| **B-09** | MQ **push 路径背压丢消息** | 设计（自述已披露） | **✅ 2026-09-19 已移出承诺面**：`mq.proto` 现在**显式**声明 `Subscribe` 是 **best-effort 推送**（channel 打满即丢弃、不报错不重投）、**不是 at-least-once 通道**，并指明 at-least-once 的唯一承诺路径是 `Poll` + `Ack`。同批补齐 `Publish`（`idempotency_key` 去重语义 + 只承诺"落本地日志"）与 `Ack`（偏移提交 + 非 Leader 返回 FAILED_PRECONDITION）的语义承诺。`services/mq.rs:558` 的自述与契约现已一致 |
| **B-10** | `idempotency_key` 在 **publish 面**未生效（ISR 复制面**已**使用 `REPL_APPLIED_KEYS`） | 运行 + 设计 | **✅ 已修复 2026-09-19（F-57 / V9）**：新增 `IDEMPOTENCY_TABLE`（键 = topic+partition+ikey），`produce_idempotent` 在**同一 redb 写事务**内查/写；两条 gRPC 分支（复制 / 本地）都传入 `req.idempotency_key`，`req.key` 也不再被丢弃（hex 存入 headers）。`coord-findings.md:1693` | **验收**：`cargo test -p coord-agent --test agent_mq_test`（23 ✓，含 5 条新增：同键去重、无键不去重、分区隔离、**重启后仍去重**、key 持久化） |
| **B-11** | Cache 容量上限**声明但未强制执行** | 自述式 | **✅ 2026-09-19 复核确认已按"可观测事实"闭合**：`cache.rs` 的 `new()` 文档写明上限**不执行**、启动告警、`Debug` 报 `max_size_enforced: false`；且**契约里没有任何容量承诺**（`grep 容量\|max_size apis/contracts/proto/coord/cache/v1/cache.proto` → 0 命中）⇒ 不存在"承诺了但没生效"。本轮另在 `cache.proto` 的"不承诺"清单里显式写入容量上限 |
| **B-12** | `feature_flags` 全内存，重启即丢 | 设计 | **✅ 已修复 2026-09-19（E2）**：新增 `feature_flags_store.rs`（`FeatureFlagStore` 抽象 + `KvFeatureFlagStore`），生产走 coord-server KV；读路径**有意不做本地缓存**（wire 只有只读 RPC，更新带外发生，无 Watch 的缓存会静默陈旧） | **验收**：`cargo test -p coord-agent --lib feature_flags`（10 ✓）/ `--test agent_feature_flags_test`（8 ✓，含存续用例） |
| **B-13** | `policy` RBAC 策略为本地内存（OPA bundle 在 KV），头部未声明边界 | 设计/文档 | `services/policy.rs:186` vs `:3` |
| **B-14** | `circuit_breaker` / `rate_limiter` 为本地内存，未在契约注释声明「不承诺跨 Agent 共享」 | 文档 | `services/circuit_breaker.rs:38`；`services/rate_limiter.rs:39` |

> **B-14 / B-13 已在 v0.2.0 部分闭合**：对应契约文件的头部注释已显式写出边界声明
> （见 `apis/contracts/proto/coord/{policy,circuitbreaker,ratelimiter}/v1/*.proto`）。
> 仍待办的是「若边界为设计意图，则在 `WHITEPAPER.md` §10 规则 4 下正式确认」。

### 已剔除（不列入 blocker）

| 原项 | 剔除理由 |
|:--|:--|
| F-34（互斥重叠） | **测试自身**（P1），且已 `closed-by-run`（99 → 0；`coord-findings.md:972/:1048`） |
| §9.1 Workflow「无持久化/补偿」 | 代码实测已具备（§1 #3） |
| §9.1 Cache「ISR 提交非原子」（本文表述） | 已重述为 B-07 |

---

## 4. `WHITEPAPER.md` §9.1 / §9.2 修订稿（W0-3）

> 按 `WHITEPAPER.md:352-370` §11 契约变更流程提交；与 `contracts/v1.2.0` 的
> EXPERIMENTAL→COMMITTED 公示**合并为一次**变更（计划书 A9）。

1. **§9.1 实验能力清单由 4 项改为 5 项**，补入 `coord.storage`
   （`apis/contracts/STATUS.md` 一直有该行，proto 也已存在，是 §9.1 漏列）。
2. **Cache 条目重述**：「ISR 提交非原子」→「**跨节点提交非原子**（leader 本地提交后、
   follower ack 前崩溃 ⇒ follower 永久落后）」；「分区 Leader 静态无故障转移」**保留**。
3. **Workflow 条目改写**：从「生产路径为内存态，无持久化/补偿」改为
   「**持久化与补偿已落地（`KvWorkflowStore` + `sw.rs` 补偿字段），待端到端验收**」。
4. **Scheduler 条目保留**，并补注「头部自述 KV CAS + Lease 与实现（内存 HashMap）不符」。
5. **§9.2 实验包规则**：补充说明「experimental 包从未有 proto 文件、零消费者，
   直接建稳定包不构成 Breaking」，作为 `contracts/v1.2.0` 的依据。
6. **新增 §9.4「自述式缺陷」类别**：把「头部注释/文档宣称某机制存在，而实现未接线」
   列为独立缺陷类（B-05 / B-11 / B-12 同族），并规定 GA 验收须含
   「自述式 no-op grep」这一检查手段。

---

## 5. 批次 7 工作量重估（W0-4）

| 服务 | 重估结论 | 依据 |
|:--|:--|:--|
| `workflow` | **大幅下调**：原估「重写为持久化 + 补偿」整块作废，改为**验收 + 补测试** | §1 #3 |
| `cache` | **持平**：仍是两项（B-07 跨节点原子性 + B-08 分区故障转移） | §1 #1/#2 |
| `mq` | **上调**（本轮后部分下调）：`Ack` 全失败（B-04）是**功能性坏**，非设计取舍；B-10（publish 幂等）**已闭环**，B-09（push 背压）仍须移出承诺面/文档化 | `coord-findings.md:1768` |
| `scheduler` | **已完成**（本轮）：三个 HashMap 改 KV 真实现（B-05），并同步修头部自述；**额外发现并修复**两处使 gRPC 面实质不可用的缺陷（task_id 注册/认领键不一致、硬编码 `"worker"`） | §1 #4；B-05 行 |

**关键路径结论（更新于本轮后）**：批次 7 的**主要风险从 workflow 转移到 mq**
（原结论不变），而 `scheduler` 已不再位于关键路径上。
计划书 §6.2 把 mq 与 cache 并列（同 2026-12-31），但 mq 含一个 P0 功能缺陷（B-04，且**尚未分诊**），
**建议 mq 独立设里程碑卡口**，不与 cache 合并验收。

---

## 6. v0.2.0 本轮已落地 vs 未落地（诚实状态）

### 已落地并验证

| 项 | 验证手段 | 结果 |
|:--|:--|:--|
| 契约面 16 个 `coord.<domain>.v1` 包（11 新建 + 5 迁移）+ `coord.storage` 转 COMMITTED | `apis/contracts/scripts/check-wire-sync.sh` | **绿**（17 条 PENDING + 无 NOTICE，EXPERIMENTAL 区已空） |
| wire 零变化（19 个 service / 全部 message / 全部字段号） | `python3 scripts/oneoff/verify-wire-split.py`（基线快照 `scripts/oneoff/agent_api.pre-v0.2.0.proto`） | **`WIRE PRESERVED: True`**（missing = 0，field/rpc diffs = 0） |
| 门禁真能置红（非空头支票） | 负向测试：把 registry 包名改回 `coord.agent` | **退出码 1 + FAIL 行** |
| 能力表路径集合（V1） | 与 `git show HEAD:` 对比集合差 | **旧 149 / 新 149，missing=0 extra=0** |
| Rust 工作区编译 | `cargo check --workspace --all-targets` | **通过**（仅既有 warning） |
| 能力表 / 拦截器单测 | `cargo test -p coord-core --lib grpc_auth`、`-p coord-agent --lib auth::interceptor` | **8 + 21 全绿** |
| Java SDK 编译 + 全量测试 | `mvn clean test -DargLine="-Dnet.bytebuddy.experimental=true"` | **149/149 全绿** |
| Java SDK 契约面收敛（V3） | 10 个显式文件 `grep sdk.internal.proto` = 0；仅 `CoordClient.java` 保留 Health | **符合预期** |
| jepsen 手写 descriptor（V4 族） | 编译 + 自定义 harness（无需 lein） | **`ALL OK`**（6 个新包、17 条方法全名、6 个 message 可构建/序列化） |
| 版本统一 v0.2.0 | `Cargo.toml` + 8 个成员 crate + `pom.xml` + `CHANGELOG.md` | **已改**（含计划书 F1 的错误前提修正） |
| **B-01 / F-50**（鉴权下 agent 自发流量无凭据） | 真进程套件 `AGENT_AUTH_REAL=1 cargo test -p coord --test agent_auth_process_test -- --ignored` + 负向对照 | **通过**；负向对照（关掉自举）复现 F-50 并**多出 4 个**原文未列的表面（pki CA / transit DEK 清扫 / config 订阅 / workflow 存储初始化） |
| **P0-4 / D6+D7**（协议版本协商） | `cargo test -p coord --test agent_handshake_test`（4 条源码级守卫）+ `mvn -o test -Dtest=ProtocolNegotiatorTest` | **4 ✓ / 9 ✓**；旧版本客户端得到 `PROTOCOL_MISMATCH`（三要素），非裸 `UNIMPLEMENTED` |
| **G1 / V7**（descriptor 级 wire 卡口） | `bash apis/contracts/scripts/check-wire-descriptor.sh` | **exit 0**（严格 16 包 + 子集 6 包 + 随附 1 包）；负向对照（改字段号）立即置红 |
| **B-06**（transit DEK 持久化） | `cargo test -p coord-agent --lib services::transit*` + `--test agent_transit_test` | **22 + 6 + 8 全绿**（含重启恢复、跨实例单次使用） |
| **B-05**（Scheduler KV 化，本轮） | `cargo test -p coord-agent --lib services::scheduler*` + `--test agent_scheduler_test` | **27 + 18 全绿** |
| **B-10**（MQ publish 幂等，本轮） | `cargo test -p coord-agent --test agent_mq_test` | **23 ✓**（含重启后仍去重） |
| **B-12**（feature_flags 持久化，本轮） | `cargo test -p coord-agent --lib feature_flags*` + `--test agent_feature_flags_test` | **10 + 8 全绿** |

### 本轮（2026-09-19 第三轮）新闭合

| 项 | 结论 | 验收 |
|:--|:--|:--|
| **B-02**（fencing 缺失） | ✅ 修复：`release` 校验 `(holder_id, lease_id)`，不匹配 ⇒ `PERMISSION_DENIED`；**顺带修 F-33**（`new_ttl` 成功也回 0） | `--lib services::lock` **21 ✓**（+3） |
| **B-03**（root 无旁路） | ✅ 修复：`ROOT_ROLE` 上移 `coord-core`，server/agent 共用；agent 在路由登记后放行 root | `--lib auth::interceptor` **23 ✓**（+2） |
| **B-04**（MQ `Ack` 59/59） | ✅ **分诊完成：根因在测试侧**（descriptor 字段 2/3 写反 ⇒ wire type 不匹配 ⇒ 解码失败）。已修 + **卡口改为全量**（含 wire type），正负对照均验 | jepsen `check-agent-wire.clj` ✅；还原 bug ⇒ 红 |
| **B-07 / B-08**（Cache 复制语义） | ✅ 按「显式声明边界」闭合（原文「永久落后」**过强**，已更正为「暂时不一致 + 心跳 Reconcile 可补齐」）；静态 Leader 判定为**设计边界**（fail-closed 不脑裂） | `cache.proto` 边界声明（两份副本逐字相同）+ `cache.rs` 模块头 |
| **B-09**（MQ push 丢消息） | ✅ 已**移出承诺面**：`Subscribe` 显式声明 best-effort、非 at-least-once 通道 | `mq.proto` 服务级语义承诺 |
| **B-11**（Cache 容量上限） | ✅ 复核为已闭合：契约无容量承诺 + 代码自述「不执行」且 `Debug` 报 `max_size_enforced:false` | 契约 grep 0 命中 |
| **D3/D4/D5**（SDK 缺 4 个客户端面） | ✅ 新增 `election` / `circuitBreaker` / `rateLimiter` / `featureFlags`（含 `CoordClient` accessor） | `NewClientFacesTest` **10 ✓** |
| **G3**（SDK ↔ 契约命名空间卡口） | ✅ 新增 `apis/contracts/scripts/check-sdk-sync.sh` 并进 CI（第 5 道） | 卡口首跑即抓出 **2 处真漂移**（见下） |

> **G3 首跑即抓到的两处真漂移（本轮的附带发现）**：
> 1. **`coord.storage` 是唯一没有 `java_package` 的 COMMITTED 契约** ⇒ 生成到裸包
>    `coord.storage`，SDK 的生成命名空间**不在契约前缀下**。已补 `java_package`
>    （**只补选项**：不改 wire、不改 proto 包名、不改 service/message 名），SDK 侧 2 行 import 同步。
>    → 15/15 GA impl 现全部落在 `cn.byteforce.coord.contracts.*`。
> 2. `AgentChannelManager` 使用内部面 `sdk.internal.proto`（Handshake 协商）—— 属**刻意保留**
>    （§4.2 表尾 / 红线 R3），已带理由进 allowlist（**无理由的"暂时留着"就是漂移**）。

### 本轮（2026-09-20 第四轮）新闭合

| 项 | 结果 | 判据 |
|:--|:--|:--|
| **B-13 / B-14**（policy RBAC 边界 / cb+rl 边界） | ✅ **§10 规则 4 的正式确认已完成**：`WHITEPAPER.md` 新增 **§9.1.1 能力边界声明**（5 项逐条给出边界 + 落点）与 **§10 规则 5/6**（局部性能力必须声明作用域；被移出承诺面的能力不得删除） | 三面卡口全绿；§11.2.1 消费者告知留档 |
| **W0-3 / A9 / P1-8 / P1-9**（白皮书与台账对齐） | ✅ §1.3 从 5 项扩为 **17 项 + `coord.storage`**；§9.1 改为**历史台账**（不删行）；§12 / §13 同步；EXPERIMENTAL 区清空已在 §1.2 说明 | `check-wire-sync.sh` ⇒ **exit 0**（17 个期限全 PENDING） |
| **P0-13**（**新发现**）：`coord.event.v1` / `coord.scheduler.v1` **没有 Java 客户端面** | ✅ 补 `EventClient` + `SchedulerClient`（含 impl、`CoordClient` accessor）。**修前**：这两个包在 SDK 里连接口都不存在（P1-3 同型） | `check-sdk-sync.sh` 第 4 道卡口**首跑即红并指名两包** ⇒ 修后 `17/17` 全绿；`EventSchedulerContractTest` **10 ✓** |
| **P0-14**（**新发现**）：Scheduler `Heartbeat` **丢弃续期结果** | ✅ 失效句柄改回 `FAILED_PRECONDITION`（与 `MqAck` 同口径）。修前「续期成功」与「句柄已失效」在 wire 上同形（都回空 OK） | `agent_scheduler_test` **22 ✓**（+4）；**负向对照**：还原 ⇒ 3 failed |
| **P0-15**（**新发现**）：Scheduler `ClaimJob.payload` 恒为空 + 存储有损 | ✅ 认领回传注册 payload（base64，**二进制安全**；保留旧文本键兼容既有记录）。修前 handler 硬编码 `vec![]`、写入用 `from_utf8_lossy`（非 UTF-8 静默变 U+FFFD） | 同上（含非 UTF-8 字节的逐字节断言） |
| **P0-15 附带**：`CompleteJob` 对未知任务**静默成功** | ✅ 无有效认领且未 Completed ⇒ `FAILED_PRECONDITION`；已 Completed 仍**幂等** | 同上 |
| **`Event.Unsubscribe` 边界** | ✅ 写入 `event.proto`（两份副本逐字相同）：实测服务端**忽略请求、恒回成功**（订阅生存期即 gRPC 流）；按 §10 规则 6 声明而非删除 | `check-wire-descriptor.sh` ⇒ **exit 0**（注释级变更不动 wire） |
| **G4**（SDK 契约面卡口进 `java-sdk` job） | ✅ `check-sdk-sync.sh` 扩为四道判据，新增第 4 道**反向覆盖**（期望清单从契约 proto 目录**反解**，不再依赖人肉清单） | 卡口首跑即抓出 **P0-13** |
| **CI 卡口「存在但从未生效」**（第三类缺陷，**发布级**） | ✅ 修闭。`origin/main` 停在 `4a5e3ee`，其后 3 个提交从未推送 ⇒ CI 从未跑过它们。实跑 `ci.yml` 的 `lint` job：① `cargo fmt --all -- --check` **54 处 diff / 20 文件**；② `clippy -D warnings` **3 errors**（根因：`mq.proto` 的 `Publish` 注释用 markdown 列表标记，prost 把注释**原样**写进生成代码的 doc comment ⇒ rustdoc `doc list item without indentation`）；③ `check-panics.sh` **2 violations**（`coord-agent/src/lib.rs` 的 `.expect("inner.is_some() checked above")`）。三条现已全部转绿：`fmt=0` / `clippy=0` / `panics=0`（`Panic-path check passed: 0 violations`） | 契约注释改用 `①②`（并在 proto 里写明为何不能用列表标记）；panic 点改为**把条件与取值绑成同一个绑定**（`inner.as_ref().map(\|i\| i.client.clone()).filter(\|_\| wanted)`） |
| **全量回归（最终树）** | ✅ `cargo test --workspace --all-targets` ⇒ **95 个 test 二进制 / 2041 passed / 0 failed**；`mvn -o -B test`（SDK）⇒ **174 passed / 0 failures** | 另：三道契约卡口 + fmt + clippy + panic 卡口全 exit 0 |

### 未落地（**v0.2.0 尚不可发布**）

| 项 | 状态 |
|:--|:--|
| ~~**B-13 / B-14**~~ | ✅ **2026-09-20 已闭合**（`WHITEPAPER` §9.1.1 + §10 规则 4/5/6，走 §11 流程） |
| ~~E10（Workflow 补偿端到端验收）~~ | ✅ **已闭环，并查出 B-15 / B-16 两个新 P0**（均已修） |
| ~~E11（Cache 分区故障转移定论）~~ | ✅ 已定论（B-08：判定为设计边界并写入契约） |
| ~~**jepsen lab 侧复核 —— V8 复跑**~~（B-04 修好后的 MQ `poll + ack`） | ✅ **2026-09-20 已执行且绿**：`Everything looks good!` / `overall-valid: true`；`publishes 108 / polls 82 / delivered 105 / acked 105`、**`:poll-ack-failures 0`**、`:violations-by-class {}` ⇒ V8 判据（`Ack` 成功率 > 0 且**无丢失**）成立。证据：`docs/production/evidence/20260920T133256Z-m5b-mq-poll-ack-single-client/` |
| **jepsen lab 侧复核 —— V11 故障注入** | 未执行（需在 lab 里真注入故障：`make test WORKLOAD=mq NEMESIS=<非 none>`）。**V8 复跑已完成**（上一行），V11 仍待一跑 |
| 72h 浸泡、M3/M4 验收、代码冻结与 `v0.2.0` tag | 未执行 |
| **未决项裁定** | U1 / U3 / U4 / U5 / U6 / U8 / U9（均非代码阻断） |

> **发布判据（更新于 2026-09-19 第三轮后）**：`contracts/v1.2.0` 的**契约面**可发布
> （Minor，无 Breaking；三道卡口全绿 + 新增第 4 道 sdk-sync）。
>
> **`v0.2.0` 代码 tag：阻断项已全部从"待修复"收敛为"待验证"** ——
> 原 7 个 P0（B-01…B-07）现在**条条有结论**：
> B-01 / B-02 / B-03 / B-05 / B-06 / B-10 / B-12 已修；
> B-07 / B-08 按契约边界显式声明闭合；
> **B-04 的根因证明不在被测系统**（测试侧 descriptor 字段号写反）。
>
> **仍未闭合的只剩两类**：
> ① **B-13 / B-14 的 §10 规则 4 正式确认**（文档流程，非代码）；
> ② **lab 侧复核**（B-04 修好后的 V8 复跑、V11 故障注入、E10 端到端验收）——
>   这些是"把结论再证一次的代价"，不是"还不知道能不能成立"。
>
> **诚实提醒（不可省略）**：`Ack` 根因在测试侧这一结论，先由"字段号 + **wire type**
> 的静态比对 + 负向对照"支撑（见上表 B-04 行），**后由 2026-09-20 的一次真实 lab 复跑
> 收口**：`AGENTS=1 CONCURRENCY=1` 单客户端跑 ⇒ `:poll-ack-failures **0**`、
> `delivered 105 = acked 105`、`:violations-by-class {}`，`Everything looks good!`
> （证据 `docs/production/evidence/20260920T133256Z-m5b-mq-poll-ack-single-client/`）。
> ⇒ 本项由**"分诊完成、待 lab 确认"**改记为**"lab 复跑确认"**。
>
> ⚠️ 同批归档的第二份（`CONCURRENCY=1n`，**每节点一个客户端**）是**红**的：唯一违反类
> `:mq-redelivered-after-ack 461`，而 `:poll-ack-failures` 仍为 **0**、`delivered = acked`
> （**无丢失**）⇒ 结论是 **checker 判据没把 Poll 请求的 `start_offset` 纳入**（每个客户端
> 各自从 0 起重放，被判成"ack 未记住"），记为 **F-68（jepsen checker 侧）**，
> **不得**据此记成"MQ at-least-once 不成立"。**两份证据必须一起引用** —— 只引绿的那份，
> 会让下一个跑 `1n` 的人重新花一天定位同一个误判。

> **发布判据（更新于 2026-09-19 本轮后）**：`contracts/v1.2.0` 的**契约面**已可发布
> （Minor，无 Breaking，卡口绿）。
> **`v0.2.0` 代码 tag 仍不成立**，但阻断项已显著收窄：
> - **B-01（F-50）已闭环** ⇒ registry / lock / election / idgen 四项的 GA 语义**不再被它卡住**；
> - **B-05 / B-06 / B-10 / B-12 已闭环** ⇒ 批次 7 的 scheduler 与 feature_flags 已具备
>   「重启不丢 + 多节点一致」的**可验证语义**；
> - **仍卡住的**：B-02 / B-03（lock / election 的 M2 前置）、**B-04（MQ at-least-once 不成立）**、
>   B-07 / B-08（Cache 副本一致性）、B-09（push 丢消息未移出承诺面）。
> **因此当前唯一真正的发布级 blocker 集中在 `mq` 与 `cache` 两项**；`lock` / `election`
> 另有 B-02/B-03。在这些闭环前打 tag，仍等于发布一份未兑现的承诺台账。
> **B-04 的下一步应是「分诊」而不是「修复」** —— F-67 原文即标「[待分诊]」，
> 在定位（能力点 / Leader 判定 / 拓扑）之前无法给出修复判据。

> **发布判据（更新于 2026-09-20 第四轮后；本节口径覆盖上面两个同名区块）**：
> - `contracts/v1.2.0` 的**契约面**可发布（Minor / 无 Breaking；三道契约卡口 +
>   `check-sdk-sync.sh` 全绿）。
> - `v0.2.0` **代码 tag 仍不成立**，但阻塞面已收窄为**非代码 / 待验证项**：
>   **V11 故障注入**、72h 浸泡、M3 / M4 验收、代码冻结与 tag、未决项
>   U1 / U3 / U4 / U5 / U6 / U8 / U9 的裁定。逐条状态以本文上两张表
>   （「本轮（2026-09-20 第四轮）新闭合」/「未落地」）为准。
> - **`B-04` 的状态以上表「未落地」为准**：**V8 复跑已由真实 lab run 确认转绿**。
>   本区块上方那处 "**B-04（MQ at-least-once 不成立）**" 与 "**B-04 的下一步应是「分诊」**"
>   属 **2026-09-19 第三轮之前**的旧口径，**已过时** —— 此处显式更正；不删旧行，
>   保持台账可追溯。
> - **卡口侧待修 `F-68`**（jepsen checker 未把 Poll 的 `start_offset` 纳入判据）
>   **不影响**发布判据（不在被测系统一侧），但应在 F-67 一并收口。
