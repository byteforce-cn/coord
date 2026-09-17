# coord Jepsen 测试缺口分析与补齐实施方案

> 版本：v1.0 ｜ 日期：2026-09-14 ｜ 基线：coord @ `ad22337`，jepsen/ 目录全量代码评审
>
> 定位：本文档是**黑盒对抗性验证**的缺口清单与施工方案。目的不是给 coord 发"通过证"，
> 而是系统性回答一个问题：**coord 契约（apis/contracts/）里的每一条语义承诺，是否有一
> 个能在故障下将其证伪的 Jepsen case？** 没有 case 的承诺 = 未验证承诺，是引入决策的
> 风险敞口；本方案把每个缺口落成可执行的 workload + checker 设计，使每条红 run 都能直
> 接转化为 coord 的缺陷单（倒逼证据）。

---

## 0. 摘要（给决策层）

- 现有 Jepsen 套件**工程质量高**：soak checker 精确、有 12 个 fixture 自测、客户端
  `:ok/:fail/:info` 映射纪律严谨、且已真实捕获过 coord 的 stale-read 缺陷
  （`partition-halves` / `partition-ring` 场景，见 `src/jepsen/coord/soak.clj` 头注释
  与 P0-1~P0-4 修正记录）。方法论已被证明有效。
- 但覆盖面对象极窄：**只验证了"单键寄存器读写 + 单键 CAS"这一条路径**。
- 对照 `apis/contracts/STATUS.md` 承诺台账：
  - **STABLE 底座 6 项**（KV / Txn / Lease / Watch / Maintenance / health）：
    Jepsen 有效覆盖约 **30%**（KV 的 Put/点查、Txn 的单键 VALUE EQUAL CAS、
    Maintenance/Status 探活）；**Lease、Watch 零覆盖**；KV/Txn 的多数契约条款未测。
  - **COMMITTED 能力面 5 项**（registry / lock / election / idgen / event，均有 GA 硬
    截止）：**Jepsen 零覆盖**。其中 Lock/Election 的互斥语义、IdGen 的唯一性语义是
    协调服务的生命线，且正是分布式系统最易在故障下失效的语义。
  - **EXPERIMENTAL 5 项**：不在引入路径，暂不要求。
- **72h soak 的结论边界**：当前 soak 模式只对 register workload 有效。cas-register 在
  soak 下会回退 knossos（`coord.clj` checker 函数），而 knossos 在 72h（~13 万 op）历
  史上必然 OOM——**72h 浸润对 CAS 路径得不出任何结论**。
- **结论**：以现状，72h soak 全绿只能为"单键线性一致读写路径的长期稳定性"注入信心；
  若引入用途包含 CAS 承载锁/选主、或任何 Lock/Election/Lease/Watch/IdGen 服务，需要
  先完成本文档 §4 的 P0 补齐项，再跑 72h，结论才成立。
- 缺口共 **18 项**（§3），其中 P0 六项（G-01/02/03/09/10/11）、P1 七项、P2 五项；
  全部附有契约原文引用、缺陷假设、checker 设计与验收标准（§4）。

---

## 1. 评估方法与判定标准

一个 case 算"覆盖"某条契约承诺，必须同时满足四条件（缺一则记为缺口）：

| # | 条件 | 反例（现状中的真实样本） |
|:--|:---|:---|
| C1 | **有 workload 触达该 API** | `proto.clj` 有 `delete-req` 但无任何 workload 调用 → Delete 不满足 C1 |
| C2 | **checker 能在该运行规模下检出违例** | cas-register 在 72h soak 回退 knossos → 满足 C1 不满足 C2 |
| C3 | **故障注入与该操作并发叠加** | reauth 路径有代码（`client.clj`）但无专门 nemesis 组合验证其边界 |
| C4 | **违例会反映到 `:valid?` 门禁** | `checker/perf` 只出报告不进 `:valid?` → 恢复时间无门禁 |

另有两条元规则：

- **R1（契约锚定）**：每个缺口必须引用契约原文条款（proto 注释即语义承诺），红 run
  的判定依据就是该条款，避免"测试自定义的严格度"扯皮。
- **R2（可证伪性）**：checker 必须说明自己的**漏检边界**（哪类违例它检不出），宁可声
  明弱结论也不声明假结论。soak checker 的 P0-1/P0-4 修正记录是正面范本。

与仓内其它验证手段的分工（避免重复建设）：

- `coord/tests/chaos_real.rs`：真实进程故障注入 + 进程内 register checker——覆盖注
  入面，但 checker 与客户端时序都在进程内，**无外部黑盒时序、无多形态分区矩阵、无
  历史持久化（history.edn）可供复盘**。
- `coord-server/tests/sim_chaos_test.rs`：文件内自建内存模型，源码自述"不得作为系统
  级混沌验证证据"。
- **Jepsen 的不可替代性**：外部客户端时序 + 持久化历史 + 严格 checker + nemesis 正交
  矩阵。本文档所有缺口均以 Jepsen 口径定义。

---

## 2. 现状覆盖矩阵

### 2.1 API 面

| 契约服务 | 承诺关键语义（契约原文摘要） | Jepsen 现状 | 判定 |
|:---|:---|:---|:---|
| `coord.kv` Put | 多数派提交后返回；`request_id` 幂等去重；`prev_kv`；`lease_id` 级联 | 仅裸 Put（单键、整数值） | **部分**（缺幂等/prev_kv/lease 绑定） |
| `coord.kv` Range | 默认线性一致读（ReadIndex）；范围扫描；`revision` 历史读（被压缩→OUT_OF_RANGE）；`keys_only`/`count_only` | 仅单键点查最新值 | **部分**（缺范围扫描/历史读/变体） |
| `coord.kv` Delete | 单键/范围删；`prev_kvs`；`request_id` 幂等 | **零**（`proto.clj:68` 有 builder 无调用） | **缺** |
| `coord.txn` Txn | compare AND 语义；4 种比较符 × 3 种比较目标；两分支内所有操作**单原子单元生效，中间状态对任何读者不可见** | 仅单键 VALUE EQUAL CAS | **部分**（原子性承诺完全未触达） |
| `coord.lease` | TTL 过期级联删除；**过期判定以服务端单调时钟为准**；Leader 切换后新 Leader 重建 TTL；不承诺精确过期时刻 | **零** | **缺** |
| `coord.watch` | 变更事件投递；`start_revision` 回放；历史已清理→HISTORY_UNAVAILABLE；`prev_kv` | **零** | **缺** |
| `coord.maintenance` Status | revision 探活 | 仅用于 readiness 探测 | **部分**（未用于滞后/一致性断言） |
| `coord.lock.v1` | **同一时刻同一锁名至多一个持有者**；TTL 未 Renew 自动释放；持有者校验；不承诺公平/可重入 | **零** | **缺** |
| `coord.election.v1` | **同一 group 同一时刻至多一个 leader**；TTL 续约；过期广播 | **零** | **缺** |
| `coord.idgen.v1` | **同名发号器 ID 全局唯一**、趋势递增；批量互异；时钟回拨防护（台账注明"落地或明确不承诺边界"） | **零** | **缺** |
| `coord.registry.v1` | 注册/心跳/发现/订阅；重复注册幂等（台账整改要点） | **零** | **缺** |
| `coord.event.v1` | 发布/订阅 at-least-once | **零** | **缺**（GA 2026-12-31 前需补） |
| `grpc.health.v1` | 标准探活 | 间接 | 足够 |

### 2.2 故障模型面

| 故障类别 | 现状 nemesis | 判定 |
|:---|:---|:---|
| 单点崩溃 kill -9（带数据目录重启 = 崩溃持久化） | `:kill` | ✅ |
| 全集群崩溃重启 | `:kill-all` | ✅ |
| 进程冻结（GC 停顿/时钟停摆窗口） | `:pause`（SIGSTOP/CONT） | ✅ |
| 网络分区：单点隔离 / 对半分 / 多数派环 | `:partition` / `-halves` / `-ring` | ✅ |
| 慢轮换浸润（30min 静 + 10min 扰动） | `:soak` | ✅ |
| **时钟偏移**（±N 分钟跳变） | 无 | **缺**（Lease/CCT/IdGen 均时钟敏感） |
| **网络降级**（时延/丢包/抖动/乱序，非全断） | 无 | **缺**（选举超时、lease 续约窗口） |
| **磁盘故障**（写满 / fsync 失效 / WAL 尾腐坏） | 无 | **缺**（kill -9 只验证健康磁盘） |
| **成员变更**（加/减/替换节点、learner 提升） | 无 | **缺**（线端 `Member*/Join` 存在但属红区，见 G-13） |
| **滚动重启 / 版本倾斜升级** | 无 | **缺** |
| 拓扑：5 节点 / 更多 region | 固定 3 节点（`coord.clj` 硬编码 `take 3`） | **缺** |

### 2.3 已验证有效的证据（方法学背书）

- soak checker 头注释：stale 判定"exactly the one that caught coord's stale-read bug"；
  P0-4 修正记录援引 2026-08-31 72h soak 与 2026-09-04 150s smoke 两次真实历史。
- 仓内 git log 亦显示 chaos 门禁多次逼出真实修复（如 `f3e33de` 流式 RPC 被鉴权层
  缓存 body 导致 watch 彻底不通、`5a720c5` `--data-dir` 覆盖配置）。
- **结论：对抗性测试对 coord 是有效的倒逼手段——缺口补齐的预期产出是真实缺陷单，
  不是走过场。**

---

## 3. 详细缺口清单

> 每项：契约锚点（R1）｜缺陷假设（这条 case 红了说明 coord 哪里坏）｜严重度。
> 严重度定义：**P0** = 引入路径上的承诺，无对抗性验证，阻塞引入；
> **P1** = STABLE 底座内未验证的契约条款；**P2** = 故障模型/拓扑缺口。

---

### G-01 [P0] CAS 在 72h 尺度上不可验证（checker 失能）

- **契约锚点**：`txn.proto` Compare 语义承诺（"比较与两个分支内的所有操作作为单个
  原子单元生效"）。
- **现状**：`coord.clj` checker 函数中，soak nemesis + cas-register → 打印 warn 后
  **静默回退 knossos WGL**。knossos 搜索在 ~13 万 op 历史上必然 OOM（`project.clj`
  注释自述 6g→12g 仍 OOM 过）。即 72h soak 对 CAS **恒无结论**，且以 warn 形式静默
  降级，不细看日志不会发现。
- **缺陷假设**：CAS 双成功（split-brain 下两客户端对同一 old 值都 CAS 成功 = 锁丢
  失的底层形态）、CAS 前提值伪造、长时运行后 CAS 状态机腐化——当前全部不可检出。
- **附带缺陷（对测试自身）**：回退应 hard-fail 而非 warn 降级（会出"假绿"报告）。
- **方案**：§4.1。

### G-02 [P0] Lock 互斥性零覆盖

- **契约锚点**：`lock/v1/lock.proto`："线性一致互斥：同一时刻同一锁名至多一个持有
  者"；"TTL 内未 Renew 即释放"；"仅 (holder_id, lease_id) 匹配者可 Release/Renew"。
- **现状**：无任何 Lock 调用。台账：COMMITTED，GA 硬截止 2026-11-30。
- **缺陷假设**：①双持有者（分区 heal 后旧 leader 路径的锁记录未过期 vs 新持有者已
  签发）；②TTL 提前过期（持有者活着且按时 Renew 但锁被他人抢走 = 生产锁抖动）；
  ③TTL 泄漏（持有者死了锁永远不释放 = 死锁）；④非持有者 Release 成功。
- **这是协调服务最具杀伤力的 case**：任何锁实现 bug 最终都表现为互斥违例。
- **方案**：§4.2。

### G-03 [P0] Election 唯一 Leader 零覆盖

- **契约锚点**：`election/v1/election.proto`："竞选原子：同一 group 同一时刻至多一
  个 leader"；"TTL 内无续约 → leader 自动过期并广播 LEADER_EXPIRED"。台账另注明
  "续约（重新 Campaign）语义验证"是整改要点；仓内 `remaining-known-gaps.md` B6 曾修
  过"exception-branch judge"的 P0 级选举缺陷（有负向对照测试）——**该路径出过大
  bug，且目前只有进程内测试守门**。
- **缺陷假设**：同 G-02 家族；另含 Campaign 续约被误判为新竞选、Resign 后旧 leader
  仍被 GetLeader 读出。
- **方案**：§4.3（与 G-02 共用互斥 checker 框架）。

### G-04 [P0] request_id 幂等承诺零验证

- **契约锚点**：`kv.proto` PutRequest.request_id："同一客户端身份下，重复提交相同
  request_id 的请求不会重复生效，返回首次执行的结果。【建议始终携带】"。
- **现状**：客户端每次随机生成 request_id（`client.clj` `request-id`），从不重发同
  id；soak.clj P0-4 注释甚至记录过"at-least-once re-issue to the new leader"被作为
  合法历史放行——**重试去重路径从未被断言过**。
- **缺陷假设**：超时重试被重复应用（计数类业务直接出错）、去重表在 leader 切换后丢
  失、去重表与快照/恢复交互失效（重启后旧 request_id 被重放成功）。
- **方案**：§4.4。

### G-05 [P0] Lease TTL 与级联删除零覆盖

- **契约锚点**：`lease.proto`："过期判定以服务端单调时钟为准，不受节点系统时钟调
  整影响"；"Leader 切换后由新 Leader 重建"；过期/Revoke 时绑定 Key 级联删除且删除
  事件正常投递 Watch。
- **缺陷假设**：①**提前过期**（承诺"不承诺精确过期时刻：实际回收可能略晚于 TTL"—
  —即契约只承诺方向：晚可以，早不行；提前回收 = 上层锁/注册抖动）；②Leader 切换
  后 TTL 被重置（变相永不过期）或丢失（立即过期）；③级联删除丢失（key 泄漏）。
- **方案**：§4.5。

### G-06 [P0] 成员变更 / 集群重配置零覆盖（条件性 P0）

- **契约锚点**：线端 `coord-proto/src/proto/maintenance.proto` 有
  `MemberAdd/MemberRemove/MemberPromote/Join`，契约维护层声明这些属"红区"（不在 v1
  对外承诺内）。**但若团队引入后需要扩缩容/换机，这条红区就是生产操作路径**。
- **现状**：`db.clj` 固定 3 节点静态 `initial_nodes`，`coord.clj` `take 3`。
- **缺陷假设**：联合共识（joint consensus）期间的脑裂、learner 追数据期间的读一致
  性、成员替换后已提交写丢失、快照传输给新成员的完整性。
- **定级说明**：固定 3 节点永不重配置 → 降为 P2；有任何扩缩容计划 → P0。
- **方案**：§4.6。

### G-07 [P1] Delete 全路径零覆盖

- **契约锚点**：`kv.proto` Delete（单键/范围删、`prev_kvs`、幂等）。
- **现状**：`proto.clj:64-75` builder 就绪，零调用——**补齐成本最低的高价值缺口**。
- **缺陷假设**：tombstone 在分区 heal 后复活（deleted key 重新可读）、范围删部分生
  效、删除后 revision/version 语义错乱。
- **方案**：§4.7。

### G-08 [P1] Range 范围扫描的多键快照一致性未测

- **契约锚点**：`kv.proto` RangeRequest（`range_end`/`limit`/`keys_only`/
  `count_only`）；MVCC 语义隐含"一次扫描 = 某一 revision 的原子快照"。
- **缺陷假设**：torn scan（扫描返回混合了两个事务代的中间态——契约 Txn 原子性承诺
  的读侧镜像）、limit 截断后的 `count` 错误、跨 region 扫描边界错乱（multi-raft
  模式下扫描跨 region 的边界语义完全是黑盒）。
- **方案**：§4.8。

### G-09 [P1] Txn 多操作原子性（多键 CAS/批量写）未测

- **契约锚点**：`txn.proto`："比较与两个分支内的所有操作作为单个原子单元生效，要
  么全部成功，要么全部不生效，**中间状态对任何读者不可见**"。
- **现状**：cas workload 的 txn 只含单 compare + 单 put。
- **缺陷假设**：多键 txn 的 partial apply 可被并发读者观察（撕裂读）；compare 的
  GREATER/LESS/NOT_EQUAL、VERSION/MOD_REV 目标路径实现缺陷（这些分支从未被外部调
  用过）；failure 分支误执行。
- **方案**：§4.9（代际撕裂检测）；进阶：Elle list-append（§4.9.2）。

### G-10 [P1] 历史 revision 读与 compaction 交互未测

- **契约锚点**：`kv.proto` RangeRequest.revision："指定历史 Revision 读取；若该
  Revision 已被压缩清理，返回 OUT_OF_RANGE"。
- **缺陷假设**：MVCC 版本链腐化（读到错误历史值）、compaction 误删存活 revision、
  OUT_OF_RANGE 边界判定错误；72h soak 隐式触发日志压缩/快照但无任何显式断言。
- **方案**：§4.10。

### G-11 [P1] prev_kv（Put/Delete）未测

- **契约锚点**：`kv.proto` Put.prev_kv / Delete.prev_kv。
- **缺陷假设**：prev_kv 返回伪造值或错乱旧值；更深一层：**两个不同 Put 返回相同的
  prev_kv = 该值被"覆盖"了两次 = 重复应用/split-brain 的直接证据**（唯一值前提下
  这是 O(n) 精确可判的）。
- **方案**：§4.11（并入 G-01 的链式 checker）。

### G-12 [P1] Watch 事件流零覆盖

- **契约锚点**：`watch.proto`：`start_revision` 回放、历史清理→HISTORY_UNAVAILABLE、
  `prev_kv`；`lease.proto` 级联删除事件"正常投递给 Watch 订阅者"。仓内史：watch 曾
  被鉴权层整体搞挂（`f3e33de`），且 scope 语义修错过一轮——**改动频繁、回归风险高**。
- **缺陷假设**：leader 切换/分区 heal 后事件丢失（at-least-once 变 at-most-once）、
  事件乱序、重复投递不幂等、revision 断档未发 HISTORY_UNAVAILABLE。
- **方案**：§4.12。

### G-13 [P2] 时钟偏移 nemesis 缺失

- **契约锚点**：`lease.proto` 单调时钟承诺；`idgen.v1` 台账整改要点"时钟回拨防护
  落地或明确不承诺边界"；CCT ~1h 过期（`client.clj` 注释）。
- **现状**：`:pause` 模拟停摆但不模拟偏移。
- **缺陷假设**：用墙钟做租约/CCT 判定 → 时钟回拨后租约永不过期或批量提前过期；
  snowflake 时钟回拨发重复 ID。
- **方案**：§4.13。

### G-14 [P2] 网络降级（时延/丢包/抖动）nemesis 缺失

- **现状**：iptables 全断分区三种形态已覆盖；无 `tc netem`。
- **缺陷假设**：高时延下选举风暴（反复换主、吞吐塌陷）、lease 续约窗口被时延吃掉
  导致的批量提前过期、raft 心跳超时参数在弱网下不适配。生产 IDC 网络抖动是常态，
  全断分区不是。
- **方案**：§4.14。

### G-15 [P2] 磁盘故障注入缺失

- **缺陷假设**：①磁盘写满后 crash-loop 或静默丢写；②fsync 失效（`dm-flakey`/
  LD_PRELOAD 注入）后已确认写丢失——直接击穿"coord fsyncs all writes before
  returning"（`db.clj` 注释）的耐久性前提；③WAL 尾部腐坏（kill -9 断电场景的真实
  形态）后节点无法启动或 checksum 兜底失效。
- **方案**：§4.15。

### G-16 [P2] IdGen 唯一性零覆盖

- **契约锚点**：`idgen/v1`："同名发号器内 ID 全局唯一"；批量"返回 count 个互异
  ID"。台账注明时钟回拨防护是待落地项。
- **定级 P2 而非 P0 的理由**：实现是 snowflake 本地发号，唯一性主要由 worker-id 分
  配保证；但 worker-id 在重启/扩缩容后的分配路径未测。若引入用途依赖 IdGen → 升 P0。
- **缺陷假设**：worker-id 冲突发重号、segment 模式 leader 切换后号段重叠。
- **方案**：§4.16。

### G-17 [P2] 恢复时间 / 可用性无 SLO 门禁

- **现状**：`checker/perf` 只出报告；恢复仅靠收尾 `until-ok`（limit 20）弱保证。
- **缺陷假设**：故障停止后 30s/60s/5min 才恢复线性一致读——正确但不可用，当前判
  valid。
- **方案**：§4.17（把 MTTR 变成 `:valid?` 门禁，产出倒逼 coord 优化选举/租约参数
  的量化证据）。

### G-18 [P2] 拓扑与参数矩阵过窄

- 固定 3 节点、soak 固定 0.5 ops/s + 1n 并发、region 数无矩阵。高并发竞争暴露依赖
  短跑 knossos（合理分工），但 5 节点、region>1 与故障叠加的组合空间未探索。
- **方案**：§4.18。

---

## 4. 补齐实施方案

> 每项给出：workload 设计 / checker 设计（含复杂度与漏检边界，遵守 R2）/ nemesis 矩
> 阵 / 72h 适配 / 改动文件 / 验收标准（红 = coord 的什么缺陷）/ 工作量估算。
>
> 通用基础设施先行（§4.0），后续各项复用。

### 4.0 通用基础设施（先行项，约 1.5d）

1. **soak 模式 + cas workload 改为 hard-fail**（修 G-01 的静默降级）：
   `coord.clj` checker 函数 warn 分支改 `throw`。
2. **value 编码方案**：为支持链式/幂等判定，soak 写值从纯计数器升级为结构编码
   `base*100 + attempt`（attempt ∈ 0..1，见 §4.4）或保持计数器+CAS 专用生成器
   （见 §4.1）。统一放 `jepsen.coord.values`（新文件）。
3. **通用写日志（write journal）**：把 soak.clj 的 `write-index` 抽成
   `jepsen.coord.journal`（新文件），供 G-01/04/07/08/10/12 的 checker 复用。
4. **新 gRPC 方法接入**：`CoordRpc.java` 补 Lock/Election/IdGen/Lease/Watch 的描述
   符（DynamicMessage 模式已成熟，照抄现有字段编号从契约 proto 抄）。
5. **时钟/网络/磁盘 nemesis 基元**放 `jepsen.coord.nemesis`（§4.13-4.15）。

### 4.1 G-01：CAS 的 O(n) soak checker（3d）

**workload**：soak 模式下 cas-register 的 `new` 值改用单调计数器（与 register soak
同源），`old` 从最近观察值池取样（现状保留）。

**checker**（新 `jepsen.coord.soak-cas`，复用 journal）：

- **链式唯一性（核心，精确）**：唯一值前提下，两个不同的 `:ok` CAS 拥有相同 `old`
  且 `new1 ≠ new2` ⇒ 线性一致下不可能（寄存器不会两次等于同一 old）。检出即红。
  检出缺陷类：split-brain CAS 双成功、事务重放。
- **前提真实性**：`:ok` CAS 的 `old` 必须是某 write/CAS 写入过的值（或 nil 初值），
  否则为伪造前提（fabricated-premise）。
- **读侧**：复用现有 fabricated/future/stale 三判定（soak checker 对 read 部分无需
  改动，CAS 的 new 值就是写值）。
- **漏检边界（R2 声明）**：不验证并发 CAS 之间的全部排序合法性（那是 knossos 的活）；
  链式唯一性 + 前提真实性 + 读侧三判定覆盖了全部**灾难性**类别（双成功/伪造/陈旧）。
- 短跑（time-limit ≤ 600）仍走 knossos cas-register 做全序验证，双轨互补。

**nemesis**：soak 轮换全谱。**72h 适配**：O(n log n)，与现 soak checker 同阶。

**验收**：注入"双 CAS 同 old 双成功" fixture 必红；接入既有
`run-soak-checker-tests.clj` fixture 框架（新增 4 个 fixture）。

**红 = coord 缺陷单**：CAS 双成功 → raft 线性化/事务判定路径缺陷；伪造前提 → MVCC
读路径缺陷。

**改动**：`coord.clj`（checker 路由 + hard-fail）、新 `soak_cas.clj`、
`scripts/soak-checker-fixtures/` 新增 fixture、`README.md` 选项表。

### 4.2 G-02：Lock 互斥 workload（4d）

**workload**（`:workload lock`）：N 客户端对**同一锁名**循环：
`Acquire(ttl=10s)` → 成功则记录 `(holder, lease, t_acq)`，持有 0.5~2s（其间可选
Renew），`Release`；失败记 `:fail acquired=false`（合法竞争，不计违例）。

**checker**（`jepsen.coord.mutex`）：

- **互斥判定（精确）**：任意两个 `:ok` Acquire 的持有区间
  `[t_acq, min(t_rel, t_last_renew + ttl)]` 不得重叠——重叠即红。区间右端用
  `last successful renew + ttl` 放宽，吸收"持有者失联后锁合法被抢"（契约允许）。
- **持有者校验**：非持有者 Release 返回 OK released=true → 红（契约承诺
  PERMISSION_DENIED）；持有者 Release 返回 PERMISSION_DENIED → 红。
- **Renew 边界**：Renew 返回 new_ttl=0 但后续 GetLockInfo 仍显示该持有者为 owner →
  状态不一致，红。
- **漏检边界**：持有区间以客户端时钟测量，控制机单点时钟、无分布式时钟误差问题；
  亚毫秒级的锁服务内部竞争窗口可能落在测量精度内——用持有期 ≥500ms 吸收。

**nemesis**：kill / partition-halves / pause 轮换（锁服务最怕 leader 切换与分区）。
soak 适配：Acquire 冲突率是天然负载，rate 0.5~2 ops/s 即可。

**验收**：单跑无故障 5min 必绿；构造"分区 heal 后双持有"历史 fixture 必红。

**红 = coord 缺陷单**：双持有 → 锁记录写路径未走线性化或 TTL 判定缺陷（最高优先
级）；提前过期 → 租约定时器缺陷；非持有者释放成功 → 鉴权/持有者校验缺陷（对照台账
"非持有者释放 PERMISSION_DENIED 对齐"整改要点，可直接复验）。

### 4.3 G-03：Election 唯一 Leader workload（3d，复用 4.2 框架）

**workload**（`:workload election`）：N 客户端对同一 group 循环
`Campaign(ttl=10s)`；当选者定期重新 Campaign 续约；随机 Resign。

**checker**：互斥判定同 4.2（`elected=true` 区间不重叠）；另加：
- GetLeader 读到的 leader 必须是某 `:ok` Campaign 的 candidate（伪造判定）；
- Resign :ok 后，GetLeader 在续约窗口后仍返回该 leader exists=true → 红；
- Watch 事件流（LEADER_ELECTED/RESIGNED/EXPIRED）与 Campaign/Resign 日志交叉一致
  （轻量版，完整 watch checker 见 §4.12）。

**红 = coord 缺陷单**：双 leader / 续约被当新竞选（B6 类缺陷的黑盒复验）/ Resign
不生效。

### 4.4 G-04：request_id 幂等验证（2d）

**workload**：在 register workload 上加客户端重试模式（`--idempotent-retry`）：
写超时（DEADLINE_EXCEEDED）后，用**相同 request_id**、value 编码为 `base*10+attempt`
重试一次（attempt=1）。

**checker**（并入 soak checker）：

- **去重生效判定（精确）**：任何 `:ok` 读观察到 attempt=1 的值 ⇒ 重复提交被重复生
  效，违反契约原文 ⇒ 红。（契约承诺"返回首次执行的结果"，故重试 :ok 时系统内只能
  是 attempt=0 的值。）
- **结果一致性**：重试 `:ok` 响应的 revision 必须等于首次写入的 revision（契约：
  "返回首次执行的结果"）。
- **重启后重放**：kill-all + 重启后，客户端用崩溃前的 request_id 重放一次——去重表
  必须随快照/日志持久化，重放不得再生效应答新 revision。
- **漏检边界**：去重窗口长度（服务端保留多久）无法黑盒探测，只能验证测试窗口内。

**红 = coord 缺陷单**：重试双生效（生产计数/扣减类业务事故的直接来源）；重启后去
重表丢失。

### 4.5 G-05：Lease 生命周期 workload（3d）

**workload**（`:workload lease`）：循环：`LeaseGrant(ttl=20s)` → `Put(key, lease_id)`
→ 半数租约 KeepAlive 续期、半数自然到期 → 轮询读 key 记录存活/消失时刻；另随机
Revoke。

**checker**（`jepsen.coord.lease-check`）：

- **提前过期（红，精确）**：距 grant/最后一次 :ok keepalive 不足 ttl 时 key 不可读
  ⇒ 红（契约只承诺"略晚"，不承诺"略早"）。
- **泄漏（红）**：超过 ttl + 宽限（60s，吸收"不承诺精确过期时刻"与 leader 重建）
  key 仍可读 ⇒ 红。
- **failover 重建**：lease 存活期间 kill 当前 leader，新 leader 必须在原 ttl 语义下
  继续（既不立即过期也不重置回满 ttl——用 grant 时刻+ttl 判定，宽限吸收时钟误差）。
- **级联+watch 联动**（依赖 §4.12）：级联删除必须产生 watch 事件。
- **漏检边界**：级联删除与 lease 过期之间允许有时延（契约未承诺原子级联），只判定
  最终一致 + 宽限。

**红 = coord 缺陷单**：提前过期 → 定时器/时钟源缺陷（生产锁抖动根因）；泄漏 → 回
收调度缺陷；failover 重置 → LeaseManager 状态重建缺陷。

### 4.6 G-06：成员变更 nemesis（4d，按需）

**设计**：新 nemesis `:membership`，驱动线端 Maintenance `MemberAdd(learner)` → 等
待 catch-up → `MemberPromote` → `MemberRemove` → 节点替换（remove 死节点 + add 新
节点）轮换，与 register workload + soak checker 叠加（checker 零改动——线性一致寄
存器在重配置下承诺不变，这本身就是断言）。

**前置**：需在 `db.clj` 支持 4-5 节点初始拓扑与动态配置下发；线端 Member* 属契约
红区，**本测试的产出之一是"这些红区接口在故障下的行为证据"，倒逼 coord 要么将其
纳入承诺面要么明确运维手册边界**。

**红 = coord 缺陷单**：重配置期间 stale/线性化违例 → joint consensus 缺陷；learner
读不一致；替换后已提交写丢失。

### 4.7 G-07：Delete workload（1d）

**设计**：register workload 增加 `:delete`（mix 比例 r:w:d = 5:4:1）；model 仍为
register（delete → nil）。soak checker：把 `:ok` delete 作为"写入 nil"进入
confirmed prefix（`soak.clj` 约 20 行改动）；knossos 侧 model/register 原生支持。

**红**：tombstone 复活（delete :ok 后无新 put，读却返回旧值——stale 判定的自然延
伸，现 checker 框架直接检出）。

### 4.8 G-08：Range 扫描一致性（3d）

**workload**（`:workload scan`）：写侧以 txn 多键批量写"代际值"（某代 g 把
k1..k10 全写成 g，g 单调）；读侧范围扫描 k1..k10。

**checker**：**撕裂扫描判定（精确，O(n)）**：扫描 S 显示混合代际，且存在代际 g 的
批量写 :ok 完成于 S 开始之前、且 S 中出现了 < g 的键值 ⇒ 撕裂（中间态对读者可见，
直接违反 txn.proto 原子性承诺）⇒ 红。in-flight 代际的混合合法（并发豁免，同 soak
checker 的 P0-1 纪律）。

**红 = coord 缺陷单**：MVCC 快照读实现缺陷；multi-raft 跨 region 扫描无快照语义
（倒逼契约补一条"扫描不跨 region 原子"或实现分布式快照读）。

### 4.9 G-09：多键 Txn 原子性（2d + 可选 Elle 5d）

**4.9.1 代际撕裂检测**：同 4.8 的写侧，读侧改为**单键点读**（便宜）：键 k1 读到代
g 而 k2 读到代 < g，且代 g 的 txn 已完成于两次读开始之前 ⇒ 撕裂 ⇒ 红。compare 全
分支（GREATER/LESS/NOT_EQUAL × VERSION/VALUE/MOD_REV）作为 generator 参数轮换，至
少保证每个分支被外部流量触达（触达率写进 soak summary，防"跑了但没测到"）。

**4.9.2（可选进阶）Elle list-append**：jepsen 自带 elle checker（knossos 同族），
对 txn 做 append/read 混合，可检出 G1/G2 等隔离级别违例；仅在 4.9.1 出红后需要精
确定位隔离级别时启用（计算贵，限短跑）。

### 4.10 G-10：历史 revision 读（2d）

**设计**：每次 :ok 点读记录 `(value, revision)` 进 journal；随后以 journal 中随机
历史 revision 发起 `RangeRequest.revision` 读：返回值必须等于该 revision 首次观察
到的值（**MVCC 保真判定，精确**）；OUT_OF_RANGE 只在服务端已 compact 之后合法
——compact 点位从 Maintenance/Status 或配置推得，判定时保守放行并计数（写入
summary，**计数异常高 = compaction 过激的间接证据**）。

**红**：历史值错乱（MVCC 链缺陷）；存活 revision 被 OUT_OF_RANGE（compaction 误
删）。

### 4.11 G-11：prev_kv 链（并入 4.1，0.5d）

soak 写全部带 `prev_kv=true`；判定：①prev 必须是 journal 中存在的值（伪造判定）；
②两个不同 :ok put 返回相同 prev ⇒ 同一值被"覆盖"两次 ⇒ 红（唯一值前提下精确）。

### 4.12 G-12：Watch 事件流 checker（4d）

**workload**：独立 watch 客户端订阅 register key（含 `start_revision` 回放与
`prev_kv`），事件流 `(value, mod_revision, type)` 全量记入 history（`:f :watch` 事
件 op）。

**checker**（`jepsen.coord.watch-check`）：

- **无伪造/无错乱**：事件值都在 journal 中；mod_revision 单调（同 watcher 内）。
- **无丢失（核心）**：journal 中每个 :ok 写（及 :ok delete）的 mod_revision 必须出
  现在事件流中——除非期间收到 HISTORY_UNAVAILABLE（契约允许的逃生门），或连接断
  开在重连窗口内（以客户端重连 op 为界豁免）。
- **乱序/重复**：revision 回退或同 revision 重复投递（重连重建窗口外）⇒ 红。
- **漏检边界**：watcher 与 writer 同在控制机，事件到达时延不计；只验证完整性/顺序/
  真实性，不验证时效。

**红 = coord 缺陷单**：leader 切换后事件流断档（无 HISTORY_UNAVAILABLE 兜底）→
watch 可靠性缺陷（仓内史证明该路径回归高发）。

### 4.13 G-13：时钟偏移 nemesis（1d）

`nemesis.clj` 新增 `:clock-skew`：选随机节点 `date -s "+90s"` / `"-90s"`（lab 节点
无 NTP 或先 `timedatectl set-ntp false`），stop 时恢复。与 lease/lock/idgen workload
叠加：**判定由各自 checker 完成**（提前过期/重号在时钟跳变下出现即红——直接验证
"过期判定以单调时钟为准"承诺）。

### 4.14 G-14：netem 降级 nemesis（1d）

`:netem-delay`（100ms±50ms）、`:netem-loss`（10%）、`:netem-flap`（周期通断）作用于
raft 端口（50052）与/或 grpc 端口（50051）。判定：register workload + soak checker
（弱网下不得出现线性化违例）+ G-17 的可用性门禁（弱网窗口内 UNAVAILABLE 比例写入
summary，超阈值红）。

### 4.15 G-15：磁盘故障 nemesis（3d）

- `:disk-fill`：fallocate 占满 data_dir 所在分区，写侧必须报错但不 crash-loop、
  已提交数据不丢（heal 后 soak checker 判定）。
- `:fsync-fail`：LD_PRELOAD 拦截 fsync 注入 EIO（需重新 start 进程带 env，`db.clj`
  start-coord! 支持注入 env）。**已 :ok 的写在 kill -9 后必须存活**——违反即击穿
  db.clj"fsyncs all writes before returning"的前提，红。
- `:wal-corrupt`：kill -9 后截断 WAL 尾 4KB 再重启：节点必须（a）以 checksum 检测
  并截断恢复，或（b）明确拒绝启动并从其他副本恢复——**不允许带着腐坏日志上线服
  务**（ soak checker 判定数据一致性，启动状态写入 summary）。

### 4.16 G-16：IdGen 唯一性 workload（2d，按升级条件）

**workload**（`:workload idgen`）：多客户端并发 `NextId(name)` /
`NextBatch(name, 100)`，id 全量入 history。

**checker**：全量 id 集合查重（O(n) 精确）；batch 内查重；**趋势递增**按契约只报
告不判红（"不承诺严格递增"）；时钟偏移/kill 叠加下重复 ⇒ 红。segment 模式
（step>0）单独一个 run。

**红**：worker-id 冲突（重启后再分配碰撞）/ segment 重叠 → 直接命中台账"时钟回拨
防护落地或明确不承诺边界"整改要点。

### 4.17 G-17：MTTR / 可用性门禁（1d）

soak checker 扩展：统计每次 nemesis stop → 首个 :ok 读的时延（recovery-latency）与
扰动窗口内 UNAVAILABLE 占比；门禁（可配 `--max-recovery-seconds`，默认 60s）入
`:valid?`。**产出即倒逼证据**：把"恢复慢"从感觉变成数字。

### 4.18 G-18：拓扑矩阵（1d，纯配置）

`coord.clj` 去掉 `take 3` 硬编码（改 `--nodes-count`），lab 支持 5 节点；soak 脚本
增加 `--regions 3/8` 矩阵与 `--concurrency 3n` 高档位；CI nightly 轮换。

---

## 5. 分阶段执行计划与 72h 协议

### 阶段 0（准入，1 周）：P0 六项

| 序 | 项 | 内容 | 工时 |
|:--|:---|:---|:--|
| 1 | §4.0 | 基础设施 + cas soak hard-fail | 1.5d |
| 2 | §4.1 | CAS O(n) soak checker | 3d |
| 3 | §4.4 | request_id 幂等 | 2d |
| 4 | §4.2/4.3 | Lock + Election 互斥 | 5d（并行可压） |
| 5 | §4.5 | Lease 生命周期 | 3d |

出口标准：上述 case 短跑（10min × kill/pause/partition-halves 三种 nemesis）全绿，
且各自的红 fixture 验证有效（负向对照）。

### 阶段 1（引入决策依据，1 周）：72h 浸润矩阵

| Run | workload | nemesis | 时长 | checker |
|:--|:---|:---|:--|:--|
| S1 | register + delete + 幂等重试 | soak | 72h | soak（扩展版） |
| S2 | cas-register | soak | 72h | soak-cas（§4.1） |
| S3 | lock + election | soak | 72h | mutex（§4.2/4.3） |
| S4 | multi-register --regions 3 | soak | 72h | soak（分组） |

四跑全绿 + MTTR 报告 → **此时"72h 浸润"才能为核心引入注入信心**（对比：现状只
有 S1 的阉割版可跑）。任何一跑红 → 转 §6 缺陷流程，修复后重跑该 run（不必全部重
跑）。

### 阶段 2（契约完整性，2 周）：P1 七项

§4.7-4.12 全部落地；每项先短跑矩阵（10min × 3 nemesis）后进一轮 24h 浸润。

### 阶段 3（环境鲁棒性，1 周）：P2 五项

§4.13-4.18；时钟/弱网/磁盘与阶段 1 矩阵正交叠加各跑 24h；成员变更（若启用）单独
72h。

### 持续化

- 所有 case 进 `make test` 矩阵轮换（nightly 短跑 + 周末 24h）；
- coord 每次发版前：阶段 1 矩阵短跑版（每项 30min）为门禁；
- fixture 库随缺陷单增长（每个 coord 修复必须附一个能复现该缺陷的历史 fixture）。

---

## 6. 倒逼机制：红 run → coord 缺陷单 → 契约台账联动

### 6.1 红 run 的证据包（每个缺陷单必含）

1. `history.edn` + checker 输出的违例 op 集合（已支持，`soak.clj` 返回 `:failures`）；
2. **违例类别 ↔ 契约条款**对照（本文 §3 各条的"契约锚点"即模板）；
3. 最小复现：`scripts/validate-soak-checker.clj` 可对裁剪后历史复判；
4. 环境指纹：coord commit、配置 TOML、nemesis 时间线（`scripts/nemesis-timeline.clj`）。

### 6.2 对 coord 的倒逼清单（预期产出）

| 触发 | 倒逼动作 |
|:---|:---|
| 任一 P0 case 红 | coord 缺陷单 + 修复 + 回归 fixture；修复前该项从"可引入能力"清单移除 |
| 契约承诺无法设计 checker（如发现某承诺含糊到不可证伪） | 倒逼契约澄清语义（R1 的反向运用：不可测的承诺 = 无承诺，须改写或删除） |
| Lock/Election 等 COMMITTED 项 GA 截止前无对应 case 全绿记录 | 倒逼 `STATUS.md` 状态回退或 GA 延期——**GA 门禁增加"Jepsen 全绿"硬条件** |
| 红区接口（Member* 等）被生产操作依赖 | 倒逼 coord 将其纳入承诺面 + 补契约条款，或提供受支持的替代运维路径 |
| MTTR 超阈值（G-17） | 量化报告倒逼选举/租约参数与恢复路径优化 |

### 6.3 引入决策的最终表述模板

> "coord 引入范围 = {已通过 Jepsen 阶段 1 矩阵 72h 全绿的能力子集}；{Lock/Election}
> 于 {日期} 通过 72h 互斥性验证（run id …）；{Watch/Registry/Event/IdGen} 未通过或未
> 测，引入后禁止使用，待阶段 2/3 收口后重新评估。"

**任何不在该表述内的能力宣称，均视为未验证承诺。**

---

## 附录 A：checker 精确性边界总表（R2 遵守）

| checker | 精确检出 | 声明漏检 |
|:---|:---|:---|
| soak（现） | fabricated / future / stale 读、不收敛、恢复率不足 | 并发写间全序合法性 |
| soak-cas（§4.1） | CAS 双成功、伪造前提、读侧三类 | 并发 CAS 全序 |
| mutex（§4.2/4.3） | 双持有/双 leader、持有者校验违例 | 测量精度（<持有期）内竞争 |
| 幂等（§4.4） | 重试双生效、重启后重放生效 | 服务端去重窗口长度 |
| lease（§4.5） | 提前过期、泄漏、failover 重置 | 级联删除的时延（最终一致+宽限） |
| 撕裂检测（§4.8/4.9） | 已提交代际的混合可见 | in-flight 代际的可见性 |
| watch（§4.12） | 丢事件、乱序、伪造、断档无兜底 | 事件时效 |
| idgen（§4.16） | 重复 ID | —（唯一性判定完备） |

## 附录 B：负向对照纪律

每个新 checker 合入前必须：①手写一个含对应违例的 fixture 判红（沿用
`scripts/soak-checker-fixtures/` 命名规约 `expect-invalid-*.edn`）；②干净历史判绿；
③CI 跑 `run-soak-checker-tests.clj` 全 fixture 通过。**没有负向对照的 checker 不
得作为门禁**（仓内 B6 的 negative-controlled 先例即此纪律）。
