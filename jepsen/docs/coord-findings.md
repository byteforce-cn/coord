# coord 侧发现 / 缺陷单（倒逼清单执行记录）

> 定位：Jepsen 任务的产出不是"通过证"，而是**缺陷单**。本文件是缺陷单的登记处：
> 每条 = 契约锚点（R1 契约锚定）+ 证据（源码位置 / run 产物）+ 缺陷假设 +
> 复现路径（哪个 task 的哪个 workload 会证伪它）+ 分级（§6）+ 状态。
>
> 状态口径：
> - `open` —— 静态审计已确认与契约不符/未实现，**待 coord 修**；
> - `needs-run` —— 静态不可判定（实现细节/时序相关），由指定 task 的 run 判决；
> - `confirmed-by-run` —— 已被某个 run 复现（附 store 路径）；
> - `closed` —— 已修，且回归在跑（附回归 run 的 store 路径）。
>
> 本轮基线：coord `ad22337`（v0.1.0），jepsen/ 全量静态审计 + 既有 store 产物复核。
> 每条"证据"都可直接点击核对，避免"评审说断言过严"的扯皮。

## 0. 本轮摘要

> **本轮更新（2026-09-16，第二轮）**：T1.4 幂等专项落地并在 **docker lab 真实跑通**，
> F-01/F-02/F-03 由静态审计升级为 **`confirmed-by-run`**（零故障注入即 100% 复现，
> 见 §F-01）。同时真跑暴露出 **4 条测试自身的缺陷（F-09…F-12）** —— 全部是
> 「报告看着正常、断言其实没生效」这一类，已修 + 补负控制 fixture。

| # | 一句话 | 分级 | 状态 | 判决任务 |
|:--|:--|:--|:--|:--|
| F-01 | `Delete` 不参与幂等去重，但契约明确承诺了 `request_id` 去重 | P1 | **`closed`**（已修 + 回归：154 组 / 352 次重放 0 违反） | T1.4 / T1.1 |
| F-02 | `Put` 幂等**命中**时 `prev_kv` 恒为 `None`，与"返回首次执行的结果"不符 | P2 | **`closed`**（已修 + 同上回归） | T1.4 / T1.1 |
| F-03 | 幂等缓存是**单节点进程内**（TTL 60s / 4096 FIFO），不随 raft 复制、不持久化 → 换节点/换 leader/重启后重放会重复生效 | P1 | **`confirmed-by-run`**（换 leader 重放：13 组 revision-advanced / 13 组 version-over-advance）+ 契约已限缩 | T1.4 ⇒ T5.2 |
| F-04 | `scripts/nemesis-timeline.clj` 把 `history.edn` 的纳秒当 epoch 毫秒输出 | P2（测试自身） | `closed` | 上一轮已修 |
| F-05 | 60s 短跑即可见 `:fail :write [:no-client Failed to authenticate to coord]`（鉴权/登录限流） | P1 | `confirmed-by-run`（**形态已定位：非限流，是登录路径需 raft quorum**，见本节末） | §5.4-④ / E4 |
| F-06 | Watch 语义既不是 coalescing 也不是 lossless：**缓冲区满时丢弃最旧事件 + 合成 `BufferOverflow`** | — | `open`（**改计划**） | T2.1 前置 |
| F-07 | 磁盘写满行为**已定义**：可用 <5% → 写 `RESOURCE_EXHAUSTED`、读仍可用 | — | `closed`（§9-② 已答） | T3.5 |
| F-08 | Lease 到期判定用**单调时钟**（`tokio::time::Instant`），契约成立 | — | `closed`（§9-⑤ 已答） | T2.2 / T3.4 |
| F-09 | `gates` 的四个门槛参数用 destructuring `:or` 解析 → 调用方显式传 nil 时**门槛静默失效** | P1（测试自身） | `closed`（本轮修 + 新 fixture） | T0.2 |
| F-10 | docker lab 无外网 → plain `lein` 挂到超时；节点无 chrony/ntpq → `--strict-clock` **恒红** | P1（测试自身） | `closed` | T0.1 / T0.4 |
| F-11 | `clojure.core/cycle` 预求值 → nemesis 节拍**恒定**，T0.6 抖动作废（且旧判据抓不到） | P1（测试自身） | `closed` | T0.6 |
| F-12 | `results.edn` 内嵌 knossos tagged literal → `summarize-results.clj` 抛错 → MANIFEST 门槛结论恒为 "unknown" | P2（测试自身） | `closed` | T0.1 |
| F-13 | `txn-req` 用了不存在的 `DynamicMessage$Builder.addAllField` → **cas / exists / txn-\* 从未真正发出请求**，knossos 对全 `:info` 历史判 valid（假绿） | P1（测试自身） | `closed`（修构造 + 新增 G6 空转门槛） | 全部 Txn 形态 |
| F-14 | `invoke-read` 把读值当整数解析 → map workload 读到非数值就抛异常、整类 `:read` 变成 `:info` | P1（测试自身） | `closed`（`parse-values?` + `:raw-value`） | T1.1 |
| F-15 | `read-at` 在无缓存 revision 时直接记 `:info`（实测 46/48）→ G6 判红；且历史读被当成陈旧读 | P1（测试自身） | `closed`（先点读取 `mod_revision` + 历史读排除出陈旧读判据） | T1.3 |
| F-16 | map 的 knossos 路径用 completion 分组（丢掉 invoke op）→ knossos `history/complete` 直接崩 | P1（测试自身） | `closed` | T1.1 |
| F-17 | scan 写索引只喂 `:ok` 写 → `:info`（可能已生效）写的值被后续扫描读到就判 `:fabricated`（假红） | P1（测试自身） | `closed` | T1.3 |

---

## F-01 [P1] `Delete` 未实现 `request_id` 幂等去重

**契约锚点（R1）** —— `apis/contracts/proto/coord/kv/kv.proto`
```
77: message DeleteRequest {
81:   bytes request_id = 4; // 可选：幂等去重键（语义同 PutRequest.request_id）
```
`PutRequest.request_id`（第 44–45 行）的定义是："同一客户端身份下，重复提交相同
`request_id` 的请求不会重复生效，返回首次执行的结果。"

**证据（静态）** —— `coord-server/src/server/mod.rs`。幂等缓存只有两个消费点：

| 行 | 内容 |
|:--|:--|
| 1423 | `put` 入口 `self.check_idempotent(&idempotency_key(...))` |
| 1492 | `put` 提交后 `self.cache_idempotent(...)` |
| 1892 | `txn` 入口 `self.check_idempotent_txn(...)` |
| 2055 | `txn` 提交后 `self.cache_idempotent_txn(...)` |

`delete` 处理器位于第 1650 行，**其函数体内没有任何 `*_idempotent*` 调用**：
`ensure_writable()`（1655）之后直接进入路由/读取 `prev_kvs`/提交路径。

**缺陷假设** —— 一次"已生效但响应丢失"（`DEADLINE_EXCEEDED` /
`UNAVAILABLE`，客户端按 `client.clj` 的规则记为 `:info`）的 Delete 重试会在
新节点上**再次执行**：

1. 计数/审计类语义重复（"删除了一次却记了两次"）；
2. `prev_kv=true` 的响应不可幂等：首次返回被删旧值，重试返回**空 `prev_kvs`**
   —— 同一个 `request_id` 的两次调用得到不同结果，与"返回首次执行的结果"
   直接矛盾；
3. 范围删在重试时若区间内已被新写入补齐，会删掉**首次执行之后新写的数据**
   —— 这是"重试放大破坏面"的形态，比重复计数严重。

**复现路径** —— T1.4（`request_id` 幂等专项）必须覆盖 Delete，不能只测 Put：
同一 rid 重放 ≤3 次，断言 (a) `prev_kvs` 三次一致，(b) 重放期间新写入的 key
不被删除。T1.1 的 delete workload 提供自然重放机会。

**分级** —— P1（有界可绕过：客户端可改用以"读-判-删"补偿；但契约承诺未兑现，
且范围删场景存在真实破坏面）。

### F-01 判决（2026-09-16，`confirmed-by-run`，零故障注入）

`--workload idempotency --nemesis none --time-limit 45`（无任何故障注入）：
每一次 delete 重放都真的又执行了一遍，**47/47 个 delete 分组全中**。

| 证据 run | seed | delete 分组 | `:delete-count-mismatch` | `:delete-prev-kvs-mismatch` | `:replay-deleted-new-write` |
|:--|:--|:--|:--|:--|:--|
| `20260916T132103Z-t1.4-idempotency` | 1727727433 | 47 | **47** | **47** | **39 / 39** |
| `20260916T132105Z-t1.4-idempotency-seed42` | 42 | 48 | **48** | **48** | **45 / 45** |

- `:delete-count-mismatch` = 首次执行返回 `deleted=1` + `prev_kvs=[旧值]`，
  重放返回 `deleted=0` + `prev_kvs=[]` ⇒ 同一个 `request_id` 两次得到不同结果，
  与「不会重复生效，返回首次执行的结果」直接矛盾。
- **`:replay-deleted-new-write` = F-01 预测的第 3 条（重试放大破坏面）已被复现**：
  范围删首次执行 `:ok` 之后、重放之前写入的 key，被重放**删掉了**（39/39、45/45）。
  这是数据破坏，不是计数问题。
- 判定不是门槛假红：同一 run 的 `:gates` 全绿（premise 值唯一、RTO p95 ≈ 0.4s、
  quiet 窗口样本足够），`:linear` 的失败**全部**来自上述三类。

**coord 侧动作**：给 `delete` 补幂等（入口 `check_idempotent`、提交后
`cache_idempotent`，并把完整 `DeleteResponse`（`deleted` / `prev_kvs` /
`revision`）一起缓存供命中时回放）。范围删必须缓存 `prev_kvs` 与 `deleted`，
否则「返回首次执行的结果」无法满足。

---

## F-02 [P2] `Put` 幂等命中时 `prev_kv` 恒为 `None`

**契约锚点** —— 同 F-01：`request_id` 语义是"返回首次执行的结果"；
`PutResponse.prev_kv`（kv.proto:50）承诺 `prev_kv=true` 时填充覆盖前旧值。

**证据（静态）** —— `coord-server/src/server/mod.rs` 第 1420–1430 行：

```rust
// 幂等检查：相同（客户端身份 + request_id）返回缓存的 revision
if !request_id.is_empty() {
    if let Some(cached_rev) = self.check_idempotent(...) {
        return Ok(tonic::Response::new(PutResponse {
            prev_kv: None,            // ← 恒为 None
            revision: cached_rev,
        }));
    }
}
```

幂等缓存条目只存 `revision`（`IdempotentEntry`，第 928 行的 `cache_idempotent`
只写 `revision`），**没有存 `prev_kv`**。对照 R-SVC-18 已给 Txn 缓存补上了完整
`responses`（第 2055 行），Put 这条路径漏了同样的修复。

**缺陷假设** —— 依赖 `prev_kv` 做 CAS 后置校验 / 审计的客户端，在重试路径上
会收到 `prev_kv: None`，把它解读为"该 key 此前不存在"——**静默的错误结论**，
比直接报错更危险。

**复现路径** —— T1.4：同一 rid 两次 `Put{prev_kv: true}`，断言两次
`prev_kv` 字节一致（首次为旧值，第二次也必须是旧值而不是 `None`）。

**分级** —— P2（不损坏数据；但会让上层做出错误判断，纳入 T1.4 一次修掉）。

### F-02 判决（2026-09-16，`confirmed-by-run`，零故障注入）

T1.4 里每 3 个 put 分组有 1 个先做一次无 rid 的 setup 写（把 key 变成"已存在"），
于是首次执行的响应真的带 `prev_kv`。

| 证据 run | seed | 带 setup 的 put 分组 | `:prev-kv-mismatch` | 不带 setup 的 put 分组 | 其中的 mismatch |
|:--|:--|:--|:--|:--|:--|
| `20260916T132103Z-t1.4-idempotency` | 1727727433 | 26 | **26（全部）** | 24 | **0** |
| `20260916T132105Z-t1.4-idempotency-seed42` | 42 | 25 | **25（全部）** | 20 | **0** |

两个数字合起来正好说明问题：**幂等命中路径确实命中了**（不带 setup 的分组里
`revision` 完全一致、`prev_kv` 一致），**但命中时把 `prev_kv` 丢了**。所以
F-02 不是"去重没生效"，而是"去重生效了但返回的不是首次执行的结果"。

**coord 侧动作**：`IdempotentEntry` 里存完整 `PutResponse`（含 `prev_kv`），
命中时整体回放。修完这条，`:prev-kv-mismatch` 应从 26 归零、而
`:delete-*` 与 `:replay-deleted-new-write` 仍红（那是 F-01，另一处改动）。

---

## F-03 [P1] 幂等去重的作用域：单节点、进程内、60s TTL、4096 FIFO

**这是 T1.4 ⇒ T5.2 硬前置的判决结论**（§2 依赖图的闸口）。

**证据（静态）** —— `coord-server/src/server/mod.rs`

| 位置 | 事实 |
|:--|:--|
| 76–91 | `idempotency_ttl = 60s`，`idempotency_max_entries = 4096` |
| 113–118 | 键 = 客户端身份哈希（8B，取自 `authorization` metadata）+ `request_id` |
| 121–160 | 存储 = `HashMap` + `VecDeque` FIFO 淘汰，容量满时**淘汰最旧**（不是拒绝） |
| 210–211 | 字段 `idempotent_cache: RwLock<IdempotencyCache>` 挂在 `CoordNode` 上 |
| 全文件 | 该缓存**从不进入** raft 日志 / 快照（无 `Command::*` 携带它） |

**结论（三个必须写进 T5.2 设计决策的推论）**：

1. **换节点即失效**：`client.clj` 在 `UNAVAILABLE` 时轮换到下一个节点
   （`try-nodes`），而缓存是每个节点各自的内存表 → 重试落到**另一个节点**
   时 100% 重复生效。agent 的重试路径（T5.2）正是"重连后重放"，命中该形态。
2. **换 leader / 重启即失效**：进程重启后缓存为空；缓存不随快照恢复。
   T6.1 的 72h soak 里 kill/restart 频繁，任何依赖去重的上层都会遇到。
3. **60s/4096 是**有界**窗口**：超出窗口或容量后静默重复生效（且 FIFO 淘汰
   会让"最近写入的 key"也可能被挤掉，因为淘汰按插入序而非访问序）。

**因此**：`request_id` 去重只能作为**单节点、60 秒内的尽力去重**使用；
任何"恰好一次"语义必须由**幂等业务语义**（版本号/CAS/唯一键）承担。

**复现路径** —— T1.4 的 run 必须包含"重试落到不同节点"的分支（用 nemesis 制造
leader 变更后重放同 rid），断言 version 前进 >1 即为复现。

**分级** —— P1（行为有界、有明确的绕过方式；但契约措辞未限缩作用域，
文档需明确"单节点、60s、4096 容量、FIFO"四个边界）。

---

## F-04 [P2，测试自身] `history.edn` 的时间单位被文档写错（已修）

**证据** —— 真实 store：`jepsen/store/coord/latest/history.edn`，`--time-limit 60`
的 run，`:time` 取值范围 `[489938198, 120021454004]`，即 **0.49s → 120.0s 的
单调纳秒时钟**；`history.txt` 的相对秒（0/2/3/31）与之吻合。
而 `scripts/nemesis-timeline.clj` 把头注释写成 "epoch seconds (ms -> s)" 并
`(/ (:time op) 1000.0)` —— 输出的"epoch 秒"整个是错的（差 6 个数量级），
任何用它做时间相关推理的结论都不可信。

**处置** —— 本轮已修（改为相对首 op 的秒，`/ 1e9`），并在
`jepsen.coord.gates` 的 RTO 计算里按纳秒实现（`nanos-per-second 1.0e9`），
用真实 store 回归：RTO 样本 `[0.38 0.87 1.80 1.82 1.03]s`，量级合理。

**教训** —— 单位这类"元数据"错误会让后续所有量化门槛失效，属于 §0-4
可复现性的一部分；新增任何按时间取值的断言都必须先在真实 store 上验证量级。

---

## F-05 [P1] 60 秒短跑就能看到鉴权失败："`no-client Failed to authenticate`"

**证据（run 产物）** —— `jepsen/store/coord/latest/history.txt` 第 31 行：

```
31      :fail   :write  462885  [:no-client Failed to authenticate to coord]
```

这是一个 `--time-limit 60 --concurrency 1n --regions 3 --nemesis partition-ring`
的短跑，**不是** soak。客户端已实现 RESOURCE_EXHAUSTED 退避重试
（`client.clj` 的 `authenticate!`：per-IP 令牌桶 ~0.5 token/s，2s 退避），
但这条 `:fail` 说明退避在 60s 内没能救回来。

**缺陷假设** —— 登录限流（E4）与"节点刚起来/刚被 kill 重启"的窗口叠加时，
**合法客户端会被自己的限流拒之门外**，表现为 `:no-client` 写失败。这既是可用性
问题（E4 的观察项），也是**测试假红风险**：`:fail` 一多，§5.1 的"`:fail` 仅限
白名单"就会把 run 判红，而根因是鉴权而不是一致性。

**复现路径** —— `make test NEMESIS=kill-all TIME_LIMIT=60 CONCURRENCY=1n` × 3
次，统计 `:fail :write [:no-client ...]` 出现率；同时观察服务端登录限流日志。
T5.2（经 agent）会放大这个问题（agent 也要登录）。

**分级** —— P1（不损坏数据；阻塞"短矩阵干净绿"的目标）。

### 本轮定位（2026-09-21，静态复核 + 证据普查）——**纠正原"限流"假设**

**证据普查**：全量 `jepsen/store/coord/*/history.txt` 里出现 `Failed to authenticate to coord`
的 run 共 **7 个**（`2026-09-06`、`09-16` ×2、`09-19` ×3，以及归档的
`docs/production/evidence/20260916T122844Z-baseline-partition-ring-60s/` 4 条）。其中
最新的 3 个（`09-19T04:25/04:32/04:35`）**全部是 M5b 的经 agent MQ workload**
（失败 op 一律是 `:mq-publish` / `:mq-poll`）。

**形态（否定"登录限流"这条假设）**：登录限流**不会**挡住合法客户端 ——
`allow_attempt` 只**查看**令牌不消耗（`coord-server/src/auth/service.rs`），只有
**密码校验失败**才 `record_failure` 消耗令牌。因此 `Failed to authenticate` 与限流无关。

**真正形态**：`AuthService::authenticate` 在签发前要**两次 raft 提案**
（`persist_session` ×2：auth token + refresh token，`auth/service.rs:456`）。
follower 上 `propose_auth_op` 返回 `UNAVAILABLE`（带 leader hint）；**分区/无 quorum 期间无法提交**。
客户端在 `auth-timeout-ms = 60000`（`client.clj:18`）内轮换全部 channel 仍失败 ⇒ 抛
`Failed to authenticate to coord`，被 `result-op` 记成 `:fail`。

⇒ 结论：`:fail` 的根因是「**登录路径的可用性 = raft quorum**」，属 §5.4-④/⑤ 的**参数裁定项**
（分区时长 vs 客户端登录超时）＋一个**可选加固**（给 `persist_session` 加有界重试以覆盖选举窗口），
而**不是**一个限流缺陷。可达加固见
`docs/production/ops/boundaries.md` §5 B-SE-4。

**待办（W1-2）**：lab 复跑统计出现率，并对「0 条」这一判据做裁定 ——
quorum 整体丢失超过客户端登录超时时，登录**必然**失败，属固有可用性属性，
不能靠改断言消除（§7 证据规范第 4 条：不得弱化断言）。

### 复跑（2026-09-21）——**本 run 未复现**，但判据已改写

**调用**：`make -C jepsen/lab test WORKLOAD=register NEMESIS=kill-all TIME_LIMIT=60
CONCURRENCY=1n SKIP_CHECKERS=1 JEPSEN_PROVIDER=docker`（binary sha256 前 8 位 `24cd089d`）。
归档：`docs/production/evidence/20260921T163732Z-w1-2-f05-kill-all-60s/`。

| 项 | 值 |
|:--|:--|
| 判决 | ✅ `:valid? true` / `:gates {:valid? true}` / 退出码 0 |
| `:fail` / `:no-client` | **0 / 0**（5 轮 `kill-all` 下） |
| RTO | p95 2.31s / max 2.53s（预算 120s），`unrecovered 0` |
| 可用率门槛 | `quiet-judged 0 / skipped-small-sample 5` ⇒ **本 run 不构成对 0.95 可用率的任何证据** |

**边界（与结论同引）**：只有 1 次 run（原复现路径要求 ×3）；形态不完全同源
（原出错的多是**经 agent** 的 M5b MQ run 与 `partition-ring`；本 run 是 kill-all +
register + 默认 AGENTS=2）。⇒ 只能得出「**本 run 未复现**」，**不能**得出「F-05 已消失」。

**判据修正（关键）**：原 W1-2 的「60s 短跑 0 条 `:no-client`」在 **quorum 整体丢失**时
**不可能成立**（`Authenticate` 要提交两次 `persist_session`），把它当缺陷去修只能改断言。
可行的判据是**两类失败必须可区分**：密码错 = `UNAUTHENTICATED`（不可重试）；
无 quorum = `UNAVAILABLE` / `DEADLINE_EXCEEDED`（**必须**可退避重试，否则客户端会
把自己锁在登录限流里，即 `:no-client` 风暴的成因）。已由
`coord-server` `auth::service::cct_tests::session_persist_failure_propagates_retryable_code`
（含负控制：改成 `unauthenticated` ⇒ 必红）钉住；本 run 是它的端到端对照。

---

## F-06 [改计划] Watch 语义：丢弃最旧 + 显式 `BufferOverflow`（§9-⑧ 的答案）

**契约锚点** —— `watch.proto` 的声音只到"at-least-once / 历史清理
→ HISTORY_UNAVAILABLE"，没说缓冲区压力下怎么办。

**证据（静态）** —— `coord-server/src/watch/mod.rs`

| 行 | 内容 |
|:--|:--|
| 228 | "非阻塞：如果某订阅者缓冲区已满，`丢弃最旧事件`并发送 `BufferOverflow`。" |
| 267 | "recv 后检查标志并合成 `BufferOverflow`，保证通知必达" |
| 90–94 | 回放水位 `[start_revision, watermark]`，实时事件从 `watermark+1` 续，订阅者按 revision **去重** |

**对计划的影响（本文件与 §5.4-⑥ 需要改）** —— 计划里
`--watch-semantics coalescing|lossless` 两个选项**都不对**：

- **不是 lossless**：缓冲区满会丢事件（丢最旧，不是拒绝订阅）；
- **不是 coalescing**：不做合并/压缩，丢就是丢；
- **实际语义**：`at-least-once 有序投递 + 溢出时丢最旧 + 必须收到显式
  BufferOverflow 标记（标记之后允许缺口）`，且**去重靠 revision**。

→ T2.1 的 checker 应参数化为 `--watch-semantics coalescing|lossless|overflow-marker`，
默认 `overflow-marker`；零违反断言改为：**要么事件序列完整且有序，要么在缺口处
之前出现过 `BufferOverflow`**。§5.4-⑥ 的"确认内容"也要同步改成这个三态。

---

## F-07 [§9-② 已答] 磁盘写满：可用 <5% → 写 `RESOURCE_EXHAUSTED`，读仍可用

**证据（静态）** —— `coord-server/src/storage/disk_watermark.rs:6`
"ReadOnly：剩余空间 < 5%，写请求返回 `RESOURCE_EXHAUSTED`（读仍可用）"；
`coord-server/src/server/mod.rs:329` `ensure_writable()` 在 **6 个写入口**调用
（行 1411 `put` / 1655 `delete` / 1883 `txn` / 2088 / 2172 / 2746），
`range`（1505）不调用 → 读路径不受影响。

**结论** —— 这是**已定义**的行为，不是"未定义"（§9-② 可闭环）。T3.5 可以写
确定性断言：填满磁盘后 (a) 写返回 `RESOURCE_EXHAUSTED`，(b) 读仍成功，
(c) 恢复空间后写自动恢复（不需要重启）。

---

## F-08 [§9-⑤ 已答] Lease 到期判定使用单调时钟，契约成立

**证据（静态）** —— `coord-server/src/lease/mod.rs:43`
`tokio::time::Instant::now() >= self.deadline` —— `tokio::time::Instant` 是
**单调**时钟（不受墙上时钟调整影响），符合 `lease.proto`
"过期判定以服务端单调时钟为准"。

**对 T2.2 / T3.4 设计的影响（重要）** —— 计划里 T3.4 的"时钟偏移
nemesis（±N 分钟跳变）"对 lease **不会产生任何作用**（实现不看墙钟），
如果照跑会得到"时钟故障下 lease 完全正常"的假绿结论。真正能证伪的故障是：

- **`pause`（SIGSTOP）**：单调时钟在冻结期间**照常前进** → 冻结超过 TTL 的
  leader 恢复后会发现自己的租约已到期。这才是单调时钟承诺的检验点。
- **进程重启**：`deadline` 是进程内相对时间，重启即丢 → 必须由 raft 重建
  （契约要求新 leader 重建 TTL），这条要用 kill 测。

→ T3.4 应更名为"时钟域故障"，实现为 `pause`（长冻结）+ `kill`（重启丢
deadline），而不是墙钟跳变；§5.4-⑤（lease grace）仍是必要条件。

### F-03 判决补充（2026-09-16）

**同 leader / 60s 窗口内，去重是真实生效的**：32 个不带 setup 的 put 分组里
`revision` 全部相同，`prev_kv` 全部一致（`prev-kv-mismatch` = 0），
没有一次 `:revision-advanced`。这既证实了 F-03 描述的机制，也排除了
"`check_idempotent` 压根没接上"这种更严重的假设。

**仍未复现的部分（需要故障注入，列为 T1.4 的收尾跑）**：`--nemesis kill-all`
+ `--idem-replay-delay-ms 1500` 让重放跨过重启/换主 —— 此时新 leader 的缓存为空，
预期 `:revision-advanced` / `:version-over-advance` 由 0 变为非 0。本轮只跑了
`--nemesis none`（对照组），该分支**尚未执行**，不得记为已覆盖。

### F-03 判决（2026-09-16 第三轮，`confirmed-by-run`）

**先记一条负面结果**（免得后人重走）：`--idem-replay-node-offset N`（把重放的
**首次尝试**从缓存 leader 往后挪 N 个节点）**不能**产生跨节点重放 —— 写请求只能
由 leader 服务，非 leader 会返回 `UNAVAILABLE` 并被 `try-nodes` 轮转回 leader，
于是重放又落在同一个节点上、命中同一个缓存。实测 offset=1 的 run：344 次重放
0 违反。**跨节点重放必须真的发生 leader 变更/进程重启**。

**有效的复现配方**：`--nemesis kill --idem-replay-delay-ms 4000 --time-limit 150`
（kill-one 每 3–8s 随机杀一个节点；4s 的重放间隔足以让部分分组跨过 leader 变更）。
在**已修完 F-01/F-02** 的二进制上跑（所以这些违反纯粹来自缓存作用域）：

| 证据 run | 重放尝试 | 违反分类 |
|:--|:--|:--|
| `20260916T150552Z-t1.4-cross-node-f03` | 425 | `:revision-advanced` **13**、`:version-over-advance` **13**、`:prev-kv-mismatch` **13**、`:delete-count-mismatch` **19**、`:delete-prev-kvs-mismatch` **19** |

对照（同一二进制、`--nemesis none`、45s）：`20260916T150557Z-t1.4-regression-after-f01-f02-fix`
—— 154 组 / 352 次重放，**0 违反**。两者合起来把结论钉死：**同 leader 内去重
生效；换 leader 后重放会重复生效**（F-03 成立），而 F-01/F-02 是另一回事、已修。

**倒逼动作（本轮已落地）**：不改实现（去重本就是重试优化，不是线性一致性
承诺），而是**把契约措辞限缩到实际作用域** ——
`apis/contracts/proto/coord/kv/kv.proto` 的 `PutRequest.request_id` /
`DeleteRequest.request_id` 与 `txn.proto` 的 `TxnRequest.request_id` 现在明确写了
「单节点 / 进程内 / 不复制不持久 / TTL 60s / 4096 FIFO / 换节点会重复生效」，
并要求上层自己用业务幂等语义承担「恰好一次」。这同时是 T5.2（agent 重试路径）
的设计前提。

---

## F-01 / F-02 修复与回归（2026-09-16 第三轮）

**coord 侧改动**（`coord-server/src/server/mod.rs`）：

1. `IdempotentEntry` 新增 `prev_kv`（Put）、`deleted` / `prev_kvs`（Delete）三个
   载荷字段，并加 `IdempotentEntry::with_revision` 构造函数；
2. `check_idempotent` → `check_idempotent_entry`（命中时取**整条**条目）；
3. 新增 `cache_idempotent_put` / `cache_idempotent_delete`；
4. `put` 命中时回放 `entry.prev_kv`（此前硬编码 `None`），提交后连同 `prev_kv`
   一起缓存；
5. `delete` 入口新增幂等命中检查（回放 `deleted` / `prev_kvs` / `revision`）、
   提交后缓存完整 `DeleteResponse` —— 这是 F-01 的核心补丁，含范围删。

**回归**（`--workload idempotency --nemesis none --time-limit 45 --concurrency 1n
--seed 42`，证据 `20260916T150557Z-t1.4-regression-after-f01-f02-fix`）：

| 指标 | 修复前（`20260916T132103Z-t1.4-idempotency`） | 修复后 |
|:--|:--|:--|
| 分组 / 重放尝试 | 136 / — | 154 / 352 |
| `:prev-kv-mismatch` | **26** | **0** |
| `:delete-count-mismatch` | **47** | **0** |
| `:delete-prev-kvs-mismatch` | **47** | **0** |
| `:replay-deleted-new-write` | **39** | **0** |
| `:version-over-advance` | 0 | 0 |
| 门槛（`:gates`） | 绿 | 绿 |

也就是说：**同 leader 窗口内的四条幂等承诺全部成立**（不重复生效 / 返回首次
执行的结果 / 不放大破坏面 / 删除不复活），剩下的唯一边界就是 F-03 的
「换节点/重启后失效」—— 那是已限缩的契约边界，不再是缺陷。

---

## F-09 [P1，测试自身] `gates` 的门槛参数被 `:or` 静默吃掉

**现象** —— `make quick` 的 `:gates` 报告里 `:availability {:min-ratio nil
:min-sample nil ...}`，即 quiet 可用率门禁的**两个门槛都是 nil**。

**根因** —— Clojure 的 destructuring `:or` 只在键**缺失**时生效，键存在但值为
`nil` 时**不**接管（实测 `(let [{:keys [a] :or {a 5}} {:a nil}] a)` => `nil`）。
而 `coord.clj` 的 checker 组合总是显式传入这些键：

```clojure
:gates (gates/checker
         {:nemesis (:nemesis opts)
          :quiet-availability-min (:quiet-availability-min opts)   ; 未给 CLI 选项 => nil
          :quiet-min-sample       (:quiet-min-sample opts)         ; 同上
          :max-rto-seconds        (:soak-max-rto-seconds opts)
          :check-premise?         (not= :idempotency (:workload opts))})
```

后果分两档：窗口为空时**静默失效**（报告里 `:min-ratio nil`，`:valid?` 仍是
true）；窗口非空时 `(>= total nil)` / `(< ratio nil)` 直接抛 NPE。两者都让
「T0.2 的硬门槛」名存实亡 —— 而它正是 T0.4/§5.2 判定的基石。

**处置** —— 两处 checker（`gates.clj`、`idem.clj`）改为在函数体内用 `or` 解析
默认值，并新增 `scripts/gates-fixtures-defaults/`：一个 150 样本 / 0.90 可用率的
quiet 窗口，**只能用生产默认门槛（100 / 0.95）判出 invalid**，`make checkers`
每次都跑它。用 `:or` 的写法在这个 fixture 上会 NPE（= 门禁变红），无法再退化。

**分级** —— P1（测试自身；不产出错误结论，但会让门槛失效并产出假绿）。

---

## F-10 [P1，测试自身] 离线 lab 上 `lein` 挂死 / `--strict-clock` 恒红

两个独立的可用性缺陷，都会让「每 run 前 preflight 必须全绿」（§0-5）无法满足：

1. **plain `lein` 挂到超时**：docker lab 的 control 容器没有出网。`lein run`
   /`lein classpath` 会卡在依赖解析上（实测 `lein -e '(println :hi)'` 4 分钟
   无输出，`lein -o classpath` 0.9 秒返回）。`make quick`/`make checkers`/
   `collect-evidence.sh` 全部受影响 —— 而 `collect-evidence.sh` 把 stderr 丢掉、
   只留 stdout，所以它表现为**MANIFEST 里门槛结论恒为 "summary unavailable"**。
2. **`--strict-clock` 恒红**：jepsen-docker 的 node 镜像里没有 chrony/ntpq，
   `env-reset.sh` 于是对每台节点都判 `FAIL no clock sync tool`，环境**永远不干净**。

**处置** ——
1. lab Makefile 的 `LEIN_RUN`/`checkers`/`smoke` 与 `collect-evidence.sh` 的
   摘要器默认加 `-o`（离线）；`ONLINE=1` 可覆盖。
2. 时钟校验换成可测的基准：真正要保证的是**节点之间**的钟差（Lease/CCT 判定
   的输入），控制机就是天然基准。`env-reset-all.sh` 用 NTP 式夹逼采样
   （`t0` 发送前 / `t1` 节点当前时间 / `t2` 收到后，`offset = t1-(t0+t2)/2`，
   不确定度 = `RTT/2`）量出偏移，再交给节点脚本判定；判定规则改为
   **只在可证超过上界时 FAIL**，精度不足时 WARN 并打印不确定度。
   高 RTT 链路上本来就无法审计 100ms 时钟，把它当 FAIL 只能产出假红。

实跑结果（`make env-reset STRICT_CLOCK=1`）：control 与 n1..n5 全部 clean，
节点侧报告 `clock offset Nms +/-Mms (control-bracket)`（本机 RTT ≈ 270ms，
故为 WARN；偏移点估计 < 150ms）。

**分级** —— P1（测试自身；F-10-2 直接违反 §0-5，F-10-1 让证据链断在摘要这一步）。

---

## F-11 [P1，测试自身] nemesis 节拍其实**恒定**：T0.6 抖动作废

**现象** —— 真跑的 nemesis 时间线是完美周期的：

```
   6.64 start     13.08 stop  |  20.92 start   27.36 stop  |  35.18 start ...
gaps: 6.42 1.20 6.64 6.42 1.19 6.64 6.42 1.19 6.64      <- 三次周期一模一样
```

**根因** —— `(clojure.core/cycle [a b c d])` 会**先求值那个向量一次**，再无限
重复它的元素。于是 `(nemesis-beat opts)` 在整个 run 里只被调用过一次，
"3–8s 抖动"退化成一个常数。`single-nemesis-gen`、`combined-nemesis-gen`、
`soak-nemesis-gen` 三个生成器**都**是这个写法（soak 的 ±20% 静/动窗口同样作废）。

**为什么旧判据抓不到** —— T0.6 的验收写的是"相邻 nemesis op 间隔 ∈ [3,8]s"。
6.42s/6.64s 落在区间里，判据**通过**；但抖动为零。判据必须看**取值的离散度**。

**处置** ——
1. 三个生成器改为惰性构造（`mapcat` over `(iterate inc 0)`，用 `iterate`
   而不是 `range`：后者分块，一次预取 32 个周期）；
2. `scripts/nemesis-timeline.clj` 增加节拍统计与自检：打印 `n`/`distinct`/
   `min`/`max`，周期数 ≥ 6 而不同取值 < 3 时直接告警"抖动疑似未生效（F-11）"。

修后同一场景实测 `n=15 distinct=13 min=1.16 max=7.52`。

**分级** —— P1（测试自身；G3 缺口的证据全部作废，且旧判据会给"已覆盖"的假结论）。

---

## F-12 [P2，测试自身] knossos tagged literal 让 MANIFEST 的门槛结论恒为 "unknown"

**现象** —— `collect-evidence.sh` 生成的 MANIFEST 里 `overall :valid?` 是
`unknown`，`summary.txt` 内容是 `summary unavailable ...`。

**根因** —— `summarize-results.clj` 用默认 `clojure.edn/read-string` 读
`results.edn`，而 register / cas-register run 的 test map 里内嵌 knossos 的
**tagged literal**（`#knossos.model.Register{...}`），默认 reader 没有该 tag 的
reader function，直接抛 `No reader function for tag knossos.model.Register`。
脚本 stdout 被写进 `summary.txt`、stderr 被丢掉，于是失败表现成"没结论"。

**处置** —— 给未知 tag 一个原样保留的 `:default` reader；`collect-evidence.sh`
的摘要器顺带改为离线调用（F-10-1）。修后 MANIFEST 才能给出真实门槛值
（例：`rto-p95-seconds 0.44 / quiet-judged 0 / premise-valid true`）。

**分级** —— P2（测试自身；不产出错误结论，但让 T0.1 的"可核对摘要"失效，
评审时无法从归档里读出门槛结果）。

---

## 9. 与 coord 侧的接口（倒逼清单）—— 当前状态

| # | 问题 | 状态 | 依据 |
|:--|:--|:--|:--|
| ① | `request_id` 去重语义/窗口 | **已答 + 已复现**（去重存在：单节点/60s/4096 FIFO，不复制不持久；同 leader 内 `revision` 完全一致；Delete 无去重、范围删重放会删掉新写入；Put 命中丢 prev_kv） | F-01/F-02/F-03 + T1.4 runs |
| ② | 磁盘写满行为 | **已答**（<5% ReadOnly，写 RESOURCE_EXHAUSTED、读可用） | F-07 |
| ③ | `snapshot_logs_since_last = 0` 语义与文档 | `needs-run` | T3.2 前置，读源码后补 |
| ④ | agent 读缓存在分区/重连窗口的一致性 | `needs-run` | T5.2（预期最重要发现） |
| ⑤ | Lease 计时基准 / clock bump 下行为 | **已答**（单调时钟；墙钟跳变无效，pause/重启才是有效故障） | F-08 |
| ⑥ | Lock Release 的 fencing 校验 | `needs-run` | T5.3 |
| ⑦ | 跨 Region Txn/Range 拒绝的错误码与无副作用 | `needs-run` | T4.2 |
| ⑧ | Watch 是否允许合并/压缩 | **已答**（丢最旧 + 显式 BufferOverflow；计划选项需改） | F-06 |
| ⑨ | key 的 version 起始值 / "不存在"的表示 | **已答**（version 新建为 1、跏号更新时递增；delete 也 +1（`mark_deleted`）；**软删除被所有读路径与 compare 过滤**，所以「不存在」= version 0） | F-18（本轮源码确认 + T1.1 的 `:exists` op 落地实施 |

**已答的 ①②⑤⑧⑨ 中，① 与 ⑧ 需要 coord 侧动作**（① 已修：补 Delete 幂等 +
`prev_kv` 回放；⑧ 待更新契约文字明确溢出语义），其余是测试侧设计修正。

---

## 10. 本轮（2026-09-16 第三轮）新增：真跑暴露的测试自身缺陷

这一批全部是「第一/二轮的断言在真跑里根本没生效」那一类 —— 它们的共同点是
**报告看着是绿的**。其中 F-13 尤其值得单列：它让 coord 的 **全部 Txn 形态**
（CAS / 存在性 compare / create-if-absent / 多 op 写集）在本轮之前从未被真实
执行过，而 `cas-register` 矩阵与 map 矩阵都是绿的。

### F-13 [P1，测试自身] `txn-req` 构造失败 → 所有 Txn op 变成 `:info`，knossos 判绿

**现象** —— `--workload cas-register` / map 的 `:exists` / `:txn-*` 全部
completion 为 `:info`，`:error` = `indeterminate: No matching method
addAllField found taking 2 args for class
com.google.protobuf.DynamicMessage$Builder`。

**根因** —— `jepsen.coord.proto/txn-req` 用 `(.addAllField b field v)`：
`DynamicMessage$Builder` 上**没有**这个方法（只有 `addRepeatedField` /
`setField`）。异常在客户端里被 jepsen 捕到并记成 `:info`，**请求从未发出**。

**为什么是假绿** —— knossos 对一份「全是 `:info`」的历史没有任何可判的 op，
于是 `valid? = true`；T1.2 的 txn checker 同理（`fails` 为空）。

**处置** ——
1. `txn-req` 改用 `setField` + List（repeated 字段的正确用法）；
2. **新增 G6「op 级活性」门槛**（`jepsen.coord.gates`，见 §5.5-5）：按 `:f`
   分组的每类客户端 op，完成数 ≥ `--min-op-sample`（默认 10）时 `:ok` 率必须
   ≥ `--min-op-ok-ratio`（默认 0.1）。这道门槛上线后立刻又抓到 F-13 的第二个
   形态（`set-rep` 的 arity 写错）—— 也就是说它确实回答的是「这条路是不是
   压根没通」。
3. 负控制：`scripts/gates-fixtures/expect-invalid-op-liveness.edn`（12 个 `:cas`
   全 `:info` → 必须 invalid）与 `expect-valid-degraded-but-alive.edn`
   （`:ok` 20% / `:info` 80% → 必须依然 valid，防止门槛调成「一有 :info 就红」）。

**分级** —— P1（测试自身；但后果是「一整个 RPC 面未被覆盖」而报告是绿的，
按 §0-2「不得弱化断言」这类必须当缺陷处理）。

### F-14 [P1，测试自身] `invoke-read` 把读值当整数解析

`Long/parseLong` 面对 T1.1 的任意字节串（`--value-size` 填充的 `vs42-12-xxx`）
抛 NumberFormatException → 被记成 `:info`。实测 map 的 60s run：`:read` 17 `:ok`
/ 62 `:info`（同一次 run 的 `:exists` 28/28 全 `:info` 是 F-13）。
**处置** —— `parse-value` 改为容错（解析不了返回 nil）；新增
`CoordClient.parse-values?`（`:register`/`:cas-register`/`:multi-register`/
`:idempotency` 为 true）与 `:raw-value`（原样字节串），mapck 的读判据用
`:raw-value`。

### F-15 [P1，测试自身] `read-at` 在无缓存 revision 时直接 `:info`

T1.3 的 `read-at` 依赖「本客户端写过这个 key」才有 revision 可读；实测 48 个
`read-at` 里 **46 个** 落在 `:info :no-cached-revision` 上 —— 于是 G6 因
「几乎没有 `:ok`」把 run 判红（**反过来也说明 G6 有效**）。另外第一版
checker 把历史读的结果当成了「最新读」去跑陈旧读判据，60s run 就假红。
**处置** —— 无缓存时**先点读**，用该 key 当前的 `mod_revision` 当历史 revision，
并把点读结果记在 `:latest-kv`；新增断言「按自己刚读到的 mod_revision 读历史，
必须拿回同一个值」（`:read-at-latest-mismatch`）；历史读的结果**排除**出
陈旧读/fabricated 判据（那是「当时的值」，不是最新值）。

### F-16 [P1，测试自身] map 的 knossos 路径丢掉 invoke op

`windex/pair-invokes` 只返回 completion；把它当作 knossos 的输入，
`knossos.history/complete` 直接抛 `Assert failed: Process completed an
operation without a prior invocation`（第一次 `WORKLOAD=map` 实跑即报）。
**处置** —— knossos 路径改为在**原始历史**上按 `:key` 分组（保留
invoke/completion 配对）；O(n) 索引仍用 `pair-invokes`。

### F-17 [P1，测试自身] scan 写索引只喂 `:ok` 写 → `:info` 写的值被判 fabricated

`:info` 写（响应丢失）**可能已生效**，它的值必须可以被后续读合法观察到。
第一版把 `writes` 过滤成 `:ok` 才喂给 `write-index`，于是 `--nemesis kill`
下的 run 一次就假红（`:fabricated`）。
**处置** —— 喂入 `:ok` + `:info`，由 `write-index` 内部用 `:confirmed` 区分
「已生效」与「可能生效」（与 `jepsen.coord.soak` 同口径）。

### F-18 [§9-⑨ 已答] version 起始值 / 「不存在」的表示

**证据（静态）** —— `coord-server/src/storage/mvcc.rs`：

| 位置 | 事实 |
|:--|:--|
| `KvMetadata::new_key` | 新建 Key 的 `version = 1`，`create_revision = mod_revision = 首次写的 rev` |
| `KvMetadata::update` | 已有（含**软删除**）Key 再次 Put → `version + 1`（**不会重置为 1**） |
| `KvMetadata::mark_deleted` | delete 也 `version + 1` 且 `deleted = true` |
| `evaluate_compares_in_tx` | `deleted=true` 的元数据被过滤，`version`/`mod_revision` 取 `unwrap_or(0)` |
| 各读路径（`get`/`range`/`range_in`） | 同样过滤 `deleted=true` |

**结论**：`Compare{VERSION, EQUAL, 0}` 就是**精确的存在性判定**（不存在 ⇔ 过滤后
version = 0 / 即 `deleted=true` 也算不存在）。T1.1 的 `:exists` op 就按这个写，
并且在负控制里固定住（`expect-invalid-exists-fabricated` / `-stale`）。
注意：**version 不是「写次数」**（delete 会 +1，且软删除后的 Put 继续递增），
所以任何用 version 做「写了几次」推断的断言都是错的 —— idem checker 的
`:expected-version` 就是这么定的（首次写 1、带 setup 2、删后为 0）。

---

## 11. 本轮（2026-09-17 第四轮）新增

### F-19 [P1，测试自身] watch 的 oneof 字段塞了普通 map → 整类 op 变 `:info`、0 事件

**现象**（首次实跑 `--workload watch --nemesis none 45s`）：69 个 `:watch-session`
completion **全是 `:info`、`sessions-with-events 0 / events 0`**。报告顶部：

```
:liveness {... :by-op {:watch-session {:total 69, :ok 0, :info 69 ...}}}
:note "有 op 类几乎从不成功：该路径没真的跑（F-13 类假绿）"
```

**根因**：`proto/watch-req` 把 `WatchRequest` 的 oneof 字段 `create` 设成了
**Clojure map**（`p/open-watch` 的调用方传的是普通 map，而不是
`watch-create-req` 的产物）。运行期报错：

```
java.lang.IllegalArgumentException: Wrong object type used with protocol message
reflection. Field number: 1, field java type: MESSAGE,
value type: clojure.lang.PersistentArrayMap
        at jepsen.coord.proto$watch_req.invokeStatic(proto.clj:347)
```

**为什么值得单独立项**：它与 F-13/F-14/F-15 是**同一形态**（构造期错误把整类 op
变成 `:info`），而且这次发生在「客户端每个 op 的必经路径」上。区别是**这次不是靠
人眼发现的**：三条自己造的网同时报警 ——
① **G6 op 级活性门槛**（`watch-session` `:ok` 率 = 0 ⇒ 判红）；
② watch checker 的**样本门槛**（0 事件 < 200 ⇒ `:insufficient-sample`）；
③ 顶层 `:valid? false`。
即：**新 workload 的第一跑没有产出任何假绿**。

**修复**：`p/open-watch` 直接接受 map 或已建好的消息
（`(if (map? create) (watch-create-req create) create)`），并在 docstring 里写明
这条宽容的理由 —— 让「忘了转」在接口层不可能发生，而不是靠调用方记得。
同时把 `:window-ms` / `:max-events` / `:resumes` 的**默认值在生成器里解析**
（CLI 选项 default 为 nil 的约定下，`(long (or nil 0))` 会把 `resumes` 变成 0，
正好关掉 T2.1 的核心路径「流中断后按契约 resume」）。

**教训（写进 §5.5 判据补充）**：凡「把一个面接进 jepsen」的第一跑，必须先看
**事件/op 计数**（`:events` / `:by-op`），再看 `:valid?`。`:valid?` 在「一条 op
都没成功」时也会是 `true`（knossos 对空历史判 valid），这正是 G6 与样本门槛存在
的理由。

### F-20 [P2，测试自身] watch 会话把「最后一次重开的起点」当成 op 的起点 → 6 条假红

**现象**（F-19 修好后的第一跑 `--workload watch --nemesis kill 120s`）：
`:events 971 / :resumes 40`（续传路径真的跑到了），但
`:violations-by-class {:watch-event-before-start 6}` ⇒ 红。

**根因**（诊断脚本 `/tmp/t2/diag-watch.clj` 读 `results.edn` 的结果）：

```
:watch-event-before-start | checker-start-revision 102 | requested-start :last
    early events: [[:put 99] [:put 100]]
    op sessions: [[n1 99 100 UNAVAILABLE: Network closed ... 2]
                  [n1 101 101 UNAVAILABLE: io exception 0]
                  [n1 102 102 UNAVAILABLE: io exception 0]]
```

会话内重开时，客户端把 `loop` 的 `start` **重新绑定**为「本次尝试的起点」
（101、102），最后汇报 op 的 `:start-revision` 时用的就是这个被覆盖的变量 ——
于是 op 自称「从 102 开始」，而它携带的事件来自第一次尝试（99、100），
checker 判「收到了起点之前的事件」。**服务端行为是对的**：第一次尝试确实按
`start_revision = 99` 发了 99/100，后两次尝试连流都没建起来（`io exception`，
正是 kill 造成的），自然没有事件。

**修复**：把 op 级的起点 `start0` 与「当前尝试的起点」`start` 分开（`:sessions`
里本来就逐次记录了每次尝试的起点）。

**教训（写进 §5.5 判据补充 10）**：**汇报字段必须与「被汇报的事实的粒度」一致**。
一次 op 内部做了 N 次尝试时，op 级字段只能是「整次 op 的不变量」（这里是**初始**
起点），每次尝试的量必须放数组（`:sessions`）。混淆这两者会造出一条**看起来像
被测系统违约**的假红 —— 而它的方向是「更容易相信的假红」（有具体 revision 数字、
有 UNAVAILABLE 记录），比假绿更难自查。

### F-21 [P2，测试自身] watch 的 `:end-revision` 取自重绑定的循环变量 → 7 条假红

**现象**（F-20 修好后的重跑 `--workload watch --nemesis kill 120s`）：
`:events 1101 / :resumes 66`，`:watch-event-before-start` 归零，但出现
**`:watch-event-loss 7`**（按 checker 的定义这是 P0 级：确认写落在会话覆盖区间内
却没有事件、且没有溢出标记）。

**根因**：这 7 条 op 的 `:events` **全是空**（三次尝试都 `UNAVAILABLE: io exception`，
一个事件都没收到），而客户端汇报的 `:end-revision` 是
`(long (or (:revision (peek events')) start))` —— 没有事件时退化成**循环里最后一次
尝试的起点**（例如 op-start 108、end 110），于是 checker 认为覆盖了 `(108, 110]`，
把区间里的确认写全判成「丢失」。

与 **F-20 同形**（派生字段取了重绑定的循环变量），但**同一处只修了一个字段**：
F-20 修的是 `:start-revision`，`:end-revision` 用了同一种兜底写法却没被一起审计。

**修复（两侧各一半）**：
1. 客户端：`:end-revision` = **最后一个事件的 revision**（没有事件 ⇒ `nil`）；
2. checker：**覆盖区间的上下界一律从事件自推**（`upper` = 事件里最大的 revision），
   且**没有事件的会话不参与缺口判定** —— 不依赖客户端汇报的派生字段。
   并新增守门员 fixture `scripts/watch-fixtures/expect-valid-empty-session.edn`
   （空会话 + 一次正常会话，必须判 valid）。

**教训（补进 §5.5 判据补充 10）**：修完一个「派生字段用了循环变量」的缺陷后，
必须**审计同一函数里所有同源字段**；并且 checker 对「客户端汇报的派生量」应尽量
**自己去推**（能从原始事件算出来的，就不要读汇总字段）。

### F-22 [P2，测试自身] `coord-soak.sh` 的 `lein` 调用漏了 `-o`（违反 F-10 的离线约定）

**现象**：`make soak WORKLOAD=mixture ...` 起跑后 `soak/soak.log` 长时间（>2 分钟）
**0 字节**，进程活着但没有任何输出。

**根因**：`scripts/coord-soak.sh` 的 `LEIN_BASE=(LEIN_ROOT=true lein run test ...)`
**没有 `-o`**。F-10 早已确认「控制机/容器没有出网，plain `lein` 会先解析
SNAPSHOT 依赖、卡在网络上几分钟」，并把 `make` 目标与 `collect-evidence.sh` 的
`lein` 调用改成默认 `-o` ——**但漏了这个脚本**。实测：`pgrep -af lein` 显示的
命令行确实没有 `-o`。

**影响**：每次 soak 起跑白等几分钟（长跑本身的排期不受影响，但「起跑后日志空」
容易被误判成卡死；若网络半通还可能直接失败）。属 F-10 的**同一条修正没铺全**。

**修复**：加 `LEIN_OFFLINE="${LEIN_OFFLINE:--o}"` 并用它拼 `LEIN_BASE`
（`ONLINE=1`/`LEIN_OFFLINE=""` 可覆盖）。修复后起跑 20s 内日志即有正常输出。

**教训**：修一条「环境假设」类缺陷时，要**把同一类调用点全部找出**（`grep -rn
"lein " scripts/ lab/`），不能只改遇到的那一处。

---

## 12. 第五轮（2026-09-17）：T2.2 lease + T6.1 组合浸泡；3 条测试自身缺陷

> 本轮把 **T2.2（Lease 未测，缺口 A6）** 与 **T6.1（组合浸泡入口）** 接上，并把
> 「soak 产生价值」结项（见 [`soak-closure-report.md`](soak-closure-report.md)）。
> 下面 3 条全是**测试自身**缺陷，与 F-13/F-19 同类：报告看着正常，但代码根本没被
> 读进去 / 断言其实没生效。它们都由「改完先加载一次」这条纪律抓到。

| # | 一句话 | 分级 | 状态 |
|:--|:--|:--|:--|
| F-23 | `parse-mix` 的 docstring 里嵌了 ASCII `"` → 整个 ns 编译不过（报错伪装成 arity 错误） | P2（测试自身） | `closed`（改用「」） |
| F-24 | `mixck/checker` 把 `:surfaces` 解构成局部名，**遮蔽**本 ns 的默认值 ⇒ 默认三面全部失效（只有显式传参路径能跑） | P1（测试自身） | `closed`（改 `(:surfaces opts)` + 两条路径各有 fixture） |
| F-25 | 编辑 `cli-opts` 时丢了一个 `[` ⇒ 整个 `coord.clj` 读不进去，而报错位置在**文件最后一行**（相差 200 行） | P2（测试自身） | `closed`（补回 `[`；17 套 fixture 回归全绿） |
| F-26 | `leaseck` 在**活性锚点缺失**时 `(long nil)` 抛 NPE ⇒ jepsen 把整个 checker 判成 `:unknown`（**既不是红也不是绿，退出码 2**） | P1（测试自身） | `closed`（锚点缺失不判活性 + 计入 `:liveness-unjudged` + 守门员 fixture） |

### F-26 [P1，测试自身] checker 崩溃 → `:valid? :unknown`：比假绿更隐蔽的「没有结论」

**现象**（第一次在 docker lab 真跑 `--workload lease`）：退出码 **2**，报告里：

```
:linear {:valid? :unknown,
 :gates {:valid? true, ...},
 :perf  {:valid? true}}
```

栈顶是 `jepsen.coord.leaseck$liveness_fails.invokeStatic(leaseck.clj:148)`。

**根因**：活性判据的相对锚点（续期场景是「停续期那一刻」的 `:during-keepalive`
点读、撤销场景是 `:revoke-at-ms`）在真实 run 里可能**缺失**（那次点读失败 ⇒ 被
`ok-obs` 的 `:read-ok?` 过滤掉）。第一版直接 `(long rel-absent)` ⇒ NPE。

**为什么这条比假绿危险**：jepsen 对**抛异常的 checker** 不是报错退出，而是把它降成
`:valid? :unknown` —— 上层若只看 `:valid?` 的「非 false 即算过」，就会把这条 run
当成中性；而实际是**整个 lease 面根本没结论**。它与 F-12（摘要器挂掉 ⇒ 门槛结论
恒 `unknown`）同一类：「`unknown` 不得当作通过」（dev.md §5.5-4）这条纪律同样适用于
checker 自身。

**修复（两侧）**：
1. checker：锚点缺失时**不判**该 op 的活性（不能凭猜测判红），并把数量记入
   summary 的 `:liveness-unjudged`（不判 ≠ 通过、可核对）；
2. **保留**「`:absent-ms` 为 nil（从未消失）= 活性违反」这条分支 —— 它与锚点是否
   存在无关。重构时正是把它一起删掉了，`expect-invalid-revoke-not-cascaded` 立刻
   从红变绿（回归门禁当场抓到）；
3. 新增守门员 fixture `scripts/lease-fixtures/expect-valid-missing-anchor.edn`：
   `:during-keepalive` 观测为读失败 + 后续确认消失 ⇒ 必须 valid 且 `:liveness-unjudged 1`。

**教训**：① 任何 checker 的活性/安全判据都要问「基准量缺失时怎么办」，答案要么是
「不判且计数」，要么是「回退到保守基准」，不能是崩溃；② 「重勾」判据时，除了新加
的早退分支，还要确认**原有分支一条都没丢**（fixture 是最快的确认手段）。

### F-23 [P2，测试自身] docstring 里的 ASCII 引号让 ns 编译不过

**现象**：`clojure -M -e "(require 'jepsen.coord)"` 报

```
Syntax error macroexpanding clojure.core/defn- at (jepsen/coord.clj:515:1).
map=40 - failed: vector? at: [:fn-tail :arity-1 :params]
```

**根因**：`parse-mix` 的 docstring 写成 ``"解析 `--soak-mix "map=40,txn=20"` …"``
—— 中文文档字符串里嵌 ASCII `"`，字符串在 `--soak-mix ` 处提前结束，后面的
`map=40,...` 被当成代码。报错信息**完全指向参数列表**，看不出是 docstring 问题。

**修复**：docstring 改用「」。**教训**：新增的中文 docstring 落笔后立刻编译一次；
这类错误不会表现为「字符串未闭合」，而是伪装成 arity/参数列表错误。

### F-24 [P1，测试自身] `:keys [.. surfaces ..]` 遮蔽本 ns 的默认值

**现象**：泛化组合 checker 后，`mixture` 的两套 fixture **没有任何输出**（抛
`mixture checker: no surfaces`），而显式传 `:surfaces` 的 `soakfull` 那套是绿的
—— **只有默认路径坏了**。

**根因**：`([{:keys [... surfaces ...] :as opts}] … (or surfaces surfaces))`：两个
`surfaces` 都是局部绑定，本 ns 的 `(def surfaces [:map :txn :scan])` 被遮蔽 ⇒ 恒
`nil`。它**不是假绿**（构造期抛异常），但会以「只有调用方显式传参的路径能跑」的
形式存在；一旦有人照抄 soakfull 的写法把 `:surfaces` 传上，错误就被掩盖。

**修复**：不解构该键，改用 `(:surfaces opts)`。
**教训**：同一 ns 里「默认值 def」与「同名 `:keys` 解构」并存时必须查遮蔽；
**默认值路径与显式传参路径都要有 fixture**（本轮：`mixture-fixtures*` 钉默认三面、
`soakfull-fixtures` 钉显式五面）。

### F-25 [P2，测试自身] 少一个 `[` → 文件读不进去，报错却在最后一行

**现象**：

```
Syntax error reading source at (jepsen/coord.clj:1071:76). Unmatched delimiter: ]
```

1071 是**文件最后一行**（`cli-opts` 的收尾），真正的错处在第 853 行。

**根因**：`(def cli-opts` 下第一项原本是 `  [[nil "--workload WORKLOAD" …`
（外层向量 + 首项向量，**两个** `[`）；一次编辑改成 `   [nil …`（少一个 `[`）。
于是首项与外层同级，后续选项全部平级，末尾多出一个 `]`。

**为什么自查没抓到**：`clojure.core/read-string` **会吞掉多余的 closer**
（`(read-string "[1]]")` → `[1]`），所以「逐个 chunk 用 read-string 检查」抓不到；
抓到它的是**真正加载一次**（离线 harness 的 `require`）。

**修复**：补回 `[`，重跑 17 套 fixture 全绿。
**教训**：① 改完 `.clj` 必须先加载一次（本仓 F-13/F-19/F-23/F-24/F-25 都是这一步
抓到的）；② 「Unmatched delimiter 指向文件末尾」时不要从末尾找：用 `read`
（**不是** `read-string`）逐 form 读，定位**第一个**失败的 form；③ 长 options 向量
编辑后，比对首项括号形状（`[[nil` vs `[nil`）是最快的自查。

---

## 13. 复跑（2026-09-17，docker lab：`make checkers` / `matrix-m1` / `matrix-m2`）

> 结项报告 §0 此前把三个门禁的「全绿」写成 **lab 结论、本轮未重跑**。本轮按
> `soak-closure-report.md` §6 的命令在 docker lab 完整复跑（coord `ddafb9d`；复跑
> 前后 `target/release/coord` 的 sha256 一致（`55ba6a54…`）⇒ 无 code-change 混杂）。
> 结论需要修正：**`matrix-m2` 不是全绿**，红档暴露了本轮唯一一条新的 coord 侧缺陷。

| # | 一句话 | 分级 | 状态 |
|:--|:--|:--|:--|
| F-27 | Lease **过期时的 revoke 提案可被静默丢弃**（`check_expired()` 已把过期 Lease 移出本地管理器，随后的 `raft.client_write` 失败只 `warn!`、**不重试不回插**）⇒ 绑定 Key 在 `ttl+grace` 内 0 消失；领导权不再变化即**永久泄漏** | P1（coord 侧） | `confirmed-by-run` → **机制已闭环 2026-09-21**（见下「修复与复跑」）；`matrix-m2` 该档**仍红**，剩余 4 条归因为**活性窗口**问题（非泄漏） |

复跑结果：

| 门禁 | 结果 | 备注 |
|:--|:--|:--|
| `make checkers` | **17 套 / 99 个全绿**（99 PASS / 0 FAIL） | 与报告 §4 的数字一致，首次被真实重跑钉住 |
| `make matrix-m1` | **9/9**（`ALL M1 MATRIX PASSED`） | map/txn/scan/mixture × none\|kill + idempotency:none |
| `make matrix-m2` | **7/8** | watch 四档 + `lease:none/kill/pause` 绿；**`lease:partition-halves` 红（F-27）** |

台账纠错：`store/coord` 全量只有 **6 个 lease run，全部发生在 2026-09-17**
（14:42 / 14:44 / 16:15 / 16:16 / 16:18 / 16:20），即 `lease × pause` 与
`lease × partition-halves` 是**本轮第一次**被跑 —— 此前「matrix-m2 8 组合全绿」
没有任何 run 支撑（见 `soak-closure-report.md` §0/§4 的修正）。

### F-27 [P1] Lease 过期 revoke 丢失 → 绑定 Key 不被级联删除

#### 修复与复跑（2026-09-21）

**修法**（worktree **DIRTY**，未提交）：`LeaseManager` 的过期记录**保留**至 revoke
**确认提交**（`finish_expired`）；`check_expired()` 对已标记录**继续上报**（= 幂等重试），
指标只在跃迁时结算一次；`keep_alive`/`attach_key`/`detach_key`/`get_lease`/计数把
「待提交」记录视为**不存在**（fail-closed，否则会用「假活」换掉「丢 revoke」）。
worker 用 `pending` 集合 + `advance_pending_revokes` 逐轮重试，提交成功才删除本地记录。

**复跑**：`make -C jepsen/lab test WORKLOAD=lease NEMESIS=partition-halves TIME_LIMIT=45
CONCURRENCY=1n SKIP_CHECKERS=1 JEPSEN_PROVIDER=docker`（binary sha256 前 8 位
`7c4b3222`），store = `store/coord/2026-09-21T14:53:38.329176044Z/`。

| 项 | 值 |
|:--|:--|
| checker | `:grants 94 / :expiries 40 / :liveness-unjudged 0 / :violations-by-class {:lease-not-expired 4}` |
| 判决 | **仍红**（`Analysis invalid`，make 退出码 2）—— 但**失效形态已变**（下两点） |

**① 静默丢失通道已闭环（本轮新增的可观测证据）**：修后 worker 对「提交未确认」的条目
保留待办并重试，日志形态：

```
n1/coord.log:210 14:53:54.782137Z WARN lease expiry revoke not committed; entries retained
  for retry (F-27) failed=1 pending=1 first_lease_id=10
  error=client_write failed: has to forward request to: Some(2), Some(BasicNode{addr:"172.19.0.4:50052"})
n2/coord.log:238 14:54:01.867070Z WARN ... failed=2 pending=2 first_lease_id=29 error=... None, None
n2/coord.log:240 14:54:06.867091Z WARN ... failed=5 pending=5 first_lease_id=29 error=... None, None
```

n1 在失败后**持续输出日志到 14:55:04**（run 14:55:07 结束）而**再无重复告警**
（告警 5s 节流；若仍失败必然每 5s 一条）⇒ 该条 revoke 在后续 tick 提交成功。
旧实现在同一时刻只会打一条 `Lease 10 expiry: failed to revoke via raft: …`，
且记录已被移出本地管理器 ⇒ 该 revoke **永久丢失**。

**② 剩余 4 条 `:lease-not-expired` 是「活性窗口」问题，不是泄漏**：nemesis 为
`partition-halves`，周期 14:53:51→54（3s）、**14:54:01.33→14:54:06.57（5.24s）**、
14:54:12.7→14:54:20.3（7.6s）…；而活性判据的窗口是 `ttl + grace = 2s + 4s = 6s`
且**锚在 op 起点**（`src/jepsen/coord/leaseck.clj:158-164`）。分区期间无 quorum ⇒
`LeaseOp::Revoke` **不可能提交**（共识语义，不是实现缺陷）⇒ 分区内到期的 Lease
必然在窗口内不消失。4 条的 `:absent-ms` 全为 `nil`。
⇒ 需要**口径裁定**（`dev.md` §5.4-⑤ `lease grace` 仍是「待确认」参数）：要么把活性
窗口定义为「quorum 恢复后 + grace」，要么把 grace 提到 > 最大分区时长。
**裁定前**：不得把该档当作产品缺陷引用，也**不得改判据迁就实现**（§7 规则 4）。

**③ 未覆盖的旧疑问**：2026-09-17 的 8 条里另有 4 条**无 WARN**，当时记为「需单独
分诊」。本轮 4 条与那 4 条是否同族**尚未证明**（两次 run 的 lease id 集合不同）⇒
待办：对同一 run 做「violating lease 是否**最终**被删除」的终态复核（建议在 `leaseck`
增加一条独立的「最终态必须消失」判据，与 §5.2 的 `ttl+grace` 时效判据**并存**：
前者判**泄漏**、后者判**时效**）。

**契约锚点**：`apis/contracts/proto/coord/lease/lease.proto` ——「Lease 过期或被
Revoke 时，所有绑定该 Lease 的 Key 被删除（级联删除）」；§5.2 的
「停止续租的 Key 在 `ttl+grace` 内消失率 **100%**」。

**判决 run**：`jepsen/store/coord/2026-09-17T16:20:01.926257732Z/`
（`make matrix-m2` 最后一档 `lease:partition-halves`，45s，concurrency 1n；
make 退出码 2 = jepsen 判 invalid）。

**现象**：`:grants 81 / :expiries 36`（§5.1 门槛 5/2 通过），
`{:violations-by-class {:lease-not-expired 8}}`，8 条全部 `:absent-ms nil`：

| 场景 | key（`/jepsen/lease/s591962667/…`） | lease |
|:--|:--|:--|
| `:ttl` | `ttl/33` · `ttl/27` · `ttl/38` | 25 · 27 · 28 |
| `:keepalive` | `ka/32` · `ka/35` · `ka/108` · `ka/112` · `ka/113` | 26 · 29 · 61 · 62 · 63 |

`:ttl` 档的判据是「op 起点 + `ttl+grace`（2s+4s）内必须观察到消失」；`:keepalive`
档是「停续期时刻 + `ttl+grace`」。8 条都是**整整 6s 窗口内一次都没观察到消失**
（`lease-wait-gone` 返回 `:absent? false`），不是「消失得略晚」。

**为什么这不是假红（观测可信）**：

1. 每个 op 在窗口内轮询 31–34 次，其中 18–29 次读失败（partition 期的 transient
   不可用）；但**成功**的读一律 `:present? true` —— 没有任何一次成功的「读不到」。
2. 读路径是 **leader-only 线性读**：`range` 处理器先调
   `ensure_linearizable_on`（`coord-server/src/server/mod.rs:1590`，ReadIndex +
   Leader 身份复核 + `applied ≤ committed` 复核），非 leader 直接
   `UNAVAILABLE read_refused=not_leader` ⇒ **排除了陈旧读**。
3. `point-read` 用 `range_req(key)`（`range_end` 为空）→
   `RangeSemantics::is_single_key()`（`server/mod.rs:1599`）= **单键精确查** ⇒
   排除「删掉目标键后读到同前缀邻键」这类假 `:present?`。

⇒ 「Key 仍在」是 leader 提交态的真实观测：**级联删除没有发生**。

**根因（代码）**：`start_lease_expiry_worker`（每 200ms tick，
`coord-server/src/server/mod.rs:737-790`）：

```rust
if !node.is_raft_leader().await { continue; }        // ① 先查身份
let actions = lm.check_expired();                    // ② 本地状态已被消耗
for action in actions {
    if let Err(e) = raft.client_write(cmd).await {   // ③ 再 propose
        tracing::warn!("Lease {} expiry: failed to revoke via raft: {}", lease_id, e);
    }                                                  // ④ 只告警，不重试
}
```

- ① 与 ③ 之间有 **TOCTOU**：partition 期间领导权恰好在这两步之间丢失，
  `client_write` 返回 `ForwardToLeader` 错误。
- ② `LeaseManager::check_expired()`（`coord-server/src/lease/mod.rs:115-141`）在返回
  action 之前就用 `leases.retain(…, false)` **把过期 Lease 从本地管理器移除了**
  —— 所以 ④ 之后 `check_expired()` 永远不会再返回这个 Lease。
- 结果：这次过期 revoke **永久丢失**。唯一的恢复路径是下一次领导权变更触发
  `rebuild()`（records 仍在状态机里，因为 revoke 没提交；deadline 已过 ⇒ 立刻
  再触发一次）。**领导权此后不再变化 = 绑定 Key 永久泄漏**（违反级联删除契约）。

**日志证据**（同一 run，`:warn` 级，与 violation 一一对应）：

```
n1/coord.log:408  16:20:28.393Z WARN Lease 26 expiry: failed to revoke via raft:
           has to forward request to: Some(2), Some(BasicNode { addr: "172.19.0.4:50052" })
n2/coord.log:395  16:20:42.910Z WARN Lease 61 expiry: failed to revoke via raft: has to forward request to: None, None
n2/coord.log:396  16:20:43.110Z WARN Lease 62 expiry: failed to revoke via raft: has to forward request to: None, None
n2/coord.log:397  16:20:44.111Z WARN Lease 63 expiry: failed to revoke via raft: has to forward request to: None, None
```

`None, None` = 此刻连 leader 都不知道（partition 刚发生）—— 正是 ①③ 之间丢失
领导权的形态。

**待分诊（8 条里仍有 4 条未解释）**：`lease 25 / 27 / 28 / 29`（= `ttl/33`、
`ttl/27`、`ttl/38`、`ka/35`）**没有**对应的 `failed to revoke` 告警。两种可能：
(a) revoke 提交成功但级联删除没删掉绑定 Key（第二条独立缺陷）；(b) 过期根本没
触发（例如 rebuild 后 deadline 被重算推迟）。**在分诊清楚之前不要把 F-27 当作
已完整解释 8 条 violation 的结论。**

**复现**：

```bash
make -C jepsen/lab test WORKLOAD=lease NEMESIS=partition-halves TIME_LIMIT=45 \
     CONCURRENCY=1n SKIP_CHECKERS=1 JEPSEN_PROVIDER=docker
# 随后看 store/coord/<新目录>/n*/coord.log 里的 "expiry: failed to revoke via raft"
```

**建议修法方向（待 coord 侧确认）**：

1. 过期动作要**幂等且可重试**：`client_write` 失败时把 `(lease_id, deadline)` 放回
   待处理集合，按退避重试，直到 revoke 提交成功；
2. 或者不在 `check_expired()` 里移除记录：由「revoke 已提交」这一事实驱动移除
   （apply 侧确认），使本地状态与状态机一致；
3. TOCTOU 的通用修法：不预先查 `is_raft_leader()`，直接 propose 并**按错误类型**
   处理（`ForwardToLeader` → 转投 / 重试，而不是丢弃）。

**同类风险点（同一 PR 里一起看）**：`start_region_lease_revoker`
（`server/mod.rs:795+`）的注释写着「若 Region leader 是其他节点，则该条目由对端
处理……本端**丢弃**以限制内存」—— 一旦对端没收到 region 0 的广播，这条待清理
条目同样会静默丢失。region 模式（Multi-Raft）目前未被矩阵覆盖。

## 14. 第六轮（2026-09-18）：M5a 落地 —— coord-agent 首次被 Jepsen 真跑

> 本轮把 `jepsen/docs/coord-agent-coverage-plan.md` 的建议**落成代码**并在 docker lab
> 真跑：agent 部署（多实例 + SSH 隧道 + 路由证明）、wire 层（13 个 `coord.agent.*`
> 方法）、4 个 agent 本地面 workload 与 checker、agent 侧 nemesis、门禁矩阵。
> 首次实跑即产出**两条 coord 侧实质发现**（F-28/F-32），另有 4 条**测试自身缺陷**
> （F-29/F-30/F-31/F-34）与 1 条**契约字段缺陷**（F-33）。
>
> **同日第二轮（F-34 闭环）**：给 lock 面加了**服务端地面真值探针**（绕开 agent 直接
> 读 `/_lock/{name}`）并修掉三层区间**度量**缺陷，互斥重叠 99 → **0**、探针硬违约
> **0**，而 F-28 依旧 104/104 命中（真红没被修掉）。

| # | 一句话 | 分级 | 状态 | 判决任务 |
|:--|:--|:--|:--|:--|
| F-28 | `LockService::release` **不校验 `lease_id`** → 任意调用方用错误 lease 即可删除他人的锁（fencing 缺失） | **P0** | **`confirmed-by-run`**（首轮 162/162，第二轮 104/104） | M5a / `--workload lock` |
| F-29 | 测试自身：lock 的持有区间用**相对毫秒跨 op 比较** → 2817 条假重叠 | P1（测试自身） | `closed`（改为以绝对时刻为锚 + 守门员 fixture） | M5a |
| F-30 | 测试自身：区间闭合依据 Release 的布尔（会撒谎）→ 一次真实 fencing 缺陷被放大成数千条假红 | P1（测试自身） | `closed`（改看可观测锁状态 + fixture） | M5a |
| F-31 | 测试自身：agent 就绪探测用 `ss -ltn`（control 镜像里**没有 ss**）→ 「隧道明明通了却判失败」 | P2（测试自身） | `closed`（改为**功能验证**：经隧道 GET /metrics） | M5a |
| F-32 | agent 侧**没有** server 那种 `root` 全能力旁路 → auth 开启时 **root CCT 经 agent 调用任何 RPC 都被拒**（via-agent 全路径不可用） | **P0** | **`confirmed-by-run`**（实测卡住首轮） | M5a / T5.2 |
| F-33 | `LockRenewResponse.new_ttl` 在 handler 里**硬编码为 0**（`grpc_handlers.rs:146`）→ 字段形同虚设；按 lease 的「ttl=0 = 已不存在」口径解读会误判 | P2 | **`confirmed-by-source`** | M5a |
| F-34 | lock 的「互斥重叠 99/162」：**测试自身**的区间度量缺陷（锚点噪声 + `exists=false` 闭合判据 + 闭合时刻记在观测循环之后）三层叠加 | **测试自身（P1）** | **`closed-by-run`**（99 → 0，服务端探针 0 违约；见下文） | M5a |

---

### F-28 [P0] `LockService::release` 不校验 `lease_id` → 非持有者可删锁（fencing 缺失）

**契约锚点**：`apis/contracts/proto/coord/lock/v1/lock.proto`：「仅 (holder_id, lease_id)
匹配者可 Release / Renew」；台账整改要点亦把 fencing 列为待验证项。

**实测证据**（`docker lab`，`--workload lock --agents 2`，2026-09-18）：

```clojure
;; history.edn 里一条 lock-contend 完成 op（节选）
{:f :lock-contend :holder-id "h-666792" :lease-id 1
 :acquired-at-ms 67 :released-at-ms 433
 ;; 探针：故意用 **错误的 lease_id**（真值 +1）调 Release
 :foreign-release {:ok? true, :released? true}      ;; ← 竟然成功了
 :released? false                                    ;; 紧随其后的「正确」Release 反被拒
 :lock-info-after-release {:exists false}}           ;; 锁已被探针删掉
```

`lockck` 汇总：`:acquires 162`、`:violations-by-class {:lock-fencing-missing 162}` —— **162/162
全部命中**。

**源码定位**（`coord-agent/src/services/lock.rs:412+`）：`release` 只按
`(name, holder_id)` 命中本地缓存，**不比较请求里的 `lease_id`**，命中的落点也不看它。

**影响**：任何知道锁名的调用方（或任何一个持过期 lease 的旧持有者）都能把当前持有者
的锁删掉 —— 互斥承诺被第三方破坏。生产上是「进程重启后带着旧 lease 回来把新持有者
踢掉」这一类最难查的形态。

**建议修法方向**：`release`/`renew` 在命中本地缓存后**必须**比对 `lease_id`
（不匹配 → `PERMISSION_DENIED`），并把该断言写进 `lockck` 的负控制 fixture（已有：
`expect-invalid-fencing.edn`）。

---

### F-32 [P0] agent 侧没有 root 全能力旁路 → 经 agent 的 root 调用全部被拒

**证据**（首轮 via-agent 卡住时的真实报错）：

```
UNAUTHENTICATED: role(s) ["root"] do not have capability 'data:kv:read'
```

同一进程内、同一 root 凭据：**直连 server 成功，经 agent 必然被拒**。

**源码对照**：

* server：`coord-server/src/auth/manager.rs:563` 起 `check_capability` 对
  `role_name == ROOT_ROLE` **直接放行**（"root 角色为引导管理员，全能力放行"）；
* agent：`coord-agent/src/auth/interceptor.rs` 只按 **RoleCache 里的显式能力集**判定
  （`scopes_for_capability(roles, capability_id)`），**没有**任何 root 旁路。

于是 root 的 CCT 在 agent 侧是一张「没有任何 capability」的令牌 —— 而 root 角色记录
里本来就没有逐项列出能力（全靠 server 的旁路兜着）。

**影响面比看起来大**：Java SDK 客户端、运维脚本、以及任何用 root/管理员凭据接入的
集成，走的都是「本机 agent」这条路（daemonset 形态）；也就是说 **auth 开启时 agent
作为应用入口的整条路径不可用**，而这一点在任何进程内测试里都看不到（进程内测试直接
调 server 或直接构造 CCT）。

**lab 的处置**（已落地，`jepsen/src/jepsen/coord/agent.clj` 的
`grant-client-capabilities!`）：把 21 个需要的 capability **显式**授给 root（幂等）。
这**不**改变任何 server 行为（server 本来放行 root），只是让同一张 CCT 在 agent 侧
也通过 —— 也就是说**没有被测系统为测试让步**。

**三种修法（由 coord 团队选）**：
1. agent 侧补 root 旁路（语义对齐 server，但要把「谁能签发 root CCT」这件事想清楚）；
2. server 侧把 root 的「全能力」**物化**进角色记录，使 RoleCache 同步即完整
   （一劳永逸，且对其它消费 RoleCache 的组件同样有效）；
3. 明确要求部署时逐项授权（那就要有默认能力集与文档，且不能让「默认不可用」成为
   运行期才发现的事实）。

---

### F-34 [测试自身，已闭环] 「互斥重叠 99/162」= 区间**度量**缺陷（三层叠加）

> **结论（2026-09-18 第二轮，已用服务端地面真值探针闭环）**：这不是 coord-agent 的
> 互斥缺陷，而是**本仓 lock 判据自身的三层区间度量缺陷**叠加出来的假红。修完后同一
> workload 的互斥重叠 = **0**，服务端探针硬违约 = **0**，而 F-28（fencing）依旧 104/104
> 命中 —— 也就是说「真红没被修掉、假红被消掉了」两件事同时成立。
>
> 分诊过程与判据纪律见 `dev.md` §5.5 第 13–15 条；工具是
> `scripts/lock-diag.clj`（同一份历史跑四种区间口径）。

**原始观测**（M5a 首轮，`2026-09-18T15:41:38Z`）：`:acquires 162`，
`:violations-by-class {:lock-mutual-exclusion 99}`，重叠样本最长 ~388ms，两侧都
`closed?=true`。

**三层根因**（每一层都单独量化过；`:f34 → :legacy → :new` 是逐层修掉一层后的计数）：

| 层 | 根因 | 证据（同一条历史的四种口径） |
|:--|:--|:--|
| ① 锚点 | 区间以 jepsen 记录的 invoke 时刻为锚 —— 那是 worker **派发** op 时的点，中间隔着队列与线程调度。同一个 JVM 里实测 `completion.time - (t0 + 最后一个 :at-ms)` 在 40ms ↔ 290ms 之间抖 | 改用 op 自己读的 `System/nanoTime`（`:t0-ns`）后，`:legacy` 从 5 → 3 |
| ② 闭合判据 | 「锁空出来了」判成 GetLockInfo 的 `exists=false`。观测窗口里**另一个持有者合法接管**时 `exists=true`，区间于是被 fail-safe 延长整整 `ttl+grace`（=9s） | 老历史里 99 条重叠中 85 条涉及这种「9s 延长」的区间 |
| ③ 闭合时刻 | `:released-at-ms` 记在**观测循环之后**（循环里最多 5×200ms 的 sleep）⇒ 区间尾部被拉长（实测中位数 +196ms、最长 +1368ms） | 区间膨胀分位：老历史 `{p50 196, p90 517, max 1368}` → 修后 `{p50 117, p90 164, max 213}`（剩下的 ~100ms 是 hold 结束后 renew 循环那次 sleep，属于真实持有） |

**闭环证据**（两条独立的证据链，缺一不可）：

1. **客户端侧**：`scripts/lock-diag.clj` 在修后的历史
   （`2026-09-18T16:18:33Z`）上给出
   `:f34 43 → :legacy 3 → :new 0`，而独立估计 `:true-end`（以「最后一次 renew +
   一次 sleep」为持有结束，**不使用任何客户端自述字段**）也是 **0**。
   在老历史（没有 `:gone-at-ms` 字段）上 `:f34 = :new = 99`、`:true-end = 0` ——
   也就是「同一批自述区间，只要换掉度量口径，重叠就消失」。
2. **服务端侧**：新增 `:f lock-probe` op —— 绕开 agent 直接从 server 用 `KV Range`
   读 `/_lock/{name}`，把「服务端 key 挂在谁名下」按时间采样（本轮 28 样本、
   0 读失败、6 个不同 holder）。判据 5 要求「服务端说 H 持有，而某条**其它** holder
   的自述区间把该时刻含在内部」⇒ 硬违约。实测 **0 硬违约 / 0 边界样本**。
   这条判据是 F-34 原始设计里的「决定性实验」，现在**常驻**为 checker 判据（探针
   缺失时判未执行，见 `expect-invalid-probe-missing.edn`）。

**顺带确认的（不是新缺陷，但值得记）**：

* 老历史里 6 条 `gone?=false` 的 op，其 `lock-info-after-release` 显示
  `exists=true` 但 **holder 已经是别人** —— 这正是根因②的现场：锁被别人合法接管，
  而旧判据（`exists=false`）把它读成「没有释放」。修后这 6 条全部正常闭合。
* `:lock-release-did-not-free` 这一条判据在 F-28 存在时**永远不会触发**
  （因为 fencing 探针先把锁删了，真 Release 必然回 false；`released?` 恒为 false）。

**为什么第一轮没能分诊**：当时只有「99 次重叠、样本可核对」这个观测，而同一批样本
在三种解释（真互斥破坏 / agent 汇报层不一致 / 区间度量错误）下长得一模一样 —— 把
未分诊的观测写成结论会误导修复方向。本轮加了探针 + 四种口径对照之后，三种解释才被
分开；这也说明「区间类判据必须自带口径对照与独立估计」是必要的工程纪律。

---

### F-29 / F-30 / F-31 [测试自身] 三条「报告很红、其实是测试的错」

见 §12 的同类纪律。三条都已修，且都补了**守门员 fixture**（去掉修复必然变红）：

* **F-29**：`lockck` 的区间锚。相对毫秒是「相对**本 op** 起点」的，跨 op 比较等于把
  每个 op 的零点当成同一个 —— 修法是把 invoke 的绝对时刻加上去
  （`lockck.clj` 的 `abs-ms`）。守门员：
  `lock-fixtures/expect-valid-relative-times-must-not-overlap.edn`。
* **F-30**：区间**闭合**的依据必须是可观测状态（GetLockInfo 的 `exists`），不是
  Release 的布尔返回值 —— 后者在 F-28 那条缺陷下会**撒谎**（探针删掉锁 ⇒ 正确
  Release 回 false ⇒ 区间被 fail-safe 延长 ⇒ 与后面所有人重叠）。守门员：
  `lock-fixtures/expect-invalid-release-said-false-but-gone.edn`（它在固定「假重叠
  消失」的同时，**仍然**命中 `:lock-fencing-missing`，防止修假红时把真红一起修掉）。
* **F-31**：agent 就绪探测原用 `ss -ltn` 判监听 —— jepsen-control 镜像里**没有 ss**，
  于是「端口明明在监听」被判成失败（假红）。改为**功能验证**：经隧道
  `GET /metrics` 拿到 `coord_agent_*` 才算通（`agent.clj` 的 `wait-tunnel-up!`）。

---

## 15. 第六轮 · 第二轮（2026-09-18）：把 coord-agent 做实 —— 四个面首次全在 lab 跑

> 本轮的目标是把 §14 留下的三件「未闭环」做完（探针 + 四个面 + 差分），但**把矩阵
> 真的跑起来**这件事本身又交出了三条**测试自身**缺陷（F-35/F-36/F-37）——每一条都
> 会让某个 cell 以上下文完全不同的方式变红。三条都已修并补了守门员 fixture。
>
> 一句话结论：**coord-agent 的四个本地面（lock / election / idgen / registry）现在
> 都能在 lab 里被判决**；其中 lock 的红**只剩 F-28（fencing 真缺陷）**，election /
> idgen / registry 的判据本身不再产出假红。

| # | 一句话 | 分级 | 状态 | 判决任务 |
|:--|:--|:--|:--|:--|
| F-35 | 测试自身：`electck` 的 leader 区间**完全没有锚点**（直接跨 op 比相对毫秒）→ 45s run 报 124 条假 `:election-two-leaders`；同一份历史加上锚点是 **0** | P1（测试自身） | `closed`（`abs-ms` + `:t0-ns` + 闭合证据 + 2 个守门员 fixture） | M5a / `--workload election` |
| F-36 | 测试自身：四个 agent nemesis 的 `invoke!` 返回**向量**（`[:killed-agent "n4"]`）而不是 op map → 每次扰动抛一条 `:jepsen.nemesis/invalid-completion`（一轮 9 cell 共 86 条） | P1（测试自身） | `closed`（`completion` 包一层，事件信息进 `:value`） | M5a / 所有 agent nemesis |
| F-37 | 测试自身：本地面 workload 的**路由证明**要求「check 时每个 agent 都抓得到」→ kill 类 nemesis 下必然假红（idgen/kill-agent 实测）；数据面还有第二个同形问题：agent 重启后代理计数归零 | P1（测试自身） | `closed`（run 期间多次采样：`:up?` = 曾经抓到过、`:total` = 各次最大值） | M5a / `:agent-route-not-proven` |

---

### F-35 [测试自身] election 的「双 leader」：区间锚点缺失（F-29 的同型复发）

**实测**（`--workload election --agents 2`，45s，`2026-09-18T16:26:49Z`）：

```
election checker: {:campaigns 110, :won 49, :not-won 61, :resign-failures 14,
                   :unclosed-intervals 14,
                   :violations-by-class {:election-two-leaders 124}}
```

重叠样本（第一版打印的就是**相对毫秒**）：

```
left  {:group "group-b" :candidate "c-924698" :start 51  :end 340}
right {:group "group-b" :candidate "c-975223" :start 289 :end 613}
```

**分诊**：`campaign-at-ms` / `resigned-at-ms` 都是「相对**本 op** 起点」的量，而
`leader-interval` 直接把它们当绝对量跨 op 比较 —— 每个 op 的零点不同，这个比较没有
意义。同一份历史（16:28:34 那轮的 9 个 won op）用两种口径重算：

```
:rel     双 leader 重叠: 8      ← 第一版（相对值直接比）
:invoke  双 leader 重叠: 0      ← 只把锚点补上（jepsen invoke 时刻）
```

⇒ 124 条全部是**度量**产物。**修法**（与 lock 面完全同口径，`dev.md` §5.5 第 13/16
条）：

* `client.clj` 的 elect op 记 `:t0-ns`（op 自读的 `System/nanoTime`）；
* `electck.clj` 加 `abs-ms`，锚点优先 `:t0-ns`；
* 闭合用**证据**而不是「Resign 的布尔」：`gone-at-ms` = Resign 成功 ⇒ resign 返回
  时刻；否则 GetLeader 明确不再是自己 ⇒ 探针时刻。没有任何证据 ⇒ 区间不闭合、
  不参与双 leader 判定（TTL 被动过期不是违约，用 fail-safe 延长的区间判会假红）。

**守门员 fixture**：`expect-valid-anchor-must-be-absolute.edn`（去掉锚点必红）与
`expect-valid-nanotime-anchor-wins-over-jepsen-time.edn`（锚点退回 jepsen 时刻必红）。

**残留（明示）**：election 目前**没有**服务端地面真值探针（lock 有 `:lock-probe`）。
`/_election/{group}` 是同类 key（`leader_election.rs:88`），所以探针可复用同一套
机制；在那之前，判据 1 只有客户端自述这一半证据 —— 按 `dev.md` §5.5 第 15 条，
这一条要显式声明为待补，不得当成「已交叉验证」。

---

### F-36 [测试自身] agent nemesis 的返回值不是 completion op

**实测**（同一轮矩阵日志，9 个 cell 共 **86** 条）：

```
INFO ... jepsen worker nemesis - jepsen.coord.agent coord-agent: a 1 on n4 is ready
 :op' [:restarted-agent "n4"],
clojure.lang.ExceptionInfo: throw+: {:type :jepsen.nemesis/invalid-completion,
 :op {:index 82, :time 22251134287, :type :info, :process :nemesis, :f :stop, :value nil},
 :problems ["should be a map" ":type should be :info" ":process should be the same"
            ":f should be the same"]}
```

**源码定位**：`kill-agent` / `kill-agent-all` / `pause-agent` /
`partition-agent-server` / `compose-agent-all` 的 `invoke!` 直接把
`agent/kill-one!` 之类的返回值（`[:killed-agent "n4"]` 这样的**向量**）作为
completion 交给 jepsen。jepsen 的 nemesis worker 对返回值有硬契约：必须是 op map，
且 `:type/:process/:f` 与 invoke 对得上。

**影响**：每个扰动都抛一条异常（被 worker 吞掉并记日志），扰动状态机拿不到动作
结果；更麻烦的是它把**真实失败**淹在噪声里（86 条 ExceptionInfo 里找真正的信号）。
`compose-agent-all` 还有第二处：它把内层 `:f` 改写成 `:start`/`:stop` 后直接返回，
`:f` 与外部 op 不一致 —— 同样违约。

**修法**：加 `completion` 助手（`(assoc op :type :info :value v)`），四个 nemesis
与 compose 全走它；事件语义放 `:value`，不丢信息。

---

### F-37 [测试自身] 「路由证明」把 nemesis 生效读成了「请求没经过 agent」

**实测**（`--workload idgen --nemesis kill-agent`，`2026-09-18T16:32:05Z`）：
idgen 判据本身全绿（`:ids 35 ≥ 10`、`:violations-by-class {}`），但整个 cell 判
**invalid**：

```
:agent {:proof {:total 0
                :agents [{:host "n4" :up? false :total nil}
                         {:host "n5" :up? true  :total 0}]}
        :local-surface? true}
:failures [{:type :agent-route-not-proven ...}]
```

**根因两条（同一条判据的两个口径问题）**：

1. **本地面分支要求「check 时每个 agent 都能抓到」**。而 `:kill-agent` 的意义就是
   让 agent 消失（杀完在 `:stop` 才重启）—— 被杀的 agent 在 check 时还没起回来，
   于是「路由证不出来」。而 `idgen` / `lock` / `election` / `registry` 的调用**只
   可能**落在 agent 上（server 没有 `coord.agent.*`），所以「一个 agent 曾经存在过」
   就已经足够当证据。
2. **代理计数是进程内的**：agent 重启 ⇒ 计数从 0 重新开始。数据面 workload
   （`--via-agent`）在 kill 之后仍然真的走了请求，但那一波被算在新进程头上 ⇒
   run 结束时的单次抓取会看到 0。

**修法**（`agent.clj` + `gates.clj` + `coord.clj`）：

* run 期间**多次采样**（`with-agent` 的 `setup!` 与 `teardown!` 各一次），
  累积进 `scrape-log`；
* `:up?` = **曾经**抓到过（判据用它），`:up-now?` = check 时能不能抓到（只作报告）；
* `:total` = 各次采样的**最大值**（防「重启归零」抹掉已经发生过的代理流量）；
* 本地面门槛从「每个 agent 都在」改成「**至少一个** agent 曾经在」。

**守门员 fixture**：`gates-agent-local-fixtures{,-ok}/`（同一条历史、只换证明值：
`{:up? true, :up-now? false}` 必须绿；两个都不曾 up 必须红）。

**这一条为什么要单独记**：它是「判据把**被测系统之外的时序**当成被测系统的失败」
的典型 —— 与 F-13/F-19 同类（假绿的反面）。矩阵每次都跑 kill 类 nemesis，所以这
类假红一旦漏过，M5b/M6 的长跑报告会长期带一条噪声红。

---

### F-38 [测试自身] agent 就绪探测只等 HTTP ⇒ 整个矩阵撞在「看起来就绪、实际连不上」的窗口里

**实测**（第二轮矩阵的**第二次**跑，9 个 cell 全部失败，且都是同一个形态）：

```
{:agent 2, :node "n5", :port 24577}
clojure.lang.ExceptionInfo: coord-agent: tunnel a2 to n5 did not come up
 (pid=1334474 alive=true); log tail:
   channel 1: open failed: connect failed: Connection refused   ×5
```

**根因**（agent 节点上的日志给出了确切时序）：

```
17:04:00.298  INFO Agent health/metrics HTTP server listening on http://127.0.0.1:19528
17:04:09.301  INFO coord-agent connected to server cluster (attempt 1)
17:04:09.30x  INFO service 'lock'/'idgen'/... registered ...   ← gRPC 到这时才起
```

agent 的启动顺序是「**HTTP 先起** → 连 server 集群 → **连上之后才起 gRPC
listener**」，实测两者相差 **~9s**。而我的就绪探测只看 HTTP `/metrics`（`wait-for-agent!`），
隧道与客户端要的却都是 **gRPC 端口** ⇒ 中间有一段「看起来就绪、实际连不上」的窗口。
隧道验证有 12s 预算，第一次矩阵跑时勉强过得去；集群变慢之后必然超时，而**失败点
被推到了隧道层**，异常文本里只有 `Connection refused`，看不出真正原因。

**修法**（三层，全部落在测试侧）：

1. `wait-for-agent!` 的判据改成**两条都要**：HTTP `/metrics` 抓得到 **且** gRPC 端口
   TCP 连得上（新脚本 `scripts/agent-port-open.sh`，它的退出码就是「TCP 连上了吗」）。
2. 就绪超时从「返回 false 被忽略」改成**抛异常并带上 http/grpc 两个探针的当前值**
   —— 以前失败点被推到后面一层，日志上根本看不出是哪一侧没起来。
3. `start-tunnel!` 加 5 次重试（失败模式几乎都是瞬时的：gRPC 刚起、上一次 run 的
   ssh 孤儿、端口 TIME_WAIT）。**不重试的代价不只是这一格红**：`open!` 崩掉意味着
   `teardown!` 也不跑 ⇒ 残留 agent 影响下一个 cell（这就是「9 个 cell 全灭」的放大机制）。

**教训（一般化）**：探「组件是否就绪」时必须探**判据真正依赖的那个端口/接口**，不能
退而求其次探一个「同进程的、更早起来的」面。这一类错误的形状很固定：**探测面与
使用面不是同一个东西**（这里是 HTTP vs gRPC），而失败信息落在使用面上，于是看起来
像被测系统的缺陷。

---

### F-39 [测试自身] 被中断的 run 把 iptables 分区规则留在 agent 节点上 ⇒ 后续 run 全部「no leader found」

**实测**（F-38 修好之后仍然全红的那一轮，`agent.clj` 的就绪超时信息）：

```
coord-agent a2 on n5 did not become ready within 60000ms (http=true grpc-19527=false)
```

agent 节点上的日志揭示了真正的原因 —— **每一步都在等超时**：

```
17:23:58  connecting to server cluster: [172.19.0.2:50051, ...]
17:24:07  connected to server cluster (attempt 1)
17:24:25  WARN IdGenService: failed to register node_id: cluster unavailable:
          no leader found; all endpoints unreachable
17:24:34  WARN RegistryService: failed to load initial catalog: ... no leader found
17:24:43  WARN RegistryService: failed to subscribe Watch: ... no leader found
17:25:01  WARN plugin identity: bootstrap CCT unavailable: ... no leader found
17:25:28  WARN plugin identity: provisioner session unavailable: ... no leader found
17:25:28  INFO coord-agent gRPC server listening on 127.0.0.1:19527     ← t+90s
```

而 `n5` 上一条被中断的 `:partition-agent-server` cell 留下的规则还在：

```
-A INPUT  -s 172.19.0.2/32 -j DROP      ← 集群三个节点的 IP，双向 6 条
-A OUTPUT -d 172.19.0.2/32 -j DROP
...
```

**根因链**：`:partition-agent-server` 的收尾（`:stop` → `iptables -D`）只在 run 正常
走到 teardown 时才执行；我为了改代码**中断了正在跑的矩阵**，于是规则留在节点上。
而 `scripts/env-reset.sh` 只覆盖**集群节点**（agent 节点从没跑过 coord server，不在
那个脚本的节点集合里）—— 于是后续每一次 run 的 agent 都连不上集群，每个启动步骤
都要等一次超时，gRPC listener 被推到 90s 之后。

**误诊成本**：我先把方向落在「就绪探测」上（F-38 确实是真问题，也确实是同一症状的
另一半），修完仍然全红才去查节点状态 —— 一个 `iptables -S | grep DROP` 就能定位的
问题，花了三轮矩阵的时间。

**修法**：把「清残留」做全 —— 进程（`stop!`）、data_dir（`teardown!`）、**网络规则**
（新增 `clear-leftover-partitions!`，在 `agent/setup!` 里幂等地删除 agent↔集群的
DROP 规则）。放在 `setup!` 而不是只放在 teardown，是因为**被中断的 run 根本没有
teardown**：清理动作必须在**下一次 run 的开头**也做一遍（幂等）。

**一般化**：任何「故障注入的收尾」都必须假设「收尾可能没跑」。所以判据侧的
`setup!` 要把**所有**可变状态（进程 / 文件 / 网络 / 时钟）都恢复到已知基线，
而不是只在正常路径的 teardown 里回滚。

---

### F-40 [测试自身] 重叠判据的最小可分辨量：1ms 的「重叠」不是重叠

**实测**（`lock/none`，F-34 三层根因都修好之后）：

```
lock checker: {:acquires 98, :violations-by-class {:lock-mutual-exclusion 1,
                                                   :lock-fencing-missing 98}}
:probe {:samples 35, :read-failures 0, :with-key 7, :contradictions 0, :near-boundary 0}
```

唯一那条重叠（同一份历史的四种口径对照）：

```
:legacy 1  :new 1  :true-end 0
A holder=h-197014 [711400455, 711400823)   ← gone-at = fencing 探针 RPC **返回**时刻
B holder=h-274749 [711400822, 711401206)   ← B 的 acquire 只早 1ms
```

**归因**：区间闭合时刻是「客户端**观察到**锁已不在我名下」，它永远是真实闭合时刻的
**上界**（差一个 RPC 往返）。上一任的 key 是在那次 RPC **内部**被删掉的，所以后继者
在 RPC 返回前 1ms 成功 CAS 完全合法 —— 而服务端探针在那段时间里**一次矛盾都没看到**
（`:contradictions 0`），独立估计 `:true-end` 也是 0。

**修法**：给重叠判据一个**明示的**测量容差（`default-overlap-tolerance-ms` = 50ms），
口径与探针的 `probe-margin-ms` 一致：
* 重叠 ≤ 容差 ⇒ `:lock-mutual-exclusion-near-boundary`（**出现在报告里**，不进 `valid?`）；
* 重叠 > 容差 ⇒ 硬违约。

**不静默丢弃**是这个取向成立的前提：报告里永远能同时看到「硬违约」与「边界争议」
两栏（`:lock.mutual-exclusion {:hard n :near-boundary m :max-overlap-ms x}`），
所以「把容差调大来刷绿」这件事在报告里是可见的。守门员：
`lock-fixtures/expect-valid-overlap-within-measurement-resolution.edn`
（容差改回 0 必红）；同时 `expect-invalid-overlap.edn`（4490ms）仍是硬违约。

---

### F-41 [测试自身] `(count ids)` 被 destructuring 遮蔽 ⇒ **NextBatch 分支从未成功执行过**

**实测**（`idgen/kill-agent` 的 op-liveness 明细）：

```
:violations [{:f :idgen, :total 166, :ok 7, :fail 143, :info 16, :ratio 0.042
              :errors {"indeterminate: class java.lang.Long cannot be cast to
                        class clojure.lang.IFn ..." 1
                       :unauthenticated 143}}]
```

**根因**（一行）：

```clojure
(let [{:keys [name batch? count]} (:value op)]     ; ← count 被绑定成「请求个数」
  ...
  (assoc op :type :ok :ids (mapv str ids) :n (count ids)))   ; ← 把 Long 当函数调
```

`count` 在 `let` 里被遮蔽成 op 的请求个数（一个 `Long`），于是 `(count ids)` 抛
`ClassCastException: Long cannot be cast to IFn`。jepsen 把它记成 indeterminate，
而它混在 `:unauthenticated` 风暴里，看报告只会以为「agent 重启窗口里全失败」。

**影响比看起来大**：`NextBatch` 是**唯一**承载「batch 内部 ID 互异」判据的分支 ——
也就是说 `idgenck` 的这条判据**一次都没真正判过**（checker 的 `:batches 0` 是唯一的
线索）。这正是 `dev.md` §5.5 第 9 条（「新接一个面的第一跑：先看计数，再看
`:valid?`」）要防的那类静默覆盖缺失：`:valid? true` 而某个子面从未执行。

**修法**：`(clojure.core/count ids)`，并把「为什么不能写 `(count ids)`」写进注释
（它是同一个 `let` 里的名字遮蔽，review 时很难一眼看出）。**顺带**：这条也说明
op 级错误串必须原样进报告（`:errors` 里那条 `indeterminate:` 是唯一把它暴露出来的
东西）。

---

### F-42 [测试自身] `try-nodes` 不在 UNAUTHENTICATED 上轮换 ⇒ 一个坏 agent 钉死整个 run

**实测**（同一 cell）：166 个 op 里 **143 个 `:unauthenticated`**，而另一个 agent
全程是好的（`:agent {:proof {... :agents [{:host "n4" :up? true} {:host "n5" :up? true}]}}`）。

**根因**：`try-nodes` 只在 `UNAVAILABLE` / `forward-request` 时轮换下一个端点。
`:kill-agent` 把 agent 杀掉并重启之后，新 agent 有一段时间 **RoleCache 是空的** ——
那时它对**所有** CCT 都 fail-closed 拒绝（UNAUTHENTICATED）。而客户端把 op 的端点
起点钉在「上一次成功的那个 agent」上（per-key leader 缓存）⇒ 所有 op 都打在坏
agent 上，另一个完好的 agent 一次都没被用到。

**修法**：轮换条件加上 `unauthenticated?`（`client.clj` 的 `try-nodes`）。
**不**把 `PERMISSION_DENIED` 之类**业务性**拒绝纳入轮换 —— 那些换节点也不会变，
而且「换个节点就好了」反而会掩盖真实的鉴权结论。

---

### F-43 [测试自身] 路由证明挂到了**对照组**上 ⇒ diff 矩阵的基线建不起来

**实测**（`matrix-m5-diff` 的 4 个 direct cell 全部红）：

```
map / none :: direct (对照)      M5 CELL ... :failures [{:type :agent-route-not-proven,
                                  :proof {:total 0 ...}}]
```

**根因**：`:agent-scrape`（AG-01 的路由证明门禁）原来只要 run 带了 agent 就挂上，
包括 **direct 对照组**。而对照组的定义就是「客户端直连 server」—— 它**本来就不该**
有 agent 流量，`:total 0` 是正确行为却被判红。于是 T5.2 的归因规则（direct 红 ⇒ 先修
server 面；direct 绿 + via-agent 红 ⇒ 红必属 agent 层）**永远走不到第二步**。

**修法**：只有 `--via-agent` 或 agent 本地面 workload 才挂这条门禁（`coord.clj` 的
`:agent-scrape` 加 `(or (:via-agent opts) local-surface)` 条件）。

---

### F-44 [测试自身] `--via-agent` 这个**布尔旗标**写错了 ⇒ 差分矩阵的 via-agent 侧从未起跑过

**实测**（`map / none :: --via-agent`，就在 F-43 修好、diff 基线第一次可用之后）：

```
Error while parsing option "--via-agent ": java.lang.ClassCastException:
class java.lang.Boolean cannot be cast to class java.lang.String
```

**根因**：选项定义里同时写了 `:default false` 与 `:parse-fn #(Boolean/parseBoolean %)`。
tools.cli 只要看到 `:parse-fn` 就把该选项当成「吃一个值」的选项，于是裸写
`--via-agent` 会把**默认值 `false`**（一个 Boolean）喂给 parse-fn ⇒ 抛异常。
（实测对照：`[[nil "--x" "d" :default false]]` 对 `["--x"]` 解出 `{:x true}`；
加上 `:parse-fn` 就抛上面那条。）

**影响**：`--via-agent` 是 T5.2 差分基线的一半，也就是说**「经 agent」这条路径在
CLI 层根本没跑起来过** —— 而失败发生在参数解析阶段，日志里最后一句是
`make: *** [Makefile:411: test] Error 1`，看起来跟 agent 无关。同一文件里的
`--no-jitter` 有同样的写法（同属一个坑，一并修了）。

**修法**：布尔旗标**只**写 `:default false`（不要 `:parse-fn`）——出现即 `true`。

---

### F-45 [测试自身] capability 授予清单漏了 `data:kv:delete` ⇒ 经 agent 的 Delete 全被拒

**实测**（F-43/F-44 修好后，`map / none :: --via-agent` 第一次真正跑起来）：

```
map / none :: --via-agent        :violations [{:f :delete, :total 13, :fail 13,
                                               :ok 0, :errors {:unauthenticated 13}}]
:agent {:proof {:total 143, :by-method {:put 67, :range 55, :delete 0}}}
```

**形状极其精确**：Put 67 次、Range 55 次全部成功，**Delete 一次都没成功**
（13/13 `:unauthenticated`，而且 agent 的代理计数里 `:delete 0` —— 请求根本没过鉴权）。
**而 direct 对照组是绿的**：server 侧对 root 有全能力旁路（F-32），Agent 侧只按显式
能力集判定 —— 所以「直连绿 + 经 agent 红」这个形态**只有差分跑能抓到**（AG-01 的
全部意义）。

**根因**：`agent.clj` 的 `client-capabilities` 漏了 `data:kv:delete`。权威表在
`coord-core/src/grpc_auth.rs` 的 `rpc_capability`：

```
"/coord.kv.KV/Range"  => "data:kv:read"
"/coord.kv.KV/Put"    => "data:kv:write"
"/coord.kv.KV/Delete" => "data:kv:delete"     ← lab 的清单里没有这一条
```

修法：补上 `data:kv:delete`（map / idempotency 两个 workload 都会走 Delete）。

**为什么这次没有靠新 gates 抓到**：抓它的是**已有的** op-liveness 门禁（第 1 轮
F-13 那条）—— 但只有差分跑才会把「经 agent 的 Delete」这一路真正跑起来，所以
**护栏是「差分矩阵」本身**，而不是又一条新判据。这也说明差分跑不是「锦上添花」，
它是**唯一**能覆盖「同一 RPC 两侧能力模型不一致」的形态。

---

### F-46 [coord-agent，P2 候选] nodeid 注册在 agent 拿到凭据**之前**执行 ⇒ 永远是「best-effort 用派生值」

**实测**（`--workload idgen --agents 2 --agent-idgen-node-ids 7,7`，即**刻意**让两个
agent 用同一个 nodeid；run 判定 **valid**，671 个 ID 互异）：

```
WARN IdGenService: failed to register node_id (best-effort, using derived):
     unauthenticated: missing CCT token
```

**观察**：`idgen.rs` 的启动期 nodeid 注册（`/_idgen/nodes/{nodeid}` 的 CAS + 冲突顺延）
在 agent **还没有 CCT** 的时候就发起了（`missing CCT token`），所以它**每次都失败**、
每次都退回「配置里写的 nodeid」。也就是说：

* 「显式撞车 ⇒ 顺延」这条路径在正常情况下**根本没有被执行过**（我这次跑的是
  `7,7`，如果 CAS 能成，第二个 agent 应该顺延到别的 nodeid 并在日志里体现）；
* 唯一性在这次 45s / 671 个 ID 的样本里成立 —— 但这只能说明**没撞上**，不能说明
  「同 nodeid 不会重复」：snowflake 是 `(ms << 22) | (nodeid << 12) | seq`，两个进程
  各自持 seq，同毫秒同 nodeid 就是**同一串 ID**。

**为什么只记 P2 候选**：要把它升级成 P1 需要构造「两个 agent 同 nodeid + 同毫秒发号」
并**观测到重复 ID**（或者在日志里看到 CAS 成功时确实顺延）。当前证据只够说
「注册路径实际上是死的」+「理论上存在重复窗口」。判据侧下一步：把 `7,7` 的 run
时长/速率拉高（比如 10 分钟、concurrency 4n）看能否抓到重复；同时这也是
`apis/contracts/STATUS.md` 里 idgen GA（2026-10-31）要求的「时钟回拨防护落地或
明确不承诺边界」的同一类问题。

---

### 本轮矩阵的实测结论（9 个 cell）

> 下面两栏分别是**第一次**（F-35/36/37 修复前）与**最终**（F-35…F-45 全部修复后）
> 的结论。最终一栏的 run 路径在 `jepsen/store/coord/`（见 `PROGRESS.md` 的记录）。

| cell | 第一次 | 最终 | 判据侧结论 |
|:--|:--|:--|:--|
| `lock / none` | 红 | 红 | **只剩 F-28**（fencing 110/110）。互斥：`:mutual-exclusion {:hard 0 :near-boundary 0}`；探针 45 样本 / 0 读失败 / **0 矛盾** / 0 边界 |
| `lock / kill-agent` | 红（建于 F-37 之上） | 红 | 同上（只有 F-28） |
| `lock / partition-agent-server` | 红 | 红 | 同上（只有 F-28） |
| `election / none` | 红（124 条假双 leader） | **绿** | F-35 修复后双 leader = 0 |
| `election / kill-agent` | 红（8 条） | **绿** | 同上 |
| `idgen / none` | 绿 | **绿** | — |
| `idgen / kill-agent` | 红（F-37 路由证明） | **绿** | F-41（`:batches 0 → 27`）+ F-42 修复后 |
| `registry / none` | 绿 | **绿** | 幽灵实例判据在 45s 里都有样本 |
| `registry / kill-agent` | 红（F-37） | **绿** | F-42 修复后 |
| `map / none`（差分对，`matrix-m5-diff`） | direct 红（F-43） | **direct 绿 / via-agent 绿** | F-43/44/45 修完后第一次拿到可用的差分基线（via-agent 的路由证明 `:total 161`） |
| `soakfull`（`--soak-mix` 默认含四个 agent 面） | — | **起得来** | soak 日志里能直接看到 `:lock-contend` / `:elect-campaign` 的 invoke/completion（§14 待办 4 的证据） |

**一句话总结**：coord-agent 的四个本地面现在都能在 lab 里被判决；`lock` 的红
**只剩 F-28（fencing 真缺陷）**，其余三面（election / idgen / registry）在本轮修复后
**全绿**；差分基线（direct vs `--via-agent`）第一次可用。

---

## 16. 第七轮（2026-09-19）：AG-06（崩溃持有者的服务端回收）+ election 服务端探针 —— 并挖出**凭据面**的 P0

> 本轮的目标是「把 coord-agent 再做实一层」：给四个本地面补上**只有多进程 + 真故障
> 才能判**的三类判据 —— ①崩溃持有者的资源回收（AG-06）；②「自述 vs 服务端真相」的
> 第二个面（election 的服务端探针，F-35 的残留）；③弃锁在故障前后**期望值相反**的
> 成对判据。为此新增了一个时间窗工具（`jepsen.coord.faultwin`）。
>
> 结果：三类判据全部落地并在 lab 真跑；**第一跑就交出一条 coord-agent 的 P0 候选
> （F-50）** —— 而且它是**整个 agent 面此前所有绿灯的共同盲区**。

| # | 一句话 | 分级 | 状态 | 判决任务 |
|:--|:--|:--|:--|:--|
| F-50 | **coord-agent**：服务端开启鉴权时，agent **自发**的后台流量不带凭据（`missing CCT token`）⇒ 锁自动续期 / registry 目录加载 / registry watch 订阅 / idgen nodeid 注册 **全部失效** | **P0 候选**（锁会「静默丢失」，调用方/SDK 以为仍持有） | `confirmed-by-run`（agent 日志 4 处 + 锁面服务端探针 9/13） | M5a / AG-06 |
| F-46 | （根因修正）原记「nodeid 注册发生在拿到 CCT 之前」，实测证明不是时序：**共享客户端根本没有凭据通道** | P2 → 归并进 F-50 | `superseded-by F-50` | M5a |
| F-51 | **coord-agent**：auth 下 registry「以空缓存启动 + 订阅失败」⇒ 多 agent 拓扑里**跨 agent 发现**结构性不可用（当前 checker 未覆盖「缺失」，只覆盖「陈旧/幽灵」） | P1 候选 | `open`（判据待补，下一轮） | M5b / registry 面 |
| F-52 | **coord-agent（边界待确认）**：锁的后台续期节拍硬编码 10s ⇒ `ttl <= 10s` 时续期跑不赢到期（已登记 dev.md §5.4-⑨ 待书面确认） | 待确认 | `open` | M5a / lock 面 |
| F-47 | 测试自身：Makefile 里传给 checker 的 EDN 选项，双引号被 `docker exec … bash -c "…"` 这一层吃掉 ⇒ 字符串常量**静默变成 symbol** | P2（测试自身） | `closed`（4 处 EDN 参数改成 `\"` + 写进 dev.md §5.5 第 25 条） | T0.3 / `make checkers` |
| F-48 | 测试自身：`phantom-loss-violations` 多一个 `)` ⇒ 函数**返回 `vec` 函数本身**；`lein check` 全绿，只有真跑才炸（`Don't know how to create ISeq from: clojure.core$vec`） | P1（测试自身） | `closed`（+ dev.md §5.5 第 26/27 条） | M5a / AG-06 |
| F-49 | 测试自身：AG-06 第一版把「判过」实现成「出现在违反列表里」⇒ **合法历史被判成未判定**，正例 fixture 第一次跑就红 | P1（测试自身） | `closed`（判定与覆盖分开算 + dev.md §5.5 第 23 条） | M5a / AG-06 |
| F-53 | 测试自身：run 边界用 jepsen `:time`（测试起点相对量），样本时刻用 `:t0-ns`（ms-since-boot）⇒ 两个时间轴混用，每条弃锁都「窗口内没有样本」（F-34 那类问题的**同型复发**） | P1（测试自身） | `closed`（run 边界也走 `abs-ms`；被 `:lock-abandon-unjudged` 门槛当场抓到） | M5a / AG-06 |
| F-56 | 测试自身（**open**）：idgen 的撞车实验（`--agent-idgen-node-ids 7,7`）零重复 —— 但解码 167709 个 ID 的 nodeid 字段**全是 7** 却也零重复，原因是 `--nemesis none` 下 `try-nodes` 的 per-key leader 缓存**不回退** ⇒ 同一把发号器名永远打**同一个** agent，「两个进程同毫秒同 seq」这个条件根本没被制造出来 | P1（测试自身） | `open`（下一轮：配 `kill-agent`/`pause-agent` 强制轮换，或让两个 agent 同时持续服务） | M5a / idgen 面 |
| F-55 | 测试自身：`:partition-agent-server` 的清理**不验证**（逐条 `meh -D`，重复规则删不掉）⇒ 正常结束的 partition cell 也在 n5 留下 6 条 DROP，后续 3 个 cell 全部以「agent 就绪超时」失败（症状与 F-38/F-39 同形） | P1（测试自身） | `closed`（删多次 + **读回验证**，非 0 直接抛异常） | M5a / 所有 partition cell |
| F-54 | 测试自身：把「TTL ≤ 续期节拍」做成**豁免**的第一版，连带赦免了「agent 归因失败」⇒ `expect-invalid-abandon-unjudged` 从 invalid 变 valid | P1（测试自身） | `closed`（豁免只对可归因的 op 生效；最终取消豁免、改成诊断字段） | M5a / AG-06 |

---

### F-50 [coord-agent，P0 候选] 鉴权开启时 agent 自发流量无凭据 ⇒ 保活/订阅/注册全线失效

**怎么撞上的**：AG-06 的服务端探针在**零故障**的 `lock:none` cell 里看到「持有者还
活着，服务端 key 却不见了」。第一反应是测量问题（F-34 的教训），于是去 agent 节点
上看日志 —— 结论完全不是测量：

```
$ grep -hoE "…" /var/log/coord-agent-a*.log | sort | uniq -c
 89  failed to register node_id (best-effort, using derived): unauthenticated: missing CCT token
 89  RegistryService: failed to subscribe Watch: unauthenticated: missing CCT token; entering self-protection
 89  RegistryService: failed to load initial catalog: … unauthenticated: missing CCT token; starting with empty cache
 65  auto-renew of lock 'lock-*-abandon' (lease=N) failed: unauthenticated: missing CCT token
     and server-side verification also failed (failed to read lock key … from server:
     unauthenticated: missing CCT token); keeping local record (fail-safe, will retry)
```

四个**互相独立**的服务（lock / registry 目录 / registry 订阅 / idgen 注册）给出同一句
错误。而这四处的共同点是：**它们是 agent 自己发起的调用，没有调用方**。

**根因（源码锚点）**：agent 的出站凭据是**任务局部量**，只装「调用方转发进来的
CCT」：

* `coord-agent/src/auth/interceptor.rs:556`：
  「第四轮 §3.2：把**调用方自己的凭据**转发给服务端 … 此前 agent 出站客户端
  `token_provider = None`，生产默认配置（auth_enabled=true）下经 agent 的调用一律
  `missing CCT token`」；
* `coord-agent/src/lib.rs:629`：
  「引导 CCT 只注入**独立**的插件身份客户端，**不污染**共享 `inner.client`（代理数据面
  流量不应携带引导凭据）」；
* 而四个服务的后台路径用的**正是**共享 `inner.client`
  （`lock.rs:638` 的 `keep_alive`、`lock.rs:113` 的回查、`registry.rs:530/586` 的
  `watch`、`registry.rs:143` 的目录加载、`idgen.rs:565` 的 CAS 注册）。

⇒ **有调用方转发凭据的路径全通（所以此前所有 run 都是绿的），agent 为自己做的事全死。**

**影响面（按契约条款排）**：

1. **lock 的「自动续期」承诺失效**（`lock.rs` 文件头明写「封装重试与**自动续期**」）：
   调用方拿到的锁在 `ttl` 后从服务端消失，而 agent 的本地缓存**仍然认为自己持有**
   （`RenewAction::Keep` 的 fail-safe 分支，连回查都因为同一原因失败）⇒ 调用方可能
   在**没有锁**的情况下跑完临界区。这正是 `lock.rs:187-215` 记录过的历史 P0 的形态
   （那次的成因是本地墙钟判过期，这次的成因是凭据）。
2. **registry 以空目录启动、且永不订阅**：`starting with empty cache` +
   `entering self-protection` ⇒ 多 agent 下**跨 agent 发现**结构性不可用（F-51）。
3. **idgen nodeid 注册永远失败** ⇒ 「显式撞车 ⇒ CAS 顺延」这条路径从未被执行过
   （F-46 的真实根因；原判断「时序问题」不成立）。
4. 附带：`config_center` / `workflow` 等同样走共享客户端的服务，只要有订阅/回查也会
   一起失效。

**为什么此前 20+ 个 cell 全绿也没抓到**（这一条比缺陷本身更值得记）：

* 所有 workload 的**前台**操作都会转发调用方 CCT ⇒ 代理路径一切正常；
* 「agent 自发的流量」只有**后台保活/订阅/注册**，而这四件事在**短跑**里几乎不可见
  —— 5s TTL 的锁在 200ms 的持有窗口里根本走不到续期；registry 的跨客户端判据只看
  「陈旧」（别人看得到），不看「缺失」（别人看不到）；
* ⇒ 这正是 `dev.md` §5.5 第 22 条（「每一个**子面**都要单独问一句它真的跑了吗」）
  的又一次翻版：**agent 的自发流量此前没有任何一条判据**。
  本轮 AG-06 的「弃锁 + 服务端探针」是第一条专门打这条路径的判据，第一次跑就红。

**复现配方**（docker lab，`--time-limit 120`，`--agents 2`，服务端 auth 开启）：

```bash
make test WORKLOAD=lock NEMESIS=none TIME_LIMIT=120 CONCURRENCY=2n AGENTS=2 \
          LOCK_TTL_SECONDS=30 SKIP_CHECKERS=1
# 判据侧：lock checker 的 :abandon {:ops 13 :judged 12 :phantom-loss 9}
# 直接证据：agent 节点 /var/log/coord-agent-a*.log 的 auto-renew 失败（4 处同因）
```

**建议的修复方向（待 coord 团队确认）**：给 agent 一个**独立的服务账户凭据**（与插件
服务账户同一机制、但能力集按服务需要最小化），并且**只在没有调用方转发的 CCT 时**
使用它（否则会把 agent 自己的权限借给调用方，反而放大权限）。判据侧不再需要新增
判据即可验证：AG-06 的两条（弃锁在 ttl+grace 内被回收 / 活着时不得消失）已经把这两侧
都钉住了。
---

### 本轮 lab cell 一览（docker lab，全部 `--agents 2`）

| cell | 判定 | 关键数字 | 归因 |
|:--|:--|:--|:--|
| `lock / none`（`LOCK_TTL_SECONDS=30`，120s） | 红 | `:abandon {:ops 13 :judged 12 :phantom-loss 9}`；`:fencing-missing 184`；`:mutual-exclusion {:hard 0 :near-boundary 2}`；探针 127 样本 / 0 矛盾 | **F-50**（假丢锁）+ **F-28**（fencing） |
| `lock / none`（ttl=5 默认，120s） | 红 | `:abandon {:ops 52 :judged 50 :phantom-loss 50}`（`phantom-loss-below-cadence 50`） | 同上（TTL 小只是**第二个**成因，见 F-52） |
| `lock / kill-agent`（180s） | 红 | `:abandon {:ops 1 :orphans 0}`；`:acquires 7`（故障期吞吐极低） | 回收侧没红；样本太少，需要更长/更狠的配置（记在下一轮） |
| `lock / partition-agent-server`（150s） | 红 | `:abandon {:ops 47 :orphans 0 :phantom-loss 42}`；`:acquires 174` | **回收侧 `:orphans 0` = 服务器确实会回收**；红全部来自 F-50 |
| `election / none`（90s） | **绿** | `:campaigns 175 :won 56`；探针 74 样本 / `with-key 64` / 0 矛盾 / 0 边界 | 服务端真值判据第一次在线（F-35 残余闭环） |
| `election / kill-agent`（120s） | **绿** | `:campaigns 208`；探针 113 样本 / 0 矛盾 | 同上；`:resign-failures 2`（leader 续期失败 → 退位，与 F-50 同源但不违反所判契约） |
| `idgen`（nodeid 7,7 / rate 200 / 8n，180s） | **绿** | `:ids 167709 :distinct-ids 167709 :batches 17704`；解码后 nodeid 字段**全部 = 7** | **但结论是「未制造出条件」**：见 F-56 |

**一句话总结**：AG-06 的两半在 lab 上分开验证了 —— **回收侧是好的**（`:orphans 0`），
**「不得假丢锁」侧是红的**，而红的原因不是测试（F-34/F-53 的教训已在判据里处理掉），
是 **F-50：agent 自发的保活流量没有凭据**。


---

## 17. 第八轮（2026-09-19）：M5b 起步 —— agent 本地数据面（cache / mq）落地

本轮把 `coord-agent-coverage-plan.md` 里 **M5b 的两个不需要插件引擎的面**做成了
可跑的 workload + checker + fixture：`--workload cache`（AG-09）与 `--workload mq`
（AG-11）。两条判据线都是**先写负控制 fixture、再进 lab**（§0-1），因此过程中
抓到的四条缺陷全部是**测试自身**的（F-59…F-62），且都被 fixture 当场拦下 ——
这正是「没有负控制的 checker 不算完成」的价值。

### F-57 [coord-agent，P1] `MqPublishRequest.idempotency_key` 被声明但没有被使用

* **形态**：proto 里有 `idempotency_key` 字段（`agent_api.proto` 的
  `MqPublishRequest`），但 gRPC 发布路径**从不读它** ——
  `coord-agent/src/services/grpc_handlers.rs:626-655` 的两条分支
  （`produce_replicated(...)` 与 `produce(...)`）都把 `None` 当作 header 传下去，
  `req.idempotency_key` 与 `req.key` 都被丢弃。
* **后果**：调用方在「响应丢失后重试」时会给下游**多一条消息**。对 at-least-once
  的消费侧这是「幂等键形同虚设」；对需要精确一次的账务/发号场景是重复副作用。
* **判据侧**：`mqck` 有判据 7（`mq-idem-not-deduped`），但**默认只记录不判红**
  （`:expect-idem-dedupe?` 默认 false，报告里是 `:idem-dups`）。理由是契约措辞
  未书面确认：proto 声明了字段，但没有任何文档承诺 broker 侧去重。两条 fixture
  把这个开关的两个方向都钉住了（`mq-fixtures/` 只记录 / `mq-fixtures-idem/` 硬判）。
* **复现**：`make test WORKLOAD=mq NEMESIS=none AGENTS=1 TIME_LIMIT=60
  SKIP_CHECKERS=1`（checker 报告里的 `:idem-dups` > 0 即为本条）。
* **状态**：`open`（等 §7 的书面确认；确认后把默认值改成 true，MQ cell 会按预期变红
  并把这条升级为硬判据）。

### F-58 [lab/拓扑，P1] cache/mq 的数据在 **agent 进程本地** ⇒ 跨 agent 面结构性不可测

* **事实**：`cache.rs`（redb）与 `mq.rs`（本地日志 + 可选 ISR 复制）都把数据放在
  **agent 进程本地**；ISR 复制默认关闭，且需要 `replication_peers` 里给出**可达的
  peer 地址**。
* **本 lab 的硬约束**：agent 刻意只绑 loopback（`agent.clj` 的 `config-str`）并由
  控制机用 SSH 隧道接入 ⇒ agent 之间**不可达** ⇒ `replication_peers` 无法建立。
* **因此未覆盖**：AG-09 的 ISR 复制承诺（复制日志 / 持久化幂等键 / 本地序列号）、
  AG-11 的跨 agent 投递、以及「分区期间的复制降级语义」。已写进两个 checker 的
  「漏检边界」段。
* **已在测试侧做的处置**（不是权宜之计，而是**拒绝假红**）：`coord.clj` 新增
  `local-consistency-workloads = #{:cache :mq}`，在 `--agents > 1` 时**构造期抛异常**
  —— 多 agent 下「读不到刚写的值」是合法的（数据在另一个 agent 上），放行只会
  产出假红或假绿。
* **待 coord/引入方决定**（§7）：是否给出一个 agent 间可达的拓扑（daemonset 形态，
  或同机多端口 + host 网络）来覆盖复制面；若不覆盖，需要在契约里明确
  「cache/mq 是进程本地、不承诺跨 agent 一致」。
* **状态**：`open`（覆盖边界已声明；拓扑决定待书面确认）。

### 本轮测试自身缺陷（F-59…F-62）—— 全部由 fixture 拦下

| 编号 | 形态 | 危害 | 处置 |
|:--|:--|:--|:--|
| F-59 | `intervened?`（区间干扰闸门）把**被审的那个写自己**也算成干扰（它的完成时刻就是窗口左端点，永远与自己相交） | 「丢写 / TTL 提前到期 / TTL 幽灵」三条判据被**静默关闭**：三条负控制 fixture 全部假绿 | 加 `op-id` 排除自身；三条 fixture 转红 |
| F-60 | TTL 判据把**毫秒**的 ttl 与**纳秒**的时间戳直接相减 | 两个方向同时错：`ttl-early` 的门槛塌成 0ns（永不触发），而 `ttl-ghost` 对**一切正常读**都成立（3s 读一个 ttl=5s 的值被判幽灵）。lab 里会红成一片，看起来像被测系统崩了 | 统一换成 ns 再比；正例 fixture（同一份历史里 TTL 两侧都合法）把它钉住 |
| F-61 | `ack-events` 用 `{[topic partition offset] → ack 时刻}` 且**后写覆盖前写** | 「已确认的消息又被投递」判据用「最后一次确认」当时刻 ⇒ 第一次确认之后的重投被漏掉（`expect-invalid-ack-not-honoured` 假绿） | 保留**最早**一次确认；该 fixture 转红 |
| F-62 | `list-violations` 少一个 `)`（F-48 的同型）：`(vec (mapcat <fn>) <coll>)` 被解析成 `vec` 的两个参数 | 编译通过、`lein check` 通过，`vec` 在运行期抛 ArityException（整条 cache 套件不可判） | 括弧修正；**先跑 fixture 套件再进 lab** 这条纪律再次生效（§5.5 第 26/27 条） |

> 顺带一条：`default-cache-keys` 的 `#(str "cache-list-" %-占位)` 少写了 `%`，
> 被 `lein check` 以「Wrong number of args (1) passed to ...」当场挡下（这类
> 加载期错误比运行期好得多）。

### 本轮交付（代码 + fixture）

| 项 | 内容 | 状态 |
|:--|:--|:--|
| wire 层 | `CoordRpc.java`：Cache（Get/Set/Delete/LPush/LRange/LLen/SAdd/SMembers）+ MQ（CreateTopic/Publish/Poll/Ack）共 12 个方法的 descriptor；`proto.clj` 的请求构造/响应读取（含 bytes ↔ String 的统一口径） | ✅ `scripts/check-agent-wire.clj` 自检通过 |
| cache 面 | `--workload cache`（三类键空间互不相交：string/list/set；一半 Set 带 TTL）、`jepsen.coord.cacheck`（7 条判据 + 4 项样本门槛）、`scripts/cache-fixtures/`（10）+ `scripts/cache-fixtures-sample/`（1） | ✅ **10/10 全绿**（含 3 条 TTL/丢写负控制与 3 条守门员） |
| mq 面 | `--workload mq`（发布 + poll 内联 ack；1/4 发布是刻意重发）、`jepsen.coord.mqck`（6 条判据 + idempotency 观察 + 2 项门槛）、`scripts/mq-fixtures/`（10）+ `scripts/mq-fixtures-idem/`（1） | ✅ **10/10 全绿** |
| 能力引导 | `agent.clj` 的 `client-capabilities` 补 `coord:cache:read/write`、`coord:mq:manage/publish/consume`（取自 `coord-core/src/auth` 的权威表；F-45 的教训） | ✅ |
| 门禁 | `make checkers` 由 28 套 / 143 个扩到 **32 套 / 152 个**；新增 `matrix-m5b`（cache/mq × none\|kill-agent\|partition-agent-server，`--agents 1`） | ✅ 离线全绿；lab 见 PROGRESS §1.8 |

### 本轮 lab 首跑暴露的**测试自身**缺陷（F-63/F-64/F-65）—— 全部是被「先跑 fixture 再进 lab」之外的当场信号抓到的

| 编号 | 形态 | 症状（看起来像什么） | 处置 |
|:--|:--|:--|:--|
| F-63 | cache/mq 的判据**混用两个时间轴**：起点用 op 自读的 `:t0-ns`（boot 相对量），完成用 jepsen 的 `:time`（**测试起点**相对量）⇒ 「写完成 < 读开始」恒真 | 一次 `cache/none` 跑出 **37 条违反**（丢写 10 / list 丢推入 18 / set 丢成员 9），看起来像「cache 写入后读不回来」的 P0 | 每个 cache/mq completion 增加 `:done-ns`（`System/nanoTime`，与 `:t0-ns` 同域），`end-ns` 优先用它；**同一 cell 从 37 条违反变为 0**（F-53 的同型复发） |
| F-64 | clojure `case` 里放 Java 枚举常量（`Descriptors$FieldDescriptor$Type/STRING`）——**编译能过、运行期永不匹配** | 每个 cache 请求都抛 `cache-request: unsupported field type STRING for :key` ⇒ 六个 cell 全部「0 条 op 完成」，报告看起来像**被测系统一条 op 都没成功** | 改 `cond` + `=`（探针实测 `=` 为 true、`case` 不匹配）；顺带说明为什么它比语法错更危险：它把「测试侧编码失败」伪装成「被测系统全灭」 |
| F-65 | `defrecord CoordClient` 新增字段后，位置构造器少传一个参数（16 个字段传 15 个） | 每个 cell 起跑即 `Wrong number of args (15) passed to ->CoordClient`（`lein check` 查不出，记录构造是运行期的） | 补参数；纪律：`defrecord` 字段数变化后必须至少真起一个 cell（§5.5 第 26 条的同族） |
| F-66 | cache/mq 的**键空间与主题名没有带 run 标签**（值带了） | 上一个 cell 留在 agent 本地 redb / MQ 日志里的数据被本 cell 读回：一条历史里出现**两个 run 标签**，并产出 `:fabricated` 假红 | 键与主题名都拼 run 标签（与 map/scan/txn 的 `run-tag` 纪律一致）；同一条 fixture 纪律：`cache-fixtures/expect-valid-clean.edn` |

> **口径说明（重要）**：F-63/F-64/F-66 都曾**看起来像 coord-agent 的缺陷**（「cache 写完读不回」、
> 「agent 一条 op 都不成功」、「读到别的 run 的值」），实际是测试侧的度量/编码/隔离问题。
> 三条都已修复，修复后 `cache/none` 的 checker 报告为 `:violations-by-class {}`。
> 这正是「负控制 + 先跑 fixture + 单变量分诊」三件事同时存在的价值：**没有任何一条被写进
> 缺陷账当成 coord 的问题**。

### F-67 [待分诊，P1] mq 的 `Ack` 一次都没成功（`:poll-ack-failures 59/59`）

* **证据**：`make test WORKLOAD=mq NEMESIS=none TIME_AGENT=45 AGENTS=1`（修好主题接线后）——
  checker 报告 `:polls 59 :poll-ack-failures 59 :acked-offsets 0`，而 59 个 Poll 全部拿到消息、
  `:violations-by-class {}`。也就是说：**消费侧从未提交过偏移**。
* **影响**：at-least-once 的「不丢」判据依赖「确认过的游标才前进」（本仓 checker 的锚），
  而游标因 Ack 全失败**停在 0** ⇒ 判据只能验证「重复投递」（合法），**验证不到「静默丢失」**。
  另外生产上消费者永远提交不了偏移 ⇒ 重启后整队列重放。
* **待分诊**：①能力点（`coord:mq:consume` 已按权威表授权）；②Ack 处理器是否要求分区 Leader
  （`grpc_handlers.rs` 的 Ack 注释提到「复制启用时仅 Leader 提交偏移」，而本 lab 关闭复制）；
  ③拓扑（1 agent 下 shard leader 判定）。
  **在分诊前，`mq` 的 cell 只能算「部分判据可用」**（已把 `:poll-ack-failures` 打进 summary 作为可见信号）。
* **状态**：`open`（下一轮的第一件事）。

### F-69 [P0 候选，open] 单节点 kill 后 leader 判 fatal（无可用快照）⇒ 全集群写入永久失败

* **发现（2026-09-25，T2.3 2h 组合浸泡）**：`soakfull`（mix `map=20,watch=40,lease=40`，
  rate 2 / 2n / seed 42，集群 n1/n2/n3）在**首次 kill 之后彻底失能**，run 判 invalid：
  - 12:27:27 kill n1（当时的 leader，term 1）⇒ n2 当选（term 2），之后 **7.5 分钟服务正常**；
  - **12:34:55.5 起 n2 的 raft 停止服务**（`raft_wm` 心跳末行 `12:34:55.540750Z`，此后无一行）；
    自 12:34:55.950 起**所有 client 写**返回
    `INTERNAL: raft auth write failed: when Read Snapshot(None): snapshot not found`
    （n2 日志 9626 处，直到 run 结束 13:59）；
  - 12:38:11 n1 重启、n3 当选 leader 后**首笔写立即同错** ⇒ 不可自愈；
  - gates：两个 quiet 窗口 `0/955`、`0/1054`（ratio 0.0），`rto-unrecovered=2`（预算 120s）。
* **机制（已定位到代码）**：错误串唯一构造点 = openraft-0.10.0-alpha.34
  `src/replication/snapshot_transmitter.rs:203`：leader 给落后 follower 发快照时，
  state machine 的 `get_current_snapshot()` 返回 `None` ⇒ `StorageError("snapshot not found")`。
  openraft 把复制路径的 StorageError 经 `replication_context.notify_storage_error()` 上报
  RaftCore ⇒ **该节点进入 fatal**（“存储错误不可恢复”是 openraft 的设计）⇒ 之后全部
  `client_write` 返回 `Fatal(StorageError)`。n2 心跳停摆与 fatal 语义一致；另注意 n2 进程
  **未退出**（服务已停、进程仍在）——生产上需要一条自愈/告警路径（见修法方向 ③）。
* **触发条件（未完全定位，已收窄）**：快照发送只在「某 follower 的 next_index <
  leader 的 log_start（已清理区）」时启动。原始 run 中该时刻 ≈ 10009（= 上次快照 4999 +
  策略 `LogsSinceLast(5000)` 的第二个触发点），当时 n1（失联）的 next_index 8185 恰落在
  第二次 purge（≈8999）之下。**候选**：purge 推进与快照可用性的时序窗口（发送启动时读到
  “无快照”）——需要一次命中该窗口的复现（见下）来证实/证伪。
* **取证缺口（两处，建议修）**：① harness 默认 `RUST_LOG=coord=info`（db.clj:188）⇒
  **openraft 自身日志不落盘**（本次已用 `COORD_RUST_LOG=coord=info,openraft=info` 绕过）；
  ② coord stdout 为**块缓冲** ⇒ SIGKILL 丢缓冲（被 kill 节点的最后一段日志消失）、
  SIGSTOP（`pause` nemesis，实测 `ps` 状态 `Tl`）造成“日志停更”的假象。
* **次生（非独立缺陷）**：mapck 把 256 条 **`:fail`** 的 delete op 记成
  `:delete-response-inconsistent`（失败 ≠ 违反；与 F-68 同族的分类问题）；leaseck 的
  3 条 `:lease-not-expired`（lease id 349–351）与故障同一时刻，是结果不是原因。
* **对照实验（已跑，未复现）**：`--workload register --nemesis soak --soak-quiet 60
  --soak-disrupt 240 --rate 30 --time-limit 600` + `COORD_RUST_LOG=…,openraft=info`
  ⇒ `Everything looks good`；结构差异：kill 仅 ~3 分钟，失联节点（n3，applied=769）
  在 leader 首次 purge（→3999，14:15:39）之前就已追平 ⇒ **从未出现“next_index <
  log_start”的 follower，也就没有快照发送**（全 run `ReplicateSnapshot`/`error
  replication` 计数 = 0）。
* **复现要点（下一次 lab 作业的第一优先）**：
  1. **让某 follower 的 next_index 落在 purge 点之下**：kill 早于首次快照触发
     （index≈5000）~1–2k 条，并让 kill 窗口跨过第二次触发（≈10000），全程不恢复；
  2. 打开 `COORD_RUST_LOG=coord=info,openraft=info`；
  3. 监控 `ReplicateSnapshot` / `snapshot sending` / `error replication to target`：
     若出现 `error replication to target: …Read Snapshot(None)` 即命中。
* **建议修法方向（待评审，不在本轮实施）**：
  1. `get_current_snapshot()==None` 时，复制侧应**按需构建**快照（openraft 启动路径
     `storage/helper.rs:197` 已有此模式），而不是让存储错误升级为节点 fatal；
  2. 或把“无快照可送”降级为该 follower 的复制暂停 + 告警（不停止整节点服务）；
  3. coord 进程对 raft fatal 的自愈（退出让编排层重启，或健康检查置死 + 告警）；
  4. mapck 失败分类修正（与 F-68 同族）。
* **影响面**：M2 收口（W3-1）受阻；P-Gate 1/3/4 的「故障注入下不得永久失能」判据必须
  等本缺陷闭环；**RC 冻结前必须修**。
* **状态**：`open`；证据归档：
  `docs/production/evidence/20260925T141209Z-t2.3-m2-watch-lease-2h-kill-snapshot-fatal/`
  （主）、`…/20260925T142520Z-diag-kill-3min-openraft-logs-no-repro/`（对照）。
* **根因（2026-09-25 第十轮已定位到代码，含判据）**：`StateMachineStore` 的
  `get_snapshot_builder()` 此前把 `current_snapshot` **深拷贝**给 builder 克隆，
  而 openraft 恰好在克隆上执行 `build_snapshot()`（`sm/worker.rs` 的
  `try_create_snapshot_builder` → `C::spawn(async { builder.build_snapshot() })`），
  复制路径的 `GetSnapshot` 又走**主实例** —— 两个实例各持一份内存槽 ⇒ 构建结果
  对主实例**永远不可见**，`get_current_snapshot()` 恒为 `None`。
  purge 守卫不受影响（`snapshot_tracker` 本来就是 `Arc` 共享，构建的
  `record_durable` 生效）⇒「日志已 purge 到 8999 + 主实例无快照」这个组合
  正是 fatal 的触发面；n1 失联使 `searching_end(8185) < purge_upto_next`
  ⇒ 快照发送启动 ⇒ `StorageError::read_snapshot(None, "snapshot not found")`
  ⇒ RaftCore fatal（不可自愈；重启节点只是把同一形态换到下一个 leader）。
* **修复（第十轮）**：`coord-server/src/raft/state_machine.rs`
  ①`current_snapshot` 改为 `Arc<Mutex<…>>` **共享槽位**（builder 与主实例共用）；
  ②`build_snapshot` 经新增的 `publish_snapshot()` **单调发布**（防迟到的旧构建
  覆盖已安装的新快照）；③`get_current_snapshot()` 增加**磁盘兜底**（内存槽为空时
  从 `META_SNAPSHOT` + SHA256 校验加载；返回 `None` 的代价是整节点 fatal，
  一次磁盘读的代价远低于此）。判据：单测 3 条 + 集成 1 条（真实 openraft，
  见 `coord-server/tests/snapshot_visibility_test.rs`），修复前单测 2 红/集成
  超时红，修复后全绿（负控制双向）。
* **次生（同轮顺手修）**：`mapck` 的 `delete-shape-fails` 此前对**所有** delete
  op（含 `:fail`）断言响应形状 ⇒ 失能窗口里的 256 条 `:fail` 被误判为
  `:delete-response-inconsistent`（失败 ≠ 违反）。修法：形状断言只适用于
  `:ok`（与 `exists-fails` 同口径）；新增守门员 fixture
  `expect-valid-delete-fail-not-shape-violation`，配对的
  `expect-invalid-delete-response-inconsistent`（`:ok`+deleted=3）必须仍红。
* **待办（不在本轮）**：修法方向 ③（进程对 raft fatal 的自愈/告警路径）仍开放：
  fatal 后进程不退出、健康检查不置死 ⇒ 生产上需要一条「编排层可感知」的路径；
  归入 W5-4/W6 的后续项评估。

## 18. 第十轮（2026-09-25）：F-69 现场闭环 + 2h soak 复跑的 lease 违规归因

> 输入：`docs/production/evidence/20260925T185131Z-t2.3-m2-watch-lease-2h-f69-fixed/`
> （run 16:40:35→18:44:29 UTC，SEED=42，二进制 `8dd7f766` @ `eb91d58`，工作树 clean）、
> 同 run 的 `store/coord/2026-09-25T16:40:27.876181482Z/{history.edn,results.edn}`。

### F-69 收口记录（现场验证通过）

* **修复已验证**：kill n1（leader，17:13:32）后，旧 run 的 fatal 窗口（kill 后 ~7.5 min，
  第二次快照 9999 触发、purge≈9000 越过失联节点位置）在本轮 **三节点 `snapshot not
  found` 全 0、客户端全时段 `:fail` 0**；n1 于 17:24 重启后日志出现
  `Installed snapshot persisted to /var/lib/coord/snapshots/snapshot-9999-2.snap`
  （17:24:02.717842Z）——快照被成功传输并安装，随即恢复服务。
* **判据已机械化**（进程内，不需要 lab）：`coord-server/tests/replication_snapshot_f69_test.rs`
  （3 节点 + 网络阻断隔离一个 follower ⇒ purge 越过其位置 ⇒ 断言 leader 不失能、
  恢复后经 install_snapshot 追上）。负控制：还原 `state_machine.rs` 至修复前 ⇒ **2.35s
  复现 `leader went fatal`**；修复版 ~10s 通过、连跑 3 次稳定。
* **状态：closed（待 CI 覆盖三节点判据后转为常驻回归）**。

### F-70 [P1 候选，fixed（待 2h soak 复验）] leader 切换窗口：keepalive 立即 `NOT_FOUND` 且绑定 Key 从未被删

* **证据（同 run）**：
  - `ka/7636`：17:13:19.019 grant `{:id 1, :ttl 2}` → Put 成功（after-write 读可见）→
    **首次 KeepAlive 即 `NOT_FOUND: keep-alive failed: lease 1 not found`**
    （`keepalives []`、`keepalive-error` 置位）；随后 6.16s 内 **41 次轮询读全部成功**
    且 Key 始终存在（`:observations [{:phase :first-absent, :absent? false, :reads 41,
    :failed 0}]`）⇒ **不是观测缺口，是真实的活性违反（Key 泄漏）**。
  - 同时刻上下文：n3 于 17:13:19.030 成为 leader 并记录
    `LeaseManager rebuilt from state machine: 0 leases`（应为在飞租约数）；
    本 run 的 lease id 轨迹在 failover 后**从 1536 跌回 1**（17:14 观测序列
    1,4,6,7,8,9,10,11），随后缓慢爬升——说明重建装载的 max_id 极小。
  - 第二次 failover：n2 于 17:51:46.937 成为 leader（n3 被 SIGSTOP），
    `rebuilt from state machine: 3 leases`；17:51:45 发起的 `ka/16692` 同在窗口内
    判 `:lease-not-expired`（keepalives 正常、停续期后 Key 未在窗口内消失）。
* **机制（待闭）：failover 路径的 LeaseManager 内存视图与状态机记录出现分叉。**
  已知的三个可疑面：
  1. `start_lease_leader_reconciler`（`server/mod.rs:960-984`）在「刚成为 leader」时
     用 `storage.list_lease_records()` 重建；本轮两次 failover 装载数（0 / 3）与
     在飞租约数不符；
  2. `LeaseManager::rebuild`（`lease/mod.rs:453`）确认在装载后 `NEXT_LEASE_ID.fetch_max`，
     但进程级静态分配器（`lease/mod.rs:72`）在新进程/新 leader 上没有全局屏障
     （Grant handler 注释自己承认「新主若尚未 apply 到前任已提交的 Grant」这一窗口）；
  3. KeepAlive 的 `NOT_FOUND` 只看 **本节点 LeaseManager 内存**
     （`server/mod.rs:2381-2390`），不看状态机记录 ⇒ 重建后的短窗口内出现
     「grant/put 均成功但 keepalive 立即 not found」的对外不自洽。
* **根因与修复（第十一轮，2026-09-26）**：
  - **根因（唯一且已闭环）**：`LeaseManager::rebuild` 旧实现「**先清空再装载**」。
    竞态序列：`lease_grant` 先在 `grant_with_id_checked` 插入本地记录、后入 raft
    日志；reconciler（每 500ms）读状态机快照（`list_lease_records`）若发生在该
    Grant 的 apply 之前 ⇒ 快照不含该租约 ⇒ `leases.clear()` 把本地记录（= 唯一
    的 TTL 调度依据）抹掉 ⇒ ①KeepAlive 查本地——`lease_mgr.get_lease` 命中 None
    ⇒ 立即 `NOT_FOUND`（grant/put 刚成功，对外不自洽）；②过期 worker 靠本地记录
    驱动（`check_expired`）⇒ 该租约**永不 revoke** ⇒ 绑定 Key 直到下一次 failover
    前**永不删除**。三个"可疑面"里 ②（分配器无全局屏障）在 C1 的 ReadIndex 屏障后
    只剩效率问题（逐 probe 查状态机），③（KeepAlive 只看本地）是本根因的放大器。
  - **修复（三处）**：①`rebuild` 改**只增不删的合并语义**（状态机 = 存在性权威，
    本地视图 = TTL 调度缓存；快照缺的本地记录一律保留；两视图都有取**更晚**
    deadline；分配器覆盖两视图最大 ID）；②reconciler 装载前加**线性一致屏障**
    （`ensure_linearizable_barrier`，追平提交位后再读，杜绝「已提交未 apply」漏读），
    失败下个 tick 重试（不再把「没装上」当「装好了」）；③KeepAlive 本地缺失时
    **以状态机为权威补水**（`resolve_keepalive_ttl`：屏障 + 读 `/_lease/{id}` +
    `LeaseManager::rehydrate`）——未过期 ⇒ 恢复跟踪并正常续期；已过期 ⇒ 记录以
    「立即到期」入本地（过期 worker 下一 tick Revoke ⇒ **泄漏自愈**）并回 NOT_FOUND；
    屏障/存储失败回可重试状态，**不得**伪装成 NOT_FOUND。
  - **判据（进程内，已绿）**：
    ①`cargo test -p coord-server --lib lease::` —— 三条 F-70 单测（在飞 Grant 不被抹
    + 到期仍上报；合并取更晚 deadline；rehydrate 自愈）；
    ②`cargo test -p coord-server --test lease_raft_test` —— 两条节点级判据
    （`test_f70_rebuild_race_keeps_inflight_grant_and_expiry_cascades`、
    `test_f70_keepalive_rehydrates_from_state_machine_and_heals_leaks`）。
    负控制：还原 `rebuild` 至「先清空再装载」⇒ 单测①与节点判据①红；去掉补水路径
    ⇒ 节点判据②红。
  - **状态：代码闭环（待 CI + 同参数 2h soak 复跑确认）**。
* **复现要点（下一轮第一优先）**：进程内 3 节点（沿用 F-69 判据的骨架）：
  1. 先建 K 个活跃租约（含 keepalive 流与 ttl=30 的 revoke 场景）；
  2. kill 当前 leader，等新 leader 上任；
  3. 断言（a）新 leader `list_lease_records()` 与实际在飞租约一致；
     （b）对旧 leader 期间创建的租约，keepalive 语义自洽（要么正常续期，要么
     明确失败且**不产生永不删除的 Key**）；（c）这些租约到期后绑定 Key 在
     ttl+grace 内被删。
  * 判据落点：`coord-server/tests/lease_failover_test.rs`（新建）；
    观测点：`rebuilt from state machine: N leases` 日志 + `results` 的
    `:lease-not-expired`。
* **影响面**：lease 级联删除契约在 failover 窗口失守（Key 泄漏直到该租约记录被
  其他路径清理）；W3-1 的「failover 下 lease 活性」判据红。**RC 冻结前必须闭环**。

### F-71 [P2，closed（checker fixture 实证）] leaseck 观测缺口：认证失败读计入 `:failed` 但不计入「未判」

* **证据**：`ttl/15232`（17:45:36）与 `ka/15242`（17:45:38）两条 `:lease-not-expired`
  的 `:observations` 均为 `{:absent? false, :reads 39, :failed 26/27}`——**读失败占多数**；
  同一秒段（17:45:39-49）客户端日志出现 **9 条 `CCT rejected as unauthenticated —
  refreshing session`**（跨 9 个 worker，各重认证一次后恢复）。
* **机制（已定位）**：CCT 的 TTL = **1h**（`coord-server/src/auth/service.rs:593`
  `let exp = now + 3600`）；run 起始认证的 token 在 ~65 min 后（17:45）集中过期，
  客户端在收到 `:unauthenticated` 时才懒重认证；`lease-wait-gone`（`client.clj:1025`）
  的轮询把失败读只计 `:failed`，**不区分「没读到」与「读到仍然存在」**，也不触发
  重认证 ⇒ 该窗口内的「消失」观测被吞掉，被判成活性违反（实为观测缺口）。
* **修法方向（二选一或同时）**：
  1. checker/工作负载侧：轮询循环对 `:unauthenticated` 触发一次会话刷新后重试
     （与 `invoke-lease-*` 的重认证等价）；记录 `:last-ok-at-ms` / `:last-ok-present?`
     供判定；「全部读失败」应计入 `:liveness-unjudged` 而不是违反（对标 F-26 的口径）。
  2. 客户端侧：CCT 接近到期（如剩 5 min）时**主动**重认证，消除集中过期波。
* **修复（第十一轮，2026-09-26）**：
  - 客户端（`client.clj`）：新增 `lease-read-with-reauth` —— 轮询中的
    `:unauthenticated` 读失败先刷新会话再重试一次；`lease-wait-gone` 改为记录
    `:ok-reads` / `:reauths` / `:last-ok-present?` / `:last-ok-at-ms`。
  - checker（`leaseck.clj`）：新增 `poll-unjudged?` —— `:ok-reads = 0`（轮询期间
    **从未读成功**）时，判据 3/3'/4 **不判**，计入 summary 的
    `:liveness-unjudged`（不判 ≠ 通过）；读成功过则照旧判定（防逃逸负控制：
    `expect-invalid-present-at-deadline-with-ok-reads.edn`）。旧格式观测（无
    `:ok-reads` 字段）按旧口径判，历史 fixture 语义不变。
  - 判据（已跑）：`jepsen.coord.leaseck` × `scripts/lease-fixtures` **12/12**、
    `lease-fixtures-sample` **2/2**（新增：`expect-valid-absence-observed-by-reauth`、
    `expect-valid-unjudged-all-poll-reads-failed`（旧 checker 下必红，
    `:liveness-unjudged 1` 为该规则的实证）、
    `expect-invalid-present-at-deadline-with-ok-reads`）。
  - **状态：closed（fixture 实证）；修法方向 2（客户端提前主动重认证）未实施 ——
    如需进一步消除集中过期波，可单独立项。**
* **判据落点**：`scripts/lease-fixtures`（若无则新建）：
  `expect-valid-absence-observed-by-reauth`（历史含一次 UNAUTHENTICATED 中断，Key 实际
  已删 ⇒ 必须判绿）；配对负控制：Key 确实未删 ⇒ 必须判红。

### 本轮 run 的门槛摘要（供 §11 引用）

* gates **valid**；rto-p95 0.295s、**rto-unrecovered 0**；quiet-windows 4 /
  judged 3 / **worst-ratio 1.0**；premise valid；map valid（2967 ops）、watch valid；
* linear **invalid**：`:violations-by-class {:lease-not-expired 4}`（见 F-70/F-71）。
* 与上一轮同参数 run（判 invalid、rto-unrecovered 2、quiet 0.0、85 min 全集群失能）
  对比：**F-69 的失能面已消失**；剩余红灯全部集中在 lease failover 窗口与认证观测缺口。
