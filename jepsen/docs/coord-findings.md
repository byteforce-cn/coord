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
| F-05 | 60s 短跑即可见 `:fail :write [:no-client Failed to authenticate to coord]`（鉴权/登录限流） | P1 | `confirmed-by-run` | §5.4-④ / E4 |
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
