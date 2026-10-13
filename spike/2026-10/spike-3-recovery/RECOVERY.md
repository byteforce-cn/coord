# Spike 3 · 崩溃恢复方案（持久化定时器 + 重启对账 + wait/listen/retry）

> Owner: Spike 执行（AI 协作者）｜ Last verified: 2026-10-13
> 状态：初版完成（决策证据，不是生产契约）
> 回答手册 §4 Spike 3 的问题 4（杀进程后 wait/listen/retry 是否继续）
> 关键实现：`coord-core/src/workflow/runtime.rs`（`with_durable` / `recover_durable_state` /
> `fire_timer` / `reconcile_resumable_instances`）、`coord-agent/src/services/outbox_kv.rs`

## 0. 结论

| 问题 | 判定 | 证据 |
|:--|:--|:--|
| 杀进程后挂起实例全部恢复 | **是**（wait / listen / retry 三类都实测；见 §3 时间线） | `evidence/kill9-{wait,listen,retry}-20261013-121200.log` |
| 事件不丢不重 | **是**（outbox 已投递集合 = 消费者去重后集合；`duplicate_event_ids = 0`） | 各场景 `summary.json` + `events.tsv` |
| 定时器持久化 | **是**（到期项在 KV，重启后扫描触发；触发后移除） | `evidence/*/timers.tsv`、日志 `TIMERS_AT_RECOVER` 行 |

## 1. 为什么需要它：现状的两处「进程即真相」

1. **进程内 tokio 定时器**：`schedule_auto_resume`/`schedule_timeout` 只 `tokio::spawn`
   一个 `sleep`（`coord-core/src/workflow/runtime.rs:1073`、`:1195` 的历史形态）。进程一死，
   wait/retry 到期与 timeout 全部消失，实例永远停在 KV 里的 `Waiting`。
2. **启动对账只管子流程**：`reconcile_suspended_subflows` 只恢复
   `context["_subflow_instance_id"]` 的父子对；普通 `Waiting/Suspended` 不会重新驱动；
   `listen` 的订阅随进程消失，实例永远等不到事件。

## 2. 机制

### 2.1 持久化定时器 key 布局

```text
/_workflow/v3/timers/{fire_at_ms:020}:{instance_id}:resume:{reason}   # wait / retry
/_workflow/v3/timers/{fire_at_ms:020}:{instance_id}:timeout:{idx|wf}   # 任务级 / 工作流级超时
```

- 第一段**零填充 20 位到期时间** ⇒ 字节字典序 = 到期顺序 ⇒ `due(now)` 就是一次前缀
  range，取到第一个 `fire_at > now` 即可停；
- 主键不变量：**AutoResume 定时器的 `fire_at_ms` 必须等于 `suspension_meta.until_ms`**。
  这条不变量是「陈旧定时器」判据的基础（§4 的缺陷修复）。

### 2.2 调度写入

- `schedule_auto_resume(instance_id, definition, store, **due_ms**, reason)`：`due_ms` 是
  **写进 `suspension_meta.until_ms` 的同一个值**；进程内 `sleep(until - now)` 保留为快速路径，
  同时把 `TimerRecord` 写 KV（`fire_at_ms = due_ms`）。
- `schedule_timeout(...)`：同理写 `TimerKind::Timeout` 定时器。
- 注入点是 builder：`WorkflowRuntime::with_durable(DurableSupport { timers, outbox, atomic_commit })`
  （`runtime.rs:341`）。**不改** `WorkflowRuntime::new` 的签名 ⇒ 内存路径行为逐字不变。

### 2.3 重启扫描

`WorkflowRuntime::start_durable_workers()`（`runtime.rs:384`）起一个 200ms tick 的循环
worker（登记在 `WorkerLiveness`，可观测），每 tick `fire_due_timers()`。`WorkflowEngineService::start()`
（`coord-agent/src/services/workflow.rs:2639`）在 `KvWorkflowStore::init()` 之后调用：

```text
recover_durable_state()           # 一次性
  ├─ fire_due_timers()            # 1) 先触发已到期定时器
  └─ reconcile_resumable_instances()
       ├─ Waiting(reason=listen) → spawn_event_wait 重新订阅（事件到达 → resume_by_event）
       ├─ Waiting(reason=wait|retry) → 按 until_ms 补写定时器（同键，幂等）
       └─ 其它（signal / 子流程）→ 不自动驱动（人工恢复 / 子流程扫描器负责）
  └─ fire_due_timers()            # 2) 补写的定时器可能已经到期（崩溃窗口），再触发一轮
```

随后 `start_durable_workers()` 持续扫描。两条驱动路径都是幂等的：`auto_resume` 有
状态 + reason 守卫，`apply_timeout` 有终态/任务索引守卫。

### 2.4 listen 重订阅

`listen` 的订阅原先是 `drive()` 里内联的 `tokio::spawn`，恢复路径无法复用。现在提取为
`spawn_event_wait(instance_id, definition, store, filter, timeout_ms)`（`runtime.rs:415`），
`drive` 与 `reconcile_resumable_instances` 共用同一入口 ⇒ 「运行期订阅」与「重启后重订阅」
不可能行为分叉。事件过滤器从 `suspension_meta.event_filter` 还原。

### 2.5 与 Spike 2「StepToken」的关系（交叉引用）

Spike 2 的结论是「副作用去重需要一个跨进程的唯一令牌」，候选是 CAS key 与 KV `request_id`；
`NEXT-SPIKES-NOTES.md` 已指明 `request_id` 是单节点/进程内/60s/不复制，**不能**当令牌基础。

本 Spike 给出的是**另一条正交的令牌**：`OutboxRecord.id`（事件 ID）与其
`(aggregate_id, sequence)`。它们的语义边界必须分清：

| 令牌 | 保证 | 不保证 |
|:--|:--|:--|
| `OutboxRecord.id` / `sequence` | 事件**发布**去重与有序 | 任务**副作用**去重 |
| Spike 2 的 StepToken | 任务**副作用**去重 | 事件发布去重 |

两者都必要：事件不丢不重解决「下游不知道发生了什么」，StepToken 解决「同一任务执行了两次
不会重复扣款」。`StepStarted/StepCompleted` 若要成为**任务级**幂等凭据，其存放点应与
实例状态同事务，可直接复用本 Spike 的 `AtomicOutboxCommit` Txn 形状（把 StepToken 记录
当作第二个 `put`）。本 Spike 未实现 StepToken —— 归 Spike 2。

## 3. 进程级 kill -9 证据

复现脚本：`spike/2026-10/spike-3-recovery/scripts/repro.sh`（一条命令；真实 `coord server` 单节点 bootstrap + 固定
data-dir + 真实 harness 二进制；`kill -9` 的是 harness，server 保持运行 —— 即
**只杀引擎进程，不杀存储**，这样测到的是「引擎恢复」而不是「Raft 重启」）。
Raft 进程级 kill -9 另有 `coord/tests/m0_recovery_suite.rs` 覆盖，本 Spike 不重复。

### 3.1 wait（`evidence/kill9-wait-20261013-121200.log`）

```text
       0 BOOT           mode=start scenario=wait relay=false
     445 STARTED        instance=d301895b-...
     860 READY          instance=d301895b-... status=Some(Waiting) timer=0000...2690:...:resume:wait
       0 KILL9          pid=226051 signal=SIGKILL wait_status=137 ts=2026-10-13T12:00:08.130Z
       0 BOOT           mode=recover scenario=wait relay=true
     418 RECOVER_SCAN   durable recovery completed
     461 TIMERS_AT_RECOVER 0000...2690:...:resume:wait@1791892812690
     502 STATUS         status=Some(Waiting) reason=wait
    4970 STATUS         status=Some(Running)      ← 到期定时器触发，实例被重新驱动
    5799 STATUS         status=Some(Completed)
    6514 OUTBOX_DRAINED pending=0
    6599 SUMMARY  {"final_status":"completed","outbox_pending":0,"missing_event_ids":0,"duplicate_event_ids":0,...}
```

关键读法：`READY` 行在 kill **之前**就带出了持久化定时器键 —— 杀进程前状态与定时器都已
落 Raft KV；恢复进程在 `recover+4970ms` 从 `Waiting` 进入 `Running`，与 PT5S 的到期点吻合，
而**进程内定时器已随 SIGKILL 消失** ⇒ 驱动它只可能是持久化定时器扫描。

### 3.2 listen（`evidence/kill9-listen-20261013-121200.log`）

```text
       0 BOOT           mode=start scenario=listen relay=false
     390 READY          instance=... status=Some(Waiting) timer=none
       0 KILL9          pid=... signal=SIGKILL ts=2026-10-13T12:00:16.xxxZ
       0 BOOT           mode=recover scenario=listen relay=true
     143 RECOVER_SCAN   durable recovery completed
     143 SUBSCRIBED     instance=...            ← 重订阅完成（恢复路径）
       0 TRIGGER        event source fires     ← 事件发生在**重启之后**
     246 EVENT_EMITTED  type=com.china-workflow.spike3.go-ahead
     247 STATUS         status=Some(Waiting)
     340 STATUS         status=Some(Running)
    2046 STATUS         status=Some(Completed)
   3967 OUTBOX_DRAINED pending=0
```

### 3.3 retry（`evidence/kill9-retry-20261013-121200.log`）

```text
       0 BOOT           mode=start scenario=retry relay=false
     406 READY          instance=... status=Some(Waiting) timer=...:resume:retry
       0 KILL9          pid=... SIGKILL
     262 TIMERS_AT_RECOVER 0000...1666:...:resume:retry@1791892821666   ← 恰好一条
     304 STATUS         status=Some(Waiting) reason=retry retry_count=1
    9958 STATUS         status=Some(Failed)  reason=retry retry_count=2  ← 重试预算耗尽
   10097 SUMMARY  {"final_status":"failed","outbox_pending":0,"missing_event_ids":0,"duplicate_event_ids":0,"remaining_timers_for_instance":0}
```

`final_status = failed` 是**预期**：重试端点 `http://127.0.0.1:1/never` 永远不可达，重启后
重试链路继续跑到预算耗尽（fault 文案逐字：「call to 'http' failed after 2 retries」），
没有人工 signal 介入 —— 这就是「retry 在重启后继续」的判据。

### 3.4 不丢不重的判据（三场景共有）

`summary.json` 的四个数一起看：

- `outbox_pending = 0`：没有停在 KV 里没投的事件；
- `missing_event_ids = 0`：outbox 里标 `delivered` 的每条都出现在消费者侧（按 `event_id` 匹配）；
- `duplicate_event_ids = 0`：消费者按 `event_id` 去重后无重复；
- `final_status` 符合预期（completed / completed / failed）。

`events.tsv` 的 `sequence` 列在 wait 场景是 1..6 连续 —— per-instance 序号单调、无空洞。

## 4. 实测发现的两个缺陷 + 修复

两者都是**先跑出证据、再修**。`evidence/` 里的文件是修复**之后**的最终产物；
修复前的实测片段逐字引用在下面（引用而非文件引用：旧证据目录已被最终产物取代）。

### 4.1 同一个挂起被写了两条定时器（键不同 ⇒ 触发两次）

修复前实测（retry 场景，`TIMERS_AT_RECOVER` 逐字）：

```text
     263 TIMERS_AT_RECOVER 00000001791892641352:9a12fd01-...:resume:retry@1791892641352,00000001791892641543:9a12fd01-...:resume:retry@1791892641543
     568 STATUS         status=Some(Waiting) reason=retry retry_count=1 until_ms=1791892641352
    7166 STATUS         status=Some(Failed)  reason=retry retry_count=2 until_ms=1791892649425
```

两个键的唯一差别是到期时间（1352 vs 1543，差 191ms）⇒ 同一挂起被写了两条定时器、
触发两次；观测到的后果是重试轮次比配置少一轮就耗尽。

**注意**：这次跑完 `verdict.tsv` 仍然写的是 `PASS`（final 状态与事件不丢不重都对）。
也就是说这个缺陷**不会被 §3.4 的四项判据抓住** —— 是 `TIMERS_AT_RECOVER` 这条
观测行暴露的。教训：判据要覆盖「轮次/次数」而不只是「终态 + 计数」。

根因：定时器主键含到期时间，而到期时间被算了**两次**：

```rust
let until = self.clock.now_ms() + delay;      // ① 写进 suspension_meta.until_ms
...save_instance(...).await;                   //    中间隔了 KV 往返 + 原子 Txn
self.schedule_auto_resume(..., delay, ...);    // ② 内部再算 now + delay（差 ~190ms）
```

两次 `now_ms()` 之间的存储往返（`save_instance` + `emit_lifecycle` 的原子 Txn）
让 ① 与 ② 差了几十到几百毫秒 ⇒ 两个不同的 key，恢复对账又按
`suspension_meta.until_ms` 写一条 ⇒ 同一挂起被触发两次。

修复：`schedule_auto_resume` 改为接收 **`due_ms`**（`runtime.rs:1073`），调用方统一传
① 的 `until`（wait 传 `until_ms`、retry 传 `until`）⇒ 定时器键与
`suspension_meta.until_ms` 恒等，恢复对账写的是**同一个键**（覆盖而非新增）。

### 4.2 陈旧定时器仍能驱动实例

即便键一致，也可能存在「对应上一个挂起」的定时器（人工改期、时钟回拨、历史遗留键）。
`auto_resume(reason="retry")` 没有版本守卫：它直接 `drive` 重跑当前任务，所以一条陈旧
定时器就能多吃一轮重试预算。

修复：`fire_timer` 的 AutoResume 分支加**陈旧守卫**（`runtime.rs:490`）：

```rust
let current_until = inst.suspension_meta.as_ref().and_then(|m| m.until_ms);
let still = inst.status == InstanceStatus::Waiting
    && inst.suspension_meta...reason == reason
    && current_until == Some(rec.fire_at_ms);   // ← 定时器必须对应「当前」挂起
```

判据用 `fire_at_ms == until_ms` 而不是「`until_ms` 是否已过」：后者依赖时钟，
在 `TestClock`（固定/推进可控）下会让 wait 的**正常**恢复被守卫拦住
（回归测试 `workflow::runtime::tests::test_wait_task_waiting_phase_auto_resume` 抓到过一次）。

## 5. MqEventProvider 两处缺陷：复现 + 处置

`coord-agent/src/services/mq_event_provider.rs` 旧实现用**一个** `Mutex<u64>` 当全 provider
的消费游标。`NEXT-SPIKES-NOTES.md` 指出的两处硬伤，本 Spike 都写了**可复现的缺陷测试**
（断言的是缺陷症状本身，跑在保留的旧逻辑复刻上）并给出修复：

| 缺陷 | 复现测试（断言缺陷发生） | 修复后断言 |
|:--|:--|:--|
| 共享单游标偷事件（不同订阅共用一个游标） | `defect_repro_shared_cursor_steals_events` | `fixed_provider_subscriptions_do_not_steal_events` |
| offset 重启归零重放已消费事件 | `defect_repro_offset_resets_on_restart` | `fixed_provider_cursor_survives_restart` |

复现测试用的 `LegacySharedCursorProvider` 就是旧逻辑的逐字复刻（保留在测试模块里），
所以「旧实现确实会丢事件/会重放」是机器可验证的事实，不是叙述。

**修复**：游标按**订阅**寻址（`subscription_key(topic, types, source, subject)` 的
FNV-1a 哈希）+ 可注入 `CursorStore`：

- `MemoryCursorStore`（默认，单进程）；
- `KvCursorStore`（`/_workflow/v3/mq-offsets/{subscription}`）—— 引擎的 durable 装配用
  它，游标随 Raft KV 存活，重启从上次位点续读。

修复后的语义（逐字）：**匹配到事件先提交游标再返回** ⇒ 同一订阅不重放（至多一次消费）。

复现命令：

```bash
CARGO_BUILD_JOBS=3 cargo test -p coord-agent --lib services::mq_event_provider
# 8 passed：3 条既有行为 + 2 条缺陷复现 + 2 条修复断言 + 1 条订阅键稳定性
```

### 5.1 残余缺口（接线前必须知道）

**listen 的崩溃窗口**：`wait_for_event` 返回（游标已提交）到
`resume_by_event` 把「已恢复」写进实例状态之间，进程若死 ⇒ 该订阅不会再收到这条事件，
实例会等到 listen 超时（若配了 timeout）或永远等待。

- 根因：事件游标与实例状态**不在同一事务**。这与 OUTBOX.md §5.1 的 outbox 原子性
  是同一类问题，解法也相同：把「消费位点推进」与「实例状态变更」放进一个 Txn
  （或引入 per-instance 事件台账按 `event_id` 去重后再推进位点）。
- 现状：**未修**。手册 §4 的失败应对要求「恢复不完整必须记录具体场景」，
  以上即该场景；阶段二把它列为 listen 语义的前置项。
- 影响面：仅 `listen`（wait/retry 走定时器，没有这个窗口）。

## 6. 复现命令汇总

```bash
# 进程级 kill -9（3 场景；输出 verdict + 证据）
bash spike/2026-10/spike-3-recovery/scripts/repro.sh

# 机制级（真实单节点 Raft 上跑恢复：wait 覆盖状态 → 新引擎恢复；listen 重订阅）
CARGO_BUILD_JOBS=3 cargo test -p coord --test spike3_durable_kv -- --test-threads=1

# MqEventProvider 缺陷复现 + 修复
CARGO_BUILD_JOBS=3 cargo test -p coord-agent --lib services::mq_event_provider
```

## 7. 缺口汇总（阶段二输入）

- **R1（listen 崩溃窗口）**：见 §5.1，未修。
- **R2（`Suspended` 不自动恢复）**：`signal` 挂起是人工语义，重启不做任何动作
  （设计如此）。若审批场景要求「重启后提醒挂起实例」，需要单独的提醒机制，不在本方案内。
- **R3（对账是全量列举）**：`reconcile_resumable_instances` 启动时
  `list_instances(None, None, usize::MAX)` 全量列举一次（页大小 `usize::MAX`）。
  实例规模大时需要改成按状态索引/分页；这是**启动一次**的代价，不在 tick 路径上。
- **R4（relay 与恢复共用进程）**：恢复扫描与 relay 都在 `start()` 里启动，没有就绪屏障；
  恢复未完成时 relay 已可能投递旧事件。当前无害（事件顺序由 per-instance 序号给出），
  但阶段二应给出「先恢复、后投递」的显式顺序或就绪门。
