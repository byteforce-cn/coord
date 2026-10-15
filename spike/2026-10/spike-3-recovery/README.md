# Spike 3 · Outbox + 崩溃恢复

> Owner: Spike 执行（AI 协作者）｜ Last verified: 2026-10-13
> 状态：✅ 初版完成（决策证据，**不是**生产代码；不并入 main）
> 分支：`spike/2026-10-spike3`
> 对应手册：《coord 工作流引擎 Spike 落地执行手册 v1.1》§4「Spike 3」；证据标准 §5；红线 §10

## 结论（Go / No-Go 输入）

| # | 手册问题 | 结论 | 关键证据 |
|:--|:--|:--|:--|
| 1 | 状态写入 + outbox 写入能否同一 KV Txn 原子完成？ | **能**（已落真实 Raft KV，含「要么都落、要么都不落」判据） | `OUTBOX.md` §2；`coord/tests/spike3_durable_kv.rs` |
| 2 | Relay 投递 + 标记 + 失败退避能否不丢（允许重复，靠 event_id 去重）？ | **能**（at-least-once；broker 幂等键 + 消费者 `event_id` 去重两道闸） | `OUTBOX.md` §4/§5；`evidence/*/events.tsv` |
| 3 | 事件契约（`com.china-workflow.*` / dataschema / per-instance sequence / partitionkey）能否落地？ | **能**（11 个类型各有 schema；序号单调；partitionkey = instance_id） | `EVENT-CONTRACT.md`；`schema/`；`spike/2026-10/spike-3-recovery/scripts/schema_check.py` |
| 4 | 持久化定时器 + 重启扫描能否让 wait/listen/retry 在杀进程后继续？ | **能**（三类均进程级 kill -9 实测通过） | `RECOVERY.md` §3；`evidence/kill9-*-20261013-121200.log` |

判定倾向：**这四块没有根本性障碍**（非 No-Go 输入）。存在的缺口都是「工程完备度」而非
「做不到」，逐条列在 `OUTBOX.md` §7、`RECOVERY.md` §7、`EVENT-CONTRACT.md` §7。

## 目录

| 文件 | 内容 |
|:--|:--|
| `OUTBOX.md` | KV key 布局、Txn 形状、记录结构、Relay 退避、投递面选择、TTL/保留口径、消费者去重契约 |
| `RECOVERY.md` | 持久化定时器布局与启动对账、listen 重订阅、MqEventProvider 缺陷复现与修复、kill -9 时间线、缺口 |
| `EVENT-CONTRACT.md` | `com.china-workflow.*` 命名空间映射、事件形态、不变量、schema 清单 |
| `schema/` | 每个事件类型一个 JSON Schema（`generate.py` 生成，`--check` 保证与清单一致） |
| `harness/` | 进程级 kill -9 工装（根工作区成员 `spike3-recovery-harness`） |
| `spike/2026-10/spike-3-recovery/scripts/repro.sh` | **一条命令**复现全部场景（起 server → 起 harness → kill -9 → 重启 → 断言） |
| `spike/2026-10/spike-3-recovery/scripts/schema_check.py` | 用 schema 校验证据里每条契约事件（fail-closed） |
| `evidence/` | 时间线日志 + outbox/事件/定时器流水 + summary + verdict |

## 复现（不依赖人工操作）

```bash
cd <repo root>

# 1) 进程级 kill -9（wait / listen / retry 三场景；含 outbox 补投）
#    默认会先 cargo build -p coord -p spike3-recovery-harness；复用产物用 SPIKE3_SKIP_BUILD=1
bash spike/2026-10/spike-3-recovery/scripts/repro.sh

# 2) 机制级（真实单节点 Raft/KV/Txn 上的原子性、relay、恢复）
CARGO_BUILD_JOBS=3 cargo test -p coord --test spike3_durable_kv -- --test-threads=1

# 3) MqEventProvider 缺陷复现 + 修复
CARGO_BUILD_JOBS=3 cargo test -p coord-agent --lib services::mq_event_provider

# 4) 契约 schema 校验（对 evidence 里的真实事件）
python3 spike/2026-10/spike-3-recovery/scripts/schema_check.py
```

临时文件一律在 `/tmp/coord-spike-spike3/`（AGENTS.md 硬性规定）；仓库内只放本目录的
证据与工装源码。

## 证据索引（`evidence/`）

2026-10-13 12:12:00 运行（`spike/2026-10/spike-3-recovery/scripts/repro.sh`，三个场景全 PASS）：

| 文件 | 内容 |
|:--|:--|
| `kill9-wait-20261013-121200.log` | wait 场景完整时间线（READY → KILL9 → RECOVER_SCAN → TIMERS_AT_RECOVER → Running → Completed → OUTBOX_DRAINED → SUMMARY） |
| `kill9-listen-20261013-121200.log` | listen 场景时间线（含 SUBSCRIBED / TRIGGER / EVENT_EMITTED） |
| `kill9-retry-20261013-121200.log` | retry 场景时间线（重试预算耗尽 → Failed） |
| `wait-20261013-121200/outbox.tsv` | wait 实例的 outbox 流水（id/aggregate/sequence/type/status/时间） |
| `wait-20261013-121200/events.tsv` | 消费者侧事件流水（offset/event_id/aggregate/sequence/type/duplicate） |
| `wait-20261013-121200/timers.tsv` | 定时器残留（触发后为空） |
| `wait-20261013-121200/summary.json` | 判定事实（final_status / outbox_pending / missing_event_ids / duplicate_event_ids） |
| `wait-20261013-121200/outbox.jsonl` / `events.jsonl` | 原始投递载荷（`spike/2026-10/spike-3-recovery/scripts/schema_check.py` 的输入） |
| `listen-20261013-121200/`、`retry-20261013-121200/` | 同上，按场景分目录 |
| `verdict.tsv`、`verdict-20261013-121200.tsv` | 机器可读判定（case / status / 四个计数 / 日志路径） |

集成修复后复跑（2026-10-13 12:56:45，`spike/2026-10-workflow-v2` 合并 Spike 2/3/5 并修复 emit 实例写通道之后）：

| 文件 | 内容 |
|:--|:--|
| `verdict.tsv`（当前指向） | wait / listen / retry 三场景全 PASS（修复后） |
| `kill9-{wait,listen,retry}-20261013-125645.log` + `{wait,listen,retry}-20261013-125645/` | 三场景完整时间线与流水 |
| `verdict-20261013-122925.tsv` + `kill9-retry-20261013-122925.log` + `retry-20261013-122925/` | 修复前首跑捕获的观测窗口：终态已 `failed`、单次 `pending==0` 之后 summary 仍见 `workflow.faulted.v1` 为 `Pending`。该次失败不是事件丢失，而是「状态写与伴生事件 append 分属两次写」；harness 排空判定已改为连续 4 轮（间隔 500ms）稳定观测，且 emit 已改为 append-only（不再写实例状态，回归测试 `durable_emit_appends_without_writing_instance_state`）。同事务集成要求见 `OUTBOX.md` §2.1 与缺口 G1。 |

时间线读法示例（wait）：

```text
     860 READY          instance=d301895b-... status=Some(Waiting) timer=0000...2690:...:resume:wait
       0 KILL9          pid=226051 signal=SIGKILL wait_status=137 ts=2026-10-13T12:00:08.130Z
     418 RECOVER_SCAN   durable recovery completed
     461 TIMERS_AT_RECOVER 0000...2690:...:resume:wait@1791892812690
    4970 STATUS         status=Some(Running)      ← 持久化定时器触发（进程内定时器已随 SIGKILL 消失）
    5799 STATUS         status=Some(Completed)
```

## 实现落点（本分支新增/改动）

新增：

- `coord-core/src/workflow/durable.rs` —— outbox/timer 契约、退避、relay、内存实现、事件命名空间映射；
- `coord-agent/src/services/outbox_kv.rs` —— KV/Txn 实现（`OutboxStore` / `DurableTimerStore` / `AtomicOutboxCommit` / `KvCursorStore`）；
- `coord-agent/src/services/outbox_relay.rs` —— MQ 投递面（`MqOutboxSink`）+ relay 循环；
- `coord/tests/spike3_durable_kv.rs` —— 机制级集成测试（6 条）；
- `spike/2026-10/spike-3-recovery/` —— 本文档集、harness、脚本、schema、证据。

改动（最小面）：

- `coord-core/src/workflow/runtime.rs` —— `with_durable` builder、持久化定时器写入、
  启动恢复与到期扫描、listen 订阅抽取、生命周期事件走 outbox；
- `coord-agent/src/services/workflow.rs` —— `DurableEngineOptions` + `new_with_kv_store_durable`
  + `start()` 接线（既有构造签名不变）；
- `coord-agent/src/services/mq_event_provider.rs` —— 按订阅寻址的持久化游标（修两处缺陷）；
- `coord-agent/src/services/workflow_store.rs` —— `instance_key` 提升为 `pub(crate)`（复用键布局）；
- 根 `Cargo.toml` —— 登记 `spike/2026-10/spike-3-recovery/harness` 工作区成员。

## 边界与红线遵守

- 只在 `spike/2026-10-spike3` 提交；**不 push、不碰 main**。
- 产出物是决策证据：不新增对外 RPC/字段，不改 `coord.workflow.v1`，不改 `apis/contracts/`。
- 每条结论都能追到「测试用例 + 日志」；未完成项逐条登记（见各文档 §7 缺口）。
