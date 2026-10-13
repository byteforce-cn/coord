# Spike 2 / 3 / 5 接续手册（10-31 检查点）

> Owner: Spike 执行（AI 协作者）｜ Last verified: 2026-10-13
> 本页把三项未启动的验证收敛成「第一步就能跑」的接续说明；锚点均已在本仓核实。
> 前置输入（阈值/需求清单）未到位时按 `CHARTER-INPUTS.md` 的临时口径执行并标注。

## Spike 2 · 单写者并发演练（截止 10-31）

> **已执行（10-13）：见 `spike-2-concurrency/`。** 真实单节点 Raft 上 7/7 全绿
> （CAS 写路径、实例租约、fencing 双 compare、引擎级单效果 vs 无租约重复）；
> 结论、残余风险（fencing 不保护外部副作用、未接线的写回点）与复现命令见该目录
> `README.md` / `DESIGN.md` / `STEPTOKEN.md`。本小节以下的“第一步”保留为历史接续记录。

**现状锚点（已核实）**

- 引擎每步用普通 Put 写回；`save_instance_atomic`（mod_revision CAS）除 trait/实现外
  **没有任何调用点**：`coord-core/src/workflow/ports.rs:393`、
  `coord-agent/src/services/workflow_store.rs:84,736`。
- KV `request_id` 去重为单节点、进程内、60s、不复制（`apis/contracts/proto/coord/kv/kv.proto`），
  不能当 StepToken 基础。
- Lease 无 fencing token（`docs/production/consumer-contract.md` §3）。

**第一步（可复现）**

1. 起单节点 Raft + KV/Lease/Txn/Watch（`coord/tests/cluster_test.rs` 已给出
   `CoordRaft::new_basic_node` + 各 server 的装配范例；`coord/tests/common/mod.rs`
   有 `start_test_server()`/`wait_until()`）。
2. 写冻结用例：两个 agent 并发对同一实例 `save_instance_atomic`（同一 expected
   mod_revision）→ 恰一个成功、一个 `StaleRevision` 类错误。
3. 在引擎驱动循环里把 `save_instance` 换成 CAS 写，并加实例租约（Lease + 本地
   fencing 计数）；注入并发 signal 验证不重复副作用。
4. 产出：`spike-2-concurrency/`（方案、注入脚本、时间线日志、StepToken 结论）。

**红线**：Lease 无 fencing 时**不得**宣称安全；记录为缺口并评估扩展 Lease 契约。

## Spike 3 · Outbox + 恢复（截止 10-31）—— **已执行**（10-13 初版完成）

> 产出物：`spike-3-recovery/`（`OUTBOX.md` / `RECOVERY.md` / `EVENT-CONTRACT.md` +
> `schema/` + `harness/` + `spike/2026-10/spike-3-recovery/scripts/repro.sh` + `evidence/`）。以下为本节原定的
> 现状锚点与第一步，保留作为对照；实际结果与缺口见各产出物 §7。

**现状锚点（已核实）**

- 状态保存与事件发布是两次独立调用；事件走进程内 `MemoryEventProvider`
  （容量 256、非持久），且**两个构造路径**（内存/KV）都经
  `with_dispatcher` 硬编码注入：`coord-agent/src/services/workflow.rs:2060-2065`。
- `MqEventProvider` 无任何装配点（除自身模块）。
- wait/retry/listen/任务超时均为进程内 tokio 定时器：
  `coord-core/src/workflow/runtime.rs:583,815,927,1220,1344`。
- 启动对账只恢复父子子流程（`reconcile_suspended_subflows`），普通
  Waiting/Suspended 不会被重新驱动。

**第一步（可复现）**

1. 定义 outbox key 布局（同实例前缀 + 时间序），状态写入与 outbox 写入合并为
   一个 KV Txn（用 `coord/txn/txn.proto` 的 CAS/原子分支）。
2. Relay：轮询 outbox → 投递 → 标位；失败指数退避；用
   `docs/production/consumer-contract.md` §5 的 Poll+Ack 正确性路径。
3. 持久化定时器：把 `schedule_auto_resume/schedule_timeout` 的到期时间落 KV，
   重启扫描到期项。
4. 杀进程注入：wait/listen/retry 恢复；对账事件不丢不重；事件契约按
   `com.china-workflow.*` + per-instance sequence + partitionkey 草案输出。

**注意**：`MqEventProvider` 现有两处缺陷（offset 重启归零、共享单游标）在接线前
必须先修，否则重启会重放/偷事件。

**执行结果（10-13）**

- 两处缺陷已写好**可复现的缺陷测试**（断言缺陷症状）+ 已修复（按订阅寻址的游标 +
  可选 KV 持久化），测试见 `coord-agent/src/services/mq_event_provider.rs`。
- kill -9 三场景（wait/listen/retry）均 PASS，进程内定时器消失后由持久化定时器
  恢复；事件不丢不重（`missing_event_ids = 0`、`duplicate_event_ids = 0`）。
- 遗留缺口（待阶段二）：listen 的「游标推进与实例状态不同事务」崩溃窗口；
  outbox 无 TTL/清理；`drive()` 的状态保存点尚未全部折叠进同事务。
- 新工装：`spike/2026-10/spike-3-recovery/harness` 已登记为根工作区成员。

## Spike 5 · 基准测试（平台架构组主责，10-18 阈值定稿）

**状态：已执行（2026-10-13 初版）。** 产出见 `../spike-5-bench/`（`REPORT.md` +
逐条原始数据 `evidence/raw/` + `coord/tests/spike5_bench.rs` +
`spike/2026-10/spike-5-bench/scripts/repro.sh`）。
结论：Txn 上限 128、单值上限 ≈4 MiB（gRPC 消息）、Watch 1000 路 0 丢失 P99 16.6 ms、
存储 2819.5 B/写（单写者 380 写/s）、KV 安静机成功吞吐 636–724 ops/s（P50 134–1331 ms）。
canonical 为安静机复跑（loadavg 1.5–2.5）；共享负载首跑（loadavg 5–8，并发 ≥500
全部 5s 超时）保留在 `evidence/raw/aux-shared-load/` 作负载敏感性证据。
**非目标硬件 + dev profile；不做达标判定，目标硬件/release 口径待复测。**

**现状锚点（已核实）**

- 已有基准入口：`coord/tests/perf_bench.rs`、`coord-core/tests/cct_perf_bench.rs`；
  Jepsen 目录 `jepsen/`。
- 目标硬件定义缺失（`CHARTER-INPUTS.md` §3.3）：当前只能在测试机（8C/31G）采原始
  数据，并标注「非目标硬件判定」。

**第一步**

1. 复用 `cluster_test.rs` 装配单节点（后续多节点）Raft；KV 写延迟/吞吐按
   并发 100/500/1000 分档、原始数据全量落盘（非汇总）。
2. Txn 操作数 / 单值大小上限：阶梯递增记录失败点。
3. Watch：建 1000 连接测端到端延迟；输出瓶颈与容量规划。
4. 数据入 `spike-5-bench/evidence/`，达标判定等 10-18 阈值。

## 一句话优先级

Spike 2 与 Spike 3 共享 KV Txn/Lease 工装：先做 Spike 2 的 CAS 写路径，Spike 3 的
outbox 直接复用同一 Txn 抽象；两者都不应改动 main，全部落在 spike 分支。
