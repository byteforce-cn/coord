# 未接线机制复核表（W1-5）

- **日期**：2026-09-21
- **锚点**：`docs/production/production-readiness-plan-2026-09-21.md` §5 W1-5、
  第四轮 §3.13（「机制齐备，但没接线」）
- **判据**：`grep` 每个机制名，人工核对**生产调用点**；下表「生产调用点」列
  = 0 即仍未接线。**本表不含测试调用**（`#[cfg(test)]` 内的调用不算消费者）。
- **本轮结论**：第四轮 §3.13 列出的 10 项里 **7 项已接线 / 已删除 / 已明确为死边界**，
  本轮又接上 1 项（`cleanup_idle`）。剩余 1 项（`remove_region_*`）走「显式声明不支持」。
- **第二轮补充（W5-4）**：§2 的「长活任务清单」不再只是台账 —— 工作流侧的两个
  长活形态（子流程扫描器 / 每实例 `drive`）已**接上出口**（指标 + 告警 + ERROR 日志）。

---

## §1 逐条状态（2026-09-21 复核）

| 机制 | 位置 | 第四轮状态 | **本轮实测** | 处置 |
|:--|:--|:--|:--|:--|
| `_prune()` 登录限流清理 | `coord-server/src/auth/service.rs` | 0 调用 | ✅ 已接线：`AuthService::start_maintenance_worker`（`spawn_supervised("auth_maintenance")`），60s tick | 闭环 |
| `cleanup_expired()` 会话清理 | 同上 | 0 调用 | ✅ 已接线：同一 tick 走 `AuthOp::ConsumeSessions`（raft 持久化删除） | 闭环 |
| `cleanup_idle()` 连接池回收 | `coord-client/src/pool.rs` | 0 调用 | 🟡 **本轮接上**：改为「访问时机会式清理」`maybe_sweep()`（挂在 `get_from` 上，窗口 = `idle_timeout`，最小节流 60s） | **本次修复**（负控制测试两条：`test_maybe_sweep_throttles_then_fires_when_due`、`test_maybe_sweep_zero_idle_timeout_uses_min_interval`） |
| `remove_region_raft()` | `coord-server/src/raft/network.rs:1085` | 0 调用 | ❌ 仍 0（含测试） | **声明不支持动态增删 region**（见 `boundaries.md` §3）；函数保留但不接线 |
| `remove_region_runtime()` | `coord-server/src/raft/region.rs:458` | 0 调用 | ❌ 仍 0（含测试） | 同上 |
| `expire_receiver()` 时间轮过期消费 | `coord-server/src/timer/mod.rs` | 0（仅测试） | ✅ 已删除：channel 移除，过期由 `LeaseManager::check_expired()` 驱动（`timer/mod.rs:14` 有记） | 闭环（删除也是收敛） |
| `dead_tasks()` / `has_dead_tasks()` | `coord-server/src/supervisor.rs` | 0 调用（唯一监督出口无消费者） | ✅ 已接线：`metrics.rs:818` 暴露为 `coord_dead_background_tasks` + `health.rs:343`（`/health?verbose=true`），并配了 `CoordBackgroundTaskDead` 告警 | 闭环 |
| `AgentThreadPools::shutdown()` | `coord-agent/src/threadpool.rs` | 0 调用 | ✅ 已删除（全仓 `grep` 无该符号） | 闭环 |
| `soft_ttl` | — | 全仓 0 引用 | ✅ 已删除（全仓无该符号） | 闭环 |
| 离线检测 | `coord-agent` | 被自身心跳刷新关掉 | 未复核（本轮未触及 agent 心跳路径） | **待办：W3 相关**（属 agent 面，列入 §3） |

---

## §2 监督覆盖率（`spawn_supervised` vs `tokio::spawn`）

**判据**：`tokio::spawn` 到底有多少处**不受监督**（即任务意外死亡时没有任何路径知道）。

| 口径 | 数量 | 说明 |
|:--|--:|:--|
| `spawn_supervised` 生产调用点 | **6** | `auth_maintenance`、`timer_wheel`、`write_batcher`、`snapshot_scheduler`、PD 执行器 ×2（`coord/src/main.rs:3191,3210`） |
| `tokio::spawn`（生产代码，排除 `mod tests` 之后） | **110** | 见下分布 |

**最高密度文件（生产代码，前 6）**：

| 文件 | `tokio::spawn` 数 | 是否都该受监督 |
|:--|--:|:--|
| `coord/src/main.rs` | 10 | 部分（装配期一次性任务不必） |
| `coord-core/src/workflow/runtime.rs` | 8 | **是**（`drive` 长活任务；死亡＝实例永久挂在 Running） |
| `coord-server/src/server/mod.rs` | 6 | 部分（per-RPC 短任务不必） |
| `coord-client/src/client.rs` | 3 | 部分 |
| `coord-agent/src/lib.rs` | 3 | 部分 |
| `coord-agent/src/services/registry.rs` | 3 | 部分 |

**结论（诚实版）**：覆盖率 `6/110` 这个数字**不能直接解读成"104 个能力会静默死亡"**——
其中相当一部分是 per-RPC 短任务（请求结束即结束，死亡＝请求失败，客户端可见）。
真正需要监督的是**长活后台任务**（会话/租约/快照/GC/PD/时间轮/工作流驱动）。

**本轮的处置**：把判据从「总数对比」升级成「**长活任务清单 + 是否受监督 + 是否可观测**」。
清单见 §3。**未完成项如实列出**，不虚报。

---

## §3 长活后台任务清单（判据升级后的真表）

| 任务 | 位置 | 受监督 | 死亡可观测 | 处置 |
|:--|:--|:--:|:--:|:--|
| `auth_maintenance` | `coord-server/src/auth/service.rs:532` | ✅ | ✅（`coord_dead_background_tasks`） | 闭环 |
| `timer_wheel` | `coord-server/src/timer/mod.rs:139` | ✅ | ✅ | 闭环 |
| `write_batcher` | `coord-server/src/storage/write_batcher.rs:150` | ✅（`_with_shutdown`） | ✅ | 闭环 |
| `snapshot_scheduler` | `coord-server/src/storage/snapshot_scheduler.rs:90` | ✅ | ✅ | 闭环 |
| PD 执行器 ×2 | `coord/src/main.rs:3191,3210` | ✅ | ✅ | 闭环 |
| 租约过 revoke worker | `coord-server/src/server/mod.rs`（`start_lease_expiry_worker`） | ❌ | 🟡（有 WARN + `pending` 集合指标，见 W1-1） | **待办 W1-1 收尾时一并评** |
| 工作流驱动 `drive` | `coord-core/src/workflow/runtime.rs`（4 处 spawn，已统一走 `spawn_drive`） | ❌（无 supervisor） | ✅ **W5-4**：`WorkerLiveness.panicked` → agent 指标 `coord_agent_workflow_worker_faults_total` + 告警 `CoordAgentWorkflowWorkerFault` | **本轮接上出口**（判据：结束但**未跑到最后一行**才算故障 —— 正常结束是预期行为） |
| 工作流子流程扫描器 | 同上（`start_subflow_scanner`） | ❌ | ✅ **W5-4**：循环型 ⇒ `is_finished()` 即故障 → `coord_agent_workflow_loops_finished` | **本轮接上出口**（tick 已从 O(实例数) 降到 O(登记表)，见 W1-4(a)） |
| agent 心跳刷新 | `coord-agent` | 未复核 | 未复核 | **待办**（§3 表末行） |

---

## §4 待办（进入下一轮，不掩盖）

1. ~~`coord-core/src/workflow/runtime.rs` 的 `drive` 与 `start_subflow_scanner` 需要
   **监督出口**~~ → ✅ **W5-4 已做**（2026-09-21）：`WorkerLiveness` + agent 指标
   + 告警。**但仍有残余**：
   * 两个形态需要**两条**判据（循环型 `is_finished()` / 一次性“结束但未收尾”），
     合并成一个数字会让指标长期噪声化；
   * agent 侧的 15s **采样任务自身**没有监督（coord-agent 无 supervisor 设施）——
     它死亡的表现是“指标停止更新”，见 `docs/production/ops/observability.md` §2.2；
   * 出口只报警，**不自动重启**（与 `CoordBackgroundTaskDead` 同取舍：任务可能持有
     不可重建的本地状态）。
2. agent 心跳/离线检测复核（与 W3 的 agent 面一起做）。
3. `remove_region_*` 的**显式声明**已写入 `boundaries.md`；若将来要支持动态增删 region，
   必须同时接线 `CompactionManager`（否则 region 的 changelog 永不压缩，
   `coord/src/main.rs` 的 `_region_compaction_mgrs` 从不更新）。
