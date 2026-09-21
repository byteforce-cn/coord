# 观测面复核（W5-1 / P-Gate 7）

- **日期**：2026-09-21
- **锚点**：`docs/production/production-readiness-plan-2026-09-21.md` §5 W5-1
  （「指标面复核：`/health?verbose=true`、`coord_dead_background_tasks`、
  `metrics.method_metrics`（`MAX_METHOD_METRICS` 有界）逐项可用且有消费者」）
- **判据形式**：每一项给出「**谁产生** / **谁消费** / **怎么验证**」。
  「有消费者」= 有 Prometheus 规则、runbook 小节、或运维会读的端点字段之一；
  **只被测试读过的指标不算有消费者**（第四轮 §3.13 的教训：`dead_tasks()` 曾长期无消费者）。

---

## §1 复核表

| # | 指标 / 端点 | 产生者（代码事实） | 消费者 | 验证方式 | 结论 |
|:--|:--|:--|:--|:--|:--|
| 1 | `GET /health?verbose=true`（server） | `coord-server/src/health.rs:340 handle_health_verbose`、`:126 verbose_status()` | 运维/值班（排障第一入口） | 端点字段见 `dead_background_tasks`（`:354`/`:370`） | ✅ 可用 |
| 2 | `coord_dead_background_tasks` / `coord_dead_background_task_info` | `coord-server/src/metrics.rs`（`dead_tasks()` 的唯一消费者） | 告警 `CoordBackgroundTaskDead`（`monitoring/prometheus-rules.yml` 的 `coord-supervision` 组）+ runbook §6 同名小节 | `bash scripts/check-gate-drills.sh`（断言告警必有可达 runbook 锚点） | ✅ 闭环（C21） |
| 3 | `metrics.method_metrics` 有界：`MAX_METHOD_METRICS = 1024` | `coord-server/src/metrics.rs:31`；超限的新方法归并到溢出桶（**计数不丢失**，只是不再细分，`:33`/`:308`） | 运维（按方法定位热点）；溢出桶本身是"方法爆炸"的信号 | 单测覆盖（`metrics::` 模块）+ 常量与溢出分支在源码可见 | ✅ 可用（有界） |
| 4 | agent `/metrics`：`coord_agent_uptime_seconds` / `_connected` / `_cache_*` / `_grpc_requests_total` / `_watch_subscribers` | `coord-agent/src/metrics.rs::render_prometheus_text` | Grafana agent 面板（`monitoring/grafana-agent-dashboard.json`）；`_connected` 也是 k8s 就绪语义的来源 | `cargo test -p coord-agent --lib metrics` | ✅ 可用 |
| 5 | agent 插件面：`coord_agent_plugin_invocations_total` / `_traps_total` / `_load_failures_total` | 同上（`record_plugin_*`） | 告警 `CoordPluginTrap` / `CoordPluginLoadFailure` / `CoordPluginInvocationErrorRate` + runbook §6 | `cargo test -p coord-agent --lib metrics::tests::test_plugin_metrics_render` | ✅ 闭环 |
| 6 | **W5-4 新增**：`coord_agent_workflow_workers_live` / `coord_agent_workflow_worker_faults_total` / `coord_agent_workflow_loops_finished` | `coord-agent/src/metrics.rs::set_workflow_worker_liveness`，由 `coord-agent/src/lib.rs` 的采样任务周期写入（数据源 `coord_core::workflow::runtime::WorkerLiveness`） | 告警 `CoordAgentWorkflowWorkerFault` + runbook §6 同名小节 + agent ERROR 日志 | `cargo test -p coord-agent --lib metrics::tests::test_workflow_worker_liveness_metrics_render`；核心侧 `cargo test -p coord-core --lib workflow::runtime`（含负控制） | ✅ 闭环（本轮新增） |

---

## §2 复核发现（诚实版）

### 2.1 「指标存在但没人消费」这一类，本轮又清掉一项

W5-4 之前的缺口是 **工作流后台 worker 完全没有出口**：`coord-core` 没有 supervisor、
没有 `tracing`，`spawn` 出去的子流程扫描器与每个实例的 `drive` 死亡时**没有任何路径
会告诉任何人**。这是第四轮 §6.3.6「能力死亡必须可观测」在 agent 面的最后一块空白
（server 面的对应物 `dead_tasks()` 已在 C21 闭环）。

**本轮做法**（三个指标的分工见 `docs/production/ops/unwired-mechanisms.md` §3）：

| 形态 | 判据 | 为什么不能合并成一个数字 |
|:--|:--|:--|
| 循环型（子流程扫描器） | 已结束 ⇒ 故障（`is_finished()`） | 它的正常行为是**永不结束** |
| 一次性（每实例 `drive`） | 结束但**未跑到最后一行** ⇒ 故障 | 它的正常结束是**预期**行为 |

合并的后果是"正常结束"也被计为故障 ⇒ 指标长期噪声化 ⇒ 没人看 ⇒ 等于没有
（`remaining-known-gaps.md:249`「红色的门禁是教人忽略的门禁」）。

### 2.2 仍未闭环：采样任务自身没有监督

agent 侧那个 15s 采样任务是**只读**的 `tokio::spawn`，不在任何 supervisor 之下
（coord-agent 没有 supervisor 设施）。它的死亡表现为"指标**停止更新**"——
这在 Prometheus 侧可以通过 `coord_agent_workflow_workers_live` 长期不变来间接发现，
但**不是**一条显式告警。列入 §3 待办，不掩盖。

### 2.3 仍未验证：SLO 阈值与真实数据的对照（W5-5）

`docs/production/ops/slo.md` 给了三项 SLO 与错误预算，但**没有** 7×24 的真实曲线
支撑（需要 W3-7 的 14 天运行）。因此本文件的结论只到"指标面可用、有消费者"，
**不**包含"阈值被真实数据校准过"。

---

## §3 待办（进入下一轮）

1. **采样任务自监督**：要么在 coord-agent 引入最小监督设施，要么把采样挪到已有受监督
   任务里（server 侧 `spawn_supervised` 是同型先例，见 `coord-server/src/supervisor.rs:42`）。
2. **W5-5 曲线**：与 W3-7 的 14 天运行共用数据，产出四条曲线（内存 / 磁盘 /
   `keep_alive` / 重启次数）。
3. `MAX_METHOD_METRICS` 的溢出桶应加一条**告警**（当前只是"存在"）：方法数接近 1024
   意味着有人在动态生成 RPC 名或指标键，属独立缺陷类。
