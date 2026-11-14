# 观测面（指标与告警）

> Owner: maintainers ｜ Last verified: 2026-10-23

本文核对关键指标/端点的**产生者 → 消费者 → 验证方式**。
「有消费者」= 有 Prometheus 规则、runbook 小节、或运维会读的端点字段之一；
**只被测试读过的指标不算有消费者**（历史教训：`dead_tasks()` 曾长期无消费者）。

---

## 1. 指标与告警对照表

| # | 指标 / 端点 | 产生者（代码事实） | 消费者 | 验证方式 | 结论 |
|:--|:--|:--|:--|:--|:--|
| 1 | `GET /health?verbose=true`（server） | `coord-server/src/health.rs:340 handle_health_verbose`、`:126 verbose_status()` | 运维/值班（排障第一入口） | 端点字段见 `dead_background_tasks`（`:354`/`:370`） | ✅ 可用 |
| 2 | `coord_dead_background_tasks` / `coord_dead_background_task_info` | `coord-server/src/metrics.rs`（`dead_tasks()` 的唯一消费者） | 告警 `CoordBackgroundTaskDead`（`monitoring/prometheus-rules.yml` 的 `coord-supervision` 组）+ runbook 同名小节 | `bash scripts/check-gate-drills.sh`（断言告警必有可达 runbook 锚点） | ✅ 闭环 |
| 3 | `metrics.method_metrics` 有界：`MAX_METHOD_METRICS = 1024` | `coord-server/src/metrics.rs:31`；超限的新方法归并到溢出桶（**计数不丢失**，只是不再细分，`:33`/`:308`） | 运维（按方法定位热点）；溢出桶本身是"方法爆炸"的信号 | 单测覆盖（`metrics::` 模块）+ 常量与溢出分支在源码可见 | ✅ 可用（有界） |
| 4 | agent `/metrics`：`coord_agent_uptime_seconds` / `_connected` / `_cache_*` / `_grpc_requests_total` / `_watch_subscribers` | `coord-agent/src/metrics.rs::render_prometheus_text` | Grafana agent 面板（`monitoring/grafana-agent-dashboard.json`）；`_connected` 也是 k8s 就绪语义的来源 | `cargo test -p coord-agent --lib metrics` | ✅ 可用 |
| 5 | agent 插件面：`coord_agent_plugin_invocations_total` / `_traps_total` / `_load_failures_total` | 同上（`record_plugin_*`） | 告警 `CoordPluginTrap` / `CoordPluginLoadFailure` / `CoordPluginInvocationErrorRate` + runbook 同名小节 | `cargo test -p coord-agent --lib metrics::tests::test_plugin_metrics_render` | ✅ 闭环 |
| 6 | `coord_agent_workflow_workers_live` / `coord_agent_workflow_worker_faults_total` / `coord_agent_workflow_loops_finished` | `coord-agent/src/metrics.rs::set_workflow_worker_liveness`，由 `coord-agent/src/lib.rs` 的采样任务周期写入（数据源 `coord_core::workflow::runtime::WorkerLiveness`） | 告警 `CoordAgentWorkflowWorkerFault` + runbook 同名小节 + agent ERROR 日志 | `cargo test -p coord-agent --lib metrics::tests::test_workflow_worker_liveness_metrics_render`；核心侧 `cargo test -p coord-core --lib workflow::runtime`（含负控制） | ✅ 闭环 |
| 7 | `coord_tso_batch_remaining` / `_refill_failures_total` / `_leader_changes_total` | `coord-server/src/metrics.rs`（`set_tso_*` / `inc_tso_*`，由 `raft/tso.rs` 的客户端批次写入） | 告警 `CoordKvTTsoRefillFailures` / `CoordKvTTsoLeaderChurn` + runbook 同名处置小节 | `bash scripts/check-gate-drills.sh`；`cargo test -p coord-server --lib metrics::tests::test_metrics_kv_t_tso_gauges_and_counters` | ✅ 闭环 |
| 8 | `coord_kvt_sequencer_batches_total` / `_txns_total` / `_conflicts_total` / `_validation_us_total` | `metrics.rs::record_sequencer_batch`（`kvt/sequencer.rs` apply 路径） | 告警 `CoordKvTSequencerValidationSlow` / `CoordKvTSequencerConflictStorm` + runbook 同名小节 + Grafana `KV-T: TSO / Sequencer` | `check-gate-drills.sh`；sequencer 单测 | ✅ 闭环 |
| 9 | `coord_kv_t_commit_watermark` / `_in_flight` / `_stalls_total` / `_seal_lag_seconds` / `coord_kv_t_commit_log_shards{,_sealed}` / `coord_kv_t_commit_log_outbox_rows_trimmed_total` | `metrics.rs` 的 `set_kv_t_commit_*`（`kvt/commit_log_shards.rs`、`kvt/commit_watermark.rs`） | 告警 `CoordKvTWatermarkStall` / `CoordKvTShardSealLag` / `CoordKvTCommitLogShardsHigh` + runbook 同名小节 + Grafana `KV-T: Commit Log / Watch` | `check-gate-drills.sh`；commit log/seal 测试 | ✅ 闭环 |
| 10 | `coord_kv_t_watch_streams` / `_batches_total` / `_events_total` / `_lag_commit_ts` / `_history_unavailable_total` / `_backpressure_total` | `metrics.rs` 的 `set_kv_t_watch_*`（`kvt/watch_rpc.rs`） | 告警 `CoordKvTWatchHistoryUnavailable` + runbook §6/§7 + Grafana 面板 | `check-gate-drills.sh`；`kv_t_g4_watch_rpc_test` | ✅ 闭环 |
| 11 | `coord_pd_queue_pending` / `_running` / `_terminal` / `coord_pd_operator_requeued_total` | `metrics.rs` 的 `set_pd_queue_depth` / `inc_pd_operator_requeued`（`pd/executor.rs`） | 告警 `CoordKvTSplitMergeStuck`（仅 v2 节点）+ runbook 同名小节 | `check-gate-drills.sh`；split/merge executor 测试 | ✅ 闭环 |
| 12 | `coord_mq_v2_publish_{requests,entries,bytes,duplicates,latency_us,not_leader,rejected}_total` / `coord_mq_v2_append_{batches,requests}_total` | `coord-server/src/metrics.rs::record_mq_v2_publish*` / `record_mq_v2_append_batch`（`mq_v2/services.rs` 写路径） | 告警 `CoordMqV2PublishNotLeader` / `CoordMqV2PublishRejected` / `CoordMqV2PublishLatencyHigh` + runbook 同名小节 + Grafana `MQ v2: Publish / Poll` | `bash scripts/check-gate-drills.sh`；`cargo test -p coord-server --lib metrics::tests::test_metrics_mq_v2` | ✅ 闭环 |
| 13 | `coord_mq_v2_poll_{requests,messages,bytes,empty,out_of_range}_total` / `coord_mq_v2_ack_{requests,entries,rejected}_total` / `coord_mq_v2_subscribe_{streams,messages,dropped}` | `metrics.rs::record_mq_v2_poll*` / `record_mq_v2_ack` / `record_mq_v2_subscribe`（`mq_v2/consume.rs`、`mq_v2/groups.rs`） | 告警 `CoordMqV2PollOutOfRange` / `CoordMqV2AckRejected` / `CoordMqV2SubscribeDrops` + runbook 同名小节 + Grafana `MQ v2: Publish / Poll` | `check-gate-drills.sh`；`mq_v2_m3_consume_test` / `mq_v2_m3_group_test` | ✅ 闭环 |
| 14 | `coord_mq_v2_group_{joins,rebalances,expired_members,gc_removed}_total` / `coord_mq_v2_dlq_moved_total` / `coord_mq_v2_poll_dlq_messages_total` / `coord_mq_v2_retention_{truncate,segments}_total` / `coord_mq_v2_deletion_{partitions,messages,bytes}_total` | `metrics.rs::record_mq_v2_group_*` / `record_mq_v2_dlq_move` / `record_mq_v2_retention` / `record_mq_v2_deletion`（reaper、组状态机、删除回收） | 告警 `CoordMqV2GroupRebalanceStorm` / `CoordMqV2GroupMembersExpired` / `CoordMqV2DlqMoved` + runbook 同名小节 + Grafana `MQ v2: Groups / DLQ / Retention` | `check-gate-drills.sh`；`mq_v2_reaper` / 组回收单测 | ✅ 闭环（lag/配额/保留无连续 gauge，见 §2.4） |

---

## 2. 发现与缺口

### 2.1 工作流 worker 已有出口（缺口已清理）

缺口是 **工作流后台 worker 完全没有出口**：`coord-core` 没有 supervisor、
没有 `tracing`，`spawn` 出去的子流程扫描器与每个实例的 `drive` 死亡时**没有任何路径
会告诉任何人**——「能力死亡必须可观测」在 agent 面的最后一块空白
（server 面的对应物 `dead_tasks()` 已闭环）。

**做法**（三个指标的分工）：

| 形态 | 判据 | 为什么不能合并成一个数字 |
|:--|:--|:--|
| 循环型（子流程扫描器） | 已结束 ⇒ 故障（`is_finished()`） | 它的正常行为是**永不结束** |
| 一次性（每实例 `drive`） | 结束但**未跑到最后一行** ⇒ 故障 | 它的正常结束是**预期**行为 |

合并的后果是"正常结束"也被计为故障 ⇒ 指标长期噪声化 ⇒ 没人看 ⇒ 等于没有。

### 2.2 仍未闭环：采样任务自身没有监督

agent 侧那个 15s 采样任务是**只读**的 `tokio::spawn`，不在任何 supervisor 之下
（coord-agent 没有 supervisor 设施）。它的死亡表现为"指标**停止更新**"——
这在 Prometheus 侧可以通过 `coord_agent_workflow_workers_live` 长期不变来间接发现，
但**不是**一条显式告警。列入下节待办，不掩盖。

### 2.3 MQ v2 的 lag/配额/保留只有按需查询

v2 的指标面是**计数器**：没有每分区 lag、每 topic 计费字节或保留水位的连续
gauge。原因不是遗漏接线，而是这些量由状态机按需计算（`DescribeGroup` 的
`GroupPartitionLag`、`DescribeTopic` 的 `log_bytes`/`earliest_offset`，
`GetTopicDeletion` 的回收进度），周期采样会给每个 topic/分区新增一条时间序列
（默认上限 1 GiB × 1024 分区/分片，基数不可控）。

因此告警覆盖的是**症状**（越界、拒绝、再平衡、死信、丢弃），lag/配额/保留的
诊断走 runbook 的按需查询路径；不写"已闭环实时 lag 指标"这类不成立的结论。
若后续需要连续 lag，先给指标定基数上限（per group 而非 per partition）再接线。

### 2.4 仍未验证：SLO 阈值与真实数据的对照

`docs/production/ops/slo.md` 给了三项 SLO 与错误预算，但**没有** 7×24 的真实曲线
支撑（需要 14 天连续运行）。因此本文件的结论只到"指标面可用、有消费者"，
**不**包含"阈值被真实数据校准过"。

---

## 3. 待办

1. **采样任务自监督**：要么在 coord-agent 引入最小监督设施，要么把采样挪到已有受监督
   任务里（server 侧 `spawn_supervised` 是同型先例，见 `coord-server/src/supervisor.rs:42`）。
2. **真实曲线**：以 14 天连续运行数据产出四条曲线（内存 / 磁盘 /
   `keep_alive` / 重启次数）。
3. `MAX_METHOD_METRICS` 的溢出桶应加一条**告警**（当前只是"存在"）：方法数接近 1024
   意味着有人在动态生成 RPC 名或指标键，属独立缺陷类。
