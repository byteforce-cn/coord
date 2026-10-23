# KV-T G6 (#176) ops alerts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** KV-T 的特有故障有 Prometheus 告警、可执行的 runbook 处置步骤和 dashboard 出口：TSO 补发失败/leader 抖动、sequencer 校验变慢/冲突风暴、Commit Watermark 停滞、shard seal 超时、split/merge operator 卡住、Watch 历史窗口不足。

**Architecture:** 只消费 `coord-server/src/metrics.rs` 已导出的指标，不加代码/新指标。告警规则集中到 `monitoring/prometheus-rules.yml` 的新 group `coord-kv-t`；每条告警的 `runbook_url` 指向 `docs/production/ops/runbook.md` §6 的同名处置小节，由 `scripts/check-gate-drills.sh` 机器校验（锚点存在 + 无孤儿小节）。Grafana 加一个 KV-T 行（TSO/Sequencer、Commit Log/Watch 两面板），观测面文档登记「指标 → 告警 → runbook」闭环。

**Tech Stack:** Prometheus / Grafana JSON / Markdown。

**Spec:** issue #176；ADR-0015 D9/D10；现有指标见 `coord-server/src/metrics.rs`（`coord_tso_*`、`coord_kvt_sequencer_*`、`coord_kv_t_commit_*`、`coord_kv_t_watch_*`、`coord_pd_queue_*`）。

## Global Constraints

- 不新增代码/指标/依赖；不承诺自动降级（ADR-0015 D10）。
- 每条告警必须有 `runbook_url`，锚点必须真实存在；§6「告警处置」里不得有孤儿小节。
- 告警阈值只做**症状可发现**，不是 SLO 校准（真实 14 天曲线归 #174）；文档不得声称已校准。
- 不把 v1 节点纳入 KV-T 告警：共享指标（PD 队列）用 `and on(instance) coord_kv_t_enabled == 1` 限定。
- 注释/文档描述当前态；不写日期/变更史；不新增 `TODO`/`FIXME`。
- 临时文件只放 `/tmp/coord-*`；门禁同 AGENTS.md。

## Rulings

1. **不新增「积压」gauge。** sequencer 只有 counters；用 `rate(validation_us)/rate(txns)` 表示单事务校验成本，用冲突率表示 OCC 放大。新增 gauge 属代码功能，超出 #176「不做代码功能」。
2. **Watermark stall 用「shard 在超时窗口被 seal」计数。** `coord_kv_t_commit_watermark_stalls_total` 是唯一直接证据；`in_flight` 只进 dashboard/runbook（高水位不一定是故障）。
3. **Split/Merge 卡住用 PD running 队列。** operator 在 `Running` 超过 15m 即卡住；requeue 计数进 dashboard 与 runbook 排查。
4. **Watch 历史不足不是服务端故障。** 告警只提示客户端起点早于保留窗口；处置是客户端全量重同步，禁止调大窗口掩盖。

## File Structure

- Modify: `monitoring/prometheus-rules.yml` — 新 group `coord-kv-t`（9 条告警）。
- Modify: `docs/production/ops/runbook.md` — §6 加 9 个同名处置小节；§7 去掉「无独立告警」并交叉引用。
- Modify: `monitoring/grafana-dashboard.json` — 末尾两个 KV-T 面板（TSO/Sequencer、Commit Log/Watch）。
- Modify: `docs/production/ops/observability.md` — 指标 → 告警 → runbook 对照行。

## Tasks

### Task 1: alerts + runbook

- [ ] **Step 1: Add the 9 rules**（阈值见 Review Focus 表）
- [ ] **Step 2: Add runbook sections** with the exact heading text used by the URLs
- [ ] **Step 3: Run `bash scripts/check-gate-drills.sh`** → OK；失败则按报错修正锚点/孤儿

### Task 2: dashboard + observability

- [ ] **Step 1: Append two panels** to `monitoring/grafana-dashboard.json`（`jq .` 通过）
- [ ] **Step 2: Register the metric families** in `docs/production/ops/observability.md`（消费者 = 告警名 + runbook）

### Task 3: gates + delivery

```bash
bash scripts/check-gate-drills.sh
bash scripts/check-doc-refs.sh
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-panics.sh
cargo audit --no-fetch
cargo deny --offline check
```

PR → CI 全绿 → merge → 删分支；#176 回填证据并关闭。

## Review Focus

| 告警 | 症状判据 | 阈值来源 |
|:--|:--|:--|
| `CoordKvTTsoRefillFailures` | `increase(coord_tso_refill_failures_total[5m]) > 0` | TSO 客户端补发失败（非 leader/不可达） |
| `CoordKvTTsoLeaderChurn` | `increase(coord_tso_leader_changes_total[10m]) >= 3` | region 0 选举抖动 |
| `CoordKvTSequencerValidationSlow` | `validation_us/txns > 50ms`（5m rate） | 单 Raft 组校验成本（D4/D9） |
| `CoordKvTSequencerConflictStorm` | `conflicts/txns > 0.5`（5m rate） | OCC 冲突放大 |
| `CoordKvTWatermarkStall` | `increase(coord_kv_t_commit_watermark_stalls_total[10m]) > 0` | D9 seal 超时 5m |
| `CoordKvTShardSealLag` | `coord_kv_t_commit_log_seal_lag_seconds > 300` | D9 seal 超时 5m |
| `CoordKvTSplitMergeStuck` | `coord_pd_queue_running > 0`（15m，限 v2） | operator Running 超时回收 |
| `CoordKvTWatchHistoryUnavailable` | `increase(coord_kv_t_watch_history_unavailable_total[10m]) > 0` | 客户端游标早于保留窗口 |
| `CoordKvTCommitLogShardsHigh` | `coord_kv_t_commit_log_shards > 100`（30m） | 24h 保留窗口稳态 ~24 shard；GC 停滞 |

- 告警是否只在 v2 节点上触发（共享 PD 队列有 `coord_kv_t_enabled` 限定）？
- runbook 是否给出「不要做什么」（手工 seal/GC、调大保留窗口、跳过 drain）？
- 每条告警的锚点是否由 `check-gate-drills.sh` 实测通过，而不是手写猜测？
