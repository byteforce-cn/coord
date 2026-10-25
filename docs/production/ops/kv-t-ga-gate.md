# KV-T GA Gate（G0–G6 证据与签字）

> Owner: maintainers ｜ Last verified: 2026-10-25

- **状态**：**NO-GO（未签）**。G0–G5 已交付；G6 有关闭项未清（见 §3）。
- 本文是 GA 状态的**单一事实源**：gate 证据、未达标项、签字记录。语义口径仍以
  `docs/adr/0015-kv-t.md` 为准；能力矩阵在 ADR D13；运维处置在 `runbook.md`。
- 划线规则：**没有可复跑证据的项不写「通过」**；「未跑」必须留在未达标列。

## 1. Gate 总览

| Gate | 出口口径 | 状态 | 证据（可复跑） |
|:--|:--|:--|:--|
| G0 契约冻结 | 契约路径/开关/骨架；v2 默认拒绝 | ✅ | `cargo test -p coord --test kv_t_g0_test`；`apis/contracts/STATUS.md`；CI `proto contract` / wire-sync |
| G1 TSO/revision | TSO 单调不复用；revision=commit_ts | ✅ | `kv_t_g1_kv_rpc_test`、`kv_t_g1_revision_test`、`kv_t_g1_tso_rpc_test`；`mvcc::tests::test_tso_*`、`test_current_revision_uses_meta_not_log_index`；`storage::snapshot::tests::test_snapshot_preserves_tso_and_revision` |
| G2 跨区读/SafeTs | read_ts 一致；SafeTs 不越过未 apply | ✅ | `kv_t_g2_lease_rpc_test`、`kv_t_g2_safe_ts_test`；`coord-server/tests/kv_v2_multi_node_range_test.rs`、`kv_v2_range_fanout_test.rs` |
| G3 Serializable | OCC + sequencer；冲突不写决策 | ✅ | `kv_t_g3_commit_decision_test`、`kv_t_g3_txn_commit_test`、`kv_t_g3_delete_range_test`；`coord-server/tests/kv_t_g3_sequencer_test.rs`（write skew / phantom / 顺序） |
| G4 Watch/Commit Log | 全局序/原子批/历史窗口 | ✅ | `kv_t_g4_watch_coordinator_test`、`kv_t_g4_watch_rpc_test`、`kv_t_g4_commit_log_shards_test`；`test_watch_history_unavailable` |
| G5 Split/Merge | 动态表/epoch；不丢不重 | ✅ | `kv_t_g5_continuity_test`、`kv_t_g5_routing_epoch_test`、`kv_t_g5_write_fence_test`、`kv_t_g5_cross_node_drain_test`、`kv_t_g5_storage_v2_test`；split/merge migrator 套件 |
| **G6 GA** | e2e/备份/SDK/Agent/混沌/性能/文档全绿 + 签字 | ⛔ **未达标** | 见 §2、§3 |

CI 基线：main 的 `workspace tests`、三片 test shard、两个 chaos job、`java example integration`、
`cargo audit + deny`、`fmt + clippy`、`check-doc-refs` 全绿（run 40159844225）。

## 2. G6 分项

| 项 | 状态 | 证据 / 未达标原因 |
|:--|:--|:--|
| v2 全链路 e2e（#168） | ✅ | `kv_t_g6_full_chain_test::test_full_chain_tso_txn_watch_split_merge` |
| v1 冻结与服务面收敛（#173） | ✅ | `kv_t_g6_v1_freeze_test`（4 例）；`[multi_raft].legacy_migration` 与 `[kv_t]` 互斥 |
| 备份/恢复（#169） | ✅ | `backup_cli_test`（save→restore：TSO 不复用/revision 不倒退/Commit Log 连续/清单 fail-closed）；恢复后 Watch 连续 `test_backup_restore_keeps_watch_stream_continuous`（备份前 cursor 续订：pre-backup 历史 → 恢复后新写，无洞；`start_revision` 重读 ts1→ts2→ts3 全可读）；窗口边界 `test_backup_restore_before_retention_is_history_unavailable`（`gc_before_ts` 随备份恢复，低于它的起点两次请求都终态 `OUT_OF_RANGE` + `HISTORY_UNAVAILABLE`）。重启可见性依赖 `region_restart_test`（PR #264） |
| Agent 数据面 v2（#170） | ✅ | `kv_t_g6_agent_services_v2_test::test_v2_cluster_serves_all_builtin_services`（15 个非 MQ 内建服务在真实 v2 集群注册并各调一次；idgen 走 segment ⇒ v2 Txn CAS）；`agent_v2_*_proxy_test`、`agent_v2_mode_test` |
| Workflow/MQ fail-closed（#171） | ✅ | `agent_workflow_durable_gate_test`（6 例：v2 拒绝 mq / workflow_durable，纯 KV workflow 可用）；`KvTV2ClusterIntegrationTest#workflowPureKvPathCompletesOnV2` |
| Rust client / Java SDK v2（#172） | ✅ | Rust：`cargo test -p coord-client`（含 `lease_v2_background_keep_alive_renews_and_stops`、`storage_v2_open_put_*`）；Java：`KvTV2ClusterIntegrationTest`（`mvn -Pit`，真实 v2 集群）；`java-example` v2 示例 `KvTV2Example`（`bash java-example/scripts/run-v2-example.sh`，非 v2 集群退出码 2，无 v1 降级）；跨语言互操作 `KvTInteropIntegrationTest` + `kvt_interop_peer`（`bash scripts/check-kvt-interop.sh`：Rust 写 → Java 按精确 `commit_ts` 读，再反向；对账 value/`mod_revision`/`create_revision` 与 Watch 批 `commit_ts`） |
| 性能/容量/长稳（#174） | ⚠️ 部分 | 已测（`performance.md` §2–§6，命令可复跑）：存储写/读/fanout/多 Region 基线；sequencer 吞吐 152/115 txn/s（批均值 1.0）、OCC 冲突率 93.8%、watch 端到端 p50 540 ms、写放大 72.4×、`PERF_GATE=1 bash scripts/bench-ci.sh` 9/9 绿。容量无 GC 按 D10 记为 `boundaries.md` B-KVT-13（停，不静默）。**未达标**：split/merge 停顿未测；**72h soak 运行中**（server commit `4633507`，start 2026-10-25T07:44:11Z），完成前不算通过 |
| 混沌/Jepsen（#175） | ⚠️ 部分 | v1 真实故障矩阵在 CI 绿（`chaos_real`、`multi_raft_process_test`）；v2 决策面故障 `kv_t_g6_chaos_v2_test::chaos_v2_mixed_writes_survive_decision_leader_loss`（3 进程 + kill -9 / SIGSTOP region 0 leader + 混合事务/直写；断言 commit_ts 可读、`mod_revision==commit_ts`、safe_ts 追上、无内部不变量锚点）已接入 `chaos-kill9-soak` job，连续 37 次运行无失败。此前「约 1/10 读不到」的根因：**PD 在首个调度 tick 合并两个空 Region**，merge watermark 之后的旧 revision 按设计返回 `HISTORY_UNAVAILABLE`（B-KVT-10），旧夹具把错误吞成「不可读」。**未覆盖**：网络分区注入、split/merge 迁移中断、shard seal stall、快照恢复中断 |
| Ops runbook/监控（#176） | ✅ | `monitoring/prometheus-rules.yml` + `docs/production/ops/runbook.md`；`bash scripts/check-gate-drills.sh`（每条告警必须有可达 runbook 锚点） |
| ADR accepted / 文档一致 | ✅ | ADR-0015 状态 accepted；`scripts/check-doc-refs.sh` 0 悬空 |
| 发布 sign-off | ⛔ | §4 未填 |

## 3. 关闭 G6 的硬条件（当前未满足）

1. #174：**72h v2 soak 仍在运行**（`SOAK_SUITE=v2 SOAK_DURATION_SECS=259200`，server
   commit `4633507`）——完成且证据目录里 `soak-report.md` 为通过，才算这一条达标。
2. #174：**split/merge 停顿未测**（语义已由 g5 套件覆盖；停顿/失败率没有数字，
   不写「达标」）。其余事务面数字已落在 `performance.md` §6。
3. #175：**混沌矩阵未覆盖**：网络分区注入、split/merge 迁移中断、shard seal stall、
   快照恢复中断。要么补实现，要么按 D10 走 ADR scope 裁定——不得用「已覆盖
   kill/pause」代替整张矩阵。
4. 容量无 GC 已在 `boundaries.md` B-KVT-13 明确停止（D10：停，不静默）；给
   `start_ts` 定下限后再做 GC 属后续版本。
5. 本节 1–4 全绿后填 §4 签字；**不得用沉没成本跳过**（ADR-0015 D10）。

## 4. 签字记录

| 日期 | 结论 | 依据 commit / CI run | 未关闭项 | 签字人 |
|:--|:--|:--|:--|:--|
| — | **未签** | — | §3 全部 | — |

## 5. 回退/降级口径

- v2 集群**不能**回退到 v1 数据面；需要回退时保留旧 v1 集群（ADR-0015 D13）。
- 任一 gate 未达标 ⇒ 停在 gate，不静默降级语义（D10）；`[kv_t].enabled=false` 时 v1
  行为字节级不变（ADR-0001）。
- MQ v2 是 G7，不在本 gate 范围；v2 集群上 MQ v1 fail-closed（B-KVT-6）。
