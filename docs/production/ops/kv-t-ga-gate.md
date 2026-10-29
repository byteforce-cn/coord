# KV-T GA Gate（G0–G6 证据与签字）

> Owner: maintainers ｜ Last verified: 2026-10-28（本轮：Region 统计/采样上报落地、自动 split 端到端打通、split 停顿实测）

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
`cargo audit + deny`、`fmt + clippy`、`check-doc-refs` 全绿（run 40180615805 重跑后；
Java v2/interop 见 #262，chaos/soak/perf 见 #263）。**已知不稳定项**：#265
（`kv_t_g2_lease_rpc_test::test_lease_cleanup_is_per_region` 间歇 30s 超时，重跑可过）
——签字前要么修掉、要么在签字记录里显式标注。

## 2. G6 分项

| 项 | 状态 | 证据 / 未达标原因 |
|:--|:--|:--|
| v2 全链路 e2e（#168） | ✅ | `kv_t_g6_full_chain_test::test_full_chain_tso_txn_watch_split_merge` |
| v1 冻结与服务面收敛（#173） | ✅ | `kv_t_g6_v1_freeze_test`（4 例）；`[multi_raft].legacy_migration` 与 `[kv_t]` 互斥 |
| 备份/恢复（#169） | ✅ | `backup_cli_test`（save→restore：TSO 不复用/revision 不倒退/Commit Log 连续/清单 fail-closed）；恢复后 Watch 连续 `test_backup_restore_keeps_watch_stream_continuous`（备份前 cursor 续订：pre-backup 历史 → 恢复后新写，无洞；`start_revision` 重读 ts1→ts2→ts3 全可读）；窗口边界 `test_backup_restore_before_retention_is_history_unavailable`（`gc_before_ts` 随备份恢复，低于它的起点两次请求都终态 `OUT_OF_RANGE` + `HISTORY_UNAVAILABLE`）。重启可见性依赖 `region_restart_test`（PR #264） |
| Agent 数据面 v2（#170） | ✅ | `kv_t_g6_agent_services_v2_test::test_v2_cluster_serves_all_builtin_services`（15 个非 MQ 内建服务在真实 v2 集群注册并各调一次；idgen 走 segment ⇒ v2 Txn CAS）；`agent_v2_*_proxy_test`、`agent_v2_mode_test` |
| Workflow/MQ fail-closed（#171） | ✅ | `agent_workflow_durable_gate_test`（6 例：v2 拒绝 mq / workflow_durable，纯 KV workflow 可用）；`KvTV2ClusterIntegrationTest#workflowPureKvPathCompletesOnV2` |
| Rust client / Java SDK v2（#172） | ✅ | Rust：`cargo test -p coord-client`（含 `lease_v2_background_keep_alive_renews_and_stops`、`storage_v2_open_put_*`）；Java：`KvTV2ClusterIntegrationTest`（`mvn -Pit`，真实 v2 集群）；`java-example` v2 示例 `KvTV2Example`（`bash java-example/scripts/run-v2-example.sh`，非 v2 集群退出码 2，无 v1 降级）；跨语言互操作 `KvTInteropIntegrationTest` + `kvt_interop_peer`（`bash scripts/check-kvt-interop.sh`：Rust 写 → Java 按精确 `commit_ts` 读，再反向；对账 value/`mod_revision`/`create_revision` 与 Watch 批 `commit_ts`） |
| 性能/容量/长稳（#174） | ⚠️ 部分 | 已测（`performance.md` §2–§7，命令可复跑）：存储写/读/fanout/多 Region 基线；sequencer 吞吐 152/115 txn/s（批均值 1.0）、OCC 冲突率 93.8%、watch 端到端 p50 540 ms、写放大 72.4×、`PERF_GATE=1 bash scripts/bench-ci.sh` 9/9 绿。容量无 GC 按 D10 记为 `boundaries.md` B-KVT-13（停，不静默）。**72h v2 soak 已通过**（2026-10-25T15:05:11Z → 2026-10-28T15:18:46Z，main `c53bbfb`；103 630 轮、TSO 2.556 亿、863 次 kill -9 region 0 leader 注入、watchdog 0 次、节点日志 0 ERROR、无不变量锚点；报告见 #174 评论）。**split/merge 停顿已测**（§3.5-6：统计/采样/leader 上报 + 自动 split 打通；数字见 `performance.md` §6.6）。剩余未达标：GA 最终长稳证据由 Jepsen v2（#272）承担 |
| 混沌/Jepsen（#175） | ✅ | v1 真实故障矩阵在 CI 绿（`chaos_real`、`multi_raft_process_test`）。v2 3 进程矩阵 `kv_t_g6_chaos_v2_test`（CI `chaos-kill9-soak` job，整文件跑）：`chaos_v2_mixed_writes_survive_decision_leader_loss`（kill -9 / SIGSTOP region 0 leader + 混合事务/直写，37 次连续绿）；`chaos_v2_partitioned_region0_leader_keeps_invariants`（对称网络分区隔离 region 0 leader：debug 构建的 `COORD_TEST_RAFT_PARTITION_FILE` 钩子在 raft RPC 层双向阻断，多数派换主、分区窗口内写入、heal 后收敛）；`chaos_v2_shard_seal_stall_keeps_watch_continuous`（`commit_log_shard_records=8` / `shard_secs=1` 下 SIGSTOP region 0 leader ⇒ TSO/sequencer/shard seal 停摆；恢复后 Watch 从 baseline+1 严格递增、无重无丢，sealed shard 指标增长）；`chaos_v2_interrupted_snapshot_install_recovers`（follower 落后 → 快照安装途中冻结 + kill -9 → 重启追平、全部 `commit_ts` 可读）。本轮矩阵抓出并修复两个 GA 阻塞缺陷（见 §3.5），`test_lease_cleanup_is_per_region` 的 CI 间歇超时也一并修掉（PR #268）。**仍未做**：split/merge 迁移中断的进程级注入（§3.3 的 D10 裁定）。 |
| Ops runbook/监控（#176） | ✅ | `monitoring/prometheus-rules.yml` + `docs/production/ops/runbook.md`；`bash scripts/check-gate-drills.sh`（每条告警必须有可达 runbook 锚点） |
| ADR accepted / 文档一致 | ✅ | ADR-0015 状态 accepted；`scripts/check-doc-refs.sh` 0 悬空 |
| 发布 sign-off | ⛔ | §4 未填 |

## 3. 关闭 G6 的硬条件（当前未满足）

1. #174：**72h v2 soak 已通过**（`SOAK_SUITE=v2 SOAK_DURATION_SECS=259200`，
   2026-10-25T15:05:11Z → 2026-10-28T15:18:46Z，main `c53bbfb`）：
   `soak-report.md` 结论 ✅（无一致性违规）；103 630 轮、TSO 2.555 亿、863 次
   kill -9 region 0 leader 注入后恢复、watchdog 0 次、三个节点日志 0 条 ERROR、
   无 `passed unapplied decision` / `decision apply stopped` /
   `behind the applied revision` 锚点。证据目录 `/tmp/coord-soak72/`
   （`driver.log`、`soak-report.md`、`v2-nodes/node-*.log`），报告全文贴在 #174。
   **最终提交口径**：本轮之后 server 代码有变更（split 导出水位、目标组转发），
   按维护者 2026-10-28 裁定，最终 GA 提交上的长稳证据由 **Jepsen v2（#272）**
   承担，不再重跑 72h bash 浸泡——在该证据落地前不签字。
2. #174：**split/merge 停顿已测**（`performance.md` §6.6，release、真实 3 进程、
   PD 驱动 split）：统计/采样/leader 上报落地后（§3.5-6），`perf_split_write_pause`
   不再依赖 debug-only 的调度暂停钩子，直接测持续写入期间的 split 写停顿。剩余
   未达标项只剩 GA 最终长稳证据（由 Jepsen v2 承担，见 §3-1）。
3. #175：**split/merge 迁移中断的进程级注入按 D10 明确停止**（不是「已覆盖」的
   代称）：executor 级失败注入已覆盖「迁移失败 → 保留 intent、不动表、释放
   write fence、不 cutover」（`kv_t_g5_split_merge_test::test_migration_failure_keeps_intent_for_recovery`、
   `pd::executor` 的 `test_split_migration_failure_*`），split/merge 数据连续性由
   g5 套件覆盖；要把「迁移进行中被 kill」做成进程级用例，必须在生产迁移路径上
   加暂停开关（会随二进制发布），代价大于收益——因此不做，剩余的故障面由进程级
   kill/pause/partition 覆盖控制面。网络分区、shard seal stall、快照恢复中断已补
   （见 §2）。
4. 容量无 GC 已在 `boundaries.md` B-KVT-13 明确停止（D10：停，不静默）；给
   `start_ts` 定下限后再做 GC 属后续版本。
5. 本节 1–4 全绿后填 §4 签字；**不得用沉没成本跳过**（ADR-0015 D10）。

## 3.5 故障矩阵抓出的缺陷（均已修）

1. **快照限速让小快照永久卡住**：`SnapshotRateLimiter` 的桶容量 = 速率；
   `transmit` 按 2MiB 分块申请，`rate < 2MiB` 时 `acquire(2MiB)` 永远攒不够
   token ⇒ 发送方无限自旋、follower 永远收不到快照（无超时/告警）。修复：桶容量
   = `max(rate, 块大小)`（`with_burst`），回归
   `storage::snapshot_limiter::tests::test_burst_capacity_covers_large_requests` /
   `test_acquire_larger_than_rate_eventually_returns`。
2. **PD 自动 split 永远被拒绝（已修）**：split-checker 生成的候选带
   `new_region_id=0`，`plan_split` 以「new region id 0 is invalid」拒绝，生成源
   从未分配 ID ⇒ 只有显式带 ID 的测试 operator 能跑。修复：入队前用
   `PdMetaStore::allocate_region_id()` 分配（生成源唯一 = region 0 leader），
   回归 `pd::tests::test_generated_split_gets_a_fresh_region_id`；进程级复现/
   测量见 `perf_split_write_pause`。
3. **自动 split 在多节点上失败（已修）**：`RangeImportMigrator` 的
   `reset_range`/`import_range` 直接对**本节点**的 target raft `client_write`；
   目标组 leader 在别的节点时返回 `ForwardToLeader`（`None, None` 竞态窗口也会
   中招），split 只在执行节点同时是源与目标 leader 时才成功。修复：新增节点间
   `SubmitRegionCommand` RPC（与 `SubmitPdOp` 同一条认证/连接池路径），搬迁器把
   目标组的 `ClearImportRange`/`ImportRange` 提到 leader 提出，换届窗口有界重试。
   回归：`kv_t_g5_region_command_forward_test::test_import_range_forwards_to_the_target_leader`
   （两节点组，导入端口建在 follower 上，旧实现报 `has to forward request to`）。
4. **直写 Region 的 split 导出守卫永不成立（已修）**：`import_pages`
   曾要求 `last_applied_decision_ts == 导出 revision`，但直写（`coord.kv.v2` 自带
   `commit_ts`、lease 清理）推进 `current_revision` 而不推进决策游标；有直写的
   Region 在 split 时永远被守卫拒绝。修复：导出水位以下不得有未 apply 的决策
   （`next_pending_decision_ts(cursor) > 导出水位`，与 safe_ts 同一判据），导出前
   有界等决策屏障，水位之后取 `current_revision()`。回归：
   `kv_t_g5_split_migrator_test::test_split_exports_a_region_whose_revision_is_ahead_of_its_decision_cursor`
   （旧实现报 `decision cursor … is behind the exported revision`）、
   `test_split_fails_closed_when_a_decision_below_the_watermark_is_unapplied`
   （已决定未 apply 的决策仍 fail-closed）。口径见 ADR-0015 D6 的 G5 cutover 连续性。
5. **快照安装后重启被判 log store corrupted**：整段 purge 后日志表为空，
   `get_log_state` 报 `last_log_id=None < last_purged=Some(..)` ⇒ openraft 拒绝
   启动（节点无法重启）。修复：`last_log_id = last_entry.or(last_purged)`（与
   openraft memstore 同口径），回归
   `raft::log_store::tests::test_get_log_state_after_full_purge_survives_reopen`；
   进程级回归 `chaos_v2_interrupted_snapshot_install_recovers`。

6. **PD 收不到 Region 心跳（已修）**：心跳循环只喂**本节点**的 PD，而 operator
   生成收敛在 region 0 leader 的 PD——其他节点上报的 `approximate_keys` /
   `approximate_size` / `storage_bytes` / leader 视图永远到不了调度节点；
   `region_sample_keys` 只有测试注入。修好迁移路径之后，自动 split/merge 依然
   不会发生。修复：Region leader 周期经节点间 `ReportRegionStats` RPC 把统计 +
   分位采样 Key 送到 region 0 leader 的 PD（内存瞬态、不进 raft 日志；
   `MvccStore::sample_keys_in` 有界探测，无样本回退 `mid_key`），并补齐
   `coord_region_split_total`/`coord_region_merge_total`/`coord_regions_total`
   的生产写入点。回归：`pd_embedded_test`（leader 节点视图与统计）、
   `kv_t_g6_chaos_v2_test::autosplit_runs_after_region_heartbeat_reaches_pd`
   （3 进程自动 split）、`storage::mvcc::tests::test_sample_keys_in_*`；
   `perf_split_write_pause` 已产出停顿数字（`performance.md` §6.6）。

7. **PD 元数据跨节点不收敛（本轮发现并修复）**：Region 表由 region 0 raft 复制到
   每个节点，但 split/merge 执行器只更新**执行节点**的 `PdMetaStore`；其他节点
   （包括调度所在的 region 0 leader 节点）的调度视图停在旧表，按旧统计重复生成
   operator。修复：心跳循环每拍把本地已 apply 的权威表对账到本机 PD 元数据
   （`PlacementDriver::reconcile_region_table`，执行器收尾复用同一实现）；回归
   `kv_t_g6_chaos_v2_test::autosplit_runs_after_region_heartbeat_reaches_pd`
   的 `coord_regions_total` 断言（旧实现 split 成功但 PD 表不增长）。

8. **merge 导出水位口径过强（已修）**：`migrate_merge` 要求
   `donor_cursor == pin == receiver_revision == receiver_cursor`。直写只推进
   revision、不产生决策，任何有直写/并发写的 Region 都会永久 fail-closed，merge
   在实际负载下不可用。修复：以**两侧决策游标相等**为判据（有界等待；游标以下双方
   都已有数据，以上才重放），导出水位取 donor 当前 revision，导入完成后复查两侧
   游标与源水位未前进（导入期间前进 → fail-closed 重试）。回归：
   `kv_t_g5_merge_migrator_test`（`test_merge_allows_revision_gaps_when_decision_cursors_align`、
   `test_merge_allows_receiver_revision_behind_pin_while_cursor_matches`、
   `test_merge_fails_closed_when_cursor_moves_during_import`、两个游标不等用例）。

## 4. 签字记录

| 日期 | 结论 | 依据 commit / CI run | 未关闭项 | 签字人 |
|:--|:--|:--|:--|:--|
| — | **未签** | — | §3 全部 | — |

## 5. 回退/降级口径

- v2 集群**不能**回退到 v1 数据面；需要回退时保留旧 v1 集群（ADR-0015 D13）。
- 任一 gate 未达标 ⇒ 停在 gate，不静默降级语义（D10）；`[kv_t].enabled=false` 时 v1
  行为字节级不变（ADR-0001）。
- MQ v2 是 G7，不在本 gate 范围；v2 集群上 MQ v1 fail-closed（B-KVT-6）。
