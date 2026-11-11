# KV-T GA Gate（G0–G6 证据与签字）

> Owner: maintainers ｜ Last verified: 2026-11-11（G6 72h 取证通过并签字；取证管道自检落地）

- **状态**：**GO（已签，2026-11-11）**。G0–G6 全部交付；最终 72h run
  `2026-11-08T06:17:09Z → 2026-11-11T06:19:54Z` 按设计判据全绿（§3-1）：历史读失败
  0/12 849、quiet 可用率 0 违规（最差窗口 99.80%）、RTO p95 98.35s ≤ 120s、mvcc
  0 errors、op-liveness 94.5% ≥ 0.9。证据 MANIFEST 与复判命令见 §3-1；身份事后
  钉住（`override(post-hoc)`）的原因与 provenance 见 §3.7。
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
| **G6 GA** | e2e/备份/SDK/Agent/混沌/性能/文档全绿 + 签字 | ✅ | 72h run 全绿（§3-1）；签字见 §4 |

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
| 性能/容量/长稳（#174） | ✅ | 性能与容量数字：`performance.md` §2–§7（sequencer 152/115 txn/s、OCC 冲突率 93.8%、watch e2e p50 540 ms、写放大 72.4×、`PERF_GATE=1 bash scripts/bench-ci.sh` 9/9）；容量无 GC 按 D10 停在 `boundaries.md` B-KVT-13。长稳：Jepsen v2 **72h 通过**（run `2026-11-08T06:17:09Z → 11-11T06:19:54Z`，commit `c105e9a1` / tree `889452f1` / bin `f94abe5d`；261 954 ops、mvcc 0 errors、历史读 0/12 849、quiet 0 违规、RTO p95 98.35s、liveness 94.5%，`make rejudge` = `valid`）——判据、seed 与 MANIFEST 见 §3-1。此前判红/作废的轮次与修复见 §3.5/§3.6/§3.7（保留窗口与历史读窗口 #317/#318、`SAFE_TS_LAG` 裁定 #308、#304、#309、取证管道）。 |
| 混沌/Jepsen（#175） | ✅ | v1 真实故障矩阵在 CI 绿（`chaos_real`、`multi_raft_process_test`）。v2 3 进程矩阵 `kv_t_g6_chaos_v2_test`（CI `chaos-kill9-soak` job，整文件跑）：`chaos_v2_mixed_writes_survive_decision_leader_loss`（kill -9 / SIGSTOP region 0 leader + 混合事务/直写，37 次连续绿）；`chaos_v2_partitioned_region0_leader_keeps_invariants`（对称网络分区隔离 region 0 leader：debug 构建的 `COORD_TEST_RAFT_PARTITION_FILE` 钩子在 raft RPC 层双向阻断，多数派换主、分区窗口内写入、heal 后收敛）；`chaos_v2_shard_seal_stall_keeps_watch_continuous`（`commit_log_shard_records=8` / `shard_secs=1` 下 SIGSTOP region 0 leader ⇒ TSO/sequencer/shard seal 停摆；恢复后 Watch 从 baseline+1 严格递增、无重无丢，sealed shard 指标增长）；`chaos_v2_interrupted_snapshot_install_recovers`（follower 落后 → 快照安装途中冻结 + kill -9 → 重启追平、全部 `commit_ts` 可读）。本轮矩阵抓出并修复两个 GA 阻塞缺陷（见 §3.5），`test_lease_cleanup_is_per_region` 的 CI 间歇超时也一并修掉（PR #268）。**仍未做**：split/merge 迁移中断的进程级注入（§3.3 的 D10 裁定）。 |
| Ops runbook/监控（#176） | ✅ | `monitoring/prometheus-rules.yml` + `docs/production/ops/runbook.md`；`bash scripts/check-gate-drills.sh`（每条告警必须有可达 runbook 锚点） |
| ADR accepted / 文档一致 | ✅ | ADR-0015 状态 accepted；`scripts/check-doc-refs.sh` 0 悬空 |
| 发布 sign-off | ✅ | §4：2026-11-11 GO（证据 MANIFEST、未关闭项见该行） |

## 3. 关闭 G6 的硬条件（当前状态：已满足）

1. #174：**72h 长稳证据已通过**（`make rejudge` = `valid`）。
   run `2026-11-08T06:17:09Z → 2026-11-11T06:19:54Z`（72h02m），commit
   `c105e9a1`（tree `889452f1`）、bin `f94abe5d`（控制机与三节点同哈希），参数
   `--soak-quiet 1800 --soak-disrupt 600 --rate 0.5 --concurrency 1n
   --min-op-ok-ratio 0.9 --mvcc-retention-revs 600000 --v2-history-window-secs 120`，
   seed `1884715972`。判据（`jepsen/soak/soak-kvt.log` + `results.edn`）：
   - 历史读（`read_ts>0`）失败 **0/12 849 = 0.00%**（上限 1%；#311 的判据首次在
     有效采样下判绿）。mvcc 一致性 **0 errors**（writes 68 034 / reads 42 668 /
     ts 12 924；`final-reads 1 280`）。
   - quiet 可用率 **109/110 窗口判、0 违规**（门槛 0.95，最差 99.80%；1 个窗口
     样本不足被跳过）。`SAFE_TS_LAG` 按 #308 的显式裁定从分母剔除并单独计数
     （`excluded-by-error 406`），真正的 `UNAVAILABLE` 仍计入（负控制见 §3.6-5）。
   - RTO **p95 98.35s ≤ 120s**（108 个采样、0 unrecovered；max 124.4s 只记录、
     不参与判定）。
   - op-liveness **94.5% ≥ 0.9**（write 95.3% / read 91.6% / ts 99.99% /
     delete 95.2%；0 `:fail`，`v2 :other` = 0）。
   - 节点日志 0 ERROR；watchdog 0 次 fail-fast。

   证据：`/tmp/coord-evidence-72h/20261111T104403Z-soak-kv-t-72h/`（`MANIFEST.md`、
   `identity.txt`、`results.edn`、`history.edn.gz`、`run.log`、`summary.txt`；
   `sha256sums.txt` 校验通过，`overall-valid: true` / `gates-valid: true`），
   MANIFEST 摘要贴在 #174/#177。复跑判据：
   `cd jepsen/lab && make rejudge JEPSEN_PROVIDER=docker`（15.5s 重判 261 954 op）。
   身份说明：本轮起跑时 #303 的生产侧尚未真正落地（日志无身份头、节点哈希
   `unavailable`），MANIFEST 用 `COORD_IDENTITY_OVERRIDE` 事后钉住并标注
   `override(post-hoc)`；provenance（run 日志 `Test version`、三节点仍在的二进制
   哈希与 mtime、最后一次 server 源码提交）逐条写在 `identity.txt`，原因见 §3.7。
2. #174：**split/merge 停顿已测**（`performance.md` §6.6，release、真实 3 进程、
   PD 驱动 split）：统计/采样/leader 上报落地后（§3.5-6），`perf_split_write_pause`
   不再依赖 debug-only 的调度暂停钩子，直接测持续写入期间的 split 写停顿。
   GA 长稳证据见 §3-1（已通过）。
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
5. 本节 1–4 全绿，§4 已于 2026-11-11 签字 GO；**不得用沉没成本跳过**
   （ADR-0015 D10）。

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

9. **v2 Delete 在 follower 上永久失败（已修）**：v2 Delete 为返回
   `deleted`/`prev_kvs` 先走 leader-only 的 `ensure_linearizable_barrier`；
   follower 上 openraft 返回 `ForwardToLeader`，该屏障把它包成 `INTERNAL`
   （`linearizable read failed: has to forward request`）。v2 客户端只对
   `NOT_LEADER`/`UNAVAILABLE`/`EPOCH_STALE` 重试，`INTERNAL` 归为不可重试的
   `:other` ⇒ 换届后 delete 钉在旧节点上。修复：`RaftError::APIError(
   LinearizableReadError::ForwardToLeader)` 映射为 `UNAVAILABLE + NOT_LEADER`
   （与 `map_client_write_error` 同口径）；回归
   `kv_v2_multi_node_range_test::test_v2_delete_on_a_follower_returns_retriable_not_leader`。
   同一轮把 `soak-kv-t` 的 per-op liveness 下界从 0.1（F-13「从未跑过」地板）
   收紧到 0.9；72h run 实测 delete 4 467/6 311 fail、ok 25.3%，旧门槛仍判绿。

## 3.6 V2 oracle 的取证缺口（已修，test-side）

72h 重跑前审计 `jepsen/src/jepsen/coord/mvcc.clj` 与 `kvv2`/`txnv2` 工作负载，
发现三处「判据写了但没有工作负载喂它」的假绿（与 F-13 同类；只动 test-side，
不动 server）：

1. **txnv2 的原子性/账本判据从未运行**：`check-txn-atomicity` 只认带 `:covers`
   的 `:v2-read`，账本总和只认 `:v2-read` 上的 `:ledger-total`；而 txnv2 只发
   `:v2-txn`/`:v2-ts`（txn op 自己的 `:kvs`/`:ledger-total` 无人读取）。修复：
   txnv2 每 5 个 op 发一次整账本区间读（`/jepsen/v2/acct/` →
   `zz-jepsen-v2/acct0`，`:covers` 16 个账户、`:ledger-total 400`）。账本种子从
   `client/setup!` 移到 generator 首相位——setup 写不进 history，checker 无法用
   `commit_ts` 定位，会把每次账本读判成 `fabricated-revision`。
2. **事务原子性判据本身不健全**：旧判据要求「read_ts ≥ commit_ts 的每次读都
   看到该 txn 的写值」，但后续 txn 合法改写同一账户 ⇒ 60s txnv2 实测 35k 条
   `txn-half-visible` 假红。改为只判「该 txn 在 read_ts 仍是最后写入」的 key：
   部分 key 可见、另一部分应当可见却缺失/陈旧 ⇒ 半应用；被后续写覆盖的 key
   不作证据。未知 `commit_ts` 的 `:info` txn 不再用值相等猜（余额会重复），
   由账本总和兜底（半应用转账必破坏总和）。
3. **kvv2 没有收尾收敛读**：`workload-gen` 的 finale 只覆盖 v1 寄存器/map key；
   v2 的 `:final` 读从未产生 ⇒ 扰动结束后 `safe_ts` 永久停滞也无人判红
   （`read_ts=0` 读会回显停滞的 safe_ts，自洽）。修复：收尾对 `kvv2-keys` 全量
   发 `:final` 读，checker 要求回显 `safe_ts` 覆盖全部已提交写，且至少一条收尾读
   成功（否则 fail-closed）。

checker 结果新增 `:judged`（`final-reads` / `txn-atomic-pairs` / `ledger-sums`），
让「判据真的看过」进 `results.edn` 证据面，而不是靠口头声明。

验证：`make checkers` 18/18（新增 3 个 fixture）；kvv2 smoke（kill/pause/partition
+ 收尾）`:final-reads 1280` 绿；txnv2 60s `:ledger-sums 214`、
`:txn-atomic-pairs 20025` 绿。

**2026-11-07 复判又抓到两处「判据写了但没判」**（与本节同类，均已修）：

4. **gates 只认 v1 op 名**（#308）：`premise`/`quiet availability`/`RTO` 在 kvv2 上
   全部空转，报告仍是 `:valid? true`。现在 write/read op 集合显式覆盖
   `:v2-write/:v2-delete/:v2-txn/:v2-read`，长跑口径（`require-samples?`）下
   「有样本却一条没判」直接判红；v2 的 premise 显式 `:disabled? true`（载荷不是
   全局序号）。负控制：`jepsen/scripts/gates-v2-fixtures/`（3 条，含 judged-0 与
   RTO-not-measured）。
5. **历史读失败率没人计数**（#311）：设计 §5.4 的「>1% ⇒ invalid」从未生效。
   现在 `mvcc/checker` 算「显式历史读（read_ts>0）失败率」并进 `results.edn`
   （`:history`），超上限判 `:reason :history-unavailable-rate`；客户端保留服务端
   `:detail`，`--v2-history-window-secs` 可配。**本轮的 10.08% 就是它第一次生效**。
   负控制：`jepsen/scripts/mvcc-fixtures/` 新增 4 条（含 0/0 必须显式 `:judged false`、
   safe_ts 读不进分母）。

6. **`[storage] changelog_retention_revisions` 只对 region 0 生效**（#317）：
   region-runtime builder 用的是 `CompactionConfig::default()`（100k），数据 Region
   的 compaction 因此忽略配置。fast-soak 实测 `commit_ts` 推进 ~1005 revs/s ⇒
   100k ≈ 100s，仍落在 120s 客户端窗口内。修复后数据 Region 与全局 manager 共用
   同一份配置。
7. **范围历史读把「T 前删除、T 后重建」的 key 误判成不可复原**（#318）：
   `replay_historical_range` 的 `view` 只存值，删除后 key 从 `view` 消失；候选补齐
   只靠 `create_revision > T` 证“T 时不存在”，而 `mark_deleted`/`update` 会**保留
   最初创建 revision**，重建的 key 因此证不出结论 ⇒ 整条范围读 `HISTORY_UNAVAILABLE`。
   单键路径（`get_at_revision`）在同一条历史上正确返回「不存在」。修复：`view`
   缺值但 `last_write` 有记录（T 前最后一次变更是删除）⇒ 确定不存在；changelog
   真被压缩时仍 fail-closed。回归
   `test_historical_range_read_treats_deleted_key_as_absent`（去掉修复会红）。
   这两个缺陷都由 30min fast-soak 在数分钟内暴露（#312 的价值）；上一轮 10.08%
   的历史读失败率里包含了 7 的贡献。

**验证口径的补强（长跑快速失败）**：`make rejudge` 用既有 store 的 262k op 历史做离线复判
（15.5s），改判据不必重跑 72h；长跑内 `jepsen/scripts/soak-watchdog.sh` 周期性复判增量 op 日志，
判据一红就停跑并写 `soak/fail-fast-<name>.edn`（只对 `op-liveness` /
`quiet-availability` 容忍连续 2 次）。验证阶梯见 `jepsen/README.md`。

## 3.7 取证管道的缺陷（已修）

72h run 收尾时发现 #303 的采集器从未端到端跑通过——判据没有放宽，坏的是
「证据怎么产生」这一层：

1. **起跑身份头只进终端，不进日志**：`coord-soak.sh` 把
   `control-binary/commit/tree/dirty` echo 到 stdout，而 `collect-evidence.sh`
   从 `$SOAK_LOG` 解析 ⇒ 每一轮都「缺头」。修复：头用 `tee` 写进日志（同时截断
   旧日志），run 输出改 `>>`。负控制 `bash scripts/check-evidence-identity.sh`
   （CI `gate self-check`）：缺头必须 exit 3、不出 MANIFEST。
2. **节点哈希永远是 `unavailable`**：`db.clj` 读 `(:out (c/exec ...))`，而
   `c/exec` 返回 stdout 字符串 ⇒ `nil` 上 NPE ⇒ 每个节点记 `unavailable`，
   采集器（正确地）拒绝出 MANIFEST。修复：直接 trim 字符串；身份 smoke 实测
   三节点 = 控制机哈希。
3. **MANIFEST 的配置生成器哈希为空**：未转义的反引号在 unquoted heredoc 里被
   当作命令执行（`command not found`），该行落空。修复：转义；自检断言该行是
   16 位 hex。
4. **本轮的 72h 身份是事后钉住的**（原因即 1+2）：`override(post-hoc)`。
   `identity.txt` 的 provenance 不是重抄的横幅——run 日志自带
   `Test version c105e9a…`；tree 是 `c105e9a^{tree}`；控制机二进制 mtime
   `05:01:19Z` 早于 06:17 起跑、晚于最后一次 server 源码提交（`88003c6`，04:40Z）；
   三节点 `/opt/coord/coord` 在 2026-11-11T10:26Z（任何新 `setup!` 之前）仍是同一
   sha256、同一 mtime（teardown 不删该文件，`c/upload` 保留 mtime）。修复后用一次
   60s `kvv2` smoke 做了端到端验证：日志有头、三节点哈希 = 控制机、
   `collect-evidence.sh` 不带 override 出 MANIFEST（PR #327）。

## 4. 签字记录

> 2026-11-11：§3-1 的最终 72h run 按**当前**判据全绿（`make rejudge` = `valid`），
> G0–G6 证据齐备，签字 **GO**。证据身份是 `override(post-hoc)`（原因与 provenance
> 见 §3.7）；列在「未关闭项」里的都是 GA 之后的 follow-up，不影响本次 GA 口径。

| 日期 | 结论 | 依据 commit / CI run | 未关闭项 | 签字人 |
|:--|:--|:--|:--|:--|
| 2026-11-11 | **GO（已签）** | run `2026-11-08T06:17:09Z → 11-11T06:19:54Z`，commit `c105e9a1` / tree `889452f1` / bin `f94abe5d`（控制机 = 三节点）；`make rejudge` = `valid`；MANIFEST `/tmp/coord-evidence-72h/20261111T104403Z-soak-kv-t-72h/`（`override(post-hoc)`，§3.7） | GA 后 follow-up：容量 GC（`boundaries.md` B-KVT-13，D10 停）、split/merge 迁移中断的进程级注入（§3-3，D10 停）、`kv_t_g2_lease_rpc_test` 偶发超时（§1） | maintainers |
| 2026-11-08 | **重跑中（已由 11-11 GO 取代）** | commit `c105e9a1` / tree `889452f1` / bin `f94abe5d`；起跑 `06:17:09Z`，ETA `2026-11-11T06:17Z`；watchdog 300s | 等跑完回填 §3-1/§4 | — |
| 2026-11-07 | **不达标（未签）** | run `2026-11-04T14:44:43Z`，bin `09138e63`，tree `dd2803cc`；`make rejudge` = `invalid`（`:history-unavailable-rate :quiet-availability`） | §3-1 三条：历史读率、quiet 可用率、重跑对齐 | — |

## 5. 回退/降级口径

- v2 集群**不能**回退到 v1 数据面；需要回退时保留旧 v1 集群（ADR-0015 D13）。
- 任一 gate 未达标 ⇒ 停在 gate，不静默降级语义（D10）；`[kv_t].enabled=false` 时 v1
  行为字节级不变（ADR-0001）。
- MQ v2 是 G7，不在本 gate 范围；设计与拆分见 ADR-0016；v2 集群上 MQ v1
  fail-closed（B-KVT-6）。
